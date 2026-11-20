"""SQLite archive for complete traces.

A trace is written in a single transaction: the trace row, every span row and
the archive bookkeeping either all land or none do.  Only after ``commit()``
returns does the sampler release the trace's slot.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from typing import Any, Optional

from .models import SpanRecord

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = FULL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS archived_traces (
    trace_id        TEXT PRIMARY KEY,
    first_seen_ns   INTEGER NOT NULL,
    last_span_ns    INTEGER NOT NULL,
    archived_at_ns  INTEGER NOT NULL,
    span_count      INTEGER NOT NULL,
    error_count     INTEGER NOT NULL,
    root_name       TEXT,
    root_span_id    TEXT,
    service_names   TEXT NOT NULL DEFAULT '[]',
    min_start_ns    INTEGER,
    max_end_ns      INTEGER,
    duration_ns     INTEGER
);

CREATE TABLE IF NOT EXISTS archived_spans (
    trace_id        TEXT NOT NULL,
    span_id         TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    parent_span_id  TEXT,
    name            TEXT NOT NULL DEFAULT '',
    kind            TEXT NOT NULL DEFAULT 'UNSPECIFIED',
    status_code     TEXT NOT NULL DEFAULT 'UNSET',
    status_message  TEXT NOT NULL DEFAULT '',
    service_name    TEXT,
    start_time_ns   INTEGER,
    end_time_ns     INTEGER,
    duration_ns     INTEGER,
    event_count     INTEGER NOT NULL DEFAULT 0,
    link_count      INTEGER NOT NULL DEFAULT 0,
    attributes_json TEXT NOT NULL DEFAULT '{}',
    resource_json   TEXT NOT NULL DEFAULT '{}',
    scope_json      TEXT NOT NULL DEFAULT '{}',
    raw_json        TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (trace_id, span_id),
    FOREIGN KEY (trace_id) REFERENCES archived_traces(trace_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_archived_traces_archived_at
    ON archived_traces (archived_at_ns DESC);
CREATE INDEX IF NOT EXISTS idx_archived_spans_trace
    ON archived_spans (trace_id, seq);
"""


class ArchiveStore:
    """Thread-safe SQLite archive.

    All methods are synchronous; callers run them in a worker thread.
    """

    def __init__(self, path: str) -> None:
        self.path = path
        directory = os.path.dirname(os.path.abspath(path))
        if directory:
            os.makedirs(directory, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, timeout=10.0, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def write_trace(self, trace_id: str, spans: list[SpanRecord]) -> None:
        """Atomically archive a whole trace.  Raises on any failure."""

        if not spans:
            raise ValueError("refusing to archive an empty trace")

        ordered = sorted(spans, key=lambda s: (s.seq, s.span_id))
        starts = [int(s.start_time_ns) for s in ordered if s.start_time_ns]
        ends = [int(s.end_time_ns) for s in ordered if s.end_time_ns]
        min_start = min(starts) if starts else None
        max_end = max(ends) if ends else None
        first_seen_ns = time.time_ns()
        duration = max_end - min_start if min_start is not None and max_end is not None else None

        roots = [s for s in ordered if s.parent_span_id is None]
        root = roots[0] if roots else ordered[0]
        services = sorted({s.service_name for s in ordered if s.service_name})
        error_count = sum(1 for s in ordered if s.is_error)

        with self._lock:
            conn = self._conn
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT OR REPLACE INTO archived_traces ("
                    " trace_id, first_seen_ns, last_span_ns, archived_at_ns,"
                    " span_count, error_count, root_name, root_span_id,"
                    " service_names, min_start_ns, max_end_ns, duration_ns"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        trace_id,
                        first_seen_ns,
                        first_seen_ns,
                        first_seen_ns,
                        len(ordered),
                        error_count,
                        root.name,
                        root.span_id,
                        json.dumps(services, ensure_ascii=False),
                        min_start,
                        max_end,
                        duration,
                    ),
                )
                conn.execute("DELETE FROM archived_spans WHERE trace_id = ?", (trace_id,))
                conn.executemany(
                    "INSERT INTO archived_spans ("
                    " trace_id, span_id, seq, parent_span_id, name, kind,"
                    " status_code, status_message, service_name,"
                    " start_time_ns, end_time_ns, duration_ns, event_count,"
                    " link_count, attributes_json, resource_json, scope_json, raw_json"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            trace_id,
                            span.span_id,
                            span.seq,
                            span.parent_span_id,
                            span.name,
                            span.kind,
                            span.status_code,
                            span.status_message,
                            span.service_name,
                            int(span.start_time_ns) if span.start_time_ns else None,
                            int(span.end_time_ns) if span.end_time_ns else None,
                            span.duration_ns,
                            span.event_count,
                            span.link_count,
                            span.attributes_json,
                            span.resource_json,
                            span.scope_json,
                            span.raw_json,
                        )
                        for span in ordered
                    ],
                )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    def get_trace(self, trace_id: str) -> Optional[dict[str, Any]]:
        trace_id = trace_id.strip().lower()
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM archived_traces WHERE trace_id = ?", (trace_id,)
            ).fetchone()
            if row is None:
                return None
            span_rows = self._conn.execute(
                "SELECT * FROM archived_spans WHERE trace_id = ? ORDER BY seq, span_id",
                (trace_id,),
            ).fetchall()
        trace = dict(row)
        trace["service_names"] = json.loads(trace.get("service_names") or "[]")
        trace["spans"] = [self._span_from_row(span) for span in span_rows]
        return trace

    def list_traces(self, limit: int = 100, offset: int = 0) -> tuple[list[dict], int]:
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        with self._lock:
            total = self._conn.execute(
                "SELECT COUNT(*) FROM archived_traces"
            ).fetchone()[0]
            rows = self._conn.execute(
                "SELECT trace_id, archived_at_ns, span_count, error_count,"
                " root_name, service_names, duration_ns, min_start_ns"
                " FROM archived_traces ORDER BY archived_at_ns DESC"
                " LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        traces = []
        for row in rows:
            item = dict(row)
            item["service_names"] = json.loads(item.get("service_names") or "[]")
            traces.append(item)
        return traces, int(total)

    def count(self) -> int:
        with self._lock:
            return int(
                self._conn.execute("SELECT COUNT(*) FROM archived_traces").fetchone()[0]
            )

    @staticmethod
    def _span_from_row(row: sqlite3.Row) -> dict[str, Any]:
        span = dict(row)
        for key in ("attributes_json", "resource_json", "scope_json", "raw_json"):
            try:
                span[key[:-5]] = json.loads(span.pop(key) or "{}")
            except json.JSONDecodeError:
                span[key[:-5]] = {}
        span["is_error"] = span.get("status_code") == "ERROR"
        return span
