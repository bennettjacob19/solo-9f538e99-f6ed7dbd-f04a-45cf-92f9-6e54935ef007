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

CREATE TABLE IF NOT EXISTS watch_orders (
    order_id              TEXT PRIMARY KEY,
    trace_id              TEXT NOT NULL,
    state                 TEXT NOT NULL,
    created_at_ns         INTEGER NOT NULL,
    updated_at_ns         INTEGER NOT NULL,
    window_opened_at_ns   INTEGER,
    closed_at_ns          INTEGER,
    timeout_seconds       REAL NOT NULL,
    capacity_rejected     INTEGER NOT NULL DEFAULT 0,
    span_count            INTEGER NOT NULL DEFAULT 0,
    write_attempts        INTEGER NOT NULL DEFAULT 0,
    last_write_error      TEXT,
    archived_trace_id     TEXT
);

CREATE INDEX IF NOT EXISTS idx_watch_orders_trace
    ON watch_orders (trace_id);
CREATE INDEX IF NOT EXISTS idx_watch_orders_state
    ON watch_orders (state);
CREATE INDEX IF NOT EXISTS idx_watch_orders_created
    ON watch_orders (created_at_ns DESC);
"""

# Lifecycle states of a watch order.  Only the first three are "in progress";
# the rest are terminal and never transition again.
WATCH_WAITING = "WAITING"
WATCH_COLLECTING = "COLLECTING"
WATCH_AWAITING_WRITE = "AWAITING_WRITE"
WATCH_SUCCESS = "SUCCESS"
WATCH_DROPPED = "DROPPED"
WATCH_TIMEOUT = "TIMEOUT"
WATCH_INTERRUPTED = "INTERRUPTED"
WATCH_ACTIVE_STATES = (WATCH_WAITING, WATCH_COLLECTING, WATCH_AWAITING_WRITE)
WATCH_TERMINAL_STATES = (
    WATCH_SUCCESS,
    WATCH_DROPPED,
    WATCH_TIMEOUT,
    WATCH_INTERRUPTED,
)


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
        with self._lock:
            self._insert_trace_locked(trace_id, spans)

    def write_trace_and_finish_watch(
        self,
        trace_id: str,
        spans: list[SpanRecord],
        order_id: str,
        span_count: int,
    ) -> None:
        """Archive a trace and close its watch order in one transaction.

        The trace rows and the order's SUCCESS conclusion commit together: if
        either write fails the whole transaction rolls back, so an order can
        never report SUCCESS without its archive, nor survive an archive whose
        own write did not land.
        """

        if not spans:
            raise ValueError("refusing to archive an empty trace")
        with self._lock:
            conn = self._conn
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._insert_trace_rows_locked(trace_id, spans)
                updated = conn.execute(
                    "UPDATE watch_orders SET state = ?, updated_at_ns = ?,"
                    " span_count = ?, closed_at_ns = COALESCE(closed_at_ns, ?),"
                    " archived_trace_id = ?"
                    " WHERE order_id = ? AND state IN (?, ?, ?)",
                    (
                        "SUCCESS",
                        time.time_ns(),
                        int(span_count),
                        time.time_ns(),
                        trace_id,
                        order_id,
                        "WAITING",
                        "COLLECTING",
                        "AWAITING_WRITE",
                    ),
                ).rowcount
                if updated != 1:
                    # The order vanished or already reached a terminal state;
                    # the combined conclusion must not half-commit.
                    raise RuntimeError(
                        f"watch order {order_id} cannot be finished (updated={updated})"
                    )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    def _insert_trace_locked(self, trace_id: str, spans: list[SpanRecord]) -> None:
        """Insert a trace, assuming the caller already holds the store lock."""

        conn = self._conn
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._insert_trace_rows_locked(trace_id, spans)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise

    def _insert_trace_rows_locked(
        self, trace_id: str, spans: list[SpanRecord]
    ) -> None:
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

        conn = self._conn
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

    # ------------------------------------------------------------- watch orders
    def create_watch_order(self, order: dict[str, Any]) -> None:
        with self._lock:
            conn = self._conn
            try:
                conn.execute("BEGIN IMMEDIATE")
                conn.execute(
                    "INSERT INTO watch_orders ("
                    " order_id, trace_id, state, created_at_ns, updated_at_ns,"
                    " window_opened_at_ns, closed_at_ns, timeout_seconds,"
                    " capacity_rejected, span_count, write_attempts,"
                    " last_write_error, archived_trace_id"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        order["order_id"],
                        order["trace_id"],
                        order["state"],
                        order["created_at_ns"],
                        order["updated_at_ns"],
                        order.get("window_opened_at_ns"),
                        order.get("closed_at_ns"),
                        order["timeout_seconds"],
                        order.get("capacity_rejected", 0),
                        order.get("span_count", 0),
                        order.get("write_attempts", 0),
                        order.get("last_write_error"),
                        order.get("archived_trace_id"),
                    ),
                )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    def next_watch_seq(self) -> int:
        """One more than the largest numeric suffix used so far."""

        with self._lock:
            rows = self._conn.execute(
                "SELECT order_id FROM watch_orders"
            ).fetchall()
        highest = 0
        for (text,) in rows:
            if text and text.startswith("WO-") and text[3:].isdigit():
                highest = max(highest, int(text[3:]))
        return highest + 1

    def list_watch_orders(
        self, limit: int = 50, offset: int = 0
    ) -> tuple[list[dict[str, Any]], int]:
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        with self._lock:
            total = int(
                self._conn.execute("SELECT COUNT(*) FROM watch_orders").fetchone()[0]
            )
            rows = self._conn.execute(
                "SELECT * FROM watch_orders ORDER BY created_at_ns DESC, rowid DESC"
                " LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [dict(row) for row in rows], total

    def get_watch_order(self, order_id: str) -> Optional[dict[str, Any]]:
        order_id = order_id.strip()
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM watch_orders WHERE order_id = ?", (order_id,)
            ).fetchone()
        return dict(row) if row is not None else None

    def get_active_watch_order(self) -> Optional[dict[str, Any]]:
        placeholders = ",".join("?" for _ in WATCH_ACTIVE_STATES)
        with self._lock:
            row = self._conn.execute(
                f"SELECT * FROM watch_orders WHERE state IN ({placeholders})"
                " ORDER BY created_at_ns DESC, rowid DESC LIMIT 1",
                WATCH_ACTIVE_STATES,
            ).fetchone()
        return dict(row) if row is not None else None

    def mark_interrupted_watch_orders(self) -> list[str]:
        """Close every unfinished order after an unclean restart."""

        now = time.time_ns()
        with self._lock:
            conn = self._conn
            try:
                conn.execute("BEGIN IMMEDIATE")
                placeholders = ",".join("?" for _ in WATCH_ACTIVE_STATES)
                rows = conn.execute(
                    f"SELECT order_id FROM watch_orders WHERE state IN ({placeholders})",
                    WATCH_ACTIVE_STATES,
                ).fetchall()
                conn.execute(
                    f"UPDATE watch_orders SET state = ?, updated_at_ns = ?,"
                    f" closed_at_ns = COALESCE(closed_at_ns, ?)"
                    f" WHERE state IN ({placeholders})",
                    (WATCH_INTERRUPTED, now, now, *WATCH_ACTIVE_STATES),
                )
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
        return [row[0] for row in rows]

    def update_watch_order(
        self, order_id: str, fields: dict[str, Any], only_active: bool = True
    ) -> bool:
        """Persist progress columns of a non-terminal order.

        With ``only_active`` the write is skipped once the order has reached a
        terminal state, so stale in-memory progress can never overwrite a
        conclusion (e.g. a later re-archive of the same trace id).
        """

        allowed = {
            "state",
            "updated_at_ns",
            "window_opened_at_ns",
            "closed_at_ns",
            "capacity_rejected",
            "span_count",
            "write_attempts",
            "last_write_error",
            "archived_trace_id",
        }
        fields = {key: value for key, value in fields.items() if key in allowed}
        if not fields:
            return False
        columns = ", ".join(f"{key} = ?" for key in fields)
        params = list(fields.values())
        with self._lock:
            conn = self._conn
            try:
                conn.execute("BEGIN IMMEDIATE")
                sql = f"UPDATE watch_orders SET {columns} WHERE order_id = ?"
                params.append(order_id)
                if only_active:
                    placeholders = ",".join("?" for _ in WATCH_ACTIVE_STATES)
                    sql += f" AND state IN ({placeholders})"
                    params.extend(WATCH_ACTIVE_STATES)
                changed = conn.execute(sql, params).rowcount
                conn.commit()
            except BaseException:
                conn.rollback()
                raise
        return bool(changed)

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
