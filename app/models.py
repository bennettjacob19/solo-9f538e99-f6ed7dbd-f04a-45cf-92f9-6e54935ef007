"""Small value objects shared by the OTLP parser, sampler and archive store."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class SpanRecord:
    """A validated OTLP span, ready to be put into an in-memory trace."""

    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    name: str
    kind: str
    status_code: str
    status_message: str
    is_error: bool
    start_time_ns: Optional[str]
    end_time_ns: Optional[str]
    duration_ns: Optional[int]
    service_name: Optional[str]
    event_count: int
    link_count: int
    resource_json: str
    scope_json: str
    attributes_json: str
    raw_json: str
    seq: int = 0


@dataclass
class ActiveTrace:
    """A trace whose two second collection window is still open."""

    trace_id: str
    deadline: float
    first_seen: float
    spans: dict[str, SpanRecord] = field(default_factory=dict)
    has_error: bool = False
    next_seq: int = 0


@dataclass
class PendingArchive:
    """A complete trace retained because archiving is paused or failed."""

    trace_id: str
    spans: list[SpanRecord]
    expired_at: float
    attempts: int = 0
    last_error: Optional[str] = None


@dataclass
class IngestResult:
    accepted: int = 0
    rejected: int = 0
    duplicates: int = 0
    accepted_new: int = 0
    reasons: dict[str, int] = field(default_factory=dict)

    def reject(self, reason: str) -> None:
        self.rejected += 1
        self.reasons[reason] = self.reasons.get(reason, 0) + 1
