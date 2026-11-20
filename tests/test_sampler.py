from __future__ import annotations

import json
import os
import sys
import uuid

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.sampler import TraceSampler  # noqa: E402
from app.storage import ArchiveStore  # noqa: E402


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FailingStore(ArchiveStore):
    """ArchiveStore whose writes fail while ``fail`` is True."""

    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.fail = True
        self.attempts = 0

    def write_trace(self, trace_id, spans):  # noqa: D401
        self.attempts += 1
        if self.fail:
            raise RuntimeError("disk full")
        return super().write_trace(trace_id, spans)


@pytest.fixture()
def clock():
    return FakeClock()


@pytest.fixture()
def store(tmp_path):
    store = ArchiveStore(str(tmp_path / "traces.db"))
    yield store
    store.close()


def make_sampler(store, clock, **kwargs):
    kwargs.setdefault("window_seconds", 2.0)
    kwargs.setdefault("capacity", 16)
    kwargs.setdefault("max_spans_per_trace", 64)
    return TraceSampler(store, clock=clock, **kwargs)


def trace_id() -> str:
    return uuid.uuid4().hex


def span(tid: str, name: str = "op", error: bool = False, sid: str | None = None):
    from app.models import SpanRecord

    return SpanRecord(
        trace_id=tid,
        span_id=sid or uuid.uuid4().hex[:16],
        parent_span_id=None,
        name=name,
        kind="SERVER",
        status_code="ERROR" if error else "UNSET",
        status_message="boom" if error else "",
        is_error=error,
        start_time_ns="1700000000000000000",
        end_time_ns="1700000000500000000",
        duration_ns=500_000_000,
        service_name="svc",
        event_count=0,
        link_count=0,
        resource_json="{}",
        scope_json="{}",
        attributes_json="{}",
        raw_json="{}",
    )


def test_discards_trace_without_error_at_window_end(store, clock):
    sampler = make_sampler(store, clock)
    tid = trace_id()
    sampler.ingest([span(tid)])
    clock.advance(2.0)
    sampler.tick()
    assert store.count() == 0
    assert sampler.snapshot()["totals"]["traces_discarded"] == 1
    # Slot freed immediately.
    assert sampler.snapshot()["slotsUsed"] == 0
    # Late span for the discarded trace is refused.
    result = sampler.ingest([span(tid)])
    assert result.rejected == 1
    assert result.reasons == {"window_closed": 1}


def test_archives_trace_with_error_at_window_end(store, clock):
    sampler = make_sampler(store, clock)
    tid = trace_id()
    sampler.ingest([span(tid, "root"), span(tid, "pay", error=True)])
    assert store.count() == 0  # not before the window closes
    clock.advance(2.0)
    sampler.tick()
    assert store.count() == 1
    archived = store.get_trace(tid)
    assert archived is not None
    assert archived["span_count"] == 2
    assert archived["error_count"] == 1


def test_spans_are_deduped_by_span_id(store, clock):
    sampler = make_sampler(store, clock)
    tid = trace_id()
    sid = "a" * 16
    sampler.ingest([span(tid, "one", sid=sid)])
    result = sampler.ingest([span(tid, "one-again", sid=sid)])
    assert result.duplicates == 1
    assert result.accepted == 0
    clock.advance(2.0)
    sampler.tick()
    assert store.count() == 0


def test_span_cap_is_64_distinct_spans(store, clock):
    sampler = make_sampler(store, clock)
    tid = trace_id()
    batch = [span(tid, f"s{i}") for i in range(70)]
    result = sampler.ingest(batch)
    assert result.accepted == 64
    assert result.rejected == 6
    assert result.reasons == {"span_limit": 6}
    clock.advance(2.0)
    sampler.tick()
    assert store.count() == 0
    assert sampler.snapshot()["totals"]["traces_discarded"] == 1


def test_span_cap_respected_in_archive(store, clock):
    sampler = make_sampler(store, clock)
    tid = trace_id()
    sampler.ingest([span(tid, "err", error=True)] + [span(tid, f"s{i}") for i in range(70)])
    clock.advance(2.0)
    sampler.tick()
    assert store.get_trace(tid)["span_count"] == 64


def test_window_boundary_rejects_span_after_two_seconds(store, clock):
    sampler = make_sampler(store, clock)
    tid = trace_id()
    sampler.ingest([span(tid, "root")])
    clock.advance(1.9)
    assert sampler.ingest([span(tid, "late-but-ok")]).accepted == 1
    clock.advance(0.1)  # exactly 2.0s since first span
    result = sampler.ingest([span(tid, "too-late", error=True)])
    assert result.rejected == 1
    assert result.reasons == {"window_closed": 1}
    sampler.tick()
    # The error span arrived too late, so the trace is discarded.
    assert store.count() == 0


def test_capacity_rejects_only_new_traces(store, clock):
    sampler = make_sampler(store, clock, capacity=2)
    a, b, c = trace_id(), trace_id(), trace_id()
    assert sampler.ingest([span(a)]).accepted == 1
    assert sampler.ingest([span(b)]).accepted == 1
    blocked = sampler.ingest([span(c)])
    assert blocked.rejected == 1
    assert blocked.reasons == {"capacity": 1}

    # Admitted traces can still collect while at capacity.
    assert sampler.ingest([span(a, "a2", error=True)]).accepted == 1
    assert sampler.ingest([span(b, "b2")]).accepted == 1

    clock.advance(2.0)
    sampler.tick()  # a archived, b discarded; both slots released
    assert sampler.snapshot()["slotsUsed"] == 0
    assert sampler.ingest([span(c)]).accepted == 1


def test_slot_held_until_commit(store, clock):
    failing = FailingStore(store.path)
    try:
        sampler = make_sampler(failing, clock, capacity=1)
        a, b = trace_id(), trace_id()
        sampler.ingest([span(a, error=True)])
        clock.advance(2.0)
        sampler.tick()
        assert store.count() == 0
        assert failing.attempts == 1
        # Write failed, so the slot stays taken and new traces are refused.
        assert sampler.snapshot()["slotsUsed"] == 1
        assert sampler.ingest([span(b)]).reasons == {"capacity": 1}
        assert len(sampler.snapshot()["pendingTraces"]) == 1

        failing.fail = False
        clock.advance(60)  # past the retry backoff
        sampler.tick()
        assert store.count() == 1
        assert sampler.snapshot()["slotsUsed"] == 0
        assert sampler.ingest([span(b)]).accepted == 1
    finally:
        failing.close()


def test_pause_holds_expired_content_and_resume_flushes(store, clock):
    sampler = make_sampler(store, clock)
    tid = trace_id()
    sampler.ingest([span(tid, "root"), span(tid, "pay", error=True)])
    sampler.pause()
    clock.advance(2.0)
    sampler.tick()
    assert store.count() == 0
    pending = sampler.snapshot()["pendingTraces"]
    assert len(pending) == 1 and pending[0]["spanCount"] == 2

    # Spans arriving after expiry are still refused while paused.
    assert sampler.ingest([span(tid, "late")]).reasons == {"window_closed": 1}

    sampler.resume()
    sampler.tick()
    assert store.count() == 1
    archived = store.get_trace(tid)
    assert {s["name"] for s in archived["spans"]} == {"root", "pay"}


def test_pause_preserves_expiry_snapshot_not_later_spans(store, clock):
    sampler = make_sampler(store, clock)
    tid = trace_id()
    sampler.ingest([span(tid, "root", error=True)])
    sampler.pause()
    clock.advance(2.0)
    sampler.tick()  # expires while paused
    sampler.ingest([span(tid, "after")])  # rejected, must not enter archive
    sampler.resume()
    sampler.tick()
    assert [s["name"] for s in store.get_trace(tid)["spans"]] == ["root"]


def test_mixed_batch_keeps_compliant_spans(store, clock):
    sampler = make_sampler(store, clock, capacity=1)
    a, b = trace_id(), trace_id()
    sampler.ingest([span(a, "a0")])
    # b cannot be admitted, a2 is fine, a duplicate is counted separately.
    result = sampler.ingest(
        [span(b, "b0"), span(a, "a1", error=True), span(a, "dup", sid="*" * 1 + "0" * 15)]
    )
    assert result.rejected == 1
    assert result.accepted == 2
    surface = sampler.ingest([span(a, "x", sid=span(a).span_id)])
    assert surface.accepted == 1


def test_archive_is_complete_and_queryable(store, clock):
    sampler = make_sampler(store, clock)
    tid = trace_id()
    sid_root = "1" * 16
    sid_child = "2" * 16
    sampler.ingest([span(tid, "root", sid=sid_root), span(tid, "child", sid=sid_child)])
    clock.advance(2.0)
    sampler.tick()
    assert store.count() == 0

    sampler2 = make_sampler(store, clock)
    tid2 = trace_id()
    sampler2.ingest([span(tid2, "root", sid=sid_root), span(tid2, "child", sid=sid_child, error=True)])
    clock.advance(2.0)
    sampler2.tick()
    archived = store.get_trace(tid2)
    assert [s["name"] for s in archived["spans"]] == ["root", "child"]
    stored_ids = [s["span_id"] for s in archived["spans"]]
    assert stored_ids == [sid_root, sid_child]
    traces, total = store.list_traces()
    assert total == 1 and traces[0]["trace_id"] == tid2


def test_visibility_only_after_complete_write(store, clock):
    """A trace is invisible on the overview until its commit has finished."""

    class SlowStore(ArchiveStore):
        def write_trace(self, trace_id, spans):
            raise RuntimeError("locked")

    slow = SlowStore(store.path)
    try:
        sampler = make_sampler(slow, clock)
        tid = trace_id()
        sampler.ingest([span(tid, error=True)])
        clock.advance(2.0)
        sampler.tick()
        traces, total = store.list_traces()
        assert total == 0
        assert store.get_trace(tid) is None
        assert sampler.snapshot()["pendingTraces"][0]["traceId"] == tid
    finally:
        slow.close()


def test_retry_backoff_grows_and_caps(store, clock):
    failing = FailingStore(store.path)
    try:
        sampler = make_sampler(failing, clock)
        tid = trace_id()
        sampler.ingest([span(tid, error=True)])
        clock.advance(2.0)
        sampler.tick()
        assert failing.attempts == 1
        clock.advance(0.4)
        sampler.tick()  # backoff not elapsed yet
        assert failing.attempts == 1
        clock.advance(0.2)
        sampler.tick()
        assert failing.attempts == 2
        clock.advance(0.9)  # 3.5, second backoff (1s) not yet elapsed
        sampler.tick()
        assert failing.attempts == 2
        clock.advance(0.1)  # 3.6
        sampler.tick()
        assert failing.attempts == 3
        assert sampler.snapshot()["pendingTraces"][0]["attempts"] == 3
        assert "disk full" in sampler.snapshot()["pendingTraces"][0]["lastError"]
    finally:
        failing.close()


def test_repeated_tick_does_not_double_archive(store, clock):
    sampler = make_sampler(store, clock)
    tid = trace_id()
    sampler.ingest([span(tid, "only", error=True)])
    clock.advance(2.0)
    sampler.tick()
    sampler.tick()
    sampler.tick()
    assert store.count() == 1
    assert sampler.snapshot()["totals"]["traces_archived"] == 1


def test_closed_trace_memory_expires(store, clock, monkeypatch):
    from app import sampler as sampler_mod

    monkeypatch.setattr(sampler_mod, "CLOSED_RETENTION_SECONDS", 10.0)
    sampler = make_sampler(store, clock)
    old = trace_id()
    sampler.ingest([span(old)])
    clock.advance(2.0)
    sampler.tick()
    assert sampler.ingest([span(old)]).reasons == {"window_closed": 1}
    clock.advance(11.0)
    # After the retention horizon the id is forgotten; a reused id can open a
    # fresh window (OTLP trace ids are expected to be unique).
    assert sampler.ingest([span(old)]).accepted == 1
