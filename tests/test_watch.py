from __future__ import annotations

import os
import sys
import uuid

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.models import SpanRecord  # noqa: E402
from app.sampler import TraceSampler  # noqa: E402
from app.storage import (  # noqa: E402
    WATCH_AWAITING_WRITE,
    WATCH_COLLECTING,
    WATCH_DROPPED,
    WATCH_INTERRUPTED,
    WATCH_SUCCESS,
    WATCH_TIMEOUT,
    WATCH_WAITING,
    ArchiveStore,
)
from app.watch import WatchConflict, WatchError, WatchManager  # noqa: E402


class FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FailingStore(ArchiveStore):
    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.fail = True
        self.attempts = 0

    def write_trace(self, trace_id, spans):
        self.attempts += 1
        if self.fail:
            raise RuntimeError("disk full")
        return super().write_trace(trace_id, spans)

    def write_trace_and_finish_watch(self, trace_id, spans, order_id, span_count):
        self.attempts += 1
        if self.fail:
            raise RuntimeError("disk full")
        return super().write_trace_and_finish_watch(
            trace_id, spans, order_id, span_count
        )


@pytest.fixture()
def clock():
    return FakeClock()


@pytest.fixture()
def store(tmp_path):
    s = ArchiveStore(str(tmp_path / "traces.db"))
    yield s
    s.close()


def make_sampler(store, clock, watch=None, **kwargs):
    kwargs.setdefault("window_seconds", 2.0)
    kwargs.setdefault("capacity", 16)
    kwargs.setdefault("max_spans_per_trace", 64)
    return TraceSampler(store, clock=clock, watch=watch, **kwargs)


def trace_id() -> str:
    return uuid.uuid4().hex


def span(tid: str, name: str = "op", error: bool = False, sid: str | None = None):
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


# --------------------------------------------------------------- manager tests


def test_create_order_validates_trace_id(store, clock):
    watch = WatchManager(store, clock=clock)
    with pytest.raises(WatchError):
        watch.create_watch("not-hex")
    with pytest.raises(WatchError):
        watch.create_watch("z" * 32)
    order = watch.create_watch("a" * 32)
    assert order.order_id == "WO-000001"
    assert order.state == WATCH_WAITING
    assert store.get_watch_order("WO-000001")["state"] == WATCH_WAITING


def test_only_one_order_in_progress(store, clock):
    watch = WatchManager(store, clock=clock)
    watch.create_watch("a" * 32)
    with pytest.raises(WatchConflict):
        watch.create_watch("b" * 32)


def test_order_times_out_after_30_seconds_waiting(store, clock):
    watch = WatchManager(store, timeout_seconds=30.0, clock=clock)
    order = watch.create_watch("a" * 32)
    clock.advance(29.9)
    watch.tick()
    assert watch.get(order.order_id)["state"] == WATCH_WAITING
    clock.advance(0.1)
    watch.tick()
    assert watch.get(order.order_id)["state"] == WATCH_TIMEOUT
    # Terminal order frees the single in-progress slot.
    second = watch.create_watch("b" * 32)
    assert second.order_id == "WO-000002"


def test_capacity_rejections_counted_only_while_waiting(store, clock):
    watch = WatchManager(store, clock=clock)
    order = watch.create_watch("a" * 32)
    watch.on_capacity_reject(order.trace_id)
    watch.on_capacity_reject(order.trace_id)
    watch.tick()
    assert watch.get(order.order_id)["capacityRejected"] == 2

    watch.on_trace_admitted(order.trace_id)
    watch.on_capacity_reject(order.trace_id)  # ignored once a window is bound
    watch.tick()
    loaded = watch.get(order.order_id)
    assert loaded["state"] == WATCH_COLLECTING
    assert loaded["capacityRejected"] == 2


def test_only_first_window_after_creation_is_tracked(store, clock):
    watch = WatchManager(store, clock=clock)
    tid = "a" * 32
    # A window already open at creation time must not bind the order.
    watch.on_trace_admitted(tid)
    order = watch.create_watch(tid)
    assert order.state == WATCH_WAITING
    assert watch.get(order.order_id)["state"] == WATCH_WAITING

    # The next window for the same id is the one being tracked.
    watch.on_trace_admitted(tid)
    watch.tick()
    assert watch.get(order.order_id)["state"] == WATCH_COLLECTING
    # Further admission events for the id cannot rebind it.
    watch.on_trace_admitted(tid)
    assert watch.get(order.order_id)["state"] == WATCH_COLLECTING


def test_restart_marks_active_orders_interrupted_and_keeps_finished(store, clock):
    watch = WatchManager(store, clock=clock)
    waiting = watch.create_watch("a" * 32)
    watch.on_trace_admitted(waiting.trace_id)
    watch.tick()

    watch2 = WatchManager(store, clock=clock)
    interrupted = watch2.recover_interrupted()
    assert interrupted == [waiting.order_id]
    row = store.get_watch_order(waiting.order_id)
    assert row["state"] == WATCH_INTERRUPTED
    assert watch2.active_order() is None
    # Recovery is idempotent.
    assert watch2.recover_interrupted() == []


# ------------------------------------------------------------- sampler wiring


def test_full_success_flow_archive_and_order_commit_together(store, clock):
    watch = WatchManager(store, clock=clock)
    sampler = make_sampler(store, clock, watch=watch)
    tid = trace_id()
    order = watch.create_watch(tid)

    sampler.ingest([span(tid, "root"), span(tid, "pay", error=True)])
    watch.tick({tid: 2})
    assert watch.get(order.order_id)["state"] == WATCH_COLLECTING
    assert watch.get(order.order_id)["spanCount"] == 2

    clock.advance(2.0)
    sampler.tick()  # window closes with an ERROR -> awaiting write, then archived
    loaded = watch.get(order.order_id)
    assert loaded["state"] == WATCH_SUCCESS
    assert loaded["spanCount"] == 2
    assert loaded["archivedTraceId"] == tid
    assert store.get_trace(tid) is not None
    # A new order is now allowed.
    assert watch.create_watch(trace_id()).order_id == "WO-000002"


def test_no_error_window_closes_order_as_dropped(store, clock):
    watch = WatchManager(store, clock=clock)
    sampler = make_sampler(store, clock, watch=watch)
    tid = trace_id()
    order = watch.create_watch(tid)
    sampler.ingest([span(tid, "root", sid="1" * 16)])
    clock.advance(2.0)
    sampler.tick()
    loaded = watch.get(order.order_id)
    assert loaded["state"] == WATCH_DROPPED
    assert loaded["spanCount"] == 1
    assert store.count() == 0


def test_dropped_order_persists_while_archiving_paused(store, clock):
    watch = WatchManager(store, clock=clock)
    sampler = make_sampler(store, clock, watch=watch)
    sampler.pause()
    tid = trace_id()
    order = watch.create_watch(tid)
    sampler.ingest([span(tid)])
    clock.advance(2.0)
    sampler.tick()  # discard is decided in memory even while paused
    assert store.get_watch_order(order.order_id)["state"] == WATCH_DROPPED


def test_timeout_when_window_never_opens(store, clock):
    watch = WatchManager(store, timeout_seconds=30.0, clock=clock)
    sampler = make_sampler(store, clock, watch=watch)
    tid = trace_id()
    order = watch.create_watch(tid)
    clock.advance(30.0)
    sampler.tick()
    assert watch.get(order.order_id)["state"] == WATCH_TIMEOUT
    assert store.count() == 0


def test_capacity_rejected_spans_reported_while_waiting(store, clock):
    watch = WatchManager(store, clock=clock)
    sampler = make_sampler(store, clock, watch=watch, capacity=1)
    # Occupy the only slot with an unrelated trace.
    other = trace_id()
    sampler.ingest([span(other)])

    tid = trace_id()
    order = watch.create_watch(tid)
    result = sampler.ingest([span(tid), span(tid, sid="b" * 16)])
    assert result.reasons == {"capacity": 2}
    watch.tick()
    assert watch.get(order.order_id)["state"] == WATCH_WAITING
    assert watch.get(order.order_id)["capacityRejected"] == 2

    # Once the other window leaves, the watched id enters a fresh window.
    clock.advance(2.0)
    sampler.tick()
    sampler.ingest([span(tid, error=True)])
    watch.tick({tid: 1})
    assert watch.get(order.order_id)["state"] == WATCH_COLLECTING
    clock.advance(2.0)
    sampler.tick()
    assert watch.get(order.order_id)["state"] == WATCH_SUCCESS


def test_failed_archive_keeps_order_awaiting_write_then_succeeds(store, clock):
    failing = FailingStore(store.path)
    try:
        watch = WatchManager(failing, clock=clock)
        sampler = make_sampler(failing, clock, watch=watch)
        tid = trace_id()
        order = watch.create_watch(tid)
        sampler.ingest([span(tid, error=True)])
        clock.advance(2.0)
        sampler.tick()
        loaded = watch.get(order.order_id)
        assert loaded["state"] == WATCH_AWAITING_WRITE
        assert loaded["writeAttempts"] == 1
        assert "disk full" in (loaded["lastWriteError"] or "")
        assert failing.get_trace(tid) is None

        failing.fail = False
        clock.advance(60)  # past backoff
        sampler.tick()
        loaded = watch.get(order.order_id)
        assert loaded["state"] == WATCH_SUCCESS
        assert failing.get_trace(tid) is not None
    finally:
        failing.close()


def test_order_success_immune_to_later_rearchive_of_same_trace_id(store, clock):
    watch = WatchManager(store, clock=clock)
    sampler = make_sampler(store, clock, watch=watch)
    tid = trace_id()
    order = watch.create_watch(tid)
    sampler.ingest([span(tid, "first", error=True, sid="1" * 16)])
    clock.advance(2.0)
    sampler.tick()
    assert watch.get(order.order_id)["state"] == WATCH_SUCCESS
    assert watch.get(order.order_id)["spanCount"] == 1

    # A later lifecycle of the same trace id re-archives a different window;
    # the finished order must keep its original conclusion.
    clock.advance(400.0)  # beyond closed-trace retention horizon
    sampler2 = make_sampler(store, clock, watch=watch)
    sampler2.ingest([span(tid, "second", error=True, sid="2" * 16)])
    clock.advance(2.0)
    sampler2.tick()
    loaded = watch.get(order.order_id)
    assert loaded["state"] == WATCH_SUCCESS
    assert loaded["spanCount"] == 1
    assert loaded["archivedTraceId"] == tid


def test_watch_does_not_change_ingest_rules(store, clock):
    watch = WatchManager(store, clock=clock)
    sampler = make_sampler(store, clock, watch=watch, capacity=1)
    other = trace_id()
    sampler.ingest([span(other)])
    order = watch.create_watch(trace_id())
    # Capacity refusal still happens exactly as without a watch.
    refused = sampler.ingest([span(order.trace_id)])
    assert refused.reasons == {"capacity": 1}
    # The unrelated trace still collects normally.
    assert sampler.ingest([span(other, "child")]).accepted == 1


def test_order_without_watch_manager_keeps_sampler_working(store, clock):
    sampler = make_sampler(store, clock, watch=None)
    tid = trace_id()
    sampler.ingest([span(tid, error=True)])
    clock.advance(2.0)
    sampler.tick()
    assert store.count() == 1


def test_paused_pending_order_resumes_to_success(store, clock):
    watch = WatchManager(store, clock=clock)
    sampler = make_sampler(store, clock, watch=watch)
    sampler.pause()
    tid = trace_id()
    order = watch.create_watch(tid)
    sampler.ingest([span(tid, error=True)])
    clock.advance(2.0)
    sampler.tick()  # window expires while archiving is paused
    assert watch.get(order.order_id)["state"] == WATCH_AWAITING_WRITE
    assert store.count() == 0

    sampler.resume()
    sampler.tick()
    loaded = watch.get(order.order_id)
    assert loaded["state"] == WATCH_SUCCESS
    assert store.get_trace(tid) is not None


def test_span_count_counts_distinct_spans_not_rejections(store, clock):
    watch = WatchManager(store, clock=clock)
    sampler = make_sampler(store, clock, watch=watch, max_spans_per_trace=64)
    tid = trace_id()
    order = watch.create_watch(tid)
    fixed_sid = "1" * 16
    # One accepted span, a duplicate and a span-limit rejection must not
    # inflate the order's distinct-span count.
    sampler.ingest([span(tid, sid=fixed_sid)])
    sampler.ingest([span(tid, name="dup", sid=fixed_sid)])  # duplicate
    watch.tick({tid: 1})
    assert watch.get(order.order_id)["spanCount"] == 1

    sampler.ingest([span(tid, f"s{i}", sid=f"{i:016x}") for i in range(70)])
    watch.tick({tid: 64})
    loaded = watch.get(order.order_id)
    assert loaded["state"] == WATCH_COLLECTING
    assert loaded["spanCount"] == 64


def test_collecting_order_does_not_time_out(store, clock):
    watch = WatchManager(store, timeout_seconds=30.0, clock=clock)
    sampler = make_sampler(store, clock, watch=watch)
    tid = trace_id()
    order = watch.create_watch(tid)
    sampler.ingest([span(tid)])  # enters a window immediately
    clock.advance(31.0)  # past the wait timeout, but a window is open
    watch.tick({tid: 1})
    assert watch.get(order.order_id)["state"] == WATCH_COLLECTING
    clock.advance(2.0)
    sampler.tick()
    assert watch.get(order.order_id)["state"] == WATCH_DROPPED


def test_order_ids_continue_after_terminal_orders(store, clock):
    watch = WatchManager(store, clock=clock)
    first = watch.create_watch("a" * 32)
    assert first.order_id == "WO-000001"
    clock.advance(30.0)
    watch.tick()  # TIMEOUT -> terminal
    second = watch.create_watch("b" * 32)
    assert second.order_id == "WO-000002"
    # A third order is still refused while the second one is active.
    with pytest.raises(WatchConflict):
        watch.create_watch("c" * 32)


def test_recover_then_new_order_uses_next_sequence(store, clock):
    watch = WatchManager(store, clock=clock)
    watch.create_watch("a" * 32)
    watch2 = WatchManager(store, clock=clock)
    watch2.recover_interrupted()
    order = watch2.create_watch("b" * 32)
    assert order.order_id == "WO-000002"
