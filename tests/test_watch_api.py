from __future__ import annotations

import json
import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import create_app  # noqa: E402
from app.storage import ArchiveStore  # noqa: E402


def tid(n: int) -> str:
    return f"{n:032x}"


def sid(n: int) -> str:
    return f"{n:016x}"


def otlp_payload(spans) -> dict:
    groups: dict[tuple, list] = {}
    for item in spans:
        trace_id = item["trace"]
        key = (item.get("service", "checkout"), item.get("scope", "test"))
        span = {
            "traceId": trace_id,
            "spanId": item["span"],
            "name": item.get("name", "op"),
            "kind": item.get("kind", 2),
            "startTimeUnixNano": str(item.get("start", 1_700_000_000_000_000_000)),
            "endTimeUnixNano": str(item.get("end", 1_700_000_000_500_000_000)),
            "status": item.get(
                "status",
                {"code": 2, "message": "payment declined"}
                if item.get("error")
                else {"code": 1},
            ),
        }
        if item.get("parent"):
            span["parentSpanId"] = item["parent"]
        groups.setdefault(key, []).append(span)
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": service}}
                    ]
                },
                "scopeSpans": [{"scope": {"name": scope}, "spans": group}],
            }
            for (service, scope), group in groups.items()
        ]
    }


class Clock:
    def __init__(self) -> None:
        self.now = 5000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture()
def clock():
    return Clock()


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "traces.db")


@pytest.fixture()
def client(db_path, clock):
    app = create_app(
        db_path,
        window_seconds=2.0,
        capacity=16,
        max_spans_per_trace=64,
        watch_timeout_seconds=30.0,
        start_sampler_thread=False,
        clock=clock,
    )
    with TestClient(app) as test_client:
        test_client.app_state = app.state
        yield test_client
    app.state.store.close()


def post_traces(client, spans):
    return client.post(
        "/v1/traces",
        content=json.dumps(otlp_payload(spans)),
        headers={"content-type": "application/json"},
    )


def test_create_watch_order_via_api_and_tracking(client, clock):
    trace = tid(11)
    response = client.post("/api/watch", json={"traceId": trace})
    assert response.status_code == 201
    order = response.json()
    assert order["orderId"] == "WO-000001"
    assert order["state"] == "WAITING"

    # The homepage shows the form and the in-progress order.
    page = client.get("/")
    assert "Open watch order" in page.text
    assert "WO-000001" in page.text

    # A second in-progress order is refused with 409.
    conflict = client.post("/api/watch", json={"traceId": tid(12)})
    assert conflict.status_code == 409

    bad = client.post("/api/watch", json={"traceId": "zzz"})
    assert bad.status_code == 400

    # The watched trace enters a window and collects two distinct spans.
    post_traces(
        client,
        [
            {"trace": trace, "span": sid(1), "name": "root"},
            {"trace": trace, "span": sid(2), "parent": sid(1), "name": "pay", "error": True},
        ],
    )
    detail = client.get("/api/watch/WO-000001").json()
    assert detail["state"] == "COLLECTING"
    assert detail["spanCount"] == 2

    clock.advance(2.0)
    client.app_state.sampler.tick()
    detail = client.get("/api/watch/WO-000001").json()
    assert detail["state"] == "SUCCESS"
    assert detail["spanCount"] == 2
    assert detail["archivedTraceId"] == trace

    # The order detail page renders and links the archived trace.
    page = client.get("/watch/WO-000001")
    assert page.status_code == 200
    assert "SUCCESS" in page.text
    assert f"/traces/{trace}" in page.text


def test_watch_order_dropped_when_no_error(client, clock):
    trace = tid(13)
    order = client.post("/api/watch", json={"traceId": trace}).json()
    post_traces(client, [{"trace": trace, "span": sid(1)}])
    clock.advance(2.0)
    client.app_state.sampler.tick()
    detail = client.get(f"/api/watch/{order['orderId']}").json()
    assert detail["state"] == "DROPPED"
    assert client.get(f"/api/traces/{trace}").status_code == 404


def test_watch_order_timeout_via_sampler_tick(client, clock):
    trace = tid(14)
    order = client.post("/api/watch", json={"traceId": trace}).json()
    clock.advance(30.0)
    client.app_state.sampler.tick()
    detail = client.get(f"/api/watch/{order['orderId']}").json()
    assert detail["state"] == "TIMEOUT"
    # Slot released: another order can be opened.
    second = client.post("/api/watch", json={"traceId": tid(15)})
    assert second.status_code == 201
    assert second.json()["orderId"] == "WO-000002"


def test_capacity_rejections_visible_on_order(client, clock):
    # Fill all 16 slots with unrelated traces.
    for i in range(16):
        post_traces(client, [{"trace": tid(100 + i), "span": sid(1)}])
    trace = tid(200)
    order = client.post("/api/watch", json={"traceId": trace}).json()
    post_traces(client, [{"trace": trace, "span": sid(1)}])
    detail = client.get(f"/api/watch/{order['orderId']}").json()
    assert detail["state"] == "WAITING"
    assert detail["capacityRejected"] == 1
    assert "1 span(s) refused" in client.get("/").text


def test_html_form_creates_order_and_redirects(client):
    trace = tid(21)
    response = client.post(
        "/watch",
        data={"traceId": trace},
        follow_redirects=False,
    )
    assert response.status_code == 303
    assert response.headers["location"] == "/watch/WO-000001"
    # Invalid id renders an error page instead of redirecting.
    bad = client.post("/watch", data={"traceId": "nope"})
    assert bad.status_code == 400
    assert "Cannot open watch order" in bad.text


def test_missing_order_returns_404_page_and_api(client):
    assert client.get("/watch/WO-999999").status_code == 404
    assert client.get("/api/watch/WO-999999").status_code == 404
    assert client.get("/api/watch/active").status_code == 404


def test_restart_marks_unfinished_order_interrupted(db_path, clock):
    app = create_app(
        db_path,
        watch_timeout_seconds=30.0,
        start_sampler_thread=False,
        clock=clock,
    )
    with TestClient(app) as first:
        order = first.post("/api/watch", json={"traceId": tid(31)}).json()
        assert order["state"] == "WAITING"
    app.state.store.close()

    # Simulate a process restart against the same SQLite file.
    clock.advance(5.0)
    app2 = create_app(
        db_path,
        watch_timeout_seconds=30.0,
        start_sampler_thread=False,
        clock=clock,
    )
    with TestClient(app2) as second:
        detail = second.get(f"/api/watch/{order['orderId']}").json()
        assert detail["state"] == "INTERRUPTED"
        # Finished orders remain queryable; a new order is now permitted.
        assert second.post("/api/watch", json={"traceId": tid(32)}).status_code == 201
    app2.state.store.close()


def test_finished_order_survives_restart(db_path, clock):
    app = create_app(
        db_path,
        watch_timeout_seconds=30.0,
        start_sampler_thread=False,
        clock=clock,
    )
    with TestClient(app) as first:
        trace = tid(41)
        order = first.post("/api/watch", json={"traceId": trace}).json()
        post_traces(first, [{"trace": trace, "span": sid(1), "error": True}])
        clock.advance(2.0)
        app.state.sampler.tick()
        assert first.get(f"/api/watch/{order['orderId']}").json()["state"] == "SUCCESS"
    app.state.store.close()

    app2 = create_app(
        db_path,
        watch_timeout_seconds=30.0,
        start_sampler_thread=False,
        clock=clock,
    )
    with TestClient(app2) as second:
        detail = second.get(f"/api/watch/{order['orderId']}").json()
        assert detail["state"] == "SUCCESS"
        assert detail["archivedTraceId"] == trace
        # Recovery did not touch it.
        assert second.get(f"/api/traces/{trace}").status_code == 200
    app2.state.store.close()


def test_archive_and_order_commit_are_atomic(client, clock, monkeypatch):
    """If the combined order UPDATE fails, the trace insert rolls back too."""

    store = client.app_state.store
    original = store.write_trace_and_finish_watch
    failures = {"n": 0}

    def broken_combined(trace_id, spans, order_id, span_count):
        failures["n"] += 1
        if failures["n"] == 1:
            raise RuntimeError("simulated combined-write failure")
        return original(trace_id, spans, order_id, span_count)

    monkeypatch.setattr(store, "write_trace_and_finish_watch", broken_combined)

    trace = tid(51)
    client.post("/api/watch", json={"traceId": trace})
    post_traces(client, [{"trace": trace, "span": sid(1), "error": True}])
    clock.advance(2.0)
    client.app_state.sampler.tick()  # combined write must fail and roll back

    # Nothing archived, order not SUCCESS, slot stays pending for retry.
    assert store.get_trace(trace) is None
    order = client.get("/api/watch/WO-000001").json()
    assert order["state"] == "AWAITING_WRITE"
    assert failures["n"] == 1

    # Once the failure goes away, retry commits both together.
    monkeypatch.undo()
    clock.advance(60.0)
    client.app_state.sampler.tick()
    assert store.get_trace(trace) is not None
    assert client.get("/api/watch/WO-000001").json()["state"] == "SUCCESS"


def test_ingest_rules_unchanged_during_watch(client, clock):
    trace = tid(61)
    client.post("/api/watch", json={"traceId": trace})
    # Normal OTLP behaviour: full acceptance response, dedupe, partial success.
    response = post_traces(
        client,
        [
            {"trace": trace, "span": sid(1)},
            {"trace": trace, "span": sid(1), "name": "duplicate"},
        ],
    )
    assert response.status_code == 200
    assert response.json() == {}
    clock.advance(2.0)
    client.app_state.sampler.tick()
    # No error -> dropped order, no trace archived, standard sampler counters.
    assert client.get("/api/watch/WO-000001").json()["state"] == "DROPPED"
    status = client.get("/api/status").json()
    assert "watch" in status
    assert status["watch"]["activeCount"] == 0


def test_watch_ignores_window_open_before_order(client, clock):
    trace = tid(71)
    # Window opens first; the order is created afterwards.
    post_traces(client, [{"trace": trace, "span": sid(1), "error": True}])
    order = client.post("/api/watch", json={"traceId": trace}).json()
    assert order["state"] == "WAITING"
    clock.advance(2.0)
    client.app_state.sampler.tick()  # first window archives, order stays waiting
    assert client.get(f"/api/watch/{order['orderId']}").json()["state"] == "WAITING"
    clock.advance(28.0)
    client.app_state.sampler.tick()
    assert client.get(f"/api/watch/{order['orderId']}").json()["state"] == "TIMEOUT"
