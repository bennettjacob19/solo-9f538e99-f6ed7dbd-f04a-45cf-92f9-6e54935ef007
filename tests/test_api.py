from __future__ import annotations

import json
import os
import sys

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.main import create_app  # noqa: E402

HEX = "0123456789abcdef"


def tid(n: int) -> str:
    return f"{n:032x}"


def sid(n: int) -> str:
    return f"{n:016x}"


def otlp_payload(spans) -> dict:
    """Build an OTLP/HTTP JSON export request from compact span dicts."""

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
            "attributes": item.get(
                "attributes",
                [{"key": "http.route", "value": {"stringValue": "/checkout"}}],
            ),
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
def client(tmp_path, clock):
    app = create_app(
        str(tmp_path / "traces.db"),
        window_seconds=2.0,
        capacity=16,
        max_spans_per_trace=64,
        start_sampler_thread=False,
        clock=clock,
    )
    with TestClient(app) as test_client:
        test_client.app_state = app.state
        yield test_client
    app.state.store.close()


def post(client, spans):
    return client.post(
        "/v1/traces",
        content=json.dumps(otlp_payload(spans)),
        headers={"content-type": "application/json"},
    )


def close_window(client, clock):
    clock.advance(2.0)
    client.app_state.sampler.tick()


def test_root_ok_then_late_error_is_rejected_and_not_archived(client, clock):
    """The exact scenario from the brief: a successful root span arrives first,
    the erroring payment span after the window ends."""

    trace = tid(1)
    response = post(
        client,
        [
            {"trace": trace, "span": sid(1), "name": "POST /checkout", "service": "api"},
        ],
    )
    assert response.status_code == 200
    assert response.json() == {}

    clock.advance(2.4)  # the payment call fails after the 2s window
    response = post(
        client,
        [
            {
                "trace": trace,
                "span": sid(2),
                "parent": sid(1),
                "name": "POST /pay",
                "service": "payments",
                "error": True,
            }
        ],
    )
    body = response.json()
    assert response.status_code == 200
    assert body["partialSuccess"]["rejectedSpans"] == 1
    assert "window_closed" in body["partialSuccess"]["errorMessage"]

    # The root trace had no error inside the window, so it was discarded.
    assert client.app_state.store.count() == 0
    assert client.get("/api/status").json()["archivedTraces"] == 0


def test_error_inside_window_is_archived_after_two_seconds(client, clock):
    trace = tid(2)
    post(
        client,
        [
            {"trace": trace, "span": sid(1), "name": "POST /checkout"},
            {"trace": trace, "span": sid(2), "parent": sid(1), "name": "POST /pay", "error": True},
        ],
    )
    assert client.app_state.store.count() == 0  # still collecting
    clock.advance(1.99)
    client.app_state.sampler.tick()
    assert client.app_state.store.count() == 0
    clock.advance(0.01)
    client.app_state.sampler.tick()
    assert client.app_state.store.count() == 1

    body = client.get(f"/api/traces/{trace}").json()
    assert body["span_count"] == 2
    assert body["error_count"] == 1
    assert {span["name"] for span in body["spans"]} == {"POST /checkout", "POST /pay"}


def test_partial_success_counts_and_keeps_good_spans(client, clock):
    # Fill all 16 slots, then a batch mixing a refused new trace with a good span.
    for i in range(16):
        post(client, [{"trace": tid(100 + i), "span": sid(1)}])

    response = post(
        client,
        [
            {"trace": tid(300), "span": sid(1)},  # new trace: no slot
            {"trace": tid(100), "span": sid(2), "error": True},  # admitted trace
        ],
    )
    assert response.status_code == 200
    partial = response.json()["partialSuccess"]
    assert partial["rejectedSpans"] == 1
    assert partial["acceptedSpans"] == 1
    assert "capacity=1" in partial["errorMessage"]

    close_window(client, clock)
    # Only trace 100 had an error.
    assert client.app_state.store.count() == 1
    assert client.get(f"/api/traces/{tid(100)}").status_code == 200
    assert client.get(f"/api/traces/{tid(300)}").status_code == 404


def test_duplicate_span_ids_are_not_kept_twice(client, clock):
    trace = tid(4)
    span = {"trace": trace, "span": sid(1), "error": True}
    post(client, [span, dict(span, name="duplicate")])
    close_window(client, clock)
    body = client.get(f"/api/traces/{trace}").json()
    assert len(body["spans"]) == 1


def test_span_cap_64_on_the_wire(client, clock):
    trace = tid(5)
    spans = [{"trace": trace, "span": sid(i), "error": i == 1} for i in range(1, 71)]
    response = post(client, spans)
    partial = response.json()["partialSuccess"]
    assert partial["rejectedSpans"] == 6
    assert "span_limit=6" in partial["errorMessage"]
    close_window(client, clock)
    assert client.get(f"/api/traces/{trace}").json()["span_count"] == 64


def test_pause_and_resume_via_http(client, clock):
    trace = tid(6)
    post(client, [{"trace": trace, "span": sid(1), "error": True}])
    assert client.post("/api/archive/pause").json() == {"paused": True}
    close_window(client, clock)
    status = client.get("/api/status").json()
    assert status["paused"] is True
    assert len(status["pendingTraces"]) == 1
    assert status["archivedTraces"] == 0
    # Nothing is visible on the page while paused.
    page = client.get("/")
    assert trace not in page.text

    assert client.post("/api/archive/resume").json() == {"paused": False}
    status = client.get("/api/status").json()
    assert status["archivedTraces"] == 1
    assert status["pendingTraces"] == []
    assert trace in client.get("/").text


def test_pause_html_buttons(client, clock):
    post(client, [{"trace": tid(7), "span": sid(1), "error": True}])
    assert "Pause archiving" in client.get("/").text
    client.post("/pause")
    assert "Resume archiving" in client.get("/").text
    close_window(client, clock)
    client.post("/resume")
    assert tid(7) in client.get("/").text


def test_index_and_detail_pages_render(client, clock):
    trace = tid(8)
    post(
        client,
        [
            {"trace": trace, "span": sid(1), "name": "POST /checkout", "service": "api"},
            {
                "trace": trace,
                "span": sid(2),
                "parent": sid(1),
                "name": "POST /pay",
                "service": "payments",
                "error": True,
            },
        ],
    )
    close_window(client, clock)
    index = client.get("/")
    assert index.status_code == 200
    assert "POST /checkout" in index.text
    assert "payments" in index.text

    detail = client.get(f"/traces/{trace}")
    assert detail.status_code == 200
    assert "POST /pay" in detail.text
    assert "payment declined" in detail.text
    assert "/checkout" in detail.text

    assert client.get(f"/traces/{tid(999)}").status_code == 404


def test_malformed_payload_returns_400(client):
    response = client.post(
        "/v1/traces", content=b"not json", headers={"content-type": "application/json"}
    )
    assert response.status_code == 400
    assert "invalid JSON" in response.json()["partialSuccess"]["errorMessage"]

    response = client.post(
        "/v1/traces", content=b"[]", headers={"content-type": "application/json"}
    )
    assert response.status_code == 400


def test_invalid_span_ids_are_rejected_but_valid_ones_kept(client, clock):
    trace = tid(9)
    payload = otlp_payload([{"trace": trace, "span": sid(1), "error": True}])
    payload["resourceSpans"][0]["scopeSpans"][0]["spans"].append(
        {"traceId": "zzz", "spanId": "short", "name": "bad"}
    )
    response = client.post(
        "/v1/traces", content=json.dumps(payload), headers={"content-type": "application/json"}
    )
    partial = response.json()["partialSuccess"]
    assert partial["rejectedSpans"] == 1
    assert "invalid_span=1" in partial["errorMessage"]
    close_window(client, clock)
    assert client.app_state.store.count() == 1
