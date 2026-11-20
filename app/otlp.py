"""OTLP/HTTP JSON parsing for trace exports.

Only the pieces the sampler needs are understood, but the whole span is kept
as JSON so the detail page can render everything that arrived.
"""

from __future__ import annotations

import json
import re
from typing import Any, Iterable

from .models import SpanRecord

_HEX32 = re.compile(r"^[0-9a-fA-F]{32}$")
_HEX16 = re.compile(r"^[0-9a-fA-F]{16}$")

_SPAN_KINDS = {
    0: "UNSPECIFIED",
    1: "INTERNAL",
    2: "SERVER",
    3: "CLIENT",
    4: "PRODUCER",
    5: "CONSUMER",
}
_STATUS_CODES = {
    0: "UNSET",
    1: "OK",
    2: "ERROR",
}


class OtlpParseError(ValueError):
    """The export request itself is malformed (HTTP 400)."""


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(text, 10)
        except ValueError:
            try:
                return int(float(text))
            except ValueError:
                return None
    return None


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _enum_name(value: Any, mapping: dict[int, str], default: str) -> str:
    if value is None:
        return default
    number = _as_int(value)
    if number is not None:
        return mapping.get(number, f"UNKNOWN({number})")
    text = str(value).strip().upper()
    for prefix in ("SPAN_KIND_", "STATUS_CODE_"):
        if text.startswith(prefix):
            text = text[len(prefix):]
    if text in mapping.values():
        return text
    return default if text == "UNSET" else text


def any_value_to_python(value: Any) -> Any:
    """Convert an OTLP AnyValue into a plain Python value."""

    if not isinstance(value, dict):
        return value
    if "stringValue" in value:
        return value["stringValue"]
    if "boolValue" in value:
        return value["boolValue"]
    if "intValue" in value:
        return _as_int(value["intValue"])
    if "doubleValue" in value:
        try:
            return float(value["doubleValue"])
        except (TypeError, ValueError):
            return value["doubleValue"]
    if "bytesValue" in value:
        return value["bytesValue"]
    if "arrayValue" in value:
        array = value["arrayValue"] if isinstance(value["arrayValue"], dict) else {}
        return [any_value_to_python(item) for item in array.get("values") or []]
    if "kvlistValue" in value:
        kvlist = value["kvlistValue"] if isinstance(value["kvlistValue"], dict) else {}
        return {
            _as_text(item.get("key")): any_value_to_python(item.get("value"))
            for item in kvlist.get("values") or []
            if isinstance(item, dict)
        }
    return None


def attributes_to_dict(attributes: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if not isinstance(attributes, list):
        return result
    for item in attributes:
        if not isinstance(item, dict):
            continue
        result[_as_text(item.get("key"))] = any_value_to_python(item.get("value"))
    return result


def _iter_span_groups(payload: dict[str, Any]) -> Iterable[tuple[dict, dict]]:
    """Yield (resource, scope) pairs for modern and legacy OTLP layouts."""

    for resource_spans in payload.get("resourceSpans") or []:
        if not isinstance(resource_spans, dict):
            continue
        resource = resource_spans.get("resource")
        resource = resource if isinstance(resource, dict) else {}
        groups = resource_spans.get("scopeSpans")
        if groups is None:
            groups = resource_spans.get("instrumentationLibrarySpans")
        for group in groups or []:
            if not isinstance(group, dict):
                continue
            scope = group.get("scope")
            if scope is None:
                scope = group.get("instrumentationLibrary")
            scope = scope if isinstance(scope, dict) else {}
            spans = group.get("spans")
            if not isinstance(spans, list):
                continue
            yield resource, scope, spans


def _normalise_ids(span: dict[str, Any]) -> tuple[str, str, str | None] | None:
    trace_id = _as_text(span.get("traceId")).strip().lower()
    span_id = _as_text(span.get("spanId")).strip().lower()
    if not _HEX32.match(trace_id) or not _HEX16.match(span_id):
        return None
    if trace_id == "0" * 32 or span_id == "0" * 16:
        return None
    raw_parent = _as_text(span.get("parentSpanId")).strip().lower()
    parent: str | None = None
    if raw_parent and raw_parent != "0" * 16 and _HEX16.match(raw_parent):
        parent = raw_parent
    return trace_id, span_id, parent


def _parse_span(
    span: dict[str, Any], resource: dict, scope: dict
) -> SpanRecord | None:
    ids = _normalise_ids(span)
    if ids is None:
        return None
    trace_id, span_id, parent_span_id = ids

    status = span.get("status")
    status = status if isinstance(status, dict) else {}
    status_code = _enum_name(status.get("code"), _STATUS_CODES, "UNSET")
    status_message = _as_text(status.get("message"))

    start_ns = _as_int(span.get("startTimeUnixNano"))
    end_ns = _as_int(span.get("endTimeUnixNano"))
    duration = None
    if start_ns is not None and end_ns is not None and end_ns >= start_ns:
        duration = end_ns - start_ns

    attrs = attributes_to_dict(span.get("attributes"))
    resource_attrs = attributes_to_dict(resource.get("attributes"))
    service_name = resource_attrs.get("service.name")
    if service_name is not None:
        service_name = _as_text(service_name)

    scope_json = json.dumps(scope, ensure_ascii=False, sort_keys=True)
    return SpanRecord(
        trace_id=trace_id,
        span_id=span_id,
        parent_span_id=parent_span_id,
        name=_as_text(span.get("name")),
        kind=_enum_name(span.get("kind"), _SPAN_KINDS, "UNSPECIFIED"),
        status_code=status_code,
        status_message=status_message,
        is_error=status_code == "ERROR",
        start_time_ns=str(start_ns) if start_ns is not None else None,
        end_time_ns=str(end_ns) if end_ns is not None else None,
        duration_ns=duration,
        service_name=service_name,
        event_count=len(span.get("events") or []),
        link_count=len(span.get("links") or []),
        resource_json=json.dumps(resource_attrs, ensure_ascii=False, sort_keys=True),
        scope_json=scope_json,
        attributes_json=json.dumps(attrs, ensure_ascii=False, sort_keys=True),
        raw_json=json.dumps(span, ensure_ascii=False, sort_keys=True),
    )


def parse_trace_request(body: bytes) -> tuple[list[SpanRecord], int]:
    """Parse an OTLP/HTTP JSON ExportTraceServiceRequest body.

    Returns ``(valid_spans, invalid_span_count)``.  Raises
    :class:`OtlpParseError` when the body is not a usable export request.
    """

    if not body or not body.strip():
        raise OtlpParseError("empty request body")
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise OtlpParseError(f"invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise OtlpParseError("request body must be a JSON object")

    spans: list[SpanRecord] = []
    invalid = 0
    seen_groups = False
    for resource, scope, group_spans in _iter_span_groups(payload):
        seen_groups = True
        for span in group_spans:
            if not isinstance(span, dict):
                invalid += 1
                continue
            parsed = _parse_span(span, resource, scope)
            if parsed is None:
                invalid += 1
                continue
            spans.append(parsed)
    if not seen_groups and payload:
        # A bare {"resourceSpans": []} is valid and simply carries nothing.
        if "resourceSpans" not in payload and "resource_spans" not in payload:
            raise OtlpParseError("missing resourceSpans")
    return spans, invalid


def export_response(rejected: int, message: str | None = None, accepted: int | None = None) -> dict:
    """Build an OTLP/HTTP JSON ExportTraceServiceResponse."""

    response: dict[str, Any] = {}
    if rejected or message:
        partial: dict[str, Any] = {}
        if rejected:
            partial["rejectedSpans"] = rejected
        if message:
            partial["errorMessage"] = message
        if accepted:
            partial["acceptedSpans"] = accepted
        response["partialSuccess"] = partial
    return response
