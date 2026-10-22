"""Readers for the Collector's OTLP JSON-lines files (var/telemetry/*.jsonl)."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# OTLP SpanKind enum values.
SPAN_KIND = {0: "UNSPECIFIED", 1: "INTERNAL", 2: "SERVER", 3: "CLIENT", 4: "PRODUCER", 5: "CONSUMER"}
STATUS_ERROR = 2


@dataclass
class Span:
    trace_id: str
    span_id: str
    parent_span_id: str
    service: str
    name: str
    kind: str
    start_ns: int
    end_ns: int
    is_error: bool
    attrs: dict[str, Any] = field(default_factory=dict)

    @property
    def duration_ms(self) -> float:
        return (self.end_ns - self.start_ns) / 1e6


@dataclass
class LogRecord:
    time_ns: int
    service: str
    severity: str
    body: str
    trace_id: str
    span_id: str
    attrs: dict[str, Any] = field(default_factory=dict)


def _value(v: dict[str, Any]) -> Any:
    for key in ("stringValue", "boolValue", "doubleValue"):
        if key in v:
            return v[key]
    if "intValue" in v:
        return int(v["intValue"])
    if "arrayValue" in v:
        return [_value(x) for x in v["arrayValue"].get("values", [])]
    if "kvlistValue" in v:
        return _attrs(v["kvlistValue"].get("values", []))
    return None


def _attrs(kvs: list[dict[str, Any]]) -> dict[str, Any]:
    return {kv["key"]: _value(kv.get("value", {})) for kv in kvs}


def _lines(path: Path, tail_bytes: int | None) -> Iterator[dict[str, Any]]:
    if not path.exists():
        return
    with open(path, "rb") as f:
        if tail_bytes is not None:
            size = f.seek(0, os.SEEK_END)
            f.seek(max(size - tail_bytes, 0))
            if size > tail_bytes:
                f.readline()  # drop the partial first line
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def iter_spans(path: Path, tail_bytes: int | None = None) -> Iterator[Span]:
    for doc in _lines(path, tail_bytes):
        for rs in doc.get("resourceSpans", []):
            service = _attrs(rs.get("resource", {}).get("attributes", [])).get("service.name", "unknown")
            for ss in rs.get("scopeSpans", []):
                for s in ss.get("spans", []):
                    yield Span(
                        trace_id=s["traceId"],
                        span_id=s["spanId"],
                        parent_span_id=s.get("parentSpanId", ""),
                        service=service,
                        name=s["name"],
                        kind=SPAN_KIND.get(s.get("kind", 0), "UNSPECIFIED"),
                        start_ns=int(s["startTimeUnixNano"]),
                        end_ns=int(s["endTimeUnixNano"]),
                        is_error=s.get("status", {}).get("code") == STATUS_ERROR,
                        attrs=_attrs(s.get("attributes", [])),
                    )


def iter_logs(path: Path, tail_bytes: int | None = None) -> Iterator[LogRecord]:
    for doc in _lines(path, tail_bytes):
        for rl in doc.get("resourceLogs", []):
            service = _attrs(rl.get("resource", {}).get("attributes", [])).get("service.name", "unknown")
            for sl in rl.get("scopeLogs", []):
                for r in sl.get("logRecords", []):
                    yield LogRecord(
                        time_ns=int(r.get("timeUnixNano") or r.get("observedTimeUnixNano") or 0),
                        service=service,
                        severity=r.get("severityText", ""),
                        body=str(_value(r.get("body", {})) or ""),
                        trace_id=r.get("traceId", ""),
                        span_id=r.get("spanId", ""),
                        attrs=_attrs(r.get("attributes", [])),
                    )


def group_traces(spans: Iterator[Span]) -> dict[str, list[Span]]:
    traces: dict[str, list[Span]] = {}
    for span in spans:
        traces.setdefault(span.trace_id, []).append(span)
    return traces


def render_tree(spans: list[Span]) -> str:
    """Indented span tree, for humans."""
    by_parent: dict[str, list[Span]] = {}
    ids = {s.span_id for s in spans}
    for s in sorted(spans, key=lambda s: s.start_ns):
        parent = s.parent_span_id if s.parent_span_id in ids else ""
        by_parent.setdefault(parent, []).append(s)

    lines: list[str] = []

    def walk(parent: str, depth: int) -> None:
        for s in by_parent.get(parent, []):
            peer = s.attrs.get("peer.service")
            target = f" -> {peer}" if peer and s.kind == "CLIENT" else ""
            err = "  ERROR" if s.is_error else ""
            lines.append(
                f"{'  ' * depth}[{s.service}] {s.kind:<8} {s.name}{target}  {s.duration_ms:.1f}ms{err}"
            )
            walk(s.span_id, depth + 1)

    walk("", 0)
    return "\n".join(lines)
