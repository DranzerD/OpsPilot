import json

from sim.otlp import group_traces, iter_logs, iter_spans, render_tree


def _kv(key, value):
    return {"key": key, "value": {"stringValue": value}}


def _span(span_id, parent, name, kind, service_attrs=()):
    return {
        "traceId": "t1",
        "spanId": span_id,
        "parentSpanId": parent,
        "name": name,
        "kind": kind,
        "startTimeUnixNano": "1000000",
        "endTimeUnixNano": "3000000",
        "attributes": [_kv(k, v) for k, v in service_attrs],
    }


def _resource_spans(service, spans):
    return {"resource": {"attributes": [_kv("service.name", service)]}, "scopeSpans": [{"spans": spans}]}


def test_parses_spans_across_services_and_renders_tree(tmp_path):
    path = tmp_path / "traces.jsonl"
    docs = [
        {"resourceSpans": [_resource_spans("gateway", [_span("a", "", "GET /api", 2)])]},
        {
            "resourceSpans": [
                _resource_spans(
                    "metadata",
                    [
                        _span("b", "a", "GET /datasets", 2),
                        _span("c", "b", "platform-db SELECT", 3, [("peer.service", "platform-db")]),
                    ],
                )
            ]
        },
    ]
    path.write_text("\n".join(json.dumps(d) for d in docs) + "\n")

    traces = group_traces(iter_spans(path))
    assert list(traces) == ["t1"]
    spans = traces["t1"]
    assert [(s.service, s.kind) for s in spans] == [
        ("gateway", "SERVER"),
        ("metadata", "SERVER"),
        ("metadata", "CLIENT"),
    ]
    assert spans[0].duration_ms == 2.0

    tree = render_tree(spans).splitlines()
    assert tree[0].startswith("[gateway] SERVER")
    assert tree[2].strip().startswith("[metadata] CLIENT") and "-> platform-db" in tree[2]


def test_parses_logs(tmp_path):
    path = tmp_path / "logs.jsonl"
    doc = {
        "resourceLogs": [
            {
                "resource": {"attributes": [_kv("service.name", "metadata")]},
                "scopeLogs": [
                    {
                        "logRecords": [
                            {
                                "timeUnixNano": "5",
                                "severityText": "ERROR",
                                "body": {"stringValue": "Timed out acquiring connection"},
                                "traceId": "t1",
                                "spanId": "b",
                                "attributes": [{"key": "pool_size", "value": {"intValue": "5"}}],
                            }
                        ]
                    }
                ],
            }
        ]
    }
    path.write_text(json.dumps(doc) + "\n")
    (rec,) = list(iter_logs(path))
    assert (rec.service, rec.severity, rec.trace_id, rec.attrs["pool_size"]) == ("metadata", "ERROR", "t1", 5)


def test_tail_skips_partial_first_line(tmp_path):
    path = tmp_path / "traces.jsonl"
    line = json.dumps({"resourceSpans": [_resource_spans("gateway", [_span("a", "", "x", 2)])]})
    path.write_text(line + "\n" + line + "\n")
    assert len(list(iter_spans(path, tail_bytes=len(line) + 5))) == 1
