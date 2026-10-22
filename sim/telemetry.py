"""OpenTelemetry setup shared by all simulated services.

Traces, metrics and logs are exported over OTLP/HTTP to the local Collector. Logs are also
written to stdout as JSON lines carrying trace/span IDs, so process logs correlate with traces.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime

from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from sim.config import OTLP_ENDPOINT, VERSION

METRIC_EXPORT_INTERVAL_MS = 5_000

# Bucket boundaries (seconds) for latency histograms; OTel's default buckets assume milliseconds.
LATENCY_BUCKETS_S = [0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0, 30.0]
JOB_BUCKETS_S = [0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0, 300.0]

_LOG_RECORD_RESERVED = set(logging.LogRecord("", 0, "", 0, "", None, None).__dict__) | {
    "message",
    "asctime",
    "color_message",  # uvicorn's ANSI-coloured duplicate of the message
}


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        ctx = trace.get_current_span().get_span_context()
        doc = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "severity": record.levelname,
            "service": self.service,
            "logger": record.name,
            "message": record.getMessage(),
            "trace_id": format(ctx.trace_id, "032x") if ctx.is_valid else None,
            "span_id": format(ctx.span_id, "016x") if ctx.is_valid else None,
        }
        attrs = {k: v for k, v in record.__dict__.items() if k not in _LOG_RECORD_RESERVED}
        if attrs:
            doc["attrs"] = attrs
        if record.exc_info:
            doc["exception"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str)


def setup_telemetry(service: str, export: bool = True) -> None:
    """Install global tracer, meter and logger providers for this process.

    With export=False (tests), providers are installed without exporters.
    """
    resource = Resource.create(
        {"service.name": service, "service.version": VERSION, "deployment.environment": "sim"}
    )

    tracer_provider = TracerProvider(resource=resource)
    readers = []
    if export:
        tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(f"{OTLP_ENDPOINT}/v1/traces")))
        readers.append(
            PeriodicExportingMetricReader(
                OTLPMetricExporter(f"{OTLP_ENDPOINT}/v1/metrics"),
                export_interval_millis=METRIC_EXPORT_INTERVAL_MS,
            )
        )
    trace.set_tracer_provider(tracer_provider)
    metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=readers))

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    stdout = logging.StreamHandler(sys.stdout)
    stdout.setFormatter(JsonFormatter(service))
    root.addHandler(stdout)

    if export:
        logger_provider = LoggerProvider(resource=resource)
        logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(OTLPLogExporter(f"{OTLP_ENDPOINT}/v1/logs"))
        )
        set_logger_provider(logger_provider)
        root.addHandler(LoggingHandler(level=logging.INFO, logger_provider=logger_provider))

    # Our own logs carry the useful detail; keep library chatter out of the evidence.
    for noisy in ("uvicorn.access", "httpx", "httpcore", "urllib3", "opentelemetry"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
