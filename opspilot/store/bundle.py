"""BundleStore: read-only access to a recorded scenario bundle (spec D1, D2).

This is what the agent's tools (and baselines) see in evaluation. It must never expose the
scenario's identity, fault window or label: those live in manifest.json and are read only by
evaluation code, through `load_ground_truth`.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Self

import duckdb

from opspilot.store.signals import SIGNALS

_LABEL_KEY = re.compile(r"^[a-z_][a-z0-9_]*$")


@dataclass
class Series:
    labels: dict[str, str]
    points: list[tuple[datetime, float]]


class BundleStore:
    TABLES = ("metrics", "spans", "logs", "changes")

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        manifest = json.loads((self.path / "manifest.json").read_text())
        # Only the overall window is exposed: fault boundaries are ground truth.
        self._start = datetime.fromisoformat(manifest["window"]["start"])
        self._end = datetime.fromisoformat(manifest["window"]["end"])
        self._topology = json.loads((self.path / "topology.json").read_text())
        self._con = duckdb.connect()
        self._con.execute("SET TimeZone = 'UTC'")
        for table in self.TABLES:
            file = (self.path / f"{table}.parquet").as_posix()
            self._con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{file}')")

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _rows(self, sql: str, params: list[Any]) -> list[dict[str, Any]]:
        cur = self._con.execute(sql, params)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]

    def _window(self, start: datetime | None, end: datetime | None) -> tuple[datetime, datetime]:
        return start or self._start, end or self._end

    # --- Topology ----------------------------------------------------------------------------

    def time_range(self) -> tuple[datetime, datetime]:
        return self._start, self._end

    def services(self) -> list[str]:
        return sorted(self._topology["services"])

    def topology(self) -> dict[str, Any]:
        return self._topology

    # --- Metrics -----------------------------------------------------------------------------

    def metric(
        self,
        signal: str,
        service: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        labels: dict[str, str] | None = None,
    ) -> list[Series]:
        """Series for a named signal (opspilot.store.signals), optionally for one service."""
        sig = SIGNALS[signal]
        start, end = self._window(start, end)
        sql = "SELECT labels, ts, value FROM metrics WHERE metric = ? AND ts BETWEEN ? AND ?"
        params: list[Any] = [sig.series, start, end]
        matchers = dict(labels or {})
        if service is not None:
            if sig.owner is not None:
                if service != sig.owner:
                    return []
            else:
                matchers[sig.service_label] = service
        for key, value in matchers.items():
            if not _LABEL_KEY.match(key):
                raise ValueError(f"invalid label name {key!r}")
            sql += f" AND json_extract_string(labels, '$.{key}') = ?"
            params.append(value)
        sql += " ORDER BY labels, ts"

        series: dict[str, Series] = {}
        for labels_json, ts, value in self._con.execute(sql, params).fetchall():
            if value is None or math.isnan(value):
                continue
            s = series.setdefault(labels_json, Series(json.loads(labels_json), []))
            s.points.append((ts, value))
        return [s for s in series.values() if s.points]

    # --- Logs, spans, changes ----------------------------------------------------------------

    def logs(
        self,
        service: str | None = None,
        severity: str | None = None,
        pattern: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Log records, newest first. `pattern` is a regular expression over the message."""
        start, end = self._window(start, end)
        sql = "SELECT ts, service, severity, message, trace_id, span_id, attrs FROM logs WHERE ts BETWEEN ? AND ?"
        params: list[Any] = [start, end]
        if service:
            sql += " AND service = ?"
            params.append(service)
        if severity:
            sql += " AND severity = ?"
            params.append(severity.upper())
        if pattern:
            sql += " AND regexp_matches(message, ?)"
            params.append(pattern)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        return self._rows(sql, params)

    def count_logs(
        self,
        service: str | None = None,
        pattern: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> int:
        start, end = self._window(start, end)
        sql = "SELECT COUNT(*) FROM logs WHERE ts BETWEEN ? AND ?"
        params: list[Any] = [start, end]
        if service:
            sql += " AND service = ?"
            params.append(service)
        if pattern:
            sql += " AND regexp_matches(message, ?)"
            params.append(pattern)
        return self._con.execute(sql, params).fetchone()[0]

    def spans(
        self,
        service: str | None = None,
        trace_id: str | None = None,
        kind: str | None = None,
        errors_only: bool = False,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = 1000,
    ) -> list[dict[str, Any]]:
        start, end = self._window(start, end)
        sql = (
            "SELECT trace_id, span_id, parent_span_id, service, name, kind, start_ts, end_ts, duration_ms, "
            "is_error, peer_service FROM spans WHERE start_ts BETWEEN ? AND ?"
        )
        params: list[Any] = [start, end]
        for col, value in (("service", service), ("trace_id", trace_id), ("kind", kind)):
            if value:
                sql += f" AND {col} = ?"
                params.append(value)
        if errors_only:
            sql += " AND is_error"
        sql += " ORDER BY start_ts LIMIT ?"
        params.append(limit)
        return self._rows(sql, params)

    def span_duration_quantile(
        self, service: str, q: float, start: datetime | None = None, end: datetime | None = None
    ) -> float | None:
        """Quantile of SERVER span duration (ms) for a service."""
        start, end = self._window(start, end)
        row = self._con.execute(
            "SELECT quantile_cont(duration_ms, ?) FROM spans WHERE service = ? AND kind = 'SERVER' "
            "AND start_ts BETWEEN ? AND ?",
            [q, service, start, end],
        ).fetchone()
        return row[0]

    def changes(
        self, service: str | None = None, start: datetime | None = None, end: datetime | None = None
    ) -> list[dict[str, Any]]:
        start, end = self._window(start, end)
        sql = "SELECT id, ts, service, kind, summary, details FROM changes WHERE ts BETWEEN ? AND ?"
        params: list[Any] = [start, end]
        if service:
            sql += " AND service = ?"
            params.append(service)
        rows = self._rows(sql + " ORDER BY ts", params)
        for row in rows:
            row["details"] = json.loads(row["details"])
        return rows


@dataclass
class GroundTruth:
    scenario_id: str
    label: dict[str, Any]
    fault_start: datetime
    fault_end: datetime


def load_ground_truth(path: Path) -> GroundTruth:
    """Evaluation-only: the scenario label and fault window. Never pass this to an agent."""
    manifest = json.loads((Path(path) / "manifest.json").read_text())
    return GroundTruth(
        scenario_id=manifest["scenario"]["id"],
        label=manifest["scenario"]["label"],
        fault_start=datetime.fromisoformat(manifest["window"]["fault_start"]),
        fault_end=datetime.fromisoformat(manifest["window"]["fault_end"]),
    )
