"""Diagnostics sink helpers for runtime and rule events."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

import datafusion_ext as _datafusion_ext
from datafusion_engine.diagnostics import (
    ensure_recorder_sink,
    rust_udf_snapshot_payload,
    view_fingerprint_payload,
    view_udf_parity_payload,
)
from datafusion_engine.schema_contracts import ValidationViolation
from datafusion_engine.view_artifacts import DataFusionViewArtifact
from obs.otel.logs import emit_diagnostics_event
from obs.otel.metrics import record_artifact_count
from serde_msgspec import StructBaseCompat
from utils.uuid_factory import uuid7_str

_ = _datafusion_ext
_OBS_SESSION_ID: Final[str] = uuid7_str()

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.view_graph_registry import ViewNode


@dataclass
class DiagnosticsCollector:
    """Collect diagnostics events and artifacts in-memory."""

    events: dict[str, list[Mapping[str, object]]] = field(default_factory=dict)
    artifacts: dict[str, list[Mapping[str, object]]] = field(default_factory=dict)

    def record_events(self, name: str, rows: Sequence[Mapping[str, object]]) -> None:
        """Append event rows under a logical name."""
        bucket = self.events.setdefault(name, [])
        normalized = [dict(row) for row in rows]
        bucket.extend(normalized)
        for row in normalized:
            emit_diagnostics_event(name, payload=row, event_kind="event")
        record_artifact_count(name, status="ok", attributes={"artifact.type": "event"})

    def record_artifact(self, name: str, payload: Mapping[str, object]) -> None:
        """Append an artifact payload under a logical name."""
        bucket = self.artifacts.setdefault(name, [])
        normalized = dict(payload)
        bucket.append(normalized)
        emit_diagnostics_event(name, payload=normalized, event_kind="artifact")
        record_artifact_count(name, status="ok", attributes={"artifact.type": "artifact"})

    def record_event(self, name: str, properties: Mapping[str, object]) -> None:
        """Append an event payload under a logical name."""
        bucket = self.events.setdefault(name, [])
        normalized = dict(properties)
        bucket.append(normalized)
        emit_diagnostics_event(name, payload=normalized, event_kind="event")
        record_artifact_count(name, status="ok", attributes={"artifact.type": "event"})

    def events_snapshot(self) -> dict[str, list[Mapping[str, object]]]:
        """Return a shallow copy of collected event rows.

        Returns
        -------
        dict[str, list[Mapping[str, object]]]
            Mapping of event names to collected rows.
        """
        return {name: list(rows) for name, rows in self.events.items()}

    def artifacts_snapshot(self) -> dict[str, list[Mapping[str, object]]]:
        """Return a shallow copy of collected artifacts.

        Returns
        -------
        dict[str, list[Mapping[str, object]]]
            Mapping of artifact names to collected payloads.
        """
        return {name: list(rows) for name, rows in self.artifacts.items()}


class PreparedStatementSpec(StructBaseCompat, frozen=True):
    """Prepared statement metadata for diagnostics reporting."""

    name: str
    sql: str
    param_types: tuple[str, ...]

    def payload(self) -> Mapping[str, object]:
        """Return a JSON-ready payload for diagnostics sinks.

        Returns
        -------
        Mapping[str, object]
            JSON-ready payload describing the prepared statement.
        """
        return {
            "name": self.name,
            "sql": self.sql,
            "param_types": list(self.param_types),
        }


def prepared_statement_hook(
    sink: DiagnosticsCollector,
) -> Callable[[PreparedStatementSpec], None]:
    """Return a hook that records prepared statements in diagnostics.

    Returns
    -------
    Callable[[PreparedStatementSpec], None]
        Hook that records prepared statement metadata.
    """

    def _hook(spec: PreparedStatementSpec) -> None:
        recorder_sink = ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)
        recorder_sink.record_artifact("datafusion_prepared_statements_v1", spec.payload())

    return _hook


def record_view_fingerprints(
    sink: DiagnosticsCollector,
    *,
    view_nodes: Sequence[ViewNode],
) -> None:
    """Record policy-aware view fingerprints into diagnostics."""
    recorder_sink = ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)
    recorder_sink.record_artifact(
        "view_fingerprints_v1",
        view_fingerprint_payload(view_nodes=view_nodes),
    )


def record_view_udf_parity(
    sink: DiagnosticsCollector,
    *,
    snapshot: Mapping[str, object],
    view_nodes: Sequence[ViewNode],
    ctx: SessionContext | None = None,
) -> None:
    """Record view/UDF parity diagnostics into the sink."""
    recorder_sink = ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)
    recorder_sink.record_artifact(
        "view_udf_parity_v1",
        view_udf_parity_payload(snapshot=snapshot, view_nodes=view_nodes, ctx=ctx),
    )


def record_rust_udf_snapshot(
    sink: DiagnosticsCollector,
    *,
    snapshot: Mapping[str, object],
) -> None:
    """Record a Rust UDF snapshot summary payload."""
    recorder_sink = ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)
    recorder_sink.record_artifact(
        "rust_udf_snapshot_v1",
        rust_udf_snapshot_payload(snapshot),
    )


def record_view_contract_violations(
    sink: DiagnosticsCollector,
    *,
    table_name: str,
    violations: Sequence[ValidationViolation],
) -> None:
    """Record schema contract violations for a view."""
    payload = {
        "view": table_name,
        "violations": [
            {
                "violation_type": violation.violation_type.value,
                "column_name": violation.column_name,
                "expected": violation.expected,
                "actual": violation.actual,
            }
            for violation in violations
        ],
    }
    recorder_sink = ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)
    recorder_sink.record_artifact("view_contract_violations_v1", payload)


def record_view_artifact(sink: DiagnosticsCollector, *, artifact: DataFusionViewArtifact) -> None:
    """Record a deterministic view artifact payload."""
    recorder_sink = ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)
    recorder_sink.record_artifact(
        "datafusion_view_artifacts_v4",
        artifact.diagnostics_payload(event_time_unix_ms=int(time.time() * 1000)),
    )


def record_cache_lineage(
    sink: DiagnosticsCollector,
    *,
    payload: Mapping[str, object],
    rows: Sequence[Mapping[str, object]] | None = None,
) -> None:
    """Record cache lineage artifacts and optional per-node rows."""
    recorder_sink = ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)
    recorder_sink.record_artifact("hamilton_cache_lineage_v2", payload)
    if rows:
        recorder_sink.record_events("hamilton_cache_lineage_nodes_v1", rows)


__all__ = [
    "DiagnosticsCollector",
    "PreparedStatementSpec",
    "prepared_statement_hook",
    "record_cache_lineage",
    "record_rust_udf_snapshot",
    "record_view_artifact",
    "record_view_contract_violations",
    "record_view_fingerprints",
    "record_view_udf_parity",
]
