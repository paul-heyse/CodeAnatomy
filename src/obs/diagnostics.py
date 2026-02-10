"""Diagnostics sink helpers for runtime and rule events."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final

import datafusion_ext as _datafusion_ext

if TYPE_CHECKING:
    from serde_schema_registry import ArtifactSpec
from datafusion_engine.lineage.diagnostics import (
    ensure_recorder_sink,
    rust_udf_snapshot_payload,
    view_fingerprint_payload,
    view_udf_parity_payload,
)
from datafusion_engine.schema.contracts import ValidationViolation
from datafusion_engine.views.artifacts import DataFusionViewArtifact
from obs.otel.logs import emit_diagnostics_event
from obs.otel.metrics import record_artifact_count
from schema_spec.pandera_bridge import validation_policy_payload
from schema_spec.system import ValidationPolicySpec
from serde_artifact_specs import (
    DATAFRAME_VALIDATION_ERRORS_SPEC,
    DATAFUSION_PREPARED_STATEMENTS_SPEC,
    DATAFUSION_VIEW_ARTIFACTS_SPEC,
    RUST_UDF_SNAPSHOT_SPEC,
    SEMANTIC_QUALITY_ARTIFACT_SPEC,
    VIEW_CONTRACT_VIOLATIONS_SPEC,
    VIEW_FINGERPRINTS_SPEC,
    VIEW_UDF_PARITY_SPEC,
)
from serde_msgspec import StructBaseCompat
from utils.uuid_factory import uuid7_str

_ = _datafusion_ext
_OBS_SESSION_ID: Final[str] = uuid7_str()

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.lineage.diagnostics import DiagnosticsSink
    from datafusion_engine.views.graph import ViewNode


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

    def record_artifact(self, name: ArtifactSpec, payload: Mapping[str, object]) -> None:
        """Append an artifact payload under a logical name."""
        resolved = name.canonical_name
        bucket = self.artifacts.setdefault(resolved, [])
        normalized = dict(payload)
        bucket.append(normalized)
        emit_diagnostics_event(resolved, payload=normalized, event_kind="artifact")
        record_artifact_count(resolved, status="ok", attributes={"artifact.type": "artifact"})

    def record_event(self, name: str, properties: Mapping[str, object]) -> None:
        """Append an event payload under a logical name."""
        bucket = self.events.setdefault(name, [])
        normalized = dict(properties)
        bucket.append(normalized)
        emit_diagnostics_event(name, payload=normalized, event_kind="event")
        record_artifact_count(name, status="ok", attributes={"artifact.type": "event"})

    def events_snapshot(self) -> dict[str, list[Mapping[str, object]]]:
        """Return a shallow copy of collected event rows.

        Returns:
        -------
        dict[str, list[Mapping[str, object]]]
            Mapping of event names to collected rows.
        """
        return {name: list(rows) for name, rows in self.events.items()}

    def artifacts_snapshot(self) -> dict[str, list[Mapping[str, object]]]:
        """Return a shallow copy of collected artifacts.

        Returns:
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

        Returns:
        -------
        Mapping[str, object]
            JSON-ready payload describing the prepared statement.
        """
        return {
            "name": self.name,
            "sql": self.sql,
            "param_types": list(self.param_types),
        }


class SemanticQualityArtifact(StructBaseCompat, frozen=True):
    """Summary payload for a semantic diagnostics artifact."""

    name: str
    row_count: int
    schema_hash: str | None
    artifact_uri: str | None
    run_id: str | None

    def payload(self) -> Mapping[str, object]:
        """Return a JSON-ready payload for diagnostics sinks.

        Returns:
        -------
        Mapping[str, object]
            JSON-ready payload describing the artifact summary.
        """
        return {
            "name": self.name,
            "row_count": self.row_count,
            "schema_hash": self.schema_hash,
            "artifact_uri": self.artifact_uri,
            "run_id": self.run_id,
        }


def prepared_statement_hook(
    sink: DiagnosticsCollector,
) -> Callable[[PreparedStatementSpec], None]:
    """Return a hook that records prepared statements in diagnostics.

    Returns:
    -------
    Callable[[PreparedStatementSpec], None]
        Hook that records prepared statement metadata.
    """

    def _hook(spec: PreparedStatementSpec) -> None:
        recorder_sink = ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)
        recorder_sink.record_artifact(DATAFUSION_PREPARED_STATEMENTS_SPEC, spec.payload())

    return _hook


def record_semantic_quality_artifact(
    sink: DiagnosticsSink,
    *,
    artifact: SemanticQualityArtifact,
) -> None:
    """Record a semantic quality artifact payload.

    Parameters
    ----------
    sink
        Diagnostics sink for recording the artifact.
    artifact
        Semantic diagnostics artifact summary payload.
    """
    recorder_sink = ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)
    recorder_sink.record_artifact(SEMANTIC_QUALITY_ARTIFACT_SPEC, artifact.payload())


def record_semantic_quality_events(
    sink: DiagnosticsSink,
    *,
    name: str,
    rows: Sequence[Mapping[str, object]],
) -> None:
    """Record semantic quality event rows.

    Parameters
    ----------
    sink
        Diagnostics sink for recording the events.
    name
        Event stream name.
    rows
        Event payload rows.
    """
    recorder_sink = ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)
    recorder_sink.record_events(name, rows)


def record_view_fingerprints(
    sink: DiagnosticsCollector,
    *,
    view_nodes: Sequence[ViewNode],
) -> None:
    """Record policy-aware view fingerprints into diagnostics."""
    recorder_sink = ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)
    recorder_sink.record_artifact(
        VIEW_FINGERPRINTS_SPEC,
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
        VIEW_UDF_PARITY_SPEC,
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
        RUST_UDF_SNAPSHOT_SPEC,
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
    recorder_sink.record_artifact(VIEW_CONTRACT_VIOLATIONS_SPEC, payload)


def _normalize_failure_cases(error: Exception) -> list[dict[str, object]] | None:
    failure_cases = getattr(error, "failure_cases", None)
    if failure_cases is None:
        return None
    try:
        head = getattr(failure_cases, "head", None)
        if callable(head):
            failure_cases = head(50)
        to_dict = getattr(failure_cases, "to_dict", None)
        if callable(to_dict):
            payload = to_dict(orient="records")
            if isinstance(payload, list):
                return payload
    except (AttributeError, TypeError, ValueError):
        return None
    return None


def record_dataframe_validation_error(
    sink: DiagnosticsSink,
    *,
    name: str,
    error: Exception,
    policy: ValidationPolicySpec | None = None,
) -> None:
    """Record a dataframe validation error payload."""
    failure_cases_payload = _normalize_failure_cases(error)
    payload = {
        "name": name,
        "error_type": type(error).__name__,
        "error_message": str(error),
        "policy": validation_policy_payload(policy) if policy is not None else None,
        "failure_cases": failure_cases_payload,
    }
    recorder_sink = ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)
    recorder_sink.record_artifact(DATAFRAME_VALIDATION_ERRORS_SPEC, payload)


def record_view_artifact(sink: DiagnosticsCollector, *, artifact: DataFusionViewArtifact) -> None:
    """Record a deterministic view artifact payload."""
    recorder_sink = ensure_recorder_sink(sink, session_id=_OBS_SESSION_ID)
    recorder_sink.record_artifact(
        DATAFUSION_VIEW_ARTIFACTS_SPEC,
        artifact.diagnostics_payload(event_time_unix_ms=int(time.time() * 1000)),
    )


__all__ = [
    "DiagnosticsCollector",
    "PreparedStatementSpec",
    "SemanticQualityArtifact",
    "prepared_statement_hook",
    "record_dataframe_validation_error",
    "record_rust_udf_snapshot",
    "record_semantic_quality_artifact",
    "record_semantic_quality_events",
    "record_view_artifact",
    "record_view_contract_violations",
    "record_view_fingerprints",
    "record_view_udf_parity",
]
