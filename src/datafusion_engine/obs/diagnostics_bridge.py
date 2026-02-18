"""Engine-side diagnostics helpers relocated from ``obs`` package."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Final

from obs.diagnostics import (
    DiagnosticsCollector,
    PreparedStatementSpec,
    SemanticQualityArtifact,
    prepared_statement_hook,
    record_semantic_quality_artifact,
    record_semantic_quality_events,
)
from schema_spec.dataset_spec import ValidationPolicySpec, validation_policy_payload
from serde_artifact_specs import (
    DATAFRAME_VALIDATION_ERRORS_SPEC,
    DATAFUSION_VIEW_ARTIFACTS_SPEC,
    RUST_UDF_SNAPSHOT_SPEC,
    VIEW_CONTRACT_VIOLATIONS_SPEC,
    VIEW_FINGERPRINTS_SPEC,
    VIEW_UDF_PARITY_SPEC,
)
from utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.lineage.diagnostics import DiagnosticsSink
    from datafusion_engine.schema.contracts import ValidationViolation
    from datafusion_engine.views.artifacts import DataFusionViewArtifact
    from datafusion_engine.views.graph import ViewNode


_OBS_SESSION_ID: Final[str] = uuid7_str()


def record_view_fingerprints(
    sink: DiagnosticsCollector,
    *,
    view_nodes: Sequence[ViewNode],
) -> None:
    """Record policy-aware view fingerprints into diagnostics."""
    from datafusion_engine.lineage.diagnostics import (
        ensure_recorder_sink,
        view_fingerprint_payload,
    )

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
    from datafusion_engine.lineage.diagnostics import (
        ensure_recorder_sink,
        view_udf_parity_payload,
    )

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
    from datafusion_engine.lineage.diagnostics import (
        ensure_recorder_sink,
        rust_udf_snapshot_payload,
    )

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
    from datafusion_engine.lineage.diagnostics import ensure_recorder_sink

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
    from datafusion_engine.lineage.diagnostics import ensure_recorder_sink

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
    from datafusion_engine.lineage.diagnostics import ensure_recorder_sink

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
