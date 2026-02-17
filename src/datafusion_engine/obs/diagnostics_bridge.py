"""Engine-side diagnostics helpers relocated from obs package."""

from __future__ import annotations

from datafusion_engine.views.artifacts import DataFusionViewArtifact
from obs.diagnostics import (
    DiagnosticsCollector,
    record_dataframe_validation_error,
    record_rust_udf_snapshot,
    record_view_contract_violations,
    record_view_fingerprints,
    record_view_udf_parity,
)

__all__ = [
    "DiagnosticsCollector",
    "record_dataframe_validation_error",
    "record_rust_udf_snapshot",
    "record_view_contract_violations",
    "record_view_fingerprints",
    "record_view_udf_parity",
]


def record_view_artifact(sink: DiagnosticsCollector, *, artifact: DataFusionViewArtifact) -> None:
    """Record a deterministic view artifact payload."""
    from obs.diagnostics import record_view_artifact as _record_view_artifact

    _record_view_artifact(sink, artifact=artifact)
