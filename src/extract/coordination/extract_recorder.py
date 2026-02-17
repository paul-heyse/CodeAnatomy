"""Extraction diagnostics recording and post-write helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import pyarrow as pa

from extraction.diagnostics import EngineEventRecorder

if TYPE_CHECKING:
    from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
    from datafusion_engine.session.facade import ExecutionResult
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from serde_schema_registry import ArtifactSpec


def record_extract_artifact(
    recorder: EngineEventRecorder | None,
    *,
    name: ArtifactSpec,
    payload: Mapping[str, object],
) -> None:
    """Record extraction artifact payload when recorder is available."""
    if recorder is None:
        return
    recorder.record_artifact(name, dict(payload))


def record_extract_execution(
    name: str,
    result: ExecutionResult,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    """Record extract execution payload."""
    row_count: int | None = None
    table = result.table
    if table is not None:
        row_count = table.num_rows
    from datafusion_engine.lineage.diagnostics import record_artifact
    from serde_artifact_specs import EXTRACT_PLAN_EXECUTE_SPEC

    payload = {
        "dataset": name,
        "result_kind": result.kind.value,
        "rows": row_count,
    }
    record_artifact(runtime_profile, EXTRACT_PLAN_EXECUTE_SPEC, payload)


def record_extract_compile(
    name: str,
    plan: DataFusionPlanArtifact,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    """Record compile fingerprint artifact for extract plans."""
    from datafusion_engine.lineage.diagnostics import record_artifact
    from serde_artifact_specs import EXTRACT_PLAN_COMPILE_SPEC

    payload = {
        "dataset": name,
        "plan_fingerprint": plan.plan_fingerprint,
    }
    record_artifact(runtime_profile, EXTRACT_PLAN_COMPILE_SPEC, payload)


def record_extract_view_artifact(
    name: str,
    plan: DataFusionPlanArtifact,
    *,
    schema: pa.Schema,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    """Record deterministic view artifact for extract outputs."""
    from datafusion_engine.lineage.reporting import extract_lineage
    from datafusion_engine.session.runtime_session import (
        record_view_definition,
        session_runtime_hash,
    )
    from datafusion_engine.views.artifacts import (
        ViewArtifactLineage,
        ViewArtifactRequest,
        build_view_artifact_from_bundle,
    )

    lineage = extract_lineage(
        plan.optimized_logical_plan,
        udf_snapshot=plan.artifacts.udf_snapshot,
    )
    required_udfs = plan.required_udfs
    referenced_tables = lineage.referenced_tables
    runtime_hash = session_runtime_hash(runtime_profile.session_runtime())
    artifact = build_view_artifact_from_bundle(
        plan,
        request=ViewArtifactRequest(
            name=name,
            schema=schema,
            lineage=ViewArtifactLineage(
                required_udfs=required_udfs,
                referenced_tables=referenced_tables,
            ),
            runtime_hash=runtime_hash,
        ),
    )
    record_view_definition(runtime_profile, artifact=artifact)


def record_extract_udf_parity(
    name: str,
    *,
    runtime_profile: DataFusionRuntimeProfile,
) -> None:
    """Record extract-scoped UDF parity diagnostics."""
    from datafusion_engine.lineage.diagnostics import record_artifact
    from datafusion_engine.udf.parity import udf_parity_report
    from serde_artifact_specs import EXTRACT_UDF_PARITY_SPEC

    session_runtime = runtime_profile.session_runtime()
    report = udf_parity_report(session_runtime.ctx, snapshot=session_runtime.udf_snapshot)
    payload = report.payload()
    payload["dataset"] = name
    record_artifact(runtime_profile, EXTRACT_UDF_PARITY_SPEC, payload)


__all__ = [
    "record_extract_artifact",
    "record_extract_compile",
    "record_extract_execution",
    "record_extract_udf_parity",
    "record_extract_view_artifact",
]
