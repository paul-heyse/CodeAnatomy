"""Top-level CPG build orchestrator.

Coordinates extraction, semantic IR compilation, spec building, Rust engine
execution, and auxiliary output writing. Replaces the Hamilton DAG-based
orchestration with direct function calls.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec
from opentelemetry import trace

from obs.otel.scopes import SCOPE_PIPELINE
from obs.otel.tracing import stage_span

if TYPE_CHECKING:
    from engine.spec_builder import SemanticExecutionSpec
    from extraction.orchestrator import ExtractionResult
    from semantics.compile_context import SemanticExecutionContext

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class BuildResult(msgspec.Struct, frozen=True):
    """Result of running the complete CPG build pipeline."""

    cpg_outputs: dict[str, str]
    auxiliary_outputs: dict[str, str]
    run_result: dict[str, object]
    extraction_timing: dict[str, float]
    warnings: list[dict[str, object]]


def orchestrate_build(  # noqa: PLR0913
    repo_root: Path,
    work_dir: Path,
    output_dir: Path,
    engine_profile: str = "medium",
    rulepack_profile: str = "default",
    *,
    runtime_config: object | None = None,
    extraction_config: dict[str, object] | None = None,
    include_errors: bool = True,
    include_manifest: bool = True,
    include_run_bundle: bool = False,
) -> BuildResult:
    """Orchestrate the complete CPG build from extraction to final outputs.

    Parameters
    ----------
    repo_root
        Repository root to analyze.
    work_dir
        Working directory for intermediate outputs (extraction Delta tables).
    output_dir
        Output directory for final CPG tables and auxiliary artifacts.
    engine_profile
        Engine execution profile, one of "small", "medium", "large".
    rulepack_profile
        Rulepack profile for semantic rules, one of "default", "strict", etc.
    runtime_config
        Optional runtime configuration for the Rust engine (reserved for future use).
    extraction_config
        Optional configuration for extraction (SCIP, tree-sitter, etc.).
    include_errors
        Whether to write extraction error artifacts.
    include_manifest
        Whether to write run manifest.
    include_run_bundle
        Whether to write full run bundle directory.

    Returns:
    -------
    BuildResult
        CPG outputs, auxiliary outputs, RunResult envelope, extraction timing, and warnings.
    """
    _ = runtime_config
    cpg_outputs: dict[str, str]
    auxiliary_outputs: dict[str, str]

    with stage_span("build_orchestrator", stage="orchestrator", scope_name=SCOPE_PIPELINE):
        extraction_result = _run_extraction_phase(
            repo_root, work_dir, extraction_config=extraction_config
        )
        _, spec = _compile_semantic_phase(extraction_result, engine_profile, rulepack_profile)
        run_result = _execute_engine_phase(extraction_result, spec, engine_profile)
        cpg_outputs = _collect_cpg_outputs(run_result, output_dir=output_dir)
        auxiliary_outputs = _write_auxiliary_outputs(
            output_dir=output_dir,
            spec=spec,
            run_result=run_result,
            extraction_result=extraction_result,
            include_errors=include_errors,
            include_manifest=include_manifest,
            include_run_bundle=include_run_bundle,
        )
        _record_observability(spec, run_result)
        warnings = _extract_warnings(run_result)

    return BuildResult(
        cpg_outputs=cpg_outputs,
        auxiliary_outputs=auxiliary_outputs,
        run_result=run_result,
        extraction_timing=extraction_result.timing,
        warnings=warnings,
    )


def _run_extraction_phase(
    repo_root: Path,
    work_dir: Path,
    *,
    extraction_config: dict[str, object] | None,
) -> ExtractionResult:
    from extraction.orchestrator import run_extraction

    with stage_span("extraction", stage="extraction", scope_name=SCOPE_PIPELINE):
        t_start = time.monotonic()
        scip_cfg = extraction_config.get("scip_index_config") if extraction_config else None
        scip_overrides = (
            extraction_config.get("scip_identity_overrides") if extraction_config else None
        )
        ts_raw = extraction_config.get("tree_sitter_enabled", True) if extraction_config else True
        ts_enabled: bool = ts_raw if isinstance(ts_raw, bool) else True
        mw_raw = extraction_config.get("max_workers", 6) if extraction_config else 6
        max_workers: int = mw_raw if isinstance(mw_raw, int) else 6
        extraction_result = run_extraction(
            repo_root=repo_root,
            work_dir=work_dir,
            scip_index_config=scip_cfg,
            scip_identity_overrides=scip_overrides,
            tree_sitter_enabled=ts_enabled,
            max_workers=max_workers,
        )
        elapsed = time.monotonic() - t_start
        logger.info(
            "Extraction complete in %.2fs (%d tables, %d errors)",
            elapsed,
            len(extraction_result.delta_locations),
            len(extraction_result.errors),
        )
        if extraction_result.errors:
            logger.warning("Extraction errors: %s", extraction_result.errors)
    return extraction_result


def _compile_semantic_phase(
    extraction_result: ExtractionResult,
    engine_profile: str,
    rulepack_profile: str,
) -> tuple[SemanticExecutionContext, SemanticExecutionSpec]:
    from engine.runtime_profile import resolve_runtime_profile
    from engine.spec_builder import build_execution_spec
    from semantics.compile_context import build_semantic_execution_context

    with stage_span("semantic_ir_compile", stage="semantic", scope_name=SCOPE_PIPELINE):
        t_ir_start = time.monotonic()
        runtime_profile = resolve_runtime_profile(engine_profile)
        ctx = runtime_profile.datafusion.session_context()

        execution_context = build_semantic_execution_context(
            runtime_profile=runtime_profile.datafusion,
            outputs=None,
            policy="schema_only",
            ctx=ctx,
            input_mapping=extraction_result.delta_locations,
        )
        ir_elapsed = time.monotonic() - t_ir_start
        logger.info(
            "Semantic IR compiled in %.2fs (%d views, %d join groups)",
            ir_elapsed,
            len(execution_context.manifest.semantic_ir.views),
            len(execution_context.manifest.semantic_ir.join_groups),
        )

    with stage_span("spec_build", stage="semantic", scope_name=SCOPE_PIPELINE):
        t_spec_start = time.monotonic()
        semantic_ir = execution_context.manifest.semantic_ir
        output_targets = _resolve_output_targets()
        spec = build_execution_spec(
            ir=semantic_ir,
            input_locations=extraction_result.delta_locations,
            output_targets=output_targets,
            rulepack_profile=rulepack_profile,
        )
        spec_elapsed = time.monotonic() - t_spec_start
        logger.info("Execution spec built in %.2fs", spec_elapsed)

    return execution_context, spec


def _execute_engine_phase(
    extraction_result: ExtractionResult,
    spec: SemanticExecutionSpec,
    engine_profile: str,
) -> dict[str, object]:
    from engine.facade import execute_cpg_build

    with stage_span("engine_execute", stage="engine", scope_name=SCOPE_PIPELINE):
        t_start = time.monotonic()
        run_result = execute_cpg_build(
            extraction_inputs=extraction_result.delta_locations,
            semantic_spec=spec,
            environment_class=engine_profile,
        )
        elapsed = time.monotonic() - t_start
        logger.info("Rust engine execution complete in %.2fs", elapsed)
    return run_result


def _record_observability(
    spec: SemanticExecutionSpec,
    run_result: dict[str, object],
) -> None:
    from obs.engine_artifacts import record_engine_plan_summary
    from obs.engine_metrics_bridge import record_engine_metrics

    with stage_span("observability", stage="orchestrator", scope_name=SCOPE_PIPELINE):
        record_engine_plan_summary(spec)
        record_engine_metrics(run_result)


def _resolve_output_targets() -> list[str]:
    from engine.output_contracts import ENGINE_CPG_OUTPUTS
    from semantics.naming import canonical_output_name

    return [canonical_output_name(name) for name in ENGINE_CPG_OUTPUTS]


def _collect_cpg_outputs(
    run_result: dict[str, object],
    *,
    output_dir: Path,
) -> dict[str, str]:
    with stage_span("collect_outputs", stage="orchestrator", scope_name=SCOPE_PIPELINE):
        _ = output_dir
        outputs: dict[str, str] = {}
        outputs_field = run_result.get("outputs")
        if not isinstance(outputs_field, (list, tuple)):
            return outputs

        for output in outputs_field:
            if not isinstance(output, dict):
                continue
            table_name = output.get("table_name")
            delta_location = output.get("delta_location")
            if not isinstance(table_name, str) or not isinstance(delta_location, str):
                continue
            outputs[table_name] = delta_location
        logger.info("Collected %d CPG outputs", len(outputs))
    return outputs


def _write_auxiliary_outputs(  # noqa: PLR0913
    *,
    output_dir: Path,
    spec: SemanticExecutionSpec,
    run_result: dict[str, object],
    extraction_result: ExtractionResult,
    include_errors: bool,
    include_manifest: bool,
    include_run_bundle: bool,
) -> dict[str, str]:
    with stage_span("auxiliary_outputs", stage="orchestrator", scope_name=SCOPE_PIPELINE):
        auxiliary: dict[str, str] = {}

        normalize_location = _write_normalize_outputs(
            output_dir=output_dir,
            spec=spec,
        )
        if normalize_location:
            auxiliary["write_normalize_outputs_delta"] = normalize_location

        if include_errors and extraction_result.errors:
            error_location = _write_extract_error_artifacts(
                output_dir=output_dir,
                errors=extraction_result.errors,
            )
            if error_location:
                auxiliary["write_extract_error_artifacts_delta"] = error_location

        if include_manifest:
            manifest_location = _write_run_manifest(
                output_dir=output_dir,
                spec=spec,
                run_result=run_result,
                extraction_result=extraction_result,
            )
            if manifest_location:
                auxiliary["write_run_manifest_delta"] = manifest_location

        if include_run_bundle:
            bundle_location = _write_run_bundle_dir(
                output_dir=output_dir,
                spec=spec,
                run_result=run_result,
                extraction_result=extraction_result,
            )
            if bundle_location:
                auxiliary["write_run_bundle_dir"] = bundle_location

        logger.info("Wrote %d auxiliary outputs", len(auxiliary))

    return auxiliary


def _write_normalize_outputs(
    *,
    output_dir: Path,
    spec: SemanticExecutionSpec,
) -> str | None:
    import deltalake
    import pyarrow as pa

    normalize_dir = output_dir / "normalize_outputs"
    normalize_dir.mkdir(parents=True, exist_ok=True)

    output_names = [view.name for view in spec.view_definitions]

    schema = pa.schema(
        [
            pa.field("event_time_unix_ms", pa.int64()),
            pa.field("output_dir", pa.string()),
            pa.field("outputs", pa.string()),
            pa.field("row_count", pa.int64()),
        ]
    )
    arrays = [
        pa.array([int(time.time() * 1000)]),
        pa.array([str(output_dir)]),
        pa.array([msgspec.json.encode(output_names).decode()]),
        pa.array([len(output_names)]),
    ]
    table = pa.table(dict(zip(schema.names, arrays, strict=False)), schema=schema)

    location = str(normalize_dir)
    deltalake.write_deltalake(location, table, mode="overwrite")
    logger.info("Wrote normalize outputs metadata to %s", location)
    return location


def _write_extract_error_artifacts(
    *,
    output_dir: Path,
    errors: list[dict[str, object]],
) -> str | None:
    import deltalake
    import pyarrow as pa

    if not errors:
        return None

    error_dir = output_dir / "extract_errors"
    error_dir.mkdir(parents=True, exist_ok=True)

    schema = pa.schema(
        [
            pa.field("extractor", pa.string()),
            pa.field("error", pa.string()),
        ]
    )
    arrays = [
        pa.array([str(err.get("extractor", "unknown")) for err in errors]),
        pa.array([str(err.get("error", "")) for err in errors]),
    ]
    table = pa.table(dict(zip(schema.names, arrays, strict=False)), schema=schema)

    location = str(error_dir)
    deltalake.write_deltalake(location, table, mode="overwrite")
    logger.info("Wrote %d extraction errors to %s", len(errors), location)
    return location


def _write_run_manifest(
    *,
    output_dir: Path,
    spec: SemanticExecutionSpec,
    run_result: dict[str, object],
    extraction_result: ExtractionResult,
) -> str | None:
    import deltalake
    import pyarrow as pa

    from utils.hashing import hash_msgpack_canonical

    manifest_dir = output_dir / "run_manifest"
    manifest_dir.mkdir(parents=True, exist_ok=True)

    spec_payload = msgspec.to_builtins(spec)
    spec_hash = hash_msgpack_canonical(spec_payload)
    run_result_hash = hash_msgpack_canonical(run_result)

    manifest_payload = {
        "spec_hash": spec_hash,
        "run_result_hash": run_result_hash,
        "extraction_delta_locations": extraction_result.delta_locations,
        "extraction_timing": extraction_result.timing,
        "extraction_errors": len(extraction_result.errors),
        "view_count": len(spec.view_definitions),
        "input_relation_count": len(spec.input_relations),
        "output_target_count": len(spec.output_targets),
    }

    schema = pa.schema(
        [
            pa.field("spec_hash", pa.string()),
            pa.field("run_result_hash", pa.string()),
            pa.field("extraction_delta_locations", pa.string()),
            pa.field("extraction_timing", pa.string()),
            pa.field("extraction_errors", pa.int64()),
            pa.field("view_count", pa.int64()),
            pa.field("input_relation_count", pa.int64()),
            pa.field("output_target_count", pa.int64()),
        ]
    )
    arrays = [
        pa.array([manifest_payload["spec_hash"]]),
        pa.array([manifest_payload["run_result_hash"]]),
        pa.array([msgspec.json.encode(manifest_payload["extraction_delta_locations"]).decode()]),
        pa.array([msgspec.json.encode(manifest_payload["extraction_timing"]).decode()]),
        pa.array([manifest_payload["extraction_errors"]]),
        pa.array([manifest_payload["view_count"]]),
        pa.array([manifest_payload["input_relation_count"]]),
        pa.array([manifest_payload["output_target_count"]]),
    ]
    table = pa.table(dict(zip(schema.names, arrays, strict=False)), schema=schema)

    location = str(manifest_dir)
    deltalake.write_deltalake(location, table, mode="overwrite")
    logger.info("Wrote run manifest to %s", location)
    return location


def _write_run_bundle_dir(
    *,
    output_dir: Path,
    spec: SemanticExecutionSpec,
    run_result: dict[str, object],
    extraction_result: ExtractionResult,
) -> str | None:
    bundle_dir = output_dir / "run_bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    spec_file = bundle_dir / "execution_spec.json"
    spec_file.write_text(msgspec.json.encode(msgspec.to_builtins(spec)).decode())

    result_file = bundle_dir / "run_result.json"
    result_file.write_text(msgspec.json.encode(run_result).decode())

    extraction_file = bundle_dir / "extraction.json"
    extraction_file.write_text(msgspec.json.encode(msgspec.to_builtins(extraction_result)).decode())

    logger.info("Wrote run bundle to %s", bundle_dir)
    return str(bundle_dir)


def _extract_warnings(run_result: dict[str, object]) -> list[dict[str, object]]:
    warnings_field = run_result.get("warnings")
    if not isinstance(warnings_field, (list, tuple)):
        return []
    return [dict(w) for w in warnings_field if isinstance(w, dict)]


__all__ = [
    "BuildResult",
    "orchestrate_build",
]
