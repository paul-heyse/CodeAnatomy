"""Top-level CPG build orchestrator.

Coordinates extraction, semantic IR compilation, spec building, Rust engine
execution, and auxiliary output writing. Replaces the Hamilton DAG-based
orchestration with direct function calls.
"""

from __future__ import annotations

import importlib
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import msgspec
from opentelemetry import trace

from graph.contracts import OrchestrateBuildRequestV1
from obs.otel import SCOPE_PIPELINE, emit_diagnostics_event, stage_span
from planning_engine.output_contracts import (
    CPG_OUTPUT_CANONICAL_TO_LEGACY,
    ENGINE_CPG_OUTPUTS,
    canonical_cpg_output_name,
)

if TYPE_CHECKING:
    from extraction.orchestrator import ExtractionResult
    from planning_engine.spec_contracts import SemanticExecutionSpec
    from semantics.compile_context import SemanticExecutionContext

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


@dataclass(frozen=True)
class _AuxiliaryOutputOptions:
    include_errors: bool
    include_manifest: bool
    include_run_bundle: bool


class BuildResult(msgspec.Struct, frozen=True):
    """Result of running the complete CPG build pipeline."""

    cpg_outputs: dict[str, dict[str, object]]
    auxiliary_outputs: dict[str, dict[str, object]]
    run_result: dict[str, object]
    extraction_timing: dict[str, float]
    warnings: list[dict[str, object]]


def _raise_typed_engine_boundary_error(exc: Exception) -> None:
    """Preserve typed Rust boundary errors and normalize unknown failures.

    Raises:
        RuntimeError: If the exception does not expose typed engine fields.
    """
    stage = getattr(exc, "stage", None)
    code = getattr(exc, "code", None)
    if isinstance(stage, str) and isinstance(code, str):
        raise exc
    raise RuntimeError(str(exc)) from exc


def _spec_payload_with_input_overrides(
    *,
    semantic_spec: SemanticExecutionSpec,
    extraction_inputs: dict[str, str],
) -> dict[str, object]:
    payload_raw = msgspec.to_builtins(semantic_spec)
    if not isinstance(payload_raw, dict):
        msg = "SemanticExecutionSpec payload must serialize to a mapping"
        raise TypeError(msg)
    payload: dict[str, object] = dict(payload_raw)
    payload.pop("spec_hash", None)
    raw_relations = payload.get("input_relations", [])
    relations: list[dict[str, object]] = []
    if isinstance(raw_relations, (list, tuple)):
        relations.extend(dict(relation) for relation in raw_relations if isinstance(relation, dict))
    seen: set[str] = set()
    for relation in relations:
        logical_name = relation.get("logical_name")
        if isinstance(logical_name, str):
            seen.add(logical_name)
            if logical_name in extraction_inputs:
                relation["delta_location"] = extraction_inputs[logical_name]
    for logical_name, delta_location in extraction_inputs.items():
        if logical_name in seen:
            continue
        relations.append(
            {
                "logical_name": logical_name,
                "delta_location": delta_location,
                "requires_lineage": False,
                "version_pin": None,
            }
        )
    payload["input_relations"] = relations
    return payload


def _validate_output_targets(semantic_spec: SemanticExecutionSpec) -> None:
    output_targets = semantic_spec.output_targets
    if not output_targets:
        msg = "SemanticExecutionSpec must define at least one output target."
        raise ValueError(msg)
    missing_locations = [
        target.table_name
        for target in output_targets
        if not isinstance(target.delta_location, str) or not target.delta_location
    ]
    if missing_locations:
        joined = ", ".join(sorted(missing_locations))
        msg = f"Missing output delta locations for targets: {joined}"
        raise ValueError(msg)


def orchestrate_build(request: OrchestrateBuildRequestV1) -> BuildResult:
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
    repo_root = Path(request.repo_root)
    work_dir = Path(request.work_dir)
    output_dir = Path(request.output_dir)
    cpg_outputs: dict[str, dict[str, object]]
    auxiliary_outputs: dict[str, dict[str, object]]

    with stage_span("build_orchestrator", stage="orchestrator", scope_name=SCOPE_PIPELINE):
        extraction_result = _run_extraction_phase(
            repo_root,
            work_dir,
            extraction_config=request.extraction_config,
        )
        semantic_inputs = (
            extraction_result.semantic_input_locations
            if extraction_result.semantic_input_locations
            else extraction_result.delta_locations
        )
        _, spec = _compile_semantic_phase(
            semantic_input_locations=semantic_inputs,
            engine_profile=request.engine_profile,
            rulepack_profile=request.rulepack_profile,
            output_dir=output_dir,
            runtime_config=request.runtime_config,
        )
        run_result, run_artifacts = _execute_engine_phase(
            semantic_inputs,
            spec,
            request.engine_profile,
        )
        cpg_outputs = _collect_cpg_outputs(run_result, output_dir=output_dir)
        auxiliary_output_options = _AuxiliaryOutputOptions(
            include_errors=request.include_errors,
            include_manifest=request.include_manifest,
            include_run_bundle=request.include_run_bundle,
        )
        auxiliary_outputs = _collect_auxiliary_outputs(
            output_dir=output_dir,
            artifacts=run_artifacts,
            extraction_result=extraction_result,
            options=auxiliary_output_options,
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
    from extraction.contracts import RunExtractionRequestV1
    from extraction.options import normalize_extraction_options
    from extraction.orchestrator import run_extraction

    with stage_span("extraction", stage="extraction", scope_name=SCOPE_PIPELINE):
        t_start = time.monotonic()
        scip_cfg = extraction_config.get("scip_index_config") if extraction_config else None
        scip_overrides = (
            extraction_config.get("scip_identity_overrides") if extraction_config else None
        )
        run_options = normalize_extraction_options(extraction_config)
        extraction_result = run_extraction(
            RunExtractionRequestV1(
                repo_root=str(repo_root),
                work_dir=str(work_dir),
                scip_index_config=scip_cfg,
                scip_identity_overrides=scip_overrides,
                tree_sitter_enabled=run_options.tree_sitter_enabled,
                max_workers=run_options.max_workers,
                options=msgspec.to_builtins(run_options),
            )
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
    semantic_input_locations: dict[str, str],
    engine_profile: str,
    rulepack_profile: str,
    *,
    output_dir: Path,
    runtime_config: object | None,
) -> tuple[SemanticExecutionContext, SemanticExecutionSpec]:
    execution_context = _build_semantic_execution_context(
        semantic_input_locations=semantic_input_locations,
        engine_profile=engine_profile,
    )
    spec = _build_semantic_execution_spec(
        semantic_input_locations=semantic_input_locations,
        rulepack_profile=rulepack_profile,
        output_dir=output_dir,
        runtime_config=runtime_config,
        semantic_execution_context=execution_context,
    )
    return execution_context, spec


def _build_semantic_execution_context(
    *,
    semantic_input_locations: dict[str, str],
    engine_profile: str,
) -> SemanticExecutionContext:
    from extraction.runtime_profile import resolve_runtime_profile
    from semantics.compile_context import build_semantic_execution_context

    with stage_span("semantic_ir_compile", stage="semantic", scope_name=SCOPE_PIPELINE):
        t_start = time.monotonic()
        runtime_profile = resolve_runtime_profile(engine_profile)
        execution_context = build_semantic_execution_context(
            runtime_profile=runtime_profile.datafusion,
            outputs=None,
            policy="schema_only",
            ctx=runtime_profile.datafusion.session_context(),
            input_mapping=semantic_input_locations,
        )
        ir_elapsed = time.monotonic() - t_start
        logger.info(
            "Semantic IR compiled in %.2fs (%d views, %d join groups)",
            ir_elapsed,
            len(execution_context.manifest.semantic_ir.views),
            len(execution_context.manifest.semantic_ir.join_groups),
        )
    return execution_context


def _build_semantic_execution_spec(
    *,
    semantic_input_locations: dict[str, str],
    rulepack_profile: str,
    output_dir: Path,
    runtime_config: object | None,
    semantic_execution_context: SemanticExecutionContext,
) -> SemanticExecutionSpec:
    from planning_engine.spec_contracts import SemanticExecutionSpec

    with stage_span("spec_build", stage="semantic", scope_name=SCOPE_PIPELINE):
        t_spec_start = time.monotonic()
        engine_module = _load_codeanatomy_engine_module()
        request_payload = _build_semantic_spec_request_payload(
            semantic_input_locations=semantic_input_locations,
            rulepack_profile=rulepack_profile,
            output_dir=output_dir,
            runtime_config=runtime_config,
        )
        compiler = engine_module.SemanticPlanCompiler()
        from serde_msgspec import to_builtins

        spec_json = compiler.build_spec_json(
            msgspec.json.encode(
                to_builtins(semantic_execution_context.manifest.semantic_ir, str_keys=True)
            ).decode(),
            msgspec.json.encode(request_payload).decode(),
        )
        spec = msgspec.json.decode(spec_json, type=SemanticExecutionSpec)
        spec_elapsed = time.monotonic() - t_spec_start
        logger.info("Execution spec built in %.2fs", spec_elapsed)
    return spec


def _load_codeanatomy_engine_module() -> Any:
    try:
        return importlib.import_module("codeanatomy_engine")
    except ImportError as exc:
        msg = (
            "codeanatomy_engine Rust extension not built. "
            "Run: bash scripts/rebuild_rust_artifacts.sh"
        )
        raise ImportError(msg) from exc


def _build_semantic_spec_request_payload(
    *,
    semantic_input_locations: dict[str, str],
    rulepack_profile: str,
    output_dir: Path,
    runtime_config: object | None,
) -> dict[str, object]:
    from planning_engine.spec_contracts import RuntimeConfig
    from serde_msgspec import to_builtins

    output_targets = _resolve_output_targets()
    output_locations = _resolve_output_locations(output_dir, output_targets)
    runtime = runtime_config if isinstance(runtime_config, RuntimeConfig) else None
    return {
        "input_locations": semantic_input_locations,
        "output_targets": output_targets,
        "rulepack_profile": rulepack_profile,
        "output_locations": output_locations,
        "runtime": to_builtins(runtime, str_keys=True) if runtime is not None else None,
    }


def _execute_engine_phase(
    semantic_input_locations: dict[str, str],
    spec: SemanticExecutionSpec,
    engine_profile: str,
) -> tuple[dict[str, object], dict[str, object]]:
    with stage_span("engine_execute", stage="engine", scope_name=SCOPE_PIPELINE):
        t_start = time.monotonic()
        try:
            engine_module = importlib.import_module("codeanatomy_engine")
        except ImportError:
            msg = (
                "codeanatomy_engine Rust extension not built. "
                "Run: bash scripts/rebuild_rust_artifacts.sh"
            )
            raise ImportError(msg) from None
        _validate_output_targets(spec)
        spec_payload = _spec_payload_with_input_overrides(
            semantic_spec=spec,
            extraction_inputs=semantic_input_locations,
        )
        spec_json = msgspec.json.encode(spec_payload).decode()
        runtime_payload = spec_payload.get("runtime")
        request_payload = {
            "engine_profile": engine_profile,
            "spec_json": spec_json,
            "runtime": runtime_payload if isinstance(runtime_payload, dict) else None,
            "orchestration": {
                "include_manifest": True,
                "include_run_bundle": False,
                "emit_auxiliary_outputs": True,
            },
        }
        response: object | None = None
        try:
            response = engine_module.run_build(msgspec.json.encode(request_payload).decode())
        except (RuntimeError, TypeError, ValueError) as exc:
            _raise_typed_engine_boundary_error(exc)
        if response is None:
            msg = "Rust run_build returned no response payload"
            raise RuntimeError(msg)
        response_payload = response if isinstance(response, dict) else {}
        run_payload = response_payload.get("run_result")
        run_result = run_payload if isinstance(run_payload, dict) else {}
        artifacts_payload = response_payload.get("artifacts")
        artifacts = artifacts_payload if isinstance(artifacts_payload, dict) else {}
        elapsed = time.monotonic() - t_start
        logger.info("Rust engine execution complete in %.2fs", elapsed)
    return run_result, artifacts


def _record_observability(
    spec: SemanticExecutionSpec,
    run_result: dict[str, object],
) -> None:
    from obs.engine_artifacts import record_engine_execution_summary, record_engine_plan_summary
    from obs.engine_metrics_bridge import record_engine_metrics

    with stage_span("observability", stage="orchestrator", scope_name=SCOPE_PIPELINE):
        plan_summary = record_engine_plan_summary(spec)
        emit_diagnostics_event(
            "engine_spec_summary_v1",
            payload={
                "spec_hash": plan_summary.spec_hash,
                "view_count": plan_summary.view_count,
                "view_names": [view.name for view in spec.view_definitions],
                "join_edge_count": plan_summary.join_edge_count,
                "rule_intent_count": plan_summary.rule_intent_count,
                "rulepack_profile": plan_summary.rulepack_profile,
                "input_relation_count": plan_summary.input_relation_count,
                "output_target_count": plan_summary.output_target_count,
            },
            event_kind="event",
        )
        execution_summary = record_engine_execution_summary(run_result)
        emit_diagnostics_event(
            "engine_execution_summary_v1",
            payload=msgspec.to_builtins(execution_summary),
            event_kind="event",
        )
        outputs_field = run_result.get("outputs")
        if isinstance(outputs_field, (list, tuple)):
            for output in outputs_field:
                if isinstance(output, dict):
                    emit_diagnostics_event(
                        "engine_output_v1",
                        payload={
                            "table_name": output.get("table_name"),
                            "delta_location": output.get("delta_location"),
                            "rows_written": output.get("rows_written"),
                            "partition_count": output.get("partition_count"),
                        },
                        event_kind="event",
                    )
        record_engine_metrics(run_result)


def _resolve_output_targets() -> list[str]:
    return list(ENGINE_CPG_OUTPUTS)


def _resolve_output_locations(output_dir: Path, output_targets: list[str]) -> dict[str, str]:
    return {target: str(output_dir / target) for target in output_targets}


def _collect_cpg_outputs(
    run_result: dict[str, object],
    *,
    output_dir: Path,
) -> dict[str, dict[str, object]]:
    with stage_span("collect_outputs", stage="orchestrator", scope_name=SCOPE_PIPELINE):
        outputs_field = run_result.get("outputs")
        if not isinstance(outputs_field, (list, tuple)):
            return {}

        outputs = {}
        for output in outputs_field:
            resolved = _build_cpg_output_payload(output=output, output_dir=output_dir)
            if resolved is None:
                continue
            output_name, payload = resolved
            outputs[output_name] = payload
            legacy_name = CPG_OUTPUT_CANONICAL_TO_LEGACY.get(output_name)
            if legacy_name is not None:
                outputs[legacy_name] = payload
        logger.info("Collected %d CPG outputs", len(outputs))
    return outputs


def _collect_auxiliary_outputs(
    *,
    output_dir: Path,
    artifacts: dict[str, object],
    extraction_result: ExtractionResult,
    options: _AuxiliaryOutputOptions,
) -> dict[str, dict[str, object]]:
    run_result: dict[str, object] = {}
    _ = run_result
    with stage_span("auxiliary_outputs", stage="orchestrator", scope_name=SCOPE_PIPELINE):
        auxiliary: dict[str, dict[str, object]] = {}
        paths = _artifact_path_map(artifacts, output_dir=output_dir)

        normalize_location = paths.get("normalize_outputs_delta")
        if isinstance(normalize_location, str) and normalize_location:
            payload: dict[str, object] = {"path": normalize_location}
            auxiliary["normalize_outputs_delta"] = payload
            auxiliary["write_normalize_outputs_delta"] = payload

        if options.include_errors:
            error_location = paths.get("extract_error_artifacts_delta")
            if isinstance(error_location, str) and error_location:
                payload: dict[str, object] = {
                    "path": error_location,
                    "rows": len(extraction_result.errors),
                }
                auxiliary["extract_error_artifacts_delta"] = payload
                auxiliary["write_extract_error_artifacts_delta"] = payload

        if options.include_manifest:
            manifest_location = paths.get("run_manifest_delta")
            if isinstance(manifest_location, str) and manifest_location:
                payload: dict[str, object] = {"path": manifest_location}
                auxiliary["run_manifest_delta"] = payload
                auxiliary["write_run_manifest_delta"] = payload

        if options.include_run_bundle:
            bundle_location = paths.get("run_bundle_dir")
            if isinstance(bundle_location, str) and bundle_location:
                payload: dict[str, object] = {
                    "bundle_dir": bundle_location,
                    "run_id": Path(bundle_location).name,
                }
                auxiliary["run_bundle_dir"] = payload
                auxiliary["write_run_bundle_dir"] = payload

        logger.info("Collected %d auxiliary outputs from Rust artifacts", len(auxiliary))

    return auxiliary


def _build_cpg_output_payload(
    output: object,
    *,
    output_dir: Path,
) -> tuple[str, dict[str, object]] | None:
    if not isinstance(output, dict):
        return None
    table_name = output.get("table_name")
    if not isinstance(table_name, str):
        return None

    canonical_name = canonical_cpg_output_name(table_name)
    if canonical_name not in ENGINE_CPG_OUTPUTS:
        return None

    delta_location = output.get("delta_location")
    location = (
        delta_location
        if isinstance(delta_location, str) and delta_location
        else str(output_dir / canonical_name)
    )
    rows = output.get("rows_written", 0)
    row_count = int(rows) if isinstance(rows, (int, float)) else 0
    return canonical_name, {
        "table_name": canonical_name,
        "path": location,
        "rows": row_count,
        "rows_written": row_count,
        "partition_count": output.get("partition_count"),
        "delta_version": output.get("delta_version"),
        "files_added": output.get("files_added"),
        "bytes_written": output.get("bytes_written"),
        "error_rows": 0,
        "paths": {
            "data": location,
            "errors": str(Path(location) / "_errors"),
            "stats": str(Path(location) / "_stats"),
            "alignment": str(Path(location) / "_alignment"),
        },
    }


def _artifact_path_map(
    artifacts: dict[str, object],
    *,
    output_dir: Path,
) -> dict[str, str]:
    paths: dict[str, str] = {}
    auxiliary = artifacts.get("auxiliary_outputs")
    if isinstance(auxiliary, dict):
        paths.update(
            {
                key: value
                for key, value in auxiliary.items()
                if isinstance(key, str) and isinstance(value, str) and value
            }
        )
    manifest_path = artifacts.get("manifest_path")
    if isinstance(manifest_path, str) and manifest_path:
        paths.setdefault("run_manifest_delta", manifest_path)
    run_bundle_dir = artifacts.get("run_bundle_dir")
    if isinstance(run_bundle_dir, str) and run_bundle_dir:
        paths.setdefault("run_bundle_dir", run_bundle_dir)
    # Compatibility fallback while the Rust contract rollout is in-flight.
    paths.setdefault("normalize_outputs_delta", str(output_dir / "normalize_outputs"))
    paths.setdefault("run_manifest_delta", str(output_dir / "run_manifest"))
    paths.setdefault("run_bundle_dir", str(output_dir / "run_bundle"))
    return paths


def _extract_warnings(run_result: dict[str, object]) -> list[dict[str, object]]:
    warnings_field = run_result.get("warnings")
    if not isinstance(warnings_field, (list, tuple)):
        return []
    return [dict(w) for w in warnings_field if isinstance(w, dict)]


__all__ = [
    "BuildResult",
    "orchestrate_build",
]
