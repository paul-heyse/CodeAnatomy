"""Top-level CPG build orchestrator.

Coordinates extraction, semantic IR compilation, spec building, Rust engine
execution, and auxiliary output writing. Replaces the Hamilton DAG-based
orchestration with direct function calls.
"""

from __future__ import annotations

import importlib
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn

import msgspec
from opentelemetry import trace

from obs.otel.logs import emit_diagnostics_event
from obs.otel.scopes import SCOPE_PIPELINE
from obs.otel.tracing import stage_span
from planning_engine.output_contracts import (
    CPG_OUTPUT_CANONICAL_TO_LEGACY,
    ENGINE_CPG_OUTPUTS,
    canonical_cpg_output_name,
)

if TYPE_CHECKING:
    from extraction.orchestrator import ExtractionResult
    from planning_engine.spec_builder import SemanticExecutionSpec
    from semantics.compile_context import SemanticExecutionContext

logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class _EngineError(RuntimeError):
    """Base error for Rust engine execution failures."""


class _EngineValidationError(_EngineError):
    """Spec validation or contract mismatch failure."""


class _EngineCompileError(_EngineError):
    """Logical plan compilation failure."""


class _EngineRuleViolationError(_EngineError):
    """Rulepack validation or policy rule violation."""


class _EngineRuntimeError(_EngineError):
    """Runtime execution or materialization failure."""


_ERROR_STAGE_MAP: dict[str, type[_EngineError]] = {
    "validation": _EngineValidationError,
    "compilation": _EngineCompileError,
    "rule_violation": _EngineRuleViolationError,
    "runtime": _EngineRuntimeError,
    "materialization": _EngineRuntimeError,
}


class BuildResult(msgspec.Struct, frozen=True):
    """Result of running the complete CPG build pipeline."""

    cpg_outputs: dict[str, dict[str, object]]
    auxiliary_outputs: dict[str, dict[str, object]]
    run_result: dict[str, object]
    extraction_timing: dict[str, float]
    warnings: list[dict[str, object]]


def _raise_engine_error(exc: Exception) -> NoReturn:
    """Classify and raise a typed local engine error from Rust exceptions.

    Raises:
        _EngineValidationError: For spec parsing/validation failures.
        _EngineCompileError: For compile-stage failures.
        _EngineRuleViolationError: For policy/rule violations.
        _EngineRuntimeError: For runtime/materialization failures.
        _EngineError: Fallback for unmapped engine exceptions.
    """
    error_stage = getattr(exc, "error_stage", None)
    if isinstance(error_stage, str) and error_stage in _ERROR_STAGE_MAP:
        error_cls = _ERROR_STAGE_MAP[error_stage]
        raise error_cls(str(exc)) from exc

    message = str(exc)
    lowered = message.lower()
    if "invalid spec json" in lowered or "failed to parse spec" in lowered:
        raise _EngineValidationError(message) from exc
    if "failed to compile plans" in lowered:
        if "rule" in lowered or "policy" in lowered or "safety" in lowered:
            raise _EngineRuleViolationError(message) from exc
        raise _EngineCompileError(message) from exc
    if "failed to build session" in lowered or "failed to register inputs" in lowered:
        raise _EngineRuntimeError(message) from exc
    if "materialization failed" in lowered:
        raise _EngineRuntimeError(message) from exc
    if isinstance(exc, ValueError):
        raise _EngineValidationError(message) from exc
    raise _EngineError(message) from exc


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
        raise _EngineValidationError(msg)
    missing_locations = [
        target.table_name
        for target in output_targets
        if not isinstance(target.delta_location, str) or not target.delta_location
    ]
    if missing_locations:
        joined = ", ".join(sorted(missing_locations))
        msg = f"Missing output delta locations for targets: {joined}"
        raise _EngineValidationError(msg)


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
    cpg_outputs: dict[str, dict[str, object]]
    auxiliary_outputs: dict[str, dict[str, object]]

    with stage_span("build_orchestrator", stage="orchestrator", scope_name=SCOPE_PIPELINE):
        extraction_result = _run_extraction_phase(
            repo_root, work_dir, extraction_config=extraction_config
        )
        semantic_inputs = (
            extraction_result.semantic_input_locations
            if extraction_result.semantic_input_locations
            else extraction_result.delta_locations
        )
        _, spec = _compile_semantic_phase(
            semantic_input_locations=semantic_inputs,
            engine_profile=engine_profile,
            rulepack_profile=rulepack_profile,
            output_dir=output_dir,
            runtime_config=runtime_config,
        )
        run_result = _execute_engine_phase(semantic_inputs, spec, engine_profile)
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
            repo_root=repo_root,
            work_dir=work_dir,
            scip_index_config=scip_cfg,
            scip_identity_overrides=scip_overrides,
            tree_sitter_enabled=run_options.tree_sitter_enabled,
            max_workers=run_options.max_workers,
            options=run_options,
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
    from planning_engine.runtime_profile import resolve_runtime_profile
    from planning_engine.spec_builder import RuntimeConfig, build_execution_spec
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
            input_mapping=semantic_input_locations,
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
        output_locations = _resolve_output_locations(output_dir, output_targets)
        resolved_runtime = runtime_config if isinstance(runtime_config, RuntimeConfig) else None
        spec = build_execution_spec(
            ir=semantic_ir,
            input_locations=semantic_input_locations,
            output_targets=output_targets,
            rulepack_profile=rulepack_profile,
            runtime_config=resolved_runtime,
            output_locations=output_locations,
        )
        spec_elapsed = time.monotonic() - t_spec_start
        logger.info("Execution spec built in %.2fs", spec_elapsed)

    return execution_context, spec


def _execute_engine_phase(
    semantic_input_locations: dict[str, str],
    spec: SemanticExecutionSpec,
    engine_profile: str,
) -> dict[str, object]:
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
        session_factory_cls = engine_module.SessionFactory
        semantic_plan_compiler_cls = engine_module.SemanticPlanCompiler
        cpg_materializer_cls = engine_module.CpgMaterializer

        _validate_output_targets(spec)
        spec_payload = _spec_payload_with_input_overrides(
            semantic_spec=spec,
            extraction_inputs=semantic_input_locations,
        )
        spec_json = msgspec.json.encode(spec_payload).decode()
        factory = session_factory_cls.from_class(engine_profile)
        compiler = semantic_plan_compiler_cls()
        try:
            compiled = compiler.compile(spec_json)
        except Exception as exc:  # noqa: BLE001 - normalize engine boundary errors
            _raise_engine_error(exc)

        materializer = cpg_materializer_cls()
        try:
            result = materializer.execute(factory, compiled)
        except Exception as exc:  # noqa: BLE001 - normalize engine boundary errors
            _raise_engine_error(exc)
        run_result = msgspec.json.decode(result.to_json(), type=dict[str, object])
        elapsed = time.monotonic() - t_start
        logger.info("Rust engine execution complete in %.2fs", elapsed)
    return run_result


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
        outputs: dict[str, dict[str, object]] = {}
        outputs_field = run_result.get("outputs")
        if not isinstance(outputs_field, (list, tuple)):
            return outputs

        for output in outputs_field:
            if not isinstance(output, dict):
                continue
            table_name = output.get("table_name")
            delta_location = output.get("delta_location")
            if not isinstance(table_name, str):
                continue
            canonical_name = canonical_cpg_output_name(table_name)
            if canonical_name not in ENGINE_CPG_OUTPUTS:
                continue
            location = (
                delta_location
                if isinstance(delta_location, str) and delta_location
                else str(output_dir / canonical_name)
            )
            rows = output.get("rows_written", 0)
            row_count = int(rows) if isinstance(rows, (int, float)) else 0
            payload: dict[str, object] = {
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
            outputs[canonical_name] = payload
            legacy = CPG_OUTPUT_CANONICAL_TO_LEGACY.get(canonical_name)
            if legacy is not None:
                outputs[legacy] = payload
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
) -> dict[str, dict[str, object]]:
    with stage_span("auxiliary_outputs", stage="orchestrator", scope_name=SCOPE_PIPELINE):
        auxiliary: dict[str, dict[str, object]] = {}

        normalize_location = _write_normalize_outputs(
            output_dir=output_dir,
            spec=spec,
        )
        if normalize_location:
            auxiliary["write_normalize_outputs_delta"] = {"path": normalize_location}

        if include_errors and extraction_result.errors:
            error_location = _write_extract_error_artifacts(
                output_dir=output_dir,
                errors=extraction_result.errors,
            )
            if error_location:
                auxiliary["write_extract_error_artifacts_delta"] = {
                    "path": error_location,
                    "rows": len(extraction_result.errors),
                }

        if include_manifest:
            manifest_location = _write_run_manifest(
                output_dir=output_dir,
                spec=spec,
                run_result=run_result,
                extraction_result=extraction_result,
            )
            if manifest_location:
                auxiliary["write_run_manifest_delta"] = {"path": manifest_location}

        if include_run_bundle:
            bundle_location = _write_run_bundle_dir(
                output_dir=output_dir,
                spec=spec,
                run_result=run_result,
                extraction_result=extraction_result,
            )
            if bundle_location:
                auxiliary["write_run_bundle_dir"] = {
                    "bundle_dir": bundle_location,
                    "run_id": Path(bundle_location).name,
                }

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
