"""Execution plan inspection command."""

from __future__ import annotations

import importlib
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal

from cyclopts import Parameter, validators

from cli.context import RunContext
from cli.groups import execution_group

if TYPE_CHECKING:
    from planning_engine.spec_contracts import SemanticExecutionSpec


@dataclass(frozen=True)
class PlanOptions:
    """CLI options for plan command."""

    show_graph: Annotated[
        bool,
        Parameter(help="Display task dependency graph (Tier 2: requires Rust introspection API)."),
    ] = False
    show_schedule: Annotated[
        bool,
        Parameter(
            help="Display computed execution schedule (Tier 2: requires Rust introspection API)."
        ),
    ] = False
    show_inferred_deps: Annotated[
        bool,
        Parameter(
            help="Display inferred task dependencies (Tier 2: requires Rust introspection API)."
        ),
    ] = False
    show_task_graph: Annotated[
        bool,
        Parameter(
            help="Display task graph structure with edges (Tier 3: requires Rust introspection API)."
        ),
    ] = False
    validate: Annotated[
        bool,
        Parameter(
            help="Validate plan consistency and report issues (Tier 3: requires Rust introspection API)."
        ),
    ] = False
    output_format: Annotated[
        Literal["text", "json", "dot"],
        Parameter(
            help="Output format for plan display.",
            env_var="CODEANATOMY_PLAN_OUTPUT_FORMAT",
        ),
    ] = "text"
    output_file: Annotated[
        Path | None,
        Parameter(
            name="-o",
            help="Write plan to file instead of stdout.",
            env_var="CODEANATOMY_PLAN_OUTPUT_FILE",
        ),
    ] = None
    engine_profile: Annotated[
        Literal["small", "medium", "large"],
        Parameter(
            name="--engine-profile",
            help="Engine resource profile (affects plan shape).",
            group=execution_group,
        ),
    ] = "medium"
    rulepack_profile: Annotated[
        Literal["Default", "LowLatency", "Replay", "Strict"],
        Parameter(
            name="--rulepack-profile",
            help="Rulepack profile for plan compilation.",
            group=execution_group,
        ),
    ] = "Default"


_DEFAULT_PLAN_OPTIONS = PlanOptions()


@dataclass(frozen=True)
class _PlanPayloadOptions:
    show_graph: bool
    show_schedule: bool
    show_inferred_deps: bool
    show_task_graph: bool
    validate: bool
    output_format: Literal["text", "json", "dot"]


@dataclass(frozen=True)
class _PlanRuntimeMetadata:
    engine_profile: str
    runtime_profile_name: str
    runtime_profile_hash: str


def plan_command(
    repo_root: Annotated[
        Path,
        Parameter(validator=validators.Path(exists=True, dir_okay=True)),
    ],
    options: Annotated[PlanOptions, Parameter(name="*")] = _DEFAULT_PLAN_OPTIONS,
    *,
    run_context: Annotated[RunContext | None, Parameter(parse=False)] = None,
) -> int:
    """Show the computed execution plan without running it.

    Tier 1 parity (available now):
      - Spec hash, view count, output target list, high-level graph summary

    Tier 2/3 features (require Rust introspection API):
      - show_graph, show_schedule, show_inferred_deps, show_task_graph, validate

    Returns:
    -------
    int
        Exit status code.
    """
    _ = run_context
    resolved_root = repo_root.resolve()
    try:
        payload = _build_plan_payload(resolved_root=resolved_root, options=options)
    except RuntimeError as exc:
        sys.stderr.write(f"{exc}\n")
        return 1

    _write_payload(payload, options.output_file, options.output_format)
    return 0


def _load_plan_engine_module() -> Any:
    try:
        return importlib.import_module("codeanatomy_engine")
    except ImportError as exc:
        msg = (
            "codeanatomy_engine Rust extension not built. "
            "Run: bash scripts/rebuild_rust_artifacts.sh"
        )
        error_message = "Error: " + msg
        raise RuntimeError(error_message) from exc


def _load_plan_session_factory(*, engine_module: Any, profile: str) -> object:
    try:
        session_factory_cls = engine_module.SessionFactory
    except AttributeError as exc:
        error_message = f"Plan runtime initialization failed: {exc}"
        raise RuntimeError(error_message) from exc

    try:
        return session_factory_cls.from_class(profile)
    except (TypeError, ValueError, RuntimeError) as exc:
        error_message = f"Plan runtime initialization failed: {exc}"
        raise RuntimeError(error_message) from exc


def _build_plan_request_payload(
    *,
    output_targets: list[str],
    output_root: Path,
    rulepack_profile: str,
) -> dict[str, object]:
    output_locations = {name: str(output_root / "build" / name) for name in output_targets}
    return {
        "input_locations": {},
        "output_targets": output_targets,
        "rulepack_profile": rulepack_profile,
        "output_locations": output_locations,
        "runtime": None,
    }


def _build_plan_payload(*, resolved_root: Path, options: PlanOptions) -> object:
    import msgspec

    from planning_engine.output_contracts import ENGINE_CPG_OUTPUTS
    from planning_engine.spec_contracts import SemanticExecutionSpec
    from semantics.ir_pipeline import build_semantic_ir
    from serde_msgspec import to_builtins

    runtime_metadata = _PlanRuntimeMetadata(
        engine_profile=options.engine_profile,
        runtime_profile_name=options.engine_profile,
        runtime_profile_hash="rust_session_factory",
    )
    engine_module = _load_plan_engine_module()
    session_factory = _load_plan_session_factory(
        engine_module=engine_module,
        profile=options.engine_profile,
    )
    ir = build_semantic_ir()
    output_targets = list(ENGINE_CPG_OUTPUTS)
    build_request = _build_plan_request_payload(
        output_targets=output_targets,
        output_root=resolved_root,
        rulepack_profile=options.rulepack_profile,
    )
    try:
        compiler = engine_module.SemanticPlanCompiler()
        spec_json = compiler.build_spec_json(
            msgspec.json.encode(to_builtins(ir, str_keys=True)).decode(),
            msgspec.json.encode(build_request).decode(),
        )
        spec = msgspec.json.decode(spec_json, type=SemanticExecutionSpec)
        compiled = compiler.compile(spec_json)
        compile_metadata = json.loads(compiler.compile_metadata_json(session_factory, spec_json))
    except (ValueError, RuntimeError) as exc:
        error_message = f"Plan compilation failed: {exc}"
        raise RuntimeError(error_message) from exc

    return _build_payload(
        spec=spec,
        compiled=compiled,
        compile_metadata=compile_metadata,
        runtime_metadata=runtime_metadata,
        options=_PlanPayloadOptions(
            show_graph=options.show_graph,
            show_schedule=options.show_schedule,
            show_inferred_deps=options.show_inferred_deps,
            show_task_graph=options.show_task_graph,
            validate=options.validate,
            output_format=options.output_format,
        ),
    )


def _build_payload(
    *,
    spec: SemanticExecutionSpec,
    compiled: object,
    compile_metadata: dict[str, object],
    runtime_metadata: _PlanRuntimeMetadata,
    options: _PlanPayloadOptions,
) -> object:
    if options.output_format == "dot":
        return _build_dot_output(spec)

    payload = _build_payload_base(spec, compiled, compile_metadata, runtime_metadata)
    _maybe_add_tier2_features(payload, options)

    if options.output_format == "json":
        return payload

    return _format_text(payload)


def _build_payload_base(
    spec: SemanticExecutionSpec,
    compiled: object,
    compile_metadata: dict[str, object],
    runtime_metadata: _PlanRuntimeMetadata,
) -> dict[str, object]:
    task_schedule = compile_metadata.get("task_schedule")
    dependency_map = compile_metadata.get("dependency_map")
    return {
        "plan_signature": compile_metadata.get(
            "spec_hash",
            getattr(compiled, "spec_hash_hex", lambda: "unknown")(),
        ),
        "view_count": len(spec.view_definitions),
        "output_targets": [target.table_name for target in spec.output_targets],
        "rulepack_profile": spec.rulepack_profile,
        "input_relation_count": len(spec.input_relations),
        "join_edge_count": len(spec.join_graph.edges),
        "dependency_map": dependency_map if isinstance(dependency_map, dict) else {},
        "task_schedule": task_schedule if isinstance(task_schedule, dict) else None,
        "engine_profile": runtime_metadata.engine_profile,
        "runtime_profile_name": runtime_metadata.runtime_profile_name,
        "runtime_profile_hash": runtime_metadata.runtime_profile_hash,
    }


def _build_dot_output(spec: SemanticExecutionSpec) -> str:
    """Build DOT graph from view definitions and join edges (Tier 1 approximation).

    Returns:
    -------
    str
        DOT format graph representation of the execution plan.
    """
    lines = ["digraph execution_plan {", "  rankdir=LR;", ""]
    lines.extend(
        f'  "{view.name}" [label="{view.name}\\n({view.view_kind})"];'
        for view in spec.view_definitions
    )
    lines.append("")
    for view in spec.view_definitions:
        lines.extend(f'  "{dep}" -> "{view.name}";' for dep in view.view_dependencies)
    lines.extend(
        f'  "{edge.left_relation}" -> "{edge.right_relation}" [style=dashed];'
        for edge in spec.join_graph.edges
    )
    lines.append("}")
    return "\n".join(lines)


def _maybe_add_tier2_features(
    payload: dict[str, object],
    options: _PlanPayloadOptions,
) -> None:
    """Add Tier 2/3 features that require Rust introspection API (currently unavailable)."""
    tier2_or_3_requested = (
        options.show_graph
        or options.show_schedule
        or options.show_inferred_deps
        or options.show_task_graph
        or options.validate
    )
    if tier2_or_3_requested:
        payload["tier2_3_notice"] = (
            "Tier 2/3 features (show_graph, show_schedule, show_inferred_deps, "
            "show_task_graph, validate) require Rust introspection API not yet available. "
            "Current implementation provides Tier 1 parity only (spec hash, view count, output targets)."
        )


def _format_text(payload: dict[str, object]) -> str:
    lines = _format_base_lines(payload)
    _append_tier2_notice(lines, payload)
    return "\n".join(lines)


def _format_base_lines(payload: dict[str, object]) -> list[str]:
    lines = [
        f"plan_signature: {payload.get('plan_signature')}",
        f"view_count: {payload.get('view_count')}",
        f"rulepack_profile: {payload.get('rulepack_profile')}",
        f"engine_profile: {payload.get('engine_profile')}",
        f"runtime_profile_name: {payload.get('runtime_profile_name')}",
        f"runtime_profile_hash: {payload.get('runtime_profile_hash')}",
        f"input_relation_count: {payload.get('input_relation_count')}",
        f"join_edge_count: {payload.get('join_edge_count')}",
    ]
    output_targets = payload.get("output_targets")
    if isinstance(output_targets, list) and output_targets:
        lines.append(f"output_targets: {', '.join(output_targets)}")
    else:
        lines.append("output_targets: (none)")
    dependency_map = payload.get("dependency_map")
    if isinstance(dependency_map, dict):
        lines.append(f"dependency_map_entries: {len(dependency_map)}")
    task_schedule = payload.get("task_schedule")
    if isinstance(task_schedule, dict):
        execution_order = task_schedule.get("execution_order")
        critical_path = task_schedule.get("critical_path")
        if isinstance(execution_order, list):
            lines.append(f"task_schedule.execution_order_count: {len(execution_order)}")
        if isinstance(critical_path, list):
            lines.append(f"task_schedule.critical_path_count: {len(critical_path)}")
    return lines


def _append_tier2_notice(lines: list[str], payload: dict[str, object]) -> None:
    notice = payload.get("tier2_3_notice")
    if isinstance(notice, str):
        lines.append("")
        lines.append(f"Note: {notice}")


def _write_payload(payload: object, output_file: Path | None, output_format: str) -> None:
    if output_format == "json":
        encoded = json.dumps(payload, indent=2, sort_keys=True)
        _write_text(encoded, output_file)
        return
    if output_format == "dot":
        _write_text(str(payload), output_file)
        return
    _write_text(str(payload), output_file)


def _write_text(payload: str, output_file: Path | None) -> None:
    if output_file is None:
        sys.stdout.write(payload + "\n")
        return
    output_file.write_text(payload + "\n", encoding="utf-8")


__all__ = ["plan_command"]
