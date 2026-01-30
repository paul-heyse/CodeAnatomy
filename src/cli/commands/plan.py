"""Execution plan inspection command."""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Literal

from cyclopts import Parameter, validators

from cli.context import RunContext
from cli.groups import execution_group
from hamilton_pipeline.types import ExecutionMode
from serde_msgspec import to_builtins
from utils.uuid_factory import uuid7_str

if TYPE_CHECKING:
    from hamilton_pipeline.plan_artifacts import PlanArtifactBundle
    from relspec.execution_plan import ExecutionPlan


@dataclass(frozen=True)
class PlanOptions:
    """CLI options for plan command."""

    show_graph: Annotated[
        bool,
        Parameter(help="Display task dependency graph."),
    ] = False
    show_schedule: Annotated[
        bool,
        Parameter(help="Display computed execution schedule."),
    ] = False
    validate: Annotated[
        bool,
        Parameter(help="Validate plan consistency and report issues."),
    ] = False
    output_format: Annotated[
        Literal["text", "json", "dot"],
        Parameter(help="Output format for plan display."),
    ] = "text"
    output_file: Annotated[
        Path | None,
        Parameter(name="-o", help="Write plan to file instead of stdout."),
    ] = None
    execution_mode: Annotated[
        ExecutionMode,
        Parameter(
            name="--execution-mode",
            help="Execution mode to use when compiling the plan.",
            group=execution_group,
        ),
    ] = ExecutionMode.PLAN_PARALLEL


_DEFAULT_PLAN_OPTIONS = PlanOptions()


@dataclass(frozen=True)
class _PlanPayloadOptions:
    show_graph: bool
    show_schedule: bool
    validate: bool
    output_format: Literal["text", "json", "dot"]


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

    Returns
    -------
    int
        Exit status code.
    """
    resolved_root = repo_root.resolve()
    config_contents = dict(run_context.config_contents) if run_context else {}
    config_contents.setdefault("repo_root", str(resolved_root))

    from hamilton_pipeline.driver_factory import DriverBuildRequest, build_plan_context
    from hamilton_pipeline.plan_artifacts import build_plan_artifact_bundle

    plan_ctx = build_plan_context(
        request=DriverBuildRequest(
            config=config_contents,
            execution_mode=options.execution_mode,
            executor_config=None,
            graph_adapter_config=None,
        )
    )
    plan: ExecutionPlan = plan_ctx.plan

    run_id = run_context.run_id if run_context else uuid7_str()
    plan_bundle: PlanArtifactBundle = build_plan_artifact_bundle(plan=plan, run_id=run_id)

    payload = _build_payload(
        plan=plan,
        plan_bundle=plan_bundle,
        options=_PlanPayloadOptions(
            show_graph=options.show_graph,
            show_schedule=options.show_schedule,
            validate=options.validate,
            output_format=options.output_format,
        ),
    )

    _write_payload(payload, options.output_file, options.output_format)
    return 0


def _build_payload(
    *,
    plan: ExecutionPlan,
    plan_bundle: PlanArtifactBundle,
    options: _PlanPayloadOptions,
) -> object:
    if options.output_format == "dot":
        return plan.diagnostics.dot

    payload: dict[str, object] = {
        "plan_signature": plan.plan_signature,
        "reduced_plan_signature": plan.reduced_task_dependency_signature,
        "task_count": len(plan.active_tasks),
    }

    if options.show_graph:
        payload["graph"] = {
            "dot": plan.diagnostics.dot,
            "critical_path": list(plan.critical_path_task_names),
            "critical_path_length": plan.critical_path_length_weighted,
        }

    if options.show_schedule:
        schedule_payload = to_builtins(plan_bundle.schedule_envelope)
        payload["schedule"] = schedule_payload

    if options.validate:
        validation_payload = to_builtins(plan_bundle.validation_envelope)
        payload["validation"] = validation_payload

    if options.output_format == "json":
        return payload

    return _format_text(payload)


def _format_text(payload: dict[str, object]) -> str:
    lines = [
        f"plan_signature: {payload.get('plan_signature')}",
        f"reduced_plan_signature: {payload.get('reduced_plan_signature')}",
        f"task_count: {payload.get('task_count')}",
    ]
    graph = payload.get("graph")
    if isinstance(graph, dict):
        lines.append("graph:")
        lines.append(f"  critical_path_length: {graph.get('critical_path_length')}")
        critical_path = graph.get("critical_path")
        if isinstance(critical_path, list):
            lines.append(f"  critical_path_tasks: {', '.join(critical_path)}")
    if "schedule" in payload:
        lines.append("schedule: <included>")
    if "validation" in payload:
        lines.append("validation: <included>")
    return "\n".join(lines)


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
