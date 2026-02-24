"""Run plan loader for cq run."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict, is_dataclass
from pathlib import Path

import msgspec

from tools.cq.core.typed_boundary import BoundaryDecodeError, convert_strict, decode_toml_strict
from tools.cq.run.spec import (
    RunLoadInput,
    RunPlan,
    RunStep,
    coerce_run_step,
    is_run_step,
    normalize_step_ids,
)
from tools.cq.run.step_payload_normalization import normalize_plan_payload, normalize_step_payload


class RunPlanError(RuntimeError):
    """Raised when a run plan cannot be loaded or validated."""


def load_run_plan(options: RunLoadInput) -> RunPlan:
    """Load a RunPlan from a plan file and inline steps.

    Args:
        options: CLI run options containing plan and inline steps.

    Returns:
        RunPlan: Validated run plan with normalized step IDs.

    Raises:
        RunPlanError: If no steps are provided or plan data is invalid.
    """
    plan = _load_plan_file(Path(options.plan)) if options.plan else RunPlan()

    inline_steps = _load_inline_steps(options.step, options.steps)
    if inline_steps:
        plan = msgspec.structs.replace(plan, steps=plan.steps + tuple(inline_steps))

    if not plan.steps:
        msg = "No steps provided (use --plan, --step, or --steps)"
        raise RunPlanError(msg)

    return msgspec.structs.replace(plan, steps=normalize_step_ids(plan.steps))


def _load_plan_file(path: Path) -> RunPlan:
    try:
        payload = path.read_bytes()
    except OSError as exc:
        msg = f"Failed to read plan file: {path}"
        raise RunPlanError(msg) from exc

    try:
        raw_plan = decode_toml_strict(payload, type_=dict[str, object])
        normalized = normalize_plan_payload(raw_plan)
        return convert_strict(normalized, type_=RunPlan)
    except BoundaryDecodeError as exc:
        msg = f"Invalid TOML in plan file: {path}"
        raise RunPlanError(msg) from exc


def _load_inline_steps(
    step_items: Iterable[object],
    steps_items: Iterable[object],
) -> list[RunStep]:
    return [_coerce_step(item) for item in list(step_items) + list(steps_items)]


def _coerce_step(item: object) -> RunStep:
    if is_run_step(item):
        return item
    if is_dataclass(item) and not isinstance(item, type):
        item = asdict(item)
    if isinstance(item, dict):
        try:
            return coerce_run_step(normalize_step_payload(item))
        except BoundaryDecodeError as exc:
            msg = f"Invalid step schema: {exc}"
            raise RunPlanError(msg) from exc
    msg = f"Unsupported step payload: {type(item)!r}"
    raise RunPlanError(msg)


__all__ = [
    "RunPlanError",
    "load_run_plan",
]
