"""Run plan specification for multi-step CQ execution."""

from __future__ import annotations

import msgspec


class RunPlan(msgspec.Struct, kw_only=True, frozen=True, omit_defaults=True):
    """Typed run plan for multi-step execution."""

    version: int = 1
    steps: tuple[RunStep, ...] = ()
    in_dir: str | None = None
    exclude: tuple[str, ...] = ()


class RunStepBase(
    msgspec.Struct,
    kw_only=True,
    frozen=True,
    omit_defaults=True,
    tag=True,
    tag_field="type",
):
    """Base class for run steps."""

    id: str | None = None


class QStep(RunStepBase, tag="q"):
    """Run step describing a q query."""

    query: str


class SearchStep(RunStepBase, tag="search"):
    """Run step describing a search query."""

    query: str
    regex: bool = False
    literal: bool = False
    include_strings: bool = False
    in_dir: str | None = None


class CallsStep(RunStepBase, tag="calls"):
    """Run step describing a calls query."""

    function: str


class ImpactStep(RunStepBase, tag="impact"):
    """Run step describing an impact query."""

    function: str
    param: str
    depth: int = 5


class ImportsStep(RunStepBase, tag="imports"):
    """Run step describing an imports query."""

    cycles: bool = False
    module: str | None = None


class ExceptionsStep(RunStepBase, tag="exceptions"):
    """Run step describing an exceptions query."""

    function: str | None = None


class SigImpactStep(RunStepBase, tag="sig-impact"):
    """Run step describing a signature impact query."""

    symbol: str
    to: str


class SideEffectsStep(RunStepBase, tag="side-effects"):
    """Run step describing a side-effects query."""

    max_files: int = 2000


class ScopesStep(RunStepBase, tag="scopes"):
    """Run step describing a scopes query."""

    target: str
    max_files: int = 500


class BytecodeSurfaceStep(RunStepBase, tag="bytecode-surface"):
    """Run step describing a bytecode-surface query."""

    target: str
    show: str = "globals,attrs,constants"
    max_files: int = 500


RunStep = (
    QStep
    | SearchStep
    | CallsStep
    | ImpactStep
    | ImportsStep
    | ExceptionsStep
    | SigImpactStep
    | SideEffectsStep
    | ScopesStep
    | BytecodeSurfaceStep
)

_STEP_TAGS: dict[type[RunStep], str] = {
    QStep: "q",
    SearchStep: "search",
    CallsStep: "calls",
    ImpactStep: "impact",
    ImportsStep: "imports",
    ExceptionsStep: "exceptions",
    SigImpactStep: "sig-impact",
    SideEffectsStep: "side-effects",
    ScopesStep: "scopes",
    BytecodeSurfaceStep: "bytecode-surface",
}


def step_type(step: RunStep) -> str:
    """Return the tag string for a run step.

    Returns
    -------
    str
        Step tag for the run step.

    Raises
    ------
    ValueError
        Raised when the step type is unknown.
    """
    try:
        return _STEP_TAGS[type(step)]
    except KeyError as exc:
        msg = f"Unknown step type: {type(step)!r}"
        raise ValueError(msg) from exc


def normalize_step_ids(steps: tuple[RunStep, ...]) -> tuple[RunStep, ...]:
    """Ensure every step has a deterministic id.

    Returns
    -------
    tuple[RunStep, ...]
        Steps with deterministic ids assigned when missing.
    """
    normalized: list[RunStep] = []
    for idx, step in enumerate(steps):
        if step.id:
            normalized.append(step)
            continue
        step_id = f"{step_type(step)}_{idx}"
        normalized.append(msgspec.structs.replace(step, id=step_id))
    return tuple(normalized)


__all__ = [
    "BytecodeSurfaceStep",
    "CallsStep",
    "ExceptionsStep",
    "ImpactStep",
    "ImportsStep",
    "QStep",
    "RunPlan",
    "RunStep",
    "RunStepBase",
    "ScopesStep",
    "SearchStep",
    "SideEffectsStep",
    "SigImpactStep",
    "normalize_step_ids",
    "step_type",
]
