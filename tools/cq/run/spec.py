"""Run plan specification for multi-step CQ execution."""

from __future__ import annotations

from typing import TypeGuard

import msgspec

from tools.cq.core.typed_boundary import convert_strict
from tools.cq.query.language import DEFAULT_QUERY_LANGUAGE_SCOPE, QueryLanguageScope


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


class QStep(RunStepBase, tag="q", frozen=True):
    """Run step describing a q query."""

    query: str


class SearchStep(RunStepBase, tag="search", frozen=True):
    """Run step describing a search query."""

    query: str
    regex: bool = False
    literal: bool = False
    include_strings: bool = False
    in_dir: str | None = None
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE


class CallsStep(RunStepBase, tag="calls", frozen=True):
    """Run step describing a calls query."""

    function: str


class ImpactStep(RunStepBase, tag="impact", frozen=True):
    """Run step describing an impact query."""

    function: str
    param: str
    depth: int = 5


class ImportsStep(RunStepBase, tag="imports", frozen=True):
    """Run step describing an imports query."""

    cycles: bool = False
    module: str | None = None


class ExceptionsStep(RunStepBase, tag="exceptions", frozen=True):
    """Run step describing an exceptions query."""

    function: str | None = None


class SigImpactStep(RunStepBase, tag="sig-impact", frozen=True):
    """Run step describing a signature impact query."""

    symbol: str
    to: str


class SideEffectsStep(RunStepBase, tag="side-effects", frozen=True):
    """Run step describing a side-effects query."""

    max_files: int = 2000


class ScopesStep(RunStepBase, tag="scopes", frozen=True):
    """Run step describing a scopes query."""

    target: str
    max_files: int = 500


class BytecodeSurfaceStep(RunStepBase, tag="bytecode-surface", frozen=True):
    """Run step describing a bytecode-surface query."""

    target: str
    show: str = "globals,attrs,constants"
    max_files: int = 500


class NeighborhoodStep(RunStepBase, tag="neighborhood", frozen=True):
    """Run step describing a neighborhood query."""

    target: str
    lang: str = "python"
    top_k: int = 10
    no_semantic_enrichment: bool = False


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
    | NeighborhoodStep
)

RUN_STEP_TYPES: tuple[type[RunStep], ...] = (
    QStep,
    SearchStep,
    CallsStep,
    ImpactStep,
    ImportsStep,
    ExceptionsStep,
    SigImpactStep,
    SideEffectsStep,
    ScopesStep,
    BytecodeSurfaceStep,
    NeighborhoodStep,
)


def is_run_step(obj: object) -> TypeGuard[RunStep]:
    """Return True if obj is a RunStep instance.

    Returns:
    -------
    bool
        ``True`` when obj is a RunStep instance.
    """
    return isinstance(obj, RunStepBase)


def coerce_run_step(payload: object) -> RunStep:
    """Convert payload into canonical run-step tagged union.

    Returns:
    -------
    RunStep
        Parsed run-step union member.
    """
    return convert_strict(payload, type_=RunStep)


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
    NeighborhoodStep: "neighborhood",
}


def step_type(step: RunStep) -> str:
    """Return the tag string for a run step.

    Args:
        step: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    try:
        return _STEP_TAGS[type(step)]
    except KeyError as exc:
        msg = f"Unknown step type: {type(step)!r}"
        raise ValueError(msg) from exc


def normalize_step_ids(steps: tuple[RunStep, ...]) -> tuple[RunStep, ...]:
    """Ensure every step has a deterministic id.

    Returns:
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
    "RUN_STEP_TYPES",
    "BytecodeSurfaceStep",
    "CallsStep",
    "ExceptionsStep",
    "ImpactStep",
    "ImportsStep",
    "NeighborhoodStep",
    "QStep",
    "RunPlan",
    "RunStep",
    "RunStepBase",
    "ScopesStep",
    "SearchStep",
    "SideEffectsStep",
    "SigImpactStep",
    "coerce_run_step",
    "is_run_step",
    "normalize_step_ids",
    "step_type",
]
