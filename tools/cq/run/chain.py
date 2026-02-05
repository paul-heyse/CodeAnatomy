"""Compile chained CQ commands into a RunPlan."""

from __future__ import annotations

from collections.abc import Iterable

from cyclopts.exceptions import CycloptsError

from tools.cq.cli_app.app import app
from tools.cq.run.spec import (
    BytecodeSurfaceStep,
    CallsStep,
    ExceptionsStep,
    ImpactStep,
    ImportsStep,
    QStep,
    RunPlan,
    RunStep,
    ScopesStep,
    SearchStep,
    SideEffectsStep,
    SigImpactStep,
)


def compile_chain_segments(groups: Iterable[list[str]]) -> RunPlan:
    """Compile token groups into a RunPlan.

    Returns
    -------
    RunPlan
        Run plan containing one step per command group.

    Raises
    ------
    RuntimeError
        Raised when a chain segment is empty or invalid.
    """
    steps: list[RunStep] = []
    for group in groups:
        if not group:
            msg = "Empty chain segment"
            raise RuntimeError(msg)
        try:
            command, bound, _ignored = app.parse_args(
                group,
                exit_on_error=False,
                print_error=False,
            )
        except CycloptsError as exc:  # pragma: no cover - defensive
            msg = str(exc)
            raise RuntimeError(msg) from exc

        step = _step_from_command(command, bound)
        steps.append(step)

    return RunPlan(steps=tuple(steps))


def _step_from_command(command: object, bound: object) -> RunStep:
    name = getattr(command, "__name__", None)
    args = getattr(bound, "args", ())
    kwargs = getattr(bound, "kwargs", {})
    builder = _STEP_BUILDERS.get(name)
    if builder is None:
        msg = f"Unsupported chain command: {name}"
        raise RuntimeError(msg)
    return builder(args, kwargs.get("opts"))


def _build_q_step(args: tuple[object, ...], _opts: object | None) -> RunStep:
    return QStep(query=args[0])


def _build_search_step(args: tuple[object, ...], opts: object | None) -> RunStep:
    return SearchStep(
        query=args[0],
        regex=getattr(opts, "regex", False),
        literal=getattr(opts, "literal", False),
        include_strings=getattr(opts, "include_strings", False),
        in_dir=getattr(opts, "in_dir", None),
    )


def _build_calls_step(args: tuple[object, ...], _opts: object | None) -> RunStep:
    return CallsStep(function=args[0])


def _build_impact_step(args: tuple[object, ...], opts: object | None) -> RunStep:
    return ImpactStep(
        function=args[0],
        param=getattr(opts, "param", None),
        depth=getattr(opts, "depth", 5),
    )


def _build_imports_step(_args: tuple[object, ...], opts: object | None) -> RunStep:
    return ImportsStep(
        cycles=getattr(opts, "cycles", False),
        module=getattr(opts, "module", None),
    )


def _build_exceptions_step(_args: tuple[object, ...], opts: object | None) -> RunStep:
    return ExceptionsStep(function=getattr(opts, "function", None))


def _build_sig_impact_step(args: tuple[object, ...], opts: object | None) -> RunStep:
    return SigImpactStep(symbol=args[0], to=getattr(opts, "to", None))


def _build_side_effects_step(_args: tuple[object, ...], opts: object | None) -> RunStep:
    return SideEffectsStep(max_files=getattr(opts, "max_files", 2000))


def _build_scopes_step(args: tuple[object, ...], _opts: object | None) -> RunStep:
    return ScopesStep(target=args[0])


def _build_bytecode_surface_step(args: tuple[object, ...], opts: object | None) -> RunStep:
    return BytecodeSurfaceStep(
        target=args[0],
        show=getattr(opts, "show", "globals,attrs,constants"),
    )


_STEP_BUILDERS = {
    "q": _build_q_step,
    "search": _build_search_step,
    "calls": _build_calls_step,
    "impact": _build_impact_step,
    "imports": _build_imports_step,
    "exceptions": _build_exceptions_step,
    "sig_impact": _build_sig_impact_step,
    "side_effects": _build_side_effects_step,
    "scopes": _build_scopes_step,
    "bytecode_surface": _build_bytecode_surface_step,
}


__all__ = [
    "compile_chain_segments",
]
