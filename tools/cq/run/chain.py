"""Compile chained CQ commands into a RunPlan."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, cast

from cyclopts.exceptions import CycloptsError

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


@dataclass(frozen=True)
class ChainCommand:
    """Parsed command + bound args for chaining."""

    name: str
    args: tuple[object, ...]
    opts: object | None


StepBuilder = Callable[[tuple[object, ...], object | None], RunStep]


def _require_name(name: object | None) -> str:
    if isinstance(name, str):
        return name
    msg = "Unsupported chain command: <unknown>"
    raise RuntimeError(msg)


def _require_str_arg(args: tuple[object, ...], idx: int, label: str) -> str:
    try:
        value = args[idx]
    except IndexError as exc:
        msg = f"Missing {label} argument"
        raise RuntimeError(msg) from exc
    if isinstance(value, str):
        return value
    msg = f"Invalid {label} argument: expected string"
    raise RuntimeError(msg)


def _require_str_attr(opts: object | None, attr: str, label: str) -> str:
    if opts is None:
        msg = f"Missing {label} option"
        raise RuntimeError(msg)
    value = getattr(opts, attr, None)
    if isinstance(value, str):
        return value
    msg = f"Invalid {label} option: expected string"
    raise RuntimeError(msg)


def compile_chain_segments(groups: Iterable[list[str]], *, cli_app: object) -> RunPlan:
    """Compile token groups into a RunPlan.

    Args:
        groups: Token groups representing chained CQ commands.
        cli_app: Cyclopts app wrapper used to parse each command segment.

    Returns:
        RunPlan: Normalized run plan built from the chain.

    Raises:
        RuntimeError: If a segment is empty or cannot be parsed.
    """
    app = cast("Any", cli_app)
    steps: list[RunStep] = []
    for group in groups:
        if not group:
            msg = "Empty chain segment"
            raise RuntimeError(msg)
        try:
            with app.app_stack(
                group,
                {
                    "exit_on_error": False,
                    "print_error": False,
                },
            ):
                command, bound, unused_tokens, _ignored = app.parse_known_args(group)
            if unused_tokens:
                msg = f"Unused chain tokens: {unused_tokens}"
                raise RuntimeError(msg)
        except CycloptsError as exc:  # pragma: no cover - defensive
            msg = str(exc)
            raise RuntimeError(msg) from exc

        step = _step_from_command(command, bound)
        steps.append(step)

    return RunPlan(steps=tuple(steps))


def _step_from_command(command: object, bound: object) -> RunStep:
    name = _require_name(getattr(command, "__name__", None))
    raw_args = getattr(bound, "args", ())
    args = tuple(raw_args)
    raw_kwargs = getattr(bound, "kwargs", {})
    kwargs: dict[str, object] = raw_kwargs if isinstance(raw_kwargs, dict) else {}
    cmd = ChainCommand(name=name, args=args, opts=kwargs.get("opts"))
    builder = _STEP_BUILDERS.get(cmd.name)
    if builder is None:
        msg = f"Unsupported chain command: {name}"
        raise RuntimeError(msg)
    return builder(cmd.args, cmd.opts)


def _build_q_step(args: tuple[object, ...], _opts: object | None) -> RunStep:
    return QStep(query=_require_str_arg(args, 0, "query"))


def _build_search_step(args: tuple[object, ...], opts: object | None) -> RunStep:
    regex_enabled = bool(getattr(opts, "regex", False))
    literal_enabled = bool(getattr(opts, "literal", False))
    if regex_enabled and literal_enabled:
        msg = "search step cannot set both regex and literal"
        raise RuntimeError(msg)
    mode = "regex" if regex_enabled else "literal" if literal_enabled else None
    return SearchStep(
        query=_require_str_arg(args, 0, "query"),
        mode=mode,
        include_strings=getattr(opts, "include_strings", False),
        in_dir=getattr(opts, "in_dir", None),
    )


def _build_calls_step(args: tuple[object, ...], _opts: object | None) -> RunStep:
    return CallsStep(function=_require_str_arg(args, 0, "function"))


def _build_impact_step(args: tuple[object, ...], opts: object | None) -> RunStep:
    return ImpactStep(
        function=_require_str_arg(args, 0, "function"),
        param=_require_str_attr(opts, "param", "param"),
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
    return SigImpactStep(
        symbol=_require_str_arg(args, 0, "symbol"),
        to=_require_str_attr(opts, "to", "to"),
    )


def _build_side_effects_step(_args: tuple[object, ...], opts: object | None) -> RunStep:
    return SideEffectsStep(max_files=getattr(opts, "max_files", 2000))


def _build_scopes_step(args: tuple[object, ...], _opts: object | None) -> RunStep:
    return ScopesStep(target=_require_str_arg(args, 0, "target"))


def _build_bytecode_surface_step(args: tuple[object, ...], opts: object | None) -> RunStep:
    return BytecodeSurfaceStep(
        target=_require_str_arg(args, 0, "target"),
        show=getattr(opts, "show", "globals,attrs,constants"),
    )


_STEP_BUILDERS: dict[str, StepBuilder] = {
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
