"""Shared module/context adaptation contract for extension entrypoints."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class ExtensionContextPolicy:
    """Policy describing context adaptation constraints."""

    module_names: tuple[str, ...]
    entrypoint: str
    required_attr: str | None = None
    allow_fallback: bool = True
    require_non_fallback: bool = False


@dataclass(frozen=True)
class ExtensionContextProbe:
    """Compatibility payload for extension context probing."""

    available: bool
    compatible: bool
    entrypoint: str
    module: str | None
    ctx_kind: str | None
    probe_result: str
    error: str | None = None


@dataclass(frozen=True)
class ExtensionContextSelection:
    """Resolved extension context selection for entrypoint invocation."""

    module_name: str
    module: object
    ctx_kind: str
    ctx: object
    entrypoint: str


@dataclass(frozen=True)
class ExtensionEntrypointInvocation:
    """Invocation configuration for context-adapted entrypoint execution."""

    ctx: object
    internal_ctx: object | None = None
    args: Sequence[object] = ()
    allow_fallback: bool = True
    fallback_ctx_factory: Callable[[object], object | None] | None = None


def resolve_extension_module(
    module_names: Sequence[str],
    *,
    required_attr: str | None = None,
    entrypoint: str | None = None,
) -> tuple[str, object] | None:
    """Resolve the first extension module satisfying attribute requirements.

    Returns
    -------
    tuple[str, object] | None
        Resolved module name and module instance, or ``None`` when unavailable.
    """
    for module_name in module_names:
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        if required_attr is not None and not hasattr(module, required_attr):
            continue
        if entrypoint is not None and not callable(getattr(module, entrypoint, None)):
            continue
        return module_name, module
    return None


def select_context_candidate(
    ctx: object,
    *,
    internal_ctx: object | None = None,
    allow_fallback: bool = True,
    fallback_ctx: object | None = None,
) -> tuple[tuple[str, object], ...]:
    """Return ordered context candidates for extension entrypoint probing.

    Returns
    -------
    tuple[tuple[str, object], ...]
        Ordered ``(kind, context)`` pairs used for entrypoint invocation.
    """
    candidates: list[tuple[str, object]] = []
    _append_candidate(candidates, "outer", ctx)
    _append_candidate(candidates, "internal", internal_ctx)
    if allow_fallback:
        _append_candidate(candidates, "fallback", fallback_ctx)
    return tuple(candidates)


def invoke_entrypoint_with_adapted_context(
    module_name: str,
    module: object,
    entrypoint: str,
    invocation: ExtensionEntrypointInvocation,
) -> tuple[ExtensionContextSelection, object]:
    """Invoke an extension entrypoint by probing ordered context candidates.

    Returns
    -------
    tuple[ExtensionContextSelection, object]
        Selected context metadata and entrypoint payload.

    Raises
    ------
    TypeError
        Raised when the requested entrypoint is unavailable.
    RuntimeError
        Raised when every context candidate fails.
    """
    fn = getattr(module, entrypoint, None)
    if not callable(fn):
        msg = f"Extension entrypoint {entrypoint} is unavailable."
        raise TypeError(msg)

    fallback_ctx = None
    if invocation.allow_fallback and invocation.fallback_ctx_factory is not None:
        fallback_ctx = invocation.fallback_ctx_factory(module)

    errors: list[str] = []
    for ctx_kind, candidate in select_context_candidate(
        invocation.ctx,
        internal_ctx=invocation.internal_ctx,
        allow_fallback=invocation.allow_fallback,
        fallback_ctx=fallback_ctx,
    ):
        try:
            payload = fn(candidate, *invocation.args)
        except (TypeError, RuntimeError, ValueError) as exc:
            errors.append(f"{ctx_kind}: {exc}")
            continue
        selection = ExtensionContextSelection(
            module_name=module_name,
            module=module,
            ctx_kind=ctx_kind,
            ctx=candidate,
            entrypoint=entrypoint,
        )
        return selection, payload

    detail = "; ".join(errors) if errors else "no context candidates"
    msg = f"Extension entrypoint {entrypoint} failed across context candidates ({detail})"
    raise RuntimeError(msg)


def _append_candidate(
    candidates: list[tuple[str, object]],
    kind: str,
    candidate: object | None,
) -> None:
    if candidate is None:
        return
    if any(existing is candidate for _, existing in candidates):
        return
    candidates.append((kind, candidate))


__all__ = [
    "ExtensionContextPolicy",
    "ExtensionContextProbe",
    "ExtensionContextSelection",
    "ExtensionEntrypointInvocation",
    "invoke_entrypoint_with_adapted_context",
    "resolve_extension_module",
    "select_context_candidate",
]
