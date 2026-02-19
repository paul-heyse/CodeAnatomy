"""Shared module/context adaptation contract for extension entrypoints."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from datafusion_engine.extensions.required_entrypoints import REQUIRED_RUNTIME_ENTRYPOINTS


@dataclass(frozen=True)
class ExtensionContextPolicy:
    """Policy describing context adaptation constraints."""

    module_names: tuple[str, ...]
    entrypoint: str
    required_attr: str | None = None
    allow_fallback: bool = False
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
    kwargs: Mapping[str, object] | None = None
    allow_fallback: bool = False
    fallback_ctx_factory: Callable[[object], object | None] | None = None


def resolve_extension_module(
    module_names: Sequence[str],
    *,
    required_attr: str | None = None,
    entrypoint: str | None = None,
) -> tuple[str, object] | None:
    """Resolve the first extension module satisfying attribute requirements.

    Returns:
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
    allow_fallback: bool = False,
    fallback_ctx: object | None = None,
) -> tuple[tuple[str, object], ...]:
    """Return ordered context candidates for extension entrypoints.

    Returns:
    -------
    tuple[tuple[str, object], ...]
        Ordered ``(kind, context)`` pairs.
    """
    candidates: list[tuple[str, object]] = [("outer", ctx)]
    if internal_ctx is not None and internal_ctx is not ctx:
        candidates.append(("internal", internal_ctx))
    if (
        allow_fallback
        and fallback_ctx is not None
        and all(fallback_ctx is not candidate[1] for candidate in candidates)
    ):
        candidates.append(("fallback", fallback_ctx))
    return tuple(candidates)


def invoke_entrypoint_with_adapted_context(
    module_name: str,
    module: object,
    entrypoint: str,
    invocation: ExtensionEntrypointInvocation,
) -> tuple[ExtensionContextSelection, object]:
    """Invoke an extension entrypoint by probing ordered context candidates.

    Args:
        module_name: Extension module name for diagnostics.
        module: Imported extension module object.
        entrypoint: Entrypoint attribute name.
        invocation: Invocation payload with candidate contexts.

    Returns:
        tuple[ExtensionContextSelection, object]: Result.

    Raises:
        RuntimeError: If all context invocation attempts fail.
        TypeError: If the entrypoint is missing or not callable.
    """
    fn = getattr(module, entrypoint, None)
    if not callable(fn):
        msg = f"Extension entrypoint {entrypoint} is unavailable."
        raise TypeError(msg)

    allow_fallback = invocation.allow_fallback and entrypoint not in REQUIRED_RUNTIME_ENTRYPOINTS
    fallback_ctx: object | None = None
    if allow_fallback and callable(invocation.fallback_ctx_factory):
        fallback_ctx = invocation.fallback_ctx_factory(module)
    candidates = select_context_candidate(
        invocation.ctx,
        internal_ctx=invocation.internal_ctx,
        allow_fallback=allow_fallback,
        fallback_ctx=fallback_ctx,
    )
    resolved_kwargs: dict[str, object] = (
        {} if invocation.kwargs is None else dict(invocation.kwargs)
    )
    failures: list[str] = []
    for ctx_kind, candidate_ctx in candidates:
        try:
            payload = fn(candidate_ctx, *invocation.args, **resolved_kwargs)
        except (TypeError, RuntimeError, ValueError) as exc:
            failures.append(f"{ctx_kind}: {exc}")
            continue
        selection = ExtensionContextSelection(
            module_name=module_name,
            module=module,
            ctx_kind=ctx_kind,
            ctx=candidate_ctx,
            entrypoint=entrypoint,
        )
        return selection, payload
    joined = "; ".join(failures) if failures else "no compatible context candidate"
    msg = f"Extension entrypoint {entrypoint} failed for context candidates ({joined})"
    raise RuntimeError(msg)


__all__ = [
    "ExtensionContextPolicy",
    "ExtensionContextProbe",
    "ExtensionContextSelection",
    "ExtensionEntrypointInvocation",
    "invoke_entrypoint_with_adapted_context",
    "resolve_extension_module",
    "select_context_candidate",
]
