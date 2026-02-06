"""Delta extension capability checks."""

from __future__ import annotations

import importlib
from collections.abc import Callable, Sequence
from dataclasses import dataclass

from datafusion import SessionContext

_DELTA_EXTENSION_MODULES: tuple[str, ...] = ("datafusion._internal", "datafusion_ext")
_DELTA_SESSION_BUILDER = "delta_session_context"


@dataclass(frozen=True)
class DeltaExtensionCompatibility:
    """Compatibility status for the Delta extension module."""

    available: bool
    compatible: bool
    error: str | None
    entrypoint: str
    module: str | None = None
    ctx_kind: str | None = None
    probe_result: str | None = None


@dataclass(frozen=True)
class DeltaExtensionModule:
    """Resolved Delta extension module metadata."""

    name: str
    module: object


def probe_delta_entrypoint(
    ctx: SessionContext,
    *,
    entrypoint: str = "delta_scan_config_from_session",
    require_non_fallback: bool = False,
) -> DeltaExtensionCompatibility:
    """Probe Delta extension compatibility for a requested entrypoint.

    The probe validates that the requested entrypoint exists and then executes a
    canonical probe entrypoint to validate context adaptation deterministically.

    Returns
    -------
    DeltaExtensionCompatibility
        Availability/compatibility status plus diagnostic payload.
    """
    resolved = resolve_delta_extension_module(entrypoint=entrypoint)
    if resolved is None:
        resolved_any = resolve_delta_extension_module()
        if resolved_any is None:
            return DeltaExtensionCompatibility(
                available=False,
                compatible=False,
                error="Delta extension module is unavailable.",
                entrypoint=entrypoint,
                module=None,
                ctx_kind=None,
                probe_result="module_unavailable",
            )
        return DeltaExtensionCompatibility(
            available=True,
            compatible=False,
            error=f"Delta entrypoint {entrypoint} is unavailable.",
            entrypoint=entrypoint,
            module=resolved_any.name,
            ctx_kind=None,
            probe_result="entrypoint_unavailable",
        )
    probe_entrypoint = "delta_scan_config_from_session"
    probe = getattr(resolved.module, probe_entrypoint, None)
    if not callable(probe):
        return DeltaExtensionCompatibility(
            available=True,
            compatible=False,
            error=f"Delta probe entrypoint {probe_entrypoint} is unavailable.",
            entrypoint=entrypoint,
            module=resolved.name,
            ctx_kind=None,
            probe_result="probe_unavailable",
        )
    args = _probe_args_for_entrypoint(probe_entrypoint)
    try:
        ctx_kind, _ = invoke_delta_entrypoint(
            resolved.module,
            probe_entrypoint,
            ctx=ctx,
            args=args,
            allow_fallback=True,
        )
    except (TypeError, RuntimeError, ValueError) as exc:
        return DeltaExtensionCompatibility(
            available=True,
            compatible=False,
            error=str(exc),
            entrypoint=entrypoint,
            module=resolved.name,
            ctx_kind=None,
            probe_result="error",
        )
    if require_non_fallback and ctx_kind == "fallback":
        return DeltaExtensionCompatibility(
            available=True,
            compatible=False,
            error="Fallback context is disallowed by strict probe policy.",
            entrypoint=entrypoint,
            module=resolved.name,
            ctx_kind=ctx_kind,
            probe_result="fallback_disallowed",
        )
    return DeltaExtensionCompatibility(
        available=True,
        compatible=True,
        error=None,
        entrypoint=entrypoint,
        module=resolved.name,
        ctx_kind=ctx_kind,
        probe_result="ok",
    )


def is_delta_extension_compatible(
    ctx: SessionContext,
    *,
    entrypoint: str = "delta_scan_config_from_session",
    require_non_fallback: bool = False,
) -> DeltaExtensionCompatibility:
    """Return whether the Delta extension entrypoint can be invoked safely.

    Returns
    -------
    DeltaExtensionCompatibility
        Availability/compatibility status plus error details if any.
    """
    return probe_delta_entrypoint(
        ctx,
        entrypoint=entrypoint,
        require_non_fallback=require_non_fallback,
    )


def resolve_delta_extension_module(
    *,
    required_attr: str | None = None,
    entrypoint: str | None = None,
) -> DeltaExtensionModule | None:
    """Return a Delta extension module matching the requested symbols.

    Returns
    -------
    DeltaExtensionModule | None
        Resolved extension module metadata, or ``None`` when unavailable.
    """
    for module_name in _DELTA_EXTENSION_MODULES:
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        if required_attr is not None and not hasattr(module, required_attr):
            continue
        if entrypoint is not None and not callable(getattr(module, entrypoint, None)):
            continue
        return DeltaExtensionModule(name=module_name, module=module)
    return None


def delta_context_candidates(
    ctx: SessionContext,
    module: object,
    *,
    allow_fallback: bool = True,
) -> tuple[tuple[str, object], ...]:
    """Return ordered context candidates for Delta extension entrypoints.

    Returns
    -------
    tuple[tuple[str, object], ...]
        Ordered (kind, context) candidates to probe.
    """
    candidates: list[tuple[str, object]] = []
    _append_candidate(candidates, "outer", ctx)
    _append_candidate(candidates, "internal", getattr(ctx, "ctx", None))
    if allow_fallback:
        _append_candidate(candidates, "fallback", _build_delta_session_context(module))
    return tuple(candidates)


def invoke_delta_entrypoint(
    module: object,
    entrypoint: str,
    *,
    ctx: SessionContext,
    args: Sequence[object] = (),
    allow_fallback: bool = True,
) -> tuple[str, object]:
    """Invoke a Delta entrypoint with context adaptation and rich errors.

    Returns
    -------
    tuple[str, object]
        Tuple containing the successful context kind and the payload.

    Raises
    ------
    TypeError
        Raised when the requested entrypoint is not callable.
    RuntimeError
        Raised when probing fails across all context candidates.
    """
    fn = getattr(module, entrypoint, None)
    if not callable(fn):
        msg = f"Delta entrypoint {entrypoint} is unavailable."
        raise TypeError(msg)
    errors: list[str] = []
    for ctx_kind, candidate in delta_context_candidates(
        ctx,
        module,
        allow_fallback=allow_fallback,
    ):
        try:
            payload = _call_with_ctx(fn, candidate, args)
        except (TypeError, RuntimeError, ValueError) as exc:
            errors.append(f"{ctx_kind}: {exc}")
            continue
        return ctx_kind, payload
    detail = "; ".join(errors) if errors else "no context candidates"
    msg = f"Delta entrypoint {entrypoint} failed across context candidates ({detail})"
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


def _build_delta_session_context(module: object) -> object | None:
    builder = getattr(module, _DELTA_SESSION_BUILDER, None)
    if not callable(builder):
        return None
    try:
        return builder()
    except (TypeError, RuntimeError, ValueError):
        try:
            return builder(None, None, None)
        except (TypeError, RuntimeError, ValueError):
            return None


def _call_with_ctx(
    fn: Callable[..., object],
    ctx_value: object,
    args: Sequence[object],
) -> object:
    return fn(ctx_value, *args)


def _probe_args_for_entrypoint(entrypoint: str) -> tuple[object, ...]:
    if entrypoint == "delta_scan_config_from_session":
        return (None, None, None, None, None)
    return ()


__all__ = [
    "DeltaExtensionCompatibility",
    "DeltaExtensionModule",
    "delta_context_candidates",
    "invoke_delta_entrypoint",
    "is_delta_extension_compatible",
    "probe_delta_entrypoint",
    "resolve_delta_extension_module",
]
