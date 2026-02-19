"""Delta extension capability checks."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from datafusion import SessionContext

from datafusion_engine.extensions.context_adaptation import (
    ExtensionContextPolicy,
    ExtensionEntrypointInvocation,
    invoke_entrypoint_with_adapted_context,
    resolve_extension_module,
    select_context_candidate,
)

_DELTA_EXTENSION_MODULES: tuple[str, ...] = ("datafusion_engine.extensions.datafusion_ext",)
_NATIVE_DML_ENTRYPOINTS: dict[str, str] = {
    "delete": "delta_delete_request",
    "update": "delta_update_request",
    "merge": "delta_merge_request",
}


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

    Returns:
    -------
    DeltaExtensionCompatibility
        Availability/compatibility status plus diagnostic payload.
    """
    policy = ExtensionContextPolicy(
        module_names=_DELTA_EXTENSION_MODULES,
        entrypoint=entrypoint,
        allow_fallback=False,
        require_non_fallback=require_non_fallback,
    )
    resolved = resolve_delta_extension_module(entrypoint=policy.entrypoint)
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
    if not callable(getattr(resolved.module, probe_entrypoint, None)):
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
    internal_ctx = getattr(ctx, "ctx", None)
    try:
        selection, _ = invoke_entrypoint_with_adapted_context(
            resolved.name,
            resolved.module,
            probe_entrypoint,
            ExtensionEntrypointInvocation(
                ctx=ctx,
                internal_ctx=internal_ctx,
                args=args,
                allow_fallback=False,
            ),
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
    ctx_kind = selection.ctx_kind
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

    Returns:
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

    Returns:
    -------
    DeltaExtensionModule | None
        Resolved extension module metadata, or ``None`` when unavailable.
    """
    resolved = resolve_extension_module(
        _DELTA_EXTENSION_MODULES,
        required_attr=required_attr,
        entrypoint=entrypoint,
    )
    if resolved is not None:
        module_name, module = resolved
        return DeltaExtensionModule(name=module_name, module=module)
    return None


def delta_context_candidates(
    ctx: object,
    module: object,
    *,
    allow_fallback: bool = False,
) -> tuple[tuple[str, object], ...]:
    """Return ordered context candidates for Delta extension entrypoints.

    Returns:
    -------
    tuple[tuple[str, object], ...]
        Ordered (kind, context) candidates to probe.
    """
    _ = module
    return select_context_candidate(
        ctx,
        internal_ctx=getattr(ctx, "ctx", None),
        allow_fallback=allow_fallback,
        fallback_ctx=None,
    )


def invoke_delta_entrypoint(
    module: object,
    entrypoint: str,
    *,
    ctx: SessionContext,
    args: Sequence[object] = (),
    allow_fallback: bool = False,
) -> tuple[str, object]:
    """Invoke a Delta entrypoint with context adaptation and rich errors.

    Returns:
    -------
    tuple[str, object]
        Tuple containing the successful context kind and the payload.

    """
    selection, payload = invoke_entrypoint_with_adapted_context(
        "unknown",
        module,
        entrypoint,
        ExtensionEntrypointInvocation(
            ctx=ctx,
            internal_ctx=getattr(ctx, "ctx", None),
            args=args,
            allow_fallback=allow_fallback,
        ),
    )
    return selection.ctx_kind, payload


def _probe_args_for_entrypoint(entrypoint: str) -> tuple[object, ...]:
    if entrypoint == "delta_scan_config_from_session":
        return (None, None, None, None, None)
    return ()


def native_dml_compatibility(
    ctx: SessionContext,
    *,
    operation: str,
    require_non_fallback: bool = True,
) -> DeltaExtensionCompatibility:
    """Return compatibility payload for a native DML operation.

    Raises:
        ValueError: If ``operation`` is not one of delete/update/merge.
    """
    try:
        entrypoint = _NATIVE_DML_ENTRYPOINTS[operation]
    except KeyError:
        msg = f"Unsupported native DML operation: {operation}"
        raise ValueError(msg) from None
    return is_delta_extension_compatible(
        ctx,
        entrypoint=entrypoint,
        require_non_fallback=require_non_fallback,
    )


def provider_supports_native_dml(
    ctx: SessionContext,
    *,
    operation: str,
    require_non_fallback: bool = True,
) -> bool:
    """Return whether provider-native DML is supported for ``operation``."""
    compatibility = native_dml_compatibility(
        ctx,
        operation=operation,
        require_non_fallback=require_non_fallback,
    )
    return compatibility.available and compatibility.compatible


__all__ = [
    "DeltaExtensionCompatibility",
    "DeltaExtensionModule",
    "delta_context_candidates",
    "invoke_delta_entrypoint",
    "is_delta_extension_compatible",
    "native_dml_compatibility",
    "probe_delta_entrypoint",
    "provider_supports_native_dml",
    "resolve_delta_extension_module",
]
