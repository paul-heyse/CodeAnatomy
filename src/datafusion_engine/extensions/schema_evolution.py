"""Shared schema-evolution adapter extension helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion_engine.extensions.context_adaptation import (
    ExtensionEntrypointInvocation,
    invoke_entrypoint_with_adapted_context,
    resolve_extension_module,
)

if TYPE_CHECKING:
    from datafusion import SessionContext

_EXTENSION_MODULES: tuple[str, ...] = ("datafusion_engine.extensions.datafusion_ext",)


def load_schema_evolution_adapter_factory() -> object:
    """Return schema-evolution adapter factory from the active extension module.

    Raises:
        RuntimeError: If no supported extension module is available.
        TypeError: If the extension does not expose a callable adapter factory.
    """
    resolved = resolve_extension_module(
        _EXTENSION_MODULES,
        required_attr="schema_evolution_adapter_factory",
    )
    if resolved is None:  # pragma: no cover - optional dependency
        msg = "Schema evolution adapter requires datafusion_ext."
        raise RuntimeError(msg)
    _module_name, module = resolved
    factory = getattr(module, "schema_evolution_adapter_factory", None)
    if not callable(factory):
        msg = "Schema evolution adapter factory is unavailable in the extension module."
        raise TypeError(msg)
    return factory()


def install_schema_evolution_adapter_factory(ctx: SessionContext) -> None:
    """Install schema-evolution adapter factory into the active session context.

    Raises:
        RuntimeError: If extension entrypoint invocation fails unexpectedly.
        TypeError: If invocation fails due to SessionContext ABI mismatch.
    """
    from datafusion_engine.udf.extension_runtime import extension_capabilities_report

    try:
        capabilities = extension_capabilities_report()
    except (RuntimeError, TypeError, ValueError):
        capabilities = {"available": False, "compatible": False}
    if not (bool(capabilities.get("available")) and bool(capabilities.get("compatible"))):
        return
    resolved = resolve_extension_module(
        _EXTENSION_MODULES,
        entrypoint="install_schema_evolution_adapter_factory",
    )
    if resolved is None:  # pragma: no cover - optional dependency
        msg = "Schema evolution adapter requires datafusion_ext."
        raise RuntimeError(msg)
    module_name, module = resolved
    installer = getattr(module, "install_schema_evolution_adapter_factory", None)
    if not callable(installer):
        msg = "Schema evolution adapter install entrypoint is unavailable in the extension module."
        raise TypeError(msg)
    try:
        invoke_entrypoint_with_adapted_context(
            module_name,
            module,
            "install_schema_evolution_adapter_factory",
            ExtensionEntrypointInvocation(
                ctx=ctx,
                internal_ctx=getattr(ctx, "ctx", None),
                allow_fallback=False,
            ),
        )
    except (RuntimeError, TypeError, ValueError) as exc:
        msg = (
            "Schema evolution adapter install failed due to SessionContext ABI mismatch. "
            "Rebuild and install matching datafusion/datafusion_ext wheels "
            "(scripts/build_datafusion_wheels.sh + uv sync)."
        )
        raise RuntimeError(msg) from exc


__all__ = [
    "install_schema_evolution_adapter_factory",
    "load_schema_evolution_adapter_factory",
]
