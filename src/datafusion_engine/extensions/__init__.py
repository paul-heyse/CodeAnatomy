"""Shared extension adaptation helpers."""

from datafusion_engine.extensions.context_adaptation import (
    ExtensionContextPolicy,
    ExtensionContextProbe,
    ExtensionContextSelection,
    invoke_entrypoint_with_adapted_context,
    resolve_extension_module,
    select_context_candidate,
)

__all__ = [
    "ExtensionContextPolicy",
    "ExtensionContextProbe",
    "ExtensionContextSelection",
    "invoke_entrypoint_with_adapted_context",
    "resolve_extension_module",
    "select_context_candidate",
]
