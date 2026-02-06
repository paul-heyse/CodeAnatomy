"""Shared extension adaptation helpers."""

from datafusion_engine.extensions.context_adaptation import (
    ExtensionContextPolicy,
    ExtensionContextProbe,
    ExtensionContextSelection,
    invoke_entrypoint_with_adapted_context,
    resolve_extension_module,
    select_context_candidate,
)
from datafusion_engine.extensions.plugin_manifest import (
    PluginManifestResolution,
    resolve_plugin_manifest,
)

__all__ = [
    "ExtensionContextPolicy",
    "ExtensionContextProbe",
    "ExtensionContextSelection",
    "PluginManifestResolution",
    "invoke_entrypoint_with_adapted_context",
    "resolve_extension_module",
    "resolve_plugin_manifest",
    "select_context_candidate",
]
