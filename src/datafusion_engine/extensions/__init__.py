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
from datafusion_engine.extensions.schema_evolution import (
    install_schema_evolution_adapter_factory,
    load_schema_evolution_adapter_factory,
)
from datafusion_engine.extensions.schema_runtime import load_schema_runtime

__all__ = [
    "ExtensionContextPolicy",
    "ExtensionContextProbe",
    "ExtensionContextSelection",
    "PluginManifestResolution",
    "install_schema_evolution_adapter_factory",
    "invoke_entrypoint_with_adapted_context",
    "load_schema_evolution_adapter_factory",
    "load_schema_runtime",
    "resolve_extension_module",
    "resolve_plugin_manifest",
    "select_context_candidate",
]
