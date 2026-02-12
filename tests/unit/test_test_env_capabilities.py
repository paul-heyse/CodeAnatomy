"""Test profile capability sanity checks."""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from pathlib import Path

from datafusion_engine.delta.capabilities import resolve_delta_extension_module
from datafusion_engine.extensions.plugin_manifest import resolve_plugin_manifest
from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    FeatureGatesConfig,
    PolicyBundleConfig,
)
from tests.test_helpers.optional_deps import (
    require_datafusion,
    require_datafusion_udfs,
    require_delta_extension,
    require_deltalake,
)


def test_required_runtime_capabilities_available(  # noqa: PLR0914
    require_native_runtime: None,
) -> None:
    """Fail fast when required runtime dependencies are missing."""
    _ = require_native_runtime
    datafusion = require_datafusion()
    _ = require_datafusion_udfs()
    _ = require_deltalake()
    _ = require_delta_extension()

    substrait_module = importlib.import_module("datafusion.substrait")
    assert callable(getattr(substrait_module.Consumer, "from_substrait_plan", None))
    assert callable(getattr(substrait_module.Producer, "to_substrait_plan", None))
    extension = importlib.import_module("datafusion._internal")
    has_unified_runtime = callable(getattr(extension, "install_codeanatomy_runtime", None))
    has_modular_runtime = all(
        callable(getattr(extension, entrypoint, None))
        for entrypoint in (
            "register_codeanatomy_udfs",
            "install_function_factory",
            "install_expr_planners",
            "registry_snapshot",
        )
    )
    assert has_unified_runtime or has_modular_runtime
    assert callable(getattr(extension, "session_context_contract_probe", None))
    assert callable(getattr(extension, "delta_write_ipc", None))
    assert callable(getattr(extension, "capabilities_snapshot", None))
    assert callable(getattr(extension, "runtime_execution_metrics_snapshot", None))
    assert hasattr(extension, "DeltaSessionRuntimePolicyOptions")
    for entrypoint in (
        "delta_scan_config_from_session",
        "delta_table_provider_from_session",
        "delta_table_provider_with_files",
    ):
        resolved = resolve_delta_extension_module(entrypoint=entrypoint)
        assert resolved is not None
        assert callable(getattr(resolved.module, entrypoint, None))
    capabilities = extension.capabilities_snapshot()
    assert isinstance(capabilities, dict)
    runtime_contract = capabilities.get("runtime_install_contract")
    if isinstance(runtime_contract, dict):
        version = runtime_contract.get("version")
        assert isinstance(version, int)
        assert version >= 3
    else:
        # Compatibility path for wheel builds that still expose legacy capability payloads.
        assert has_unified_runtime or has_modular_runtime
    for name in ("delta_control_plane", "substrait", "async_udf"):
        payload = capabilities.get(name)
        assert isinstance(payload, dict)
        assert isinstance(payload.get("available"), bool)
    udf_registry_payload = capabilities.get("udf_registry")
    assert isinstance(udf_registry_payload, dict)
    table_count = udf_registry_payload.get("table")
    assert isinstance(table_count, int)
    assert table_count >= 2
    assert callable(getattr(extension, "plugin_manifest", None))
    manifest_resolution = resolve_plugin_manifest("datafusion_ext")
    manifest = manifest_resolution.manifest
    assert isinstance(manifest, Mapping), manifest_resolution.error
    plugin_path = manifest.get("plugin_path")
    assert isinstance(plugin_path, str)
    assert Path(plugin_path).exists()
    assert hasattr(datafusion, "SessionContext")

    profile = DataFusionRuntimeProfile(
        features=FeatureGatesConfig(enable_async_udfs=True),
        policies=PolicyBundleConfig(
            async_udf_timeout_ms=500,
            async_udf_batch_size=64,
        ),
    )
    runtime = profile.session_runtime()
    assert runtime.profile.features.enable_async_udfs is True
