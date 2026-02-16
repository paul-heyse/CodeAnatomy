"""Tests for Delta session runtime policy bridging in the session factory."""

from __future__ import annotations

import importlib
from types import ModuleType

import pytest
from datafusion import RuntimeEnvBuilder, SessionContext

from datafusion_engine.session.context_pool import _build_delta_session_context
from datafusion_engine.session.delta_session_builder import (
    build_runtime_policy_options,
    parse_runtime_size,
)
from datafusion_engine.session.runtime import DataFusionRuntimeProfile, PolicyBundleConfig

LEGACY_BRIDGE_ARG_COUNT = 3
RUNTIME_BRIDGE_ARG_COUNT = 4


class _RuntimePolicyOptions:
    def __init__(self) -> None:
        self.memory_limit: int | None = None
        self.metadata_cache_limit: int | None = None
        self.temp_directory: str | None = None
        self.max_temp_directory_size: int | None = None


class _BridgeModule(ModuleType):
    def __init__(self) -> None:
        super().__init__("datafusion_ext")
        self.calls: list[tuple[object, ...]] = []
        self.DeltaSessionRuntimePolicyOptions = _RuntimePolicyOptions
        self.delta_session_context = self._bridge_builder

    def _bridge_builder(self, *args: object) -> SessionContext:
        self.calls.append(args)
        return SessionContext()


class _LegacyBridgeModule(ModuleType):
    def __init__(self) -> None:
        super().__init__("datafusion_ext")
        self.calls: list[tuple[object, ...]] = []
        self.DeltaSessionRuntimePolicyOptions = _RuntimePolicyOptions
        self.delta_session_context = self._legacy_builder

    def _legacy_builder(self, *args: object) -> SessionContext:
        if len(args) != LEGACY_BRIDGE_ARG_COUNT:
            msg = "delta_session_context() takes 3 positional arguments but 4 were given"
            raise TypeError(msg)
        self.calls.append(args)
        return SessionContext()


def _import_override(module: ModuleType) -> object:
    def _load(name: str) -> ModuleType:
        if name in {"datafusion_ext", "datafusion_engine.extensions.datafusion_ext"}:
            return module
        msg = f"no module named {name}"
        raise ImportError(msg)

    return _load


def test_delta_session_factory_bridges_runtime_settings_to_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test delta session factory bridges runtime settings to options."""
    module = _BridgeModule()
    monkeypatch.setattr(importlib, "import_module", _import_override(module))
    profile = DataFusionRuntimeProfile(
        policies=PolicyBundleConfig(cache_profile_name="always_latest_ttl30s"),
    )

    result = _build_delta_session_context(profile, RuntimeEnvBuilder())
    settings = profile.settings_payload()

    assert result.installed is True
    assert result.error is None
    assert module.calls
    call = module.calls[-1]
    assert len(call) == RUNTIME_BRIDGE_ARG_COUNT
    runtime_policy = call[3]
    assert isinstance(runtime_policy, _RuntimePolicyOptions)
    assert runtime_policy.metadata_cache_limit == int(
        settings["datafusion.runtime.metadata_cache_limit"]
    )
    assert runtime_policy.memory_limit == int(settings["datafusion.runtime.memory_limit"])
    assert runtime_policy.temp_directory == settings["datafusion.runtime.temp_directory"]
    assert runtime_policy.max_temp_directory_size == int(
        settings["datafusion.runtime.max_temp_directory_size"]
    )
    assert result.runtime_policy_bridge is not None
    assert result.runtime_policy_bridge.get("enabled") is True
    unsupported = result.runtime_policy_bridge.get("unsupported_runtime_settings")
    assert isinstance(unsupported, dict)
    assert "datafusion.runtime.list_files_cache_limit" in unsupported
    assert "datafusion.runtime.list_files_cache_ttl" in unsupported


def test_delta_session_factory_falls_back_to_legacy_builder_signature(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test delta session factory falls back to legacy builder signature."""
    module = _LegacyBridgeModule()
    monkeypatch.setattr(importlib, "import_module", _import_override(module))
    profile = DataFusionRuntimeProfile(
        policies=PolicyBundleConfig(cache_profile_name="snapshot_pinned"),
    )

    result = _build_delta_session_context(profile, RuntimeEnvBuilder())

    assert result.installed is True
    assert result.error is None
    assert module.calls
    call = module.calls[-1]
    assert len(call) == LEGACY_BRIDGE_ARG_COUNT
    assert result.runtime_policy_bridge is not None
    assert result.runtime_policy_bridge.get("reason") == "legacy_builder_signature"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1024", 1024),
        ("64M", 64 * 1024 * 1024),
        ("", None),
        ("abc", None),
        ("-1m", None),
    ],
)
def test_parse_runtime_size(raw: object, expected: int | None) -> None:
    """Parse runtime-size values into bytes or reject invalid payloads."""
    assert parse_runtime_size(raw) == expected


def test_runtime_policy_options_reports_missing_options_class() -> None:
    """Report deterministic bridge metadata when options class is unavailable."""
    bridge = build_runtime_policy_options(
        module=object(),
        runtime_settings={"datafusion.runtime.memory_limit": "1M"},
    )
    assert bridge.options is None
    assert bridge.payload is not None
    assert bridge.payload.get("reason") == "options_type_unavailable"


def test_runtime_policy_options_reports_no_supported_settings() -> None:
    """Return disabled payload when no runtime settings can be bridged."""
    module = _BridgeModule()
    bridge = build_runtime_policy_options(
        module=module,
        runtime_settings={"datafusion.runtime.list_files_cache_limit": "64M"},
    )
    assert bridge.options is None
    assert bridge.payload is not None
    assert bridge.payload.get("reason") == "no_supported_runtime_settings"
