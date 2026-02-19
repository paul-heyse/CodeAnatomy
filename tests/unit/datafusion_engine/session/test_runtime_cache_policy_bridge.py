"""Runtime cache-policy bridge tests for Delta session options mapping."""

from __future__ import annotations

from collections.abc import Mapping
from types import SimpleNamespace
from typing import cast

from datafusion_engine.session.delta_session_builder import build_runtime_policy_options

_TTL_SECONDS = 30


class _FakeRuntimePolicyOptions:
    def __init__(self) -> None:
        self.memory_limit = None
        self.metadata_cache_limit = None
        self.list_files_cache_limit = None
        self.list_files_cache_ttl_seconds = None
        self.temp_directory = None
        self.max_temp_directory_size = None


def test_runtime_cache_policy_bridge_maps_cache_manager_fields() -> None:
    """Supported runtime cache settings should map to typed bridge options."""
    module = SimpleNamespace(DeltaSessionRuntimePolicyOptions=_FakeRuntimePolicyOptions)
    settings = {
        "datafusion.runtime.metadata_cache_limit": "32mb",
        "datafusion.runtime.list_files_cache_limit": "16mb",
        "datafusion.runtime.list_files_cache_ttl": f"{_TTL_SECONDS}s",
        "datafusion.runtime.memory_limit": "1gb",
    }

    bridge = build_runtime_policy_options(module, settings)

    assert isinstance(bridge.options, _FakeRuntimePolicyOptions)
    assert bridge.options.metadata_cache_limit == 32 * 1024 * 1024
    assert bridge.options.list_files_cache_limit == 16 * 1024 * 1024
    assert bridge.options.list_files_cache_ttl_seconds == _TTL_SECONDS
    assert bridge.options.memory_limit == 1024 * 1024 * 1024
    assert isinstance(bridge.payload, dict)
    assert bridge.payload["enabled"] is True
    consumed = cast("Mapping[str, object]", bridge.payload["consumed_runtime_settings"])
    assert consumed["datafusion.runtime.list_files_cache_ttl"] == _TTL_SECONDS


def test_runtime_cache_policy_bridge_reports_unsupported_keys() -> None:
    """Unsupported runtime keys should be surfaced in bridge diagnostics."""
    module = SimpleNamespace(DeltaSessionRuntimePolicyOptions=_FakeRuntimePolicyOptions)
    settings = {
        "datafusion.runtime.unknown": "1",
    }

    bridge = build_runtime_policy_options(module, settings)

    assert bridge.options is None
    assert isinstance(bridge.payload, dict)
    assert bridge.payload["enabled"] is False
    assert bridge.payload["unsupported_runtime_settings"] == {"datafusion.runtime.unknown": "1"}
