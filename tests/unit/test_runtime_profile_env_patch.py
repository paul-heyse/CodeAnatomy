"""Tests for runtime profile environment patch parsing."""

from __future__ import annotations

import msgspec
import pytest

from extraction import runtime_profile as runtime_profile_module


def test_runtime_profile_env_patch_defaults_to_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unset environment variables should decode to UNSET defaults."""
    monkeypatch.delenv("CODEANATOMY_DATAFUSION_POLICY", raising=False)
    monkeypatch.delenv("CODEANATOMY_DATAFUSION_CATALOG_LOCATION", raising=False)
    monkeypatch.delenv("CODEANATOMY_DATAFUSION_CATALOG_FORMAT", raising=False)
    monkeypatch.delenv("CODEANATOMY_CACHE_OUTPUT_ROOT", raising=False)
    monkeypatch.delenv("CODEANATOMY_RUNTIME_ARTIFACT_CACHE_ROOT", raising=False)
    monkeypatch.delenv("CODEANATOMY_RUNTIME_ARTIFACT_CACHE_ENABLED", raising=False)
    monkeypatch.delenv("CODEANATOMY_METADATA_CACHE_SNAPSHOT_ENABLED", raising=False)
    monkeypatch.delenv("CODEANATOMY_DIAGNOSTICS_SINK", raising=False)

    patch = runtime_profile_module._runtime_profile_env_patch()  # noqa: SLF001
    assert patch.config_policy_name is msgspec.UNSET
    assert patch.runtime_artifact_cache_enabled is msgspec.UNSET
    assert patch.diagnostics_sink is msgspec.UNSET


def test_runtime_profile_env_patch_rejects_invalid_payload_type(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Invalid intermediate payload types should surface as ValueError."""

    def _invalid_bool(_name: str) -> str:
        return "not-a-bool"

    monkeypatch.setattr(runtime_profile_module, "_env_patch_bool", _invalid_bool)

    with pytest.raises(ValueError, match="Runtime profile env patch validation failed"):
        runtime_profile_module._runtime_profile_env_patch()  # noqa: SLF001
