"""Tests for DataFusion plugin discovery."""

from __future__ import annotations

import pytest

from datafusion_engine.plugin_discovery import (
    plugin_diagnostics,
    plugin_manifest,
    plugin_stub_enabled,
    resolve_plugin_path,
)


def test_plugin_manifest_contains_required_fields() -> None:
    """Ensure plugin manifest exposes core compatibility fields."""
    if plugin_stub_enabled():
        pytest.skip("plugin stub enabled")
    manifest = plugin_manifest()
    for key in (
        "plugin_path",
        "plugin_abi_major",
        "plugin_abi_minor",
        "df_ffi_major",
        "datafusion_major",
        "arrow_major",
        "plugin_name",
        "plugin_version",
        "build_id",
        "capabilities",
        "features",
    ):
        assert key in manifest


def test_plugin_diagnostics_report_has_hash() -> None:
    """Ensure diagnostics report includes manifest hash and path."""
    if plugin_stub_enabled():
        pytest.skip("plugin stub enabled")
    report = plugin_diagnostics()
    assert report.plugin_path is not None
    assert report.plugin_path.exists()
    assert report.manifest is not None
    assert report.manifest_hash is not None


def test_resolve_plugin_path_exists() -> None:
    """Resolve plugin path and assert it exists on disk."""
    if plugin_stub_enabled():
        pytest.skip("plugin stub enabled")
    path = resolve_plugin_path()
    assert path.exists()
