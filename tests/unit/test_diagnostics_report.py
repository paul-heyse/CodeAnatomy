"""Unit tests for runtime diagnostics report synthesis."""

from __future__ import annotations

from pathlib import Path

from obs.diagnostics_report import build_diagnostics_report, write_run_diagnostics_report


def test_runtime_capability_summary_from_runtime_artifact() -> None:
    """Summarize runtime capability payload emitted by runtime profiles."""
    snapshot = {
        "spans": [],
        "logs": [
            {
                "attributes": {
                    "event.name": "datafusion_runtime_capabilities_v1",
                    "event_time_unix_ms": 42,
                    "strict_native_provider_enabled": True,
                    "delta_available": True,
                    "delta_compatible": True,
                    "delta_probe_result": "ok",
                    "delta_ctx_kind": "outer",
                    "delta_module": "datafusion_ext",
                    "plugin_manifest": {"plugin_path": "/tmp/plugin.so"},
                }
            }
        ],
    }
    report = build_diagnostics_report(snapshot)
    assert report.runtime_capabilities["total"] == 1
    assert report.runtime_capabilities["strict_native_provider_enabled"] is True
    assert report.runtime_capabilities["delta_compatible"] is True
    assert report.runtime_capabilities["plugin_path"] == "/tmp/plugin.so"


def test_provider_mode_summary_includes_strict_violation_counts() -> None:
    """Count strict native-provider violations from provider-mode diagnostics."""
    snapshot = {
        "spans": [],
        "logs": [
            {
                "attributes": {
                    "event.name": "dataset_provider_mode_v1",
                    "provider_mode": "delta_dataset_fallback",
                    "strict_native_provider_enabled": True,
                    "strict_native_provider_violation": True,
                    "diagnostic.severity": "warn",
                }
            },
            {
                "attributes": {
                    "event.name": "dataset_provider_mode_v1",
                    "provider_mode": "delta_table_provider",
                    "strict_native_provider_enabled": True,
                    "strict_native_provider_violation": False,
                    "diagnostic.severity": "info",
                }
            },
        ],
    }
    report = build_diagnostics_report(snapshot)
    assert report.provider_modes["total"] == 2
    assert report.provider_modes["warnings"] == 1
    assert report.provider_modes["strict_native_provider_enabled"] is True
    assert report.provider_modes["strict_native_provider_violations"] == 1


def test_diagnostics_markdown_includes_runtime_capabilities_section(tmp_path: Path) -> None:
    """Render runtime capabilities summary into the markdown report."""
    snapshot = {
        "spans": [],
        "logs": [
            {
                "attributes": {
                    "event.name": "datafusion_runtime_capabilities_v1",
                    "event_time_unix_ms": 7,
                    "strict_native_provider_enabled": True,
                    "delta_available": True,
                    "delta_compatible": True,
                    "delta_probe_result": "ok",
                    "delta_ctx_kind": "outer",
                }
            }
        ],
    }
    path = write_run_diagnostics_report(
        snapshot=snapshot,
        run_bundle_dir=tmp_path,
    )
    rendered = path.read_text(encoding="utf-8")
    assert "## Runtime Capabilities" in rendered
    assert "strict_native_provider_enabled: True" in rendered
