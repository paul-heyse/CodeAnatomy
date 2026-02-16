"""Unit tests for runtime diagnostics report synthesis."""

from __future__ import annotations

from pathlib import Path

from obs.diagnostics_report import build_diagnostics_report, write_run_diagnostics_report

EXECUTION_METRICS_ROWS = 2
MEMORY_RESERVED_BYTES = 4096
METADATA_CACHE_ENTRIES = 12
METADATA_CACHE_HITS = 21
LIST_FILES_CACHE_ENTRIES = 5
STATISTICS_CACHE_ENTRIES = 9
RUNTIME_POLICY_CONSUMED_SETTINGS = 2
RUNTIME_CAPABILITIES_TOTAL = 2


def test_runtime_capability_summary_from_runtime_artifact() -> None:
    """Summarize runtime capability payload emitted by runtime profiles."""
    spans: list[dict[str, object]] = []
    snapshot = {
        "spans": spans,
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


def test_runtime_capability_summary_includes_execution_metrics() -> None:
    """Include structured runtime execution metrics in capability summaries."""
    spans: list[dict[str, object]] = []
    snapshot = {
        "spans": spans,
        "logs": [
            {
                "attributes": {
                    "event.name": "datafusion_runtime_capabilities_v1",
                    "event_time_unix_ms": 51,
                    "strict_native_provider_enabled": True,
                    "delta_available": True,
                    "delta_compatible": True,
                    "execution_metrics": {
                        "summary": {
                            "memory_reserved_bytes": 4096,
                            "metadata_cache_entries": 12,
                            "metadata_cache_hits": 21,
                            "list_files_cache_entries": 5,
                            "statistics_cache_entries": 9,
                        },
                        "rows": [
                            {"metric_name": "memory_reserved_bytes", "value": 4096},
                            {"metric_name": "metadata_cache_entries", "value": 12},
                        ],
                    },
                }
            }
        ],
    }
    report = build_diagnostics_report(snapshot)
    assert report.runtime_capabilities["execution_metrics_rows"] == EXECUTION_METRICS_ROWS
    assert report.runtime_capabilities["execution_memory_reserved_bytes"] == MEMORY_RESERVED_BYTES
    assert report.runtime_capabilities["execution_metadata_cache_entries"] == METADATA_CACHE_ENTRIES
    assert report.runtime_capabilities["execution_metadata_cache_hits"] == METADATA_CACHE_HITS
    assert (
        report.runtime_capabilities["execution_list_files_cache_entries"]
        == LIST_FILES_CACHE_ENTRIES
    )
    assert (
        report.runtime_capabilities["execution_statistics_cache_entries"]
        == STATISTICS_CACHE_ENTRIES
    )


def test_runtime_capability_summary_includes_runtime_policy_bridge_fields() -> None:
    """Include runtime-policy bridge details from session defaults artifacts."""
    spans: list[dict[str, object]] = []
    snapshot = {
        "spans": spans,
        "logs": [
            {
                "attributes": {
                    "event.name": "datafusion_runtime_capabilities_v1",
                    "event_time_unix_ms": 51,
                    "strict_native_provider_enabled": True,
                    "delta_available": True,
                    "delta_compatible": True,
                }
            },
            {
                "attributes": {
                    "event.name": "datafusion_delta_session_defaults_v1",
                    "event_time_unix_ms": 52,
                    "enabled": True,
                    "available": True,
                    "installed": True,
                    "runtime_policy_bridge": {
                        "enabled": True,
                        "consumed_runtime_settings": {
                            "datafusion.runtime.memory_limit": 1024,
                            "datafusion.runtime.metadata_cache_limit": 2048,
                        },
                        "unsupported_runtime_settings": {
                            "datafusion.runtime.list_files_cache_ttl": "60s"
                        },
                    },
                }
            },
        ],
    }
    report = build_diagnostics_report(snapshot)
    assert report.runtime_capabilities["delta_session_defaults_enabled"] is True
    assert report.runtime_capabilities["delta_session_defaults_available"] is True
    assert report.runtime_capabilities["delta_session_defaults_installed"] is True
    assert report.runtime_capabilities["runtime_policy_bridge_enabled"] is True
    assert (
        report.runtime_capabilities["runtime_policy_bridge_consumed_settings"]
        == RUNTIME_POLICY_CONSUMED_SETTINGS
    )
    assert report.runtime_capabilities["runtime_policy_bridge_unsupported_settings"] == 1
    assert report.runtime_capabilities["runtime_policy_bridge_reason"] is None


def test_runtime_capability_summary_reports_runtime_policy_bridge_reason() -> None:
    """Expose explicit runtime-policy bridge disable reason when provided."""
    spans: list[dict[str, object]] = []
    snapshot = {
        "spans": spans,
        "logs": [
            {
                "attributes": {
                    "event.name": "datafusion_runtime_capabilities_v1",
                    "event_time_unix_ms": 51,
                    "strict_native_provider_enabled": True,
                    "delta_available": True,
                    "delta_compatible": True,
                }
            },
            {
                "attributes": {
                    "event.name": "datafusion_delta_session_defaults_v1",
                    "event_time_unix_ms": 52,
                    "enabled": True,
                    "available": True,
                    "installed": True,
                    "runtime_policy_bridge": {
                        "enabled": False,
                        "reason": "legacy_builder_signature",
                    },
                }
            },
        ],
    }
    report = build_diagnostics_report(snapshot)
    assert report.runtime_capabilities["runtime_policy_bridge_enabled"] is False
    assert report.runtime_capabilities["runtime_policy_bridge_reason"] == "legacy_builder_signature"


def test_runtime_capability_summary_prefers_runtime_events_over_fallbacks() -> None:
    """Prefer primary runtime capability artifacts when fallback events also exist."""
    spans: list[dict[str, object]] = []
    snapshot = {
        "spans": spans,
        "logs": [
            {
                "attributes": {
                    "event.name": "datafusion_extension_parity_v1",
                    "runtime_capabilities": {
                        "event_time_unix_ms": 5,
                        "strict_native_provider_enabled": False,
                        "delta_available": False,
                        "delta_compatible": False,
                    },
                }
            },
            {
                "attributes": {
                    "event.name": "delta_service_provider_v1",
                    "event_time_unix_ms": 8,
                    "strict_native_provider_enabled": False,
                    "available": False,
                    "compatible": False,
                }
            },
            {
                "attributes": {
                    "event.name": "datafusion_runtime_capabilities_v1",
                    "event_time_unix_ms": 42,
                    "strict_native_provider_enabled": True,
                    "delta_available": True,
                    "delta_compatible": True,
                }
            },
        ],
    }
    report = build_diagnostics_report(snapshot)
    assert report.runtime_capabilities["total"] == 1
    assert report.runtime_capabilities["strict_native_provider_enabled"] is True
    assert report.runtime_capabilities["delta_compatible"] is True


def test_runtime_capability_summary_handles_malformed_execution_metrics() -> None:
    """Treat malformed execution metrics payloads as absent summary fields."""
    spans: list[dict[str, object]] = []
    snapshot = {
        "spans": spans,
        "logs": [
            {
                "attributes": {
                    "event.name": "datafusion_runtime_capabilities_v1",
                    "event_time_unix_ms": 51,
                    "strict_native_provider_enabled": True,
                    "delta_available": True,
                    "delta_compatible": True,
                    "execution_metrics": {"rows": "not-a-sequence", "summary": "not-a-mapping"},
                }
            }
        ],
    }
    report = build_diagnostics_report(snapshot)
    assert report.runtime_capabilities["execution_metrics_rows"] is None
    assert report.runtime_capabilities["execution_memory_reserved_bytes"] is None


def test_provider_mode_summary_includes_strict_violation_counts() -> None:
    """Count strict native-provider violations from provider-mode diagnostics."""
    spans: list[dict[str, object]] = []
    snapshot = {
        "spans": spans,
        "logs": [
            {
                "attributes": {
                    "event.name": "dataset_provider_mode_v1",
                    "provider_mode": "delta_table_provider",
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
    assert report.provider_modes["total"] == RUNTIME_CAPABILITIES_TOTAL
    assert report.provider_modes["warnings"] == 1
    assert report.provider_modes["strict_native_provider_enabled"] is True
    assert report.provider_modes["strict_native_provider_violations"] == 1


def test_diagnostics_markdown_includes_runtime_capabilities_section(tmp_path: Path) -> None:
    """Render runtime capabilities summary into the markdown report."""
    spans: list[dict[str, object]] = []
    snapshot = {
        "spans": spans,
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


def test_runtime_capability_summary_falls_back_to_service_provider_artifact() -> None:
    """Use delta service provider artifacts when runtime capability artifacts are absent."""
    spans: list[dict[str, object]] = []
    snapshot = {
        "spans": spans,
        "logs": [
            {
                "attributes": {
                    "event.name": "delta_service_provider_v1",
                    "event_time_unix_ms": 11,
                    "strict_native_provider_enabled": True,
                    "available": True,
                    "compatible": True,
                    "probe_result": "ok",
                    "ctx_kind": "outer",
                    "module": "datafusion_ext",
                }
            }
        ],
    }
    report = build_diagnostics_report(snapshot)
    assert report.runtime_capabilities["total"] == 1
    assert report.runtime_capabilities["strict_native_provider_enabled"] is True
    assert report.runtime_capabilities["delta_available"] is True
    assert report.runtime_capabilities["delta_compatible"] is True
    assert report.runtime_capabilities["delta_probe_result"] == "ok"
    assert report.runtime_capabilities["delta_ctx_kind"] == "outer"
