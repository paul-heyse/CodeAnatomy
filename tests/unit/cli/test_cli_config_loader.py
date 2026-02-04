"""Tests for CLI configuration normalization."""

from __future__ import annotations

from cli.config_loader import normalize_config_contents
from core_types import JsonValue


def test_normalize_config_contents_flattens_sections() -> None:
    """Ensure nested TOML sections are flattened to driver_factory keys."""
    config: dict[str, JsonValue] = {
        "plan": {
            "allow_partial": True,
            "requested_tasks": ["task_a"],
            "impacted_tasks": ["task_b"],
            "enable_metric_scheduling": False,
            "enable_plan_diagnostics": True,
        },
        "cache": {
            "policy_profile": "aggressive",
            "path": "/tmp/cache",
            "log_to_file": True,
        },
        "graph_adapter": {
            "kind": "ray",
            "options": {"address": "local"},
        },
        "incremental": {
            "enabled": True,
            "state_dir": "build/state",
            "repo_id": "repo-123",
            "impact_strategy": "hybrid",
        },
        "otel": {
            "enable_node_tracing": True,
            "endpoint": "http://localhost:4317",
        },
        "hamilton": {
            "enable_tracker": True,
            "graph_adapter_kind": "local",
            "graph_adapter_options": {"sync": True},
            "cache_path": "/tmp/hamilton-cache",
            "tags": {"team": "infra"},
        },
    }

    normalized = normalize_config_contents(config)

    assert normalized["plan_allow_partial"] is True
    assert normalized["plan_requested_tasks"] == ["task_a"]
    assert normalized["plan_impacted_tasks"] == ["task_b"]
    assert normalized["enable_metric_scheduling"] is False
    assert normalized["enable_plan_diagnostics"] is True
    assert normalized["cache_policy_profile"] == "aggressive"
    assert normalized["cache_path"] == "/tmp/cache"
    assert normalized["cache_log_to_file"] is True
    assert normalized["graph_adapter_kind"] == "ray"
    assert normalized["graph_adapter_options"] == {"address": "local"}
    assert normalized["incremental_enabled"] is True
    assert normalized["incremental_state_dir"] == "build/state"
    assert normalized["incremental_repo_id"] == "repo-123"
    assert normalized["incremental_impact_strategy"] == "hybrid"
    assert normalized["enable_otel_node_tracing"] is True
    assert normalized["otel_endpoint"] == "http://localhost:4317"
    assert normalized["enable_hamilton_tracker"] is True
    assert normalized["hamilton_graph_adapter_kind"] == "local"
    assert normalized["hamilton_graph_adapter_options"] == {"sync": True}
    assert normalized["hamilton_cache_path"] == "/tmp/hamilton-cache"
    assert normalized["hamilton_tags"] == {"team": "infra"}
