"""Tests for CLI configuration normalization."""

from __future__ import annotations

from cli.config_loader import normalize_config_contents
from core_types import JsonValue


def test_normalize_config_contents_flattens_sections() -> None:
    """Ensure nested TOML sections remain nested for downstream consumers."""
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
            "tags": {"team": "infra"},
        },
    }

    normalized = normalize_config_contents(config)

    assert normalized == config
