"""Tests for configuration source tracking."""

from __future__ import annotations

from pathlib import Path

import pytest

from cli.config_loader import load_effective_config_with_sources
from cli.config_source import ConfigSource


def _write_config(path: Path) -> None:
    contents = """
[plan]
allow_partial = true
requested_tasks = ["task_a"]

[cache]
policy_profile = "default"
path = "build/cache"

[graph_adapter]
kind = "ray"

[incremental]
enabled = true
state_dir = "build/state"
repo_id = "repo-123"
impact_strategy = "hybrid"
"""
    path.write_text(contents.strip() + "\n", encoding="utf-8")


def test_config_sources_from_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure config file values are normalized and sourced correctly."""
    config_path = tmp_path / "codeanatomy.toml"
    _write_config(config_path)
    monkeypatch.chdir(tmp_path)

    config = load_effective_config_with_sources(None)

    assert "plan_allow_partial" in config.values
    assert "cache_policy_profile" in config.values
    assert "incremental_state_dir" in config.values

    plan_entry = config.values["plan_allow_partial"]
    assert plan_entry.source == ConfigSource.CONFIG_FILE
    assert plan_entry.location == str(config_path)


def test_env_overrides_take_precedence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Ensure environment variables override config values."""
    config_path = tmp_path / "codeanatomy.toml"
    _write_config(config_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CODEANATOMY_OUTPUT_DIR", "/tmp/out")

    config = load_effective_config_with_sources(None)

    assert "output_dir" in config.values
    entry = config.values["output_dir"]
    assert entry.source == ConfigSource.ENV
    assert entry.location == "CODEANATOMY_OUTPUT_DIR"
