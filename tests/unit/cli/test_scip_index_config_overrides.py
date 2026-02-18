"""Tests for SCIP CLI override parsing."""

from __future__ import annotations

from pathlib import Path

from extract.extractors.scip.config import ScipIndexConfig


def test_from_cli_overrides_merges_config_and_paths() -> None:
    repo_root = Path("/tmp/repo")
    config = {
        "scip": {
            "enabled": True,
            "output_dir": "cfg/scip",
            "index_path_override": "cfg/index.scip",
            "env_json_path": "cfg/env.json",
            "scip_python_bin": "scip-python-custom",
            "timeout_s": "42",
            "extra_args": ["--foo", "--bar"],
        }
    }

    settings = ScipIndexConfig.from_cli_overrides(
        config,
        repo_root=repo_root,
        disable_scip=False,
        scip_output_dir="cli/scip",
        scip_index_path_override=None,
        scip_env_json=None,
        scip_python_bin="scip-python",
        default_scip_python="scip-python",
        scip_target_only="pkg/module",
        scip_timeout_s=None,
        node_max_old_space_mb=None,
        scip_extra_args=(),
    )

    assert settings.enabled is True
    assert settings.output_dir == str(repo_root / "cli/scip")
    assert settings.index_path_override == "cfg/index.scip"
    assert settings.env_json_path == "cfg/env.json"
    assert settings.scip_python_bin == "scip-python-custom"
    assert settings.target_only == "pkg/module"
    assert settings.timeout_s == 42
    assert settings.extra_args == ("--foo", "--bar")


def test_from_cli_overrides_disable_and_python_override() -> None:
    settings = ScipIndexConfig.from_cli_overrides(
        {"scip": {"enabled": True, "scip_python_bin": "cfg-python"}},
        repo_root=Path("/repo"),
        disable_scip=True,
        scip_output_dir=None,
        scip_index_path_override=None,
        scip_env_json=None,
        scip_python_bin="cli-python",
        default_scip_python="scip-python",
        scip_target_only=None,
        scip_timeout_s=7,
        node_max_old_space_mb=2048,
        scip_extra_args=("--fast",),
    )

    assert settings.enabled is False
    assert settings.scip_python_bin == "cli-python"
    assert settings.timeout_s == 7
    assert settings.node_max_old_space_mb == 2048
    assert settings.extra_args == ("--fast",)
