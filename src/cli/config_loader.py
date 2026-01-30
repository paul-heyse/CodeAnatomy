"""Config loading and normalization helpers for the CLI."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Mapping

    from core_types import JsonValue


def load_effective_config(config_file: str | None) -> dict[str, JsonValue]:
    """Load config contents from codeanatomy.toml / pyproject.toml or explicit --config.

    Parameters
    ----------
    config_file
        Optional explicit config file path.

    Returns
    -------
    dict[str, object]
        Parsed configuration contents.
    """
    if config_file:
        path = Path(config_file)
        if path.exists():
            with path.open("rb") as handle:
                return cast("dict[str, JsonValue]", tomllib.load(handle))
        return {}

    config: dict[str, JsonValue] = {}
    codeanatomy_path = _find_in_parents("codeanatomy.toml")
    if codeanatomy_path is not None:
        with codeanatomy_path.open("rb") as handle:
            config = cast("dict[str, JsonValue]", tomllib.load(handle))

    pyproject_path = _find_in_parents("pyproject.toml")
    if pyproject_path is not None:
        with pyproject_path.open("rb") as handle:
            pyproject = cast("dict[str, JsonValue]", tomllib.load(handle))
        tool_section = pyproject.get("tool")
        if isinstance(tool_section, dict):
            nested = tool_section.get("codeanatomy")
            if isinstance(nested, dict):
                config = cast("dict[str, JsonValue]", nested)

    return config


def normalize_config_contents(config: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    """Normalize nested TOML sections into flat keys used by driver_factory.

    Parameters
    ----------
    config
        Raw configuration contents.

    Returns
    -------
    dict[str, object]
        Normalized configuration payload with flat keys.
    """
    flat: dict[str, JsonValue] = {}
    flat.update(config)

    plan = config.get("plan")
    if isinstance(plan, dict):
        _copy_key(flat, plan, "allow_partial", "plan_allow_partial")
        _copy_key(flat, plan, "requested_tasks", "plan_requested_tasks")
        _copy_key(flat, plan, "impacted_tasks", "plan_impacted_tasks")
        _copy_key(flat, plan, "enable_metric_scheduling", "enable_metric_scheduling")
        _copy_key(flat, plan, "enable_plan_diagnostics", "enable_plan_diagnostics")
        _copy_key(
            flat,
            plan,
            "enable_plan_task_submission_hook",
            "enable_plan_task_submission_hook",
        )
        _copy_key(
            flat,
            plan,
            "enable_plan_task_grouping_hook",
            "enable_plan_task_grouping_hook",
        )
        _copy_key(flat, plan, "enforce_plan_task_submission", "enforce_plan_task_submission")

    cache = config.get("cache")
    if isinstance(cache, dict):
        _copy_key(flat, cache, "policy_profile", "cache_policy_profile")
        _copy_key(flat, cache, "path", "cache_path")
        _copy_key(flat, cache, "log_to_file", "cache_log_to_file")

    graph_adapter = config.get("graph_adapter")
    if isinstance(graph_adapter, dict):
        _copy_key(flat, graph_adapter, "kind", "graph_adapter_kind")
        _copy_key(flat, graph_adapter, "options", "graph_adapter_options")

    incremental = config.get("incremental")
    if isinstance(incremental, dict):
        _copy_key(flat, incremental, "enabled", "incremental_enabled")
        _copy_key(flat, incremental, "state_dir", "incremental_state_dir")
        _copy_key(flat, incremental, "repo_id", "incremental_repo_id")
        _copy_key(flat, incremental, "impact_strategy", "incremental_impact_strategy")

    return flat


def _copy_key(
    target: dict[str, JsonValue],
    source: Mapping[str, JsonValue],
    source_key: str,
    dest_key: str,
) -> None:
    value = source.get(source_key)
    if value is not None:
        target[dest_key] = value


def _find_in_parents(filename: str) -> Path | None:
    """Walk parents from cwd to find a filename.

    Parameters
    ----------
    filename
        Filename to locate.

    Returns
    -------
    Path | None
        Path to the first matching file in the current directory or parents.
    """
    path = Path.cwd()
    while True:
        candidate = path / filename
        if candidate.exists():
            return candidate
        if path.parent == path:
            return None
        path = path.parent


__all__ = ["load_effective_config", "normalize_config_contents"]
