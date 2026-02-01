"""Config loading and normalization helpers for the CLI."""

from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import TYPE_CHECKING, cast

from cli.config_source import ConfigSource, ConfigValue, ConfigWithSources

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


def load_effective_config_with_sources(
    config_file: str | None,
) -> ConfigWithSources:
    """Load config contents with source tracking.

    Parameters
    ----------
    config_file
        Optional explicit config file path.

    Returns
    -------
    ConfigWithSources
        Configuration with source tracking for each value.
    """
    values: dict[str, ConfigValue] = {}
    _load_config_values(values, config_file)
    _apply_env_overrides(values)

    return ConfigWithSources(values=values)


def _load_config_values(values: dict[str, ConfigValue], config_file: str | None) -> None:
    if config_file:
        _load_explicit_config(values, Path(config_file))
        return
    _load_default_configs(values)


def _load_explicit_config(values: dict[str, ConfigValue], path: Path) -> None:
    if not path.exists():
        return
    raw = _read_toml(path)
    _apply_config_values(values, raw, location=str(path), skip_existing=False)


def _load_default_configs(values: dict[str, ConfigValue]) -> None:
    codeanatomy_path = _find_in_parents("codeanatomy.toml")
    if codeanatomy_path is not None:
        raw = _read_toml(codeanatomy_path)
        _apply_config_values(values, raw, location=str(codeanatomy_path), skip_existing=False)

    pyproject_path = _find_in_parents("pyproject.toml")
    if pyproject_path is None:
        return
    pyproject = _read_toml(pyproject_path)
    tool_section = pyproject.get("tool")
    if not isinstance(tool_section, dict):
        return
    nested = tool_section.get("codeanatomy")
    if not isinstance(nested, dict):
        return
    _apply_config_values(values, nested, location=str(pyproject_path), skip_existing=True)


def _read_toml(path: Path) -> dict[str, JsonValue]:
    with path.open("rb") as handle:
        return cast("dict[str, JsonValue]", tomllib.load(handle))


def _apply_config_values(
    values: dict[str, ConfigValue],
    raw: Mapping[str, JsonValue],
    *,
    location: str,
    skip_existing: bool,
) -> None:
    normalized = normalize_config_contents(raw)
    for key, value in normalized.items():
        if skip_existing and key in values:
            continue
        values[key] = ConfigValue(
            key=key,
            value=value,
            source=ConfigSource.CONFIG_FILE,
            location=location,
        )


def _apply_env_overrides(values: dict[str, ConfigValue]) -> None:
    env_mappings = _get_env_var_mappings()
    for key, env_var in env_mappings.items():
        env_value = os.environ.get(env_var)
        if env_value is None:
            continue
        values[key] = ConfigValue(
            key=key,
            value=_parse_env_value(env_value),
            source=ConfigSource.ENV,
            location=env_var,
        )


def _get_env_var_mappings() -> dict[str, str]:
    """Get mapping of config keys to environment variable names.

    Returns
    -------
    dict[str, str]
        Mapping of config key to environment variable name.
    """
    return {
        "log_level": "CODEANATOMY_LOG_LEVEL",
        "runtime_profile_name": "CODEANATOMY_RUNTIME_PROFILE",
        "output_dir": "CODEANATOMY_OUTPUT_DIR",
        "work_dir": "CODEANATOMY_WORK_DIR",
        "execution_mode": "CODEANATOMY_EXECUTION_MODE",
        "determinism_override": "CODEANATOMY_DETERMINISM_TIER",
        "incremental_state_dir": "CODEANATOMY_STATE_DIR",
        "incremental_repo_id": "CODEANATOMY_REPO_ID",
        "incremental_impact_strategy": "CODEANATOMY_INCREMENTAL_IMPACT_STRATEGY",
        "incremental_git_base_ref": "CODEANATOMY_GIT_BASE_REF",
        "incremental_git_head_ref": "CODEANATOMY_GIT_HEAD_REF",
        "incremental_git_changed_only": "CODEANATOMY_GIT_CHANGED_ONLY",
        "disable_scip": "CODEANATOMY_DISABLE_SCIP",
        "scip_output_dir": "CODEANATOMY_SCIP_OUTPUT_DIR",
    }


def _parse_env_value(value: str) -> JsonValue:
    """Parse environment variable value to appropriate type.

    Parameters
    ----------
    value
        String value from environment.

    Returns
    -------
    JsonValue
        Parsed value (bool, int, or string).
    """
    lower = value.lower()
    if lower in {"true", "1", "yes", "on"}:
        return True
    if lower in {"false", "0", "no", "off"}:
        return False
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


__all__ = [
    "load_effective_config",
    "load_effective_config_with_sources",
    "normalize_config_contents",
]
