"""Config loading and normalization helpers for the CLI."""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

import msgspec

from cli.config_models import RootConfig
from cli.config_source import ConfigSource, ConfigValue, ConfigWithSources
from core_types import JsonValue
from serde_msgspec import validation_error_payload

_DIAGNOSTICS_PROFILE_PRESETS: dict[str, dict[str, JsonValue]] = {
    "debug": {
        "enable_plan_diagnostics": True,
        "enable_hamilton_node_diagnostics": True,
        "enable_structured_run_logs": True,
        "enable_otel_node_tracing": True,
    },
    "dev": {
        "enable_plan_diagnostics": True,
        "enable_hamilton_node_diagnostics": True,
        "enable_structured_run_logs": True,
    },
    "prod": {
        "enable_plan_diagnostics": False,
        "enable_hamilton_node_diagnostics": False,
        "enable_structured_run_logs": False,
        "enable_otel_node_tracing": False,
    },
}

_LEGACY_SECTION_MAPS: dict[str, dict[str, str]] = {
    "plan": {
        "plan_allow_partial": "allow_partial",
        "plan_requested_tasks": "requested_tasks",
        "plan_impacted_tasks": "impacted_tasks",
        "enable_metric_scheduling": "enable_metric_scheduling",
        "enable_plan_diagnostics": "enable_plan_diagnostics",
        "enable_plan_task_submission_hook": "enable_plan_task_submission_hook",
        "enable_plan_task_grouping_hook": "enable_plan_task_grouping_hook",
        "enforce_plan_task_submission": "enforce_plan_task_submission",
    },
    "cache": {
        "cache_policy_profile": "policy_profile",
        "cache_path": "path",
        "cache_log_to_file": "log_to_file",
        "cache_opt_in": "opt_in",
    },
    "graph_adapter": {
        "graph_adapter_kind": "kind",
        "graph_adapter_options": "options",
    },
    "incremental": {
        "incremental_enabled": "enabled",
        "incremental_state_dir": "state_dir",
        "incremental_repo_id": "repo_id",
        "incremental_impact_strategy": "impact_strategy",
        "incremental_git_base_ref": "git_base_ref",
        "incremental_git_head_ref": "git_head_ref",
        "incremental_git_changed_only": "git_changed_only",
    },
    "otel": {
        "enable_otel_node_tracing": "enable_node_tracing",
        "enable_otel_plan_tracing": "enable_plan_tracing",
        "otel_endpoint": "endpoint",
        "otel_protocol": "protocol",
        "otel_sampler": "sampler",
        "otel_sampler_arg": "sampler_arg",
        "otel_log_correlation": "log_correlation",
        "otel_metric_export_interval_ms": "metric_export_interval_ms",
        "otel_metric_export_timeout_ms": "metric_export_timeout_ms",
        "otel_bsp_schedule_delay_ms": "bsp_schedule_delay_ms",
        "otel_bsp_export_timeout_ms": "bsp_export_timeout_ms",
        "otel_bsp_max_queue_size": "bsp_max_queue_size",
        "otel_bsp_max_export_batch_size": "bsp_max_export_batch_size",
        "otel_blrp_schedule_delay_ms": "blrp_schedule_delay_ms",
        "otel_blrp_export_timeout_ms": "blrp_export_timeout_ms",
        "otel_blrp_max_queue_size": "blrp_max_queue_size",
        "otel_blrp_max_export_batch_size": "blrp_max_export_batch_size",
    },
    "hamilton": {
        "enable_hamilton_tracker": "enable_tracker",
        "enable_hamilton_type_checker": "enable_type_checker",
        "enable_hamilton_node_diagnostics": "enable_node_diagnostics",
        "hamilton_graph_adapter_kind": "graph_adapter_kind",
        "hamilton_graph_adapter_options": "graph_adapter_options",
        "enable_structured_run_logs": "enable_structured_run_logs",
        "structured_log_path": "structured_log_path",
        "run_log_path": "run_log_path",
        "enable_graph_snapshot": "enable_graph_snapshot",
        "graph_snapshot_path": "graph_snapshot_path",
        "graph_snapshot_hamilton_path": "graph_snapshot_hamilton_path",
        "enable_cache_lineage": "enable_cache_lineage",
        "cache_lineage_path": "cache_lineage_path",
        "hamilton_cache_path": "cache_path",
        "hamilton_capture_data_statistics": "capture_data_statistics",
        "hamilton_max_list_length_capture": "max_list_length_capture",
        "hamilton_max_dict_length_capture": "max_dict_length_capture",
        "hamilton_tags": "tags",
        "hamilton_project_id": "project_id",
        "hamilton_username": "username",
        "hamilton_dag_name": "dag_name",
        "hamilton_api_url": "api_url",
        "hamilton_ui_url": "ui_url",
        "hamilton_telemetry_profile": "telemetry_profile",
    },
}


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
        if not path.exists():
            return {}
        raw, location = _resolve_explicit_payload(path)
        root = _decode_root_config(raw, location=location)
        normalized = normalize_config_contents(_config_to_mapping(root))
        overrides = _env_overrides()
        if overrides:
            normalized.update(overrides)
        return normalized

    config: RootConfig | None = None
    codeanatomy_path = _find_in_parents("codeanatomy.toml")
    if codeanatomy_path is not None:
        raw = _read_toml(codeanatomy_path)
        config = _decode_root_config(raw, location=str(codeanatomy_path))

    pyproject_path = _find_in_parents("pyproject.toml")
    if pyproject_path is not None:
        raw = _read_toml(pyproject_path)
        nested = _extract_tool_config(raw)
        if nested is not None:
            config = _decode_root_config(nested, location=f"{pyproject_path}:tool.codeanatomy")

    resolved = config or RootConfig()
    normalized = normalize_config_contents(_config_to_mapping(resolved))
    overrides = _env_overrides()
    if overrides:
        normalized.update(overrides)
    return normalized


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
    expanded = _apply_diagnostics_profile(config)
    flat: dict[str, JsonValue] = {}
    flat.update(expanded)

    for section_name, mapping in _LEGACY_SECTION_MAPS.items():
        section = expanded.get(section_name)
        if isinstance(section, dict):
            for flat_key, nested_key in mapping.items():
                _copy_key(flat, section, nested_key, flat_key)

    return flat


def _apply_diagnostics_profile(
    config: Mapping[str, JsonValue],
) -> dict[str, JsonValue]:
    value = config.get("diagnostics_profile")
    if not isinstance(value, str) or not value.strip():
        return dict(config)
    profile = value.strip().lower()
    preset = _DIAGNOSTICS_PROFILE_PRESETS.get(profile)
    if preset is None:
        choices = ", ".join(sorted(_DIAGNOSTICS_PROFILE_PRESETS))
        msg = f"Unsupported diagnostics_profile {value!r}. Available: {choices}."
        raise ValueError(msg)
    merged = dict(config)
    for key, preset_value in preset.items():
        applied = False
        for section_name, mapping in _LEGACY_SECTION_MAPS.items():
            nested_key = mapping.get(key)
            if nested_key is None:
                continue
            section = _ensure_section(merged, section_name)
            if section is None:
                break
            section[nested_key] = preset_value
            applied = True
            break
        if not applied:
            merged[key] = preset_value
    return merged


def _copy_key(
    target: dict[str, JsonValue],
    source: Mapping[str, JsonValue],
    source_key: str,
    dest_key: str,
) -> None:
    value = source.get(source_key)
    if value is not None:
        target[dest_key] = value


def _ensure_section(
    target: dict[str, JsonValue],
    section_name: str,
) -> dict[str, JsonValue] | None:
    existing = target.get(section_name)
    if existing is None:
        section: dict[str, JsonValue] = {}
        target[section_name] = section
        return section
    if isinstance(existing, dict):
        return existing
    if isinstance(existing, Mapping):
        section = dict(existing)
        target[section_name] = section
        return section
    return None


def _normalize_legacy_config(raw: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    payload = dict(raw)
    for section_name, mapping in _LEGACY_SECTION_MAPS.items():
        if not any(key in payload for key in mapping):
            continue
        section = _ensure_section(payload, section_name)
        if section is None:
            continue
        for legacy_key, nested_key in mapping.items():
            if legacy_key not in payload:
                continue
            if nested_key not in section:
                section[nested_key] = payload[legacy_key]
            payload.pop(legacy_key, None)
    return payload


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
    raw, location = _resolve_explicit_payload(path)
    root = _decode_root_config(raw, location=location)
    _apply_config_values(values, root, location=location, skip_existing=False)


def _load_default_configs(values: dict[str, ConfigValue]) -> None:
    codeanatomy_path = _find_in_parents("codeanatomy.toml")
    if codeanatomy_path is not None:
        raw = _read_toml(codeanatomy_path)
        root = _decode_root_config(raw, location=str(codeanatomy_path))
        _apply_config_values(values, root, location=str(codeanatomy_path), skip_existing=False)

    pyproject_path = _find_in_parents("pyproject.toml")
    if pyproject_path is None:
        return
    pyproject = _read_toml(pyproject_path)
    nested = _extract_tool_config(pyproject)
    if nested is None:
        return
    root = _decode_root_config(nested, location=f"{pyproject_path}:tool.codeanatomy")
    _apply_config_values(values, root, location=str(pyproject_path), skip_existing=True)


def _read_toml(path: Path) -> dict[str, JsonValue]:
    payload = msgspec.toml.decode(path.read_text(encoding="utf-8"), type=object, strict=True)
    if not isinstance(payload, dict):
        msg = f"Expected TOML mapping in {path}, got {type(payload).__name__}."
        raise TypeError(msg)
    return cast("dict[str, JsonValue]", payload)


def _apply_config_values(
    values: dict[str, ConfigValue],
    raw: RootConfig,
    *,
    location: str,
    skip_existing: bool,
) -> None:
    normalized = normalize_config_contents(_config_to_mapping(raw))
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


def _env_overrides() -> dict[str, JsonValue]:
    overrides: dict[str, JsonValue] = {}
    for key, env_var in _get_env_var_mappings().items():
        env_value = os.environ.get(env_var)
        if env_value is None:
            continue
        overrides[key] = _parse_env_value(env_value)
    return overrides


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
    if lower in {"none", "null"}:
        return None
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


def _decode_root_config(raw: Mapping[str, JsonValue], *, location: str) -> RootConfig:
    normalized = _normalize_legacy_config(raw)
    try:
        config = msgspec.convert(normalized, type=RootConfig, strict=True)
    except msgspec.ValidationError as exc:
        details = validation_error_payload(exc)
        msg = f"Config validation failed for {location}: {details}"
        raise ValueError(msg) from exc
    _validate_json_payloads(config, location=location)
    return config


def _validate_json_payloads(config: RootConfig, *, location: str) -> None:
    payloads: list[tuple[str, Mapping[str, object]]] = []
    if config.graph_adapter is not None and config.graph_adapter.options is not None:
        payloads.append(("graph_adapter.options", config.graph_adapter.options))
    if config.hamilton is not None and config.hamilton.graph_adapter_options is not None:
        payloads.append(("hamilton.graph_adapter_options", config.hamilton.graph_adapter_options))
    if config.hamilton is not None and config.hamilton.tags is not None:
        payloads.append(("hamilton.tags", config.hamilton.tags))
    for field, payload in payloads:
        _validate_json_mapping(payload, path=field, location=location)


def _validate_json_mapping(
    payload: Mapping[str, object],
    *,
    path: str,
    location: str,
) -> None:
    for key, value in payload.items():
        if not isinstance(key, str):
            msg = f"Config validation failed for {location}: {path} keys must be strings."
            raise TypeError(msg)
        _validate_json_value(value, path=f"{path}.{key}", location=location)


def _validate_json_value(value: object, *, path: str, location: str) -> None:
    if value is None or isinstance(value, (str, int, float, bool)):
        return
    if isinstance(value, Mapping):
        _validate_json_mapping(value, path=path, location=location)
        return
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray, memoryview),
    ):
        for index, item in enumerate(value):
            _validate_json_value(item, path=f"{path}[{index}]", location=location)
        return
    msg = f"Config validation failed for {location}: {path} must be JSON-compatible."
    raise ValueError(msg)


def _config_to_mapping(config: RootConfig) -> dict[str, JsonValue]:
    payload = msgspec.to_builtins(config, str_keys=True)
    return cast("dict[str, JsonValue]", payload)


def _resolve_explicit_payload(path: Path) -> tuple[Mapping[str, JsonValue], str]:
    raw = _read_toml(path)
    if _is_pyproject_config(path):
        nested = _extract_tool_config(raw)
        if nested is None:
            msg = f"Config validation failed for {path}: missing [tool.codeanatomy] section."
            raise ValueError(msg)
        return nested, f"{path}:tool.codeanatomy"
    return raw, str(path)


def _extract_tool_config(raw: Mapping[str, JsonValue]) -> dict[str, JsonValue] | None:
    tool_section = raw.get("tool")
    if not isinstance(tool_section, dict):
        return None
    nested = tool_section.get("codeanatomy")
    if not isinstance(nested, dict):
        return None
    return cast("dict[str, JsonValue]", nested)


def _is_pyproject_config(path: Path) -> bool:
    return path.name == "pyproject.toml"


__all__ = [
    "load_effective_config",
    "load_effective_config_with_sources",
    "normalize_config_contents",
]
