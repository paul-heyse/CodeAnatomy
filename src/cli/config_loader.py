"""Config loading and normalization helpers for the CLI."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import msgspec

from cli.config_models import RootConfigSpec
from cli.config_source import ConfigSource, ConfigValue, ConfigWithSources
from core_types import JsonValue
from runtime_models.adapters import ROOT_CONFIG_ADAPTER
from runtime_models.root import RootConfigRuntime
from serde_msgspec import validation_error_payload

logger = logging.getLogger(__name__)


def load_effective_config(config_file: str | None) -> dict[str, JsonValue]:
    """Load config contents from codeanatomy.toml / pyproject.toml or explicit --config.

    Parameters
    ----------
    config_file
        Optional explicit config file path.

    Returns:
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
        return normalize_config_contents(_config_to_mapping(root))

    config: RootConfigSpec | None = None
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

    resolved = config or RootConfigSpec()
    return normalize_config_contents(_config_to_mapping(resolved))


def normalize_config_contents(config: Mapping[str, JsonValue]) -> dict[str, JsonValue]:
    """Normalize config contents for downstream consumers.

    Parameters
    ----------
    config
        Raw configuration contents.

    Returns:
    -------
    dict[str, object]
        Normalized configuration payload.
    """
    return dict(config)


def _find_in_parents(filename: str) -> Path | None:
    """Walk parents from cwd to find a filename.

    Parameters
    ----------
    filename
        Filename to locate.

    Returns:
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

    Returns:
    -------
    ConfigWithSources
        Configuration with source tracking for each value.
    """
    values: dict[str, ConfigValue] = {}
    _load_config_values(values, config_file)
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
    raw: RootConfigSpec,
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


def _decode_root_config(raw: Mapping[str, JsonValue], *, location: str) -> RootConfigSpec:
    translated = _translate_deprecated_config(raw, location=location)
    try:
        config = msgspec.convert(translated, type=RootConfigSpec, strict=True)
    except msgspec.ValidationError as exc:
        details = validation_error_payload(exc)
        msg = f"Config validation failed for {location}: {details}"
        raise ValueError(msg) from exc
    _validate_root_runtime(config, location=location)
    return config


def _config_to_mapping(config: RootConfigSpec) -> dict[str, JsonValue]:
    payload = msgspec.to_builtins(config, str_keys=True)
    return cast("dict[str, JsonValue]", payload)


def _validate_root_runtime(config: RootConfigSpec, *, location: str) -> None:
    payload = msgspec.to_builtins(config, str_keys=True)
    try:
        ROOT_CONFIG_ADAPTER.validate_python(payload)
    except Exception as exc:
        msg = f"Config validation failed for {location}: {exc}"
        raise ValueError(msg) from exc


def _translate_deprecated_config(
    raw: Mapping[str, JsonValue],
    *,
    location: str,
) -> dict[str, JsonValue]:
    """Translate deprecated config sections to canonical engine config.

    Returns:
    -------
    dict[str, JsonValue]
        Normalized config payload with deprecated sections translated when possible.
    """
    payload = dict(raw)
    legacy_graph_adapter = payload.pop("graph_adapter", None)
    if isinstance(legacy_graph_adapter, Mapping):
        logger.warning(
            "Deprecated [graph_adapter] config detected at %s; section is ignored in Rust-first execution mode.",
            location,
        )
    legacy_hamilton = payload.get("hamilton")
    if "engine" in payload or not isinstance(legacy_hamilton, Mapping):
        return payload
    translated_engine: dict[str, JsonValue] = {}
    key_map = {
        "profile": "profile",
        "rulepack_profile": "rulepack_profile",
        "enable_compliance": "compliance_capture",
        "enable_rule_tracing": "rule_tracing",
        "enable_plan_preview": "plan_preview",
        "tracing_preset": "tracing_preset",
        "instrument_object_store": "instrument_object_store",
    }
    for old_key, new_key in key_map.items():
        value = legacy_hamilton.get(old_key)
        if value is not None:
            translated_engine[new_key] = value
    if translated_engine:
        payload["engine"] = translated_engine
        logger.warning(
            "Deprecated [hamilton] config detected at %s; translated to [engine].",
            location,
        )
    return payload


def _resolve_explicit_payload(path: Path) -> tuple[Mapping[str, JsonValue], str]:
    if path.suffix == ".json":
        raw_text = path.read_text(encoding="utf-8")
        try:
            RootConfigRuntime.model_validate_json(raw_text)
        except Exception as exc:
            msg = f"Config validation failed for {path}: {exc}"
            raise ValueError(msg) from exc
        raw = json.loads(raw_text)
        if not isinstance(raw, dict):
            msg = f"Config validation failed for {path}: JSON root must be an object."
            raise ValueError(msg)
        return cast("Mapping[str, JsonValue]", raw), str(path)
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
