"""Configuration providers for cq CLI.

This module provides config chain building for cyclopts integration,
supporting environment variables and pyproject.toml configuration.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import msgspec

from tools.cq.cli_app.config_types import CqConfig


def build_config_chain(
    config_file: str | None = None,
    *,
    no_config: bool = False,
) -> list[Any]:
    """Build config provider chain based on CLI options.

    The config chain supports:
    1. Environment variables with CQ_ prefix (highest priority)
    2. Explicit config file (if provided)
    3. pyproject.toml [tool.cq] section (default, lowest priority)

    Parameters
    ----------
    config_file
        Optional explicit path to a TOML config file.
    no_config
        If True, skip all config file loading (env vars only).

    Returns:
    -------
    list[Any]
        List of config providers for cyclopts (Env, Toml, etc.).

    Examples:
    --------
    >>> # Normal usage with pyproject.toml
    >>> providers = build_config_chain()
    >>> len(providers)
    2

    >>> # Skip config files entirely
    >>> providers = build_config_chain(no_config=True)
    >>> len(providers)
    0
    """
    from cyclopts.config import Env, Toml

    if no_config:
        return []

    providers: list[Any] = [Env(prefix="CQ_")]

    if config_file:
        providers.append(Toml(Path(config_file), must_exist=True))
    else:
        providers.append(Toml("pyproject.toml", root_keys=("tool", "cq"), must_exist=False))

    return providers


def load_typed_config(
    config_file: str | None = None,
    *,
    no_config: bool = False,
) -> CqConfig | None:
    """Load typed config using msgspec TOML decoding.

    Parameters
    ----------
    config_file
        Optional explicit path to a TOML config file.
    no_config
        If True, skip loading configuration.

    Returns:
    -------
    CqConfig | None
        Parsed configuration, or None if no config is available.

    """
    if no_config:
        return None
    path = _resolve_config_path(config_file)
    if path is None:
        return None

    raw = _read_toml_dict(path)
    if raw is None:
        return None

    data = _extract_cq_section(raw)
    if data is None:
        return None

    coerced = _coerce_config_data(data)
    return _convert_config_data(coerced)


def _resolve_config_path(config_file: str | None) -> Path | None:
    if config_file:
        path = Path(config_file)
        if not path.exists():
            msg = f"Config file not found: {config_file}"
            raise FileNotFoundError(msg)
        return path
    path = Path("pyproject.toml")
    return path if path.exists() else None


def _read_toml_dict(path: Path) -> dict[str, object] | None:
    raw = msgspec.toml.decode(path.read_bytes())
    return raw if isinstance(raw, dict) else None


def _extract_cq_section(raw: dict[str, object]) -> dict[str, object] | None:
    tool_section = raw.get("tool")
    if isinstance(tool_section, dict):
        cq_section = tool_section.get("cq")
        if isinstance(cq_section, dict):
            return cq_section
    cq_section = raw.get("cq")
    if isinstance(cq_section, dict):
        return cq_section
    return None


def _env_value(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _collect_env_overrides() -> dict[str, object]:
    data: dict[str, object] = {}

    if (value := _env_value("CQ_ROOT")) is not None:
        data["root"] = value
    if (value := _env_value("CQ_VERBOSE")) is not None:
        data["verbose"] = value
    if (value := _env_value("CQ_FORMAT")) is not None:
        data["format"] = value
    if (value := _env_value("CQ_ARTIFACT_DIR")) is not None:
        data["artifact_dir"] = value
    if (value := _env_value("CQ_SAVE_ARTIFACT")) is not None:
        data["save_artifact"] = value
    if (value := _env_value("CQ_NO_SAVE_ARTIFACT")) is not None:
        data["no_save_artifact"] = value

    return data


def load_typed_env_config() -> CqConfig | None:
    """Load typed config overrides from CQ_* environment variables.

    Returns:
    -------
    CqConfig | None
        Parsed configuration, or None if no environment values are set.
    """
    data = _collect_env_overrides()
    if not data:
        return None

    coerced = _coerce_config_data(data)
    return _convert_config_data(coerced)


def _coerce_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        if value in {0, 1}:
            return bool(value)
        return None
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return None


def _coerce_int(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return None


def _coerce_str(value: object) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, Path):
        return str(value)
    return None


def _coerce_config_data(data: dict[str, object]) -> dict[str, object] | None:
    allowed = {
        "root",
        "verbose",
        "format",
        "output_format",
        "artifact_dir",
        "save_artifact",
        "no_save_artifact",
    }
    coerced: dict[str, object] = {}

    for key, value in data.items():
        if key not in allowed:
            return None
        if value is None:
            continue
        normalized_key = "output_format" if key == "format" else key
        if normalized_key in {"root", "output_format", "artifact_dir"}:
            coerced_value = _coerce_str(value)
        elif normalized_key == "verbose":
            coerced_value = _coerce_int(value)
        elif normalized_key in {"save_artifact", "no_save_artifact"}:
            coerced_value = _coerce_bool(value)
        else:
            coerced_value = None
        if coerced_value is None:
            return None
        coerced[normalized_key] = coerced_value

    if "no_save_artifact" in coerced and "save_artifact" not in coerced:
        no_save = coerced.pop("no_save_artifact")
        if isinstance(no_save, bool):
            coerced["save_artifact"] = not no_save
    else:
        coerced.pop("no_save_artifact", None)

    return coerced or None


def _convert_config_data(data: dict[str, object] | None) -> CqConfig | None:
    if not data:
        return None
    try:
        return msgspec.convert(data, type=CqConfig, strict=True)
    except msgspec.ValidationError:
        return None


__all__ = [
    "build_config_chain",
    "load_typed_config",
    "load_typed_env_config",
]
