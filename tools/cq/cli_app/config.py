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

    Returns
    -------
    list[Any]
        List of config providers for cyclopts (Env, Toml, etc.).

    Examples
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
        providers.append(
            Toml("pyproject.toml", root_keys=("tool", "cq"), must_exist=False)
        )

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

    Returns
    -------
    CqConfig | None
        Parsed configuration, or None if no config is available.

    Raises
    ------
    FileNotFoundError
        If an explicit config file is provided but does not exist.
    """
    if no_config:
        return None

    if config_file:
        path = Path(config_file)
        if not path.exists():
            msg = f"Config file not found: {config_file}"
            raise FileNotFoundError(msg)
    else:
        path = Path("pyproject.toml")
        if not path.exists():
            return None

    raw = msgspec.toml.decode(path.read_bytes())
    if not isinstance(raw, dict):
        return None

    data: object = raw
    tool_section = raw.get("tool")
    if isinstance(tool_section, dict):
        cq_section = tool_section.get("cq")
        if isinstance(cq_section, dict):
            data = cq_section

    try:
        return msgspec.convert(data, type=CqConfig)
    except msgspec.ValidationError:
        return None


def _env_value(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _parse_bool(value: str) -> bool | None:
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _parse_int(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None


def _collect_env_overrides() -> dict[str, object]:
    data: dict[str, object] = {}

    if (value := _env_value("CQ_ROOT")) is not None:
        data["root"] = value
    if (value := _env_value("CQ_VERBOSE")) is not None:
        parsed = _parse_int(value)
        if parsed is not None:
            data["verbose"] = parsed
    if (value := _env_value("CQ_FORMAT")) is not None:
        data["output_format"] = value
    if (value := _env_value("CQ_ARTIFACT_DIR")) is not None:
        data["artifact_dir"] = value
    if (value := _env_value("CQ_SAVE_ARTIFACT")) is not None:
        parsed = _parse_bool(value)
        if parsed is not None:
            data["save_artifact"] = parsed
    if (value := _env_value("CQ_NO_SAVE_ARTIFACT")) is not None:
        parsed = _parse_bool(value)
        if parsed is not None:
            data["save_artifact"] = not parsed

    return data


def load_typed_env_config() -> CqConfig | None:
    """Load typed config overrides from CQ_* environment variables.

    Returns
    -------
    CqConfig | None
        Parsed configuration, or None if no environment values are set.
    """
    data = _collect_env_overrides()
    if not data:
        return None

    try:
        return msgspec.convert(data, type=CqConfig)
    except msgspec.ValidationError:
        return None


__all__ = [
    "build_config_chain",
    "load_typed_config",
    "load_typed_env_config",
]
