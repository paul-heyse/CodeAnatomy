"""DataFusion SessionConfig helper utilities."""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from datafusion import SessionConfig


def apply_config_value(
    config: SessionConfig,
    *,
    method: str,
    key: str,
    value: int | bool | str,
) -> SessionConfig:
    """Apply a configuration value to SessionConfig.

    Parameters
    ----------
    config
        DataFusion session configuration.
    method
        Method name to call.
    key
        Configuration key.
    value
        Configuration value (converted to string for setter fallback).

    Returns
    -------
    SessionConfig
        Updated configuration.
    """
    updater = getattr(config, method, None)
    if callable(updater):
        updated = updater(value)
        return cast("SessionConfig", updated)
    setter = getattr(config, "set", None)
    if callable(setter):
        str_value = str(value).lower() if isinstance(value, bool) else str(value)
        updated = setter(key, str_value)
        return cast("SessionConfig", updated)
    return config


def apply_optional_config(
    config: SessionConfig,
    *,
    method: str,
    key: str,
    value: int | bool | str | None,
) -> SessionConfig:
    """Apply an optional configuration value to SessionConfig.

    Parameters
    ----------
    config
        DataFusion session configuration.
    method
        Method name to call.
    key
        Configuration key.
    value
        Configuration value. If None, config is returned unchanged.

    Returns
    -------
    SessionConfig
        Updated configuration (unchanged if value is None).
    """
    if value is None:
        return config
    return apply_config_value(config, method=method, key=key, value=value)


def apply_setting(
    config: SessionConfig,
    *,
    key: str,
    value: int | bool | str,
) -> SessionConfig:
    """Apply a configuration setting via SessionConfig.set().

    Returns
    -------
    SessionConfig
        Updated configuration.
    """
    setter = getattr(config, "set", None)
    if callable(setter):
        str_value = str(value).lower() if isinstance(value, bool) else str(value)
        updated = setter(key, str_value)
        return cast("SessionConfig", updated)
    return config


def apply_optional_setting(
    config: SessionConfig,
    *,
    key: str,
    value: int | bool | str | None,
) -> SessionConfig:
    """Apply an optional configuration setting via SessionConfig.set().

    Returns
    -------
    SessionConfig
        Updated configuration.
    """
    if value is None:
        return config
    return apply_setting(config, key=key, value=value)


def apply_int_config(
    config: SessionConfig,
    *,
    method: str,
    key: str,
    value: int | None,
) -> SessionConfig:
    """Apply optional integer configuration.

    Returns
    -------
    SessionConfig
        Updated configuration.
    """
    return apply_optional_config(config, method=method, key=key, value=value)


def apply_bool_config(
    config: SessionConfig,
    *,
    method: str,
    key: str,
    value: bool | None,
) -> SessionConfig:
    """Apply optional boolean configuration.

    Returns
    -------
    SessionConfig
        Updated configuration.
    """
    return apply_optional_config(config, method=method, key=key, value=value)


def apply_str_config(
    config: SessionConfig,
    *,
    method: str,
    key: str,
    value: str | None,
) -> SessionConfig:
    """Apply optional string configuration.

    Returns
    -------
    SessionConfig
        Updated configuration.
    """
    return apply_optional_config(config, method=method, key=key, value=value)


__all__ = [
    "apply_bool_config",
    "apply_config_value",
    "apply_int_config",
    "apply_optional_config",
    "apply_optional_setting",
    "apply_setting",
    "apply_str_config",
]
