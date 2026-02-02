"""Configuration providers for cq CLI.

This module provides config chain building for cyclopts integration,
supporting environment variables and pyproject.toml configuration.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


def build_config_chain(
    config_file: str | None = None,
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
