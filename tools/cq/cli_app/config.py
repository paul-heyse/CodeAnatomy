"""Configuration providers for cq CLI."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def build_config_chain(
    config_file: str | None = None,
    *,
    use_config: bool = True,
) -> list[Any]:
    """Build Cyclopts config providers with `CLI > env > config > defaults` precedence.

    Returns:
        list[Any]: Ordered config providers for Cyclopts.
    """
    from cyclopts.config import Env, Toml

    providers: list[Any] = [Env(prefix="CQ_", command=False)]
    if not use_config:
        return providers

    if config_file:
        providers.append(Toml(Path(config_file), must_exist=True))
    else:
        providers.append(Toml("pyproject.toml", root_keys=("tool", "cq"), must_exist=False))

    return providers


__all__ = [
    "build_config_chain",
]
