"""View registry and management."""

from __future__ import annotations

import warnings

__all__: list[str] = []


def __getattr__(name: str) -> object:
    if name == "VIEW_SELECT_REGISTRY":
        warnings.warn(
            "VIEW_SELECT_REGISTRY is deprecated and has been removed.",
            DeprecationWarning,
            stacklevel=2,
        )
        raise AttributeError(name)
    raise AttributeError(name)
