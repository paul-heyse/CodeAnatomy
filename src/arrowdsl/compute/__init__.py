"""Compute-layer expressions, predicates, and kernels."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

__all__ = ["pc"]


def __getattr__(name: str) -> object:
    if name == "pc":
        return importlib.import_module("pyarrow.compute")
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


if TYPE_CHECKING:
    from pyarrow import compute as pc


def __dir__() -> list[str]:
    return sorted(__all__)
