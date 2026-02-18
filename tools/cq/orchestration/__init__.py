"""CQ orchestration layer integrating core, search, query, and macros."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.orchestration import (
        bundles,
        multilang_summary,
        orchestrator,
        request_factory,
        schema_export,
    )

__all__ = [
    "bundles",
    "multilang_summary",
    "orchestrator",
    "request_factory",
    "schema_export",
]


def __getattr__(name: str) -> object:
    if name in __all__:
        return import_module(f"{__name__}.{name}")
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
