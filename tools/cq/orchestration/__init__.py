"""CQ orchestration layer integrating core, search, query, and macros."""

from __future__ import annotations

from importlib import import_module

__all__ = [
    "bundles",
    "multilang_orchestrator",
    "multilang_summary",
    "request_factory",
    "schema_export",
]


def __getattr__(name: str) -> object:
    if name in __all__:
        return import_module(f"{__name__}.{name}")
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)
