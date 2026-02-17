"""View registry and management."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING

from utils.lazy_module import make_lazy_loader

if TYPE_CHECKING:
    from datafusion_engine.views.graph import (
        SchemaContractViolationError,
        ViewGraphOptions,
        ViewNode,
        register_view_graph,
        view_graph_registry,
    )

__all__ = [
    "SchemaContractViolationError",
    "ViewGraphOptions",
    "ViewNode",
    "register_view_graph",
    "view_graph_registry",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "SchemaContractViolationError": (
        "datafusion_engine.views.graph",
        "SchemaContractViolationError",
    ),
    "ViewGraphOptions": ("datafusion_engine.views.graph", "ViewGraphOptions"),
    "ViewNode": ("datafusion_engine.views.graph", "ViewNode"),
    "register_view_graph": ("datafusion_engine.views.graph", "register_view_graph"),
    "view_graph_registry": ("datafusion_engine.views.graph", "view_graph_registry"),
}

_lazy_getattr, _lazy_dir = make_lazy_loader(_EXPORT_MAP, __name__, globals())


def __getattr__(name: str) -> object:
    if name == "VIEW_SELECT_REGISTRY":
        warnings.warn(
            "VIEW_SELECT_REGISTRY is deprecated and has been removed.",
            DeprecationWarning,
            stacklevel=2,
        )
        raise AttributeError(name)
    return _lazy_getattr(name)


def __dir__() -> list[str]:
    return sorted([*_lazy_dir(), "VIEW_SELECT_REGISTRY"])
