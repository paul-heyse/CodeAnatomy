"""View registry and management."""

from __future__ import annotations

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

__getattr__, __dir__ = make_lazy_loader(_EXPORT_MAP, __name__, globals())
