"""Index modules for definition resolution and call binding."""

from __future__ import annotations

from tools.cq.index.arg_binder import BoundCall, bind_call_to_params
from tools.cq.index.call_resolver import resolve_call_targets
from tools.cq.index.def_index import ClassDecl, DefIndex, FnDecl, ModuleInfo
from tools.cq.index.graph_utils import (
    find_sccs,
    find_simple_cycles,
    get_ancestors,
    get_descendants,
    topological_sort,
)

__all__ = [
    "BoundCall",
    "ClassDecl",
    "DefIndex",
    "FnDecl",
    "ModuleInfo",
    "bind_call_to_params",
    "find_sccs",
    "find_simple_cycles",
    "get_ancestors",
    "get_descendants",
    "resolve_call_targets",
    "topological_sort",
]
