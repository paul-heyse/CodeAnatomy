"""Shared enrichment contract and helper exports."""

from importlib import import_module
from typing import TYPE_CHECKING

from tools.cq.search.enrichment.contracts import (
    EnrichmentMeta,
    EnrichmentStatus,
    PythonEnrichmentPayload,
    RustEnrichmentPayload,
)
from tools.cq.search.enrichment.core import (
    append_source,
    check_payload_budget,
    has_value,
    merge_gap_fill_payload,
    normalize_python_payload,
    normalize_rust_payload,
    payload_size_hint,
    set_degraded,
    trim_payload_to_budget,
)
from tools.cq.search.enrichment.incremental_compound_plane import (
    build_binding_join,
    build_compound_bundle,
)
from tools.cq.search.enrichment.incremental_dis_plane import build_dis_bundle
from tools.cq.search.enrichment.incremental_inspect_plane import (
    build_inspect_bundle,
    inspect_object_inventory,
)
from tools.cq.search.enrichment.incremental_symtable_plane import (
    build_incremental_symtable_plane,
    build_sym_scope_graph,
    resolve_binding_id,
)

if TYPE_CHECKING:
    from tools.cq.search.enrichment.incremental_provider import enrich_incremental_anchor

__all__ = [
    "EnrichmentMeta",
    "EnrichmentStatus",
    "PythonEnrichmentPayload",
    "RustEnrichmentPayload",
    "append_source",
    "build_binding_join",
    "build_compound_bundle",
    "build_dis_bundle",
    "build_incremental_symtable_plane",
    "build_inspect_bundle",
    "build_sym_scope_graph",
    "check_payload_budget",
    "enrich_incremental_anchor",
    "has_value",
    "inspect_object_inventory",
    "merge_gap_fill_payload",
    "normalize_python_payload",
    "normalize_rust_payload",
    "payload_size_hint",
    "resolve_binding_id",
    "set_degraded",
    "trim_payload_to_budget",
]


def __getattr__(name: str) -> object:
    if name == "enrich_incremental_anchor":
        module = import_module("tools.cq.search.enrichment.incremental_provider")
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(name)
