"""Registry-backed detail kinds for enrichment preview/expand flows."""

from __future__ import annotations

from types import MappingProxyType

import msgspec

from tools.cq.core.structs import CqStruct


class KindSpec(CqStruct, frozen=True):
    """Details-kind registry entry."""

    kind: str
    version: int = 1
    rank: int = 1000
    preview_keys: tuple[str, ...] = msgspec.field(default_factory=tuple)


_DETAILS_KIND_REGISTRY_MUTABLE: dict[str, KindSpec] = {
    "sym.scope_graph": KindSpec(kind="sym.scope_graph", rank=100, preview_keys=("tables_count",)),
    "sym.partitions": KindSpec(kind="sym.partitions", rank=110, preview_keys=("partition_keys",)),
    "sym.binding_resolve": KindSpec(
        kind="sym.binding_resolve",
        rank=120,
        preview_keys=("status", "candidates_count"),
    ),
    "dis.cfg": KindSpec(kind="dis.cfg", rank=230, preview_keys=("edges_n", "exc_edges_n")),
    "dis.anchor_metrics": KindSpec(
        kind="dis.anchor_metrics",
        rank=240,
        preview_keys=("anchor_defs", "anchor_uses"),
    ),
    "inspect.object_inventory": KindSpec(
        kind="inspect.object_inventory",
        rank=320,
        preview_keys=("object_kind", "members_count"),
    ),
    "inspect.callsite_bind_check": KindSpec(
        kind="inspect.callsite_bind_check",
        rank=330,
        preview_keys=("bind_ok",),
    ),
}
DETAILS_KIND_REGISTRY = MappingProxyType(_DETAILS_KIND_REGISTRY_MUTABLE)


def resolve_kind(kind: str) -> KindSpec | None:
    """Resolve kind string to registry entry.

    Returns:
        KindSpec | None: Registered specification when present.
    """
    return DETAILS_KIND_REGISTRY.get(kind)


def preview_for_kind(kind: str, payload: dict[str, object]) -> dict[str, object]:
    """Project a payload preview according to the registered kind spec.

    Returns:
        dict[str, object]: Preview subset keyed by configured preview fields.
    """
    spec = resolve_kind(kind)
    if spec is None:
        return {}
    preview: dict[str, object] = {}
    for key in spec.preview_keys:
        if key in payload:
            preview[key] = payload[key]
    return preview


def sorted_kinds() -> tuple[KindSpec, ...]:
    """Return registry rows sorted by rank then kind."""
    return tuple(sorted(DETAILS_KIND_REGISTRY.values(), key=lambda spec: (spec.rank, spec.kind)))


__all__ = [
    "DETAILS_KIND_REGISTRY",
    "KindSpec",
    "preview_for_kind",
    "resolve_kind",
    "sorted_kinds",
]
