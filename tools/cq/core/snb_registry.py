"""Typed kind registry for SNB schema runtime validation."""

from __future__ import annotations

import msgspec

from tools.cq.core.snb_schema import (
    BundleMetaV1,
    DegradeEventV1,
    NeighborhoodGraphSummaryV1,
    NeighborhoodSliceV1,
    SemanticEdgeV1,
    SemanticNeighborhoodBundleRefV1,
    SemanticNeighborhoodBundleV1,
    SemanticNodeRefV1,
)

DETAILS_KIND_REGISTRY: dict[str, type[msgspec.Struct]] = {
    "cq.snb.bundle_ref.v1": SemanticNeighborhoodBundleRefV1,
    "cq.snb.bundle.v1": SemanticNeighborhoodBundleV1,
    "cq.snb.node.v1": SemanticNodeRefV1,
    "cq.snb.edge.v1": SemanticEdgeV1,
    "cq.snb.slice.v1": NeighborhoodSliceV1,
    "cq.snb.degrade.v1": DegradeEventV1,
    "cq.snb.bundle_meta.v1": BundleMetaV1,
    "cq.snb.graph_summary.v1": NeighborhoodGraphSummaryV1,
}


def resolve_kind(kind: str) -> type[msgspec.Struct] | None:
    """Resolve kind string to struct type for runtime validation.

    Parameters
    ----------
    kind : str
        Kind identifier string (e.g. "cq.snb.bundle.v1").

    Returns:
    -------
    type[msgspec.Struct] | None
        Struct type if kind is registered, None otherwise.
    """
    return DETAILS_KIND_REGISTRY.get(kind)


def validate_finding_details(details: dict[str, object]) -> bool:
    """Validate that finding details dict has valid kind and structure.

    Used by enrichment pipeline to catch malformed payloads early.

    Parameters
    ----------
    details : dict[str, object]
        Details dictionary to validate.

    Returns:
    -------
    bool
        True if details has a registered kind, False otherwise.
    """
    kind = details.get("kind")
    if not isinstance(kind, str):
        return False
    struct_type = resolve_kind(kind)
    return struct_type is not None


def decode_finding_details(details: dict[str, object]) -> msgspec.Struct | None:
    """Decode finding details dict into typed struct.

    Parameters
    ----------
    details : dict[str, object]
        Details dictionary to decode.

    Returns:
    -------
    msgspec.Struct | None
        Decoded struct instance if successful, None otherwise.
    """
    kind = details.get("kind")
    if not isinstance(kind, str):
        return None

    struct_type = resolve_kind(kind)
    if struct_type is None:
        return None

    try:
        return msgspec.convert(details, type=struct_type)
    except (msgspec.ValidationError, TypeError):
        return None


__all__ = [
    "DETAILS_KIND_REGISTRY",
    "decode_finding_details",
    "resolve_kind",
    "validate_finding_details",
]
