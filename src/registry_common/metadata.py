"""Shared helpers for registry metadata payloads."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass


def json_bytes(payload: object) -> bytes:
    """Serialize a payload to compact JSON bytes.

    Returns
    -------
    bytes
        UTF-8 encoded JSON payload.
    """
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


@dataclass(frozen=True)
class EvidenceMetadataSpec:
    """Specification for common evidence metadata fields."""

    evidence_family: str
    coordinate_system: str
    ambiguity_policy: str
    superior_rank: int
    span_coord_policy: bytes | None = None
    streaming_safe: bool | None = None
    pipeline_breaker: bool | None = None


def evidence_metadata(
    *,
    spec: EvidenceMetadataSpec,
    extra: Mapping[bytes, bytes] | None = None,
) -> dict[bytes, bytes]:
    """Return evidence metadata with common keys applied.

    Returns
    -------
    dict[bytes, bytes]
        Metadata payload with evidence keys applied.
    """
    meta: dict[bytes, bytes] = {
        b"evidence_family": spec.evidence_family.encode("utf-8"),
        b"coordinate_system": spec.coordinate_system.encode("utf-8"),
        b"ambiguity_policy": spec.ambiguity_policy.encode("utf-8"),
        b"superior_rank": str(spec.superior_rank).encode("utf-8"),
    }
    if spec.span_coord_policy is not None:
        meta[b"span_coord_policy"] = spec.span_coord_policy
    if spec.streaming_safe is not None:
        meta[b"streaming_safe"] = str(spec.streaming_safe).lower().encode("utf-8")
    if spec.pipeline_breaker is not None:
        meta[b"pipeline_breaker"] = str(spec.pipeline_breaker).lower().encode("utf-8")
    if extra:
        meta.update(extra)
    return meta


__all__ = ["EvidenceMetadataSpec", "evidence_metadata", "json_bytes"]
