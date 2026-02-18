"""Typed contracts and canonicalization helpers for lane payload keys."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Literal

import msgspec

from tools.cq.core.structs import CqOutputStruct

EnrichmentStatus = Literal["applied", "degraded", "skipped"]


class PythonTreeSitterPayloadV1(CqOutputStruct, frozen=True):
    """Canonical Python lane payload contract subset."""

    language: Literal["python"] = "python"
    enrichment_status: EnrichmentStatus = "applied"
    cst_diagnostics: list[dict[str, object]] = msgspec.field(default_factory=list)
    cst_query_hits: list[dict[str, object]] = msgspec.field(default_factory=list)
    query_runtime: dict[str, object] = msgspec.field(default_factory=dict)


class RustTreeSitterPayloadV1(CqOutputStruct, frozen=True):
    """Canonical Rust lane payload contract subset."""

    language: Literal["rust"] = "rust"
    enrichment_status: EnrichmentStatus = "applied"
    cst_diagnostics: list[dict[str, object]] = msgspec.field(default_factory=list)
    cst_query_hits: list[dict[str, object]] = msgspec.field(default_factory=list)
    query_runtime: dict[str, object] = msgspec.field(default_factory=dict)


def _coerce_mapping_rows(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]


def _canonicalize_lane_payload(
    payload: Mapping[str, object],
    *,
    target_type: type[PythonTreeSitterPayloadV1 | RustTreeSitterPayloadV1],
) -> PythonTreeSitterPayloadV1 | RustTreeSitterPayloadV1:
    """Canonicalize tree-sitter lane payload diagnostics/query-hit keys.

    Returns:
        Typed canonical lane payload contract.
    """
    payload = dict(payload)
    legacy = payload.pop("tree_sitter_diagnostics", None)
    if "cst_diagnostics" not in payload and isinstance(legacy, list):
        payload["cst_diagnostics"] = legacy
    payload["cst_diagnostics"] = _coerce_mapping_rows(payload.get("cst_diagnostics"))
    payload["cst_query_hits"] = _coerce_mapping_rows(payload.get("cst_query_hits"))
    return msgspec.convert(payload, type=target_type, strict=False)


def canonicalize_python_lane_payload(payload: Mapping[str, object]) -> PythonTreeSitterPayloadV1:
    """Canonicalize Python lane payload diagnostics/query-hit keys.

    Returns:
        Typed canonical Python lane payload.
    """
    return msgspec.convert(
        _canonicalize_lane_payload(payload, target_type=PythonTreeSitterPayloadV1),
        type=PythonTreeSitterPayloadV1,
        strict=False,
    )


def canonicalize_rust_lane_payload(payload: Mapping[str, object]) -> RustTreeSitterPayloadV1:
    """Canonicalize Rust lane payload diagnostics/query-hit keys.

    Returns:
        Typed canonical Rust lane payload.
    """
    return msgspec.convert(
        _canonicalize_lane_payload(payload, target_type=RustTreeSitterPayloadV1),
        type=RustTreeSitterPayloadV1,
        strict=False,
    )


__all__ = [
    "PythonTreeSitterPayloadV1",
    "RustTreeSitterPayloadV1",
    "canonicalize_python_lane_payload",
    "canonicalize_rust_lane_payload",
]
