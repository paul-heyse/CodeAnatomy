"""Typed contracts and canonicalization helpers for lane payload keys."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

import msgspec

from tools.cq.core.structs import CqOutputStruct

EnrichmentStatus = Literal["applied", "degraded", "skipped"]


class PythonTreeSitterPayloadV1(CqOutputStruct, frozen=True):
    """Canonical Python lane payload contract subset."""

    language: Literal["python"] = "python"
    enrichment_status: EnrichmentStatus = "applied"
    cst_diagnostics: list[dict[str, Any]] = msgspec.field(default_factory=list)
    cst_query_hits: list[dict[str, Any]] = msgspec.field(default_factory=list)
    query_runtime: dict[str, Any] = msgspec.field(default_factory=dict)


class RustTreeSitterPayloadV1(CqOutputStruct, frozen=True):
    """Canonical Rust lane payload contract subset."""

    language: Literal["rust"] = "rust"
    enrichment_status: EnrichmentStatus = "applied"
    cst_diagnostics: list[dict[str, Any]] = msgspec.field(default_factory=list)
    cst_query_hits: list[dict[str, Any]] = msgspec.field(default_factory=list)
    query_runtime: dict[str, Any] = msgspec.field(default_factory=dict)


def _coerce_mapping_rows(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]


def _canonicalize_lane_payload(
    payload: dict[str, Any],
    *,
    target_type: type[PythonTreeSitterPayloadV1 | RustTreeSitterPayloadV1],
) -> dict[str, Any]:
    """Canonicalize tree-sitter lane payload diagnostics/query-hit keys.

    Returns:
        dict[str, Any]: Canonicalized lane payload.
    """
    payload = dict(payload)
    legacy = payload.pop("tree_sitter_diagnostics", None)
    if "cst_diagnostics" not in payload and isinstance(legacy, list):
        payload["cst_diagnostics"] = legacy
    payload["cst_diagnostics"] = _coerce_mapping_rows(payload.get("cst_diagnostics"))
    payload["cst_query_hits"] = _coerce_mapping_rows(payload.get("cst_query_hits"))
    _ = msgspec.convert(payload, type=target_type, strict=False)
    return payload


def canonicalize_python_lane_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Canonicalize Python lane payload diagnostics/query-hit keys.

    Returns:
        dict[str, Any]: Function return value.
    """
    return _canonicalize_lane_payload(payload, target_type=PythonTreeSitterPayloadV1)


def canonicalize_rust_lane_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Canonicalize Rust lane payload diagnostics/query-hit keys.

    Returns:
        dict[str, Any]: Function return value.
    """
    return _canonicalize_lane_payload(payload, target_type=RustTreeSitterPayloadV1)


__all__ = [
    "PythonTreeSitterPayloadV1",
    "RustTreeSitterPayloadV1",
    "canonicalize_python_lane_payload",
    "canonicalize_rust_lane_payload",
]
