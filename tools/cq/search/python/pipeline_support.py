"""Python pipeline support: stages, ast-grep rules, resolution payloads, and orchestration."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING

import msgspec

from tools.cq.core.structs import CqOutputStruct
from tools.cq.search._shared.requests import PythonByteRangeEnrichmentRequest
from tools.cq.search.python.extractors_entrypoints import extract_python_byte_range
from tools.cq.search.python.resolution_index import build_resolution_index

if TYPE_CHECKING:
    from ast_grep_py import SgNode

# --- From stages.py ---


def extract_signature_stage(node: SgNode) -> dict[str, object]:
    """Extract a bounded signature preview from an ast-grep node.

    Returns:
        dict[str, object]: Function return value.
    """
    signature = node.text().split("{", maxsplit=1)[0].strip()
    if not signature:
        return {}
    return {"signature": signature[:200]}


# --- From ast_grep_rules.py ---


def find_import_aliases(node: SgNode) -> list[SgNode]:
    """Find ``import X as Y`` alias bindings with identifier constraints.

    Returns:
        list[SgNode]: Function return value.
    """
    return node.find_all(
        {
            "rule": {"pattern": "import $X as $Y"},
            "constraints": {"Y": {"regex": "^[A-Za-z_][A-Za-z0-9_]*$"}},
        }
    )


# --- From resolution_payload.py ---


class PythonResolutionPayloadV1(CqOutputStruct, frozen=True):
    """Normalized subset of Python native-resolution output."""

    symbol: str | None = None
    symbol_role: str | None = None
    enclosing_callable: str | None = None
    enclosing_class: str | None = None
    qualified_name_candidates: tuple[dict[str, object], ...] = msgspec.field(default_factory=tuple)


def coerce_resolution_payload(payload: dict[str, object]) -> PythonResolutionPayloadV1:
    """Coerce raw resolution mapping into typed contract payload.

    Returns:
        PythonResolutionPayloadV1: Function return value.
    """
    return msgspec.convert(payload, type=PythonResolutionPayloadV1, strict=False)


# --- From orchestrator.py ---


def run_python_enrichment_pipeline(
    request: PythonByteRangeEnrichmentRequest,
) -> dict[str, object]:
    """Run enrichment + native-resolution merge for one byte-range request.

    Returns:
        dict[str, object]: Function return value.
    """
    from tools.cq.search.python.evidence import evaluate_python_semantic_signal_from_mapping

    payload = extract_python_byte_range(request)
    resolution = build_resolution_index(
        source_bytes=request.source_bytes,
        byte_start=request.byte_start,
        byte_end=request.byte_end,
        cache_key=request.cache_key,
        session=request.session,
    )
    if resolution:
        payload.setdefault("resolution", resolution)
    has_signal, reasons = evaluate_python_semantic_signal_from_mapping(
        payload if isinstance(payload, Mapping) else {}
    )
    payload["semantic_signal"] = {"has_signal": has_signal, "reasons": list(reasons)}
    return payload


__all__ = [
    "PythonResolutionPayloadV1",
    "coerce_resolution_payload",
    "extract_signature_stage",
    "find_import_aliases",
    "run_python_enrichment_pipeline",
]
