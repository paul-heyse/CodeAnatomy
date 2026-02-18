"""Rust lane pipeline-stage ownership module."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from tools.cq.search._shared.error_boundaries import ENRICHMENT_ERRORS
from tools.cq.search.tree_sitter.rust_lane.extractor_dispatch import (
    _build_enrichment_payload,
)
from tools.cq.search.tree_sitter.rust_lane.extractor_dispatch import (
    canonicalize_payload as _canonicalize_payload,
)
from tools.cq.search.tree_sitter.rust_lane.runtime_cache import _parse_with_session
from tools.cq.search.tree_sitter.rust_lane.runtime_payload_builders import (
    collect_query_pack_payload,
)

if TYPE_CHECKING:
    from tree_sitter import Node

    from tools.cq.search.tree_sitter.core.parse import ParseSession

logger = logging.getLogger(__name__)

_REQUIRED_PAYLOAD_KEYS: tuple[str, ...] = (
    "language",
    "enrichment_status",
    "enrichment_sources",
)


@dataclass(frozen=True, slots=True)
class RustPayloadBuildRequestV1:
    node: Node
    tree_root: Node
    source_bytes: bytes
    changed_ranges: tuple[object, ...]
    byte_span: tuple[int, int]
    max_scope_depth: int
    query_budget_ms: int | None
    file_key: str | None


@dataclass(frozen=True, slots=True)
class RustPipelineRequestV1:
    source: str
    cache_key: str | None
    max_scope_depth: int
    query_budget_ms: int | None
    resolve_node: Callable[[Node], Node | None]
    byte_span_for_node: Callable[[Node], tuple[int, int]]
    error_prefix: str
    parse_session: ParseSession | None = None
    cache_backend: object | None = None


def _coerce_timings(payload: Mapping[str, object], *, key: str) -> dict[str, float]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        return {}
    return {
        stage: float(duration)
        for stage, duration in value.items()
        if isinstance(stage, str) and isinstance(duration, (int, float))
    }


def _coerce_status(payload: Mapping[str, object], *, key: str) -> dict[str, str]:
    value = payload.get(key)
    if not isinstance(value, Mapping):
        return {}
    return {
        stage: stage_status
        for stage, stage_status in value.items()
        if isinstance(stage, str) and isinstance(stage_status, str)
    }


def _assert_required_payload_keys(payload: dict[str, object]) -> None:
    missing = [key for key in _REQUIRED_PAYLOAD_KEYS if key not in payload]
    if missing:
        msg = f"Rust enrichment payload missing required keys: {missing}"
        raise ValueError(msg)


def _collect_payload_with_timings(request: RustPayloadBuildRequestV1) -> dict[str, object]:
    payload_build_started = time.perf_counter()
    payload = _build_enrichment_payload(
        request.node,
        request.source_bytes,
        max_scope_depth=request.max_scope_depth,
    )
    payload_build_ms = max(0.0, (time.perf_counter() - payload_build_started) * 1000.0)
    query_pack_started = time.perf_counter()
    payload.update(
        collect_query_pack_payload(
            root=request.tree_root,
            source_bytes=request.source_bytes,
            byte_span=request.byte_span,
            changed_ranges=request.changed_ranges,
            query_budget_ms=request.query_budget_ms,
            file_key=request.file_key,
        )
    )
    query_pack_ms = max(0.0, (time.perf_counter() - query_pack_started) * 1000.0)
    payload["stage_timings_ms"] = {
        "query_pack": query_pack_ms,
        "payload_build": payload_build_ms,
    }
    payload["stage_status"] = {
        "query_pack": "applied",
        "payload_build": "applied",
    }
    _assert_required_payload_keys(payload)
    return payload


def collect_payload_with_timings(request: object) -> dict[str, object]:
    """Collect enrichment payload with stage timing metadata.

    Returns:
        Enrichment payload annotated with stage timings and status.
    """
    return _collect_payload_with_timings(cast("RustPayloadBuildRequestV1", request))


def _finalize_enrichment_payload(
    *,
    payload: dict[str, object],
    total_started: float,
) -> dict[str, object]:
    attachment_started = time.perf_counter()
    canonical = _canonicalize_payload(payload)
    attachment_ms = max(0.0, (time.perf_counter() - attachment_started) * 1000.0)
    timings = _coerce_timings(canonical, key="stage_timings_ms")
    timings["attachment"] = attachment_ms
    timings["total"] = max(0.0, (time.perf_counter() - total_started) * 1000.0)
    canonical["stage_timings_ms"] = timings
    stage_status = _coerce_status(canonical, key="stage_status")
    stage_status.setdefault("ast_grep", "skipped")
    stage_status.setdefault("query_pack", "applied")
    stage_status.setdefault("payload_build", "applied")
    stage_status["attachment"] = "applied"
    canonical["stage_status"] = stage_status
    return canonical


def _run_rust_enrichment_pipeline(request: RustPipelineRequestV1) -> dict[str, object] | None:
    total_started = time.perf_counter()
    tree_sitter_started = time.perf_counter()
    try:
        _ = request.cache_backend
        tree, source_bytes, changed_ranges = _parse_with_session(
            request.source,
            cache_key=request.cache_key,
            parse_session=request.parse_session,
        )
        if tree is None:
            return None
        node = request.resolve_node(tree.root_node)
        if node is None:
            return None
        payload = _collect_payload_with_timings(
            RustPayloadBuildRequestV1(
                node=node,
                tree_root=tree.root_node,
                source_bytes=source_bytes,
                changed_ranges=changed_ranges,
                byte_span=request.byte_span_for_node(node),
                max_scope_depth=request.max_scope_depth,
                query_budget_ms=request.query_budget_ms,
                file_key=request.cache_key,
            )
        )
    except ENRICHMENT_ERRORS as exc:
        logger.warning("%s failed: %s", request.error_prefix, type(exc).__name__)
        return None
    timings = _coerce_timings(payload, key="stage_timings_ms")
    timings.setdefault("ast_grep", 0.0)
    timings["tree_sitter"] = max(0.0, (time.perf_counter() - tree_sitter_started) * 1000.0)
    payload["stage_timings_ms"] = timings
    status = _coerce_status(payload, key="stage_status")
    status.setdefault("ast_grep", "skipped")
    status["tree_sitter"] = "applied"
    payload["stage_status"] = status
    return _finalize_enrichment_payload(payload=payload, total_started=total_started)


def run_rust_enrichment_pipeline(request: object) -> dict[str, object] | None:
    """Run full Rust enrichment pipeline for a source request.

    Returns:
        Canonical payload on success, or ``None`` when enrichment is unavailable.
    """
    return _run_rust_enrichment_pipeline(cast("RustPipelineRequestV1", request))


__all__ = [
    "RustPayloadBuildRequestV1",
    "RustPipelineRequestV1",
    "collect_payload_with_timings",
    "run_rust_enrichment_pipeline",
]
