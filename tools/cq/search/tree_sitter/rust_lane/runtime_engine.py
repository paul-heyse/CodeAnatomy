"""Optional Rust context enrichment using ``tree-sitter-rust``.

This module is now a thin boundary layer. Query-pack execution, payload
assembly, and pipeline-stage ownership live in dedicated runtime modules.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import cast

from tools.cq.search.tree_sitter.rust_lane.availability import (
    is_tree_sitter_rust_available as _is_tree_sitter_rust_available,
)
from tools.cq.search.tree_sitter.rust_lane.runtime_cache import (
    clear_tree_sitter_rust_cache,
    get_tree_sitter_rust_cache_stats,
)
from tools.cq.search.tree_sitter.rust_lane.runtime_payload_builders import (
    collect_query_pack_payload as _collect_query_pack_payload_impl,
)
from tools.cq.search.tree_sitter.rust_lane.runtime_pipeline_stages import (
    RustPayloadBuildRequestV1,
    RustPipelineRequestV1,
)
from tools.cq.search.tree_sitter.rust_lane.runtime_pipeline_stages import (
    collect_payload_with_timings as _collect_payload_with_timings_impl,
)
from tools.cq.search.tree_sitter.rust_lane.runtime_pipeline_stages import (
    run_rust_enrichment_pipeline as _run_rust_enrichment_pipeline_impl,
)
from tools.cq.search.tree_sitter.rust_lane.runtime_query_execution import (
    collect_query_pack_captures as _collect_query_pack_captures_impl,
)

try:
    from tree_sitter import Point as _TreeSitterPoint
except ImportError:  # pragma: no cover - availability guard
    _TreeSitterPoint = None

MAX_SOURCE_BYTES = 5 * 1024 * 1024  # 5 MB
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class RustLaneEnrichmentSettingsV1:
    """Execution settings for Rust tree-sitter enrichment."""

    max_scope_depth: int = 24
    query_budget_ms: int | None = None


@dataclass(frozen=True, slots=True)
class RustLaneRuntimeDepsV1:
    """Runtime dependency overrides for Rust tree-sitter enrichment."""

    parse_session: object | None = None
    cache_backend: object | None = None


# Preserve private request type names for internal callsites during migration.
_RustPayloadBuildRequestV1 = RustPayloadBuildRequestV1
_RustPipelineRequestV1 = RustPipelineRequestV1


def is_tree_sitter_rust_available() -> bool:
    """Return whether tree-sitter Rust enrichment dependencies are available."""
    return _is_tree_sitter_rust_available()


def _collect_query_pack_captures(
    *,
    root: object,
    source_bytes: bytes,
    windows: tuple[object, ...],
    settings: object,
) -> tuple[dict[str, list[object]], tuple[object, ...], tuple[object, ...], dict[str, object], tuple[object, ...], tuple[object, ...]]:
    return cast(
        "tuple[dict[str, list[object]], tuple[object, ...], tuple[object, ...], dict[str, object], tuple[object, ...], tuple[object, ...]]",
        _collect_query_pack_captures_impl(
            root=cast("object", root),
            source_bytes=source_bytes,
            windows=cast("tuple[object, ...]", windows),
            settings=cast("object", settings),
        ),
    )


def collect_query_pack_captures(
    *,
    root: object,
    source_bytes: bytes,
    windows: tuple[object, ...],
    settings: object,
) -> tuple[dict[str, list[object]], tuple[object, ...], tuple[object, ...], dict[str, object], tuple[object, ...], tuple[object, ...]]:
    """Collect captures, telemetry, and lane artifacts for Rust query packs."""
    return _collect_query_pack_captures(
        root=root,
        source_bytes=source_bytes,
        windows=windows,
        settings=settings,
    )


def _collect_query_pack_payload(
    *,
    root: object,
    source_bytes: bytes,
    byte_span: tuple[int, int],
    changed_ranges: tuple[object, ...] = (),
    query_budget_ms: int | None = None,
    file_key: str | None = None,
) -> dict[str, object]:
    return _collect_query_pack_payload_impl(
        root=cast("object", root),
        source_bytes=source_bytes,
        byte_span=byte_span,
        changed_ranges=changed_ranges,
        query_budget_ms=query_budget_ms,
        file_key=file_key,
    )


def collect_query_pack_payload(
    *,
    root: object,
    source_bytes: bytes,
    byte_span: tuple[int, int],
    changed_ranges: tuple[object, ...] = (),
    query_budget_ms: int | None = None,
    file_key: str | None = None,
) -> dict[str, object]:
    """Collect query-pack payload for the provided byte span."""
    return _collect_query_pack_payload(
        root=root,
        source_bytes=source_bytes,
        byte_span=byte_span,
        changed_ranges=changed_ranges,
        query_budget_ms=query_budget_ms,
        file_key=file_key,
    )


def _collect_payload_with_timings(request: _RustPayloadBuildRequestV1) -> dict[str, object]:
    return _collect_payload_with_timings_impl(request)


def collect_payload_with_timings(request: object) -> dict[str, object]:
    """Collect enrichment payload with stage timing metadata."""
    return _collect_payload_with_timings(cast("_RustPayloadBuildRequestV1", request))


def _run_rust_enrichment_pipeline(request: _RustPipelineRequestV1) -> dict[str, object] | None:
    return _run_rust_enrichment_pipeline_impl(request)


def run_rust_enrichment_pipeline(request: object) -> dict[str, object] | None:
    """Run full Rust enrichment pipeline for a source request."""
    return _run_rust_enrichment_pipeline(cast("_RustPipelineRequestV1", request))


def enrich_rust_context(
    source: str,
    *,
    line: int,
    col: int,
    cache_key: str | None = None,
    settings: RustLaneEnrichmentSettingsV1 | None = None,
    runtime_deps: RustLaneRuntimeDepsV1 | None = None,
) -> dict[str, object] | None:
    """Extract optional Rust context details for a match location."""
    if not is_tree_sitter_rust_available() or line < 1 or col < 0 or len(source) > MAX_SOURCE_BYTES:
        if len(source) > MAX_SOURCE_BYTES:
            logger.warning(
                "Skipping Rust tree-sitter enrichment for oversized source (%d chars)",
                len(source),
            )
        return None

    if _TreeSitterPoint is None:
        return None
    effective_settings = settings or RustLaneEnrichmentSettingsV1()
    effective_runtime = runtime_deps or RustLaneRuntimeDepsV1()
    point = _TreeSitterPoint(max(0, line - 1), max(0, col))
    return _run_rust_enrichment_pipeline(
        _RustPipelineRequestV1(
            source=source,
            cache_key=cache_key,
            max_scope_depth=effective_settings.max_scope_depth,
            query_budget_ms=effective_settings.query_budget_ms,
            resolve_node=lambda root: root.named_descendant_for_point_range(point, point),
            byte_span_for_node=lambda node: (
                int(getattr(node, "start_byte", 0)),
                int(getattr(node, "end_byte", 0)),
            ),
            error_prefix="Rust context enrichment",
            parse_session=effective_runtime.parse_session,
            cache_backend=effective_runtime.cache_backend,
        ),
    )


def enrich_rust_context_by_byte_range(
    source: str,
    *,
    byte_start: int,
    byte_end: int,
    cache_key: str | None = None,
    settings: RustLaneEnrichmentSettingsV1 | None = None,
    runtime_deps: RustLaneRuntimeDepsV1 | None = None,
) -> dict[str, object] | None:
    """Extract optional Rust context using byte offsets instead of line/col."""
    if not is_tree_sitter_rust_available() or byte_start < 0 or byte_end <= byte_start:
        return None

    source_byte_len = len(source.encode("utf-8", errors="replace"))
    if source_byte_len > MAX_SOURCE_BYTES or byte_end > source_byte_len:
        if source_byte_len > MAX_SOURCE_BYTES:
            logger.warning(
                "Skipping Rust byte-range enrichment for oversized source (%d bytes)",
                source_byte_len,
            )
        return None

    effective_settings = settings or RustLaneEnrichmentSettingsV1()
    effective_runtime = runtime_deps or RustLaneRuntimeDepsV1()
    return _run_rust_enrichment_pipeline(
        _RustPipelineRequestV1(
            source=source,
            cache_key=cache_key,
            max_scope_depth=effective_settings.max_scope_depth,
            query_budget_ms=effective_settings.query_budget_ms,
            resolve_node=lambda root: root.named_descendant_for_byte_range(byte_start, byte_end),
            byte_span_for_node=lambda _node: (byte_start, byte_end),
            error_prefix="Rust byte-range enrichment",
            parse_session=effective_runtime.parse_session,
            cache_backend=effective_runtime.cache_backend,
        )
    )


__all__ = [
    "MAX_SOURCE_BYTES",
    "RustLaneEnrichmentSettingsV1",
    "RustLaneRuntimeDepsV1",
    "clear_tree_sitter_rust_cache",
    "collect_payload_with_timings",
    "collect_query_pack_captures",
    "collect_query_pack_payload",
    "enrich_rust_context",
    "enrich_rust_context_by_byte_range",
    "get_tree_sitter_rust_cache_stats",
    "is_tree_sitter_rust_available",
    "run_rust_enrichment_pipeline",
]
