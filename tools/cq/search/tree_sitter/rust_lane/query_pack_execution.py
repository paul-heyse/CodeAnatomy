"""Rust query-pack execution public API.

This module exposes stable collection entry points used by orchestration layers
without requiring private ``__dict__`` lookups.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tree_sitter import Node

    from tools.cq.search.tree_sitter.contracts.core_models import (
        ObjectEvidenceRowV1,
        QueryExecutionSettingsV1,
        QueryWindowV1,
        TreeSitterQueryHitV1,
    )
    from tools.cq.search.tree_sitter.rust_lane.injections import InjectionPlanV1
    from tools.cq.search.tree_sitter.tags import RustTagEventV1


__all__ = ["collect_query_pack_captures", "collect_query_pack_payload"]


def collect_query_pack_captures(
    *,
    root: Node,
    source_bytes: bytes,
    windows: tuple[QueryWindowV1, ...],
    settings: QueryExecutionSettingsV1,
) -> tuple[
    dict[str, list[Node]],
    tuple[ObjectEvidenceRowV1, ...],
    tuple[TreeSitterQueryHitV1, ...],
    dict[str, object],
    tuple[InjectionPlanV1, ...],
    tuple[RustTagEventV1, ...],
]:
    """Collect captures, diagnostics, telemetry, and lane artifacts.

    Returns:
        tuple[dict[str, list[Node]], tuple[ObjectEvidenceRowV1, ...], tuple[TreeSitterQueryHitV1, ...], dict[str, object], tuple[InjectionPlanV1, ...], tuple[RustTagEventV1, ...]]: Aggregated capture and telemetry artifacts.
    """
    from tools.cq.search.tree_sitter.rust_lane.runtime_engine import (
        _collect_query_pack_captures as collect_query_pack_captures_impl,
    )

    return collect_query_pack_captures_impl(
        root=root,
        source_bytes=source_bytes,
        windows=windows,
        settings=settings,
    )


def collect_query_pack_payload(
    *,
    root: Node,
    source_bytes: bytes,
    byte_span: tuple[int, int],
    changed_ranges: tuple[object, ...] = (),
    query_budget_ms: int | None = None,
    file_key: str | None = None,
) -> dict[str, object]:
    """Collect fully assembled query-pack payload for a byte span.

    Returns:
        dict[str, object]: Aggregated query-pack payload for the requested span.
    """
    from tools.cq.search.tree_sitter.rust_lane.runtime_engine import (
        _collect_query_pack_payload as collect_query_pack_payload_impl,
    )

    return collect_query_pack_payload_impl(
        root=root,
        source_bytes=source_bytes,
        byte_span=byte_span,
        changed_ranges=changed_ranges,
        query_budget_ms=query_budget_ms,
        file_key=file_key,
    )
