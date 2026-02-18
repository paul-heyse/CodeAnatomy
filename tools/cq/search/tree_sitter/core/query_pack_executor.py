"""Shared query-pack execution helpers for tree-sitter lanes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import msgspec

from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryExecutionSettingsV1,
    QueryExecutionTelemetryV1,
    QueryPointWindowV1,
    QueryWindowV1,
    TreeSitterQueryHitV1,
)
from tools.cq.search.tree_sitter.structural.match_rows import build_match_rows_with_query_hits

if TYPE_CHECKING:
    from tree_sitter import Node, Query

    from tools.cq.search.tree_sitter.contracts.core_models import ObjectEvidenceRowV1
    from tools.cq.search.tree_sitter.core.runtime_engine import QueryExecutionCallbacksV1


@dataclass(frozen=True, slots=True)
class QueryPackExecutionContextV1:
    """Shared runtime options for executing a query pack."""

    windows: tuple[QueryWindowV1, ...]
    settings: QueryExecutionSettingsV1
    callbacks: QueryExecutionCallbacksV1 | None = None


def run_bounded_query_captures(
    query: Query,
    root: Node,
    *,
    windows: tuple[QueryWindowV1, ...] | None = None,
    point_windows: tuple[QueryPointWindowV1, ...] | None = None,
    settings: QueryExecutionSettingsV1 | None = None,
    callbacks: QueryExecutionCallbacksV1 | None = None,
) -> tuple[dict[str, list[Node]], QueryExecutionTelemetryV1]:
    from tools.cq.search.tree_sitter.core.runtime_engine import run_bounded_query_captures as _impl

    return _impl(
        query=query,
        root=root,
        windows=windows,
        point_windows=point_windows,
        settings=settings,
        callbacks=callbacks,
    )


def run_bounded_query_matches(
    query: Query,
    root: Node,
    *,
    windows: tuple[QueryWindowV1, ...] | None = None,
    point_windows: tuple[QueryPointWindowV1, ...] | None = None,
    settings: QueryExecutionSettingsV1 | None = None,
    callbacks: QueryExecutionCallbacksV1 | None = None,
) -> tuple[list[tuple[int, dict[str, list[Node]]]], QueryExecutionTelemetryV1]:
    from tools.cq.search.tree_sitter.core.runtime_engine import run_bounded_query_matches as _impl

    return _impl(
        query=query,
        root=root,
        windows=windows,
        point_windows=point_windows,
        settings=settings,
        callbacks=callbacks,
    )


def _match_settings(settings: QueryExecutionSettingsV1) -> QueryExecutionSettingsV1:
    return msgspec.structs.replace(
        settings,
        require_containment=True,
        window_mode="containment_preferred",
    )


def execute_pack_rows(
    *,
    query: Query,
    query_name: str,
    root: Node,
    source_bytes: bytes,
    context: QueryPackExecutionContextV1,
) -> tuple[
    dict[str, list[Node]],
    tuple[ObjectEvidenceRowV1, ...],
    tuple[TreeSitterQueryHitV1, ...],
    QueryExecutionTelemetryV1,
    QueryExecutionTelemetryV1,
]:
    """Execute captures + matches and project row/hit payloads for one query pack.

    Returns:
        tuple[dict[str, list[Node]], tuple[ObjectEvidenceRowV1, ...], tuple[TreeSitterQueryHitV1, ...], QueryExecutionTelemetryV1, QueryExecutionTelemetryV1]: Function return value.
    """
    captures, matches, rows, hits, capture_telemetry, match_telemetry = (
        execute_pack_rows_with_matches(
            query=query,
            query_name=query_name,
            root=root,
            source_bytes=source_bytes,
            context=context,
        )
    )
    _ = matches
    return captures, rows, hits, capture_telemetry, match_telemetry


def execute_pack_rows_with_matches(
    *,
    query: Query,
    query_name: str,
    root: Node,
    source_bytes: bytes,
    context: QueryPackExecutionContextV1,
) -> tuple[
    dict[str, list[Node]],
    list[tuple[int, dict[str, list[Node]]]],
    tuple[ObjectEvidenceRowV1, ...],
    tuple[TreeSitterQueryHitV1, ...],
    QueryExecutionTelemetryV1,
    QueryExecutionTelemetryV1,
]:
    """Execute captures + matches and include raw match tuples.

    Returns:
        tuple[dict[str, list[Node]], list[tuple[int, dict[str, list[Node]]]], tuple[ObjectEvidenceRowV1, ...], tuple[TreeSitterQueryHitV1, ...], QueryExecutionTelemetryV1, QueryExecutionTelemetryV1]: Function return value.
    """
    captures, capture_telemetry = run_bounded_query_captures(
        query,
        root,
        windows=context.windows,
        settings=context.settings,
        callbacks=context.callbacks,
    )
    matches, match_telemetry = run_bounded_query_matches(
        query,
        root,
        windows=context.windows,
        settings=_match_settings(context.settings),
        callbacks=context.callbacks,
    )
    rows, hits = build_match_rows_with_query_hits(
        query=query,
        matches=matches,
        source_bytes=source_bytes,
        query_name=query_name,
    )
    return captures, matches, rows, hits, capture_telemetry, match_telemetry


__all__ = ["QueryPackExecutionContextV1", "execute_pack_rows", "execute_pack_rows_with_matches"]
