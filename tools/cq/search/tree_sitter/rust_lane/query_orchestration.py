"""Rust query-pack orchestration helpers.

This module provides a stable orchestration boundary so runtime entry points can
call query-pack planning/execution logic without depending on monolithic internals.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tree_sitter import Node

    from tools.cq.search.tree_sitter.contracts.core_models import (
        QueryExecutionSettingsV1,
        QueryWindowV1,
    )


def orchestrate_query_packs(
    *,
    root: Node,
    source_bytes: bytes,
    windows: tuple[QueryWindowV1, ...],
    settings: QueryExecutionSettingsV1,
) -> tuple[
    dict[str, list[Node]],
    tuple[object, ...],
    tuple[object, ...],
    dict[str, object],
    tuple[object, ...],
    tuple[object, ...],
]:
    """Execute all Rust query packs and return raw orchestration outputs.

    Returns:
        Raw query-pack captures, diagnostics, telemetry, and contract tuples.
    """
    from tools.cq.search.tree_sitter.rust_lane import runtime_core as _runtime_core

    collect_query_pack_captures = _runtime_core.__dict__["_collect_query_pack_captures"]
    return collect_query_pack_captures(
        root=root,
        source_bytes=source_bytes,
        windows=windows,
        settings=settings,
    )


__all__ = ["orchestrate_query_packs"]
