"""Rust lane query-execution surface.

Provides canonical query-pack capture entry points separated from runtime orchestration.
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
    """Collect Rust query-pack captures and telemetry artifacts.

    Returns:
        Query matches, object-evidence rows, query-hit rows, telemetry payload,
        injection plans, and rust tag events.
    """
    from tools.cq.search.tree_sitter.rust_lane.runtime_engine import (
        collect_query_pack_captures as collect_query_pack_captures_impl,
    )

    return collect_query_pack_captures_impl(
        root=root,
        source_bytes=source_bytes,
        windows=windows,
        settings=settings,
    )


__all__ = ["collect_query_pack_captures"]
