"""Rust lane query-payload assembly surface."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tree_sitter import Node


def collect_query_pack_payload(
    *,
    root: Node,
    source_bytes: bytes,
    byte_span: tuple[int, int],
    changed_ranges: tuple[object, ...] = (),
    query_budget_ms: int | None = None,
    file_key: str | None = None,
) -> dict[str, object]:
    """Collect fully assembled Rust query-pack payload for a byte span.

    Returns:
        Assembled query-pack payload for the requested Rust byte span.
    """
    from tools.cq.search.tree_sitter.rust_lane.runtime_engine import (
        collect_query_pack_payload as collect_query_pack_payload_impl,
    )

    return collect_query_pack_payload_impl(
        root=root,
        source_bytes=source_bytes,
        byte_span=byte_span,
        changed_ranges=changed_ranges,
        query_budget_ms=query_budget_ms,
        file_key=file_key,
    )


__all__ = ["collect_query_pack_payload"]
