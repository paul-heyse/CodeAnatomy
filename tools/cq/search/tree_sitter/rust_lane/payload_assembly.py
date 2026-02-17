"""Rust query-pack payload assembly helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tree_sitter import Node


def assemble_query_pack_payload(
    *,
    root: Node,
    source_bytes: bytes,
    byte_span: tuple[int, int],
    changed_ranges: tuple[object, ...] = (),
    query_budget_ms: int | None = None,
    file_key: str | None = None,
) -> dict[str, object]:
    """Assemble Rust query-pack payload for a byte span.

    Returns:
        Query-pack payload dictionary with runtime capture metadata.
    """
    from tools.cq.search.tree_sitter.rust_lane import runtime_core as _runtime_core

    collect_query_pack_payload = _runtime_core.__dict__["_collect_query_pack_payload"]
    return collect_query_pack_payload(
        root=root,
        source_bytes=source_bytes,
        byte_span=byte_span,
        changed_ranges=changed_ranges,
        query_budget_ms=query_budget_ms,
        file_key=file_key,
    )


__all__ = ["assemble_query_pack_payload"]
