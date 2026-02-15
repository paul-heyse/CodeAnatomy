"""Build metadata-driven evidence rows from tree-sitter `matches()` output."""

from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING

from tools.cq.search.tree_sitter_match_row_contracts import ObjectEvidenceRowV1
from tools.cq.search.tree_sitter_pack_metadata import first_capture, pattern_settings

if TYPE_CHECKING:
    from tree_sitter import Node, Query


def _node_text(node: Node, source_bytes: bytes) -> str:
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    if end <= start:
        return ""
    return source_bytes[start:end].decode("utf-8", errors="replace").strip()


def _capture_payload(capture_map: dict[str, list[Node]], source_bytes: bytes) -> dict[str, str]:
    out: OrderedDict[str, str] = OrderedDict()
    for capture_name, nodes in capture_map.items():
        if not isinstance(capture_name, str):
            continue
        if capture_name.startswith(("payload.", "debug.")):
            continue
        if not isinstance(nodes, list) or not nodes:
            continue
        text = _node_text(nodes[0], source_bytes)
        if not text:
            continue
        out[capture_name] = text
    return dict(out)


def build_match_rows(
    *,
    query: Query,
    matches: list[tuple[int, dict[str, list[Node]]]],
    source_bytes: bytes,
) -> tuple[ObjectEvidenceRowV1, ...]:
    """Build object evidence rows with metadata-driven anchors.

    Returns:
        tuple[ObjectEvidenceRowV1, ...]: Ordered evidence rows for all matched rows.
    """
    rows: list[ObjectEvidenceRowV1] = []
    for pattern_idx, capture_map in matches:
        settings = pattern_settings(query, pattern_idx)
        emit = settings.get("cq.emit", "unknown")
        kind = settings.get("cq.kind", "unknown")
        anchor_capture = settings.get("cq.anchor")
        anchor = (
            first_capture(capture_map, anchor_capture)
            if isinstance(anchor_capture, str) and anchor_capture
            else None
        )
        if anchor is None:
            for nodes in capture_map.values():
                if isinstance(nodes, list) and nodes:
                    anchor = nodes[0]
                    break
        if anchor is None:
            continue
        rows.append(
            ObjectEvidenceRowV1(
                emit=emit,
                kind=kind,
                anchor_start_byte=int(getattr(anchor, "start_byte", 0)),
                anchor_end_byte=int(getattr(anchor, "end_byte", 0)),
                pattern_index=pattern_idx,
                captures=_capture_payload(capture_map, source_bytes),
            )
        )
    return tuple(rows)


__all__ = ["build_match_rows"]
