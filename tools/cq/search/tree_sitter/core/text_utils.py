"""Shared text helpers for tree-sitter nodes."""

from __future__ import annotations


def node_text(
    node: object,
    source_bytes: bytes,
    *,
    strip: bool = True,
    max_len: int | None = None,
) -> str:
    """Extract UTF-8 text for a tree-sitter node byte span."""
    start = int(getattr(node, "start_byte", 0))
    end = int(getattr(node, "end_byte", start))
    if end <= start:
        return ""
    text = source_bytes[start:end].decode("utf-8", errors="replace")
    out = text.strip() if strip else text
    if max_len is not None and len(out) >= max_len:
        return out[: max(1, max_len - 3)] + "..."
    return out


__all__ = ["node_text"]
