"""Shared structural node-id helpers."""

from __future__ import annotations


def build_node_id(*, file_path: str, node: object | None) -> str:
    """Build a stable structural node identifier from file and byte span.

    Returns:
        str: Function return value.
    """
    if node is None:
        return f"{file_path}:::null"
    node_type = str(getattr(node, "type", "unknown"))
    return (
        f"{file_path}:{int(getattr(node, 'start_byte', 0))}:"
        f"{int(getattr(node, 'end_byte', 0))}:{node_type}"
    )


__all__ = ["build_node_id"]
