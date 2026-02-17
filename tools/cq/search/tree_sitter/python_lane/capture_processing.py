"""Capture processing helpers for Python lane runtime."""

from __future__ import annotations

from collections import defaultdict

from tools.cq.search.tree_sitter.python_lane.fact_contracts import PythonCaptureFactV1

_SCOPE_KINDS = {"function_definition", "class_definition", "module"}
_CAPTURE_TUPLE_PARTS = 2


__all__ = ["group_captures_by_scope", "process_captures"]


def _capture_parts(capture: object) -> tuple[str, object | None]:
    if isinstance(capture, tuple) and len(capture) == _CAPTURE_TUPLE_PARTS:
        name, node = capture
        if isinstance(name, str):
            return name, node
    if isinstance(capture, dict):
        name = capture.get("name")
        node = capture.get("node")
        if isinstance(name, str):
            return name, node
    return "capture", None


def _scope_label(node: object | None) -> str | None:
    current = node
    while current is not None:
        kind = str(getattr(current, "type", ""))
        if kind in _SCOPE_KINDS:
            return kind
        current = getattr(current, "parent", None)
    return None


def process_captures(
    captures: list[object],
    *,
    source_bytes: bytes,
) -> tuple[PythonCaptureFactV1, ...]:
    """Convert raw captures into typed capture facts.

    Returns:
        tuple[PythonCaptureFactV1, ...]: Typed capture facts.
    """
    _ = source_bytes
    rows: list[PythonCaptureFactV1] = []
    for capture in captures:
        name, node = _capture_parts(capture)
        rows.append(
            PythonCaptureFactV1(
                capture_name=name,
                node_kind=str(getattr(node, "type", "unknown")),
                start_byte=int(getattr(node, "start_byte", 0) or 0),
                end_byte=int(getattr(node, "end_byte", 0) or 0),
                scope=_scope_label(node),
            )
        )
    return tuple(rows)


def group_captures_by_scope(
    captures: list[object],
    *,
    source_bytes: bytes,
) -> dict[str, tuple[PythonCaptureFactV1, ...]]:
    """Group processed captures by nearest scope kind.

    Returns:
        dict[str, tuple[PythonCaptureFactV1, ...]]: Capture facts grouped by scope key.
    """
    grouped: dict[str, list[PythonCaptureFactV1]] = defaultdict(list)
    for row in process_captures(captures, source_bytes=source_bytes):
        key = row.scope or "unknown"
        grouped[key].append(row)
    return {key: tuple(rows) for key, rows in grouped.items()}
