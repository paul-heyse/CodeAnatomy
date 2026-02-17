"""Metavariable extraction helpers for ast-grep matches."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol, TypeGuard


class _PointProtocol(Protocol):
    line: int
    column: int


class _RangeProtocol(Protocol):
    start: _PointProtocol
    end: _PointProtocol


class CapturedNodeProtocol(Protocol):
    """Structural node protocol used for metavariable payload extraction."""

    def text(self) -> str:
        """Return source text for this captured node."""
        ...

    def kind(self) -> str:
        """Return syntax kind token for this captured node."""
        ...

    def range(self) -> _RangeProtocol:
        """Return ast-grep-style range object consumed by payload helpers."""
        ...


class MatchCaptureProtocol(Protocol):
    """Structural protocol for ast-grep match capture surfaces."""

    def get_match(self, meta_var: str, /) -> object | None:
        """Return a single capture for the provided metavariable name."""
        ...

    def get_multiple_matches(self, meta_var: str, /) -> Sequence[object]:
        """Return variadic captures for the provided metavariable name."""
        ...


def _is_captured_node(value: object) -> TypeGuard[CapturedNodeProtocol]:
    """Return whether value satisfies the captured-node protocol."""
    return all(hasattr(value, attr) for attr in ("text", "kind", "range"))


def _is_variadic_separator(node: CapturedNodeProtocol) -> bool:
    text = node.text().strip()
    return node.kind() in {",", ";"} or text in {",", ";"}


def _node_payload(node: CapturedNodeProtocol) -> dict[str, object]:
    range_obj = node.range()
    return {
        "text": node.text(),
        "start": {"line": range_obj.start.line, "column": range_obj.start.column},
        "end": {"line": range_obj.end.line, "column": range_obj.end.column},
    }


def extract_match_metavars(
    match: MatchCaptureProtocol,
    *,
    metavar_names: tuple[str, ...],
    variadic_names: frozenset[str],
    include_multi: bool = False,
) -> dict[str, object]:
    """Extract metavariable captures from an ast-grep-py match.

    Returns:
    -------
    dict[str, object]
        Mapping of captured metavariable keys to normalized values.
    """
    metavars: dict[str, object] = {}
    for bare_name in metavar_names:
        captured = match.get_match(bare_name)
        if captured is not None and _is_captured_node(captured):
            text = captured.text()
            # Keep both bare and `$`-prefixed keys for output compatibility.
            metavars[bare_name] = text
            metavars[f"${bare_name}"] = text

        if include_multi and bare_name in variadic_names:
            captured_multi = match.get_multiple_matches(bare_name)
            captured_nodes = [
                node
                for node in captured_multi
                if _is_captured_node(node) and not _is_variadic_separator(node)
            ]
            if captured_nodes:
                text = ", ".join(node.text() for node in captured_nodes)
                metavars[f"$$${bare_name}"] = {
                    "kind": "multi",
                    "text": text,
                    "nodes": [_node_payload(node) for node in captured_nodes],
                }
    return metavars


__all__ = ["extract_match_metavars"]
