"""Metavariable extraction helpers for ast-grep matches."""

from __future__ import annotations

from typing import TYPE_CHECKING

from tools.cq.astgrep.sgpy_scanner import is_variadic_separator, node_payload

if TYPE_CHECKING:
    from ast_grep_py import SgNode


def extract_match_metavars(
    match: SgNode,
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
        if captured is not None:
            text = captured.text()
            # Keep both bare and `$`-prefixed keys for output compatibility.
            metavars[bare_name] = text
            metavars[f"${bare_name}"] = text

        if include_multi and bare_name in variadic_names:
            captured_multi = match.get_multiple_matches(bare_name)
            all_nodes: list[SgNode] = list(captured_multi) if captured_multi is not None else []
            captured_nodes = [node for node in all_nodes if not is_variadic_separator(node)]
            if captured_nodes:
                text = ", ".join(node.text() for node in captured_nodes)
                metavars[f"$$${bare_name}"] = {
                    "kind": "multi",
                    "text": text,
                    "nodes": [node_payload(node) for node in captured_nodes],
                }
    return metavars


__all__ = ["extract_match_metavars"]
