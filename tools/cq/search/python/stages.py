"""Reusable Python-lane stage helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ast_grep_py import SgNode


def extract_signature_stage(node: SgNode) -> dict[str, object]:
    """Extract a bounded signature preview from an ast-grep node."""
    signature = node.text().split("{", maxsplit=1)[0].strip()
    if not signature:
        return {}
    return {"signature": signature[:200]}


__all__ = ["extract_signature_stage"]
