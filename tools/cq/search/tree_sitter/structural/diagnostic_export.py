"""Typed diagnostic export helpers for tree-sitter artifact payloads."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from tools.cq.search.tree_sitter.contracts.core_models import (
    QueryWindowV1,
    TreeSitterDiagnosticV1,
)
from tools.cq.search.tree_sitter.diagnostics.collector import collect_tree_sitter_diagnostics

if TYPE_CHECKING:
    from tree_sitter import Node


def export_diagnostic_rows(
    rows: Iterable[object],
) -> tuple[TreeSitterDiagnosticV1, ...]:
    """Normalize diagnostic iterable into a typed tuple payload."""
    return tuple(row for row in rows if isinstance(row, TreeSitterDiagnosticV1))


def collect_diagnostic_rows(
    *,
    language: str,
    root: Node,
    windows: tuple[QueryWindowV1, ...],
    match_limit: int,
) -> tuple[TreeSitterDiagnosticV1, ...]:
    """Collect diagnostics and return typed tuple rows."""
    rows = collect_tree_sitter_diagnostics(
        language=language,
        root=root,
        windows=windows,
        match_limit=match_limit,
    )
    return export_diagnostic_rows(rows)


__all__ = ["collect_diagnostic_rows", "export_diagnostic_rows"]
