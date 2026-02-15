"""Tests for merged structural exports module (diagnostics, tokens, query hits)."""

from __future__ import annotations

from dataclasses import dataclass

from tools.cq.search.tree_sitter.contracts.core_models import (
    TreeSitterDiagnosticV1,
)
from tools.cq.search.tree_sitter.structural.exports import (
    export_cst_tokens,
    export_diagnostic_rows,
    export_query_hits,
)

# -- Diagnostic Export --------------------------------------------------------


def test_export_diagnostic_rows_filters_to_typed_rows() -> None:
    """Verify export_diagnostic_rows keeps only TreeSitterDiagnosticV1 instances."""
    typed = TreeSitterDiagnosticV1(
        kind="ERROR",
        start_byte=0,
        end_byte=1,
        start_line=1,
        start_col=0,
        end_line=1,
        end_col=1,
        message="error",
    )
    rows = export_diagnostic_rows([typed, object()])
    assert rows == (typed,)


# -- Query Hit Export ---------------------------------------------------------


@dataclass(frozen=True)
class _FakeNode:
    """Minimal node stub for query hit export."""

    type: str
    start_byte: int
    end_byte: int


def test_export_query_hits_emits_typed_rows() -> None:
    """Verify export_query_hits produces TreeSitterQueryHitV1 rows."""
    matches = [
        (1, {"name": [_FakeNode(type="identifier", start_byte=4, end_byte=7)]}),
    ]
    rows = export_query_hits(file_path="sample.rs", matches=matches)
    assert len(rows) == 1
    assert rows[0].query_name == "sample.rs"
    assert rows[0].pattern_index == 1
    assert rows[0].capture_name == "name"


# -- Token Export (smoke) -----------------------------------------------------


def test_export_cst_tokens_is_callable() -> None:
    """Verify export_cst_tokens is available from merged module."""
    assert callable(export_cst_tokens)
