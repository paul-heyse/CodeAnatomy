"""Tests for typed structural diagnostic export helpers."""

from __future__ import annotations

from tools.cq.search.tree_sitter.contracts.core_models import TreeSitterDiagnosticV1
from tools.cq.search.tree_sitter.structural.exports import export_diagnostic_rows


def test_export_diagnostic_rows_filters_to_typed_rows() -> None:
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
