"""Unit tests for LSP position-encoding conversions."""

from __future__ import annotations

from tools.cq.search.lsp.position_encoding import from_lsp_character, to_lsp_character


def test_utf16_roundtrip_for_multibyte_codepoint() -> None:
    line_text = "𝄞value"
    lsp_col = to_lsp_character(line_text, 1, "utf-16")
    assert lsp_col == 2
    cq_col = from_lsp_character(line_text, lsp_col, "utf-16")
    assert cq_col == 1


def test_utf8_roundtrip_for_ascii() -> None:
    line_text = "example"
    lsp_col = to_lsp_character(line_text, 3, "utf-8")
    assert lsp_col == 3
    cq_col = from_lsp_character(line_text, lsp_col, "utf-8")
    assert cq_col == 3
