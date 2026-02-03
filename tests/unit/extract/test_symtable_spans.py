"""Unit tests for symtable span hints."""

from __future__ import annotations

from extract.coordination.context import FileContext
from extract.extractors.symtable_extract import SymtableExtractOptions, _symtable_file_row


def test_symtable_span_hint_byte_start() -> None:
    code = "x = 1\n"
    file_ctx = FileContext(
        file_id="file-1",
        path="foo.py",
        abs_path=None,
        file_sha256=None,
        encoding="utf-8",
        text=code,
        data=code.encode("utf-8"),
    )
    options = SymtableExtractOptions()

    row = _symtable_file_row(file_ctx, options=options, cache=None, cache_ttl=None)

    assert row is not None
    blocks = row["blocks"]
    assert isinstance(blocks, list)

    module_block = next(
        (
            block
            for block in blocks
            if isinstance(block, dict) and block.get("block_type") == "MODULE"
        ),
        None,
    )
    assert module_block is not None

    span_hint = module_block.get("span_hint")
    assert isinstance(span_hint, dict)
    assert span_hint["col_unit"] == "byte"
    byte_span = span_hint.get("byte_span")
    assert isinstance(byte_span, dict)
    assert byte_span.get("byte_start") == 0
