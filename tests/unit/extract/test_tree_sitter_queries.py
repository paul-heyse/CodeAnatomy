"""Unit tests for tree-sitter query coverage."""

from __future__ import annotations

from collections.abc import Iterable

from extract.coordination.context import FileContext
from extract.extractors.tree_sitter import compile_query_pack
from extract.extractors.tree_sitter.builders import (
    PY_LANGUAGE,
    TreeSitterExtractOptions,
    _parser,
)
from extract.extractors.tree_sitter.builders_runtime import _extract_ts_file_row


def test_tree_sitter_async_def_query() -> None:
    """Test tree sitter async def query."""
    code = "async def foo():\n    return 1\n"
    file_ctx = FileContext(
        file_id="file-1",
        path="foo.py",
        abs_path=None,
        file_sha256=None,
        encoding="utf-8",
        text=code,
        data=code.encode("utf-8"),
    )
    options = TreeSitterExtractOptions(
        include_nodes=False,
        include_edges=False,
        include_errors=False,
        include_missing=False,
        include_captures=False,
        include_calls=False,
        include_imports=False,
        include_docstrings=False,
        include_stats=False,
        include_defs=True,
    )
    parser = _parser(options)
    query_pack = compile_query_pack(PY_LANGUAGE)

    row = _extract_ts_file_row(
        file_ctx,
        parser=parser,
        cache=None,
        options=options,
        query_pack=query_pack,
    )

    assert row is not None
    assert isinstance(row, dict)
    defs = row.get("defs")
    if not isinstance(defs, Iterable) or isinstance(defs, (str, bytes)):
        def_rows: list[dict[str, object]] = []
    else:
        def_rows = [item for item in defs if isinstance(item, dict)]
    matching = [row for row in def_rows if row.get("name") == "foo"]
    assert matching
    def_row = matching[0]
    assert def_row.get("kind") == "function_definition"
    span = def_row.get("span")
    assert isinstance(span, dict)
    byte_span = span.get("byte_span")
    assert isinstance(byte_span, dict)
    byte_start = byte_span.get("byte_start")
    assert isinstance(byte_start, int)
    assert code.encode("utf-8")[byte_start:].startswith(b"async def")
