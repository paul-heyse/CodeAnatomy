"""Unit tests for tree-sitter query coverage."""

from __future__ import annotations

from collections.abc import Iterable

from extract.coordination.context import FileContext
from extract.extractors.tree_sitter.extract import (
    PY_LANGUAGE,
    TreeSitterExtractOptions,
    _extract_ts_file_row,
    _parser,
)
from extract.extractors.tree_sitter.queries import compile_query_pack


def test_tree_sitter_async_def_query() -> None:
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
    assert any(def_row.get("kind") == "async_function_definition" for def_row in def_rows)
