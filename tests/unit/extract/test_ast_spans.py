"""Unit tests for AST byte spans."""

from __future__ import annotations

from collections.abc import Iterable

from extract.coordination.context import FileContext
from extract.extractors.ast import AstExtractOptions
from extract.extractors.ast.builders_runtime import _extract_ast_for_context


def _as_mapping(value: object) -> dict[str, object]:
    assert isinstance(value, dict)
    return value


def _as_list_of_mappings(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Iterable) or isinstance(value, (str, bytes)):
        return []
    return [item for item in value if isinstance(item, dict)]


def _find_node(
    rows: list[dict[str, object]],
    kind: str,
    name: str | None = None,
) -> dict[str, object] | None:
    for row in rows:
        if row.get("kind") != kind:
            continue
        if name is not None and row.get("name") != name:
            continue
        return row
    return None


def test_ast_byte_span_for_function_def() -> None:
    """Test ast byte span for function def."""
    code = "def foo(x):\n    return x\n"
    file_ctx = FileContext(
        file_id="file-1",
        path="foo.py",
        abs_path=None,
        file_sha256=None,
        encoding="utf-8",
        text=code,
        data=code.encode("utf-8"),
    )
    options = AstExtractOptions(cache_by_sha=False)

    row = _extract_ast_for_context(file_ctx, options=options, cache=None, cache_ttl=None)

    assert row is not None
    row_map = _as_mapping(row)
    nodes = _as_list_of_mappings(row_map.get("nodes"))
    node = _find_node(nodes, "FunctionDef", "foo")
    assert node is not None

    span = _as_mapping(node["span"])
    assert span["col_unit"] == "byte"
    byte_span = _as_mapping(span["byte_span"])
    assert byte_span is not None
    assert byte_span["byte_start"] == 0
    assert byte_span["byte_len"] == len(b"def foo(x):\n    return x")
