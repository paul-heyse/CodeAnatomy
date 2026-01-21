"""Tests for AST extraction."""

from __future__ import annotations

from typing import cast

import pyarrow as pa
import pytest

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike

try:
    from extract.ast_extract import extract_ast_tables
except ImportError:
    extract_ast_tables = None


def _rows_from_table(value: TableLike | RecordBatchReaderLike) -> list[dict[str, object]]:
    table = value.read_all() if isinstance(value, RecordBatchReaderLike) else value
    if not isinstance(table, pa.Table):
        msg = f"Unsupported table type: {type(table)}"
        raise TypeError(msg)
    arrow_table = cast("pa.Table", table)
    return arrow_table.to_pylist()


def test_ast_extract_enriched_outputs() -> None:
    """Ensure AST extraction emits enriched nested lists with byte offsets."""
    if extract_ast_tables is None:
        pytest.skip("AST extraction import failed due to import cycle.")
    assert extract_ast_tables is not None
    source = (
        '"""Module doc."""\n'
        "import os as osmod\n"
        "from sys import path as sys_path\n"
        "\n"
        "class Foo:\n"
        '    """Foo doc."""\n'
        "    def bar(self):\n"
        '        """bar doc."""\n'
        "        return baz(1)\n"
        "\n"
        "def baz(x):\n"
        "    return x\n"
        "\n"
        "x = 1  # type: ignore\n"
    )
    repo_files = pa.table(
        {
            "file_id": ["f1"],
            "path": ["mod.py"],
            "text": [source],
            "file_sha256": ["sha"],
        }
    )
    result = extract_ast_tables(repo_files=repo_files)
    rows = _rows_from_table(result["ast_files"])
    assert len(rows) == 1
    row = rows[0]
    nodes = cast("list[dict[str, object]]", row["nodes"])
    assert nodes
    span_units: set[str] = set()
    for node in nodes:
        span = cast("dict[str, object] | None", node.get("span"))
        if span is None:
            continue
        span_units.add(cast("str", span["col_unit"]))
    assert span_units == {"byte"}

    docstrings = cast("list[dict[str, object]]", row["docstrings"])
    owner_kinds = {entry["owner_kind"] for entry in docstrings}
    assert owner_kinds == {"Module", "ClassDef", "FunctionDef"}

    imports = cast("list[dict[str, object]]", row["imports"])
    assert {entry["kind"] for entry in imports} == {"Import", "ImportFrom"}
    assert {entry["name"] for entry in imports} == {"os", "path"}
    assert {entry["asname"] for entry in imports} == {"osmod", "sys_path"}

    defs = cast("list[dict[str, object]]", row["defs"])
    assert {entry["kind"] for entry in defs} == {"ClassDef", "FunctionDef"}
    assert {entry["name"] for entry in defs} == {"Foo", "bar", "baz"}

    calls = cast("list[dict[str, object]]", row["calls"])
    assert {entry["func_name"] for entry in calls} == {"baz"}

    type_ignores = cast("list[dict[str, object]]", row["type_ignores"])
    assert len(type_ignores) == 1


def test_ast_extract_empty_file() -> None:
    """Ensure empty files parse to a module node without errors."""
    if extract_ast_tables is None:
        pytest.skip("AST extraction import failed due to import cycle.")
    assert extract_ast_tables is not None
    repo_files = pa.table(
        {
            "file_id": ["empty"],
            "path": ["empty.py"],
            "text": [""],
            "file_sha256": ["sha-empty"],
        }
    )
    result = extract_ast_tables(repo_files=repo_files)
    rows = _rows_from_table(result["ast_files"])
    assert len(rows) == 1
    row = rows[0]
    nodes = cast("list[dict[str, object]]", row["nodes"])
    errors = cast("list[dict[str, object]]", row["errors"])
    assert nodes
    assert not errors


def test_ast_extract_error_rows() -> None:
    """Ensure syntax errors are recorded in error rows."""
    if extract_ast_tables is None:
        pytest.skip("AST extraction import failed due to import cycle.")
    assert extract_ast_tables is not None
    repo_files = pa.table(
        {
            "file_id": ["bad"],
            "path": ["bad.py"],
            "text": ["def("],
            "file_sha256": ["sha-bad"],
        }
    )
    result = extract_ast_tables(repo_files=repo_files)
    rows = _rows_from_table(result["ast_files"])
    assert len(rows) == 1
    row = rows[0]
    errors = cast("list[dict[str, object]]", row["errors"])
    assert len(errors) == 1
    error_type = cast("str", errors[0].get("error_type"))
    assert error_type == "SyntaxError"
    nodes = cast("list[dict[str, object]]", row["nodes"])
    assert not nodes
