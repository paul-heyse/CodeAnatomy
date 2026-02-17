"""Tests for import-analysis visitor behavior."""

from __future__ import annotations

import ast

from tools.cq.analysis.visitors.import_visitor import ImportVisitor

EXPECTED_IMPORT_COUNT = 2


def _import_info(**kwargs: object) -> dict[str, object]:
    return dict(kwargs)


def _resolve_relative(_file: str, _level: int, module: str | None) -> str | None:
    return module


def test_import_visitor_collects_imports() -> None:
    """Verify visitor records import and from-import statements."""
    tree = ast.parse("import os\nfrom x import y\n")
    visitor = ImportVisitor(
        "a.py",
        make_import_info=_import_info,
        resolve_relative_import=_resolve_relative,
    )
    visitor.visit(tree)
    assert len(visitor.imports) == EXPECTED_IMPORT_COUNT
    assert visitor.imports[0]["module"] == "os"
