"""Decommission guardrails for removed legacy Delta query entrypoints."""

from __future__ import annotations

import ast
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]


def test_query_delta_sql_removed_from_source_tree() -> None:
    """Prevent reintroducing the removed query_delta_sql symbol."""
    roots = (_ROOT / "src", _ROOT / "tests" / "harness")
    occurrences: list[str] = []
    for root in roots:
        for path in root.rglob("*.py"):
            if not path.is_file():
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef) and node.name == "query_delta_sql":
                    relative = path.relative_to(_ROOT)
                    occurrences.append(f"{relative}:{node.lineno}")
                    continue
                if not isinstance(node, ast.Call):
                    continue
                func = node.func
                name: str | None = None
                if isinstance(func, ast.Name):
                    name = func.id
                elif isinstance(func, ast.Attribute):
                    name = func.attr
                if name != "query_delta_sql":
                    continue
                relative = path.relative_to(_ROOT)
                occurrences.append(f"{relative}:{node.lineno}")
    assert occurrences == [], f"Unexpected query_delta_sql references: {occurrences}"
