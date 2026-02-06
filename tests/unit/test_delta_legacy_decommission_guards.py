"""Decommission guardrails for legacy Delta query entrypoints."""

from __future__ import annotations

import ast
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]


def test_query_delta_sql_has_no_production_callsites() -> None:
    """Prevent reintroducing production callsites to legacy query_delta_sql."""
    roots = (_ROOT / "src", _ROOT / "tests" / "harness")
    callsites: list[str] = []
    for root in roots:
        for path in root.rglob("*.py"):
            if not path.is_file():
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
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
                callsites.append(f"{relative}:{node.lineno}")
    assert callsites == [], f"Unexpected query_delta_sql callsites: {callsites}"
