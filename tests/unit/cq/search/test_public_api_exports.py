"""Public API export smoke tests for consolidated search packages."""

from __future__ import annotations

import importlib


def test_consolidated_package_imports() -> None:
    modules = [
        "tools.cq.search.rg",
        "tools.cq.search.pipeline",
        "tools.cq.search.python",
        "tools.cq.search.rust",
        "tools.cq.search.tree_sitter",
        "tools.cq.search.semantic",
        "tools.cq.search.objects",
    ]
    for module_name in modules:
        imported = importlib.import_module(module_name)
        assert imported is not None
