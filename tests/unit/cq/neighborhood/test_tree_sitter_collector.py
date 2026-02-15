"""Tests for tree-sitter neighborhood collector."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.neighborhood.tree_sitter_collector import collect_tree_sitter_neighborhood
from tools.cq.neighborhood.tree_sitter_contracts import TreeSitterNeighborhoodCollectRequest


@pytest.mark.skipif(
    __import__("importlib").util.find_spec("tree_sitter_python") is None,
    reason="tree_sitter_python not available",
)
def test_collect_tree_sitter_neighborhood_python(tmp_path: Path) -> None:
    target_file = tmp_path / "sample.py"
    target_file.write_text(
        """
def helper(x):
    return x


def target(value):
    temp = helper(value)
    return temp
""".strip()
        + "\n",
        encoding="utf-8",
    )

    result = collect_tree_sitter_neighborhood(
        TreeSitterNeighborhoodCollectRequest(
            root=str(tmp_path),
            target_name="target",
            target_file="sample.py",
            language="python",
            target_line=4,
            target_col=4,
            max_per_slice=5,
        )
    )

    assert result.subject is not None
    kinds = {slice_.kind for slice_ in result.slices}
    assert "parents" in kinds
    assert "children" in kinds
    assert "siblings" in kinds


def test_collect_tree_sitter_neighborhood_missing_file(tmp_path: Path) -> None:
    result = collect_tree_sitter_neighborhood(
        TreeSitterNeighborhoodCollectRequest(
            root=str(tmp_path),
            target_name="target",
            target_file="missing.py",
            language="python",
        )
    )

    assert result.subject is None
    assert any(d.category == "file_missing" for d in result.diagnostics)
