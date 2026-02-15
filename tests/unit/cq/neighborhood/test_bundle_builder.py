"""Tests for tree-sitter neighborhood bundle builder."""

from __future__ import annotations

from pathlib import Path

from tools.cq.neighborhood.bundle_builder import (
    BundleBuildRequest,
    build_neighborhood_bundle,
    plan_feasible_slices,
)


def _write_python_fixture(root: Path) -> None:
    (root / "test.py").write_text(
        """
class Adapter:
    def target(self, value):
        result = helper(value)
        return result


def helper(value):
    return value
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_build_bundle_tree_sitter_only(tmp_path: Path) -> None:
    _write_python_fixture(tmp_path)
    request = BundleBuildRequest(
        target_name="target",
        target_file="test.py",
        root=tmp_path,
        language="python",
        enable_semantic_enrichment=False,
    )

    bundle = build_neighborhood_bundle(request)

    assert bundle.bundle_id
    assert bundle.subject is not None
    assert bundle.subject.file_path == "test.py"
    assert bundle.meta is not None
    assert bundle.meta.workspace_root == str(tmp_path)
    assert any(slice_.kind == "parents" for slice_ in bundle.slices)


def test_bundle_deterministic_id(tmp_path: Path) -> None:
    _write_python_fixture(tmp_path)
    request1 = BundleBuildRequest(
        target_name="target",
        target_file="test.py",
        root=tmp_path,
        language="python",
    )
    request2 = BundleBuildRequest(
        target_name="target",
        target_file="test.py",
        root=tmp_path,
        language="python",
    )

    bundle1 = build_neighborhood_bundle(request1)
    bundle2 = build_neighborhood_bundle(request2)
    assert bundle1.bundle_id == bundle2.bundle_id


def test_plan_feasible_slices_returns_degrades() -> None:
    requested = ("references", "implementations")
    capabilities: dict[str, object] = {}

    feasible, degrades = plan_feasible_slices(requested, capabilities)

    assert len(feasible) == 0
    assert len(degrades) == len(requested)
    assert all(degrade.category == "unavailable" for degrade in degrades)


def test_bundle_includes_semantic_marker_diagnostic(tmp_path: Path) -> None:
    _write_python_fixture(tmp_path)
    request = BundleBuildRequest(
        target_name="target",
        target_file="test.py",
        root=tmp_path,
        language="python",
        enable_semantic_enrichment=True,
    )

    bundle = build_neighborhood_bundle(request)

    assert any(d.stage == "semantic.enrichment" for d in bundle.diagnostics)


def test_bundle_subject_fallback_when_anchor_missing(tmp_path: Path) -> None:
    _write_python_fixture(tmp_path)
    request = BundleBuildRequest(
        target_name="missing_symbol",
        target_file="test.py",
        root=tmp_path,
        language="python",
    )

    bundle = build_neighborhood_bundle(request)

    assert bundle.subject is not None
    assert bundle.subject.name == "missing_symbol"
    assert any(d.category == "anchor_unresolved" for d in bundle.diagnostics)
