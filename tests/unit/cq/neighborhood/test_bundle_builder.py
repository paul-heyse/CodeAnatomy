"""Tests for bundle builder and assembly."""

from __future__ import annotations

from pathlib import Path

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.neighborhood.bundle_builder import (
    BundleBuildRequest,
    build_neighborhood_bundle,
    plan_feasible_slices,
)
from tools.cq.neighborhood.scan_snapshot import ScanSnapshot


def _def_record(
    kind: str,
    file: str,
    start_line: int,
    end_line: int,
    text: str,
) -> SgRecord:
    """Create a def-type SgRecord for testing.

    Returns:
    -------
    SgRecord
        Test record with "def" record type.
    """
    return SgRecord(
        record="def",
        kind=kind,
        file=file,
        start_line=start_line,
        start_col=0,
        end_line=end_line,
        end_col=10,
        text=text,
        rule_id=f"python-def-{kind}",
    )


def test_build_bundle_with_structural_only() -> None:
    """Test bundle building with structural slices only (LSP disabled)."""
    def_records = (
        _def_record("function", "test.py", 1, 3, "def target():\n    pass"),
        _def_record("function", "test.py", 5, 7, "def caller():\n    target()"),
    )

    snapshot = ScanSnapshot(def_records=def_records)

    request = BundleBuildRequest(
        target_name="target",
        target_file="test.py",
        root=Path("/test"),
        snapshot=snapshot,
        language="python",
        enable_lsp=False,
    )

    bundle = build_neighborhood_bundle(request)

    assert bundle.bundle_id is not None
    assert bundle.schema_version == "cq.snb.v1"
    assert bundle.subject is not None
    assert bundle.subject.name == "target"
    assert len(bundle.slices) > 0
    assert bundle.meta is not None
    assert bundle.meta.tool == "cq"


def test_build_bundle_with_lsp_enabled_produces_degrades() -> None:
    """Test that LSP enabled produces degrade events (stub implementation)."""
    def_records = (
        _def_record("function", "test.py", 1, 3, "def target():\n    pass"),
    )

    snapshot = ScanSnapshot(def_records=def_records)

    request = BundleBuildRequest(
        target_name="target",
        target_file="test.py",
        root=Path("/test"),
        snapshot=snapshot,
        language="python",
        enable_lsp=True,
    )

    bundle = build_neighborhood_bundle(request)

    assert len(bundle.diagnostics) > 0
    lsp_degrades = [d for d in bundle.diagnostics if "lsp" in d.stage.lower()]
    assert len(lsp_degrades) > 0


def test_artifact_storage_only_when_overflow() -> None:
    """Test that artifacts are only stored when total > preview count."""
    def_records = (
        _def_record("function", "test.py", 1, 3, "def target():\n    pass"),
    )

    snapshot = ScanSnapshot(def_records=def_records)

    request = BundleBuildRequest(
        target_name="target",
        target_file="test.py",
        root=Path("/test"),
        snapshot=snapshot,
        language="python",
        enable_lsp=False,
        artifact_dir=Path("/tmp/test_artifacts_cq"),
        top_k=10,
    )

    bundle = build_neighborhood_bundle(request)

    # Minimal test data < 10 items per slice → no artifacts
    assert len(bundle.artifacts) == 0


def test_deterministic_bundle_id() -> None:
    """Test that bundle ID is deterministic based on target."""
    def_records = (
        _def_record("function", "test.py", 1, 3, "def target():\n    pass"),
    )

    snapshot = ScanSnapshot(def_records=def_records)

    request1 = BundleBuildRequest(
        target_name="target",
        target_file="test.py",
        root=Path("/test"),
        snapshot=snapshot,
        language="python",
    )

    request2 = BundleBuildRequest(
        target_name="target",
        target_file="test.py",
        root=Path("/test"),
        snapshot=snapshot,
        language="python",
    )

    bundle1 = build_neighborhood_bundle(request1)
    bundle2 = build_neighborhood_bundle(request2)

    assert bundle1.bundle_id == bundle2.bundle_id


def test_plan_feasible_slices_returns_degrades() -> None:
    """Test that plan_feasible_slices returns degrade events for stub."""
    requested = ("references", "implementations")
    capabilities: dict[str, object] = {}

    feasible, degrades = plan_feasible_slices(requested, capabilities)

    assert len(feasible) == 0
    assert len(degrades) == len(requested)

    for degrade in degrades:
        assert degrade.stage == "lsp.planning"
        assert degrade.severity == "info"
        assert degrade.category == "not_implemented"


def test_graph_summary_aggregates_slices() -> None:
    """Test that graph summary aggregates stats from all slices."""
    def_records = (
        _def_record("function", "test.py", 1, 3, "def target():\n    pass"),
        _def_record("function", "test.py", 5, 7, "def caller():\n    target()"),
    )

    snapshot = ScanSnapshot(def_records=def_records)

    request = BundleBuildRequest(
        target_name="target",
        target_file="test.py",
        root=Path("/test"),
        snapshot=snapshot,
        language="python",
        enable_lsp=False,
    )

    bundle = build_neighborhood_bundle(request)

    assert bundle.graph is not None
    assert bundle.graph.node_count >= 0
    assert bundle.graph.edge_count >= 0


def test_metadata_includes_timing() -> None:
    """Test that metadata includes timing information."""
    def_records = (
        _def_record("function", "test.py", 1, 3, "def target():\n    pass"),
    )

    snapshot = ScanSnapshot(def_records=def_records)

    request = BundleBuildRequest(
        target_name="target",
        target_file="test.py",
        root=Path("/test"),
        snapshot=snapshot,
        language="python",
    )

    bundle = build_neighborhood_bundle(request)

    assert bundle.meta is not None
    assert bundle.meta.created_at_ms is not None
    assert bundle.meta.created_at_ms >= 0


def test_subject_node_from_snapshot() -> None:
    """Test that subject node is built from snapshot when target found."""
    def_records = (
        _def_record(
            "function",
            "test.py",
            10,
            15,
            "def my_function(x, y):\n    return x + y",
        ),
    )

    snapshot = ScanSnapshot(def_records=def_records)

    request = BundleBuildRequest(
        target_name="my_function",
        target_file="test.py",
        root=Path("/test"),
        snapshot=snapshot,
        language="python",
    )

    bundle = build_neighborhood_bundle(request)

    assert bundle.subject is not None
    assert bundle.subject.name == "my_function"
    assert bundle.subject.kind == "function"
    assert bundle.subject.file_path == "test.py"


def test_subject_node_fallback_when_not_found() -> None:
    """Test that subject node uses fallback when target not found in snapshot."""
    snapshot = ScanSnapshot()

    request = BundleBuildRequest(
        target_name="missing_function",
        target_file="test.py",
        root=Path("/test"),
        snapshot=snapshot,
        language="python",
    )

    bundle = build_neighborhood_bundle(request)

    assert bundle.subject is not None
    assert bundle.subject.name == "missing_function"
    assert bundle.subject.file_path == "test.py"
    assert bundle.subject.kind == "unknown"


def test_preview_limit_applied_to_slices() -> None:
    """Test that top_k preview limit is applied to slices."""
    def_records = tuple(
        _def_record(
            "function",
            "test.py",
            i,
            i + 2,
            f"def func_{i}():\n    pass",
        )
        for i in range(1, 50, 3)
    )

    snapshot = ScanSnapshot(def_records=def_records)

    request = BundleBuildRequest(
        target_name="func_1",
        target_file="test.py",
        root=Path("/test"),
        snapshot=snapshot,
        language="python",
        top_k=5,
        enable_lsp=False,
    )

    bundle = build_neighborhood_bundle(request)

    for slice_ in bundle.slices:
        assert len(slice_.preview) <= 5


def test_workspace_root_in_metadata() -> None:
    """Test that workspace root is captured in metadata."""
    def_records = (
        _def_record("function", "test.py", 1, 3, "def target():\n    pass"),
    )

    snapshot = ScanSnapshot(def_records=def_records)

    workspace = Path("/my/workspace")
    request = BundleBuildRequest(
        target_name="target",
        target_file="test.py",
        root=workspace,
        snapshot=snapshot,
        language="python",
    )

    bundle = build_neighborhood_bundle(request)

    assert bundle.meta is not None
    assert bundle.meta.workspace_root == str(workspace)
