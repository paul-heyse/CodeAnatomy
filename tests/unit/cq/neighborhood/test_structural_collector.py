"""Tests for structural neighborhood collector."""

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.snb_schema import DegradeEventV1, NeighborhoodSliceV1
from tools.cq.neighborhood.scan_snapshot import ScanSnapshot
from tools.cq.neighborhood.structural_collector import (
    StructuralNeighborhoodCollectRequest,
    collect_structural_neighborhood,
)


def _collect(
    *,
    target_name: str,
    target_file: str,
    snapshot: ScanSnapshot,
    max_per_slice: int = 50,
    slice_limits: Mapping[str, int] | None = None,
    target_position: tuple[int | None, int | None] = (None, None),
) -> tuple[tuple[NeighborhoodSliceV1, ...], tuple[DegradeEventV1, ...]]:
    target_line, target_col = target_position
    return collect_structural_neighborhood(
        StructuralNeighborhoodCollectRequest(
            target_name=target_name,
            target_file=target_file,
            snapshot=snapshot,
            target_line=target_line,
            target_col=target_col,
            max_per_slice=max_per_slice,
            slice_limits=slice_limits,
        )
    )


def test_collect_empty_snapshot() -> None:
    """Test collector with empty snapshot."""
    snapshot = ScanSnapshot(
        def_records=(),
        call_records=(),
        interval_index=None,
        file_index=None,
        calls_by_def={},
    )

    slices, degrades = _collect(
        target_name="foo",
        target_file="test.py",
        snapshot=snapshot,
    )

    # Should have one degrade event for target not found
    assert len(slices) == 0
    assert len(degrades) == 1
    assert degrades[0].stage == "structural.target_resolution"
    assert degrades[0].severity == "error"
    assert degrades[0].category == "not_found"


def test_collect_simple_function() -> None:
    """Test collector with simple function definition."""
    # Create mock records
    target_def = SgRecord(
        record="def",
        kind="function",
        file="test.py",
        start_line=10,
        start_col=0,
        end_line=15,
        end_col=0,
        text="def target_func(): pass",
        rule_id="py_def_function",
    )

    snapshot = ScanSnapshot.from_records([target_def])

    slices, degrades = _collect(
        target_name="target_func",
        target_file="test.py",
        snapshot=snapshot,
    )

    # Should have no degrades and minimal slices (no relationships)
    assert len(degrades) == 0
    # Enclosing context may be present if at module level
    assert len(slices) >= 0


def test_collect_nested_functions() -> None:
    """Test collector with nested function definitions."""
    parent_def = SgRecord(
        record="def",
        kind="function",
        file="test.py",
        start_line=5,
        start_col=0,
        end_line=20,
        end_col=0,
        text="def parent_func(): pass",
        rule_id="py_def_function",
    )

    target_def = SgRecord(
        record="def",
        kind="function",
        file="test.py",
        start_line=10,
        start_col=4,
        end_line=15,
        end_col=0,
        text="def target_func(): pass",
        rule_id="py_def_function",
    )

    child_def = SgRecord(
        record="def",
        kind="function",
        file="test.py",
        start_line=12,
        start_col=8,
        end_line=14,
        end_col=0,
        text="def child_func(): pass",
        rule_id="py_def_function",
    )

    snapshot = ScanSnapshot.from_records([parent_def, target_def, child_def])

    slices, degrades = _collect(
        target_name="target_func",
        target_file="test.py",
        snapshot=snapshot,
    )

    assert len(degrades) == 0

    # Extract slice kinds
    slice_kinds = {s.kind for s in slices}

    # Should have parents (parent_func) and children (child_func)
    assert "parents" in slice_kinds
    assert "children" in slice_kinds
    assert "enclosing_context" in slice_kinds

    # Validate parents slice
    parents_slice = next(s for s in slices if s.kind == "parents")
    assert parents_slice.total >= 1
    assert len(parents_slice.preview) >= 1

    # Validate children slice
    children_slice = next(s for s in slices if s.kind == "children")
    assert children_slice.total >= 1
    assert len(children_slice.preview) >= 1


def test_collect_siblings() -> None:
    """Test collector with sibling definitions."""
    parent_def = SgRecord(
        record="def",
        kind="class",
        file="test.py",
        start_line=1,
        start_col=0,
        end_line=50,
        end_col=0,
        text="class Parent: pass",
        rule_id="py_def_class",
    )

    target_def = SgRecord(
        record="def",
        kind="function",
        file="test.py",
        start_line=10,
        start_col=4,
        end_line=15,
        end_col=0,
        text="def target_method(self): pass",
        rule_id="py_def_function",
    )

    sibling_def = SgRecord(
        record="def",
        kind="function",
        file="test.py",
        start_line=20,
        start_col=4,
        end_line=25,
        end_col=0,
        text="def sibling_method(self): pass",
        rule_id="py_def_function",
    )

    snapshot = ScanSnapshot.from_records([parent_def, target_def, sibling_def])

    slices, degrades = _collect(
        target_name="target_method",
        target_file="test.py",
        snapshot=snapshot,
    )

    assert len(degrades) == 0

    # Extract slice kinds
    slice_kinds = {s.kind for s in slices}

    # Should have siblings
    assert "siblings" in slice_kinds

    # Validate siblings slice
    siblings_slice = next(s for s in slices if s.kind == "siblings")
    assert siblings_slice.total >= 1
    assert len(siblings_slice.preview) >= 1


def test_collect_callers_and_callees() -> None:
    """Test collector with call relationships."""
    caller_def = SgRecord(
        record="def",
        kind="function",
        file="test.py",
        start_line=5,
        start_col=0,
        end_line=8,
        end_col=0,
        text="def caller_func(): pass",
        rule_id="py_def_function",
    )

    target_def = SgRecord(
        record="def",
        kind="function",
        file="test.py",
        start_line=10,
        start_col=0,
        end_line=15,
        end_col=0,
        text="def target_func(): pass",
        rule_id="py_def_function",
    )

    callee_def = SgRecord(
        record="def",
        kind="function",
        file="test.py",
        start_line=20,
        start_col=0,
        end_line=25,
        end_col=0,
        text="def callee_func(): pass",
        rule_id="py_def_function",
    )

    # Call from target to callee
    call_to_callee = SgRecord(
        record="call",
        kind="name_call",
        file="test.py",
        start_line=12,
        start_col=4,
        end_line=12,
        end_col=20,
        text="callee_func()",
        rule_id="py_call_name",
    )

    # Call from caller to target
    call_to_target = SgRecord(
        record="call",
        kind="name_call",
        file="test.py",
        start_line=7,
        start_col=4,
        end_line=7,
        end_col=20,
        text="target_func()",
        rule_id="py_call_name",
    )

    snapshot = ScanSnapshot.from_records(
        [caller_def, target_def, callee_def, call_to_callee, call_to_target]
    )

    slices, degrades = _collect(
        target_name="target_func",
        target_file="test.py",
        snapshot=snapshot,
    )

    assert len(degrades) == 0

    # Extract slice kinds
    slice_kinds = {s.kind for s in slices}

    # Should have callees (target calls callee_func)
    assert "callees" in slice_kinds

    # Should have callers (caller_func calls target)
    assert "callers" in slice_kinds

    # Validate callees slice
    callees_slice = next(s for s in slices if s.kind == "callees")
    assert callees_slice.total >= 1
    assert len(callees_slice.preview) >= 1
    assert len(callees_slice.edges) >= 1

    # Validate callers slice
    callers_slice = next(s for s in slices if s.kind == "callers")
    assert callers_slice.total >= 1
    assert len(callers_slice.preview) >= 1
    assert len(callers_slice.edges) >= 1


def test_max_per_slice_limit() -> None:
    """Test that max_per_slice limit is respected."""
    target_def = SgRecord(
        record="def",
        kind="function",
        file="test.py",
        start_line=10,
        start_col=0,
        end_line=200,
        end_col=0,
        text="def target_func(): pass",
        rule_id="py_def_function",
    )

    # Create many child definitions (all within target span)
    children = [
        SgRecord(
            record="def",
            kind="function",
            file="test.py",
            start_line=20 + i,
            start_col=4,
            end_line=20 + i,
            end_col=20,
            text=f"def child_{i}(): pass",
            rule_id="py_def_function",
        )
        for i in range(100)
    ]

    snapshot = ScanSnapshot.from_records([target_def, *children])

    slices, degrades = _collect(
        target_name="target_func",
        target_file="test.py",
        snapshot=snapshot,
        max_per_slice=5,
    )

    assert len(degrades) == 0

    # Children slice should have total=100 but preview limited to 5
    children_slice = next(s for s in slices if s.kind == "children")
    assert children_slice.total == 100
    assert len(children_slice.preview) == 5


def test_slice_edge_structure() -> None:
    """Test that edges are correctly structured."""
    parent_def = SgRecord(
        record="def",
        kind="function",
        file="test.py",
        start_line=5,
        start_col=0,
        end_line=20,
        end_col=0,
        text="def parent_func(): pass",
        rule_id="py_def_function",
    )

    target_def = SgRecord(
        record="def",
        kind="function",
        file="test.py",
        start_line=10,
        start_col=4,
        end_line=15,
        end_col=0,
        text="def target_func(): pass",
        rule_id="py_def_function",
    )

    snapshot = ScanSnapshot.from_records([parent_def, target_def])

    slices, degrades = _collect(
        target_name="target_func",
        target_file="test.py",
        snapshot=snapshot,
    )

    assert len(degrades) == 0

    # Validate edge structure in parents slice
    parents_slice = next(s for s in slices if s.kind == "parents")
    assert len(parents_slice.edges) >= 1

    for edge in parents_slice.edges:
        assert edge.edge_id
        assert edge.source_node_id
        assert edge.target_node_id
        assert edge.edge_kind == "contains"
        assert edge.evidence_source == "structural.ast"


def test_node_ref_structure() -> None:
    """Test that node references are correctly structured."""
    target_def = SgRecord(
        record="def",
        kind="function",
        file="test.py",
        start_line=10,
        start_col=0,
        end_line=15,
        end_col=0,
        text="def target_func(x, y): return x + y",
        rule_id="py_def_function",
    )

    child_def = SgRecord(
        record="def",
        kind="function",
        file="test.py",
        start_line=12,
        start_col=4,
        end_line=14,
        end_col=0,
        text="def helper(): pass",
        rule_id="py_def_function",
    )

    snapshot = ScanSnapshot.from_records([target_def, child_def])

    slices, degrades = _collect(
        target_name="target_func",
        target_file="test.py",
        snapshot=snapshot,
    )

    assert len(degrades) == 0

    # Validate node references in children slice
    children_slice = next(s for s in slices if s.kind == "children")
    assert len(children_slice.preview) >= 1

    for node_ref in children_slice.preview:
        assert node_ref.node_id
        assert node_ref.kind
        assert node_ref.name
        assert node_ref.display_label
        assert node_ref.file_path == "test.py"


def test_target_resolution_normalizes_dot_slash_path() -> None:
    """Target file with leading './' should resolve snapshot records."""
    target_def = SgRecord(
        record="def",
        kind="function",
        file="src/test.py",
        start_line=3,
        start_col=0,
        end_line=8,
        end_col=0,
        text="def normalize_path(): pass",
        rule_id="py_def_function",
    )
    snapshot = ScanSnapshot.from_records([target_def])
    slices, degrades = _collect(
        target_name="normalize_path",
        target_file="./src/test.py",
        snapshot=snapshot,
    )
    assert len(degrades) == 0
    assert isinstance(slices, tuple)


def test_rust_pub_fn_name_extraction_for_target_resolution() -> None:
    """Rust 'pub fn' definitions should resolve by function name."""
    rust_def = SgRecord(
        record="def",
        kind="function_item",
        file="crates/corelib/src/lib.rs",
        start_line=10,
        start_col=0,
        end_line=20,
        end_col=1,
        text="pub fn compile_target(input: &str) -> String {",
        rule_id="rust_function_item",
    )
    snapshot = ScanSnapshot.from_records([rust_def])
    slices, degrades = _collect(
        target_name="compile_target",
        target_file="./crates/corelib/src/lib.rs",
        snapshot=snapshot,
    )
    assert len(degrades) == 0
    assert isinstance(slices, tuple)
