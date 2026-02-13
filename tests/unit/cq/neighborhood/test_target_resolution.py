"""Tests for canonical neighborhood target resolution."""

from __future__ import annotations

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.neighborhood.scan_snapshot import ScanSnapshot
from tools.cq.neighborhood.target_resolution import parse_target_spec, resolve_target


def _def_record(
    *,
    name: str,
    file: str,
    start_line: int,
    end_line: int,
    start_col: int = 0,
    end_col: int = 20,
) -> SgRecord:
    return SgRecord(
        record="def",
        kind="function",
        file=file,
        start_line=start_line,
        start_col=start_col,
        end_line=end_line,
        end_col=end_col,
        text=f"def {name}(): pass",
        rule_id="py_def_function",
    )


def test_parse_symbol_target() -> None:
    spec = parse_target_spec("build_graph")
    assert spec.target_name == "build_graph"
    assert spec.target_file is None
    assert spec.target_line is None


def test_parse_file_line_col_target() -> None:
    spec = parse_target_spec("src/module.py:42:7")
    assert spec.target_file == "src/module.py"
    assert spec.target_line == 42
    assert spec.target_col == 7


def test_resolve_symbol_only_prefers_deterministic_first() -> None:
    snapshot = ScanSnapshot(
        def_records=(
            _def_record(name="foo", file="b.py", start_line=1, end_line=2),
            _def_record(name="foo", file="a.py", start_line=10, end_line=20),
        )
    )
    resolved = resolve_target(parse_target_spec("foo"), snapshot)
    assert resolved.target_name == "foo"
    assert resolved.target_file == "a.py"
    assert resolved.resolution_kind == "symbol_fallback"
    assert any(event.category == "ambiguous" for event in resolved.degrade_events)


def test_resolve_anchor_target_selects_innermost() -> None:
    snapshot = ScanSnapshot(
        def_records=(
            _def_record(name="outer", file="x.py", start_line=1, end_line=100),
            _def_record(name="inner", file="x.py", start_line=40, end_line=60),
        )
    )
    resolved = resolve_target(parse_target_spec("x.py:50"), snapshot)
    assert resolved.target_name == "inner"
    assert resolved.target_file == "x.py"
    assert resolved.target_line == 50
    assert resolved.resolution_kind == "anchor"


def test_resolve_missing_target_returns_error_degrade() -> None:
    snapshot = ScanSnapshot(def_records=())
    resolved = resolve_target(parse_target_spec("missing_symbol"), snapshot)
    assert resolved.target_name == "missing_symbol"
    assert any(event.category == "not_found" for event in resolved.degrade_events)


def test_resolve_rust_pub_crate_anchor_extracts_function_name() -> None:
    snapshot = ScanSnapshot(
        def_records=(
            SgRecord(
                record="def",
                kind="function_item",
                file="rust/lib.rs",
                start_line=7,
                start_col=0,
                end_line=14,
                end_col=1,
                text="pub(crate) fn compute_fanout(input: usize) -> usize {",
                rule_id="rust_function_item",
            ),
        )
    )
    resolved = resolve_target(parse_target_spec("rust/lib.rs:8"), snapshot)
    assert resolved.target_name == "compute_fanout"
    assert resolved.target_file == "rust/lib.rs"
    assert resolved.resolution_kind == "anchor"
