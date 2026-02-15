"""Tests for neighborhood target resolution."""

from __future__ import annotations

from pathlib import Path

from tools.cq.neighborhood.target_resolution import parse_target_spec, resolve_target


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


def test_resolve_existing_file_anchor(tmp_path: Path) -> None:
    file_path = tmp_path / "x.py"
    file_path.write_text("def inner():\n    return 1\n", encoding="utf-8")

    resolved = resolve_target(
        parse_target_spec("x.py:1"),
        root=tmp_path,
        language="python",
    )
    assert resolved.target_file == "x.py"
    assert resolved.target_line == 1
    assert resolved.resolution_kind == "anchor"


def test_resolve_symbol_fallback_with_rg(tmp_path: Path) -> None:
    (tmp_path / "a.py").write_text("def alpha():\n    return 1\n", encoding="utf-8")
    (tmp_path / "b.py").write_text("def beta():\n    return alpha()\n", encoding="utf-8")

    resolved = resolve_target(
        parse_target_spec("alpha"),
        root=tmp_path,
        language="python",
    )

    assert resolved.target_name == "alpha"
    assert resolved.target_file.endswith(".py")
    assert resolved.resolution_kind == "symbol_fallback"


def test_resolve_missing_target_returns_not_found(tmp_path: Path) -> None:
    (tmp_path / "main.py").write_text("def foo():\n    return 1\n", encoding="utf-8")

    resolved = resolve_target(
        parse_target_spec("missing_symbol"),
        root=tmp_path,
        language="python",
    )

    assert resolved.target_name == "missing_symbol"
    assert any(event.category == "not_found" for event in resolved.degrade_events)


def test_resolve_rust_file_anchor(tmp_path: Path) -> None:
    rust_dir = tmp_path / "rust"
    rust_dir.mkdir()
    (rust_dir / "lib.rs").write_text("pub fn compute_fanout() -> usize { 1 }\n", encoding="utf-8")

    resolved = resolve_target(
        parse_target_spec("rust/lib.rs:1"),
        root=tmp_path,
        language="rust",
    )

    assert resolved.target_file == "rust/lib.rs"
    assert resolved.resolution_kind == "anchor"
