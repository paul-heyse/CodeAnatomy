"""Tests for unified target spec parsing."""

from __future__ import annotations

from tools.cq.core.target_specs import parse_target_spec

TARGET_LINE = 120
TARGET_COLUMN = 4
WINDOWS_TARGET_LINE = 10


def test_parse_bundle_form() -> None:
    """Test parsing bundle kind:value form."""
    spec = parse_target_spec("function:foo")
    assert spec.bundle_kind == "function"
    assert spec.bundle_value == "foo"
    assert spec.target_name == "foo"
    assert spec.target_file is None
    assert spec.target_line is None
    assert spec.target_col is None


def test_parse_anchor_form_with_line() -> None:
    """Test parsing file:line anchor form."""
    spec = parse_target_spec(f"src/foo.py:{TARGET_LINE}")
    assert spec.bundle_kind is None
    assert spec.bundle_value is None
    assert spec.target_name is None
    assert spec.target_file == "src/foo.py"
    assert spec.target_line == TARGET_LINE
    assert spec.target_col is None


def test_parse_anchor_form_with_line_col() -> None:
    """Test parsing file:line:col anchor form."""
    spec = parse_target_spec(f"src/foo.py:{TARGET_LINE}:{TARGET_COLUMN}")
    assert spec.bundle_kind is None
    assert spec.bundle_value is None
    assert spec.target_name is None
    assert spec.target_file == "src/foo.py"
    assert spec.target_line == TARGET_LINE
    assert spec.target_col == TARGET_COLUMN


def test_parse_symbol_form() -> None:
    """Test parsing bare symbol fallback."""
    spec = parse_target_spec("build_graph_product")
    assert spec.bundle_kind is None
    assert spec.bundle_value is None
    assert spec.target_name == "build_graph_product"
    assert spec.target_file is None
    assert spec.target_line is None
    assert spec.target_col is None


def test_parse_empty_string() -> None:
    """Test parsing empty string."""
    spec = parse_target_spec("")
    assert spec.raw == ""
    assert spec.target_name is None
    assert spec.target_file is None


def test_parse_module_bundle() -> None:
    """Test parsing module bundle form."""
    spec = parse_target_spec("module:foo.bar")
    assert spec.bundle_kind == "module"
    assert spec.bundle_value == "foo.bar"
    assert spec.target_name == "foo.bar"


def test_parse_path_bundle() -> None:
    """Test parsing path bundle form."""
    spec = parse_target_spec("path:src/semantics")
    assert spec.bundle_kind == "path"
    assert spec.bundle_value == "src/semantics"
    assert spec.target_name is None


def test_parse_file_with_colons() -> None:
    """Test parsing file path containing colons."""
    spec = parse_target_spec("C:/Users/foo/bar.py:10")
    assert spec.target_file == "C:/Users/foo/bar.py"
    assert spec.target_line == WINDOWS_TARGET_LINE


def test_parse_line_zero_clamped() -> None:
    """Test line number clamping to minimum 1."""
    spec = parse_target_spec("src/foo.py:0")
    assert spec.target_line == 1


def test_parse_col_zero_allowed() -> None:
    """Test column zero is allowed."""
    spec = parse_target_spec("src/foo.py:10:0")
    assert spec.target_col == 0
