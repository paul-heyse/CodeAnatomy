"""Tests for sgpy_scanner module using ast-grep-py."""

from __future__ import annotations

from pathlib import Path

from tools.cq.astgrep.rulepack_loader import clear_rulepack_cache, load_default_rulepacks
from tools.cq.astgrep.sgpy_scanner import (
    RecordType,
    RuleSpec,
    SgRecord,
    filter_records_by_type,
    group_records_by_file,
    scan_files,
    scan_with_pattern,
)

clear_rulepack_cache()
_PY_RULES = {rule.rule_id: rule for rule in load_default_rulepacks().get("python", ())}
PY_CALL_NAME = _PY_RULES["py_call_name"]
PY_DEF_CLASS = _PY_RULES["py_def_class"]
PY_DEF_FUNCTION = _PY_RULES["py_def_function"]
PY_IMPORT = _PY_RULES["py_import"]

FUNCTION_LINE_NUMBER = 3
TWO_RECORDS = 2


class TestRuleSpec:
    """Tests for RuleSpec dataclass."""

    @staticmethod
    def test_to_config_returns_rule_dict() -> None:
        """to_config should return the rule portion of config."""
        spec = RuleSpec(
            rule_id="test_rule",
            record_type="def",
            kind="function",
            config={"rule": {"kind": "function_definition"}},
        )
        config = spec.to_config()
        assert config == {"kind": "function_definition"}

    @staticmethod
    def test_to_config_empty_config() -> None:
        """to_config should handle empty config."""
        spec = RuleSpec(
            rule_id="test_rule",
            record_type="def",
            kind="function",
            config={},
        )
        config = spec.to_config()
        assert config == {}


class TestSgRecord:
    """Tests for SgRecord dataclass."""

    @staticmethod
    def test_location_property() -> None:
        """Location property should return file:line:col format."""
        record = SgRecord(
            record="def",
            kind="function",
            file="test.py",
            start_line=10,
            start_col=4,
            end_line=15,
            end_col=0,
            text="def foo(): pass",
            rule_id="py_def_function",
        )
        assert record.location == "test.py:10:4"


class TestScanFiles:
    """Tests for scan_files function."""

    @staticmethod
    def test_scan_empty_file(tmp_path: Path) -> None:
        """Scanning an empty file should return empty list."""
        test_file = tmp_path / "empty.py"
        test_file.write_text("", encoding="utf-8")

        records = scan_files([test_file], (PY_DEF_FUNCTION,), tmp_path)
        assert records == []

    @staticmethod
    def test_scan_function_definition(tmp_path: Path) -> None:
        """Scanning should find function definitions."""
        test_file = tmp_path / "funcs.py"
        test_file.write_text("def foo():\n    pass\n", encoding="utf-8")

        records = scan_files([test_file], (PY_DEF_FUNCTION,), tmp_path)
        assert len(records) == 1
        assert records[0].kind == "function"
        assert records[0].record == "def"
        assert records[0].start_line == 1

    @staticmethod
    def test_scan_class_definition(tmp_path: Path) -> None:
        """Scanning should find class definitions."""
        test_file = tmp_path / "classes.py"
        test_file.write_text("class Foo:\n    pass\n", encoding="utf-8")

        records = scan_files([test_file], (PY_DEF_CLASS,), tmp_path)
        assert len(records) == 1
        assert records[0].kind == "class"
        assert records[0].record == "def"

    @staticmethod
    def test_scan_imports(tmp_path: Path) -> None:
        """Scanning should find import statements."""
        test_file = tmp_path / "imports.py"
        test_file.write_text("import os\n", encoding="utf-8")

        records = scan_files([test_file], (PY_IMPORT,), tmp_path)
        assert len(records) == 1
        assert records[0].record == "import"

    @staticmethod
    def test_scan_calls(tmp_path: Path) -> None:
        """Scanning should find function calls."""
        test_file = tmp_path / "calls.py"
        test_file.write_text("print('hello')\n", encoding="utf-8")

        records = scan_files([test_file], (PY_CALL_NAME,), tmp_path)
        assert len(records) == 1
        assert records[0].record == "call"

    @staticmethod
    def test_scan_line_number_conversion(tmp_path: Path) -> None:
        """Line numbers should be 1-indexed."""
        test_file = tmp_path / "lines.py"
        test_file.write_text("\n\ndef foo():\n    pass\n", encoding="utf-8")

        records = scan_files([test_file], (PY_DEF_FUNCTION,), tmp_path)
        assert len(records) == 1
        # Function is on line 3 (1-indexed)
        assert records[0].start_line == FUNCTION_LINE_NUMBER

    @staticmethod
    def test_scan_unicode_file(tmp_path: Path) -> None:
        """Scanning should handle unicode content."""
        test_file = tmp_path / "unicode.py"
        test_file.write_text("def greet():\n    return '\u4f60\u597d'\n", encoding="utf-8")

        records = scan_files([test_file], (PY_DEF_FUNCTION,), tmp_path)
        assert len(records) == 1

    @staticmethod
    def test_scan_malformed_file_skip(tmp_path: Path) -> None:
        """Scanning should skip files that can't be read."""
        nonexistent = tmp_path / "nonexistent.py"
        records = scan_files([nonexistent], (PY_DEF_FUNCTION,), tmp_path)
        assert records == []

    @staticmethod
    def test_scan_multiple_files(tmp_path: Path) -> None:
        """Scanning should process multiple files."""
        file1 = tmp_path / "a.py"
        file1.write_text("def foo(): pass\n", encoding="utf-8")
        file2 = tmp_path / "b.py"
        file2.write_text("def bar(): pass\n", encoding="utf-8")

        records = scan_files([file1, file2], (PY_DEF_FUNCTION,), tmp_path)
        assert len(records) == TWO_RECORDS


class TestScanWithPattern:
    """Tests for scan_with_pattern function."""

    @staticmethod
    def test_simple_pattern_match(tmp_path: Path) -> None:
        """scan_with_pattern should find pattern matches."""
        test_file = tmp_path / "test.py"
        test_file.write_text("print('hello')\n", encoding="utf-8")

        matches = scan_with_pattern([test_file], "print($A)", tmp_path)
        assert len(matches) == 1
        assert "range" in matches[0]
        assert "text" in matches[0]

    @staticmethod
    def test_pattern_with_metavar(tmp_path: Path) -> None:
        """scan_with_pattern should capture metavariables."""
        test_file = tmp_path / "test.py"
        test_file.write_text("print('hello')\n", encoding="utf-8")

        matches = scan_with_pattern([test_file], "print($A)", tmp_path)
        assert len(matches) == 1
        # Metavariables are captured
        assert "metaVariables" in matches[0]
        assert "$A" in matches[0]["metaVariables"]

    @staticmethod
    def test_pattern_with_variadic_metavar(tmp_path: Path) -> None:
        """scan_with_pattern should capture variadic metavariables."""
        test_file = tmp_path / "test.py"
        test_file.write_text("print(1, 2)\n", encoding="utf-8")

        matches = scan_with_pattern([test_file], "print($$$ARGS)", tmp_path)
        assert len(matches) == 1
        metavars = matches[0]["metaVariables"]
        assert "$$$ARGS" in metavars
        capture = metavars["$$$ARGS"]
        assert capture["kind"] == "multi"
        assert len(capture["nodes"]) == TWO_RECORDS


class TestFilterRecordsByType:
    """Tests for filter_records_by_type function."""

    @staticmethod
    def test_filter_none_returns_all() -> None:
        """None filter should return all records."""
        records = [
            SgRecord("def", "function", "test.py", 1, 0, 2, 0, "def f(): pass", "py_def_function"),
            SgRecord("call", "name_call", "test.py", 3, 0, 3, 5, "foo()", "py_call_name"),
        ]
        filtered = filter_records_by_type(records, None)
        assert len(filtered) == TWO_RECORDS

    @staticmethod
    def test_filter_by_type() -> None:
        """Should filter to specified types."""
        records = [
            SgRecord("def", "function", "test.py", 1, 0, 2, 0, "def f(): pass", "py_def_function"),
            SgRecord("call", "name_call", "test.py", 3, 0, 3, 5, "foo()", "py_call_name"),
        ]
        record_types: set[RecordType] = {"def"}
        filtered = filter_records_by_type(records, record_types)
        assert len(filtered) == 1
        assert filtered[0].record == "def"


class TestGroupRecordsByFile:
    """Tests for group_records_by_file function."""

    @staticmethod
    def test_group_by_file() -> None:
        """Should group records by file path."""
        records = [
            SgRecord("def", "function", "a.py", 1, 0, 2, 0, "def f(): pass", "py_def_function"),
            SgRecord("def", "function", "b.py", 1, 0, 2, 0, "def g(): pass", "py_def_function"),
            SgRecord("def", "function", "a.py", 5, 0, 6, 0, "def h(): pass", "py_def_function"),
        ]
        grouped = group_records_by_file(records)
        assert len(grouped) == TWO_RECORDS
        assert len(grouped["a.py"]) == TWO_RECORDS
        assert len(grouped["b.py"]) == 1
