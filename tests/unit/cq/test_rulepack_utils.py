"""Tests for ast-grep utility-rule loading and runtime usage."""

from __future__ import annotations

from pathlib import Path

from tools.cq.astgrep.rulepack_loader import load_utils
from tools.cq.astgrep.sgpy_scanner import RuleSpec, scan_files


def test_load_utils_groups_by_language() -> None:
    """Test load utils groups by language."""
    utils = load_utils(Path("tools/cq/astgrep/utils"))

    assert "python" in utils
    assert "rust" in utils
    assert "is-python-literal" in utils["python"]
    assert "is-python-builtin-call" in utils["python"]
    assert "is-rust-test-attr" in utils["rust"]


def test_scan_files_supports_utils_matches_config(tmp_path: Path) -> None:
    """Test scan files supports utils matches config."""
    test_file = tmp_path / "calls.py"
    test_file.write_text("print('hello')\ncustom('x')\n", encoding="utf-8")

    rule = RuleSpec(
        rule_id="py_call_builtin_print",
        record_type="call",
        kind="builtin_call",
        config={
            "rule": {
                "kind": "call",
                "has": {
                    "matches": "is-python-builtin-call",
                    "field": "function",
                },
            },
            "utils": {
                "is-python-builtin-call": {
                    "kind": "identifier",
                    "regex": "^print$",
                }
            },
        },
    )

    records = scan_files([test_file], (rule,), tmp_path, prefilter=False)
    assert len(records) == 1
    assert "print" in records[0].text
