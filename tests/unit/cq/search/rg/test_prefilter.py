"""Tests for ripgrep prefilter helpers."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.astgrep.sgpy_scanner import RuleSpec
from tools.cq.search.rg import prefilter


def test_extract_literal_fragments_drops_metavars() -> None:
    """Test extract literal fragments drops metavars."""
    fragments = prefilter.extract_literal_fragments("def $F($$$ARGS): return $X")
    assert any("def" in fragment for fragment in fragments)
    assert all("$" not in fragment for fragment in fragments)


def test_collect_prefilter_fragments_from_rules() -> None:
    """Test collect prefilter fragments from rules."""
    rules = (
        RuleSpec(
            rule_id="r1",
            record_type="def",
            kind="function",
            config={"rule": {"pattern": "def $F($$$ARGS):"}},
        ),
        RuleSpec(
            rule_id="r2",
            record_type="call",
            kind="call",
            config={"rule": {"pattern": "print($A)"}},
        ),
    )
    literals = prefilter.collect_prefilter_fragments(rules)
    assert literals
    assert any("def" in fragment or "print(" in fragment for fragment in literals)


def test_rg_prefilter_files_fail_open_on_runner_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Test rg prefilter files fail open on runner error."""
    file_path = tmp_path / "a.py"
    file_path.write_text("print('x')\n", encoding="utf-8")

    monkeypatch.setattr(prefilter, "run_rg_files_with_matches", lambda **_kwargs: None)

    files = prefilter.rg_prefilter_files(
        tmp_path,
        files=[file_path],
        literals=("print",),
        lang_scope="python",
    )
    assert files == [file_path]
