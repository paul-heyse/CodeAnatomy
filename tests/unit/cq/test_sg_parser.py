"""Tests for sg_parser helpers."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.astgrep.sgpy_scanner import RuleSpec
from tools.cq.core.types import QueryLanguage
from tools.cq.query import sg_parser
from tools.cq.query.parser import QueryParseError
from tools.cq.query.sg_parser import normalize_record_types, sg_scan


def test_normalize_record_types_invalid() -> None:
    """Invalid record types should raise a QueryParseError."""
    with pytest.raises(QueryParseError, match="Invalid record types"):
        normalize_record_types({"bogus"})


def test_sg_scan_enables_prefilter(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """sg_scan should enable rg prefilter for multi-file ast-grep scans."""
    test_file = tmp_path / "a.py"
    test_file.write_text("def foo():\n    return 1\n", encoding="utf-8")

    monkeypatch.setattr(sg_parser, "_tabulate_scan_files", lambda *_args, **_kwargs: [test_file])
    monkeypatch.setattr(
        sg_parser,
        "get_rules_for_types",
        lambda *_args, **_kwargs: (
            RuleSpec(
                rule_id="py_def_function",
                record_type="def",
                kind="function",
                config={"rule": {"pattern": "def $F($$$ARGS):"}},
            ),
        ),
    )

    observed: dict[str, object] = {}

    def _fake_scan(
        files: list[Path],
        _rules: tuple[RuleSpec, ...],
        _root: Path,
        *,
        lang: QueryLanguage = "python",
        prefilter: bool = True,
    ) -> list[object]:
        _ = lang
        observed["files"] = files
        observed["prefilter"] = prefilter
        return []

    monkeypatch.setattr(sg_parser, "scan_files", _fake_scan)
    rows = sg_scan(paths=[tmp_path], root=tmp_path, lang="python")
    assert rows == []
    assert observed["prefilter"] is True
