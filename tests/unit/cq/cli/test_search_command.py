"""Unit tests for search command include scope wiring."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from tools.cq.cli_app.commands.search import search as cmd_search
from tools.cq.cli_app.context import CliContext
from tools.cq.cli_app.params import SearchParams
from tools.cq.core.schema import CqResult, mk_result, mk_runmeta, ms


def _empty_result(argv: list[str], root: Path) -> CqResult:
    run = mk_runmeta(
        macro="search",
        argv=argv,
        root=str(root),
        started_ms=ms(),
        toolchain={},
    )
    return mk_result(run)


def test_search_command_in_dir_directory_appends_recursive_glob(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """--in with a directory should pass <dir>/** to smart_search include globs."""
    captured: dict[str, Any] = {}

    def fake_smart_search(*_args: object, **kwargs: object) -> CqResult:
        captured.update(kwargs)
        return _empty_result(argv=["cq", "search"], root=tmp_path)

    monkeypatch.setattr("tools.cq.search.smart_search.smart_search", fake_smart_search)

    ctx = CliContext.build(argv=["cq", "search"], root=tmp_path)
    cmd_search(
        "PythonAnalysisSession",
        opts=SearchParams(in_dir="tools/cq"),
        ctx=ctx,
    )

    assert captured.get("include_globs") == ["tools/cq/**"]


def test_search_command_in_dir_file_keeps_file_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """--in with a file path should pass the exact file path include glob."""
    target = tmp_path / "tools" / "cq" / "search"
    target.mkdir(parents=True)
    file_path = target / "python_analysis_session.py"
    file_path.write_text("class PythonAnalysisSession:\n    pass\n", encoding="utf-8")

    captured: dict[str, Any] = {}

    def fake_smart_search(*_args: object, **kwargs: object) -> CqResult:
        captured.update(kwargs)
        return _empty_result(argv=["cq", "search"], root=tmp_path)

    monkeypatch.setattr("tools.cq.search.smart_search.smart_search", fake_smart_search)

    ctx = CliContext.build(argv=["cq", "search"], root=tmp_path)
    cmd_search(
        "PythonAnalysisSession",
        opts=SearchParams(in_dir="tools/cq/search/python_analysis_session.py"),
        ctx=ctx,
    )

    assert captured.get("include_globs") == ["tools/cq/search/python_analysis_session.py"]
