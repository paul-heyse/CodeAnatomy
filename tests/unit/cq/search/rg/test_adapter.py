"""Tests for ripgrep adapter helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import pytest
from tools.cq.search._shared.requests import RgRunRequest
from tools.cq.search.pipeline.classifier import QueryMode
from tools.cq.search.pipeline.profiles import SearchLimits
from tools.cq.search.rg import adapter
from tools.cq.search.rg.codec import RgEvent


def test_list_candidate_files_uses_files_operation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Test list candidate files uses files operation."""
    monkeypatch.setattr(
        adapter,
        "run_with_settings",
        lambda **_kwargs: type(
            "R",
            (),
            {"returncode": 0, "stdout_lines": ("src/a.py", "src/b.py")},
        )(),
    )
    rows = adapter.list_candidate_files(
        tmp_path,
        lang_types=("py",),
        limits=SearchLimits(),
    )
    assert rows == [(tmp_path / "src/a.py").resolve(), (tmp_path / "src/b.py").resolve()]


def test_find_call_candidates_uses_identifier_mode(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Test find call candidates uses identifier mode."""
    captured: dict[str, object] = {}

    def _fake_search(
        _fn: Any,
        _timeout: float,
        *,
        kwargs: dict[str, object],
    ) -> object:
        request = cast("RgRunRequest", kwargs["request"])
        captured["mode"] = request.mode
        captured["pattern"] = request.pattern
        return type(
            "P",
            (),
            {
                "events": [
                    RgEvent(
                        type="match",
                        data={
                            "path": {"text": "src/a.py"},
                            "line_number": 3,
                            "lines": {"text": "foo()"},
                            "submatches": [{"start": 0, "end": 3}],
                        },
                    )
                ]
            },
        )()

    monkeypatch.setattr(adapter, "search_sync_with_timeout", _fake_search)

    rows = adapter.find_call_candidates(
        tmp_path,
        "pkg.mod.foo",
        limits=SearchLimits(),
        lang_scope="python",
    )
    assert rows == [((tmp_path / "src/a.py").resolve(), 3)]
    assert captured["mode"] == QueryMode.IDENTIFIER
    assert "\\b" not in str(captured["pattern"])
    assert "foo\\s*\\(" in str(captured["pattern"])
