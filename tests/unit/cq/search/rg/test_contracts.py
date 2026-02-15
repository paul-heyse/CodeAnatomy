"""Tests for ripgrep lane contracts."""

from __future__ import annotations

from pathlib import Path

from tools.cq.search._shared.core import RgRunRequest
from tools.cq.search.pipeline.classifier import QueryMode
from tools.cq.search.pipeline.profiles import SearchLimits
from tools.cq.search.rg.codec import RgEvent
from tools.cq.search.rg.contracts import result_from_process, settings_from_request
from tools.cq.search.rg.runner import RgProcessResult


def test_settings_from_request() -> None:
    request = RgRunRequest(
        root=Path(),
        pattern="foo",
        mode=QueryMode.REGEX,
        lang_types=("py",),
        limits=SearchLimits(),
        include_globs=["**/*.py"],
        exclude_globs=["**/tests/**"],
    )
    settings = settings_from_request(request)
    assert settings.pattern == "foo"
    assert settings.mode == "regex"
    assert settings.include_globs == ("**/*.py",)


def test_result_from_process() -> None:
    process = RgProcessResult(
        command=["rg", "--json"],
        events=[RgEvent(type="summary", data={"stats": {"matches": 1}})],
        timed_out=False,
        returncode=0,
        stderr="",
    )
    result = result_from_process(process)
    assert result.command == ("rg", "--json")
    assert result.returncode == 0
    assert len(result.events) == 1
