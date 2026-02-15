"""Tests for ripgrep runner wrappers."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.search._shared.core import RgRunRequest
from tools.cq.search.pipeline.classifier import QueryMode
from tools.cq.search.pipeline.profiles import SearchLimits
from tools.cq.search.rg.codec import RgEvent
from tools.cq.search.rg.contracts import RgRunSettingsV1
from tools.cq.search.rg.runner import RgProcessResult, build_command_from_settings, run_with_request


def test_build_command_from_settings_contains_pattern() -> None:
    settings = RgRunSettingsV1(
        pattern="foo",
        mode="regex",
        lang_types=("py",),
        include_globs=("**/*.py",),
    )
    command = build_command_from_settings(settings, limits=SearchLimits())
    assert "foo" in command
    assert "--json" in command


def test_run_with_request_normalizes_result(monkeypatch: pytest.MonkeyPatch) -> None:
    from tools.cq.search.rg import runner as runner_module

    def _fake_run(_request: RgRunRequest) -> RgProcessResult:
        return RgProcessResult(
            command=["rg", "--json"],
            events=[RgEvent(type="summary", data={"stats": {"matches": 1}})],
            timed_out=False,
            returncode=0,
            stderr="",
        )

    monkeypatch.setattr(runner_module, "run_rg_json", _fake_run)
    request = RgRunRequest(
        root=Path(),
        pattern="foo",
        mode=QueryMode.REGEX,
        lang_types=("py",),
        limits=SearchLimits(),
    )
    result = run_with_request(request)
    assert result.returncode == 0
    assert len(result.events) == 1
