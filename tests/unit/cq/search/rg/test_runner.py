"""Tests for ripgrep runner wrappers."""

from __future__ import annotations

from pathlib import Path

import pytest
from tools.cq.search._shared.core import RgRunRequest
from tools.cq.search.pipeline.classifier import QueryMode
from tools.cq.search.pipeline.profiles import SearchLimits
from tools.cq.search.rg.codec import RgEvent
from tools.cq.search.rg.contracts import RgRunSettingsV1
from tools.cq.search.rg.runner import (
    RgCountRequest,
    RgFilesWithMatchesRequest,
    RgProcessResult,
    build_command_from_settings,
    run_rg_count,
    run_rg_files_with_matches,
    run_with_request,
)


def test_build_command_from_settings_identifier_mode() -> None:
    """Test build command from settings identifier mode."""
    settings = RgRunSettingsV1(
        pattern="foo",
        mode="identifier",
        lang_types=("py",),
        include_globs=("**/*.py",),
        operation="json",
        paths=("src",),
    )
    command = build_command_from_settings(settings, limits=SearchLimits())
    assert "foo" in command
    assert "--json" in command
    assert "-w" in command
    assert command[-1] == "src"


def test_build_command_from_settings_files_operation() -> None:
    """Test build command from settings files operation."""
    settings = RgRunSettingsV1(
        pattern="",
        mode="regex",
        lang_types=("py",),
        operation="files",
        paths=("tools",),
    )
    command = build_command_from_settings(settings, limits=SearchLimits())
    assert "--files" in command
    assert "-e" not in command
    assert command[-1] == "tools"


def test_build_command_applies_context_multiline_and_sort() -> None:
    """Test build command applies context multiline and sort."""
    settings = RgRunSettingsV1(
        pattern="foo",
        mode="regex",
        lang_types=(),
    )
    limits = SearchLimits(context_before=2, context_after=1, multiline=True, sort_by_path=True)
    command = build_command_from_settings(settings, limits=limits)
    assert "--before-context" in command
    assert "--after-context" in command
    assert "-U" in command
    assert "--multiline-dotall" in command
    assert "--sort" in command


def test_build_command_enables_pcre2_for_lookaround_patterns() -> None:
    """Test build command enables pcre2 for lookaround patterns."""
    settings = RgRunSettingsV1(
        pattern="(?<=foo)bar",
        mode="regex",
        lang_types=(),
    )
    command = build_command_from_settings(
        settings,
        limits=SearchLimits(),
        pcre2_available=True,
    )
    assert "-P" in command


def test_run_with_request_normalizes_result(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test run with request normalizes result."""
    from tools.cq.search.rg import runner as runner_module

    def _fake_run(_request: RgRunRequest, *, pcre2_available: bool = False) -> RgProcessResult:
        _ = pcre2_available
        return RgProcessResult(
            command=["rg", "--json"],
            events=[RgEvent(type="summary", data={"stats": {"matches": 1}})],
            timed_out=False,
            returncode=0,
            stderr="",
            stdout_lines=[],
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


def test_run_rg_count_parses_stdout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test run rg count parses stdout."""
    from tools.cq.search.rg import runner as runner_module

    monkeypatch.setattr(
        runner_module,
        "run_with_settings",
        lambda **_kwargs: type("R", (), {"returncode": 0, "stdout_lines": ("a.py:3", "b.py:7")})(),
    )

    counts = run_rg_count(
        RgCountRequest(
            root=Path(),
            pattern="foo",
            mode=QueryMode.REGEX,
            lang_types=("py",),
        )
    )
    assert counts == {"a.py": 3, "b.py": 7}


def test_run_rg_files_with_matches_returns_none_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test run rg files with matches returns none on failure."""
    from tools.cq.search.rg import runner as runner_module

    monkeypatch.setattr(
        runner_module,
        "run_with_settings",
        lambda **_kwargs: type("R", (), {"returncode": 2, "stdout_lines": ()})(),
    )

    rows = run_rg_files_with_matches(
        RgFilesWithMatchesRequest(
            root=Path(),
            patterns=("foo", "bar"),
            mode=QueryMode.LITERAL,
            lang_types=("py",),
        )
    )
    assert rows is None
