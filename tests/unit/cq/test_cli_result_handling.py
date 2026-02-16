"""Tests for CLI result handling.

These tests verify that:
1. FilterConfig correctly identifies when filters are present
2. Result rendering works for different formats
3. handle_result uses context settings correctly
"""

from __future__ import annotations

from pathlib import Path

from tools.cq.cli_app.app import app
from tools.cq.cli_app.context import CliContext, CliResult, FilterConfig
from tools.cq.cli_app.result import render_result
from tools.cq.cli_app.result_action import (
    CQ_DEFAULT_RESULT_ACTION,
    apply_result_action,
    cq_result_action,
)
from tools.cq.cli_app.telemetry import invoke_with_telemetry
from tools.cq.cli_app.types import OutputFormat
from tools.cq.core.schema import CqResult, RunMeta

INT_RESULT_EXIT_CODE = 42
PASSTHROUGH_EXIT_CODE = 7
WRAPPED_INT_RESULT = 5
CLI_PARSE_ERROR_EXIT_CODE = 2


class TestFilterConfig:
    """Tests for FilterConfig dataclass."""

    def test_empty_config_has_no_filters(self) -> None:
        """Test empty filter config has no filters."""
        config = FilterConfig()
        assert not config.has_filters

    def test_include_has_filters(self) -> None:
        """Test filter config with include patterns has filters."""
        config = FilterConfig(include=["src/"])
        assert config.has_filters

    def test_exclude_has_filters(self) -> None:
        """Test filter config with exclude patterns has filters."""
        config = FilterConfig(exclude=["tests/"])
        assert config.has_filters

    def test_impact_has_filters(self) -> None:
        """Test filter config with impact has filters."""
        config = FilterConfig(impact=["high"])
        assert config.has_filters

    def test_confidence_has_filters(self) -> None:
        """Test filter config with confidence has filters."""
        config = FilterConfig(confidence=["med"])
        assert config.has_filters

    def test_severity_has_filters(self) -> None:
        """Test filter config with severity has filters."""
        config = FilterConfig(severity=["error"])
        assert config.has_filters

    def test_limit_has_filters(self) -> None:
        """Test filter config with limit has filters."""
        config = FilterConfig(limit=10)
        assert config.has_filters


class TestRenderResult:
    """Tests for render_result function."""

    def _make_result(self, tmp_path: Path) -> CqResult:
        """Create a minimal CqResult for testing.

        Returns:
        -------
        CqResult
            A minimal CqResult for testing.
        """
        run = RunMeta(
            macro="test",
            argv=["test"],
            root=str(tmp_path),
            started_ms=0,
            elapsed_ms=0,
        )
        return CqResult(run=run)

    def test_render_md(self, tmp_path: Path) -> None:
        """Test rendering as markdown."""
        result = self._make_result(tmp_path)
        output = render_result(result, OutputFormat.md)
        # Should produce some markdown output
        assert isinstance(output, str)

    def test_render_json(self, tmp_path: Path) -> None:
        """Test rendering as JSON."""
        result = self._make_result(tmp_path)
        output = render_result(result, OutputFormat.json)
        # Should be valid JSON
        import json

        parsed = json.loads(output)
        assert "run" in parsed

    def test_render_summary(self, tmp_path: Path) -> None:
        """Test rendering as summary."""
        result = self._make_result(tmp_path)
        output = render_result(result, OutputFormat.summary)
        assert isinstance(output, str)


class TestCliResult:
    """Tests for CliResult wrapper."""

    def test_int_result_not_cq_result(self, tmp_path: Path) -> None:
        """Test that int result is not a CqResult."""
        ctx = CliContext.build(argv=["test"], root=tmp_path)
        result = CliResult(result=0, context=ctx)
        assert not result.is_cq_result

    def test_cq_result_is_cq_result(self, tmp_path: Path) -> None:
        """Test that CqResult is identified correctly."""
        ctx = CliContext.build(argv=["test"], root=tmp_path)
        run = RunMeta(
            macro="test",
            argv=["test"],
            root=str(tmp_path),
            started_ms=0,
            elapsed_ms=0,
        )
        cq_result = CqResult(run=run)
        result = CliResult(result=cq_result, context=ctx)
        assert result.is_cq_result

    def test_exit_code_from_int_result(self, tmp_path: Path) -> None:
        """Test getting exit code from int result."""
        ctx = CliContext.build(argv=["test"], root=tmp_path)
        result = CliResult(result=INT_RESULT_EXIT_CODE, context=ctx)
        assert result.get_exit_code() == INT_RESULT_EXIT_CODE

    def test_explicit_exit_code_overrides(self, tmp_path: Path) -> None:
        """Test that explicit exit code overrides result."""
        ctx = CliContext.build(argv=["test"], root=tmp_path)
        result = CliResult(result=0, context=ctx, exit_code=1)
        assert result.get_exit_code() == 1

    def test_default_exit_code_is_zero(self, tmp_path: Path) -> None:
        """Test that default exit code for non-int result is 0."""
        ctx = CliContext.build(argv=["test"], root=tmp_path)
        run = RunMeta(
            macro="test",
            argv=["test"],
            root=str(tmp_path),
            started_ms=0,
            elapsed_ms=0,
        )
        cq_result = CqResult(run=run)
        result = CliResult(result=cq_result, context=ctx)
        assert result.get_exit_code() == 0


class TestResultAction:
    """Tests for CQ result-action adapter."""

    def test_result_action_passes_int(self) -> None:
        """Test int passthrough behavior."""
        assert cq_result_action(PASSTHROUGH_EXIT_CODE) == PASSTHROUGH_EXIT_CODE

    def test_result_action_handles_cli_result_int(self, tmp_path: Path) -> None:
        """Test CliResult wrapper handling for integer payloads."""
        ctx = CliContext.build(argv=["cq", "cache"], root=tmp_path)
        wrapped = CliResult(result=WRAPPED_INT_RESULT, context=ctx)
        assert cq_result_action(wrapped) == WRAPPED_INT_RESULT

    def test_apply_result_action_pipeline_default(self, tmp_path: Path) -> None:
        """Test default result-action pipeline returns a normalized exit code."""
        ctx = CliContext.build(argv=["cq", "cache"], root=tmp_path)
        wrapped = CliResult(result=WRAPPED_INT_RESULT, context=ctx)
        assert apply_result_action(wrapped, CQ_DEFAULT_RESULT_ACTION) == WRAPPED_INT_RESULT


class TestInvokeWithTelemetry:
    """Tests for CQ telemetry wrapper."""

    def test_invoke_success(self, tmp_path: Path) -> None:
        """Test successful command invocation telemetry."""
        ctx = CliContext.build(argv=["cq", "cache"], root=tmp_path)
        exit_code, event = invoke_with_telemetry(app, ["cache"], ctx=ctx)
        assert exit_code == 0
        assert event.ok is True
        assert event.command is not None
        assert event.parse_ms >= 0
        assert event.event_id is not None
        assert event.event_uuid_version in {0, 7}

    def test_invoke_parse_error(self, tmp_path: Path) -> None:
        """Test parse-failure telemetry."""
        ctx = CliContext.build(argv=["cq", "unknown"], root=tmp_path)
        exit_code, event = invoke_with_telemetry(app, ["unknown-command"], ctx=ctx)
        assert exit_code == CLI_PARSE_ERROR_EXIT_CODE
        assert event.ok is False
        assert event.error_stage is not None
        assert event.event_id is not None
