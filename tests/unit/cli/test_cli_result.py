"""Tests for CLI result contract."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
from typing import Any

import pytest

from cli.exit_codes import ExitCode
from cli.result import CliResult

CUSTOM_ERROR_EXIT_CODE = 42


class TestCliResultSuccess:
    """Test CliResult.success factory."""

    @staticmethod
    def test_creates_success_result() -> None:
        """success() should create a result with exit code 0."""
        result = CliResult.success()
        assert result.exit_code == ExitCode.SUCCESS
        assert result.ok is True

    @staticmethod
    def test_with_summary() -> None:
        """success() should accept a summary."""
        result = CliResult.success(summary="Build complete")
        assert result.summary == "Build complete"

    @staticmethod
    def test_with_artifacts() -> None:
        """success() should accept artifacts."""
        artifacts = {"output": Path("/tmp/output")}
        result = CliResult.success(artifacts=artifacts)
        assert result.artifacts == artifacts

    @staticmethod
    def test_with_metrics() -> None:
        """success() should accept metrics."""
        metrics = {"duration_ms": 100.5}
        result = CliResult.success(metrics=metrics)
        assert result.metrics == metrics


class TestCliResultError:
    """Test CliResult.error factory."""

    @staticmethod
    def test_creates_error_result() -> None:
        """error() should create a result with specified exit code."""
        result = CliResult.error(ExitCode.VALIDATION_ERROR)
        assert result.exit_code == ExitCode.VALIDATION_ERROR
        assert result.ok is False

    @staticmethod
    def test_with_int_exit_code() -> None:
        """error() should accept integer exit code."""
        result = CliResult.error(CUSTOM_ERROR_EXIT_CODE)
        assert result.exit_code == CUSTOM_ERROR_EXIT_CODE

    @staticmethod
    def test_with_summary() -> None:
        """error() should accept a summary."""
        result = CliResult.error(ExitCode.GENERAL_ERROR, summary="Failed")
        assert result.summary == "Failed"


class TestCliResultFromException:
    """Test CliResult.from_exception factory."""

    @staticmethod
    def test_creates_error_from_value_error() -> None:
        """from_exception should map ValueError to VALIDATION_ERROR."""
        exc = ValueError("Invalid input")
        result = CliResult.from_exception(exc)
        assert result.exit_code == ExitCode.VALIDATION_ERROR
        assert result.ok is False

    @staticmethod
    def test_uses_exception_message() -> None:
        """from_exception should use exception message as summary."""
        exc = ValueError("Invalid input")
        result = CliResult.from_exception(exc)
        assert result.summary == "Invalid input"

    @staticmethod
    def test_custom_summary_overrides() -> None:
        """from_exception should allow custom summary."""
        exc = ValueError("Invalid input")
        result = CliResult.from_exception(exc, summary="Custom message")
        assert result.summary == "Custom message"


class TestCliResultOk:
    """Test CliResult.ok property."""

    @staticmethod
    def test_ok_true_for_zero() -> None:
        """Ok should be True when exit_code is 0."""
        result = CliResult(exit_code=0)
        assert result.ok is True

    @staticmethod
    def test_ok_false_for_nonzero() -> None:
        """Ok should be False when exit_code is nonzero."""
        result = CliResult(exit_code=1)
        assert result.ok is False
        result = CliResult(exit_code=ExitCode.VALIDATION_ERROR)
        assert result.ok is False


class TestCliResultImmutable:
    """Test CliResult immutability."""

    @staticmethod
    def test_is_frozen() -> None:
        """CliResult should be frozen (immutable)."""
        result = CliResult.success()
        result_any: Any = result
        with pytest.raises(FrozenInstanceError):
            result_any.exit_code = 1
