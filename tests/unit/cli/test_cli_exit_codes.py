"""Tests for CLI exit code taxonomy."""

from __future__ import annotations

from cli.exit_codes import ExitCode


class TestExitCodeValues:
    """Test exit code enum values."""

    def test_success_is_zero(self) -> None:
        """SUCCESS should be 0."""
        assert ExitCode.SUCCESS == 0

    def test_general_error_is_one(self) -> None:
        """GENERAL_ERROR should be 1."""
        assert ExitCode.GENERAL_ERROR == 1

    def test_parse_error_is_two(self) -> None:
        """PARSE_ERROR should be 2."""
        assert ExitCode.PARSE_ERROR == 2

    def test_validation_error_is_three(self) -> None:
        """VALIDATION_ERROR should be 3."""
        assert ExitCode.VALIDATION_ERROR == 3

    def test_pipeline_errors_in_range(self) -> None:
        """Pipeline errors should be in 10-19 range."""
        assert 10 <= ExitCode.EXTRACTION_ERROR <= 19
        assert 10 <= ExitCode.NORMALIZATION_ERROR <= 19
        assert 10 <= ExitCode.SCHEDULING_ERROR <= 19
        assert 10 <= ExitCode.EXECUTION_ERROR <= 19

    def test_backend_errors_in_range(self) -> None:
        """Backend errors should be in 20-29 range."""
        assert 20 <= ExitCode.BACKEND_ERROR <= 29
        assert 20 <= ExitCode.DATAFUSION_ERROR <= 29


class TestExitCodeFromException:
    """Test exception to exit code mapping."""

    def test_value_error_maps_to_validation(self) -> None:
        """ValueError should map to VALIDATION_ERROR."""
        exc = ValueError("test")
        assert ExitCode.from_exception(exc) == ExitCode.VALIDATION_ERROR

    def test_type_error_maps_to_validation(self) -> None:
        """TypeError should map to VALIDATION_ERROR."""
        exc = TypeError("test")
        assert ExitCode.from_exception(exc) == ExitCode.VALIDATION_ERROR

    def test_file_not_found_maps_to_config(self) -> None:
        """FileNotFoundError should map to CONFIG_ERROR."""
        exc = FileNotFoundError("test")
        assert ExitCode.from_exception(exc) == ExitCode.CONFIG_ERROR

    def test_permission_error_maps_to_config(self) -> None:
        """PermissionError should map to CONFIG_ERROR."""
        exc = PermissionError("test")
        assert ExitCode.from_exception(exc) == ExitCode.CONFIG_ERROR

    def test_unknown_exception_maps_to_general(self) -> None:
        """Unknown exceptions should map to GENERAL_ERROR."""
        exc = RuntimeError("test")
        assert ExitCode.from_exception(exc) == ExitCode.GENERAL_ERROR


class TestExitCodeInt:
    """Test exit code integer behavior."""

    def test_can_be_used_as_int(self) -> None:
        """ExitCode values can be used as integers."""
        assert int(ExitCode.SUCCESS) == 0
        assert int(ExitCode.GENERAL_ERROR) == 1

    def test_comparable_to_int(self) -> None:
        """ExitCode values can be compared to integers."""
        assert ExitCode.SUCCESS == 0
        assert ExitCode.GENERAL_ERROR != 0
