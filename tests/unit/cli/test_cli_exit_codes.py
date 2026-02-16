"""Tests for CLI exit code taxonomy."""

from __future__ import annotations

from cli.exit_codes import ExitCode

PARSE_ERROR_CODE = 2
VALIDATION_ERROR_CODE = 3
PIPELINE_ERROR_MIN = 10
PIPELINE_ERROR_MAX = 19
BACKEND_ERROR_MIN = 20
BACKEND_ERROR_MAX = 29


class TestExitCodeValues:
    """Test exit code enum values."""

    @staticmethod
    def test_success_is_zero() -> None:
        """SUCCESS should be 0."""
        assert ExitCode.SUCCESS == 0

    @staticmethod
    def test_general_error_is_one() -> None:
        """GENERAL_ERROR should be 1."""
        assert ExitCode.GENERAL_ERROR == 1

    @staticmethod
    def test_parse_error_is_two() -> None:
        """PARSE_ERROR should be 2."""
        assert ExitCode.PARSE_ERROR == PARSE_ERROR_CODE

    @staticmethod
    def test_validation_error_is_three() -> None:
        """VALIDATION_ERROR should be 3."""
        assert ExitCode.VALIDATION_ERROR == VALIDATION_ERROR_CODE

    @staticmethod
    def test_pipeline_errors_in_range() -> None:
        """Pipeline errors should be in 10-19 range."""
        assert PIPELINE_ERROR_MIN <= ExitCode.EXTRACTION_ERROR <= PIPELINE_ERROR_MAX
        assert PIPELINE_ERROR_MIN <= ExitCode.NORMALIZATION_ERROR <= PIPELINE_ERROR_MAX
        assert PIPELINE_ERROR_MIN <= ExitCode.SCHEDULING_ERROR <= PIPELINE_ERROR_MAX
        assert PIPELINE_ERROR_MIN <= ExitCode.EXECUTION_ERROR <= PIPELINE_ERROR_MAX

    @staticmethod
    def test_backend_errors_in_range() -> None:
        """Backend errors should be in 20-29 range."""
        assert BACKEND_ERROR_MIN <= ExitCode.BACKEND_ERROR <= BACKEND_ERROR_MAX
        assert BACKEND_ERROR_MIN <= ExitCode.DATAFUSION_ERROR <= BACKEND_ERROR_MAX


class TestExitCodeFromException:
    """Test exception to exit code mapping."""

    @staticmethod
    def test_value_error_maps_to_validation() -> None:
        """ValueError should map to VALIDATION_ERROR."""
        exc = ValueError("test")
        assert ExitCode.from_exception(exc) == ExitCode.VALIDATION_ERROR

    @staticmethod
    def test_type_error_maps_to_validation() -> None:
        """TypeError should map to VALIDATION_ERROR."""
        exc = TypeError("test")
        assert ExitCode.from_exception(exc) == ExitCode.VALIDATION_ERROR

    @staticmethod
    def test_file_not_found_maps_to_config() -> None:
        """FileNotFoundError should map to CONFIG_ERROR."""
        exc = FileNotFoundError("test")
        assert ExitCode.from_exception(exc) == ExitCode.CONFIG_ERROR

    @staticmethod
    def test_permission_error_maps_to_config() -> None:
        """PermissionError should map to CONFIG_ERROR."""
        exc = PermissionError("test")
        assert ExitCode.from_exception(exc) == ExitCode.CONFIG_ERROR

    @staticmethod
    def test_unknown_exception_maps_to_general() -> None:
        """Unknown exceptions should map to GENERAL_ERROR."""
        exc = RuntimeError("test")
        assert ExitCode.from_exception(exc) == ExitCode.GENERAL_ERROR


class TestExitCodeInt:
    """Test exit code integer behavior."""

    @staticmethod
    def test_can_be_used_as_int() -> None:
        """ExitCode values can be used as integers."""
        assert int(ExitCode.SUCCESS) == 0
        assert int(ExitCode.GENERAL_ERROR) == 1

    @staticmethod
    def test_comparable_to_int() -> None:
        """ExitCode values can be compared to integers."""
        assert ExitCode.SUCCESS == 0
        assert ExitCode.GENERAL_ERROR != 0
