"""Unit tests for engine error taxonomy."""

from __future__ import annotations

from engine.facade import (
    EngineCompileError,
    EngineError,
    EngineRuleViolationError,
    EngineRuntimeError,
    EngineValidationError,
)


class TestEngineErrorTaxonomy:
    """Error classification tests."""

    def test_error_stage_map_validation(self) -> None:
        """Verify validation errors map to EngineValidationError."""
        assert issubclass(EngineValidationError, EngineError)

    def test_error_stage_map_compilation(self) -> None:
        """Verify compilation errors map to EngineCompileError."""
        assert issubclass(EngineCompileError, EngineError)

    def test_error_stage_map_rule_violation(self) -> None:
        """Verify rule violation errors map to EngineRuleViolationError."""
        assert issubclass(EngineRuleViolationError, EngineError)

    def test_error_stage_map_runtime(self) -> None:
        """Verify runtime errors map to EngineRuntimeError."""
        assert issubclass(EngineRuntimeError, EngineError)

    def test_exit_codes(self) -> None:
        """Verify each error subclass has correct exit_code."""
        assert EngineValidationError("test").exit_code == 2
        assert EngineCompileError("test").exit_code == 3
        assert EngineRuleViolationError("test").exit_code == 4
        assert EngineRuntimeError("test").exit_code == 5
