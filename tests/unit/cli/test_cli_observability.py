"""Tests for CLI observability integration."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from cli.context import RunContext


class TestRunContextOtelOptions:
    """Test RunContext otel_options field."""

    @staticmethod
    def test_default_otel_options_is_none() -> None:
        """Default otel_options should be None."""
        ctx = RunContext(run_id="test", log_level="INFO")
        assert ctx.otel_options is None

    @staticmethod
    def test_can_set_otel_options() -> None:
        """Should be able to set otel_options."""
        from obs.otel import OtelBootstrapOptions

        options = OtelBootstrapOptions(enable_traces=True)
        ctx = RunContext(
            run_id="test",
            log_level="INFO",
            otel_options=options,
        )
        assert ctx.otel_options is not None
        assert ctx.otel_options.enable_traces is True

    @staticmethod
    def test_context_is_frozen() -> None:
        """RunContext should be immutable."""
        ctx = RunContext(run_id="test", log_level="INFO")
        ctx_any: Any = ctx
        with pytest.raises(FrozenInstanceError):
            ctx_any.run_id = "changed"


class TestOtelBootstrapOptions:
    """Test OtelBootstrapOptions configuration."""

    @staticmethod
    def test_all_signals_none_by_default() -> None:
        """All signal flags should be None by default."""
        from obs.otel import OtelBootstrapOptions

        options = OtelBootstrapOptions()
        assert options.enable_traces is None
        assert options.enable_metrics is None
        assert options.enable_logs is None
        assert options.test_mode is None

    @staticmethod
    def test_can_enable_individual_signals() -> None:
        """Should be able to enable individual signals."""
        from obs.otel import OtelBootstrapOptions

        options = OtelBootstrapOptions(
            enable_traces=True,
            enable_metrics=False,
            enable_logs=True,
        )
        assert options.enable_traces is True
        assert options.enable_metrics is False
        assert options.enable_logs is True

    @staticmethod
    def test_test_mode_flag() -> None:
        """Should support test_mode flag."""
        from obs.otel import OtelBootstrapOptions

        options = OtelBootstrapOptions(test_mode=True)
        assert options.test_mode is True
