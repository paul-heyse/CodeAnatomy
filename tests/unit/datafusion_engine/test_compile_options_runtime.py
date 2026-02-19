"""Tests for msgspec compile options runtime validation."""

from __future__ import annotations

import pytest

from datafusion_engine.compile.options import DataFusionCompileOptionsSpec, compile_options_runtime


def test_compile_options_runtime_accepts_valid_spec() -> None:
    """Valid specs should pass compile options runtime parsing."""
    spec = DataFusionCompileOptionsSpec(
        cache=True,
        cache_max_columns=16,
        prepared_statements=True,
        prepared_param_types={"limit": "Int64"},
    )
    resolved = compile_options_runtime(spec)
    assert resolved == spec


def test_compile_options_runtime_rejects_cache_columns_when_cache_disabled() -> None:
    """cache_max_columns requires cache=True."""
    spec = DataFusionCompileOptionsSpec(cache=False, cache_max_columns=1)
    with pytest.raises(ValueError, match="cache_max_columns requires cache=True"):
        compile_options_runtime(spec)


def test_compile_options_runtime_rejects_prepared_types_when_disabled() -> None:
    """prepared_param_types requires prepared_statements=True."""
    spec = DataFusionCompileOptionsSpec(
        prepared_statements=False,
        prepared_param_types={"foo": "Utf8"},
    )
    with pytest.raises(ValueError, match="prepared_param_types requires prepared_statements=True"):
        compile_options_runtime(spec)
