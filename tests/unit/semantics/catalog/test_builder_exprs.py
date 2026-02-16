"""Tests for shared analysis builder expression helpers."""

from __future__ import annotations

import pytest
from datafusion import lit

from semantics.catalog.builder_exprs import _span_expr, _stable_id_expr


def test_stable_id_expr_requires_parts() -> None:
    """Stable id helper rejects empty part lists."""
    with pytest.raises(ValueError, match="at least one part"):
        _ = _stable_id_expr("prefix", (), null_sentinel="__null__")


def test_span_expr_uses_span_make_udf() -> None:
    """Span helper emits span_make UDF expressions."""
    expr = _span_expr(bstart=lit(1), bend=lit(2))

    assert "span_make" in str(expr)
