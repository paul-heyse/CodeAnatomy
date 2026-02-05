"""Scalar literal helpers for schema specifications."""

from __future__ import annotations

from datafusion_engine.expr.spec import ScalarLiteralSpec, scalar_literal


def parse_scalar_value(value: object) -> ScalarLiteralSpec:
    """Parse a scalar literal value.

    Returns
    -------
    ScalarLiteralSpec
        Parsed scalar literal.

    """
    return scalar_literal(value)


__all__ = ["parse_scalar_value"]
