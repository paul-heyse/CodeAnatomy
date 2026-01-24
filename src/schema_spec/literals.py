"""Scalar literal helpers for schema specifications."""

from __future__ import annotations

from arrowdsl.core.expr_types import ScalarValue
from arrowdsl.core.interop import ScalarLike


def parse_scalar_value(value: object) -> ScalarValue:
    """Parse a scalar literal value.

    Returns
    -------
    ScalarValue
        Parsed scalar literal.

    Raises
    ------
    TypeError
        Raised when the value is not a supported scalar type.
    """
    if value is None:
        return None
    if isinstance(value, (bool, int, float, str, bytes)):
        return value
    if isinstance(value, ScalarLike):
        return value
    msg = "Scalar literal must be a supported scalar type."
    raise TypeError(msg)


__all__ = ["parse_scalar_value"]
