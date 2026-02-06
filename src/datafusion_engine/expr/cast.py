"""Casting helpers for DataFusion expressions."""

from __future__ import annotations

import pyarrow as pa
from datafusion import Expr

from datafusion_engine.arrow.interop import DataTypeLike
from datafusion_engine.udf.signature import parse_type_signature

type CastType = DataTypeLike | str


def _resolve_cast_type(dtype: CastType) -> pa.DataType:
    if isinstance(dtype, pa.DataType):
        return dtype
    if isinstance(dtype, str):
        return parse_type_signature(dtype)
    msg = f"Unsupported cast type: {type(dtype)!r}."
    raise TypeError(msg)


def safe_cast(expr: Expr, dtype: CastType) -> Expr:
    """Return a cast expression for a DataFusion expression.

    Parameters
    ----------
    expr
        Expression to cast.
    dtype
        Target type expressed as a pyarrow.DataType or type string.

    Returns:
    -------
    Expr
        Cast expression.
    """
    return expr.cast(_resolve_cast_type(dtype))


__all__ = ["safe_cast"]
