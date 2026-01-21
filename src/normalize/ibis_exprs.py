"""Shared Ibis expression helpers for normalization."""

from __future__ import annotations

import ibis
from ibis.expr.types import Value

from normalize.text_index import DEFAULT_POSITION_ENCODING, ENC_UTF8, ENC_UTF16, ENC_UTF32


def position_encoding_norm_expr(value: Value) -> Value:
    """Normalize position encoding values to SCIP enum integers.

    Parameters
    ----------
    value:
        Input expression holding encoding values.

    Returns
    -------
    ibis.expr.types.Value
        Normalized encoding expression.
    """
    default = ibis.literal(DEFAULT_POSITION_ENCODING, type="int32")
    text = value.cast("string")
    normalized = text.strip().upper()
    as_int = normalized.try_cast("int32")
    valid_int = as_int.isin([ENC_UTF8, ENC_UTF16, ENC_UTF32])
    return (
        ibis.case()
        .when(
            value.isnull(),
            default,
        )
        .when(
            valid_int,
            as_int,
        )
        .when(
            normalized.contains("UTF8"),
            ibis.literal(ENC_UTF8, type="int32"),
        )
        .when(
            normalized.contains("UTF16"),
            ibis.literal(ENC_UTF16, type="int32"),
        )
        .when(
            normalized.contains("UTF32"),
            ibis.literal(ENC_UTF32, type="int32"),
        )
        .else_(default)
    )


__all__ = ["position_encoding_norm_expr"]
