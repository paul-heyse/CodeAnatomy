"""Builtin Ibis UDFs for backend-native execution."""

from __future__ import annotations

import ibis
import ibis.expr.datatypes as dt


@ibis.udf.scalar.builtin
def cpg_score(value: dt.Float64) -> dt.Float64:
    """Return a placeholder scoring value for backend-native execution.

    Returns
    -------
    ibis.expr.types.Value
        Placeholder scoring expression.
    """
    return value
