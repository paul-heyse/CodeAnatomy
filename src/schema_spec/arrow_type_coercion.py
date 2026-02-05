"""Arrow type coercion helpers for schema specs."""

from __future__ import annotations

from typing import cast

import pyarrow as pa

from datafusion_engine.arrow import interop
from schema_spec.arrow_types import ArrowTypeBase, ArrowTypeSpec, arrow_type_from_pyarrow

type ArrowTypeLike = ArrowTypeSpec | pa.DataType | interop.DataTypeLike


def coerce_arrow_type(value: ArrowTypeLike) -> ArrowTypeSpec:
    """Return a canonical ArrowTypeSpec for an Arrow-like input.

    Returns
    -------
    ArrowTypeSpec
        Canonical Arrow type specification.
    """
    if isinstance(value, ArrowTypeBase):
        return cast("ArrowTypeSpec", value)
    dtype = interop.ensure_arrow_dtype(value)
    return arrow_type_from_pyarrow(dtype)


__all__ = ["ArrowTypeLike", "coerce_arrow_type"]
