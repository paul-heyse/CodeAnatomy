"""Empty table helpers."""

from __future__ import annotations

import arrowdsl.pyarrow_core as pa
from arrowdsl.pyarrow_protocols import SchemaLike, TableLike


def empty_table(schema: SchemaLike) -> TableLike:
    """Return an empty table with the provided schema.

    Parameters
    ----------
    schema:
        Target schema.

    Returns
    -------
    pyarrow.Table
        Empty table with the requested schema.
    """
    return pa.Table.from_arrays([pa.array([], type=field.type) for field in schema], schema=schema)
