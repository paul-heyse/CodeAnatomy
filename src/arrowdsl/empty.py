"""Empty table helpers."""

from __future__ import annotations

import pyarrow as pa


def empty_table(schema: pa.Schema) -> pa.Table:
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
