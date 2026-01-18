"""Explode list column kernel checks."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.compute.kernels import explode_list_column


def test_explode_list_column_alignment() -> None:
    """Align exploded rows with parent IDs."""
    table = pa.table(
        {
            "parent_id": [1, 2, 3],
            "values": [[10, 11], [], [12]],
        }
    )
    result = explode_list_column(
        table,
        parent_id_col="parent_id",
        list_col="values",
    )
    assert result["src_id"].to_pylist() == [1, 1, 3]
    assert result["dst_id"].to_pylist() == [10, 11, 12]
