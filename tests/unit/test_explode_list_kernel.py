"""Coverage for explode_list kernel ordering and alignment."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.compute.kernels import explode_list_column


def test_explode_list_kernel_preserves_parent_alignment() -> None:
    """Explode list values while preserving parent alignment."""
    table = pa.table(
        {
            "parent_id": ["p1", "p2", "p3"],
            "children": [[1, 2], None, [3]],
        }
    )
    result = explode_list_column(
        table,
        parent_id_col="parent_id",
        list_col="children",
        out_parent_col="parent",
        out_value_col="child",
    )
    data = result.to_pydict()
    assert data.get("parent", []) == ["p1", "p1", "p3"]
    assert data.get("child", []) == [1, 2, 3]


def test_explode_list_kernel_keeps_list_ordering() -> None:
    """Maintain list element ordering for exploded rows."""
    table = pa.table(
        {
            "parent_id": [10, 20],
            "children": [[3, 1, 2], [4]],
        }
    )
    result = explode_list_column(table, parent_id_col="parent_id", list_col="children")
    data = result.to_pydict()
    assert data.get("src_id", []) == [10, 10, 10, 20]
    assert data.get("dst_id", []) == [3, 1, 2, 4]
