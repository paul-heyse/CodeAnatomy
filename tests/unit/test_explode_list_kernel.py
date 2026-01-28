"""Coverage for explode_list kernel ordering and alignment."""

from __future__ import annotations

import pyarrow as pa

from arrow_utils.core.expr_types import ExplodeSpec
from datafusion_engine.kernels import explode_list_kernel


def test_explode_list_kernel_preserves_parent_alignment() -> None:
    """Explode list values while preserving parent alignment."""
    table = pa.table(
        {
            "parent_id": ["p1", "p2", "p3"],
            "children": [[1, 2], None, [3]],
        }
    )
    spec = ExplodeSpec(
        parent_keys=("parent_id",),
        list_col="children",
        value_col="child",
        idx_col="idx",
        keep_empty=True,
    )
    result = explode_list_kernel(table, spec=spec)
    data = result.to_pydict()
    assert data.get("parent_id", []) == ["p1", "p1", "p2", "p3"]
    assert data.get("child", []) == [1, 2, None, 3]
    assert data.get("idx", []) == [0, 1, None, 0]


def test_explode_list_kernel_keeps_list_ordering() -> None:
    """Maintain list element ordering for exploded rows."""
    table = pa.table(
        {
            "parent_id": [10, 20],
            "children": [[3, 1, 2], [4]],
        }
    )
    spec = ExplodeSpec(
        parent_keys=("parent_id",),
        list_col="children",
        value_col="dst_id",
        idx_col="idx",
        keep_empty=True,
    )
    result = explode_list_kernel(table, spec=spec)
    data = result.to_pydict()
    assert data.get("parent_id", []) == [10, 10, 10, 20]
    assert data.get("dst_id", []) == [3, 1, 2, 4]
    assert data.get("idx", []) == [0, 1, 2, 0]
