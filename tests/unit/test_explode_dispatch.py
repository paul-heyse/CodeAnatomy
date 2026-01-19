"""Unit tests for explode dispatch helpers."""

from __future__ import annotations

from typing import cast

import pyarrow as pa
import pytest

from arrowdsl.compute.explode_dispatch import explode_list_dispatch
from arrowdsl.compute.expr_core import ExplodeSpec
from arrowdsl.core.context import execution_context_factory
from arrowdsl.core.interop import pc

ibis = pytest.importorskip("ibis")

EXPECTED_EMPTY_LIST_ROWS = 4


def test_explode_dispatch_handles_empty_lists() -> None:
    """Preserve empty list rows when keep_empty is enabled."""
    table = pa.table({"id": [1, 2, 3], "items": [[1, 2], None, []]})
    spec = ExplodeSpec(
        parent_keys=("id",),
        list_col="items",
        value_col="item",
        idx_col="idx",
        keep_empty=True,
    )
    ctx = execution_context_factory("default")
    result = explode_list_dispatch(table, spec=spec, ctx=ctx).output
    exploded = cast("pa.Table", result)
    assert exploded.num_rows == EXPECTED_EMPTY_LIST_ROWS
    id_values = exploded.column("id").to_pylist()
    assert id_values.count(2) == 1
    assert id_values.count(3) == 1


def test_explode_dispatch_list_struct_unpacks_fields() -> None:
    """Explode list<struct> values and access struct fields."""
    struct_type = pa.struct([("key", pa.string()), ("value", pa.int64())])
    table = pa.table(
        {
            "id": [1],
            "items": [[{"key": "a", "value": 1}, {"key": "b", "value": 2}]],
        },
        schema=pa.schema([("id", pa.int64()), ("items", pa.list_(struct_type))]),
    )
    spec = ExplodeSpec(parent_keys=("id",), list_col="items", value_col="item", idx_col=None)
    ctx = execution_context_factory("default")
    result = explode_list_dispatch(table, spec=spec, ctx=ctx).output
    exploded = cast("pa.Table", result)
    assert pa.types.is_struct(exploded.column("item").type)
    keys = cast("pa.Array", pc.struct_field(exploded.column("item"), "key")).to_pylist()
    assert keys == ["a", "b"]


def test_explode_dispatch_map_entries() -> None:
    """Explode map values into key/value entries."""
    map_type = pa.map_(pa.string(), pa.int64())
    table = pa.table(
        {"id": [1], "attrs": [{"alpha": 1, "beta": 2}]},
        schema=pa.schema([("id", pa.int64()), ("attrs", map_type)]),
    )
    spec = ExplodeSpec(parent_keys=("id",), list_col="attrs", value_col="entry", idx_col=None)
    ctx = execution_context_factory("default")
    result = explode_list_dispatch(table, spec=spec, ctx=ctx).output
    exploded = cast("pa.Table", result)
    assert pa.types.is_struct(exploded.column("entry").type)
    keys = cast("pa.Array", pc.struct_field(exploded.column("entry"), "key")).to_pylist()
    values = cast("pa.Array", pc.struct_field(exploded.column("entry"), "value")).to_pylist()
    assert keys == ["alpha", "beta"]
    assert values == [1, 2]


def test_explode_dispatch_uses_ibis_lane() -> None:
    """Route Ibis tables through the Ibis explode lane."""
    table = ibis.memtable({"id": [1], "items": [[1, 2]]})
    spec = ExplodeSpec(parent_keys=("id",), list_col="items", value_col="item", idx_col=None)
    result = explode_list_dispatch(table, spec=spec).output
    assert isinstance(result, ibis.expr.types.Table)
