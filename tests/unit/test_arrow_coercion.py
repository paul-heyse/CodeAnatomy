"""Tests for Arrow coercion helpers."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.arrow.coercion import (
    coerce_table_to_storage,
    storage_schema,
    storage_type,
    to_arrow_table,
)

_ROW_COUNT = 2


class _ExampleExtension(pa.ExtensionType):
    def __init__(self) -> None:
        super().__init__(pa.string(), "codeanatomy.example")


def _example_extension_serialize() -> bytes:
    return b""


def _example_extension_deserialize(
    cls: type[_ExampleExtension],
    _storage_type: pa.DataType,
    _serialized: bytes,
) -> pa.ExtensionType:
    return cls()


_ExampleExtension.__arrow_ext_serialize__ = staticmethod(_example_extension_serialize)
_ExampleExtension.__arrow_ext_deserialize__ = classmethod(_example_extension_deserialize)


def _extension_array(values: list[str]) -> pa.ExtensionArray:
    ext_type = _ExampleExtension()
    storage = pa.array(values, type=pa.string())
    return pa.ExtensionArray.from_storage(ext_type, storage)


def test_storage_type_unwraps_extension() -> None:
    """Ensure storage_type unwraps extension types."""
    ext_type = _ExampleExtension()
    resolved = storage_type(ext_type)
    assert resolved == pa.string()


def test_storage_type_unwraps_struct() -> None:
    """Ensure storage_type unwraps nested struct fields."""
    ext_type = _ExampleExtension()
    struct_type = pa.struct([pa.field("value", ext_type, nullable=False)])
    resolved = storage_type(struct_type)
    assert pa.types.is_struct(resolved)
    assert resolved[0].type == pa.string()


def test_storage_type_normalizes_view_types() -> None:
    """Ensure storage_type normalizes Arrow view dtypes to storage dtypes."""
    assert storage_type(pa.string_view()) == pa.string()
    assert storage_type(pa.binary_view()) == pa.binary()
    list_view = pa.list_view(pa.string_view())
    resolved_list_view = storage_type(list_view)
    assert pa.types.is_list(resolved_list_view)
    assert resolved_list_view.value_field.type == pa.string()
    large_list_view = pa.large_list_view(pa.string_view())
    resolved_large_list_view = storage_type(large_list_view)
    assert pa.types.is_large_list(resolved_large_list_view)
    assert resolved_large_list_view.value_field.type == pa.string()


def test_storage_schema_unwraps_extension() -> None:
    """Ensure storage_schema unwraps extension fields."""
    ext_type = _ExampleExtension()
    schema = pa.schema([pa.field("value", ext_type)])
    resolved = storage_schema(schema)
    assert resolved.field("value").type == pa.string()


def test_coerce_table_to_storage() -> None:
    """Ensure tables are coerced to storage types."""
    ext_array = _extension_array(["a", "b"])
    table = pa.Table.from_arrays([ext_array], names=["value"])
    coerced = coerce_table_to_storage(table)
    assert coerced.schema.field("value").type == pa.string()
    assert coerced.column("value").to_pylist() == ["a", "b"]


def test_coerce_table_to_storage_normalizes_view_columns() -> None:
    """Ensure view-typed columns are cast to storage types."""
    value = pa.array(["a", "b"], type=pa.string()).cast(pa.string_view())
    table = pa.Table.from_arrays([value], names=["value"])
    coerced = coerce_table_to_storage(table)
    assert coerced.schema.field("value").type == pa.string()
    assert coerced.column("value").to_pylist() == ["a", "b"]


def test_to_arrow_table_from_reader() -> None:
    """Ensure to_arrow_table converts RecordBatchReader inputs."""
    batch = pa.record_batch([pa.array([1, 2])], names=["x"])
    reader = pa.RecordBatchReader.from_batches(batch.schema, [batch])
    table = to_arrow_table(reader)
    assert table.num_rows == _ROW_COUNT
    assert table.schema.names == ["x"]


def test_to_arrow_table_from_table() -> None:
    """Ensure to_arrow_table accepts tables directly."""
    table = pa.table({"x": [1, 2]})
    result = to_arrow_table(table)
    assert result.schema == table.schema
