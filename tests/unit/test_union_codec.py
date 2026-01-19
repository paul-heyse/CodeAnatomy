"""Union codec round-trip tests."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.schema.nested_builders import nested_array_factory, union_array_from_tagged_values
from arrowdsl.schema.union_codec import (
    decode_union_table,
    encode_union_table,
    schema_has_union_encoding,
    schema_has_union_types,
)
from arrowdsl.spec.infra import SCALAR_UNION_TYPE


def test_union_codec_round_trip() -> None:
    """Encode union columns for Delta and decode them back to unions."""
    union_values = [
        {"__union_tag__": "int", "value": 1},
        {"__union_tag__": "string", "value": "alpha"},
        {"__union_tag__": "bool", "value": True},
        None,
    ]
    scalar_union = union_array_from_tagged_values(union_values, union_type=SCALAR_UNION_TYPE)
    payload_type = pa.struct(
        [
            pa.field("value_union", SCALAR_UNION_TYPE, nullable=True),
            pa.field("name", pa.string(), nullable=False),
        ]
    )
    payload_values: list[dict[str, object] | None] = [
        {"value_union": {"__union_tag__": "float", "value": 1.5}, "name": "first"},
        {"value_union": {"__union_tag__": "binary", "value": b"bin"}, "name": "second"},
        {"value_union": None, "name": "third"},
        None,
    ]
    payload_array = nested_array_factory(
        pa.field("payload", payload_type, nullable=True),
        payload_values,
    )
    schema = pa.schema(
        [
            pa.field("scalar_union", SCALAR_UNION_TYPE, nullable=True),
            pa.field("payload", payload_type, nullable=True),
        ]
    )
    table = pa.Table.from_arrays([scalar_union, payload_array], schema=schema)
    assert schema_has_union_types(table.schema)

    encoded = encode_union_table(table)
    assert schema_has_union_encoding(encoded.schema)
    assert not schema_has_union_types(encoded.schema)

    decoded = decode_union_table(encoded)
    assert decoded.schema.equals(table.schema)
    assert decoded.to_pydict() == table.to_pydict()
