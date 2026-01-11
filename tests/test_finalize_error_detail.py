"""Tests for finalize error_detail aggregation."""

import arrowdsl.pyarrow_core as pa
from arrowdsl.contracts import Contract
from arrowdsl.finalize import finalize
from arrowdsl.runtime import ExecutionContext, RuntimeProfile


def test_error_detail_schema_and_aggregation() -> None:
    """Validate error_detail schema and aggregation behavior."""
    schema = pa.schema(
        [
            ("id", pa.string()),
            ("value", pa.int32()),
            ("kind", pa.string()),
        ]
    )
    contract = Contract(
        name="test_contract",
        schema=schema,
        key_fields=("id",),
        required_non_null=("value", "kind"),
    )
    table = pa.table({"id": ["node-1"], "value": [None], "kind": [None]})
    ctx = ExecutionContext(runtime=RuntimeProfile(name="TEST"))

    result = finalize(table, contract=contract, ctx=ctx)
    errors = result.errors

    assert errors.num_rows == 1
    assert errors.schema.field("row_id").type == pa.int64()

    error_detail_type = errors.schema.field("error_detail").type
    expected_detail_struct = pa.struct(
        [
            ("code", pa.string()),
            ("message", pa.string()),
            ("column", pa.string()),
            ("severity", pa.string()),
            ("rule_name", pa.string()),
            ("rule_priority", pa.int32()),
            ("source", pa.string()),
        ]
    )
    assert error_detail_type == pa.list_(expected_detail_struct)

    detail = errors["error_detail"].to_pylist()[0]
    assert isinstance(detail, list)
    assert {item["column"] for item in detail} == {"value", "kind"}
    assert all(item["code"] == "REQUIRED_NON_NULL" for item in detail)
