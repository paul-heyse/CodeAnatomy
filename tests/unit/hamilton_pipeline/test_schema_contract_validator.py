"""Tests for SchemaContractValidator behavior."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.schema.contracts import SchemaContract
from hamilton_pipeline.validators import SchemaContractValidator, set_schema_contracts


def test_schema_contract_validator_reports_mismatch() -> None:
    """Schema contract validation should fail on mismatched schemas."""
    contract_schema = pa.schema([pa.field("node_id", pa.string())])
    contract = SchemaContract.from_arrow_schema("cpg_nodes_v1", contract_schema)
    set_schema_contracts({"cpg_nodes_v1": contract})
    try:
        validator = SchemaContractValidator(
            dataset_name="cpg_nodes_v1",
            importance="fail",
        )
        table = pa.table({"node_id": ["1"], "extra": ["x"]})
        result = validator.validate(table)
        assert not result.passes
        assert result.message == "Schema contract validation failed."
    finally:
        set_schema_contracts({})
