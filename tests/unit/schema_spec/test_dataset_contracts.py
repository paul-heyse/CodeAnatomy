"""Unit tests for dataset_contracts module."""

from __future__ import annotations

import pyarrow as pa

from schema_spec.dataset_contracts import ContractRow, TableSchemaContract


def test_contract_row_instantiation() -> None:
    """Contract row stores constraints and version metadata."""
    row = ContractRow(constraints=("id IS NOT NULL",), version=1)
    assert row.version == 1


def test_table_schema_contract_instantiation() -> None:
    """Table schema contract preserves declared partition column metadata."""
    contract = TableSchemaContract(
        file_schema=pa.schema([("id", pa.int64())]),
        partition_cols=(("id", pa.int64()),),
    )
    assert contract.partition_cols[0][0] == "id"
