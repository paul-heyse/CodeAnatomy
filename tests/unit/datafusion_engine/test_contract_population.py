"""Tests for contract auto-population from catalog schemas."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.arrow.metadata_codec import encode_metadata_list
from datafusion_engine.schema.contract_population import (
    ContractPopulationOptions,
    ContractPopulationResult,
    populate_contract_from_schema,
    populate_contract_from_schema_detailed,
)
from schema_spec.system import ContractCatalogSpec, ContractSpec


def test_populate_contract_from_schema_basic() -> None:
    """Populate a contract from a simple schema."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.utf8(), nullable=True),
            pa.field("value", pa.float64(), nullable=False),
        ]
    )

    contract = populate_contract_from_schema("test_table", schema)

    assert isinstance(contract, ContractSpec)
    assert contract.name == "test_table"
    assert contract.version == 1
    # id should be inferred as key field, value as required non-null
    table_schema = contract.table_schema
    assert "id" in table_schema.key_fields
    assert "value" in table_schema.required_non_null


def test_populate_contract_infers_key_fields_from_suffix() -> None:
    """Infer key fields from column name suffixes."""
    schema = pa.schema(
        [
            pa.field("user_id", pa.int64(), nullable=False),
            pa.field("order_pk", pa.utf8(), nullable=False),
            pa.field("data", pa.utf8(), nullable=True),
        ]
    )

    contract = populate_contract_from_schema("test_table", schema)

    assert "user_id" in contract.table_schema.key_fields
    assert "order_pk" in contract.table_schema.key_fields


def test_populate_contract_respects_metadata_key_fields() -> None:
    """Prefer key fields from metadata over inference."""
    metadata = {
        b"key_fields": encode_metadata_list(["explicit_key"]),
    }
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),  # Would be inferred
            pa.field("explicit_key", pa.utf8(), nullable=False),
            pa.field("data", pa.utf8(), nullable=True),
        ],
        metadata=metadata,
    )

    result = populate_contract_from_schema_detailed("test_table", schema)

    assert "explicit_key" in result.contract.table_schema.key_fields
    assert result.metadata_key_fields == ("explicit_key",)
    # Since metadata has key fields, inference should be skipped
    assert result.inferred_key_fields == ()


def test_populate_contract_disables_key_field_inference() -> None:
    """Disable key field inference via options."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.utf8(), nullable=True),
        ]
    )

    options = ContractPopulationOptions(infer_key_fields=False)
    contract = populate_contract_from_schema("test_table", schema, options=options)

    assert contract.table_schema.key_fields == ()


def test_populate_contract_disables_required_inference() -> None:
    """Disable required non-null inference via options."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("value", pa.float64(), nullable=False),
        ]
    )

    options = ContractPopulationOptions(infer_required_non_null=False)
    contract = populate_contract_from_schema("test_table", schema, options=options)

    # Without inference, required_non_null should only come from key fields
    # id is a key field, so value should not be in required_non_null
    assert "value" not in contract.table_schema.required_non_null


def test_populate_contract_with_dedupe_inference() -> None:
    """Infer dedupe spec from key fields."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.utf8(), nullable=True),
        ]
    )

    options = ContractPopulationOptions(infer_dedupe=True)
    contract = populate_contract_from_schema("test_table", schema, options=options)

    assert contract.dedupe is not None
    assert "id" in contract.dedupe.keys


def test_populate_contract_with_canonical_sort() -> None:
    """Generate canonical sort from key fields."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.utf8(), nullable=True),
        ]
    )

    options = ContractPopulationOptions(include_canonical_sort=True)
    contract = populate_contract_from_schema("test_table", schema, options=options)

    assert contract.canonical_sort
    assert contract.canonical_sort[0].column == "id"
    assert contract.canonical_sort[0].order == "ascending"


def test_populate_contract_custom_key_patterns() -> None:
    """Use custom key field patterns."""
    schema = pa.schema(
        [
            pa.field("record_code", pa.utf8(), nullable=False),
            pa.field("name", pa.utf8(), nullable=True),
        ]
    )

    options = ContractPopulationOptions(key_field_suffixes=("_code",))
    contract = populate_contract_from_schema("test_table", schema, options=options)

    assert "record_code" in contract.table_schema.key_fields


def test_populate_contract_result_provenance() -> None:
    """Verify detailed result contains provenance information."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("value", pa.float64(), nullable=False),
        ]
    )

    result = populate_contract_from_schema_detailed("test_table", schema)

    assert isinstance(result, ContractPopulationResult)
    assert result.inferred_key_fields == ("id",)
    assert "value" in result.inferred_required


def test_populate_contract_default_version() -> None:
    """Use custom default version."""
    schema = pa.schema([pa.field("id", pa.int64())])
    expected_version = 5

    options = ContractPopulationOptions(default_version=expected_version)
    contract = populate_contract_from_schema("test_table", schema, options=options)

    assert contract.version == expected_version


def test_populate_contract_preserves_field_metadata() -> None:
    """Preserve field metadata in the contract."""
    field_meta = {b"description": b"Primary key"}
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), metadata=field_meta),
        ]
    )

    contract = populate_contract_from_schema("test_table", schema)

    field_spec = contract.table_schema.fields[0]
    assert field_spec.metadata.get("description") == "Primary key"


def test_populate_contract_dictionary_encoding() -> None:
    """Detect dictionary encoding hint from metadata."""
    field_meta = {b"encoding": b"dictionary"}
    schema = pa.schema(
        [
            pa.field("category", pa.utf8(), metadata=field_meta),
        ]
    )

    contract = populate_contract_from_schema("test_table", schema)

    field_spec = contract.table_schema.fields[0]
    assert field_spec.encoding == "dictionary"


def test_populate_for_tables_creates_catalog() -> None:
    """Create a contract catalog from multiple schemas."""
    schema_a = pa.schema([pa.field("id", pa.int64())])
    schema_b = pa.schema([pa.field("key", pa.utf8())])

    contract_a = populate_contract_from_schema("table_a", schema_a)
    contract_b = populate_contract_from_schema("table_b", schema_b)

    catalog = ContractCatalogSpec(contracts={"table_a": contract_a, "table_b": contract_b})

    assert "table_a" in catalog.contracts
    assert "table_b" in catalog.contracts


def test_populate_for_tables_with_options() -> None:
    """Apply options consistently across tables."""
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
        ]
    )
    expected_version = 10

    options = ContractPopulationOptions(default_version=expected_version)
    contract = populate_contract_from_schema("test", schema, options=options)

    assert contract.version == expected_version


def test_populate_contract_empty_schema() -> None:
    """Handle empty schema gracefully."""
    schema = pa.schema([])

    contract = populate_contract_from_schema("empty_table", schema)

    assert contract.name == "empty_table"
    assert len(contract.table_schema.fields) == 0
    assert contract.table_schema.key_fields == ()
    assert contract.table_schema.required_non_null == ()


def test_populate_contract_all_nullable_fields() -> None:
    """Handle schema with all nullable fields."""
    schema = pa.schema(
        [
            pa.field("a", pa.int64(), nullable=True),
            pa.field("b", pa.utf8(), nullable=True),
        ]
    )

    contract = populate_contract_from_schema("nullable_table", schema)

    # No required_non_null should be inferred
    assert contract.table_schema.required_non_null == ()
