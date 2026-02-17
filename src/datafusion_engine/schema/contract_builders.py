"""Schema-contract builders and shared conversion helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, cast

import pyarrow as pa

from schema_spec.arrow_types import arrow_type_from_pyarrow
from schema_spec.dataset_spec import ContractSpec, DatasetSpec, TableSchemaContract
from schema_spec.field_spec import FieldSpec

if TYPE_CHECKING:
    from datafusion_engine.schema.contracts import EvolutionPolicy, SchemaContract


def decode_field_metadata(metadata: Mapping[bytes, bytes] | None) -> dict[str, str]:
    """Decode Arrow field metadata bytes into UTF-8 text pairs.

    Returns:
        dict[str, str]: Decoded metadata key/value pairs.
    """
    if not metadata:
        return {}
    return {
        key.decode("utf-8", errors="replace"): value.decode("utf-8", errors="replace")
        for key, value in metadata.items()
    }


def field_spec_from_arrow_field(field: pa.Field) -> FieldSpec:
    """Convert an Arrow field into a schema-spec field contract.

    Returns:
        FieldSpec: Field contract with normalized dtype and metadata.
    """
    metadata = decode_field_metadata(field.metadata)
    encoding_value = metadata.get("encoding")
    encoding = "dictionary" if encoding_value == "dictionary" else None
    return FieldSpec(
        name=field.name,
        dtype=arrow_type_from_pyarrow(field.type),
        nullable=field.nullable,
        metadata=metadata,
        default_value=metadata.get("default_value"),
        encoding=encoding,
    )


def _should_enforce_columns(spec: DatasetSpec) -> bool:
    if spec.view_specs:
        return False
    return spec.query_spec is None


def schema_contract_from_table_schema_contract(
    *,
    table_name: str,
    contract: TableSchemaContract,
    evolution_policy: EvolutionPolicy | None = None,
    enforce_columns: bool = True,
) -> SchemaContract:
    """Build a ``SchemaContract`` from a ``TableSchemaContract``.

    Returns:
        SchemaContract: Schema contract populated from table schema details.
    """
    from datafusion_engine.schema.contracts import EvolutionPolicy, SchemaContract

    policy = EvolutionPolicy.STRICT if evolution_policy is None else evolution_policy
    columns = tuple(field_spec_from_arrow_field(field) for field in contract.file_schema)
    partition_cols = tuple(name for name, _dtype in contract.partition_cols)
    if contract.partition_cols:
        partition_fields = tuple(
            FieldSpec(
                name=name,
                dtype=arrow_type_from_pyarrow(dtype),
                nullable=False,
            )
            for name, dtype in contract.partition_cols
        )
        columns = (*columns, *partition_fields)
    schema_metadata = dict(contract.file_schema.metadata or {})
    return SchemaContract(
        table_name=table_name,
        columns=columns,
        partition_cols=partition_cols,
        evolution_policy=policy,
        schema_metadata=schema_metadata,
        enforce_columns=enforce_columns,
    )


def schema_contract_from_dataset_spec(
    *,
    name: str,
    spec: DatasetSpec,
    evolution_policy: EvolutionPolicy | None = None,
    enforce_columns: bool | None = None,
) -> SchemaContract:
    """Build a ``SchemaContract`` from a ``DatasetSpec``.

    Returns:
        SchemaContract: Schema contract derived from dataset specification metadata.
    """
    from datafusion_engine.schema.contracts import EvolutionPolicy
    from schema_spec.dataset_spec import dataset_spec_datafusion_scan, dataset_spec_schema

    policy = EvolutionPolicy.STRICT if evolution_policy is None else evolution_policy
    table_schema = cast("pa.Schema", dataset_spec_schema(spec))
    partition_cols = ()
    datafusion_scan = dataset_spec_datafusion_scan(spec)
    if datafusion_scan is not None:
        partition_cols = datafusion_scan.partition_cols
    table_contract = TableSchemaContract(
        file_schema=table_schema,
        partition_cols=partition_cols,
    )
    resolved_enforce = _should_enforce_columns(spec) if enforce_columns is None else enforce_columns
    return schema_contract_from_table_schema_contract(
        table_name=name,
        contract=table_contract,
        evolution_policy=policy,
        enforce_columns=resolved_enforce,
    )


def schema_contract_from_contract_spec(
    *,
    name: str,
    spec: ContractSpec,
    evolution_policy: EvolutionPolicy | None = None,
    enforce_columns: bool = True,
) -> SchemaContract:
    """Build a ``SchemaContract`` from a static ``ContractSpec``.

    Returns:
        SchemaContract: Schema contract derived from a static contract spec.
    """
    from datafusion_engine.schema.contracts import EvolutionPolicy

    policy = EvolutionPolicy.STRICT if evolution_policy is None else evolution_policy
    table_schema = spec.table_schema.to_arrow_schema()
    table_contract = TableSchemaContract(file_schema=table_schema, partition_cols=())
    return schema_contract_from_table_schema_contract(
        table_name=name,
        contract=table_contract,
        evolution_policy=policy,
        enforce_columns=enforce_columns,
    )


__all__ = [
    "decode_field_metadata",
    "field_spec_from_arrow_field",
    "schema_contract_from_contract_spec",
    "schema_contract_from_dataset_spec",
    "schema_contract_from_table_schema_contract",
]
