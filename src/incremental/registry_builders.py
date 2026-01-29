"""Programmatic builders for incremental dataset specs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from datafusion_engine.arrow_schema.metadata import SchemaMetadataSpec
from incremental.registry_rows import DatasetRow
from schema_spec.contract_row import ContractRow
from schema_spec.registration import DatasetRegistration, register_dataset
from schema_spec.system import make_contract_spec, make_table_spec

if TYPE_CHECKING:
    from schema_spec.system import ContractSpec, DatasetSpec, TableSchemaSpec


def _metadata_spec(row: DatasetRow) -> SchemaMetadataSpec:
    meta = {
        b"incremental_stage": b"incremental",
        b"incremental_dataset": row.name.encode("utf-8"),
    }
    meta.update(row.metadata_extra)
    return SchemaMetadataSpec(schema_metadata=meta)


def build_table_spec(row: DatasetRow) -> TableSchemaSpec:
    """Build the TableSchemaSpec for a dataset row.

    Returns
    -------
    TableSchemaSpec
        Table schema specification for the dataset row.
    """
    return make_table_spec(
        name=row.name,
        version=row.version,
        bundles=(),
        fields=row.fields,
        constraints=row.constraints,
    )


def build_contract_spec(
    contract: ContractRow | None,
    *,
    table_spec: TableSchemaSpec,
) -> ContractSpec | None:
    """Build the ContractSpec for a dataset row.

    Returns
    -------
    ContractSpec | None
        Contract spec or ``None`` when no contract row is provided.
    """
    if contract is None:
        return None
    return make_contract_spec(
        table_spec=table_spec,
        dedupe=contract.dedupe,
        canonical_sort=contract.canonical_sort,
        constraints=contract.constraints,
        version=contract.version,
    )


def build_dataset_spec(row: DatasetRow) -> DatasetSpec:
    """Build the DatasetSpec for a dataset row.

    Returns
    -------
    DatasetSpec
        Dataset specification for the row.
    """
    table_spec = build_table_spec(row)
    contract_spec = build_contract_spec(row.contract, table_spec=table_spec)
    registration = DatasetRegistration(
        contract_spec=contract_spec,
        metadata_spec=_metadata_spec(row),
    )
    return register_dataset(table_spec=table_spec, registration=registration)


__all__ = ["build_contract_spec", "build_dataset_spec", "build_table_spec"]
