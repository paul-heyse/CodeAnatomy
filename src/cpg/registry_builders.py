"""Programmatic builders for CPG dataset specs."""

from __future__ import annotations

from arrowdsl.schema.schema import SchemaMetadataSpec
from arrowdsl.spec.infra import DatasetRegistration, register_dataset
from cpg.registry_bundles import bundle
from cpg.registry_fields import field
from cpg.registry_rows import ContractRow, DatasetRow
from cpg.registry_templates import template
from schema_spec.system import (
    ContractSpec,
    DatasetSpec,
    TableSchemaSpec,
    make_contract_spec,
    make_table_spec,
)


def _metadata_spec(row: DatasetRow) -> SchemaMetadataSpec:
    templ = template(row.template) if row.template is not None else None
    if templ is None:
        return SchemaMetadataSpec()
    contract_name = row.contract_name or row.name
    return SchemaMetadataSpec(
        schema_metadata={
            b"cpg_stage": templ.stage.encode("utf-8"),
            b"cpg_dataset": row.name.encode("utf-8"),
            b"contract_name": contract_name.encode("utf-8"),
            b"determinism_tier": templ.determinism_tier.encode("utf-8"),
        }
    )


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
        bundles=(bundle(name) for name in row.bundles),
        fields=(field(name) for name in row.fields),
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
