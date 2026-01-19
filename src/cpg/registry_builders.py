"""Programmatic builders for CPG dataset specs."""

from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING

from arrowdsl.schema.schema import SchemaMetadataSpec
from arrowdsl.spec.infra import DatasetRegistration, register_dataset
from cpg.registry_readers import (
    bundle_catalog_from_table,
    field_catalog_from_table,
    registry_templates_from_table,
)
from cpg.registry_rows import ContractRow, DatasetRow
from cpg.registry_tables import (
    bundle_catalog_table,
    field_catalog_table,
    registry_templates_table,
)
from registry_common.registry_builders import build_contract_spec as shared_build_contract_spec
from schema_spec.system import make_table_spec

if TYPE_CHECKING:
    from cpg.registry_templates import RegistryTemplate
    from schema_spec.specs import ArrowFieldSpec, FieldBundle
    from schema_spec.system import ContractSpec, DatasetSpec, TableSchemaSpec


@cache
def _field_catalog() -> dict[str, ArrowFieldSpec]:
    return field_catalog_from_table(field_catalog_table())


@cache
def _bundle_catalog() -> dict[str, FieldBundle]:
    return bundle_catalog_from_table(
        bundle_catalog_table(),
        field_catalog=_field_catalog(),
    )


@cache
def _registry_templates() -> dict[str, RegistryTemplate]:
    return registry_templates_from_table(registry_templates_table())


def _metadata_spec(row: DatasetRow) -> SchemaMetadataSpec:
    templ = None if row.template is None else _registry_templates()[row.template]
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
    bundle_map = _bundle_catalog()
    field_map = _field_catalog()
    return make_table_spec(
        name=row.name,
        version=row.version,
        bundles=(bundle_map[name] for name in row.bundles),
        fields=(field_map[name] for name in row.fields),
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
    return shared_build_contract_spec(contract, table_spec=table_spec)


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
