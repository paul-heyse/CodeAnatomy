"""Schema specification models and adapters."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from arrowdsl.spec.tables.schema import (
        CONTRACT_SPEC_SCHEMA,
        FIELD_SPEC_SCHEMA,
        TABLE_CONSTRAINTS_SCHEMA,
        SchemaSpecTables,
        contract_spec_table,
        contract_specs_from_table,
        dataset_specs_from_tables,
        field_spec_table,
        schema_spec_tables_from_dataset_specs,
        table_constraints_table,
        table_specs_from_tables,
    )
from schema_spec.specs import (
    DICT_STRING,
    ENCODING_DICTIONARY,
    ENCODING_META,
    KEY_FIELDS_META,
    PROVENANCE_COLS,
    PROVENANCE_SOURCE_FIELDS,
    REQUIRED_NON_NULL_META,
    SCHEMA_META_NAME,
    SCHEMA_META_VERSION,
    ArrowFieldSpec,
    DerivedFieldSpec,
    FieldBundle,
    NestedFieldSpec,
    TableSchemaSpec,
)
from schema_spec.system import (
    GLOBAL_SCHEMA_REGISTRY,
    ArrowValidationOptions,
    ContractCatalogSpec,
    ContractSpec,
    DatasetOpenSpec,
    DatasetSpec,
    DedupeSpecSpec,
    SchemaRegistry,
    SortKeySpec,
    TableSpecConstraints,
    ValidationPlans,
    VirtualFieldSpec,
    contract_catalog_spec_from_tables,
    dataset_spec_from_contract,
    dataset_spec_from_dataset,
    dataset_spec_from_path,
    dataset_spec_from_schema,
    make_contract_spec,
    make_dataset_spec,
    make_table_spec,
    table_spec_from_schema,
    validate_arrow_table,
)

_SCHEMA_TABLE_EXPORTS: set[str] = {
    "CONTRACT_SPEC_SCHEMA",
    "FIELD_SPEC_SCHEMA",
    "TABLE_CONSTRAINTS_SCHEMA",
    "SchemaSpecTables",
    "contract_spec_table",
    "contract_specs_from_table",
    "dataset_specs_from_tables",
    "field_spec_table",
    "schema_spec_tables_from_dataset_specs",
    "table_constraints_table",
    "table_specs_from_tables",
}


def __getattr__(name: str) -> object:
    if name in _SCHEMA_TABLE_EXPORTS:
        module = importlib.import_module("arrowdsl.spec.tables.schema")
        return getattr(module, name)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def __dir__() -> list[str]:
    return sorted(list(globals()) + list(_SCHEMA_TABLE_EXPORTS))


__all__ = [
    "CONTRACT_SPEC_SCHEMA",
    "DICT_STRING",
    "ENCODING_DICTIONARY",
    "ENCODING_META",
    "FIELD_SPEC_SCHEMA",
    "GLOBAL_SCHEMA_REGISTRY",
    "KEY_FIELDS_META",
    "PROVENANCE_COLS",
    "PROVENANCE_SOURCE_FIELDS",
    "REQUIRED_NON_NULL_META",
    "SCHEMA_META_NAME",
    "SCHEMA_META_VERSION",
    "TABLE_CONSTRAINTS_SCHEMA",
    "ArrowFieldSpec",
    "ArrowValidationOptions",
    "ContractCatalogSpec",
    "ContractSpec",
    "DatasetOpenSpec",
    "DatasetSpec",
    "DedupeSpecSpec",
    "DerivedFieldSpec",
    "FieldBundle",
    "NestedFieldSpec",
    "SchemaRegistry",
    "SchemaSpecTables",
    "SortKeySpec",
    "TableSchemaSpec",
    "TableSpecConstraints",
    "ValidationPlans",
    "VirtualFieldSpec",
    "contract_catalog_spec_from_tables",
    "contract_spec_table",
    "contract_specs_from_table",
    "dataset_spec_from_contract",
    "dataset_spec_from_dataset",
    "dataset_spec_from_path",
    "dataset_spec_from_schema",
    "dataset_specs_from_tables",
    "field_spec_table",
    "make_contract_spec",
    "make_dataset_spec",
    "make_table_spec",
    "schema_spec_tables_from_dataset_specs",
    "table_constraints_table",
    "table_spec_from_schema",
    "table_specs_from_tables",
    "validate_arrow_table",
]
