"""Schema spec tables for normalize datasets."""

from __future__ import annotations

from arrowdsl.spec.tables.schema import SchemaSpecTables, schema_spec_tables_from_dataset_specs
from normalize.registry_specs import dataset_specs

SCHEMA_TABLES: SchemaSpecTables = schema_spec_tables_from_dataset_specs(dataset_specs())
FIELD_TABLE = SCHEMA_TABLES.field_table
CONSTRAINTS_TABLE = SCHEMA_TABLES.constraints_table
CONTRACT_TABLE = SCHEMA_TABLES.contract_table

__all__ = [
    "CONSTRAINTS_TABLE",
    "CONTRACT_TABLE",
    "FIELD_TABLE",
    "SCHEMA_TABLES",
]
