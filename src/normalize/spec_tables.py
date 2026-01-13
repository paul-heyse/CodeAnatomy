"""Schema spec tables for normalize datasets."""

from __future__ import annotations

from functools import cache

import pyarrow as pa

from arrowdsl.spec.tables.schema import SchemaSpecTables, schema_spec_tables_from_dataset_specs
from normalize.registry_specs import dataset_specs
from normalize.rule_registry import rule_family_spec_table_cached

SCHEMA_TABLES: SchemaSpecTables = schema_spec_tables_from_dataset_specs(dataset_specs())
FIELD_TABLE = SCHEMA_TABLES.field_table
CONSTRAINTS_TABLE = SCHEMA_TABLES.constraints_table
CONTRACT_TABLE = SCHEMA_TABLES.contract_table


@cache
def rule_family_spec_table() -> pa.Table:
    """Return the normalize rule family spec table.

    Returns
    -------
    pa.Table
        Arrow table of normalize rule family specs.
    """
    return rule_family_spec_table_cached()


__all__ = [
    "CONSTRAINTS_TABLE",
    "CONTRACT_TABLE",
    "FIELD_TABLE",
    "SCHEMA_TABLES",
    "rule_family_spec_table",
]
