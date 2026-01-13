"""Schema spec tables for normalize datasets."""

from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING

import pyarrow as pa

from arrowdsl.spec.tables.schema import SchemaSpecTables, schema_spec_tables_from_dataset_specs
from normalize.registry_specs import dataset_specs
from normalize.rule_registry import (
    normalize_rule_definitions,
    rule_definition_table_cached,
    rule_family_spec_table_cached,
)

if TYPE_CHECKING:
    from normalize.rule_definitions import NormalizeRuleDefinition

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


@cache
def rule_definition_table() -> pa.Table:
    """Return the normalize rule definition spec table.

    Returns
    -------
    pa.Table
        Arrow table of normalize rule definitions.
    """
    return rule_definition_table_cached()


@cache
def normalize_rule_spec_rows() -> tuple[NormalizeRuleDefinition, ...]:
    """Return normalize rule definition rows in registry order.

    Returns
    -------
    tuple[NormalizeRuleDefinition, ...]
        Normalize rule definition rows.
    """
    return normalize_rule_definitions()


__all__ = [
    "CONSTRAINTS_TABLE",
    "CONTRACT_TABLE",
    "FIELD_TABLE",
    "SCHEMA_TABLES",
    "normalize_rule_spec_rows",
    "rule_definition_table",
    "rule_family_spec_table",
]
