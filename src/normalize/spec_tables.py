"""Schema spec tables for normalize datasets."""

from __future__ import annotations

from collections.abc import Sequence
from functools import cache

import pyarrow as pa

from arrowdsl.schema.build import iter_rows_from_table, list_view_type, table_from_rows
from arrowdsl.schema.schema import EncodingPolicy, EncodingSpec
from arrowdsl.spec.codec import parse_string_tuple
from arrowdsl.spec.tables.schema import SchemaSpecTables, schema_spec_tables_from_dataset_specs
from normalize.registry_specs import dataset_specs
from normalize.rule_registry_specs import rule_family_specs
from normalize.rule_specs import NormalizeRuleFamilySpec

RULE_FAMILY_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("factory", pa.string(), nullable=False),
        pa.field("inputs", list_view_type(pa.string()), nullable=True),
        pa.field("output", pa.string(), nullable=True),
        pa.field("confidence_policy", pa.string(), nullable=True),
        pa.field("ambiguity_policy", pa.string(), nullable=True),
        pa.field("option_flag", pa.string(), nullable=True),
        pa.field("execution_mode", pa.string(), nullable=True),
    ],
    metadata={b"spec_kind": b"normalize_rule_families"},
)

NORMALIZE_RULE_FAMILY_ENCODING = EncodingPolicy(
    specs=(
        EncodingSpec(column="name"),
        EncodingSpec(column="factory"),
        EncodingSpec(column="option_flag"),
    )
)


def normalize_rule_family_table(
    specs: Sequence[NormalizeRuleFamilySpec],
) -> pa.Table:
    """Build a normalize rule family spec table.

    Returns
    -------
    pa.Table
        Arrow table with normalize rule family specs.
    """
    rows = [
        {
            "name": spec.name,
            "factory": spec.factory,
            "inputs": list(spec.inputs) or None,
            "output": spec.output,
            "confidence_policy": spec.confidence_policy,
            "ambiguity_policy": spec.ambiguity_policy,
            "option_flag": spec.option_flag,
            "execution_mode": spec.execution_mode,
        }
        for spec in specs
    ]
    table = table_from_rows(RULE_FAMILY_SCHEMA, rows)
    return NORMALIZE_RULE_FAMILY_ENCODING.apply(table)


def normalize_rule_family_specs_from_table(
    table: pa.Table,
) -> tuple[NormalizeRuleFamilySpec, ...]:
    """Compile NormalizeRuleFamilySpec objects from a spec table.

    Returns
    -------
    tuple[NormalizeRuleFamilySpec, ...]
        Rule family specs parsed from the table.
    """
    return tuple(
        NormalizeRuleFamilySpec(
            name=str(row["name"]),
            factory=str(row["factory"]),
            inputs=parse_string_tuple(row.get("inputs"), label="inputs"),
            output=_coerce_optional_str(row.get("output")),
            confidence_policy=_coerce_optional_str(row.get("confidence_policy")),
            ambiguity_policy=_coerce_optional_str(row.get("ambiguity_policy")),
            option_flag=_coerce_optional_str(row.get("option_flag")),
            execution_mode=_coerce_optional_str(row.get("execution_mode")),
        )
        for row in iter_rows_from_table(table)
    )


def _coerce_optional_str(value: object | None) -> str | None:
    if value is None:
        return None
    return str(value)


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
    return normalize_rule_family_table(rule_family_specs())


__all__ = [
    "CONSTRAINTS_TABLE",
    "CONTRACT_TABLE",
    "FIELD_TABLE",
    "RULE_FAMILY_SCHEMA",
    "SCHEMA_TABLES",
    "normalize_rule_family_specs_from_table",
    "normalize_rule_family_table",
    "rule_family_spec_table",
]
