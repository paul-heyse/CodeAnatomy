"""Arrow spec tables for normalize rule families."""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa

from arrowdsl.spec.codec import parse_string_tuple
from arrowdsl.spec.io import table_from_rows
from normalize.rule_specs import NormalizeRuleFamilySpec

RULE_FAMILY_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("factory", pa.string(), nullable=False),
        pa.field("inputs", pa.list_(pa.string()), nullable=True),
        pa.field("output", pa.string(), nullable=True),
        pa.field("confidence_policy", pa.string(), nullable=True),
        pa.field("ambiguity_policy", pa.string(), nullable=True),
        pa.field("option_flag", pa.string(), nullable=True),
        pa.field("execution_mode", pa.string(), nullable=True),
    ],
    metadata={b"spec_kind": b"normalize_rule_families"},
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
    return table_from_rows(RULE_FAMILY_SCHEMA, rows)


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
            output=row.get("output"),
            confidence_policy=row.get("confidence_policy"),
            ambiguity_policy=row.get("ambiguity_policy"),
            option_flag=row.get("option_flag"),
            execution_mode=row.get("execution_mode"),
        )
        for row in table.to_pylist()
    )


__all__ = [
    "normalize_rule_family_specs_from_table",
    "normalize_rule_family_table",
]
