"""Relationship dataset specs and contract catalog builders."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import cache

from datafusion import SessionContext, SQLOptions

from datafusion_engine.runtime import dataset_spec_from_context
from datafusion_engine.schema_introspection import table_constraint_rows
from schema_spec.system import (
    ContractCatalogSpec,
    DatasetSpec,
    DedupeSpecSpec,
    SortKeySpec,
    VirtualFieldSpec,
    make_contract_spec,
)

RELATIONSHIP_SCHEMA_VERSION: int = 1


@cache
def rel_name_symbol_spec() -> DatasetSpec:
    """Return the dataset spec for name-to-symbol relationships.

    Returns
    -------
    DatasetSpec
        Dataset spec for name-to-symbol relationship rows.
    """
    return dataset_spec_from_context("rel_name_symbol_v1")


@cache
def rel_import_symbol_spec() -> DatasetSpec:
    """Return the dataset spec for import-to-symbol relationships.

    Returns
    -------
    DatasetSpec
        Dataset spec for import-to-symbol relationship rows.
    """
    return dataset_spec_from_context("rel_import_symbol_v1")


@cache
def rel_def_symbol_spec() -> DatasetSpec:
    """Return the dataset spec for definition-to-symbol relationships.

    Returns
    -------
    DatasetSpec
        Dataset spec for definition-to-symbol relationship rows.
    """
    return dataset_spec_from_context("rel_def_symbol_v1")


@cache
def rel_callsite_symbol_spec() -> DatasetSpec:
    """Return the dataset spec for callsite-to-symbol relationships.

    Returns
    -------
    DatasetSpec
        Dataset spec for callsite-to-symbol relationship rows.
    """
    return dataset_spec_from_context("rel_callsite_symbol_v1")


@cache
def rel_callsite_qname_spec() -> DatasetSpec:
    """Return the dataset spec for callsite-to-qname relationships.

    Returns
    -------
    DatasetSpec
        Dataset spec for callsite-to-qname relationship rows.
    """
    return dataset_spec_from_context("rel_callsite_qname_v1")


def relationship_dataset_specs() -> tuple[DatasetSpec, ...]:
    """Return relationship dataset specs.

    Returns
    -------
    tuple[DatasetSpec, ...]
        Relationship dataset specs sorted by name.
    """
    specs = (
        rel_name_symbol_spec(),
        rel_import_symbol_spec(),
        rel_def_symbol_spec(),
        rel_callsite_symbol_spec(),
        rel_callsite_qname_spec(),
    )
    return tuple(sorted(specs, key=lambda spec: spec.name))


def relationship_contract_spec() -> ContractCatalogSpec:
    """Build the contract spec catalog for relationship datasets.

    Returns
    -------
    ContractCatalogSpec
        Contract catalog for relationship datasets.
    """
    rel_name_symbol = rel_name_symbol_spec()
    rel_import_symbol = rel_import_symbol_spec()
    rel_def_symbol = rel_def_symbol_spec()
    rel_callsite_symbol = rel_callsite_symbol_spec()
    rel_callsite_qname = rel_callsite_qname_spec()
    return ContractCatalogSpec(
        contracts={
            "rel_name_symbol_v1": make_contract_spec(
                table_spec=rel_name_symbol.table_spec,
                virtual=VirtualFieldSpec(fields=("origin",)),
                dedupe=DedupeSpecSpec(
                    keys=("ref_id", "symbol", "path", "bstart", "bend"),
                    tie_breakers=(
                        SortKeySpec(column="score", order="descending"),
                        SortKeySpec(column="confidence", order="descending"),
                        SortKeySpec(column="rule_priority", order="ascending"),
                    ),
                    strategy="KEEP_FIRST_AFTER_SORT",
                ),
                canonical_sort=(
                    SortKeySpec(column="path", order="ascending"),
                    SortKeySpec(column="bstart", order="ascending"),
                    SortKeySpec(column="ref_id", order="ascending"),
                ),
                version=RELATIONSHIP_SCHEMA_VERSION,
            ),
            "rel_import_symbol_v1": make_contract_spec(
                table_spec=rel_import_symbol.table_spec,
                virtual=VirtualFieldSpec(fields=("origin",)),
                dedupe=DedupeSpecSpec(
                    keys=("import_alias_id", "symbol", "path", "bstart", "bend"),
                    tie_breakers=(
                        SortKeySpec(column="score", order="descending"),
                        SortKeySpec(column="confidence", order="descending"),
                        SortKeySpec(column="rule_priority", order="ascending"),
                    ),
                    strategy="KEEP_FIRST_AFTER_SORT",
                ),
                canonical_sort=(
                    SortKeySpec(column="path", order="ascending"),
                    SortKeySpec(column="bstart", order="ascending"),
                    SortKeySpec(column="import_alias_id", order="ascending"),
                ),
                version=RELATIONSHIP_SCHEMA_VERSION,
            ),
            "rel_def_symbol_v1": make_contract_spec(
                table_spec=rel_def_symbol.table_spec,
                virtual=VirtualFieldSpec(fields=("origin",)),
                dedupe=DedupeSpecSpec(
                    keys=("def_id", "symbol", "path", "bstart", "bend"),
                    tie_breakers=(
                        SortKeySpec(column="score", order="descending"),
                        SortKeySpec(column="confidence", order="descending"),
                        SortKeySpec(column="rule_priority", order="ascending"),
                    ),
                    strategy="KEEP_FIRST_AFTER_SORT",
                ),
                canonical_sort=(
                    SortKeySpec(column="path", order="ascending"),
                    SortKeySpec(column="bstart", order="ascending"),
                    SortKeySpec(column="def_id", order="ascending"),
                ),
                version=RELATIONSHIP_SCHEMA_VERSION,
            ),
            "rel_callsite_symbol_v1": make_contract_spec(
                table_spec=rel_callsite_symbol.table_spec,
                virtual=VirtualFieldSpec(fields=("origin",)),
                dedupe=DedupeSpecSpec(
                    keys=("call_id", "symbol", "path", "bstart", "bend"),
                    tie_breakers=(
                        SortKeySpec(column="score", order="descending"),
                        SortKeySpec(column="confidence", order="descending"),
                        SortKeySpec(column="rule_priority", order="ascending"),
                    ),
                    strategy="KEEP_FIRST_AFTER_SORT",
                ),
                canonical_sort=(
                    SortKeySpec(column="path", order="ascending"),
                    SortKeySpec(column="bstart", order="ascending"),
                    SortKeySpec(column="call_id", order="ascending"),
                ),
                version=RELATIONSHIP_SCHEMA_VERSION,
            ),
            "rel_callsite_qname_v1": make_contract_spec(
                table_spec=rel_callsite_qname.table_spec,
                virtual=VirtualFieldSpec(fields=("origin",)),
                dedupe=DedupeSpecSpec(
                    keys=("call_id", "qname_id", "path", "bstart", "bend"),
                    tie_breakers=(
                        SortKeySpec(column="score", order="descending"),
                        SortKeySpec(column="confidence", order="descending"),
                        SortKeySpec(column="rule_priority", order="ascending"),
                    ),
                    strategy="KEEP_FIRST_AFTER_SORT",
                ),
                canonical_sort=(
                    SortKeySpec(column="path", order="ascending"),
                    SortKeySpec(column="bstart", order="ascending"),
                    SortKeySpec(column="call_id", order="ascending"),
                    SortKeySpec(column="qname_id", order="ascending"),
                ),
                version=RELATIONSHIP_SCHEMA_VERSION,
            ),
        }
    )


def _constraint_key_sets(rows: Sequence[Mapping[str, object]]) -> list[tuple[str, ...]]:
    constraints: dict[str, list[tuple[int, str]]] = {}
    for row in rows:
        constraint_type = row.get("constraint_type")
        if not isinstance(constraint_type, str):
            continue
        if constraint_type.upper() not in {"PRIMARY KEY", "UNIQUE"}:
            continue
        constraint_name = row.get("constraint_name")
        column_name = row.get("column_name")
        if not isinstance(constraint_name, str) or not constraint_name:
            continue
        if not isinstance(column_name, str) or not column_name:
            continue
        ordinal = row.get("ordinal_position")
        position = int(ordinal) if isinstance(ordinal, (int, float)) else 0
        constraints.setdefault(constraint_name, []).append((position, column_name))
    return [
        tuple(name for _, name in sorted(columns, key=lambda item: item[0]))
        for _, columns in sorted(constraints.items(), key=lambda item: item[0])
    ]


def _expected_dedupe_keys() -> dict[str, tuple[str, ...]]:
    contracts = relationship_contract_spec().contracts
    return {
        name: contract.dedupe.keys
        for name, contract in contracts.items()
        if contract.dedupe is not None and contract.dedupe.keys
    }


def relationship_constraint_errors(
    ctx: SessionContext,
    *,
    sql_options: SQLOptions | None = None,
) -> dict[str, object]:
    """Validate relationship dataset constraints via information_schema.

    Returns
    -------
    dict[str, object]
        Mapping of dataset name to constraint error details.
    """
    expected = _expected_dedupe_keys()
    if not expected:
        return {}
    errors: dict[str, object] = {}
    for name, keys in expected.items():
        try:
            rows = table_constraint_rows(
                ctx,
                table_name=name,
                sql_options=sql_options,
            )
        except (RuntimeError, TypeError, ValueError) as exc:
            errors[name] = {"error": str(exc), "expected_keys": list(keys)}
            continue
        observed_sets = _constraint_key_sets(rows)
        if not observed_sets:
            errors[name] = {"expected_keys": list(keys), "observed_keys": []}
            continue
        expected_set = set(keys)
        if any(set(observed) == expected_set for observed in observed_sets):
            continue
        errors[name] = {
            "expected_keys": list(keys),
            "observed_keys": [list(observed) for observed in observed_sets],
        }
    return errors


__all__ = [
    "RELATIONSHIP_SCHEMA_VERSION",
    "rel_callsite_qname_spec",
    "rel_callsite_symbol_spec",
    "rel_def_symbol_spec",
    "rel_import_symbol_spec",
    "rel_name_symbol_spec",
    "relationship_constraint_errors",
    "relationship_contract_spec",
    "relationship_dataset_specs",
]
