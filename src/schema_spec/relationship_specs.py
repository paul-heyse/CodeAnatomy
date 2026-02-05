"""Relationship dataset specs and contract catalog builders.

This module provides data-driven generation of relationship dataset specs
and contract catalogs. Each relationship is defined by minimal unique data,
with all other properties derived from standard patterns.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from functools import cache
from typing import TYPE_CHECKING

from datafusion import SQLOptions

from datafusion_engine.schema.introspection import table_constraint_rows
from datafusion_engine.session.runtime import SessionRuntime, dataset_spec_from_context
from schema_spec.dataset_spec_ops import dataset_spec_name
from schema_spec.system import (
    ContractCatalogSpec,
    ContractSpec,
    DatasetSpec,
    DedupeSpecSpec,
    SortKeySpec,
    VirtualFieldSpec,
    make_contract_spec,
)
from serde_msgspec import StructBaseStrict

if TYPE_CHECKING:
    from datafusion import SessionContext

RELATIONSHIP_SCHEMA_VERSION: int = 1

SpecAccessor = Callable[["SessionContext | None"], DatasetSpec]


# =============================================================================
# DATA: Minimal unique information per relationship
# =============================================================================


class RelationshipData(StructBaseStrict, frozen=True):
    """Minimal data defining a relationship - everything else is derived.

    Attributes
    ----------
    name
        Short relationship name without version suffix (e.g., "rel_name_symbol").
    table_name
        Full table name with version suffix (e.g., "rel_name_symbol").
    entity_id_col
        Primary entity identifier column (e.g., "entity_id").
    dedupe_keys
        Complete dedupe key tuple (overrides standard pattern if provided).
    extra_sort_keys
        Additional canonical sort keys beyond the standard pattern.
    """

    name: str
    table_name: str
    entity_id_col: str
    dedupe_keys: tuple[str, ...] | None = None
    extra_sort_keys: tuple[str, ...] = ()


# Registry of relationship data - only unique values specified
RELATIONSHIP_DATA: tuple[RelationshipData, ...] = (
    RelationshipData(
        name="rel_name_symbol",
        table_name="rel_name_symbol",
        entity_id_col="entity_id",
    ),
    RelationshipData(
        name="rel_import_symbol",
        table_name="rel_import_symbol",
        entity_id_col="entity_id",
    ),
    RelationshipData(
        name="rel_def_symbol",
        table_name="rel_def_symbol",
        entity_id_col="entity_id",
    ),
    RelationshipData(
        name="rel_callsite_symbol",
        table_name="rel_callsite_symbol",
        entity_id_col="entity_id",
    ),
)

# Lookup by table name for contract generation
_RELATIONSHIP_DATA_BY_TABLE: dict[str, RelationshipData] = {
    data.table_name: data for data in RELATIONSHIP_DATA
}


# =============================================================================
# STANDARD PATTERNS: Shared across all relationships
# =============================================================================

# Standard tie-breakers (identical for ALL relationships)
STANDARD_RELATIONSHIP_TIE_BREAKERS: tuple[SortKeySpec, ...] = (
    SortKeySpec(column="score", order="descending"),
    SortKeySpec(column="confidence", order="descending"),
)

# Standard virtual field (identical for ALL relationships)
_STANDARD_VIRTUAL_FIELD = VirtualFieldSpec(fields=("origin",))


# =============================================================================
# GENERATORS: Derive contract entries from minimal data
# =============================================================================


def _dedupe_keys_for_relationship(data: RelationshipData) -> tuple[str, ...]:
    """Return dedupe keys for a relationship.

    Standard pattern: (entity_id_col, "symbol", "path", "bstart", "bend")
    Can be overridden via dedupe_keys attribute.

    Parameters
    ----------
    data
        Relationship data with entity_id_col and optional override keys.

    Returns
    -------
    tuple[str, ...]
        Complete dedupe key tuple.
    """
    if data.dedupe_keys is not None:
        return data.dedupe_keys
    return (data.entity_id_col, "symbol", "path", "bstart", "bend")


def _canonical_sort_for_relationship(data: RelationshipData) -> tuple[SortKeySpec, ...]:
    """Return canonical sort keys for a relationship.

    Standard pattern: path ASC, bstart ASC, entity_id_col ASC, + extras ASC

    Parameters
    ----------
    data
        Relationship data with entity_id_col and optional extra sort keys.

    Returns
    -------
    tuple[SortKeySpec, ...]
        Complete canonical sort specification.
    """
    base_sort = (
        SortKeySpec(column="path", order="ascending"),
        SortKeySpec(column="bstart", order="ascending"),
        SortKeySpec(column=data.entity_id_col, order="ascending"),
    )
    extra_sort = tuple(SortKeySpec(column=col, order="ascending") for col in data.extra_sort_keys)
    return base_sort + extra_sort


def generate_relationship_contract_entry(
    data: RelationshipData,
    spec: DatasetSpec,
) -> ContractSpec:
    """Generate a ContractSpec from relationship data and dataset spec.

    Parameters
    ----------
    data
        Minimal relationship data defining unique properties.
    spec
        DatasetSpec resolved from context for schema information.

    Returns
    -------
    ContractSpec
        Complete contract specification for the relationship.
    """
    return make_contract_spec(
        table_spec=spec.table_spec,
        virtual=_STANDARD_VIRTUAL_FIELD,
        dedupe=DedupeSpecSpec(
            keys=_dedupe_keys_for_relationship(data),
            tie_breakers=STANDARD_RELATIONSHIP_TIE_BREAKERS,
            strategy="KEEP_FIRST_AFTER_SORT",
        ),
        canonical_sort=_canonical_sort_for_relationship(data),
        version=RELATIONSHIP_SCHEMA_VERSION,
    )


# =============================================================================
# CACHED SPEC ACCESSORS: Preserve @cache pattern for lazy resolution
# =============================================================================


@cache
def _rel_name_symbol_spec_cached() -> DatasetSpec:
    return dataset_spec_from_context("rel_name_symbol")


def rel_name_symbol_spec(ctx: SessionContext | None = None) -> DatasetSpec:
    """Return the dataset spec for name-to-symbol relationships.

    Parameters
    ----------
    ctx
        Optional SessionContext to resolve the schema.

    Returns
    -------
    DatasetSpec
        Dataset spec for name-to-symbol relationship rows.
    """
    if ctx is None:
        return _rel_name_symbol_spec_cached()
    return dataset_spec_from_context("rel_name_symbol", ctx=ctx)


@cache
def _rel_import_symbol_spec_cached() -> DatasetSpec:
    return dataset_spec_from_context("rel_import_symbol")


def rel_import_symbol_spec(ctx: SessionContext | None = None) -> DatasetSpec:
    """Return the dataset spec for import-to-symbol relationships.

    Parameters
    ----------
    ctx
        Optional SessionContext to resolve the schema.

    Returns
    -------
    DatasetSpec
        Dataset spec for import-to-symbol relationship rows.
    """
    if ctx is None:
        return _rel_import_symbol_spec_cached()
    return dataset_spec_from_context("rel_import_symbol", ctx=ctx)


@cache
def _rel_def_symbol_spec_cached() -> DatasetSpec:
    return dataset_spec_from_context("rel_def_symbol")


def rel_def_symbol_spec(ctx: SessionContext | None = None) -> DatasetSpec:
    """Return the dataset spec for definition-to-symbol relationships.

    Parameters
    ----------
    ctx
        Optional SessionContext to resolve the schema.

    Returns
    -------
    DatasetSpec
        Dataset spec for definition-to-symbol relationship rows.
    """
    if ctx is None:
        return _rel_def_symbol_spec_cached()
    return dataset_spec_from_context("rel_def_symbol", ctx=ctx)


@cache
def _rel_callsite_symbol_spec_cached() -> DatasetSpec:
    return dataset_spec_from_context("rel_callsite_symbol")


def rel_callsite_symbol_spec(ctx: SessionContext | None = None) -> DatasetSpec:
    """Return the dataset spec for callsite-to-symbol relationships.

    Parameters
    ----------
    ctx
        Optional SessionContext to resolve the schema.

    Returns
    -------
    DatasetSpec
        Dataset spec for callsite-to-symbol relationship rows.
    """
    if ctx is None:
        return _rel_callsite_symbol_spec_cached()
    return dataset_spec_from_context("rel_callsite_symbol", ctx=ctx)


@cache
def _relation_output_spec_cached() -> DatasetSpec:
    return dataset_spec_from_context("relation_output")


def relation_output_spec(ctx: SessionContext | None = None) -> DatasetSpec:
    """Return the dataset spec for canonical relationship outputs.

    Parameters
    ----------
    ctx
        Optional SessionContext to resolve the schema.

    Returns
    -------
    DatasetSpec
        Dataset spec for relation_output rows.
    """
    if ctx is None:
        return _relation_output_spec_cached()
    return dataset_spec_from_context("relation_output", ctx=ctx)


# =============================================================================
# AGGREGATE ACCESSORS
# =============================================================================

# Mapping of table names to spec accessor functions
_SPEC_ACCESSORS: dict[str, SpecAccessor] = {
    "rel_name_symbol": rel_name_symbol_spec,
    "rel_import_symbol": rel_import_symbol_spec,
    "rel_def_symbol": rel_def_symbol_spec,
    "rel_callsite_symbol": rel_callsite_symbol_spec,
}


def relationship_dataset_specs(ctx: SessionContext | None = None) -> tuple[DatasetSpec, ...]:
    """Return relationship dataset specs.

    Parameters
    ----------
    ctx
        Optional SessionContext to resolve the schema.

    Returns
    -------
    tuple[DatasetSpec, ...]
        Relationship dataset specs sorted by name.
    """
    specs = (
        rel_name_symbol_spec(ctx),
        rel_import_symbol_spec(ctx),
        rel_def_symbol_spec(ctx),
        rel_callsite_symbol_spec(ctx),
        relation_output_spec(ctx),
    )
    return tuple(sorted(specs, key=dataset_spec_name))


def relationship_contract_spec(ctx: SessionContext | None = None) -> ContractCatalogSpec:
    """Build the contract spec catalog for relationship datasets.

    Parameters
    ----------
    ctx
        Optional SessionContext to resolve the schema.

    Returns
    -------
    ContractCatalogSpec
        Contract catalog for relationship datasets.
    """
    # Generate contracts from data-driven definitions
    contracts: dict[str, ContractSpec] = {}
    for data in RELATIONSHIP_DATA:
        accessor = _SPEC_ACCESSORS.get(data.table_name)
        if accessor is None:
            continue
        spec = accessor(ctx)
        contracts[data.table_name] = generate_relationship_contract_entry(data, spec)

    return ContractCatalogSpec(contracts=contracts)


# =============================================================================
# CONSTRAINT VALIDATION
# =============================================================================


def _constraint_key_sets(rows: Sequence[Mapping[str, object]]) -> list[tuple[str, ...]]:
    """Extract constraint key sets from information_schema rows.

    Parameters
    ----------
    rows
        Rows from information_schema.key_column_usage query.

    Returns
    -------
    list[tuple[str, ...]]
        List of constraint key tuples.
    """
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


def _expected_dedupe_keys(ctx: SessionContext | None) -> dict[str, tuple[str, ...]]:
    """Return expected dedupe keys from contract catalog.

    Parameters
    ----------
    ctx
        Optional SessionContext to resolve the schema.

    Returns
    -------
    dict[str, tuple[str, ...]]
        Mapping of table names to expected dedupe key tuples.
    """
    contracts = relationship_contract_spec(ctx=ctx).contracts
    return {
        name: contract.dedupe.keys
        for name, contract in contracts.items()
        if contract.dedupe is not None and contract.dedupe.keys
    }


def relationship_constraint_errors(
    session_runtime: SessionRuntime,
    *,
    sql_options: SQLOptions | None = None,
) -> dict[str, object]:
    """Validate relationship dataset constraints via information_schema.

    Parameters
    ----------
    session_runtime
        Session runtime with DataFusion context.
    sql_options
        Optional SQL execution options.

    Returns
    -------
    dict[str, object]
        Mapping of dataset name to constraint error details.
    """
    try:
        expected = _expected_dedupe_keys(session_runtime.ctx)
    except (KeyError, RuntimeError, TypeError, ValueError):
        return {}
    if not expected:
        return {}
    errors: dict[str, object] = {}
    ctx = session_runtime.ctx
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
    "RELATIONSHIP_DATA",
    "RELATIONSHIP_SCHEMA_VERSION",
    "STANDARD_RELATIONSHIP_TIE_BREAKERS",
    "RelationshipData",
    "generate_relationship_contract_entry",
    "rel_callsite_symbol_spec",
    "rel_def_symbol_spec",
    "rel_import_symbol_spec",
    "rel_name_symbol_spec",
    "relation_output_spec",
    "relationship_constraint_errors",
    "relationship_contract_spec",
    "relationship_dataset_specs",
]
