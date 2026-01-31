"""Relationship contract spec generator for CPG relationship datasets.

This module provides data-driven generation of ContractSpec instances for
relationship datasets. All relationship contracts share common patterns
(virtual fields, tie-breakers, deduplication strategy) with only the
entity-specific columns varying.

The generator eliminates repetitive make_contract_spec() calls by defining
relationship metadata declaratively and generating contracts programmatically.
This module integrates with the relationship specs in cpg/relationship_specs.py
for a unified, data-driven relationship system.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from cpg.relationship_specs import (
    ALL_RELATIONSHIP_SPECS,
    QNameRelationshipSpec,
    RelationshipSpec,
)
from schema_spec.system import (
    ContractCatalogSpec,
    ContractSpec,
    DedupeSpecSpec,
    SortKeySpec,
    VirtualFieldSpec,
    make_contract_spec,
)

if TYPE_CHECKING:
    from schema_spec.specs import TableSchemaSpec

# Standard tie-breakers used by ALL relationship contracts.
# Order: prefer higher score, then higher confidence, then lower task priority.
STANDARD_RELATIONSHIP_TIE_BREAKERS: tuple[SortKeySpec, ...] = (
    SortKeySpec(column="score", order="descending"),
    SortKeySpec(column="confidence", order="descending"),
    SortKeySpec(column="task_priority", order="ascending"),
)

# Standard virtual fields for relationship contracts.
STANDARD_VIRTUAL_FIELDS: tuple[str, ...] = ("origin",)

# Standard deduplication strategy for relationship contracts.
STANDARD_DEDUPE_STRATEGY: Literal[
    "KEEP_FIRST_AFTER_SORT",
    "KEEP_BEST_BY_SCORE",
    "COLLAPSE_LIST",
    "KEEP_ARBITRARY",
] = "KEEP_FIRST_AFTER_SORT"

# Standard relationship columns included in all dedupe keys.
STANDARD_SYMBOL_DEDUPE_COLUMNS: tuple[str, ...] = (
    "symbol",
    "path",
    "bstart",
    "bend",
)

# Standard relationship columns for qname relationships.
STANDARD_QNAME_DEDUPE_COLUMNS: tuple[str, ...] = (
    "qname_id",
    "path",
    "bstart",
    "bend",
)

# Standard canonical sort prefix for path-anchored relationships.
STANDARD_CANONICAL_SORT_PREFIX: tuple[SortKeySpec, ...] = (
    SortKeySpec(column="path", order="ascending"),
    SortKeySpec(column="bstart", order="ascending"),
)

# Schema version for relationship contracts.
RELATIONSHIP_SCHEMA_VERSION: int = 1


@dataclass(frozen=True)
class RelationshipContractData:
    """Configuration data for generating a relationship contract.

    Attributes
    ----------
    table_name : str
        The table/contract name (e.g., "rel_name_symbol_v1").
    entity_id_cols : tuple[str, ...]
        Entity-specific ID columns that form the primary key prefix
        (e.g., ("ref_id",) or ("call_id", "qname_id")).
    dedupe_columns : tuple[str, ...]
        Standard dedupe columns following entity ID cols
        (e.g., symbol+path+bstart+bend or qname_id+path+bstart+bend).
    extra_dedupe_keys : tuple[str, ...]
        Additional columns beyond entity_id_cols and standard relationship
        columns for deduplication. Usually empty.
    custom_tie_breakers : tuple[SortKeySpec, ...] | None
        Override tie-breakers if the standard ones do not apply.
        When None, uses STANDARD_RELATIONSHIP_TIE_BREAKERS.
    custom_canonical_sort_suffix : tuple[SortKeySpec, ...] | None
        Override the canonical sort suffix (after path, bstart).
        When None, derives from entity_id_cols.
    version : int | None
        Override schema version. When None, uses RELATIONSHIP_SCHEMA_VERSION.
    """

    table_name: str
    entity_id_cols: tuple[str, ...]
    dedupe_columns: tuple[str, ...] = STANDARD_SYMBOL_DEDUPE_COLUMNS
    extra_dedupe_keys: tuple[str, ...] = ()
    custom_tie_breakers: tuple[SortKeySpec, ...] | None = None
    custom_canonical_sort_suffix: tuple[SortKeySpec, ...] | None = None
    version: int | None = None

    @property
    def dedupe_keys(self) -> tuple[str, ...]:
        """Return the complete dedupe key tuple for this relationship.

        Returns
        -------
        tuple[str, ...]
            Entity ID columns + standard relationship columns + extra keys.
        """
        return self.entity_id_cols + self.dedupe_columns + self.extra_dedupe_keys

    @property
    def tie_breakers(self) -> tuple[SortKeySpec, ...]:
        """Return the tie-breakers for this relationship.

        Returns
        -------
        tuple[SortKeySpec, ...]
            Custom tie-breakers if specified, otherwise standard.
        """
        if self.custom_tie_breakers is not None:
            return self.custom_tie_breakers
        return STANDARD_RELATIONSHIP_TIE_BREAKERS

    @property
    def canonical_sort(self) -> tuple[SortKeySpec, ...]:
        """Return the canonical sort order for this relationship.

        Returns
        -------
        tuple[SortKeySpec, ...]
            Path prefix + entity ID columns (or custom suffix).
        """
        if self.custom_canonical_sort_suffix is not None:
            return STANDARD_CANONICAL_SORT_PREFIX + self.custom_canonical_sort_suffix
        # Default: sort by entity ID columns after path/bstart
        suffix = tuple(SortKeySpec(column=col, order="ascending") for col in self.entity_id_cols)
        return STANDARD_CANONICAL_SORT_PREFIX + suffix

    @property
    def resolved_version(self) -> int:
        """Return the schema version for this relationship.

        Returns
        -------
        int
            Custom version if specified, otherwise RELATIONSHIP_SCHEMA_VERSION.
        """
        if self.version is not None:
            return self.version
        return RELATIONSHIP_SCHEMA_VERSION


def _contract_data_from_symbol_spec(spec: RelationshipSpec) -> RelationshipContractData:
    """Derive contract data from a symbol relationship spec.

    Parameters
    ----------
    spec
        Symbol relationship specification.

    Returns
    -------
    RelationshipContractData
        Contract data for the relationship.
    """
    entity_col = spec.entity_id_alias
    return RelationshipContractData(
        table_name=spec.output_view_name,
        entity_id_cols=(entity_col,),
        dedupe_columns=STANDARD_SYMBOL_DEDUPE_COLUMNS,
    )


def _contract_data_from_qname_spec(spec: QNameRelationshipSpec) -> RelationshipContractData:
    """Derive contract data from a qname relationship spec.

    Parameters
    ----------
    spec
        QName relationship specification.

    Returns
    -------
    RelationshipContractData
        Contract data for the relationship.
    """
    entity_col = spec.entity_id_alias
    return RelationshipContractData(
        table_name=spec.output_view_name,
        entity_id_cols=(entity_col, "qname_id"),
        dedupe_columns=STANDARD_QNAME_DEDUPE_COLUMNS,
    )


def contract_data_from_spec(
    spec: RelationshipSpec | QNameRelationshipSpec,
) -> RelationshipContractData:
    """Derive contract data from any relationship spec.

    Parameters
    ----------
    spec
        Relationship specification (symbol or qname).

    Returns
    -------
    RelationshipContractData
        Contract data for the relationship.

    Raises
    ------
    TypeError
        When an unknown spec type is provided.
    """
    if isinstance(spec, RelationshipSpec):
        return _contract_data_from_symbol_spec(spec)
    if isinstance(spec, QNameRelationshipSpec):
        return _contract_data_from_qname_spec(spec)
    msg = f"Unknown relationship spec type: {type(spec)!r}"
    raise TypeError(msg)


def generate_relationship_contract(
    data: RelationshipContractData,
    table_spec: TableSchemaSpec,
) -> ContractSpec:
    """Generate a ContractSpec from relationship contract data.

    Parameters
    ----------
    data
        Configuration data describing the relationship contract.
    table_spec
        The TableSchemaSpec for the relationship dataset.

    Returns
    -------
    ContractSpec
        Fully configured contract specification.
    """
    return make_contract_spec(
        table_spec=table_spec,
        virtual=VirtualFieldSpec(fields=STANDARD_VIRTUAL_FIELDS),
        dedupe=DedupeSpecSpec(
            keys=data.dedupe_keys,
            tie_breakers=data.tie_breakers,
            strategy=STANDARD_DEDUPE_STRATEGY,
        ),
        canonical_sort=data.canonical_sort,
        version=data.resolved_version,
    )


# Relationship contract data for all standard relationship datasets.
# The 5 relationship types map entity nodes to SCIP symbols.
RELATIONSHIP_CONTRACT_DATA: tuple[RelationshipContractData, ...] = (
    RelationshipContractData(
        table_name="rel_name_symbol_v1",
        entity_id_cols=("ref_id",),
        dedupe_columns=STANDARD_SYMBOL_DEDUPE_COLUMNS,
    ),
    RelationshipContractData(
        table_name="rel_import_symbol_v1",
        entity_id_cols=("import_alias_id",),
        dedupe_columns=STANDARD_SYMBOL_DEDUPE_COLUMNS,
    ),
    RelationshipContractData(
        table_name="rel_def_symbol_v1",
        entity_id_cols=("def_id",),
        dedupe_columns=STANDARD_SYMBOL_DEDUPE_COLUMNS,
    ),
    RelationshipContractData(
        table_name="rel_callsite_symbol_v1",
        entity_id_cols=("call_id",),
        dedupe_columns=STANDARD_SYMBOL_DEDUPE_COLUMNS,
    ),
    RelationshipContractData(
        table_name="rel_callsite_qname_v1",
        entity_id_cols=("call_id", "qname_id"),
        dedupe_columns=STANDARD_QNAME_DEDUPE_COLUMNS,
    ),
)


def derive_relationship_contract_data() -> tuple[RelationshipContractData, ...]:
    """Derive contract data from all registered relationship specs.

    Returns
    -------
    tuple[RelationshipContractData, ...]
        Contract data derived from ALL_RELATIONSHIP_SPECS.
    """
    return tuple(contract_data_from_spec(spec) for spec in ALL_RELATIONSHIP_SPECS)


def relationship_contract_data_by_name() -> Mapping[str, RelationshipContractData]:
    """Return relationship contract data indexed by table name.

    Returns
    -------
    Mapping[str, RelationshipContractData]
        Mapping of table name to contract data.
    """
    return {data.table_name: data for data in RELATIONSHIP_CONTRACT_DATA}


def generate_relationship_contract_catalog(
    table_spec_factory: Callable[[str], TableSchemaSpec],
) -> ContractCatalogSpec:
    """Generate a ContractCatalogSpec from relationship contract data.

    Parameters
    ----------
    table_spec_factory
        Callable that returns a TableSchemaSpec for a given table name.
        Typically this wraps dataset_spec_from_context() to resolve schemas
        from the DataFusion catalog.

    Returns
    -------
    ContractCatalogSpec
        Contract catalog containing all relationship contracts.
    """
    contracts: dict[str, ContractSpec] = {}
    for data in RELATIONSHIP_CONTRACT_DATA:
        table_spec = table_spec_factory(data.table_name)
        contracts[data.table_name] = generate_relationship_contract(data, table_spec)
    return ContractCatalogSpec(contracts=contracts)


def relationship_contract_catalog_from_specs(
    specs: Mapping[str, TableSchemaSpec],
) -> ContractCatalogSpec:
    """Generate a ContractCatalogSpec from pre-resolved table specs.

    Parameters
    ----------
    specs
        Mapping of table names to their TableSchemaSpec instances.
        Must contain all keys from RELATIONSHIP_CONTRACT_DATA table names.

    Returns
    -------
    ContractCatalogSpec
        Contract catalog containing all relationship contracts.
    """
    contracts: dict[str, ContractSpec] = {}
    for data in RELATIONSHIP_CONTRACT_DATA:
        table_spec = specs[data.table_name]
        contracts[data.table_name] = generate_relationship_contract(data, table_spec)
    return ContractCatalogSpec(contracts=contracts)


__all__ = [
    "RELATIONSHIP_CONTRACT_DATA",
    "RELATIONSHIP_SCHEMA_VERSION",
    "STANDARD_CANONICAL_SORT_PREFIX",
    "STANDARD_DEDUPE_STRATEGY",
    "STANDARD_QNAME_DEDUPE_COLUMNS",
    "STANDARD_RELATIONSHIP_TIE_BREAKERS",
    "STANDARD_SYMBOL_DEDUPE_COLUMNS",
    "STANDARD_VIRTUAL_FIELDS",
    "RelationshipContractData",
    "contract_data_from_spec",
    "derive_relationship_contract_data",
    "generate_relationship_contract",
    "generate_relationship_contract_catalog",
    "relationship_contract_catalog_from_specs",
    "relationship_contract_data_by_name",
]
