"""Relationship dataset specs and contract catalog builders."""

from __future__ import annotations

from functools import cache

from datafusion_engine.schema_authority import dataset_spec_from_context
from schema_spec.system import (
    ContractCatalogSpec,
    DatasetSpec,
    DedupeSpecSpec,
    SchemaRegistry,
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
    registry = SchemaRegistry()
    relationship_contract_spec().register_into(registry)
    return tuple(sorted(registry.dataset_specs.values(), key=lambda spec: spec.name))


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


__all__ = [
    "RELATIONSHIP_SCHEMA_VERSION",
    "rel_callsite_qname_spec",
    "rel_callsite_symbol_spec",
    "rel_def_symbol_spec",
    "rel_import_symbol_spec",
    "rel_name_symbol_spec",
    "relationship_contract_spec",
    "relationship_dataset_specs",
]
