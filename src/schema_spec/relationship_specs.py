"""Relationship dataset specs and contract catalog builders."""

from __future__ import annotations

from functools import cache

import pyarrow as pa

from schema_spec.specs import ArrowFieldSpec, call_span_bundle, span_bundle
from schema_spec.system import (
    ContractCatalogSpec,
    DatasetSpec,
    DedupeSpecSpec,
    SchemaRegistry,
    SortKeySpec,
    TableSpecConstraints,
    VirtualFieldSpec,
    make_contract_spec,
    make_dataset_spec,
    make_table_spec,
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
    return make_dataset_spec(
        table_spec=make_table_spec(
            name="rel_name_symbol_v1",
            version=RELATIONSHIP_SCHEMA_VERSION,
            bundles=(span_bundle(),),
            fields=[
                ArrowFieldSpec(name="ref_id", dtype=pa.string()),
                ArrowFieldSpec(name="symbol", dtype=pa.string()),
                ArrowFieldSpec(name="symbol_roles", dtype=pa.int32()),
                ArrowFieldSpec(name="path", dtype=pa.string()),
                ArrowFieldSpec(name="edge_owner_file_id", dtype=pa.string()),
                ArrowFieldSpec(name="resolution_method", dtype=pa.string()),
                ArrowFieldSpec(name="confidence", dtype=pa.float32()),
                ArrowFieldSpec(name="score", dtype=pa.float32()),
                ArrowFieldSpec(name="rule_name", dtype=pa.string()),
                ArrowFieldSpec(name="rule_priority", dtype=pa.int32()),
            ],
            constraints=TableSpecConstraints(required_non_null=("ref_id", "symbol")),
        )
    )


@cache
def rel_import_symbol_spec() -> DatasetSpec:
    """Return the dataset spec for import-to-symbol relationships.

    Returns
    -------
    DatasetSpec
        Dataset spec for import-to-symbol relationship rows.
    """
    return make_dataset_spec(
        table_spec=make_table_spec(
            name="rel_import_symbol_v1",
            version=RELATIONSHIP_SCHEMA_VERSION,
            bundles=(span_bundle(),),
            fields=[
                ArrowFieldSpec(name="import_alias_id", dtype=pa.string()),
                ArrowFieldSpec(name="symbol", dtype=pa.string()),
                ArrowFieldSpec(name="symbol_roles", dtype=pa.int32()),
                ArrowFieldSpec(name="path", dtype=pa.string()),
                ArrowFieldSpec(name="edge_owner_file_id", dtype=pa.string()),
                ArrowFieldSpec(name="resolution_method", dtype=pa.string()),
                ArrowFieldSpec(name="confidence", dtype=pa.float32()),
                ArrowFieldSpec(name="score", dtype=pa.float32()),
                ArrowFieldSpec(name="rule_name", dtype=pa.string()),
                ArrowFieldSpec(name="rule_priority", dtype=pa.int32()),
            ],
            constraints=TableSpecConstraints(required_non_null=("import_alias_id", "symbol")),
        )
    )


@cache
def rel_def_symbol_spec() -> DatasetSpec:
    """Return the dataset spec for definition-to-symbol relationships.

    Returns
    -------
    DatasetSpec
        Dataset spec for definition-to-symbol relationship rows.
    """
    return make_dataset_spec(
        table_spec=make_table_spec(
            name="rel_def_symbol_v1",
            version=RELATIONSHIP_SCHEMA_VERSION,
            bundles=(span_bundle(),),
            fields=[
                ArrowFieldSpec(name="def_id", dtype=pa.string()),
                ArrowFieldSpec(name="symbol", dtype=pa.string()),
                ArrowFieldSpec(name="symbol_roles", dtype=pa.int32()),
                ArrowFieldSpec(name="path", dtype=pa.string()),
                ArrowFieldSpec(name="edge_owner_file_id", dtype=pa.string()),
                ArrowFieldSpec(name="resolution_method", dtype=pa.string()),
                ArrowFieldSpec(name="confidence", dtype=pa.float32()),
                ArrowFieldSpec(name="score", dtype=pa.float32()),
                ArrowFieldSpec(name="rule_name", dtype=pa.string()),
                ArrowFieldSpec(name="rule_priority", dtype=pa.int32()),
            ],
            constraints=TableSpecConstraints(required_non_null=("def_id", "symbol")),
        )
    )


@cache
def rel_callsite_symbol_spec() -> DatasetSpec:
    """Return the dataset spec for callsite-to-symbol relationships.

    Returns
    -------
    DatasetSpec
        Dataset spec for callsite-to-symbol relationship rows.
    """
    return make_dataset_spec(
        table_spec=make_table_spec(
            name="rel_callsite_symbol_v1",
            version=RELATIONSHIP_SCHEMA_VERSION,
            bundles=(),
            fields=[
                ArrowFieldSpec(name="call_id", dtype=pa.string()),
                ArrowFieldSpec(name="symbol", dtype=pa.string()),
                ArrowFieldSpec(name="symbol_roles", dtype=pa.int32()),
                ArrowFieldSpec(name="path", dtype=pa.string()),
                ArrowFieldSpec(name="edge_owner_file_id", dtype=pa.string()),
                *call_span_bundle().fields,
                ArrowFieldSpec(name="resolution_method", dtype=pa.string()),
                ArrowFieldSpec(name="confidence", dtype=pa.float32()),
                ArrowFieldSpec(name="score", dtype=pa.float32()),
                ArrowFieldSpec(name="rule_name", dtype=pa.string()),
                ArrowFieldSpec(name="rule_priority", dtype=pa.int32()),
            ],
            constraints=TableSpecConstraints(required_non_null=("call_id", "symbol")),
        )
    )


@cache
def rel_callsite_qname_spec() -> DatasetSpec:
    """Return the dataset spec for callsite-to-qname relationships.

    Returns
    -------
    DatasetSpec
        Dataset spec for callsite-to-qname relationship rows.
    """
    return make_dataset_spec(
        table_spec=make_table_spec(
            name="rel_callsite_qname_v1",
            version=RELATIONSHIP_SCHEMA_VERSION,
            bundles=(),
            fields=[
                ArrowFieldSpec(name="call_id", dtype=pa.string()),
                ArrowFieldSpec(name="qname_id", dtype=pa.string()),
                ArrowFieldSpec(name="qname_source", dtype=pa.string()),
                ArrowFieldSpec(name="path", dtype=pa.string()),
                ArrowFieldSpec(name="edge_owner_file_id", dtype=pa.string()),
                *call_span_bundle().fields,
                ArrowFieldSpec(name="confidence", dtype=pa.float32()),
                ArrowFieldSpec(name="score", dtype=pa.float32()),
                ArrowFieldSpec(name="ambiguity_group_id", dtype=pa.string()),
                ArrowFieldSpec(name="rule_name", dtype=pa.string()),
                ArrowFieldSpec(name="rule_priority", dtype=pa.int32()),
            ],
            constraints=TableSpecConstraints(required_non_null=("call_id", "qname_id")),
        )
    )


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
