"""Dataset row specifications for programmatic CPG schemas."""

from __future__ import annotations

from dataclasses import dataclass

from schema_spec.system import DedupeSpecSpec, SortKeySpec, TableSpecConstraints

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ContractRow:
    """Row configuration for a dataset contract."""

    dedupe: DedupeSpecSpec | None = None
    canonical_sort: tuple[SortKeySpec, ...] = ()
    constraints: tuple[str, ...] = ()
    version: int | None = None


@dataclass(frozen=True)
class DatasetRow:
    """Row specification for a programmatic CPG dataset."""

    name: str
    version: int
    bundles: tuple[str, ...]
    fields: tuple[str, ...]
    constraints: TableSpecConstraints | None = None
    contract: ContractRow | None = None
    template: str | None = None
    contract_name: str | None = None


DATASET_ROWS: tuple[DatasetRow, ...] = (
    DatasetRow(
        name="cpg_nodes_v1",
        version=SCHEMA_VERSION,
        bundles=("file_identity", "span"),
        fields=("node_id", "node_kind"),
        constraints=TableSpecConstraints(
            required_non_null=("node_id", "node_kind"),
            key_fields=("node_id", "node_kind"),
        ),
        contract=ContractRow(
            dedupe=DedupeSpecSpec(
                keys=("node_kind", "node_id"),
                tie_breakers=(
                    SortKeySpec(column="path", order="ascending"),
                    SortKeySpec(column="bstart", order="ascending"),
                    SortKeySpec(column="bend", order="ascending"),
                ),
                strategy="KEEP_FIRST_AFTER_SORT",
            ),
            canonical_sort=(
                SortKeySpec(column="node_kind", order="ascending"),
                SortKeySpec(column="path", order="ascending"),
                SortKeySpec(column="bstart", order="ascending"),
                SortKeySpec(column="bend", order="ascending"),
                SortKeySpec(column="node_id", order="ascending"),
            ),
            version=SCHEMA_VERSION,
        ),
        template="cpg",
    ),
    DatasetRow(
        name="cpg_edges_v1",
        version=SCHEMA_VERSION,
        bundles=("span",),
        fields=(
            "edge_id",
            "edge_kind",
            "src_node_id",
            "dst_node_id",
            "path",
            "edge_owner_file_id",
            "origin",
            "resolution_method",
            "confidence",
            "score",
            "symbol_roles",
            "qname_source",
            "ambiguity_group_id",
            "rule_name",
            "rule_priority",
        ),
        constraints=TableSpecConstraints(
            required_non_null=("edge_id", "edge_kind", "src_node_id", "dst_node_id"),
            key_fields=("edge_kind", "src_node_id", "dst_node_id", "path", "bstart", "bend"),
        ),
        contract=ContractRow(
            dedupe=DedupeSpecSpec(
                keys=("edge_kind", "src_node_id", "dst_node_id", "path", "bstart", "bend"),
                tie_breakers=(
                    SortKeySpec(column="score", order="descending"),
                    SortKeySpec(column="confidence", order="descending"),
                    SortKeySpec(column="rule_priority", order="ascending"),
                    SortKeySpec(column="edge_id", order="ascending"),
                ),
                strategy="KEEP_FIRST_AFTER_SORT",
            ),
            canonical_sort=(
                SortKeySpec(column="path", order="ascending"),
                SortKeySpec(column="bstart", order="ascending"),
                SortKeySpec(column="bend", order="ascending"),
                SortKeySpec(column="edge_kind", order="ascending"),
                SortKeySpec(column="src_node_id", order="ascending"),
                SortKeySpec(column="dst_node_id", order="ascending"),
                SortKeySpec(column="edge_id", order="ascending"),
            ),
            version=SCHEMA_VERSION,
        ),
        template="cpg",
    ),
    DatasetRow(
        name="cpg_props_v1",
        version=SCHEMA_VERSION,
        bundles=(),
        fields=(
            "entity_kind",
            "entity_id",
            "prop_key",
            "value_str",
            "value_int",
            "value_float",
            "value_bool",
            "value_json",
        ),
        constraints=TableSpecConstraints(
            required_non_null=("entity_kind", "entity_id", "prop_key"),
        ),
        contract=ContractRow(
            dedupe=DedupeSpecSpec(
                keys=(
                    "entity_kind",
                    "entity_id",
                    "prop_key",
                    "value_str",
                    "value_int",
                    "value_float",
                    "value_bool",
                    "value_json",
                ),
                tie_breakers=(SortKeySpec(column="prop_key", order="ascending"),),
                strategy="KEEP_FIRST_AFTER_SORT",
            ),
            canonical_sort=(
                SortKeySpec(column="entity_kind", order="ascending"),
                SortKeySpec(column="entity_id", order="ascending"),
                SortKeySpec(column="prop_key", order="ascending"),
            ),
            version=SCHEMA_VERSION,
        ),
        template="cpg",
    ),
    DatasetRow(
        name="cpg_props_json_v1",
        version=SCHEMA_VERSION,
        bundles=(),
        fields=(
            "entity_kind",
            "entity_id",
            "prop_key",
            "value_str",
            "value_int",
            "value_float",
            "value_bool",
            "value_json",
        ),
        constraints=TableSpecConstraints(
            required_non_null=("entity_kind", "entity_id", "prop_key"),
        ),
        contract=ContractRow(
            dedupe=DedupeSpecSpec(
                keys=(
                    "entity_kind",
                    "entity_id",
                    "prop_key",
                    "value_str",
                    "value_int",
                    "value_float",
                    "value_bool",
                    "value_json",
                ),
                tie_breakers=(SortKeySpec(column="prop_key", order="ascending"),),
                strategy="KEEP_FIRST_AFTER_SORT",
            ),
            canonical_sort=(
                SortKeySpec(column="entity_kind", order="ascending"),
                SortKeySpec(column="entity_id", order="ascending"),
                SortKeySpec(column="prop_key", order="ascending"),
            ),
            version=SCHEMA_VERSION,
        ),
        template="cpg",
    ),
    DatasetRow(
        name="cpg_props_by_file_id_v1",
        version=SCHEMA_VERSION,
        bundles=(),
        fields=(
            "file_id",
            "entity_kind",
            "entity_id",
            "prop_key",
            "value_str",
            "value_int",
            "value_float",
            "value_bool",
            "value_json",
        ),
        constraints=TableSpecConstraints(
            required_non_null=("file_id", "entity_kind", "entity_id", "prop_key"),
        ),
        contract=ContractRow(
            dedupe=DedupeSpecSpec(
                keys=(
                    "entity_kind",
                    "entity_id",
                    "prop_key",
                    "value_str",
                    "value_int",
                    "value_float",
                    "value_bool",
                    "value_json",
                ),
                tie_breakers=(SortKeySpec(column="prop_key", order="ascending"),),
                strategy="KEEP_FIRST_AFTER_SORT",
            ),
            canonical_sort=(
                SortKeySpec(column="entity_kind", order="ascending"),
                SortKeySpec(column="entity_id", order="ascending"),
                SortKeySpec(column="prop_key", order="ascending"),
            ),
            version=SCHEMA_VERSION,
        ),
        template="cpg",
    ),
    DatasetRow(
        name="cpg_props_global_v1",
        version=SCHEMA_VERSION,
        bundles=(),
        fields=(
            "entity_kind",
            "entity_id",
            "prop_key",
            "value_str",
            "value_int",
            "value_float",
            "value_bool",
            "value_json",
        ),
        constraints=TableSpecConstraints(
            required_non_null=("entity_kind", "entity_id", "prop_key"),
        ),
        contract=ContractRow(
            dedupe=DedupeSpecSpec(
                keys=(
                    "entity_kind",
                    "entity_id",
                    "prop_key",
                    "value_str",
                    "value_int",
                    "value_float",
                    "value_bool",
                    "value_json",
                ),
                tie_breakers=(SortKeySpec(column="prop_key", order="ascending"),),
                strategy="KEEP_FIRST_AFTER_SORT",
            ),
            canonical_sort=(
                SortKeySpec(column="entity_kind", order="ascending"),
                SortKeySpec(column="entity_id", order="ascending"),
                SortKeySpec(column="prop_key", order="ascending"),
            ),
            version=SCHEMA_VERSION,
        ),
        template="cpg",
    ),
)


__all__ = ["DATASET_ROWS", "SCHEMA_VERSION", "ContractRow", "DatasetRow"]
