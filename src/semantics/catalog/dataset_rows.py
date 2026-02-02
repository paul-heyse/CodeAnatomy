"""Semantic dataset rows describing schemas, derivations, and operational metadata.

This module provides a unified catalog of semantic datasets with metadata for:
- Schema versioning and field definitions
- Category classification (semantic, analysis, diagnostic)
- CDF (Change Data Feed) support for incremental processing
- Partition and merge key specifications for Delta Lake operations

The catalog consolidates datasets from normalize layer and semantic layer
into a single registry with consistent operational metadata.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, Literal, overload

DatasetCategory = Literal["semantic", "analysis", "diagnostic"]

if TYPE_CHECKING:
    from semantics.registry import SemanticModel

# Schema version for semantic dataset rows
SEMANTIC_SCHEMA_VERSION: Final[int] = 1

# Normalize schema version (mirrors normalize.dataset_rows.SCHEMA_VERSION)
_NORMALIZE_SCHEMA_VERSION: Final[int] = 1


def _bundle_field_names(bundles: Sequence[str]) -> set[str]:
    from semantics.catalog.spec_builder import _bundle

    field_names: set[str] = set()
    for bundle_name in bundles:
        try:
            bundle = _bundle(bundle_name)
        except KeyError:
            continue
        field_names.update(field.name for field in bundle.fields)
    return field_names


def _drop_bundle_fields(fields: Sequence[str], bundles: Sequence[str]) -> tuple[str, ...]:
    excluded = _bundle_field_names(bundles)
    if not excluded:
        return tuple(fields)
    return tuple(name for name in fields if name not in excluded)


@dataclass(frozen=True)
class SemanticDatasetRow:
    """Row spec describing a semantic dataset with operational metadata.

    Extends the normalize DatasetRow pattern with additional metadata
    for semantic pipeline operations including CDF support, partition
    configuration, and merge key specifications.

    Attributes
    ----------
    name
        Unique dataset name (canonical output name with version suffix).
    version
        Schema version for forward compatibility.
    bundles
        Field bundles to include (e.g., "file_identity", "span").
    fields
        Explicit field names in output schema.
    category
        Dataset category for operational classification.
    supports_cdf
        Whether the dataset supports Delta Lake Change Data Feed.
    partition_cols
        Column names for Delta Lake partitioning.
    merge_keys
        Column names for Delta Lake merge operations, or None if merge
        is not supported.
    join_keys
        Primary key columns for join operations.
    template
        Template name for schema generation.
    view_builder
        Name of the view builder function.
    kind
        Semantic output kind (table, scalar, artifact).
    semantic_id
        Stable semantic identifier for the dataset.
    entity
        Semantic entity type for the dataset rows.
    grain
        Row-level grain for the dataset.
    stability
        Stability marker for the dataset contract.
    schema_ref
        Logical schema reference name.
    materialization
        External materialization type (delta, parquet, etc.).
    materialized_name
        External materialized object name.
    metadata_extra
        Additional schema metadata (bytes -> bytes mapping).
    register_view
        Whether to register as a DataFusion view.
    source_dataset
        Original source dataset name when this is a normalization.
    """

    name: str
    version: int
    bundles: tuple[str, ...]
    fields: tuple[str, ...]
    category: DatasetCategory
    supports_cdf: bool = True
    partition_cols: tuple[str, ...] = ()
    merge_keys: tuple[str, ...] | None = None
    join_keys: tuple[str, ...] = ()
    template: str | None = None
    view_builder: str | None = None
    kind: str = "table"
    semantic_id: str | None = None
    entity: str | None = None
    grain: str | None = None
    stability: str | None = None
    schema_ref: str | None = None
    materialization: str | None = None
    materialized_name: str | None = None
    metadata_extra: dict[bytes, bytes] = field(default_factory=dict)
    register_view: bool = True
    source_dataset: str | None = None


# -----------------------------------------------------------------------------
# Normalize Layer Dataset Rows
# -----------------------------------------------------------------------------
# These mirror the definitions in normalize.dataset_rows.DATASET_ROWS
# but are defined here to avoid circular import issues.

_NORMALIZE_DATASET_ROWS: Final[tuple[SemanticDatasetRow, ...]] = (
    # Core semantic datasets
    SemanticDatasetRow(
        name="normalize_evidence_v1",
        version=_NORMALIZE_SCHEMA_VERSION,
        bundles=("file_identity", "span"),
        fields=(
            "span_id",
            "evidence_family",
            "source",
            "role",
            "confidence",
            "ambiguity_group_id",
            "task_name",
        ),
        category="semantic",
        supports_cdf=True,
        join_keys=("span_id", "task_name"),
        template="normalize_evidence",
        register_view=False,
    ),
    SemanticDatasetRow(
        name="type_exprs_norm_v1",
        version=_NORMALIZE_SCHEMA_VERSION,
        bundles=("file_identity", "span"),
        fields=(
            "type_expr_id",
            "owner_def_id",
            "param_name",
            "expr_kind",
            "expr_role",
            "expr_text",
            "type_repr",
            "type_id",
        ),
        category="semantic",
        supports_cdf=True,
        join_keys=("type_expr_id",),
        template="normalize_cst",
        view_builder="type_exprs_df_builder",
    ),
    SemanticDatasetRow(
        name="type_nodes_v1",
        version=_NORMALIZE_SCHEMA_VERSION,
        bundles=(),
        fields=("type_id", "type_repr", "type_form", "origin"),
        category="semantic",
        supports_cdf=True,
        join_keys=("type_id",),
        template="normalize_type",
        view_builder="type_nodes_df_builder",
    ),
    # Analysis datasets (bytecode/CFG)
    SemanticDatasetRow(
        name="py_bc_blocks_norm_v1",
        version=_NORMALIZE_SCHEMA_VERSION,
        bundles=("file_identity", "span"),
        fields=("block_id", "code_unit_id", "start_offset", "end_offset", "kind"),
        category="analysis",
        supports_cdf=True,
        partition_cols=("path",),
        join_keys=("code_unit_id", "block_id"),
        template="normalize_bytecode",
        view_builder="cfg_blocks_df_builder",
    ),
    SemanticDatasetRow(
        name="py_bc_cfg_edges_norm_v1",
        version=_NORMALIZE_SCHEMA_VERSION,
        bundles=("file_identity",),
        fields=(
            "edge_id",
            "code_unit_id",
            "src_block_id",
            "dst_block_id",
            "kind",
            "cond_instr_id",
            "exc_index",
        ),
        category="analysis",
        supports_cdf=True,
        partition_cols=("path",),
        join_keys=("code_unit_id", "edge_id"),
        template="normalize_bytecode",
        view_builder="cfg_edges_df_builder",
    ),
    SemanticDatasetRow(
        name="py_bc_def_use_events_v1",
        version=_NORMALIZE_SCHEMA_VERSION,
        bundles=("file_identity", "span"),
        fields=("event_id", "instr_id", "code_unit_id", "kind", "symbol", "opname", "offset"),
        category="analysis",
        supports_cdf=True,
        partition_cols=("path",),
        join_keys=("code_unit_id", "event_id"),
        template="normalize_bytecode",
        view_builder="def_use_events_df_builder",
    ),
    SemanticDatasetRow(
        name="py_bc_reaches_v1",
        version=_NORMALIZE_SCHEMA_VERSION,
        bundles=("file_identity",),
        fields=("edge_id", "code_unit_id", "def_event_id", "use_event_id", "symbol"),
        category="analysis",
        supports_cdf=True,
        partition_cols=("path",),
        join_keys=("code_unit_id", "symbol", "def_event_id", "use_event_id"),
        template="normalize_bytecode",
        view_builder="reaching_defs_df_builder",
    ),
    # Diagnostic datasets
    SemanticDatasetRow(
        name="span_errors_v1",
        version=_NORMALIZE_SCHEMA_VERSION,
        bundles=(),
        fields=("document_id", "path", "reason"),
        category="diagnostic",
        supports_cdf=False,  # Error snapshots don't track changes
        template="normalize_span",
        view_builder="span_errors_df_builder",
    ),
    SemanticDatasetRow(
        name="diagnostics_norm_v1",
        version=_NORMALIZE_SCHEMA_VERSION,
        bundles=("file_identity", "span"),
        fields=(
            "diag_id",
            "severity",
            "message",
            "diag_source",
            "code",
            "details",
        ),
        category="diagnostic",
        supports_cdf=True,  # Diagnostics can be incremental
        join_keys=("diag_id",),
        template="normalize_diagnostics",
        view_builder="diagnostics_df_builder",
    ),
)


# -----------------------------------------------------------------------------
# Semantic Layer Dataset Definitions
# -----------------------------------------------------------------------------
# Semantic normalization outputs and relationship outputs


def _build_semantic_normalization_rows(
    model: SemanticModel,
) -> tuple[SemanticDatasetRow, ...]:
    """Build semantic rows from semantic normalization specs.

    Returns
    -------
    tuple[SemanticDatasetRow, ...]
        Semantic dataset rows for CST normalization outputs.
    """
    return tuple(
        SemanticDatasetRow(
            name=spec.output_name,
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=("file_identity", "span"),
            fields=(spec.spec.entity_id.out_col,),
            category="semantic",
            supports_cdf=True,
            partition_cols=(),
            merge_keys=(spec.spec.entity_id.out_col,),
            join_keys=(spec.spec.entity_id.out_col,),
            template="semantic_normalize",
            view_builder=f"{spec.source_table}_norm_df_builder",
            register_view=True,
            source_dataset=spec.source_table,
        )
        for spec in model.normalization_specs
    )


def _build_scip_normalization_row() -> SemanticDatasetRow:
    """Build semantic row for SCIP occurrences normalization.

    Returns
    -------
    SemanticDatasetRow
        Semantic dataset row for SCIP normalization output.
    """
    # Lazy import to avoid circular dependencies
    from semantics.naming import canonical_output_name

    return SemanticDatasetRow(
        name=canonical_output_name("scip_occurrences_norm"),
        version=SEMANTIC_SCHEMA_VERSION,
        bundles=("file_identity", "span"),
        fields=("symbol", "role", "is_definition", "is_read", "is_import"),
        category="semantic",
        supports_cdf=True,
        partition_cols=(),
        merge_keys=("path", "bstart", "bend", "symbol"),
        join_keys=("path", "bstart", "bend"),
        template="semantic_scip",
        view_builder="scip_occurrences_norm_df_builder",
        register_view=True,
        source_dataset="scip_occurrences",
    )


def _build_relationship_rows(
    model: SemanticModel,
) -> tuple[SemanticDatasetRow, ...]:
    """Build semantic rows from relationship specs.

    Returns
    -------
    tuple[SemanticDatasetRow, ...]
        Semantic dataset rows for relationship outputs.
    """
    # Lazy import to avoid circular dependencies
    from relspec.view_defs import (
        DEFAULT_REL_TASK_PRIORITY,
        REL_CALLSITE_SYMBOL_OUTPUT,
        REL_DEF_SYMBOL_OUTPUT,
        REL_IMPORT_SYMBOL_OUTPUT,
        REL_NAME_SYMBOL_OUTPUT,
    )
    from semantics.catalog.projections import (
        SemanticProjectionConfig,
        semantic_projection_options,
    )

    projection_options = semantic_projection_options(
        SemanticProjectionConfig(
            default_priority=DEFAULT_REL_TASK_PRIORITY,
            rel_name_output=REL_NAME_SYMBOL_OUTPUT,
            rel_import_output=REL_IMPORT_SYMBOL_OUTPUT,
            rel_def_output=REL_DEF_SYMBOL_OUTPUT,
            rel_call_output=REL_CALLSITE_SYMBOL_OUTPUT,
            relationship_specs=model.relationship_specs,
        )
    )
    feature_fields = tuple(
        f"feat_{name}"
        for name in sorted(
            {feature.name for spec in model.relationship_specs for feature in spec.signals.features}
        )
    )
    max_hard = max((len(spec.signals.hard) for spec in model.relationship_specs), default=0)
    hard_fields = tuple(f"hard_{index}" for index in range(1, max_hard + 1))

    def _relationship_fields(entity_id_alias: str) -> tuple[str, ...]:
        return (
            entity_id_alias,
            "symbol",
            "symbol_roles",
            "path",
            "edge_owner_file_id",
            "bstart",
            "bend",
            "resolution_method",
            "confidence",
            "score",
            "task_name",
            "task_priority",
            *hard_fields,
            *feature_fields,
        )

    rows: list[SemanticDatasetRow] = []
    for spec in model.relationship_specs:
        options = projection_options[spec.name]
        join_keys = (options.entity_id_alias, "symbol")
        rows.append(
            SemanticDatasetRow(
                name=spec.name,
                version=SEMANTIC_SCHEMA_VERSION,
                bundles=(),
                fields=_relationship_fields(options.entity_id_alias),
                category="semantic",
                supports_cdf=True,
                partition_cols=(),
                merge_keys=join_keys,
                join_keys=join_keys,
                template="semantic_relationship",
                view_builder=f"{spec.name.replace('_v1', '')}_df_builder",
                register_view=True,
                source_dataset=None,
            )
        )
    return tuple(rows)


def _build_file_quality_row() -> SemanticDatasetRow:
    """Build the semantic row for file_quality view.

    Returns
    -------
    SemanticDatasetRow
        Semantic dataset row for file quality signals.
    """
    fields = (
        "file_id",
        "file_sha256",
        "has_cst_parse_errors",
        "cst_error_count",
        "ts_timed_out",
        "ts_error_count",
        "ts_missing_count",
        "ts_match_limit_exceeded",
        "has_scip_diagnostics",
        "scip_diagnostic_count",
        "scip_encoding_unspecified",
        "file_quality_score",
    )
    return SemanticDatasetRow(
        name="file_quality_v1",
        version=SEMANTIC_SCHEMA_VERSION,
        bundles=(),
        fields=fields,
        category="analysis",
        supports_cdf=True,
        partition_cols=(),
        merge_keys=("file_id",),
        join_keys=("file_id",),
        template="file_quality",
        view_builder="file_quality_df_builder",
        register_view=True,
        source_dataset=None,
    )


def _build_diagnostic_rows() -> tuple[SemanticDatasetRow, ...]:
    """Build semantic rows for diagnostic quality views.

    Returns
    -------
    tuple[SemanticDatasetRow, ...]
        Semantic dataset rows for diagnostic reports.
    """
    from semantics.registry import RELATIONSHIP_SPECS

    feature_fields = tuple(
        f"feat_{name}"
        for name in sorted(
            {feature.name for spec in RELATIONSHIP_SPECS for feature in spec.signals.features}
        )
    )
    max_hard = max((len(spec.signals.hard) for spec in RELATIONSHIP_SPECS), default=0)
    hard_fields = tuple(f"hard_{index}" for index in range(1, max_hard + 1))
    return (
        SemanticDatasetRow(
            name="relationship_quality_metrics_v1",
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=(),
            fields=(
                "relationship_name",
                "total_edges",
                "distinct_sources",
                "distinct_targets",
                "avg_confidence",
                "min_confidence",
                "max_confidence",
                "low_confidence_edges",
                "avg_score",
                "min_score",
                "max_score",
            ),
            category="diagnostic",
            supports_cdf=False,
            partition_cols=(),
            merge_keys=("relationship_name",),
            join_keys=("relationship_name",),
            template="relationship_quality_metrics",
            view_builder="relationship_quality_metrics_df_builder",
            register_view=True,
            source_dataset=None,
        ),
        SemanticDatasetRow(
            name="relationship_ambiguity_report_v1",
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=(),
            fields=(
                "relationship_name",
                "total_sources",
                "ambiguous_sources",
                "max_candidates",
                "avg_candidates",
                "ambiguity_rate",
            ),
            category="diagnostic",
            supports_cdf=False,
            partition_cols=(),
            merge_keys=("relationship_name",),
            join_keys=("relationship_name",),
            template="relationship_ambiguity_report",
            view_builder="relationship_ambiguity_report_df_builder",
            register_view=True,
            source_dataset=None,
        ),
        SemanticDatasetRow(
            name="relationship_candidates_v1",
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=(),
            fields=(
                "relationship_name",
                "src",
                "dst",
                "path",
                "bstart",
                "bend",
                "confidence",
                "score",
                "ambiguity_group_id",
                "task_name",
                "task_priority",
                "edge_kind",
                *hard_fields,
                *feature_fields,
            ),
            category="diagnostic",
            supports_cdf=False,
            partition_cols=(),
            merge_keys=("relationship_name", "src", "dst"),
            join_keys=("relationship_name", "src", "dst"),
            template="relationship_candidates",
            view_builder="relationship_candidates_df_builder",
            register_view=True,
            source_dataset=None,
        ),
        SemanticDatasetRow(
            name="relationship_decisions_v1",
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=(),
            fields=(
                "relationship_name",
                "src",
                "dst",
                "path",
                "bstart",
                "bend",
                "confidence",
                "score",
                "ambiguity_group_id",
                "task_name",
                "task_priority",
                "edge_kind",
                *hard_fields,
                *feature_fields,
            ),
            category="diagnostic",
            supports_cdf=False,
            partition_cols=(),
            merge_keys=("relationship_name", "src", "dst"),
            join_keys=("relationship_name", "src", "dst"),
            template="relationship_decisions",
            view_builder="relationship_decisions_df_builder",
            register_view=True,
            source_dataset=None,
        ),
        SemanticDatasetRow(
            name="schema_anomalies_v1",
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=(),
            fields=(
                "view_name",
                "violation_type",
                "column_name",
                "detail",
            ),
            category="diagnostic",
            supports_cdf=False,
            partition_cols=(),
            merge_keys=("view_name", "violation_type", "column_name"),
            join_keys=("view_name", "violation_type", "column_name"),
            template="schema_anomalies",
            view_builder="schema_anomalies_df_builder",
            register_view=True,
            source_dataset=None,
        ),
        SemanticDatasetRow(
            name="file_coverage_report_v1",
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=(),
            fields=(
                "file_id",
                "has_cst",
                "has_tree_sitter",
                "has_scip",
                "extraction_count",
            ),
            category="diagnostic",
            supports_cdf=False,
            partition_cols=(),
            merge_keys=("file_id",),
            join_keys=("file_id",),
            template="file_coverage_report",
            view_builder="file_coverage_report_df_builder",
            register_view=True,
            source_dataset=None,
        ),
    )


def _build_relation_output_row() -> SemanticDatasetRow:
    """Build the semantic row for relation_output union view.

    Returns
    -------
    SemanticDatasetRow
        Semantic dataset row for relation_output output.
    """
    from relspec.contracts import RELATION_OUTPUT_ORDERING_KEYS
    from relspec.view_defs import RELATION_OUTPUT_NAME

    join_keys = tuple(key for key, _order in RELATION_OUTPUT_ORDERING_KEYS)
    fields = (
        "src",
        "dst",
        "path",
        "edge_owner_file_id",
        "bstart",
        "bend",
        "origin",
        "resolution_method",
        "binding_kind",
        "def_site_kind",
        "use_kind",
        "kind",
        "reason",
        "confidence",
        "score",
        "symbol_roles",
        "qname_source",
        "ambiguity_group_id",
        "diag_source",
        "severity",
        "task_name",
        "task_priority",
    )

    return SemanticDatasetRow(
        name=RELATION_OUTPUT_NAME,
        version=SEMANTIC_SCHEMA_VERSION,
        bundles=(),
        fields=fields,
        category="semantic",
        supports_cdf=True,
        partition_cols=(),
        merge_keys=join_keys,
        join_keys=join_keys,
        template="relation_output",
        register_view=False,
        source_dataset=None,
    )


def _build_cpg_output_rows() -> tuple[SemanticDatasetRow, ...]:
    """Build semantic rows for final CPG outputs.

    Returns
    -------
    tuple[SemanticDatasetRow, ...]
        Semantic dataset rows for cpg_nodes and cpg_edges.
    """
    # Lazy import to avoid circular dependencies
    from cpg.emit_specs import _EDGE_OUTPUT_FIELDS, _NODE_OUTPUT_FIELDS, _PROP_OUTPUT_COLUMNS
    from semantics.naming import canonical_output_name

    nodes_name = canonical_output_name("cpg_nodes")
    edges_name = canonical_output_name("cpg_edges")
    props_name = canonical_output_name("cpg_props")
    nodes_quality_name = canonical_output_name("cpg_nodes_quality")
    props_quality_name = canonical_output_name("cpg_props_quality")
    props_map_name = canonical_output_name("cpg_props_map")
    edges_by_src_name = canonical_output_name("cpg_edges_by_src")
    edges_by_dst_name = canonical_output_name("cpg_edges_by_dst")

    node_fields = _drop_bundle_fields(_NODE_OUTPUT_FIELDS, ("file_identity", "span"))
    edge_fields = _drop_bundle_fields(_EDGE_OUTPUT_FIELDS, ("file_identity", "span"))
    prop_fields = _drop_bundle_fields(_PROP_OUTPUT_COLUMNS, ("file_identity",))

    nodes_quality_fields = (
        *node_fields,
        "cpg_nodes_path_depth",
        "cpg_nodes_span_length",
        "cpg_nodes_has_file_id",
        "cpg_nodes_has_task_name",
    )
    props_quality_fields = (
        *prop_fields,
        "cpg_props_value_present",
        "cpg_props_value_is_numeric",
        "cpg_props_key_length",
    )

    return (
        SemanticDatasetRow(
            name=nodes_name,
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=("file_identity", "span"),
            fields=node_fields,
            category="semantic",
            supports_cdf=True,
            partition_cols=(),
            merge_keys=("node_id",),
            join_keys=("node_id",),
            template="cpg_output",
            view_builder="cpg_nodes_df_builder",
            register_view=True,
            source_dataset=None,
            entity="node",
            grain="per_node",
            stability="design",
            materialization="delta",
            materialized_name=f"semantic.{nodes_name}",
        ),
        SemanticDatasetRow(
            name=edges_name,
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=("file_identity", "span"),
            fields=edge_fields,
            category="semantic",
            supports_cdf=True,
            partition_cols=(),
            merge_keys=("edge_id",),
            join_keys=("edge_id",),
            template="cpg_output",
            view_builder="cpg_edges_df_builder",
            register_view=True,
            source_dataset=None,
            entity="edge",
            grain="per_edge",
            stability="design",
            materialization="delta",
            materialized_name=f"semantic.{edges_name}",
        ),
        SemanticDatasetRow(
            name=props_name,
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=("file_identity",),
            fields=prop_fields,
            category="semantic",
            supports_cdf=True,
            partition_cols=(),
            merge_keys=("entity_kind", "entity_id", "prop_key"),
            join_keys=("entity_kind", "entity_id", "prop_key"),
            template="cpg_output",
            view_builder="cpg_props_df_builder",
            register_view=True,
            source_dataset=None,
            entity="prop",
            grain="per_prop",
            stability="design",
            materialization="delta",
            materialized_name=f"semantic.{props_name}",
        ),
        SemanticDatasetRow(
            name=nodes_quality_name,
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=("file_identity", "span"),
            fields=nodes_quality_fields,
            category="semantic",
            supports_cdf=True,
            partition_cols=(),
            merge_keys=("node_id",),
            join_keys=("node_id",),
            template="cpg_quality",
            view_builder=None,
            register_view=False,
            source_dataset=nodes_name,
            entity="node",
            grain="per_node",
            stability="design",
            materialization="delta",
            materialized_name=f"semantic.{nodes_quality_name}",
        ),
        SemanticDatasetRow(
            name=props_quality_name,
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=("file_identity",),
            fields=props_quality_fields,
            category="semantic",
            supports_cdf=True,
            partition_cols=(),
            merge_keys=("entity_kind", "entity_id", "prop_key"),
            join_keys=("entity_kind", "entity_id", "prop_key"),
            template="cpg_quality",
            view_builder=None,
            register_view=False,
            source_dataset=props_name,
            entity="prop",
            grain="per_prop",
            stability="design",
            materialization="delta",
            materialized_name=f"semantic.{props_quality_name}",
        ),
        SemanticDatasetRow(
            name=props_map_name,
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=(),
            fields=("entity_kind", "entity_id", "node_kind", "props"),
            category="semantic",
            supports_cdf=True,
            partition_cols=(),
            merge_keys=("entity_kind", "entity_id"),
            join_keys=("entity_kind", "entity_id"),
            template="cpg_adjacency",
            view_builder="cpg_props_map_df_builder",
            register_view=True,
            source_dataset=props_name,
            entity="prop",
            grain="per_entity",
            stability="design",
            materialization="delta",
            materialized_name=f"semantic.{props_map_name}",
        ),
        SemanticDatasetRow(
            name=edges_by_src_name,
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=(),
            fields=("src_node_id", "edges"),
            category="semantic",
            supports_cdf=True,
            partition_cols=(),
            merge_keys=("src_node_id",),
            join_keys=("src_node_id",),
            template="cpg_adjacency",
            view_builder="cpg_edges_by_src_df_builder",
            register_view=True,
            source_dataset=edges_name,
            entity="edge",
            grain="per_node",
            stability="design",
            materialization="delta",
            materialized_name=f"semantic.{edges_by_src_name}",
        ),
        SemanticDatasetRow(
            name=edges_by_dst_name,
            version=SEMANTIC_SCHEMA_VERSION,
            bundles=(),
            fields=("dst_node_id", "edges"),
            category="semantic",
            supports_cdf=True,
            partition_cols=(),
            merge_keys=("dst_node_id",),
            join_keys=("dst_node_id",),
            template="cpg_adjacency",
            view_builder="cpg_edges_by_dst_df_builder",
            register_view=True,
            source_dataset=edges_name,
            entity="edge",
            grain="per_node",
            stability="design",
            materialization="delta",
            materialized_name=f"semantic.{edges_by_dst_name}",
        ),
    )


# -----------------------------------------------------------------------------
# Combined Registry
# -----------------------------------------------------------------------------


def build_semantic_dataset_rows(model: SemanticModel) -> tuple[SemanticDatasetRow, ...]:
    """Build the complete semantic dataset row registry.

    Parameters
    ----------
    model
        Semantic model defining normalization and relationship specs.

    Returns
    -------
    tuple[SemanticDatasetRow, ...]
        All semantic dataset rows in dependency order.
    """
    rows: list[SemanticDatasetRow] = []
    # Normalize layer datasets (foundation)
    rows.extend(_NORMALIZE_DATASET_ROWS)
    # Semantic normalization outputs
    rows.extend(_build_semantic_normalization_rows(model))
    # SCIP normalization
    rows.append(_build_scip_normalization_row())
    # File quality signals (used by quality-aware relationships)
    rows.append(_build_file_quality_row())
    # Relationship outputs
    rows.extend(_build_relationship_rows(model))
    # Diagnostic quality reports
    rows.extend(_build_diagnostic_rows())
    # Relation output union
    rows.append(_build_relation_output_row())
    # Final CPG outputs
    rows.extend(_build_cpg_output_rows())
    return tuple(rows)


# Use a lazily-initialized cache for the dataset rows
_SEMANTIC_DATASET_ROWS_CACHE: tuple[SemanticDatasetRow, ...] | None = None
_ROWS_BY_NAME_CACHE: Mapping[str, SemanticDatasetRow] | None = None


def _get_semantic_dataset_rows() -> tuple[SemanticDatasetRow, ...]:
    """Return the cached semantic dataset rows, building if needed.

    Returns
    -------
    tuple[SemanticDatasetRow, ...]
        All semantic dataset rows in dependency order.
    """
    global _SEMANTIC_DATASET_ROWS_CACHE  # noqa: PLW0603
    if _SEMANTIC_DATASET_ROWS_CACHE is None:
        from semantics.ir_pipeline import build_semantic_ir

        _SEMANTIC_DATASET_ROWS_CACHE = build_semantic_ir().dataset_rows
    return _SEMANTIC_DATASET_ROWS_CACHE


def _get_rows_by_name() -> Mapping[str, SemanticDatasetRow]:
    """Return the indexed lookup mapping, building if needed.

    Returns
    -------
    Mapping[str, SemanticDatasetRow]
        Mapping from dataset name to row.
    """
    global _ROWS_BY_NAME_CACHE  # noqa: PLW0603
    if _ROWS_BY_NAME_CACHE is None:
        _ROWS_BY_NAME_CACHE = {row.name: row for row in _get_semantic_dataset_rows()}
    return _ROWS_BY_NAME_CACHE


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------


@overload
def dataset_row(name: str, *, strict: Literal[True]) -> SemanticDatasetRow: ...


@overload
def dataset_row(name: str, *, strict: Literal[False] = ...) -> SemanticDatasetRow | None: ...


def dataset_row(name: str, *, strict: bool = False) -> SemanticDatasetRow | None:
    """Return the semantic dataset row for a given name.

    Parameters
    ----------
    name
        Dataset name to look up.
    strict
        When True, raise KeyError if not found.

    Returns
    -------
    SemanticDatasetRow | None
        Dataset row when found, or None when not found and strict is False.

    Raises
    ------
    KeyError
        Raised when strict is True and dataset is not found.
    """
    rows_by_name = _get_rows_by_name()
    row = rows_by_name.get(name)
    if row is None and strict:
        msg = f"Dataset not found: {name}"
        raise KeyError(msg)
    return row


def dataset_rows(names: Sequence[str]) -> tuple[SemanticDatasetRow, ...]:
    """Return semantic dataset rows for the given names.

    Parameters
    ----------
    names
        Dataset names to look up.

    Returns
    -------
    tuple[SemanticDatasetRow, ...]
        Dataset rows in the order requested (skips missing names).
    """
    rows_by_name = _get_rows_by_name()
    return tuple(row for name in names if (row := rows_by_name.get(name)) is not None)


def get_all_dataset_rows() -> tuple[SemanticDatasetRow, ...]:
    """Return all semantic dataset rows.

    Returns
    -------
    tuple[SemanticDatasetRow, ...]
        All registered semantic dataset rows in dependency order.
    """
    return _get_semantic_dataset_rows()


def get_semantic_dataset_rows() -> tuple[SemanticDatasetRow, ...]:
    """Return semantic dataset rows with category 'semantic'.

    Returns
    -------
    tuple[SemanticDatasetRow, ...]
        Semantic category dataset rows.
    """
    return tuple(row for row in _get_semantic_dataset_rows() if row.category == "semantic")


def get_analysis_dataset_rows() -> tuple[SemanticDatasetRow, ...]:
    """Return semantic dataset rows with category 'analysis'.

    Returns
    -------
    tuple[SemanticDatasetRow, ...]
        Analysis category dataset rows.
    """
    return tuple(row for row in _get_semantic_dataset_rows() if row.category == "analysis")


def get_diagnostic_dataset_rows() -> tuple[SemanticDatasetRow, ...]:
    """Return semantic dataset rows with category 'diagnostic'.

    Returns
    -------
    tuple[SemanticDatasetRow, ...]
        Diagnostic category dataset rows.
    """
    return tuple(row for row in _get_semantic_dataset_rows() if row.category == "diagnostic")


def get_cdf_enabled_dataset_rows() -> tuple[SemanticDatasetRow, ...]:
    """Return semantic dataset rows that support CDF.

    Returns
    -------
    tuple[SemanticDatasetRow, ...]
        Dataset rows with supports_cdf=True.
    """
    return tuple(row for row in _get_semantic_dataset_rows() if row.supports_cdf)


def dataset_names() -> tuple[str, ...]:
    """Return all dataset names in registry order.

    Returns
    -------
    tuple[str, ...]
        Dataset names in dependency order.
    """
    return tuple(row.name for row in _get_semantic_dataset_rows())


def dataset_names_by_category(category: DatasetCategory) -> tuple[str, ...]:
    """Return dataset names for the given category.

    Parameters
    ----------
    category
        Dataset category to filter by.

    Returns
    -------
    tuple[str, ...]
        Dataset names in the category.
    """
    return tuple(row.name for row in _get_semantic_dataset_rows() if row.category == category)


__all__ = [
    "SEMANTIC_SCHEMA_VERSION",
    "DatasetCategory",
    "SemanticDatasetRow",
    "build_semantic_dataset_rows",
    "dataset_names",
    "dataset_names_by_category",
    "dataset_row",
    "dataset_rows",
    "get_all_dataset_rows",
    "get_analysis_dataset_rows",
    "get_cdf_enabled_dataset_rows",
    "get_diagnostic_dataset_rows",
    "get_semantic_dataset_rows",
]
