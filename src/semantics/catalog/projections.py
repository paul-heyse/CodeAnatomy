"""Projection builders for semantic catalog outputs.

This module provides projection specifications and builders for transforming
semantic relationship outputs into the canonical relation_output format.
The projections handle column mapping, type coercion, and optional field
population.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

if TYPE_CHECKING:
    from collections.abc import Callable

    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from semantics.adapters import RelationshipProjectionOptions
    from semantics.specs import RelationshipSpec

    DataFrameBuilder = Callable[[SessionContext], DataFrame]


@dataclass(frozen=True)
class RelationOutputSpec:
    """Parameters for relation_output projection.

    Specifies how to transform a semantic relationship DataFrame into
    the canonical relation_output schema, mapping source and destination
    columns and providing constant values for edge classification.

    Attributes
    ----------
    src_col
        Source column name to map to "src" in output.
    dst_col
        Destination column name to map to "dst" in output.
    kind
        Edge kind constant for this relationship type.
    origin
        Origin identifier (e.g., "cst", "scip").
    qname_source_col
        Optional column for qualified name source. When None, output is null.
    ambiguity_group_col
        Optional column for ambiguity group ID. When None, output is null.
    """

    src_col: str
    dst_col: str
    kind: str
    origin: str
    qname_source_col: str | None = None
    ambiguity_group_col: str | None = None


@dataclass(frozen=True)
class SemanticProjectionConfig:
    """Configuration for semantic relationship projection options."""

    default_priority: int
    rel_name_output: str
    rel_import_output: str
    rel_def_output: str
    rel_call_output: str
    relationship_specs: Sequence[RelationshipSpec]


def relation_output_projection(
    df: DataFrame,
    spec: RelationOutputSpec,
) -> DataFrame:
    """Project a semantic relationship DataFrame to relation_output schema.

    Transforms input columns according to the spec, adding constant values
    for kind and origin, and handling optional columns with null defaults.

    Parameters
    ----------
    df
        Input DataFrame with relationship data.
    spec
        Projection specification defining column mappings.

    Returns
    -------
    DataFrame
        Projected DataFrame matching relation_output schema.
    """
    from datafusion import col, lit
    from datafusion import functions as f

    null_str = lit(None).cast(pa.string())
    qname_source = col(spec.qname_source_col) if spec.qname_source_col else null_str
    ambiguity_group = col(spec.ambiguity_group_col) if spec.ambiguity_group_col else null_str

    return df.select(
        col(spec.src_col).alias("src"),
        col(spec.dst_col).alias("dst"),
        col("path").alias("path"),
        f.coalesce(col("edge_owner_file_id"), col("file_id")).alias("edge_owner_file_id"),
        col("bstart").alias("bstart"),
        col("bend").alias("bend"),
        lit(spec.origin).alias("origin"),
        col("resolution_method").alias("resolution_method"),
        col("binding_kind").alias("binding_kind"),
        col("def_site_kind").alias("def_site_kind"),
        col("use_kind").alias("use_kind"),
        lit(spec.kind).alias("kind"),
        col("reason").alias("reason"),
        col("confidence").alias("confidence"),
        col("score").alias("score"),
        col("symbol_roles").alias("symbol_roles"),
        qname_source.alias("qname_source"),
        ambiguity_group.alias("ambiguity_group_id"),
        col("diag_source").alias("diag_source"),
        col("severity").alias("severity"),
        col("task_name").alias("task_name"),
        col("task_priority").alias("task_priority"),
    )


def projected_builder(
    builder: DataFrameBuilder,
    *,
    options: RelationshipProjectionOptions,
) -> DataFrameBuilder:
    """Wrap a semantic builder to project outputs into legacy schema.

    Returns
    -------
    DataFrameBuilder
        Builder that projects semantic outputs into the legacy schema.
    """

    def _build(inner_ctx: SessionContext) -> DataFrame:
        from semantics.adapters import project_semantic_to_legacy

        return project_semantic_to_legacy(builder(inner_ctx), options=options)

    return _build


def semantic_projection_options(
    config: SemanticProjectionConfig,
) -> dict[str, RelationshipProjectionOptions]:
    """Build projection options for semantic relationship outputs.

    Returns
    -------
    dict[str, RelationshipProjectionOptions]
        Projection options keyed by relationship output name.

    Raises
    ------
    KeyError
        Raised when relationship specs are missing from the projection map.
    """
    from cpg.kind_catalog import (
        EDGE_KIND_PY_CALLS_SYMBOL,
        EDGE_KIND_PY_DEFINES_SYMBOL,
        EDGE_KIND_PY_IMPORTS_SYMBOL,
        EDGE_KIND_PY_REFERENCES_SYMBOL,
    )
    from semantics.adapters import RelationshipProjectionOptions

    projection_map: dict[str, RelationshipProjectionOptions] = {
        config.rel_name_output: RelationshipProjectionOptions(
            entity_id_alias="ref_id",
            edge_kind=str(EDGE_KIND_PY_REFERENCES_SYMBOL),
            task_name="rel.name_symbol",
            task_priority=config.default_priority,
        ),
        config.rel_import_output: RelationshipProjectionOptions(
            entity_id_alias="import_alias_id",
            edge_kind=str(EDGE_KIND_PY_IMPORTS_SYMBOL),
            task_name="rel.import_symbol",
            task_priority=config.default_priority,
        ),
        config.rel_def_output: RelationshipProjectionOptions(
            entity_id_alias="def_id",
            edge_kind=str(EDGE_KIND_PY_DEFINES_SYMBOL),
            task_name="rel.def_symbol",
            task_priority=config.default_priority,
        ),
        config.rel_call_output: RelationshipProjectionOptions(
            entity_id_alias="call_id",
            edge_kind=str(EDGE_KIND_PY_CALLS_SYMBOL),
            task_name="rel.callsite_symbol",
            task_priority=config.default_priority,
        ),
    }
    required = {spec.name for spec in config.relationship_specs}
    missing = required - set(projection_map)
    if missing:
        msg = f"Missing relationship projection metadata for: {sorted(missing)!r}."
        raise KeyError(msg)
    return projection_map


def semantic_view_specs(
    base_specs: list[tuple[str, DataFrameBuilder]],
    *,
    relationship_specs: Sequence[RelationshipSpec],
    projection_options: Mapping[str, RelationshipProjectionOptions],
) -> tuple[list[tuple[str, DataFrameBuilder]], dict[str, DataFrameBuilder]]:
    """Return semantic view specs with relationship projections applied.

    Returns
    -------
    tuple[list[tuple[str, DataFrameBuilder]], dict[str, DataFrameBuilder]]
        Updated view specs and the extracted relationship builders.
    """
    relationship_names = {spec.name for spec in relationship_specs}
    relationship_builders: dict[str, DataFrameBuilder] = {
        name: builder for name, builder in base_specs if name in relationship_names
    }
    view_specs: list[tuple[str, DataFrameBuilder]] = []
    for name, builder in base_specs:
        if name in relationship_names:
            view_specs.append((name, projected_builder(builder, options=projection_options[name])))
        else:
            view_specs.append((name, builder))
    return view_specs, relationship_builders


def relation_output_builder(
    relationship_builders: Mapping[str, DataFrameBuilder],
    projection_options: Mapping[str, RelationshipProjectionOptions],
) -> DataFrameBuilder:
    """Build relation_output projection from relationship builders.

    Returns
    -------
    DataFrameBuilder
        Builder that unions projected relationship outputs into relation_output.
    """

    def _build(inner_ctx: SessionContext) -> DataFrame:
        from dataclasses import replace

        from semantics.adapters import project_semantic_to_legacy

        frames: list[DataFrame] = []
        for rel_name in sorted(relationship_builders):
            builder = relationship_builders[rel_name]
            options = projection_options[rel_name]
            extended_opts = replace(options, include_extended_columns=True)
            rel_df = project_semantic_to_legacy(builder(inner_ctx), options=extended_opts)
            output_spec = RelationOutputSpec(
                src_col=options.entity_id_alias,
                dst_col="symbol",
                kind=str(options.edge_kind),
                origin="cst",
            )
            frames.append(relation_output_projection(rel_df, output_spec))
        if not frames:
            msg = "Semantic relationship builders did not produce any relation outputs."
            raise ValueError(msg)
        result = frames[0]
        for frame in frames[1:]:
            result = result.union(frame)
        return result

    return _build


def cpg_nodes_builder() -> DataFrameBuilder:
    """Build CPG nodes projection from union_nodes_v1.

    Returns
    -------
    DataFrameBuilder
        A callable that returns the CPG nodes DataFrame.
    """

    def _builder(ctx: SessionContext) -> DataFrame:
        return ctx.table("union_nodes_v1")

    return _builder


def cpg_edges_builder() -> DataFrameBuilder:
    """Build CPG edges projection from union_edges_v1.

    Returns
    -------
    DataFrameBuilder
        A callable that returns the CPG edges DataFrame.
    """

    def _builder(ctx: SessionContext) -> DataFrame:
        return ctx.table("union_edges_v1")

    return _builder


__all__ = [
    "RelationOutputSpec",
    "SemanticProjectionConfig",
    "cpg_edges_builder",
    "cpg_nodes_builder",
    "projected_builder",
    "relation_output_builder",
    "relation_output_projection",
    "semantic_projection_options",
    "semantic_view_specs",
]
