"""View builder registry for semantic catalog.

This module provides a unified registry of view builders for both analysis
and semantic views. The registry supports dynamic generation of semantic
builders based on input mapping and configuration.

Example
-------
>>> from semantics.catalog.view_builders import view_builders, view_builder
>>>
>>> # Get all view builders
>>> builders = view_builders(input_mapping={}, config=None)
>>>
>>> # Get a specific builder
>>> builder = view_builder("type_exprs_norm_v1", input_mapping={}, config=None)
>>> if builder:
...     df = builder(ctx)
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import TYPE_CHECKING

from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from semantics.catalog.analysis_builders import (
    VIEW_BUILDERS as ANALYSIS_VIEW_BUILDERS,
)
from semantics.catalog.analysis_builders import (
    VIEW_BUNDLE_BUILDERS as ANALYSIS_VIEW_BUNDLE_BUILDERS,
)
from semantics.catalog.analysis_builders import (
    DataFrameBuilder,
    PlanBundleBuilder,
)

if TYPE_CHECKING:
    from semantics.config import SemanticConfig
    from semantics.spec_registry import SemanticNormalizationSpec
    from semantics.specs import RelationshipSpec


def _get_analysis_builders() -> dict[str, DataFrameBuilder]:
    """Return the registry of analysis view builders.

    These builders produce normalized analysis outputs like type expressions,
    CFG blocks, def/use events, and reaching definitions.

    Returns
    -------
    dict[str, DataFrameBuilder]
        Mapping of view names to DataFrame builder functions.
    """
    return dict(ANALYSIS_VIEW_BUILDERS)


def _get_semantic_builders(
    *,
    input_mapping: Mapping[str, str],
    config: SemanticConfig | None,
) -> dict[str, DataFrameBuilder]:
    """Return dynamically generated semantic view builders.

    Semantic builders are generated based on the normalization and relationship
    specs from the spec registry. Each builder produces a normalized or joined
    view using the SemanticCompiler.

    Parameters
    ----------
    input_mapping
        Mapping of canonical input names to registered table names. Used to
        resolve actual table names for spec-based normalization.
    config
        Optional semantic configuration for compiler behavior.

    Returns
    -------
    dict[str, DataFrameBuilder]
        Mapping of view names to DataFrame builder functions.
    """
    from semantics.spec_registry import (
        RELATIONSHIP_SPECS,
        SEMANTIC_NORMALIZATION_SPECS,
        semantic_spec_index,
    )

    builders: dict[str, DataFrameBuilder] = {}

    # Build normalization builders from specs
    for spec_entry in SEMANTIC_NORMALIZATION_SPECS:
        output_name = spec_entry.output_name
        builders[output_name] = _normalize_spec_builder(
            spec_entry,
            input_mapping=input_mapping,
            config=config,
        )

    # Build relationship builders from specs
    relationship_by_name = {spec.name: spec for spec in RELATIONSHIP_SPECS}
    for spec_index in semantic_spec_index():
        if spec_index.kind == "scip_normalize":
            builders[spec_index.name] = _scip_norm_builder(
                _input(input_mapping, "scip_occurrences"),
                line_index_table=_input(input_mapping, "file_line_index_v1"),
            )
        elif spec_index.kind == "relate":
            rel_spec = relationship_by_name.get(spec_index.name)
            if rel_spec is not None:
                builders[spec_index.name] = _relationship_builder(
                    rel_spec,
                    config=config,
                    use_cdf=False,  # Default to no CDF for registry builders
                )
        elif spec_index.kind == "union_nodes":
            builders[spec_index.name] = _union_nodes_builder(
                list(spec_index.inputs),
                config=config,
            )
        elif spec_index.kind == "union_edges":
            builders[spec_index.name] = _union_edges_builder(
                list(spec_index.inputs),
                config=config,
            )

    return builders


def _input(mapping: Mapping[str, str], name: str) -> str:
    """Resolve an input name from the mapping.

    Returns
    -------
    str
        Resolved table name from mapping, or original name if not found.
    """
    return mapping.get(name, name)


def _normalize_spec_builder(
    spec: SemanticNormalizationSpec,
    *,
    input_mapping: Mapping[str, str],
    config: SemanticConfig | None,
) -> DataFrameBuilder:
    """Build a DataFrame builder from a normalization spec.

    Parameters
    ----------
    spec
        The SemanticNormalizationSpec to use.
    input_mapping
        Mapping of canonical names to actual table names.
    config
        Optional semantic configuration.

    Returns
    -------
    DataFrameBuilder
        A callable that builds the normalized DataFrame.

    Raises
    ------
    TypeError
        Raised when spec is not a SemanticNormalizationSpec.
    """
    from semantics.spec_registry import SemanticNormalizationSpec

    if not isinstance(spec, SemanticNormalizationSpec):
        msg = f"Expected SemanticNormalizationSpec, got {type(spec)}"
        raise TypeError(msg)

    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.compiler import SemanticCompiler

        resolved_table = input_mapping.get(spec.source_table, spec.source_table)
        resolved_spec = replace(spec.spec, table=resolved_table)
        return SemanticCompiler(inner_ctx, config=config).normalize_from_spec(resolved_spec)

    return _builder


def _relationship_builder(
    spec: RelationshipSpec,
    *,
    config: SemanticConfig | None,
    use_cdf: bool,
) -> DataFrameBuilder:
    """Build a DataFrame builder from a relationship spec.

    Parameters
    ----------
    spec
        The RelationshipSpec to use.
    config
        Optional semantic configuration.
    use_cdf
        Whether to enable CDF-aware incremental joins.

    Returns
    -------
    DataFrameBuilder
        A callable that builds the relationship DataFrame.

    Raises
    ------
    TypeError
        Raised when spec is not a RelationshipSpec.
    """
    from semantics.specs import RelationshipSpec

    if not isinstance(spec, RelationshipSpec):
        msg = f"Expected RelationshipSpec, got {type(spec)}"
        raise TypeError(msg)

    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.compiler import RelationOptions, SemanticCompiler

        return SemanticCompiler(inner_ctx, config=config).relate(
            spec.left_table,
            spec.right_table,
            options=RelationOptions(
                strategy_hint=spec.to_strategy_type(),
                filter_sql=spec.filter_sql,
                origin=spec.origin,
                use_cdf=use_cdf,
                output_name=spec.name,
            ),
        )

    return _builder


def _scip_norm_builder(
    table: str,
    *,
    line_index_table: str,
) -> DataFrameBuilder:
    """Build a DataFrame builder for SCIP normalization.

    Parameters
    ----------
    table
        The SCIP occurrences table name.
    line_index_table
        The line index table name for byte offset conversion.

    Returns
    -------
    DataFrameBuilder
        A callable that builds the normalized SCIP DataFrame.
    """

    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.scip_normalize import scip_to_byte_offsets

        return scip_to_byte_offsets(
            inner_ctx,
            occurrences_table=table,
            line_index_table=line_index_table,
        )

    return _builder


def _union_nodes_builder(
    names: list[str],
    *,
    config: SemanticConfig | None,
) -> DataFrameBuilder:
    """Build a DataFrame builder for node union.

    Parameters
    ----------
    names
        List of view names to union.
    config
        Optional semantic configuration.

    Returns
    -------
    DataFrameBuilder
        A callable that builds the unioned nodes DataFrame.
    """

    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.compiler import SemanticCompiler

        return SemanticCompiler(inner_ctx, config=config).union_nodes(
            names,
            discriminator="node_kind",
        )

    return _builder


def _union_edges_builder(
    names: list[str],
    *,
    config: SemanticConfig | None,
) -> DataFrameBuilder:
    """Build a DataFrame builder for edge union.

    Parameters
    ----------
    names
        List of view names to union.
    config
        Optional semantic configuration.

    Returns
    -------
    DataFrameBuilder
        A callable that builds the unioned edges DataFrame.
    """

    def _builder(inner_ctx: SessionContext) -> DataFrame:
        from semantics.compiler import SemanticCompiler

        return SemanticCompiler(inner_ctx, config=config).union_edges(
            names,
            discriminator="edge_kind",
        )

    return _builder


def view_builder(
    name: str,
    *,
    input_mapping: Mapping[str, str],
    config: SemanticConfig | None,
) -> DataFrameBuilder | None:
    """Get a single view builder by name.

    Looks up the builder first in analysis builders, then in semantic
    builders. Returns None if no builder is found for the given name.

    Parameters
    ----------
    name
        The view name to look up.
    input_mapping
        Mapping of canonical input names to registered table names.
    config
        Optional semantic configuration.

    Returns
    -------
    DataFrameBuilder | None
        The builder function, or None if not found.
    """
    # Check analysis builders first (they don't need input_mapping/config)
    analysis = _get_analysis_builders()
    if name in analysis:
        return analysis[name]

    # Check semantic builders
    semantic = _get_semantic_builders(input_mapping=input_mapping, config=config)
    return semantic.get(name)


def view_builders(
    *,
    input_mapping: Mapping[str, str],
    config: SemanticConfig | None,
) -> dict[str, DataFrameBuilder]:
    """Get all view builders combined.

    Returns a dictionary containing both analysis builders and dynamically
    generated semantic builders. Analysis builders are included first, then
    semantic builders (which may override analysis builders if names conflict).

    Parameters
    ----------
    input_mapping
        Mapping of canonical input names to registered table names.
    config
        Optional semantic configuration.

    Returns
    -------
    dict[str, DataFrameBuilder]
        Combined mapping of view names to DataFrame builder functions.
    """
    builders: dict[str, DataFrameBuilder] = {}
    builders.update(_get_analysis_builders())
    builders.update(_get_semantic_builders(input_mapping=input_mapping, config=config))
    return builders


# Backward compatibility exports for analysis builders only
# These are safe to use without input_mapping/config since they don't
# depend on semantic normalization specs.
VIEW_BUILDERS: dict[str, DataFrameBuilder] = ANALYSIS_VIEW_BUILDERS
VIEW_BUNDLE_BUILDERS: dict[str, PlanBundleBuilder] = ANALYSIS_VIEW_BUNDLE_BUILDERS


__all__ = [
    "VIEW_BUILDERS",
    "VIEW_BUNDLE_BUILDERS",
    "DataFrameBuilder",
    "PlanBundleBuilder",
    "view_builder",
    "view_builders",
]
