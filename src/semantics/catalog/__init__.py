"""Semantic view catalog for programmatic view registration.

This module provides a registry of semantic views with metadata including
evidence tier, upstream dependencies, and plan fingerprints. The catalog
enables dependency-ordered view construction and incremental processing.

Additionally provides SemanticDatasetRow for unified dataset metadata
including CDF support, partition configuration, and merge keys.

Example
-------
>>> from semantics.catalog import SEMANTIC_CATALOG, CatalogEntry
>>> from semantics.builders import SemanticViewBuilder
>>>
>>> # Register a builder
>>> SEMANTIC_CATALOG.register(my_builder)
>>>
>>> # Get dependency order for building views
>>> build_order = SEMANTIC_CATALOG.topological_order()
>>> for name in build_order:
...     entry = SEMANTIC_CATALOG.get(name)
...     df = entry.builder.build(ctx)
...     ctx.register_view(name, df)

>>> # Access dataset rows with operational metadata
>>> from semantics.catalog import dataset_row, get_semantic_dataset_rows
>>> row = dataset_row("cpg_nodes_v1", strict=True)
>>> row.supports_cdf
True
>>> semantic_rows = get_semantic_dataset_rows()

>>> # Access dataset specs and schemas
>>> from semantics.catalog import dataset_spec, dataset_schema
>>> spec = dataset_spec("cpg_nodes_v1")
>>> schema = dataset_schema("cpg_nodes_v1")
"""

from __future__ import annotations

from semantics.catalog.catalog import (
    SEMANTIC_CATALOG,
    CatalogEntry,
    SemanticCatalog,
)
from semantics.catalog.dataset_rows import (
    SEMANTIC_SCHEMA_VERSION,
    DatasetCategory,
    SemanticDatasetRow,
    dataset_names,
    dataset_names_by_category,
    dataset_row,
    dataset_rows,
    get_all_dataset_rows,
    get_analysis_dataset_rows,
    get_cdf_enabled_dataset_rows,
    get_diagnostic_dataset_rows,
    get_semantic_dataset_rows,
)
from semantics.catalog.dataset_specs import (
    dataset_alias,
    dataset_contract,
    dataset_contract_schema,
    dataset_input_schema,
    dataset_merge_keys,
    dataset_name_from_alias,
    dataset_schema,
    dataset_spec,
    dataset_specs,
    supports_incremental,
)
from semantics.catalog.spec_builder import (
    build_dataset_spec,
    build_input_schema,
)
from semantics.catalog.view_builders import (
    VIEW_BUILDERS,
    VIEW_BUNDLE_BUILDERS,
    DataFrameBuilder,
    PlanBundleBuilder,
    view_builder,
    view_builders,
)

__all__ = [
    "SEMANTIC_CATALOG",
    "SEMANTIC_SCHEMA_VERSION",
    "VIEW_BUILDERS",
    "VIEW_BUNDLE_BUILDERS",
    "CatalogEntry",
    "DataFrameBuilder",
    "DatasetCategory",
    "PlanBundleBuilder",
    "SemanticCatalog",
    "SemanticDatasetRow",
    "build_dataset_spec",
    "build_input_schema",
    "dataset_alias",
    "dataset_contract",
    "dataset_contract_schema",
    "dataset_input_schema",
    "dataset_merge_keys",
    "dataset_name_from_alias",
    "dataset_names",
    "dataset_names_by_category",
    "dataset_row",
    "dataset_rows",
    "dataset_schema",
    "dataset_spec",
    "dataset_specs",
    "get_all_dataset_rows",
    "get_analysis_dataset_rows",
    "get_cdf_enabled_dataset_rows",
    "get_diagnostic_dataset_rows",
    "get_semantic_dataset_rows",
    "supports_incremental",
    "view_builder",
    "view_builders",
]
