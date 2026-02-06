"""Semantic catalog helpers for dataset metadata and tags.

This module provides the semantic dataset rows, dataset specs, and tag helpers
used across the semantic-compile pipeline. View registration is IR-driven and
no longer uses a catalog registry.

Example:
-------
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

from semantics.catalog.dataset_rows import (
    SEMANTIC_SCHEMA_VERSION,
    DatasetCategory,
    SemanticDatasetRow,
    dataset_names,
    dataset_names_by_category,
    dataset_row,
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
from semantics.catalog.tags import (
    SemanticColumnTagSpec,
    SemanticTagSpec,
    column_name_prefix,
    prefixed_column_names,
    tag_spec_for_column,
    tag_spec_for_dataset,
)

__all__ = [
    "SEMANTIC_SCHEMA_VERSION",
    "DatasetCategory",
    "SemanticColumnTagSpec",
    "SemanticDatasetRow",
    "SemanticTagSpec",
    "build_dataset_spec",
    "build_input_schema",
    "column_name_prefix",
    "dataset_alias",
    "dataset_contract",
    "dataset_contract_schema",
    "dataset_input_schema",
    "dataset_merge_keys",
    "dataset_name_from_alias",
    "dataset_names",
    "dataset_names_by_category",
    "dataset_row",
    "dataset_schema",
    "dataset_spec",
    "dataset_specs",
    "get_all_dataset_rows",
    "get_analysis_dataset_rows",
    "get_cdf_enabled_dataset_rows",
    "get_diagnostic_dataset_rows",
    "get_semantic_dataset_rows",
    "prefixed_column_names",
    "supports_incremental",
    "tag_spec_for_column",
    "tag_spec_for_dataset",
]
