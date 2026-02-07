"""Semantic dataset row accessors sourced from Semantic IR.

This module provides a thin accessor layer over Semantic IR dataset rows.
The IR emission pipeline is the single source of truth for dataset row
assembly and ordering.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Final, Literal, overload

DatasetCategory = Literal["semantic", "analysis", "diagnostic"]
DatasetRole = Literal["input", "intermediate", "output"]

# Schema version for semantic dataset rows
SEMANTIC_SCHEMA_VERSION: Final[int] = 1


@dataclass(frozen=True)
class SemanticDatasetRow:
    """Row spec describing a semantic dataset with operational metadata.

    Extends the normalize DatasetRow pattern with additional metadata
    for semantic pipeline operations including CDF support, partition
    configuration, and merge key specifications.

    Attributes:
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
    role
        Semantic role for evidence seeding and input validation.
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
    role: DatasetRole = "output"


@dataclass
class _SemanticDatasetRowCache:
    rows: tuple[SemanticDatasetRow, ...] | None = None
    rows_by_name: Mapping[str, SemanticDatasetRow] | None = None


_SEMANTIC_DATASET_ROWS_CACHE = _SemanticDatasetRowCache()


def _get_semantic_dataset_rows() -> tuple[SemanticDatasetRow, ...]:
    """Return the cached semantic dataset rows, building if needed.

    Returns:
    -------
    tuple[SemanticDatasetRow, ...]
        All semantic dataset rows in dependency order.
    """
    rows = _SEMANTIC_DATASET_ROWS_CACHE.rows
    if rows is None:
        from semantics.compile_context import semantic_ir_for_outputs

        rows = semantic_ir_for_outputs().dataset_rows
        _SEMANTIC_DATASET_ROWS_CACHE.rows = rows
    return rows


def _get_rows_by_name() -> Mapping[str, SemanticDatasetRow]:
    """Return the indexed lookup mapping, building if needed.

    Returns:
    -------
    Mapping[str, SemanticDatasetRow]
        Mapping from dataset name to row.
    """
    rows_by_name = _SEMANTIC_DATASET_ROWS_CACHE.rows_by_name
    if rows_by_name is None:
        rows_by_name = {row.name: row for row in _get_semantic_dataset_rows()}
        _SEMANTIC_DATASET_ROWS_CACHE.rows_by_name = rows_by_name
    return rows_by_name


@overload
def dataset_row(name: str, *, strict: Literal[True]) -> SemanticDatasetRow: ...


@overload
def dataset_row(name: str, *, strict: Literal[False] = ...) -> SemanticDatasetRow | None: ...


def dataset_row(name: str, *, strict: bool = False) -> SemanticDatasetRow | None:
    """Return the semantic dataset row for a given name.

    Args:
        name: Description.
        strict: Description.

    Raises:
        KeyError: If the operation cannot be completed.
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

    Returns:
    -------
    tuple[SemanticDatasetRow, ...]
        Dataset rows in the order requested (skips missing names).
    """
    rows_by_name = _get_rows_by_name()
    return tuple(row for name in names if (row := rows_by_name.get(name)) is not None)


def get_all_dataset_rows() -> tuple[SemanticDatasetRow, ...]:
    """Return all semantic dataset rows.

    Returns:
    -------
    tuple[SemanticDatasetRow, ...]
        All registered semantic dataset rows in dependency order.
    """
    return _get_semantic_dataset_rows()


def get_semantic_dataset_rows() -> tuple[SemanticDatasetRow, ...]:
    """Return semantic dataset rows with category 'semantic'.

    Returns:
    -------
    tuple[SemanticDatasetRow, ...]
        Semantic category dataset rows.
    """
    return tuple(row for row in _get_semantic_dataset_rows() if row.category == "semantic")


def get_analysis_dataset_rows() -> tuple[SemanticDatasetRow, ...]:
    """Return semantic dataset rows with category 'analysis'.

    Returns:
    -------
    tuple[SemanticDatasetRow, ...]
        Analysis category dataset rows.
    """
    return tuple(row for row in _get_semantic_dataset_rows() if row.category == "analysis")


def get_diagnostic_dataset_rows() -> tuple[SemanticDatasetRow, ...]:
    """Return semantic dataset rows with category 'diagnostic'.

    Returns:
    -------
    tuple[SemanticDatasetRow, ...]
        Diagnostic category dataset rows.
    """
    return tuple(row for row in _get_semantic_dataset_rows() if row.category == "diagnostic")


def get_cdf_enabled_dataset_rows() -> tuple[SemanticDatasetRow, ...]:
    """Return semantic dataset rows that support CDF.

    Returns:
    -------
    tuple[SemanticDatasetRow, ...]
        Dataset rows with supports_cdf=True.
    """
    return tuple(row for row in _get_semantic_dataset_rows() if row.supports_cdf)


def dataset_names() -> tuple[str, ...]:
    """Return all dataset names in registry order.

    Returns:
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

    Returns:
    -------
    tuple[str, ...]
        Dataset names in the category.
    """
    return tuple(row.name for row in _get_semantic_dataset_rows() if row.category == category)


__all__ = [
    "SEMANTIC_SCHEMA_VERSION",
    "DatasetCategory",
    "SemanticDatasetRow",
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
