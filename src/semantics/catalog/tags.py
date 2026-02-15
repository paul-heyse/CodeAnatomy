"""Semantic tag policy helpers derived from the semantic catalog."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa

from semantics.catalog.dataset_rows import SemanticDatasetRow, dataset_row
from semantics.catalog.dataset_specs import dataset_alias, dataset_name_from_alias, dataset_schema
from semantics.column_types import ColumnType, TableType, infer_column_type, infer_table_type
from semantics.types.core import CompatibilityGroup, get_compatibility_groups


@dataclass(frozen=True)
class SemanticTagSpec:
    """Resolved semantic tagging metadata for a dataset."""

    dataset_name: str
    artifact: str
    semantic_id: str
    kind: str
    entity: str
    grain: str
    version: str
    stability: str
    schema_ref: str
    entity_keys: tuple[str, ...]
    join_keys: tuple[str, ...]
    materialization: str | None
    materialized_name: str | None


@dataclass(frozen=True)
class SemanticColumnTagSpec:
    """Resolved semantic tagging metadata for a dataset column."""

    dataset_name: str
    column_name: str
    artifact: str
    semantic_id: str
    kind: str
    entity: str
    grain: str
    version: str
    stability: str
    schema_ref: str
    dtype: str | None


def tag_spec_for_dataset(name: str) -> SemanticTagSpec:
    """Return a resolved semantic tag spec for a dataset name or alias.

    Parameters
    ----------
    name
        Dataset name or alias.

    Returns:
    -------
    SemanticTagSpec
        Resolved semantic tagging metadata.
    """
    canonical = _canonical_dataset_name(name)
    row = dataset_row(canonical, strict=True)
    artifact = _safe_dataset_alias(canonical)
    semantic_id = row.semantic_id or canonical
    entity, grain = _resolve_entity_grain(row)
    stability = _resolve_stability(row)
    schema_ref = row.schema_ref or canonical
    join_keys = tuple(row.join_keys)
    entity_keys = tuple(row.merge_keys or row.join_keys)
    materialization = row.materialization
    materialized_name = row.materialized_name
    if materialization and not materialized_name:
        materialized_name = f"semantic.{canonical}"
    return SemanticTagSpec(
        dataset_name=canonical,
        artifact=artifact,
        semantic_id=semantic_id,
        kind=row.kind or "table",
        entity=entity,
        grain=grain,
        version=str(row.version),
        stability=stability,
        schema_ref=schema_ref,
        entity_keys=entity_keys,
        join_keys=join_keys,
        materialization=materialization,
        materialized_name=materialized_name,
    )


def tag_spec_for_column(dataset_name: str, column_name: str) -> SemanticColumnTagSpec:
    """Return a resolved semantic tag spec for a dataset column.

    Parameters
    ----------
    dataset_name
        Dataset name or alias.
    column_name
        Column name to tag.

    Returns:
    -------
    SemanticColumnTagSpec
        Resolved semantic column tagging metadata.
    """
    dataset_spec = tag_spec_for_dataset(dataset_name)
    dtype = _column_dtype(dataset_spec.dataset_name, column_name)
    semantic_id = f"{dataset_spec.semantic_id}.{column_name}"
    artifact = f"{dataset_spec.artifact}.{column_name}"
    return SemanticColumnTagSpec(
        dataset_name=dataset_spec.dataset_name,
        column_name=column_name,
        artifact=artifact,
        semantic_id=semantic_id,
        kind="column",
        entity=dataset_spec.entity,
        grain=dataset_spec.grain,
        version=dataset_spec.version,
        stability=dataset_spec.stability,
        schema_ref=dataset_spec.schema_ref,
        dtype=dtype,
    )


def _canonical_dataset_name(name: str) -> str:
    try:
        return dataset_name_from_alias(name)
    except KeyError:
        return name


def _safe_dataset_alias(name: str) -> str:
    try:
        return dataset_alias(name)
    except KeyError:
        return name


def _resolve_entity_grain(row: SemanticDatasetRow) -> tuple[str, str]:
    if row.entity and row.grain:
        return row.entity, row.grain

    category_mapping = {
        "diagnostic": ("diagnostic", "per_record"),
        "analysis": ("analysis", "per_record"),
    }
    if row.category in category_mapping:
        return category_mapping[row.category]

    inferred = _infer_entity_grain_from_metadata(row)
    if inferred is None:
        inferred = _infer_entity_from_schema(row.name)
    return inferred if inferred is not None else ("entity", "per_row")


def _infer_entity_from_schema(name: str) -> tuple[str, str] | None:
    try:
        schema = dataset_schema(name)
    except KeyError:
        return None
    if not isinstance(schema, pa.Schema):
        return None
    column_types = {
        infer_column_type(field.name) for field in schema if isinstance(field, pa.Field)
    }
    table_type = infer_table_type(column_types)
    mapping = {
        TableType.RELATION: ("edge", "per_edge"),
        TableType.ENTITY: ("entity", "per_entity"),
        TableType.EVIDENCE: ("evidence", "per_evidence"),
        TableType.SYMBOL_SOURCE: ("symbol", "per_symbol"),
    }
    return mapping.get(table_type)


def _infer_entity_grain_from_table_type(
    table_type: TableType,
) -> tuple[str, str] | None:
    mapping = {
        TableType.RELATION: ("edge", "per_edge"),
        TableType.ENTITY: ("entity", "per_entity"),
        TableType.EVIDENCE: ("evidence", "per_evidence"),
        TableType.SYMBOL_SOURCE: ("symbol", "per_symbol"),
    }
    return mapping.get(table_type)


def _infer_entity_grain_from_metadata(
    row: SemanticDatasetRow,
) -> tuple[str, str] | None:
    join_keys = tuple(row.join_keys)
    join_key_types = {infer_column_type(name) for name in join_keys}
    join_key_groups = {group for name in join_keys for group in get_compatibility_groups(name)}
    all_columns = {*join_keys, *row.fields, *row.partition_cols}
    table_type = infer_table_type({infer_column_type(name) for name in all_columns})

    inferred = _infer_entity_grain_by_join_key_signature(
        join_key_groups=join_key_groups,
        join_key_types=join_key_types,
    )
    if inferred is None:
        inferred = _infer_entity_grain_from_table_type(table_type)
    if inferred is None:
        inferred = _infer_entity_grain_by_join_key_fallback(
            join_key_groups=join_key_groups,
            join_key_types=join_key_types,
        )
    return inferred


def _infer_entity_grain_by_join_key_signature(
    *,
    join_key_groups: set[CompatibilityGroup],
    join_key_types: set[ColumnType],
) -> tuple[str, str] | None:
    if (
        CompatibilityGroup.ENTITY_IDENTITY in join_key_groups
        and CompatibilityGroup.SYMBOL_IDENTITY in join_key_groups
    ):
        return ("edge", "per_edge")
    if ColumnType.FILE_ID in join_key_types or ColumnType.PATH in join_key_types:
        return ("file", "per_file")
    if (
        CompatibilityGroup.SYMBOL_IDENTITY in join_key_groups
        or ColumnType.SYMBOL in join_key_types
    ):
        return ("symbol", "per_symbol")
    return None


def _infer_entity_grain_by_join_key_fallback(
    *,
    join_key_groups: set[CompatibilityGroup],
    join_key_types: set[ColumnType],
) -> tuple[str, str] | None:
    if (
        CompatibilityGroup.ENTITY_IDENTITY in join_key_groups
        or ColumnType.ENTITY_ID in join_key_types
    ):
        return ("entity", "per_entity")
    if ColumnType.EVIDENCE in join_key_types:
        return ("evidence", "per_evidence")
    if ColumnType.TEXT in join_key_types:
        return ("text", "per_record")
    if ColumnType.NESTED in join_key_types:
        return ("nested", "per_record")
    return None


def _resolve_stability(row: SemanticDatasetRow) -> str:
    if row.stability:
        return row.stability
    if row.category == "semantic":
        return "design"
    if row.category == "analysis":
        return "experimental"
    return "diagnostic"


def _column_dtype(dataset_name: str, column_name: str) -> str | None:
    try:
        schema = dataset_schema(dataset_name)
    except KeyError:
        return None
    if not isinstance(schema, pa.Schema):
        return None
    field = schema.field(column_name) if column_name in schema.names else None
    if field is None:
        return None
    return str(field.type)


def column_name_prefix(dataset_alias: str) -> str:
    """Return the standard prefix for namespaced column nodes.

    Parameters
    ----------
    dataset_alias
        Dataset alias for the semantic output.

    Returns:
    -------
    str
        Prefix used for namespaced column nodes.
    """
    return f"{dataset_alias}__"


def prefixed_column_names(
    dataset_alias: str,
    columns: Sequence[str],
) -> tuple[str, ...]:
    """Return column names prefixed with the dataset alias.

    Parameters
    ----------
    dataset_alias
        Dataset alias used as the prefix.
    columns
        Column names to prefix.

    Returns:
    -------
    tuple[str, ...]
        Prefixed column names.
    """
    prefix = column_name_prefix(dataset_alias)
    return tuple(f"{prefix}{column}" for column in columns)


__all__ = [
    "SemanticColumnTagSpec",
    "SemanticTagSpec",
    "column_name_prefix",
    "prefixed_column_names",
    "tag_spec_for_column",
    "tag_spec_for_dataset",
]
