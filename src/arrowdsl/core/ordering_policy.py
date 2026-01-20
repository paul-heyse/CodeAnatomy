"""Ordering policy helpers shared across execution surfaces."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.determinism import DeterminismTier
from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.core.ordering import Ordering, OrderingKey, OrderingLevel
from arrowdsl.schema.metadata import (
    infer_ordering_keys,
    ordering_from_schema,
    ordering_metadata_spec,
)
from arrowdsl.schema.schema import SchemaMetadataSpec
from datafusion_engine.compute_ops import sort_indices


def ordering_keys_for_schema(schema: SchemaLike) -> tuple[OrderingKey, ...]:
    """Infer ordering keys from schema metadata or column names.

    Returns
    -------
    tuple[OrderingKey, ...]
        Ordering keys inferred for the schema.
    """
    ordering = ordering_from_schema(schema)
    if ordering.keys:
        return ordering.keys
    return infer_ordering_keys(schema.names)


def apply_canonical_sort(
    table: TableLike,
    *,
    determinism: DeterminismTier,
) -> tuple[TableLike, tuple[OrderingKey, ...]]:
    """Return canonical sort output and keys used when determinism is canonical.

    Returns
    -------
    tuple[TableLike, tuple[OrderingKey, ...]]
        Sorted table and ordering keys used.
    """
    if determinism != DeterminismTier.CANONICAL:
        return table, ()
    keys = ordering_keys_for_schema(table.schema)
    if not keys:
        return table, ()
    indices = sort_indices(table, sort_keys=list(keys))
    return table.take(indices), tuple(keys)


def ordering_metadata_for_plan(
    ordering: Ordering,
    *,
    schema: SchemaLike,
    canonical_keys: Sequence[OrderingKey] | None = None,
    determinism: DeterminismTier | None = None,
) -> SchemaMetadataSpec:
    """Return ordering metadata spec for a plan and output schema.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec containing ordering annotations.
    """
    level = ordering.level
    keys: Sequence[OrderingKey] = ()
    if canonical_keys:
        level = OrderingLevel.EXPLICIT
        keys = tuple(canonical_keys)
    elif ordering.level == OrderingLevel.EXPLICIT and ordering.keys:
        keys = ordering.keys
    elif ordering.level == OrderingLevel.IMPLICIT:
        keys = ordering_keys_for_schema(schema)
    extra: dict[bytes, bytes] | None = None
    if determinism is not None:
        extra = {b"determinism_tier": determinism.value.encode("utf-8")}
    return ordering_metadata_spec(level, keys=keys, extra=extra)


def require_explicit_ordering(schema: SchemaLike, *, label: str) -> tuple[OrderingKey, ...]:
    """Return ordering keys when explicit ordering metadata is present.

    Parameters
    ----------
    schema:
        Schema carrying ordering metadata.
    label:
        Label used in error messages when ordering metadata is missing.

    Returns
    -------
    tuple[OrderingKey, ...]
        Explicit ordering keys.

    Raises
    ------
    ValueError
        Raised when the schema lacks explicit ordering metadata.
    """
    ordering = ordering_from_schema(schema)
    if ordering.level != OrderingLevel.EXPLICIT or not ordering.keys:
        msg = f"{label} requires explicit ordering metadata."
        raise ValueError(msg)
    return ordering.keys


__all__ = [
    "apply_canonical_sort",
    "ordering_keys_for_schema",
    "ordering_metadata_for_plan",
    "require_explicit_ordering",
]
