"""Shared ordering policy for ArrowDSL execution."""

from __future__ import annotations

from collections.abc import Sequence

from arrowdsl.core.context import DeterminismTier, Ordering, OrderingKey, OrderingLevel
from arrowdsl.core.interop import SchemaLike, TableLike, pc
from arrowdsl.schema.metadata import (
    infer_ordering_keys,
    ordering_from_schema,
    ordering_metadata_spec,
)
from arrowdsl.schema.schema import SchemaMetadataSpec


def ordering_keys_for_schema(schema: SchemaLike) -> tuple[OrderingKey, ...]:
    """Infer ordering keys from schema metadata or column names.

    Returns
    -------
    tuple[OrderingKey, ...]
        Ordering keys inferred for the schema.
    """
    metadata = schema.metadata or {}
    raw = metadata.get(b"ordering_keys")
    if raw:
        decoded = raw.decode("utf-8")
        parts = [part.strip() for part in decoded.split(",") if part.strip()]
        keys: list[OrderingKey] = []
        for part in parts:
            col, order = ([*part.split(":", maxsplit=1), "ascending"])[:2]
            keys.append((col.strip(), order.strip()))
        return tuple(keys)
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
    indices = pc.sort_indices(table, sort_keys=list(keys))
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
