"""Ibis plan wrapper with Arrow metadata helpers."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, field
from typing import cast, overload

import pyarrow as pa
from ibis.expr.types import Scalar, Table, Value

from arrowdsl.core.context import Ordering, OrderingLevel
from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.schema.metadata import ordering_metadata_spec
from arrowdsl.schema.schema import SchemaMetadataSpec


@overload
def _apply_metadata_spec(
    result: TableLike,
    *,
    metadata_spec: SchemaMetadataSpec | None,
) -> TableLike: ...


@overload
def _apply_metadata_spec(
    result: RecordBatchReaderLike,
    *,
    metadata_spec: SchemaMetadataSpec | None,
) -> RecordBatchReaderLike: ...


def _apply_metadata_spec(
    result: TableLike | RecordBatchReaderLike,
    *,
    metadata_spec: SchemaMetadataSpec | None,
) -> TableLike | RecordBatchReaderLike:
    if metadata_spec is None:
        return result
    if not metadata_spec.schema_metadata and not metadata_spec.field_metadata:
        return result
    schema = metadata_spec.apply(result.schema)
    if isinstance(result, pa.RecordBatchReader):
        return pa.RecordBatchReader.from_batches(schema, result)
    table = cast("TableLike", result)
    return table.cast(schema)


def _ordering_spec(ordering: Ordering) -> SchemaMetadataSpec | None:
    if ordering.level == OrderingLevel.UNORDERED:
        return None
    return ordering_metadata_spec(ordering.level, keys=ordering.keys)


@dataclass(frozen=True)
class IbisPlan:
    """Ibis-backed plan with ordering metadata."""

    expr: Table
    ordering: Ordering = field(default_factory=Ordering.unordered)

    def to_table(self, *, params: Mapping[Value, object] | None = None) -> TableLike:
        """Materialize the plan to an Arrow table.

        Returns
        -------
        TableLike
            Arrow table with ordering metadata applied when available.
        """
        table = self.expr.to_pyarrow(
            params=cast("Mapping[Scalar, object] | None", params),
        )
        return _apply_metadata_spec(table, metadata_spec=_ordering_spec(self.ordering))

    def to_reader(
        self,
        *,
        batch_size: int | None = None,
        params: Mapping[Value, object] | None = None,
    ) -> RecordBatchReaderLike:
        """Return a RecordBatchReader for the plan.

        Returns
        -------
        RecordBatchReaderLike
            RecordBatchReader with ordering metadata applied when available.
        """
        if batch_size is None:
            reader = self.expr.to_pyarrow_batches(params=params)
        else:
            reader = self.expr.to_pyarrow_batches(chunk_size=batch_size, params=params)
        return _apply_metadata_spec(reader, metadata_spec=_ordering_spec(self.ordering))

    @contextmanager
    def cache(self) -> Iterator[IbisPlan]:
        """Yield a cached plan scoped to the context.

        Yields
        ------
        IbisPlan
            Cached plan context for repeated evaluation.
        """
        cached = self.expr.cache()
        if hasattr(cached, "__enter__") and hasattr(cached, "__exit__"):
            with cast("AbstractContextManager[Table]", cached) as cached_table:
                yield IbisPlan(expr=cached_table, ordering=self.ordering)
        else:
            yield IbisPlan(expr=cached, ordering=self.ordering)
