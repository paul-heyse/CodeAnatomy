"""DataFusion plan wrapper with Arrow metadata helpers."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast, overload

import pyarrow as pa
from datafusion.dataframe import DataFrame

from arrow_utils.core.interop import RecordBatchReaderLike, TableLike
from arrow_utils.core.ordering import Ordering, OrderingLevel
from arrow_utils.schema.metadata import SchemaMetadataSpec, ordering_metadata_spec

if TYPE_CHECKING:
    from datafusion_engine.view_artifacts import DataFusionViewArtifact


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
class DataFusionPlan:
    """DataFusion-backed plan with ordering metadata."""

    df: DataFrame
    ordering: Ordering = field(default_factory=Ordering.unordered)
    artifact: DataFusionViewArtifact | None = None

    @property
    def expr(self) -> DataFrame:
        """Expose the plan DataFrame for compatibility.

        Returns
        -------
        datafusion.dataframe.DataFrame
            DataFusion DataFrame for the plan.
        """
        return self.df

    def to_table(self) -> TableLike:
        """Materialize the plan to an Arrow table.

        Returns
        -------
        arrowdsl.core.interop.TableLike
            Arrow table for the plan result.
        """
        table = self.df.to_arrow_table()
        return _apply_metadata_spec(table, metadata_spec=_ordering_spec(self.ordering))

    def to_reader(self, *, batch_size: int | None = None) -> RecordBatchReaderLike:
        """Return a RecordBatchReader for the plan.

        Returns
        -------
        arrowdsl.core.interop.RecordBatchReaderLike
            RecordBatchReader for the plan result.
        """
        if batch_size is None:
            table = self.df.to_arrow_table()
            reader = pa.RecordBatchReader.from_batches(table.schema, table.to_batches())
            return _apply_metadata_spec(reader, metadata_spec=_ordering_spec(self.ordering))
        stream = cast("RecordBatchReaderLike", self.df.execute_stream())
        return _apply_metadata_spec(stream, metadata_spec=_ordering_spec(self.ordering))

    @contextmanager
    def cache(self) -> Iterator[DataFusionPlan]:
        """Yield a cached plan scoped to the context.

        Yields
        ------
        DataFusionPlan
            Cached plan instance.
        """
        yield self


__all__ = ["DataFusionPlan"]
