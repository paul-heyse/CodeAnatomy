"""Arrow ingestion helpers for DataFusion contexts."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import cast

import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from arrowdsl.core.interop import RecordBatchReaderLike, coerce_table_like
from arrowdsl.schema.build import table_from_row_dicts
from datafusion_engine.introspection import invalidate_introspection_cache


@dataclass(frozen=True)
class ArrowIngestEvent:
    """Arrow ingest event payload for diagnostics."""

    name: str
    method: str
    partitioning: str | None
    batch_size: int | None
    batch_count: int | None
    row_count: int | None


def datafusion_from_arrow(
    ctx: SessionContext,
    *,
    name: str,
    value: object,
    batch_size: int | None = None,
    ingest_hook: Callable[[Mapping[str, object]], None] | None = None,
) -> DataFrame:
    """Register Arrow-like input and return a DataFusion DataFrame.

    Returns
    -------
    datafusion.dataframe.DataFrame
        DataFrame for the registered table.
    """
    if _is_pydict_input(value):
        pydict = cast("Mapping[str, object]", value)
        return _ingest_pydict(
            ctx,
            name=name,
            pydict=pydict,
            batch_size=batch_size,
            ingest_hook=ingest_hook,
        )
    if _is_row_mapping_sequence(value):
        rows = cast("Sequence[Mapping[str, object]]", value)
        value = table_from_row_dicts(rows)
    table = coerce_table_like(value, requested_schema=None)
    return _ingest_table(
        ctx,
        name=name,
        table=table,
        batch_size=batch_size,
        ingest_hook=ingest_hook,
    )


def _ingest_pydict(
    ctx: SessionContext,
    *,
    name: str,
    pydict: Mapping[str, object],
    batch_size: int | None,
    ingest_hook: Callable[[Mapping[str, object]], None] | None,
) -> DataFrame:
    from_pydict = getattr(ctx, "from_pydict", None)
    if callable(from_pydict) and batch_size is None:
        df = cast("DataFrame", from_pydict(dict(pydict), name=name))
        _emit_arrow_ingest(
            ingest_hook,
            ArrowIngestEvent(
                name=name,
                method="from_pydict",
                partitioning="datafusion_native",
                batch_size=None,
                batch_count=None,
                row_count=None,
            ),
        )
        return df
    table = pa.Table.from_pydict(dict(pydict))
    batches = _record_batches(table, batch_size=batch_size)
    df = _register_record_batches(ctx, name=name, batches=batches)
    _emit_arrow_ingest(
        ingest_hook,
        ArrowIngestEvent(
            name=name,
            method="record_batches",
            partitioning="record_batches",
            batch_size=batch_size,
            batch_count=len(batches),
            row_count=table.num_rows,
        ),
    )
    return df


def _ingest_table(
    ctx: SessionContext,
    *,
    name: str,
    table: RecordBatchReaderLike | pa.Table,
    batch_size: int | None,
    ingest_hook: Callable[[Mapping[str, object]], None] | None,
) -> DataFrame:
    from_arrow = getattr(ctx, "from_arrow", None)
    if callable(from_arrow) and batch_size is None:
        try:
            df = cast("DataFrame", from_arrow(table, name=name))
        except (RuntimeError, TypeError, ValueError):
            df = None
        else:
            if df is not None:
                _emit_arrow_ingest(
                    ingest_hook,
                    ArrowIngestEvent(
                        name=name,
                        method="from_arrow",
                        partitioning="datafusion_native",
                        batch_size=None,
                        batch_count=None,
                        row_count=None,
                    ),
                )
                return df
    if isinstance(table, RecordBatchReaderLike):
        table = table.read_all()
    if not isinstance(table, pa.Table):
        msg = "Unsupported Arrow input for DataFusion ingestion."
        raise TypeError(msg)
    table_obj = cast("pa.Table", table)
    batches = _record_batches(table_obj, batch_size=batch_size)
    df = _register_record_batches(ctx, name=name, batches=batches)
    _emit_arrow_ingest(
        ingest_hook,
        ArrowIngestEvent(
            name=name,
            method="record_batches",
            partitioning="record_batches",
            batch_size=batch_size,
            batch_count=len(batches),
            row_count=table_obj.num_rows,
        ),
    )
    return df


def _register_record_batches(
    ctx: SessionContext,
    *,
    name: str,
    batches: list[list[pa.RecordBatch]],
) -> DataFrame:
    register_batches = getattr(ctx, "register_record_batches", None)
    if not callable(register_batches):
        msg = "DataFusion SessionContext missing register_record_batches."
        raise TypeError(msg)
    register_batches(name, batches)
    invalidate_introspection_cache(ctx)
    return ctx.table(name)


def _record_batches(table: pa.Table, *, batch_size: int | None) -> list[list[pa.RecordBatch]]:
    if batch_size is None or batch_size <= 0:
        return [table.to_batches()]
    batches = table.to_batches(max_chunksize=batch_size)
    return [batches]


def _is_pydict_input(value: object) -> bool:
    if not isinstance(value, Mapping):
        return False
    try:
        return all(
            isinstance(key, str) and isinstance(col, Sequence) and not isinstance(col, (str, bytes))
            for key, col in value.items()
        )
    except (AttributeError, TypeError):
        return False


def _is_row_mapping_sequence(value: object) -> bool:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return False
    if not value:
        return False
    return all(isinstance(item, Mapping) for item in value)


def _emit_arrow_ingest(
    ingest_hook: Callable[[Mapping[str, object]], None] | None,
    event: ArrowIngestEvent,
) -> None:
    if ingest_hook is None:
        return
    ingest_hook(
        {
            "name": event.name,
            "method": event.method,
            "partitioning": event.partitioning,
            "batch_size": event.batch_size,
            "batch_count": event.batch_count,
            "row_count": event.row_count,
        }
    )


__all__ = ["ArrowIngestEvent", "datafusion_from_arrow"]
