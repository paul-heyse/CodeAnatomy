"""Batch conversion helpers for extractors."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence

import pyarrow as pa

from arrowdsl.schema.build import table_from_rows


def record_batches_from_row_batches(
    schema: pa.Schema,
    row_batches: Iterable[Sequence[Mapping[str, object]]],
) -> Iterator[pa.RecordBatch]:
    """Yield record batches aligned to a schema from row batches.

    Parameters
    ----------
    schema:
        Target schema for alignment.
    row_batches:
        Iterable of row batches (list of row mappings).

    Yields
    ------
    pyarrow.RecordBatch
        Record batches aligned to the requested schema.
    """
    for batch in row_batches:
        if not batch:
            continue
        aligned = table_from_rows(schema, batch)
        extra = pa.Table.from_pylist(batch)
        for name in extra.column_names:
            if name in aligned.column_names:
                continue
            aligned = aligned.append_column(name, extra[name])
        yield from aligned.to_batches()


__all__ = ["record_batches_from_row_batches"]
