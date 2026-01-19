"""Tests for DataFusion Arrow ingestion helpers."""

from __future__ import annotations

from collections.abc import Callable, Mapping

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.bridge import datafusion_from_arrow

TWO_ROWS = 2
THREE_ROWS = 3


def _ingest_payloads() -> tuple[list[Mapping[str, object]], Callable[[Mapping[str, object]], None]]:
    payloads: list[Mapping[str, object]] = []

    def _hook(payload: Mapping[str, object]) -> None:
        payloads.append(dict(payload))

    return payloads, _hook


def test_datafusion_from_arrow_table() -> None:
    """Ingest Arrow tables via the unified helper."""
    ctx = SessionContext()
    table = pa.table({"id": [1, 2], "label": ["a", "b"]})
    payloads, hook = _ingest_payloads()
    df = datafusion_from_arrow(ctx, name="arrow_table", value=table, ingest_hook=hook)
    result = df.to_arrow_table()
    assert result.num_rows == table.num_rows
    assert payloads
    method = payloads[0].get("method")
    if callable(getattr(ctx, "from_arrow", None)):
        assert method == "from_arrow"
    else:
        assert method == "record_batches"


def test_datafusion_from_arrow_pydict() -> None:
    """Ingest columnar dicts via the unified helper."""
    ctx = SessionContext()
    payloads, hook = _ingest_payloads()
    df = datafusion_from_arrow(
        ctx,
        name="pydict_table",
        value={"id": [1, 2], "label": ["x", "y"]},
        ingest_hook=hook,
    )
    assert df.to_arrow_table().num_rows == TWO_ROWS
    assert payloads
    method = payloads[0].get("method")
    if callable(getattr(ctx, "from_pydict", None)):
        assert method == "from_pydict"
    else:
        assert method == "record_batches"


def test_datafusion_from_arrow_row_mappings() -> None:
    """Ingest row-wise dicts via the unified helper."""
    ctx = SessionContext()
    payloads, hook = _ingest_payloads()
    df = datafusion_from_arrow(
        ctx,
        name="row_table",
        value=[{"id": 1, "label": "x"}, {"id": 2, "label": "y"}],
        ingest_hook=hook,
    )
    assert df.to_arrow_table().num_rows == TWO_ROWS
    assert payloads
    method = payloads[0].get("method")
    if callable(getattr(ctx, "from_arrow", None)):
        assert method == "from_arrow"
    else:
        assert method == "record_batches"


def test_datafusion_record_batch_partitioning_payload() -> None:
    """Record batch partitioning details for memtable ingestion."""
    ctx = SessionContext()
    table = pa.table({"id": [1, 2, 3], "label": ["a", "b", "c"]})
    payloads, hook = _ingest_payloads()
    df = datafusion_from_arrow(
        ctx,
        name="partitioned_table",
        value=table,
        batch_size=1,
        ingest_hook=hook,
    )
    assert df.to_arrow_table().num_rows == table.num_rows
    assert payloads
    payload = payloads[0]
    assert payload.get("method") == "record_batches"
    assert payload.get("partitioning") == "record_batches"
    assert payload.get("batch_count") == THREE_ROWS
    assert payload.get("row_count") == table.num_rows
