"""Determinism checks for winner selection."""

from __future__ import annotations

import pyarrow as pa

from arrow_utils.core.ordering import OrderingLevel
from datafusion_engine.arrow_schema.build import rows_from_table, table_from_rows
from datafusion_engine.arrow_schema.metadata import ordering_metadata_spec
from datafusion_engine.kernels import WinnerSelectRequest, winner_select_kernel


def _ordered_schema() -> pa.Schema:
    base = pa.schema(
        [
            ("group", pa.string()),
            ("score", pa.float64()),
            ("path", pa.string()),
            ("bstart", pa.int64()),
        ]
    )
    return ordering_metadata_spec(
        OrderingLevel.EXPLICIT,
        keys=(("path", "ascending"), ("bstart", "ascending")),
    ).apply(base)


def _winner_map(table: pa.Table) -> dict[str, str]:
    return {
        str(row["group"]): str(row["path"])
        for row in rows_from_table(table)
        if row.get("group") is not None and row.get("path") is not None
    }


def test_winner_select_deterministic_across_reruns() -> None:
    """Select the same winners across input order permutations."""
    rows = [
        {"group": "g1", "score": 1.0, "path": "b", "bstart": 20},
        {"group": "g1", "score": 1.0, "path": "a", "bstart": 10},
        {"group": "g2", "score": 2.0, "path": "c", "bstart": 5},
        {"group": "g2", "score": 2.0, "path": "a", "bstart": 1},
    ]
    schema = _ordered_schema()
    table = table_from_rows(rows, schema=schema)
    reversed_table = table.take(pa.array(list(reversed(range(table.num_rows)))))

    winners = winner_select_kernel(
        table,
        request=WinnerSelectRequest(keys=("group",), score_col="score"),
    )
    winners_reordered = winner_select_kernel(
        reversed_table,
        request=WinnerSelectRequest(keys=("group",), score_col="score"),
    )

    expected = {"g1": "a", "g2": "a"}
    assert _winner_map(winners) == expected
    assert _winner_map(winners_reordered) == expected
