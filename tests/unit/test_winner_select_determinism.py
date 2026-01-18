"""Determinism checks for winner selection."""

from __future__ import annotations

import pyarrow as pa

from arrowdsl.compute.kernels import winner_select_by_score
from arrowdsl.core.context import OrderingLevel
from arrowdsl.schema.metadata import ordering_metadata_spec


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
    return {row["group"]: row["path"] for row in table.to_pylist()}


def test_winner_select_deterministic_across_reruns() -> None:
    """Select the same winners across input order permutations."""
    rows = [
        {"group": "g1", "score": 1.0, "path": "b", "bstart": 20},
        {"group": "g1", "score": 1.0, "path": "a", "bstart": 10},
        {"group": "g2", "score": 2.0, "path": "c", "bstart": 5},
        {"group": "g2", "score": 2.0, "path": "a", "bstart": 1},
    ]
    schema = _ordered_schema()
    table = pa.Table.from_pylist(rows, schema=schema)
    reversed_table = table.take(pa.array(list(reversed(range(table.num_rows)))))

    winners = winner_select_by_score(table, keys=("group",), score_col="score")
    winners_reordered = winner_select_by_score(
        reversed_table,
        keys=("group",),
        score_col="score",
    )

    expected = {"g1": "a", "g2": "a"}
    assert _winner_map(winners) == expected
    assert _winner_map(winners_reordered) == expected
