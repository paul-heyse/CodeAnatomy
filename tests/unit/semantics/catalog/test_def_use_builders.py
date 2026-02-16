"""Tests for def-use analysis builder module."""

from __future__ import annotations

from datafusion import SessionContext

from semantics.catalog.def_use_builders import def_use_events_df_builder

EXPECTED_EVENT_ROWS = 2


def test_def_use_events_builder_classifies_def_and_use() -> None:
    """Def-use builder emits event rows with derived kind values."""
    ctx = SessionContext()
    ctx.from_pydict(
        {
            "code_unit_id": ["cu1", "cu1"],
            "instr_id": [1, 2],
            "offset": [0, 1],
            "opname": ["STORE_FAST", "LOAD_FAST"],
            "argval_str": ["x", "x"],
            "argrepr": ["x", "x"],
        },
        name="py_bc_instructions",
    )

    df = def_use_events_df_builder(ctx)
    rows = df.to_arrow_table().to_pylist()

    assert len(rows) == EXPECTED_EVENT_ROWS
    assert {row["kind"] for row in rows} == {"def", "use"}
