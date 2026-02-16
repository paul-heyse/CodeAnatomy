"""Tests for CFG analysis builder module."""

from __future__ import annotations

from datafusion import SessionContext

from semantics.catalog.cfg_builders import cfg_blocks_df_builder


def test_cfg_blocks_builder_adds_span() -> None:
    """CFG block builder enriches rows with span struct."""
    ctx = SessionContext()
    ctx.from_pydict(
        {
            "code_unit_id": ["cu1"],
            "start_offset": [5],
            "end_offset": [10],
        },
        name="py_bc_blocks",
    )
    ctx.from_pydict(
        {
            "code_unit_id": ["cu1"],
            "file_id": ["f1"],
            "path": ["a.py"],
        },
        name="py_bc_code_units",
    )

    df = cfg_blocks_df_builder(ctx)

    assert "span" in df.schema().names
