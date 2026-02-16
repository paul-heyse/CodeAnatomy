"""Tests for diagnostic analysis builder module."""

from __future__ import annotations

from datafusion import SessionContext

from semantics.catalog.diagnostic_builders import diagnostics_df_builder


def test_diagnostics_builder_returns_diag_id() -> None:
    """Diagnostics builder emits deterministic diagnostic ids."""
    ctx = SessionContext()
    ctx.from_pydict(
        {
            "path": ["a.py"],
            "bstart": [1],
            "bend": [2],
        },
        name="ts_errors",
    )

    df = diagnostics_df_builder(ctx)

    assert "diag_id" in df.schema().names
