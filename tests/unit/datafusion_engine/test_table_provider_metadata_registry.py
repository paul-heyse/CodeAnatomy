# ruff: noqa: D100, D103
from __future__ import annotations

from datafusion import SessionContext

from datafusion_engine.tables.metadata import (
    TableProviderMetadata,
    all_table_provider_metadata,
    clear_table_provider_metadata,
    record_table_provider_metadata,
    table_provider_metadata,
)


def test_table_provider_metadata_is_scoped_by_context() -> None:
    ctx_one = SessionContext()
    ctx_two = SessionContext()

    meta_one = TableProviderMetadata(table_name="alpha", file_format="delta")
    meta_two = TableProviderMetadata(table_name="alpha", file_format="parquet")

    record_table_provider_metadata(ctx_one, metadata=meta_one)
    record_table_provider_metadata(ctx_two, metadata=meta_two)

    assert table_provider_metadata(ctx_one, table_name="alpha") == meta_one
    assert table_provider_metadata(ctx_two, table_name="alpha") == meta_two


def test_clear_table_provider_metadata_only_clears_target_context() -> None:
    ctx_one = SessionContext()
    ctx_two = SessionContext()

    record_table_provider_metadata(
        ctx_one,
        metadata=TableProviderMetadata(table_name="alpha", file_format="delta"),
    )
    record_table_provider_metadata(
        ctx_two,
        metadata=TableProviderMetadata(table_name="beta", file_format="delta"),
    )

    clear_table_provider_metadata(ctx_one)

    assert all_table_provider_metadata(ctx_one) == {}
    assert "beta" in all_table_provider_metadata(ctx_two)
