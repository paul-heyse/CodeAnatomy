"""Tests for semantics.incremental.cdf_reader module."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.io.ingest import datafusion_from_arrow
from semantics.incremental.cdf_cursors import CdfCursorStore
from semantics.incremental.cdf_reader import CdfReadOptions, read_cdf_changes
from storage.deltalake import DeltaCdfOptions

LATEST_DELTA_VERSION = 5
EXPECTED_STARTING_VERSION = 3


def test_read_cdf_updates_cursor(tmp_path: Path) -> None:
    """read_cdf_changes updates cursor after a successful read."""
    store = CdfCursorStore(cursors_path=tmp_path / "cursors")
    dataset_name = "my_dataset"
    table = pa.table({"value": [1]})
    captured: dict[str, DeltaCdfOptions | DataFrame] = {}
    ctx = SessionContext()

    def fake_delta_cdf_enabled(
        _path: str,
        *,
        storage_options: object | None = None,
        log_storage_options: object | None = None,
    ) -> bool:
        _ = storage_options, log_storage_options
        return True

    def fake_delta_table_version(
        _path: str,
        *,
        storage_options: object | None = None,
        log_storage_options: object | None = None,
    ) -> int:
        _ = storage_options, log_storage_options
        return LATEST_DELTA_VERSION

    def fake_read_delta_cdf(
        _path: str,
        *,
        storage_options: object | None = None,
        log_storage_options: object | None = None,
        cdf_options: DeltaCdfOptions | None = None,
    ) -> pa.Table:
        _ = storage_options, log_storage_options
        if cdf_options is not None:
            captured["cdf_options"] = cdf_options
        return table

    def fake_arrow_to_df(_ctx: SessionContext, _table: pa.Table) -> DataFrame:
        df = datafusion_from_arrow(_ctx, name="cdf_updates", value=_table)
        captured["df"] = df
        return df

    result = read_cdf_changes(
        ctx,
        table_path="/tmp/fake-table",
        options=CdfReadOptions(
            cursor_store=store,
            dataset_name=dataset_name,
            delta_table_version_fn=fake_delta_table_version,
            delta_cdf_enabled_fn=fake_delta_cdf_enabled,
            read_delta_cdf_fn=fake_read_delta_cdf,
            arrow_table_to_dataframe_fn=fake_arrow_to_df,
        ),
    )

    assert result is not None
    cursor = store.load_cursor(dataset_name)
    assert cursor is not None
    assert cursor.last_version == LATEST_DELTA_VERSION
    assert cursor.last_timestamp is not None


def test_read_cdf_uses_cursor_start_version(tmp_path: Path) -> None:
    """read_cdf_changes starts from cursor version + 1."""
    store = CdfCursorStore(cursors_path=tmp_path / "cursors")
    dataset_name = "my_dataset"
    store.update_version(dataset_name, 2)
    table = pa.table({"value": [1]})
    captured: dict[str, DeltaCdfOptions] = {}
    ctx = SessionContext()

    def fake_delta_cdf_enabled(
        _path: str,
        *,
        storage_options: object | None = None,
        log_storage_options: object | None = None,
    ) -> bool:
        _ = storage_options, log_storage_options
        return True

    def fake_delta_table_version(
        _path: str,
        *,
        storage_options: object | None = None,
        log_storage_options: object | None = None,
    ) -> int:
        _ = storage_options, log_storage_options
        return LATEST_DELTA_VERSION

    def fake_read_delta_cdf(
        _path: str,
        *,
        storage_options: object | None = None,
        log_storage_options: object | None = None,
        cdf_options: DeltaCdfOptions | None = None,
    ) -> pa.Table:
        _ = storage_options, log_storage_options
        if cdf_options is not None:
            captured["cdf_options"] = cdf_options
        return table

    def fake_arrow_to_df(_ctx: SessionContext, _table: pa.Table) -> DataFrame:
        return datafusion_from_arrow(_ctx, name="cdf_updates", value=_table)

    result = read_cdf_changes(
        ctx,
        table_path="/tmp/fake-table",
        options=CdfReadOptions(
            cursor_store=store,
            dataset_name=dataset_name,
            delta_table_version_fn=fake_delta_table_version,
            delta_cdf_enabled_fn=fake_delta_cdf_enabled,
            read_delta_cdf_fn=fake_read_delta_cdf,
            arrow_table_to_dataframe_fn=fake_arrow_to_df,
        ),
    )

    assert result is not None
    cdf_options = captured.get("cdf_options")
    assert cdf_options is not None
    assert cdf_options.starting_version == EXPECTED_STARTING_VERSION
    cursor = store.load_cursor(dataset_name)
    assert cursor is not None
    assert cursor.last_version == LATEST_DELTA_VERSION
