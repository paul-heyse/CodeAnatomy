"""Tests for Delta CDF port abstractions."""

from __future__ import annotations

from typing import cast

import pyarrow as pa

from datafusion_engine.delta.service import DeltaService
from semantics.incremental.delta_port import DeltaServiceCdfPort
from storage.deltalake import DeltaCdfOptions, StorageOptions

EXPECTED_TABLE_VERSION = 7


class _FakeService:
    @staticmethod
    def table_version(
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
    ) -> int:
        _ = path, storage_options, log_storage_options
        return EXPECTED_TABLE_VERSION

    @staticmethod
    def cdf_enabled(
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
    ) -> bool:
        _ = path, storage_options, log_storage_options
        return True

    @staticmethod
    def read_cdf_eager(
        *,
        table_path: str,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        cdf_options: DeltaCdfOptions | None = None,
    ) -> pa.Table:
        _ = table_path, storage_options, log_storage_options, cdf_options
        return pa.table({"_change_type": ["insert"]})


def test_delta_service_cdf_port_delegates_core_calls() -> None:
    """DeltaServiceCdfPort delegates table version/enabled/read operations."""
    port = DeltaServiceCdfPort(cast("DeltaService", _FakeService()))

    assert port.table_version("/tmp/table") == EXPECTED_TABLE_VERSION
    assert port.cdf_enabled("/tmp/table") is True
    table = port.read_cdf("/tmp/table", cdf_options=DeltaCdfOptions(starting_version=1))
    assert table.num_rows == 1
