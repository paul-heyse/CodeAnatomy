"""Tests for canonical CDF core orchestration."""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa

from semantics.incremental.cdf_core import CanonicalCdfReadRequest, read_cdf_table
from storage.deltalake import DeltaCdfOptions, StorageOptions

START_VERSION_SUCCESS = 3
END_VERSION_FROM_PORT = 5


@dataclass
class _FakePort:
    table: pa.Table
    enabled: bool = True
    version: int | None = 5
    raise_on_read: bool = False
    fallback_calls: int = 0

    def table_version(
        self,
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
    ) -> int | None:
        _ = path, storage_options, log_storage_options
        return self.version

    def cdf_enabled(
        self,
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
    ) -> bool:
        _ = path, storage_options, log_storage_options
        return self.enabled

    def read_cdf(
        self,
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        cdf_options: DeltaCdfOptions | None = None,
    ) -> pa.Table:
        _ = path, storage_options, log_storage_options, cdf_options
        if self.raise_on_read:
            msg = "simulate provider read failure"
            raise ValueError(msg)
        return self.table

    def fallback_read_cdf(
        self,
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        cdf_options: DeltaCdfOptions,
    ) -> pa.Table:
        _ = path, storage_options, cdf_options
        self.fallback_calls += 1
        return self.table


def test_read_cdf_table_success() -> None:
    """read_cdf_table returns canonical result on direct read success."""
    port = _FakePort(table=pa.table({"_change_type": ["insert"]}))

    result = read_cdf_table(
        request=CanonicalCdfReadRequest(table_path="/tmp/table", start_version=3),
        port=port,
    )

    assert result is not None
    assert result.start_version == START_VERSION_SUCCESS
    assert result.end_version == END_VERSION_FROM_PORT
    assert result.has_changes


def test_read_cdf_table_uses_fallback_on_value_error() -> None:
    """read_cdf_table uses fallback read path on ValueError."""
    port = _FakePort(table=pa.table({"_change_type": ["insert"]}), raise_on_read=True)

    result = read_cdf_table(
        request=CanonicalCdfReadRequest(table_path="/tmp/table", start_version=1),
        port=port,
    )

    assert result is not None
    assert port.fallback_calls == 1
