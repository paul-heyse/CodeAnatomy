"""Delta CDF access port and default adapter implementations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

import pyarrow as pa

from datafusion_engine.arrow.coercion import coerce_table_to_storage, to_arrow_table
from datafusion_engine.arrow.interop import TableLike
from datafusion_engine.delta.service_protocol import DeltaServicePort
from storage.deltalake import DeltaCdfOptions, StorageOptions


class DeltaCdfPort(Protocol):
    """Port abstraction for Delta CDF operations used by incremental semantics."""

    def table_version(
        self,
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
    ) -> int | None:
        """Return latest table version for ``path`` when discoverable."""
        ...

    def cdf_enabled(
        self,
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
    ) -> bool:
        """Return whether CDF is enabled for ``path``."""
        ...

    def read_cdf(
        self,
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        cdf_options: DeltaCdfOptions | None = None,
    ) -> TableLike:
        """Read CDF rows for ``path`` using the active Delta adapter."""
        ...

    @staticmethod
    def fallback_read_cdf(
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        cdf_options: DeltaCdfOptions,
    ) -> pa.Table:
        """Read CDF rows via direct DeltaTable fallback path."""
        ...


@dataclass(frozen=True)
class DeltaServiceCdfPort:
    """Default Delta CDF port backed by ``DeltaService``."""

    service: DeltaServicePort

    def table_version(
        self,
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
    ) -> int | None:
        """Return latest Delta table version from backing service."""
        return self.service.table_version(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )

    def cdf_enabled(
        self,
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
    ) -> bool:
        """Return whether CDF is enabled from backing service metadata."""
        return self.service.cdf_enabled(
            path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
        )

    def read_cdf(
        self,
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        log_storage_options: StorageOptions | None = None,
        cdf_options: DeltaCdfOptions | None = None,
    ) -> TableLike:
        """Read CDF rows through ``DeltaService.read_cdf_eager``.

        Returns:
            TableLike: Eager CDF read payload from the Delta service.
        """
        return self.service.read_cdf_eager(
            table_path=path,
            storage_options=storage_options,
            log_storage_options=log_storage_options,
            cdf_options=cdf_options,
        )

    @staticmethod
    def fallback_read_cdf(
        path: str,
        *,
        storage_options: StorageOptions | None = None,
        cdf_options: DeltaCdfOptions,
    ) -> pa.Table:
        """Read CDF rows through direct ``deltalake.DeltaTable`` fallback.

        Returns:
            pa.Table: Canonical Arrow table returned from fallback CDF read.
        """
        from deltalake import DeltaTable

        normalized_storage = dict(storage_options) if storage_options else None
        table = DeltaTable(path, storage_options=normalized_storage)
        reader = table.load_cdf(
            starting_version=cdf_options.starting_version,
            ending_version=cdf_options.ending_version,
            starting_timestamp=cdf_options.starting_timestamp,
            ending_timestamp=cdf_options.ending_timestamp,
            columns=cdf_options.columns,
            predicate=cdf_options.predicate,
            allow_out_of_range=cdf_options.allow_out_of_range,
        )
        return coerce_table_to_storage(to_arrow_table(reader.read_all()))


__all__ = ["DeltaCdfPort", "DeltaServiceCdfPort"]
