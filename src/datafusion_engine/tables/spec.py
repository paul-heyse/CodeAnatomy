"""Unified table specification for DataFusion providers.

This module provides canonical table specifications that consolidate
schema, storage, and Delta version information for deterministic
table registration and execution.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa

from utils.hashing import hash_sha256_hex

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DataFusionProvider, DatasetLocation
    from schema_spec.system import DataFusionScanOptions, DatasetSpec


@dataclass(frozen=True)
class TableSpec:
    """Unified table specification for DataFusion table providers.

    Consolidates all information needed to register a deterministic
    table provider with the SessionContext.

    Parameters
    ----------
    name
        Logical table name for registration.
    schema
        PyArrow schema for the table.
    storage_location
        Path to the table data (local or cloud).
    datafusion_scan
        Optional DataFusion scan overrides (listing/cache behavior).
    datafusion_provider
        Optional DataFusion provider override (listing vs delta CDF).
    format
        Storage format (e.g., 'delta', 'parquet', 'csv').
    delta_version
        Optional pinned Delta version for deterministic reads.
    delta_timestamp
        Optional Delta timestamp for time-travel queries.
    required_udfs
        UDF names required for queries against this table.
    partition_columns
        Partition column names.
    storage_options
        Key-value storage options for cloud access.
    table_properties
        Delta table properties or format-specific metadata.
    """

    name: str
    schema: pa.Schema
    storage_location: str
    dataset_spec: DatasetSpec | None = None
    datafusion_scan: DataFusionScanOptions | None = None
    datafusion_provider: DataFusionProvider | None = None
    format: str = "delta"
    delta_version: int | None = None
    delta_timestamp: str | None = None
    required_udfs: tuple[str, ...] = ()
    partition_columns: tuple[str, ...] = ()
    storage_options: Mapping[str, str] = field(default_factory=dict)
    table_properties: Mapping[str, str] = field(default_factory=dict)

    def with_delta_version(self, version: int) -> TableSpec:
        """Return a new TableSpec with the given Delta version pinned.

        Parameters
        ----------
        version
            Delta version to pin.

        Returns
        -------
        TableSpec
            New TableSpec with version pinned.
        """
        return TableSpec(
            name=self.name,
            schema=self.schema,
            dataset_spec=self.dataset_spec,
            datafusion_scan=self.datafusion_scan,
            datafusion_provider=self.datafusion_provider,
            storage_location=self.storage_location,
            format=self.format,
            delta_version=version,
            delta_timestamp=None,
            required_udfs=self.required_udfs,
            partition_columns=self.partition_columns,
            storage_options=self.storage_options,
            table_properties=self.table_properties,
        )

    def cache_key(self) -> str:
        """Return a stable cache key for this table specification.

        Returns
        -------
        str
            Stable identifier for caching and comparison.
        """
        from serde_msgspec import dumps_json, to_builtins

        payload = {
            "name": self.name,
            "storage_location": self.storage_location,
            "format": self.format,
            "delta_version": self.delta_version,
            "delta_timestamp": self.delta_timestamp,
            "required_udfs": list(self.required_udfs),
            "partition_columns": list(self.partition_columns),
        }
        raw = dumps_json(to_builtins(payload))
        return hash_sha256_hex(raw, length=16)


def table_spec_from_location(
    name: str,
    location: DatasetLocation,
    *,
    schema: pa.Schema | None = None,
    required_udfs: Sequence[str] = (),
) -> TableSpec:
    """Build a TableSpec from a DatasetLocation.

    Parameters
    ----------
    name
        Logical table name.
    location
        Dataset location with path and format metadata.
    schema
        Optional schema override. If None, will need to be inferred.
    required_udfs
        UDF names required for queries against this table.

    Returns
    -------
    TableSpec
        Unified table specification.

    Raises
    ------
    ValueError
        When schema is required but not provided.
    """
    if schema is None:
        msg = f"Schema required for TableSpec: {name}"
        raise ValueError(msg)

    resolved = location.resolved
    return TableSpec(
        name=name,
        schema=schema,
        dataset_spec=location.dataset_spec,
        datafusion_scan=resolved.datafusion_scan,
        datafusion_provider=resolved.datafusion_provider,
        storage_location=str(location.path),
        format=location.format or "delta",
        delta_version=location.delta_version,
        delta_timestamp=location.delta_timestamp,
        required_udfs=tuple(required_udfs),
        partition_columns=(),
        storage_options=dict(location.storage_options),
        table_properties={},
    )


__all__ = [
    "TableSpec",
    "table_spec_from_location",
]
