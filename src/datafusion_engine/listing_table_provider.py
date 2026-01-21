"""Helpers for PyCapsule-backed ListingTable providers."""

from __future__ import annotations

import importlib
from collections.abc import Sequence
from dataclasses import dataclass

import pyarrow as pa


@dataclass(frozen=True)
class TableProviderCapsule:
    """Expose a PyCapsule as a DataFusion table provider."""

    capsule: object

    def datafusion_table_provider(self) -> object:
        """Return the wrapped provider capsule.

        Returns
        -------
        object
            PyCapsule provider used by DataFusion.
        """
        return self.capsule


_TABLE_PROVIDER_ATTR = "__datafusion_table_provider__"
setattr(
    TableProviderCapsule,
    _TABLE_PROVIDER_ATTR,
    TableProviderCapsule.datafusion_table_provider,
)


@dataclass(frozen=True)
class ParquetListingTableConfig:
    """Describe configuration for a parquet listing table provider."""

    path: str
    schema: object | None
    file_extension: str
    table_name: str
    table_definition: str | None
    table_partition_cols: Sequence[tuple[str, pa.DataType]] | None
    file_sort_order: Sequence[str] | None
    key_fields: Sequence[str] | None
    expr_adapter_factory: object | None
    parquet_pruning: bool | None
    skip_metadata: bool | None
    collect_statistics: bool | None


def _schema_ipc_payload(schema: pa.Schema | None) -> bytes | None:
    if schema is None:
        return None
    buffer = schema.serialize()
    return buffer.to_pybytes()


def _resolve_pyarrow_schema(schema: object | None) -> pa.Schema | None:
    if schema is None:
        return None
    if isinstance(schema, pa.Schema):
        return schema
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Listing table provider requires a pyarrow.Schema when schema is supplied."
    raise TypeError(msg)


def parquet_listing_table_provider(
    config: ParquetListingTableConfig,
) -> TableProviderCapsule | None:
    """Return a PyCapsule-backed Parquet ListingTable provider when available.

    Parameters
    ----------
    config:
        Listing table provider configuration.

    Returns
    -------
    TableProviderCapsule | None
        Listing table provider capsule wrapper, or ``None`` when unavailable.
    """
    try:
        module = importlib.import_module("datafusion_ext")
    except ImportError:  # pragma: no cover - optional dependency
        return None
    factory = getattr(module, "parquet_listing_table_provider", None)
    if not callable(factory):
        return None
    resolved_schema = _resolve_pyarrow_schema(config.schema)
    if resolved_schema is None:
        return None
    partition_schema = None
    if config.table_partition_cols:
        partition_schema = pa.schema(
            [pa.field(name, dtype, nullable=False) for name, dtype in config.table_partition_cols]
        )
    capsule = factory(
        path=config.path,
        file_extension=config.file_extension,
        table_name=config.table_name,
        table_definition=config.table_definition,
        schema_ipc=_schema_ipc_payload(resolved_schema),
        partition_schema_ipc=_schema_ipc_payload(partition_schema),
        file_sort_order=list(config.file_sort_order) if config.file_sort_order else None,
        key_fields=list(config.key_fields) if config.key_fields else None,
        expr_adapter_factory=config.expr_adapter_factory,
        parquet_pruning=config.parquet_pruning,
        skip_metadata=config.skip_metadata,
        collect_statistics=config.collect_statistics,
    )
    return TableProviderCapsule(capsule)


__all__ = [
    "ParquetListingTableConfig",
    "TableProviderCapsule",
    "parquet_listing_table_provider",
]
