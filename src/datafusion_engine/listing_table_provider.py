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

    def __datafusion_table_provider__(self) -> object:
        """Return the wrapped provider capsule."""
        return self.capsule


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
    *,
    path: str,
    schema: object | None,
    file_extension: str,
    table_name: str,
    table_definition: str | None,
    table_partition_cols: Sequence[tuple[str, pa.DataType]] | None,
    expr_adapter_factory: object | None,
    parquet_pruning: bool | None,
    skip_metadata: bool | None,
    collect_statistics: bool | None,
) -> TableProviderCapsule | None:
    """Return a PyCapsule-backed Parquet ListingTable provider when available.

    Parameters
    ----------
    path:
        Root path for the listing table.
    schema:
        Optional schema to use for the listing table.
    file_extension:
        File extension for listing table discovery.
    table_name:
        Table name for provenance metadata.
    table_definition:
        Optional CREATE EXTERNAL TABLE statement.
    table_partition_cols:
        Optional partition columns expressed as (name, dtype) tuples.
    expr_adapter_factory:
        Optional physical expression adapter factory capsule.
    parquet_pruning:
        Optional parquet pruning flag for the listing table provider.
    skip_metadata:
        Optional parquet metadata skipping flag for the listing table provider.
    collect_statistics:
        Optional statistics collection flag for the listing table provider.

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
    resolved_schema = _resolve_pyarrow_schema(schema)
    if resolved_schema is None:
        return None
    partition_schema = None
    if table_partition_cols:
        partition_schema = pa.schema(
            [pa.field(name, dtype, nullable=False) for name, dtype in table_partition_cols]
        )
    capsule = factory(
        path=path,
        file_extension=file_extension,
        table_name=table_name,
        table_definition=table_definition,
        schema_ipc=_schema_ipc_payload(resolved_schema),
        partition_schema_ipc=_schema_ipc_payload(partition_schema),
        expr_adapter_factory=expr_adapter_factory,
        parquet_pruning=parquet_pruning,
        skip_metadata=skip_metadata,
        collect_statistics=collect_statistics,
    )
    return TableProviderCapsule(capsule)


__all__ = ["TableProviderCapsule", "parquet_listing_table_provider"]
