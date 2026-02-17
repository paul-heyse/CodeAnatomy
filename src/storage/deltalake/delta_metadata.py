"""Delta Lake metadata/snapshot helpers."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa

from datafusion_engine.arrow.interop import coerce_arrow_schema
from obs.otel import SCOPE_STORAGE, stage_span
from storage.deltalake.delta_read import (
    DeltaSchemaRequest,
    SnapshotKey,
    _open_delta_table,
)
from storage.deltalake.delta_runtime_ops import _storage_span_attributes


def canonical_table_uri(table_uri: str) -> str:
    """Resolve canonical Delta table URI.

    Returns:
    -------
    str
        Canonicalized table URI string.
    """
    raw = str(table_uri).strip()
    parsed = urlparse(raw)
    if not parsed.scheme:
        return str(Path(raw).expanduser().resolve())
    scheme = parsed.scheme.lower()
    if scheme in {"s3a", "s3n"}:
        scheme = "s3"
    netloc = parsed.netloc
    if scheme in {"s3", "gs", "az", "abfs", "abfss", "http", "https"}:
        netloc = netloc.lower()
    path = parsed.path or ""
    if netloc and path and not path.startswith("/"):
        path = f"/{path}"
    return parsed._replace(scheme=scheme, netloc=netloc, path=path).geturl()


def delta_table_schema(request: DeltaSchemaRequest) -> pa.Schema | None:
    """Resolve Delta table schema snapshot.

    Returns:
    -------
    pa.Schema | None
        Resolved table schema when available.
    """
    attrs = _storage_span_attributes(
        operation="metadata",
        table_path=request.path,
        extra={"codeanatomy.metadata_kind": "schema"},
    )
    with stage_span(
        "storage.metadata",
        stage="storage",
        scope_name=SCOPE_STORAGE,
        attributes=attrs,
    ) as span:
        table = _open_delta_table(
            path=request.path,
            storage_options=request.storage_options,
            log_storage_options=request.log_storage_options,
            version=request.version,
            timestamp=request.timestamp,
        )
        if table is None:
            return None
        resolved_schema = coerce_arrow_schema(table.schema())
        if resolved_schema is not None:
            span.set_attribute("codeanatomy.schema_columns", len(resolved_schema))
        return resolved_schema


def snapshot_key_for_table(table_uri: str, version: int) -> SnapshotKey:
    """Resolve table snapshot key payload.

    Returns:
    -------
    SnapshotKey
        Canonical snapshot identity for the table/version pair.
    """
    return SnapshotKey(canonical_uri=canonical_table_uri(table_uri), version=int(version))


__all__ = [
    "DeltaSchemaRequest",
    "canonical_table_uri",
    "delta_table_schema",
    "snapshot_key_for_table",
]
