"""Observability table management extracted from delta.observability."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.delta.observability import (
    _bootstrap_observability_table,
    _ensure_observability_schema,
    _ensure_observability_table,
)

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile


def ensure_observability_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    name: str,
    schema: pa.Schema,
) -> DatasetLocation | None:
    """Ensure observability table exists and is schema-compatible.

    Returns:
        DatasetLocation | None: Resolved table location when registration succeeds.
    """
    return _ensure_observability_table(
        ctx,
        profile,
        name=name,
        schema=schema,
    )


def ensure_observability_schema(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    table_path: Path,
    schema: pa.Schema,
    operation: str,
) -> bool:
    """Ensure registered table schema aligns with expected observability schema.

    Returns:
        bool: True when schema is aligned or has been reconciled.
    """
    return _ensure_observability_schema(
        ctx,
        profile,
        table_path=table_path,
        schema=schema,
        operation=operation,
    )


def bootstrap_observability_table(
    ctx: SessionContext,
    profile: DataFusionRuntimeProfile,
    *,
    table_path: Path,
    schema: pa.Schema,
    operation: str,
) -> None:
    """Bootstrap the observability Delta table at the provided path."""
    _bootstrap_observability_table(
        ctx,
        profile,
        table_path=table_path,
        schema=schema,
        operation=operation,
    )


__all__ = [
    "bootstrap_observability_table",
    "ensure_observability_schema",
    "ensure_observability_table",
]
