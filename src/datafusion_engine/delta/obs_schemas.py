"""Schema builders extracted from delta.observability."""

from __future__ import annotations

import pyarrow as pa

from datafusion_engine.delta.observability import (
    _delta_maintenance_schema,
    _delta_mutation_schema,
    _delta_scan_plan_schema,
    _delta_snapshot_schema,
)


def delta_snapshot_schema() -> pa.Schema:
    """Return the canonical Delta snapshot observability schema."""
    return _delta_snapshot_schema()


def delta_mutation_schema() -> pa.Schema:
    """Return the canonical Delta mutation observability schema."""
    return _delta_mutation_schema()


def delta_scan_plan_schema() -> pa.Schema:
    """Return the canonical Delta scan-plan observability schema."""
    return _delta_scan_plan_schema()


def delta_maintenance_schema() -> pa.Schema:
    """Return the canonical Delta maintenance observability schema."""
    return _delta_maintenance_schema()


__all__ = [
    "delta_maintenance_schema",
    "delta_mutation_schema",
    "delta_scan_plan_schema",
    "delta_snapshot_schema",
]
