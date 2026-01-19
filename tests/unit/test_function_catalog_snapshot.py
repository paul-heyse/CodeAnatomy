"""Unit tests for DataFusion function catalog snapshots."""

from __future__ import annotations

import pytest

from datafusion_engine.runtime import DataFusionRuntimeProfile

pytest.importorskip("datafusion")


def test_function_catalog_snapshot_sorted() -> None:
    """Sort function catalog snapshots deterministically."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    snapshot = profile.function_catalog_snapshot(ctx)
    assert snapshot
    names = [
        str(row.get("function_name")) for row in snapshot if row.get("function_name") is not None
    ]
    assert names == sorted(names)


def test_function_catalog_snapshot_routines_optional() -> None:
    """Include information_schema routines when enabled."""
    profile = DataFusionRuntimeProfile()
    ctx = profile.session_context()
    snapshot = profile.function_catalog_snapshot(ctx, include_routines=True)
    assert snapshot
    names = [
        str(row.get("function_name")) for row in snapshot if row.get("function_name") is not None
    ]
    assert names == sorted(names)
