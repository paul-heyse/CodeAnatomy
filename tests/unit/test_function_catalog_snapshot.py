"""Unit tests for DataFusion function catalog snapshots."""

from __future__ import annotations

from datafusion_engine.session.runtime_session import function_catalog_snapshot_for_profile
from tests.test_helpers.datafusion_runtime import df_profile
from tests.test_helpers.optional_deps import require_datafusion

require_datafusion()


def test_function_catalog_snapshot_sorted() -> None:
    """Sort function catalog snapshots deterministically."""
    profile = df_profile()
    ctx = profile.session_context()
    snapshot = function_catalog_snapshot_for_profile(profile, ctx)
    assert snapshot
    names = [
        str(row.get("function_name")) for row in snapshot if row.get("function_name") is not None
    ]
    assert names == sorted(names)


def test_function_catalog_snapshot_routines_optional() -> None:
    """Include information_schema routines when enabled."""
    profile = df_profile()
    ctx = profile.session_context()
    snapshot = function_catalog_snapshot_for_profile(
        profile,
        ctx,
        include_routines=True,
    )
    assert snapshot
    names = [
        str(row.get("function_name")) for row in snapshot if row.get("function_name") is not None
    ]
    assert names == sorted(names)
