"""Tests for delete predicate safety guards."""

from __future__ import annotations

import pytest
from datafusion import SessionContext

from datafusion_engine.delta.control_plane_core import DeltaDeleteRequest
from datafusion_engine.delta.control_plane_mutation import delta_delete
from storage.deltalake.delta_read import DeltaDeleteWhereRequest
from storage.deltalake.delta_write import delta_delete_where


def test_delta_delete_rejects_empty_predicate() -> None:
    """delta_delete should reject empty predicates before calling the engine."""
    ctx = SessionContext()
    request = DeltaDeleteRequest(
        table_uri="s3://bucket/table",
        storage_options=None,
        version=None,
        timestamp=None,
        predicate="   ",
        extra_constraints=None,
    )

    with pytest.raises(ValueError, match="delta_delete requires a non-empty predicate"):
        delta_delete(ctx, request=request)


def test_delta_delete_where_rejects_empty_predicate() -> None:
    """delta_delete_where should reject empty predicates before mutation setup."""
    ctx = SessionContext()
    request = DeltaDeleteWhereRequest(
        path="s3://bucket/table",
        predicate="",
    )

    with pytest.raises(ValueError, match="delta_delete_where requires a non-empty predicate"):
        delta_delete_where(ctx, request=request)
