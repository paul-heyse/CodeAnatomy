"""Tests for transaction API delegation to control-plane bridges."""

from __future__ import annotations

import msgspec
import pytest
from datafusion import SessionContext

from datafusion_engine.delta import transactions
from datafusion_engine.delta.control_plane_core import (
    DeltaDeleteRequest,
    DeltaMergeRequest,
    DeltaUpdateRequest,
    DeltaWriteRequest,
)


def test_transactions_delegate_to_control_plane(monkeypatch: pytest.MonkeyPatch) -> None:
    """Transaction helpers delegate to control-plane operations."""
    monkeypatch.setattr(transactions, "delta_write_ipc", lambda _ctx, **_kwargs: {"op": "write"})
    monkeypatch.setattr(transactions, "delta_delete", lambda _ctx, **_kwargs: {"op": "delete"})
    monkeypatch.setattr(transactions, "delta_update", lambda _ctx, **_kwargs: {"op": "update"})
    monkeypatch.setattr(transactions, "delta_merge", lambda _ctx, **_kwargs: {"op": "merge"})

    ctx = SessionContext()
    write_request = DeltaWriteRequest(
        table_uri="s3://bucket/table",
        storage_options=None,
        version=None,
        timestamp=None,
        data_ipc=msgspec.Raw(b""),
        mode="append",
        schema_mode=None,
        partition_columns=None,
        target_file_size=None,
        extra_constraints=None,
    )
    delete_request = DeltaDeleteRequest(
        table_uri="s3://bucket/table",
        storage_options=None,
        version=None,
        timestamp=None,
        predicate=None,
        extra_constraints=None,
    )
    update_request = DeltaUpdateRequest(
        table_uri="s3://bucket/table",
        storage_options=None,
        version=None,
        timestamp=None,
        predicate=None,
        updates={},
        extra_constraints=None,
    )
    merge_request = DeltaMergeRequest(
        table_uri="s3://bucket/table",
        storage_options=None,
        version=None,
        timestamp=None,
        source_table="source",
        predicate="id = source.id",
        source_alias=None,
        target_alias=None,
        matched_predicate=None,
        matched_updates={},
        not_matched_predicate=None,
        not_matched_inserts={},
        not_matched_by_source_predicate=None,
        delete_not_matched_by_source=False,
        extra_constraints=None,
    )

    assert transactions.write_transaction(ctx, request=write_request)["op"] == "write"
    assert transactions.delete_transaction(ctx, request=delete_request)["op"] == "delete"
    assert transactions.update_transaction(ctx, request=update_request)["op"] == "update"
    assert transactions.merge_transaction(ctx, request=merge_request)["op"] == "merge"
