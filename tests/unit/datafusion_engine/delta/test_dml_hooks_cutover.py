"""Tests for DF52 DML request entrypoint cutover."""

from __future__ import annotations

from collections.abc import Callable

import msgspec
import pytest
from datafusion import SessionContext

from datafusion_engine.delta import control_plane_mutation
from datafusion_engine.delta.control_plane_types import (
    DeltaDeleteRequest,
    DeltaMergeRequest,
    DeltaUpdateRequest,
)


def test_delete_update_merge_use_df52_request_entrypoints(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Delete/update/merge paths target canonical request entrypoints."""
    ctx = SessionContext()
    requested: list[str] = []
    payloads: dict[str, bytes] = {}

    def _resolve(name: str) -> Callable[[object, bytes], dict[str, object]]:
        requested.append(name)

        def _entrypoint(_ctx: object, payload: bytes) -> dict[str, object]:
            payloads[name] = payload
            return {"operation": name}

        return _entrypoint

    monkeypatch.setattr(control_plane_mutation, "_require_internal_entrypoint", _resolve)
    monkeypatch.setattr(control_plane_mutation, "_internal_ctx", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        control_plane_mutation,
        "provider_supports_native_dml",
        lambda *_args, **_kwargs: True,
    )

    delete_response = control_plane_mutation.delta_delete(
        ctx,
        request=DeltaDeleteRequest(
            table_uri="s3://bucket/table",
            storage_options=None,
            version=3,
            timestamp=None,
            predicate="id > 0",
            extra_constraints=None,
            gate=None,
            commit_options=None,
        ),
    )
    update_response = control_plane_mutation.delta_update(
        ctx,
        request=DeltaUpdateRequest(
            table_uri="s3://bucket/table",
            storage_options=None,
            version=3,
            timestamp=None,
            predicate="id > 0",
            updates={"status": "'ok'"},
            extra_constraints=None,
            gate=None,
            commit_options=None,
        ),
    )
    merge_response = control_plane_mutation.delta_merge(
        ctx,
        request=DeltaMergeRequest(
            table_uri="s3://bucket/table",
            storage_options=None,
            version=3,
            timestamp=None,
            source_table="source_view",
            predicate="target.id = source.id",
            source_alias="source",
            target_alias="target",
            matched_predicate=None,
            matched_updates={"status": "source.status"},
            not_matched_predicate=None,
            not_matched_inserts={},
            not_matched_by_source_predicate=None,
            delete_not_matched_by_source=False,
            extra_constraints=None,
            gate=None,
            commit_options=None,
        ),
    )

    assert requested == [
        "delta_delete_request",
        "delta_update_request",
        "delta_merge_request",
    ]
    assert delete_response["operation"] == "delta_delete_request"
    assert update_response["operation"] == "delta_update_request"
    assert merge_response["operation"] == "delta_merge_request"

    delete_payload = msgspec.msgpack.decode(payloads["delta_delete_request"], type=dict)
    update_payload = msgspec.msgpack.decode(payloads["delta_update_request"], type=dict)
    merge_payload = msgspec.msgpack.decode(payloads["delta_merge_request"], type=dict)

    assert delete_payload["predicate"] == "id > 0"
    assert update_payload["updates"] == {"status": "'ok'"}
    assert merge_payload["source_table"] == "source_view"
