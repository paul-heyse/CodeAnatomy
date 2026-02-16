"""Unit tests for DeltaAccessContext storage resolution."""

from __future__ import annotations

from typing import Any

import pytest

from semantics.incremental.delta_context import DeltaAccessContext, DeltaStorageOptions


class _RuntimeStub:
    dataset_resolver = object()

    @staticmethod
    def delta_store_policy() -> dict[str, str]:
        return {"policy": "active"}


def test_resolve_storage_prefers_policy_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    """DeltaAccessContext.resolve_storage applies runtime store policy."""

    def _resolve_delta_store_policy(
        *,
        table_uri: str,
        policy: Any,
        storage_options: Any,
        log_storage_options: Any,
    ) -> tuple[dict[str, str], dict[str, str]]:
        _ = table_uri, policy, storage_options, log_storage_options
        return ({"resolved": "yes"}, {"log": "yes"})

    from semantics.incremental import delta_context as module

    monkeypatch.setattr(module, "resolve_delta_store_policy", _resolve_delta_store_policy)

    context = DeltaAccessContext(
        runtime=_RuntimeStub(),
        storage=DeltaStorageOptions(storage_options={"in": "1"}),
    )

    resolved = context.resolve_storage(table_uri="s3://bucket/table")

    assert resolved.storage_options == {"resolved": "yes"}
    assert resolved.log_storage_options == {"log": "yes"}
