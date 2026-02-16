"""Unit tests for incremental metadata artifact writers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from semantics.incremental import metadata as module
from semantics.incremental.metadata import ArtifactWriteContext


class _RuntimeStub:
    @staticmethod
    def delta_store_policy() -> None:
        return None


def test_write_named_artifact_uses_shared_delta_writer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """_write_named_artifact delegates through write_delta_table_via_pipeline."""
    captured: dict[str, object] = {}

    def _resolve_delta_store_policy(
        *,
        table_uri: str,
        policy: Any,
        storage_options: Any,
        log_storage_options: Any,
    ) -> tuple[dict[str, str], dict[str, str]]:
        _ = table_uri, policy, storage_options, log_storage_options
        return ({"s": "1"}, {"l": "1"})

    def _write_delta_table_via_pipeline(*, runtime: Any, table: pa.Table, request: Any) -> None:
        captured["runtime"] = runtime
        captured["table"] = table
        captured["request"] = request

    monkeypatch.setattr(module, "resolve_delta_store_policy", _resolve_delta_store_policy)
    monkeypatch.setattr(module, "write_delta_table_via_pipeline", _write_delta_table_via_pipeline)

    context = ArtifactWriteContext(runtime=_RuntimeStub())
    path = tmp_path / "artifact"

    write_named_artifact = module.__dict__["_write_named_artifact"]
    written = write_named_artifact(
        name="artifact_name",
        path=path,
        table=pa.table({"value": [1]}),
        context=context,
    )

    assert written == str(path)
    request = captured["request"]
    assert request.commit_metadata == {"artifact_name": "artifact_name"}


def test_write_artifact_rows_delegates_to_shared_writer(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """_write_artifact_rows routes through _write_named_artifact."""
    calls: list[tuple[str, str]] = []

    def _write_named_artifact(
        *,
        name: str,
        path: Path,
        table: pa.Table,
        context: ArtifactWriteContext,
        operation_prefix: str = "incremental_artifact",
    ) -> str:
        _ = table, context, operation_prefix
        calls.append((name, str(path)))
        return str(path)

    monkeypatch.setattr(module, "_write_named_artifact", _write_named_artifact)

    context = ArtifactWriteContext(runtime=_RuntimeStub())
    write_artifact_rows = module.__dict__["_write_artifact_rows"]
    result = write_artifact_rows(
        name="artifact_rows",
        path=tmp_path / "rows",
        rows=[{"k": "v"}],
        context=context,
    )

    assert result == str(tmp_path / "rows")
    assert calls == [("artifact_rows", str(tmp_path / "rows"))]
