"""Tests for shared extraction runtime loop helpers."""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest

from extract.coordination.context import FileContext
from extract.coordination.extraction_runtime_loop import (
    ExtractionRuntimeWorklist,
    collect_runtime_rows,
    iter_runtime_row_batches,
    runtime_worklist_contexts,
)
from extract.infrastructure.worklists import WorklistRequest


def _file_ctx(name: str) -> FileContext:
    return FileContext(
        file_id=name,
        path=f"{name}.py",
        abs_path=f"/tmp/{name}.py",
        file_sha256=f"sha-{name}",
    )


def test_collect_runtime_rows_filters_none() -> None:
    """Collector should skip None rows from worker output."""
    contexts = [_file_ctx("a"), _file_ctx("b"), _file_ctx("c")]

    def _worker(file_ctx: FileContext) -> dict[str, object] | None:
        if file_ctx.file_id == "b":
            return None
        return {"file_id": file_ctx.file_id}

    rows = collect_runtime_rows(
        contexts,
        worker=_worker,
        parallel=False,
        max_workers=1,
    )

    assert rows == [{"file_id": "a"}, {"file_id": "c"}]


def test_iter_runtime_row_batches_chunks() -> None:
    """Batch iterator should emit deterministic fixed-size chunks."""
    rows = [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}, {"id": 5}]
    batches = list(iter_runtime_row_batches(rows, batch_size=2))
    assert batches == [
        [{"id": 1}, {"id": 2}],
        [{"id": 3}, {"id": 4}],
        [{"id": 5}],
    ]


def test_runtime_worklist_contexts_uses_queue_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Worklist resolver should wire queue_name when queue mode is enabled."""
    captured: dict[str, WorklistRequest] = {}
    expected = [_file_ctx("queued")]

    def _fake_queue_name(*, output_table: str, repo_id: str | None) -> str:
        return f"{output_table}:{repo_id or ''}"

    def _fake_iter(request: WorklistRequest) -> Iterator[FileContext]:
        captured["request"] = request
        yield from expected

    monkeypatch.setattr(
        "extract.coordination.extraction_runtime_loop.worklist_queue_name",
        _fake_queue_name,
    )
    monkeypatch.setattr(
        "extract.coordination.extraction_runtime_loop.iter_worklist_contexts",
        _fake_iter,
    )

    contexts = runtime_worklist_contexts(
        ExtractionRuntimeWorklist(
            repo_files=pa.table({"path": ["a.py"]}),
            output_table="ast_files_v1",
            runtime_profile=None,
            file_contexts=None,
            scope_manifest=None,
            use_worklist_queue=True,
            repo_id="repo-1",
        )
    )

    request = captured["request"]
    assert request.queue_name == "ast_files_v1:repo-1"
    assert contexts == expected
