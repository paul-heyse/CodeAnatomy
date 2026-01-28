"""Tests for git-backed repo blob extraction."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, cast

import pyarrow as pa
import pytest

from datafusion_engine.arrow_interop import RecordBatchReaderLike, TableLike
from extract.repo_blobs import RepoBlobOptions, scan_repo_blobs

pygit2 = pytest.importorskip("pygit2")

if TYPE_CHECKING:
    from pygit2 import Oid, Repository


def _rows_from_table(value: TableLike | RecordBatchReaderLike) -> list[dict[str, object]]:
    table = value.read_all() if isinstance(value, RecordBatchReaderLike) else value
    if not isinstance(table, pa.Table):
        msg = f"Unsupported table type: {type(table)}"
        raise TypeError(msg)
    arrow_table = cast("pa.Table", table)
    return arrow_table.to_pylist()


def _commit_file(repo: Repository, path: Path, content: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    repo.index.add(path.as_posix())
    repo.index.write()
    author = pygit2.Signature("Test User", "test@example.com")
    tree_id = repo.index.write_tree()
    parents: list[Oid] = []
    if not repo.head_is_unborn:
        parents = [cast("Oid", repo.head.target)]
    commit_id = repo.create_commit("HEAD", author, author, "commit", tree_id, parents)
    return str(commit_id)


def test_repo_blobs_source_ref_uses_committed_bytes(tmp_path: Path) -> None:
    """Ensure source_ref reads blob bytes from git, not the working tree."""
    repo = pygit2.init_repository(tmp_path, bare=False)
    repo_path = tmp_path / "file.py"
    commit_id = _commit_file(repo, repo_path, 'print("v1")\n')
    repo_path.write_text('print("v2")\n', encoding="utf-8")
    repo_files = pa.table(
        {
            "file_id": ["f1"],
            "path": ["file.py"],
            "abs_path": [str(repo_path)],
            "file_sha256": [None],
            "size_bytes": [None],
            "mtime_ns": [None],
        }
    )
    options = RepoBlobOptions(include_bytes=False, include_text=True, source_ref=commit_id)
    result = scan_repo_blobs(repo_files, options=options)
    rows = _rows_from_table(result)
    assert len(rows) == 1
    row = rows[0]
    assert row["text"] == 'print("v1")\n'
    assert row["encoding"] == "utf-8"


def test_repo_blobs_respects_size_limits_with_source_ref(tmp_path: Path) -> None:
    """Ensure max_file_bytes still filters rows when reading from git."""
    repo = pygit2.init_repository(tmp_path, bare=False)
    repo_path = tmp_path / "file.py"
    commit_id = _commit_file(repo, repo_path, 'print("large")\n')
    repo_files = pa.table(
        {
            "file_id": ["f1"],
            "path": ["file.py"],
            "abs_path": [str(repo_path)],
            "file_sha256": [None],
            "size_bytes": [None],
            "mtime_ns": [None],
        }
    )
    options = RepoBlobOptions(
        include_bytes=False,
        include_text=True,
        max_file_bytes=2,
        source_ref=commit_id,
    )
    result = scan_repo_blobs(repo_files, options=options)
    rows = _rows_from_table(result)
    assert rows == []
