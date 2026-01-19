"""Tests for Delta metadata snapshots."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from storage.deltalake import (
    DeltaWriteOptions,
    delta_commit_metadata,
    delta_history_snapshot,
    delta_protocol_snapshot,
    write_table_delta,
)


def _write_delta_table(path: Path) -> None:
    table = pa.table({"id": [1, 2], "value": ["alpha", "beta"]})
    options = DeltaWriteOptions(
        mode="overwrite",
        commit_metadata={"source": "unit_test"},
    )
    write_table_delta(table, str(path), options=options)


def test_delta_commit_metadata_roundtrip(tmp_path: Path) -> None:
    """Persist and recover custom Delta commit metadata."""
    path = tmp_path / "table"
    _write_delta_table(path)
    metadata = delta_commit_metadata(str(path))
    assert metadata is not None
    assert metadata.get("source") == "unit_test"


def test_delta_history_and_protocol_snapshot(tmp_path: Path) -> None:
    """Capture Delta history and protocol snapshots after a write."""
    path = tmp_path / "table"
    _write_delta_table(path)
    history = delta_history_snapshot(str(path))
    assert history is not None
    assert "version" in history
    protocol = delta_protocol_snapshot(str(path))
    assert protocol is not None
    assert protocol["min_reader_version"] is not None
    assert protocol["min_writer_version"] is not None
