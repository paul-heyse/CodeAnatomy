"""Integration tests for incremental Delta file pruning."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa

from ibis_engine.sources import IbisDeltaWriteOptions
from incremental.pruning import FileScopePolicy, prune_delta_files
from tests.utils import write_delta_table

EXPECTED_TOTAL_FILES = 2
EXPECTED_CANDIDATES = 1


def test_prune_delta_files_by_file_id(tmp_path: Path) -> None:
    """Prune Delta file list to the requested file_id scope."""
    dataset_path = tmp_path / "dataset"
    table = pa.table({"file_id": ["a", "b"], "value": [1, 2]})
    write_delta_table(
        table,
        str(dataset_path),
        options=IbisDeltaWriteOptions(
            mode="overwrite",
            schema_mode="overwrite",
            partition_by=("file_id",),
        ),
    )

    result = prune_delta_files(
        dataset_path,
        FileScopePolicy(file_id_column="file_id", file_ids=("a",)),
    )
    assert result.candidate_count == EXPECTED_CANDIDATES
    assert result.pruned_count == EXPECTED_TOTAL_FILES - EXPECTED_CANDIDATES
    assert result.total_files == EXPECTED_TOTAL_FILES
    for path in result.candidate_paths:
        assert "file_id=a" in path
