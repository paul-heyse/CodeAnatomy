"""Tests for Delta CLI commands."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pytest

from cli.commands.delta import (
    DeltaCloneOptions,
    VacuumOptions,
    checkpoint_command,
    clone_delta_snapshot,
    vacuum_command,
)
from datafusion_engine.delta.service import DeltaService
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from tests.test_helpers.delta_seed import write_delta_table
from tests.test_helpers.optional_deps import (
    require_datafusion,
    require_delta_extension,
    require_deltalake,
)

require_datafusion()
require_deltalake()
require_delta_extension()

_DELTA_SERVICE = DeltaService(profile=DataFusionRuntimeProfile())

EXPECTED_ROW_COUNT = 2


def _seed_delta_table(tmp_path: Path) -> Path:
    table = pa.table({"id": [1, 2], "name": ["alpha", "beta"]})
    _profile, _ctx, path = write_delta_table(tmp_path, table=table)
    return path


def test_clone_delta_snapshot_round_trip(tmp_path: Path) -> None:
    """Ensure Delta snapshot cloning produces a valid target table."""
    source = _seed_delta_table(tmp_path)
    target = tmp_path / "clone"

    report = clone_delta_snapshot(str(source), str(target))

    assert report.rows == EXPECTED_ROW_COUNT
    assert report.schema_identity_hash
    assert _DELTA_SERVICE.table_version(path=str(target)) is not None


def test_clone_delta_snapshot_rejects_conflicting_targets() -> None:
    """Ensure clone rejects version+timestamp combinations."""
    options = DeltaCloneOptions(version=1, timestamp="2024-01-01T00:00:00Z")
    with pytest.raises(ValueError, match=r"Specify only one of version or timestamp\."):
        clone_delta_snapshot("src", "dest", options=options)


def test_vacuum_command_writes_report(tmp_path: Path) -> None:
    """Ensure vacuum command emits a JSON report."""
    source = _seed_delta_table(tmp_path)
    report_path = tmp_path / "vacuum.json"

    exit_code = vacuum_command(
        path=str(source),
        options=VacuumOptions(report_path=str(report_path)),
    )

    assert exit_code == 0
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["path"] == str(source)
    assert payload["vacuum"]["dry_run"] is True


def test_checkpoint_command_writes_report(tmp_path: Path) -> None:
    """Ensure checkpoint command emits a JSON report."""
    source = _seed_delta_table(tmp_path)
    report_path = tmp_path / "checkpoint.json"

    exit_code = checkpoint_command(path=str(source), report_path=str(report_path))

    assert exit_code == 0
    payload = json.loads(report_path.read_text(encoding="utf-8"))
    assert payload["path"] == str(source)
    assert payload["checkpoint"] is True
