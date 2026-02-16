"""Tests for CQ run metadata and artifact naming."""

from __future__ import annotations

from pathlib import Path

import msgspec
from tools.cq.core.artifacts import save_artifact_json
from tools.cq.core.schema import CqResult, RunMeta, mk_runmeta, ms


def _toolchain_dict() -> dict[str, str | None]:
    return {"rg": None, "sgpy": "0.40.0", "python": "3.13.11"}


def test_mk_runmeta_generates_run_id(tmp_path: Path) -> None:
    """Test mk runmeta generates run id."""
    run = mk_runmeta("q", [], str(tmp_path), ms(), _toolchain_dict())
    assert run.run_id
    assert isinstance(run.run_id, str)
    assert run.run_uuid_version is not None
    assert run.run_created_ms is not None


def test_runmeta_serializes_run_id(tmp_path: Path) -> None:
    """Test runmeta serializes run id."""
    run = RunMeta(
        macro="q",
        argv=[],
        root=str(tmp_path),
        started_ms=1.0,
        elapsed_ms=2.0,
        toolchain=_toolchain_dict(),
        run_id="run-123",
    )
    payload = msgspec.json.encode(run)
    restored = msgspec.json.decode(payload, type=RunMeta)
    assert restored.run_id == "run-123"


def test_artifact_filename_includes_run_id(tmp_path: Path) -> None:
    """Test artifact filename includes run id."""
    run = RunMeta(
        macro="q",
        argv=[],
        root=str(tmp_path),
        started_ms=1.0,
        elapsed_ms=2.0,
        toolchain=_toolchain_dict(),
        run_id="run-456",
    )
    result = CqResult(run=run)
    artifact = save_artifact_json(result)
    assert "run-456" in artifact.path
