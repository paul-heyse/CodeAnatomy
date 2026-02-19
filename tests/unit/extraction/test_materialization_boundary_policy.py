"""Materialization boundary tests for extraction stage output routing."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from extraction import orchestrator as orchestrator_mod

if TYPE_CHECKING:
    from pyarrow import Table


def test_materialize_stage_output_routes_to_delta(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Delta mode should route stage materialization through _write_delta."""
    captured: dict[str, object] = {}

    def _write_delta(
        _table: Table,
        location: Path,
        name: str,
        *,
        write_ctx: object | None = None,
    ) -> str:
        captured["name"] = name
        captured["location"] = location
        captured["write_ctx"] = write_ctx
        return str(location)

    monkeypatch.setattr(orchestrator_mod, "_write_delta", _write_delta)

    result = orchestrator_mod._materialize_stage_output(  # noqa: SLF001
        table=pa.table({"x": [1]}),
        location=tmp_path / "ast",
        name="ast",
        mode="delta",
    )

    assert result == str(tmp_path / "ast")
    assert captured["name"] == "ast"


def test_materialize_stage_output_copy_mode_routes_to_copy_sink(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """COPY mode should route stage materialization through COPY sink helper."""
    called = {"count": 0}

    def _write_datafusion_copy(
        _table: Table,
        location: Path,
        name: str,
        *,
        write_ctx: object | None = None,
    ) -> str:
        _ = name
        _ = write_ctx
        called["count"] += 1
        return str(location)

    monkeypatch.setattr(
        orchestrator_mod,
        "_write_datafusion_copy",
        _write_datafusion_copy,
    )

    result = orchestrator_mod._materialize_stage_output(  # noqa: SLF001
        table=pa.table({"x": [1]}),
        location=tmp_path / "imports",
        name="python_imports",
        mode="datafusion_copy",
    )

    assert result == str(tmp_path / "imports")
    assert called["count"] == 1


def test_materialize_stage_output_rejects_unknown_mode(tmp_path: Path) -> None:
    """Unsupported materialization mode should raise ValueError."""
    with pytest.raises(ValueError, match="materialization_mode must be either"):
        orchestrator_mod._materialize_stage_output(  # noqa: SLF001
            table=pa.table({"x": [1]}),
            location=tmp_path / "x",
            name="x",
            mode="unknown",
        )


def test_load_delta_table_reads_parquet_when_delta_log_missing(tmp_path: Path) -> None:
    """Parquet fallback path should be used for COPY-mode stage locations."""
    table = pa.table({"x": [1, 2]})
    parquet_path = tmp_path / "copy_stage"
    parquet_path.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, parquet_path / "part-0.parquet")

    loaded = orchestrator_mod._load_delta_table(str(parquet_path))  # noqa: SLF001

    assert loaded is not None
    assert loaded.to_pydict() == {"x": [1, 2]}
