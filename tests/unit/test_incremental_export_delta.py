"""Unit tests for incremental export delta computation."""

from __future__ import annotations

from importlib import import_module
from pathlib import Path

import pyarrow as pa
import pytest
from ibis.backends import BaseBackend

from arrowdsl.schema.build import rows_from_table
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from incremental.deltas import compute_changed_exports
from incremental.registry_specs import dataset_schema
from storage.deltalake import DeltaWriteOptions, write_table_delta
from tests.utils import values_as_list


def test_compute_changed_exports_added_removed(tmp_path: Path) -> None:
    """Only changed files should emit added/removed export rows."""
    backend = _build_backend_or_skip()
    prev_exports = pa.table(
        {
            "file_id": pa.array(["file_1", "file_1", "file_2"], type=pa.string()),
            "path": pa.array(["src/a.py", "src/a.py", "src/b.py"], type=pa.string()),
            "qname_id": pa.array(["q1", "q2", "q3"], type=pa.string()),
            "qname": pa.array(["pkg.a.foo", "pkg.a.bar", "pkg.b.baz"], type=pa.string()),
            "symbol": pa.array(["sym1", "sym2", "sym3"], type=pa.string()),
        }
    )
    curr_exports = pa.table(
        {
            "file_id": pa.array(["file_1", "file_1", "file_2"], type=pa.string()),
            "path": pa.array(["src/a.py", "src/a.py", "src/b.py"], type=pa.string()),
            "qname_id": pa.array(["q2", "q4", "q3"], type=pa.string()),
            "qname": pa.array(["pkg.a.bar", "pkg.a.baz", "pkg.b.baz"], type=pa.string()),
            "symbol": pa.array(["sym2", "sym4", "sym3"], type=pa.string()),
        }
    )
    prev_path = tmp_path / "prev_exports"
    write_table_delta(
        prev_exports,
        str(prev_path),
        options=DeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
    )

    changed_files = pa.table({"file_id": pa.array(["file_1"], type=pa.string())})
    result = compute_changed_exports(
        backend=backend,
        prev_exports=str(prev_path),
        curr_exports=curr_exports,
        changed_files=changed_files,
    )

    rows = {(row["delta_kind"], row["qname_id"]) for row in rows_from_table(result)}
    assert rows == {("added", "q4"), ("removed", "q1")}
    assert _result_files(result) == {"file_1"}
    assert result.schema == dataset_schema("inc_changed_exports_v1")


def _build_backend_or_skip() -> BaseBackend:
    try:
        module = import_module("datafusion_engine.runtime")
    except ImportError as exc:
        pytest.skip(str(exc))
    profile = module.DataFusionRuntimeProfile()
    return build_backend(IbisBackendConfig(datafusion_profile=profile))


def _result_files(result: pa.Table) -> set[str]:
    values = result["file_id"]
    return {value for value in values_as_list(values) if isinstance(value, str)}
