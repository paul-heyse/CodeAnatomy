"""Unit tests for incremental export delta computation."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from arrowdsl.schema.build import rows_from_table
from ibis_engine.sources import IbisDeltaWriteOptions
from incremental.deltas import compute_changed_exports
from incremental.registry_specs import dataset_schema
from incremental.runtime import IncrementalRuntime, TempTableRegistry
from tests.utils import values_as_list, write_delta_table


def test_compute_changed_exports_added_removed(tmp_path: Path) -> None:
    """Only changed files should emit added/removed export rows."""
    runtime = _build_runtime_or_skip()
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
    write_delta_table(
        prev_exports,
        str(prev_path),
        options=IbisDeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
    )

    changed_files = pa.table({"file_id": pa.array(["file_1"], type=pa.string())})
    result = compute_changed_exports(
        runtime=runtime,
        prev_exports=str(prev_path),
        curr_exports=curr_exports,
        changed_files=changed_files,
    )

    rows = {(row["delta_kind"], row["qname_id"]) for row in rows_from_table(result)}
    assert rows == {("added", "q4"), ("removed", "q1")}
    assert _result_files(result) == {"file_1"}
    assert result.schema == dataset_schema("inc_changed_exports_v1")


def test_compute_changed_exports_sql_parity(tmp_path: Path) -> None:
    """Match Ibis export deltas with equivalent DataFusion SQL."""
    runtime = _build_runtime_or_skip()
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
    write_delta_table(
        prev_exports,
        str(prev_path),
        options=IbisDeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
    )
    changed_files = pa.table({"file_id": pa.array(["file_1"], type=pa.string())})

    ibis_result = compute_changed_exports(
        runtime=runtime,
        prev_exports=str(prev_path),
        curr_exports=curr_exports,
        changed_files=changed_files,
    )
    ctx = runtime.session_context()
    with TempTableRegistry(ctx) as registry:
        prev_name = registry.register_table(prev_exports, prefix="prev_exports")
        curr_name = registry.register_table(curr_exports, prefix="curr_exports")
        changed_name = registry.register_table(changed_files, prefix="changed_files")
        sql = f"""
            WITH prev_f AS (
                SELECT p.* FROM {prev_name} AS p
                INNER JOIN {changed_name} AS c ON p.file_id = c.file_id
            ),
            curr_f AS (
                SELECT c.* FROM {curr_name} AS c
                INNER JOIN {changed_name} AS f ON c.file_id = f.file_id
            ),
            added AS (
                SELECT
                    'added' AS delta_kind,
                    curr_f.file_id,
                    curr_f.path,
                    curr_f.qname_id,
                    curr_f.qname,
                    curr_f.symbol
                FROM curr_f
                LEFT JOIN prev_f
                ON curr_f.file_id = prev_f.file_id
                    AND curr_f.qname_id = prev_f.qname_id
                    AND curr_f.symbol = prev_f.symbol
                WHERE prev_f.file_id IS NULL
            ),
            removed AS (
                SELECT
                    'removed' AS delta_kind,
                    prev_f.file_id,
                    prev_f.path,
                    prev_f.qname_id,
                    prev_f.qname,
                    prev_f.symbol
                FROM prev_f
                LEFT JOIN curr_f
                ON prev_f.file_id = curr_f.file_id
                    AND prev_f.qname_id = curr_f.qname_id
                    AND prev_f.symbol = curr_f.symbol
                WHERE curr_f.file_id IS NULL
            )
            SELECT * FROM added
            UNION ALL
            SELECT * FROM removed
        """
        sql_result = ctx.sql(sql).to_arrow_table()

    assert _sorted_rows(ibis_result) == _sorted_rows(sql_result)


def _build_runtime_or_skip() -> IncrementalRuntime:
    try:
        runtime = IncrementalRuntime.build()
        _ = runtime.ibis_backend()
    except ImportError as exc:
        pytest.skip(str(exc))
    else:
        return runtime
    msg = "Incremental runtime unavailable."
    raise RuntimeError(msg)


def _result_files(result: pa.Table) -> set[str]:
    values = result["file_id"]
    return {value for value in values_as_list(values) if isinstance(value, str)}


def _sorted_rows(table: pa.Table) -> list[tuple[object, ...]]:
    rows = table.to_pylist()
    keys = ["delta_kind", "file_id", "path", "qname_id", "qname", "symbol"]
    return sorted(tuple(row.get(key) for key in keys) for row in rows)
