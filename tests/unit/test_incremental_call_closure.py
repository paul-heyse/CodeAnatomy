"""Unit tests for incremental callsite and import closures."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from arrowdsl.schema.build import rows_from_table
from ibis_engine.sources import IbisDeltaWriteOptions
from incremental.impact import (
    impacted_callers_from_changed_exports,
    impacted_importers_from_changed_exports,
    import_closure_only_from_changed_exports,
)
from incremental.registry_specs import dataset_schema
from incremental.runtime import IncrementalRuntime
from tests.utils import values_as_list, write_delta_table


def test_impacted_callers_from_changed_exports(tmp_path: Path) -> None:
    """Callsite closures should attribute callers by edge_owner_file_id."""
    runtime = _build_runtime_or_skip()
    changed_exports = _changed_exports()

    qname_path = _write_table(
        tmp_path / "rel_callsite_qname",
        pa.table(
            {
                "call_id": pa.array(["call_1"], type=pa.string()),
                "qname_id": pa.array(["qname_1"], type=pa.string()),
                "path": pa.array(["src/caller.py"], type=pa.string()),
                "edge_owner_file_id": pa.array(["file_call"], type=pa.string()),
            }
        ),
    )
    symbol_path = _write_table(
        tmp_path / "rel_callsite_symbol",
        pa.table(
            {
                "call_id": pa.array(["call_2"], type=pa.string()),
                "symbol": pa.array(["sym1"], type=pa.string()),
                "path": pa.array(["src/caller_symbol.py"], type=pa.string()),
                "edge_owner_file_id": pa.array(["file_call_symbol"], type=pa.string()),
            }
        ),
    )

    impacted = impacted_callers_from_changed_exports(
        runtime=runtime,
        changed_exports=changed_exports,
        prev_rel_callsite_qname=qname_path,
        prev_rel_callsite_symbol=symbol_path,
    )

    reasons = _reason_kinds(impacted)
    assert reasons == {
        ("file_call", "callsite_qname"),
        ("file_call_symbol", "callsite_symbol"),
    }


def test_impacted_importers_from_changed_exports(tmp_path: Path) -> None:
    """Import closures should include name and star importers."""
    runtime = _build_runtime_or_skip()
    changed_exports = _changed_exports()

    imports_path = _write_table(
        tmp_path / "imports_resolved",
        pa.table(
            {
                "importer_file_id": pa.array(
                    ["file_import", "file_module", "file_star"], type=pa.string()
                ),
                "importer_path": pa.array(
                    ["src/import.py", "src/module.py", "src/star.py"], type=pa.string()
                ),
                "imported_module_fqn": pa.array(
                    ["pkg.mod", "pkg.mod", "pkg.mod"], type=pa.string()
                ),
                "imported_name": pa.array(["foo", None, None], type=pa.string()),
                "is_star": pa.array([False, False, True], type=pa.bool_()),
            }
        ),
    )

    impacted = impacted_importers_from_changed_exports(
        runtime=runtime,
        changed_exports=changed_exports,
        prev_imports_resolved=imports_path,
    )
    closure_only = import_closure_only_from_changed_exports(
        runtime=runtime,
        changed_exports=changed_exports,
        prev_imports_resolved=imports_path,
    )

    assert _result_files(impacted) == {"file_import", "file_star"}
    assert _result_files(closure_only) == {"file_module", "file_star"}


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


def _changed_exports() -> pa.Table:
    schema = dataset_schema("inc_changed_exports_v1")
    return pa.table(
        {
            "delta_kind": pa.array(["added"], type=pa.string()),
            "file_id": pa.array(["file_mod"], type=pa.string()),
            "path": pa.array(["src/mod.py"], type=pa.string()),
            "qname_id": pa.array(["qname_1"], type=pa.string()),
            "qname": pa.array(["pkg.mod.foo"], type=pa.string()),
            "symbol": pa.array(["sym1"], type=pa.string()),
        },
        schema=schema,
    )


def _write_table(path: Path, table: pa.Table) -> str:
    write_delta_table(
        table,
        str(path),
        options=IbisDeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
    )
    return str(path)


def _result_files(result: pa.Table) -> set[str]:
    values = result["file_id"]
    return {value for value in values_as_list(values) if isinstance(value, str)}


def _reason_kinds(result: pa.Table) -> set[tuple[str, str]]:
    rows = []
    for row in rows_from_table(result):
        file_id = row.get("file_id")
        reason_kind = row.get("reason_kind")
        if isinstance(file_id, str) and isinstance(reason_kind, str):
            rows.append((file_id, reason_kind))
    return set(rows)
