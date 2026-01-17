"""Smoke test for incremental symbol-closure impact derivation."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from arrowdsl.io.parquet import write_dataset_parquet
from arrowdsl.schema.build import table_from_arrays
from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from incremental.impact import (
    changed_file_impact_table,
    impacted_callers_from_changed_exports,
    impacted_importers_from_changed_exports,
    import_closure_only_from_changed_exports,
    merge_impacted_files,
)
from incremental.registry_specs import dataset_schema
from incremental.state_store import StateStore
from incremental.types import IncrementalFileChanges


@pytest.mark.integration
def test_incremental_symbol_closure_smoke(tmp_path: Path) -> None:
    """Ensure impact strategies include expected caller/importer closures."""
    try:
        backend = build_backend(IbisBackendConfig(datafusion_profile=DataFusionRuntimeProfile()))
    except ImportError as exc:
        pytest.skip(str(exc))
    state_store = StateStore(root=tmp_path / "state")
    state_store.ensure_dirs()

    changed_exports = table_from_arrays(
        dataset_schema("inc_changed_exports_v1"),
        columns={
            "delta_kind": pa.array(["added"], type=pa.string()),
            "file_id": pa.array(["file_mod"], type=pa.string()),
            "path": pa.array(["src/mod.py"], type=pa.string()),
            "qname_id": pa.array(["qname_1"], type=pa.string()),
            "qname": pa.array(["pkg.mod.symbol"], type=pa.string()),
            "symbol": pa.array(["sym1"], type=pa.string()),
        },
        num_rows=1,
    )

    rel_callsite_qname = pa.table(
        {
            "call_id": ["call_1"],
            "qname_id": ["qname_1"],
            "path": ["src/caller.py"],
            "edge_owner_file_id": ["file_call"],
        }
    )
    rel_callsite_symbol = pa.table(
        {
            "call_id": ["call_2"],
            "symbol": ["sym1"],
            "path": ["src/caller_symbol.py"],
            "edge_owner_file_id": ["file_call_symbol"],
        }
    )
    imports_resolved = pa.table(
        {
            "importer_file_id": ["file_import", "file_module", "file_star"],
            "importer_path": ["src/import.py", "src/module_import.py", "src/star_import.py"],
            "imported_module_fqn": ["pkg.mod", "pkg.mod", "pkg.mod"],
            "imported_name": ["symbol", None, None],
            "is_star": [False, False, True],
        }
    )

    write_dataset_parquet(
        rel_callsite_qname,
        base_dir=state_store.dataset_dir("rel_callsite_qname_v1"),
    )
    write_dataset_parquet(
        rel_callsite_symbol,
        base_dir=state_store.dataset_dir("rel_callsite_symbol_v1"),
    )
    write_dataset_parquet(
        imports_resolved,
        base_dir=state_store.dataset_dir("py_imports_resolved_v1"),
    )

    callers = impacted_callers_from_changed_exports(
        backend=backend,
        changed_exports=changed_exports,
        prev_rel_callsite_qname=str(state_store.dataset_dir("rel_callsite_qname_v1")),
        prev_rel_callsite_symbol=str(state_store.dataset_dir("rel_callsite_symbol_v1")),
    )
    importers = impacted_importers_from_changed_exports(
        backend=backend,
        changed_exports=changed_exports,
        prev_imports_resolved=str(state_store.dataset_dir("py_imports_resolved_v1")),
    )
    module_importers = import_closure_only_from_changed_exports(
        backend=backend,
        changed_exports=changed_exports,
        prev_imports_resolved=str(state_store.dataset_dir("py_imports_resolved_v1")),
    )

    incremental_diff = pa.table(
        {
            "file_id": ["file_mod"],
            "path": ["src/mod.py"],
            "change_kind": ["modified"],
        }
    )
    base = changed_file_impact_table(
        file_changes=IncrementalFileChanges(changed_file_ids=("file_mod",)),
        incremental_diff=incremental_diff,
        repo_snapshot=None,
        scip_changed_file_ids=(),
    )

    symbol_closure = merge_impacted_files(
        base,
        callers,
        importers,
        module_importers,
        strategy="symbol_closure",
    )
    hybrid = merge_impacted_files(
        base,
        callers,
        importers,
        module_importers,
        strategy="hybrid",
    )

    symbol_ids = _file_ids(symbol_closure)
    hybrid_ids = _file_ids(hybrid)

    assert "file_mod" in symbol_ids
    assert "file_call" in symbol_ids
    assert "file_call_symbol" in symbol_ids
    assert "file_import" in symbol_ids
    assert "file_star" in symbol_ids
    assert "file_module" not in symbol_ids

    assert "file_module" in hybrid_ids
    assert symbol_ids.issubset(hybrid_ids)


def _file_ids(table: pa.Table) -> set[str]:
    values = table["file_id"]
    if isinstance(values, pa.ChunkedArray):
        values = values.combine_chunks()
    return {value for value in values.to_pylist() if isinstance(value, str)}
