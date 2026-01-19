"""Smoke test for incremental symbol-closure impact derivation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pytest

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
from storage.deltalake import DeltaWriteOptions, write_dataset_delta
from tests.utils import values_as_list


@dataclass(frozen=True)
class _SymbolClosureSources:
    changed_exports: pa.Table
    rel_callsite_qname: pa.Table
    rel_callsite_symbol: pa.Table
    imports_resolved: pa.Table


def _build_symbol_closure_sources() -> _SymbolClosureSources:
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
    return _SymbolClosureSources(
        changed_exports=changed_exports,
        rel_callsite_qname=rel_callsite_qname,
        rel_callsite_symbol=rel_callsite_symbol,
        imports_resolved=imports_resolved,
    )


def _write_symbol_closure_sources(
    state_store: StateStore,
    *,
    sources: _SymbolClosureSources,
    options: DeltaWriteOptions,
) -> dict[str, str]:
    rel_callsite_qname_path = str(state_store.dataset_dir("rel_callsite_qname_v1"))
    rel_callsite_symbol_path = str(state_store.dataset_dir("rel_callsite_symbol_v1"))
    imports_resolved_path = str(state_store.dataset_dir("py_imports_resolved_v1"))
    write_dataset_delta(sources.rel_callsite_qname, rel_callsite_qname_path, options=options)
    write_dataset_delta(sources.rel_callsite_symbol, rel_callsite_symbol_path, options=options)
    write_dataset_delta(sources.imports_resolved, imports_resolved_path, options=options)
    return {
        "rel_callsite_qname_v1": rel_callsite_qname_path,
        "rel_callsite_symbol_v1": rel_callsite_symbol_path,
        "py_imports_resolved_v1": imports_resolved_path,
    }


def _assert_symbol_closure_results(symbol_closure: pa.Table, hybrid: pa.Table) -> None:
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


@pytest.mark.integration
def test_incremental_symbol_closure_smoke(tmp_path: Path) -> None:
    """Ensure impact strategies include expected caller/importer closures."""
    try:
        backend = build_backend(IbisBackendConfig(datafusion_profile=DataFusionRuntimeProfile()))
    except ImportError as exc:
        pytest.skip(str(exc))
    state_store = StateStore(root=tmp_path / "state")
    state_store.ensure_dirs()
    delta_options = DeltaWriteOptions(mode="overwrite", schema_mode="overwrite")
    sources = _build_symbol_closure_sources()
    paths = _write_symbol_closure_sources(state_store, sources=sources, options=delta_options)

    callers = impacted_callers_from_changed_exports(
        backend=backend,
        changed_exports=sources.changed_exports,
        prev_rel_callsite_qname=paths["rel_callsite_qname_v1"],
        prev_rel_callsite_symbol=paths["rel_callsite_symbol_v1"],
    )
    importers = impacted_importers_from_changed_exports(
        backend=backend,
        changed_exports=sources.changed_exports,
        prev_imports_resolved=paths["py_imports_resolved_v1"],
    )
    module_importers = import_closure_only_from_changed_exports(
        backend=backend,
        changed_exports=sources.changed_exports,
        prev_imports_resolved=paths["py_imports_resolved_v1"],
    )

    base = changed_file_impact_table(
        file_changes=IncrementalFileChanges(changed_file_ids=("file_mod",)),
        incremental_diff=pa.table(
            {
                "file_id": ["file_mod"],
                "path": ["src/mod.py"],
                "change_kind": ["modified"],
            }
        ),
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

    _assert_symbol_closure_results(symbol_closure, hybrid)


def _file_ids(table: pa.Table) -> set[str]:
    values = table["file_id"]
    if isinstance(values, pa.ChunkedArray):
        values = values.combine_chunks()
    return {value for value in values_as_list(values) if isinstance(value, str)}
