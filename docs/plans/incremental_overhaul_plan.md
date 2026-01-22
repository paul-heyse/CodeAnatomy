# Incremental Pipeline Overhaul Plan (Beyond combined_advanced_library_utilization_plan.md)

This plan describes the incremental-module changes that are **not** covered by
`docs/palns/combined_advanced_library_utilization_plan.md`. It assumes the
combined plan scopes land first (Delta DDL registration, CDF provider, SQLGlot
AST execution, DeltaScanConfig, idempotent writes, etc.) and focuses on how to
apply and extend those capabilities inside `src/incremental`.

## Goals
- Unify incremental execution around a single DataFusion + Ibis + SQLGlot runtime.
- Replace ad-hoc SQL strings and per-function sessions with Ibis-first IR and
  policy-driven SQLGlot execution.
- Consolidate Delta upsert/write helpers into a single service with consistent
  partition and schema handling.
- Add file-pruning and streaming write paths to cut memory usage and IO.
- Capture incremental-specific metadata and diagnostics as Delta artifacts.
- Retire duplicated per-dataset update modules after consolidation.

## Explicit exclusions (already covered by the combined plan)
- Delta DDL registration via DeltaTableFactory.
- Dynamic builtin function catalog via information_schema.
- UNBOUNDED external tables for streaming.
- DataFusion INSERT paths for Delta writes.
- SQLGlot AST execution path via Ibis backend.
- Ibis-native Delta read/write integration.
- SQLGlot lineage artifacts and canonicalization ingress.
- Native Delta CDF TableProvider integration.
- DeltaScanConfig derived from session defaults.
- Idempotent Delta writes via CommitProperties.

---

## Scope 1: Incremental runtime unification (single DataFusion/Ibis/SQLGlot context)

**Objective:** create a shared `IncrementalRuntime` that owns the DataFusion
`SessionContext`, Ibis backend, SQLGlot policy, and temp-table lifecycle so all
incremental steps execute with the same settings, caches, and diagnostics.

**Representative code snippet**
```python
# src/incremental/runtime.py
from __future__ import annotations

from collections.abc import Mapping, Iterator
from dataclasses import dataclass
from typing import ContextManager

import pyarrow as pa
from datafusion import SessionContext

from datafusion_engine.runtime import DataFusionRuntimeProfile
from ibis_engine.backend import build_backend
from ibis_engine.config import IbisBackendConfig
from sqlglot_tools.optimizer import SqlGlotPolicy, default_sqlglot_policy


@dataclass(frozen=True)
class IncrementalRuntime:
    profile: DataFusionRuntimeProfile
    sqlglot_policy: SqlGlotPolicy

    @classmethod
    def build(cls, *, sqlglot_policy: SqlGlotPolicy | None = None) -> IncrementalRuntime:
        policy = sqlglot_policy or default_sqlglot_policy()
        return cls(profile=DataFusionRuntimeProfile(), sqlglot_policy=policy)

    def session(self) -> SessionContext:
        return self.profile.session_context()

    def ibis_backend(self) -> object:
        return build_backend(IbisBackendConfig(datafusion_profile=self.profile))


class TempTableRegistry:
    def __init__(self, ctx: SessionContext) -> None:
        self._ctx = ctx
        self._names: list[str] = []

    def register_batches(self, name: str, batches: list[pa.RecordBatch]) -> str:
        self._ctx.register_record_batches(name, batches)
        self._names.append(name)
        return name

    def close(self) -> None:
        for name in self._names:
            if hasattr(self._ctx, "deregister_table"):
                self._ctx.deregister_table(name)
        self._names.clear()
```

**Target files**
- `src/incremental/runtime.py` (new)
- `src/incremental/scip_snapshot.py`
- `src/incremental/changes.py`
- `src/incremental/diff.py`
- `src/incremental/impact.py`
- `src/incremental/relspec_update.py`

**Implementation checklist**
- [ ] Introduce `IncrementalRuntime` and `TempTableRegistry`.
- [ ] Replace per-function `DataFusionRuntimeProfile()` instantiations.
- [ ] Thread the runtime into incremental entrypoints and helper functions.
- [ ] Standardize temp-table registration and cleanup through the registry.
- [ ] Ensure diagnostics sink is used consistently across incremental steps.

---

## Scope 2: Incremental CDF pipeline refactor (provider-first, SQLGlot-aware)

**Objective:** move incremental diff and file-change derivation onto the Delta
CDF TableProvider and execute filtering via SQLGlot policy, not ad-hoc SQL.

**Representative code snippet**
```python
# src/incremental/cdf_runtime.py
from __future__ import annotations

from dataclasses import dataclass

import ibis
import pyarrow as pa

from datafusion_engine.registry_bridge import register_delta_cdf_df
from incremental.cdf_filters import CdfFilterPolicy
from incremental.cdf_cursors import CdfCursorStore, CdfCursor
from incremental.runtime import IncrementalRuntime


@dataclass(frozen=True)
class CdfReadResult:
    table: pa.Table
    updated_version: int


def read_cdf_changes(
    runtime: IncrementalRuntime,
    *,
    name: str,
    path: str,
    cursor_store: CdfCursorStore,
    filter_policy: CdfFilterPolicy | None,
) -> CdfReadResult | None:
    ctx = runtime.session()
    df = register_delta_cdf_df(ctx, name=name, path=path)
    ibis_backend = runtime.ibis_backend()
    expr = ibis_backend.table(name)
    predicate = (filter_policy or CdfFilterPolicy.include_all()).to_sql_predicate()
    if predicate:
        expr = expr.filter(ibis.sql(predicate))
    table = expr.to_pyarrow()
    cursor = cursor_store.load_cursor(name)
    if cursor is None:
        return None
    updated_version = cursor.last_version + 1
    cursor_store.save_cursor(CdfCursor(dataset_name=name, last_version=updated_version))
    return CdfReadResult(table=table, updated_version=updated_version)
```

**Target files**
- `src/incremental/cdf_runtime.py` (new)
- `src/incremental/diff.py`
- `src/incremental/changes.py`
- `src/incremental/cdf_cursors.py`

**Implementation checklist**
- [ ] Add a provider-first CDF read helper that uses `register_delta_cdf_df`.
- [ ] Replace `read_delta_cdf` materialization in `diff_snapshots_with_delta_cdf`.
- [ ] Route `file_changes_from_cdf` through the new helper (no ad-hoc SQL).
- [ ] Add SQLGlot policy application for CDF predicates where applicable.
- [ ] Ensure cursor updates are idempotent and version-correct.

---

## Scope 3: Ibis-first IR for snapshots, diffs, and impact computation

**Objective:** re-implement incremental transformations as Ibis expressions,
execute via SQLGlot AST, and persist plan artifacts for invalidation and audits.

**Representative code snippet**
```python
# src/incremental/scip_snapshot.py (conceptual)
import ibis

def build_scip_snapshot_expr(docs: ibis.Table, occ: ibis.Table, diag: ibis.Table) -> ibis.Table:
    doc_hash = ibis.udf.scalar.builtin("stable_hash64")(docs.document_id)
    occ_hash = ibis.udf.scalar.builtin("stable_hash64")(occ.document_id)
    diag_hash = ibis.udf.scalar.builtin("stable_hash64")(diag.document_id)

    hashes = ibis.union(
        docs.select(document_id=docs.document_id, row_hash=doc_hash),
        occ.select(document_id=occ.document_id, row_hash=occ_hash),
        diag.select(document_id=diag.document_id, row_hash=diag_hash),
        distinct=False,
    )
    fingerprints = hashes.group_by("document_id").aggregate(
        fingerprint=ibis.string_agg(hashes.row_hash, delimiter="|")
    )
    return docs.left_join(fingerprints, ["document_id"]).select(
        docs.document_id, docs.path, fingerprints.fingerprint
    )
```

**Target files**
- `src/incremental/scip_snapshot.py`
- `src/incremental/changes.py`
- `src/incremental/deltas.py`
- `src/incremental/impact.py`
- `src/incremental/invalidations.py`

**Implementation checklist**
- [ ] Replace DataFusion SQL strings with Ibis expressions in snapshots/diffs.
- [ ] Execute via SQLGlot AST path (no string round-trips).
- [ ] Emit plan artifacts (SQLGlot AST, canonical SQL, fingerprints).
- [ ] Store per-step artifacts in diagnostics and invalidation snapshots.
- [ ] Add coverage for Ibis expression parity vs current SQL outputs.

---

## Scope 4: Unified Delta dataset update service (partition-aware upserts)

**Objective:** consolidate duplicated `*_update.py` logic into a single, typed
service that handles schema alignment, partition deletes, and write policies.

**Representative code snippet**
```python
# src/incremental/delta_updates.py
from __future__ import annotations

from dataclasses import dataclass

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.metadata import encoding_policy_from_schema
from incremental.types import IncrementalFileChanges
from storage.deltalake import DeltaUpsertOptions, DeltaWriteOptions, coerce_delta_table


@dataclass(frozen=True)
class PartitionedDatasetSpec:
    name: str
    partition_column: str


def upsert_partitioned_dataset(
    table: TableLike,
    *,
    spec: PartitionedDatasetSpec,
    base_dir: str,
    changes: IncrementalFileChanges,
) -> str:
    delete_partitions = tuple({spec.partition_column: value} for value in changes.deleted_file_ids)
    schema = table.schema
    coerced = coerce_delta_table(
        table,
        schema=schema,
        encoding_policy=encoding_policy_from_schema(schema),
    )
    result = upsert_dataset_partitions_delta(
        coerced,
        options=DeltaUpsertOptions(
            base_dir=base_dir,
            partition_cols=(spec.partition_column,),
            delete_partitions=delete_partitions,
            options=DeltaWriteOptions(schema_mode="merge"),
        ),
    )
    return result.path
```

**Target files**
- `src/incremental/delta_updates.py` (new)
- `src/incremental/edges_update.py`
- `src/incremental/nodes_update.py`
- `src/incremental/props_update.py`
- `src/incremental/exports_update.py`
- `src/incremental/index_update.py`
- `src/incremental/normalize_update.py`
- `src/incremental/extract_update.py`
- `src/incremental/impact_update.py`

**Implementation checklist**
- [ ] Introduce `PartitionedDatasetSpec` and `upsert_partitioned_dataset`.
- [ ] Migrate each dataset update module to the unified service.
- [ ] Remove duplicated partition-spec helpers from individual modules.
- [ ] Add a single validation path for required partition columns.
- [ ] Add tests for schema alignment and delete partition behavior.

---

## Scope 5: Delta file index pruning for scoped incremental reads

**Objective:** reduce scan cost by using Delta log file metadata to select only
candidate files for large file-id scopes before DataFusion planning.

**Representative code snippet**
```python
# src/incremental/pruning.py
from __future__ import annotations

from dataclasses import dataclass

from datafusion import SessionContext
from deltalake import DeltaTable
from storage.deltalake.file_index import build_delta_file_index
from storage.deltalake.file_pruning import FilePruningPolicy, evaluate_filters_against_index


@dataclass(frozen=True)
class FileScopePolicy:
    file_id_column: str
    file_ids: tuple[str, ...]

    def to_policy(self) -> FilePruningPolicy:
        predicates = [f"{self.file_id_column} IN ({', '.join(self.file_ids)})"]
        return FilePruningPolicy(partition_filters=predicates, stats_filters=[])


def prune_delta_files(ctx: SessionContext, path: str, policy: FileScopePolicy) -> list[str]:
    table = DeltaTable(path)
    index = build_delta_file_index(table)
    filtered = evaluate_filters_against_index(index, policy.to_policy(), ctx)
    return [str(value) for value in filtered.column("path").to_pylist()]
```

**Target files**
- `src/incremental/pruning.py` (new)
- `src/incremental/relspec_update.py`
- `src/incremental/impact.py`
- `src/storage/deltalake/file_index.py`
- `rust/datafusion_ext/src/lib.rs` (optional file-set provider exposure)

**Implementation checklist**
- [ ] Add pruning helper that produces candidate file paths.
- [ ] Apply pruning in `relspec_inputs_from_state` when file-id scopes are large.
- [ ] Expose file-set provider in the Rust extension if needed.
- [ ] Add diagnostics for pruning effectiveness (pruned vs total files).
- [ ] Add fallback path when pruning metadata is missing.

---

## Scope 6: Streaming execution and write paths for large outputs

**Objective:** avoid full materialization by streaming Ibis/DF outputs directly
to Delta or Arrow dataset writers when outputs are large.

**Representative code snippet**
```python
# src/incremental/streaming_writes.py
from __future__ import annotations

from dataclasses import dataclass

from ibis.expr.types import Table as IbisTable
from storage.deltalake import DeltaWriteOptions, write_table_delta


@dataclass(frozen=True)
class StreamingWriteOptions:
    chunk_size: int = 250_000


def stream_expr_to_delta(
    expr: IbisTable,
    path: str,
    *,
    options: StreamingWriteOptions,
    write_options: DeltaWriteOptions,
) -> None:
    reader = expr.to_pyarrow_batches(chunk_size=options.chunk_size)
    write_table_delta(reader, path, options=write_options)
```

**Target files**
- `src/incremental/streaming_writes.py` (new)
- `src/incremental/exports.py`
- `src/incremental/impact.py`
- `src/incremental/scip_snapshot.py`

**Implementation checklist**
- [ ] Add streaming write helper (RecordBatchReader input).
- [ ] Replace full-table materialization in large outputs.
- [ ] Add fallback to materialized writes for small outputs.
- [ ] Record chunk sizes and output stats in diagnostics.

---

## Scope 7: Incremental metadata and diagnostics artifacts

**Objective:** store incremental-run metadata (runtime settings hash, SQLGlot
policy hash, CDF cursor state, pruning metrics) as Delta artifacts in the state
store for reproducibility and audits.

**Representative code snippet**
```python
# src/incremental/metadata.py
from __future__ import annotations

import pyarrow as pa

from datafusion_engine.runtime import DataFusionRuntimeProfile
from sqlglot_tools.optimizer import sqlglot_policy_snapshot
from storage.deltalake import DeltaWriteOptions, write_table_delta


def write_incremental_metadata(path: str, runtime: DataFusionRuntimeProfile) -> None:
    policy = sqlglot_policy_snapshot()
    payload = {
        "datafusion_settings_hash": runtime.settings_hash(),
        "sqlglot_policy_hash": policy.policy_hash,
        "sqlglot_policy_version": policy.policy_version,
    }
    table = pa.Table.from_pylist([payload])
    write_table_delta(
        table,
        path,
        options=DeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
    )
```

**Target files**
- `src/incremental/metadata.py` (new)
- `src/incremental/invalidations.py`
- `src/incremental/state_store.py`
- `src/obs/diagnostics.py`

**Implementation checklist**
- [ ] Add a metadata table path under `StateStore.metadata_dir()`.
- [ ] Persist runtime/settings/policy hashes on each run.
- [ ] Add CDF cursor snapshots as a Delta artifact table.
- [ ] Extend invalidation logic to include metadata changes.
- [ ] Hook metadata writes into incremental entrypoints.

---

## Scope 8: Module retirements and consolidation

**Objective:** remove duplicated, per-dataset update modules after consolidation
into the unified dataset update service.

**Representative code snippet**
```python
# src/incremental/__init__.py (conceptual cleanup)
__all__ = [
    "upsert_partitioned_dataset",
    "write_incremental_metadata",
    "read_cdf_changes",
]
```

**Target files**
- `src/incremental/edges_update.py`
- `src/incremental/nodes_update.py`
- `src/incremental/props_update.py`
- `src/incremental/exports_update.py`
- `src/incremental/index_update.py`
- `src/incremental/normalize_update.py`
- `src/incremental/extract_update.py`
- `src/incremental/impact_update.py`
- `src/incremental/__init__.py`

**Implementation checklist**
- [ ] Migrate callers to `incremental/delta_updates.py`.
- [ ] Delete retired modules after all references are removed.
- [ ] Trim `incremental.__all__` and lazy import map.
- [ ] Update docs and examples referencing retired modules.

**Decommission list (modules + functions)**

Modules to delete once all call sites migrate to the unified update service:
- `src/incremental/edges_update.py` (exports: `upsert_cpg_edges`; internal: `_partition_specs`)
- `src/incremental/nodes_update.py` (exports: `upsert_cpg_nodes`; internal: `_partition_specs`)
- `src/incremental/exports_update.py` (exports: `upsert_exported_defs`; internal: `_partition_specs`)
- `src/incremental/index_update.py` (exports: `upsert_module_index`, `upsert_imports_resolved`; internal: `_partition_specs`)
- `src/incremental/normalize_update.py` (exports: `upsert_normalize_outputs`; internal: `_partition_specs`)
- `src/incremental/extract_update.py` (exports: `upsert_extract_outputs`; internal: `_partition_specs`)
- `src/incremental/impact_update.py` (exports: `write_impacted_callers`, `write_impacted_importers`, `write_impacted_files`)
- `src/incremental/props_update.py` (exports: `upsert_cpg_props`, `split_props_by_file_id`; internal helpers listed below)

Function-level retirements after runtime unification and Ibis-first refactors:
- `src/incremental/scip_snapshot.py`: `_session_profile`, `_session_context`,
  `_register_table`, `_deregister_table`, `_sql_identifier`, `_stringify_expr`,
  `_row_hash_expr`, `_snapshot_added_sql`, `_snapshot_diff_sql`
- `src/incremental/changes.py`: `_session_profile`, `_session_context`,
  `_register_table`, `_deregister_table`
- `src/incremental/diff.py`: `_session_profile`, `_session_context`, `_deregister_table`
- `src/incremental/props_update.py`: `_attach_file_id`, `_filter_props_kind`,
  `_filter_props_other`, `_concat_tables`, `_ensure_table`, `_datafusion_context`,
  `_sql_options`, `_register_table`, `_deregister_table`, `_sql_identifier`,
  `_sql_literal`, `_empty_by_file`, `_empty_global`, `_partition_specs`

`src/incremental/__init__.py` cleanup required after removals:
- Remove `edges_update`, `nodes_update`, `exports_update`, `index_update`,
  `normalize_update`, `extract_update`, `impact_update`, `props_update`
  entries from `__all__` and `_LAZY_IMPORTS`.

---

## Rollout and validation (cross-cutting)

**Checklist**
- [ ] Add unit tests for new runtime + temp table lifecycle.
- [ ] Add golden tests for Ibis expression parity (SQL/AST fingerprints).
- [ ] Add integration tests for CDF provider pipeline and cursor updates.
- [ ] Add performance/IO regression tests for pruning and streaming paths.
- [ ] Run `uv run ruff check --fix`, `uv run pyrefly check`, and
      `uv run pyright --warnings --pythonversion=3.13` on touched modules.
