Below is the approach I would implement **in *this* codebase** (given what’s in `src/hamilton_pipeline`, `extract/`, `normalize/`, `relspec/`, and the Parquet IO helpers you already have) to make the pipeline **incremental** in a way that is:

* **rigorous** (handles “changes elsewhere can change resolution here”),
* **fast** (partition-pruning + “replace only touched partitions”),
* **rule-driven** (the rule framework decides what to recompute),
* **minimizes Python work** (Python orchestrates; Arrow/DataFusion do the heavy lifting).

I’m going to lay this out as a concrete technical design you can implement end-to-end, with code shapes and exact integration points.

---

## 1) The key design choice: “unit of incrementality” = file_id partition

Your architecture already has the perfect anchor:

* `file_id` is **stable** (hash of repo-relative `path`, optionally salted with `repo_id`) via `extract.registry_ids.repo_file_id_spec(...)`.
* Almost all expensive artifacts (CST tables, normalized spans, SCIP occurrences, interval-align relationship outputs, and the CPG edges) are naturally attributable to **a single file** (or at least have a `path` you can hash into `file_id`).

So: **store every heavy dataset as a Parquet dataset partitioned by `file_id`**.

### Why this works in your pipeline

The mapping rules you have now are mostly “same-file” joins:

* `INTERVAL_ALIGN` rules align `(cst_* in file X)` with `(scip_occurrences in file X)` by `path` and span → **local to a file**.
* `rel_callsite_qname` is also effectively local (qname_id is a deterministic hash of qname), and candidates originate from the file’s callsites.

So incremental recompute is mostly:

> “Replace partitions for file_ids that changed (or were semantically impacted), then reuse everything else.”

The **only non-trivial part** is: how to decide impacted file_ids when a change in file B can change resolution in file A.

That’s solved in §3.

---

## 2) Introduce a persistent state store (separate from output_dir)

Add a new notion: **state_dir** (persistent cache across runs).

* `output_dir` remains “run artifacts” (what you already write: `cpg_edges.parquet`, reports, etc.).
* `state_dir` becomes “incremental storage” (partitioned datasets + snapshots).

### Proposed directory layout

```
.state/
  snapshots/
    latest.json
    2026-01-16T12-34-56Z/
      repo_files.parquet
      scip_doc_fingerprints.parquet
      pipeline_fingerprints.json
      diff_report.parquet
  datasets/
    extract/
      cst_name_refs_v1/
        file_id=.../*.parquet
      cst_callsites_v1/
      ...
      scip_occurrences_v1/
      ...
    normalize/
      cst_imports_norm_v1/
      scip_occurrences_norm_v1/
      ...
    relspec/
      rel_callsite_symbol_v1/
      rel_import_symbol_v1/
      rel_name_symbol_v1/
      rel_callsite_qname_v1/
    cpg/
      cpg_nodes_v1/
      cpg_edges_v1/
      cpg_props_v1/
```

This lets you:

* update only a few partitions each run,
* still expose “full tables” by scanning the dataset directory.

---

## 3) Rigorous impact detection: don’t rely only on file diffs

Your intuition is right: file diffs alone are not enough, because symbol resolution can change in untouched files.

The cleanest way **in your architecture** is:

### Use SCIP “document fingerprint” diff as the semantic-change oracle

You already run/consume SCIP and build `scip_occurrences` / symbol info. If a change in file B alters the semantic resolution in file A, **SCIP output for document A will change**.

So: store a **per-document fingerprint** from the SCIP index, and diff it across runs.

#### Implementation detail

When parsing the SCIP index, for each document:

* compute a stable `doc_fingerprint = sha256(doc.SerializeToString(deterministic=True))`
* store `(path, file_id, doc_fingerprint)`

Then:

* `scip_changed_file_ids = {file_id | doc_fingerprint changed OR doc added/removed}`

This gives you a **semantic impacted set** even when file contents didn’t change.

> This is the single biggest “make it rigorous” lever you have, because it pushes the hard dependency logic into the SCIP indexer, which is exactly what you want.

### Final impacted set per stage

You’ll keep *multiple* impacted sets:

* `content_changed_file_ids`: from repo snapshot diff (sha256 changed, added, deleted)
* `scip_changed_file_ids`: from SCIP doc fingerprint diff
* `mapping_impacted_file_ids = content_changed ∪ scip_changed`

Then:

* CST extraction recomputes only `content_changed`
* SCIP extraction recomputes only `scip_changed` (or everything if you can’t diff, but still partitioned)
* Relationship outputs recompute `mapping_impacted`
* CPG edges/nodes/props recompute `mapping_impacted` plus whatever else you enable

This avoids the “cascade recompute everything” trap.

---

## 4) Core mechanism: partitioned Parquet upserts (replace only touched file_ids)

You already have `arrowdsl/io/parquet.py::write_dataset_parquet(...)` using `ds.write_dataset`.

To make incremental real, add a **partitioned upsert writer**.

### `PartitionedDatasetStore` (new module)

Create `src/storage/partitioned_store.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.dataset as ds

from arrowdsl.compute.ids import apply_hash_column
from extract.registry_ids import repo_file_id_spec

@dataclass(frozen=True)
class PartitionedDatasetStore:
    root: Path
    repo_id: str | None = None
    partition_col: str = "file_id"

    def dataset_dir(self, stage: str, name: str) -> Path:
        return self.root / "datasets" / stage / name

    def _ensure_file_id(self, table: pa.Table) -> pa.Table:
        if self.partition_col in table.column_names:
            return table
        if "path" not in table.column_names:
            raise ValueError("Need either file_id or path to partition.")
        spec = repo_file_id_spec(repo_id=self.repo_id)
        return apply_hash_column(table, spec=spec, required=("path",))

    def upsert_by_file_id(self, stage: str, name: str, table: pa.Table) -> None:
        out_dir = self.dataset_dir(stage, name)
        out_dir.mkdir(parents=True, exist_ok=True)

        table = self._ensure_file_id(table)

        partitioning = ds.partitioning(
            pa.schema([pa.field(self.partition_col, pa.string())]),
            flavor="hive",
        )

        ds.write_dataset(
            table,
            base_dir=str(out_dir),
            format="parquet",
            partitioning=partitioning,
            existing_data_behavior="delete_matching",  # critical
        )

    def delete_file_partitions(self, stage: str, name: str, file_ids: Iterable[str]) -> None:
        out_dir = self.dataset_dir(stage, name)
        for fid in file_ids:
            part = out_dir / f"{self.partition_col}={fid}"
            if part.exists():
                # rmtree for partitions; use shutil.rmtree
                import shutil
                shutil.rmtree(part)
```

This is the foundational primitive.

### What to deprecate

Deprecate the current “whole dataset overwrite” pattern for intermediate stores, especially:

* `hamilton_pipeline/modules/cpg_build.py::persist_relspec_input_datasets` writing full datasets each run via `write_named_datasets_parquet(...)`.

Instead:

* in incremental mode: **write only changed file_id partitions** to `state_dir/datasets/...`.

---

## 5) Snapshot + diff tables (repo_files + scip_fingerprints)

Add `src/incremental/snapshots.py`:

### Repo snapshot

* Store only cheap metadata per file:

  * `file_id`, `path`, `size_bytes`, `mtime_ns`, `file_sha256`
* In incremental mode, do a “fast scan”:

  * list files + stat → compare to previous
  * hash only those that look changed (mtime/size changed or new)

**This alone** prevents re-reading the whole repo every run.

### SCIP doc fingerprint snapshot

Modify `extract/scip_extract.py` to optionally emit fingerprints:

```python
# new helper in extract/scip_extract.py (or separate file)
import hashlib

def scip_document_fingerprints(index) -> list[dict[str, object]]:
    rows = []
    for doc in index.documents:
        raw = doc.SerializeToString(deterministic=True)
        rows.append({
            "path": doc.relative_path,
            "doc_fingerprint": hashlib.sha256(raw).hexdigest(),
        })
    return rows
```

Write this as a small Parquet table in `snapshots/<run_id>/scip_doc_fingerprints.parquet`.

### Diff computation

Compute:

* added/removed/changed by `file_sha256` for repo snapshot
* added/removed/changed by `doc_fingerprint` for SCIP snapshot

Write a diff report table:

`diff_report_v1(file_id, path, reason, stage_mask, prev_val, curr_val)`

This is both **diagnostics** and **the driver** for incremental scheduling.

---

## 6) Incremental extraction: only parse changed files, upsert partitions

### CST extraction (content_changed only)

Right now CST extraction uses:

* `repo_files` → `file_contexts` → `extract_cst_tables(file_contexts)`

Implement:

* `changed_file_contexts(repo_files, diff)` (only changed/new files)
* run `extract_cst_tables` only on that subset
* upsert each CST output table into `state_dir/datasets/extract/<dataset_name>/` partitioned by file_id
* for deleted file_ids: delete partitions

Then provide “full CST tables” to downstream by scanning the store (not by holding everything in memory).

**Minimal integration path:** add a new execution mode where your extraction Hamilton nodes return `DatasetSource` pointing at the store instead of returning an in-memory `pa.Table`.

You already support `TableLike | DatasetSource` in many places (`CstBuildInputs`, etc.), so this fits.

### SCIP extraction (scip_changed only)

Add an optional allowlist to `extract_scip_tables(...)` so you only emit rows for impacted docs:

```python
def extract_scip_tables(..., allowed_paths: set[str] | None = None, ...) -> dict[str, TableLike]:
    ...
    for doc in index.documents:
        if allowed_paths is not None and doc.relative_path not in allowed_paths:
            continue
        # existing extraction logic for this doc
```

Then:

* compute `allowed_paths` from `scip_changed_file_ids` (via mapping `file_id -> path`)
* upsert resulting scip tables into `state_dir/datasets/extract/scip_occurrences_v1/...` etc.

This is where SCIP fingerprint diff pays off:

* you recompute scip tables even for semantically impacted files whose text didn’t change,
* but only those.

---

## 7) Incremental normalization: recompute only partitions for impacted file_ids

Normalization is best treated the same way:

* normalization outputs are stored partitioned by `file_id`
* run normalization only for the impacted set relevant to those outputs
* upsert partitions

### Practical shortcut for v1 incremental normalization

Start by incrementalizing only the normalization tables that are actually used in mapping + CPG:

* `cst_imports_norm`
* `cst_defs_norm`
* `scip_occurrences_norm`
* (optionally) `diagnostics_norm`, `types_norm`, etc.

If you do those first, your mapping + edges become incremental even if other normalizations aren’t yet.

Implementation technique:

* when you build `NormalizeInputs`, instead of giving it full extracted datasets, give it **scanners filtered by file_id partitions**.

If you’re in plan-lane (Acero), the easiest “filter” is:

* open dataset only on the subset of `file_id=...` directories for the delta computation.

(For DataFusion-backed normalize steps, you can use the same param-table join trick described next for relspec.)

---

## 8) Incremental relationship rules: execute rules scoped to file_allowlist, upsert outputs

This is the heart of what you asked: **only perform the expensive mapping work needed**.

### Use an allowlist param table to scope Ibis/DataFusion scans

You already have param table infrastructure and a default `file_allowlist` spec in `hamilton_pipeline/modules/params.py`.

Make it real:

* Populate `param_bundle.lists["file_allowlist"] = impacted_file_ids`
* Ensure it’s registered into DataFusion via `ParamTableRegistry.register_into_backend(...)` (already present)

### Add a resolver wrapper that “joins every scanned dataset to file_allowlist”

Create `src/relspec/scoped_resolver.py`:

```python
from __future__ import annotations
from dataclasses import dataclass
from ibis.expr.types import Table

from arrowdsl.core.context import ExecutionContext
from ibis.backends import BaseBackend
from ibis_engine.plan import IbisPlan
from relspec.model import DatasetRef

@dataclass(frozen=True)
class ScopeConfig:
    allowlist_logical_name: str = "file_allowlist"
    file_id_col: str = "file_id"
    # Which datasets should be scoped (usually: all file-backed ones)
    scoped_dataset_names: frozenset[str] = frozenset()

class ScopedPlanResolver:
    def __init__(self, *, base, allowlist_table: Table, scope: ScopeConfig):
        self.base = base
        self.allowlist = allowlist_table
        self.scope = scope

    def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> IbisPlan:
        plan = self.base.resolve(ref, ctx=ctx)
        if self.scope.scoped_dataset_names and ref.name not in self.scope.scoped_dataset_names:
            return plan
        t = plan.expr
        # inner join restricts to file_allowlist
        scoped = t.inner_join(self.allowlist, [self.scope.file_id_col]).select(t.columns)
        return IbisPlan(expr=scoped, ordering=plan.ordering)
```

Then in `hamilton_pipeline/modules/cpg_build.py::relationship_tables(...)`:

* detect incremental mode
* build `ScopedPlanResolver(base=relspec_resolver, allowlist_table=param_table_registry.ibis_tables(backend)["file_allowlist"], ...)`
* execute compiled outputs with that resolver

### Store relationship outputs incrementally

After executing each relationship output, you **do not overwrite the whole table**.

Instead:

* upsert delta results into `state_dir/datasets/relspec/<contract_name>/` partitioned by `file_id` (derived from path if necessary)
* delete partitions for deleted file_ids

Then downstream CPG building reads the **full relationship tables** by scanning those datasets.

This makes mapping essentially O(changed_files) rather than O(repo).

---

## 9) Incremental CPG build: compute delta nodes/edges, upsert, then optionally materialize full outputs

### Delta build

Do not rebuild full `cpg_edges` and `cpg_nodes` every run.

Instead:

* Compute delta nodes/edges for `mapping_impacted_file_ids`
* Upsert into `state_dir/datasets/cpg/cpg_edges_v1/` and `.../cpg_nodes_v1/`

Mechanically:

* build a `PlanCatalog` / `TableCatalog` where each input is:

  * a dataset source opened only on impacted partitions, **or**
  * the full dataset if the rule is global (e.g., symbol relationship edges if you keep them global)
* run `build_cpg_edges` / `build_cpg_nodes`
* upsert results

### Materialization (optional)

If you still want the run’s `output_dir/cpg_edges.parquet` single file:

* materialize from the store at the end by scanning the full dataset and writing a single parquet file.

This is still a full scan/write, but importantly:

* it’s usually cheaper than recomputing joins,
* and you can make it optional (LLM agents may prefer querying the partitioned store).

---

## 10) What I would deprecate immediately (to unblock incremental)

These are the specific “this makes incremental impossible” behaviors:

1. **Deprecate**: writing intermediate datasets as monolithic parquet files per run

   * Replace with: partitioned datasets in `state_dir`

2. **Deprecate**: `persist_relspec_input_datasets(... overwrite=True)` semantics for incremental mode

   * Replace with: `PartitionedDatasetStore.upsert_by_file_id(...)`

3. **Deprecate**: relying on Hamilton node-level caching for incremental correctness

   * Hamilton caching is coarse: any change in `repo_files` invalidates tons of nodes.
   * Replace with: dataset-store reuse + scoped rule execution.

---

## 11) A concrete staged implementation plan (what I’d do next in code)

This is the shortest path to “incremental mapping + incremental CPG edges”:

### PR-A: State store + snapshots

* Add `state_dir` param to `execute_pipeline(...)`
* Implement `RepoSnapshot` writer/loader
* Implement `PartitionedDatasetStore`
* Write `repo_files_snapshot.parquet` to `state_dir/snapshots/latest/...`

### PR-B: SCIP doc fingerprints + semantic diff

* Emit `scip_doc_fingerprints.parquet`
* Compute `scip_changed_file_ids` diff
* Produce `diff_report.parquet` with reasons

### PR-C: Incremental CST extraction

* Extract CST only for `content_changed_file_ids`
* Upsert CST datasets into `state_dir/datasets/extract/...`
* Downstream reads CST as `DatasetSource` (filesystem scan)

### PR-D: Incremental SCIP extraction

* Filter `extract_scip_tables(... allowed_paths=...)`
* Upsert scip datasets into `state_dir/datasets/extract/...`

### PR-E: Incremental normalization (minimum set)

* Normalize only what mapping needs:

  * `cst_imports_norm`, `cst_defs_norm`, `scip_occurrences_norm`
* Upsert normalized datasets

### PR-F: Scoped relationship execution + incremental relationship outputs

* Implement `ScopedPlanResolver`
* Populate `file_allowlist` = `mapping_impacted_file_ids`
* Execute relationship outputs scoped
* Upsert relationship outputs to store

### PR-G: Incremental CPG edges first (big win)

* Build edges only for impacted file_ids
* Upsert to store
* Optional: materialize consolidated edges parquet

(Then do nodes/props similarly.)

---

## 12) The “hard edge cases” and how this design handles them

### Case: file B changed → name resolution in file A changes

* SCIP doc fingerprint for A changes ⇒ A in `scip_changed_file_ids`
* relationship rules scoped to `mapping_impacted` include A
* you recompute mapping for A even though A’s text is unchanged
* CST for A is reused (content unchanged)

### Case: file renamed (path changes)

* old `file_id` deleted, new `file_id` added
* delete partitions for old file_id, write partitions for new
* edges/nodes referencing old file_id vanish naturally when partitions are replaced

### Case: pipeline code/rules changed

Store invalidation rule:

* keep `pipeline_fingerprints.json` containing:

  * schema fingerprints (or schema registry version)
  * rule registry signature (hash of rule definitions)
* if mismatched: force full rebuild (or per-stage rebuild)

This avoids “incremental corruption” when you update rule logic.

---

## 13) If you want, I can map this onto your exact module entrypoints

If you implement the above, the most invasive but highest-leverage integration points in *your repo* are:

* `hamilton_pipeline/execution.py`
  Add `state_dir`, load previous snapshot, compute diffs, pass impacted sets into nodes.

* `hamilton_pipeline/modules/extraction.py`
  Add “incremental mode” nodes that:

  * run extractors only for changed contexts
  * upsert into `state_dir`
  * return `DatasetSource` for downstream.

* `extract/scip_extract.py`
  Add `allowed_paths` filter + doc fingerprints.

* `hamilton_pipeline/modules/cpg_build.py::relationship_tables`
  Inject `ScopedPlanResolver` and write relationship outputs into store instead of returning only in-memory.

* `storage/io.py` + `arrowdsl/io/parquet.py`
  Add partitioned dataset support / wrappers.

---

If you tell me which incremental “slice” you want first (e.g., **only incremental mapping + edges**, or **incremental everything through props**), I can turn the above into a PR-by-PR checklist *at the file/function level* with the exact signatures and minimal diffs you’d apply.


Below is a concrete, **PR-by-PR “incremental everything through props” implementation plan** for your current architecture (Hamilton-driven pipeline + DataFusion/Ibis + Arrow/Parquet artifacts), written **at the file/function level** with **exact signatures**, **minimal-diff tactics**, and explicit **deprecations**.

I’m assuming the end-state you want is:

* A persistent **state store** (Parquet datasets) that represents the **latest committed build**.
* Each new run:

  1. snapshots the repo,
  2. diffs against previous snapshot,
  3. computes an **impact closure** (which files must be re-resolved),
  4. recomputes only those slices,
  5. **upserts** (replaces partitions) through **relationships → edges → props**.

Key design choice that makes this tractable:

* Treat **`file_id` as the universal partition key** (it already exists widely via `file_identity_bundle()`).
* For tables that don’t have `file_id` (notably **relationship outputs**, **cpg_edges**, **cpg_props**), derive a **stable `origin_file_id`** from `path` using the same hashing scheme as `repo_files_v1.file_id` (prefix `"file"`, args `path`, optional `repo_id`).

---

## The incremental contract: what we will guarantee

After these PRs:

1. **Idempotent latest-state store**
   Running incremental twice with no code changes yields **no table changes**.

2. **Correctness rule**
   Incremental output (nodes/edges/props) matches a clean full run for the same repo state, modulo row ordering.

3. **Upsert semantics everywhere**

   * For per-file tables: partitions are replaced for impacted `file_id`s and deleted for removed files.
   * For global tables: append-only or periodic compaction (configurable).

---

# PR11 — Incremental state store + snapshots + diff (no pipeline logic changes yet)

### Goal

Introduce a persistent **state store layout**, a **repo snapshot table**, and a **diff table**. This PR does *not* change extraction/normalization/CPG execution yet.

### New files

* `src/incremental/state_store.py`
* `src/incremental/snapshot.py`
* `src/incremental/diff.py`
* `src/incremental/types.py`

### Modified files

* `src/hamilton_pipeline/pipeline_types.py` (add config/type plumbing)
* `src/config.py` (optional: add incremental config defaults)
* `scripts/run_full_pipeline.py` (optional: add `--state-dir` + `--write-snapshot` flag)

### New types & signatures

#### `src/incremental/types.py`

```python
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Sequence

ChangeKind = Literal["added", "modified", "deleted", "unchanged", "renamed"]

@dataclass(frozen=True)
class IncrementalConfig:
    enabled: bool = False
    state_dir: Path | None = None
    repo_id: str | None = None  # optional salt for file_id derivation
    max_impact_depth: int = 50  # for closure iteration safety
```

#### `src/incremental/state_store.py`

```python
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class StateStore:
    root: Path

    def dataset_dir(self, dataset_name: str) -> Path: ...
    def snapshot_path(self) -> Path: ...
    def diff_path(self) -> Path: ...
    def ensure_dirs(self) -> None: ...
```

#### `src/incremental/snapshot.py`

```python
import pyarrow as pa

def build_repo_snapshot(repo_files: pa.Table) -> pa.Table:
    """Return a minimal snapshot: file_id, path, file_sha256, size_bytes, mtime_ns?"""
```

#### `src/incremental/diff.py`

```python
import pyarrow as pa

def diff_snapshots(prev: pa.Table | None, cur: pa.Table) -> pa.Table:
    """Full outer join on file_id; compute change_kind + prev/cur sha/path."""
```

### Minimal-diff implementation notes

* Snapshot is derived from **`repo_files_v1`** (already produced).
* On first run, `prev=None` => everything is `added`.

### What to deprecate (start now, remove later)

* **Deprecate** ad-hoc “incremental mode” gating via env var:

  * `CODEANATOMY_PIPELINE_MODE` in `src/hamilton_pipeline/modules/inputs.py`
  * Replace with `IncrementalConfig.enabled` (keep env var as backward compat for 1–2 releases).

### Suggested commit slices

1. **Add incremental types + state store layout**
2. **Add snapshot builder + writer**
3. **Add diff builder + writer**
4. **Wire config into pipeline_types (no behavior change)**

### Tiny code snippet (diff using DataFusion, preferred)

```python
# inside diff_snapshots()
# use ctx.runtime.datafusion when available; fallback to pyarrow joins otherwise
```

---

# PR12 — Partition upsert writer for Parquet datasets (the core primitive)

### Goal

Enable **replace-only-these-partitions** writes. This is the enabling step for incremental across all stages.

### Modified files

* `src/arrowdsl/io/parquet.py` (add “upsert partitions” API)
* `src/storage/io.py` (thin wrapper to reuse everywhere)
* Add tests: `tests/test_partition_upsert.py` (or your existing test layout)

### New API (exact signature)

#### `src/arrowdsl/io/parquet.py`

```python
from __future__ import annotations
from collections.abc import Sequence
from pathlib import Path
import pyarrow as pa

def upsert_dataset_partitions_parquet(
    table: pa.Table,
    *,
    base_dir: str | Path,
    partition_cols: Sequence[str],
    delete_partitions: Sequence[dict[str, str]] = (),
    schema: pa.Schema | None = None,
    existing_data_behavior: str = "delete_matching",
) -> str:
    """
    Writes `table` as a parquet dataset partitioned by `partition_cols`
    and deletes existing matching partitions before writing.

    IMPORTANT: does NOT delete the dataset root directory.
    """
```

### Implementation sketch

* For explicit deletions: remove directories like `file_id=<id>/`.
* For upsert: call `pyarrow.dataset.write_dataset(... existing_data_behavior="delete_matching")`
  using `partitioning=partition_cols` (Hive style).

### What to deprecate

* Direct calls to `write_dataset_parquet(... overwrite=True)` for incremental datasets.

  * Keep for “full rebuild” mode.

### Suggested commit slices

1. **Add upsert API in arrowdsl/io/parquet.py**
2. **Add storage/io wrapper**
3. **Add tests proving only touched partitions change**

---

# PR13 — Incremental extract + normalize state store (changed files only)

### Goal

Maintain **latest extract+normalize datasets** partitioned by `file_id`.
This PR handles:

* `changed_files = added ∪ modified`
* `deleted_files = deleted`

and updates:

* extract outputs (AST/CST/bytecode/tree-sitter/runtime inspect as configured)
* normalization outputs (spans, ids, normalized CST, callsite candidates)
* plus the minimal global dims that must stay consistent (see below)

### New files

* `src/incremental/extract_update.py`
* `src/incremental/normalize_update.py`
* `src/incremental/bootstrap.py` (first-run initialization)

### Modified files (minimal)

* `scripts/run_full_pipeline.py` (add `--incremental` entrypoint OR add new script in PR15)
* Potentially: `src/hamilton_pipeline/execution.py` (add a new exported function)

### Core orchestration signature

#### `src/incremental/bootstrap.py`

```python
from __future__ import annotations
from incremental.state_store import StateStore
import pyarrow as pa

def bootstrap_latest_state(
    *,
    store: StateStore,
    full_run_outputs: dict[str, pa.Table],
) -> None:
    """One-time: write all datasets into partitioned state store."""
```

#### `src/incremental/extract_update.py`

```python
import pyarrow as pa
from incremental.state_store import StateStore

def update_extract_state(
    *,
    store: StateStore,
    changed_repo_files: pa.Table,
    deleted_file_ids: list[str],
    # you will pass ExtractExecutionContext + options from existing pipeline modules
    extract_outputs: dict[str, pa.Table],
) -> None:
    """Upsert extract datasets partitioned by file_id; delete removed partitions."""
```

#### `src/incremental/normalize_update.py`

```python
import pyarrow as pa
from incremental.state_store import StateStore

def update_normalize_state(
    *,
    store: StateStore,
    changed_inputs: dict[str, pa.Table],
    deleted_file_ids: list[str],
    normalize_outputs: dict[str, pa.Table],
) -> None:
    """Upsert normalize datasets partitioned by file_id; delete removed partitions."""
```

### “Global” normalized tables: how we keep them incremental

Some tables are effectively global but don’t need full rebuild:

* **`dim_qualified_names`**: qname_id is already deterministic (`prefixed_hash_id(qname)`), so the dim can be **append-only**.

  * Incremental update = extract qnames from changed files, union, distinct.
  * Deletions do not require removing qnames.

In this PR:

* Store global dims under `state/datasets/<name>/` **unpartitioned** (single dataset).
* Provide `compact_dims()` hook for later.

### How to reuse your existing code with minimal diffs

Do **not** re-implement extraction/normalization.
Instead:

* Run the existing functions to produce **tables for changed files only**:

  * You can do this by filtering `repo_files_v1` to changed `file_id`s and invoking the same extraction/normalize nodes.
* Then call `update_extract_state()` and `update_normalize_state()` to persist.

### Suggested commit slices

1. **Bootstrap: write full state store from a full run**
2. **Changed-only extraction update**
3. **Changed-only normalization update**
4. **Append-only dim updates (dim_qualified_names)**

### What to deprecate

* Treating `work_dir/relspec_inputs` as the durable storage for inputs.

  * After this PR, **`state_dir` is the durable source of truth**.

---

# PR14 — Impact analysis (import graph closure) → impacted file set

### Goal

Compute **impacted file_ids** beyond the changed set, using a conservative but effective rule:

* Build an import dependency graph:

  * importer_file → imported_module (and optionally imported_file, if resolvable)
* Compute **reverse closure**:

  * any file that (transitively) imports a changed/deleted/added module is impacted.

We’ll keep this:

* **Fast**
* **Safe** (over-approximates instead of under-approximates)
* **Purely rule/data driven** (from normalized `cst_imports_norm` + `repo_files`)

### New files

* `src/incremental/module_index.py`
* `src/incremental/impact.py`

### Modified files

* none required outside incremental package (preferred)

### New tables in state store

* `py_module_index_v1` (per-file, partitioned by file_id)
* `py_import_edges_v1` (per-file, partitioned by file_id)

#### `src/incremental/module_index.py`

```python
import pyarrow as pa

def module_name_from_path(path: str) -> str | None:
    """src/pkg/foo.py -> pkg.foo ; src/pkg/__init__.py -> pkg"""

def build_module_index(repo_files: pa.Table) -> pa.Table:
    """file_id, path, module_name, is_init"""
```

#### `src/incremental/impact.py`

```python
import pyarrow as pa

def compute_impacted_files(
    *,
    prev_snapshot: pa.Table | None,
    cur_snapshot: pa.Table,
    changes: pa.Table,                # output of diff_snapshots()
    module_index: pa.Table,           # current module_index
    import_edges: pa.Table,           # derived from cst_imports_norm
    max_depth: int = 50,
) -> pa.Array:
    """Return array of impacted file_ids (changed + reverse import closure)."""
```

### Import edges: what we store

From `cst_imports_norm`:

* `importer_file_id`
* `imported_module` (absolute module string; resolve relative using importer module prefix)
* `is_star` (from `cst_imports_v1.is_star`)
* `kind` (“Import” vs “ImportFrom”)
* optional: `relative_level`

### Closure rule (safe default)

* Start frontier = modules from changed/deleted/added files
* Find importers whose `imported_module` matches frontier
* Add those importers’ file_ids to impacted
* Repeat by promoting **importer modules** into frontier when:

  * importer is `__init__.py` OR
  * the import was `is_star=True`
    This captures the common “re-export surface changes” cases without exploding to “everything imports everything”.

### Suggested commit slices

1. **Module index builder + persistence**
2. **Import edges builder + persistence**
3. **Impact closure algorithm + tests**
4. **Wire into incremental runner plan object: (changed, deleted, impacted)**

### What to deprecate

* Any earlier notion of “impacted = changed only” (if it exists in scripts/docs).

  * Mark as incorrect and remove.

---

# PR15 — Incremental relationship outputs + CPG edges upsert (impacted files)

### Goal

Recompute only:

* relationship outputs (`rel_*`) for **impacted files**
* `cpg_edges` for **impacted files**

…and upsert them into the state store.

### Key minimal-diff tactic

**Do not rewrite the relspec compiler.**
Instead, reuse your existing relationship execution path, but provide **scoped inputs** for “site tables” (callsites/name_refs/imports/occurrences/etc) by reading only impacted partitions from the state store.

Concretely:

* Keep “target tables” global (defs, symbol_information, dims)
* Override “site tables” in a composite resolver with in-memory filtered tables

### New files

* `src/incremental/relspec_update.py`
* `src/incremental/edges_update.py`

### Modified files (small, optional)

* `src/hamilton_pipeline/modules/cpg_build.py`

  * Only if you want to expose `_CompositePlanResolver` publicly; otherwise copy a tiny resolver wrapper into `incremental/`.

### Exact signatures

#### `src/incremental/relspec_update.py`

```python
from __future__ import annotations
import pyarrow as pa
from ibis.backends import BaseBackend
from relspec.compiler import FilesystemPlanResolver, InMemoryPlanResolver
from relspec.engine import PlanResolver
from relspec.registry import DatasetCatalog, DatasetLocation
from incremental.state_store import StateStore

SCOPED_SITE_DATASETS: tuple[str, ...] = (
    "cst_name_refs_v1",
    "cst_callsites_v1",
    "cst_imports_norm_v1",
    "scip_occurrences_norm_v1",
    "diagnostics_norm_v1",
    "type_exprs_norm_v1",
    # add others as needed
)

GLOBAL_TARGET_DATASETS: tuple[str, ...] = (
    "cst_defs_norm_v1",
    "scip_symbol_information_v1",
    "scip_symbol_relationships_v1",
    "dim_qualified_names_v1",
)

def build_state_catalog(store: StateStore) -> DatasetCatalog:
    """Catalog that points dataset_name -> DatasetLocation(path=state/datasets/<name>/)."""

def load_scoped_tables(
    *,
    store: StateStore,
    dataset_names: tuple[str, ...],
    impacted_file_ids: list[str],
) -> dict[str, pa.Table]:
    """Read only impacted partitions (file_id in impacted set)."""

def make_incremental_resolver(
    *,
    store: StateStore,
    ibis_backend: BaseBackend,
    impacted_file_ids: list[str],
) -> PlanResolver:
    """Composite: in-memory scoped tables override filesystem base tables."""
```

#### `src/incremental/edges_update.py`

```python
import pyarrow as pa
from incremental.state_store import StateStore

def update_edges_state(
    *,
    store: StateStore,
    impacted_file_ids: list[str],
    cpg_edges_delta: pa.Table,    # edges only for impacted
    deleted_file_ids: list[str],
    repo_id: str | None = None,
) -> None:
    """Upsert edge partitions by origin_file_id derived from path."""
```

### Deriving `origin_file_id` for relationship outputs & edges

You already have deterministic ID tooling (`prefixed_hash_id`). Use it:

```python
from arrowdsl.schema.ids import prefixed_hash_id
import pyarrow.compute as pc

def origin_file_id_col(path_col: pa.Array, repo_id: str | None) -> pa.Array:
    # if repo_id is None, hash only path; else include repo_id
    # simplest minimal: hash(path) and keep repo_id optional in later PR
    return prefixed_hash_id(path_col, prefix="file")
```

Then:

* attach `origin_file_id` to relationship outputs & cpg_edges delta tables
* upsert partitioned by `origin_file_id`

### Suggested commit slices

1. **Build state catalog from StateStore**
2. **Load scoped site tables by impacted_file_ids**
3. **Execute relationship outputs with composite resolver**
4. **Build cpg_edges delta from relationship outputs**
5. **Upsert relationship outputs + edges partitions**

### What to deprecate

* Full rewrite of relationship outputs every run (filesystem mode writing entire dataset)
* Any code path that assumes `cpg_edges` is only ever a single monolithic parquet file

Keep monolithic output as a *materialization option* (see PR16), but not the internal canonical store.

---

# PR16 — Incremental nodes + props upsert (through props), plus “publish” materializations

### Goal

Complete “incremental everything through props” by updating:

* **`cpg_nodes`** for **changed files only**
* **`cpg_props`** for **(changed ∪ impacted)** files
  (simplest correct rule: if edges changed for a file, its props may also change)

…and optionally materialize monolithic outputs:

* `build/.../cpg_nodes.parquet`
* `build/.../cpg_edges.parquet`
* `build/.../cpg_props.parquet`

from the partitioned canonical state store.

### New files

* `src/incremental/nodes_update.py`
* `src/incremental/props_update.py`
* `src/incremental/publish.py`
* `scripts/run_incremental_pipeline.py` (new preferred entrypoint)

### Signatures

#### `src/incremental/nodes_update.py`

```python
import pyarrow as pa
from incremental.state_store import StateStore

def update_nodes_state(
    *,
    store: StateStore,
    changed_file_ids: list[str],
    deleted_file_ids: list[str],
    cpg_nodes_delta: pa.Table,   # nodes only for changed
) -> None:
    """Upsert by file_id; delete removed file partitions."""
```

#### `src/incremental/props_update.py`

```python
import pyarrow as pa
from incremental.state_store import StateStore

def annotate_props_with_file_id(
    *,
    props: pa.Table,
    nodes_for_scope: pa.Table,
    edges_for_scope: pa.Table,
) -> pa.Table:
    """
    Adds file_id column used ONLY for partitioning in the internal props store:
      file_id := coalesce(
         nodes.file_id where props.entity_id == nodes.node_id,
         hash(edges.path) where props.entity_id == edges.edge_id,
         "__global__"
      )
    """

def update_props_state(
    *,
    store: StateStore,
    props_scope_file_ids: list[str],
    deleted_file_ids: list[str],
    cpg_props_delta: pa.Table,
    nodes_scope: pa.Table,
    edges_scope: pa.Table,
) -> None:
    """Partition-upsert props by file_id (internal-only partition key)."""
```

#### `src/incremental/publish.py`

```python
from __future__ import annotations
from pathlib import Path
import pyarrow as pa
from incremental.state_store import StateStore

def publish_monolithic_outputs(
    *,
    store: StateStore,
    output_dir: Path,
    datasets: tuple[str, ...] = ("cpg_nodes_v1", "cpg_edges_v1", "cpg_props_v1"),
) -> None:
    """Scan partitioned datasets, drop internal partition cols, write single parquet per dataset."""
```

### Deprecations to finalize in this PR

* **Deprecate** `build/e2e_full_pipeline/*.parquet` as the canonical source of truth.

  * Keep writing them only via `publish_monolithic_outputs()` as an optional materialization.
* **Deprecate** using env var `CODEANATOMY_PIPELINE_MODE` as the primary control surface.

  * Keep as alias → config for one cycle, then remove.

### Suggested commit slices

1. **Nodes delta: changed files only**
2. **Edges delta: already done in PR15 (wire into props scope)**
3. **Props delta: annotate with file_id + partition upsert**
4. **Publish monolithic outputs from canonical partitioned datasets**
5. **Docs + “incremental runner” CLI**

---

## One concrete “incremental runner” skeleton (what ties PR11–PR16 together)

This is the orchestration shape you’ll end up with in `scripts/run_incremental_pipeline.py`:

```python
def run_incremental(repo_root: Path, state_dir: Path, out_dir: Path, cfg: IncrementalConfig) -> None:
    store = StateStore(state_dir)
    store.ensure_dirs()

    # 1) snapshot + diff
    repo_files_cur = scan_repo_to_repo_files_v1(repo_root)          # existing
    snap_cur = build_repo_snapshot(repo_files_cur)
    snap_prev = read_snapshot_if_exists(store)
    changes = diff_snapshots(snap_prev, snap_cur)

    changed_file_ids = extract_ids(changes, kinds=("added","modified"))
    deleted_file_ids = extract_ids(changes, kinds=("deleted",))

    if snap_prev is None:
        # full bootstrap path
        full = run_full_pipeline(repo_root, out_dir, ...)           # existing
        bootstrap_latest_state(store=store, full_run_outputs=full)
        write_snapshot(store, snap_cur)
        return

    # 2) update extract+normalize for changed files only
    delta_extract = run_extract_for_files(repo_files_cur, changed_file_ids)
    update_extract_state(store=store, changed_repo_files=..., deleted_file_ids=..., extract_outputs=delta_extract)

    delta_norm = run_normalize_for_files(delta_extract, changed_file_ids)
    update_normalize_state(store=store, changed_inputs=..., deleted_file_ids=..., normalize_outputs=delta_norm)

    # 3) impact analysis → impacted file set
    module_index = load_or_build_module_index(store, repo_files_cur)
    import_edges = build_import_edges_from_state(store, module_index)   # uses cst_imports_norm
    impacted_file_ids = compute_impacted_files(...)

    # 4) relationships + edges for impacted
    rel_outputs_delta = run_relationships_scoped(store, impacted_file_ids)
    upsert_relationship_outputs(store, rel_outputs_delta, impacted_file_ids)

    edges_delta = build_edges_scoped(store, rel_outputs_delta, impacted_file_ids)
    update_edges_state(store=store, impacted_file_ids=impacted_file_ids, cpg_edges_delta=edges_delta, deleted_file_ids=deleted_file_ids)

    # 5) nodes for changed
    nodes_delta = build_nodes_scoped(store, changed_file_ids)
    update_nodes_state(store=store, changed_file_ids=changed_file_ids, deleted_file_ids=deleted_file_ids, cpg_nodes_delta=nodes_delta)

    # 6) props for (changed ∪ impacted)
    props_scope = sorted(set(changed_file_ids) | set(impacted_file_ids))
    props_delta = build_props_scoped(store, props_scope)
    update_props_state(...)

    # 7) persist snapshot + optional publish
    write_snapshot(store, snap_cur)
    publish_monolithic_outputs(store=store, output_dir=out_dir)
```

---

## Why this plan is “maximally incremental” without destabilizing your architecture

* **Extraction & normalization** are incrementally upserted by `file_id` partitions.
* **Relationship resolution** is recomputed for **impacted** files only (not whole repo), but still joins to global targets where needed.
* **Edges** are derived from those relationship deltas and upserted by `origin_file_id`.
* **Props** are recomputed and upserted by `file_id` via an internal partition key, without changing the public schema.

---

