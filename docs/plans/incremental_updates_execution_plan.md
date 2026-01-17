## Incremental Updates Execution Plan (v2)

### Goals
- Enable incremental updates keyed by stable `file_id` partitions.
- Keep correctness with explicit invalidation gates (rule signatures + schema metadata).
- Reuse existing IO and plan execution infrastructure (Hamilton, Arrow, Ibis, DataFusion).
- Preserve run bundle diagnostics and make incremental diffs auditable.

### Non-goals
- Full module index / import graph closure in the first iteration.
- Overhauling core relationship rule compilation.
- Introducing a separate orchestration stack outside Hamilton.

### Guiding Principles
- Reuse existing `file_id` derivation (`extract.registry_ids.repo_file_id_spec`) everywhere.
- Prefer predicate pushdown on `file_id` over partition path enumeration.
- Use existing dataset writers with partitioned upsert; do not fork IO logic.
- Keep canonical state in a persistent `state_dir`, materialize monolithic outputs only
  when needed.

---

## Scope 1: Incremental config + snapshot/diff plumbing

### Objective
Add a persistent `state_dir` and snapshot/diff tables without changing execution
behavior yet.

### Design
- Introduce `IncrementalConfig` and `state_dir` in pipeline types.
- Snapshots are derived from existing `repo_files_v1` table.
- Diff table is written both to `state_dir` and run bundle diagnostics.
- Default `state_dir` should live under `build/state` (not `.state`).

### Code patterns
```python
@dataclass(frozen=True)
class IncrementalConfig:
    enabled: bool = False
    state_dir: Path | None = None
    repo_id: str | None = None


def build_repo_snapshot(repo_files: pa.Table) -> pa.Table:
    cols = ["file_id", "path", "file_sha256", "size_bytes", "mtime_ns"]
    return repo_files.select(cols)
```

### Target files
- src/hamilton_pipeline/pipeline_types.py
- src/hamilton_pipeline/execution.py
- src/obs/repro.py
- src/obs/manifest.py
- src/incremental/snapshot.py (new)
- src/incremental/diff.py (new)
- src/incremental/types.py (new)

### Implementation checklist
- [x] Add `IncrementalConfig` to pipeline types and execution options.
- [x] Default `state_dir` to `build/state` when incremental is enabled.
- [x] Build snapshot from `repo_files_v1`.
- [x] Diff snapshot vs previous; emit `incremental_diff_v1`.
- [x] Write snapshot/diff to both state store and run bundle diagnostics.

---

## Scope 2: Partitioned Parquet upsert writer (core primitive)

### Objective
Enable replace-only-these-partitions writes using the existing Parquet IO helpers.

### Design
- Add a `upsert_dataset_partitions_parquet` API to `arrowdsl/io/parquet.py`.
- Use `existing_data_behavior="delete_matching"` with partitioning on `file_id`.
- Provide a thin wrapper in `storage/io.py` if needed for reuse.

### Code patterns
```python
def upsert_dataset_partitions_parquet(
    table: pa.Table,
    *,
    base_dir: PathLike,
    partition_cols: Sequence[str],
    delete_partitions: Sequence[dict[str, str]] = (),
) -> str:
    for part in delete_partitions:
        _delete_partition_dir(base_dir, part)
    ds.write_dataset(
        table,
        base_dir=str(base_dir),
        format="parquet",
        partitioning=partition_cols,
        existing_data_behavior="delete_matching",
    )
    return str(base_dir)
```

### Target files
- src/arrowdsl/io/parquet.py
- src/storage/io.py (optional wrapper)
- tests/integration/test_partition_upsert.py (new)

### Implementation checklist
- [x] Add upsert API and partition deletion helper.
- [x] Ensure hive partitioning by `file_id` is supported.
- [x] Add tests to verify only impacted partitions change.

---

## Scope 3: Incremental extract and normalize (changed files only)

### Objective
Update extract/normalize datasets for changed files and store them in `state_dir`.

### Design
- Compute `changed_file_ids` from snapshot diff.
- Build filtered `repo_files_v1` for changed files only.
- Run existing extraction/normalization logic on the filtered inputs.
- Upsert each dataset by `file_id` into `state_dir`.
- Use predicate pushdown on `file_id` for downstream reads.

### Code patterns
```python
def filter_by_file_id(table: pa.Table, file_ids: set[str]) -> pa.Table:
    mask = pc.is_in(table["file_id"], value_set=list(file_ids))
    return table.filter(mask)


def dataset_query_for_file_ids(file_ids: Sequence[str]) -> QuerySpec:
    return QuerySpec.simple(
        predicate=ExprSpec(op="in", args=(ExprIR(op="field", name="file_id"), file_ids)),
        pushdown_predicate=ExprSpec(op="in", args=(ExprIR(op="field", name="file_id"), file_ids)),
    )
```

### Target files
- src/hamilton_pipeline/modules/extraction.py
- src/hamilton_pipeline/modules/normalization.py
- src/arrowdsl/plan/query.py
- src/arrowdsl/plan_helpers.py
- src/incremental/extract_update.py (new)
- src/incremental/normalize_update.py (new)

### Implementation checklist
- [x] Filter `repo_files_v1` to changed files only for extract.
- [x] Upsert extract datasets by `file_id` into `state_dir`.
- [x] Normalize only required datasets for mapping (initial scope).
- [x] Pushdown `file_id` filters in scans for incremental reads.

---

## Scope 4: Incremental relationship outputs (impacted files)

### Objective
Execute relationship rules only for impacted files and upsert results.

### Design
- Impacted set = `changed_file_ids` union `scip_changed_file_ids`.
- Use a scoped resolver that applies `file_id` predicate to site datasets.
- Keep target tables global (defs, symbol info, dims).
- Add `edge_owner_file_id` to relspec outputs using consistent rules.

### Code patterns
```python
def scoped_plan_resolver(
    base: PlanResolver,
    file_ids: Sequence[str],
) -> PlanResolver:
    class ScopedResolver(PlanResolver):
        def resolve(self, ref: DatasetRef, *, ctx: ExecutionContext) -> IbisPlan:
            plan = base.resolve(ref, ctx=ctx)
            if ref.name in SCOPED_SITE_DATASETS:
                expr = plan.expr.filter(plan.expr["file_id"].isin(file_ids))
                return IbisPlan(expr=expr, ordering=plan.ordering)
            return plan
    return ScopedResolver()
```

### Target files
- src/relspec/compiler.py
- src/relspec/engine.py
- src/relspec/contracts.py
- src/cpg/relationship_plans.py
- src/hamilton_pipeline/modules/cpg_build.py
- src/incremental/relspec_update.py (new)

### Implementation checklist
- [x] Compute impacted set from snapshot + SCIP document diff (targeted file_id set).
- [x] Implement scoped resolver with `file_id` predicate pushdown.
- [x] Add `edge_owner_file_id` to relationship outputs and contracts.
- [x] Upsert relationship outputs by `edge_owner_file_id`.

---

## Scope 5: Incremental CPG edges, nodes, props

### Objective
Recompute only impacted partitions and update state store.

### Design
- Edges partitioned by `edge_owner_file_id`.
- Nodes partitioned by `file_id`.
- Props split into `props_by_file_id` and `props_global` datasets.
- Monolithic outputs are materialized from state store on demand.

### Code patterns
```python
def split_props(props: pa.Table) -> tuple[pa.Table, pa.Table]:
    has_file = pc.is_valid(props["file_id"])
    return props.filter(has_file), props.filter(pc.invert(has_file))
```

### Target files
- src/cpg/build_edges.py
- src/cpg/build_nodes.py
- src/cpg/build_props.py
- src/hamilton_pipeline/modules/outputs.py
- src/incremental/edges_update.py (new)
- src/incremental/nodes_update.py (new)
- src/incremental/props_update.py (new)
- src/incremental/publish.py (new)

### Implementation checklist
- [x] Ensure edges include `edge_owner_file_id`.
- [x] Upsert edges by `edge_owner_file_id`.
- [x] Upsert nodes by `file_id`.
- [x] Split props into file-scoped vs global datasets.
- [x] Add optional monolithic publish step from state store.

---

## Scope 6: Invalidation gates (rules + schemas)

### Objective
Force safe rebuilds when rules or schemas change.

### Design
- Store rule signatures from `relspec.rules.cache`.
- Store schema identity metadata for each dataset in state store.
- On mismatch, mark full rebuild required (or per-stage rebuild).

### Code patterns
```python
def pipeline_signature() -> dict[str, str]:
    return {
        "rule_plan": rule_plan_signatures_cached(),
        "rule_graph": rule_graph_signature_cached(),
    }
```

### Target files
- src/relspec/rules/cache.py
- src/arrowdsl/schema/metadata.py
- src/incremental/invalidations.py (new)
- src/hamilton_pipeline/execution.py

### Implementation checklist
- [x] Persist rule signature snapshot with each run.
- [x] Compare signatures before reusing partitions.
- [x] Validate schema identity on read; reject stale partitions.

---

## Scope 7: Diagnostics + tests

### Objective
Make incremental decisions observable and verify parity vs full runs.

### Design
- Emit `incremental_diff_v1` into run bundle diagnostics.
- Add parity test comparing full vs incremental outputs (hashes or row counts).
- Extend `scripts/e2e_diagnostics_report.py` to include incremental coverage.

### Code patterns
```python
def table_digest(table: pa.Table) -> str:
    return hashlib.sha256(table.to_pydict().__repr__().encode("utf-8")).hexdigest()
```

### Target files
- src/obs/repro.py
- scripts/e2e_diagnostics_report.py
- tests/e2e/test_full_pipeline_repo.py
- tests/e2e/test_incremental_parity.py (new)

### Implementation checklist
- [x] Add incremental diff dataset to run bundle.
- [x] Add parity test for at least nodes, edges, props.
- [x] Expand diagnostics report to cover incremental state.

---

## Scope 8: Deferred (post-v1)

### Objective
Optional enhancements after incremental correctness is proven.

### Items
- Import graph closure (module index + import edges).
- Partial compaction for global datasets.
- Specialized heuristics for symbol-based impact propagation.

### Target files (future)
- src/incremental/module_index.py
- src/incremental/impact.py

### Implementation checklist
- [ ] Design import graph closure rules.
- [ ] Add module index table and import edges table.
- [ ] Validate closure on repo fixtures before enabling by default.

---

## Implementation Order (recommended)
1) Scope 1 (config + snapshot/diff)
2) Scope 2 (partition upsert)
3) Scope 3 (extract + normalize incremental)
4) Scope 4 (relationships incremental)
5) Scope 5 (edges/nodes/props incremental)
6) Scope 6 (invalidations)
7) Scope 7 (diagnostics + tests)
8) Scope 8 (deferred enhancements)
