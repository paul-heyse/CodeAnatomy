# Extract Streamline & Engine Integration Plan

## Goal
Aggressively streamline `src/extract` to align with the DataFusion/Ibis/SQLGlot/Delta architecture, reduce duplicated pipeline logic, and centralize execution policy in engine/runtime layers. The outcome is a smaller extract surface that composes engine‑level primitives consistently across extract, normalize, and incremental workflows.

## Guiding Principles
- **Single execution surface:** Extract should use the same session/runtime orchestration as the engine stack.
- **SQL‑first query semantics:** Query/predicate/projection logic should flow through SQLGlot policy and DataFusion execution paths.
- **Centralized normalization:** Schema alignment/validation should go through a unified finalize/normalization pipeline.
- **Minimal extractor boilerplate:** Extractor modules should focus on “facts extraction,” not pipeline wiring.
- **Delta‑native scans:** Repo state, worklists, and incremental scans should leverage Delta/DF instead of ad‑hoc Python diffs.

---

## Scope 1 — Unify extract sessions with EngineSession
**Objective:** Remove extract‑specific session bootstrapping and reuse engine runtime wiring (DataFusion profile, Ibis registry, diagnostics).

**Status:** Completed (ExtractSession now wraps EngineSession).

**Representative pattern**
```python
# engine/session_factory.py
session = build_engine_session(ctx=exec_ctx)
ctx = session.ctx
backend = session.ibis_backend
```

**Target files**
- `src/extract/session.py` (refactor or decommission)
- `src/extract/helpers.py`
- `src/extract/*_extract.py` (any module that builds sessions)

**Implementation checklist**
- ✅ Add an `ExtractSession` constructor that wraps `EngineSession`.
- ✅ Route extract entry points through `build_engine_session` to obtain DataFusion/Ibis handles.
- ✅ Ensure diagnostics and dataset registry are available to extract pipelines.

**Decommission candidates**
- None (session wrapper retained to provide extract‑specific accessors).

---

## Scope 2 — Create a shared extract pipeline helper
**Objective:** Centralize the “reader → query/projection → normalization → materialization” sequence into one reusable API.

**Status:** Completed (shared helper API in `src/extract/helpers.py`, applied across extractors).

**Representative pattern**
```python
# extract/helpers.py
plan = extract_plan_from_reader(
    name,
    reader,
    session=session,
    options=ExtractPlanOptions(normalize=normalize, evidence_plan=evidence_plan),
)
return materialize_extract_plan(name, plan, ctx=session.exec_ctx, options=options)
```

**Target files**
- `src/extract/helpers.py` (new pipeline entry points)
- `src/extract/ast_extract.py`
- `src/extract/cst_extract.py`
- `src/extract/scip_extract.py`
- `src/extract/tree_sitter_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/runtime_inspect_extract.py`
- `src/extract/repo_scan.py`
- `src/extract/repo_blobs.py`

**Implementation checklist**
- ✅ Add `extract_plan_from_reader(...)`, `extract_plan_from_rows(...)`, and `extract_plan_from_row_batches(...)` helpers.
- ✅ Add `ExtractPlanOptions` to centralize normalize/evidence/repo_id handling.
- ✅ Consolidate evidence gating + registry query spec application into shared helper.
- ✅ Replace per‑extractor orchestration logic with the shared helper.
- ✅ Add `raw_plan_from_rows(...)` to support runtime‑inspect joins without duplicating reader logic.

**Decommission candidates**
- Duplicated plan assembly boilerplate in extractor modules (completed).
- `empty_ibis_plan` (still used; revisit with Scope 7 when evidence projection moves into registry specs).

---

## Scope 3 — SQLGlot policy normalization for worklists and SQL execution
**Objective:** Enforce centralized SQLGlot policy on worklist queries and DataFusion execution.

**Status:** Completed (worklist SQLGlot expressions normalized via `datafusion_compile` policy; output datasets registered from Delta locations).

**Representative pattern**
```python
policy = resolve_sqlglot_policy("datafusion_compile")
expr = normalize_expr(expr, options=NormalizeExprOptions(policy=policy))
df = df_from_sqlglot(ctx, expr)
```

**Target files**
- `src/extract/worklists.py`
- `src/sqlglot_tools/optimizer.py` (policy references)
- `src/datafusion_engine/df_builder.py` (if needed for policy hooks)

**Implementation checklist**
- ✅ Normalize worklist SQLGlot expressions via policy before execution.
- ✅ Replace custom INFO_SCHEMA SQL checks with `table_names_snapshot`.
- ⏸️ `SqlExprSpec` wrapper for diagnostics/fingerprints (optional; skipped).

**Decommission candidates**
- `_table_exists` manual INFO_SCHEMA query in `src/extract/worklists.py` (replaced with introspector helper).

---

## Scope 4 — Centralize schema normalization via finalize pipeline
**Objective:** Remove custom schema alignment/encoding paths in extract and route normalization through the unified finalize pipeline.

**Status:** Completed (normalize-only lane via finalize; extract normalization now uses finalize schema policy).

**Representative pattern**
```python
finalize_ctx = finalize_context_for_dataset(name, ctx=ctx)
result = finalize_ctx.run(table, ctx=ctx)
normalized = result.good
```

**Target files**
- `src/extract/schema_ops.py`
- `src/extract/helpers.py`
- `src/datafusion_engine/finalize.py` (add normalization mode if needed)

**Implementation checklist**
- ✅ Add a finalize “normalize‑only” lane for extract outputs.
- ✅ Replace `normalize_extract_output` logic to use finalize normalization.
- ✅ Ensure streaming normalization uses finalize’s schema policy when compatible.

**Decommission candidates**
- Custom align/encode logic inside `normalize_extract_output` (replaced by finalize normalization).

---

## Scope 5 — Delta‑native repo scans and worklists
**Objective:** Store repo scans as Delta datasets and drive worklists with SQL/DataFusion rather than Python diffs.

**Status:** Completed (repo scans write via extract materialization; worklists register Delta datasets).

**Representative pattern**
```python
# datafusion_engine / storage layer
location = runtime.dataset_location("repo_files_v1")
write_delta_dataset(table, location=location)
# later: compute worklist via SQL against Delta table
```

**Target files**
- `src/extract/repo_scan.py`
- `src/extract/repo_blobs.py`
- `src/extract/worklists.py`
- `src/storage/deltalake/*`

**Implementation checklist**
- ✅ Persist repo scan outputs via extract materialization (Delta when configured).
- ✅ Replace hash‑index cache with SQL diff query (removed hash index reuse).
- ✅ Register output datasets from Delta locations before worklist SQL execution.

**Decommission candidates**
- `RepoFileFingerprint` + `repo_scan_hash_index` (removed).

---

## Scope 6 — Consolidate row‑batch conversion utilities
**Objective:** Remove duplicated row‑batch conversion utilities and centralize in Arrow/Storage utilities.

**Status:** Completed (row‑batch conversion helpers centralized in `arrowdsl.schema.build`).

**Representative pattern**
```python
# arrowdsl.schema.build or storage.ipc_utils
reader = record_batch_reader_from_rows(schema, rows)
```

**Target files**
- `src/extract/helpers.py`
- `src/extract/batching.py`
- `src/arrowdsl/schema/build.py` or `src/storage/ipc.py` (new canonical helper)

**Implementation checklist**
- ✅ Centralize row‑batch conversion helpers in `arrowdsl.schema.build`.
- ✅ Route extract helper wrappers to the canonical helpers.
- ✅ Remove redundant `src/extract/batching.py`.

**Decommission candidates**
- `src/extract/batching.py` (removed).

---

## Scope 7 — Evidence projection moved into registry query specs
**Objective:** Make evidence projection an explicit part of registry query specs instead of extract‑side ad‑hoc projections.

**Status:** Completed (evidence projection resolved via registry query projection).

**Representative pattern**
```python
# datafusion_engine.extract_registry
spec = dataset_query(name, repo_id=repo_id, projection=required_columns)
```

**Target files**
- `src/extract/helpers.py`
- `src/datafusion_engine/extract_registry.py`
- `src/extract/evidence_plan.py`

**Implementation checklist**
- ✅ Add evidence-driven projection to `dataset_query`.
- ✅ Remove `apply_evidence_projection` from extract pipeline.
- ✅ Inject evidence-required columns into query spec generation.

**Decommission candidates**
- `apply_evidence_projection` (removed).

---

## Scope 8 — Runtime‑aware parallelism
**Objective:** Make extract parallelism honor runtime CPU/I/O thread settings.

**Status:** Completed (parallel workers resolved from runtime profile).

**Representative pattern**
```python
max_workers = ctx.runtime.cpu_threads
parallel_map(items, fn, max_workers=max_workers)
```

**Target files**
- `src/extract/parallel.py`
- `src/extract/*_extract.py` (sites calling `parallel_map`)

**Implementation checklist**
- ✅ Resolve max_workers from `ExecutionContext.runtime` when unset.
- ✅ Update extractors to use runtime-aware worker counts.

**Decommission candidates**
- None (behavioral refactor only)

---

## Scope 9 — Evidence metadata and extractor defaults centralized
**Objective:** Centralize evidence metadata and extractor defaults under `datafusion_engine.extract_registry` so extract reads from a single canonical API.

**Representative pattern**
```python
# datafusion_engine.extract_registry
metadata = evidence_metadata(name)
defaults = extractor_defaults(template)
```

**Target files**
- `src/extract/evidence_specs.py`
- `src/extract/spec_helpers.py`
- `src/datafusion_engine/extract_registry.py`

**Implementation checklist**
- Add evidence metadata getters in registry.
- Migrate `evidence_specs` usage to registry APIs.
- Remove redundant caching logic in extract.

**Decommission candidates**
- `src/extract/evidence_specs.py` (if fully superseded)
- `_metadata_defaults` / `_feature_flag_rows` caches in `src/extract/spec_helpers.py` (if registry provides unified APIs)

---

## Scope 10 — Cleanup
**Objective:** Remove legacy/unused artifacts.

**Target files**
- `src/extract/Untitled` (remove if unused)

**Implementation checklist**
- Confirm unused file(s) and remove.

---

## Decommission List (post‑migration)
**Files**
- `src/extract/session.py` (if EngineSession is canonical)
- `src/extract/batching.py` (if replaced by centralized helpers)
- `src/extract/evidence_specs.py` (if registry owns evidence metadata)
- `src/extract/Untitled` (cleanup)

**Functions**
- `empty_ibis_plan` (if replaced by pipeline helper)
- `apply_evidence_projection`, `_projection_columns` (if registry query spec handles projections)
- `normalize_extract_output`, `normalize_extract_reader` (if finalize pipeline handles normalization)
- `_table_exists` in `src/extract/worklists.py` (replace with introspector helper)

---

## Sequencing Recommendation
1. **Scope 1** (EngineSession unification)
2. **Scope 2** (Shared extract pipeline helper)
3. **Scope 3** (SQLGlot policy normalization for worklists)
4. **Scope 4** (Finalize‑based normalization)
5. **Scope 5** (Delta‑native repo scans/worklists)
6. **Scope 6** (Row‑batch conversion consolidation)
7. **Scope 7** (Evidence projection in registry specs)
8. **Scope 8** (Runtime‑aware parallelism)
9. **Scope 9** (Evidence/defaults centralization)
10. **Scope 10** (Cleanup)

---

## Success Criteria
- Extract uses EngineSession (or equivalent) everywhere.
- All extract pipelines use a single orchestration helper.
- SQLGlot policy applied consistently to extract SQL queries.
- Normalization and validation are routed through finalize pipeline.
- Repo scan/worklist operations are Delta‑native with SQL diffing.
- Duplicate helpers are removed; extract code volume reduced significantly.
