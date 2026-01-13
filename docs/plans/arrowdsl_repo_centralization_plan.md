## ArrowDSL Repo Centralization Plan

### Goals
- Consolidate all Arrow usage behind ArrowDSL so plan-lane and kernel-lane behavior is uniform.
- Expand use of Acero streaming plans while preserving pipeline-breaker semantics and ordering metadata.
- Centralize IO, schema/row factories, and compute macros to eliminate per-module duplication.
- Leverage advanced PyArrow features (dataset scanning, nested types, dictionary encoding, UDFs).

### Constraints
- Preserve schema metadata, ordering semantics, and contract enforcement.
- Keep plan-lane (Acero) and kernel-lane responsibilities explicit and separable.
- Avoid relative imports and keep all modules fully typed (pyright strict + pyrefly).
- Prefer centralized factories/macros over ad hoc `pyarrow` calls in feature modules.

---

### Scope 1: Row/Table Construction Factory Consolidation
**Description**
Replace direct `pa.Table.from_pylist`/`pa.array` usage with ArrowDSL row/table factories so empty
tables, nested arrays, and schema metadata are handled consistently across modules.

**Code patterns**
```python
# src/arrowdsl/schema/factories.py
def rows_to_table(rows: Sequence[Mapping[str, object]], schema: SchemaLike) -> TableLike:
    if not rows:
        return empty_table(schema)
    return table_from_rows(schema, rows)
```

**Target files**
- Add: `src/arrowdsl/schema/factories.py`
- Update: `src/normalize/span_pipeline.py`
- Update: `src/normalize/diagnostics.py`
- Update: `src/obs/stats.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`
- Update: `src/cpg/builders.py`
- Update: `src/hamilton_pipeline/modules/normalization.py`

**Implementation checklist**
- [ ] Add shared row/table factories (empty table + row table + nested arrays).
- [ ] Replace `pa.Table.from_pylist` and manual empty-table assembly in normalization/obs/cpg.
- [ ] Ensure schema metadata is preserved on factory outputs.

**Status**
Planned.

---

### Scope 2: Hash + Validity Mask Macro Layer
**Description**
Centralize mask + hash generation (kernel-lane and plan-lane) so ID creation and masking are
uniform and reusable.

**Code patterns**
```python
# src/arrowdsl/compute/ids.py
def masked_hash_expr(spec: HashSpec, *, required: Sequence[str]) -> ComputeExpression:
    mask = valid_mask_expr(required)
    expr = hash_expression(spec)
    return ensure_expression(pc.if_else(mask, expr, pc.scalar(None)))
```

**Target files**
- Add: `src/arrowdsl/compute/masks.py`
- Add: `src/arrowdsl/compute/ids.py`
- Update: `src/normalize/ids.py`
- Update: `src/extract/hashing.py`
- Update: `src/extract/scip_extract.py`
- Update: `src/cpg/builders.py`
- Update: `src/cpg/emit_edges.py`

**Implementation checklist**
- [ ] Add kernel-lane and plan-lane mask helpers (array + expression forms).
- [ ] Route ID creation through `masked_hash_expr` and `masked_hash_array`.
- [ ] Remove per-module validity mask duplication.

**Status**
Planned.

---

### Scope 3: Plan Execution Consolidation (Pipeline-Breaker Aware)
**Description**
Replace direct `Declaration.to_table()` usage with `Plan` + `PlanSpec` execution helpers to
centralize pipeline-breaker handling, streaming vs materialization, and ordering metadata.

**Code patterns**
```python
# src/arrowdsl/plan/runner.py
result = run_plan(plan, ctx=ctx, prefer_reader=True, attach_ordering_metadata=True)
```

**Target files**
- Update: `src/normalize/pipeline.py`
- Update: `src/hamilton_pipeline/modules/normalization.py`
- Update: `src/cpg/relations.py`
- Update: `src/relspec/compiler.py`
- Update: `src/arrowdsl/plan/runner.py`

**Implementation checklist**
- [ ] Ensure all plan execution flows go through `run_plan`/`PlanSpec`.
- [ ] Propagate pipeline-breaker metadata to outputs.
- [ ] Prefer streaming `to_reader()` when safe (no breakers + non-canonical determinism).

**Status**
Planned.

---

### Scope 4: Dataset Scan + IO Unification
**Description**
Centralize dataset scan and IO (parquet/ipc) behind ArrowDSL IO helpers and scan specs,
including schema alignment, encoding policies, and metadata sidecars.

**Code patterns**
```python
# src/arrowdsl/io/parquet.py
def write_dataset(data: DatasetWriteInput, *, config: DatasetWriteConfig) -> str:
    aligned = apply_schema_and_encoding(data, config.schema, config.encoding_policy)
    return write_dataset_parquet(aligned, config)
```

**Target files**
- Add: `src/arrowdsl/io/__init__.py`
- Add: `src/arrowdsl/io/parquet.py`
- Add: `src/arrowdsl/io/ipc.py`
- Update: `src/storage/parquet.py`
- Update: `src/storage/ipc.py`
- Update: `src/hamilton_pipeline/arrow_adapters.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`

**Implementation checklist**
- [ ] Move IO defaults and schema alignment into ArrowDSL IO helpers.
- [ ] Standardize dataset metadata sidecar generation via `ParquetMetadataSpec`.
- [ ] Update Hamilton adapters to call ArrowDSL IO wrappers.

**Status**
Planned.

---

### Scope 5: Stats + Quality Plan-Lane Helpers
**Description**
Introduce ArrowDSL plan helpers for dataset/column stats and quality tables to replace
per-module Python loops and manual table assembly.

**Code patterns**
```python
# src/arrowdsl/plan/stats.py
def dataset_stats_plan(plan: Plan, *, ctx: ExecutionContext) -> Plan:
    return plan.aggregate(group_keys=("dataset_name",), aggs=(("rows", "count"),), ctx=ctx)
```

**Target files**
- Add: `src/arrowdsl/plan/stats.py`
- Update: `src/obs/stats.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`
- Update: `src/cpg/quality.py`

**Implementation checklist**
- [ ] Build plan-lane stats helpers with aggregate + projection defaults.
- [ ] Replace row-iteration stats builders with ArrowDSL plans.
- [ ] Ensure quality tables share a single schema factory.

**Status**
Planned.

---

### Scope 6: Join/Projection Defaults + Metadata Enrichment
**Description**
Standardize join/projection defaults (join keys, output suffixing, metadata columns) through
ArrowDSL join helpers to eliminate repeated logic in normalize/cpg.

**Code patterns**
```python
# src/arrowdsl/plan/joins.py
config = JoinConfig.on_keys(keys=("code_unit_id",), left_output=left_cols, right_output=right_cols)
joined = left_join(left, right, config=config, use_threads=True)
```

**Target files**
- Update: `src/arrowdsl/plan/joins.py`
- Update: `src/arrowdsl/plan_helpers.py`
- Update: `src/normalize/arrow_utils.py`
- Update: `src/hamilton_pipeline/modules/normalization.py`
- Update: `src/cpg/relations.py`
- Update: `src/normalize/bytecode_cfg.py`

**Implementation checklist**
- [ ] Add standardized join config factories for common patterns.
- [ ] Centralize metadata join patterns (code_unit/file/path).
- [ ] Ensure projection helpers preserve ordering metadata.

**Status**
Planned.

---

### Scope 7: Provenance + Scanner Telemetry Consolidation
**Description**
Use `QuerySpec` provenance projection and `ScanContext.telemetry()` across the repo to unify
dataset scan telemetry and provenance columns without per-module bespoke logic.

**Code patterns**
```python
# src/arrowdsl/plan/query.py
scan_ctx = dataset_spec.scan_context(dataset, ctx)
telemetry = scan_ctx.telemetry()
plan = scan_ctx.to_plan(label=dataset_spec.name)
```

**Target files**
- Update: `src/arrowdsl/plan/query.py`
- Update: `src/schema_spec/specs.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`
- Update: `src/obs/stats.py`

**Implementation checklist**
- [ ] Adopt provenance-aware projections via `ctx.provenance`.
- [ ] Route scan telemetry through `ScanContext.telemetry()` everywhere.
- [ ] Standardize provenance column naming via `PROVENANCE_COLS`.

**Status**
Planned.

---

### Scope 8: Nested Types + Dictionary Normalization
**Description**
Promote list_view/map/union/dictionary patterns into ArrowDSL helpers and ensure dictionary
encoding + chunk normalization precede joins/sorts to improve performance and memory use.

**Code patterns**
```python
# src/arrowdsl/schema/encoding.py
table = encode_columns(table, specs=encoding_specs)
table = table.combine_chunks().unify_dictionaries()
```

**Target files**
- Update: `src/arrowdsl/schema/arrays.py`
- Update: `src/arrowdsl/schema/nested_builders.py`
- Update: `src/arrowdsl/schema/encoding.py`
- Update: `src/normalize/diagnostics.py`
- Update: `src/normalize/span_pipeline.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/cpg/build_nodes.py`

**Implementation checklist**
- [ ] Standardize list_view and map builders via nested array factory.
- [ ] Add dictionary/unify helpers before join/sort paths.
- [ ] Replace manual nested array construction in normalize/extract.

**Status**
Planned.

---

### Scope 9: UDF + Compute Options Registry
**Description**
Centralize compute options and UDF registration for span/diagnostic transforms to avoid
Python loops and standardize kernel usage.

**Code patterns**
```python
# src/arrowdsl/compute/udfs.py
name = ensure_span_bytes_udf()
expr = pc.call_function(name, [pc.field("line"), pc.field("col"), pc.field("encoding")])
```

**Target files**
- Update: `src/arrowdsl/compute/udfs.py`
- Update: `src/arrowdsl/compute/macros.py`
- Update: `src/normalize/spans.py`
- Update: `src/normalize/diagnostics.py`
- Update: `src/cpg/emit_props.py`

**Implementation checklist**
- [ ] Add UDF registrations for span/position normalization where vectorizable.
- [ ] Centralize compute option builders (CastOptions, ScalarAggregateOptions).
- [ ] Replace per-row loops with compute expressions where feasible.

**Status**
Planned.

