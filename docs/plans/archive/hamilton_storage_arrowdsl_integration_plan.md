## Hamilton + Storage ArrowDSL Integration Plan

### Goals
- Replace bespoke storage and pipeline table handling with ArrowDSL schema/evolution,
  compute, and plan helpers.
- Keep dataset I/O aligned with Acero’s plan-lane execution model (streaming where possible).
- Maximize reuse of ArrowDSL kernels and join utilities for normalization outputs.
- Preserve existing schemas/contracts and output semantics.

### Constraints
- Preserve current dataset schemas and contract versions unless explicitly updated in scope items.
- Keep plan-lane work inside Acero and kernel-lane work inside ArrowDSL compute helpers.
- Avoid adding new non-Arrow runtime dependencies.
- Maintain strict typing and Ruff compliance (no suppressions).

---

### Scope 1: Standardize empty-table creation + schema metadata
**Description**
Replace bespoke empty-table builders and ad hoc schema metadata attachment with the shared
ArrowDSL schema helpers. This keeps schema metadata consistent across IPC/Parquet and
reduces duplication across pipeline nodes.

**Code pattern**
```python
# src/storage/ipc.py
from arrowdsl.schema.schema import empty_table
from arrowdsl.schema.schema import SchemaMetadataSpec

schema = SchemaMetadataSpec(schema_metadata=meta).apply(table.schema)
empty = empty_table(schema)
```

**Target files**
- Update: `src/storage/ipc.py`
- Update: `src/hamilton_pipeline/modules/extraction.py`
- Update: `src/hamilton_pipeline/modules/cpg_build.py`

**Implementation checklist**
- [x] Replace local `_empty_table(...)` helpers with `arrowdsl.schema.schema.empty_table`.
- [x] Apply schema metadata via `SchemaMetadataSpec` before writing IPC files when needed.
- [x] Ensure empty fallback outputs use contract or inferred schemas.

**Completed changes**
- `src/storage/ipc.py`: added schema metadata application before IPC writes.
- `src/hamilton_pipeline/modules/extraction.py`: replaced local empty helpers with `empty_table`.
- `src/hamilton_pipeline/modules/cpg_build.py`: empty fallback outputs use
  `empty_table(contract.schema)`.

**Status**
Completed.

---

### Scope 2: ArrowDSL-first dataset I/O (alignment + encoding)
**Description**
Wrap Parquet dataset writing in ArrowDSL schema alignment and encoding policy application.
This ensures output datasets consistently carry aligned schemas and dictionary encoding
policies before writing, improving scan compatibility and determinism.

**Code pattern**
```python
# src/storage/parquet.py
from arrowdsl.schema.schema import SchemaTransform, EncodingPolicy

transform = SchemaTransform(schema=target_schema, safe_cast=True, keep_extra_columns=False)
encoded = EncodingPolicy(specs=encoding_specs).apply(transform.apply(table))
write_dataset_parquet(encoded, base_dir, opts=opts, overwrite=overwrite)
```

**Target files**
- Update: `src/storage/parquet.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`
- Update: `src/hamilton_pipeline/modules/cpg_build.py`

**Implementation checklist**
- [x] Add optional `schema`/`encoding_policy` parameters to Parquet helpers.
- [x] Apply `SchemaTransform` + `EncodingPolicy` before dataset writes.
- [x] Use dataset specs (or inferred schemas) to select target schemas.

**Completed changes**
- `src/storage/parquet.py`: added schema/encoding alignment hook for dataset writes.
- `src/hamilton_pipeline/modules/outputs.py`: passes schema + encoding policy for debug datasets.
- `src/hamilton_pipeline/modules/cpg_build.py`: passes schema + encoding policy for relspec inputs.

**Status**
Completed.

---

### Scope 3: Stream-first dataset writes via RecordBatchReader
**Description**
Enable streaming outputs for dataset writes by accepting `RecordBatchReader` inputs
and emitting them to Parquet datasets directly. This aligns storage with Acero’s
push-based execution and reduces materialization pressure.

**Code pattern**
```python
# src/storage/parquet.py
import pyarrow.dataset as ds

if isinstance(data, pa.RecordBatchReader):
    ds.write_dataset(data, base_dir=str(base_path), format="parquet", ...)
```

**Target files**
- Update: `src/storage/parquet.py`
- Update: `src/arrowdsl/plan/runner.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`

**Implementation checklist**
- [x] Accept `RecordBatchReader` in dataset write helpers.
- [x] Add plan-runner helpers to return readers when plans are streamable.
- [x] Use streamable outputs in materialization paths where possible.

**Completed changes**
- `src/storage/parquet.py`: accepts `RecordBatchReaderLike` and streams when possible.
- `src/arrowdsl/plan/runner.py`: added `run_plan_streamable`.
- `src/hamilton_pipeline/modules/outputs.py`: supports reader-backed dataset writes and reporting.

**Status**
Completed.

---

### Scope 4: Replace Python row loops in normalization with ArrowDSL kernels/joins
**Description**
Use ArrowDSL compute kernels and join helpers to remove Python-side row processing
in normalization outputs (e.g., qualified-name dimension and callsite candidates).
This keeps logic vectorized and consistent with plan/kernal-lane separation.

**Code pattern**
```python
# src/hamilton_pipeline/modules/normalization.py
from arrowdsl.compute.kernels import explode_list_column
from arrowdsl.plan.joins import left_join, JoinConfig

exploded = explode_list_column(...)
joined = left_join(exploded, meta_table, JoinConfig(keys=("call_id",)))
```

**Target files**
- Update: `src/hamilton_pipeline/modules/normalization.py`
- Update: `src/arrowdsl/compute/kernels.py`
- Update: `src/arrowdsl/plan/joins.py`

**Implementation checklist**
- [x] Replace `dim_qualified_names` Python list building with Arrow compute kernels.
- [x] Replace `callsite_qname_candidates` Python metadata join with ArrowDSL join helpers.
- [x] Preserve ordering + schema metadata after kernel operations.

**Completed changes**
- `src/hamilton_pipeline/modules/normalization.py`: vectorized qname dimension + candidates.
- `src/arrowdsl/compute/kernels.py`: added `distinct_sorted` + `flatten_list_struct_field`.
- `src/arrowdsl/plan/joins.py`: added `JoinConfig.on_keys` for consistent joins.

**Status**
Completed.

---

### Scope 5: DatasetSource adoption for plan-lane execution
**Description**
Use `DatasetSource` and ArrowDSL plan helpers in pipeline modules to keep CPG build
and relationship stages streamable. This aligns with the Acero ExecPlan model and
avoids premature table materialization.

**Code pattern**
```python
# src/cpg/build_edges.py
from arrowdsl.plan.source import DatasetSource

inputs = EdgeBuildInputs(
    relationship_outputs={"rel_name_symbol": DatasetSource(dataset=ds, spec=spec)},
    ...
)
```

**Target files**
- Update: `src/hamilton_pipeline/modules/cpg_build.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_props.py`

**Implementation checklist**
- [x] Allow pipeline nodes to pass `DatasetSource` where tables are currently forced.
- [x] Update `PlanCatalog` creation to handle dataset-backed sources consistently.
- [x] Keep kernel-lane operations as explicit materialization boundaries.

**Completed changes**
- `src/hamilton_pipeline/pipeline_types.py`: widened CPG input bundles to accept `DatasetSource`.
- `src/hamilton_pipeline/modules/cpg_build.py`: updated bundle constructors to accept sources.
- `src/cpg/build_edges.py`, `src/cpg/build_nodes.py`, `src/cpg/build_props.py`: verified existing
  `DatasetSource` support (no changes required).

**Status**
Completed.

---

### Scope 6: Inference-first schema alignment for row-built outputs
**Description**
Apply inference-first schema alignment to all row-built tables and fallback outputs.
This keeps schemas derived from evidence while still ensuring deterministic column
order and types.

**Code pattern**
```python
# src/normalize/schema_infer.py
schema = infer_schema_or_registry(name, tables)
return align_table_to_schema(out, schema)
```

**Target files**
- Update: `src/normalize/schema_infer.py`
- Update: `src/hamilton_pipeline/modules/normalization.py`
- Update: `src/hamilton_pipeline/modules/cpg_build.py`

**Implementation checklist**
- [x] Apply `infer_schema_or_registry(...)` wherever row dicts are converted to tables.
- [x] Ensure fallback outputs use inferred or contract schemas for alignment.
- [x] Preserve schema/contract versions unless explicitly revised.

**Completed changes**
- `src/normalize/schema_infer.py`: inference ignores empty-column evidence for registry fallback.
- `src/hamilton_pipeline/modules/normalization.py`: aligns row-built outputs to inferred schema.

**Status**
Completed.
