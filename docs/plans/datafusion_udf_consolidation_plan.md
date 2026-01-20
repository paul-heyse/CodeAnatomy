# DataFusion UDF Consolidation Plan (PyArrow Compute Migration)

## Purpose
Consolidate expression-level computation behind DataFusion UDFs and built-ins so
the rest of the repo interacts with DataFusion extensions instead of `pyarrow.compute`.

## Objectives
- Inventory and convert all expression-level `pc.*` usage into DataFusion UDFs or built-ins.
- Centralize UDF registration in `datafusion_engine/udf_registry.py` and access via
  `ibis_engine/builtin_udfs.py` or DataFusion SQL.
- Keep PyArrow compute utilities internal to `datafusion_engine` only when they are
  truly non-UDF (schema/metadata/table construction).
- Eliminate `pc.*` usage from normalization, incremental, and finalize pipelines.

## Non-goals
- Replace schema alignment/validation utilities with UDFs.
- Convert table-construction helpers into UDFs.
- Remove PyArrow from the runtime (it remains a dependency for DataFusion and schemas).

## Progress update (current)
Status: in progress.

Completed so far:
- Centralized `pyarrow.compute` access in `src/datafusion_engine/compute_ops.py` and removed
  `src/arrowdsl/core/pyarrow_ops.py`.
- Added stable hash utilities in `src/datafusion_engine/hash_utils.py`.
- Routed hash/ID helpers in `src/arrowdsl/core/ids.py` through DataFusion UDF-backed
  hash functions (string IDs now use stable 128-bit hashes).
- Added `scripts/check_pyarrow_compute_usage.py` and documented DataFusion-first compute
  usage in `docs/python_library_reference/datafusion_builtin_and_udf.md`.

## Inventory (UDF candidates only)
These are the expression-level `pc.*` call sites targeted for UDF conversion:
- Hash/ID: `src/arrowdsl/core/ids.py`
- Validity masks: `src/arrowdsl/core/validity.py`
- Column transforms: `src/datafusion_engine/compute_ops.py` (centralized, still pending UDFs)
- Normalize pipeline: `src/hamilton_pipeline/modules/normalization.py`
- Incremental pipelines: `src/incremental/diff.py`, `src/incremental/exports.py`,
  `src/incremental/scip_snapshot.py`
- Finalize pipeline: `src/arrowdsl/finalize/finalize.py`

## Scope items

### Scope 1: UDF registry expansion (hash/ID and validity primitives)
Goal: add UDFs for hash/ID generation and validity masks, and expose them through
Ibis builtins and DataFusion SQL.

Representative code patterns:
```python
def _hash64(value: pa.Array | pa.ChunkedArray | pa.Scalar) -> pa.Array | pa.Scalar:
    # deterministic hash implementation
    ...

_HASH64_UDF = udf(_hash64, [pa.string()], pa.int64(), "stable", "hash64")

_SCALAR_UDF_SPECS: tuple[tuple[DataFusionUdfSpec, ScalarUDF], ...] = (
    (
        DataFusionUdfSpec(
            func_id="hash64",
            engine_name="hash64",
            kind="scalar",
            input_types=(pa.string(),),
            return_type=pa.int64(),
            arg_names=("value",),
            rewrite_tags=("hash",),
        ),
        _HASH64_UDF,
    ),
)
```

```python
@ibis.udf.scalar.builtin(signature=((dt.string,), dt.int64), name="hash64")
def hash64(value: Value) -> Value:
    ...
```

Target files:
- `src/datafusion_engine/udf_registry.py`
- `src/datafusion_engine/function_factory.py`
- `src/ibis_engine/builtin_udfs.py`
- `src/arrowdsl/core/ids.py`
- `src/arrowdsl/core/validity.py`

Implementation checklist:
- [ ] Add UDFs for `hash64`, `prefixed_hash64`, and `stable_id` variants.
- [ ] Add UDF for multi-column validity mask (or DataFusion expression helper).
- [ ] Expose new UDF names in `ibis_engine/builtin_udfs.py`.
- [x] Replace `pc.*` usage in `arrowdsl.core.ids` with DataFusion UDF-backed hash calls.
- [ ] Replace `pc.*` usage in `arrowdsl.core.validity` with DataFusion expressions/UDF calls.

Status (current): in progress.

### Scope 2: PyArrow ops -> DataFusion UDFs (distinct + list/struct flatten)
Goal: replace `arrowdsl.core.pyarrow_ops` with UDF-backed operations.

Representative code patterns:
```python
def _distinct_sorted(values: pa.Array | pa.ChunkedArray) -> pa.Array:
    # implementation preserves null placement
    ...

_DISTINCT_SORTED_UDF = udf(_distinct_sorted, [pa.string()], pa.string(), "stable", "distinct_sorted")
```

```python
@ibis.udf.scalar.builtin(signature=((dt.list(dt.struct)),), dt.string, name="flatten_list_struct_field")
def flatten_list_struct_field(value: Value) -> Value:
    ...
```

Target files:
- `src/datafusion_engine/udf_registry.py`
- `src/datafusion_engine/compute_ops.py`
- `src/hamilton_pipeline/modules/normalization.py`

Implementation checklist:
- [ ] Implement `distinct_sorted` as a UDF (or map to DataFusion built-ins if available).
- [ ] Implement `flatten_list_struct_field` as a UDF.
- [ ] Replace call sites to use UDFs instead of `pc.*`.
- [x] Remove `arrowdsl.core.pyarrow_ops` and centralize helpers in
      `datafusion_engine/compute_ops.py`.

Status (current): in progress.

### Scope 3: Normalize pipeline migration (DataFusion expressions)
Goal: eliminate `pc.*` usage from normalization and make all compute operations
expressible as DataFusion UDFs or built-ins.

Representative code patterns:
```python
expr = ibis.literal("").cast("string")
expr = ibis.case().when(expr == "", None).else_(expr).end()
expr = expr.coalesce(ibis.literal("fallback"))
```

```python
expr = ibis.literal(col).call("hash64")  # DataFusion UDF call
```

Target files:
- `src/hamilton_pipeline/modules/normalization.py`
- `src/normalize/spans.py`
- `src/normalize/text_index.py`

Implementation checklist:
- [ ] Replace `pc.cast`, `pc.coalesce`, `pc.if_else`, `pc.struct_field`, `pc.is_valid`
      with Ibis/DataFusion expressions.
- [ ] Use UDFs for list/struct transforms where DataFusion lacks built-ins.
- [ ] Ensure UDFs are registered in the DataFusion runtime profile for normalization.

Status (current): not started.

### Scope 4: Incremental pipeline migration (diff/exports/scip)
Goal: convert incremental path `pc.*` usage into DataFusion expressions/UDFs.

Representative code patterns:
```python
expr = ibis.case().when(lhs.isnull(), rhs).else_(lhs).end()
expr = expr.isin(value_list)
```

Target files:
- `src/incremental/diff.py`
- `src/incremental/exports.py`
- `src/incremental/scip_snapshot.py`
- `src/incremental/props_update.py`

Implementation checklist:
- [ ] Replace `pc.case_when`, `pc.is_in`, `pc.is_null`, `pc.is_valid`, `pc.not_equal`
      with Ibis/DataFusion expressions.
- [ ] Convert list/struct operations to UDFs if DataFusion lacks built-ins.
- [ ] Update any unit tests to validate DataFusion execution paths.

Status (current): not started.

### Scope 5: Finalize pipeline migration (dedupe + stats)
Goal: remove `pc.*` usage from the finalize gate and replace with DataFusion UDFs
or built-in aggregates.

Representative code patterns:
```python
# DataFusion aggregate via Ibis
counts = table.group_by(keys).aggregate(count=table[keys[0]].count())
```

Target files:
- `src/arrowdsl/finalize/finalize.py`
- `src/datafusion_engine/kernels.py`

Implementation checklist:
- [ ] Replace `pc.value_counts`, `pc.list_flatten`, `pc.list_value_length` with
      DataFusion built-ins or UDFs.
- [ ] Ensure deterministic behavior for canonical sort and dedupe paths.
- [ ] Update kernel registry to treat finalize transforms as DataFusion-native.

Status (current): not started.

### Scope 6: Enforcement and cleanup
Goal: prevent new `pc.*` usage outside DataFusion and keep PyArrow compute internal.

Representative code patterns:
```python
# DataFusion-only expression entrypoint
def expr_hash64(value: Value) -> Value:
    return value.call("hash64")
```

Target files:
- `src/datafusion_engine/udf_registry.py`
- `src/datafusion_engine/function_factory.py`
- `src/engine/pyarrow_registry.py`
- `docs/python_library_reference/datafusion_builtin_and_udf.md`

Implementation checklist:
- [ ] Consolidate any remaining `pc.*` helpers into `datafusion_engine` only.
- [x] Update docs to reflect DataFusion-first UDF usage patterns.
- [x] Add a lightweight repo check (script or doc note) to flag new `pc.*` usage.

Status (current): in progress.

## Migration order
1) Hash/ID UDFs (highest reuse and lowest risk).
2) Normalization pipeline expressions.
3) Incremental pipelines.
4) Finalize pipeline.
5) Cleanup and enforcement.
