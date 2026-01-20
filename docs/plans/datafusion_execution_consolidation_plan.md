# DataFusion Built-ins + UDF Consolidation Plan

## Purpose
Consolidate all execution on DataFusion built-ins and DataFusion UDFs, keeping
PyArrow compute as an internal implementation detail only. Remove ArrowDSL
compute fallback lanes and custom Ibis UDFs that duplicate DataFusion.

## Objectives
- Make DataFusion the single execution substrate (SQL + DataFrame APIs).
- Centralize UDF registration in `datafusion_engine` and surface it to Ibis.
- Replace ArrowDSL compute expressions/macros with DataFusion built-ins/UDFs.
- Remove ArrowDSL compute fallback modules after call sites are migrated.
- Adopt streaming-first execution surfaces for large outputs.
- Persist DataFusion function catalogs and plan artifacts for coverage.

## Non-goals
- No attempt to remove PyArrow compute usage inside UDF bodies.
- No change to SQLGlot or Ibis lineage tooling (planning only).
- No incremental compatibility shim for ArrowDSL compute lanes.

## Scope items

### Scope 1: DataFusion function catalog + coverage baseline
Goal: produce a runtime-sourced list of available built-ins and use it as the
source of truth for function coverage.

Representative code patterns:
```python
from datafusion import SessionContext
import pyarrow as pa
import pyarrow.parquet as pq

ctx = SessionContext()
table = pa.Table.from_batches(ctx.sql("SHOW FUNCTIONS").collect())
pq.write_table(table, "docs/plans/datafusion_function_catalog.parquet")
```

Target files:
- `src/datafusion_engine/runtime.py`
- `src/relspec/rules/coverage.py`
- `docs/plans/datafusion_function_catalog.parquet`

Implementation checklist:
- [ ] Add a helper to export `SHOW FUNCTIONS` into an Arrow dataset artifact.
- [ ] Use the catalog to validate which ExprIR ops map to built-ins vs UDFs.
- [ ] Record catalog version in plan artifacts (for regressions).

---

### Scope 2: Consolidated DataFusion UDF registry
Goal: register all custom functions via `datafusion_engine/udf_registry.py`
and treat it as the sole UDF source.

Representative code patterns:
```python
from datafusion import udf
import pyarrow as pa

@udf([pa.string()], pa.int32(), "stable", "position_encoding_norm")
def position_encoding_norm_udf(values: pa.Array) -> pa.Array:
    return values.cast(pa.int32())
```

Target files:
- `src/datafusion_engine/udf_registry.py`
- `src/datafusion_engine/function_factory.py`
- `src/datafusion_engine/registry_loader.py`

Implementation checklist:
- [ ] Define all required UDFs here (stable_hash64/128, position_encoding_norm, etc).
- [ ] Ensure all UDFs are registered into the shared SessionContext.
- [ ] Remove UDF registration from any non-DataFusion module.

---

### Scope 3: Ibis -> DataFusion UDF bridge
Goal: make Ibis use DataFusion UDF names (no custom Ibis UDF logic).

Representative code patterns:
```python
import ibis.expr.datatypes as dt
import ibis.udf

@ibis.udf.scalar.builtin(signature=((dt.string,), dt.int32), name="position_encoding_norm")
def position_encoding_norm(value: dt.String) -> dt.Int32:
    return value
```

Target files:
- `src/ibis_engine/builtin_udfs.py`
- `src/ibis_engine/expr_compiler.py`

Implementation checklist:
- [ ] Replace Ibis UDFs that duplicate DataFusion with thin builtin wrappers.
- [ ] Ensure wrapper names match DataFusion UDF names exactly.
- [ ] Remove any Python-side UDF implementation logic from Ibis.

---

### Scope 4: ExprIR mapping to DataFusion built-ins
Goal: map ExprIR ops to built-in DataFusion SQL/Ibis expressions wherever
available, and only fall back to UDF calls when required.

Representative code patterns:
```python
def _concat_expr(*values: Value) -> Value:
    # Built-in concat for strings/arrays (no UDF needed).
    result = values[0]
    for part in values[1:]:
        result = result.concat(part)
    return result
```

Target files:
- `src/ibis_engine/expr_compiler.py`
- `src/arrowdsl/spec/expr_ir.py`
- `src/relspec/compiler.py`

Implementation checklist:
- [ ] Update `default_expr_registry` to prefer DataFusion built-ins.
- [ ] Track remaining ops requiring DataFusion UDFs (not Ibis UDFs).
- [ ] Validate ExprIR compilation against `SHOW FUNCTIONS`.

---

### Scope 5: Migrate call sites off ArrowDSL compute helpers
Goal: replace direct usage of ArrowDSL compute utilities with DataFusion-native
expressions or UDFs.

Representative code patterns:
```python
def apply_position_encoding(table: Table, column: str) -> Table:
    return table.mutate(
        position_encoding_norm=position_encoding_norm(table[column].cast("string"))
    )
```

Target files:
- `src/normalize/spans.py`
- `src/normalize/ibis_spans.py`
- `src/relspec/cpg/emit_props_ibis.py`
- `src/hamilton_pipeline/modules/normalization.py`

Implementation checklist:
- [ ] Replace `arrowdsl.compute.filters.position_encoding_array`.
- [ ] Remove `arrowdsl.compute.udf_helpers` usage in macros/filters.
- [ ] Replace `arrowdsl.compute.kernels` usage where DataFusion built-ins exist.

---

### Scope 6: Decommission ArrowDSL compute modules
Goal: remove compute fallback modules and registry once all call sites are
migrated.

Representative code patterns:
```python
# Delete the following modules after migration:
# - src/arrowdsl/compute/macros.py
# - src/arrowdsl/compute/filters.py
# - src/arrowdsl/compute/expr_core.py
# - src/arrowdsl/compute/predicates.py
# - src/arrowdsl/compute/registry.py
```

Target files:
- `src/arrowdsl/compute/*`
- `src/arrowdsl/ops/*`
- `src/arrowdsl/kernel/registry.py`
- `src/relspec/rules/validation.py`
- `src/relspec/rules/diagnostics.py`

Implementation checklist:
- [ ] Remove ArrowDSL compute and ops packages after all call sites migrate.
- [ ] Update kernel-lane policies to DataFusion-only.
- [ ] Remove ArrowDSL fallback diagnostics from rule validation.

---

### Scope 7: Streaming-first execution surfaces
Goal: standardize on streaming outputs for large queries.

Representative code patterns:
```python
def stream_batches(df: DataFrame) -> Iterator[pa.RecordBatch]:
    for batch in df.execute_stream():
        yield batch.to_pyarrow()
```

Target files:
- `src/datafusion_engine/runtime.py`
- `src/ibis_engine/execution.py`
- `src/obs/outputs.py`

Implementation checklist:
- [ ] Prefer `execute_stream()` or `__arrow_c_stream__` for large results.
- [ ] Restrict `collect()` to diagnostics and small datasets.
- [ ] Document streaming defaults in runtime profiles.

---

### Scope 8: Plan artifacts + function coverage reporting
Goal: persist logical/optimized/physical plans and coverage artifacts on each
rulepack build to validate that no fallback logic is used.

Representative code patterns:
```python
def persist_plan_artifacts(df: DataFrame, base: Path) -> None:
    base.write_text(df.optimized_logical_plan().display())
```

Target files:
- `src/obs/repro.py`
- `src/obs/outputs.py`

Implementation checklist:
- [ ] Persist optimized logical + physical plans for each rulepack.
- [ ] Emit a coverage report based on DataFusion catalog + ExprIR ops.
- [ ] Mark any UDF usage explicitly in coverage output.

---

## Decommission candidates (post-migration)
- `src/arrowdsl/ops/`
- `src/arrowdsl/compute/udf_helpers.py`
- `src/arrowdsl/compute/filters.py`
- `src/arrowdsl/compute/macros.py`
- `src/arrowdsl/compute/expr_core.py`
- `src/arrowdsl/compute/expr_ops.py`
- `src/arrowdsl/compute/predicates.py`
- `src/arrowdsl/compute/registry.py`
- `src/arrowdsl/compute/kernels.py`
- `src/arrowdsl/compute/kernel_utils.py`

## Validation gates
- DataFusion catalog exported and versioned.
- All ExprIR ops compile to DataFusion built-ins or registered UDFs.
- No kernel-lane or ArrowDSL fallback usage in diagnostics/coverage.
- Streaming outputs used for large products (no accidental `collect()`).
