## Normalize Arrow Intensification Plan (PyArrow/Acero/DSL)

### Goals
- Increase plan-lane usage in `src/normalize` and reduce Python-side work.
- Centralize shared plan-lane expressions and ID/hash policies.
- Align normalize with Arrow DSL determinism, ordering, and observability guidance.
- Improve maintainability by consolidating contracts/query specs and minimizing duplicated patterns.
- Leverage Arrow schema/field metadata and nested type controls for richer contracts and tooling.

### Constraints
- Preserve current normalized outputs and dataset contracts.
- Keep span/diagnostic text conversions in kernel lane.
- Avoid introducing new non-Arrow dependencies.
- Maintain strict typing and Ruff compliance (no suppressions).

---

### Scope 1: Plan-lane expression helpers (dedupe repeated patterns)
**Description**
Create a shared module for common plan-lane expressions (missing-column handling, string trimming,
coalesce, def/use kind classification) to remove repeated logic in normalize modules.

**Code pattern**
```python
# src/normalize/plan_exprs.py
import pyarrow as pa
from arrowdsl.core.interop import ComputeExpression, ensure_expression, pc


def column_or_null_expr(name: str, dtype: pa.DataType, *, available: set[str]) -> ComputeExpression:
    if name in available:
        return pc.field(name)
    return pc.scalar(pa.scalar(None, type=dtype))


def trimmed_non_empty_expr(col: str) -> tuple[ComputeExpression, ComputeExpression]:
    trimmed = ensure_expression(pc.call_function("utf8_trim_whitespace", [pc.field(col)]))
    non_empty = ensure_expression(
        pc.and_(
            pc.is_valid(trimmed),
            pc.greater(ensure_expression(pc.call_function("utf8_length", [trimmed])), pc.scalar(0)),
        )
    )
    return trimmed, non_empty


def coalesce_string_expr(cols: list[str]) -> ComputeExpression:
    exprs = [pc.field(col) for col in cols]
    return ensure_expression(pc.coalesce(*exprs))
```

**Target files**
- New: `src/normalize/plan_exprs.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/bytecode_cfg.py`

**Implementation checklist**
- [x] Add shared expression helpers (`column_or_null_expr`, `trimmed_non_empty_expr`, etc.).
- [x] Replace local `_column_expr` and duplicated trimming/coalesce logic.
- [x] Keep all plan-lane predicates wrapped via `ensure_expression`.

---

### Scope 2: Consolidate QuerySpec + ContractSpec into DatasetSpec
**Description**
Move dataset contracts and query specs into `normalize.schemas` so each dataset spec is fully
self-describing (schema + contract + query). This reduces indirection and keeps policy centralized.

**Code pattern**
```python
# src/normalize/schemas.py
from schema_spec.system import make_dataset_spec
from normalize.query_specs import TYPE_EXPRS_QUERY
from normalize.contracts import TYPE_EXPRS_CONTRACT

TYPE_EXPRS_NORM_SPEC = GLOBAL_SCHEMA_REGISTRY.register_dataset(
    make_dataset_spec(
        table_spec=...,
        query_spec=TYPE_EXPRS_QUERY,
        contract_spec=TYPE_EXPRS_CONTRACT,
    )
)
```

**Target files**
- Update: `src/normalize/schemas.py`
- Update: `src/normalize/query_specs.py` (re-export or slimmed)
- Update: `src/normalize/contracts.py` (re-export or slimmed)
- Update: `src/normalize/types.py`, `src/normalize/bytecode_dfg.py`, `src/normalize/bytecode_cfg.py`, `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Attach `query_spec` and `contract_spec` in each dataset registration.
- [x] Update normalize pipelines to fetch specs from dataset specs where possible.
- [x] Avoid duplicate or divergent source-of-truth specs.

---

### Scope 3: Determinism tier handling in normalize entrypoints
**Description**
Make determinism explicit at normalize API entrypoints and provide canonical wrappers to guarantee
stable ordering and dedupe semantics when needed.

**Code pattern**
```python
# src/normalize/runner.py
from arrowdsl.core.context import DeterminismTier


def ensure_canonical(ctx: ExecutionContext) -> ExecutionContext:
    runtime = ctx.runtime.with_determinism(DeterminismTier.CANONICAL)
    return ExecutionContext(
        runtime=runtime,
        mode=ctx.mode,
        provenance=ctx.provenance,
        safe_cast=ctx.safe_cast,
    )
```

**Target files**
- Update: `src/normalize/runner.py`
- Update: `src/normalize/__init__.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/bytecode_cfg.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Add canonical-context helper (`ensure_canonical`).
- [x] Provide `*_canonical(...)` entrypoints or require explicit `ctx` at call sites.
- [x] Keep default behavior backward compatible where needed.

---

### Scope 4: HashSpec registry for normalized IDs
**Description**
Centralize hash ID definitions so IDs are consistent and reusable across plan-lane and kernel-lane
usage.

**Code pattern**
```python
# src/normalize/hash_specs.py
from arrowdsl.core.ids import HashSpec

TYPE_EXPR_ID_SPEC = HashSpec(prefix="cst_type_expr", cols=("path", "bstart", "bend"), as_string=True)
TYPE_ID_SPEC = HashSpec(prefix="type", cols=("type_repr",), as_string=True)
DEF_USE_EVENT_ID_SPEC = HashSpec(prefix="df_event", cols=("code_unit_id", "instr_id", "kind", "symbol"))
REACH_EDGE_ID_SPEC = HashSpec(prefix="df_reach", cols=("def_event_id", "use_event_id"))
```

**Target files**
- New: `src/normalize/hash_specs.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Add normalized hash spec registry.
- [x] Use `hash_expression(spec)` + `masked_hash_expression(...)` where needed.
- [x] Remove duplicate `HashSpec(...)` literals in normalize modules.

---

### Scope 5: Move derived columns into QuerySpec.derived
**Description**
Shift derived columns (hash IDs, coalesced symbols, computed fields) into `QuerySpec.derived` so
projections are declarative and centralized.

**Code pattern**
```python
# src/normalize/query_specs.py
from arrowdsl.compute.expr import E
from arrowdsl.plan.query import ProjectionSpec, QuerySpec

TYPE_EXPRS_QUERY = QuerySpec(
    projection=ProjectionSpec(
        base=("file_id", "path", "bstart", "bend", "expr_text", ...),
        derived={
            "type_repr": E.call("utf8_trim_whitespace", E.field("expr_text")),
            "type_id": E.hash(...),
        },
    )
)
```

**Target files**
- Update: `src/normalize/query_specs.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Define `ProjectionSpec.derived` entries for computed columns.
- [x] Simplify plan-lane steps to `apply_query_spec(...)` wherever possible.
- [x] Keep kernel-lane conversions outside derived projections.

---

### Scope 6: Expose FinalizeResult artifacts for normalize pipelines
**Description**
Expose finalize artifacts (good/errors/stats/alignment) for normalize pipelines to align with
observability guidance and enable CI regression checks.

**Code pattern**
```python
# src/normalize/types.py
from arrowdsl.finalize.finalize import FinalizeResult


def normalize_type_exprs_result(..., ctx: ExecutionContext) -> FinalizeResult:
    plan = type_exprs_plan(...)
    return run_normalize(plan=plan, post=(), contract=TYPE_EXPRS_CONTRACT, ctx=ctx)
```

**Target files**
- Update: `src/normalize/runner.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/bytecode_cfg.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Add `*_result(...)` entrypoints returning `FinalizeResult`.
- [x] Preserve existing table-returning wrappers for compatibility.
- [x] Ensure schema alignment + dedupe always occur in finalize.

---

### Scope 7: Plan-lane union + encoding for diagnostics
**Description**
Keep diagnostics composition in plan-lane by unioning plan-backed fragments, then applying encoding
projection before finalize.

**Code pattern**
```python
# src/normalize/diagnostics.py
from arrowdsl.plan.plan import Plan, union_all_plans

plans = [Plan.table_source(part) for part in parts]
plan = union_all_plans(plans)
exprs, names = encoding_projection(encode_cols, available=plan.schema(ctx=ctx).names)
plan = plan.project(exprs, names, ctx=ctx)
result = run_normalize(plan=plan, post=(), contract=DIAG_CONTRACT, ctx=ctx)
```

**Target files**
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Wrap diagnostic parts in `Plan.table_source(...)`.
- [x] Use `union_all_plans(...)` to assemble plan-lane output.
- [x] Apply encoding via projection before finalize.

---

### Scope 8: Streaming-safe normalize outputs
**Description**
Provide reader outputs only when finalize is not required (no dedupe/sort) and no pipeline breakers
exist.

**Code pattern**
```python
# src/normalize/runner.py
from arrowdsl.plan.plan import PlanSpec


def run_normalize_streamable(plan: Plan, *, ctx: ExecutionContext) -> RecordBatchReaderLike | TableLike:
    spec = PlanSpec.from_plan(plan)
    if not spec.pipeline_breakers:
        return spec.to_reader(ctx=ctx)
    return spec.to_table(ctx=ctx)
```

**Target files**
- Update: `src/normalize/runner.py`
- Update: `src/normalize/__init__.py`

**Implementation checklist**
- [x] Add `run_normalize_streamable(...)` helper.
- [ ] Document that finalize contracts are skipped when streaming.
- [ ] Use `finalize_plan(prefer_reader=True)` where applicable.

---

### Scope 9: Plan-lane compute for diagnostics (reduce Python overhead)
**Description**
Convert Tree-sitter diagnostics and other pure column transforms to plan-lane compute projections
where feasible, reducing Python list materialization.

**Code pattern**
```python
# src/normalize/diagnostics.py
plan = Plan.table_source(ts_table)
exprs = [pc.field("file_id"), pc.field("path"), pc.field("start_byte"), pc.field("end_byte")]
plan = plan.project(exprs, ["file_id", "path", "bstart", "bend"], ctx=ctx)
```

**Target files**
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Identify per-row diagnostics conversions that can become projections.
- [x] Keep kernel-lane conversions where text index lookup is required.
- [x] Preserve diagnostic detail struct construction semantics.

---

### Scope 10: Schema + field metadata control plane
**Description**
Attach schema-level and field-level metadata to normalize outputs to capture contract version,
normalization stage, determinism tier, and encoding expectations.

**Code pattern**
```python
# src/normalize/contracts.py
from arrowdsl.schema.schema import SchemaMetadataSpec

SCHEMA_META = {
    b"contract_name": b"type_exprs_norm_v1",
    b"normalize_stage": b"normalize",
}

meta_schema = SchemaMetadataSpec(schema_metadata=SCHEMA_META).apply(TYPE_EXPRS_NORM_SCHEMA)
```

**Target files**
- Update: `src/normalize/contracts.py`
- Update: `src/normalize/schemas.py`
- Update: `src/normalize/runner.py`

**Implementation checklist**
- [x] Define schema metadata for normalize datasets.
- [x] Optionally add field metadata for encoding/semantic labels.
- [x] Ensure metadata is preserved through finalize.

---

### Scope 11: Schema evolution alignment + metadata-aware unify
**Description**
Extend schema alignment helpers to unify and cast tables while respecting schema/field metadata
semantics described in PyArrow advanced docs.

**Code pattern**
```python
# src/normalize/schema_infer.py
unified = pa.unify_schemas(schemas, promote_options="permissive")
aligned = [t.cast(unified) for t in tables]
```

**Target files**
- Update: `src/normalize/schema_infer.py`
- Update: `src/normalize/diagnostics.py`
- Update: `src/normalize/plan_helpers.py`

**Implementation checklist**
- [x] Add a metadata-aware unify helper (preserve first-schema metadata).
- [x] Use this helper where tables are merged; diagnostics now uses base-schema plan unions.
- [ ] Document metadata inheritance behavior.

---

### Scope 12: Nested type tuning for diagnostics detail arrays
**Description**
Evaluate and optionally adopt `large_list` or `list_view` for diagnostic details/tags to avoid
32-bit offset overflow and reduce unnecessary copying for large diagnostic datasets.

**Code pattern**
```python
# src/normalize/schemas.py
DIAG_DETAIL_STRUCT = pa.struct([...])
DIAG_DETAILS_TYPE = pa.large_list(DIAG_DETAIL_STRUCT)
```

**Target files**
- Update: `src/normalize/schemas.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Assess expected list sizes for diagnostics.
- [x] Switch to `large_list` when size risk is high.
- [x] Keep schema alignment and encoding consistent.

---

### Scope 13: Field flattening helpers for nested outputs
**Description**
Add helpers to flatten struct fields for projection and join planning, reducing manual child-column
handling in future normalize expansions.

**Code pattern**
```python
# src/normalize/plan_helpers.py
import pyarrow as pa


def flatten_struct_field(field: pa.Field) -> list[pa.Field]:
    return field.flatten()
```

**Target files**
- Update: `src/normalize/plan_helpers.py`
- Update: `src/normalize/schema_infer.py`

**Implementation checklist**
- [x] Add flattening helper for struct fields.
- [x] Use in schema evolution or projection utilities where nested fields appear.

---

### Scope 14: Encoding policy via field metadata
**Description**
Drive dictionary encoding projections from field metadata (e.g., `encoding=dictionary`) rather than
hard-coded column lists.

**Code pattern**
```python
# src/normalize/plan_helpers.py

def encoding_columns_from_metadata(schema: SchemaLike) -> list[str]:
    return [field.name for field in schema if field.metadata and field.metadata.get(b"encoding") == b"dictionary"]
```

**Target files**
- Update: `src/normalize/plan_helpers.py`
- Update: `src/normalize/schemas.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Annotate fields that should be dictionary-encoded.
- [x] Compute encoding projection from schema metadata.
- [x] Remove hard-coded encoding column lists where possible.

---

### Scope 15: Metadata consistency tests
**Description**
Add lightweight tests to confirm normalize outputs include expected schema metadata and encoding
annotations.

**Code pattern**
```python
# tests/normalize/test_schema_metadata.py
assert output.schema.metadata[b"contract_name"] == b"type_exprs_norm_v1"
```

**Target files**
- New: `tests/normalize/test_schema_metadata.py`
- Update: existing normalize tests as needed

**Implementation checklist**
- [x] Add tests for schema-level metadata on normalize outputs.
- [x] Add tests for field-level encoding metadata.

---

### Acceptance criteria
- Shared plan expression helpers replace duplicated plan-lane logic.
- Dataset specs in `src/normalize/schemas.py` fully define `schema + query + contract`.
- Normalize entrypoints support explicit determinism tiers.
- Hash specs are centralized and reused across normalize modules.
- Derived columns live in `QuerySpec.derived` wherever applicable.
- `FinalizeResult` artifacts are accessible for normalize pipelines.
- Diagnostics are composed via plan-lane union + encoding projection.
- Streaming helpers only return readers when safe to do so.
- Diagnostics plan-lane compute paths reduce Python-side materialization.
- Schema/field metadata is attached and preserved through finalize.
- Metadata-aware schema unify is used where tables are merged.
- Nested diagnostics types use `large_list`/`list_view` when needed.
- Encoding policy is driven from field metadata.
- Metadata expectations are covered by tests.
