## Normalize Plan-Lane Modularization Plan (PyArrow/Acero/DSL)

### Goals
- Make `src/normalize` plan-lane first (Acero) with explicit kernel-lane boundaries.
- Centralize schema alignment, dedupe, and canonical ordering at a finalize boundary.
- Enable streaming (`to_reader`) where no pipeline breakers exist.
- Keep all existing semantics intact while increasing modularity and reuse.

### Constraints
- Preserve current normalized outputs and schema contracts.
- Use existing ArrowDSL primitives (`Plan`, `QuerySpec`, `FinalizeResult`, `JoinSpec`).
- Keep span/diagnostic text conversions in kernel-lane (non-streaming).
- Avoid introducing new non-Arrow dependencies.

### Status
- Completed (all scopes implemented; normalize pipelines now run through plan → post → finalize).

---

### Scope 1: Normalize QuerySpec registry (projection contracts)
**Description**
Define `QuerySpec` projections for each normalized dataset to standardize plan-lane projections and
derived columns (if any), mirroring `src/extract` structure.

**Code pattern**
```python
# src/normalize/query_specs.py
from arrowdsl.plan.query import QuerySpec
from normalize.schemas import (
    TYPE_EXPRS_NORM_SCHEMA,
    TYPE_NODES_SCHEMA,
    DEF_USE_SCHEMA,
    REACHES_SCHEMA,
    CFG_BLOCKS_NORM_SCHEMA,
    CFG_EDGES_NORM_SCHEMA,
    DIAG_SCHEMA,
)

TYPE_EXPRS_QUERY = QuerySpec.simple(*TYPE_EXPRS_NORM_SCHEMA.names)
TYPE_NODES_QUERY = QuerySpec.simple(*TYPE_NODES_SCHEMA.names)
DEF_USE_QUERY = QuerySpec.simple(*DEF_USE_SCHEMA.names)
REACHES_QUERY = QuerySpec.simple(*REACHES_SCHEMA.names)
CFG_BLOCKS_QUERY = QuerySpec.simple(*CFG_BLOCKS_NORM_SCHEMA.names)
CFG_EDGES_QUERY = QuerySpec.simple(*CFG_EDGES_NORM_SCHEMA.names)
DIAG_QUERY = QuerySpec.simple(*DIAG_SCHEMA.names)
```

**Target files**
- New: `src/normalize/query_specs.py`
- New: `src/normalize/plan_helpers.py` (`apply_query_spec`, `query_for_schema`)
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/bytecode_cfg.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Add `QuerySpec` constants per normalized dataset.
- [x] Add `apply_query_spec(...)` and `query_for_schema(...)` helpers mirroring extract.
- [x] Route plan-lane projections through `apply_query_spec(...)`.
- [x] Keep any derived columns in `ProjectionSpec.derived` when applicable.

---

### Scope 2: Normalize pipeline runner (plan → post → finalize)
**Description**
Introduce a small, typed runner that mirrors extract’s plan-lane structure and uses `FinalizeResult`
for schema alignment, dedupe, and canonical ordering.

**Code pattern**
```python
# src/normalize/runner.py
from collections.abc import Callable, Iterable
from arrowdsl.core.context import ExecutionContext
from arrowdsl.finalize.finalize import FinalizeResult, finalize
from arrowdsl.plan.plan import Plan, PlanSpec
from schema_spec.system import ContractSpec

PostFn = Callable[[TableLike, ExecutionContext], TableLike]

def run_normalize(
    *,
    plan: Plan,
    post: Iterable[PostFn],
    contract: ContractSpec,
    ctx: ExecutionContext,
) -> FinalizeResult:
    table = PlanSpec.from_plan(plan).to_table(ctx=ctx)
    for fn in post:
        table = fn(table, ctx)
    return finalize(table, contract=contract.to_contract(), ctx=ctx)
```

```python
# src/normalize/plan_helpers.py
from arrowdsl.plan.plan import Plan, PlanSpec

def materialize_plan(plan: Plan, *, ctx: ExecutionContext) -> TableLike:
    return PlanSpec.from_plan(plan).to_table(ctx=ctx)

def stream_plan(plan: Plan, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
    return PlanSpec.from_plan(plan).to_reader(ctx=ctx)

def finalize_plan(plan: Plan, *, ctx: ExecutionContext, prefer_reader: bool = False) -> TableLike | RecordBatchReaderLike:
    spec = PlanSpec.from_plan(plan)
    if prefer_reader and not spec.pipeline_breakers:
        return spec.to_reader(ctx=ctx)
    return spec.to_table(ctx=ctx)

def finalize_plan_bundle(
    plans: Mapping[str, Plan],
    *,
    ctx: ExecutionContext,
    prefer_reader: bool = False,
) -> dict[str, TableLike | RecordBatchReaderLike]:
    return {name: finalize_plan(plan, ctx=ctx, prefer_reader=prefer_reader) for name, plan in plans.items()}
```

**Target files**
- New: `src/normalize/runner.py`
- New: `src/normalize/plan_helpers.py` (extract-style helpers)
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Add normalize runner with `PlanSpec` + `FinalizeResult`.
- [x] Add minimal normalize contracts for determinism/dedupe where appropriate.
- [x] Use runner to execute plan-lane flows; keep kernel-lane in `post`.
- [x] Mirror extract helpers: `materialize_plan`, `stream_plan`, `finalize_plan`, `finalize_plan_bundle`.

---

### Scope 3: Plan-lane joins in normalize
**Description**
Replace table-joins with plan-lane joins where inputs are plan-backed, preserving ordering metadata.

**Code pattern**
```python
# src/normalize/join_helpers.py
from arrowdsl.plan.ops import JoinSpec
from arrowdsl.plan.plan import Plan

def join_plan(left: Plan, right: Plan, *, spec: JoinSpec, ctx: ExecutionContext) -> Plan:
    return left.join(right, spec=spec, ctx=ctx)
```

**Target files**
- New: `src/normalize/join_helpers.py`
- Update: `src/normalize/bytecode_cfg.py`
- Update: `src/normalize/bytecode_dfg.py`

**Implementation checklist**
- [x] Use plan-lane join when inputs originate from `Plan`.
- [x] Preserve kernel-lane join fallback for eager tables.
- [x] Mark ordering as unordered post-join.

---

### Scope 4: Plan-lane hash IDs + encoding
**Description**
Move hash ID generation and dictionary encoding into plan-lane projections where feasible.

**Code pattern**
```python
# src/normalize/types.py (plan-lane)
from arrowdsl.core.ids import HashSpec, hash_expression
from arrowdsl.schema.schema import encode_expression

hash_expr = hash_expression(HashSpec(prefix="type", cols=("type_repr",), as_string=True))
plan = plan.project([pc.field("type_repr"), hash_expr], ["type_repr", "type_id"], ctx=ctx)
```

**Target files**
- Update: `src/normalize/types.py`
- Update: `src/normalize/diagnostics.py`
- Update: `src/normalize/ids.py` (optional plan-lane helpers)

**Implementation checklist**
- [x] Replace kernel-lane hashing with `hash_expression` in plan-lane when possible.
- [x] Use `encode_expression(...)` for dictionary encoding via projections.
- [x] Add an `encoding_projection(...)` helper mirroring `src/extract/postprocess.py`.
- [x] Keep kernel-lane helpers for non-plan inputs.

---

### Scope 5: Determinism + ordering at finalize
**Description**
Define normalize contracts that specify dedupe keys, tie-breakers, and canonical ordering. Apply
determinism policies through finalize (not ad hoc in normalizers).

**Code pattern**
```python
# src/normalize/contracts.py
from schema_spec.system import make_contract_spec, SortKeySpec, DedupeSpecSpec
from normalize.schemas import TYPE_NODES_SPEC

TYPE_NODES_CONTRACT = make_contract_spec(
    table_spec=TYPE_NODES_SPEC.table_spec,
    dedupe=DedupeSpecSpec(
        keys=("type_id",),
        tie_breakers=(SortKeySpec(column="type_repr", order="ascending"),),
        strategy="KEEP_FIRST_AFTER_SORT",
    ),
    canonical_sort=(SortKeySpec(column="type_id", order="ascending"),),
)
```

**Target files**
- New: `src/normalize/contracts.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Define contract specs for each normalized dataset.
- [x] Use `FinalizeResult` from `run_normalize(...)`.
- [x] Remove ad hoc canonical sorts in favor of finalize gates.

---

### Scope 6: Kernel-lane boundaries for spans + diagnostics
**Description**
Keep span conversion and diagnostics row derivation in kernel-lane, but wrap them as explicit post
steps in the normalize pipeline so plan-lane is the default elsewhere.

**Code pattern**
```python
# src/normalize/spans.py
def ast_span_post_step(repo_index: RepoTextIndex) -> PostFn:
    def _apply(table: TableLike, ctx: ExecutionContext) -> TableLike:
        _ = ctx
        return add_ast_byte_spans(repo_index, table)
    return _apply
```

**Target files**
- Update: `src/normalize/spans.py`
- Update: `src/normalize/diagnostics.py`
- Update: `src/normalize/runner.py`

**Implementation checklist**
- [x] Expose span/diagnostic kernel steps as `PostFn` callables.
- [x] Ensure runner enforces these steps after plan-lane transforms.
- [x] Keep schema alignment + dedupe in finalize, not inside kernel helpers.

---

### Scope 7: Streaming surfaces for normalize pipelines
**Description**
Use `PlanSpec` to select `to_reader()` when no pipeline breakers exist, and make streaming
the default output surface where safe.

**Code pattern**
```python
# src/normalize/runner.py
def run_normalize_reader(plan: Plan, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
    return PlanSpec.from_plan(plan).to_reader(ctx=ctx)
```

**Target files**
- Update: `src/normalize/runner.py`
- Update: `src/normalize/plan_helpers.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/bytecode_cfg.py`

**Implementation checklist**
- [x] Add `run_normalize_reader(...)` for streaming outputs.
- [x] Prefer readers for plan-only pipelines.
- [x] Fallback to `to_table()` when pipeline breakers are present.
- [x] Add extract-style `finalize_plan(prefer_reader=True)` gating with `PlanSpec`.

---

### Scope 8: Normalize module API reshaping
**Description**
Mirror extract’s structure (plan-lane + kernel-lane) with a small set of public pipeline entrypoints.

**Code pattern**
```python
# src/normalize/__init__.py
from normalize.runner import run_normalize, run_normalize_reader
from normalize.query_specs import TYPE_EXPRS_QUERY, TYPE_NODES_QUERY
```

**Target files**
- Update: `src/normalize/__init__.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Expose plan-lane entrypoints and `QuerySpec` constants.
- [x] Remove direct table-only entrypoints where a plan is feasible.
- [x] Keep compatibility wrappers that return tables for legacy call sites.

---

### Scope 9: Plan shaping helpers (append/rename projections)
**Description**
Adopt extract’s plan-shaping helpers to keep plan-lane transformations readable and standardized.

**Code pattern**
```python
# src/normalize/plan_helpers.py
from collections.abc import Mapping, Sequence
from arrowdsl.core.interop import ComputeExpression, pc
from arrowdsl.plan.plan import Plan

def append_projection(
    plan: Plan,
    *,
    base: Sequence[str],
    extras: Sequence[tuple[ComputeExpression, str]],
    ctx: ExecutionContext | None = None,
) -> Plan:
    expressions = [pc.field(name) for name in base]
    names = list(base)
    for expr, name in extras:
        expressions.append(expr)
        names.append(name)
    return plan.project(expressions, names, ctx=ctx)

def rename_plan_columns(
    plan: Plan,
    *,
    columns: Sequence[str],
    rename: Mapping[str, str],
    ctx: ExecutionContext | None = None,
) -> Plan:
    names = [rename.get(name, name) for name in columns]
    expressions = [pc.field(name) for name in columns]
    return plan.project(expressions, names, ctx=ctx)
```

**Target files**
- New: `src/normalize/plan_helpers.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Add plan shaping helpers mirroring `src/extract/tables.py`.
- [x] Use helpers for derived columns and renames instead of ad hoc projections.
- [x] Keep helpers small and only for plan-lane usage.

---

### Acceptance criteria
- All normalize datasets have QuerySpecs.
- All normalize pipelines go through a plan-lane runner + finalize boundary.
- Joins and hash IDs are plan-lane by default.
- Span/diagnostic conversions are explicit kernel post-steps.
- Streaming surfaces are used where no pipeline breakers exist.
- Normalize plan helpers (materialize/stream/finalize) mirror extract usage.
