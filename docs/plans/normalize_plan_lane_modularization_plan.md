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

---

### Scope 1: Normalize QuerySpec registry (projection contracts)
**Description**
Define `QuerySpec` projections for each normalized dataset to standardize plan-lane projections and
derived columns (if any), mirroring `src/extract` structure.

**Code pattern**
```python
# src/normalize/query_specs.py
from arrowdsl.plan.query import ProjectionSpec, QuerySpec
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
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/bytecode_cfg.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [ ] Add `QuerySpec` constants per normalized dataset.
- [ ] Route plan-lane projections through `apply_query_spec(...)`.
- [ ] Keep any derived columns in `ProjectionSpec.derived` when applicable.

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

**Target files**
- New: `src/normalize/runner.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [ ] Add normalize runner with `PlanSpec` + `FinalizeResult`.
- [ ] Add minimal normalize contracts for determinism/dedupe where appropriate.
- [ ] Use runner to execute plan-lane flows; keep kernel-lane in `post`.

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
- [ ] Use plan-lane join when inputs originate from `Plan`.
- [ ] Preserve kernel-lane join fallback for eager tables.
- [ ] Mark ordering as unordered post-join.

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
- [ ] Replace kernel-lane hashing with `hash_expression` in plan-lane when possible.
- [ ] Use `encode_expression(...)` for dictionary encoding via projections.
- [ ] Keep kernel-lane helpers for non-plan inputs.

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
- [ ] Define contract specs for each normalized dataset.
- [ ] Use `FinalizeResult` from `run_normalize(...)`.
- [ ] Remove ad hoc canonical sorts in favor of finalize gates.

---

### Scope 6: Kernel-lane boundaries for spans + diagnostics
**Description**
Keep span conversion and diagnostics row derivation in kernel-lane, but wrap them as explicit post
steps in the normalize pipeline so plan-lane is the default elsewhere.

**Code pattern**
```python
# src/normalize/spans.py
def span_post_step(repo_index: RepoTextIndex) -> PostFn:
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
- [ ] Expose span/diagnostic kernel steps as `PostFn` callables.
- [ ] Ensure runner enforces these steps after plan-lane transforms.
- [ ] Keep schema alignment + dedupe in finalize, not inside kernel helpers.

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
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/bytecode_cfg.py`

**Implementation checklist**
- [ ] Add `run_normalize_reader(...)` for streaming outputs.
- [ ] Prefer readers for plan-only pipelines.
- [ ] Fallback to `to_table()` when pipeline breakers are present.

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
- [ ] Expose plan-lane entrypoints and `QuerySpec` constants.
- [ ] Remove direct table-only entrypoints where a plan is feasible.
- [ ] Keep compatibility wrappers that return tables for legacy call sites.

---

### Acceptance criteria
- All normalize datasets have QuerySpecs.
- All normalize pipelines go through a plan-lane runner + finalize boundary.
- Joins and hash IDs are plan-lane by default.
- Span/diagnostic conversions are explicit kernel post-steps.
- Streaming surfaces are used where no pipeline breakers exist.
