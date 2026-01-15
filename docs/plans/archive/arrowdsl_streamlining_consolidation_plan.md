# ArrowDSL Streamlining and Consolidation Plan

## Context
This plan consolidates ArrowDSL internals while **preserving external interfaces**
(`Plan`, `QuerySpec`, `ExprIR`, `run_plan`, `plan_helpers`). It is aligned with:

- `docs/plans/Target_Architecture.md`
- `docs/plans/PR1through4_integrated_implementation_plan.md`
- `docs/plans/PR5through6.md`
- `docs/plans/PR5through6_remaining_implementation_plan.md`

**Goal:** maximize consolidation inside `src/arrowdsl` without changing downstream
call sites in `src/cpg`, `src/normalize`, `src/extract`, etc.

## Principles
- **One internal execution path**: PlanIR -> SegmentPlan -> runtime execution.
- **No compatibility layers**: no adapter modules, no dual runner branches.
- **External API stability**: keep signatures/types of `Plan`, `QuerySpec`,
  `ExprIR`, `run_plan`, and `plan_helpers` unchanged.
- **Single source of truth** for ordering, encoding, join output resolution,
  and op capability metadata.
- **ArrowDSL stays fallback-lane**; DataFusion remains the primary executor.

## Alignment and Fallback Prioritization
ArrowDSL remains an internal fallback lane that **does not compete** with
DataFusion or Ibis/SQLGlot. Consolidation is scoped to `src/arrowdsl` and
must not add new authoring surfaces.

Fallback priority stays:
1. DataFusion built-ins.
2. DataFusion UDF/UDTF using Arrow-native arrays.
3. Arrow compute / Acero fallback in ArrowDSL.

## Target Architecture (Internal)
PlanIR/ExprNode are attached to existing public types; there are no adapters.

```
Ibis/SQLGlot (authoring IR) -> DataFusion executor (primary)
                              |
                              v
Plan/QuerySpec/ExprIR -> PlanIR/ExprNode -> Planner -> SegmentPlan
SegmentPlan -> Acero or Kernel fallback -> Runtime -> Finalize
```

Key modules in the target state:
- `arrowdsl/ir/*` defines canonical expression and plan IR.
- `arrowdsl/ops/catalog.py` and `arrowdsl/kernel/specs.py` define op
  capabilities and lane constraints.
- `arrowdsl/plan/planner.py` segments IR into streaming-safe subplans.
- `arrowdsl/compile/*` contains Acero and kernel compilers for fallback lanes.
- `arrowdsl/plan/ordering_policy.py` centralizes ordering behavior.
- `arrowdsl/schema/*` centralizes type/encoding/metadata alignment.
- `arrowdsl/exec/*` runs fallback segments (single execution path).

---

## Scope 1: In-place IR Migration (ExprIR, Plan, QuerySpec)

### Pattern and Functions to Deploy
Public ArrowDSL types emit IR directly. There are no adapters or parallel
execution paths.

```python
# src/arrowdsl/plan/plan.py
@dataclass(frozen=True)
class Plan:
    ir: PlanIR
    label: str = ""
    ordering: Ordering = field(default_factory=Ordering.unordered)
    pipeline_breakers: tuple[str, ...] = ()

    def to_reader(self, *, ctx: ExecutionContext) -> RecordBatchReaderLike:
        result = run_plan(self, ctx=ctx, prefer_reader=True)
        if isinstance(result.value, pa.RecordBatchReader):
            return result.value
        raise ValueError("Plan is not streamable.")

    def to_table(self, *, ctx: ExecutionContext) -> TableLike:
        result = run_plan(self, ctx=ctx, prefer_reader=False)
        if isinstance(result.value, pa.RecordBatchReader):
            return result.value.read_all()
        return result.value


# src/arrowdsl/plan/query.py
def to_plan(self, *, dataset: ds.Dataset, ctx: ExecutionContext, label: str = "") -> Plan:
    builder = PlanBuilder()
    builder = builder.scan(
        dataset=dataset,
        columns=self.scan_columns(
            provenance=ctx.provenance,
            scan_provenance=ctx.runtime.scan.scan_provenance_columns,
        ),
        predicate=self.pushdown_expression(),
    )
    predicate = self.predicate_expression()
    if predicate is not None:
        builder = builder.filter(predicate=predicate)
    builder = builder.project(columns=self.scan_columns(
        provenance=ctx.provenance,
        scan_provenance=ctx.runtime.scan.scan_provenance_columns,
    ))
    return builder.build(label=label)


# src/arrowdsl/spec/expr_ir.py
def to_expr_spec(self, *, registry: ExprRegistry | None = None) -> ExprSpec:
    node = expr_from_expr_ir(self)

    def _materialize(table: TableLike) -> ArrayLike:
        args = [arg.materialize(table) for arg in self.args]
        result = pc.call_function(self.name, args, options=_deserialize_options(...))
        return result.combine_chunks() if isinstance(result, ChunkedArrayLike) else result

    return ComputeExprSpec(expr=node, materialize_fn=_materialize, registry=registry)
```

### Target Files
- Update: `src/arrowdsl/plan/plan.py`
- Update: `src/arrowdsl/plan/query.py`
- Update: `src/arrowdsl/spec/expr_ir.py`
- New: `src/arrowdsl/plan/builder.py`

### Implementation Checklist
- [ ] Make `Plan.ir` the sole execution source (remove decl/reader/table thunks).
- [ ] Add `PlanBuilder` to assemble IR nodes for `Plan` and `QuerySpec`.
- [ ] Ensure `ExprIR` compiles to `ExprNode` directly.
- [ ] Keep `Plan`/`QuerySpec` method signatures unchanged.

---

## Scope 2: Unified Op Catalog + Kernel Specs (single source of truth)

### Pattern and Functions to Deploy
Use one catalog to define lane support, ordering effects, and pipeline breakers,
then derive kernel specs and planner decisions from it.

```python
# src/arrowdsl/ops/catalog.py
@dataclass(frozen=True)
class OpDef:
    name: str
    ordering_effect: OrderingEffect
    pipeline_breaker: bool
    lanes: frozenset[Lane]
    acero_node: str | None = None
    kernel_name: str | None = None

OP_CATALOG = {
    "filter": OpDef(...),
    "project": OpDef(...),
    "order_by": OpDef(...),
    "aggregate": OpDef(...),
    "dedupe": OpDef(..., kernel_name="dedupe"),
}


# src/arrowdsl/kernel/specs.py
KERNEL_SPECS = {
    "dedupe": KernelSpec(name="dedupe", requires_ordering=True, impl_name="dedupe"),
    "winner_select": KernelSpec(...),
}
```

### Target Files
- Update: `src/arrowdsl/ops/catalog.py`
- Update: `src/arrowdsl/kernel/specs.py`
- Update: `src/arrowdsl/plan/ops.py` (remove duplicated ordering/breaker logic)
- Update: `src/arrowdsl/compute/kernels.py`

### Implementation Checklist
- [ ] Drive ordering effects and pipeline breakers from `OP_CATALOG` only.
- [ ] Make kernel capability checks use `KERNEL_SPECS`.
- [ ] Remove local ordering/breaker tables from plan/kernels.

---

## Scope 3: Planner Segmentation and Streamability

### Pattern and Functions to Deploy
Use catalog metadata to segment plans and centralize streamability logic.

```python
# src/arrowdsl/plan/planner.py
segments: list[Segment] = []
current_ops: list[OpNode] = []
current_lane: Lane | None = None
for node in ir.nodes:
    op_def = catalog[node.name]
    lane = select_lane(op_def, ctx=ctx)
    if current_lane is None:
        current_lane = lane
    if lane != current_lane and current_ops:
        segments.append(Segment(tuple(current_ops), current_lane))
        current_ops = []
        current_lane = lane
    current_ops.append(node)
    if op_def.pipeline_breaker:
        segments.append(Segment(tuple(current_ops), current_lane))
        breakers.append(node.name)
        current_ops = []
        current_lane = None
```

### Target Files
- Update: `src/arrowdsl/plan/planner.py`
- Update: `src/arrowdsl/plan/runner.py`
- Update: `src/arrowdsl/exec/runtime.py`

### Implementation Checklist
- [ ] Segment on lane changes and pipeline breakers.
- [ ] Determine streamability from `SegmentPlan` only.
- [ ] Ensure ordering metadata uses a shared policy (Scope 5).

---

## Scope 4: Compiler Consolidation (Expr + Plan + Kernel)

### Pattern and Functions to Deploy
Single compilation pipeline from IR to Acero or kernel fallback.

```python
# src/arrowdsl/compile/plan_compiler.py
for node in plan.nodes:
    op_def = catalog[node.name]
    if op_def.acero_node is None:
        raise ValueError(f"Op {node.name!r} has no Acero node.")
    decl = build_acero_decl(op_def.acero_node, node.args, decl, ctx)

# src/arrowdsl/compile/kernel_compiler.py
for node in plan.nodes:
    op_def = catalog[node.name]
    kernel = resolve_kernel(op_def.kernel_name, ctx=ctx)
    out = kernel(out, **node.args)
```

### Target Files
- Update: `src/arrowdsl/compile/expr_compiler.py`
- Update: `src/arrowdsl/compile/plan_compiler.py`
- Update: `src/arrowdsl/compile/kernel_compiler.py`
- Update: `src/arrowdsl/compute/macros.py`

### Implementation Checklist
- [ ] Use `ExprCompiler` from `ExprNode` everywhere.
- [ ] Remove any direct Acero declaration building outside `PlanCompiler`.
- [ ] Ensure kernel fallback uses `KernelCompiler` only.

---

## Scope 5: Unified Ordering Policy

### Pattern and Functions to Deploy
Centralize ordering inference, canonical sort, and metadata emission.

```python
# src/arrowdsl/plan/ordering_policy.py
keys = ordering_keys_for_schema(schema)
if determinism == DeterminismTier.CANONICAL and keys:
    indices = pc.sort_indices(table, sort_keys=list(keys))
    return table.take(indices), tuple(keys)
```

### Target Files
- Update: `src/arrowdsl/plan/ordering_policy.py`
- Update: `src/arrowdsl/plan/runner.py`
- Update: `src/arrowdsl/compute/kernels.py`
- Update: `src/arrowdsl/schema/metadata.py`

### Implementation Checklist
- [ ] Replace all local ordering logic with `ordering_policy`.
- [ ] Ensure identical metadata output across kernel and plan lanes.

---

## Scope 6: Schema Types and Builders Consolidation

### Pattern and Functions to Deploy
Centralize advanced Arrow type constructors and reuse in builders.

```python
# src/arrowdsl/schema/types.py
def list_view_type(value_type: DataTypeLike, *, large: bool = False) -> DataTypeLike:
    return pa.large_list_view(value_type) if large else pa.list_view(value_type)
```

### Target Files
- Update: `src/arrowdsl/schema/types.py`
- Update: `src/arrowdsl/schema/build.py`
- Update: `src/arrowdsl/schema/nested_builders.py`

### Implementation Checklist
- [ ] Use `schema/types.py` from all builders.
- [ ] Remove duplicate list/map/union constructors.

---

## Scope 7: Encoding Policy Unification

### Pattern and Functions to Deploy
Apply dictionary encoding consistently via a shared policy module.

```python
# src/arrowdsl/schema/encoding_policy.py
for name in policy.dictionary_cols:
    if name not in table.column_names:
        continue
    encoded = pc.dictionary_encode(table[name], index_type=index_type)
    table = table.set_column(idx, name, encoded.cast(target_type))
```

### Target Files
- Update: `src/arrowdsl/schema/encoding_policy.py`
- Update: `src/arrowdsl/schema/metadata.py`
- Update: `src/arrowdsl/schema/schema.py`
- Update: `src/arrowdsl/finalize/finalize.py`

### Implementation Checklist
- [ ] Eliminate `EncodingSpec` or duplicated encoding logic.
- [ ] Apply encoding in schema alignment and finalize only via policy.

---

## Scope 8: Join Output Resolution Consolidation

### Pattern and Functions to Deploy
Single join output resolver used in both plan and kernel lanes.

```python
# src/arrowdsl/plan/join_compiler.py
output = resolve_join_outputs(joined_columns, spec=spec).output_names
```

### Target Files
- Update: `src/arrowdsl/plan/join_compiler.py`
- Update: `src/arrowdsl/plan/joins.py`
- Update: `src/arrowdsl/compute/kernels.py`

### Implementation Checklist
- [ ] Remove local join output resolution logic from kernels and plans.
- [ ] Validate suffix collision once in `join_compiler`.

---

## Scope 9: Compute Registry + Function Capability

### Pattern and Functions to Deploy
Unify UDF registration and capability selection in one registry.

```python
# src/arrowdsl/compute/registry.py
@dataclass
class ComputeRegistry:
    registered: set[str]

    def ensure(self, spec: UdfSpec) -> str:
        if spec.name not in self.registered:
            pc.register_scalar_function(...)
        self.registered.add(spec.name)
        return spec.name
```

### Target Files
- Update: `src/arrowdsl/compute/registry.py`
- Update: `src/arrowdsl/compute/filters.py`
- Update: `src/arrowdsl/spec/expr_ir.py`

### Implementation Checklist
- [ ] Ensure all UDF registration goes through `ComputeRegistry`.
- [ ] Remove any ad hoc registration helpers elsewhere.

---

## Scope 10: Execution Runtime Consolidation

### Pattern and Functions to Deploy
A single runtime entry point executes `SegmentPlan` for fallback lanes.

```python
# src/arrowdsl/exec/runtime.py
for segment in plan.segments:
    if segment.lane == "acero":
        current = PlanCompiler(...).to_acero(PlanIR(segment.ops), ctx=ctx)
    elif segment.lane == "kernel":
        current = KernelCompiler(...).apply(PlanIR(segment.ops), table=current, ctx=ctx)
```

### Target Files
- Update: `src/arrowdsl/exec/runtime.py`
- Update: `src/arrowdsl/plan/runner.py`

### Implementation Checklist
- [ ] Remove any direct execution in `Plan` or `PlanSpec`.
- [ ] Ensure `run_plan` always goes through `segment_plan` + `run_segments`.

---

## Scope 11: plan_helpers Consolidation

### Pattern and Functions to Deploy
Make `plan_helpers` a thin wrapper over shared builders and schema helpers.

```python
# src/arrowdsl/plan_helpers.py
builder = PlanBuilder().from_plan(plan)
plan = builder.project(expressions, names).build(label=plan.label)
```

### Target Files
- Update: `src/arrowdsl/plan_helpers.py`
- Update: `src/arrowdsl/plan/builder.py`
- Update: `src/arrowdsl/schema/ops.py`

### Implementation Checklist
- [ ] Remove any duplicated projection/encoding logic from plan_helpers.
- [ ] Route helpers through `PlanBuilder` and schema utilities.

---

## Scope 12: Deletions and Dead Code Removal

### Pattern and Functions to Deploy
Remove modules that exist only for legacy conversion or dual execution paths.

### Target Files
- Delete: `src/arrowdsl/adapters/legacy.py`
- Update: `src/arrowdsl/plan/plan.py` (remove decl/table/reader thunks)
- Update: `src/arrowdsl/plan/query.py` (remove direct Acero op assembly)

### Implementation Checklist
- [ ] Remove adapter module and any `PlanIR` conversion helpers.
- [ ] Remove any plan execution branches that bypass `run_plan`.
- [ ] Prune unused imports/exports from `arrowdsl/__init__.py`.

---

## Scope 13: Conformance Tests and Invariants

### Pattern and Functions to Deploy
Validate that lane outputs match and ordering metadata is consistent.

```python
# tests/arrowdsl/conformance/test_lane_equivalence.py
kernel_out = KernelCompiler(...).apply(plan, table=table, ctx=ctx)
acero_out = PlanCompiler(...).to_acero(plan, ctx=ctx).to_table()
assert kernel_out.to_pydict() == acero_out.to_pydict()
```

### Target Files
- Update: `tests/arrowdsl/conformance/test_lane_equivalence.py`

### Implementation Checklist
- [ ] Add equivalence tests for filter/project/order_by/aggregate.
- [ ] Validate ordering metadata behavior for canonical determinism.
- [ ] Keep tests focused on cross-lane equivalence.

---

## Delivery Notes
- This roadmap consolidates ArrowDSL internals without changing external
  interfaces. Downstream packages should not require code changes.
- The consolidation is strictly internal and keeps ArrowDSL as a fallback lane
  aligned with the Target Architecture and PR-05/06 direction.
