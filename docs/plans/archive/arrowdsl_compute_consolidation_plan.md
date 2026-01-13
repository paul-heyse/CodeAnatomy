## ArrowDSL Compute Consolidation Plan

### Goals
- Consolidate overlapping ArrowDSL compute, plan, and schema helpers into a single modular surface.
- Reduce bespoke kernel-lane logic by routing through PyArrow/Acero primitives where possible.
- Centralize UDF registration, hashing, and row-ingestion to eliminate parallel implementations.
- Preserve ordering semantics, schema metadata, and strict typing.

### Constraints
- Keep plan lane (Acero) and kernel lane responsibilities separated.
- Maintain deterministic ordering metadata and pipeline-breaker visibility.
- No relative imports; keep modules fully typed and pyright clean.
- Avoid suppressed lint/type errors; prefer structural consolidation.

---

### Scope 1: Predicate Spec Consolidation into Compute Macros
**Description**
Collapse `compute/predicates.py` into the canonical macro surface so all predicates
share a single ExprSpec implementation with consistent plan/kernel behavior.

**Code patterns**
```python
# src/arrowdsl/compute/macros.py
@dataclass(frozen=True)
class UnaryExprSpec:
    expr_fn: Callable[[ComputeExpression], ComputeExpression]
    materialize_fn: Callable[[ArrayLike], ArrayLike]
    field: str

    def to_expression(self) -> ComputeExpression:
        return self.expr_fn(pc.field(self.field))

    def materialize(self, table: TableLike) -> ArrayLike:
        return self.materialize_fn(table[self.field])
```

**Target files**
- Update: `src/arrowdsl/compute/macros.py`
- Update: `src/arrowdsl/compute/predicates.py`
- Update: `src/arrowdsl/compute/expr.py`

**Implementation checklist**
- [ ] Replace predicate classes with macro-backed specs.
- [ ] Ensure materialize/to_expression share the same kernel path.
- [ ] Keep scalar-safety checks centralized.

**Status**
Not started.

---

### Scope 2: Centralize UDF Registration in Compute Registry
**Description**
Move all UDF registrations (JSON stringify, expr context normalization, hash64)
through `ComputeRegistry` to eliminate duplicate caching and registration logic.

**Code patterns**
```python
# src/arrowdsl/compute/registry.py
def ensure_udf(spec: UdfSpec) -> str:
    return default_registry().ensure(spec)

# src/arrowdsl/compute/udfs.py
return ensure_udf(UdfSpec(...))
```

**Target files**
- Update: `src/arrowdsl/compute/registry.py`
- Update: `src/arrowdsl/compute/udfs.py`
- Update: `src/arrowdsl/core/ids.py`

**Implementation checklist**
- [ ] Move hash64 UDF registration into the registry path.
- [ ] Remove duplicate caching logic for UDF names.
- [ ] Keep UDF metadata and inputs/outputs centralized.

**Status**
Not started.

---

### Scope 3: Hash Pipeline Simplification via PyArrow Compute
**Description**
Unify hashing to use a single compute path that prefers native `pc.hash*`/string
concat when available, with UDF fallback only when needed.

**Code patterns**
```python
# src/arrowdsl/core/ids.py
def hash_expression(spec: HashSpec, *, available: Sequence[str] | None = None) -> ComputeExpression:
    parts = [pc.field(col) for col in spec.cols if available is None or col in available]
    joined = pc.binary_join_element_wise(parts, separator=pc.scalar(spec.null_sentinel))
    return ensure_expression(pc.hash64(joined))
```

**Target files**
- Update: `src/arrowdsl/core/ids.py`
- Update: `src/arrowdsl/compute/expr_specs.py`

**Implementation checklist**
- [ ] Prefer native hash kernels when available.
- [ ] Keep UDF fallback for compatibility.
- [ ] Remove duplicated hash concatenation logic.

**Status**
Not started.

---

### Scope 4: Interval Alignment via Join Primitives
**Description**
Replace bespoke interval alignment logic with join-based paths (hash join + filter,
or join_asof when compatible) to reuse the canonical join surface.

**Code patterns**
```python
# src/arrowdsl/plan/joins.py
def interval_join(left: Plan, right: Plan, *, spec: JoinSpec, ctx: ExecutionContext) -> Plan:
    joined = left.join(right, spec=spec, ctx=ctx)
    return joined.filter(interval_predicate, ctx=ctx)
```

**Target files**
- Update: `src/arrowdsl/compute/kernels.py`
- Update: `src/arrowdsl/plan/joins.py`
- Update: `src/arrowdsl/plan/ops.py`

**Implementation checklist**
- [ ] Express interval alignment via join + filter or join_asof.
- [ ] Reuse JoinSpec/JoinOutputSpec for outputs.
- [ ] Retire bespoke interval matching helpers.

**Status**
Not started.

---

### Scope 5: Column Ops Unification (Plan + Kernel)
**Description**
Unify column-or-null logic and default column handling into shared column specs,
so plan-lane and kernel-lane use the same definition.

**Code patterns**
```python
# src/arrowdsl/compute/macros.py
@dataclass(frozen=True)
class ColumnOrNullExpr(ComputeExprSpec):
    name: str
    dtype: DataTypeLike
```

**Target files**
- Update: `src/arrowdsl/compute/macros.py`
- Update: `src/arrowdsl/plan_helpers.py`
- Update: `src/arrowdsl/schema/builders.py`

**Implementation checklist**
- [ ] Replace `column_or_null_expr`/`column_or_null` with a shared spec.
- [ ] Keep typed null casting consistent across lanes.
- [ ] Remove duplicated helper implementations.

**Status**
Not started.

---

### Scope 6: Scan Compilation Unification
**Description**
Collapse scan compilation into a single builder so `QuerySpec` and `ScanSpec` share
one plan construction path with consistent ordering and pipeline-breaker metadata.

**Code patterns**
```python
# src/arrowdsl/plan/query.py
def to_plan(self, *, dataset: ds.Dataset, ctx: ExecutionContext) -> Plan:
    decl = compile_to_acero_scan(dataset, spec=self, ctx=ctx)
    return Plan(decl=decl, ordering=Ordering.implicit(), pipeline_breakers=())
```

**Target files**
- Update: `src/arrowdsl/plan/query.py`
- Update: `src/arrowdsl/plan/source.py`
- Update: `src/arrowdsl/plan/scan_specs.py`

**Implementation checklist**
- [ ] Add a single `QuerySpec.to_plan`/`ScanSpec.to_plan` entrypoint.
- [ ] Route dataset scan compilation through that entrypoint.
- [ ] Remove parallel scan/plan construction in `plan.source`.

**Status**
Not started.

---

### Scope 7: Dataset Telemetry Consolidation via Scanner Tasks
**Description**
Unify dataset fragment telemetry with scanner task inspection using PyArrow dataset
APIs to avoid parallel fragment counting logic.

**Code patterns**
```python
# src/arrowdsl/plan/fragments.py
def scan_task_count(scanner: ds.Scanner) -> int:
    return sum(1 for _ in scanner.scan_tasks())
```

**Target files**
- Update: `src/arrowdsl/plan/fragments.py`
- Update: `src/arrowdsl/plan/query.py`

**Implementation checklist**
- [ ] Base telemetry on scanner tasks where available.
- [ ] Retire redundant fragment/row-group counters.
- [ ] Keep fragment hints for reporting, not control flow.

**Status**
Not started.

---

### Scope 8: Schema Unification + Alignment Consolidation
**Description**
Route schema unification through a single alignment pipeline that preserves metadata
and applies chunk policy consistently.

**Code patterns**
```python
# src/arrowdsl/schema/unify.py
def unify_tables(tables: Sequence[TableLike]) -> TableLike:
    schema = unify_schemas([t.schema for t in tables])
    aligned = [align_to_schema(t, schema=schema, safe_cast=True)[0] for t in tables]
    return pa.concat_tables(aligned)
```

**Target files**
- Update: `src/arrowdsl/schema/unify.py`
- Update: `src/arrowdsl/schema/alignment.py`
- Update: `src/arrowdsl/schema/schema.py`

**Implementation checklist**
- [ ] Use `align_to_schema` for table alignment in unification.
- [ ] Preserve metadata via a single alignment policy.
- [ ] Apply chunk policy at one point in the pipeline.

**Status**
Not started.

---

### Scope 9: Nested Builder Consolidation for Struct/List/Map
**Description**
Unify struct/list/map builders so nested arrays are built through one canonical
entrypoint that uses PyArrow’s advanced nested semantics.

**Code patterns**
```python
# src/arrowdsl/schema/nested_builders.py
def build_struct_from_rows(rows: Sequence[Mapping[str, object]], *, dtype: pa.StructType) -> ArrayLike:
    return pa.array(rows, type=dtype)
```

**Target files**
- Update: `src/arrowdsl/schema/nested_builders.py`
- Update: `src/arrowdsl/schema/arrays.py`

**Implementation checklist**
- [ ] Replace duplicate struct/list builders with a canonical path.
- [ ] Use `pa.array(..., type=...)` for struct inference when safe.
- [ ] Keep list_view/map/dictionary support centralized.

**Status**
Not started.

---

### Scope 10: Row Ingestion Unification Across IO + Plan
**Description**
Unify row ingestion so `spec/io.py` and `plan/rows.py` share a single row→table path,
with nested-aware conversion when needed.

**Code patterns**
```python
# src/arrowdsl/plan/rows.py
def rows_to_table(rows: Sequence[Mapping[str, object]], schema: SchemaLike) -> TableLike:
    return table_from_rows(schema, rows)
```

**Target files**
- Update: `src/arrowdsl/plan/rows.py`
- Update: `src/arrowdsl/spec/io.py`

**Implementation checklist**
- [ ] Reuse `table_from_rows` for schema-aligned ingestion.
- [ ] Keep nested coercion via `array_from_values` for complex types.
- [ ] Remove duplicate row->array conversions once call sites migrate.

**Status**
Not started.
