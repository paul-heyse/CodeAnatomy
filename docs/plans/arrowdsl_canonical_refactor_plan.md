## ArrowDSL Canonical Refactor Plan

### Scope 1: Canonical Array Builder Surface

### Description
Consolidate duplicated array builders in `arrowdsl.schema.arrays` by keeping the
base builders (`build_struct`, `build_list`, `build_list_view`) and removing the
wrapper `*_array` variants. Update call sites to use the canonical builders.

### Code patterns
```python
# src/arrowdsl/schema/arrays.py

def build_struct(fields: dict[str, ArrayLike], *, mask: ArrayLike | None = None) -> StructArrayLike:
    ...


def build_list(offsets: ArrayLike, values: ArrayLike) -> ListArrayLike:
    ...


def build_list_view(
    offsets: ArrayLike,
    sizes: ArrayLike,
    values: ArrayLike,
    *,
    list_type: DataTypeLike | None = None,
    mask: ArrayLike | None = None,
) -> ListArrayLike:
    ...

# Call sites
# before: build_struct_array(...)
# after:  build_struct(...)
```

### Target files
- `src/arrowdsl/schema/arrays.py`
- `src/arrowdsl/finalize/finalize.py`
- `src/normalize/diagnostics.py`
- `src/extract/nested_lists.py`
- `src/extract/scip_extract.py`

### Implementation checklist
- [ ] Remove `build_struct_array`, `build_list_array`, and `build_list_view_array` wrappers.
- [ ] Update all call sites to use `build_struct`, `build_list`, or `build_list_view`.
- [ ] Trim `__all__` exports to the canonical builders only.

---

### Scope 2: Canonical Expression Spec Surface

### Description
Remove overlapping expression surfaces by consolidating on a single expression
spec model. Use `ExprSpec` + `FieldExpr`/`ConstExpr` for plan- and kernel-lane
expression semantics, and deprecate `E` as a separate surface where it simply
wraps `pc.field` and `pc.scalar`.

### Code patterns
```python
# src/arrowdsl/compute/expr.py
class ExprSpec(Protocol):
    def to_expression(self) -> ComputeExpression: ...
    def materialize(self, table: TableLike) -> ArrayLike: ...
    def is_scalar(self) -> bool: ...

# src/arrowdsl/schema/arrays.py
@dataclass(frozen=True)
class FieldExpr(ExprSpec):
    name: str
    def to_expression(self) -> ComputeExpression:
        return pc.field(self.name)

@dataclass(frozen=True)
class ConstExpr(ExprSpec):
    value: object
    dtype: DataTypeLike | None = None
    def to_expression(self) -> ComputeExpression:
        return ensure_expression(pc.scalar(self.value))
```

### Target files
- `src/arrowdsl/compute/expr.py`
- `src/arrowdsl/schema/arrays.py`
- `src/relspec/compiler.py`
- `src/arrowdsl/__init__.py`

### Implementation checklist
- [ ] Replace `E.field` and `E.scalar` uses with `FieldExpr(...).to_expression()` and
      `ConstExpr(...).to_expression()` or direct `pc.field`/`pc.scalar`.
- [ ] Remove or minimize the `E` facade if it no longer adds canonical value.
- [ ] Update exports so the canonical surface is `ExprSpec` + `FieldExpr` + `ConstExpr`.

---

### Scope 3: QuerySpec Plan Application Canonicalization

### Description
Move QuerySpec application into `QuerySpec.apply_to_plan(...)` to remove overlap
with `arrowdsl.plan_helpers.apply_query_spec`. Centralize predicate + projection
logic inside `QuerySpec` to keep scan and plan usage consistent.

### Code patterns
```python
# src/arrowdsl/plan/query.py
@dataclass(frozen=True)
class QuerySpec:
    ...

    def apply_to_plan(self, plan: Plan, *, ctx: ExecutionContext) -> Plan:
        predicate = self.predicate_expression()
        if predicate is not None:
            plan = plan.filter(predicate, ctx=ctx)
        cols = self.scan_columns(provenance=ctx.provenance)
        if isinstance(cols, dict):
            names = list(cols.keys())
            exprs = list(cols.values())
        else:
            names = list(cols)
            exprs = [pc.field(name) for name in cols]
        return plan.project(exprs, names, ctx=ctx)

# src/arrowdsl/plan_helpers.py
# remove apply_query_spec
```

### Target files
- `src/arrowdsl/plan/query.py`
- `src/arrowdsl/plan_helpers.py`
- `src/normalize/plan_helpers.py`
- `src/extract/tables.py`
- `src/extract/*.py` (modules using `apply_query_spec`)

### Implementation checklist
- [ ] Add `QuerySpec.apply_to_plan` with predicate + projection application.
- [ ] Replace `apply_query_spec` usage with `spec.apply_to_plan(plan, ctx=ctx)`.
- [ ] Remove `apply_query_spec` from `arrowdsl.plan_helpers` and update exports.

---

### Scope 4: Projection Helper Consolidation

### Description
Unify the overlapping projection helpers (`append_projection`, `rename_plan_columns`)
into a single projection utility that handles base column selection, renames, and
extras in one pass.

### Code patterns
```python
# src/arrowdsl/plan_helpers.py

def project_columns(
    plan: Plan,
    *,
    base: Sequence[str],
    rename: Mapping[str, str] | None = None,
    extras: Sequence[tuple[ComputeExpression, str]] = (),
    ctx: ExecutionContext | None = None,
) -> Plan:
    rename = rename or {}
    names = [rename.get(name, name) for name in base]
    exprs = [pc.field(name) for name in base]
    for expr, name in extras:
        exprs.append(expr)
        names.append(name)
    return plan.project(exprs, names, ctx=ctx)
```

### Target files
- `src/arrowdsl/plan_helpers.py`
- `src/normalize/plan_helpers.py`
- `src/extract/symtable_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/runtime_inspect_extract.py`

### Implementation checklist
- [ ] Introduce a unified projection helper (`project_columns`).
- [ ] Replace `append_projection` and `rename_plan_columns` usage.
- [ ] Remove the old helpers and update `__all__` exports.

---

### Scope 5: Constant Column Canonicalization

### Description
Standardize constant column creation on `const_array` for table-level operations
and make `ConstExpr.materialize` delegate to `const_array` to avoid duplicated
constant-building logic.

### Code patterns
```python
# src/arrowdsl/schema/arrays.py

def const_array(n: int, value: object, *, dtype: DataTypeLike | None = None) -> ArrayLike:
    ...

@dataclass(frozen=True)
class ConstExpr(ExprSpec):
    value: object
    dtype: DataTypeLike | None = None

    def materialize(self, table: TableLike) -> ArrayLike:
        return const_array(table.num_rows, self.value, dtype=self.dtype)
```

### Target files
- `src/arrowdsl/schema/arrays.py`
- `src/arrowdsl/finalize/finalize.py`
- `src/normalize/span_pipeline.py`
- `src/cpg/builders.py`

### Implementation checklist
- [ ] Refactor `ConstExpr.materialize` to call `const_array`.
- [ ] Replace ad hoc constant array construction with `const_array` where possible.
- [ ] Confirm dictionary encoding behavior remains unchanged.
