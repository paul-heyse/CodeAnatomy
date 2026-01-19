## IbisQuerySpec Migration Plan

### Goal
Replace ArrowDSL `QuerySpec` usage with `IbisQuerySpec` while preserving the plan-lane fallback
execution path. The pivot should standardize on `ExprIR` for derived expressions and predicates,
then adapt Ibis specs to Arrow plan-lane constructs only where necessary.

### Scope Summary
- Unify expression IR around `arrowdsl.spec.expr_ir.ExprIR`.
- Move dataset/query specs to `ibis_engine.query_compiler.IbisQuerySpec`.
- Introduce a plan-lane adapter that compiles `IbisQuerySpec` to Arrow plan-lane expressions.
- Update normalize registry builders/specs and any callers (tests included).
- Deprecate or remove ArrowDSL `QuerySpec` once no callers remain.

---

## Phase 0: Inventory and Boundaries

### Scope Item 0.1: Confirm remaining `QuerySpec` call sites

Representative snippet (usage to eliminate):
```python
from arrowdsl.plan.query import ProjectionSpec, QuerySpec

query = QuerySpec(
    projection=ProjectionSpec(base=("a", "b")),
    predicate=some_expr_spec,
)
```

Target files
- `src/normalize/registry_builders.py`
- `src/normalize/registry_specs.py`
- `src/normalize/schemas.py`
- `src/schema_spec/system.py`
- `src/arrowdsl/spec/infra.py`
- `src/arrowdsl/plan_utils.py`
- `tests/unit/test_required_columns_scan.py`
- `tests/unit/test_scan_from_batches.py`
- `tests/unit/test_ordering_policy.py`

Implementation checklist
- [ ] Confirm all `QuerySpec` imports in `src/` and `tests/`.
- [ ] Confirm any dynamic imports or registry references to `QuerySpec`.
- [ ] Decide whether to keep a compatibility shim or remove entirely.

---

## Phase 1: Unify Expression IR

### Scope Item 1.1: Update `DerivedFieldSpec` to use `ExprIR`

Representative snippet (new type contract):
```python
from arrowdsl.spec.expr_ir import ExprIR

@dataclass(frozen=True)
class DerivedFieldSpec:
    name: str
    expr: ExprIR
```

Target files
- `src/schema_spec/specs.py`
- `src/normalize/registry_rows.py`
- `src/normalize/policies.py` (if policy helpers return ExprIR)

Implementation checklist
- [ ] Update `DerivedFieldSpec.expr` annotation to `ExprIR`.
- [ ] Replace `ExprSpec` uses in registry rows with `ExprIR` constructors.
- [ ] Ensure any helper builders produce `ExprIR` nodes (no `ExprSpec`).

### Scope Item 1.2: Provide simple `ExprIR` builders for common macros

Representative snippet (helper constructors):
```python
from arrowdsl.spec.expr_ir import ExprIR

def field(name: str) -> ExprIR:
    return ExprIR(op="field", name=name)

def literal(value: object) -> ExprIR:
    return ExprIR(op="literal", value=value)

def call(name: str, *args: ExprIR) -> ExprIR:
    return ExprIR(op="call", name=name, args=tuple(args))
```

Target files
- `src/ibis_engine/hashing.py` (already uses `ExprIR`)
- `src/normalize/registry_rows.py` (convert derived field macros)
- New module if needed: `src/ibis_engine/expr_builders.py`

Implementation checklist
- [ ] Add minimal builder helpers (optional but improves readability).
- [ ] Update derived field specs to use builder helpers.

---

## Phase 2: Move Dataset Specs to `IbisQuerySpec`

### Scope Item 2.1: Replace `DatasetSpec.query_spec` with `IbisQuerySpec`

Representative snippet (schema spec surface):
```python
from ibis_engine.query_compiler import IbisProjectionSpec, IbisQuerySpec

@dataclass(frozen=True)
class DatasetSpec:
    query_spec: IbisQuerySpec | None = None

    def query(self) -> IbisQuerySpec:
        if self.query_spec is not None:
            return self.query_spec
        cols = tuple(field.name for field in self.table_spec.fields)
        derived = {spec.name: spec.expr for spec in self.derived_fields}
        return IbisQuerySpec(
            projection=IbisProjectionSpec(base=cols, derived=derived),
            predicate=self.predicate,
            pushdown_predicate=self.pushdown_predicate,
        )
```

Target files
- `src/schema_spec/system.py`
- `src/arrowdsl/spec/infra.py`
- `src/schema_spec/__init__.py`

Implementation checklist
- [ ] Swap `QuerySpec` -> `IbisQuerySpec` in `DatasetSpec`.
- [ ] Update `DatasetRegistration.query_spec` type to `IbisQuerySpec`.
- [ ] Adjust imports and re-exports.
- [ ] Ensure `predicate`/`pushdown_predicate` types align with `ExprIRLike`.

---

## Phase 3: Update Normalize Registry Builders/Specs

### Scope Item 3.1: Migrate `build_query_spec` to `IbisQuerySpec`

Representative snippet:
```python
from ibis_engine.query_compiler import IbisProjectionSpec, IbisQuerySpec

def build_query_spec(row: DatasetRow) -> IbisQuerySpec:
    base_cols = _base_field_keys(row)
    derived = {spec.name: spec.expr for spec in row.derived}
    if not derived:
        return IbisQuerySpec.simple(*base_cols)
    return IbisQuerySpec(
        projection=IbisProjectionSpec(base=tuple(base_cols), derived=derived),
    )
```

Target files
- `src/normalize/registry_builders.py`
- `src/normalize/registry_specs.py`
- `src/normalize/schemas.py`

Implementation checklist
- [ ] Replace `QuerySpec` returns with `IbisQuerySpec`.
- [ ] Update imports and re-exports in normalize modules.
- [ ] Update `dataset_query` signature and callers.

---

## Phase 4: Introduce Plan-Lane Adapter

### Scope Item 4.1: Convert `IbisQuerySpec` to Arrow plan-lane spec

Representative snippet (adapter):
```python
from arrowdsl.plan.query import ProjectionSpec, QuerySpec
from arrowdsl.spec.expr_ir import ExprIR
from ibis_engine.query_compiler import IbisQuerySpec

def ibis_query_to_plan(spec: IbisQuerySpec) -> QuerySpec:
    derived = {name: expr.to_expr_spec() for name, expr in spec.projection.derived.items()}
    return QuerySpec(
        projection=ProjectionSpec(base=spec.projection.base, derived=derived),
        predicate=spec.predicate.to_expr_spec() if spec.predicate is not None else None,
        pushdown_predicate=(
            spec.pushdown_predicate.to_expr_spec() if spec.pushdown_predicate is not None else None
        ),
    )
```

Target files
- New module: `src/arrowdsl/plan/query_adapter.py` (preferred)
- `src/arrowdsl/plan/scan_builder.py`
- `src/arrowdsl/plan/scan_io.py`

Implementation checklist
- [ ] Add adapter from `IbisQuerySpec` to plan-lane `QuerySpec`.
- [ ] Ensure adapter expects `ExprIR` and calls `to_expr_spec()`.
- [ ] Update scan builder to accept Ibis specs and call adapter.
- [ ] Keep Arrow plan lane isolated behind the adapter.

---

## Phase 5: Update Plan Helpers and Tests

### Scope Item 5.1: Replace `QuerySpec` in plan helpers

Representative snippet:
```python
from ibis_engine.query_compiler import IbisProjectionSpec, IbisQuerySpec

def query_for_schema(schema: SchemaLike) -> IbisQuerySpec:
    return IbisQuerySpec(projection=IbisProjectionSpec(base=tuple(schema.names)))
```

Target files
- `src/arrowdsl/plan_utils.py`
- `tests/unit/test_required_columns_scan.py`
- `tests/unit/test_scan_from_batches.py`
- `tests/unit/test_ordering_policy.py`

Implementation checklist
- [ ] Update helper utilities to return `IbisQuerySpec`.
- [ ] Update tests to construct `IbisQuerySpec` inputs.
- [ ] Ensure plan adapter coverage in tests (explicit or via scan).

---

## Phase 6: Deprecate or Remove `QuerySpec`

### Scope Item 6.1: Remove remaining `QuerySpec` exports

Representative snippet (compat shim if needed):
```python
class QuerySpec:
    def __init__(self, *args: object, **kwargs: object) -> None:
        raise RuntimeError("QuerySpec is deprecated; use IbisQuerySpec.")
```

Target files
- `src/arrowdsl/plan/query.py`
- `src/arrowdsl/plan/__init__.py`
- `src/schema_spec/__init__.py`

Implementation checklist
- [ ] Confirm no runtime imports remain.
- [ ] Remove or replace `QuerySpec` exports.
- [ ] Update any documentation references.

---

## Validation Plan

### Quality gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`

### Targeted tests
- `uv run pytest tests/unit/test_required_columns_scan.py`
- `uv run pytest tests/unit/test_scan_from_batches.py`
- `uv run pytest tests/unit/test_ordering_policy.py`

---

## File List (Summary)

Core type changes
- `src/schema_spec/specs.py`
- `src/schema_spec/system.py`
- `src/arrowdsl/spec/infra.py`

Normalize registry updates
- `src/normalize/registry_rows.py`
- `src/normalize/registry_builders.py`
- `src/normalize/registry_specs.py`
- `src/normalize/schemas.py`

Plan-lane adapter updates
- `src/arrowdsl/plan/query_adapter.py` (new)
- `src/arrowdsl/plan/scan_builder.py`
- `src/arrowdsl/plan/scan_io.py`
- `src/arrowdsl/plan_utils.py`

Tests
- `tests/unit/test_required_columns_scan.py`
- `tests/unit/test_scan_from_batches.py`
- `tests/unit/test_ordering_policy.py`

---

## Sequencing Checklist

1) Unify expression IR (`ExprIR`) and update derived field specs.  
2) Swap dataset spec surfaces to `IbisQuerySpec`.  
3) Migrate normalize registry builders/specs.  
4) Add plan-lane adapter and update scan builder/IO.  
5) Update plan helpers and unit tests.  
6) Deprecate or remove `QuerySpec`.  
7) Run lint/type checks and targeted tests.
