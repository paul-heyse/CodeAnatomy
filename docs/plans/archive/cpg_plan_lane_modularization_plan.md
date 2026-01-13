# Implementation Plan: CPG Plan-Lane Modularization (Acero/DSL)

This plan migrates CPG construction to a plan-first, Acero-backed pipeline that
materializes only at the finalize boundary. It mirrors the extract plan patterns
(`Plan`, `PlanSpec`, `projection_for_schema`, `hash_expression`, plan-lane joins)
and embraces the ExecPlan semantics described in `arrow_acero_dsl_guide.md`.

## Assumptions (confirmed)
- Build surfaces should return plans/readers and materialize only at finalize.
- Output ordering does not need to be deterministic as long as IDs are stable.
- Acero is required for all CPG builds; kernel-lane fallbacks are not needed.

## Investigation summary: property transforms
- The only `PropFieldSpec.transform` functions are:
  - `expr_context_value` (string normalization + upper)
  - `flag_to_bool` (bool/int flag -> optional bool)
- Additional implicit transform today: `PropBuilder` JSON-serializes any non-scalar
  property values (e.g., `qnames`, `callee_qnames`, `details`) via `json.dumps`.
- Result: `flag_to_bool` is trivially vectorizable; `expr_context_value` is either
  vectorizable with string kernels or should be a compute UDF; JSON serialization
  needs a UDF (or upstream normalization).

---

## Scope 1: Plan-First CPG Build Surface

### Description
Introduce plan-based build functions for nodes/edges/props. `build_*_raw` will
return a `Plan` and will not call `to_table()` until finalize.

### Code patterns
```python
# src/cpg/plan_helpers.py
def finalize_plan(plan: Plan, *, ctx: ExecutionContext) -> TableLike:
    return PlanSpec.from_plan(plan).to_table(ctx=ctx)
```

```python
# src/cpg/build_nodes.py
def build_cpg_nodes_raw(...) -> Plan:
    plans = [emit_node_plan(...)]
    return union_all_plans(plans, label="cpg_nodes_raw")
```

### Target files
- New: `src/cpg/plan_helpers.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`
- Update: `src/cpg/artifacts.py`

### Implementation checklist
- [x] Make `build_cpg_*_raw` return `Plan` (no separate `build_cpg_*_plan` functions).
- [x] Replace early `to_table()` calls with `PlanSpec.to_table()` at finalize only.
- [x] Keep existing `build_cpg_*` entrypoints to return `CpgBuildArtifacts` with finalized tables.
- [x] Ensure all plan builders accept and require an `ExecutionContext`.

---

## Scope 2: Plan Catalog + Derived Sources

### Description
Extend the CPG table catalog to serve `Plan` sources (table-backed or derived),
matching the extract pattern in `src/extract/tables.py`.

### Code patterns
```python
# src/cpg/catalog.py
type PlanGetter = Callable[[Mapping[str, TableLike | Plan]], PlanSource | None]

@dataclass(frozen=True)
class PlanRef:
    name: str
    derive: Callable[[PlanCatalog, ExecutionContext], PlanSource | None] | None = None

    def getter(self) -> PlanGetter:
        def _get(tables: Mapping[str, TableLike | Plan]) -> PlanSource | None:
            value = tables.get(self.name)
            if isinstance(value, Plan):
                return value
            if value is None:
                return None
            return Plan.table_source(value, label=self.name)
        return _get
```

### Target files
- Update: `src/cpg/catalog.py`
- New: `src/cpg/plan_helpers.py` (plan/table normalization helpers)
- Update: `src/cpg/spec_registry.py` (use `PlanRef` for plan getters)

### Implementation checklist
- [x] Add `PlanRef` and `PlanCatalog` (parallel to `TableRef/TableCatalog`).
- [x] Register derived plan sources (file nodes, symbol nodes).
- [x] Ensure all plan specs use `PlanRef.getter()` and normalize `PlanSource` to `Plan`.

---

## Scope 3: Plan-Lane Node Emission

### Description
Replace `emit_nodes_from_table` with a plan projection that builds node columns
using compute expressions (coalesce for id/path/span columns).

### Code patterns
```python
# src/cpg/emit_nodes.py
def node_plan(table: Plan, *, spec: NodeEmitSpec, schema_version: int) -> Plan:
    node_id = pc.coalesce(*(pc.field(col) for col in spec.id_cols))
    exprs = [
        node_id,
        pc.scalar(spec.node_kind.value),
        pc.coalesce(*(pc.field(col) for col in spec.path_cols)),
        pc.coalesce(*(pc.field(col) for col in spec.bstart_cols)),
        pc.coalesce(*(pc.field(col) for col in spec.bend_cols)),
        pc.coalesce(*(pc.field(col) for col in spec.file_id_cols)),
        pc.scalar(schema_version),
    ]
    names = ["node_id", "node_kind", "path", "bstart", "bend", "file_id", "schema_version"]
    return table.project(exprs, names)
```

### Target files
- New: `src/cpg/emit_nodes.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/specs.py` (plan-lane emit helpers)

### Implementation checklist
- [x] Implement `node_plan(...)` projection with `pc.coalesce`.
- [x] Add optional schema alignment via `projection_for_schema`.
- [x] Replace `NodeBuilder` usage with plan-lane assembly and union.

---

## Scope 4: Plan-Lane Edge Relations + Emission

### Description
Make each `EdgeRelationSpec` return a `Plan` and keep joins/filters in the plan
lane. Edge emission becomes a plan projection (including `hash_expression` for
edge IDs and default fill logic).

### Code patterns
```python
# src/cpg/emit_edges.py
def edge_plan(rel: Plan, *, spec: EdgeEmitSpec, schema_version: int) -> Plan:
    edge_id = hash_expression(HashSpec(prefix="edge", cols=("edge_kind", "src", "dst")))
    exprs = [
        edge_id,
        pc.scalar(spec.edge_kind.value),
        pc.field("src"),
        pc.field("dst"),
        pc.field("path"),
        pc.field("bstart"),
        pc.field("bend"),
        pc.fill_null(pc.field("origin"), pc.scalar(spec.origin)),
        pc.fill_null(pc.field("resolution_method"), pc.scalar(spec.default_resolution_method)),
        pc.field("confidence"),
        pc.field("score"),
    ]
    names = [
        "edge_id",
        "edge_kind",
        "src_node_id",
        "dst_node_id",
        "path",
        "bstart",
        "bend",
        "origin",
        "resolution_method",
        "confidence",
        "score",
    ]
    return rel.project(exprs, names)
```

### Target files
- Update: `src/cpg/relations.py`
- New: `src/cpg/emit_edges.py`
- Update: `src/cpg/build_edges.py`

### Implementation checklist
- [x] Change all relation builders to return `Plan`, not `TableLike`.
- [x] Replace `_with_repo_file_ids` with a plan-lane hash join.
- [x] Emit edges via plan projection using `hash_expression`.
- [x] Add dictionary encoding in plan-lane (`encode_expression`) before finalize.

---

## Scope 5: Plan-Lane Prop Emission (Vectorized)

### Description
Replace `PropBuilder`’s row loop with plan-based projection and union. Each
property field becomes its own plan (row-preserving) and is then unioned.

### Code patterns
```python
# src/cpg/emit_props.py
def prop_field_plan(
    table: Plan,
    *,
    entity_kind: EntityKind,
    entity_id_expr: ComputeExpression,
    field: PropFieldSpec,
    schema_version: int,
) -> Plan:
    value_expr, value_cols = value_expression(field)
    exprs = [
        pc.scalar(entity_kind.value),
        entity_id_expr,
        pc.scalar(field.prop_key),
        *value_cols,
        pc.scalar(schema_version),
    ]
    names = [
        "entity_kind",
        "entity_id",
        "prop_key",
        "value_str",
        "value_int",
        "value_float",
        "value_bool",
        "value_json",
        "schema_version",
    ]
    plan = table.project(exprs, names)
    if field.skip_if_none:
        plan = plan.filter(pc.is_valid(pc.field("value_str")) | pc.is_valid(pc.field("value_json")))
    return plan
```

### Target files
- New: `src/cpg/emit_props.py`
- Update: `src/cpg/build_props.py`
- Update: `src/cpg/specs.py` (plan-lane helpers for prop specs)

### Implementation checklist
- [x] Build per-field plans and union them with `union_all_plans`.
- [x] Add a `node_kind` prop plan when `node_kind` is present.
- [x] Preserve rows with missing/invalid entity IDs (nulls captured for quality).
- [x] Ensure heavy JSON gating happens at plan selection time.

---

## Scope 6: Property Transforms + JSON UDFs

### Description
Convert property transforms to compute expressions or compute UDFs. Introduce
generic JSON string UDF to replace `json.dumps` for list/struct values.

### Code patterns
```python
# src/arrowdsl/compute/udfs.py
def register_expr_context_udf() -> None:
    def _expr_ctx(ctx: pa.UdfContext, val: pa.ScalarLike) -> pa.ScalarLike:
        raw = val.as_py()
        if not isinstance(raw, str):
            return pa.scalar(None, type=pa.string())
        text = raw.rsplit(".", 1)[-1].strip().upper()
        return pa.scalar(text or None, type=pa.string())
    pa.pc.register_scalar_function(_expr_ctx, "expr_ctx_norm", ..., pa.string(), pa.string())
```

```python
# src/cpg/prop_transforms.py (plan-lane expression helpers)
def expr_context_expr(col: str) -> ComputeExpression:
    return pc.call_function("expr_ctx_norm", [pc.field(col)])
```

### Target files
- New: `src/arrowdsl/compute/udfs.py`
- Update: `src/cpg/prop_transforms.py` (add plan-lane expression helpers)
- Update: `src/cpg/spec_registry.py` (use expression helpers)

### Implementation checklist
- [x] Implement `expr_ctx_norm` UDF or equivalent string-kernel expression.
- [x] Implement `flag_to_bool` expression with `pc.if_else`.
- [x] Add `to_json_string` UDF for list/struct values and use it for JSON props.
- [ ] Document which prop keys rely on JSON serialization (`qnames`, `callee_qnames`, `details`).

---

## Scope 7: Plan-Lane Quality Artifacts

### Description
Generate quality tables via plan filters so invalid IDs are computed alongside
plan outputs and materialized only at finalize.

### Code patterns
```python
# src/cpg/quality.py
def quality_plan_from_ids(plan: Plan, *, id_col: str, issue: str) -> Plan:
    values = pc.field(id_col)
    invalid = pc.or_(pc.is_null(values), pc.equal(values, pc.scalar("0")))
    return plan.filter(invalid)
```

### Target files
- Update: `src/cpg/quality.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`

### Implementation checklist
- [x] Add `quality_plan_from_ids` returning a plan (not table).
- [x] Materialize quality tables only at finalize boundary.
- [x] Preserve missing-column behavior (emit null IDs when column absent).

---

## Scope 8: Finalize Boundary + Output Surfaces

### Description
Standardize finalize boundary logic for CPG outputs (nodes/edges/props/quality),
using `PlanSpec` for pipeline-breaker awareness and schema alignment/encoding
projection before materialization.

### Code patterns
```python
# src/cpg/plan_helpers.py
def finalize_cpg_dataset(plan: Plan, *, spec: DatasetSpec, ctx: ExecutionContext) -> TableLike:
    aligned = align_plan(plan, schema=spec.arrow_schema, ctx=ctx)
    return PlanSpec.from_plan(aligned).to_table(ctx=ctx)
```

### Target files
- New: `src/cpg/plan_helpers.py`
- Update: `src/cpg/build_nodes.py`
- Update: `src/cpg/build_edges.py`
- Update: `src/cpg/build_props.py`
- Update: `src/hamilton_pipeline/modules/cpg_build.py`

### Implementation checklist
- [x] Align plan outputs to schemas via `projection_for_schema`.
- [x] Apply dictionary encoding via `encode_expression` in plan-lane.
- [x] Ensure `CpgBuildArtifacts` holds finalized tables + quality tables.

---

## Scope 9: Tests + Documentation

### Description
Add plan-lane tests similar to extract to ensure plan outputs equal prior tables
and that quality artifacts still capture invalid IDs.

### Code patterns
```python
# tests/test_cpg_plan_props.py
plan = build_cpg_props_plan(...)
table = PlanSpec.from_plan(plan).to_table(ctx=ctx)
assert "prop_key" in table.column_names
```

### Target files
- New: `tests/test_cpg_plan_nodes.py`
- New: `tests/test_cpg_plan_edges.py`
- New: `tests/test_cpg_plan_props.py`
- Update: `docs/guide/cpg_migration.md`

### Implementation checklist
- [ ] Add parity tests for plan vs previous outputs (where possible).
- [x] Add tests for UDF-backed JSON/expr_context transforms.
- [x] Document the plan-lane architecture and finalize boundary in CPG docs.
- [x] Update existing schema/builder tests to exercise plan-lane emitters.
