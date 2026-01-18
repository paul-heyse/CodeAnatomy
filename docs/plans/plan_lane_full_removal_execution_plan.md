# Plan-Lane Full Removal Execution Plan (Ibis-Only)

## Purpose
Retire all remaining ArrowDSL plan-lane fallback surfaces in CPG + Normalize and converge on
Ibis-first execution, aligned with `docs/plans/compute_architecture_integrated_implementation_plan.md`.

## Goals
- Remove plan-lane compilation, builders, and wrappers in CPG and Normalize.
- Standardize all CPG/Normalize execution on Ibis plans + Ibis execution contexts.
- Update extractor registry references and public exports to the Ibis-first API.

## Non-goals
- Removing the ArrowDSL plan system globally (other subsystems may still rely on it).
- Changing dataset schemas or rule semantics beyond necessary Ibis alignment.

---

## Scope 1 - CPG emission: Ibis-only handlers and builders
Remove plan-lane emission modules and handlers; make CPG edges/nodes/props Ibis-only.

**Code pattern**
```python
@dataclass(frozen=True)
class EdgeEmitRuleHandler:
    def compile(self, plan: IbisPlan, *, spec: EdgePlanSpec, include_keys: bool) -> IbisPlan:
        return emit_edges_ibis(plan, spec=spec.emit, include_keys=include_keys)
```

**Target files**
- src/relspec/rules/handlers/cpg_emit.py
- src/cpg/emit_edges.py
- src/cpg/emit_nodes.py
- src/cpg/emit_props.py
- src/relspec/cpg/build_edges.py
- src/relspec/cpg/build_nodes.py
- src/relspec/cpg/build_props.py
- src/cpg/specs.py
- src/relspec/cpg/emit_nodes_ibis.py
- src/relspec/cpg/emit_props_ibis.py

**Implementation checklist**
- [ ] Delete plan-lane emit modules and remove plan compile methods from handlers.
- [ ] Make `build_cpg_edges_raw/build_cpg_nodes_raw/build_cpg_props_raw` return `IbisPlan` only.
- [ ] Remove `Plan` types, `union_all_plans`, and plan-lane branches from CPG builders.
- [ ] Drop `PlanPreprocessor` and plan preprocessors from `cpg.specs` (Ibis does not support them).

---

## Scope 2 - CPG relationship compilation: Ibis-only
Collapse CPG relationship compilation to Ibis plans only, removing plan-lane compilation and
plan catalog resolution.

**Code pattern**
```python
@dataclass(frozen=True)
class RelationPlanBundle:
    plans: Mapping[str, IbisPlan]
    telemetry: Mapping[str, ScanTelemetry] = field(default_factory=dict)
    coverage: Mapping[str, IbisPlan] = field(default_factory=dict)

def compile_relation_plans(
    catalog: IbisPlanCatalog,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    options: RelationPlanCompileOptions | None = None,
) -> RelationPlanBundle:
    context = _ibis_relation_context(catalog, ctx=ctx, backend=backend, options=options)
    plans, coverage = _compile_relation_plans_ibis(context, backend=backend, name_prefix="cpg_rel")
    return RelationPlanBundle(plans=plans, telemetry=context.telemetry, coverage=coverage)
```

**Target files**
- src/cpg/relationship_plans.py
- src/cpg/catalog.py
- src/relspec/cpg/build_edges.py

**Implementation checklist**
- [ ] Remove `compile_relation_plans` (plan-lane) and make Ibis path canonical.
- [ ] Replace `PlanCatalog` resolution with `IbisPlanCatalog` only.
- [ ] Update `EdgePlanBundle` to carry `IbisPlan` only.
- [ ] Delete or refactor any `PlanSource`-based resolvers that do not feed Ibis.

---

## Scope 3 - CPG table utilities: remove plan_specs dependency
Extract table alignment and schema helpers to non-plan utilities and remove `cpg.plan_specs`
usage in Ibis-only paths.

**Code pattern**
```python
def align_table_to_schema(
    table: TableLike,
    *,
    schema: SchemaLike,
    safe_cast: bool = True,
    keep_extra_columns: bool = False,
) -> TableLike:
    aligned, _ = align_to_schema(
        table,
        schema=schema,
        safe_cast=safe_cast,
        keep_extra_columns=keep_extra_columns,
    )
    return aligned
```

**Target files**
- src/cpg/plan_specs.py
- src/cpg/merge.py
- src/cpg/catalog.py
- src/relspec/cpg/build_edges.py
- src/relspec/cpg/build_nodes.py
- src/relspec/cpg/build_props.py

**Implementation checklist**
- [ ] Move table-only helpers to a new `cpg/table_utils.py` (or import directly from ArrowDSL).
- [ ] Remove plan-only helpers (`Plan`, `QuerySpec`, `plan_from_dataset`) from CPG codepaths.
- [ ] Update all imports to use Ibis/table alignment helpers (no plan-lane symbols).

---

## Scope 4 - Normalize rule model + handler: Ibis-first
Convert normalize rules to carry Ibis builder names and Ibis query specs only.

**Code pattern**
```python
@dataclass(frozen=True)
class NormalizeRule:
    name: str
    output: str
    inputs: tuple[str, ...] = ()
    ibis_builder: str | None = None
    query: IbisQuerySpec | None = None
```

**Target files**
- src/normalize/rule_model.py
- src/relspec/rules/handlers/normalize.py
- src/normalize/rule_factories.py

**Implementation checklist**
- [ ] Replace `PlanDeriver` with `ibis_builder: str | None`.
- [ ] Store and apply `IbisQuerySpec` directly (no `QuerySpec` conversion).
- [ ] Update `NormalizeRuleHandler` to map payloads to Ibis builder names.

---

## Scope 5 - Normalize Ibis compilation + execution
Remove plan-lane compilation and implement Ibis-only compilation and materialization paths,
including query application and finalize behavior.

**Code pattern**
```python
def compile_normalize_plans_ibis(
    catalog: IbisPlanCatalog,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    options: NormalizeIbisPlanOptions,
) -> dict[str, IbisPlan]:
    for rule in ordered:
        plan = resolve_rule_plan_ibis(rule, catalog, ctx=ctx, backend=backend)
        if rule.query is not None:
            expr = apply_query_spec(plan.expr, spec=rule.query)
            plan = IbisPlan(expr=expr, ordering=plan.ordering)
        plans[rule.output] = plan
```

**Target files**
- src/normalize/runner.py
- src/normalize/ibis_plan_builders.py
- src/normalize/utils.py
- src/normalize/catalog.py
- src/normalize/registry_plans.py
- src/normalize/pipeline.py
- src/normalize/materializers.py
- src/normalize/ibis_bridge.py

**Implementation checklist**
- [x] Remove plan-lane compile functions (`compile_normalize_rules`, `compile_normalize_graph_plan`).
- [x] Apply rule queries in Ibis paths using `apply_query_spec`.
- [x] Replace `PlanSource`/`plan_from_source` in `IbisPlanCatalog` with Ibis/TableLike inputs.
- [x] Make `run_normalize` Ibis-only for materialize + finalize execution.

---

## Scope 6 - Normalize API surface: Ibis-only wrappers
Replace plan-lane modules (`bytecode_cfg`, `bytecode_dfg`, `types`, `diagnostics`, `spans`,
`bytecode_anchor`) with Ibis-first wrappers or move their public APIs to a new Ibis module.

**Code pattern**
```python
def build_cfg_edges(
    py_bc_code_units: TableLike,
    py_bc_cfg_edges: TableLike,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
) -> TableLike:
    catalog = IbisPlanCatalog(backend=backend, tables={
        "py_bc_code_units": py_bc_code_units,
        "py_bc_cfg_edges": py_bc_cfg_edges,
    })
    plan = cfg_edges_plan_ibis(catalog, ctx, backend)
    return materialize_ibis_plan(plan, execution=IbisExecutionContext(ctx=ctx, ibis_backend=backend))
```

**Target files**
- src/normalize/bytecode_cfg.py
- src/normalize/bytecode_dfg.py
- src/normalize/types.py
- src/normalize/diagnostics.py
- src/normalize/spans.py
- src/normalize/bytecode_anchor.py
- src/normalize/diagnostics_plans.py
- src/normalize/bytecode_cfg_plans.py
- src/normalize/bytecode_dfg_plans.py
- src/normalize/types_plans.py
- src/normalize/ibis_spans.py
- src/normalize/ibis_plan_builders.py

**Implementation checklist**
- [x] Remove plan-lane wrappers or reimplement them as Ibis-only entrypoints.
- [x] Route span normalization to `normalize.ibis_spans` functions.
- [x] Ensure all normalize output APIs accept `BaseBackend` and return `TableLike`.

---

## Scope 7 - Extractor registry updates
Update derivation extractor references to Ibis-first normalize APIs.

**Code pattern**
```python
DerivationRow(
    kind=NodeKind.CFG_BLOCK,
    extractor="normalize.ibis_api:build_cfg_blocks",
    status="planned",
)
```

**Target files**
- src/cpg/kinds_ultimate.py
- src/cpg/contract_registry.py

**Implementation checklist**
- [x] Replace `normalize.bytecode_cfg:*`, `normalize.bytecode_dfg:*`, `normalize.types:*`,
  `normalize.diagnostics:*`, `normalize.spans:*` with Ibis API references.
- [ ] Ensure `validate_derivation_extractors` passes with new callables.

---

## Scope 8 - Pipeline wiring, exports, and tests
Rewire Hamilton pipeline normalization to Ibis-only execution, trim exports, and update legacy
surface tests.

**Code pattern**
```python
plans = compile_normalize_plans_ibis(
    catalog,
    ctx=ctx,
    options=NormalizeIbisPlanOptions(backend=backend, materialize_outputs=outputs),
)
table = materialize_ibis_plan(plans[output], execution=execution)
finalize = dataset_spec(output).finalize_context(ctx).run(table, ctx=ctx)
```

**Target files**
- src/hamilton_pipeline/modules/normalization.py
- src/normalize/__init__.py
- tests/test_legacy_surface.py
- docs/guide/cpg_migration.md
- README.md

**Implementation checklist**
- [x] Remove plan-lane `NormalizePlanCatalog` usage in Hamilton normalization.
- [x] Export only Ibis-first normalize APIs from `normalize.__init__`.
- [x] Expand legacy-surface denylist to cover removed plan-lane modules if needed.

---

## Validation and Quality Gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.13`

## Exit Criteria
- No imports of plan-lane normalize/CPG modules in `src/`.
- All CPG + Normalize entrypoints accept Ibis inputs and execute via Ibis.
- Extractor registry validates without planned/legacy plan-lane references.
