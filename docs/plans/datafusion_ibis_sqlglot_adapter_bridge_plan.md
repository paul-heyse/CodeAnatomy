## DataFusion/Ibis/SQLGlot Adapter Bridge Plan

### Goals
- Build adapter layers that translate existing ArrowDSL plan builders, IO, and materialization into the target Ibis/DataFusion/SQLGlot design without a wholesale rewrite.
- Preserve schema contracts, ordering metadata, and kernel-lane invariants while enabling new engine adapters.
- Keep legacy plan-lane modules operational until adapter coverage is complete, then retire them in a controlled step.

### Constraints
- Avoid SQL string authoring; use Ibis expressions and SQLGlot ASTs.
- Preserve output schemas, column names, and metadata semantics.
- Keep strict typing and Ruff compliance (no suppressions).
- No new tests in this phase; rely on diagnostics artifacts and plan inspection.
- Adapters should live in new bridge modules to minimize churn in legacy code paths.

### Adapter Matrix (Legacy -> Adapter -> Target State)
| Legacy surface | Adapter surface | Target-state module |
| --- | --- | --- |
| `arrowdsl.plan.Plan` / `PlanSpec` | `plan_to_ibis` bridge | `ibis_engine.plan`, `ibis_engine.runner` |
| `arrowdsl.plan.query.QuerySpec` | `queryspec_to_ibis` bridge | `ibis_engine.query_compiler` |
| Normalize/extract/cpg plan builders | Builder bridge wrappers | `normalize/*`, `extract/*`, `cpg/*` |
| `arrowdsl.io.parquet` writers | Ibis IO bridge | `ibis_engine/io_bridge`, `storage/io` |
| Plan/Query diagnostics | SQLGlot bridge | `sqlglot_tools/*` |
| DataFusion execution | Ibis/SQLGlot -> DF bridge | `datafusion_engine/*` |
| Dataset registry + scan config | Registry bridge | `ibis_engine/registry`, `datafusion_engine/df_builder` |
| Parameters + kernel-lane ops | Param + kernel bridge | `ibis_engine/params_bridge`, `datafusion_engine/kernels` |
| Legacy retirement | Decommission gates | `config.py`, `arrowdsl/*` |

---

### Scope 1: Plan -> IbisPlan Adapter (PlanBridge)
**Description**
Wrap ArrowDSL Plan/PlanSpec/PlanSource results into Ibis expressions by materializing
to Arrow and registering an Ibis view. Ibis v11 removes memtable naming, so use
`create_view`/`create_table` for stable identifiers.

**Code patterns**
```python
def plan_to_ibis(
    plan: Plan,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
    name: str,
) -> IbisPlan:
    table = plan.to_table(ctx=ctx)
    backend.create_view(name, ibis.memtable(table), overwrite=True)
    return IbisPlan(expr=backend.table(name), ordering=plan.ordering)
```

**Target files**
- Add: `src/ibis_engine/plan_bridge.py`
- Update: `src/normalize/runner.py`
- Update: `src/extract/plan_helpers.py`
- Update: `src/cpg/relationship_plans.py`

**Implementation checklist**
- [x] Add `plan_to_ibis`, `source_to_ibis`, `reader_to_ibis` helpers.
- [x] Preserve `Plan.ordering` metadata in the returned `IbisPlan`.
- [x] Provide opt-in hooks so legacy plan builders can be wrapped without rewriting.

**Status**
Implemented.

---

### Scope 2: QuerySpec + ExprSpec Adapter (QueryBridge)
**Description**
Translate existing `QuerySpec`/`ProjectionSpec`/`ExprSpec` into `IbisQuerySpec`
using `ExprIR` and the Ibis expression compiler. This allows existing plan builders
to remain unchanged while generating Ibis-native filters/projections.

**Code patterns**
```python
def queryspec_to_ibis(spec: QuerySpec) -> IbisQuerySpec:
    derived = {name: expr_spec_to_ir(expr) for name, expr in spec.projection.derived.items()}
    return IbisQuerySpec(
        projection=IbisProjectionSpec(base=spec.projection.base, derived=derived),
        predicate=expr_spec_to_ir(spec.predicate) if spec.predicate else None,
        pushdown_predicate=expr_spec_to_ir(spec.pushdown_predicate) if spec.pushdown_predicate else None,
    )
```

**Target files**
- Add: `src/ibis_engine/query_bridge.py`
- Update: `src/arrowdsl/plan_helpers.py`
- Update: `src/extract/plan_helpers.py`

**Implementation checklist**
- [x] Implement `expr_spec_to_ir` for the core ExprSpec family.
- [x] Provide fallback to kernel-lane materialization when ExprSpec cannot compile.
- [x] Keep pushdown predicates separate from post-filter predicates.

**Status**
Implemented (core ExprSpec coverage; extend as new ExprSpec variants appear).

---

### Scope 3: Builder Bridge Wrappers (Normalize/Extract/CPG)
**Description**
Introduce adapter wrappers that call legacy plan builders and immediately
convert their outputs to `IbisPlan` using the PlanBridge. This allows the pipeline
to adopt Ibis execution while reusing existing plan logic.

**Code patterns**
```python
def cfg_blocks_ibis(
    blocks: PlanSource,
    code_units: PlanSource,
    *,
    ctx: ExecutionContext,
    backend: BaseBackend,
) -> IbisPlan:
    plan = cfg_blocks_plan(blocks, code_units, ctx=ctx)
    return plan_to_ibis(plan, ctx=ctx, backend=backend, name="cfg_blocks_norm")
```

**Target files**
- Add: `src/normalize/ibis_bridge.py`
- Add: `src/extract/ibis_bridge.py`
- Add: `src/cpg/ibis_bridge.py`
- Update: `src/normalize/plan_builders.py`
- Update: `src/extract/plan_helpers.py`
- Update: `src/cpg/relationship_plans.py`

**Implementation checklist**
- [x] Add Ibis bridge wrappers for each existing plan builder output.
- [ ] Route pipeline entry points through the bridge when the Ibis mode is enabled (CPG entry points still pending).
- [x] Keep legacy builders unchanged so they remain the source of truth.

**Status**
Partially implemented (wrappers added; CPG entry-point routing still pending).

---

### Scope 4: IO + Materialization Bridge (Ibis Outputs)
**Description**
Add Ibis IO adapters that convert `IbisPlan`/Ibis tables to Arrow readers and
reuse the existing Parquet sidecar + schema policy helpers.

**Code patterns**
```python
def write_ibis_dataset_parquet(
    plan: IbisPlan,
    *,
    base_dir: str,
    config: DatasetWriteConfig | None = None,
) -> str:
    reader = plan.to_reader()
    return write_dataset_parquet(reader, base_dir, config=config)
```

**Target files**
- Add: `src/ibis_engine/io_bridge.py`
- Update: `src/storage/io.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`

**Implementation checklist**
- [x] Support table + reader materialization from Ibis expressions.
- [x] Preserve metadata sidecars and schema/encoding policies.
- [x] Provide a consistent `write_*` surface that accepts `IbisPlan`.

**Status**
Implemented.

---

### Scope 5: SQLGlot Diagnostics Bridge
**Description**
Provide adapter helpers that compile Ibis expressions to SQLGlot ASTs, then
apply SQLGlot optimizer, lineage extraction, and semantic diffing. This yields
engine-agnostic diagnostics artifacts without SQL strings.

**Code patterns**
```python
def ibis_to_sqlglot(expr: Table, *, backend: BaseBackend) -> Expression:
    return backend.compiler.to_sqlglot(expr)
```

**Target files**
- Add: `src/sqlglot_tools/bridge.py`
- Update: `src/relspec/rules/diagnostics.py`
- Update: `src/relspec/rules/validation.py`

**Implementation checklist**
- [x] Compile Ibis expressions into SQLGlot ASTs consistently.
- [x] Run SQLGlot optimize + lineage extraction on compiled ASTs.
- [x] Emit diff artifacts for rule migration diagnostics.

**Status**
Implemented.

---

### Scope 6: DataFusion Execution Bridge
**Description**
Provide a bridge that takes Ibis expressions or SQLGlot ASTs and produces
DataFusion DataFrames without SQL strings. This uses DataFusion’s SessionContext
and the SQLGlot -> DataFusion translator.

**Code patterns**
```python
def ibis_plan_to_df(
    plan: IbisPlan,
    *,
    backend: IbisCompilerBackend,
    ctx: SessionContext,
) -> DataFrame:
    return ibis_to_datafusion(plan.expr, backend=backend, ctx=ctx)
```

**Target files**
- Add: `src/datafusion_engine/bridge.py`
- Update: `src/ibis_engine/runner.py`
- Update: `src/datafusion_engine/df_builder.py`

**Implementation checklist**
- [x] Accept Ibis expressions and SQLGlot ASTs as inputs.
- [x] Register datasets via DataFusion SessionContext as needed.
- [ ] Preserve output schemas and ordering metadata (helpers added; wiring into execution path still pending).

**Status**
Partially implemented (ordering metadata helpers added; not yet wired into execution path).

---

### Scope 7: Dataset Registry + Scan Adapter
**Description**
Bridge existing dataset specs and locations into Ibis/DataFusion registration,
including listing tables (Hive partitioning), pruning, and file sort order.
This lets current dataset specs remain canonical while feeding engine adapters.

**Code patterns**
```python
def register_dataset_df(
    ctx: SessionContext,
    *,
    name: str,
    location: DatasetLocation,
) -> DataFrame:
    return register_dataset(ctx, name=name, location=location)
```

**Target files**
- Add: `src/datafusion_engine/registry_bridge.py`
- Update: `src/ibis_engine/registry.py`
- Update: `src/datafusion_engine/df_builder.py`

**Implementation checklist**
- [x] Convert dataset spec hints to DataFusion listing table options.
- [x] Support parquet/csv/json/avro registration without SQL.
- [x] Keep precedence rules for dataset hints vs read options explicit.

**Status**
Implemented.

---

### Scope 8: Parameter + Kernel Bridge
**Description**
Introduce parameter adapters for Ibis and DataFusion that map existing relspec
param ops to Ibis parameters and DataFusion parameter bindings. Add kernel bridges
that register DataFusion UDF/UDTF equivalents when available and fall back to
Arrow kernel-lane materialization when not.

**Code patterns**
```python
threshold = ibis.param("int64")
expr = table.filter(table.score >= threshold)
result = backend.execute(expr, params={threshold: 10})
```

**Target files**
- Add: `src/ibis_engine/params_bridge.py`
- Add: `src/datafusion_engine/kernels.py`
- Update: `src/ibis_engine/hybrid.py`

**Implementation checklist**
- [x] Define param registries for scalar and list parameters.
- [x] Add kernel capability registry (DF UDF/UDTF vs Arrow fallback).
- [x] Ensure ordering metadata and contracts survive kernel application.
- [ ] Wire param bindings through execution entry points.

**Status**
Partially implemented (registry supports scalar/list params; execution wiring still pending).

---

### Scope 9: Legacy Decommission Gates
**Description**
Define explicit feature flags and deprecation checkpoints for retiring ArrowDSL
plan-lane modules once adapters provide parity with target-state behavior.

**Code patterns**
```python
@dataclass(frozen=True)
class AdapterMode:
    use_ibis_bridge: bool = True
    use_datafusion_bridge: bool = False
```

**Target files**
- Update: `src/config.py`
- Update: `src/arrowdsl/plan_helpers.py`
- Update: `src/arrowdsl/plan/runner.py`

**Implementation checklist**
- [x] Introduce adapter mode flags in configuration.
- [ ] Wire pipeline entry points to select adapter vs legacy path (CPG entry points still pending).
- [x] Define a removal checklist for ArrowDSL plan lane.

**Status**
Partially implemented (adapter mode exists; CPG entry-point routing still pending).
