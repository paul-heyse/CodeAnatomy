## DataFusion + Ibis Programmatic Migration Plan

### Goals
- Pivot the plan lane from ArrowDSL Acero plans to Ibis expression IR (fully programmatic).
- Use SQLGlot for AST-level analysis, optimization, and lineage without handwritten SQL.
- Preserve ArrowDSL schema contracts, finalize gates, and custom kernel operations.
- Enable SQL-free authoring across relspec, normalize, extract, and CPG.
- Provide an optional DataFusion execution adapter that avoids SQL strings at runtime.

### Constraints
- Preserve output schemas, column names, and metadata semantics.
- Keep strict typing and Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Breaking changes are acceptable when they improve cohesion and correctness.

---

### Scope 1: Ibis Backend and Session Configuration
**Description**
Introduce a dedicated Ibis backend module that owns backend config, session state,
and filesystem registration. This becomes the top-level entry point for all plan execution.

**Code patterns**
```python
import ibis
from dataclasses import dataclass

@dataclass(frozen=True)
class IbisBackendConfig:
    database: str = ":memory:"
    read_only: bool = False
    extensions: tuple[str, ...] = ()
    config: dict[str, object] = None
    filesystem: object | None = None

def build_backend(cfg: IbisBackendConfig):
    con = ibis.duckdb.connect(
        database=cfg.database,
        read_only=cfg.read_only,
        extensions=list(cfg.extensions) or None,
        **(cfg.config or {}),
    )
    if cfg.filesystem is not None:
        con.register_filesystem(cfg.filesystem)
    return con
```

**Target files**
- Add: `src/ibis_engine/__init__.py`
- Add: `src/ibis_engine/config.py`
- Add: `src/ibis_engine/backend.py`
- Update: `src/hamilton_pipeline/modules/inputs.py`
- Update: `src/config.py`

**Implementation checklist**
- [ ] Define backend config dataclasses (database, extensions, config, filesystem).
- [ ] Build a shared backend factory with explicit session defaults.
- [ ] Thread backend config through pipeline entry points.
- [ ] Document backend naming and session lifetimes.

**Status**
Pending.

---

### Scope 2: Dataset Registry to Ibis Tables (No SQL Strings)
**Description**
Replace dataset scanning with Ibis table registration. Map DatasetSpec to Ibis
readers (parquet/csv/json) and materialize stable names via create_view/create_table.

**Code patterns**
```python
def register_dataset(con, name: str, location: DatasetLocation):
    table = con.read_parquet(location.path, **location.read_options)
    con.create_view(name, table, overwrite=True)
    return con.table(name)
```

**Target files**
- Add: `src/ibis_engine/registry.py`
- Update: `src/relspec/registry.py`
- Update: `src/schema_spec/system.py`

**Implementation checklist**
- [ ] Map DatasetSpec/Location to Ibis read_* options.
- [ ] Use create_view/create_table for stable names (memtable naming removed).
- [ ] Register filesystem access through the backend when needed.
- [ ] Keep dataset naming consistent with catalog conventions.

**Status**
Pending.

---

### Scope 3: Ibis Plan Abstraction
**Description**
Introduce an IbisPlan wrapper that carries ordering metadata and exposes
Arrow-native materialization helpers.

**Code patterns**
```python
from dataclasses import dataclass
import pyarrow as pa
import ibis

@dataclass(frozen=True)
class IbisPlan:
    expr: ibis.expr.types.Table
    ordering: Ordering = Ordering.unordered()

    def to_table(self) -> pa.Table:
        table = self.expr.to_pyarrow()
        return apply_ordering_metadata(table, ordering=self.ordering)
```

**Target files**
- Add: `src/ibis_engine/plan.py`
- Add: `src/ibis_engine/runner.py`
- Update: `src/relspec/compiler.py`
- Update: `src/normalize/pipeline.py`

**Implementation checklist**
- [ ] Implement an IbisPlan wrapper with ordering metadata.
- [ ] Provide Arrow materialization helpers (table and reader).
- [ ] Integrate deterministic ordering metadata with ArrowDSL finalize.

**Status**
Pending.

---

### Scope 4: ExprIR and QuerySpec to Ibis Expressions
**Description**
Replace SQL rendering with a direct ExprIR/QuerySpec to Ibis expression compiler.
This keeps all authoring programmatic and type-aware.

**Code patterns**
```python
import ibis

FN_REGISTRY: dict[str, callable] = {
    "coalesce": ibis.coalesce,
}

def expr_ir_to_ibis(expr: ExprIR, table: ibis.expr.types.Table):
    if expr.op == "field":
        return table[expr.name or ""]
    if expr.op == "literal":
        return ibis.literal(expr.value)
    if expr.op == "call":
        fn = FN_REGISTRY.get(expr.name or "")
        if fn is None:
            raise ValueError(f"Unsupported function: {expr.name}")
        args = [expr_ir_to_ibis(arg, table) for arg in expr.args]
        return fn(*args)
    raise ValueError(f"Unsupported ExprIR op: {expr.op}")
```

**Target files**
- Add: `src/ibis_engine/expr_compiler.py`
- Add: `src/ibis_engine/query_compiler.py`
- Update: `src/arrowdsl/spec/expr_ir.py`
- Update: `src/arrowdsl/plan_helpers.py`

**Implementation checklist**
- [ ] Define a function registry mapping ExprIR calls to Ibis expressions.
- [ ] Compile QuerySpec projections and predicates into Ibis operations.
- [ ] Preserve pushdown predicates when the source is scan-backed.
- [ ] Add a fallback path to Arrow kernels for unsupported functions.

**Status**
Pending.

---

### Scope 5: SQLGlot AST Pipeline for Validation and Optimization
**Description**
Use SQLGlot as the compiler IR for analysis, optimization, and lineage without
handwritten SQL. This is a policy and diagnostics layer only.

**Code patterns**
```python
from sqlglot.optimizer import optimize, qualify

sg_expr = con.compiler.to_sqlglot(expr)
sg_expr = qualify(sg_expr, schema=schema_map)
sg_expr = optimize(sg_expr, schema=schema_map)
```

**Target files**
- Add: `src/sqlglot_tools/optimizer.py`
- Add: `src/sqlglot_tools/lineage.py`
- Update: `src/ibis_engine/plan.py`

**Implementation checklist**
- [ ] Extract SQLGlot AST from Ibis expressions.
- [ ] Apply qualify/optimize with schema-derived types.
- [ ] Add AST-level diffing for regression tests and upgrades.

**Status**
Pending.

---

### Scope 6: DataFusion Adapter (SQL-Free Runtime Execution)
**Description**
Provide an optional adapter that maps Ibis (or SQLGlot AST) to DataFusion
DataFrame operations directly, avoiding SQL string generation at runtime.

**Code patterns**
```python
from datafusion import SessionContext

def ibis_to_datafusion(expr, ctx: SessionContext):
    sg_expr = con.compiler.to_sqlglot(expr)
    return df_from_sqlglot(ctx, sg_expr)
```

**Target files**
- Add: `src/ibis_engine/datafusion_adapter.py`
- Add: `src/datafusion_engine/df_builder.py`
- Update: `src/ibis_engine/runner.py`

**Implementation checklist**
- [ ] Define a SQLGlot-to-DataFusion DataFrame translator.
- [ ] Support core relational ops (select/filter/join/group/order/union).
- [ ] Keep Arrow output parity with Ibis and ArrowDSL contracts.

**Status**
Pending.

---

### Scope 7: Relspec Compiler Migration to Ibis Plans
**Description**
Replace ArrowDSL plan compilation with Ibis expression pipelines, while keeping
post-kernel hooks and contract finalization intact.

**Code patterns**
```python
left = resolver.resolve(ref_left)
right = resolver.resolve(ref_right)
joined = left.expr.join(right.expr, predicates=preds, how="inner")
expr = joined.select(columns).filter(predicate)
plan = IbisPlan(expr=expr, ordering=Ordering.unordered())
```

**Target files**
- Update: `src/relspec/compiler.py`
- Update: `src/relspec/compiler_graph.py`
- Update: `src/relspec/model.py`
- Update: `src/relspec/rules/definitions.py`

**Implementation checklist**
- [ ] Map rule kinds to Ibis expressions (filter/project/join/union/aggregate).
- [ ] Preserve rule metadata columns and ordering semantics.
- [ ] Keep post-kernel hooks and finalize contracts unchanged.

**Status**
Pending.

---

### Scope 8: Normalize, Extract, and CPG Plan Builders in Ibis
**Description**
Convert normalize/extract/CPG plan builders to Ibis expressions, preserving
dataset schemas and output invariants.

**Code patterns**
```python
defs = events.filter(events.kind == "def").select("code_unit_id", "symbol", "event_id")
uses = events.filter(events.kind == "use").select("code_unit_id", "symbol", "event_id")
joined = defs.join(uses, predicates=[defs.code_unit_id == uses.code_unit_id])
```

**Target files**
- Update: `src/normalize/plan_builders.py`
- Update: `src/normalize/bytecode_cfg_plans.py`
- Update: `src/normalize/bytecode_dfg_plans.py`
- Update: `src/normalize/types_plans.py`
- Update: `src/cpg/relationship_plans.py`
- Update: `src/extract/plan_helpers.py`

**Implementation checklist**
- [ ] Replace plan builders with Ibis expression builders.
- [ ] Keep dataset query specs and evidence projections consistent.
- [ ] Validate output schema alignment after execution.
- [ ] Preserve behavior for optional inputs (empty tables when missing).

**Status**
Pending.

---

### Scope 9: Hybrid Kernel Lane Integration
**Description**
Introduce a hybrid path where Ibis handles relational work and ArrowDSL kernels
handle custom operations (interval align, explode list, dedupe).

**Code patterns**
```python
table = ibis_plan.expr.to_pyarrow()
table = interval_align_table(table, spec=spec, ctx=ctx)
table = explode_list_column(table, parent_id_col="src_id", list_col="dst_ids")
```

**Target files**
- Add: `src/ibis_engine/hybrid.py`
- Update: `src/relspec/compiler.py`
- Update: `src/arrowdsl/compute/kernels.py`

**Implementation checklist**
- [ ] Add a hybrid executor that bridges Ibis -> Arrow tables.
- [ ] Apply kernel-lane operations after Ibis execution.
- [ ] Re-apply ordering metadata and canonical sort when required.

**Status**
Pending.

---

### Scope 10: IO and Materialization via Ibis Outputs
**Description**
Standardize dataset writes and streams on Ibis outputs, while preserving ArrowDSL
metadata sidecars and schema policies when needed.

**Code patterns**
```python
reader = expr.to_pyarrow_batches(chunk_size=16384)
path = write_dataset_parquet(reader, out_dir, config=DatasetWriteConfig())
```

**Target files**
- Add: `src/ibis_engine/io.py`
- Update: `src/storage/io.py`
- Update: `src/hamilton_pipeline/modules/outputs.py`
- Update: `src/arrowdsl/io/parquet.py`

**Implementation checklist**
- [ ] Use Ibis to produce Arrow tables/readers for persistence.
- [ ] Preserve metadata sidecar generation when required.
- [ ] Replace direct Parquet writes where possible.

**Status**
Pending.

---

### Scope 11: Parameterization and Safe Execution (Ibis Params)
**Description**
Adopt Ibis parameters for safe, programmatic execution without SQL string
interpolation.

**Code patterns**
```python
threshold = ibis.param("int64")
expr = table.filter(table.score >= threshold)
result = con.execute(expr, params={threshold: 10})
```

**Target files**
- Add: `src/ibis_engine/params.py`
- Update: `src/relspec/compiler.py`
- Update: `src/normalize/pipeline.py`

**Implementation checklist**
- [ ] Define parameter registries for scalar and list inputs.
- [ ] Use memtable + join patterns for list parameters.
- [ ] Keep parameter binding out of SQL strings entirely.

**Status**
Pending.

---

### Scope 12: Decommission ArrowDSL Plan Lane
**Description**
Remove or freeze ArrowDSL plan-lane modules and migrate call sites to the Ibis
plan layer, keeping kernel-lane and schema systems intact.

**Code patterns**
```python
# Old: Plan + Acero
# plan = PlanFactory(ctx=ctx).scan(dataset, columns=cols)

# New: Ibis expression plan
plan = IbisPlan(expr=expr, ordering=Ordering.unordered())
```

**Target files**
- Remove/retire: `src/arrowdsl/plan/plan.py`
- Remove/retire: `src/arrowdsl/plan/ops.py`
- Remove/retire: `src/arrowdsl/plan/query.py`
- Remove/retire: `src/arrowdsl/plan/runner.py`
- Update: `src/arrowdsl/plan_helpers.py`

**Implementation checklist**
- [ ] Migrate plan-lane call sites to Ibis plan APIs.
- [ ] Remove Acero plan modules and unused helpers.
- [ ] Keep kernel-lane and schema modules as the contract layer.

**Status**
Pending.
