## DataFusion + Ibis Programmatic Migration Plan (Aligned with Relspec Optimization)

### Goals
- Pivot the plan lane from ArrowDSL Acero plans to a programmatic Ibis expression IR.
- Keep relspec rule execution engine-agnostic via typed relational ops and RelPlan compilation.
- Use SQLGlot for AST-level analysis, optimization, and lineage without handwritten SQL.
- Preserve ArrowDSL schema contracts, finalize gates, and custom kernel operations.
- Provide an optional DataFusion execution adapter that avoids SQL strings at runtime.

### Constraints
- Preserve output schemas, column names, and metadata semantics.
- Keep strict typing and Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Avoid SQL string authoring; prefer Ibis/SQLGlot and DataFusion APIs.
- Breaking changes are acceptable when they improve cohesion and correctness.

---

### Scope 1: Ibis Backend and Session Configuration
**Description**
Introduce a dedicated Ibis backend module that owns backend config, session state,
and filesystem registration. This becomes the top-level entry point for all plan execution.

**Code patterns**
```python
from dataclasses import dataclass, field
import ibis

@dataclass(frozen=True)
class IbisBackendConfig:
    database: str = ":memory:"
    read_only: bool = False
    extensions: tuple[str, ...] = ()
    config: dict[str, object] = field(default_factory=dict)
    filesystem: object | None = None

def build_backend(cfg: IbisBackendConfig):
    con = ibis.duckdb.connect(
        database=cfg.database,
        read_only=cfg.read_only,
        extensions=list(cfg.extensions) or None,
        **cfg.config,
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
- [x] Define backend config dataclasses (database, extensions, config, filesystem).
- [x] Build a shared backend factory with explicit session defaults.
- [x] Thread backend config through pipeline entry points.
- [x] Document backend naming and session lifetimes.

**Status**
Implemented.

---

### Scope 2: Dataset Registry to Ibis Tables + Scan Options
**Description**
Replace dataset scanning with Ibis table registration. Map DatasetSpec to Ibis
readers (parquet/csv/json) and materialize stable names via create_view/create_table.
Add DataFusion scan options to DatasetSpec/registry so partitioning and pruning
can be honored by engine adapters.

**Code patterns**
```python
from dataclasses import dataclass, field
from typing import Mapping

@dataclass(frozen=True)
class DataFusionScanOptions:
    partition_cols: tuple[tuple[str, str], ...] = ()
    file_sort_order: tuple[str, ...] = ()
    parquet_pruning: bool = True
    skip_metadata: bool = False

@dataclass(frozen=True)
class DatasetLocation:
    path: str
    format: str = "parquet"
    partitioning: str | None = "hive"
    read_options: Mapping[str, object] = field(default_factory=dict)
    filesystem: object | None = None
    scan_options: DataFusionScanOptions | None = None
```

**Target files**
- Add: `src/ibis_engine/registry.py`
- Update: `src/schema_spec/system.py`
- Update: `src/relspec/registry.py`
- Update: `src/datafusion_engine/df_builder.py`

**Implementation checklist**
- [x] Map DatasetLocation to Ibis read_* options and stable view registration.
- [x] Register filesystem access through the backend when needed.
- [x] Preserve partitioning hints for Ibis parquet scans (hive support).
- [ ] Add DataFusionScanOptions to DatasetSpec/registry entries.
- [ ] Thread scan options into DataFusion table registration and scans.
- [ ] Document precedence rules for scan options vs schema defaults.

**Status**
In progress.

---

### Scope 3: Ibis Plan Abstraction
**Description**
Introduce an IbisPlan wrapper that carries ordering metadata and exposes
Arrow-native materialization helpers.

**Code patterns**
```python
from dataclasses import dataclass, field
from ibis.expr.types import Table

@dataclass(frozen=True)
class IbisPlan:
    expr: Table
    ordering: Ordering = field(default_factory=Ordering.unordered)

    def to_table(self) -> TableLike:
        table = self.expr.to_pyarrow()
        metadata = ordering_metadata_spec(self.ordering.level, keys=self.ordering.keys)
        return table.cast(metadata.apply(table.schema))
```

**Target files**
- Add: `src/ibis_engine/plan.py`
- Add: `src/ibis_engine/runner.py`

**Implementation checklist**
- [x] Implement an IbisPlan wrapper with ordering metadata.
- [x] Provide Arrow materialization helpers (table and reader).
- [x] Integrate deterministic ordering metadata with ArrowDSL finalize.

**Status**
Implemented.

---

### Scope 4: ExprIR and Ibis QuerySpec Compiler
**Description**
Replace SQL rendering with a direct ExprIR/QuerySpec to Ibis expression compiler.
This keeps all authoring programmatic and type-aware.

**Code patterns**
```python
import ibis

class IbisQuerySpec:
    projection: IbisProjectionSpec
    predicate: ExprIR | None
    pushdown_predicate: ExprIR | None

expr = expr_ir_to_ibis(expr_ir, table)
```

**Target files**
- Add: `src/ibis_engine/expr_compiler.py`
- Add: `src/ibis_engine/query_compiler.py`
- Update: `src/arrowdsl/spec/expr_ir.py`
- Update: `src/arrowdsl/plan_helpers.py`

**Implementation checklist**
- [x] Define a function registry mapping ExprIR calls to Ibis expressions.
- [x] Compile QuerySpec projections and predicates into Ibis operations.
- [x] Preserve pushdown predicates when the source is scan-backed.
- [ ] Route unsupported ops to kernel-lane fallbacks (see Hybrid Kernel scope).

**Status**
Implemented (core compiler), kernel fallback pending.

---

### Scope 5: SQLGlot Tooling + Relspec Diagnostics Layer
**Description**
Use SQLGlot as the compiler IR for analysis, optimization, and lineage without
handwritten SQL. Add a relspec diagnostics layer that compiles rule plans to
SQLGlot for validation and migration safety.

**Code patterns**
```python
sg_expr = con.compiler.to_sqlglot(expr)
sg_expr = qualify(sg_expr, schema=schema_map)
sg_expr = optimize(sg_expr, schema=schema_map)
```

**Target files**
- Add: `src/sqlglot_tools/optimizer.py`
- Add: `src/sqlglot_tools/lineage.py`
- Add: `src/relspec/sqlglot_diagnostics.py`
- Update: `src/relspec/rules/diagnostics.py`
- Update: `src/relspec/rules/validation.py`

**Implementation checklist**
- [x] Extract SQLGlot AST from Ibis expressions.
- [x] Apply qualify/optimize with schema-derived types.
- [ ] Add relspec diagnostics: lineage extraction, semantic diffing, validation gates.
- [ ] Store optimized ASTs in diagnostics artifacts for migrations.

**Status**
In progress.

---

### Scope 6: DataFusion Adapter (SQL-Free Runtime Execution)
**Description**
Provide an optional adapter that maps Ibis (or SQLGlot AST) to DataFusion
DataFrame operations directly, avoiding SQL string generation at runtime.

**Code patterns**
```python
from datafusion import SessionContext

def ibis_to_datafusion(expr, *, backend, ctx: SessionContext):
    sg_expr = backend.compiler.to_sqlglot(expr)
    return df_from_sqlglot(ctx, sg_expr)
```

**Target files**
- Add: `src/ibis_engine/datafusion_adapter.py`
- Add: `src/datafusion_engine/df_builder.py`
- Update: `src/ibis_engine/runner.py`

**Implementation checklist**
- [x] Define a SQLGlot-to-DataFusion DataFrame translator.
- [x] Support core relational ops (select/filter/join/group/order/union).
- [x] Keep Arrow output parity with Ibis and ArrowDSL contracts.

**Status**
Implemented.

---

### Scope 7: Typed Relational Op IR for Rule Definitions
**Description**
Replace untyped `pipeline_ops` mappings with structured relational operations so rule
execution is programmatically defined and portable across engines.

**Code patterns**
```python
from dataclasses import dataclass
from typing import Literal

RelOpKind = Literal["scan", "filter", "project", "join", "aggregate", "union", "param"]

@dataclass(frozen=True)
class RelOp:
    kind: RelOpKind

@dataclass(frozen=True)
class FilterOp(RelOp):
    predicate: ExprIR
```

**Target files**
- Add: `src/relspec/rules/rel_ops.py`
- Update: `src/relspec/rules/definitions.py`
- Update: `src/relspec/model.py`
- Update: `src/relspec/compiler.py`

**Implementation checklist**
- [x] Define typed op dataclasses for scan/filter/project/join/aggregate/union/param.
- [x] Replace `pipeline_ops` with `rel_ops: tuple[RelOp, ...]` on RuleDefinition.
- [x] Add validation to ensure op sequences are well-formed (scan-first, etc.).
- [ ] Update rule authorship to emit structured ops everywhere.

**Status**
Implemented (core IR), rule authorship rollout pending.

---

### Scope 8: Engine-Agnostic RelPlan + Compiler Interface
**Description**
Introduce a RelPlan container plus compiler interface that emits engine-specific
plans (Ibis/DataFusion) while preserving schemas and ordering metadata.

**Code patterns**
```python
from dataclasses import dataclass
from arrowdsl.core.context import Ordering

@dataclass(frozen=True)
class RelPlan:
    root: RelNode
    schema: SchemaLike | None
    ordering: Ordering

class RelPlanCompiler(Protocol):
    def compile(self, plan: RelPlan, *, ctx: ExecutionContext) -> object: ...
```

**Target files**
- Add: `src/relspec/plan.py`
- Add: `src/relspec/engine.py`
- Update: `src/relspec/compiler.py`
- Update: `src/relspec/compiler_graph.py`

**Implementation checklist**
- [x] Define RelPlan with schema + ordering metadata.
- [x] Introduce RelPlanCompiler protocol and Ibis adapter.
- [ ] Convert relspec rule compilation to produce RelPlan first.
- [ ] Defer execution to engine adapters (Ibis/DataFusion).

**Status**
In progress.

---

### Scope 9: Rule Graph Canonicalization and Plan Hashing
**Description**
Generate a stable logical plan hash and canonical graph signature so rules can be
cached, diffed, and validated independently of the execution engine.

**Code patterns**
```python
def plan_signature(plan: RelPlan) -> str:
    payload = _rel_plan_payload(plan)
    encoded = json.dumps(payload, sort_keys=True, separators=(\",\", \":\"))
    return hashlib.sha256(encoded.encode(\"utf-8\")).hexdigest()
```

**Target files**
- Update: `src/relspec/rules/graph.py`
- Update: `src/relspec/rules/cache.py`
- Update: `src/relspec/rules/diagnostics.py`

**Implementation checklist**
- [x] Define canonical serialization helpers for RelPlan payloads.
- [ ] Store plan hash in rule diagnostics and cache entries.
- [ ] Emit graph-level signatures for rule bundles.
- [ ] Expose plan signatures for migration/regression tooling.

**Status**
Pending (signature wiring not yet integrated).

---

### Scope 10: DataFusion Runtime Profile
**Description**
Introduce a runtime profile for DataFusion execution controls (partitions, memory
pool, spill paths, batch sizes) and integrate it into relspec execution paths.

**Code patterns**
```python
from dataclasses import dataclass
from typing import Literal

@dataclass(frozen=True)
class DataFusionRuntimeProfile:
    target_partitions: int = 8
    batch_size: int = 8192
    spill_dir: str | None = None
    memory_pool: Literal["greedy", "fair"] = "greedy"
```

**Target files**
- Add: `src/datafusion_engine/runtime.py`
- Update: `src/arrowdsl/core/context.py`
- Update: `src/relspec/compiler.py`

**Implementation checklist**
- [ ] Define a DataFusion runtime profile dataclass.
- [ ] Add profile hooks to relspec execution context.
- [ ] Configure DataFusion RuntimeEnv/SessionContext from the profile.
- [ ] Store runtime settings in telemetry for diagnostics.

**Status**
Pending.

---

### Scope 11: Hybrid Kernel Bridge (DataFusion UDF/UDTF + Arrow Fallback)
**Description**
Create a bridge that maps kernel ops to DataFusion UDF/UDTF when available, while
preserving ArrowDSL kernel fallback for unsupported operations.

**Code patterns**
```python
from arrowdsl.compute.kernels import interval_align_table, explode_list_column

def apply_kernel_bridge(table: pa.Table) -> pa.Table:
    table = interval_align_table(table, cfg=cfg)
    table = explode_list_column(table, parent_id_col="src_id", list_col="dst_ids")
    return table
```

**Target files**
- Add: `src/datafusion_engine/kernels.py`
- Add: `src/ibis_engine/hybrid.py`
- Update: `src/relspec/compiler.py`

**Implementation checklist**
- [x] Add Arrow kernel bridge helpers for Ibis -> Arrow tables.
- [ ] Define kernel capability registry (DF UDF/UDTF vs Arrow fallback).
- [ ] Add DataFusion kernel adapters for interval align/explode/dedupe.
- [ ] Ensure ordering metadata and schema contracts are preserved post-kernel.

**Status**
In progress.

---

### Scope 12: Normalize, Extract, and CPG Plan Builders in Ibis
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

### Scope 13: IO and Materialization via Ibis Outputs
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

### Scope 14: Parameterization and Safe Execution (Ibis Params)
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

### Scope 15: Decommission ArrowDSL Plan Lane
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
