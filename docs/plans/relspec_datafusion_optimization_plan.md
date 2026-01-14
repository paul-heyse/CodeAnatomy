## Relspec DataFusion Optimization Plan

### Goals
- Make relspec rule execution engine-agnostic while keeping ArrowDSL kernel lane intact.
- Shift relspec to a typed, programmatic relational-op IR that compiles to Ibis/DataFusion.
- Prepare relspec datasets and scans for DataFusion-native IO/partitioning controls.
- Add SQLGlot-based diagnostics, lineage, and plan diffing for rule migrations.
- Keep schema contracts and ordering metadata as first-class, engine-neutral artifacts.

### Constraints
- Preserve output schemas, column names, and metadata semantics.
- Keep strict typing and Ruff compliance (no suppressions).
- Avoid SQL string authoring; prefer Ibis/SQLGlot and DataFusion APIs.
- Breaking changes are acceptable when they improve cohesion and correctness.

---

### Scope 1: Typed Relational Op IR for Rule Definitions
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

@dataclass(frozen=True)
class ParamRef:
    name: str
    dtype: str
```

**Target files**
- Add: `src/relspec/rules/rel_ops.py`
- Update: `src/relspec/rules/definitions.py`
- Update: `src/relspec/model.py`
- Update: `src/relspec/compiler.py`

**Implementation checklist**
- [x] Define typed op dataclasses for scan/filter/project/join/aggregate/union/param.
- [x] Replace `pipeline_ops` with `rel_ops: tuple[RelOp, ...]` on `RuleDefinition`.
- [x] Update rule authorship to emit structured ops, not raw mappings.
- [x] Add validation to ensure op sequences are well-formed (scan-first, etc.).

**Status**
Completed.

---

### Scope 2: Engine-Agnostic Plan Container and Compiler Interface
**Description**
Introduce a `RelPlan` container plus a compiler interface that emits engine-specific
plans (ArrowDSL, Ibis, DataFusion) while preserving schemas and ordering metadata.

**Code patterns**
```python
from dataclasses import dataclass
from arrowdsl.core.context import Ordering

@dataclass(frozen=True)
class RelPlan:
    ops: tuple[RelOp, ...]
    schema: pa.Schema
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
- [x] Define `RelPlan` with schema + ordering metadata.
- [x] Introduce `RelPlanCompiler` protocol and engine adapters.
- [x] Convert current rule compilation to produce `RelPlan` first.
- [x] Defer execution to engine adapters (ArrowDSL/Ibis/DataFusion).

**Status**
Completed.

---

### Scope 3: Rule Graph Canonicalization and Plan Hashing
**Description**
Generate a stable logical plan hash and canonical graph signature so rules can be
cached, diffed, and validated independently of the execution engine.

**Code patterns**
```python
def plan_signature(plan: RelPlan) -> str:
    payload = serialize_rel_ops(plan.ops)
    return hashlib.sha256(payload).hexdigest()
```

**Target files**
- Update: `src/relspec/rules/graph.py`
- Update: `src/relspec/rules/cache.py`
- Update: `src/relspec/rules/diagnostics.py`

**Implementation checklist**
- [x] Add canonical serialization for `RelOp` sequences.
- [x] Store plan hash in cache entries.
- [x] Store plan hash in rule diagnostics.
- [x] Emit graph-level signatures for rule bundles.
- [x] Expose plan signatures for migration/regression tooling.

**Status**
Completed.

---

### Scope 4: DataFusion Scan Options in Dataset Specs
**Description**
Extend dataset specs to capture DataFusion-native scan configuration (partitioning,
file sort order, pruning flags, metadata skipping) and propagate to engine adapters.

**Code patterns**
```python
@dataclass(frozen=True)
class DataFusionScanOptions:
    partition_cols: tuple[tuple[str, pa.DataType], ...] = ()
    file_sort_order: tuple[str, ...] = ()
    parquet_pruning: bool = True
    skip_metadata: bool = False
```

**Target files**
- Update: `src/schema_spec/system.py`
- Update: `src/relspec/registry.py`
- Update: `src/datafusion_engine/df_builder.py`
- Update: `src/ibis_engine/registry.py`

**Implementation checklist**
- [x] Add scan options to dataset specs and registry entries.
- [x] Thread options into DataFusion `register_listing_table` and `read_parquet`.
- [x] Preserve compatibility with Arrow dataset scanning.
- [x] Document precedence rules for scan options vs schema defaults.

**Status**
Completed.

---

### Scope 5: Runtime Profile for DataFusion Execution
**Description**
Introduce a runtime profile for DataFusion execution controls (partitions, memory
pool, spill paths, batch sizes) and integrate it into relspec execution paths.

**Code patterns**
```python
@dataclass(frozen=True)
class DataFusionRuntimeProfile:
    target_partitions: int = 8
    batch_size: int = 8192
    spill_dir: str | None = None
    memory_pool: Literal["greedy", "fair"] = "greedy"
```

**Target files**
- Add: `src/datafusion_engine/runtime.py`
- Update: `src/relspec/compiler.py`
- Update: `src/arrowdsl/core/context.py`

**Implementation checklist**
- [x] Define a DataFusion runtime profile dataclass.
- [x] Add profile hooks to relspec execution context.
- [x] Configure DataFusion `RuntimeEnv` and `SessionContext` from the profile at execution time.
- [x] Store runtime settings in telemetry for diagnostics.

**Status**
Completed.

---

### Scope 6: Hybrid Kernel Bridge (DataFusion UDF/UDTF + Arrow Fallback)
**Description**
Create a bridge that maps kernel ops to DataFusion UDF/UDTF when available, while
preserving ArrowDSL kernel fallback for unsupported operations.

**Code patterns**
```python
def apply_kernel_bridge(table: pa.Table, *, ctx: ExecutionContext) -> pa.Table:
    table = interval_align_table(table, spec=spec, ctx=ctx)
    table = explode_list_column(table, parent_id_col="src_id", list_col="dst_ids")
    return table
```

**Target files**
- Add: `src/datafusion_engine/kernels.py`
- Update: `src/arrowdsl/compute/kernels.py`
- Update: `src/relspec/compiler.py`

**Implementation checklist**
- [x] Define kernel capability registry (DF UDF/UDTF vs Arrow fallback).
- [x] Add DataFusion kernel adapters for interval align/explode/dedupe (Arrow fallback).
- [x] Implement DataFusion-native kernels (DataFrame ops + span UDF; unnest for list explode).
- [x] Ensure ordering metadata and schema contracts are preserved post-kernel.
- [x] Document kernel-lane fallbacks and invariants.

**Status**
Completed. DataFusion-native behavior uses DataFrame ops + span UDF; no explicit UDTF needed.

---

### Scope 7: SQLGlot Diagnostics and Validation Layer
**Description**
Compile relspec plans to SQLGlot AST via Ibis, then use metadata extraction,
optimization, and semantic diffing for rule validation and migration safety.

**Code patterns**
```python
sg_expr = con.compiler.to_sqlglot(expr)
sg_expr = optimize(sg_expr, schema=schema_map)
columns = [col.alias_or_name for col in sg_expr.find_all(exp.Column)]
```

**Target files**
- Add: `src/relspec/sqlglot_diagnostics.py`
- Update: `src/relspec/rules/validation.py`
- Update: `src/relspec/rules/diagnostics.py`

**Implementation checklist**
- [x] Provide SQLGlot compilation + metadata helpers.
- [x] Compile Ibis expressions to SQLGlot AST for each rule.
- [x] Extract referenced columns/tables and validate against schema contracts.
- [x] Store optimized AST and semantic diff metadata for migrations.
- [x] Gate rule execution on validation failures where appropriate.

**Status**
Completed. SQLGlot diagnostics are integrated into rule validation and gating.
