# Semantic Compiler Design Document

## Overview

The semantic compiler is a minimal abstraction layer that transforms extraction tables into CPG outputs using 10 composable rules. It bridges the gap between raw extraction outputs (PyArrow Tables with frozen schemas) and the rustworkx/Hamilton scheduling infrastructure by leveraging DataFusion's native plan building capabilities.

**Key insight**: Column presence determines available operations. We don't build a custom framework—we build expression helpers that plug into DataFusion's native `LogicalPlanBuilder`.

---

## Problem Statement

### The Gap

The CPG pipeline has three major phases:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   EXTRACTION    │ ──► │   COMPILATION   │ ──► │   SCHEDULING    │
│                 │     │                 │     │                 │
│ PyArrow Tables  │     │      ???        │     │ rustworkx +     │
│ with frozen     │     │                 │     │ Hamilton DAGs   │
│ schemas         │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

**Extraction** produces PyArrow Tables with well-defined schemas:
- CST refs, defs, imports, callsites from LibCST
- SCIP occurrences from language indexers
- Bytecode/symtable data from Python introspection

**Scheduling** requires InferredDeps for rustworkx task graph construction:
- Task names and outputs
- Input dependencies (which tables are read)
- Required columns per dataset
- Plan fingerprints for cache invalidation

**The gap**: How do we go from "tables with columns" to "tasks with dependencies"?

### The Old Approach (What We're Replacing)

The previous implementation used ~7,000+ lines of specification code:

| Component | Lines | Problem |
|-----------|-------|---------|
| `_VIEW_SELECT_EXPRS` dict | ~3,000 | Hardcoded column names, breaks on schema changes |
| Schema registries | ~1,500 | Redundant with what DataFusion already knows |
| Custom spec classes | ~2,000 | Parallel to DataFusion's own spec system |
| Join inference code | ~500 | Duplicated logic across relationship builders |

This led to:
- **Brittleness**: Changing `bstart` to `byte_start` required updates in 50+ places
- **Redundancy**: 5 parallel relationship builders with 95% identical code
- **Opacity**: Dependencies were declared, not derived from actual plan structure

---

## Core Design Principle

### Column Presence Determines Operations

The fundamental insight is that tables have **semantic types** based on what columns they contain:

```
EVIDENCE_TABLE := has(PATH) ∧ has(SPAN_START) ∧ has(SPAN_END)
ENTITY_TABLE   := EVIDENCE_TABLE ∧ has(ENTITY_ID)
SYMBOL_TABLE   := has(SYMBOL)
RELATION_TABLE := has(ENTITY_ID) ∧ has(SYMBOL)
```

Once we know the semantic type, the available operations follow:

- **EVIDENCE_TABLE** → can derive entity IDs, can derive span structs
- **ENTITY_TABLE** → can join with SYMBOL_TABLE to create relationships
- **RELATION_TABLE** → can union into CPG edges

This is not a new abstraction—it's formalizing what DataFusion's schema system already knows.

### Why This Works

DataFusion's `LogicalPlanBuilder` already handles:
- Arbitrary combinations of inputs via `join`, `union`, etc.
- Schema propagation—output schema derived from inputs automatically
- Expression composition—`col()`, `lit()`, function calls, nested types
- Optimization—push down filters, prune columns, reorder joins

We don't reinvent this. We add three helpers:
1. **Semantic column discovery**: Find columns by type pattern, not hardcoded name
2. **Expression templates**: Build expressions using discovered columns
3. **Join inference**: Match tables by compatible semantic types

---

## The Semantic Type System

### Column Types

Columns are classified by naming patterns:

```python
class ColumnType(StrEnum):
    PATH = auto()       # path, file_path, document_path
    FILE_ID = auto()    # file_id
    SPAN_START = auto() # bstart, *_bstart, byte_start, *_byte_start
    SPAN_END = auto()   # bend, *_bend, byte_end, *_byte_end
    ENTITY_ID = auto()  # *_id (excluding file_id)
    SYMBOL = auto()     # symbol, qname, qualified_name
    TEXT = auto()       # *_text, *_name, name
    EVIDENCE = auto()   # confidence, score, origin
    OTHER = auto()      # unclassified
```

Pattern matching uses regex, evaluated in order (first match wins):

```python
TYPE_PATTERNS = (
    (re.compile(r"^(path|file_path|document_path)$"), ColumnType.PATH),
    (re.compile(r"^file_id$"), ColumnType.FILE_ID),
    (re.compile(r"^bstart$|_bstart$|^byte_start$|_byte_start$"), ColumnType.SPAN_START),
    # ...
)
```

**Key benefit**: When column names change, patterns update in one place.

### Table Types

Table types are derived from column type combinations:

```python
def infer_table_type(column_types: set[ColumnType]) -> TableType:
    has_path = ColumnType.PATH in column_types
    has_span = ColumnType.SPAN_START in column_types and ColumnType.SPAN_END in column_types
    has_entity_id = ColumnType.ENTITY_ID in column_types
    has_symbol = ColumnType.SYMBOL in column_types

    if has_entity_id and has_symbol:
        return TableType.RELATION
    if has_entity_id and has_path and has_span:
        return TableType.ENTITY
    if has_path and has_span:
        return TableType.EVIDENCE
    if has_symbol:
        return TableType.SYMBOL_SOURCE
    return TableType.RAW
```

The hierarchy:
- **RELATION** > **ENTITY** > **EVIDENCE** > **RAW**
- A table can "upgrade" from EVIDENCE to ENTITY by adding an entity ID column

---

## The 10 Semantic Rules

Each rule defines: **IF** condition **THEN** operation is available.

### Derivation Rules (1-3)

These add computed columns to tables:

| Rule | Condition | Operation | Rust UDF |
|------|-----------|-----------|----------|
| 1 | PATH + SPAN | Derive entity_id | `stable_id_parts(prefix, path, bstart, bend)` |
| 2 | SPAN_START + SPAN_END | Derive span struct | `span_make(bstart, bend)` |
| 3 | TEXT columns | Normalize text | `utf8_normalize(text, form="NFC")` |

Example:
```python
def normalize(self, table_name: str, *, prefix: str) -> DataFrame:
    info = self.get(table_name)
    sem = info.sem

    # Rule 1 + Rule 2 applied together
    return (
        info.df
        .with_column(f"{prefix}_id", sem.entity_id_expr(prefix))
        .with_column("span", sem.span_expr())
    )
```

### Join Rules (4-6)

These combine tables based on semantic compatibility:

| Rule | Condition | Operation |
|------|-----------|-----------|
| 4 | Both have PATH | Equijoin on path |
| 5 | Both EVIDENCE + path join | Filter by `span_overlaps(a, b)` |
| 6 | Both EVIDENCE + path join | Filter by `span_contains(a, b)` |

The join helpers encode these patterns:

```python
def join_by_span_overlap(left, right, left_sem, right_sem):
    joined = join_by_path(left, right, left_sem, right_sem)
    return joined.filter(span_overlaps(left_sem.span_expr(), right_sem.span_expr()))
```

### Projection Rules (7)

| Rule | Condition | Operation |
|------|-----------|-----------|
| 7 | ENTITY_ID + SYMBOL after join | Project to relation schema |

The relation schema is canonical:
```
(entity_id, symbol, path, bstart, bend, origin)
```

### Aggregation Rules (8-10)

| Rule | Condition | Operation |
|------|-----------|-----------|
| 8 | Compatible schemas | Union with discriminator |
| 9 | GROUP_KEY + VALUES | `array_agg(values)` by group |
| 10 | KEY + SCORE | Keep best row per key |

---

## Integration with DataFusion

### Why DataFusion Is the Framework

DataFusion provides everything we need for plan building:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         EXTRACTION TABLES                                │
│            (registered as TableProviders in SessionContext)             │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      SEMANTIC COLUMN DISCOVERY                           │
│                                                                          │
│  ctx.table("cst_refs").schema() → classify each column:                 │
│    - file_id → FILE_IDENTITY                                            │
│    - path → PATH                                                         │
│    - bstart, bend → SPAN_START, SPAN_END                                │
│                                                                          │
│  Result: SemanticSchema with path_col(), span_expr(), entity_id_expr()  │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     RUST UDFs (registered in SessionContext)             │
│                                                                          │
│  span_make(bstart, bend) → struct<start, end, unit>                     │
│  stable_id_parts(prefix, path, bstart, bend) → string                   │
│  span_overlaps(span_a, span_b) → bool                                   │
│  span_contains(span_a, span_b) → bool                                   │
│  utf8_normalize(text, form) → string                                    │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    DATAFUSION LOGICALPLANBUILDER                         │
│                                                                          │
│  ctx.table("cst_refs")                                                  │
│      .with_column("ref_id", stable_id_parts(...))                       │
│      .with_column("span", span_make(...))                               │
│      .join(ctx.table("scip"), on=[path])                                │
│      .filter(span_overlaps(...))                                        │
│      .select(entity_id, symbol, path, bstart, bend, origin)             │
│                                                                          │
│  → DataFusion handles schema propagation, optimization, execution       │
└─────────────────────────────────────────────────────────────────────────┘
```

### Rust UDF Integration

The 47 Rust UDFs are accessed through `datafusion_engine.udf.shims`:

```python
from datafusion_engine.udf.shims import span_make, stable_id_parts, span_overlaps

def span_expr(self) -> Expr:
    return span_make(self.span_start_col(), self.span_end_col())

def entity_id_expr(self, prefix: str) -> Expr:
    return stable_id_parts(prefix, self.path_col(),
                           self.span_start_col(), self.span_end_col())
```

The shims handle:
- Unwrapping `Expr` arguments to raw values
- Calling `datafusion._internal` functions
- Wrapping results back into `Expr`

---

## Integration with rustworkx

### The Bridge: Plan Introspection

The connection between semantic compilation and rustworkx scheduling is **plan introspection**:

```python
def build_cpg_from_inferred_deps(ctx: SessionContext) -> dict[str, object]:
    from datafusion_engine.lineage.datafusion import extract_lineage

    build_cpg(ctx)  # Registers all views

    deps = {}
    for name in view_names:
        df = ctx.table(name)
        plan = df.optimized_logical_plan()
        lineage = extract_lineage(plan)

        deps[name] = {
            "task_name": name,
            "output": name,
            "inputs": lineage.referenced_tables,
            "required_columns": lineage.required_columns_by_dataset,
            "plan_fingerprint": hash(str(plan)),
        }

    return deps
```

### How It Works

1. **Semantic compilation** produces DataFusion views (lazy—not executed yet)
2. **Plan introspection** walks the `LogicalPlan` tree to extract:
   - Which tables are scanned (`TableScan` nodes)
   - Which columns are required per table (projection analysis)
   - Plan structure for fingerprinting
3. **rustworkx** uses this to build the task graph:
   - Nodes = tasks (one per view)
   - Edges = dependencies (from `inputs` sets)
4. **Hamilton** generates DAG functions from the scheduled order

```
┌────────────────────┐
│ Semantic Compiler  │
│                    │
│ compiler.normalize │──┐
│ compiler.relate    │  │
│ compiler.union     │  │
└────────────────────┘  │
                        ▼
┌────────────────────────────────────┐
│ DataFusion SessionContext          │
│                                    │
│ Views registered:                  │
│ - cst_refs_norm                    │
│ - rel_name_symbol                  │
│ - cpg_edges                        │
└────────────────────────────────────┘
                        │
                        ▼ df.optimized_logical_plan()
┌────────────────────────────────────┐
│ Plan Introspection                 │
│                                    │
│ extract_lineage(plan) →            │
│   referenced_tables: {"cst_refs"}  │
│   required_columns: {path, bstart} │
└────────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────┐
│ rustworkx TaskGraph                │
│                                    │
│ cst_refs_norm ◄── cst_refs         │
│       │                            │
│       ▼                            │
│ rel_name_symbol ◄── scip_occs      │
│       │                            │
│       ▼                            │
│ cpg_edges                          │
└────────────────────────────────────┘
                        │
                        ▼
┌────────────────────────────────────┐
│ Hamilton DAG                       │
│                                    │
│ @config.when(...)                  │
│ def cst_refs_norm(cst_refs): ...   │
│                                    │
│ def rel_name_symbol(                │
│     cst_refs_norm, scip_occs       │
│ ): ...                             │
└────────────────────────────────────┘
```

### Why This Is Better Than Declared Dependencies

**Old approach**: Dependencies were manually declared:
```python
TaskSpec(
    name="rel_name_symbol",
    inputs=["cst_refs_norm", "scip_occurrences"],  # Manual!
    # ...
)
```

**New approach**: Dependencies are inferred from the plan:
```python
ctx.register_view("rel_name_symbol", compiler.relate(
    "cst_refs_norm", "scip_occurrences",
    join_type="overlap",
    origin="cst_ref",
))
# Dependencies extracted automatically from the LogicalPlan
```

Benefits:
- **Correctness**: Dependencies match what the plan actually reads
- **Maintenance**: Add a new table to the join → dependencies update automatically
- **Cache invalidation**: `plan_fingerprint` changes when the plan structure changes

---

## Integration with Hamilton

### How Hamilton Consumes the Task Graph

Hamilton orchestrates execution of the scheduled tasks:

```python
# Generated by rustworkx scheduling
@config.when(task="cst_refs_norm")
def cst_refs_norm(cst_refs: pa.Table) -> pa.Table:
    """Normalize CST refs with entity IDs and spans."""
    ctx = SessionContext()
    ctx.register_table("cst_refs", cst_refs)

    compiler = SemanticCompiler(ctx)
    df = compiler.normalize("cst_refs", prefix="ref")

    return df.collect()  # Execute and return PyArrow Table
```

The Hamilton DAG mirrors the rustworkx task graph:
- Each task becomes a Hamilton function
- Function parameters = input dependencies
- Function return = output table

### Lazy vs Eager Execution

The semantic compiler operates **lazily**:
- `compiler.normalize()` returns a `DataFrame` (lazy)
- The view is registered but not executed
- Plan introspection happens on the lazy plan

Execution happens in Hamilton:
- `df.collect()` materializes the result
- Hamilton manages parallelism and caching
- Results flow to downstream tasks

---

## Composition: How Rules Chain

### Example Pipeline

```
EXTRACTION TABLE (cst_refs)
    │
    ├─── Rule 1 (Entity ID) + Rule 2 (Span) ───► ENTITY_TABLE (cst_refs_norm)
    │
    │                    SYMBOL_TABLE (scip_occurrences)
    │                           │
    └───── Rule 4 (Path Join) ──┴──────┐
                                       │
                     Rule 5 (Span Overlap) ────────────────┐
                                                           │
                               Rule 7 (Relation Project) ──┴──► RELATION_TABLE
                                                                (rel_name_symbol)

RELATION_TABLES
    │
    └─── Rule 8 (Union) ───► CPG_EDGES
```

### The Full Pipeline in ~50 Lines

```python
def build_cpg(ctx: SessionContext) -> None:
    compiler = SemanticCompiler(ctx)

    # Stage 1: Normalize extraction tables (Rule 1 + Rule 2)
    ctx.register_view("cst_refs_norm", compiler.normalize("cst_refs", prefix="ref"))
    ctx.register_view("cst_defs_norm", compiler.normalize("cst_defs", prefix="def"))
    ctx.register_view("cst_imports_norm", compiler.normalize("cst_imports", prefix="import"))
    ctx.register_view("cst_calls_norm", compiler.normalize("cst_callsites", prefix="call"))

    # Register for relationship building
    for name in ["cst_refs_norm", "cst_defs_norm", "cst_imports_norm", "cst_calls_norm"]:
        compiler.register(name)
    compiler.register("scip_occurrences")

    # Stage 2: Build relationships (Rule 5/6 + Rule 7)
    ctx.register_view("rel_name_symbol", compiler.relate(
        "cst_refs_norm", "scip_occurrences",
        join_type="overlap", filter_sql="is_read = true", origin="cst_ref",
    ))
    ctx.register_view("rel_def_symbol", compiler.relate(
        "cst_defs_norm", "scip_occurrences",
        join_type="contains", filter_sql="is_definition = true", origin="cst_def",
    ))
    ctx.register_view("rel_import_symbol", compiler.relate(
        "cst_imports_norm", "scip_occurrences",
        join_type="overlap", filter_sql="is_import = true", origin="cst_import",
    ))
    ctx.register_view("rel_call_symbol", compiler.relate(
        "cst_calls_norm", "scip_occurrences",
        join_type="overlap", origin="cst_call",
    ))

    # Stage 3: Build CPG outputs (Rule 8)
    ctx.register_view("cpg_edges", compiler.union_with_discriminator(
        ["rel_name_symbol", "rel_def_symbol", "rel_import_symbol", "rel_call_symbol"],
        discriminator="edge_kind",
    ))
    ctx.register_view("cpg_nodes", compiler.union_with_discriminator(
        ["cst_refs_norm", "cst_defs_norm", "cst_imports_norm", "cst_calls_norm"],
        discriminator="node_kind",
    ))
```

---

## What This Enables

### Schema Changes → Update Patterns

When `bstart` becomes `byte_start`:
1. Add pattern to `TYPE_PATTERNS`: `r'^byte_start$|_byte_start$'`
2. All pipeline functions automatically find the new column
3. No changes to relationship builders, normalization logic, etc.

### New Relationship Types → Compose Rules

To add a new relationship type:
```python
ctx.register_view("rel_new_thing", compiler.relate(
    "some_entity_table", "some_symbol_source",
    join_type="overlap",  # or "contains"
    filter_sql="some_condition = true",
    origin="new_thing",
))
```

The compiler:
1. Analyzes schemas → infers column types
2. Checks rules → determines available operations
3. Applies rules → generates DataFusion expressions
4. Registers view → DataFusion handles the rest

### Debugging → Inspect Plans

Since everything is DataFusion plans, debugging uses standard tools:
```python
df = ctx.table("rel_name_symbol")
print(df.explain())  # Show logical plan
print(df.optimized_logical_plan())  # Show optimized plan
```

---

## Comparison: Before and After

| Aspect | Before | After |
|--------|--------|-------|
| Lines of code | ~7,000 | ~1,200 |
| Schema changes | 50+ locations | 1 pattern file |
| Dependency declaration | Manual | Inferred from plans |
| Join logic | 5 parallel builders | 3 helper functions |
| Relationship schema | 4 separate definitions | 1 canonical projection |
| Adding new relationship | Copy-paste 200+ lines | 5-line `compiler.relate()` call |

---

## Summary

The semantic compiler works because:

1. **Column presence is semantic**: Tables have meaning based on what columns they contain
2. **DataFusion is the framework**: We don't build a custom plan builder—we use DataFusion's
3. **Rules compose cleanly**: Each rule is independent; they chain through data flow
4. **Plan introspection bridges to scheduling**: Dependencies come from the plan, not declarations
5. **Rust UDFs do the heavy lifting**: span_make, stable_id, span_overlaps are fast and correct

The result: A minimal, maintainable system where the semantics are explicit and the implementation is derived.

---

# Implementation Plan: Semantic Compiler Hardening (DataFrame‑Expression First)

This plan converts the current `src/semantics` draft into a **production‑grade DataFrame compiler** with deterministic behavior, explicit validation, and stable metadata extraction. The goal is to keep the elegant rule‑based model but remove hidden foot‑guns.

---

## Best‑in‑Class Extensions (DataFusion + Delta)

**Scope note:** SQL function factory is **explicitly excluded** for now (no imminent value). Everything else below is in‑scope.

---

## A) Planning Artifacts & Deterministic Fingerprints

Status: Complete (plan bundles + artifact persistence wired; gated by `runtime_profile.capture_plan_artifacts`).

**Why**  
Plan artifacts are the most reliable source of inferred dependencies and cache invalidation. DataFusion already exposes P0/P1/P2 and explain outputs; we should persist them and compute stable fingerprints from them.

### Target Implementation
```python
# Build and persist plan bundle for each view
bundle = build_plan_bundle(ctx, df, options=PlanBundleOptions(...))
fingerprint = bundle.identity_hash()  # stable, version‑aware
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/semantics/pipeline.py` | Replace `hash(str(plan))` with bundle identity |
| Modify | `src/datafusion_engine/plan/bundle.py` | Stable identity + plan details |
| Modify | `src/datafusion_engine/views/graph.py` | Persist plan artifacts per view |
| Modify | `src/datafusion_engine/plan/artifact_store.py` | Delta-backed plan artifact persistence |

### Checklist
- [x] Use plan bundle identity for fingerprints
- [x] Persist P0/P1/P2 + EXPLAIN artifacts
- [x] Add dependency extraction from P1 (optimized logical)

---

## B) SessionContext Hardening (Planning Environment Pinning)

Status: Complete (SessionFactory pins defaults; runtime captures settings payloads).

**Why**  
Plan shape depends on session config. Pinning config ensures reproducible logical plans.

### Target Implementation
```python
SessionConfig()
  .with_default_catalog_and_schema("datafusion", "public")
  .with_information_schema(True)
  .with_target_partitions(16)
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/datafusion_engine/session/factory.py` | Pin catalog/schema + info_schema |
| Modify | `src/datafusion_engine/session/runtime.py` | Record config into artifacts |

### Checklist
- [x] Pin catalog/schema defaults
- [x] Enable information_schema by default
- [x] Record config in artifacts / runtime profile

---

## C) Schema Contracts from Information Schema

Status: Complete (information_schema contracts validated in view graph).

**Why**  
Information schema is the canonical metadata surface. This eliminates manual schema registries and reduces drift.

### Target Implementation
```python
columns = info_schema.columns(...)
constraints = info_schema.table_constraints(...)
contract = SchemaContract.from_information_schema(...)
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/datafusion_engine/schema/catalog_contracts.py` | Build contracts from information_schema |
| Modify | `src/datafusion_engine/views/graph.py` | Validate schema contracts on registration |

### Checklist
- [x] Build contracts from info_schema columns/constraints
- [x] Validate view schemas on registration

---

## D) Delta TableProvider + Snapshot Pinning

Status: Complete (Delta providers + version/time pinning already supported).

**Why**  
TableProvider gives file‑level pruning; Arrow Dataset fallback loses pushdown. Snapshot pinning avoids cross‑stage drift.

### Target Implementation
```python
table = DeltaTable(path, version=...)
ctx.register_table("t", table)
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/datafusion_engine/dataset/resolution.py` | Resolve Delta TableProvider |
| Modify | `src/datafusion_engine/dataset/registration.py` | Register provider with optional version/time pinning |

### Checklist
- [x] Prefer DeltaTable provider for all Delta IO
- [x] Add explicit version/time‑travel pinning

---

## E) Delta Change Data Feed (CDF) for Incremental Runs

Status: Complete (CDF registration helper + optional pipeline substitution).

**Why**  
CDF exposes incremental changes natively; no custom diff logic required.

### Target Implementation
```python
ctx.register_table("cdf", DeltaCdfTableProvider(...))
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/datafusion_engine/delta/cdf.py` | CDF registration helpers |
| Modify | `src/semantics/pipeline.py` | Optional incremental path |

### Checklist
- [x] Add CDF provider registration
- [x] Use CDF in incremental pipelines

---

## F) Cache + Stats Policy (Performance Contract)

Status: Complete (disk-backed cache policies only; stats/cache defaults in runtime profiles).

**Why**  
DataFusion’s cost‑based optimizer depends on stats; caches reduce repeated scans.

### Target Implementation
```python
SessionConfig()
  .set("datafusion.execution.collect_statistics", "true")
  .set("datafusion.runtime.metadata_cache_limit", "...")
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/datafusion_engine/session/factory.py` | Set stats + cache knobs |
| Modify | `src/datafusion_engine/session/runtime.py` | Record cache policy metadata |

### Checklist
- [x] Enable collect_statistics
- [x] Configure metadata/listing caches
- [x] Cache hot normalized views (Delta/parquet staging or output caches)

---

## G) Rust UDF Best‑Practice Upgrades

Status: Complete (UDF audit payload emitted; volatility + fast‑path coverage tracked).

**Why**  
Modern UDF APIs improve correctness, typing, and optimization.

### Target Implementation
- Use `ScalarUDFImpl` with `return_field_from_args`
- Use explicit volatility flags
- Support named arguments where possible

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `rust/` UDF implementations | Upgrade to ScalarUDFImpl patterns |
| Modify | `src/datafusion_engine/udf/shims.py` | Match new arg signatures |

### Checklist
- [x] Switch UDFs to ScalarUDFImpl
- [x] Define return field metadata
- [x] Audit volatility + scalar fast paths

---

## H) Diagnostics & Observability

Status: Complete (plan artifacts + cache telemetry + view/UDF parity recorded).

**Why**  
Best‑in‑class systems expose plan lineage, UDF parity, schema hashes, and cache health.

### Target Implementation
```python
artifact = build_view_artifact_from_bundle(...)
record_view_definition(...)
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/datafusion_engine/views/artifacts.py` | Extend diagnostics payloads |
| Modify | `src/datafusion_engine/lineage/diagnostics.py` | Add plan + cache telemetry |

### Checklist
- [x] Persist view + UDF parity diagnostics
- [x] Store schema hash + runtime config
- [x] Add cache/stats telemetry in artifacts

---

## I) Union/Join Determinism + Schema Safety

Status: Complete (prefixed joins, canonical union ordering, semantic validators).

**Why**  
Unstable unions and ambiguous joins can silently corrupt outputs.

### Target Implementation
- Canonical projection before union
- Prefix columns before join
- Validate semantic requirements before applying rules

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/semantics/compiler.py` | Canonical union + prefixed joins |
| Modify | `src/semantics/schema.py` | Semantic validators |

### Checklist
- [x] Add schema‑safe union paths
- [x] Add deterministic join projection
- [x] Validate evidence/entity/symbol tables early

---

## J) Catalog Hygiene & Namespacing

Status: Remaining.

**Why**  
DataFusion catalog/schema boundaries are real contracts; enforcing them prevents ambiguous table resolution.

### Target Implementation
```python
ctx.register_catalog_provider("cpg", catalog)
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/datafusion_engine/session/factory.py` | Create canonical catalogs/schemas |
| Modify | `src/datafusion_engine/io/adapter.py` | Ensure catalog‑aware registration |

### Checklist
- [ ] Standardize catalog/schema names (raw/staging/curated/views)
- [ ] Ensure view registration respects catalog namespace

---

## K) Delta‑Backed Cache & Historization

Status: Complete (Delta cache registration + reuse wired).

**Why**  
In‑memory cache is fast but volatile. A Delta‑backed cache makes results durable, shareable across sessions, and introspectable. It also improves planning because cached tables have statistics and schema stability.

### Target Implementation
```python
# Persist hot views into Delta tables
cache_path = f"{cache_root}/{view_name}"
write_deltalake(cache_path, data, mode="overwrite")

# Register cache table for reuse
ctx.register_table(view_name, DeltaTable(cache_path))
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/datafusion_engine/cache/registry.py` | Cache registration + inventory tracking |
| Create | `src/datafusion_engine/cache/inventory.py` | Cache inventory schema + writer |
| Modify | `src/datafusion_engine/views/graph.py` | Cache materialization hook + reuse |

### Checklist
- [x] Add Delta cache registration helpers
- [x] Persist cache inventory (plan hash, schema hash, snapshot)
- [x] Use cache tables in plan reuse

---

## L) Cache Introspection & Inventory Tables

Status: Complete (inventory table + per‑refresh entries recorded).

**Why**  
Operational visibility is required for debugging cache staleness and performance regressions.

### Target Implementation
```python
# cache_inventory schema
{
  "view_name": str,
  "plan_hash": str,
  "schema_hash": str,
  "snapshot_version": int,
  "created_at": str,
}
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/datafusion_engine/cache/inventory.py` | Delta table schema + writer |
| Modify | `src/datafusion_engine/views/graph.py` | Record cache inventory + artifacts |

### Checklist
- [x] Create cache inventory Delta table
- [x] Emit cache metrics per view refresh

---

## M) Incremental Updates via Delta CDF

Status: Complete (runtime CDF registration + auto CDF input mapping).

**Why**  
Most refreshes affect a small subset of rows. Change Data Feed allows incremental recompute.

### Target Implementation
```python
ctx.register_table("cdf_input", DeltaCdfTableProvider(...))
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/datafusion_engine/delta/cdf.py` | Wire CDF in cache refresh |
| Modify | `src/semantics/pipeline.py` | Incremental paths per view |

### Checklist
- [x] Add CDF registration in runtime
- [x] Partition incremental recompute by CDF changes (CDF inputs drive incremental paths)

---

## N) Planning‑Aware Cache Reuse

Status: Complete (plan identity + snapshot comparison wired).

**Why**  
Avoid recompute when plan + inputs haven’t changed.

### Target Implementation
```python
if plan_hash == cache.plan_hash and snapshot_version == cache.snapshot_version:
    return cached_df
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/datafusion_engine/views/graph.py` | Cache short‑circuit logic |
| Modify | `src/datafusion_engine/cache/registry.py` | Compare plan/snapshot |

### Checklist
- [x] Compare plan hashes before recompute
- [x] Compare Delta snapshot version before recompute

---

## O) Telemetry + Error Correction

Status: Complete (structured cache errors + thresholded explain-analyze artifacts).

**Why**  
Best‑in‑class systems emit telemetry for debugging and enforce safe error recovery.

### Target Implementation
- Record `EXPLAIN ANALYZE` metrics for slow views
- Persist schema mismatch errors + retry guidance

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/datafusion_engine/views/artifacts.py` | Attach execution metrics |
| Modify | `src/datafusion_engine/session/runtime.py` | Standardize retry/abort policies |

### Checklist
- [x] Emit EXPLAIN ANALYZE when thresholds exceeded
- [x] Persist structured error artifacts

---

## P) Schema Evolution Gate (No Whiplash)

Status: Complete (schema mismatch gating enforced).

**Why**  
Delta schema changes are metadata commits that can break streams and concurrent writes. Default must be hard‑error.

### Target Implementation
```python
if schema_hash != expected and not allow_evolution:
    raise SchemaMismatchError(...)
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/datafusion_engine/delta/contracts.py` | Schema hash gating |
| Modify | `src/datafusion_engine/cache/registry.py` | Enforce allow‑evolution flag |

### Checklist
- [x] Enforce schema mismatch as error by default
- [x] Require explicit schema evolution mode

---

## Q) Partition‑Aligned Computation

Status: Complete (partition defaults + validation enforced for cache writes).

**Why**  
Partition‑aligned writes reduce recompute and maximize pruning.

### Target Implementation
```python
write_deltalake(path, data, partition_by=["repo", "path_prefix"])
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/datafusion_engine/io/write.py` | Partition config defaults |
| Modify | `src/datafusion_engine/cache/registry.py` | Store partition policy |

### Checklist
- [x] Default partitioning policy per dataset type
- [x] Ensure partition columns are in schema contracts

---

## 1) Semantic Configuration & Overrides

Status: Complete (SemanticConfig + overrides integrated).

**Why**  
Name‑only inference is fragile. `*_id` patterns can misclassify columns (e.g., `span_id`, `code_unit_id`), and table schemas differ across extractors. A configuration layer allows:
- explicit overrides per dataset
- safer matching (pattern + dtype)
- easier future migrations

### Target Implementation

**New configuration object:**
```python
# src/semantics/config.py
@dataclass(frozen=True)
class SemanticConfig:
    type_patterns: tuple[tuple[re.Pattern[str], ColumnType], ...] = TYPE_PATTERNS
    table_overrides: dict[str, dict[ColumnType, str]] = field(default_factory=dict)
    disallow_entity_id_patterns: tuple[re.Pattern[str], ...] = (
        re.compile(r"^span_id$"),
        re.compile(r"^code_unit_id$"),
    )
```

**Schema inference uses config:**
```python
schema = SemanticSchema.from_df(df, config=SemanticConfig(...))
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/semantics/config.py` | SemanticConfig + overrides |
| Modify | `src/semantics/schema.py` | Accept config; apply overrides |
| Modify | `src/semantics/types.py` | Split patterns into base + configurable |

### Decommission / Deletion
- No deletions; this hardens the existing inference logic.

### Checklist
- [x] Add `SemanticConfig` with per‑table overrides
- [x] Allow schema inference to accept overrides
- [x] Add “disallow entity ID” patterns to avoid false positives

---

## 2) Join Disambiguation & Deterministic Projection

Status: Complete (prefixed joins + explicit projections in compiler).

**Why**  
DataFusion joins on identical column names can coalesce or become ambiguous. The current `relate()` uses unqualified columns, which can silently select the wrong side. This breaks correctness in overlap/contains joins.

### Target Implementation

**Prefix columns prior to join:**
```python
def _prefix_cols(df: DataFrame, prefix: str) -> DataFrame:
    exprs = [col(c).alias(f"{prefix}{c}") for c in df.schema().names]
    return df.select(*exprs)
```

**Join with prefixed columns and project deterministically:**
```python
left_df = _prefix_cols(left_info.df, "l_")
right_df = _prefix_cols(right_info.df, "r_")
joined = join_by_path(left_df, right_df, left_sem.prefixed("l_"), right_sem.prefixed("r_"))
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/semantics/join_helpers.py` | Add prefix‑aware join helpers |
| Modify | `src/semantics/compiler.py` | Use prefixed join + explicit projection |
| Modify | `src/semantics/schema.py` | Add prefix accessor for semantic columns |

### Decommission / Deletion
- Remove any reliance on ambiguous column names in `relate()`.

### Checklist
- [x] Add prefix helper for DataFrames
- [x] Add prefix support to SemanticSchema accessors
- [x] Update relation projection to use explicit prefixed columns

---

## 3) UDF Availability Gate

Status: Complete (UDF gate + required checks wired).

**Why**  
`span_make`, `stable_id_parts`, `span_overlaps`, and `utf8_normalize` are Rust UDFs. If the UDF platform is missing, errors appear late and opaque.

### Target Implementation

**UDF gate helper:**
```python
def require_udfs(ctx: SessionContext, required: tuple[str, ...]) -> None:
    snapshot = rust_udf_snapshot(ctx)
    validate_required_udfs(snapshot, required=required)
```

**Guard compiler entrypoints:**
```python
self._require_udfs(("span_make", "stable_id_parts"))
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/semantics/compiler.py` | Add `_require_udfs()` method |
| Modify | `src/semantics/schema.py` | Call UDF gate for span/entity expressions |

### Checklist
- [x] Add `require_udfs` helper
- [x] Guard `normalize()`, `relate()`, `normalize_text()` paths

---

## 4) Canonical Union Contracts

Status: Complete (canonical column order enforced).

**Why**  
`union_with_discriminator()` currently unions raw DataFrames without ensuring column order or missing columns. This can silently corrupt data.

### Target Implementation

**Canonical projection before union:**
```python
def _project_to_schema(df: DataFrame, schema: list[str]) -> DataFrame:
    return df.select(*[col(c).alias(c) for c in schema])
```

Use first table as canonical, or provide explicit schema list.

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/semantics/compiler.py` | Force canonical projection in union |

### Checklist
- [x] Add canonical column order for union
- [x] Require explicit schema for union if types differ

---

## 5) Stable Plan Fingerprints

Status: Complete (plan bundle fingerprints used).

**Why**  
`hash(str(plan))` is unstable and varies with DataFusion versions. We need deterministic fingerprints for scheduling/cache invalidation.

### Target Implementation

**Use existing plan bundle / artifact identities:**
```python
from datafusion_engine.plan.bundle import build_plan_bundle
from datafusion_engine.identity import schema_identity_hash
```

Compute fingerprint from the plan bundle’s normalized logical plan.

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/semantics/pipeline.py` | Replace `hash(str(plan))` with stable identity |

### Checklist
- [x] Use plan bundle for fingerprinting
- [x] Ensure fingerprint stable across runs

---

## 6) Explicit Semantic Validation Errors

Status: Complete (SemanticSchemaError + validators).

**Why**  
Missing columns currently produce raw `ValueError` from accessors. We need context‑rich error messages (table, expected semantics, discovered columns).

### Target Implementation

**Validation helpers:**
```python
def require_evidence(sem: SemanticSchema, *, table: str) -> None:
    if not sem.is_evidence():
        raise SemanticSchemaError(...)
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/semantics/schema.py` | Add validation helpers + errors |
| Modify | `src/semantics/compiler.py` | Call validators before rules |

### Checklist
- [x] Add `SemanticSchemaError`
- [x] Validate schema before normalize/relate

---

## 7) Spec‑Driven Primary Span + Entity ID Selection

Status: Remaining.

**Why**  
Multi‑span tables (e.g., `cst_defs`) and FK‑first schemas make “first match wins” unsafe and can cause ID collisions and wrong joins.

### Target Implementation
```python
spec = SemanticTableSpec(
    table="cst_defs",
    primary_span=SpanBinding("def_bstart", "def_bend"),
    entity_id=IdDerivation(out_col="def_id", namespace="cst_def"),
    foreign_keys=(
        ForeignKeyDerivation(
            out_col="container_def_id",
            target_namespace="cst_def",
            start_col="container_def_bstart",
            end_col="container_def_bend",
            guard_null_if=("container_def_kind",),
        ),
    ),
)
df = SemanticCompiler(ctx).normalize_from_spec(spec)
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/semantics/specs.py` | Spec dataclasses + helpers |
| Modify | `src/semantics/compiler.py` | Add `normalize_from_spec()` + FK derivations |
| Modify | `src/semantics/schema.py` | Prefer canonical `bstart/bend/entity_id` |
| Modify | `src/semantics/config.py` | Optional spec registry hookup |

### Checklist
- [ ] Spec explicitly selects primary span + entity id
- [ ] FK IDs derived from reference spans with namespace correctness
- [ ] Canonical `bstart/bend/entity_id` emitted in normalize output
- [ ] Fail fast when inference is ambiguous without a spec

---

## 8) SCIP Normalization to Byte Spans

Status: Remaining.

**Why**  
`scip_occurrences` is line/char‑based; semantic joins require byte spans (`bstart/bend`). Without normalization, `relate()` is invalid.

### Target Implementation
```python
scip_norm = scip_to_byte_offsets(ctx.table("scip_occurrences"), source_text=...)
ctx.register_view("scip_occurrences_norm", scip_norm)
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/semantics/scip_normalize.py` | Line/char → byte span transform |
| Modify | `src/semantics/pipeline.py` | Use `scip_occurrences_norm` for relations |
| Modify | `src/semantics/specs.py` | Add SCIP spec (span unit metadata) |

### Checklist
- [ ] Emit `bstart/bend` for SCIP with unit validation
- [ ] Preserve original `(line,char)` columns
- [ ] Ensure unit parity before span joins

---

## 9) Canonical Node/Edge Projection Before Union

Status: Remaining.

**Why**  
Unioning heterogeneous normalized tables will fail. Canonical projection makes unions deterministic and schema‑safe.

### Target Implementation
```python
CANONICAL_NODE_COLS = ("entity_id", "node_kind", "path", "bstart", "bend", "file_id", "attrs")
df = df.select(*[col(c).alias(c) for c in CANONICAL_NODE_COLS])
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/semantics/compiler.py` | Add canonical node/edge projection helpers |
| Modify | `src/semantics/pipeline.py` | Apply projection pre‑union |

### Checklist
- [ ] Define canonical node/edge schemas
- [ ] Project before `union_with_discriminator()`

---

## 10) Text Normalization Policy (Non‑Destructive)

Status: Remaining.

**Why**  
`utf8_normalize` defaults to casefolding/collapse; overwriting identifiers can corrupt semantics.

### Target Implementation
```python
df = df.with_column(
    "symbol_norm",
    utf8_normalize(col("symbol"), form="NFKC", casefold=False, collapse_ws=False),
)
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/semantics/compiler.py` | Add `normalize_text(..., output_suffix="_norm")` |
| Modify | `src/semantics/specs.py` | Per‑table text columns + policy |

### Checklist
- [ ] Do not overwrite identifier columns in place
- [ ] Explicit casefold/collapse flags
- [ ] Normalize only declared text columns

---

## 11) Enforce ViewGraph‑Only Execution (No Direct SessionContext Registration)

Status: Remaining.

**Why**  
Semantic pipelines must use the same contract/caching/artifact path as the rest of the system.

### Target Implementation
```python
nodes = semantic_view_nodes(ctx, runtime_profile=profile, options=...)
register_view_graph(ctx, nodes=nodes, snapshot=..., runtime_options=..., options=...)
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/semantics/pipeline.py` | Remove direct `ctx.register_view` path |
| Modify | `src/semantics/pipeline.py` | Require runtime profile or return `ViewNode`s |

### Checklist
- [ ] All semantic builds go through `register_view_graph`
- [ ] Plan bundles + artifacts always captured
- [ ] Schema contracts always enforced

---

## 12) Span Unit Metadata Gate

Status: Remaining.

**Why**  
Mixing line/char spans with byte spans will silently break overlaps/contains.

### Target Implementation
```python
validate_span_unit(schema, expected_unit="byte")
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Modify | `src/semantics/schema.py` | Inspect span unit metadata |
| Modify | `src/semantics/compiler.py` | Gate joins on unit compatibility |

### Checklist
- [ ] Reject joins on incompatible units
- [ ] Surface metadata in error payloads

---

## 13) Semantic Spec Registry (Minimal Declarative Layer)

Status: Remaining.

**Why**  
We need a single source of truth for primary spans/ids with minimal declarative burden.

### Target Implementation
```python
SEMANTIC_TABLE_SPECS = {
    "cst_defs": SemanticTableSpec(...),
    "cst_imports": SemanticTableSpec(...),
}
```

### Target File List
| Action | File | Description |
|--------|------|-------------|
| Create | `src/semantics/spec_registry.py` | Canonical spec map |
| Modify | `src/semantics/compiler.py` | Consult registry in `normalize()` |
| Modify | `src/semantics/config.py` | Wire registry into config |

### Checklist
- [ ] Register specs for ambiguous tables
- [ ] Inference used only as fallback
- [ ] Clear errors when spec is missing and ambiguous

---

## Summary of Decommissioning

Current state (implemented):
- Ambiguous join projections removed (prefixed joins + explicit projection).
- UDF availability is validated early.
- Plan fingerprints are stable via plan bundles.
- Cache reuse, inventory, and schema evolution gating are wired.

Remaining before decommissioning legacy paths:
- Spec‑driven primary span/entity ID selection.
- SCIP normalization to byte spans.
- Canonical node/edge projections pre‑union.
- Non‑destructive text normalization policy.
- Enforce view‑graph‑only execution for semantics.
- Span unit metadata gating + spec registry.

Once remaining scope is complete, the semantic compiler will be robust enough to fully replace legacy spec‑driven logic.
