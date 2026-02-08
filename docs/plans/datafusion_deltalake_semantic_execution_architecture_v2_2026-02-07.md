# DataFusion + DeltaLake Semantic Execution Architecture v2

Date: 2026-02-07

---

## 1) Design Thesis

Replace Hamilton DAG orchestration and rustworkx graph scheduling with a single
DataFusion planning + execution pipeline backed by DeltaLake storage. The semantic
compiler emits a single LogicalPlan DAG (not task graphs, not Hamilton modules)
and DataFusion's own optimizer, physical planner, and partitioned executor handle
all scheduling, concurrency, and optimization automatically.

**Core bet:** DataFusion's five-stage compiler pipeline (parse, analyze, logical
optimize, physical plan, physical optimize) is a strictly better execution planner
than any bespoke Python-side task graph. We build a semantic-to-relational compiler
that produces LogicalPlan DAGs, and let the engine do what engines do.

### What Dies

- Hamilton DAG orchestration (`src/hamilton_pipeline/`)
- Rustworkx graph scheduling (`src/relspec/rustworkx_graph.py`, `rustworkx_schedule.py`)
- Manual `TaskSpec` / `PlanCatalog` / `TaskGraph` construction
- Bespoke dependency inference from plan lineage (DataFusion handles this natively)
- Python-side execution sequencing and materialization coordination

### What Stays

- Semantic compiler (`src/semantics/compiler.py`) - upgraded to emit LogicalPlan
- View specifications and ViewKind registry
- Entity model and semantic type system
- Extraction layer (produces Delta tables as inputs)
- CPG schema contracts (define output shape)

### What's New

- Rust `SemanticPlanCompiler`: semantic view graph to combined LogicalPlan DAG
- Rust `DeltaSessionFactory`: deterministic SessionContext construction
- Rust `CpgMaterializer`: stream execution + Delta commit
- Rust custom UDFs/UDAFs for CPG-specific operations
- Plan artifact bundle (P0/P1/P2) with fingerprinting and regression detection
- Delta-native caching hierarchy replacing `_default_semantic_cache_policy()`

---

## 2) Architecture Overview

### 2.1 Runtime Topology

```
Extraction Outputs (Delta tables)
        |
        v
[1] DeltaSessionFactory ──────────── Deterministic SessionContext
        |                              - SessionConfig (plan shape)
        |                              - RuntimeEnvBuilder (memory/spill)
        |                              - Catalog/schema/table registration
        |                              - UDF/UDAF/UDWF/UDTF registration
        v
[2] SemanticPlanCompiler ─────────── Semantic view graph → LogicalPlan DAG
        |                              - View spec → SQL/DataFrame/Builder
        |                              - Join inference → join nodes
        |                              - Union-by-name for schema drift
        |                              - CTE structure for reusable subplans
        v
[3] DataFusion Planning Pipeline ─── Engine-native compilation
        |  Stage 1: AnalyzerRules (type coercion, name resolution)
        |  Stage 2: OptimizerRules (pushdown, simplification, join reorder)
        |  Stage 3: Physical planning (operator selection, partitioning)
        |  Stage 4: PhysicalOptimizerRules (distribution, sort enforcement)
        v
[4] Plan Artifact Capture ────────── P0 + P1 + P2 + EXPLAIN snapshots
        |
        v
[5] Partitioned Stream Execution ─── execute_stream_partitioned()
        |                              - In-query parallelism via target_partitions
        |                              - Memory-bounded via fair spill pool
        |                              - Delta two-tier pruning (file + row-group)
        v
[6] CpgMaterializer ─────────────── Delta commit
        |                              - insert_into (append/overwrite)
        |                              - Schema enforcement via DeltaDataChecker
        |                              - Compact result envelope
        v
CPG Delta Outputs + Plan Bundle + Metrics
```

### 2.2 Language Split

| Layer | Language | Rationale |
|-------|----------|-----------|
| Session factory | Rust | Deterministic registration order, optimizer rule injection |
| Semantic plan compiler | Rust | `LogicalPlanBuilder` + `DataFrame` composition at plan-node level |
| Custom UDFs/UDAFs | Rust | `ScalarUDFImpl` / `AggregateUDFImpl` with Arrow kernel performance |
| Physical execution | Rust | DataFusion's Tokio-based partitioned executor |
| Delta providers | Rust | `DeltaTableProvider::try_new` + `DeltaScanConfig` knobs |
| Delta commits | Rust | `insert_into` via TableProvider + transaction log semantics |
| Python shell | Python | CLI/API ergonomics, semantic spec submission, result retrieval |
| Semantic view specs | Python | View definitions as declarative Python (existing pattern) |

### 2.3 One-Context Rule

All plan fragments in a run are built within a single `SessionContext`. This is not
a policy choice; it is a DataFusion invariant. Plans are only executable inside the
context that owns their catalog/function/config universe. Cross-context plan merging
is undefined behavior.

---

## 3) Deterministic Session Contract

### 3.1 Session Envelope

Every execution run is defined by a deterministic envelope captured before any
plan construction:

```rust
pub struct SessionEnvelope {
    // Version pins
    datafusion_version: String,        // e.g., "52.1.0"
    delta_rs_version: String,          // e.g., "0.25.0"
    codeanatomy_version: String,       // from Cargo.toml

    // Session config snapshot (plan-shape knobs)
    config_snapshot: BTreeMap<String, String>,  // from df_settings

    // Runtime config (execution feasibility)
    target_partitions: u32,
    batch_size: u32,
    memory_pool_bytes: u64,
    spill_enabled: bool,

    // Catalog state (registered tables + schemas)
    table_registrations: Vec<TableRegistration>,  // name, kind, delta_version, schema_hash

    // Function registry (UDF/UDAF/UDWF/UDTF)
    registered_functions: Vec<FunctionRegistration>,  // name, signature, volatility

    // Semantic spec identity
    semantic_spec_hash: [u8; 32],

    // Computed identity
    envelope_hash: [u8; 32],  // hash of all above fields
}
```

**Capture mechanism:** After session construction and before plan compilation,
snapshot via `SELECT * FROM information_schema.df_settings` for config, plus
`SHOW FUNCTIONS` for the function registry, plus provider schema introspection
for table state.

### 3.2 SessionContext Construction (Rust)

```rust
use datafusion::prelude::*;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;

pub fn build_session(profile: &EnvironmentProfile) -> Result<SessionContext> {
    let runtime = RuntimeEnvBuilder::default()
        .with_fair_spill_pool(profile.memory_pool_bytes)
        .with_disk_manager_specified(&profile.spill_paths)
        .build()?;

    let config = SessionConfig::new()
        .with_default_catalog_and_schema("codeanatomy", "public")
        .with_information_schema(true)
        .with_target_partitions(profile.target_partitions)
        .with_batch_size(profile.batch_size)
        .with_repartition_joins(true)
        .with_repartition_aggregations(true)
        .with_repartition_windows(true)
        .with_parquet_pruning(true)
        .set("datafusion.execution.parquet.pushdown_filters", "true")
        .set("datafusion.sql_parser.enable_ident_normalization", "false");

    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime));

    // Register custom UDFs in deterministic order
    register_cpg_scalar_udfs(&ctx)?;
    register_cpg_aggregate_udfs(&ctx)?;
    register_cpg_window_udfs(&ctx)?;

    Ok(ctx)
}
```

**Key config choices:**

| Knob | Value | Rationale |
|------|-------|-----------|
| `target_partitions` | From environment profile | Controls in-query parallelism |
| `repartition_joins` | `true` | Enable hash-repartitioned joins for large tables |
| `repartition_aggregations` | `true` | Multi-partition aggregation |
| `repartition_windows` | `true` | Partitioned window evaluation |
| `parquet_pruning` | `true` | Row-group pruning via Parquet statistics |
| `pushdown_filters` | `true` | Push predicates into Parquet scan |
| `enable_ident_normalization` | `false` | Preserve mixed-case column names from Delta schemas |
| `information_schema` | `true` | Enable metadata introspection for envelope capture |

### 3.3 Join and Combination Config Knobs

Beyond the baseline session config, these knobs directly control how combined
plan DAGs behave at the join, union, and sort layers:

| Knob | Value | Rationale |
|------|-------|-----------|
| `prefer_hash_join` | `true` | Hash join default; override to `false` for memory-constrained envs |
| `hash_join_inlist_pushdown_max_size` | `1024` | Push build-side values as IN-list into probe scan (enables bloom filter usage) |
| `hash_join_inlist_pushdown_max_distinct_values` | `64` | Cap distinct values in IN-list pushdown (memory per partition) |
| `enable_dynamic_filter_pushdown` | `true` | Runtime-derived filters from join probes prune scans |
| `enable_topk_dynamic_filter_pushdown` | `true` | TopK-aware filter pushdown (ORDER BY + LIMIT → early scan termination) |
| `enable_join_dynamic_filter_pushdown` | `true` | Join-specific dynamic filters |
| `enable_aggregate_dynamic_filter_pushdown` | `true` | Aggregate-derived dynamic filters |
| `enable_sort_pushdown` | `true` | Push sort requirements into scans when data is pre-sorted |
| `prefer_existing_sort` | `true` | Avoid redundant sort if input already ordered |
| `repartition_sorts` | `true` | Parallel sort across partitions |
| `coalesce_batches` | `true` | Stabilize batch sizes after selective filters/joins |
| `enable_round_robin_repartition` | `true` | Increase parallelism for skewed partitions |
| `planning_concurrency` | From profile | Parallel planning of UNION children (reduces compilation latency for many-input union graphs) |
| `enable_recursive_ctes` | `true` | Enable recursive plan graphs for scope/call-graph traversal |
| `prefer_existing_union` | `false` | Allow optimizer to convert Union → InterleaveExec when partitioning aligns |

### 3.4 Spill and Cache Tuning

| Knob | Value | Rationale |
|------|-------|-----------|
| `spill_compression` | `lz4` | Compress spill files to reduce disk I/O |
| `max_spill_file_size_bytes` | `128MB` | Bound individual spill file size |
| `sort_spill_reservation_bytes` | `64MB` | Pre-reserve memory for sort spill buffers |
| `metadata_cache_limit` | `256MB` | Cache Parquet/Delta metadata across view re-expansions |
| `list_files_cache_limit` | `128MB` | Cache file listings for repeated scan planning |
| `list_files_cache_ttl` | `300s` | TTL for file listing cache entries |

These caches are critical for our architecture because lazy views are
re-planned at each reference point — metadata and file listing caches
amortize the cost across multiple downstream view expansions.

### 3.5 Environment Profiles

```rust
pub enum EnvironmentClass {
    Small,   // < 50 files, < 10K LOC
    Medium,  // 50-500 files, 10K-100K LOC
    Large,   // > 500 files, > 100K LOC
}

pub struct EnvironmentProfile {
    pub target_partitions: u32,  // Small: 4, Medium: 8, Large: 16
    pub batch_size: u32,         // Small: 4096, Medium: 8192, Large: 16384
    pub memory_pool_bytes: u64,  // Small: 512MB, Medium: 2GB, Large: 8GB
    pub spill_paths: Vec<PathBuf>,
}
```

---

## 4) Delta Lake Native Provider Strategy

### 4.1 Provider Registration

Every extraction output is registered as a native Delta provider. No Arrow Dataset
fallback. No Parquet-direct scan.

```rust
use deltalake::delta_datafusion::{DeltaTableProvider, DeltaScanConfig};
use deltalake::DeltaTable;

pub async fn register_extraction_inputs(
    ctx: &SessionContext,
    inputs: &[ExtractionInput],
) -> Result<Vec<TableRegistration>> {
    let mut registrations = Vec::new();

    // Sort inputs for deterministic registration order
    let mut sorted_inputs = inputs.to_vec();
    sorted_inputs.sort_by_key(|i| i.logical_name.clone());

    for input in &sorted_inputs {
        let table = DeltaTable::open(&input.delta_location).await?;

        let scan_config = DeltaScanConfig::new()
            .with_parquet_pushdown(true)
            .with_wrap_partition_values(true);

        // Optional: add file provenance column for lineage-required views
        let scan_config = if input.requires_lineage {
            scan_config.with_file_column_name("__source_file")
        } else {
            scan_config
        };

        let provider = DeltaTableProvider::try_new(
            table.snapshot()?.clone(),
            table.log_store(),
            scan_config,
        )?;

        ctx.register_table(
            &input.logical_name,
            Arc::new(provider),
        )?;

        registrations.push(TableRegistration {
            name: input.logical_name.clone(),
            kind: TableKind::DeltaNative,
            delta_version: table.version(),
            schema_hash: hash_arrow_schema(table.schema()?),
        });
    }

    Ok(registrations)
}
```

### 4.2 DeltaScanConfig Standard

| Knob | Default | Override Condition |
|------|---------|-------------------|
| `enable_parquet_pushdown` | `true` | Never disable in production |
| `wrap_partition_values` | `true` | Disable only if partition decode overhead measured |
| `schema_force_view_types` | `false` | Enable for Utf8View/BinaryView perf experiments |
| `file_column_name` | `None` | Set to `"__source_file"` for lineage-required views |
| `schema` | `None` | Set only for explicit cast normalization at scan boundary |

### 4.3 Two-Tier Pruning

Delta + DataFusion pruning operates in two complementary tiers:

1. **Tier 1 (Delta file-level):** Delta transaction log contains per-file min/max
   statistics. Before DataFusion ever sees a file, Delta skips files whose statistics
   prove they cannot satisfy the predicate. This is free (no I/O to Parquet files).

2. **Tier 2 (Parquet row-group):** For files that survive Tier 1, DataFusion's
   Parquet reader uses row-group statistics and page indexes for further pruning.
   Enabled by `enable_parquet_pushdown: true` in `DeltaScanConfig` and
   `datafusion.execution.parquet.pushdown_filters: true` in session config.

**Combined effect:** Fewer files read (Delta), fewer row groups scanned (Parquet),
fewer rows materialized (predicate evaluation). This replaces any bespoke file
selection or scan policy logic.

### 4.4 Snapshot Management

| Mode | Mechanism | Use Case |
|------|-----------|----------|
| Latest snapshot | `DeltaTable::open(path)` | Default production runs |
| Version-pinned | `DeltaTable::open_with_version(path, version)` | Deterministic replay |
| Time-travel | `DeltaTable::open_at_timestamp(path, ts)` | Point-in-time analysis |

Snapshot selection is a provider-level concern, not a planning concern. The
`DeltaTableProvider` receives a snapshot at construction time; all subsequent
planning and execution sees exactly that snapshot regardless of concurrent writes.

### 4.5 Caching Hierarchy (Replaces `_default_semantic_cache_policy`)

The current `_default_semantic_cache_policy()` at `pipeline.py:262` is replaced by
a three-tier Delta-first caching strategy:

| Tier | What | Scope | Mechanism |
|------|------|-------|-----------|
| 1. Delta snapshot/log | Table membership + schema | Per-run | `DeltaTableProvider` snapshot reuse |
| 2. Parquet metadata | Row-group stats, page indexes | Per-session | DataFusion metadata cache |
| 3. Materialized subplans | Hot intermediate results | Explicit | `DataFrame.cache()` after filter/project |

**Tier 3 (explicit caching):** For subplans reused by multiple downstream views,
the semantic compiler inserts explicit `cache()` boundaries:

```rust
// Subplan used by 3+ downstream views -> cache after projection/filter
let hot_subplan = extraction_df
    .filter(col("kind").eq(lit("function_def")))
    .select_columns(&["qualified_name", "bstart", "bend", "scope_id"])?
    .cache()
    .await?;

// Register as named view for reuse
ctx.register_view("fn_defs_cached", hot_subplan)?;
```

---

## 5) Semantic-to-Relational Compiler

This is the core innovation: translating semantic view specifications into a single
combined LogicalPlan DAG that DataFusion can optimize and execute as one unit.

### 5.1 Input Contract

```rust
pub struct SemanticExecutionSpec {
    /// Canonical relation definitions (extraction inputs)
    pub input_relations: Vec<InputRelation>,

    /// Semantic view definitions (the computation graph)
    pub view_definitions: Vec<ViewDefinition>,

    /// Join graph with inferred key constraints
    pub join_graph: JoinGraph,

    /// Output target contract (CPG tables to materialize)
    pub output_targets: Vec<OutputTarget>,

    /// Spec identity for fingerprinting
    pub spec_hash: [u8; 32],
}

pub struct ViewDefinition {
    pub name: String,
    pub view_kind: ViewKind,
    pub dependencies: Vec<String>,  // names of input relations or other views
    pub transform: ViewTransform,   // the computation to apply
}

pub enum ViewTransform {
    /// SQL expression (parsed by DataFusion)
    Sql(String),
    /// DataFrame-style operations (compiled to LogicalPlan)
    Relational(RelationalOps),
    /// Custom UDF application
    UdfApplication { udf_name: String, args: Vec<String> },
}
```

### 5.2 Compilation Algorithm

The compiler converts the view graph into a single LogicalPlan DAG:

```
Step 1: Topological sort of view dependency graph
Step 2: For each view in topological order:
   a. Build LogicalPlan fragment from ViewTransform
   b. Register as named view in SessionContext (lazy, no execution)
   c. Downstream views reference by name (CTE-like expansion)
Step 3: Build final output plan combining all materialization targets
Step 4: Return single LogicalPlan DAG for optimization
```

**Implementation using three plan construction surfaces:**

#### Surface 1: SQL (for views defined as SQL expressions)

```rust
// View defined as SQL against registered tables/views
let df = ctx.sql(&view_def.sql_text)?;
ctx.register_view(&view_def.name, df)?;
```

Views registered this way are lazy: DataFusion re-expands them at each reference
point during optimization, enabling cross-view predicate pushdown.

#### Surface 2: DataFrame API (for programmatic composition)

```rust
// Join two extraction tables with inferred keys
let left = ctx.table("ast_nodes")?;
let right = ctx.table("symtable_entries")?;

let joined = left.join(
    right,
    JoinType::Inner,
    &["qualified_name"],
    &["qualified_name"],
    None,
)?;

ctx.register_view("ast_sym_joined", joined)?;
```

#### Surface 3: LogicalPlanBuilder (for complex DAG surgery)

```rust
use datafusion::logical_expr::LogicalPlanBuilder;

// Combine multiple extraction sources with union-by-name
let plans: Vec<LogicalPlan> = extraction_sources.iter()
    .map(|s| ctx.table(s).map(|df| df.into_optimized_plan()))
    .collect::<Result<Vec<_>>>()?;

let mut builder = LogicalPlanBuilder::from(plans[0].clone());
for plan in &plans[1..] {
    builder = builder.union_by_name(plan.clone())?;
}

let combined = builder.build()?;
let df = DataFrame::new(ctx.state(), combined);
ctx.register_view("all_extractions_unioned", df)?;
```

**`union_by_name` is critical:** Extraction sources may have different column orders
or optional columns. Rust's `union_by_name` aligns by column name and fills missing
columns with NULL. This handles schema drift without manual alignment.

### 5.3 Join Strategy

The semantic compiler infers join keys from the semantic model (replacing the current
18 hardcoded join keys in `src/relspec/`):

```rust
pub struct JoinGraph {
    pub edges: Vec<JoinEdge>,
}

pub struct JoinEdge {
    pub left_table: String,
    pub right_table: String,
    pub join_type: JoinType,
    pub left_keys: Vec<String>,
    pub right_keys: Vec<String>,
    pub inferred_from: JoinInferenceSource,
}

pub enum JoinInferenceSource {
    /// Byte-span overlap (bstart/bend intersection)
    ByteSpanOverlap,
    /// Shared qualified name
    QualifiedNameEquality,
    /// Shared scope identifier
    ScopeIdEquality,
    /// Semantic type compatibility
    SemanticTypeMatch,
    /// Explicit view spec declaration
    Declared,
}
```

**Join optimization is delegated to DataFusion.** The compiler declares join
predicates; DataFusion chooses the algorithm (hash join, sort-merge join, nested
loop) and the repartitioning strategy based on table statistics and session config.

### 5.4 Output Plan Assembly

The final step combines all materialization targets into a single execution:

```rust
pub async fn build_output_plan(
    ctx: &SessionContext,
    targets: &[OutputTarget],
) -> Result<Vec<(String, DataFrame)>> {
    let mut output_plans = Vec::new();

    for target in targets {
        let df = ctx.table(&target.source_view)?;

        // Apply final projection to match CPG output schema
        let projected = df.select(
            target.output_columns.iter()
                .map(|c| col(c))
                .collect::<Vec<_>>()
        )?;

        output_plans.push((target.output_table.clone(), projected));
    }

    Ok(output_plans)
}
```

Each output plan is executed independently via `execute_stream_partitioned()`,
but all share the same SessionContext (and therefore the same cached subplans
and optimized view expansions).

### 5.5 Parameterized Plan Templates

For repeated query patterns (incremental scans, validation probes, threshold-based
filtering), the compiler emits parameterized plans that reuse a stable LogicalPlan
shape with different scalar values:

**SQL prepared statements (Rust):**

```rust
// Prepare once, execute many times with different thresholds
ctx.sql("PREPARE scan_by_kind(VARCHAR) AS
    SELECT * FROM ast_nodes WHERE kind = $1")?;

// Execute with different parameters
let functions = ctx.sql("EXECUTE scan_by_kind('function_def')")?.collect().await?;
let classes = ctx.sql("EXECUTE scan_by_kind('class_def')")?.collect().await?;
```

**Rust DataFrame parameter binding:**

```rust
// Build plan template with placeholder
let template_df = ctx.sql("SELECT * FROM ast_nodes WHERE bstart > $1 AND bend < $2")?;

// Bind parameters at execution time (stable plan shape, different values)
let result = template_df
    .with_param_values(ParamValues::List(vec![
        ScalarValue::Int64(Some(1000)),
        ScalarValue::Int64(Some(2000)),
    ]))?
    .collect()
    .await?;
```

**Value:** Prepared statements avoid re-parsing and re-analyzing for repeated query
patterns. The logical plan shape is stable, enabling plan fingerprint comparison
across parameter variations.

### 5.6 Subquery Composition

The compiler uses subqueries for complex derivations that don't fit the
join/union model:

```sql
-- Scalar subquery in SELECT: compute per-function metric inline
SELECT
    f.qualified_name,
    f.bstart,
    f.bend,
    (SELECT COUNT(*) FROM ast_calls c
     WHERE c.caller_qualified_name = f.qualified_name) AS call_count
FROM ast_functions f;

-- Correlated EXISTS: find functions that have at least one decorator
SELECT f.*
FROM ast_functions f
WHERE EXISTS (
    SELECT 1 FROM ast_decorators d
    WHERE d.target_qualified_name = f.qualified_name
);

-- Derived table (FROM subquery): intermediate aggregation
SELECT scope_id, total_complexity
FROM (
    SELECT scope_id, SUM(complexity) AS total_complexity
    FROM ast_nodes
    GROUP BY scope_id
) sub
WHERE total_complexity > 10;
```

**Watchout:** DataFusion only supports correlated subqueries for EXISTS/NOT EXISTS.
The optimizer rewrites correlated subqueries to joins when possible. LATERAL joins
are not yet supported — use explicit joins or CTEs instead.

### 5.7 Recursive CTEs for Graph Traversal

CPG construction involves inherently recursive structures: scope nesting, call
chains, import resolution. Recursive CTEs express these as plan-native operations
without host-language loops:

```rust
// Build recursive scope hierarchy using LogicalPlanBuilder
let base_case = LogicalPlanBuilder::scan("scopes", scope_source, None)?
    .filter(col("parent_scope_id").is_null())?  // root scopes
    .build()?;

let recursive_term = LogicalPlanBuilder::scan("scopes", scope_source, None)?
    .join(
        LogicalPlanBuilder::scan("scope_hierarchy", /* recursive ref */, None)?.build()?,
        JoinType::Inner,
        (vec![col("parent_scope_id")], vec![col("scope_id")]),
        None,
    )?
    .build()?;

let recursive_plan = LogicalPlanBuilder::from(base_case)
    .to_recursive_query("scope_hierarchy", recursive_term, false)?  // UNION ALL
    .build()?;
```

**SQL equivalent:**

```sql
WITH RECURSIVE scope_hierarchy AS (
    -- Base case: root scopes
    SELECT scope_id, qualified_name, 0 AS depth
    FROM scopes
    WHERE parent_scope_id IS NULL

    UNION ALL

    -- Recursive step: child scopes
    SELECT s.scope_id, s.qualified_name, h.depth + 1
    FROM scopes s
    JOIN scope_hierarchy h ON s.parent_scope_id = h.scope_id
)
SELECT * FROM scope_hierarchy;
```

**Requires:** `SET datafusion.execution.enable_recursive_ctes = true` (set in
session factory).

**Use cases in CPG construction:**
- Scope nesting hierarchy (module → class → function → nested function)
- Call graph transitive closure
- Import chain resolution
- Type inheritance traversal

### 5.8 Inequality Joins for Byte-Span Operations

Many CPG relationships involve range predicates on byte spans. The compiler uses
`join_on` with inequality predicates (not just equality keys):

```rust
// Find all AST nodes contained within a scope's byte span
let contained = scopes.join_on(
    ast_nodes,
    JoinType::Inner,
    [
        col("scopes.bstart").lt_eq(col("ast_nodes.bstart")),
        col("scopes.bend").gt_eq(col("ast_nodes.bend")),
        col("scopes.file_path").eq(col("ast_nodes.file_path")),
    ],
)?;
```

**Physical implications:** Inequality predicates prevent hash join; DataFusion
selects SortMergeJoin or NestedLoopJoin. The `ByteSpanOverlapOptimizer` custom
rule (WS9) can rewrite these into more efficient patterns when file_path
equality enables partition-local evaluation.

**Join algorithm selection by predicate type:**

| Predicate | Physical Join | Memory | Notes |
|-----------|---------------|--------|-------|
| Equality only | HashJoinExec | High (build side in memory) | Fastest for equality |
| Equality + inequality | SortMergeJoinExec | Lower (streaming) | Good for range predicates |
| Inequality only | NestedLoopJoinExec | Depends on filter selectivity | Fallback; avoid for large inputs |
| Cross join + filter | CrossJoinExec + FilterExec | Cartesian product risk | Only when unavoidable |

### 5.9 Struct Coercion in Plan Combination

When combining extraction sources that produce nested struct columns (e.g.,
metadata bundles, property maps), DataFusion applies name-based struct field
mapping:

```rust
// Union two sources with different struct field orders
// DataFusion matches fields by NAME, not position
let source_a = ctx.sql("SELECT named_struct('kind', kind, 'line', line_no) AS meta FROM src_a")?;
let source_b = ctx.sql("SELECT named_struct('line', line_no, 'kind', kind) AS meta FROM src_b")?;

// union_by_name handles top-level column alignment;
// struct coercion handles nested field alignment
let combined = source_a.union_by_name(source_b)?;
```

**Missing fields are filled with NULL** when casting to a unified schema:

```sql
-- Source A has {kind, line, col}, Source B has {kind, line}
-- After union, Source B rows get col = NULL
SELECT CAST(meta AS STRUCT(kind VARCHAR, line INT, col INT)) ...
```

**Verification:** Use `arrow_typeof(meta)` to confirm unified struct types after
combination. Schema mismatches in nested structs are a common source of silent
data corruption in union pipelines.

### 5.10 UNNEST for Nested Data Expansion

Extraction outputs may contain list-valued columns (e.g., decorator lists,
parameter lists, base classes). The compiler uses UNNEST to expand these into
flat relational form inside the plan graph:

```sql
-- Expand list-valued decorator column into one row per decorator
SELECT
    f.qualified_name,
    unnest(f.decorators) AS decorator
FROM ast_functions f;

-- Expand struct column into individual columns
SELECT unnest(metadata) FROM extraction_output;
```

**Watchout:** Struct unnest uses placeholder-prefixed column names. Always follow
with explicit `SELECT ... AS ...` rename to stable identifiers before downstream
joins or unions.

---

## 6) DataFusion Optimizer Utilization (The Auto-Optimization Engine)

This is where the "seamlessly auto-optimizing" requirement is met. Rather than
building a custom optimizer, we configure and leverage DataFusion's built-in
optimization pipeline.

### 6.1 Built-In Optimizer Rules (Enabled by Default)

DataFusion ships approximately 30+ logical optimizer rules. The critical ones for
our workload:

| Rule | Effect | CPG Relevance |
|------|--------|---------------|
| **Predicate pushdown** | Push filters below joins/projections into scans | Reduces extraction table I/O |
| **Projection pushdown** | Eliminate unused columns early | Extraction tables are wide; only need subset |
| **Join reordering** | Optimal join order based on statistics | Multi-source extraction joins |
| **Common subexpression elimination** | Share identical subplan computations | Shared extraction subplans |
| **Constant folding/simplification** | Evaluate constant expressions at plan time | Simplify semantic type checks |
| **Filter/limit pushdown** | Push limits/filters through operators | Early row reduction |
| **Aggregate pushdown** | Push aggregates below joins when safe | CPG property aggregation |
| **Sort pushdown** | Push sort requirements into scans | Ordered extraction outputs |
| **Dynamic filter pushdown** | Runtime-generated filters from join probes | Cross-extraction filtering |

### 6.2 Custom Optimizer Rules (Rust Extension Points)

For CPG-specific optimizations, we inject custom rules via the SessionContext:

```rust
// Analyzer rule: semantic type coercion
ctx.add_analyzer_rule(Arc::new(SemanticTypeCoercionRule::new()));

// Optimizer rule: byte-span overlap predicate rewrite
ctx.add_optimizer_rule(Arc::new(ByteSpanOverlapOptimizer::new()));

// Physical optimizer rule: CPG-aware repartitioning
ctx.state().add_physical_optimizer_rule(
    Arc::new(CpgRepartitionRule::new())
);
```

**Specific custom rules to implement:**

1. **`SemanticTypeCoercionRule`** (AnalyzerRule): Ensure semantic type annotations
   are preserved through plan transformations. Insert explicit `arrow_cast` nodes
   where heterogeneous extraction sources produce type mismatches.

2. **`ByteSpanOverlapOptimizer`** (OptimizerRule): Rewrite byte-span overlap
   predicates (`a.bstart <= b.bend AND a.bend >= b.bstart`) into optimized
   range-join patterns that DataFusion can execute efficiently.

3. **`CpgRepartitionRule`** (PhysicalOptimizerRule): Override default repartitioning
   for CPG-specific access patterns (e.g., repartition by `file_path` for
   file-local graph construction).

### 6.3 Optimizer Debugging

`EXPLAIN VERBOSE` shows the effect of each optimizer rule in sequence:

```sql
EXPLAIN VERBOSE
SELECT n.qualified_name, e.edge_type, e.target
FROM cpg_nodes n
JOIN cpg_edges e ON n.node_id = e.source_id
WHERE n.kind = 'function_def';
```

This produces rule-by-rule plan deltas, making optimization behavior transparent
and debuggable. Store `EXPLAIN VERBOSE` output in the plan bundle for regression
detection.

### 6.4 Optimizer Toggle Controls

For experimentation and regression isolation, individual optimizer rules can be
removed:

```rust
// Disable a specific optimizer rule for A/B testing
ctx.remove_optimizer_rule("push_down_filter");
```

This is the mechanism for testing whether a custom rule improves or regresses
specific workloads.

### 6.5 TreeNode API for Plan Normalization

Before handing a composed LogicalPlan to the optimizer, the semantic compiler
applies deterministic normalization passes using DataFusion's TreeNode API:

```rust
use datafusion::common::tree_node::{TreeNode, Transformed};

/// Normalize all join key expressions to a canonical form
/// (left key alphabetically < right key)
fn canonicalize_join_keys(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_down(|node| {
        match node {
            LogicalPlan::Join(join) if needs_key_swap(&join) => {
                let normalized = swap_join_keys(join);
                Ok(Transformed::yes(LogicalPlan::Join(normalized)))
            }
            _ => Ok(Transformed::no(node)),
        }
    })
    .map(|t| t.data)
}

/// Strip redundant sorts that the compiler may have inserted conservatively
fn strip_redundant_sorts(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up(|node| {
        match node {
            LogicalPlan::Sort(sort) if child_already_sorted(&sort) => {
                Ok(Transformed::yes(sort.input.as_ref().clone()))
            }
            _ => Ok(Transformed::no(node)),
        }
    })
    .map(|t| t.data)
}
```

**TreeNode methods available:**

| Method | Direction | Use Case |
|--------|-----------|----------|
| `transform_down` | Root → leaves | Inject safety filters, canonicalize expressions |
| `transform_up` | Leaves → root | Strip redundant operators, merge adjacent projections |
| `transform_down_up` | Both passes | Complex rewrites needing both pre/post context |
| `apply` | Visitor (no mutation) | Plan analysis, metric collection |
| `exists` | Short-circuit visitor | Check if plan contains a specific pattern |
| `rewrite` | Controlled mutation | Full rewrite with explicit "stop/continue" control |

**Rule:** Use `transform_*` APIs instead of manual recursion — they avoid
unnecessary cloning and handle the `Transformed::yes/no` tracking correctly.

### 6.6 Plan-Time Acceleration via Optimizer Rules

Custom optimizer rules can rewrite expensive subgraphs into precomputed scans
— effectively implementing semantic caching at the plan level:

```rust
/// Optimizer rule that detects repeated extraction scans with identical
/// predicates and replaces them with a single cached scan reference.
impl OptimizerRule for SharedScanOptimizer {
    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Find identical TableScan + Filter subtrees
        // Replace duplicates with SubqueryAlias referencing first occurrence
        // This turns N scans of the same table with the same filter into 1
        detect_and_merge_shared_scans(plan)
    }
}
```

This is the engine-native replacement for our current Python-side
`_default_semantic_cache_policy()` — the optimizer identifies reuse
opportunities automatically.

### 6.7 Custom SQL Syntax Extension Points

For domain-specific constructs that the semantic compiler must express in SQL,
DataFusion provides planner extension hooks:

```rust
// ExprPlanner: custom expression syntax
ctx.register_expr_planner(Arc::new(CpgExprPlanner::new()));

// RelationPlanner: custom FROM-clause syntax
ctx.register_relation_planner(Arc::new(CpgRelationPlanner::new()));
```

**Potential use:** A custom `RelationPlanner` could parse
`FROM cpg_traverse('calls', depth=3)` directly in SQL, compiling to a
recursive CTE plan without requiring the UDTF registration path.

**Watchout:** Planner precedence is "last registered wins." Return
`Original(...)` to delegate to the default planner when the custom
planner doesn't handle the construct.

---

## 7) Custom Rust UDFs for CPG Operations

### 7.1 Scalar UDFs

CPG construction requires domain-specific scalar operations that don't exist in
standard SQL. These are implemented as Rust `ScalarUDFImpl` for maximum performance.

#### `byte_span_contains(outer_start, outer_end, inner_start, inner_end) -> bool`

```rust
impl ScalarUDFImpl for ByteSpanContains {
    fn name(&self) -> &str { "byte_span_contains" }

    fn signature(&self) -> &Signature {
        Signature::exact(
            vec![DataType::Int64, DataType::Int64, DataType::Int64, DataType::Int64],
            Volatility::Immutable,
        )
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Columnar fast path using Arrow compute kernels
        let outer_start = args.args[0].as_int64_array()?;
        let outer_end = args.args[1].as_int64_array()?;
        let inner_start = args.args[2].as_int64_array()?;
        let inner_end = args.args[3].as_int64_array()?;

        let result: BooleanArray = outer_start.iter()
            .zip(outer_end.iter())
            .zip(inner_start.iter())
            .zip(inner_end.iter())
            .map(|(((os, oe), is), ie)| {
                match (os, oe, is, ie) {
                    (Some(os), Some(oe), Some(is), Some(ie)) =>
                        Some(os <= is && ie <= oe),
                    _ => None,
                }
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(result)))
    }
}
```

#### Additional scalar UDFs

| UDF | Signature | Purpose |
|-----|-----------|---------|
| `byte_span_contains` | `(i64, i64, i64, i64) -> bool` | Containment test for span nesting |
| `byte_span_overlaps` | `(i64, i64, i64, i64) -> bool` | Overlap test for span intersection |
| `qualified_name_depth` | `(utf8) -> i32` | Depth of dotted qualified name |
| `qualified_name_parent` | `(utf8) -> utf8` | Parent scope of qualified name |
| `qualified_name_leaf` | `(utf8) -> utf8` | Leaf identifier of qualified name |
| `semantic_type_compat` | `(utf8, utf8) -> bool` | Semantic type compatibility check |
| `stable_hash_id` | `(utf8, utf8, i64, i64) -> binary` | Deterministic node/edge ID generation |

### 7.2 Aggregate UDFs

```rust
impl AggregateUDFImpl for SpanMerge {
    fn name(&self) -> &str { "span_merge" }

    fn accumulator(&self, _: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SpanMergeAccumulator::new()))
    }

    fn groups_accumulator_supported(&self, _: AccumulatorArgs) -> bool {
        true  // Enable vectorized group-by path
    }

    fn create_groups_accumulator(&self, _: AccumulatorArgs)
        -> Result<Box<dyn GroupsAccumulator>>
    {
        Ok(Box::new(SpanMergeGroupsAccumulator::new()))
    }
}
```

| UDAF | Signature | Purpose |
|------|-----------|---------|
| `span_merge` | `(i64, i64) -> struct{i64, i64}` | Merge overlapping byte spans |
| `evidence_consensus` | `(utf8[]) -> struct{utf8, f64}` | Multi-source evidence agreement score |
| `scope_tree_agg` | `(utf8, utf8, i64) -> list<struct>` | Build scope tree from flat entries |

### 7.3 Window UDFs

| UDWF | Purpose |
|------|---------|
| `span_rank` | Rank spans by containment depth within partition |
| `scope_level` | Assign scope nesting level based on byte-span ordering |

### 7.4 Table Functions (UDTFs)

```rust
// UDTF: generate CPG edges from a set of matched relationships
ctx.register_udtf("emit_cpg_edges", Arc::new(EmitCpgEdges::new()));

// Usage in SQL:
// SELECT * FROM emit_cpg_edges('calls', 'ast_calls_v1')
```

### 7.5 Performance Patterns

All UDFs follow these performance rules:

1. **Columnar fast path:** Always implement `invoke_with_args` using Arrow array
   operations, never row-at-a-time.
2. **GroupsAccumulator:** All aggregate UDFs implement `GroupsAccumulator` for
   vectorized group-by execution.
3. **Immutable volatility:** Mark all deterministic UDFs as `Volatility::Immutable`
   to enable constant folding and plan-time evaluation.
4. **Named arguments:** Use `Signature::with_parameter_names` for self-documenting
   SQL call sites.

### 7.6 UDF Optimizer Hooks

Scalar UDFs can participate in optimization:

```rust
impl ScalarUDFImpl for ByteSpanContains {
    // Tell the optimizer this function short-circuits on NULL
    fn short_circuits(&self) -> bool { true }

    // Simplify at plan time when inputs are constant
    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        // If all args are literals, evaluate at plan time
        if args.iter().all(|a| matches!(a, Expr::Literal(_))) {
            let result = self.evaluate_constants(&args)?;
            Ok(ExprSimplifyResult::Simplified(Expr::Literal(result)))
        } else {
            Ok(ExprSimplifyResult::Original(args))
        }
    }
}
```

---

## 8) Plan Artifact Bundle (Rich Diagnostics)

### 8.1 Multi-Format Plan Capture

Every execution captures a complete plan bundle in multiple formats optimized
for different consumers (human review, CI diffing, tooling ingestion):

```rust
pub struct PlanBundle {
    /// P0: Unoptimized logical plan (after parse + analyze)
    pub p0_logical: String,           // display_indent_schema format
    pub p0_graphviz: String,          // DOT format for visual rendering

    /// P1: Optimized logical plan (after all OptimizerRules)
    pub p1_optimized: String,         // display_indent_schema format
    pub p1_graphviz: String,          // DOT format for visual rendering

    /// P2: Physical plan (ExecutionPlan after physical optimization)
    pub p2_physical: String,          // DisplayableExecutionPlan indent format
    pub p2_partition_count: usize,    // Parallelism topology

    /// EXPLAIN VERBOSE output (rule-by-rule optimization trace)
    pub explain_verbose: String,

    /// EXPLAIN FORMAT TREE output (diff-friendly rendering)
    pub explain_tree: String,

    /// EXPLAIN FORMAT GRAPHVIZ (machine-ingestible for plan diffing tooling)
    pub explain_graphviz: String,

    /// Plan fingerprints for regression detection
    pub p0_hash: [u8; 32],
    pub p1_hash: [u8; 32],
    pub p2_hash: [u8; 32],

    /// Session envelope at plan time
    pub session_envelope: SessionEnvelope,

    /// Timestamp
    pub captured_at: DateTime<Utc>,
}
```

**EXPLAIN config knobs** set on the session before capture:

| Config | Value | Purpose |
|--------|-------|---------|
| `datafusion.explain.format` | `indent` | Default for EXPLAIN (also tree/pgjson/graphviz) |
| `datafusion.explain.show_schema` | `true` | Include schema at each plan node |
| `datafusion.explain.show_statistics` | `true` | Include estimated row counts |
| `datafusion.explain.show_sizes` | `true` | Include estimated byte sizes |
| `datafusion.explain.analyze_level` | `verbose` | Per-partition metrics detail in EXPLAIN ANALYZE |
| `datafusion.explain.logical_plan_only` | `false` | Both logical and physical (don't restrict) |
| `datafusion.explain.physical_plan_only` | `false` | Both logical and physical (don't restrict) |

**Graphviz rendering** uses `LogicalPlan::display_graphviz()` which produces DOT
format directly from the plan object — no SQL EXPLAIN parsing required:

```rust
let p0_graphviz = format!("{}", p0.display_graphviz());
let p1_graphviz = format!("{}", p1.display_graphviz());
```

### 8.2 Capture Implementation

```rust
pub async fn capture_plan_bundle(
    ctx: &SessionContext,
    df: &DataFrame,
    envelope: &SessionEnvelope,
) -> Result<PlanBundle> {
    // P0: unoptimized logical
    let p0 = df.logical_plan();
    let p0_text = format!("{}", p0.display_indent_schema());

    // P1: optimized logical
    let p1 = df.optimized_logical_plan()?;
    let p1_text = format!("{}", p1.display_indent_schema());

    // P2: physical (without executing)
    let p2 = df.execution_plan().await?;
    let p2_text = format!("{}", DisplayableExecutionPlan::indent(&*p2, true));

    // EXPLAIN VERBOSE (rule-by-rule trace, non-executing)
    let explain_verbose = {
        let explain_df = ctx.sql(&format!(
            "EXPLAIN VERBOSE {}",
            reconstruct_sql_from_plan(&p0)?
        ))?;
        format_explain_output(explain_df.collect().await?)
    };

    // EXPLAIN TREE (diff-friendly)
    let explain_tree = {
        let tree_df = ctx.sql(&format!(
            "EXPLAIN FORMAT TREE {}",
            reconstruct_sql_from_plan(&p0)?
        ))?;
        format_explain_output(tree_df.collect().await?)
    };

    Ok(PlanBundle {
        p0_logical: p0_text.clone(),
        p1_optimized: p1_text.clone(),
        p2_physical: p2_text.clone(),
        explain_verbose,
        explain_tree,
        p0_hash: hash_string(&p0_text),
        p1_hash: hash_string(&p1_text),
        p2_hash: hash_string(&p2_text),
        session_envelope: envelope.clone(),
        captured_at: Utc::now(),
    })
}
```

### 8.3 Regression Detection

Plan regression detection operates on P1 (semantic-preserving rewrites):

```rust
pub fn detect_plan_regression(
    current: &PlanBundle,
    baseline: &PlanBundle,
) -> PlanRegressionReport {
    PlanRegressionReport {
        // P1 change indicates optimizer behavior shifted
        p1_changed: current.p1_hash != baseline.p1_hash,

        // P2 change is expected across environments (hardware-dependent)
        p2_changed: current.p2_hash != baseline.p2_hash,

        // Config drift detection
        config_drift: diff_session_envelopes(
            &current.session_envelope,
            &baseline.session_envelope,
        ),

        // Severity: P1 change without config change = unexpected regression
        severity: if current.p1_hash != baseline.p1_hash
            && current.session_envelope.config_snapshot
                == baseline.session_envelope.config_snapshot
        {
            RegressionSeverity::Unexpected
        } else {
            RegressionSeverity::Expected
        },
    }
}
```

### 8.4 Execution Metrics (Post-Run)

After execution, capture runtime metrics via both `EXPLAIN ANALYZE` (text) and
the programmatic `ExecutionPlan::metrics()` API (structured):

```rust
pub struct ExecutionMetrics {
    /// EXPLAIN ANALYZE VERBOSE output (per-partition detail)
    pub explain_analyze: String,

    /// Programmatic per-operator metrics from ExecutionPlan::metrics()
    pub operator_metrics: Vec<OperatorMetric>,

    /// Per-partition statistics from ExecutionPlan::partition_statistics()
    pub partition_stats: Vec<PartitionStatistics>,

    /// Total execution time
    pub wall_time: Duration,

    /// Peak memory usage
    pub peak_memory_bytes: u64,

    /// Rows read / rows produced ratio (scan efficiency)
    pub scan_efficiency: f64,

    /// Files pruned by Delta (Tier 1) and Parquet (Tier 2)
    pub files_pruned_delta: u64,
    pub row_groups_pruned_parquet: u64,

    /// Spill metrics
    pub spill_count: u64,
    pub spill_bytes: u64,
}

pub struct OperatorMetric {
    pub operator_name: String,
    pub elapsed_compute: Duration,
    pub output_rows: u64,
    pub output_batches: u64,
    pub spilled_bytes: u64,
}
```

**Programmatic metric extraction** (Rust, after execution):

```rust
fn extract_metrics(plan: &dyn ExecutionPlan) -> Vec<OperatorMetric> {
    let mut metrics = Vec::new();
    collect_plan_metrics(plan, &mut metrics);
    metrics
}

fn collect_plan_metrics(plan: &dyn ExecutionPlan, out: &mut Vec<OperatorMetric>) {
    if let Some(metric_set) = plan.metrics() {
        out.push(OperatorMetric {
            operator_name: plan.name().to_string(),
            elapsed_compute: metric_set.elapsed_compute().unwrap_or_default(),
            output_rows: metric_set.output_rows().unwrap_or(0),
            output_batches: metric_set.sum_by_name("output_batches").map(|m| m.as_usize() as u64).unwrap_or(0),
            spilled_bytes: metric_set.sum_by_name("spill_count").map(|m| m.as_usize() as u64).unwrap_or(0),
        });
    }
    for child in plan.children() {
        collect_plan_metrics(child.as_ref(), out);
    }
}
```

This is strictly better than parsing EXPLAIN ANALYZE text — it provides typed,
structured access to per-operator metrics for automated regression detection.

---

## 9) Execution and Materialization

### 9.1 Stream-First Execution

All execution uses streaming. No `collect()` for production workloads.

```rust
pub async fn execute_and_materialize(
    ctx: &SessionContext,
    output_plans: Vec<(String, DataFrame)>,
    output_config: &OutputConfig,
) -> Result<Vec<MaterializationResult>> {
    let mut results = Vec::new();

    for (table_name, df) in output_plans {
        // Execute as partitioned streams
        let plan = df.execution_plan().await?;
        let partition_count = plan.output_partitioning().partition_count();

        // Stream into Delta table via insert_into
        let target_table = open_or_create_delta_table(
            &output_config.output_path,
            &table_name,
            plan.schema(),
        ).await?;

        // Register target as writable provider
        let target_provider = DeltaTableProvider::try_new(
            target_table.snapshot()?.clone(),
            target_table.log_store(),
            DeltaScanConfig::new(),
        )?;

        ctx.register_table(&format!("__output_{table_name}"), Arc::new(target_provider))?;

        // Execute INSERT INTO (streaming, partitioned)
        let insert_df = ctx.sql(&format!(
            "INSERT INTO __output_{table_name} SELECT * FROM {table_name}"
        ))?;
        let insert_results = insert_df.collect().await?;

        results.push(MaterializationResult {
            table_name,
            delta_version: target_table.version() + 1,
            rows_written: extract_row_count(&insert_results),
            partition_count: partition_count as u32,
        });
    }

    Ok(results)
}
```

### 9.2 Schema Enforcement on Write

Delta's `DeltaDataChecker` enforces schema constraints using embedded DataFusion
expression evaluation:

```rust
// DeltaDataChecker validates:
// 1. Schema compatibility (column names, types, nullability)
// 2. CHECK constraints (SQL expressions evaluated per-batch)
// 3. NOT NULL invariants

// Schema enforcement happens automatically through DeltaTableProvider::insert_into
// No additional application-level validation needed
```

### 9.3 Write Modes

| Mode | DeltaLake API | Use Case |
|------|---------------|----------|
| Append | `InsertOp::Append` | Incremental CPG updates |
| Overwrite | `InsertOp::Overwrite` | Full CPG rebuild |
| Schema merge | `schema_mode=merge` | Additive schema evolution |

### 9.4 Alternative Materialization Paths

Beyond `INSERT INTO`, two additional materialization surfaces are available:

**`DataFrame::write_table` (Rust):** Direct terminal operation that executes
and persists in one call:

```rust
let df = ctx.table("cpg_nodes_view")?;
df.write_table("cpg_nodes_output", DataFrameWriteOptions::new()).await?;
```

**`COPY TO` with partitioned output:** For hive-style partitioned Delta output:

```sql
COPY (SELECT * FROM cpg_edges_view)
TO 'delta://output/cpg_edges'
STORED AS PARQUET
PARTITIONED BY (edge_type)
OPTIONS (
    'execution.keep_partition_by_columns' 'true'
);
```

**CTAS for intermediate cache boundaries:** When the optimizer's view expansion
creates redundant scans, materialize as a named in-memory table:

```sql
CREATE TABLE cached_functions AS
SELECT * FROM ast_functions
WHERE kind = 'function_def';
```

This is a plan-level cache boundary: downstream joins against `cached_functions`
read from materialized data, not re-expanded view plans. Use for subplans
referenced by 5+ downstream views.

### 9.5 Delta Write Tuning

Control output file characteristics for downstream read performance:

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `target_file_size` | `128MB` | Keep files in the sweet spot for Parquet readers |
| `writer_properties.compression` | `ZSTD(3)` | Balance compression ratio vs write speed |
| `writer_properties.max_row_group_size` | `1048576` | Row group size for statistics granularity |
| Column bloom filters | Enabled for `qualified_name`, `node_id` | Accelerate point lookups in downstream queries |
| Column statistics | Enabled for `bstart`, `bend`, `kind` | Enable Delta file-level and Parquet row-group pruning |

**Partial overwrite with `replaceWhere`:**

For targeted CPG updates (e.g., only re-emit edges for changed files):

```rust
use deltalake::operations::DeltaOps;

DeltaOps(output_table)
    .write(updated_batches)
    .with_save_mode(SaveMode::Overwrite)
    .with_replace_where(col("file_path").eq(lit("changed_file.py")))
    .await?;
```

This replaces only the rows matching the predicate — the incoming data must
satisfy the predicate or the operation fails.

### 9.6 Compact Result Envelope

```rust
pub struct RunResult {
    /// Unique run identifier
    pub run_id: Uuid,

    /// Semantic spec that was executed
    pub spec_hash: [u8; 32],

    /// Session envelope fingerprint
    pub envelope_hash: [u8; 32],

    /// Output table locations and versions
    pub outputs: Vec<MaterializationResult>,

    /// Plan bundle (P0/P1/P2 + explains)
    pub plan_bundle: PlanBundle,

    /// Execution metrics (post-run)
    pub metrics: ExecutionMetrics,

    /// Wall-clock timing
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
}
```

---

## 10) Change Management

### 10.1 Plan Fingerprinting

Every semantic spec produces a deterministic plan fingerprint chain:

```
spec_hash → P0 hash → P1 hash → P2 hash (environment-dependent)
```

**Invariant:** Same `spec_hash` + same `config_snapshot` + same Delta versions
must produce identical `P1 hash` (optimized logical plan). Violations indicate
engine bugs or non-deterministic UDF behavior.

### 10.2 Incremental Execution via Delta Versioning

Delta's transaction log provides natural change tracking:

```rust
pub async fn detect_input_changes(
    current_versions: &BTreeMap<String, i64>,
    previous_versions: &BTreeMap<String, i64>,
) -> Vec<ChangedInput> {
    current_versions.iter()
        .filter_map(|(name, version)| {
            let prev = previous_versions.get(name).copied().unwrap_or(-1);
            if *version != prev {
                Some(ChangedInput {
                    name: name.clone(),
                    previous_version: prev,
                    current_version: *version,
                })
            } else {
                None
            }
        })
        .collect()
}
```

When only a subset of extraction inputs have changed, the semantic compiler can
mark unchanged subplans for skip-execution (the plan is still compiled for
fingerprint validation, but execution is short-circuited).

### 10.3 Change Data Feed for Incremental CPG Updates

For incremental updates, use Delta's CDF provider:

```rust
use deltalake::delta_datafusion::DeltaCdfTableProvider;

// Register CDF provider for changed extraction data
let cdf_builder = CdfLoadBuilder::new(table.log_store(), table.snapshot()?)
    .with_start_version(previous_version + 1);

let cdf_provider = DeltaCdfTableProvider::try_new(cdf_builder)?;
ctx.register_table("extraction_changes", Arc::new(cdf_provider))?;

// Query only changed rows
let changes = ctx.sql(
    "SELECT * FROM extraction_changes WHERE _change_type IN ('insert', 'update_postimage')"
)?;
```

CDF provides `_change_type`, `_commit_version`, and `_commit_timestamp` columns
automatically, enabling precise incremental processing.

### 10.4 Delta File Inventory for Incremental Scan Shaping

Beyond version-level change detection, delta-rs exposes file-level inventory
via `get_add_actions()`, enabling file-subset scans for surgical recompute:

```rust
// Get current file inventory from Delta log
let add_actions = delta_table.get_add_actions()?;

// Filter to only files modified after the last run
let changed_files: Vec<Add> = add_actions.iter()
    .filter(|a| a.modification_time > last_run_timestamp)
    .cloned()
    .collect();

// Register provider restricted to changed files only
let provider = DeltaTableProvider::try_new(
    delta_table.snapshot()?.clone(),
    delta_table.log_store(),
    DeltaScanConfig::new(),
)?.with_files(changed_files);

ctx.register_table("extraction_changed_files", Arc::new(provider))?;
```

**Treat file lists as snapshot-bound artifacts** — never reuse across Delta
versions without a stable snapshot key.

### 10.5 Column Mapping Hazard

Delta Lake supports "column mapping" where logical column names can differ
from physical Parquet column names (enabling rename/drop without file rewrite).

**Hazard for plan combination:** Unions/joins across Delta versions with
different column mappings can mis-resolve columns silently. Mitigation:

1. Pin delta-rs versions tightly
2. Validate schemas via `SHOW COLUMNS` / `information_schema.columns` before
   combining tables across versions
3. Prefer `arrow_typeof()` spot-checks for nested struct columns after combination

### 10.6 Schema Evolution

Schema evolution is Delta-managed:

| Evolution Type | Mechanism | Automatic? |
|----------------|-----------|------------|
| Column addition | `schema_mode=merge` on write | Yes (additive) |
| Column removal | Requires explicit `overwrite_schema=true` | No (breaking) |
| Type widening | Cast normalization at scan boundary | Semi-automatic |
| Nested field changes | `schema_mode=merge` handles struct field addition | Yes (additive) |

**Detection:** Compare `arrow_typeof()` of provider schema against stored schema
hash. Any mismatch triggers a schema evolution event in the run result.

---

## 11) Concurrency and Parallelism

### 11.1 In-Query Parallelism (DataFusion-Native)

DataFusion provides automatic in-query parallelism through three mechanisms:

1. **`target_partitions`:** Controls the number of output partitions for scan
   and repartition operators. Set from environment profile.

2. **Repartition operators:** When `repartition_joins`/`repartition_aggregations`
   are enabled, DataFusion inserts `RepartitionExec` nodes to redistribute data
   across partitions for parallel execution.

3. **Partitioned scans:** Delta providers expose file groups as scan partitions;
   DataFusion schedules these across `target_partitions` threads.

**Key insight:** We do not build a task graph or schedule tasks. DataFusion's
physical planner determines the partition topology, and the Tokio runtime
schedules partition execution across threads. This is strictly superior to
Python-side scheduling because:

- No GIL contention (pure Rust execution)
- No serialization overhead between tasks
- Optimizer-aware partition placement (colocated joins, sort-preserving repartition)

### 11.2 Physical Union Semantics

When the compiler unions multiple extraction sources, the physical layer has two
options that directly affect partition topology and downstream performance:

**UnionExec (default):** Concatenates partitions from all inputs. If input A has
4 partitions and input B has 4 partitions, the output has 8 partitions. This
preserves per-partition sort order but can explode partition count in
many-input union graphs.

**InterleaveExec:** Interleaves streams when all inputs share the same
hash-partitioning scheme. Maintains the original partition count and preserves
hash-partitioning locality for downstream joins.

| Operator | Output Partitions | Preserves Hash Partitioning | When Used |
|----------|-------------------|----------------------------|-----------|
| `UnionExec` | Sum of inputs | No | Default; any schema match |
| `InterleaveExec` | Same as inputs | Yes | All inputs hash-partitioned identically |

**Config:** `datafusion.optimizer.prefer_existing_union = false` allows the
optimizer to convert `Union` → `InterleaveExec` when partitioning aligns.
Set to `true` to prevent this conversion (useful for debugging partition
topology changes).

### 11.3 Pre-Positioned Repartitioning

The semantic compiler can insert explicit repartition operators to pre-position
data for downstream joins, avoiding redundant shuffles:

```rust
// Pre-hash extraction data on join keys before the join node
let ast_nodes = ctx.table("ast_nodes")?
    .repartition_by_hash(vec![col("qualified_name")], profile.target_partitions)?;

let sym_entries = ctx.table("symtable_entries")?
    .repartition_by_hash(vec![col("qualified_name")], profile.target_partitions)?;

// Join is now partition-local (no repartition inserted by optimizer)
let joined = ast_nodes.join(
    sym_entries,
    JoinType::Inner,
    &["qualified_name"],
    &["qualified_name"],
    None,
)?;
```

**Rule:** Only pre-repartition when the join key is reused by multiple downstream
joins on the same column. Otherwise, let the optimizer choose repartitioning.

**CoalescePartitionsExec / CoalesceBatchesExec:** After selective filters or joins
that produce tiny batches, DataFusion inserts coalesce operators. The
`coalesce_batches = true` config ensures batch sizes stay efficient for
vectorized processing.

### 11.4 Cross-Query Concurrency

For independent output targets that share no intermediate state:

```rust
// Execute independent output targets concurrently
let handles: Vec<_> = output_plans.into_iter()
    .map(|(name, df)| {
        let ctx = ctx.clone();  // SessionContext is Arc-wrapped
        tokio::spawn(async move {
            materialize_output(&ctx, &name, df).await
        })
    })
    .collect();

let results = futures::future::join_all(handles).await;
```

### 11.5 Memory Pressure Management

The `fair_spill_pool` runtime configuration provides automatic memory management:

- DataFusion tracks memory reservations per operator
- When the pool is exhausted, operators spill to disk automatically
- No application-level memory management needed

```rust
let runtime = RuntimeEnvBuilder::default()
    .with_fair_spill_pool(profile.memory_pool_bytes)
    .with_disk_manager_specified(&profile.spill_paths)
    .build()?;
```

---

## 12) Streaming Data Interchange (Python ↔ Rust)

### 12.1 Arrow C Stream ABI

The Python shell communicates with the Rust engine via Arrow C Stream interface:

```rust
// Rust side: expose execution results as PyCapsule
#[pymethod]
fn execute_stream(&self, py: Python) -> PyResult<PyObject> {
    let stream = self.inner.execute_stream()?;
    // Return as Arrow PyCapsule (stable ABI)
    stream.to_pycapsule(py)
}
```

```python
# Python side: consume as RecordBatchReader
import pyarrow as pa

capsule = engine.execute_stream()
reader = pa.RecordBatchReader.from_stream(capsule)

for batch in reader:
    process(batch)
```

### 12.2 Zero-Copy Guarantees

The Arrow C Stream interface provides:
- No serialization/deserialization
- No memory copies for columnar data
- Stable ABI across Python/Rust version boundaries
- Compatible with `abi3` wheel builds

---

## 13) PyO3/Maturin Packaging

### 13.1 Crate Structure

```
rust/
├── codeanatomy-engine/          # Main engine crate
│   ├── Cargo.toml               # cdylib + rlib, pyo3 + abi3
│   ├── src/
│   │   ├── lib.rs               # #[pymodule] entry point
│   │   ├── session.rs           # Session factory
│   │   ├── compiler.rs          # Semantic plan compiler
│   │   ├── materializer.rs      # CPG output writer
│   │   ├── udfs/                # Custom UDF implementations
│   │   │   ├── scalar.rs
│   │   │   ├── aggregate.rs
│   │   │   └── window.rs
│   │   ├── providers/           # Delta provider management
│   │   └── artifacts/           # Plan bundle capture
│   └── tests/
├── codeanatomy-plugin-api/      # Existing plugin API crate
├── codeanatomy-plugin-host/     # Existing plugin host crate
└── ...                          # Existing Rust workspaces
```

### 13.2 Cargo.toml

```toml
[package]
name = "codeanatomy-engine"
version = "0.1.0"
edition = "2021"

[lib]
name = "codeanatomy_engine"
crate-type = ["cdylib", "rlib"]

[dependencies]
pyo3 = { version = "0.27", features = ["extension-module", "abi3-py313"] }
datafusion = { version = "52", features = ["default"] }
deltalake = { version = "0.25", features = ["datafusion"] }
arrow = { version = "54" }
tokio = { version = "1", features = ["full"] }
```

### 13.3 Python Module Surface

```python
# Thin Python facade
from codeanatomy_engine import (
    SessionFactory,
    SemanticPlanCompiler,
    CpgMaterializer,
    RunResult,
    PlanBundle,
)

# Minimal Python entry point
def execute_cpg_build(
    extraction_inputs: list[ExtractionInput],
    semantic_spec: SemanticSpec,
    environment_profile: EnvironmentProfile,
) -> RunResult:
    """Single-call entry point from Python to Rust engine."""
    factory = SessionFactory(environment_profile)
    session = factory.build_session()

    # Register extraction inputs as Delta providers
    session.register_inputs(extraction_inputs)

    # Compile semantic spec to LogicalPlan DAG
    compiler = SemanticPlanCompiler(session)
    plan = compiler.compile(semantic_spec)

    # Capture plan bundle (non-executing)
    bundle = plan.capture_bundle()

    # Execute and materialize
    materializer = CpgMaterializer(session)
    result = materializer.execute(plan)

    return RunResult(
        plan_bundle=bundle,
        outputs=result.outputs,
        metrics=result.metrics,
    )
```

---

## 14) Plan Serialization, Interchange, and Unparser

### 14.1 datafusion-proto: Binary Plan Serialization

For plan caching and cross-process plan transport within the same DataFusion
version:

```rust
use datafusion_proto::bytes::{
    logical_plan_to_bytes, logical_plan_from_bytes,
    physical_plan_to_bytes, physical_plan_from_bytes,
};

// Serialize logical plan to bytes (compact, fast)
let logical_bytes = logical_plan_to_bytes(&optimized_plan)?;

// Deserialize in the same (or compatible) SessionContext
let restored = logical_plan_from_bytes(&logical_bytes, &ctx)?;

// Physical plan serialization (more fragile, version-coupled)
let physical_bytes = physical_plan_to_bytes(physical_plan.clone())?;
let restored_physical = physical_plan_from_bytes(&physical_bytes, &ctx)?;
```

**Version lock:** Serialized bytes are NOT guaranteed compatible across DataFusion
versions. Treat as version-coupled cache artifacts, not persistent storage.

**MemTable limitation:** Plans referencing in-memory record batch tables cannot be
serialized via proto. All plan inputs must be catalog-registered providers.

### 14.2 Substrait: Portable Plan Interchange

For plan portability across DataFusion versions and external analysis:

```rust
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;

// Serialize to Substrait (version-independent IR)
let substrait_bytes = to_substrait_plan(&optimized_plan, &ctx.state())?;

// Deserialize — still requires compatible SessionContext (tables, UDFs)
let restored_df = from_substrait_plan(&ctx.state(), &substrait_bytes)?;

// Can now combine with other DataFrames
let combined = restored_df.join(other_df, JoinType::Inner, &["key"], &["key"], None)?;
```

Substrait enables:
- Plan storage independent of DataFusion version
- Cross-engine plan analysis (validate against DuckDB)
- Plan diffing at the IR level
- "Planner agent → executor agent" patterns (ship Substrait, hydrate, execute)

### 14.3 Plan ↔ SQL Unparser

The unparser converts a LogicalPlan back to SQL text — enabling "plan fragment
regeneration" workflows where the compiler builds plan fragments, unparsess to SQL,
and re-combines as CTEs in a larger statement:

```rust
use datafusion::sql::unparser::{plan_to_sql, Unparser};

// Convert optimized plan fragment to SQL
let sql_stmt = plan_to_sql(&optimized_fragment)?;
let sql_text = sql_stmt.to_string();

// Embed as CTE in a larger query
let combined_sql = format!(
    "WITH fragment AS ({sql_text}) SELECT f.*, t.extra FROM fragment f JOIN other t ON f.key = t.key"
);
let combined_df = ctx.sql(&combined_sql)?;
```

**Dialect-aware unparser (Python):**

```python
from datafusion.unparser import Unparser, Dialect

unparser = Unparser(Dialect.postgres()).with_pretty(True)
sql_text = unparser.plan_to_sql(df.optimized_logical_plan())
```

**Use case:** When the semantic compiler needs to move plan fragments between
different catalog contexts (e.g., staging → production), unparse to SQL and
re-plan in the target context. This is safer than binary serialization because
it re-resolves names and types.

**Watchout:** Not all plans can be converted to SQL — custom operators and
provider-specific nodes may fail. The unparser uses qualified identifiers
(`"table"."col"`); do not post-process with naive string operations.

### 14.4 Plan Safety: `into_parts()` vs `into_unoptimized_plan()`

When extracting a LogicalPlan from a DataFrame for combination:

- **`into_parts()`** — Returns both the plan AND the SessionState snapshot.
  This is the safe path for plan combination because the state is preserved.
- **`into_unoptimized_plan()`** — Returns only the plan, discarding the
  SessionState. Explicitly documented as "should not be used outside testing"
  because functions like `now()` may evaluate differently.
- **`into_view()`** — Discards the DataFrame's SessionState; uses whatever
  SessionState is provided at scan time. Safe only within our One-Context
  architecture where all operations share the same session.

**Rule:** Always use `into_parts()` when combining plans that may be executed
in different timing contexts. Use `into_optimized_plan()` (which preserves
state) for the common case.

---

## 15) Implementation Workstreams

### WS1: Rust Session Factory (Week 1-2)

**Deliverables:**
- `SessionFactory` struct with deterministic construction
- `SessionEnvelope` capture and hashing
- Environment profile detection
- UDF registration framework (empty UDF bodies initially)

**Exit criteria:**
- `SessionContext` construction is deterministic (same inputs = same `envelope_hash`)
- `information_schema.df_settings` snapshot matches expected config

### WS2: Delta Provider Manager (Week 2-3)

**Deliverables:**
- `register_extraction_inputs` with native `DeltaTableProvider`
- `DeltaScanConfig` standardization
- Snapshot management (latest + version-pinned)
- Schema hash computation and drift detection

**Exit criteria:**
- All extraction outputs register as native Delta providers
- Two-tier pruning verified via `EXPLAIN ANALYZE` output
- No Arrow Dataset fallback path

### WS3: Custom UDFs (Week 2-4)

**Deliverables:**
- `byte_span_contains`, `byte_span_overlaps` scalar UDFs
- `qualified_name_*` scalar UDFs
- `span_merge` aggregate UDF with `GroupsAccumulator`
- `stable_hash_id` for deterministic ID generation

**Exit criteria:**
- All UDFs registered and callable from SQL
- UDF performance benchmarks (columnar vs row-at-a-time comparison)
- `Volatility::Immutable` set for all deterministic UDFs

### WS4: Semantic Plan Compiler (Week 3-6)

**Deliverables:**
- View definition → SQL/DataFrame/LogicalPlanBuilder compilation
- Join graph inference from semantic model
- Union-by-name for multi-source extraction composition
- CTE/view structure for reusable subplans
- `cache()` insertion for shared subplans

**Exit criteria:**
- Single combined LogicalPlan DAG produced from full semantic spec
- All 14 ViewKinds compilable (existing view specs translate to plans)
- Join keys inferred from semantic model (no hardcoded join keys)

### WS5: Plan Artifact Bundle (Week 4-5)

**Deliverables:**
- `PlanBundle` capture (P0/P1/P2 + EXPLAIN snapshots)
- Plan fingerprinting and regression detection
- Execution metrics capture via `EXPLAIN ANALYZE`

**Exit criteria:**
- Complete plan bundle captured for every execution
- P1 fingerprint stability verified (same input = same P1 hash)
- Regression detection functional for P1 changes

### WS6: CPG Materializer (Week 5-7)

**Deliverables:**
- Stream-first execution via `execute_stream_partitioned`
- Delta `insert_into` for CPG output tables
- Schema enforcement via `DeltaDataChecker`
- Compact `RunResult` envelope

**Exit criteria:**
- End-to-end execution from extraction Delta inputs to CPG Delta outputs
- All CPG output tables written with correct schema
- `RunResult` captures plan bundle + metrics + output locations

### WS7: Python Shell (Week 6-8)

**Deliverables:**
- PyO3 module with `SessionFactory`, `SemanticPlanCompiler`, `CpgMaterializer`
- Arrow C Stream interface for result streaming
- Thin `execute_cpg_build` entry point

**Exit criteria:**
- Full build callable from Python with `uv run`
- Results accessible as `pyarrow.RecordBatchReader`
- Plan bundle inspectable from Python

### WS8: Incremental Execution (Week 7-9)

**Deliverables:**
- Delta version change detection
- CDF provider registration for incremental input
- Skip-execution for unchanged subplans
- Incremental CPG update path

**Exit criteria:**
- Changed-input detection based on Delta versions
- Incremental execution produces correct CPG updates
- Unchanged extraction inputs are not re-scanned

### WS9: Custom Optimizer Rules (Week 8-10)

**Deliverables:**
- `SemanticTypeCoercionRule` (AnalyzerRule)
- `ByteSpanOverlapOptimizer` (OptimizerRule)
- `CpgRepartitionRule` (PhysicalOptimizerRule)
- Optimizer rule toggle mechanism for A/B testing

**Exit criteria:**
- Custom rules improve relevant query patterns
- `EXPLAIN VERBOSE` shows custom rule effects
- No regressions on existing query patterns

### WS10: Adaptive Tuner (Week 9-11)

**Deliverables:**
- Narrow-scope auto-tuner reading execution metrics
- `target_partitions` adjustment based on scan pruning effectiveness
- Bounded knob adjustment with regression rollback

**Exit criteria:**
- Tuner starts in observe-only mode
- After stability window, adjustments are bounded and reversible
- Any detected regression reverts to last stable config

---

## 16) Delivery Phases

### Phase 1: Deterministic Core (WS1 + WS2 + WS3)

**Exit:** Deterministic session with registered Delta providers and custom UDFs.
Can execute hand-written SQL against extraction tables and verify two-tier pruning.

### Phase 2: Semantic Compilation (WS4 + WS5)

**Exit:** Full semantic spec compiles to a single LogicalPlan DAG. Plan artifact
bundle captured with fingerprinting. No execution yet (plan-only validation).

### Phase 3: End-to-End Execution (WS6 + WS7)

**Exit:** Complete pipeline from extraction Delta inputs through semantic compilation
to CPG Delta outputs. Callable from Python. Hamilton and rustworkx can be removed.

### Phase 4: Incremental + Optimization (WS8 + WS9 + WS10)

**Exit:** Incremental execution, custom optimizer rules, and adaptive tuning.
Production-grade performance and change management.

---

## 17) Acceptance Criteria

1. **Determinism:** Same semantic spec + same Delta versions + same session config
   produces identical P1 hash (optimized logical plan fingerprint).

2. **No fallback paths:** All production scans use native `DeltaTableProvider`.
   No Arrow Dataset. No Parquet-direct. No Hamilton. No rustworkx.

3. **Single DAG:** The entire CPG build compiles to one LogicalPlan DAG (or a
   minimal set of independent DAGs for unconnected output targets).

4. **Auto-optimization:** DataFusion's optimizer handles predicate pushdown, join
   ordering, projection pruning, and repartitioning without custom scheduling logic.

5. **Rich artifacts:** Every run produces a `PlanBundle` (P0/P1/P2 + EXPLAIN
   snapshots) and a `RunResult` (outputs + metrics + envelope).

6. **Incremental:** Changed extraction inputs are detected via Delta versions;
   unchanged subplans are skipped; CDF enables row-level incremental processing.

7. **Regression detection:** P1 hash changes without config changes are flagged
   as unexpected regressions.

8. **Memory bounded:** Fair spill pool prevents OOM; disk spill is automatic.

9. **Zero-copy Python interface:** Arrow C Stream / PyCapsule for result streaming.

10. **Portable wheels:** abi3 + manylinux2014 for Linux; universal macOS; via
    maturin with `--locked --compatibility pypi`.

---

## 18) Feature Utilization Matrix

### DataFusion Features Directly Used

| Feature | API Surface | Implementation Location |
|---------|-------------|------------------------|
| SessionContext construction | `SessionContext::new_with_config_rt` | WS1: Session Factory |
| SessionConfig knobs | `with_target_partitions`, `with_repartition_*`, etc. | WS1: Session Factory |
| RuntimeEnvBuilder | `with_fair_spill_pool`, `with_disk_manager_specified` | WS1: Session Factory |
| information_schema | `SELECT * FROM df_settings` | WS1: Envelope capture |
| DataFrame chaining | `filter`, `select`, `aggregate`, `join`, `sort` | WS4: Compiler |
| DataFrame.join | `join(right, JoinType, left_cols, right_cols)` | WS4: Join graph |
| DataFrame.union_by_name | `union_by_name(other)` (Rust only) | WS4: Multi-source union |
| DataFrame.cache | `cache()` for shared subplans | WS4: Subplan caching |
| View registration | `ctx.register_view(name, df)` | WS4: CTE structure |
| LogicalPlanBuilder | `from(plan).join().union().build()` | WS4: Complex DAG surgery |
| SQL planning | `ctx.sql(query)` → DataFrame | WS4: SQL-defined views |
| SQLOptions safety | `sql_with_options` + `SQLOptions::with_allow_ddl(false)` | WS4: Plan-only mode |
| EXPLAIN / EXPLAIN VERBOSE | Non-executing plan inspection | WS5: Plan bundle |
| EXPLAIN FORMAT TREE | Diff-friendly plan rendering | WS5: Plan bundle |
| EXPLAIN ANALYZE | Post-execution metrics | WS5: Execution metrics |
| logical_plan() | Unoptimized logical plan accessor | WS5: P0 capture |
| optimized_logical_plan() | Optimized logical plan accessor | WS5: P1 capture |
| execution_plan() | Physical plan accessor | WS5: P2 capture |
| execute_stream_partitioned | Partitioned streaming execution | WS6: Materializer |
| INSERT INTO | DML for Delta writes | WS6: Materializer |
| ScalarUDFImpl | Custom scalar functions | WS3: CPG UDFs |
| AggregateUDFImpl | Custom aggregate functions | WS3: CPG UDFs |
| GroupsAccumulator | Vectorized aggregation | WS3: CPG UDFs |
| Signature.with_parameter_names | Named UDF arguments | WS3: CPG UDFs |
| Volatility::Immutable | Constant folding eligibility | WS3: CPG UDFs |
| ScalarUDFImpl::simplify | Plan-time constant evaluation | WS3/WS9: UDF optimization |
| add_analyzer_rule | Custom analyzer passes | WS9: Custom rules |
| add_optimizer_rule | Custom logical optimizer passes | WS9: Custom rules |
| add_physical_optimizer_rule | Custom physical optimizer passes | WS9: Custom rules |
| remove_optimizer_rule | Rule toggle for A/B testing | WS9: Experimentation |
| Substrait serialize/deserialize | Portable plan interchange | WS5: Optional |
| parse_sql_expr | SQL text → typed Expr | WS4: Expression parsing |
| arrow_cast / arrow_typeof | Type introspection + explicit casts | WS3/WS4: Schema alignment |
| named_struct / struct | Nested CPG output construction | WS6: Output schema |
| get_field / bracket syntax | Nested field extraction | WS4: Struct navigation |
| unnest | Array/struct expansion to rows | WS4: List-valued extraction |
| PREPARE / EXECUTE | Prepared statement plan reuse | WS4: Parameterized templates |
| with_param_values | Rust DataFrame parameter binding | WS4: Parameterized templates |
| Recursive CTE / to_recursive_query | Graph-style plan expansion | WS4: Scope/call graph traversal |
| join_on (inequality predicates) | Range-join for byte spans | WS4: Span containment joins |
| Subqueries (correlated/derived) | SELECT/FROM/WHERE subqueries | WS4: Complex derivations |
| union_by_name_distinct | By-name union with dedup | WS4: Multi-source dedup |
| intersect / except_all | Set difference operations | WS4: Evidence disagreement detection |
| coalesce_duplicate_keys | Post-join key column behavior | WS4: Join schema control |
| DataFrame.repartition_by_hash | Pre-position data for joins | WS4/WS9: Partition optimization |
| TreeNode transform/rewrite | Deterministic plan normalization | WS4/WS9: Plan canonicalization |
| LogicalPlan.display_graphviz | DOT format plan rendering | WS5: Visual plan artifacts |
| EXPLAIN FORMAT GRAPHVIZ | Machine-ingestible plan format | WS5: Plan tooling |
| EXPLAIN ANALYZE VERBOSE | Per-partition execution metrics | WS5: Detailed diagnostics |
| explain.show_schema/show_statistics | Enriched EXPLAIN output | WS5: Plan bundle enrichment |
| ExecutionPlan::metrics() | Programmatic metric extraction | WS5: Structured metrics |
| ExecutionPlan::partition_count | Physical parallelism topology | WS5: Partition tracking |
| planning_concurrency | Parallel UNION child planning | WS1: Session config |
| enable_dynamic_filter_pushdown family | TopK/join/aggregate dynamic filters | WS1: Session config |
| enable_sort_pushdown / prefer_existing_sort | Sort optimization knobs | WS1: Session config |
| spill_compression / max_spill_file_size | Spill tuning | WS1: Session config |
| metadata_cache_limit / list_files_cache | Runtime caches for view re-expansion | WS1: Session config |
| plan_to_sql (Unparser) | LogicalPlan → SQL round-trip | WS4: Fragment regeneration |
| logical_plan_to_bytes / from_bytes | Binary plan serialization | WS5: Plan caching |
| physical_plan_to_bytes / from_bytes | Physical plan serialization | WS5: Plan caching |
| COPY TO PARTITIONED BY | Hive-style partitioned output | WS6: Output materialization |
| DataFrame::write_table | Direct terminal write operation | WS6: Materializer |
| CREATE TABLE AS SELECT | In-memory cache boundary | WS4: Subplan materialization |
| CREATE VIEW (SQL DDL) | SQL-level modular subplans | WS4: View composition |
| ExprPlanner / RelationPlanner | Custom SQL syntax extension | WS9: Domain-specific SQL |
| into_parts() | Safe plan + state extraction | WS4: Plan combination safety |
| UnionExec / InterleaveExec awareness | Physical union partition topology | WS4/WS9: Union optimization |
| datafusion-tracing | OpenTelemetry plan/execution tracing | WS5: Observability |

### DeltaLake Features Directly Used

| Feature | API Surface | Implementation Location |
|---------|-------------|------------------------|
| DeltaTableProvider::try_new | Native provider construction | WS2: Provider manager |
| DeltaScanConfig | Scan knob configuration | WS2: Provider manager |
| enable_parquet_pushdown | Two-tier pruning | WS2: Pruning |
| wrap_partition_values | Dictionary-encoded partitions | WS2: Provider manager |
| file_column_name | Source file provenance column | WS2: Lineage views |
| with_files | Curated file-subset scans | WS8: Incremental |
| DeltaTable::open | Latest snapshot | WS2: Default mode |
| DeltaTable::open_with_version | Version-pinned snapshot | WS2: Replay mode |
| DeltaCdfTableProvider | Change data feed provider | WS8: Incremental |
| insert_into (Append) | Incremental CPG writes | WS6: Materializer |
| insert_into (Overwrite) | Full CPG rebuild writes | WS6: Materializer |
| DeltaDataChecker | Schema/constraint enforcement | WS6: Write validation |
| Transaction log | Version tracking, schema storage | WS8: Change detection |
| schema_mode=merge | Additive schema evolution | WS6: Schema evolution |
| statistics() on provider | Table statistics for optimizer | WS4: Join optimization |
| constraints() on provider | Constraint metadata for optimizer | WS9: Custom rules |
| get_add_actions | File inventory from Delta log | WS8: Incremental file-subset scans |
| with_files + file inventory | Surgical file-subset provider scans | WS8: Changed-files-only recompute |
| replaceWhere partial overwrite | Targeted row replacement on write | WS8: Incremental CPG updates |
| target_file_size | Output file sizing control | WS6: Write tuning |
| writer_properties | Compression, bloom filters, statistics | WS6: Write tuning |
| Column mapping awareness | Logical ↔ physical name mapping | WS8: Schema evolution safety |

### Rust Packaging Features Used

| Feature | API Surface | Implementation Location |
|---------|-------------|------------------------|
| PyO3 #[pymodule] | Module entry point | WS7: Python shell |
| PyO3 abi3-py313 | Stable ABI wheel | WS7: Packaging |
| maturin build | Wheel production | WS7: CI/CD |
| cdylib + rlib | Shared lib + Rust tests | WS7: Crate config |
| Arrow C Stream PyCapsule | Zero-copy data interchange | WS7: Streaming interface |
| manylinux2014 | Linux portability | WS7: CI/CD |

---

## 19) Migration Path

### Step 1: Build alongside (Phases 1-2)

New Rust engine builds in parallel with existing Hamilton/rustworkx pipeline.
Both produce plan artifacts; compare for correctness.

### Step 2: Shadow execution (Phase 3)

New engine executes the full pipeline in shadow mode. Compare CPG outputs
byte-for-byte with Hamilton-produced outputs.

### Step 3: Cutover (after Phase 3 validation)

Switch `build_cpg()` entry point to call Rust engine. Remove:
- `src/hamilton_pipeline/` (Hamilton DAG orchestration)
- `src/relspec/rustworkx_graph.py` (rustworkx scheduling)
- `src/relspec/rustworkx_schedule.py` (rustworkx scheduling)
- `_default_semantic_cache_policy()` at `pipeline.py:262`
- Manual `TaskSpec` / `PlanCatalog` / `TaskGraph` construction

### Step 4: Optimize (Phase 4)

Add incremental execution, custom optimizer rules, and adaptive tuning
on top of the established Rust engine.
