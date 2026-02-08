# Rust-First DataFusion + DeltaLake Performance Optimization Protocols v1

**Date:** 2026-02-08  
**Status:** Implementation playbook (syntax-level)  
**Audience:** Engine and platform implementers  
**Primary goal:** Remove ambiguity in how to implement planning, concurrency, compute, and Delta performance optimizations in a Rust-first execution stack.

---

## 1) Scope and Operating Model

This document expands the optimization proposals into concrete implementation surfaces and code patterns.

- Keep `src/engine` Python API stable; push performance-critical behavior into Rust.
- Use DataFusion for plan creation, rewriting, and execution.
- Use DeltaLake as canonical storage, pruning layer, and transactional sink.
- Make optimization behavior deterministic via explicit rulepacks/config snapshots.

---

## 2) Canonical Optimization Stack (What to Implement)

Implement all layers below; treat them as one integrated protocol:

1. Session/runtime controls (parallelism, memory, spill, pruning toggles).
2. Rulepack controls (analyzer/logical/physical rules, deterministic ordering).
3. Explain/provenance controls (`P0/P1/P2`, `EXPLAIN VERBOSE`, `EXPLAIN ANALYZE`).
4. Delta provider controls (session-aware provider creation, scan config, file pruning).
5. Delta maintenance controls (optimize, vacuum, checkpoint, metadata cleanup).
6. Compute extension controls (UDF/UDAF/UDWF hooks that the optimizer can reason about).
7. Cache controls (runtime caches + explicit cache boundaries in plan graph).
8. Validation controls (metrics-based promotion gates and deterministic regression checks).

---

## 3) Session and Runtime Protocol (Rust Syntax)

### 3.1 Deterministic Session Construction

Use a single construction path (existing pattern in `rust/codeanatomy_engine/src/session/factory.rs`), then extend with additional high-ROI knobs.

```rust
use std::sync::Arc;
use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

let runtime = RuntimeEnvBuilder::default()
    .with_memory_pool(Arc::new(FairSpillPool::new(memory_pool_bytes)))
    .with_disk_manager_builder(
        DiskManagerBuilder::default().with_mode(DiskManagerMode::OsTmpDirectory),
    )
    // optional but recommended for bounded spill behavior:
    .with_max_temp_directory_size(max_temp_dir_bytes)
    .build_arc()?;

let mut config = SessionConfig::new()
    .with_default_catalog_and_schema("codeanatomy", "public")
    .with_information_schema(true)
    .with_target_partitions(target_partitions)
    .with_batch_size(batch_size)
    .with_repartition_joins(true)
    .with_repartition_aggregations(true)
    .with_repartition_windows(true)
    .with_repartition_sorts(true)
    .with_repartition_file_scans(true)
    .with_repartition_file_min_size(64 * 1024 * 1024)
    .with_parquet_pruning(true);

let opts = config.options_mut();
opts.execution.coalesce_batches = true;
opts.execution.collect_statistics = true;
opts.execution.parquet.pushdown_filters = true;
opts.execution.enable_recursive_ctes = true;
opts.execution.planning_concurrency = target_partitions;
opts.optimizer.max_passes = 3;
opts.optimizer.skip_failed_rules = false;
opts.optimizer.filter_null_join_keys = true;
opts.optimizer.enable_dynamic_filter_pushdown = true;
opts.optimizer.enable_topk_dynamic_filter_pushdown = true;
opts.sql_parser.enable_ident_normalization = false;
opts.explain.show_statistics = true;
opts.explain.show_schema = true;

let state = SessionStateBuilder::new()
    .with_config(config)
    .with_runtime_env(runtime)
    .with_analyzer_rules(analyzer_rules)
    .with_optimizer_rules(optimizer_rules)
    .with_physical_optimizer_rules(physical_rules)
    .build();

let ctx = SessionContext::new_with_state(state);
```

### 3.2 Required Runtime Profiles

Use explicit profiles (small/medium/large) with deterministic values, then tune:

- `target_partitions`
- `batch_size`
- `memory_pool_bytes`
- `max_temp_directory_size`
- `repartition_file_min_size`

Use the existing `EnvironmentProfile` model in `rust/codeanatomy_engine/src/session/profiles.rs` and add spill/disk cache controls there rather than ad hoc call-site overrides.

---

## 4) Rulepack and Plan Rewriting Protocol

### 4.1 Rulepack Determinism

Keep rule ordering immutable and fingerprinted (existing `CpgRuleSet` pattern).

```rust
use crate::rules::registry::CpgRuleSet;

let ruleset = CpgRuleSet::new(
    analyzer_rules,    // deterministic order
    optimizer_rules,   // deterministic order
    physical_rules,    // deterministic order
);

// fingerprint is part of session/run envelope
let rulepack_id = ruleset.fingerprint;
```

### 4.2 Policy as Planner Rule (Not Post-Execution Policy)

Use analyzer rules for pre-execution enforcement (existing pattern in `rust/datafusion_ext/src/planner_rules.rs`).

```rust
use std::sync::Arc;
use datafusion_ext::planner_rules::CodeAnatomyPolicyRule;

let mut state = ctx.state_ref().write();
state.add_analyzer_rule(Arc::new(CodeAnatomyPolicyRule::default()));
```

### 4.3 Physical Rule Integration

Compose physical rules through `SessionStateBuilder` for deterministic inclusion (existing pattern in `rust/datafusion_ext/src/physical_rules.rs` and engine rulepack wiring).

```rust
use std::sync::Arc;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion_ext::physical_rules::CodeAnatomyPhysicalRule;

let mut state = ctx.state_ref().write();
let rebuilt = SessionStateBuilder::from(state.clone())
    .with_physical_optimizer_rule(Arc::new(CodeAnatomyPhysicalRule::default()))
    .build();
*state = rebuilt;
```

### 4.4 Explain/Plan Artifact Protocol (Required)

Capture all three artifacts for representative queries:

- `P0`: unoptimized logical plan
- `P1`: rule-by-rule trace (`EXPLAIN VERBOSE`)
- `P2`: physical plan and metrics (`EXPLAIN ANALYZE`)

```rust
use datafusion::prelude::DataFrame;

pub async fn capture_plan_artifacts(df: DataFrame) -> datafusion_common::Result<(String, String, String)> {
    let p0 = format!("{:?}", df.logical_plan());
    let p2 = format!("{:?}", df.clone().create_physical_plan().await?);
    let p1 = format!("{:?}", df.explain(true, false)?.collect().await?); // verbose
    Ok((p0, p1, p2))
}
```

Promotion rule: no optimizer/rule change merges without stable `P1`/`P2` diff evidence on benchmark queries.

---

## 5) Delta Provider and Scan Protocol

### 5.1 Session-Aware Delta Provider Construction

Use control-plane APIs only (existing pattern in `rust/codeanatomy_engine/src/providers/registration.rs` + `rust/datafusion_ext/src/delta_control_plane.rs`).

```rust
use datafusion_ext::delta_control_plane::{delta_provider_from_session, DeltaScanOverrides};
use datafusion_ext::DeltaFeatureGate;

let overrides = DeltaScanOverrides {
    file_column_name: Some("__source_file".to_string()),
    enable_parquet_pushdown: Some(true),
    schema_force_view_types: Some(false),
    wrap_partition_values: Some(true),
    schema: None,
};

let (provider, snapshot, resolved_scan_config, pruned_files, predicate_error) =
    delta_provider_from_session(
        &ctx,
        table_uri,
        None,        // storage options
        version_pin, // Option<i64>
        None,        // timestamp
        predicate,   // Option<String>
        overrides,
        Some(DeltaFeatureGate::default()),
    ).await?;

if let Some(err) = predicate_error {
    return Err(datafusion_common::DataFusionError::Plan(format!("predicate parse failed: {err}")));
}

ctx.register_table(logical_name, std::sync::Arc::new(provider))?;
```

### 5.2 File-Subset and Incremental Recompute

When recomputing only changed files, use provider file-subsetting (via control plane) instead of ad hoc filters.

- API surface already present (`with_files`) in `delta_control_plane`.
- Source for changed files: CDF or action-log analysis.

### 5.3 Required Scan Config Validation

Enforce scan config constraints before registration (existing `validate_scan_config` pattern):

- `enable_parquet_pushdown == true`
- `wrap_partition_values == true`
- lineage file column name valid if set

This must fail at plan time, never at execution.

---

## 6) Delta Maintenance Performance Protocol

Use existing maintenance wrappers in `rust/datafusion_ext/src/delta_maintenance.rs`; run as planned operations with explicit retention/concurrency assumptions.

```rust
use datafusion_ext::delta_maintenance::{
    delta_optimize_compact, delta_vacuum, delta_create_checkpoint, delta_cleanup_metadata
};

// 1) Compact / layout rewrite
let optimize_report = delta_optimize_compact(
    &ctx, table_uri, None, None, None,
    Some(512 * 1024 * 1024), // target file size
    gate,
    commit_options,
).await?;

// 2) Vacuum with safety checks
let vacuum_report = delta_vacuum(
    &ctx, table_uri, None, None, None,
    Some(168),   // retention hours (7d)
    false,       // dry_run
    true,        // enforce retention duration
    true,        // require vacuumProtocolCheck
    gate,
    commit_options,
).await?;

// 3) Checkpoint then metadata cleanup
let checkpoint_report = delta_create_checkpoint(&ctx, table_uri, None, None, None, gate).await?;
let cleanup_report = delta_cleanup_metadata(&ctx, table_uri, None, None, None, gate).await?;
```

### 6.1 Non-Negotiable Safety Rules

- Never run unsafe vacuum retention to “speed up cleanup”.
- Gate vacuum on writer feature checks (`vacuumProtocolCheck` path already supported).
- Couple checkpoint/cleanup cadence to recovery and replay SLOs.

---

## 7) Cache and Materialization Protocol

### 7.1 Runtime Cache Introspection

The cache UDTFs are already exposed in the stack (`list_files_cache`, `metadata_cache`, `predicate_cache`, `statistics_cache`). Query them in automated health checks.

```sql
SELECT * FROM list_files_cache();
SELECT * FROM metadata_cache();
SELECT * FROM predicate_cache();
SELECT * FROM statistics_cache();
```

### 7.2 Cache Boundaries in Logical DAG

Use explicit `DataFrame.cache()` only at high-fanout/high-cost boundaries (existing pattern in `rust/codeanatomy_engine/src/compiler/cache_boundaries.rs`).

```rust
let df = ctx.table("expensive_view").await?;
let cached = df.cache().await?;
ctx.deregister_table("expensive_view")?;
ctx.register_table("expensive_view", cached.into_view())?;
```

Do not cache low-fanout simple projections/filters by default.

---

## 8) Rust UDF/UDAF/UDWF Performance Protocol

### 8.1 Scalar UDF Contract (Optimizer-Visible)

Implement all relevant planner-visible hooks (see current patterns in `rust/datafusion_ext/src/function_factory.rs`, `udf_async.rs`, and multiple `udf/*` modules).

```rust
use std::any::Any;
use std::sync::Arc;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ExprProperties, Interval, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};

#[derive(Debug, PartialEq, Eq, Hash)]
struct MyUdf {
    signature: Signature,
}

impl MyUdf {
    fn new() -> Self {
        Self {
            signature: Signature::one_of(vec![TypeSignature::UserDefined], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MyUdf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "my_udf" }
    fn signature(&self) -> &Signature { &self.signature }
    fn documentation(&self) -> Option<&Documentation> { None }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        // explicit coercion path because signature is UserDefined
        Ok(arg_types.to_vec())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new("my_udf", DataType::Utf8, nullable)))
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        // Keep return_type + return_field_from_args aligned for metadata surfaces
        Ok(DataType::Utf8)
    }

    fn short_circuits(&self) -> bool { false }

    fn simplify(&self, args: Vec<Expr>, _info: &dyn SimplifyInfo) -> Result<ExprSimplifyResult> {
        // Return Original unless simplification is schema-preserving
        Ok(ExprSimplifyResult::Original(args))
    }

    fn evaluate_bounds(&self, _inputs: &[&Interval]) -> Result<Interval> {
        Interval::make_unbounded(&DataType::Utf8)
    }

    fn propagate_constraints(
        &self,
        _interval: &Interval,
        _inputs: &[&Interval],
    ) -> Result<Option<Vec<Interval>>> {
        Ok(Some(Vec::new()))
    }

    fn preserves_lex_ordering(&self, _inputs: &[ExprProperties]) -> Result<bool> {
        Ok(false)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // vectorized execution implementation
        Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(format!("{:?}", args.args.len())))))
    }
}

pub fn my_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(MyUdf::new())
}
```

### 8.2 UDAF Protocol

For heavy aggregations, prefer `GroupsAccumulator` paths; add `retract_batch` if window usage is expected (see existing `udaf_builtin.rs` patterns).

Required:

- deterministic state layout
- merge associativity tests
- retract correctness tests for bounded windows

### 8.3 UDWF Protocol

Use `WindowUDFImpl` when frame semantics and ordering behavior are central. Implement:

- `sort_options` when valid
- `reverse_expr` where reversible
- `coerce_types` for user-defined signatures
- batch-boundary invariance tests

### 8.4 Async UDF Protocol

Use async UDFs only for unavoidable external I/O. Baseline compute paths should stay synchronous and Arrow-kernel-driven. If async UDFs are enabled, enforce:

- timeout
- ideal batch size
- bounded concurrency policy

Pattern already exists in `rust/datafusion_ext/src/udf_async.rs`.

---

## 9) Concurrency and Calculation Performance Protocol

### 9.1 Join/Union/CTE Planning and Parallel Planning

- Keep related computations in one DAG (join/union/CTE) so optimizer sees the full graph.
- Set `planning_concurrency` high enough for large union fan-out trees.
- Use schema-aligned unions (`union_by_name` variants where available in Rust path) for drift tolerance.

### 9.2 Repartition Strategy

Control both auto and manual repartition behavior:

- auto: joins/aggs/windows/sorts/file_scans toggles
- manual: pre-hash on known join keys for large stable joins

For very small datasets, force lower partition counts to avoid shuffle overhead.

### 9.3 Spill-Aware Throughput

Performance goal is “controlled spill”, not “no spill ever”.

Set and track:

- memory pool size
- temp directory size limit
- spill metrics (`spilled_bytes`, spill count)
- repartition exchange overhead (`RepartitionExec` metrics)

---

## 10) Delta Write-Path Optimization Protocol

Use the write path with explicit controls for idempotency and schema guardrails (current core in `rust/codeanatomy_engine/src/executor/delta_writer.rs`).

Required controls:

1. `ensure_output_table` on first run.
2. strict schema compatibility validation before write.
3. deterministic partition/clustering mapping from spec.
4. commit metadata lineage fields on every write.
5. retry/verification policy for expected optimistic concurrency conflicts.

---

## 11) Observability and Regression Gates (Performance CI Protocol)

### 11.1 Required Metrics Capture

Capture per representative query:

- planning time
- execution time
- bytes scanned
- files considered vs files read (pruning ratio)
- repartition count
- spill bytes / spill files
- Delta commit latency

### 11.2 Explain Gate

Store and diff:

- `EXPLAIN FORMAT INDENT`
- `EXPLAIN VERBOSE`
- `EXPLAIN ANALYZE`

No optimizer/rule/config changes without updated baseline artifacts.

### 11.3 Cache Gate

Add automated checks that cache tables are populated for repeated workloads:

- `metadata_cache()`
- `statistics_cache()`
- `list_files_cache()`

---

## 12) Implementation Backlog (Priority Order)

### P0 (Immediate, High ROI)

1. Extend `SessionFactory` with `with_repartition_sorts`, `with_repartition_file_scans`, and `with_repartition_file_min_size`.
2. Add runtime bounds (`with_max_temp_directory_size`) and expose profile fields.
3. Standardize `P0/P1/P2` capture across engine runs.
4. Enforce provider registration via control-plane only.
5. Add cache snapshot checks into run diagnostics.

### P1 (Next)

1. Expand rulepack profile behavior with explicit per-profile rule lists and golden plan snapshots.
2. Add UDF/UDAF/UDWF contract checklist in PR template (hooks + tests).
3. Add Delta maintenance scheduler with explicit retention policy and feature-gate checks.

### P2 (Advanced)

1. Adaptive autotuning loop (next-run partition and spill policy suggestions from observed metrics).
2. Extended plan serialization/transport workflows for multi-process planner/executor topologies.
3. More aggressive compute specialization (additional custom UDF/UDAF kernels for dominant semantic transforms).

---

## 13) Definition of Done

A performance enhancement is complete only if all are true:

1. Compiles and runs with deterministic session/rule construction.
2. `EXPLAIN VERBOSE` shows expected rule effects.
3. `EXPLAIN ANALYZE` shows expected performance movement on benchmark queries.
4. Delta provider path remains session-aware and feature-gated.
5. Regression artifacts (`P0/P1/P2` + metrics) are stored and diffed.
6. No fallback to ad hoc non-Rust execution paths for core pipeline behavior.

