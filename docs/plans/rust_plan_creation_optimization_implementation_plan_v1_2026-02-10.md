# Rust Plan Creation + Optimization: Detailed Implementation Plan

Date: 2026-02-10
Parent Design: `docs/plans/rust_plan_creation_optimization_best_in_class_design_v1_2026-02-10.md`
Prerequisite: `docs/plans/cli_datafusion_transition_plan_v1_2026-02-09.md` (substantially implemented)

---

## 0) Executive Summary

This plan converts the high-level design in the parent document into concrete, file-level implementation steps organized into 14 workstreams (WS1-WS14). Each workstream specifies exact files to create or modify, struct/trait definitions, integration points, test contracts, and dependency ordering.

**Current state baseline** (from codebase exploration):

- **Rust engine** (`rust/codeanatomy_engine/`): ~12k LOC across compiler, session, rules, providers, executor, compliance, spec, stability, and tuner modules. Core compile->optimize->materialize pipeline is operational with two-layer determinism (spec_hash + envelope_hash), BLAKE3 fingerprinting, and PyO3 bindings.
- **Python planning stack** (files targeted for displacement): ~8,500 LOC across `src/relspec/execution_plan.py` (2,007 LOC), `src/datafusion_engine/plan/bundle.py` (2,589 LOC), `src/datafusion_engine/plan/pipeline.py` (542 LOC), `src/datafusion_engine/session/factory.py` (434 LOC), `src/datafusion_engine/views/graph.py` (1,460 LOC), and supporting files.
- **DataFusion version**: 51.0.0 (workspace-pinned in Rust), Python bindings 51.0.0+.
- **Key Rust strengths already present**: `SemanticPlanCompiler`, `PlanBundleArtifact` (P0/P1/P2 digests), `PlanningSurfaceSpec`/`PlanningSurfaceManifest`, `CpgRuleSet` with deterministic fingerprints, `FilterPushdownStatus` (Unsupported/Inexact/Exact), `ComplianceCapture`, `RunResult` with full artifact envelope.

**Already completed baseline scope** (do not re-implement):

- Core/bindings split is already complete (`codeanatomy_engine` core + `codeanatomy_engine_py` binding crate).
- Structured warning model is already live (`RunWarning`, `WarningCode`, `WarningStage`).
- Canonical session build APIs and override wiring already exist (`build_session_state*` with overrides).
- Shared pre-pipeline orchestration is already active for native/Python entrypoints.
- Runtime toggles for optimizer lab and delta codec capture are already in the runtime contract.
- Delta mutation request-struct APIs and compatibility wrappers are already present in `datafusion_ext`.

**Displacement summary**: ~5,000 LOC of Python planning logic (execution_plan, bundle, pipeline) moves to Rust. ~1,800 LOC (views, session factory, execution facade) migrates gradually. ~1,700 LOC (config models, spec_builder mirror) remains Python.

---

## 1) Dependency Graph Between Workstreams

```
WS1 (Semantic Validator) ──────────────┐
WS2 (Optimizer Pipeline) ──────────────┤
WS3 (Planning Surface Governance) ─────┤──> WS5 (Artifact Evolution) ──┐
WS4 (Pushdown Contracts) ──────────────┘                                │
                                                                         ├──> WS7 (Hard Cutover) ──> WS8 (Breaking Contract)
WS6 (Cost Model + Scheduling) ────────────────────────────────────────────┘

WS9  (Planner Environment Identity) ───┐
WS10 (Standalone Optimizer API) ───────┤
WS11 (Stats-Quality Governance) ───────┤──> WS13 (Portable Plan Policy)
WS12 (Delta Compatibility Contract) ───┤
WS14 (Custom Source Contract Hardening)┘
```

**Critical path (initial cutover)**: WS1 + WS2 + WS3 -> WS5 -> WS7 -> WS8

**Critical path (best-in-class hardening)**: WS9 + WS10 + WS11 + WS12 + WS13 + WS14

**Parallelizable**: WS1 || WS2 || WS3 || WS4 || WS6, and later WS9 || WS10 || WS11 || WS12 || WS14.

---

## 2) WS1: Semantic Validator Upgrade

### 2.1 Goal

Upgrade `graph_validator.rs` from structural-only validation (name/reference checks) to full semantic validation (transform legality, expression binding, join-key compatibility, output contract satisfiability).

### 2.2 Current State

- `compiler/graph_validator.rs` (~250 LOC): `validate_graph()` checks duplicate view names, unknown dependency references, and missing source references.
- No expression-level or type-level validation exists in Rust. Python relies on DataFusion's analyzer for implicit type coercion and binding errors, surfaced at plan-build time (not pre-validated).
- ViewTransform variants (`Normalize`, `Relate`, `Union`, `Project`, `Filter`, `Aggregate`, `IncrementalCdf`, `Metadata`, `FileManifest`) are defined in `spec/relations.rs` (~137 LOC) but not semantically validated.

### 2.3 Implementation Steps

#### Step 1: Create `compiler/semantic_validator.rs`

**New file**: `rust/codeanatomy_engine/src/compiler/semantic_validator.rs`

Define the validation framework:

```rust
pub struct SemanticValidationResult {
    pub errors: Vec<SemanticValidationError>,
    pub warnings: Vec<SemanticValidationWarning>,
}

pub enum SemanticValidationError {
    UnsupportedTransformComposition {
        view_name: String,
        detail: String,
    },
    UnresolvedColumnReference {
        view_name: String,
        column: String,
        context: String,
    },
    JoinKeyIncompatibility {
        view_name: String,
        left_key: String,
        right_key: String,
        left_type: String,
        right_type: String,
    },
    OutputContractViolation {
        output_name: String,
        expected_columns: Vec<String>,
        actual_columns: Vec<String>,
    },
    AggregationInvariantViolation {
        view_name: String,
        detail: String,
    },
}

pub enum SemanticValidationWarning {
    ImplicitTypeCoercion {
        view_name: String,
        column: String,
        from_type: String,
        to_type: String,
    },
    BroadProjection {
        view_name: String,
        column_count: usize,
    },
}

pub fn validate_semantics(
    spec: &SemanticExecutionSpec,
    ctx: &SessionContext,
) -> Result<SemanticValidationResult>;
```

Implement validation passes:

1. **Transform legality check**: For each `ViewDefinition`, validate that the `ViewTransform` variant is compatible with its input schema(s). E.g., a `Relate` (join) requires exactly two input sources; `Aggregate` requires non-empty group-by or aggregation expressions.

2. **Column reference resolution**: For `Filter` and `Project` transforms, parse expression strings against the input view schema using `SessionContext::parse_sql_expr(expr, schema)`. Flag unresolved/ambiguous column references before plan compilation.

   Reference: DataFusion's schema-aware SQL expression parsing (`datafusion_planning.md` S3.2.4). Use `SessionState::create_logical_plan()` for parse-only without DDL side effects (`datafusion_planning.md` S3.3.6).

3. **Join key compatibility**: For `Relate` transforms, resolve left/right key columns and check Arrow type compatibility. Use DataFusion's `TypeCoercionRewriter` semantics as reference -- if types require implicit coercion beyond what DataFusion supports, flag as error. Check for nullable join keys (emit warning).

4. **Output contract satisfiability**: For each `OutputTarget`, verify that the producing view's schema (after all transforms) contains all required output columns with compatible types.

#### Step 2: Integrate into `plan_compiler.rs`

**Modify**: `rust/codeanatomy_engine/src/compiler/plan_compiler.rs`

Insert semantic validation between graph validation and topological sort:

```rust
// Existing flow:
// 1. validate_graph()      -> structural checks
// 2. topological_sort()    -> ordering
// 3. compile views

// New flow:
// 1. validate_graph()      -> structural checks
// 2. validate_semantics()  -> semantic checks  <-- NEW
// 3. topological_sort()    -> ordering
// 4. compile views
```

Semantic validation errors are fatal (stop compilation). Warnings are collected into `RunResult.warnings`.

#### Step 3: Add expression binding validation

**Modify**: `compiler/semantic_validator.rs`

For each `FilterTransform` and `ProjectTransform`, use DataFusion's expression parsing to pre-validate binding:

```rust
// For filter expressions:
let source_df = ctx.table(&input_view_name).await?;
match ctx.parse_sql_expr(&filter_expr, source_df.schema()) {
    Ok(_) => { /* binding succeeded */ }
    Err(e) => {
        errors.push(SemanticValidationError::UnresolvedColumnReference {
            view_name: view_def.name.clone(),
            column: filter_expr.clone(),
            context: format!("Filter expression binding failed: {e}"),
        });
    }
}
```

This leverages DataFusion's built-in name resolution against the registered schema, catching binding failures before plan compilation. Per `datafusion_planning.md` S5.1, binding failures are caused by missing tables/functions, ambiguous/unknown columns, quoting/normalization mismatches.

### 2.4 Tests

**New file**: `rust/codeanatomy_engine/tests/semantic_validation.rs`

| Test Case | Input | Expected |
|-----------|-------|----------|
| Valid spec passes | Well-formed spec with compatible types | Empty errors |
| Unknown column in filter | `Filter("nonexistent > 0")` | `UnresolvedColumnReference` error |
| Incompatible join keys | Int32 join Utf8 | `JoinKeyIncompatibility` error |
| Missing output column | OutputTarget requires `col_x`, view lacks it | `OutputContractViolation` |
| Relate with wrong input count | Relate transform with 3 deps | `UnsupportedTransformComposition` |
| Implicit coercion warning | Int32 join Int64 | `ImplicitTypeCoercion` warning |

### 2.5 Acceptance Criteria

- All ViewTransform variants validated for input arity and schema compatibility
- Column references in Filter/Project expressions pre-validated against input schemas
- Join key type compatibility checked with DataFusion-aligned coercion rules
- Output contract columns verified against producing view schemas
- Warnings (not errors) for implicit coercions DataFusion can handle

---

## 3) WS2: Optimizer Pipeline Framework

### 3.1 Goal

Create an explicit optimizer orchestration module that provides configured pass sequences, deterministic pass IDs, per-pass observer output, and policy-controlled experiment modes.

### 3.2 Current State

- `rules/optimizer.rs` (~150 LOC): Contains `SpanContainmentRewriteRule` and `DeltaScanAwareRule` -- both `OptimizerRule` implementations using TreeNode API.
- `rules/physical.rs` (~200 LOC): Contains `CpgPhysicalRule` (filter coalescing, hash join hints) and `CostShapeRule` (repartitioning).
- `rules/registry.rs` (~150 LOC): `CpgRuleSet` -- immutable ordered container with BLAKE3 fingerprint. `RulepackFactory` builds from `RulepackProfile` + `RuleIntent[]`.
- `rules/overlay.rs`: `RuleOverlayProfile` -- per-execution additions/exclusions/priority overrides.
- `stability/optimizer_lab.rs` (~291 LOC): `run_lab_from_ruleset()` + `RuleStep` for offline experimentation.
- **Missing**: No first-class pipeline component for controlled pass sequencing, per-pass digest capture, or configurable strictness modes in the main execution path.

### 3.3 Implementation Steps

#### Step 1: Create `compiler/optimizer_pipeline.rs`

**New file**: `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs`

```rust
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;

/// Configuration for the optimizer pipeline.
pub struct OptimizerPipelineConfig {
    /// Maximum optimizer passes before stopping.
    pub max_passes: usize,
    /// What to do when a rule fails.
    pub failure_policy: RuleFailurePolicy,
    /// Whether to capture per-pass digests (adds overhead).
    pub capture_pass_traces: bool,
    /// Whether to emit plan diffs between passes.
    pub capture_plan_diffs: bool,
}

pub enum RuleFailurePolicy {
    /// Convert rule failure to warning; continue with remaining rules.
    SkipFailed,
    /// Hard-fail the entire compilation on any rule error.
    FailFast,
}

/// Result of running the optimizer pipeline.
pub struct OptimizerPipelineResult {
    /// Optimized logical plan.
    pub optimized_logical: LogicalPlan,
    /// Physical plan (post physical optimization).
    pub physical: Arc<dyn ExecutionPlan>,
    /// Per-pass trace (only populated if capture_pass_traces=true).
    pub pass_traces: Vec<OptimizerPassTrace>,
    /// Warnings from skipped/failed rules.
    pub warnings: Vec<RunWarning>,
}

/// Trace of a single optimizer pass.
pub struct OptimizerPassTrace {
    /// Deterministic pass ID (rule name + pass index).
    pub pass_id: String,
    /// Phase: Analyzer, LogicalOptimizer, PhysicalOptimizer.
    pub phase: OptimizerPhase,
    /// Rule name.
    pub rule_name: String,
    /// BLAKE3 digest of plan before this pass.
    pub before_digest: [u8; 32],
    /// BLAKE3 digest of plan after this pass.
    pub after_digest: [u8; 32],
    /// Whether this pass changed the plan.
    pub plan_changed: bool,
    /// Optional: normalized plan diff (if capture_plan_diffs=true).
    pub plan_diff: Option<String>,
    /// Duration of this pass.
    pub duration_us: u64,
}

pub enum OptimizerPhase {
    Analyzer,
    LogicalOptimizer,
    PhysicalOptimizer,
}
```

#### Step 2: Implement the pipeline orchestrator

```rust
pub async fn run_optimizer_pipeline(
    ctx: &SessionContext,
    unoptimized_plan: LogicalPlan,
    config: &OptimizerPipelineConfig,
) -> Result<OptimizerPipelineResult> {
    let state = ctx.state();
    let mut traces = Vec::new();
    let mut warnings = Vec::new();

    // Phase 1: Analyzer rules
    let analyzed = run_phase(
        &state.analyzer_rules(),
        unoptimized_plan,
        OptimizerPhase::Analyzer,
        config,
        &mut traces,
        &mut warnings,
    )?;

    // Phase 2: Logical optimizer rules
    let optimized_logical = run_phase(
        &state.optimizer_rules(),
        analyzed,
        OptimizerPhase::LogicalOptimizer,
        config,
        &mut traces,
        &mut warnings,
    )?;

    // Phase 3: Physical planning (LogicalPlan -> ExecutionPlan)
    let physical = state.create_physical_plan(&optimized_logical).await?;

    // Phase 4: Physical optimizer rules
    let optimized_physical = run_physical_phase(
        &state.physical_optimizer_rules(),
        physical,
        config,
        &mut traces,
        &mut warnings,
    )?;

    Ok(OptimizerPipelineResult {
        optimized_logical,
        physical: optimized_physical,
        pass_traces: traces,
        warnings,
    })
}
```

The `run_phase` helper iterates rules, computes before/after BLAKE3 digests of `plan.display_indent()`, detects changes, and collects traces.

#### Step 3: Integrate with `plan_compiler.rs`

**Modify**: `rust/codeanatomy_engine/src/compiler/plan_compiler.rs`

Replace direct `ctx.state().create_physical_plan()` calls with `run_optimizer_pipeline()`:

```rust
// Before:
let physical = ctx.state().create_physical_plan(&logical).await?;

// After:
let pipeline_config = OptimizerPipelineConfig::from_runtime_config(&spec.runtime);
let pipeline_result = run_optimizer_pipeline(&ctx, logical, &pipeline_config).await?;
let physical = pipeline_result.physical;
// Store traces in plan bundle if capture enabled
```

#### Step 4: Harden existing `datafusion-tracing` integration

**Modify**: `rust/codeanatomy_engine/src/session/factory.rs`, `rust/codeanatomy_engine/src/executor/tracing/context.rs`, `rust/codeanatomy_engine/src/executor/runner.rs`, `rust/codeanatomy_engine/src/stability/optimizer_lab.rs`.

Reference: `datafusion-tracing` provides:
- `instrument_rules_with_spans!` macro for per-rule span emission
- `RuleInstrumentationOptions::full()` / `.with_plan_diff()` for plan diff capture
- Phase-level spans (`analyze_logical_plan`, `optimize_logical_plan`, `optimize_physical_plan`)

`datafusion-tracing` is already present in the workspace and engine feature graph. Focus implementation on:
- consistent instrumentation mode wiring through runtime profiles
- stable pass-level trace IDs and summary fields
- warning/error propagation for observer failures without breaking compilation unless explicitly configured

#### Step 5: Integrate optimizer lab

**Modify**: `rust/codeanatomy_engine/src/stability/optimizer_lab.rs`

Refactor `run_lab_from_ruleset()` to use `OptimizerPipelineConfig` with `capture_pass_traces=true` and `capture_plan_diffs=true`. The lab becomes a thin caller of the pipeline with full trace capture enabled.

### 3.4 Tests

**New file**: `rust/codeanatomy_engine/tests/optimizer_pipeline.rs`

| Test Case | Input | Expected |
|-----------|-------|----------|
| Pass trace capture | Simple plan with known rule effects | Non-empty pass_traces with correct phase IDs |
| Deterministic digests | Same plan twice | Identical before/after digests per pass |
| SkipFailed policy | Intentionally failing rule | Warning emitted, pipeline completes |
| FailFast policy | Intentionally failing rule | Error returned |
| Plan diff capture | Plan with pushdown effect | `plan_diff` populated with readable diff |
| Trace disabled | capture_pass_traces=false | Empty pass_traces vector |

### 3.5 Acceptance Criteria

- Same spec + same rules -> identical pass traces (deterministic)
- Per-pass digests use BLAKE3 of `display_indent()` (consistent with existing plan hashing)
- `FailFast` mode surfaces rule errors immediately
- `SkipFailed` mode converts rule errors to warnings and continues
- When `tracing` feature enabled, OpenTelemetry spans emitted for each rule
- Optimizer lab uses same pipeline infrastructure (no code duplication)

---

## 4) WS3: Planning Surface Governance Expansion

### 4.1 Goal

Elevate `PlanningSurfaceSpec` into an explicit extension governance model with strict allowlists for file formats, table factories, and planner registries, ensuring the manifest hash captures all planning-affecting state.

### 4.2 Current State

- `session/planning_surface.rs` (~122 LOC): `PlanningSurfaceSpec` with `apply_to_builder()` and `install_rewrites()`.
- `session/planning_manifest.rs` (~180 LOC): `PlanningSurfaceManifest` with `manifest_from_surface()` producing deterministic BLAKE3 hash.
- `session/factory.rs` (~500 LOC): `SessionFactory` with `SessionBuildState` containing `planning_surface_hash`.
- `session/format_policy.rs`: `FormatPolicySpec` for file format registration policy.
- Current manifest hashes: planning surface spec fields, registered formats, and table options. Does NOT yet include: function registry identities, custom table factories, expression planner registries, or DataFusion config keys that affect plan shape.

### 4.3 Implementation Steps

#### Step 1: Expand `PlanningSurfaceSpec`

**Modify**: `rust/codeanatomy_engine/src/session/planning_surface.rs`

Add governance fields:

```rust
pub struct PlanningSurfaceSpec {
    // --- Existing ---
    pub file_formats: Vec<FileFormatEntry>,
    pub table_options: Vec<TableOptionsEntry>,
    pub query_planners: Vec<QueryPlannerEntry>,
    pub expression_planners: Vec<ExpressionPlannerEntry>,
    pub function_rewrites: Vec<FunctionRewriteEntry>,

    // --- New governance fields ---
    /// Explicit allowlist of table factory types.
    pub table_factory_allowlist: Vec<TableFactoryEntry>,
    /// Function registry identity snapshot (names + signatures).
    pub function_registry_identity: FunctionRegistryIdentity,
    /// Planning-affecting config key overrides captured at build time.
    pub planning_config_keys: BTreeMap<String, String>,
    /// Extension registry governance mode.
    pub extension_policy: ExtensionGovernancePolicy,
}

pub struct TableFactoryEntry {
    pub factory_type: String,
    pub identity_hash: [u8; 32],
}

pub struct FunctionRegistryIdentity {
    /// Sorted list of (function_name, return_type_signature).
    pub scalar_functions: Vec<(String, String)>,
    pub aggregate_functions: Vec<(String, String)>,
    pub window_functions: Vec<(String, String)>,
}

pub enum ExtensionGovernancePolicy {
    /// Only explicitly registered extensions are allowed.
    StrictAllowlist,
    /// Warn on unregistered extensions but allow them.
    WarnOnUnregistered,
    /// No governance (legacy behavior).
    Permissive,
}
```

#### Step 2: Capture function registry identity

**Modify**: `session/planning_manifest.rs`

After session build, snapshot all registered functions:

```rust
fn capture_function_registry(ctx: &SessionContext) -> FunctionRegistryIdentity {
    let state = ctx.state();

    let scalar_functions: Vec<(String, String)> = state
        .scalar_functions()
        .iter()
        .map(|(name, udf)| (name.clone(), format!("{:?}", udf.return_type())))
        .sorted()
        .collect();

    // Similarly for aggregate_functions, window_functions

    FunctionRegistryIdentity {
        scalar_functions,
        aggregate_functions,
        window_functions,
    }
}
```

Include in manifest hash computation.

#### Step 3: Capture planning-affecting config keys

**Modify**: `session/planning_manifest.rs`

Define the set of DataFusion config keys that affect plan shape (per `datafusion_planning.md` S2.4, S5.2):

```rust
const PLANNING_AFFECTING_CONFIG_KEYS: &[&str] = &[
    "datafusion.catalog.default_catalog",
    "datafusion.catalog.default_schema",
    "datafusion.sql_parser.enable_ident_normalization",
    "datafusion.sql_parser.dialect",
    "datafusion.sql_parser.parse_float_as_decimal",
    "datafusion.sql_parser.map_string_types_to_utf8view",
    "datafusion.sql_parser.collect_spans",
    "datafusion.execution.enable_ansi_mode",
    "datafusion.optimizer.max_passes",
    "datafusion.optimizer.skip_failed_rules",
    "datafusion.optimizer.prefer_hash_join",
    "datafusion.execution.target_partitions",
    "datafusion.execution.parquet.pushdown_filters",
    "datafusion.execution.parquet.enable_page_index",
    "datafusion.execution.parquet.bloom_filter_on_read",
    "datafusion.execution.collect_statistics",
];
```

Snapshot these from the built `SessionState` and include in the manifest hash.

#### Step 4: Enforce extension governance

**Modify**: `session/factory.rs`

During `build_session_state_internal()`, validate extensions against the governance policy:

```rust
match spec.extension_policy {
    ExtensionGovernancePolicy::StrictAllowlist => {
        // After building session, verify all registered providers/formats
        // are in the explicit allowlists
        for factory in registered_table_factories {
            if !spec.table_factory_allowlist
                .iter()
                .any(|a| a.factory_type == factory.type_name())
            {
                return Err(/* unregistered factory error */);
            }
        }
    }
    ExtensionGovernancePolicy::WarnOnUnregistered => {
        // Same check but emit warnings instead of errors
    }
    ExtensionGovernancePolicy::Permissive => { /* no-op */ }
}
```

### 4.4 Tests

**Modify**: `rust/codeanatomy_engine/tests/session_determinism.rs`

| Test Case | Expected |
|-----------|----------|
| Same surface spec -> same manifest hash | Deterministic |
| Different config key -> different manifest hash | Hash changes |
| Additional UDF registration -> different manifest hash | Hash changes |
| StrictAllowlist with unknown factory -> error | Build fails |
| WarnOnUnregistered with unknown factory -> warning | Build succeeds with warning |
| Config key order does not matter | BTreeMap ensures deterministic ordering |

### 4.5 Acceptance Criteria

- Manifest hash includes function registry identity, config keys, and factory identities
- `StrictAllowlist` mode rejects unregistered extensions
- Config keys listed in `PLANNING_AFFECTING_CONFIG_KEYS` are captured and hashed
- Manifest hash is order-independent (BTreeMap/sorted collections)
- Adding or removing any planning-affecting registration changes the hash

---

## 5) WS4: Pushdown Correctness Contract Enforcement

### 5.1 Goal

Promote pushdown from "probe-only diagnostics" to "planner contract" with compile-time enforcement of residual filter assertions and deterministic warning/hard-fail modes.

### 5.2 Current State

- `providers/pushdown_contract.rs` (~200 LOC): `FilterPushdownStatus` (Unsupported/Inexact/Exact), `PushdownProbe`, `PushdownStatusCounts`.
- `providers/registration.rs` (~150 LOC): `probe_provider_pushdown()` probes filter capabilities at registration time.
- `compiler/pushdown_probe_extract.rs` (~250 LOC): `extract_input_filter_predicates()` derives pushdown probes from filter transforms.
- `compliance/capture.rs` (~200 LOC): `ComplianceCapture` includes `pushdown_probes: BTreeMap<String, PushdownProbe>`.
- **Gap**: Pushdown status is captured but not enforced. An `Inexact` pushdown with no residual filter above the scan is silently correct-but-dangerous.

### 5.3 Implementation Steps

#### Step 1: Define pushdown contract enforcement types

**Modify**: `rust/codeanatomy_engine/src/providers/pushdown_contract.rs`

```rust
/// Contract assertion for a single predicate's pushdown status.
pub struct PushdownContractAssertion {
    pub table_name: String,
    pub predicate_text: String,
    pub declared_status: FilterPushdownStatus,
    pub residual_filter_present: bool,
    pub assertion_result: PushdownContractResult,
}

pub enum PushdownContractResult {
    /// Status and residual presence are consistent.
    Satisfied,
    /// Inexact pushdown but no residual filter above scan.
    InexactWithoutResidual {
        detail: String,
    },
    /// Exact pushdown but residual filter still present (redundant, not incorrect).
    ExactWithRedundantResidual,
    /// Unsupported pushdown but predicate not in residual filter chain.
    UnsupportedPredicateLost {
        detail: String,
    },
}

pub struct PushdownContractReport {
    pub assertions: Vec<PushdownContractAssertion>,
    pub violations: Vec<PushdownContractAssertion>,
    pub enforcement_mode: PushdownEnforcementMode,
}

pub enum PushdownEnforcementMode {
    /// Report violations as warnings (default).
    Warn,
    /// Hard-fail on any Inexact-without-residual violation.
    Strict,
    /// Skip pushdown validation entirely.
    Disabled,
}
```

#### Step 2: Implement post-optimization pushdown verification

**New function in** `compiler/pushdown_probe_extract.rs`:

```rust
pub fn verify_pushdown_contracts(
    optimized_plan: &LogicalPlan,
    pushdown_probes: &BTreeMap<String, PushdownProbe>,
    mode: PushdownEnforcementMode,
) -> Result<PushdownContractReport> {
    // 1. Walk the optimized plan to find TableScan nodes
    // 2. For each scan, extract pushed filters from scan.filters
    // 3. Walk upward to find Filter nodes above the scan
    // 4. For each predicate in the pushdown probe:
    //    - If Inexact: assert a residual filter exists above the scan
    //    - If Exact: note if residual filter redundantly present
    //    - If Unsupported: assert predicate appears in residual filter chain
    // 5. Return report with assertions and violations
}
```

Use DataFusion's `TreeNode` API to walk the optimized `LogicalPlan` and match `TableScan.filters` against `Filter.predicate` nodes above each scan.

Per `datafusion_planning.md` S6.3.2:
- **Inexact**: Provider can push but may miss rows -> residual filter MUST be retained
- **Exact**: Provider guarantees full application -> residual eligible for elimination
- **Unsupported**: No pushdown -> filter remains above scan

#### Step 3: Integrate into compilation pipeline

**Modify**: `compiler/plan_compiler.rs`

After optimizer pipeline produces `optimized_logical`:

```rust
// After optimization, verify pushdown contracts
let pushdown_report = verify_pushdown_contracts(
    &pipeline_result.optimized_logical,
    &pushdown_probes,
    spec.runtime.pushdown_enforcement_mode(),
)?;

match pushdown_report.enforcement_mode {
    PushdownEnforcementMode::Strict => {
        if !pushdown_report.violations.is_empty() {
            return Err(CompilationError::PushdownContractViolation {
                violations: pushdown_report.violations,
            });
        }
    }
    PushdownEnforcementMode::Warn => {
        for violation in &pushdown_report.violations {
            warnings.push(RunWarning::pushdown_contract_warning(violation));
        }
    }
    PushdownEnforcementMode::Disabled => {}
}
```

#### Step 4: Include contract report in artifacts

**Modify**: `compiler/plan_bundle.rs`

Add `pushdown_contract_report: Option<PushdownContractReport>` to `PlanBundleArtifact`.

**Modify**: `compliance/capture.rs`

Replace raw `pushdown_probes` with structured `PushdownContractReport` in `ComplianceCapture`.

### 5.4 Tests

**New file**: `rust/codeanatomy_engine/tests/pushdown_contracts.rs`

| Test Case | Setup | Expected |
|-----------|-------|----------|
| Exact pushdown, no residual | Exact filter, optimizer removes Filter | `Satisfied` |
| Inexact pushdown, residual present | Inexact filter, Filter above scan | `Satisfied` |
| Inexact pushdown, no residual | Inexact filter, Filter optimized away | `InexactWithoutResidual` violation |
| Unsupported, filter in chain | Unsupported filter, Filter above scan | `Satisfied` |
| Strict mode violation -> error | Inexact without residual, Strict mode | Compilation error |
| Warn mode violation -> warning | Inexact without residual, Warn mode | Warning in RunResult |

### 5.5 Acceptance Criteria

- `Inexact` pushdown without residual filter detected and reported
- `Strict` mode fails compilation on pushdown contract violations
- `Warn` mode surfaces violations as structured `RunWarning`
- Contract reports included in plan bundle artifacts
- No false positives: `Exact` pushdown with optimizer-removed filter is `Satisfied`

---

## 6) WS5: Plan Artifact Evolution and Replay Readiness

### 6.1 Goal

Expand `PlanBundleArtifact` to include optimizer pass traces, pushdown contract summaries, provider lineage, and explicit artifact schema versioning.

### 6.2 Current State

- `compiler/plan_bundle.rs` (~700 LOC): `PlanBundleArtifact` with P0/P1/P2 digests, schema fingerprints, explain captures, UDF snapshot, and Substrait optionals.
- `compiler/plan_codec.rs`: Delta extension codec (optional).
- `compiler/substrait.rs`: Substrait encoding (optional).
- **Missing**: No optimizer trace sections, no pushdown summaries, no explicit version migration.

### 6.3 Implementation Steps

#### Step 1: Expand `PlanBundleArtifact`

**Modify**: `rust/codeanatomy_engine/src/compiler/plan_bundle.rs`

```rust
pub struct PlanBundleArtifact {
    // --- Existing fields ---
    pub p0_digest: [u8; 32],        // Unoptimized logical
    pub p1_digest: [u8; 32],        // Optimized logical
    pub p2_digest: [u8; 32],        // Physical plan
    pub p0_text: Option<String>,
    pub p1_text: Option<String>,
    pub p2_text: Option<String>,
    pub explain_verbose: Vec<ExplainEntry>,
    pub explain_analyze: Vec<ExplainEntry>,
    pub schema_fingerprints: SchemaFingerprints,
    pub provider_identities: Vec<ProviderIdentity>,
    pub substrait_bytes: Option<Vec<u8>>,
    pub delta_codec_logical_bytes: Option<Vec<u8>>,
    pub delta_codec_physical_bytes: Option<Vec<u8>>,

    // --- New fields (WS5) ---
    /// Optimizer pass traces (WS2 integration).
    pub optimizer_traces: Vec<OptimizerPassTrace>,
    /// Pushdown contract validation summary (WS4 integration).
    pub pushdown_report: Option<PushdownContractReport>,
    /// Provider lineage: maps scan nodes to source table identities.
    pub provider_lineage: Vec<ProviderLineageEntry>,
    /// Artifact schema version for migration support.
    pub artifact_version: u32,
    /// Replay compatibility flags.
    pub replay_flags: ReplayCompatibilityFlags,
}

pub struct ProviderLineageEntry {
    pub scan_node_id: String,
    pub provider_name: String,
    pub provider_identity_hash: [u8; 32],
    pub delta_version: Option<i64>,
    pub file_count: Option<usize>,
}

pub struct ReplayCompatibilityFlags {
    /// True if all inputs use deterministic version pins.
    pub deterministic_inputs: bool,
    /// True if no volatile UDFs are used in the plan.
    pub no_volatile_udfs: bool,
    /// True if all optimizer rules are deterministic.
    pub deterministic_optimizer: bool,
}

/// Current artifact schema version.
pub const ARTIFACT_SCHEMA_VERSION: u32 = 2;
```

#### Step 2: Implement provider lineage extraction

**New function in** `compiler/plan_bundle.rs`:

```rust
fn extract_provider_lineage(
    optimized_plan: &LogicalPlan,
    provider_identities: &[ProviderIdentity],
) -> Vec<ProviderLineageEntry> {
    // Walk optimized plan, find TableScan nodes, map each to its
    // ProviderIdentity by table_name.
    let mut entries = Vec::new();
    let mut stack = vec![optimized_plan.clone()];

    while let Some(plan) = stack.pop() {
        if let LogicalPlan::TableScan(scan) = &plan {
            if let Some(identity) = provider_identities
                .iter()
                .find(|p| p.table_name == scan.table_name.to_string())
            {
                entries.push(ProviderLineageEntry {
                    scan_node_id: format!(
                        "{}:{}",
                        scan.table_name,
                        scan.projection.as_ref().map_or(0, |p| p.len())
                    ),
                    provider_name: identity.table_name.clone(),
                    provider_identity_hash: identity.identity_hash,
                    delta_version: None, // derived from source metadata enrichment
                    file_count: None,    // derived from source metadata enrichment
                });
            }
        }
        stack.extend(plan.inputs().iter().cloned());
    }

    entries.sort_by(|a, b| a.provider_name.cmp(&b.provider_name));
    entries
}
```

#### Step 3: Implement replay compatibility detection

```rust
fn compute_replay_flags(
    spec: &SemanticExecutionSpec,
    ruleset: &CpgRuleSet,
    provider_identities: &[ProviderIdentity],
) -> ReplayCompatibilityFlags {
    ReplayCompatibilityFlags {
        deterministic_inputs: spec
            .input_relations
            .iter()
            .all(|relation| relation.version_pin.is_some()),
        no_volatile_udfs: spec.view_definitions
            .iter()
            .all(|v| !v.requires_volatile_udfs()),
        deterministic_optimizer: true, // Rules are always deterministic
    }
}
```

#### Step 4: Add artifact version migration

**New function in** `compiler/plan_bundle.rs`:

```rust
pub fn migrate_artifact(artifact: &PlanBundleArtifact) -> Result<PlanBundleArtifact> {
    match artifact.artifact_version {
        1 => {
            // V1 -> V2: Add empty optimizer_traces, None pushdown_report,
            // empty provider_lineage, default replay_flags
            Ok(PlanBundleArtifact {
                artifact_version: 2,
                optimizer_traces: Vec::new(),
                pushdown_report: None,
                provider_lineage: Vec::new(),
                replay_flags: ReplayCompatibilityFlags {
                    deterministic_inputs: false,
                    no_volatile_udfs: false,
                    deterministic_optimizer: true,
                },
                ..artifact.clone()
            })
        }
        2 => Ok(artifact.clone()),
        v => Err(anyhow!("Unknown artifact version: {v}")),
    }
}
```

### 6.4 Tests

**Modify**: `rust/codeanatomy_engine/tests/end_to_end.rs`

| Test Case | Expected |
|-----------|----------|
| Artifact includes optimizer traces when enabled | Non-empty `optimizer_traces` |
| Artifact includes pushdown report | Non-None `pushdown_report` |
| Provider lineage maps all scan nodes | Entry per registered input |
| Replay flags correct for pinned inputs | `deterministic_inputs = true` |
| V1 artifact migrates to V2 | All new fields populated with defaults |
| Artifact serialization roundtrip | serde(serialize) == serde(deserialize) |

### 6.5 Acceptance Criteria

- Artifact version bumped to 2 with explicit migration path from V1
- Optimizer traces embedded in artifacts (when capture enabled)
- Pushdown contract report embedded in artifacts
- Provider lineage traces every scan node to its source identity
- Replay compatibility flags accurately reflect input/UDF/optimizer determinism

---

## 7) WS6: Cost Model + Scheduling Ownership in Rust

### 7.1 Goal

Move task-cost and scheduling authority from Python (`src/relspec/execution_plan.py`, 2,007 LOC) to Rust, replacing the rustworkx-based graph with native Rust graph algorithms.

### 7.2 Current State (Python)

- `execution_plan.py`: `ExecutionPlan` frozen dataclass with task graph, schedule, costs, critical path.
- `rustworkx_graph.py` (1,524 LOC): Task graph construction from `InferredDeps`, transitive reduction, upstream/downstream closure.
- `rustworkx_schedule.py`: HEFT-style scheduling with `ScheduleCostContext`.
- `inferred_deps.py`: `infer_deps_from_view_nodes()` -- dependency inference from DataFusion plan lineage.
- Cost derivation: `derive_task_costs_from_plan()` at `execution_plan.py:1965`.

### 7.3 Implementation Steps

#### Step 1: Create `compiler/scheduling.rs`

**New file**: `rust/codeanatomy_engine/src/compiler/scheduling.rs`

```rust
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

/// Task graph node.
pub struct TaskNode {
    pub name: String,
    pub task_type: TaskType,
    pub estimated_cost: f64,
}

pub enum TaskType {
    View,
    Scan,
    Output,
}

/// Directed acyclic task graph with cost annotations.
pub struct TaskGraph {
    /// Adjacency list: task_name -> set of dependency task names.
    pub dependencies: BTreeMap<String, BTreeSet<String>>,
    /// Task metadata.
    pub nodes: BTreeMap<String, TaskNode>,
    /// Topological order (computed lazily).
    pub topological_order: Vec<String>,
    /// Transitive reduction applied.
    pub is_reduced: bool,
}

/// Scheduling result.
pub struct TaskSchedule {
    /// Ordered list of task names in execution order.
    pub execution_order: Vec<String>,
    /// Critical path: list of task names.
    pub critical_path: Vec<String>,
    /// Critical path weighted length.
    pub critical_path_length: f64,
    /// Bottom-level costs per task.
    pub bottom_level_costs: BTreeMap<String, f64>,
    /// Slack per task (how much it can be delayed without delaying completion).
    pub slack_by_task: BTreeMap<String, f64>,
}

impl TaskGraph {
    /// Build from inferred dependencies.
    pub fn from_inferred_deps(
        view_deps: &[(String, Vec<String>)],
        scan_deps: &[(String, Vec<String>)],
        output_deps: &[(String, Vec<String>)],
    ) -> Result<Self>;

    /// Apply transitive reduction (remove redundant edges).
    pub fn reduce(&mut self) -> ReductionReport;

    /// Prune to active task set.
    pub fn prune(&mut self, active_tasks: &HashSet<String>) -> PruneReport;

    /// Compute topological ordering (Kahn's algorithm).
    pub fn topological_sort(&mut self) -> Result<()>;

    /// Upstream closure: all tasks that must complete before `task_name`.
    pub fn upstream_closure(&self, task_name: &str) -> HashSet<String>;

    /// Downstream closure: all tasks that depend on `task_name`.
    pub fn downstream_closure(&self, task_name: &str) -> HashSet<String>;
}
```

#### Step 2: Create `compiler/cost_model.rs`

**New file**: `rust/codeanatomy_engine/src/compiler/cost_model.rs`

```rust
/// Cost model configuration.
pub struct CostModelConfig {
    /// Default cost for views without prior execution data.
    pub default_view_cost: f64,
    /// Default cost for scan tasks.
    pub default_scan_cost: f64,
    /// Scaling factor for row count -> cost conversion.
    pub row_count_scale: f64,
}

/// Derive task costs from prior execution metrics.
pub fn derive_task_costs(
    metrics: &CollectedMetrics,
    config: &CostModelConfig,
) -> BTreeMap<String, f64>;

/// HEFT-style scheduling: assign tasks to execution slots based on
/// bottom-level priority and dependency constraints.
pub fn schedule_tasks(
    graph: &TaskGraph,
    costs: &BTreeMap<String, f64>,
) -> TaskSchedule {
    // 1. Compute bottom-level costs (longest path from task to sink)
    let bottom_level = compute_bottom_level_costs(graph, costs);

    // 2. Sort tasks by bottom-level cost (descending = highest priority first)
    // 3. Assign tasks respecting dependencies
    let mut execution_order = Vec::new();
    let mut completed = HashSet::new();
    // ... priority-based assignment loop ...

    // 4. Compute critical path and slack
    let critical_path = find_critical_path(graph, &bottom_level);
    let slack = compute_slack(graph, &bottom_level, &critical_path);

    TaskSchedule {
        execution_order,
        critical_path: critical_path.tasks,
        critical_path_length: critical_path.length,
        bottom_level_costs: bottom_level,
        slack_by_task: slack,
    }
}
```

#### Step 3: Wire into execution pipeline

**Modify**: `rust/codeanatomy_engine/src/executor/pipeline.rs`

After `SemanticPlanCompiler::compile()` produces output DataFrames, build the task graph from the compilation's dependency information and compute the schedule:

```rust
// In execute_pipeline():

// Compile spec -> output DataFrames
let compiled = compiler.compile().await?;

// Build task graph from compilation metadata
let task_graph = TaskGraph::from_inferred_deps(
    &compiled.view_deps,
    &compiled.scan_deps,
    &compiled.output_deps,
)?;

// Schedule tasks
let schedule = schedule_tasks(&task_graph, &cost_context);

// Include schedule in RunResult
result_builder.with_schedule(schedule);
```

#### Step 4: Expose schedule metadata via PyO3

**Modify**: `rust/codeanatomy_engine_py/src/result.rs`

Add schedule fields to `PyRunResult`:

```rust
#[pymethods]
impl PyRunResult {
    fn task_schedule(&self) -> PyResult<PyObject> {
        // Serialize TaskSchedule to Python dict
    }

    fn critical_path(&self) -> PyResult<Vec<String>> {
        Ok(self.inner.schedule.critical_path.clone())
    }
}
```

### 7.4 Tests

**New file**: `rust/codeanatomy_engine/tests/scheduling.rs`

| Test Case | Expected |
|-----------|----------|
| Linear dependency chain | Topological order matches chain |
| Diamond dependency | Correct transitive reduction |
| Critical path identification | Longest weighted path returned |
| Slack computation | Non-critical tasks have positive slack |
| Prune to subset | Only active tasks and their dependencies retained |
| Empty graph | Empty schedule (no error) |
| Cycle detection | Error returned |

### 7.5 Acceptance Criteria

- Task graph built from compilation metadata (not from Python InferredDeps)
- Transitive reduction removes redundant edges
- HEFT-style scheduling produces deterministic execution order
- Critical path and slack computed correctly
- Schedule metadata accessible from Python via PyO3
- Python `execution_plan.py` scheduling logic no longer called

---

## 8) WS7: Hard Cutover of Python Planning Stack

### 8.1 Goal

Delete or freeze Python planning internals after Rust replacements from WS1-WS6 are in place.

### 8.2 Prerequisites

- WS1 (Semantic Validator) -- complete and tested
- WS2 (Optimizer Pipeline) -- complete and tested
- WS4 (Pushdown Contracts) -- complete and tested
- WS5 (Artifact Evolution) -- complete and tested
- WS6 (Cost Model + Scheduling) -- complete and tested

### 8.3 Implementation Steps

#### Phase 1: Dual-Read Validation (CI gate)

Before removing Python code, add a CI validation step that runs both paths and compares:

**New file**: `tests/integration/test_dual_planning_validation.py`

```python
def test_rust_python_planning_parity():
    """Compare Rust and Python planning outputs for identical inputs."""
    spec = build_test_spec()

    # Python path (legacy)
    py_plan = compile_execution_plan(session_runtime, request)

    # Rust path (new)
    rust_result = execute_rust_pipeline(spec)

    # Compare
    assert rust_result.schedule.execution_order == py_plan.task_schedule.execution_order
    assert rust_result.schedule.critical_path == py_plan.critical_path_task_names
    # ... additional parity checks
```

Run this in CI for one short transition window (1-2 weeks).

#### Phase 2: Flip default to Rust-authoritative

**Modify**: `src/engine/build_orchestrator.py`

Route all planning through Rust engine, bypassing Python planning stack:

```python
# Before:
plan = compile_execution_plan(session_runtime, request)
# After:
rust_result = engine_facade.execute(session_factory, compiled_spec)
schedule = rust_result.task_schedule()
```

#### Phase 3: Remove Python planning modules

**Delete** (or move to `_deprecated/`):

| File | LOC | Reason |
|------|-----|--------|
| `src/relspec/execution_plan.py` | 2,007 | Replaced by WS6 `scheduling.rs` + `cost_model.rs` |
| `src/datafusion_engine/plan/pipeline.py` | 542 | Replaced by Rust `plan_compiler.rs` + `optimizer_pipeline.rs` |
| `src/datafusion_engine/plan/bundle.py` | 2,589 | Replaced by Rust `plan_bundle.rs` (expanded in WS5) |
| `src/datafusion_engine/plan/execution.py` | 349 | Replaced by Rust `executor/runner.rs` |

**Adapt callers**:

| Caller | Current Import | New Import |
|--------|---------------|------------|
| `build_orchestrator.py` | `compile_execution_plan()` | `engine_facade.execute()` |
| `spec_builder.py` | `DataFusionPlanBundle` | Keep as Python mirror (no change) |
| `views/graph.py` | `build_plan_bundle()` | Rust-side plan bundle capture |

#### Phase 4: Retain Python facades

**Keep unchanged** (boundary/orchestration only):

| File | Reason |
|------|--------|
| `src/engine/spec_builder.py` | Python mirror of Rust SemanticExecutionSpec -- serialization boundary |
| `src/engine/build_orchestrator.py` | CLI orchestration layer (extraction remains Python) |
| `src/engine/runtime.py` | Thin config composition |
| `src/engine/runtime_profile.py` | Environment patching |
| `src/relspec/execution_authority.py` | Validation context (lightweight) |
| `src/relspec/calibration_bounds.py` | Pure data model |

#### Phase 5: Update session factory

**Modify**: `src/datafusion_engine/session/factory.py`

Remove planning-internal logic that duplicates Rust session construction. Retain only:
- Pool management (`DataFusionContextPool`)
- Ephemeral cleanup
- Any Python-only configuration assembly not yet in Rust

### 8.4 Tests

- All existing tests in `tests/e2e/test_cli_build_engine.py` must pass with Rust-only path
- All existing tests in `tests/unit/test_extraction_orchestrator.py` must pass (extraction unchanged)
- Add migration smoke test: `tests/integration/test_cutover_smoke.py` -- exercises full build pipeline end-to-end with Rust planning

### 8.5 Acceptance Criteria

- No runtime path invokes deprecated Python planning modules
- All tests pass with Rust-only planning
- `import src.relspec.execution_plan` from production code paths -> import error or removed
- `import src.datafusion_engine.plan.pipeline` from production code paths -> import error or removed
- Build orchestrator routes directly through Rust engine

---

## 9) WS8: Compatibility Contract and Breaking Change Package

### 9.1 Goal

Ship all changes as a coordinated breaking release with explicit version bumps.

### 9.2 Implementation Steps

#### Step 1: Version bump spec schema

**Modify**: `rust/codeanatomy_engine/src/spec/execution_spec.rs`

```rust
// Bump from current version
pub const SPEC_SCHEMA_VERSION: u32 = 3;  // Was 2

// Enforce unknown-field rejection
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]  // Already present -- enforce at boundary
pub struct SemanticExecutionSpec { ... }
```

**Modify**: `src/engine/spec_builder.py`

Update Python mirror to match version 3 schema.

#### Step 2: Version bump artifact schema

Artifact version already bumped to 2 in WS5. Ensure `deny_unknown_fields` on all Rust boundary types.

#### Step 3: Remove deprecated Python plan/bundle types

After WS7 Phase 3, verify no remaining imports of removed types in production code:

```bash
# CI check: no production imports of removed modules
grep -r "from src.relspec.execution_plan import" src/ --include="*.py"
grep -r "from src.datafusion_engine.plan.pipeline import" src/ --include="*.py"
grep -r "from src.datafusion_engine.plan.bundle import" src/ --include="*.py"
grep -r "from src.datafusion_engine.plan.execution import" src/ --include="*.py"
```

#### Step 4: Enforce strict unknown-field rejection at Rust boundary

**Modify**: All `#[derive(Deserialize)]` types at the Python-Rust boundary to include `#[serde(deny_unknown_fields)]`.

**Modify**: `rust/codeanatomy_engine_py/src/compiler.rs`

Add version check at deserialization:

```rust
fn compile_spec(&self, spec_json: &str) -> PyResult<CompiledPlan> {
    let spec: SemanticExecutionSpec = serde_json::from_str(spec_json)
        .map_err(|e| PyValueError::new_err(
            format!("Spec deserialization failed: {e}")
        ))?;

    if spec.version < SPEC_SCHEMA_VERSION {
        return Err(PyValueError::new_err(format!(
            "Spec version {} is below minimum required version {}",
            spec.version, SPEC_SCHEMA_VERSION
        )));
    }

    // ... compile
}
```

### 9.3 Tests

| Test Case | Expected |
|-----------|----------|
| V2 spec submitted to V3 engine | Clear error with version mismatch message |
| Unknown field in spec JSON | Deserialization error (deny_unknown_fields) |
| V1 artifact loaded by V2 reader | Successful migration via `migrate_artifact()` |
| No production imports of removed modules | CI grep returns empty |

### 9.4 Acceptance Criteria

- Spec schema version bumped to 3
- Artifact schema version bumped to 2
- Unknown fields rejected at Rust boundary
- Version mismatch produces clear error messages
- No backward-compatible shims (clean break)

---

## 9.5) WS9-WS14: Additional Best-in-Class Scopes

These workstreams close the remaining architecture gaps identified during the in-depth review and make the Rust planning stack fully best-in-class beyond baseline cutover.

## WS9: Planner Environment Identity + Governance Control Plane

Goal:
- Elevate planning environment identity into a first-class deterministic contract.

Scope:
- Expand manifest capture to include:
  - planning-affecting config key snapshot
  - default catalog/schema identity
  - catalog provider and factory identity digests
  - function registry identity snapshot
- Add governance policy modes for extension registration:
  - `StrictAllowlist`
  - `WarnOnUnregistered`
  - `Permissive`

Primary files:
- `rust/codeanatomy_engine/src/session/planning_surface.rs`
- `rust/codeanatomy_engine/src/session/planning_manifest.rs`
- `rust/codeanatomy_engine/src/session/factory.rs`

Acceptance:
- Any planning-affecting environment change produces a manifest hash delta.
- Governance mode behavior is deterministic and tested.

## WS10: Standalone Optimizer API Surface

Goal:
- Separate optimizer orchestration from full materialization so rulepack experiments and compile-only analysis are first-class.

Scope:
- Add compile-only optimizer entrypoint that returns:
  - optimized logical plan digest/text
  - per-pass traces with stable pass IDs
  - warning/error report
- Reuse same orchestration in lab and production paths to prevent drift.

Primary files:
- `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs`
- `rust/codeanatomy_engine/src/stability/optimizer_lab.rs`
- `rust/codeanatomy_engine/src/compiler/plan_compiler.rs`

Acceptance:
- Lab and production optimizer traces are structurally equivalent for same inputs.
- Compile-only API can run without materialization path.

## WS11: Statistics-Quality + Cost-Reliability Governance

Goal:
- Prevent unstable cost/scheduling decisions when statistics quality is low.

Scope:
- Add stats quality grading (`exact`, `estimated`, `unknown`) and record in artifacts.
- Gate sensitive cost decisions behind quality thresholds.
- Emit structured warnings when heuristic fallback is used.

Primary files:
- `rust/codeanatomy_engine/src/compiler/cost_model.rs`
- `rust/codeanatomy_engine/src/compiler/scheduling.rs`
- `rust/codeanatomy_engine/src/compiler/plan_bundle.rs`
- `rust/codeanatomy_engine/src/executor/warnings.rs`

Acceptance:
- Scheduling remains deterministic under unknown stats.
- Low-quality stats paths are visible in warnings and artifacts.

## WS12: Delta Protocol + Column-Mapping Compatibility Contract

Goal:
- Make Delta compatibility constraints explicit in planning artifacts and validation.

Scope:
- Capture protocol/table compatibility facts needed for plan correctness.
- Add compatibility checks around schema evolution and column mapping in join/union-heavy plans.
- Add deterministic failure/warning taxonomy for incompatible states.

Primary files:
- `rust/codeanatomy_engine/src/providers/registration.rs`
- `rust/codeanatomy_engine/src/executor/delta_writer.rs`
- `rust/codeanatomy_engine/src/compiler/graph_validator.rs`
- `rust/codeanatomy_engine/src/compiler/plan_bundle.rs`

Acceptance:
- Incompatible Delta protocol/mapping cases fail early with actionable errors.
- Compatible states are persisted in artifact metadata.

## WS13: Portable Plan Policy (Substrait-First Portable Layer)

Goal:
- Cleanly separate portable logical artifacts from environment-sensitive physical diagnostics.

Scope:
- Treat Substrait as the primary portable interchange artifact when capture is enabled.
- Keep physical-plan artifacts as diagnostics only (non-portable by contract).
- Add explicit fingerprint envelope policy for portable bytes.

Primary files:
- `rust/codeanatomy_engine/src/compiler/substrait.rs`
- `rust/codeanatomy_engine/src/compiler/plan_bundle.rs`
- `rust/codeanatomy_engine/src/compiler/plan_codec.rs`

Acceptance:
- Artifact contract explicitly marks portable vs non-portable fields.
- Portability fingerprints are deterministic and version-aware.

## WS14: Custom Source Contract Hardening (FileSource/TableProvider Readiness)

Goal:
- Ensure future custom sources remain optimizer-compatible and pushdown-effective.

Scope:
- Add contract tests for:
  - projection pushdown
  - filter pushdown (`Unsupported|Inexact|Exact`)
  - statistics propagation quality
- Enforce provider/source contract assertions in CI to prevent regression.

Primary files:
- `rust/codeanatomy_engine/src/providers/pushdown_contract.rs`
- `rust/codeanatomy_engine/src/providers/registration.rs`
- `rust/codeanatomy_engine/tests/provider_pushdown_contract.rs`
- `rust/codeanatomy_engine/tests/provider_registration.rs`

---

## 10) Implementation Order and Phasing

### Phase 1: Foundations (WS1 + WS2 + WS3 + WS4 -- parallel)

All four workstreams are independent and can be implemented in parallel.

| Workstream | New Files | Modified Files | Estimated Effort |
|-----------|-----------|----------------|-----------------|
| WS1 | `semantic_validator.rs` | `plan_compiler.rs` | Medium |
| WS2 | `optimizer_pipeline.rs` | `plan_compiler.rs`, `optimizer_lab.rs`, tracing integration modules | Medium-Large |
| WS3 | -- | `planning_surface.rs`, `planning_manifest.rs`, `factory.rs` | Medium |
| WS4 | -- | `pushdown_contract.rs`, `pushdown_probe_extract.rs`, `plan_compiler.rs` | Medium |

### Phase 2: Integration (WS5 + WS6)

Depends on WS1-WS4 being substantially complete.

| Workstream | New Files | Modified Files | Estimated Effort |
|-----------|-----------|----------------|-----------------|
| WS5 | -- | `plan_bundle.rs` | Medium |
| WS6 | `scheduling.rs`, `cost_model.rs` | `pipeline.rs`, `result.rs` | Large |

### Phase 3: Cutover (WS7 + WS8)

Depends on WS5 + WS6 being complete and tested.

| Workstream | New Files | Modified/Deleted Files | Estimated Effort |
|-----------|-----------|----------------------|-----------------|
| WS7 | `test_dual_planning_validation.py`, `test_cutover_smoke.py` | Remove 4 Python files, adapt callers | Large |
| WS8 | -- | Version bumps, boundary enforcement | Small |

### Phase 4: Best-in-Class Hardening (WS9 + WS10 + WS11 + WS12 + WS13 + WS14)

Depends on WS1-WS8 being stable in CI and cutover complete.

| Workstream | New Files | Modified Files | Estimated Effort |
|-----------|-----------|----------------|-----------------|
| WS9 | -- | `planning_surface.rs`, `planning_manifest.rs`, `factory.rs` | Medium |
| WS10 | Optional API files | `optimizer_pipeline.rs`, `optimizer_lab.rs`, `plan_compiler.rs` | Medium |
| WS11 | Optional helper files | `cost_model.rs`, `scheduling.rs`, `plan_bundle.rs`, `warnings.rs` | Medium-Large |
| WS12 | -- | `registration.rs`, `delta_writer.rs`, `graph_validator.rs`, `plan_bundle.rs` | Medium |
| WS13 | -- | `substrait.rs`, `plan_bundle.rs`, `plan_codec.rs` | Medium |
| WS14 | Provider contract tests | `pushdown_contract.rs`, `registration.rs`, provider test suites | Medium |

---

## 11) Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| DataFusion version drift during implementation | Keep workspace pinned at `datafusion = "51.0.0"`; run lockfile-stable test matrix before merges |
| Python planning removal breaks untested code paths | Dual-read validation (WS7 Phase 1) catches regressions before removal |
| Optimizer pipeline overhead from trace capture | Trace capture behind `capture_pass_traces` flag; disabled by default |
| Planning surface manifest hash instability | BTreeMap ordering guarantees; deterministic sorting of all collections |
| Pushdown contract false positives | Conservative: only flag `InexactWithoutResidual`, not `ExactWithRedundantResidual` |
| Breaking changes affect downstream consumers | WS8 provides clear version bumps and error messages |
| Planner environment identity over-captures volatile fields | Restrict to explicit planning-affecting key allowlist and normalized registry identities (WS9) |
| Substrait portability drift across feature/profile combinations | Define portable vs non-portable artifact policy and hash envelope contract (WS13) |
| Custom provider regressions silently degrade optimizer effectiveness | Add provider/source contract CI suites for projection/filter/stats behavior (WS14) |

---

## 12) Testing Strategy Summary

### Unit Tests (per workstream)

| Test File | Workstream | Location |
|-----------|-----------|----------|
| `semantic_validation.rs` | WS1 | `tests/` |
| `optimizer_pipeline.rs` | WS2 | `tests/` |
| `session_determinism.rs` (expanded) | WS3 | `tests/` |
| `pushdown_contracts.rs` | WS4 | `tests/` |
| `end_to_end.rs` (expanded) | WS5 | `tests/` |
| `scheduling.rs` | WS6 | `tests/` |

### Integration Tests (cutover validation)

| Test File | Purpose | Location |
|-----------|---------|----------|
| `test_dual_planning_validation.py` | Rust/Python parity | `tests/integration/` |
| `test_cutover_smoke.py` | End-to-end with Rust-only | `tests/integration/` |

### Best-in-Class Hardening Tests (WS9-WS14)

| Test Focus | Assertion |
|-----------|-----------|
| Planner environment identity hash | Any planning-affecting config/catalog/factory/UDF change updates manifest hash deterministically |
| Portable artifact policy | Substrait portability artifact remains stable; physical diagnostics are treated as non-portable |
| Stats-quality gating | Unknown/estimated stats paths emit warnings and use deterministic fallback scheduling behavior |
| Delta compatibility contract | Protocol/column-mapping incompatibilities fail early with deterministic diagnostics |
| Provider/source contract | Pushdown and projection contract tests prevent regressions in custom source behavior |

### CI Gates

1. Existing: `cargo test` for all Rust tests
2. New: Dual-read validation (Phase 1 of WS7)
3. New: Import guard (WS8) -- grep for removed module imports
4. New: Planner identity drift suite (WS9/WS11)
5. New: Portability and Delta compatibility suites (WS12/WS13)
6. New: Provider/source contract suite (WS14)

---

## 13) DataFusion API Reference Summary

Key DataFusion APIs used across workstreams (per dfdl_ref):

| API | Workstream | Purpose | Reference |
|-----|-----------|---------|-----------|
| `SessionContext::parse_sql_expr(expr, schema)` | WS1 | Schema-aware expression binding validation | `datafusion_planning.md` S3.2.4 |
| `SessionState::create_logical_plan()` | WS1 | Parse-only planning (no DDL side effects) | `datafusion_planning.md` S3.3.6 |
| `SessionContext::add_optimizer_rule()` | WS2 | Custom rule injection | `datafusion_planning.md` S6.5 |
| `SessionContext::remove_optimizer_rule()` | WS2 | Rule exclusion for experiments | `datafusion_planning.md` S6.5 |
| `EXPLAIN VERBOSE` output parsing | WS2 | Rule-by-rule plan deltas | `datafusion_planning.md` S6.2.2 |
| `instrument_rules_with_spans!` | WS2 | OpenTelemetry rule instrumentation | `datafusion-tracing.md` S6 |
| `RuleInstrumentationOptions::full().with_plan_diff()` | WS2 | Plan diff capture | `datafusion-tracing.md` S8 |
| `SessionState::scalar_functions()` | WS3 | Function registry snapshot | `datafusion_planning.md` S2.3 |
| `SessionConfig::set()` | WS3 | Config key capture | `datafusion_planning.md` S2.4 |
| `TreeNode` API (plan walking) | WS4, WS5 | Optimized plan traversal | `Datafusion_logicplan_rust.md` S7 |
| `LogicalPlan::TableScan` fields | WS4, WS5 | Pushdown filter / projection extraction | `datafusion_planning.md` S4.2.1 |
| `FilterPushdownStatus` semantics | WS4 | Unsupported/Inexact/Exact contracts | `deltalake_datafusion_integration.md` S3.3 |
| `LogicalPlanBuilder` | WS6 | Programmatic plan construction | `Datafusion_logicplan_rust.md` S5 |
| `Statistics`, `Precision<T>` | WS6 | Cost estimation inputs | `datafusion_planning.md` S7.1 |
| `SessionConfig` planning-affecting keys snapshot | WS9 | Deterministic planner-environment identity | `datafusion_planning.md` S2.4 |
| `Optimizer` / observer-style pass instrumentation | WS10 | Compile-only optimizer diagnostics API | `datafusion_planning.md` S6 |
| `TableProvider` filter pushdown contract | WS14 | Source/provider correctness hardening | `deltalake_datafusion_integration.md` S3.3 |
| Substrait producer/consumer portability surfaces | WS13 | Portable plan artifact policy | `datafusion_plan_combination.md` S11 |

---

## 14) File Inventory

### New Files

| File | Workstream | Purpose |
|------|-----------|---------|
| `rust/codeanatomy_engine/src/compiler/semantic_validator.rs` | WS1 | Semantic validation framework |
| `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs` | WS2 | Optimizer orchestration |
| `rust/codeanatomy_engine/src/compiler/scheduling.rs` | WS6 | Task graph + scheduling |
| `rust/codeanatomy_engine/src/compiler/cost_model.rs` | WS6 | Cost derivation + HEFT scheduling |
| `tests/integration/test_dual_planning_validation.py` | WS7 | Rust/Python parity validation |
| `tests/integration/test_cutover_smoke.py` | WS7 | End-to-end cutover smoke test |

### Additional New/Expanded Files (WS9-WS14)

| File | Workstream | Purpose |
|------|-----------|---------|
| `rust/codeanatomy_engine/src/compiler/optimizer_pipeline.rs` (expanded API surface) | WS10 | Compile-only optimizer API + deterministic pass observer contract |
| `rust/codeanatomy_engine/tests/provider_pushdown_contract.rs` (expanded) | WS14 | Provider contract hardening tests |
| `rust/codeanatomy_engine/tests/provider_registration.rs` (expanded) | WS12, WS14 | Delta compatibility + source contract assertions |
| `rust/codeanatomy_engine/tests/session_determinism.rs` (expanded) | WS9, WS11 | Planner-environment identity + stats-quality drift checks |

### Modified Files (Rust)

| File | Workstreams | Changes |
|------|------------|---------|
| `compiler/plan_compiler.rs` | WS1, WS2, WS4 | Insert semantic validation, optimizer pipeline, pushdown verification |
| `compiler/plan_bundle.rs` | WS5 | Expand artifact with traces, pushdown reports, lineage, versioning |
| `compiler/pushdown_probe_extract.rs` | WS4 | Add `verify_pushdown_contracts()` |
| `session/planning_surface.rs` | WS3 | Expand spec with governance fields |
| `session/planning_manifest.rs` | WS3 | Capture functions, config keys in manifest hash |
| `session/factory.rs` | WS3 | Extension governance enforcement |
| `providers/pushdown_contract.rs` | WS4 | Add contract assertion/report types |
| `executor/pipeline.rs` | WS6 | Wire task graph + scheduling |
| `stability/optimizer_lab.rs` | WS2 | Refactor to use optimizer pipeline |
| `compliance/capture.rs` | WS4 | Structured pushdown reports |
| `codeanatomy_engine_py/src/result.rs` | WS6 | Expose schedule via PyO3 |
| `Cargo.toml` | WS2 | Verify existing `tracing` feature wiring and keep DataFusion 51-aligned instrumentation dependencies |

### Deleted Files (Python, WS7)

| File | LOC | Replacement |
|------|-----|------------|
| `src/relspec/execution_plan.py` | 2,007 | `compiler/scheduling.rs` + `cost_model.rs` |
| `src/datafusion_engine/plan/pipeline.py` | 542 | `compiler/plan_compiler.rs` + `optimizer_pipeline.rs` |
| `src/datafusion_engine/plan/bundle.py` | 2,589 | `compiler/plan_bundle.rs` |
| `src/datafusion_engine/plan/execution.py` | 349 | `executor/runner.rs` |

### Retained Files (Python, unchanged or minimally adapted)

| File | Role |
|------|------|
| `src/engine/spec_builder.py` | Serialization boundary (Python mirror) |
| `src/engine/build_orchestrator.py` | CLI orchestration |
| `src/engine/runtime.py` | Config composition |
| `src/engine/runtime_profile.py` | Environment patching |
| `src/relspec/execution_authority.py` | Validation context |
| `src/relspec/calibration_bounds.py` | Data model |
| `src/datafusion_engine/session/factory.py` | Pool management (planning internals removed) |
| `src/datafusion_engine/views/graph.py` | View builders (presentation helpers retained) |

---

## 15) Success Metrics

### Determinism

- Same spec + same planning surface + same provider identities -> identical:
  - Envelope hash
  - Planning surface hash (expanded with WS3 governance fields)
  - Plan digests (P0/P1/P2)
  - Optimizer pass traces (WS2)
  - Task schedule execution order (WS6)

### Pushdown Correctness (WS4)

- `Unsupported`: filter remains above scan in optimized plan
- `Inexact`: residual filter retained and recorded in contract report
- `Exact`: residual eligible for elimination

### Cross-Boundary Consistency (WS7)

- Python facade outputs exactly match Rust artifacts
- No Python-only plan logic drift

### Cutover Safety (WS7 + WS8)

- No runtime path invokes deprecated Python planning modules
- All tests pass with Rust-only planning
- Version mismatch produces clear error messages

### Planner Environment Identity (WS9)

- Any planning-affecting environment mutation (catalog defaults, config keys, registry identity) deterministically changes planner identity hash.
- Ordering-only variations do not change hash (stable canonicalization).

### Optimizer Observability (WS10)

- Compile-only optimizer traces and production traces agree on rule ordering and pass IDs.
- Rule failure policy (`FailFast` vs `SkipFailed`) is deterministic and observable in warnings/errors.

### Cost Reliability and Compatibility (WS11 + WS12 + WS13 + WS14)

- Stats-quality grade is captured and warning-emitted when heuristics fallback.
- Delta protocol/column-mapping incompatibilities fail fast with explicit diagnostics.
- Substrait portability artifacts remain stable across repeated runs with fixed inputs.
- Provider/source contract suites enforce projection/filter pushdown correctness and prevent regression.
