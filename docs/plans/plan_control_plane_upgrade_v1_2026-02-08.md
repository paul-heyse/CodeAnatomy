# Plan-Control-Plane Upgrade: Parametric Planning, Plan Artifacts, Semantic Rule/UDF Leverage

Date: 2026-02-08

Prerequisite: `blank_page_implementation_plan_v2_2026-02-08.md` (WS0-WS10 foundation)

---

## 0) Motivation and Gap Analysis

The v2 blank-page plan establishes a Rust-first semantic execution engine (`codeanatomy_engine`) with session factory, plan compiler, rule lifecycle, Delta providers, and execution pipeline. This plan targets the **next stage**: upgrading the plan control plane to exploit the highest-value capabilities identified by gap analysis against the running codebase and DataFusion/DeltaLake stack documentation.

### 0.1 Identified Gaps (Current vs Best-in-Class)

| # | Gap | Current State | Location | Impact |
|---|-----|--------------|----------|--------|
| G1 | Missing canonical P1 artifact contract | `validate_plan()` captures P0 + P2 + EXPLAIN VERBOSE; no canonical optimized logical artifact (P1) with stable persisted schema | `compiler/plan_compiler.rs:386-410` | Weak regression control for optimizer rewrite drift |
| G2 | No Rust-native Substrait portability artifact | Substrait exists in `datafusion_python/src/substrait.rs:126` (Python-only) | Not in `codeanatomy_engine` | No portable logical-plan exchange artifact for external tooling |
| G3 | Env-var templating and SQL literal rendering | `collect_parameter_values()` reads `CODEANATOMY_PARAM_*`; `render_parameter_literal()` formats SQL strings | `compiler/plan_compiler.rs:257-341` | Not strongly typed and not ideal for optimizer-visible parametric plans |
| G4 | Synthetic tuner metrics | `ExecutionMetrics { spill_count: 0, scan_selectivity: 1.0, ... }` | `python/materializer.rs:208-214` | Tuner decisions based on placeholders, not real execution metrics |
| G5 | Delta UDTFs not integrated into compiler DAG + CDF range not wired | `read_delta`, `read_delta_cdf`, `delta_snapshot`, `delta_add_actions` exist; compiler does not model them as transforms | `datafusion_ext/src/udtf_sources.rs`, `compiler/plan_compiler.rs` | Incremental/CDF semantics remain side-orchestrated rather than optimizer-visible |
| G6 | Overlay session rebuild complexity | Proposed overlay design reconstructs session then transfers registrations | `rules/overlay` proposal | Higher drift risk vs cloning from existing `SessionState` |
| G7 | UDF/UDAF/UDWF gap is governance-level, not zero-feature | Multiple advanced hooks already exist; coverage and activation policy are inconsistent | `datafusion_ext/src/udf/*`, `udaf_builtin.rs` | Risk of duplicate work and incomplete high-ROI hook adoption |
| G8 | Cross-view registration boundaries | Every compiled view registered as named view → optimizer sees fragmented graph segments | `compiler/plan_compiler.rs:63-67` | Misses cross-view pushdown/join-reorder opportunities |
| G9 | Artifact model conflates runtime objects and persisted records | Single bundle mixes `LogicalPlan` / `ExecutionPlan` with serializable metadata | `WS-P1 proposal` | Persistence, diffing, and compatibility become brittle |
| G10 | Missing explicit capability matrix and fallback policy | Optional features (`substrait`, tracing, UDTF extensions) lack one canonical enablement matrix | `WS-P2/WS-P13 proposal` | Behavior divergence across environments is hard to diagnose |

### 0.2 Design Principles (Additive to v2)

1. **Plan artifacts are first-class Rust structs** — not formatted debug strings.
2. **Typed parameters compile to `Expr`** — no SQL string interpolation.
3. **Rust binding path uses positional placeholders only** — avoid named-map assumptions in Rust DataFrame binding.
4. **Real metrics from executed plans** — extracted from physical metric trees with stable metric-name folding.
5. **Rules compose per-plan-intent** — new `SessionState` per profile, never in-place mutation.
6. **UDTFs are planning primitives** — compose with joins/unions in one DAG.
7. **UDF hooks are opt-in per-function** — only add where analysis confirms benefit.
8. **Substrait is portability-only** — logical artifact exchange, not deterministic replay contract.
9. **Feature gates are explicit** — each optional capability has declared fallback and diagnostics.

### 0.3 Capability Matrix (Required)

Every run must emit a small capability summary in run metadata:

| Capability | Flag | Default | Fallback |
|------------|------|---------|----------|
| PlanBundle artifact capture | `runtime.compliance_capture` | `false` | No persisted bundle, only baseline run result |
| Substrait portability bytes | `feature=substrait` + `runtime.capture_substrait` | `false` | `substrait_bytes = None`; use schema/rule/provider fingerprints |
| OpenTelemetry tracing | `feature=tracing` + `runtime.enable_tracing` | `false` | No tracing spans; core execution unchanged |
| Rule-level tracing | `runtime.enable_rule_tracing` | `false` | Capture standard `EXPLAIN VERBOSE` only |
| Function factory | `runtime.enable_function_factory` | `false` | Pre-registered builtins only |
| Domain expr planners | `runtime.enable_domain_planner` | `false` | Default planner path |
| Delta maintenance | `spec.maintenance` | `enabled when present` | No maintenance phase |

If requested capability cannot be activated (feature disabled, missing registration), fail fast with a typed diagnostic.

---

## 1) WS-P1: PlanBundle — Structured Plan Artifact Contract

### 1.1 Purpose

Replace formatted debug-string comparisons with a split model:
1. runtime-only plan handles for execution (`PlanBundleRuntime`)
2. persisted, versioned artifact payload for storage/diffing (`PlanBundleArtifact`)

This prevents runtime object leakage into persisted contracts and makes plan comparisons deterministic.

### 1.2 New Structs: Runtime vs Persisted Artifact

```rust
// rust/codeanatomy_engine/src/compiler/plan_bundle.rs

use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Runtime-only plan handles, never serialized.
#[derive(Debug)]
pub struct PlanBundleRuntime {
    pub p0_logical: LogicalPlan,          // unoptimized logical
    pub p1_optimized: LogicalPlan,        // optimized logical
    pub p2_physical: Arc<dyn ExecutionPlan>, // physical
}

/// Persisted, versioned plan artifact payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanBundleArtifact {
    pub artifact_version: u32,

    // Canonical digests (primary diff identity)
    pub p0_digest: [u8; 32],
    pub p1_digest: [u8; 32],
    pub p2_digest: [u8; 32],

    // Optional normalized text for audit/debug
    pub p0_text: Option<String>,
    pub p1_text: Option<String>,
    pub p2_text: Option<String>,

    pub explain_verbose: Vec<ExplainEntry>,
    pub explain_analyze: Vec<ExplainEntry>,

    pub rulepack_fingerprint: [u8; 32],
    pub provider_identities: Vec<ProviderIdentity>,
    pub schema_fingerprints: SchemaFingerprints,

    // Portability-only logical artifact (optional)
    pub substrait_bytes: Option<Vec<u8>>,
    pub sql_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExplainEntry {
    pub plan_type: String,
    pub plan_text: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProviderIdentity {
    pub table_name: String,
    pub identity_hash: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaFingerprints {
    pub p0_schema_hash: [u8; 32],
    pub p1_schema_hash: [u8; 32],
    pub p2_schema_hash: [u8; 32],
}
```

### 1.3 Capture Pipeline

```rust
// rust/codeanatomy_engine/src/compiler/plan_bundle.rs (continued)

use datafusion::prelude::*;

pub async fn capture_plan_bundle_runtime(
    ctx: &SessionContext,
    df: &DataFrame,
) -> Result<PlanBundleRuntime> {
    let p0_logical = df.logical_plan().clone();
    let p1_optimized = ctx.state().optimize(&p0_logical)?;
    let p2_physical = df.create_physical_plan().await?;

    Ok(PlanBundleRuntime {
        p0_logical,
        p1_optimized,
        p2_physical,
    })
}

pub async fn build_plan_bundle_artifact(
    ctx: &SessionContext,
    runtime: &PlanBundleRuntime,
    rulepack_fingerprint: [u8; 32],
    provider_identities: Vec<ProviderIdentity>,
    capture_substrait: bool,
    capture_sql: bool,
) -> Result<PlanBundleArtifact> {
    let p0_text = normalize_logical(&runtime.p0_logical);
    let p1_text = normalize_logical(&runtime.p1_optimized);
    let p2_text = normalize_physical(runtime.p2_physical.as_ref());

    let explain_verbose = capture_explain_verbose(ctx, &runtime.p1_optimized).await?;
    let explain_analyze = capture_explain_analyze(ctx, &runtime.p1_optimized).await?;

    Ok(PlanBundleArtifact {
        artifact_version: 1,
        p0_digest: blake3_hash_bytes(p0_text.as_bytes()),
        p1_digest: blake3_hash_bytes(p1_text.as_bytes()),
        p2_digest: blake3_hash_bytes(p2_text.as_bytes()),
        p0_text: Some(p0_text),
        p1_text: Some(p1_text),
        p2_text: Some(p2_text),
        explain_verbose,
        explain_analyze,
        rulepack_fingerprint,
        provider_identities,
        schema_fingerprints: SchemaFingerprints {
            p0_schema_hash: hash_schema(runtime.p0_logical.schema()),
            p1_schema_hash: hash_schema(runtime.p1_optimized.schema()),
            p2_schema_hash: hash_schema(&runtime.p2_physical.schema()),
        },
        substrait_bytes: if capture_substrait {
            try_substrait_encode(ctx, &runtime.p1_optimized).ok()
        } else {
            None
        },
        sql_text: if capture_sql {
            try_sql_unparse(&runtime.p1_optimized).ok()
        } else {
            None
        },
    })
}
```

### 1.4 PlanDiff Uses Digests, Not Debug Strings

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanDiff {
    pub p0_changed: bool,
    pub p1_changed: bool,
    pub p2_changed: bool,
    pub schema_drift: bool,
    pub rulepack_changed: bool,
    pub providers_changed: bool,
    pub summary: Vec<String>,
}

pub fn diff_artifacts(a: &PlanBundleArtifact, b: &PlanBundleArtifact) -> PlanDiff {
    let p0_changed = a.p0_digest != b.p0_digest;
    let p1_changed = a.p1_digest != b.p1_digest;
    let p2_changed = a.p2_digest != b.p2_digest;
    let schema_drift = a.schema_fingerprints.p0_schema_hash != b.schema_fingerprints.p0_schema_hash
        || a.schema_fingerprints.p1_schema_hash != b.schema_fingerprints.p1_schema_hash
        || a.schema_fingerprints.p2_schema_hash != b.schema_fingerprints.p2_schema_hash;
    let rulepack_changed = a.rulepack_fingerprint != b.rulepack_fingerprint;
    let providers_changed = a.provider_identities != b.provider_identities;

    let mut summary = Vec::new();
    if p0_changed {
        summary.push("P0 logical digest changed".into());
    }
    if p1_changed {
        summary.push("P1 optimized digest changed".into());
    }
    if p2_changed {
        summary.push("P2 physical digest changed".into());
    }
    if schema_drift {
        summary.push("Schema fingerprints changed".into());
    }
    if rulepack_changed {
        summary.push("Rulepack fingerprint changed".into());
    }
    if providers_changed {
        summary.push("Provider identities changed".into());
    }

    PlanDiff {
        p0_changed,
        p1_changed,
        p2_changed,
        schema_drift,
        rulepack_changed,
        providers_changed,
        summary,
    }
}
```

### 1.5 Integration with Existing Compiler

`SemanticPlanCompiler::compile()` should return runtime plan handles for immediate execution and optional persisted artifacts for compliance/debug capture.

```rust
pub async fn compile(
    &self,
) -> Result<Vec<(OutputTarget, DataFrame, Option<PlanBundleRuntime>, Option<PlanBundleArtifact>)>> {
    let capture_artifacts = self.spec.runtime.compliance_capture;
    let mut outputs = Vec::new();

    for target in &self.spec.output_targets {
        let df = self.ctx.table(&target.source_view).await?;
        let projected = df.select(
            target.columns.iter().map(|c| col(c)).collect::<Vec<_>>(),
        )?;

        if capture_artifacts {
            let runtime = capture_plan_bundle_runtime(self.ctx, &projected).await?;
            let artifact = build_plan_bundle_artifact(
                self.ctx,
                &runtime,
                self.rulepack_fingerprint,
                self.provider_identities.clone(),
                cfg!(feature = "substrait") && self.spec.runtime.capture_substrait,
                true,
            )
            .await?;
            outputs.push((target.clone(), projected, Some(runtime), Some(artifact)));
        } else {
            outputs.push((target.clone(), projected, None, None));
        }
    }

    Ok(outputs)
}
```

### 1.6 SQL Unparse via datafusion-sql Unparser

```rust
use datafusion_sql::unparser::plan_to_sql;

fn try_sql_unparse(plan: &LogicalPlan) -> Result<String> {
    Ok(plan_to_sql(plan)?.to_string())
}
```

### 1.7 Implementation Checklist

- [ ] Create `compiler/plan_bundle.rs` with **both** `PlanBundleRuntime` and `PlanBundleArtifact`
- [ ] Implement `capture_plan_bundle_runtime()` (P0/P1/P2 capture path)
- [ ] Implement `build_plan_bundle_artifact()` (digest-first persisted payload)
- [ ] Use digest-based `PlanDiff` (`p0_digest/p1_digest/p2_digest`) instead of debug-string comparisons
- [ ] Implement `try_sql_unparse()` using `datafusion_sql::unparser::plan_to_sql()`
- [ ] Add `datafusion-sql = { version = "51.0.0", features = ["unparser"] }` to Cargo.toml
- [ ] Integrate optional artifact capture into `SemanticPlanCompiler::compile()`
- [ ] Wire artifact capture to `runtime.compliance_capture`
- [ ] Extend `RunResult` with optional serialized `Vec<PlanBundleArtifact>`
- [ ] Test: P1 digest differs from P0 digest when optimizer rules fire
- [ ] Test: `diff_artifacts()` flags rulepack/provider/schema drift correctly
- [ ] Test: identical inputs produce stable digests in repeated runs
- [ ] Test: SQL unparse produces valid SQL text for representative plans

---

## 2) WS-P2: Rust-Native Substrait Round-Trip

### 2.1 Purpose

Add Substrait serialization to `codeanatomy_engine` as a **portability artifact** for logical plans.

This workstream is explicitly not a deterministic replay contract. Determinism remains governed by:
1. spec hash
2. session envelope/runtime profile hash
3. rulepack fingerprint
4. provider identity fingerprints

### 2.2 Substrait Integration (Logical-Only)

```rust
// rust/codeanatomy_engine/src/compiler/substrait.rs

use datafusion::prelude::*;
use datafusion::logical_expr::LogicalPlan;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use prost::Message;

/// Encode an optimized logical plan to Substrait bytes.
///
/// Substrait encoding is logical-only: the encoded plan captures
/// the computation graph but not physical execution decisions.
/// This makes it suitable for:
/// - Portability to external tooling
/// - Logical-plan comparison support
/// - Optional compliance artifacts
///
/// Note: Execution always remains SessionContext-bound. Substrait
/// plans are never executed directly.
pub fn encode_substrait(
    ctx: &SessionContext,
    plan: &LogicalPlan,
) -> Result<Vec<u8>> {
    let state = ctx.state();
    let substrait_plan = to_substrait_plan(plan, &state)?;
    let mut bytes = Vec::new();
    substrait_plan.encode(&mut bytes)?;
    Ok(bytes)
}

/// Decode Substrait bytes back to a LogicalPlan.
///
/// The decoded plan is bound to the provided SessionContext,
/// which must have compatible catalog/schema registrations.
pub async fn decode_substrait(
    ctx: &SessionContext,
    bytes: &[u8],
) -> Result<LogicalPlan> {
    let substrait_plan = substrait::proto::Plan::decode(bytes)?;
    let state = ctx.state();
    let plan = from_substrait_plan(&state, &substrait_plan).await?;
    Ok(plan)
}

/// Compute a portability hash from Substrait bytes.
///
/// Hash equality is a useful signal, but not a standalone determinism contract.
pub fn substrait_plan_hash(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(bytes);
    *hasher.finalize().as_bytes()
}
```

### 2.3 Integration with PlanBundle

The Substrait bytes are captured in `PlanBundleArtifact` when:
1. `feature = "substrait"` is enabled
2. runtime capability `capture_substrait` is enabled

If encoding fails, behavior is governed by capability policy:
1. strict mode: fail with typed diagnostic
2. permissive mode: store `substrait_bytes = None` and continue with other fingerprints

### 2.4 Cargo.toml Addition

```toml
# In [dependencies]
datafusion-substrait = { version = "51.0.0", optional = true }
# Optional only if explicit proto decode APIs are required:
substrait = { version = "0.52", optional = true }
prost = "0.13"

# In [features]
substrait = ["dep:datafusion-substrait"]
```

### 2.5 Implementation Checklist

- [ ] Create `compiler/substrait.rs` with `encode_substrait()`, `decode_substrait()`, `substrait_plan_hash()`
- [ ] Add `datafusion-substrait` as optional dependency (add `substrait` crate only if explicit proto typing is required)
- [ ] Add `substrait` feature flag to Cargo.toml
- [ ] Wire `PlanBundleArtifact` builder to call `encode_substrait()` when capability is enabled
- [ ] Test: round-trip encode→decode produces equivalent LogicalPlan
- [ ] Test: Substrait hash participates in portability diagnostics but is not treated as replay contract
- [ ] Test: Substrait bytes are None when feature is disabled (zero overhead)
- [ ] Test: Substrait encoding handles all ViewTransform variants (normalize, join, union, etc.)

---

## 3) WS-P3: Typed Parametric Planning

### 3.1 Purpose

Replace env-var string templating with typed bindings that compile directly to `Expr` nodes, while enforcing a single Rust binding constraint:

1. placeholder bindings are positional (`$1`, `$2`, ...) and compiled to `ParamValues::List`
2. no named-map binding path in Rust (`ParamValues::Map` is not used)

### 3.2 Current State Analysis

The current implementation at `plan_compiler.rs:257-341`:
1. `collect_parameter_values()` reads `CODEANATOMY_PARAM_*` environment variables
2. `render_parameter_literal()` converts raw strings to SQL literal text via type-specific formatting
3. `resolve_source_with_templates()` applies string-substituted SQL filters and registers temporary views

This approach has three problems:
- Environment variable coupling (not testable, not composable)
- SQL string interpolation (injection risk, not optimizer-visible)
- Per-template temporary view registration (unnecessary intermediate boundaries)

### 3.3 New Spec Struct: TypedParameter (Positional-First)

```rust
// rust/codeanatomy_engine/src/spec/parameters.rs

use datafusion::common::ScalarValue;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedParameter {
    /// Optional diagnostic label (not used for binding identity).
    pub label: Option<String>,
    /// Parameter target: positional placeholder or direct filter binding.
    pub target: ParameterTarget,
    /// Typed value.
    pub value: ParameterValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ParameterTarget {
    /// Positional placeholder binding: `$1`, `$2`, ...
    PlaceholderPos { position: u32 },
    /// Direct typed filter expression (no SQL literal interpolation).
    FilterEq {
        base_table: Option<String>,
        filter_column: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum ParameterValue {
    Utf8(String),
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    Date(String),           // ISO 8601 date string
    Timestamp(String),      // ISO 8601 timestamp string
    Null { data_type: String },
}

impl ParameterValue {
    /// Convert to DataFusion ScalarValue.
    pub fn to_scalar_value(&self) -> Result<ScalarValue> {
        match self {
            Self::Utf8(v) => Ok(ScalarValue::Utf8(Some(v.clone()))),
            Self::Int64(v) => Ok(ScalarValue::Int64(Some(*v))),
            Self::Float64(v) => Ok(ScalarValue::Float64(Some(*v))),
            Self::Boolean(v) => Ok(ScalarValue::Boolean(Some(*v))),
            Self::Date(v) => Ok(ScalarValue::Date32(Some(parse_date32(v)?))),
            Self::Timestamp(v) => Ok(ScalarValue::TimestampMicrosecond(
                Some(parse_timestamp_micros(v)?), None,
            )),
            Self::Null { data_type } => {
                let dt = parse_data_type(data_type)?;
                Ok(ScalarValue::try_from(&dt)?)
            }
        }
    }
}
```

### 3.4 Typed Parameter Compilation

```rust
// rust/codeanatomy_engine/src/compiler/param_compiler.rs

use datafusion::prelude::*;
use datafusion::common::ParamValues;

/// Compile placeholder parameters into positional ParamValues::List.
///
/// Contract:
/// - positions are 1-based
/// - no gaps are allowed (1..N must be present)
/// - duplicate positions are rejected
pub fn compile_positional_param_values(
    params: &[TypedParameter],
) -> Result<ParamValues> {
    let mut positional: Vec<(u32, ScalarValue)> = Vec::new();
    for p in params {
        if let ParameterTarget::PlaceholderPos { position } = p.target {
            positional.push((position, p.value.to_scalar_value()?));
        }
    }
    if positional.is_empty() {
        return Ok(ParamValues::List(vec![]));
    }

    positional.sort_by_key(|(pos, _)| *pos);
    for (idx, (pos, _)) in positional.iter().enumerate() {
        let expected = (idx as u32) + 1;
        if *pos != expected {
            return plan_err!(
                "typed_parameters placeholder positions must be contiguous from 1; expected ${expected}, got ${pos}"
            );
        }
    }

    Ok(ParamValues::List(
        positional.into_iter().map(|(_, v)| v).collect(),
    ))
}

/// Apply typed parameters using positional binding + typed filter expressions.
pub async fn apply_typed_parameters(
    df: DataFrame,
    params: &[TypedParameter],
) -> Result<DataFrame> {
    if params.is_empty() {
        return Ok(df);
    }

    let placeholder_values = compile_positional_param_values(params)?;
    let mut result = if matches!(placeholder_values, ParamValues::List(ref v) if !v.is_empty()) {
        df.with_param_values(placeholder_values)?
    } else {
        df
    };

    for param in params {
        if let ParameterTarget::FilterEq { filter_column, .. } = &param.target {
            let scalar = param.value.to_scalar_value()?;
            result = result.filter(col(filter_column).eq(lit(scalar)))?;
        }
    }

    Ok(result)
}
```

### 3.5 Spec Extension

The `SemanticExecutionSpec` at `spec/execution_spec.rs` gains a new field:

```rust
// Addition to SemanticExecutionSpec
pub struct SemanticExecutionSpec {
    // ... existing fields ...

    /// Typed parameter bindings (replaces env-var binding path).
    /// Placeholder bindings are positional only.
    pub typed_parameters: Vec<TypedParameter>,

    /// Legacy: parameter_templates retained for backward compatibility.
    /// Deprecated: use typed_parameters instead.
    #[serde(default)]
    pub parameter_templates: Vec<ParameterTemplate>,
}
```

### 3.6 Backward Compatibility

Migration path is explicit:
1. compiler uses `typed_parameters` first
2. legacy `parameter_templates` + env-var path remains temporarily
3. emit deprecation warning whenever legacy path is selected
4. remove legacy path after all producers are migrated to typed positional bindings

### 3.7 Implementation Checklist

- [ ] Create `spec/parameters.rs` with `TypedParameter`, `ParameterTarget`, `ParameterValue`
- [ ] Implement `ParameterValue::to_scalar_value()` for all type variants
- [ ] Create `compiler/param_compiler.rs` with `compile_positional_param_values()` and `apply_typed_parameters()`
- [ ] Strategy 1: `DataFrame::with_param_values(ParamValues::List(...))` for positional placeholders
- [ ] Strategy 2: Typed `col().eq(lit(scalar))` for filter-scoped bindings
- [ ] Reject non-contiguous or duplicate placeholder positions with typed diagnostics
- [ ] Add `typed_parameters: Vec<TypedParameter>` field to `SemanticExecutionSpec`
- [ ] Update plan compiler to check typed_parameters before falling back to legacy env-var path
- [ ] Update Python `spec_builder.py` msgspec mirror with `TypedParameter` struct
- [ ] Update `build_spec_from_ir()` to populate typed_parameters from Python semantic IR
- [ ] Test: typed Int64 parameter compiles to correct filter predicate
- [ ] Test: typed Utf8 parameter binds via positional placeholder (`$1`, `$2`, ...) without SQL parsing
- [ ] Test: placeholder position validation rejects gaps (`$1`, `$3`) and duplicates
- [ ] Test: plan shape remains stable across different parameter values (same ScalarValue type)
- [ ] Test: legacy env-var path still works when typed_parameters is empty
- [ ] Test: mixed positional placeholder + filter-scoped bindings apply correctly

---

## 4) WS-P4: Dynamic Rule Composition per Plan Intent

### 4.1 Purpose

Promote rule intents from static-at-build to dynamically composed per plan execution intent. A new `RuleOverlayProfile` allows the caller to specify per-execution rule additions, removals, or ordering changes without rebuilding the entire session.

### 4.2 Design: Profile Switch, Not In-Place Mutation

Following the v2 immutability principle (no `remove_optimizer_rule`, no `add_physical_optimizer_rule`), dynamic rule composition works by building a **new `SessionState`** from the base state with the overlay applied:

```rust
// rust/codeanatomy_engine/src/rules/overlay.rs

use datafusion::execution::session_state::SessionStateBuilder;
use std::sync::Arc;
use crate::rules::registry::CpgRuleSet;
use crate::rules::rulepack::RulepackFactory;
use crate::spec::rule_intents::{RuleIntent, RulepackProfile};

/// Per-execution rule overlay specification.
///
/// Allows callers to customize rules for a specific compilation
/// without rebuilding the session from scratch.
///
/// The overlay produces a NEW SessionState via SessionStateBuilder —
/// it never mutates the existing session's rules in place.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleOverlayProfile {
    /// Additional rule intents to include (appended after base rules).
    pub additional_intents: Vec<RuleIntent>,

    /// Rule names to exclude from the base ruleset.
    /// Exclusion happens at build time, not via post-build removal.
    pub exclude_rules: Vec<String>,

    /// Explicit rule ordering overrides.
    /// Maps rule name → priority (lower = earlier). Rules not listed
    /// retain their default order.
    pub priority_overrides: Vec<RulePriority>,

    /// If true, capture per-rule EXPLAIN deltas showing each rule's effect.
    pub explain_per_rule: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RulePriority {
    pub rule_name: String,
    pub priority: i32,
}

/// Build a new CpgRuleSet from a base profile + overlay.
///
/// Returns a new immutable ruleset. The base session is NOT mutated.
pub fn build_overlaid_ruleset(
    base_profile: &RulepackProfile,
    base_intents: &[RuleIntent],
    overlay: &RuleOverlayProfile,
    env_profile: &EnvironmentProfile,
) -> CpgRuleSet {
    // 1. Start with base ruleset
    let mut combined_intents: Vec<RuleIntent> = base_intents.to_vec();

    // 2. Append additional intents from overlay
    combined_intents.extend(overlay.additional_intents.clone());

    // 3. Build the combined ruleset
    let mut ruleset = RulepackFactory::build_ruleset(
        base_profile, &combined_intents, env_profile,
    );

    // 4. Apply exclusions (filter at build time, not post-build removal)
    let exclude_set: HashSet<&str> = overlay.exclude_rules.iter()
        .map(|s| s.as_str()).collect();
    ruleset.analyzer_rules.retain(|r| !exclude_set.contains(r.name()));
    ruleset.optimizer_rules.retain(|r| !exclude_set.contains(r.name()));
    ruleset.physical_rules.retain(|r| !exclude_set.contains(r.name()));

    // 5. Apply priority ordering
    if !overlay.priority_overrides.is_empty() {
        apply_priority_ordering(&mut ruleset, &overlay.priority_overrides);
    }

    // 6. Recompute fingerprint (overlay changes the identity)
    ruleset.fingerprint = compute_ruleset_fingerprint(
        &ruleset.analyzer_rules,
        &ruleset.optimizer_rules,
        &ruleset.physical_rules,
    );

    ruleset
}

/// Build a new SessionContext with the overlaid rules.
///
/// Creates a fresh SessionState from the existing state, replacing only
/// rule vectors. This avoids manual registration-transfer drift.
pub async fn build_overlaid_session(
    base_ctx: &SessionContext,
    overlaid_ruleset: &CpgRuleSet,
) -> Result<SessionContext> {
    let state = base_ctx.state();

    let new_state = SessionStateBuilder::new_from_existing(state.clone())
        .with_analyzer_rules(overlaid_ruleset.analyzer_rules.clone())
        .with_optimizer_rules(overlaid_ruleset.optimizer_rules.clone())
        .with_physical_optimizer_rules(overlaid_ruleset.physical_rules.clone())
        .build();
    Ok(SessionContext::new_with_state(new_state))
}
```

### 4.3 Per-Rule EXPLAIN Deltas

```rust
// rust/codeanatomy_engine/src/rules/overlay.rs (continued)

/// Capture rule effects from EXPLAIN VERBOSE output.
///
/// This path avoids direct per-rule rewrite loops that can diverge from the
/// actual optimizer pipeline. Rule names and plan transitions are parsed from
/// DataFusion-generated explain rows.
pub async fn capture_per_rule_deltas(df: &DataFrame) -> Result<Vec<RuleDelta>> {
    let batches = df.explain(true, false)?.collect().await?;
    parse_rule_deltas_from_explain_verbose(&batches)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleDelta {
    pub rule_name: String,
    pub changed: bool,
    pub plan_after: Option<String>,
}
```

### 4.4 Spec Extension

```rust
// Addition to SemanticExecutionSpec
pub struct SemanticExecutionSpec {
    // ... existing fields ...

    /// Optional per-execution rule overlay.
    /// When present, a new SessionState is built with the overlay applied.
    #[serde(default)]
    pub rule_overlay: Option<RuleOverlayProfile>,
}
```

### 4.5 Implementation Checklist

- [ ] Create `rules/overlay.rs` with `RuleOverlayProfile`, `RulePriority`, `RuleDelta` structs
- [ ] Implement `build_overlaid_ruleset()` with intent combination, exclusion, and priority ordering
- [ ] Implement `build_overlaid_session()` using `SessionStateBuilder::new_from_existing(...)`
- [ ] Do not use registration transfer helpers; inherit registrations via cloned base state
- [ ] Implement `capture_per_rule_deltas()` via EXPLAIN VERBOSE parsing
- [ ] Add `rule_overlay: Option<RuleOverlayProfile>` to `SemanticExecutionSpec`
- [ ] Wire overlay into `CpgMaterializer::execute()` pipeline (between ruleset build and session build)
- [ ] Recompute fingerprint after overlay to maintain determinism contract
- [ ] Test: overlay adding an intent produces expected rule in new ruleset
- [ ] Test: overlay excluding a rule removes it from new ruleset (not from base session)
- [ ] Test: priority ordering changes deterministic rule application order
- [ ] Test: per-rule EXPLAIN deltas capture individual rule effects from DataFusion explain output
- [ ] Test: base session is unmodified after overlay session is created
- [ ] Test: fingerprint changes when overlay is applied (determinism tracking)

---

## 5) WS-P5: Delta UDTFs as Planning Primitives

### 5.1 Purpose

Leverage existing Delta table functions (`read_delta`, `read_delta_cdf`, `delta_snapshot`, `delta_add_actions`) as planning primitives in the engine. This allows incremental semantics (CDF-based change detection) and table metadata queries to be expressed as optimizer-visible plan nodes rather than side-channel orchestration.

### 5.2 Current State

Four UDTFs exist in `datafusion_ext/src/udtf_sources.rs`:
1. `read_delta(uri)` → Full Delta scan with session predicate context
2. `read_delta_cdf(uri)` → Change Data Feed scan (currently URI-only; version/timestamp range wiring is missing)
3. `delta_snapshot(uri)` → Single-row metadata table (version, schema, protocol, properties)
4. `delta_add_actions(uri)` → Multi-row file manifest (path, size, stats, partitions)

These are registered in the session but not used by the engine compiler.

### 5.3 New ViewTransform Variants

```rust
// Extension to spec/relations.rs ViewTransform enum

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum ViewTransform {
    // ... existing variants ...

    /// Incremental change detection via Delta CDF.
    ///
    /// Expresses a change-data-feed scan as a planning primitive,
    /// allowing the optimizer to push down predicates into the CDF scan
    /// and compose the result with downstream joins/unions.
    IncrementalCdf {
        /// Delta table location
        source: String,
        /// Starting version for CDF range (inclusive)
        starting_version: Option<i64>,
        /// Ending version for CDF range (inclusive)
        ending_version: Option<i64>,
        /// Starting timestamp (ISO 8601)
        starting_timestamp: Option<String>,
        /// Ending timestamp (ISO 8601)
        ending_timestamp: Option<String>,
    },

    /// Table metadata inspection via delta_snapshot UDTF.
    ///
    /// Produces a single-row relation with table metadata:
    /// version, schema_json, protocol, properties, partition_columns.
    /// Useful for schema-aware conditional compilation.
    Metadata {
        /// Delta table location
        source: String,
    },

    /// File manifest via delta_add_actions UDTF.
    ///
    /// Produces one row per file in the Delta table, with path, size,
    /// stats, and partition values. Useful for file-level cost estimation
    /// and selective file scanning.
    FileManifest {
        /// Delta table location
        source: String,
    },
}
```

### 5.4 UDTF View Builders

```rust
// rust/codeanatomy_engine/src/compiler/udtf_builder.rs

use datafusion::prelude::*;

/// Build an IncrementalCdf view using the read_delta_cdf UDTF.
///
/// The CDF result is a standard relation with change_type column
/// (insert, update_preimage, update_postimage, delete) that can
/// be composed with downstream joins/unions/filters in one DAG.
pub async fn build_incremental_cdf(
    ctx: &SessionContext,
    source: &str,
    starting_version: Option<i64>,
    ending_version: Option<i64>,
    starting_timestamp: Option<&str>,
    ending_timestamp: Option<&str>,
) -> Result<DataFrame> {
    // Required closure:
    // 1) extend `read_delta_cdf` UDTF signature to accept optional range args
    //    read_delta_cdf(uri, start_version, end_version, start_ts, end_ts)
    // 2) route those args to delta_control_plane::delta_cdf_provider(...)
    //
    // Use positional placeholders and ParamValues::List (no SQL interpolation).
    let template = ctx
        .sql(
            "SELECT * FROM read_delta_cdf($1, $2, $3, $4, $5)",
        )
        .await?;
    let bound = template.with_param_values(ParamValues::List(vec![
        ScalarValue::Utf8(Some(source.to_string())),
        starting_version.map_or(ScalarValue::Int64(None), |v| ScalarValue::Int64(Some(v))),
        ending_version.map_or(ScalarValue::Int64(None), |v| ScalarValue::Int64(Some(v))),
        starting_timestamp
            .map(|v| ScalarValue::Utf8(Some(v.to_string())))
            .unwrap_or(ScalarValue::Utf8(None)),
        ending_timestamp
            .map(|v| ScalarValue::Utf8(Some(v.to_string())))
            .unwrap_or(ScalarValue::Utf8(None)),
    ]))?;
    Ok(bound)
}

/// Build a Metadata view using the delta_snapshot UDTF.
pub async fn build_metadata(
    ctx: &SessionContext,
    source: &str,
) -> Result<DataFrame> {
    let sql = format!("SELECT * FROM delta_snapshot('{source}')");
    ctx.sql(&sql).await
}

/// Build a FileManifest view using the delta_add_actions UDTF.
pub async fn build_file_manifest(
    ctx: &SessionContext,
    source: &str,
) -> Result<DataFrame> {
    let sql = format!("SELECT * FROM delta_add_actions('{source}')");
    ctx.sql(&sql).await
}
```

### 5.5 Integration with Plan Compiler

The `compile_view()` dispatch in `plan_compiler.rs:168-254` is extended to route the new variants:

```rust
// Addition to compile_view() match arm
ViewTransform::IncrementalCdf {
    source, starting_version, ending_version,
    starting_timestamp, ending_timestamp,
} => {
    udtf_builder::build_incremental_cdf(
        self.ctx, source,
        *starting_version, *ending_version,
        starting_timestamp.as_deref(), ending_timestamp.as_deref(),
    ).await
}
ViewTransform::Metadata { source } => {
    udtf_builder::build_metadata(self.ctx, source).await
}
ViewTransform::FileManifest { source } => {
    udtf_builder::build_file_manifest(self.ctx, source).await
}
```

### 5.6 CDF-Aware Downstream Composition

The key value of UDTF-as-planning-primitive is that CDF results compose with the rest of the DAG:

```
IncrementalCdf("symbols") → filter(cdf_is_upsert) → join(normalized_symbols) → output
```

This is expressed entirely as connected plan nodes. The optimizer can push predicates into the CDF scan, merge join strategies, and optimize the full graph as a unit.

### 5.7 Implementation Checklist

- [ ] Add `IncrementalCdf`, `Metadata`, `FileManifest` variants to `ViewTransform` enum
- [ ] Create `compiler/udtf_builder.rs` with `build_incremental_cdf()`, `build_metadata()`, `build_file_manifest()`
- [ ] Ensure `read_delta_cdf`, `delta_snapshot`, `delta_add_actions` UDTFs are registered during session setup (via existing `datafusion_ext::udf_registry::register_all`)
- [ ] Extend `ReadDeltaCdfTableFunction::call()` to parse optional version/timestamp arguments and pass them to `delta_cdf_provider(...)`
- [ ] Extend `compile_view()` dispatch to route new ViewTransform variants
- [ ] Extend `extract_sources()` in `graph_validator.rs` to handle new variants
- [ ] Update Python `spec_builder.py` mirror structs for new ViewTransform variants
- [ ] Test: IncrementalCdf produces valid DataFrame with change_type column
- [ ] Test: CDF result composes with downstream join in single plan DAG
- [ ] Test: `cdf_is_upsert` UDF filter pushes into CDF view correctly
- [ ] Test: Metadata view returns correct schema/version for Delta table
- [ ] Test: FileManifest view returns correct file count and size totals
- [ ] Test: EXPLAIN shows CDF scan as optimizer-visible node (not side-channel)

---

## 6) WS-P6: Advanced UDF Hooks

### 6.1 Purpose

Shift from "add hooks everywhere" to a governance-first model:
1. inventory current hook coverage across scalar/aggregate/window functions
2. close only high-ROI missing hooks
3. prevent duplicate or stale recommendations when hooks already exist

### 6.2 Hook Inventory and Applicability

Based on current `datafusion_ext` sources, many hooks are already implemented. The gap is consistency and activation, not greenfield feature absence.

| Hook family | Current status | Remaining action |
|-------------|----------------|------------------|
| `simplify()` | Widely implemented in hash/span/string/collection UDFs | Add golden tests proving optimizer-visible simplification for target SQL patterns |
| `coerce_types()` | Widely implemented across scalar and aggregate functions | Normalize coercion behavior and error payloads for edge types |
| `short_circuits()` | Implemented for key collection functions | Audit null-path behavior for all nullable-heavy UDFs |
| `propagate_constraints()` | Present in metadata/function-factory paths | Expand only where interval propagation is demonstrably useful |
| UDAF groups/retract support | Already present for selected aggregates | Extend selectively where profile benchmarks show material win |

### 6.3 Governance Pattern: Capability Registry + Activation Policy

```rust
// rust/datafusion_ext/src/registry_snapshot.rs (new helper)

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionHookCapabilities {
    pub name: String,
    pub kind: FunctionKind, // Scalar | Aggregate | Window
    pub has_simplify: bool,
    pub has_coerce_types: bool,
    pub has_short_circuits: bool,
    pub has_propagate_constraints: bool,
    pub has_groups_accumulator: bool,
    pub has_retract_batch: bool,
}

pub fn snapshot_hook_capabilities() -> Vec<FunctionHookCapabilities> {
    // Build from currently registered functions; used by CI assertions and docs
    // generation so recommendations stay synchronized with real implementations.
    collect_capabilities_from_registry()
}
```

### 6.4 Implementation Checklist

- [ ] Add a generated hook-capability snapshot (scalar/aggregate/window) and check it in CI
- [ ] Remove stale tasks for hooks that are already implemented
- [ ] Define ROI criteria for new hook work (plan diff impact, elapsed time impact, memory/spill impact)
- [ ] Prioritize missing hooks only where benchmarks justify complexity
- [ ] Add EXPLAIN-based tests demonstrating simplification/constraint impact for selected functions
- [ ] Add regression tests ensuring hook behavior parity against non-optimized execution

---

## 7) WS-P7: Real Physical Metrics from Executed Plans

### 7.1 Purpose

Replace the synthetic placeholder metrics (`spill_count: 0`, `scan_selectivity: 1.0`) in the tuner with real metrics extracted from the executed `ExecutionPlan` tree.

### 7.2 Metrics Collector

```rust
// rust/codeanatomy_engine/src/executor/metrics_collector.rs

use datafusion::physical_plan::{ExecutionPlan, metrics::MetricsSet};
use std::sync::Arc;

/// Real execution metrics collected from the executed plan tree.
///
/// Replaces the synthetic metrics at materializer.rs:208-214.
#[derive(Debug, Clone, Default)]
pub struct CollectedMetrics {
    /// Total rows produced across all output partitions.
    pub output_rows: u64,
    /// Total spill events across all operators.
    pub spill_count: u64,
    /// Total bytes spilled to disk.
    pub spilled_bytes: u64,
    /// Total CPU time across all operators (nanoseconds).
    pub elapsed_compute_nanos: u64,
    /// Peak memory usage estimate (bytes).
    pub peak_memory_bytes: u64,
    /// Scan selectivity: output_rows / input_rows for leaf scans.
    /// 0.0..1.0, where 1.0 means all rows passed filters.
    pub scan_selectivity: f64,
    /// Number of partitions in the physical plan.
    pub partition_count: usize,
    /// Per-operator metric summaries.
    pub operator_metrics: Vec<OperatorMetricSummary>,
}

#[derive(Debug, Clone)]
pub struct OperatorMetricSummary {
    pub operator_name: String,
    pub output_rows: u64,
    pub elapsed_compute_nanos: u64,
    pub spill_count: u64,
    pub spilled_bytes: u64,
    pub memory_usage: u64,
}

/// Walk the executed plan tree and collect real metrics.
///
/// DataFusion populates MetricsSet on each ExecutionPlan node during
/// execution. After execution completes, we walk the tree and aggregate.
///
/// Key MetricValue variants used:
/// - OutputRows(Count) — rows produced
/// - ElapsedCompute(Time) — CPU time
/// - SpillCount(Count) — spill events
/// - SpilledBytes(Count) — bytes spilled
/// - CurrentMemoryUsage(Gauge) — memory usage
pub fn collect_plan_metrics(plan: &dyn ExecutionPlan) -> CollectedMetrics {
    let mut collected = CollectedMetrics::default();
    let mut scan_input_rows: u64 = 0;
    let mut scan_output_rows: u64 = 0;

    collect_recursive(plan, &mut collected, &mut scan_input_rows, &mut scan_output_rows);

    // Compute scan selectivity from leaf scan nodes
    collected.scan_selectivity = if scan_input_rows > 0 {
        scan_output_rows as f64 / scan_input_rows as f64
    } else {
        1.0
    };

    collected.partition_count = plan.output_partitioning().partition_count();

    collected
}

fn collect_recursive(
    plan: &dyn ExecutionPlan,
    collected: &mut CollectedMetrics,
    scan_input_rows: &mut u64,
    scan_output_rows: &mut u64,
) {
    // Collect metrics from this node
    if let Some(metrics) = plan.metrics() {
        let aggregated = metrics.aggregate_by_name();

        let output_rows = aggregated.output_rows().unwrap_or(0);
        let elapsed_nanos = aggregated.elapsed_compute().unwrap_or(0);
        let spills = aggregated.spill_count().unwrap_or(0);
        let spill_bytes = aggregated.spilled_bytes().unwrap_or(0);

        collected.output_rows += output_rows as u64;
        collected.elapsed_compute_nanos += elapsed_nanos as u64;
        collected.spill_count += spills as u64;
        collected.spilled_bytes += spill_bytes as u64;

        // Track scan selectivity from leaf nodes
        let name = plan.name();
        if name.contains("Scan") || name.contains("Parquet") || name.contains("Delta") {
            *scan_output_rows += output_rows as u64;
            // For scans, input rows ≈ total rows before pushdown
            // This is approximated from statistics when available
            if let Ok(stats) = plan.statistics() {
                if let Some(num_rows) = stats.num_rows.get_value() {
                    *scan_input_rows += *num_rows as u64;
                } else {
                    *scan_input_rows += output_rows as u64;
                }
            } else {
                *scan_input_rows += output_rows as u64;
            }
        }

        collected.operator_metrics.push(OperatorMetricSummary {
            operator_name: name.to_string(),
            output_rows: output_rows as u64,
            elapsed_compute_nanos: elapsed_nanos as u64,
            spill_count: spills as u64,
            spilled_bytes: spill_bytes as u64,
            memory_usage: 0, // populated from gauge if available
        });
    }

    // Recurse into children
    for child in plan.children() {
        collect_recursive(child.as_ref(), collected, scan_input_rows, scan_output_rows);
    }
}
```

### 7.3 Integration with Tuner

```rust
// Modification to python/materializer.rs execute() method
// Replace lines 208-214 with:

let collected = collect_plan_metrics(physical_plan.as_ref());

let metrics = ExecutionMetrics {
    elapsed_ms: (Utc::now() - start_time).num_milliseconds() as u64,
    spill_count: collected.spill_count as u32,
    scan_selectivity: collected.scan_selectivity,
    peak_memory_bytes: collected.peak_memory_bytes,
    rows_processed: collected.output_rows,
};
```

### 7.4 Metrics Persistence for Cross-Run Learning

```rust
// rust/codeanatomy_engine/src/tuner/metrics_store.rs

/// Bounded metrics history for cross-run tuner learning.
///
/// Stores up to `max_entries` historical ExecutionMetrics + TunerConfig pairs.
/// The tuner consults this history to detect trends and make better decisions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsStore {
    pub entries: Vec<MetricsEntry>,
    pub max_entries: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsEntry {
    pub spec_hash: [u8; 32],
    pub metrics: ExecutionMetrics,
    pub config_used: TunerConfig,
    pub config_recommended: Option<TunerConfig>,
    pub timestamp: DateTime<Utc>,
}

impl MetricsStore {
    pub fn new(max_entries: usize) -> Self {
        Self { entries: Vec::new(), max_entries }
    }

    pub fn record(&mut self, entry: MetricsEntry) {
        self.entries.push(entry);
        if self.entries.len() > self.max_entries {
            self.entries.remove(0); // FIFO eviction
        }
    }

    /// Get historical metrics for a specific spec hash.
    pub fn history_for_spec(&self, spec_hash: &[u8; 32]) -> Vec<&MetricsEntry> {
        self.entries.iter()
            .filter(|e| &e.spec_hash == spec_hash)
            .collect()
    }
}
```

### 7.5 Implementation Checklist

- [ ] Create `executor/metrics_collector.rs` with `CollectedMetrics`, `OperatorMetricSummary`
- [ ] Implement `collect_plan_metrics()` recursive tree walk over `ExecutionPlan`
- [ ] Extract real metrics: `output_rows()`, `elapsed_compute()`, `spill_count()`, `spilled_bytes()`
- [ ] Compute `scan_selectivity` from leaf scan nodes (output_rows / input_rows estimate)
- [ ] Replace synthetic metrics in `materializer.rs:208-214` with `collect_plan_metrics()` call
- [ ] Create `tuner/metrics_store.rs` with bounded `MetricsStore` for cross-run history
- [ ] Wire metrics persistence to `RunResult` output (optional, in tuner_hints extension)
- [ ] Test: spill_count reflects actual spills during hash join with small memory pool
- [ ] Test: scan_selectivity < 1.0 when predicate pushdown filters rows
- [ ] Test: elapsed_compute_nanos > 0 for non-trivial plans
- [ ] Test: operator_metrics contains one entry per physical operator
- [ ] Test: metrics store FIFO eviction works at capacity

---

## 8) WS-P8: Cross-View Compilation Optimization

### 8.1 Purpose

Reduce intermediate view registration boundaries where possible, allowing the optimizer to see more of the plan DAG as a single unit. The current approach registers every compiled view as a named view, which creates optimization barriers.

### 8.2 Strategy: Selective Inlining

Not all views benefit from registration boundaries. Simple transforms (filter, project) on single sources can be inlined into the consuming view's plan, giving the optimizer more rewrite opportunities.

```rust
// rust/codeanatomy_engine/src/compiler/inline_policy.rs

use crate::spec::relations::{ViewDefinition, ViewTransform};
use std::collections::HashMap;

/// Determine which views should be inlined vs registered as named views.
///
/// Inlining eliminates the optimization barrier of named view references,
/// allowing the optimizer to push down predicates and reorder joins across
/// view boundaries.
///
/// A view is eligible for inlining when ALL of:
/// 1. It has exactly ONE downstream consumer (fanout == 1)
/// 2. It uses a simple transform (Filter, Project)
/// 3. It does not appear in any output target's source_view
///
/// Views that are NOT inlined (always registered):
/// - Multi-consumer views (fanout > 1) — must be registered for sharing
/// - Complex transforms (Join, Union, Aggregate) — optimizer handles these better as named views
/// - Output views — must be registered for materialization
/// - Cache boundary views — must be registered for caching
pub fn compute_inline_policy(
    views: &[ViewDefinition],
    output_views: &[String],
    ref_counts: &HashMap<&str, usize>,
) -> HashMap<String, InlineDecision> {
    let output_set: HashSet<&str> = output_views.iter()
        .map(|s| s.as_str()).collect();

    views.iter().map(|v| {
        let fanout = ref_counts.get(v.name.as_str()).copied().unwrap_or(0);
        let is_output = output_set.contains(v.name.as_str());
        let is_simple = matches!(
            v.transform,
            ViewTransform::Filter { .. } | ViewTransform::Project { .. }
        );

        let decision = if is_output || fanout > 1 || !is_simple {
            InlineDecision::Register
        } else {
            InlineDecision::Inline
        };

        (v.name.clone(), decision)
    }).collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InlineDecision {
    /// Register as a named view (current behavior).
    Register,
    /// Inline into the consuming view's plan (no registration boundary).
    Inline,
}
```

### 8.3 Modified Compilation Loop

```rust
// Modification to plan_compiler.rs compile() view loop

for view_def in &ordered {
    let df = self.compile_view(view_def).await?;

    match inline_policy.get(&view_def.name) {
        Some(InlineDecision::Inline) => {
            // Store the DataFrame for later inlining into the consumer.
            // Do NOT register as a named view — the consumer will
            // reference this DataFrame directly.
            inline_cache.insert(view_def.name.clone(), df);
        }
        _ => {
            // Register as named view (current behavior)
            let view = df.into_view();
            self.ctx.register_table(&view_def.name, view)?;
        }
    }
}
```

When building a downstream view that references an inlined view, the compiler substitutes the DataFrame directly instead of calling `ctx.table()`:

```rust
// Modified source resolution in view builders
async fn resolve_source(
    &self,
    source_name: &str,
    inline_cache: &HashMap<String, DataFrame>,
) -> Result<DataFrame> {
    if let Some(df) = inline_cache.get(source_name) {
        Ok(df.clone())
    } else {
        self.ctx.table(source_name).await
    }
}
```

### 8.4 Implementation Checklist

- [ ] Create `compiler/inline_policy.rs` with `compute_inline_policy()`, `InlineDecision`
- [ ] Compute reference counts (fanout) for all views before compilation loop
- [ ] Build `inline_cache: HashMap<String, DataFrame>` for inlined views
- [ ] Modify compilation loop to check inline policy before registering
- [ ] Implement `resolve_source()` that checks inline cache before `ctx.table()`
- [ ] Update all view builders (normalize, relate, union, project, filter, aggregate) to use `resolve_source()`
- [ ] Preserve correctness: inlined views must still be topologically sorted correctly
- [ ] Test: filter→project chain inlines into consuming join (verify via EXPLAIN)
- [ ] Test: multi-consumer view is NOT inlined (still registered)
- [ ] Test: output view is NOT inlined (still registered)
- [ ] Test: inlined plans produce identical results to registered plans
- [ ] Test: EXPLAIN shows fewer scan nodes with inlining enabled

---

## 9) WS-P9: Deterministic Session Runtime Profiles

### 9.1 Purpose

Extend `SessionFactory` with explicit, deterministic runtime profiles that codify session configuration knobs (parallelism, memory, spill, repartition, cache, statistics) as versioned, fingerprinted profile objects.

Current `session/factory.rs` already sets several important defaults (`collect_statistics`, `pushdown_filters`, join repartition, dynamic filter pushdown, recursive CTEs). This workstream only targets remaining high-ROI deltas.

### 9.2 Gap Analysis (Actual Remaining Deltas)

| Knob | Current State | Best-in-Class | Impact |
|------|--------------|---------------|--------|
| `with_repartition_sorts` | Not set | `true` — enables sort-based repartitioning | Large sorted datasets shuffle more efficiently |
| `with_repartition_file_scans` | Not set | `true` — enables file-level scan parallelism | Multi-file Delta scans utilize all partitions |
| `with_repartition_file_min_size` | Not set | `64 * 1024 * 1024` (64 MB) | Prevents over-partitioning small files |
| `execution.parquet.enable_page_index` | Not set | `true` | Page-level pruning reduces decoded rows |
| `execution.parquet.metadata_size_hint` | Not set | Profile-specific | Improves metadata prefetch behavior |
| `execution.parquet.max_predicate_cache_size` | Not set | Profile-specific | Avoids repeated predicate evaluation work |
| `planning_concurrency` | Set to `target_partitions` only | Profile-tunable override | Better large-workload planning throughput control |
| `meta_fetch_concurrency` | Not set | `32` | Schema/statistics inference parallelism |
| `list_files_cache_limit` | Not set | `1M` default, tunable | Caches file listings across repeated scans |
| `metadata_cache_limit` | Not set | `50M` default, tunable | Caches Parquet file metadata |
| `max_temp_directory_size` | Not set | Profile-dependent | Bounds spill disk usage |
| Runtime profile artifact | Ad-hoc via `EnvironmentProfile` | Explicit `RuntimeProfileSpec` with fingerprint | Deterministic compatibility and auditability |

### 9.3 New Struct: RuntimeProfileSpec

```rust
// rust/codeanatomy_engine/src/session/runtime_profiles.rs

use serde::{Serialize, Deserialize};

/// Deterministic runtime profile that captures ALL session configuration
/// knobs as a versioned, fingerprinted object.
///
/// Extends the existing EnvironmentProfile with comprehensive session
/// config including repartition, cache, statistics, and optimizer knobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeProfileSpec {
    /// Profile name for logging/audit (e.g., "small", "medium", "large").
    pub profile_name: String,

    // --- Parallelism ---
    pub target_partitions: usize,
    pub batch_size: usize,
    pub planning_concurrency: usize,
    pub meta_fetch_concurrency: usize,

    // --- Memory & Spill ---
    pub memory_pool_bytes: usize,
    pub max_temp_directory_bytes: usize,

    // --- Repartition ---
    pub repartition_joins: bool,
    pub repartition_aggregations: bool,
    pub repartition_windows: bool,
    pub repartition_sorts: bool,
    pub repartition_file_scans: bool,
    pub repartition_file_min_size: usize,

    // --- Parquet Scan ---
    pub parquet_pruning: bool,
    pub pushdown_filters: bool,
    pub enable_page_index: bool,
    pub metadata_size_hint: usize,
    pub max_predicate_cache_size: Option<usize>,

    // --- Cache ---
    pub list_files_cache_limit: usize,
    pub list_files_cache_ttl: Option<String>,
    pub metadata_cache_limit: usize,

    // --- Statistics ---
    pub collect_statistics: bool,

    // --- Optimizer ---
    pub optimizer_max_passes: usize,
    pub skip_failed_rules: bool,
    pub filter_null_join_keys: bool,
    pub enable_dynamic_filter_pushdown: bool,
    pub enable_topk_dynamic_filter_pushdown: bool,
    pub enable_recursive_ctes: bool,

    // --- SQL Parser ---
    pub enable_ident_normalization: bool,

    // --- Explain ---
    pub show_statistics: bool,
    pub show_schema: bool,
}

impl RuntimeProfileSpec {
    /// Compute a deterministic fingerprint of the full profile.
    ///
    /// Two identical profiles produce identical fingerprints.
    /// Profile fingerprint is part of the envelope_hash determinism contract.
    pub fn fingerprint(&self) -> [u8; 32] {
        let serialized = serde_json::to_vec(self).expect("profile serializable");
        *blake3::hash(&serialized).as_bytes()
    }
}
```

### 9.4 Profile Presets

```rust
// rust/codeanatomy_engine/src/session/runtime_profiles.rs (continued)

impl RuntimeProfileSpec {
    /// Small profile: development, CI, small repos (< 100 files).
    pub fn small() -> Self {
        Self {
            profile_name: "small".into(),
            target_partitions: 4,
            batch_size: 4096,
            planning_concurrency: 4,
            meta_fetch_concurrency: 8,
            memory_pool_bytes: 256 * 1024 * 1024,         // 256 MB
            max_temp_directory_bytes: 1024 * 1024 * 1024,  // 1 GB
            repartition_joins: true,
            repartition_aggregations: true,
            repartition_windows: true,
            repartition_sorts: true,
            repartition_file_scans: true,
            repartition_file_min_size: 32 * 1024 * 1024,  // 32 MB
            parquet_pruning: true,
            pushdown_filters: true,
            enable_page_index: true,
            metadata_size_hint: 524288,
            max_predicate_cache_size: None,
            list_files_cache_limit: 1024 * 1024,           // 1 MB
            list_files_cache_ttl: None,
            metadata_cache_limit: 50 * 1024 * 1024,        // 50 MB
            collect_statistics: true,
            optimizer_max_passes: 3,
            skip_failed_rules: false,
            filter_null_join_keys: true,
            enable_dynamic_filter_pushdown: true,
            enable_topk_dynamic_filter_pushdown: true,
            enable_recursive_ctes: true,
            enable_ident_normalization: false,
            show_statistics: true,
            show_schema: true,
        }
    }

    /// Medium profile: standard workloads (100-10K files).
    pub fn medium() -> Self {
        let mut p = Self::small();
        p.profile_name = "medium".into();
        p.target_partitions = 8;
        p.batch_size = 8192;
        p.planning_concurrency = 8;
        p.meta_fetch_concurrency = 32;
        p.memory_pool_bytes = 1024 * 1024 * 1024;         // 1 GB
        p.max_temp_directory_bytes = 10 * 1024 * 1024 * 1024; // 10 GB
        p.repartition_file_min_size = 64 * 1024 * 1024;
        p
    }

    /// Large profile: large repos (10K+ files), production.
    pub fn large() -> Self {
        let mut p = Self::medium();
        p.profile_name = "large".into();
        p.target_partitions = 16;
        p.batch_size = 16384;
        p.planning_concurrency = 16;
        p.memory_pool_bytes = 4 * 1024 * 1024 * 1024;     // 4 GB
        p.max_temp_directory_bytes = 100 * 1024 * 1024 * 1024; // 100 GB
        p.repartition_file_min_size = 128 * 1024 * 1024;
        p.metadata_cache_limit = 200 * 1024 * 1024;         // 200 MB
        p
    }
}
```

### 9.5 Integration with SessionFactory

```rust
// Modification to session/factory.rs build_session()

use crate::session::runtime_profiles::RuntimeProfileSpec;

impl SessionFactory {
    /// Build a SessionContext from a RuntimeProfileSpec.
    ///
    /// All config knobs are applied from the profile. The resulting
    /// session is deterministic: same profile → same session config.
    pub async fn build_session_from_profile(
        &self,
        profile: &RuntimeProfileSpec,
    ) -> Result<SessionContext> {
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_pool(Arc::new(FairSpillPool::new(profile.memory_pool_bytes)))
            .with_disk_manager_builder(
                DiskManagerBuilder::default()
                    .with_mode(DiskManagerMode::OsTmpDirectory),
            )
            .with_max_temp_directory_size(profile.max_temp_directory_bytes)
            .build_arc()?;

        let mut config = SessionConfig::new()
            .with_default_catalog_and_schema("codeanatomy", "public")
            .with_information_schema(true)
            .with_target_partitions(profile.target_partitions)
            .with_batch_size(profile.batch_size)
            .with_repartition_joins(profile.repartition_joins)
            .with_repartition_aggregations(profile.repartition_aggregations)
            .with_repartition_windows(profile.repartition_windows)
            .with_repartition_sorts(profile.repartition_sorts)
            .with_repartition_file_scans(profile.repartition_file_scans)
            .with_repartition_file_min_size(profile.repartition_file_min_size)
            .with_parquet_pruning(profile.parquet_pruning);

        let opts = config.options_mut();
        opts.execution.coalesce_batches = true;
        opts.execution.collect_statistics = profile.collect_statistics;
        opts.execution.parquet.pushdown_filters = profile.pushdown_filters;
        opts.execution.parquet.enable_page_index = profile.enable_page_index;
        opts.execution.parquet.metadata_size_hint =
            Some(profile.metadata_size_hint);
        if let Some(pred_cache) = profile.max_predicate_cache_size {
            opts.execution.parquet.max_predicate_cache_size = Some(pred_cache);
        }
        opts.execution.enable_recursive_ctes = profile.enable_recursive_ctes;
        opts.execution.planning_concurrency = profile.planning_concurrency;
        opts.optimizer.max_passes = profile.optimizer_max_passes;
        opts.optimizer.skip_failed_rules = profile.skip_failed_rules;
        opts.optimizer.filter_null_join_keys = profile.filter_null_join_keys;
        opts.optimizer.enable_dynamic_filter_pushdown =
            profile.enable_dynamic_filter_pushdown;
        opts.optimizer.enable_topk_dynamic_filter_pushdown =
            profile.enable_topk_dynamic_filter_pushdown;
        opts.sql_parser.enable_ident_normalization =
            profile.enable_ident_normalization;
        opts.explain.show_statistics = profile.show_statistics;
        opts.explain.show_schema = profile.show_schema;

        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(runtime)
            .with_analyzer_rules(self.ruleset.analyzer_rules.clone())
            .with_optimizer_rules(self.ruleset.optimizer_rules.clone())
            .with_physical_optimizer_rules(self.ruleset.physical_rules.clone())
            .build();

        Ok(SessionContext::new_with_state(state))
    }
}
```

### 9.6 Spec Extension

```rust
// Addition to SemanticExecutionSpec
pub struct SemanticExecutionSpec {
    // ... existing fields ...

    /// Runtime profile (replaces ad-hoc per-field session config).
    /// When set, this profile is used to build the session;
    /// individual fields (target_partitions, batch_size) are ignored.
    pub runtime_profile: Option<RuntimeProfileSpec>,
}
```

### 9.7 Implementation Checklist

- [ ] Create `session/runtime_profiles.rs` with `RuntimeProfileSpec` struct
- [ ] Implement all three profile presets (small, medium, large)
- [ ] Implement `fingerprint()` for deterministic profile identity
- [ ] Add `build_session_from_profile()` to `SessionFactory`
- [ ] Apply all remaining knobs from the WS-P9 gap table in `build_session_from_profile()`
- [ ] Include profile fingerprint in `envelope_hash` determinism contract
- [ ] Add `runtime_profile: Option<RuntimeProfileSpec>` to `SemanticExecutionSpec`
- [ ] Update `run_full_pipeline()` to use profile-based session when spec has runtime_profile
- [ ] Capture runtime config snapshot (`information_schema.df_settings`) in `PlanBundle`
- [ ] Test: small/medium/large profiles produce different session configs
- [ ] Test: same profile → same fingerprint across runs
- [ ] Test: profile fingerprint changes when any knob changes
- [ ] Test: pushdown_filters + parquet_pruning produce measurable scan reduction
- [ ] Test: collect_statistics enables cost-based join reordering (verify via EXPLAIN)

---

## 10) WS-P10: Plan-Aware Cache Boundaries

### 10.1 Purpose

Upgrade the existing cache boundary system (at `compiler/cache_boundaries.rs`) from static heuristic decisions to plan-aware, metrics-informed cache placement that considers fanout, estimated cost, and historical execution data.

### 10.2 Current State

The current `cache_boundaries.rs` identifies high-fanout views for `DataFrame.cache()` materialization. This is correct but limited:
- No cost estimation (all fanout-1 views treated equally)
- No historical data (no learning from previous runs)
- No interaction with the RuntimeProfileSpec memory budget

### 10.3 New Struct: CachePlacementPolicy

```rust
// rust/codeanatomy_engine/src/compiler/cache_policy.rs

use serde::{Serialize, Deserialize};

/// Policy controlling DataFrame.cache() placement in the compiled plan DAG.
///
/// Combines static analysis (fanout, transform complexity) with optional
/// historical metrics to make cost-informed caching decisions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachePlacementPolicy {
    /// Minimum fanout to consider caching (default: 2).
    /// Views consumed by fewer downstream views are never cached.
    pub min_fanout: usize,

    /// Estimated row threshold: only cache if upstream estimates exceed this.
    /// Uses DataFusion statistics when collect_statistics is enabled.
    pub min_estimated_rows: Option<u64>,

    /// Memory budget for cached DataFrames (bytes).
    /// Sum of all cache() materializations must stay under this limit.
    /// Derived from RuntimeProfileSpec.memory_pool_bytes when not explicit.
    pub cache_memory_budget: Option<usize>,

    /// Enable historical cost learning from MetricsStore.
    /// When true, views that historically produce large intermediate results
    /// are prioritized for caching.
    pub use_historical_metrics: bool,

    /// Explicit cache boundary overrides (view name → force cache/no cache).
    pub overrides: Vec<CacheOverride>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheOverride {
    pub view_name: String,
    pub action: CacheAction,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CacheAction {
    ForceCache,
    NeverCache,
}

impl Default for CachePlacementPolicy {
    fn default() -> Self {
        Self {
            min_fanout: 2,
            min_estimated_rows: None,
            cache_memory_budget: None,
            use_historical_metrics: false,
            overrides: Vec::new(),
        }
    }
}
```

### 10.4 Statistics-Informed Cache Decision

```rust
// rust/codeanatomy_engine/src/compiler/cache_policy.rs (continued)

use datafusion::prelude::*;

/// Determine which views should be cached based on policy + statistics.
///
/// Uses DataFusion's plan statistics (when collect_statistics is enabled)
/// to estimate intermediate result sizes and prioritize cache placement
/// within the memory budget.
pub async fn compute_cache_boundaries(
    ctx: &SessionContext,
    views: &[ViewDefinition],
    ref_counts: &HashMap<&str, usize>,
    policy: &CachePlacementPolicy,
    metrics_store: Option<&MetricsStore>,
) -> Vec<String> {
    let mut candidates: Vec<CacheCandidate> = Vec::new();

    for view in views {
        let fanout = ref_counts.get(view.name.as_str()).copied().unwrap_or(0);

        // Check override first
        if let Some(ovr) = policy.overrides.iter().find(|o| o.view_name == view.name) {
            match ovr.action {
                CacheAction::ForceCache => {
                    candidates.push(CacheCandidate {
                        view_name: view.name.clone(),
                        priority: u64::MAX,
                    });
                    continue;
                }
                CacheAction::NeverCache => continue,
            }
        }

        // Apply minimum fanout filter
        if fanout < policy.min_fanout {
            continue;
        }

        // Estimate cost using statistics when available
        let estimated_rows = estimate_view_rows(ctx, &view.name).await;
        if let Some(min_rows) = policy.min_estimated_rows {
            if estimated_rows < min_rows {
                continue;
            }
        }

        // Priority = fanout * estimated_rows (higher = more valuable to cache)
        let priority = (fanout as u64) * estimated_rows;
        candidates.push(CacheCandidate {
            view_name: view.name.clone(),
            priority,
        });
    }

    // Sort by priority descending, apply memory budget
    candidates.sort_by(|a, b| b.priority.cmp(&a.priority));

    candidates.iter().map(|c| c.view_name.clone()).collect()
}

struct CacheCandidate {
    view_name: String,
    priority: u64,
}

async fn estimate_view_rows(ctx: &SessionContext, view_name: &str) -> u64 {
    if let Ok(df) = ctx.table(view_name).await {
        if let Ok(plan) = df.create_physical_plan().await {
            if let Ok(stats) = plan.statistics() {
                if let Some(rows) = stats.num_rows.get_value() {
                    return *rows as u64;
                }
            }
        }
    }
    1000 // fallback estimate
}
```

### 10.5 Implementation Checklist

- [ ] Create `compiler/cache_policy.rs` with `CachePlacementPolicy`, `CacheOverride`, `CacheAction`
- [ ] Implement `compute_cache_boundaries()` with statistics-informed prioritization
- [ ] Wire `CachePlacementPolicy` into `SemanticExecutionSpec` as optional field
- [ ] Integrate with `RuntimeProfileSpec.memory_pool_bytes` for budget derivation
- [ ] Support explicit per-view overrides (ForceCache / NeverCache)
- [ ] Integrate with `MetricsStore` for historical cost learning (when `use_historical_metrics` is true)
- [ ] Update existing `cache_boundaries.rs` to delegate to new policy system
- [ ] Test: high-fanout views are cached, low-fanout views are not
- [ ] Test: statistics-aware caching skips views with small estimated row counts
- [ ] Test: ForceCache override bypasses fanout check
- [ ] Test: NeverCache override prevents caching regardless of fanout
- [ ] Test: cache placement respects memory budget (total cached bytes bounded)

---

## 11) WS-P11: Delta Maintenance Integration in Engine Pipeline

### 11.1 Purpose

Wire the existing Delta maintenance operations (at `datafusion_ext/src/delta_maintenance.rs`: compact, vacuum, checkpoint, cleanup, constraints) into the engine pipeline as spec-driven post-execution maintenance steps with explicit retention policies and safety guardrails.

Execution policy for this plan:
1. if `spec.maintenance` is present, maintenance runs inline in the same execution flow
2. no separate "maintenance-off" override once schedule is provided
3. safety controls are expressed through policy fields, not hidden runtime toggles

### 11.2 Existing API Surface

The `delta_maintenance` module provides 8 async functions:
- `delta_optimize_compact()` — Compact small files into target size
- `delta_vacuum()` — Remove unreferenced files with retention policy
- `delta_create_checkpoint()` — Create transaction log checkpoint
- `delta_cleanup_metadata()` — Clean up old metadata entries
- `delta_restore()` — Restore table to a previous version
- `delta_set_properties()` — Set Delta table properties
- `delta_add_features()` — Enable protocol features
- `delta_add_constraints()` / `delta_drop_constraints()` — Manage check constraints

All return `DeltaMaintenanceReport` with `operation`, `version`, `snapshot`, `metrics`.

### 11.3 New Struct: MaintenanceSchedule

```rust
// rust/codeanatomy_engine/src/executor/maintenance.rs

use serde::{Serialize, Deserialize};

/// Spec-driven post-execution Delta maintenance schedule.
///
/// Runs maintenance operations on output tables AFTER successful
/// materialization. Operations execute in dependency order:
/// compact → checkpoint → vacuum → cleanup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaintenanceSchedule {
    /// Which output tables to maintain (empty = all output tables).
    pub target_tables: Vec<String>,

    /// Compact small files into larger ones.
    pub compact: Option<CompactPolicy>,

    /// Vacuum unreferenced files.
    pub vacuum: Option<VacuumPolicy>,

    /// Create checkpoints for faster log replay.
    pub checkpoint: bool,

    /// Clean up old metadata.
    pub metadata_cleanup: bool,

    /// Add/validate check constraints on output tables.
    pub constraints: Vec<ConstraintSpec>,

    /// Maximum tables to maintain concurrently.
    /// Default is 1 (serialized) for safety.
    pub max_parallel_tables: usize,

    /// Require table quiescence / exclusive write window for destructive operations.
    pub require_table_quiescence: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactPolicy {
    /// Target file size in bytes (default: 512 MB).
    pub target_file_size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VacuumPolicy {
    /// Minimum retention in hours (MUST be >= 168 = 7 days).
    /// The engine enforces this minimum regardless of spec value.
    pub retention_hours: u64,

    /// Require vacuum protocol check feature.
    pub require_protocol_check: bool,

    /// Dry run first to see what would be deleted.
    pub dry_run_first: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConstraintSpec {
    pub name: String,
    pub expression: String,
}

/// Non-negotiable safety minimum: 7 days retention.
const MIN_VACUUM_RETENTION_HOURS: u64 = 168;
```

### 11.4 Maintenance Executor

```rust
// rust/codeanatomy_engine/src/executor/maintenance.rs (continued)

use datafusion_ext::delta_maintenance::*;

/// Execute post-materialization maintenance on output tables.
///
/// Operations run in strict order: compact → checkpoint → vacuum → cleanup.
/// This order ensures:
/// 1. Compact creates larger files before vacuum removes old ones
/// 2. Checkpoint creates a recovery point before vacuum
/// 3. Vacuum only runs after checkpoint ensures recoverability
pub async fn execute_maintenance(
    ctx: &SessionContext,
    schedule: &MaintenanceSchedule,
    output_locations: &[(String, String)], // (table_name, delta_uri)
    gate: Option<DeltaFeatureGate>,
    commit_options: Option<DeltaCommitOptions>,
) -> Result<Vec<DeltaMaintenanceReport>> {
    let mut reports = Vec::new();
    let parallelism = schedule.max_parallel_tables.max(1);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(parallelism));

    let targets: Vec<&(String, String)> = if schedule.target_tables.is_empty() {
        output_locations.iter().collect()
    } else {
        output_locations.iter()
            .filter(|(name, _)| schedule.target_tables.contains(name))
            .collect()
    };

    for (table_name, uri) in targets {
        let _permit = semaphore.clone().acquire_owned().await?;
        // 1) Compact
        if let Some(compact) = &schedule.compact {
            let report = delta_optimize_compact(
                ctx, uri, None, None, None,
                Some(compact.target_file_size),
                gate.clone(), commit_options.clone(),
            ).await?;
            reports.push(report);
        }

        // 2) Checkpoint
        if schedule.checkpoint {
            let report = delta_create_checkpoint(
                ctx, uri, None, None, None, gate.clone(),
            ).await?;
            reports.push(report);
        }

        // 3) Vacuum (with enforced minimum retention)
        // If quiescence is required, verify policy guard before vacuum.
        if let Some(vacuum) = &schedule.vacuum {
            if schedule.require_table_quiescence {
                ensure_quiescent_window(table_name, uri).await?;
            }
            let safe_retention = vacuum.retention_hours
                .max(MIN_VACUUM_RETENTION_HOURS);

            if vacuum.dry_run_first {
                let _dry = delta_vacuum(
                    ctx, uri, None, None, None,
                    Some(safe_retention as i64),
                    true,  // dry_run
                    true,  // enforce retention
                    vacuum.require_protocol_check,
                    gate.clone(), commit_options.clone(),
                ).await?;
            }

            let report = delta_vacuum(
                ctx, uri, None, None, None,
                Some(safe_retention as i64),
                false, // not dry_run
                true,  // enforce retention
                vacuum.require_protocol_check,
                gate.clone(), commit_options.clone(),
            ).await?;
            reports.push(report);
        }

        // 4) Metadata cleanup
        if schedule.metadata_cleanup {
            let report = delta_cleanup_metadata(
                ctx, uri, None, None, None, gate.clone(),
            ).await?;
            reports.push(report);
        }

        // 5) Constraints
        for constraint in &schedule.constraints {
            let report = delta_add_constraints(
                ctx, uri, None, None, None,
                vec![(constraint.name.clone(), constraint.expression.clone())],
                gate.clone(), commit_options.clone(),
            ).await?;
            reports.push(report);
        }
    }

    Ok(reports)
}
```

### 11.5 Spec Extension

```rust
// Addition to SemanticExecutionSpec
pub struct SemanticExecutionSpec {
    // ... existing fields ...

    /// Post-execution Delta maintenance schedule.
    /// When present, maintenance runs after successful materialization.
    #[serde(default)]
    pub maintenance: Option<MaintenanceSchedule>,
}
```

### 11.6 Implementation Checklist

- [ ] Create `executor/maintenance.rs` with `MaintenanceSchedule`, `CompactPolicy`, `VacuumPolicy`, `ConstraintSpec`
- [ ] Implement `execute_maintenance()` with strict operation ordering
- [ ] Enforce `MIN_VACUUM_RETENTION_HOURS` (7 days) regardless of spec value
- [ ] Wire dry_run_first for vacuum to preview deletions before executing
- [ ] Run maintenance inline whenever `spec.maintenance` is present
- [ ] Add `max_parallel_tables` and `require_table_quiescence` safety controls
- [ ] Add `maintenance: Option<MaintenanceSchedule>` to `SemanticExecutionSpec`
- [ ] Wire maintenance into `run_full_pipeline()` post-materialization phase
- [ ] Include maintenance reports in `RunResult` output
- [ ] Update Python `spec_builder.py` mirror with maintenance structs
- [ ] Test: compact reduces file count for output table with many small files
- [ ] Test: vacuum respects minimum retention (< 168 hours → forced to 168)
- [ ] Test: checkpoint creates valid log checkpoint file
- [ ] Test: constraint validation rejects invalid SQL expressions at compile time
- [ ] Test: maintenance only runs on target_tables when specified
- [ ] Test: quiescence policy blocks vacuum when concurrent writer guard fails
- [ ] Test: `max_parallel_tables=1` enforces serialized maintenance execution

---

## 12) WS-P12: UDAF/UDWF Optimizer Integration

### 12.1 Purpose

Upgrade the existing built-in UDAFs (at `datafusion_ext/src/udaf_builtin.rs`: `list_unique`, `collect_set`, `count_distinct_agg`, etc.) and UDWFs (at `udwf_builtin.rs`) with advanced `AggregateUDFImpl` / `WindowUDFImpl` hooks that enable optimizer integration, retract support, and merge correctness.

### 12.2 Hook Inventory and Applicability

| Hook | Description | Applicable Functions |
|------|-------------|---------------------|
| `groups_accumulator_supported()` | Enable vectorized aggregation path | All UDAFs (already partially implemented for some) |
| `create_groups_accumulator()` | GroupsAccumulator for vectorized grouping | `count_distinct_agg`, `list_unique`, `collect_set` |
| `create_sliding_accumulator()` | Sliding window aggregation | `count_if`, `string_agg` |
| `retract_batch()` | Support bounded window frame retraction | `count_if`, `string_agg`, `any_value_det` |
| `state_fields()` | Explicit state layout for merge correctness | All UDAFs |
| `sort_options()` | Ordering requirement for window function | UDWFs: `lag`, `lead` |
| `reverse_expr()` | Reversible window expression | UDWFs: `lag` ↔ `lead` |
| `coerce_types()` | User-defined type coercion | UDAFs with variadic inputs |

### 12.3 Representative: GroupsAccumulator for count_distinct

```rust
// Enhancement to udaf_builtin.rs CountDistinctUdaf

impl AggregateUDFImpl for CountDistinctUdaf {
    // ... existing methods ...

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        // Enable for primitive types where hashing is efficient
        matches!(
            args.exprs[0].data_type(args.schema).unwrap(),
            DataType::Int64 | DataType::Utf8 | DataType::Int32
        )
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        let dt = args.exprs[0].data_type(args.schema)?;
        match dt {
            DataType::Int64 => Ok(Box::new(
                Int64DistinctGroupsAccumulator::new()
            )),
            DataType::Utf8 => Ok(Box::new(
                Utf8DistinctGroupsAccumulator::new()
            )),
            _ => internal_err!("unsupported type for groups accumulator: {dt}"),
        }
    }
}
```

### 12.4 Representative: Retract for count_if

```rust
// Enhancement to udaf_builtin.rs CountIfAccumulator

impl Accumulator for CountIfAccumulator {
    // ... existing methods ...

    /// Support retraction for bounded window frames.
    ///
    /// When a row exits the window frame, retract_batch is called
    /// with the rows leaving the frame. For count_if, we decrement
    /// the count for rows that matched the predicate.
    fn retract_batch(
        &mut self,
        values: &[ArrayRef],
    ) -> Result<()> {
        let predicate = values[0].as_boolean();
        for i in 0..predicate.len() {
            if predicate.value(i) {
                self.count -= 1;
            }
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}
```

### 12.5 Implementation Checklist

- [ ] Add `groups_accumulator_supported()` + `create_groups_accumulator()` to `CountDistinctUdaf` for Int64/Utf8
- [ ] Add `groups_accumulator_supported()` + `create_groups_accumulator()` to `ListUniqueUdaf`
- [ ] Add `groups_accumulator_supported()` + `create_groups_accumulator()` to `CollectSetUdaf`
- [ ] Add `retract_batch()` + `supports_retract_batch()` to `CountIfAccumulator`
- [ ] Add `retract_batch()` to `StringAggAccumulator`
- [ ] Add `retract_batch()` to `AnyValueDetAccumulator`
- [ ] Verify `state_fields()` + `merge_batch()` associativity for all UDAFs
- [ ] Add `sort_options()` to `lag`/`lead` UDWFs
- [ ] Add `reverse_expr()` to `lag`/`lead` UDWFs (lag ↔ lead reversal)
- [ ] Test: GroupsAccumulator produces same results as row-level accumulator
- [ ] Test: retract_batch produces correct count when window frame shrinks
- [ ] Test: merge correctness across multiple partitions (associativity)
- [ ] Test: EXPLAIN shows GroupsAccumulator usage for supported types

---

## 13) WS-P13: OpenTelemetry-Native Execution Tracing

### 13.1 Purpose

Add structured OpenTelemetry tracing to the engine execution pipeline, enabling per-operator span trees, per-rule instrumentation, and integration with the project's existing observability layer (at `src/obs/`).

### 13.2 Design: datafusion-tracing Integration

Use `datafusion-tracing` for plan-level instrumentation when available, but treat it as an optional contrib dependency (not ASF core). Provide a first-party fallback path using plain `tracing` spans when the feature is unavailable or version-mismatched.

```rust
// rust/codeanatomy_engine/src/executor/tracing.rs

#[cfg(feature = "tracing")]
pub fn build_tracing_state(...) -> SessionState {
    // datafusion-tracing integration path
}

#[cfg(not(feature = "tracing"))]
pub fn build_tracing_state(base_state: SessionState, ...) -> SessionState {
    // fallback: no DataFusion instrumentation macros, keep baseline behavior
    base_state
}
```

Primary integration (feature-enabled path):

```rust
// rust/codeanatomy_engine/src/executor/tracing.rs

use datafusion_tracing::{
    InstrumentationOptions,
    RuleInstrumentationOptions,
    instrument_with_info_spans,
    instrument_rules_with_info_spans,
};
use tracing::info_span;

/// Build instrumented SessionState with OpenTelemetry spans.
///
/// Adds per-operator and per-rule tracing when enabled.
/// Controlled by runtime flag — zero overhead when disabled.
pub fn build_tracing_state(
    base_state: SessionState,
    enable_rule_tracing: bool,
    enable_plan_preview: bool,
) -> SessionState {
    let mut opts = InstrumentationOptions::builder()
        .record_metrics(true);

    if enable_plan_preview {
        opts = opts.preview_limit(5);
    }

    let instrumentation = opts.build();

    let state = if enable_rule_tracing {
        let rule_opts = RuleInstrumentationOptions::full()
            .with_plan_diff();
        // Instrument all optimizer rules with info spans
        instrument_rules_with_info_spans!(base_state, rule_opts)
    } else {
        base_state
    };

    // Add physical plan instrumentation
    instrument_with_info_spans!(state, instrumentation)
}

/// Execution span builder for structured tracing.
///
/// Creates a root span for each engine execution with spec identity,
/// rulepack fingerprint, and profile metadata as span attributes.
pub fn execution_span(
    spec_hash: &[u8; 32],
    envelope_hash: &[u8; 32],
    rulepack_fingerprint: &[u8; 32],
    profile_name: &str,
) -> tracing::Span {
    info_span!(
        "codeanatomy_engine.execute",
        spec_hash = hex::encode(spec_hash),
        envelope_hash = hex::encode(envelope_hash),
        rulepack_fingerprint = hex::encode(rulepack_fingerprint),
        profile = profile_name,
    )
}
```

### 13.3 Spec Extension

```rust
// Addition to SemanticExecutionSpec runtime config
pub struct RuntimeConfig {
    // ... existing fields ...

    /// Enable OpenTelemetry execution tracing.
    pub enable_tracing: bool,

    /// Enable per-rule plan diff tracing (expensive, compliance mode only).
    pub enable_rule_tracing: bool,

    /// Enable plan preview in tracing spans (records first N rows).
    pub enable_plan_preview: bool,
}
```

### 13.4 Cargo.toml Addition

```toml
# New optional dependencies
datafusion-tracing = { version = "51.0.0", optional = true }
tracing = { version = "0.1", optional = true }
hex = "0.4"

# Updated [features]
tracing = ["dep:datafusion-tracing", "dep:tracing", "dep:hex"]
```

Pinning note: `datafusion-tracing` version must match the DataFusion major used by the workspace. CI should fail fast on mismatched feature resolution.

### 13.5 Implementation Checklist

- [ ] Create `executor/tracing.rs` with `build_tracing_state()` and `execution_span()`
- [ ] Add `datafusion-tracing` as optional dependency behind `tracing` feature flag
- [ ] Add fallback implementation path when `tracing` feature is disabled or unavailable
- [ ] Wire tracing state into `SessionFactory` when `enable_tracing` is true
- [ ] Create root execution span with spec/envelope/rulepack identity attributes
- [ ] Enable per-rule plan diffs when `enable_rule_tracing` is true
- [ ] Enable plan preview (first N rows) when `enable_plan_preview` is true
- [ ] Add `enable_tracing`, `enable_rule_tracing`, `enable_plan_preview` to RuntimeConfig
- [ ] Wire tracing spans into `run_full_pipeline()` execution path
- [ ] Zero overhead when tracing feature is disabled (compile-time elimination)
- [ ] Add CI check to verify tracing feature resolves against current DataFusion version
- [ ] Test: execution span contains correct spec_hash and envelope_hash
- [ ] Test: per-rule tracing captures rule name and plan-changed flag
- [ ] Test: tracing disabled produces zero additional overhead (benchmark)

---

## 14) WS-P14: UDAF/UDWF Named Arguments and Custom Expression Planning

### 14.1 Purpose

Leverage the existing `FunctionFactory` (at `datafusion_ext/src/function_factory.rs`) and `ExprPlanner` (at `datafusion_ext/src/expr_planner.rs`) infrastructure to enable named-argument UDFs/UDAFs for rule authoring stability, and domain-specific expression planning for span-alignment operations.

### 14.2 Current State

The infrastructure already exists:
- `SqlMacroFunctionFactory` at `function_factory.rs:109` implements `FunctionFactory` for `CREATE FUNCTION`
- `install_sql_macro_factory_native()` at `lib.rs:41` wires the factory into session
- `CodeAnatomyDomainPlanner` at `expr_planner.rs` implements custom `ExprPlanner`
- `install_expr_planners_native()` at `lib.rs:49` registers domain planners
- Named arguments via `.with_parameter_names()` on signatures at `function_factory.rs:197`

What's missing: integration of these capabilities into the engine session build pipeline (they're available as standalone functions but not wired into `SessionFactory`).

Capability policy:
1. if `enable_function_factory` or `enable_domain_planner` is requested, installation failures are hard errors
2. if not requested, the base session path remains unchanged

### 14.3 Engine Integration

```rust
// Modification to session/factory.rs

impl SessionFactory {
    pub async fn build_session_from_profile(
        &self,
        profile: &RuntimeProfileSpec,
        enable_function_factory: bool,
        enable_domain_planner: bool,
    ) -> Result<SessionContext> {
        let ctx = self.build_base_session(profile).await?;

        // Install SQL macro factory for CREATE FUNCTION support
        if enable_function_factory {
            install_sql_macro_factory_native(&ctx)?;
        }

        // Install domain-specific expression planners
        if enable_domain_planner {
            install_expr_planners_native(&ctx, &["codeanatomy_domain"])?;
        }

        // Register all UDFs, UDAFs, UDWFs, UDTFs
        self.register_all_functions(&ctx)?;

        Ok(ctx)
    }
}
```

### 14.4 Named Arguments for Rule-Authoring Stability

```rust
// Enhancement: add named arguments to existing UDAFs

impl AggregateUDFImpl for AsofSelectUdaf {
    fn signature(&self) -> &Signature {
        // Named arguments make rule SQL more stable and readable:
        // SELECT asof_select(value => col("price"), ts => col("event_ts"),
        //                    target_ts => col("target_ts"))
        &self.signature
    }
}

// In builtin_udafs(), when constructing the signature:
let sig = Signature::one_of(
    vec![TypeSignature::Exact(vec![DataType::Utf8, DataType::Int64, DataType::Int64])],
    Volatility::Immutable,
)
.with_parameter_names(vec!["value".into(), "ts".into(), "target_ts".into()]);
```

### 14.5 Implementation Checklist

- [ ] Wire `install_sql_macro_factory_native()` into `SessionFactory.build_session_from_profile()`
- [ ] Wire `install_expr_planners_native()` into `SessionFactory.build_session_from_profile()`
- [ ] Add `enable_function_factory` and `enable_domain_planner` flags to spec runtime config
- [ ] Emit typed `CapabilityUnavailable` diagnostics when requested install fails
- [ ] Add `.with_parameter_names()` to `asof_select` UDAF signature
- [ ] Add `.with_parameter_names()` to `count_if` UDAF signature
- [ ] Add `.with_parameter_names()` to `arg_max`/`arg_min` UDAF signatures
- [ ] Add `.with_parameter_names()` to multi-arg scalar UDFs (`span_make`, `prefixed_hash64`)
- [ ] Test: `CREATE FUNCTION` works via SQL after factory installation
- [ ] Test: Named argument syntax works for UDAFs (e.g., `asof_select(value => ...)`)
- [ ] Test: `CodeAnatomyDomainPlanner` correctly plans domain expressions
- [ ] Test: Named arguments are validated (wrong names → clear error)

---

## 15) New File Structure Summary

```
rust/codeanatomy_engine/src/
├── compiler/
│   ├── plan_bundle.rs           # WS-P1: Structured plan artifacts (P0/P1/P2 + metadata)
│   ├── substrait.rs             # WS-P2: Rust-native Substrait encode/decode
│   ├── param_compiler.rs        # WS-P3: Typed parameter compilation
│   ├── udtf_builder.rs          # WS-P5: Delta UDTF view builders
│   ├── inline_policy.rs         # WS-P8: Selective view inlining
│   ├── cache_policy.rs          # WS-P10: Plan-aware cache placement
│   ├── plan_compiler.rs         # (modified) Extended compile() output, resolve_source()
│   ├── view_builder.rs          # (modified) resolve_source() integration
│   ├── join_builder.rs          # (modified) resolve_source() integration
│   ├── union_builder.rs         # (modified) resolve_source() integration
│   ├── cache_boundaries.rs      # (modified) Delegates to cache_policy.rs
│   └── graph_validator.rs       # (modified) New ViewTransform variant handling
│
├── rules/
│   ├── overlay.rs               # WS-P4: Dynamic rule overlay composition
│   ├── registry.rs              # (existing)
│   ├── rulepack.rs              # (existing)
│   └── intent_compiler.rs       # (existing)
│
├── session/
│   ├── runtime_profiles.rs      # WS-P9: Deterministic runtime profile specs
│   ├── factory.rs               # (modified) build_session_from_profile(), function factory, planners
│   └── profiles.rs              # (existing EnvironmentProfile, extended)
│
├── spec/
│   ├── parameters.rs            # WS-P3: TypedParameter, ParameterValue
│   ├── execution_spec.rs        # (modified) typed_parameters, rule_overlay, runtime_profile,
│   │                            #   maintenance, cache_policy, tracing flags
│   └── relations.rs             # (modified) IncrementalCdf, Metadata, FileManifest variants
│
├── executor/
│   ├── metrics_collector.rs     # WS-P7: Real plan metrics extraction
│   ├── maintenance.rs           # WS-P11: Delta maintenance integration
│   ├── tracing.rs               # WS-P13: OpenTelemetry execution tracing
│   ├── runner.rs                # (modified) Profile-based session, maintenance, tracing
│   ├── delta_writer.rs          # (existing)
│   └── result.rs                # (modified) PlanBundle summary, maintenance reports in RunResult
│
├── tuner/
│   ├── metrics_store.rs         # WS-P7: Bounded cross-run metrics history
│   └── adaptive.rs              # (modified) Uses real metrics from collector
│
└── python/
    └── materializer.rs          # (modified) Real metrics, PlanBundle, overlay, tracing support

rust/datafusion_ext/src/
├── udaf_builtin.rs              # (modified) WS-P12: GroupsAccumulator, retract_batch
├── udwf_builtin.rs              # (modified) WS-P12: sort_options, reverse_expr
├── udf/hash.rs                  # (modified) WS-P6: simplify() hooks
├── registry_snapshot.rs         # (modified) WS-P6: hook capability inventory
├── udf/span.rs                  # (modified) WS-P6: hook parity + simplification coverage
├── udf/cdf.rs                   # (modified) WS-P6: targeted constraint propagation
├── function_factory.rs          # (existing) WS-P14: Already implements FunctionFactory
├── expr_planner.rs              # (existing) WS-P14: Already implements ExprPlanner
└── delta_maintenance.rs         # (existing) WS-P11: Called by engine maintenance executor
```

---

## 16) Cargo.toml Additions

```toml
# New dependencies in [dependencies]
datafusion-sql = { version = "51.0.0", features = ["unparser"] }
prost = "0.13"
hex = "0.4"

# New optional dependencies
datafusion-substrait = { version = "51.0.0", optional = true }
# Optional only when explicit proto decode types are needed:
substrait = { version = "0.52", optional = true }
datafusion-tracing = { version = "51.0.0", optional = true }
tracing = { version = "0.1", optional = true }

# Updated [features]
[features]
default = []
compliance = []
substrait = ["dep:datafusion-substrait"]
tracing = ["dep:datafusion-tracing", "dep:tracing", "dep:hex"]
```

---

## 17) Delivery Phases with Exit Criteria

### Phase A: Plan Artifacts + Substrait (WS-P1 + WS-P2)

**Target:** Structured plan capture with P0/P1/P2 + optional Substrait encoding.

**Exit criteria:**
- [ ] `capture_plan_bundle_runtime()` + `build_plan_bundle_artifact()` produce P0/P1/P2 + persisted artifact payloads
- [ ] P1 captured via `SessionState::optimize()` — not just formatted debug string
- [ ] Digest-based `PlanDiff` detects optimizer rule changes between two compilations
- [ ] Substrait round-trip (encode→decode) produces equivalent LogicalPlan
- [ ] SQL unparse via `plan_to_sql()` produces valid SQL for simple plans
- [ ] Schema fingerprints stable across identical compilations
- [ ] Zero overhead when compliance_capture is false

### Phase B: Typed Parameters + Dynamic Rules (WS-P3 + WS-P4)

**Target:** Replace env-var templating with typed parameters; enable per-execution rule composition.

**Exit criteria:**
- [ ] `TypedParameter` compiles to `ScalarValue` for all type variants
- [ ] `ParamValues::List` positional binding works via `DataFrame::with_param_values()`
- [ ] Filter-scoped parameters build typed `Expr` without SQL parsing
- [ ] `RuleOverlayProfile` produces new `CpgRuleSet` with added/removed/reordered rules
- [ ] `build_overlaid_session()` creates new `SessionContext` without mutating base session
- [ ] Per-rule EXPLAIN deltas captured in compliance mode
- [ ] Legacy env-var path still works when `typed_parameters` is empty

### Phase C: Delta UDTFs + UDF Hooks (WS-P5 + WS-P6)

**Target:** Delta table functions as planning primitives; UDF hook governance with targeted high-ROI closures.

**Exit criteria:**
- [ ] `IncrementalCdf` view produces DataFrame with `change_type` column
- [ ] CDF result composes with downstream joins in single optimizer-visible DAG
- [ ] `delta_snapshot` and `delta_add_actions` UDTFs callable from view transforms
- [ ] Hook capability snapshot is generated and checked in CI
- [ ] At least one missing high-ROI hook gap is closed with EXPLAIN + benchmark evidence
- [ ] No UDF correctness regressions after hook-governance changes

### Phase D: Real Metrics + Cross-View Optimization (WS-P7 + WS-P8)

**Target:** Real execution metrics feed tuner; selective view inlining reduces optimization barriers.

**Exit criteria:**
- [ ] `collect_plan_metrics()` extracts real spill_count, scan_selectivity, elapsed_compute from executed plans
- [ ] Tuner decisions based on real metrics (not placeholders)
- [ ] Metrics store persists bounded cross-run history
- [ ] Filter/project views inline into consumers (verified via EXPLAIN showing fewer scan nodes)
- [ ] Multi-consumer and output views are NOT inlined
- [ ] Inlined plans produce identical results to registered plans
- [ ] End-to-end throughput measurably improved for representative multi-branch join/union graphs

### Phase E: Runtime Profiles + Cache Policy (WS-P9 + WS-P10)

**Target:** Deterministic session profiles; statistics-informed cache placement.

**Exit criteria:**
- [ ] `RuntimeProfileSpec` codifies all remaining WS-P9 knobs with explicit defaults
- [ ] Small/medium/large presets produce different, correct session configs
- [ ] Profile fingerprint is part of `envelope_hash` determinism contract
- [ ] `pushdown_filters` + `parquet_pruning` + `enable_page_index` produce measurable scan reduction
- [ ] `collect_statistics` enables cost-based join reordering (EXPLAIN verification)
- [ ] `CachePlacementPolicy` uses statistics to inform cache decisions
- [ ] Cache memory budget respects `RuntimeProfileSpec.memory_pool_bytes`

### Phase F: Delta Maintenance + UDAF/UDWF Hooks (WS-P11 + WS-P12)

**Target:** Spec-driven post-execution maintenance; advanced aggregate/window optimizer hooks.

**Exit criteria:**
- [ ] `MaintenanceSchedule` drives compact → checkpoint → vacuum → cleanup ordering
- [ ] Vacuum retention enforced ≥ 168 hours regardless of spec value
- [ ] Dry-run-first vacuum preview works correctly
- [ ] `GroupsAccumulator` enabled for `count_distinct_agg` (Int64/Utf8 types)
- [ ] `retract_batch` works correctly for `count_if` in bounded window frames
- [ ] Merge associativity verified for all UDAFs across partitions

### Phase G: Tracing + Named Args + Expression Planning (WS-P13 + WS-P14)

**Target:** OpenTelemetry execution tracing; named-argument UDFs; domain expression planning in engine.

**Exit criteria:**
- [ ] `datafusion-tracing` wired into session when tracing feature enabled
- [ ] Fallback tracing path works when contrib crate is disabled/unavailable
- [ ] Per-operator and per-rule spans captured with correct identity attributes
- [ ] Zero overhead when tracing feature is compile-time disabled
- [ ] `SqlMacroFunctionFactory` wired into `SessionFactory`
- [ ] `CodeAnatomyDomainPlanner` wired into `SessionFactory`
- [ ] Named arguments work for multi-arg UDAFs (`asof_select`, `arg_max`)
- [ ] `CREATE FUNCTION` SQL works after factory installation

---

## 18) Dependency Summary

| Workstream | Depends On | Blocks |
|-----------|-----------|--------|
| WS-P1 (PlanBundle) | v2 WS4 compiler + WS2 session | WS-P2 (Substrait) |
| WS-P2 (Substrait) | WS-P1 (for PlanBundle integration) | None |
| WS-P3 (Typed Params) | v2 WS1 spec + WS4 compiler | None |
| WS-P4 (Dynamic Rules) | v2 WS2.5 rulepack + WS2 session | None |
| WS-P5 (Delta UDTFs) | v2 WS3 providers + WS4 compiler | None |
| WS-P6 (UDF Hooks) | v2 WS0 compat (datafusion_ext UDFs exist) | None |
| WS-P7 (Real Metrics) | v2 WS6 executor + WS7 tuner | WS-P10 (metrics inform cache policy) |
| WS-P8 (Inline Policy) | v2 WS4 compiler (view loop) | None |
| WS-P9 (Runtime Profiles) | v2 WS2 session (SessionFactory exists) | WS-P10 (profile informs cache budget) |
| WS-P10 (Cache Policy) | WS-P7 (metrics store) + WS-P9 (memory budget) | None |
| WS-P11 (Delta Maintenance) | v2 WS6 executor + datafusion_ext maintenance exists | None |
| WS-P12 (UDAF/UDWF Hooks) | v2 WS0 compat (UDAF/UDWF builtins exist) | None |
| WS-P13 (Tracing) | v2 WS2 session + WS6 executor | None |
| WS-P14 (Named Args + Planners) | v2 WS2 session + datafusion_ext factory/planners exist | None |

WS-P3, WS-P4, WS-P5, WS-P6, WS-P8, WS-P11, WS-P12, WS-P13, and WS-P14 are independent of each other and can be implemented in parallel after their v2 prerequisites are met.

WS-P10 depends on both WS-P7 (metrics store for historical data) and WS-P9 (memory budget from profile).

---

## 19) Risk Mitigations

| Risk | Mitigation |
|------|-----------|
| Substrait serialization doesn't cover all UDFs | Substrait is optional (feature flag) and portability-only. Artifact capture falls back to schema/rule/provider fingerprints. |
| View inlining changes optimization behavior | Inlining is conservative (only fanout-1 simple transforms). Disabled by default; opt-in via spec flag. |
| Real metrics introduce execution overhead | `ExecutionPlan::metrics()` is a snapshot read — zero additional computation. Tree walk is O(n) in plan size. |
| Dynamic rule overlay breaks determinism | Every overlay produces a new fingerprint. The determinism contract tracks the overlay-inclusive fingerprint. |
| Typed parameters break backward compat | Legacy `parameter_templates` + env-var path retained. Typed parameters checked first; fallback if empty. |
| UDF simplify hooks change query results | Simplify only activates for literal inputs. All transformations produce mathematically equivalent results. Regression tests compare hook-enabled vs hook-disabled outputs. |
| Runtime profile proliferation | Three canonical presets (small/medium/large) cover 95% of use cases. Custom profiles must pass fingerprint validation. |
| Cache placement policy too aggressive | Conservative defaults (min_fanout=2, no row threshold). Statistics-based decisions only when `collect_statistics` is true. Explicit overrides available. |
| Delta vacuum deletes needed files | Enforced 7-day minimum retention regardless of spec. Dry-run-first mode previews deletions. `require_protocol_check` validates feature support. |
| Delta maintenance slows pipeline | Maintenance is post-execution, never blocks compilation/execution. Optional per spec. |
| GroupsAccumulator produces different results | All GroupsAccumulator implementations tested against row-level accumulators with identical inputs. |
| retract_batch correctness for windowed aggregates | Explicit retraction correctness tests with known frame boundaries. |
| datafusion-tracing compatibility/version drift | Treat as optional contrib dependency; pin to matching DataFusion major and keep plain-tracing fallback path. |
| datafusion-tracing adds runtime overhead | Feature-flag gated (compile-time elimination when disabled). When enabled, measure and cap tracing overhead with profile defaults. |
| Named arguments break existing SQL | Named arguments are additive — existing positional call syntax continues to work. |
| FunctionFactory conflicts with pre-registered functions | Factory only handles `CREATE FUNCTION` SQL; pre-registered UDFs are not affected. |
