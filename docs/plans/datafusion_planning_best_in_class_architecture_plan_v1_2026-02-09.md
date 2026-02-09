# DataFusion Planning Best-in-Class Architecture Plan (v1)

**Date:** 2026-02-09  
**Revision:** v1.5 (deep codebase re-audit + status sync)  
**Status:** All mandatory v1 scope is implemented; only optional follow-on enhancements remain  
**Scope:** Rust engine planning architecture upgrades across session construction, planning determinism, provider contracts, optimizer workflows, Delta integration seams, test infrastructure, and orchestration unification

---

## 0) Goals

This updated plan targets a production-grade end-state with thirteen coordinated scope items:

1. Unify session planning-surface construction in one DataFusion 51-correct builder path.
2. Promote `FileFormatFactory` + `TableOptions` policy to a first-class planning control plane.
3. Add a planning-surface manifest hash and compose it into envelope + plan artifacts.
4. Complete artifact fidelity: provider identities + output write metadata + commit lineage.
5. Add an offline optimizer lab for deterministic rulepack experiments.
6. Add explicit pushdown contract modeling (`Unsupported|Exact|Inexact`) and tests.
7. Add optional Delta planner/codec seams, feature-gated and compatibility-safe.
8. Complete cutover from env-var templates to typed parameter planning.
9. Add shared test infrastructure for session/planning fixtures.
10. Replace silent error swallowing with warning propagation in run results.
11. Harden determinism contract finalization order (capture after provider registration).
12. Add runtime profile coverage contract (every profile knob applied or explicitly reserved).
13. Unify runner/materializer orchestration into one pipeline core.

---

## 1) API Corrections Applied In This Revision

This v1 update intentionally fixes drift introduced in the external review draft:

- DataFusion baseline is **51.0.0** (workspace-pinned), not 50.x.
- Builder API uses `with_expr_planners(...)` (plural), not `with_expr_planner(...)`.
- Builder supports `with_function_factory(...)`; there is no `with_function_rewrite(...)` builder API.
- Pushdown modeling is based on provider contracts (`supports_filters_pushdown`), not a nonexistent `input.expected_filters` field.
- Delta codec path is treated as optional/experimental due current upstream completeness constraints.

---

## 1.5) Implementation Status Audit (2026-02-09, deep re-audit)

This section reflects the current code state in `rust/codeanatomy_engine` and
`rust/datafusion_ext` after implementation by the other team.

| Scope | Status | Implemented Evidence | Remaining to Reach End-State |
|---|---|---|---|
| 1. Unified planning surface builder | **Complete (v1)** | `SessionFactory` now routes both profile/non-profile session builds through `build_session_state_internal(...)`; canonical `SessionBuildOverrides` APIs added; wrappers preserved for compatibility | Optional: remove compatibility wrappers in a cleanup release |
| 2. FileFormat + TableOptions policy | **Complete (v1)** | Non-profile and profile session-build paths both apply `FormatPolicySpec` + `build_table_options`; policy contributes to manifest digest | Optional: expand policy surface for future non-default file-format overrides |
| 3. Planning-surface manifest hash | **Complete** | Real manifest hash generated in `SessionFactory`, propagated into `SessionEnvelope`, `PreparedExecutionContext`, and `PlanBundleArtifact`; placeholder zero-hash propagation removed | None for current scope |
| 4. Artifact completeness | **Complete (core lineage path)** | `delta_write_batches(...)` introduced and used for output writes; commit metadata + retries wired via `build_delta_commit_options`; lineage metadata verified in Delta log test | Optional: extend lineage metadata schema with additional run-level attributes |
| 5. Offline optimizer lab | **Complete (v1)** | Runtime config includes `capture_optimizer_lab`; Python materializer executes `run_lab_from_ruleset(...)` under compliance capture and records steps into compliance JSON with non-fatal warnings | Optional: add richer lab telemetry aggregation in observability exports |
| 6. Pushdown contract modeling | **Complete (execution + tests)** | Real-plan residual-filter assertion added for `Inexact`; pushdown probes recorded in compliance capture path; probe field serialized in compliance payload | Optional: broaden probe extraction to more transform shapes beyond direct input filters |
| 7. Optional Delta planner/codec seams | **Complete (v1)** | Runtime config includes `capture_delta_codec`; codec install is toggled in canonical session build; plan bundle artifact now captures optional codec bytes with warning fallback when unavailable | Optional: add codec-bytes diff tooling in artifact diagnostics |
| 8. Typed parameter cutover | **Complete (transition safeguards)** | Deprecated template-mode warning emitted via pipeline warning surface; exclusivity validation retained | Optional: remove template mode after deprecation window |
| 9. Shared test infrastructure | **Complete (v1 major suites)** | `tests/common/mod.rs` expanded and adopted across `end_to_end`, `plan_compilation`, `spec_contract`, `graph_validation`, and `provider_registration` to remove repeated fixture assembly | Optional: continue migrating remaining low-duplication suites opportunistically |
| 10. Warning propagation | **Complete (v1)** | Optional plan-capture failures return warnings; Python compliance explain capture now uses explicit error-to-warning mapping; warnings merged into `RunResult.warnings` | Optional: add warning categories/codes for downstream analytics |
| 11. Determinism hardening (capture order + composition) | **Complete (core contract)** | Envelope now composes provider-identity aggregate hash; shared pre-pipeline ordering in `prepare_execution_context` enforces capture after input registration | Optional: retire compatibility wrapper `build_session(...)` in cleanup release |
| 12. Runtime profile coverage contract | **Complete** | Reserved-profile fields produce explicit runtime warnings via `reserved_profile_warnings`; warnings threaded through preflight/pipeline surfaces | Optional: promote warnings to telemetry counters |
| 13. Pipeline orchestration unification | **Complete (v1 + parity hardening)** | Shared `PreparedExecutionContext` and `prepare_execution_context(...)` power runner/materializer; Rust and Python parity regression tests now assert stable envelope/planning/provider identity surfaces | Optional: add cross-language golden fixtures for long-term regression baselines |

Overall assessment:
- All thirteen v1 scope items are implemented on production code paths.
- Remaining work is optional optimization/cleanup scope, not correctness or contract coverage gaps.

## 1.6) Remaining Implementation Scope (Actionable, Re-Audited)

No mandatory v1 scope remains.

Optional follow-on work:
1. Remove compatibility wrappers in `SessionFactory` after one release cycle.
2. Add structured warning categories/codes and warning-level telemetry counters.
3. Extend parity coverage with cross-language golden fixtures and long-horizon drift tests.
4. Extend pushdown probe extraction beyond direct input-filter transforms.
5. Enrich Delta lineage metadata with additional run-level provenance fields.

Note: the per-scope sections below preserve the original end-state design narrative and checklists; use Sections 1.5 and 1.6 as the authoritative implementation-status tracker.

---

## Scope Item 1: Unified Planning-Surface Builder (DataFusion 51-Correct)

### Why

Session construction is currently split (`build_session` vs `build_session_from_profile`), and one path still performs post-build state mutation for SQL macro factory/expr planners. We need one canonical assembly path so planning behavior is explicit, testable, and deterministic.

### Current Gaps

- Duplicate construction logic between `build_session` and `build_session_from_profile`.
- Post-build mutation via `install_sql_macro_factory_native()` / `install_expr_planners_native()` conflicts with the documented no-mutation contract.
- Planning registries are not modeled as one typed object.

### Representative Code Snippet

```rust
// New: rust/codeanatomy_engine/src/session/planning_surface.rs

use std::sync::Arc;

use datafusion::catalog::TableProviderFactory;
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::execution::context::{FunctionFactory, QueryPlanner, SessionContext};
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion_common::config::TableOptions;
use datafusion_common::Result;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::planner::ExprPlanner;
use datafusion_expr::registry::FunctionRegistry;

#[derive(Clone, Default)]
pub struct PlanningSurfaceSpec {
    pub enable_default_features: bool,
    pub file_formats: Vec<Arc<dyn FileFormatFactory>>,
    pub table_options: Option<TableOptions>,
    pub table_factories: Vec<(String, Arc<dyn TableProviderFactory>)>,
    pub expr_planners: Vec<Arc<dyn ExprPlanner>>,
    pub function_factory: Option<Arc<dyn FunctionFactory>>,
    pub query_planner: Option<Arc<dyn QueryPlanner + Send + Sync>>,
    pub function_rewrites: Vec<Arc<dyn FunctionRewrite + Send + Sync>>,
}

pub fn apply_to_builder(
    mut builder: SessionStateBuilder,
    spec: &PlanningSurfaceSpec,
) -> SessionStateBuilder {
    if spec.enable_default_features {
        builder = builder.with_default_features();
    }
    if !spec.file_formats.is_empty() {
        builder = builder.with_file_formats(spec.file_formats.clone());
    }
    if let Some(options) = spec.table_options.clone() {
        builder = builder.with_table_options(options);
    }
    if !spec.expr_planners.is_empty() {
        builder = builder.with_expr_planners(spec.expr_planners.clone());
    }
    if let Some(factory) = spec.function_factory.clone() {
        builder = builder.with_function_factory(Some(factory));
    }
    if let Some(planner) = spec.query_planner.clone() {
        builder = builder.with_query_planner(planner);
    }
    for (name, factory) in &spec.table_factories {
        builder = builder.with_table_factory(name.clone(), Arc::clone(factory));
    }
    builder
}

pub fn install_rewrites(
    ctx: &mut SessionContext,
    rewrites: &[Arc<dyn FunctionRewrite + Send + Sync>],
) -> Result<()> {
    for rewrite in rewrites {
        ctx.register_function_rewrite(Arc::clone(rewrite))?;
    }
    Ok(())
}
```

### Target Files

| File | Action |
|---|---|
| `rust/codeanatomy_engine/src/session/planning_surface.rs` | **New** |
| `rust/codeanatomy_engine/src/session/factory.rs` | **Edit** — both builders route through one planning-surface path |
| `rust/codeanatomy_engine/src/session/mod.rs` | **Edit** — export new module |
| `rust/datafusion_ext/src/lib.rs` | **Edit** — replace ad-hoc state writes with reusable planning spec helpers |
| `rust/datafusion_ext/src/function_factory.rs` | **Edit** — expose factory constructor for builder path |
| `rust/codeanatomy_engine/tests/session_determinism.rs` | **Edit** — assert deterministic planning-surface behavior |

### Implementation Checklist

- [ ] Add `PlanningSurfaceSpec` + `apply_to_builder()`.
- [ ] Refactor `build_session()` and `build_session_from_profile()` to a shared internal builder function.
- [ ] Keep rewrite installation in a single controlled post-build hook (`install_rewrites`) until a builder-native pattern is available.
- [ ] Remove direct post-build mutation calls from `factory.rs` and replace with centralized planning-surface assembly.
- [ ] Add tests proving equivalent specs produce equivalent planning surfaces.
- [ ] Add tests proving planning-surface differences change deterministic fingerprints.

---

## Scope Item 2: FileFormat + TableOptions Policy Layer

### Why

`FileFormatFactory` and `TableOptions` are a planning-time control plane, not incidental settings. We should codify this into a typed policy module, with explicit compatibility behavior per profile.

### Current Gaps

- Session builder path does not explicitly use `with_file_formats()` / `with_table_options()` in engine factory.
- Format behavior is implicit in defaults.
- No explicit policy contract for future custom file formats.

### Representative Code Snippet

```rust
// New: rust/codeanatomy_engine/src/session/format_policy.rs

use std::sync::Arc;

use datafusion::datasource::file_format::FileFormatFactory;
use datafusion_common::config::{ConfigFileType, TableOptions};
use datafusion_common::Result;
use datafusion_execution::config::SessionConfig;

#[derive(Debug, Clone, Default)]
pub struct FormatPolicySpec {
    pub parquet_pushdown_filters: bool,
    pub parquet_enable_page_index: bool,
    pub csv_delimiter: Option<String>,
}

pub fn build_table_options(
    config: &SessionConfig,
    policy: &FormatPolicySpec,
) -> Result<TableOptions> {
    let mut options = TableOptions::default_from_session_config(config.options());

    options.set_config_format(ConfigFileType::PARQUET);
    options.set(
        "format.pushdown_filters",
        if policy.parquet_pushdown_filters { "true" } else { "false" },
    )?;
    options.set(
        "format.enable_page_index",
        if policy.parquet_enable_page_index { "true" } else { "false" },
    )?;

    if let Some(delim) = &policy.csv_delimiter {
        options.set_config_format(ConfigFileType::CSV);
        options.set("format.delimiter", delim)?;
    }

    Ok(options)
}

pub fn default_file_formats() -> Vec<Arc<dyn FileFormatFactory>> {
    // Starts from DataFusion defaults via with_default_features(), then optional overrides.
    vec![]
}
```

```rust
// In session/factory.rs
let format_policy = FormatPolicySpec {
    parquet_pushdown_filters: profile.pushdown_filters,
    parquet_enable_page_index: profile.enable_page_index,
    csv_delimiter: None,
};
let planning_surface = PlanningSurfaceSpec {
    enable_default_features: true,
    file_formats: default_file_formats(),
    table_options: Some(build_table_options(&config, &format_policy)?),
    ..PlanningSurfaceSpec::default()
};
builder = apply_to_builder(builder, &planning_surface);
```

### Target Files

| File | Action |
|---|---|
| `rust/codeanatomy_engine/src/session/format_policy.rs` | **New** |
| `rust/codeanatomy_engine/src/session/factory.rs` | **Edit** — wire format policy into planning surface |
| `rust/codeanatomy_engine/src/session/runtime_profiles.rs` | **Edit** — align profile knobs with policy fields |
| `rust/codeanatomy_engine/tests/compat_smoke.rs` | **Edit** — compile checks for `with_file_formats`/`with_table_options` |
| `rust/codeanatomy_engine/tests/session_determinism.rs` | **Edit** — policy changes alter fingerprints |

### Implementation Checklist

- [ ] Introduce typed `FormatPolicySpec`.
- [ ] Build `TableOptions` via `default_from_session_config(config.options())`.
- [ ] Apply explicit format keys with validation (`set(...)` errors are fatal).
- [ ] Use policy object in both environment-profile and runtime-profile session paths.
- [ ] Add deterministic serialization/fingerprint coverage for policy knobs.
- [ ] Document which file formats are default, overridden, or disabled.

---

## Scope Item 3: Planning-Surface Manifest Hash

### Why

Current envelope hashing does not isolate planning-surface identity as a first-class object. We need a deterministic planning manifest and digest that is reusable across envelope, bundles, and diffs.

### Current Gaps

- `SessionEnvelope` hashes broad state but no dedicated planning-surface manifest.
- `PlanBundleArtifact` has no planning-surface hash field.
- Harder to diagnose plan drift caused by registry/policy changes.

### Representative Code Snippet

```rust
// New: rust/codeanatomy_engine/src/session/planning_manifest.rs

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanningSurfaceManifest {
    pub datafusion_version: String,
    pub default_features_enabled: bool,
    pub file_format_names: Vec<String>,
    pub table_factory_names: Vec<String>,
    pub expr_planner_names: Vec<String>,
    pub function_factory_name: Option<String>,
    pub query_planner_name: Option<String>,
    pub table_options_digest: [u8; 32],
}

impl PlanningSurfaceManifest {
    pub fn hash(&self) -> [u8; 32] {
        let bytes = serde_json::to_vec(self).expect("planning manifest serializable");
        *blake3::hash(&bytes).as_bytes()
    }
}
```

```rust
// In session/envelope.rs + compiler/plan_bundle.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionEnvelope {
    // existing fields...
    pub planning_surface_hash: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanBundleArtifact {
    // existing fields...
    pub planning_surface_hash: [u8; 32],
}
```

### Target Files

| File | Action |
|---|---|
| `rust/codeanatomy_engine/src/session/planning_manifest.rs` | **New** |
| `rust/codeanatomy_engine/src/session/envelope.rs` | **Edit** — add planning manifest/hash composition |
| `rust/codeanatomy_engine/src/compiler/plan_bundle.rs` | **Edit** — persist planning hash in artifact |
| `rust/codeanatomy_engine/src/executor/runner.rs` | **Edit** — pass planning hash into artifact capture |
| `rust/codeanatomy_engine/src/python/materializer.rs` | **Edit** — pass planning hash through Python path |
| `rust/codeanatomy_engine/tests/session_determinism.rs` | **Edit** — planning hash stability tests |

### Implementation Checklist

- [ ] Add `PlanningSurfaceManifest` model and hash method.
- [ ] Capture normalized planner/file-format/factory names in deterministic sort order.
- [ ] Add `planning_surface_hash` to `SessionEnvelope`.
- [ ] Add `planning_surface_hash` to `PlanBundleArtifact`.
- [ ] Include planning hash in artifact diff summaries.
- [ ] Add serialization-order stability tests for manifest hashing.

---

## Scope Item 4: Artifact Completeness (Provider Identities + Output Metadata)

### Why

Plan artifacts currently miss critical provenance: provider identities are dropped (`vec![]` call sites), and materialization metadata fields (`delta_version`, `files_added`, `bytes_written`) are often left `None`.

### Current Gaps

- `build_plan_bundle_artifact(...)` call sites still pass empty provider lists.
- Commit properties are built but unused in write path.
- Output target knobs (`partition_by`, `write_metadata`, `max_commit_retries`) are not fully honored.

### Representative Code Snippet

```rust
// In runner/materializer before plan bundle capture
let registrations = register_extraction_inputs(ctx, &spec.input_relations).await?;
let mut provider_identities = registrations
    .iter()
    .map(|r| plan_bundle::ProviderIdentity {
        table_name: r.name.clone(),
        identity_hash: r.provider_identity,
    })
    .collect::<Vec<_>>();
provider_identities.sort_by(|a, b| a.table_name.cmp(&b.table_name));

let artifact = plan_bundle::build_plan_bundle_artifact(
    ctx,
    &runtime,
    rulepack_fingerprint,
    provider_identities,
    spec.runtime.capture_substrait,
    false,
)
.await?;
```

```rust
// In executor/runner.rs write path
use deltalake::kernel::transaction::CommitProperties;

let write_options = DataFrameWriteOptions::new()
    .with_insert_operation(insert_op)
    .with_partition_by(target.partition_by.clone());
let write_result = df.write_table(&target.table_name, write_options).await?;

let commit_metadata = crate::executor::delta_writer::build_commit_properties(
    &target,
    spec_hash,
    envelope_hash,
);
let _commit_properties = CommitProperties::default()
    .with_metadata(
        commit_metadata
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::String(v))),
    )
    .with_max_retries(target.max_commit_retries.unwrap_or(3) as usize);

let materialized = crate::executor::delta_writer::read_write_outcome(
    ctx,
    &target.table_name,
    target.delta_location.as_deref(),
)
.await?;
```

### Target Files

| File | Action |
|---|---|
| `rust/codeanatomy_engine/src/executor/runner.rs` | **Edit** — pass provider identities; populate output metadata fields |
| `rust/codeanatomy_engine/src/python/materializer.rs` | **Edit** — retain registrations and pass provider identities |
| `rust/codeanatomy_engine/src/compiler/plan_bundle.rs` | **Edit** — enforce provider identities when inputs exist |
| `rust/codeanatomy_engine/src/executor/delta_writer.rs` | **Edit** — add `read_write_outcome(...)` helper |
| `rust/codeanatomy_engine/src/spec/outputs.rs` | **Edit** — clarify semantics of `partition_by`/`write_metadata`/`max_commit_retries` |
| `rust/codeanatomy_engine/tests/provider_registration.rs` | **Edit** — identity propagation assertions |
| `rust/codeanatomy_engine/tests/plan_compilation.rs` | **Edit** — bundle provider identity coverage |
| `rust/codeanatomy_engine/tests/end_to_end.rs` | **Edit** — output metadata fields populated |

### Implementation Checklist

- [ ] Capture and sort provider identities in every execution path.
- [ ] Remove all placeholder `vec![]` provider identity calls.
- [ ] Thread `partition_by` into `DataFrameWriteOptions`.
- [ ] Activate commit metadata path and connect retries policy.
- [ ] Populate `delta_version` / `files_added` / `bytes_written` where obtainable.
- [ ] Add regression tests for provider drift and output metadata completeness.

---

## Scope Item 5: Offline Optimizer Lab

### Why

In-session optimization is necessary but not sufficient for rulepack R&D. A standalone optimizer harness enables deterministic per-rule diffs and reproducible CI experiments.

### Current Gaps

- Rule tracing is tightly coupled to runtime execution path.
- No controlled harness for rule order/observer experiments.
- Limited ability to run what-if optimization profiles offline.

### Representative Code Snippet

```rust
// New: rust/codeanatomy_engine/src/stability/optimizer_lab.rs

use std::sync::Arc;

use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::optimizer::{Optimizer, OptimizerContext};
use datafusion::optimizer::OptimizerRule;
use datafusion_common::Result;

#[derive(Debug, Clone)]
pub struct RuleStep {
    pub ordinal: usize,
    pub rule_name: String,
    pub plan_digest: [u8; 32],
}

pub fn run_optimizer_lab(
    input: LogicalPlan,
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    max_passes: usize,
    skip_failed_rules: bool,
) -> Result<(LogicalPlan, Vec<RuleStep>)> {
    let optimizer = Optimizer::with_rules(rules);
    let config = OptimizerContext::new()
        .with_max_passes(max_passes)
        .with_skip_failing_rules(skip_failed_rules);

    let mut steps = Vec::new();
    let optimized = optimizer.optimize(input, &config, |plan, rule| {
        let digest = blake3::hash(plan.display_indent().to_string().as_bytes());
        steps.push(RuleStep {
            ordinal: steps.len(),
            rule_name: rule.name().to_string(),
            plan_digest: *digest.as_bytes(),
        });
    })?;

    Ok((optimized, steps))
}
```

### Target Files

| File | Action |
|---|---|
| `rust/codeanatomy_engine/src/stability/optimizer_lab.rs` | **New** |
| `rust/codeanatomy_engine/src/stability/mod.rs` | **Edit** — export lab API |
| `rust/codeanatomy_engine/src/compliance/capture.rs` | **Edit** — optional lab capture integration |
| `rust/codeanatomy_engine/tests/rule_registration.rs` | **Edit** — deterministic step sequence tests |
| `rust/codeanatomy_engine/tests/rule_tracing_diff.rs` | **Edit** — compare in-session vs lab traces |

### Implementation Checklist

- [ ] Add standalone optimizer lab harness.
- [ ] Support rulepack subsets and max-pass overrides.
- [ ] Persist per-step digests and rule ordering.
- [ ] Integrate optional lab output into compliance capture.
- [ ] Add deterministic test fixtures for rule-step traces.

---

## Scope Item 6: Generic Pushdown Contract Modeling + Tests

### Why

Provider pushdown capabilities should be modeled explicitly at filter granularity. Boolean capability flags are not enough for optimizer trust and correctness auditing.

### Current Gaps

- No first-class model of per-filter `Unsupported|Exact|Inexact` outcomes.
- Existing capability metadata is coarse.
- No dedicated correctness tests for `Inexact` residual filtering behavior.

### Representative Code Snippet

```rust
// New: rust/codeanatomy_engine/src/providers/pushdown_contract.rs

use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion_expr::Expr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushdownProbe {
    pub provider: String,
    pub filter_sql: Vec<String>,
    pub statuses: Vec<TableProviderFilterPushDown>,
}

pub fn probe_pushdown(
    provider_name: &str,
    provider: &dyn datafusion::datasource::TableProvider,
    filters: &[Expr],
) -> datafusion_common::Result<PushdownProbe> {
    let refs = filters.iter().collect::<Vec<_>>();
    let statuses = provider.supports_filters_pushdown(&refs)?;
    Ok(PushdownProbe {
        provider: provider_name.to_string(),
        filter_sql: filters.iter().map(ToString::to_string).collect(),
        statuses,
    })
}
```

### Target Files

| File | Action |
|---|---|
| `rust/codeanatomy_engine/src/providers/pushdown_contract.rs` | **New** |
| `rust/codeanatomy_engine/src/providers/mod.rs` | **Edit** — export contract types |
| `rust/codeanatomy_engine/src/providers/registration.rs` | **Edit** — optional contract capture hook |
| `rust/codeanatomy_engine/tests/provider_pushdown_contract.rs` | **New** |
| `rust/codeanatomy_engine/tests/provider_registration.rs` | **Edit** — serialize pushdown probe metadata |

### Implementation Checklist

- [ ] Add pushdown probe model capturing per-filter statuses.
- [ ] Add helper to invoke `supports_filters_pushdown` on providers.
- [ ] Persist probe output in compliance/debug artifacts (optional at runtime).
- [ ] Add tests for exact, unsupported, and inexact statuses.
- [ ] Add test confirming residual filters remain in logical plan when required.

---

## Scope Item 7: Optional Delta Planner + Delta Codec Seams

### Why

Core execution should keep a simple default path, but advanced deployments need feature-gated seams for Delta planner integration and extension-node serialization.

### Current Gaps

- No feature-gated Delta planner path in engine session factory.
- No engine-owned codec seam for `datafusion-proto` extension codecs.
- Compatibility risks are not isolated behind explicit flags.

### Representative Code Snippet

```rust
// In session/factory.rs
#[cfg(feature = "delta-planner")]
{
    let planner = deltalake::delta_datafusion::planner::DeltaPlanner::new();
    builder = builder.with_query_planner(planner);
}
```

```rust
// New: rust/codeanatomy_engine/src/compiler/plan_codec.rs
#[cfg(feature = "delta-codec")]
pub fn encode_with_delta_codecs(
    logical: &datafusion::logical_expr::LogicalPlan,
    physical: &dyn datafusion::physical_plan::ExecutionPlan,
) -> datafusion_common::Result<(Vec<u8>, Vec<u8>)> {
    use datafusion_proto::bytes::{
        logical_plan_to_bytes_with_extension_codec,
        physical_plan_to_bytes_with_extension_codec,
    };
    use deltalake::delta_datafusion::{DeltaLogicalCodec, DeltaPhysicalCodec};

    let logical_bytes = logical_plan_to_bytes_with_extension_codec(logical, &DeltaLogicalCodec {})?;
    let physical_bytes =
        physical_plan_to_bytes_with_extension_codec(physical, &DeltaPhysicalCodec {})?;
    Ok((logical_bytes, physical_bytes))
}
```

### Target Files

| File | Action |
|---|---|
| `rust/codeanatomy_engine/Cargo.toml` | **Edit** — add `delta-planner` and `delta-codec` feature flags |
| `rust/codeanatomy_engine/src/session/factory.rs` | **Edit** — optional planner install |
| `rust/codeanatomy_engine/src/compiler/plan_codec.rs` | **New** |
| `rust/codeanatomy_engine/src/compiler/mod.rs` | **Edit** — export codec module |
| `rust/codeanatomy_engine/tests/compat_smoke.rs` | **Edit** — feature-gated compile checks |

### Implementation Checklist

- [ ] Add explicit feature gates for planner and codec paths.
- [ ] Install `DeltaPlanner` only when feature enabled.
- [ ] Implement codec helpers via `datafusion-proto` extension codec APIs.
- [ ] Keep codec path disabled by default; add compatibility tests around current Delta codec behavior.
- [ ] Document operational guidance for default/off and advanced/on modes.

---

## Scope Item 8: Typed Parameter Planning Cutover

### Why

Typed parameter support exists but main compilation still uses env-var template substitution. We should make typed parameters canonical and reduce template mode to a temporary compatibility path.

### Current Gaps

- `plan_compiler.rs` still traverses `parameter_templates` and env vars.
- No strict exclusivity validation between typed and template modes.
- Deterministic cache behavior remains coupled to env-based substitutions.

### Representative Code Snippet

```rust
// In compiler/plan_compiler.rs
use crate::compiler::param_compiler::apply_typed_parameters;

fn validate_parameter_mode(spec: &SemanticExecutionSpec) -> datafusion_common::Result<()> {
    if !spec.typed_parameters.is_empty() && !spec.parameter_templates.is_empty() {
        return Err(datafusion_common::DataFusionError::Plan(
            "Use either typed_parameters or parameter_templates, not both".to_string(),
        ));
    }
    Ok(())
}

async fn apply_parameters(
    &self,
    df: datafusion::prelude::DataFrame,
) -> datafusion_common::Result<datafusion::prelude::DataFrame> {
    if !self.spec.typed_parameters.is_empty() {
        return apply_typed_parameters(df, &self.spec.typed_parameters).await;
    }

    // Transitional fallback only; remove after deprecation window.
    self.apply_template_parameters(df).await
}
```

### Target Files

| File | Action |
|---|---|
| `rust/codeanatomy_engine/src/compiler/plan_compiler.rs` | **Edit** — typed path becomes primary |
| `rust/codeanatomy_engine/src/compiler/graph_validator.rs` | **Edit** — parameter mode exclusivity validation |
| `rust/codeanatomy_engine/src/spec/execution_spec.rs` | **Edit** — deprecation notes for template mode |
| `rust/codeanatomy_engine/src/spec/rule_intents.rs` | **Edit** — phase out template intent surfaces |
| `rust/codeanatomy_engine/tests/plan_compilation.rs` | **Edit** — typed parameter determinism tests |
| `rust/codeanatomy_engine/tests/spec_contract.rs` | **Edit** — invalid dual-mode parameter tests |

### Implementation Checklist

- [ ] Add hard validation for typed-vs-template exclusivity.
- [ ] Route compilation through `apply_typed_parameters()` whenever typed parameters exist.
- [ ] Keep template mode as explicit transitional fallback only.
- [ ] Add warnings when template mode is used.
- [ ] Add tests for placeholder binding and typed filter semantics.
- [ ] Add tests for deterministic plan shape with typed parameters.

---

## Scope Item 9: Shared Test Infrastructure (New)

### Why

Planning/session tests are currently repetitive and drift-prone. A shared fixture module reduces boilerplate and keeps deterministic assertions consistent across suites.

### Current Gaps

- Repeated session/ruleset setup logic in multiple test files.
- Inconsistent conventions for determinism assertions.
- Harder to evolve test defaults safely.

### Representative Code Snippet

```rust
// New: rust/codeanatomy_engine/tests/common/mod.rs

use crate::rules::registry::CpgRuleSet;
use crate::session::factory::SessionFactory;
use crate::session::profiles::{EnvironmentClass, EnvironmentProfile};

pub fn empty_ruleset() -> CpgRuleSet {
    CpgRuleSet {
        analyzer_rules: vec![],
        optimizer_rules: vec![],
        physical_rules: vec![],
        fingerprint: [0u8; 32],
    }
}

pub async fn test_session() -> (datafusion::prelude::SessionContext, crate::session::envelope::SessionEnvelope) {
    let factory = SessionFactory::new(EnvironmentProfile::from_class(EnvironmentClass::Small));
    factory.build_session(&empty_ruleset(), [0u8; 32], None).await.unwrap()
}
```

### Target Files

| File | Action |
|---|---|
| `rust/codeanatomy_engine/tests/common/mod.rs` | **New** |
| `rust/codeanatomy_engine/tests/session_determinism.rs` | **Edit** — use shared fixtures |
| `rust/codeanatomy_engine/tests/provider_registration.rs` | **Edit** — use shared fixtures |
| `rust/codeanatomy_engine/tests/plan_compilation.rs` | **Edit** — use shared fixtures |
| `rust/codeanatomy_engine/tests/compat_smoke.rs` | **Edit** — centralize API gate helpers |

### Implementation Checklist

- [ ] Add shared `tests/common` fixture module.
- [ ] Migrate determinism/provider/plan tests to shared helpers.
- [ ] Standardize hash/assert helper utilities.
- [ ] Keep per-test overrides explicit (avoid hidden defaults).
- [ ] Ensure no coverage loss during fixture migration.

---

## Scope Item 10: Warning Propagation (No More Silent `.ok()` Swallowing)

### Why

Non-fatal planning/compliance failures are currently dropped silently in some paths. We need explicit warning propagation to keep runs observable without making these paths hard-fail.

### Current Gaps

- `.ok()` and similar swallowing patterns in materializer and bundle capture paths.
- `RunResult` has no warnings field.
- Python boundary cannot surface non-fatal degradations.

### Representative Code Snippet

```rust
// In executor/result.rs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    // existing fields...
    pub warnings: Vec<String>,
}

impl RunResultBuilder {
    pub fn add_warning(mut self, warning: impl Into<String>) -> Self {
        self.warnings.push(warning.into());
        self
    }
}
```

```rust
// In python/materializer.rs
let mut warnings: Vec<String> = Vec::new();

match capture.to_json() {
    Ok(json) => compliance_capture_json = Some(json),
    Err(e) => warnings.push(format!("Compliance capture serialization failed: {e}")),
}

// Thread warnings into RunResult
builder = builder.with_warnings(warnings);
```

### Target Files

| File | Action |
|---|---|
| `rust/codeanatomy_engine/src/executor/result.rs` | **Edit** — add warnings field + builder support |
| `rust/codeanatomy_engine/src/executor/runner.rs` | **Edit** — convert swallow paths to warning capture |
| `rust/codeanatomy_engine/src/python/materializer.rs` | **Edit** — replace silent `.ok()` with warning accumulation |
| `rust/codeanatomy_engine/src/python/result.rs` | **Edit** — include warnings in serialized payload |
| `rust/codeanatomy_engine/tests/end_to_end.rs` | **Edit** — warning behavior assertions |

### Implementation Checklist

- [ ] Add `warnings: Vec<String>` to `RunResult`.
- [ ] Add builder helpers (`add_warning`, `with_warnings`).
- [ ] Replace silent swallow patterns with warnings.
- [ ] Preserve non-fatal behavior for optional capture paths.
- [ ] Expose warnings to Python consumers.
- [ ] Add tests for both warning and no-warning scenarios.

---

## Scope Item 11: Determinism Contract Hardening (Capture Order + Hash Composition)

### Why

Envelope capture currently occurs before input registration in at least one execution path, which can produce incomplete determinism fingerprints. Envelope finalization must happen only after provider registrations and planning-surface hashing are finalized.

### Current Gaps

- Envelope capture happens before `register_extraction_inputs(...)` in Python path.
- Envelope hash does not explicitly include provider-identity aggregate hash.
- Finalization order differs between execution paths.

### Representative Code Snippet

```rust
// New sequencing contract in orchestrator/materializer path
let (mut ctx, planning_surface) = session_factory.build_session_state(...).await?;

let registrations = register_extraction_inputs(&ctx, &spec.input_relations).await?;
let provider_hash = crate::session::envelope::hash_provider_identities(&registrations);

let planning_manifest = planning_surface.to_manifest();
let planning_hash = planning_manifest.hash();

let envelope = SessionEnvelope::capture_after_registration(
    &ctx,
    spec_hash,
    rulepack_fingerprint,
    planning_hash,
    provider_hash,
)
.await?;
```

### Target Files

| File | Action |
|---|---|
| `rust/codeanatomy_engine/src/session/envelope.rs` | **Edit** — provider hash + planning hash in envelope composition |
| `rust/codeanatomy_engine/src/python/materializer.rs` | **Edit** — move envelope capture after input registration |
| `rust/codeanatomy_engine/src/executor/runner.rs` | **Edit** — enforce same sequencing contract |
| `rust/codeanatomy_engine/src/session/factory.rs` | **Edit** — expose pre-envelope session build step |
| `rust/codeanatomy_engine/tests/session_determinism.rs` | **Edit** — ordering-sensitive determinism tests |

### Implementation Checklist

- [ ] Define explicit sequencing: session build -> provider registration -> envelope capture.
- [ ] Add provider identity aggregate hash function.
- [ ] Compose planning hash + provider hash into final envelope hash.
- [ ] Enforce ordering in both Python and native paths.
- [ ] Add regression tests that fail on early capture.

---

## Scope Item 12: Runtime Profile Coverage Contract

### Why

`RuntimeProfileSpec` contains many knobs that are not currently applied by session factory paths. We need an explicit coverage contract so every field is either applied or intentionally marked reserved.

### Current Gaps

- Profile fields like `meta_fetch_concurrency`, `max_predicate_cache_size`, listing/metadata cache limits are not wired in factory.
- No machine-checkable mapping from profile fields to application points.
- Fingerprints can imply controls that are currently non-operative.

### Representative Code Snippet

```rust
// New: rust/codeanatomy_engine/src/session/profile_coverage.rs

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum CoverageState {
    Applied,
    Reserved,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeProfileCoverage {
    pub field: String,
    pub state: CoverageState,
    pub note: String,
}

pub fn evaluate_profile_coverage() -> Vec<RuntimeProfileCoverage> {
    vec![
        RuntimeProfileCoverage {
            field: "target_partitions".into(),
            state: CoverageState::Applied,
            note: "SessionConfig::with_target_partitions".into(),
        },
        RuntimeProfileCoverage {
            field: "meta_fetch_concurrency".into(),
            state: CoverageState::Reserved,
            note: "No stable DataFusion 51 surface in current engine path".into(),
        },
    ]
}
```

### Target Files

| File | Action |
|---|---|
| `rust/codeanatomy_engine/src/session/profile_coverage.rs` | **New** |
| `rust/codeanatomy_engine/src/session/runtime_profiles.rs` | **Edit** — link fields to coverage contract |
| `rust/codeanatomy_engine/src/session/factory.rs` | **Edit** — update application mapping |
| `rust/codeanatomy_engine/tests/compat_smoke.rs` | **Edit** — coverage contract smoke checks |
| `rust/codeanatomy_engine/tests/session_determinism.rs` | **Edit** — applied/reserved set must be deterministic |

### Implementation Checklist

- [ ] Add runtime profile coverage registry (`Applied` vs `Reserved`).
- [ ] Require every `RuntimeProfileSpec` field to have one registry entry.
- [ ] Wire additional fields that have stable API support now.
- [ ] Emit warnings or docs for reserved knobs.
- [ ] Add CI test that fails when new profile fields lack coverage entries.

---

## Scope Item 13: Pipeline Orchestration Unification (Runner + Python)

### Why

`python/materializer.rs` and `executor/runner.rs` currently duplicate orchestration logic (plan bundle capture, materialization, compliance, metrics, maintenance). This divergence causes behavior drift and duplicated bugs.

### Current Gaps

- Two orchestration paths with overlapping but inconsistent behavior.
- Provider identity, warning handling, and determinism sequencing differ by path.
- Harder to enforce one correctness contract end-to-end.

### Representative Code Snippet

```rust
// New: rust/codeanatomy_engine/src/executor/pipeline.rs

pub struct PipelineOutcome {
    pub run_result: crate::executor::result::RunResult,
    pub envelope: crate::session::envelope::SessionEnvelope,
}

pub async fn execute_pipeline(
    spec: &crate::spec::execution_spec::SemanticExecutionSpec,
    ruleset: &crate::rules::registry::CpgRuleSet,
    session_factory: &crate::session::factory::SessionFactory,
) -> datafusion_common::Result<PipelineOutcome> {
    // Shared sequence:
    // 1) build session state
    // 2) register inputs
    // 3) capture envelope
    // 4) compile
    // 5) capture bundles
    // 6) materialize
    // 7) metrics + maintenance + warnings
    // 8) build RunResult
    unimplemented!()
}
```

```rust
// In python/materializer.rs
let outcome = crate::executor::pipeline::execute_pipeline(&spec, &ruleset, session_factory.inner()).await
    .map_err(|e| PyRuntimeError::new_err(format!("Execution failed: {e}")))?;
Ok(PyRunResult::from_run_result(&outcome.run_result))
```

### Target Files

| File | Action |
|---|---|
| `rust/codeanatomy_engine/src/executor/pipeline.rs` | **New** |
| `rust/codeanatomy_engine/src/executor/mod.rs` | **Edit** — export pipeline module |
| `rust/codeanatomy_engine/src/executor/runner.rs` | **Edit** — delegate orchestration to shared module |
| `rust/codeanatomy_engine/src/python/materializer.rs` | **Edit** — delegate orchestration to shared module |
| `rust/codeanatomy_engine/tests/end_to_end.rs` | **Edit** — parity assertions between native/Python entrypoints |

### Implementation Checklist

- [ ] Introduce shared pipeline orchestrator module.
- [ ] Migrate runner path to orchestrator.
- [ ] Migrate Python materializer path to orchestrator.
- [ ] Ensure one shared warning/provider/envelope sequence.
- [ ] Add parity tests for both entrypoints.

---

## 14) Cross-Scope Delivery Order

Phase-by-phase sequence:

1. **Phase A: Foundations**
   - Scope 1 (Unified planning-surface builder)
   - Scope 2 (Format/Table policy)
   - Scope 12 (Runtime profile coverage contract)

2. **Phase B: Determinism + Artifact Integrity**
   - Scope 3 (Planning manifest hash)
   - Scope 11 (Determinism capture order hardening)
   - Scope 4 (Artifact completeness)

3. **Phase C: Parameter + Pushdown Correctness**
   - Scope 8 (Typed parameter cutover)
   - Scope 6 (Pushdown contract modeling)

4. **Phase D: Observability + Testing**
   - Scope 5 (Offline optimizer lab)
   - Scope 9 (Shared test infrastructure)
   - Scope 10 (Warning propagation)

5. **Phase E: Advanced/Optional + Convergence**
   - Scope 7 (Optional Delta planner/codec seams)
   - Scope 13 (Pipeline orchestration unification)

Dependency notes:

- Scope 3 depends on Scope 1/2 output (manifest derives from planning surface).
- Scope 11 depends on Scope 3 and Scope 4 inputs (planning + provider hashes).
- Scope 13 should be executed after Scopes 4/10/11 to avoid rework.

---

## 15) Quality Gate

After implementation, run:

```bash
uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q
```

Rust-focused validation:

```bash
cd rust && cargo test -p codeanatomy-engine && cargo clippy -p codeanatomy-engine -- -D warnings
```

Compatibility checks:

```bash
cd rust && cargo test -p codeanatomy-engine --test compat_smoke
```

---

## 16) Completion Criteria

Implementation is complete for v1 scope when all are true:

- [x] Session construction is centralized through one planning-surface module.
- [x] Planning surface uses DataFusion 51-correct APIs (`with_expr_planners`, `with_function_factory`, etc.).
- [x] Format and table-option policy is explicit, deterministic, and fingerprinted end-to-end.
- [x] Envelope hash includes planning-surface and provider identity composition.
- [x] Plan bundles persist planning hash (non-placeholder) and non-empty provider identities when inputs exist.
- [x] Output materialization metadata and commit lineage are populated consistently.
- [x] Typed parameter mode is canonical; template mode is transitional and validated.
- [x] Offline optimizer lab is available with deterministic step traces.
- [x] Pushdown contract tests cover `Unsupported|Exact|Inexact` semantics.
- [x] Non-fatal execution/capture degradations surface as `RunResult.warnings`.
- [x] Runtime profile fields are fully mapped (`Applied` or `Reserved`) with CI guardrails.
- [x] Python and native execution paths share one full orchestration contract (including pre-pipeline phases).

---

## 17) Remaining Scope (Optional Follow-On, Prioritized)

No mandatory v1 items remain. Remaining scope below is optional follow-on hardening.

### Priority A: Release Cleanup + Safety

1. **Retire compatibility wrappers after migration window**
   - Remove legacy wrapper methods once all downstream callers use `build_session_state_*` APIs.
   - Keep one release-cycle deprecation period with migration notes.

2. **Finalize template-mode deprecation removal**
   - Remove `parameter_templates` path after transition period.
   - Keep typed-parameter path as the single canonical planning mode.

### Priority B: Observability + Diagnostics

3. **Add structured warning taxonomy**
   - Add warning codes/categories in addition to free-form text.
   - Emit warning counters for SLO and drift dashboards.

4. **Extend lineage payload richness**
   - Add optional run-level metadata fields (for example: release/build identifiers, ruleset/profile labels) to Delta commit metadata policy.
   - Maintain deterministic metadata ordering and compatibility defaults.

### Priority C: Determinism/Correctness Expansion

5. **Broaden pushdown probing coverage**
   - Add extraction/probing for additional transform shapes (joins, computed predicates, nested transforms) where predicates can be mapped safely.
   - Keep deterministic skip-with-warning behavior for unparsable expressions.

6. **Expand parity and drift baselines**
   - Add cross-language golden fixtures to complement existing parity assertions.
   - Add long-horizon determinism tests that compare artifact identity across repeated runs and environment-stable profiles.

7. **Add artifact-diagnostics tooling for codec capture**
   - Add optional diff/inspection tooling for `delta_codec_logical_bytes` and `delta_codec_physical_bytes` to improve debugging when codec capture is enabled.
