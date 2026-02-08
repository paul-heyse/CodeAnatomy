# DataFusion Tracing Maximalist Implementation Plan v1 (2026-02-08)

## 1. Summary

This plan implements a full-surface DataFusion tracing stack for CodeAnatomy, including:

1. Physical operator execution spans (`instrument_with_*_spans!`)
2. Rule/analyzer/logical/physical planning spans (`instrument_rules_with_*_spans!`)
3. Plan diff capture (`RuleInstrumentationOptions::with_plan_diff()`)
4. Real metric attachment (`datafusion.metrics.*`)
5. Optional bounded row previews (`datafusion.preview`)
6. End-to-end storage spans via `instrumented-object-store`
7. OpenTelemetry export pipeline with backpressure and sampling controls
8. Deterministic runtime control through spec-driven tracing config

Design intent: implement all capabilities now, keep them integrated behind one control plane, and allow "maximal" operation without future architecture rewrites.

## 2. Design Principles (from `datafusion-tracing.md`)

1. Keep `datafusion` and `datafusion-tracing` on the same major/minor line (lockstep compatibility).
2. Register physical instrumentation rule last so other optimizer rules never see wrapped nodes.
3. Use `otel.name` for operator/rule/phase naming, attributes for high-cardinality context.
4. Keep preview bounded and redacted; metrics always on for maximal observability mode.
5. Use phase-only vs full-rule tracing as an explicit runtime mode (both implemented).
6. Include object-store spans under query spans for full storage-to-operator visibility.
7. Configure OTel exporter, BSP queue, sampling, and collector batching as first-class runtime settings.

## 3. Scope Items (Maximal Implementation)

### Scope 1: Dependency and Feature Wiring

**Objective**
Add full tracing dependencies and feature-gated builds for `codeanatomy-engine`.

**Target files**
- `rust/Cargo.toml`
- `rust/codeanatomy_engine/Cargo.toml`

**Representative snippet**
```toml
# rust/Cargo.toml
[workspace.dependencies]
datafusion = { version = "51.0.0", default-features = false, features = ["parquet"] }
datafusion-tracing = "51.0.0"
instrumented-object-store = "0.1.2"
tracing = "0.1"
tracing-subscriber = "0.3"
tracing-opentelemetry = "0.31"
opentelemetry = "0.30"
opentelemetry_sdk = "0.30"
opentelemetry-otlp = "0.30"

# rust/codeanatomy_engine/Cargo.toml
[dependencies]
datafusion-tracing = { workspace = true, optional = true }
instrumented-object-store = { workspace = true, optional = true }
tracing = { workspace = true, optional = true }
tracing-subscriber = { workspace = true, optional = true }
tracing-opentelemetry = { workspace = true, optional = true }
opentelemetry = { workspace = true, optional = true }
opentelemetry_sdk = { workspace = true, optional = true }
opentelemetry-otlp = { workspace = true, optional = true }

[features]
tracing = [
  "dep:datafusion-tracing",
  "dep:instrumented-object-store",
  "dep:tracing",
  "dep:tracing-subscriber",
  "dep:tracing-opentelemetry",
  "dep:opentelemetry",
  "dep:opentelemetry_sdk",
  "dep:opentelemetry-otlp",
]
```

### Scope 2: Unified Runtime Tracing Control Plane

**Objective**
Move from loose booleans to a comprehensive tracing config schema in spec/runtime.

**Target files**
- `rust/codeanatomy_engine/src/spec/runtime.rs`
- `rust/codeanatomy_engine/src/spec/execution_spec.rs`
- `src/engine/spec_builder.py`

**Representative snippet**
```rust
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuleTraceMode {
    Disabled,
    PhaseOnly,
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TracingConfig {
    pub enabled: bool,
    pub record_metrics: bool,
    pub rule_mode: RuleTraceMode,
    pub plan_diff: bool,
    pub preview_limit: usize,
    pub preview_max_width: usize,
    pub preview_max_row_height: usize,
    pub preview_min_compacted_col_width: usize,
    pub instrument_object_store: bool,
    pub otlp_endpoint: Option<String>,
    pub otlp_protocol: Option<String>, // grpc, http/protobuf
    pub otel_service_name: Option<String>,
    pub otel_resource_attributes: std::collections::BTreeMap<String, String>,
}
```

### Scope 3: Tracing Runtime Bootstrap (OTel + Subscriber + Backpressure)

**Objective**
Create one bootstrap path for exporter + subscriber setup with batch/backpressure/sampling knobs.

**Target files**
- `rust/codeanatomy_engine/src/executor/tracing/bootstrap.rs` (new)
- `rust/codeanatomy_engine/src/executor/tracing/mod.rs` (new or convert from `tracing.rs`)

**Representative snippet**
```rust
pub fn init_otel_tracing(cfg: &TracingConfig) -> anyhow::Result<()> {
    if !cfg.enabled {
        return Ok(());
    }

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(cfg.otlp_endpoint.as_deref().unwrap_or("http://localhost:4317"))
        .build()?;

    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name(cfg.otel_service_name.as_deref().unwrap_or("codeanatomy_engine"))
        .build();

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(exporter) // BSP queue + batch handling
        .build();

    let tracer = provider.tracer("codeanatomy_engine");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    let subscriber = tracing_subscriber::registry().with(otel_layer);
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}
```

### Scope 4: Physical Plan Instrumentation Rule Integration

**Objective**
Install `instrument_with_*_spans!` as the last physical optimizer rule.

**Target files**
- `rust/codeanatomy_engine/src/session/factory.rs`
- `rust/codeanatomy_engine/src/executor/tracing/exec_instrumentation.rs` (new)

**Representative snippet**
```rust
use datafusion_tracing::{instrument_with_info_spans, InstrumentationOptions};
use tracing::field;

fn build_exec_rule(cfg: &TracingConfig) -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
    let opts = InstrumentationOptions::builder()
        .record_metrics(cfg.record_metrics)
        .preview_limit(cfg.preview_limit)
        .preview_fn(Arc::new(move |batch| {
            datafusion_tracing::pretty_format_compact_batch(
                batch,
                cfg.preview_max_width,
                cfg.preview_max_row_height,
                cfg.preview_min_compacted_col_width,
            )
            .map(|d| d.to_string())
        }))
        .add_custom_field("trace.profile", "maximal")
        .build();

    Arc::new(instrument_with_info_spans!(
        options: opts,
        trace.profile = field::Empty,
        trace.spec_hash = field::Empty,
        trace.rulepack = field::Empty
    ))
}

// SessionStateBuilder ordering requirement:
// .with_physical_optimizer_rules(existing_rules)
// .with_physical_optimizer_rule(exec_instrument_rule) // always last
```

### Scope 5: Rule/Planner Instrumentation Integration

**Objective**
Enable analyzer/logical/physical phase spans plus optional per-rule and diff spans.

**Target files**
- `rust/codeanatomy_engine/src/executor/tracing/rule_instrumentation.rs` (new)
- `rust/codeanatomy_engine/src/session/factory.rs`

**Representative snippet**
```rust
use datafusion_tracing::{instrument_rules_with_info_spans, RuleInstrumentationOptions};

fn wrap_rules(state: SessionState, cfg: &TracingConfig) -> SessionState {
    let mut options = match cfg.rule_mode {
        RuleTraceMode::Disabled => return state,
        RuleTraceMode::PhaseOnly => RuleInstrumentationOptions::phase_only(),
        RuleTraceMode::Full => RuleInstrumentationOptions::full(),
    };
    if cfg.plan_diff {
        options = options.with_plan_diff();
    }

    instrument_rules_with_info_spans!(
        options: options,
        state: state
    )
}
```

### Scope 6: Root Execution Span and Context Propagation

**Objective**
Guarantee a root run span and child span correlation for rule, exec, and object-store spans.

**Target files**
- `rust/codeanatomy_engine/src/executor/runner.rs`
- `rust/codeanatomy_engine/src/executor/tracing/context.rs` (new)

**Representative snippet**
```rust
let span = tracing::info_span!(
    "codeanatomy_engine.execute",
    spec_hash = %hex::encode(spec.spec_hash),
    envelope_hash = %hex::encode(envelope_hash),
    rulepack_fingerprint = %hex::encode(rulepack_fingerprint),
    profile = %"maximal"
);
let _guard = span.enter();

// compile + execute + materialize under one parent span
let output_plans = compiler.compile().await?;
let (results, physical_plans) = execute_and_materialize_with_plans(...).await?;
```

### Scope 7: Preview Redaction and Safe Formatting

**Objective**
Use bounded compact previews with opt-in redaction policies to avoid sensitive data leakage.

**Target files**
- `rust/codeanatomy_engine/src/executor/tracing/preview.rs` (new)
- `rust/codeanatomy_engine/src/spec/runtime.rs`

**Representative snippet**
```rust
pub fn redacting_preview_fn(cfg: &TracingConfig) -> Arc<datafusion_tracing::PreviewFn> {
    Arc::new(move |batch: &RecordBatch| {
        // optional: drop configured sensitive columns before formatting
        let safe_batch = redact_columns(batch, &["token", "secret", "password"])?;
        datafusion_tracing::pretty_format_compact_batch(
            &safe_batch,
            cfg.preview_max_width,
            cfg.preview_max_row_height,
            cfg.preview_min_compacted_col_width,
        )
        .map(|d| d.to_string())
    })
}
```

### Scope 8: Full Storage Tracing with `instrumented-object-store`

**Objective**
Trace Delta/object-store operations (get/list/put/etc.) as child spans under query execution.

**Target files**
- `rust/codeanatomy_engine/src/session/factory.rs`
- `rust/codeanatomy_engine/src/executor/tracing/object_store.rs` (new)

**Representative snippet**
```rust
use instrumented_object_store::instrument_object_store;
use object_store::ObjectStore;
use url::Url;

fn register_instrumented_store(
    ctx: &SessionContext,
    prefix: &str,
    store: Arc<dyn ObjectStore>,
) -> datafusion_common::Result<()> {
    let wrapped = instrument_object_store(store, "delta_store");
    ctx.register_object_store(&Url::parse(prefix)?, wrapped);
    Ok(())
}
```

### Scope 9: Metrics Contract and RunResult Surface

**Objective**
Expose the maximal metrics payload from traces + physical metrics in a stable schema.

**Target files**
- `rust/codeanatomy_engine/src/executor/metrics_collector.rs`
- `rust/codeanatomy_engine/src/executor/result.rs`
- `src/engine/facade.py`

**Representative snippet**
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceMetricsSummary {
    pub output_rows: u64,
    pub output_batches: u64,
    pub output_bytes: u64,
    pub elapsed_compute_nanos: u64,
    pub spill_file_count: u64,
    pub spilled_bytes: u64,
    pub spilled_rows: u64,
    pub selectivity: Option<f64>,
    pub operator_count: usize,
}
```

### Scope 10: Configurable Sampling and Export Throughput Policy

**Objective**
Implement explicit head-sampling and BSP/collector tuning policy in runtime config and deployment docs.

**Target files**
- `rust/codeanatomy_engine/src/executor/tracing/bootstrap.rs`
- `docs/plans/...` rollout docs
- `.github/workflows/wheels.yml` (env smoke coverage)

**Representative snippet**
```rust
pub struct ExportPolicy {
    pub traces_sampler: String, // parentbased_always_on, parentbased_traceidratio
    pub traces_sampler_arg: Option<String>,
    pub bsp_max_queue_size: usize,
    pub bsp_max_export_batch_size: usize,
    pub bsp_schedule_delay_ms: u64,
    pub bsp_export_timeout_ms: u64,
}
```

### Scope 11: CI and Drift Controls

**Objective**
Add tests to prevent regression in rule ordering, preview behavior, and span shape.

**Target files**
- `rust/codeanatomy_engine/tests/tracing_integration.rs` (new)
- `rust/codeanatomy_engine/tests/object_store_tracing.rs` (new)
- `rust/codeanatomy_engine/tests/rule_tracing_diff.rs` (new)
- `.github/workflows/check_drift_surfaces.yml`

**Representative snippet**
```rust
#[tokio::test]
async fn tracing_rule_is_last_in_physical_optimizer_chain() {
    let (ctx, _) = build_test_session_with_tracing().await?;
    let rules = ctx.state().physical_optimizers();
    assert!(rules.last().unwrap().name().contains("instrument"));
    Ok(())
}
```

### Scope 12: Rollout and Operational Profiles

**Objective**
Ship one maximal profile plus lower-noise profiles without re-architecture.

**Target files**
- `rust/codeanatomy_engine/src/spec/runtime.rs`
- `src/engine/spec_builder.py`
- `docs/plans/programmatic_architecture_remaining_scope_2026-02-08.md` (status links)

**Representative snippet**
```rust
pub enum TracingPreset {
    Maximal,        // full rules + plan diff + metrics + previews + object store
    MaximalNoData,  // full rules + plan diff + metrics, preview_limit=0
    ProductionLean, // phase_only + metrics + object store, no diffs/previews
}
```

## 4. Proposed File Layout

```text
rust/codeanatomy_engine/src/
  executor/
    tracing/
      mod.rs
      bootstrap.rs
      config.rs
      context.rs
      exec_instrumentation.rs
      rule_instrumentation.rs
      preview.rs
      object_store.rs
  session/
    factory.rs                     (modified)
  spec/
    runtime.rs                     (modified)
    execution_spec.rs              (modified if new nested tracing config is added)
  executor/
    runner.rs                      (modified)
    result.rs                      (modified)
    metrics_collector.rs           (modified)

src/engine/
  spec_builder.py                  (modified)
  facade.py                        (optional error/reporting mapping extension)

rust/codeanatomy_engine/tests/
  tracing_integration.rs           (new)
  object_store_tracing.rs          (new)
  rule_tracing_diff.rs             (new)
  tracing_preview_redaction.rs     (new)
```

## 5. Implementation Checklist

### Phase A: Core enablement
- [ ] Add lockstep tracing dependencies and feature flags.
- [ ] Implement unified `TracingConfig` contract in Rust and Python mirror.
- [ ] Add bootstrap path for OTel exporter + subscriber initialization.

### Phase B: DataFusion instrumentation wiring
- [ ] Implement execution instrumentation rule construction from `TracingConfig`.
- [ ] Guarantee physical instrumentation rule is appended last.
- [ ] Implement rule-phase wrapper (`full` / `phase_only` + optional `plan_diff`).
- [ ] Wire root execution span around compile+execute lifecycle.

### Phase C: Maximal observability signals
- [ ] Implement bounded/redacted preview formatter path.
- [ ] Wire full metrics collection into `RunResult`.
- [ ] Register instrumented object store in `SessionContext`.
- [ ] Add stable custom field policy (declared placeholder keys + recorded values).

### Phase D: Operational hardening
- [ ] Add exporter queue/schedule/timeout/sampling controls and defaults.
- [ ] Add collector deployment guidance (memory_limiter -> sampling -> batch order).
- [ ] Add CI assertions for instrumentation ordering and span schema drift.

### Phase E: Validation
- [ ] Validate phase spans appear: `analyze_logical_plan`, `optimize_logical_plan`, `optimize_physical_plan`.
- [ ] Validate operator spans use `otel.name` = physical operator names.
- [ ] Validate `datafusion.metrics.*` attributes for representative scan/join/aggregate/sort plans.
- [ ] Validate preview behavior (`preview_limit=0` off, bounded format on).
- [ ] Validate object store spans (`get`, `get_range`, `list`, `put`) nest under query spans.
- [ ] Validate no instrumentation holes when custom physical rules are present.

## 6. Acceptance Criteria

1. All tracing capabilities in `docs/python_library_reference/datafusion-tracing.md` are represented in code paths or explicit config toggles.
2. Session builds support both maximal and lean presets without code changes.
3. Rule and execution traces are correlated under one root run span with stable identity attributes.
4. Storage spans are visible for Delta/object-store operations in the same trace.
5. Trace volume/perf controls are explicit, tested, and documented (sampling + BSP + collector batch).

## 7. Recommended Initial Defaults

For your "maximal by design" target, start with:

1. `enabled=true`
2. `record_metrics=true`
3. `rule_mode=Full`
4. `plan_diff=true`
5. `preview_limit=5` with compact formatter and redaction
6. `instrument_object_store=true`
7. `OTEL_TRACES_SAMPLER=parentbased_always_on` in preprod, then move to ratio-based in production as volume dictates

This gives full coverage from planner to execution to storage, with bounded preview payloads and explicit trace-volume controls.
