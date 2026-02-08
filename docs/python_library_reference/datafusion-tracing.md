## DataFusion Tracing (Rust) — feature category catalog (sections to deep-dive)

> **What it is:** a community extension that instruments **Apache DataFusion** with **`tracing` spans + OpenTelemetry export**, capturing **execution metrics**, **execution-step spans**, and **partial-result previews** (not an ASF release). ([Docs.rs][1])

---

### 0) Scope, versioning, and “where it hooks”

* **Version coupling:** crate is published in lockstep with a DataFusion version (e.g., `datafusion = "52.0.0"`, `datafusion-tracing = "52.0.0"`). ([Docs.rs][1])
* **Hook boundary:** all instrumentation is introduced via **DataFusion extension points**:

  * **Physical optimizer rule** that wraps an `ExecutionPlan` tree in tracing spans. ([Docs.rs][2])
  * **SessionState rule-chain wrapping** (analyzer/logical optimizer/physical optimizer phases). ([Docs.rs][3])

---

### 1) Public API surface inventory (crate-level “what you can call”)

**Macros — ExecutionPlan instrumentation**

* `instrument_with_spans!` + level-specific wrappers: `instrument_with_trace_spans!`, `..._debug_...`, `..._info_...`, `..._warn_...`, `..._error_...` ([Docs.rs][1])

**Macros — Rule-phase instrumentation**

* `instrument_rules_with_spans!` + level-specific wrappers: `instrument_rules_with_trace_spans!`, `..._debug_...`, `..._info_...`, `..._warn_...`, `..._error_...` ([Docs.rs][1])

**Structs**

* `InstrumentationOptions` (exec instrumentation config) ([Docs.rs][4])
* `RuleInstrumentationOptions` (rule-phase instrumentation config) ([Docs.rs][5])

**Functions**

* `pretty_format_compact_batch(batch, max_width, max_row_height, min_compacted_col_width)` (compact batch preview formatter) ([Docs.rs][6])

---

### 2) ExecutionPlan-level tracing (the “runtime spans” layer)

* **What it does:** creates a **PhysicalOptimizerRule** that wraps *each physical node* in an instrumented wrapper (internally `InstrumentedExec`) to emit spans and metrics. ([Docs.rs][2])
* **Deep dive topics:**

  * span naming + hierarchy per physical operator
  * how wrappers preserve `ExecutionPlan` behavior (child replacement, repartitioning, etc.)
  * what fields are emitted by default vs user-provided fields

---

### 3) `InstrumentationOptions` (execution instrumentation control plane)

* **Fields to deep-dive (exact knobs):**

  * `record_metrics: bool` (enable/disable DataFusion metrics recording) ([Docs.rs][4])
  * `preview_limit: usize` (rows per span preview; `0` disables preview) ([Docs.rs][4])
  * `preview_fn: Option<Arc<Fn(&RecordBatch)->Result<String,ArrowError>>>` (custom batch-to-string) ([Docs.rs][4])
  * `custom_fields: HashMap<String,String>` (static tag set applied to spans) ([Docs.rs][4])
* **Builder surface:** `.builder() … .build()` (document the builder methods as the primary UX). ([Docs.rs][4])

---

### 4) Metrics capture: “DataFusion metrics → span fields”

* **What’s captured:** “native DataFusion metrics such as execution time and output row count” (and more), emitted as span fields. ([Docs.rs][1])
* **Deep dive topics:**

  * metrics naming scheme / prefixing (how fields are labeled on spans)
  * which metrics exist per operator class (scan/join/agg/sort/shuffle/etc.)
  * how metric collection overhead scales (and how `record_metrics` gates it)

---

### 5) Partial result preview subsystem (debuggability without full materialization)

* **Feature:** per-span “preview” of RecordBatches (bounded by `preview_limit`) with optional custom formatting. ([Docs.rs][2])
* **Deep dive topics:**

  * preview sampling strategy (rows per batch, per operator span)
  * safe formatting defaults vs `pretty_format_compact_batch`
  * output truncation rules (width/height constraints; column dropping) ([Docs.rs][6])

---

### 6) Rule-phase tracing (the “planning pipeline spans” layer)

* **What it does:** wraps DataFusion **analyzer**, **logical optimizer**, and **physical optimizer** rules with spans, grouped under phase spans (`analyze_logical_plan`, `optimize_logical_plan`, `optimize_physical_plan`). ([Docs.rs][3])
* **Deep dive topics:**

  * per-rule span naming + rule ordering visibility
  * correlation of rule spans with plan artifacts (before/after)

---

### 7) Physical plan creation tracing (planner instrumentation)

* **Behavior:** when physical-optimizer instrumentation is enabled, **physical plan creation** is also traced automatically. ([Docs.rs][3])
* **Deep dive topics:**

  * which planner stages are spanned
  * how to correlate plan-build spans with the eventual physical operator tree

---

### 8) `RuleInstrumentationOptions` (rule instrumentation verbosity + diffs)

* **Canonical presets:**

  * `RuleInstrumentationOptions::full()` (all phases enabled at “Full” level) ([Docs.rs][5])
  * `RuleInstrumentationOptions::phase_only()` (phase spans only; suppress per-rule spans to reduce verbosity) ([Docs.rs][5])
  * `.with_plan_diff()` (emit plan diffs as part of rule instrumentation) ([Docs.rs][5])
* **Deep dive topics:**

  * “Full” vs “PhaseOnly” tradeoffs (cardinality + overhead)
  * diff format, diff anchoring (what constitutes “plan equality”)

---

### 9) OpenTelemetry export + collector wiring (operational integration)

* **Baseline:** you “initialize tracing subscriber as usual” and export via OpenTelemetry; project docs call out Jaeger, Datadog, and generic OTel collectors. ([Docs.rs][1])
* **Deep dive topics:**

  * OTLP endpoint/protocol choices and batching
  * resource/service attributes strategy
  * exporter backpressure and sampling policies (to control trace volume)

---

### 10) End-to-end visibility to storage (ObjectStore tracing)

* **Companion crate:** `instrumented-object-store` wraps an `ObjectStore` to emit spans for **get/put/list/etc.**, with path/size metadata and error capture; can be registered into DataFusion via `SessionContext.register_object_store(...)`. ([Docs.rs][7])
* **Deep dive topics:**

  * span naming conventions (store prefix + op name)
  * correlating storage spans under query/operator spans (context propagation)

---

### 11) Composition rules + “don’t break other optimizer rules”

* **Rule ordering requirement:** if you have other physical optimizer rules, register the instrumentation rule **last** so it observes the final plan and doesn’t leak wrapped nodes to other rules. ([GitHub][8])
* **Wrapper transparency:** `InstrumentedExec` is intended to be “mostly invisible”; it delegates `as_any()` to the inner plan, and newly created nodes remain wrapped automatically. ([GitHub][8])
* **Deep dive topics:**

  * interaction with `ExecutionPlan` methods (`with_new_children`, `repartitioned`, etc.)
  * diagnostics patterns that depend on downcasting

---

### 12) Workspace components, examples, and non-production helpers

* **Repo layout categories:** core crate, object-store instrumentation, integration utilities (explicitly “not for production use”), examples, tests. ([GitHub][8])
* **Deep dive topics:**

  * example configurations (OTLP exporter, collector targets)
  * integration test harness + datasets (TPCH generation workflow)

---

If you want the next step to be maximally “cyclopts.md-like,” the natural deep-dive order is: **(1) ExecutionPlan instrumentation → (3) InstrumentationOptions → (4) metrics field schema → (5) previews → (6–8) rule + plan-diff instrumentation → (9–10) OTEL + storage end-to-end → (11) composition footguns**.

[1]: https://docs.rs/datafusion-tracing "datafusion_tracing - Rust"
[2]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/macro.instrument_with_spans.html "instrument_with_spans in datafusion_tracing - Rust"
[3]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/macro.instrument_rules_with_spans.html "instrument_rules_with_spans in datafusion_tracing - Rust"
[4]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/struct.InstrumentationOptions.html "InstrumentationOptions in datafusion_tracing - Rust"
[5]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/struct.RuleInstrumentationOptions.html "RuleInstrumentationOptions in datafusion_tracing - Rust"
[6]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/fn.pretty_format_compact_batch.html "pretty_format_compact_batch in datafusion_tracing - Rust"
[7]: https://docs.rs/instrumented-object-store "instrumented_object_store - Rust"
[8]: https://github.com/datafusion-contrib/datafusion-tracing "GitHub - datafusion-contrib/datafusion-tracing: Integration of opentelemetry with the tracing crate"

### 0) Scope, versioning, and where it hooks

#### 0.1 What “DataFusion Tracing” is (scope boundary)

* **Non-ASF extension crate** that instruments **DataFusion query lifecycle** using `tracing` + OpenTelemetry export (you bring your own subscriber/exporter). ([Docs.rs][1])
* Intended outcomes (high-level): **execution-step spans**, **native DataFusion metrics attached to spans**, and optional **partial-result previews** for debugging. ([Docs.rs][1])

---

#### 0.2 Version coupling (Cargo semantics that matter)

**Observed coupling pattern (practical, not philosophical):**

* The crate docs show the canonical pairing style in `Cargo.toml`:

  ```toml
  [dependencies]
  datafusion = "52.0.0"
  datafusion-tracing = "52.0.0"
  ```

  ([Docs.rs][1])
* Internally, `datafusion-tracing` depends on DataFusion with a caret constraint (e.g., `datafusion ^52.0.0`), so **it’s built around a specific DataFusion major series** and expects DataFusion’s public traits/types for that series. ([Docs.rs][2])

**Value proposition**

* You get a stable “known-good” integration surface per DataFusion release line; most breakage becomes **compile-time** (trait/type drift), not runtime mystery.

**Watchouts**

* **Don’t mix majors** (e.g., `datafusion-tracing = 52.*` with `datafusion = 53.*`): physical optimizer traits / plan node types are not guaranteed compatible. The crate is explicitly designed as a DataFusion integration layer. ([Docs.rs][2])
* If you follow the docs’ “same version number” pattern, you avoid subtle “works but missing fields” situations caused by metrics / planner evolution.

---

#### 0.3 Hook boundary #1 — Physical optimizer rule that wraps the `ExecutionPlan` tree

**Mechanism**

* `instrument_with_spans!` (and the level wrappers) **construct a `PhysicalOptimizerRule`** that wraps each node in the physical plan with an internal `InstrumentedExec`. ([Docs.rs][2])
* This leverages DataFusion’s physical optimizer interface: `PhysicalOptimizerRule` is defined as a transformation from one `ExecutionPlan` to another equivalent plan. ([Docs.rs][3])

**Installation / syntax (SessionStateBuilder)**

* DataFusion exposes `SessionStateBuilder::with_physical_optimizer_rule(...)` which **adds a rule to the end** of the physical optimizer list. ([Docs.rs][4])
* Canonical usage from the crate docs:

  ```rust
  let instrument_rule = instrument_with_info_spans!(options: exec_options, env = field::Empty, region = field::Empty);

  let session_state = SessionStateBuilder::new()
      .with_default_features()
      .with_physical_optimizer_rule(instrument_rule)
      .build();
  ```

  ([Docs.rs][1])

**Value proposition**

* Physical plan spans line up with *actual* execution operators (scans/joins/aggregates/repartitions), so you can attribute latency + row counts to operator boundaries without custom executors.

**Key watchouts**

* **Register it last** if you also have your own physical optimizer rules. Otherwise other rules may see instrumented nodes rather than canonical nodes. The project calls this out explicitly and shows both chaining and vector forms:

  ```rust
  builder.with_physical_optimizer_rule(rule_a)
      .with_physical_optimizer_rule(rule_b)
      .with_physical_optimizer_rule(instrument_rule)
  // or
  builder.with_physical_optimizer_rules(vec![..., instrument_rule])
  ```

  ([GitHub][5])
* The wrapper `InstrumentedExec` is intentionally not part of the public contract; the supported surface is “use the optimizer rule.” For rare downcast/inspection needs, the project notes `InstrumentedExec` delegates `as_any()` to the inner plan. ([GitHub][5])

---

#### 0.4 Hook boundary #2 — SessionState rule-chain wrapping (analyzer / logical optimizer / physical optimizer phases)

**Mechanism**

* `instrument_rules_with_spans!` **instruments a `SessionState`** by wrapping analyzer, logical optimizer, and physical optimizer rules with spans, grouped under phase spans:

  * `analyze_logical_plan`
  * `optimize_logical_plan`
  * `optimize_physical_plan` ([Docs.rs][6])
* When physical optimizer instrumentation is enabled, it also instruments **physical plan creation** (planner) automatically. ([Docs.rs][6])

**Installation / syntax**

```rust
let rule_options = RuleInstrumentationOptions::full().with_plan_diff();
let session_state = instrument_rules_with_info_spans!(
    options: rule_options,
    state: session_state
);
```

([Docs.rs][1])

**Value proposition**

* Lets you see “planning time” as a first-class latency budget, and attribute it down to “which rule(s) blew up the plan / time.”

**Key watchouts**

* If you enable verbose rule spans + plan diffs, trace volume grows quickly on complex queries (rule count × passes). Use `phase_only()` when you want phase timing without per-rule spans. ([Docs.rs][7])

---

### 1) Public API surface inventory (what you actually call)

#### 1.1 ExecutionPlan instrumentation macros

##### `instrument_with_spans!` (core)

**What it returns**

* A **`PhysicalOptimizerRule`** that instruments an `ExecutionPlan` by wrapping each node in `InstrumentedExec`. ([Docs.rs][2])

**Syntax (macro forms)**

```rust
instrument_with_spans!(target: $target, $lvl, options: $options, $($fields)*)
instrument_with_spans!(target: $target, $lvl, options: $options)
instrument_with_spans!($lvl, options: $options, $($fields)*)
instrument_with_spans!($lvl, options: $options)
```

([Docs.rs][2])

**Span field syntax**

* Explicitly modeled after `tracing::span!` field/value syntax (you can pass extra fields like `foo = 42`, `bar = "x"`, or declare placeholders using `field::Empty`). ([Docs.rs][2])

**Default behavior**

* “Includes all known metrics fields relevant to DataFusion execution” by default. ([Docs.rs][2])
* Preview formatting: if you don’t supply `preview_fn`, it uses Arrow’s pretty formatting (mentioned in the macro docs). ([Docs.rs][2])

**Canonical example (fields + custom_fields map)**

```rust
let instrument_rule = instrument_with_spans!(
    Level::INFO,
    options: options,
    datafusion.additional_info = "some info",
    datafusion.user_id = 42,
    custom.key1 = field::Empty,
    custom.key2 = field::Empty,
);
```

([Docs.rs][2])

**Value proposition**

* Fastest path to “operator-level distributed traces” with **no plan/executor rewrites**: one optimizer rule insertion.

**Key watchouts**

* Field explosion is real: “all known metrics fields” + your own fields can generate large span payloads (costly exporters, cardinality surprises). ([Docs.rs][2])
* If you enable previews, you are effectively logging data content; treat it like logs (PII + secrets + volume). The options explicitly support disabling previews via `preview_limit = 0`. ([Docs.rs][2])

---

##### Level-specific convenience wrappers (`instrument_with_trace_spans!`, `..._debug_...`, `..._info_...`, `..._warn_...`, `..._error_...`)

**What they do**

* They’re wrappers around `instrument_with_spans!` that set the tracing level for you (e.g., `TRACE` / `INFO`). ([Docs.rs][8])

**Syntax (example: TRACE wrapper)**

```rust
let instrument_rule =
    instrument_with_trace_spans!(options: InstrumentationOptions::default());
```

([Docs.rs][8])

**Key watchouts**

* If your subscriber filter drops that level/target, you’ll see “nothing happened” (spans exist but aren’t recorded). These macros only *create* spans; they don’t configure subscribers/exporters. ([Docs.rs][1])

---

#### 1.2 Rule-phase instrumentation macros

##### `instrument_rules_with_spans!` (core)

**What it does**

* Instruments `SessionState` rule phases (analyzer/logical optimizer/physical optimizer) with grouped phase spans; can also instrument physical plan creation when physical optimizer instrumentation is enabled. ([Docs.rs][6])

**Syntax**

```rust
instrument_rules_with_spans!(
    $level,
    options: $options,
    state: $session_state,
    $($fields)*
)
```

([Docs.rs][6])

**Value proposition**

* Turns opaque optimization time into attributable spans (phase and optionally per-rule).

**Key watchouts**

* Combining per-rule spans + plan diffs is *very* verbose; prefer `RuleInstrumentationOptions::phase_only()` in “always-on” environments. ([Docs.rs][7])

##### Level-specific wrappers (`instrument_rules_with_trace_spans!`, `..._debug_...`, `..._info_...`, `..._warn_...`, `..._error_...`)

* Same rationale as the plan instrumentation wrappers: choose a default tracing level without spelling it every time. ([Docs.rs][1])

---

#### 1.3 Struct: `InstrumentationOptions` (execution instrumentation config)

**Type + fields**

```rust
pub struct InstrumentationOptions {
  pub record_metrics: bool,
  pub preview_limit: usize,
  pub preview_fn: Option<Arc<dyn Fn(&RecordBatch)->Result<String, ArrowError> + Send + Sync>>,
  pub custom_fields: HashMap<String, String>,
}
```

([Docs.rs][9])

**Semantics**

* `record_metrics`: gate metrics collection. ([Docs.rs][9])
* `preview_limit`: max rows per span preview; `0` disables previews. ([Docs.rs][9])
* `preview_fn`: optional formatter called for each previewed batch. ([Docs.rs][9])
* `custom_fields`: user key/value metadata. ([Docs.rs][9])

**Builder usage (canonical)**

```rust
let exec_options = InstrumentationOptions::builder()
    .record_metrics(true)
    .preview_limit(5)
    .preview_fn(Arc::new(|batch: &RecordBatch| {
        pretty_format_compact_batch(batch, 64, 3, 10).map(|fmt| fmt.to_string())
    }))
    .add_custom_field("env", "production")
    .add_custom_field("region", "us-west")
    .build();
```

([Docs.rs][1])

**Key watchouts**

* `preview_fn` runs during execution; make it **cheap + non-panicking** (formatting can become dominant cost if your spans preview frequently). ([Docs.rs][9])

---

#### 1.4 Struct: `RuleInstrumentationOptions` (rule-phase config)

**Primary constructors / toggles**

* `RuleInstrumentationOptions::full()` → all phases, “Full” level. ([Docs.rs][7])
* `RuleInstrumentationOptions::phase_only()` → phase spans without individual rule spans. ([Docs.rs][7])
* `.with_plan_diff()` → enable plan diff emission. ([Docs.rs][7])

**Key watchouts**

* Plan diffs are high-signal but can blow up trace size; treat as “debug mode”, not default. ([Docs.rs][7])

---

#### 1.5 Function: `pretty_format_compact_batch(...)` (compact preview formatter)

**Signature**

```rust
pub fn pretty_format_compact_batch(
    batch: &RecordBatch,
    max_width: usize,
    max_row_height: usize,
    min_compacted_col_width: usize,
) -> Result<impl Display, ArrowError>
```

([Docs.rs][10])

**Behavior (from implementation)**

* Produces an ASCII table with:

  * optional **table width constraint** (`max_width == 0` means “no constraint”) and **column dropping** when width can’t fit. ([Docs.rs][11])
  * optional **row height constraint** (`max_row_height > 0` applies `row.max_height(max_row_height)`). ([Docs.rs][11])
  * “compaction” heuristic where per-column width is bounded (uses `width.min(min_compacted_col_width).max(4)` for fit calculations). ([Docs.rs][11])

**Key watchouts**

* With narrow `max_width`, it may **drop columns** (great for trace payload control, risky if you assume “all fields visible”). ([Docs.rs][10])
* Formatting is work; don’t combine high `preview_limit` + expensive formatting in hot paths unless you’re intentionally profiling. ([Docs.rs][1])

[1]: https://docs.rs/datafusion-tracing "datafusion_tracing - Rust"
[2]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/macro.instrument_with_spans.html "instrument_with_spans in datafusion_tracing - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/index.html "datafusion::physical_optimizer - Rust"
[4]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionStateBuilder.html "SessionStateBuilder in datafusion::execution::session_state - Rust"
[5]: https://github.com/datafusion-contrib/datafusion-tracing "GitHub - datafusion-contrib/datafusion-tracing: Integration of opentelemetry with the tracing crate"
[6]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/macro.instrument_rules_with_spans.html "instrument_rules_with_spans in datafusion_tracing - Rust"
[7]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/struct.RuleInstrumentationOptions.html "RuleInstrumentationOptions in datafusion_tracing - Rust"
[8]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/macro.instrument_with_trace_spans.html "instrument_with_trace_spans in datafusion_tracing - Rust"
[9]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/struct.InstrumentationOptions.html "InstrumentationOptions in datafusion_tracing - Rust"
[10]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/fn.pretty_format_compact_batch.html "pretty_format_compact_batch in datafusion_tracing - Rust"
[11]: https://docs.rs/datafusion-tracing/latest/src/datafusion_tracing/preview_utils.rs.html "preview_utils.rs - source"

## 2) ExecutionPlan-level tracing (runtime spans layer)

### 2.1 Hook-in mechanism (what actually gets inserted)

**Conceptual unit:** a `PhysicalOptimizerRule` (`InstrumentRule`) that traverses the physical plan and wraps every node in an `InstrumentedExec` wrapper. ([Docs.rs][1])

**Rule construction**

* The `instrument_with_*_spans!` macro family builds the rule by producing a `SpanCreateFn` closure that creates a `tracing::span!(...)` with a fixed Rust span name `"InstrumentedExec"` plus a fixed set of fields (including `otel.name`, `datafusion.node`, and the `datafusion.metrics.*` namespace). ([Docs.rs][2])
* `new_instrument_rule(...)` registers a DataFusion runtime **JoinSet tracer** once (via `INIT.call_once`) so **spawned tasks inherit the current tracing context** (critical for async operator internals). ([Docs.rs][1])

**Plan traversal / wrapping**

* `optimize(...)` uses `plan.transform_down(...)` to ensure:

  * every node is wrapped (top-down),
  * and **new nodes created by earlier physical optimizer rules are also wrapped** in the same pass. ([Docs.rs][1])
* Double-wrapping is avoided by checking `InstrumentedExec::is_instrumented(...)` and returning `Transformed::no(plan)` when already wrapped. ([Docs.rs][1])

**Watchouts**

* This is **physical-optimizer-time** wrapping, not planner-time: the wrapper is inserted into the final (or near-final) physical plan tree that DataFusion will execute. ([Docs.rs][1])
* Register the rule **last** in your physical optimizer chain so other rules never see `InstrumentedExec` nodes (the project explicitly calls this out). ([GitHub][3])

---

### 2.2 Span naming (what shows up in Jaeger/Datadog/etc.)

#### 2.2.1 “Static tracing span name” vs “OTel exported span name”

At creation-time, the macro creates a `tracing::span!` whose Rust span name is literally `"InstrumentedExec"` and declares `otel.name` as an empty field (to be filled later). ([Docs.rs][2])

At runtime, `InstrumentedExec::create_populated_span()` records:

* `otel.name = self.inner.name()` (the operator’s `ExecutionPlan::name()`), and
* `datafusion.partitioning`, `datafusion.emission_type`, `datafusion.boundedness` from `PlanProperties`. ([Docs.rs][4])

**Why `otel.name` matters**

* `tracing-opentelemetry` treats `otel.name` as a **special field that overrides the exported OpenTelemetry span name**, specifically intended for non-static span names. ([Docs.rs][5])

**Resulting behavior (practical)**

* In your trace backend, span names typically appear as **the physical operator names** (whatever `ExecutionPlan::name()` returns for each node), not `"InstrumentedExec"`. ([Docs.rs][4])

**Watchouts**

* You don’t get full control over exported span names via macro fields because `create_populated_span()` always records `otel.name` from `inner.name()` at first span initialization. ([Docs.rs][4])
* If your backend enforces strict naming conventions, treat span *attributes* (the macro fields + `datafusion.*`) as the customization surface, not the name. ([Docs.rs][2])

---

### 2.3 Span hierarchy (parent/child structure per physical operator)

#### 2.3.1 Why the spans form an operator tree

Every physical node is wrapped. During execution:

* the wrapper obtains (or lazily initializes) a span (`get_span()`),
* executes the inner plan within `span.in_scope(|| ...)`, and
* returns a stream that is `.instrument(span)`’d so polling the stream occurs within the span context. ([Docs.rs][4])

Because `inner.execute(...)` happens while the parent span is in scope, when the child node’s wrapper runs, its span is created under the current span → **nested trace tree that mirrors the physical plan structure**. ([Docs.rs][4])

#### 2.3.2 Async propagation: spawned tasks keep the right parent span

DataFusion operators can spawn async tasks / blocking work. The instrumentation rule registers a `JoinSetTracer` that wraps:

* futures with `.in_current_span()`
* blocking closures with `Span::current().in_scope(...)` ([Docs.rs][1])

This is the difference between “spans exist but don’t nest correctly” vs “operator internals remain causally attached.”

**Watchouts**

* The JoinSet tracer registration is **global** (done once via `INIT.call_once`). If your process already configures a different join-set tracer, this may fail and emit a warn. ([Docs.rs][1])
* If your subscriber filters out the macro’s chosen level, you can end up with “no trace tree” even though the plan is wrapped. (Rule creates spans; your subscriber decides if they’re recorded/exported.) ([Docs.rs][2])

---

### 2.4 What each operator span records (and *when*)

#### 2.4.1 Immediately at first span init (cheap, deterministic)

`create_populated_span()` records:

* `otel.name` (operator name)
* `datafusion.partitioning`
* `datafusion.emission_type`
* `datafusion.boundedness` ([Docs.rs][4])

#### 2.4.2 On completion (requires “all partitions finished”)

The wrapper intentionally records certain fields only after all partition streams complete, via “keep-alive recorders” stored behind `OnceLock<Arc<...>>` and dropped after the last stream finishes.

* **`datafusion.node`**: recorded in `NodeRecorder::drop` using `DefaultDisplay(execution_plan)` → default `DisplayAs(DisplayFormatType::Default)` formatting of the execution plan. ([Docs.rs][6])
* **`datafusion.metrics.*`**: recorded in `MetricsRecorder::drop`, using `metrics.aggregate_by_name()` and `span.record("datafusion.metrics.<name>", …)` for every aggregated metric. ([Docs.rs][7])

**Partition semantics (non-obvious but critical)**

* The span is **shared across all partitions**; metrics are aggregated across partitions “before being reported.” ([Docs.rs][4])
* Preview (if enabled) applies a **global** `preview_limit` across partitions, not per-partition. ([Docs.rs][4])

**Watchouts**

* If you want per-partition spans/metrics, this design doesn’t give you that directly (you get per-operator spans with cross-partition aggregation). ([Docs.rs][4])
* `datafusion.node` can be large (full default-format node display). Many backends impose attribute size limits; expect truncation or dropped attributes in some collectors/backends. ([Docs.rs][6])

---

### 2.5 Wrapper transparency (preserving `ExecutionPlan` behavior)

#### 2.5.1 “Delegate everything” baseline

`InstrumentedExec` implements `ExecutionPlan` by delegating most methods straight to the inner plan (`delegate! { to self.inner { ... } }`), including:

* `schema`, `properties`, `name`, `children`, `metrics`, statistics APIs, ordering/distribution requirements, pushdown-related hooks, etc. ([Docs.rs][4])

This is why the wrapper can be inserted without breaking downstream behaviors that rely on core plan semantics.

#### 2.5.2 Rewrap on plan-returning APIs (child replacement, repartitioning, pushdowns)

Methods that can return a modified plan are explicitly overridden to:

1. call the inner method,
2. wrap the resulting plan in a new `InstrumentedExec` via `with_new_inner(...)` (preserving instrumentation configuration: metrics on/off, preview config, and span creation function). ([Docs.rs][4])

Concrete overridden methods (the “don’t lose instrumentation” set):

* `repartitioned(...) -> Result<Option<Arc<dyn ExecutionPlan>>>` ([Docs.rs][4])
* `with_fetch(...) -> Option<Arc<dyn ExecutionPlan>>` ([Docs.rs][4])
* `try_swapping_with_projection(...) -> Result<Option<Arc<dyn ExecutionPlan>>>` ([Docs.rs][4])
* `handle_child_pushdown_result(...) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>>` (rewraps `updated_node`) ([Docs.rs][4])
* `try_pushdown_sort(...) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>>` (rewraps `Exact` / `Inexact`) ([Docs.rs][4])
* `with_new_children(...) -> Result<Arc<dyn ExecutionPlan>>` ([Docs.rs][4])
* `reset_state(...) -> Result<Arc<dyn ExecutionPlan>>` ([Docs.rs][4])
* `with_new_state(...) -> Option<Arc<dyn ExecutionPlan>>` ([Docs.rs][4])

#### 2.5.3 Downcasting + “internal optimizer visibility”

`as_any()` behavior is intentionally split:

* **During optimization passes** (guard active), `as_any()` returns `self` so the optimizer can detect already-instrumented nodes. ([Docs.rs][4])
* Otherwise, it delegates to `inner.as_any()` for “transparent downcasting” (callers can downcast to the original node type without dealing with the wrapper). ([Docs.rs][4])

**Watchouts**

* If you have tooling that expects wrapper types to be discoverable via `as_any()` outside optimizer context, that’s intentionally blocked (wrapper aims to be observational and private by design). ([GitHub][3])

---

### 2.6 Minimal “runtime spans enabled” wiring (syntax refresher)

```rust
let exec_options = InstrumentationOptions::builder()
    .record_metrics(true)
    .preview_limit(0) // keep runtime spans + metrics, skip data previews
    .build();

let instrument_rule = instrument_with_info_spans!(
    options: exec_options,
    env = tracing::field::Empty,
);

let session_state = SessionStateBuilder::new()
    .with_default_features()
    .with_physical_optimizer_rule(instrument_rule) // put last if you add other rules
    .build();
```

Macro → rule creation semantics and fields: ([Docs.rs][2])
“Register last” operational guidance: ([GitHub][3])

[1]: https://docs.rs/crate/datafusion-tracing/latest/source/src/exec_instrument_rule.rs "datafusion-tracing 52.0.0 - Docs.rs"
[2]: https://docs.rs/datafusion-tracing/latest/src/datafusion_tracing/exec_instrument_macros.rs.html "exec_instrument_macros.rs - source"
[3]: https://github.com/datafusion-contrib/datafusion-tracing "GitHub - datafusion-contrib/datafusion-tracing: Integration of opentelemetry with the tracing crate"
[4]: https://docs.rs/crate/datafusion-tracing/latest/source/src/instrumented_exec.rs "datafusion-tracing 52.0.0 - Docs.rs"
[5]: https://docs.rs/tracing-opentelemetry?utm_source=chatgpt.com "tracing_opentelemetry - Rust"
[6]: https://docs.rs/crate/datafusion-tracing/latest/source/src/node.rs "datafusion-tracing 52.0.0 - Docs.rs"
[7]: https://docs.rs/crate/datafusion-tracing/latest/source/src/metrics.rs "datafusion-tracing 52.0.0 - Docs.rs"

## 3) `InstrumentationOptions` (execution instrumentation control plane)

### 3.1 Type shape + where each knob is consumed

**Public struct (user-facing contract)**

````rust
pub struct InstrumentationOptions {
    pub record_metrics: bool,
    pub preview_limit: usize,
    pub preview_fn: Option<Arc<dyn Fn(&RecordBatch) -> Result<String, ArrowError> + Send + Sync>>,
    pub custom_fields: HashMap<String, String>,
}
``` :contentReference[oaicite:0]{index=0}

**Actual consumption points (what your settings *do* at runtime)**

- `InstrumentedExec::new(.., options)` captures:
  - `record_metrics` → gates whether the stream is wrapped in a metrics-recording stream (`if !record_metrics { return inner_stream; }`). :contentReference[oaicite:1]{index=1}
  - `preview_limit` + `preview_fn` → gates whether the stream is wrapped in a preview-recording stream (`if preview_limit == 0 { return inner_stream; }`), and passes both into `PreviewRecorder::builder(..).limit(preview_limit).preview_fn(preview_fn.clone())`. :contentReference[oaicite:2]{index=2}
  - **`custom_fields` are *not* used by `InstrumentedExec`** (explicit comment); they’re applied in the macro-generated span-creation closure instead. :contentReference[oaicite:3]{index=3}

---

### 3.2 `record_metrics: bool` — execution metrics on spans

#### What it does
- If `record_metrics == false`: execution streams are **not** wrapped with metrics recording. :contentReference[oaicite:4]{index=4}  
- If `record_metrics == true`: each operator span records aggregated DataFusion metrics **when the operator’s partitions complete**:
  - `MetricsRecorder::drop()` pulls `execution_plan.metrics()`, iterates `metrics.aggregate_by_name()`, and records each metric as span field `datafusion.metrics.<metric_name>`. :contentReference[oaicite:5]{index=5}  
  - The wrapper is designed to be safe under concurrent partition execution and to aggregate across partitions before reporting. :contentReference[oaicite:6]{index=6}

#### Syntax (builder + macro)
```rust
let opts = InstrumentationOptions::builder()
    .record_metrics(true)
    .preview_limit(0) // keep metrics only
    .build();

let rule = instrument_with_info_spans!(options: opts);
````

Builder methods are simple setters returning `Self`. ([Docs.rs][1])

#### Value proposition

* Turns DataFusion’s internal operator metrics into **first-class span attributes** (per operator), letting you sort/aggregate traces by `output_rows`, `elapsed_compute`, etc., directly in your tracing backend. ([Docs.rs][2])

#### Watchouts

* Metrics are recorded on **drop** after all partition streams complete; if your execution is canceled early, you may get partial/no final metrics depending on how the streams unwind. (Design is drop-triggered.) ([Docs.rs][2])
* Metrics field names are *dynamic* at record time (`format!("datafusion.metrics.{}", ...)`), but the macro pre-declares a large catalog of `datafusion.metrics.* = field::Empty` to make those records “stick” in span metadata. ([Docs.rs][3])

---

### 3.3 `preview_limit: usize` — data preview capture gate + global cap

#### What it does

* `preview_limit == 0` → **no preview wrapping**. ([Docs.rs][4])
* `preview_limit > 0` → wraps each operator’s output stream with preview recording. ([Docs.rs][4])
* The limit is applied **globally across all partitions** for that operator span (not “per partition”). ([Docs.rs][4])

#### Syntax

```rust
let opts = InstrumentationOptions::builder()
    .preview_limit(5)   // up to 5 rows total per operator span
    .build();
```

([Docs.rs][1])

#### Value proposition

* Quick “what is this operator emitting?” visibility without materializing full results; the preview is attached as a span field for the operator. ([Docs.rs][5])

#### Watchouts

* This is a **data exfiltration footgun**: the preview string is recorded to `datafusion.preview` on the span. Treat it like logs (PII/secrets). ([Docs.rs][5])
* Preview strings can be large; many trace backends / collectors enforce attribute size limits (truncation/drop behavior varies). Keep previews tight.

---

### 3.4 `preview_fn: Option<Arc<PreviewFn>>` — preview formatting callback

#### Exact callback type

In the implementation layer, the callback type is:

```rust
pub type PreviewFn = dyn Fn(&RecordBatch) -> Result<String, ArrowError> + Send + Sync;
```

([Docs.rs][5])

#### Default behavior (when `preview_fn == None`)

* The default preview formatter uses Arrow’s `pretty_format_batches(...)` and converts to `String`. ([Docs.rs][5])
* `PreviewRecorderBuilder::preview_fn(None)` resets to that default. ([Docs.rs][5])

#### Where/when it runs

* On drop of the preview recorder, it concatenates collected batches, slices to `limit`, then calls your formatter; on success it records to the span field `datafusion.preview`. ([Docs.rs][5])
* If your formatter returns `Err`, it **warns and does not record** the preview (query execution is not failed by preview formatting). ([Docs.rs][5])

#### Syntax (custom formatter)

```rust
let opts = InstrumentationOptions::builder()
    .preview_limit(5)
    .preview_fn(Arc::new(|batch: &RecordBatch| {
        pretty_format_compact_batch(batch, 64, 3, 10).map(|fmt| fmt.to_string())
    }))
    .build();
```

Quick-start uses this exact pattern. ([Docs.rs][6])

#### Watchouts

* Your formatter runs in the execution pipeline’s teardown path; keep it **cheap**, allocation-bounded, and non-blocking. ([Docs.rs][5])
* `preview_limit > 0` is required for `preview_fn` to matter (preview wrapping is skipped when limit is zero). ([Docs.rs][4])

---

### 3.5 `custom_fields: HashMap<String, String>` — static tags (compile-time fields, runtime values)

#### What it does (mechanically)

* `instrument_with_spans!` clones `options.custom_fields` into the span-create closure and does:

  ```rust
  for (key, value) in custom_fields.iter() {
      span.record(key.as_str(), value);
  }
  ```

  ([Docs.rs][3])

#### Critical constraint (from `tracing`)

* You **cannot** record a value for a field that was not declared when the span was created; calling `span.record("new_field", ...)` for an undeclared field has **no effect**. ([Docs.rs][7])
  ⇒ Therefore, custom field *keys* must be declared in the macro invocation (typically as `field::Empty`) so `span.record` can actually attach values. ([Docs.rs][8])

#### Canonical syntax pattern

```rust
let options = InstrumentationOptions::builder()
    .add_custom_field("custom.key1", "value1")
    .add_custom_field("custom.key2", "value2")
    .build();

let rule = instrument_with_spans!(
    Level::INFO,
    options: options,
    custom.key1 = field::Empty,
    custom.key2 = field::Empty,
);
```

This is exactly the documented “custom_fields + placeholder fields” pattern. ([Docs.rs][8])

#### Watchouts

* `custom_fields` are applied in the macro-generated span creation, **not** inside `InstrumentedExec` itself (explicit comment). If you bypass macros and construct wrappers manually, you must handle field recording yourself. ([Docs.rs][4])
* The macro clones the entire `HashMap` into the closure (`let custom_fields = options.custom_fields.clone();`). Don’t put large maps here. ([Docs.rs][3])

---

### 3.6 Builder UX (primary surface)

`InstrumentationOptions::builder()` returns an `InstrumentationOptionsBuilder`. ([Docs.rs][9])

**Builder methods (exact names + signatures)**

* `record_metrics(self, record: bool) -> Self` ([Docs.rs][1])
* `preview_limit(self, limit: usize) -> Self` (0 disables previews) ([Docs.rs][1])
* `preview_fn(self, func: Arc<PreviewFn>) -> Self` ([Docs.rs][1])
* `add_custom_field<K: Into<String>, V: Into<String>>(self, key: K, value: V) -> Self` ([Docs.rs][1])
* `custom_fields(self, fields: HashMap<String, String>) -> Self` (replace-all) ([Docs.rs][1])
* `build(self) -> InstrumentationOptions` ([Docs.rs][1])

**Default behavior**

* The struct derives `Default`; with Rust defaults this implies: `record_metrics=false`, `preview_limit=0`, `preview_fn=None`, `custom_fields=empty`. ([Docs.rs][1])

[1]: https://docs.rs/datafusion-tracing/latest/src/datafusion_tracing/options.rs.html "options.rs - source"
[2]: https://docs.rs/crate/datafusion-tracing/latest/source/src/metrics.rs "datafusion-tracing 52.0.0 - Docs.rs"
[3]: https://docs.rs/datafusion-tracing/latest/src/datafusion_tracing/exec_instrument_macros.rs.html "exec_instrument_macros.rs - source"
[4]: https://docs.rs/crate/datafusion-tracing/latest/source/src/instrumented_exec.rs "datafusion-tracing 52.0.0 - Docs.rs"
[5]: https://docs.rs/crate/datafusion-tracing/latest/source/src/preview.rs "datafusion-tracing 52.0.0 - Docs.rs"
[6]: https://docs.rs/datafusion-tracing "datafusion_tracing - Rust"
[7]: https://docs.rs/tracing/latest/tracing/struct.Span.html?utm_source=chatgpt.com "Span in tracing - Rust"
[8]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/macro.instrument_with_spans.html "instrument_with_spans in datafusion_tracing - Rust"
[9]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/struct.InstrumentationOptions.html "InstrumentationOptions in datafusion_tracing - Rust"

## 4) Metrics capture: “DataFusion metrics → span fields”

### 4.1 Source of truth: DataFusion physical-plan metrics (what exists before tracing)

DataFusion physical operators emit **runtime metrics** (per operator / per partition) so you can see where time is spent and how much data moved. The canonical inspection surface is `EXPLAIN ANALYZE` (it annotates each physical node with `metrics=[...]`). ([Apache DataFusion][1])

**Baseline metrics (present on “most physical operators”)**

* `elapsed_compute` — CPU time spent processing work (not wall-clock).
* `output_rows` — total rows produced.
* `output_bytes` — memory usage of all output batches (can be **overestimated** if batches share buffers).
* `output_batches` — number of output batches produced. ([Apache DataFusion][1])

**Operator-specific metrics exist and vary**

* Example: `FilterExec` exposes `selectivity = output_rows / input_rows`. ([Apache DataFusion][1])
* Example: `DataSourceExec` (Parquet) can expose `bytes_scanned`, `time_elapsed_opening`, `time_elapsed_scanning_total`, plus pruning-related counters when pushdown is enabled. ([Apache DataFusion][2])
* Example: `SortExec` (TopK) can expose `row_replacements`. ([Apache DataFusion][2])

**Metric “shape” primitives you’ll see across operators**
DataFusion has helper metric families beyond baseline (e.g., `RatioMetrics`, `PruningMetrics`, `SpillMetrics`, `SplitMetrics`). ([Docs.rs][3])
A concrete example: `SpillMetrics` defines `spill_file_count`, `spilled_bytes`, `spilled_rows`. ([Docs.rs][4])

**Aggregation semantics (partitions → one value)**
DataFusion’s `MetricsSet::aggregate_by_name()` aggregates metrics with the same name into a derived set where the result has `Partition=None` (i.e., “rolled up across partitions”). ([Docs.rs][5])

**Watchouts**

* **CPU-time semantics:** `elapsed_compute` is CPU time; DataFusion notes times/counters are reported “across all cores” (summed). ([Apache DataFusion][2])
* **Coverage is evolving:** the user-guide metrics page explicitly calls out TODO coverage for remaining operators. ([Apache DataFusion][1])
* **`output_bytes` inflation:** can double-count shared buffers. ([Apache DataFusion][1])

---

### 4.2 How datafusion-tracing maps metrics into span fields (naming + prefixing)

#### 4.2.1 Span attribute namespace

DataFusion Tracing emits metrics as **span fields** under a stable prefix:

> `datafusion.metrics.<metric_name>`

Where `<metric_name>` is the DataFusion runtime metric’s string name (the same names you see in `EXPLAIN ANALYZE`, e.g., `output_rows`, `elapsed_compute`, `bytes_scanned`, …). ([Apache DataFusion][2])

#### 4.2.2 Field declaration strategy (why you “get all metrics by default”)

`instrument_with_spans!` says: “By default, it includes all known metrics fields relevant to DataFusion execution.” ([Docs.rs][6])

It also documents a **regen script** that enumerates native DataFusion metric names and formats them as:

> `datafusion.metrics.<name> = tracing::field::Empty,`

so the span is created with a declared field-set large enough for later `span.record(...)` calls to stick. ([Docs.rs][6])

**Value proposition**

* You don’t have to “curate” metrics keys per operator. If DataFusion emits it, you get an attribute of the form `datafusion.metrics.*` on that operator’s span (subject to exporter limits).

**Watchouts**

* **Attribute size limits** (collector/backend): high-metric operators can generate lots of fields; some backends truncate/drop attributes.
* **Version drift:** metric-name inventory changes with DataFusion versions; the crate’s “regen list” note is your canary for schema changes. ([Docs.rs][6])

---

### 4.3 “Which metrics exist?” — operator-class inventory (what to expect, and how to enumerate)

#### 4.3.1 Universal-ish (most operators)

Expect (at least) baseline:

* `datafusion.metrics.elapsed_compute`
* `datafusion.metrics.output_rows`
* `datafusion.metrics.output_batches`
* `datafusion.metrics.output_bytes` ([Apache DataFusion][1])

#### 4.3.2 Filters

* `datafusion.metrics.selectivity` (ratio: `output_rows / input_rows`). ([Apache DataFusion][1])

#### 4.3.3 Scans / Data sources (File/Parquet are the richest)

From `EXPLAIN ANALYZE` examples, `DataSourceExec` with Parquet can emit:

* `bytes_scanned`
* `time_elapsed_opening`
* `time_elapsed_scanning_total`
* pushdown/pruning counters such as `page_index_rows_pruned`, `page_index_pages_pruned`, `row_groups_pruned_bloom_filter`, `row_groups_pruned_statistics` ([Apache DataFusion][2])

#### 4.3.4 Sort / TopK

Example metric:

* `row_replacements` on `SortExec: TopK(fetch=...)` ([Apache DataFusion][2])

#### 4.3.5 Spill-capable operators (sorts / joins / aggs / windowing depending on plan + memory manager)

If an operator spills, the metric *family* you should expect is:

* `spill_file_count`
* `spilled_bytes`
* `spilled_rows` ([Docs.rs][4])

*(Whether a specific operator uses spilling metrics depends on the operator + config + input size; `SpillMetrics` is the reusable helper.)* ([Docs.rs][4])

#### 4.3.6 “Shuffle” / exchange boundaries

Operators like `RepartitionExec`, `CoalescePartitions`, `SortPreservingMergeExec` are explicitly called out as “data crosses between cores” points. ([Apache DataFusion][2])
Metrics here are often baseline + operator-specific counters; the fastest way to enumerate is to run the exact query and inspect either:

* `EXPLAIN ANALYZE` (ground truth list of metric names per operator), or
* trace spans (attributes under `datafusion.metrics.*`). ([Apache DataFusion][2])

---

### 4.4 Overhead: how it scales, and how `record_metrics` gates it

#### 4.4.1 Scaling model (practical)

Metric work is driven by:

* **# partitions** (metrics are typically instantiated per-partition and later aggregated) ([Docs.rs][5])
* **# batches / # polls** (baseline metrics like `output_rows` and timers update as batches flow) ([Apache DataFusion][1])
* **operator-specific work** (e.g., pruning counters, spill counters) ([Apache DataFusion][2])

Also note the interpretability gotcha: CPU time is summed across cores. ([Apache DataFusion][2])

#### 4.4.2 `record_metrics` as the hard gate

DataFusion Tracing surfaces a single kill-switch at the instrumentation layer:

* `record_metrics: bool` — “Enable or disable recording of DataFusion execution metrics.” ([Docs.rs][6])

**Operational pattern**

* **Perf debugging / profiling:** `record_metrics=true` (and usually `preview_limit=0` to avoid data-content overhead).
* **Always-on production tracing:** keep `record_metrics=true` only if (a) your exporter/backends tolerate the attribute volume and (b) you have sampling in place; otherwise disable or downsample traces upstream.

**Watchouts**

* Turning metrics on is “cheap until it isn’t”: high-throughput plans (many partitions × many batches) amplify even small per-batch counter/timer updates.
* Don’t interpret `elapsed_compute` as latency; it is CPU-time and additive across concurrency. ([Apache DataFusion][2])

[1]: https://datafusion.apache.org/user-guide/metrics.html "Metrics — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/user-guide/explain-usage.html "Reading Explain Plans — Apache DataFusion  documentation"
[3]: https://docs.rs/datafusion/latest/datafusion/physical_plan/metrics/index.html?utm_source=chatgpt.com "datafusion::physical_plan::metrics - Rust"
[4]: https://docs.rs/datafusion/latest/datafusion/physical_expr_common/metrics/struct.SpillMetrics.html?utm_source=chatgpt.com "SpillMetrics in datafusion::physical_expr_common::metrics - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/physical_expr_common/metrics/struct.MetricsSet.html?utm_source=chatgpt.com "MetricsSet in datafusion::physical_expr_common::metrics - Rust"
[6]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/macro.instrument_with_spans.html?utm_source=chatgpt.com "instrument_with_spans in datafusion_tracing - Rust"

## 5) Partial result preview subsystem (bounded “data sniffing” per operator span)

### 5.1 Wiring + syntax (how previews are turned on, and where they land)

**Enable preview capture**

* Set `InstrumentationOptions.preview_limit > 0` (rows) and optionally `preview_fn`. The `instrument_with_spans!` docs define `preview_limit` as “rows to preview per span (0 disables)” and `preview_fn` as an optional formatter (default is Arrow `pretty_format_batches`). ([Docs.rs][1])
* Canonical “compact preview” setup (from crate docs) uses `.preview_limit(5)` plus `pretty_format_compact_batch(...)` in `preview_fn`. ([Docs.rs][2])

**Where the preview is recorded**

* The macro-created span **declares** `datafusion.preview = field::Empty` so later `Span::record("datafusion.preview", ...)` is legal and actually attaches the value. ([Docs.rs][3])
* Preview recorder records the formatted string into the span field `"datafusion.preview"` during `Drop`. ([Docs.rs][4])

**ExecutionPlan integration point (runtime)**

* `InstrumentedExec::preview_recording_stream(...)` wraps each partition stream *only if* `preview_limit != 0`, creates a single shared `PreviewRecorder` sized to `output_partitioning().partition_count()`, and returns a `PreviewRecordingStream` per partition. ([Docs.rs][5])

---

### 5.2 Sampling strategy (what rows get captured, per batch / per partition / per operator span)

#### 5.2.1 Per-partition sampling (stream-time)

Each `PreviewRecordingStream` does:

* On every successful `poll_next` returning a `RecordBatch`, it takes **only the first rows of that batch**: `batch.slice(0, to_store)` where `to_store = min(needed, batch.num_rows())`. ([Docs.rs][4])
* It accumulates those slices into a per-partition `preview_batch` (concatenating with `concat_batches` as new slices arrive) until `stored_rows == limit`. ([Docs.rs][4])
* On stream drop, it stores that partition’s accumulated `preview_batch` into a `OnceLock` slot for that partition index (`partition_previews[partition].set(...)`). ([Docs.rs][4])

**Implication:** previews are “first-N rows observed on each partition stream”, bounded by `limit`, with *no* attempt to sample from later rows.

#### 5.2.2 Global preview cap (operator-span-time)

At operator completion (when the shared `PreviewRecorder` drops):

* It collects all partitions’ preview batches, concatenates them, and then applies the **global cap**: `num_rows = concat.num_rows().min(self.limit)` followed by `concat.slice(0, num_rows)`. ([Docs.rs][4])
* The `InstrumentedExec` comment explicitly states the limit is “applied globally on all partitions before the preview is reported.” ([Docs.rs][5])

**Two non-obvious consequences**

* **Bias to low partition IDs:** concatenation follows partition index order (`partition_previews.iter()`), and then slices from row 0. If partition 0 has ≥ `limit` rows early, you’ll likely see only partition 0. ([Docs.rs][4])
* **Memory can scale as `partition_count * limit` rows** (each partition may store up to `limit` rows, even though the final emitted preview is capped to `limit`). ([Docs.rs][4])

#### 5.2.3 When the preview appears (lifecycle)

* Preview is recorded in `PreviewRecorder::drop()`, i.e., effectively **after the operator has finished** and all partition streams have unwound (the recorder is held behind `Arc` and shared across partition streams). ([Docs.rs][5])

---

### 5.3 Formatting: defaults vs “safe compact” formatting

#### Default formatting (safe in correctness, unsafe in volume)

* `default_preview_fn` uses Arrow’s `pretty_format_batches` to render a table string. ([Docs.rs][4])
* `PreviewRecorderBuilder::preview_fn(None)` resets to this default. ([Docs.rs][4])

**Watchouts**

* `pretty_format_batches` can produce very wide / tall strings (trace attribute bloat + backend truncation risk).
* The preview string is **data content**, emitted into traces (`datafusion.preview`), so treat as log-grade sensitive output. ([Docs.rs][4])

#### Custom formatting (your “safety/volume policy” hook)

* Preview formatting errors do **not** fail query execution: formatter errors cause a `warn!` and skip recording the preview. ([Docs.rs][4])
* Recommended pattern: set `preview_fn` to a bounded formatter like `pretty_format_compact_batch(...)` (crate’s own quick-start does exactly this). ([Docs.rs][2])

---

### 5.4 `pretty_format_compact_batch` truncation rules (width/height control + column dropping)

#### Contract (public docs)

* Formats a `RecordBatch` to an ASCII table constrained by `max_width`; “columns are dynamically resized or truncated” and columns that can’t fit “may be dropped.” ([Docs.rs][6])

#### How width drives “column dropping” (implementation)

* If `max_width == 0`: **no constraint**, all columns displayed. ([Docs.rs][7])
* Else:

  * It precomputes “natural” column widths (max cell width + 3 chars for separators/padding). ([Docs.rs][7])
  * It walks columns left-to-right, computing an effective per-column width:

    * `col_width = width.min(min_compacted_col_width).max(4)`
    * If adding that width would exceed `max_width`, it stops; remaining columns are **dropped** (`nb_displayed_columns = fit_columns`). ([Docs.rs][7])

**Notes on `min_compacted_col_width`**

* It behaves like a **cap** used for “fit calculation” (smaller cap → more columns *attempt* to fit). The `.max(4)` guard means even `0` effectively becomes `4` during the fit pass. ([Docs.rs][7])

#### Truncation UX controls

* Uses a truncated preset if not all columns fit, and sets a truncation indicator `"…"` for cell truncation. ([Docs.rs][7])
* Applies row height constraint via `row.max_height(max_row_height)` when `max_row_height > 0`. ([Docs.rs][7])
* Applies table width via `table.set_width(max_width as u16)` when `max_width > 0`. ([Docs.rs][7])

---

### 5.5 Key watchouts (production-grade posture)

* **Data leakage:** `datafusion.preview` contains row values; do not enable previews where sensitive data can appear unless you also redact in `preview_fn`. ([Docs.rs][4])
* **Trace payload blow-up:** previews are large attributes; prefer `pretty_format_compact_batch(max_width, max_row_height, …)` or disable previews entirely (`preview_limit = 0`). ([Docs.rs][5])
* **Partition bias / non-determinism:** preview is “first rows observed per partition” and then truncated globally; it’s not an ordered sample of the logical result unless the plan enforces ordering. ([Docs.rs][4])
* **Memory overhead:** worst case stores up to `limit` rows per partition in memory before emitting a single global `limit` preview. ([Docs.rs][4])
* **Span field declaration:** if you bypass the macros and build your own span factory, you must declare `datafusion.preview = field::Empty` (or equivalent) or `Span::record` won’t attach. ([Docs.rs][3])

[1]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/macro.instrument_with_spans.html "instrument_with_spans in datafusion_tracing - Rust"
[2]: https://docs.rs/datafusion-tracing/latest/src/datafusion_tracing/lib.rs.html "lib.rs - source"
[3]: https://docs.rs/crate/datafusion-tracing/latest/source/src/exec_instrument_macros.rs "datafusion-tracing 52.0.0 - Docs.rs"
[4]: https://docs.rs/crate/datafusion-tracing/latest/source/src/preview.rs "datafusion-tracing 52.0.0 - Docs.rs"
[5]: https://docs.rs/crate/datafusion-tracing/latest/source/src/instrumented_exec.rs "datafusion-tracing 52.0.0 - Docs.rs"
[6]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/fn.pretty_format_compact_batch.html?utm_source=chatgpt.com "pretty_format_compact_batch in datafusion_tracing - Rust"
[7]: https://docs.rs/crate/datafusion-tracing/latest/source/src/preview_utils.rs "datafusion-tracing 52.0.0 - Docs.rs"

## 6) Rule-phase tracing (planning pipeline spans)

### 6.1 What gets wrapped + where spans appear

`instrument_rules_with_spans!` (and the level wrappers like `instrument_rules_with_info_spans!`) takes an **already-built `SessionState`** and returns a new `SessionState` whose rule pipelines are wrapped in spans. It covers:

* **Analyzer rules**
* **Logical optimizer rules**
* **Physical optimizer rules**
* grouped under phase spans named (via `otel.name`) **`analyze_logical_plan`**, **`optimize_logical_plan`**, **`optimize_physical_plan`**. ([Docs.rs][1])

When physical optimizer instrumentation is enabled, the macro also auto-instruments physical plan creation (planner) — that’s why this layer often becomes your “planning timeline.” ([Docs.rs][1])

---

### 6.2 Syntax (macro invocation patterns you’ll actually use)

#### Minimal (INFO-level, default target)

```rust
use datafusion_tracing::{instrument_rules_with_info_spans, RuleInstrumentationOptions};

let rule_opts = RuleInstrumentationOptions::full(); // or phase_only()
let state = instrument_rules_with_info_spans!(
    options: rule_opts,
    state: state
);
```

Wrapper macros are just convenience around the generic macro with a fixed level. ([Docs.rs][2])

#### Explicit level + explicit target + extra declared fields

```rust
use tracing::field;

let state = instrument_rules_with_spans!(
    target: "df.plan",
    tracing::Level::DEBUG,
    options: RuleInstrumentationOptions::full().with_plan_diff(),
    state: state,
    // declare any extra fields you want to populate later:
    tenant.id = field::Empty,
    build.git_sha = field::Empty,
);
```

The macro supports `target:` and “`tracing::span!`-style” `$fields` passthrough. ([Docs.rs][1])

---

### 6.3 Span naming model (how rule spans are named in your backend)

#### 6.3.1 Static Rust span names vs exported OTel span names

The macro creates spans with static Rust names:

* rule spans named `"Rule"`
* phase spans named `"Phase"`

…but it sets **`otel.name = rule_name`** for each rule span and **`otel.name = phase_name`** for each phase span. ([Docs.rs][3])

If you’re exporting via `tracing-opentelemetry`, `otel.name` is a special field that **overrides the span name sent to OpenTelemetry exporters**. ([Docs.rs][4])

**Net effect in Jaeger/Datadog/etc.:**

* You typically *see span names equal to* the **rule’s name string** and **phase names** (`analyze_logical_plan`, …), not `"Rule"`/`"Phase"`. ([Docs.rs][3])

#### 6.3.2 Where `rule_name` comes from (what you should assume)

The macro’s span factory takes `rule_name: &str` and uses it as `otel.name`. ([Docs.rs][3])
In practice, that string is sourced from DataFusion rule naming surfaces (e.g. `PhysicalOptimizerRule::name()` is explicitly “a human readable name for this optimizer rule”). ([Docs.rs][5])

---

### 6.4 Rule ordering visibility (the “why this is useful” part)

#### 6.4.1 Ordering is first-class in DataFusion’s optimizer design

DataFusion (like other industrial engines) structures optimization as **a sequence of passes/rules**; the blog explicitly notes the “overall optimizer is composed of a sequence of these rules” and that “the specific order of the rules often matters.” ([Apache DataFusion][6])
Even within a single physical optimizer implementor, DataFusion calls out **ordering-dependent** subrules (example shown on `EnforceSorting`). ([Docs.rs][5])

#### 6.4.2 How to *read* ordering from traces

Within each phase span, rule spans will appear in the **execution order** of that phase’s rule list:

* **you get a linear timeline view** (which rule ran first/next)
* and a **hierarchical view** (rule spans parented under the phase span)

This is the practical payoff: you can answer “which rule(s) are expensive?” and “which rule caused the big plan churn?” without instrumenting DataFusion internals yourself. ([Docs.rs][1])

#### 6.4.3 Ensuring ordering is stable (when you add your own rules)

DataFusion’s public APIs append rules:

* `SessionContext::add_optimizer_rule(...)` **adds an optimizer rule to the end**
* `SessionContext::add_analyzer_rule(...)` **adds an analyzer rule to the end** ([Docs.rs][7])

So if you inject custom rules, trace ordering will naturally reflect the *actual append order* (and you can verify that visually in the span timeline). ([Docs.rs][7])

---

### 6.5 Span fields that support “ordering + deltas” workflows (what to filter/group on)

#### 6.5.1 Per-rule span fields

Rule span factory declares:

* `otel.name = rule_name`
* `datafusion.plan_diff = field::Empty` (populated when plan diffs are enabled) ([Docs.rs][3])

#### 6.5.2 Per-phase span fields (end-of-phase summaries)

Phase span factory declares:

* `datafusion.effective_rules`
* `datafusion.plan_diff`
* `datafusion.optimizer.pass`
* `datafusion.optimizer.max_passes` ([Docs.rs][3])

The pass fields are specifically there because optimizers are commonly multi-pass (rule sequences can repeat until a fixpoint / max pass limit). ([Docs.rs][3])

---

### 6.6 Watchouts (the ones that bite fast)

* **If you use `phase_only()` you lose per-rule ordering visibility.** It creates phase-level spans “without individual rule spans” to reduce verbosity. ([Docs.rs][8])
* **Plan diffs can explode span payload size.** `with_plan_diff()` enables diff recording; great for debugging, noisy for always-on. ([Docs.rs][8])
* **Span visibility is subscriber/exporter-gated.** If you instrument at `DEBUG` but export at `INFO`, you’ll think it’s broken (spans are created, then filtered). ([Docs.rs][1])
* **Span names should stay low-cardinality.** Using rule names and fixed phase names is “safe”; avoid stuffing tenant IDs into `otel.name` (put them in attributes instead). ([Docs.rs][4])

[1]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/macro.instrument_rules_with_spans.html "instrument_rules_with_spans in datafusion_tracing - Rust"
[2]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/macro.instrument_rules_with_info_spans.html "instrument_rules_with_info_spans in datafusion_tracing - Rust"
[3]: https://docs.rs/datafusion-tracing/latest/src/datafusion_tracing/rule_instrumentation_macros.rs.html "rule_instrumentation_macros.rs - source"
[4]: https://docs.rs/tracing-opentelemetry?utm_source=chatgpt.com "tracing_opentelemetry - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/trait.PhysicalOptimizerRule.html "PhysicalOptimizerRule in datafusion::physical_optimizer - Rust"
[6]: https://datafusion.apache.org/blog/2025/06/15/optimizing-sql-dataframes-part-one/ "Optimizing SQL (and DataFrames) in DataFusion, Part 1: Query Optimization Overview - Apache DataFusion Blog"
[7]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html "SessionContext in datafusion::execution::context - Rust"
[8]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/struct.RuleInstrumentationOptions.html "RuleInstrumentationOptions in datafusion_tracing - Rust"

## 7) Physical plan creation tracing (planner instrumentation)

### 7.1 Enablement semantics (what “auto-enabled” actually means)

**Trigger condition**

* Planner tracing is **only** enabled when you instrument rules *and* you have **physical optimizer instrumentation enabled** in the rule instrumentation options. The macro docs are explicit: *“When physical optimizer instrumentation is enabled, the query planner is also automatically instrumented to trace physical plan creation.”* ([Docs.rs][1])

**Canonical wiring (syntax)**

```rust
use datafusion_tracing::{instrument_rules_with_info_spans, RuleInstrumentationOptions};

let rule_opts = RuleInstrumentationOptions::full(); // includes physical optimizer phase
let state = instrument_rules_with_info_spans!(
    options: rule_opts,
    state: state,
);
```

This yields phase spans named:

* `analyze_logical_plan`
* `optimize_logical_plan`
* `optimize_physical_plan` ([Docs.rs][1])

**Value proposition**

* “Physical plan build time” becomes visible as a *first-class segment* in the traced query lifecycle (i.e., not conflated with optimization or execution). The crate’s top-level doc explicitly calls out the lifecycle order including “Physical Plan Creation” between logical optimization and physical optimization/execution. ([Docs.rs][2])

---

### 7.2 Which planner stages are spanned (what you should expect to see)

**Hard guarantee (from the crate’s contract)**

* The **query planner** is instrumented such that **physical plan creation** is traced when physical optimizer instrumentation is on. That means the span boundary at minimum surrounds the DataFusion query-planning operation that converts the optimized logical plan into an `ExecutionPlan` tree. ([Docs.rs][1])

**Practical planner boundary (DataFusion API surface)**

* The pivot point for “physical plan creation” in DataFusion’s extension API is the `QueryPlanner` trait’s `create_physical_plan` method (the hook where logical→physical is performed / extended). ([Docs.rs][3])
* Conceptually, physical plan creation is: **LogicalPlan → ExecutionPlan**, and the resulting physical plan can differ by hardware/config (DataFusion calls this out in the explain-plan docs). ([Apache DataFusion][4])

**What tends to show up as sub-stages (how to reason about it even if the exact span names vary)**
Even if the library chooses *one* big “create physical plan” span vs multiple nested spans, the *work* you are measuring is typically dominated by:

* **planning physical operators** (scan/filter/projection/join/aggregate/sort/repartition nodes)
* **planning physical expressions** (scalar exprs, aggregates, window exprs)
* **data-source planning** (table providers, file formats, pushdowns / pruning decisions)
* **extension planning** (if you use custom `QueryPlanner` / extension nodes)

The only stable claim you should rely on for downstream automation is: **there is traced coverage around the logical→physical conversion boundary** (via query planner instrumentation). ([Docs.rs][1])

---

### 7.3 Correlating plan-build spans with the eventual physical operator tree

#### 7.3.1 The object you’re correlating: `ExecutionPlan` is the bridge

* Physical plan creation yields an `ExecutionPlan` tree (DataFusion’s physical plan node interface). ([Docs.rs][5])
* That same tree is what later produces execution streams (`execute(...)`) during runtime. ([Docs.rs][5])

So correlation is fundamentally: **planner span(s) that build the `ExecutionPlan`** ↔ **runtime spans emitted while executing that `ExecutionPlan` tree**.

#### 7.3.2 Two concrete correlation techniques that work reliably

**A) Match by physical plan shape (tree structure)**

1. In the planner span window, capture/print the physical plan (or run `EXPLAIN` / `EXPLAIN ANALYZE` out-of-band) to get the operator tree you expect. DataFusion explicitly distinguishes logical vs physical plans and provides explain tooling. ([Apache DataFusion][4])
2. In the trace UI, the runtime operator spans should form a nested tree mirroring the physical operator structure (parent/child).
3. Use “shape matching”: operator ordering and nesting (scan → filter → agg → …) is the strongest invariant.

**B) Match by operator identity (operator names)**

* Use `ExecutionPlan`’s display/name surfaces to line up operator spans with nodes you see in printed physical plans (DataFusion supports displaying plans, and each node represents a physical operator). ([Docs.rs][5])

> Operationally: once you find the span corresponding to the root operator, you can walk children spans to reconstruct the exact operator tree that executed, and compare it to the plan that was built in the planner span window.

#### 7.3.3 Making correlation “machine-joinable” (recommended tagging discipline)

Even without bespoke “plan-id” plumbing, you can make correlation robust by ensuring both planner-phase spans and runtime spans share the same **query identity fields**:

* Use the macros’ **span field passthrough** to attach stable identifiers (query hash, statement name, request id) at instrumentation time.
* Use a **single `target:`** namespace for all DataFusion tracing spans (rule + exec) so filtering in the backend is trivial.

Rule macro has explicit `target:` support and records rule/phase spans with `otel.name` = rule/phase name. ([Docs.rs][6])

---

### 7.4 Watchouts (the ones that matter in production)

* **Planning can be expensive** for wide schemas / many expressions; don’t assume “execution is the only cost center.” The fact that the project explicitly adds planner tracing is a recognition that physical planning can be non-trivial. ([Docs.rs][1])
* **Span volume**: if you enable full per-rule spans + plan diffs, you increase trace cardinality before you even start execution (good for debugging, noisy for always-on). ([Docs.rs][6])
* **Hardware/config sensitivity**: DataFusion notes physical plans can differ across hardware/config; if you compare traces across environments, expect planner spans and operator trees to legitimately diverge. ([Apache DataFusion][4])

[1]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/macro.instrument_rules_with_spans.html "instrument_rules_with_spans in datafusion_tracing - Rust"
[2]: https://docs.rs/datafusion-tracing "datafusion_tracing - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/execution/context/trait.QueryPlanner.html?utm_source=chatgpt.com "QueryPlanner in datafusion::execution::context - Rust"
[4]: https://datafusion.apache.org/user-guide/explain-usage.html?utm_source=chatgpt.com "Reading Explain Plans — Apache DataFusion documentation"
[5]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html?utm_source=chatgpt.com "ExecutionPlan in datafusion::physical_plan - Rust"
[6]: https://docs.rs/datafusion-tracing/latest/src/datafusion_tracing/rule_instrumentation_macros.rs.html "rule_instrumentation_macros.rs - source"

## 8) `RuleInstrumentationOptions` (verbosity + diffs)

### 8.1 Mental model: 3 phases × 3 levels (+ plan diff)

`RuleInstrumentationOptions` is essentially:

* `plan_diff: bool` — unified diff emission toggle
* `analyzer: InstrumentationLevel`
* `optimizer: InstrumentationLevel`
* `physical_optimizer: InstrumentationLevel` ([Docs.rs][1])

`InstrumentationLevel` is hierarchical (rule spans require a parent phase span):

* `Disabled` (default): no spans
* `PhaseOnly`: only the phase span
* `Full`: phase span + per-rule spans ([Docs.rs][1])

Helpers define behavior precisely:

* `phase_span_enabled() == PhaseOnly|Full`
* `rule_spans_enabled() == Full` ([Docs.rs][1])

---

### 8.2 Canonical presets (the “do the obvious thing” constructors)

#### 8.2.1 `RuleInstrumentationOptions::full()`

**Meaning**

* `analyzer=Full`, `optimizer=Full`, `physical_optimizer=Full`, `plan_diff=false` ([Docs.rs][1])

**Syntax**

```rust
use datafusion_tracing::{instrument_rules_with_info_spans, RuleInstrumentationOptions};

let opts = RuleInstrumentationOptions::full();
let state = instrument_rules_with_info_spans!(options: opts, state: state);
```

([Docs.rs][2])

**What spans you get**

* Phase spans are always created (`analyze_logical_plan`, `optimize_logical_plan`, `optimize_physical_plan`)
* Per-rule spans created inside each phase (since `rule_spans_enabled()==true`) ([Docs.rs][3])

**Automatic side-effect**

* Physical plan creation (planner) tracing is enabled because `physical_optimizer.phase_span_enabled()` is true in Full. ([Docs.rs][4])

---

#### 8.2.2 `RuleInstrumentationOptions::phase_only()`

**Meaning**

* `analyzer=PhaseOnly`, `optimizer=PhaseOnly`, `physical_optimizer=PhaseOnly`, `plan_diff=false` ([Docs.rs][1])

**Syntax**

```rust
let opts = RuleInstrumentationOptions::phase_only();
let state = instrument_rules_with_info_spans!(options: opts, state: state);
```

([Docs.rs][2])

**What spans you get**

* Phase spans only (no per-rule spans); rules are passed through unwrapped when `rule_spans_enabled()==false`. ([Docs.rs][4])

**Automatic side-effect**

* Planner tracing is still enabled (physical optimizer phase span is enabled under PhaseOnly). ([Docs.rs][4])

---

#### 8.2.3 `.with_plan_diff()`

**Meaning**

* Sets `plan_diff = true` (does not change phase levels). ([Docs.rs][1])

**Canonical syntax**

```rust
let opts = RuleInstrumentationOptions::full().with_plan_diff();
let state = instrument_rules_with_info_spans!(options: opts, state: state);
```

([Docs.rs][2])

---

### 8.3 What “diffs” and “effective rules” actually mean (implementation semantics)

#### 8.3.1 Where diffs appear (fields)

The macro declares these fields up-front:

* **Rule spans**: `datafusion.plan_diff`
* **Phase spans**: `datafusion.effective_rules`, `datafusion.plan_diff`, plus pass counters `datafusion.optimizer.pass` / `datafusion.optimizer.max_passes` ([Docs.rs][3])

#### 8.3.2 Rule-level diff + “(modified)” rule span names (Full only)

When a rule wrapper detects a real plan change:

* If `plan_diff==true`: records `datafusion.plan_diff` on that **rule span**
* Always updates `otel.name` to `"<rule> (modified)"`
* Always appends the rule name into the phase’s `effective_rules` list ([Docs.rs][4])

Detection is guarded to avoid waste:

* Analyzer: clone+compare only when span is active; quick structural check before formatting strings ([Docs.rs][4])
* Optimizer: trusts `Transformed.transformed=false` to skip string work; verifies when `true` (rules can false-positive) ([Docs.rs][4])
* Physical optimizer: uses `Arc::ptr_eq` to avoid formatting if the plan `Arc` didn’t change ([Docs.rs][4])

Plan strings used for diffs:

* Logical: `display_indent_schema()`
* Physical: `displayable(plan).indent(true)` ([Docs.rs][4])

Unified diff generation:

* Uses `similar::TextDiff::from_lines(before, after)` and emits a line-oriented “sign + line” stream (`-` delete, `+` insert, ` ` equal). ([Docs.rs][4])

#### 8.3.3 Phase-level diff (PhaseOnly *or* Full, when `plan_diff==true`)

At phase open, it captures `plan_before` **only if** `plan_diff` is enabled **and** the phase span is active (`!span.is_disabled()`). ([Docs.rs][4])
At phase close, it records:

* `datafusion.effective_rules` (comma-joined) *only if non-empty*
* `datafusion.plan_diff` between phase start and end (only if plan changed) ([Docs.rs][4])

**Key implication:** `PhaseOnly + with_plan_diff()` gives you a **phase diff**, but (typically) no `effective_rules` because rules aren’t wrapped, so nothing records “which rule modified the plan.” ([Docs.rs][4])

---

### 8.4 “Full” vs “PhaseOnly” tradeoffs (cardinality + overhead)

#### Cardinality (span count)

**PhaseOnly**

* ~1 phase span per enabled phase invocation.
* Logical optimizer can be **multi-pass**; the phase sentinel records `datafusion.optimizer.pass` and `datafusion.optimizer.max_passes` and opens/closes an `optimize_logical_plan` span per pass. ([Docs.rs][4])

**Full**

* Everything in PhaseOnly, plus:
* +1 rule span per rule application inside each phase (analyzer/optimizer/physical optimizer), per pass. Wrapping is explicitly conditional on `rule_spans_enabled()` (Full only). ([Docs.rs][4])
* Optimizer rules that request engine-managed recursion (`apply_order() = Some(TopDown|BottomUp)`) are handled specially: the instrumentation performs traversal itself so you get **one span per rule application**, not one span per visited node. ([Docs.rs][4])

#### Overhead (runtime cost)

**Common (PhaseOnly + Full)**

* Expensive work is gated by `Span::is_disabled()` checks (“lazy instrumentation”): if the span is filtered out by your subscriber, it skips plan string capture, diff generation, and other heavyweight recording. ([Docs.rs][4])

**Full-only overhead**

* Per-rule span creation + span enter/exit per rule invocation.
* Modification detection overhead:

  * plan cloning (logical plans) or Arc cloning (physical) when span active
  * plan-to-string formatting when modifications are suspected
  * optional diff computation (TextDiff) when `plan_diff==true` ([Docs.rs][4])

#### Practical defaults

* **Always-on production telemetry:** `phase_only()` (optionally at INFO) + **no plan diffs** (keep payload + CPU bounded). ([Docs.rs][2])
* **Debug sessions / optimizer investigations:** `full().with_plan_diff()` (accept higher span count and string/diff cost for maximal attribution). ([Docs.rs][2])

---

### 8.5 Builder knobs (when presets are too blunt)

If you need per-phase tuning (e.g., Full only for optimizer, PhaseOnly elsewhere), use the builder:

* `RuleInstrumentationOptions::builder()`
* phase toggles:

  * `.analyzer()` / `.analyzer_phase_only()`
  * `.optimizer()` / `.optimizer_phase_only()`
  * `.physical_optimizer()` / `.physical_optimizer_phase_only()`
  * convenience: `.all()` / `.all_phase_only()`
* diff toggle: `.plan_diff()`
* finalize: `.build()` ([Docs.rs][1])

[1]: https://docs.rs/datafusion-tracing/latest/src/datafusion_tracing/rule_options.rs.html "rule_options.rs - source"
[2]: https://docs.rs/datafusion-tracing/latest/datafusion_tracing/struct.RuleInstrumentationOptions.html "RuleInstrumentationOptions in datafusion_tracing - Rust"
[3]: https://docs.rs/crate/datafusion-tracing/latest/source/src/rule_instrumentation_macros.rs "datafusion-tracing 52.0.0 - Docs.rs"
[4]: https://docs.rs/crate/datafusion-tracing/latest/source/src/rule_instrumentation.rs "datafusion-tracing 52.0.0 - Docs.rs"

## 9) OpenTelemetry export + collector wiring (operational integration)

### 9.1 End-to-end wiring model (DataFusion-tracing → `tracing` → OTel → backend)

**Core idea:** DataFusion-tracing emits **`tracing` spans** (with `otel.*` special fields). You export them by:

1. Building an **OpenTelemetry `TracerProvider`** + **OTLP exporter** (in-process)
2. Attaching a **`tracing-opentelemetry` layer** to your `tracing_subscriber` so `tracing` spans become OTel spans. ([Docs.rs][1])

`tracing-opentelemetry` special fields worth knowing:

* `otel.name`: overrides the span name sent to OTel exporters (DataFusion-tracing uses this heavily for operator/phase names). ([Docs.rs][1])

---

## 9.2 OTLP endpoint/protocol choices (+ exporter construction)

### 9.2.1 Protocol selection (OTLP/gRPC vs OTLP/HTTP)

**OTLP exporter config is standardized via env vars:**

* `OTEL_EXPORTER_OTLP_PROTOCOL` / `OTEL_EXPORTER_OTLP_TRACES_PROTOCOL`

  * `grpc`
  * `http/protobuf`
  * `http/json` ([OpenTelemetry][2])

**Defaults (important when you “just enable it”):**

* `OTEL_EXPORTER_OTLP_ENDPOINT`

  * gRPC default: `http://localhost:4317`
  * HTTP default: `http://localhost:4318` ([OpenTelemetry][2])

**OTLP/HTTP path semantics (easy to misconfigure):**

* If you set only `OTEL_EXPORTER_OTLP_ENDPOINT` for HTTP, the SDK constructs per-signal URLs:

  * traces: `/v1/traces`, metrics: `/v1/metrics`, logs: `/v1/logs` ([OpenTelemetry][2])
* Or set `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` explicitly (HTTP default ends with `/v1/traces`). ([OpenTelemetry][2])

**Operational rule of thumb**

* Prefer **OTLP/gRPC** (throughput/efficiency), fall back to **OTLP/HTTP + protobuf** when proxies/firewalls make HTTP/2 painful, and reserve **OTLP/HTTP + JSON** mainly for debugging payloads. ([OpenTelemetry][2])

---

### 9.2.2 Rust exporter construction (programmatic)

`opentelemetry-otlp` supports:

* gRPC
* HTTP (protobuf or JSON) ([Docs.rs][3])

**Canonical “collector default” gRPC setup (from OTel Rust docs)**

```rust
use opentelemetry::global;
use opentelemetry_otlp::SpanExporter;
use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};

fn init_tracer_provider() {
    let exporter = SpanExporter::builder()
        .with_tonic()     // OTLP/gRPC
        .build()
        .expect("Failed to create span exporter");

    let provider = SdkTracerProvider::builder()
        .with_resource(Resource::builder().with_service_name("dice_server").build())
        .with_batch_exporter(exporter)
        .build();

    global::set_tracer_provider(provider);
}
```

([OpenTelemetry][4])

**Explicit OTLP/HTTP exporter example (protobuf)**

```rust
use opentelemetry_otlp::{SpanExporter, Protocol, Compression};
use std::time::Duration;

let exporter = SpanExporter::builder()
    .with_http()
    .with_endpoint("http://localhost:4318/v1/traces")
    .with_timeout(Duration::from_secs(3))
    .with_protocol(Protocol::HttpBinary)
    .with_compression(Compression::Gzip)
    .build()?;
```

([Docs.rs][3])

**Attach to `tracing`**

```rust
use tracing_subscriber::{layer::SubscriberExt, Registry};
use tracing_opentelemetry::OpenTelemetryLayer;

// tracer = provider.tracer("…") from your SdkTracerProvider
let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
let subscriber = Registry::default().with(otel_layer);
tracing::subscriber::set_global_default(subscriber)?;
```

`tracing-opentelemetry` provides the layer and documents `otel.name` semantics. ([Docs.rs][1])

**Watchouts**

* **Headers/auth:** use `OTEL_EXPORTER_OTLP_HEADERS` for API keys/custom headers (gRPC or HTTP). ([OpenTelemetry][2])
* **Timeouts:** `OTEL_EXPORTER_OTLP_TIMEOUT` (and per-signal variants) set exporter wait time; too low → lots of failed exports under load. ([OpenTelemetry][2])

---

## 9.3 Batching: SDK-side vs Collector-side (both matter)

### 9.3.1 SDK-side batching (BatchSpanProcessor) — reduce per-span export overhead

OpenTelemetry recommends batch exporting for async runtimes (“more efficient” than exporting each span as it ends). ([Docs.rs][5])

**BatchSpanProcessor env knobs (standardized)**

* `OTEL_BSP_SCHEDULE_DELAY` (ms; default 5000)
* `OTEL_BSP_EXPORT_TIMEOUT` (ms; default 30000)
* `OTEL_BSP_MAX_QUEUE_SIZE` (default 2048)
* `OTEL_BSP_MAX_EXPORT_BATCH_SIZE` (default 512; must be ≤ queue size) ([OpenTelemetry][6])

**Backpressure behavior (what happens under overload)**

* When exporter throughput < span production rate, queues fill; implementations drop spans and may emit “queue full” diagnostics (opentelemetry-rust logs dropped spans “due to a queue being full”). ([GitHub][7])

**Tuning heuristics**

* If you see drops:

  * increase `OTEL_BSP_MAX_QUEUE_SIZE` (buys time)
  * decrease `OTEL_BSP_SCHEDULE_DELAY` (flush more frequently)
  * increase `OTEL_BSP_MAX_EXPORT_BATCH_SIZE` *only if* exporter/backend can handle larger payloads
  * raise `OTEL_BSP_EXPORT_TIMEOUT` if exports routinely time out ([OpenTelemetry][6])

---

### 9.3.2 Collector-side batching — stabilize export throughput to backends

Even if apps batch, you still want collector batching to:

* amortize backend request overhead
* smooth bursts from many services

**Processor ordering (important)**

* Upstream Collector guidance: put the **batch processor after `memory_limiter` and after any sampling processors**, because batching should happen after drops. ([GitHub][8])

**Batch config semantics**

* Collector batch `timeout` sends a batch “regardless of size”; `timeout=0` sends immediately. ([GitHub][9])

**Minimal collector skeleton (OTLP receiver + batch processor)**

```yaml
receivers:
  otlp:
    protocols:
      grpc: {}
      http: {}

processors:
  memory_limiter: {}
  batch:
    timeout: 5s

service:
  pipelines:
    traces:
      receivers: [otlp]
      processors: [memory_limiter, batch]
      exporters: [/* your exporter */]
```

Ordering rationale: memory + sampling → then batch. ([GitHub][8])

---

## 9.4 Resource/service attributes strategy (how your traces stay queryable)

### 9.4.1 “Service identity” (non-negotiable)

Set `service.name` consistently:

* `OTEL_SERVICE_NAME` sets `service.name`
* If `service.name` is also in `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_SERVICE_NAME` wins ([OpenTelemetry][10])

In Rust, you can also set it in code via `Resource::builder().with_service_name("...")` as shown in the OTel Rust exporter docs. ([OpenTelemetry][4])

### 9.4.2 Enrichment via `OTEL_RESOURCE_ATTRIBUTES`

* `OTEL_RESOURCE_ATTRIBUTES` is the cross-language way to attach resource attributes (points to Resource semantic conventions for the canonical keys). ([OpenTelemetry][10])

**Practical policy (agent-friendly)**

* Keep **resource attributes low-cardinality** (env, region, version, instance id).
* Put request-/query-specific high-cardinality values on **span attributes**, not the resource.

### 9.4.3 Span attribute naming conventions

* `tracing-opentelemetry` passes fields through, and explicitly supports semantic convention attribute names (e.g., `url.full`, `server.port`) as ordinary span fields. ([Docs.rs][1])

---

## 9.5 Exporter backpressure + sampling policies (trace volume control)

### 9.5.1 Head sampling (SDK-side)

Standard env knobs:

* `OTEL_TRACES_SAMPLER` (default `parentbased_always_on`)
* `OTEL_TRACES_SAMPLER_ARG` ([OpenTelemetry][10])

For distributed systems, parent-based sampling avoids inconsistent partial traces:

* OTel Trace SDK spec notes `TraceIdRatioBased` ignores parent sampling state and recommends using it as a delegate of `ParentBased` to respect parent decisions. ([OpenTelemetry][11])

### 9.5.2 Tail sampling (Collector-side)

If you tail-sample (keep “slow/error” traces at higher rates), you must ensure **all spans of a trace reach the same collector instance**, otherwise traces fragment and decisions are wrong. ([OpenTelemetry][12])
This implies trace-aware load balancing (often: agent tier → load-balancing exporter → sampling tier).

### 9.5.3 Datadog-specific routing choices (two common patterns)

* **OTel Collector → Datadog exporter**: Datadog explicitly supports and recommends Collector setups for advanced processing. ([Datadog Monitoring][13])
* **Direct to Datadog Agent (OTLP ingest)**: Agent can ingest OTLP traces/metrics (gRPC or HTTP), but Datadog notes the ingesting Agent is intended to be local to the generating host; don’t route from one host to an Agent on another host. ([Datadog Monitoring][14])

---

### 9.6 “Known-good” local smoke test (Jaeger all-in-one)

OTel Rust docs show Jaeger all-in-one with OTLP ports exposed (4317/4318) as a quick validation target. ([OpenTelemetry][4])

---

If you want, the next “drop-in” deep dive is: **10) End-to-end visibility to storage (instrumented-object-store)** because it’s where OTLP plumbing + attribute strategy immediately pays off (query spans parent storage spans cleanly).

[1]: https://docs.rs/tracing-opentelemetry "tracing_opentelemetry - Rust"
[2]: https://opentelemetry.io/docs/languages/sdk-configuration/otlp-exporter/ "OTLP Exporter Configuration | OpenTelemetry"
[3]: https://docs.rs/opentelemetry-otlp/latest/opentelemetry_otlp/ "opentelemetry_otlp - Rust"
[4]: https://opentelemetry.io/docs/languages/rust/exporters/ "Exporters | OpenTelemetry"
[5]: https://docs.rs/opentelemetry/latest/opentelemetry/trace/index.html?utm_source=chatgpt.com "opentelemetry::trace - Rust"
[6]: https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/?utm_source=chatgpt.com "Environment Variable Specification"
[7]: https://github.com/open-telemetry/opentelemetry-rust/blob/main/opentelemetry-sdk/src/trace/span_processor.rs?utm_source=chatgpt.com "span_processor.rs"
[8]: https://github.com/open-telemetry/opentelemetry-collector/blob/main/processor/batchprocessor/README.md?plain=1&utm_source=chatgpt.com "open-telemetry/opentelemetry-collector - processor"
[9]: https://github.com/open-telemetry/opentelemetry-collector/blob/main/processor/batchprocessor/config.go?utm_source=chatgpt.com "opentelemetry-collector/processor/batchprocessor/config. ..."
[10]: https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/ "Environment Variable Specification | OpenTelemetry"
[11]: https://opentelemetry.io/docs/specs/otel/trace/sdk/?utm_source=chatgpt.com "Tracing SDK"
[12]: https://opentelemetry.io/blog/2022/tail-sampling/?utm_source=chatgpt.com "Tail Sampling with OpenTelemetry: Why it's useful, how to ..."
[13]: https://docs.datadoghq.com/opentelemetry/setup/collector_exporter/?utm_source=chatgpt.com "Install and Configure the OpenTelemetry Collector"
[14]: https://docs.datadoghq.com/opentelemetry/setup/otlp_ingest_in_the_agent/ "OTLP Ingestion by the Datadog Agent"

## 10) End-to-end visibility to storage (`instrumented-object-store`)

### 10.1 What it is + integration boundary

`instrumented-object-store` wraps an `object_store::ObjectStore` and emits `tracing` spans for **all public storage operations** (get/put/list/etc.), recording metadata (path, content sizes) and errors; it’s explicitly designed to “work with OpenTelemetry for distributed tracing.” ([Docs.rs][1])

**Primary entrypoint**

```rust
pub fn instrument_object_store(
    store: Arc<dyn ObjectStore>,
    name: &str,
) -> Arc<dyn ObjectStore>
```

([Docs.rs][2])

**DataFusion boundary**

* Register the wrapped store with a URL prefix via `SessionContext::register_object_store(&Url, Arc<dyn ObjectStore>)` (“used with a specific URL prefix”). ([Docs.rs][3])

---

### 10.2 Syntax: create + register (minimal “works” wiring)

```rust
use std::sync::Arc;
use url::Url;

use datafusion::execution::context::SessionContext;
use instrumented_object_store::instrument_object_store;
use object_store::local::LocalFileSystem;

let store = Arc::new(LocalFileSystem::new());
let store = instrument_object_store(store, "local_fs"); // span-name prefix

let ctx = SessionContext::new();
ctx.register_object_store(&Url::parse("file://").unwrap(), store);
```

This is the crate’s own Getting Started pattern (including the “prefix for span names” semantics) and the DataFusion registration hook. ([Docs.rs][1])

**Watchout (prefix matching):** DataFusion selects the store by **URL prefix**, so your `Url` must match the scheme/prefix you use in table paths (e.g., `file://`, `s3://`, etc.). ([Docs.rs][3])

---

### 10.3 Span naming conventions (store prefix + op name)

#### 10.3.1 Exported span name is `otel.name = "{store}.{op}"`

Every instrumented method uses `#[instrument(skip_all, fields(...))]` and sets a **dynamic `otel.name`** like:

* `otel.name = format!("{}.get", self.name)`
* `otel.name = format!("{}.get_range", self.name)`
* `otel.name = format!("{}.put", self.name)`
* `otel.name = format!("{}.list", self.name)`
* etc. ([Docs.rs][4])

Because `tracing-opentelemetry` treats `otel.name` as “override the span name sent to OpenTelemetry exporters,” your backend typically shows span names like `local_fs.get_range` rather than the static Rust span name. ([Docs.rs][5])

#### 10.3.2 Operation suffix catalog (what you will see)

From the implementation, the op suffix set is (non-exhaustive but practically complete):

* Reads: `get`, `get_opts`, `get_range`, `get_ranges`, `head`
* Writes: `put`, `put_opts`
* Multipart: `put_multipart`, `put_multipart_opts`, `complete`, `abort` (but **not** per-part uploads; see watchouts)
* Listing: `list`, `list_with_offset`, `list_with_delimiter`
* Mutations: `delete` (incl `delete_stream`), `copy`, `rename`, `copy_if_not_exists`, `rename_if_not_exists` ([Docs.rs][4])

#### 10.3.3 Naming policy (so spans stay queryable)

* `name` should be **low-cardinality** (e.g., `s3`, `gcs`, `azure`, `local_fs`, or an internal alias like `warehouse`) and *not* include bucket names, object keys, or tenant IDs. The path already exists as an attribute; putting it in the span name destroys aggregation and search ergonomics.

---

### 10.4 Span fields: what gets recorded (paths, sizes, errors)

#### 10.4.1 Request-side fields (`object_store.*`)

Patterns you can key off:

* Location/path: `object_store.location = %location` (put/get/head/delete) ([Docs.rs][4])
* Prefix/offset for listing:

  * `object_store.prefix = …`
  * `object_store.offset = …` ([Docs.rs][4])
* Range reads:

  * `object_store.range = ?range`
  * `object_store.ranges = ?ranges` ([Docs.rs][4])
* Copy/rename:

  * `object_store.from = %from`
  * `object_store.to = %to` ([Docs.rs][4])
* Payload size (writes): `object_store.content_length = %payload.content_length()` ([Docs.rs][4])
* Options (where applicable): `object_store.options = ?options` / `?opts` ([Docs.rs][4])

`skip_all` is used so large arguments (payload bytes, option structs unless explicitly included, etc.) aren’t implicitly captured by `#[instrument]`. ([Docs.rs][4])

#### 10.4.2 Result-side fields (`object_store.result.*`) + error capture

A helper `instrument_result` inspects the `Result` and records either:

* On success: type-specific result metadata
* On error: `object_store.result.err = "<error string>"` ([Docs.rs][4])

Concrete result fields you’ll see:

* `object_store.result.meta` (debug of `ObjectMeta` / `GetResult.meta`)
* `object_store.result.range` (from `GetResult.range`)
* `object_store.result.content_length` (from `Bytes.len()` / sum of `Vec<Bytes>`)
* `object_store.result.object_count` (from `ListResult.objects.len()`)
* `object_store.result.e_tag`, `object_store.result.version` (from `PutResult`) ([Docs.rs][4])

---

### 10.5 Correlating storage spans under query/operator spans (context propagation)

#### 10.5.1 Why correlation “just works” when the current span is preserved

* `#[instrument]` creates a span as part of the method call; unless you explicitly override, it nests under the **current `tracing` span** active at that call site. ([Docs.rs][6])
* DataFusion-tracing’s operator spans run execution inside span scope, so any object-store calls done “during operator execution” naturally become children (e.g., `DataSourceExec` → `s3.get_range`). (This nesting depends on context not being lost across threads/tasks.)

#### 10.5.2 Streams + async: explicit propagation is required

Tokio tasks do **not** automatically inherit the current span unless you propagate it (e.g., `.in_current_span()` / `.instrument(Span::current())`). ([GitHub][7])

`instrumented-object-store` does this for stream-returning APIs:

* `list`, `list_with_offset`, `delete_stream` return a `BoxStream` and wrap it with `.in_current_span()` so polling the stream stays within the span context. ([Docs.rs][4])

#### 10.5.3 DataFusion task hopping: use JoinSetTracer-enabled builds

DataFusion introduced an API to propagate user-defined context across thread boundaries (JoinSetTracer), explicitly called out for tracing/logging, and mentions community crates like `datafusion-tracing` as integrated options. ([Apache DataFusion][8])

**Practical operational recipe**

* Enable **DataFusion execution tracing** (operator spans) + **rule tracing** (planner/optimizer spans) via `datafusion-tracing`.
* Register an **instrumented object store** in the same `SessionContext`.
* Ensure your runtime/subscriber exports `otel.name` and preserves context across spawned tasks (JoinSetTracer in DataFusion; `.in_current_span()` for streams; avoid custom spawns that drop span context).

---

### 10.6 Watchouts (high-signal pitfalls)

* **Multipart uploads:** `put_part` is explicitly **not traced** (“too many parts to trace”), so you’ll see `*.put_multipart` + `*.complete`/`*.abort`, but not per-part timings. ([Docs.rs][4])
* **`delete_stream` visibility:** it instruments the returned stream with `.in_current_span()` but doesn’t annotate per-item errors into `object_store.result.err` the way the async methods do (you’ll see stream errors as events/logs depending on subscriber, not necessarily as a span field). ([Docs.rs][4])
* **High-cardinality fields:** paths are recorded (`object_store.location`, etc.). Keep your exporter/backend attribute limits in mind; avoid adding dynamic identifiers to span *names* (use attributes).
* **“It doesn’t show up” debugging:** storage spans are `tracing` spans; they can be filtered out by subscriber level/target settings even if DataFusion spans are visible. ([Docs.rs][6])

[1]: https://docs.rs/instrumented-object-store "instrumented_object_store - Rust"
[2]: https://docs.rs/instrumented-object-store/latest/instrumented_object_store/fn.instrument_object_store.html "instrument_object_store in instrumented_object_store - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html?utm_source=chatgpt.com "SessionContext in datafusion::execution::context - Rust"
[4]: https://docs.rs/instrumented-object-store/latest/src/instrumented_object_store/instrumented_object_store.rs.html "instrumented_object_store.rs - source"
[5]: https://docs.rs/tracing-opentelemetry?utm_source=chatgpt.com "tracing_opentelemetry - Rust"
[6]: https://docs.rs/tracing?utm_source=chatgpt.com "tracing - Rust"
[7]: https://github.com/tokio-rs/tokio/discussions/6008?utm_source=chatgpt.com "Do tasks inherit tracing spans? #6008"
[8]: https://datafusion.apache.org/blog/output/2025/07/11/datafusion-47.0.0/?utm_source=chatgpt.com "Apache DataFusion 47.0.0 Released"

## 11) Composition rules + “don’t break other optimizer rules”

### 11.1 Physical optimizer rule ordering: **instrument last**

**Hard requirement (practical):** if you have *any* other `PhysicalOptimizerRule`s, register the DataFusion-tracing instrumentation rule **last**. The project docs call this out explicitly: last means you (a) capture the **final** optimized plan and (b) keep instrumentation “purely observational.” ([GitHub][1])

**Syntax (chaining)**

```rust
let session_state = SessionStateBuilder::new()
  .with_default_features()
  .with_physical_optimizer_rule(rule_a)
  .with_physical_optimizer_rule(rule_b)
  .with_physical_optimizer_rule(instrument_rule) // last
  .build();
```

Rationale: `with_physical_optimizer_rule(...)` appends to the end of the list. ([Docs.rs][2])

**Syntax (vector)**

```rust
builder.with_physical_optimizer_rules(vec![rule_a, rule_b, instrument_rule]);
```

This is the repo’s recommended “keep it last” pattern. ([GitHub][1])

**Why last matters (beyond “final plan”)**

* The instrumentation pass is a *tree rewrite* that wraps every node it sees **at that time** via a top-down traversal (`transform_down`). ([Docs.rs][3])
* If you run instrumentation **before** another optimizer rule, that later rule can **construct brand-new physical nodes** (common in physical optimizers) that never go through the instrumentation wrapper pass → you get “holes” (uninstrumented operators) in runtime spans. The instrumentation pass itself explicitly states it uses `transform_down` to cover “new nodes added by other optimizer rules” — meaning **rules that ran earlier**. ([Docs.rs][3])

---

### 11.2 What the instrumentation optimizer rule actually does (composition mechanics)

The instrumentation rule (`InstrumentRule`) implements `PhysicalOptimizerRule::optimize(...)` and:

1. **Enables an internal “I’m instrumenting now” context** using an RAII guard (`InternalOptimizerGuard`) backed by a thread-local boolean. ([Docs.rs][3])
2. Walks the plan with `plan.transform_down(...)` and wraps each node in `InstrumentedExec::new(...)`. ([Docs.rs][3])
3. Skips already-wrapped nodes using `InstrumentedExec::is_instrumented(plan.as_ref())` to avoid double wrapping. ([Docs.rs][3])

**Critical detail:** `InstrumentedExec::is_instrumented` relies on that internal context being active (it uses `plan.as_any().is::<InstrumentedExec>()`, which only “reveals” `InstrumentedExec` during the guard window). ([Docs.rs][4])

---

### 11.3 Wrapper transparency: “mostly invisible” + `as_any()` delegation

The repo states the design intent clearly:

* With the rule registered last, most optimizer rules never encounter `InstrumentedExec`.
* `InstrumentedExec` delegates `as_any()` to the inner plan for “transparent downcasting.” ([GitHub][1])

**The actual `as_any()` rule (important for diagnostics + custom tooling)**
`InstrumentedExec::as_any()` behaves differently depending on the internal guard flag:

* **During the instrumentation optimizer pass:** `as_any()` returns `self` so the instrumentation rule can detect already-instrumented nodes.
* **Otherwise:** `as_any()` delegates to `self.inner.as_any()` so callers can downcast to the underlying operator type as if the wrapper didn’t exist. ([Docs.rs][4])

**What sets that internal flag**

* A `thread_local!` boolean (`IS_INTERNAL_OPTIMIZER_CHECK`)
* Toggled by `InternalOptimizerGuard::new()` and restored on drop ([Docs.rs][5])

---

### 11.4 Interaction with `ExecutionPlan` methods (how instrumentation survives rewrites)

#### 11.4.1 Delegation baseline (preserve behavior)

`InstrumentedExec` delegates most `ExecutionPlan` methods straight to the inner plan (`schema`, `properties`, `name`, `children`, `metrics`, statistics accessors, ordering/distribution requirements, pushdown plumbing, etc.). ([Docs.rs][4])

This is the “don’t change semantics” backbone: you preserve the operator’s original schema, properties, and invariants while adding observation.

#### 11.4.2 Rewrap-on-return: methods that can produce a *new* plan

Any method that can return a modified plan is overridden to:

1. call the inner method
2. wrap the returned plan with `with_new_inner(...)` (which preserves the instrumentation configuration) ([Docs.rs][4])

**Canonical rewrap helpers**

* `with_new_inner(&self, inner: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan>`: creates a new `InstrumentedExec` with the same span factory + settings. ([Docs.rs][4])

**Rewrap list (these are the ones that matter in practice)**

* **Child replacement**

  * `with_new_children(self: Arc<Self>, children: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>>` → delegates and rewaps. ([Docs.rs][4])
  * Why it matters: DataFusion’s default patterns rebuild nodes during rewrites; you want the new instance to stay instrumented.

* **Repartitioning**

  * `repartitioned(&self, target_partitions, config) -> Result<Option<Arc<dyn ExecutionPlan>>>` → wraps `Some(new_inner)` and passes through `None`. ([Docs.rs][4])
  * Semantics: DataFusion uses `repartitioned` to increase parallelism when supported. ([Docs.rs][6])

* **Fetch / limit pushdown**

  * `with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>>` → wraps `Some(...)`. ([Docs.rs][4])

* **Projection swapping optimization**

  * `try_swapping_with_projection(&self, projection: &ProjectionExec) -> Result<Option<Arc<dyn ExecutionPlan>>>` → wraps `Some(...)`. ([Docs.rs][4])

* **Filter pushdown propagation**

  * `handle_child_pushdown_result(...) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>>`
  * It rewraps `updated_node` if present. ([Docs.rs][4])

* **Sort pushdown**

  * `try_pushdown_sort(...) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>>`
  * Wraps `Exact{inner}` and `Inexact{inner}`; passes through `Unsupported`. ([Docs.rs][4])

* **State reset + runtime state injection**

  * `reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>>` → wraps returned plan. ([Docs.rs][4])
  * `with_new_state(&self, state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>>` → wraps returned plan. ([Docs.rs][4])
  * Semantics: `reset_state` is used when plans need re-execution (e.g., recursive queries). ([Docs.rs][6])

**Net effect:** if a rewrite is expressed via `ExecutionPlan` methods (children replacement, pushdowns, repartitioning, etc.), instrumentation stays attached automatically.

---

### 11.5 Diagnostics patterns that depend on downcasting

#### 11.5.1 “Normal” diagnostics: downcast to the underlying operator (works)

Outside the instrumentation rule’s internal guard window, `InstrumentedExec::as_any()` delegates to the inner plan, enabling transparent `downcast_ref`. ([Docs.rs][4])

Example pattern:

```rust
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::sorts::sort::SortExec;

fn is_sort(plan: &dyn ExecutionPlan) -> bool {
    plan.as_any().is::<SortExec>()
}
```

This should behave as if the wrapper weren’t present (by design). ([Docs.rs][4])

#### 11.5.2 “Internal” diagnostics: detecting the wrapper itself is intentionally constrained

* `InstrumentedExec::is_instrumented(plan)` only works when the internal optimizer check flag is active (“relies on the internal optimization context being active in the current thread”). ([Docs.rs][4])
* That flag is set by `InternalOptimizerGuard` inside the instrumentation rule’s own `optimize` pass. ([Docs.rs][3])
* The wrapper is also described as intentionally private; supported surface is the optimizer rule + `ExecutionPlan` trait. ([GitHub][1])

**Implication:** don’t build external tooling that depends on “seeing InstrumentedExec” as a type. Treat instrumentation presence as a configuration fact (“we installed the rule”), not as something you discover by walking the plan type graph.

---

### 11.6 Composition watchouts (high-signal failure modes)

* **Wrong ordering = missing spans.** If instrumentation doesn’t run last, later rules can add new physical nodes that never get wrapped (because wrapping is a one-pass tree transform over the current plan). ([Docs.rs][3])
* **Rule chains are append-ordered.** `SessionStateBuilder::with_physical_optimizer_rule` appends to the end—so “last call wins for last position.” ([Docs.rs][2])
* **Wrapper detection is thread-local + scoped.** The “reveal wrapper type” behavior is gated by a thread-local boolean set by an RAII guard during the instrumentation rule’s synchronous `optimize` call. If you’re debugging wrapper visibility, that’s why it seems to “appear/disappear.” ([Docs.rs][5])
* **Downcast-based tooling is safe; pointer-identity tooling isn’t.** If your custom optimizer relies on `Arc::ptr_eq` / identity semantics of specific nodes, any wrapper layer (even transparent downcasting) can invalidate those assumptions. (That’s another reason to keep instrumentation observational by running it last.) ([GitHub][1])

[1]: https://github.com/datafusion-contrib/datafusion-tracing "GitHub - datafusion-contrib/datafusion-tracing: Integration of opentelemetry with the tracing crate"
[2]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionStateBuilder.html "SessionStateBuilder in datafusion::execution::session_state - Rust"
[3]: https://docs.rs/crate/datafusion-tracing/latest/source/src/exec_instrument_rule.rs "datafusion-tracing 52.0.0 - Docs.rs"
[4]: https://docs.rs/crate/datafusion-tracing/latest/source/src/instrumented_exec.rs "datafusion-tracing 52.0.0 - Docs.rs"
[5]: https://docs.rs/crate/datafusion-tracing/latest/source/src/utils.rs "datafusion-tracing 52.0.0 - Docs.rs"
[6]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html "ExecutionPlan in datafusion::physical_plan - Rust"
