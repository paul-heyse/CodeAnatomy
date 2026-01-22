
After unzipping and walking `src/`, you’re already leveraging a *lot* of the “hard stuff”:

* **DataFusion**: centralized `DataFusionRuntimeProfile`, config presets (stats, metadata/listing caches, parquet pushdown + predicate cache), Substrait capture/replay/validation, prepared statements, schema registry + `information_schema` introspection, `df.cache()`-style reuse, Delta registration via TableProvider, and a Rust extension shim (`rust/datafusion_ext`).
* **Ibis**: DataFusion backend via a shared `SessionContext`, builtin UDF stubs, Arrow batch export (`to_pyarrow_batches`), and SQLGlot-based compilation checkpoints.
* **SQLGlot**: qualification, pushdown rewrites, type annotation, canonicalization, lineage, and policy hashing.
* **Delta Lake**: DeltaTable open/time-travel, write policies, commit metadata, vacuum/checkpoint, DataFusion inserts (when possible), and CDF provider wiring.

So the biggest wins now are “finish the last 20%” of the extension points you’ve already scaffolded, and push more work into streaming + engine-native observability.

## DataFusion: highest-ROI additions

### 1) Turn on real observability (metrics + tracing), not just EXPLAIN strings

DataFusion operators collect detailed runtime metrics; you can access them via `EXPLAIN ANALYZE` *and* programmatically via `ExecutionPlan::metrics`. ([Apache DataFusion][1])
You also have a ready-made ecosystem extension (`datafusion-tracing`) that integrates DataFusion execution with `tracing` + OpenTelemetry for query debugging and partial-result previews. ([GitHub][2])

**Why you’ll feel it immediately:** you’re compiling many “rulepack” queries; when a single join explodes or spills, you want *machine-readable* operator counters/timers keyed by `{rule_name, output_dataset}` (you already carry execution labels in your runtime).

**Where to wire it (in your code):**

* `src/datafusion_engine/runtime.py`: you already have `enable_metrics`, `enable_tracing`, `metrics_collector`, `tracing_collector`, and `DiagnosticsSink` plumbing—this is the switchboard.
* Add a Rust-side hook in `rust/datafusion_ext` (or a sibling crate) to expose either:

  * (A) `ExecutionPlan::metrics` snapshots per executed plan, or
  * (B) `datafusion-tracing` spans exported as OTLP/JSON, then surfaced back through `DiagnosticsSink`.

### 2) Standardize “big outputs” on Arrow C Stream streaming, not `collect()`

DataFusion DataFrames implement `__arrow_c_stream__`, which executes lazily and yields record batches incrementally (zero-copy into Arrow-capable consumers). ([Apache DataFusion][3])

**Why it matters here:** a lot of your intermediate / final artifacts are Arrow/Parquet/Delta-bound. Pushing results through `__arrow_c_stream__` → `pyarrow.RecordBatchReader.from_stream(...)` → `pyarrow.dataset.write_dataset(...)` gives you “streaming-first materialization” and avoids accidental full materialization spikes.

**Where to wire it:**

* You already have the adapter in `src/arrowdsl/core/streaming.py` and multiple places that detect `__arrow_c_stream__`. The missing step is to *prefer it* in your materializers/exports for any non-trivial dataset.

### 3) Actually use DataFusion’s SQL extension points for domain operators + named args

DataFusion’s library guide explicitly calls out using **ExprPlanner** to customize how SQL expressions become logical expressions (custom operators, field access patterns, etc.). ([Apache DataFusion][4])
You’ve already created an “expr planner” control plane in `DataFusionRuntimeProfile` (`enable_expr_planners`, `expr_planner_names`), but your Rust extension currently doesn’t implement `install_expr_planners`.

**Concrete payoff:** stable “domain operators” (e.g., `span_contains(a_span, b_span)` or `cpg_score(...)`) that the optimizer can recognize and push down / rewrite consistently, instead of encoding everything as fragile SQL patterns.

### 4) Finish FunctionFactory as a first-class rule primitive system

DataFusion’s **FunctionFactory** API is designed to handle `CREATE FUNCTION` statements and register UDF/UDAF/UDWF dynamically. ([Apache DataFusion][5])
You already have `src/datafusion_engine/function_factory.py` + a Rust `install_function_factory` implementation—nice.

**But there’s a critical mismatch to fix:** your Python default policy includes primitives like `prefixed_hash64` and `stable_id`, while the Rust `build_udf` match arm doesn’t implement them. That means “native FunctionFactory install” will fail as soon as you ship the extension broadly. Fixing this gives you:

* consistent availability of primitives across Ibis/SQL/DataFusion,
* ability to prefer *named arguments* in function signatures when supported,
* and a clean place to add more “CPG domain” functions without Python-loop UDF cost.

## Ibis: the biggest missing lever

### 1) Add an Ibis → Substrait lane (skip SQL text for many pipelines)

Ibis can produce **Substrait plans** from Ibis table expressions via `ibis-substrait`. ([Ibis][6])
Given you already do Substrait capture/replay/validation on the DataFusion side, this is the cleanest way to:

* avoid SQL string generation + parsing roundtrips,
* make plan caching more stable (plan bytes become the artifact),
* and use Substrait validation as a “compiler correctness gate” across engines.

**Where to wire it:**

* your existing “plan artifact” machinery in `src/datafusion_engine/bridge.py` already stores substrait bytes and can replay them.
* add an optional compilation path: `ibis_expr -> substrait bytes -> DataFusion Consumer -> DataFrame`, alongside your current `ibis_expr -> sqlglot -> sql -> DataFusion` path.

## SQLGlot: what’s left that’s genuinely additive

You’re already deep into SQLGlot’s optimizer and lineage. The best incremental wins now are *testing + safety*:

### 1) Use SQLGlot’s optimizer docs as the contract for “schema-aware rewrite gating”

SQLGlot’s optimizer is explicitly schema-aware (`optimize(expression, schema=...)`) and type annotation is a first-class phase. ([SqlGlot][7])
You already pass schema maps through your normalization pipeline; the next step is to treat “schema-aware optimization” as a **hard preflight** for any SQL ingress (including LLM-authored SQL), and surface failures as structured diagnostics (not runtime DataFusion errors).

### 2) Add a “rewrite regression harness” using SQLGlot’s own Python engine for tiny fixtures

SQLGlot includes a Python SQL engine (useful for validating rewrite semantics on small in-memory fixtures). ([GitHub][8])
For you, that’s a cheap way to unit-test “policy rewrites” without spinning DataFusion and without involving parquet/metadata. It’s not a performance path—just a correctness harness.

## Delta Lake: the strongest additions for your incremental story

### 1) Actually exploit CDF for incremental rebuilds (replace anti-join diffs where possible)

Delta change data feed is explicitly designed to track row-level changes between versions. ([Delta Lake][9])
You already enable `delta.enableChangeDataFeed` by default in your Delta write helpers and you already have a **CDF TableProvider registration** path in `registry_bridge.py`.

**Concrete payoff:** many “incremental” diffs can become “read CDF between versions” rather than “read prev + read curr + anti-join,” which is a big I/O and planning reduction when tables grow.

### 2) Expose the remaining DeltaScanConfig knobs you’re not using

Delta’s DataFusion scan config includes fields like `wrap_partition_values` (dictionary-encode partition values) and `schema_force_view_types`. ([Docs.rs][10])
Right now:

* your Python `DeltaScanOptions` doesn’t expose `wrap_partition_values`, and
* your Rust `delta_table_provider` pyfunction doesn’t accept `schema_force_view_types` even though your Python call site passes it (so the “native provider” path would TypeError if the extension is installed).

Fixing this gives you:

* smaller memory footprint for partition columns (`wrap_partition_values`),
* consistent Arrow view-type behavior when you want it (`schema_force_view_types`),
* and a working “native delta provider” path that won’t silently fall back.

### 3) Use `get_add_actions()` as an “external index feed” for pruning and provenance

`DeltaTable.get_add_actions()` exposes the active file set and associated metadata parsed from the log. ([Delta][11])
This is ideal for:

* building a “file-level pruning index” keyed by partition values / stats / file path,
* driving selective refresh (changed files only),
* and attaching provenance (file column) without extra listing calls.

### 4) Optional: QueryBuilder as a lightweight “Delta-native SQL lane”

delta-rs has a DataFusion-embedded SQL surface (`QueryBuilder`) that can execute SQL over registered Delta tables and return record batches; it’s discussed by maintainers/users and shows up in downstream performance investigations. ([GitHub][12])
I’d treat this as an *optional* lane for tooling and debugging (not your primary engine path), but it’s useful when you want “query delta without depending on datafusion-python packaging.”

---

## If you want a tight priority order

1. **Fix the two Rust/Python signature mismatches** (Delta provider args; FunctionFactory primitive set).
2. **Enable DataFusion metrics/tracing** and emit structured telemetry per rule execution. ([Apache DataFusion][1])
3. **Make Arrow C Stream streaming the default materialization boundary** for big outputs. ([Apache DataFusion][3])
4. **Add Ibis→Substrait compilation lane** and reuse your existing Substrait artifact pipeline. ([Ibis][6])
5. **Use Delta CDF for incremental diffs** wherever you’re currently doing “prev vs curr anti-join.” ([Delta][13])

[1]: https://datafusion.apache.org/user-guide/explain-usage.html?utm_source=chatgpt.com "Reading Explain Plans — Apache DataFusion documentation"
[2]: https://github.com/datafusion-contrib/datafusion-tracing?utm_source=chatgpt.com "datafusion-contrib/datafusion-tracing: Integration of ..."
[3]: https://datafusion.apache.org/python/user-guide/io/arrow.html?utm_source=chatgpt.com "Apache Arrow DataFusion documentation"
[4]: https://datafusion.apache.org/library-user-guide/extending-sql.html?utm_source=chatgpt.com "Extending SQL Syntax — Apache DataFusion documentation"
[5]: https://datafusion.apache.org/blog/2024/07/24/datafusion-40.0.0/?utm_source=chatgpt.com "Apache DataFusion 40.0.0 Released"
[6]: https://ibis-project.org/posts/ibis_substrait_to_duckdb/?utm_source=chatgpt.com "Ibis + Substrait + DuckDB"
[7]: https://sqlglot.com/sqlglot/optimizer/optimizer.html?utm_source=chatgpt.com "sqlglot.optimizer.optimizer API documentation"
[8]: https://github.com/tobymao/sqlglot/blob/main/posts/python_sql_engine.md?utm_source=chatgpt.com "sqlglot/posts/python_sql_engine.md at main"
[9]: https://docs.delta.io/delta-change-data-feed/?utm_source=chatgpt.com "Change data feed"
[10]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaScanConfig.html?utm_source=chatgpt.com "DeltaScanConfig in deltalake::delta_datafusion - Rust"
[11]: https://delta-io.github.io/delta-rs/python/api_reference.html?utm_source=chatgpt.com "API Reference — delta-rs documentation"
[12]: https://github.com/delta-io/delta-rs/issues/3076?utm_source=chatgpt.com "user friendly methods for Querybuilder , show and sql ..."
[13]: https://delta-io.github.io/delta-rs/usage/read-cdf/?utm_source=chatgpt.com "Reading Change Data - Delta Lake Documentation"


Below is a “drop-in architecture sketch” for each recommendation, **grounded in your current `src/` layout** (notably `src/datafusion_engine/runtime.py`, `src/datafusion_engine/bridge.py`, `src/datafusion_engine/expr_planner.py`, `src/datafusion_engine/function_factory.py`, `src/datafusion_engine/registry_bridge.py`, `src/storage/deltalake/*`, `src/sqlglot_tools/*`, `src/obs/*`, `src/incremental/*`) and your existing Rust extension at `rust/datafusion_ext/src/lib.rs`.

I’ll structure each section as:

* **What the feature gives you (deep dive)**
* **New/changed architecture pattern**
* **Representative code snippets (Python + Rust where relevant)**
* **Tests / failure modes / perf notes**

---

## 1) DataFusion observability: metrics + tracing that actually help you debug “why this plan blew up”

### What the feature gives you

**DataFusion collects per-operator runtime metrics** (rows, batches, time, spills, etc.). You can view them via `EXPLAIN ANALYZE`, and *also* access them programmatically via `ExecutionPlan::metrics`. ([Apache DataFusion][1])

You also have the (very real) “I want results + metrics without running twice” tension: `EXPLAIN ANALYZE` is its own execution path, while `collect()` runs the plan normally. There isn’t a simple “run once, return both result and explain metrics” high-level API in the typical surfaces, which is why the `ExecutionPlan::metrics` approach matters for “single execution” capture. ([GitHub][2])

For tracing, **datafusion-tracing** instruments query lifecycle and execution using `tracing` + OpenTelemetry, can attach native metrics, and can preview partial results during debugging. ([Docs.rs][3])

### Architecture pattern

You already have the right “wiring points” in `DataFusionRuntimeProfile`:

* `enable_metrics`, `metrics_collector`
* `enable_tracing`, `tracing_hook`, `tracing_collector`
* `diagnostics_sink` to persist artifacts/events
* `capture_explain` and `plan_collector` for plan artifacts

**The missing piece is a first-class “query run envelope”** that:

1. starts a trace/span with stable IDs,
2. records SQL/plan artifacts,
3. executes (possibly streaming),
4. captures metrics/tracing correlation IDs and stores them into your diagnostics tables.

### Code: Python “run envelope” + OTel wiring (new module)

Create something like `src/obs/datafusion_runs.py`:

```python
# src/obs/datafusion_runs.py
from __future__ import annotations

import time
import uuid
from dataclasses import dataclass
from typing import Any, Mapping

from opentelemetry import trace
from opentelemetry.trace import Span

from datafusion_engine.runtime import DiagnosticsSink

@dataclass(frozen=True)
class DataFusionRun:
    run_id: str
    label: str
    start_unix_ms: int
    trace_id: str | None

def _trace_id_hex(span: Span) -> str | None:
    ctx = span.get_span_context()
    if not ctx or not ctx.trace_id:
        return None
    return f"{ctx.trace_id:032x}"

def start_run(
    *,
    label: str,
    sink: DiagnosticsSink | None,
    attrs: Mapping[str, Any] | None = None,
) -> tuple[DataFusionRun, Span]:
    tracer = trace.get_tracer("codeanatomy.datafusion")
    run_id = str(uuid.uuid4())
    start_ms = int(time.time() * 1000)

    span = tracer.start_span(
        "datafusion.query",
        attributes={
            "run_id": run_id,
            "label": label,
            **(dict(attrs) if attrs else {}),
        },
    )
    run = DataFusionRun(
        run_id=run_id,
        label=label,
        start_unix_ms=start_ms,
        trace_id=_trace_id_hex(span),
    )
    if sink is not None:
        sink.record_artifact(
            "datafusion_run_started_v1",
            {
                "run_id": run.run_id,
                "label": run.label,
                "start_unix_ms": run.start_unix_ms,
                "trace_id": run.trace_id,
            },
        )
    return run, span

def finish_run(
    *,
    run: DataFusionRun,
    span: Span,
    sink: DiagnosticsSink | None,
    status: str,
    extra: Mapping[str, Any] | None = None,
) -> None:
    end_ms = int(time.time() * 1000)
    span.set_attribute("status", status)
    span.end()

    if sink is not None:
        sink.record_artifact(
            "datafusion_run_finished_v1",
            {
                "run_id": run.run_id,
                "label": run.label,
                "status": status,
                "start_unix_ms": run.start_unix_ms,
                "end_unix_ms": end_ms,
                "duration_ms": end_ms - run.start_unix_ms,
                **(dict(extra) if extra else {}),
            },
        )
```

And a basic OTLP tracer setup (e.g. `src/obs/otel.py`):

```python
# src/obs/otel.py
from __future__ import annotations

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# choose http/proto or grpc exporter depending on your infra
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

def configure_otlp_tracing(*, service_name: str, endpoint: str) -> None:
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)
```

### Code: integrate into your existing runtime profile

Your `DataFusionRuntimeProfile._install_tracing()` currently calls `tracing_hook()` without passing the `SessionContext`. If you want DataFusion-native instrumentation (e.g., installing optimizer rules / function factory / expr planners that depend on ctx), **pass `ctx`**.

Representative patch to `src/datafusion_engine/runtime.py`:

```python
# src/datafusion_engine/runtime.py (representative change)

# 1) change typing:
# tracing_hook: Callable[[SessionContext], None] | None = None

def session_context(self) -> SessionContext:
    ...
    self._install_tracing(ctx)  # pass ctx
    ...

def _install_tracing(self, ctx: SessionContext) -> None:
    if not self.enable_tracing:
        return
    if self.tracing_hook is None:
        raise ValueError("Tracing enabled but tracing_hook is not set.")
    self.tracing_hook(ctx)
```

Then wire it:

```python
from obs.otel import configure_otlp_tracing
from obs.diagnostics import DiagnosticsCollector
from datafusion_engine.runtime import DataFusionRuntimeProfile

sink = DiagnosticsCollector()

profile = DataFusionRuntimeProfile(
    enable_tracing=True,
    tracing_hook=lambda ctx: configure_otlp_tracing(
        service_name="codeanatomy",
        endpoint="http://localhost:4318/v1/traces",
    ),
    diagnostics_sink=sink,
)
ctx = profile.session_context()
```

### DataFusion-native execution tracing (Rust, optional but high ROI)

If you want “real plan/operator spans” and per-operator metric capture, integrate **datafusion-tracing** at the Rust `SessionStateBuilder` layer (it instruments physical optimizer rules, plus analyzer/logical/physical optimizer rulechains). ([Docs.rs][3])

Representative Rust (this is the upstream pattern, adapted to “return a Python SessionContext” you can embed into your extension crate):

```rust
// rust/datafusion_ext/src/tracing_ctx.rs (sketch)
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::*;
use datafusion_tracing::{
    instrument_rules_with_info_spans, instrument_with_info_spans,
    pretty_format_compact_batch, InstrumentationOptions, RuleInstrumentationOptions,
};
use std::sync::Arc;
use tracing::field;

pub fn make_instrumented_state() -> datafusion::execution::session_state::SessionState {
    let exec_options = InstrumentationOptions::builder()
        .record_metrics(true)
        .preview_limit(5)
        .preview_fn(Arc::new(|batch| {
            pretty_format_compact_batch(batch, 64, 3, 10).map(|fmt| fmt.to_string())
        }))
        .add_custom_field("env", "dev")
        .build();

    let instrument_rule = instrument_with_info_spans!(
        options: exec_options,
        env = field::Empty,
    );

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rule(instrument_rule)
        .build();

    let rule_options = RuleInstrumentationOptions::full().with_plan_diff();
    instrument_rules_with_info_spans!(options: rule_options, state: state)
}
```

That’s the exact technique the project docs show: configure instrumentation, attach as optimizer rule, and instrument rule stages. ([Docs.rs][3])

**Why this matters for you:** your pipeline is “lots of transformations + lots of joins on nested structs”. Having “plan diff” + per-operator timing makes regressions and optimizer changes obvious.

### Tests / failure modes / perf notes

* **Don’t always enable heavy tracing**; make it profile-based (you already have profiles).
* `EXPLAIN ANALYZE` is useful but can be “double-run” if you also execute results. Use it selectively, or use a Rust-level metrics collector via `ExecutionPlan::metrics`. ([Apache DataFusion][1])
* Store *correlation IDs* (run_id, trace_id) in your own delta-backed `obs/*` datasets so your later analysis can join to OTLP/Jaeger/Datadog.

---

## 2) Streaming as the default boundary: Arrow C Stream → RecordBatchReader everywhere

### What the feature gives you

DataFusion Python DataFrames implement `__arrow_c_stream__`, enabling **zero-copy, lazy streaming** into Arrow consumers: the result set is not fully materialized in memory; batches are produced on demand. ([Apache DataFusion][4])

You already use this in `datafusion_to_reader`:

```py
reader = pa.RecordBatchReader.from_stream(df)
```

So the “new architecture” is: **enforce this as the boundary for any “large result” paths** (writers, delta materializers, downstream transforms), and only call `to_arrow_table()` when you know the result is small.

### Architecture pattern

Create “materializers” that accept:

* `pa.RecordBatchReader` (stream)
* optionally, “partitioned readers” (`execute_stream_partitioned`) for better parallel IO

and never require a `pa.Table` except for small metadata artifacts.

### Code: an enforced streaming materializer wrapper

```python
# src/datafusion_engine/materialize.py
from __future__ import annotations
import pyarrow as pa
from datafusion.dataframe import DataFrame
from datafusion_engine.bridge import datafusion_to_reader, datafusion_partitioned_readers

def df_to_stream(df: DataFrame) -> pa.RecordBatchReader:
    # your existing helper already tries Arrow C Stream first
    return datafusion_to_reader(df)

def df_to_partitioned_streams(df: DataFrame) -> list[pa.RecordBatchReader]:
    return datafusion_partitioned_readers(df)

def stream_row_count(reader: pa.RecordBatchReader) -> int:
    n = 0
    for b in reader:
        n += b.num_rows
    return n
```

### Tests / perf notes

* Add a “contract test” that validates no path uses `to_arrow_table()` for outputs above a threshold row count.
* Ensure your batch sizing (`DataFusionRuntimeProfile.batch_size`) is aligned with downstream writer chunk sizes.

---

## 3) ExprPlanner: domain operators + stable syntax for nested access / named args

### What the feature gives you

`ExprPlanner` is DataFusion’s extension point to customize how SQL AST expressions become DataFusion logical expressions—especially for **custom operators** (e.g., `->`, `@>`) and custom field access behavior. ([Apache DataFusion][5])

DataFusion’s SQL extension docs explicitly call out:

* you register planners with `ctx.register_expr_planner()`,
* planners are chained with precedence (“last registered wins”). ([Apache DataFusion][5])

Named arguments are controlled by function signatures: `Signature` can include parameter names and enable `func(a => 1, b => 2)` style calls. ([Docs.rs][6])

### Architecture pattern

You already have:

* `src/datafusion_engine/expr_planner.py` (payload + installer), **but your Rust extension does not implement `install_expr_planners`**.

So implement it and add at least one real planner: **a domain planner** that:

* maps one or two “syntactic sugar” operators into canonical functions (your own Rust UDFs),
* optionally normalizes nested path access.

### Code: Rust `install_expr_planners` + domain planner (skeleton)

Add to `rust/datafusion_ext/src/lib.rs`:

```rust
use datafusion::common::Result;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion_expr::{Expr, Operator};
use sqlparser::ast::BinaryOperator;
use std::sync::Arc;

#[derive(Debug)]
struct CodeAnatomyDomainPlanner;

impl ExprPlanner for CodeAnatomyDomainPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &datafusion_common::DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        // Example: map a custom binary operator to something DataFusion understands
        // e.g. using postgres dialect to parse `->` and then rewriting it.
        match &expr.op {
            BinaryOperator::Arrow => {
                // Example: treat `a -> b` as string concat or as a domain function
                // Here: a || b
                Ok(PlannerResult::Planned(Expr::BinaryExpr(
                    datafusion_expr::BinaryExpr {
                        left: Box::new(expr.left.clone()),
                        op: Operator::StringConcat,
                        right: Box::new(expr.right.clone()),
                    }.into()
                )))
            }
            _ => Ok(PlannerResult::Original(expr)),
        }
    }
}

#[pyfunction]
fn install_expr_planners(ctx: PyRef<PySessionContext>, planner_names: Vec<String>) -> PyResult<()> {
    for name in planner_names {
        match name.as_str() {
            "codeanatomy_domain" => {
                ctx.ctx
                    .register_expr_planner(Arc::new(CodeAnatomyDomainPlanner))
                    .map_err(|e| PyRuntimeError::new_err(format!("register_expr_planner failed: {e}")))?;
            }
            other => {
                return Err(PyValueError::new_err(format!("Unknown ExprPlanner: {other}")));
            }
        }
    }
    Ok(())
}
```

This aligns with DataFusion’s documented registration model. ([Apache DataFusion][5])

### Code: Python wiring (your existing installer now works)

```python
# somewhere in profile config
profile = DataFusionRuntimeProfile(
    enable_expr_planners=True,
    expr_planner_names=("codeanatomy_domain",),
)
ctx = profile.session_context()

# make sure dialect allows the operator syntax you want:
ctx.sql("SET datafusion.sql_parser.dialect = 'postgres'").collect()
```

### Tests / failure modes / perf notes

* Ensure SQL dialect supports the tokens you want (e.g. Postgres operator parsing).
* Add “golden SQL” tests: input SQL with custom ops → plan artifacts include rewritten expressions.
* Beware: ExprPlanner changes happen *during SQL→LogicalPlan planning*, so they affect optimizer reasoning—good, but test carefully.

---

## 4) “FunctionFactory” done right: fix your current mismatch + optionally support CREATE FUNCTION

### What the feature gives you

DataFusion has a **FunctionFactory** interface to handle `CREATE FUNCTION` statements; DataFusion parses `CREATE FUNCTION ...` into `CreateFunction` structs and asks the factory to create/register UDFs/UDAFs/UDWFs dynamically. ([Docs.rs][7])

You don’t strictly need `CREATE FUNCTION` for your current system (you can register UDFs at startup), but:

* it makes the CLI/service SQL surface much more powerful,
* it enables “on-demand install” of functions based on policies.

Separately, you have a **hard bug** right now:

* Python policy declares `prefixed_hash64` and `stable_id` primitives,
* Rust `build_udf` does not implement them, so native install will fail at runtime.

### Architecture pattern

Do both:

1. **Make your current “policy-driven UDF registration” correct and complete**
2. Optionally add “true FunctionFactory” for `CREATE FUNCTION` flows when you want it

### Code: fix the primitive mismatch in your Rust extension

In `rust/datafusion_ext/src/lib.rs`, extend `build_udf`:

```rust
fn build_udf(primitive: &RulePrimitive, prefer_named: bool) -> Result<ScalarUDF> {
    let signature = primitive_signature(primitive, prefer_named)?;
    match primitive.name.as_str() {
        "cpg_score" => Ok(ScalarUDF::from(Arc::new(CpgScoreUdf { signature }))),
        "stable_hash64" => Ok(ScalarUDF::from(Arc::new(StableHash64Udf { signature }))),
        "stable_hash128" => Ok(ScalarUDF::from(Arc::new(StableHash128Udf { signature }))),
        "prefixed_hash64" => Ok(ScalarUDF::from(Arc::new(PrefixedHash64Udf { signature }))),
        "stable_id" => Ok(ScalarUDF::from(Arc::new(StableIdUdf { signature }))),
        "position_encoding_norm" => Ok(ScalarUDF::from(Arc::new(PositionEncodingUdf { signature }))),
        "col_to_byte" => Ok(ScalarUDF::from(Arc::new(ColToByteUdf { signature }))),
        name => Err(DataFusionError::Plan(format!("Unsupported rule primitive: {name}"))),
    }
}
```

And implement them (representative):

```rust
#[derive(Debug)]
struct PrefixedHash64Udf { signature: Signature }

impl ScalarUDFImpl for PrefixedHash64Udf {
    fn as_any(&self) -> &dyn Any { self }
    fn name(&self) -> &str { "prefixed_hash64" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke(&self, args: &ScalarFunctionArgs) -> Result<ColumnarValue> {
        // args: prefix, value
        let prefix = args.args[0].clone().into_array(args.number_rows)?;
        let value  = args.args[1].clone().into_array(args.number_rows)?;
        // build output strings; (vectorization omitted here for brevity)
        // output[i] = format!("{prefix}{hash64(value)}")
        ...
    }
}
```

### Code: named args support (you already do the important part)

Your `primitive_signature()` calls `signature.with_parameter_names(...)`. That is the mechanism DataFusion uses to enable named argument notation. ([Docs.rs][6])
So once your primitives exist, the user can call:

```sql
SELECT stable_hash64(value => some_col)
```

### Optional: true `CREATE FUNCTION` via FunctionFactory

If you want to support:

```sql
CREATE FUNCTION stable_hash64 AS 'codeanatomy.stable_hash64';
```

you set a function factory on the SessionContext (there’s a `with_function_factory` API). ([Docs.rs][8])
Your factory can interpret the `CreateFunction` struct and call `ctx.register_udf(...)` accordingly.

---

## 5) Ibis → Substrait lane: skip SQL strings for many plans

### What the feature gives you

`ibis-substrait` compiles an Ibis expression into a Substrait plan (`SubstraitCompiler().compile(expr)`), producing a protobuf plan. ([GitHub][9])
Ibis explicitly supports producing Substrait plans (and describes why this matters vs SQL). ([Ibis][10])

You already have:

* DataFusion Substrait serde + consumer in `datafusion_engine/bridge.py`,
* `replay_substrait_bytes(ctx, plan_bytes)` that deserializes bytes and rehydrates a DataFrame.

So the “new architecture” is a **dual-lane compiler**:

* Lane A: Ibis → SQLGlot → SQL → DataFusion (current)
* Lane B: Ibis → Substrait bytes → DataFusion consumer (new)
* with fallback from B→A when unsupported

### Code: new bridge module

```python
# src/ibis_engine/substrait_bridge.py
from __future__ import annotations

from ibis.expr.types import Table as IbisTable
from ibis_substrait.compiler.core import SubstraitCompiler

def ibis_to_substrait_bytes(expr: IbisTable) -> bytes:
    compiler = SubstraitCompiler()
    plan = compiler.compile(expr)  # protobuf
    return plan.SerializeToString()
```

### Code: integrate into your existing DataFusion bridge

```python
# src/datafusion_engine/bridge.py (representative new helper)
from ibis_engine.substrait_bridge import ibis_to_substrait_bytes
from datafusion_engine.bridge import replay_substrait_bytes, ibis_to_datafusion

def ibis_to_datafusion_dual_lane(expr, *, backend, ctx, options=None):
    try:
        plan_bytes = ibis_to_substrait_bytes(expr)
        return replay_substrait_bytes(ctx, plan_bytes)
    except Exception:
        # fallback to your existing SQLGlot lane
        return ibis_to_datafusion(expr, backend=backend, ctx=ctx, options=options)
```

### Tests / failure modes / perf notes

* **Function extension mapping**: Substrait portability depends on function IDs/extensions; expect some Ibis ops to fail for DataFusion and require fallback.
* **Plan caching becomes easier**: hash the Substrait bytes and use that as the canonical plan artifact key.

---

## 6) Delta Change Data Feed (CDF): make incremental rebuilds cheap and exact

### What the feature gives you

In delta-rs Python, `DeltaTable.load_cdf(...)` returns a **RecordBatchReader** stream of changes. ([Delta][11])
CDF requires `delta.enableChangeDataFeed=true` when creating the table. ([Delta][12]) (You already set this by default in `DEFAULT_DELTA_FEATURE_PROPERTIES`.)

CDF emits row-level changes with commit metadata (version/timestamp + change type). This is the correct primitive for:

* “update only what changed”
* auditability and lineage
* avoiding full-table anti-joins

### Architecture pattern

You already have `register_delta_cdf_df(...)` that can register a CDF provider as a DataFusion table (via `DeltaTable.cdf_table_provider` when available). So you can do:

* CDF → DataFusion → joins/filters → write updated artifacts

The missing “system” piece is a **cursor** (last processed version per dataset) and a **fanout strategy** (which downstream tables become invalid).

### Code: CDF cursor table + reader

```python
# src/incremental/delta_cdf.py
from __future__ import annotations

from dataclasses import dataclass
import pyarrow as pa
from deltalake import DeltaTable

@dataclass(frozen=True)
class CdfCursor:
    dataset: str
    last_version: int

def read_cdf_since(
    *,
    table_path: str,
    cursor: CdfCursor,
    storage_options: dict[str, str] | None = None,
) -> pa.RecordBatchReader:
    dt = DeltaTable(table_path, storage_options=storage_options)
    # delta-rs API: load_cdf(...) -> RecordBatchReader
    return dt.load_cdf(starting_version=cursor.last_version + 1)  # type: ignore
```

### Code: “changed keys” extraction + downstream invalidation

```python
# src/incremental/delta_cdf.py
def changed_primary_keys(
    reader: pa.RecordBatchReader,
    *,
    key_cols: list[str],
) -> pa.Array:
    # Build a dictionary of unique keys; simplistic version shown.
    keys = []
    for b in reader:
        t = pa.Table.from_batches([b])
        keys.append(t.select(key_cols))
    if not keys:
        return pa.array([], type=pa.struct([pa.field(k, pa.string()) for k in key_cols]))
    all_keys = pa.concat_tables(keys, promote_options="default")
    return all_keys.combine_chunks().to_struct_array()
```

### CDF inside DataFusion (using your existing registry bridge)

```python
from datafusion_engine.registry_bridge import register_delta_cdf_df
from storage.deltalake.delta import DeltaCdfOptions
from datafusion_engine.runtime import DataFusionRuntimeProfile

profile = DataFusionRuntimeProfile(diagnostics_sink=sink)
ctx = profile.session_context()

cdf_df = register_delta_cdf_df(
    ctx,
    name="ast_files_cdf",
    path="/data/ast_files_v1_delta",
    options=DeltaCdfRegistrationOptions(
        cdf_options=DeltaCdfOptions(starting_version=123),
        runtime_profile=profile,
    ),
)

# Now CDF is queryable:
df = ctx.sql("""
SELECT _commit_version, _change_type, repo, path, file_id
FROM ast_files_cdf
WHERE _change_type IN ('insert', 'update_postimage', 'delete')
""")
```

### Tests / failure modes / perf notes

* **Retention**: if old versions are vacuumed, CDF queries can fail for out-of-range versions; you already have `allow_out_of_range` in your options model.
* **Update semantics**: many Delta implementations encode updates as pre/post images; decide whether you want “postimage-only” for rebuild triggers.
* **Incremental correctness**: use idempotency tests—reapplying the same CDF range should not change outputs.

---

## 7) Delta `get_add_actions()` as an external pruning/index feed (and “with_files” to actually prune scans)

### What the feature gives you

`DeltaTable.get_add_actions(flatten=...)` returns a dataframe describing the **current active files** (add actions), with optional flattening:

* partition values prefixed `partition.`
* stats prefixed `min.`, `max.`, `null_count.`
* tags prefixed `tags.` ([Delta][13])

This is exactly the “file-level metadata stream” you need to build an external index for pruning.

**Critical enabler on the DataFusion side:** the Rust `DeltaTableProvider` supports `with_files(...)` to restrict which `Add` actions are considered during scan. ([Docs.rs][14])
So an external index can choose candidate files, and your table provider can scan only those.

### Architecture pattern

Build:

1. a Delta “file index table” (one row per data file, carrying stats/partition values + any extra metadata you compute),
2. a custom `TableProvider` wrapper (or a factory) that:

   * inspects filters,
   * queries the index to get candidate file paths,
   * constructs `DeltaTableProvider::try_new(...).with_files(...)`.

### Code: build/update a file index table in Python

```python
# src/storage/deltalake/file_index.py
from __future__ import annotations

import pyarrow as pa
from deltalake import DeltaTable
from storage.deltalake import DeltaWriteOptions, write_dataset_delta

def delta_file_index_table(dt: DeltaTable, *, flatten: bool = True) -> pa.Table:
    # get_add_actions returns a dataframe-like object; delta-rs docs describe it as a “dataframe”
    adds = dt.get_add_actions(flatten=flatten)  # type: ignore
    # Many environments expose it as Arrow already; if not, coerce accordingly.
    if hasattr(adds, "to_pyarrow_table"):
        return adds.to_pyarrow_table()
    if isinstance(adds, pa.Table):
        return adds
    raise TypeError("Unsupported get_add_actions return type")

def write_delta_file_index(
    *,
    table_path: str,
    index_path: str,
    storage_options: dict[str, str] | None = None,
) -> str:
    dt = DeltaTable(table_path, storage_options=storage_options)
    index_table = delta_file_index_table(dt, flatten=True)
    return write_dataset_delta(
        index_table,
        index_path,
        options=DeltaWriteOptions(mode="overwrite", schema_mode="overwrite"),
    ).path
```

### Code: Rust-side “with_files” pruning hook (sketch)

This is the “hard part” that makes the index *actually prune IO*.

```rust
// rust/datafusion_ext/src/delta_pruning.rs (sketch)
use deltalake::delta_datafusion::{DeltaScanConfigBuilder, DeltaTableProvider};
use deltalake::{DeltaTableBuilder, ensure_table_uri};
use std::collections::HashSet;

pub fn delta_provider_with_candidate_paths(
    table_uri: &str,
    candidate_paths: HashSet<String>,
    /* ... storage opts, version, timestamp, scan config ... */
) -> Result<DeltaTableProvider, deltalake::DeltaTableError> {
    let table_url = ensure_table_uri(table_uri)?;
    let table = DeltaTableBuilder::from_url(table_url).load().await?;
    let snapshot = table.snapshot()?.snapshot().clone();
    let log_store = table.log_store();

    let scan_config = DeltaScanConfigBuilder::new().build(&snapshot)?;

    let provider = DeltaTableProvider::try_new(snapshot.clone(), log_store, scan_config)?;

    // Filter snapshot file list to only candidates, then:
    // provider.with_files(files)
    // (Docs: with_files defines which files to consider) :contentReference[oaicite:20]{index=20}
    let files = snapshot
        .file_actions()
        .iter()
        .filter(|add| candidate_paths.contains(add.path.as_ref()))
        .cloned()
        .collect();

    Ok(provider.with_files(files))
}
```

### Tests / perf notes

* Index table generation is cheap (log parsing) compared to scanning parquet footers for many files.
* The real payoff is when your index is keyed on *non-partition* fields (e.g. repo/path ranges, span.start.line0 ranges, symbol prefixes).

---

## 8) Delta scan config completeness: fix your current Rust/Python signature mismatch + expose `wrap_partition_values`

### What the feature gives you

`DeltaScanConfig` (delta-rs DataFusion integration) includes:

* `wrap_partition_values` (dictionary encoding of partition values; defaults true),
* `schema_force_view_types`,
* `enable_parquet_pushdown`,
* `file_column_name`,
* optional schema override. ([Docs.rs][15])

Right now your Python `registry_bridge._delta_rust_table_provider(...)` passes `schema_force_view_types=...` into `datafusion_ext.delta_table_provider(...)`, but your Rust `#[pyfunction] fn delta_table_provider(...)` signature does **not** accept it.

### Architecture pattern

* Extend `schema_spec.system.DeltaScanOptions` to include `wrap_partition_values: bool | None`
* Update `_delta_scan_config(...)` and `_delta_rust_table_provider(...)` to pass it
* Update Rust `delta_table_provider(...)` to accept both and set them in the scan builder (or in the final config)

### Code: Python additions

```python
# src/schema_spec/system.py
@dataclass(frozen=True)
class DeltaScanOptions:
    file_column_name: str | None = None
    enable_parquet_pushdown: bool = True
    schema_force_view_types: bool = False
    wrap_partition_values: bool | None = None  # NEW
    schema: pa.Schema | None = None
```

And in `src/datafusion_engine/registry_bridge.py`:

```python
capsule = factory(
    table_uri=str(context.location.path),
    ...
    schema_force_view_types=delta_scan.schema_force_view_types if delta_scan else None,
    wrap_partition_values=delta_scan.wrap_partition_values if delta_scan else None,  # NEW
    schema_ipc=schema_ipc,
)
```

### Code: Rust signature fix (minimum viable)

```rust
#[pyfunction]
fn delta_table_provider(
    py: Python<'_>,
    table_uri: String,
    storage_options: Option<Vec<(String, String)>>,
    version: Option<i64>,
    timestamp: Option<String>,
    file_column_name: Option<String>,
    enable_parquet_pushdown: Option<bool>,
    schema_force_view_types: Option<bool>,      // NEW
    wrap_partition_values: Option<bool>,        // NEW
    schema_ipc: Option<Vec<u8>>,
) -> PyResult<PyObject> {
    ...
    let mut scan_builder = DeltaScanConfigBuilder::new();
    if let Some(v) = schema_force_view_types {
        scan_builder = scan_builder.with_schema_force_view_types(v); // method name may differ by version
    }
    if let Some(v) = wrap_partition_values {
        scan_builder = scan_builder.with_wrap_partition_values(v);   // method name may differ by version
    }
    ...
}
```

Even if the exact builder method names differ in your deltalake version, the *fields you’re trying to set are real and documented*. ([Docs.rs][15])

---

## 9) SQLGlot: enforce schema-aware rewrites + add a regression harness using the Python engine

### What the feature gives you

SQLGlot’s optimizer can run with a provided schema mapping (`optimize(expression, schema=...)`). ([SqlGlot][16])
SQLGlot can also annotate types (`annotate_types(expression, schema=...)`), which is very useful to fail fast on ill-typed LLM SQL or policy-generated SQL. ([SqlGlot][17])
And SQLGlot’s Python SQL engine articles describe the core pipeline and qualification steps (tables/columns qualification is foundational). ([GitHub][18])

### Architecture pattern

* Make “schema-aware optimize + annotate_types” a **hard preflight** before you hand SQL to DataFusion (especially for dynamic rule SQL).
* Add a small fixture-based harness that runs “canonical SQL” against tiny in-memory dict tables via SQLGlot’s executor to regression-test rewrites.

### Code: schema-aware preflight guard

```python
# src/sqlglot_tools/guards.py
from __future__ import annotations

from sqlglot import parse_one
from sqlglot.optimizer import optimize
from sqlglot.optimizer.annotate_types import annotate_types

def preflight_sql(sql: str, *, schema: dict) -> None:
    expr = parse_one(sql)
    # 1) normalize/optimize using schema awareness
    optimized = optimize(expr, schema=schema)
    # 2) annotate types to catch mismatches early
    annotate_types(optimized, schema=schema)
```

### Code: rewrite regression harness (tiny fixtures)

```python
# tests/test_sql_rewrites_regression.py (sketch)
from sqlglot.executor import execute

def test_rewrite_semantics():
    tables = {
        "t": [{"a": "x", "b": 1}, {"a": "y", "b": 2}],
    }
    result = execute("SELECT a, SUM(b) AS s FROM t GROUP BY a", tables=tables)
    assert sorted(result.rows) == [("x", 1), ("y", 2)]
```

---

# Suggested implementation order (min risk → max payoff)

1. **Fix the Rust/Python mismatches**

   * `delta_table_provider` args (`schema_force_view_types`, plus `wrap_partition_values`)
   * `install_function_factory` primitive completeness (`prefixed_hash64`, `stable_id`)
   * `install_expr_planners` exists and registers at least one planner

2. **Tracing/metrics envelope**

   * implement OTel + `datafusion_run_started/finished` artifacts
   * add opt-in `EXPLAIN ANALYZE` capture for debug profiles (metrics are documented there) ([Apache DataFusion][19])

3. **Ibis→Substrait dual lane**

   * compile bytes via `ibis-substrait`, replay via your existing `replay_substrait_bytes` ([GitHub][9])

4. **Delta CDF cursor-based incremental rebuilds**

   * `load_cdf` is already a streaming reader; integrate with your incremental invalidation graph ([Delta][11])

5. **File-index pruning**

   * `get_add_actions` → build index
   * Rust `DeltaTableProvider.with_files(...)` to actually restrict scans ([Delta][13])

If you want, I can also map each of these into a concrete PR-by-PR sequence (PR-01…PR-n) matching your repo’s style (new diagnostics schemas, delta-backed obs datasets, golden fixtures, and contract tests).

[1]: https://datafusion.apache.org/user-guide/explain-usage.html?utm_source=chatgpt.com "Reading Explain Plans — Apache DataFusion documentation"
[2]: https://github.com/apache/datafusion/discussions/13096?utm_source=chatgpt.com "Getting results and explain analyze plan at the same time"
[3]: https://docs.rs/datafusion-tracing "https://docs.rs/datafusion-tracing"
[4]: https://datafusion.apache.org/python/user-guide/dataframe/index.html?utm_source=chatgpt.com "DataFrames — Apache Arrow DataFusion documentation"
[5]: https://datafusion.apache.org/library-user-guide/extending-sql.html "https://datafusion.apache.org/library-user-guide/extending-sql.html"
[6]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.Signature.html?utm_source=chatgpt.com "Signature in datafusion::logical_expr - Rust"
[7]: https://docs.rs/datafusion/latest/datafusion/execution/context/index.html?utm_source=chatgpt.com "datafusion::execution::context - Rust"
[8]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html "https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html"
[9]: https://github.com/ibis-project/ibis-substrait "https://github.com/ibis-project/ibis-substrait"
[10]: https://ibis-project.org/posts/ibis_substrait_to_duckdb/ "https://ibis-project.org/posts/ibis_substrait_to_duckdb/"
[11]: https://delta-io.github.io/delta-rs/api/delta_table/ "https://delta-io.github.io/delta-rs/api/delta_table/"
[12]: https://delta-io.github.io/delta-rs/usage/read-cdf/ "https://delta-io.github.io/delta-rs/usage/read-cdf/"
[13]: https://delta-io.github.io/delta-rs/python/api_reference.html "https://delta-io.github.io/delta-rs/python/api_reference.html"
[14]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaTableProvider.html "https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaTableProvider.html"
[15]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaScanConfig.html "https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaScanConfig.html"
[16]: https://sqlglot.com/sqlglot/optimizer/optimizer.html "https://sqlglot.com/sqlglot/optimizer/optimizer.html"
[17]: https://sqlglot.com/sqlglot/optimizer/annotate_types.html "https://sqlglot.com/sqlglot/optimizer/annotate_types.html"
[18]: https://github.com/tobymao/sqlglot/blob/main/posts/python_sql_engine.md "https://github.com/tobymao/sqlglot/blob/main/posts/python_sql_engine.md"
[19]: https://datafusion.apache.org/user-guide/sql/explain.html?utm_source=chatgpt.com "EXPLAIN — Apache DataFusion documentation"
