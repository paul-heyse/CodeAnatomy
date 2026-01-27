Below is a **minimum technical reference** for the **“best-in-class deployment gaps”** we identified (relative to your current DataFusion guide), focused on: **capabilities**, **benefits**, **requirements**, **performance tradeoffs**, and **the knobs/APIs that matter**—with explicit notes where something is **Rust-only** vs **available from `datafusion` (Python)**.

---

# 1) Caching as a first-class deployment policy

## 1.1 In-memory result caching (`DataFrame.cache`)

**Capability**

* Cache a DataFusion DataFrame as an **in-memory table** (materialize once, reuse many times). In Python: `DataFrame.cache() -> DataFrame`. ([Apache DataFusion][1])

**Benefit (rule engine / CPG)**

* Cuts repeated scans/joins for “hot” normalized inputs reused across many relationship rules (fanout rule packs).

**Requirements**

* Python: supported directly in `datafusion` (`cache()` method). ([Apache DataFusion][1])
* Memory budget must accommodate cached batches (no implicit eviction policy at the DataFrame level).

**Performance / tradeoffs**

* Fast reuse, but increases peak memory; best after projection+filter to reduce cached width/rows.

---

## 1.2 RuntimeEnv CacheManager (engine-level cache infrastructure)

**Capability**

* `RuntimeEnv` manages **MemoryPool**, **DiskManager**, **CacheManager**, **ObjectStoreRegistry**. ([Docs.rs][2])
* CacheManager supports default caches and allows **custom cache implementations via traits**. ([Docs.rs][3])

**Benefit**

* Establishes an engine-native place to manage cache lifecycles consistently across sessions/components in an embedded system.

**Requirements**

* Primarily a **Rust embedding** feature (Python config surfaces may not expose all cache customization; but the underlying runtime concept exists). ([Docs.rs][2])

---

## 1.3 ListingTable file-enumeration cache (`list_files_cache_*`)

**Capability**

* DataFusion caches file listings for `ListingTable`:

  * `datafusion.runtime.list_files_cache_limit` (bytes)
  * `datafusion.runtime.list_files_cache_ttl` (optional TTL) ([Apache DataFusion][4])

**Benefit**

* Avoids repeated object-store directory listings (huge for S3/GCS/Azure) across many rules querying the same dataset prefix.

**Correctness / staleness**

* Without expiration, **new/removed files may not be seen until the ListingTableProvider is dropped/recreated**. ([Apache DataFusion][5])
  ⇒ For “run_id output prefixes”, this is usually fine; for “mutable prefixes”, you need TTL or explicit provider lifecycle rules.

---

## 1.4 Parquet metadata cache (`metadata_cache_limit`) + automatic metadata caching

**Capability**

* `datafusion.runtime.metadata_cache_limit` caps memory used for file metadata (Parquet metadata, page indexes, etc.). ([Apache DataFusion][4])
* DataFusion 50+ automatically caches Parquet metadata for built-in ListingTable scans and reports large speedups for repeated point reads. ([Apache DataFusion][6])

**Benefit**

* Repeated rule execution over the same normalized datasets pays the metadata cost once.

**Requirements**

* Works best with built-in ListingTable provider. ([Apache DataFusion][6])

---

## 1.5 Parquet predicate evaluation cache (`max_predicate_cache_size`) for late materialization

**Capability**

* When `datafusion.execution.parquet.pushdown_filters=true` (late materialization), `datafusion.execution.parquet.max_predicate_cache_size` caps memory used to cache predicate evaluation results; 0 disables caching. ([Apache DataFusion][4])

**Benefit**

* Reduces repeated predicate evaluation overhead between filter evaluation and output generation in Parquet decoding.

**Tradeoff**

* Lower cache size reduces memory, but can increase IO/CPU. ([Apache DataFusion][4])

---

## 1.6 Cache introspection surfaces (CLI table functions; can be replicated in embedding)

**Capability (DataFusion CLI)**

* `metadata_cache()` table function (file metadata cache)
* `statistics_cache()` table function (file statistics cache; requires `collect_statistics`)
* `list_files_cache()` table function (listing cache; TTL aware) ([Apache DataFusion][7])

**Benefit**

* Makes cache behavior observable (size, hits, expiry) during performance debugging.

**Requirement note**

* These are documented as **CLI-specific functions**; for a Python embedded deployment you may need to provide equivalent introspection via metrics or custom tables. ([Apache DataFusion][7])

---

# 2) Statistics as an optimizer input + a run artifact

## 2.1 Cost model depends on table/column statistics

**Capability**

* DataFusion’s query optimizer uses a **cost-based model** that relies on **table/column statistics** to estimate selectivity and costs for joins/filters. ([Apache DataFusion][8])

**Benefit**

* Rule packs with many joins are highly sensitive to join choice/order and repartition decisions; statistics reduce “bad plan” risk.

---

## 2.2 Statistics collection policy (`collect_statistics`) and default change

**Capability**

* `datafusion.execution.collect_statistics` controls whether statistics are collected when first creating a table (ListingTable). ([Apache DataFusion][4])
* In DataFusion 48.0.0+, `collect_statistics` default is **true** (breaking change). ([Apache DataFusion][9])

**Benefit**

* Better plans on subsequent queries; less guesswork for join/scan planning.

**Tradeoff**

* Table registration can be slower because it must read enough metadata to infer stats. ([Apache DataFusion][9])

---

## 2.3 Statistics cache observability (`statistics_cache`)

**Capability (CLI)**

* `statistics_cache()` shows per-file cached statistics; doc states it requires `datafusion.execution.collect_statistics` enabled. ([Apache DataFusion][7])

**Benefit**

* “Why did the plan change?” debugging; verifying stats exist before expecting good join plans.

---

## 2.4 Planning/metadata concurrency knobs that interact with stats

**Capability**

* `datafusion.execution.meta_fetch_concurrency` controls number of files read in parallel when inferring schema/statistics. ([Apache DataFusion][4])

**Benefit**

* Large repo datasets with many Parquet files: faster registration and stats inference (at the cost of more concurrent IO). ([Apache DataFusion][4])

---

# 3) SQL-defined functions via `CREATE FUNCTION` (FunctionFactory)

## 3.1 Pluggable `CREATE FUNCTION` (FunctionFactory)

**Capability**

* DataFusion 40 introduced **pluggable support** for `CREATE FUNCTION` via a **`FunctionFactory`** API: systems can handle `CREATE FUNCTION …` statements (SQL functions, ML model calls, etc.). ([Apache DataFusion][10])

**Benefit (global rulesets)**

* Lets you define/standardize “global rule primitives” (scoring, tie-breakers, normalization helpers) under stable function names—so rule authors call `cpg_score_*()` instead of re-implementing logic inline.

**Requirements**

* **Not enabled by default** unless you configure a function factory; DataFusion CLI shows “Function factory has not been configured” when absent. ([GitHub][11])
* Practically: **Rust embedding or PyO3 extension** to wire the factory into the session.

---

# 4) Advanced function system features beyond basic UDFs

DataFusion’s function docs explicitly enumerate these extension capabilities: **Async scalar UDF**, **Named arguments**, **Custom expression planning**, plus Window/Aggregate/Table UDFs. ([Apache DataFusion][12])

## 4.1 Async Scalar UDF

**Capability**

* Define scalar UDFs that can run asynchronously (feature is called out as a first-class option). ([Apache DataFusion][12])

**Benefit**

* Enables “engine-controlled” integration with remote/IO-bound lookups (when unavoidable).

**Tradeoffs**

* Adds async runtime complexity; usually only worth it when the function is latency-bound and cannot be expressed as pushdown.

**Requirements**

* Implemented at the engine/UDF interface level (generally Rust-side).

---

## 4.2 Named Arguments

**Capability**

* Named arguments support is explicitly called out in the functions docs index. ([Apache DataFusion][12])

**Benefit**

* Improves stability of rule authoring (especially for LLM-authored SQL) by making parameter intent explicit.

**Requirements**

* Feature support varies by specific function implementations and SQL planner support; treat as version-dependent.

---

## 4.3 Custom Expression Planning (domain operators)

**Capability**

* DataFusion supports **custom expression planning** (explicitly called out in functions docs), enabling systems to intercept/plan expressions in specialized ways. ([Apache DataFusion][12])

**Benefit (CPG)**

* This is the “engine-level hook” for domain operators like span-alignment joins or rule-scoring expressions that you want optimized/pushed down consistently.

**Requirements**

* System-builder feature (Rust embedding; often paired with custom logical nodes / extension codecs).

---

# 5) Streaming / incremental inputs (unbounded tables)

## 5.1 `StreamingTable` for potentially unbounded inputs

**Capability**

* DataFusion includes a `StreamingTable` concept: “reads data from potentially unbounded inputs.” ([Docs.rs][13])

**Benefit (repo watch mode)**

* Enables incremental pipelines (file watcher → deltas → continuous rule evaluation) without treating every run as a full batch rebuild.

**Requirements**

* This is a **Rust API** concept (not commonly exposed as a turnkey Python surface); in Python you’d typically implement similar behavior via a custom TableProvider/connector.

---

# 6) Distributed execution (Ballista / DataFusion Ray)

## 6.1 Ballista (official distributed runtime)

**Capability**

* Ballista is a distributed compute platform powered by DataFusion (scheduler + executors; supports container deployment). ([Apache DataFusion][14])

**Benefit**

* Scale relationship compilation across multiple nodes (many repos / very large repos) while keeping Arrow semantics.

**Python requirements**

* Ballista provides **Python bindings**, supporting SQL and DataFrame queries executed in a distributed environment. ([Apache DataFusion][15])

---

## 6.2 DataFusion Ray (ecosystem)

**Capability**

* The DataFusion Python project explicitly notes “DataFusion Ray” as another distributed query engine using the Python bindings. ([GitHub][16])

**Benefit**

* If your orchestration already uses Ray (actors/object store), this is a candidate path for distributed execution.

**Requirement note**

* Ray integration details are not part of core DataFusion docs; treat as ecosystem / project-specific.

---

# 7) Observability beyond EXPLAIN: metrics APIs + tracing/OTel

## 7.1 Programmatic runtime metrics (`ExecutionPlan::metrics`) + Metrics types

**Capability**

* DataFusion operators collect detailed metrics; docs state you can access them via `EXPLAIN ANALYZE` **and** programmatically via `ExecutionPlan::metrics`. ([Apache DataFusion][17])
* There is a full metrics module (gauges/counters/timers/metrics sets). ([Docs.rs][18])

**Benefit**

* In a rule engine, you can attach rule IDs → plan nodes → operator hotspots (shuffle, spill, scan IO) as structured telemetry.

**Requirements**

* Programmatic `ExecutionPlan::metrics` is a Rust API; Python typically consumes metrics via `EXPLAIN ANALYZE` unless bindings expose metrics directly. ([Apache DataFusion][17])

---

## 7.2 `datafusion-tracing` (OpenTelemetry / tracing integration)

**Capability**

* `datafusion-tracing` is an extension that uses **tracing + OpenTelemetry** to gather DataFusion metrics, trace execution steps, and preview partial results. ([GitHub][19])
* It is explicitly “not an official ASF release,” but is purpose-built for observability. ([Docs.rs][20])

**Benefit**

* Production-grade profiling of rule packs (spans, operator timing, IO) without relying on ad-hoc logs.

**Requirements**

* Rust embedding (you install/enable the extension around SessionContext/SessionState).

---

# 8) Planning scalability knobs (many tables/views/files)

## 8.1 Physical planning fan-out (`planning_concurrency`)

**Capability**

* `datafusion.execution.planning_concurrency`: “fan-out during initial physical planning,” used mostly to plan `UNION` children in parallel; defaults to CPU core count when 0. ([Apache DataFusion][4])

**Benefit**

* Rule packs that generate large `UNION ALL` plans (or many independent branches) can reduce planning latency.

**Tradeoff**

* More parallel planning work means higher CPU usage during planning; can contend with execution on smaller machines.

---

## 8.2 Related planning/IO knobs commonly tuned together

(These aren’t “new gaps,” but they’re essential to plan scaling and are easy to standardize as a policy set.)

* `datafusion.execution.meta_fetch_concurrency` (schema/stats inference parallelism) ([Apache DataFusion][4])
* `datafusion.runtime.list_files_cache_limit` / `list_files_cache_ttl` (avoid repeated listing during planning) ([Apache DataFusion][4])

---

# Practical “best-in-class deployment” mapping for your rule engine

If your goal is “global policies & rulesets inside DataFusion,” the clean technical split is:

* **Pure Python deployment (today):**

  * Use `DataFrame.cache()` strategically for rule pack memoization. ([Apache DataFusion][1])
  * Set config keys (SQL `SET`) for list-files cache, metadata cache, predicate cache, statistics collection, and planning concurrency. ([Apache DataFusion][4])
  * Use `EXPLAIN ANALYZE` snapshots as your regression harness. ([Apache DataFusion][17])

* **System-builder “best possible” (Python + small Rust extension):**

  * Enable `CREATE FUNCTION` via FunctionFactory for global rule primitives. ([Apache DataFusion][10])
  * Add custom expression planning for domain operators (span alignment). ([Apache DataFusion][12])
  * Add tracing/OTel via `datafusion-tracing` for production diagnostics. ([GitHub][19])
  * Consider Ballista when single-host constraints dominate. ([Apache DataFusion][14])

If you want, I can turn this into a **“PolicyPack spec”**: a single versioned object that defines all recommended config keys (dev/CI/prod), plus which ones you only enable in “system-builder mode” (FunctionFactory, tracing).

[1]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html?utm_source=chatgpt.com "datafusion.dataframe"
[2]: https://docs.rs/datafusion/latest/datafusion/execution/runtime_env/struct.RuntimeEnv.html?utm_source=chatgpt.com "RuntimeEnv in datafusion::execution::runtime_env - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/execution/cache/cache_manager/struct.CacheManager.html?utm_source=chatgpt.com "CacheManager in datafusion::execution - cache_manager"
[4]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[5]: https://datafusion.apache.org/library-user-guide/upgrading.html?utm_source=chatgpt.com "Upgrade Guides — Apache DataFusion documentation"
[6]: https://datafusion.apache.org/blog/2025/09/29/datafusion-50.0.0/?utm_source=chatgpt.com "Apache DataFusion 50.0.0 Released"
[7]: https://datafusion.apache.org/user-guide/cli/functions.html "CLI Specific Functions — Apache DataFusion  documentation"
[8]: https://datafusion.apache.org/library-user-guide/query-optimizer.html?utm_source=chatgpt.com "Query Optimizer — Apache DataFusion documentation"
[9]: https://datafusion.apache.org/blog/2025/07/16/datafusion-48.0.0/?utm_source=chatgpt.com "Apache DataFusion 48.0.0 Released"
[10]: https://datafusion.apache.org/blog/2024/07/24/datafusion-40.0.0/?utm_source=chatgpt.com "Apache DataFusion 40.0.0 Released"
[11]: https://github.com/apache/datafusion/discussions/12848?utm_source=chatgpt.com "What is the status of \"CREATE FUNCTION\"? #12848"
[12]: https://datafusion.apache.org/library-user-guide/functions/index.html?utm_source=chatgpt.com "Functions — Apache DataFusion documentation"
[13]: https://docs.rs/datafusion/latest/datafusion/?utm_source=chatgpt.com "datafusion - Rust"
[14]: https://datafusion.apache.org/ballista/user-guide/introduction.html?utm_source=chatgpt.com "Overview — Apache DataFusion Ballista documentation"
[15]: https://datafusion.apache.org/ballista/user-guide/python.html?utm_source=chatgpt.com "Ballista Python Bindings - Apache DataFusion"
[16]: https://github.com/apache/datafusion-python?utm_source=chatgpt.com "Apache DataFusion Python Bindings"
[17]: https://datafusion.apache.org/user-guide/explain-usage.html?utm_source=chatgpt.com "Reading Explain Plans — Apache DataFusion documentation"
[18]: https://docs.rs/datafusion/latest/datafusion/physical_plan/metrics/index.html?utm_source=chatgpt.com "datafusion::physical_plan::metrics - Rust"
[19]: https://github.com/datafusion-contrib/datafusion-tracing?utm_source=chatgpt.com "datafusion-contrib/datafusion-tracing: Integration of ..."
[20]: https://docs.rs/datafusion-tracing?utm_source=chatgpt.com "datafusion_tracing - Rust"


Below is a **command-gap assessment** (what I *didn’t* enumerate last time) and then a **complete command + option inventory** for each of the “gap topics” (caching, stats, CREATE FUNCTION, advanced function system features, streaming/unbounded, distributed, observability, planning scalability). Everything is expressed in terms an AI coding agent can directly look up / call in Python (or, when Rust-only, the exact Rust trait / builder hook you must implement).

---

## Global note: “how to set / inspect options” (Python vs CLI vs Rust)

### Python (`datafusion` package)

**Set arbitrary config key**

* `SessionConfig.set(key: str, value: str) -> SessionConfig` (stringly typed, returns a new config). ([Apache DataFusion][1])

**Set config via SQL**

* DataFusion config keys can be set via SQL `SET …` (works in DataFusion SQL generally; you execute it through `ctx.sql("SET ...")`). ([Apache DataFusion][2])

**Inspect config**

* If `information_schema` is enabled, query: `information_schema.df_settings` (works in Ballista and DataFusion; shown in Ballista docs). ([Apache DataFusion][1])

### `datafusion-cli` (helpful for inspection even if you deploy via Python)

* `SHOW ALL [VERBOSE]`, `SHOW <OPTION>`, `SET <OPTION> TO <VALUE>` are explicitly supported by the CLI. ([Apache DataFusion][3])
* CLI env-var mapping: `DATAFUSION_<KEY_WITH_DOTS_AS_UNDERSCORES>` (e.g., `DATAFUSION_EXECUTION_BATCH_SIZE`). ([Apache DataFusion][3])
* CLI also has **extra table functions** (cache introspection, parquet_metadata) that are not necessarily in core SQL. ([Apache DataFusion][4])

---

# 1) Caching policy layer

## 1A) Command gap assessment (what I previously omitted)

I previously omitted:

* The **complete set of cache-related config keys** from the official config registry (esp. `list_files_cache_ttl`, `parquet.metadata_size_hint`, `parquet.enable_page_index`, and `parquet.max_predicate_cache_size` semantics). ([Apache DataFusion][2])
* The **CLI cache inspection functions** (`metadata_cache()`, `list_files_cache()`, `statistics_cache()`) and their returned columns. ([Apache DataFusion][4])
* The **object store profiling command** in the CLI (`\object_store_profiling`) that is often used to validate “caching is helping / hurting” on S3/HTTP. ([Apache DataFusion][3])

## 1B) Total command & option inventory

### Python commands (in-process caching)

* `datafusion.DataFrame.cache()`

  * Executes the plan and buffers output into a new in-memory DataFrame. ([Apache Arrow][5])

### DataFusion configuration keys (cache-related)

All of these can be set via:

* Python: `SessionConfig.set(key, value)` ([Apache DataFusion][1])
* SQL: `SET key = 'value'` ([Apache DataFusion][2])

**Listing (file enumeration) cache**

* `datafusion.runtime.list_files_cache_limit` (default `1M`) — max memory used for ListingTable “list files” cache. ([Apache DataFusion][2])
* `datafusion.runtime.list_files_cache_ttl` (default `NULL`) — TTL for list-files cache entries; supports `m` and `s` units (example `2m`). ([Apache DataFusion][2])

  * Operational rule: without TTL, file adds/removes aren’t seen until the `ListingTableProvider` is dropped/recreated. ([Apache DataFusion][6])

**File metadata cache (Parquet metadata, page indexes, etc.)**

* `datafusion.runtime.metadata_cache_limit` (default `50M`) — cap for file metadata cache. ([Apache DataFusion][2])

**Parquet late-materialization predicate cache**

* `datafusion.execution.parquet.pushdown_filters` (default `false`) — enables “late materialization” (apply filters during parquet decoding). ([Apache DataFusion][2])
* `datafusion.execution.parquet.max_predicate_cache_size` (default `NULL`) — max bytes for caching predicate eval results when `pushdown_filters` is enabled; `0` disables caching. ([Apache DataFusion][2])

**Parquet metadata IO knobs that directly affect cache cost/benefit**

* `datafusion.execution.parquet.enable_page_index` (default `true`) — reads Parquet PageIndex metadata (if present) to reduce I/O / decoded rows; increases metadata footprint. ([Apache DataFusion][2])
* `datafusion.execution.parquet.metadata_size_hint` (default `524288`) — optimistic “read last N bytes” to fetch footer/metadata with fewer I/Os; impacts remote-store latency. ([Apache DataFusion][2])

**Runtime guardrails that interact with caching**

* `datafusion.runtime.memory_limit` (default `NULL`) — global query execution memory cap (includes memory pressure from cached artifacts). ([Apache DataFusion][2])
* `datafusion.runtime.temp_directory` (default `NULL`) — spill/caching temp location (if disk manager used). ([Apache DataFusion][2])
* `datafusion.runtime.max_temp_directory_size` (default `100G`) — guardrail for spill/cache temp usage. ([Apache DataFusion][2])

### CLI cache inspection (commands you can use as reference even if deploying via Python)

These are **`datafusion-cli` table functions**:

* `metadata_cache()` — lists cached file metadata and includes columns such as `metadata_size_bytes`, `hits`, and `extra` (e.g., page_index flags). ([Apache DataFusion][4])
* `list_files_cache()` — lists ListingTable file enumeration cache entries; cache entries are scoped to tables. ([Apache DataFusion][4])
* `statistics_cache()` — lists file statistics cache; requires `datafusion.execution.collect_statistics=true`. ([Apache DataFusion][4])
* `parquet_metadata('path.parquet')` — inspect detailed parquet metadata (stats, sizes, encodings) to reason about pruning/caching. ([Apache DataFusion][4])

### CLI object-store profiling (I/O proof tool)

* `\object_store_profiling [disabled|summary|trace]` — prints detailed object-store operations for queries (helps validate caching benefits).

---

# 2) Statistics as optimizer input + run artifact

## 2A) Command gap assessment

I previously omitted:

* The **official optimizer statement** that DataFusion uses a **cost-based model** relying on **table/column statistics**. ([Apache DataFusion][7])
* The DDL-level guidance that `collect_statistics` is applied at **table creation time**, and you must set it **before** creating the table. ([Apache DataFusion][2])
* The CLI `parquet_metadata()` function which is often the fastest way to inspect statistics/min-max availability and why pruning is (not) happening. ([Apache DataFusion][4])

## 2B) Total command & option inventory

### Key idea (what DataFusion actually uses)

* DataFusion optimizer is **cost-based**, relying on table and column statistics to estimate selectivity and join/filter costs. ([Apache DataFusion][7])

### Config keys that control statistics collection & ingestion

* `datafusion.execution.collect_statistics` (default `true`)

  * Collect stats when first creating a table (ListingTable provider); has no effect after table is created. ([Apache DataFusion][2])
* `datafusion.execution.meta_fetch_concurrency` (default `32`)

  * Number of files to read in parallel when inferring schema and statistics. ([Apache DataFusion][2])

### SQL commands that “control stats”

DataFusion does **not** rely on an `ANALYZE` statement; the documented workflow is:

* set `collect_statistics` before table creation
* create/register the table
* stats are gathered during creation and used by planner thereafter ([Apache DataFusion][2])

### CLI stats inspection

* `statistics_cache()` — requires `datafusion.execution.collect_statistics=true`; shows `num_rows`, `table_size_bytes`, etc. ([Apache DataFusion][4])
* `parquet_metadata('file.parquet')` — inspect row group stats min/max presence and compression, useful for pruning reasoning. ([Apache DataFusion][4])

---

# 3) SQL-defined functions: `CREATE FUNCTION` + `FunctionFactory`

## 3A) Command gap assessment

I previously omitted:

* The **precise CREATE FUNCTION SQL forms** DataFusion advertises (SQL body vs LANGUAGE TORCH/WASM). ([Apache DataFusion][8])
* The **exact SessionStateBuilder hook** used to install a function factory (`with_function_factory`).
* The “failure mode” signature (`Function factory has not been configured`) which is important for an agent debugging why CREATE FUNCTION fails. ([GitHub][9])

## 3B) Total command & option inventory

### SQL commands (what a rule author writes)

Supported pattern examples shown by the project:

* `CREATE FUNCTION my_func(DOUBLE, DOUBLE) RETURNS DOUBLE RETURN ...;`
* `CREATE FUNCTION iris(FLOAT[]) RETURNS FLOAT[] LANGUAGE TORCH AS 'models:/...';`
* `CREATE FUNCTION func(FLOAT[]) RETURNS FLOAT[] LANGUAGE WASM AS 'func.wasm'` ([Apache DataFusion][8])

### Requirement / guardrail

* If no FunctionFactory is installed, CREATE FUNCTION fails with “Function factory has not been configured”. ([GitHub][9])

### Rust embedding commands (the *actual* best-in-class mechanism)

**Plan-side objects**

* `datafusion::logical_expr::CreateFunction` represents CREATE FUNCTION arguments; it is turned into an executable function using a FunctionFactory. ([Apache DataFusion][8])

**Install the handler**

* `SessionStateBuilder.with_function_factory(...)` is the canonical install point.

**Minimum “wiring” surface (agent should know these symbols exist)**

* `datafusion::execution::session_state::SessionStateBuilder` methods include:

  * `with_function_factory`
  * `with_scalar_functions`, `with_aggregate_functions`, `with_window_functions`, `with_table_functions` (for registry control)

### Python requirement note

There is **no standard “turnkey” Python API** to configure FunctionFactory in stock `datafusion` Python; you typically need a **small PyO3 Rust extension** (or embed DataFusion in Rust) that constructs a `SessionState` with your factory and then exposes a `SessionContext` into Python.

---

# 4) Advanced function system features (Async UDF, named args, custom expression planning)

## 4A) Command gap assessment

I previously omitted:

* The **exact traits/classes** for async UDFs and how they become runnable functions. ([Apache DataFusion][10])
* The **named arguments call syntax** and the **signature API** `.with_parameter_names(...)`. ([Apache DataFusion][10])
* The explicit **ExprPlanner** extension point and its **registration hook** (`SessionStateBuilder.with_expr_planners`). ([Apache DataFusion][11])

## 4B) Total command & option inventory

### 4B-1) Async Scalar UDF (Rust extension API)

From the UDF guide:

* Implement `AsyncScalarUDFImpl`
* Wrap with `AsyncScalarUDF::new(Arc<...>)`
* Register via `SessionContext.register_udf(udf.into_scalar_udf())` (async UDF converted into normal scalar UDF) ([Apache DataFusion][10])

### 4B-2) Named arguments (SQL + UDF signature)

**SQL call forms**

* `SELECT substr(str => 'hello', start_pos => 2, length => 3);`
* Named args can mix with positional args, but positional must come first. ([Apache DataFusion][10])

**Implementation requirement**

* Add parameter names on the signature: `Signature::... .with_parameter_names(vec![...])` ([Apache DataFusion][10])

### 4B-3) Custom expression planning (ExprPlanner)

**What it is**

* Customize how SQL AST expressions are planned into DataFusion logical `Expr`s; used for custom operators, custom field access, custom function handling. ([Apache DataFusion][11])

**Commands / hooks**

* Implement `ExprPlanner` trait (Rust). ([Docs.rs][12])
* Register with `SessionStateBuilder.with_expr_planners(Vec<Arc<dyn ExprPlanner>>)` ([Apache DataFusion][11])

### 4B-4) Function registry “policy” hooks (system builder)

If your goal is “global policy / curated function surface,” the **SessionStateBuilder** registry APIs are the commands to know:

* `with_scalar_functions`
* `with_aggregate_functions`
* `with_window_functions`
* `with_table_functions`
* `with_function_factory` (ties into CREATE FUNCTION)

---

# 5) Streaming / incremental inputs (UNBOUNDED / StreamingTable)

## 5A) Command gap assessment

I previously omitted:

* The DDL syntax showing **`CREATE [UNBOUNDED] EXTERNAL TABLE`**.
* The concrete Rust module that provides a streaming table provider (`datafusion::catalog::streaming::StreamingTable`). ([Docs.rs][13])
* The config knob that is explicitly about unbounded joins (`allow_symmetric_joins_without_pruning`). ([Apache DataFusion][2])

## 5B) Total command & option inventory

### SQL DDL (streaming/unbounded surface)

* `CREATE [UNBOUNDED] EXTERNAL TABLE ... STORED AS ... LOCATION ...`

### Rust table provider (streaming surface)

* `datafusion::catalog::streaming::StreamingTable` — “simplified TableProvider for streaming partitioned datasets.” ([Docs.rs][13])

### Config knobs that matter for unbounded/streaming joins

* `datafusion.optimizer.allow_symmetric_joins_without_pruning` (default `true`) — explicitly documented as a control for symmetric hash joins over unbounded sources and memory behavior. ([Apache DataFusion][2])

---

# 6) Distributed execution (Ballista, DataFusion Ray)

## 6A) Command gap assessment

I previously omitted:

* The concrete Ballista **Python** entrypoints (`BallistaBuilder`, `.standalone()`, `.remote()`) and supported actions (`df.show`, `df.collect`, `df.explain`). ([Apache DataFusion][14])
* Ballista scheduler **CLI flags** and the scheduler-policy / executor-slots-policy option set.
* DataFusion Ray’s actual Python entrypoints (`DFRayContext`, `df_ray_runtime_env`) and the fact the project is unmaintained.
* The Ray-specific config keys being proposed (`datafusion.ray.execution.*`) and `DFRayContext.set`.

## 6B) Total command & option inventory

### 6B-1) Ballista (Python client)

**Connect**

* `from ballista import BallistaBuilder`
* `BallistaBuilder().config(key, value).standalone()`
* `BallistaBuilder().config(key, value).remote("df://...:50050")` ([Apache DataFusion][14])

**Register data**

* `ctx.register_parquet(name, path)`
* Or SQL: `ctx.sql("CREATE EXTERNAL TABLE ...")` ([Apache DataFusion][14])

**Execute**

* `df = ctx.sql("...")` → lazy DataFrame
* actions: `df.show()`, `df.collect()` (returns PyArrow RecordBatches), `df.explain()` ([Apache DataFusion][14])

**Limitations**

* “Underlying DataFusion supports Python UDFs but not yet implemented in Ballista” (tracking issue referenced). ([Apache DataFusion][14])

### 6B-2) Ballista (configuration & scheduler flags)

**Rust-side session config**

* `SessionConfig::new_with_ballista()`
* `SessionConfigExt::with_ballista_*` methods, including:

  * `with_ballista_logical_extension_codec`
  * `with_ballista_physical_extension_codec`
  * `with_ballista_query_planner`

**Inspect Ballista config (SQL)**

* `SELECT name, value FROM information_schema.df_settings WHERE name LIKE 'ballista'`

**Scheduler CLI flags + allowed values**

* `ballista-scheduler --scheduler-policy {pull-staged|push-staged} --event-loop-buffer-size <int> --executor-slots-policy {bias|round-robin|round-robin-local} ...`
* Cleanup flags:

  * `finished-job-data-clean-up-interval-seconds`
  * `finished-job-state-clean-up-interval-seconds`

### 6B-3) DataFusion Ray (Python)

**Project status**

* The repo explicitly warns it is **no longer maintained**.

**Core commands**

* `import ray`
* `from datafusion_ray import DFRayContext, df_ray_runtime_env`
* `ray.init(runtime_env=df_ray_runtime_env)`
* `ctx = DFRayContext()`
* `ctx.register_csv(name, url_or_path)`
* `df = ctx.sql("...")`, `df.show()`

**(Proposed/used) Ray execution config keys + setter**

* `DFRayContext.set(...)` proposed keys include:

  * `datafusion.ray.execution.batch_size`
  * `datafusion.ray.execution.partitions_per_processor`
  * `datafusion.ray.execution.prefetch_buffer_size`
  * `datafusion.ray.execution.processor_pool_min`

---

# 7) Observability beyond EXPLAIN (metrics + tracing/OTel)

## 7A) Command gap assessment

I previously omitted:

* The DataFusion **Metrics** doc as the canonical “metric names + meaning” reference.
* The explicit `ExecutionPlan::metrics()` runtime API (Rust) that returns metric snapshots.
* The concrete `datafusion-tracing` integration points: macros + builder options (`record_metrics`, `preview_limit`, `add_custom_field`, rule instrumentation chain).
* The CLI’s `--object-store-profiling` and interactive `\object_store_profiling` (useful to validate caching and remote I/O).

## 7B) Total command & option inventory

### 7B-1) EXPLAIN / metrics verbosity controls

* SQL syntax: `EXPLAIN [ANALYZE] [VERBOSE] [FORMAT format] statement`
* Config key: `datafusion.explain.analyze_level` (`summary` vs `dev`) introduced/improved in later releases.

### 7B-2) Metrics surfaces (Rust core)

* `ExecutionPlan::metrics()` — returns a snapshot of metrics for an operator/plan; set stabilizes after execution completes.
* Metrics types/modules to know exist:

  * `datafusion::physical_plan::metrics` (includes spill metrics helpers, ratio metrics, etc.)
  * `ExecutionPlanMetricsSet`, `Metric` (structs used by operators)

### 7B-3) `datafusion-tracing` (Rust extension for tracing + OpenTelemetry)

**Install**

* Add dependency: `datafusion-tracing = "<matching datafusion version>"`

**Key commands / APIs**

* `InstrumentationOptions::builder() ... build()` with options:

  * `.record_metrics(true|false)`
  * `.preview_limit(n)`
  * `.preview_fn(Arc<fn(&RecordBatch)->String>)`
  * `.add_custom_field(k, v)`
* Macros:

  * `instrument_with_info_spans!(...)`
  * `instrument_rules_with_info_spans!(...)`
* Rule configuration:

  * `RuleInstrumentationOptions::full().with_plan_diff()`
* Wiring hook:

  * `SessionStateBuilder.with_physical_optimizer_rule(instrument_rule)` (instrument execution plans)
  * plus optional “instrument all rules” call using `instrument_rules_with_info_spans!`

### 7B-4) CLI profiling options (useful for diagnosis)

* CLI flag: `--object-store-profiling {disabled|summary|trace}`
* Interactive command: `\object_store_profiling [disabled|summary|trace]`

---

# 8) Planning scalability knobs (large rule packs, many views/files)

## 8A) Command gap assessment

I previously omitted:

* The exact config key + semantics for planning fanout: `datafusion.execution.planning_concurrency`. ([Apache DataFusion][2])
* The “how to set config” CLI surfaces (`SHOW ALL`, env var mapping), which are useful even if you deploy via Python because they’re the fastest way to enumerate keys. ([Apache DataFusion][2])

## 8B) Total command & option inventory

### Planning concurrency (physical planning)

* `datafusion.execution.planning_concurrency` (default `0` ⇒ uses CPU core count)

  * “fan-out during initial physical planning”; mostly used to plan `UNION` children in parallel. ([Apache DataFusion][2])

### Supporting knobs that commonly matter in “many files” planning

* `datafusion.execution.meta_fetch_concurrency` (default `32`) — parallel file reads for schema/statistics inference (also a stats knob). ([Apache DataFusion][2])
* `datafusion.runtime.list_files_cache_limit` / `list_files_cache_ttl` — reduces repeated listing overhead during repeated planning. ([Apache DataFusion][2])

### Commands to set/inspect (agent-friendly)

* Python: `SessionConfig.set(key, value)` ([Apache DataFusion][1])
* SQL: `SET key = 'value'` ([Apache DataFusion][2])
* CLI:

  * `SHOW ALL [VERBOSE]`
  * `SHOW <OPTION>`
  * `SET <OPTION> TO <VALUE>`
  * env var mapping `DATAFUSION_<...>` ([Apache DataFusion][2])

---

## What your AI agent should do in practice

For each topic, the agent can treat this as a **mechanical checklist**:

1. Confirm the Python symbol exists (`help(datafusion.DataFrame.cache)`, `help(datafusion.SessionConfig.set)`, etc.). ([Apache Arrow][5])
2. Apply config keys via `SessionConfig.set` or `ctx.sql("SET ...")`. ([Apache DataFusion][1])
3. Validate behavior via `EXPLAIN ANALYZE` and (when needed) CLI functions like `metadata_cache()` / `statistics_cache()` / `list_files_cache()`. ([Apache DataFusion][4])
4. If you need CREATE FUNCTION / ExprPlanner / tracing: accept that this is **system-builder mode** and requires a small Rust/PyO3 extension using `SessionStateBuilder.with_function_factory` / `.with_expr_planners` / `.with_physical_optimizer_rule`. ([Apache DataFusion][11])

If you want, I can turn this into a **single “PolicyPack schema”** (JSON/YAML) with the exact keys/values you’ll standardize (dev/CI/prod), plus a “capability probe” script that asserts your environment supports each command (and prints the help/dir signatures for the agent).

[1]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/user-guide/cli/usage.html "Usage — Apache DataFusion  documentation"
[4]: https://datafusion.apache.org/user-guide/cli/functions.html "CLI Specific Functions — Apache DataFusion  documentation"
[5]: https://arrow.staged.apache.org/datafusion-python/generated/datafusion.DataFrame.html "datafusion.DataFrame — Apache Arrow DataFusion  documentation"
[6]: https://datafusion.apache.org/library-user-guide/upgrading.html?utm_source=chatgpt.com "Upgrade Guides — Apache DataFusion documentation"
[7]: https://datafusion.apache.org/library-user-guide/query-optimizer.html?utm_source=chatgpt.com "Query Optimizer — Apache DataFusion documentation"
[8]: https://datafusion.apache.org/blog/2024/07/24/datafusion-40.0.0/ "Apache DataFusion 40.0.0 Released - Apache DataFusion Blog"
[9]: https://github.com/apache/datafusion/discussions/12848 "What is the status of \"CREATE FUNCTION\"? · apache datafusion · Discussion #12848 · GitHub"
[10]: https://datafusion.apache.org/library-user-guide/functions/adding-udfs.html "Adding User Defined Functions: Scalar/Window/Aggregate/Table Functions — Apache DataFusion  documentation"
[11]: https://datafusion.apache.org/library-user-guide/extending-sql.html?utm_source=chatgpt.com "Extending SQL Syntax — Apache DataFusion documentation"
[12]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/trait.ExprPlanner.html?utm_source=chatgpt.com "ExprPlanner in datafusion::logical_expr::planner - Rust"
[13]: https://docs.rs/datafusion/latest/datafusion/?utm_source=chatgpt.com "datafusion - Rust"
[14]: https://datafusion.apache.org/ballista/user-guide/python.html "Ballista Python Bindings — Apache DataFusion Ballista  documentation"
