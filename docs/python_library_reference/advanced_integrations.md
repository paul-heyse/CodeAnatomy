## 0) Integration invariants for your codebase

### 0.1 Batch ABI: Arrow C Stream / PyCapsule as the internal interchange standard across DF/Ibis/PyArrow

Your “wire format” should be **Arrow C Stream** exposed via the **Arrow PyCapsule Interface**:

* Arrow C Stream semantics: a streaming source of chunks (same schema each chunk), pulled via a blocking iteration function. ([Apache Arrow][1])
* PyCapsule standardizes *names* and *lifetime semantics* (destructor calls release; consumer must null the release callback after moving data; capsules are single-consumption). ([Apache Arrow][2])
* DataFusion DataFrames implement `__arrow_c_stream__` and can be consumed without materializing the full result. ([Apache DataFusion][3])
* PyArrow can create a `RecordBatchReader` from *any* object implementing `__arrow_c_stream__` (including DataFusion). ([Apache DataFusion][3])
* Ibis exposes a uniform “streaming reader” surface: `to_pyarrow_batches(...)` returns a `RecordBatchReader`, and `to_parquet_dir` delegates to `pyarrow.dataset.write_dataset`. ([Ibis][4])

#### Canonical interchange types (what your internal APIs should accept)

In practice you standardize on two shapes:

1. **Arrow stream** (preferred): `RecordBatchReader` or “ArrowStreamExportable” (`__arrow_c_stream__`)
2. **Materialized** (debug/small only): `pa.Table` / `pa.RecordBatch`

**Why `RecordBatchReader` is the canonical internal type**
It’s the one type that:

* can be produced by DataFusion streaming (`pa.RecordBatchReader.from_stream(df)`), ([Apache DataFusion][3])
* can be produced by Ibis (`to_pyarrow_batches`), ([Ibis][4])
* can be consumed directly by `pyarrow.dataset.write_dataset`. ([Apache Arrow][5])

#### “One function to normalize everything into a reader”

This adapter is the single most important integration utility in your stack:

```python
from __future__ import annotations
from typing import Any, Iterable, Optional
import pyarrow as pa
import pyarrow.dataset as ds

def to_reader(obj: Any, *, schema: Optional[pa.Schema] = None) -> pa.RecordBatchReader:
    """
    Normalize common producers (DataFusion DF, Ibis Table, PyArrow Scanner, etc.)
    into a PyArrow RecordBatchReader.
    """
    # 1) Already a reader
    if isinstance(obj, pa.RecordBatchReader):
        return obj

    # 2) Any Arrow PyCapsule Stream producer
    #    (includes DataFusion DataFrame implementing __arrow_c_stream__)
    if hasattr(obj, "__arrow_c_stream__"):
        return pa.RecordBatchReader.from_stream(obj, schema=schema)

    # 3) Ibis table expression (stream surface)
    if hasattr(obj, "to_pyarrow_batches"):
        # note: chunk_size is a hint; backend may ignore it
        return obj.to_pyarrow_batches()

    # 4) PyArrow Scanner
    if isinstance(obj, ds.Scanner):
        return obj.to_reader()

    # 5) Iterable of RecordBatches
    if schema is None:
        raise TypeError("schema is required when adapting an iterable of RecordBatch")
    return pa.RecordBatchReader.from_batches(schema, obj)  # type: ignore[arg-type]
```

Key correctness notes you should codify:

* **Do not reuse** a PyCapsule stream object after consumption (single-consumption semantics). ([Apache Arrow][2])
* Requested schema negotiation is **best-effort** and only for “alternate representations,” not arbitrary reshaping; incompatible requests should raise. ([Apache Arrow][2])

---

### 0.2 Streaming-first: avoid table materialization unless required; “reader/batches” as the default result type

The central integration rule:

> **Everything produces a stream unless you have an explicit reason to materialize.**

Concrete “streaming-first” levers:

* **DataFusion → stream**: `pa.RecordBatchReader.from_stream(df)` produces batches lazily, and `df.execute_stream_partitioned()` exists when partition boundaries matter. ([Apache DataFusion][3])
* **DataFusion → materialize** is still available (`collect()`, `to_arrow_table()`), but `pa.table(df)` collects eagerly (it’s a deliberate boundary). ([Apache DataFusion][3])
* **Ibis → stream**: `to_pyarrow_batches(...)` returns a `RecordBatchReader` and executes eagerly (so treat it as “execution now,” but “streaming output”). ([Ibis][4])
* **Stream → sink**: `pyarrow.dataset.write_dataset` accepts `RecordBatchReader` directly. ([Apache Arrow][5])

#### Materialization boundaries you should make explicit

Only allow `pa.Table` materialization for:

1. **Canonical global sorts** (when needed for deterministic IDs/ordering)
2. **Non-streaming operators** (e.g., Acero `order_by` accumulates all data; no larger-than-memory sort) ([Apache Arrow][6])
3. **Small debug artifacts** (golden snapshots, quick previews)

#### Streaming pipeline skeleton (the “default” in your build system)

```python
import pyarrow.dataset as ds

reader = to_reader(rule_output)  # DataFusion DF or Ibis Table
ds.write_dataset(
    reader,
    base_dir=out_dir,
    format="parquet",
    use_threads=True,
    preserve_order=False,   # see determinism tiers
)
```

`preserve_order` is a performance/correctness dial you must set deliberately. ([Apache Arrow][5])

---

### 0.3 Determinism tiers (cheap implicit ordering → preserved writer order → canonical global sort)

Your system should define an explicit “ordering tier” per product.

#### Tier 1: Implicit scan ordering (cheap, conditional)

Use when you need stable-ish ordering *without global sorts* and the plan is simple.

* In Acero scans, `require_sequenced_output=true` yields batches sequentially; `implicit_ordering=true` augments batches with fragment/batch indices “to enable stable ordering for simple ExecPlans.” ([Apache Arrow][7])
* These are exposed at least at the engine level (Acero/dataset scanner options); treat them as part of your scan policy. ([Apache Arrow][7])

Use this tier for:

* provenance-stable outputs
* “append-only-ish” datasets where exact ordering is not semantically meaningful

#### Tier 2: Writer-preserved order (guaranteed, slower)

Use when ordering has semantic meaning and you do *not* want to sort.

* `pyarrow.dataset.write_dataset(..., preserve_order=True)` guarantees row order even with threads, but “may cause notable performance degradation.” ([Apache Arrow][5])

Also note that multi-threaded writes can change order when `preserve_order=False`. ([Apache Arrow][8])

#### Tier 3: Canonical global sort (unambiguous, may require materialization)

Use when determinism must be absolute (IDs, stable hashes, canonical “winner tables”).

* `pyarrow.compute.sort_indices` computes indices that define a **stable sort** of an array/record batch/table. ([Apache Arrow][9])
* Apply indices via `take` (kernel lane) and then write.

```python
import pyarrow.compute as pc

ix = pc.sort_indices(table, sort_keys=[("stable_id", "ascending")])
sorted_table = pc.take(table, ix)
```

The stability of `sort_indices` is the key property you rely on. ([Apache Arrow][9])

Also: do **not** rely on Acero `order_by` for large datasets; it accumulates all data and “larger-than-memory sort is not currently supported.” ([Apache Arrow][6])

---

### 0.4 Single-box concurrency budget: Arrow CPU/IO pools + DataFusion partitions must be owned centrally

#### Arrow has *two* global thread pools

* CPU pool: `pyarrow.set_cpu_count(count)` sets threads used in parallel CPU operations. ([Apache Arrow][10])
* IO pool: `pyarrow.io_thread_count()` is used implicitly by operations like dataset scanning and can be changed via `set_io_thread_count`. ([Apache Arrow][11])

#### DataFusion parallelizes via partitions

* DataFusion “uses partitions to parallelize work”; `SessionConfig.with_target_partitions(N)` controls how many partitions can run concurrently. ([Apache DataFusion][12])

DataFusion’s configuration guide shows:

* building a `SessionContext` with `SessionConfig` + `RuntimeEnvBuilder`, and
* tuning partitions + repartition toggles for joins/aggs/windows. ([Apache DataFusion][12])

#### A practical “budgeting rule” for your runtime profile

You want to avoid:
**(DataFusion partitions) × (Arrow CPU threads)** oversubscribing cores.

A conservative approach for a machine with `C` cores:

* Pick DataFusion partitions `P ≈ C` (or slightly higher if IO-bound)
* Set Arrow CPU threads `T ≈ max(1, C / max(1, P_active))`
* Set Arrow IO threads based on storage latency (higher for remote object stores)

And always record these values in the run artifacts (see 0.5).

---

### 0.5 Versioned runtime profile: config keys are part of correctness (cache keys + plan shape)

Your “runtime profile” is not just performance tuning. It is part of **semantic reproducibility**:

#### DataFusion: config is explicitly key/value, and it changes behavior

* `SessionConfig.set(key, value)` sets arbitrary config keys. ([Apache DataFusion][13])
* DataFusion’s config docs show an example mixing structured setters with `.set("datafusion.execution.parquet.pushdown_filters", "true")`. ([Apache DataFusion][12])
* Core DataFusion also documents that runtime configs can be set via SQL `SET ...`, e.g., memory limits and caches. ([Apache DataFusion][14])

This means your cache keys and plan-fingerprint artifacts must include:

* `SessionConfig` structured fields (batch_size, target_partitions, repartition toggles)
* *all* `.set(k,v)` overrides
* `RuntimeEnvBuilder` choices (spill pool, disk manager, etc.) ([Apache DataFusion][12])

#### SQLGlot: your optimizer pipeline is part of correctness

SQLGlot’s optimizer:

* has a default ordered rule list (`qualify`, pushdowns, unnesting, simplify, etc.)
* warns that many rules require qualification and “Do not remove `qualify` … unless you know what you’re doing”
* accepts the original SQL string for error highlighting (important for deterministic debug bundles) ([SQLGlot][15])

Therefore your profile must pin:

* SQLGlot version
* `read_dialect` / `write_dialect`
* optimizer rule list (or rule hash)
* generator options (`pretty`, `identify`, strictness)
* schema snapshot used for qualification/type inference

#### PyArrow: writer policy affects both order and layout

`write_dataset` writer knobs like `use_threads` and `preserve_order` affect ordering and performance, and must be recorded. ([Apache Arrow][5])

#### Minimal runtime profile “shape” you should implement

At minimum, version and hash:

* `pyarrow`: cpu/io threads + memory pool selection
* `datafusion`: SessionConfig + RuntimeEnvBuilder + config kvs + SQLOptions surfaces
* `sqlglot`: dialects + rules + kwargs + generator options
* `ibis`: any options that change compilation shape (e.g., `ibis.options.sql.fuse_selects`)

---

## 1) Topology map: the three planes

### 1.1 Control plane: compilation policies, dialect routing, runtime profile selection

**Responsibilities**

* Choose the **runtime profile** for the run (dev/perf/strict)
* Build the **execution substrate**:

  * DataFusion `SessionContext(config, runtime)` ([Apache DataFusion][12])
  * attach Ibis to the same session (`ibis.datafusion.connect(...)`) ([Ibis][16])
  * set Arrow thread pools (`set_cpu_count`, `set_io_thread_count`) ([Apache Arrow][10])
* Route and enforce **dialect policy**:

  * Ibis IR → SQLGlot AST checkpoint (`con.compiler.to_sqlglot`) (your compiler boundary) ([Ibis][17])
  * SQLGlot optimizer pipeline pinned (qualify + pushdowns + simplify, etc.) ([SQLGlot][15])
  * final SQL emission for DataFusion execution

**Outputs**

* A fully configured `(df_ctx, ibis_con, sql_policy_engine, arrow_runtime)` bundle
* A *hashable* “profile fingerprint” included in cache keys and artifact bundle IDs

---

### 1.2 Data plane: streaming batches, dataset scans, engine execution

**Responsibilities**

* Ingest sources via PyArrow Dataset:

  * dataset discovery (quick path or factory path)
  * scan with projection/filter pushdown and tuned readahead/threads ([Apache Arrow][11])
* Execute relational logic via:

  * DataFusion DataFrames, streaming via Arrow C Stream (`pa.RecordBatchReader.from_stream(df)`) ([Apache DataFusion][3])
  * Ibis expressions, streaming via `to_pyarrow_batches()` ([Ibis][4])
* Materialize products primarily via:

  * `pyarrow.dataset.write_dataset(reader, ...)` streaming writes ([Apache Arrow][5])

**Key integration boundary**
Everything in the data plane should normalize to `RecordBatchReader` (or `__arrow_c_stream__`) using a single adapter (see 0.1). ([Apache Arrow][18])

---

### 1.3 Artifact plane: plans, metrics, schemas, fingerprints, lineage, metadata sidecars

This plane is what makes your system *reviewable*, *reproducible*, and *incrementally rebuildable*.

**Artifacts you should standardize**

* **Runtime profile snapshot** (serialized + hashed)
* **Plan artifacts**

  * DataFusion: logical/optimized/physical plan + explain/analyze
  * SQLGlot: canonical AST serialization (serde) + emitted SQL
* **Schema artifacts**

  * PyArrow schema (including metadata/extension types if you use them)
  * DataFusion table schemas (via registration / info schema)
* **Lineage artifacts**

  * SQLGlot column lineage for required-column extraction and audit
* **Dataset manifests**

  * `file_visitor` metadata collection when writing datasets (and optionally `_common_metadata`/`_metadata` when bounded) ([Apache Arrow][5])
* **Determinism proof**

  * ordering tier used
  * if tier 3: stable sort keys and sort_indices usage ([Apache Arrow][9])

**Why this is non-optional**

* DataFusion config keys influence plan behavior and performance (so they must be captured). ([Apache DataFusion][12])
* SQLGlot’s rewrite pipeline is the definition of “canonical query identity” (so it must be pinned and captured). ([SQLGlot][15])
* Arrow write order is explicitly conditional (`preserve_order`), and multi-threading can change order, so ordering policy must be captured for every dataset product. ([Apache Arrow][5])

---

### A concrete “three-plane” object model (implementation-ready)

If you want a crisp internal API boundary, it looks like:

```python
@dataclass(frozen=True)
class ControlPlane:
    profile: RuntimeProfile          # hashable / serializable
    sql_policy: SqlPolicyEngine      # SQLGlot pipeline config
    df_ctx: "datafusion.SessionContext"
    ibis_con: "ibis.backends.datafusion.Backend"

@dataclass(frozen=True)
class DataPlane:
    def stream(self, obj) -> pa.RecordBatchReader: ...
    def scan_dataset(self, ref, *, columns, filter) -> pa.RecordBatchReader: ...
    def write_dataset(self, reader, out_ref, *, ordering_tier) -> None: ...

@dataclass(frozen=True)
class ArtifactPlane:
    def capture_profile(self, profile) -> None: ...
    def capture_plans(self, df_or_sqlglot_ast) -> None: ...
    def capture_lineage(self, sqlglot_ast) -> None: ...
    def capture_dataset_manifest(self, written_files) -> None: ...
```

This model forces the invariants:

* **control plane builds the substrate** (and pins policy)
* **data plane streams** (and only materializes on explicit boundaries)
* **artifact plane makes everything reproducible**

If you want, the next step is to deep dive **Section 2 (Unified execution substrate)** and implement the “single object” that owns: `SessionContext` + Ibis backend + Arrow pools + SQLGlot policy + artifact sinks, with a deterministic profile hash used across all cache keys and dataset output directory naming.

[1]: https://arrow.apache.org/docs/format/CStreamInterface.html?utm_source=chatgpt.com "The Arrow C stream interface — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html "The Arrow PyCapsule Interface — Apache Arrow v22.0.0"
[3]: https://datafusion.apache.org/python/user-guide/dataframe/index.html "DataFrames — Apache Arrow DataFusion  documentation"
[4]: https://ibis-project.org/reference/expression-tables "expression-tables – Ibis"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html?utm_source=chatgpt.com "pyarrow.dataset.write_dataset — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.OrderByNodeOptions.html?utm_source=chatgpt.com "pyarrow.acero.OrderByNodeOptions — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/cpp/api/dataset.html?utm_source=chatgpt.com "Dataset — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/_modules/pyarrow/dataset.html?utm_source=chatgpt.com "Source code for pyarrow.dataset - Apache Arrow"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.sort_indices.html?utm_source=chatgpt.com "pyarrow.compute.sort_indices — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/python/generated/pyarrow.set_cpu_count.html?utm_source=chatgpt.com "pyarrow.set_cpu_count — Apache Arrow v22.0.0"
[11]: https://arrow.apache.org/docs/python/generated/pyarrow.io_thread_count.html?utm_source=chatgpt.com "pyarrow.io_thread_count — Apache Arrow v22.0.0"
[12]: https://datafusion.apache.org/python/user-guide/configuration.html "Configuration — Apache Arrow DataFusion  documentation"
[13]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html?utm_source=chatgpt.com "datafusion.context — Apache Arrow DataFusion documentation"
[14]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[15]: https://sqlglot.com/sqlglot/optimizer/optimizer.html "sqlglot.optimizer.optimizer API documentation"
[16]: https://ibis-project.org/backends/datafusion?utm_source=chatgpt.com "DataFusion - Ibis"
[17]: https://ibis-project.org/posts/does-ibis-understand-sql/?utm_source=chatgpt.com "Does Ibis understand SQL?"
[18]: https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatchReader.html?utm_source=chatgpt.com "pyarrow.RecordBatchReader — Apache Arrow v22.0.0"

## 2) Unified execution substrate: DataFusion `SessionContext` + Ibis backend binding

Goal: **one “engine session”** that owns *runtime policy + data registration + safety gates*, and that **both Ibis and direct DataFusion calls share**.

---

### 2.1 `SessionContext` construction + configuration (`SessionConfig`, `RuntimeEnvBuilder`)

#### 2.1.1 The three objects you treat as your substrate

* **`SessionConfig`**: “logical” config knobs (batch size, target partitions, repartition toggles, catalog defaults, etc.) and a generic `.set(key, value)` for arbitrary config keys. ([Apache DataFusion][1])
* **`RuntimeEnvBuilder`**: “runtime resources” (disk manager + memory pool/spill pool). ([Apache DataFusion][1])
* **`SessionContext(config, runtime)`**: the session kernel; everything hangs off it. ([Apache DataFusion][1])

#### 2.1.2 Canonical construction pattern (pin everything in one place)

DataFusion’s config guide shows creating a context with explicit runtime + config settings, including spill pool and config keys such as Parquet pushdown filters. ([Apache DataFusion][1])

```python
from datafusion import RuntimeEnvBuilder, SessionConfig, SessionContext

runtime = (
    RuntimeEnvBuilder()
    .with_disk_manager_os()
    .with_fair_spill_pool(10_000_000)
)

config = (
    SessionConfig()
    .with_create_default_catalog_and_schema(True)
    .with_default_catalog_and_schema("foo", "bar")
    .with_target_partitions(8)
    .with_information_schema(True)
    .with_repartition_joins(False)
    .with_repartition_aggregations(False)
    .with_repartition_windows(False)
    .with_parquet_pruning(False)
    .set("datafusion.execution.parquet.pushdown_filters", "true")
)

ctx = SessionContext(config, runtime)
```

That’s not just “tuning”: it’s the reproducible *contract* for execution behavior. ([Apache DataFusion][1])

#### 2.1.3 Runtime memory/spill design: choose your pool intentionally

`RuntimeEnvBuilder` exposes:

* disk manager modes (`with_disk_manager_disabled`, `with_disk_manager_os`, `with_disk_manager_specified`, `with_temp_file_path`) ([Apache DataFusion][2])
* memory pool choices:

  * `with_fair_spill_pool(size)`: best when you expect multiple spillable operators, but docs warn it can cause spills even if there would have been enough free memory because memory is reserved for other operators. ([Apache DataFusion][2])
  * `with_greedy_memory_pool(size)`: best for “no spill / single spillable operator” workloads. ([Apache DataFusion][2])
  * `with_unbounded_memory_pool()`: you generally only want this in controlled environments. ([Apache DataFusion][2])

#### 2.1.4 Concurrency knobs live here too

DataFusion’s configuration guide explicitly states it uses partitions to parallelize work and shows:

* `with_target_partitions(n)` for concurrency
* enabling repartitioning toggles for joins/aggs/windows
* manual `df.repartition(...)` and `df.repartition_by_hash(...)` when you need exact shaping ([Apache DataFusion][1])

---

### 2.2 Object stores registration and policy encapsulation

#### 2.2.1 Supported object stores (Python bindings)

DataFusion’s data sources guide lists supported object stores:
`AmazonS3`, `GoogleCloud`, `Http`, `LocalFileSystem`, `MicrosoftAzure`. ([Apache DataFusion][3])

#### 2.2.2 Registering object stores (the one method you standardize)

`SessionContext.register_object_store(schema: str, store: Any, host: str | None = None)` installs an object store into the session. ([Apache DataFusion][2])

The user guide shows an S3 example:

* build an `AmazonS3` object store
* register it with `ctx.register_object_store("s3://", s3, None)`
* then register parquet at `s3://bucket/` and query. ([Apache DataFusion][3])

```python
from datafusion.object_store import AmazonS3
import os

s3 = AmazonS3(
    bucket_name="yellow-trips",
    region="us-east-1",
    access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
)

ctx.register_object_store("s3://", s3, None)
ctx.register_parquet("trips", "s3://yellow-trips/")
```

([Apache DataFusion][3])

#### 2.2.3 Why object stores belong in the substrate object

Because registration affects:

* how path URIs resolve
* remote IO performance characteristics
* reproducibility (credentials/endpoint/region differences)

So: treat object store config as *part of your runtime profile artifact*.

---

### 2.3 Namespacing via catalogs/schemas/tables/views (per-run and per-product isolation)

#### 2.3.1 Default namespace + enabling information schema

DataFusion comes with a default catalog and schema (the docs describe a catalog/schema/table hierarchy and that a default `SessionContext` has a single catalog and schema). ([Apache DataFusion][3])

Your substrate should typically set:

* `with_create_default_catalog_and_schema(True)`
* `with_default_catalog_and_schema(catalog, schema)`
* `with_information_schema(True)` for introspection surfaces ([Apache DataFusion][1])

#### 2.3.2 Registering tables: one entrypoint, many producers

`SessionContext.register_table(name, table)` accepts:

* a DataFusion `Table`
* a `TableProviderExportable` (PyCapsule)
* a `DataFrame` (converted to a view/table wrapper)
* a `pyarrow.dataset.Dataset` ([Apache DataFusion][2])

This matters because your ingestion layer will produce *different* “table-like” things:

* Arrow datasets (artifact directories)
* DataFusion DataFrames (derived logical products)
* external table providers (future)

#### 2.3.3 Multi-file datasets as single tables (artifact directories)

`register_listing_table(name, path, ...)` registers multiple files as a single table and explicitly says it assembles multiple files from an `ObjectStore`. ([Apache DataFusion][2])

For “graph product build outputs” stored as partitioned Parquet directories, this is the native ingestion mechanism.

#### 2.3.4 Views: the “cheap named intermediate”

Use `register_view(name, df)` to expose a DataFrame as a view and query it via SQL. The user guide shows end-to-end usage. ([Apache DataFusion][4])

This is a core technique for:

* debugging intermediate stages
* giving your compilation system stable “names” for subplans without writing Parquet

#### 2.3.5 Recommended naming scheme for your system

Use a stable hierarchy:

* **catalog**: `code_anatomy` (or environment)
* **schema**: `run_<run_id>` or `prod_<contract_version>`
* **table/view**: `edges_<product>`, `nodes_<product>`, `props_<product>`

…and enforce it in your substrate so all consumers (Ibis + direct DF SQL) see the same namespace.

---

### 2.4 SQL safety surfaces (`SQLOptions`) for any “agent SQL” lane

#### 2.4.1 SQLOptions defaults are permissive — so don’t rely on them

`SQLOptions` defaults:

* DDL allowed
* DML allowed
* statements (`SET`, `BEGIN TRANSACTION`, etc.) allowed ([Apache DataFusion][2])

You should treat those defaults as **unsafe** for any surface that may execute untrusted SQL (LLM, user-supplied).

#### 2.4.2 The safe API: `sql_with_options(...)`

`SessionContext.sql_with_options(query, options, ...)` validates whether the query is allowed by the options **before** producing a DataFrame. ([Apache DataFusion][2])

```python
from datafusion import SessionContext, SQLOptions

ctx = SessionContext()

untrusted = (
    SQLOptions()
    .with_allow_ddl(False)
    .with_allow_dml(False)
    .with_allow_statements(False)
)

df = ctx.sql_with_options("SELECT * FROM some_table", untrusted)
```

([Apache DataFusion][2])

#### 2.4.3 Parameter substitution: scalar params vs macro substitution

The `sql(...)` and `sql_with_options(...)` APIs accept:

* `param_values`: substitution of scalar values after parsing
* `named_params`: string or DataFrame substitution in the query string ([Apache DataFusion][2])

Operationally:

* treat `param_values` as the “safe scalar parameter lane”
* treat `named_params` as a “macro expansion lane” (powerful, must validate carefully)

#### 2.4.4 `sql(query, options=...)` also exists

`sql(query, options=..., ...)` can validate against options too, but the docs explicitly point to `sql_with_options` and note that `sql` includes in-memory default implementations for DDL/DML (meaning it’s not purely “compile a query”). ([Apache DataFusion][2])

---

### 2.5 Ibis attaches to DataFusion: pass `SessionContext` into `ibis.datafusion.connect`

#### 2.5.1 Ibis DataFusion backend supports `connect(SessionContext)`

Ibis DataFusion backend `connect(config)` accepts either:

* a mapping of table names to files (deprecated in Ibis 10.0), or
* a DataFusion `SessionContext` instance ([Ibis][5])

Docs include the exact pattern:

```python
from datafusion import SessionContext
ctx = SessionContext()
_ = ctx.from_pydict({"a": [1, 2, 3]}, "mytable")
import ibis
con = ibis.datafusion.connect(ctx)
con.list_tables()
```

([Ibis][5])

#### 2.5.2 Why this is the correct binding in your architecture

Because it guarantees:

* the same object stores and tables registered in DataFusion are visible to Ibis
* the same session config/runtime spill policy is applied regardless of whether the plan originated from Ibis or direct DF calls

#### 2.5.3 SQLGlot AST execution lane through Ibis DataFusion backend

Ibis DataFusion backend `raw_sql(query)` accepts `str | sge.Expression` (SQLGlot AST). ([Ibis][5])

That’s your “best-in-class” boundary for deterministic SQL shapes:

* compile in Ibis → checkpoint SQLGlot AST → apply SQLGlot policy passes → execute the AST directly via `raw_sql(sge.Expression)` (no fragile string SQL required).

---

## 3) Interchange & streaming interfaces (the “glue” layer)

### 3.1 Arrow C Data / C Stream / PyCapsule protocols: what to standardize

#### 3.1.1 The required capsule names and why they exist

Arrow’s PyCapsule interface standardizes capsule names:

* `arrow_schema`, `arrow_array`, `arrow_array_stream`, `arrow_device_array`, `arrow_device_array_stream` ([Apache Arrow][6])

Capsules attach names + destructors, and the spec explicitly frames them as safer than passing pointers as integers. ([Apache Arrow][6])

#### 3.1.2 The dunder methods (CPU + device)

CPU protocols:

* `__arrow_c_schema__`
* `__arrow_c_array__(requested_schema=None)`
* `__arrow_c_stream__(requested_schema=None)` ([Apache Arrow][6])

Device protocols:

* `__arrow_c_device_array__(requested_schema=None, **kwargs)`
* `__arrow_c_device_stream__(requested_schema=None, **kwargs)` ([Apache Arrow][6])

#### 3.1.3 Requested schema negotiation is not arbitrary reshaping

The spec is explicit:

* requested schema is for negotiating between multiple compatible Arrow representations
* if incompatible (e.g., different number of fields), the producer **should raise**
* “not meant to allow arbitrary schema transformations” ([Apache Arrow][6])

**System rule:** do transformations in your IR/engine layer, not via requested_schema.

---

### 3.2 DataFusion → Arrow streaming

#### 3.2.1 `DataFrame.__arrow_c_stream__` is the canonical streaming export

DataFusion docs state:

* DataFrames implement `__arrow_c_stream__`, enabling zero-copy, lazy streaming into Arrow-based Python libraries; batches are produced on demand. ([Apache DataFusion][7])
* Invoking `__arrow_c_stream__` triggers query execution, but yields batches incrementally (not materialized all at once). ([Apache DataFusion][8])

#### 3.2.2 Three streaming surfaces you should standardize

**(A) PyArrow reader**

```python
import pyarrow as pa
reader = pa.RecordBatchReader.from_stream(df)
for batch in reader:
    ...
```

DataFusion docs show this exact pattern. ([Apache DataFusion][7])

**(B) DataFrame iteration**
DataFusion DataFrames are iterable and yield `datafusion.RecordBatch` lazily. ([Apache DataFusion][7])

**(C) Explicit streams (partition-aware)**

* `df.execute_stream()`
* `df.execute_stream_partitioned()` returns one stream per partition ([Apache DataFusion][7])

Use (C) when you need parallel downstream sinks keyed to partitions.

#### 3.2.3 The “materialization boundary” callouts

* `pa.table(df)` collects the entire DataFrame eagerly into a PyArrow Table. ([Apache DataFusion][7])
  Make this an explicit, reviewed boundary in your pipeline.

---

### 3.3 Ibis → Arrow streaming (`to_pyarrow_batches`, chunk_size as hint)

#### 3.3.1 The primary streaming API

Ibis generic expressions expose:
`to_pyarrow_batches(limit=None, params=None, chunk_size=1000000, **kwargs) -> RecordBatchReader` and explicitly states it’s eager and executes immediately. ([Ibis][9])

That is your standard “Ibis → Arrow stream” boundary.

#### 3.3.2 Minimal pipeline pattern (Ibis → dataset write)

```python
import pyarrow.dataset as ds

reader = expr.to_pyarrow_batches(chunk_size=250_000)
ds.write_dataset(reader, base_dir="out/", format="parquet")
```

RecordBatchReader output and write_dataset streaming inputs are both first-class. ([Ibis][9])

#### 3.3.3 Chunk size semantics

Ibis defines `chunk_size` as “maximum number of rows in each returned record batch.” ([Ibis][9])
In practice, backends can have their own batching behavior, so treat it as a **hint** and build sinks that don’t assume exact batch sizes.

---

### 3.4 PyArrow ingestion of Arrow streams

#### 3.4.1 `pyarrow.table(...)` accepts PyCapsule producers

`pyarrow.table(...)` accepts “any tabular object implementing the Arrow PyCapsule Protocol,” including objects with `__arrow_c_array__`, `__arrow_c_device_array__`, or `__arrow_c_stream__`. ([Apache Arrow][10])

Use this for “debug materialization,” not as your default path.

#### 3.4.2 `RecordBatchReader.from_stream(...)` is the universal adapter

PyArrow documents `RecordBatchReader.from_stream(data, schema=None)`:

* accepts any object implementing `__arrow_c_stream__`
* optional schema “cast” if supported by the stream object ([Apache Arrow][11])

This is your canonical adapter:

* DataFusion DF → RecordBatchReader
* Polars DF → RecordBatchReader
* any compliant producer → RecordBatchReader

#### 3.4.3 `Scanner.from_batches(...)`: the “single-pass source → scan policy → sink” bridge

PyArrow’s dataset scanner supports:
`Scanner.from_batches(source, schema=None, ...)` and explicitly says:

* the scanner can be used only once
* it is intended to support writing a dataset from a source which can be read only once (RecordBatchReader or generator)
* `source` can be a RecordBatchReader, any object implementing Arrow PyCapsule stream protocol, or an iterator of RecordBatches (schema required for iterator) ([Apache Arrow][12])

This is extremely valuable in your system because it lets you apply **scan-time policy** (projection/filter/batch sizing/readahead) to a streaming producer before writing.

---

### 3.5 Dataframe interchange protocol (secondary interoperability path)

PyArrow supports the DataFrame interchange protocol:

* `from_dataframe(df, allow_copy=True)` builds a `pa.Table` from any object implementing `__dataframe__`. ([Apache Arrow][13])

This is useful when:

* a library supports `__dataframe__` but not Arrow PyCapsule stream export
* you want a fallback conversion lane (possibly with copying)

But: your **primary** interchange should still be Arrow C Stream via PyCapsule, because it’s Arrow-native and streaming-first.

---

### 3.6 Device arrays / future GPU lane (optional)

Arrow’s PyCapsule interface explicitly supports “device-aware” exchange:

* `__arrow_c_device_array__` and `__arrow_c_device_stream__`
* device producers/consumers should accept `**kwargs` with default None to allow future extensions
* CPU-only producers may implement either CPU-only methods or both CPU + device methods
* device-aware producers can choose to implement only device methods if they never have CPU pointers ([Apache Arrow][6])

For your roadmap:

* You can keep the same substrate API shape (`to_reader` / `to_stream`) and later add a device-stream branch (e.g., CUDA) without changing higher-level orchestration.

---

### 3.7 Lifetime/ownership rules (“who frees memory”) — segfault avoidance checklist

These are non-negotiable if you exchange streams across multiple libraries.

#### 3.7.1 PyCapsule lifetime semantics (authoritative rules)

* Exported capsules should have a destructor that calls the Arrow struct `release` callback if it’s not null (prevents leaks if never consumed). ([Apache Arrow][6])
* Consumers should “move the data” and set the release callback to null to avoid double-free. ([Apache Arrow][6])
* PyCapsules defined by this protocol can only be consumed once. ([Apache Arrow][6])

#### 3.7.2 Practical “safe usage” checklist for your integration code

* **Never reuse** a stream capsule or producer object expecting it to be rewindable. (Treat streams as one-shot.) ([Apache Arrow][6])
* Prefer `RecordBatchReader` as the normalized internal type; it makes “one-shot” explicit and composes with writers. ([Apache Arrow][14])
* Keep “requested_schema” usage minimal; don’t rely on it for transformations. ([Apache Arrow][6])
* For any custom producer you implement: ensure capsule destructors call `release` if unconsumed. ([Apache Arrow][6])

---

If you want the next deep-dive increment after these: Section **4 (Ingestion pipeline: PyArrow Dataset discovery → DF/Ibis registration)** is where we turn this into “end-to-end operational patterns” (factory-based dataset discovery, fragment pruning, scan policy, and streaming materialization), and we can write it in the same “API surface + power knobs + failure modes + minimal runnable snippets” style.

[1]: https://datafusion.apache.org/python/user-guide/configuration.html "Configuration — Apache Arrow DataFusion  documentation"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[3]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/python/user-guide/common-operations/views.html "Registering Views — Apache Arrow DataFusion  documentation"
[5]: https://ibis-project.org/backends/datafusion?utm_source=chatgpt.com "DataFusion - Ibis"
[6]: https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html "The Arrow PyCapsule Interface — Apache Arrow v22.0.0"
[7]: https://datafusion.apache.org/python/user-guide/dataframe/index.html "DataFrames — Apache Arrow DataFusion  documentation"
[8]: https://datafusion.apache.org/python/user-guide/io/arrow.html "Arrow — Apache Arrow DataFusion  documentation"
[9]: https://ibis-project.org/reference/expression-generic "expression-generic – Ibis"
[10]: https://arrow.apache.org/docs/python/generated/pyarrow.table.html?utm_source=chatgpt.com "pyarrow.table — Apache Arrow v22.0.0"
[11]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.RecordBatchStreamReader.html?utm_source=chatgpt.com "pyarrow.ipc.RecordBatchStreamReader - Apache Arrow"
[12]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html "pyarrow.dataset.Scanner — Apache Arrow v22.0.0"
[13]: https://arrow.apache.org/docs/python/interchange_protocol.html?utm_source=chatgpt.com "Dataframe Interchange Protocol — Apache Arrow v22.0.0"
[14]: https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatchReader.html?utm_source=chatgpt.com "pyarrow.RecordBatchReader — Apache Arrow v22.0.0"


## 4) Ingestion pipeline: PyArrow Dataset discovery → DataFusion / Ibis registration

This section is the “operational spine” for your graph-product storage model: **artifact directories of Parquet files** become **PyArrow Datasets**, which become **streaming readers**, which either (a) get written as new datasets, or (b) get registered into **DataFusion** and surfaced through **Ibis**—without losing determinism, pushdown performance, or debuggability.

---

### 4.0 Mental model and the three output “shapes”

You want every ingestion path to produce one of:

1. **`pyarrow.dataset.Dataset`** (logical multi-file table; fragments + partitioning + schema)
2. **`pyarrow.dataset.Scanner`** (materialized scan plan: projection + filter + batching + readahead + threads) ([Apache Arrow][1])
3. **`pyarrow.RecordBatchReader`** (streaming batches; default interchange/sink boundary)

Everything else (Tables) is a deliberate materialization boundary.

---

## 4.1 Resolve filesystem + dataset reference (local vs remote, URI vs abstract path)

### 4.1.1 Always normalize “URI → (FileSystem, abstract path)”

Use `pyarrow.fs.FileSystem.from_uri(...)` so your pipeline works uniformly for local paths, S3/GCS, and fsspec-backed stores. It returns **(filesystem, abstract path)** and documents recognized schemes. ([Apache Arrow][2])

```python
from pyarrow import fs

filesystem, path = fs.FileSystem.from_uri("s3://my-bucket/artifacts/run_123/edges/")
# filesystem: S3FileSystem, path: "my-bucket/artifacts/run_123/edges"
```

Filesystem invariants you should enforce in your codebase:

* paths are abstract `/`-separated, should not include `.` or `..` ([Apache Arrow][3])
* symlinks are dereferenced (don’t assume symlink boundaries for security) ([Apache Arrow][3])

### 4.1.2 Choosing the Dataset constructor: single path vs list of files vs selector

`pyarrow.dataset.dataset(...)` can take:

* a directory path (common for your artifact directories), or
* explicitly given files (must be on the same filesystem; passing URIs as paths is not allowed in the multi-file list case) ([Apache Arrow][4])

If you have “subset-of-files” ingestion needs, prefer the **factory path** in §4.2.

---

## 4.2 Dataset discovery strategies (quick path vs factory path vs `_metadata` fast path)

### 4.2.1 Quick path: `ds.dataset(...)`

Use this when:

* the dataset directory is well-formed,
* schema drift is rare,
* you don’t need pre-inspection artifacts.

The `dataset(...)` API exposes critical ingestion knobs:

* `partition_base_dir`: paths are stripped of this base before partitioning is applied; files not matching the prefix are **skipped for partition discovery** but still included without partition info ([Apache Arrow][4])
* `exclude_invalid_files` (default True in docs) ([Apache Arrow][4])

```python
import pyarrow.dataset as ds

dataset = ds.dataset(
    path,
    format="parquet",
    filesystem=filesystem,
    partition_base_dir=path,          # ensures partition parsing is relative to the dataset root
    exclude_invalid_files=True,
)
```

### 4.2.2 Factory path (recommended for “best-in-class”): `FileSystemDatasetFactory.inspect() → finish()`

Use this when you care about:

* deterministic schema unification,
* explicit handling of schema evolution,
* controlled discovery and ignore-prefix policy.

`FileSystemDatasetFactory` is built to discover and inspect schemas before creating a dataset. ([Apache Arrow][5])
You typically pair it with `FileSelector` and `FileSystemFactoryOptions`:

* `selector_ignore_prefixes` defaults to `['.', '_']` for discovered files/dirs ([Apache Arrow][6])
* `partition_base_dir`, `exclude_invalid_files`, `partitioning` / `partitioning_factory` are all first-class discovery controls ([Apache Arrow][6])

Schema inspection:

* `inspect(promote_options='default'|'permissive')` where:

  * **default**: types must match exactly (except null merges)
  * **permissive**: types are promoted when possible ([Apache Arrow][5])

```python
import pyarrow.dataset as ds
from pyarrow import fs

selector = fs.FileSelector(path, recursive=True)

opts = ds.FileSystemFactoryOptions(
    partition_base_dir=path,
    exclude_invalid_files=True,
    selector_ignore_prefixes=[".", "_"],
)

factory = ds.FileSystemDatasetFactory(filesystem, selector, ds.ParquetFileFormat(), opts)

schema = factory.inspect(promote_options="default")     # or "permissive"
dataset = factory.finish(schema=schema)                 # use inspected schema unless you override
```

**Why this matters to your repo:** the factory path gives you a stable, inspectable “ingestion compile step” that you can cache and emit as an artifact.

### 4.2.3 `_metadata` fast path: `ds.parquet_dataset("_metadata")`

If you maintain Parquet `_metadata` sidecars (see §4.6.4), you can build a dataset from them directly via `pyarrow.dataset.parquet_dataset(...)`.

This is the fastest “bootstrap large artifact directory” approach because you avoid scanning all files for schema/stats.

---

## 4.3 Partitioning, schema evolution, and “schema truth”

### 4.3.1 Partitioning is both an optimization and a correctness surface

Your artifact directories will typically be Hive-partitioned (`key=value` segments). Arrow’s dataset docs explicitly describe this scheme (order ignored, missing fields ignored). ([Apache Arrow][7])

**Critical correctness dial:** `partition_base_dir`
If you get it wrong, partition discovery can silently fail (files remain in dataset but without partition info). ([Apache Arrow][4])

### 4.3.2 Schema unification: strict vs permissive is an explicit policy

Two unification tools you should use deliberately:

* **Factory inspection**: `inspect(promote_options=...)` default vs permissive ([Apache Arrow][5])
* **Schema merge**: `pyarrow.unify_schemas(schemas, promote_options=...)` where:

  * default: only null can unify with another type
  * permissive: types promoted to a “greater common denominator”
  * result inherits metadata from first schema ([Apache Arrow][8])

```python
import pyarrow as pa

unified = pa.unify_schemas(list_of_schemas, promote_options="default")  # strict
```

### 4.3.3 Fragment schema drift: unify at scan time

Fragments can have different physical schemas. Both `Fragment.scanner` and `Scanner.from_fragment` accept a `schema` parameter explicitly described as “used to unify a Fragment to its Dataset’s schema.” ([Apache Arrow][9])

This is your “schema truth enforcement” step: if you’ve decided the dataset schema, pass it everywhere.

---

## 4.4 Fragment pruning: do less work before scanning

### 4.4.1 Coarse pruning: `Dataset.get_fragments(filter=...)`

`Dataset.get_fragments(filter=Expression)` returns fragments matching the optional filter, using either:

* `partition_expression`, or
* internal info like Parquet statistics. ([Apache Arrow][10])

```python
import pyarrow.dataset as ds

pred = (ds.field("kind") == "CALL")
fragments = list(dataset.get_fragments(filter=pred))
```

This is your best first-step for pruning huge datasets without scanning all row groups.

### 4.4.2 Fine pruning: Parquet row-group subset via `ParquetFileFragment.subset(...)`

`ParquetFileFragment.subset(filter=..., schema=...)` can create a fragment that views only the row groups satisfying a predicate **using Parquet RowGroup statistics**. ([Apache Arrow][11])

This is your “stats-driven second stage” pruning lever.

### 4.4.3 Provenance: fragment metadata surfaces you should log

`Fragment.partition_expression` (true for all data in the fragment) and `physical_schema` are explicit attributes. ([Apache Arrow][12])

Emit them in your artifact plane when debugging “why did this scan touch this file?”

---

## 4.5 Scan policy: `Scanner` is the ingestion workhorse

A `Scanner` is “the class that glues scan tasks, data fragments and data sources together.” ([Apache Arrow][1])
Treat it as your **materialized scan plan** object and standardize its knobs in your runtime profile.

### 4.5.1 Projection: minimize IO by default

`Scanner` accepts `columns` as:

* a list of column names (order and duplicates preserved), or
* a dict `{new_col: expression}` for derived projections. ([Apache Arrow][13])

It also supports special fields in projections:

* `__batch_index`, `__fragment_index`, `__last_in_fragment`, `__filename` ([Apache Arrow][13])

Most importantly: projected columns are “passed down to Datasets and fragments to avoid loading/copying/deserializing columns not required further down the compute chain.” ([Apache Arrow][13])

### 4.5.2 Filtering: pushdown when possible, fallback to batch filtering

Filter semantics are explicit:

* If possible, predicates are pushed down using partition info or internal metadata (e.g., Parquet statistics)
* Otherwise, Arrow filters the loaded RecordBatches before yielding them ([Apache Arrow][13])

### 4.5.3 Batch sizing and readahead are your primary memory/throughput dials

Scanner options (with explicit behavior notes):

* `batch_size`: if batches overflow memory, reduce batch_size ([Apache Arrow][13])
* `batch_readahead`: more read-ahead increases RAM usage but can improve IO utilization ([Apache Arrow][13])
* `fragment_readahead`: same tradeoff at the file level ([Apache Arrow][13])
* `use_threads=True`: uses max parallelism determined by CPU cores ([Apache Arrow][13])
* `cache_metadata=True`: can speed up repeated scans ([Apache Arrow][13])

### 4.5.4 Fragment-specific scan tuning: `ParquetFragmentScanOptions`

For remote storage (S3/GCS), `ParquetFragmentScanOptions.pre_buffer=True` is a major lever:

* it pre-buffers raw Parquet data instead of one read per column chunk
* improves high-latency filesystem performance by coalescing reads in parallel (background IO pool)
* trades memory for speed ([Apache Arrow][14])

---

## 4.6 Streaming materialization: Scanner → Reader → `write_dataset`

### 4.6.1 Always materialize as a stream by default: `scanner.to_reader()`

Scanner exposes `to_reader()` (“Consume this scanner as a RecordBatchReader”). ([Apache Arrow][13])

This is your canonical sink boundary because `write_dataset` accepts streaming inputs.

### 4.6.2 Batch provenance: `scan_batches()` and `TaggedRecordBatch`

If you need provenance, consume the scanner as tagged batches:

* `TaggedRecordBatch` is “a combination of a record batch and the fragment it came from.” ([Apache Arrow][15])

This is invaluable for:

* debugging pushdown/pruning behavior
* building deterministic “sharding” policies aligned with fragments

### 4.6.3 `write_dataset` is the canonical dataset sink — and it is *explicitly* order-sensitive

Key writer semantics:

* `use_threads=True` writes files in parallel; this **may change row order** if `preserve_order=False` ([Apache Arrow][16])
* `preserve_order=True` guarantees row order even with threads, but “may cause notable performance degradation.” ([Apache Arrow][16])
* `file_options` are “FileFormat specific write options, created using `FileFormat.make_write_options()`” ([Apache Arrow][16])
* `existing_data_behavior`:

  * `error` default
  * `overwrite_or_ignore` enables append workflows with unique basenames
  * `delete_matching` is partition overwrite (deletes partition dir on first encounter) ([Apache Arrow][16])

**Minimal streaming materialization**

```python
import pyarrow.dataset as ds

reader = dataset.scanner(
    columns=["src_id", "dst_id", "kind"],
    filter=(ds.field("kind") == "CALL"),
    batch_size=131_072,
    batch_readahead=16,
    fragment_readahead=4,
    use_threads=True,
    cache_metadata=True,
).to_reader()

ds.write_dataset(
    reader,
    base_dir="out/edges_call/",
    format="parquet",
    use_threads=True,
    preserve_order=False,
    existing_data_behavior="delete_matching",
)
```

(Every knob above is directly part of the scanner / writer contract.) ([Apache Arrow][13])

### 4.6.4 `file_visitor` is your manifest/sidecar hook

`write_dataset` calls `file_visitor` with a `WrittenFile` that has:

* `path`
* `metadata` (Parquet metadata; None for non-parquet)
  and explicitly notes that the metadata can be used to build a `_metadata` file. ([Apache Arrow][16])

---

## 4.7 Register into DataFusion and expose through Ibis

You have **two** reliable integration paths from PyArrow ingestion into DataFusion. Which one you choose depends on whether you trust DataFusion’s current PyArrow-dataset pushdowns for your workload.

### 4.7.1 Path A (stable and predictable): Pre-scan in PyArrow → `SessionContext.from_arrow(reader)`

DataFusion’s Arrow IO docs state:

* `SessionContext.from_arrow()` accepts any Python object implementing `__arrow_c_stream__` or `__arrow_c_array__` (struct array required for C array) ([Apache DataFusion][17])

So you can:

1. build a scanner (pushdowns + batching in PyArrow),
2. turn into a reader,
3. import into DataFusion as a DataFrame:

```python
from datafusion import SessionContext

ctx = SessionContext()

df = ctx.from_arrow(reader, name="edges_call")  # reader is Arrow C Stream-capable
```

This pattern is also framed more generally in DataFusion’s data sources guide: since DataFusion 42, anything that supports Arrow PyCapsule interface can be imported via `from_arrow()`. ([Apache DataFusion][18])

**Why this path is “best-in-class” today:** there are open issues indicating gaps in DataFusion’s ability to work with filtered PyArrow datasets and predicate pushdowns in some situations. ([GitHub][19])

### 4.7.2 Path B (convenient): `SessionContext.register_dataset(name, dataset)`

DataFusion Python exposes `SessionContext.register_dataset(name, dataset)` as an API surface. ([Apache Arrow][20])

Use this when:

* you want a named table in the DF catalog directly from a `pyarrow.dataset.Dataset`,
* and your workload doesn’t depend on nuanced pushdown behavior.

### 4.7.3 Bind Ibis to the *same* SessionContext

Ibis provides a DataFusion backend constructor from an existing `SessionContext`:

* `from_connection(cls, con)` “Create a DataFusion Backend from an existing SessionContext instance.” ([Ibis][21])

Once attached, your Ibis expressions and direct DataFusion SQL see the same registered tables/views/object stores.

---

## 4.8 Failure modes and “don’t step on rakes” checklist

### Schema drift and evolution

* If you want strict drift detection: `inspect(promote_options="default")` and `unify_schemas(promote_options="default")` will error on incompatible types (beyond null merges). ([Apache Arrow][5])
* If you want graceful evolution: use `"permissive"` but record the promoted schema as an artifact (it becomes part of correctness). ([Apache Arrow][5])

### Partition parsing silently missing

If `partition_base_dir` is wrong, partition discovery is skipped for files outside the prefix (files still included but without partition info). ([Apache Arrow][4])

### Memory blowups during scanning

* If batches overflow memory, reduce `batch_size`. ([Apache Arrow][13])
* Increasing `batch_readahead` / `fragment_readahead` increases RAM usage (but can improve IO utilization). ([Apache Arrow][13])
* For remote Parquet, `pre_buffer=True` improves throughput but increases memory usage. ([Apache Arrow][14])

### Ordering surprises on write

* Parallel writes can reorder rows unless `preserve_order=True`. ([Apache Arrow][16])
* If ordering is semantically meaningful, either:

  * sort upstream (kernel lane) or
  * pay the `preserve_order=True` cost.

### DataFusion + PyArrow dataset pushdown assumptions

There are real reports of missing support/pushdowns for filtered PyArrow datasets in DataFusion. ([GitHub][19])
If correctness/perf depends on pushdown, prefer **pre-scan in PyArrow → `from_arrow(reader)`**.

### Special fields are primarily for projections/provenance

Scanner allows special fields like `__filename` and `__fragment_index` in projection columns. ([Apache Arrow][13])
There are open questions/issues around filtering by these special fields; if you need fragment-based filtering, prefer `get_fragments(...)` or consume `scan_batches()` and filter by `TaggedRecordBatch.fragment`. ([Apache Arrow][10])

---

If you want the next step: the natural continuation is **Section 5 (Compilation boundary: Ibis IR → SQLGlot AST → DataFusion execution)**, where we standardize the “SQLGlot policy engine” and show how the ingestion tables produced above become deterministic, cacheable relational products with lineage + semantic diffs.

[1]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html?utm_source=chatgpt.com "pyarrow.dataset.Scanner — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html "pyarrow.fs.FileSystem — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/filesystems.html?utm_source=chatgpt.com "Filesystem Interface — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.dataset.html?utm_source=chatgpt.com "pyarrow.dataset. - Apache Arrow"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.FileSystemDatasetFactory.html?utm_source=chatgpt.com "pyarrow.dataset.FileSystemDatasetFactory - Apache Arrow"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.FileSystemFactoryOptions.html?utm_source=chatgpt.com "pyarrow.dataset.FileSystemFactoryOptions - Apache Arrow"
[7]: https://arrow.apache.org/docs/_modules/pyarrow/dataset.html?utm_source=chatgpt.com "Source code for pyarrow.dataset - Apache Arrow"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.unify_schemas.html?utm_source=chatgpt.com "pyarrow.unify_schemas — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Fragment.html "pyarrow.dataset.Fragment — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html "pyarrow.dataset.Dataset — Apache Arrow v22.0.0"
[11]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetFileFragment.html?utm_source=chatgpt.com "pyarrow.dataset.ParquetFileFragment — Apache Arrow v22.0.0"
[12]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Fragment.html?utm_source=chatgpt.com "pyarrow.dataset.Fragment — Apache Arrow v22.0.0"
[13]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html "pyarrow.dataset.Scanner — Apache Arrow v22.0.0"
[14]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetFragmentScanOptions.html?utm_source=chatgpt.com "pyarrow.dataset.ParquetFragmentScanOptions - Apache Arrow"
[15]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.TaggedRecordBatch.html?utm_source=chatgpt.com "pyarrow.dataset.TaggedRecordBatch — Apache Arrow v22.0.0"
[16]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html "pyarrow.dataset.write_dataset — Apache Arrow v22.0.0"
[17]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[18]: https://datafusion.apache.org/python/user-guide/data-sources.html?utm_source=chatgpt.com "Data Sources — Apache Arrow DataFusion documentation"
[19]: https://github.com/apache/datafusion/issues/10267?utm_source=chatgpt.com "Support for filtered arrow datasets · Issue #10267"
[20]: https://arrow.staged.apache.org/datafusion-python/generated/datafusion.SessionContext.html "datafusion.SessionContext — Apache Arrow DataFusion  documentation"
[21]: https://ibis-project.org/backends/datafusion "datafusion – Ibis"


## 5) Compilation boundary: Ibis IR → SQLGlot AST → DataFusion execution

This is where you turn “ingested tables” into **deterministic, cacheable relational products**—by standardizing:

* a **SQLGlot policy engine** (canonicalization + compatibility rewrites),
* **query identity** (fingerprints + semantic diffs),
* **lineage** (required-column extraction / audit),
* and **execution surfaces** (Ibis + DataFusion) that preserve your safety + streaming invariants.

---

### 5.0 The boundary contract

**Inputs**

* An Ibis expression (preferred) or SQL (escape hatch)
* A schema snapshot (contract truth)
* Dialect policy (read dialect vs write dialect)
* Runtime profile identifiers (affects plan choice, pushdowns, performance)

**Outputs**

* A canonical SQLGlot AST (the “compiler IR artifact”)
* A stable fingerprint + semantic diff signals
* Column lineage / required-column sets
* Executable plan:

  * Either: SQLGlot AST → Ibis DataFusion backend `raw_sql(sge.Expression)` ([Ibis][1])
  * Or: emitted SQL string → DataFusion `SessionContext.sql_with_options(...)` (gated) ([Apache DataFusion][2])

---

## 5.1 Ibis IR → SQLGlot AST checkpoint

### 5.1.1 Preferred: build rules as Ibis expressions (not SQL strings)

Ibis table expressions are lazy; **row order is undefined unless you call `.order_by()`**. That means your rule outputs must explicitly encode ordering when determinism matters. ([Ibis][3])

### 5.1.2 The SQLGlot checkpoint from Ibis

Ibis compiles to SQLGlot under the hood, and you can inspect that intermediate representation via the compiler (`con.compiler.to_sqlglot(expr)` / `compiler.to_sqlglot(expr)`), which is the correct hook for your “policy engine” lane. ([Ibis][4])

**Why this is the right checkpoint**

* It’s structured (AST), not string SQL.
* It’s the exact IR SQLGlot’s optimizer/lineage/diff tools operate on.
* It makes your build artifacts stable even when pretty-printing changes.

### 5.1.3 SQL escape hatches: what you should allow and what you should treat as opaque

Ibis does **not** understand the internal structure of SQL passed to `Table.sql()` / `Backend.sql()` in the same way as Ibis-native expressions, but it still compiles them into SQLGlot expressions under the hood—so SQLGlot tools like lineage can still apply. ([Ibis][4])

Practical policy:

* Allow raw SQL only in explicitly labeled “escape hatch” rulesets.
* Always require an explicit schema contract for SQL escape hatch outputs (so qualification/type inference is deterministic).

---

## 5.2 The SQLGlot policy engine: canonicalization + compatibility rewrites

You can implement your policy engine in two equivalent ways:

* **A. Use `sqlglot.optimizer.optimize(...)` with a pinned rule list**
* **B. Run individual passes explicitly in your chosen order**

I recommend A (optimize) as the baseline and B for “surgical overrides.”

### 5.2.1 Baseline: `sqlglot.optimizer.optimize` and its default `RULES`

SQLGlot publishes its default optimizer pipeline (`RULES`) as:

`qualify → pushdown_projections → normalize → unnest_subqueries → pushdown_predicates → optimize_joins → eliminate_subqueries → merge_subqueries → eliminate_joins → eliminate_ctes → quote_identifiers → annotate_types → canonicalize → simplify`. ([SQLGlot][5])

And it explicitly warns: **many rules require qualification**; do not remove `qualify` unless you know what you’re doing. ([SQLGlot][5])

### 5.2.2 Your “pinned policy engine” object (what you version + hash)

At minimum, the policy engine config must pin:

* `sqlglot_version`
* `read_dialect` (parsing + qualification semantics)
* `write_dialect` (generation / engine compatibility)
* `rules` list (or hash of list)
* `qualify kwargs` (expand stars, validate, quoting rules)
* `normalize thresholds` (CNF/DNF max_distance)
* generator settings (`pretty`, `identify`, `normalize`, strict unsupported policy)

The generator settings matter because dialects define `identify` semantics (including `'safe'`) and identifier normalization, and these affect emitted SQL stability. ([SQLGlot][6])

---

## 5.3 Pass-by-pass: what each stage buys you (and what knobs matter)

### 5.3.1 Qualification: make columns/tables resolvable and optimizer-safe

`sqlglot.optimizer.qualify.qualify(...)` rewrites the AST to have normalized and qualified tables/columns and is described as **necessary for all further SQLGlot optimizations**. ([SQLGlot][7])

Key knobs you should treat as policy:

* `validate_qualify_columns` (make ambiguity fatal)
* `quote_identifiers` / `identify` (quoting rules)
* `canonicalize_table_aliases` (stabilize aliasing)
* `sql=<original SQL string>` for error highlighting (also supported by `optimize`) ([SQLGlot][7])

### 5.3.2 Identifier normalization: stabilize cache keys while preserving semantics

`normalize_identifiers(...)` converts identifiers to lower/upper case while preserving semantics per dialect and plays “a very important role in standardization of the AST.” It also supports the `/* sqlglot.meta case_sensitive */` escape hatch. ([SQLGlot][8])

### 5.3.3 Predicate normalization (CNF/DNF): controlled, guarded

`sqlglot.optimizer.normalize.normalize(...)` rewrites connector expressions to CNF by default (or DNF) and accepts `max_distance` as the maximal estimated distance to attempt conversion. ([SQLGlot][9])

Use this only when distance is below your threshold; otherwise skip (the point is to avoid exponential blowups).

### 5.3.4 Pushdown projections: make scans cheaper

`pushdown_projections(expression, remove_unused_selections=True)` removes unused inner selections and has a concrete example showing an inner `SELECT` dropping unused columns. ([SQLGlot][10])

This is one of your highest ROI wins because it directly reduces Parquet column decoding in DataFusion.

### 5.3.5 Pushdown predicates: make pruning happen earlier

`pushdown_predicates(expression, dialect=None)` rewrites AST to push predicates into FROM/JOIN scopes; the example shows the outer WHERE becoming `WHERE TRUE` after pushdown. ([SQLGlot][11])

This stabilizes SQL shape (good for diffs) and typically improves engine pushdown.

### 5.3.6 Subquery unnesting and decorrelation: improve join-planner visibility

`unnest_subqueries(expression)` converts some predicates with subqueries into joins:

* scalar subqueries → cross joins
* correlated/vectorized subqueries → group-by so it’s not a many-to-many left join
  and provides an explicit example transformation. ([SQLGlot][12])

This is extremely relevant for “rulepacks authored by humans/agents” that naturally produce subqueries.

### 5.3.7 Type inference + canonicalization: make equivalences explicit

`annotate_types` annotates an AST with types; its API includes `coerces_to` and `overwrite_types` knobs. ([SQLGlot][13])

`canonicalize(expression, dialect=None)` converts SQL into a standard form and explicitly states it relies on `annotate_types` for type inference. ([SQLGlot][14])

### 5.3.8 Simplification: safe logical simplifications (with opt-in knobs)

`simplify(expression, constant_propagation=False, coalesce_simplification=False, dialect=None)` rewrites the AST to simplify expressions, with an explicit example `TRUE AND TRUE → TRUE`. ([SQLGlot][15])

Use the knobs deliberately:

* `constant_propagation` is useful for rulepacks that add repeated constraints
* `coalesce_simplification` can make analysis easier but can leave SQL more verbose ([SQLGlot][15])

---

## 5.4 Compatibility lane: SQLGlot transforms you apply only when needed

Some SQL shapes exist in upstream dialects but not in DataFusion (or not in the dialect you choose to generate). That’s what `sqlglot.transforms` is for.

A key example: `eliminate_qualify` (QUALIFY → subquery + WHERE) is the canonical portability transformation when a backend can’t handle QUALIFY but you want to keep “winner selection” authored in QUALIFY form. The transforms module also includes UNNEST/EXPLODE conversions such as `unnest_to_explode`. ([SQLGlot][16])

**Policy rule**

* Keep one **internal canonical form** for analysis (usually UNNEST-style).
* Apply **generate-time transforms** to match the engine’s accepted SQL.

---

## 5.5 Query identity: fingerprints, semantic diffs, lineage

This is what makes incremental rebuild possible.

### 5.5.1 AST serialization: `sqlglot.serde.dump/load`

SQLGlot provides:

* `dump(Expression)` → JSON-serializable list
* `load(payloads)` → Expression ([SQLGlot][17])

Use Serde for stable fingerprints that don’t depend on pretty-printing.

**Fingerprint recipe**

1. canonicalize AST via your policy engine
2. `payload = dump(expr)`
3. `sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")))`

### 5.5.2 Semantic diff: `sqlglot.diff`

SQLGlot’s diff algorithm is explicitly described as two high-level steps:

1. find appropriate matchings between AST nodes
2. identify the changes based on those matchings ([SQLGlot][18])

Use semantic diff **after canonicalization** so you’re not diffing noise (alias churn, star expansion differences, predicate placement).

### 5.5.3 Column lineage: `sqlglot.lineage.lineage`

`lineage(column, sql, schema=None, sources=None, dialect=None, scope=None, trim_selects=True, copy=True, **kwargs)` builds lineage for a specific output column. The docs explicitly call out `trim_selects` and that `**kwargs` are qualification kwargs. ([SQLGlot][19])

**Your production pattern**

* Build canonical AST once.
* Build scope once (optional but faster/stabler).
* For each output column: compute lineage with `trim_selects=True`.
* Aggregate required base columns (table, column) across outputs.
* Feed required columns back into your **PyArrow scan projection** policy (Section 4), so you decode only what the rule needs.

---

## 5.6 Execution: run the canonical plan in DataFusion (two lanes)

### 5.6.1 Lane A (preferred): execute SQLGlot AST directly via Ibis DataFusion backend

Ibis DataFusion backend `raw_sql(self, query)` explicitly accepts `str | sge.Expression`. ([Ibis][1])

This is the cleanest end-to-end flow:

* Ibis IR → SQLGlot AST
* SQLGlot policy engine rewrites AST
* Execute AST without string SQL fragility

### 5.6.2 Lane B (gated): emit SQL string and execute via DataFusion `sql_with_options`

DataFusion Python’s `SessionContext.sql_with_options` validates the query against `SQLOptions`, and the `sql` API documents:

* `param_values`: scalar substitution after parsing
* `named_params`: string/DataFrame substitution in the query string ([Apache DataFusion][2])

Use this lane when:

* you want `SET`/DDL/DML surfaces with strict gating
* you want full control over what “SQL statements” are allowed

---

## 5.7 Minimal end-to-end example (ingested table → deterministic product)

This is the “shape” you standardize in your repo (pseudocode-ish, but directly implementable):

```python
import json, hashlib
import sqlglot
from sqlglot.optimizer import optimize
from sqlglot.serde import dump
from sqlglot.lineage import lineage
import pyarrow as pa
import pyarrow.dataset as ds
from datafusion import SessionContext, SQLOptions

def build_product(con, expr, *, schema_mapping, read_dialect, write_dialect, out_dir):
    # 1) Ibis → SQLGlot AST checkpoint (documented pattern)
    # ast = con.compiler.to_sqlglot(expr)  # from Ibis SQLGlot boundary

    # For illustration, assume we already have a SQLGlot AST named `ast`.

    # 2) SQLGlot policy engine: canonicalize + pushdowns + unnesting
    canon = optimize(ast, schema=schema_mapping, dialect=read_dialect, sql=expr.to_sql())

    # 3) Fingerprint (Serde)
    payload = dump(canon)
    fp = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    # 4) Lineage: required columns per output column
    # (You’d iterate over the SELECT list of canon to get output column names.)
    deps = lineage("some_output_col", canon, schema=schema_mapping, dialect=read_dialect, trim_selects=True)

    # 5) Execute (Lane A): Ibis DataFusion raw_sql accepts sge.Expression
    # result = con.raw_sql(canon)  # returns cursor-like; backend-specific
    # Or Lane B: DataFusion gated SQL execution
    ctx = SessionContext()
    opts = SQLOptions().with_allow_ddl(False).with_allow_dml(False).with_allow_statements(False)
    df = ctx.sql_with_options(canon.sql(dialect=write_dialect, pretty=False, identify=False), opts)

    # 6) Stream to Parquet dataset
    reader = pa.RecordBatchReader.from_stream(df)
    ds.write_dataset(reader, base_dir=out_dir, format="parquet")

    return fp
```

What matters structurally:

* canonical AST is the artifact
* Serde fingerprint is the cache key component
* lineage output drives scan projection
* execution is either AST-native via Ibis or string SQL via gated DataFusion

(Each building block above is anchored in published APIs: optimize rules list, qualify requirement, serde dump/load, lineage API, Ibis raw_sql accepts SQLGlot expressions, and DataFusion sql_with_options and parameter semantics.) ([SQLGlot][5])

---

## 5.8 Failure modes and hardening checklist

1. **Schema missing or incomplete → qualification errors**
   Qualify is required for many optimizations; make schema provisioning a build-time contract. ([SQLGlot][7])

2. **Identifier case differences → unstable cache keys**
   Normalize identifiers per dialect; use the case-sensitive escape hatch only when needed. ([SQLGlot][8])

3. **CNF/DNF explosion**
   Always cap normalization via `max_distance`. ([SQLGlot][9])

4. **Opaque SQL segments in Ibis**
   `Table.sql` / `Backend.sql` are less transparent, so gate them and require explicit schemas. ([Ibis][4])

5. **Execution surface safety**
   Use `sql_with_options` for any untrusted SQL path; default SQLOptions are permissive. ([Apache DataFusion][2])

---

If you want the next section: **Section 6 (Pushdowns and performance coherence across the stack)** is where we translate lineage + canonical SQL shapes into concrete scan policies (PyArrow projection/filter pushdown) and DataFusion runtime profile defaults (dynamic filters, repartitioning, spill, metadata caches) so the whole pipeline is fast *and* reproducible.

[1]: https://ibis-project.org/backends/datafusion?utm_source=chatgpt.com "DataFusion - Ibis"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html?utm_source=chatgpt.com "datafusion.context — Apache Arrow DataFusion documentation"
[3]: https://ibis-project.org/reference/expression-tables?utm_source=chatgpt.com "Table expressions - Ibis"
[4]: https://ibis-project.org/posts/does-ibis-understand-sql/?utm_source=chatgpt.com "Does Ibis understand SQL?"
[5]: https://sqlglot.com/sqlglot/optimizer/optimizer.html "sqlglot.optimizer.optimizer API documentation"
[6]: https://sqlglot.com/sqlglot/dialects/presto.html?utm_source=chatgpt.com "sqlglot.dialects.presto API documentation"
[7]: https://sqlglot.com/sqlglot/optimizer/qualify.html?utm_source=chatgpt.com "sqlglot.optimizer.qualify API documentation"
[8]: https://sqlglot.com/sqlglot/optimizer/normalize_identifiers.html?utm_source=chatgpt.com "sqlglot.optimizer.normalize_identifiers API documentation"
[9]: https://sqlglot.com/sqlglot/optimizer/normalize.html?utm_source=chatgpt.com "sqlglot.optimizer.normalize API documentation"
[10]: https://sqlglot.com/sqlglot/optimizer/pushdown_projections.html?utm_source=chatgpt.com "sqlglot.optimizer.pushdown_projections API documentation"
[11]: https://sqlglot.com/sqlglot/optimizer/pushdown_predicates.html?utm_source=chatgpt.com "sqlglot.optimizer.pushdown_predicates API documentation"
[12]: https://sqlglot.com/sqlglot/optimizer/unnest_subqueries.html "sqlglot.optimizer.unnest_subqueries API documentation"
[13]: https://sqlglot.com/sqlglot/optimizer/annotate_types.html?utm_source=chatgpt.com "sqlglot.optimizer.annotate_types API documentation"
[14]: https://sqlglot.com/sqlglot/optimizer/canonicalize.html?utm_source=chatgpt.com "sqlglot.optimizer.canonicalize API documentation"
[15]: https://sqlglot.com/sqlglot/optimizer/simplify.html "sqlglot.optimizer.simplify API documentation"
[16]: https://sqlglot.com/sqlglot/transforms.html?utm_source=chatgpt.com "sqlglot.transforms API documentation"
[17]: https://sqlglot.com/sqlglot/serde.html?utm_source=chatgpt.com "sqlglot.serde API documentation"
[18]: https://sqlglot.com/sqlglot/diff.html?utm_source=chatgpt.com "Semantic Diff for SQL"
[19]: https://sqlglot.com/sqlglot/lineage.html?utm_source=chatgpt.com "sqlglot.lineage API documentation"


## 6) Pushdowns and performance coherence across the stack

This section standardizes a **single “pushdown story”** across:

* **SQLGlot** (compile-time shape → makes engine pushdowns possible),
* **DataFusion** (planner + Parquet reader pushdowns + dynamic filters + caches),
* **PyArrow Dataset** (partition + Parquet-stats pushdown + scan policy),
* **Ibis** (IR knobs that change SQL shape).

The goal is: **fast, reproducible scans** with **explicit, versioned knobs**, where “what got pushed down” is visible in artifacts.

---

### 6.0 Pushdown layers: where “doing less work” can happen

#### Layer 1 — Compile-time SQL shape (SQLGlot)

You rewrite SQL into a form that *makes downstream pushdowns easy*:

* `pushdown_predicates` pushes filters into inner scopes and can rewrite the outer WHERE to `WHERE TRUE` (stabilizes structure and enables earlier pruning). ([SQLGlot][1])
* `pushdown_projections` trims unused inner projections (reduces downstream column decoding). ([SQLGlot][2])
* Lineage tooling (`lineage(..., trim_selects=True)`) gives you a principled “required input columns” set. ([SQLGlot][3])
* `ibis.options.sql.fuse_selects` changes whether consecutive selects are fused, which changes how easily pushdowns apply (and changes planner behavior). ([Ibis][4])

#### Layer 2 — Scan-time pushdown (PyArrow Dataset)

PyArrow can push filtering into:

* **partition pruning** and **Parquet statistics** (row group pruning), otherwise it filters loaded batches. ([Apache Arrow][5])

It also exposes scan policy knobs (batch sizing, readahead, metadata caching) via scanner/fragments. ([Apache Arrow][6])

#### Layer 3 — Engine-time pushdown and runtime pruning (DataFusion)

DataFusion adds pushdowns beyond static SQL:

* **Dynamic filters**: push runtime-derived filters from operators into file scans. Config keys allow enabling/disabling TopK/Join/Aggregate dynamic filter pushdown independently. ([Apache DataFusion][7])
* **Parquet reader pushdowns**: page index reads, row group pruning, and “late materialization” (`parquet.pushdown_filters`) where filters are applied during parquet decoding. ([Apache DataFusion][8])
* **Metadata caches** (list-files cache and Parquet metadata cache) to avoid repeated remote IO. ([Apache DataFusion][9])

---

## 6.1 SQLGlot → scan policy: making “required columns” and “pushdown-safe predicates” explicit

### 6.1.1 Canonicalization order that maximizes pushdown

A minimum “pushdown-friendly” pipeline:

1. **Qualify + expand stars** (so scopes/columns are explicit)
2. **Pushdown predicates** (move filters inward) ([SQLGlot][1])
3. **Pushdown projections** (trim unused inner columns) ([SQLGlot][2])
4. Optional: simplify/canonicalize for stable identity

This ensures that:

* Parquet filters and projections can be recognized by DataFusion and/or PyArrow
* your lineage analysis runs on an already “pruning-friendly” AST

### 6.1.2 Required-column extraction (lineage-first)

Use SQLGlot lineage as the source of truth:

* `sqlglot.lineage.lineage(..., trim_selects=True)` is explicitly designed to “clean up selects by trimming to only relevant columns.” ([SQLGlot][3])

Practical pattern:

* For each output column in your SELECT list:

  * compute lineage node
  * walk upstream to collect base `(table, column)` deps
* Union those deps → **RequiredColumnSet**

### 6.1.3 ScanSpec: columns must include both “projection fields” and “filter-only fields”

Even if a column isn’t in the final projection, it may still be required to evaluate filters. Arrow’s dataset scanner implementation explicitly treats “fields referenced in projection and filter expression” as requiring materialization. ([Gemfury][10])

So your ScanSpec should be:

* `scan_columns = required_projection_cols ∪ required_filter_cols`

This is why we treat filter rewriting (pushdown_predicates) and lineage together: you can *see* which predicates still require which columns.

### 6.1.4 “Pushdown-safe predicate subset” (for PyArrow pre-scan mode)

If you pre-scan with PyArrow (instead of letting DataFusion scan Parquet directly), you’ll only translate a conservative subset of predicates into `pyarrow.dataset.Expression`:

* comparisons (`=, <, <=, >, >=`), `IN`, `IS NULL`, boolean `AND/OR/NOT`
* avoid functions unless you know Dataset/Parquet can push them

PyArrow explicitly pushes down predicates where possible using partition info or Parquet statistics, and otherwise filters batches after load. ([Apache Arrow][5])

---

## 6.2 PyArrow Dataset scan policy: projection + partition/stat pushdown + remote IO tuning

### 6.2.1 Default scan interface (fragment or dataset)

Fragments can be scanned with:

* `Fragment.scanner(schema=..., columns=..., filter=..., batch_size=..., batch_readahead=..., fragment_readahead=..., fragment_scan_options=..., use_threads=..., cache_metadata=...)` ([Apache Arrow][6])

This is your standard “ScanSpec → Scanner” compilation.

### 6.2.2 Filter pushdown semantics (what you can count on)

For Parquet fragments:

* “If possible the predicate will be pushed down to exploit the partition information or internal metadata found in the data source, e.g. Parquet statistics. Otherwise filters the loaded RecordBatches before yielding them.” ([Apache Arrow][5])
* `batch_size` is explicitly documented as a lever to reduce memory if batches overflow. ([Apache Arrow][5])

### 6.2.3 Remote object store tuning: `ParquetFragmentScanOptions.pre_buffer`

For S3/GCS-style latency, `pre_buffer=True` is the high-leverage knob:

* “pre-buffer the raw Parquet data instead of issuing one read per column chunk,” improving high-latency FS performance by coalescing reads and issuing them in parallel via a background IO thread pool; trade memory for speed. ([Apache Arrow][11])

Recommended policy:

* `pre_buffer=True` for remote Parquet scans (unless memory constrained)
* Use `pyarrow.set_io_thread_count(...)` to provision IO parallelism (Section 0/2 invariant)

### 6.2.4 Schema fidelity and dictionary read policy: `ParquetReadOptions`

`ParquetReadOptions` includes knobs like `dictionary_columns` (read selected columns as dictionary) and type interpretation overrides (ignored if Arrow schema metadata is present). ([Apache Arrow][12])

This is relevant for:

* join-heavy workloads where dictionary encoding can reduce memory and speed comparisons
* consistent type interpretation across mixed Parquet writers

---

## 6.3 DataFusion runtime profile defaults: “fast, reproducible, debuggable”

You want defaults that:

* maximize pruning (Parquet pushdowns + dynamic filters)
* avoid pathological oversubscription
* keep reproducibility by capturing all keys

### 6.3.1 Dynamic filters (TopK / Join / Aggregate): enable, but make them togglable

DataFusion exposes the dynamic filter config family:

* `datafusion.optimizer.enable_topk_dynamic_filter_pushdown`
* `datafusion.optimizer.enable_join_dynamic_filter_pushdown`
* `datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown`
* `datafusion.optimizer.enable_dynamic_filter_pushdown` (umbrella) ([Apache DataFusion][7])

Why they matter:

* DataFusion 49 highlights TopK dynamic filters for `ORDER BY ... LIMIT ...` patterns. ([Apache DataFusion][13])
* DataFusion 50 extends dynamic filter pushdown to **inner hash joins**, reducing scanned data when one relation is small/selective. ([Apache DataFusion][14])
* The “Dynamic Filters” blog explains the general mechanism and performance wins. ([Apache DataFusion][15])

**Safety posture:** keep a “kill switch.” There are known correctness issues reported for join dynamic filter pushdown with certain predicates; disabling `enable_dynamic_filter_pushdown` was reported to restore correct results in at least one case. ([GitHub][16])
And DF50 release tracking includes regressions/known issues around optimizer behavior. ([GitHub][17])

### 6.3.2 Parquet reader pushdown controls (DataFusion config keys you should version)

From the DataFusion config catalog:

**Row group / page-level pruning**

* `datafusion.execution.parquet.enable_page_index` (read page index metadata to reduce IO/decoded rows) ([Apache DataFusion][8])
* `datafusion.execution.parquet.pruning` (skip row groups using min/max stats) ([Apache DataFusion][8])

**Metadata read minimization**

* `datafusion.execution.parquet.metadata_size_hint` (optimistically fetch tail bytes to reduce IO round trips) ([Apache DataFusion][8])
* `datafusion.execution.parquet.skip_metadata` (avoid schema conflicts across files due to differing embedded metadata) ([Apache DataFusion][8])

**Late materialization (“filter during decode”)**

* `datafusion.execution.parquet.pushdown_filters` (apply filters during parquet decoding; “late materialization”) ([Apache DataFusion][8])
* `datafusion.execution.parquet.reorder_filters` and `force_filter_selections` tune how pushed-down filters are evaluated. ([Apache DataFusion][8])
* `datafusion.execution.parquet.max_predicate_cache_size` bounds memory used to cache predicate results when `pushdown_filters` is enabled. ([Apache DataFusion][8])

DataFusion has a dedicated post explaining how Parquet filter pushdown works (reading filter columns first and then selectively reading other columns), which is the conceptual reason `pushdown_filters` can be valuable. ([Apache DataFusion][18])

### 6.3.3 Metadata caches: list-files cache + Parquet metadata cache

From the runtime config guide (configs.md):

* `datafusion.runtime.list_files_cache_limit` and optional TTL reduce repeated directory listing overhead (important for partitioned datasets / object stores). ([Apache DataFusion][9])
* `datafusion.runtime.metadata_cache_limit` bounds Parquet metadata cache (ties directly to DF50 “Parquet Metadata Cache”). ([Apache DataFusion][9])

### 6.3.4 Partitioning and memory-limited tuning: `target_partitions` + `batch_size`

The DataFusion tuning guide (configs.md) gives explicit advice:

* For very small datasets, set `target_partitions = 1` to avoid repartition overhead. ([Apache DataFusion][9])
* For tight memory limits, set both `target_partitions` and `batch_size` smaller; higher partitions means less memory per partition under fair spill pool, which can trigger more spilling. ([Apache DataFusion][9])

### 6.3.5 Spill behavior and spill compression

* `datafusion.execution.spill_compression` is a config key (introduced in DF49 upgrade guidance) with valid values `uncompressed`, `lz4_frame`, `zstd`. ([Apache DataFusion][19])
* DF50 improves larger-than-memory sorts via multi-level merge sorts (meaning spill-enabled sorts are much more robust). ([Apache DataFusion][14])

### 6.3.6 Repartition toggles (turn on by default, but make profile-tiered)

The DataFusion Python API exposes `SessionConfig` helpers for repartitioning:

* file scans, joins, aggregations, sorts, windows ([Apache DataFusion][20])

This is your main “parallelism vs determinism” dial:

* enable for throughput
* disable when:

  * plan overhead dominates (tiny datasets)
  * you need simpler, more deterministic execution for debugging

### 6.3.7 Join algorithm selection knobs (important for stable-id join heavy workloads)

DataFusion supports multiple join algorithms and exposes optimizer configs:

* `datafusion.optimizer.prefer_hash_join`
* `datafusion.optimizer.enable_piecewise_merge_join`
* `datafusion.optimizer.allow_symmetric_joins_without_pruning` ([Apache DataFusion][9])

These should be profile fields because they affect performance and sometimes memory behavior.

---

## 6.4 Coherence patterns: two “pushdown execution modes”

### Mode A: **Engine-first scanning** (DataFusion scans Parquet itself)

Use when:

* you want DataFusion’s dynamic filters and Parquet reader pushdowns to do most of the pruning,
* you’re confident in DataFusion’s scan behavior for your datasets.

How you get coherence:

* SQLGlot policy engine emits pushdown-friendly SQL (predicates/projections)
* DataFusion config enables Parquet pruning + (optionally) late materialization + dynamic filters

What you capture:

* optimized SQLGlot AST + emitted SQL
* DataFusion config snapshot
* EXPLAIN / EXPLAIN ANALYZE artifacts

### Mode B: **Arrow-first scanning** (PyArrow Dataset scanner → stream into DF/Ibis)

Use when:

* you need deterministic scan behavior (especially on remote object stores),
* you want to control `pre_buffer`, readahead, and schema unification explicitly,
* you want fragment-level provenance.

How you get coherence:

* SQLGlot lineage → required columns
* Optional predicate translation → PyArrow Dataset filter expression subset
* Scanner emits a reader → you import to DF via Arrow stream and continue in DataFusion/Ibis

Why it’s coherent:

* PyArrow pushes predicates down using partition info / Parquet stats when possible, and otherwise filters batches after load (documented). ([Apache Arrow][5])
* You still use DataFusion for joins/window/aggregation, but you’ve already minimized scan IO.

---

## 6.5 Recommended “default profiles” (make these real runtime profile presets)

### 6.5.1 `prod_fast` (default)

* SQLGlot: pushdown_predicates + pushdown_projections enabled; canonicalize + simplify
* Ibis: `ibis.options.sql.fuse_selects=True` (fewer subqueries, often better pushdowns) ([Ibis][4])
* DataFusion:

  * enable Parquet page index + pruning, metadata_size_hint default, bloom_filter_on_read default ([Apache DataFusion][8])
  * dynamic filters enabled (TopK + join + aggregate), but record all toggles ([Apache DataFusion][7])
  * metadata caches enabled with explicit limits (list_files + metadata) ([Apache DataFusion][9])
* PyArrow (only if Mode B): `pre_buffer=True` on remote FS ([Apache Arrow][11])

### 6.5.2 `prod_safe` (when correctness or regressions matter)

* dynamic filters: disable join dynamic filter pushdown first, then full dynamic filters if necessary ([Apache DataFusion][7])
* keep Parquet pruning/page index on
* keep SQLGlot canonicalization and lineage (identity should not depend on dynamic filters)

### 6.5.3 `memory_limited`

* lower `target_partitions` and `batch_size` as recommended by DataFusion tuning guide ([Apache DataFusion][9])
* enable spill + consider spill compression (`lz4_frame` or `zstd`) ([Apache DataFusion][19])
* avoid `pre_buffer=True` if memory is tight (PyArrow)

---

## 6.6 What you must emit as artifacts to keep “fast” reproducible

For every product build, emit:

1. **SQLGlot canonical AST** (serde dump) + emitted SQL (engine dialect)
2. **Pushdown evidence**

   * required columns set (from lineage) ([SQLGlot][3])
   * whether pushdown_predicates/pushdown_projections ran (and pass order) ([SQLGlot][1])
3. **DataFusion runtime snapshot**

   * all config keys you set (including dynamic filter toggles, Parquet pushdown settings, caches, spill settings) ([Apache DataFusion][7])
4. **PyArrow scan snapshot** (Mode B)

   * scanner knobs (batch size, readahead, cache_metadata)
   * ParquetFragmentScanOptions (pre_buffer, etc.) ([Apache Arrow][11])

This is what prevents “it got slower” from being un-debuggable: you can see whether the pushdowns happened, whether caches were on, and whether the scan policy matched what lineage required.

---

If you want the next deep dive: **Section 7 (Streaming materialization)** is where we unify *all* producer surfaces (DF/Ibis/PyArrow scanner) into a single “stream → writer” pipeline, and standardize deterministic dataset layout (partitioning, basenames, existing_data_behavior, metadata sidecars) with a minimal “writer contract” and test harness.

[1]: https://sqlglot.com/sqlglot/optimizer/pushdown_predicates.html?utm_source=chatgpt.com "sqlglot.optimizer.pushdown_predicates API documentation"
[2]: https://sqlglot.com/sqlglot/optimizer/pushdown_projections.html?utm_source=chatgpt.com "sqlglot.optimizer.pushdown_projections API documentation"
[3]: https://sqlglot.com/sqlglot/lineage.html?utm_source=chatgpt.com "sqlglot.lineage API documentation"
[4]: https://ibis-project.org/reference/sql?utm_source=chatgpt.com "sql - Ibis"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetFileFragment.html?utm_source=chatgpt.com "pyarrow.dataset.ParquetFileFragment — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Fragment.html?utm_source=chatgpt.com "pyarrow.dataset.Fragment — Apache Arrow v22.0.0"
[7]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[8]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[9]: https://datafusion.apache.org/_sources/user-guide/configs.md.txt "datafusion.apache.org"
[10]: https://gemfury.com/arrow-nightlies/python%3Apyarrow/pyarrow-22.0.0.dev64-cp311-cp311-macosx_12_0_x86_64.whl/content/include/arrow/dataset/scanner.h?utm_source=chatgpt.com "include/arrow/dataset/scanner.h"
[11]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetFragmentScanOptions.html?utm_source=chatgpt.com "pyarrow.dataset.ParquetFragmentScanOptions - Apache Arrow"
[12]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetReadOptions.html?utm_source=chatgpt.com "pyarrow.dataset.ParquetReadOptions — Apache Arrow v22.0.0"
[13]: https://datafusion.apache.org/blog/2025/07/28/datafusion-49.0.0/?utm_source=chatgpt.com "Apache DataFusion 49.0.0 Released"
[14]: https://datafusion.apache.org/blog/2025/09/29/datafusion-50.0.0/?utm_source=chatgpt.com "Apache DataFusion 50.0.0 Released"
[15]: https://datafusion.apache.org/blog/2025/09/10/dynamic-filters/?utm_source=chatgpt.com "Dynamic Filters: Passing Information Between Operators ..."
[16]: https://github.com/apache/datafusion/issues/17188?utm_source=chatgpt.com "Dynamic Filter Pushdown causes JOIN to return incorrect ..."
[17]: https://github.com/apache/datafusion/issues/16799?utm_source=chatgpt.com "Release DataFusion 50.0.0 (Aug/Sep 2025) #16799"
[18]: https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/?utm_source=chatgpt.com "Efficient Filter Pushdown in Parquet - Apache DataFusion Blog"
[19]: https://datafusion.apache.org/library-user-guide/upgrading.html?utm_source=chatgpt.com "Upgrade Guides — Apache DataFusion documentation"
[20]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html?utm_source=chatgpt.com "datafusion.context — Apache Arrow DataFusion documentation"

## 7) Streaming materialization: unified “stream → writer” pipeline + deterministic dataset layout

This section standardizes **one materialization primitive** for your whole stack:

> **Any producer → `RecordBatchReader` → `pyarrow.dataset.write_dataset` (+ optional sidecars)**

That gives you: streaming-first execution, controlled ordering, reproducible file layouts, and a single place to enforce “best-in-class” write policies.

---

# 7.1 Producer surfaces → one streaming type

### 7.1.1 Canonical stream type: `pyarrow.RecordBatchReader`

Everything should normalize into a `RecordBatchReader`, because it is:

* directly producible from **Arrow PyCapsule stream producers** via `RecordBatchReader.from_stream(...)` (requires `__arrow_c_stream__`) ([Apache Arrow][1])
* directly consumable by `pyarrow.dataset.write_dataset(...)` ([Apache Arrow][2])

### 7.1.2 DataFusion producers

**Preferred conversion** (documented explicitly in DataFusion docs):

* `pa.RecordBatchReader.from_stream(df)` ([Apache DataFusion][3])
  If you need partition boundaries:
* `df.execute_stream_partitioned()` yields one stream per partition ([Apache DataFusion][3])

DataFusion’s Arrow IO docs also emphasize the semantics: invoking `__arrow_c_stream__` triggers execution, but batches are yielded incrementally rather than fully materialized. ([Apache DataFusion][4])

### 7.1.3 Ibis producers

Ibis provides two key streaming surfaces:

* `to_pyarrow_batches(..., chunk_size=...)` returns a `RecordBatchReader` and is eager (executes immediately). ([Ibis][5])
* `to_parquet_dir(...)` writes to a directory and explicitly forwards kwargs to `pyarrow.dataset.write_dataset`. ([Ibis][5])

> In your system: **prefer** `to_pyarrow_batches → write_dataset` when you want consistent manifest/sidecar behavior, and use `to_parquet_dir` as the “Ibis-managed sink” when you don’t need additional writer instrumentation beyond what Arrow already offers. ([Ibis][5])

### 7.1.4 PyArrow producers (Dataset/Scanner)

* `Scanner.to_reader()` is the standard “scan plan → streaming batches” surface.
* If you start from a one-shot stream and want scanner-like controls, use `Scanner.from_batches(...)`, which explicitly accepts:

  * a `RecordBatchReader`,
  * any object implementing the Arrow PyCapsule stream protocol,
  * or an iterator of `RecordBatch` (schema required). ([Apache Arrow][6])

This is the bridge that lets you apply “scan-like policy” (projection/filter/batching) even when the upstream source is a streaming engine output.

---

# 7.2 The unified adapter: “anything → RecordBatchReader”

You want one internal helper that collapses all producer shapes:

```python
import pyarrow as pa
import pyarrow.dataset as ds

def to_reader(obj, *, schema: pa.Schema | None = None) -> pa.RecordBatchReader:
    # (1) already a reader
    if isinstance(obj, pa.RecordBatchReader):
        return obj

    # (2) Arrow PyCapsule stream producer (DataFusion DF, etc.)
    if hasattr(obj, "__arrow_c_stream__"):
        return pa.RecordBatchReader.from_stream(obj, schema=schema)

    # (3) Ibis table expression
    if hasattr(obj, "to_pyarrow_batches"):
        return obj.to_pyarrow_batches()

    # (4) PyArrow scanner
    if isinstance(obj, ds.Scanner):
        return obj.to_reader()

    raise TypeError(f"Unsupported producer: {type(obj)}")
```

Key correctness constraints:

* `RecordBatchReader.from_stream` requires an object implementing `__arrow_c_stream__`. ([Apache Arrow][1])
* Treat streams as **one-shot** (this is explicit in Arrow’s scanner-from-batches contract, and it matches PyCapsule single-consumption semantics). ([Apache Arrow][6])

---

# 7.3 The writer contract: one function, versioned knobs

## 7.3.1 Why `pyarrow.dataset.write_dataset` is the sink primitive

`write_dataset(...)` is explicitly designed to write:

* datasets,
* in-memory tables/batches,
* **RecordBatchReaders**,
* and iterables of RecordBatches (schema required). ([Apache Arrow][2])

It is also where you get the core “layout policy” knobs in one place:

* partitioning
* basename template
* thread parallelism vs ordering
* file size and row group boundaries
* existing-data overwrite/append policies
* file visitor for manifest/metadata collection ([Apache Arrow][2])

## 7.3.2 The minimal `WriteSpec` you should define

Think of your writer config as a *stable ABI* for data products:

### Partitioning and directory shape

* `partitioning`: a `Partitioning` object **or** a list of field names (optionally with `partitioning_flavor`) ([Apache Arrow][2])
* Use Hive-style (`key=value`) if you want common interoperability; Arrow’s dataset module describes HivePartitioning semantics (field order ignored, missing/unrecognized fields ignored). ([Apache Arrow][7])

### File naming (append safety)

* `basename_template`: token `{i}` is replaced by an incrementing integer; defaults to `part-{i}.<ext>`. ([Apache Arrow][2])
  Best practice:
* include *run id* and *product id* in the template so concurrent runs never collide:

  * `basename_template=f"run={run_id}_product={product}_part-{{i}}"`

### Existing data policy

`existing_data_behavior` is the canonical “overwrite vs append vs partition overwrite” switch: ([Apache Arrow][2])

* `'error'`: fail if any data exists
* `'overwrite_or_ignore'`: overwrite output files with matching names, ignore other existing files; **with unique basenames this enables append-like workflows** ([Apache Arrow][2])
* `'delete_matching'`: for partitioned datasets, delete each partition directory the first time it’s encountered (partition overwrite). ([Apache Arrow][2])

### Ordering policy

* `use_threads=True` maximizes parallelism but **may change row order** if `preserve_order=False`. ([Apache Arrow][2])
* `preserve_order=True` guarantees row order even with threads, but may significantly degrade performance. ([Apache Arrow][2])

Your deterministic policy should therefore be:

* Default `preserve_order=False`
* If ordering matters:

  * **Tier 3**: sort upstream into canonical order (preferred), or
  * **Tier 2**: set `preserve_order=True` (expensive but simple) ([Apache Arrow][2])

### File sizing and row group boundaries

The writer exposes “layout shaping” knobs that you should treat as *profile fields*: ([Apache Arrow][2])

* `max_open_files`: too low fragments output into many small files
* `max_rows_per_file`: caps rows per file
* `min_rows_per_group` / `max_rows_per_group`: writer batches rows into row groups; docs explicitly recommend setting both to avoid very small row groups ([Apache Arrow][2])
* `max_partitions`: cap partitions any batch may be written into (guardrail)

### Format-specific write options

`file_options` are “FileFormat specific write options, created using `FileFormat.make_write_options()`.” ([Apache Arrow][2])
This is where you put Parquet-level policies like compression, dictionary encoding, statistics, etc.

### Directory creation

* `create_dir`: defaults to True; set False for filesystems that don’t require directories. ([Apache Arrow][2])

---

# 7.4 One “stream → write_dataset” implementation (the core pipeline)

```python
import pyarrow.dataset as ds

def materialize_dataset(
    producer,
    *,
    out_dir: str,
    partition_cols: list[str] | None = None,
    hive: bool = True,
    basename_template: str | None = None,
    existing_data_behavior: str = "error",
    use_threads: bool = True,
    preserve_order: bool = False,
    max_rows_per_file: int | None = None,
    min_rows_per_group: int | None = None,
    max_rows_per_group: int | None = None,
    file_visitor=None,
):
    reader = to_reader(producer)

    partitioning = None
    if partition_cols:
        partitioning = partition_cols
        partitioning_flavor = "hive" if hive else None
    else:
        partitioning_flavor = None

    ds.write_dataset(
        reader,
        base_dir=out_dir,
        format="parquet",
        basename_template=basename_template,
        partitioning=partitioning,
        partitioning_flavor=partitioning_flavor,
        use_threads=use_threads,
        preserve_order=preserve_order,
        max_rows_per_file=max_rows_per_file or 0,
        min_rows_per_group=min_rows_per_group or 0,
        max_rows_per_group=max_rows_per_group or 0,
        file_visitor=file_visitor,
        existing_data_behavior=existing_data_behavior,
        create_dir=True,
    )
```

Everything in this function maps to first-class write_dataset parameters. ([Apache Arrow][2])

---

# 7.5 Deterministic dataset layout spec (what you commit to)

### 7.5.1 Directory layout

Define a stable path convention like:

```
<root>/<product_family>/<product_name>/v=<contract_version>/run=<run_id>/
```

Partition directories (Hive flavor):

```
.../kind=CALL/...
.../kind=IMPORT/...
```

Hive-style partition directory semantics are described in the dataset module (key=value, field order ignored). ([Apache Arrow][7])

### 7.5.2 File naming

Use a basename template that embeds:

* run id
* product id
* optional partition “shard” tags

Because `overwrite_or_ignore` only becomes “append-like” when basenames are unique across writes. ([Apache Arrow][2])

### 7.5.3 Existing-data behavior

Pick exactly one per product type:

* “fresh build” products: `existing_data_behavior="error"` (fail fast if path exists)
* “partition refresh” products: `existing_data_behavior="delete_matching"` (replace touched partitions)
* “append event log” products: `existing_data_behavior="overwrite_or_ignore"` with unique basenames ([Apache Arrow][2])

---

# 7.6 Metadata sidecars and manifests

## 7.6.1 Per-file manifest (always)

Use `file_visitor` to collect:

* written file paths
* per-file Parquet metadata objects

Arrow docs explicitly state that `WrittenFile.metadata` is the Parquet metadata, has the file path attribute set, and “can be used to build a _metadata file.” ([Apache Arrow][2])

## 7.6.2 `_common_metadata` and `_metadata` (optional, must be bounded)

Two mechanisms:

### A) Schema-only metadata file

`pyarrow.parquet.write_metadata(schema, where, ...)` writes a metadata-only Parquet file from schema and explicitly says it can be used to generate `_common_metadata` and `_metadata` sidecars. ([Apache Arrow][8])

### B) Full `_metadata` with row groups

If you want full row-group metadata:

* merge `FileMetaData` objects:

  * `append_row_groups(other)`
  * `set_file_path(path)` to set ColumnChunk file paths
  * `write_metadata_file(where)` to write metadata-only Parquet file ([Apache Arrow][9])

Practical pattern:

1. collect per-file `FileMetaData` via `file_visitor`
2. choose one as accumulator, `append_row_groups` all others
3. normalize paths via `set_file_path` (relative paths are often preferable)
4. `write_metadata_file(<out_dir>/_metadata)`

### Scalability warning (don’t blindly build `_metadata`)

Parquet metadata size grows with **row groups × columns** (because there’s metadata for each column chunk in each row group). Independent analyses and real-world systems have run into memory pressure when accumulating metadata for very wide and/or highly partitioned datasets. ([InfluxData][10])

**Best practice for your system**

* Always produce a lightweight manifest (paths + basic stats).
* Only produce full `_metadata` for bounded datasets or when an engine explicitly benefits from it.

---

# 7.7 Producer-specific recipes

## 7.7.1 DataFusion → streaming writer

DataFusion docs explicitly recommend:

* `pa.RecordBatchReader.from_stream(df)`
* `execute_stream_partitioned()` when partition boundaries matter ([Apache DataFusion][3])

So your standard flow is:

```python
import pyarrow as pa

reader = pa.RecordBatchReader.from_stream(df)  # df is DataFusion DataFrame
materialize_dataset(reader, out_dir=..., partition_cols=[...])
```

## 7.7.2 Ibis → streaming writer (two options)

Option A (preferred for uniform writer policy):
`to_pyarrow_batches → materialize_dataset` ([Ibis][5])

Option B (Ibis-managed sink):
`expr.to_parquet_dir(directory, **kwargs)` forwards kwargs to `pyarrow.dataset.write_dataset`. ([Ibis][5])

Also note Ibis explicitly warns that Hive-partitioned output is only supported for DuckDB in `to_parquet` (single-file / backend write lane), which is another reason to use `to_parquet_dir` / Arrow dataset writer for partitioned datasets. ([Ibis][5])

## 7.7.3 PyArrow scanner → writer

A canonical ingest/transform/write loop:

* dataset → scanner (projection/filter/batching) → `to_reader()` → `write_dataset`

---

# 7.8 Minimal test harness (correctness + determinism + layout)

You want a small suite you can run in CI for every writer-related PR:

### 7.8.1 Layout invariants

* writes create expected partition directories (if partitioning enabled)
* basenames match template
* file count within expected bounds for `max_rows_per_file` / `max_open_files` ([Apache Arrow][2])

### 7.8.2 Ordering invariants

* If `preserve_order=True`, reading back yields the same row order as input (for a test dataset). ([Apache Arrow][2])
* If you enforce canonical sort upstream, validate `stable_sort_key` monotonicity post-write (don’t rely on implicit order when `preserve_order=False`). ([Apache Arrow][2])

### 7.8.3 Manifest/sidecar invariants

* `file_visitor` collected paths for every file
* if `_metadata` enabled: it exists and is loadable
* capture per-file metadata sizes and warn if aggregate grows unexpectedly ([Apache Arrow][2])

### 7.8.4 “One-shot source” invariants

When upstream is a one-shot stream:

* wrap it with `Scanner.from_batches` to apply scan policy
* verify only a single consumption occurs (no reuse) ([Apache Arrow][6])

---

If you want the next deep dive: **Section 8 (Functions and extensibility: unified function registry)** is where we standardize how you add “kernel lane” functions (PyArrow compute UDFs), “IR lane” functions (Ibis builtin/pyarrow UDFs), and “engine lane” functions (DataFusion UDFs), with a single signature/versioning strategy so your rulepacks stay deterministic and portable.

[1]: https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatchReader.html?utm_source=chatgpt.com "pyarrow.RecordBatchReader — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html "pyarrow.dataset.write_dataset — Apache Arrow v22.0.0"
[3]: https://datafusion.apache.org/python/user-guide/dataframe/index.html "DataFrames — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/python/user-guide/io/arrow.html?utm_source=chatgpt.com "Apache Arrow DataFusion documentation"
[5]: https://ibis-project.org/reference/expression-generic "expression-generic – Ibis"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html "pyarrow.dataset.Scanner — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/_modules/pyarrow/dataset.html?utm_source=chatgpt.com "Source code for pyarrow.dataset - Apache Arrow"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_metadata.html "pyarrow.parquet.write_metadata — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.FileMetaData.html "pyarrow.parquet.FileMetaData — Apache Arrow v22.0.0"
[10]: https://www.influxdata.com/blog/how-good-parquet-wide-tables/?utm_source=chatgpt.com "How Good is Parquet for Wide Tables (Machine Learning ..."

## 8) Functions and extensibility: a unified function registry across PyArrow, Ibis, and DataFusion

This section standardizes **one function contract** that can be implemented at three “lanes”:

* **Kernel lane (PyArrow)**: Arrow compute kernels + Arrow compute UDFs (fastest, Arrow-native)
* **IR lane (Ibis)**: Ibis expressions + Ibis UDF constructors (portable relational IR)
* **Engine lane (DataFusion)**: DataFusion UDF/UDAF/UDWF/UDTF (planner-visible, SQL-executable)

The goal is: **rulepacks stay deterministic and portable** because every function has a single, versioned identity + signature + volatility semantics + fallback mapping.

---

# 8.0 The “lane” decision rule

### Prefer the lowest lane that still meets requirements

1. **Use a built-in (engine or compute) function** whenever possible (max planner optimization; no Python overhead). DataFusion explicitly notes it has “powerful expressions and functions” and recommends using built-in PyArrow functions when possible to avoid copies and improve performance. ([Apache DataFusion][1])
2. If not available, implement as:

   * **Ibis builtin UDF** if the backend already has the function (planner-friendly). ([Ibis][2])
   * **Ibis pyarrow UDF** if it can be expressed as Arrow compute on arrays (vectorized). ([Ibis][2])
3. If it’s execution-engine-specific or needs SQL exposure:

   * **DataFusion Python UDF** for quick iteration (batch-at-a-time over Arrow arrays). ([Apache DataFusion][1])
   * **Rust-backed DataFusion UDF (PyCapsule)** for best performance and named-argument ergonomics (agent-facing surfaces). ([Apache DataFusion][3])

### Hard constraint: which operations are allowed where

* **PyArrow compute UDFs are scalar/elementwise**: “no state,” output length matches input length; these are the only UDFs allowed inside query-engine expressions. ([Apache Arrow][4])
* Any function that requires **global knowledge** (grouping, sorting, window context) belongs in **DataFusion UDAF/UDWF** or engine/IR-level relational operators, not kernel-lane scalar UDFs. ([Apache DataFusion][5])

---

# 8.1 The unified FunctionSpec (your single source of truth)

Every function in your system should be represented by a spec that is **serializable**, **hashable**, and **installable** into one or more lanes.

### 8.1.1 Function identity

* `id`: stable identifier (e.g., `ca.func.stable_hash.v1`)
* `display_name`: human/LLM name
* `engine_name`: the emitted name in the target engine/registry

  * PyArrow compute registry names must be globally unique (one function per name). ([Apache Arrow][4])

### 8.1.2 Signature and typing (Arrow is the lingua franca)

* `kind`: `scalar | aggregate | window | table`
* `input_types`: **Arrow DataTypes** (portable across DF/Ibis/PyArrow)
* `return_type`: Arrow DataType
* `arg_names`: optional, but strongly recommended for agent-facing surfaces:

  * PyArrow compute UDFs accept `in_types` as a **dict mapping argument names → DataType** (argument names used to generate docs). ([Apache Arrow][4])
  * DataFusion Rust UDFs can support named arguments via signature `.with_parameter_names(...)` (see §8.5.6). ([Apache DataFusion][3])

### 8.1.3 Semantics

* `volatility`: `immutable | stable | volatile`

  * DataFusion explicitly defines `Volatility` with string equivalents `"immutable"`, `"stable"`, `"volatile"`. ([Apache DataFusion][5])
* `null_semantics`: propagate / treat-nulls-as / error-on-null (document)
* `ordering_dependence`: whether output depends on row order (should be false for scalar kernels)
* `determinism`: is it deterministic given inputs? (separate from volatility)
* `side_effects`: forbid in compile-time policy unless explicitly allowed

### 8.1.4 Install targets (what code exists)

* `pyarrow_compute`: callable + registration metadata (optional)
* `ibis_udf`: builtin/pyarrow/pandas/python decorator config
* `datafusion_udf`: python udf/udaf/udwf/udtf or PyCapsule export handle

### 8.1.5 Compatibility and fallback mapping

* `fallbacks`: ordered list of alternative implementations (e.g., `builtin → pyarrow → datafusion_py → datafusion_rust`)
* `pushdown_visibility`: `planner_visible` vs `opaque` (UDFs are often opaque; see §8.4.4)

---

# 8.2 Registry shape: compile-time catalog + runtime installers

You want **two layers**:

### 8.2.1 Compile-time registry (pure metadata)

* Holds all `FunctionSpec` objects
* Exposes:

  * `function(id)` lookup
  * `catalog()` to emit LLM-friendly docs
  * `fingerprint()` used in cache keys (functions are part of correctness)

### 8.2.2 Runtime installers (side effects)

* `install_pyarrow_compute()` (process-global)
* `install_datafusion(ctx)` (session-local)
* `install_ibis(con)` (backend-local)

---

# 8.3 Kernel lane: PyArrow compute functions + compute UDFs

## 8.3.1 Built-in kernels are the default

PyArrow compute functions are stored in a global function registry and can be looked up by name. ([Apache Arrow][6])
This matters for:

* auto-generating catalogs
* checking availability before choosing fallbacks

`pyarrow.compute.get_function(name)` looks up a function in the global registry. ([Apache Arrow][7])

## 8.3.2 Expression IR for pushdown contexts (Dataset/Scanner/Acero)

PyArrow dataset expressions are built using compute expressions:

* `pyarrow.compute.field()` to reference columns
* `pyarrow.compute.scalar()` to create scalar expressions
* comparisons + boolean composition build filter expressions ([Apache Arrow][8])

Also: `pyarrow.compute.scalar()` creates an **Expression**, distinct from `pyarrow.scalar()` which creates an Arrow Scalar memory object. ([Apache Arrow][9])

## 8.3.3 Registering a compute UDF: `register_scalar_function`

The core API:
`pyarrow.compute.register_scalar_function(func, function_name, function_doc, in_types, out_type, func_registry=None)` ([Apache Arrow][4])

Key constraints (must be captured in your FunctionSpec):

* scalar UDFs are elementwise; no state; output length matches inputs; **only scalar UDFs are allowed in query-engine expressions** ([Apache Arrow][4])
* first argument is a per-invocation `UdfContext`, with:

  * `batch_length`
  * `memory_pool` (use this pool when calling Arrow APIs that accept `memory_pool=`) ([Apache Arrow][10])
* `in_types` is a **dict of arg name → DataType** (arg names become documentation) ([Apache Arrow][4])
* varargs supported by defining `*args` and letting the last `in_type` apply to remaining args ([Apache Arrow][4])

### Minimal “kernel lane” UDF example (memory-pool aware)

```python
import pyarrow as pa
import pyarrow.compute as pc

DOC = {"summary": "Add one", "description": "Elementwise add 1 to int64"}

def add1(ctx: pc.UdfContext, x: pa.Array) -> pa.Array:
    # Use ctx.memory_pool if downstream calls accept it
    return pc.add(x, 1, memory_pool=ctx.memory_pool)

pc.register_scalar_function(
    add1,
    function_name="ca__add1__v1",
    function_doc=DOC,
    in_types={"x": pa.int64()},
    out_type=pa.int64(),
)
```

(Registration contract and UdfContext semantics are documented.) ([Apache Arrow][4])

### Practical integration guidance

* Use compute UDFs primarily as **building blocks inside**:

  * Ibis `@udf.scalar.pyarrow` bodies
  * DataFusion Python UDF bodies
    because these environments already feed Arrow arrays in batch form (and DataFusion explicitly recommends using PyArrow compute functions for performance). ([Apache DataFusion][1])

---

# 8.4 IR lane: Ibis UDFs (builtin/pandas/pyarrow/python) + rewriteability

## 8.4.1 Scalar UDF constructors are the IR-level extensibility spine

Ibis exposes four scalar UDF constructors:

* `builtin` (maps to a backend built-in)
* `pandas` (vectorized over pandas Series)
* `pyarrow` (vectorized over PyArrow Arrays)
* `python` (row-by-row; slowest) ([Ibis][2])

Each supports:

* `name` (backend name override)
* `database`, `catalog`
* `signature` (or derive from type annotations; for builtin only return type is required)
* backend-specific kwargs ([Ibis][2])

## 8.4.2 Preferred pattern: builtin first

Builtin UDFs are the “planner-visible” option: you expose a backend-native function name but keep Ibis typing and composition.

```python
import ibis

@ibis.udf.scalar.builtin
def hamming(a: str, b: str) -> int:
    """Compute Hamming distance."""
```

(Builtin UDF decorator is documented, including signature rules.) ([Ibis][2])

## 8.4.3 Vectorized compute inside Ibis: `@ibis.udf.scalar.pyarrow`

Use this for kernel-lane logic you want to keep inside the IR layer (portable + fast-ish):

* takes PyArrow Arrays
* returns PyArrow Array/Scalar

(Exact constructor signature is the same pattern as builtin/pandas/python.) ([Ibis][2])

## 8.4.4 Pushdown vs UDF opacity (the “optimizer visibility” problem)

Ibis explicitly discusses that UDFs can prevent planners from applying optimizations like predicate pushdowns because they are opaque. The “Dynamic UDF Rewriting with Predicate Pushdowns” post is the canonical pattern: represent UDF intent at a higher level and rewrite into pushdown-friendly forms under certain predicates. ([Ibis][11])

**Your registry should classify each function as:**

* `pushdown_visible`: builtin mapping or rewriteable macro
* `pushdown_opaque`: python/pyarrow/pandas UDF with no rewrite rule
* `rewriteable`: has an associated SQLGlot/Ibis rewrite pass

## 8.4.5 Naming policy: avoid shadowing backend built-ins

There is a real failure mode: defining an Ibis UDF named `uuid` can shadow a backend builtin `uuid()` and change query behavior. ([GitHub][12])

**Hard rule for your system**

* All non-builtin UDFs must be prefixed (e.g., `ca__...`) and versioned (`__v1`) at the backend name level.

---

# 8.5 Engine lane: DataFusion UDFs/UDAFs/UDWFs/UDTFs

## 8.5.1 DataFusion Python scalar UDFs (batch-at-a-time Arrow arrays)

DataFusion’s Python user guide defines a scalar UDF as:

* a Python function that takes one or more **PyArrow arrays** and returns a single array
* operates on a batch at a time, but evaluation should be row-by-row conceptually ([Apache DataFusion][1])

It also explicitly recommends using PyArrow compute functions when possible because they can compute without copying arrays, improving performance. ([Apache DataFusion][1])

### Minimal scalar UDF example (from DataFusion docs pattern)

```python
import pyarrow as pa
from datafusion import udf, col

def is_null(arr: pa.Array) -> pa.Array:
    return arr.is_null()

is_null_udf = udf(is_null, [pa.int64()], pa.bool_(), "stable")
```

(DataFusion UDF definition pattern + volatility is documented.) ([Apache DataFusion][1])

## 8.5.2 Python API surface for all UDF families (what your registry must support)

The DataFusion Python autoapi documents constructors for:

* scalar UDF: `udf(func, input_types, return_type, volatility, name)`
* aggregate UDF: `udaf(accum, input_types, return_type, state_type, volatility, name)`
* window UDF: `udwf(func, input_types, return_type, volatility, name)` with `WindowEvaluator.evaluate_all(...)`
* table function: `udtf(func, name)` returning a table provider ([Apache DataFusion][5])

It also documents that these constructors can accept **Rust-backed functions via PyCapsule** (signature inferred from underlying function), which is your “graduation path” for performance and richer signatures. ([Apache DataFusion][5])

## 8.5.3 Volatility is a first-class contract in DataFusion

DataFusion defines `Volatility` and allows passing either the enum or the lowercase string equivalents `"immutable"`, `"stable"`, `"volatile"`. ([Apache DataFusion][5])

This should be mirrored in your FunctionSpec and used in:

* cache key policy (immutable functions can be pre-evaluated safely; volatile cannot)
* SQLGlot canonicalization policy (don’t fold volatile calls)

## 8.5.4 Window UDFs (UDWF): deterministic winner-selection tool

The Python API exposes a `WindowEvaluator` pattern (`evaluate_all(values, num_rows)`), and UDWFs can be declared using either a class or a factory returning a class instance. ([Apache DataFusion][5])

This is the engine-lane tool for:

* “winner selection” with deterministic tie-break logic
* partition-aware computations without materializing whole tables in Python loops

## 8.5.5 Table UDFs (UDTF): table-provider generation

DataFusion’s Python API explicitly says:

* “Table functions generate new table providers based on the input expressions.” ([Apache DataFusion][5])

For your system, UDTFs are a good place to hide:

* complex “scan + normalize” logic behind a SQL-callable surface
* or expose “artifact dataset resolution” as a function

## 8.5.6 Named arguments: how to make functions agent-friendly

DataFusion supports SQL calls with named arguments (Postgres-style `param => value`). To support named arguments in a UDF, you add parameter names to the signature using `.with_parameter_names(...)`, and DataFusion will resolve named args into positional order before invocation. ([Apache DataFusion][3])

**Implication for your registry**

* If you want named arguments in SQL calls for your own functions, you likely need a **Rust-backed DataFusion UDF** (PyCapsule export) where you can set parameter names in the signature. ([Apache DataFusion][3])

---

# 8.6 Cross-lane signature and naming strategy (avoid collisions, maximize portability)

## 8.6.1 Canonical naming

* **PyArrow compute registry**: names are global and must be unique (one function per name). ([Apache Arrow][4])
* **DataFusion session**: names are per session but still collide with built-ins; keep prefixing.
* **Ibis**: `name=` controls backend name; collision hazards are real. ([Ibis][2])

**Recommended convention**

* `ca__<function>__v<major>` for backend-visible name
* Keep human-friendly aliases in your FunctionSpec but do not register them in engines

## 8.6.2 Arrow types as the single signature language

* PyArrow UDF registration uses Arrow DataTypes for `in_types` and `out_type`. ([Apache Arrow][4])
* DataFusion Python UDF constructors take lists of `pyarrow.DataType` for inputs and return. ([Apache DataFusion][1])
* Ibis signatures accept types as Python/ibis types and can derive from annotations; your FunctionSpec should store Arrow types and generate Ibis signatures from them. ([Ibis][2])

---

# 8.7 Installation order and lifecycle (what runs when)

## 8.7.1 PyArrow compute UDFs: install once per process

* Registering is global; don’t do it per run.
* Make installer idempotent: check existing names via compute registry lookup (e.g., `get_function`). ([Apache Arrow][7])

## 8.7.2 DataFusion UDFs: install per SessionContext

* Register all UDF/UDAF/UDWF/UDTF objects as part of building your unified execution substrate. ([Apache DataFusion][5])

## 8.7.3 Ibis UDFs: treat as IR-level wrappers

* Most Ibis UDF definitions become expressions that compile down into backend semantics.
* You still track them in the registry so a rulepack knows which functions exist and which lanes are available. ([Ibis][2])

---

# 8.8 “Function registry snapshots” (artifact plane requirements)

For each run, persist:

* registry version hash (FunctionSpec list + versions)
* per-lane installed set:

  * PyArrow compute UDF names registered (process-global)
  * DataFusion UDF/UDAF/UDWF/UDTF names registered in the session
  * Ibis UDF wrappers used by the rulepack

DataFusion’s Python autoapi explicitly provides printable representations of UDF objects and volatility enums; your snapshot should store these values. ([Apache DataFusion][5])

---

# 8.9 Minimal “unified registry” scaffolding (implementation-ready)

```python
from dataclasses import dataclass
import pyarrow as pa

@dataclass(frozen=True)
class FunctionSpec:
    id: str
    engine_name: str            # ca__foo__v1
    kind: str                   # scalar/aggregate/window/table
    input_types: list[pa.DataType]
    return_type: pa.DataType
    volatility: str             # immutable/stable/volatile
    arg_names: list[str] | None # for docs / named args lanes
    lane_priority: list[str]    # ["builtin", "ibis_pyarrow", "df_py", "df_rust", "pa_compute"]

@dataclass
class FunctionRegistry:
    funcs: dict[str, FunctionSpec]

    def install_pyarrow_compute(self):
        # register_scalar_function for those with pa_compute impls
        ...

    def install_datafusion(self, ctx):
        # ctx.register_udf/udaf/udwf/udtf or udf(...) objects
        ...

    def install_ibis(self, con):
        # no-op for builtin wrappers; keep metadata for catalogs
        ...
```

---

## What to do next

If you want, the next deep dive is **Section 9 (Nested data integration)** where we unify list/struct/map handling across:

* SQLGlot transforms (UNNEST↔EXPLODE),
* Ibis `unnest/unpack`,
* DataFusion UNNEST,
* PyArrow explode kernels (`list_parent_indices`, `list_flatten`, `take`),
  with a deterministic “explode offsets + parent alignment” contract.

[1]: https://datafusion.apache.org/python/user-guide/common-operations/udf-and-udfa.html "User-Defined Functions — Apache Arrow DataFusion  documentation"
[2]: https://ibis-project.org/reference/scalar-udfs "scalar-udfs – Ibis"
[3]: https://datafusion.apache.org/library-user-guide/functions/adding-udfs.html "Adding User Defined Functions: Scalar/Window/Aggregate/Table Functions — Apache DataFusion  documentation"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.register_scalar_function.html "pyarrow.compute.register_scalar_function — Apache Arrow v22.0.0"
[5]: https://datafusion.apache.org/python/autoapi/datafusion/user_defined/index.html "datafusion.user_defined — Apache Arrow DataFusion  documentation"
[6]: https://arrow.apache.org/docs/cpp/compute.html?utm_source=chatgpt.com "Compute Functions — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/dev/python/generated/pyarrow.compute.get_function.html?utm_source=chatgpt.com "pyarrow.compute.get_function — Apache Arrow v23.0.0.dev86"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Expression.html?utm_source=chatgpt.com "pyarrow.dataset.Expression — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs//python/generated/pyarrow.compute.scalar.html "pyarrow.compute.scalar — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.UdfContext.html "pyarrow.compute.UdfContext — Apache Arrow v22.0.0"
[11]: https://ibis-project.org/posts/udf-rewriting/?utm_source=chatgpt.com "Dynamic UDF Rewriting with Predicate Pushdowns - Ibis"
[12]: https://github.com/ibis-project/ibis/issues/8263?utm_source=chatgpt.com "don't shadow builtin functions in python, pyrarrow, pandas ..."

## 9) Nested data integration: list/struct/map end-to-end

This section standardizes *one* cross-library contract for nested data so you can author rules once and implement them in whichever lane is best (Ibis/DF, DF direct, or Arrow kernels) without losing determinism.

---

# 9.0 Canonical contract: “Explode with offsets + parent alignment”

### 9.0.1 Canonical relational output shape

For any nested column you explode, your canonical relational form is:

* `parent_key...` (one or more stable parent identifiers; **required**)
* `idx` (0-based integer element index within the parent list; **optional but strongly recommended**)
* `value` (element value; can be scalar or struct)
* optional flags:

  * `is_empty_parent` (true if parent list was empty)
  * `is_null_parent` (true if parent list was null)

**Determinism invariants**

1. `idx` is **0-based** everywhere (matches Ibis `offset` examples and DataFusion `[]` indexing). ([Ibis][1])
2. When `keep_empty=True`, every parent row emits **≥ 1** output row:

   * empty or null list → one row with `value = NULL`, `idx = NULL` (matches Ibis behavior). ([Ibis][1])
3. Output ordering is always made explicit:

   * `ORDER BY parent_key, idx` (and if you need original parent row order, include `original_row` as a stable key; see Ibis guidance). ([Ibis][1])

---

# 9.1 SQLGlot layer: keep UNNEST as the canonical AST, transform per dialect

SQLGlot is where you normalize *syntax differences* (UNNEST vs EXPLODE) while keeping the *semantic contract* stable.

### 9.1.1 Canonical AST shape: `UNNEST` with optional offset

Treat `UNNEST` as your internal canonical representation. Dialect emission happens later via transforms.

### 9.1.2 UNNEST ↔ EXPLODE transform (SQLGlot)

SQLGlot provides a first-class transform:

* `unnest_to_explode(expression, unnest_using_arrays_zip=True)` converts **cross join UNNEST** into **lateral view explode**, and:

  * uses `ARRAYS_ZIP(...)` + `INLINE(...)` for multi-array unnest (when enabled)
  * chooses `POSEXPLODE` when an offset is present
  * otherwise chooses `EXPLODE` (single array) or `INLINE` (multi expr) ([SQLGlot][2])

This is the exact mechanism that lets you keep your internal `idx` contract (offset) and still generate Spark/Databricks-friendly SQL.

### 9.1.3 “Qualify side effects”: unnest alias cleanup

Qualification can introduce unnest table aliases. SQLGlot has `unqualify_unnest(...)` to remove those alias references added by `qualify_columns`. ([SQLGlot][2])

**Policy rule**

* Your pipeline should canonicalize to `UNNEST` first (qualify/pushdowns), then apply dialect transforms like `unnest_to_explode` at emit time.

---

# 9.2 Ibis layer: the preferred authoring surface (offset + keep_empty + ordering guidance)

Ibis is your best “rule authoring” surface for nested data because it already encodes the determinism knobs directly.

### 9.2.1 `Table.unnest(column, offset=None, keep_empty=False)`

Ibis `unnest` is explicitly defined as:
`unnest(column, *, offset=None, keep_empty=False)` ([Ibis][1])

* `offset`: name of the index column (produces 0-based indices in the example) ([Ibis][1])
* `keep_empty=True`: keeps empty arrays **and existing NULL arrays** as `NULL` rows (matching your canonical “≥1 row per parent” contract). ([Ibis][1])

**Critical determinism note (Ibis docs say this explicitly):**
If you need to preserve original row order for kept empty arrays / nulls, create an index column first using `row_number()` before `unnest`, then `order_by`. ([Ibis][1])

#### Canonical Ibis explode pattern

```python
import ibis
from ibis import _

t2 = (
    t
    .mutate(original_row=ibis.row_number())
    .unnest("x", offset="idx", keep_empty=True)
    .order_by("original_row", "idx")
)
```

This pattern is directly recommended in the docs. ([Ibis][1])

### 9.2.2 `Table.unpack(*columns)` for struct flattening

`unpack` “projects the struct fields … into self” and retains existing fields. ([Ibis][1])

This is your canonical move after exploding `list<struct<...>>`:

```python
t_exploded = t.unnest("edges", offset="idx")
t_flat = t_exploded.unpack("edges")  # edges.foo, edges.bar become columns
```

### 9.2.3 Keep Ibis as the “offset truth”

Because DataFusion and SQL dialects differ on ordinality support, your best practice is:

* author list explosions in Ibis with `offset="idx"`
* let the backend compilation + SQLGlot policy engine guarantee the semantic contract

---

# 9.3 DataFusion layer: UNNEST, struct unnest, map normalization, and DataFrame API

DataFusion has explicit SQL and Python APIs for nested types and unnesting.

## 9.3.1 SQL: `unnest(array_or_map)` expands into rows

DataFusion SQL “Special Functions” documents:

* `unnest`: “Expands an array or map into rows.” ([Apache DataFusion][3])
* example: `select unnest(make_array(1, 2, 3, 4, 5))` ([Apache DataFusion][3])

This is the primitive for “row-multiplying” explosions.

## 9.3.2 SQL: `unnest(struct)` expands fields into columns (not row-multiplying)

DataFusion also documents:

* `unnest(struct)` expands struct fields into individual columns, with fields prefixed by `__unnest_placeholder(...)` and accessed via `"__unnest_placeholder(<struct>).<field>"`. ([Apache DataFusion][3])

This is “struct flattening” in SQL form.

## 9.3.3 SQL: Map normalization via `map_entries(map)` → list<struct{key,value}>

DataFusion documents a very clean map→list conversion:

* `map_entries(map)` returns a list of entries like `[{key: a, value: 1}, ...]`. ([Apache DataFusion][4])

So your canonical map explosion in DataFusion is:

1. `map_entries(m)` → list of structs
2. `unnest(...)` to expand entries into rows
3. then either:

   * `unnest(struct)` to expand `{key,value}` fields into columns, or
   * struct field access (see below)

This avoids ambiguity about what “unnest(map)” yields.

## 9.3.4 Python DataFrame API: `unnest_columns(*columns, preserve_nulls=True)`

DataFusion Python documents a direct method:

* `unnest_columns(*columns: str, preserve_nulls: bool = True)`
  “Expand columns of arrays into a single row per array element.” ([Apache DataFusion][5])

Use this for:

* simple array explosions where you do **not** need `idx`
* fast iteration before you push offset logic into Ibis/SQLGlot or into Arrow kernels

### 9.3.5 Offsets and indexing in DataFusion Python: 0-based `[]` and `cardinality`

DataFusion Python expression docs state:

* array element access via `col("a")[0]` is **0-based**, while `array_element()` is **1-based** for SQL compatibility ([Apache DataFusion][6])
* `cardinality(col("a"))` returns total number of elements in an array ([Apache DataFusion][6])
* `array_empty(col("a"))` checks emptiness ([Apache DataFusion][6])

These are the pieces you use when you must synthesize `idx` in DataFusion-land (outside Ibis):

* build an index array `range(0, cardinality(arr))` (DataFusion SQL shows `range` feeding `unnest`) ([Apache DataFusion][3])
* retrieve `arr[idx]` using 0-based bracket indexing ([Apache DataFusion][6])

**Practical guidance:** for consistent `idx` semantics across backends, prefer the Ibis `offset=` surface; treat “DF direct idx synthesis” as a specialized path.

### 9.3.6 Struct field access in DataFusion Python

DataFusion Python docs show struct access with bracket notation (dict-style):

* `col("a")["size"]` extracts a struct field. ([Apache DataFusion][6])

This pairs naturally with:

* exploding `list<struct>` into a struct column
* then selecting fields directly

---

# 9.4 PyArrow kernel lane: explode and normalize without Python loops

This is your fastest path when you want to avoid engine overhead and stay Arrow-native.

## 9.4.1 Core explode kernels

* `pc.list_parent_indices(lists)` emits the top-level parent list index for each flattened element. ([Apache Arrow][7])
* `pc.list_flatten(lists, recursive=False)` flattens list values; **null list values emit nothing**. ([Apache Arrow][8])
* `pc.take(data, indices)` selects values/records; **null indices yield nulls**. ([Apache Arrow][9])
* `pc.list_value_length(lists)` emits list lengths; null lists emit null. ([Apache Arrow][10])

### Canonical kernel explode (parent alignment)

```python
import pyarrow.compute as pc

parent_ix = pc.list_parent_indices(list_col)   # parent row index per element
values    = pc.list_flatten(list_col)          # element values
parent_id = pc.take(parent_id_col, parent_ix)  # align parent id to values
```

All three operations are fully specified in the docs. ([Apache Arrow][7])

## 9.4.2 Generating `idx` (offset) deterministically in PyArrow

PyArrow list arrays expose their offsets directly:

* `ListArray.offsets` returns list offsets as an int32 array. ([Apache Arrow][11])
  And Arrow’s data model doc reminds that List arrays have an offsets buffer (and ListView differs by having offsets + sizes). ([Apache Arrow][12])

**Vectorized `idx` generation approach**

1. Obtain offsets and lengths.
2. Construct `idx` per element as `arange(total_values) - repeat(offsets[:-1], lengths)` (NumPy repeat), then convert back to Arrow.

This keeps the “no Python loops per element” property (it’s vectorized), while giving you stable 0-based idx.

## 9.4.3 Implementing `keep_empty=True` in kernel lane

Because `list_flatten` emits **no rows** for empty lists and null lists, you must add the “≥1 row per parent” rows yourself.

Use:

* `lengths = pc.list_value_length(list_col)` (null lists → null length) ([Apache Arrow][10])
* identify parents where `lengths == 0` or `lengths is null`
* left-join the exploded table back to the parent table, or union in sentinel rows

(Join can be done Arrow-native via `Table.join` / `Dataset.join` if the intermediate is in memory, or via DataFusion/Ibis as a relational step.)

## 9.4.4 Struct construction and extraction in kernel lane

When values are structs (e.g., exploding `list<struct<...>>`) or you need to pack/unpack:

* `pc.make_struct(*args, field_names=..., field_nullability=..., field_metadata=...)` builds a `StructArray` ([Apache Arrow][13])
* `pc.struct_field(values, indices, options=...)` extracts struct children (supports chained lookup via `StructFieldOptions`). ([Apache Arrow][14])
* You can also build struct arrays directly with `StructArray.from_arrays`. ([Apache Arrow][15])

**Map note:** Arrow maps are represented as a list of struct entries (`key`, `value`), so the same explode+struct extraction pattern applies once you’ve normalized to entries (mirrors DataFusion `map_entries`). ([Apache DataFusion][4])

---

# 9.5 Multi-array unnest (“zip” semantics) and how to keep it deterministic

You will eventually encounter rules that want to unnest multiple arrays “in parallel” (zip), not cross-product.

### SQLGlot supports zipped multi-array UNNEST → `ARRAYS_ZIP` + `INLINE`

SQLGlot’s `unnest_to_explode` explicitly uses `ARRAYS_ZIP` when unnesting multiple expressions (and then `INLINE`), and it can raise if multi-array unnest is requested but arrays_zip is disabled. ([SQLGlot][2])

**Your policy choice**

* Canonical internal form: `UNNEST(array1, array2, ...) WITH offset` (if your authoring surface supports it)
* Emit per dialect:

  * Spark-like: `INLINE(ARRAYS_ZIP(...))` and `POSEXPLODE` if you need idx ([SQLGlot][2])
* If the target engine doesn’t support zipped unnest natively:

  * normalize to `idx` + `array[idx]` per array (index-driven reconstruction)

---

# 9.6 Failure modes and hardening checklist

### Ordering and offsets

* Do not assume row order is preserved by unnest operations; always make ordering explicit (`ORDER BY parent_key, idx`) and use explicit `idx` when correctness depends on order. (Ibis explicitly calls this out and provides the `row_number()` pattern.) ([Ibis][1])

### Empty/null parent semantics

* Ibis `keep_empty=True` is the simplest and most explicit semantics for empty/null parents (produces NULL rows). ([Ibis][1])
* PyArrow `list_flatten` drops null parents entirely (emits nothing), so you must add rows yourself when you need “keep_empty.” ([Apache Arrow][8])

### Struct expansion differences (row-multiplying vs row-preserving)

* DataFusion distinguishes:

  * `unnest(array_or_map)` → row-multiplying
  * `unnest(struct)` → row-preserving field expansion with placeholder prefix ([Apache DataFusion][3])

### Map semantics

* Normalize maps to entries first (`map_entries`) to avoid ambiguity; DataFusion documents that `map_entries` returns list of `{key,value}` structs. ([Apache DataFusion][4])

### Zipped multi-array unnest

* If you rely on zipped semantics across dialects, treat SQLGlot’s `ARRAYS_ZIP` rewrite as the canonical cross-dialect bridge (and ensure you test “shorter array fills missing with NULL” semantics in the target dialect). ([SQLGlot][2])

---

## What to implement next in your repo

1. A shared `ExplodeSpec` (parent keys, column name, idx name, keep_empty, map/struct mode).
2. A **single dispatcher**:

   * prefer Ibis `unnest(offset=..., keep_empty=...)` for most rules
   * fallback to kernel lane for hotspots (PyArrow explode + idx)
   * allow DataFusion `unnest_columns` for simple “no-idx” expansions ([Apache DataFusion][5])
3. Golden tests:

   * list with empty + null + null-elements
   * list<struct> + unpack
   * map_entries + explode + struct field extraction

If you want the next deep dive: **Section 10 (Schema contracts and type systems)** is where we make these nested outputs stable across releases by pinning Arrow schemas, SQLGlot qualification schema maps, and Ibis Schema→SQLGlot DDL shapes—so nested products don’t drift silently.

[1]: https://ibis-project.org/reference/expression-tables "expression-tables – Ibis"
[2]: https://sqlglot.com/sqlglot/transforms.html "sqlglot.transforms API documentation"
[3]: https://datafusion.apache.org/user-guide/sql/special_functions.html "Special Functions — Apache DataFusion  documentation"
[4]: https://datafusion.apache.org/user-guide/sql/scalar_functions.html "Scalar Functions — Apache DataFusion  documentation"
[5]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[6]: https://datafusion.apache.org/python/user-guide/common-operations/expressions.html "Expressions — Apache Arrow DataFusion  documentation"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_parent_indices.html?utm_source=chatgpt.com "pyarrow.compute.list_parent_indices — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_flatten.html?utm_source=chatgpt.com "pyarrow.compute.list_flatten — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.take.html?utm_source=chatgpt.com "pyarrow.compute.take — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_value_length.html?utm_source=chatgpt.com "pyarrow.compute.list_value_length — Apache Arrow v22.0.0"
[11]: https://arrow.apache.org/docs/python/generated/pyarrow.ListArray.html?utm_source=chatgpt.com "pyarrow.ListArray — Apache Arrow v22.0.0"
[12]: https://arrow.apache.org/docs/python/data.html?utm_source=chatgpt.com "Data Types and In-Memory Data Model - Apache Arrow"
[13]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.make_struct.html?utm_source=chatgpt.com "pyarrow.compute.make_struct — Apache Arrow v22.0.0"
[14]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.struct_field.html?utm_source=chatgpt.com "pyarrow.compute.struct_field — Apache Arrow v22.0.0"
[15]: https://arrow.apache.org/docs/python/generated/pyarrow.StructArray.html?utm_source=chatgpt.com "pyarrow.StructArray — Apache Arrow v22.0.0"

## 10) Schema contracts and type systems

The goal is that **nested outputs (list/struct/map)** and **wide relational products** don’t drift silently across:

* **PyArrow** physical batches + Parquet storage,
* **DataFusion** execution schemas,
* **Ibis** IR schemas and type inference,
* **SQLGlot** qualification/type inference and canonicalization.

This section standardizes a *single schema contract* that every lane can produce, validate, and fingerprint.

---

# 10.1 The “Schema Contract Pack” as the unit of correctness

For each materialized product (e.g., `edges_call`, `nodes_symbol`, `props_func`), define a Schema Contract Pack (SCP) with:

1. **Arrow schema** (`pa.Schema`) as the canonical storage/ABI schema (includes metadata) ([Apache Arrow][1])
2. **Ibis schema** (`ibis.Schema`) derived from Arrow schema (or authored first) and used for rule authoring / validation ([Ibis][2])
3. **SQLGlot schema mapping** used for qualification/type inference and deterministic canonical SQL shapes ([SQLGlot][3])
4. **DataFusion read schema** (explicitly supplied where possible) to avoid inference drift and metadata conflicts ([Apache DataFusion][4])
5. **Parquet fidelity policy** (whether `ARROW:schema` is preserved; whether readers skip schema metadata) ([Apache Arrow][5])

Everything else (DDL, manifests, lineage, plan fingerprints) is derived from these.

---

# 10.2 Arrow schema as the canonical “batch ABI” contract

## 10.2.1 Schema metadata is part of the contract

PyArrow schemas include metadata and are used to roundtrip semantics (e.g., pandas types). ([Apache Arrow][1])
Your SCP must treat schema **metadata** as correctness-relevant (not optional decoration).

## 10.2.2 Field-level metadata and deterministic flattening rules

`pyarrow.Field` supports:

* `with_metadata(metadata)` / `remove_metadata()` for metadata control
* `flatten()` to expand nested struct fields deterministically (prefixing child names) ([Apache Arrow][6])

Use this for:

* stable “flattened views” of nested payload structs (e.g., for fingerprinting, for backends with poor nested support).

### Canonical Arrow schema authoring pattern

* Always use `pa.schema([...])` and explicit `pa.field(...)`
* Explicitly set `nullable` and metadata where it matters
* Keep field order stable (it is semantically significant for some comparisons)

---

# 10.3 Extension types: embedding semantic types (StableId, Span) without changing storage

If you want your schemas to carry **domain semantics** (e.g., `StableId`, `Span`), use Arrow **extension types**.

## 10.3.1 Extension type identity is name-based and IPC-relevant

`pyarrow.ExtensionType(storage_type, extension_name)` requires a **unique name**, which “will be used when deserializing IPC data.” ([Apache Arrow][7])

## 10.3.2 Registration is based on extension name

`pyarrow.register_extension_type(ext_type)` registers a Python extension type, and registration is based on the extension name (unique per type). Once registered, it applies to all instances of that subclass. ([Apache Arrow][8])

**Contract rule**

* Extension type names must be globally unique and versioned (e.g., `ca.stable_id.v1`).
* Put the extension name in your schema fingerprint.

---

# 10.4 Parquet schema fidelity: controlling what gets preserved and what gets ignored

## 10.4.1 Arrow schema round-tripping via `ARROW:schema`

`pyarrow.parquet.ParquetWriter` documents that by default the Arrow schema is serialized and stored in Parquet metadata under `ARROW:schema`, and it is used on read to more faithfully recreate data (timezone-aware timestamps, duration restoration, etc.). ([Apache Arrow][5])

**Contract rule**

* For any product where logical types matter (timestamps with tz, duration, extension types), keep `store_schema=True` (default) and treat `ARROW:schema` as part of the persisted contract. ([Apache Arrow][5])

## 10.4.2 Page index and corruption checks (optional but contract-relevant)

ParquetWriter documents:

* `write_page_index` gathers statistics in a page index to avoid scattered IO; page index “is not yet used on the read side by PyArrow.” ([Apache Arrow][5])
* `write_page_checksum` enables corruption detection. ([Apache Arrow][5])

These are “robustness knobs” you capture in your runtime profile.

---

# 10.5 Ibis Schema as the authoring + validation surface (and how it ties to SQLGlot and Arrow)

## 10.5.1 Nullability syntax and schema equality

Ibis schema examples show `!string` to mark non-nullable string fields. ([Ibis][2])
`Schema.equals(other)` takes **field order into account**, so order becomes part of the contract. ([Ibis][2])

## 10.5.2 Bidirectional SQLGlot schema interop

Ibis Schema supports:

* `Schema.from_sqlglot(schema_expr, dialect=None)` where `schema_expr` is a SQLGlot `sge.Schema` of column definitions (dialect-aware type conversion) ([Ibis][2])
* `Schema.to_sqlglot_column_defs(dialect)` which returns a list of SQLGlot `ColumnDef` nodes (preferred; `to_sqlglot` is deprecated) ([Ibis][2])

This is the critical bridge for contract-driven DDL: “schema is AST.”

## 10.5.3 Arrow schema interop

Ibis Schema includes `to_pyarrow()` to return an equivalent PyArrow schema. ([Ibis][2])

**Contract rule**

* Keep *one* canonical schema representation (Arrow), but always be able to derive Ibis Schema and SQLGlot column defs from it, and assert equality in tests.

---

# 10.6 SQLGlot schema mapping: qualification/type inference must be deterministic

## 10.6.1 Schema mapping forms and nesting depth

SQLGlot’s optimizer APIs accept schema mappings in three nesting forms:

1. `{table: {col: type}}`
2. `{db: {table: {col: type}}}`
3. `{catalog: {db: {table: {col: type}}}}` ([SQLGlot][9])

This directly matches your namespacing plan (catalog/schema/table).

## 10.6.2 Qualification uses schema and has explicit db/catalog defaults

`sqlglot.optimizer.qualify.qualify(...)` accepts:

* `db` and `catalog` defaults
* `schema` used to infer column names/types
* `canonicalize_table_aliases` to stabilize aliasing
* and it stresses that `expand_stars` is necessary for most optimizer rules. ([SQLGlot][3])

**Contract rule**

* Qualification schema must be derived from your canonical Arrow schema (or Ibis Schema) and include the correct depth (catalog/db/table) you intend to use.
* Always record `db`, `catalog`, and `dialect` in artifacts; they affect qualification results. ([SQLGlot][3])

## 10.6.3 Schema object API (normalize + match depth)

SQLGlot’s `schema` module emphasizes:

* added tables must match the schema’s nesting depth
* `normalize` and `match_depth` control identifier normalization and depth enforcement when adding or querying schema entries ([SQLGlot][10])

This is where you enforce: “queries must reference the same qualified paths your contract declares.”

---

# 10.7 DataFusion schema ingestion: always prefer explicit schemas + metadata policy

## 10.7.1 Parquet reading supports explicit schemas and metadata skipping

DataFusion’s Parquet reader surface documents:

* `skip_metadata`: skip metadata in file schema to avoid schema conflicts due to metadata
* `schema`: optional explicit schema; if None, DataFusion will infer it from the file
* `table_partition_cols`: declare partition columns
* `parquet_pruning`: use predicate to prune row groups
* `file_sort_order`: declare sort order ([Apache DataFusion][4])

**Contract rule**

* For contracted products, provide `schema=` explicitly wherever you can.
* Use `skip_metadata=True` as a compatibility tool when differing Parquet metadata causes conflicts. ([Apache DataFusion][4])

## 10.7.2 Known hazard: schema metadata propagation may differ from expectations

There are reported issues where DataFusion `read_parquet` / `register_parquet` does not return schema metadata from Parquet even when configured not to skip metadata. Treat metadata fidelity as something you validate in integration tests rather than assume. ([GitHub][11])

Practical response:

* keep Arrow schema fidelity via `ARROW:schema` in Parquet (PyArrow writer default) ([Apache Arrow][5])
* validate that DataFusion’s inferred/returned schema matches the canonical Arrow schema (with an explicit test gate)

---

# 10.8 Schema evolution and compatibility tiers (strict vs permissive)

## 10.8.1 Discovery-time schema unification (best practice for artifact directories)

`FileSystemDatasetFactory.inspect(promote_options='default'|'permissive')` explicitly defines:

* `default`: types must match exactly, except null merges
* `permissive`: types promoted when possible ([Apache Arrow][12])

And `finish(schema=None)` creates a Dataset using the inspected schema or an explicit schema. ([Apache Arrow][13])

## 10.8.2 Explicit schema merge: `pyarrow.unify_schemas`

`pyarrow.unify_schemas(schemas, promote_options='default')`:

* merges fields by name (union of fields)
* fails on different types by default
* inherits field metadata from the first schema where the field appears
* preserves ordering based on the first schema’s field order ([Apache Arrow][14])

**Compatibility tiering**

* **Strict contract tier**: `promote_options="default"` everywhere (fail fast on drift).
* **Evolution tier**: `promote_options="permissive"` but *write back* the promoted schema as the new contract version and re-fingerprint the product.

---

# 10.9 Deterministic schema fingerprints: what to hash and why

You need schema fingerprints to:

* decide whether to rebuild downstream products,
* prevent silent drift across lanes,
* verify that “nested explode outputs” still have the same structural meaning.

## 10.9.1 What must be included

At minimum:

* field names (ordered)
* logical Arrow types (including nested shapes and extension type names)
* nullability flags
* field metadata and schema metadata (sorted keys)
* for extension types: `extension_name` (IPC deserialization identity) ([Apache Arrow][7])

## 10.9.2 Cross-lane fingerprints

Store *two* fingerprints:

1. **Arrow schema fingerprint** (storage/ABI truth)
2. **SQLGlot DDL fingerprint** derived from Ibis Schema → SQLGlot ColumnDefs (execution/compiler truth) ([Ibis][2])

This catches mismatches where Arrow types look identical but SQL dialect mapping changes.

---

# 10.10 “Contract gates” you should enforce in CI

For every materialized product:

1. **Arrow schema gate**

* read back a representative batch / schema
* assert it equals the canonical schema (including metadata if you care)
* assert extension types are registered and resolved correctly (if used) ([Apache Arrow][8])

2. **Ibis schema gate**

* derive Ibis Schema from contract
* assert it equals expected (order-sensitive) ([Ibis][2])

3. **SQLGlot qualification gate**

* build SQLGlot schema mapping at correct depth
* run `qualify(..., validate_qualify_columns=True, expand_stars=True)` and fail fast on unresolved columns ([SQLGlot][3])

4. **DataFusion read schema gate**

* read parquet with explicit `schema=` where possible
* decide policy for `skip_metadata` (and record it)
* assert DataFusion’s resulting schema matches the contract (at least on names/types; metadata may require special handling) ([Apache DataFusion][4])

---

## Next deep dive

Section 11 (“Plans, fingerprints, and incremental rebuild”) is the natural continuation: we combine **schema fingerprints** (this section) with **canonical SQLGlot AST fingerprints + semantic diffs + lineage** to get “rebuild only what changed” with high confidence.

[1]: https://arrow.apache.org/docs/python/generated/pyarrow.Schema.html "pyarrow.Schema — Apache Arrow v22.0.0"
[2]: https://ibis-project.org/reference/schemas "schemas – Ibis"
[3]: https://sqlglot.com/sqlglot/optimizer/qualify.html "sqlglot.optimizer.qualify API documentation"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/io/index.html "datafusion.io — Apache Arrow DataFusion  documentation"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html "pyarrow.parquet.ParquetWriter — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.Field.html "pyarrow.Field — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.ExtensionType.html "pyarrow.ExtensionType — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.register_extension_type.html "pyarrow.register_extension_type — Apache Arrow v22.0.0"
[9]: https://sqlglot.com/sqlglot/optimizer/optimizer.html "sqlglot.optimizer.optimizer API documentation"
[10]: https://sqlglot.com/sqlglot/schema.html "sqlglot.schema API documentation"
[11]: https://github.com/apache/arrow-datafusion/issues/9081?utm_source=chatgpt.com "`ctx.read_parquet` and `ctx.register_parquet` don't load ..."
[12]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.FileSystemDatasetFactory.html?utm_source=chatgpt.com "pyarrow.dataset.FileSystemDatasetFactory - Apache Arrow"
[13]: https://arrow.apache.org/docs/13.0/python/generated/pyarrow.dataset.FileSystemDatasetFactory.html?utm_source=chatgpt.com "pyarrow.dataset.FileSystemDatasetFactory - Apache Arrow"
[14]: https://arrow.apache.org/docs/python/generated/pyarrow.unify_schemas.html "pyarrow.unify_schemas — Apache Arrow v22.0.0"

## 11) Plans, fingerprints, and incremental rebuild

This section turns your stack into a **deterministic build system**:

* every product build produces a **stable identity** (fingerprint),
* every build emits **plan + lineage artifacts** to explain “why,”
* and rebuild decisions are made by comparing **semantic diffs**, not timestamps.

---

# 11.1 Build identity: what you fingerprint and why

You want a single **Build Identity** for each product that answers:

> “Given these inputs + policies, is the output *guaranteed* to be the same?”

### 11.1.1 Four required fingerprints

**A) Schema fingerprint (storage/ABI truth)**
Use your Section 10 Schema Contract Pack. For Arrow schemas, include:

* ordered field names/types/nullability
* field + schema metadata
* extension type names (IPC deserialization identity) (if used)

**B) Query fingerprint (compiler truth)**
Use **SQLGlot canonical AST** as the query identity:

* `sqlglot.serde.dump(Expression)` produces a JSON-serializable payload; `load` reconstructs it. ([SQLGlot][1])
* Always fingerprint **after** your canonicalization pipeline.

Recommended:

```python
import json, hashlib
from sqlglot.serde import dump

payload = dump(canonical_expr)
query_fp = hashlib.sha256(
    json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()
```

([SQLGlot][1])

**C) Runtime profile fingerprint (planner truth)**
Your output can differ when runtime policy differs. So hash:

* DataFusion `SessionConfig` structured knobs + every `.set(k,v)` override
* DataFusion `RuntimeEnvBuilder` choices (spill pool, disk manager)
* SQLGlot policy engine config (read/write dialects, rule list, generator knobs)
* Arrow CPU/IO pool sizes + Parquet read/write option policies

(You already have a `DataFusionRuntimeProfile` schema; this is where it becomes a cache key.)

**D) Function registry fingerprint (semantic truth)**
Because UDFs and function mappings alter semantics:

* registry version hash
* installed UDF sets (PyArrow compute UDFs are process-global; DataFusion UDFs are session-local)
  PyArrow scalar UDF registration is name-based in the compute registry. ([Apache DataFusion][2])

### 11.1.2 Product Build ID

A practical Build ID is:

```
build_id = hash(schema_fp, query_fp, runtime_fp, func_fp)
```

Store outputs under:

```
.../product=<name>/build=<build_id>/
```

That gives you perfect cache hits and makes “incremental rebuild” a pure DAG recomputation problem.

---

# 11.2 Canonical query identity: the SQLGlot policy engine as your compiler backend

### 11.2.1 Pin the optimizer pipeline

SQLGlot’s `optimize()` is the canonical “rewrite to optimized/canonical form” entrypoint. ([SQLGlot][3])

Qualification is explicitly described as necessary for all further SQLGlot optimizations. ([SQLGlot][4])
So the minimal “identity pipeline” is:

1. `qualify(...)` (expand stars, validate columns, canonicalize aliases)
2. `normalize_identifiers(...)` (stabilize case)
3. optional predicate normalization (CNF/DNF with guardrails)
4. `pushdown_predicates(...)` and `pushdown_projections(...)` for shape stability and pushdown-friendliness ([SQLGlot][5])
5. `annotate_types` + `canonicalize` + `simplify` (type-driven canonical form) ([SQLGlot][6])

You can use the default RULES or a curated list, but whichever you choose must be **versioned** (rule list becomes part of the runtime fingerprint). ([SQLGlot][3])

---

# 11.3 Semantic diffs: change detection that drives rebuilds

### 11.3.1 SQLGlot diff outputs are edit scripts on AST nodes

SQLGlot’s diff documentation describes:

* building a set of node matchings
* generating an “edit script” (insert/remove/update/etc.) that transforms source AST → target AST. ([SQLGlot][7])

This is the right primitive for rebuild decisions because it’s structure-aware.

### 11.3.2 “Diff after canonicalization” is non-negotiable

Run the policy engine first, then diff:

* It eliminates noise (alias churn, predicate placement changes)
* It aligns with your “canonical SQL shape spec”

### 11.3.3 A rebuild policy map that’s implementable today

You’ll refine this over time, but a pragmatic starting map:

**Always rebuild the product if any of these change types occur:**

* `FROM` sources change (table name, CTE definition changes)
* join graph changes (join type, join predicate)
* WHERE predicate changes (semantic filter)
* GROUP BY / window specs change
* UNNEST / explode patterns change (row multiplication risk)

**Conditionally rebuild if:**

* only projections change

  * If downstream consumers don’t select the new columns, you can skip rebuilding downstream, but the product itself changed (schema_fp changes).
* only formatting changes

  * should not happen after canonicalization, which is the point.

Because SQLGlot diff can have edge cases (especially around window constructs in some real-world reports), keep a conservative fallback: “if uncertain, rebuild” and validate semantics on tiny fixtures via SQLGlot’s executor harness when needed. ([SQLGlot][7])

---

# 11.4 Lineage-driven dependency extraction: “rebuild only what changed” at column granularity

### 11.4.1 Lineage API and the “trim_selects” lever

`sqlglot.lineage.lineage(...)` accepts:

* optional pre-built `scope`
* `trim_selects=True` to clean up selects by trimming to relevant columns
* qualification kwargs via `**kwargs` ([SQLGlot][8])

This gives you column-level dependencies:

* product output column → base source columns

### 11.4.2 Scope caching for scalability

Scope traversal is its own subsystem: it traverses SQL by “scopes,” where Scope is the context of a SELECT, needed for resolving sources in subqueries. ([SQLGlot][9])

SQLMesh shows a production pattern: cache `(query, scope)` and call SQLGlot lineage repeatedly with `scope=...` and `copy=False` to avoid rebuilding scope per column. ([sqlmesh.readthedocs.io][10])

### 11.4.3 Using lineage to shrink scans and stabilize plans

Once you have `RequiredColumnSet`:

* drive PyArrow scanner projections (`columns=[...]`)
* drive DataFusion projection pushdown expectations
* record “required inputs” as an artifact so reviewers understand why a scan reads certain columns

---

# 11.5 Engine plan artifacts: DataFusion plans, explain metrics, and Substrait bundles

This is how you prove that a rebuild decision was correct and why a run got slower/faster.

### 11.5.1 DataFusion plan objects: logical + physical

DataFusion exposes both:

* `LogicalPlan` with `display`, `display_indent`, `display_graphviz`, `display_indent_schema`, and proto serialize/deserialize (`to_proto`, `from_proto`) ([Apache DataFusion][11])
* `ExecutionPlan` with `display`, `display_indent`, partition count (`partition_count`), and proto serialize/deserialize, with an important caveat: in-memory tables from record batches are not supported for proto conversion. ([Apache DataFusion][11])

**Artifact recommendation per product build**

* logical plan (unoptimized) and optimized logical plan (from DataFrame, see below)
* physical plan (`ExecutionPlan.display_indent()`)
* physical `partition_count` (helps interpret streaming/write parallelism) ([Apache DataFusion][11])

### 11.5.2 DataFusion DataFrame introspection surfaces

DataFusion DataFrames expose:

* `logical_plan()`
* `optimized_logical_plan()`
* `execution_plan()`
* `explain()`
  …and streaming surfaces (`__arrow_c_stream__`, iteration, `execute_stream*`). ([Apache DataFusion][12])

`__arrow_c_stream__` is explicitly streaming-first: “Record batches are produced incrementally, so the full result set is never materialized,” and requested_schema only supports simple projection/reordering. ([Apache DataFusion][12])

### 11.5.3 EXPLAIN / EXPLAIN ANALYZE as “why is this fast/slow”

DataFusion’s docs describe:

* using SQL `EXPLAIN` to see plans (logical and physical)
* `EXPLAIN ANALYZE` to include runtime metrics ([Apache DataFusion][13])

**Artifact tip**: Persist explain output alongside the plan objects, but treat it as “human-facing.” The plan objects are the structured identity.

### 11.5.4 Substrait bundles: portable plan artifacts

The DataFusion Python `datafusion.substrait` module provides:

* `Producer` (logical plan → Substrait plan)
* `Consumer` (Substrait plan → logical plan)
* `Serde` for serialization/deserialization
* `Plan.encode()` to bytes ([Apache DataFusion][14])

Substrait bytes make excellent “repro artifacts”:

* stable, portable plan encoding
* can be used to reproduce planner differences across versions

### 11.5.5 Unparser: plan → SQL for minimal repros

`datafusion.unparser.Unparser.plan_to_sql(logical_plan)` converts a DataFusion logical plan back to SQL, with dialect selection and pretty formatting control. ([Apache DataFusion][15])

This is extremely useful when:

* you need to file an upstream DataFusion issue,
* or you want a minimal SQL repro that corresponds to a particular optimized plan.

---

# 11.6 The incremental rebuild algorithm (product DAG)

At build time, you compute identities and decide what to rebuild.

### 11.6.1 Inputs

* requested product set (e.g., “edges + nodes + props”)
* current source dataset refs and their schema fingerprints
* runtime profile fingerprint
* function registry fingerprint

### 11.6.2 Per-product compile stage

For each product:

1. compile Ibis IR → SQLGlot AST checkpoint
2. run SQLGlot policy engine → canonical AST
3. compute:

   * query_fp (serde dump hash)
   * lineage deps (and required column sets)
4. compute schema_fp for the product’s expected output schema (contract truth)

### 11.6.3 Decide rebuild

A product must rebuild if:

* output path for `(product, build_id)` doesn’t exist (cache miss), or
* any upstream dependency’s build_id changed **and** lineage shows the changed upstream columns are used by this product

This is where lineage gives you “rebuild only what changed” instead of “rebuild everything.”

### 11.6.4 Execute and materialize

If rebuilding:

* execute using your standard lane:

  * DataFusion streaming output (`__arrow_c_stream__`) or Ibis `to_pyarrow_batches`
* materialize via `pyarrow.dataset.write_dataset` with deterministic layout spec (Section 7)
* emit artifacts: canonical AST, diff vs previous version, lineage graphs, DataFusion plans/explain, schema snapshots

---

# 11.7 What to store as the build graph state

To make incremental rebuild durable, persist a small “Build Graph State” table per run:

* product_id
* build_id
* schema_fp, query_fp, runtime_fp, func_fp
* lineage deps:

  * upstream_table → required columns
* artifact pointers:

  * canonical AST payload location
  * plan artifacts locations
  * dataset manifest

This becomes the source of truth for “what exists” and “what depends on what.”

---

If you want the next deep dive: **Section 12 (Observability and debugging across the integrated stack)** is where we turn these artifacts into actionable telemetry: standard log events, “why was this product rebuilt,” “what pushdowns happened,” and “where did memory/time go,” including PyArrow memory pool snapshots and DataFusion operator metrics capture.

[1]: https://sqlglot.com/sqlglot/serde.html?utm_source=chatgpt.com "sqlglot.serde API documentation"
[2]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html?utm_source=chatgpt.com "datafusion.dataframe"
[3]: https://sqlglot.com/sqlglot/optimizer/optimizer.html?utm_source=chatgpt.com "sqlglot.optimizer.optimizer API documentation"
[4]: https://sqlglot.com/sqlglot/optimizer/qualify.html?utm_source=chatgpt.com "sqlglot.optimizer.qualify API documentation"
[5]: https://sqlglot.com/sqlglot/optimizer/pushdown_predicates.html?utm_source=chatgpt.com "sqlglot.optimizer.pushdown_predicates API documentation"
[6]: https://sqlglot.com/sqlglot/optimizer/annotate_types.html?utm_source=chatgpt.com "sqlglot.optimizer.annotate_types API documentation"
[7]: https://sqlglot.com/sqlglot/diff.html?utm_source=chatgpt.com "Semantic Diff for SQL"
[8]: https://sqlglot.com/sqlglot/lineage.html?utm_source=chatgpt.com "sqlglot.lineage API documentation"
[9]: https://sqlglot.com/sqlglot/optimizer/scope.html?utm_source=chatgpt.com "sqlglot.optimizer.scope API documentation"
[10]: https://sqlmesh.readthedocs.io/en/stable/_readthedocs/html/sqlmesh/core/lineage.html?utm_source=chatgpt.com "sqlmesh.core.lineage API documentation - Read the Docs"
[11]: https://datafusion.apache.org/python/autoapi/datafusion/plan/index.html "datafusion.plan — Apache Arrow DataFusion  documentation"
[12]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html "datafusion.dataframe — Apache Arrow DataFusion  documentation"
[13]: https://datafusion.apache.org/user-guide/sql/explain.html?utm_source=chatgpt.com "EXPLAIN — Apache DataFusion documentation"
[14]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html "datafusion.substrait — Apache Arrow DataFusion  documentation"
[15]: https://datafusion.apache.org/python/autoapi/datafusion/unparser/index.html "datafusion.unparser — Apache Arrow DataFusion  documentation"

## 12) Observability and debugging across the integrated stack

The objective is that every build/run can answer—**from artifacts + structured telemetry alone**:

* **Why was this product rebuilt?**
* **What got pushed down and where?**
* **Where did time and memory go?**
* **If output changed, was it a schema change, semantic change, or runtime-policy change?**

This section gives you a single instrumentation model that works across **SQLGlot → Ibis → DataFusion → PyArrow**.

---

# 12.1 Telemetry contract: one event schema, three planes

### 12.1.1 Correlation keys (always present)

At minimum, every log/metric span should carry:

* `run_id`
* `product_id` (or rulepack id)
* `build_id` (composed from schema/query/runtime/function fingerprints; see Section 11)
* `schema_fp`, `query_fp`, `runtime_fp`, `func_fp`
* `upstream_build_ids[]`
* `ordering_tier` (1/2/3)

This makes it possible to answer “why rebuilt” without re-running anything.

### 12.1.2 Event taxonomy (recommended)

**Control plane**

* `run_start`, `run_end`
* `profile_selected`, `profile_hash`
* `schema_contract_loaded`, `function_registry_loaded`

**Compiler plane (SQLGlot/Ibis)**

* `compile_start`, `compile_end`
* `sqlglot_canonicalize_start/end`
* `sqlglot_diff_computed`
* `sqlglot_lineage_computed`

**Data plane (scan/execute/write)**

* `scan_start/end` (PyArrow Dataset scanner)
* `execute_start/end` (DataFusion / Ibis execution)
* `write_start/end` (write_dataset)
* `manifest_written`, `_metadata_written` (optional)

**Artifact plane**

* `plan_captured` (logical/optimized/physical)
* `explain_analyze_captured` (with analyze_level)
* `schema_snapshot_captured`
* `dataset_manifest_captured`

---

# 12.2 “Why was this product rebuilt?” as a first-class artifact

Your rebuild explanation should be **a deterministic function** that emits a structured payload:

### 12.2.1 Reasons you can prove

1. **Cache miss**: `build_id` output directory absent
2. **Schema contract changed**: `schema_fp` changed
3. **Query semantics changed**: `query_fp` changed and/or SQLGlot diff includes non-trivial operations (insert/remove/update/move) ([SQLGlot][1])
4. **Runtime policy changed**: `runtime_fp` changed (DataFusion config keys, spill policy, analyze_level, etc.) ([Apache DataFusion][2])
5. **Function registry changed**: `func_fp` changed (new UDF version / mapping)

### 12.2.2 “Why rebuilt” payload template

```json
{
  "event": "product_rebuild_decision",
  "run_id": "...",
  "product_id": "edges_call",
  "build_id_new": "...",
  "build_id_prev": "...",
  "rebuild": true,
  "reasons": [
    {"kind": "query_fp_changed", "prev": "...", "new": "..."},
    {"kind": "sqlglot_diff", "ops": {"Insert": 2, "Update": 1, "Remove": 0, "Move": 0}}
  ],
  "lineage_impact": {
    "upstream_table": "staging.symbols",
    "columns_used": ["symbol_id", "module_path"]
  }
}
```

SQLGlot’s semantic diff is explicitly an **edit script** of node-level operations (insert/remove/update/etc.) between ASTs. ([SQLGlot][1])

---

# 12.3 DataFusion observability: plans + EXPLAIN ANALYZE metrics (operator-level)

## 12.3.1 Plan capture surfaces (Python)

In DataFusion Python:

* `DataFrame.explain(verbose=False, analyze=False)` prints an explanation of the plan; if `analyze=True`, it runs the plan and includes metrics. ([Apache DataFusion][3])
* `execute_stream_partitioned()` provides per-partition streams (useful for partition-aware sinks). ([Apache DataFusion][3])

### Capture `explain()` output programmatically (stdout redirect)

Because `df.explain(...)` prints, the simplest capture is redirecting stdout:

```python
import io
from contextlib import redirect_stdout

def capture_df_explain(df, *, verbose=False, analyze=False) -> str:
    buf = io.StringIO()
    with redirect_stdout(buf):
        df.explain(verbose=verbose, analyze=analyze)  # prints
    return buf.getvalue()
```

## 12.3.2 EXPLAIN / EXPLAIN ANALYZE syntax (SQL-side)

DataFusion documents:

* `EXPLAIN [ANALYZE] [VERBOSE] [FORMAT format] statement` ([Apache DataFusion][4])

This matters if you keep a “SQL lane” and want to capture EXPLAIN output as rows rather than printed strings.

## 12.3.3 Metrics: what to extract and store

DataFusion has a dedicated Metrics doc; metrics are intended to show “where time is spent and how much data flows through the pipeline,” and are surfaced in `EXPLAIN ANALYZE`. ([Apache DataFusion][5])

Baseline metrics (available for most operators): ([Apache DataFusion][5])

* `elapsed_compute` (CPU time spent processing)
* `output_rows`
* `output_bytes` (note: may be overestimated if batches share buffers)
* `output_batches`

Operator-specific example:

* `FilterExec.selectivity = output_rows / input_rows` ([Apache DataFusion][5])

### 12.3.4 Control verbosity: `datafusion.explain.analyze_level`

DataFusion has a config option:

* `datafusion.explain.analyze_level` with `summary` vs `dev` (“summary shows common metrics… dev provides deep operator-level introspection”), default `dev`. ([Apache DataFusion][2])

DataFusion 51 introduced this option and expanded metrics such as `output_bytes` and filter selectivity. ([Apache DataFusion][6])

**Policy**

* CI/perf baseline: `summary`
* Deep debugging: `dev`
* Always record the chosen analyze level in artifacts (it changes output)

## 12.3.5 Explain-format knobs that make plans reviewer-friendly

From the config catalog:

* `datafusion.explain.format` can be `indent` (default) or `tree` for a tree-rendered format ([Apache DataFusion][2])
* `datafusion.explain.show_sizes` prints partition sizes (default true) ([Apache DataFusion][2])
* `datafusion.explain.show_schema` and `show_statistics` can be enabled for deeper introspection ([Apache DataFusion][2])

## 12.3.6 “Source location spans” for error attribution

If you want to map logical plan nodes back to SQL source locations, DataFusion can collect spans:

* `datafusion.sql_parser.collect_spans` (when true, collects source locations / spans and records them in logical plan nodes). ([Apache DataFusion][2])

This is a powerful debugging aid when agents generate SQL and you need to highlight exactly which part of SQL caused a plan problem.

---

# 12.4 PyArrow observability: memory accounting, scan provenance, and write manifests

## 12.4.1 Memory: pool snapshots (per phase)

Use two complementary views:

**A) Pool-level stats**
`pyarrow.MemoryPool` exposes:

* `backend_name`
* `bytes_allocated()`
* `max_memory()` (peak; may be approximate; may be None depending on pool) ([Apache Arrow][7])

**B) Default pool total**

* `pyarrow.total_allocated_bytes()` returns allocated bytes from the **default** memory pool (other pools may not be accounted for). ([Apache Arrow][8])

**Instrumentation snippet**

```python
import pyarrow as pa

def arrow_mem_snapshot(label: str) -> dict:
    pool = pa.default_memory_pool()
    return {
        "label": label,
        "pool_backend": pool.backend_name,
        "bytes_allocated": pool.bytes_allocated(),
        "max_memory": pool.max_memory(),
        "total_allocated_bytes_default_pool": pa.total_allocated_bytes(),
    }
```

**Allocator logging for deep debugging**

* `pyarrow.log_memory_allocations(enable=True)` enables allocator logging for debugging. ([Apache Arrow][9])

## 12.4.2 Scan provenance: `TaggedRecordBatch`

When you need “what file did these rows come from?”, consume scanners via `scan_batches()` and record fragment metadata.

PyArrow defines `TaggedRecordBatch` as:

* “a combination of a record batch and the fragment it came from.” ([Apache Arrow][10])

This is the foundation for:

* per-file timings
* per-file row counts
* “was partition pruning effective?” (fragments touched)

## 12.4.3 Write manifests: `file_visitor` + per-file Parquet metadata

`pyarrow.dataset.write_dataset` supports `file_visitor(written_file)` and documents:

* `written_file.path`
* `written_file.metadata` is the Parquet metadata (None if not parquet), has file path set, and “can be used to build a _metadata file.” ([Apache Arrow][11])

**Manifest capture pattern**

```python
visited = []

def file_visitor(wf):
    visited.append({
        "path": wf.path,
        "has_parquet_metadata": wf.metadata is not None,
    })
```

You should persist `visited` as the dataset manifest for each product build (and optionally aggregate to `_metadata` only when bounded, as discussed earlier).

---

# 12.5 SQLGlot observability: canonical AST, diffs, and lineage reports

## 12.5.1 Canonical AST snapshots: `sqlglot.serde`

SQLGlot provides:

* `dump(Expression)` → JSON-serializable list
* `load(payloads)` → Expression ([SQLGlot][12])

**Policy**

* Persist the dumped payload as the canonical plan artifact (machine-readable)
* Persist rendered SQL as the human artifact (dialect-specific)

## 12.5.2 Semantic diffs: edit scripts

SQLGlot’s diff docs explain producing an “edit script” consisting of operations like insert/remove/update on individual AST nodes. ([SQLGlot][1])

Store:

* counts by operation type
* the full edit script (for deep debugging)
* any seeded matchings (if you ever add them)

## 12.5.3 Lineage: required columns and scope caching

SQLGlot lineage arguments include:

* `scope` (pre-created scope to use instead)
* `trim_selects` (trim selects to only relevant columns)
* `copy` (copy expression args) ([SQLGlot][13])

This supports an efficient production pattern:

* Build scope once per canonical query
* Compute lineage for each output column using cached scope
* Emit `RequiredColumnSet` as an artifact and use it to drive scan projections (Section 6)

---

# 12.6 Ibis observability: IR graphs and SQL transparency

## 12.6.1 Expression graphs (GraphViz)

Ibis supports visualizing an expression as a directed graph using GraphViz. ([Ibis][14])

This is a high-signal reviewer artifact:

* “what is the rule doing?” without reading SQL
* especially valuable when SQLGlot or DataFusion rewrites restructure SQL

## 12.6.2 SQL transparency of raw SQL segments

Ibis explicitly notes:

* It doesn’t understand the inner structure of SQL passed to `Table.sql()` or `Backend.sql()`, but it still compiles to SQLGlot under the hood, meaning SQLGlot capabilities like lineage remain applicable. ([Ibis][15])

So even in SQL escape hatches, you can still compute lineage/diffs at the SQLGlot layer and keep observability intact.

---

# 12.7 “What pushdowns happened?” — make it visible

You generally cannot rely on a single “pushdown metric” across engines, so you treat pushdown observability as **multi-signal**:

### 12.7.1 SQLGlot “shape evidence”

* Record whether `pushdown_predicates` and `pushdown_projections` ran
* Record diff edit counts between pre/post pushdown passes (SQLGlot diff edit script) ([SQLGlot][1])
* Record required columns derived from lineage (and compare to actual scan columns)

### 12.7.2 DataFusion “runtime evidence”

* Capture `EXPLAIN ANALYZE` at `summary` or `dev`
* Extract baseline metrics (`elapsed_compute`, `output_rows`, `output_bytes`, `output_batches`) and FilterExec selectivity. ([Apache DataFusion][5])

### 12.7.3 PyArrow “scan evidence”

* Fragments touched vs total fragments (if you use `get_fragments(filter=...)`)
* If scanning via `scan_batches`, record per-fragment row counts using `TaggedRecordBatch.fragment` provenance. ([Apache Arrow][10])

---

# 12.8 “Where did memory/time go?” — phase-level budgets

### 12.8.1 Phase timers

For each phase (`compile`, `scan`, `execute`, `write`):

* capture duration
* capture Arrow memory snapshot before/after (pool bytes + peak)
* capture DataFusion operator metrics where applicable

### 12.8.2 DataFusion operator-level timing

Baseline metric `elapsed_compute` gives CPU time spent processing in the operator, and is intended for understanding where time is spent. ([Apache DataFusion][5])

This maps directly to “hot operator identification” in your telemetry.

---

# 12.9 Optional: traces via OpenTelemetry (when you want full flame graphs)

If you compile Arrow C++ with OpenTelemetry enabled, “portions of Arrow C++ are instrumented with the OpenTelemetry C++ SDK,” enabling detailed tracing analyzable in other tools. ([Apache Arrow][16])

This is not required for correctness, but it’s the closest you can get to a “native flame graph” for Arrow compute and Acero internals.

---

# 12.10 Minimal implementation skeleton: one `TelemetrySink`, many hooks

```python
import time
from contextlib import contextmanager

class TelemetrySink:
    def event(self, name: str, **fields): ...
    def artifact(self, name: str, payload): ...

@contextmanager
def phase(sink: TelemetrySink, name: str, **fields):
    start = time.time()
    sink.event(f"{name}_start", **fields)
    try:
        yield
        ok = True
    except Exception as e:
        ok = False
        sink.event(f"{name}_error", error=str(e), **fields)
        raise
    finally:
        sink.event(f"{name}_end", ok=ok, duration_ms=int((time.time() - start) * 1000), **fields)
```

Hook it into:

* SQLGlot canonicalization: store serde payload + diff counts + lineage
* DataFusion execution: capture `df.explain(analyze=True)` output and store analyze_level used ([Apache DataFusion][3])
* PyArrow scan/write: memory snapshots + TaggedRecordBatch provenance + file_visitor manifests ([Apache Arrow][7])

---

If you want the next deep dive: **Section 13 (Remote query/serving lane)** covers using **Arrow Flight** as a streaming transport so your “artifact plane” and “data plane” can be served remotely with low overhead (and you can keep RecordBatchReader as the unit of transfer).

[1]: https://sqlglot.com/sqlglot/diff.html?utm_source=chatgpt.com "Semantic Diff for SQL"
[2]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/dataframe/index.html?utm_source=chatgpt.com "datafusion.dataframe"
[4]: https://datafusion.apache.org/user-guide/sql/explain.html?utm_source=chatgpt.com "EXPLAIN — Apache DataFusion documentation"
[5]: https://datafusion.apache.org/user-guide/metrics.html "Metrics — Apache DataFusion  documentation"
[6]: https://datafusion.apache.org/blog/2025/11/25/datafusion-51.0.0/?utm_source=chatgpt.com "Apache DataFusion 51.0.0 Released"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.MemoryPool.html?utm_source=chatgpt.com "pyarrow.MemoryPool — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.total_allocated_bytes.html?utm_source=chatgpt.com "pyarrow.total_allocated_bytes — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.log_memory_allocations.html?utm_source=chatgpt.com "pyarrow.log_memory_allocations — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.TaggedRecordBatch.html?utm_source=chatgpt.com "pyarrow.dataset.TaggedRecordBatch — Apache Arrow v22.0.0"
[11]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html?utm_source=chatgpt.com "pyarrow.dataset.write_dataset — Apache Arrow v22.0.0"
[12]: https://sqlglot.com/sqlglot/serde.html?utm_source=chatgpt.com "sqlglot.serde API documentation"
[13]: https://sqlglot.com/sqlglot/lineage.html?utm_source=chatgpt.com "sqlglot.lineage API documentation"
[14]: https://ibis-project.org/how-to/visualization/graphs?utm_source=chatgpt.com "graphs – Ibis"
[15]: https://ibis-project.org/posts/does-ibis-understand-sql/?utm_source=chatgpt.com "Does Ibis understand SQL?"
[16]: https://arrow.apache.org/docs/cpp/opentelemetry.html?utm_source=chatgpt.com "OpenTelemetry — Apache Arrow v22.0.0"

## 14) Integration relevance map per library

Use this as a **living checklist**: every item is labeled **Core**, **Optional**, or **Deprioritized** for your current architecture (rules → canonical relational plans → streaming artifacts).

For each item, I’m including:

* what it enables in *this* stack,
* the minimal APIs/surfaces to standardize,
* what to capture as artifacts,
* and a “done when…” acceptance signal.

---

# 14.1 DataFusion (Python)

## Core integration

### ✅ Arrow C Stream export + Arrow ingestion

**Why it’s core**
This is the **zero-copy streaming boundary**: DataFusion can stream results into PyArrow (and anything Arrow-stream capable) without materializing full tables. DataFusion also accepts Arrow stream producers into the engine session. ([Apache DataFusion][1])

**Standardize**

* Export: `DataFrame.__arrow_c_stream__` (consume via `pa.RecordBatchReader.from_stream(df)`) ([Apache DataFusion][1])
* Import: `SessionContext.from_arrow(data)` accepts any object implementing `__arrow_c_stream__` or `__arrow_c_array__` (struct array for `__arrow_c_array__`). ([Apache DataFusion][1])

**Artifacts**

* “producer surface” used (`__arrow_c_stream__` vs collect/table)
* batch counts and sizes (from consumer loop)

**Done when**

* Any DataFusion result can be piped to `pyarrow.dataset.write_dataset` via `RecordBatchReader.from_stream` without intermediate `pa.Table`.

---

### ✅ SessionContext config/runtime + object stores

**Why it’s core**
All execution semantics (parallelism, spill behavior, pruning, caches) depend on session/runtime configuration. DataFusion can read from multiple sources including Parquet/CSV/JSON/AVRO via the Python interface. ([Apache DataFusion][2])

**Standardize**

* Construct `SessionContext` from a pinned `SessionConfig` + `RuntimeEnvBuilder` (your runtime profile object owns this)
* Register object stores (S3/GCS/Azure/HTTP/local) into the session. DataFusion’s data sources guide lists the supported object store classes. ([Apache DataFusion][3])

**Artifacts**

* full profile snapshot (structured knobs + raw key/value overrides)
* object store registrations (scheme + host + type)

**Done when**

* Ibis and direct DataFusion calls share the *same* `SessionContext` instance (single substrate).

---

### ✅ SQLOptions gating for safe SQL surfaces

**Why it’s core**
The default `SQLOptions` values allow DDL, DML, and statements; you need explicit gating for any agent/untrusted SQL lane. ([Apache DataFusion][4])

**Standardize**

* Always use `sql_with_options(...)` (or `sql(..., options=...)`) for untrusted surfaces. ([Apache DataFusion][4])
* Define 2–3 named “SQL surfaces” in your runtime profile (internal, untrusted, admin), each with explicit allow_ddl/allow_dml/allow_statements.

**Artifacts**

* surface name used + SQLOptions values
* full SQL text (or AST) executed + build_id

**Done when**

* Untrusted requests cannot run DDL/DML/SET unless explicitly allowed by surface policy.

---

### ✅ UDF families + PyCapsule export types

**Why it’s core**
Your rules will need “custom kernels” (stable hash, normalization, special matching). DataFusion supports multiple UDF classes (scalar/aggregate/window/table), and Python UDFs operate on Arrow arrays batch-at-a-time. ([Apache DataFusion][5])

**Standardize**

* Prefer built-ins first (“reducing the need for custom Python functions”) but register UDFs when needed. ([Apache DataFusion][5])
* Session-local installation into `SessionContext` (UDF registry snapshot is part of artifacts).
* A “graduation path” from Python UDF → Rust UDF via PyCapsule for performance and named-argument ergonomics (DataFusion’s functions docs include “Named Arguments” and UDF authoring guidance). ([Apache DataFusion][6])

**Artifacts**

* UDF registry snapshot (name, kind, input types, return type, volatility)
* function registry fingerprint in build_id

**Done when**

* Every function used by a rulepack resolves through your unified FunctionSpec registry and is installed deterministically per session.

---

### ✅ Substrait + Unparser for artifacts (repro + minimal SQL)

**Why it’s core**
You need stable plan artifacts for debugging and reproducibility. DataFusion Python exposes Substrait support (`Producer/Consumer/Serde`) and an Unparser module to convert plans back into SQL (with errors if conversion isn’t possible). ([Apache DataFusion][7])

**Standardize**

* Persist Substrait bytes for every product build (repro bundle)
* Persist Unparser SQL output for “minimal repro SQL” when available

**Artifacts**

* substrait plan bytes + schema snapshot
* plan_to_sql output (or error) ([Docs.rs][8])

**Done when**

* Any slow/broken build can be reproduced from: (substrait bytes + runtime profile + input dataset refs).

---

## Optional integration

### ◻ SQL `COPY` / format options (writer-side control)

**When it’s worth it**
If you want engine-native sinks with table-level/statement-level options (compression, partitioning, column-specific parquet options), DataFusion’s SQL supports `COPY ... TO ... STORED AS ... OPTIONS(...)` and documents a comprehensive Format Options system used by `COPY`, `INSERT INTO`, and `CREATE EXTERNAL TABLE`. ([Apache DataFusion][9])

**Standardize**

* Only in controlled surfaces (use SQLOptions gating)
* Prefer Arrow `write_dataset` for your default sink; use `COPY` when you specifically need DF’s SQL-layer formatting behavior

**Artifacts**

* full COPY statement + effective format options (as text)

---

### ◻ Experimental SQL→DataFrame transpilation to other dataframe engines

**When it’s worth it**
Only if you need a fallback or alternative execution lane for dev/prototyping. The `datafusion` PyPI page lists experimental support for transpiling SQL queries to DataFrame calls with Polars, Pandas, and cuDF. ([PyPI][10])

**Standardize**

* Keep disabled by default; enable only in dev tooling profiles

---

## Deprioritized (supported, but not central)

* Global-context convenience APIs (good for notebooks; not for reproducible builds)
* Any integration that bypasses your runtime profile hashing / artifact emission

---

# 14.2 Ibis

## Core integration

### ✅ DataFusion backend bound to your SessionContext + `raw_sql(str | sge.Expression)`

**Why it’s core**
This is the cleanest “one substrate” model: Ibis attaches to the same DataFusion `SessionContext`, and the DataFusion backend `raw_sql` explicitly accepts `str | sge.Expression` (SQLGlot AST). ([Ibis][11])

**Standardize**

* Construct DataFusion `SessionContext` once (profile-owned)
* `con = ibis.datafusion.connect(ctx)` (or equivalent) ([Ibis][11])
* Execute canonical SQLGlot AST directly via `con.raw_sql(sge.Expression)` ([Ibis][11])

**Artifacts**

* “IR mode” used (Ibis expr vs raw SQL)
* canonical SQLGlot AST payload stored alongside build

---

### ✅ Streaming export surfaces: `to_pyarrow_batches`, `to_parquet_dir`

**Why it’s core**
Your writer boundary is `RecordBatchReader`. Ibis is the “IR lane” that can always produce batches and can also write parquet directories by delegating to `pyarrow.dataset.write_dataset`. ([Ibis][12])

**Standardize**

* Use `to_pyarrow_batches()` for uniform “stream → writer” flows
* Treat `chunk_size` as a *hint*; docs explicitly say it may have no effect depending on backend. ([Ibis][13])
* Use `to_parquet_dir(..., **kwargs)` when you’re fine with the backend delegating to Arrow’s dataset writer. ([Ibis][12])

**Artifacts**

* batch counts/sizes observed (do not assume chunk_size honored)

---

### ✅ SQLGlot boundary: `con.compiler.to_sqlglot(expr)` for canonicalization

**Why it’s core**
This is your compile-time checkpoint into the canonical AST layer that enables: qualification, normalization, semantic diffs, lineage.

**Standardize**

* Always materialize a SQLGlot AST artifact before execution (even if you execute via Ibis)

**Artifacts**

* canonical AST (serde dump)
* emitted SQL (for humans)

---

### ✅ Rulepack ergonomics: selectors + params + ordering rule

**Why it’s core**

* Selectors simplify wide-table transformations (critical for CPG-like schemas). ([Ibis][14])
* Scalar params enable reusable query templates. ([Tessl][15])
* Row order is undefined unless `.order_by()`—must be encoded as a determinism invariant. ([Ibis][16])

**Standardize**

* Require `.order_by()` for any materialized product whose order matters (or define canonical sort tier elsewhere)
* Treat selectors/params as “standard DSL vocabulary” for rule authoring

---

## Optional integration

### ◻ SQL ingestion lanes: `Table.sql` / `Backend.sql`

**When it’s worth it**
For onboarding legacy SQL or letting an agent supply SQL fragments. Keep in mind these tend to be “opaque segments” at the Ibis IR level (you still recover analysis using SQLGlot once compiled).

**Standardize**

* Gated, schema-required, and always normalized through your SQLGlot policy engine.

---

## Deprioritized

* Backend-specific interactive/pretty output behavior
* Pure pandas export paths for large products (keep streaming-first)

---

# 14.3 SQLGlot

## Core integration

### ✅ Qualification + identifier normalization baseline

**Why it’s core**

* `qualify` is explicitly “necessary for all further SQLGlot optimizations.” ([SQLGlot][17])
* `normalize_identifiers` is “very important” for standardizing ASTs and mirrors engine identifier resolution semantics; includes a case-sensitive escape hatch comment. ([SQLGlot][18])

**Standardize**

* Always run `qualify` (expand stars, validate columns) before other optimizer passes
* Always run `normalize_identifiers` before fingerprinting

**Artifacts**

* schema mapping used for qualification
* dialect read/write (must be pinned)
* normalized AST payload

---

### ✅ Serde + diff for incremental rebuild identity

**Why it’s core**

* `sqlglot.serde.dump/load` gives stable, JSON-serializable structural identity. ([SQLGlot][19])
* Diff yields an “edit script” of operations (insert/remove/update/etc.) for semantic change detection. ([SQLGlot][20])

**Standardize**

* Fingerprint canonical AST via serde dump hash
* Use diff on canonical ASTs to drive invalidation rules

**Artifacts**

* serde payload
* diff edit script counts + full script for deep debug

---

### ✅ Transforms for dialect compatibility: QUALIFY and UNNEST/EXPLODE

**Why it’s core**
Your engine truth (DataFusion) may not accept upstream dialect constructs. SQLGlot transforms give explicit bridges:

* `eliminate_qualify`, `unnest_to_explode`, and helpers like `unqualify_unnest` are documented in `sqlglot.transforms`. ([SQLGlot][21])

**Standardize**

* Keep an internal canonical form (generally UNNEST)
* Apply dialect-specific preprocess transform chains at generation time (Spark/Hive dialects show preprocess pipelines using eliminate_qualify and unnest_to_explode). ([SQLGlot][22])

---

### ✅ Lineage + scope for contracts and required-column extraction

**Why it’s core**
`lineage(...)` explicitly supports:

* pre-created `scope`
* `trim_selects` to keep only relevant columns
* `**kwargs` to pass qualification parameters ([SQLGlot][23])

This is the lever that translates compiler analysis into scan projection policy.

**Standardize**

* Build scope once per canonical query; reuse for lineage over many columns
* Persist “required input columns” artifacts per product

---

## Optional integration

### ◻ SQLGlot executor for semantic canary tests

**When it’s worth it**
Use in CI to validate “rewrite passes preserve semantics” on small fixtures. SQLGlot documents its Python execution engine and executor modules. ([SQLGlot][24])

**Standardize**

* Only for tiny datasets; not a performance tool.

---

## Deprioritized

* Pure formatting-only usage (you’re using SQLGlot as IR/canonicalizer, not just formatter)
* Custom dialect authoring unless DataFusion quirks force it (keep transform lane first)

---

# 14.4 PyArrow

## Core integration

### ✅ Dataset/Scanner + `write_dataset` streaming sinks

**Why it’s core**
This is your universal scan/materialization layer. `write_dataset` is explicit about threading vs ordering: using multiple threads may change row order unless `preserve_order=True`. It also supports format-specific `file_options` created by `FileFormat.make_write_options()`. ([Apache Arrow][25])

**Standardize**

* All products written via `write_dataset(reader, ...)` with a versioned WriteSpec
* Only materialize to `pa.Table` for explicit boundaries (canonical sorts, debugging)

**Artifacts**

* writer knobs (use_threads, preserve_order, basenames, existing_data_behavior)
* file_visitor manifest rows (paths + metadata presence)

---

### ✅ PyCapsule / C Stream protocols for interop correctness

**Why it’s core**
It’s the shared ABI between DF↔PyArrow↔Polars/pandas in a streaming-first design, and it’s the root cause of segfaults if mishandled. (You treat “one-shot consumption” as a contract.)

**Standardize**

* All internal interchange uses Arrow streams (`RecordBatchReader`) whenever possible

---

### ✅ Acero declarations for Arrow-native streaming compute

**Why it’s core**
Acero gives you a streaming compute lane when you want to stay Arrow-native without the engine (useful for kernel-heavy transforms and scan+filter+project pipelines).

**Standardize**

* ExecPlan definition via `pyarrow.acero.Declaration` and `ScanNodeOptions` for dataset scans. ([Apache Arrow][26])
* Use ordering controls when needed:

  * `require_sequenced_output` and `implicit_ordering` semantics are described in the dataset scanner/execution docs (“stable ordering for simple ExecPlans”). ([Apache Arrow][27])

---

### ✅ Compute kernels + stable sort indices + nested explode kernels

**Why it’s core**
This is your “never write Python loops” toolkit:

* `list_parent_indices` (parent alignment) ([Apache Arrow][28])
* `list_flatten` (null list emits nothing) ([Apache Arrow][29])
* `take` (null indices emit null) ([Apache Arrow][30])
* `sort_indices` defines a **stable sort** of arrays/record batches/tables ([Apache Arrow][31])

These enable deterministic explode + parent alignment and canonical sorting.

---

### ✅ Compute UDF registration for plan-lane extensions

**Why it’s core**
When you need custom scalar kernels usable in expression plans, PyArrow supports registering scalar UDFs, and it explicitly states scalar functions are the only UDFs allowed in query engine expressions. ([Apache Arrow][32])

**Standardize**

* `pyarrow.compute.register_scalar_function` with named args and stable names; treat this as process-global install.
* Use `UdfContext.memory_pool` when calling Arrow APIs that accept `memory_pool=`. ([Apache Arrow][33])

---

### ✅ Parquet scan options for high-latency storage: `pre_buffer`

**Why it’s core**
`ParquetFragmentScanOptions.pre_buffer` is explicitly described as improving performance on high-latency filesystems like S3/GCS by coalescing reads via a background I/O thread pool, trading memory for speed. ([Apache Arrow][34])

**Standardize**

* Enable `pre_buffer=True` in remote profiles (unless memory-limited)
* Couple to Arrow IO thread pool sizing (runtime profile owned)

---

## Optional integration

### ◻ Flight for remote streaming service surfaces

**When it’s worth it**
If you want a remote query service that still streams RecordBatches with minimal Python overhead.

* `FlightServerBase` starts serving immediately on instantiation; you override methods to define the service. ([Apache Arrow][35])
* `RecordBatchStream` is handled in C++ for the remainder of DoGet “without having to acquire the GIL.” ([Apache Arrow][36])

**Standardize**

* Serve product datasets and artifact tables as Flight streams (RecordBatchReader → RecordBatchStream).

---

### ◻ Substrait execution in Arrow (experimental interchange lane)

**When it’s worth it**
For portability experiments and “plan artifact replay” without DataFusion.

* `pyarrow.substrait.run_query(plan, ...)` executes a Substrait plan and returns a `RecordBatchReader`. ([Apache Arrow][37])
* `get_supported_functions()` enumerates function ids supported by the underlying engine. ([Apache Arrow][38])

---

## Deprioritized

* Full `_metadata` sidecar generation for huge datasets (often scales poorly with row_groups × columns; use bounded-only)
* Materialize-heavy convenience APIs (`Dataset.to_table`) except in debug tooling

---

[1]: https://datafusion.apache.org/python/user-guide/io/arrow.html?utm_source=chatgpt.com "Apache Arrow DataFusion documentation"
[2]: https://datafusion.apache.org/python/?utm_source=chatgpt.com "DataFusion in Python"
[3]: https://datafusion.apache.org/python/user-guide/data-sources.html?utm_source=chatgpt.com "Data Sources — Apache Arrow DataFusion documentation"
[4]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html?utm_source=chatgpt.com "datafusion.context — Apache Arrow DataFusion documentation"
[5]: https://datafusion.apache.org/python/user-guide/common-operations/udf-and-udfa.html?utm_source=chatgpt.com "User-Defined Functions - Apache DataFusion"
[6]: https://datafusion.apache.org/library-user-guide/functions/index.html?utm_source=chatgpt.com "Functions — Apache DataFusion documentation"
[7]: https://datafusion.apache.org/python/autoapi/datafusion/substrait/index.html?utm_source=chatgpt.com "datafusion.substrait"
[8]: https://docs.rs/datafusion/latest/datafusion/sql/unparser/fn.plan_to_sql.html?utm_source=chatgpt.com "plan_to_sql in datafusion::sql::unparser - Rust"
[9]: https://datafusion.apache.org/user-guide/sql/dml.html?utm_source=chatgpt.com "DML — Apache DataFusion documentation"
[10]: https://pypi.org/project/datafusion/25.0.0/?utm_source=chatgpt.com "DataFusion in Python"
[11]: https://ibis-project.org/backends/datafusion?utm_source=chatgpt.com "DataFusion - Ibis"
[12]: https://ibis-project.org/backends/postgresql?utm_source=chatgpt.com "PostgreSQL - Ibis"
[13]: https://ibis-project.org/backends/oracle?utm_source=chatgpt.com "oracle – Ibis"
[14]: https://ibis-project.org/reference/selectors?utm_source=chatgpt.com "selectors - Ibis"
[15]: https://tessl.io/registry/tessl/pypi-ibis-framework/10.8.0/files/docs/expressions.md?utm_source=chatgpt.com "tessl/pypi-ibis-framework@10.8.x - Registry"
[16]: https://ibis-project.org/reference/expression-tables?utm_source=chatgpt.com "Table expressions - Ibis"
[17]: https://sqlglot.com/sqlglot/optimizer/qualify.html?utm_source=chatgpt.com "sqlglot.optimizer.qualify API documentation"
[18]: https://sqlglot.com/sqlglot/optimizer/normalize_identifiers.html?utm_source=chatgpt.com "sqlglot.optimizer.normalize_identifiers API documentation"
[19]: https://sqlglot.com/sqlglot/serde.html?utm_source=chatgpt.com "sqlglot.serde API documentation"
[20]: https://sqlglot.com/sqlglot/diff.html?utm_source=chatgpt.com "Semantic Diff for SQL"
[21]: https://sqlglot.com/sqlglot/transforms.html?utm_source=chatgpt.com "sqlglot.transforms API documentation"
[22]: https://sqlglot.com/sqlglot/dialects/spark2.html?utm_source=chatgpt.com "sqlglot.dialects.spark2 API documentation"
[23]: https://sqlglot.com/sqlglot/lineage.html?utm_source=chatgpt.com "sqlglot.lineage API documentation"
[24]: https://sqlglot.com/sqlglot/executor.html?utm_source=chatgpt.com "Writing a Python SQL engine from scratch"
[25]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html?utm_source=chatgpt.com "pyarrow.dataset.write_dataset — Apache Arrow v22.0.0"
[26]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.Declaration.html?utm_source=chatgpt.com "pyarrow.acero.Declaration — Apache Arrow v22.0.0"
[27]: https://arrow.apache.org/docs/cpp/api/dataset.html?utm_source=chatgpt.com "Dataset — Apache Arrow v22.0.0"
[28]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_parent_indices.html?utm_source=chatgpt.com "pyarrow.compute.list_parent_indices — Apache Arrow v22.0.0"
[29]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.list_flatten.html?utm_source=chatgpt.com "pyarrow.compute.list_flatten — Apache Arrow v22.0.0"
[30]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.take.html?utm_source=chatgpt.com "pyarrow.compute.take — Apache Arrow v22.0.0"
[31]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.sort_indices.html?utm_source=chatgpt.com "pyarrow.compute.sort_indices — Apache Arrow v22.0.0"
[32]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.register_scalar_function.html?utm_source=chatgpt.com "pyarrow.compute.register_scalar_function - Apache Arrow"
[33]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.UdfContext.html?utm_source=chatgpt.com "pyarrow.compute.UdfContext — Apache Arrow v22.0.0"
[34]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetFragmentScanOptions.html?utm_source=chatgpt.com "pyarrow.dataset.ParquetFragmentScanOptions - Apache Arrow"
[35]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightServerBase.html?utm_source=chatgpt.com "pyarrow.flight.FlightServerBase — Apache Arrow v22.0.0"
[36]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.RecordBatchStream.html?utm_source=chatgpt.com "pyarrow.flight.RecordBatchStream — Apache Arrow v22.0.0"
[37]: https://arrow.apache.org/docs/python/api/substrait.html?utm_source=chatgpt.com "Substrait — Apache Arrow v22.0.0"
[38]: https://arrow.apache.org/docs/python/generated/pyarrow.substrait.get_supported_functions.html?utm_source=chatgpt.com "pyarrow.substrait.get_supported_functions - Apache Arrow"
