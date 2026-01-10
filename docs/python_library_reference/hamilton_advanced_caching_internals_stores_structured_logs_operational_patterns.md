
# Caching internals, stores, structured logs, and operational patterns #

# 3) Caching internals + stores + structured logs + operational patterns

## 3.0 Core architecture (what Hamilton caching actually *is*)

Hamilton caching is a **Graph Adapter** (`HamiltonCacheAdapter`) inserted by `Builder.with_cache()` that decides **per node** whether to:

1. compute vs retrieve,
2. version code + data,
3. store/retrieve results. ([Hamilton][1]) ([Hamilton][2])

It is composed of three parts:

* **Cache adapter**: decision + orchestration
* **Metadata store**: `cache_key -> data_version` + run metadata
* **Result store**: `data_version -> result` ([Hamilton][1])

The “high-performance” trick: downstream nodes can often propagate **small `data_version` strings** instead of moving full results, until a node must execute. ([Hamilton][1])

---

## 3.1 Cache key, code version, and the “correctness boundary”

### 3.1.1 `cache_key` composition

A `cache_key` is structurally:

* `node_name`
* `code_version` (hash of the node’s *source code*)
* `dependencies_data_versions` (each dependency’s `data_version`) ([Hamilton][1])

Hamilton notes that by traversing dependency data versions in cache keys you can reconstruct the dataflow structure. ([Hamilton][1])

**Stability warning:** cache keys can be unstable across Python/Hamilton versions; upgrades may require a new empty cache for reliable behavior. ([Hamilton][3])

### 3.1.2 `code_version` pitfalls (major)

Hamilton hashes a node’s **source code** and ignores docstrings + comments; it **does not version nested function calls**. ([Hamilton][1])

So if you change a helper function used inside a node, the node’s `code_version` may not change and you can get a “false hit.” ([Hamilton][1])
Operationally, Hamilton recommends forcing recompute (or deleting metadata/results) in these situations. ([Hamilton][1])

**Design implication:** anything you treat as part of the “semantic contract” should either:

* be expressed as a **node dependency** (so it enters `dependencies_data_versions`), or
* be guarded by **RECOMPUTE** and/or explicit cache invalidation when helper code changes.

---

## 3.2 Caching behaviors (DEFAULT / RECOMPUTE / DISABLE / IGNORE)

Hamilton defines four behaviors and explains the intended semantics and use cases: ([Hamilton][1])

* **DEFAULT**: try cache, else execute; store result+metadata. ([Hamilton][1])
* **RECOMPUTE**: always execute; store result+metadata. Useful for stochastic nodes or external reads. ([Hamilton][1])
* **DISABLE**: act as if caching isn’t enabled for that node; downstream will typically be forced to re-execute due to missing metadata. ([Hamilton][1])
* **IGNORE**: like disable for the node, but downstream nodes **ignore** it as a dependency for lookup—useful for clients/credentials/connections that shouldn’t affect cache keys. ([Hamilton][1]) ([Hamilton][2])

### 3.2.1 Precedence rules

Behavior can be set:

* node-level via `@cache(...)`
* builder-level via `.with_cache(...)`

Builder-level settings override `@cache` because they’re closer to execution. ([Hamilton][1])

### 3.2.2 “Opt-in caching” is first-class

By default caching is “opt-out” (everything cached). To make it “opt-in,” set `default_behavior="disable"` and then select which nodes get DEFAULT. ([Hamilton][1])

```python
from hamilton import driver
import my_flow

dr = (
    driver.Builder()
    .with_modules(my_flow)
    .with_cache(
        default=["expensive_df", "semantic_view_df"],
        default_behavior="disable",
        log_to_file=True,  # JSONL logs under the metadata_store directory :contentReference[oaicite:18]{index=18}
    )
    .build()
)
```

---

## 3.3 The `@cache` decorator (behavior + format + multi-node targeting)

Hamilton exposes caching controls as a decorator: `hamilton.function_modifiers.metadata.cache` with:

* `behavior`: `'default'|'recompute'|'ignore'|'disable'`
* `format`: `'json'|'file'|'pickle'|'parquet'|'csv'|'feather'|'orc'|'excel'|str`
* `target_`: select which produced nodes to decorate when modifiers expand into multiple nodes. ([Hamilton][2])

**Critical note:** this feature is implemented via tags today, but “could change,” and you should not rely on these tags for other purposes. ([Hamilton][2])

### 3.3.1 Format control (non-pickle caching)

By default results are stored as pickle, but you can specify per-node formats via `@cache(format=...)`. ([Hamilton][1])
The tutorial lists built-in formats: `json, parquet, csv, excel, file, feather, orc`, and notes this uses `DataLoader`/`DataSaver` under the hood. ([Hamilton][4]) ([Hamilton][1])

```python
import pandas as pd
from hamilton.function_modifiers import cache

@cache(format="parquet")
def expensive_df(...) -> pd.DataFrame:
    ...

@cache(format="json")
def small_stats(expensive_df: pd.DataFrame) -> dict:
    ...
```

### 3.3.2 Multi-output targeting (`target_`)

If a function modifier expands nodes, `target_` lets you cache only a subset (example: cache only “performance” output). ([Hamilton][2])

```python
from hamilton.function_modifiers import cache, extract_fields

@cache(format="json", target_="performance")
@extract_fields(trained_model=object, performance=dict)
def model_training(...) -> dict:
    ...
```

---

## 3.4 Stores: interfaces, built-ins, and how to extend

### 3.4.1 Store interfaces (the real “plug boundary”)

**MetadataStore** must support: `get/set/exists/delete/delete_all`, run lifecycle (`initialize`), and run querying (`get_run`, `get_run_ids`, `get_last_run`, `last_run_id`, `size`). It should return per-run metadata including `cache_key` and `data_version` so you can manually query stores; decoding `cache_key` yields `node_name`, `code_version`, and dependency versions. ([Hamilton][5]) ([Hamilton][5])

**ResultStore** must support: `get/set/exists/delete/delete_all` mapping `data_version -> result`. ([Hamilton][5])

There’s also `search_data_adapter_registry(name, type_) -> (DataSaver, DataLoader)` to locate loader/saver pairs registered for a format + type—this is part of how “format caching” works. ([Hamilton][5])

### 3.4.2 Built-in stores (what you can use immediately)

* **SQLiteMetadataStore** (persistent metadata): includes `get_run_ids()` and returns run node metadata including `cache_key`, `data_version`, `node_name`, `code_version`. ([Hamilton][5])
* **FileResultStore** (persistent results): `set(data_version, result, saver_cls=None, loader_cls=None)` enables storing with an explicit materializer pair. ([Hamilton][5])
* **InMemoryMetadataStore / InMemoryResultStore**: dev/notebook mode; can `load_from(...)` persisted stores and `persist_to(...)` other stores (note: result stores don’t have a key registry; loading requires metadata or explicit `data_versions`). ([Hamilton][5]) ([Hamilton][5])

### 3.4.3 Store wiring patterns

**Single path (default):** `.with_cache()` stores both metadata and results under `./.hamilton_cache`. ([Hamilton][1])

**Split paths:** pass store instances; then `path=` is ignored. ([Hamilton][1])

```python
from hamilton import driver
from hamilton.caching.stores.sqlite import SQLiteMetadataStore
from hamilton.caching.stores.file import FileResultStore
import my_flow

metadata = SQLiteMetadataStore(path="/var/lib/myapp/cache_meta")
results = FileResultStore(path="/var/lib/myapp/cache_results")

dr = (
    driver.Builder()
    .with_modules(my_flow)
    .with_cache(metadata_store=metadata, result_store=results, log_to_file=True)
    .build()
)
```

**In-memory dev session with persistence:**

```python
from hamilton import driver
from hamilton.caching.stores.memory import InMemoryMetadataStore, InMemoryResultStore
from hamilton.caching.stores.sqlite import SQLiteMetadataStore
from hamilton.caching.stores.file import FileResultStore
import my_flow

dr = (
    driver.Builder()
    .with_modules(my_flow)
    .with_cache(metadata_store=InMemoryMetadataStore(),
                result_store=InMemoryResultStore())
    .build()
)

dr.execute([...])

dr.cache.metadata_store.persist_to(SQLiteMetadataStore(path="./.hamilton_cache"))
dr.cache.result_store.persist_to(FileResultStore(path="./.hamilton_cache"))
```

These persist/load flows are explicitly documented. ([Hamilton][1])

### 3.4.4 Storage reclamation

The tutorial recommends:

* delete the entire cache directory, **or**
* call `delete_all()` on both stores (keep them in sync if you delete results). ([Hamilton][6])

```python
dr.cache.metadata_store.delete_all()
dr.cache.result_store.delete_all()
```

---

## 3.5 Observability: runtime logs, structured logs, visual runs

### 3.5.1 Live logging (`logging` module)

Hamilton emits cache events via the logger `"hamilton.caching"`; `INFO` shows key events (GET_RESULT / EXECUTE_NODE), `DEBUG` is noisier. ([Hamilton][7])

The log line shape is:
`{node_name}::{task_id}::{actor}::{event_type}::{message}` (empty parts omitted). ([Hamilton][1])

```python
import logging

logger = logging.getLogger("hamilton.caching")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())
```

### 3.5.2 Structured logs (in-memory + JSONL)

* Programmatic: `dr.cache.logs(level="info"|"debug", run_id=...)` ([Hamilton][1])
* File-backed: `with_cache(log_to_file=True)` appends JSONL “as they happen” and is ideal for production; the Builder docs specify JSONL is stored under the metadata store directory. ([Hamilton][1]) ([Hamilton][8])

Return shapes:

* no `run_id`: `{run_id: List[CachingEvent]}`
* with `run_id`: `{node_name: List[CachingEvent]}`
* with `Parallelizable/Collect`: branch nodes include `task_id` dimension `{node_name: {task_id: List[CachingEvent]}}`. ([Hamilton][1])

### 3.5.3 Run visualization

`dr.cache.view_run(run_id=..., output_file_path=...)` highlights cache hits, but it does not currently support task-based execution / Parallelizable/Collect. ([Hamilton][1])

---

## 3.6 Introspection APIs you should actually build into your system tooling

The cache adapter exposes user-facing methods to make caching “inspectable”:

* `run_ids`, `last_run_id` (note: last started, not necessarily last completed) ([Hamilton][2])
* `get_cache_key(run_id, node_name, task_id=None)` ([Hamilton][2])
* `get_data_version(run_id, node_name, ...)` checks in-memory and metadata store ([Hamilton][2])
* `resolve_behaviors(run_id)` shows final per-node behavior resolution; Builder settings override `@cache`; also note that for `@expand` (Parallelizable), behavior is set to RECOMPUTE to version yielded items individually. ([Hamilton][2])
* `resolve_code_versions(run_id, final_vars=..., inputs=..., overrides=...)` ([Hamilton][2])
* `version_code(node_name)` and `version_data(result)` (manual diagnostics) ([Hamilton][2])

### Debug move: decode cache keys

The caching tutorial shows decoding cache keys using `decode_key()` for introspection/debugging. ([Hamilton][6])

---

## 3.7 Data versioning: hashing strategy, recursion depth, and type extensions

### 3.7.1 Default hashing semantics

Hamilton’s data versioning is implemented as singledispatch hashing over Python objects; standard library types are supported, and “abstract type” shims allow hashing pandas/polars/numpy without importing them in the core hashing module. ([Hamilton][9])

Important operational note: recursive container hashing must pass `depth` to avoid recursion errors; there’s a configurable max depth. ([Hamilton][9])

### 3.7.2 Control recursion depth

Hamilton shows setting max depth via `fingerprinting.set_max_depth(depth=...)`. ([Hamilton][1])

```python
from hamilton.io import fingerprinting

fingerprinting.set_max_depth(depth=3)
```

### 3.7.3 Register hashing for your own types

Hamilton documents registering a `hash_value` implementation per type (example for polars DataFrame) and also overriding the base case by registering for `object`. ([Hamilton][1])

```python
from __future__ import annotations
from dataclasses import dataclass
from hamilton.io import fingerprinting

@dataclass(frozen=True)
class GraphDigest:
    # keep this small + deterministic
    node_count: int
    edge_count: int
    sha256_edges: str

@fingerprinting.hash_value.register(GraphDigest)
def hash_graph_digest(obj: GraphDigest, *args, **kwargs) -> str:
    # rely on primitive hashing for stable behavior
    return fingerprinting.hash_value({"n": obj.node_count, "e": obj.edge_count, "h": obj.sha256_edges})
```

### 3.7.4 Known edge-case: unhashable values become non-deterministic

Hamilton documents that if `fingerprinting.hash_value()` hits its base unhashable case, it returns `UNHASHABLE_VALUE`, and the adapter appends a random UUID to prevent collisions—this makes `data_version` non-deterministic for that value. ([Hamilton][2])
In practice: ensure your “semantic” nodes return hash-friendly outputs, or add custom hashing.

---

## 3.8 Operational patterns (what “best in class” typically looks like)

### 3.8.1 Production default: opt-in caching + JSONL audit trail

* `default_behavior="disable"` to avoid caching everything by accident ([Hamilton][1])
* explicitly list which nodes are cached (`default=[...]`)
* set `log_to_file=True` to emit JSONL under the metadata store directory ([Hamilton][8])

### 3.8.2 External reads/writes and materializers

For external data, force recompute on loaders (or all loaders via `default_loader_behavior="recompute"`). The caching tutorial demonstrates controlling loader/saver behaviors via builder parameters. ([Hamilton][6])

```python
dr = (
  driver.Builder()
  .with_modules(my_flow)
  .with_cache(
      default_loader_behavior="recompute",
      default_saver_behavior="disable",
  )
  .build()
)
```

### 3.8.3 Ignore ephemeral nodes (clients, creds, connections)

Use `IGNORE` so downstream cache keys don’t get poisoned by irrelevant dependencies. ([Hamilton][1]) ([Hamilton][2])

### 3.8.4 Dynamic execution (`Parallelizable/Collect`) and caching

Caching has explicit special-case handling for Collect/Expand in task-based execution because collect order can be inconsistent and branch nodes may lose access to outside data versions in some executors (multiprocessing/multithreading). ([Hamilton][2])
Also, `resolve_behaviors` sets `@expand` nodes to RECOMPUTE to ensure yielded items are versioned individually. ([Hamilton][2])

### 3.8.5 Adapter compatibility footgun

Caching uses the `do_node_execute()` hook, and Hamilton notes it is currently incompatible with other adapters that also leverage it (e.g., `PDBDebugger`, `GracefulErrorAdapter`, etc.). ([Hamilton][2])
Operationally: treat caching as an “execution profile” (dev vs prod), and avoid stacking conflicting do-node adapters.

---

# 3.9 Holistic example: “CodeIntel-style cache profile”

This combines: opt-in caching, per-node formats, IGNORE clients, RECOMPUTE external reads, custom stores, JSONL logs, and “retrieve-by-data_version”.

```python
from __future__ import annotations

import logging
from hamilton import driver
from hamilton.function_modifiers import cache
from hamilton.caching.stores.sqlite import SQLiteMetadataStore
from hamilton.caching.stores.file import FileResultStore
from hamilton.caching.adapter import CachingEventType

# -------------------------
# Nodes (my_flow.py)
# -------------------------

@cache(behavior="ignore")  # ignore creds/clients in downstream cache keys
def duckdb_conn_str() -> str:
    return "duckdb:///var/lib/codeintel/warehouse.duckdb"

@cache(behavior="recompute")  # always read latest state, but still version/store the result
def repo_snapshot(duckdb_conn_str: str) -> dict:
    # e.g., list of files, commit, etc.
    ...

@cache(format="parquet")  # large tabular output -> parquet-backed caching
def function_facts(repo_snapshot: dict):
    ...

@cache(format="parquet")
def call_graph_edges(function_facts):
    ...

@cache(format="parquet")
def semantic_function_risk_v1(function_facts, call_graph_edges):
    ...

# -------------------------
# Driver assembly
# -------------------------

logger = logging.getLogger("hamilton.caching")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

metadata = SQLiteMetadataStore(path="/var/lib/codeintel/cache_meta")
results = FileResultStore(path="/var/lib/codeintel/cache_results")

dr = (
    driver.Builder()
    .with_modules(__import__("my_flow"))
    .with_cache(
        metadata_store=metadata,
        result_store=results,
        default_behavior="disable",              # opt-in caching :contentReference[oaicite:64]{index=64}
        default=["function_facts", "call_graph_edges", "semantic_function_risk_v1"],
        default_loader_behavior="recompute",     # if you use DataLoader nodes :contentReference[oaicite:65]{index=65}
        log_to_file=True,                        # JSONL under metadata_store dir :contentReference[oaicite:66]{index=66}
    )
    .build()
)

# Execute
out = dr.execute(["semantic_function_risk_v1"])

# Retrieve a cached artifact by data_version (latest run)
dv = dr.cache.data_versions["semantic_function_risk_v1"]  # points to latest run :contentReference[oaicite:67]{index=67}
cached_value = dr.cache.result_store.get(dv)

# Or: find the data_version for a specific historical run via structured logs
run_id = dr.cache.last_run_id
for ev in dr.cache.logs(level="debug")[run_id]:
    if ev.event_type == CachingEventType.SET_RESULT and ev.node_name == "semantic_function_risk_v1":
        dv2 = ev.value
        break
cached_value_2 = dr.cache.result_store.get(dv2)
```

If you want next, I can turn this into a **recommended “cache policy matrix”** for your codebase (node categories → behavior/format/store), plus a compact **JSONL log ingester** that converts caching events into your own DuckDB `pipeline_runs/pipeline_steps` tables for unified observability.

[1]: https://hamilton.dagworks.io/en/latest/concepts/caching/ "Caching - Hamilton"
[2]: https://hamilton.dagworks.io/en/latest/reference/caching/caching-logic/ "Caching logic - Hamilton"
[3]: https://hamilton.dagworks.io/en/latest/concepts/caching/?utm_source=chatgpt.com "Caching - Hamilton"
[4]: https://hamilton.apache.org/how-tos/caching-tutorial/?utm_source=chatgpt.com "Caching - Hamilton"
[5]: https://hamilton.dagworks.io/en/latest/reference/caching/stores/ "Stores - Hamilton"
[6]: https://hamilton.dagworks.io/en/latest/how-tos/caching-tutorial/?utm_source=chatgpt.com "Caching - Hamilton"
[7]: https://hamilton.apache.org/concepts/caching/?utm_source=chatgpt.com "Caching - Hamilton"
[8]: https://hamilton.apache.org/reference/drivers/Driver/?utm_source=chatgpt.com "Builder - Hamilton"
[9]: https://hamilton.dagworks.io/en/latest/reference/caching/data-versioning/?utm_source=chatgpt.com "Data versioning - Hamilton"


# Cache policy matrix and JSONL log ingester #

Below are the two deliverables you asked for:

1. a **recommended cache policy matrix** (node categories → behavior/format/store), and
2. a **compact JSONL cache-log ingester** that lands Hamilton cache events into **DuckDB `pipeline_runs` / `pipeline_steps`** for unified observability.

I’m assuming you enable cache structured logs via `Builder.with_cache(log_to_file=True)` (Hamilton appends cache events “as they happen in JSONL format” ([Hamilton][1]); the events are `CachingEvent(run_id, actor, event_type, node_name, task_id?, msg?, value?, timestamp)` ([Hamilton][1])). Hamilton’s docs also state the default cache location is `./.hamilton_cache` when caching is enabled. ([Hamilton][2])

---

## A) Recommended cache policy matrix

### A.1 Baseline “best in class” cache profile (builder defaults)

**Production-default stance:** opt-in caching + structured logs + deterministic cache path.

```python
from hamilton import driver

dr = (
  driver.Builder()
  .with_modules(...)
  .with_cache(
      path="/var/lib/codeintel/hamilton_cache",   # don’t leave it in CWD in services
      default_behavior="disable",                 # opt-in caching only
      default_loader_behavior="recompute",        # external reads: assume “fresh”
      default_saver_behavior="disable",           # side effects: don’t treat as cacheable
      log_to_file=True,                           # JSONL event stream for observability :contentReference[oaicite:3]{index=3}
  )
  .build()
)
```

Then explicitly opt nodes into caching via **either**:

* `@cache(behavior="default", format="parquet/json/file/pickle")`, or
* `.with_cache(default=[...])` (hard-coded list; good for small graphs, less good at scale).

### A.2 The matrix

| Node category                                       | Typical CodeIntel examples                                                   |                                 Behavior |                            Format | Result store tier                                    | Why / notes                                                                                                                                                                                                   |
| --------------------------------------------------- | ---------------------------------------------------------------------------- | ---------------------------------------: | --------------------------------: | ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Ephemeral runtime objects**                       | `duckdb_connection`, `http_client`, `token_provider`, `logger`, thread pools |                               **IGNORE** |                                 — | none                                                 | Prevents cache keys being polluted by non-semantic “plumbing”; IGNORE means downstream lookups ignore it (clients/connections are explicitly cited as a use case). ([Hamilton][1])                            |
| **True external reads (no explicit version input)** | read from live DB, API calls, filesystem scans without commit/hash           |                            **RECOMPUTE** |                      parquet/json | file store                                           | Forces re-execution each run while still versioning/storing results for audit/replay.                                                                                                                         |
| **External reads with explicit version input**      | `repo_commit_sha` input drives “snapshot read”, `warehouse_snapshot_id`      |                              **DEFAULT** |                      parquet/json | file store                                           | Prefer this over RECOMPUTE: make “what changed” an input, then caching becomes correct + fast.                                                                                                                |
| **Large tabular “facts”**                           | `function_facts`, `call_graph_edges`, `symbols`, `imports`, `docstrings`     |                              **DEFAULT** |                       **parquet** | file store                                           | Big fan-out + expensive to recompute; parquet makes results portable and debuggable.                                                                                                                          |
| **Derived metrics tables**                          | centrality tables, clustering assignments, risk-factor tables                | **DEFAULT** (or RECOMPUTE if stochastic) |                           parquet | file store                                           | If algorithm can be seeded/deterministic, treat as DEFAULT; otherwise RECOMPUTE.                                                                                                                              |
| **Small dictionaries / manifests**                  | `semantic_registry`, `dataset_inventory`, counts/stats                       |                              **DEFAULT** |                          **json** | file store                                           | Cheap but very valuable for CI diffs and “what did we run?” artifacts.                                                                                                                                        |
| **Binary artifacts**                                | embeddings index, faiss index, compiled graph blobs                          |                              **DEFAULT** |                          **file** | file store (or object store)                         | “file” lets you manage your own serialization; good for non-tabular artifacts.                                                                                                                                |
| **Non-deterministic nodes**                         | sampling, randomized clustering w/o fixed seed, model training               |                            **RECOMPUTE** | file/pickle (or json for metrics) | file store                                           | Docs explicitly position RECOMPUTE for stochastic nodes. ([Hamilton][1])                                                                                                                                      |
| **Cheap / low reuse intermediates**                 | one-off transforms used by a single downstream                               |                              **DISABLE** |                                 — | none                                                 | Avoid cache bloat; keep only high fan-out / high cost nodes cached.                                                                                                                                           |
| **“Semantic” outputs (public API to agents)**       | `semantic.function_risk_v1`, `semantic.call_graph_enriched`                  |                              **DEFAULT** |                      parquet/json | file store + (optionally) also materialize to DuckDB | Treat these as stable “products”; ensure they’re always cacheable + introspectable.                                                                                                                           |
| **Dynamic execution (`Parallelizable/Collect`)**    | branch-per-module/per-file/per-shard workflows                               |                  *Expect special-casing* |                           depends | depends                                              | Hamilton’s cache adapter has explicit special-case handling for task-based execution; the adapter may set EXPAND nodes to RECOMPUTE to version yielded items individually (per its own docs). ([Hamilton][1]) |

### A.3 Encoding policy in code (recommended)

**Encode “category” locally, keep global defaults strict.** Concretely:

```python
from hamilton.function_modifiers import cache

@cache(behavior="ignore")
def duckdb_conn_str() -> str: ...

@cache(behavior="default", format="parquet")
def function_facts(...) -> "pd.DataFrame": ...

@cache(behavior="recompute", format="json")
def external_api_healthcheck(...) -> dict: ...
```

This scales better than maintaining huge `Builder.with_cache(default=[...])` node lists.

---

## B) JSONL cache-log ingester → DuckDB `pipeline_runs` / `pipeline_steps`

### B.1 Target table design (minimal but production-safe)

Key properties:

* **append-only raw event table** (idempotent inserts via `event_id`)
* a run table with derived start/end and summary stats
* store “extra/unknown” fields safely as JSON

```sql
-- schema choice is yours; "ops" keeps it separate from semantic data
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
  run_id TEXT PRIMARY KEY,
  engine TEXT NOT NULL,                -- e.g. 'hamilton_cache'
  started_at TIMESTAMP,
  ended_at TIMESTAMP,
  duration_ms BIGINT,
  status TEXT,                         -- 'observed' | 'complete' | 'error' (best-effort)
  cache_path TEXT,
  source_jsonl_glob TEXT,
  context JSON,                        -- repo/commit/pipeline name/etc (caller supplied)
  stats JSON,                          -- computed summary (hits/misses/etc)
  created_at TIMESTAMP DEFAULT now(),
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ops.pipeline_steps (
  event_id TEXT PRIMARY KEY,           -- sha1 of stable event fields
  run_id TEXT NOT NULL,
  engine TEXT NOT NULL,
  seq BIGINT,                          -- source order (line number / monotonic)
  node_name TEXT,
  task_id TEXT,
  actor TEXT,                          -- 'adapter'|'metadata_store'|'result_store' :contentReference[oaicite:7]{index=7}
  event_type TEXT,
  msg TEXT,
  value JSON,
  ts DOUBLE,                           -- raw float timestamp :contentReference[oaicite:8]{index=8}
  event_at TIMESTAMP,                  -- derived from ts
  source_file TEXT,
  source_line BIGINT,
  context JSON
);

CREATE INDEX IF NOT EXISTS idx_pipeline_steps_run ON ops.pipeline_steps(run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_steps_node ON ops.pipeline_steps(run_id, node_name);
```

### B.2 The ingester (single-file, drop-in module)

This is intentionally:

* tolerant to minor schema drift,
* idempotent (`ON CONFLICT DO NOTHING`),
* and capable of aggregating run summaries (hits/misses/executions) without needing Hamilton in-process.

```python
from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import duckdb


def _norm_event_type(x: Any) -> str | None:
    if x is None:
        return None
    if isinstance(x, str):
        return x
    if isinstance(x, dict):
        # tolerate {"value": "..."} / {"name": "..."} / etc
        for k in ("value", "name", "event_type"):
            if k in x and x[k] is not None:
                return str(x[k])
    return str(x)


def _to_json_safe(x: Any) -> Any:
    # Keep JSON scalars/containers as-is; stringify unknown objects.
    try:
        json.dumps(x)
        return x
    except TypeError:
        return str(x)


def _event_id(e: dict[str, Any], source_file: str, source_line: int) -> str:
    stable = {
        "run_id": e.get("run_id"),
        "actor": e.get("actor"),
        "event_type": _norm_event_type(e.get("event_type")),
        "node_name": e.get("node_name"),
        "task_id": e.get("task_id"),
        "msg": e.get("msg"),
        "value": _to_json_safe(e.get("value")),
        "timestamp": e.get("timestamp"),
        "source_file": source_file,
        "source_line": source_line,
    }
    blob = json.dumps(stable, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha1(blob).hexdigest()


def _ts_to_event_at(ts: float | None) -> dt.datetime | None:
    if ts is None:
        return None
    # docs show timestamp as float field on event :contentReference[oaicite:9]{index=9}
    return dt.datetime.fromtimestamp(float(ts), tz=dt.timezone.utc)


def discover_jsonl_files(cache_dir: str | Path) -> list[Path]:
    p = Path(cache_dir)
    if p.is_file() and p.suffix == ".jsonl":
        return [p]
    # Search broadly; Hamilton only specifies “a .jsonl file” under the cache metadata store dir. :contentReference[oaicite:10]{index=10}
    return sorted({*p.rglob("*.jsonl")})


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE SCHEMA IF NOT EXISTS ops;

        CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
          run_id TEXT PRIMARY KEY,
          engine TEXT NOT NULL,
          started_at TIMESTAMP,
          ended_at TIMESTAMP,
          duration_ms BIGINT,
          status TEXT,
          cache_path TEXT,
          source_jsonl_glob TEXT,
          context JSON,
          stats JSON,
          created_at TIMESTAMP DEFAULT now(),
          updated_at TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS ops.pipeline_steps (
          event_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          engine TEXT NOT NULL,
          seq BIGINT,
          node_name TEXT,
          task_id TEXT,
          actor TEXT,
          event_type TEXT,
          msg TEXT,
          value JSON,
          ts DOUBLE,
          event_at TIMESTAMP,
          source_file TEXT,
          source_line BIGINT,
          context JSON
        );

        CREATE INDEX IF NOT EXISTS idx_pipeline_steps_run ON ops.pipeline_steps(run_id);
        CREATE INDEX IF NOT EXISTS idx_pipeline_steps_node ON ops.pipeline_steps(run_id, node_name);
        """
    )


def ingest_hamilton_cache_jsonl(
    *,
    duckdb_path: str | Path,
    cache_dir: str | Path,
    context: dict[str, Any] | None = None,
    engine: str = "hamilton_cache",
) -> dict[str, Any]:
    """
    Reads all *.jsonl under cache_dir and inserts cache events into ops.pipeline_steps.
    Also upserts ops.pipeline_runs with derived start/end + hit/miss stats per run_id.

    Assumes each JSONL line is a serialized CachingEvent with fields:
      run_id, actor, event_type, node_name, task_id?, msg?, value?, timestamp :contentReference[oaicite:11]{index=11}
    """
    context = context or {}
    jsonl_files = discover_jsonl_files(cache_dir)
    con = duckdb.connect(str(duckdb_path))
    ensure_tables(con)

    # 1) ingest events
    inserted = 0
    seen_run_ids: set[str] = set()

    for f in jsonl_files:
        with f.open("r", encoding="utf-8") as fp:
            for line_no, line in enumerate(fp, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue

                run_id = e.get("run_id")
                if not run_id:
                    continue
                seen_run_ids.add(str(run_id))

                ev_id = _event_id(e, str(f), line_no)
                ts = e.get("timestamp")
                event_at = _ts_to_event_at(ts)

                row = {
                    "event_id": ev_id,
                    "run_id": str(run_id),
                    "engine": engine,
                    "seq": int(line_no),
                    "node_name": e.get("node_name"),
                    "task_id": e.get("task_id"),
                    "actor": e.get("actor"),
                    "event_type": _norm_event_type(e.get("event_type")),
                    "msg": e.get("msg"),
                    "value": json.dumps(_to_json_safe(e.get("value")), default=str) if e.get("value") is not None else None,
                    "ts": float(ts) if ts is not None else None,
                    "event_at": event_at.isoformat() if event_at else None,
                    "source_file": str(f),
                    "source_line": int(line_no),
                    "context": json.dumps(context, default=str),
                }

                con.execute(
                    """
                    INSERT INTO ops.pipeline_steps
                    (event_id, run_id, engine, seq, node_name, task_id, actor, event_type, msg, value, ts, event_at, source_file, source_line, context)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), ?, ?, ?, ?, CAST(? AS JSON))
                    ON CONFLICT(event_id) DO NOTHING
                    """,
                    [
                        row["event_id"], row["run_id"], row["engine"], row["seq"], row["node_name"], row["task_id"],
                        row["actor"], row["event_type"], row["msg"], row["value"], row["ts"], row["event_at"],
                        row["source_file"], row["source_line"], row["context"],
                    ],
                )
                inserted += con.rowcount

    # 2) derive + upsert runs
    # We infer run start/end as min/max event_at; Hamilton only guarantees per-event timestamp. :contentReference[oaicite:12]{index=12}
    for run_id in seen_run_ids:
        stats = con.execute(
            """
            SELECT
              min(event_at) AS started_at,
              max(event_at) AS ended_at,
              count(*) AS n_events,
              -- best-effort cache hit/miss heuristics based on common log patterns:
              sum(CASE WHEN actor='result_store' AND event_type='get_result' AND msg='hit' THEN 1 ELSE 0 END) AS cache_hits,
              sum(CASE WHEN actor='result_store' AND event_type='get_result' AND msg='miss' THEN 1 ELSE 0 END) AS cache_misses,
              count(DISTINCT CASE WHEN actor='adapter' AND event_type='execute_node' THEN node_name ELSE NULL END) AS executed_nodes
            FROM ops.pipeline_steps
            WHERE run_id = ?
            """,
            [run_id],
        ).fetchone()

        started_at, ended_at, n_events, hits, misses, executed_nodes = stats
        duration_ms = None
        if started_at and ended_at:
            duration_ms = int((ended_at - started_at).total_seconds() * 1000)

        run_stats = {
            "n_events": int(n_events or 0),
            "cache_hits": int(hits or 0),
            "cache_misses": int(misses or 0),
            "executed_nodes": int(executed_nodes or 0),
        }

        con.execute(
            """
            INSERT INTO ops.pipeline_runs
              (run_id, engine, started_at, ended_at, duration_ms, status, cache_path, source_jsonl_glob, context, stats, updated_at)
            VALUES
              (?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), CAST(? AS JSON), now())
            ON CONFLICT(run_id) DO UPDATE SET
              started_at=excluded.started_at,
              ended_at=excluded.ended_at,
              duration_ms=excluded.duration_ms,
              context=excluded.context,
              stats=excluded.stats,
              updated_at=excluded.updated_at
            """,
            [
                run_id,
                engine,
                started_at,
                ended_at,
                duration_ms,
                "observed",
                str(cache_dir),
                str(Path(cache_dir) / "**/*.jsonl"),
                json.dumps(context, default=str),
                json.dumps(run_stats, default=str),
            ],
        )

    con.close()

    return {
        "inserted_events": inserted,
        "runs_updated": len(seen_run_ids),
        "jsonl_files": [str(p) for p in jsonl_files],
    }
```

### B.3 A couple of “immediately useful” DuckDB views/queries

**Per-run cache efficiency:**

```sql
SELECT
  run_id,
  started_at,
  duration_ms,
  json_extract(stats, '$.cache_hits')::INT AS hits,
  json_extract(stats, '$.cache_misses')::INT AS misses,
  json_extract(stats, '$.executed_nodes')::INT AS executed_nodes
FROM ops.pipeline_runs
WHERE engine = 'hamilton_cache'
ORDER BY started_at DESC;
```

**Which semantic nodes were recomputed (vs hit):**

```sql
SELECT
  node_name,
  count(*) AS n_exec_events
FROM ops.pipeline_steps
WHERE run_id = ? AND actor='adapter' AND event_type='execute_node'
GROUP BY 1
ORDER BY 2 DESC;
```

---

If you want one more refinement that tends to pay off: I can adapt the ingester to also pull **`cache_key` / `data_version`** per node by joining against the cache metadata store (or by running an in-process “post-run export” that calls `dr.cache.get_cache_key(...)` / `dr.cache.get_data_version(...)` and emits a deterministic per-node record). That’s the step that turns cache logs into a full lineage-grade “what exact versions fed this semantic artifact?” trace.

[1]: https://hamilton.apache.org/reference/caching/caching-logic/ "Caching logic - Hamilton"
[2]: https://hamilton.apache.org/concepts/caching/ "Caching - Hamilton"


# Lineage grade tracing #

Absolutely — here are **two concrete, drop-in refinements** that give you a lineage-grade trace by capturing **`cache_key` + `data_version` (+ decoded dependency versions)** for each node.

This uses the fact that a cache key encodes `{node_name, code_version, dependencies_data_versions}` ([Hamilton][1]) and can be decoded via `from hamilton.caching.cache_key import decode_key` ([Hamilton][2]). Metadata stores are expected to return per-node metadata including `cache_key` and `data_version`, and decoding the `cache_key` yields dependency data versions. ([Hamilton][3])

---

## 1) Schema: add a single “node lineage” table (don’t duplicate per-event)

Keep your `ops.pipeline_steps` as raw event stream. Add **one row per (run_id, node_name, task_id)**:

```sql
CREATE TABLE IF NOT EXISTS ops.pipeline_node_lineage (
  lineage_id TEXT PRIMARY KEY,     -- sha1(run_id,node_name,task_id,cache_key,data_version)
  run_id TEXT NOT NULL,
  engine TEXT NOT NULL,

  node_name TEXT NOT NULL,
  task_id TEXT,                    -- present when task-based execution is used

  cache_key TEXT,
  data_version TEXT,
  code_version TEXT,

  dependencies JSON,               -- decode_key(cache_key)["dependencies_data_versions"]
  recorded_at TIMESTAMP,
  source TEXT,                     -- 'metadata_store' | 'in_process'
  context JSON
);

CREATE INDEX IF NOT EXISTS idx_node_lineage_run_node
  ON ops.pipeline_node_lineage(run_id, node_name);

CREATE INDEX IF NOT EXISTS idx_node_lineage_run_dv
  ON ops.pipeline_node_lineage(run_id, data_version);
```

Why separate table: lineage is **node-level**, while logs are **event-level**; repeating cache_key/data_version on every event line explodes size and makes queries noisier.

---

## 2) Option A (out-of-process): enrich from the cache **metadata store** (SQLite, etc.)

### When to prefer

* Your ingester runs independently (e.g., cron / service) and you have access to the metadata store path.
* You *don’t* need per-task granularity for dynamic execution (task-based runs can be handled, but in-process is usually cleaner).

### How it works

`MetadataStore.get_run(run_id)` returns node metadata; docs specify it should include `cache_key` and `data_version`, and decoding the key yields `code_version` and dependency versions. ([Hamilton][3])
For SQLite specifically, `get_run` returns node metadata including `cache_key`, `data_version`, `node_name`, `code_version`. ([Hamilton][4])

### Code: ingest lineage from SQLiteMetadataStore

```python
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Iterable

import duckdb
from hamilton.caching.cache_key import decode_key  # :contentReference[oaicite:5]{index=5}
from hamilton.caching.stores.sqlite import SQLiteMetadataStore


def _lineage_id(run_id: str, node_name: str, task_id: str | None, cache_key: str | None, data_version: str | None) -> str:
    blob = json.dumps(
        {"run_id": run_id, "node": node_name, "task_id": task_id, "cache_key": cache_key, "data_version": data_version},
        sort_keys=True,
        default=str,
    ).encode("utf-8")
    return hashlib.sha1(blob).hexdigest()


def ensure_lineage_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE SCHEMA IF NOT EXISTS ops;

        CREATE TABLE IF NOT EXISTS ops.pipeline_node_lineage (
          lineage_id TEXT PRIMARY KEY,
          run_id TEXT NOT NULL,
          engine TEXT NOT NULL,
          node_name TEXT NOT NULL,
          task_id TEXT,
          cache_key TEXT,
          data_version TEXT,
          code_version TEXT,
          dependencies JSON,
          recorded_at TIMESTAMP,
          source TEXT,
          context JSON
        );

        CREATE INDEX IF NOT EXISTS idx_node_lineage_run_node
          ON ops.pipeline_node_lineage(run_id, node_name);

        CREATE INDEX IF NOT EXISTS idx_node_lineage_run_dv
          ON ops.pipeline_node_lineage(run_id, data_version);
        """
    )


def ingest_lineage_from_sqlite_metadata_store(
    *,
    duckdb_path: str,
    sqlite_metadata_path: str,
    run_ids: Iterable[str],
    engine: str = "hamilton_cache",
    context: dict[str, Any] | None = None,
) -> int:
    """
    Reads metadata_store.get_run(run_id) and upserts one lineage row per node.
    """
    context = context or {}
    con = duckdb.connect(duckdb_path)
    ensure_lineage_table(con)

    ms = SQLiteMetadataStore(path=sqlite_metadata_path)

    inserted = 0
    now = datetime.now(timezone.utc).isoformat()

    for run_id in run_ids:
        # Returns a list of dicts including cache_key/data_version/node_name/code_version for SQLiteMetadataStore. :contentReference[oaicite:6]{index=6}
        node_rows = ms.get_run(run_id)

        for r in node_rows:
            node_name = r.get("node_name")
            cache_key = r.get("cache_key")
            data_version = r.get("data_version")
            code_version = r.get("code_version")

            deps = None
            if cache_key:
                decoded = decode_key(cache_key)
                deps = decoded.get("dependencies_data_versions")  # mapping dep_name -> dep_data_version :contentReference[oaicite:7]{index=7}
                code_version = code_version or decoded.get("code_version")

            lid = _lineage_id(run_id, node_name, None, cache_key, data_version)

            con.execute(
                """
                INSERT INTO ops.pipeline_node_lineage
                  (lineage_id, run_id, engine, node_name, task_id, cache_key, data_version, code_version,
                   dependencies, recorded_at, source, context)
                VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), ?, ?, CAST(? AS JSON))
                ON CONFLICT(lineage_id) DO NOTHING
                """,
                [
                    lid,
                    run_id,
                    engine,
                    node_name,
                    None,
                    cache_key,
                    data_version,
                    code_version,
                    json.dumps(deps) if deps is not None else None,
                    now,
                    "metadata_store",
                    json.dumps(context, default=str),
                ],
            )
            inserted += con.rowcount

    con.close()
    return inserted
```

---

## 3) Option B (recommended for task-based/dynamic): **in-process post-run export** using `dr.cache.get_cache_key/get_data_version`

### When to prefer

* You use task-based execution (`Parallelizable/Collect`) or want full fidelity across executors.
* You want a deterministic “run artifact” that can be shipped/stored independently of the cache store internals.

Hamilton’s caching adapter exposes **public-facing** inspection methods:

* `dr.cache.get_cache_key(run_id, node_name, task_id)` ([Hamilton][5])
* `dr.cache.get_data_version(run_id, node_name, task_id)` (checks in-memory + metadata store) ([Hamilton][5])

It also clarifies uniqueness: with task-based execution, `(node_name, task_id)` is the unique identifier. ([Hamilton][5])

### 3.1 Export a deterministic `cache_lineage.jsonl`

```python
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from hamilton.caching.cache_key import decode_key  # :contentReference[oaicite:11]{index=11}


def export_cache_lineage_jsonl(
    *,
    dr,                           # Hamilton Driver with caching enabled
    out_path: str | Path,
    run_id: str | None = None,
    context: dict[str, Any] | None = None,
) -> Path:
    """
    Writes one JSONL record per (node_name, task_id) observed in dr.cache.logs(run_id, level='debug').
    Each record includes cache_key + data_version + decoded deps.
    """
    context = context or {}
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    rid = run_id or dr.cache.last_run_id  # last started run_id (not necessarily last to complete) :contentReference[oaicite:12]{index=12}

    # logs(run_id=...) returns per-node or per-(node,task) buckets. :contentReference[oaicite:13]{index=13}
    run_logs = dr.cache.logs(run_id=rid, level="debug")

    keys: set[tuple[str, str | None]] = set()
    for k in run_logs.keys():
        if isinstance(k, tuple):
            node_name, task_id = k
        else:
            node_name, task_id = k, None
        keys.add((str(node_name), str(task_id) if task_id is not None else None))

    def _lid(node: str, task: str | None, ck: str | None, dv: str | None) -> str:
        blob = json.dumps({"run_id": rid, "node": node, "task_id": task, "cache_key": ck, "data_version": dv}, sort_keys=True).encode()
        return hashlib.sha1(blob).hexdigest()

    records = []
    for node_name, task_id in sorted(keys, key=lambda x: (x[0], x[1] or "")):
        ck = dr.cache.get_cache_key(run_id=rid, node_name=node_name, task_id=task_id)  # :contentReference[oaicite:14]{index=14}
        dv = dr.cache.get_data_version(run_id=rid, node_name=node_name, task_id=task_id)  # :contentReference[oaicite:15]{index=15}

        # Some nodes may return sentinel values if not present; treat non-str as missing.
        ck = ck if isinstance(ck, str) else None
        dv = dv if isinstance(dv, str) else None

        decoded = decode_key(ck) if ck else {}
        deps = decoded.get("dependencies_data_versions")  # dep_name -> dep_data_version :contentReference[oaicite:16]{index=16}
        code_version = decoded.get("code_version")

        records.append(
            {
                "lineage_id": _lid(node_name, task_id, ck, dv),
                "run_id": rid,
                "node_name": node_name,
                "task_id": task_id,
                "cache_key": ck,
                "data_version": dv,
                "code_version": code_version,
                "dependencies": deps,
                "source": "in_process",
                "context": context,
            }
        )

    with out_path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, sort_keys=True, default=str) + "\n")

    return out_path
```

### 3.2 Ingest that lineage JSONL into DuckDB

```python
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


def ingest_lineage_jsonl(
    *,
    duckdb_path: str,
    lineage_jsonl: str | Path,
    engine: str = "hamilton_cache",
) -> int:
    con = duckdb.connect(duckdb_path)
    ensure_lineage_table(con)

    inserted = 0
    now = datetime.now(timezone.utc).isoformat()

    p = Path(lineage_jsonl)
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            r = json.loads(line)

            con.execute(
                """
                INSERT INTO ops.pipeline_node_lineage
                  (lineage_id, run_id, engine, node_name, task_id, cache_key, data_version, code_version,
                   dependencies, recorded_at, source, context)
                VALUES
                  (?, ?, ?, ?, ?, ?, ?, ?, CAST(? AS JSON), ?, ?, CAST(? AS JSON))
                ON CONFLICT(lineage_id) DO NOTHING
                """,
                [
                    r.get("lineage_id"),
                    r.get("run_id"),
                    engine,
                    r.get("node_name"),
                    r.get("task_id"),
                    r.get("cache_key"),
                    r.get("data_version"),
                    r.get("code_version"),
                    json.dumps(r.get("dependencies")) if r.get("dependencies") is not None else None,
                    now,
                    r.get("source", "in_process"),
                    json.dumps(r.get("context") or {}, default=str),
                ],
            )
            inserted += con.rowcount

    con.close()
    return inserted
```

---

## 4) “Lineage-grade trace”: how you query upstream versions for a semantic artifact

Because `decode_key(cache_key)["dependencies_data_versions"]` is a mapping of `{dep_node_name: dep_data_version}` ([Hamilton][2]), you can reconstruct the exact inputs to a semantic node.

A quick approach in DuckDB is:

1. fetch the lineage row for the semantic node,
2. expand `dependencies` JSON into rows,
3. join back onto `ops.pipeline_node_lineage` on `(run_id, node_name, data_version)`.

(Exact JSON table-function syntax varies by DuckDB version; conceptually:)

```sql
-- PSEUDO-SQL: expand dependencies and join to find upstream nodes’ own lineage rows
WITH target AS (
  SELECT run_id, node_name, dependencies
  FROM ops.pipeline_node_lineage
  WHERE run_id = ? AND node_name = ?
),
deps AS (
  SELECT
    t.run_id,
    dep.key   AS dep_node_name,
    dep.value AS dep_data_version
  FROM target t
  -- use DuckDB’s JSON expansion function of choice for your version
)
SELECT
  d.dep_node_name,
  d.dep_data_version,
  u.cache_key AS dep_cache_key,
  u.code_version AS dep_code_version
FROM deps d
LEFT JOIN ops.pipeline_node_lineage u
  ON u.run_id = d.run_id
 AND u.node_name = d.dep_node_name
 AND u.data_version = d.dep_data_version;
```

That join is the “magic”: it makes cache lineage **queryable like a run graph**, without OpenLineage.

---

## Recommendation: ship both paths, prefer in-process for task-based runs

* Use **metadata-store enrichment** for “offline, simple” runs; metadata stores are explicitly designed to expose `cache_key`/`data_version` per node. ([Hamilton][3])
* Use **in-process export** when task IDs matter or you want the most robust, portable artifact; the adapter’s public methods are explicitly task-aware. ([Hamilton][5])

If you want, I can also add a tiny “post-run hook” wrapper that automatically:

1. exports `cache_lineage.jsonl` next to the cache log JSONL, and
2. calls your ingester (or drops a “ready” marker file) so your observability pipeline is fully hands-off.

[1]: https://hamilton.dagworks.io/en/latest/concepts/caching/?utm_source=chatgpt.com "Caching - Hamilton"
[2]: https://hamilton.dagworks.io/en/latest/how-tos/caching-tutorial/ "Caching - Hamilton"
[3]: https://hamilton.dagworks.io/en/latest/reference/caching/stores/ "Stores - Hamilton"
[4]: https://hamilton.dagworks.io/en/latest/reference/caching/stores/?utm_source=chatgpt.com "Stores - Hamilton"
[5]: https://hamilton.apache.org/reference/caching/caching-logic/?utm_source=chatgpt.com "Caching logic - Hamilton"
