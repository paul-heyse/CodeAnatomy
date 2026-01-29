Below is a deep “pre-orchestration” pass over your pipeline (view specs + extract schema → DataFusion plans → Delta-backed CPG outputs), focused on (a) making the DataFusion calculation layer more seamless / high‑performance / programmatically driven, (b) materially improving metadata generation, and (c) tightening Delta Lake integrations so the *DataFusion+Delta layer itself* does more of the heavy lifting.

I’m anchoring this in (1) what’s currently in your repo (notably `src/datafusion_engine/*`, `src/storage/deltalake/*`, `src/cpg/*`), (2) the project docs you provided on DataFusion planning/schema and Delta+DataFusion integration, and (3) recent upstream docs on DataFusion + delta-rs.

---

## 1) Make the calculation pipeline “scan-boundary clean” so everything downstream is simpler

A theme you can lean into harder: **do as much coercion, defaulting, and schema-evolution handling as possible at the *scan boundary*** (the moment you turn “a dataset location/provider” into a DataFusion relation). This makes every view spec simpler, pushes work into the most optimizable place, and unlocks more reliable plan caching / scan overrides / incremental modes.

### 1.1 Use `DeltaTableProvider` + `DeltaScanConfig` everywhere you can

Your project docs (and delta-rs itself) make this explicit: `DeltaTableProvider` can use Delta log metadata for file skipping and is typically more efficient than generic file listing approaches. It also exposes scan‑time knobs via `DeltaScanConfig` such as a file path column and partition wrapping. 

**Actionable improvements**

* **Make “Delta-first” the default** for *all* persisted intermediate & final datasets (extract → normalize → CPG outputs). Only use in-memory/arrow ingestion for tests or very small ephemeral tables.
* Standardize your per-dataset `DeltaScanOptions`:

  * `enable_parquet_pushdown = true` and `schema_force_view_types = true` (both are explicitly supported by delta-rs and intended to be driven from Session settings) .
  * Keep `wrap_partition_values = true` if you’re leaning on partition columns with dictionary semantics .
  * Only enable `file_column_name` when you actually need it for debugging/lineage (it has overhead) .

### 1.2 Use projection expressions / schema adapters at scan time (not scattered casts later)

Your `DatasetRegistry` already has the right conceptual slot (`projection_exprs` and “scan-time options”). You can push this further using the patterns described in your DataFusion schema doc: **`projection_exprs` can cast and fill missing columns at the scan level**, and **`PhysicalExprAdapter`** can help adapt schemas when columns differ across files/partitions. 

**What to do**

* For any dataset family where schemas may drift (especially “extract” tables, and anything that evolves over time), define a single “canonical contract schema” and:

  * express missing columns as `NULL` literals / defaults at scan time;
  * cast all columns into canonical types once (scan-time), not repeatedly in view builders.

**Why this matters**

* You’ll remove a huge amount of `coalesce + cast` boilerplate from the downstream CPG views.
* Optimizer pushdowns become more predictable (casts that occur *after* filters can inhibit pushdown).

---

## 2) Reduce planning/execution overhead by reshaping “view spec → plan” interactions

Your planning docs capture the canonical lifecycle: SQL/DF → logical plan → analyze → optimize → physical plan → execute; plus plan introspection/EXPLAIN forms you can store.

The biggest wins here are usually:

* less repeated work across many views
* fewer “accidental” full scans of common upstream relations
* better plan caching/reuse boundaries

### 2.1 Add *explicit* “cache boundaries” in the view graph

DataFusion Python supports `DataFrame.cache()` to cache a DataFrame (materializing it for reuse) . Today your pipeline builds separate outputs (`cpg_nodes`, `cpg_edges`, `cpg_props`) which can cause repeated scans of the same upstream extract/normalize tables across multiple output computations.

**What I’d add**

* Extend your `ViewNode` / view registry spec with a small hint like:

  * `cache_policy: NONE | MEMORY | DELTA_STAGING`
  * `cache_key: stable fingerprint` (e.g., based on input snapshot + view identity)
* Then implement this in the execution layer (materialization), not the planning layer:

  * If `MEMORY`: execute once + `df.cache()` + register cached DF as a temp view for downstream consumers.
  * If `DELTA_STAGING`: write once to a temporary Delta table and override scans downstream to point at it.

This pairs beautifully with your existing **scan override / re-planning** pattern: plan upstream once, materialize caches, then re-plan downstream with pinned providers.

### 2.2 Plan artifacts: store *multiple* explain formats and plan fingerprints

Your planning doc highlights the different explain formats available (`logical_plan`, `optimized_logical_plan`, `physical_plan`, `execution_plan`) and how to retrieve them programmatically.

If you want “best-in-class” operational visibility and regression safety:

* Store **(a)** unoptimized logical plan, **(b)** optimized logical plan, **(c)** physical plan, and **(d)** `EXPLAIN ANALYZE` metrics per view/output per run.
* Compute a “plan fingerprint” hash from:

  * logical plan proto/variant (or Substrait if you prefer),
  * config snapshot,
  * relevant provider snapshot/version info.

This gives you:

* cheap plan diffs (“why did this query slow down?”)
* deterministic caching keys
* CI regression tests that catch accidental plan explosions early

### 2.3 Stop relying on “in-memory record batch tables” if you want true plan serialization/caching

DataFusion’s Python plan serialization (`to_proto`/`from_proto`) explicitly notes that **tables created in memory from record batches are not supported** for plan proto serialization.

So if you want to go harder on plan caching and “precompiled plans”:

* make your extract phase write to Delta/Parquet first (even locally),
* then register providers (DeltaTableProvider) instead of `register_record_batches` for most non-trivial pipelines.

That unlocks:

* more reliable physical plan serde,
* better scan pushdowns,
* and fewer Python↔Rust crossings.

---

## 3) Under-leveraged DataFusion capabilities you should explicitly exploit

### 3.1 Parquet metadata cache + dynamic filtering

DataFusion 50 introduced/spotlighted:

* **Parquet metadata cache** (helps when scanning lots of Parquet files repeatedly).
* **Dynamic filter pushdown** for hash joins (useful when joining big fact tables to small filtered dimensions). ([delta-io.github.io][1])

Even if you already set some runtime knobs, make sure you’re:

* using metadata cache for all read-heavy stages (normalize + CPG generation),
* structuring joins such that the “filtered small side” is actually recognizable (e.g., push filters before joins; avoid wrapping join keys in expensive UDFs pre-join).

### 3.2 Use bloom filters for your “hot lookup keys”

DataFusion’s parquet settings include bloom filter read/write toggles and are designed exactly for “I filter on this high-cardinality column constantly”. ([delta-io.github.io][1])

For a CPG, the obvious “hot keys” are:

* `node_id`, `edge_id`, `file_id`
* maybe `prop_key` / `node_kind` (but those might benefit more from dictionary + stats)

If you do lots of point lookups / semi-joins on IDs:

* consider enabling bloom filter writing (where supported by your writer stack),
* or at least writing row groups sorted by these keys (see Z-order below).

### 3.3 Lean into Arrow schema metadata + extension types for IDs and spans

DataFusion (48+) significantly improved propagating field metadata through functions and supports implementing user-defined logical types via Arrow extension types. This is a *huge* opportunity for a CPG system where “this string is a NodeId” is semantically different from “this string is a Path”. ([datafusion.apache.org][2])

**What to implement**

* Define Arrow extension types for:

  * `NodeId`, `EdgeId`, `SpanId`, maybe `QName`
* In your Rust UDFs:

  * validate input types via metadata at planning time,
  * return output fields with the appropriate extension metadata.

**Benefits**

* You prevent entire classes of mistakes (accidental joins on wrong IDs).
* You make downstream schema/contract validation much stronger.
* You can generate better metadata tables automatically.

---

## 4) Delta Lake: practical integration upgrades that materially improve performance and operability

Your Delta+DataFusion integration doc and delta-rs docs are rich. Here are the “big lifts” that most directly improve a DataFusion-driven CPG pipeline.

### 4.1 Treat table layout as a first-class API: compaction + Z-order + stats selection

Delta best practices call out:

* right-sized files (often ~100MB–1GB depending on workload),
* compaction for append-heavy pipelines,
* Z-ordering to colocate related values for better file skipping,
* and carefully choosing which columns get min/max stats (don’t waste time collecting stats on columns that won’t help skipping). ([delta-io.github.io][3])

#### 4.1.a Add **Z-order** to your maintenance policy

You already support optimize/compact/vacuum patterns. What’s missing is **Z-order** as a first-class, per-table policy.

Delta docs emphasize Z-ordering as a table-layout optimization for file skipping. ([delta-io.github.io][3])
And delta-rs explicitly documents a Z-order implementation in its optimize docs. ([delta-io.github.io][4])

**Concrete recommendation**

* Extend `DeltaMaintenancePolicy` (in `schema_spec/system.py`) with something like:

  * `z_order_cols: tuple[str, ...] = ()`
  * `z_order_when: "never" | "periodic" | "after_partition_complete"`
* For CPG tables, good starting Z-order columns:

  * `cpg_nodes`: `repo_id`, `file_id`, `node_kind`
  * `cpg_edges`: `repo_id`, `edge_owner_file_id`, `edge_kind`, `edge_source_node_id`
  * `cpg_props`: `repo_id`, `owner_id`, `prop_key`

Then do Z-order only when it’s worth it (e.g., after bulk loads, or when a partition is “done”)—the small file compaction guide explicitly recommends running optimize less frequently than appends. ([delta-io.github.io][5])

#### 4.1.b Don’t collect stats for “junk columns”; do collect stats for skip columns

Delta best practices explicitly recommend:

* ensuring stats exist for columns used in file skipping,
* and avoiding stats collection for columns unlikely to be filtered (e.g., long arbitrary strings). ([delta-io.github.io][3])

**How to apply to your CPG schema**

* Likely *do* want stats for:

  * `repo_id`, `file_id`, `node_kind`, `edge_kind`, `prop_key`, maybe `span_bstart/bend`
* Likely *don’t* want stats for:

  * huge JSON blobs, source snippets, long docstrings, any “payload” columns rarely filtered.

Implementation-wise:

* introduce a per-table “stats policy” derived from your schema/metadata,
* wire it into your delta writer options (or writer properties).

### 4.2 Change Data Feed for incremental graph refresh

Delta change data feed requires enabling table property `delta.enableChangeDataFeed = true` and can be read via `load_cdf` APIs. ([Spice AI][6])

**Where this helps your pipeline**

* If your extraction/normalization writes to Delta, CDF can drive:

  * which files changed,
  * which entities/edges need recomputation,
  * incremental rebuild of affected partitions/rows.

Even if you keep your own “changed file list”, CDF becomes a *format-level* source of truth that can reduce bespoke bookkeeping.

### 4.3 Be careful with advanced Delta table features: row tracking + deletion vectors

I noticed in your repo you’re defaulting several feature properties (row tracking, deletion vectors, in-commit timestamps). The intent is good (these are powerful), but **engine support is uneven** outside Spark.

Two important points from current upstream docs:

* Delta best practices explicitly say: *deletion vectors make DML faster*, but **“delta-rs doesn’t support deletion vectors yet.”** ([delta-io.github.io][3])
* The Delta row tracking doc warns that enabling row tracking bumps protocol/writer version (writer v7) and tables **are not writable by clients that don’t support the enabled features**. ([Delta Lake][7])

**Recommendation**

* Treat these as **capabilities you can toggle per environment**, not global defaults:

  * `enableChangeDataFeed`: generally safe / supported → keep
  * `enableDeletionVectors`: don’t default on until you confirm your exact delta-rs version + writers can write reliably
  * `enableRowTracking`: only enable if you have a clear consumer story and verified client support
  * `enableInCommitTimestamps`: similar caution

This will prevent “mysterious write failures” and reduce compatibility traps.

---

## 5) Best-in-class CPG on DataFusion+Delta: what I’d change in the *data model + compute contracts*

This is where you can get “best in class” even if the *high-level outputs* remain nodes/edges/props.

### 5.1 Make the “CPG snapshot” a first-class, versioned entity

A best-in-class lakehouse CPG behaves like a *versioned graph snapshot*:

* reproducible by commit/version/time travel,
* queryable at any snapshot,
* incrementally updatable.

**Add a small but critical Delta table**

* `cpg_runs` (or `graph_snapshots`) with:

  * `run_id`, `repo_id`, repo commit hash, extraction version,
  * input Delta versions of each upstream dataset,
  * output Delta versions for nodes/edges/props,
  * plan artifact ids / fingerprints,
  * row counts & summary stats.

This becomes the “manifest” that makes everything else debuggable and reproducible.

### 5.2 Turn your schema contracts into a “semantic schema” (not just column names/types)

You already have `schema_spec` and contract ideas. Take the next step by using Arrow metadata/extension types (section 3.3) and enforcing:

* primary key semantics (even if not physically enforced),
* foreign key semantics (node_id referenced by edges/props),
* stable ID derivation version.

Then:

* emit these semantics into Delta table properties / metadata tables,
* and validate them during writing and/or in a post-write verification view.

### 5.3 Add “query-facing” helper tables/views that match real graph workloads

Most graph workloads want:

* “get me all properties for node X”
* “get me all outgoing edges for node X”
* “get me nodes/edges for file F”
* “get me all nodes of kind K with property P…”

You can make those fast in a lakehouse with a handful of derived representations, without abandoning the normalized triple model.

**Two high-value additions**

1. **Node property map view / table** (optional)

   * Group `cpg_props` into per-node property bundles (map/struct).
   * DataFusion is strong with nested types and scalar/nested functions; your project docs emphasize nested structs/maps and extraction helpers. 

2. **Edge adjacency index view / table** (optional)

   * Materialize `edges_by_src_node_id` and/or `edges_by_dst_node_id` (possibly with Z-order on node id).
   * This is essentially a “secondary index” in lakehouse form.

These are the kinds of “graph accelerators” that let you keep everything relational while feeling graph-native.

---

## 6) Concrete “do this next” checklist (high impact, pre-orchestration)

Here’s a tight list of changes that I believe will pay off immediately and also set you up for a clearer orchestration story later.

### P0: correctness + compatibility + big wins

* **Gate Delta table features**: don’t default-enable deletion vectors / row tracking unless you’ve verified full write support in your exact delta-rs stack. ([delta-io.github.io][3])
* **Implement Z-order policy** for CPG outputs (nodes/edges/props) and run it at sensible cadence (e.g., after bulk loads). ([delta-io.github.io][3])
* **Add “stats policy”**: collect min/max stats only on columns useful for file skipping; skip stats for huge payload columns. ([delta-io.github.io][3])

### P1: simplify view code + push work to scan boundary

* Expand scan-boundary schema alignment with `projection_exprs` and/or schema adapters so downstream views don’t need repeated casts/coalesces. 
* Ensure all persisted datasets are Delta (providers, not in-memory tables) if you want stronger plan caching / serialization.

### P2: “best-in-class” semantics and ergonomics

* Introduce Arrow extension types + metadata for ID columns and spans; validate in Rust UDFs; propagate metadata through outputs. ([datafusion.apache.org][2])
* Add snapshot/run manifest Delta table that ties together inputs, outputs, versions, and plan artifacts.

---

## If you want one “north star” refactor

If you do only one major architectural refactor before touching orchestration:

**Refactor to a staged, Delta-backed pipeline with explicit boundaries**

1. Extract → **write to Delta** (typed, canonical schema)
2. Normalize → **write to Delta** (stable IDs, canonical entity tables)
3. CPG emit (nodes/edges/props + optional accelerators) → **write to Delta**
4. Maintenance (compact/z-order/vacuum) + metrics/manifest

Then your view specs remain DataFusion-native, but:

* planning gets simpler (providers are stable and pushdown-friendly),
* metadata becomes first-class (Delta versions + plan fingerprints),
* incremental work becomes “just compute deltas between Delta versions”.

That’s the foundation that will make the later rustworkx/Hamilton decision obvious.

If you want, in the next step I can propose:

* the exact table properties / partitioning / Z-order columns for each of your concrete outputs (based on your actual query patterns),
* and a concrete set of code changes (PR-style) in `storage/deltalake/*`, `schema_spec/system.py`, and `datafusion_engine/write_pipeline.py` to implement Z-order + stats policy + feature gating.

[1]: https://delta-io.github.io/delta-rs/usage/read-cdf/ "Reading Change Data - Delta Lake Documentation"
[2]: https://datafusion.apache.org/blog/2025/09/21/custom-types-using-metadata/ "Implementing User Defined Types and Custom Metadata in DataFusion - Apache DataFusion Blog"
[3]: https://delta-io.github.io/delta-rs/delta-lake-best-practices/ "Best practices - Delta Lake Documentation"
[4]: https://delta-io.github.io/delta-rs/usage/optimize/delta-lake-z-order/ "Z Order - Delta Lake Documentation"
[5]: https://delta-io.github.io/delta-rs/usage/optimize/small-file-compaction-with-optimize/ "Small file compaction - Delta Lake Documentation"
[6]: https://spice.ai/blog/how-we-use-apache-datafusion-at-spice-ai "How we use Apache DataFusion at Spice AI | Spice AI"
[7]: https://docs.delta.io/delta-row-tracking/ "Use row tracking for Delta tables | Delta Lake"

Absolutely — here are the **two additional deliverables** you asked for:

1. **Exact table layout recommendations** (partitioning + Z-order/cluster columns + stats columns + key Delta properties) for each *concrete* output table you currently materialize (extract → normalize → final CPG + metadata tables).

2. A **PR‑style concrete patch plan** (diff-like) touching exactly the modules you named:

* `src/storage/deltalake/*`
* `src/schema_spec/system.py`
* `src/datafusion_engine/write_pipeline.py`

I’m going to be *very explicit* and “implementation-ready” (names, option keys, defaults, and where to wire them).

---

# 1) ✅ Exact table layout recommendations (per output)

## Guiding assumptions (from your codebase + query patterns)

Your code strongly indicates the following “hot” access paths:

* **File-scoped filters**: `file_id`, `path`
* **Span-scoped filters**: `bstart`, `bend`
* **Point lookups / joins**: IDs such as `node_id`, `edge_id`, `expr_id`, `type_id`, `bc_*_id`, `diag_id`
* **Graph-type filters**: `node_kind`, `edge_kind`, `prop_key`
* **Incremental maintenance patterns**: delete/upsert by file-ish keys (even if not always a Delta partition)

So the design below aims for:

> **(A) minimal / safe partitioning** (avoid high-cardinality partitions by default),
> **(B) aggressive stats-on-hot-columns** (Delta data skipping),
> **(C) clustering (“Z-order-like”) sorting on write** (cheap win; true Z-order optional later),
> **(D) feature gating**: only enable protocol/table features when explicitly requested.

### A note on “Z-order”

DataFusion doesn’t provide a native Z-order rewrite like Spark’s `OPTIMIZE ZORDER BY`. The best practical option in your stack is:

* **Sort/cluster on write** by `(partition_cols + zorder_cols)` to improve Parquet row-group locality and Delta stats pruning.
* If you later want “true Z-order”, you can add a UDF to compute an interleaved key and sort by it (optional future enhancement).

---

## 📦 A) Extract delta tables (9)

> Default: **no Delta partition columns** (avoid `path`/`file_id` explosion); rely on **stats + clustering**.

| Table                  | Partition By | Cluster (“Z-order”) By | `delta.dataSkippingStatsColumns`                                           | `delta.targetFileSize` |
| ---------------------- | -----------: | ---------------------- | -------------------------------------------------------------------------- | ---------------------: |
| `repo_files_v1`        |         `()` | `(file_id, path)`      | `file_id,path,file_sha256,size_bytes,mtime_ns`                             |                  128MB |
| `repo_file_blobs_v1`   |         `()` | `(file_id, path)`      | `file_id,path,file_sha256,size_bytes,mtime_ns` *(exclude huge text/bytes)* |               64–128MB |
| `file_line_index_v1`   |         `()` | `(file_id, line_no)`   | `file_id,path,line_no,line_start_byte,line_end_byte`                       |                  128MB |
| `ast_files_v1`         |         `()` | `(file_id, path)`      | `file_id,path`                                                             |                  128MB |
| `libcst_files_v1`      |         `()` | `(file_id, path)`      | `file_id,path`                                                             |                  128MB |
| `tree_sitter_files_v1` |         `()` | `(file_id, path)`      | `file_id,path`                                                             |                  128MB |
| `bytecode_files_v1`    |         `()` | `(file_id, path)`      | `file_id,path`                                                             |                  128MB |
| `symtable_files_v1`    |         `()` | `(file_id, path)`      | `file_id,path`                                                             |                  128MB |
| `scip_index_v1`        |         `()` | `(index_path)`         | `index_id,index_path`                                                      |                  128MB |

---

## 🧱 B) Normalize delta tables (9)

| Table                     |                                  Partition By | Cluster (“Z-order”) By                                      | Stats Columns                                                                     | TargetFileSize |
| ------------------------- | --------------------------------------------: | ----------------------------------------------------------- | --------------------------------------------------------------------------------- | -------------: |
| `normalize_evidence_v1`   | `()` *(or `dataset_name` if low-cardinality)* | `(file_id, dataset_name, evidence_key)`                     | `file_id,dataset_name,evidence_family,evidence_key,timestamp_ms`                  |          128MB |
| `type_exprs_norm_v1`      |                                          `()` | `(file_id, bstart, bend, expr_id)`                          | `file_id,path,expr_id,expr_kind,bstart,bend`                                      |          256MB |
| `type_nodes_v1`           |                                          `()` | `(file_id, bstart, bend, type_id)`                          | `file_id,path,type_id,type_kind,type_name`                                        |          256MB |
| `py_bc_blocks_norm_v1`    |                                          `()` | `(file_id, code_unit_id, bc_offset_start, bc_block_id)`     | `file_id,code_unit_id,bc_block_id,bc_offset_start,bc_offset_end`                  |          256MB |
| `py_bc_cfg_edges_norm_v1` |                                          `()` | `(file_id, code_unit_id, bc_src_block_id, bc_dst_block_id)` | `file_id,code_unit_id,bc_edge_id,bc_edge_kind,bc_src_block_id,bc_dst_block_id`    |          256MB |
| `py_bc_def_use_events_v1` |                                          `()` | `(file_id, code_unit_id, bc_block_id, bc_event_offset)`     | `file_id,code_unit_id,bc_event_id,bc_event_kind,bc_block_id,bc_var_name`          |          256MB |
| `py_bc_reaches_v1`        |                                          `()` | `(file_id, code_unit_id, bc_use_event_id, bc_def_event_id)` | `file_id,code_unit_id,bc_reach_id,bc_reach_kind,bc_use_event_id,bc_def_event_id`  |          256MB |
| `span_errors_v1`          |                                          `()` | `(file_id, bstart, bend, span_error_kind)`                  | `file_id,span_error_id,span_error_kind,bstart,bend`                               |          128MB |
| `diagnostics_norm_v1`     |                                          `()` | `(file_id, diag_line_start, diag_col_start, diag_id)`       | `file_id,diag_id,diag_kind,diag_severity,diag_code,diag_line_start,diag_line_end` |          128MB |

---

## 🧬 C) Final CPG outputs (3)

Here I *do* recommend **moderate-cardinality partitions**, because these tables tend to be huge and commonly filtered by kind.

| Table                               |    Partition By | Cluster (“Z-order”) By                     | Stats Columns                                                | TargetFileSize |
| ----------------------------------- | --------------: | ------------------------------------------ | ------------------------------------------------------------ | -------------: |
| `cpg_nodes` *(from `cpg_nodes_v1`)* |   `(node_kind)` | `(file_id, bstart, node_id)`               | `file_id,path,node_id,node_kind,bstart,bend`                 |          256MB |
| `cpg_edges` *(from `cpg_edges_v1`)* |   `(edge_kind)` | `(path, bstart, src_node_id, dst_node_id)` | `path,edge_id,edge_kind,src_node_id,dst_node_id,bstart,bend` |          256MB |
| `cpg_props` *(from `cpg_props_v1`)* | `(entity_kind)` | `(entity_kind, prop_key, entity_id)`       | `entity_kind,entity_id,prop_key,node_kind`                   |          256MB |

### Optional but high leverage schema tweak (not required for this step)

Consider adding `owner_file_id` to edges + props (from `path → file_id` join) so you can cluster and prune by file much more effectively. Your incremental pipeline already uses `edge_owner_file_id` for the v1 store.

---

## 🧾 D) Metadata / diagnostics outputs (2)

| Table                     | Partition By | Cluster By                       | Stats Columns                           | TargetFileSize |
| ------------------------- | -----------: | -------------------------------- | --------------------------------------- | -------------: |
| `extract_error_artifacts` |         `()` | `(run_id, extractor_name, path)` | `run_id,extractor_name,error_type,path` |           64MB |
| `run_manifest`            |         `()` | `(run_id, started_at)`           | `run_id,started_at,status`              |           32MB |

---

## 🔥 Delta feature gating recommendation (per table class)

Because some Delta protocol features are still evolving across engines, you want these **opt-in** and **explicit**.

* **Default (all tables)**: **no protocol feature enablement**
* **Incremental / mutable tables only** (e.g. “store” tables, or when you later want CDF):

  * enable **Change Data Feed** *only if you truly consume it*
  * consider **Deletion Vectors** only if your reader stack supports it end-to-end

> Delta’s stats property (`delta.dataSkippingStatsColumns`) is standard and safe, and should be used aggressively for pruning. (It is documented in Delta Lake table properties docs.)

---

# 2) PR‑style code changes (Z-order + stats policy + feature gating)

Below is a concrete “patch plan” across the exact files you asked for.

## ✅ Goals

1. **Z-order-like clustering on write** (sort by `(partition + zorder)` before streaming write)
2. **Stats policy**: automatically set `delta.dataSkippingStatsColumns` based on policy + schema presence
3. **Feature gating**:

   * Stop blindly applying feature properties everywhere
   * Only enable protocol/table features if explicitly requested via policy/options
   * Avoid setting feature properties without protocol enablement

---

## A) `src/storage/deltalake/config.py`

### Add: policy fields

* `partition_by`
* `zorder_by`
* `stats_policy` (`"off" | "explicit" | "auto"`)
* `stats_max_columns`
* `enable_features` (tuple of feature names)

### Add: helper to resolve stats columns

```diff
@@
 from dataclasses import dataclass
+from typing import Literal, Sequence

 type StorageOptions = Mapping[str, str]

 DEFAULT_TARGET_FILE_SIZE = 96 * 1024 * 1024

+type DeltaStatsPolicy = Literal["off", "explicit", "auto"]
+type DeltaFeatureName = Literal[
+    "change_data_feed",
+    "deletion_vectors",
+    "row_tracking",
+    "in_commit_timestamps",
+    "column_mapping",
+    "v2_checkpoints",
+]

 @dataclass(frozen=True)
 class DeltaWritePolicy:
     target_file_size: int | None = DEFAULT_TARGET_FILE_SIZE
-    stats_columns: tuple[str, ...] | None = None
+    # layout
+    partition_by: tuple[str, ...] = ()
+    zorder_by: tuple[str, ...] = ()
+    # stats
+    stats_policy: DeltaStatsPolicy = "auto"
+    stats_columns: tuple[str, ...] | None = None      # used when stats_policy="explicit"
+    stats_max_columns: int = 32
+    # feature gating (protocol/table features)
+    enable_features: tuple[DeltaFeatureName, ...] = ()

@@
 def delta_write_configuration(
     write_policy: DeltaWritePolicy,
 ) -> dict[str, str]:
     conf: dict[str, str] = {}
     if write_policy.target_file_size is not None:
         conf["delta.targetFileSize"] = str(write_policy.target_file_size)
-    if write_policy.stats_columns is not None:
+    if write_policy.stats_policy == "explicit" and write_policy.stats_columns is not None:
         conf["delta.dataSkippingStatsColumns"] = ",".join(write_policy.stats_columns)
     return conf

+def resolve_stats_columns(
+    *,
+    policy: DeltaWritePolicy | None,
+    partition_by: Sequence[str] = (),
+    zorder_by: Sequence[str] = (),
+    schema_columns: Sequence[str] | None = None,
+    override: Sequence[str] | None = None,
+) -> tuple[str, ...] | None:
+    # explicit override always wins
+    if override is not None:
+        cols = list(dict.fromkeys(override))
+    elif policy is None or policy.stats_policy == "off":
+        return None
+    elif policy.stats_policy == "explicit":
+        if policy.stats_columns is None:
+            return None
+        cols = list(dict.fromkeys(policy.stats_columns))
+    else:
+        # auto: union(partition_by, zorder_by) capped
+        cols = list(dict.fromkeys([*partition_by, *zorder_by]))
+        cols = cols[: policy.stats_max_columns]
+
+    if schema_columns is not None:
+        schema_set = set(schema_columns)
+        cols = [c for c in cols if c in schema_set]
+    return tuple(cols)
```

---

## B) `src/storage/deltalake/delta.py`

### Fix: default feature properties should not be enabled everywhere

Right now you effectively enable these across all tables by default, which is risky and often ineffective unless protocol enablement is also performed.

```diff
@@
-DEFAULT_DELTA_FEATURE_PROPERTIES: dict[str, str] = {
-    "delta.enableChangeDataFeed": "true",
-    "delta.enableRowTracking": "true",
-    "delta.enableDeletionVectors": "true",
-    "delta.enableInCommitTimestamps": "true",
-}
+# IMPORTANT: keep defaults safe. Protocol/table features must be explicitly enabled.
+DEFAULT_DELTA_FEATURE_PROPERTIES: dict[str, str] = {}
+
+# Optional convenience for opt-in callers (do not auto-apply)
+ADVANCED_DELTA_FEATURE_PROPERTIES: dict[str, str] = {
+    "delta.enableChangeDataFeed": "true",
+    "delta.enableRowTracking": "true",
+    "delta.enableDeletionVectors": "true",
+    "delta.enableInCommitTimestamps": "true",
+}

@@
 def enable_delta_features(
     options: DeltaFeatureMutationOptions,
     *,
     storage: StorageOptions | None = None,
     log_storage: StorageOptions | None = None,
     features: Mapping[str, object],
 ) -> None:
@@
     delta_set_properties(
         request=DeltaSetPropertiesRequest(
             table_uri=options.path,
             storage_options=storage or None,
             version=None,
             timestamp=None,
             properties=properties,
+            gate=options.gate,
             commit_options=DeltaCommitOptions(metadata=dict(options.commit_metadata or {})),
         ),
         log_storage_options=log_storage or None,
     )
```

✅ This ensures set-properties respects your gate when provided.

---

## C) `src/schema_spec/system.py`

This file doesn’t need major logic changes, but it’s a good place to make specs easier to author by allowing dict-style policies (especially if you load from JSON/YAML in tooling).

```diff
@@
 def make_dataset_spec(
     *,
@@
-    delta_write_policy: DeltaWritePolicy | None = None,
+    delta_write_policy: DeltaWritePolicy | Mapping[str, object] | None = None,
@@
 ) -> DatasetSpec:
+    if isinstance(delta_write_policy, Mapping):
+        delta_write_policy = DeltaWritePolicy(**delta_write_policy)
     return DatasetSpec(
@@
         delta_write_policy=delta_write_policy,
@@
     )
```

This lets you define per-table policy blocks declaratively.

---

## D) `src/datafusion_engine/write_pipeline.py`

This is the heart of the change.

### Key changes

1. Parse **policy.partition_by** and **policy.zorder_by** (plus overrides from `format_options`)
2. Apply **sort** before streaming write when:

   * `format == delta`
   * and `zorder_by` is non-empty
   * and (by default) write mode is overwrite-ish (you can choose always)
3. Compute `delta.dataSkippingStatsColumns` via `resolve_stats_columns()` and inject into table properties
4. Feature gating:

   * strip legacy feature property keys from “set properties”
   * call real protocol enablement functions only if requested via policy (`enable_features`)
   * optionally validate if caller tries to set feature props without enabling features

### Patch sketch (diff-like)

```diff
@@
-from storage.deltalake.config import delta_schema_configuration, delta_write_configuration
+from storage.deltalake.config import (
+  delta_schema_configuration,
+  delta_write_configuration,
+  resolve_stats_columns,
+  DeltaWritePolicy,
+)
 from storage.deltalake.delta import (
-  DEFAULT_DELTA_FEATURE_PROPERTIES,
   enable_delta_features,
+  enable_delta_change_data_feed,
+  enable_delta_deletion_vectors,
+  enable_delta_row_tracking,
+  enable_delta_in_commit_timestamps,
 )
+from datafusion.expr import col, SortExpr

@@
 @dataclass(frozen=True)
 class _DeltaPolicyContext:
     table_properties: dict[str, str]
     target_file_size: int | None
+    partition_by: tuple[str, ...]
+    zorder_by: tuple[str, ...]
+    enable_features: tuple[str, ...]
     storage_options: StorageOptions | None
     log_storage_options: StorageOptions | None

@@
 def _delta_policy_context(
     *,
     options: Mapping[str, object],
     dataset_location: DatasetLocation | None,
+    request_partition_by: tuple[str, ...] | None,
+    schema_columns: tuple[str, ...] | None = None,
 ) -> _DeltaPolicyContext:
@@
     write_policy = resolve_delta_write_policy(dataset_location) if dataset_location else None
@@
-    table_properties = _delta_table_properties(options)
+    table_properties = _delta_table_properties(options)   # NOTE: no auto feature props now

+    # resolve layout from (request overrides) or (policy defaults)
+    policy_partition_by = write_policy.partition_by if write_policy else ()
+    partition_by = request_partition_by if request_partition_by is not None else policy_partition_by
+
+    zorder_by = _delta_zorder_by(options) or (write_policy.zorder_by if write_policy else ())
+    enable_features = tuple(_delta_enable_features(options) or (write_policy.enable_features if write_policy else ()))

@@
     if write_policy:
         table_properties.update(delta_write_configuration(write_policy))
@@
     if schema_policy:
         table_properties.update(delta_schema_configuration(schema_policy))
@@
+    # Resolve stats columns (auto by default)
+    stats_override = _delta_stats_columns_override(options)  # e.g. format_options["stats_columns"]
+    resolved_stats = resolve_stats_columns(
+        policy=write_policy,
+        partition_by=partition_by,
+        zorder_by=zorder_by,
+        schema_columns=schema_columns,
+        override=stats_override,
+    )
+    if resolved_stats:
+        table_properties["delta.dataSkippingStatsColumns"] = ",".join(resolved_stats)

     return _DeltaPolicyContext(
         table_properties=table_properties,
         target_file_size=target_file_size,
+        partition_by=tuple(partition_by),
+        zorder_by=tuple(zorder_by),
+        enable_features=enable_features,
         storage_options=storage_options,
         log_storage_options=log_storage_options,
     )

@@
 class WritePipeline:
@@
 def write_via_streaming(...):
@@
     df = self._source_df(request)

+    # --- delta layout prep BEFORE StreamingExecutionResult ---
+    if _is_delta(request.format):
+        schema_cols = tuple(df.schema().names) if hasattr(df, "schema") else None
+        policy_ctx = _delta_policy_context(
+            options=request.format_options or {},
+            dataset_location=dataset_binding.dataset_location,
+            request_partition_by=request.partition_by,
+            schema_columns=schema_cols,
+        )
+        if policy_ctx.zorder_by and request.mode in (WriteMode.OVERWRITE, WriteMode.OVERWRITE_TABLE):
+            sort_cols = list(dict.fromkeys([*policy_ctx.partition_by, *policy_ctx.zorder_by]))
+            order_exprs = [SortExpr(col(c), asc=True, nulls_first=False) for c in sort_cols]
+            df = df.sort(*order_exprs)

     result = StreamingExecutionResult(... df=df ...)

@@
 def _delta_write_spec(...):
-    policy_ctx = _delta_policy_context(options=..., dataset_location=...)
+    schema_cols = tuple(result.df.schema().names) if hasattr(result.df, "schema") else None
+    policy_ctx = _delta_policy_context(
+        options=format_options or {},
+        dataset_location=dataset_binding.dataset_location,
+        request_partition_by=request.partition_by,
+        schema_columns=schema_cols,
+    )
@@
     return DeltaWriteSpec(
         destination=request.destination,
         mode=delta_mode,
         partition_by=policy_ctx.partition_by,
+        zorder_by=policy_ctx.zorder_by,
+        enable_features=policy_ctx.enable_features,
         ...
     )

@@
 def _write_delta(...):
@@
-    enable_delta_features(... features=spec.table_properties ...)
+    # 1) Set non-protocol properties (targetFileSize, statsColumns, etc.)
+    enable_delta_features(... features=spec.table_properties ...)
+
+    # 2) Enable protocol/table features ONLY if requested (feature gating)
+    if "change_data_feed" in spec.enable_features:
+        enable_delta_change_data_feed(...)
+    if "deletion_vectors" in spec.enable_features:
+        enable_delta_deletion_vectors(...)
+    if "row_tracking" in spec.enable_features:
+        enable_delta_row_tracking(...)
+    if "in_commit_timestamps" in spec.enable_features:
+        enable_delta_in_commit_timestamps(...)
```

### Helper parsing functions to add near existing `_delta_*` helpers

* `_delta_zorder_by(options)` → tuple[str,...]
* `_delta_enable_features(options)` → tuple[str,...]
* `_delta_stats_columns_override(options)` → tuple[str,...] | None

These can be simple:

```python
def _delta_zorder_by(options):
    raw = options.get("zorder_by")
    if raw is None:
        return ()
    if isinstance(raw, str):
        return tuple(s.strip() for s in raw.split(",") if s.strip())
    return tuple(raw)
```

---

# ✅ How you’ll apply the table recommendations

### Extract + Normalize tables

You already resolve policies from dataset locations (runtime profile catalogs). So you’ll set:

```python
delta_write_policy = DeltaWritePolicy(
  target_file_size=256*1024*1024,
  zorder_by=("file_id","bstart","node_id"),
  stats_policy="auto",
  partition_by=(),
  enable_features=(),  # opt-in only
)
```

### Final CPG outputs

Because those destinations aren’t currently registered in the dataset registry, the cleanest approach is:

* either **register output dataset locations** in the runtime profile
* or pass `format_options` keys (`zorder_by`, `stats_columns`, `enable_features`) when writing outputs.

(Your new write pipeline support for `format_options["zorder_by"]` etc. makes this trivial.)

---

# 🔚 What’s complete vs what’s optional

✅ **Complete in this response**

* Exact per-table recommendations (partitioning, clustering columns, stats columns, file sizes)
* Concrete PR-style changes in the requested files for:

  * Z-order-like clustering on overwrite writes
  * Automatic stats columns policy
  * Delta feature gating + explicit enablement

🟡 **Optional next improvements (not required but high ROI)**

* Add `owner_file_id` to `cpg_edges` and `cpg_props` (or a derived bucket column) for much stronger file-local pruning
* Implement “true Z-order” by sorting on an interleaved key (small Rust UDF addition)
* Add `v2 checkpoints` enablement if/when your reader stack supports it broadly

---


