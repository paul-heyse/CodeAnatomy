# DataFusion 52 vs 51 — change & feature catalog (deep-dive index)

**Scope:** Core **DataFusion 52.0.0** deltas relative to **51.0.0**, plus the **DataFusion-Python** upgrade implications where the 52 line changes the FFI/extension boundary. ([DataFusion][1])

---

## A) Execution engine performance deltas

1. **CASE evaluation acceleration**

   * Lookup-table-based evaluation for certain `CASE … WHEN <literal> THEN <literal> …` patterns (final work in the CASE perf epic). ([DataFusion][1])
2. **MIN/MAX aggregate dynamic filters (no GROUP BY + filtered aggregate)**

   * Dynamic predicate tightening during execution for `MIN`/`MAX` aggregates with filters (scan pruning improves as bounds tighten). ([DataFusion][1])
3. **Sort-merge join rewrite (“new merge join”)**

   * Large speedups in pathological cases; big improvements called out for TPCH Q21. ([DataFusion][1])
4. **Hash join dynamic filtering upgrade**

   * Pushdown extends from min/max bounds to **build-side hash map contents**; small build sides can become `IN (...)` predicates enabling stats-based pruning. ([DataFusion][1])
5. **IN-LIST performance restoration (regression work)**

   * Specialized `StaticFilters` per datatype to regain performance lost from generic dispatch (tracked/closed in 52 cycle). ([GitHub][2])

---

## B) Dynamic filtering & “sideways information passing” (planner ↔ execution ↔ scans)

1. **Build-side → probe-scan filter propagation (expanded)**

   * HashJoinExec pushes full build-side sets (bounded) into scans; the “small build side → IN expression” path is explicitly described in the 52 release notes. ([DataFusion][1])
2. **Aggregate-driven dynamic filters (MIN/MAX)**

   * Runtime-updated bounds injected into scan predicates for filtered MIN/MAX queries. ([DataFusion][1])

---

## C) Caching, metadata, and listing-table scalability

1. **File statistics cache (new)**

   * New **statistics cache** for file metadata; CLI introspection via `statistics_cache()` is explicitly introduced. ([DataFusion][1])
2. **Prefix-aware list-files cache (new)**

   * Optimizes partition predicate evaluation for Hive-partitioned datasets; CLI introspection via `list_files_cache()`. ([DataFusion][1])
3. **ListingTableProvider now caches object-store `LIST` results (behavior change + knobs)**

   * Cache lifetime = provider lifetime (or TTL); default TTL is none → **staleness until provider drop/recreate**.
   * Config: `datafusion.runtime.list_files_cache_limit`, `datafusion.runtime.list_files_cache_ttl`; disable by setting limit to `0`. ([DataFusion][3])
4. **Known “refresh semantics” footgun (still tracked)**

   * Release cycle discussion notes confusion around “DROP + recreate external table no longer forces LIST refresh” due to session-scoped caching. ([GitHub][4])

---

## D) Scan pushdown architecture (major capability expansion)

1. **Expression evaluation pushdown to scans (TableProvider-level)**

   * Pushes expression evaluation into TableProviders using `PhysicalExprAdapter` (replacing SchemaAdapter-style runtime batch adaptation). ([DataFusion][1])
2. **Sort pushdown to scans**

   * Planner can push sorts into data sources; Parquet scan can reverse file/row-group order to exploit pre-sorted data; large top-K speedups are reported. ([DataFusion][1])
3. **Projection pushdown moved from `FileScanConfig` → `FileSource` (breaking + new helpers)**

   * `FileSource::with_projection` removed → `try_pushdown_projection(&ProjectionExprs) -> Result<Option<...>>`
   * `FileScanConfigBuilder::with_projection_indices` now returns `Result<Self>`
   * `FileSource::create_file_opener` now returns `Result<Arc<dyn FileOpener>>`
   * Helpers: `SplitProjection`, `ProjectionOpener` to split pushdownable vs residual projections. ([DataFusion][3])
4. **Partition column handling moved out of `PhysicalExprAdapter` (breaking)**

   * `FilePruner::try_new()` drops `partition_fields`; partition substitution now done via `replace_columns_with_literals()` *before* adapter rewriting. ([DataFusion][3])
5. **Statistics handling moved from `FileSource` → `FileScanConfig` (breaking)**

   * `FileSource::{with_statistics, statistics}` removed; use `config.statistics()`; stats marked inexact when filters exist for correctness. ([DataFusion][3])

---

## E) TableProvider contract expansions (DML/mutations)

1. **`TableProvider` supports `DELETE` and `UPDATE`**

   * New trait hooks + MemTable implementation; enables storage engines/providers to implement row-level mutation semantics behind SQL `DELETE/UPDATE`. ([DataFusion][1])

---

## F) SQL planner extensibility (dialect & FROM-clause hooks)

1. **`RelationPlanner` (new FROM-clause extension point)**

   * Lets embeddings intercept/plan FROM-clause constructs at any nesting level; complements `ExprPlanner` and `TypePlanner`. ([DataFusion][1])
2. **Extensibility workflow codified (parse → plan → execute)**

   * Concrete extension strategies: rewrite to existing operators (e.g., PIVOT/UNPIVOT → GROUP BY + CASE) vs custom logical+physical (e.g., TABLESAMPLE). ([DataFusion][5])

---

## G) File formats & IO surface additions

1. **Arrow IPC *stream* file support (read path)**

   * DataFusion can read Arrow IPC stream files; `CREATE EXTERNAL TABLE … STORED AS ARROW LOCATION …` shown as canonical SQL shape. ([DataFusion][1])
2. **Parquet pushdown filter representation change (toggleable)**

   * Adaptive filter strategy for pushed-down Parquet filters; can disable via `datafusion.execution.parquet.force_filter_selections = true`. ([DataFusion][3])
3. **Parquet row-filter builder signature simplification (breaking)**

   * `build_row_filter(expr, file_schema, metadata, …)` drops the duplicated schema parameter. ([DataFusion][3])
4. **CSV parsing knob moved (breaking)**

   * `newlines_in_values` moved from `FileScanConfig`/builder to `CsvOptions`. ([DataFusion][3])

---

## H) Operator graph simplification & batch sizing

1. **`CoalesceBatchesExec` removed**

   * Coalescing integrated into operators themselves using Arrow’s coalesce kernel; reduces plan complexity and unblocks other optimizer transforms (e.g., LIMIT pushdown through joins). ([DataFusion][1])

---

## I) UDF / aggregate semantics tightening (planning-time enforcement)

1. **WITHIN GROUP now requires explicit UDAF opt-in**

   * Planner accepts `WITHIN GROUP (ORDER BY …)` only if `AggregateUDFImpl::supports_within_group_clause()` returns `true`. ([DataFusion][3])
2. **Null handling clause opt-in tightened**

   * `AggregateUDFImpl::supports_null_handling_clause` defaults to `false`; `IGNORE NULLS` / `RESPECT NULLS` now error unless explicitly supported. ([DataFusion][3])

---

## J) Core API / IR surface changes (planner & embedding)

1. **DFSchema API returns `&FieldRef` (Arc-clone vs Field clone)**

   * Methods now return references to `FieldRef`; migration is `Arc::clone(df_schema.field("x"))` vs cloning `Field`. ([DataFusion][3])
2. **CacheAccessor API change**

   * Remove API no longer requires a mutable instance. ([DataFusion][3])
3. **Expr struct size reduction (memory/layout micro-opt)**

   * Boxing of specific Expr variants (tracked as a “feature to mention” for 52). ([GitHub][6])
4. **Correctness/regression fixes called out during the 52 cycle (closed issues)**

   * Schema nullability mismatch regression (TPC-DS planning) was closed via #17813. ([GitHub][7])
   * DynamicFilterPhysicalExpr Hash/Eq contract issue closed via #19659. ([GitHub][8])

---

## K) FFI boundary & DataFusion-Python upgrade implications (critical if you expose providers/UDFs via PyCapsule)

1. **`datafusion-ffi` crate updates (Rust side)**

   * Easier conversion to underlying trait objects; **FFI provider constructors now require a `TaskContextProvider`** (commonly `SessionContext`) and optionally a `LogicalExtensionCodec`. ([DataFusion][3])
2. **DataFusion-Python 52.0.0 upgrade guide: extension signature changes**

   * `__datafusion_*_provider__` methods now take an additional `session` parameter used to extract an `FFI_LogicalExtensionCodec`; custom Catalog/Schema/Table/TableFunction providers via FFI must provide **LogicalExtensionCodec + TaskContextProvider**. ([DataFusion][9])
3. **Core crate feature cleanup**

   * `pyarrow` feature flag removed from core DataFusion (migrated to datafusion-python). ([DataFusion][3])

---

## L) “Document build plan” (what we’ll deep-dive next)

For each section A–K, the deep dive can follow a consistent unit template:

* **What changed (52 vs 51)**
* **Why it exists (planner/executor invariant)**
* **Public surface** (trait/method/config/SQL syntax)
* **Plan-shape impact** (logical/physical deltas; what to expect in EXPLAIN)
* **Integration patterns** (Rust embedder vs DataFusion-Python)
* **Footguns / migration notes**
* **Minimal runnable example(s)**


[1]: https://datafusion.apache.org/blog/2026/01/12/datafusion-52.0.0 "Apache DataFusion 52.0.0 Released - Apache DataFusion Blog"
[2]: https://github.com/apache/datafusion/issues/18824 "Restore IN_LIST performance -- Implement specialized `StaticFilters` for different data types · Issue #18824 · apache/datafusion · GitHub"
[3]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[4]: https://github.com/apache/datafusion/issues/19573 "Confusing behavior now required to to refresh the files of a listing table · Issue #19573 · apache/datafusion · GitHub"
[5]: https://datafusion.apache.org/blog/2026/01/12/extending-sql/ "Extending SQL in DataFusion: from ->> to TABLESAMPLE - Apache DataFusion Blog"
[6]: https://github.com/apache/datafusion/issues/18566 "Release DataFusion `52.0.0` (Dec 2025 / Jan 2026) · Issue #18566 · apache/datafusion · GitHub"
[7]: https://github.com/apache/datafusion/issues/17801 "Regression: error planning TPC-DS query: input schema nullability mismatch · Issue #17801 · apache/datafusion · GitHub"
[8]: https://github.com/apache/datafusion/issues/19641 "DynamicFilterPhysicalExpr violates Hash/Eq contract · Issue #19641 · apache/datafusion · GitHub"
[9]: https://datafusion.apache.org/python/user-guide/upgrade-guides.html "Upgrade Guides — Apache Arrow DataFusion  documentation"


# Additional DF52 deltas in deep dive

## L) **Schema plumbing refactor** (fold into D as a dedicated deep dive)

Everything under **D-gap-1** (TableSchema constructor flow, builder signature changes, partition cols migration, FileFormat signature change). ([datafusion.apache.org][3])

## M) **Physical optimizer extension surface** (new deep dive)

`PhysicalOptimizerRule` API change / `optimize_plan` migration, plus how this interacts with DF52’s operator-embedded coalescing and newer physical optimizer rules. ([GitHub][4])

## N) **Math / datetime / type-system “long tail” changes** (optional but real)

These are not headline features, but they are user-visible changes worth cataloging if you want true exhaustiveness:

* allow `log/pow` on negative-scale decimals (bugfix tracked for 52 cycle) ([GitHub][4])
* timezone-related `date_trunc` fast-path adjustment ([GitHub][2])
* RunEndEncoded type coercion addition ([GitHub][2])

## O) **CLI + examples / documentation reshaping** (low priority)

* datafusion-cli help/UX tweaks (example: invalid command help) ([GitHub][2])
* consolidated data IO examples (useful for agent playbooks / “known-good snippets”) ([GitHub][2])

---


[1]: https://datafusion.apache.org/blog/2026/01/12/datafusion-52.0.0 "Apache DataFusion 52.0.0 Released - Apache DataFusion Blog"
[2]: https://raw.githubusercontent.com/apache/datafusion/branch-52/dev/changelog/52.0.0.md "raw.githubusercontent.com"
[3]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[4]: https://github.com/apache/datafusion/issues/18566 "Release DataFusion `52.0.0` (Dec 2025 / Jan 2026) · Issue #18566 · apache/datafusion · GitHub"
[5]: https://docs.rs/datafusion/52.1.0/datafusion/physical_optimizer/trait.PhysicalOptimizerRule.html "https://docs.rs/datafusion/52.1.0/datafusion/physical_optimizer/trait.PhysicalOptimizerRule.html"

## A) Execution engine performance deltas (DataFusion 52 vs 51) — deep dive index + implementation notes (Rust-centric)

**Anchor:** DataFusion 52.0.0 release notes enumerate all five items below under “Performance Improvements,” including **lookup-table CASE**, **MIN/MAX aggregate dynamic filters**, **sort-merge join rewrite**, and **hash join filter pushdown upgrade**. ([DataFusion][1])

---

### A.1 Faster `CASE` evaluation via lookup-table specialization (literal mapping)

**Delta (52 vs 51):** DataFusion 52 adds **lookup-table-based evaluation** for certain `CASE` forms to avoid repeated branch evaluation for “code → label” ETL patterns. ([DataFusion][1])

**Trigger shape (SQL):** “simple CASE” mapping on a single expression with many literal branches:

```sql
SELECT
  CASE company
    WHEN 1 THEN 'Apple'
    WHEN 5 THEN 'Samsung'
    WHEN 2 THEN 'Motorola'
    WHEN 3 THEN 'LG'
    ELSE 'Other'
  END AS company_name
FROM t;
```

(Exact example appears in the 52 release post and the PR rationale.) ([DataFusion][1])

**Mechanics (what changed in the physical expression path):**

* Instead of evaluating each `WHEN` condition sequentially with selection masks, the physical `CASE` expression detects the “lookup-table-like” pattern and **prebuilds a mapping** so each row can be resolved via **key lookup** rather than repeated predicate evaluation. (PR scope: “Implement the case when as a lookup table”.) ([GitHub][2])
* This keeps SQL semantics (short-circuit / laziness) for general `CASE`; the optimization is a **special-case fast path** for a restricted form, not a semantic change. (The broader `CASE` laziness/correctness rationale is documented in the CASE evaluation blog—short-circuiting is “critical for correctness”.) ([DataFusion][3])

**Value proposition (why you care):**

* High-leverage for ETL classification / recoding patterns: “accelerating common ETL patterns.” ([DataFusion][1])
* Reported microbench speedups in the implementation PR: **2.5×–22.5× faster** on the targeted pattern. ([GitHub][2])

**Value at risk / footguns (agent-callouts):**

* **Pattern sensitivity:** only applies to the “lookup-table-like” subset; do not expect wins for “searched CASE” with arbitrary predicates.
* **Type/coercion edge cases:** literal branches require coercion to a common output type; any mismatch shifts you back to the generic evaluation path.
* **Memory vs CPU trade:** building a map costs allocations; net win depends on batch size, cardinality, and branch count.

**Implementation callouts (where to read / what to grep):**

* PR: `perf: optimize CASE WHEN lookup table` (#18183). ([GitHub][2])
* Code hotspot mentioned in PR page: `datafusion/physical-expr/src/expressions/case.rs` (look for lookup-table module/types). ([GitHub][2])

**Rust usage (52 syntax) — no special API required (automatic):**

```rust
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    // register data source (CSV/Parquet/etc.) as usual...
    let df = ctx.sql(r#"
        SELECT CASE company
            WHEN 1 THEN 'Apple'
            WHEN 5 THEN 'Samsung'
            WHEN 2 THEN 'Motorola'
            WHEN 3 THEN 'LG'
            ELSE 'Other'
        END AS company_name
        FROM t
    "#).await?;
    df.show().await?;
    Ok(())
}
```

---

### A.2 `MIN`/`MAX` aggregate dynamic filters (filtered aggregate, no GROUP BY)

**Delta (52 vs 51):** DataFusion 52 synthesizes **runtime “tightening” predicates** for `MIN`/`MAX` aggregates **with filters and no GROUP BY**, pushing them into scans so files/row-groups/rows can be pruned as bounds tighten during execution. ([DataFusion][1])

**Trigger shape (SQL):**

```sql
SELECT min(l_shipdate)
FROM lineitem
WHERE l_returnflag = 'R';
```

52 rewrites the *effective* predicate during execution to include a moving bound:

```sql
-- __current_min updates during execution
WHERE l_returnflag = 'R' AND l_shipdate < __current_min;
```

([DataFusion][1])

**Enable conditions (as implemented):**

* **No grouping** (single global aggregate; no `GROUP BY`). ([GitHub][4])
* Aggregate expressions are **`min`/`max` directly over column references** (e.g., `min(col1)`), possibly multiple such aggregates combined. ([GitHub][4])

**Mechanics (operator/plan wiring):**

* A shared state struct (`AggrDynFilter`) stores the current runtime bounds and is shared across partition streams. ([GitHub][4])
* The dynamic filter is kept in `DataSourceExec` and updated during aggregation execution **after each batch** (current implementation updates every batch; PR notes potential batching to reduce overhead). ([GitHub][4])
* The PR explicitly calls out integration with pushdown plumbing: updates to `AggregateExec`’s `gather_filters_for_pushdown` / `handle_child_pushdown_result` to generate + push filters. ([GitHub][4])

**Value proposition (why you care):**

* Turns “scan everything then aggregate” into “scan until bound converges,” enabling **statistics pruning mid-query** for partitioned Parquet with min/max stats (skipping remaining files once the bound is tight enough). ([GitHub][4])

**Value at risk / correctness hot zone:**

* **Dynamic filter pushdown can cause wrong results if misapplied.** A recent bug report describes incorrect pruning when dynamic filter pushdown + aggregate dynamic filter pushdown are enabled with `target_partitions > 1`. Treat this as a must-test area for production upgrades. ([GitHub][5])
* Mitigation knobs exist (see below): you can disable only aggregate dynamic filters while leaving join/topk dynamic filtering on.

**Knobs (DataFusion 52 config keys):**

* Global gate + per-source gates:

  * `datafusion.optimizer.enable_dynamic_filter_pushdown`
  * `datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown`
  * (also topk/join variants) ([DataFusion][6])

**Rust: enable/disable via `SessionConfig` (52 API):**
`SessionConfig` supports `.set_bool(key, value)` and `.options_mut()` mutation. ([Docs.rs][7])

```rust
use datafusion::prelude::*;
use datafusion_execution::config::SessionConfig;

let config = SessionConfig::new()
    // keep global on; selectively disable aggregate if needed
    .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
    .set_bool("datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown", true);

let ctx = SessionContext::new_with_config(config);
```

([Docs.rs][7])

**Implementation callouts (where to read / what to grep):**

* PR: `optimizer: Support dynamic filter in MIN/MAX aggregates` (#18644). ([GitHub][4])
* Grep terms: `AggrDynFilter`, `init_dynamic_filter`, `gather_filters_for_pushdown`, `handle_child_pushdown_result`.

---

### A.3 Sort-merge join rewrite (“New Merge Join” / SMJ rewrite)

**Delta (52 vs 51):** DataFusion 52 rewrites the **Sort-Merge Join (SMJ)** operator, delivering **orders-of-magnitude speedups** in pathological cases (notably anti-joins) and dramatic improvements on TPC-H Q21 (minutes → milliseconds) while leaving other queries unchanged or modestly faster. ([DataFusion][1])

**Why this mattered (value prop):**

* Comet (Spark acceleration) often relies on SMJ because DataFusion doesn’t provide a larger-than-memory hash join operator; SMJ slowness forced Comet to fall back to Spark. ([GitHub][8])
* Root cause observed: time spent in `concat_batches` producing single-digit-row batches. ([GitHub][8])

**Concrete regression case:** LeftAnti joins / multiple anti-joins were far slower than hash join (example reported: ~10s vs ~60s). ([GitHub][9])

**Mechanics (what changed):**

* PR #18875: **reduce batch concatenation**, use `BatchCoalescer` internally and to buffer final output; removes redundant concatenation, especially for filtered joins. ([GitHub][8])

**Value at risk / tuning:**

* Join algorithm selection still matters:

  * Default optimizer behavior tends to prefer hash join when viable; you can influence this via `datafusion.optimizer.prefer_hash_join`. ([DataFusion][10])
* SMJ still implies sort requirements and can be CPU-heavy if inputs aren’t already ordered; validate plan choice for your workload.

**Knob (force planner to consider SMJ more often):**

* `datafusion.optimizer.prefer_hash_join = false` (lets the physical planner choose SMJ more readily). ([DataFusion][10])

**Rust usage (52) — toggling join preference:**

```rust
use datafusion::prelude::*;
use datafusion_execution::config::SessionConfig;

let config = SessionConfig::new()
    .set_bool("datafusion.optimizer.prefer_hash_join", false);

let ctx = SessionContext::new_with_config(config);

// Example: anti join query likely to exercise SMJ if chosen
let df = ctx.sql(r#"
    SELECT *
    FROM a
    WHERE NOT EXISTS (
      SELECT 1 FROM b WHERE a.k = b.k
    )
"#).await?;
```

([Docs.rs][7])

**Implementation callouts:**

* Issue: “SMJ extremely slow on LeftAnti joins” (#18487). ([GitHub][9])
* PR: “Reduce batch concatenation, use BatchCoalescer … Q21 up to ~4000× faster” (#18875). ([GitHub][8])

---

### A.4 Hash join dynamic filtering upgrade (build-side contents → scan-time pruning)

**Delta (52 vs 51):** DataFusion 52 extends hash join dynamic filtering from min/max bounds to **pushing build-side membership information** into scans:

* If build side is “small,” push an `IN (...)` predicate for **statistics-based pruning**
* Else push a **hash-table-backed membership test** ([DataFusion][1])

**Mechanics (internal strategy selection):**

* `PushdownStrategy` chooses between `InList` and `HashTable` based on build-side size limits; the PR shows the core branch: ([GitHub][11])

  * build empty → `Empty`
  * build small enough → `InList(in_list_values)`
  * otherwise → `HashTable(Arc<hash_map>)`

**Why `IN (...)` is special (value prop):**

* Enables pruning expressions to “explode” `InList` into `col = v1 OR col = v2 ...` when the list has **< 20 items**, improving statistics pruning and allowing bloom-filter checks per value. ([GitHub][11])
* Config docs explicitly call out that InList pushdown can improve pruning and bloom filter usage, but adds build-side copy + memory cost. ([DataFusion][10])

**Key config knobs (DataFusion 52):**

* Enablement:

  * `datafusion.optimizer.enable_dynamic_filter_pushdown` (global)
  * `datafusion.optimizer.enable_join_dynamic_filter_pushdown` (join-only gate) ([DataFusion][6])
* InList thresholds:

  * `datafusion.optimizer.hash_join_inlist_pushdown_max_size` (bytes, per-partition)
  * `datafusion.optimizer.hash_join_inlist_pushdown_max_distinct_values` (deduped row count, per-partition) ([DataFusion][10])

**Value at risk / performance traps:**

* **Memory multiplier:** config docs warn the memory overhead is per-partition (`max_size * target_partitions`) because InList requires copying build-side values. ([DataFusion][10])
* **Plan serialization:** pushing down references to hash tables introduces nontrivial physical-expr serialization concerns; the PR narrative references explicit work around serialization and new expression types (e.g., `HashTableLookupExpr`, protobuf handling). ([GitHub][11])
* **Perf regressions in some workloads:** dynamic filter pushdown is broadly beneficial but not uniformly so; keep knobs surfaced in your embedding (especially if you generate plans for distributed execution).

**Rust: configure for “aggressive but bounded” IN-list pushdown (52 API):**

```rust
use datafusion::prelude::*;
use datafusion_execution::config::SessionConfig;

let config = SessionConfig::new()
    .set_bool("datafusion.optimizer.enable_dynamic_filter_pushdown", true)
    .set_u64("datafusion.optimizer.hash_join_inlist_pushdown_max_size", 128 * 1024)
    .set_usize("datafusion.optimizer.hash_join_inlist_pushdown_max_distinct_values", 150);

let ctx = SessionContext::new_with_config(config);
```

(These keys + semantics are documented; the setter APIs exist on SessionConfig in 52.) ([Docs.rs][7])

**Implementation callouts:**

* Release: “Improved Hash Join Filter Pushdown” section (52 release post). ([DataFusion][1])
* PR/EPIC context: #17171 (hash table pushdown) and #18393 (inlist vs hash table + supporting expressions). ([GitHub][12])

---

### A.5 IN-LIST performance restoration (specialized `StaticFilters` per datatype)

**Delta (52 vs 51):** A regression in `IN (...)` evaluation performance (introduced when making InList set handling generic) is addressed by adding **specialized StaticFilter implementations** to avoid expensive dynamic dispatch and regain prior performance. ([GitHub][13])

**What regressed / why (issue summary):**

* Generic set handling for all types “lost specialization,” slowing down InList; fix is to reintroduce specialized hash-set paths per common scalar family (primitives/boolean/utf8/binary). ([GitHub][13])

**What changed (PR implementation bullets):**

* Adds macros (`primitive_static_filter!`, `define_static_filter!`) to generate specialized filters for many scalar types.
* Routes additional types through `instantiate_static_filter`.
* Refactors `in_list` to use `instantiate_static_filter` rather than defaulting to the generic `ArrayStaticFilter`. ([GitHub][14])

**Why this matters (value prop):**

* IN-list evaluation is a core primitive:

  * directly used by user SQL (`WHERE k IN (...)`)
  * indirectly used by **hash-join dynamic filtering** when build-side values are pushed down as `InList`
* A slow `IN` filter can erase the win from dynamic pruning or join pushdown.

**Value at risk:**

* **Coverage gaps:** specialization targets common scalar types; complex/nested types may remain on generic paths.
* **Bench sensitivity:** improvements depend on distribution (null rate, match rate, list length)—always benchmark on representative data.

**Implementation callouts:**

* Issue: #18824 (restore IN_LIST performance). ([GitHub][13])
* PR: #18832 (specialized InList implementations). ([GitHub][14])

---

## Agent-ready validation checklist (A.1–A.5)

1. **CASE lookup-table path hit-rate**

   * Collect representative queries with “code → label” CASE mapping and compare 51 vs 52 wall time / CPU.
   * Ensure output types are stable (avoid coercion changes that defeat specialization). ([DataFusion][1])

2. **Aggregate dynamic filter correctness**

   * Run MIN/MAX filtered aggregates with `target_partitions > 1` on partitioned Parquet; compare against pushdown-disabled runs.
   * Keep a kill-switch: `datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown=false`. ([DataFusion][6])

3. **SMJ pathological joins**

   * Re-test anti-join heavy workloads (TPCDS-style) and TPC-H Q21; confirm the “concat_batches” hotspot is gone. ([DataFusion][1])

4. **Hash-join pushdown memory bounding**

   * Tune `hash_join_inlist_pushdown_max_size` / `max_distinct_values` relative to `target_partitions` (per-partition memory amplification). ([DataFusion][10])

5. **IN-list microbench regression guard**

   * Add a CI perf test for `IN` filters on your dominant key types (Utf8View/Int64/etc.), because InList speed materially impacts dynamic filter pushdown wins. ([GitHub][13])0

[1]: https://datafusion.apache.org/blog/2026/01/12/datafusion-52.0.0 "Apache DataFusion 52.0.0 Released - Apache DataFusion Blog"
[2]: https://github.com/apache/datafusion/pull/18183 "perf: optimize CASE WHEN lookup table (2.5-22.5 times faster) by rluvaton · Pull Request #18183 · apache/datafusion · GitHub"
[3]: https://datafusion.apache.org/blog/2026/02/02/datafusion_case "Optimizing SQL CASE Expression Evaluation - Apache DataFusion Blog"
[4]: https://github.com/apache/datafusion/pull/18644 "optimizer: Support dynamic filter in `MIN/MAX` aggregates by 2010YOUY01 · Pull Request #18644 · apache/datafusion · GitHub"
[5]: https://github.com/apache/datafusion/issues/20267?utm_source=chatgpt.com "[Bug] Dynamic filter leads to wrong results in some ..."
[6]: https://datafusion.apache.org/_sources/user-guide/configs.md.txt "datafusion.apache.org"
[7]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionConfig.html "SessionConfig in datafusion::execution::context - Rust"
[8]: https://github.com/apache/datafusion/pull/18875 "Sort Merge Join: Reduce batch concatenation, use `BatchCoalescer`, new benchmarks (TPC-H Q21 SMJ up to ~4000x faster) by mbutrovich · Pull Request #18875 · apache/datafusion · GitHub"
[9]: https://github.com/apache/datafusion/issues/18487 "Sort Merge Join is extremely slow on LeftAnti joins · Issue #18487 · apache/datafusion · GitHub"
[10]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[11]: https://github.com/apache/datafusion/pull/18393 "Push down InList or hash table references from HashJoinExec depending on the size of the build side by adriangb · Pull Request #18393 · apache/datafusion · GitHub"
[12]: https://github.com/apache/datafusion/issues/17171 "Push down entire hash table from HashJoinExec into scans · Issue #17171 · apache/datafusion · GitHub"
[13]: https://github.com/apache/datafusion/issues/18824 "Restore IN_LIST performance -- Implement specialized `StaticFilters` for different data types · Issue #18824 · apache/datafusion · GitHub"
[14]: https://github.com/apache/datafusion/pull/18832 "add specialized InList implementations for common scalar types by adriangb · Pull Request #18832 · apache/datafusion · GitHub"

## B) Dynamic filtering & “sideways information passing” — **syntax-first** deltas (DF51 → DF52)

### B.0 Core API you “see” at runtime (DF52)

**Dynamic filter carrier (mutable PhysicalExpr):**

```rust
use std::sync::Arc;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_expr::PhysicalExpr;

// ctor: children are fixed even if inner starts as `true`
let dynf = Arc::new(DynamicFilterPhysicalExpr::new(
    /* children */ vec![ /* Arc<dyn PhysicalExpr> leaf columns */ ],
    /* inner    */ datafusion::physical_expr::expressions::lit(true),
));

// producer updates inner as execution learns more
dynf.update(/* Arc<dyn PhysicalExpr> */)?;
```

`DynamicFilterPhysicalExpr::new(children, inner)` and `.update(new_expr)` are the stable hooks; `.update` requires the new expression’s children to be a subset of the originally provided `children`.

**PhysicalExpr constructors you’ll use to *write* the before/after payloads:**

```rust
use arrow_schema::Schema;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{col, lit, binary, in_list};
use std::sync::Arc;

// col(name, schema) -> Result<Arc<dyn PhysicalExpr>, DataFusionError>
let k = col("k", &schema)?;

// lit(value) -> Arc<dyn PhysicalExpr>
let v = lit(123_i64);

// binary(lhs, op, rhs, schema) -> Result<Arc<dyn PhysicalExpr>, DataFusionError>
let ge = binary(k.clone(), Operator::GtEq, v, &schema)?;

// in_list(expr, list, negated, schema) -> Result<Arc<dyn PhysicalExpr>, DataFusionError>
let isin = in_list(k.clone(), vec![lit(1_i64), lit(5_i64)], &false, &schema)?;
```

Signatures: `col`, `lit`, `binary`, `in_list`.

**Join-membership payload (new in DF52 path):**

```rust
use datafusion::physical_plan::joins::HashTableLookupExpr;
// HashTableLookupExpr::new(hash_expr, hash_map, description) -> HashTableLookupExpr
let ht = Arc::new(HashTableLookupExpr::new(hash_expr, hash_map, "k∈build".to_string()));
dynf.update(ht)?;
```

`HashTableLookupExpr` checks membership of **hash values** in a join hash table; ctor is `new(hash_expr, hash_map, description)`.

---

## B.1 HashJoinExec → probe-side scans: **what was pushed before vs now**

### B.1.1 **Before (DF51)**: push **bounds** (min/max join key range)

Release notes explicitly: “initial implementation passed **min/max values** for the join keys.”
Concrete mental model (dynamic filter blog): if build keys are in `[100, 200]`, probe scan filters out keys outside that range.

**Payload shape (physical expr tree) — generic:**

```rust
// build-side learned: min_k, max_k
let min_k = lit(100_i64);
let max_k = lit(200_i64);

let k    = col("k", &probe_schema)?;
let ge   = binary(k.clone(), Operator::GtEq, min_k, &probe_schema)?; // k >= 100
let le   = binary(k.clone(), Operator::LtEq, max_k, &probe_schema)?; // k <= 200
let rng  = binary(ge, Operator::And, le, &probe_schema)?;            // (k >= 100) AND (k <= 200)

dynf.update(rng)?;
```

**Scan predicate “before vs after execution” (conceptual):**

```text
DF51 plan-time probe scan predicate:
    WHERE <original_probe_filters> AND DynamicFilter[ true ]    // placeholder

DF51 mid-execution probe scan predicate:
    WHERE <original_probe_filters> AND DynamicFilter[ (k >= min_k) AND (k <= max_k) ]
```

Dynamic filter placeholders are expected: you seed `inner` with `true` and later replace it via `.update(...)`.

---

### B.1.2 **Now (DF52)**: push **build-side set membership** (bounded)

Release notes explicitly: DF52 “extends … to pass the **contents of the build side hash map**.”

#### DF52-A: **small build side** ⇒ push **`IN (...)`** (stats-pruning-friendly)

Release notes: when build side contains **20 or fewer rows**, hash map contents are transformed to an `IN` expression and used for **statistics-based pruning**.

**Why 20 matters (not narrative; actual pruning rewrite constraint):** `PredicateRewriter` “does not handle … `InListExpr` greater than 20” and will fall back to `unhandled_hook`. Translation: `IN` lists **≤ 20** are the ones that reliably become min/max-stat pruning predicates.

**Payload shape (physical expr tree) — generic:**

```rust
let k = col("k", &probe_schema)?;

// build-side set (deduped) extracted from join build:
let values = vec![lit(1_i64), lit(5_i64), lit(7_i64)];

let isin = in_list(k.clone(), values, &false, &probe_schema)?; // k IN (1,5,7)
dynf.update(isin)?;
```

**Scan predicate “before vs after execution” (conceptual):**

```text
DF52 plan-time probe scan predicate:
    WHERE <original_probe_filters> AND DynamicFilter[ true ]

DF52 mid-execution probe scan predicate (small build):
    WHERE <original_probe_filters> AND DynamicFilter[ k IN (v1, v2, ..., vN) ]
```

---

#### DF52-B: **large build side** ⇒ push **hash-table membership lookup**

When build side exceeds the “push as IN list” thresholds, DataFusion uses hash table lookups. Config docs: build sides larger than `hash_join_inlist_pushdown_max_size` use hash table lookups; same for exceeding `hash_join_inlist_pushdown_max_distinct_values`.

**Payload shape (physical expr tree) — generic:**

```rust
use datafusion::physical_plan::joins::HashTableLookupExpr;

// join-internal hash expression (produces UInt64 hashes for probe-side join keys)
let hash_expr: Arc<dyn PhysicalExpr> = /* e.g. HashExpr over join key(s) */;

// join build-side hash map
let hash_map: Arc<dyn datafusion::physical_plan::joins::join_hash_map::JoinHashMapType> =
    /* built during HashJoinExec build phase */;

let mem = Arc::new(HashTableLookupExpr::new(hash_expr, hash_map, "join_key∈build".into()));
dynf.update(mem)?;
```

`HashTableLookupExpr` is explicitly “Physical expression that checks if hash values exist in a hash table,” taking `hash_expr` + `hash_map`.

---

### B.1.3 Filter propagation hook points (the explicit “where it gets attached”)

The propagation plumbing is literally the `ExecutionPlan` filter-pushdown protocol:

```rust
fn gather_filters_for_pushdown(
    &self,
    phase: FilterPushdownPhase,
    parent_filters: Vec<Arc<dyn PhysicalExpr>>,
    config: &ConfigOptions,
) -> Result<FilterDescription>;

fn handle_child_pushdown_result(
    &self,
    phase: FilterPushdownPhase,
    child_pushdown_result: ChildPushdownResult,
    config: &ConfigOptions,
) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>>;
```

Docs spell out the canonical chain `FilterExec -> HashJoinExec -> DataSourceExec`, and that `HashJoinExec` may **add its own filters** to push down, while `DataSourceExec` absorbs applicable filters to apply them during scan.

---

### B.1.4 Config surface (DF52; exact keys)

**Enablement (and override semantics):**

```text
datafusion.optimizer.enable_join_dynamic_filter_pushdown      = true
datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown = true
datafusion.optimizer.enable_dynamic_filter_pushdown           = true
  └─ overrides/suppresses per-type toggles when enabled
```

This key set + the “global overrides per-type” rule is explicitly documented.

**Join IN-list vs hash-table thresholds:**

```text
datafusion.optimizer.hash_join_inlist_pushdown_max_size            = <bytes>
datafusion.optimizer.hash_join_inlist_pushdown_max_distinct_values = <rows>
  └─ build sides larger than these use hash-table lookups
```

Documented semantics (including “set to 0 to always use hash table lookups”).

---

## B.2 Aggregate-driven dynamic filters (MIN/MAX): **what changed in predicate terms**

### B.2.1 **Before (DF51)**: no runtime-tightening bound injected into scans

**Conceptual scan predicate:**

```text
DF51:
    WHERE <user_filter_only>
```

(no dynamic “current min/max” bound is introduced at scan time)

---

### B.2.2 **Now (DF52)**: scan predicate = user filter **AND** dynamic bound

Release notes provide the exact semantic rewrite for `MIN` (analogous for `MAX`): DF52 executes:

```sql
WHERE l_returnflag = 'R' AND l_shipdate < __current_min
```

with `__current_min` updated during execution.

**Payload shape (physical expr tree) — generic MIN case:**

```rust
let ship = col("l_shipdate", &schema)?;
let dynf = Arc::new(DynamicFilterPhysicalExpr::new(
    vec![ship.clone()],   // children must include the eventual column refs
    lit(true),            // placeholder; replaced at runtime
));

// later, after each batch (or update point), tighten the bound:
let current_min = lit(/* ScalarValue / timestamp literal */);
let lt = binary(ship.clone(), Operator::Lt, current_min, &schema)?; // l_shipdate < current_min
dynf.update(lt)?;
```

This is exactly the supported model: create with placeholder `true`, then `.update(new_expr)` as execution learns more.

**Payload shape (MAX case):**

```rust
let current_max = lit(/* ... */);
let gt = binary(ship.clone(), Operator::Gt, current_max, &schema)?; // col > current_max
dynf.update(gt)?;
```

**Conceptual scan predicate “before vs after execution”:**

```text
DF52 plan-time:
    WHERE (l_returnflag = 'R') AND DynamicFilter[ true ]

DF52 mid-execution:
    WHERE (l_returnflag = 'R') AND DynamicFilter[ l_shipdate < current_min ]
```

---

### B.2.3 Config keys (DF52; exact)

From the config table:

```text
datafusion.optimizer.enable_aggregate_dynamic_filter_pushdown = true
datafusion.optimizer.enable_dynamic_filter_pushdown           = true  (overrides per-type)
```

Explicitly documented.

---

If you want the next refinement to be even more “agent-executable,” I can turn B.1/B.2 into a strict **“payload registry”**:

* `dynfilter.join.bounds(min,max)` (DF51 legacy)
* `dynfilter.join.inlist(values<=20)` (DF52 small build)
* `dynfilter.join.hash_table(HashTableLookupExpr)` (DF52 large build)
* `dynfilter.aggr.min(col < current_min)` / `dynfilter.aggr.max(col > current_max)` (DF52)

…each with: constructor snippet, update snippet, config gates, and the exact “scan predicate before/after” string form.

## C) Caching, metadata, listing-table scalability (DF52) — **syntax-first** + value/risk callouts

### C.0 Cache surfaces (what exists in DF52, in types + knobs)

#### C.0.1 `CacheManagerConfig` (compile-time / embedder surface)

`CacheManagerConfig` is the *typed* configuration surface that controls which caches exist, their limits, and TTL behavior:

```rust
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use std::time::Duration;

// key fields (all DF52):
// - table_files_statistics_cache: Option<Arc<dyn FileStatisticsCache<Extra = ObjectMeta>>>
// - list_files_cache: Option<Arc<dyn ListFilesCache<Extra = Option<Path>>>>
// - list_files_cache_limit: usize (default 1MiB)
// - list_files_cache_ttl: Option<Duration> (default None = infinite)
// - file_metadata_cache: Option<Arc<dyn FileMetadataCache<Extra = ObjectMeta>>>
// - metadata_cache_limit: usize
let cfg = CacheManagerConfig::default()
    .with_list_files_cache_limit(5 * 1024 * 1024)
    .with_list_files_cache_ttl(Some(Duration::from_secs(30)));
```

Field semantics + defaults: list-files cache limit defaults to **1MiB**, TTL defaults to **None (infinite)**, and list-files cache “won’t see updates until entry expires.” ([Docs.rs][1])

#### C.0.2 `CacheManager` (runtime / introspection surface)

`CacheManager` is the runtime handle that owns these caches and exposes them for inspection:

```rust
use datafusion::execution::cache::cache_manager::{CacheManager, CacheManagerConfig};

let cm = CacheManager::try_new(&cfg)?;

// optional: get caches if configured/enabled
let stats_cache = cm.get_file_statistic_cache(); // Option<Arc<dyn FileStatisticsCache<Extra=ObjectMeta>>>
```

Key signatures: `CacheManager::try_new(&CacheManagerConfig)`, `get_file_statistic_cache() -> Option<Arc<dyn FileStatisticsCache<Extra=ObjectMeta>>>`. ([Docs.rs][2])

#### C.0.3 SQL/CLI introspection (agent-friendly “show me the cache”)

DF52 adds/standardizes CLI table-functions:

* `statistics_cache()` (file statistics cache)
* `list_files_cache()` (list-files cache; includes TTL “expires_in”)
* (existing) `metadata_cache()` (file-embedded metadata cache)

The DF52 release post explicitly demonstrates `statistics_cache()` and `list_files_cache()` and their outputs. ([datafusion.apache.org][3])

---

## C.1 File statistics cache (NEW in DF52): **“avoid repeatedly (re)calculating file statistics”**

### C.1.1 Before vs after (what the engine *does*)

#### Before (DF51): statistics inference is effectively “recompute or re-fetch”

Generic path (conceptual): planning / table creation needs per-file `Statistics` → infer from Parquet metadata / partial reads → repeat across queries / table recreations.

```rust
// DF51-ish conceptual loop (not a literal API):
for file in list_files(table_path).await? {
    let stats = infer_statistics(file).await?;   // repeated across runs
    planner.consume(stats);
}
```

#### After (DF52): `Statistics` cached by `(Path, ObjectMeta)` with change-detection

DF52 introduces `FileStatisticsCache`:

```rust
pub trait FileStatisticsCache:
    CacheAccessor<Path, Arc<Statistics>, Extra = ObjectMeta>
{
    fn list_entries(&self) -> HashMap<Path, FileStatisticsCacheEntry>;
}
```

This is the literal trait definition shape: it extends `CacheAccessor<Path, Arc<Statistics>, Extra=ObjectMeta>` and adds `list_entries()`. ([Docs.rs][4])

Default implementation used by DF52 is `DefaultFileStatisticsCache`, with explicit invalidation semantics:

```rust
pub struct DefaultFileStatisticsCache { /* .. */ }

// key property:
/// Cache is invalided when file size or last modification has changed
```

…and it implements:

* `get(&Path) -> Option<Arc<Statistics>>`
* `get_with_extra(&Path, &ObjectMeta) -> Option<Arc<Statistics>>` (returns `None` if file changed)
  ([Docs.rs][5])

**Operationally (what changes):**

```rust
// DF52 conceptual fast path:
if let Some(stats) = stats_cache.get_with_extra(&path, &object_meta) {
    // reuse (planning-time win)
} else {
    let stats = infer_statistics(file).await?;
    stats_cache.put_with_extra(&path, Arc::new(stats), &object_meta);
}
```

The “key delta” for an agent: **stats are keyed with `ObjectMeta` as Extra**, enabling “same path but changed file” invalidation. ([Docs.rs][4])

### C.1.2 Enablement + observability (explicit DF52 syntax)

#### 1) Ensure stats are actually collected

`statistics_cache()` only shows meaningful content if DataFusion is collecting statistics.

* CLI function doc: `datafusion.execution.collect_statistics` must be enabled for stats to be collected / shown. ([datafusion.apache.org][6])
* DDL doc: DataFusion reads files at table creation to gather statistics by default; you can disable with:

  ```sql
  SET datafusion.execution.collect_statistics = false;
  ```

  ([datafusion.apache.org][7])

#### 2) Inspect the cache (CLI or via `SessionContext.sql`)

Release post demonstration (DF52):

```sql
select * from statistics_cache();
-- columns include:
-- path, file_modified, file_size_bytes, e_tag, version,
-- num_rows, num_columns, table_size_bytes, statistics_size_bytes
```

([datafusion.apache.org][3])

### C.1.3 Value proposition / value at risk

**Value (planning-time, especially remote + many files):**

* DF52 release notes: “new statistics cache … significantly improves planning time for certain queries.” ([datafusion.apache.org][3])
* This particularly matters when:

  * you repeatedly run similar queries,
  * the table has many files,
  * stats inference requires remote reads / Parquet footer decoding.

**Risk profile (freshness / determinism):**

* Statistics cache is *safer than list caching* because it has **change detection** on file size / last modification (invalidates on change). ([Docs.rs][5])
* Determinism improves for repeated plans within a session because stats don’t jitter across repeated inference runs, but correctness remains tied to the invalidation inputs (`ObjectMeta` quality).

---

## C.2 Prefix-aware list-files cache (NEW behavior in DF52): **partition predicates without extra LIST**

### C.2.1 The explicit DF52 “prefix-aware” capability (real method signature)

`DefaultListFilesCache` (DF52) is prefix-aware *at lookup time*:

```rust
pub fn new(memory_limit: usize, ttl: Option<Duration>) -> DefaultListFilesCache;

// critical lookup:
fn get_with_extra(
    &self,
    table_scoped_path: &TableScopedPath,
    prefix: &Option<Path>,
) -> Option<Arc<Vec<ObjectMeta>>>;

// Extra = Option<Path>
type Extra = Option<Path>;
```

* It caches by `TableScopedPath` (table base) and supports `prefix: Option<Path>` for partition filtering. ([Docs.rs][8])
* It uses LRU eviction and respects `memory_limit`; TTL is optional. ([Docs.rs][8])

The implementation contract is explicitly described:

* fetch cache entry for `table_base`
* if `prefix` is `Some`, filter results to files matching `table_base/prefix`
* return filtered results
  This is stated verbatim in the rustdoc. ([Docs.rs][8])

### C.2.2 Before vs after (LIST call pattern)

#### Before (DF51): partition predicate ⇒ repeated LIST per prefix

Hive partitioned table:

```
/table_root/theme=base/type=infrastructure/...
/table_root/theme=buildings/type=.../...
...
```

Conceptual DF51 behavior:

```rust
// DF51-ish conceptual:
let all = object_store.list("/table_root/").await?;

// for query predicate theme='base'
let base_only = object_store.list("/table_root/theme=base/").await?; // extra LIST
```

#### After (DF52): single LIST ⇒ cached ⇒ filtered by prefix (no extra LIST)

DF52 behavior:

```rust
// First: LIST once, cache under TableScopedPath(table_root)
list_cache.put(&table_scoped_path, Arc::new(all_object_meta));

// Later: partition predicate theme='base' yields prefix = Some("theme=base/")
let filtered = list_cache.get_with_extra(&table_scoped_path, &Some(prefix_path))
    .expect("cache hit; filtered locally");
```

This is exactly the “prefix-aware cache lookup” contract described for `get_with_extra`. ([Docs.rs][8])

### C.2.3 “Proof by release post” (explicit SQL)

DF52 release post provides the canonical demonstration:

```sql
CREATE EXTERNAL TABLE overturemaps
STORED AS PARQUET LOCATION 's3://overturemaps-us-west-2/release/2025-12-17.0/';

-- Partition predicate (hive partition columns)
select count(*) from overturemaps where theme='base';
-- … “without requiring another LIST call”
```

([datafusion.apache.org][3])

### C.2.4 Observability (list-files cache table function)

DF52 release post shows how to inspect cached listings:

```sql
select table, path, metadata_size_bytes, expires_in,
       unnest(metadata_list)['file_size_bytes'] as file_size_bytes,
       unnest(metadata_list)['e_tag'] as e_tag
from list_files_cache()
limit 10;
```

([datafusion.apache.org][3])

---

## C.3 ListingTableProvider LIST caching (DF52): **behavior change + knobs + determinism trade**

This is the “big user-visible semantic shift” called out in the **DF52 upgrade guide**.

### C.3.1 Before vs after (explicit behavior statement)

#### Before (≤ DF51)

`ListingTableProvider` issues `LIST` calls “each time it needed to list files for a query.” ([datafusion.apache.org][9])

#### After (DF52)

`ListingTableProvider` caches the results of `LIST`:

* lifetime: **ListingTableProvider instance lifetime** *or* until TTL expiry
* default TTL: **no expiration**
* consequence: underlying object store file additions/removals are **not observed** until provider is dropped/recreated (or TTL expires) ([datafusion.apache.org][9])

### C.3.2 Runtime knobs (exact keys + disable semantics)

Upgrade guide (DF52) lists:

* `datafusion.runtime.list_files_cache_limit` (bytes)
* `datafusion.runtime.list_files_cache_ttl` (TTL)
* Disable by setting limit to **0**:

  ```sql
  SET datafusion.runtime.list_files_cache_limit TO "0K";
  ```

([datafusion.apache.org][9])

Config docs specify defaults + units:

* `datafusion.runtime.list_files_cache_limit` default **1M**
* `datafusion.runtime.list_files_cache_ttl` default **NULL** (no TTL), supports `m` and `s` units ([datafusion.apache.org][10])

### C.3.3 Determinism vs freshness (what changes in practice)

* **Determinism/stability increase:** default TTL is infinite “to ensure stability” (explicitly stated in the issue that introduced runtime configurability). ([GitHub][11])
* **Freshness risk / value at risk:** default infinite TTL means “new files won’t be seen” until provider drop/recreate. ([datafusion.apache.org][9])

Agent takeaway: caching **trades remote object-store latency + LIST variability** for **staleness** unless you set TTL or disable caching.

---

## C.4 Known “refresh semantics” footgun (DF52): DROP+recreate no longer forces LIST refresh

### C.4.1 The explicit before/after sequence (straight from the issue)

The DF52 cycle introduced a **session/global LIST cache** (benefit: “visible / reportable / aligned with other session scoped caches”) but it creates a refresh problem. ([GitHub][12])

The issue provides an explicit SQL reproduction:

```sql
-- calls LIST to get the files
create external table foo ...;
drop table foo;

-- DF52: reuses cached file list (previously would call LIST again)
create external table foo ...;
```

And it states the key semantic shift:

* Previously the cache was **local to each ListingTable** ⇒ recreated on each `CREATE EXTERNAL TABLE` ⇒ user could force refresh by recreating the table.
* Now the cached list can be reused across drop/recreate in the same session, which is “pretty confusing.” ([GitHub][12])

### C.4.2 Mitigation patterns (explicit DF52 syntax)

**Mitigation A (freshness): set TTL**

```sql
SET datafusion.runtime.list_files_cache_ttl = '30s';
-- then CREATE EXTERNAL TABLE ...
```

TTL exists + semantics documented. ([datafusion.apache.org][10])

**Mitigation B (force LIST always): disable list-files cache**

```sql
SET datafusion.runtime.list_files_cache_limit TO '0K';
```

Disable semantics explicitly documented. ([datafusion.apache.org][9])

**Mitigation C (hard reset): drop/recreate the provider/session**
If you need “fresh LIST now” and can’t tolerate TTL delay, the upgrade guide states DF52 will not see changes “until the ListingTableProvider instance is dropped and recreated.” ([datafusion.apache.org][9])

---

## C.5 Implementation callouts (what an LLM agent should do in code)

### C.5.1 If you are embedding DataFusion: expose these knobs as first-class controls

Minimum recommended control surface for agent-driven systems:

* `datafusion.execution.collect_statistics` (whether stats exist at all) ([datafusion.apache.org][7])
* `datafusion.runtime.list_files_cache_limit` (0 disables) ([datafusion.apache.org][9])
* `datafusion.runtime.list_files_cache_ttl` (freshness vs stability) ([datafusion.apache.org][10])

### C.5.2 If you are extending/replicating ListingTable behavior: use prefix-aware `get_with_extra`

The “prefix-aware” contract is the concrete optimization that makes hive partition predicates cheap after one LIST:

```rust
// contract (from rustdoc):
// - cache entry key: table_base
// - extra/prefix: relative prefix under table_base
// - returns filtered list without storage calls
let metas = list_cache.get_with_extra(&table_scoped_path, &Some(prefix));
```

This is not optional “nice to have”; it is the *mechanism* behind “partition predicate evaluation without another LIST call.” ([Docs.rs][8])

### C.5.3 Use the CLI table-functions as automated regression probes

For agent-driven debugging, treat these as “assertable state”:

```sql
select sum(statistics_size_bytes) from statistics_cache();
select table, path, expires_in from list_files_cache();
```

They are explicitly documented and demonstrated in DF52 release + CLI docs. ([datafusion.apache.org][3])

[1]: https://docs.rs/datafusion/latest/datafusion/execution/cache/cache_manager/struct.CacheManagerConfig.html "CacheManagerConfig in datafusion::execution::cache::cache_manager - Rust"
[2]: https://docs.rs/datafusion/latest/datafusion/execution/cache/cache_manager/struct.CacheManager.html "CacheManager in datafusion::execution::cache::cache_manager - Rust"
[3]: https://datafusion.apache.org/blog/2026/01/12/datafusion-52.0.0 "Apache DataFusion 52.0.0 Released - Apache DataFusion Blog"
[4]: https://docs.rs/datafusion/latest/datafusion/execution/cache/cache_manager/trait.FileStatisticsCache.html "FileStatisticsCache in datafusion::execution::cache::cache_manager - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/execution/cache/cache_unit/struct.DefaultFileStatisticsCache.html "DefaultFileStatisticsCache in datafusion::execution::cache::cache_unit - Rust"
[6]: https://datafusion.apache.org/user-guide/cli/functions.html "CLI Specific Functions — Apache DataFusion  documentation"
[7]: https://datafusion.apache.org/user-guide/sql/ddl.html?utm_source=chatgpt.com "DDL — Apache DataFusion documentation"
[8]: https://docs.rs/datafusion/latest/datafusion/execution/cache/struct.DefaultListFilesCache.html "DefaultListFilesCache in datafusion::execution::cache - Rust"
[9]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[10]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[11]: https://github.com/apache/datafusion/issues/19056?utm_source=chatgpt.com "Add a way to dynamically configure / update the ..."
[12]: https://github.com/apache/datafusion/issues/19573?utm_source=chatgpt.com "Confusing behavior now required to to refresh the files of a ..."

## D) Scan pushdown architecture (DF52) — **syntax-first** + value/risk callouts

> **Release anchor (DF52):** “Expression evaluation pushdown to scans” + “Sort pushdown to scans” are first-class DF52 features; sort pushdown cites ~30× gains on pre-sorted data. ([Apache Git Repositories][1])

---

### D.0 Scan pushdown surfaces (DF52: what exists, in signatures)

#### D.0.1 `PhysicalExprAdapter` (new “rewrite expressions to file schema” core primitive)

```rust
pub trait PhysicalExprAdapter: Send + Sync + Debug {
    fn rewrite(
        &self,
        expr: Arc<dyn PhysicalExpr>,
    ) -> Result<Arc<dyn PhysicalExpr>, DataFusionError>;
}
```

This is the DF52 canonical mechanism for adapting pushed-down predicates/projections to the **physical file schema** (schema evolution, missing cols, type mismatches). ([Docs.rs][2])

**Partition-value substitution (now explicit, separate step):**

```rust
pub fn replace_columns_with_literals<K, V>(
    expr: Arc<dyn PhysicalExpr>,
    replacements: &HashMap<K, V>,
) -> Result<Arc<dyn PhysicalExpr>, DataFusionError>
where
    K: Borrow<str> + Eq + Hash,
    V: Borrow<ScalarValue>;
```

Used for partition column replacement *before* adapter rewriting. ([Docs.rs][3])

---

#### D.0.2 `FileSource` (DF52: projection + sort pushdown hooks live here)

Key DF52 trait surface (selected methods):

```rust
pub trait FileSource: Send + Sync {
    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>, DataFusionError>;

    fn try_pushdown_projection(
        &self,
        projection: &ProjectionExprs,
    ) -> Result<Option<Arc<dyn FileSource>>, DataFusionError>;

    fn try_reverse_output(
        &self,
        order: &[PhysicalSortExpr],
        eq_properties: &EquivalenceProperties,
    ) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>, DataFusionError>;
}
```

`create_file_opener` returning `Result<…>` is now the required signature. ([Docs.rs][4])
`try_reverse_output` defines Exact/Inexact/Unsupported ordering outcomes. ([Docs.rs][4])

---

#### D.0.3 `ProjectionExprs` (DF52 projection payload type)

Column-index convenience (for “simple projection” builders):

```rust
pub fn from_indices(indices: &[usize], schema: &Schema) -> ProjectionExprs
```

([Docs.rs][5])

---

#### D.0.4 Helpers for “partial projection pushdown” (CSV-like sources)

**SplitProjection**: split a `ProjectionExprs` into file indices + remainder projection

```rust
pub struct SplitProjection {
    pub source: ProjectionExprs,
    pub file_indices: Vec<usize>,
    /* + partition_value_indices + remapped remainder (private) */
}

pub fn new(logical_file_schema: &Schema, projection: &ProjectionExprs) -> Self
```

([Docs.rs][6])

**ProjectionOpener**: wrapper that applies remainder projection + partition columns

```rust
pub struct ProjectionOpener { /* … */ }

pub fn try_new(
    projection: SplitProjection,
    inner: Arc<dyn FileOpener>,
    file_schema: &Schema,
) -> Result<Arc<dyn FileOpener>>
```

([Docs.rs][7])

---

## D.1 Expression evaluation pushdown to scans (TableProvider-level)

### D.1.1 Before → After (payload shape)

**Before (SchemaAdapter era):** “read → adapt batches” (SchemaMapping/SchemaMapper mapped `RecordBatch` and stats). DF52 marks this path as removed and tells you to use `PhysicalExprAdapterFactory` instead. ([Docs.rs][8])

**After (DF52):** “adapt the *expression* to each file schema”:

* pushed-down filter/projection is a `PhysicalExpr`
* per file, rewrite it to the file’s **physical schema**
* evaluation happens in the scan/reader using the rewritten expression ([Docs.rs][2])

**Why it matters (value):**

* Enables *per-file* expression specialization (“customized for each individual file schema”), unlocking more aggressive optimization (e.g., schema evolution, format-specific rewrites). ([Apache Git Repositories][1])
* Reduces CPU/memory by avoiding “post-read batch adaptation” and enabling earlier pruning/pushdown (value composes with filters/projections/sorts).

---

### D.1.2 DF52 syntax: implement an adapter (minimal “missing column default” pattern)

```rust
use std::sync::Arc;
use datafusion_physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_common::{Result, ScalarValue, tree_node::{Transformed, TreeNode}};
use datafusion_physical_expr::expressions::{self, Column};
use arrow_schema::SchemaRef;

#[derive(Debug)]
struct MyAdapter {
    physical_file_schema: SchemaRef,
}

impl PhysicalExprAdapter for MyAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        expr.transform(|e| {
            if let Some(c) = e.as_any().downcast_ref::<Column>() {
                if self.physical_file_schema.index_of(c.name()).is_err() {
                    // fill missing physical column with default instead of NULL
                    return Ok(Transformed::yes(expressions::lit(ScalarValue::Int32(Some(0)))));
                }
            }
            Ok(Transformed::no(e))
        }).data()
    }
}

#[derive(Debug)]
struct MyAdapterFactory;

impl PhysicalExprAdapterFactory for MyAdapterFactory {
    fn create(&self, _logical_file_schema: SchemaRef, physical_file_schema: SchemaRef)
        -> Arc<dyn PhysicalExprAdapter>
    {
        Arc::new(MyAdapter { physical_file_schema })
    }
}
```

The `rewrite(...) -> Result<Arc<dyn PhysicalExpr>>` signature is the DF52 contract; factory `create(...) -> Arc<dyn PhysicalExprAdapter>` is demonstrated in the docs. ([Docs.rs][2])

---

### D.1.3 Wiring: where the adapter lives (DF52)

* **Expressions are rewritten in file scans**; you attach a `PhysicalExprAdapterFactory` to the scan config (`FileScanConfig` carries an `expr_adapter_factory`). ([Docs.rs][9])
* Legacy schema-adapter hooks on `FileSource` are deprecated and explicitly tell you to migrate to `PhysicalExprAdapterFactory`. ([Docs.rs][4])

**Value at risk:** incorrect rewrite semantics = wrong results (this is a “semantic compiler” component). Treat adapter changes like optimizer rule changes: unit tests + golden EXPLAIN + file-schema-matrix tests.

---

## D.2 Sort pushdown to scans (planner → FileSource `try_reverse_output`)

### D.2.1 Before → After (plan/operator shape)

**Before (DF51 typical):**

```text
DataSourceExec(file scan) -> SortExec(order_by=…) -> LimitExec(topk)
```

**After (DF52):**

* Planner can ask the source to satisfy or approximate required ordering.
* If the source can produce requested order (or an optimized approximation), SortExec can be reduced/eliminated in some cases and TopK can prune earlier. ([Apache Git Repositories][1])

### D.2.2 DF52 syntax: the pushdown hook and result contract

```rust
fn try_reverse_output(
    &self,
    order: &[PhysicalSortExpr],
    eq_properties: &EquivalenceProperties,
) -> Result<SortOrderPushdownResult<Arc<dyn FileSource>>, DataFusionError>;
```

Return semantics:

* `Exact` / `Inexact` / `Unsupported` ([Docs.rs][4])

**Concrete example (from trait docs):**

```text
File ordering: [a ASC, b DESC]
Requested:     [a DESC]
Reversed file: [a DESC, b ASC]
Result: prefix match → Inexact
```

([Docs.rs][4])

**Phase-1 capability summary (reverse-scan optimization)**

* Reverse scan when requested sort is reverse of natural ordering (read row groups in reverse order)
* Prefix matching support (ordering transforms still satisfy requested prefix) ([GitHub][10])

**Value proposition:** DF52 reports ~30× speedup on benchmarks with pre-sorted data. ([Apache Git Repositories][1])

### D.2.3 Determinism & correctness considerations

* **Inexact** means “optimized for ordering but not perfectly sorted” — downstream may still enforce full sort if required for correctness. ([Docs.rs][4])
* Ordering metadata must remain consistent under projection/rewrite; DF52 upgrade surfaced regressions where projection handling didn’t preserve ordering metadata (see §D.3 “value at risk”). ([GitHub][11])

---

## D.3 Projection pushdown moved from `FileScanConfig` → `FileSource` (breaking)

### D.3.1 Before → After (API deltas)

**Before (≤ DF51):**

* `FileScanConfig` stored projection info (`projection_exprs`)
* `FileSource::with_projection(&self, config: &FileScanConfig) -> Arc<dyn FileSource>`

**After (DF52):**

* `FileSource::with_projection` removed
* replaced with:

```rust
fn try_pushdown_projection(
    &self,
    projection: &ProjectionExprs,
) -> Result<Option<Arc<dyn FileSource>>, DataFusionError>;
```

* projections are stored in `FileSource`, not `FileScanConfig` ([Apache DataFusion][12])

**Builder break:**

* `FileScanConfigBuilder::with_projection_indices` now returns `Result<Self>` (projection pushdown can fail). ([Apache DataFusion][12])

**Opener break:**

* `FileSource::create_file_opener` returns `Result<Arc<dyn FileOpener>>` (was infallible). ([Apache DataFusion][12])

**Value proposition:** format-specific projection pushdown is now a first-class capability (Parquet: struct field access; Vortex: computed expressions into undecoded data). ([Apache DataFusion][12])

---

### D.3.2 DF52 syntax: “simple column-only pushdown” vs “split + remainder”

#### Case 1: Source can push down the whole projection

```rust
let projection = ProjectionExprs::from_indices(&[0, 3, 5], &table_schema)?;
let new_source = source.try_pushdown_projection(&projection)?;
```

`ProjectionExprs::from_indices` is the convenience builder; `try_pushdown_projection` is the new hook. ([Docs.rs][5])

#### Case 2: Source only supports column indices (CSV-like) ⇒ use SplitProjection + ProjectionOpener

```rust
use datafusion_datasource::projection::{SplitProjection, ProjectionOpener};

fn try_pushdown_projection(
    &self,
    projection: &ProjectionExprs,
) -> Result<Option<Arc<dyn FileSource>>, DataFusionError> {
    // 1) split: file_indices + remainder projection (also computes partition handling)
    let split = SplitProjection::new(self.logical_file_schema(), projection);

    // 2) store split in the new FileSource so create_file_opener can use it
    Ok(Some(Arc::new(self.clone_with_split(split))))
}

fn create_file_opener(
    &self,
    object_store: Arc<dyn ObjectStore>,
    base_config: &FileScanConfig,
    partition: usize,
) -> Result<Arc<dyn FileOpener>, DataFusionError> {
    // a) create inner opener that reads ONLY file_indices
    let inner: Arc<dyn FileOpener> = self.create_simple_index_opener(object_store, &self.split.file_indices)?;

    // b) wrap: apply remainder projection + partition columns
    let opener = ProjectionOpener::try_new(self.split.clone(), inner, base_config.file_schema())?;
    Ok(opener)
}
```

* `SplitProjection` defines `file_indices` and the remainder mapping rules. ([Docs.rs][6])
* `ProjectionOpener::try_new(...) -> Result<Arc<dyn FileOpener>>` is the DF52 wrapper. ([Docs.rs][7])
* `create_file_opener` must return `Result<…>` in DF52. ([Docs.rs][4])

---

### D.3.3 Value at risk (real upgrade footguns)

Projection pushdown now affects **plan properties** (ordering / equivalence / cached properties). Two upgrade-cycle regressions to be aware of:

* Projection pushdown can drop equivalence properties / cached plan properties if it recreates `DataSourceExec` improperly (discussion points at `FileScanConfig::try_swapping_with_projection` losing cached properties). ([GitHub][13])
* Pre-sorted data being re-sorted after projection due to failing to apply projection to existing orderings (DF52 regression report). ([GitHub][11])

Agent action: if you implement `try_pushdown_projection`, ensure you also correctly **project/rewrite ordering metadata** and any cached equivalence properties.

---

## D.4 Partition column handling moved out of `PhysicalExprAdapter` (breaking)

### D.4.1 Before → After (explicit signature + behavior)

**FilePruner signature (migration guide):**

```rust
// Before:
let pruner = FilePruner::try_new(predicate, file_schema, partition_fields, file_stats)?;

// After:
let pruner = FilePruner::try_new(predicate, file_schema, file_stats)?;
```

([Apache DataFusion][12])

**Partition column replacement is now explicit:**

```rust
// Before: adapter handled partition replacement internally
let adapted_expr = adapter.rewrite(expr)?;

// After:
let expr_with_literals = replace_columns_with_literals(expr, &partition_values)?;
let adapted_expr = adapter.rewrite(expr_with_literals)?;
```

([Apache DataFusion][12])

### D.4.2 Value proposition / risk

* **Value:** adapter focuses on schema differences; partition substitution becomes explicit and reusable (also useful for constant folding / default fills). ([Apache DataFusion][12])
* **Risk:** forgetting literal substitution means pruning and pushdown predicates that reference partition columns can silently degrade (no pruning) or mis-evaluate.

---

## D.5 Statistics handling moved from `FileSource` → `FileScanConfig` (breaking + correctness rule)

### D.5.1 Before → After (API delta)

Upgrade guide explicitly removes:

```rust
with_statistics(&self, statistics: Statistics) -> Arc<dyn FileSource>
statistics(&self) -> Result<Statistics>
```

and replaces access:

```diff
- let stats = config.file_source.statistics()?;
+ let stats = config.statistics();
```

([Apache DataFusion][12])

### D.5.2 DF52 correctness rule: `FileScanConfig::statistics()` marks stats inexact when filters exist

DF52 doc string:

```rust
pub fn statistics(&self) -> Statistics
// “Returns the unprojected table statistics, marking them as inexact if filters are present.”
```

Rationale: if filters are pushed down (pruning predicates / bloom filters), row counts become unknown, so stats must not be claimed exact. ([Docs.rs][9])

**Value:** consistent, correct stats semantics across formats; avoids downstream planner incorrectly trusting filtered stats.
**Risk:** if your custom `FileSource` previously stored “projected stats,” migrate that state into `FileScanConfig` (or accept unknown/inexact).

---

## D.6 Implementation checklist (what an LLM programming agent should actually change)

1. **Stop using SchemaAdapter APIs**

   * Remove SchemaAdapterFactory usage; migrate to `PhysicalExprAdapterFactory` + `PhysicalExprAdapter::rewrite`. ([Docs.rs][2])

2. **Projection pushdown**

   * Implement `FileSource::try_pushdown_projection(&ProjectionExprs) -> Result<Option<Arc<dyn FileSource>>>`. ([Docs.rs][4])
   * If you only support indices: use `SplitProjection` + `ProjectionOpener::try_new(...)`. ([Docs.rs][6])
   * Update any call sites: `.with_projection_indices(..)?` because it now returns `Result<Self>`. ([Apache DataFusion][12])

3. **Sort pushdown**

   * Implement `try_reverse_output(...) -> SortOrderPushdownResult` and use file ordering metadata + `EquivalenceProperties`. ([Docs.rs][4])
   * Add regression tests for “projection + ordering preservation” (known DF52 pain point). ([GitHub][11])

4. **Partition columns**

   * Update `FilePruner::try_new` call sites (drop `partition_fields`). ([Apache DataFusion][12])
   * Always run `replace_columns_with_literals` before adapter rewrite. ([Docs.rs][3])

5. **Statistics**

   * Remove `FileSource::{with_statistics, statistics}` overrides; read via `config.statistics()` and expect “inexact when filters exist.” ([Apache DataFusion][12])

If you want the same “before/after payload” treatment for **filters pushdown** under the new adapter (including Parquet `build_row_filter` single-schema change), I can extend D.* with an explicit “predicate pipeline” section (logical Expr → PhysicalExpr → replace partition cols → adapter rewrite → pruning predicate → row filter).

[1]: https://apache.googlesource.com/datafusion-site/%2B/refs/heads/asf-staging/blog/feeds/pmc.atom.xml?utm_source=chatgpt.com "blog/feeds/pmc.atom.xml - datafusion-site - Git at Google"
[2]: https://docs.rs/datafusion/latest/datafusion/physical_expr_adapter/trait.PhysicalExprAdapter.html "PhysicalExprAdapter in datafusion::physical_expr_adapter - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/physical_expr_adapter/fn.replace_columns_with_literals.html "replace_columns_with_literals in datafusion::physical_expr_adapter - Rust"
[4]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/trait.FileSource.html "FileSource in datafusion::datasource::physical_plan - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/physical_expr/projection/struct.ProjectionExprs.html?utm_source=chatgpt.com "ProjectionExprs in datafusion::physical_expr::projection"
[6]: https://docs.rs/datafusion-datasource/52.1.0/x86_64-unknown-linux-gnu/datafusion_datasource/projection/struct.SplitProjection.html "SplitProjection in datafusion_datasource::projection - Rust"
[7]: https://docs.rs/datafusion-datasource/52.1.0/x86_64-unknown-linux-gnu/datafusion_datasource/projection/struct.ProjectionOpener.html "ProjectionOpener in datafusion_datasource::projection - Rust"
[8]: https://docs.rs/datafusion/latest/datafusion/datasource/schema_adapter/struct.SchemaMapping.html?utm_source=chatgpt.com "SchemaMapping in datafusion::datasource::schema_adapter"
[9]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.FileScanConfig.html?utm_source=chatgpt.com "FileScanConfig in datafusion::datasource::physical_plan"
[10]: https://github.com/apache/datafusion/issues/19329?utm_source=chatgpt.com "Phase 2: make sort pushdown support exact mode which ..."
[11]: https://github.com/apache/datafusion/issues/20173?utm_source=chatgpt.com "Existing file ordering is lost in some cases with projections"
[12]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[13]: https://github.com/apache/datafusion/issues/17077?utm_source=chatgpt.com "Equivalence properties lost if projection is pushed down in ..."

## E) TableProvider contract expansions (DML / mutations) (DF52) — **syntax-first** + value/risk callouts

> **Release anchor (DF52):** `TableProvider` now has hooks for `DELETE` + `UPDATE`, and `MemTable` implements them. ([apache.googlesource.com][1])

---

### E.0 Contract delta (DF51 → DF52): *from “read/insert only” → “row-level mutate hooks”*

#### E.0.1 Before vs After (trait surface)

**Before (≤ DF51):** `DELETE` existed as a logical concept (`Dml(Delete)`), but execution errored (“Unsupported logical plan: Dml(Delete).”) and there was no `TableProvider` hook. ([GitHub][2])

**After (DF52):** `TableProvider` adds two provided async methods:

```rust
fn delete_from<'a>(
  &'a self,
  state: &'a dyn Session,
  filters: Vec<Expr>,
) -> Future<Result<Arc<dyn ExecutionPlan>>>;

fn update<'a>(
  &'a self,
  state: &'a dyn Session,
  assignments: Vec<(String, Expr)>,
  filters: Vec<Expr>,
) -> Future<Result<Arc<dyn ExecutionPlan>>>;
```

Semantics: both return an `ExecutionPlan` that produces **one row** with a `UInt64` column named **`count`**; empty `filters` means “apply to all rows” (delete-all / update-all). ([Docs.rs][3])

#### E.0.2 Logical plan carrier: `LogicalPlan::Dml(DmlStatement)` + `WriteOp`

DML statements are represented as:

* `LogicalPlan::Dml(DmlStatement)` ([Docs.rs][4])
* `WriteOp::{Insert(..), Delete, Update, Ctas}` ([Docs.rs][5])

`DmlStatement` explicitly models `INSERT/DELETE/UPDATE/CTAS`, and ties them to `TableProvider::{insert_into, delete_from, update}`. ([Docs.rs][6])
Its constructor sets the output schema to a single `count` column. ([Docs.rs][6])

**Value prop:** DF52 gives storage engines a *first-class* “mutate” integration point (row deletion + in-place update semantics) without requiring downstreams to fork/replace the planner. ([apache.googlesource.com][1])

---

## E.1 `DELETE` (SQL → LogicalPlan → TableProvider::delete_from → count)

### E.1.1 SQL surface (DF52 parser/planner constraints)

`DELETE` is accepted, but several clauses are explicitly rejected:

```text
DELETE <TABLE>        -- not supported
USING ...             -- not supported
RETURNING ...         -- not yet supported
ORDER BY ...          -- not yet supported
LIMIT ...             -- not yet supported
```

These are enforced in the SQL planner’s `Statement::Delete` arm. ([Docs.rs][7])

### E.1.2 Planner lowering: `DELETE FROM t WHERE p` → `LogicalPlan::Dml(WriteOp::Delete, input=Filter(Scan(t)))`

This is literally how DF52 builds the plan:

```rust
// scan target table
let scan = LogicalPlanBuilder::scan(table_ref.clone(), Arc::clone(&table_source), None)?.build()?;

// optional WHERE predicate → Filter(scan)
let source = match predicate_expr {
  None => scan,
  Some(predicate_expr) => {
    let filter_expr = self.sql_to_expr(predicate_expr, &schema, &mut planner_context)?;
    LogicalPlan::Filter(Filter::try_new(filter_expr, Arc::new(scan))?)
  }
};

// DML wrapper
LogicalPlan::Dml(DmlStatement::new(
  table_ref,
  table_source,
  WriteOp::Delete,
  Arc::new(source),
))
```

(See `delete_to_plan`.) ([Docs.rs][7])

### E.1.3 Provider hook contract (what you implement)

`TableProvider::delete_from(state, filters)` contract is:

* “Delete rows matching the filter predicates”
* Returns `ExecutionPlan` producing single row `{ count: UInt64 }`
* Empty `filters` ⇒ delete all rows ([Docs.rs][3])

**Release-post example (DF52):**

```sql
DELETE FROM mem_table WHERE status = 'obsolete';
```

([apache.googlesource.com][1])

### E.1.4 Value at risk (correctness / safety)

**Hard footgun (by design):** empty `filters` means “delete all rows.” ([Docs.rs][3])

**Known DF52 bug pattern:** if your provider supports exact filter pushdown, filters may get pushed into `TableScan` and *not* be extracted for `delete_from`, resulting in `filters = []` and therefore delete-all. This is reported in #19840 with a concrete explanation of the failure mode. ([GitHub][8])

**Provider-side mitigation pattern (recommended for engines exposed to LLM agents):**

* Treat `filters.is_empty()` as **dangerous** unless explicitly authorized (session knob / provider option).
* Optionally require a “force full-table delete” flag; otherwise return an error even though DF semantics allow it.

---

## E.2 `UPDATE` (SQL → assignment rewrite → TableProvider::update → count)

### E.2.1 SQL surface constraints (DF52 planner)

DF52 parses `UPDATE`, but rejects several modifiers:

```text
UPDATE ... SET ... FROM <multiple tables>  -- not yet supported (only 0/1)
UPDATE ... RETURNING ...                  -- not yet supported
UPDATE ... ON CONFLICT ...                -- not supported
UPDATE ... LIMIT ...                      -- not supported
```

Enforced in the `Statement::Update` arm. ([Docs.rs][7])

### E.2.2 Assignment extraction + validation (syntax → typed logical expressions)

`update_to_plan` explicitly:

* maps each SQL `Assignment` to `(col_name, SQLExpr)` and validates the target column exists in the target schema ([Docs.rs][7])
* builds a scan (and joins the optional `FROM` table, if provided)
* applies optional `WHERE` predicate as a `Filter` node ([Docs.rs][7])

### E.2.3 “Overwrite with assignment expressions” = per-column expression synthesis

DF52 constructs a full set of output expressions for the target schema:

* If a column is assigned: convert RHS SQL expr → `Expr`, patch placeholder typing to the target field, then `cast_to(target_type)` ([Docs.rs][7])
* Else: use the existing column (qualified by table alias if present) ([Docs.rs][7])
* Alias every expression back to the original field name (`expr.alias(field.name())`) ([Docs.rs][7])

This is the core semantics encoded in:

```rust
// Build updated values for each column, using previous value if not modified
match assign_map.remove(field.name()) {
  Some(new_value_sql) => {
    let mut expr = self.sql_to_expr(new_value_sql, source.schema(), &mut planner_context)?;
    if let Expr::Placeholder(p) = &mut expr { p.field = p.field.take().or_else(|| Some(Arc::clone(field))); }
    expr.cast_to(field.data_type(), source.schema())?
  }
  None => Expr::Column(...),
}
.alias(field.name())
```

([Docs.rs][7])

### E.2.4 Provider hook contract (what you implement)

`TableProvider::update(state, assignments, filters)` contract is:

* `assignments: Vec<(String, Expr)>` (column name + logical expression)
* `filters: Vec<Expr>` (predicate list; empty ⇒ update all rows)
* returns `ExecutionPlan` that outputs one `{ count: UInt64 }` row ([Docs.rs][3])

**Value prop:** update expressions are planned with proper typing/casting (planner enforces column existence and casts RHS to target type), reducing downstream engine “stringly-typed update” errors. ([Docs.rs][7])

**Value at risk:** UPDATE is inherently side-effecting; determinism depends on storage engine commit semantics, concurrency control, and how you interpret `Expr` over rows.

---

## E.3 `MemTable` as the reference implementation (what it implies for engine authors)

* `MemTable` is explicitly listed as implementing `TableProvider` and provides `delete_from` + `update` in DF52. ([Docs.rs][9])
* Internal mutability model: partitions are held under `RwLock<Vec<RecordBatch>>` (per partition), meaning mutations are lock-mediated (expect write contention / blocking reads during updates/deletes in naive implementations). ([Docs.rs][9])
* `MemTable` has optional pre-known sort order, and the docs warn incorrect results if the data isn’t actually sorted; inserting “removes the order” (mutations generally should be treated as invalidating ordering guarantees). ([Docs.rs][9])

---

## E.4 Implementation blueprint (custom TableProvider: minimal, correct-by-contract)

### E.4.1 Required outputs: return a “count plan”

Contract requirement: one `UInt64 count` row. ([Docs.rs][3])

```rust
fn make_count_plan(n: u64) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
    // Build a single RecordBatch: schema { count: UInt64 } rows: [n]
    // Return as a 1-partition MemoryExec (or your own ExecPlan).
    todo!()
}
```

### E.4.2 Delete hook skeleton (with safety guard)

```rust
#[async_trait]
impl TableProvider for MyProvider {
    async fn delete_from(
        &self,
        state: &dyn Session,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {

        // Guard recommended for agent-facing engines:
        if filters.is_empty() && !self.allow_full_table_delete(state) {
            return Err(DataFusionError::Execution("refusing full-table delete".into()));
        }

        let affected: u64 = self.engine_delete(state, &filters).await?;
        make_count_plan(affected)
    }
}
```

**Why the guard matters:** DF52 has an open bug where filters may not be passed to `delete_from` under pushdown, leading to delete-all. ([GitHub][8])

### E.4.3 Update hook skeleton (typed assignments + filters)

```rust
#[async_trait]
impl TableProvider for MyProvider {
    async fn update(
        &self,
        state: &dyn Session,
        assignments: Vec<(String, Expr)>,
        filters: Vec<Expr>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {

        if filters.is_empty() && !self.allow_full_table_update(state) {
            return Err(DataFusionError::Execution("refusing full-table update".into()));
        }

        // Interpret Expr using your engine rules:
        // - translate Expr → engine predicate / update expression
        // - enforce atomicity / idempotence if needed
        let affected: u64 = self.engine_update(state, &assignments, &filters).await?;
        make_count_plan(affected)
    }
}
```

---

## E.5 Roadmap note (what DF52 enables, what remains)

DF52 provides the `TableProvider` hooks for `DELETE` and `UPDATE`; the “Complete DML Support” epic tracks remaining surface like `MERGE INTO`, `INSERT OVERWRITE`, and `TRUNCATE TABLE` (currently expressible as `DELETE WHERE true`). ([GitHub][10])

[1]: https://apache.googlesource.com/datafusion-site/%2B/refs/heads/asf-staging/blog/feeds/pmc.atom.xml "blog/feeds/pmc.atom.xml - datafusion-site - Git at Google"
[2]: https://github.com/apache/datafusion/issues/12406?utm_source=chatgpt.com "How to support deletes? · Issue #12406 · apache/datafusion"
[3]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html "TableProvider in datafusion::datasource - Rust"
[4]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.LogicalPlan.html "LogicalPlan in datafusion::logical_expr - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/logical_expr/enum.WriteOp.html "WriteOp in datafusion::logical_expr - Rust"
[6]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.DmlStatement.html "DmlStatement in datafusion::logical_expr - Rust"
[7]: https://docs.rs/crate/datafusion-sql/latest/source/src/statement.rs "datafusion-sql 52.1.0 - Docs.rs"
[8]: https://github.com/apache/datafusion/issues/19840 "`TableProvider::delete_from` problem with pushed down filters · Issue #19840 · apache/datafusion · GitHub"
[9]: https://docs.rs/datafusion/latest/datafusion/datasource/struct.MemTable.html "MemTable in datafusion::datasource - Rust"
[10]: https://github.com/apache/datafusion/issues/19617?utm_source=chatgpt.com "[EPIC] Complete DML Support (MERGE, INSERT ..."

## F) SQL planner extensibility (dialect + FROM-clause hooks) (DF52) — **syntax-first** + value/risk callouts

> **Doc anchor:** DF52 formalizes a 3-stage pipeline (**parse → SqlToRel → LogicalPlan**) and explicitly positions **ExprPlanner / TypePlanner / RelationPlanner** as interceptors inside **SqlToRel**. ([Apache DataFusion][1])

---

### F.0 Extension pipeline (parse → plan → execute) — explicit objects + insertion points

#### F.0.1 Parser + planner primitives (crate surfaces)

`datafusion_sql` explicitly exposes:

* `DFParser`: SQL string → `Statement` (AST)
* `SqlToRel`: `Statement` → `LogicalPlan`
* (unparser) `Expr`/`LogicalPlan` → SQL ([Docs.rs][2])

```rust
use datafusion_sql::{DFParser, sqlparser};
use datafusion_sql::planner::SqlToRel;

// parse
let stmts: Vec<sqlparser::ast::Statement> =
    DFParser::parse_sql("SELECT 1")?;

// plan (requires a ContextProvider; SessionState provides one internally)
let planner: SqlToRel<'_, /* ContextProvider */ _> = /* ... */;
let plan = planner.sql_statement_to_plan(stmts[0].clone())?;
```

`SqlToRel` is “binder + mechanical translation” (name/type resolution via `ContextProvider`, then AST→LogicalPlan), and **does not** do optimization (optimizer runs later). ([Docs.rs][3])

#### F.0.2 Extension planners (what DF52 standardizes)

DF52’s “Extending SQL Syntax” page lists the three extension traits + where you register them:

* `ExprPlanner`: custom operators/expressions → `ctx.register_expr_planner(...)`
* `TypePlanner`: custom SQL types → `SessionStateBuilder::with_type_planner(...)`
* `RelationPlanner`: custom FROM relations (table factors) → `ctx.register_relation_planner(...)` ([Apache DataFusion][1])

---

## F.1 RelationPlanner (DF52) — FROM-clause extension at **any nesting level**

### F.1.1 Contract (trait + return types)

#### Trait surface (what you implement)

```rust
use datafusion::logical_expr::planner::{RelationPlanner, RelationPlannerContext, RelationPlanning};
use datafusion_sql::sqlparser::ast::TableFactor;

pub trait RelationPlanner: std::fmt::Debug + Send + Sync {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning, datafusion::error::DataFusionError>;
}
```

This is the exact DF52 signature: input is a **sqlparser** `TableFactor` (FROM element), output is `RelationPlanning`. ([Docs.rs][4])

#### Return protocol (short-circuit vs delegate)

```rust
pub enum RelationPlanning {
    Planned(PlannedRelation),
    Original(TableFactor),
}
```

* `Planned(..)` = you handled it → stop; use your `LogicalPlan`
* `Original(..)` = you didn’t → next planner (or default DataFusion) handles it ([Docs.rs][5])

#### Planned payload (what you must construct)

```rust
pub struct PlannedRelation {
    pub plan: datafusion_expr::logical_plan::LogicalPlan,
    pub alias: Option<datafusion_sql::sqlparser::ast::TableAlias>,
}

impl PlannedRelation {
    pub fn new(plan: LogicalPlan, alias: Option<TableAlias>) -> PlannedRelation { /* ... */ }
}
```

So RelationPlanner returns a **(LogicalPlan, optional alias)** tuple. ([Docs.rs][6])

---

### F.1.2 Planner context utilities (what DF52 gives you to avoid reinventing SqlToRel)

`RelationPlannerContext` required methods (all DF52):

```rust
pub trait RelationPlannerContext {
    fn context_provider(&self) -> &dyn ContextProvider;

    fn plan(&mut self, relation: TableFactor) -> Result<LogicalPlan>;

    fn sql_to_expr(&mut self, expr: sqlparser::ast::Expr, schema: &DFSchema) -> Result<datafusion_expr::Expr>;

    fn sql_expr_to_logical_expr(&mut self, expr: sqlparser::ast::Expr, schema: &DFSchema) -> Result<datafusion_expr::Expr>;

    fn normalize_ident(&self, ident: sqlparser::ast::Ident) -> String;

    fn object_name_to_table_reference(&self, name: sqlparser::ast::ObjectName) -> Result<TableReference>;
}
```

Key semantics:

* `plan(relation)` re-enters the **full relation planning pipeline**, starting from the first registered relation planner (i.e., recursion for nested relations is “correct by construction”). ([Docs.rs][7])

---

### F.1.3 Registration + precedence (embedding API)

#### Register on `SessionContext` (runtime/session-level)

```rust
use datafusion::prelude::*;
use std::sync::Arc;

let ctx = SessionContext::new();
ctx.register_relation_planner(Arc::new(MyRelationPlanner))?;
```

* Relation planners run in **reverse registration order** (“newer wins”). ([Docs.rs][8])

#### Or set planners on `SessionStateBuilder` (state-level)

```rust
use datafusion::execution::session_state::SessionStateBuilder;

let state = SessionStateBuilder::new()
    .with_default_features()
    .with_relation_planners(vec![Arc::new(MyRelationPlanner)])
    .build();
```

`with_relation_planners(Vec<Arc<dyn RelationPlanner>>)` is the typed builder hook. ([Docs.rs][9])

---

### F.1.4 DF51 workaround vs DF52 hook — **what changed**

#### Before (pre-RelationPlanner): AST rewrite or root-only transformations

DataFusion’s 2024 architecture paper documents the classic pattern: “rewrite the SQL AST prior to calling the SQL planner.” ([andrew.nerdnetworks.org][10])

In practice, many embeddings used a **root-only** workaround: rewrite the top-level relation in a custom planner pass. DF52’s RelationPlanner proposal explicitly calls out the limitation:

> “only work at query root: custom relations inside JOINs, CTEs, or subqueries aren't transformed.” ([GitHub][11])

#### After (DF52): TableFactor interception at **any nesting level**

RelationPlanner is designed to intercept **table factor planning** “at any nesting level” and chain multiple planners. ([GitHub][11])

**Value proposition**

* Enables dialect features that are syntactically “FROM-ish” (PIVOT/UNPIVOT/TABLESAMPLE/MATCH_RECOGNIZE) without forking SqlToRel. ([Apache DataFusion][1])
* Eliminates “root-only” rewrites → correctness + composability across nested queries.

**Value at risk**

* You are inserting custom LogicalPlan fragments during binding; mistakes in aliasing / identifier normalization can create subtle semantic bugs. `PlannedRelation` carries `alias` explicitly—if you drop/ignore it, downstream column resolution may break. ([Docs.rs][6])

---

## F.2 Implementation strategies — rewrite vs custom logical+physical nodes (DF52 codified)

DataFusion’s DF52 guide explicitly codifies two approaches for RelationPlanner:

1. **Rewrite to standard SQL / standard LogicalPlan operators**
2. **Custom logical + custom physical** (UserDefinedLogicalNode + ExecutionPlan) ([Apache DataFusion][1])

---

### F.2.1 Strategy A — rewrite to standard operators (PIVOT / UNPIVOT)

#### “Before → After” in SQL (explicit rewrite target)

The DF52 example documents:

**PIVOT**

```sql
-- Original
SELECT * FROM sales
PIVOT (SUM(amount) FOR quarter IN ('Q1','Q2'))

-- Rewritten
SELECT region,
  SUM(CASE quarter WHEN 'Q1' THEN amount END) AS Q1,
  SUM(CASE quarter WHEN 'Q2' THEN amount END) AS Q2
FROM sales
GROUP BY region
```

**UNPIVOT**

```sql
-- Original
SELECT * FROM wide UNPIVOT (sales FOR quarter IN (q1, q2))

-- Rewritten
SELECT region, 'q1' AS quarter, q1 AS sales FROM wide
UNION ALL
SELECT region, 'q2' AS quarter, q2 AS sales FROM wide
```

(From the DF52 `pivot_unpivot.rs` example.) ([GitHub][12])

#### Actual DF52 RelationPlanner rewrite (core skeleton)

The example implements:

```rust
impl RelationPlanner for PivotUnpivotPlanner {
    fn plan_relation(&self, relation: TableFactor, ctx: &mut dyn RelationPlannerContext)
      -> Result<RelationPlanning>
    {
        match relation {
          TableFactor::Pivot { table, aggregate_functions, value_column, value_source, alias, .. } =>
              plan_pivot(ctx, *table, &aggregate_functions, &value_column, value_source, alias),
          TableFactor::Unpivot { table, value, name, columns, null_inclusion, alias } =>
              plan_unpivot(ctx, *table, &value, name, &columns, null_inclusion.as_ref(), alias),
          other => Ok(RelationPlanning::Original(Box::new(other))),
        }
    }
}
```

And the rewrite body is **pure LogicalPlanBuilder + Expr construction**:

* PIVOT: builds `case(col(pivot_col)).when(value, agg_input).end()?` then wraps in aggregate and calls `.aggregate(group_by_cols, pivot_exprs)` ([GitHub][12])
* UNPIVOT: builds one `.project(...)` per source column, then folds `.union(...)`, then optional `.filter(value_col.is_not_null())` for EXCLUDE NULLS default ([GitHub][12])

**Value proposition**

* Uses only built-in operators → existing optimizer + physical planner remain fully applicable; lowest “surface area at risk.” ([Apache DataFusion][1])

**Value at risk**

* Rewrite coverage constraints become your “dialect contract” (example explicitly rejects “Dynamic PIVOT (ANY/Subquery)”). ([GitHub][12])

---

### F.2.2 Strategy B — custom logical + custom physical (TABLESAMPLE)

DF52’s `table_sample.rs` example is explicitly “full pipeline from parsing to execution,” demonstrating:

1. **Parse** TABLESAMPLE via RelationPlanner
2. **Plan** as a `UserDefinedLogicalNode` (`TableSamplePlanNode`)
3. **Execute** via custom physical operator (`SampleExec`) ([GitHub][13])

#### DF52 RelationPlanner interception (TABLESAMPLE lives on `TableFactor::Table.sample`)

The example matches the `TableFactor::Table { sample: Some(sample), ... }` shape, removes the sample to plan the base table, then returns either:

* `LIMIT` plan for `(N ROWS)` or `(N>=1.0)`
* custom `TableSamplePlanNode` for Bernoulli / fraction sampling ([GitHub][13])

```rust
let input = context.plan(base_relation_without_sample)?;
let quantity_value_expr = context.sql_to_expr(quantity.value, input.schema())?;
match quantity.unit {
  Some(TableSampleUnit::Rows) => LogicalPlanBuilder::from(input).limit(0, Some(rows as usize))?.build()?,
  Some(TableSampleUnit::Percent) => TableSamplePlanNode::new(input, percent / 100.0, seed).into_plan(),
  None => { /* fraction vs row-limit split */ }
}
```

([GitHub][13])

#### Custom logical node payload (what gets embedded into `LogicalPlan::Extension`)

```rust
#[derive(Debug, Clone, Hash, Eq, PartialEq, PartialOrd)]
struct TableSamplePlanNode { input: LogicalPlan, lower_bound: ..., upper_bound: ..., seed: u64 }

impl TableSamplePlanNode {
  fn into_plan(self) -> LogicalPlan {
    LogicalPlan::Extension(Extension { node: Arc::new(self) })
  }
}

impl UserDefinedLogicalNodeCore for TableSamplePlanNode {
  fn name(&self) -> &str { "TableSample" }
  fn inputs(&self) -> Vec<&LogicalPlan> { vec![&self.input] }
  fn schema(&self) -> &DFSchemaRef { self.input.schema() }
  fn fmt_for_explain(&self, f: &mut Formatter) -> fmt::Result { /* emits bounds + seed */ }
  fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, mut inputs: Vec<LogicalPlan>) -> Result<Self> { /* ... */ }
}
```

([GitHub][13])

#### Determinism callout (sampling must define reproducibility)

The example uses `REPEATABLE(seed)` and explicitly relies on it for deterministic snapshot testing. ([GitHub][13])

**Value proposition**

* Enables semantics that are not “just a rewrite,” especially when execution semantics matter (randomness, streaming, distribution). ([GitHub][13])

**Value at risk**

* You must own logical-node invariants + physical execution invariants (partitioning, ordering, reproducibility). DF52’s own TABLESAMPLE example includes explicit method validation and unsupported branches to constrain semantics. ([GitHub][13])

---

## F.3 Dialect knobs + ExprPlanner / TypePlanner interplay (for “dialect & FROM” completeness)

### F.3.1 Dialect selection (parser surface)

Extending SQL Syntax shows enabling Postgres dialect via config:

```rust
let config = SessionConfig::new()
    .set_str("datafusion.sql_parser.dialect", "postgres");
let mut ctx = SessionContext::new_with_config(config);
```

…and then registering an `ExprPlanner` to map `->` into `Operator::StringConcat`. ([Apache DataFusion][1])

### F.3.2 ExprPlanner + TypePlanner signatures (DF52)

`ExprPlanner` is a “provided-method” trait; key method example:

```rust
fn plan_binary_op(
  &self,
  expr: RawBinaryExpr,
  _schema: &DFSchema,
) -> Result<PlannerResult<RawBinaryExpr>, DataFusionError>
```

([Docs.rs][14])

`TypePlanner` signature:

```rust
fn plan_type(
  &self,
  _sql_type: &sqlparser::ast::DataType,
) -> Result<Option<arrow_schema::DataType>, DataFusionError>
```

([Docs.rs][15])

---

## F.4 Agent-executable checklist (what to implement / where to hook)

1. **Decide the extension surface**

* Expression/operator: `ExprPlanner`
* Type mapping: `TypePlanner`
* FROM/table-factor: `RelationPlanner` ([Apache DataFusion][1])

2. **Implement RelationPlanner**

* Match on `sqlparser::ast::TableFactor` variants
* Use `ctx.plan(relation)` to recurse
* Use `ctx.sql_to_expr(..)` to convert SQL expressions
* Return `RelationPlanning::Planned(PlannedRelation::new(plan, alias))` ([Docs.rs][4])

3. **Choose strategy**

* Rewrite-only → emit standard `LogicalPlanBuilder` nodes (PIVOT/UNPIVOT example)
* Custom nodes → implement `UserDefinedLogicalNodeCore` + physical planning/execution (TABLESAMPLE example) ([Apache DataFusion][1])

4. **Register + precedence**

* `ctx.register_relation_planner(...)` (reverse registration order)
* Or `SessionStateBuilder::with_relation_planners(...)`
* Return `Original(...)` to delegate ([Docs.rs][8])

If you want the next increment in this same style, the natural follow-on is to deep-dive the **custom physical planning** side for Strategy B (how `UserDefinedLogicalNode` gets planned via an `ExtensionPlanner` into an `ExecutionPlan`, how to preserve `PlanProperties`, and what to do for serialization).

[1]: https://datafusion.apache.org/library-user-guide/extending-sql.html "Extending SQL Syntax — Apache DataFusion  documentation"
[2]: https://docs.rs/datafusion-sql "datafusion_sql - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/sql/planner/struct.SqlToRel.html?utm_source=chatgpt.com "SqlToRel in datafusion::sql::planner - Rust - Docs.rs"
[4]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/trait.RelationPlanner.html "RelationPlanner in datafusion::logical_expr::planner - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/enum.RelationPlanning.html "RelationPlanning in datafusion::logical_expr::planner - Rust"
[6]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/struct.PlannedRelation.html "PlannedRelation in datafusion::logical_expr::planner - Rust"
[7]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/trait.RelationPlannerContext.html "RelationPlannerContext in datafusion::logical_expr::planner - Rust"
[8]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html "SessionContext in datafusion::execution::context - Rust"
[9]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionStateBuilder.html "SessionStateBuilder in datafusion::execution::session_state - Rust"
[10]: https://andrew.nerdnetworks.org/pdf/SIGMOD-2024-lamb.pdf "Apache Arrow DataFusion: a Fast, Embeddable, Modular Analytic Query Engine"
[11]: https://github.com/apache/datafusion/issues/18078 "Relation Planner Extension API · Issue #18078 · apache/datafusion · GitHub"
[12]: https://raw.githubusercontent.com/apache/datafusion/main/datafusion-examples/examples/relation_planner/pivot_unpivot.rs "raw.githubusercontent.com"
[13]: https://raw.githubusercontent.com/apache/datafusion/main/datafusion-examples/examples/relation_planner/table_sample.rs "raw.githubusercontent.com"
[14]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/trait.ExprPlanner.html "ExprPlanner in datafusion::logical_expr::planner - Rust"
[15]: https://docs.rs/datafusion/latest/datafusion/logical_expr/planner/trait.TypePlanner.html "TypePlanner in datafusion::logical_expr::planner - Rust"

## G) File formats & IO surface additions (DF52) — **syntax-first** + value/risk callouts

---

### G.0 “What exists” in DF52 (formats + knobs you actually touch)

#### G.0.1 `CREATE EXTERNAL TABLE … STORED AS <file_type>` includes `ARROW`

DataFusion’s DDL grammar explicitly lists `file_type` ∈ `{CSV, ARROW, PARQUET, AVRO, JSON}`. ([Apache DataFusion][1])

```sql
CREATE EXTERNAL TABLE t
STORED AS ARROW
LOCATION '/path/or/object_store/foo.arrow';
```

---

#### G.0.2 Parquet late materialization (“filter pushdown”) knobs

Parquet “pushdown_filters” is **off by default**; “force_filter_selections” defaults false and controls the **representation** of filter results. ([Apache DataFusion][2])

```sql
-- enable late materialization (“filter pushdown”) for parquet scans
SET datafusion.execution.parquet.pushdown_filters = true;

-- DF52/Arrow 57.1+ adaptive selection vs bitmap; this forces legacy RowSelection-only
SET datafusion.execution.parquet.force_filter_selections = true;
```

---

### G.1 Arrow IPC **stream** file support (read path)

#### G.1.1 Before → After (capability delta)

**Before (≤ DF51)**: Arrow IPC *file* format works; Arrow IPC *stream* format fails because it lacks a footer (“file does not contain correct footer”). ([GitHub][3])

```rust
// DF51: Arrow IPC FILE works
ctx.register_arrow("my_table", "file.arrow", ArrowReadOptions::default()).await?;

// DF51: Arrow IPC STREAM fails (no footer)
ctx.register_arrow("my_stream", "file.stream.arrow", ArrowReadOptions::default()).await?;
// -> ArrowError(ParseError("Arrow file does not contain correct footer"), ...)
```

([GitHub][3])

**After (DF52)**: Arrow IPC *stream* files are supported as a first-class data source. ([Apache DataFusion][4])

```sql
-- Canonical SQL shape shown in DF52 release post:
CREATE EXTERNAL TABLE ipc_events
STORED AS ARROW
LOCATION 's3://bucket/events.arrow';
```

([Apache DataFusion][4])

#### G.1.2 “Stream vs File” semantics (why this matters)

Arrow defines two IPC encodings:

* **Streaming format**: must be processed start→end, **no random access**
* **File format**: supports random access (seek/mmap) ([Apache Arrow][5])

This changes IO/perf expectations:

* Stream is ideal for “append-only event streams / micro-batch dumps”
* File format is better for mmap/seek heavy patterns (e.g., interactive sampling / repeated scans) ([Apache Arrow][5])

#### G.1.3 DF52 Rust surface (the stable APIs you call)

`SessionContext` provides both “read into DataFrame” and “register as table” APIs for ARROW sources: ([Docs.rs][6])

```rust
// DataFrame API
let df = ctx.read_arrow("events.arrow", ArrowReadOptions::default()).await?;

// SQL API via registration
ctx.register_arrow("events", "events.arrow", ArrowReadOptions::default()).await?;
let out = ctx.sql("SELECT count(*) FROM events").await?.collect().await?;
```

([Docs.rs][6])

#### G.1.4 Value proposition / value at risk

* **Value**: removes a conversion step for producers emitting Arrow streams directly; interop is explicitly called out as the motivator in DF52 release notes. ([Apache DataFusion][4])
* **Risk**: stream format forbids random access; any workflow relying on seek/mmap behavior should prefer IPC *file* format. ([Apache Arrow][5])

---

### G.2 Parquet pushdown filter representation change (adaptive; toggleable)

#### G.2.1 Before → After (representation passed through the scan pipeline)

**Before (≤ DF51 / earlier Arrow)**: pushed-down predicate evaluation results are represented as **RowSelection** (runs of select/skip), pushed down **prior to reading column data**. ([Apache Arrow][7])

```rust
// “legacy” shape (conceptual):
let sel: parquet::arrow::arrow_reader::RowSelection = eval_predicate_to_row_selection(...);

// applied before reading column data (skips IO)
reader.with_row_selection(sel);
```

RowSelection is explicitly designed to be applied *before* reading column data and can skip IO. ([Apache Arrow][7])

**After (DF52 + Arrow 57.1+)**: DataFusion uses an **adaptive strategy**: represent filter results as either:

* `RowSelection` (run-length selectors), OR
* a **bitmask / BooleanBuffer** (“Bitmap”) applied later

The Arrow parquet team describes the intended switch explicitly: when predicates are granular, use a late evaluation approach storing a `BooleanBuffer` mask rather than a RowSelector list. ([GitHub][8])

```rust
// DF52 conceptual enum (matches the documented behavior):
enum FilterRepr {
    Selection(RowSelection),        // run-length selectors
    Bitmap(BooleanBuffer),          // per-row mask
}
```

#### G.2.2 DF52 behavior gate + hard toggle

Upgrade guide: DF52 uses the new adaptive strategy for pushed down Parquet filters; you can disable it by forcing selections. ([Apache DataFusion][9])

```sql
-- Applies only when Parquet late materialization is enabled
SET datafusion.execution.parquet.pushdown_filters = true;

-- Force legacy selection-only representation (disable adaptive “bitmap vs selection”)
SET datafusion.execution.parquet.force_filter_selections = true;
```

([Apache DataFusion][9])

Config docs spell out the selection rule:

* if `force_filter_selections = false`, reader auto-chooses RowSelection vs Bitmap based on “number and pattern of selected rows.” ([Apache DataFusion][2])

#### G.2.3 Value proposition / value at risk

* **Value**: the adaptive strategy is explicitly introduced as a performance improvement when a bitmask is more efficient than selectors. ([Apache DataFusion][9])
* **Risk / stability**:

  * This is a **heuristics-based choice**; perf profiles can shift with Arrow/Parquet version upgrades (same query, different repr, different CPU/memory trade).
  * If you need predictable perf characteristics across upgrades, pin behavior with `force_filter_selections=true`. ([Apache DataFusion][2])

---

### G.3 Parquet row-filter builder signature simplification (breaking)

#### G.3.1 Before → After (exact signature delta)

Upgrade guide provides the literal signature change: the duplicated schema parameter is removed. ([Apache DataFusion][9])

```rust
// BEFORE (≤ DF51)
pub fn build_row_filter(
    expr: &Arc<dyn PhysicalExpr>,
    physical_file_schema: &SchemaRef,
    predicate_file_schema: &SchemaRef,  // removed
    metadata: &ParquetMetaData,
    reorder_predicates: bool,
    file_metrics: &ParquetFileMetrics,
) -> Result<Option<RowFilter>>

// AFTER (DF52)
pub fn build_row_filter(
    expr: &Arc<dyn PhysicalExpr>,
    file_schema: &SchemaRef,
    metadata: &ParquetMetaData,
    reorder_predicates: bool,
    file_metrics: &ParquetFileMetrics,
) -> Result<Option<RowFilter>>
```

([Apache DataFusion][9])

#### G.3.2 Migration (call-site diff)

```diff
- build_row_filter(&predicate, &file_schema, &file_schema, metadata, reorder, metrics)
+ build_row_filter(&predicate, &file_schema, metadata, reorder, metrics)
```

([Apache DataFusion][9])

#### G.3.3 Contract shift (what DF52 now assumes)

DF52 expectation: the filter expression has already been adapted to the **physical file schema** (e.g., via `PhysicalExprAdapter`) before calling `build_row_filter`. ([Apache DataFusion][9])

**Value**: eliminates “which schema is which?” ambiguity; makes “predicate must match file schema” an explicit invariant. ([Apache DataFusion][9])
**Risk**: if you call `build_row_filter` with an unadapted expr, you own any mismatch behavior (wrong pushdown / wrong filtering).

---

### G.4 CSV parsing knob moved (breaking): `newlines_in_values`

#### G.4.1 Before → After (Rust API: exact structural move)

Upgrade guide’s migration is explicit: the knob moved from `FileScanConfigBuilder` to `CsvOptions`, and is then attached via `CsvSource::with_csv_options`. ([Apache DataFusion][9])

```rust
// BEFORE (≤ DF51)
let source = Arc::new(CsvSource::new(file_schema.clone()));
let config = FileScanConfigBuilder::new(object_store_url, source)
    .with_newlines_in_values(true)
    .build();

// AFTER (DF52)
let options = CsvOptions {
    newlines_in_values: Some(true),
    ..Default::default()
};
let source = Arc::new(CsvSource::new(file_schema.clone())
    .with_csv_options(options));
let config = FileScanConfigBuilder::new(object_store_url, source)
    .build();
```

([Apache DataFusion][9])

#### G.4.2 SQL equivalent (table-level override)

CSV Format Options explicitly support `NEWLINES_IN_VALUES` in `CREATE EXTERNAL TABLE … OPTIONS(...)`. ([Apache DataFusion][10])

```sql
CREATE EXTERNAL TABLE t (col1 varchar, col2 int)
STORED AS CSV
LOCATION '/tmp/foo/'
OPTIONS('NEWLINES_IN_VALUES' 'true');
```

([Apache DataFusion][10])

#### G.4.3 Value proposition / value at risk

Config docs warn about the tradeoff:

* enabling newline-in-quoted-values increases correctness robustness (esp. under parallel scanning),
* but **may reduce performance**. ([Apache DataFusion][2])

Agent takeaway:

* If CSV producers emit embedded newlines inside quotes, set this explicitly (table option or `CsvOptions`).
* Otherwise keep it off for throughput. ([Apache DataFusion][2])

[1]: https://datafusion.apache.org/user-guide/sql/ddl.html "DDL — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[3]: https://github.com/apache/datafusion/issues/16688 "Add support for registering files in the Arrow IPC stream format as tables using `register_arrow` or similar · Issue #16688 · apache/datafusion · GitHub"
[4]: https://datafusion.apache.org/blog/2026/01/12/datafusion-52.0.0 "Apache DataFusion 52.0.0 Released - Apache DataFusion Blog"
[5]: https://arrow.apache.org/docs/python/ipc.html "Streaming, Serialization, and IPC — Apache Arrow v23.0.1"
[6]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html "SessionContext in datafusion::execution::context - Rust"
[7]: https://arrow.apache.org/rust/parquet/arrow/arrow_reader/struct.RowSelection.html "RowSelection in parquet::arrow::arrow_reader - Rust"
[8]: https://github.com/apache/arrow-rs/issues/5523 "Adaptive Parquet Predicate Pushdown Evaluation · Issue #5523 · apache/arrow-rs · GitHub"
[9]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[10]: https://datafusion.apache.org/user-guide/sql/format_options.html "Format Options — Apache DataFusion  documentation"

## H) Operator graph simplification & batch sizing (DF52) — **syntax-first** + value/risk callouts

> **Release anchor (DF52):** DataFusion 52 **removes the standalone `CoalesceBatchesExec` node** and **integrates coalescing into operators themselves** using Arrow’s **coalesce kernel**, explicitly to **reduce plan complexity** and unblock optimizations like **`LIMIT` pushdown through joins**. ([datafusion.apache.org][1])

---

### H.0 What “coalescing” is (in DataFusion terms)

DataFusion executes in streams of Arrow `RecordBatch`. Many operators (filters, selective joins, repartition shuffles) can produce **tiny batches** which are expensive due to fixed per-batch overhead.

**Arrow’s canonical coalescing primitive** is `BatchCoalescer`:

```rust
use arrow_select::coalesce::BatchCoalescer;

let mut c = BatchCoalescer::new(schema, /*target_batch_size*/ 8192);
c.push_batch(batch)?;
if let Some(out) = c.next_completed_batch() { /* ... */ }
c.finish_buffered_batch()?;
```

It’s explicitly intended “after operations such as filter and take that produce smaller batches.” ([Docs.rs][2])

---

## H.1 **Before → After** (DF51 vs DF52): *external node removed; semantics move inside operators*

### H.1.1 Before (DF51): physical optimizer injects `CoalesceBatchesExec` nodes

The **old model**: a standalone `CoalesceBatchesExec` operator is inserted after “filter-like” operators that can produce small batches (explicitly: `FilterExec`, `HashJoinExec`, `RepartitionExec`). ([GitHub][3])

**Plan shape (representative):**

```text
… 
FilterExec(predicate=…)
  DataSourceExec(…)
↓
CoalesceBatchesExec(target_batch_size=<batch_size>, fetch=<limit?>)
↓
… downstream vectorized operators …
```

**Operator semantics (standalone node):**

* buffers until it collects `target_batch_size` rows, then emits a concatenated batch
* if `fetch` is set (LIMIT-like), it stops buffering once `fetch` rows are collected ([Docs.rs][4])

---

### H.1.2 After (DF52): coalescing is **embedded in operators**

DF52 release notes: coalescing is “integrated into the operators themselves … using Arrow’s coalesce kernel” and this reduces plan complexity + unblocks optimizations like pushing `LIMIT` through joins. ([datafusion.apache.org][1])

**Plan shape (DF52 intent):**

```text
…
FilterExec(predicate=…, batch_size=<batch_size>, fetch=<limit?>)
  DataSourceExec(…)
…
(no standalone CoalesceBatchesExec wrapper node)
```

Consequence: optimizers no longer need to “look through” a wrapper node for transformations (the release post calls out `LIMIT` pushdown through joins specifically). ([datafusion.apache.org][1])

---

## H.2 The new embedded primitive (DF52): `LimitedBatchCoalescer`

DataFusion wraps Arrow’s coalescing into a fetch-aware helper: `LimitedBatchCoalescer`.

### H.2.1 API surface (exact signatures)

```rust
use datafusion_physical_plan::coalesce::{LimitedBatchCoalescer, PushBatchStatus};

let mut c = LimitedBatchCoalescer::new(
    schema,
    /*target_batch_size*/ 8192,
    /*fetch*/ Some(1000), // None = unbounded
);

let status: PushBatchStatus = c.push_batch(batch)?;
if let Some(out) = c.next_completed_batch() { /* yield */ }
c.finish()?; // afterwards push_batch() errors
```

* `new(schema, target_batch_size, fetch)`
* `push_batch(batch) -> Result<PushBatchStatus>`
* `next_completed_batch() -> Option<RecordBatch>`
* `finish()` finalizes and prevents further pushes ([Docs.rs][5])

### H.2.2 Why Arrow kernel mattered (value proposition)

Arrow’s `BatchCoalescer` exists partly to avoid the “filter then concat_batches” pattern which can cause:

* ≥2× peak memory (hold input + concat output)
* extra copies (filter output then concat output) ([Docs.rs][2])

DF52 explicitly points to this shift so that future performance work concentrates in the Arrow kernel rather than in DataFusion wrapper logic. ([datafusion.apache.org][1])

---

## H.3 Concrete DF52 integration example: `FilterExec` (real code paths)

DF52’s `FilterExec` now **owns batch sizing parameters** and embeds `LimitedBatchCoalescer` in its stream.

### H.3.1 “New fields” (operator owns batch sizing)

`FilterExec` carries:

* `batch_size: usize`
* `fetch: Option<usize>` ([Docs.rs][6])

### H.3.2 Embedded coalescer construction (exact code)

When executing a partition, `FilterExec` constructs the stream with:

```rust
batch_coalescer: LimitedBatchCoalescer::new(
    self.schema(),
    self.batch_size,
    self.fetch,
)
```

…as part of `FilterExecStream`. ([Docs.rs][6])

### H.3.3 Embedded coalescer drive loop (exact behavior)

Inside `poll_next`:

1. **drain** `next_completed_batch()` if available
2. if input ends → `finish()` then keep draining
3. on each input batch: evaluate predicate → `filter_record_batch` → `push_batch(filtered)`
4. if `PushBatchStatus::LimitReached` → `finish()` early ([Docs.rs][6])

This is the *mechanical* replacement for an external `CoalesceBatchesExec` node.

---

## H.4 “Operator graph simplification” (what exactly got simpler)

### H.4.1 The blocker (why the external node was bad)

The DF52 release post states plainly:

* wrapper node “blocks other optimizations such as pushing `LIMIT` through joins”
* “made optimizer rules more complex” ([datafusion.apache.org][1])

The epic issue for removal repeats the same rationale: an extra operator in the tree forces other rules to special-case it (example called out: pushing limit through joins). ([GitHub][3])

### H.4.2 The cleanup (optimizer rule becomes unnecessary)

Once coalescing was integrated into the remaining operators, DataFusion tracked removal of the **`CoalesceBatches` physical optimizer rule** because “there is nothing remaining” for it to do. ([GitHub][7])

(That issue even shows the old pattern: optimizer rule detects an operator and wraps its child with `CoalesceBatchesExec::new(child, target_batch_size)`.) ([GitHub][7])

---

## H.5 Batch sizing knobs (DF52 syntax: config keys + Rust `SessionConfig`)

### H.5.1 Global batch size

```sql
SET datafusion.execution.batch_size = '8192';
```

Default is `8192`. ([datafusion.apache.org][8])

Rust:

```rust
use datafusion::execution::context::SessionConfig;

let cfg = SessionConfig::new().with_batch_size(8192);
```

([Docs.rs][9])

### H.5.2 “Coalesce small batches” toggle

```sql
SET datafusion.execution.coalesce_batches = 'true';
```

This option still exists and is described as examining batches and coalescing small ones into larger batches (helpful for highly selective filters/joins). ([datafusion.apache.org][8])

Rust:

```rust
let cfg = SessionConfig::new().with_coalesce_batches(true);
```

([Docs.rs][9])

### H.5.3 Join-specific batch-size enforcement

```sql
SET datafusion.execution.enforce_batch_size_in_joins = 'true';
```

Docs: default `false`; enabling can reduce memory usage for highly selective joins but is “slightly slower.” ([datafusion.apache.org][8])

Rust:

```rust
let cfg = SessionConfig::new().with_enforce_batch_size_in_joins(true);
```

([Docs.rs][9])

---

## H.6 Determinism / semantics (what changes, what does not)

### H.6.1 Row semantics: unchanged; batch boundaries change

Arrow `BatchCoalescer` explicitly preserves row order (“Output rows are produced in the same order as the input rows”). ([Docs.rs][2])
So the “determinism delta” is about **batching boundaries** (how rows are chunked into output batches), not row contents or order.

### H.6.2 Practical implication for agent tooling

* If your embedding consumes results batch-by-batch (e.g., streaming sinks, UDFs with per-batch overhead), DF52’s internal coalescing reduces “tiny batch storms.”
* If you rely on exact batch boundaries for tests (rare, but happens in engine-integrations), treat this as a **non-stable contract** and assert on rows, not batches.

---

## H.7 Implementation blueprint (for custom `ExecutionPlan` operators)

If you’re writing an operator that can emit tiny batches, DF52’s “native” pattern is:

```rust
struct MyStream {
  input: SendableRecordBatchStream,
  coalescer: LimitedBatchCoalescer, // schema + batch_size + fetch
}

fn poll_next(...) -> Poll<Option<Result<RecordBatch>>> {
  if let Some(out) = self.coalescer.next_completed_batch() {
    return Ready(Some(Ok(out)));
  }

  match poll_input()? {
    None => { self.coalescer.finish()?; /* then drain */ }
    Some(batch) => {
      let produced = /* transform -> RecordBatch */;
      match self.coalescer.push_batch(produced)? {
        PushBatchStatus::Continue => {}
        PushBatchStatus::LimitReached => self.coalescer.finish()?,
      }
    }
  }
}
```

That blueprint is literally how DF52’s `FilterExecStream` is structured. ([Docs.rs][6])

---

If you want the next increment in the same style, the natural follow-on is a **“plan diff checklist”** for H:

* what `EXPLAIN VERBOSE` nodes to expect to disappear (CoalesceBatches wrappers),
* what per-operator fields now carry `batch_size/fetch`,
* and where join behavior changes depending on `enforce_batch_size_in_joins`.

[1]: https://datafusion.apache.org/blog/2026/01/12/datafusion-52.0.0 "Apache DataFusion 52.0.0 Released - Apache DataFusion Blog"
[2]: https://docs.rs/arrow/latest/arrow/compute/kernels/coalesce/struct.BatchCoalescer.html "BatchCoalescer in arrow::compute::kernels::coalesce - Rust"
[3]: https://github.com/apache/datafusion/issues/18779 "[EPIC] Remove `CoalesceBatchesExec` operator · Issue #18779 · apache/datafusion · GitHub"
[4]: https://docs.rs/datafusion/latest/datafusion/physical_plan/coalesce_batches/struct.CoalesceBatchesExec.html?utm_source=chatgpt.com "CoalesceBatchesExec in datafusion::physical_plan"
[5]: https://docs.rs/datafusion-physical-plan/latest/datafusion_physical_plan/coalesce/struct.LimitedBatchCoalescer.html "LimitedBatchCoalescer in datafusion_physical_plan::coalesce - Rust"
[6]: https://docs.rs/crate/datafusion-physical-plan/latest/target-redirect/src/datafusion_physical_plan/filter.rs.html "filter.rs - source"
[7]: https://github.com/apache/datafusion/issues/19591 "Remove `CoalesceBatches` optimizer rule · Issue #19591 · apache/datafusion · GitHub"
[8]: https://datafusion.apache.org/_sources/user-guide/configs.md.txt "datafusion.apache.org"
[9]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionConfig.html "SessionConfig in datafusion::execution::context - Rust"

## I) UDF / aggregate semantics tightening (planning-time enforcement) (DF52) — **syntax-first** + value/risk callouts

> **Upgrade anchor (DF52):** planner now **rejects** `WITHIN GROUP` unless the UDAF opts in, and `IGNORE/RESPECT NULLS` now **errors by default** unless the aggregate opts in. ([Apache DataFusion][1])

---

### I.0 What exists in DF52 (the *actual* contract surface)

#### I.0.1 Two new “planning gates” on `AggregateUDFImpl`

```rust
trait AggregateUDFImpl {
    /// ordered-set (WITHIN GROUP) gate (default false)
    fn supports_within_group_clause(&self) -> bool { false }

    /// null treatment (IGNORE/RESPECT NULLS) gate (default false)
    fn supports_null_handling_clause(&self) -> bool { false }

    fn accumulator(&self, acc_args: AccumulatorArgs<'_>) -> Result<Box<dyn Accumulator>>;
}
```

* `supports_within_group_clause=false` ⇒ SQL using `WITHIN GROUP (ORDER BY …)` **errors during SQL parsing/planning**. ([Docs.rs][2])
* `supports_null_handling_clause=false` ⇒ SQL using `IGNORE NULLS` / `RESPECT NULLS` **errors during SQL parsing/planning**. ([Docs.rs][2])

#### I.0.2 The only runtime “knobs” you get (when you opt-in): `AccumulatorArgs`

```rust
pub struct AccumulatorArgs<'a> {
    pub ignore_nulls: bool,                  // IGNORE NULLS → true
    pub order_bys: &'a [PhysicalSortExpr],   // ordering clause passed to this aggregate
    pub is_reversed: bool,
    pub exprs: &'a [Arc<dyn PhysicalExpr>],
    pub expr_fields: &'a [Arc<Field>],
    /* … */
}
```

`AccumulatorArgs` explicitly carries both **`ignore_nulls`** and **`order_bys`** (the ordering expressions passed to the aggregate). ([Docs.rs][3])

---

## I.1 `WITHIN GROUP` now requires explicit UDAF opt-in

### I.1.1 **Before → After** (DF51 vs DF52) — planner behavior (literal)

#### Before (DF51): *too permissive*

Planner could forward `WITHIN GROUP` to order-sensitive aggregates even when they **did not implement ordered-set semantics**, allowing queries like this to plan successfully:

```sql
-- DF51 could plan (too permissive)
SELECT SUM(x) WITHIN GROUP (ORDER BY x) FROM t;
```

This behavior is explicitly called out as “too permissive” in the DF52 upgrade guide. ([Apache DataFusion][1])

#### After (DF52): *ordered-set only*

Planner accepts `WITHIN GROUP` **only if** the function advertises support:

```text
DF52: plan-time gate
if !udaf.supports_within_group_clause() && within_group_present => ERROR
```

Upgrade guide (DF52): “accepted only if … returns true from `supports_within_group_clause()`.” ([Apache DataFusion][1])
SQL docs: attempting `WITHIN GROUP` with a regular aggregate fails with:
`"WITHIN GROUP is only supported for ordered-set aggregate functions"`. ([Apache DataFusion][4])

---

### I.1.2 The **UDAF-side** “before/after” attribute diff (what you must change)

#### (A) Default / non-ordered-set UDAF (DF52): do **not** opt in

```rust
#[derive(Debug, PartialEq, Eq, Hash)]
struct MyOrderSensitiveButNotOrderedSet { /* … */ }

impl AggregateUDFImpl for MyOrderSensitiveButNotOrderedSet {
    fn supports_within_group_clause(&self) -> bool { false } // default
    /* … */
}
```

Result:

```sql
SELECT my_udaf(x) WITHIN GROUP (ORDER BY y) FROM t;
-- DF52: planning error (accumulator() is never called)
```

#### (B) Ordered-set UDAF (DF52): opt in + implement ordering semantics yourself

```rust
#[derive(Debug, PartialEq, Eq, Hash)]
struct MyOrderedSetAgg { /* signature, etc */ }

impl AggregateUDFImpl for MyOrderedSetAgg {
    fn supports_within_group_clause(&self) -> bool { true } // ✅ opt-in

    fn accumulator(&self, acc: AccumulatorArgs<'_>)
      -> datafusion_common::Result<Box<dyn Accumulator>>
    {
        // DF52 conveys ordering into the accumulator args; you own how to use it.
        let order_bys: &[PhysicalSortExpr] = acc.order_bys;
        Ok(Box::new(MyOrderedSetAccumulator::new(order_bys.to_vec())))
    }
}
```

Two critical semantics (DF52 contract):

* `supports_within_group_clause=true` is intended **only for ordered-set aggregates**. ([Docs.rs][2])
* **DataFusion does not introduce a sort node** for `WITHIN GROUP`; your implementation must **buffer + sort internally** (or otherwise honor the order). ([Docs.rs][2])

---

### I.1.3 Canonical SQL shapes (DF52)

Ordered-set aggregate example:

```sql
SELECT percentile_cont(0.75) WITHIN GROUP (ORDER BY c1 DESC) FROM t;
```

Trait docs show the permitted forms and note default ordering behavior is implementation-dependent if not specified. ([Docs.rs][2])

DataFusion SQL reference explicitly states `WITHIN GROUP` is **opt-in** and lists current built-ins supporting it: `percentile_cont`, `approx_percentile_cont`, `approx_percentile_cont_with_weight`. ([Apache DataFusion][4])

---

### I.1.4 Value proposition / value at risk

**Value (correctness + determinism):**

* Eliminates the “silent acceptance” of semantically nonsensical queries (e.g., `SUM … WITHIN GROUP`) and aligns behavior with documented ordered-set semantics. ([Apache DataFusion][1])

**Risk (UDAF implementer burden):**

* Opting in implies you must implement ordering semantics yourself (buffer/sort), which can be **memory-heavy** and runtime-costly. ([Docs.rs][2])

---

## I.2 Null handling clause opt-in tightened (`IGNORE NULLS` / `RESPECT NULLS`)

### I.2.1 **Before → After** (DF51 vs DF52) — planner behavior

#### Before (DF51): clause often “silently ignored”

DF52 upgrade guide states: most aggregate functions “silently ignored this syntax” previously because it was permitted by default and not used. ([Apache DataFusion][1])

Example called out:

```sql
-- Previously could silently succeed (but semantics not guaranteed)
SELECT median(c1) IGNORE NULLS FROM table;
```

#### After (DF52): clause errors unless explicitly supported

Upgrade guide: SQL parsing now fails for such queries “instead of silently succeeding,” unless the function overrides `supports_null_handling_clause()`. ([Apache DataFusion][1])
Trait doc: returning `false` causes an error “during SQL parsing if these clauses are detected.” ([Docs.rs][2])

---

### I.2.2 UDAF implementation: **exact attribute + behavior you must add**

#### (A) Default UDAF (DF52): clause rejected

```rust
impl AggregateUDFImpl for MyAgg {
    fn supports_null_handling_clause(&self) -> bool { false } // default
}
```

Result:

```sql
SELECT my_agg(x) IGNORE NULLS FROM t;
-- DF52: planning error
```

#### (B) Opt-in UDAF (DF52): enable + honor `acc_args.ignore_nulls`

```rust
#[derive(Debug, PartialEq, Eq, Hash)]
struct MyNullAwareAgg;

impl AggregateUDFImpl for MyNullAwareAgg {
    fn supports_null_handling_clause(&self) -> bool { true } // ✅ opt-in

    fn accumulator(&self, acc: AccumulatorArgs<'_>)
      -> datafusion_common::Result<Box<dyn Accumulator>>
    {
        Ok(Box::new(MyAcc { ignore_nulls: acc.ignore_nulls }))
    }
}

struct MyAcc { ignore_nulls: bool }

impl Accumulator for MyAcc {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        let arr = &values[0];

        if self.ignore_nulls {
            // required semantic: IGNORE NULLS should skip null input rows
            // (implementation-specific; e.g., iterate only valid indices)
        } else {
            // RESPECT NULLS / default: nulls contribute according to your aggregate rules
        }
        Ok(())
    }

    /* state/evaluate/etc */
}
```

* `AccumulatorArgs.ignore_nulls` is the explicit carrier for `IGNORE NULLS`. ([Docs.rs][3])
* `supports_null_handling_clause=true` implies you **must** respect the configuration “present in `AccumulatorArgs`, `ignore_nulls`.” ([Docs.rs][2])

#### Built-ins that opted in (DF52 guidance)

Upgrade guide names examples that do respect the clause: `array_agg`, `first_value`, `last_value`. ([Apache DataFusion][1])

---

## I.3 Registration pattern (DF52) — advanced UDAF API (required for these opt-ins)

To expose `supports_within_group_clause` / `supports_null_handling_clause`, you generally use the **trait-based** UDAF API (`AggregateUDFImpl`) and wrap via `AggregateUDF::new_from_impl`:

```rust
use datafusion::prelude::*;
use datafusion::logical_expr::AggregateUDF;

let udaf = AggregateUDF::new_from_impl(MyOrderedSetAgg { /*…*/ });
ctx.register_udaf(udaf);
```

`AggregateUDF::new_from_impl` / `new_from_shared_impl` are the documented constructors for trait-based UDAFs. ([Docs.rs][5])

---

### I.4 Agent checklist (migration / enforcement)

1. **Do you intend ordered-set semantics?**

   * Yes → `supports_within_group_clause() = true`, and your accumulator must honor ordering (DataFusion won’t sort for you). ([Apache DataFusion][1])
   * No → keep false; instruct callers to use an explicit function signature (e.g., `my_udaf(value, sort_key)`), not `WITHIN GROUP`. ([Apache DataFusion][1])

2. **Do you intend IGNORE/RESPECT NULLS semantics?**

   * Yes → `supports_null_handling_clause() = true` and honor `acc_args.ignore_nulls`. ([Apache DataFusion][1])
   * No → keep false; DF52 will now surface misuse as a plan-time error (preferred over silent ignore). ([Apache DataFusion][1])

[1]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[2]: https://docs.rs/datafusion-expr/latest/datafusion_expr/trait.AggregateUDFImpl.html "AggregateUDFImpl in datafusion_expr - Rust"
[3]: https://docs.rs/datafusion-expr/latest/datafusion_expr/function/struct.AccumulatorArgs.html "AccumulatorArgs in datafusion_expr::function - Rust"
[4]: https://datafusion.apache.org/user-guide/sql/aggregate_functions.html "Aggregate Functions — Apache DataFusion  documentation"
[5]: https://docs.rs/datafusion-expr/latest/datafusion_expr/struct.AggregateUDF.html "AggregateUDF in datafusion_expr - Rust"

## J) Core API / IR surface changes (planner & embedding) (DF52) — **syntax-first** + value/risk callouts

---

### J.1 `DFSchema` API now returns `&FieldRef` (Arc-clone vs `Field` clone)

#### J.1.1 Before → After (call-site delta)

**Before (≤ DF51):** `df_schema.field(..)` returned `&Field`; common pattern cloned the full `Field`:

```diff
- let field = df_schema.field("my_column").as_ref().clone();  // clones Field
```

**After (DF52):** `df_schema.field(..)` returns `&FieldRef` so you clone the `Arc`:

```diff
+ let field = Arc::clone(df_schema.field("my_column"));       // clones Arc<Field>
```

The DF52 upgrade guide calls out the motivation explicitly: methods now return `&FieldRef` to permit cheaper copies via `Arc::clone` instead of cloning the entire `Field` structure. ([Apache DataFusion][1])

#### J.1.2 Value proposition / value at risk

* **Value:** planner hot paths touch schema fields constantly; switching to Arc clones reduces per-plan allocation/copy cost, especially in dense planner code and schema-heavy rewrites. ([Apache DataFusion][1])
* **Risk:** any embedding code that assumed `&Field` (and did `.as_ref().clone()`) will fail to compile; migration is mechanical to `Arc::clone(...)`. ([Apache DataFusion][1])

---

### J.2 `CacheAccessor::remove` no longer requires mutable access (`&mut self` → `&self`)

#### J.2.1 Before → After (trait signature delta)

**Before (≤ DF51):**

```rust
fn remove(&mut self, k: &K) -> Option<V>;
```

**After (DF52):**

```rust
fn remove(&self, k: &K) -> Option<V>;
```

DF52 `CacheAccessor` trait definition shows `remove(&self, ..)` (and all methods are `&self`) and explicitly requires implementors to use **internal mutability** because the cache may be accessed concurrently. ([Docs.rs][2])
Upgrade guide summarizes this as: “The remove API no longer requires a mutable instance.” ([Apache DataFusion][1])

#### J.2.2 “What it enables” (embedding ergonomics)

**Before:** if you held `Arc<dyn CacheAccessor<..>>`, you still had to produce `&mut` to call `remove`, forcing awkward wrapper types or external locking.

**After (DF52):** you can keep caches behind shared references and still support evictions:

```rust
use std::sync::Arc;
use datafusion::execution::cache::CacheAccessor;

fn invalidate<K, V>(cache: Arc<dyn CacheAccessor<K, V, Extra = ()>>, key: &K) -> Option<V> {
    cache.remove(key)
}
```

This aligns `remove` with the rest of the cache API (shared access everywhere). ([Docs.rs][2])

#### J.2.3 Value proposition / value at risk

* **Value:** eliminates an artificial exclusivity requirement; easier to expose caches across planning/execution components without “mutable borrow plumbing.” ([Apache DataFusion][1])
* **Risk:** implementors must ensure `remove` is thread-safe (DashMap/RwLock/etc.); DF52 explicitly states caches may be accessed by multiple concurrent queries. ([Docs.rs][2])

---

### J.3 `Expr` struct size reduction (micro-opt): boxing large variants

#### J.3.1 Why it is in the DF52 “core IR changes” bucket

The DF52 release tracking ticket explicitly calls out: “change Expr OuterReferenceColumn and Alias to Box type for reducing expr struct size” as a feature to mention for the 52 line. ([GitHub][3])

#### J.3.2 Before → After (enum variant layout delta)

PR summary spells out the mechanical change (and why):

**Variant boxing (conceptual diff):**

```diff
- Expr::Alias(Alias)
+ Expr::Alias(Box<Alias>)

- Expr::Column(Column)
+ Expr::Column(Box<Column>)

- Expr::OuterReferenceColumn(FieldRef, Column)
+ Expr::OuterReferenceColumn(Box<OuterReference>)   // new struct wrapper
```

This is documented directly in the PR’s commit message: “Box three large variants … reduces Expr enum size from 112 bytes to 80 bytes (28% reduction) … improves cache locality and reduces memory usage.” ([GitHub][4])

#### J.3.3 “User-visible” migration surface (pattern matching + construction)

If your embedding pattern-matches on `Expr`, the *shape* changes from by-value fields → boxed payloads.

**Before:**

```rust
use datafusion_expr::Expr;
use datafusion_expr::expr::Alias;

match expr {
    Expr::Alias(Alias { expr, name, .. }) => { /* ... */ }
    _ => {}
}
```

**After (DF52):**

```rust
use datafusion_expr::Expr;
use datafusion_expr::expr::Alias;

match expr {
    Expr::Alias(alias_box) => {
        let Alias { expr, name, .. } = &**alias_box;  // explicit deref
        /* ... */
    }
    _ => {}
}
```

**Construction also becomes explicit:**

```rust
use datafusion_expr::Expr;
use datafusion_expr::expr::Alias;

let e = Expr::Alias(Box::new(Alias { /* fields */ }));
```

(These are the necessary downstream consequences of `Alias(Box<Alias>)` etc. ([GitHub][4]))

#### J.3.4 Value proposition / value at risk

* **Value:** `Expr` is one of the most frequently created/traversed types in planning; shrinking it improves cache locality and reduces memory footprint across the engine. ([GitHub][4])
* **Risk:** downstream crates that destructure `Expr` variants must update to box-aware patterns; also debug formatting / error message snapshots may shift (PR notes updates to expectations). ([GitHub][4])

---

### J.4 Correctness / regression fixes called out in the DF52 cycle (closed)

The DF52 release ticket’s “Bugs that should be fixed” list includes both the TPC-DS nullability regression and the DynamicFilter Hash/Eq contract violation. ([GitHub][3])

#### J.4.1 TPC-DS planning regression: input schema nullability mismatch (fixed via #17813)

**Symptom (before):** a planner/bench panic with an explicit nullability mismatch between physical and logical input schemas:

```text
Internal("Physical input schema should be the same as the one converted from logical input schema. Differences:
  - field nullability at index 5 [sales_cnt]: (physical) true vs (logical) false
  - field nullability at index 6 [sales_amt]: (physical) true vs (logical) false")
```

Repro reported via `cargo bench ... physical_plan_tpcds_all`. ([GitHub][5])

**Root cause + fix (as implemented):**

* A prior change rewrote `coalesce` into `case`, but `case` nullability reporting was less precise than `coalesce`.
* Fix “tweaks nullable logic” for both logical and physical `case` expressions (including best-effort const evaluation to determine reachability).
* Also “relaxes the optimizer schema check slightly” to allow nullability removal without disabling the check entirely.
* Re-enables the panicking benchmark. ([GitHub][6])

**Value at risk:** schema nullability is enforced by DataFusion at execution time; incorrect nullability propagation can either (a) block planning via schema verification or (b) lead to runtime constraint violations if providers return nulls for non-nullable fields. ([GitHub][5])

#### J.4.2 `DynamicFilterPhysicalExpr` Hash/Eq contract violation (fixed via #19659)

**Bug (before):** `Hash` and `Eq` took separate read locks, so the underlying expression could change between `hash()` and `eq()`; this breaks HashMap invariants and can cause “spin loop / corruption” behaviors. ([GitHub][7])

**Fix (DF52): identity-based Hash/Eq**

* `Hash`: `Arc::as_ptr(&self.inner)` (hash stable identity, not mutable contents)
* `PartialEq`: `Arc::ptr_eq(&self.inner)` (pointer equality, not content equality)

PR rationale explicitly enumerates the consequences it prevents: key instability (“keys disappear”), infinite loops, corrupted HashMap state. ([GitHub][8])

**Semantic consequence (intentional):** two dynamic filters with different inner `Arc`s are treated as different filters even if their current expressions match at some instant (they have independent update lifecycles). ([GitHub][8])

[1]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[2]: https://docs.rs/datafusion/52.1.0/datafusion/execution/cache/trait.CacheAccessor.html "CacheAccessor in datafusion::execution::cache - Rust"
[3]: https://github.com/apache/datafusion/issues/18566 "Release DataFusion `52.0.0` (Dec 2025 / Jan 2026) · Issue #18566 · apache/datafusion · GitHub"
[4]: https://github.com/apache/datafusion/pull/16771 "feat: change Expr OuterReferenceColumn and Alias to Box type for reducing expr struct size by zhuqi-lucas · Pull Request #16771 · apache/datafusion · GitHub"
[5]: https://github.com/apache/datafusion/issues/17801 "Regression: error planning TPC-DS query: input schema nullability mismatch · Issue #17801 · apache/datafusion · GitHub"
[6]: https://github.com/apache/datafusion/pull/17813 "#17801 Improve nullability reporting of case expressions by pepijnve · Pull Request #17813 · apache/datafusion · GitHub"
[7]: https://github.com/apache/datafusion/issues/19641 "DynamicFilterPhysicalExpr violates Hash/Eq contract · Issue #19641 · apache/datafusion · GitHub"
[8]: https://github.com/apache/datafusion/pull/19659 "fix: DynamicFilterPhysicalExpr violates Hash/Eq contract by kumarUjjawal · Pull Request #19659 · apache/datafusion · GitHub"

## K) FFI boundary & DataFusion-Python upgrade implications (DF52) — **syntax-first** + value/risk callouts

---

### K.0 What changed in DF52 (surface map)

| Surface                            | DF51                                                                                                        | DF52                                                                                                                    |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- |
| `datafusion-ffi` conversions       | often “FFI → Foreign* wrapper → Arc<dyn Trait>”                                                             | direct “FFI → Arc<dyn Trait>” conversions (fewer wrapper hops) ([Apache DataFusion][1])                                 |
| Provider constructors              | `FFI_TableProvider::new(provider, …)` (no `TaskContextProvider`, no `LogicalExtensionCodec`) ([Docs.rs][2]) | `new(…, task_ctx_provider, logical_codec)` + `new_with_ffi_codec(…, FFI_LogicalExtensionCodec)` ([Docs.rs][3])          |
| Python extension PyCapsule methods | `__datafusion_*_provider__(py)` (no session param; examples show this) ([Apache DataFusion][4])             | `__datafusion_*_provider__(py, session)` (session used to extract `FFI_LogicalExtensionCodec`) ([Apache DataFusion][5]) |
| Core crate features                | `pyarrow` feature existed                                                                                   | `pyarrow` feature removed; migrated to `datafusion-python` ([Apache DataFusion][1])                                     |

---

## K.1 `datafusion-ffi` crate updates (Rust side)

### K.1.1 “Easier conversion to underlying trait objects” (explicit before/after)

#### SchemaProvider conversion

**Before (DF51-ish pattern):**

```rust
let foreign_provider: ForeignSchemaProvider = ffi_provider.into();
let foreign_provider = Arc::new(foreign_provider) as Arc<dyn SchemaProvider>;
```

**After (DF52):**

```rust
let foreign_provider: Arc<dyn SchemaProvider + Send> = ffi_provider.into();
let foreign_provider = foreign_provider as Arc<dyn SchemaProvider>;
```

([Apache DataFusion][1])

#### Scalar UDF conversion

**Before (DF51-ish pattern):**

```rust
let foreign_udf: ForeignScalarUDF = ffi_udf.try_into()?;
let foreign_udf: ScalarUDF = foreign_udf.into();
```

**After (DF52):**

```rust
let foreign_udf: Arc<dyn ScalarUDFImpl> = ffi_udf.into();
let foreign_udf = ScalarUDF::new_from_shared_impl(foreign_udf);
```

([Apache DataFusion][1])

**Value:** fewer wrapper allocations + fewer “FFI round-trip” code paths; less friction in embeddings that keep everything behind `Arc<dyn Trait>`. ([Apache DataFusion][1])
**Value at risk:** code that relied on concrete `Foreign*` wrapper types now needs to pivot to trait-object conversions (or explicitly reconstruct wrappers).

---

### K.1.2 Provider constructor contract change: **TaskContextProvider + optional LogicalExtensionCodec**

DF52 upgrade guide explicitly: when creating these FFI structs you must provide a `TaskContextProvider` and optionally a `LogicalExtensionCodec`:
`FFI_CatalogListProvider`, `FFI_CatalogProvider`, `FFI_SchemaProvider`, `FFI_TableProvider`, `FFI_TableFunction`. ([Apache DataFusion][1])

#### Concrete “before vs after” on `FFI_TableProvider::new`

**DF51 `FFI_TableProvider::new` signature (no task ctx, no codec):**

```rust
pub fn new(
    provider: Arc<dyn TableProvider + Send>,
    can_support_pushdown_filters: bool,
    runtime: Option<Handle>,
) -> Self
```

([Docs.rs][2])

**DF52 `FFI_TableProvider::new` signature (requires TaskContextProvider; optional codec):**

```rust
pub fn new(
    provider: Arc<dyn TableProvider + Send>,
    can_support_pushdown_filters: bool,
    runtime: Option<Handle>,
    task_ctx_provider: impl Into<FFI_TaskContextProvider>,
    logical_codec: Option<Arc<dyn LogicalExtensionCodec>>,
) -> Self
```

([Docs.rs][3])

**DF52 convenience constructor: `new_with_ffi_codec`**

```rust
pub fn new_with_ffi_codec(
    provider: Arc<dyn TableProvider + Send>,
    can_support_pushdown_filters: bool,
    runtime: Option<Handle>,
    logical_codec: FFI_LogicalExtensionCodec,
) -> Self
```

([Docs.rs][3])

#### “Most common TaskContextProvider” (explicit)

Upgrade guide: “most common implementation … is `SessionContext`.” ([Apache DataFusion][1])

```rust
let ctx = Arc::new(SessionContext::default());
let table = Arc::new(MyTableProvider::new());
let ffi_table = FFI_TableProvider::new(table, /*...*/, ctx, None);
```

([Apache DataFusion][1])

---

### K.1.3 Why `TaskContextProvider` exists (not optional in practice)

`datafusion-ffi` uses `datafusion-proto` to serialize/deserialize across the boundary; function (de)serialization needs a `TaskContext` (implements `FunctionRegistry`), but you can’t safely “freeze” a task context at provider-registration time because later registrations must be visible. DF52 therefore introduces `FFI_TaskContextProvider` that holds a **Weak** reference and is implemented on `SessionContext` (and can be implemented by any `Session`). ([Docs.rs][6])

**Value:** fixes “custom UDF present in expression → FFI de/serialize fails” class of issues; you’re using the *current* registry, not a default throwaway context. ([GitHub][7])
**Value at risk:** provider lifetime rules become strict: the weak-linked context/provider must remain valid during calls. ([Docs.rs][6])

---

### K.1.4 `FFI_LogicalExtensionCodec` as the “bundle” (codec + task ctx)

Upgrade guide shows the intended bundling pattern:

```rust
let codec = Arc::new(DefaultLogicalExtensionCodec {});
let ctx = Arc::new(SessionContext::default());
let ffi_codec = FFI_LogicalExtensionCodec::new(codec, None, ctx);

let table = Arc::new(MyTableProvider::new());
let ffi_table = FFI_TableProvider::new_with_ffi_codec(table, None, ffi_codec);
```

([Apache DataFusion][1])

**Value:** one object (`FFI_LogicalExtensionCodec`) can carry the task-context provider (and extension codec) so you don’t thread both everywhere. ([Apache DataFusion][1])

---

## K.2 DataFusion-Python 52.0.0: PyCapsule extension signature changes

### K.2.1 Python-facing contract: `__datafusion_*_provider__` now takes `session`

Python upgrade guide (DF52) explicitly: users exporting `CatalogProvider`, `SchemaProvider`, `TableProvider`, or `TableFunction` via FFI must provide `LogicalExtensionCodec` + `TaskContextProvider`, and the capsule-export methods now require an **additional parameter** used to extract the `FFI_LogicalExtensionCodec`. ([Apache DataFusion][5])

#### New signature (example: CatalogProvider)

```rust
#[pymethods]
impl MyCatalogProvider {
    pub fn __datafusion_catalog_provider__<'py>(
        &self,
        py: Python<'py>,
        session: Bound<PyAny>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let name = cr"datafusion_catalog_provider".into();
        let provider = Arc::clone(&self.inner) as Arc<dyn CatalogProvider + Send>;

        let codec = ffi_logical_codec_from_pycapsule(session)?;
        let provider = FFI_CatalogProvider::new_with_ffi_codec(provider, None, codec);

        PyCapsule::new(py, provider, Some(name))
    }
}
```

([Apache DataFusion][5])

#### Codec extraction helper (literal mechanism)

```rust
pub(crate) fn ffi_logical_codec_from_pycapsule(obj: Bound<PyAny>) -> PyResult<FFI_LogicalExtensionCodec> {
    let attr_name = "__datafusion_logical_extension_codec__";
    let capsule = if obj.hasattr(attr_name)? {
        obj.getattr(attr_name)?.call0()?
    } else {
        obj
    };

    let capsule = capsule.downcast::<PyCapsule>()?;
    validate_pycapsule(capsule, "datafusion_logical_extension_codec")?;
    let codec = unsafe { capsule.reference::<FFI_LogicalExtensionCodec>() };
    Ok(codec.clone())
}
```

([Apache DataFusion][5])

**Value:** Python side now passes the *session* (or a session-like object exposing `__datafusion_logical_extension_codec__`) so your Rust provider can use the same logical codec registry and current task context. ([Apache DataFusion][5])
**Value at risk:** any third-party package still exporting `__datafusion_table_provider__(py)` will break on DF52 unless updated.

> Note: the “Custom Table Provider” user-guide page still shows the *old* 2-arg signature and `FFI_TableProvider::new(provider, false, None)`; treat that snippet as **pre-DF52** and migrate per the upgrade guide. ([Apache DataFusion][4])

---

### K.2.2 Required components for FFI providers (DF52 rule)

Python upgrade guide: FFI-exported providers must now provide access to:

* `LogicalExtensionCodec`
* `TaskContextProvider` ([Apache DataFusion][5])

**Reason (mechanistic):** logical/physical expr and plan (de)serialization must resolve custom UDFs/extension nodes against the correct registry; DF52 routes that via codec + task context provider rather than a default context. ([GitHub][7])

---

## K.3 Core crate feature cleanup (DF52): `pyarrow` feature removal + dependency hygiene

### K.3.1 Removal of `pyarrow` feature flag from core DataFusion

Core upgrade guide (DF52):

> “The `pyarrow` feature flag has been removed. This feature has been migrated to the `datafusion-python` repository since version `44.0.0`.” ([Apache DataFusion][1])

**Practical consequence (Cargo):**

```toml
# BEFORE (legacy; no longer valid in DF52)
datafusion = { version = "51", features = ["pyarrow"] }

# AFTER (DF52+)
# - core datafusion has no pyarrow feature
# - Python bindings live in datafusion-python project
datafusion = { version = "52" }
```

([Apache DataFusion][1])

### K.3.2 FFI crate decoupling from `datafusion` “core” crate

Python upgrade guide notes: FFI interface updates “no longer depend directly on the `datafusion` core crate” and recommends using the specific subcrates (e.g., `datafusion_catalog::MemTable` rather than `datafusion::catalog::MemTable`) for improved build times / smaller binaries. ([Apache DataFusion][5])

---

## K.4 Agent-ready migration checklist (DF51 → DF52)

1. **Rust FFI producers**: update constructors

   * Replace `FFI_*::new(old_args…)` with `FFI_*::new(…, task_ctx_provider, logical_codec)` or `new_with_ffi_codec(…, ffi_codec)`. ([Apache DataFusion][1])
2. **Rust FFI consumers**: simplify conversions

   * Prefer `ffi_provider.into()` → `Arc<dyn Trait>` directly; remove intermediate `Foreign*` wrappers unless you specifically need them. ([Apache DataFusion][1])
3. **Python PyCapsule exporters**: change method signatures

   * `__datafusion_*_provider__(py)` → `__datafusion_*_provider__(py, session)` and extract codec via the provided helper pattern. ([Apache DataFusion][5])
4. **Dependency cleanup**

   * Remove `datafusion` `pyarrow` feature usage; ensure your Python integration depends on `datafusion-python` rather than core `datafusion` feature flags. ([Apache DataFusion][1])

[1]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[2]: https://docs.rs/datafusion-ffi/51.0.0/datafusion_ffi/table_provider/struct.FFI_TableProvider.html "FFI_TableProvider in datafusion_ffi::table_provider - Rust"
[3]: https://docs.rs/datafusion-ffi/latest/datafusion_ffi/table_provider/struct.FFI_TableProvider.html "FFI_TableProvider in datafusion_ffi::table_provider - Rust"
[4]: https://datafusion.apache.org/python/user-guide/io/table_provider.html "Custom Table Provider — Apache Arrow DataFusion  documentation"
[5]: https://datafusion.apache.org/python/user-guide/upgrade-guides.html "Upgrade Guides — Apache Arrow DataFusion  documentation"
[6]: https://docs.rs/crate/datafusion-ffi/latest "datafusion-ffi 52.1.0 - Docs.rs"
[7]: https://github.com/apache/datafusion/issues/18671?utm_source=chatgpt.com "Support user defined functions as expressions in FFI calls"

## D-gap-1 / L) `TableSchema` + “schemas upfront” refactor (DF52) — **syntax-first** + value/risk callouts

> **Upgrade anchor (DF52):** file sources now require schema (incl. partition cols) **at construction time**, `FileScanConfigBuilder` no longer takes schema, `with_table_partition_cols()` is removed, and `FileFormat::file_source()` now takes a `TableSchema`.

---

### L.0 `TableSchema` (schema container you now pass *up front*)

#### L.0.1 Type contract (what it represents)

`TableSchema` = `{ file_schema, table_partition_cols, table_schema }` with cheap accessors; it precomputes the full schema and avoids repeated concatenation/allocation.

**Constructor + accessors (DF52):**

```rust
use std::sync::Arc;
use arrow_schema::{Schema, Field};

// file_schema: Arc<Schema>
// table_partition_cols: Vec<Arc<Field>>
let table_schema = TableSchema::new(file_schema, table_partition_cols);

let file_schema_ref = table_schema.file_schema();            // &Arc<Schema>
let partition_cols  = table_schema.table_partition_cols();   // &Vec<Arc<Field>>
let full_schema     = table_schema.table_schema();           // &Arc<Schema>
```

`TableSchema::new`, `file_schema`, `table_partition_cols`, `table_schema` are explicitly documented.

**Non-partitioned shorthand:** `TableSchema` implements `From<Arc<Schema>>`, so `SchemaRef` can be passed where `impl Into<TableSchema>` is accepted.

---

## L.1 DF51 → DF52: **schemas must be provided “up front”**

### L.1.1 Before (DF51): schema flowed via builder + `with_table_partition_cols`

Upgrade guide’s “before” shape (schematic):

```rust
// DF51 (conceptual, per upgrade guide):
let source = ParquetSource::default();
let config = FileScanConfigBuilder::new(url, schema, source)
    .with_table_partition_cols(vec![Field::new("date", DataType::Utf8, false)])
    .with_file(partitioned_file)
    .build();
```

DF52 guide explicitly shows the **old** constructor+builder signature and the presence of `with_table_partition_cols()` on the builder in migration examples.

### L.1.2 After (DF52): schema lives in the `FileSource` (constructed with `TableSchema`)

Upgrade guide’s “after” shape:

```rust
let table_schema = TableSchema::new(
    file_schema,
    vec![Arc::new(Field::new("date", DataType::Utf8, false))],
);
let source = ParquetSource::new(table_schema);

let config = FileScanConfigBuilder::new(url, Arc::new(source))
    .with_file(partitioned_file)
    .build();
```

Key deltas are listed explicitly in the DF52 upgrade guide:

1. FileSource constructors require `TableSchema`
2. `FileScanConfigBuilder::new(url, source)` (no separate schema param)
3. partition columns are part of `TableSchema` (builder method removed)

---

## L.2 Built-in file sources now take `impl Into<TableSchema>` (DF52)

### L.2.1 Parquet

```rust
// DF52 signature:
ParquetSource::new(table_schema: impl Into<TableSchema>) -> ParquetSource
```

This is the exact docs.rs signature.

**Non-partitioned:**

```rust
let file_source = Arc::new(ParquetSource::new(file_schema.clone())); // SchemaRef → Into<TableSchema>
```

**Partitioned:**

```rust
let table_schema = TableSchema::new(file_schema.clone(), partition_cols);
let file_source  = Arc::new(ParquetSource::new(table_schema));
```

(Why this works: `TableSchema::new` + `From<Arc<Schema>>` exist.)

### L.2.2 CSV

```rust
// DF52 signature:
CsvSource::new(table_schema: impl Into<TableSchema>) -> CsvSource
```

Exact signature in docs.rs.

(And CSV parsing options now attach to the source via `with_csv_options`.)

### L.2.3 JSON

```rust
// DF52 signature:
JsonSource::new(table_schema: impl Into<TableSchema>) -> JsonSource
```

Exact signature in docs.rs.

---

## L.3 `FileScanConfigBuilder` signature + schema invariants (DF52)

### L.3.1 `FileScanConfigBuilder::new` no longer accepts schema

**DF52 signature:**

```rust
pub fn new(
    object_store_url: ObjectStoreUrl,
    file_source: Arc<dyn FileSource>,
) -> FileScanConfigBuilder
```

Docs.rs explicitly states: “The file source must have a schema set via its constructor.”

So “schema upfront” is enforced as a builder precondition, not a convention.

### L.3.2 Projection indices are interpreted against `{file_schema + partition_cols}`

`with_projection_indices` explicitly documents the index mapping:

> “Indexes that are higher than the number of columns of `file_schema` refer to `table_partition_cols`.”

**Concrete example (index math is a common embedder footgun):**

```text
file_schema fields:      [user_id, amount]              (len = 2)
table_partition_cols:    [date, region]                 (len = 2)
table_schema fields:     [user_id, amount, date, region]

projection indices:
  [0, 2]  => user_id (file col 0), date (partition col 0)
  [3]     => region (partition col 1)
```

(Use this rule whenever your embedding passes projection indices downstream.)

---

## L.4 Partition columns: moved from builder calls → `TableSchema` construction (DF52)

### L.4.1 What was removed

Upgrade guide:

> “Partition columns are now part of TableSchema: the `with_table_partition_cols()` method has been removed from `FileScanConfigBuilder`.”

### L.4.2 What replaces it

Always construct `TableSchema` with partition cols *before* creating the `FileSource`:

```rust
let table_schema = TableSchema::new(file_schema, partition_cols);
let file_source  = Arc::new(ParquetSource::new(table_schema));
let config = FileScanConfigBuilder::new(url, file_source).build();
```

This is the canonical DF52 migration pattern shown in the upgrade guide.

---

## L.5 Custom `FileFormat` implementations: `file_source(table_schema)` (breaking)

### L.5.1 Trait signature (DF52)

`FileFormat` now requires:

```rust
fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource>;
```

This is the explicit docs.rs signature and the argument is defined as “includes partition columns.”

### L.5.2 Migration diff (exactly as the upgrade guide expresses it)

```diff
impl FileFormat for MyFileFormat {
-   fn file_source(&self) -> Arc<dyn FileSource> {
-       Arc::new(MyFileSource::default())
-   }
+   fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
+       Arc::new(MyFileSource::new(table_schema))
+   }
}
```

Upgrade guide provides this exact migration pattern.

---

## L.6 Custom `FileSource` implications (DF52): you must expose `table_schema()`

### L.6.1 Required method (DF52 `FileSource`)

Every `FileSource` must return a reference to its schema container:

```rust
fn table_schema(&self) -> &TableSchema;
```

…and it “always returns the unprojected schema (the full schema of the data).”

### L.6.2 Minimal custom source skeleton (schema stored once)

```rust
#[derive(Debug, Clone)]
pub struct MyFileSource {
    table_schema: TableSchema,
    // … opener config, metrics, etc …
}

impl MyFileSource {
    pub fn new(table_schema: TableSchema) -> Self {
        Self { table_schema }
    }
}

impl FileSource for MyFileSource {
    fn table_schema(&self) -> &TableSchema { &self.table_schema }

    fn create_file_opener(
        &self,
        object_store: Arc<dyn ObjectStore>,
        base_config: &FileScanConfig,
        partition: usize,
    ) -> Result<Arc<dyn FileOpener>, DataFusionError> {
        // …
    }

    fn with_batch_size(&self, batch_size: usize) -> Arc<dyn FileSource> { /* … */ }
    fn metrics(&self) -> &ExecutionPlanMetricsSet { /* … */ }
    fn file_type(&self) -> &str { "myfmt" }
}
```

(The “table_schema required method” is the key DF52 contract change for schema propagation.)

---

## L.7 Value proposition / value at risk (for embedders)

### L.7.1 Value proposition

* **Single source of truth for schema composition:** file columns + partition columns are unified in `TableSchema` and carried everywhere, reducing “schema mismatch” bugs caused by independently-threaded schema + partition col lists.
* **Lower allocation churn:** `TableSchema` precomputes/caches the full schema, avoiding repeated concatenation across scans / plans.
* **Cleaner extension contract:** `FileFormat::file_source(table_schema)` makes schema a first-class construction dependency rather than an after-the-fact mutation step.

### L.7.2 Value at risk (primary failure modes)

* **Index drift bugs:** many APIs accept column indices; if you add/reorder partition cols in `TableSchema`, you can silently change meanings of `with_projection_indices`, ordering metadata, and filter column references. Use the documented “indices > file_schema → partition cols” rule as your guardrail.
* **Partition columns now “always there”:** if your custom source previously treated partition columns as optional decoration, you must decide whether to materialize them in the opener/projection path (especially if you implement projection pushdown). FileSource docs explicitly state a `FileSource` that accepts a projection is expected to handle partition columns as well.

---

## L.8 Migration checklist (DF51 → DF52) — embedder tasks

1. **Replace builder constructor calls**

```diff
- FileScanConfigBuilder::new(url, schema, source)
+ FileScanConfigBuilder::new(url, source)
```

(Explicit upgrade guide delta.)

2. **Delete all `.with_table_partition_cols(...)` usage**

```diff
- .with_table_partition_cols(partition_cols)
+ // partition cols go into TableSchema::new(...)
```

(Explicitly removed in DF52.)

3. **Construct `TableSchema` and pass it into source constructors**

```rust
let table_schema = TableSchema::new(file_schema, partition_cols);
let source = Arc::new(ParquetSource::new(table_schema));
```

(Required by DF52 key changes; ParquetSource ctor signature confirms `impl Into<TableSchema>`.)

4. **Update custom `FileFormat` implementations**

```diff
- fn file_source(&self) -> Arc<dyn FileSource>
+ fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource>
```

(Upgrade guide + trait docs.)

5. **Audit projection index math**

* Any `with_projection_indices` call must be validated against the “file columns + partition cols” combined schema rule.

---

If you want the next increment in this same style: we can deep-dive **how `ListingOptions / ListingTable` produce a `TableSchema` (incl. Hive partition inference)** and how that `TableSchema` flows into `FileFormat::file_source → FileScanConfigBuilder → DataSourceExec` (including where partition values become literals for pushdown/projection).

## L-follow-on) ListingOptions / ListingTable → `TableSchema` → `FileFormat::file_source` → `FileScanConfigBuilder` → `DataSourceExec` (DF52) — **syntax-first** + value/risk callouts

---

### L.1 Inputs: the 3 objects that determine schema + partition behavior

#### L.1.1 `ListingOptions` (format + partition schema *intent*)

Key fields (DF52):

```rust
pub struct ListingOptions {
  pub format: Arc<dyn FileFormat>,
  pub file_extension: String,
  pub table_partition_cols: Vec<(String, DataType)>,
  pub collect_stat: bool,
  pub target_partitions: usize,
  pub file_sort_order: Vec<Vec<Sort>>,
}
```

`table_partition_cols` is the **expected Hive partition column names/types**. ([Docs.rs][1])

#### L.1.2 `ListingTableConfig` (file schema *only* + options)

`ListingTableConfig::with_schema(schema)` sets the **file schema** and must **exclude partition columns**. ([Docs.rs][2])

```rust
let config = ListingTableConfig::new(table_path)
  .with_listing_options(listing_options)
  .with_schema(file_schema_only /* NOT incl partition cols */);
```

([Docs.rs][2])

#### L.1.3 `ListingTable` (TableProvider that will build the scan plan)

Two key “planner entrypoints”:

* `scan(state, projection, filters, limit)` (TableProvider API) ([Docs.rs][3])
* `list_files_for_scan(ctx, filters, limit)` (ListingTable internal planner primitive) ([Docs.rs][3])

---

### L.2 Hive partition inference + validation (where partition columns come from)

#### L.2.1 Explicit partition columns (DDL / embedding)

SQL DDL lets you declare Hive partitions for pruning:

```sql
CREATE EXTERNAL TABLE taxi
STORED AS PARQUET
PARTITIONED BY (year, month)
LOCATION '/mnt/nyctaxi';
```

([datafusion.apache.org][4])

Rust embedding equivalent (declare expected path-derived columns + types):

```rust
let listing_options = ListingOptions::new(Arc::new(ParquetFormat::new()))
  .with_file_extension(".parquet")
  .with_table_partition_cols(vec![
    ("year".to_string(), DataType::Utf8),
    ("month".to_string(), DataType::Utf8),
  ]);
```

Semantics: partition values come **solely from the path** (not the parquet file). Files not matching the scheme are ignored. ([Docs.rs][1])

#### L.2.2 Infer partition *names* from Hive directories (optional automation)

`ListingOptions::infer_partitions(state, table_path) -> Result<Vec<String>>` infers partitioning from `LOCATION` without reading all files. ([Docs.rs][1])

Validation hook:

```rust
listing_options.validate_partitions(state, &table_path).await?;
```

This compares inferred partition columns in `LOCATION` vs columns provided in `PARTITIONED BY` / `table_partition_cols`. ([Docs.rs][1])

#### L.2.3 ListingTableFactory auto-inference switch (CLI / dynamic catalogs)

Runtime config:

```sql
SET datafusion.execution.listing_table_factory_infer_partitions = 'true';  -- default true
```

Meaning: ListingTables created via ListingTableFactory will infer Hive partitions and represent them in the table schema. ([datafusion.apache.org][5])

---

### L.3 File schema inference is **partition-blind**; `TableSchema` is the join point

#### L.3.1 Infer file schema (no partition cols)

```rust
let file_schema_only: Arc<Schema> =
  listing_options.infer_schema(session, &table_path).await?;
```

Note: inferred schema **does not include partitioning columns**. ([Docs.rs][1])

#### L.3.2 Build `TableSchema = file_schema + partition_cols`

```rust
use datafusion_datasource::TableSchema;

let partition_cols: Vec<Arc<Field>> = listing_options.table_partition_cols.iter()
  .map(|(name, dt)| Arc::new(Field::new(name, dt.clone(), false)))
  .collect();

let table_schema = TableSchema::new(file_schema_only, partition_cols);
```

`TableSchema::new(file_schema, table_partition_cols)` appends partition columns to form the full query-visible schema. ([Docs.rs][6])

**Value:** single “schema of record” for *both* scan adaptation and partition materialization; avoids repeated schema concatenation. ([Docs.rs][6])

---

### L.4 How `ListingTable` acquires `ListingOptions` (SessionState-driven path)

When you don’t provide `ListingOptions` explicitly, DF52 provides an **extension trait** for `ListingTableConfig` (`ListingTableConfigExt`) that can infer format/options using the SessionState’s file format factories:

```rust
let config = ListingTableConfig::new(table_path)
  .infer_options(session)      // picks file format by extension + compression suffix
  .await?
  .infer_schema(session)       // fills file_schema (no partition cols)
  .await?;
```

Mechanics (from DF52 source):

* lists one file to infer extension + optional compression suffix
* uses `SessionState::get_file_format_factory(ext).create(state, format_options)`
* builds `ListingOptions::new(file_format)`

  * `.with_target_partitions(state.config().target_partitions())`
  * `.with_collect_stat(state.config().collect_statistics())` ([Docs.rs][7])

---

### L.5 `TableSchema` → `FileFormat::file_source` → `FileScanConfigBuilder` → `DataSourceExec`

#### L.5.1 `FileFormat::file_source(table_schema)` (DF52 schema-forward contract)

Custom and built-in file formats now construct their `FileSource` using `TableSchema` (schema + partition cols). ([datafusion.apache.org][8])

```rust
// DF52 contract (conceptual):
let file_source: Arc<dyn FileSource> = listing_options.format.file_source(table_schema);
```

(You update custom FileFormat impls accordingly.) ([datafusion.apache.org][8])

#### L.5.2 `FileScanConfigBuilder::new(url, file_source)` (no separate schema param)

`FileScanConfigBuilder::new(object_store_url, file_source)` requires the file source already has schema via its constructor. ([datafusion.apache.org][8])

```rust
let config = FileScanConfigBuilder::new(object_store_url, file_source)
  .with_limit(limit)
  .with_projection_indices(projection_indices)?   // now fallible pushdown
  .build();
```

(“Use DataSourceExec::from_data_source to create a DataSourceExec from a FileScanConfig” is the canonical flow.) ([Docs.rs][9])

#### L.5.3 `ListingTable::scan` builds the **file list** + **PartitionedFile payload**

`ListingTable::list_files_for_scan(ctx, filters, limit)` returns grouped files + file-level stats for distribution. ([Docs.rs][3])

Each file becomes a `PartitionedFile`:

```rust
pub struct PartitionedFile {
  pub object_meta: ObjectMeta,
  pub partition_values: Vec<ScalarValue>,
  pub statistics: Option<Arc<Statistics>>,
  // ...
}
```

Partition values are appended to each row, and must match the count/order/type of the table partition columns. ([Docs.rs][10])

**Stats propagation:** when set, partition column stats are derived exactly from `partition_values` (`min=max=value`, `null_count=0`, `distinct_count=1`), enabling pruning and planning. ([Docs.rs][10])

#### L.5.4 Where the physical plan lands: `DataSourceExec`

`DataSourceExec` is the ExecutionPlan used by ListingTable; it wraps a `DataSource` (file scan config + file source behavior) and supports pushdown hooks. ([Docs.rs][11])

```rust
let exec: Arc<DataSourceExec> = DataSourceExec::from_data_source(file_scan_config);
```

([Docs.rs][11])

---

### L.6 Partition values → literals for pushdown/projection (the “where constants appear” step)

#### L.6.1 Why literals exist: partition columns are not in the file schema

Filters/projections can reference partition columns that **do not exist in the parquet/csv file schema**. DF52 requires explicit replacement of such columns with per-file literals *before* schema adaptation / pruning evaluation. ([datafusion.apache.org][8])

#### L.6.2 The DF52 canonical rewrite sequence (from upgrade guide)

```rust
use datafusion_physical_expr_adapter::replace_columns_with_literals;

// 1) Replace partition columns first
let expr_with_literals = replace_columns_with_literals(expr, &partition_values)?;

// 2) Then apply PhysicalExprAdapter rewrite to physical file schema
let adapted_expr = adapter.rewrite(expr_with_literals)?;
```

([datafusion.apache.org][8])

This is the *mechanical* bridge between:

* `PartitionedFile.partition_values` (path-derived constants) ([Docs.rs][10])
* scan pushdown / pruning predicates that must be expressed in **file schema terms** (adapter-rewritten) ([datafusion.apache.org][8])

#### L.6.3 File pruning API migration (partition fields removed)

If you use `FilePruner` directly, DF52 removed the `partition_fields` arg; partition handling must be done via the literal replacement step above. ([datafusion.apache.org][8])

```diff
- FilePruner::try_new(predicate, file_schema, partition_fields, file_stats)?
+ FilePruner::try_new(predicate, file_schema, file_stats)?
```

([datafusion.apache.org][8])

#### L.6.4 Dictionary encoding for partition columns (optional perf knob)

If you wrap partition column *types* (dictionary encoding), you must wrap partition *values* consistently:

* `ListingOptions` notes you may use `wrap_partition_type_in_dict` and then `wrap_partition_value_in_dict` for the per-file values. ([Docs.rs][1])

---

### L.7 Value proposition / value at risk (embedder-focused)

#### L.7.1 Value proposition (why this pipeline matters)

* **Partition predicate pruning before reads**: ListingTable adds partition columns from directory structure and can read only the matching partition directory under predicates like `WHERE date='2024-06-01'`. ([Docs.rs][3])
* **Planner + executor can use partition bounds**: partition stats are exact per file/group (derived from partition_values), enabling pruning and better planning. ([Docs.rs][10])
* **Schema-forward design**: `TableSchema` centralizes file schema + partition cols so all downstream pushdown and projection code uses a single schema-of-record. ([Docs.rs][6])

#### L.7.2 Value at risk (common footguns)

* **Partition value ordering/type mismatch**: `PartitionedFile.partition_values` MUST match the count/order/type of the table’s partition columns; mismatch can yield incorrect results. ([Docs.rs][10])
* **Forgetting literal substitution**: if you rely on partition columns in pruning/pushdown and don’t call `replace_columns_with_literals` first, pruning can silently fail or mis-evaluate. ([datafusion.apache.org][8])
* **Auto-inference surprises**: ListingTableFactory can infer partitions by default (`listing_table_factory_infer_partitions=true`), changing query-visible schema unexpectedly for “directory catalogs” unless you pin it. ([datafusion.apache.org][5])

---

If you want the next slice in the same style, the natural continuation is the **“projection + partition columns”** micro-mechanics: how projection indices map onto `file_schema + table_partition_cols`, and how `ProjectionOpener/SplitProjection` ensure partition columns are materialized (and constant-folded) even when the underlying format can only project file columns.

[1]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingOptions.html "ListingOptions in datafusion::datasource::listing - Rust"
[2]: https://docs.rs/datafusion-catalog-listing/latest/datafusion_catalog_listing/struct.ListingTableConfig.html "ListingTableConfig in datafusion_catalog_listing - Rust"
[3]: https://docs.rs/datafusion-catalog-listing/latest/datafusion_catalog_listing/struct.ListingTable.html "ListingTable in datafusion_catalog_listing - Rust"
[4]: https://datafusion.apache.org/user-guide/sql/ddl.html?utm_source=chatgpt.com "DDL — Apache DataFusion documentation"
[5]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[6]: https://docs.rs/datafusion/latest/datafusion/datasource/table_schema/struct.TableSchema.html?utm_source=chatgpt.com "TableSchema in datafusion::datasource::table_schema"
[7]: https://docs.rs/datafusion/latest/src/datafusion/datasource/listing/table.rs.html "table.rs - source"
[8]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[9]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.FileScanConfig.html?utm_source=chatgpt.com "FileScanConfig in datafusion::datasource::physical_plan"
[10]: https://docs.rs/datafusion-datasource/latest/datafusion_datasource/struct.PartitionedFile.html "PartitionedFile in datafusion_datasource - Rust"
[11]: https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.DataSourceExec.html "DataSourceExec in datafusion::datasource::memory - Rust"

## L-continuation) Projection + partition columns micro-mechanics (DF52) — **index mapping, SplitProjection, ProjectionOpener, constant folding**

---

### L.P0 Projection coordinate system (where indices are defined)

**`TableProvider::scan` projection** is *always* “indices into `TableProvider::schema`” (aka the **full table schema**) and must be returned “in the order specified.” ([Docs.rs][1])

```text
projection: Option<&[usize]>  // indices into TableProvider::schema
```

`ProjectionExprs::from_indices(&[...], schema)` preserves **ordering** (e.g., `[2,0,1]` stays that order) and permits **duplicates** (e.g., `[0,0]`). ([Docs.rs][2])

---

### L.P1 Table schema indexing rule (file columns first, partition columns appended)

`TableSchema` defines:

* **file schema** = actual on-disk columns
* **partition columns** = path-derived constants
* **table schema** = `[file_cols..., partition_cols...]` (partition cols appended **at the end**) ([Docs.rs][3])

This ordering is the basis for all “index arithmetic” below.

---

### L.P2 Scan-level projection pushdown (indices): `FileScanConfigBuilder::with_projection_indices`

At the file-scan layer, DF52 keeps an **indices-based** API:

```rust
FileScanConfigBuilder::with_projection_indices(indices: Option<Vec<usize>>) -> Result<Self>
```

Crucial rule:

> “Indexes that are higher than the number of columns of `file_schema` refer to `table_partition_cols`.” ([Docs.rs][4])

So if:

```text
n_file = file_schema.fields().len()
index i <  n_file  => file column i
index i >= n_file  => partition column (i - n_file)
```

This is the “raw” projection mapping for simple scans.

---

### L.P3 FileSource projection contract (DF52): must handle partition columns

DF52’s `FileSource::try_pushdown_projection(&ProjectionExprs)` makes partition handling explicit:

* If a `FileSource` accepts a projection, it is expected to handle it **in its entirety, including partition columns**.
* Typical mechanism: “replacing partition column references with literal values derived from each file’s partition values.”
* For “simple-column-only” sources (CSV etc.), DF52 explicitly recommends `SplitProjection` + `ProjectionOpener` and notes they “also handle partition column projection.” ([Docs.rs][5])

---

## L.P4 `SplitProjection`: split a table-schema projection into (file_indices + remainder + partition mapping)

### L.P4.1 What `SplitProjection` produces (public + internal payloads)

Docs define the goal precisely: split a `ProjectionExprs` (meant for table schema) into: ([Docs.rs][6])

* `file_indices`: projection indices into **file schema**
* `partition_value_indices` / mapping: precomputed mapping into `PartitionedFile.partition_values`
* `remapped_projection`: a projection to apply **after** file projection, with indices remapped such that:

  * file cols → `[0 .. file_indices.len())`
  * partition cols → `[file_indices.len() .. )` ([Docs.rs][6])

`SplitProjection::new(logical_file_schema, projection)` requires:

* `logical_file_schema` = “table schema minus partition columns”
* partition columns “expected to be at the end of the table schema”
* and warns that this schema is **logical**, not physical-file schema. ([Docs.rs][6])

### L.P4.2 Exact remap algorithm (source-true)

`SplitProjection::new` (source) does:

1. Walk each projection expression tree; collect all unique `Column(index, name)` references. ([Docs.rs][7])
2. Sort by original index; classify:

   * `< num_file_schema_columns` → file column
   * `>= num_file_schema_columns` → partition column ([Docs.rs][7])
3. Assign new indices:

   * file columns get packed to `0..num_file_columns`
   * partition columns get packed to `num_file_columns..` ([Docs.rs][7])
4. Transform all `Column` nodes in the projection into their remapped `Column(name, new_index)` equivalents. ([Docs.rs][7])
5. Precompute a `PartitionColumnIndex` per partition col:

```rust
pub struct PartitionColumnIndex {
  pub in_remainder_projection: usize,  // >= num_file_columns
  pub in_partition_values: usize,      // index into PartitionedFile.partition_values
}
```

and:

```text
in_partition_values = table_index - num_file_schema_columns
```

([Docs.rs][7])

This is the bridge from “table schema index” to “partition_values[] index”.

---

## L.P5 `ProjectionOpener`: apply remainder projection + inject partition literals (constant fold)

### L.P5.1 What it is

`ProjectionOpener` is a `FileOpener` wrapper that “applies a projection on top of an inner opener,” **including partition columns**, by splitting pushed-down projection into:

* “simple column indices / selection” (done by inner opener)
* “remainder projection” (applied by wrapper) ([Docs.rs][8])

Constructor:

```rust
ProjectionOpener::try_new(
  projection: SplitProjection,
  inner: Arc<dyn FileOpener>,
  file_schema: &Schema,
) -> Result<Arc<dyn FileOpener>>
```

([Docs.rs][8])

### L.P5.2 Core mechanics (source-true)

**Build-time** (`try_new`):

* Stores `projection.remapped_projection`
* Computes `input_schema = file_schema.project(&projection.file_indices)` ([Docs.rs][7])

**Open-time** (`open(partitioned_file)`):

1. Clone `partitioned_file.partition_values` ([Docs.rs][7])
2. If there are partition columns:

   * transform the projection expression trees:

     * any `Column` whose `index == PartitionColumnIndex.in_remainder_projection`
     * is replaced with a `Literal(ScalarValue)` pulled from `partition_values[in_partition_values]`
   * literals are pre-created to avoid repeated ScalarValue cloning ([Docs.rs][7])
3. Build a projector: `projection.make_projector(&input_schema)` and stream-map each inner batch through `projector.project_batch(&batch)`. ([Docs.rs][7])

This is literal, operator-level constant folding: partition columns become `Literal` physical expressions, not “late-joined” columns.

---

## L.P6 Where partition constants come from: `PartitionedFile.partition_values`

`PartitionedFile` carries per-file partition constants:

```rust
pub partition_values: Vec<ScalarValue>; // appended to each row
```

Rules (must-hold invariants):

* “Values of partition columns to be appended to each row.”
* MUST match the “same count, order, and type” as `table_partition_cols`. ([Docs.rs][9])

Optimization implication:

* When per-file statistics are set, DF52 computes exact stats for partition columns from `partition_values` (min=max=value, null_count=0, distinct_count=1), enabling pruning and planning. ([Docs.rs][9])

---

## L.P7 End-to-end worked example (indices → SplitProjection → ProjectionOpener)

**Schemas**

```text
file_schema:      [user_id@0, amount@1]                 (n_file = 2)
partition_cols:   [date@2, region@3]                    (appended)
table_schema:     [user_id@0, amount@1, date@2, region@3]
```

(“table schema is file schema + partition columns.”) ([Docs.rs][3])

**Query wants reorder + partition col:**

```sql
SELECT region, user_id FROM t;
```

Projection indices into table schema:

```text
projection_indices = [3, 0]     // region then user_id
```

Build physical projection (table schema indices) preserving order:

```rust
let proj = ProjectionExprs::from_indices(&[3, 0], table_schema.table_schema());
```

Ordering guarantee: `[3,0]` stays in that order. ([Docs.rs][2])

**SplitProjection**

```rust
let split = SplitProjection::new(table_schema.file_schema(), &proj);
```

What it yields (deterministic by source algorithm):

* `file_indices = [0]` (only `user_id` read from file)
* partition mapping for `region`:

  * `in_remainder_projection = 1` (because `num_file_columns = 1`)
  * `in_partition_values = 1` (because `table_index=3`, `num_file_schema_columns=2`, so `3-2=1`) ([Docs.rs][7])

Remainder projection is remapped to operate on the post-file-projection schema:

```text
input_schema after file projection: [user_id@0]
remapped columns: user_id@0, region@1
remapped projection preserves requested output order: [region@1, user_id@0]
```

(remap rules are specified in `SplitProjection` docs/source.) ([Docs.rs][6])

**ProjectionOpener wraps inner opener**

* Inner opener reads only `file_indices=[0]`
* Wrapper injects `Literal(partition_values[1])` where it sees `Column(index=1)` (region) ([Docs.rs][7])
* Wrapper projects each batch to `[region_literal, user_id]` ([Docs.rs][7])

**Result:** even if the underlying format supports only “select these file columns,” the scan still returns partition columns in the correct order, as constants, without reading them from file. ([Docs.rs][5])

---

## L.P8 Relationship to filter pushdown: same constant-fold principle, different hook

For predicates (pruning/pushdown), DF52 uses a general rewrite utility:

```rust
replace_columns_with_literals(expr, replacements: HashMap<col_name, ScalarValue>)
```

Use cases explicitly include “Partition column pruning” and “Constant folding.” ([Docs.rs][10])

ProjectionOpener does the analogous rewrite, but keyed off **remapped column indices** (not names), replacing partition `Column` nodes with `Literal` nodes. ([Docs.rs][7])

---

## L.P9 Value proposition / value at risk

### Value proposition (why this design exists)

* **Max projection pushdown even for “dumb” formats:** read minimal file columns; partition cols are injected as literals at open-time. ([Docs.rs][5])
* **Stable semantics for partition columns:** partition values are constant per file and formally carried as `PartitionedFile.partition_values`, with exact per-file stats derivable. ([Docs.rs][9])
* **Separates “what must be read” vs “what must be produced”:** `file_indices` are for IO; `remapped_projection` is for output shaping. ([Docs.rs][6])

### Value at risk (common embedder failure modes)

1. **Partition value misalignment** (hard correctness bug)
   `partition_values` must match partition columns’ count/order/type; otherwise literals injected into the projection are wrong. ([Docs.rs][9])

2. **Schema ordering invariant violation**
   `SplitProjection` assumes partition columns are at the end of the table schema. If you construct a nonconforming schema, remap math breaks. ([Docs.rs][7])

3. **Filter-only column hazards**
   DataFusion can push filters into scans even when the projected output does not include those filter columns (example given in `TableProvider` docs). If your file source only reads `file_indices` derived from projection and ignores filter columns, predicate evaluation can fail or force late filtering. ([Docs.rs][1])

4. **Plan-property regressions (ordering / equivalence)**
   Projection pushdown changes the scan’s output schema and must also correctly transform ordering metadata; a DF52 regression report notes “pre-sorted data was being resorted” due to not applying projections to existing orderings. ([GitHub][11])

---

If you want the next slice: I can do the same “syntax-first” treatment for **how filters + projection combine inside DataSourceExec** in DF52 (i.e., how scan decides which *extra* file columns must be read to evaluate pushed-down filters even if not in projection, and where `PhysicalExprAdapter` vs `ProjectionOpener` runs in that pipeline).

[1]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html "TableProvider in datafusion::datasource - Rust"
[2]: https://docs.rs/datafusion/latest/datafusion/physical_expr/projection/struct.ProjectionExprs.html "ProjectionExprs in datafusion::physical_expr::projection - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/datasource/table_schema/struct.TableSchema.html "TableSchema in datafusion::datasource::table_schema - Rust"
[4]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.FileScanConfigBuilder.html "FileScanConfigBuilder in datafusion::datasource::physical_plan - Rust"
[5]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/trait.FileSource.html "FileSource in datafusion::datasource::physical_plan - Rust"
[6]: https://docs.rs/datafusion-datasource/52.1.0/x86_64-unknown-linux-gnu/datafusion_datasource/projection/struct.SplitProjection.html "SplitProjection in datafusion_datasource::projection - Rust"
[7]: https://docs.rs/datafusion-datasource/52.1.0/src/datafusion_datasource/projection.rs.html "projection.rs - source"
[8]: https://docs.rs/datafusion-datasource/52.1.0/x86_64-unknown-linux-gnu/datafusion_datasource/projection/struct.ProjectionOpener.html "ProjectionOpener in datafusion_datasource::projection - Rust"
[9]: https://docs.rs/datafusion-datasource/latest/datafusion_datasource/struct.PartitionedFile.html "PartitionedFile in datafusion_datasource - Rust"
[10]: https://docs.rs/datafusion/latest/datafusion/physical_expr_adapter/fn.replace_columns_with_literals.html?utm_source=chatgpt.com "replace_columns_with_literals in datafusion"
[11]: https://github.com/apache/datafusion/issues/20173?utm_source=chatgpt.com "Existing file ordering is lost in some cases with projections"

## L-filters + projection inside `DataSourceExec` (DF52) — **how extra columns are read, and where `PhysicalExprAdapter` vs `ProjectionOpener` runs**

---

### L.F0 The contract that forces “extra columns” (filter-only columns)

`TableProvider::scan(state, projection, filters, limit)` defines:

* `projection: Option<&Vec<usize>>` = indices into `Self::schema()` (table schema), order-preserving.
* `filters: &[Expr]` = logical predicates; may be pushed down.
* **Important:** *columns may appear only in filters*, and when filter pushdown is complete the projection may omit them. The docs explicitly walk the canonical case:

```text
SELECT t.a FROM t WHERE t.b > 5
Final Scan payload:
  filter=(t.b > 5)
  projection=(t.a)
BUT internally evaluating the predicate still requires t.b
```

([Docs.rs][1])

This is the “why” behind everything below: **scan output projection ≠ scan internal read-set**.

---

## L.F1 `DataSourceExec` is just the routing layer: it delegates pushdowns

### L.F1.1 Projection pushdown entrypoint (`ProjectionExec` → `DataSource::try_swapping_with_projection`)

`DataSourceExec::try_swapping_with_projection` delegates to `data_source.try_swapping_with_projection(projection.projection_expr())` and wraps the new data source back into a `DataSourceExec`. ([Docs.rs][2])

### L.F1.2 Filter pushdown entrypoint (upward phase)

`DataSourceExec::handle_child_pushdown_result` takes the remaining parent filters and calls:

```rust
self.data_source.try_pushdown_filters(parent_filters, config)?
```

and if the data source is updated, it recomputes plan properties. ([Docs.rs][2])

**Implication:** the actual “filters + projection combine” logic lives in the underlying `DataSource` implementation, i.e. `FileScanConfig` for file scans.

---

## L.F2 `FileScanConfig` = the merge point for (projection ↔ filters ↔ schema indices)

### L.F2.1 Projection pushdown: `FileScanConfig::try_swapping_with_projection`

In DF52, file scans push projection into the `FileSource`:

```rust
match self.file_source.try_pushdown_projection(projection)? {
  Some(new_source) => { self.clone().file_source = new_source; ... }
  None => Ok(None)
}
```

([Docs.rs][3])

So the scan’s “output shaping” projection is stored in `FileSource::projection() -> Option<&ProjectionExprs>`. ([Docs.rs][4])

---

### L.F2.2 Filter pushdown: `FileScanConfig::try_pushdown_filters` remaps filters through projection, then into table schema

This is the most important DF52 micro-mechanic: **filters can be created against a projected schema (with aliases / reordered indices)**, but the `FileSource` expects filters expressed against the **table schema (file + partition columns)**.

`FileScanConfig::try_pushdown_filters` does two explicit rewrites before calling `FileSource::try_pushdown_filters(...)`: ([Docs.rs][3])

#### Step A — if a projection exists, map the filter *back through* the projection (undo aliasing / expression projection)

Source comment gives the exact example:

```text
filter:   c1_c2 > 5
projection output had: c1 + c2 as c1_c2
rewrite filter back to: c1 + c2 > 5
```

Mechanism (literal code path):

```rust
use datafusion_physical_plan::projection::update_expr;
update_expr(&filter, projection.as_ref(), true)?
  .ok_or_else(|| internal_err!("Failed to map filter through projection"))
```

([Docs.rs][3])

#### Step B — remap `Column` indices to the **table schema** (file + partition cols)

After alias rewrite, it remaps column indices:

```rust
reassign_expr_columns(filter, table_schema.as_ref())
```

so the resulting filter’s `Column(index)` matches `table_schema = file_cols + partition_cols`. ([Docs.rs][3])

#### Step C — delegate: `FileSource::try_pushdown_filters(remapped_filters, config)`

Finally, it calls the `FileSource` hook:

```rust
fn try_pushdown_filters(
  &self,
  filters: Vec<Arc<dyn PhysicalExpr>>,
  config: &ConfigOptions,
) -> Result<FilterPushdownPropagation<Arc<dyn FileSource>>>
```

([Docs.rs][4])

**Net:** DF52 ensures filter pushdown remains correct even when projection pushdown has already occurred (and changed schema/indices).

---

## L.F3 Where the scan decides “extra columns to read” (output projection vs predicate read-set)

This is **format-specific** and lives inside the `FileSource`.

### L.F3.1 Parquet is the canonical “projection excludes filter columns, but scan still reads them”

`ParquetSource` stores **two independent payloads**:

* `projection: ProjectionExprs` (output shaping)
* `predicate: Option<Arc<dyn PhysicalExpr>>` (scan-time filtering / pruning)

You can see both in the ParquetSource implementation:

* `projection()` returns `Some(&self.projection)` ([Docs.rs][5])
* `filter()` returns `self.predicate.clone()` ([Docs.rs][5])

Projection pushdown merges projections:

```rust
source.projection = self.projection.try_merge(projection)?;
```

([Docs.rs][5])

Filter pushdown decides whether filters become “scan-responsibility” based on config, **but always folds supported filters into `predicate` for pruning**:

* `pushdown_filters = self.pushdown_filters() || config.execution.parquet.pushdown_filters`
* build `allowed_filters` where `PushedDown::Yes`
* set `source.predicate = Some(conjunction(existing_predicate, allowed_filters))`
* if `pushdown_filters == false`, report `PushedDown::No` back to parents (parents must still filter), but predicate is still used for stats pruning
* unsupported filters become `PushedDown::No` and remain parent-handled

([Docs.rs][5])

#### **The “extra columns” mechanism for Parquet is late materialization**

DataFusion’s Parquet filter pushdown explicitly works by reading **filter columns first**, computing a row-selection, and then reading **projection columns only for matching rows**. ([datafusion.apache.org][6])

A config knob makes this two-phase pipeline concrete:

* `datafusion.execution.parquet.max_predicate_cache_size`: cache “results of predicate evaluation between filter evaluation and output generation” (memory/perf trade). ([datafusion.apache.org][7])

So for the canonical query:

```sql
SELECT a FROM t WHERE b > 5;
```

the **scan payload** can legitimately be:

```text
Scan:
  projection=(a)
  filter/predicate=(b > 5)
```

where column `b` is **not** in the output projection but is still read internally for predicate evaluation. ([Docs.rs][1])

---

## L.F4 Execute-time ordering: where `ProjectionOpener` and `PhysicalExprAdapter` run

### L.F4.1 The execution envelope (`FileScanConfig::open`)

At runtime, `FileScanConfig::open` does:

```rust
let source = self.file_source.with_batch_size(batch_size);
let opener = source.create_file_opener(object_store, self, partition)?;
let stream = FileStream::new(self, partition, opener, source.metrics())?;
```

So **whatever projection/filter mechanics you implement get realized inside `create_file_opener` (and downstream stream decode)**. ([Docs.rs][3])

---

### L.F4.2 `ProjectionOpener` runs in the **opener layer** (format can only push down file-column selections)

DF52’s `FileSource` contract says:

* If a `FileSource` accepts a projection, it must handle it *including partition columns*, typically by **replacing partition column references with literal values derived from each file’s partition values**.
* If it can’t push down complex projections (CSV-like), it should use **`SplitProjection` + `ProjectionOpener`** to push down the file-index part and apply the remainder on top; these helpers “also handle partition column projection.” ([Docs.rs][4])

**Interpretation (pipeline):**

```text
create_file_opener():
  inner opener reads file_indices (IO-level projection)
  ProjectionOpener wraps inner:
    - injects partition columns as literals
    - applies remainder projection to produce final output schema/order
```

This is where “partition columns are materialized / constant-folded” even if the format can’t project them from the file.

---

### L.F4.3 `PhysicalExprAdapter` runs in the **expression/schema adaptation layer** (filters/projections and schema evolution)

There are two closely related uses:

#### (A) Adapting pushed-down expressions to each file’s *physical* schema

`FileScanConfig` carries:

```text
expr_adapter_factory: Option<Arc<dyn PhysicalExprAdapterFactory>>
```

documented as “adapt filters and projections that are pushed down into the scan from the logical schema to the physical schema of the file.” ([Docs.rs][8])

A common prerequisite step for partitioned datasets is **literal substitution** on partition columns:

```rust
replace_columns_with_literals(expr, replacements)
```

Use cases explicitly include “partition column pruning” and “constant folding.” ([Docs.rs][9])

So the canonical predicate prep per file is:

```text
predicate(table schema)
  -> replace partition cols with literals (per PartitionedFile.partition_values)
  -> PhysicalExprAdapter::rewrite(...) to the file’s physical schema
  -> pruning/row-filter evaluation
```

#### (B) Adapting output `RecordBatch` to the table schema (schema evolution)

`ParquetSource`’s execution overview explicitly states that after reading batches:

> “it may be adapted by a DefaultPhysicalExprAdapterFactory to match the table schema… missing columns are filled with nulls” (customizable). ([Docs.rs][10])

**Implication:** `ProjectionOpener` is about *output shaping + partition column injection*; `PhysicalExprAdapter` is about *schema correctness across files + expression evaluation against physical schemas*.

---

## L.F5 Practical checklist (agent-ready)

### (1) If your `FileSource` pushes filters down (`PushedDown::Yes`)

You must support the TableProvider invariant:

```text
projection may omit filter-only columns
but scan must still evaluate predicate correctly
```

([Docs.rs][1])

Parquet does this via late materialization (read predicate columns first, then output columns) and predicate result caching (max_predicate_cache_size). ([datafusion.apache.org][6])

### (2) If your `FileSource` cannot push filters down

Return `PushedDown::No` and rely on the normal plan shape where filter columns remain in the scan projection until the upstream `FilterExec` is satisfied (DataFusion’s optimizer handles this; your job is just to not lie about pushdown). ([Docs.rs][5])

### (3) If your source supports projection pushdown but only as file-column selection

Use `SplitProjection` + `ProjectionOpener` inside `create_file_opener` to:

* read only `file_indices` (IO savings),
* inject partition values as literals,
* apply remainder projection on top.

([Docs.rs][4])

### (4) Always respect DF52’s filter remap rules

If you accept pushed-down filters at the scan layer, expect them to arrive already:

* mapped back through projection aliases (`update_expr`)
* remapped to table-schema indices (`reassign_expr_columns`)

because `FileScanConfig::try_pushdown_filters` enforces this before calling you. ([Docs.rs][3])

[1]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html "TableProvider in datafusion::datasource - Rust"
[2]: https://docs.rs/datafusion-datasource/52.1.0/x86_64-unknown-linux-gnu/src/datafusion_datasource/source.rs.html "source.rs - source"
[3]: https://docs.rs/datafusion-datasource/52.1.0/x86_64-unknown-linux-gnu/src/datafusion_datasource/file_scan_config.rs.html "file_scan_config.rs - source"
[4]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/trait.FileSource.html "FileSource in datafusion::datasource::physical_plan - Rust"
[5]: https://docs.rs/datafusion-datasource-parquet/latest/src/datafusion_datasource_parquet/source.rs.html "source.rs - source"
[6]: https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/?utm_source=chatgpt.com "Efficient Filter Pushdown in Parquet - Apache DataFusion Blog"
[7]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[8]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.FileScanConfig.html "FileScanConfig in datafusion::datasource::physical_plan - Rust"
[9]: https://docs.rs/datafusion/latest/datafusion/physical_expr_adapter/fn.replace_columns_with_literals.html?utm_source=chatgpt.com "replace_columns_with_literals in datafusion"
[10]: https://docs.rs/datafusion-datasource-parquet/latest/datafusion_datasource_parquet/source/struct.ParquetSource.html "ParquetSource in datafusion_datasource_parquet::source - Rust"

## M) Physical optimizer extension surface (DF52) — `PhysicalOptimizerRule` API reality, the (reverted) `optimize_plan` proposal, and how this interacts with DF52 rules (sort pushdown, dynamic filters, coalescing)

---

### M.0 Reality check: DF52 **did not ship** `PhysicalOptimizerRule::optimize_plan` (it was merged, then reverted)

A PR in the 52 cycle introduced `PhysicalOptimizerRule::optimize_plan(…, OptimizerContext)` and deprecated `optimize(…, ConfigOptions)` for “more context than config options,” including typed `Extensions` from `SessionConfig`. ([GitHub][1])
That PR was subsequently reverted (“Revert adding PhysicalOptimizerRule::optimize_plan …”) so **DF52.0/52.1 remain on the `optimize(plan, &ConfigOptions)` signature**. ([GitHub][1])

**Implication for the 52 vs 51 doc:** there is *no shipped* “optimize → optimize_plan” migration in 52. The actionable surface is still `optimize`. ([Docs.rs][2])

---

## M.1 Shipped rule trait surface (DF51 and DF52): identical, syntax-first

### M.1.1 `PhysicalOptimizerRule` (DF51)

```rust
pub trait PhysicalOptimizerRule: Debug {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    fn name(&self) -> &str;

    fn schema_check(&self) -> bool;
}
```

([Docs.rs][3])

### M.1.2 `PhysicalOptimizerRule` (DF52)

Same signature in DF52.1.0 (and 52.0.0). ([Docs.rs][2])

**Value / risk:**

* **Value:** stable extension surface for DF52 embedders (no forced migration). ([Docs.rs][2])
* **Risk:** if you *need* session-scoped custom context (e.g., `SessionConfig.extensions`) inside a rule, DF52 does not provide it via the method signature; you must inject it via your rule’s constructor or by capturing it in the rule instance.

---

## M.2 Registering custom physical optimizer rules (DF52 syntax)

In DF52, custom physical optimizer rules are appended via `SessionStateBuilder`:

```rust
use std::sync::Arc;
use datafusion::prelude::*;
use datafusion_physical_optimizer::PhysicalOptimizerRule;

let my_rule: Arc<dyn PhysicalOptimizerRule + Send + Sync> = Arc::new(MyRule::new());

let state = SessionStateBuilder::new()
    .with_config(SessionConfig::new())
    .with_default_features()
    .with_physical_optimizer_rule(my_rule)  // appends to physical optimizer rules
    .build();
```

`SessionStateBuilder::with_physical_optimizer_rule` explicitly “adds `physical_optimizer_rule` to the end of the list of `PhysicalOptimizerRule`s used to rewrite queries.” ([Docs.rs][4])

**Ordering consequence:** because this appends, your rule runs **after** the built-in default physical optimizer rules unless you replace the full optimizer/rule list (see §M.4 for why that can be unsafe for DF52 dynamic filters).

---

## M.3 How rules rewrite plans in practice (DF52: TreeNode transforms + with_new_children)

### M.3.1 Pattern A — `transform_up` wrapper insertion (example: CoalesceBatches for AsyncFuncExec)

DF52’s `CoalesceBatches` rule is a canonical “wrap child with a new `ExecutionPlan` node” rewrite:

```rust
plan.transform_up(|plan| {
    if let Some(async_exec) = plan.as_any().downcast_ref::<AsyncFuncExec>() {
        let children = async_exec.children();
        let coalesce_exec = Arc::new(CoalesceBatchesExec::new(
            Arc::clone(children[0]),
            target_batch_size,
        ));
        let new_plan = plan.with_new_children(vec![coalesce_exec])?;
        Ok(Transformed::yes(new_plan))
    } else {
        Ok(Transformed::no(plan))
    }
})
.data()
```

It is gated by `config.execution.coalesce_batches` and uses `config.execution.batch_size` to pick the target batch size. ([Docs.rs][5])

**DF52 coalescing interaction:** even though DF52 moved a lot of coalescing “inside operators,” the physical optimizer still uses `CoalesceBatchesExec` *selectively* (here: to reduce invocations of `AsyncFuncExec`). This is the practical “operator-embedded coalescing + targeted optimizer wrapper” split you need to design around. ([Docs.rs][5])

---

### M.3.2 Pattern B — `transform_down` + delegated pushdown hooks (example: PushdownSort)

DF52 introduces a dedicated **physical optimizer rule** `PushdownSort` that:

* finds `SortExec`
* calls `try_pushdown_sort()` on its input
* rewrites based on `SortOrderPushdownResult::{Exact, Inexact, Unsupported}`

Core loop:

```rust
plan.transform_down(|plan| {
    let Some(sort_exec) = plan.as_any().downcast_ref::<SortExec>() else {
        return Ok(Transformed::no(plan));
    };

    let sort_input = Arc::clone(sort_exec.input());
    let required_ordering = sort_exec.expr();

    match sort_input.try_pushdown_sort(required_ordering)? {
        SortOrderPushdownResult::Exact { inner } => Ok(Transformed::yes(inner)),
        SortOrderPushdownResult::Inexact { inner } => Ok(Transformed::yes(Arc::new(
            SortExec::new(required_ordering.clone(), inner)
                .with_fetch(sort_exec.fetch())
                .with_preserve_partitioning(sort_exec.preserve_partitioning()),
        ))),
        SortOrderPushdownResult::Unsupported => Ok(Transformed::no(plan)),
    }
})
.data()
```

It is gated by `config.optimizer.enable_sort_pushdown`. ([Docs.rs][6])

**Value:** this rule is the bridge between DF52 scan-level sort pushdown (sources advertising ordering optimizations) and plan-level sort removal/TopK acceleration (Exact vs Inexact). ([Docs.rs][6])

---

## M.4 Default physical optimizer pipeline changed from DF51 → DF52 (new rules + ordering differences)

### M.4.1 DF51 default rule list (notable tail)

DF51 includes (in order, excerpt):

* `ProjectionPushdown`
* `CoalesceBatches`
* `CoalesceAsyncExecInput`
* `OutputRequirements::remove`
* `TopKAggregation`
* `LimitPushPastWindows`
* `LimitPushdown`
* `ProjectionPushdown` (second pass)
* `EnsureCooperative`
* `FilterPushdown::new_post_optimization()` (dynamic filters)
* `SanityCheckPlan` ([Docs.rs][3])

### M.4.2 DF52 default rule list (notable deltas)

DF52 includes:

* **removes `CoalesceAsyncExecInput`**
* keeps `CoalesceBatches`
* **adds `PushdownSort`** (new)
* keeps `FilterPushdown::new_post_optimization()` late as a dedicated “dynamic filter safe” phase
* keeps `SanityCheckPlan` final gate ([Docs.rs][2])

---

## M.5 The “dynamic filter reference hazard” (why rule ordering matters more in DF52)

DF52’s own default pipeline has an explicit warning:

> “This FilterPushdown handles dynamic filters that may have references to the source ExecutionPlan. Therefore it should be run at the end … since any changes to the plan may break the dynamic filter’s references.” ([Docs.rs][2])

**Actionable embedder rule:** if you append a custom physical optimizer rule **after** DF52’s post-optimization `FilterPushdown`, you risk invalidating dynamic filter references (join/topk/aggregate dynamic filters). To be safe:

* either run your rule **before** that late phase (by replacing the whole optimizer rule list), or
* ensure your rewrite is “structure preserving” with respect to any dynamic filter references (hard, usually not worth it). ([Docs.rs][2])

---

## M.6 The (reverted) `optimize_plan` proposal — what it was, and how to future-proof anyway

### M.6.1 Proposed API shape (as described in PR #18739)

The PR introduced:

* `OptimizerContext` wrapping `ConfigOptions` + typed `Extensions`
* `PhysicalOptimizerRule::optimize_plan(plan, &OptimizerContext)` + deprecate `optimize(plan, &ConfigOptions)`
* backward compatibility via default implementation
  …but then reverted. ([GitHub][1])

### M.6.2 Future-proof pattern that works **today** in DF52

Because DF52 only passes `&ConfigOptions`, “session-specific knobs” must be captured in your rule instance:

```rust
#[derive(Debug)]
struct MyRule {
    // your own extension surface; do not depend on SessionConfig being passed
    my_flag: bool,
}

impl PhysicalOptimizerRule for MyRule {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions)
      -> Result<Arc<dyn ExecutionPlan>> {
        if !self.my_flag { return Ok(plan); }
        // rewrite using transform_up / transform_down
        Ok(plan)
    }
    fn name(&self) -> &str { "my_rule" }
    fn schema_check(&self) -> bool { true }
}
```

If/when a future DataFusion version reintroduces an `OptimizerContext`, the migration is then localized: you swap `self.my_flag` sourcing to `ctx.session_config().extensions()`-style access (rather than rewriting core logic). ([GitHub][1])

---

### What we should deep dive next (if you want to keep going)

The next “physical optimizer extension” topic with high leverage is **`ExecutionPlan::try_pushdown_sort` / `SortOrderPushdownResult` semantics** as a contract for custom `ExecutionPlan` nodes (transparent vs blocking nodes), because DF52’s `PushdownSort` rule makes this hook central. ([Docs.rs][6])

[1]: https://github.com/apache/datafusion/pull/18739 "Add PhysicalOptimizerRule::optimize_plan to allow passing more context into optimizer rules by adriangb · Pull Request #18739 · apache/datafusion · GitHub"
[2]: https://docs.rs/crate/datafusion-physical-optimizer/latest/source/src/optimizer.rs "datafusion-physical-optimizer 52.1.0 - Docs.rs"
[3]: https://docs.rs/crate/datafusion-physical-optimizer/51.0.0/source/src/optimizer.rs "datafusion-physical-optimizer 51.0.0 - Docs.rs"
[4]: https://docs.rs/datafusion/latest/src/datafusion/execution/session_state.rs.html "session_state.rs - source"
[5]: https://docs.rs/crate/datafusion-physical-optimizer/latest/source/src/coalesce_batches.rs "datafusion-physical-optimizer 52.1.0 - Docs.rs"
[6]: https://docs.rs/crate/datafusion-physical-optimizer/latest/source/src/pushdown_sort.rs "datafusion-physical-optimizer 52.1.0 - Docs.rs"

According to a document from **Mon 12 January 2026**, DataFusion 52 introduces **sort pushdown to scans** (and related physical-optimizer wiring) as a first-class performance feature, especially for **Top-K** patterns on pre-sorted data. ([Apache DataFusion][1])
Building on your DF52↔DF51 change catalog, the high-leverage “physical optimizer extension” surface is now the **`ExecutionPlan::try_pushdown_sort` / `SortOrderPushdownResult`** contract, because DF52’s **`PushdownSort`** rule calls it directly and expects custom nodes to participate correctly. 



--/ ult` (DF52 contract for custom physical nodes)

### M.7.1 What changed in DF52 (why this is now “central”)

Your doc already notes DF52 adds **`PushdownSort`** to the default physical optimizer pipeline. 
That rule:

* finds `Sor the *input*
* rewrites the plan based on `SortOrderPushdownResult::{Exact|Inexact|Unsupported}` (Exact → remove Sort; Inexact → keep Sort but swap in an “optimized” input). 

This is **gated by** `data:contentReference[oaicite:8]{index=8}ult `true`). :contentReference[oaicite:9]{index=9}  
Practical implication: **any custom `ExecutionPlan`node you insert between`SortExec` and the leaf** can now become the difference between “DF52 gets the win” vs “DF52 can’t reach the source and does nothing.”

---

### M.7.2 Public surface (exact signatures + default behavior)

#### ExecutionPlan hook

```rust
fn try_pushdown_sort(
    &self,
    order: &[PhysicalSortExpr],
) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>, DataFusionError>;
```

The trait doc is explicit about semantics, and the **default implementation returns `Unsupported`**. ([Docs.rs][2])
So: **no compile break** for existing custom nodes, but also **no participation** unless you override.

#### Result type and helper methods

```rust
pub enum SortOrderPushdownResult<T> {
    Exact { inner: T },
    Inexact { inner: T },
    Unsupported,
}
```

([Docs.rs][3])

Key helper methods you should treat as part of the contract surface:

* `map(...)` / `try_map(...)`: transform `inner` while preserving `Exact` vs `Inexact` vs `Unsupported`. ([Docs.rs][3])
* `into_inexact()`: **downgrade Exact → Inexact** when a wrapper cannot preserve “perfect ordering” even if the child can. ([Docs.rs][3])

---

### M.7.3 Semantics: what “Exact” vs “Inexact” really promises

#### `Exact`

* Meaning: this node can **guarantee** the requested ordering (“data is perfectly sorted” for the request). ([Docs.rs][3])
* Optimizer consequence: **it may remove `SortExec`** above this subtree. ([Docs.rs][3])

This aligns with DF’s broader ordering analysis: if the input is already ordered for the requirement, you can remove a sort and still be correct. ([Apache DataFusion][4])

#### `Inexact`

* Meaning: this node applied a **real optimization** for the requested ordering, but **cannot guarantee perfect sorting**. ([Docs.rs][3])
* Optimizer consequence: **keep the Sort** for correctness, but use the optimized input (so Top-K can “terminate earlier” / scan less). ([Docs.rs][3])

#### `Unsupported`

* Meaning: do nothing; stop the pushdown chain at this node. ([Docs.rs][3])

---

### M.7.4 Transparent vs blocking nodes (the contract classification)

The `PushdownSort` module-level docs (from the PR) define the core taxonomy you want your own nodes to fit into:

* **Transparent nodes**: delegate to children and wrap the result.
* **Data sources**: decide whether they can optimize for ordering.
* **Blocking nodes**: return `Unsupported` to stop pushdown. ([Mail Archive][5])

This matches the `ExecutionPlan` trait guidance: “For transparent nodes (that preserve ordering), implement this to delegate to children and wrap the result.” ([Docs.rs][2])

#### Concrete “blocking” example (don’t guess — use the docs)

`CoalescePartitionsExec` is explicitly **not order-preserving**: it merges partitions and makes **no guarantees** about output order. ([Docs.rs][6])
That should strongly bias you toward `Unsupported` (or, if you’re doing something exotic, at least downgrading via `into_inexact()`).

#### Concrete “conditionally blocking” example: repartition

`RepartitionExec` states that when more than one stream is repartitioned, output is “some arbitrary interleaving (and thus unordered) unless … preserve order” is enabled. ([Docs.rs][7])
So for sort pushdown: `RepartitionExec` is **only “transparent” under preserve-order semantics**; otherwise it is blocking.

---

### M.7.5 Implementation patterns for custom nodes (copy/paste templates)

#### Pattern A — “Pure wrapper” (transparent, schema-preserving)

Use this for: tracing wrappers, metrics wrappers, cooperative wrappers, “decorator” nodes.

**Rule:** delegate `try_pushdown_sort` to the child and re-wrap `inner` using `map/try_map`.

```rust
use std::sync::Arc;
use datafusion_common::Result;
use datafusion_physical_plan::{ExecutionPlan, SortOrderPushdownResult};
use datafusion_physical_expr::PhysicalSortExpr;

impl ExecutionPlan for MyWrapperExec {
    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        // Delegate to child; preserve variant using try_map
        self.input
            .try_pushdown_sort(order)?
            .try_map(|new_child| {
                Ok(Arc::new(Self::new(new_child, self.wrapper_config.clone()))
                    as Arc<dyn ExecutionPlan>)
            })
    }

    // (Also: maintains_input_order should be true for this child;
    // and output_ordering should track input if you truly preserve order.)
}
```

Why this matters: without this, your wrapper becomes an “accidental blocker” and DF52 can’t reach the leaf even though your node *doesn’t actually disturb ordering*. ([Docs.rs][2])

#### Pattern B — “Wrapper that may break exactness” (downgrade)

Use this when your node *can still benefit* from a child optimization but cannot promise “perfect ordering” itself.

```rust
fn try_pushdown_sort(&self, order: &[PhysicalSortExpr]) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
    let res = self.input.try_pushdown_sort(order)?;
    // This wrapper makes exactness unsafe → downgrade
    let res = res.into_inexact(); // Exact→Inexact, others unchanged
    res.try_map(|new_child| Ok(Arc::new(Self::new(new_child)) as Arc<dyn ExecutionPlan>))
}
```

`into_inexact()` exists *specifically* for this “wrapper degrades exactness” situation. ([Docs.rs][3])

#### Pattern C — “Schema-shaping wrapper” (projection/rename/derive): only delegate when you can rewrite the ordering safely

If your node changes the schema, you must ensure the pushed order is expressed in terms of what the child can produce. If you *don’t* adjust, you can create an invalid plan (this happened historically when join nodes pushed a `SortExec` “as-is” and the sort expression no longer referenced valid columns). ([GitHub][8])

**Minimum safe rule (practical):**
*Only delegate sort pushdown when the ordering is a pure `Column` reference that you can map 1:1 through your wrapper.*

Pseudo-structure:

```rust
fn try_pushdown_sort(&self, order: &[PhysicalSortExpr]) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
    let Some(mapped) = self.try_map_ordering_to_input(order) else {
        return Ok(SortOrderPushdownResult::Unsupported);
    };

    self.input.try_pushdown_sort(&mapped)?
        .try_map(|new_child| Ok(Arc::new(Self::with_new_child(new_child)) as Arc<dyn ExecutionPlan>))
}
```

**Don’t cheat here.** If your wrapper rewrites expressions, you also need to keep `equivalence_properties` consistent; DataFusion warns that incorrect equivalence propagation can cause unnecessary resorting (or worse, wrong assumptions). ([Docs.rs][9])

#### Pattern D — “Blocking node” (reorders/merges)

```rust
fn try_pushdown_sort(&self, _order: &[PhysicalSortExpr]) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
    Ok(SortOrderPushdownResult::Unsupported)
}
```

Use this when your node can reorder rows/partitions in ways that cannot respect a downstream sort optimization (e.g., uncontrolled interleaving, nondeterministic merge, etc.). ([Docs.rs][6])

---

### M.7.6 Interaction with TopK, partitioning, and “why Inexact still wins”

DataFusion’s config docs explicitly say: the optimization often returns **inexact ordering** and keeps the Sort for correctness, but optimized inputs can enable **early termination for Top-K**. ([Apache DataFusion][10])

This matches how TopK appears in physical plans:

* `SortExec` can run as `TopK(fetch=10)` (keeps only top N in memory)
* it can run per-partition (`preserve_partitioning=[true]`)
* then a `SortPreservingMergeExec` merges sorted streams
* a `GlobalLimitExec` enforces the final limit. ([Apache DataFusion][11])

So even if your source can only do an **inexact** optimization (e.g., reorder file/row-group reads), it can still materially reduce work in the `ORDER BY … LIMIT N` pipeline.

---

### M.7.7 “Correctness at risk” checklist (what to unit test)

1. **Sort expression validity after rewrite**

   * If you push a sort below a node that changes column layout, you must rewrite sort expressions accordingly. The failure mode is “invalid plan / wrong column index”, and it’s real. ([GitHub][8])

2. **Ordering guarantees vs claims**

   * Only return `Exact` if you truly guarantee the full ordering. Otherwise, your node can cause the optimizer to remove a required `SortExec`. ([Docs.rs][3])

3. **Partitioning semantics**

   * Nodes like `RepartitionExec` can destroy ordering unless preserve-order is enabled. Make the pushdown behavior conditional on that. ([Docs.rs][7])
   * Likewise, merging partitions can destroy ordering; use `into_inexact()` when appropriate. ([Docs.rs][3])

4. **Property propagation**

   * If your node is order-preserving, its `output_ordering` should reflect that. DataFusion notes that even “less obvious” nodes like `Projection` can still produce sorted output if the input is sorted (because it doesn’t reorder rows). ([Docs.rs][9])
   * Preserve `equivalence_properties` when valid; otherwise DataFusion may “unnecessarily resort data.” ([Docs.rs][9])

---

### M.7.8 Upgrade guidance for LLM agents (DF51 → DF52, custom nodes)

**If your codebase implements custom `ExecutionPlan` nodes:**

1. **Audit which nodes are “decorators”**

   * If the node doesn’t reorder rows, implement `try_pushdown_sort` as a delegating wrapper (Pattern A). ([Docs.rs][2])

2. **Audit schema-changing nodes**

   * Either implement safe “column-only” mapping for sort expressions, or explicitly block with `Unsupported` until you can do correct expression rewriting (Pattern C). ([GitHub][8])

3. **Golden-plan diffs**

   * With `datafusion.optimizer.enable_sort_pushdown=true` (default), expect physical plan deltas around `SortExec` placement/removal. ([Apache DataFusion][10])
   * Keep a regression test that toggles the flag and asserts plan stability (on/off). ([Apache DataFusion][10])

4. **Don’t forget the “bridge” layer**

   * DF52’s rule literally calls `try_pushdown_sort` on the *SortExec input* and rewrites based on `Exact/Inexact/Unsupported`. If your node blocks, the optimizer stops at your node. 

---

If you want the next increment in the same doc style: I can produce a **concrete “node taxonomy” checklist** (by DataFusion built-in exec type: which ones are transparent vs blocking today, and which already implement sort pushdown) plus a **minimal integration test harness** that builds a physical plan tree with your custom node inserted and asserts the `PushdownSort` rewrite behavior under `enable_sort_pushdown` toggles.



[1]: https://datafusion.apache.org/blog/2026/01/12/datafusion-52.0.0 "Apache DataFusion 52.0.0 Released - Apache DataFusion Blog"
[2]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html "ExecutionPlan in datafusion::physical_plan - Rust"
[3]: https://docs.rs/datafusion/latest/datafusion/physical_plan/enum.SortOrderPushdownResult.html "SortOrderPushdownResult in datafusion::physical_plan - Rust"
[4]: https://datafusion.apache.org/blog/2025/03/11/ordering-analysis/ "Using Ordering for Better Plans in Apache DataFusion - Apache DataFusion Blog"
[5]: https://www.mail-archive.com/github%40datafusion.apache.org/msg90789.html "Re: [PR] Establish the high level API for sort pushdown and the optimizer rule and support reverse files and row groups  [datafusion]"
[6]: https://docs.rs/datafusion/latest/datafusion/physical_plan/coalesce_partitions/struct.CoalescePartitionsExec.html "CoalescePartitionsExec in datafusion::physical_plan::coalesce_partitions - Rust"
[7]: https://docs.rs/datafusion/latest/datafusion/physical_plan/repartition/struct.RepartitionExec.html "RepartitionExec in datafusion::physical_plan::repartition - Rust"
[8]: https://github.com/apache/datafusion/issues/12558 " Sort Pushdown Causes an Invalid Plan · Issue #12558 · apache/datafusion · GitHub"
[9]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlanProperties.html "ExecutionPlanProperties in datafusion::physical_plan - Rust"
[10]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[11]: https://datafusion.apache.org/user-guide/explain-usage.html "Reading Explain Plans — Apache DataFusion  documentation"

## M.7.9 Node taxonomy checklist (DF52+): “transparent vs blocking” + who actually participates in sort pushdown

### M.7.9.1 Ground truth: the contract and the default

`ExecutionPlan::try_pushdown_sort` is the hook; **default implementation returns `SortOrderPushdownResult::Unsupported`** and the trait docs explicitly say transparent nodes should delegate+wrap. ([Docs.rs][1])

So the practical taxonomy is:

* **Participating transparent**: overrides `try_pushdown_sort` and delegates (optionally rewriting sort exprs)
* **Participating but exactness-breaking**: delegates but **downgrades `Exact → Inexact`** (or always returns `Inexact`)
* **Blocking**: returns `Unsupported` (explicitly or by inheriting the default)

---

### M.7.9.2 “Participation inventory” (what DF52.1.0 *actually implements* in core physical-plan)

Below are the **built-in Exec nodes in `datafusion-physical-plan`** you’ll actually see in most plans (full inventory is on the crate “all items” page). ([Docs.rs][2])

#### A) Transparent nodes that **already implement** `try_pushdown_sort` (delegate/wrap)

1. **`ProjectionExec`** (transparent *with expression rewrite constraints*)

* Rewrites the requested ordering through the projection mapping.
* **Only pushes down when each sort key maps to a “simple child column”**; if projection contains computed expressions, it returns `Unsupported`.
* Delegates to child and wraps `Exact`/`Inexact` accordingly. ([Docs.rs][3])

2. **`CoalesceBatchesExec`** (transparent)

* Declares itself transparent and delegates to child; wraps result. ([Docs.rs][4])

3. **`RepartitionExec`** (conditionally transparent)

* Delegates only when `maintains_input_order()[0]` is true (i.e., **`preserve_order` enabled or only one partition**).
* Otherwise returns `Unsupported` (blocking). ([Docs.rs][5])

#### B) Nodes that implement `try_pushdown_sort` but **must degrade exactness** (partition merge destroys global order)

4. **`CoalescePartitionsExec`** (partition merge)

* Important nuance: it *can* still push down for **per-partition** optimization, but global order is not preserved.
* Implementation delegates to child, but if the input has multiple partitions it **downgrades `Exact → Inexact`** (`into_inexact()`), because merging partitions destroys global ordering. ([Docs.rs][6])

This is the canonical example of a **“blocking for Exact, but still useful for Inexact”** node.

---

### M.7.9.3 Scans / sources: where “real” sort pushdown happens

#### `DataSourceExec` (physical scan wrapper)

`DataSourceExec` is the scan-side `ExecutionPlan` wrapper that ties into `DataSource` / `FileSource` for file-backed reads. It has its own `try_pushdown_sort(...)` entrypoint. ([Docs.rs][7])

#### `FileScanConfig::try_pushdown_sort` (DataSource-level negotiation)

At the DataSource layer, `FileScanConfig` exposes `try_pushdown_sort` returning `SortOrderPushdownResult<Arc<dyn DataSource>>`, i.e., “rebuild the source to produce a desired order” (or partially match it). ([Docs.rs][8])

#### Config: on/off switch

The top-level config knob is `datafusion.optimizer.enable_sort_pushdown` (default `true`). ([Apache DataFusion][9])
Additionally, `SessionConfig::with_enable_sort_pushdown(enabled)` exists and notes it **currently only applies to Parquet** (i.e., built-in pushdown support is Parquet-focused today). ([Docs.rs][10])

---

### M.7.9.4 Common “accidental blockers” (order-preserving nodes that still stop pushdown unless they override)

Because the trait default is `Unsupported`, any unary node that does not override becomes a pushdown barrier even if it **does not reorder rows**. ([Docs.rs][1])

Concrete examples visible in docs:

* **`FilterExec`** shows `try_pushdown_sort(&self, _order: ...)` (underscore param), strongly indicating it’s using the default (i.e., blocks). ([Docs.rs][11])
* **`GlobalLimitExec`** likewise shows `_order` in its `try_pushdown_sort` signature in docs snippets. ([Docs.rs][12])

If you’re building custom decorator nodes (metrics, tracing, policy gates, etc.), treat these as “what happens if you forget to override.”

---

### M.7.9.5 Joins and other multi-input nodes (treat as blocking unless explicitly implemented)

A representative example: **`SortMergeJoinExec`** shows `try_pushdown_sort(&self, _order: ...)`, again consistent with inheriting the default (blocking). ([Docs.rs][13])

Guideline: for *most* multi-input nodes, pushing down a global ordering requirement is either not meaningful or requires sophisticated rewrite; so expect them to be blockers unless a specific node explicitly participates.

---

### M.7.9.6 Optimizer context: sort pushdown is a dedicated physical rule

The physical optimizer pipeline description explicitly calls out a **sort-pushdown phase** that works top-down (“push down sort operators as deep as possible”). ([Docs.rs][14])
The concrete rule is `physical_optimizer::pushdown_sort::PushdownSort`. ([Docs.rs][15])

---

## M.7.10 Minimal integration test harness (custom node inserted; asserts PushdownSort rewrite under toggle)

This harness:

* builds a physical plan tree: `SortExec → (custom wrapper) → (source that claims Exact)`
* runs `PushdownSort` with `enable_sort_pushdown` on/off
* asserts whether `SortExec` is removed (Exact path) or preserved (blocked / disabled path)

### M.7.10.1 Test prerequisites

* Uses `MockExec::new(...)` from `datafusion_physical_plan::test::exec` to emit a known, already-sorted batch. ([Docs.rs][16])
* Uses `PushdownSort` optimizer rule. ([Docs.rs][15])
* Uses the config knob `datafusion.optimizer.enable_sort_pushdown`. ([Apache DataFusion][9])

### M.7.10.2 Code (drop into `tests/pushdown_sort_contract.rs`)

```rust
use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::Int32Array;
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use datafusion_common::config::ConfigOptions;
use datafusion_common::Result;
use datafusion_execution::TaskContext;

use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

use datafusion_physical_plan::display::{DisplayAs, DisplayFormatType};
use datafusion_physical_plan::sort_pushdown::SortOrderPushdownResult;
use datafusion_physical_plan::ExecutionPlan;

// Optimizer rule + trait
use datafusion::physical_optimizer::pushdown_sort::PushdownSort;
use datafusion::physical_optimizer::PhysicalOptimizerRule;

// SortExec
use datafusion_physical_plan::sorts::sort::SortExec;

// Test data source
use datafusion_physical_plan::test::exec::MockExec;

/// A “source-ish” wrapper that CLAIMS it can produce any requested ordering exactly.
/// In a real implementation you would verify `order` matches your intrinsic ordering,
/// and/or rebuild the scan.
/// For this harness, we ensure the underlying MockExec emits already-sorted data.
#[derive(Debug, Clone)]
struct ExactSortCapableExec {
    input: Arc<dyn ExecutionPlan>,
}

impl ExactSortCapableExec {
    fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self { input }
    }
}

impl DisplayAs for ExactSortCapableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExactSortCapableExec")
    }
}

impl ExecutionPlan for ExactSortCapableExec {
    fn name(&self) -> &str {
        "ExactSortCapableExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
        // Keep it simple: reuse the child’s properties.
        // (If you want invariants + ordering analysis to “see” the ordering, compute your own PlanProperties.)
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children[0].clone())) as Arc<dyn ExecutionPlan>)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<datafusion_execution::SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    fn try_pushdown_sort(
        &self,
        _order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        // Exact: optimizer may remove a SortExec above us
        Ok(SortOrderPushdownResult::Exact {
            inner: Arc::new(self.clone()) as Arc<dyn ExecutionPlan>,
        })
    }
}

/// Custom wrapper node (transparent): delegates sort pushdown to its child and wraps the result.
#[derive(Debug, Clone)]
struct MyTransparentWrapperExec {
    input: Arc<dyn ExecutionPlan>,
}

impl MyTransparentWrapperExec {
    fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self { input }
    }
}

impl DisplayAs for MyTransparentWrapperExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MyTransparentWrapperExec")
    }
}

impl ExecutionPlan for MyTransparentWrapperExec {
    fn name(&self) -> &str {
        "MyTransparentWrapperExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children[0].clone())) as Arc<dyn ExecutionPlan>)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<datafusion_execution::SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        // Delegate + wrap, preserving Exact vs Inexact vs Unsupported
        self.input.try_pushdown_sort(order)?.try_map(|new_input| {
            Ok(Arc::new(Self::new(new_input)) as Arc<dyn ExecutionPlan>)
        })
    }
}

/// Custom wrapper node (blocking): stops pushdown.
#[derive(Debug, Clone)]
struct MyBlockingWrapperExec {
    input: Arc<dyn ExecutionPlan>,
}

impl MyBlockingWrapperExec {
    fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        Self { input }
    }
}

impl DisplayAs for MyBlockingWrapperExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MyBlockingWrapperExec")
    }
}

impl ExecutionPlan for MyBlockingWrapperExec {
    fn name(&self) -> &str {
        "MyBlockingWrapperExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion_physical_plan::PlanProperties {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children[0].clone())) as Arc<dyn ExecutionPlan>)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<datafusion_execution::SendableRecordBatchStream> {
        self.input.execute(partition, context)
    }

    fn try_pushdown_sort(
        &self,
        _order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        Ok(SortOrderPushdownResult::Unsupported)
    }
}

fn sort_expr_a() -> PhysicalSortExpr {
    PhysicalSortExpr {
        expr: Arc::new(Column::new("a", 0)),
        options: SortOptions::default(),
    }
}

fn make_sorted_mock_source() -> Arc<dyn ExecutionPlan> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4]))],
    )
    .unwrap();

    // MockExec::new returns a single partition of specified batches
    // (we already supply them sorted)
    let mock = MockExec::new(vec![Ok(batch)], schema);
    Arc::new(mock) as Arc<dyn ExecutionPlan>
}

fn make_sort_plan(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let ordering = vec![sort_expr_a()];
    Arc::new(SortExec::new(ordering.into(), input)) as Arc<dyn ExecutionPlan>
}

#[test]
fn pushdown_sort_respects_toggle_and_wrapper_contracts() -> Result<()> {
    // Base source that claims Exact sort capability
    let source = Arc::new(ExactSortCapableExec::new(make_sorted_mock_source())) as Arc<dyn ExecutionPlan>;

    // Case 1: sort pushdown DISABLED => SortExec remains
    {
        let plan = make_sort_plan(Arc::new(MyTransparentWrapperExec::new(source.clone())) as Arc<dyn ExecutionPlan>);
        let mut cfg = ConfigOptions::new();
        cfg.optimizer.enable_sort_pushdown = false;

        let optimized = PushdownSort::new().optimize(plan.clone(), &cfg)?;
        assert!(optimized.as_any().downcast_ref::<SortExec>().is_some());
    }

    // Case 2: sort pushdown ENABLED + transparent wrapper => SortExec removed
    {
        let plan = make_sort_plan(Arc::new(MyTransparentWrapperExec::new(source.clone())) as Arc<dyn ExecutionPlan>);
        let mut cfg = ConfigOptions::new();
        cfg.optimizer.enable_sort_pushdown = true;

        let optimized = PushdownSort::new().optimize(plan.clone(), &cfg)?;
        assert!(optimized.as_any().downcast_ref::<SortExec>().is_none());
    }

    // Case 3: sort pushdown ENABLED + blocking wrapper => SortExec stays
    {
        let plan = make_sort_plan(Arc::new(MyBlockingWrapperExec::new(source.clone())) as Arc<dyn ExecutionPlan>);
        let mut cfg = ConfigOptions::new();
        cfg.optimizer.enable_sort_pushdown = true;

        let optimized = PushdownSort::new().optimize(plan.clone(), &cfg)?;
        assert!(optimized.as_any().downcast_ref::<SortExec>().is_some());
    }

    Ok(())
}
```

### M.7.10.3 What this harness asserts (and why it’s the right minimal test)

* **Toggle coverage**: `enable_sort_pushdown=false` should keep the original plan (rule checks the config before rewriting). ([Apache DataFusion][9])
* **Contract coverage**: a transparent custom node must delegate+wrap, otherwise it becomes a barrier (mirrors how built-ins like `CoalesceBatchesExec` delegate). ([Docs.rs][4])
* **Blocking semantics**: returning `Unsupported` blocks pushdown (same ultimate effect as inheriting the default). ([Docs.rs][1])

If you want one more micro-test for the **Inexact path**, add a source wrapper that returns `SortOrderPushdownResult::Inexact { inner: ... }` and assert `SortExec` remains but its child changes (PushdownSort keeps Sort for correctness on Inexact). ([Docs.rs][1])

[1]: https://docs.rs/datafusion-physical-plan/52.1.0/x86_64-unknown-linux-gnu/src/datafusion_physical_plan/execution_plan.rs.html "execution_plan.rs - source"
[2]: https://docs.rs/datafusion-physical-plan/latest/datafusion_physical_plan/all.html "List of all items in this crate"
[3]: https://docs.rs/datafusion-physical-plan/52.1.0/x86_64-unknown-linux-gnu/src/datafusion_physical_plan/projection.rs.html "projection.rs - source"
[4]: https://docs.rs/crate/datafusion-physical-plan/latest/source/src/coalesce_batches.rs "https://docs.rs/crate/datafusion-physical-plan/latest/source/src/coalesce_batches.rs"
[5]: https://docs.rs/datafusion-physical-plan/52.1.0/x86_64-unknown-linux-gnu/src/datafusion_physical_plan/repartition/mod.rs.html "mod.rs - source"
[6]: https://docs.rs/crate/datafusion-physical-plan/latest/source/src/coalesce_partitions.rs "https://docs.rs/crate/datafusion-physical-plan/latest/source/src/coalesce_partitions.rs"
[7]: https://docs.rs/datafusion/latest/datafusion/datasource/memory/struct.DataSourceExec.html "DataSourceExec in datafusion::datasource::memory - Rust"
[8]: https://docs.rs/datafusion/latest/datafusion/datasource/physical_plan/struct.FileScanConfig.html "FileScanConfig in datafusion::datasource::physical_plan - Rust"
[9]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[10]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionConfig.html?utm_source=chatgpt.com "SessionConfig in datafusion::execution::context - Rust"
[11]: https://docs.rs/datafusion/latest/datafusion/physical_plan/filter/struct.FilterExec.html?utm_source=chatgpt.com "FilterExec in datafusion::physical_plan::filter - Rust"
[12]: https://docs.rs/datafusion/latest/datafusion/physical_plan/limit/struct.GlobalLimitExec.html?utm_source=chatgpt.com "GlobalLimitExec in datafusion::physical_plan::limit - Rust"
[13]: https://docs.rs/datafusion/latest/datafusion/physical_plan/joins/struct.SortMergeJoinExec.html "SortMergeJoinExec in datafusion::physical_plan::joins - Rust"
[14]: https://docs.rs/datafusion/latest/datafusion/physical_optimizer/trait.PhysicalOptimizerRule.html "https://docs.rs/datafusion/latest/datafusion/physical_optimizer/trait.PhysicalOptimizerRule.html"
[15]: https://docs.rs/datafusion/latest/datafusion/all.html "https://docs.rs/datafusion/latest/datafusion/all.html"
[16]: https://docs.rs/datafusion-physical-plan/latest/datafusion_physical_plan/test/exec/struct.MockExec.html "https://docs.rs/datafusion-physical-plan/latest/datafusion_physical_plan/test/exec/struct.MockExec.html"
