## 0.1 Format vs engine vs product (the “3-layer model”)

Delta Lake is easiest to reason about if you **treat it as three orthogonal layers**:

1. **Format / Protocol**: what exists *on disk* and what is *legally allowed* to read/write it.
2. **Client implementations**: the concrete *APIs* and *execution models* that manipulate the format.
3. **Platform/product optimizations**: higher-level behaviors and managed features that may *depend on* protocol features, but aren’t the protocol itself.

This section is the “semantic boundary contract” that keeps the rest of the deep dive crisp and non-mushy.

---

### 0.1.1 Delta Table Format / Protocol (OSS, cross-engine): the ground truth

**Definition (scope):** The *only* universal source of truth is the **Delta Transaction Log Protocol** spec: it defines table layout, file types, commit semantics, and compatibility gating (reader/writer protocol + table features). ([GitHub][1])

#### A) On-disk artifacts (what physically exists)

A Delta table is a directory containing (at minimum):

* **Data files** (typically Parquet) in the table root or non-hidden subdirs; partition directory layout is conventional but *not required*—true partition values are recorded in the log. ([GitHub][1])
* **`_delta_log/`** containing:

  * **Delta log entries**: newline-delimited JSON files named by monotonically increasing version numbers, zero-padded to 20 digits (`000…00000.json`, `000…00001.json`, …). ([GitHub][1])
  * **Checkpoints**: materialized snapshot state stored in `_delta_log/` (classic `n.checkpoint.parquet` plus newer “v2” / UUID-named checkpoint variants + sidecars). ([GitHub][1])
  * `_last_checkpoint`: a pointer/summary file that lets readers jump to the latest checkpoint quickly. ([GitHub][1])
* Optional: **change data files** under `_change_data/` and **deletion vector files** (DV) in the table root, used by specific table features. ([GitHub][1])

> **Key mental model:** the physical directory may contain “extra” files from failed jobs or superseded versions; readers must use the log snapshot to decide which files are valid. ([Delta][2])

#### B) Snapshot semantics (MVCC + “table state at version N”)

Protocol defines Delta as MVCC over immutable files:

* A **table version** is an atomic commit; a **snapshot** at version *N* is defined by:

  * protocol requirements (reader/writer protocol + any enabled table features),
  * metadata (schema, partition columns, config properties),
  * active set of data files + file-level metadata/stats,
  * tombstones for recently removed files,
  * app transaction identifiers. ([GitHub][1])
* Writers do a **two-phase** workflow: write new/updated data files, then commit a new log entry that *logically* adds/removes files. ([GitHub][1])

#### C) Compatibility gating: protocol versions + table features

Delta uses **explicit compatibility contracts** embedded in the log:

* Tables specify **separate read and write protocol requirements**, and writers must be able to construct snapshots (so they must respect both). ([Delta Lake][3])
* **Table features** are granular flags indicating enabled capabilities (successor to coarse protocol bundles; introduced with newer protocol levels). ([Delta Lake][3])
* Crucial nuance: some features upgrade **write protocol only** (writers must enforce; readers unaffected), while others upgrade **read+write** (changes storage interpretation, so readers must understand it). Delta docs explicitly contrast `CHECK` constraints (write-only) vs column mapping (read+write). ([Delta Lake][3])

**Why this matters for your deep dive:** *any feature that changes storage interpretation must be categorized as “protocol-level,” not “engine-level.”* (Column mapping, deletion vectors, v2 checkpoints, row tracking, etc. are in the protocol spec TOC for a reason.) ([GitHub][1])

---

### 0.1.2 Client implementations: where protocol becomes usable APIs

The protocol is necessary but not sufficient: it tells you *what must happen*; implementations define *how you do it* (APIs, execution, tuning knobs, failure modes).

#### (1) Spark / `delta-spark` (reference + richest surface)

**What it is:** a Spark-integrated implementation that adds Delta behaviors to Spark SQL/DataFrame APIs via extensions/catalog.

**Activation:** configure SparkSession with Delta extensions + DeltaCatalog; Python quick start shows `configure_spark_with_delta_pip(builder).getOrCreate()` and sets Spark configs for extensions/catalog. ([Delta Lake][4])

**API surface characteristics:**

* SQL DDL/DML (`CREATE TABLE ... USING DELTA`, `MERGE`, `UPDATE`, `DELETE`, `DESCRIBE HISTORY`, `RESTORE`, etc.)
* Python/Scala/Java wrappers (e.g., `delta.tables.DeltaTable.merge()` builder pattern). ([Delta Lake][5])

**Boundary rule:** anything described as Spark SQL semantics is often *implementation-defined behavior* **until** you can point to a protocol representation (actions/files/properties) that makes it cross-engine.

#### (2) `delta-rs` / `deltalake` Python bindings (Sparkless Rust implementation)

**What it is:** a Rust reimplementation of the Delta protocol with Python APIs, explicitly **no Spark/Java dependency**. ([Delta Io][6])

**Core abstraction:** `DeltaTable` = “state of a delta table at a particular version” (files, schema, metadata). ([Delta Io][7])

**Boundary rule:** delta-rs must still obey protocol (log parsing, reconciliation, checkpoints, etc.), but the exposed APIs (e.g., how you pass filters, how you write Arrow/Pandas, which maintenance ops exist) are **client/API layer** choices.

#### (3) Delta Kernel (Java + Rust libraries): connector-building substrate

**What it is:** a set of libraries for building *connectors/engines* that can read/write Delta tables **without reimplementing protocol details**. ([Delta Lake][8])

**Key architectural boundary:**

* Kernel provides narrow APIs for log reading + scan planning + protocol semantics.
* The connector supplies an **`Engine` interface** implementation for heavy-lift ops (Parquet/JSON IO, expression eval, filesystem listing, etc.). ([Delta Lake][8])

**Practical consequence:** Kernel is the “protocol compliance core,” while the connector owns execution primitives (IO, compute, parallelism). Kernel docs explicitly call out single-process single-thread reads and multi-thread reads as supported shapes. ([Delta Lake][8])

---

### 0.1.3 Platform/runtime optimizations (often Databricks-first): “product layer”

**Definition (scope):** behaviors that are *not required* to read/write a Delta table, but that deliver performance/governance/convenience in a particular runtime.

Databricks explicitly frames this split:

* Many “optimizations that leverage Delta Lake features” exist in Databricks runtimes, may require enabling table features, and you should test OSS compatibility before enabling features in production if you also use OSS clients. ([Databricks Documentation][9])
* Example: “liquid clustering” appears in Databricks’ feature list and is tied to runtime versions; treat it as “platform layer” until you can map it to a protocol feature (e.g., clustered table metadata) and confirm cross-engine behavior. ([Databricks Documentation][9])

**Boundary rule:** platform features can *depend on protocol features*, but the user-facing behavior (automation, UI, background jobs, governance integration) is product-specific.

---

## 0.1.4 How to scope *every* feature in your deep dive: Protocol vs API vs Ops impact

This is the “pyarrow-advanced.md style” rubric you’ll reuse for every later section.

### Step 1 — Classify: is it a protocol feature?

A feature is **protocol-level** if any of the below are true:

* It introduces new **file types**, new **log actions**, or new **interpretation rules** for existing files/actions.
* It requires a **reader or writer protocol/table-feature flag** to be enabled for correctness. ([Delta Lake][3])

**Examples (from Delta docs):**

* `CHECK` constraints: write-protocol feature (writers must enforce). ([Delta Lake][3])
* Column mapping: read+write protocol feature (readers must interpret storage differently). ([Delta Lake][3])
* Deletion vectors / change data files / v2 checkpoints / row tracking: appear as protocol sections/file types/features. ([GitHub][1])

### Step 2 — Describe the API surface per implementation

For each implementation you care about (Spark, delta-rs, Kernel), capture:

* **Entry points** (e.g., `DeltaTable` constructors, Spark SQL commands, Kernel scan APIs)
* **Capability coverage** (read-only vs write; DML; maintenance)
* **Extension hooks** (Spark conf/session flags; Kernel Engine interface; delta-rs storage_options)
* **Behavioral deltas** (e.g., how filters are expressed; which operations are supported; concurrency expectations)

Ground these with the official docs for each client (Spark quick start, delta-rs usage, Kernel design). ([Delta Lake][4])

### Step 3 — Operational impact (compat, maintenance, perf)

For every feature, make “ops impact” a first-class subheading:

* **Compatibility**: required minReader/minWriter + table feature flags; downgrade constraints (some features can’t be dropped). ([Databricks Documentation][9])
* **Maintenance**: checkpointing/log replay implications; vacuum retention; metadata cleanup. ([GitHub][1])
* **Performance**: how it affects file counts, log scan costs, data skipping, read amplification, commit contention.

---

## 0.1.5 A reusable “feature write-up template” (use this everywhere)

For each Delta feature section in your eventual `delta-lake-advanced.md`, standardize the unit as:

1. **Protocol representation**

   * log actions, file types, table properties, reconciliation rules
2. **Client surfaces**

   * Spark SQL / delta-spark
   * delta-rs / deltalake
   * Delta Kernel (connector-level)
3. **Operational impact**

   * compatibility gates (reader/writer + features)
   * performance + maintenance
   * failure modes (what breaks when a client doesn’t support a feature)

This mirrors how Delta itself is designed: *protocol first*, *implementations second*, *products last*. ([GitHub][1])

[1]: https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md "raw.githubusercontent.com"
[2]: https://delta.io/pdfs/dldg_databricks.pdf "Delta Lake: The Definitive Guide"
[3]: https://docs.delta.io/versioning/ "How does Delta Lake manage feature compatibility? | Delta Lake"
[4]: https://docs.delta.io/quick-start/ "Quick Start | Delta Lake"
[5]: https://docs.delta.io/api/latest/python/spark/?utm_source=chatgpt.com "Welcome to Delta Lake's Python documentation page"
[6]: https://delta-io.github.io/delta-rs/?utm_source=chatgpt.com "Home - Delta Lake Documentation"
[7]: https://delta-io.github.io/delta-rs/python/usage.html "Usage — delta-rs  documentation"
[8]: https://docs.delta.io/delta-kernel/ "Delta Kernel | Delta Lake"
[9]: https://docs.databricks.com/aws/en/delta/feature-compatibility "Delta Lake feature compatibility and protocols | Databricks on AWS"

## A) Core table format + transaction protocol (the “storage layer”)

This section is **pure protocol**: what exists on storage + the reconciliation rules that define the table snapshot. Everything else (Spark APIs, delta-rs APIs, Kernel APIs) is an implementation of these artifacts/rules.

---

# A1) Table layout primitives

## A1.1 Directory structure + file type taxonomy

A Delta table is a directory containing **immutable data files** plus **a transaction log** under `_delta_log/`. The protocol is MVCC: writers create new files, then atomically append a new log version describing **adds/removes**; readers compute a snapshot by selecting only the live files from the log. ([GitHub][1])

### Table root

* **Data files**: stored in the root or any non-hidden subdir (name not starting with `_`). Partition directory naming is conventional but *not required*; the authoritative partition values come from log actions. ([GitHub][1])
* **Deletion vector files** (if feature active): stored alongside data files in table root; may contain DVs for multiple partitions/files. ([GitHub][1])
* **Change data files** (if CDF written): stored under `_change_data/` (optionally partitioned under that directory). ([GitHub][1])

### `_delta_log/` (the canonical history)

Contains (not exhaustive):

* **Commit JSON files**: `00000000000000000000.json`, `...00001.json`, … (20-digit zero padded, contiguous versions). Each is newline-delimited JSON where each line is one “action”. ([GitHub][1])
* **Checkpoint files**: materialized snapshots to short-circuit log replay, plus optional **sidecars** in `_delta_log/_sidecars/`. Delta supports multiple checkpoint naming schemes + “specs” (V1/V2). ([GitHub][1])
* **Log compaction files**: `start.end.compacted.json` representing reconciled actions over a commit range `[start, end]` (optional optimization). ([GitHub][1])
* **`_last_checkpoint`**: JSON pointer to a recent checkpoint to avoid listing huge directories. ([GitHub][1])
* **Version checksum files**: `{version}.crc` JSON objects used to validate snapshot integrity / detect non-compliant modifications (optional). ([GitHub][1])

### Writer immutability constraints (hard protocol requirements)

* Writers **must never overwrite** an existing log entry; use atomic filesystem primitives when possible. ([GitHub][1])
* Data files **must be uniquely named and never overwritten** (reference uses GUIDs). ([GitHub][1])

---

## A1.2 Commit actions model (“what a commit can contain”)

A commit file `n.json` is an atomic set of actions to apply to state `n-1` to construct snapshot `n`. Actions are the sole mechanism by which table state evolves. ([GitHub][1])

### Action categories (high-frequency core set)

**State-defining**

* `metaData`: full table metadata (schema, partition columns, configuration). First table version must include it; subsequent ones overwrite prior metadata; max one per version. ([GitHub][1])
* `protocol`: min reader/writer protocol versions (+ optional feature lists) defining compatibility gates. ([GitHub][1])

**Data plane (file-set)**

* `add` / `remove`: add or remove a *logical file* (data file path + optional DV descriptor). Includes `dataChange` to mark “rearrangement/stat-only” actions. ([GitHub][1])
* `cdc` (AddCDCFile): add a change data file for CDF consumption; change readers must prefer `cdc` actions when present. ([GitHub][1])

**Idempotency / streaming**

* `txn`: application transaction identifiers (`appId`, `version`) stored in-log for exactly-once/idempotent external writers. ([GitHub][1])

**Provenance / time semantics**

* `commitInfo`: arbitrary JSON payload for provenance; when In-Commit Timestamps are enabled, commitInfo is required, must include `inCommitTimestamp`, and must be the first action in the commit. ([GitHub][1])

**Domain-level metadata (advanced; feature-gated)**

* `domainMetadata`: per-domain config strings (user-controlled domains vs system-controlled `delta.*` domains). Overlapping writes conflict if they touch same domain. Feature-gated via writer features. ([GitHub][1])

### Commit validity / dedup constraints (file actions)

Because action ordering within a commit is not guaranteed, versions are constrained to avoid ambiguous reconciliation:

* within a version: at most one `add` (resp. `remove`) per `(path, dvId)` and also at most one per `path` regardless of dvId; multiple occurrences are illegal except specific allowed combinations. ([GitHub][1])
* `dataChange=false` means “net snapshot semantics unchanged” (compaction, clustering, stats-only updates); tailing readers can skip these for incremental computation. ([GitHub][1])

### Log-entry uniqueness constraints (writer requirements)

A single log entry must not include more than one mutually-reconciling action (e.g., >1 metadata action; >1 protocol action; multiple `txn` with same appId; add/remove conflicts on same tuple). ([GitHub][1])

---

# A2) Snapshots, checkpoints, and log scalability

## A2.1 Snapshot reconstruction = replay + action reconciliation

**Canonical model:** compute snapshot *V* by replaying commits in ascending version and applying **Action Reconciliation** rules. ([GitHub][1])

Snapshot state components (protocol-level, not “engine convenience”):

* exactly one `protocol` action (latest wins)
* exactly one `metaData` action (latest wins)
* `txn`: per `appId`, latest `version` wins
* `domainMetadata`: per domain, latest wins; `removed=true` tombstones suppress older entries; removed domains are not returned in snapshots ([GitHub][1])
* `add`: live logical files keyed by `(path, dvId)` with “latest encountered wins” semantics
* `remove`: tombstones keyed by `(path, dvId)`; intersection with live adds must be empty; reads ignore remove tombstones entirely (they exist to support VACUUM). ([GitHub][1])

**Implementation note (python LLM agent):**

* model snapshot as dicts keyed by:

  * `protocol` singleton
  * `metadata` singleton
  * `txn[appId] -> version`
  * `domains[domain] -> config or tombstone`
  * `adds[(path, dvId)] -> AddFile struct`
  * `removes[(path, dvId)] -> RemoveFile tombstone`
* apply reconciliation as a pure fold; you do not need Spark semantics.

## A2.2 Fast-path: checkpoints (plus sidecars)

Checkpoint semantics:

* a checkpoint is a materialized replay of all actions up to version *N*, with invalid actions removed per reconciliation rules, and it includes remove tombstones that are not expired. ([GitHub][1])
* readers should use newest complete checkpoint ≤ target version (time travel constraint). ([GitHub][1])
* checkpoints allow metadata cleanup to delete old JSON commits. ([GitHub][1])

Checkpoint content requirements:

* must include protocol, metadata, live adds, unexpired removes, txn identifiers, domain metadata; plus v2-only checkpoint metadata + sidecar references. ([GitHub][1])
* must *not* preserve `commitInfo` or `cdc` actions. ([GitHub][1])

### Checkpoint specs vs naming schemes (don’t conflate)

Delta defines both:

* **Spec**: V1 vs V2 (V2 enables sidecars + checkpointMetadata).
* **Naming**: classic vs UUID-named vs multi-part. ([GitHub][1])

#### V2 spec (feature-gated)

* V2 allows placing all `add`/`remove` file actions in **sidecar parquet files** under `_delta_log/_sidecars/` and referencing them from the checkpoint via `sidecar` actions. Sidecars currently only contain add/remove actions. ([GitHub][1])
* V2 checkpoint must include exactly one `checkpointMetadata` action; all non-file actions must be in the checkpoint itself; add/remove must be either entirely embedded or entirely in sidecars (no partial split). ([GitHub][1])

#### Naming schemes

* **UUID-named checkpoint**: `n.checkpoint.<uuid>.json|parquet` (V2 spec). ([GitHub][1])
* **Classic checkpoint**: `n.checkpoint.parquet` (may be V1 or V2 if v2Checkpoint feature enabled). Two writers can race; latest wins but contents should be equivalent. ([GitHub][1])
* **Multi-part** (deprecated): `n.checkpoint.o.p.parquet` (always V1; non-atomic; readers ignore if parts missing; writers should avoid; v2 checkpoint feature forbids creating these). ([GitHub][1])

#### Backward-compat strategy when UUID-named V2 enabled

Writers should occasionally create **V2 classic checkpoints** so older clients that don’t understand UUID-named checkpoints can still discover protocol and fail cleanly. ([GitHub][1])

## A2.3 `_last_checkpoint`: avoid LIST storms

The protocol explicitly calls out `_delta_log` can reach 10k+ files; listing can be expensive. `_last_checkpoint` provides a pointer “near end of log” so readers can jump to the latest checkpoint and only list forward from there (leveraging lexicographic ordering of zero-padded filenames). ([GitHub][1])

The `_last_checkpoint` JSON has a defined schema (version, size, optional parts/sizeInBytes, optional numOfAddFiles/schema/tags, optional MD5 checksum). Readers are encouraged to validate checksum if present; writers encouraged to write it. ([GitHub][1])

## A2.4 Log compaction files: range-reconciled commits

Optional optimization: a log compaction file `x.y.compacted.json` contains reconciled actions for the commit range `[x, y]`; an implementation can substitute it for reading all commits in that span during snapshot construction. ([GitHub][1])

## A2.5 Version checksum (`*.crc`): integrity signals

Optional but protocol-defined: for each commit, writers can emit `{version}.crc` containing table-state metrics post-reconciliation; readers can recompute and compare; mismatch implies potential corruption/non-compliant modification. ([GitHub][1])

## A2.6 Metadata cleanup + retention (protocol + table-properties layer)

### Protocol-level cleanup algorithm (spec)

Protocol provides a concrete multi-step cleanup approach:

* choose cutoff timestamp/commit; keep everything ≥ cutoffCommit
* find newest checkpoint ≤ cutoffCommit; preserve checkpoint *and* the JSON commit at that checkpoint version
* preserve all commits after; delete older commits/checkpoints; delete older compaction files; protect sidecars referenced by remaining checkpoints and recent sidecars to avoid breaking in-progress checkpoints ([GitHub][1])

Critical nuance: preserve the JSON commit at `cutOffCheckpoint` because checkpoints don’t preserve `commitInfo` (needed for features like in-commit timestamps) and don’t preserve `cdc` actions. ([GitHub][1])

### Table properties (implementation knobs)

Delta exposes storage-layer knobs as table properties; relevant here:

* `delta.logRetentionDuration`: how long log/history is kept; older log entries are cleaned up automatically when checkpoints are written ([Delta Lake][2])
* `delta.deletedFileRetentionDuration`: minimum time to keep logically removed data files before physical deletion (stale readers/streaming restart safety) ([Delta Lake][2])
* checkpoint serialization knobs:

  * `delta.checkpoint.writeStatsAsJson` / `delta.checkpoint.writeStatsAsStruct` affects whether checkpoints carry stats as JSON string vs parsed struct and typed partitionValues in `partitionValues_parsed` ([Delta Lake][2])
* `delta.checkpointPolicy`: `classic` vs `v2` checkpoint policy selection (ties directly to v2 checkpoint spec/compat) ([Delta Lake][2])
* `delta.setTransactionRetentionDuration`: retention for txn identifiers used by idempotent writers (when older than threshold, snapshots treat as expired). ([Delta Lake][2])

---

# A3) Protocol versioning + “table features” flags

## A3.1 Protocol action schema + evolution rules

The `protocol` action defines:

* `minReaderVersion` + `minWriterVersion` (integer-based legacy protocol)
* optional `readerFeatures` (only when minReaderVersion==3)
* optional `writerFeatures` (only when minWriterVersion==7) ([GitHub][1])

Protocol evolution rule that matters for implementers:

* unrecognized actions/fields/metadata domains are never required for correctness *unless* the protocol/feature set says so; readers must ignore unknown fields/actions rather than error, and writers must only introduce breaking changes with protocol upgrade or feature enablement. ([GitHub][1])

## A3.2 Table features: supported vs active (don’t confuse them)

Protocol-level semantics:

* A feature is **supported** if its name appears in `protocol.readerFeatures` and/or `protocol.writerFeatures`. Clients must not remove supported features; writers can add support by adding names. ([GitHub][1])
* Reader features must appear in both `readerFeatures` and `writerFeatures`; listing a feature only in `readerFeatures` is invalid. ([GitHub][1])
* Supported does not imply **active**: a feature is active only when supported *and* its metadata requirements are satisfied (often table properties). Example: `appendOnly` supported but not active unless `delta.appendOnly=true`. ([GitHub][1])

## A3.3 Upgrading to Reader v3 / Writer v7: historical feature detection problem

When upgrading an existing table to reader v3 and/or writer v7, a client should determine which features have ever been used in any historical version reachable by time travel; if it can’t prove non-use, it must assume the feature was used. The spec explicitly highlights this pitfall using Change Data Feed as an example (feature toggled on/off historically). ([GitHub][1])

Practical consequence: “feature set derivation” is either:

* full history scan, or
* conservative superset assumption (compat-costly).

## A3.4 Compatibility boundaries: reader vs writer features

Databricks’ compatibility framing maps directly to protocol semantics:

* writer features require `minWriterVersion == 7` and only constrain writers; read-only legacy access can remain possible.
* reader features require `minReaderVersion == 3` and `minWriterVersion == 7`, and they constrain both read and write because a client can’t write what it can’t read. ([Databricks Documentation][3])

Protocol changes occur when enabling a new feature (upgrade) or dropping a feature (downgrade); merely disabling a feature via property does not downgrade. ([Databricks Documentation][3])

## A3.5 Dropping / downgrading table features (Delta Lake 4+; legacy in 3.x)

Delta Lake supports feature drop + protocol downgrade workflows:

* command: `ALTER TABLE <table> DROP FEATURE <feature>` ([Delta Lake][4])
* not all features are droppable; delta.io lists droppable set (e.g., deletionVectors, typeWidening, v2Checkpoint, columnMapping, vacuumProtocolCheck, checkConstraints, inCommitTimestamp, checkpointProtection). ([Delta Lake][4])

Drop mechanics (protocol + storage impact):

* disable dependent properties
* rewrite data/metadata to remove traces of the dropped feature from current backing files
* create “protected checkpoints” enabling correct interpretation of history post-downgrade
* add `checkpointProtection` writer feature
* downgrade protocol to the lowest reader/writer versions that support remaining active features ([Delta Lake][4])

Operational constraints:

* feature drops conflict with concurrent writes (treat as exclusive maintenance operation). ([Databricks Documentation][5])

---


[1]: https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md "raw.githubusercontent.com"
[2]: https://docs.delta.io/table-properties/ "Delta Table Properties Reference | Delta Lake"
[3]: https://docs.databricks.com/aws/en/delta/feature-compatibility "Delta Lake feature compatibility and protocols | Databricks on AWS"
[4]: https://docs.delta.io/delta-drop-feature/ "Drop Delta table features | Delta Lake"
[5]: https://docs.databricks.com/aws/en/delta/drop-feature?utm_source=chatgpt.com "Drop a Delta Lake table feature and downgrade ..."

## B) Transactions, isolation, and multi-writer semantics

This section is a **syntax-first reference** for how Delta Lake achieves transactional guarantees and how you *configure / invoke / handle* concurrency behavior from Python (primarily via **delta-spark / Spark SQL**, with protocol-context where needed).

---

# B1) ACID + snapshot isolation

## B1.1 What Delta guarantees (protocol semantics)

Delta’s transactional model is **MVCC over immutable files**. At a high level:

* **Readers** see a **consistent snapshot** of the table (the version the job started with), even if concurrent writes commit during the job. ([Delta Lake][1])
* **Writers** commit as an atomic new table version, and (for supported storage systems) concurrent writes serialize into a well-defined commit order. ([Delta Lake][1])

## B1.2 “Pin a snapshot” APIs (Spark / delta-spark)

Delta read isolation is typically implicit (“job reads the snapshot it started with”), but you can explicitly **time-travel** to bind reads to a specific version/timestamp (useful for reproducibility and for conflict-debugging).

### DataFrameReader (path-based)

```python
df_v = (
    spark.read.format("delta")
      .option("versionAsOf", 123)
      .load("/mnt/delta/t")
)

df_t = (
    spark.read.format("delta")
      .option("timestampAsOf", "2026-01-05T23:09:47.000+00:00")
      .load("/mnt/delta/t")
)
```

([Databricks Documentation][2])

### DataFrameReader (table-based)

```python
df_v = spark.read.option("versionAsOf", 0).table("schema.table")
df_t = spark.read.option("timestampAsOf", "2019-01-01").table("schema.table")
```

([Databricks Documentation][2])

### SQL time travel (Delta on Spark / Databricks SQL)

```sql
SELECT * FROM events VERSION AS OF 0;
SELECT * FROM events TIMESTAMP AS OF '2019-01-29 00:37:58';
-- alternative @ syntax in some runtimes:
SELECT * FROM people10m@v123;
SELECT * FROM people10m@20190101000000000;
```

([Microsoft Learn][3])

## B1.3 Write isolation levels (Databricks-specific knob)

Databricks exposes a **table-level isolation** setting:

* Table property: `delta.isolationLevel`
* Values: `WriteSerializable` (default) or `Serializable`
* Set via SQL:

```sql
ALTER TABLE <table-name> SET TBLPROPERTIES ('delta.isolationLevel' = 'Serializable');
```

([Databricks Documentation][4])

**Semantics (Databricks doc):**

* Reads always use snapshot isolation.
* `WriteSerializable` is weaker than full `Serializable`: it enforces serializability for *writes* but can allow outcomes equivalent to reordering concurrent writes (which can produce table states that don’t align with the physical commit order shown in history). Setting `Serializable` disallows that reordering by forcing conflicts instead. ([Databricks Documentation][4])

Operational implications called out explicitly:

* **Metadata changes** (protocol, schema, table properties) cause concurrent writes to fail; streaming reads fail on metadata-change commits and must be restarted. ([Databricks Documentation][4])

---

# B2) Optimistic concurrency control (write conflict model)

## B2.1 OCC algorithm (the “3-phase” write)

Delta’s concurrency control is **optimistic**:

1. **Read** the latest snapshot (if needed) to find which files must be rewritten
2. **Write** new data files (staging)
3. **Validate & commit**: check for conflicts with commits since the read snapshot; if no conflicts, commit as a new version; otherwise fail with a concurrent modification exception. ([Delta Lake][1])

## B2.2 Conflict matrix (file/partition level)

Delta’s docs give the core matrix (where “Compaction” refers to file compaction written with `dataChange=false`): ([Delta Lake][1])

| Operation A \ B     | INSERT (blind append) | UPDATE/DELETE/MERGE | COMPACTION (`dataChange=false`) |
| ------------------- | --------------------: | ------------------: | ------------------------------: |
| INSERT              |       cannot conflict |                   — |                               — |
| UPDATE/DELETE/MERGE |          can conflict |        can conflict |                    can conflict |
| COMPACTION          |       cannot conflict |        can conflict |                    can conflict |

Interpretation (protocol-level):

* **Blind append** (“INSERT that doesn’t read the table”) usually only adds files; no rewrite set → low conflict surface.
* **UPDATE/DELETE/MERGE** rewrite files → conflict surface is the overlap in files read/rewritten.
* **Compaction** rewrites files but marks `dataChange=false` → still conflicts if overlapping rewritten sets. ([Delta Lake][1])

Databricks adds a richer table conditioned on isolation level and (optionally) “row-level concurrency” (DV-based), but the key takeaway remains: conflicts are driven by *overlap in rewritten/read files* unless you enable a more granular mechanism. ([Databricks Documentation][4])

## B2.3 The concrete operations that generate transactional writes (delta-spark APIs)

You need the exact API calls because “conflictability” depends on whether the operation **reads/re-writes**.

### DELETE

SQL:

```sql
DELETE FROM people10m WHERE birthDate < '1955-01-01';
DELETE FROM delta.`/tmp/delta/people-10m` WHERE birthDate < '1955-01-01';
```

([Delta Lake][5])

Python:

```python
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, "/tmp/delta/people-10m")
dt.delete("birthDate < '1955-01-01'")
```

([Delta Lake][5])

### UPDATE

Python:

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

dt = DeltaTable.forPath(spark, "/tmp/delta/people-10m")

dt.update(
    condition="gender = 'F'",
    set={"gender": "'Female'"},
)

dt.update(
    condition=col("gender") == "M",
    set={"gender": lit("Male")},
)
```

([Delta Lake][5])

### MERGE (upsert) — highest conflict surface if predicates aren’t tight

SQL:

```sql
MERGE INTO people10m
USING people10mupdates
ON people10m.id = people10mupdates.id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT (...);
```

([Delta Lake][5])

Python builder:

```python
from delta.tables import DeltaTable

t = DeltaTable.forPath(spark, "/tmp/delta/people-10m")

(
  t.alias("t")
   .merge(source.alias("s"), "t.id = s.id")
   .whenMatchedUpdateAll()
   .whenNotMatchedInsertAll()
   .execute()
)
```

(Builder semantics and clause APIs are documented in the delta-spark Python API reference.) ([Delta Lake][6])

## B2.4 Conflict exceptions (names + triggers) and how to handle them in Python

Delta documents these conflict exception types and when they occur: ([Delta Lake][1])

* `ConcurrentAppendException`: another txn added files in the same partition (or anywhere if unpartitioned) that your txn read. ([Delta Lake][1])
* `ConcurrentDeleteReadException`: another txn deleted a file you read (typical with rewrite operations: DELETE/UPDATE/MERGE). ([Delta Lake][1])
* `ConcurrentDeleteDeleteException`: another txn deleted a file you also delete (e.g., two compactions rewriting same files). ([Delta Lake][1])
* `MetadataChangedException`: another txn updated table metadata (ALTER TABLE, protocol upgrade, schema evolution). ([Delta Lake][1])
* `ConcurrentTransactionException`: Structured Streaming started multiple times with the same checkpoint location writing concurrently. ([Delta Lake][1])
* `ProtocolChangedException`: protocol upgrade or multiple writers creating/replacing/initializing the table concurrently. ([Delta Lake][1])

### PySpark catching pattern (practical)

In PySpark you’ll typically see these wrapped in `Py4JJavaError`. A robust handler inspects the JVM exception class name:

```python
import time, random
from py4j.protocol import Py4JJavaError

RETRYABLE = {
    "io.delta.exceptions.ConcurrentAppendException",
    "io.delta.exceptions.ConcurrentDeleteReadException",
    "io.delta.exceptions.ConcurrentDeleteDeleteException",
    "io.delta.exceptions.ConcurrentTransactionException",
}

def run_with_delta_retries(fn, max_attempts=8, base=0.25):
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except Py4JJavaError as e:
            jcls = e.java_exception.getClass().getName()
            if jcls not in RETRYABLE:
                raise
            # exponential backoff + jitter
            time.sleep(base * (2 ** (attempt - 1)) * (0.5 + random.random()))
    raise RuntimeError("Exceeded retries for Delta concurrent modification")
```

**Do not auto-retry** `MetadataChangedException` / `ProtocolChangedException` blindly: those often mean “your planned write is based on stale metadata/protocol; refresh client/runtime or re-resolve schema/protocol first.” ([Delta Lake][1])

## B2.5 Avoiding conflicts: partitioning + disjoint predicates + *explicit* conditions

Delta’s guidance: conflicts occur when operations touch the same set of files; you can make file sets disjoint by **partitioning on predicate columns** and writing **command conditions that restrict scans to those partitions**. ([Delta Lake][1])

### MERGE predicate tightening (the canonical “why did my disjoint jobs still conflict?” fix)

Delta’s docs show that a merge condition that isn’t explicit enough can scan more than intended and conflict; the fix is to add explicit partition filters to the merge condition. ([Delta Lake][1])

Python equivalent:

```python
# assume partitioned by (date, country)
date = "2026-01-01"
country = "US"

(
  tgt.alias("t")
     .merge(
        src.alias("s"),
        f"""
        s.user_id = t.user_id
        AND s.date = t.date AND s.country = t.country
        AND t.date = '{date}' AND t.country = '{country}'
        """
     )
     .whenMatchedUpdateAll()
     .whenNotMatchedInsertAll()
     .execute()
)
```

(Concept mirrors the Delta concurrency doc’s guidance: make the file set disjoint by construction.) ([Delta Lake][1])

### Streaming checkpoint uniqueness (non-negotiable)

Avoid `ConcurrentTransactionException` by ensuring *no two streaming queries* write with the same checkpoint location concurrently. ([Delta Lake][1])

---

# B3) Coordinated commits (and the pre-4.x “LogStore-based” reality)

## B3.1 Storage requirements that Delta needs (why multi-writer is hard)

Delta ACID is predicated on storage providing:

* atomic visibility,
* mutual exclusion for commit file creation/rename,
* consistent listing.

If storage lacks these, Delta relies on the **LogStore API** to implement transactional safety. ([Delta Lake][7])

### Configure a LogStore (Spark conf)

```ini
spark.delta.logStore.<scheme>.impl=<fully-qualified-class-name>
```

([Delta Lake][7])

Important operational constraints:

* Local filesystem may not support concurrent transactional writes (atomic renames not guaranteed); don’t use it to test concurrency. ([Delta Lake][7])

## B3.2 “Multi-writer” on S3 before coordinated commits: `S3DynamoDBLogStore`

Delta’s official storage docs split S3 into:

* **single-cluster**: concurrent reads OK; concurrent writes must originate from a single Spark driver (otherwise possible data loss)
* **multi-cluster** (experimental): uses DynamoDB-backed LogStore for mutual exclusion. ([Delta Lake][7])

### Spark shell / submit config (exact flags)

```bash
bin/spark-shell \
  --packages io.delta:delta-spark_2.13:3,org.apache.hadoop:hadoop-aws:3.4.0,io.delta:delta-storage-s3-dynamodb:4.0.0 \
  --conf spark.hadoop.fs.s3a.access.key=<...> \
  --conf spark.hadoop.fs.s3a.secret.key=<...> \
  --conf spark.delta.logStore.s3a.impl=io.delta.storage.S3DynamoDBLogStore \
  --conf spark.io.delta.storage.S3DynamoDBLogStore.ddb.region=us-west-2
```

([Delta Lake][7])

Per-session configs include:

```ini
spark.io.delta.storage.S3DynamoDBLogStore.ddb.tableName=delta_log
spark.io.delta.storage.S3DynamoDBLogStore.ddb.region=us-east-1
spark.delta.logStore.s3.impl=io.delta.storage.S3DynamoDBLogStore
```

([Delta Lake][7])

## B3.3 Coordinated Commits (Delta 4.x direction): commit coordinator instead of filesystem atomicity

**Goal:** decouple commit success from object-store atomic rename semantics and make multi-engine / multi-cloud writes reliable.

Delta 4.0 preview describes an updated commit protocol:

* user designates a **Commit Coordinator** that manages all writes,
* it coordinates concurrent writes and ensures readers see the freshest version,
* comes with a DynamoDB commit coordinator,
* positions Delta toward future multi-statement/multi-table transactions. ([Delta Lake][8])

Protocol RFC/issue details (mechanics + guarantees):

* introduces a new **writer-only table feature** `coordinatedCommits`,
* compliant clients refuse filesystem-based commits on such tables,
* client sends actions to the commit store; commit store writes unique commit file, ratifies commit; commit may be backfilled to `V.json`. ([GitHub][9])

### Coordinated commits configuration surface (table props + SparkSession defaults)

In Databricks error-class docs (which function as a de facto “contract”), coordinated commits configs appear as:

**Table properties (must be set together):**

* `delta.coordinatedCommits.commitCoordinator-preview`
* `delta.coordinatedCommits.commitCoordinatorConf-preview`
  (and internal/derived: `delta.coordinatedCommits.tableConf-preview`) ([Databricks Documentation][10])

**SparkSession defaults (must be set together):**

* `coordinatedCommits.commitCoordinator-preview`
* `coordinatedCommits.commitCoordinatorConf-preview` ([Databricks Documentation][10])

Databricks explicitly enforces:

* you must set both configs together (command/session) or neither,
* you cannot override coordinated commits confs on an existing target table,
* coordinated commits depends on in-commit timestamps and sets those internally (don’t override `delta.enableInCommitTimestamps` / `delta.inCommitTimestampEnablement*`). ([Databricks Documentation][10])

### Example (preview) CREATE TABLE with coordinator (syntax in the wild)

A concrete example using DynamoDB as coordinator is shown here (preview properties): ([LinkedIn][11])

```sql
CREATE TABLE cc_db.events (
  id  BIGINT,
  txt STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.coordinatedCommits.commitCoordinator-preview' = 'dynamodb',
  'delta.coordinatedCommits.commitCoordinatorConf-preview' =
    '{"dynamoDBTableName":"delta-cc-coordinator","dynamoDBEndpoint":"http://localhost:8000"}'
);
```

### Example SparkSession defaults (preview)

```python
spark.conf.set("coordinatedCommits.commitCoordinator-preview", "dynamodb")
spark.conf.set("coordinatedCommits.commitCoordinatorConf-preview",
               '{"dynamoDBTableName":"delta-cc-coordinator","dynamoDBEndpoint":"http://localhost:8000"}')
```

(Exact key names and “set both or neither” constraint are codified in error conditions.) ([Databricks Documentation][10])

---



[1]: https://docs.delta.io/concurrency-control/?utm_source=chatgpt.com "Concurrency control"
[2]: https://docs.databricks.com/aws/en/delta/tutorial?utm_source=chatgpt.com "Tutorial: Create and manage Delta Lake tables"
[3]: https://learn.microsoft.com/en-us/azure/databricks/delta/history?utm_source=chatgpt.com "Work with table history - Azure Databricks"
[4]: https://docs.databricks.com/aws/en/optimizations/isolation-level "Isolation levels and write conflicts on Databricks | Databricks on AWS"
[5]: https://docs.delta.io/delta-update/ "Table deletes, updates, and merges | Delta Lake"
[6]: https://docs.delta.io/api/latest/python/spark/ "Welcome to Delta Lake’s Python documentation page — delta-spark 4.0.0 documentation"
[7]: https://docs.delta.io/delta-storage/ "Storage configuration | Delta Lake"
[8]: https://delta.io/blog/delta-lake-4-0/ "Delta Lake 4.0 Preview | Delta Lake"
[9]: https://github.com/delta-io/delta/issues/2598?utm_source=chatgpt.com "[Protocol Change Request] Delta Coordinated Commits"
[10]: https://docs.databricks.com/aws/en/error-messages/error-classes?utm_source=chatgpt.com "Error conditions in Databricks"
[11]: https://ro.linkedin.com/pulse/spark-delta-table-coordinated-commit-example-krzysztof-baranski-27sjf?utm_source=chatgpt.com "Spark Delta table Coordinated Commit Example"

## C) Write path + data mutation APIs (the “DML surface”)

Below is a **syntax-first** catalog of the **exact knobs / APIs** you use to ingest, overwrite, mutate, and stream into Delta tables (Spark / `delta-spark` as the reference surface, plus the Sparkless `delta-rs` / `deltalake` bindings where applicable).

---

# C1) Ingest / write modes

## C1.1 Table addressing: **path-defined** vs **metastore-defined**

Delta supports both **metastore-defined tables** and **path-defined tables** (no metastore entry). Your DML code usually needs to choose one style and stick to it. ([Delta Lake][1])

### Spark SQL identifiers

```sql
-- metastore table
SELECT * FROM default.people10m;

-- path-defined table
SELECT * FROM delta.`/tmp/delta/people10m`;
```

([Delta Lake][1])

### delta-spark Python: open a table

```python
from delta.tables import DeltaTable

# path-defined
dt = DeltaTable.forPath(spark, "/tmp/delta/people-10m")

# metastore-defined
dt = DeltaTable.forName(spark, "default.people10m")
```

([Delta Lake][2])

---

## C1.2 Create / replace table (DDL + builders)

### SQL DDL (metastore vs path)

```sql
CREATE TABLE IF NOT EXISTS default.people10m (...) USING DELTA;

CREATE OR REPLACE TABLE delta.`/tmp/delta/people10m` (...) USING DELTA;
```

([Delta Lake][1])

### DataFrameWriter “create + write”

```python
# create metastore table from df schema
df.write.format("delta").saveAsTable("default.people10m")

# create/replace path-defined table
df.write.format("delta").mode("overwrite").save("/tmp/delta/people10m")
```

([Delta Lake][1])

### DeltaTableBuilder (structured create/replace; Python API surface)

Use when you want “DDL-like” control programmatically (columns, partitioning, properties, location).

```python
from delta.tables import DeltaTable
from pyspark.sql.types import IntegerType

t = (DeltaTable.createIfNotExists(spark)
     .tableName("default.people10m")
     .addColumn("id", "INT")
     .addColumn("c2", IntegerType(), generatedAlwaysAs="id + 1")
     .partitionedBy("id")
     .property("description", "table with people data")
     .execute())
```

Methods include `.tableName(...)`, `.location(...)`, `.addColumn(...)`, `.addColumns(...)`, `.partitionedBy(...)`, `.property(...)`, then `.execute()`. ([Delta Lake][1])

---

## C1.3 Append vs overwrite (batch writes)

### Append

```python
df.write.format("delta").mode("append").save("/tmp/delta/people10m")
df.write.format("delta").mode("append").saveAsTable("default.people10m")
```

```sql
INSERT INTO default.people10m SELECT * FROM morePeople;
```

([Delta Lake][1])

### Overwrite (replace *all* data)

```python
df.write.format("delta").mode("overwrite").save("/tmp/delta/people10m")
df.write.format("delta").mode("overwrite").saveAsTable("default.people10m")
```

```sql
INSERT OVERWRITE TABLE default.people10m SELECT * FROM morePeople;
```

([Delta Lake][1])

---

## C1.4 Selective overwrite patterns

### (1) `replaceWhere` (predicate overwrite; “overwrite subset atomically”)

```python
(replace_data.write
  .format("delta")
  .mode("overwrite")
  .option("replaceWhere", "start_date >= '2017-01-01' AND end_date <= '2017-01-31'")
  .save("/tmp/delta/events")
)
```

Delta validates that **all written rows match the predicate**; you can disable this check:

```python
spark.conf.set("spark.databricks.delta.replaceWhere.constraintCheck.enabled", False)
```

([Delta Lake][1])

### (2) Dynamic partition overwrite (`partitionOverwriteMode=dynamic`)

Semantics: overwrite only partitions *present in the incoming write*, leave other partitions unchanged. Only applicable with overwrite mode. ([Delta Lake][1])

Session-level:

```python
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.write.format("delta").mode("overwrite").saveAsTable("default.people10m")
```

Per-write override:

```python
(df.write.format("delta")
  .mode("overwrite")
  .option("partitionOverwriteMode", "dynamic")
  .saveAsTable("default.people10m"))
```

([Delta Lake][1])

**Hard constraints / interactions (practical correctness):**

* cannot set `overwriteSchema=true` when using dynamic partition overwrite ([Databricks Documentation][3])
* cannot specify both `partitionOverwriteMode` and `replaceWhere` in the same `DataFrameWriter` op ([Databricks Documentation][3])
* if `replaceWhere` is specified, it takes precedence over the session-level `partitionOverwriteMode` ([Databricks Documentation][3])

---

## C1.5 File sizing knob (small-files control at write time)

```python
df.write.format("delta").mode("append").option("maxRecordsPerFile", "10000").save("/tmp/delta/people10m")
```

Also available as Spark session config `spark.sql.files.maxRecordsPerFile`. ([Delta Lake][1])

---

## C1.6 Idempotent batch writes (dedupe retries via txn markers)

Delta supports **idempotent** batch writes using two DataFrameWriter options:

* `txnAppId` (stable writer/job identity)
* `txnVersion` (monotonic version for the data payload; must increase) ([Delta Lake][1])

```python
(dataFrame.write.format("delta")
  .option("txnAppId", "dailyETL")
  .option("txnVersion", 23424)
  .mode("append")
  .save("/tmp/delta/people10m"))
```

Session-level equivalents:

* `spark.databricks.delta.write.txnAppId`
* `spark.databricks.delta.write.txnVersion`
* `spark.databricks.delta.write.txnVersion.autoReset.enabled` (reset after every write) ([Delta Lake][1])

**Failure mode:** if you reuse `(txnAppId, txnVersion)` with *different* data, the second write is ignored. ([Delta Lake][1])

---

## C1.7 User-defined commit metadata (`userMetadata`)

Write-side:

```python
(df.write.format("delta")
  .mode("overwrite")
  .option("userMetadata", "overwritten-for-fixing-incorrect-data")
  .save("/tmp/delta/people10m"))
```

Or session-level: `spark.databricks.delta.commitInfo.userMetadata`. Commit metadata is visible via `history`. ([Delta Lake][1])

---

## C1.8 Sparkless (delta-rs / `deltalake`) equivalents

### `write_deltalake(...)` signature + key parameters

Core writer function:

* `mode`: `'error' | 'append' | 'overwrite' | 'ignore'`
* `schema_mode`: `'merge' | 'overwrite'` (also supported for append for `'merge'`)
* `partition_by` (only needed for creating a new table)
* `predicate` (replaceWhere-style partial overwrite when `mode='overwrite'`)
* perf knobs: `target_file_size`, `writer_properties`, `commit_properties`, hooks ([Delta IO][4])

```python
from deltalake import write_deltalake
write_deltalake("path/to/table", data, mode="append")
write_deltalake("path/to/table", data, mode="overwrite", schema_mode="merge")
write_deltalake("path/to/table", data, mode="overwrite", predicate="id = '1'")
```

([Delta IO][5])

---

# C2) Row-level mutation (copy-on-write + merge-on-read variants)

## C2.1 DELETE / UPDATE (Spark SQL + DeltaTable API)

### SQL

```sql
DELETE FROM people10m WHERE birthDate < '1955-01-01';
UPDATE people10m SET gender = 'Female' WHERE gender = 'F';
```

([Delta Lake][6])

### Python (DeltaTable API)

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

dt = DeltaTable.forPath(spark, "/tmp/delta/people-10m")

dt.delete("birthDate < '1955-01-01'")
dt.delete(col("birthDate") < "1960-01-01")

dt.update(condition="gender = 'F'", set={"gender": "'Female'"})
dt.update(condition=col("gender") == "M", set={"gender": lit("Male")})
```

([Delta Lake][6])

**Important behavioral note:** `DELETE` removes rows from the latest snapshot but old data files remain until vacuum/retention policy removes them. ([Delta Lake][6])

---

## C2.2 MERGE (upsert) — syntax + builder methods

### SQL MERGE (baseline)

```sql
MERGE INTO target t
USING source s
ON s.key = t.key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
```

Semantics and clause ordering are defined; order of multiple clauses matters; only the last clause in each group may omit its condition. ([Delta Lake][6])

### Python builder surface (delta-spark)

`DeltaTable.merge(source, condition)` returns `DeltaMergeBuilder`, then you add clauses and `.execute()`. Clause methods and signatures: ([Delta Lake][7])

* Matched:

  * `whenMatchedUpdate(condition=None, set: dict[str, str|Column])`
  * `whenMatchedUpdateAll(condition=None)`
  * `whenMatchedDelete(condition=None)`
* Not matched:

  * `whenNotMatchedInsert(condition=None, values: dict[str, str|Column])`
  * `whenNotMatchedInsertAll(condition=None)`
* Not matched by source:

  * `whenNotMatchedBySourceUpdate(condition=None, set: dict[str, str|Column])`
  * `whenNotMatchedBySourceDelete(condition=None)`
* Terminal:

  * `execute()`

Example (mixed SQL strings + expressions):

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, expr, lit

t = DeltaTable.forPath(spark, "/tmp/delta/events")

(t.alias("events")
 .merge(source=updatesDF.alias("updates"),
        condition="events.eventId = updates.eventId")
 .whenMatchedUpdate(set={
     "data": "updates.data",
     "count": "events.count + 1",
 })
 .whenNotMatchedInsert(values={
     "date": "updates.date",
     "eventId": "updates.eventId",
     "data": "updates.data",
     "count": "1",
     "missed_count": "0",
 })
 .whenNotMatchedBySourceUpdate(set={
     "missed_count": "events.missed_count + 1",
 })
 .execute())
```

([Delta Lake][7])

### MERGE failure mode: multiple source rows match a single target row

A merge can fail if multiple source rows match the same target row and the merge attempts to update it (ambiguous). Pre-deduplicate source by key (e.g., window + `row_number = 1`). ([Delta Lake][6])

---

## C2.3 Schema evolution + schema replacement in DML

### MERGE automatic schema evolution (OSS Delta)

Enable before merge:

```python
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

Then MERGE can add missing columns from source into target schema under documented rules. ([Delta Lake][6])

### MERGE “with schema evolution” (Databricks Runtime 15.4+)

SQL:

```sql
MERGE WITH SCHEMA EVOLUTION INTO target
USING source
ON source.key = target.key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE THEN DELETE;
```

Python:

```python
(targetTable
  .merge(sourceDF, "source.key = target.key")
  .withSchemaEvolution()
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .whenNotMatchedBySourceDelete()
  .execute())
```

([Databricks Documentation][8])

### Schema evolution for writes (append/stream)

Enable per write:

```python
(df.write
  .option("mergeSchema", "true")
  .mode("append")
  .saveAsTable("table_name"))
```

Or for streaming:

```python
(df.writeStream
  .option("mergeSchema", "true")
  .option("checkpointLocation", "<path>")
  .toTable("table_name"))
```

([Microsoft Learn][9])

### Schema replacement on overwrite

```python
(df.write
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable("table_name"))
```

Constraint: cannot set `overwriteSchema=true` with dynamic partition overwrite. ([Microsoft Learn][9])

---

## C2.4 Copy-on-write vs merge-on-read for DML: **Deletion Vectors**

Default behavior: row-level DML rewrites the affected Parquet files. With **deletion vectors enabled**, some operations can mark rows as removed without rewriting the base file (merge-on-read), and readers apply DVs at read time. ([Delta Lake][10])

Enable (protocol upgrade; breaks older-reader compatibility):

```sql
ALTER TABLE <table_name> SET TBLPROPERTIES('delta.enableDeletionVectors' = true);
```

([Delta Lake][10])

DV applicability by Delta version (protocol doc summary):

* SCAN: 2.3+
* DELETE: 2.4+
* UPDATE: 3.0+ (enabled by default since 3.1)
* MERGE: 3.1+ (enabled by default since 3.1) ([Delta Lake][10])

**“Apply DV changes physically”** (rewrite base files):

* run DML with DVs disabled,
* run `OPTIMIZE`,
* or run `REORG TABLE … APPLY (PURGE)` (strongest “rewrite all impacted files”). ([Delta Lake][10])

---

## C2.5 Sparkless delta-rs mutation APIs (Python)

### `DeltaTable.delete(...)`

```python
from deltalake import DeltaTable
dt = DeltaTable("path/to/table")
dt.delete(predicate="id = '1'")  # predicate is SQL WHERE fragment
```

Semantics: scans to find candidate files, then rewrites files without the deleted rows. ([Delta IO][11])

### `DeltaTable.update(...)`

Two update modes:

* `updates`: mapping column → SQL expression string
* `new_values`: mapping column → python scalar/list, with optional `error_on_type_mismatch` ([Delta IO][11])

```python
dt.update(
  updates={"count": "count + 1"},
  predicate="eventId = 'abc'",
)
```

([Delta IO][11])

### `DeltaTable.merge(...)` → `TableMerger` builder

`DeltaTable.merge(source, predicate, source_alias=..., target_alias=..., streamed_exec=True, ...)` returns a builder with clause methods analogous to Spark. ([Delta IO][11])

Clause example (API names are snake_case):

```python
tm = dt.merge(source=source_table, predicate="target.id = source.id")
(tm
  .when_matched_delete(predicate="source.is_deleted = true")
  .when_not_matched_by_source_delete()
  # ... other clauses ...
)
```

Backtick quoting for special column names is explicitly called out. ([Delta IO][12])

---

# C3) Streaming writes “exactly once” (Delta + Structured Streaming)

## C3.1 Delta as a streaming **sink** (append + complete)

Delta sink integration is `writeStream.format("delta")` with `checkpointLocation`. Delta docs explicitly tie “exactly-once” to the transaction log. ([Delta Lake][13])

### Append mode (default)

```python
(events.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/tmp/delta/events/_checkpoints/")
  .start("/tmp/delta/events"))
```

Or write to a metastore table (Spark 3.1+):

```python
(events.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/tmp/delta/events/_checkpoints/")
  .toTable("events"))
```

([Delta Lake][13])

### Complete mode (rewrite target each micro-batch; aggregates)

```python
(spark.readStream.format("delta").load("/tmp/delta/events")
  .groupBy("customerId").count()
  .writeStream.format("delta").outputMode("complete")
  .option("checkpointLocation", "/tmp/delta/eventsByCustomer/_checkpoints/")
  .start("/tmp/delta/eventsByCustomer"))
```

([Delta Lake][13])

---

## C3.2 Delta as a streaming **source** (tail the log)

Baseline:

```python
df = spark.readStream.format("delta").load("/tmp/delta/events")
```

([Delta Lake][13])

### Throughput control (micro-batch sizing)

* `maxFilesPerTrigger`
* `maxBytesPerTrigger` ([Delta Lake][13])

```python
(spark.readStream.format("delta")
  .option("maxFilesPerTrigger", "1000")
  .option("maxBytesPerTrigger", "134217728")  # ~128MiB
  .load("/tmp/delta/events"))
```

([Delta Lake][13])

### Handling non-append changes in the source

Structured Streaming will error on source updates/deletes unless you opt in:

* `ignoreDeletes`
* `ignoreChanges` (subsumes ignoreDeletes; can emit duplicates; deletes not propagated) ([Delta Lake][13])

```python
(spark.readStream.format("delta")
  .option("ignoreChanges", "true")
  .load("/tmp/delta/user_events"))
```

([Delta Lake][13])

### Start position (tail from a version/timestamp)

* `startingVersion` (or `"latest"`)
* `startingTimestamp` (mutually exclusive; only applied on first start; ignored once checkpoint has progress) ([Delta Lake][13])

```python
(spark.readStream.format("delta")
  .option("startingVersion", "latest")
  .load("/tmp/delta/user_events"))
```

([Delta Lake][13])

### Initial snapshot ordering for watermark correctness

`withEventTimeOrder=true` to process the initial snapshot in event-time buckets (prevents “late event drops” induced by file mtime ordering). ([Delta Lake][13])

---

## C3.3 Streaming + schema evolution / non-additive changes

### Schema tracking for column mapping / non-additive schema changes

Use `schemaTrackingLocation` on the **readStream**, tied under the same checkpoint root; this enables streaming reads “as if time-travelled” for past schema versions. ([Delta Lake][13])

```python
checkpoint_path = "/path/to/checkpointLocation"

(spark.readStream
  .option("schemaTrackingLocation", checkpoint_path)
  .table("delta_source_table")
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .toTable("output_table"))
```

([Delta Lake][13])

---

## C3.4 Streaming upserts: `foreachBatch` + MERGE (and how to make it idempotent)

### Core issue

`foreachBatch` lets you run arbitrary batch logic per micro-batch, but it does **not** make writes idempotent by itself (retries can duplicate data). ([Delta Lake][13])

### Idempotent `foreachBatch` pattern (txn markers)

Use `txnAppId` and `txnVersion` inside the batch write; Delta uses `(txnAppId, txnVersion)` to identify duplicates and ignore them on retries. ([Delta Lake][13])

```python
app_id = "my_stream_or_job_id"  # stable across restarts if reusing same checkpoint

def write_idempotent(batch_df, batch_id: int):
    (batch_df.write.format("delta")
      .option("txnAppId", app_id)
      .option("txnVersion", batch_id)
      .mode("append")
      .save("/tmp/delta/target"))

(streamingDF.writeStream
  .foreachBatch(write_idempotent)
  .option("checkpointLocation", "/tmp/delta/_checkpoints/my_stream")
  .start())
```

**Critical restart rule:** if you delete the streaming checkpoint and restart from a new checkpoint, you must change `app_id`, otherwise writes can be ignored because batch IDs restart at 0. ([Delta Lake][13])

> For actual upserts, the common pattern is `foreachBatch(lambda df, id: DeltaTable.forPath(...).merge(...).execute())`, and you still need a replay-safe strategy (either txn markers + append log, or deterministic MERGE with exactly-once upstream). The Delta docs explicitly push txn markers as the primitive for replay detection. ([Delta Lake][13])

[1]: https://docs.delta.io/delta-batch/ "Table batch reads and writes | Delta Lake"
[2]: https://docs.delta.io/delta-utility/ "Table utility commands | Delta Lake"
[3]: https://docs.databricks.com/aws/en/delta/selective-overwrite "Selectively overwrite data with Delta Lake | Databricks on AWS"
[4]: https://delta-io.github.io/delta-rs/api/delta_writer/ "Writer - Delta Lake Documentation"
[5]: https://delta-io.github.io/delta-rs/usage/writing/ "Writing Delta Tables - Delta Lake Documentation"
[6]: https://docs.delta.io/delta-update/ "Table deletes, updates, and merges | Delta Lake"
[7]: https://docs.delta.io/api/latest/python/spark/ "Welcome to Delta Lake’s Python documentation page — delta-spark 4.0.0 documentation"
[8]: https://docs.databricks.com/aws/en/delta/update-schema "Update table schema | Databricks on AWS"
[9]: https://learn.microsoft.com/en-us/azure/databricks/delta/update-schema "Update table schema - Azure Databricks | Microsoft Learn"
[10]: https://docs.delta.io/delta-deletion-vectors/ "What are deletion vectors? | Delta Lake"
[11]: https://delta-io.github.io/delta-rs/api/delta_table/ "DeltaTable - Delta Lake Documentation"
[12]: https://delta-io.github.io/delta-rs/api/delta_table/delta_table_merger/ "TableMerger - Delta Lake Documentation"
[13]: https://docs.delta.io/delta-streaming/ "Table streaming reads and writes | Delta Lake"

## D) Versioning, historization, rollback, and reproducibility

---

# D1) Time travel

## D1.1 Spark SQL `AS OF` (version / timestamp)

**Syntax (Delta on Spark):**

```sql
SELECT * FROM table_name TIMESTAMP AS OF timestamp_expression;
SELECT * FROM table_name VERSION  AS OF version;
```

`timestamp_expression` accepts timestamp-castable expressions (string literal, `cast(...)`, date literal, arithmetic like `current_timestamp() - interval 12 hours`, etc.). Version is a `long` typically taken from `DESCRIBE HISTORY`. ([Delta Lake][1])

**Examples (metastore vs path-defined):**

```sql
SELECT * FROM default.people10m TIMESTAMP AS OF '2018-10-18T22:15:12.013Z';
SELECT * FROM delta.`/tmp/delta/people10m` VERSION AS OF 123;
```

([Delta Lake][1])

## D1.2 Spark DataFrameReader options (`versionAsOf`, `timestampAsOf`)

```python
df_ts = (
  spark.read.format("delta")
    .option("timestampAsOf", "2019-01-01T00:00:00.000Z")  # or "2019-01-01"
    .load("/tmp/delta/people10m")
)

df_v = (
  spark.read.format("delta")
    .option("versionAsOf", 123)
    .load("/tmp/delta/people10m")
)
```

([Delta Lake][1])

**“Pin latest version once” pattern (avoid mid-job drift):**

```python
history = spark.sql("DESCRIBE HISTORY delta.`/tmp/delta/people10m`")
latest_version = history.selectExpr("max(version)").collect()[0][0]

df = (spark.read.format("delta")
        .option("versionAsOf", latest_version)
        .load("/tmp/delta/people10m"))
```

([Delta Lake][1])

## D1.3 Timestamp-based time travel caveat (table copy / DR)

Delta’s *timestamp* for version **N** is (by default) derived from the **modification timestamp of `_delta_log/N.json`**, so *copying the table directory* can change those mtimes and break timestamp-based time travel; version-based time travel is unaffected. ([Delta Lake][1])

### Mitigation: In-Commit Timestamps (protocol-level)

The **In-Commit Timestamps** writer feature stores a monotonically increasing commit timestamp inside the commit itself (in `commitInfo.inCommitTimestamp`) and readers should use it for commit timestamp semantics (time travel, `DESCRIBE HISTORY`). ([GitHub][2])

**Protocol requirements (must all hold):**

* table on **Writer Version 7**
* feature `inCommitTimestamp` present in `protocol.writerFeatures`
* table property `delta.enableInCommitTimestamps = true` ([GitHub][2])

**Writer requirements (when enabled):**

* `commitInfo` must exist and be the **first** action in every commit
* `commitInfo` must include `inCommitTimestamp` (`long`, ms since epoch)
* `inCommitTimestamp` must be `max(attempt_time, prev_inCommitTimestamp + 1ms)`
* enablement provenance for mixed-history tables:

  * `delta.inCommitTimestampEnablementVersion`
  * `delta.inCommitTimestampEnablementTimestamp` ([GitHub][2])

**Reader rules (mixed history):**

* for versions ≥ enablementVersion: use `commitInfo.inCommitTimestamp`
* for versions < enablementVersion: use log file modification time
* for timestamp-based travel “as of X”: choose candidate versions based on whether X is before/after enablementTimestamp ([GitHub][2])

> Practical upshot: if your workflow involves table directory copy/move (DR, replication, shallow clones, manual repairs), prefer `VERSION AS OF` unless you’ve enabled In-Commit Timestamps.

## D1.4 Sparkless (delta-rs / `deltalake`) time travel APIs

Create at a fixed version:

```python
from deltalake import DeltaTable
dt = DeltaTable("s3://bucket/table", version=2)
```

([Delta IO][3])

Move between versions / datetime:

```python
dt.load_version(1)
dt.load_with_datetime("2021-11-04 00:05:23.283+00:00")
```

Warning: versions may be unavailable if vacuumed. ([Delta IO][3])

---

# D2) Table history / audit trail

## D2.1 SQL: `DESCRIBE HISTORY` (+ LIMIT)

```sql
DESCRIBE HISTORY '/data/events/';          -- path
DESCRIBE HISTORY delta.`/data/events/`;    -- explicit delta path
DESCRIBE HISTORY '/data/events/' LIMIT 1;  -- last op only
DESCRIBE HISTORY eventsTable;              -- metastore table
```

([Delta Lake][4])

Delta’s docs note history is retained for **30 days by default** (driven by log retention). ([Delta Lake][4])

## D2.2 delta-spark Python: `DeltaTable.history([limit])`

```python
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, pathToTable)
full = dt.history()      # DataFrame
last = dt.history(1)     # DataFrame
```

([Delta Lake][4])

History output columns (core set): `version`, `timestamp`, `operation`, `operationParameters`, `readVersion`, `isolationLevel`, `isBlindAppend`, `operationMetrics`, `userMetadata`, etc. ([Delta Lake][4])

## D2.3 delta-rs Python: `DeltaTable.history(limit=None) -> list[dict]`

```python
from deltalake import DeltaTable
dt = DeltaTable("path/to/table")
ops = dt.history()        # list[dict]
ops1 = dt.history(limit=1)
```

([Delta IO][5])

## D2.4 Commit timestamp semantics in history

Protocol: commit timestamp used for operations like time travel and `DESCRIBE HISTORY` is derived from either `commitInfo.inCommitTimestamp` (if In-Commit Timestamps enabled) or the log file mtime (default). ([GitHub][2])

---

# D3) Restore and clone

## D3.1 RESTORE (rollback “current state”)

### SQL

```sql
RESTORE TABLE db.target_table TO VERSION   AS OF <version>;
RESTORE TABLE delta.`/data/target/` TO TIMESTAMP AS OF <timestamp>;
```

Timestamp formats supported: `yyyy-MM-dd HH:mm:ss` or date-only `yyyy-MM-dd`. ([Delta Lake][4])

### delta-spark Python API

```python
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, "/data/target")

metrics_v = dt.restoreToVersion(0)
metrics_t = dt.restoreToTimestamp("2019-02-14")
```

Both return a single-row DataFrame of metrics. ([Delta Lake][4])

### Failure modes + knobs

* Restoring to a version where backing files were deleted (manual delete or `VACUUM`) fails.
* Partial restore is possible by setting `spark.sql.files.ignoreMissingFiles = true`. ([Delta Lake][4])

### Streaming interaction

`RESTORE` commits are `dataChange=true`; downstream structured streaming that tails the table can treat restored files as newly added and reprocess them (duplicate effects). ([Delta Lake][4])

### delta-rs Python: `DeltaTable.restore(target, ...)`

```python
from deltalake import DeltaTable
dt = DeltaTable("path/to/table")

out = dt.restore(
  target=12,  # or datetime/date-string
  ignore_missing_files=False,
  protocol_downgrade_allowed=False,
)
```

Restore compares current vs target snapshot and commits new `AddFile`/`RemoveFile` actions; if concurrent ops occur, restore can fail. ([Delta IO][6])

---

## D3.2 CLONE (reproducibility / experiments / backup)

### OSS Delta: **SHALLOW CLONE** (Delta Lake 2.3+)

Creates a new Delta table whose metadata points at existing data files; optional time-travel pinning.

```sql
CREATE TABLE delta.`/data/target/` SHALLOW CLONE delta.`/data/source/`;

CREATE OR REPLACE TABLE db.target_table SHALLOW CLONE db.source_table;

CREATE TABLE db.target_table SHALLOW CLONE delta.`/data/source` VERSION   AS OF version;

CREATE TABLE db.target_table SHALLOW CLONE delta.`/data/source` TIMESTAMP AS OF timestamp_expression;
```

([Delta Lake][4])

Key semantics:

* cloned table has **independent history**; time travel inputs differ (source version 100 → clone version 0). ([Delta Lake][4])
* shallow clones reference source files; `VACUUM` on source can break the clone until re-cloned/replace. ([Delta Lake][4])

**Clone + retention overrides (archive/repro):**

```sql
CREATE OR REPLACE TABLE archive.my_table SHALLOW CLONE prod.my_table
TBLPROPERTIES (
  delta.logRetentionDuration = '3650 days',
  delta.deletedFileRetentionDuration = '3650 days'
)
LOCATION 'xx://archive/my_table';
```

([Delta Lake][4])

### Databricks: `CREATE TABLE ... [SHALLOW|DEEP] CLONE ...` (+ Python clone APIs)

SQL syntax:

```sql
CREATE TABLE [IF NOT EXISTS] t
  [SHALLOW | DEEP] CLONE source_table [TBLPROPERTIES (...)] [LOCATION '...'];

CREATE OR REPLACE TABLE t
  [SHALLOW | DEEP] CLONE source_table [TBLPROPERTIES (...)] [LOCATION '...'];
```

([Databricks Documentation][7])

Python DeltaTable clone APIs (Databricks docs):

```python
from delta.tables import DeltaTable

src = DeltaTable.forName(spark, "source_table")
src.clone(target="target_table", isShallow=True, replace=False)
src.cloneAtVersion(version=1, target="target_table", isShallow=True, replace=False)
src.cloneAtTimestamp(timestamp="2019-01-01", target="target_table", isShallow=True, replace=False)
```

([Databricks Documentation][8])

Deep vs shallow behavior notes (Databricks):

* deep clone copies data + metadata; shallow references source files
* deep clones can also clone stream metadata so streaming can resume on the clone target. ([Databricks Documentation][8])

---

# D4) Retention controls that bound historization

Time travel/rollback depends on retaining **both**:

1. log entries + checkpoints
2. data files referenced by historical snapshots ([Delta Lake][1])

## D4.1 Retention table properties (protocol-level knobs surfaced as TBLPROPERTIES)

### `delta.logRetentionDuration`

Controls how long log/history is kept; Delta automatically cleans up log entries older than this interval when checkpoints are written. Default is **30 days**. ([Delta Lake][1])

### `delta.deletedFileRetentionDuration`

Minimum age before a logically removed data file becomes eligible for vacuum. Default **1 week**; must exceed max job runtime / max stream downtime to avoid stale-reader failures. ([Delta Lake][9])

Set / override:

```sql
ALTER TABLE my_table SET TBLPROPERTIES (
  delta.logRetentionDuration = 'interval 180 days',
  delta.deletedFileRetentionDuration = 'interval 30 days'
);
```

Default time travel is “up to 30 days” unless you vacuum or change these thresholds. ([Delta Lake][1])

**Non-obvious log pruning edge case:** time travel to version *V* requires consecutive log entries since the previous checkpoint; aggressive log cleanup can make some versions unreachable even if V is newer than the retention interval. ([Delta Lake][1])

## D4.2 VACUUM (data-file GC) is what actually destroys old snapshots

### OSS Delta / delta-spark SQL

Core forms (selected):

```sql
VACUUM eventsTable;
VACUUM eventsTable LITE;
VACUUM delta.`/data/events/` RETAIN 100 HOURS;
VACUUM eventsTable DRY RUN;
VACUUM eventsTable USING INVENTORY inventoryTable;
```

Python:

```python
from delta.tables import DeltaTable
dt = DeltaTable.forPath(spark, pathToTable)
dt.vacuum()      # default retention
dt.vacuum(100)   # hours
```

Config knobs:

* parallel delete: `spark.databricks.delta.vacuum.parallelDelete.enabled=true`
* safety check toggle: `spark.databricks.delta.retentionDurationCheck.enabled=false` ([Delta Lake][4])

### Databricks SQL VACUUM syntax (mode selector)

```sql
VACUUM table_name { { FULL | LITE } | DRY RUN } [...]
```

`LITE` uses the delta log instead of directory listing and can fail if the log has been pruned. ([Databricks Documentation][10])

**Safety**: recommended retention ≥ 7 days; Databricks documents a safety check and how to disable it via `spark.databricks.delta.retentionDurationCheck.enabled=false`. ([Databricks Documentation][10])

**Data-loss footgun**: disabling the safety check + vacuuming with `RETAIN 0 HOURS` can delete committed/uncommitted/temporary files and corrupt concurrent workflows. ([Databricks Knowledge Base][11])

### delta-rs VACUUM (Sparkless)

`DeltaTable.vacuum()` is dry-run by default; signature includes:

* `retention_hours: Optional[int]` (defaults to table config or 1 week)
* `dry_run: bool`
* `enforce_retention_duration: bool` (allow smaller than configured retention when false) ([Delta IO][3])

## D4.3 VACUUM vs log retention: what gets deleted

* `VACUUM` deletes **data files** (and non-Delta junk files depending on implementation), not `_delta_log`; log files are cleaned asynchronously after checkpoints, controlled by `delta.logRetentionDuration`. ([Delta Lake][4])
* Running `VACUUM` destroys your ability to time travel older than the retained window because the required data files are gone. ([Databricks Documentation][10])

[1]: https://docs.delta.io/delta-batch/ "Table batch reads and writes | Delta Lake"
[2]: https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md "raw.githubusercontent.com"
[3]: https://delta-io.github.io/delta-rs/python/usage.html?utm_source=chatgpt.com "Usage — delta-rs documentation"
[4]: https://docs.delta.io/delta-utility/ "Table utility commands | Delta Lake"
[5]: https://delta-io.github.io/delta-rs/api/delta_table/?utm_source=chatgpt.com "DeltaTable - Delta Lake Documentation"
[6]: https://delta-io.github.io/delta-rs/python/api_reference.html?utm_source=chatgpt.com "API Reference — delta-rs documentation"
[7]: https://docs.databricks.com/aws/en/sql/language-manual/delta-clone "CREATE TABLE CLONE | Databricks on AWS"
[8]: https://docs.databricks.com/aws/en/delta/clone "Clone a table on Databricks | Databricks on AWS"
[9]: https://docs.delta.io/table-properties/ "Delta Table Properties Reference | Delta Lake"
[10]: https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum "VACUUM | Databricks on AWS"
[11]: https://kb.databricks.com/delta/data-missing-vacuum-parallel-write?utm_source=chatgpt.com "Vaccuming with zero retention results in data loss"

## E) Schema, types, and data quality enforcement

This section is about **(a)** what the protocol can encode + enforce, and **(b)** the concrete **Spark / delta-spark Python** and **delta-rs (`deltalake`) Python** surfaces you use to exercise those capabilities.

---

# E1) Schema enforcement (“reject bad writes”)

## E1.1 Spark / delta-spark: schema validation rules (default behavior)

Delta validates that the **DataFrame schema is compatible with the target table** on write. The core rules are: **every DataFrame column must exist in the table**, data types must match, and column names can’t differ only by case; extra DF columns or type mismatches raise exceptions; missing DF columns become `NULL` on write. Partitioning mismatches (e.g., passing `partitionBy` that doesn’t match the existing table) also error. ([Delta Lake][1])

### Minimal “it will error if incompatible” batch write

```python
(df.write
  .format("delta")
  .mode("append")
  .save("/mnt/delta/t"))  # errors on extra columns / type mismatch / case-only diffs
```

(Compatibility rules are enforced as above.) ([Delta Lake][1])

## E1.2 “Allow new columns” vs “replace schema” knobs (Spark)

Delta gives you two orthogonal mechanisms when a write is rejected:

### Additive schema evolution (new columns)

Enable **per write**:

```python
(df.write
  .format("delta")
  .mode("append")
  .option("mergeSchema", "true")
  .save("/mnt/delta/t"))
```

Or session-level default (prefer per-write in production):

```python
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

([Databricks Documentation][2])

### Destructive schema replacement (rewrite table with new schema)

Overwrite + replace schema:

```python
(df.write
  .format("delta")
  .mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable("db.t"))
```

([Databricks Documentation][2])

## E1.3 delta-rs (`deltalake`) enforcement + schema evolution knobs

`write_deltalake(...)` raises `ValueError` when the incoming schema differs from the table schema, unless you explicitly opt into schema change via `schema_mode`. ([Delta IO][3])

```python
from deltalake import write_deltalake

# default: schema mismatch => ValueError
write_deltalake("path/to/table", data, mode="append")

# allow schema evolution:
write_deltalake("path/to/table", data, mode="append", schema_mode="merge")
write_deltalake("path/to/table", data, mode="overwrite", schema_mode="overwrite")
```

([Delta IO][3])

---

# E2) Schema evolution

Delta separates **explicit schema DDL** from **implicit schema evolution during writes/DML**.

## E2.1 Explicit schema updates (SQL DDL; metadata operations unless noted)

### Add columns (top-level and nested struct fields; position control)

```sql
ALTER TABLE table_name
ADD COLUMNS (col_name data_type [COMMENT col_comment] [FIRST|AFTER colA_name], ...);

ALTER TABLE table_name
ADD COLUMNS (col_name.nested_col_name data_type [COMMENT col_comment] [FIRST|AFTER colA_name], ...);
```

Nested adds are supported only for **struct** fields (not arrays/maps). ([Databricks Documentation][2])

### Reorder columns / change comments (including nested)

```sql
ALTER TABLE table_name
ALTER [COLUMN] col_name (COMMENT col_comment | FIRST | AFTER colA_name);

ALTER TABLE table_name
ALTER [COLUMN] col_name.nested_col_name (COMMENT col_comment | FIRST | AFTER colA_name);
```

([Databricks Documentation][2])

### Replace columns (schema rewrite)

```sql
ALTER TABLE table_name
REPLACE COLUMNS (col_name1 col_type1 [COMMENT col_comment1], ...);
```

([Databricks Documentation][2])

### Rename columns (nested supported)

```sql
ALTER TABLE table_name
RENAME COLUMN old_col_name TO new_col_name;

ALTER TABLE table_name
RENAME COLUMN col_name.old_nested_field TO new_nested_field;
```

To rename **without rewriting existing data**, you must enable **column mapping** (see E3). ([Databricks Documentation][2])

### Drop columns (metadata-only drop requires column mapping)

```sql
ALTER TABLE table_name DROP COLUMN col_name;
ALTER TABLE table_name DROP COLUMNS (col_name_1, col_name_2);
```

Dropping from metadata does **not** delete underlying file data; to purge dropped-column data you must rewrite files (e.g., `REORG TABLE`) then `VACUUM`. ([Databricks Documentation][2])

### “Do it by rewriting the table” (when you don’t have column mapping / need hard changes)

Databricks doc explicitly recommends `overwriteSchema` for rewriting-based changes like rename/type change/drop: ([Databricks Documentation][2])

```python
# type change by rewrite
(spark.read.table("db.t")
  .withColumn("birthDate", col("birthDate").cast("date"))
  .write.mode("overwrite")
  .option("overwriteSchema", "true")
  .saveAsTable("db.t"))
```

([Databricks Documentation][2])

## E2.2 Implicit / automatic schema evolution (write + DML surfaces)

### Enable for writes (add new columns)

Per write / writeStream:

* `.option("mergeSchema", "true")`
* or session default `spark.databricks.delta.schema.autoMerge.enabled=true` (lower precedence) ([Databricks Documentation][2])

### MERGE schema evolution

Databricks doc includes MERGE schema evolution as a first-class mechanism (either `MERGE WITH SCHEMA EVOLUTION` or enabling autoMerge). ([Databricks Documentation][2])

### Explicit limitation

No schema evolution clause exists for `INSERT INTO` statements. ([Databricks Documentation][2])

## E2.3 Concurrency / streaming implications of schema changes

Schema updates conflict with concurrent writes and terminate streams reading the table; you must coordinate schema changes and restart streams. ([Databricks Documentation][2])

---

# E3) Column mapping (logical vs Parquet column identity)

Column mapping is the protocol mechanism that makes **rename/drop “metadata-only”** (no Parquet rewrite) and allows column names with characters Parquet disallows. ([Delta Lake][4])

## E3.1 Enablement (OSS Delta)

Enabling is a **protocol upgrade** and is **irreversible**; for classic OSS syntax you set min reader/writer versions and the mapping mode: ([Delta Lake][4])

```sql
ALTER TABLE <table_name> SET TBLPROPERTIES (
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5',
  'delta.columnMapping.mode' = 'name'
);
```

Once enabled, you cannot turn it off by setting mode back to `none` (it errors). ([Delta Lake][4])

## E3.2 Protocol model: `delta.columnMapping.mode ∈ {none, id, name}`

The protocol assigns **every column (nested or leaf)**:

* a unique physical name: `delta.columnMapping.physicalName`
* a unique 32-bit id: `delta.columnMapping.id`
  and governs resolution via `delta.columnMapping.mode` (`none`, `id`, `name`). Writers also maintain `delta.columnMapping.maxColumnId`. ([GitHub][5])

### Writer requirements (high-signal)

When column mapping is enabled, writers must: ([GitHub][5])

* write Parquet files using **physical names** (stable; display name may change)
* write the column id into Parquet metadata as `field_id`
* track partition values and file stats in the log using **physical names**
* assign globally unique physical names for new columns; monotonically increase `delta.columnMapping.maxColumnId`

### Reader requirements: `id` vs `name` mode

* **none**: resolve by display name from schema (`StructField.name`). ([GitHub][5])
* **id**: resolve by Parquet `field_id` matching `delta.columnMapping.id`; stats/partition values still resolved by *physical name*; if a file lacks field ids, readers must refuse or return nulls. ([GitHub][5])
* **name**: resolve by `delta.columnMapping.physicalName`; stats/partition values resolved by physical name; ids are not used for resolution. ([GitHub][5])

## E3.3 DDL enabled by column mapping

Once enabled, you can do:

```sql
ALTER TABLE <table_name> RENAME COLUMN old_col_name TO new_col_name;

ALTER TABLE table_name DROP COLUMN col_name;
ALTER TABLE table_name DROP COLUMNS (col_name_1, col_name_2, ...);
```

([Delta Lake][4])

## E3.4 Supported characters in column names

With column mapping enabled, Delta column names can include spaces and `,;{}()\n\t=`. ([Delta Lake][4])

## E3.5 Known limitations (not exhaustive)

* Column mapping can break downstream operations relying on change data feed.
* Structured streaming reads are blocked in older Delta versions; in Delta 3.0+ streaming reads require schema tracking after rename/drop. ([Delta Lake][4])

---

# E4) Constraints + generated/identity columns

## E4.1 Column invariants (per-column, enforced on write)

Protocol feature: `invariants` (writer-only in table features). Invariants live in **column metadata** under `delta.invariants` as a JSON string containing a boolean SQL expression at `expression.expression`. Writers must abort any transaction that adds a row where the invariant evaluates to `false` or `null`. ([GitHub][5])

Example schema fragment (protocol):

```json
{
  "name": "x",
  "type": "integer",
  "nullable": true,
  "metadata": {
    "delta.invariants": "{\"expression\": { \"expression\": \"x > 3\"} }"
  }
}
```

([GitHub][5])

## E4.2 CHECK constraints (table-level, enforced on write)

### SQL DDL surface (OSS)

```sql
ALTER TABLE default.people10m
ADD CONSTRAINT dateWithinRange CHECK (birthDate > '1900-01-01');

ALTER TABLE default.people10m
DROP CONSTRAINT dateWithinRange;
```

Adding a constraint verifies existing rows satisfy it. Constraints appear as table properties in `DESCRIBE DETAIL` / `SHOW TBLPROPERTIES`. ([Delta Lake][6])

### Protocol representation

CHECK constraints are stored in table metadata configuration as:

* key: `delta.constraints.{name}`
* value: boolean SQL expression string
  All rows must satisfy it; writers must validate existing data on add, and validate all new rows on write (otherwise the write must fail). ([GitHub][5])

### delta-rs (`deltalake`) API

```python
from deltalake import DeltaTable

dt = DeltaTable("path/to/table")
dt.alter.add_constraint({"id_gt_0": "id > 0"})
```

Violations cause the append/write to error. ([Delta IO][7])

## E4.3 Generated columns (computed/persisted; enforced like a constraint)

### Semantics + enforcement rule

* If you omit a generated column in writes, Delta computes it.
* If you provide an explicit value, it must satisfy `(<value> <=> <generation expression>) IS TRUE` (else write fails). ([Delta Lake][1])
* Expressions must be deterministic; no UDFs, aggregates, window functions, or multi-row functions. ([Delta Lake][1])

### delta-spark Python (DeltaTableBuilder / ColumnBuilder)

```python
from delta.tables import DeltaTable
from pyspark.sql.types import DateType

(DeltaTable.create(spark)
  .tableName("default.people10m")
  .addColumn("birthDate", "TIMESTAMP")
  .addColumn("dateOfBirth", DateType(), generatedAlwaysAs="CAST(birthDate AS DATE)")
  .execute())
```

([Delta Lake][1])

### Protocol representation

Generated columns store the expression in column metadata under `delta.generationExpression`; writer feature is `generatedColumns` when on writer v7. ([GitHub][5])

## E4.4 Identity columns (autoincrement; writer-side feature; concurrency caveat)

### OSS delta-spark Python creation surface

Identity columns are supported in Delta Lake 3.3+ and are declared via builder APIs using `IdentityGenerator`. ([Delta Lake][1])

```python
from delta.tables import DeltaTable, IdentityGenerator
from pyspark.sql.types import LongType

(DeltaTable.create(spark)
  .tableName("table_name")
  .addColumn("id_col1", dataType=LongType(), generatedAlwaysAs=IdentityGenerator())
  .addColumn("id_col2", dataType=LongType(), generatedAlwaysAs=IdentityGenerator(start=-1, step=1))
  .addColumn("id_col3", dataType=LongType(), generatedByDefaultAs=IdentityGenerator())
  .execute())
```

([Delta Lake][1])

**Important operational note (OSS doc):** declaring an identity column disables concurrent transactions; use only when concurrent writes aren’t required. ([Delta Lake][1])

Other mechanics (OSS doc): LongType only; start/step (non-zero); values unique but not guaranteed contiguous; `generated by default` allows explicit inserts; `generated always` forbids. Also supports:

```sql
ALTER TABLE table_name ALTER COLUMN column_name SYNC IDENTITY;
```

to sync metadata with actual data. ([Delta Lake][1])

### Protocol representation (identity metadata keys)

Identity columns are supported on writer v6 or writer v7 with `identityColumns` in `writerFeatures`. Identity properties are stored as column metadata keys like `delta.identity.start` and `delta.identity.allowExplicitInsert` (should not change after creation). ([GitHub][5])

---

# E5) Type system evolution features

## E5.1 Type widening (no Parquet rewrite; protocol feature)

Type widening allows changing column types to a wider type via:

* manual `ALTER TABLE ALTER COLUMN ... TYPE ...`
* automatic widening when schema evolution is enabled and the table allows it ([Delta Lake][8])

### Enable / disable / drop feature

```sql
ALTER TABLE <table_name>
SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');

ALTER TABLE <table_name>
ALTER COLUMN <col_name> TYPE <new_type>;

ALTER TABLE <table_name>
DROP FEATURE 'typeWidening' [TRUNCATE HISTORY];
```

Enabling sets a **reader+writer table feature** (`typeWidening` or `typeWidening-preview` depending on Delta version), so only compliant clients can read/write the table after enablement. ([Delta Lake][8])

### Supported widenings (selected)

Delta’s doc enumerates supported changes, including integer widenings, `float → double`, decimal precision/scale increases, and `date → timestampNTZ`; supported for top-level and nested fields (struct/map/array). ([Delta Lake][8])

### Interaction with schema evolution

Schema evolution can widen target column types only when all conditions hold (auto schema evolution enabled, table has type widening enabled, widening is supported, etc.); integer→decimal/double must be applied manually to avoid accidental promotion. ([Delta Lake][8])

## E5.2 `timestamp_ntz` / `TIMESTAMP_NTZ` (timestamp without timezone)

### Protocol requirement

When stored in Parquet, timestamp-without-timezone must have `isAdjustedToUTC=false`, and the table must support feature `timestampNtz`. ([GitHub][5])

### Databricks SQL / Runtime surface (practical syntax + enablement)

* Data type syntax: `TIMESTAMP_NTZ` (or `TIMESTAMP WITHOUT TIME ZONE` on newer runtimes).
* Creating a new Delta table with a `TIMESTAMP_NTZ` column auto-enables support; adding such a column later does **not**, and you must explicitly enable support with:

```sql
ALTER TABLE table_name
SET TBLPROPERTIES ('delta.feature.timestampNtz' = 'supported');
```

Also defines literal syntax (`TIMESTAMP_NTZ'2020-12-31'`) and casts. ([Databricks Documentation][9])

## E5.3 `VARIANT` (semi-structured; Delta 4.0-era)

### Storage/protocol model

Delta protocol requires a `variantType` feature for readers; readers must tolerate a `variant` logical type and read it from Parquet as a **struct-of-binary** physical representation (`value`, `metadata`) (or expose raw struct/string if engine lacks Variant). ([GitHub][5])

### Databricks SQL / Runtime: core functions and operators

Data type: `VARIANT`. To create values, use `parse_json` or cast. ([Databricks Documentation][10])

**Create / ingest**

```sql
CREATE TABLE t (id BIGINT, raw VARIANT) USING DELTA;

INSERT INTO t VALUES (1, parse_json('{"key":123,"data":[4,5,"str"]}'));
```

`parse_json` is the canonical ingress function. ([Databricks Documentation][10])

**Extract**

* function form:

```sql
SELECT variant_get(raw, '$.key', 'int') AS key FROM t;
```

([Databricks Documentation][11])

* shorthand operators: `:` for top-level, `.` / `[key]` for nested, `[idx]` for arrays; escape special field names with backticks or bracket syntax for periods. ([Databricks Documentation][12])

**Explode / unnest**

```sql
SELECT * FROM variant_explode(raw);
```

([Databricks Documentation][13])

**Inspect schema**

* `schema_of_variant` / `schema_of_variant_agg` exist at the SQL level (Databricks docs), and in PySpark `schema_of_variant` is available (Spark 4.0). ([Databricks Documentation][10])

**Convert back to JSON**

```sql
SELECT to_json(raw) FROM t;
```

([Databricks Documentation][14])

### PySpark surfaces (Spark 4.0+)

* `pyspark.sql.functions.parse_json(col)` → VariantType column (throws on invalid JSON). ([Apache Spark][15])
* `pyspark.sql.functions.schema_of_variant(v)` → schema string for a Variant value. ([Apache Spark][16])

### Notable limitations (Databricks doc)

Variant columns cannot be used as partition/clustering/z-order keys, and Variant values cannot be used for comparisons/grouping/ordering/set ops (see limitations). ([Databricks Documentation][12])

[1]: https://docs.delta.io/delta-batch/ "Table batch reads and writes | Delta Lake"
[2]: https://docs.databricks.com/aws/en/delta/update-schema "Update table schema | Databricks on AWS"
[3]: https://delta-io.github.io/delta-rs/usage/writing/ "Writing Delta Tables - Delta Lake Documentation"
[4]: https://docs.delta.io/delta-column-mapping/ "Delta column mapping | Delta Lake"
[5]: https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md "raw.githubusercontent.com"
[6]: https://docs.delta.io/delta-constraints/ "Constraints | Delta Lake"
[7]: https://delta-io.github.io/delta-rs/usage/constraints/ "Adding a constraint - Delta Lake Documentation"
[8]: https://docs.delta.io/delta-type-widening/ "Delta type widening | Delta Lake"
[9]: https://docs.databricks.com/aws/en/sql/language-manual/data-types/timestamp-ntz-type "TIMESTAMP_NTZ type | Databricks on AWS"
[10]: https://docs.databricks.com/aws/en/sql/language-manual/data-types/variant-type "VARIANT type | Databricks on AWS"
[11]: https://docs.databricks.com/aws/en/sql/language-manual/functions/variant_get?utm_source=chatgpt.com "variant_get function | Databricks on AWS"
[12]: https://docs.databricks.com/aws/en/semi-structured/variant "Query variant data | Databricks on AWS"
[13]: https://docs.databricks.com/aws/en/sql/language-manual/functions/variant_explode?utm_source=chatgpt.com "variant_explode table-valued function | Databricks on AWS"
[14]: https://docs.databricks.com/aws/en/sql/language-manual/functions/to_json?utm_source=chatgpt.com "to_json function | Databricks on AWS"
[15]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.parse_json.html?utm_source=chatgpt.com "pyspark.sql.functions.parse_json - Apache Spark"
[16]: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.schema_of_variant.html?utm_source=chatgpt.com "pyspark.sql.functions.schema_of_variant - Apache Spark"

## F) Row-level change tracking and incremental processing primitives

This section is a **protocol + API reference** for four “incremental primitives”:

* **CDF (Change Data Feed)**: row-level CDC stream between versions/timestamps
* **Deletion Vectors (DVs)**: merge-on-read row deletes/updates without rewriting base Parquet
* **Row Tracking**: stable row identity + last-modified version per row
* **In-Commit Timestamps**: stable commit timestamps stored inside commits (fixes mtime drift)

---

# F1) Change Data Feed (row-level CDC between versions)

## F1.1 Enablement (table property + session default)

### Enable on a table (SQL)

```sql
-- new table
CREATE TABLE student (id INT, name STRING, age INT)
TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- existing table
ALTER TABLE myDeltaTable
SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

([Delta Lake][1])

### Enable by default for all new tables in a session

```sql
SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;
```

([Delta Lake][1])

### Compatibility / capture window notes (hard constraints)

* Only changes **after** enablement are captured. ([Delta Lake][1])
* Once enabled, Delta docs warn you cannot write to the table using **Delta Lake 1.2.1 or below** (reads remain possible). ([Delta Lake][1])

## F1.2 Storage model (protocol + runtime behavior)

### What gets materialized

Protocol defines **Change Data Files** under `_change_data/` (optionally partitioned). Writers may generate them for `UPDATE`/`DELETE`/`MERGE`; if an operation is insert-only or removes files without updating rows, writers can skip change files and record only `add`/`remove` actions. ([GitHub][2])

### When `_change_data` might be absent for a commit

Delta docs: `_change_data` is used for `UPDATE`/`DELETE`/`MERGE`, but Delta may compute CDF directly from the transaction log; **insert-only** and **full partition deletes** won’t generate `_change_data` records. ([Delta Lake][1])

### Retention / vacuum interaction

CDF files follow the table’s retention policy; running `VACUUM` deletes them (and if versions are cleaned up from the log, you can’t read CDF for those versions anymore). ([Delta Lake][1])

## F1.3 Read changes in batch (SQL + PySpark)

### SQL table-valued functions

```sql
-- version range (inclusive)
SELECT * FROM table_changes('tableName', 0, 10);

-- timestamp range (inclusive)
SELECT * FROM table_changes('tableName', '2021-04-21 05:45:46', '2021-05-21 12:00:00');

-- open-ended (start -> latest)
SELECT * FROM table_changes('tableName', 0);

-- escaping dotted identifiers
SELECT * FROM table_changes('dbName.`dotted.tableName`', '2021-04-21 06:45:46', '2021-05-21 12:00:00');

-- path-based table
SELECT * FROM table_changes_by_path('/pathToMyDeltaTable', '2021-04-21 05:45:46');
```

([Delta Lake][1])

### PySpark batch read API (`readChangeFeed`)

```python
# versions (inclusive)
cdf = (
  spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .option("endingVersion", 10)
    .table("myDeltaTable")
)

# timestamps (inclusive), format: yyyy-MM-dd[ HH:mm:ss[.SSS]]
cdf = (
  spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingTimestamp", "2021-04-21 05:45:46")
    .option("endingTimestamp", "2021-05-21 12:00:00")
    .table("myDeltaTable")
)

# open-ended (start -> latest)
cdf = (
  spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("myDeltaTable")
)

# path-based
cdf = (
  spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingTimestamp", "2021-04-21 05:45:46")
    .load("/pathToMyDeltaTable")
)
```

([Delta Lake][1])

### Range semantics + errors

* Start/end are **inclusive**; versions are ints/longs; timestamps are strings in `yyyy-MM-dd[ HH:mm:ss[.SSS]]`. ([Delta Lake][1])
* If you request a version/timestamp older than when CDF was enabled → error (“CDF not enabled”). ([Delta Lake][1])

### Out-of-range tolerance (Databricks)

Databricks Runtime can treat out-of-range timestamps/versions as empty / truncated reads:

```sql
SET spark.databricks.delta.changeDataFeed.timestampOutOfRange.enabled = true;
```

Semantics: start beyond last commit → empty; end beyond last commit → reads through last commit. ([Databricks Documentation][3])

## F1.4 Read changes in streaming (Structured Streaming)

```python
# starting version
cdf_stream = (
  spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("myDeltaTable")
)

# starting timestamp
cdf_stream = (
  spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .option("startingTimestamp", "2021-04-21 05:35:43")
    .load("/pathToMyDeltaTable")
)

# no startingVersion/timestamp => Databricks: initial snapshot returned as INSERTs, then future changes
cdf_stream = (
  spark.readStream.format("delta")
    .option("readChangeFeed", "true")
    .table("myDeltaTable")
)
```

([Delta Lake][1])

### “Archive CDF” pattern (make a permanent audit log)

Databricks explicitly recommends writing CDF into a separate table for permanent history; example uses `availableNow=True` to drain current changes as a batch-like run:

```python
(
  spark.readStream
    .option("readChangeFeed", "true")
    .table("source_table")
    .writeStream
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .toTable("target_table")
)
```

([Databricks Documentation][3])

## F1.5 Schema + metadata columns

CDF output = “table schema (latest)” + metadata columns:

* `_change_type`: `insert`, `update_preimage`, `update_postimage`, `delete`
* `_commit_version`: long table version containing the change
* `_commit_timestamp`: commit timestamp
  ([Databricks Documentation][3])

Protocol-level: change data files themselves contain an additional `_change_type` column. ([GitHub][2])

### Naming collision constraint

You cannot enable CDF if your table already has columns named `_change_type`, `_commit_version`, `_commit_timestamp`. ([Databricks Documentation][3])

## F1.6 Clone / history coupling

CDF is tied to table history; clones have independent history, so CDF on a clone won’t match the source’s CDF. ([Databricks Documentation][3])

## F1.7 delta-rs / `deltalake` Python: `DeltaTable.load_cdf(...)`

### Signature (high-signal parameters)

`DeltaTable.load_cdf(...) -> RecordBatchReader` supports:

* `starting_version`, `ending_version`
* `starting_timestamp`, `ending_timestamp`
* `columns`, `predicate`
* `allow_out_of_range` ([Tessl][4])

### Minimal usage

```python
import polars as pl
from deltalake import DeltaTable

dt = DeltaTable("path/to/cdf-enabled-table")
batches = dt.load_cdf(starting_version=0, ending_version=4).read_all()
df = pl.from_arrow(batches)
```

([Delta IO][5])

---

# F2) Deletion vectors (merge-on-read deletes/updates)

## F2.1 What DVs change (copy-on-write → merge-on-read)

Without DVs, deleting/updating a single row rewrites the whole Parquet file containing it. With DVs enabled, some operations mark rows as removed without rewriting the Parquet file; reads apply the DV to filter invalidated rows. ([Delta Lake][6])

## F2.2 Enablement (SQL) + compatibility blast radius

```sql
ALTER TABLE <table_name>
SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
```

([Delta Lake][6])

Enabling upgrades the table protocol and makes the table unreadable by clients that do not support deletion vectors. ([Delta Lake][6])

## F2.3 Operation availability by Delta version

Delta docs summarize first availability / default enablement: ([Delta Lake][6])

* `SCAN`: 2.3.0 (default since 2.3.0)
* `DELETE`: 2.4.0 (default since 2.4.0)
* `UPDATE`: 3.0.0 (default since 3.1.0)
* `MERGE`: 3.1.0 (default since 3.1.0)

## F2.4 Protocol representation (what shows up in the log)

Protocol requirements:

* Reader Version 3 + Writer Version 7
* `deletionVectors` must be present in `readerFeatures` and `writerFeatures`
* Table property `delta.enableDeletionVectors=true` gates whether writers emit *new* DVs ([GitHub][2])

Log/action-level:

* `add` / `remove` actions may include a DV descriptor; readers must honor DVs **even if** `delta.enableDeletionVectors` is not set (because old DVs may exist in history). ([GitHub][2])

### `DeletionVectorDescriptor` schema (log-level)

Fields: `storageType`, `pathOrInlineDv`, `offset`, `sizeInBytes`, `cardinality`. ([GitHub][2])

* `storageType ∈ {'u','i','p'}`:

  * `'u'`: relative-path DV (file name reconstructed from UUID-like encoding)
  * `'i'`: inline DV stored in log
  * `'p'`: absolute-path DV ([GitHub][2])

Derived fields include `uniqueId` and `absolutePath`; protocol provides JSON examples for all three storage types. ([GitHub][2])

### Writer constraint when emitting DVs

When a logical file is added with a deletion vector, that file must have correct `numRecords` in its `stats` field. ([GitHub][2])

## F2.5 When DV “soft deletes” get applied physically

Databricks: DV-recorded changes get physically applied when files are rewritten by:

* `OPTIMIZE`
* auto-compaction rewriting DV-bearing files
* `REORG TABLE ... APPLY (PURGE)` (explicitly rewrites all files containing DV-recorded modifications; strongest guarantee) ([Databricks Documentation][7])

---

# F3) Row tracking / row lineage (`_metadata.row_id`, `_metadata.row_commit_version`)

Row tracking adds **row identity + last-modified version** that persists across updates/merges (subject to enablement + preservation rules).

## F3.1 Enable (SQL / session default)

### Delta Lake user-facing property

```sql
-- new table / CTAS
CREATE TABLE student (id INT, name STRING, age INT)
TBLPROPERTIES ('delta.enableRowTracking' = 'true');

-- existing table (Delta 3.3+)
ALTER TABLE grade
SET TBLPROPERTIES ('delta.enableRowTracking' = 'true');
```

([Delta Lake][8])

### Default for all new tables in a session (Spark conf)

```sql
SET spark.databricks.delta.properties.defaults.enableRowTracking = true;
```

```python
spark.conf.set("spark.databricks.delta.properties.defaults.enableRowTracking", True)
```

([Delta Lake][8])

### Disable (does not remove feature)

```sql
ALTER TABLE table_name
SET TBLPROPERTIES (delta.enableRowTracking = false);
```

Disabling does not remove the table feature or downgrade protocol; IDs become unreliable for tracking uniqueness. ([Databricks Documentation][9])

## F3.2 Query the metadata fields (Spark)

Row tracking metadata is **not automatically projected**; you must select from hidden `_metadata`:

```sql
SELECT _metadata.row_id, _metadata.row_commit_version, *
FROM table_name;
```

```python
(spark.read.table("table_name")
  .select("_metadata.row_id", "_metadata.row_commit_version", "*"))
```

([Delta Lake][8])

Databricks documents the schema:

* `_metadata.row_id`: Long, unique identifier; stable across `MERGE` / `UPDATE`
* `_metadata.row_commit_version`: Long, last inserted/updated table version ([Databricks Documentation][9])

## F3.3 Storage model (log vs hidden columns)

Delta stores row tracking metadata in hidden metadata columns in data files; some operations (e.g., insert-only) may track row ids / commit versions using Delta log metadata instead; `OPTIMIZE` / `REORG` causes materialization into hidden columns. ([Delta Lake][8])

## F3.4 Clone caveat

Row ids / row commit versions do not match between a cloned table and its source because clones have independent history. ([Delta Lake][8])

## F3.5 Interaction with CDF

Protocol: readers cannot read Row IDs / Row Commit Versions while reading change data files from `cdc` actions (so don’t expect `_metadata.row_id` to be available inside CDF reads). ([GitHub][2])

## F3.6 Protocol-level mechanics (for implementers / inspectors)

Row tracking has two states:

* **supported**: `rowTracking` present in `protocol.writerFeatures` (writers must assign row IDs/commit versions unless suspended, but presence can’t be relied on)
* **enabled**: additionally `delta.enableRowTracking=true` (row IDs/commit versions must be present for all rows; writers preserve stable IDs) ([GitHub][2])

Control property:

* `delta.rowTrackingSuspended=true` tells writers to suspend assignment; should not be enabled together with `delta.enableRowTracking`. ([GitHub][2])

How row IDs are computed/stored:

* **default generated row IDs**: `baseRowId` in `add`/`remove` + row position in file (fresh IDs)
* **materialized row IDs**: hidden column whose name is stored in metadata config key `delta.rowTracking.materializedRowIdColumnName` (stable IDs preserved across rewrite) ([GitHub][2])

Row commit versions:

* default: `defaultRowCommitVersion` in `add`/`remove`
* materialized: hidden column configured by `delta.rowTracking.materializedRowCommitVersionColumnName` ([GitHub][2])

Writers also maintain a row-id high-water mark via a `domainMetadata` action (`delta.rowTracking` domain) with `rowIdHighWaterMark`. ([GitHub][2])

---

# F4) In-commit timestamps (stable commit-time semantics)

## F4.1 Why it exists

Timestamp-based time travel can break after copying/moving a table directory because commit timestamps traditionally depend on `_delta_log/<version>.json` file modification time. ([Delta Lake][10])

In-commit timestamps fix this by writing a monotonically increasing timestamp into the commit itself. ([GitHub][2])

## F4.2 Enablement (protocol contract)

Enablement requirements (protocol):

* Writer Version 7
* `inCommitTimestamp` present in `protocol.writerFeatures`
* `delta.enableInCommitTimestamps=true` ([GitHub][2])

Practical DDL:

```sql
ALTER TABLE table_name
SET TBLPROPERTIES ('delta.enableInCommitTimestamps' = 'true');
```

(Your engine must also upgrade protocol + add the writer feature per spec.) ([GitHub][2])

## F4.3 Writer requirements (what must be written into commits)

When enabled: ([GitHub][2])

1. Every commit must include `commitInfo`
2. `commitInfo` must be the **first** action in the commit
3. `commitInfo` must include `inCommitTimestamp: long` (ms since epoch)
4. `inCommitTimestamp` must be `max(attempt_time, previous_inCommitTimestamp + 1ms)`
5. For mixed-history tables, writers must record enablement provenance in table properties:

   * `delta.inCommitTimestampEnablementVersion`
   * `delta.inCommitTimestampEnablementTimestamp` (must equal the enabling commit’s `inCommitTimestamp`) ([GitHub][2])

## F4.4 Reader guidance (what to use as “commit time”)

Readers should use `inCommitTimestamp` as the commit timestamp for time travel and `DESCRIBE HISTORY`; enablementVersion/enablementTimestamp identify the boundary where old commits lack it. ([GitHub][2])

## F4.5 Operational coupling (Databricks)

Databricks coordinated commits depends on and may manage in-commit timestamp properties internally; Databricks error classes explicitly reference `delta.enableInCommitTimestamps`, `delta.inCommitTimestampEnablementVersion`, `delta.inCommitTimestampEnablementTimestamp`. ([Databricks Documentation][11])

---

If you want the next increment: I can convert **F1–F4 into a “minimum runnable harness”** (PySpark + delta-rs) that:

1. creates a table with CDF + DV + row tracking + in-commit timestamps,
2. performs `INSERT/UPDATE/MERGE/DELETE`,
3. shows: `table_changes(...)`, `readChangeFeed` batch/stream, `_metadata.row_id` queries, and
4. inspects the raw `_delta_log` JSON actions to show where `cdc`/DV descriptors/rowTracking fields/`commitInfo.inCommitTimestamp` actually appear.

[1]: https://docs.delta.io/delta-change-data-feed/ "Change data feed | Delta Lake"
[2]: https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md "raw.githubusercontent.com"
[3]: https://docs.databricks.com/aws/en/delta/delta-change-data-feed "Use Delta Lake change data feed on Databricks | Databricks on AWS"
[4]: https://tessl.io/registry/tessl/pypi-deltalake/1.1.0/files/docs/data-reading.md "tessl/pypi-deltalake@1.1.x - Registry - Tessl"
[5]: https://delta-io.github.io/delta-rs/usage/read-cdf/ "Reading Change Data - Delta Lake Documentation"
[6]: https://docs.delta.io/delta-deletion-vectors/ "What are deletion vectors? | Delta Lake"
[7]: https://docs.databricks.com/aws/en/delta/deletion-vectors "Deletion vectors in Databricks | Databricks on AWS"
[8]: https://docs.delta.io/delta-row-tracking/ "Use row tracking for Delta tables | Delta Lake"
[9]: https://docs.databricks.com/aws/en/delta/row-tracking?utm_source=chatgpt.com "Row tracking in Databricks | Databricks on AWS"
[10]: https://docs.delta.io/delta-batch/ "Table batch reads and writes | Delta Lake"
[11]: https://docs.databricks.com/aws/en/error-messages/error-classes?utm_source=chatgpt.com "Error conditions in Databricks"

## G) Physical layout, indexing-like behavior, and performance tooling

Delta Lake “performance” is mostly: **file-level pruning** (stats + partitions + clustering) + **file rewrite operators** (OPTIMIZE/compaction/ZORDER/clustering) + **write-time file sizing** (optimize write / auto compaction). The knobs below are the ones you actually set/invoke.

---

# G1) Statistics + data skipping (file pruning)

## G1.1 What stats exist + what they’re used for

Delta collects **file-level column-local statistics** (min/max, null counts, total records per file) and uses them at query planning time to **skip files** that cannot satisfy predicates. Databricks explicitly describes the collected stats as: *minimum and maximum values, null counts, and total records per file* used for faster queries. ([Databricks Documentation][1])

**Hard requirement for Z-order / clustering**: columns used in `ZORDER BY` must have stats collected; otherwise Z-order is ineffective / wasteful. ([Databricks Documentation][1])

## G1.2 Configure which columns get stats

### (A) “First N columns” strategy (`delta.dataSkippingNumIndexedCols`)

* Default is **32** (Databricks external tables; Delta OSS also uses 32 by default). ([Databricks Documentation][1])
* Set `-1` to collect stats for **all columns** (Delta OSS doc). ([Delta Lake][2])

```sql
ALTER TABLE table_name
SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = 64);

-- all columns (may increase write overhead)
ALTER TABLE table_name
SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = -1);
```

([Delta Lake][2])

**Important**: updating this property does **not** recompute stats for existing files; it changes future collection + what’s considered usable for skipping. ([Databricks Documentation][1])

### (B) “Named list” strategy (`delta.dataSkippingStatsColumns`)

Databricks supports specifying an explicit list of columns for stats; this **supersedes** `dataSkippingNumIndexedCols`. ([Databricks Documentation][1])

```sql
ALTER TABLE table_name
SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = 'col1, col2, col3');
```

([Databricks Documentation][1])

### Long strings / binary columns (avoid expensive stats)

Databricks notes long strings are truncated during stats collection; exclude long string columns if not used in predicates. Delta OSS explicitly calls stats on long `string`/`binary` expensive and recommends using `delta.dataSkippingNumIndexedCols` and/or column reordering to avoid them. ([Databricks Documentation][1])

## G1.3 Recompute stats after changing the stats-column set (Databricks)

Databricks provides a manual recompute hook (DBR 14.3 LTS+):

```sql
ANALYZE TABLE table_name COMPUTE DELTA STATISTICS;
```

([Databricks Documentation][1])

(Also: Databricks notes “predictive optimization” can automatically run `ANALYZE` and choose stats columns for Unity Catalog managed tables.) ([Databricks Documentation][1])

---

# G2) Partitioning strategies (Hive-style partitions + tradeoffs)

## G2.1 DDL / write syntax

### SQL: define partitions at table creation

```sql
CREATE TABLE t(
  university STRING,
  major      STRING,
  name       STRING
)
USING DELTA
PARTITIONED BY (university, major);
```

([Databricks Documentation][3])

### Spark DataFrameWriter: `partitionBy(...)`

```python
(df.write.format("delta")
  .partitionBy("university", "major")
  .mode("overwrite")
  .save("/mnt/delta/t"))
```

### SQL: partition spec in INSERT (engine-supported SQL feature)

```sql
INSERT INTO student
PARTITION(university = 'TU Kaiserslautern') (major, name)
SELECT major, name FROM freshmen;
```

([Databricks Documentation][3])

## G2.2 The two “partition rules” (practical heuristics)

Delta OSS best practices: ([Delta Lake][4])

* Don’t partition on **high-cardinality** columns (e.g., `userId` with ~1M distinct values).
* Partition only if each partition is expected to be **≥ ~1 GB**.

Databricks guidance aligns and is more aggressive about *not partitioning*:

* Don’t partition tables < **~1 TB** on Databricks. ([Databricks Documentation][5])
* Prefer fewer/larger partitions; Databricks recommends partitions contain at least **~1 GB**. ([Databricks Documentation][5])

## G2.3 Non-obvious protocol warning: don’t “depend” on directory layout

Databricks explicitly cautions that Hive-style partition directories are **not part of the Delta protocol**, and workloads should not rely on the directory structure; always use Delta clients/APIs. ([Databricks Documentation][5])

### Column mapping interaction

If column mapping is enabled, partition directories may use randomized prefixes instead of column names (Databricks note). ([Databricks Documentation][5])

## G2.4 Partitioning ↔ OPTIMIZE / ZORDER interaction

* For partitioned tables, compaction/layout rewrite is performed **within partitions**. ([Databricks Documentation][6])
* `OPTIMIZE ... WHERE ...` on Databricks only supports **partition predicates**. ([Databricks Documentation][7])
* If you’re relying on Databricks ingestion-time clustering (unpartitioned), Databricks recommends `OPTIMIZE ... ZORDER BY (<ingestion-order-col>)` if heavy `UPDATE`/`MERGE` breaks ingestion-order locality. ([Databricks Documentation][5])

---

# G3) OPTIMIZE / compaction (small file problem)

## G3.1 SQL syntax (Databricks Runtime)

```sql
OPTIMIZE table_name [FULL] [WHERE predicate]
  [ZORDER BY (col_name1 [, ...])];
```

Constraints:

* `WHERE` supports only **partition predicates**. ([Databricks Documentation][7])
* On **liquid clustered tables**, Databricks disallows `WHERE` and `ZORDER BY`; use clustering + (optionally) `OPTIMIZE FULL`. ([Databricks Documentation][7])
* `FULL` (DBR 16+) rewrites **all data files** (recluster all records for liquid clustering, or recompress after codec change). ([Databricks Documentation][7])

## G3.2 Python DeltaTable API (Databricks)

Compaction:

```python
from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, "table_name")  # or forPath

metrics = dt.optimize().executeCompaction()
metrics = dt.optimize().where("date='2021-11-18'").executeCompaction()
```

([Databricks Documentation][6])

Z-order:

```python
metrics = dt.optimize().executeZOrderBy("eventType")
metrics = dt.optimize().where("date='2021-11-18'").executeZOrderBy("eventType")
```

(`executeZOrderBy(columns: String*)` and `.where(partitionFilter: String)` are the underlying builder methods.) ([Delta Lake][8])

## G3.3 Output sizing knobs for OPTIMIZE

Databricks SQL: set max output file size for OPTIMIZE:

```sql
SET spark.databricks.delta.optimize.maxFileSize = 1073741824; -- default 1 GiB
```

([Databricks Documentation][7])

Delta OSS also documents a performance knob for the compaction plan:

```sql
SET spark.databricks.delta.optimize.repartition.enabled = true;
```

This switches from `coalesce(1)` to `repartition(1)` for better performance when compacting many small files. ([Delta Lake][8])

## G3.4 “Compaction without data change” (minimize downstream semantics/conflicts)

Delta OSS best practices show using `dataChange=false` on rewrite-based compaction jobs:

```python
path = "..."
numFiles = 16

(spark.read.format("delta").load(path)
  .repartition(numFiles)
  .write
  .option("dataChange", "false")
  .format("delta")
  .mode("overwrite")
  .save(path))
```

And partition-scoped compaction using `replaceWhere`:

```python
partition = "year = '2019'"

(spark.read.format("delta").load(path).where(partition)
  .repartition(16)
  .write.option("dataChange", "false")
  .format("delta")
  .mode("overwrite")
  .option("replaceWhere", partition)
  .save(path))
```

([Delta Lake][4])

---

# G4) Z-Order (multi-column clustering for skipping)

## G4.1 SQL

```sql
OPTIMIZE events
WHERE date >= current_timestamp() - INTERVAL 1 day
ZORDER BY (eventType);
```

([Databricks Documentation][1])

## G4.2 Python (DeltaTable API)

```python
from delta.tables import DeltaTable
dt = DeltaTable.forName(spark, "events")
dt.optimize().executeZOrderBy("eventType")

# multiple columns
dt.optimize().executeZOrderBy("eventType", "customerId")
```

([Delta Lake][9])

## G4.3 Practical constraints / behavior

* You can specify multiple Z-order columns, but effectiveness drops with more columns. ([Databricks Documentation][1])
* Z-ordering is **not idempotent** (it attempts incremental reclustering; repeated runs can still do work). ([Databricks Documentation][1])
* Z-order requires stats on the Z-order columns. ([Databricks Documentation][1])
* Databricks explicitly recommends **liquid clustering instead of ZORDER** for new tables. ([Databricks Documentation][6])

---

# G5) Liquid clustering (runtime-level automated clustering)

Treat this as a **platform-layer** feature (Databricks / Delta OSS “clustered table” feature family), implemented via Delta table features and driven operationally by `OPTIMIZE`.

## G5.1 Enable / change clustering keys

### SQL (create)

```sql
CREATE TABLE table1(col0 INT, col1 STRING) CLUSTER BY (col0);

-- CTAS: CLUSTER BY after table name (not in SELECT)
CREATE TABLE table2 CLUSTER BY (col0)
AS SELECT * FROM table1;
```

([Databricks Documentation][10])

### SQL (alter existing)

```sql
ALTER TABLE table_name CLUSTER BY (col0, col1);
ALTER TABLE table_name CLUSTER BY NONE;   -- disable
```

([Databricks Documentation][10])

### SQL (Databricks-only “AUTO”)

```sql
ALTER TABLE table_name CLUSTER BY AUTO;
```

([Databricks Documentation][11])

## G5.2 Python APIs (Databricks)

### DeltaTable builder (empty table creation)

```python
from delta.tables import DeltaTable

(DeltaTable.create()
  .tableName("table1")
  .addColumn("col0", dataType="INT")
  .addColumn("col1", dataType="STRING")
  .clusterBy("col0")
  .execute())
```

([Databricks Documentation][10])

### DataFrameWriter / DataFrameWriterV2 (table creation)

```python
df = spark.read.table("table1")

df.write.clusterBy("col0").saveAsTable("table2")

# DF writer V2
df.writeTo("table2").using("delta").clusterBy("col0").create()
```

([Databricks Documentation][10])

### Structured Streaming (DBR 16+)

```python
(spark.readStream.table("source_table")
  .writeStream
  .clusterBy("column_name")
  .option("checkpointLocation", checkpointPath)
  .toTable("target_table"))
```

([Databricks Documentation][10])

**Operational constraint:** DataFrame APIs can only set clustering keys at creation time or overwrite; you can’t change clustering keys during append-mode writes—use `ALTER TABLE ... CLUSTER BY ...` for that. ([Databricks Documentation][10])

## G5.3 Trigger clustering + recluster semantics

### Incremental clustering trigger

Run `OPTIMIZE` to incrementally cluster data:

```sql
OPTIMIZE table_name;
```

([Delta Lake][12])

### Force recluster (rewrite all data)

* Delta OSS: `OPTIMIZE table_name FULL;` (Delta 3.3+) ([Delta Lake][12])
* Databricks: `OPTIMIZE table_name FULL;` (DBR 16+) ([Databricks Documentation][7])

## G5.4 Compatibility + limits

* Not compatible with **partitioning** or `ZORDER`. ([Databricks Documentation][10])
* Column constraints:

  * Delta OSS notes: up to **4 clustering columns**, and clustering columns must have stats collected (first 32 columns by default). ([Delta Lake][12])

### Protocol/version note (expect variance across docs/runtimes)

Delta OSS doc states clustered tables use Delta writer v7 and reader v1. Databricks doc warns their liquid-clustered Delta tables use writer v7 and reader v3. Treat this as: **verify your client compatibility against the table’s protocol/features** in your environment. ([Delta Lake][12])

---

# G6) Auto optimization knobs (auto compaction + optimized writes + file size controls)

## G6.1 Auto compaction (post-write synchronous compaction)

Databricks / Delta OSS both describe auto compaction as: runs after a successful write, compacts small files, and is triggered only once per file set. Delta OSS lists it as available in Delta Lake 3.1+. ([Delta Lake][8])

### Enable (table vs session)

```sql
ALTER TABLE table_name
SET TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'auto');  -- or true/false
```

([Databricks Documentation][13])

Session:

```sql
SET spark.databricks.delta.autoCompact.enabled = auto;  -- or true/false/legacy
```

([Databricks Documentation][13])

### Size + trigger thresholds

```sql
SET spark.databricks.delta.autoCompact.maxFileSize = 134217728;  -- bytes (example)
SET spark.databricks.delta.autoCompact.minNumFiles = 10;
```

([Databricks Documentation][13])

### Options (Databricks)

* `auto` (recommended; autotunes target file size; DBR 10.4+)
* `legacy` (alias for true; DBR 10.4+)
* `true` (fixed 128MB target)
* `false` (off) ([Databricks Documentation][13])

## G6.2 Optimized writes (write-time bin-packing / shuffle sizing)

Delta OSS: optimized writes are disabled by default; enable at table/session/DataFrameWriter option with defined precedence and advanced tuning knobs. ([Delta Lake][8])

### Enable

Table property:

```sql
ALTER TABLE table_name
SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');
```

([Delta Lake][8])

Session:

```sql
SET spark.databricks.delta.optimizeWrite.enabled = true;
```

([Databricks Documentation][13])

DataFrameWriter option (Delta OSS doc explicitly lists it):

```python
(df.write.format("delta")
  .option("optimizeWrite", "true")
  .mode("append")
  .save("/mnt/delta/t"))
```

([Delta Lake][8])

### Advanced optimized-write tuning (Delta OSS)

```sql
SET spark.databricks.delta.optimizeWrite.binSize = 536870912;          -- 512MiB
SET spark.databricks.delta.optimizeWrite.numShuffleBlocks = 50000000;
SET spark.databricks.delta.optimizeWrite.maxShufflePartitions = 2000;
```

([Delta Lake][8])

### Databricks runtime notes

Databricks documents that optimized writes are enabled by default for certain operations in newer runtimes (for example, `MERGE`, `UPDATE`/`DELETE` with subqueries), and that it can reduce the need for manual `coalesce(n)`/`repartition(n)` before writes. ([Databricks Documentation][13])

## G6.3 Target file size controls (table properties)

Databricks exposes table property `delta.targetFileSize` (bytes or units) and suggests it impacts layout rewrite ops (OPTIMIZE, Z-order, auto compaction, optimized writes), with caveats around UC managed tables and which operations respect it. ([Databricks Documentation][13])

```sql
ALTER TABLE table_name
SET TBLPROPERTIES ('delta.targetFileSize' = '100mb');
```

([Databricks Documentation][13])

Databricks also provides workload-aware tuning:

```sql
ALTER TABLE table_name
SET TBLPROPERTIES ('delta.tuneFileSizesForRewrites' = 'true');
```

And notes it may auto-enable when many recent operations are `MERGE` unless you set it explicitly to `false`. ([Databricks Documentation][13])

## G6.4 “Predictive optimization” (Databricks managed tables; platform layer)

Databricks notes predictive optimization can:

* run `OPTIMIZE` automatically for Unity Catalog managed tables ([Databricks Documentation][6])
* run `ANALYZE` to collect skipping stats and remove the 32-column limit for managed tables ([Databricks Documentation][1])

(If you’re writing a cross-engine Delta deep dive, treat this as product/platform layer, not protocol.)

---

If you want the next increment after G: I can produce a **“perf control plane matrix”** (OSS Delta vs Databricks vs delta-rs) mapping each knob to (1) table property, (2) Spark conf, (3) per-write option, (4) supported runtime/version gates, and (5) observable effects (file counts, stats columns, clustering state) — in the same “API surface + power knobs + failure modes” style you’ve been using.

[1]: https://docs.databricks.com/aws/en/delta/data-skipping "Data skipping | Databricks on AWS"
[2]: https://docs.delta.io/table-properties/ "Delta Table Properties Reference | Delta Lake"
[3]: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-partition "Partitions | Databricks on AWS"
[4]: https://docs.delta.io/best-practices/ "Best practices | Delta Lake"
[5]: https://docs.databricks.com/aws/en/tables/partitions "When to partition tables on Databricks | Databricks on AWS"
[6]: https://docs.databricks.com/aws/en/delta/optimize "Optimize data file layout | Databricks on AWS"
[7]: https://docs.databricks.com/aws/en/sql/language-manual/delta-optimize "OPTIMIZE | Databricks on AWS"
[8]: https://docs.delta.io/optimizations-oss/ "Optimizations | Delta Lake"
[9]: https://docs.delta.io/api/latest/scala/spark/io/delta/tables/deltaoptimizebuilder "Delta Spark 4.0.0 - Scala API Docs  - io.delta.tables.DeltaOptimizeBuilder"
[10]: https://docs.databricks.com/aws/en/delta/clustering "Use liquid clustering for tables | Databricks on AWS"
[11]: https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-cluster-by "CLUSTER BY clause (TABLE) | Databricks on AWS"
[12]: https://docs.delta.io/delta-clustering/ "Use liquid clustering for Delta tables | Delta Lake"
[13]: https://docs.databricks.com/aws/en/delta/tune-file-size "Control data file size | Databricks on AWS"

## H) Maintenance, lifecycle management, and safety mechanisms

This section is a **command/config reference** for: (1) physical garbage collection of data files, (2) scale accelerators for GC, (3) `_delta_log` lifecycle (checkpoints + metadata cleanup), and (4) protocol-safety hardening around maintenance ops.

---

# H1) `VACUUM` + retention safety checks

## H1.1 What `VACUUM` deletes (and what it never deletes)

### Delta OSS / protocol-level intent

* Delta retains old data files for MVCC; files no longer in the latest snapshot are eligible for deletion by `VACUUM` after a retention window (default 7 days). ([GitHub][1])

### Databricks semantics (practical)

* `VACUUM` removes:

  * *data files* no longer referenced in the current transaction log state and older than the retention threshold, **and**
  * **files not managed by Delta** in the table directory (depends on mode), while skipping directories beginning with `_` (including `_delta_log`). ([Databricks Documentation][2])
* Deletion eligibility is based on **time since the file was logically removed from the transaction log** + retention window, **not** the storage system mtime. ([Databricks Documentation][2])

### Log files are not vacuumed

Delta explicitly: `vacuum` deletes only data files; log files are deleted asynchronously after checkpoint operations and governed by log retention, not `VACUUM`. ([Delta Lake][3])

---

## H1.2 SQL syntax (Delta OSS + Databricks)

### Delta OSS utility surface (Spark SQL)

```sql
VACUUM eventsTable;                        -- FULL semantics (see below)
VACUUM eventsTable RETAIN 100 HOURS;       -- override retention for this run

VACUUM eventsTable DRY RUN;                -- list files that would be deleted

VACUUM eventsTable LITE;                   -- Delta 3.3+ (log-driven)

VACUUM delta.`/data/events/`;              -- path-based
VACUUM '/data/events';                     -- path-based (implementation-dependent)

VACUUM eventsTable USING INVENTORY inventoryTable;
VACUUM eventsTable USING INVENTORY (SELECT * FROM inventoryTable);
```

([Delta Lake][3])

### Databricks SQL / DBR 16.1+ explicit mode selector

```sql
VACUUM table_name { { FULL | LITE } | DRY RUN } [...];
```

* `DRY RUN`: returns up to 1000 candidate files. ([Databricks Documentation][2])
* `FULL`: deletes data files outside retention + all non-referenced files in the table directory. ([Databricks Documentation][2])
* `LITE`: uses the Delta log (no directory listing); fails if log pruning prevents completion (see H1.4). ([Databricks Documentation][2])

---

## H1.3 Python API (delta-spark)

```python
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, "/data/events")   # or DeltaTable.forName(...)
dt.vacuum()          # default retention
dt.vacuum(100)       # retention hours for this vacuum
```

(Shown in Delta OSS utility docs.) ([Delta Lake][3])

---

## H1.4 Retention controls (table property) vs safety check (session property)

### Retention window that bounds time travel

* Databricks: the vacuum retention window is controlled by `delta.deletedFileRetentionDuration` (default 7 days), and vacuum beyond that destroys time travel older than retained data files. ([Databricks Documentation][2])

Set on table:

```sql
ALTER TABLE table_name
SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '30 days');
```

([Databricks Documentation][2])

### Safety guardrail (blocks “dangerous vacuum”)

Both Delta OSS docs and Databricks docs describe the same safety check: you can disable it only if you are sure no concurrent ops exceed the retention interval.

```sql
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
```

([Databricks Documentation][2])

#### The “RETAIN 0 HOURS” footgun

Databricks explicitly documents that vacuuming with too-small retention can delete files still required by concurrent readers/writers and can corrupt tables. ([Databricks Documentation][2])
Concrete failure mode: with the safety check disabled and `RETAIN 0 HOURS`, vacuum can delete committed files, uncommitted files, and temporary files for concurrent transactions. ([Databricks Knowledge Base][4])

---

## H1.5 FULL vs LITE vacuum: semantics and failure modes

### FULL (directory-listing driven)

* Enumerates table directory (minus underscore dirs) and deletes:

  * non-managed files, plus
  * eligible unreferenced Delta data files. ([Databricks Documentation][2])

### LITE (log-driven)

* Uses the Delta transaction log to identify eligible unreferenced files; no full directory listing. ([Delta Lake][3])
* **If the Delta log has been pruned**, LITE cannot complete and raises `DELTA_CANNOT_VACUUM_LITE`. ([Delta Lake][3])
* LITE does **not** delete files not referenced in the transaction log (e.g., aborted-transaction debris); those require FULL. ([Azure Docs][5])

---

## H1.6 Sparkless maintenance (delta-rs / `deltalake` Python)

### `DeltaTable.vacuum(...)` signature (important knobs)

```python
from deltalake import DeltaTable
dt = DeltaTable("s3://bucket/table", storage_options={...})

dt.vacuum(
    retention_hours=None,               # else uses delta.deletedFileRetentionDuration (or 1 week)
    dry_run=True,                       # list only
    enforce_retention_duration=True,    # if False, allows retention < table retention
    full=False,                         # if True, “full” vacuum: remove all files not referenced in log
    keep_versions=None,                 # optional: protect specific versions from deletion
)
```

([Delta IO][6])

**Notes:**

* `retention_hours=None` pulls from `delta.deletedFileRetentionDuration` (or default 1 week). ([Delta IO][6])
* `full=True` approximates “directory sweep” behavior for orphan files vs log-referenced-only deletes. ([Delta IO][6])

---

# H2) Vacuum inventory / “vacuum lite” accelerations

## H2.1 Inventory-based vacuum (Delta 3.2+ family): SQL surface

Delta exposes an **inventory reservoir** to avoid expensive recursive file listing. The inventory is used “instead of file listings,” making vacuum effectively:

1. identify unreferenced files by comparing inventory ↔ delta log, then
2. delete those files. ([Delta Lake][7])

### Required inventory schema

Inventory must have (names + types): ([Delta Lake][7])

* `path`: `StringType` (fully qualified URI)
* `length`: `LongType` (bytes)
* `isDir`: `BooleanType`
* `modificationTime`: `LongType` (ms since epoch)

### Syntax (table reservoir)

```sql
VACUUM my_schema.my_table
USING INVENTORY delta.`s3://my-bucket/path/to/inventory_delta_table`
RETAIN 24 HOURS;
```

([Delta Lake][7])

### Syntax (query reservoir)

```sql
VACUUM my_schema.my_table
USING INVENTORY (
  SELECT
    's3://'||bucket||'/'||key AS path,
    length,
    isDir,
    modificationTime
  FROM inventory.datalake_report
  WHERE bucket = 'my-datalake'
    AND table  = 'my_schema.my_table'
)
RETAIN 24 HOURS;
```

([Delta Lake][7])

## H2.2 LITE vs USING INVENTORY

* **LITE**: log-driven; still depends on log completeness; fails if log pruned. ([Delta Lake][3])
* **USING INVENTORY**: replaces directory listing with your provided inventory reservoir; still constrained by table history/log retention for correctness, but avoids object-store list storms. ([Delta Lake][7])

---

# H3) Log cleanup and checkpoint management

## H3.1 Key table properties + what they govern

### `delta.logRetentionDuration` (log/history retention)

* Governs how long `_delta_log` history is retained; when a checkpoint is written, Delta can delete log entries older than retention. ([Delta Lake][8])

### `delta.deletedFileRetentionDuration` (data-file retention / vacuum window)

* Minimum duration to keep logically deleted data files before physical deletion (prevents stale-reader failures). ([Delta Lake][8])

---

## H3.2 Checkpoints: schema knobs you must know (protocol-level)

Checkpoint content is schema-driven and controlled by table properties:

* `delta.checkpoint.writeStatsAsJson` (default true): write file stats as JSON string column `stats`. ([GitHub][1])
* `delta.checkpoint.writeStatsAsStruct` (default false): write typed stats in `stats_parsed`; also gates `partitionValues_parsed` presence when partitioned. ([GitHub][1])

Protocol constraints:

* Checkpoints **must not** preserve `commitInfo` provenance nor change data (`cdc`) actions; remove actions in checkpoints are tombstones for vacuum and omit `stats`/`tags`. ([GitHub][1])

---

## H3.3 V2 checkpoints (feature-gated) and sidecars

### Enablement constraints (protocol)

To support V2 checkpoints:

* Reader Version **3** and Writer Version **7**
* `v2Checkpoint` must exist in both `readerFeatures` and `writerFeatures`. ([GitHub][1])

### Behavior constraints

* When v2 checkpoints are enabled:

  * multi-part checkpoints are **forbidden**
  * writers may create v2 spec checkpoints (classic or UUID-named). ([GitHub][1])
* V2 spec enables placing `add`/`remove` actions in **sidecar parquet files**. ([GitHub][1])
* Backward compatibility: when UUID-named v2 checkpoints are used, writers should periodically emit a **classic** v2 checkpoint so older clients can detect protocol and fail gracefully. ([GitHub][1])

---

## H3.4 Metadata cleanup (log cleanup algorithm) — protocol reference

Delta protocol defines a recommended log cleanup procedure:

1. pick a `cutOffTimestamp` and identify `cutoffCommit` (newest commit ≤ cutoff)
2. identify `cutOffCheckpoint` (newest checkpoint ≤ cutoffCommit); preserve checkpoint **and** the JSON commit at that version (because checkpoints do not preserve `commitInfo`, which may be required by features like in-commit timestamps)
3. delete delta log entries + checkpoints before cutOffCheckpoint; delete compaction files with startVersion ≤ cutOffCheckpoint
4. enumerate checkpoints and their referenced sidecars; protect those sidecars
5. in `_delta_log/_sidecars`, preserve sidecars < 1 day old and referenced sidecars; delete everything else ([GitHub][1])

This is the “why” behind the common statement that log retention is enforced *after checkpoints exist*. ([GitHub][1])

---

## H3.5 delta-rs maintenance entrypoints (Sparkless)

### Create a checkpoint

```python
dt.create_checkpoint()
```

([Delta IO][6])

### Cleanup expired log files (honors `delta.logRetentionDuration`)

```python
dt.cleanup_metadata()
```

Described as deleting expired log files before current version; based on `delta.logRetentionDuration` (30 days default). ([Delta IO][6])

---

## H3.6 Checkpoint protection feature (lifecycle tied to protocol downgrades)

When you `DROP FEATURE` (Delta Lake 4+), Delta performs atomic maintenance steps including:

* rewriting data/metadata to remove traces of the dropped feature in the current version,
* creating **protected checkpoints** to allow correct interpretation after protocol downgrade,
* adding the `checkpointProtection` writer feature, and
* downgrading protocol to the lowest versions supporting remaining features. ([Delta Lake][9])

Maintenance semantics under `checkpointProtection`:

* maintenance ops must respect protected checkpoints (protocol downgrade checkpoints marked protected). ([Delta Lake][9])
* to fully remove `checkpointProtection`, you must `TRUNCATE HISTORY` (which removes history > 24 hours). ([Delta Lake][9])

---

# H4) Protocol safety during maintenance: `vacuumProtocolCheck`

## H4.1 Why it exists

Historically, “vacuum implementations” could perform only reader protocol checks; that is unsafe because VACUUM is a **destructive** operation and needs to respect writer-feature constraints. The `vacuumProtocolCheck` ReaderWriter feature forces consistent reader+writer protocol checks during VACUUM to mitigate corruption risk. ([GitHub][1])

## H4.2 Enablement (protocol constraints)

To enable vacuum protocol check:

* table must be Reader Version **3** and Writer Version **7**
* `vacuumProtocolCheck` must be present in both `readerFeatures` and `writerFeatures` in the `protocol` action. ([GitHub][1])

**Implication:** old VACUUM implementations that lack writer protocol checks will fail on tables with this feature. ([GitHub][1])

## H4.3 Writer/reader requirements (implementation guidance)

* A VACUUM-capable writer must unconditionally check the table’s write protocol before vacuum; easiest implementation is “always do writer protocol check for vacuum” rather than per-table property inference. ([GitHub][1])
* Readers don’t need to change behavior beyond acknowledging the feature exists. ([GitHub][1])

## H4.4 Drop support (maintenance interaction)

`vacuumProtocolCheck` is explicitly listed among droppable table features; dropping it triggers the protected-checkpoint + checkpointProtection downgrade flow. ([Delta Lake][9])

---

If you want the next increment after H: I can produce a **maintenance “control plane” matrix** (Delta OSS vs Databricks vs delta-rs) that maps each operation to: (1) SQL syntax, (2) delta-spark API, (3) delta-rs API, (4) required protocol/table features, and (5) failure modes + mitigation patterns (especially around LITE/inventory/log pruning and safety-check envelopes).

[1]: https://raw.githubusercontent.com/delta-io/delta/master/PROTOCOL.md "raw.githubusercontent.com"
[2]: https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum "VACUUM | Databricks on AWS"
[3]: https://docs.delta.io/delta-utility/ "Table utility commands | Delta Lake"
[4]: https://kb.databricks.com/delta/data-missing-vacuum-parallel-write?utm_source=chatgpt.com "Vaccuming with zero retention results in data loss"
[5]: https://docs.azure.cn/en-us/databricks/delta/vacuum "Remove unused data files with vacuum - Azure Databricks | Azure Docs"
[6]: https://delta-io.github.io/delta-rs/api/delta_table/ "DeltaTable - Delta Lake Documentation"
[7]: https://delta.io/blog/efficient-delta-vacuum/ "Efficient Delta Vacuum with File Inventory | Delta Lake"
[8]: https://docs.delta.io/table-properties/ "Delta Table Properties Reference | Delta Lake"
[9]: https://docs.delta.io/delta-drop-feature/ "Drop Delta table features | Delta Lake"


## I) Interoperability and integration surfaces

Delta interop is “table-format interop,” not “engine interop.” Every connector’s *real* contract is: **which Delta protocol/table-features it can read/write** + how it discovers metadata (HMS/Glue/Unity Catalog/path) + what storage backends it can access.

---

# I1) Primary “native” clients

## I1.1 Spark / `delta-spark` (canonical high-level API + richest feature coverage)

### Install + SparkSession wiring (Python)

Delta Lake’s official quick start for Python is: install `delta-spark` and configure a SparkSession with Delta extensions/catalog, typically via `configure_spark_with_delta_pip()`. ([Delta Lake][1])

```python
import pyspark
from delta.pip_utils import configure_spark_with_delta_pip

builder = (
    pyspark.sql.SparkSession.builder
      .appName("MyApp")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
```

([Delta Lake][1])

### Core object model (Python)

`delta.tables.DeltaTable` is the primary mutation/utility surface:

* constructors:

  * `DeltaTable.forPath(spark, path)`
  * `DeltaTable.forName(spark, "db.table")`
* DML:

  * `update(condition=..., set={...})`
  * `delete(condition=...)`
  * `merge(source_df, condition)` → `DeltaMergeBuilder`
* lifecycle/ops:

  * `vacuum(retentionHours=None)`
  * `history(limit=None)`
  * `restoreToVersion(version)` / `restoreToTimestamp(timestamp)`
  * `optimize()` → `DeltaOptimizeBuilder` ([Delta Lake][2])

Minimal “call shapes” (exact signatures & examples are in the API docs): ([Delta Lake][2])

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit

dt = DeltaTable.forPath(spark, "/tmp/delta/t")

dt.update(condition="eventType = 'clck'", set={"eventType": "'click'"})
dt.update(condition=col("eventType") == "clck", set={"eventType": lit("click")})
dt.delete("date < '2017-01-01'")

(dt.merge(source_df, "t.id = s.id")
   .whenMatchedUpdateAll()
   .whenNotMatchedInsertAll()
   .execute())

dt.vacuum(100)
dt.history(10)
dt.restoreToVersion(1)
dt.optimize().where("date='2021-11-18'").executeCompaction()
```

([Delta Lake][2])

### Why Spark is “the native baseline”

Spark + Delta implements essentially the full Delta OSS surface (DML, CDF, DV, clustering/optimize tooling, table feature management, etc.). The other clients typically:

* implement a subset, or
* implement read-only + selected write paths, or
* offload protocol semantics to **Delta Kernel**.

---

## I1.2 delta-rs / `deltalake` (Rust core + Python bindings; Sparkless)

### Core API objects

* `deltalake.DeltaTable(table_uri, version=None, storage_options=None, without_files=False, log_buffer_size=None)` ([Delta][3])
* Top-level write helper: `write_deltalake(table_uri, data, mode=..., schema_mode=..., partition_by=..., predicate=...)` ([Delta][4])

Key “interop-focused” knobs:

* `storage_options`: backend-specific (S3/ADLS/GCS/local) ([Delta][3])
* `log_buffer_size`: controls concurrent log-file fetches (reduces latency for long logs; increases memory/backing-store pressure) ([Delta][3])
* `without_files=True`: load metadata without tracking file state (useful for append-only patterns where you don’t need the live file set). ([Delta][3])

### “Protocol-level introspection” surfaces (useful for building *your own* connector behaviors)

* `get_add_actions(flatten=False)` → low-level live-file table parsed from `_delta_log` (paths, partition values, and stats) ([Delta][4])
* `cleanup_metadata()` → log retention cleanup (honors `delta.logRetentionDuration`) ([Delta][3])
* `vacuum(...)` and `optimize.compact()` / `optimize.z_order()` exist as first-class ops (so delta-rs can act as the “maintenance engine” even when query engines are elsewhere). ([Delta][5])

### “Interop shape”

delta-rs is typically used as:

* a Sparkless writer/maintainer for Delta tables (especially in Python/Arrow-heavy stacks), or
* the Delta protocol layer under another engine (directly or indirectly).

---

## I1.3 Delta Kernel (Java + Rust) (connector substrate; protocol compliance core)

### What it is

Delta Kernel is explicitly positioned as libraries (Java + Rust) for building connectors that can read/write Delta tables **without implementing the Delta protocol** yourself. It supports single-thread/single-process reads, multi-thread reads, and insert/append paths, and it splits “Table APIs” from “Engine APIs.” ([Delta Lake][6])

### Java dependency coordinates (connector builds)

Kernel docs specify:

* `io.delta:delta-kernel-api`
* `io.delta:delta-kernel-defaults` (optional if you provide your own Engine) ([Delta Lake][6])

### Engine abstraction (where you plug your runtime)

Kernel uses an `Engine` interface for heavy-lift ops (Parquet/JSON IO, expression evaluation, filesystem listing, etc.), and provides `DefaultEngine` in `delta-kernel-defaults`. ([Delta Lake][6])

### Minimal “scan” skeleton (Java; connector authorship baseline)

Kernel docs show the canonical shape:

* `Engine myEngine = DefaultEngine.create(...)`
* `Table.forPath(...)`
* `Snapshot mySnapshot = myTable.getLatestSnapshot(...)`
* `Scan myScan = mySnapshot.getScanBuilder(...).withFilters(...).build()` ([Delta Lake][6])

This is the key point: **Kernel is the on-disk protocol interpreter; your engine implements the execution primitives.** ([Delta Lake][6])

---

# I2) External query engines & connectors

## I2.1 Trino Delta Lake connector (read/write varies by storage + config)

### Catalog configuration (metastore + filesystem are mandatory)

Trino config is via `etc/catalog/<name>.properties`:

```properties
connector.name=delta_lake
hive.metastore.uri=thrift://example.net:9083
fs.x.enabled=true
```

If using AWS Glue as metastore:

```properties
connector.name=delta_lake
hive.metastore=glue
```

Trino requires a metastore (HMS/Glue) and supported filesystem access (S3/Azure/GCS/HDFS). ([Trino][7])

### Supported Delta table features (hard interop boundary)

Trino explicitly lists the Delta table features it supports (and whether they’re reader/writer features). If a table enables a feature outside this list, interop breaks. ([Trino][7])

Supported features include (as listed by Trino): append-only, column invariants, CHECK constraints, CDF, column mapping, deletion vectors, Iceberg compatibility V1/V2 (readers), timestamp without timezone, type widening (readers), vacuum protocol check, v2 checkpoint (readers). ([Trino][7])

### Time travel syntax (Trino SQL)

```sql
SELECT * FROM catalog.schema.table FOR VERSION AS OF 3;

SELECT * FROM catalog.schema.table
FOR TIMESTAMP AS OF TIMESTAMP '2022-03-23 09:59:29.803 America/Los_Angeles';
```

([Trino][7])

### “Register an existing Delta table” (procedure; disabled by default)

Enable `delta.register-table-procedure.enabled=true`, then:

```sql
CALL catalog.system.register_table(
  schema_name => 'testdb',
  table_name => 'customer_orders',
  table_location => 's3://my-bucket/a/path'
);
```

([Trino][7])

Also supports:

```sql
CALL catalog.system.unregister_table(schema_name => 'testdb', table_name => 'customer_orders');
```

([Trino][7])

### Connector-side VACUUM (procedure)

Trino exposes vacuum as a procedure and includes a min-retention safety config:

```sql
CALL catalog.system.vacuum('schema', 'table', '7d');
```

`delta.vacuum.min-retention` acts as a safety floor; session property exists as well. ([Trino][7])

### Table properties (Trino-managed Delta table creation)

Trino supports table properties like:

* `location`
* `partitioned_by`
* `checkpoint_interval`
* `change_data_feed_enabled`
* `column_mapping_mode` (`ID|NAME|NONE`)
* `deletion_vectors_enabled` ([Trino][7])

Example:

```sql
CREATE TABLE catalog.default.example_partitioned_table
WITH (
  location = 's3://my-bucket/a/path',
  partitioned_by = ARRAY['regionkey'],
  checkpoint_interval = 5,
  change_data_feed_enabled = false,
  column_mapping_mode = 'name',
  deletion_vectors_enabled = false
)
AS SELECT ...;
```

([Trino][7])

### Type mapping boundary

Trino documents explicit Delta↔Trino type mappings (notably `VARIANT` mapped to `JSON`, and `TIMESTAMPNTZ` handling). ([Trino][7])

---

## I2.2 Starburst Delta Lake connector (Trino-derived, enterprise packaging)

Starburst Enterprise documents the Delta Lake connector as able to read Delta transaction logs, detect external changes, and notes it builds on Trino connector functionality with enterprise features; catalog config pattern is the same (`connector.name=delta_lake`, metastore + filesystem). ([Starburst][8])

---

## I2.3 DuckDB `delta` extension (Delta Kernel-based; read support; experimental)

### Core claims (interop boundary)

DuckDB docs state:

* the `delta` extension is built using **Delta Kernel**
* it offers **read support** for Delta tables (local + remote)
* it is **experimental** and platform-limited ([DuckDB][9])

### Install/load + scan syntax

```sql
INSTALL delta;
LOAD delta;

SELECT * FROM delta_scan('file:///some/path/on/local/machine');
```

([DuckDB][9])

### Remote object storage reads (S3/Azure) + auth via Secrets

S3:

```sql
SELECT * FROM delta_scan('s3://some/delta/table');

CREATE SECRET (
  TYPE s3,
  PROVIDER credential_chain
);
SELECT * FROM delta_scan('s3://some/delta/table/with/auth');
```

Public bucket region override:

```sql
CREATE SECRET (TYPE s3, REGION 'my-region');
SELECT * FROM delta_scan('s3://some/public/table/in/my-region');
```

Azure:

```sql
SELECT * FROM delta_scan('az://my-container/my-table');

CREATE SECRET (TYPE azure, PROVIDER credential_chain);
SELECT * FROM delta_scan('az://my-container/my-table-with-auth');
```

([DuckDB][9])

### Supported scan-side features (as documented)

DuckDB lists scanning-side features including:

* multithreaded scans + Parquet metadata reading
* data skipping / filter pushdown (row-group skip via Parquet metadata; file skip via Delta partition info)
* projection pushdown
* scanning tables with deletion vectors
* primitive types + structs
* S3 support with secrets ([DuckDB][9])

Version/platform constraints:

* requires DuckDB **0.10.3+**
* limited platform set (Linux/macOS/Windows amd64 + some arm64) ([DuckDB][9])

### Unity Catalog note

DuckDB docs mention an experimental `unity_catalog` extension for connecting to Unity Catalog, explicitly “proof-of-concept.” ([DuckDB][9])

---

## I2.4 “Integrations directory” (ecosystem map)

Delta maintains an integrations listing (high-signal for “who reads/writes Delta”) including:

* Spark (read/write)
* ClickHouse (read-only in S3, per listing)
* Dagster Delta Lake IO manager (read/write from orchestration) ([Delta Lake][10])

The “Other connectors” page enumerates additional integration surfaces with brief capability notes, e.g.:

* Apache Druid connector (read)
* Apache Pulsar connector (read/write)
* Hive integration (read)
* Kafka Delta ingest daemon (stream into Delta)
* StarRocks ability to read Delta ([Delta Lake][11])

Use this as your “capability discovery” list, but treat each as requiring a protocol/table-feature compatibility check.

---

# I3) UniForm (Delta tables readable as Iceberg/Hudi)

UniForm is explicitly an **interop layer**: it generates Iceberg/Hudi metadata asynchronously so Iceberg/Hudi clients can read the same Parquet data files “as if” they were Iceberg/Hudi tables. Delta docs emphasize negligible Delta write overhead because the conversion happens asynchronously after the Delta commit. ([Delta Lake][12])

## I3.1 Requirements (hard gates)

Uniform Iceberg requirements include:

* **column mapping enabled**
* `minReaderVersion >= 2` and `minWriterVersion >= 7`
* writes use Delta Lake **3.1+**
* Hive Metastore configured as the catalog (for Iceberg catalog mapping) ([Delta Lake][12])

Uniform Hudi (preview):

* writes use Delta Lake **3.2+** ([Delta Lake][12])

## I3.2 Enablement (table properties)

Iceberg:

```sql
'delta.enableIcebergCompatV2' = 'true'
'delta.universalFormat.enabledFormats' = 'iceberg'
```

Hudi:

```sql
'delta.universalFormat.enabledFormats' = 'hudi'
```

Both:

```sql
'delta.enableIcebergCompatV2' = 'true'
'delta.universalFormat.enabledFormats' = 'iceberg,hudi'
```

([Delta Lake][12])

Create-table example (column mapping auto-set on create in this example):

```sql
CREATE TABLE T(c1 INT) USING DELTA TBLPROPERTIES(
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

([Delta Lake][12])

Enable/upgrade on an existing table (Delta 3.3+):

```sql
ALTER TABLE table_name SET TBLPROPERTIES(
  'delta.enableIcebergCompatV2' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

([Delta Lake][12])

## I3.3 Required Spark packages for enabling

Delta docs call out you must supply:

* `delta-iceberg` package for Iceberg enablement
* `delta-hudi` package for Hudi enablement ([Delta Lake][12])

## I3.4 REORG path (when you need physical rewrites)

```sql
REORG TABLE table_name APPLY (UPGRADE UNIFORM(ICEBERG_COMPAT_VERSION=2));
```

Use REORG if (per docs):

* deletion vectors are enabled
* you previously enabled IcebergCompatV1
* you need to read from Iceberg engines that don’t support Hive-style Parquet (Athena/Redshift mentioned) ([Delta Lake][12])

## I3.5 Metadata generation, status tracking, and external-client registration

* UniForm runs asynchronously after Delta commits; workloads with frequent commits may bundle multiple Delta commits into one Iceberg/Hudi commit; only one metadata generation process per format runs at a time in a cluster. ([Delta Lake][12])
* Status properties written into Iceberg/Hudi metadata:

  * `converted_delta_version`
  * `converted_delta_timestamp`
* For Spark, check via:

```sql
SHOW TBLPROPERTIES <table-name>;
```

([Delta Lake][12])

Iceberg metadata file naming pattern:

```
<table-path>/metadata/v<version-number>-uuid.metadata.json
```

Some Iceberg clients can register by providing the metadata JSON path. ([Delta Lake][12])

## I3.6 Limitations (interop safety)

Key limitations in Delta docs:

* UniForm is **read-only** from Iceberg/Hudi perspective; external writers can corrupt the Delta table (Iceberg/Hudi GC unaware of Delta).
* UniForm does **not** work on tables with deletion vectors enabled (unless you go through the REORG upgrade path that rewrites/purges DVs per docs).
* Delta features like CDF and Delta Sharing remain Delta-only even when UniForm is enabled. ([Delta Lake][12])

---

# I4) Delta Connect (Spark Connect support for Delta APIs)

Delta Connect provides Spark Connect (client/server) support for Delta Lake operations. Delta docs state:

* available in Delta Lake **4.0.0+**
* **preview**, not recommended for production
* enables remote connectivity via Spark Connect gRPC model and keeps DeltaTable APIs working against a remote Spark server ([Delta Lake][13])

## I4.1 Start Spark Connect server with Delta plugins

Delta docs give an explicit server start command:

```bash
sbin/start-connect-server.sh \
  --packages io.delta:delta-connect-server_2.13:4.0.0,com.google.protobuf:protobuf-java:3.25.1 \
  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.connect.extensions.relation.classes=org.apache.spark.sql.connect.delta.DeltaRelationPlugin" \
  --conf "spark.connect.extensions.command.classes=org.apache.spark.sql.connect.delta.DeltaCommandPlugin"
```

([Delta Lake][13])

## I4.2 Python client (thin client + same DeltaTable API)

Docs specify:

* `pip install pyspark==4.0.0`
* `pip install delta-spark==4.0.0`
* run with Spark Connect remote (e.g. `./bin/pyspark --remote "sc://localhost"`)
* pass the **remote SparkSession** into DeltaTable APIs ([Delta Lake][13])

Example in docs:

```python
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

deltaTable = DeltaTable.forName(spark, "my_table")
deltaTable.toDF().show()
deltaTable.update(condition="id % 2 == 0", set={"id": "id + 100"})
```

([Delta Lake][13])

---

## Practical interop rule you can encode in tooling

For any “client X can read/write table T” decision, treat it as:

1. inspect `protocol` (minReader/minWriter + table features)
2. inspect table properties enabling feature behavior (DV/CDF/UniForm/etc.)
3. match against the connector’s declared feature support (e.g., Trino’s supported features list, DuckDB delta extension’s supported scan features) ([Trino][7])

[1]: https://docs.delta.io/quick-start/?utm_source=chatgpt.com "Quick Start"
[2]: https://docs.delta.io/api/latest/python/spark/ "Welcome to Delta Lake’s Python documentation page — delta-spark 4.0.0 documentation"
[3]: https://delta-io.github.io/delta-rs/api/delta_table/ "DeltaTable - Delta Lake Documentation"
[4]: https://delta-io.github.io/delta-rs/python/api_reference.html "API Reference — delta-rs  documentation"
[5]: https://delta-io.github.io/delta-rs/python/usage.html "Usage — delta-rs  documentation"
[6]: https://docs.delta.io/delta-kernel/ "Delta Kernel | Delta Lake"
[7]: https://trino.io/docs/current/connector/delta-lake.html "Delta Lake connector — Trino 479 Documentation"
[8]: https://docs.starburst.io/latest/connector/delta-lake.html "Delta Lake connector — Starburst Enterprise"
[9]: https://duckdb.org/docs/stable/core_extensions/delta.html "Delta Extension – DuckDB"
[10]: https://delta.io/integrations/ "Integrations | Delta Lake"
[11]: https://docs.delta.io/delta-more-connectors/ "Other connectors | Delta Lake"
[12]: https://docs.delta.io/delta-uniform/ "Universal Format (UniForm) | Delta Lake"
[13]: https://docs.delta.io/delta-spark-connect/ "Delta Connect (aka Spark Connect Support in Delta) | Delta Lake"




