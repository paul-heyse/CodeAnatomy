
Below is a **PyIceberg “public surface index”** in two parts:

1. a **strict module → feature → public API symbols matrix** (LLM-agent friendly index)
2. a **guide skeleton** with the same “**full surface area + invariants + footguns + minimal patterns**” structure as your PyArrow doc.

This is keyed off the **PyIceberg docs for `pyiceberg-0.10.0`** (as indicated by the reference navigation) evident in the code reference and repo release tags. ([py.iceberg.apache.org][1])

---

# 1) Module → Feature → Public API symbols matrix

## 1.1 Entry points, configuration, and “catalog-centric” shape

> **Mental model:** PyIceberg is **catalog-centric**: you load a `Catalog`, then `load_table`, then call `Table.scan()/append()/…`. ([py.iceberg.apache.org][2])
> Configuration is via `.pyiceberg.yaml` / env / API/CLI, with a defined search order and FileIO auto-selection by URI scheme. ([py.iceberg.apache.org][3])

| Module                   | Feature buckets                           | Public API symbols (primary)                                                                        |
| ------------------------ | ----------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `pyiceberg.catalog`      | catalog base API + identifier conventions | `Catalog` (ABC), `load_catalog(...)` (doc’d in API guide) ([py.iceberg.apache.org][4])              |
| `pyiceberg.utils.config` | config loading / env mapping              | config utilities (used by `load_catalog` + CLI; see config docs) ([py.iceberg.apache.org][3])       |
| `pyiceberg.io`           | FileIO abstraction                        | `FileIO`, `InputFile`, `OutputFile` (and concrete impls in submodules) ([py.iceberg.apache.org][3]) |

---

## 1.2 Catalog implementations

PyIceberg documents native “type” support for **REST, SQL, Hive, Glue, DynamoDB** and also supports specifying `py-catalog-impl`. ([py.iceberg.apache.org][3])

| Module                                                    | Feature buckets                             | Public API symbols (primary)                                                                     |                              |
| --------------------------------------------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------ | ---------------------------- |
| `pyiceberg.catalog.rest` (+ `rest.auth`, `rest.response`) | REST catalog protocol client                | REST catalog class(es) + REST auth helpers (incl. SigV4 via extras) ([py.iceberg.apache.org][3]) |                              |
| `pyiceberg.catalog.sql`                                   | JDBC-like metastore semantics backed by SQL | `SqlCatalog` ([py.iceberg.apache.org][5])                                                        |                              |
| `pyiceberg.catalog.hive`                                  | Hive metastore catalog                      | Hive catalog implementation (doc’d as native type) ([py.iceberg.apache.org][3])                  |                              |
| `pyiceberg.catalog.glue`                                  | AWS Glue Data Catalog                       | Glue catalog implementation (doc’d as native type) ([py.iceberg.apache.org][3])                  |                              |
| `pyiceberg.catalog.dynamodb`                              | DynamoDB-backed catalog                     | DynamoDB catalog implementation                                                                  | ([py.iceberg.apache.org][3]) |
| `pyiceberg.catalog.memory`                                | ephemeral in-memory catalog                 | In-memory catalog (exists in code ref nav) ([py.iceberg.apache.org][1])                          |                              |
| `pyiceberg.catalog.noop`                                  | placeholder / no-op                         | No-op catalog (exists in code ref nav) ([py.iceberg.apache.org][1])                              |                              |

---

## 1.3 Tables, scans, transactions

The `pyiceberg.table` reference page is effectively the “surface area map” for agent work: it enumerates `Table`, `DataScan`, `Transaction`, and commit request/response models. ([py.iceberg.apache.org][6])

| Module                         | Feature buckets                                 | Public API symbols (primary)                                                                                                                         |
| ------------------------------ | ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pyiceberg.table`              | data reads, writes, metadata refs, transactions | `Table`, `DataScan`, `Transaction`, `CreateTableTransaction`, `StaticTable`, `FileScanTask`, `UpsertResult` ([py.iceberg.apache.org][6])             |
| `pyiceberg.table.snapshots`    | snapshot model + history semantics              | `Snapshot` + snapshot summary semantics (operation types, etc.) ([py.iceberg.apache.org][7])                                                         |
| `pyiceberg.table.metadata`     | metadata JSON model                             | `TableMetadata` (+ fields like `location`, `refs`, `schemas`, etc.) ([py.iceberg.apache.org][8])                                                     |
| `pyiceberg.table.refs`         | named snapshot refs                             | ref models (`SnapshotRef`, `SnapshotRefType`) exist in nav + metadata docs mention refs map and “main” branch invariant ([py.iceberg.apache.org][8]) |
| `pyiceberg.table.maintenance`  | maintenance entrypoints                         | `MaintenanceTable` (currently exposes snapshot expiration builder) ([py.iceberg.apache.org][9])                                                      |
| `pyiceberg.table.locations`    | location providers                              | pluggable location provider concept (configured via table properties) ([py.iceberg.apache.org][3])                                                   |
| `pyiceberg.table.name_mapping` | name-mapping (schema evolution interop)         | name-mapping objects (module exists in nav; used for schema/name resolution workflows) ([py.iceberg.apache.org][1])                                  |
| `pyiceberg.table.statistics`   | table statistics models                         | statistics module exists; `update_statistics` is a first-class `Table` + `Transaction` capability ([py.iceberg.apache.org][10])                      |
| `pyiceberg.table.puffin`       | puffin files (DV etc.)                          | puffin module exists (important for v3 deletion vectors ecosystem) ([GitHub][11])                                                                    |
| `pyiceberg.table.upsert_util`  | helper ops for upsert diffs                     | e.g., `get_rows_to_update(...)` exists (used by upsert pipeline) ([py.iceberg.apache.org][12])                                                       |

---

## 1.4 Update builders (schema/spec/sort/snapshot/statistics + validation)

| Module                              | Feature buckets                         | Public API symbols (primary)                                                                                                              |
| ----------------------------------- | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- |
| `pyiceberg.table.update`            | commit requirements + update primitives | requirement types like `AssertCurrentSchemaId`, `AssertDefaultSpecId`, etc. (optimistic concurrency guards) ([py.iceberg.apache.org][13]) |
| `pyiceberg.table.update.schema`     | schema evolution builder                | `UpdateSchema` methods like `add_column(...)` (and others in API guide) ([py.iceberg.apache.org][14])                                     |
| `pyiceberg.table.update.spec`       | partition evolution builder             | `UpdateSpec` and APIs `add_field`, `remove_field`, `rename_field` ([py.iceberg.apache.org][10])                                           |
| `pyiceberg.table.update.sorting`    | sort order builder                      | `UpdateSortOrder` + `.asc(...)` / `.desc(...)` in API guide ([py.iceberg.apache.org][15])                                                 |
| `pyiceberg.table.update.snapshot`   | snapshot mgmt + expiration              | `ExpireSnapshots` (builder; commit applies staged changes) ([py.iceberg.apache.org][16])                                                  |
| `pyiceberg.table.update.statistics` | stats updates                           | statistics update module exists (paired with `update_statistics`) ([py.iceberg.apache.org][6])                                            |
| `pyiceberg.table.update.validate`   | validate requirements                   | validate module exists (ties to table requirements) ([py.iceberg.apache.org][13])                                                         |

---

## 1.5 Types, schema model, partitioning, transforms, sorting

| Module                    | Feature buckets                             | Public API symbols (primary)                                                                                              |
| ------------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `pyiceberg.types`         | Iceberg type system                         | `StructType`, `NestedField`, primitives like `StringType`, `IntegerType`, etc. ([py.iceberg.apache.org][17])              |
| `pyiceberg.schema`        | schema container + visitors + compat checks | `Schema`, `Accessor`, visitor interfaces; `check_format_version_compatibility(...)` ([py.iceberg.apache.org][18])         |
| `pyiceberg.partitioning`  | partition specs / fields                    | `PartitionField` (+ spec types) ([py.iceberg.apache.org][19])                                                             |
| `pyiceberg.transforms`    | transform library                           | `BucketTransform`, `DayTransform`, etc. (transform semantics; parameterized bucket hashing) ([py.iceberg.apache.org][20]) |
| `pyiceberg.table.sorting` | sort order model                            | `SortOrder`, `SortField`, null ordering etc. ([py.iceberg.apache.org][21])                                                |

---

## 1.6 Expressions / row filtering

| Module                           | Feature buckets                              | Public API symbols (primary)                                                                      |
| -------------------------------- | -------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| `pyiceberg.expressions`          | boolean expression tree, binding, evaluation | `Reference(...).bind(...)` + predicate classes, boolean combinators ([py.iceberg.apache.org][22]) |
| `pyiceberg.expressions.literals` | typed literals                               | literal ordering/comparison infrastructure ([py.iceberg.apache.org][23])                          |
| `pyiceberg.expressions.parser`   | string row-filter parsing                    | string syntax support (SQL-like WHERE) ([py.iceberg.apache.org][24])                              |
| `pyiceberg.expressions.visitors` | evaluation / residual computation            | used in scan planning + residual evaluation ([py.iceberg.apache.org][6])                          |

---

## 1.7 IO, file formats, serialization

| Module                  | Feature buckets                     | Public API symbols (primary)                                                                             |
| ----------------------- | ----------------------------------- | -------------------------------------------------------------------------------------------------------- |
| `pyiceberg.io.pyarrow`  | Arrow FS-backed IO + ArrowScan      | PyArrow FileIO + scan helpers; uses `pyarrow.fs.from_uri` for FS inference ([py.iceberg.apache.org][25]) |
| `pyiceberg.io.fsspec`   | fsspec-backed IO                    | fsspec FileIO implementation ([py.iceberg.apache.org][26])                                               |
| `pyiceberg.manifest`    | manifest read primitives            | `fetch_manifest_entry(io, discard_deleted=True)` (and manifest models) ([py.iceberg.apache.org][27])     |
| `pyiceberg.serializers` | write Iceberg objects to OutputFile | `ToOutputFile` ([py.iceberg.apache.org][28])                                                             |
| `pyiceberg.avro.*`      | avro encode/decode/resolution       | resolver/reader/writer/codec modules (e.g., `construct_writer`) ([py.iceberg.apache.org][29])            |
| `pyiceberg.conversions` | value conversions                   | conversion utilities (exposed in nav) ([py.iceberg.apache.org][3])                                       |

---

## 1.8 CLI

| Module                                  | Feature buckets            | Public API symbols (primary)                                                                                         |
| --------------------------------------- | -------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `pyiceberg.cli` (+ `console`, `output`) | CLI entrypoint + rendering | CLI modules exist; configuration supports CLI credentials injection as a config channel ([py.iceberg.apache.org][3]) |

---

# 2) Guide skeleton: full surface area + invariants + footguns + minimal patterns

## A) Configuration & Catalogs

### Mental model

* **Catalog-centric**: all meaningful table access goes through a `Catalog` (preferred) rather than direct metadata pointers. ([py.iceberg.apache.org][15])
* Config sources: `.pyiceberg.yaml` (recommended), env vars, or API/CLI credential injection. Config file search order is defined. ([py.iceberg.apache.org][3])
* FileIO is **pluggable**; PyIceberg will choose a FileIO based on URI scheme and installed backends. ([py.iceberg.apache.org][3])

### Full surface area (public API)

**Catalog base (`pyiceberg.catalog.Catalog`)**

* Identifier conventions:

  * `identifier: str | tuple[str, ...]` where a string is split on `"."`. ([py.iceberg.apache.org][4])
* Table lifecycle:

  * `create_table(identifier, schema: Schema | pyarrow.Schema, location=None, partition_spec=..., sort_order=..., properties=...) -> Table` ([py.iceberg.apache.org][4])
  * `create_table_transaction(...) -> CreateTableTransaction` ([py.iceberg.apache.org][4])
  * `load_table(identifier) -> Table` ([py.iceberg.apache.org][4])
  * `drop_table(identifier) -> None`, `purge_table(identifier) -> None` (purge also deletes data/metadata files; logs warnings on deletion failure) ([py.iceberg.apache.org][4])
  * `rename_table(from_identifier, to_identifier) -> Table` ([py.iceberg.apache.org][4])
  * `table_exists(identifier) -> bool` ([py.iceberg.apache.org][4])
* Namespace ops are part of the catalog base contract (and documented in config/API guide). ([py.iceberg.apache.org][30])

**Loading catalogs**

* `load_catalog(name="...")` loads by name from `.pyiceberg.yaml`, or you can pass properties directly (API guide shows the pattern). ([py.iceberg.apache.org][2])

### Invariants (design rules you can rely on)

* Catalog identifiers normalize to tuples; don’t bake in string parsing yourself unless you need custom delimiters. ([py.iceberg.apache.org][4])
* Table IO + metadata access is designed to be independent of any JVM runtime. ([py.iceberg.apache.org][31])

### Footguns

* **`purge_table` ≠ `drop_table`**: purge implies deleting table files; errors may be warnings rather than hard failures. ([py.iceberg.apache.org][4])
* Multi-backend deployments: FileIO selection is scheme-driven and “first installed wins” per scheme—this is convenient but can be surprising if you have multiple IO extras installed. ([py.iceberg.apache.org][3])

### Minimal patterns

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("prod")               # .pyiceberg.yaml-defined
table   = catalog.load_table("ns.tbl")       # identifier split on "."
```

Catalog-centric workflow + config examples are canonical. ([py.iceberg.apache.org][2])

---

## B) Table commit protocol, transactions, and atomicity

### Mental model

Iceberg’s table state is snapshot-based; table changes produce new metadata, and readers observe committed snapshots (serializable, no partial visibility). ([iceberg.apache.org][32])
In PyIceberg, “multi-step” table changes are staged in a `Transaction` / builder and applied on `commit()`. ([py.iceberg.apache.org][16])

### Full surface area (public API)

**`pyiceberg.table.Table`**

* Transaction entrypoint:

  * `table.transaction() -> Transaction` ([py.iceberg.apache.org][6])
* Shorthand write APIs (internally route through transactions):

  * `append(df: pyarrow.Table, snapshot_properties=..., branch=...)` ([py.iceberg.apache.org][31])
  * `overwrite(df, overwrite_filter=..., snapshot_properties=..., case_sensitive=..., branch=...)` ([py.iceberg.apache.org][6])
  * `dynamic_partition_overwrite(df, snapshot_properties=..., branch=...)` (detect partitions touched by incoming df; delete those partitions; append new data) ([py.iceberg.apache.org][10])
  * `delete(delete_filter=..., snapshot_properties=..., case_sensitive=..., branch=...)` ([py.iceberg.apache.org][6])
  * `upsert(df, join_cols=..., ...) -> UpsertResult` (rows updated/inserted counts shown in API guide) ([py.iceberg.apache.org][2])
  * `add_files(file_paths=[...], check_duplicate_files=True, branch=...)` ([py.iceberg.apache.org][6])
* Metadata evolution entrypoints:

  * `update_schema(...)`, `update_spec()`, `update_sort_order(...)`, `update_statistics(...)` ([py.iceberg.apache.org][10])
  * Table properties: `set_properties(...) -> Transaction` ([py.iceberg.apache.org][10])
* Read/scan entrypoint:

  * `scan(row_filter=..., selected_fields=..., case_sensitive=..., snapshot_id=..., options=..., limit=...) -> DataScan` ([py.iceberg.apache.org][6])
* Snapshot & ref APIs:

  * `current_snapshot()`, `history()`, `snapshot_by_id(...)`, `snapshot_by_name(...)`, `snapshot_as_of_timestamp(...)`, `refs()` ([py.iceberg.apache.org][6])
  * `manage_snapshots()` (tag/branch management) ([py.iceberg.apache.org][15])
* Introspection entrypoints:

  * `table.inspect` (property) and `table.maintenance` (property) ([py.iceberg.apache.org][6])

**`pyiceberg.table.Transaction`**

* Batch DML equivalents + metadata updates:

  * `append(...)`, `overwrite(...)`, `delete(...)`, `upsert(...)`, `add_files(...)` ([py.iceberg.apache.org][6])
  * `set_properties(...)`, `remove_properties(...)` ([py.iceberg.apache.org][10])
  * `update_schema()`, `update_spec()`, `update_sort_order()`, `update_snapshot()`, `update_statistics()` ([py.iceberg.apache.org][16])
  * `update_location()` (location provider / base location change) ([py.iceberg.apache.org][3])
  * `upgrade_table_version()` ([GitHub][33])

### Invariants

* Table base location is explicit metadata (`TableMetadata.location`) and used to derive where data, manifest, and metadata files live. ([py.iceberg.apache.org][8])
* Snapshot refs include a “main” branch pointer invariant (refs map always implies a main branch to current snapshot, even if refs is otherwise null-ish). ([py.iceberg.apache.org][8])

### Footguns

* `dynamic_partition_overwrite` is not “SQL MERGE”; it’s “replace partitions touched by the incoming DF then append.” It depends on *correct partition-value detection* in the incoming table. ([py.iceberg.apache.org][10])
* `upsert` can be memory-expensive on larger tables in some scenarios; there are reports of large memory growth on big inputs. ([GitHub][34])
* `Table.name` behavior changed recently to *exclude catalog name* (if you previously used `Table.identifier`, this matters for join keys / logging). ([GitHub][35])

### Minimal patterns

```python
# Multi-update atomic staging (pattern)
with table.transaction() as tx:
    tx.set_properties(owner="ml-platform", purpose="features")
    tx.update_spec().add_field("event_ts", DayTransform(), "day_ts")
    tx.commit_transaction()
```

The “use builders inside a transaction then commit” shape is canonical across update types. ([py.iceberg.apache.org][15])

---

## C) Read path: scans, planning, materialization targets

### Mental model

A `DataScan` is a *planned* query: it performs **manifest filtering** + **partition projection** + **metrics evaluation**, then produces `FileScanTask`s. `to_arrow()` / `to_arrow_batch_reader()` materialize by executing those tasks. ([py.iceberg.apache.org][6])

### Full surface area (public API)

**`pyiceberg.table.Table.scan(...) -> DataScan`**

* Parameters:

  * `row_filter: str | BooleanExpression`
  * `selected_fields: tuple[str, ...]` (supports `"*"` special case)
  * `case_sensitive: bool`
  * `snapshot_id: int | None` (time travel)
  * `options: Properties` (scan options)
  * `limit: int | None` ([py.iceberg.apache.org][6])

**`pyiceberg.table.DataScan`**

* Planning:

  * `plan_files() -> Iterable[FileScanTask]` (“Plans relevant files by filtering on PartitionSpecs; includes data + delete files”) ([py.iceberg.apache.org][6])
  * `scan_plan_helper()` (manifest-entry iterator filtered by partition + metrics evaluators) ([py.iceberg.apache.org][6])
* Materialization:

  * `to_arrow() -> pyarrow.Table` (eager; all rows in memory) ([py.iceberg.apache.org][10])
  * `to_arrow_batch_reader() -> pyarrow.RecordBatchReader` (streaming; lower peak memory) ([py.iceberg.apache.org][10])
  * `to_pandas(**kwargs) -> pandas.DataFrame` (via Arrow) ([py.iceberg.apache.org][6])
  * `to_duckdb(table_name, connection=None) -> DuckDBPyConnection` (registers `to_arrow()` result into DuckDB) ([py.iceberg.apache.org][10])
  * `to_polars() -> polars.DataFrame` and `to_ray() -> ray.data.Dataset` (listed in table reference TOC) ([py.iceberg.apache.org][6])

### Invariants

* Scan planning explicitly distinguishes **data files** vs **position delete files** vs **equality delete files** via file content types. ([py.iceberg.apache.org][2])
* Manifest semantics: snapshots are unions of manifest data files; manifests can be reused across snapshots to avoid rewriting slow-changing metadata. ([iceberg.apache.org][36])

### Footguns

* **Equality deletes are not supported in PyIceberg read planning**: `plan_files()` raises a `ValueError` when encountering `EQUALITY_DELETES`. ([py.iceberg.apache.org][10])
* `to_arrow()` is eager and can blow memory on large scans; batch reader is the intended lower-memory path. ([py.iceberg.apache.org][10])
* There are reports of **schema differences** between `to_arrow` vs `to_arrow_batch_reader` in some versions (e.g., string widening to `large_string`), so treat the batch reader as its own execution path and validate schemas in tests. ([GitHub][37])

### Minimal patterns

```python
scan = table.scan(
    row_filter='city = "Paris" AND inhabitants >= 1000000',
    selected_fields=("city", "inhabitants"),
    limit=10,
)

# Plan-only phase (file tasks):
tasks = list(scan.plan_files())

# Execution phase:
rb = scan.to_arrow_batch_reader()
```

Row filter can be string syntax (SQL-ish) and is parsed into the scan’s boolean expression tree. ([py.iceberg.apache.org][24])

---

## D) Expressions & row filter syntax

### Mental model

Two equal-power entrypoints:

* **Expression DSL** for type-safe construction (build boolean expressions programmatically). ([py.iceberg.apache.org][22])
* **Row-filter string syntax** intended to feel like a SQL `WHERE` clause. ([py.iceberg.apache.org][24])

### Full surface area

* DSL components:

  * `Reference(name)` and `.bind(schema, case_sensitive=True)` to resolve fields against a schema. ([py.iceberg.apache.org][38])
  * Predicate + boolean combinators (documented in expression DSL guide) ([py.iceberg.apache.org][22])
* String syntax:

  * supports quoted identifiers (e.g., `"column.name"`) and SQL-like operators per the syntax guide. ([py.iceberg.apache.org][24])

### Invariants

* Binding is schema-aware; case sensitivity is explicit (and threaded through `Table.scan(..., case_sensitive=...)`). ([py.iceberg.apache.org][38])

### Footguns

* Nested field references can be tricky when field names include `"."` — PyIceberg explicitly calls out the ambiguity in schema evolution APIs (and similar ambiguity exists conceptually in expression parsing). ([py.iceberg.apache.org][24])

### Minimal patterns

```python
from pyiceberg.expressions import Reference

expr = (Reference("inhabitants") >= 1_000_000)  # DSL-style
table.scan(row_filter=expr).to_arrow()
```

Expression DSL is explicitly designed for `row_filter`. ([py.iceberg.apache.org][22])

---

## E) Schema & type system

### Mental model

Iceberg schema evolution is **metadata-first** and relies on **stable field IDs** across rename/reorder/type promotion so readers can map old files to new schemas. External systems emphasize “IDs over positions.” ([AWS Documentation][39])

### Full surface area

* Types: `pyiceberg.types` implements Iceberg spec types; schema is constructed from `Schema(NestedField(...), ...)` and nested `StructType(...)`. ([py.iceberg.apache.org][17])
* Schema compatibility checks: `Schema.check_format_version_compatibility(format_version)` ([py.iceberg.apache.org][18])
* Schema evolution: `table.update_schema()` (builder), with API guide sections: add/rename/move/update/delete column. ([py.iceberg.apache.org][2])

### Invariants

* Table metadata carries schema versions and current schema id (used in optimistic commit requirements). ([py.iceberg.apache.org][13])

### Footguns

* **Nested column add path**: `UpdateSchema.add_column(path, ...)` disallows a single string for nested paths because `"."` is ambiguous; you must use a tuple path for nested struct fields or field names containing `"."`. ([py.iceberg.apache.org][14])

### Minimal patterns

```python
from pyiceberg.types import NestedField, StringType
from pyiceberg.schema import Schema

schema = Schema(
  NestedField(field_id=1, name="city", field_type=StringType(), required=True),
)

tbl = catalog.create_table("ns.cities", schema=schema)
```

Table creation accepts Iceberg `Schema` (and also `pyarrow.Schema`). ([py.iceberg.apache.org][4])

---

## F) Partitioning, transforms, sort order

### Mental model

Partitioning is declarative: a **`PartitionField`** derives partition values from a source column via a **Transform**; partition evolution mutates specs without rewriting old files (conceptually). ([py.iceberg.apache.org][19])

### Full surface area

* Partition objects:

  * `PartitionField(source_id, field_id, transform, name)` ([py.iceberg.apache.org][19])
* Transforms:

  * `BucketTransform(num_buckets)` defines bucket hashing semantics and is parameterized by bucket count. ([py.iceberg.apache.org][20])
* Partition evolution:

  * `table.update_spec()` with `.add_field(...)`, `.remove_field(name)`, `.rename_field(old, new)` (API guide) ([py.iceberg.apache.org][15])
* Sorting:

  * `SortOrder(fields=[...], order_id=...)` model. ([py.iceberg.apache.org][21])
  * `table.update_sort_order(case_sensitive=True)` + `.asc(...)` / `.desc(...)` pattern (API guide) ([py.iceberg.apache.org][10])

### Invariants

* `DataScan.plan_files()` filters relevant files by `PartitionSpecs` and yields tasks that include delete files and residual expressions (when partition/metrics pruning isn’t sufficient). ([py.iceberg.apache.org][6])

### Footguns

* Partition evolution details matter for repeatability; there are edge-case reports around remove/add sequences unless names are pinned (community report). ([GitHub][40])
* Some “identity transform projection” corner cases have been discussed as spec conformance issues in the ecosystem (treat partition-projection behavior as testable, not axiomatic). ([GitHub][41])

### Minimal patterns

```python
from pyiceberg.transforms import BucketTransform, DayTransform

with table.update_spec() as u:
    u.add_field("id", BucketTransform(16), "bucketed_id")
    u.add_field("event_ts", DayTransform(), "day_ts")
```

This exact pattern is shown in the API guide. ([py.iceberg.apache.org][15])

---

## G) Inspect / metadata tables + time travel

### Mental model

Iceberg metadata is queryable as “metadata tables.” PyIceberg exposes these via `table.inspect.*()` methods that return Arrow tables. ([py.iceberg.apache.org][2])

### Full surface area (as documented)

From the API guide, `InspectTable` exposes (at least):

* `table.inspect.snapshots()` ([py.iceberg.apache.org][2])
* `table.inspect.partitions()` ([py.iceberg.apache.org][2])
* `table.inspect.entries(...)` (supports `snapshot_id` time travel) ([py.iceberg.apache.org][2])
* `table.inspect.refs()` ([py.iceberg.apache.org][2])
* `table.inspect.manifests()` ([py.iceberg.apache.org][2])
* `table.inspect.metadata_log_entries()` ([py.iceberg.apache.org][2])
* `table.inspect.history()` ([py.iceberg.apache.org][2])
* `table.inspect.files()` ([py.iceberg.apache.org][2])
* Helpers called out in the guide:

  * `table.inspect.data_files()` and `table.inspect.delete_files()` (content-filtered subsets) ([py.iceberg.apache.org][15])

Time travel note: inspection supports `snapshot_id` for metadata tables **except** `snapshots` and `refs`. ([py.iceberg.apache.org][2])

### Invariants

* Snapshot table includes operation type values (append/overwrite/replace/…) and summary map. ([py.iceberg.apache.org][2])

### Footguns

* Inspecting `.files()` / `.partitions()` can be expensive for large manifest sets (community perf concerns), so treat these as “debug/introspection” calls unless you’ve validated performance. ([GitHub][42])

### Minimal patterns

```python
snapshots = table.inspect.snapshots()
files     = table.inspect.files()
entries_t = table.inspect.entries(snapshot_id=some_snapshot_id)
```

These are direct from the API guide. ([py.iceberg.apache.org][2])

---

## H) Snapshot refs: tags + branches

### Mental model

Refs are named pointers to snapshots:

* **Tags** are immutable named references used for retention / version pinning. ([py.iceberg.apache.org][15])

### Full surface area (as documented)

* `table.manage_snapshots().create_tag(snapshot_id=..., tag_name=...).commit()` ([py.iceberg.apache.org][15])
* `ExpireSnapshots` builder exists and is returned via `table.maintenance.expire_snapshots()` (maintenance wrapper). ([py.iceberg.apache.org][9])

### Footguns

* Branch/tag operations are commit-based: staged changes don’t apply until `commit()` (same mental model as schema/spec updates). ([py.iceberg.apache.org][16])

---

## I) Maintenance

### Mental model

Iceberg tables accumulate metadata and snapshots; recommended practice is to regularly expire snapshots and clean old metadata. Iceberg docs emphasize that each change produces a new metadata file and old ones accumulate. ([iceberg.apache.org][43])

### Full surface area (PyIceberg-exposed)

* `table.maintenance` returns `MaintenanceTable`, which exposes `expire_snapshots() -> ExpireSnapshots` builder. ([py.iceberg.apache.org][9])

*(Other maintenance tasks exist in Iceberg engines, but the PyIceberg public surface currently spotlights snapshot expiration.)* ([iceberg.apache.org][43])

---

## J) FileIO + storage backends

### Mental model

All reads/writes flow through a `FileIO` implementation; by default PyIceberg picks one based on URI scheme + installed backends. ([py.iceberg.apache.org][3])

### Full surface area

* `pyiceberg.io.pyarrow`: IO implemented on `pyarrow.fs`, relying on `from_uri` to infer filesystem. ([py.iceberg.apache.org][25])
* `pyiceberg.io.fsspec`: IO implemented via fsspec filesystem adapters. ([py.iceberg.apache.org][26])

### Invariants

* Supported schemes are documented (s3/gs/file/hdfs/abfs/…) and map to fileio implementations. ([py.iceberg.apache.org][3])

### Footguns

* Mixed-mount environments (separate FS for metadata vs data) can create surprises if the chosen IO is inferred from metadata location; validate IO resolution in integration tests for multi-scheme tables. ([py.iceberg.apache.org][8])

---

## K) Manifests, Avro, and serialization primitives

### Mental model

Iceberg scan planning is metadata-driven:

* snapshots point to manifest lists; manifests enumerate data files + partition + metrics; manifests can be reused across snapshots. ([iceberg.apache.org][36])
  PyIceberg exposes manifest readers and Avro resolver/writer components as first-class modules. ([py.iceberg.apache.org][27])

### Full surface area

* `pyiceberg.manifest.fetch_manifest_entry(io, discard_deleted=True) -> list[ManifestEntry]` ([py.iceberg.apache.org][27])
* `pyiceberg.serializers.ToOutputFile` for serializing Iceberg objects to `OutputFile`. ([py.iceberg.apache.org][28])
* `pyiceberg.avro.resolver.construct_writer(file_schema)` and related reader/writer resolution helpers. ([py.iceberg.apache.org][29])

### Footguns

* Equality deletes are a known pain point in scan planning/reading paths across engines; PyIceberg currently hard-errors when equality deletes appear in planning. ([GitHub][44])

---

## L) Engine interop (Arrow / Pandas / DuckDB / Polars / Ray / DataFusion)

### Mental model

Two patterns:

1. **PyIceberg executes** → returns Arrow/Pandas/Polars DF (eager)
2. **Engine-native lazy scan** uses Iceberg metadata + pushdown capabilities (e.g., Polars `scan_iceberg`, DataFusion table provider).

### Full surface area (documented)

* Arrow:

  * `table.scan().to_arrow()` / `to_arrow_batch_reader()`
* Pandas:

  * `table.scan().to_pandas(...)`
* DuckDB:

  * `table.scan().to_duckdb(table_name, connection=None)`
* Polars:

  * For eager DF: `table.scan().to_polars()` (doc’d in API guide)
  * For lazy: `table.to_polars()` (API guide recommends this when you want Polars pushdown)
  * Also `polars.scan_iceberg(path_or_table, storage_options=...) -> LazyFrame` (Polars side)
* Ray:

  * `table.scan().to_ray()` is listed in PyIceberg table surface; Ray also documents `ray.data.read_iceberg(...)` as a PyIceberg-powered integration.
* DataFusion:

  * `Table.__datafusion_table_provider__()` exists (Table TOC) for provider registration.

### Footguns

* Polars `write_iceberg` is explicitly marked unstable on the Polars side; treat it as experimental unless pinned and tested.

---

If you want the *next* increment after this: I’d convert this into a **mechanical “symbol index”** (one-liners per symbol with signature + return types + where it’s used), and then a **per-feature deep dive** that mirrors your PyArrow doc’s density (including the “what’s immutable,” “where IDs matter,” and “how scan planning composes partition + metrics + delete files”).

[1]: https://py.iceberg.apache.org/reference/pyiceberg/table/inspect/ "inspect - PyIceberg"
[2]: https://py.iceberg.apache.org/api/ "API - PyIceberg"
[3]: https://py.iceberg.apache.org/configuration/?utm_source=chatgpt.com "Configuration"
[4]: https://py.iceberg.apache.org/reference/pyiceberg/catalog/ "catalog - PyIceberg"
[5]: https://py.iceberg.apache.org/reference/pyiceberg/catalog/sql/?utm_source=chatgpt.com "sql"
[6]: https://py.iceberg.apache.org/reference/pyiceberg/table/ "table - PyIceberg"
[7]: https://py.iceberg.apache.org/reference/pyiceberg/table/snapshots/?utm_source=chatgpt.com "snapshots"
[8]: https://py.iceberg.apache.org/reference/pyiceberg/table/metadata/?utm_source=chatgpt.com "metadata"
[9]: https://py.iceberg.apache.org/reference/pyiceberg/table/maintenance/?utm_source=chatgpt.com "maintenance"
[10]: https://py.iceberg.apache.org/reference/pyiceberg/table/?utm_source=chatgpt.com "table"
[11]: https://github.com/apache/iceberg-python/issues/1549?utm_source=chatgpt.com "Support Deletion Vectors #1549 - apache/iceberg-python"
[12]: https://py.iceberg.apache.org/reference/pyiceberg/table/upsert_util/?utm_source=chatgpt.com "upsert_util"
[13]: https://py.iceberg.apache.org/reference/pyiceberg/table/update/?utm_source=chatgpt.com "update"
[14]: https://py.iceberg.apache.org/reference/pyiceberg/table/update/schema/?utm_source=chatgpt.com "schema"
[15]: https://py.iceberg.apache.org/api/?utm_source=chatgpt.com "Python API"
[16]: https://py.iceberg.apache.org/reference/pyiceberg/table/update/snapshot/?utm_source=chatgpt.com "snapshot - PyIceberg"
[17]: https://py.iceberg.apache.org/reference/pyiceberg/types/?utm_source=chatgpt.com "types"
[18]: https://py.iceberg.apache.org/reference/pyiceberg/schema/?utm_source=chatgpt.com "schema"
[19]: https://py.iceberg.apache.org/reference/pyiceberg/partitioning/?utm_source=chatgpt.com "partitioning"
[20]: https://py.iceberg.apache.org/reference/pyiceberg/transforms/?utm_source=chatgpt.com "transforms"
[21]: https://py.iceberg.apache.org/reference/pyiceberg/table/sorting/?utm_source=chatgpt.com "sorting"
[22]: https://py.iceberg.apache.org/expression-dsl/?utm_source=chatgpt.com "Expression DSL"
[23]: https://py.iceberg.apache.org/reference/pyiceberg/expressions/literals/?utm_source=chatgpt.com "literals"
[24]: https://py.iceberg.apache.org/row-filter-syntax/?utm_source=chatgpt.com "Row Filter Syntax"
[25]: https://py.iceberg.apache.org/reference/pyiceberg/io/pyarrow/?utm_source=chatgpt.com "pyarrow - PyIceberg"
[26]: https://py.iceberg.apache.org/reference/pyiceberg/io/fsspec/?utm_source=chatgpt.com "fsspec"
[27]: https://py.iceberg.apache.org/reference/pyiceberg/manifest/?utm_source=chatgpt.com "manifest - PyIceberg"
[28]: https://py.iceberg.apache.org/reference/pyiceberg/serializers/?utm_source=chatgpt.com "serializers"
[29]: https://py.iceberg.apache.org/reference/pyiceberg/avro/resolver/?utm_source=chatgpt.com "resolver - PyIceberg"
[30]: https://py.iceberg.apache.org/reference/pyiceberg/catalog/dynamodb/?utm_source=chatgpt.com "dynamodb"
[31]: https://py.iceberg.apache.org/?utm_source=chatgpt.com "PyIceberg"
[32]: https://iceberg.apache.org/spec/?h=equality&utm_source=chatgpt.com "Equality Delete Files - Spec - Apache Iceberg™"
[33]: https://github.com/apache/iceberg-python/issues/1540?utm_source=chatgpt.com "Support reading V3 tables · Issue #1540 · apache/iceberg- ..."
[34]: https://github.com/apache/iceberg-python/issues/2138?utm_source=chatgpt.com "Upsertion memory usage grows exponentially as table size ..."
[35]: https://github.com/apache/iceberg-python/releases?utm_source=chatgpt.com "Releases · apache/iceberg-python"
[36]: https://iceberg.apache.org/spec/?utm_source=chatgpt.com "Spec - Apache Iceberg™"
[37]: https://github.com/apache/iceberg-python/issues/2250?utm_source=chatgpt.com "`to_arrow_batch_reader` returns a different schema than ` ..."
[38]: https://py.iceberg.apache.org/reference/pyiceberg/expressions/?utm_source=chatgpt.com "expressions"
[39]: https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-evolving-table-schema.html?utm_source=chatgpt.com "Evolve Iceberg table schema - Amazon Athena"
[40]: https://github.com/apache/iceberg-python/issues/1131?utm_source=chatgpt.com "Removing then adding the same partition in update_spec ..."
[41]: https://github.com/apache/iceberg-python/issues/1401?utm_source=chatgpt.com "API table.scan does not conform to Iceberg spec for identity ..."
[42]: https://github.com/apache/iceberg-python/issues/1229?utm_source=chatgpt.com "[Feature Request] Speed up InspectTable.files() #1229"
[43]: https://iceberg.apache.org/docs/1.7.1/maintenance/?utm_source=chatgpt.com "Maintenance - Apache Iceberg™"
[44]: https://github.com/apache/iceberg-python/issues/1210?utm_source=chatgpt.com "[feature request] Support reading equality delete files #1210"


Below are the **two deliverables** you asked for:

1. a **mechanical “symbol index”** (module → symbol → one-liner with **signature / return type / where used**)
2. a **per-feature deep dive** in the same “**mental model → full surface area → invariants → footguns → minimal patterns**” style as your PyArrow guide.

**Version anchor:** this is keyed to the **PyIceberg docs/code reference for `pyiceberg-0.10.0`** as rendered on `py.iceberg.apache.org`. ([py.iceberg.apache.org][1])

---

# 1) Mechanical symbol index (LLM-agent index)

## 1.1 `pyiceberg.catalog` (catalog resolution + core CRUD)

### Bootstrapping

* `load_catalog(name: str | None = None, **properties: str | None) -> Catalog` — merges config + passed props; rejects simultaneously setting `type` and `py-catalog-impl`; instantiates from inferred type or explicit catalog impl. **Used by:** basically every “entrypoint” workflow. ([py.iceberg.apache.org][2])
* `infer_catalog_type(name: str, catalog_properties: RecursiveDict) -> CatalogType | None` — infers by `uri` scheme (`http`→REST, `thrift`→Hive, `sqlite/postgresql`→SQL). **Used by:** `load_catalog` when `type` omitted. ([py.iceberg.apache.org][3])
* `Catalog.identifier_to_tuple(identifier: str | Identifier) -> Identifier` — `"a.b.c"` → `("a","b","c")` (tuple preserved). **Used by:** every identifier-taking API. ([py.iceberg.apache.org][3])

### Core catalog interface

* `Catalog.create_table(identifier, schema: Schema | pa.Schema, location=None, partition_spec=UNPARTITIONED_PARTITION_SPEC, sort_order=UNSORTED_SORT_ORDER, properties=EMPTY_DICT) -> Table` — creates table metadata + commits. **Used by:** table creation; often followed by `Table.append/overwrite`. ([py.iceberg.apache.org][2])
* `Catalog.create_table_transaction(...) -> CreateTableTransaction` — staged create; caller later `commit_transaction() -> Table`. **Used by:** “build metadata then commit” flows. ([py.iceberg.apache.org][1])
* `Catalog.load_table(identifier) -> Table` — loads metadata only (no data scan); typical existence check via catching `NoSuchTableError`. **Used by:** read/write entrypoint. ([py.iceberg.apache.org][2])
* `Catalog.register_table(identifier, metadata_location: str) -> Table` — registers an existing metadata.json as a table in the catalog. **Used by:** “bring-your-own-metadata” table registration. ([py.iceberg.apache.org][2])
* `Catalog.purge_table(identifier) -> None` — drop + best-effort deletion of data/metadata; deletion failures logged as warnings (not raised). **Used by:** cleanup / destructive test teardown. ([py.iceberg.apache.org][2])
* `Catalog.list_tables(namespace) -> list[Identifier]` — enumerate tables under a namespace. **Used by:** discovery/UX tooling. ([py.iceberg.apache.org][2])

---

## 1.2 `pyiceberg.table` (Table, Scan, Transaction, commit models)

### Primary objects

* `Table(identifier: Identifier, metadata: TableMetadata, metadata_location: str, io: FileIO, catalog: Catalog, config: dict[str,str]=EMPTY_DICT) -> None` — in-memory handle to metadata + IO + catalog. **Used by:** returned from `Catalog.load_table`. ([py.iceberg.apache.org][1])
* `StaticTable.from_metadata(metadata_location: str) -> StaticTable` — read-only handle (no catalog). **Used by:** direct metadata.json reads (debugging / offline). ([py.iceberg.apache.org][2])
* `Transaction(table: Table, autocommit: bool=False) -> Transaction` — stages updates + requirements; later `commit_transaction() -> Table`. **Used by:** multi-operation atomic updates and most builders. ([py.iceberg.apache.org][1])
* `DataScan(table_metadata: TableMetadata, io: FileIO, row_filter=..., selected_fields=..., case_sensitive=True, snapshot_id=None, options=..., limit=None)` — scan plan + materializers. **Used by:** `Table.scan(...)`. ([py.iceberg.apache.org][1])
* `FileScanTask(file: DataFile, delete_files: list[DataFile], residual: BooleanExpression)` — “unit of read work” after planning. **Used by:** `DataScan.plan_files()`, then consumed by ArrowScan. ([py.iceberg.apache.org][1])

### Commit payload models (useful for REST / advanced integrations)

* `CommitTableRequest(identifier: TableIdentifier, requirements: tuple[TableRequirement,...], updates: tuple[TableUpdate,...])` — commit request model. **Used by:** catalog commit path. ([py.iceberg.apache.org][1])
* `CommitTableResponse(metadata: TableMetadata, metadata_location: str)` — commit response model. **Used by:** commit path returns. ([py.iceberg.apache.org][1])

### Read surface

* `Table.scan(row_filter: str | BooleanExpression = ALWAYS_TRUE, selected_fields=('*',), case_sensitive=True, snapshot_id=None, options=EMPTY_DICT, limit=None) -> DataScan` — constructs scan over a snapshot (current unless time-travel). **Used by:** all reads. ([py.iceberg.apache.org][1])
* `DataScan.plan_files() -> Iterable[FileScanTask]` — manifest planning + deletes matching + residual computation. **Used by:** all scan execution backends. ([py.iceberg.apache.org][1])
* `DataScan.to_arrow() -> pa.Table` — eager materialization (all rows). **Used by:** “small enough to fit in memory” reads, also feeds pandas/polars/duckdb/ray. ([py.iceberg.apache.org][1])
* `DataScan.to_arrow_batch_reader() -> pa.RecordBatchReader` — streaming batches; schema built from projected schema then `.cast(target_schema)`. **Used by:** large reads / streaming. ([py.iceberg.apache.org][1])
* `DataScan.to_pandas(**kwargs) -> pd.DataFrame` — delegates to `to_arrow().to_pandas`. **Used by:** pandas interop. ([py.iceberg.apache.org][1])
* `DataScan.to_duckdb(table_name: str, connection: DuckDBPyConnection|None=None) -> DuckDBPyConnection` — registers Arrow table in an in-memory DuckDB connection. **Used by:** interactive SQL over scan result. ([py.iceberg.apache.org][1])
* `DataScan.to_polars() -> pl.DataFrame` — `pl.from_arrow(self.to_arrow())`. **Used by:** eager polars interop. ([py.iceberg.apache.org][1])
* `DataScan.to_ray() -> ray.data.Dataset` — `ray.data.from_arrow(self.to_arrow())`. **Used by:** Ray Dataset interop. ([py.iceberg.apache.org][1])

### Engine-native lazy reads

* `Table.to_polars() -> pl.LazyFrame` — calls `pl.scan_iceberg(self)` (pushdown-friendly lazy). **Used by:** polars-native query planning. ([py.iceberg.apache.org][1])
* `Table.__datafusion_table_provider__() -> IcebergDataFusionTable` — returns a PyCapsule provider for DataFusion registration. **Used by:** DataFusion SessionContext integration. ([py.iceberg.apache.org][4])

### Write / mutate surface (Arrow-first)

* `Table.transaction() -> Transaction` — staged updates. **Used by:** atomic multi-op commits and by builder wrappers. ([py.iceberg.apache.org][1])
* `Table.append(df: pa.Table, snapshot_properties=EMPTY_DICT, branch=MAIN_BRANCH) -> None` — shorthand `with transaction(): tx.append(...)`. **Used by:** append writes. ([py.iceberg.apache.org][1])
* `Table.overwrite(df: pa.Table, overwrite_filter: str|BooleanExpression=ALWAYS_TRUE, snapshot_properties=..., case_sensitive=True, branch=...) -> None` — may yield multiple snapshots depending on operation type (delete/overwrite/append). **Used by:** full or filtered overwrite. ([py.iceberg.apache.org][1])
* `Table.dynamic_partition_overwrite(df: pa.Table, snapshot_properties=..., branch=...) -> None` — detects partition values in incoming Arrow table, deletes matching partitions, then appends. **Used by:** “replace touched partitions” semantics. ([py.iceberg.apache.org][1])
* `Table.delete(delete_filter: str|BooleanExpression, snapshot_properties=..., case_sensitive=True, branch=...) -> None` — predicate delete. **Used by:** row deletes (subject to delete-file support). ([py.iceberg.apache.org][1])
* `Table.add_files(file_paths: list[str], check_duplicate_files=True, branch=...) -> None` — adds existing files to table metadata. **Used by:** “register data files” workflows. ([py.iceberg.apache.org][1])
* `Table.upsert(df: pa.Table, join_cols: list[str]|None=None, when_matched_update_all=True, when_not_matched_insert_all=True, case_sensitive=True, branch=MAIN_BRANCH) -> UpsertResult` — uses `identifier-field-ids` when join_cols omitted. **Used by:** merge-like behavior. ([py.iceberg.apache.org][1])

### Metadata introspection

* `Table.inspect -> InspectTable` — exposes metadata tables (snapshots/entries/files/etc.), with time travel on most. **Used by:** debugging, auditing, UX. ([py.iceberg.apache.org][4])
* `Table.maintenance -> MaintenanceTable` — maintenance entrypoint. **Used by:** snapshot expiration. ([py.iceberg.apache.org][4])
* `Table.refs() -> dict[str, SnapshotRef]` — returns refs map (main branch, tags, etc.). **Used by:** time travel + governance. ([py.iceberg.apache.org][4])

---

## 1.3 `pyiceberg.table.update` (requirements, base builder behaviors, metadata update engine)

### Update builder base contract

* `UpdateTableMetadata.__enter__() -> U` — returns `self` for `with ... as update:`. **Used by:** all update builders. ([py.iceberg.apache.org][5])
* `UpdateTableMetadata.__exit__(..., ...) -> None` — calls `self.commit()` on context manager exit. **Used by:** all `with table.update_*(): ...` idioms. ([py.iceberg.apache.org][5])
* `update_table_metadata(base_metadata: TableMetadata, updates: tuple[TableUpdate,...], enforce_validation=False, metadata_location=None) -> TableMetadata` — applies updates, optionally validates, updates metadata log + `last_updated_ms` if changes occurred. **Used by:** commit engine. ([py.iceberg.apache.org][5])

### Concurrency/consistency requirements (selected)

* `AssertCurrentSchemaId`, `AssertDefaultSpecId`, `AssertDefaultSortOrderId`, `AssertLastAssignedFieldId`, `AssertLastAssignedPartitionId`, `AssertRefSnapshotId`, `AssertTableUUID`, … — validate base metadata before applying updates; failures raise `CommitFailedException`. **Used by:** transaction/builders to enforce optimistic concurrency. ([py.iceberg.apache.org][1])

---

## 1.4 `pyiceberg.table.update.schema` (schema evolution builder)

* `UpdateSchema.union_by_name(new_schema: Schema | pa.Schema, format_version: TableVersion = 2) -> UpdateSchema` — adds fields present in `new_schema` but absent in existing schema, mapped by name with case-sensitivity control. **Used by:** evolving schema alongside Arrow data. ([py.iceberg.apache.org][6])
* `UpdateSchema.add_column(path: str | tuple[str,...], field_type: IcebergType, doc=None, required=False, default_value=None) -> UpdateSchema` — disallows ambiguous dotted strings; nested-type field IDs are reassigned when added. **Used by:** add columns (incl. nested). ([py.iceberg.apache.org][7])
* `UpdateSchema.rename_column(path_from: str|tuple[str,...], new_name: str) -> UpdateSchema` — rename by path. **Used by:** rename columns (top-level or nested). ([py.iceberg.apache.org][7])

---

## 1.5 `pyiceberg.table.update.sorting` (sort order evolution builder)

* `UpdateSortOrder.asc(source_column_name: str, transform: Transform[Any,Any], null_order: NullOrder = NULLS_LAST) -> UpdateSortOrder` — adds ascending sort field; maps column name → field id at commit time. **Used by:** sort order updates. ([py.iceberg.apache.org][8])
* `UpdateSortOrder.desc(source_column_name: str, transform: Transform[Any,Any], null_order: NullOrder = NULLS_LAST) -> UpdateSortOrder` — descending variant. **Used by:** sort order updates. ([py.iceberg.apache.org][8])

---

## 1.6 `pyiceberg.table.update.snapshot` (snapshot expiration builder)

* `ExpireSnapshots(...)` — builder for expiring snapshots by ID; staged ops applied on commit. **Used by:** `table.maintenance.expire_snapshots()`. ([py.iceberg.apache.org][2])

---

## 1.7 `pyiceberg.expressions` (filters, binding)

* `Reference(name: str).bind(schema: Schema, case_sensitive: bool=True) -> BoundReference` — binds a column reference to an Iceberg schema field (resolves IDs/types). **Used by:** expression evaluation + scan planning pushdown. ([py.iceberg.apache.org][9])

---

## 1.8 `pyiceberg.partitioning` / `pyiceberg.transforms` / `pyiceberg.table.sorting`

* `PartitionSpec(spec_id: int, fields: tuple[PartitionField,...])` — spec changes produce a new `spec_id`. **Used by:** planning + writers. ([py.iceberg.apache.org][10])
* `BucketTransform(num_buckets: int)` — 32-bit hash bucket transform semantics. **Used by:** partition transforms and sort transforms. ([py.iceberg.apache.org][11])
* `SortOrder(*fields: SortField, order_id: int=...)` — sort order model; referenced by default sort order id in table metadata. **Used by:** table metadata + planning/validation. ([py.iceberg.apache.org][12])

---

## 1.9 `pyiceberg.io.pyarrow` (Arrow execution backend)

* `ArrowScan(...).to_table(tasks: Iterable[FileScanTask]) -> pa.Table` — executes scan tasks into Arrow table (honoring projection + row_filter + deletes). **Used by:** `DataScan.to_arrow()`. ([py.iceberg.apache.org][13])
* `ArrowScan(...).to_record_batches(tasks) -> Iterator[pa.RecordBatch]` — batch iterator backend. **Used by:** `DataScan.to_arrow_batch_reader()`. ([py.iceberg.apache.org][13])

---

## 1.10 `pyiceberg.table.metadata` (metadata model)

* `TableMetadata` / `TableMetadataV*` models — base `location` defines where data/manifest/metadata files live; metadata log captures prior metadata file locations. **Used by:** everything; `Table.metadata` is the canonical state. ([py.iceberg.apache.org][14])
* `partition_statistics: list[PartitionStatisticsFile]` — optional; readers may ignore; at most one per snapshot. **Used by:** planning acceleration / analytics. ([py.iceberg.apache.org][14])

---

# 2) Per-feature deep dives (PyArrow-doc style)

## A) Catalogs & configuration — deep dive

### Mental model

PyIceberg is **catalog-centric**: you load a `Catalog`, then `load_table`, then mutate/query through `Table`. The docs explicitly position read/write flows through the catalog as the preferred path, with `StaticTable` as a read-only escape hatch. ([py.iceberg.apache.org][2])

Configuration is sourced from `.pyiceberg.yaml` (searched in several locations) and/or merged with explicit kwargs passed to `load_catalog`. ([py.iceberg.apache.org][2])

### Full surface area

**Catalog resolution**

* `load_catalog(name=None, **properties) -> Catalog` merges config + kwargs and instantiates a catalog implementation based on either explicit `type`/`py-catalog-impl` or inferred from `uri`. ([py.iceberg.apache.org][3])
* `infer_catalog_type(...)` (scheme-based inference) and identifier parsing helpers (`identifier_to_tuple`, `namespace_from`, `table_name_from`, etc.). ([py.iceberg.apache.org][3])

**CRUD**

* `create_table`, `create_table_transaction`, `load_table`, `register_table`, `purge_table`, namespace list/create/drop, list tables/views, etc. ([py.iceberg.apache.org][3])

### Invariants

* Identifier parsing is consistent across APIs: strings split on `"."`, tuples preserved (critical for namespaces/table names containing dots). ([py.iceberg.apache.org][2])
* If no explicit `type` is provided, `uri` determines the catalog type (REST/Hive/SQL) by scheme. ([py.iceberg.apache.org][3])

### Footguns

* `purge_table` is deliberately **best-effort** on deletion failures (warnings instead of exceptions) — don’t use it as a “must delete everything” primitive without follow-up verification. ([py.iceberg.apache.org][3])
* `load_catalog` rejects simultaneously specifying `type` and `py-catalog-impl`; this matters for “override remote config locally” scenarios. ([py.iceberg.apache.org][3])

### Minimal patterns

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("prod")      # uses ~/.pyiceberg.yaml search + merge
table = catalog.load_table("ns.tbl")
```

([py.iceberg.apache.org][2])

---

## B) Table metadata, immutability, and “where IDs matter” — deep dive

### Mental model

An Iceberg table’s “truth” is the **metadata graph** (metadata.json → snapshots → manifest lists → manifests → data/delete files). PyIceberg’s `Table` is essentially a **typed wrapper around `TableMetadata` + IO + catalog commit plumbing**. ([py.iceberg.apache.org][1])

IDs are *first-class*: schema fields have stable **field IDs**, partition specs have **spec_id**, sort orders have **order_id**, and updates are guarded by **requirements** (optimistic concurrency) that validate “base metadata hasn’t changed” (e.g., schema id, spec id, UUID). ([py.iceberg.apache.org][10])

### Full surface area

* `Table.metadata: TableMetadata` and `metadata_location: str` are the canonical state pointers. ([py.iceberg.apache.org][1])
* Base location: `TableMetadata.location` is used by writers to determine where to store data/manifest/metadata files. ([py.iceberg.apache.org][14])
* ID tracking:

  * `Table.last_partition_id()` returns “highest assigned partition field ID” (or default start-1) and is backed by `TableMetadata.last_partition_id`. ([py.iceberg.apache.org][1])
  * `PartitionSpec.spec_id` increments whenever the spec changes. ([py.iceberg.apache.org][10])
* Optional advanced metadata:

  * `partition_statistics` is optional; readers may ignore; one per snapshot max. ([py.iceberg.apache.org][14])

### Invariants

* Metadata log grows as new metadata files are created; it’s explicitly modeled (and can be truncated by table properties / maintenance policies depending on engine). ([py.iceberg.apache.org][14])
* Partition specs used for *writing* are not necessarily the same specs used for *reading*; reads use specs stored in manifests (older specs remain relevant). ([py.iceberg.apache.org][14])

### Footguns

* **Field IDs are not optional**: using Arrow schemas without proper Iceberg field IDs can cause downstream confusion when building partition specs or evolving schemas; there are real-world reports of `field_id = -1` with Arrow-derived schemas causing partitioning issues. ([GitHub][15])
* Schema evolution + partitioned reads can trigger “field id not found” failures in some edge cases (e.g., projected schema missing partition field while reading older partition files). ([GitHub][16])

### Minimal patterns

* Treat “field IDs and spec IDs” as durable join keys in your own metadata layer; do not key internal mappings purely by name (names change). ([py.iceberg.apache.org][5])

---

## C) Scan planning & execution (partition + metrics + delete files) — deep dive

### Mental model

A `DataScan` is a **planner + executor façade**:

* It plans **which files to read** (data + delete files) by walking snapshot metadata + manifests.
* It computes a **residual filter** per file (what wasn’t proven by partition/metrics pruning).
* Then it materializes via `ArrowScan` (`to_table` / `to_record_batches`). ([py.iceberg.apache.org][13])

### Full surface area

**Planning**

* `DataScan.scan_plan_helper() -> Iterator[list[ManifestEntry]]`:

  1. `snapshot.manifests(io)`
  2. filter manifests using a per-spec **manifest evaluator** built from partition summaries
  3. open manifests in parallel and filter entries using **partition evaluator** + **metrics evaluator** ([py.iceberg.apache.org][1])
* `DataScan.plan_files() -> Iterable[FileScanTask]`:

  * partitions entries by `DataFileContent` (DATA vs POSITION_DELETES vs EQUALITY_DELETES)
  * matches positional deletes to each data file
  * computes residual expression per data file partition
  * hard-errors on equality deletes (currently unsupported) ([py.iceberg.apache.org][1])

**Materialization**

* `to_arrow()` uses `ArrowScan(...).to_table(self.plan_files())` (eager). ([py.iceberg.apache.org][1])
* `to_arrow_batch_reader()` builds a target schema from the projection, streams record batches from `ArrowScan.to_record_batches(plan_files)`, then constructs a `RecordBatchReader.from_batches(...).cast(target_schema)`. ([py.iceberg.apache.org][1])

### Invariants

* Planning is spec-aware: partition filters and manifest evaluators are built **per `spec_id`** because manifest partition summaries are defined relative to the spec that wrote them. ([py.iceberg.apache.org][10])
* Planning explicitly anticipates concurrency: partition evaluator creation avoids sharing a bound evaluator instance across threads; manifest entry opening is executed through an executor map. ([py.iceberg.apache.org][1])

### Footguns

* **Equality deletes**: `plan_files()` raises `ValueError("does not yet support equality deletes")`. If you ingest tables that produce equality delete files, reads will fail. ([py.iceberg.apache.org][2])
* `to_arrow_batch_reader()` vs `to_arrow()` schema mismatches have been reported (e.g., strings widened to `large_string`), which matters if you’re doing schema-sensitive downstream conversions. ([GitHub][17])
* Metadata costs can dominate “small scans”; users report nontrivial overhead in reading metadata/manifests for operations like `inspect.manifests().to_pandas()`. ([iceberg.apache.org][18])

### Minimal patterns

```python
scan = table.scan(row_filter='city = "Paris"', selected_fields=("city", "inhabitants"))
tasks = list(scan.plan_files())                 # plan only
rb = scan.to_arrow_batch_reader()               # execute streaming
```

([py.iceberg.apache.org][1])

---

## D) Writes, commits, and snapshot semantics — deep dive

### Mental model

Writes are **metadata commits** that produce **new snapshots**; PyIceberg’s public write APIs are Arrow-first and route through `Transaction` under the hood. ([py.iceberg.apache.org][1])

### Full surface area

* `Table.transaction() -> Transaction` (staged), plus shorthand APIs that open a transaction context and invoke the corresponding transaction operation:

  * `append`, `overwrite`, `dynamic_partition_overwrite`, `delete`, `add_files`, `upsert`. ([py.iceberg.apache.org][1])
* `Transaction.commit_transaction() -> Table` commits updates + requirements to catalog; create-table transaction has a special “AssertCreate-only” requirement shape. ([py.iceberg.apache.org][1])

### Invariants

* `overwrite` semantics can produce different snapshot operations (“append”, “overwrite”, “replace”, etc.) depending on whether existing files can be dropped vs rewritten vs appended. The behavior is documented in the `overwrite` docstring. ([py.iceberg.apache.org][1])
* `upsert` defaults to using Iceberg **identifier field IDs** if `join_cols` are not specified (i.e., it’s metadata-driven, not name-driven). ([py.iceberg.apache.org][1])

### Footguns

* `dynamic_partition_overwrite` is not a “merge”: it (1) detects partition values in incoming Arrow table using current partition spec, (2) deletes matching partitions, then (3) appends new data. If your incoming table lacks the partition fields or has mismatched partition typing, you’ll get surprising results. ([py.iceberg.apache.org][10])
* `upsert` can be memory-heavy on large tables (community reports) and is not a streaming merge in the SQL-engine sense. ([py.iceberg.apache.org][1])

### Minimal patterns

```python
# “snapshot_properties” lets you stamp custom metadata on the resulting snapshot
table.append(df, snapshot_properties={"producer": "etl_job_17"})
```

([py.iceberg.apache.org][1])

---

## E) Schema evolution & name mapping — deep dive

### Mental model

Schema evolution is **field-id driven**. Updates are staged on an `UpdateSchema` builder and committed (often via context manager). The builder enforces rules like “no ambiguous dotted names for nested adds” and may **reassign nested-type field IDs** when you add a nested type into an existing schema.

### Full surface area

* `Table.update_schema(allow_incompatible_changes=False, case_sensitive=True) -> UpdateSchema` (wrapper that creates an autocommit transaction and attaches name mapping). ([py.iceberg.apache.org][1])
* `UpdateSchema`:

  * `case_sensitive(bool) -> UpdateSchema`
  * `union_by_name(new_schema: Schema|pa.Schema, format_version: TableVersion=2) -> UpdateSchema`
  * `add_column(path: str|tuple[str,...], field_type: IcebergType, doc=None, required=False, default_value=None) -> UpdateSchema`
  * `rename_column(path_from: str|tuple[str,...], new_name: str) -> UpdateSchema` ([py.iceberg.apache.org][7])

### Invariants

* `add_column` rejects ambiguous dotted strings and forces tuples when needed; this is a deliberate correctness guard. ([py.iceberg.apache.org][7])
* Adding a nested type triggers ID reassignment for the nested structure’s fields (so you can’t “import” nested IDs wholesale by default). ([GitHub][15])

### Footguns

* Some schema-update sequences have known edge cases:

  * adding a parent struct and a nested child field in the same transaction has been reported as problematic. ([GitHub][19])
  * Glue-backed catalogs may show eventual-consistency behavior across back-to-back operations (e.g., rename then move) in some reports. ([GitHub][20])
* Creating tables from schemas with pre-set field IDs has historically been a friction point (discussion about `create_table` ignoring provided IDs). Treat “input field IDs” as potentially non-authoritative unless you’ve validated in your exact version/catalog. ([GitHub][15])

### Minimal patterns

```python
with table.update_schema() as u:
    u.add_column(("details", "confirmed_by"), StringType(), doc="Approver")
    u.rename_column("old_name", "new_name")
```

Tuple paths avoid the dotted-name ambiguity rules. ([py.iceberg.apache.org][7])

---

## F) Partitioning, transforms, and sort order — deep dive

### Mental model

Partitioning = `PartitionSpec` (spec id + partition fields). Each `PartitionField` maps a source column to a derived partition value via a `Transform` (bucket/day/month/etc.). Spec evolution creates a new `spec_id`. ([py.iceberg.apache.org][10])

Sort order = `SortOrder` + `SortField` list; `UpdateSortOrder` translates “column name → field id” and builds/sets a sort order (including reusing an existing identical sort order id).

### Full surface area

* Partition model: `PartitionSpec(spec_id, fields)` where `spec_id` changes on evolution. ([py.iceberg.apache.org][10])
* Transform example: `BucketTransform(num_buckets)` defines bucket hashing semantics. ([py.iceberg.apache.org][11])
* Partition evolution via `table.update_spec()` exists; the rendered code-reference page for `update/spec` is currently empty in the docs build, but the API guide documents `add_field(...)` and `remove_field(...)` usage. ([py.iceberg.apache.org][21])
* Sort evolution:

  * `Table.update_sort_order(case_sensitive=True) -> UpdateSortOrder` ([py.iceberg.apache.org][1])
  * `UpdateSortOrder.asc(name, transform, null_order=NULLS_LAST)` / `desc(...)`

### Invariants

* Scan planning is spec-aware: it builds partition projections and evaluators per `spec_id` and uses manifest partition summaries first (coarse prune), then per-entry partition + metrics evaluation (fine prune). ([py.iceberg.apache.org][10])
* `UpdateSortOrder` resolves field IDs by looking up the column in the current schema (case-sensitive optional).

### Footguns

* Removing and re-adding the “same” partition field can behave differently depending on whether you explicitly pin the partition field name; an issue report shows failures unless the partition field name is provided explicitly.
* Sort order updates require a **Transform** argument on `asc/desc` (it’s not implicitly identity in the signature), which is easy to miss if you expect a default transform.

### Minimal patterns

```python
from pyiceberg.transforms import BucketTransform, IdentityTransform

with table.update_sort_order() as s:
    s.asc("id", IdentityTransform())
    s.desc("event_ts", IdentityTransform())
```

([py.iceberg.apache.org][1])

---

## G) `table.inspect.*` metadata tables — deep dive

### Mental model

`InspectTable` materializes Iceberg metadata into **Arrow tables** (snapshots, partitions, entries, manifests, refs, files, history, metadata log…). Most are time-travel capable via `snapshot_id`, except `snapshots` and `refs`. ([py.iceberg.apache.org][2])

### Full surface area (documented)

* `table.inspect.snapshots()` → snapshots table (committed_at, snapshot_id, operation, manifest_list, summary, …) ([py.iceberg.apache.org][2])
* `table.inspect.partitions()` → partition aggregates (record_count, file_count, delete counts, last_updated…) ([py.iceberg.apache.org][2])
* `table.inspect.entries(snapshot_id=...)` → manifest entries (data_file struct includes metrics maps, bounds, equality_ids…) ([py.iceberg.apache.org][2])
* `table.inspect.refs()` → refs (branch/tag metadata like retention hints) ([py.iceberg.apache.org][2])
* `table.inspect.manifests()` → manifest list view ([py.iceberg.apache.org][2])
* `table.inspect.metadata_log_entries()`, `table.inspect.history()`, `table.inspect.files()` (+ `data_files()` / `delete_files()` helpers) ([py.iceberg.apache.org][2])

### Invariants

* The `files()` view includes `content` codes (0=data, 1=position deletes, 2=equality deletes) and the API guide explicitly calls out the meaning and helper filters. ([py.iceberg.apache.org][2])

### Footguns

* `.files()` can be slow for large tables because it scans manifests; there’s a feature request discussing sequential processing cost and parallelization ideas. ([py.iceberg.apache.org][2])

### Minimal patterns

```python
snapshots = table.inspect.snapshots()
entries_t = table.inspect.entries(snapshot_id=some_snapshot_id)  # time travel
files_t   = table.inspect.files()
```

([py.iceberg.apache.org][2])

---

## H) Maintenance (snapshot expiration) — deep dive

### Mental model

Maintenance is exposed via `table.maintenance`, currently spotlighting **snapshot expiration** as a builder (`ExpireSnapshots`) that stages changes and applies on commit. ([py.iceberg.apache.org][4])

### Full surface area

* `ExpireSnapshots` builder (expire snapshots by ID; staged ops apply on commit).

### Footguns

* There are reports of snapshot-expiration commit confusion in threaded scenarios (commit targeting the wrong table), so treat “maintenance in threads” as something to validate in your own harness.

---

## I) Engine interop patterns — deep dive

### Mental model

Two integration tiers:

1. **PyIceberg executes** → returns Arrow/Pandas/Polars/Ray (via `DataScan.*`), easiest but may be eager.
2. **Engine-native lazy**: Polars lazy scan (`Table.to_polars() -> LazyFrame`) or DataFusion provider capsule for pushdown. ([py.iceberg.apache.org][1])

### Full surface area

* Eager: `DataScan.to_arrow / to_pandas / to_polars / to_duckdb / to_ray` (note: `to_duckdb` registers Arrow in an in-memory DuckDB connection). ([py.iceberg.apache.org][1])
* Streaming: `DataScan.to_arrow_batch_reader()` with schema cast.
* Lazy:

  * `Table.to_polars() -> pl.LazyFrame` uses `pl.scan_iceberg(self)` ([py.iceberg.apache.org][1])
  * `Table.__datafusion_table_provider__()` returns DataFusion provider capsule. ([py.iceberg.apache.org][4])

### Footguns

* `to_duckdb`’s default is `duckdb.connect(database=":memory:")`; if you expect persistence or cross-process reuse, pass your own connection / database. ([py.iceberg.apache.org][1])

---

If you want the *next* increment after this, the natural continuation is to turn each deep-dive section into a **“micro-playbook”**: (a) invariants you should encode as assertions/tests, (b) minimal “golden patterns” for agents (create → evolve schema → write → inspect → time travel), and (c) a “compat/perf checklist” (metadata IO costs, delete-file support, schema/ID pitfalls) keyed to the exact PyIceberg versions you’ll support.

[1]: https://py.iceberg.apache.org/reference/pyiceberg/table/ "table - PyIceberg"
[2]: https://py.iceberg.apache.org/api/ "API - PyIceberg"
[3]: https://py.iceberg.apache.org/reference/pyiceberg/catalog/ "catalog - PyIceberg"
[4]: https://py.iceberg.apache.org/reference/pyiceberg/table/?utm_source=chatgpt.com "table"
[5]: https://py.iceberg.apache.org/reference/pyiceberg/table/update/ "update - PyIceberg"
[6]: https://py.iceberg.apache.org/?utm_source=chatgpt.com "PyIceberg"
[7]: https://py.iceberg.apache.org/reference/pyiceberg/table/update/schema/?utm_source=chatgpt.com "schema"
[8]: https://py.iceberg.apache.org/reference/pyiceberg/table/update/sorting/ "sorting - PyIceberg"
[9]: https://py.iceberg.apache.org/reference/pyiceberg/expressions/?utm_source=chatgpt.com "expressions"
[10]: https://py.iceberg.apache.org/reference/pyiceberg/partitioning/?utm_source=chatgpt.com "partitioning"
[11]: https://py.iceberg.apache.org/reference/pyiceberg/transforms/?utm_source=chatgpt.com "transforms"
[12]: https://py.iceberg.apache.org/reference/pyiceberg/table/sorting/?utm_source=chatgpt.com "sorting"
[13]: https://py.iceberg.apache.org/reference/pyiceberg/io/pyarrow/?utm_source=chatgpt.com "pyarrow - PyIceberg"
[14]: https://py.iceberg.apache.org/reference/pyiceberg/table/metadata/?utm_source=chatgpt.com "metadata"
[15]: https://github.com/apache/iceberg-python/issues/1284?utm_source=chatgpt.com "Enhance `catalog.create_table` API to enable creation of ..."
[16]: https://github.com/apache/iceberg-python/issues/2672?utm_source=chatgpt.com "ValueError when reading partitioned tables with column ..."
[17]: https://github.com/apache/iceberg-python/issues/2250?utm_source=chatgpt.com "`to_arrow_batch_reader` returns a different schema than ` ..."
[18]: https://iceberg.apache.org/spec/?utm_source=chatgpt.com "Spec - Apache Iceberg™"
[19]: https://github.com/apache/iceberg-python/issues/1493?utm_source=chatgpt.com "Issue #1493 · apache/iceberg-python"
[20]: https://github.com/apache/iceberg-python/issues/2599?utm_source=chatgpt.com "Issue #2599 · apache/iceberg-python"
[21]: https://py.iceberg.apache.org/api/?utm_source=chatgpt.com "Python API"


Below is the **“micro-playbook pack”** for **PyIceberg 0.10.0** (current PyPI latest, released Sep 11, 2025). ([PyPI][1])
It’s structured so an LLM coding agent can **mechanically** implement tests + “golden workflows” without having to reason about Iceberg internals.

---

# Version policy + gating (pin what the guide guarantees)

## Supported baseline

* **Supported:** `pyiceberg==0.10.0` (docs at `py.iceberg.apache.org` are built around this tag). ([PyPI][1])
* **Python:** `>=3.9` (per PyPI metadata). ([PyPI][1])

## Version gate pattern (tests + runtime)

```python
from importlib.metadata import version
assert version("pyiceberg") == "0.10.0"
```

---

# Shared test harness building blocks (use everywhere)

## Catalog fixture: prefer **in-memory catalog** for unit tests

* PyIceberg’s **in-memory catalog** is built on `SqlCatalog` using SQLite in-memory DB; **not production**; **not concurrent**, but ideal for tests. ([PyIceberg][2])
* Config can be injected via API props (no `.pyiceberg.yaml` required). ([PyIceberg][3])

```python
from pyiceberg.catalog import load_catalog

def make_test_catalog(tmp_path):
    # Explicit FileIO avoids “first installed wins” scheme inference surprises.
    return load_catalog(
        "test",
        **{
            "type": "in-memory",
            "warehouse": str(tmp_path / "warehouse"),
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )
```

Why explicit FileIO?

* Default FileIO is selected by URI scheme and “first installed wins”; you can override via `py-io-impl`. ([PyIceberg][2])

## Integration fixture: SQL catalog with PostgreSQL (not SQLite) if you need concurrency realism

* SQLite is explicitly “development only” and “not built for concurrency.” ([PyIceberg][2])
* SQL catalog config uses `type: sql` + `uri: postgresql+psycopg2://...`. ([PyIceberg][2])

---

# Micro-playbook 1 — Catalogs & configuration

## A) Invariants → encode as assertions/tests

1. **Catalog-centric contract:** reads/writes go via a catalog; `StaticTable` is read-only. ([PyIceberg][3])

   * **Test:** writing via `StaticTable` raises (any mutation API should fail).
2. **Config location / injection:** `.pyiceberg.yaml` search locations and API injection are stable. ([PyIceberg][3])

   * **Test:** `load_catalog(..., **props)` works without `.pyiceberg.yaml` (smoke).
3. **Identifier tuple safety:** tuple identifier form is required when namespace/table contains dots. ([PyIceberg][3])

   * **Test:** create a namespace name containing `.` and verify tuple form is accepted.

## B) Golden patterns (agent workflow)

**Pattern: “catalog bootstrap → namespace create → table create”**

```python
catalog = make_test_catalog(tmp_path)
catalog.create_namespace("ns")

# Use tuple form if names contain dots:
# catalog.create_namespace(("ns.with.dot",))
```

## C) Compat/perf checklist (0.10.0)

* **FileIO selection:** default chooses first installed implementation per scheme; set `py-io-impl` explicitly in production and tests to avoid silent drift. ([PyIceberg][2])
* **SQL catalog:** don’t use SQLite for concurrent tests/services. ([PyIceberg][2])
* **In-memory catalog:** not concurrent; unit tests only. ([PyIceberg][2])

---

# Micro-playbook 2 — Table metadata, immutability, and “IDs matter”

## A) Invariants → encode as assertions/tests

1. **Create-table reassigns field IDs for uniqueness.** ([PyIceberg][3])

   * **Test:** create a schema with intentionally duplicated IDs; assert resulting `table.schema()` has unique IDs (and does *not* preserve your duplicates).
2. **Catalog table vs StaticTable:** catalog load returns a mutable `Table`; metadata-path load returns read-only `StaticTable`. ([PyIceberg][3])

   * **Test:** `StaticTable.from_metadata(...).append(...)` fails.
3. **Prefer transactions to avoid manual field-ID handling** when setting partition spec / sort order on create. ([PyIceberg][3])

   * **Test:** `create_table_transaction(...).update_spec().add_identity("col")` works without manually specifying IDs.

## B) Golden patterns

**Pattern: “create via transaction (ID-safe)”**

```python
import pyarrow as pa

schema = pa.schema([pa.field("id", pa.int64()), pa.field("ts", pa.timestamp("us"))])

with catalog.create_table_transaction("ns.tbl", schema=schema) as txn:
    with txn.update_spec() as spec:
        spec.identity("id")
    txn.set_properties(owner="ci", purpose="tests")
```

This is explicitly positioned as the “friendly API” because you don’t have to manage field IDs. ([PyIceberg][3])

## C) Compat/perf checklist (0.10.0)

* Treat **field IDs** as the durable identity; don’t key internal logic purely on names (names can change; IDs persist). (Design invariant reinforced by schema evolution + add_files behavior.) ([PyIceberg][3])

---

# Micro-playbook 3 — Scan planning (partition + metrics + deletes) and materialization

## A) Invariants → encode as assertions/tests

1. `plan_files()` returns `FileScanTask`s that include both **data and delete files** and computes a **residual** expression per data file. ([PyIceberg][4])

   * **Test:** `list(table.scan().plan_files())` yields tasks; each task has `.file` and `.residual`.
2. **Equality deletes hard-fail in 0.10.0 scan planning** (explicit `ValueError`). ([PyIceberg][4])

   * **Test:** keep a “known table with equality deletes” fixture; assert read fails with that error string.
3. **Eager vs streaming memory behavior:**

   * `to_arrow()` loads all rows at once. ([PyIceberg][4])
   * `to_arrow_batch_reader()` is explicitly lower-memory streaming. ([PyIceberg][4])
   * **Test:** `to_arrow()` and reading all batches from `to_arrow_batch_reader()` produce identical row counts and equal data for small fixtures.

## B) Golden patterns

**Pattern: “plan-only then execute”**

```python
scan = table.scan(row_filter='id >= 0', selected_fields=("id", "ts"))

tasks = list(scan.plan_files())       # planning step (metadata heavy)
rb = scan.to_arrow_batch_reader()     # execution step (streaming)
```

## C) Compat/perf checklist (0.10.0)

* **Delete-file compatibility hazard:** equality deletes are a hard stop. ([PyIceberg][4])
* **Metadata IO dominates small scans:** plan step walks manifests; keep unit tests tiny; treat `inspect.*` and scan planning as “metadata IO” workloads.

---

# Micro-playbook 4 — Writes, commits, snapshots (append/overwrite/dynamic overwrite)

## A) Invariants → encode as assertions/tests

1. **Snapshot properties:** `append(..., snapshot_properties=...)` and `overwrite(..., snapshot_properties=...)` persist into the latest snapshot summary. ([PyIceberg][3])

   * **Test:** after write, assert `table.metadata.snapshots[-1].summary["k"] == "v"`. ([PyIceberg][3])
2. **dynamic_partition_overwrite semantics are deterministic:**

   * detects partition values in incoming Arrow table using the **current partition spec**
   * deletes matching partitions
   * appends incoming data ([PyIceberg][4])
   * **Test:** create table partitioned by day(ts). Append day1+day2. Then dynamic overwrite day1 only. Assert day2 unchanged and day1 replaced.
3. **Manifest merge default behavior:** PyIceberg defaults to “fast append” and `commit.manifest-merge.enabled=False` by default. ([PyIceberg][2])

   * **Test:** repeated small appends produce increasing manifest count unless you enable merge.

## B) Golden patterns

**Pattern: “append → partial overwrite → dynamic partition overwrite”**

```python
import pyarrow as pa

tbl.append(pa.table({"id":[1,2], "ts":[...]}), snapshot_properties={"stage":"seed"})

# filtered overwrite (copy-on-write semantics depend on engine behavior)
tbl.overwrite(
    pa.table({"id":[2], "ts":[...]}),
    overwrite_filter="id = 2",
    snapshot_properties={"stage":"overwrite_id_2"},
)

# replace partitions touched by input
tbl.dynamic_partition_overwrite(
    pa.table({"id":[1], "ts":[...]}),
    snapshot_properties={"stage":"dyn_part_overwrite"},
)
```

## C) Compat/perf checklist (0.10.0)

* If you want fewer manifests, enable manifest merge and tune:

  * `commit.manifest-merge.enabled`, `commit.manifest.target-size-bytes`, `commit.manifest.min-count-to-merge`. ([PyIceberg][2])
* Metadata bloat control:

  * `write.metadata.delete-after-commit.enabled` + `write.metadata.previous-versions-max`. ([PyIceberg][2])

---

# Micro-playbook 5 — Schema evolution (safe-by-default) + partition evolution + sort order rules

## A) Invariants → encode as assertions/tests

1. **Schema evolution is safe-by-default:** only non-breaking changes allowed unless `allow_incompatible_changes=True`. ([PyIceberg][3])

   * **Test:** require an optional field (incompatible) fails unless `allow_incompatible_changes=True`. ([PyIceberg][3])
2. **Complex-type rule:** a complex type must exist before adding nested fields; nested paths are tuples. ([PyIceberg][3])

   * **Test:** `add_column(("details","x"), ...)` fails if `details` struct doesn’t exist; succeeds after adding `details` struct.
3. **Partition evolution supported:** update_spec supports `add_field`, `remove_field`, `rename_field`, plus identity shortcut. ([PyIceberg][3])
4. **Sort order updates are append-only:** “can only be updated by adding a new sort order; cannot be deleted or modified.” ([PyIceberg][3])

   * **Test:** successive `update_sort_order()` produces increasing sort-order IDs; table should retain older sort orders.

## B) Golden patterns

**Pattern: “evolve schema + evolve partitions + add a sort order”**

```python
from pyiceberg.types import IntegerType, StructType, StringType
from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform
from pyiceberg.table.sorting import NullOrder

with tbl.update_schema() as u:
    u.add_column("retries", IntegerType(), "Number of retries")
    u.add_column("details", StructType())
with tbl.update_schema() as u:
    u.add_column(("details", "confirmed_by"), StringType(), "Processor")

with tbl.update_spec() as p:
    p.add_field("id", BucketTransform(16), "bucketed_id")
    p.add_field("ts", DayTransform(), "day_ts")
    p.identity("retries")

with tbl.update_sort_order() as s:
    s.desc("ts", DayTransform(), NullOrder.NULLS_FIRST)
    s.asc("id", IdentityTransform(), NullOrder.NULLS_LAST)
```

All of these APIs and constraints are explicitly documented. ([PyIceberg][3])

## C) Compat/perf checklist (0.10.0)

* Prefer `union_by_name()` when merging external schemas: avoids field-ID handling and produces stable, deterministic field IDs in resulting table schema. ([PyIceberg][3])

---

# Micro-playbook 6 — `add_files` (field IDs, name mapping, duplicate protection)

## A) Invariants → encode as assertions/tests

1. **Field IDs / name mapping:**

   * If Parquet files have field IDs, they must match table field IDs.
   * If missing field IDs, table needs a **Name Mapping**; PyIceberg auto-creates one if absent. ([PyIceberg][3])
   * **Test:** after `add_files(...)`, `assert tbl.name_mapping() is not None` (this is in the docs). ([PyIceberg][3])
2. **Duplicate check default True:** prevents accidental duplication but can be expensive on tables with many files; disabling increases corruption risk. ([PyIceberg][3])

   * **Test:** calling `add_files` twice with the same file errors when `check_duplicate_files=True`.

## B) Golden patterns

```python
tbl.add_files(
    file_paths=["s3a://warehouse/default/existing-1.parquet"],
    snapshot_properties={"source":"bootstrap"},
    check_duplicate_files=True,
)
```

([PyIceberg][3])

## C) Compat/perf checklist (0.10.0)

* `check_duplicate_files=True` is the “safe default”; only disable if you enforce uniqueness elsewhere. ([PyIceberg][3])

---

# Micro-playbook 7 — Inspect + time travel (snapshots/entries/files/refs) and ref-based access

## A) Invariants → encode as assertions/tests

1. Inspect metadata tables report **content types** and support helper methods (`data_files()`, `delete_files()`). ([PyIceberg][3])

   * **Test:** after writes, `table.inspect.files()` includes content codes and expected counts.
2. **Snapshot management is commit-applied:** `.manage_snapshots().create_tag(...).commit()` applies operations on commit. ([PyIceberg][3])

   * **Test:** after commit, `table.inspect.refs()` includes `tag123`.

## B) Golden patterns (the “agent happy path”)

**End-to-end: create → evolve schema → write → inspect → time travel**

```python
# 1) create + load
catalog.create_namespace("ns")
tbl = catalog.create_table("ns.tbl", schema=arrow_schema)

# 2) write
tbl.append(df, snapshot_properties={"stage":"seed"})

# 3) inspect snapshots
snaps = tbl.inspect.snapshots()        # Arrow table of snapshots

# 4) evolve schema
with tbl.update_schema() as u:
    u.add_column("new_col", "string")

# 5) write again
tbl.append(df2, snapshot_properties={"stage":"post_schema"})

# 6) time travel by snapshot_id (picked from inspect.snapshots)
tbl_tt = catalog.load_table("ns.tbl")
old_snapshot_id = int(snaps.column("snapshot_id")[0].as_py())
old = tbl_tt.scan(snapshot_id=old_snapshot_id).to_arrow()
```

Catalog-centric workflow + snapshot/tag semantics are in the API docs. ([PyIceberg][3])

## C) Compat/perf checklist (0.10.0)

* Treat `inspect.*` as **metadata IO heavy**; keep test tables tiny; in perf suites measure “inspect latency” separately from “data scan latency”.

---

# Micro-playbook 8 — Maintenance hygiene (metadata retention, manifest merge, expiration)

## A) Invariants → encode as assertions/tests

1. **Metadata retention knobs exist and are deterministic**:

   * `write.metadata.previous-versions-max`
   * `write.metadata.delete-after-commit.enabled` ([PyIceberg][2])
   * **Test:** set `delete-after-commit.enabled=True`, run many commits, and assert older metadata versions are pruned (inspect filesystem under metadata path).
2. **Manifest merge is off by default** due to fast-append default; enabling changes manifest count and commit overhead. ([PyIceberg][2])

   * **Test:** compare manifest count growth with merge enabled vs disabled across N appends.

## B) Golden patterns

* For “production-ish” tables with frequent writes:

  * enable metadata deletion after commit
  * consider manifest merge after threshold

## C) Compat/perf checklist (0.10.0)

* If you run many small appends: expect manifest explosion unless you tune merge. ([PyIceberg][2])

---

# Micro-playbook 9 — Engine interop (Arrow/Pandas/DuckDB/Polars/Ray/DataFusion)

## A) Invariants → encode as assertions/tests

1. **Streaming read exists:** `to_arrow_batch_reader()` is the low-memory path. ([PyIceberg][4])
2. **DuckDB interop exists:** `to_duckdb(table_name, connection=None)` registers scan results for SQL. (Validate you’re not accidentally using `:memory:` when you expect persistence.)
3. **Polars lazy path exists** (preferred for pushdown) in the API docs “Query the data → Polars.” ([PyIceberg][3])

## B) Golden patterns

* Small: `scan.to_arrow().to_pandas()`
* Big: `scan.to_arrow_batch_reader()` then stream into downstream pipeline
* SQL: `scan.to_duckdb("t").sql("select ...")`

## C) Compat/perf checklist (0.10.0)

* **Data file formats:** official status indicates PyIceberg supports **Parquet** data files; **ORC/Avro data files not supported** (so tables written with those formats won’t read via PyIceberg). ([Apache Iceberg][5])
* **Delete files:** equality delete reads are not supported (hard error in scan planning). ([PyIceberg][4])

---

## If you want one more increment after this

I can convert these playbooks into a **repo-ready pytest suite outline**:

* `tests/iceberg/test_catalog_contract.py`
* `tests/iceberg/test_schema_evolution_contract.py`
* `tests/iceberg/test_scan_planning_contract.py`
* `tests/iceberg/test_delete_file_compat.py`
  …with explicit fixtures (in-memory vs sql-postgres), deterministic tiny Arrow datasets, and “expected snapshot/manifest counts” assertions tied to the config knobs above.

[1]: https://pypi.org/project/pyiceberg/ "pyiceberg · PyPI"
[2]: https://py.iceberg.apache.org/configuration/ "Configuration - PyIceberg"
[3]: https://py.iceberg.apache.org/api/ "API - PyIceberg"
[4]: https://py.iceberg.apache.org/reference/pyiceberg/table/ "table - PyIceberg"
[5]: https://iceberg.apache.org/status/ "Implementation Status - Apache Iceberg™"


Below is a **repo-ready pytest suite outline** (copy/pasteable) that implements the contracts we’ve been cataloging, with:

* **explicit fixtures** for `in-memory` and optional **SQL/Postgres** catalogs
* **deterministic tiny Arrow datasets**
* **snapshot + manifest count assertions** tied to PyIceberg table property knobs (fast-append vs manifest merge, metadata retention) ([PyIceberg][1])

It is anchored to the PyIceberg **Python API + configuration docs** (notably: catalog-centric usage, tuple identifiers for dotted names, `create_table_transaction`, partition evolution APIs, sort order append-only rule, snapshot properties, FileIO selection, and table property keys). ([PyIceberg][2])

---

## Directory layout

```text
tests/
  iceberg/
    conftest.py
    _util.py
    test_catalog_contract.py
    test_schema_evolution_contract.py
    test_scan_planning_contract.py
    test_delete_file_compat.py
pytest.ini
```

---

## `pytest.ini`

```ini
[pytest]
markers =
  iceberg: iceberg contract tests
  iceberg_postgres: tests that require a live Postgres-backed SqlCatalog
  iceberg_external_fixture: tests that require externally-produced fixture tables (e.g. equality deletes)
addopts = -q
```

---

## `tests/iceberg/_util.py`

```python
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import pyarrow as pa


def stable_suffix(nodeid: str, n: int = 10) -> str:
    """Stable per-test suffix (deterministic across runs)."""
    return hashlib.sha1(nodeid.encode("utf-8")).hexdigest()[:n]


def file_uri(path: Path) -> str:
    # Path.as_uri() yields file:///... (what PyIceberg docs use heavily)
    return path.resolve().as_uri()


def path_from_uri(uri: str) -> Path:
    parsed = urlparse(uri)
    # For file:/// URIs, the path is in parsed.path
    return Path(parsed.path)


@dataclass(frozen=True)
class TinyDataset:
    schema: pa.Schema
    rows_a: pa.Table
    rows_b: pa.Table


def tiny_dataset() -> TinyDataset:
    # Small, deterministic schema
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("city", pa.string(), nullable=True),
            pa.field("lat", pa.float64(), nullable=True),
            pa.field("lon", pa.float64(), nullable=True),
        ]
    )

    rows_a = pa.Table.from_pylist(
        [
            {"id": 1, "city": "Amsterdam", "lat": 52.371807, "lon": 4.896029},
            {"id": 2, "city": "San Francisco", "lat": 37.773972, "lon": -122.431297},
        ],
        schema=schema,
    )
    rows_b = pa.Table.from_pylist(
        [
            {"id": 3, "city": "Drachten", "lat": 53.11254, "lon": 6.0989},
            {"id": 4, "city": "Paris", "lat": 48.864716, "lon": 2.349014},
        ],
        schema=schema,
    )
    return TinyDataset(schema=schema, rows_a=rows_a, rows_b=rows_b)


def collect_record_batches(reader: "pa.RecordBatchReader") -> pa.Table:
    batches = list(reader)
    return pa.Table.from_batches(batches, schema=reader.schema)


def arrow_equals(a: pa.Table, b: pa.Table) -> bool:
    # Deterministic equality for tiny fixtures
    if a.schema != b.schema:
        return False
    return a.to_pylist() == b.to_pylist()
```

---

## `tests/iceberg/conftest.py`

```python
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterator, Literal, Tuple

import pytest
import pyarrow as pa

from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table

from ._util import TinyDataset, file_uri, stable_suffix, tiny_dataset


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--iceberg-pg",
        action="store_true",
        default=False,
        help="Enable Postgres-backed SqlCatalog runs (requires ICEBERG_TEST_PG_URI).",
    )
    parser.addoption(
        "--iceberg-strict-counts",
        action="store_true",
        default=False,
        help="Enable strict manifest/metadata file count assertions (can be flaky across engines/versions).",
    )


CatalogFlavor = Literal["inmem", "pg"]


@pytest.fixture(scope="session")
def ds() -> TinyDataset:
    return tiny_dataset()


@pytest.fixture(params=["inmem", "pg"])
def iceberg_catalog(
    request: pytest.FixtureRequest,
    tmp_path: Path,
) -> Iterator[Tuple[CatalogFlavor, object, str]]:
    """
    Yields: (flavor, catalog, warehouse_uri)

    - inmem: always enabled (unit tests)
    - pg: enabled only if --iceberg-pg and ICEBERG_TEST_PG_URI are provided
    """
    flavor: CatalogFlavor = request.param

    warehouse = tmp_path / f"warehouse_{flavor}"
    warehouse_uri = file_uri(warehouse)

    if flavor == "inmem":
        catalog = load_catalog(
            "test",
            **{
                "type": "in-memory",
                "warehouse": warehouse_uri,
                # FileIO selection is scheme-driven by default; be explicit for determinism.
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            },
        )
        yield flavor, catalog, warehouse_uri
        return

    # Postgres-backed SqlCatalog (integration)
    if not request.config.getoption("--iceberg-pg"):
        pytest.skip("Postgres catalog disabled (enable with --iceberg-pg).")

    pg_uri = os.environ.get("ICEBERG_TEST_PG_URI")
    if not pg_uri:
        pytest.skip("ICEBERG_TEST_PG_URI not set.")

    catalog = load_catalog(
        "test_pg",
        **{
            "type": "sql",
            "uri": pg_uri,
            "warehouse": warehouse_uri,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )
    yield flavor, catalog, warehouse_uri


@pytest.fixture
def iceberg_ident(request: pytest.FixtureRequest) -> Tuple[str, str]:
    """
    Returns (namespace, identifier_str) deterministic for a given test nodeid.
    """
    suf = stable_suffix(request.node.nodeid)
    namespace = f"ns_{suf}"
    identifier = f"{namespace}.tbl_{suf}"
    return namespace, identifier


@pytest.fixture
def iceberg_table(
    iceberg_catalog: Tuple[CatalogFlavor, object, str],
    iceberg_ident: Tuple[str, str],
    ds: TinyDataset,
) -> Table:
    """
    Creates a fresh table per test using the tiny Arrow schema.
    """
    _flavor, catalog, _warehouse_uri = iceberg_catalog
    namespace, identifier = iceberg_ident

    catalog.create_namespace(namespace)
    tbl = catalog.create_table(identifier=identifier, schema=ds.schema)
    return tbl
```

Key design choices:

* The fixtures use `file:///...` URIs and set `py-io-impl` explicitly to avoid scheme-based auto-selection drift. ([PyIceberg][1])
* Postgres tests are opt-in (`--iceberg-pg`) and gated by `ICEBERG_TEST_PG_URI`.

---

# `tests/iceberg/test_catalog_contract.py`

```python
from __future__ import annotations

import pytest
import pyarrow as pa

from pyiceberg.table import StaticTable

from ._util import file_uri


pytestmark = pytest.mark.iceberg


def test_load_table_string_and_tuple_equivalent(iceberg_catalog, ds):
    _flavor, catalog, _wh = iceberg_catalog

    ns = "docs_example"
    catalog.create_namespace(ns)

    ident_str = f"{ns}.bids"
    catalog.create_table(identifier=ident_str, schema=ds.schema)

    t1 = catalog.load_table(ident_str)
    t2 = catalog.load_table((ns, "bids"))

    assert t1.schema().as_arrow() == t2.schema().as_arrow()


def test_tuple_identifier_required_for_dotted_names(iceberg_catalog, ds):
    _flavor, catalog, _wh = iceberg_catalog

    dotted_ns = ("ns.with.dot",)
    catalog.create_namespace(dotted_ns)

    # Table name normal; namespace contains dot -> MUST use tuple identifiers
    ident_tuple = ("ns.with.dot", "tbl")
    catalog.create_table(identifier=ident_tuple, schema=ds.schema)

    t = catalog.load_table(ident_tuple)
    assert t.schema().as_arrow() == ds.schema

    # String form would split on "." and point somewhere else.
    with pytest.raises(Exception):
        catalog.load_table("ns.with.dot.tbl")


def test_static_table_is_read_only(iceberg_catalog, iceberg_ident, ds, tmp_path):
    _flavor, catalog, _wh = iceberg_catalog
    namespace, identifier = iceberg_ident

    catalog.create_namespace(namespace)
    tbl = catalog.create_table(identifier=identifier, schema=ds.schema)

    # Load via catalog (mutable)
    assert tbl.scan().to_arrow().num_rows == 0

    # Load via metadata pointer (read-only)
    meta_loc = tbl.metadata_location
    static_tbl = StaticTable.from_metadata(meta_loc)

    with pytest.raises(Exception):
        static_tbl.append(ds.rows_a)  # type: ignore[attr-defined]


def test_create_table_transaction_smoke(iceberg_catalog, iceberg_ident, ds):
    _flavor, catalog, _wh = iceberg_catalog
    namespace, identifier = iceberg_ident

    catalog.create_namespace(namespace)

    # Docs show create_table_transaction as a context-manager API. :contentReference[oaicite:3]{index=3}
    with catalog.create_table_transaction(identifier=identifier, schema=ds.schema) as txn:
        with txn.update_schema() as us:
            us.add_column(path="new_col", field_type="string")
        with txn.update_spec() as up:
            up.add_identity("id")
        txn.set_properties(test_a="a", test_b="b")

    tbl = catalog.load_table(identifier)
    assert "test_a" in tbl.properties
```

---

# `tests/iceberg/test_schema_evolution_contract.py`

```python
from __future__ import annotations

import pytest
import pyarrow as pa

from pyiceberg.transforms import BucketTransform, DayTransform, IdentityTransform
from pyiceberg.table.sorting import NullOrder

pytestmark = pytest.mark.iceberg


def test_schema_field_ids_are_unique(iceberg_table):
    # Iceberg relies on stable field IDs; regardless of input, the table’s schema IDs must be unique.
    schema = iceberg_table.schema()
    ids = [f.field_id for f in schema.fields]
    assert len(ids) == len(set(ids))


def test_incompatible_change_requires_allow_flag(iceberg_table):
    # API docs show requiring an optional field is “incompatible” unless allow_incompatible_changes=True. :contentReference[oaicite:4]{index=4}
    with pytest.raises(Exception):
        with iceberg_table.update_schema() as upd:
            upd.update_column("city", required=True)

    # Now allow it
    with iceberg_table.update_schema(allow_incompatible_changes=True) as upd:
        upd.update_column("city", required=True)


def test_partition_evolution_smoke(iceberg_table):
    # Partition evolution API: add_field/remove_field/rename_field, plus identity shortcut. :contentReference[oaicite:5]{index=5}
    with iceberg_table.update_spec() as upd:
        upd.add_field("id", BucketTransform(16), "bucketed_id")
        upd.add_field("city", IdentityTransform(), "city_id")
        upd.identity("lon")  # shortcut API

    # Verify spec registry grew
    assert len(iceberg_table.specs()) >= 1


def test_sort_order_updates_are_append_only(iceberg_table):
    # Sort orders can only be updated by adding a new one; cannot delete/modify. :contentReference[oaicite:6]{index=6}
    before = len(iceberg_table.sort_orders())

    with iceberg_table.update_sort_order() as upd:
        upd.desc("id", IdentityTransform(), NullOrder.NULLS_FIRST)
        upd.asc("city", IdentityTransform(), NullOrder.NULLS_LAST)

    after = len(iceberg_table.sort_orders())
    assert after == before + 1
```

---

# `tests/iceberg/test_scan_planning_contract.py`

```python
from __future__ import annotations

import pytest

from ._util import arrow_equals, collect_record_batches

pytestmark = pytest.mark.iceberg


def test_plan_files_returns_tasks(iceberg_table, ds):
    iceberg_table.append(ds.rows_a)

    scan = iceberg_table.scan(selected_fields=("id", "city"))
    tasks = list(scan.plan_files())

    assert tasks, "Expected at least one FileScanTask"
    t0 = tasks[0]
    # FileScanTask is expected to carry file + residual filter. :contentReference[oaicite:7]{index=7}
    assert hasattr(t0, "file")
    assert hasattr(t0, "residual")


def test_to_arrow_equals_to_arrow_batch_reader(iceberg_table, ds):
    iceberg_table.append(ds.rows_a)
    iceberg_table.append(ds.rows_b)

    scan = iceberg_table.scan()
    eager = scan.to_arrow()
    streamed = collect_record_batches(scan.to_arrow_batch_reader())

    assert arrow_equals(eager, streamed)


def test_row_filter_string_parses(iceberg_table, ds):
    iceberg_table.append(ds.rows_a)
    out = iceberg_table.scan(row_filter='city = "Amsterdam"').to_arrow()
    assert out.num_rows == 1
    assert out.column("id").to_pylist() == [1]


def test_time_travel_by_snapshot_id(iceberg_table, ds):
    iceberg_table.append(ds.rows_a)
    snap1 = iceberg_table.metadata.snapshots[-1].snapshot_id

    iceberg_table.append(ds.rows_b)
    snap2 = iceberg_table.metadata.snapshots[-1].snapshot_id
    assert snap2 != snap1

    # Read as-of snapshot1
    t1 = iceberg_table.scan(snapshot_id=snap1).to_arrow()
    assert t1.num_rows == ds.rows_a.num_rows


def test_snapshot_properties_are_recorded(iceberg_table, ds):
    iceberg_table.append(ds.rows_a, snapshot_properties={"abc": "def"})
    assert iceberg_table.metadata.snapshots[-1].summary["abc"] == "def"  # :contentReference[oaicite:8]{index=8}


def test_manifest_count_fast_append_vs_merge(iceberg_catalog, iceberg_ident, ds, request):
    """
    This test ties manifest counts to config knobs.

    By default PyIceberg uses fast append and `commit.manifest-merge.enabled` defaults False. :contentReference[oaicite:9]{index=9}
    """
    _flavor, catalog, _wh = iceberg_catalog
    namespace, base_ident = iceberg_ident
    catalog.create_namespace(namespace)

    # ---- default (merge disabled) ----
    ident_a = base_ident + "_a"
    t_a = catalog.create_table(identifier=ident_a, schema=ds.schema)
    t_a.append(ds.rows_a)
    t_a.append(ds.rows_b)
    m_a = t_a.inspect.manifests().num_rows  # current manifests :contentReference[oaicite:10]{index=10}

    # ---- merge enabled (threshold lowered) ----
    ident_b = base_ident + "_b"
    t_b = catalog.create_table(
        identifier=ident_b,
        schema=ds.schema,
        properties={
            "commit.manifest-merge.enabled": "true",
            "commit.manifest.min-count-to-merge": "2",
            # keep target size large so we merge into 1 manifest deterministically for tiny data
            "commit.manifest.target-size-bytes": str(128 * 1024 * 1024),
        },
    )
    t_b.append(ds.rows_a)
    t_b.append(ds.rows_b)
    m_b = t_b.inspect.manifests().num_rows

    if request.config.getoption("--iceberg-strict-counts"):
        # strict expectations for tiny tables:
        assert m_a == 2, f"expected 2 manifests after 2 fast appends, got {m_a}"
        assert m_b == 1, f"expected merged manifests to collapse to 1, got {m_b}"
    else:
        # weaker (still useful) contract:
        assert m_a >= 1
        assert m_b >= 1
```

**Why the strict toggle?** Manifest merge behavior is a “table behavior option” controlled by these keys; the docs define the keys + defaults. ([PyIceberg][1])
The `--iceberg-strict-counts` flag lets you decide whether “exact manifest counts” are part of your contract suite or a “behavioral/perf check” (which is often what teams prefer).

---

# `tests/iceberg/test_delete_file_compat.py`

This file separates:

1. **what PyIceberg can express and read now** (position deletes show up in metadata tables; equality deletes are a known read-planning incompat)
2. **fixture-based regression** for equality delete tables produced by another engine.

```python
from __future__ import annotations

from pathlib import Path

import pytest

from pyiceberg.catalog import load_catalog

from ._util import file_uri, path_from_uri

pytestmark = pytest.mark.iceberg


def test_files_table_exposes_content_codes(iceberg_table, ds):
    iceberg_table.append(ds.rows_a)

    files = iceberg_table.inspect.files()
    assert files.num_rows >= 1

    # Content: 0=data, 1=position deletes, 2=equality deletes. :contentReference[oaicite:12]{index=12}
    content = files.column("content").to_pylist()
    assert all(c in (0, 1, 2) for c in content)
    assert 0 in content  # we wrote data


@pytest.mark.iceberg_external_fixture
def test_equality_deletes_fixture_fails_scan_plan(tmp_path: Path):
    """
    PyIceberg scan planning currently does not support equality deletes (expected failure).
    This test requires an externally produced fixture table that contains equality delete files.

    Layout expectation:
      tests/fixtures/iceberg/equality_deletes_table/metadata/...
      tests/fixtures/iceberg/equality_deletes_table/data/...

    How to generate:
      - Use Spark/Flink/Trino to create an Iceberg table that writes equality delete files
        (e.g., MERGE/DELETE semantics that produce equality deletes), then copy the table directory here.
    """
    fixture_root = Path("tests/fixtures/iceberg/equality_deletes_table")
    if not fixture_root.exists():
        pytest.skip("Missing external fixture table at tests/fixtures/iceberg/equality_deletes_table")

    # Load via StaticTable metadata pointer (read-only is fine; we only need to scan).
    from pyiceberg.table import StaticTable

    static_tbl = StaticTable.from_metadata(str(fixture_root))

    with pytest.raises(ValueError, match="equality deletes"):
        list(static_tbl.scan().plan_files())
```

Notes:

* The “content codes” and “files vs delete files” are explicitly described in the API docs. ([PyIceberg][2])
* The equality delete fixture is intentionally external; PyIceberg’s own write APIs may not generate equality deletes in a deterministic way across engines (and the point of the test is compatibility).

---

## Running the suite

Unit (in-memory only):

```bash
pytest -m iceberg
```

Enable Postgres catalog runs:

```bash
export ICEBERG_TEST_PG_URI="postgresql+psycopg2://user:pass@localhost:5432/iceberg"
pytest -m iceberg --iceberg-pg
```

Enable strict manifest count expectations:

```bash
pytest -m iceberg --iceberg-strict-counts
```

---

## What this suite “locks in” as contracts (quick checklist)

* Catalog-centric behavior + tuple identifiers for dotted names + StaticTable read-only. ([PyIceberg][2])
* Schema evolution incompatibility gating (`allow_incompatible_changes`). ([PyIceberg][2])
* Partition evolution and sort order update rules (append-only). ([PyIceberg][2])
* Scan planning + materialization correctness (`to_arrow` == streamed batches). ([PyIceberg][2])
* Table property knobs for manifest merge + metadata retention are explicit and testable. ([PyIceberg][1])

If you want the very next increment after this: I can add a **`test_metadata_retention_contract.py`** that asserts metadata JSON file pruning when `write.metadata.delete-after-commit.enabled=true` and `write.metadata.previous-versions-max` is small, using the same strict-toggle pattern. ([PyIceberg][1])

[1]: https://py.iceberg.apache.org/configuration/ "Configuration - PyIceberg"
[2]: https://py.iceberg.apache.org/api/ "API - PyIceberg"


Below is a **repo-ready** `tests/iceberg/test_metadata_retention_contract.py` that asserts metadata JSON pruning when:

* `write.metadata.delete-after-commit.enabled=true`
* `write.metadata.previous-versions-max` is small

Per PyIceberg’s configuration docs, enabling `delete-after-commit` deletes **old tracked metadata files after each commit**, retaining a bounded number of recent metadata files controlled by `previous-versions-max`. ([PyIceberg][1])

```python
# tests/iceberg/test_metadata_retention_contract.py
from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

import pytest

pytestmark = pytest.mark.iceberg


def _metadata_dir_from_metadata_location(metadata_location: str) -> Path:
    """
    Convert the table's metadata_location (usually file://...) into a local Path,
    then return the parent directory (the 'metadata/' directory).
    """
    if "://" in metadata_location:
        parsed = urlparse(metadata_location)
        p = Path(parsed.path)
    else:
        p = Path(metadata_location)
    return p.parent


def _count_metadata_json_files(metadata_dir: Path) -> int:
    """
    Count Iceberg metadata json versions (e.g., 00000-...metadata.json).
    We intentionally ignore manifest lists / manifests (*.avro) and other non-json metadata files.
    """
    if not metadata_dir.exists():
        return 0
    return sum(
        1
        for p in metadata_dir.iterdir()
        if p.is_file() and p.name.endswith("metadata.json")
    )


def _append_n_times(tbl, ds, n: int) -> None:
    # Alternate between the two tiny inputs to ensure deterministic commits.
    for i in range(n):
        tbl.append(ds.rows_a if i % 2 == 0 else ds.rows_b)


def test_metadata_retention_enabled_caps_metadata_json_files(
    iceberg_catalog, iceberg_ident, ds, request: pytest.FixtureRequest
) -> None:
    """
    Contract:
      If write.metadata.delete-after-commit.enabled=true and previous-versions-max=k,
      then after sufficiently many commits the number of metadata json files in /metadata
      should be bounded (should not grow unbounded with commits). :contentReference[oaicite:1]{index=1}

    Strict mode asserts an exact cap consistent with the config description; non-strict mode
    asserts only that growth is capped and materially smaller than the no-retention case.
    """
    strict = request.config.getoption("--iceberg-strict-counts")

    _flavor, catalog, _wh = iceberg_catalog
    namespace, base_ident = iceberg_ident
    catalog.create_namespace(namespace)

    prev_max = 2  # small, deterministic
    n_appends = 8  # > prev_max to ensure pruning must occur

    ident = f"{base_ident}_retention_on"
    tbl = catalog.create_table(
        identifier=ident,
        schema=ds.schema,
        properties={
            # These keys and semantics are documented in PyIceberg configuration. :contentReference[oaicite:2]{index=2}
            "write.metadata.delete-after-commit.enabled": "true",
            "write.metadata.previous-versions-max": str(prev_max),
        },
    )

    _append_n_times(tbl, ds, n_appends)

    # Reload to ensure we observe the latest metadata_location after commits.
    tbl = catalog.load_table(ident)

    meta_dir = _metadata_dir_from_metadata_location(tbl.metadata_location)
    count = _count_metadata_json_files(meta_dir)

    # Expectation: the number of retained metadata json files is bounded.
    # We cap at prev_max + 1 (current + previous versions). This is safe even if an impl chooses to be stricter.
    cap = prev_max + 1

    if strict:
        # In strict mode we assert we're at (or under) the cap after many commits.
        assert count <= cap, f"expected <= {cap} metadata json files, got {count} in {meta_dir}"
        # And we assert pruning actually happened: count must be far less than unbounded growth (n_appends + 1).
        assert count < (n_appends + 1), f"expected pruning, but saw {count} files for {n_appends} commits"
    else:
        assert count <= cap, f"expected <= {cap} metadata json files, got {count} in {meta_dir}"
        assert count < (n_appends + 1), "expected capping vs commit count growth"


def test_metadata_retention_disabled_allows_metadata_json_growth(
    iceberg_catalog, iceberg_ident, ds, request: pytest.FixtureRequest
) -> None:
    """
    Control test:
      With write.metadata.delete-after-commit.enabled=false (default), metadata json files
      should grow with the number of commits (no pruning). :contentReference[oaicite:3]{index=3}
    """
    strict = request.config.getoption("--iceberg-strict-counts")

    _flavor, catalog, _wh = iceberg_catalog
    namespace, base_ident = iceberg_ident
    catalog.create_namespace(namespace)

    prev_max = 2
    n_appends = 8

    ident = f"{base_ident}_retention_off"
    tbl = catalog.create_table(
        identifier=ident,
        schema=ds.schema,
        properties={
            # Explicitly set to false for clarity; default is False in docs. :contentReference[oaicite:4]{index=4}
            "write.metadata.delete-after-commit.enabled": "false",
            "write.metadata.previous-versions-max": str(prev_max),
        },
    )

    _append_n_times(tbl, ds, n_appends)

    tbl = catalog.load_table(ident)
    meta_dir = _metadata_dir_from_metadata_location(tbl.metadata_location)
    count = _count_metadata_json_files(meta_dir)

    # Without pruning, each commit should create a new metadata json version.
    # Expected minimum: 1 (create) + n_appends.
    expected_min = 1 + n_appends

    if strict:
        assert count >= expected_min, f"expected >= {expected_min} metadata json files, got {count} in {meta_dir}"
        # Also assert it's meaningfully above the retention cap we'd expect if pruning were on.
        assert count > (prev_max + 1), "expected growth beyond retention cap when pruning disabled"
    else:
        assert count >= expected_min
```

### What this locks in (contract-wise)

* The **property keys** are correct and wired into commit behavior, as documented in PyIceberg configuration. ([PyIceberg][1])
* When pruning is enabled, metadata JSON versions become **bounded**; when disabled, they **grow with commits**. ([PyIceberg][1])

[1]: https://py.iceberg.apache.org/configuration/ "Configuration - PyIceberg"
