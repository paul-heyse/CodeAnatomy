
Here’s a more complete picture of what **DataFusion** offers for **schema / catalog management + object creation**, and then what a **Delta Lake (delta-rs / `deltalake`) integration** looks like—*including where the real “synergy” comes from*.

---

## 1) DataFusion schema management and object creation: what’s actually there

### A. Namespace objects: catalogs, schemas, tables, views (DDL + API)

DataFusion organizes metadata in the hierarchy **CatalogProviderList → CatalogProvider → SchemaProvider → TableProvider**, and ships with an in-memory implementation you can use directly or replace/extend. ([Apache DataFusion][1])

On the SQL side, DataFusion supports a focused DDL set for creating catalog objects (and `DESCRIBE`): `CREATE DATABASE`, `CREATE SCHEMA`, `CREATE EXTERNAL TABLE`, `CREATE TABLE`, `DROP TABLE`, `CREATE VIEW`, `DROP VIEW`, `DESCRIBE`. ([Apache DataFusion][2])
Notably, **“ALTER TABLE” isn’t part of the documented SQL DDL surface** (so most “schema evolution” is done by re-registering a table/provider or using a table format like Delta/Iceberg). ([Apache DataFusion][2])

On the API side (Rust), schemas and tables are mutable via the `SchemaProvider` methods (register/deregister/existence), and `SchemaProvider::table` is **async** specifically so you can fetch metadata from remote sources if needed. ([Apache DataFusion][1])

### B. External table creation is much richer than “just register a file”

`CREATE EXTERNAL TABLE` is the core “schema management” primitive in SQL: it registers a location (local or object store) as a named table. You can:

* **Infer schema** (CSV/JSON) or **declare schema explicitly** (including `NOT NULL`). ([Apache DataFusion][3])
* Register **partitioned tables**; hive-style partitions are automatically detected and incorporated into schema, and you can also declare `PARTITIONED BY` for pruning. ([Apache DataFusion][3])
* Declare **unbounded sources** via `CREATE UNBOUNDED EXTERNAL TABLE` to force streaming-compatible planning rules. ([Apache DataFusion][3])
* Declare an **ordering contract** with `WITH ORDER (...)` (this can remove work in later plans, but correctness depends on the files actually being sorted). ([Apache DataFusion][3])
* Control table-level stats collection at creation time: DataFusion will read files to gather statistics by default; you can disable via `SET datafusion.execution.collect_statistics = false`. ([Apache DataFusion][3])

### C. Catalog auto-loading (“registry-lite”) knobs

If you want a “default schema” to automatically load tables from a directory/object-store prefix, DataFusion exposes config keys:

* `datafusion.catalog.location`: location scanned to load tables for the default schema
* `datafusion.catalog.format`: what `TableProvider` type to use when loading ([Apache DataFusion][4])

This is a useful bridge between “pure in-memory catalog” and “real metastore,” especially if your registry is storage-layout-driven.

### D. Introspection surfaces (information_schema + SHOW)

DataFusion supports an ISO-ish `information_schema` plus `SHOW` commands:

* `SHOW TABLES` / `information_schema.tables`
* `SHOW COLUMNS` / `information_schema.columns`
* `SHOW ALL` / `information_schema.df_settings`
* `SHOW FUNCTIONS` / `information_schema.routines` + `information_schema.parameters` ([Apache DataFusion][5])

(You typically need to enable information schema via config: `datafusion.catalog.information_schema`.) ([Apache DataFusion][4])

### E. Constraints and “schema correctness” semantics

Table providers can describe **constraints** (primary/unique/foreign/check) via `TableConstraint` / `Constraints`, but DataFusion **does not enforce them** at runtime today. ([Apache DataFusion][6])
The one thing DataFusion *does* enforce is **field nullability**: returning nulls for non-nullable fields can trigger runtime errors (but ingestion itself isn’t checked). ([Apache DataFusion][6])

### F. “Creation” downstream of schema: COPY / INSERT as schema-bearing operations

For producing new datasets:

* `COPY {table|query} TO 'path' [STORED AS ...] [PARTITIONED BY ...] [OPTIONS(...)]` writes Parquet/CSV/JSON/Arrow files, and can emit hive-style partitioned directories. ([Apache DataFusion][7])
* `INSERT INTO table VALUES (...) | query` exists; whether it works depends on the target `TableProvider` supporting inserts. ([Apache DataFusion][7])

---

## 2) What a DataFusion ↔ Delta Lake integration looks like

### A. Python: register a `deltalake.DeltaTable` as a TableProvider

As of **DataFusion 43.0.0+**, the Python bindings can register Delta Lake table providers directly (requiring a recent `deltalake` version that implements the needed interfaces):

```python
from datafusion import SessionContext
from deltalake import DeltaTable

ctx = SessionContext()
dt = DeltaTable("path_to_table")

ctx.register_table("my_delta_table", dt)
ctx.sql("SELECT * FROM my_delta_table LIMIT 10").show()
```

([Apache DataFusion][8])

If you’re on **older `deltalake` (<0.22)**, you can fall back to Arrow Dataset (`dt.to_pyarrow_dataset()`), but you lose important behaviors like **filter pushdown**, which can be a big performance hit. ([Apache DataFusion][8])

### B. Rust: use the Delta Lake `DeltaTableProvider` (TableProvider implementation)

On the Rust side, `deltalake` exposes a `DeltaTableProvider` that implements DataFusion’s `TableProvider`. It’s explicitly designed to support richer scan behavior (including optional metadata columns), and it exposes key hooks DataFusion cares about:

* `schema()`
* `scan(...)` with projection/filters/limit
* `supports_filters_pushdown(...)`
* `statistics()`
* `constraints()`
* importantly: `insert_into(...)` (append/overwrite supported) ([Docs.rs][9])

This means a “real” integration can be more than read-only: DataFusion `INSERT` can map to provider inserts when supported by the provider implementation. ([Docs.rs][9])

---

## 3) Where the added synergies come from (DataFusion + Delta > DataFusion + Parquet)

### A. Better pruning = less I/O

DataFusion can prune Parquet at the **row-group/page** level using Parquet metadata, but Delta can additionally prune at the **file** level because it records file-level metadata in the transaction log.

The delta-rs DataFusion guide highlights this explicitly: Delta can “skip entire files” using transaction-log metadata, then DataFusion can still prune within files (row groups), yielding better performance than raw Parquet in many multi-file layouts. ([Delta IO][10])

### B. Delta gives you a stable, versioned schema contract (stored in the log)

A `DeltaTable` represents the state of a table **at a particular version**, including which files are active, the schema, and other metadata. ([Delta IO][11])
The schema is saved in the transaction log and is retrievable both as Delta schema and as a PyArrow schema (with JSON round-tripping). ([Delta IO][11])

This is *huge* for “schema management” because you’re no longer guessing from files-on-disk: the table itself declares its schema, partition columns, and configuration. ([Delta IO][11])

### C. Schema enforcement and controlled evolution on writes

Delta’s Python writer (`write_deltalake`) will error if you attempt to write data with a different schema unless you explicitly opt into overwriting schema (`overwrite_schema=True` in overwrite mode). ([Delta IO][11])
That’s a ready-made contract enforcement mechanism you can pair with DataFusion’s catalog/registry layer.

### D. Transactional semantics and time travel

Delta brings ACID transactions, concurrency protection, and time travel/versioned data; delta-rs calls these out as key features for DataFusion users. ([Delta IO][10])
For DataFusion, this means you can query a **consistent snapshot** of a table even as writes happen elsewhere—without inventing your own snapshotting mechanism.

### E. Symbiotic relationship: Delta uses DataFusion internally for SQL-expression features

delta-rs explicitly notes that it depends on DataFusion for SQL-related functionality—e.g., **update/merge** expressed in SQL expressions and **constraints/invariants** expressed in SQL expressions. ([Delta IO][10])
That’s a real “synergy”: expression semantics align, and improvements in DataFusion’s expression engine can directly benefit Delta’s higher-level operations.

---

## 4) Practical “registry + Delta + DataFusion” pattern (what I’d standardize)

If you’re building a dataset registry (your earlier Hamilton-ish metastore design), Delta becomes a *high-quality physical table kind*:

* Registry stores `(catalog, schema, table)` → `{kind: "delta", location, optional version/time_travel}`
* Resolver loads `DeltaTable(...)` (or Rust `DeltaTableProvider`)
* Contract enforcer:

  * compare registry schema hash ↔ delta-log schema JSON ↔ DataFusion provider schema
  * enforce evolution rules (strict vs additive vs cast-permitted)
* Execution:

  * register into DataFusion catalog
  * query with DataFusion SQL / DataFrame
* Write-back:

  * either use provider `insert_into` when available (Rust provider supports append/overwrite inserts) ([Docs.rs][9])
  * or materialize results to Arrow/Parquet and commit via Delta write APIs (depending on your pipeline needs)

---

If you want, I can turn this into an **implementation-ready “adapter spec”**: exact registry columns for Delta (location, storage_options, version selector, schema JSON/hash, partition columns), plus a resolver that (1) loads Delta metadata once, (2) surfaces it in `information_schema`-like views, and (3) enforces deterministic schema evolution policies (strict/additive/casts) before DataFusion planning.

[1]: https://datafusion.apache.org/library-user-guide/catalogs.html "Catalogs, Schemas, and Tables — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/user-guide/sql/index.html "SQL Reference — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/user-guide/sql/ddl.html "DDL — Apache DataFusion  documentation"
[4]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[5]: https://datafusion.apache.org/user-guide/sql/information_schema.html "Information Schema — Apache DataFusion  documentation"
[6]: https://datafusion.apache.org/library-user-guide/table-constraints.html "Table Constraint Enforcement — Apache DataFusion  documentation"
[7]: https://datafusion.apache.org/user-guide/sql/dml.html "DML — Apache DataFusion  documentation"
[8]: https://datafusion.apache.org/python/user-guide/data-sources.html "Data Sources — Apache Arrow DataFusion  documentation"
[9]: https://docs.rs/deltalake/latest/deltalake/delta_datafusion/struct.DeltaTableProvider.html "DeltaTableProvider in deltalake::delta_datafusion - Rust"
[10]: https://delta-io.github.io/delta-rs/integrations/delta-lake-datafusion/ "DataFusion - Delta Lake Documentation"
[11]: https://delta-io.github.io/delta-rs/python/usage.html "Usage — delta-rs  documentation"


Below is the **“schema / type / nested-shape” function surface** in DataFusion (SQL + what’s typically used from Python). DataFusion’s docs concentrate these in **Struct/Map/Union**, **Arrow type/metadata**, and **Expansion** functions. ([Apache DataFusion][1])

---

## Fast way to generate the complete function list (then filter for “schema-ish”)

DataFusion supports:

```sql
SHOW FUNCTIONS [ LIKE <pattern> ];
```

…and also exposes function metadata via `information_schema ... routines` + `parameters` (see docs for the exact view path in your build). ([Apache DataFusion][2])

Useful filters:

```sql
show functions like '%arrow_%';
show functions like '%struct%';
show functions like '%map_%';
show functions like '%union_%';
show functions like '%unnest%';
```

---

## Catalog: schema-related functions

### A) Arrow type / metadata introspection + Arrow-precise casting

These are the “schema debugging” primitives.

* **`arrow_typeof(expression)` → `Utf8`**
  Returns the **Arrow data type name** of the expression (useful to confirm coercions / nested types). ([Apache DataFusion][1])

* **`arrow_cast(expression, datatype)` → `datatype`**
  Cast using an **Arrow type string** (same format as `arrow_typeof` output). This is the most exact cast mechanism when SQL type syntax is too coarse (timestamps, dictionaries, etc.). ([Apache DataFusion][1])

* **`arrow_metadata(expression[, key])` → `Map` or scalar**
  Returns Arrow metadata attached to the input expression (whole map if no key; value if key provided). ([Apache DataFusion][1])

* **`version()` → `Utf8`**
  DataFusion version string (often used to gate schema/feature expectations). ([Apache DataFusion][1])

**Python note:** `arrow_cast()` and `arrow_typeof()` are exposed as expression builders in `datafusion.functions`. ([Apache DataFusion][3])

---

### B) Struct construction (schema *shaping* into nested fields)

These create `struct`-typed columns—useful when you want to bundle columns into a single nested field or enforce a nested output schema.

* **`named_struct(name1, expr1, ..., nameN, exprN)` → `Struct`**
  Build a struct with explicit field names (names must be constant strings). ([Apache DataFusion][1])

* **`struct(expr1[, ..., exprN])` → `Struct`**
  Build a struct; fields are either explicitly named via `expr AS field_name` or auto-named `c0`, `c1`, … ([Apache DataFusion][1])

* **`row(...)`**
  Alias of `struct(...)`. ([Apache DataFusion][1])

**Python note:** `named_struct()` and `struct()` exist in the Python functions module (handy for DataFrame API plan-building). ([Apache DataFusion][3])

---

### C) Nested field access (struct/map lookup)

* **`get_field(expression, field_name[, field_name2, ...])`**
  Returns a field inside a **struct** or **map**; supports nested access with multiple names. Most users hit this **indirectly** via field access syntax like `my_struct['field']`, and nested indexing is optimized into a single `get_field` call. ([Apache DataFusion][1])

---

### D) Map functions (map-type “schema-aware” utilities)

These operate on Arrow `Map` columns—often the “schema drift” shape you get from semi-structured sources.

* **`map(key, value)` / `map(key: value)` / `make_map(keys_list, values_list)` → `Map`**
  Construct a map (docs note key uniqueness / non-null requirements for list-based construction). ([Apache DataFusion][1])

* **`map_entries(map)` → `List<Struct{key,value}>`**
  Returns a list of all map entries. ([Apache DataFusion][1])

* **`map_extract(map, key)` → `List<value>`**
  Returns a **single-element list** containing the value for `key`, or an empty list if absent. ([Apache DataFusion][1])

  * **`element_at(map, key)`** is an alias of `map_extract`. ([Apache DataFusion][1])

* **`map_keys(map)` → `List<key>`**
  List of keys. ([Apache DataFusion][1])

* **`map_values(map)` → `List<value>`**
  List of values. ([Apache DataFusion][1])

---

### E) Union (tagged-union / variant) functions

If you use Arrow Union types (rare, but powerful), these are the key helpers.

* **`union_tag(union_expression)` → `Utf8`**
  Name of the currently selected union field. ([Apache DataFusion][1])

* **`union_extract(union_expression, field_name)` → value or `NULL`**
  Returns the field’s value if selected, else `NULL`. ([Apache DataFusion][1])

---

### F) Expansion functions (turn nested values into rows/columns)

These are “schema reshaping” operations that change row shape.

* **`unnest(array_or_map)` → rows**
  Expands an array or map into rows. ([Apache DataFusion][4])

* **`unnest(struct)` → columns**
  Expands struct fields into individual columns using a placeholder-prefixed naming scheme (see docs). ([Apache DataFusion][4])

---

## Practical Python usage patterns (minimal)

* **SQL-first (always works):**

  * `ctx.sql("select arrow_typeof(col), arrow_metadata(col) from t")`
  * `ctx.sql("select unnest(my_list_col) from t")`
* **DataFrame API (when exposed):**

  * `from datafusion import functions as F`
  * `df.select(F.arrow_typeof(F.col("x")).alias("x_type"))` ([Apache DataFusion][3])
  * `df.select(F.named_struct("a", F.col("a"), "b", F.col("b")).alias("s"))` ([Apache DataFusion][3])

If you tell me whether you want this catalog scoped to **SQL-only**, **Python DataFrame API-only**, or **both (with “best available” mappings)**, I can rewrite it into the exact style you’re using for your internal references (e.g., signature + return Arrow type + failure modes per function).

[1]: https://datafusion.apache.org/user-guide/sql/scalar_functions.html "Scalar Functions — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/user-guide/sql/information_schema.html "Information Schema — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/python/autoapi/datafusion/functions/index.html "datafusion.functions — Apache Arrow DataFusion  documentation"
[4]: https://datafusion.apache.org/user-guide/sql/special_functions.html "Special Functions — Apache DataFusion  documentation"


You’re running into the fact that **DataFusion’s “schema” is Arrow’s schema**, and Arrow types are **recursive**: a table schema is a list of *Fields*, and each field’s *DataType* can itself contain **child fields** (struct/map/list/union). So “schema” isn’t just “column → primitive type”; a *single column* can be a whole nested record with its own internal schema. ([Apache Arrow][1])

---

## 1) Two levels of “schema” in DataFusion (Arrow mental model)

### Table schema

An Arrow **Schema** is basically “column names + column types” for a record batch/table, and Arrow docs explicitly note it’s *similar to a struct array type* (i.e., a struct whose fields are the columns). ([Apache Arrow][1])

### Column schema (nested types)

Arrow supports nested value types like **struct**, **map**, **list**, **union**; when you create these, you must specify the child types/fields. That’s why it feels “bigger” than your initial conception. ([Apache Arrow][1])

---

## 2) Struct construction = “one column that contains named subcolumns”

A **struct column** is like a JSON object: one column value contains multiple named fields, each with its own type and nullability. In schema terms, the column’s type is `Struct<field1: T1, field2: T2, ...>`.

### DataFusion constructors

#### `struct(expr1[, ..., exprN])`

Creates an Arrow struct from expressions. If you don’t name fields, DataFusion auto-names them `c0`, `c1`, …; you can name individual fields with `AS`. ([Apache DataFusion][2])

```sql
-- default field names c0/c1
select struct(a, b) as s from t;

-- name specific fields
select struct(a as field_a, b) as s from t;
```

In the docs’ example, `struct(a,b)` yields values like `{c0: 1, c1: 2}`, and naming produces `{field_a: 1, c1: 2}`. ([Apache DataFusion][2])

#### `named_struct(name1, expr1, ..., nameN, exprN)`

Creates a struct with **explicit names** (names are provided as expressions). ([Apache DataFusion][2])

```sql
select named_struct('name', name_col, 'age', age_col) as person from people;
```

#### `row(...)`

Alias of `struct(...)`. ([Apache DataFusion][2])

### Why this is “schema”

If you do:

```sql
select struct(a as x, b as y) as s from t;
```

the *result schema* contains a single column `s`, whose type is a **struct with fields** `x` and `y`. That’s “schema inside a column”.

### Flattening / projecting struct fields

DataFusion also has an expansion form: `unnest(struct)` “expands struct fields into individual columns” (with a placeholder naming pattern you can reference). ([Apache DataFusion][3])

---

## 3) Nested field access = “schema-aware projection from struct (and sometimes map)”

DataFusion exposes a core primitive:

### `get_field(expr, field_name[, field_name2, ...])`

Returns a field within a **struct or map**, supports nested access by providing multiple field names, and DataFusion optimizes chained bracket access into a single `get_field` call. ([Apache DataFusion][2])

Most people don’t call it directly; they use bracket syntax:

```sql
select struct_col['name'] as name from test;

-- nested
select struct_col['outer']['inner_val'] as result from test;
```

Those bracket forms compile to `get_field(struct_col, 'name')` or `get_field(struct_col, 'outer', 'inner_val')`. ([Apache DataFusion][2])

### Struct vs map: important schema distinction

* **Struct field names are part of the schema**. `struct_col['name']` is essentially a typed projection: the planner knows the output type because the struct’s child fields are defined in the column’s type.
* **Map keys are *data*, not schema**. A map’s schema is only “key type” + “value type”; the *set of keys* can vary per row. So “access by key” is more like semi-structured querying than schema projection (and has different edge cases).

---

## 4) Map functions = “a typed dictionary column” (schema = key/value types)

An Arrow **MapType** is a nested type with:

* a **key field** (not null),
* a **value field**,
* and optional “keys sorted” metadata; it’s structurally a collection of key/value entries. ([Apache Arrow][4])

In schema terms, a map column is `Map<key: K, value: V>` (conceptually).

### DataFusion map constructors

#### `map(key, value)` / `map(key: value)` / `make_map(keys_list, values_list)`

Creates an Arrow map. DataFusion notes: for `make_map`, **each key must be unique and non-null**. ([Apache DataFusion][2])

```sql
select map('type', 'test') as m;
select make_map(['a','b'], [1,2]) as m;
```

### Map “shape” functions (how schema changes)

#### `map_entries(map) -> List<Struct{key,value}>`

Returns a **list of entry structs** (each entry has `key` and `value`). ([Apache DataFusion][2])

This is the most “schema-revealing” operator: it makes the map’s implicit entry structure explicit.

#### `map_keys(map) -> List<key_type>`

Returns a list of keys. ([Apache DataFusion][2])

#### `map_values(map) -> List<value_type>`

Returns a list of values. ([Apache DataFusion][2])

#### `map_extract(map, key) -> List<value_type>` (alias `element_at`)

Returns a **list containing the value** for the key, or an **empty list** if the key is not present. ([Apache DataFusion][2])

Key detail: because values can be `NULL`, `map_extract` returning a list is often used as an “optional value” encoding (0 elements vs 1 element). In the docs, extracting a key whose value is `NULL` yields `[]`. ([Apache DataFusion][2])

---

## 5) How to reconcile this with your current “schema = one column, many types” intuition

What you described (“a singular column with different data types”) is closer to an **Arrow Union** (a tagged variant) than a struct/map. Arrow *does* support union as a nested type, and DataFusion has union-oriented functions (e.g., `union_tag`, `union_extract`)—but structs/maps are different: they’re **one column with internal structure**, not “one column that changes type row-by-row.” ([Apache Arrow][1])

A quick rule of thumb for designing schemas in DataFusion/Arrow terms:

* Use **STRUCT** when you want a *stable contract* of named fields (like a typed JSON object).
* Use **MAP** when you want a *flexible attribute bag* where keys vary per row, but key/value *types* are stable.

And the function families you asked about are precisely the “schema shaping” toolkit for those two nested-column styles.

[1]: https://arrow.apache.org/docs/python/data.html "Data Types and In-Memory Data Model — Apache Arrow v23.0.0"
[2]: https://datafusion.apache.org/user-guide/sql/scalar_functions.html "Scalar Functions — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/user-guide/sql/special_functions.html "Special Functions — Apache DataFusion  documentation"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.MapType.html "pyarrow.MapType — Apache Arrow v23.0.0"




Here’s a concrete **CPG-style** example where **a single “node” column** contains *typed nested structure* (structs + maps), and you use **DataFusion’s** `named_struct/struct`, **nested field access** (`get_field` via `[...]`), and **map functions** to stitch together **LibCST**, **Python AST**, and **SCIP** outputs.

---

## 0) Why nested “schema” matters for CPGs

With Arrow/DataFusion, a “column type” can itself be a **record**:

* `STRUCT<...>` = one column that contains named, typed subfields.
* `MAP<K,V>` = one column that contains a typed dictionary of key/value pairs.
* `LIST<STRUCT<...>>` = one column that contains *many* structured records per row, which you can explode with `unnest`. ([Apache DataFusion][1])

This matches code analysis well: “one symbol occurrence” or “one AST node” naturally wants **location**, **kind**, **provenance**, **attributes**, etc.

---

## 1) Normalize coordinates first (LibCST vs SCIP)

You *can* store raw coordinates, but if you want cross-tool joins (CST ↔ SCIP ↔ AST), normalize into one canonical convention.

### LibCST positions

LibCST’s `PositionProvider` returns a `CodeRange` with `start` and `end` `CodePosition`; **line numbers are 1-indexed**, **columns are 0-indexed**, and `end` is **exclusive**. ([LibCST][2])

### SCIP occurrence ranges

SCIP `Occurrence.range` is a `repeated int32` with **3 or 4 elements**: `[startLine, startCharacter, endLine, endCharacter]` or `[startLine, startCharacter, endCharacter]` (endLine inferred). **Lines and characters are 0-based**, and “character” interpretation depends on the document’s `position_encoding`. ([GitHub][3])

SCIP explicitly recommends: for an indexer implemented in **Python**, use `UTF32CodeUnitOffsetFromLineStart`. ([GitHub][3])

**Practical move:** normalize everything to `(line0, col0)` where `line0` is 0-based, and pick a single “character” basis for comparisons (often UTF-32 code unit offsets if you’re aligning with `scip-python`).

---

## 2) A CPG-friendly nested schema (one “node” column)

You can represent each extracted thing (CST node, AST node, SCIP occurrence/symbol) as a *single* nested struct column:

```text
cpg_nodes(
  repo: Utf8,
  path: Utf8,
  node_id: Int64,
  node: STRUCT<
    kind: Utf8,                  -- e.g. "cst.FunctionDef", "ast.Call", "scip.Occurrence"
    span: STRUCT<
      path: Utf8,
      start: STRUCT<line: Int32, col: Int32>,
      end:   STRUCT<line: Int32, col: Int32>,
      coord: MAP<Utf8, Utf8>     -- conventions: encoding, end_exclusive, etc.
    >,
    scip: STRUCT<
      symbol: Utf8,
      role_bits: Int32
    >,
    attrs: MAP<Utf8, Utf8>       -- flexible bag: node-specific fields / plugin facts
  >
)
```

* Use **STRUCT** for stable, query-critical fields (`span.start.line`, `scip.symbol`, etc.).
* Use **MAP** for “attribute bag” fields that vary by extractor or by node kind.

DataFusion gives you constructors for both:

* `named_struct(...)` / `struct(...)` (and `row` alias) ([Apache DataFusion][4])
* `map(...)` / `make_map(...)`, plus `map_entries`, `map_extract` ([Apache DataFusion][4])

---

## 3) Building the nested shape in SQL (struct construction + maps)

Assume you have a flat, unioned staging table:

```sql
-- raw_extract(repo, path, node_id, kind, tool,
--             start_line0, start_col, end_line0, end_col,
--             scip_symbol, scip_role_bits,
--             cst_name, ast_node_type, extra_json)

create view cpg_nodes as
select
  repo,
  path,
  node_id,
  named_struct(
    'kind', kind,

    'span', named_struct(
      'path', path,
      'start', named_struct('line', start_line0, 'col', start_col),
      'end',   named_struct('line', end_line0,   'col', end_col),

      -- conventions/provenance about coordinates
      'coord', map(
        ['tool', 'line_base', 'end_exclusive', 'char_encoding'],
        [tool,   '0',         'true',          'utf32']
      )
    ),

    'scip', named_struct(
      'symbol', scip_symbol,
      'role_bits', scip_role_bits
    ),

    -- attribute bag for tool-specific / node-specific stuff
    'attrs', map(
      ['cst_name', 'ast_node_type', 'extra'],
      [cst_name,   ast_node_type,   extra_json]
    )
  ) as node
from raw_extract;
```

* `named_struct` builds a typed struct column from name/value pairs. ([Apache DataFusion][4])
* `map(...)` builds a typed map; `make_map(keys, values)` exists too. ([Apache DataFusion][4])

---

## 4) Nested field access (schema-aware querying)

DataFusion’s `get_field` is what powers `node['span']['start']['line']` style access; nested chains get optimized. ([Apache DataFusion][4])

### Example: “all SCIP references inside a file region”

```sql
select
  node_id,
  node['scip']['symbol'] as sym,
  node['span']['start']['line'] as line0
from cpg_nodes
where node['kind'] = 'scip.Occurrence'
  and node['span']['path'] = 'src/foo.py'
  and node['span']['start']['line'] between 40 and 80;
```

This reads like object traversal because it *is* object traversal, but it’s still Arrow-typed and columnar. ([Apache DataFusion][4])

---

## 5) Map functions as “schema extension points”

A `MAP<Utf8, Utf8>` is a typed dictionary: keys aren’t schema, but the **value type is**. This is perfect for “plugin facts” and cross-tool annotations.

### 5.1 Pull a single attribute (optional semantics)

`map_extract(map, key)` returns a **list** with the value, or empty list if absent. ([Apache DataFusion][4])

A common pattern is to turn it into a scalar with `unnest`:

```sql
select
  node_id,
  unnest(map_extract(node['attrs'], 'cst_name')) as cst_name
from cpg_nodes
where node['kind'] = 'cst.FunctionDef';
```

* `map_extract` → list (0 or 1 elements) ([Apache DataFusion][4])
* `unnest` expands arrays/maps into rows ([Apache DataFusion][1])

### 5.2 Enumerate all attributes as key/value rows

`map_entries(map)` returns a `List<Struct{key,value}>`. ([Apache DataFusion][4])
Explode it:

```sql
select
  node_id,
  entry['key']   as k,
  entry['value'] as v
from cpg_nodes,
     unnest(map_entries(node['attrs'])) as entry;
```

### 5.3 “Schema projection” vs “schema-less facts”

* Put **location + IDs + core semantics** in `STRUCT` (fast, stable, joinable).
* Put **tool-specific extras** in `MAP` (flexible, sparse, evolve without schema migrations).

---

## 6) A CPG-style join across tools: “SCIP occurrences inside LibCST function bodies”

If you store function-definition spans from LibCST and occurrences from SCIP with the same normalized coordinates, you can do region containment joins:

```sql
with funcs as (
  select
    node_id as func_id,
    node['span'] as fspan,
    unnest(map_extract(node['attrs'], 'cst_name')) as func_name
  from cpg_nodes
  where node['kind'] = 'cst.FunctionDef'
),
occ as (
  select
    node_id as occ_id,
    node['span'] as ospan,
    node['scip']['symbol'] as sym
  from cpg_nodes
  where node['kind'] = 'scip.Occurrence'
)
select
  f.func_name,
  o.sym,
  count(*) as n_occurrences
from funcs f
join occ o
  on o.ospan['path'] = f.fspan['path']
 and o.ospan['start']['line'] >= f.fspan['start']['line']
 and o.ospan['start']['line'] <  f.fspan['end']['line']
group by 1,2;
```

This is a “CPG-ish” query: it’s not yet edges, but it’s already *contextual* linking (symbol occurrences inside syntactic regions) using nested schema access. ([Apache DataFusion][4])

---

## 7) Where this becomes “real CPG” (nodes + edges)

Once `cpg_nodes.node` is stable, edges become straightforward:

```text
cpg_edges(
  repo, src_id, dst_id,
  kind: Utf8,                     -- "AST_PARENT", "SCIP_DEFINITION_OF", "CALLS", etc.
  props: MAP<Utf8, Utf8>          -- edge-specific metadata
)
```

Then:

* `src_id/dst_id` are your stable graph IDs
* `props` holds sparse edge metadata (argument position, resolution confidence, etc.)
* `kind` is your primary discriminator

Same struct/map principles apply (stable core → columns/structs; sparse extras → map).

---

If you want, I can tighten this into a single “CPG canonical schema” that explicitly shows:

* how to encode **SCIP’s `Occurrence.range` (3 vs 4 ints)** into your `span` struct, ([GitHub][3])
* and how to encode LibCST `CodeRange` into the same `span` (1-based → 0-based conversion), ([LibCST][2])
  plus a minimal DataFusion “explosion recipe” for `LIST<STRUCT>` so you can store per-file arrays of nodes/edges and `unnest` them on demand. ([Apache DataFusion][1])

[1]: https://datafusion.apache.org/user-guide/sql/special_functions.html "Special Functions — Apache DataFusion  documentation"
[2]: https://libcst.readthedocs.io/en/latest/metadata.html "Metadata — LibCST  documentation"
[3]: https://raw.githubusercontent.com/sourcegraph/scip/main/scip.proto "raw.githubusercontent.com"
[4]: https://datafusion.apache.org/user-guide/sql/scalar_functions.html "Scalar Functions — Apache DataFusion  documentation"



Below is a **single “CPG canonical schema”** that makes *positions/spans* first-class (nested) schema, and shows exactly how to normalize **LibCST `CodeRange`** and **SCIP `Occurrence.range`** into the same `span` struct, plus a minimal **DataFusion explode/unnest recipe** for `LIST<STRUCT>`.

---

## 0) Canonical coordinate contract (make this your “Span ABI”)

Use **LSP-style** semantics as the canonical contract:

* `Position`: **zero-based** `line` and **zero-based** `character` offset, where the meaning of `character` is determined by a **position encoding**. ([Microsoft GitHub][1])
* `Range`: `(start, end)` where **end is exclusive** (half-open). ([Microsoft GitHub][1])

That gives you a clean “between-character” model that matches how you want to do joins/containment (“occurrence inside function body”, etc.).

### Canonical Arrow / DataFusion types

**Position**

```text
pos := STRUCT<
  line:      Int32,
  character: Int32
>
```

**Span**

```text
span := STRUCT<
  start: pos,
  end:   pos,

  -- how to interpret `character`
  position_encoding: Utf8,   -- e.g. "utf-16", "utf-8", "utf32-code-unit"

  -- provenance + raw source form (optional but very useful)
  source: STRUCT<
    tool: Utf8,              -- "libcst" | "scip" | "ast"
    raw_range: List<Int32>   -- populated for SCIP (3 or 4 ints); null/empty otherwise
  >
>
```

**Node / Edge (per-file, stored as lists)**

```text
node := STRUCT<
  id:   Int64,
  kind: Utf8,                -- "cst.FunctionDef", "ast.Call", "scip.Occurrence", ...
  span: span,
  attrs: MAP<Utf8, Utf8>     -- extension bag (plugin facts, names, hashes, etc.)
>

edge := STRUCT<
  src:  Int64,
  dst:  Int64,
  kind: Utf8,                -- "AST_PARENT", "CALLS", "REFERS_TO", ...
  attrs: MAP<Utf8, Utf8>
>

cpg_files := (
  repo: Utf8,
  path: Utf8,
  nodes: List<node>,
  edges: List<edge>
)
```

Why this works well with DataFusion: it can construct structs/maps (`struct`, `named_struct`, `map`, `make_map`) ([Apache DataFusion][2]), and it supports nested field access via `get_field` / `col['field']` including nested chains. ([Apache DataFusion][2])

---

## 1) Encode LibCST `CodeRange` → canonical `span`

LibCST gives:

* `CodeRange.start` is **inclusive**
* `CodeRange.end` is **exclusive**
* `CodePosition.line` is **1-indexed**
* `CodePosition.column` is **0-indexed** ([LibCST][3])

So conversion is deterministic:

```python
# libcst_range: CodeRange(start=CodePosition(line, column), end=CodePosition(line, column))

start_line0 = libcst_range.start.line - 1
start_char  = libcst_range.start.column

end_line0   = libcst_range.end.line - 1
end_char    = libcst_range.end.column

span = {
  "start": {"line": start_line0, "character": start_char},
  "end":   {"line": end_line0,   "character": end_char},
  "position_encoding": "utf32-code-unit",     # see note below
  "source": {"tool": "libcst", "raw_range": None},
}
```

**Encoding note:** LibCST also offers `ByteSpanPositionProvider` with byte offsets + length (and explicitly notes bytes vs characters for Unicode). ([LibCST][3])
If you ever get bitten by encoding mismatches between tools, storing **byte spans** (or both) is the “no ambiguity” escape hatch.

---

## 2) Encode SCIP `Occurrence.range` (3 vs 4 ints) → canonical `span`

SCIP’s `Occurrence.range` is either:

* **4 elements**: `[startLine, startCharacter, endLine, endCharacter]`
* **3 elements**: `[startLine, startCharacter, endCharacter]` with endLine inferred ([GitHub][4])

And the **character offsets depend on a position encoding**; in the SCIP schema comments, the recommendation for a Python indexer is to use `UTF32CodeUnitOffsetFromLineStart`. ([GitHub][4])
This matches the LSP idea that `character` meaning is encoding-dependent. ([Microsoft GitHub][1])

Canonical normalization:

```python
r = occurrence.range  # List[int]

if len(r) == 4:
    sl, sc, el, ec = r
elif len(r) == 3:
    sl, sc, ec = r
    el = sl
else:
    # empty/missing/invalid range
    sl = sc = el = ec = None

span = {
  "start": {"line": sl, "character": sc},
  "end":   {"line": el, "character": ec},
  "position_encoding": "utf32-code-unit",     # if your SCIP indexer used UTF32CodeUnitOffsetFromLineStart
  "source": {"tool": "scip", "raw_range": r},
}
```

**End-exclusive:** treat the `(start,end)` as half-open per LSP Range semantics (“end position is exclusive”). ([Microsoft GitHub][1])
That makes LibCST and SCIP align naturally because LibCST’s `CodeRange.end` is also exclusive. ([LibCST][3])

---

## 3) Minimal DataFusion “explosion recipe” for `LIST<STRUCT>` nodes/edges

DataFusion has two different “unnest” behaviors:

1. **`unnest(array_or_map)`**: expands an array/map into **rows**. ([Apache DataFusion][5])
2. **`unnest(struct)`**: expands struct fields into **columns**, with `__unnest_placeholder(...)` prefixed output column names. ([Apache DataFusion][5])

Also, the unnest operation **replicates the non-nested columns** (conceptually “join each row with all values in the list column”), and has `preserve_nulls` behavior. ([Docs.rs][6])

### 3.1 Explode nodes into one row per node

```sql
-- nodes is List<Struct<...>>
SELECT
  repo,
  path,
  unnest(nodes) AS node
FROM cpg_files;
```

(Per DataFusion’s unnest semantics, `repo/path` repeat for each emitted node. ([Docs.rs][6]))

### 3.2 Read nested fields (recommended: `get_field` / `[...]`)

```sql
SELECT
  repo,
  path,
  node['id'] AS node_id,
  node['kind'] AS kind,
  node['span']['start']['line'] AS start_line0,
  node['span']['start']['character'] AS start_char,
  node['span']['position_encoding'] AS pos_enc
FROM (
  SELECT repo, path, unnest(nodes) AS node
  FROM cpg_files
) t;
```

This uses DataFusion’s `get_field` behavior (nested `x['a']['b']` becomes one optimized call). ([Apache DataFusion][2])

### 3.3 (Optional) Expand a node struct into columns with `unnest(struct)`

If you want “wide” columns quickly (and are okay with the placeholder names):

```sql
SELECT
  repo,
  path,
  unnest(node)
FROM (
  SELECT repo, path, unnest(nodes) AS node
  FROM cpg_files
) t;
```

The output columns will look like:
`__unnest_placeholder(node).id`, `__unnest_placeholder(node).kind`, … ([Apache DataFusion][5])

### 3.4 (Optional) Explode the `attrs` map

* `map_entries(attrs)` returns a list of `{key,value}` structs ([Apache DataFusion][2])
* `map_extract(attrs, key)` returns a 0-or-1 element list (empty if missing) ([Apache DataFusion][2])

Example: list all attrs as key/value rows:

```sql
SELECT
  repo, path, node['id'] AS node_id,
  kv['key'] AS k,
  kv['value'] AS v
FROM (
  SELECT repo, path, unnest(nodes) AS node
  FROM cpg_files
) t,
unnest(map_entries(node['attrs'])) AS kv;
```

---

## 4) A tiny “sanity checklist” for cross-tool alignment

* **Make `span.position_encoding` explicit** and don’t assume the same meaning of “column” across tools (LSP explicitly says it’s encoding-dependent). ([Microsoft GitHub][1])
* Prefer **end-exclusive** everywhere (LSP Range, LibCST CodeRange already agree). ([Microsoft GitHub][1])
* If you ever see off-by-N issues on Unicode-heavy lines, add **byte spans** (LibCST `ByteSpanPositionProvider`) as a second coordinate system. ([LibCST][3])

If you want, I can extend this canonical schema with one more “CPG practicality” layer: **stable IDs** (repo+path+kind+span hash), and a standard edge set that binds SCIP symbols/occurrences into your AST/CST node IDs via span containment + name matching.

[1]: https://microsoft.github.io/language-server-protocol/specifications/lsp/3.17/specification/ "Specification"
[2]: https://datafusion.apache.org/user-guide/sql/scalar_functions.html "Scalar Functions — Apache DataFusion  documentation"
[3]: https://libcst.readthedocs.io/en/latest/metadata.html "Metadata — LibCST  documentation"
[4]: https://github.com/sourcegraph/scip/blob/main/scip.proto?utm_source=chatgpt.com "scip/scip.proto at main · sourcegraph/scip"
[5]: https://datafusion.apache.org/user-guide/sql/special_functions.html "Special Functions — Apache DataFusion  documentation"
[6]: https://docs.rs/datafusion/latest/datafusion/common/struct.UnnestOptions.html "UnnestOptions in datafusion::common - Rust"



You’re right: **`index.scip` is not “a table”**—it’s a **protobuf message tree** (`scip.Index`) whose fields include *repeated messages* like `documents`, and each `Document` contains repeated `occurrences` and `symbols`. In Arrow/DataFusion terms, that maps naturally to **nested columns**:

* protobuf `message` → Arrow **`STRUCT<...>`**
* protobuf `repeated T` → Arrow **`LIST<T>`** (and if `T` is a message, it becomes `LIST<STRUCT<...>>`)
* “attribute bag / extensible facts” → Arrow **`MAP<K,V>`** (DataFusion has direct map functions) ([GitHub][1])

DataFusion can query these using:

* struct/map field access `col['field']` (implemented via `get_field`) ([GitHub][1])
* list expansion via `unnest` (and `unnest(struct)` for flattening struct fields) ([datafusion.apache.org][2])
* map helpers `map_entries`, `map_extract`, `map_keys`, `map_values` ([datafusion.apache.org][3])

Below is a **concrete “how you configure it in DataFusion”** blueprint: **PyArrow nested schemas + DataFusion registration + query patterns** for LibCST, SCIP, symtable, and bytecode.

---

## 1) Common nested types you’ll reuse everywhere

This gives you the “CPG-style” anchor (span ABI) and a standard extension bag.

```python
import pyarrow as pa

# A canonical position/span type you can use across all extractors
pos_t = pa.struct([
    ("line0", pa.int32()),
    ("col", pa.int32()),  # interpretation depends on col_unit
])

byte_span_t = pa.struct([
    ("byte_start", pa.int32()),
    ("byte_len", pa.int32()),
])

span_t = pa.struct([
    ("start", pos_t),
    ("end", pos_t),
    ("end_exclusive", pa.bool_()),     # keep True for half-open ranges
    ("col_unit", pa.string()),         # "libcst.column" | "scip.UTF32..." | "py.utf8_byte_offset" etc.
    ("byte_span", byte_span_t),        # nullable in practice
])

attrs_t = pa.map_(pa.string(), pa.string())  # flexible key/value facts
```

---

## 2) SCIP: represent `index.scip` as nested Arrow columns (exactly)

From `scip.proto`:

* `Index` has `metadata`, `documents`, `external_symbols` ([GitHub][4])
* `Document` has `relative_path`, `occurrences`, `symbols`, optional `text`, and `position_encoding` ([GitHub][4])
* `Occurrence.range` is `repeated int32` of **3 or 4 elements**, half-open `[start,end)` and 0-based; `enclosing_range` uses same encoding ([GitHub][4])
* `SymbolInformation` has `symbol`, `documentation`, `relationships`, `kind`, plus `display_name` and `enclosing_symbol` ([GitHub][4])
* `Document.position_encoding` specifies how to interpret “character”, with guidance for Python indexers (UTF32 offsets) ([GitHub][4])

### 2.1 “Index bundle” schema (one row per `index.scip` file)

This is the shape that preserves the *original tree*:

```python
# --- SCIP nested message types ---
tool_info_t = pa.struct([
    ("name", pa.string()),
    ("version", pa.string()),
    ("arguments", pa.list_(pa.string())),
])

metadata_t = pa.struct([
    ("protocol_version", pa.int32()),
    ("tool_info", tool_info_t),
    ("project_root", pa.string()),
    ("text_document_encoding", pa.int32()),
])

diagnostic_t = pa.struct([
    ("severity", pa.int32()),
    ("code", pa.string()),
    ("message", pa.string()),
    ("source", pa.string()),
    ("tags", pa.list_(pa.int32())),
])

occurrence_t = pa.struct([
    # Keep raw proto range for auditability
    ("range_raw", pa.list_(pa.int32())),
    ("range", span_t),  # normalized (you compute once when ingesting)
    ("symbol", pa.string()),
    ("symbol_roles", pa.int32()),
    ("override_documentation", pa.list_(pa.string())),
    ("syntax_kind", pa.int32()),
    ("diagnostics", pa.list_(diagnostic_t)),
    ("enclosing_range_raw", pa.list_(pa.int32())),
    ("enclosing_range", span_t),
    ("attrs", attrs_t),  # optional: any extra you compute
])

relationship_t = pa.struct([
    ("symbol", pa.string()),
    ("is_reference", pa.bool_()),
    ("is_implementation", pa.bool_()),
    ("is_type_definition", pa.bool_()),
    ("is_definition", pa.bool_()),
])

symbol_info_t = pa.struct([
    ("symbol", pa.string()),
    ("documentation", pa.list_(pa.string())),
    ("relationships", pa.list_(relationship_t)),
    ("kind", pa.int32()),
    ("display_name", pa.string()),
    ("enclosing_symbol", pa.string()),
    ("attrs", attrs_t),
])

document_t = pa.struct([
    ("relative_path", pa.string()),
    ("language", pa.string()),
    ("text", pa.string()),  # optional
    ("position_encoding", pa.int32()),  # keep raw enum value
    ("occurrences", pa.list_(occurrence_t)),
    ("symbols", pa.list_(symbol_info_t)),
    ("attrs", attrs_t),
])

scip_index_schema = pa.schema([
    ("index_id", pa.uint64()),      # your stable id for this index file/snapshot
    ("metadata", metadata_t),
    ("documents", pa.list_(document_t)),
    ("external_symbols", pa.list_(symbol_info_t)),
])
```

### 2.2 Register this nested table in DataFusion (Python)

DataFusion can register **PyArrow RecordBatches** as a SQL table via `register_record_batches`. ([datafusion.apache.org][5])

```python
from datafusion import SessionContext

ctx = SessionContext()

# Build a pyarrow.Table with exactly the schema above (from your parsed scip.Index)
# rows = [ { "index_id": ..., "metadata": {...}, "documents": [...], "external_symbols": [...] }, ... ]
table = pa.Table.from_pylist(rows, schema=scip_index_schema)

batches = table.to_batches()
ctx.register_record_batches("scip_index", [batches])  # one partition containing N batches
```

`register_record_batches(name, partitions)` expects a list of partitions, each partition is a list of record batches. ([datafusion.apache.org][5])

### 2.3 Querying: nested field access + explosion

**Nested field access** works via bracket syntax (SQL uses `get_field` under the hood). ([GitHub][1])

```sql
-- metadata is a STRUCT
select
  metadata['project_root'] as project_root,
  metadata['tool_info']['name'] as tool_name
from scip_index;
```

**Explode documents**: `documents` is `LIST<STRUCT<...>>`, so `unnest(documents)` yields rows of `Document` structs. Unnest conceptually replicates non-nested columns across emitted rows. ([datafusion.apache.org][2])

```sql
select
  index_id,
  doc['relative_path'] as rel_path,
  doc['language'] as lang
from (
  select index_id, unnest(documents) as doc
  from scip_index
) t;
```

**Explode occurrences** (nested list inside a document):

```sql
select
  index_id,
  doc['relative_path'] as rel_path,
  occ['symbol'] as symbol,
  occ['range']['start']['line0'] as start_line0,
  occ['range']['start']['col'] as start_col,
  occ['range']['col_unit'] as col_unit
from (
  select index_id, unnest(documents) as doc
  from scip_index
) d,
(
  select unnest(d.doc['occurrences']) as occ
) o;
```

If you keep `attrs` as a `MAP`, you can expand it with `map_entries` (returns list of `{key,value}` structs) and `unnest`. ([datafusion.apache.org][3])

---

## 3) LibCST: store per-file CST as `LIST<STRUCT>` nodes/edges

A “best in class” Arrow/DataFusion representation is a **file bundle table**:

```python
cst_node_t = pa.struct([
    ("cst_id", pa.int64()),
    ("kind", pa.string()),     # e.g. "libcst.FunctionDef"
    ("span", span_t),          # position provider span
    ("span_ws", span_t),       # whitespace-inclusive span (optional)
    ("attrs", attrs_t),        # e.g. name, operator, decorator flags, etc.
])

cst_edge_t = pa.struct([
    ("src", pa.int64()),
    ("dst", pa.int64()),
    ("kind", pa.string()),     # "CHILD", "NEXT_SIBLING", "TOKEN", ...
    ("slot", pa.string()),     # parent field name (body/params/...)
    ("idx", pa.int32()),       # index within list field
    ("attrs", attrs_t),
])

libcst_files_schema = pa.schema([
    ("repo", pa.string()),
    ("path", pa.string()),
    ("file_id", pa.uint64()),
    ("nodes", pa.list_(cst_node_t)),
    ("edges", pa.list_(cst_edge_t)),
    ("attrs", attrs_t),
])
```

Query patterns are identical to SCIP: `unnest(nodes)` and then `node['span']['start']['line0']` etc. ([datafusion.apache.org][2])

---

## 4) symtable: represent scope blocks with nested symbol lists

symtable is naturally hierarchical: file → blocks (module/function/class) → symbols in each block.

```python
sym_flags_t = pa.struct([
    ("is_referenced", pa.bool_()),
    ("is_imported", pa.bool_()),
    ("is_parameter", pa.bool_()),
    ("is_type_parameter", pa.bool_()),
    ("is_global", pa.bool_()),
    ("is_nonlocal", pa.bool_()),
    ("is_declared_global", pa.bool_()),
    ("is_local", pa.bool_()),
    ("is_annotated", pa.bool_()),
    ("is_free", pa.bool_()),
    ("is_assigned", pa.bool_()),
    ("is_namespace", pa.bool_()),
])

sym_symbol_t = pa.struct([
    ("name", pa.string()),
    ("flags", sym_flags_t),
    ("attrs", attrs_t),          # any extras / derived facts
])

sym_block_t = pa.struct([
    ("block_id", pa.int64()),
    ("parent_block_id", pa.int64()),
    ("block_type", pa.string()), # "module"|"function"|"class"|...
    ("name", pa.string()),
    ("lineno1", pa.int32()),
    ("span_hint", span_t),       # optional: if you can map to a CST/AST node span
    ("symbols", pa.list_(sym_symbol_t)),
    ("attrs", attrs_t),
])

symtable_files_schema = pa.schema([
    ("repo", pa.string()),
    ("path", pa.string()),
    ("file_id", pa.uint64()),
    ("blocks", pa.list_(sym_block_t)),
    ("attrs", attrs_t),
])
```

This gives you one “scope graph” column (`blocks`) you can explode on demand.

---

## 5) Bytecode: code objects → instructions as nested lists

Bytecode also nests cleanly: file → code objects → instructions.

```python
instr_t = pa.struct([
    ("offset", pa.int32()),
    ("opname", pa.string()),
    ("opcode", pa.int32()),
    ("arg", pa.int32()),
    ("argrepr", pa.string()),
    ("is_jump_target", pa.bool_()),
    ("jump_target", pa.int32()),
    ("span", span_t),     # col_unit often "py.utf8_byte_offset"
    ("attrs", attrs_t),
])

code_obj_t = pa.struct([
    ("code_id", pa.uint64()),
    ("qualname", pa.string()),
    ("name", pa.string()),
    ("firstlineno1", pa.int32()),
    ("argcount", pa.int32()),
    ("posonlyargcount", pa.int32()),
    ("kwonlyargcount", pa.int32()),
    ("nlocals", pa.int32()),
    ("varnames", pa.list_(pa.string())),
    ("freevars", pa.list_(pa.string())),
    ("cellvars", pa.list_(pa.string())),
    ("names", pa.list_(pa.string())),
    ("consts_json", pa.string()),     # safest: serialize constants
    ("instructions", pa.list_(instr_t)),
    ("attrs", attrs_t),
])

bytecode_files_schema = pa.schema([
    ("repo", pa.string()),
    ("path", pa.string()),
    ("file_id", pa.uint64()),
    ("code_objects", pa.list_(code_obj_t)),
    ("attrs", attrs_t),
])
```

---

## 6) “Exactly how you’d configure it” in DataFusion: the pattern

For *any* of these schemas:

1. Produce a `pyarrow.Table` with nested types (struct/list/map)
2. Register into DataFusion with `register_record_batches` (or `register_table` if you have a suitable provider) ([datafusion.apache.org][5])
3. Query with:

   * `col['field']` / `get_field` for struct + map access ([GitHub][1])
   * `unnest(list_col)` to explode lists into rows ([datafusion.apache.org][2])
   * `map_entries/map_extract/...` for map bags ([datafusion.apache.org][3])

And crucially: **for SCIP, you preserve the real structure** (`documents` is a list of documents; each has list of occurrences/symbols) exactly as the proto defines it. ([GitHub][4])

---

If you want the next “best-in-class” step: I can show a **paired set of SQL views** that “explode” each bundle table into **normalized 2D views** (`scip_documents`, `scip_occurrences`, `libcst_nodes`, `bytecode_instructions`, etc.) while keeping your underlying storage nested (so you get both ergonomic SQL joins *and* compact storage).

[1]: https://raw.githubusercontent.com/apache/datafusion/main/docs/source/user-guide/sql/scalar_functions.md "raw.githubusercontent.com"
[2]: https://datafusion.apache.org/user-guide/sql/special_functions.html "Special Functions — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/user-guide/sql/scalar_functions.html "Scalar Functions — Apache DataFusion  documentation"
[4]: https://raw.githubusercontent.com/sourcegraph/scip/main/scip.proto "raw.githubusercontent.com"
[5]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"

Below is a set of *schema-related* DataFusion capabilities that either **weren’t mentioned** or only **touched lightly** so far. I’m grouping by “where schema shows up” in DataFusion: *introspection*, *type system*, *format/schema inference*, *planning correctness*, and *extensibility*.

---

## 1) Built-in schema introspection beyond the basics

### `DESCRIBE` / `DESC` (schema display, nullability included)

DataFusion has a first-class `DESCRIBE table` (and `DESC`) that returns **column name**, **data type**, and **nullable status**. This is the quickest “what schema did DF actually register?” check, including nullability. ([Apache DataFusion][1])

### `information_schema` goes beyond tables/columns

You likely saw `SHOW TABLES` / `SHOW COLUMNS`, but `information_schema` also exposes:

* `information_schema.tables` (including table_type like BASE TABLE / VIEW)
* `information_schema.columns`
* `information_schema.df_settings` (schema-adjacent: settings that change type mapping / inference)
* function catalogs via `SHOW FUNCTIONS` and `information_schema…routines` / `…parameters` (returns *return_type* and *parameter_types*, which is very “schema-y” when you’re reasoning about overloads / coercions). ([Apache DataFusion][2])

---

## 2) “Schema debugging” via plans: printing schema in `EXPLAIN`

You can configure `EXPLAIN` output to include **schema information** via `datafusion.explain.show_schema`. This is extremely useful when your schema changes due to projection, casts, view-type coercions, etc. ([Apache DataFusion][3])

Related: DataFusion also has a `DFSchema.tree_string()` helper to render schemas as a tree including field names, types, and nullability—handy when nested schemas get deep. ([Docs.rs][4])

---

## 3) Schema-affecting configuration knobs (these change your *actual* types)

### View types for strings/binary (`Utf8View` / `BinaryView`)

DataFusion can map string SQL types to **view types** by default (and can also coerce “view types” back to non-view types at output).

Key knobs you probably want to be aware of:

* `datafusion.sql_parser.map_string_types_to_utf8view` (default true): maps VARCHAR/CHAR/Text/String to `Utf8View` rather than `Utf8`. ([Apache DataFusion][3])
* `datafusion.execution.parquet.schema_force_view_types` (default true): Parquet reader reads `Utf8/Utf8Large` as `Utf8View` and `Binary/BinaryLarge` as `BinaryView`. ([Apache DataFusion][5])
* `datafusion.optimizer.expand_views_at_output`: if enabled, coerces view types like `Utf8View` → `LargeUtf8` and `BinaryView` → `LargeBinary` at output. ([Apache DataFusion][3])

Why this matters: **your schema will show `Utf8View` even when you expect `Utf8`**, and certain downstream interop/serialization pathways may need special handling.

### Parser knobs that directly impact schema typing

A few SQL parser config options change the *types in the schema*:

* `datafusion.sql_parser.parse_float_as_decimal`: parse floats as DECIMAL instead of float types. ([Apache DataFusion][3])
* `datafusion.sql_parser.support_varchar_with_length`: allow `VARCHAR(n)` (but Arrow can’t enforce length). ([Apache DataFusion][3])
* `datafusion.sql_parser.dialect`: different SQL dialects change identifier/type parsing behaviors. ([Apache DataFusion][3])
* `datafusion.sql_parser.collect_spans`: records SQL source spans in logical plan nodes (useful for tracing schema derivation back to SQL). ([Apache DataFusion][3])

---

## 4) The full DataFusion type system (and where schema mapping happens)

DataFusion’s SQL types map onto **Arrow types**, and that mapping is what you get in schemas created by `CREATE EXTERNAL TABLE` and by `CAST`. The “Data Types” doc is basically the authoritative mapping table. ([Apache DataFusion][6])

This is also the starting point for understanding which Arrow types are *supported* in SQL DDL vs which only exist at the Arrow layer.

---

## 5) Field metadata, extension types, and “user-defined types” (UDTs)

This is a big one that we didn’t go deep on:

Arrow `Field`s can carry **metadata** (string→string map), and Arrow supports **extension types** (logical typing layered on physical types). DataFusion 48+ improved propagation of field metadata and made UDF APIs receive and return full `Field` information (not just `DataType`), enabling UDT-like behavior via metadata/extension types. ([Apache DataFusion][7])

This is “schema capability” in the strongest sense: you can attach and preserve semantic typing (e.g., UUID-as-FixedSizeBinary(16)) across plans if you design your functions/providers correctly.

---

## 6) Type coercion rules (schema compatibility) and customizing them for UDFs

DataFusion has a formal type coercion layer:

* It **automatically inserts lossless CASTs** when operator/function argument types don’t match required types. ([Docs.rs][8])
* Function signatures can describe coercion requirements; the `Coercion` enum explicitly models “Exact” vs “Implicit” coercion rules. ([Docs.rs][9])
* For custom UDFs, `ScalarUDFImpl::coerce_types` lets you implement *user-defined* coercion when you use a user-defined signature. ([Docs.rs][10])
* UDF registration is part of schema governance because DataFusion plans using the **function signature and return type** (what schema comes out) for resolution. ([Docs.rs][11])

If you’re trying to enforce a contract for nested columns (e.g., `span.start.line0` must be int32), understanding coercion is crucial.

---

## 7) TableProvider schema APIs are richer than “schema()”

In Rust (and by extension for Rust-backed Python providers), `TableProvider` can expose more schema-adjacent information than just `schema()`:

* `constraints()` (PK/unique/FK/check metadata)
* `get_table_definition()` (a create statement string, if available)
* `get_logical_plan()` (if the “table” is actually a plan/view-like object)
* `get_column_default()` (default expressions for columns)
* `scan_with_args(...)` structured scanning args (projection/filters/limit/order preferences), which is the basis for projection/filter/limit pushdown behavior. ([Docs.rs][12])

Even though DataFusion doesn’t enforce most constraints, it documents exactly what is (and isn’t) enforced: **only nullability is enforced at runtime; other constraints are informational and not validated/used in planning.** ([Apache DataFusion][13])

---

## 8) Schema correctness checks and invariants (planner-level validation)

Two important “schema safety” pieces that I didn’t call out earlier:

* There’s an execution config `datafusion.execution.skip_physical_aggregate_schema_check` controlling whether DataFusion verifies that the planned aggregate input schema matches exactly (including nullability + metadata). Disabling it is described as a workaround for planner bugs that are now caught by stricter schema verification. ([Apache DataFusion][5])
* DataFusion’s invariants/specs explicitly state things like:

  * every logical field’s `(relation, name)` tuple must be unique, and builders/optimizers **must error** if violated
  * physical arrays must be consistent with their declared schema types. ([Apache DataFusion][14])

This becomes relevant when you “stitch” multiple sources and you need deterministic, unambiguous field identity.

---

## 9) Arrow interop: requesting a schema projection at the boundary

In `datafusion-python`, `RecordBatch.__arrow_c_array__(requested_schema=...)` can attempt to provide the record batch using a **requested schema**, applying straightforward projections like column selection/reordering, enabling zero-copy interchange with Arrow C Data Interface users. ([Apache DataFusion][15])

This is not “DDL schema management,” but it’s a real *schema manipulation capability* at the interop boundary.

---

## 10) Important limitations / rough edges (schema-related) you should plan around

These are gaps in *capability maturity* (especially for nested types and drift):

### Nested/structured types are still an “active area”

DataFusion has an explicit epic tracking “proper support” for `Struct`, `List`, `Map`, `Union`, including nested identifier access and nested projection pushdown. ([GitHub][16])

Concrete gotchas you may encounter:

* “Nested identifiers not yet supported” errors for certain nested field access syntaxes in some versions/usages. ([GitHub][17])
* Some expressions/operators historically didn’t support nested columns (example: `IN (...)` panic “does not yet support nested columns” in older versions). ([GitHub][18])
* Bugs around name resolution when a struct has a field name that collides with a top-level column name. ([GitHub][19])

### Schema merging across file sets (Parquet drift) is not “solved”

There’s an explicit issue about improving schema merging when Parquet files evolve types over time (example given: `Int64` becoming `Float64`), and it notes Arrow-rs can’t merge those schemas. ([GitHub][20])

### “SHOW CREATE TABLE” is not (fully) there

There’s an issue to implement `SHOW CREATE TABLE` (for external tables), and the proposed path is to implement `TableProvider::get_table_definition` for `ListingTable`. ([GitHub][21])

### Substrait interchange has type edge cases (esp. view types)

DataFusion Python advertises Substrait plan serialization/deserialization. ([GitHub][22])
But there have been explicit issues to ensure view scalar types (`Utf8View`, `BinaryView`) serialize in Substrait, tied to efforts to use StringView arrays by default. ([GitHub][23])

---


[1]: https://datafusion.apache.org/user-guide/sql/ddl.html "DDL — Apache DataFusion  documentation"
[2]: https://datafusion.apache.org/user-guide/sql/information_schema.html "Information Schema — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[4]: https://docs.rs/datafusion/latest/datafusion/common/struct.DFSchema.html "DFSchema in datafusion::common - Rust"
[5]: https://datafusion.apache.org/_sources/user-guide/configs.md.txt "datafusion.apache.org"
[6]: https://datafusion.apache.org/user-guide/sql/data_types.html "Data Types — Apache DataFusion  documentation"
[7]: https://datafusion.apache.org/blog/2025/09/21/custom-types-using-metadata/ "Implementing User Defined Types and Custom Metadata in DataFusion - Apache DataFusion Blog"
[8]: https://docs.rs/datafusion-expr/latest/datafusion_expr/type_coercion/index.html "datafusion_expr::type_coercion - Rust"
[9]: https://docs.rs/datafusion-expr/latest/datafusion_expr/enum.Coercion.html "Coercion in datafusion_expr - Rust"
[10]: https://docs.rs/datafusion/latest/datafusion/logical_expr/trait.ScalarUDFImpl.html "ScalarUDFImpl in datafusion::logical_expr - Rust"
[11]: https://docs.rs/datafusion/latest/datafusion/logical_expr/struct.ScalarUDF.html "ScalarUDF in datafusion::logical_expr - Rust"
[12]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html "TableProvider in datafusion::datasource - Rust"
[13]: https://datafusion.apache.org/library-user-guide/table-constraints.html "Table Constraint Enforcement — Apache DataFusion  documentation"
[14]: https://datafusion.apache.org/contributor-guide/specification/invariants.html "Invariants — Apache DataFusion  documentation"
[15]: https://datafusion.apache.org/python/autoapi/datafusion/record_batch/index.html "datafusion.record_batch — Apache Arrow DataFusion  documentation"
[16]: https://github.com/apache/datafusion/issues/2326 "[EPIC] Improved support for nested / structured types (`Struct` , `List`, `ListArray`, and other Composite types) · Issue #2326 · apache/datafusion · GitHub"
[17]: https://github.com/apache/datafusion/issues/14768 "Nested Fields Access on StructArray field not working · Issue #14768 · apache/datafusion · GitHub"
[18]: https://github.com/apache/datafusion/issues/3778 "Panicked at InList does not yet support nested columns. · Issue #3778 · apache/datafusion · GitHub"
[19]: https://github.com/apache/arrow-datafusion/issues/8456 " query result empty  when a struct field name and a regular field name is same · Issue #8456 · apache/datafusion · GitHub"
[20]: https://github.com/apache/arrow-datafusion/issues/4223 "Improve schema merging · Issue #4223 · apache/datafusion · GitHub"
[21]: https://github.com/apache/arrow-datafusion/issues/2848 "Implement \"SHOW CREATE TABLE\" for external tables · Issue #2848 · apache/datafusion · GitHub"
[22]: https://github.com/apache/datafusion-python "GitHub - apache/datafusion-python: Apache DataFusion Python Bindings"
[23]: https://github.com/apache/datafusion/issues/12118 "Support substrait serialization  for `ScalarValue::Utf8View` and `ScalarValue::BinaryView` · Issue #12118 · apache/datafusion · GitHub"


Below, I’m focusing on things that actually change **what schema DataFusion sees**, how it **derives** it, and how it **keeps it stable** across sources / time.

---

## 1) `TableSchema`: DataFusion’s “table schema = file schema + partition columns” object

Beyond Arrow `Schema`, DataFusion has a distinct `TableSchema` type that explicitly models:

* **file schema** (columns physically in the files)
* **partition columns** (derived from directory layout / hive partitions)
* and the combined **table schema** queries see

`TableSchema::new(file_schema, partition_cols)` appends partition columns to the file schema to compute the query-visible schema, and exposes `file_schema()`, `table_partition_cols()`, and `table_schema()`. ([Docs.rs][1])

This is important because DataFusion has also **refactored file scanning APIs** so file sources now require the schema (including partition columns) up-front: built-in sources like `ParquetSource`, `CsvSource`, `JsonSource`, `AvroSource` now take a `TableSchema` in their constructors. ([Apache DataFusion][2])

**Why you should care (CPG style workloads):** if you implement a custom file-backed provider or build file scan configs directly, schema is now a first-class input to the source, not a late-bound afterthought. ([Apache DataFusion][2])

---

## 2) Schema adapters + schema evolution hooks (the *real* story for drift)

DataFusion (and the surrounding Arrow/DataFusion ecosystem) is moving toward formal “schema adapter” infrastructure: compute a mapping from **file schema → table schema** once, and apply it across many batches.

You can see this explicitly in:

* DataFusion feature requests to allow **injecting a custom `SchemaAdapter` into `ParquetExec`**, motivated by table formats like Delta where the “true schema” lives outside parquet and may enrich/normalize file schemas (nested columns, timezones, etc.). ([GitHub][3])
* The Arrow-rs issue about adding schema evolution / adapter APIs and noting DataFusion already has a `SchemaAdapter` concept it wants to reuse. ([GitHub][4])

And very concretely, delta-rs’s DataFusion-side `ListingTable` exposes:

* `with_schema_adapter_factory(...)` where the adapter “can handle schema evolution and type conversions when reading files with different schemas than the table schema.” ([Docs.rs][5])

**Why you should care:** If your CPG datasets evolve (add fields to nested structs, enrich metadata, etc.), the “best” DataFusion-native place to reconcile drift is *at the scan boundary* via schema adapters, not via ad hoc casts/projections in every query. ([Docs.rs][5])

---

## 3) The “format options” stack (and column-specific Parquet options)

I previously mentioned some format knobs, but there’s a deeper capability here: DataFusion defines a **precedence chain** for schema-affecting read/write options:

1. `CREATE EXTERNAL TABLE ... OPTIONS(...)`
2. `COPY ... OPTIONS(...)`
3. session-level defaults (lowest precedence) ([Apache DataFusion][6])

And it supports **column-specific Parquet options** using the `OPTION::COLUMN...` path convention (e.g., `compression::col1`, nested paths, etc.). ([Apache DataFusion][6])

Schema-relevant examples:

* CSV `SCHEMA_INFER_MAX_REC` can disable inference (force `Utf8` for all columns) ([Apache DataFusion][6])
* CSV `NULL_VALUE` / `NULL_REGEX` and time/date parsing formats directly influence which values parse and thus the resulting types ([Apache DataFusion][6])
* Parquet write options like `ENCODING`, `DICTIONARY_ENABLED`, `STATISTICS_ENABLED`, `BLOOM_FILTER_ENABLED` are not “schema” in the strict DDL sense, but they change the metadata and statistics DataFusion later uses to reason about the column’s shape and pruning behavior ([Apache DataFusion][6])

---

## 4) Parquet schema/metadata interpretation knobs you didn’t fully enumerate

There’s a substantial cluster of Parquet options that affect *how schemas are interpreted* and *how schema conflicts appear*:

* `datafusion.execution.parquet.skip_metadata`: skip optional embedded metadata in Parquet schema to avoid conflicts across files with compatible types but different metadata ([Apache DataFusion][7])
* `datafusion.execution.parquet.schema_force_view_types`: read `Utf8/Utf8Large` as `Utf8View` and `Binary/BinaryLarge` as `BinaryView` (this changes the schema) ([Apache DataFusion][7])
* `datafusion.execution.parquet.binary_as_string`: treat `Binary/LargeBinary` as `Utf8` (schema change for legacy writers) ([Apache DataFusion][7])
* `datafusion.execution.parquet.coerce_int96`: control interpretation of Parquet INT96 timestamps (schema/time semantics) ([Apache DataFusion][7])
* `datafusion.execution.parquet.skip_arrow_metadata` (writing): skip encoding embedded Arrow metadata in parquet KV metadata (impacts cross-engine schema round-tripping) ([Apache DataFusion][7])

Even things like `enable_page_index`, `bloom_filter_on_read`, `pruning`, and `pushdown_filters` aren’t “schema” per se, but they’re tightly coupled to schema-derived statistics and how DataFusion uses them during scan planning. ([Apache DataFusion][7])

---

## 5) Output schema *naming* is formally specified

DataFusion has a formal spec for how it generates **output field names** (the schema you see after projection/expressions):

* “bare columns” should not contain a qualifier (`SELECT t1.id` → `id`)
* “compound expressions” must contain relation qualifiers (e.g., `foo + bar` becomes something like `table.foo PLUS table.bar`)
* function names are lowercased (`AVG(c1)` → `avg(table.c1)`) ([Apache DataFusion][8])

If you’re building stable downstream datasets (CPG nodes/edges views), this matters because expression-heavy projections can otherwise create very noisy / unstable output schemas. ([Apache DataFusion][8])

---

## 6) Prepared statements have *typed parameters* (schema-adjacent planning)

DataFusion supports SQL `PREPARE` with explicit parameter types:

```` sql
PREPARE greater_than(INT) AS SELECT * FROM example WHERE a > $1;
EXECUTE greater_than(20);
```` :contentReference[oaicite:20]{index=20}

This is a schema capability because parameter typing influences planning/type-checking for prepared queries and enables repeated execution without re-planning in some embeddings. :contentReference[oaicite:21]{index=21}

---

## 7) DFSchema vs Arrow Schema (relation-qualified schemas)
At the logical layer DataFusion uses `DFSchema`, which wraps an Arrow schema but includes **relation (table) names** and supports mixed qualified/unqualified fields. :contentReference[oaicite:22]{index=22}

On the DataFrame API, `DataFrame.schema()` returns a `DFSchema` describing output name/type/nullability. :contentReference[oaicite:23]{index=23}

This matters for “stitching” because ambiguity rules, qualification, and join output schemas live here, not in plain Arrow schema objects. :contentReference[oaicite:24]{index=24}

---

## 8) TableProvider can expose “schema provenance” / DDL
Even if SQL `SHOW CREATE TABLE` isn’t universally available, the underlying `TableProvider` trait includes:

- `get_table_definition()` → optional CREATE statement text
- `get_logical_plan()` → optional plan backing the table/view-like provider :contentReference[oaicite:25]{index=25}

There’s a long-running lineage of work/issues to surface this more directly for external tables (e.g., implementing `get_table_definition` for `ListingTable`). :contentReference[oaicite:26]{index=26}

---

## 9) “Show me the types” knobs for debugging schemas
A minor-but-useful capability: `datafusion.format.types_info` controls whether pretty-printed batches show types. This is helpful when you’re debugging view types (`Utf8View`), nested fields, or drift. :contentReference[oaicite:27]{index=27}

---

## 10) If you want to go even deeper: metadata/extension types as schema governance
You already saw this earlier, but it’s worth flagging as “still more depth exists”: DataFusion 48+ significantly improved propagation of Arrow Field metadata and made UDF APIs operate on full `Field` (not just `DataType`), enabling user-defined/logical types via Arrow extension types and metadata-aware validation. :contentReference[oaicite:28]{index=28}

---

### The “what should I prioritize” short list for your CPG system
If your goal is *durable, stitchable datasets* (LibCST + SCIP + symtable + bytecode):

1) **Decide your table schema contract layer**: Arrow `Schema` vs DataFusion `DFSchema` semantics (qualification + output naming). :contentReference[oaicite:29]{index=29}  
2) **Pin view-type behavior** (`Utf8View` / `BinaryView`) and Parquet metadata flags so schemas don’t surprise you. :contentReference[oaicite:30]{index=30}  
3) **Plan for schema evolution** using scan-boundary schema adapters (especially if you’ll store “true schema” outside Parquet like Delta). :contentReference[oaicite:31]{index=31}  
4) Use **format options + inference controls** intentionally for CSV/JSON sources (don’t let inference drift become your “schema manager”). :contentReference[oaicite:32]{index=32}  

If you want, I can produce a “schema hardening profile” (a concrete set of `SET ...` defaults + recommended file/table registration patterns) specifically tailored to nested `LIST<STRUCT>` CPG datasets.
::contentReference[oaicite:33]{index=33}
````

[1]: https://docs.rs/datafusion/latest/datafusion/datasource/table_schema/struct.TableSchema.html "TableSchema in datafusion::datasource::table_schema - Rust"
[2]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[3]: https://github.com/apache/datafusion/issues/10398?utm_source=chatgpt.com "Support custom SchemaAdapter on ParquetExec #10398"
[4]: https://github.com/apache/arrow-rs/issues/6735?utm_source=chatgpt.com "Support for \"Schema evolution\" / Schema Adapters #6735"
[5]: https://docs.rs/deltalake/latest/deltalake/datafusion/datasource/listing/struct.ListingTable.html "ListingTable in deltalake::datafusion::datasource::listing - Rust"
[6]: https://datafusion.apache.org/user-guide/sql/format_options.html "Format Options — Apache DataFusion  documentation"
[7]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[8]: https://datafusion.apache.org/contributor-guide/specification/output-field-name-semantic.html "Output field name semantics — Apache DataFusion  documentation"

Below is a **“schema hardening profile”** for DataFusion when your primary datasets are **nested** (`LIST<STRUCT<…>>`, `STRUCT<…>`, `MAP<…>`) CPG artifacts (LibCST/SCIP/symtable/bytecode bundles), and you want:

* **stable, predictable schemas** across sessions and sources,
* **minimal “surprise types”** (especially `Utf8View` / `BinaryView`),
* **safe Parquet reading/writing** when you have many files and occasional drift,
* **good introspection** so you can verify what DataFusion thinks the schema is.

Everything here is either `SET …` (SQL) or `SessionConfig` (Python).

---

## 1) Session bootstrap: Contract-first (recommended default)

This profile optimizes for **schema stability + interoperability** rather than absolute max throughput.

### 1.1 Catalog + introspection defaults

Use a dedicated catalog/schema and enable `information_schema` so you can inspect registered schemas easily. DataFusion exposes these config keys and `information_schema` can be enabled via config. ([Apache DataFusion][1])

```sql
-- Namespace hygiene
SET datafusion.catalog.create_default_catalog_and_schema = true;
SET datafusion.catalog.default_catalog = 'cpg';
SET datafusion.catalog.default_schema  = 'public';

-- Introspection surfaces
SET datafusion.catalog.information_schema = true;
```

### 1.2 Make schema debugging “always on”

Turn on schema printing in plans + show types in batch rendering. ([Apache DataFusion][1])

```sql
SET datafusion.explain.show_schema = true;
SET datafusion.format.types_info = true;
```

### 1.3 Keep strict schema verification (don’t paper over mismatches)

DataFusion has a schema verification check around aggregates; keep it enabled (default is `false` for “skip…”). Only flip this if you hit a known planner bug and need a temporary workaround. ([Apache DataFusion][1])

```sql
SET datafusion.execution.skip_physical_aggregate_schema_check = false;
```

### 1.4 Normalize time semantics

If you ever emit timestamps (pipeline run tables, etc.), pin the session timezone. ([Apache DataFusion][2])

```sql
SET datafusion.execution.time_zone = 'UTC';
```

### 1.5 “No view types” mode (reduces schema surprises)

If you want `Utf8` / `Binary` rather than `Utf8View` / `BinaryView`, you need to control **both** SQL type mapping and Parquet read mapping:

* `datafusion.sql_parser.map_string_types_to_utf8view` (introduced in 49.0.0, default `true`) controls how SQL string types map during planning. ([Apache DataFusion][3])
* `datafusion.execution.parquet.schema_force_view_types` (default `true`) controls how Parquet strings/binary are read. ([Apache DataFusion][2])

```sql
SET datafusion.sql_parser.map_string_types_to_utf8view = false;
SET datafusion.execution.parquet.schema_force_view_types = false;
```

---

## 2) Parquet hardening knobs for many-file datasets (CPG bundles)

These matter a lot when your CPG tables are “many files per dataset.”

### 2.1 Avoid schema conflicts from embedded metadata

If you read many parquet files that have compatible physical types but different embedded metadata, `skip_metadata` can reduce conflicts (default `true`). ([Apache DataFusion][2])

```sql
SET datafusion.execution.parquet.skip_metadata = true;
```

If you **intentionally rely on Arrow field metadata / extension typing** stored in Parquet schema metadata, flip this to `false` and ensure your writers emit consistent metadata across all files. (Otherwise you’ll reintroduce “metadata conflict” failures.) ([Apache DataFusion][2])

### 2.2 Only enable “binary_as_string” when you need it

Some legacy writers store strings as Parquet `BINARY` without the UTF8 annotation; DataFusion can interpret those as strings with `binary_as_string`. ([Apache DataFusion][2])

```sql
-- default is false; set true only for those specific datasets
SET datafusion.execution.parquet.binary_as_string = false;
```

### 2.3 Preserve Arrow metadata when writing Parquet

If you write Parquet from DataFusion, the config includes `skip_arrow_metadata` (default `false`). Leaving it `false` keeps Arrow metadata encoded in Parquet KV metadata. ([Apache DataFusion][2])

```sql
SET datafusion.execution.parquet.skip_arrow_metadata = false;
```

### 2.4 Statistics collection: choose “fast registration” vs “faster queries”

DataFusion can collect file statistics when a table is created; it’s useful for planning but can make “table creation” expensive. The DDL docs explicitly call this out and show how to disable it via `SET datafusion.execution.collect_statistics = false`. ([Apache DataFusion][4])

For CPG datasets (many small files, frequent re-registration), I recommend:

* **Ingest/iteration sessions:** disable statistics
* **Stable production registry sessions:** enable statistics

```sql
-- ingest / dev iteration
SET datafusion.execution.collect_statistics = false;

-- production / long-lived catalog
-- SET datafusion.execution.collect_statistics = true;
```

---

## 3) Optional “Arrow-performance” variant (embrace view types, convert at boundaries)

If you want maximum performance on string-heavy data, keep view types enabled:

* SQL mapping to `Utf8View` default true ([Apache DataFusion][3])
* Parquet read as view types default true ([Apache DataFusion][2])

```sql
SET datafusion.sql_parser.map_string_types_to_utf8view = true;
SET datafusion.execution.parquet.schema_force_view_types = true;
```

If you need to **coerce view types to non-view types at output**, there’s an optimizer option `expand_views_at_output` that coerces `Utf8View → LargeUtf8` and `BinaryView → LargeBinary`. ([Docs.rs][5])
Be aware there have been edge cases where enabling it changes output column naming due to inserted casts. ([GitHub][6])

---

## 4) Recommended registration patterns for nested CPG bundle tables

### Pattern A: Register in-memory Arrow batches (fast, exact schema)

Build a `pyarrow.Table` with your nested schema and register it. DataFusion Python exposes `register_record_batches` for this. ([Apache DataFusion][7])

```python
import pyarrow as pa
from datafusion import SessionContext

ctx = SessionContext()

# libcst_files_table: pa.Table with schema like:
# repo: string, path: string, file_id: uint64,
# nodes: list<struct<...>>, edges: list<struct<...>>, attrs: map<string,string>

batches = libcst_files_table.to_batches()
ctx.register_record_batches("libcst_files", [batches])
```

### Pattern B: Persist bundles as Parquet, then register with CREATE EXTERNAL TABLE

Use Parquet for storage, but keep **bundle tables** nested. Control read behavior via global `SET` (above) or table-level `OPTIONS` (Format Options support precedence for `CREATE EXTERNAL TABLE` / `COPY` / `INSERT`). ([Apache DataFusion][8])

Example (Parquet):

```sql
CREATE EXTERNAL TABLE libcst_files
STORED AS PARQUET
LOCATION 's3://…/libcst_files/'
OPTIONS (
  'skip_metadata' 'true',
  'schema_force_view_types' 'false'
);
```

(Those Parquet options are documented as `SKIP_METADATA`, `SCHEMA_FORCE_VIEW_TYPES`, etc., and can be supplied via `OPTIONS`. ([Apache DataFusion][8]))

---

## 5) “Query-hardening” pattern: keep bundles nested, expose 2D views for joins

Your best practice for CPG querying is:

* **Store** nested bundles (`*_files`: one row per file with `nodes: LIST<STRUCT>`, `edges: LIST<STRUCT>`)
* **Query** via views that explode lists

DataFusion’s `unnest` is the key primitive for exploding arrays/maps into rows. ([Apache DataFusion][9])
And DataFusion supports `CREATE VIEW` for virtual tables. ([Apache DataFusion][4])

```sql
CREATE OR REPLACE VIEW libcst_nodes AS
SELECT
  file_id,
  path,
  n
FROM libcst_files
CROSS JOIN unnest(nodes) AS n;

CREATE OR REPLACE VIEW libcst_edges AS
SELECT
  file_id,
  path,
  e
FROM libcst_files
CROSS JOIN unnest(edges) AS e;
```

Now you do all stitching / graph queries against `libcst_nodes` and `libcst_edges` (2D relations), while the underlying storage remains nested.

---

## 6) Built-in schema verification workflow (use this every time you tweak schemas)

Once `information_schema` is enabled, you can inspect table schemas via `SHOW COLUMNS` or `information_schema.columns`. ([Apache DataFusion][10])
You can also inspect session config via `SHOW ALL` / `information_schema.df_settings`. ([Apache DataFusion][10])

```sql
SHOW ALL;
SHOW COLUMNS FROM libcst_files;

SELECT * FROM information_schema.columns WHERE table_name = 'libcst_files';
SELECT * FROM information_schema.df_settings;
```

When something looks off, run `EXPLAIN` with schema printing enabled (via `datafusion.explain.show_schema`). ([Apache DataFusion][1])

---


[1]: https://datafusion.apache.org/user-guide/configs.html?utm_source=chatgpt.com "Configuration Settings — Apache DataFusion documentation"
[2]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/library-user-guide/upgrading.html?utm_source=chatgpt.com "Upgrade Guides — Apache DataFusion documentation"
[4]: https://datafusion.apache.org/user-guide/sql/ddl.html?utm_source=chatgpt.com "DDL — Apache DataFusion documentation"
[5]: https://docs.rs/datafusion/latest/datafusion/common/config/struct.OptimizerOptions.html?utm_source=chatgpt.com "OptimizerOptions in datafusion::common::config - Rust"
[6]: https://github.com/apache/datafusion/issues/18818?utm_source=chatgpt.com "Enabling expand_views_at_output config changes column ..."
[7]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html?utm_source=chatgpt.com "datafusion.context — Apache Arrow DataFusion documentation"
[8]: https://datafusion.apache.org/user-guide/sql/format_options.html?utm_source=chatgpt.com "Format Options — Apache DataFusion documentation"
[9]: https://datafusion.apache.org/user-guide/sql/special_functions.html?utm_source=chatgpt.com "Special Functions — Apache DataFusion documentation"
[10]: https://datafusion.apache.org/user-guide/sql/information_schema.html?utm_source=chatgpt.com "Information Schema — Apache DataFusion documentation"

Here are the **additional schema-related DataFusion capabilities** that are *still worth considering* beyond what we’ve already covered. I’m focusing on things that either (a) materially change how schemas are **discovered/derived**, (b) enable **schema evolution/adaptation**, or (c) affect **correctness/stability** of schemas in real systems.

---

## 1) “URL tables” / dynamic file catalog (schema-on-read without registration)

DataFusion can be configured to let users query files **directly as tables** in SQL:

```sql
SELECT * FROM 'my_file.parquet'
```

This is gated behind a security-sensitive feature (`SessionContext.enable_url_table`) and uses a “dynamic file catalog” internally. ([Docs.rs][1])

Why it’s schema-relevant:

* DataFusion must infer/resolve the **schema from the referenced file** at planning time (or near it).
* It’s a *third* schema discovery mode besides (1) explicit registration and (2) auto-loading via catalog scanning.

Practical knobs / surfaces:

* SQL setting `datafusion.catalog.files_as_tables` exists (at least in CLI contexts). ([GitHub][2])
* CLI docs describe querying files/directories/remote locations by quoting the path. ([Apache DataFusion][3])

---

## 2) `DESCRIBE <query>` now returns the computed output schema (not a plan)

This is a **newer schema introspection capability**: `DESCRIBE` on an arbitrary query returns the **computed schema** of the query (previously it behaved like `EXPLAIN`). ([Apache DataFusion][4])

Why it matters:

* It’s the fastest way to validate your *derived schema* when you’re building CPG “views” (e.g., `cpg_nodes`, `cpg_edges`) without executing the query.

---

## 3) Scan-time “projection expressions” (not just column indices)

DataFusion introduced `ProjectionExprs`: projections aren’t limited to `Vec<usize>` column indices anymore—projections can be **arbitrary physical expressions** evaluated during scanning. ([Apache DataFusion][5])

Why it’s schema-related:

* You can effectively “reshape” schema *at the scan boundary* (compute derived columns, reorder, cast) in a way that’s closer to a schema adapter than a normal post-scan projection.
* This becomes relevant when you’re trying to standardize nested CPG bundle schemas across multiple file versions.

---

## 4) Schema evolution/adaptation is shifting from `SchemaAdapter` → `PhysicalExprAdapterFactory`

You (and I) discussed “SchemaAdapterFactory / SchemaAdapter” earlier—but DataFusion has been actively **moving away from that API**:

* The `schema_adapter` module is marked deprecated/removed in newer DataFusion docs, and `SchemaMapping` is removed with guidance to use **PhysicalExprAdapterFactory** instead. ([Docs.rs][6])
* There’s an explicit plan/issue to replace `SchemaAdapter` with `PhysicalExprAdapter`. ([GitHub][7])
* The dedicated crate `datafusion-physical-expr-adapter` exists (and is published) to provide these schema adaptation utilities. ([Crates.io][8])

Why it matters for your CPG datasets:

* If you expect schema drift across Parquet files (new fields inside `STRUCT`, reordering, changed partition columns), the “best in class” path is now trending toward **rewriting physical expressions / adapting batches** to a target schema, rather than relying on older schema-adapter hooks.

---

## 5) Partition schema validation and partition inference “gotchas”

### 5.1 Validate partitions against directory structure

`ListingOptions.validate_partitions(...)` can infer partition columns from `LOCATION` and compare them to the `PARTITIONED BY` columns to prevent accidental corruption/mismatches. ([Docs.rs][9])

This is a real schema-management feature if you use Hive-style partitioning for any of your datasets (even “meta tables” like run registries, manifests, etc.).

### 5.2 Auto-infer Hive partitions now changes schema by default

DataFusion 50 automatically infers Hive partitions when using `ListingTableFactory` and `CREATE EXTERNAL TABLE`, meaning partition columns may now appear in the table schema when they didn’t previously. This can be disabled via `datafusion.execution.listing_table_factory_infer_partitions=false`. ([Apache DataFusion][4])

---

## 6) Schema *discovery caching* for listing tables (staleness vs stability)

`ListingTableProvider` now caches object-store `LIST` results for its lifetime (or until TTL expiry), with runtime knobs:

* `datafusion.runtime.list_files_cache_limit`
* `datafusion.runtime.list_files_cache_ttl` ([Apache DataFusion][5])

Why it’s schema-relevant:

* “What files are in the dataset?” is part of table identity and sometimes schema (when schemas differ by file version).
* Caching can make your table appear “stable” (good) but also “stale” (surprising) if files are added/removed. ([Apache DataFusion][5])

---

## 7) Provider-level schema metadata you can exploit more: defaults + DDL provenance

The `TableProvider` / `TableSource` APIs include schema-adjacent hooks you can use to build a richer catalog experience:

* `get_table_definition()` → return the CREATE statement (enables a future `SHOW CREATE TABLE` style UX) ([Docs.rs][10])
* `get_logical_plan()` → for view-like providers ([Docs.rs][10])
* `get_column_default(column)` → default expressions if available ([Docs.rs][10])

If you’re building a registry-backed metastore, these are the “schema provenance” hooks that let your registry surface more than just Arrow types.

---

## 8) Nested types are still an active frontier (practical caution)

Even though DataFusion supports `STRUCT/LIST/MAP` and you can model CPG bundles naturally, nested-field access and nested projections still have sharp edges depending on version and expression shapes:

* There’s an epic tracking “proper support” for struct/list/map/union and nested identifier access. ([GitHub][11])
* Users still hit “Nested identifiers not yet supported” in some contexts/versions. ([GitHub][12])

This is less a “feature” than a maturity note: it should influence whether you store *everything* nested vs provide exploded relational views as the main query surface.

---

### If you want one more deliverable

I can take these “remaining capabilities” and turn them into:

* a **version-aware compatibility matrix** (what to rely on in DataFusion 43/50/52/53-ish),
* and a **recommended operating mode** for your CPG pipeline (nested bundles as storage + exploded views as query surface + physical-expr adapter for evolution).

[1]: https://docs.rs/datafusion/latest/datafusion/execution/context/struct.SessionContext.html?utm_source=chatgpt.com "SessionContext in datafusion::execution::context - Rust"
[2]: https://github.com/apache/arrow-datafusion/issues/4850?utm_source=chatgpt.com "Support `select .. from 'data.parquet'` files in SQL from any ` ..."
[3]: https://datafusion.apache.org/user-guide/cli/datasources.html?utm_source=chatgpt.com "Local Files / Directories — Apache DataFusion documentation"
[4]: https://datafusion.apache.org/library-user-guide/upgrading.html?utm_source=chatgpt.com "Upgrade Guides — Apache DataFusion documentation"
[5]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[6]: https://docs.rs/datafusion/latest/datafusion/datasource/index.html?utm_source=chatgpt.com "datafusion::datasource - Rust"
[7]: https://github.com/apache/datafusion/issues/16800?utm_source=chatgpt.com "Plan to replace `SchemaAdapter` with `PhysicalExprAdapter`"
[8]: https://crates.io/crates/datafusion-physical-expr-adapter/52.0.0?utm_source=chatgpt.com "datafusion-physical-expr-adapter"
[9]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingOptions.html?utm_source=chatgpt.com "ListingOptions in datafusion::datasource::listing - Rust"
[10]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html?utm_source=chatgpt.com "TableProvider in datafusion::datasource - Rust"
[11]: https://github.com/apache/datafusion/issues/2326?utm_source=chatgpt.com "Issue #2326 · apache/datafusion"
[12]: https://github.com/apache/datafusion/issues/14768?utm_source=chatgpt.com "Nested Fields Access on StructArray field not working"

Below is a technical deep dive on the three topics:

* **5) Partition schema validation + partition inference gotchas**
* **6) Listing-table file discovery caching (staleness vs stability)**
* **7) Provider-level schema metadata (defaults + DDL provenance)**

I’ll anchor this in the **ListingTable / object-store scan path**, because that’s where these features matter most.

---

## 5) Partition schema validation and partition inference gotchas

### 5.1 What DataFusion means by “partition columns”

For `ListingTable`-style tables (directories/prefixes of files), DataFusion’s **partition columns are *not* read from Parquet/CSV/…**; they are **derived solely from the file path** (Hive-style `col=value` directories). The docs call this out explicitly. ([Docs.rs][1])

Implications:

* You can have a Parquet file schema that contains *no* `year` column, but a table schema that *does* include `year` if the file is in `.../year=2024/...`.
* If your directory layout is wrong/inconsistent, your “table schema” can be wrong even if every file’s physical schema is fine.

### 5.2 The three schema layers (TableSchema): file schema vs partition cols vs table schema

DataFusion introduced **`TableSchema`** to explicitly separate:

* **file schema** (what’s in the data files)
* **partition columns** (derived from directories)
* **table schema** (file schema + partition columns) ([Apache DataFusion][2])

This matters because file sources now take schema **up front**, and if you’re implementing custom `FileSource` you’re expected to decide whether you need `file_schema()` vs `table_schema()` vs `table_partition_cols()`. ([Apache DataFusion][2])

### 5.3 Inference APIs: what’s inferred and what’s not

`ListingOptions` exposes three related async helpers:

* **`infer_schema(...)`**: infers and merges schema from the files; *explicitly does not include partition columns*. ([Docs.rs][1])
* **`infer_partitions(...)`**: infers partition columns from path; it’s explicitly a **best-effort** optimization and “may fail to detect invalid partitioning” because it doesn’t read all files. ([Docs.rs][1])
* **`validate_partitions(...)`**: infers partitions from `LOCATION` and compares them against `PARTITIONED BY` to prevent accidental corruption; it “allows specifying partial partitions.” ([Docs.rs][1])

The most important “gotcha” here is: **inference can be incomplete**, and **schema inference doesn’t automatically “know” partitions**. If you want the *table schema* to include partition cols, you must explicitly provide them (or enable the factory-level inference discussed next).

### 5.4 The DataFusion 50 behavior change: auto-infer Hive partitions (schema changes!)

Starting with **DataFusion 50**, `ListingTableFactory` + `CREATE EXTERNAL TABLE` **automatically infer Hive partitions** and include those columns in the table schema/data by default. ([Apache DataFusion][2])

* Controlled by: `datafusion.execution.listing_table_factory_infer_partitions` (default `true`). ([Apache DataFusion][3])
* If you want pre-50 behavior (no inferred hive cols): set it to `false`. ([Apache DataFusion][2])

**Why this bites people:** upgrading can silently add columns (partition keys) to your table schema, which can:

* change `SELECT *` output,
* introduce duplicate/ambiguous names if your file schema already contains a same-named field,
* or alter downstream views that assumed the old schema.

### 5.5 Directory scanning gotcha: ignoring subdirectories

When scanning directories for data files, DataFusion has `datafusion.execution.listing_table_ignore_subdirectory` (default `true`) to ignore subdirectories, “consistent with Hive”; docs note this **does not affect reading partitioned tables** like `/table/year=2021/month=01/data.parquet`. ([Apache DataFusion][3])

This is relevant if you store CPG artifacts in nested folder trees that are *not* Hive partitions (e.g., sharding by hash prefix): you may need to flip this to ensure you actually read all files.

### 5.6 Validation failure mode: partition column ordering matters

A concrete historic pitfall: creating an external table with `PARTITIONED BY` columns in a different order than the directory structure could lead to a **runtime panic** instead of an error in older versions (example issue demonstrates wrong ordering not rejected at creation, later panics during query). ([GitHub][4])

**Best practice for hardening:**

* Prefer **auto-inference** (when it matches your layout) rather than manually specifying partition columns in fragile order.
* If you do specify `PARTITIONED BY`, add a registration-time “smoke query” (or call `validate_partitions(...)` in Rust) to fail fast. ([Docs.rs][1])

---

## 6) Schema discovery caching for ListingTable (staleness vs stability)

### 6.1 What is cached: object-store `LIST` results

DataFusion’s `ListingTableProvider` used to call `LIST` on the object store repeatedly; now it **caches the results of `LIST`** for performance. The upgrade guide is explicit:

* Cache lasts for the **lifetime of the ListingTableProvider** or until cache entry expires. ([Apache DataFusion][2])
* **Default is no expiration**; if files are added/removed, the provider won’t see changes until the provider is dropped/recreated. ([Apache DataFusion][2])

### 6.2 Runtime knobs: cache limit + TTL (and disabling)

The cache is controlled via runtime config:

* `datafusion.runtime.list_files_cache_limit` (default `1M`): max memory for list-files cache. ([Apache DataFusion][3])
* `datafusion.runtime.list_files_cache_ttl` (default `NULL`): TTL for entries; supports minute/second units. ([Apache DataFusion][3])

The upgrade guide notes **you can disable caching** by setting the limit to 0 (example: `0K`). ([Apache DataFusion][2])

### 6.3 How the default cache improves partition pruning

`DefaultListFilesCache` supports **prefix-aware lookups**: it caches a full listing for a table base path and then filters that cached list for a requested prefix. The docs call out the point:

> “This enables efficient partition pruning - a single cached listing of the full table can serve queries for any partition subset without additional storage calls.” ([Docs.rs][5])

That’s the core synergy: hive-partition pruning often needs “which files exist under this prefix,” and prefix filtering avoids expensive re-LIST calls.

### 6.4 The staleness problem got sharper with session/global cache

There’s an active discussion that moving list caching to a session/global level created a confusing outcome: dropping and recreating an external table may reuse the cached list rather than re-LISTing, removing an easy “refresh mechanism.” ([GitHub][6])

**Operational consequences:**

* For **append-only** pipelines (new CPG shard files appear), “queries don’t see new shards” until TTL expiry (if any) or explicit invalidation.
* For **mutable** datasets (compaction / deletes), you can see “ghost files” until refresh.

### 6.5 Practical tuning guidance for CPG datasets

Think in terms of workload:

**A) Immutable snapshot datasets (recommended for CPG runs)**

* Treat each run output as immutable (e.g., `runs/run_id=.../libcst_files/part-*.parquet`)
* Keep TTL = `NULL` (no expiration) for maximum performance and stable results
* New run → new table location or new SessionContext/catalog snapshot

**B) Append-only location (one logical table grows forever)**

* Set a TTL (`30s`, `2m`, etc.) so new shards appear reasonably quickly ([Apache DataFusion][3])
* Or explicitly disable cache for correctness during ingestion windows (`0K`) ([Apache DataFusion][2])

---

## 7) Provider-level schema metadata: defaults + DDL provenance

This is the “make schemas *self-describing*” layer, and it’s where you get **best-in-class registry behavior**.

### 7.1 What DataFusion exposes on `TableProvider`

Beyond `schema()` and `scan()`, the `TableProvider` trait includes optional metadata methods:

* **`get_table_definition() -> Option<&str>`**
  “Get the create statement used to create this table, if available.” ([Docs.rs][7])
* **`get_logical_plan() -> Option<Cow<LogicalPlan>>`**
  “Get the LogicalPlan of this table, if available.” ([Docs.rs][7])
* **`get_column_default(column) -> Option<&Expr>`**
  “Get the default value for a column, if available.” ([Docs.rs][7])
* Plus `constraints()` and others (informational constraints, etc.). ([Docs.rs][7])

### 7.2 What you can do with `get_table_definition` (DDL provenance)

DataFusion doesn’t universally provide `SHOW CREATE TABLE` today, but the intended path is clear: there’s a long-standing feature request to implement it for external tables by implementing `TableProvider::get_table_definition` for `ListingTable`. ([GitHub][8])

For your CPG registry, this is very practical even without SQL support:

* Store canonical `CREATE EXTERNAL TABLE ... OPTIONS(...)` text in your registry
* In your custom provider, return it from `get_table_definition()`
* Use it for auditability (“how was this table defined?”) and exact reproducibility

### 7.3 What you can do with `get_logical_plan` (views + derived datasets)

If you build view-like datasets (e.g., `cpg_nodes`/`cpg_edges` views), exposing a `LogicalPlan` lets you treat “a table” as “a plan” for provenance and advanced refresh semantics. The API explicitly models that some tables (views) have logical plans while raw files generally do not. ([Docs.rs][7])

This becomes powerful if you want a registry entry like:

* dataset `cpg_nodes` is defined as a plan over `libcst_files` + `scip_index`
* your provider returns that plan so you can introspect and rebuild deterministically

### 7.4 Column defaults as schema contracts (and why they’re useful even if DF doesn’t enforce them)

`get_column_default` gives you a way to declare “missing value policy” as part of the provider’s schema contract. DataFusion exposes the hook; how it’s applied is provider-specific and often used by downstream systems / custom optimizers. ([Docs.rs][7])

For a CPG system, defaults are useful for:

* evolving nested structs/maps without breaking older readers (e.g., new `attrs['extractor_version']` default)
* ensuring inserts/materializations fill required metadata fields consistently

### 7.5 How to exploit these hooks in practice

If you implement a custom provider (Rust, then expose to Python), the DataFusion docs describe the pattern: implement `TableProvider` (and an `ExecutionPlan`) and register it with `SessionContext`. ([Apache DataFusion][9])

Once you do that, you can:

* lock down `schema()` to your canonical nested Arrow schema
* enforce evolution in `scan_with_args` / scan path
* expose provenance via `get_table_definition` and `get_logical_plan`
* expose defaults via `get_column_default` ([Docs.rs][7])

---

## Putting it together for your use case

For **nested CPG bundle tables** on object storage:

* **Partitions:** use Hive partitions only when you truly want partition pruning, and assume *partitions live in path*, not file schema. Validate or infer carefully, and be aware DataFusion 50 changed default behavior. ([Docs.rs][1])
* **Listing cache:** treat list caching as a *consistency choice* (freshness vs stability). Configure TTL/limit (or disable) explicitly per workload. ([Apache DataFusion][2])
* **Provider metadata:** if you want “real metastore” behavior, implement a provider that returns DDL + defaults + plan provenance; DataFusion has explicit hooks for that. ([Docs.rs][7])

If you want, I can also sketch a concrete “CPG registry table provider” interface: given `(dataset_name, run_id)` it returns a `TableProvider` whose `schema()` is your nested canonical schema, whose `get_table_definition()` returns the exact `CREATE EXTERNAL TABLE` text (with options), and whose scan path either (a) uses ListingTable with tuned list-files cache or (b) bypasses listing entirely using a registry-managed file manifest.

[1]: https://docs.rs/datafusion/latest/datafusion/datasource/listing/struct.ListingOptions.html "ListingOptions in datafusion::datasource::listing - Rust"
[2]: https://datafusion.apache.org/library-user-guide/upgrading.html "Upgrade Guides — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/user-guide/configs.html "Configuration Settings — Apache DataFusion  documentation"
[4]: https://github.com/apache/datafusion/issues/9785 "[BUG] Panic when querying table with wrong partition columns order · Issue #9785 · apache/datafusion · GitHub"
[5]: https://docs.rs/datafusion/latest/datafusion/execution/cache/struct.DefaultListFilesCache.html "DefaultListFilesCache in datafusion::execution::cache - Rust"
[6]: https://github.com/apache/datafusion/issues/19573 "Confusing behavior now required to to refresh the files of a listing table · Issue #19573 · apache/datafusion · GitHub"
[7]: https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html "TableProvider in datafusion::datasource - Rust"
[8]: https://github.com/apache/arrow-datafusion/issues/2848?utm_source=chatgpt.com "Implement \"SHOW CREATE TABLE\" for external tables"
[9]: https://datafusion.apache.org/library-user-guide/custom-table-providers.html "Custom Table Provider — Apache DataFusion  documentation"
