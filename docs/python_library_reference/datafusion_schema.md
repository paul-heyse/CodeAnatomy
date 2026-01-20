
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
