Yes — you can “stitch” **SCIP + LibCST** in DataFusion **without rewriting either physical dataset** by doing **logical stitching**:

* keep each dataset registered as-is (LibCST remains nested; SCIP outputs are already flat),
* define **views** that *project/explode* the parts you need,
* define **bridge edges** via joins (file + span),
* optionally expose the result as either:

  * **two virtual relations** (`cpg_nodes`, `cpg_edges`) *(most scalable)*, or
  * a **single “graph object” column** `STRUCT<nodes: LIST<…>, edges: LIST<…>>` using `array_agg` *(nice for “one object”, but can get huge)*.

DataFusion supports **CREATE VIEW** (virtual tables) so you’re not materializing a new stored schema; you’re defining a derived, query-time schema. ([datafusion.apache.org][1])

---

## 1) The key move: normalize to a *common node/edge shape* at query time

You don’t change your underlying `scip_*` / `libcst_files` schemas — you just *project* them into a common interface.

DataFusion has:

* nested field access (`col['field']`) and nested types support ([datafusion.apache.org][2])
* `unnest(list)` to explode `LIST<…>` into rows, and `unnest(struct)` if you want “wide” columns ([datafusion.apache.org][3])
* struct + map constructors (`named_struct`, `struct`, `map`, `map_entries`, `map_extract`, …) ([GitHub][4])
* set ops like `UNION ALL` ([datafusion.apache.org][2])

---

## 2) Build row views (no new physical schema)

### 2.1 SCIP occurrences (flat table with a span projection)

```sql
CREATE OR REPLACE VIEW scip_occurrence_spans AS
SELECT
  document_id,
  path,
  symbol,
  symbol_roles,
  named_struct(
    'start', named_struct('line0', start_line, 'col', start_char),
    'end', named_struct('line0', end_line, 'col', end_char),
    'end_exclusive', end_exclusive,
    'col_unit', col_unit
  ) AS span
FROM scip_occurrences;
```

SCIP occurrences are already flat in CodeAnatomy; this view just builds a span struct.
Nested access uses `[...]` field access (supported for nested types). ([datafusion.apache.org][2])

### 2.2 LibCST nodes (from `libcst_files.nodes[*]`)

```sql
CREATE OR REPLACE VIEW libcst_nodes AS
SELECT
  f.file_id,
  f.path,
  n['cst_id']  AS cst_id,
  n['kind']    AS kind,
  n['span']    AS span,
  n['attrs']   AS attrs
FROM libcst_files f
CROSS JOIN unnest(f.nodes) AS n;
```

---

## 3) Stitching = create **bridge edges** (SCIP occurrence ↔ LibCST token node) as a view

If you want “no new schema”, you still need *some* deterministic rule for joining. The best “first pass” is **exact span match** (fast, unambiguous) for identifier-like CST nodes.

```sql
CREATE OR REPLACE VIEW cpg_bridge_cst_scip AS
SELECT
  l.file_id,
  -- canonical node ids as STRUCT<ns, id> so they union cleanly
  named_struct('ns','libcst','id', CAST(l.cst_id AS VARCHAR)) AS src,
  named_struct(
    'ns','scip',
    'path', s.path,
    'symbol', s.symbol,
    'start_line', s.span['start']['line0'],
    'start_col', s.span['start']['col'],
    'end_line', s.span['end']['line0'],
    'end_col', s.span['end']['col']
  ) AS dst,
  'CST_SPAN_MATCH_SCIP_OCC' AS kind,
  map(['match'], ['exact_span']) AS attrs
FROM libcst_nodes l
JOIN scip_occurrence_spans s
  ON l.path = s.path
 AND l.span['start']['line0'] = s.span['start']['line0']
 AND l.span['start']['col']   = s.span['start']['col']
 AND l.span['end']['line0']   = s.span['end']['line0']
 AND l.span['end']['col']     = s.span['end']['col']
WHERE l.kind IN ('libcst.Name', 'libcst.Attribute');
```

This uses:

* `named_struct` to build a stable structured ID ([GitHub][4])
* `map(...)` to attach edge metadata ([GitHub][4])
* nested field access (`span['start']['line0']`, …) ([datafusion.apache.org][2])

---

## 4) Define the CPG as **two stitched views**: `cpg_nodes` + `cpg_edges`

### 4.1 Nodes = union of (LibCST nodes) + (SCIP occurrences) + (optionally SCIP symbols)

```sql
CREATE OR REPLACE VIEW cpg_nodes AS
SELECT
  file_id,
  named_struct('ns','libcst','id', CAST(cst_id AS VARCHAR)) AS node_id,
  kind,
  span,
  attrs
FROM libcst_nodes

UNION ALL

SELECT
  CAST(NULL AS BIGINT) AS file_id,  -- or derive file_id from path if you have a file dimension table
  named_struct(
    'ns','scip',
    'path', path,
    'symbol', symbol,
    'start_line', span['start']['line0'],
    'start_col', span['start']['col'],
    'end_line', span['end']['line0'],
    'end_col', span['end']['col']
  ) AS node_id,
  'scip.Occurrence' AS kind,
  span,
  map(
    ['symbol','roles'],
    [symbol, CAST(symbol_roles AS VARCHAR)]
  ) AS attrs
FROM scip_occurrence_spans;
```

`UNION ALL` is supported as a set operation. ([datafusion.apache.org][2])
Struct/map constructors come from DataFusion scalar functions. ([GitHub][4])

### 4.2 Edges = (LibCST edges) + (SCIP semantic edges) + (bridge edges)

If `libcst_files.edges` exists (tree edges), explode it similarly; then union with the bridge edges above.

```sql
CREATE OR REPLACE VIEW libcst_edges AS
SELECT
  f.file_id,
  named_struct('ns','libcst','id', CAST(e['src'] AS VARCHAR)) AS src,
  named_struct('ns','libcst','id', CAST(e['dst'] AS VARCHAR)) AS dst,
  e['kind'] AS kind,
  e['attrs'] AS attrs
FROM libcst_files f
CROSS JOIN unnest(f.edges) AS e;

CREATE OR REPLACE VIEW cpg_edges AS
SELECT file_id, src, dst, kind, attrs FROM libcst_edges
UNION ALL
SELECT file_id, src, dst, kind, attrs FROM cpg_bridge_cst_scip;
```

---

## 5) If you truly want a **single “graph object” column**, aggregate into arrays

DataFusion has `array_agg(expression [ORDER BY …])` which returns an array of the aggregated expression values. ([GitHub][5])

A common pattern is **one row per file**:

```sql
CREATE OR REPLACE VIEW cpg_graph_by_file AS
SELECT
  file_id,
  named_struct(
    'nodes', array_agg(n ORDER BY n['node_id']['id']),
    'edges', array_agg(e ORDER BY e['kind'])
  ) AS cpg
FROM
  (SELECT file_id, named_struct('node_id', node_id, 'kind', kind, 'span', span, 'attrs', attrs) AS n
   FROM cpg_nodes) nn
JOIN
  (SELECT file_id, named_struct('src', src, 'dst', dst, 'kind', kind, 'attrs', attrs) AS e
   FROM cpg_edges) ee
USING (file_id)
GROUP BY file_id;
```

This yields: `cpg: STRUCT<nodes: LIST<STRUCT…>, edges: LIST<STRUCT…>>` — a literal “single object” per file.

---

## 6) “Configure it in DataFusion” from Python

You can create these views purely via SQL (`CREATE VIEW …`) ([datafusion.apache.org][1]), or you can build a DataFrame and `register_view` it. ([datafusion.apache.org][6])

Conceptually:

```python
ctx.sql("CREATE OR REPLACE VIEW scip_occurrences AS ...")
ctx.sql("CREATE OR REPLACE VIEW libcst_nodes AS ...")
ctx.sql("CREATE OR REPLACE VIEW cpg_nodes AS ...")
ctx.sql("CREATE OR REPLACE VIEW cpg_edges AS ...")

# Now “the CPG” is just: ctx.table("cpg_nodes") + ctx.table("cpg_edges")
# or: ctx.table("cpg_graph_by_file")
```

---

## Practical recommendation

* Treat the CPG as **two relations** (`nodes`, `edges`) as your “singular object” (a *logical object*, not a materialized file). It scales, and DataFusion is fundamentally relational.
* Only build `cpg_graph_by_file` (arrays-of-structs) when you need to *export a self-contained graph blob* per file/repo (e.g., hand to a graph library in one round trip).

If you tell me what you’re using as the **file join key** today (`file_id`? `repo+path`? content hash?), I’ll tighten the stitching SQL to be fully consistent (and show the “span containment” version that matches occurrences to the *smallest enclosing* LibCST node, not just exact-span).

[1]: https://datafusion.apache.org/user-guide/sql/ddl.html?utm_source=chatgpt.com "DDL — Apache DataFusion documentation"
[2]: https://datafusion.apache.org/user-guide/features.html "Features — Apache DataFusion  documentation"
[3]: https://datafusion.apache.org/user-guide/sql/special_functions.html?utm_source=chatgpt.com "Special Functions — Apache DataFusion documentation"
[4]: https://raw.githubusercontent.com/apache/datafusion/main/docs/source/user-guide/sql/scalar_functions.md "raw.githubusercontent.com"
[5]: https://raw.githubusercontent.com/apache/datafusion/main/docs/source/user-guide/sql/aggregate_functions.md "raw.githubusercontent.com"
[6]: https://datafusion.apache.org/python/user-guide/common-operations/views.html "Registering Views — Apache Arrow DataFusion  documentation"

DataFusion doesn’t “register a schema” separately — it registers a **table-like Arrow source** (record batches / dataset / provider), and the **Arrow schema (including nested `struct` / `list` / `map`) rides along with the batches**.

For your `symtable_files_schema` (which includes `blocks: list<struct<...>>`, and maps inside), the two most direct registration patterns in **datafusion-python** are:

---

## Option A (most common): register Arrow RecordBatches as a table

`SessionContext.register_record_batches(name, partitions)` takes **`list[list[pyarrow.RecordBatch]]`** and registers them as a SQL table. DataFusion “converts the provided partitions into a table and registers it into the session.” ([datafusion.apache.org][1])

```python
import pyarrow as pa
from datafusion import SessionContext

# You already defined:
#   symtable_files_schema = pa.schema([...])  # with nested list/struct/map types

# 1) Build a pyarrow.Table that conforms to symtable_files_schema
rows = [
    {
        "repo": "myrepo",
        "path": "src/foo.py",
        "file_id": 123,
        "blocks": [
            {
                "block_id": 1,
                "parent_block_id": None,
                "block_type": "module",
                "name": "foo",
                "lineno1": 1,
                "span_hint": None,  # or dict matching span_t struct
                "symbols": [
                    {
                        "name": "x",
                        "flags": {
                            "is_referenced": True,
                            "is_imported": False,
                            "is_parameter": False,
                            "is_type_parameter": False,
                            "is_global": False,
                            "is_nonlocal": False,
                            "is_declared_global": False,
                            "is_local": True,
                            "is_annotated": False,
                            "is_free": False,
                            "is_assigned": True,
                            "is_namespace": False,
                        },
                        # MAP<string,string> — safest representation is list of (k,v) pairs
                        "attrs": [("origin", "symtable")],
                    }
                ],
                "attrs": [("scope", "top")],
            }
        ],
        "attrs": [("extractor_version", "v1")],
    }
]

table = pa.Table.from_pylist(rows, schema=symtable_files_schema)

# 2) Convert to record batches (chunk size is up to you)
batches = table.to_batches(max_chunksize=10_000)

# 3) Partitioning: DataFusion expects list[list[RecordBatch]]
partitions = [batches]  # single partition; you can split into multiple for parallelism

# 4) Register as a SQL table
ctx = SessionContext()
ctx.register_record_batches("symtable_files", partitions)
```

Now you can query it. Example “explode” across the nested lists:

```sql
SELECT
  f.file_id,
  f.path,
  blk['block_id'] AS block_id,
  sym['name']     AS sym_name,
  sym['flags']['is_imported'] AS is_imported
FROM symtable_files f
CROSS JOIN unnest(f.blocks) AS blk
CROSS JOIN unnest(blk['symbols']) AS sym
WHERE sym['flags']['is_imported'] = true;
```

---

## Option B: create a DataFrame from partitions, then register it as a view

If you already have `partitions` (same `list[list[RecordBatch]]`), you can create a DataFrame and register it:

* `SessionContext.create_dataframe(partitions, name=None, schema=None) -> DataFrame` ([datafusion.apache.org][1])
* `SessionContext.register_view(name, df)` registers that DataFrame for SQL ([datafusion.apache.org][1])

```python
df = ctx.create_dataframe(partitions, schema=symtable_files_schema)  # :contentReference[oaicite:3]{index=3}
ctx.register_view("symtable_files", df)                              # :contentReference[oaicite:4]{index=4}
```

This is useful if you want to build up a plan first, then expose it.

---

## Option C: import an Arrow table directly into a DataFrame, then register a view

`SessionContext.from_arrow()` accepts any object implementing `__arrow_c_stream__` or `__arrow_c_array__` (and `from_arrow_table(table)` is an alias). ([datafusion.apache.org][1])

```python
df = ctx.from_arrow_table(table, name="symtable_df")  # alias for from_arrow :contentReference[oaicite:6]{index=6}
ctx.register_view("symtable_files", df)               # :contentReference[oaicite:7]{index=7}
```

---

## Practical notes for your nested types

### Struct / list-of-struct values

* `struct<...>`: provide a Python `dict` whose keys match the struct field names.
* `list<struct<...>>`: provide a Python `list` of those dicts.

### Map values (`pa.map_(pa.string(), pa.string())`)

Arrow maps are safest to represent as **lists of `(key, value)` tuples**. Arrow’s Python data model explicitly calls out constructing map arrays from “lists of lists of tuples (key-item pairs)” when you pass the map type. ([Apache Arrow][2])
(Using Python dicts can work in many conversions, but dicts can’t represent duplicate keys; Arrow maps can.)

---

If you tell me whether you want **one row per file** (bundle table like you sketched) or **fully exploded 2D tables** (`symtable_blocks`, `symtable_symbols`), I can give you the exact registration + view DDL for both so you get ergonomic joins without changing the underlying nested storage.

[1]: https://datafusion.apache.org/python/autoapi/datafusion/context/index.html "datafusion.context — Apache Arrow DataFusion  documentation"
[2]: https://arrow.apache.org/docs/python/data.html?utm_source=chatgpt.com "Data Types and In-Memory Data Model - Apache Arrow"
