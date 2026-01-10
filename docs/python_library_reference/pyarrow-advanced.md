
## A) PyArrow type system beyond primitives (nested layouts, schema evolution, metadata) — deep dive (PyArrow/Arrow v22)

### Mental model: `DataType` + `Field` + `Schema` are **metadata objects**, and they’re **immutable**

Arrow’s in-memory model composes **type metadata** with **buffers**; `Schema` and `Field` additionally carry **key/value metadata**. ([Apache Arrow][1])
In practice this means “edits” are **copy-on-write**: methods return new objects; the old ones remain valid. `Schema.append()` explicitly documents this (“returns a new object, leaving the original Schema unmodified”). ([Apache Arrow][2])

---

# A1) Nested / composite types: constructors, layout semantics, and creation patterns

## A1.1 List-family: `list_` / `large_list` / `fixed_size_list` + `list_view` / `large_list_view`

### Full surface area

**Type constructors (schema-time)**

* `pa.list_(value_type, list_size=-1)` creates either variable-size ListType or FixedSizeListType depending on `list_size` (size ≥0 yields fixed-size). ([Apache Arrow][3])
* `pa.large_list(value_type)` (64-bit offsets) ([Apache Arrow][3])
* `pa.list_view(value_type)` and `pa.large_list_view(value_type)` create view types with offset+size buffers (supports out-of-order views). ([Apache Arrow][3])

**Layout semantics (critical)**

* **List**: one offsets buffer; element boundaries inferred from consecutive offsets.
* **LargeList**: same layout, but offsets are `int64` instead of `int32`. ([Apache Arrow][4])
* **ListView**: offsets **and** sizes buffers; sizes not derived from “next offset”, so offsets can be out-of-order. ([Apache Arrow][5])

**Array construction (data-time)**

* `pa.array([...])` infers `list<item: ...>` for nested python lists. ([Apache Arrow][5])
* For ListView specifically, you can either:

  * request the type in `pa.array(..., type=pa.list_view(...))`, or
  * build directly from buffers: `pa.ListViewArray.from_arrays(offsets, sizes, values)`. ([Apache Arrow][5])

### Minimal implementation snippets

**(1) Inferred List vs explicit ListView**

```python
import pyarrow as pa

lst = pa.array([[], None, [1, 2], [None, 1]])                 # -> list<item: int64>
lv  = pa.array([[], None, [1, 2], [None, 1]], type=pa.list_view(pa.int64()))
print(lst.type)  # list<item: int64>
print(lv.type)   # list_view<item: int64>
```

List inference + ListView type selection are documented. ([Apache Arrow][5])

**(2) “View” semantics: out-of-order slices**

```python
import pyarrow as pa

values  = [1, 2, 3, 4, 5, 6]
offsets = [4, 2, 0]
sizes   = [2, 2, 2]

arr = pa.ListViewArray.from_arrays(offsets, sizes, values)
# -> [[5,6],[3,4],[1,2]]
```

ListView’s additional sizes buffer enables this pattern. ([Apache Arrow][5])

---

## A1.2 Struct: nested records (`struct<...>`) + field-level semantics

### Full surface area

**Type constructor**

* `pa.struct(fields)` where fields can be `Field` objects, `(name, DataType)` tuples, or mappings. ([Apache Arrow][6])

**Array construction**

* `pa.array([{"x": 1, "y": True}, {"z": 3.4, "x": 4}])` infers a `StructArray` and unions keys across rows. ([Apache Arrow][5])
* For tuples, you must pass the explicit struct type. ([Apache Arrow][5])
* Missing dict keys become nulls at the field level (important for schema evolution). ([Apache Arrow][5])

**Field flattening**

* `Field.flatten()` turns a `struct` field into a list of child fields with parent-name prefixes (`ff.bar`, `ff.foo`, …). ([Apache Arrow][7])

### Minimal implementation snippets

**(1) Inferred from dicts**

```python
import pyarrow as pa

arr = pa.array([{"x": 1, "y": True}, {"z": 3.4, "x": 4}])
# StructArray; missing keys -> null children
```

This inference behavior is documented. ([Apache Arrow][5])

**(2) Explicit struct type for tuples**

```python
import pyarrow as pa

ty = pa.struct([("x", pa.int8()), ("y", pa.bool_())])
arr = pa.array([(3, True), (4, False)], type=ty)
```

Tuple initialization requiring explicit type is documented. ([Apache Arrow][5])

---

## A1.3 Map: `map_<key,item>` (repeated key/value pairs) + flattened key/item buffers

### Full surface area

**Type constructor**

* `pa.map_(key_type, item_type, keys_sorted=False)`; `keys_sorted` is a real property on `MapType` and can be inspected. ([Apache Arrow][8])

**Array construction**

* From python data: lists-of-(key,value) tuples **requires an explicit type**. ([Apache Arrow][5])
* From buffers: `pa.MapArray.from_arrays(offsets, keys, items)`; `MapArray.keys` / `MapArray.items` are *flattened*. To re-associate per-row segments, use `ListArray.from_arrays(arr.offsets, arr.keys/items)`. ([Apache Arrow][5])

### Minimal implementation snippets

**(1) Python data → MapArray (explicit type required)**

```python
import pyarrow as pa

data = [[("x", 1), ("y", 0)], [("a", 2), ("b", 45)]]
ty = pa.map_(pa.string(), pa.int64())
arr = pa.array(data, type=ty)
```

This “must pass type” rule is documented. ([Apache Arrow][5])

**(2) Buffer-level construction + row association**

```python
import pyarrow as pa

m = pa.MapArray.from_arrays([0, 2, 3], ["x", "y", "z"], [4, 5, 6])
keys_by_row  = pa.ListArray.from_arrays(m.offsets, m.keys)
items_by_row = pa.ListArray.from_arrays(m.offsets, m.items)
```

Flattened key/item behavior and offsets-based regrouping are documented. ([Apache Arrow][5])

---

## A1.4 Union: “one-of-N types” with **sparse** vs **dense** storage

### Full surface area

* Union types represent values where each element is **exactly one** of multiple child types. PyArrow supports **sparse** and **dense** union arrays. ([Apache Arrow][5])
* Sparse union: child arrays have the same length as the union; an `int8` type id selects which child to read. ([Apache Arrow][5])
* Dense union: additionally has an `int32` offsets array (per-element index into the selected child). ([Apache Arrow][5])
* Constructors exist at both type-level (`pa.union`, `pa.sparse_union`, `pa.dense_union`) and array-level (`pa.UnionArray.from_sparse`, `.from_dense`). ([Apache Arrow][3])

### Minimal implementation snippet

```python
import pyarrow as pa

# Sparse
xs = pa.array([5, 6, 7])
ys = pa.array([False, False, True])
types = pa.array([0, 1, 1], type=pa.int8())
u_sparse = pa.UnionArray.from_sparse(types, [xs, ys])

# Dense
xs = pa.array([5, 6, 7])
ys = pa.array([False, True])
types   = pa.array([0, 1, 1, 0, 0], type=pa.int8())
offsets = pa.array([0, 0, 1, 1, 2], type=pa.int32())
u_dense = pa.UnionArray.from_dense(types, offsets, [xs, ys])
```

Dense vs sparse construction semantics are shown directly in the Arrow Python docs. ([Apache Arrow][5])

---

## A1.5 Dictionary: categorical encoding (`dictionary<values=..., indices=...>`)

### Full surface area

* A DictionaryArray is “categorical”: it stores **integer indices** referencing a **dictionary of distinct values**; commonly used for strings to save memory and improve performance. ([Apache Arrow][5])
* Construct via `pa.DictionaryArray.from_arrays(indices, dictionary)`; the resulting type is a `dictionary<values=..., indices=..., ordered=...>`. ([Apache Arrow][5])

### Minimal implementation snippet

```python
import pyarrow as pa

indices = pa.array([0, 1, 0, 1, 2, 0, None, 2])
dictionary = pa.array(["foo", "bar", "baz"])
arr = pa.DictionaryArray.from_arrays(indices, dictionary)
print(arr.type)  # dictionary<values=string, indices=int64, ordered=0>
```

DictionaryArray semantics + example are documented. ([Apache Arrow][5])

---

# A2) Schema evolution & compatibility: how you keep heterogeneous data mergeable

## A2.1 `pyarrow.unify_schemas`: deterministic union/merge of schemas by field name

### Full surface area

* `pa.unify_schemas(schemas, promote_options="default"|"permissive")` merges fields **by name**; output is the union of fields across inputs. ([Apache Arrow][9])
* Failure modes:

  * duplicate field names **within** an input schema
  * fields with same name but incompatible types (depending on `promote_options`) ([Apache Arrow][9])
* Promotion:

  * `"default"`: only null can unify with another type
  * `"permissive"`: “types are promoted to the greater common denominator” ([Apache Arrow][9])
* Metadata + ordering rules:

  * unified field inherits field-level metadata from the schema where the field is **first defined**
  * result schema inherits its metadata from the **first input schema**
  * the first N fields preserve ordering from the first schema ([Apache Arrow][9])

### Minimal implementation pattern: unify → cast → concatenate

```python
import pyarrow as pa

unified = pa.unify_schemas([t1.schema, t2.schema], promote_options="permissive")

# Align both tables to the unified schema (adds missing columns / normalizes nullability/metadata)
t1_aligned = t1.cast(unified)
t2_aligned = t2.cast(unified)

t_all = pa.concat_tables([t1_aligned, t2_aligned])
```

`unify_schemas` behavior + options are documented; metadata preservation rules matter when you’re relying on metadata as a contract layer. ([Apache Arrow][9])

## A2.2 Low-level schema/field surgery (when you need precise control)

**Schema editing primitives** (all return new schemas):

* `schema.field(i|name)` (field lookup) ([Apache Arrow][2])
* `schema.get_field_index(name)` / `get_all_field_indices(name)` (handle duplicate names) ([Apache Arrow][2])
* `schema.insert(i, field)`, `schema.set(i, field)`, `schema.remove(i)`, `schema.append(field)` ([Apache Arrow][2])
* Equality checks with/without metadata: `schema.equals(other, check_metadata=...)` ([Apache Arrow][2])
* Debug printing controls: `schema.to_string(show_field_metadata=..., show_schema_metadata=..., truncate_metadata=...)` ([Apache Arrow][2])

**Field editing primitives**:

* `field.with_name(...)`, `with_type(...)`, `with_nullable(...)`, `with_metadata(...)`, `remove_metadata()` ([Apache Arrow][7])
* `field.equals(other, check_metadata=...)` and `field.flatten()` for struct fields ([Apache Arrow][7])

---

# A3) Metadata: schema-level and field-level control plane (and how to mutate it correctly)

### Full surface area

* Arrow supports **schema-level** and **field-level** key/value metadata; schema metadata is on `Schema.metadata`, field metadata on `Field.metadata`. ([Apache Arrow][5])
* Metadata is preserved through IPC (“Streaming, Serialization, and IPC”). ([Apache Arrow][5])
* In Python, keys/values appear as `bytes` because the underlying C++ representation is `std::string`. ([Apache Arrow][5])

### Mutation rules (the gotcha)

* `Table.replace_schema_metadata(...)` replaces the **schema-level** key/value map and returns a shallow copy. ([Apache Arrow][10])
* `Field.with_metadata(...)` returns a new Field (does not mutate existing). ([Apache Arrow][7])
* If you need to update **field metadata inside a table’s schema**, you typically:

  1. build a new `Schema` with the updated `Field`s, then
  2. `table.cast(new_schema)` to attach it. ([Apache Arrow][5])

### Minimal implementation snippets

**(1) Replace schema metadata on a table**

```python
import pyarrow as pa

t2 = table.replace_schema_metadata({"source": "ingest:v3"})
print(t2.schema.metadata)  # {b"source": b"ingest:v3"}
```

Documented example + immutability notes. ([Apache Arrow][5])

**(2) Update a field’s metadata in a table (requires new schema + cast)**

```python
import pyarrow as pa

new_schema = pa.schema(
    [
        pa.field("f0", pa.int64(), metadata={"name": "First dose"}),
        pa.field("f1", pa.string(), metadata={"name": "Second dose"}),
        pa.field("f2", pa.bool_()),
    ],
    metadata={"f2": "booster"},
)

t3 = table.cast(new_schema)
```

This pattern is spelled out in the Arrow docs. ([Apache Arrow][5])

**(3) Schema-level metadata helpers**

```python
s2 = schema.with_metadata({"contract": "events_v2"})   # attaches schema metadata
s3 = s2.remove_metadata()                              # strips schema metadata entirely
```

`Schema.with_metadata` / `remove_metadata` are documented. ([Apache Arrow][2])

[1]: https://arrow.apache.org/docs/python/data.html?utm_source=chatgpt.com "Data Types and In-Memory Data Model - Apache Arrow"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.Schema.html "pyarrow.Schema — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/api/datatypes.html "Data Types and Schemas — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/format/Intro.html?utm_source=chatgpt.com "Introduction — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/data.html "Data Types and In-Memory Data Model — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.struct.html?utm_source=chatgpt.com "pyarrow.struct — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.Field.html "pyarrow.Field — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.map_.html?utm_source=chatgpt.com "pyarrow.map_ — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.unify_schemas.html "pyarrow.unify_schemas — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html?utm_source=chatgpt.com "pyarrow.Table — Apache Arrow v22.0.0"


## A) PyArrow type system beyond primitives (nested layouts, schema evolution, metadata) — deep dive (PyArrow/Arrow v22)

### Mental model: `DataType` + `Field` + `Schema` are **metadata objects**, and they’re **immutable**

Arrow’s in-memory model composes **type metadata** with **buffers**; `Schema` and `Field` additionally carry **key/value metadata**. ([Apache Arrow][1])
In practice this means “edits” are **copy-on-write**: methods return new objects; the old ones remain valid. `Schema.append()` explicitly documents this (“returns a new object, leaving the original Schema unmodified”). ([Apache Arrow][2])

---

# A1) Nested / composite types: constructors, layout semantics, and creation patterns

## A1.1 List-family: `list_` / `large_list` / `fixed_size_list` + `list_view` / `large_list_view`

### Full surface area

**Type constructors (schema-time)**

* `pa.list_(value_type, list_size=-1)` creates either variable-size ListType or FixedSizeListType depending on `list_size` (size ≥0 yields fixed-size). ([Apache Arrow][3])
* `pa.large_list(value_type)` (64-bit offsets) ([Apache Arrow][3])
* `pa.list_view(value_type)` and `pa.large_list_view(value_type)` create view types with offset+size buffers (supports out-of-order views). ([Apache Arrow][3])

**Layout semantics (critical)**

* **List**: one offsets buffer; element boundaries inferred from consecutive offsets.
* **LargeList**: same layout, but offsets are `int64` instead of `int32`. ([Apache Arrow][4])
* **ListView**: offsets **and** sizes buffers; sizes not derived from “next offset”, so offsets can be out-of-order. ([Apache Arrow][5])

**Array construction (data-time)**

* `pa.array([...])` infers `list<item: ...>` for nested python lists. ([Apache Arrow][5])
* For ListView specifically, you can either:

  * request the type in `pa.array(..., type=pa.list_view(...))`, or
  * build directly from buffers: `pa.ListViewArray.from_arrays(offsets, sizes, values)`. ([Apache Arrow][5])

### Minimal implementation snippets

**(1) Inferred List vs explicit ListView**

```python
import pyarrow as pa

lst = pa.array([[], None, [1, 2], [None, 1]])                 # -> list<item: int64>
lv  = pa.array([[], None, [1, 2], [None, 1]], type=pa.list_view(pa.int64()))
print(lst.type)  # list<item: int64>
print(lv.type)   # list_view<item: int64>
```

List inference + ListView type selection are documented. ([Apache Arrow][5])

**(2) “View” semantics: out-of-order slices**

```python
import pyarrow as pa

values  = [1, 2, 3, 4, 5, 6]
offsets = [4, 2, 0]
sizes   = [2, 2, 2]

arr = pa.ListViewArray.from_arrays(offsets, sizes, values)
# -> [[5,6],[3,4],[1,2]]
```

ListView’s additional sizes buffer enables this pattern. ([Apache Arrow][5])

---

## A1.2 Struct: nested records (`struct<...>`) + field-level semantics

### Full surface area

**Type constructor**

* `pa.struct(fields)` where fields can be `Field` objects, `(name, DataType)` tuples, or mappings. ([Apache Arrow][6])

**Array construction**

* `pa.array([{"x": 1, "y": True}, {"z": 3.4, "x": 4}])` infers a `StructArray` and unions keys across rows. ([Apache Arrow][5])
* For tuples, you must pass the explicit struct type. ([Apache Arrow][5])
* Missing dict keys become nulls at the field level (important for schema evolution). ([Apache Arrow][5])

**Field flattening**

* `Field.flatten()` turns a `struct` field into a list of child fields with parent-name prefixes (`ff.bar`, `ff.foo`, …). ([Apache Arrow][7])

### Minimal implementation snippets

**(1) Inferred from dicts**

```python
import pyarrow as pa

arr = pa.array([{"x": 1, "y": True}, {"z": 3.4, "x": 4}])
# StructArray; missing keys -> null children
```

This inference behavior is documented. ([Apache Arrow][5])

**(2) Explicit struct type for tuples**

```python
import pyarrow as pa

ty = pa.struct([("x", pa.int8()), ("y", pa.bool_())])
arr = pa.array([(3, True), (4, False)], type=ty)
```

Tuple initialization requiring explicit type is documented. ([Apache Arrow][5])

---

## A1.3 Map: `map_<key,item>` (repeated key/value pairs) + flattened key/item buffers

### Full surface area

**Type constructor**

* `pa.map_(key_type, item_type, keys_sorted=False)`; `keys_sorted` is a real property on `MapType` and can be inspected. ([Apache Arrow][8])

**Array construction**

* From python data: lists-of-(key,value) tuples **requires an explicit type**. ([Apache Arrow][5])
* From buffers: `pa.MapArray.from_arrays(offsets, keys, items)`; `MapArray.keys` / `MapArray.items` are *flattened*. To re-associate per-row segments, use `ListArray.from_arrays(arr.offsets, arr.keys/items)`. ([Apache Arrow][5])

### Minimal implementation snippets

**(1) Python data → MapArray (explicit type required)**

```python
import pyarrow as pa

data = [[("x", 1), ("y", 0)], [("a", 2), ("b", 45)]]
ty = pa.map_(pa.string(), pa.int64())
arr = pa.array(data, type=ty)
```

This “must pass type” rule is documented. ([Apache Arrow][5])

**(2) Buffer-level construction + row association**

```python
import pyarrow as pa

m = pa.MapArray.from_arrays([0, 2, 3], ["x", "y", "z"], [4, 5, 6])
keys_by_row  = pa.ListArray.from_arrays(m.offsets, m.keys)
items_by_row = pa.ListArray.from_arrays(m.offsets, m.items)
```

Flattened key/item behavior and offsets-based regrouping are documented. ([Apache Arrow][5])

---

## A1.4 Union: “one-of-N types” with **sparse** vs **dense** storage

### Full surface area

* Union types represent values where each element is **exactly one** of multiple child types. PyArrow supports **sparse** and **dense** union arrays. ([Apache Arrow][5])
* Sparse union: child arrays have the same length as the union; an `int8` type id selects which child to read. ([Apache Arrow][5])
* Dense union: additionally has an `int32` offsets array (per-element index into the selected child). ([Apache Arrow][5])
* Constructors exist at both type-level (`pa.union`, `pa.sparse_union`, `pa.dense_union`) and array-level (`pa.UnionArray.from_sparse`, `.from_dense`). ([Apache Arrow][3])

### Minimal implementation snippet

```python
import pyarrow as pa

# Sparse
xs = pa.array([5, 6, 7])
ys = pa.array([False, False, True])
types = pa.array([0, 1, 1], type=pa.int8())
u_sparse = pa.UnionArray.from_sparse(types, [xs, ys])

# Dense
xs = pa.array([5, 6, 7])
ys = pa.array([False, True])
types   = pa.array([0, 1, 1, 0, 0], type=pa.int8())
offsets = pa.array([0, 0, 1, 1, 2], type=pa.int32())
u_dense = pa.UnionArray.from_dense(types, offsets, [xs, ys])
```

Dense vs sparse construction semantics are shown directly in the Arrow Python docs. ([Apache Arrow][5])

---

## A1.5 Dictionary: categorical encoding (`dictionary<values=..., indices=...>`)

### Full surface area

* A DictionaryArray is “categorical”: it stores **integer indices** referencing a **dictionary of distinct values**; commonly used for strings to save memory and improve performance. ([Apache Arrow][5])
* Construct via `pa.DictionaryArray.from_arrays(indices, dictionary)`; the resulting type is a `dictionary<values=..., indices=..., ordered=...>`. ([Apache Arrow][5])

### Minimal implementation snippet

```python
import pyarrow as pa

indices = pa.array([0, 1, 0, 1, 2, 0, None, 2])
dictionary = pa.array(["foo", "bar", "baz"])
arr = pa.DictionaryArray.from_arrays(indices, dictionary)
print(arr.type)  # dictionary<values=string, indices=int64, ordered=0>
```

DictionaryArray semantics + example are documented. ([Apache Arrow][5])

---

# A2) Schema evolution & compatibility: how you keep heterogeneous data mergeable

## A2.1 `pyarrow.unify_schemas`: deterministic union/merge of schemas by field name

### Full surface area

* `pa.unify_schemas(schemas, promote_options="default"|"permissive")` merges fields **by name**; output is the union of fields across inputs. ([Apache Arrow][9])
* Failure modes:

  * duplicate field names **within** an input schema
  * fields with same name but incompatible types (depending on `promote_options`) ([Apache Arrow][9])
* Promotion:

  * `"default"`: only null can unify with another type
  * `"permissive"`: “types are promoted to the greater common denominator” ([Apache Arrow][9])
* Metadata + ordering rules:

  * unified field inherits field-level metadata from the schema where the field is **first defined**
  * result schema inherits its metadata from the **first input schema**
  * the first N fields preserve ordering from the first schema ([Apache Arrow][9])

### Minimal implementation pattern: unify → cast → concatenate

```python
import pyarrow as pa

unified = pa.unify_schemas([t1.schema, t2.schema], promote_options="permissive")

# Align both tables to the unified schema (adds missing columns / normalizes nullability/metadata)
t1_aligned = t1.cast(unified)
t2_aligned = t2.cast(unified)

t_all = pa.concat_tables([t1_aligned, t2_aligned])
```

`unify_schemas` behavior + options are documented; metadata preservation rules matter when you’re relying on metadata as a contract layer. ([Apache Arrow][9])

## A2.2 Low-level schema/field surgery (when you need precise control)

**Schema editing primitives** (all return new schemas):

* `schema.field(i|name)` (field lookup) ([Apache Arrow][2])
* `schema.get_field_index(name)` / `get_all_field_indices(name)` (handle duplicate names) ([Apache Arrow][2])
* `schema.insert(i, field)`, `schema.set(i, field)`, `schema.remove(i)`, `schema.append(field)` ([Apache Arrow][2])
* Equality checks with/without metadata: `schema.equals(other, check_metadata=...)` ([Apache Arrow][2])
* Debug printing controls: `schema.to_string(show_field_metadata=..., show_schema_metadata=..., truncate_metadata=...)` ([Apache Arrow][2])

**Field editing primitives**:

* `field.with_name(...)`, `with_type(...)`, `with_nullable(...)`, `with_metadata(...)`, `remove_metadata()` ([Apache Arrow][7])
* `field.equals(other, check_metadata=...)` and `field.flatten()` for struct fields ([Apache Arrow][7])

---

# A3) Metadata: schema-level and field-level control plane (and how to mutate it correctly)

### Full surface area

* Arrow supports **schema-level** and **field-level** key/value metadata; schema metadata is on `Schema.metadata`, field metadata on `Field.metadata`. ([Apache Arrow][5])
* Metadata is preserved through IPC (“Streaming, Serialization, and IPC”). ([Apache Arrow][5])
* In Python, keys/values appear as `bytes` because the underlying C++ representation is `std::string`. ([Apache Arrow][5])

### Mutation rules (the gotcha)

* `Table.replace_schema_metadata(...)` replaces the **schema-level** key/value map and returns a shallow copy. ([Apache Arrow][10])
* `Field.with_metadata(...)` returns a new Field (does not mutate existing). ([Apache Arrow][7])
* If you need to update **field metadata inside a table’s schema**, you typically:

  1. build a new `Schema` with the updated `Field`s, then
  2. `table.cast(new_schema)` to attach it. ([Apache Arrow][5])

### Minimal implementation snippets

**(1) Replace schema metadata on a table**

```python
import pyarrow as pa

t2 = table.replace_schema_metadata({"source": "ingest:v3"})
print(t2.schema.metadata)  # {b"source": b"ingest:v3"}
```

Documented example + immutability notes. ([Apache Arrow][5])

**(2) Update a field’s metadata in a table (requires new schema + cast)**

```python
import pyarrow as pa

new_schema = pa.schema(
    [
        pa.field("f0", pa.int64(), metadata={"name": "First dose"}),
        pa.field("f1", pa.string(), metadata={"name": "Second dose"}),
        pa.field("f2", pa.bool_()),
    ],
    metadata={"f2": "booster"},
)

t3 = table.cast(new_schema)
```

This pattern is spelled out in the Arrow docs. ([Apache Arrow][5])

**(3) Schema-level metadata helpers**

```python
s2 = schema.with_metadata({"contract": "events_v2"})   # attaches schema metadata
s3 = s2.remove_metadata()                              # strips schema metadata entirely
```

`Schema.with_metadata` / `remove_metadata` are documented. ([Apache Arrow][2])

[1]: https://arrow.apache.org/docs/python/data.html?utm_source=chatgpt.com "Data Types and In-Memory Data Model - Apache Arrow"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.Schema.html "pyarrow.Schema — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/api/datatypes.html "Data Types and Schemas — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/format/Intro.html?utm_source=chatgpt.com "Introduction — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/data.html "Data Types and In-Memory Data Model — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.struct.html?utm_source=chatgpt.com "pyarrow.struct — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.Field.html "pyarrow.Field — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.map_.html?utm_source=chatgpt.com "pyarrow.map_ — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.unify_schemas.html "pyarrow.unify_schemas — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html?utm_source=chatgpt.com "pyarrow.Table — Apache Arrow v22.0.0"


## C) Zero-copy interoperability: Arrow C Data Interface + PyCapsule protocol — deep dive (Arrow/PyArrow v22)

### Mental model: ABI-stable structs + “consume-once” capsules

The Arrow **C Data Interface** defines a tiny ABI-stable set of C structs (not a full C API) meant for **in-process, zero-copy** exchange of Arrow-formatted memory without taking a dependency on Arrow C++/Java/etc. ([Apache Arrow][1])
The **PyCapsule Interface** standardizes how Python libraries expose those structs to each other safely (named capsule + destructor), replacing older patterns that required PyArrow-specific `_import_from_c/_export_to_c` usage and could leak if mishandled. ([Apache Arrow][2])

---

# C1) The protocol surface area: `__arrow_c_*__` exports (CPU + device)

## C1.1 Required protocol methods (CPU)

A producer object can implement:

* **Schema/type export**

  * `__arrow_c_schema__(self) -> PyCapsule("arrow_schema")` ([Apache Arrow][2])

* **Array / RecordBatch export (contiguous)**

  * `__arrow_c_array__(self, requested_schema=None) -> (schema_capsule, array_capsule)`
  * returns a pair: `PyCapsule("arrow_schema")`, `PyCapsule("arrow_array")` ([Apache Arrow][2])

* **Stream / Table / Chunked export**

  * `__arrow_c_stream__(self, requested_schema=None) -> PyCapsule("arrow_array_stream")` ([Apache Arrow][2])

**Capsule names are not cosmetic**; they’re part of the safety contract and must match exactly (`arrow_schema`, `arrow_array`, `arrow_array_stream`, …). ([Apache Arrow][2])

## C1.2 Device-aware variants (GPU / non-CPU memory)

If you support the Arrow **C Device Data Interface**, you can additionally implement:

* `__arrow_c_device_array__(..., **kwargs) -> (PyCapsule("arrow_schema"), PyCapsule("arrow_device_array"))`
* `__arrow_c_device_stream__(..., **kwargs) -> PyCapsule("arrow_device_array_stream")` ([Apache Arrow][2])

Device method rules that matter:

* accept `**kwargs` only if defaults are `None` (forward compatibility); unknown kwargs should raise `NotImplementedError`. ([Apache Arrow][2])
* exporting device arrays/streams is expected to avoid cross-device copies; CPU-only export methods should only return valid CPU pointers (raise or copy-to-CPU if the object currently lives on device). ([Apache Arrow][2])

---

# C2) Schema requests: negotiation, not arbitrary transforms

`__arrow_c_array__` / `__arrow_c_stream__` accept `requested_schema`, a PyCapsule containing an `ArrowSchema`. The intent is to let the consumer request a specific **representation** when multiple Arrow encodings are possible (e.g., `utf8` vs `large_utf8` vs `string_view`, dictionary encoding choices, integer width choices). ([Apache Arrow][2])

Rules:

* producer should *try* to satisfy the request; if it can’t, it may return its default representation.
* if the request is **incompatible with the data** (e.g., different field count), the producer should raise. This mechanism is not meant for arbitrary schema reshaping. ([Apache Arrow][2])

---

# C3) Lifetime + “consume once” semantics (the part that prevents leaks/crashes)

## C3.1 Capsule destructors must call `release` (if not null)

Exported capsules should attach a destructor that calls the struct’s `release` callback (if not already NULL). This prevents leaks if the capsule is never consumed. ([Apache Arrow][2])

## C3.2 Consumers “move” ownership and null out `release`

If a consumer imports the struct, it should *move* the data and set `release = NULL` on the imported struct so the capsule destructor won’t free memory the consumer now owns/uses. ([Apache Arrow][2])

## C3.3 Capsules are single-consumption

The PyCapsule interface mirrors the C Data Interface rule: a capsule can only be consumed once. ([Apache Arrow][2])

---

# C4) Consumer-side usage in PyArrow: “just pass the object”

PyArrow constructors recognize these protocols and import zero-copy when possible. ([Apache Arrow][3])

### What accepts what (practically)

* `pa.array(obj)` accepts objects with `__arrow_c_array__` or `__arrow_c_device_array__`. ([Apache Arrow][4])
* `pa.chunked_array([...])` accepts chunk sources that implement `__arrow_c_array__` or `__arrow_c_stream__`. ([Apache Arrow][5])
* `pa.record_batch(obj)` accepts tabular objects with `__arrow_c_array__` / `__arrow_c_device_array__`. ([Apache Arrow][6])
* `pa.table(obj)` accepts tabular objects with `__arrow_c_array__`, `__arrow_c_stream__`, or device variants. ([Apache Arrow][7])
* `pa.RecordBatchReader.from_stream(obj, schema=...)` accepts objects with `__arrow_c_stream__` and optionally requests a cast schema. ([Apache Arrow][8])

### Minimal “import a stream” recipe

```python
import pyarrow as pa

# obj implements __arrow_c_stream__()
reader = pa.RecordBatchReader.from_stream(obj)

# consume incrementally
for batch in reader:
    ...

# or materialize
table = reader.read_all()
```

`RecordBatchReader.from_stream` is the canonical “PyCapsule stream” ingestion API. ([Apache Arrow][8])

### Minimal “request a representation” recipe (schema negotiation)

```python
import pyarrow as pa

target_schema = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("txt", pa.large_string()),   # request large offsets representation
])

reader = pa.RecordBatchReader.from_stream(obj, schema=target_schema)
```

PyArrow documents the schema parameter as a cast target “if supported by the stream object,” which maps directly to the `requested_schema` negotiation mechanism. ([Apache Arrow][8])

---

# C5) Producer-side implementation: what you actually have to build

There are two layers:

1. **C Data Interface layer**: populate `ArrowSchema` / `ArrowArray` / `ArrowArrayStream` (and optionally device equivalents).
2. **PyCapsule layer**: wrap pointers in named capsules with destructors.

## C5.1 The structs you must populate (C Data Interface)

The minimal ABI structs are literally (fields shown in spec): ([Apache Arrow][1])

* `struct ArrowSchema { format, name, metadata, flags, n_children, children, dictionary, release, private_data }`
* `struct ArrowArray { length, null_count, offset, n_buffers, n_children, buffers, children, dictionary, release, private_data }` ([Apache Arrow][1])

Where:

* `format` is the top-level type description string (nested children described separately). ([Apache Arrow][1])
* `metadata` is a binary encoding of key/value pairs (not null-terminated). ([Apache Arrow][1])
* `buffers` is an array of pointers to contiguous physical buffers; null buffer pointers are only allowed in specific cases (e.g., null bitmap when `null_count==0`). ([Apache Arrow][1])
* `release` is mandatory; it must free producer-owned resources and then set `release = NULL`. ([Apache Arrow][1])

## C5.2 Capsule naming + destructor: the exact rules

PyCapsule names are standardized (must match): ([Apache Arrow][2])

* `ArrowSchema` → `"arrow_schema"`
* `ArrowArray` → `"arrow_array"`
* `ArrowArrayStream` → `"arrow_array_stream"`
* `ArrowDeviceArray` → `"arrow_device_array"`
* `ArrowDeviceArrayStream` → `"arrow_device_array_stream"`

### Minimal “create capsule” pattern (C / Cython)

You create a capsule with `PyCapsule_New(pointer, name, destructor)`. The destructor should:

1. `PyCapsule_GetPointer(..., name)`
2. if `struct->release != NULL`: call it
3. free the struct itself. ([Apache Arrow][2])

The spec also recommends wrapping the malloc’ed struct in a capsule **immediately** (with `release=NULL`) before populating fields, so exceptions don’t leak. ([Apache Arrow][2])

## C5.3 Implementing the Python methods (shape + return types)

At Python level you expose something like:

* `__arrow_c_schema__` → return `PyCapsule("arrow_schema")`
* `__arrow_c_array__(requested_schema=None)` → return `(schema_capsule, array_capsule)`
* `__arrow_c_stream__(requested_schema=None)` → return `stream_capsule` ([Apache Arrow][2])

…and inside these methods you typically call into your extension module to allocate/populate the structs and hand back the capsule(s).

## C5.4 Pick array vs stream correctly

* Use **`__arrow_c_array__`** when you can represent the object as one contiguous ArrowArray (arrays, record batches, “contiguous tables”). ([Apache Arrow][2])
* Use **`__arrow_c_stream__`** when the object is naturally chunked/streaming (chunked arrays, tables with chunked columns, scanners, iterators). ([Apache Arrow][2])

---

# C6) “Accept Arrow from anyone” in your own library (duck-typing + Protocols)

If your library takes user data, you can support *any* Arrow producer by checking for these methods instead of hard-coding “must be PyArrow.” This is explicitly called out as a goal of the PyCapsule interface. ([Apache Arrow][3])

The spec even provides copy-pastable `typing.Protocol` definitions for annotation (schema/array/stream and device variants). ([Apache Arrow][2])

---

# C7) Backwards compatibility with older PyArrow

When talking to PyArrow, the PyCapsule protocol is preferred over `_export_to_c/_import_from_c`, but if you need to support a range of PyArrow versions you do it by duck typing (feature-detect the protocol methods vs legacy). ([Apache Arrow][2])

[1]: https://arrow.apache.org/docs/format/CDataInterface.html "The Arrow C data interface — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html "The Arrow PyCapsule Interface — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/extending_types.html "Extending PyArrow — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.array.html "pyarrow.array — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.chunked_array.html "pyarrow.chunked_array — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.record_batch.html "pyarrow.record_batch — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.table.html "pyarrow.table — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.RecordBatchStreamReader.html "pyarrow.ipc.RecordBatchStreamReader — Apache Arrow v22.0.0"


## D) DataFrame interchange protocol (`__dataframe__`) + `pyarrow.interchange` — deep dive (Arrow/PyArrow v22)

### Mental model: a *dataframe-shaped* view over **contiguous buffers**

The DataFrame interchange protocol is a Python-level, in-process contract for **(attempted) zero-copy** interchange between DataFrame libraries. In Arrow’s Python binding, it’s implemented for `pa.Table` / `pa.RecordBatch` and is scoped (today) to **primitive + dictionary** dtypes, with missing-data and chunking support. ([Apache Arrow][1])

Critically: the protocol is **buffer-oriented** (data buffer + optional validity mask + optional offsets buffer), with explicit dtype descriptors and explicit “can I copy?” control (`allow_copy`). ([Data APIs][2])

---

# D1) Protocol surface area you actually need to know

## D1.1 The `__dataframe__` entrypoint signature + semantics

The protocol defines `__dataframe__(nan_as_null: bool = False, allow_copy: bool = True) -> DataFrame`. The spec calls out that:

* `allow_copy` governs whether a producer may copy to satisfy requirements like **contiguous buffers** (e.g., if the producer has strided storage).
* `nan_as_null` is **deprecated** and slated for removal; the spec explicitly warns to call using `.__dataframe__(allow_copy=...)`. ([Data APIs][2])

PyArrow’s `Table.__dataframe__(nan_as_null=False, allow_copy=True)` mirrors this, and documents `allow_copy=False` as “fail if non-zero-copy export is required”; it also notes `nan_as_null` currently has no effect in PyArrow’s implementation. ([Apache Arrow][3])

---

## D1.2 The exchange object model: DataFrame / Column / Buffer

The spec models the interchange object graph like this: ([Data APIs][2])

* **Buffer**: contiguous memory with a `ptr` (address) and `bufsize` (bytes). It optionally supports `__dlpack__`; it must expose device info via `__dlpack_device__`. ([Data APIs][2])
* **Column**: one dtype; can be chunked; exposes:

  * `dtype: (kind, bit_width, format_str, endianness)`
  * `describe_null` / `null_count`
  * `get_buffers(): {data, validity?, offsets?}`
  * `describe_categorical` for categorical columns (dictionary-like) ([Data APIs][2])
* **DataFrame**: ordered named columns, unique string names, row-aligned, chunkable; exposes `column_names`, `get_column_by_name`, `get_chunks`, etc. The spec requires producers to ensure **all columns are chunked the same way** when subdividing. ([Data APIs][2])

**Key limitation (important for expectations):** the spec explicitly excludes many types, including **nested (list/struct/map/union)**, binary, decimal, complex, etc. ([Data APIs][2])

---

# D2) PyArrow export path: `pa.Table.__dataframe__()` (producer role)

### Full surface area (PyArrow)

`pa.Table.__dataframe__(nan_as_null=False, allow_copy=True)` returns a DataFrame interchange object the consumer can ingest. Parameters: ([Apache Arrow][3])

* `allow_copy`: if `False`, any export requiring copying should fail.
* `nan_as_null`: present for compatibility; PyArrow notes it currently has no effect.

**Practical implication:** in a “strict zero-copy export” mode, call:

```python
xchg = table.__dataframe__(allow_copy=False)
```

…and be prepared for failures if the table can’t be represented under the protocol constraints without copying. ([Apache Arrow][3])

### Advanced but real: using the exchange object directly

Arrow’s docs stress that `table.__dataframe__()` is mainly for consumer libraries and “not meant to be used manually.” ([Apache Arrow][1])
Still, for instrumentation/debugging you can inspect `xchg.get_chunks()`, `xchg.column_names()`, and per-column buffers, which is useful for validating “will this be zero-copy?” before calling into a consuming stack. ([Data APIs][2])

---

# D3) PyArrow import path: `pyarrow.interchange.from_dataframe(...)` (consumer role)

## D3.1 API + behavior contract

`pyarrow.interchange.from_dataframe(df, allow_copy=True) -> pa.Table`:

* accepts any object that supports `__dataframe__`
* if `df` is already a `pa.Table`, it returns it; if it’s a `pa.RecordBatch`, it wraps via `pa.Table.from_batches([df])` ([Apache Arrow][4])
* it calls `df.__dataframe__(allow_copy=allow_copy)` and then converts the resulting interchange object into Arrow record batches and finally `pa.Table.from_batches(...)`. ([Apache Arrow][5])

### Chunking behavior (critical for large ingest)

PyArrow’s implementation iterates `df.get_chunks()` and converts each chunk to a `pa.RecordBatch`, then constructs the table from batches. ([Apache Arrow][5])
So if your producer yields multiple chunks, you get a natural streaming-ish ingestion path *without* materializing one huge intermediate buffer per column.

## D3.2 dtype coverage (what PyArrow handles today)

In PyArrow’s current implementation, each chunk conversion does:

* Validates `column_names()` are **strings** and **unique** (else `ValueError`). ([Apache Arrow][5])
* Dispatches by `DtypeKind`:

  * `INT/UINT/FLOAT/STRING/DATETIME` → `column_to_array(...)`
  * `BOOL` → `bool_column_to_array(...)`
  * `CATEGORICAL` → `categorical_column_to_dictionary(...)`
  * otherwise → `NotImplementedError` ([Apache Arrow][5])

This is why nested types “work” poorly via this route: the protocol (and PyArrow’s converter) intentionally doesn’t cover them. ([Data APIs][2])

---

# D4) Zero-copy invariants, memory ownership, and the sharp edges

## D4.1 “Zero-copy requested” is not “zero-copy guaranteed”

Both the spec and PyArrow treat `allow_copy=False` as a *constraint*: if a copy is required, you should fail. ([Data APIs][2])

PyArrow’s converter has explicit hard-fail cases when `allow_copy=False`, including:

* **Byte-packed booleans**: if the producer exposes booleans as 8-bit values, PyArrow needs to cast to Arrow’s bit-packed boolean representation; with `allow_copy=False` it raises. ([Apache Arrow][5])
* **Categoricals**: PyArrow’s categorical conversion path currently raises under `allow_copy=False` (it documents a copy requirement in that path). ([Apache Arrow][5])

## D4.2 Lifetime: imported arrays may not own memory

PyArrow’s internal `buffers_to_array(...)` explicitly constructs Arrow buffers via `pa.foreign_buffer(ptr, bufsize, base=...)` and warns:

> “The returned array doesn’t own the memory. The caller … is responsible for keeping the memory owner object alive…” ([Apache Arrow][5])

**Implication for implementers:** if your `Buffer` object is the `base=...`, that object must hold (directly or indirectly) a reference to the real owning allocation (e.g., a NumPy array) so Python GC doesn’t free it while Arrow arrays still reference its pointer.

## D4.3 Strings require an offsets buffer (and offset width influences Arrow string type)

The protocol’s `get_buffers()` includes an optional `offsets` buffer for variable-size binary data (like variable-length strings). ([Data APIs][2])
PyArrow uses the offset dtype bit-width to choose between `string()` and `large_string()` in `buffers_to_array`. ([Apache Arrow][5])

---

# D5) Minimal implementation recipes (Python expert level)

## D5.1 Ingest any interchange-capable DF into Arrow

```python
import pyarrow as pa
from pyarrow.interchange import from_dataframe

tbl = from_dataframe(df_like, allow_copy=False)  # strict: fail if copy required
```

This is the canonical consumer entrypoint and uses `df_like.__dataframe__(allow_copy=...)` internally. ([Apache Arrow][4])

If you want “best-effort” conversions (permitting copies where required):

```python
tbl = from_dataframe(df_like, allow_copy=True)
```

([Apache Arrow][4])

---

## D5.2 Export Arrow to a consumer library via the protocol (strict/no-copy)

```python
xchg = table.__dataframe__(allow_copy=False)
# Pass `xchg` (or the table itself) to a consumer that speaks the protocol.
```

PyArrow documents this method and its `allow_copy` behavior; note `nan_as_null` is currently ineffectual in PyArrow and deprecated in the spec. ([Apache Arrow][3])

---

## D5.3 Implement `__dataframe__` for your own DF so PyArrow can ingest it

If your goal is: **“I have custom storage; I want `pyarrow.interchange.from_dataframe(my_df)` to be zero-copy”**, you only need the subset PyArrow calls:

* DataFrame: `column_names()`, `get_column_by_name()`, `get_chunks()`
* Column: `dtype`, `size()`, `offset`, `describe_null`, `get_buffers()`, `describe_categorical` (if categorical)
* Buffer: `ptr`, `bufsize` ([Apache Arrow][5])

Below is a *minimal* “contiguous NumPy-backed” skeleton (CPU-only), aligned with the spec concepts:

```python
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional, Sequence, Tuple

import numpy as np

# Spec dtype tuple: (kind, bit_width, format_str, endianness)
# You should align kind/format strings with the spec enums; keep this minimal for your use-case.
Dtype = Tuple[int, int, str, str]

@dataclass
class NPBuffer:
    arr: np.ndarray  # owns the memory

    @property
    def ptr(self) -> int:
        return int(self.arr.__array_interface__["data"][0])

    @property
    def bufsize(self) -> int:
        return int(self.arr.nbytes)

    def __dlpack_device__(self):
        # CPU per protocol enum mapping; returning (device_type, device_id)
        return (1, None)

@dataclass
class NPColumn:
    name: str
    data: np.ndarray                  # contiguous 1D
    validity: Optional[np.ndarray]    # optional bit/byte mask as uint8
    dtype: Dtype
    _offset: int = 0

    def size(self) -> int:
        return int(self.data.shape[0])

    @property
    def offset(self) -> int:
        return self._offset

    @property
    def describe_null(self):
        # In full spec this is (ColumnNullType, value); keep it simple:
        # - bitmask/byte mask is recommended when you want strict null handling
        # Provide correct enum values for your chosen null mode.
        return (3, 0)  # USE_BITMASK, missing indicated by 0/1 per spec

    @property
    def describe_categorical(self):
        raise TypeError("not categorical")

    def get_buffers(self) -> Dict[str, Any]:
        out = {
            "data": (NPBuffer(self.data), self.dtype),
            "validity": None,
            "offsets": None,
        }
        if self.validity is not None:
            # validity dtype is boolean with bit width 1 (bitmask) or 8 (bytemask)
            out["validity"] = (NPBuffer(self.validity), (20, 1, "b", "="))
        return out

@dataclass
class NPDataFrameXchg:
    cols: Dict[str, NPColumn]
    _metadata: Dict[str, Any]

    # Interchange object itself is also required to implement __dataframe__ per spec
    def __dataframe__(self, nan_as_null: bool = False, allow_copy: bool = True):
        # nan_as_null is deprecated in the protocol; allow_copy is the real knob.
        return self

    @property
    def metadata(self) -> Dict[str, Any]:
        return self._metadata

    def column_names(self) -> Iterable[str]:
        return self.cols.keys()

    def get_column_by_name(self, name: str) -> NPColumn:
        return self.cols[name]

    def get_chunks(self, n_chunks: Optional[int] = None) -> Iterable["NPDataFrameXchg"]:
        # simplest: single chunk
        yield self
```

What this *buys* you in PyArrow:

* PyArrow will wrap your buffers using `pa.foreign_buffer(ptr, bufsize, base=buffer_obj)` (so your `NPBuffer` must keep the NumPy array alive). ([Apache Arrow][5])
* If you expose booleans as uint8 (“byte-packed”), PyArrow may need a cast and can fail under `allow_copy=False`. Prefer bit-packed booleans for strict zero-copy. ([Apache Arrow][5])
* If you expose categoricals, expect `allow_copy=False` to fail in PyArrow’s current converter. ([Apache Arrow][5])

---

## D6) When to *not* use this protocol (and what to use instead)

If you need:

* nested types (list/struct/map/union),
* binary/decimal,
* richer Arrow-native semantics,

…the interchange protocol is the wrong interchange plane (by design). Use Arrow-native interchange (`__arrow_c_array__` / `__arrow_c_stream__`) instead. ([Data APIs][2])

[1]: https://arrow.apache.org/docs/python/interchange_protocol.html "Dataframe Interchange Protocol — Apache Arrow v22.0.0"
[2]: https://data-apis.org/dataframe-protocol/latest/API.html "
   API of the __dataframe__ protocol — Python dataframe interchange protocol 2021-DRAFT documentation
  "
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html "pyarrow.Table — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.interchange.from_dataframe.html "pyarrow.interchange.from_dataframe — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/_modules/pyarrow/interchange/from_dataframe.html "pyarrow.interchange.from_dataframe — Apache Arrow v22.0.0"


## E) PyArrow memory + IO primitives (Buffers, mmap, compression streams) — deep dive (PyArrow/Arrow v22)

### Mental model: Arrow IO is “buffers + thin stream wrappers”

Arrow’s Python IO layer is built around:

* **`Buffer`** = contiguous memory region (may or may not own memory). ([Apache Arrow][1])
* **`NativeFile`** subclasses = C++-backed stream/file façade over buffers, OS file descriptors, memory maps, Python file objects, and compression wrappers. ([Apache Arrow][2])

This layer matters because it’s where you actually get **zero-copy** reads (buffer slices, mmap-backed reads, buffer-backed readers) and where you decide whether “IO” means **bytes**, **buffers**, or **streamed record batches**.

---

# E1) Buffers: referencing, allocating, mutability/resizability, lifetime

## E1.1 `pyarrow.Buffer` (core physical memory object)

**What it is**

* Base class for Arrow buffers; represents contiguous memory; some buffers **own** their memory and some are views into memory owned elsewhere. ([Apache Arrow][1])

**Key surface**

* `buffer.slice(offset=0, length=None)` is **zero-copy** (“Memory is not copied”; slicing returns a logical view). ([Apache Arrow][1])
* `buffer.to_pybytes()` copies into Python `bytes` (explicitly “Memory is copied”). ([Apache Arrow][1])
* Introspection: `.address`, `.size`, `.is_mutable`, `.is_cpu`, `.device` / `.device_type`. ([Apache Arrow][1])

### Minimal usage

```python
import pyarrow as pa

b = pa.py_buffer(b"hello world")
b2 = b.slice(6, 5)        # zero-copy view
assert b2.to_pybytes() == b"world"  # copy-out
```

([Apache Arrow][1])

---

## E1.2 Allocate Arrow-owned memory: `allocate_buffer(...)`

**API**

* `pa.allocate_buffer(size, memory_pool=None, resizable=False)` allocates a **mutable** buffer (optionally resizable) from a memory pool; returns `Buffer` or `ResizableBuffer`. ([Apache Arrow][3])

```python
import pyarrow as pa
buf = pa.allocate_buffer(1024, resizable=False)
```

([Apache Arrow][3])

---

## E1.3 Wrap Python-owned memory: `py_buffer(obj)`

**API**

* `pa.py_buffer(obj)` constructs a `Buffer` from a bytes-like / buffer-like Python object. ([Apache Arrow][4])

This is the “cheap view” path for `bytes`, `bytearray`, `memoryview`, NumPy arrays, etc.

---

## E1.4 Wrap foreign memory: `foreign_buffer(address, size, base=...)` (lifetime pinning)

**API**

* `pa.foreign_buffer(address, size, base=None)` constructs a `Buffer` pointing at an address.
* If `base` is provided, Arrow keeps it alive **as long as the Buffer is alive**, including across language boundaries. ([Apache Arrow][5])

This is the primitive you use when you have memory allocated by another runtime (C extension, mmap, CUDA, shared memory) and you need Arrow to reference it safely.

---

## E1.5 `ResizableBuffer` (in-place growth/shrink + device-aware properties)

**Surface**

* `.resize(new_size, shrink_to_fit=False)` resizes (with optional shrink) ([Apache Arrow][6])
* `.slice(...)` remains zero-copy ([Apache Arrow][6])
* `.to_pybytes()` copies out ([Apache Arrow][6])
* Introspection includes `.address` + device/CPU access flags. ([Apache Arrow][6])

---

# E2) Memory pools (allocation control + debugging)

## E2.1 `MemoryPool` interface (what it tracks + why it exists)

**What it does**

* Tracks allocated bytes and ensures **64-byte alignment** required by Arrow data. ([Apache Arrow][7])

**Key methods**

* `bytes_allocated()`, `total_bytes_allocated()`, `max_memory()`, `num_allocations()`, `print_stats()` ([Apache Arrow][7])
* `release_unused()` attempts to return memory to the OS; Arrow warns it can be expensive and is mainly for benchmarking/debugging. ([Apache Arrow][7])

## E2.2 Global pool controls

* `pa.default_memory_pool()` returns the process-global pool. ([Apache Arrow][8])
* `pa.set_memory_pool(pool)` sets the default pool used by allocations when a pool isn’t explicitly passed. ([Apache Arrow][9])
* `pa.log_memory_allocations(enable=True)` toggles allocator logging. ([Apache Arrow][10])
* `pa.total_allocated_bytes()` returns bytes currently allocated from the **default** pool (may not account for other pools). ([Apache Arrow][11])

---

# E3) NativeFile ecosystem (what streams exist + when to use each)

Arrow’s “stream/file façade” types are explicitly enumerated in the Memory/IO docs: `OSFile`, `MemoryMappedFile`, `BufferReader`, `BufferOutputStream`, `FixedSizeBufferWriter`, `PythonFile`, `CompressedInputStream`, `CompressedOutputStream`. ([Apache Arrow][2])

Below is the advanced subset that actually changes system design.

---

## E3.1 `OSFile` (FD-backed, seekable random access)

* “A stream backed by a regular file descriptor.” ([Apache Arrow][12])
* Supports `read_at(nbytes, offset)` for random-access reads and `get_stream(file_offset, nbytes)` to create an independent segment stream (doesn’t interfere with the file’s current position). ([Apache Arrow][12])
* `read1()` semantics differ: if `nbytes is None`, `read1()` reads a **chunk**, not the whole file. ([Apache Arrow][12])

**Concurrency pattern**: use `get_stream()` to read multiple segments safely in parallel. ([Apache Arrow][12])

---

## E3.2 `BufferReader` (zero-copy “file” over bytes/Buffer)

* “Zero-copy reader from objects convertible to Arrow buffer”; takes `bytes` or `pyarrow.Buffer`. ([Apache Arrow][13])
* Provides `read_at`, `read_buffer`, `seek`, etc. like a file. ([Apache Arrow][13])

This is the canonical adapter when you receive Parquet/IPC payloads as bytes and want Arrow readers that require a file-like input.

---

## E3.3 `BufferOutputStream` (write to a resizable in-memory buffer)

* Writes into a resizable buffer; `getvalue()` **finalizes** and returns a `pyarrow.Buffer`. ([Apache Arrow][14])
* Common use: build an Arrow IPC file/stream in memory, then send bytes over HTTP.

---

## E3.4 `FixedSizeBufferWriter` (write into preallocated memory)

* “A stream writing to an Arrow buffer.” ([Apache Arrow][15])
* Typical: `buf = pa.allocate_buffer(N)` then `pa.output_stream(buf)` yields a `FixedSizeBufferWriter` and writes directly into that buffer. ([Apache Arrow][15])

This is the “no reallocations, bounded output size” path.

---

## E3.5 `PythonFile` (bridge arbitrary Python file objects into Arrow)

* Lets Arrow use Python file objects (including from other languages) but has a “non-zero redirection cost” and the GIL can limit parallelism. ([Apache Arrow][16])

Use it when integration pressure wins; avoid it in hot paths when you can use `OSFile`/filesystem streams.

---

# E4) Memory-mapped IO (mmap) — true zero-copy file reads

## E4.1 `pyarrow.MemoryMappedFile`

* Represents a memory-mapped file; supports `'r'`, `'r+'`, `'w'` modes. ([Apache Arrow][17])
* Creation: `pa.create_memory_map(path, size)` (pre-sized). ([Apache Arrow][17])
* Also has `get_stream(file_offset, nbytes)` (segment stream), and `resize(new_size)` to resize map + underlying file. ([Apache Arrow][17])

## E4.2 `pyarrow.memory_map(path, mode='r')`

* “Open memory map at file path. Size of the memory map cannot change.” ([Apache Arrow][18])
* Intended for the classic “read without allocations/copies” style access (`read_at`). ([Apache Arrow][18])

### Minimal mmap snippet (read-by-offset)

```python
import pyarrow as pa

with pa.memory_map("example_mmap.txt") as mm:
    chunk = mm.read_at(6, 45)   # random access read
```

([Apache Arrow][18])

## E4.3 Filesystem-level mmap toggling: `LocalFileSystem(use_mmap=True)`

`pyarrow.fs.LocalFileSystem(use_mmap=...)` controls whether `open_input_stream` / `open_input_file` return a mmap’ed file or a regular file. ([Apache Arrow][19])

This is a clean “policy flip” if you’re already using `pyarrow.fs` and want mmap by default on local paths.

---

# E5) Compression primitives: codecs, buffer compression, stream wrappers

You have two orthogonal layers:

1. **Buffer-level compression**: `compress` / `decompress` (whole payload, in memory).
2. **Stream-level compression**: CompressedInput/OutputStream wrappers (pipe bytes through a codec while reading/writing).

## E5.1 `pyarrow.Codec` (capability detection + compression levels)

* Construct with `pa.Codec(compression, compression_level=None)`; supported names include: `gzip`, `bz2`, `brotli`, `lz4` / `lz4_frame`, `lz4_raw`, `zstd`, `snappy`. ([Apache Arrow][20])
* Useful static helpers:

  * `Codec.is_available(name)` (compiled/available?) ([Apache Arrow][20])
  * `Codec.detect(path)` (infer from file extension) ([Apache Arrow][20])
  * `default_compression_level`, min/max, `supports_compression_level`. ([Apache Arrow][20])

## E5.2 Buffer-level APIs: `pyarrow.compress` / `pyarrow.decompress`

* `pa.compress(buf, codec='lz4', ...)` compresses a buffer-like object; docs list supported codecs including brotli/gzip/lz4/lz4_raw/snappy/zstd. ([Apache Arrow][21])
* `pa.decompress(buf, decompressed_size=None, codec='lz4', ...)` decompresses. ([Apache Arrow][22])

Use these when you already have the entire payload in memory and you want a one-shot transform.

## E5.3 Stream wrappers: `CompressedOutputStream` / `CompressedInputStream`

* `CompressedInputStream(stream, compression)` = “decompresses data on the fly”; `compression` can be `bz2`, `brotli`, `gzip`, `lz4`, `zstd`. ([Apache Arrow][23])
* The docs explicitly show that `pa.input_stream(cdata, compression="gzip")` is effectively `BufferReader(cdata)` + `CompressedInputStream(raw, "gzip")`. ([Apache Arrow][23])
* `CompressedOutputStream` compresses bytes written into an underlying stream (example uses `BufferOutputStream` + gzip). ([Apache Arrow][24])

### Minimal “compress-to-buffer then read back” recipe

```python
import pyarrow as pa

raw = pa.BufferOutputStream()
with pa.CompressedOutputStream(raw, "gzip") as z:
    z.write(b"Compressed stream")

cdata = raw.getvalue()

with pa.input_stream(cdata, compression="gzip") as inp:
    assert inp.read() == b"Compressed stream"
```

([Apache Arrow][23])

## E5.4 High-level factories: `input_stream` / `output_stream` (with compression=detect)

* `pa.input_stream(source, compression='detect', buffer_size=None)` chooses codec based on file extension when `compression="detect"` and the source is a file path; `buffer_size` controls buffering (0/None disables). ([Apache Arrow][25])
* `pa.output_stream(source, compression='detect', buffer_size=None)` similarly supports on-the-fly compression + buffering. ([Apache Arrow][26])

This is the “do the right wrapper stack automatically” entrypoint (BufferReader vs OSFile vs PythonFile, + optional compression).

---

# E6) “Minimum viable” implementation patterns (what experts actually wire up)

## Pattern 1: Serve Arrow IPC bytes without intermediate copies (in-memory sink)

* Use `BufferOutputStream()` to build bytes and call `getvalue()` for a `Buffer`. ([Apache Arrow][14])
* Only convert to Python `bytes` at the edge if your transport requires it (`Buffer.to_pybytes()` copies). ([Apache Arrow][1])

## Pattern 2: Read Parquet/IPC from an in-memory payload

* Wrap with `BufferReader(payload)` to provide a `NativeFile` to Parquet/IPC readers. ([Apache Arrow][13])

## Pattern 3: Avoid long-lived `PythonFile` in hot loops

* `PythonFile` is convenient but has redirection overhead and GIL implications; prefer `OSFile`/filesystem streams when possible. ([Apache Arrow][16])

## Pattern 4: High-throughput random reads from one file (multi-thread safe)

* Use `OSFile.get_stream(offset, nbytes)` (or the same on `MemoryMappedFile`) to create independent segment streams for parallel processing. ([Apache Arrow][12])

---

If you want the next section after E: the natural continuation is **F) IPC deep control (IpcReadOptions/IpcWriteOptions, message-level APIs, tensors, custom metadata)** because it builds directly on the stream/buffer primitives above.

[1]: https://arrow.apache.org/docs/python/generated/pyarrow.Buffer.html "pyarrow.Buffer — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/memory.html?utm_source=chatgpt.com "Memory and IO Interfaces — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.allocate_buffer.html "pyarrow.allocate_buffer — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.py_buffer.html "pyarrow.py_buffer — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.foreign_buffer.html "pyarrow.foreign_buffer — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.ResizableBuffer.html "pyarrow.ResizableBuffer — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.MemoryPool.html "pyarrow.MemoryPool — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.default_memory_pool.html "pyarrow.default_memory_pool — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.set_memory_pool.html "pyarrow.set_memory_pool — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/python/generated/pyarrow.log_memory_allocations.html?utm_source=chatgpt.com "pyarrow.log_memory_allocations — Apache Arrow v22.0.0"
[11]: https://arrow.apache.org/docs/python/generated/pyarrow.total_allocated_bytes.html?utm_source=chatgpt.com "pyarrow.total_allocated_bytes — Apache Arrow v22.0.0"
[12]: https://arrow.apache.org/docs/python/generated/pyarrow.OSFile.html "pyarrow.OSFile — Apache Arrow v22.0.0"
[13]: https://arrow.apache.org/docs/python/generated/pyarrow.BufferReader.html "pyarrow.BufferReader — Apache Arrow v22.0.0"
[14]: https://arrow.apache.org/docs/python/generated/pyarrow.BufferOutputStream.html "pyarrow.BufferOutputStream — Apache Arrow v22.0.0"
[15]: https://arrow.apache.org/docs/python/generated/pyarrow.FixedSizeBufferWriter.html?utm_source=chatgpt.com "pyarrow.FixedSizeBufferWriter — Apache Arrow v22.0.0"
[16]: https://arrow.apache.org/docs/python/generated/pyarrow.PythonFile.html "pyarrow.PythonFile — Apache Arrow v22.0.0"
[17]: https://arrow.apache.org/docs/python/generated/pyarrow.MemoryMappedFile.html "pyarrow.MemoryMappedFile — Apache Arrow v22.0.0"
[18]: https://arrow.apache.org/docs/python/generated/pyarrow.memory_map.html "pyarrow.memory_map — Apache Arrow v22.0.0"
[19]: https://arrow.apache.org/docs/python/generated/pyarrow.fs.LocalFileSystem.html?utm_source=chatgpt.com "pyarrow.fs.LocalFileSystem — Apache Arrow v22.0.0"
[20]: https://arrow.apache.org/docs/python/generated/pyarrow.Codec.html "pyarrow.Codec — Apache Arrow v22.0.0"
[21]: https://arrow.apache.org/docs/python/generated/pyarrow.compress.html?utm_source=chatgpt.com "pyarrow.compress — Apache Arrow v22.0.0"
[22]: https://arrow.apache.org/docs/python/generated/pyarrow.decompress.html?utm_source=chatgpt.com "pyarrow.decompress — Apache Arrow v22.0.0"
[23]: https://arrow.apache.org/docs/python/generated/pyarrow.CompressedInputStream.html "pyarrow.CompressedInputStream — Apache Arrow v22.0.0"
[24]: https://arrow.apache.org/docs/python/generated/pyarrow.CompressedOutputStream.html "pyarrow.CompressedOutputStream — Apache Arrow v22.0.0"
[25]: https://arrow.apache.org/docs/python/generated/pyarrow.input_stream.html "pyarrow.input_stream — Apache Arrow v22.0.0"
[26]: https://arrow.apache.org/docs/python/generated/pyarrow.output_stream.html "pyarrow.output_stream — Apache Arrow v22.0.0"


## F) IPC “deep control” (options, message-level APIs, tensors, custom metadata) — deep dive (PyArrow/Arrow v22)

### Mental model: IPC = *schema + dictionaries + record batches* serialized as **messages**

PyArrow exposes IPC at three layers of abstraction:

1. **High-level stream/file readers & writers** (`open_stream/open_file`, `new_stream/new_file`) for “normal” batch/table IO. ([Apache Arrow][1])
2. **Mid-level options objects** (`IpcWriteOptions`, `IpcReadOptions`) that control compatibility, compression, alignment, projection, and dictionary behavior. ([Apache Arrow][2])
3. **Low-level message primitives** (`read_message`, `MessageReader`, `Message`, `read_record_batch`) for custom protocols and partial parsing. ([Apache Arrow][3])

---

# F1) IPC formats & entrypoints (file vs stream)

### Stream format (sequential)

* **Write**: `pa.ipc.new_stream(sink, schema, options=...) → RecordBatchStreamWriter` ([Apache Arrow][4])
* **Read**: `pa.ipc.open_stream(source, options=..., memory_pool=...) → RecordBatchStreamReader` ([Apache Arrow][5])
* Use when the transport is inherently streaming (pipes, sockets, chunked HTTP), or when you want minimal footer/indexing overhead.

### File format (seekable, footer)

* **Write**: `pa.ipc.new_file(sink, schema, options=..., metadata=...) → RecordBatchFileWriter` (note file-level footer metadata) ([Apache Arrow][6])
* **Read**: `pa.ipc.open_file(source, footer_offset=..., options=..., memory_pool=...) → RecordBatchFileReader` (random access, batch index, footer metadata) ([Apache Arrow][7])
* Use when you want random access (`get_batch(i)`), embedded-file reading (`footer_offset`), and file-level metadata. ([Apache Arrow][8])

---

# F2) IPC options (the real “advanced surface area”)

## F2.1 Write options: `pyarrow.ipc.IpcWriteOptions(...)`

Signature (v22):
`IpcWriteOptions(metadata_version=V5, allow_64bit=False, use_legacy_format=False, compression=None, use_threads=True, emit_dictionary_deltas=False, unify_dictionaries=False)` ([Apache Arrow][2])

### Parameters you actually tune

**`metadata_version`**

* V5 is “current and latest”; V4 is pre-1.0 with an incompatible union layout. ([Apache Arrow][2])
* Use V4 only to interop with very old readers that choke on V5 unions.

**`use_legacy_format`**

* Forces pre-Arrow 0.15 IPC format. ([Apache Arrow][2])
* This is separate from `metadata_version`: you can have V5 metadata but still request legacy format behavior where supported.

**`allow_64bit`**

* Allows field lengths that don’t fit in signed 32-bit int. ([Apache Arrow][2])
* Flip this when you have *very* large arrays/strings where offsets/lengths exceed 2^31-1.

**`compression`**

* Record-batch body buffer compression: `"lz4"` or `"zstd"` (or `None`). To specify compression level, pass a `pyarrow.Codec`. ([Apache Arrow][2])
* Operational implication: compression happens per-batch buffer; you can combine with `use_threads=True`.

**`use_threads`**

* Uses Arrow’s CPU thread pool to parallelize compression work. ([Apache Arrow][2])

**`emit_dictionary_deltas`**

* Emits dictionary deltas; default is false for “maximum stream compatibility.” ([Apache Arrow][2])

**`unify_dictionaries`**

* When writing tables, attempts to unify dictionaries across all batches; helps avoid replacement dictionaries (file format doesn’t support them) but requires computing unified dictionaries + remapping indices. ([Apache Arrow][2])
* **Ignored for IPC stream format** because stream format can support replacement dictionaries. ([Apache Arrow][2])

### Two practical presets

```python
import pyarrow as pa

# 1) “Interop-safe”: maximize compatibility
opts_compat = pa.ipc.IpcWriteOptions(
    metadata_version=pa.ipc.MetadataVersion.V4,  # only if you need it
    use_legacy_format=False,
    compression=None,
    emit_dictionary_deltas=False,
)

# 2) “Perf/size”: modern readers, compressed
opts_perf = pa.ipc.IpcWriteOptions(
    metadata_version=pa.ipc.MetadataVersion.V5,
    compression="zstd",
    use_threads=True,
    unify_dictionaries=True,  # matters most for file format + dict columns
)
```

(Use V4/legacy only when you have an explicit compatibility target.) ([Apache Arrow][2])

---

## F2.2 Read options: `pyarrow.ipc.IpcReadOptions(...)`

Signature (v22):
`IpcReadOptions(ensure_native_endian=True, ensure_alignment=Alignment.Any, use_threads=True, included_fields=None)` ([Apache Arrow][9])

### Parameters you actually tune

**`ensure_native_endian`**

* Converts incoming data to platform-native endianness. ([Apache Arrow][9])

**`ensure_alignment`**

* If mis-aligned, data is copied to aligned memory; useful for consumers that require e.g. 4-byte alignment for int32 buffers. ([Apache Arrow][9])
* Default alignment mode keeps as-is (so no copy) in the underlying Arrow semantics. ([Apache Arrow][10])

**`use_threads`**

* Parallelizes decompression / other compute tasks during reading. ([Apache Arrow][9])

**`included_fields` (top-level projection)**

* If empty/None: deserialize all fields.
* If non-empty: list of **top-level field indices** to read. ([Apache Arrow][9])

### Minimal “projection + strict alignment” example

```python
import pyarrow as pa

ropts = pa.ipc.IpcReadOptions(
    included_fields=[0, 3, 5],  # top-level columns only
    ensure_alignment=pa.ipc.Alignment.DataTypeSpecific,
    use_threads=True,
)
reader = pa.ipc.open_file("data.arrow", options=ropts)
```

([Apache Arrow][9])

---

# F3) High-level readers/writers (and the “advanced” methods people miss)

## F3.1 Stream writer: `RecordBatchStreamWriter`

Key methods:

* `write(table_or_batch)` (Table or RecordBatch)
* `write_table(table, max_chunksize=...)`
* `write_batch(batch, custom_metadata=...)`
* `.stats` for write statistics ([Apache Arrow][11])

**Per-batch custom metadata**: `write_batch(..., custom_metadata=...)` where keys/values are string-like/coercible to bytes. ([Apache Arrow][11])

## F3.2 Stream reader: `RecordBatchStreamReader`

“Deep control” methods:

* `read_next_batch()` (iterator-style consumption)
* `read_next_batch_with_custom_metadata()` and `iter_batches_with_custom_metadata()` (metadata-aware streaming)
* `cast(target_schema)` wraps reader and lazily casts each batch (safe cast; names + order must match) ([Apache Arrow][12])

```python
import pyarrow as pa

r = pa.ipc.open_stream(source)                       # RecordBatchStreamReader
r2 = r.cast(target_schema)                           # lazy safe cast
batch, md = r.read_next_batch_with_custom_metadata() # per-batch metadata
```

([Apache Arrow][5])

## F3.3 File writer: `RecordBatchFileWriter` (+ file-level metadata)

* Constructed via `ipc.new_file(..., metadata=...)` or directly as `RecordBatchFileWriter(..., metadata=...)`. ([Apache Arrow][6])
* Same write surface as stream writer, including `write_batch(..., custom_metadata=...)`. ([Apache Arrow][13])

## F3.4 File reader: `RecordBatchFileReader` (random access + embedded files)

Notable “advanced” surface:

* `footer_offset` for reading an IPC file embedded inside a larger blob/file. ([Apache Arrow][8])
* `metadata` property: **file-level** custom metadata from footer. ([Apache Arrow][8])
* `get_batch_with_custom_metadata(i) → (RecordBatch, KeyValueMetadata)` for per-batch metadata. ([Apache Arrow][8])
* `stats` for read statistics. ([Apache Arrow][8])

```python
import pyarrow as pa

r = pa.ipc.open_file("data.arrow")
file_meta = r.metadata  # footer key/value metadata
for i in range(r.num_record_batches):
    batch, batch_meta = r.get_batch_with_custom_metadata(i)
```

([Apache Arrow][7])

---

# F4) Message-level APIs (custom protocols, partial parsing, re-wrapping)

## F4.1 Read raw messages

* `ipc.read_message(source) -> Message` reads a **length-prefixed** IPC message from file/buffer. ([Apache Arrow][3])
* `MessageReader.open_stream(source)` + `read_next_message()` iterates messages until `StopIteration`. ([Apache Arrow][14])

## F4.2 Inspect and re-serialize messages

A `Message` exposes:

* `body`, `metadata`, `metadata_version`, `type`
* `serialize(alignment=8, memory_pool=None) -> Buffer`
* `serialize_to(sink, alignment=8, memory_pool=None)` ([Apache Arrow][15])

## F4.3 Decode record batches from a message buffer

* `ipc.read_record_batch(obj, schema, dictionary_memo=None)` reads a RecordBatch from a `Message` or buffer-like object **given a known schema**; if dictionaries are involved you must pass a populated `DictionaryMemo`. ([Apache Arrow][16])

## F4.4 Size planning for custom transport buffers

* `ipc.get_record_batch_size(batch)` returns total serialized size including metadata + padding. ([Apache Arrow][17])
* `RecordBatch.serialize()` writes an encapsulated IPC message **without a Schema** (useful when your schema is out-of-band). ([Apache Arrow][18])

---

# F5) Tensors over IPC (ndarray transport that can be zero-copy)

## F5.1 Tensor object basics

* `pa.Tensor.from_numpy(ndarray, dim_names=...)`
* `tensor.to_numpy()` is **zero-copy** conversion to NumPy (when possible). ([Apache Arrow][19])

## F5.2 IPC tensor IO

* `ipc.write_tensor(tensor, dest)` writes at the current position and returns bytes written. ([Apache Arrow][20])
* `ipc.read_tensor(source)` reads from current position; if the source supports zero-copy (e.g., memory map), it allocates no memory; it doesn’t assume the stream is aligned. ([Apache Arrow][21])
* `ipc.get_tensor_size(tensor)` returns total size including metadata + padding (useful for prealloc / shared-memory). ([Apache Arrow][22])

```python
import numpy as np
import pyarrow as pa

x = np.arange(12, dtype=np.int32).reshape(3, 4)
t = pa.Tensor.from_numpy(x, dim_names=["r", "c"])
nbytes = pa.ipc.get_tensor_size(t)

sink = pa.BufferOutputStream()
pa.ipc.write_tensor(t, sink)
buf = sink.getvalue()

src = pa.BufferReader(buf)
t2 = pa.ipc.read_tensor(src)
x2 = t2.to_numpy()   # zero-copy view of tensor buffer
```

([Apache Arrow][19])

---

# F6) Custom metadata (file-level vs batch-level) — precise mechanics

## F6.1 File-level footer metadata (IPC **file** format only)

* `ipc.new_file(..., metadata=...)` stores key/value pairs in the file footer; **both must be bytes-like**; read via `ipc.open_file(...).metadata`. ([Apache Arrow][6])

## F6.2 RecordBatch-level metadata (stream + file)

* `RecordBatchStreamWriter.write_batch(batch, custom_metadata=...)` and `RecordBatchFileWriter.write_batch(...)` accept custom metadata; keys/values must be string-like/coercible to bytes. ([Apache Arrow][11])
* Read it back via:

  * stream: `read_next_batch_with_custom_metadata()` / `iter_batches_with_custom_metadata()` ([Apache Arrow][12])
  * file: `get_batch_with_custom_metadata(i)` ([Apache Arrow][8])

**Minimal “batch metadata round-trip”**

```python
import pyarrow as pa

tbl = pa.table({"a": [1, 2, 3]})
sink = pa.BufferOutputStream()

# file-level footer metadata + per-batch metadata
with pa.ipc.new_file(sink, tbl.schema, metadata={b"creator": b"me"}) as w:
    w.write_batch(tbl.to_batches()[0], custom_metadata={"chunk": "0"})

buf = sink.getvalue()
r = pa.ipc.open_file(buf)
assert r.metadata == {b"creator": b"me"}  # footer metadata
batch, md = r.get_batch_with_custom_metadata(0)
```

([Apache Arrow][6])

[1]: https://arrow.apache.org/docs/python/api/ipc.html "Serialization and IPC — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.IpcWriteOptions.html "pyarrow.ipc.IpcWriteOptions — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.read_message.html "pyarrow.ipc.read_message — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.new_stream.html "pyarrow.ipc.new_stream — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.open_stream.html "pyarrow.ipc.open_stream — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.new_file.html "pyarrow.ipc.new_file — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.open_file.html "pyarrow.ipc.open_file — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.RecordBatchFileReader.html "pyarrow.ipc.RecordBatchFileReader — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.IpcReadOptions.html "pyarrow.ipc.IpcReadOptions — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/cpp/api/ipc.html?utm_source=chatgpt.com "Arrow IPC — Apache Arrow v22.0.0"
[11]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.RecordBatchStreamWriter.html "pyarrow.ipc.RecordBatchStreamWriter — Apache Arrow v22.0.0"
[12]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.RecordBatchStreamReader.html "pyarrow.ipc.RecordBatchStreamReader — Apache Arrow v22.0.0"
[13]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.RecordBatchFileWriter.html "pyarrow.ipc.RecordBatchFileWriter — Apache Arrow v22.0.0"
[14]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.MessageReader.html "pyarrow.ipc.MessageReader — Apache Arrow v22.0.0"
[15]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.Message.html "pyarrow.ipc.Message — Apache Arrow v22.0.0"
[16]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.read_record_batch.html "pyarrow.ipc.read_record_batch — Apache Arrow v22.0.0"
[17]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.get_record_batch_size.html?utm_source=chatgpt.com "pyarrow.ipc.get_record_batch_size — Apache Arrow v22.0.0"
[18]: https://arrow.apache.org/docs/python/generated/pyarrow.RecordBatch.html "pyarrow.RecordBatch — Apache Arrow v22.0.0"
[19]: https://arrow.apache.org/docs/python/generated/pyarrow.Tensor.html "pyarrow.Tensor — Apache Arrow v22.0.0"
[20]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.write_tensor.html "pyarrow.ipc.write_tensor — Apache Arrow v22.0.0"
[21]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.read_tensor.html "pyarrow.ipc.read_tensor — Apache Arrow v22.0.0"
[22]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.get_tensor_size.html?utm_source=chatgpt.com "pyarrow.ipc.get_tensor_size — Apache Arrow v22.0.0"


## G) Parquet “power user” APIs (incremental reads, metadata, row groups) — deep dive (PyArrow/Arrow v22)

### Mental model: *row groups* are the “IO granularity”, and the footer metadata is the “query planner”

A Parquet file is organized as **row groups**, each row group contains **column chunks** (one per column), and the file footer contains **FileMetaData → RowGroupMetaData → ColumnChunkMetaData (+ Statistics)**. PyArrow exposes this hierarchy directly via `ParquetFile.metadata` and the `FileMetaData/RowGroupMetaData/ColumnChunkMetaData/Statistics` classes. ([Apache Arrow][1])

---

# G1) Incremental reads (streaming + row-group targeted IO)

## G1.1 The “power reader”: `pyarrow.parquet.ParquetFile(...)`

`pq.ParquetFile` is the single-file reader that exposes **row-group** and **batch** streaming APIs. It’s also where the low-level performance knobs live. ([Apache Arrow][1])

Key constructor knobs (advanced, actually useful):

* `memory_map=True`: mmap local paths (can improve performance depending on environment). ([Apache Arrow][1])
* `buffer_size>0`: enable buffered reads when deserializing column chunks. ([Apache Arrow][1])
* `pre_buffer=True`: coalesce + parallelize file reads using Arrow’s background IO pool; explicitly called out as beneficial for high-latency filesystems like S3. ([Apache Arrow][1])
* `metadata=` / `common_metadata=`: reuse already-loaded `FileMetaData` objects (avoid rereading footers; `common_metadata` is used for pandas schema metadata fallback). ([Apache Arrow][1])
* `read_dictionary=[...]`: force listed columns to be read as `DictionaryArray` (categorical-ish). ([Apache Arrow][1])
* `arrow_extensions_enabled=True`: read certain Parquet logical types as Arrow canonical extension types (e.g., JSON/UUID). ([Apache Arrow][1])

Also: if you already have the bytes, docs explicitly recommend wrapping the payload with `pyarrow.BufferReader` and passing that as `source`. ([Apache Arrow][1])

---

## G1.2 Streaming record batches: `ParquetFile.iter_batches(...)`

This is the “don’t materialize a whole table” entrypoint:

```python
import pyarrow.parquet as pq

pf = pq.ParquetFile("big.parquet", memory_map=True, pre_buffer=True)

for rb in pf.iter_batches(
    batch_size=131072,
    row_groups=[0, 3, 7],
    columns=["id", "ts", "payload"],   # can be nested prefix
    use_threads=True,
):
    consume(rb)  # rb is a pyarrow.RecordBatch
```

Semantics you should rely on:

* `iter_batches(batch_size=..., row_groups=..., columns=..., use_threads=..., use_pandas_metadata=...)` yields `pyarrow.RecordBatch` objects. ([Apache Arrow][1])
* `columns` supports *nested prefix selection* (`"a"` selects `"a.b"`, `"a.c"`, etc.). ([Apache Arrow][1])
* `row_groups` constrains which row groups are read (the fundamental primitive for “partial reads”). ([Apache Arrow][1])

> Practical use: “read first N rows” = loop `iter_batches(...)` and stop when you’ve accumulated N, instead of reading a full table.

---

## G1.3 Row-group reads (table materialization, but scoped): `read_row_group` / `read_row_groups`

When you want a `Table` but *only for selected row groups*:

```python
import pyarrow.parquet as pq

pf = pq.ParquetFile("big.parquet", pre_buffer=True)

t_rg0 = pf.read_row_group(0, columns=["id", "ts"])          # one row group
t_some = pf.read_row_groups([0, 5, 9], columns=["id", "ts"])  # multiple row groups
```

Both APIs are explicit about row-group targeting and share the key knobs:

* `columns` (prefix-friendly for nested fields)
* `use_threads`
* `use_pandas_metadata` ([Apache Arrow][1])

---

## G1.4 “Scan the file” (benchmarking): `scan_contents(...)`

`scan_contents(columns=None, batch_size=...) -> num_rows` exists primarily for benchmarking and is documented as executing on a single thread. ([Apache Arrow][1])

---

# G2) Metadata-only workflows (fast planning without reading column data)

## G2.1 Read only the footer: `pyarrow.parquet.read_metadata(...)`

If you want *just* file metadata (no column pages):

```python
import pyarrow.parquet as pq

md = pq.read_metadata("big.parquet", memory_map=True)  # returns FileMetaData
```

* `read_metadata(where, memory_map=False, decryption_properties=None, filesystem=None)` returns `FileMetaData` from the footer and supports `memory_map=True` when `where` is a file path. ([Apache Arrow][2])

## G2.2 Reuse metadata to avoid rereads

`ParquetFile(..., metadata=<FileMetaData>)` lets you feed an already-loaded footer into the reader (useful when you do a planning pass first, then do targeted reads). ([Apache Arrow][1])

---

# G3) The metadata object graph (row groups → column chunks → statistics)

## G3.1 `FileMetaData` (file-level)

Notable `FileMetaData` surface:

* Attributes: `created_by`, `format_version`, `metadata` (key/value `dict[bytes, bytes]`), `num_row_groups`, `num_rows`, `schema`, `serialized_size`. ([Apache Arrow][3])
* `row_group(i) -> RowGroupMetaData` (entrypoint into per-row-group metadata). ([Apache Arrow][3])
* Dataset-style composition helpers:

  * `append_row_groups(other)` (merge row groups from another file’s metadata)
  * `write_metadata_file(where)` (write metadata to a metadata-only Parquet file)
  * `set_file_path(path)` (set column-chunk file paths) ([Apache Arrow][3])

## G3.2 `RowGroupMetaData` (row-group level)

Notable surface:

* `num_rows`, `num_columns`, `total_byte_size`, `sorting_columns` ([Apache Arrow][4])
* `column(i) -> ColumnChunkMetaData` ([Apache Arrow][4])
* `to_dict()` for debugging/inspection ([Apache Arrow][4])

## G3.3 `ColumnChunkMetaData` (column-chunk level)

This is where you get the “planner-grade” facts:

* Location/identity: `file_offset`, `file_path`, `path_in_schema` ([Apache Arrow][5])
* Compression + encoding: `compression`, `encodings`, `total_compressed_size`, `total_uncompressed_size` ([Apache Arrow][5])
* Page offsets: `data_page_offset`, `dictionary_page_offset` ([Apache Arrow][5])
* Statistics presence + values: `is_stats_set`, `statistics`, plus `metadata` (kv) ([Apache Arrow][5])
* Index-related flags also surface here (some marked “not yet supported” in Python bindings). ([Apache Arrow][5])

## G3.4 `Statistics` (column statistics within one row group)

Core surface:

* `has_min_max`, `min`, `max`, `min_raw`, `max_raw`
* `null_count`, `distinct_count`, plus `has_null_count`, `has_distinct_count` ([Apache Arrow][6])

---

# G4) Row-group pruning (manual planner → targeted read)

### The canonical “planner loop”

```python
import pyarrow.parquet as pq

md = pq.read_metadata("big.parquet", memory_map=True)

wanted = []
col_name = "ts"  # choose the column you’ll prune on

# Find the column index once (simple approach: align to schema order)
# In production, map name->index using the Parquet schema / Arrow schema mapping you already maintain.
col_idx = 0

for rg_i in range(md.num_row_groups):
    rg = md.row_group(rg_i)
    cc = rg.column(col_idx)
    if not cc.is_stats_set:
        continue
    st = cc.statistics
    if st.has_min_max and st.min <= SOME_VALUE <= st.max:
        wanted.append(rg_i)

pf = pq.ParquetFile("big.parquet", metadata=md, pre_buffer=True)
for batch in pf.iter_batches(row_groups=wanted, columns=["id", "ts", "payload"]):
    consume(batch)
```

Key correctness rules (don’t skip these):

* Stats may be absent (`is_stats_set` / `has_min_max`), so pruning must degrade gracefully. ([Apache Arrow][5])
* Row-group pruning is naturally expressed as selecting `row_groups=[...]` in `iter_batches(...)` or `read_row_groups(...)`. ([Apache Arrow][1])

---

# G5) Writing & dataset-sidecar metadata (`_metadata` / `_common_metadata`) — the “power” pattern

## G5.1 Collect per-file metadata while writing

`ParquetWriter` supports a `metadata_collector` option: if present, it is populated with the `FileMetaData` of the written file. ([Apache Arrow][7])

## G5.2 Emit metadata-only Parquet files

Two official ways:

* `pyarrow.parquet.write_metadata(schema, where, metadata_collector=..., ...)` writes a metadata-only Parquet file from a schema, and is explicitly noted as usable to generate `_common_metadata` and `_metadata` sidecars with dataset writing workflows. ([Apache Arrow][8])
* `FileMetaData.write_metadata_file(where)` writes an existing metadata object to a metadata-only Parquet file. ([Apache Arrow][3])

## G5.3 Aggregate many file metadatas into one

Use:

* `FileMetaData.append_row_groups(other)` to build an aggregated metadata object, then
* `write_metadata_file(...)` to persist it. ([Apache Arrow][3])

This is the core building block behind “single metadata file that describes many shards”, which is often the difference between instant dataset discovery vs O(N files) footer reads.

---

If you want, I can do the next section (**H) Dataset deep control**) in the same style, and explicitly connect **Dataset Scanner → fragment scan tasks → Parquet row-group pruning** so you can choose when to stay in `pq.ParquetFile` vs when to move to `pyarrow.dataset`.

[1]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetFile.html "pyarrow.parquet.ParquetFile — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.read_metadata.html "pyarrow.parquet.read_metadata — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.FileMetaData.html "pyarrow.parquet.FileMetaData — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.RowGroupMetaData.html "pyarrow.parquet.RowGroupMetaData — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ColumnChunkMetaData.html "pyarrow.parquet.ColumnChunkMetaData — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.Statistics.html "pyarrow.parquet.Statistics — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html "pyarrow.parquet.ParquetWriter — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_metadata.html "pyarrow.parquet.write_metadata — Apache Arrow v22.0.0"


## H) PyArrow Dataset deep control (discovery, factories, fragments, scan tasks, streaming reads) — deep dive (PyArrow/Arrow v22)

### Mental model: `Dataset` = *union of Fragments*; `Scanner` = *bound scan plan* that drives fragment/task execution

* `ds.dataset(...)` is the **high-level constructor**: it does source discovery + basic normalization, and advertises predicate pushdown, projection, and parallel reading / task management, but explicitly points you to the low-level classes when you need deterministic control. ([Apache Arrow][1])
* A `Scanner` is the object that binds **projection + filter + readahead + threading + fragment-specific scan options**; in Arrow’s own dataset model, fragments are “independently loadable units,” the fragment’s format creates load tasks, and the scanner sequences record batches while obeying readahead limits (so it doesn’t have to buffer the entire dataset). ([Apache Arrow][2])

---

# H1) Discovery & construction: `pyarrow.dataset.dataset(...)` as the “front door”

## H1.1 Full surface area: supported sources + returned dataset type

Signature (v22):
`ds.dataset(source, schema=None, format=None, filesystem=None, partitioning=None, partition_base_dir=None, exclude_invalid_files=None, ignore_prefixes=None)` ([Apache Arrow][1])

`source` can be:

* path (file or directory), list of paths, dataset, list of datasets
* record batches / tables, iterable of record batches, `RecordBatchReader`, or URI ([Apache Arrow][1])

Return type:

* `FileSystemDataset` (typical), or `UnionDataset` (when you pass a list of datasets) ([Apache Arrow][1])

Key semantics you should design around:

* **List of datasets** → constructs a *nested `UnionDataset`* and forbids extra kwargs. ([Apache Arrow][1])
* **Iterable / `RecordBatchReader`** → creates an `InMemoryDataset`; if you pass an iterable or `RecordBatchReader`, the resulting dataset **can only be scanned once** (subsequent scans raise). ([Apache Arrow][1])

### Minimal “right default” constructor patterns

```python
import pyarrow.dataset as ds

# 1) single parquet file
d1 = ds.dataset("data/file.parquet", format="parquet")

# 2) directory (recursive discovery + partition inference if configured)
d2 = ds.dataset("data/partitioned/", format="parquet")

# 3) mix sources (UnionDataset)
d3 = ds.dataset([
    ds.dataset("s3://bucket/old/", format="parquet"),
    ds.dataset("local/new.arrow", format="ipc"),
])
```

Discovery + union semantics are specified in the `dataset()` docs. ([Apache Arrow][1])

---

## H1.2 Discovery knobs that materially change behavior

### `partitioning` + `partition_base_dir`

* `partitioning` accepts a `Partitioning`, `PartitioningFactory`, flavor string shortcut, or list of field names (infers a directory partitioning). ([Apache Arrow][1])
* `partition_base_dir` causes paths to be stripped before applying partitioning; files not matching the prefix are skipped for partition discovery but still included (they just won’t have partition info). ([Apache Arrow][1])

### `exclude_invalid_files`

If True (default), Arrow checks file validity during discovery; this “will incur IO for each files in a serial and single threaded fashion.” ([Apache Arrow][1])
Disabling avoids that IO, but you may discover unsupported files and fail at scan time. ([Apache Arrow][1])

### `ignore_prefixes`

When discovering from a directory, ignore basenames matching prefixes (default `['.', '_']`). ([Apache Arrow][1])

---

# H2) Factories: deterministic discovery + schema inspection + “finish()” (no magic)

When you need explicit control over *what was discovered* and *how schema is unified*, the main primitive is:

## H2.1 `FileSystemDatasetFactory(filesystem, paths_or_selector, format, options=...)`

* `paths_or_selector` can be a `FileSelector` (directory crawl) or an explicit list of paths. ([Apache Arrow][3])
* `format`: **only** `ParquetFileFormat` and `IpcFileFormat` are supported for this factory. ([Apache Arrow][3])

Methods (the ones you actually use):

* `inspect(promote_options='default'|'permissive', fragments=N)` → returns a **common schema** based on inspecting up to N fragments; permissive promotes types where possible. ([Apache Arrow][3])
* `finish(schema=None)` → create the Dataset using inspected schema (or enforce your explicit schema). ([Apache Arrow][3])

### Minimal “inspect → override schema → finish” workflow

```python
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as fs

filesystem = fs.LocalFileSystem()

selector = fs.FileSelector("data/partitioned/", recursive=True)

opts = ds.FileSystemFactoryOptions(
    partition_base_dir="data/partitioned/",
    exclude_invalid_files=False,                 # avoid serial per-file IO during discovery
    selector_ignore_prefixes=[".", "_"],         # discovery hygiene
)

factory = ds.FileSystemDatasetFactory(
    filesystem=filesystem,
    paths_or_selector=selector,
    format=ds.ParquetFileFormat(),
    options=opts,
)

inspected = factory.inspect(promote_options="permissive", fragments=50)

# enforce stable contract (e.g., field ordering / explicit nullability)
target_schema = pa.schema([...])
dataset = factory.finish(schema=target_schema)
```

Inspection semantics + promotion modes + `finish()` contract are the core of `FileSystemDatasetFactory`. ([Apache Arrow][3])

---

## H2.2 `FileSystemFactoryOptions` (discovery policy plane)

Constructor: `FileSystemFactoryOptions(partition_base_dir=None, partitioning=None, exclude_invalid_files=None, selector_ignore_prefixes=None)` ([Apache Arrow][4])

Key advanced behaviors:

* `partitioning` vs `partitioning_factory` are mutually overwriting knobs: setting one overwrites the other (and vice versa). ([Apache Arrow][4])
* `partitioning_factory` is the right hook when you want discovery-time inference of partition field schemas and dictionary behavior. ([Apache Arrow][4])

---

# H3) Fragments: treat your dataset as a set of independently scannable units

## H3.1 Enumerating fragments (with filter-aware pruning)

`Dataset.get_fragments(filter=None)` returns an iterator of `Fragment` objects. ([Apache Arrow][5])
If you pass a filter, Arrow may prune fragments using:

* partition expression, or
* internal info like Parquet statistics ([Apache Arrow][5])

### Minimal “filter-aware fragment enumeration”

```python
import pyarrow.dataset as ds

dataset = ds.dataset("data/partitioned/", format="parquet")
pred = (ds.field("year") == 2024) & (ds.field("region") == "us")

for frag in dataset.get_fragments(filter=pred):
    # frag is a pyarrow.dataset.Fragment (often FileFragment/ParquetFileFragment)
    ...
```

Filter-based fragment pruning is explicitly documented. ([Apache Arrow][5])

---

## H3.2 Fragment-level scanning (`Fragment.scanner(...)`)

Calling `frag.scanner(...)` builds a `Scanner` bound to that fragment; **data is not loaded immediately**. ([Apache Arrow][6])

Important advanced parameters:

* `schema`: unify fragment to the dataset schema; if omitted it may scan using the fragment’s physical schema (which can differ per fragment). ([Apache Arrow][6])
* `columns` supports:

  * list of names (order preserved, duplicates allowed), or
  * dict `{new_name: expression}` for computed projections ([Apache Arrow][6])
* supports special “augmented” fields in expressions: `__batch_index`, `__fragment_index`, `__last_in_fragment`, `__filename` ([Apache Arrow][6])

---

## H3.3 Parquet-specific fragment surgery: row-group–level control

A `ParquetFileFragment` can be split/subset by row groups:

* `split_by_row_group(filter=None, schema=None)` yields fragments wrapping each row group; row groups are excluded when their metadata contradicts the optional filter (using **RowGroup statistics**). ([Apache Arrow][7])
* `subset(filter=None, schema=None, row_group_ids=None)` returns a ParquetFileFragment that views only selected row groups (by IDs) or those matching stats-prunable filter. ([Apache Arrow][7])

### Minimal “row-group fragmentation for maximal pushdown”

```python
import pyarrow.dataset as ds

dataset = ds.dataset("data/partitioned/", format="parquet")
pred = ds.field("ts") >= 1700000000

for frag in dataset.get_fragments(filter=pred):
    if isinstance(frag, ds.ParquetFileFragment):
        row_group_frags = frag.split_by_row_group(filter=pred)
        for rg_frag in row_group_frags:
            # now scan at row-group granularity
            for batch in rg_frag.scanner(filter=pred).to_batches():
                consume(batch)
```

Row-group splitting + stats contradiction pruning are core Parquet fragment APIs. ([Apache Arrow][7])

---

# H4) Scan tasks: what they are, and how Python exposes the “control knobs” today

Even if Python no longer exposes a first-class `ScanTask` iterator on `Scanner` (older versions did), the underlying execution model remains:

* Fragments are intended to be read in parallel, and the scanner creates scan tasks from fragments, sequencing record batches while respecting readahead constraints. ([Apache Arrow][2])
* Readahead exists to prevent buffering the entire dataset and to pause IO when the consumer is slow. ([Apache Arrow][2])

**Where you control this in v22 Python: `Scanner` scan parameters**

* `batch_size` (default 131072): cap record-batch size to control memory pressure. ([Apache Arrow][8])
* `batch_readahead` (default 16): number of batches to read ahead in a file (RAM vs IO utilization). ([Apache Arrow][8])
* `fragment_readahead` (default 4): number of files/fragments to read ahead (RAM vs throughput). ([Apache Arrow][8])
* `use_threads=True`: enables maximum parallelism based on CPU cores. ([Apache Arrow][8])
* `cache_metadata=True`: cache metadata to speed repeated scans. ([Apache Arrow][8])

**Pushdown rules (filter & projection)**

* Projection columns are pushed down to fragments to avoid loading / copying / deserializing unused columns. ([Apache Arrow][8])
* Filter pushdown: if possible, predicates are pushed down using partition info or Parquet stats; otherwise filters are applied to loaded record batches. ([Apache Arrow][8])

---

# H5) Streaming reads: `Scanner.to_batches`, `Scanner.to_reader`, `Scanner.scan_batches` (tagged)

## H5.1 The three consumption modes

A `Scanner` can be consumed as:

1. `to_batches()` → iterator of `RecordBatch` (batch stream) ([Apache Arrow][8])
2. `to_reader()` → `RecordBatchReader` (for APIs that want a reader) ([Apache Arrow][8])
3. `scan_batches()` → iterator of `TaggedRecordBatch` (batch + fragment provenance) ([Apache Arrow][8])

`TaggedRecordBatch` is explicitly “a combination of a record batch and the fragment it came from.” ([Apache Arrow][9])

### Minimal “stream + provenance” pattern

```python
import pyarrow.dataset as ds

dataset = ds.dataset("data/partitioned/", format="parquet")
scanner = dataset.scanner(
    columns={"ts": ds.field("ts"), "file": ds.field("__filename")},
    filter=ds.field("year") == 2024,
    batch_size=65536,
)

for tagged in scanner.scan_batches():
    frag = tagged.fragment
    batch = tagged.record_batch
    consume(batch)
```

Tagged batches + special fields are explicitly supported. ([Apache Arrow][9])

---

## H5.2 “Careful with materialization”: `to_table()` is explicitly a memory hazard

`Scanner.to_table()` warns it “will serially materialize the Scan result in memory before creating the Table.” ([Apache Arrow][8])
Use it only when you know the filtered/projection result fits.

---

## H5.3 `take(indices)` is a streaming-friendly row selection primitive

`Scanner.take(indices)` “will only consume as many batches of the underlying dataset as needed”; otherwise it’s equivalent to `to_table().take(indices)`. ([Apache Arrow][8])

This is a useful building block for “index-guided sampling” or “retrieve rows by id list” without fully materializing.

---

# H6) Fragment-specific scan options (the “format knobs” that actually matter)

For Parquet scanning under `pyarrow.dataset`, pass `fragment_scan_options=ds.ParquetFragmentScanOptions(...)` either to `dataset.scanner(...)` or `fragment.scanner(...)`. ([Apache Arrow][8])

Key options (Parquet):

* `use_buffered_stream` + `buffer_size`: read through buffered streams to reduce memory overhead vs loading entire row groups. ([Apache Arrow][10])
* `pre_buffer`: coalesce and parallelize reads (good for high-latency FS like S3/GCS; higher memory). ([Apache Arrow][10])
* `page_checksum_verification`: verify checksums per page. ([Apache Arrow][10])
* `arrow_extensions_enabled`: read Parquet logical types as Arrow canonical extension types (JSON/UUID) when possible. ([Apache Arrow][10])

### Minimal “parquet scan tuned for remote FS”

```python
import pyarrow.dataset as ds

parquet_opts = ds.ParquetFragmentScanOptions(
    pre_buffer=True,                 # throughput on high-latency FS
    use_buffered_stream=False,       # prefer speed over minimal memory
    arrow_extensions_enabled=True,
)

dataset = ds.dataset("s3://bucket/data/", format="parquet")
scanner = dataset.scanner(
    filter=ds.field("dt") == "2025-12-26",
    fragment_scan_options=parquet_opts,
    fragment_readahead=8,
    batch_readahead=32,
)

for batch in scanner.to_batches():
    consume(batch)
```

Scan-option semantics + readahead controls are all part of the v22 Scanner/ParquetFragmentScanOptions surface. ([Apache Arrow][10])

[1]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.dataset.html "pyarrow.dataset.dataset — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/cpp/api/dataset.html "Dataset — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.FileSystemDatasetFactory.html "pyarrow.dataset.FileSystemDatasetFactory — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.FileSystemFactoryOptions.html "pyarrow.dataset.FileSystemFactoryOptions — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Dataset.html "pyarrow.dataset.Dataset — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Fragment.html "pyarrow.dataset.Fragment — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetFileFragment.html "pyarrow.dataset.ParquetFileFragment — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Scanner.html "pyarrow.dataset.Scanner — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.TaggedRecordBatch.html?utm_source=chatgpt.com "pyarrow.dataset.TaggedRecordBatch — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.ParquetFragmentScanOptions.html "pyarrow.dataset.ParquetFragmentScanOptions — Apache Arrow v22.0.0"


## I) PyArrow Table-level relational/transform ops (joins, sort, unify dictionaries) — deep dive (PyArrow/Arrow v22)

### Mental model: `pa.Table` is an immutable *schema + columnar chunked arrays* container

Most “table ops” are **pure** (return a new `Table`) and many are implemented in terms of **compute kernels** (e.g., filter/take) or column-chunk rewrites (combine/unify dictionaries). The hot-path knobs are typically: **chunk layout**, **dictionary normalization**, and **threading**. ([Apache Arrow][1])

---

# I1) Joins: `Table.join(...)` + `Table.join_asof(...)`

## I1.1 `Table.join(...)` (hash join + join-types + residual filter)

### Full surface area

Signature:
`join(self, right_table, keys, right_keys=None, join_type='left outer', left_suffix=None, right_suffix=None, coalesce_keys=True, use_threads=True, filter_expression=None) -> Table` ([Apache Arrow][1])

Key parameters (what they *mean* operationally):

* **`keys` / `right_keys`**: left key columns and (optionally) different-named right key columns; when `right_keys=None`, it uses the same key names on the right. ([Apache Arrow][1])
* **`join_type`** supported values:
  `left semi`, `right semi`, `left anti`, `right anti`, `inner`, `left outer`, `right outer`, `full outer` ([Apache Arrow][1])
* **`left_suffix` / `right_suffix`**: suffixes for name collisions between left/right non-key columns. ([Apache Arrow][1])
* **`coalesce_keys`**: if True, duplicated key columns are omitted from one side in the output. ([Apache Arrow][1])
* **`use_threads`**: enables multithreaded execution. ([Apache Arrow][1])
* **`filter_expression`**: a *residual filter* applied to matching rows (a `pyarrow.compute.Expression`). ([Apache Arrow][1])

### Minimum implementation patterns

**(1) Multi-key join**

```python
import pyarrow as pa

t1 = pa.table({"id": [1, 2, 3], "year": [2020, 2022, 2019]})
t2 = pa.table({"id": [3, 4], "year": [2019, 2022], "animal": ["brittle", "centipede"]})

out = t1.join(t2, keys=["id", "year"], join_type="left outer")
```

Multi-key joins are explicitly supported. ([Apache Arrow][2])

**(2) Different key names + collision-safe suffixes**

```python
out = left.join(
    right,
    keys=["left_id"],
    right_keys=["right_id"],
    left_suffix="_l",
    right_suffix="_r",
)
```

`right_keys` and suffix controls are first-class join parameters. ([Apache Arrow][1])

**(3) Residual mismatch filter (post-match predicate)**

```python
import pyarrow.compute as pc

out = t1.join(
    t2,
    keys="id",
    join_type="inner",
    filter_expression=(pc.field("n_legs") == 100),
)
```

`filter_expression` is a residual filter applied to matches (documented with an example). ([Apache Arrow][1])

---

## I1.2 `Table.join_asof(...)` (time-series “nearest match” join)

### Full surface area

Signature:
`join_asof(self, right_table, on, by, tolerance, right_on=None, right_by=None) -> Table` ([Apache Arrow][1])

Core semantics:

* “Similar to a left-join except we match on nearest key rather than equal keys”; **both tables must be sorted by the key**. ([Apache Arrow][1])
* `on` must be a single field of a common type and (currently) must be **integer, date, or timestamp**. ([Apache Arrow][1])
* `tolerance` defines an inexact match window via `right.on - left.on` and supports:

  * negative → “past-as-of”
  * positive → “future-as-of”
  * zero → exact match ([Apache Arrow][1])
* Optional `by` partitions the join so matching only occurs within equivalent “by” groups. ([Apache Arrow][1])
* `right_on` / `right_by` allow different-named right columns. ([Apache Arrow][1])

### Minimum implementation patterns

**(1) Past-as-of join (typical “latest known value”)**

```python
out = left.join_asof(
    right,
    on="ts",
    by=["account_id"],
    tolerance=-60,      # match right.ts within last 60 units of ts
)
```

Negative tolerance yields past-as-of semantics; `by` partitions the match. ([Apache Arrow][1])

**(2) Different key names**

```python
out = left.join_asof(
    right,
    on="ts_left",
    right_on="ts_right",
    by="symbol",
    tolerance=0,
)
```

`right_on/right_by` are explicit. ([Apache Arrow][1])

---

# I2) Sorting: `Table.sort_by(...)` (and when to drop down to `sort_indices`)

## I2.1 `Table.sort_by(...)`

Signature: `sort_by(self, sorting, **kwargs) -> Table` ([Apache Arrow][1])

* `sorting` can be:

  * `"colname"` (ascending), or
  * list of `(name, order)` where order ∈ `{ "ascending", "descending" }`. ([Apache Arrow][1])
* `**kwargs` are passed as `SortOptions` (notably `null_placement={"at_start","at_end"}`). ([Apache Arrow][1])

```python
sorted_t = t.sort_by(
    [("year", "ascending"), ("id", "descending")],
    null_placement="at_start",
)
```

`null_placement` is a `SortOptions` control. ([Apache Arrow][3])

## I2.2 “Sort by expression” pattern (advanced)

`SortOptions` supports sort keys where the “field name” can be a **column name or expression** (this is most directly usable through `pc.sort_indices`). ([Apache Arrow][3])

```python
import pyarrow.compute as pc

idx = pc.sort_indices(
    t,
    sort_keys=[(pc.field("x") + pc.field("y"), "descending")],
)
t2 = t.take(idx)
```

This is the canonical way to sort by computed keys: sort indices → take. ([Apache Arrow][3])

---

# I3) Dictionary normalization: `Table.unify_dictionaries()` + `Table.combine_chunks()`

## I3.1 `Table.unify_dictionaries(...)`

Signature: `unify_dictionaries(self, memory_pool=None) -> Table` ([Apache Arrow][1])

Semantics:

* Returns an equivalent table where **all chunks of each dictionary-typed column share the same dictionary values**, with indices transposed accordingly. ([Apache Arrow][1])
* Non-dictionary columns are unchanged. ([Apache Arrow][1])

Why this matters:

* Many downstream systems/tools expect dictionary columns to have consistent dictionaries across chunks/batches; Arrow also exposes “unify dictionaries while writing IPC tables” because the file format can’t represent replacement dictionaries. ([Apache Arrow][4])

Minimal usage:

```python
t2 = t.unify_dictionaries()
```

([Apache Arrow][1])

## I3.2 `Table.combine_chunks(...)`

Signature: `combine_chunks(self, memory_pool=None) -> Table` ([Apache Arrow][1])

Semantics:

* Concatenates each column’s underlying chunks into **zero or one chunk**. ([Apache Arrow][1])
* To avoid buffer overflow, **binary columns may still end up as multiple chunks**, with maximum possible chunk lengths. ([Apache Arrow][1])

Practical sequencing rule:

* `unify_dictionaries()` normalizes dictionary values across chunks.
* `combine_chunks()` then “compacts” chunk layout (often useful before `sort_by`, `join`, or exporting). ([Apache Arrow][1])

---

# I4) Row selection + row filtering (often used with join/sort pipelines)

## I4.1 `Table.take(indices)` (gather by integer index)

`take(self, indices)` delegates to `pyarrow.compute.take()` and returns a same-schema table with gathered rows. ([Apache Arrow][1])

Performance/footgun: `TakeOptions(boundscheck=...)` exists; disabling bounds checks can be unsafe (“process may crash” if out-of-bounds). ([Apache Arrow][5])

## I4.2 `Table.filter(mask | Expression, null_selection_behavior=...)`

Signature: `filter(self, mask, null_selection_behavior='drop')` where mask is boolean array-like **or** a boolean `Expression`. ([Apache Arrow][1])

* If you pass an array-like mask, it uses `pyarrow.compute.filter()`; null-handling is controlled by `null_selection_behavior={'drop','emit_null'}`. ([Apache Arrow][1])
* If you pass an `Expression`, null_selection_behavior does nothing (the expression defines semantics). ([Apache Arrow][1])

---

# I5) Schema/column shaping that commonly brackets joins/sorts

These are the “table algebra” pieces that make join/sort pipelines ergonomic:

* `cast(target_schema, safe=True, options=None)` requires **names and order of fields match**; `safe` controls overflow/unsafe conversions. ([Apache Arrow][1])
* `select(columns)` returns a new table with metadata preserved. ([Apache Arrow][1])
* `rename_columns(names | mapping)` supports list or dict; mapping requires all referenced names exist (else `KeyError`). ([Apache Arrow][1])
* `append_column(field_, column)` / `add_column(i, ...)` are pure and return a new table; if `field_` is a string, type is deduced from data. ([Apache Arrow][1])

---

# I6) Validation: `Table.validate(full=False)`

`validate(full=False)` runs cheap checks by default; `full=True` runs thorough checks that can be **O(n)** and raises `ArrowInvalid` on failure. ([Apache Arrow][1])

[1]: https://arrow.apache.org/docs/python/generated/pyarrow.Table.html "pyarrow.Table — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/compute.html "Compute Functions — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.SortOptions.html "pyarrow.compute.SortOptions — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.ipc.IpcWriteOptions.html?utm_source=chatgpt.com "pyarrow.ipc.IpcWriteOptions — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.TakeOptions.html?utm_source=chatgpt.com "pyarrow.compute.TakeOptions — Apache Arrow v22.0.0"


## J) PyArrow compute advanced (expression model, UDF registration, option classes) — deep dive (PyArrow/Arrow v22)

### Mental model: **kernels** behind **named functions** in a **global function registry**

Arrow compute operations are “logical functions” that dispatch to one or more concrete **kernels** based on input types, and functions live in a **global FunctionRegistry** you can look up by name. ([Apache Arrow][1])
In Python, the standard surface is `pyarrow.compute` (imported as `pc`). ([Apache Arrow][2])

---

# J1) Expression model (fields + literals + scalar functions; filter & projection)

## J1.1 Two different “scalar” concepts: `pa.scalar` vs `pc.scalar`

You must keep these separate:

* `pyarrow.scalar(...)` creates a **`pyarrow.Scalar`** (a single value in Arrow’s *memory model*). ([Apache Arrow][3])
* `pyarrow.compute.scalar(...)` creates an **`Expression`** representing a scalar literal usable inside compute expressions and dataset filters/projections. ([Apache Arrow][4])

Minimal distinction:

```python
import pyarrow as pa
import pyarrow.compute as pc

s_mem  = pa.scalar(3)      # Scalar (material value)
s_expr = pc.scalar(3)      # Expression (literal in an expression tree)
```

([Apache Arrow][3])

---

## J1.2 Field references: `pc.field(...)` / `ds.field(...)` are *symbolic*

A field reference stores only the **field name**; type resolution happens only once the expression is bound/evaluated against a dataset/table schema. Nested references are supported by passing multiple names or a tuple of names (e.g. `("foo", "bar")` for `foo.bar`). ([Apache Arrow][5])

```python
import pyarrow.dataset as ds

f  = ds.field("x")               # x
nf = ds.field(("outer", "inner"))  # outer.inner
```

([Apache Arrow][5])

---

## J1.3 Building expressions: comparisons, boolean algebra, and scalar-function calls

Dataset expressions are built by combining field refs and literals; comparisons like `<`, `==`, `>=` are supported directly. ([Apache Arrow][6])

```python
import pyarrow.compute as pc
import pyarrow.dataset as ds

expr = (ds.field("x") >= 0) & (ds.field("y") == "ok")
```

([Apache Arrow][6])

### Important gotcha: **function-call arguments are expressions**

When you call compute functions in an expression context (projection/filter), arguments are expressions—not `pa.scalar()` values. The compute docs explicitly call out the difference and that `pc.scalar()` produces an expression literal. ([Apache Arrow][2])

Rule of thumb:

* comparisons often accept Python literals naturally,
* but for *explicit expression function calls*, be ready to wrap literals with `pc.scalar(...)`.

---

## J1.4 Filtering with expressions (Table + Dataset)

PyArrow explicitly supports filtering **both `Table` and `Dataset`** using a boolean `Expression`, typically built starting from `pc.field(...)`/`ds.field(...)`. ([Apache Arrow][2])

Example filter:

```python
import pyarrow.compute as pc

flt = (pc.field("nums") % pc.scalar(2)) == pc.scalar(0)  # “nums is even”
filtered = table.filter(flt)
```

Filtering-by-expression is a first-class pattern in the compute docs. ([Apache Arrow][2])

---

## J1.5 Projection (computed columns): **scalar expressions only**

A projection expression is evaluated row-by-row and must emit **one output value per input row**; it must depend only on the current row (no cross-row state). PyArrow calls this out as a limitation of projection expressions. ([Apache Arrow][2])

Acero’s `ProjectNodeOptions` makes the rule explicit: projection expressions must be **scalar expressions** composed of scalar literals, field refs, and **scalar functions** (elementwise functions). ([Apache Arrow][7])

A typical “computed column” shape:

```python
import pyarrow.compute as pc
import pyarrow.dataset as ds

proj_expr = pc.add(ds.field("a"), pc.scalar(1))  # scalar function + literal
# applied via Dataset/Scanner projection or an Acero Project node
```

Projection mechanics + expression evaluation timing are described in the compute guide. ([Apache Arrow][2])

---

## J1.6 Function registry access: `call_function`, `get_function`, `list_functions`

When you need dynamic dispatch (e.g., plugin kernels, UDF invocation by name), you use the registry API:

* `pc.call_function(name, args, options=None, memory_pool=None, length=None)` calls a named function looked up in the global registry. ([Apache Arrow][8])
* `pc.get_function(name)` gets a function by name from the global registry. ([Apache Arrow][9])
* `pc.list_functions()` returns all names in the global registry. ([Apache Arrow][10])

These are the primitives that make “UDFs + runtime-configured compute plans” possible.

---

# J2) UDF registration (scalar/vector/aggregate/tabular) + calling

### Status: **EXPERIMENTAL API**

PyArrow’s compute docs explicitly warn that UDF registration is experimental. ([Apache Arrow][11])

Also, UDFs are registered in the compute function registry and can then be called using their registered name (including from other Arrow C++-based bindings). ([Apache Arrow][11])

---

## J2.1 `UdfContext`: per-invocation state + memory pool

Every UDF receives a `UdfContext` as its first argument. It provides:

* `batch_length`: common length of inputs (and “actual length” even if inputs are scalars encoding a constant column) ([Apache Arrow][12])
* `memory_pool`: the memory pool supplied by the caller; docs explicitly recommend using it for any Arrow calls that accept `memory_pool`. ([Apache Arrow][12])

---

## J2.2 Scalar UDFs: `pc.register_scalar_function(...)`

Signature:
`register_scalar_function(func, function_name, function_doc, in_types, out_type, func_registry=None)` ([Apache Arrow][13])

Semantics (scalar UDF = elementwise, stateless):

* computed row-by-row; output length matches input length
* **scalar functions are the only functions allowed in query-engine expressions** ([Apache Arrow][13])

Contract for `func`:

* first arg is `UdfContext`
* then arguments per `in_types`
* must return an `Array` or `Scalar` matching `out_type`
* must return a `Scalar` only if all inputs are scalars; otherwise return an `Array`
* varargs supported via `*args` and the final `in_type` becomes the vararg type ([Apache Arrow][13])

`function_doc` and types:

* `function_doc` is a dict with keys `"summary"` and `"description"` ([Apache Arrow][13])
* `in_types` is a dict `{arg_name: DataType}` and determines arity + generated docs ([Apache Arrow][13])
* `function_name` must be unique in the registry ([Apache Arrow][13])

### Minimal implementation (directly aligned with the docs)

```python
import pyarrow as pa
import pyarrow.compute as pc

func_doc = {"summary": "simple udf", "description": "add a constant to a scalar"}

def add_constant(ctx, array):
    # use ctx.memory_pool for allocations in downstream arrow calls
    return pc.add(array, 1, memory_pool=ctx.memory_pool)

pc.register_scalar_function(
    add_constant,
    function_name="py_add_func",
    function_doc=func_doc,
    in_types={"array": pa.int64()},
    out_type=pa.int64(),
)
```

([Apache Arrow][13])

### Calling a registered UDF

The docs show the canonical invocation flow using `get_function` + `call_function`. ([Apache Arrow][13])

```python
f = pc.get_function("py_add_func")
out = pc.call_function("py_add_func", [pa.array([20])])
```

([Apache Arrow][13])

---

## J2.3 Vector UDFs: `pc.register_vector_function(...)`

Vector UDFs are described as “vector operations on arrays” and are intended for cases that don’t fit scalar or aggregate UDF categories. ([Apache Arrow][14])

The signature matches the scalar registration shape and uses `UdfContext` + `in_types` + `out_type`. ([Apache Arrow][14])

Key contract mirrors scalar UDFs: return `Scalar` if all inputs are scalar, else return `Array`. ([Apache Arrow][14])

---

## J2.4 Aggregate UDFs: `pc.register_aggregate_function(...)`

Aggregate UDFs here are **non-decomposable** aggregations: they operate on “the whole data that it is aggregating” and cannot be split into consume/merge/finalize steps. ([Apache Arrow][15])

Signature is analogous:
`register_aggregate_function(func, function_name, function_doc, in_types, out_type, func_registry=None)` ([Apache Arrow][15])

Contract differences vs scalar/vector:

* returns a **Scalar** matching `out_type` ([Apache Arrow][15])
* docs note a current limit: any input column size cannot exceed 2GB for a single segment. ([Apache Arrow][15])

---

## J2.5 Tabular UDFs (table-valued): `pc.register_tabular_function(...)`

Tabular UDFs are table-valued generators:

* accept only `UdfContext`
* `in_types` must be empty
* `out_type` is a **schema**
* function returns a callable that yields `StructArray` chunks; an empty array signals end. ([Apache Arrow][16])

This is the advanced primitive for “produce rows” rather than “transform existing rows.”

---

# J3) Option classes (`*Options`) — strongly-typed kernel configuration

### Mental model: options objects are *serializable config blobs* passed into kernels

Many compute functions accept an `options=` parameter, which is an instance of a specific `*Options` class. Options classes are real objects with `serialize()` / `deserialize()` methods, so you can persist configuration or ship it around in higher-level planning layers. ([Apache Arrow][17])

Also, `pc.call_function(..., options=...)` is the low-level entrypoint to pass options into named kernel invocations. ([Apache Arrow][8])

---

## J3.1 `CastOptions` (safe/unsafe casting, overflow/truncation policy)

`CastOptions(target_type=..., allow_int_overflow=..., allow_float_truncate=..., allow_invalid_utf8=..., ...)` controls exactly what “unsafe” behavior is permitted during casting. ([Apache Arrow][17])

Notable helpers:

* `CastOptions.safe(target_type=...)` and `CastOptions.unsafe(target_type=...)` constructors ([Apache Arrow][17])

```python
import pyarrow as pa
import pyarrow.compute as pc

opts = pc.CastOptions(target_type=pa.int32(), allow_int_overflow=False)
out = pc.cast(pa.array([1, 2, 3], type=pa.int64()), options=opts)
```

([Apache Arrow][17])

---

## J3.2 `ScalarAggregateOptions` (null policy + minimum count)

Scalar aggregations (like `product`) commonly expose `skip_nulls` and `min_count`; the docs note this behavior is governed by `ScalarAggregateOptions`. ([Apache Arrow][18])

```python
import pyarrow.compute as pc

opts = pc.ScalarAggregateOptions(skip_nulls=True, min_count=3)
val = pc.product(pa.array([1, None, 2, 3]), options=opts)
```

([Apache Arrow][18])

---

## J3.3 `TakeOptions` (bounds checking vs raw speed)

`TakeOptions(boundscheck=True|False)` controls whether Arrow validates indices. Disabling bounds checks can crash the process if indices are out of bounds. ([Apache Arrow][19])

```python
import pyarrow.compute as pc
opts = pc.TakeOptions(boundscheck=True)
out = pc.take(table, indices, options=opts)
```

([Apache Arrow][19])

---

## J3.4 `SortOptions` (expression-based sort keys + null placement)

`SortOptions(sort_keys=[(name_or_expr, order), ...], null_placement=...)` supports:

* sort keys as **column names or expressions**
* per-key order `ascending|descending`
* null placement `at_start|at_end` ([Apache Arrow][20])

```python
import pyarrow.compute as pc

idx = pc.sort_indices(
    table,
    options=pc.SortOptions(
        sort_keys=[("y", "ascending"), (pc.field("x") + pc.scalar(1), "descending")],
        null_placement="at_end",
    ),
)
sorted_table = table.take(idx)
```

Expression sort keys are explicitly supported in `SortOptions`. ([Apache Arrow][20])

---

If you want next: the most natural continuation after J is to deep-dive **K) Acero + Substrait** (how expressions and registered functions become executable plans), because that’s where the “expression model + UDFs + options objects” converge into an actual query engine.

[1]: https://arrow.apache.org/docs/cpp/compute.html?utm_source=chatgpt.com "Compute Functions — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/compute.html "Compute Functions — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/python/generated/pyarrow.scalar.html?utm_source=chatgpt.com "pyarrow.scalar — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/_modules/pyarrow/compute.html?utm_source=chatgpt.com "pyarrow.compute — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.field.html?utm_source=chatgpt.com "pyarrow.dataset.field — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.dataset.Expression.html?utm_source=chatgpt.com "pyarrow.dataset.Expression — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.acero.ProjectNodeOptions.html?utm_source=chatgpt.com "pyarrow.acero.ProjectNodeOptions — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/dev/python/generated/pyarrow.compute.call_function.html "pyarrow.compute.call_function — Apache Arrow v23.0.0.dev234"
[9]: https://arrow.apache.org/docs/dev/python/generated/pyarrow.compute.get_function.html "pyarrow.compute.get_function — Apache Arrow v23.0.0.dev234"
[10]: https://arrow.apache.org/docs/dev/python/generated/pyarrow.compute.list_functions.html "pyarrow.compute.list_functions — Apache Arrow v23.0.0.dev234"
[11]: https://arrow.apache.org/docs/python/compute.html?utm_source=chatgpt.com "Compute Functions — Apache Arrow v22.0.0"
[12]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.UdfContext.html "pyarrow.compute.UdfContext — Apache Arrow v22.0.0"
[13]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.register_scalar_function.html "pyarrow.compute.register_scalar_function — Apache Arrow v22.0.0"
[14]: https://arrow.apache.org/docs/dev/python/generated/pyarrow.compute.register_vector_function.html "pyarrow.compute.register_vector_function — Apache Arrow v23.0.0.dev234"
[15]: https://arrow.apache.org/docs/dev/python/generated/pyarrow.compute.register_aggregate_function.html "pyarrow.compute.register_aggregate_function — Apache Arrow v23.0.0.dev234"
[16]: https://arrow.apache.org/docs/dev/python/generated/pyarrow.compute.register_tabular_function.html "pyarrow.compute.register_tabular_function — Apache Arrow v23.0.0.dev234"
[17]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.CastOptions.html "pyarrow.compute.CastOptions — Apache Arrow v22.0.0"
[18]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.ScalarAggregateOptions.html?utm_source=chatgpt.com "pyarrow.compute.ScalarAggregateOptions - Apache Arrow"
[19]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.TakeOptions.html "pyarrow.compute.TakeOptions — Apache Arrow v22.0.0"
[20]: https://arrow.apache.org/docs/python/generated/pyarrow.compute.SortOptions.html "pyarrow.compute.SortOptions — Apache Arrow v22.0.0"
