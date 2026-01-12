## ArrowDSL Shared Routine Consolidation Plan

### Scope 1: Canonical Row/Reader/Plan Builders

### Description
Centralize row-to-Arrow construction (rows → RecordBatchReader → Plan) so extract, normalize,
and CPG pipelines share one canonical path for schema-aligned ingestion. This replaces the
current `extract.tables` helpers with ArrowDSL-native utilities.

### Code patterns
```python
# src/arrowdsl/plan/rows.py

def record_batches_from_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    schema: SchemaLike,
    batch_size: int = 4096,
) -> Iterator[pa.RecordBatch]:
    buffer: list[Mapping[str, object]] = []
    for row in rows:
        buffer.append(row)
        if len(buffer) >= batch_size:
            yield pa.RecordBatch.from_pylist(buffer, schema=schema)
            buffer.clear()
    if buffer:
        yield pa.RecordBatch.from_pylist(buffer, schema=schema)


def plan_from_rows(
    rows: Iterable[Mapping[str, object]],
    *,
    schema: SchemaLike,
    batch_size: int = 4096,
    label: str = "",
) -> Plan:
    reader = pa.RecordBatchReader.from_batches(
        schema,
        record_batches_from_rows(rows, schema=schema, batch_size=batch_size),
    )
    return Plan.from_reader(reader, label=label)
```

### Target files
- `src/arrowdsl/plan/rows.py` (new)
- `src/extract/tables.py`
- `src/extract/ast_extract.py`
- `src/extract/cst_extract.py`
- `src/extract/bytecode_extract.py`
- `src/extract/symtable_extract.py`
- `src/extract/scip_extract.py`
- `src/extract/repo_scan.py`
- `src/extract/tree_sitter_extract.py`

### Implementation checklist
- [ ] Add canonical row/reader/plan helpers under ArrowDSL.
- [ ] Replace `extract.tables` usages with ArrowDSL helpers.
- [ ] Remove redundant row/reader helpers from `src/extract/tables.py`.

---

### Scope 2: Typed Scalar/Null Compute Expressions

### Description
Standardize typed scalar and null expressions for plan-lane projections to avoid repeated
`pc.cast(pc.scalar(None), ...)` boilerplate and ensure consistent `safe=False` casting.

### Code patterns
```python
# src/arrowdsl/compute/scalars.py

def null_expr(dtype: DataTypeLike) -> ComputeExpression:
    return ensure_expression(pc.cast(pc.scalar(None), dtype, safe=False))


def scalar_expr(value: object, *, dtype: DataTypeLike) -> ComputeExpression:
    return ensure_expression(pc.cast(pc.scalar(value), dtype, safe=False))
```

### Target files
- `src/arrowdsl/compute/scalars.py` (new)
- `src/cpg/emit_props.py`
- `src/cpg/relations.py`
- `src/cpg/quality.py`
- `src/cpg/catalog.py`
- `src/normalize/bytecode_cfg.py`
- `src/normalize/bytecode_dfg.py`
- `src/normalize/types.py`

### Implementation checklist
- [ ] Add `null_expr`/`scalar_expr` helpers in ArrowDSL.
- [ ] Replace repeated scalar cast blocks in CPG/normalize.
- [ ] Remove duplicated inline scalar/null patterns where possible.

---

### Scope 3: Canonical String/List Normalization Helpers

### Description
Consolidate string list normalization (e.g., `Sequence[object] -> list[str | None]`) so
CST/SCIP parsing uses a single helper and keeps Arrow input normalization consistent.

### Code patterns
```python
# src/arrowdsl/compute/transforms.py

def normalize_string_items(items: Sequence[object]) -> list[str | None]:
    out: list[str | None] = []
    for item in items:
        if item is None:
            out.append(None)
        elif isinstance(item, str):
            out.append(item)
        else:
            out.append(str(item))
    return out
```

### Target files
- `src/arrowdsl/compute/transforms.py`
- `src/extract/cst_extract.py`
- `src/extract/scip_extract.py`

### Implementation checklist
- [ ] Add `normalize_string_items` to ArrowDSL transforms.
- [ ] Replace `_normalize_string_items` in CST/SCIP extractors.
- [ ] Remove local normalization helpers after migration.

---

### Scope 4: Schema-Driven Projection Helpers

### Description
Provide a canonical helper to project/align plans to a schema while filling missing
columns with typed null expressions, replacing repeated `_ensure_output_columns` patterns
in normalize.

### Code patterns
```python
# src/arrowdsl/plan_helpers.py

def project_to_schema(
    plan: Plan,
    *,
    schema: SchemaLike,
    ctx: ExecutionContext,
) -> Plan:
    available = set(plan.schema(ctx=ctx).names)
    exprs: list[ComputeExpression] = []
    names: list[str] = []
    for field in schema:
        names.append(field.name)
        exprs.append(column_or_null_expr(field.name, field.type, available=available, cast=True))
    return plan.project(exprs, names, ctx=ctx)
```

### Target files
- `src/arrowdsl/plan_helpers.py`
- `src/normalize/bytecode_cfg.py`
- `src/normalize/bytecode_dfg.py`
- `src/normalize/types.py`
- `src/cpg/catalog.py` (optional for derived plan normalization)

### Implementation checklist
- [ ] Add `project_to_schema` helper to ArrowDSL plan helpers.
- [ ] Replace local `_ensure_output_columns`/manual projections in normalize.
- [ ] Remove redundant helpers after migration.

---

### Scope 5: Schema-Aligned Table Builders

### Description
Expand canonical table creation helpers to cover the repeated `pa.Table.from_arrays` and
empty-table patterns with schema metadata preserved. Use typed null filling based on
schema field types.

### Code patterns
```python
# src/arrowdsl/schema/tables.py

def table_from_arrays(
    schema: SchemaLike,
    *,
    columns: Mapping[str, ArrayLike],
    num_rows: int,
) -> TableLike:
    arrays = [columns.get(field.name, pa.nulls(num_rows, type=field.type)) for field in schema]
    return pa.Table.from_arrays(arrays, schema=schema)
```

### Target files
- `src/arrowdsl/schema/tables.py` (new) or extend `src/arrowdsl/schema/columns.py`
- `src/normalize/span_pipeline.py`
- `src/normalize/diagnostics.py`
- `src/extract/scip_extract.py`
- `src/cpg/builders.py`

### Implementation checklist
- [ ] Add schema-aligned `table_from_arrays` helper to ArrowDSL.
- [ ] Replace local `pa.Table.from_arrays` usage with the helper.
- [ ] Remove ad-hoc empty table builders where redundant.

---

### Scope 6: ListView/Map/Struct Array Construction Helpers

### Description
Provide higher-level constructors for list_view arrays, map arrays, and struct arrays using
PyArrow’s advanced APIs (list_view offsets/sizes, map types, struct inference). This reduces
manual buffer handling and ensures consistent dtype selection.

### Code patterns
```python
# src/arrowdsl/schema/arrays.py

def list_view_array_from_lists(
    values: Sequence[Sequence[object] | None],
    *,
    value_type: DataTypeLike,
    large: bool = True,
) -> ArrayLike:
    list_type = pa.large_list_view(value_type) if large else pa.list_view(value_type)
    return pa.array(values, type=list_type)


def map_array_from_pairs(
    values: Sequence[Sequence[tuple[object, object]] | None],
    *,
    key_type: DataTypeLike,
    item_type: DataTypeLike,
) -> ArrayLike:
    return pa.array(values, type=pa.map_(key_type, item_type))


def struct_array_from_dicts(
    values: Sequence[Mapping[str, object]],
    *,
    struct_type: DataTypeLike | None = None,
) -> ArrayLike:
    return pa.array(values, type=struct_type)
```

### Target files
- `src/arrowdsl/schema/arrays.py`
- `src/extract/cst_extract.py` (qname list_view + struct)
- `src/extract/scip_extract.py` (signature occurrences / documentation structs)
- `src/extract/runtime_inspect_extract.py` (map field construction)

### Implementation checklist
- [ ] Add list_view/map/struct array constructors to ArrowDSL.
- [ ] Replace manual list_view/map handling in extractors.
- [ ] Ensure dtype selection is centralized in ArrowDSL helpers.

---

### Scope 7: Plan/Table Catalog Abstractions

### Description
Generalize the plan/table catalog pattern from CPG into ArrowDSL to standardize plan source
normalization (Plan/Table/DatasetSource) and cached derivations across pipelines.

### Code patterns
```python
# src/arrowdsl/plan/catalog.py

@dataclass
class PlanCatalog:
    entries: dict[str, PlanSource] = field(default_factory=dict)

    def resolve(self, name: str, *, ctx: ExecutionContext) -> Plan | None:
        value = self.entries.get(name)
        if value is None:
            return None
        return plan_from_source(value, ctx=ctx, label=name)
```

### Target files
- `src/arrowdsl/plan/catalog.py` (new)
- `src/cpg/catalog.py`
- `src/normalize/*` (optional: normalize plans that derive from source catalogs)

### Implementation checklist
- [ ] Introduce ArrowDSL plan/table catalog helpers.
- [ ] Migrate `cpg.catalog` to reuse ArrowDSL catalog utilities.
- [ ] Adopt in normalize where plan caching is needed.

---

### Scope 8: HashSpec Registry Utilities

### Description
Create a small HashSpec registry/factory to reduce boilerplate in CPG/extract/normalize
hash spec modules and centralize defaults like `null_sentinel` and `as_string`.

### Code patterns
```python
# src/arrowdsl/core/ids_registry.py

def hash_spec(
    *,
    prefix: str,
    cols: Sequence[str],
    out_col: str | None = None,
    null_sentinel: str | None = None,
) -> HashSpec:
    return HashSpec(
        prefix=prefix,
        cols=tuple(cols),
        as_string=True,
        out_col=out_col,
        null_sentinel=null_sentinel,
    )
```

### Target files
- `src/arrowdsl/core/ids_registry.py` (new)
- `src/extract/hash_specs.py`
- `src/normalize/hash_specs.py`
- `src/cpg/hash_specs.py`

### Implementation checklist
- [ ] Add a HashSpec factory/registry in ArrowDSL.
- [ ] Update hash spec modules to use the factory for consistency.
- [ ] Remove duplicated HashSpec boilerplate.

---

### Scope 9: Schema Metadata Mutation Helpers

### Description
Centralize safe schema/field metadata updates using Arrow’s immutable schema rules, ensuring
field metadata edits always round-trip through schema + cast.

### Code patterns
```python
# src/arrowdsl/schema/metadata.py

def update_field_metadata(
    table: TableLike,
    *,
    updates: Mapping[str, Mapping[bytes, bytes]],
) -> TableLike:
    fields = []
    for field in table.schema:
        meta = updates.get(field.name)
        fields.append(field.with_metadata(meta) if meta is not None else field)
    schema = pa.schema(fields, metadata=table.schema.metadata)
    return table.cast(schema)
```

### Target files
- `src/arrowdsl/schema/metadata.py`
- `src/arrowdsl/schema/unify.py`
- `src/normalize/schema_infer.py` (optional for metadata repairs)

### Implementation checklist
- [ ] Add metadata update helpers that follow Arrow immutability rules.
- [ ] Use the helper where metadata updates are currently manual or ad-hoc.
- [ ] Ensure schema equality checks include metadata where required.

---

### Scope 10: Struct Field Flattening Helpers

### Description
Expose a dedicated struct-flattening helper in ArrowDSL (wrapping `Field.flatten()`) to avoid
ad-hoc flatten logic in extractors and to standardize parent-name prefixing.

### Code patterns
```python
# src/arrowdsl/schema/structs.py

def flatten_struct_field(field: pa.Field) -> list[pa.Field]:
    return list(field.flatten())
```

### Target files
- `src/arrowdsl/schema/structs.py` (new)
- `src/extract/cst_extract.py`
- `src/extract/scip_extract.py`

### Implementation checklist
- [ ] Add `flatten_struct_field` helper under ArrowDSL schema utilities.
- [ ] Replace local flatten usages in extractors.
- [ ] Remove redundant flatten helpers after migration.
