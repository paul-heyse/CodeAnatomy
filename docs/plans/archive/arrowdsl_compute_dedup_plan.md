## ArrowDSL Compute Dedup Plan

### Goals
- Consolidate overlapping compute, schema, and spec IO helpers into a single modular ArrowDSL architecture.
- Replace duplicated plan-lane/kernel-lane helpers with shared, typed primitives.
- Leverage advanced PyArrow functionality (joins, sorting, IPC options, datasets) to reduce custom code.
- Preserve functional behavior while simplifying call sites.

### Constraints
- Keep plan lane (Acero) and kernel lane responsibilities separated.
- Preserve schema metadata and ordering semantics.
- Maintain strict typing and Ruff compliance; no suppressions.
- No relative imports; keep modules fully typed and pyright clean.

---

### Scope 1: Expression Macro Consolidation
**Description**
Unify expression helpers across `compute/exprs.py`, `compute/expr_specs.py`,
`compute/scalars.py`, and `compute/transforms.py` into a single canonical macro surface.
This removes duplicate implementations of trim, coalesce, scalar/null expressions,
expr-context normalization, and materializers.

**Code patterns**
```python
# src/arrowdsl/compute/macros.py
@dataclass(frozen=True)
class ExprMacro:
    expr: ComputeExpression
    materialize: Callable[[TableLike], ArrayLike]
    scalar_safe: bool


def trim_expr(col: str) -> ExprMacro:
    expr = ensure_expression(pc.call_function("utf8_trim_whitespace", [pc.field(col)]))
    return ExprMacro(expr, lambda table: pc.call_function("utf8_trim_whitespace", [table[col]]), True)
```

**Target files**
- Add: `src/arrowdsl/compute/macros.py`
- Update: `src/arrowdsl/compute/exprs.py`
- Update: `src/arrowdsl/compute/expr_specs.py`
- Update: `src/arrowdsl/compute/scalars.py`
- Update: `src/arrowdsl/compute/transforms.py`

**Implementation checklist**
- [x] Consolidate trim/coalesce/scalar helpers into a single macro module.
- [x] Replace ad-hoc materializers with shared macro-backed implementations.
- [x] Preserve plan-lane scalar safety checks in one place.

**Status**
Completed.

---

### Scope 2: Hashing and ID Helper Dedup
**Description**
Unify hash expression builders in `core/ids.py` and compute expression specs so the
hash pipeline is defined once and used by both plan and kernel lanes.

**Code patterns**
```python
# src/arrowdsl/core/ids.py

def hash_expr(spec: HashSpec, *, available: Sequence[str] | None = None) -> ComputeExpression:
    return hash_expression(spec, available=available)


def hash_array(table: TableLike, *, spec: HashSpec) -> ArrayLike:
    return hash_column_values(table, spec=spec)
```

**Target files**
- Update: `src/arrowdsl/core/ids.py`
- Update: `src/arrowdsl/compute/expr_specs.py`

**Implementation checklist**
- [x] Replace `HashFromExprsSpec` usage with core hash helpers.
- [x] Keep masked hash behavior centralized in `core/ids.py`.
- [x] Remove duplicate hash expression assembly in compute specs.

**Status**
Completed.

---

### Scope 3: Join Spec and Join Execution Consolidation
**Description**
Merge `plan/join_specs.py` and `plan/joins.py` into a single join surface that handles
both plan and kernel execution. Use PyArrow `Table.join` for all table joins and add
optional `Table.join_asof` for interval/time alignment when applicable.

**Code patterns**
```python
# src/arrowdsl/plan/joins.py

def join_any(
    left: TableLike | Plan,
    right: TableLike | Plan,
    *,
    spec: JoinSpec,
    ctx: ExecutionContext | None = None,
) -> TableLike | Plan:
    if isinstance(left, Plan) or isinstance(right, Plan):
        return Plan.table_source(left).join(Plan.table_source(right), spec=spec, ctx=ctx)
    return left.join(
        right,
        keys=list(spec.left_keys),
        right_keys=list(spec.right_keys),
        join_type=spec.join_type,
        use_threads=True,
    )
```

**Target files**
- Update: `src/arrowdsl/plan/join_specs.py`
- Update: `src/arrowdsl/plan/joins.py`
- Update: `src/arrowdsl/compute/kernels.py`

**Implementation checklist**
- [x] Collapse join spec builders into one canonical API.
- [x] Route table joins through `Table.join` (and `join_asof` when configured).
- [x] Remove redundant join helpers in kernel lane.

**Status**
Completed.

---

### Scope 4: Nested Array Builder Unification
**Description**
Consolidate nested array builders across `schema/arrays.py`, `schema/nested_builders.py`,
`schema/nested.py`, and `plan/rows.py` into a single nested builder module that uses
PyArrow list_view/map/union/dictionary types consistently.

**Code patterns**
```python
# src/arrowdsl/schema/nested_builders.py

def array_from_values(values: Sequence[object | None], field: FieldLike) -> ArrayLike:
    dtype = field.type
    if patypes.is_struct(dtype):
        return struct_array_from_dicts(values, struct_type=dtype)
    if patypes.is_list_view(dtype) or patypes.is_large_list_view(dtype):
        return list_view_array_from_lists(values, value_type=dtype.value_type, large=True)
    if patypes.is_map(dtype):
        return map_array_from_pairs(values, key_type=dtype.key_type, item_type=dtype.item_type)
    if patypes.is_dictionary(dtype):
        return dictionary_array_from_values(values, dictionary_type=dtype)
    if patypes.is_union(dtype):
        return union_array_from_values(values, union_type=dtype)
    return pa.array(values, type=dtype)
```

**Target files**
- Update: `src/arrowdsl/schema/arrays.py`
- Update: `src/arrowdsl/schema/nested_builders.py`
- Update: `src/arrowdsl/schema/nested.py`
- Update: `src/arrowdsl/plan/rows.py`

**Implementation checklist**
- [x] Create a single `array_from_values` entrypoint for nested types.
- [x] Move list/struct accumulator logic to one module.
- [x] Remove duplicate row-to-array conversion logic in plan helpers.

**Status**
Completed.

---

### Scope 5: Schema Alignment and Encoding Policy Consolidation
**Description**
Unify schema alignment (`align_to_schema`, `SchemaTransform`, `projection_for_schema`) and
encoding policy application (`encoding_policy_from_spec`, `encode_plan`, `encode_table`) into
one canonical alignment/encoding service for both plan and table workflows.

**Code patterns**
```python
# src/arrowdsl/schema/alignment.py

def align_any(table: TableLike, *, schema: SchemaLike, ctx: ExecutionContext) -> TableLike:
    aligned, _ = align_to_schema(table, schema=schema, safe_cast=ctx.safe_cast)
    return aligned


def encode_any(value: TableLike | Plan, *, policy: EncodingPolicy, ctx: ExecutionContext):
    if isinstance(value, Plan):
        cols = tuple(spec.column for spec in policy.specs)
        return value.project(encoding_projection(cols, available=value.schema(ctx=ctx).names)[0])
    return encode_columns(value, specs=policy.specs)
```

**Target files**
- Add: `src/arrowdsl/schema/alignment.py`
- Update: `src/arrowdsl/schema/schema.py`
- Update: `src/arrowdsl/schema/encoding.py`
- Update: `src/arrowdsl/plan_helpers.py`

**Implementation checklist**
- [x] Centralize alignment logic and reuse it in plan/table callers.
- [x] Route encoding via one policy application helper.
- [x] Use `Table.unify_dictionaries` and `Table.combine_chunks` consistently.

**Status**
Completed.

---

### Scope 6: Spec IO and Adapter Consolidation
**Description**
Collapse `spec/adapters.py` into `SpecTableCodec` and `spec/io.py`, using advanced
IPC options (IpcReadOptions/IpcWriteOptions) for a single IO path.

**Code patterns**
```python
# src/arrowdsl/spec/io.py

def write_spec_table(
    path: Path | str,
    table: pa.Table,
    *,
    options: ipc.IpcWriteOptions | None = None,
) -> None:
    with ipc.new_file(str(path), table.schema, options=options) as writer:
        writer.write_table(table)
```

**Target files**
- Update: `src/arrowdsl/spec/io.py`
- Update: `src/arrowdsl/spec/tables/base.py`
- Delete: `src/arrowdsl/spec/adapters.py`

**Implementation checklist**
- [x] Remove `spec/adapters.py` and update call sites to use `SpecTableCodec`.
- [x] Add IPC options to read/write helpers.
- [x] Standardize JSON and IPC IO in the codec layer.

**Status**
Completed.

---

### Scope 7: Row Ingestion and Streaming Consolidation
**Description**
Unify row ingestion helpers in `plan/rows.py` with dataset/reader sources in
`plan/source.py`. Use `RecordBatchReader` and `Scanner.to_reader` for streaming
instead of custom batching paths.

**Code patterns**
```python
# src/arrowdsl/plan/source.py

def plan_from_rows(rows: Iterable[Mapping[str, object]], *, schema: SchemaLike, label: str = "") -> Plan:
    reader = pa.RecordBatchReader.from_batches(schema, record_batches_from_rows(rows, schema=schema))
    return Plan.from_reader(reader, label=label)
```

**Target files**
- Update: `src/arrowdsl/plan/rows.py`
- Update: `src/arrowdsl/plan/source.py`

**Implementation checklist**
- [x] Make `plan_from_rows` the single row-ingestion path.
- [x] Route streaming through `RecordBatchReader` consistently.
- [x] Remove duplicate row-to-table helpers once call sites migrate.

**Status**
Completed.

---

### Scope 8: Wrapper Module Cleanup
**Description**
Remove trivial wrapper modules in ArrowDSL (`schema/columns.py`, `schema/tables.py`) and
update imports to canonical builder modules to eliminate redundancy.

**Code patterns**
```python
# Replace wrappers
from arrowdsl.schema.builders import table_from_schema
```

**Target files**
- Delete: `src/arrowdsl/schema/columns.py`
- Delete: `src/arrowdsl/schema/tables.py`
- Update: call sites importing these wrappers

**Implementation checklist**
- [x] Remove wrapper modules and update imports to `schema/builders.py`.
- [x] Ensure no public API breakage by updating ArrowDSL exports.
- [x] Validate all call sites compile under pyright strict.

**Status**
Completed.
