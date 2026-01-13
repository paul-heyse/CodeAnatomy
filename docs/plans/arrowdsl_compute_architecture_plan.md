## ArrowDSL Compute Architecture Consolidation Plan

### Goals
- Consolidate Arrow compute helpers into a unified, modular architecture.
- Preserve downstream flexibility while reducing duplicate helper patterns.
- Expand ArrowDSL to use advanced PyArrow dataset, scanner, and fragment APIs.
- Introduce shared spec/context factories to standardize compute and scan behavior.

### Constraints
- Maintain strict typing and Ruff compliance (no suppressions).
- Preserve existing schemas and contracts unless explicitly updated.
- Keep plan-lane operations in Acero and kernel-lane operations in Arrow compute.

---

### Scope 1: Canonical table builders and column helpers
**Description**
Unify table construction utilities into a single canonical module and re-export
from existing modules for compatibility. This consolidates empty-table creation,
schema-aligned table creation, and typed-null columns.

**Code patterns**
```python
# src/arrowdsl/schema/builders.py
def empty_table(schema: SchemaLike) -> TableLike:
    return pa.Table.from_arrays([pa.array([], type=f.type) for f in schema], schema=schema)


def table_from_schema(
    schema: SchemaLike,
    *,
    columns: Mapping[str, ArrayLike],
    num_rows: int,
) -> TableLike:
    arrays = [columns.get(f.name, pa.nulls(num_rows, type=f.type)) for f in schema]
    return pa.Table.from_arrays(arrays, schema=schema)
```

**Target files**
- Add: `src/arrowdsl/schema/builders.py`
- Update: `src/arrowdsl/schema/schema.py`
- Update: `src/arrowdsl/schema/tables.py`
- Update: `src/arrowdsl/schema/columns.py`

**Implementation checklist**
- [x] Move or re-export `empty_table`, `table_from_schema`, `table_from_arrays`,
      and `column_or_null` into `schema/builders.py`.
- [x] Update imports in ArrowDSL to reference the canonical builders.
- [x] Leave compatibility re-exports in old modules to avoid downstream churn.

**Status**
Completed.

---

### Scope 2: Unified ExprSpec surface
**Description**
Centralize expression specs (Const/Field/Coalesce/Compute/Trim/Hash) into one
module so plan and kernel lanes share a single expression definition surface.

**Code patterns**
```python
# src/arrowdsl/compute/exprs.py
@dataclass(frozen=True)
class FieldExpr(ExprSpec):
    name: str
    def to_expression(self) -> ComputeExpression:
        return pc.field(self.name)
    def materialize(self, table: TableLike) -> ArrayLike:
        return table[self.name]
```

**Target files**
- Add: `src/arrowdsl/compute/exprs.py`
- Update: `src/arrowdsl/compute/expr_specs.py`
- Update: `src/arrowdsl/schema/arrays.py`
- Update: `src/arrowdsl/compute/__init__.py`

**Implementation checklist**
- [x] Move `ConstExpr`, `FieldExpr`, `CoalesceExpr` to the canonical module.
- [x] Re-export from `schema/arrays.py` for compatibility.
- [x] Update downstream imports to use the canonical expression module.

**Status**
Completed.

---

### Scope 3: Compute registry for UDFs and transforms
**Description**
Create a `ComputeRegistry` that owns UDF registration and transform creation.
This removes duplicated caching logic and keeps UDF names stable.

**Code patterns**
```python
# src/arrowdsl/compute/registry.py
@dataclass(frozen=True)
class UdfSpec:
    name: str
    inputs: Mapping[str, DataTypeLike]
    output: DataTypeLike
    fn: Callable[..., ValuesLike]


class ComputeRegistry:
    def ensure(self, spec: UdfSpec) -> str:
        try:
            pc.get_function(spec.name)
        except KeyError:
            pc.register_scalar_function(spec.fn, spec.name, {}, spec.inputs, spec.output)
        return spec.name
```

**Target files**
- Add: `src/arrowdsl/compute/registry.py`
- Update: `src/arrowdsl/compute/udfs.py`
- Update: `src/arrowdsl/compute/transforms.py`

**Implementation checklist**
- [x] Implement `ComputeRegistry` with UDF registration + caching.
- [x] Refactor `ensure_expr_context_udf` and `ensure_json_udf` to use the registry.
- [x] Expose transform helpers that build expressions from registry-backed UDFs.

**Status**
Completed.

---

### Scope 4: ScanSpec + dataset factory integration
**Description**
Add a deterministic dataset discovery layer using `FileSystemDatasetFactory`,
plus a `ScanSpec` factory that combines discovery + query + runtime scan options.
This unlocks advanced schema inspection and partition control.

**Code patterns**
```python
# src/arrowdsl/plan/scan_specs.py
@dataclass(frozen=True)
class DatasetFactorySpec:
    root: str
    format: ds.FileFormat
    partition_base_dir: str | None = None
    exclude_invalid_files: bool = False
    promote_options: str = "permissive"

    def build(self, *, schema: SchemaLike | None = None) -> ds.Dataset:
        factory = ds.FileSystemDatasetFactory(
            filesystem=fs.LocalFileSystem(),
            paths_or_selector=fs.FileSelector(self.root, recursive=True),
            format=self.format,
            options=ds.FileSystemFactoryOptions(
                partition_base_dir=self.partition_base_dir,
                exclude_invalid_files=self.exclude_invalid_files,
            ),
        )
        inspected = factory.inspect(promote_options=self.promote_options)
        return factory.finish(schema=schema or inspected)
```

**Target files**
- Add: `src/arrowdsl/plan/scan_specs.py`
- Update: `src/arrowdsl/plan/query.py`
- Update: `src/arrowdsl/plan/source.py`
- Update: `src/arrowdsl/core/context.py`

**Implementation checklist**
- [x] Introduce `DatasetFactorySpec` and `ScanSpec` to build datasets and scans.
- [x] Allow `open_dataset` to accept factory specs and inspected schemas.
- [x] Thread `ScanProfile.scanner_kwargs()` into scan creation paths.

**Status**
Completed.

---

### Scope 5: Fragment and row-group aware scanning + telemetry
**Description**
Extend scan helpers to expose fragment and row-group stats, and allow optional
row-group split/subset to maximize pushdown in large Parquet datasets.

**Code patterns**
```python
# src/arrowdsl/plan/query.py
def fragment_telemetry(
    dataset: ds.Dataset,
    *,
    predicate: ComputeExpression | None,
) -> ScanTelemetry:
    fragments = list(dataset.get_fragments(filter=predicate))
    row_groups = sum(
        len(list(f.split_by_row_group()))
        for f in fragments
        if hasattr(f, "split_by_row_group")
    )
    return ScanTelemetry(
        fragment_count=len(fragments),
        row_group_count=row_groups,
        estimated_rows=None,
    )
```

**Target files**
- Update: `src/arrowdsl/plan/query.py`
- Add: `src/arrowdsl/plan/fragments.py`

**Implementation checklist**
- [x] Add fragment enumeration helpers with optional filter predicate.
- [x] Add row-group split/subset helpers for Parquet fragments.
- [x] Expand `ScanTelemetry` to include row-group counts and file hints.

**Status**
Completed.

---

### Scope 6: JoinSpec factory for plan + kernel lanes
**Description**
Create a join spec factory that produces consistent join configurations for both
plan-lane and kernel-lane operations, reducing suffix and output selection drift.

**Code patterns**
```python
# src/arrowdsl/plan/join_specs.py
def join_spec_for_keys(
    *,
    keys: Sequence[str],
    left_out: Sequence[str],
    right_out: Sequence[str],
) -> JoinSpec:
    return JoinSpec(
        join_type="left outer",
        left_keys=tuple(keys),
        right_keys=tuple(keys),
        left_output=tuple(left_out),
        right_output=tuple(right_out),
    )
```

**Target files**
- Add: `src/arrowdsl/plan/join_specs.py`
- Update: `src/arrowdsl/plan/joins.py`
- Update: `src/arrowdsl/compute/kernels.py`

**Implementation checklist**
- [x] Introduce a join spec factory for common join patterns.
- [x] Update `left_join` and `apply_join` to accept the factory output.
- [x] Normalize output selection and suffix rules in one place.

**Status**
Completed.

---

### Scope 7: Schema policy + validation factory
**Description**
Provide a unified schema policy object that bundles alignment, encoding, metadata,
and validation rules. This standardizes schema enforcement across finalize steps.

**Code patterns**
```python
# src/arrowdsl/schema/policy.py
@dataclass(frozen=True)
class SchemaPolicy:
    schema: SchemaLike
    encoding: EncodingPolicy | None = None
    metadata: SchemaMetadataSpec | None = None
    validation: SchemaValidationPolicy | None = None

    def apply(self, table: TableLike) -> TableLike:
        out = SchemaTransform(schema=self.schema).apply(table)
        return self.encoding.apply(out) if self.encoding else out
```

**Target files**
- Add: `src/arrowdsl/schema/policy.py`
- Update: `src/arrowdsl/schema/validation.py`
- Update: `src/arrowdsl/finalize/finalize.py`

**Implementation checklist**
- [x] Introduce `SchemaPolicy` with alignment + encoding + metadata.
- [x] Add optional validation hooks for strict/filter modes.
- [x] Replace duplicated policy wiring in finalize and validation helpers.

**Status**
Completed.

---

### Scope 8: Nested array builder specs
**Description**
Add explicit nested array builder specs for list-view, map, struct, union, and
dictionary types. This standardizes row-based ingestion and nested value handling.

**Code patterns**
```python
# src/arrowdsl/schema/nested_builders.py
def map_array(
    values: Sequence[object | None],
    *,
    key_type: DataTypeLike,
    item_type: DataTypeLike,
) -> ArrayLike:
    pairs = [
        [tuple(item) for item in row] if row is not None else None
        for row in values
    ]
    return pa.array(pairs, type=pa.map_(key_type, item_type))
```

**Target files**
- Add: `src/arrowdsl/schema/nested_builders.py`
- Update: `src/arrowdsl/plan/rows.py`
- Update: `src/arrowdsl/schema/arrays.py`

**Implementation checklist**
- [x] Add builders for list_view, map, struct, union, and dictionary arrays.
- [x] Use builders in row ingestion to avoid ad hoc casting logic.
- [x] Provide type-aware fallbacks for missing or malformed values.

**Status**
Completed.

---

### Scope 9: Dataset source semantics for in-memory readers
**Description**
Introduce explicit one-shot dataset sources for `RecordBatchReader` and iterables,
so repeated scans are guarded or materialized explicitly.

**Code patterns**
```python
# src/arrowdsl/plan/source.py
@dataclass(frozen=True)
class InMemoryDatasetSource:
    reader: RecordBatchReaderLike
    one_shot: bool = True
```

**Target files**
- Update: `src/arrowdsl/plan/source.py`
- Update: `src/arrowdsl/plan/catalog.py`

**Implementation checklist**
- [x] Add a dataset source wrapper that encodes one-shot semantics.
- [x] Guard repeated scans or force materialization when `one_shot` is true.
- [x] Document the behavior in plan source helpers.

**Status**
Completed.
