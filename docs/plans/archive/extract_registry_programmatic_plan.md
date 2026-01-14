## Extract Registry Programmatic Refactor Plan

### Goals
- Replace static extractor schema/config blocks with programmatic catalogs and row-driven builders.
- Keep extraction logic in `src/extract/*` focused on parsing + row emission only.
- Leverage ArrowDSL helpers for specs, metadata, plans, and derived IDs.
- Ensure schema/metadata/query definitions remain consistent across extractors.

### Constraints
- Preserve existing output schemas, column names, and metadata semantics.
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Allow extractor modules to remain independent of each other.

---

### Scope 1: Field + Bundle Catalogs
**Description**
Create centralized catalogs for reusable `ArrowFieldSpec` entries and `FieldBundle`s.
Extractor modules reference catalog keys instead of defining inline `_FIELDS` lists.

**Code patterns**
```python
# src/extract/registry_fields.py
_FIELD_CATALOG = {
    "file_id": ArrowFieldSpec(name="file_id", dtype=pa.string()),
    "path": ArrowFieldSpec(name="path", dtype=pa.string()),
    "bstart": ArrowFieldSpec(name="bstart", dtype=pa.int64()),
    # ...
}

def field(key: str) -> ArrowFieldSpec:
    return _FIELD_CATALOG[key]


# src/extract/registry_bundles.py
_BUNDLE_CATALOG = {
    "file_identity": file_identity_bundle(),
    "span": span_bundle(),
    "scip_range": scip_range_bundle(),
}

def bundle(name: str) -> FieldBundle:
    return _BUNDLE_CATALOG[name]
```

**Target files**
- Add: `src/extract/registry_fields.py`
- Add: `src/extract/registry_bundles.py`
- Update: `src/extract/*_extract.py` (replace inline field lists with catalog keys)

**Implementation checklist**
- [x] Define `FIELD_CATALOG` for all extract columns.
- [x] Define `BUNDLE_CATALOG` for standard bundles (file identity, spans, ranges, provenance).
- [x] Replace `_FIELDS` lists with catalog lookups.

**Status**
Completed.

---

### Scope 2: HashSpec Registry (Programmatic IDs)
**Description**
Move static hash specs to a row-driven registry. Derived IDs in extractors are built
from this registry, not from inline specs.

**Code patterns**
```python
# src/extract/registry_ids.py
_HASH_SPECS = {
    "ts_node_id": hash_spec_factory(
        prefix="ts_node",
        cols=("path", "start_byte", "end_byte", "ts_type"),
        out_col="ts_node_id",
    ),
    # ...
}

def hash_spec(name: str, *, repo_id: str | None = None) -> HashSpec:
    if name == "repo_file_id":
        return repo_file_id_spec(repo_id)
    return _HASH_SPECS[name]
```

**Target files**
- Add: `src/extract/registry_ids.py`
- Update: `src/extract/hash_specs.py` (replace with registry or delete)
- Update: `src/extract/*_extract.py` (use `hash_spec(...)` + `MaskedHashExprSpec`)

**Implementation checklist**
- [x] Define the HashSpec registry in `registry_ids.py`.
- [x] Migrate all existing hash specs to the registry.
- [x] Replace inline/explicit HashSpec references in extractors.

**Status**
Completed.

---

### Scope 3: Dataset Rows + Templates
**Description**
Define each extractor dataset as a compact row spec that references bundles/fields
and inherits defaults from extractor templates.

**Code patterns**
```python
# src/extract/registry_rows.py
@dataclass(frozen=True)
class DatasetRow:
    name: str
    version: int
    bundles: tuple[str, ...]
    fields: tuple[str, ...]
    derived: tuple[DerivedIdSpec, ...] = ()
    row_fields: tuple[str, ...] = ()
    row_extras: tuple[str, ...] = ()
    ordering_keys: tuple[str, ...] = ()
    template: str | None = None
    postprocess: str | None = None

# src/extract/registry_templates.py
@dataclass(frozen=True)
class ExtractorTemplate:
    extractor_name: str
    ordering_level: OrderingLevel
    metadata_extra: Mapping[bytes, bytes] | None = None
```

**Target files**
- Add: `src/extract/registry_rows.py`
- Add: `src/extract/registry_templates.py`
- Update: `src/extract/*_extract.py` (refer to dataset rows by name)

**Implementation checklist**
- [x] Define `DatasetRow` and extractor templates.
- [x] Add rows for CST/AST/SCIP/TS/BC/Symtable/Runtime/Repo datasets.
- [x] Encode per-dataset extras (metadata, derived IDs, row fields).

**Status**
Completed.

---

### Scope 4: Programmatic Dataset Builder
**Description**
Build `TableSchemaSpec`, `QuerySpec`, and `SchemaMetadataSpec` from dataset rows,
using ArrowDSL/spec helpers to avoid inline projections and metadata blocks.

**Code patterns**
```python
# src/extract/registry_builders.py
def build_dataset_spec(row: DatasetRow, *, ctx: QueryContext) -> DatasetSpec:
    table_spec = make_table_spec(
        name=row.name,
        version=row.version,
        bundles=tuple(bundle(name) for name in row.bundles),
        fields=fields(row.fields),
    )
    metadata_spec = build_metadata_spec(row, base_columns=table_spec.to_arrow_schema().names)
    registration = DatasetRegistration(
        query_spec=build_query_spec(row, ctx=ctx),
        metadata_spec=metadata_spec,
    )
    return register_dataset(table_spec=table_spec, registration=registration)
```

**Target files**
- Add: `src/extract/registry_builders.py`
- Add: `src/extract/registry_specs.py` (public registry API)
- Update: `src/extract/*_extract.py` (replace `*_SPEC`/`*_SCHEMA`/`*_QUERY`)

**Implementation checklist**
- [x] Implement `build_query_spec` that auto-derives base/derived columns.
- [x] Implement `build_metadata_spec` with `extractor_metadata_spec` + ordering.
- [x] Expose `dataset_schema`, `dataset_query`, `dataset_row_schema` helpers.

**Status**
Completed.

---

### Scope 5: Programmatic Metadata + Ordering
**Description**
Make ordering/metadata automatic from dataset rows (ordering keys inferred from
columns, extractor metadata attached from templates).

**Code patterns**
```python
def build_metadata_spec(row: DatasetRow, base_columns: Sequence[str]) -> SchemaMetadataSpec:
    templ = template(row.template) if row.template is not None else None
    extractor_name = templ.extractor_name if templ is not None else "extract"
    extractor_extra = templ.metadata_extra if templ is not None else None
    ordering = ordering_metadata_spec(
        OrderingLevel.IMPLICIT,
        keys=infer_ordering_keys(base_columns),
    )
    extractor_meta = extractor_metadata_spec(extractor_name, row.version, extra=extractor_extra)
    return merge_metadata_specs(ordering, extractor_meta)
```

**Target files**
- Update: `src/extract/helpers.py` (use registry metadata for materialization)
- Update: `src/extract/*_extract.py` (remove inline `_METADATA` constants)

**Implementation checklist**
- [x] Ensure ordering keys inferred from base columns.
- [x] Attach extractor metadata from templates.
- [x] Merge runtime options metadata when present.

**Status**
Completed.

---

### Scope 6: Extractor Module Integration
**Description**
Simplify extractors to use registry lookups (`dataset(...)`) and focus on row emission.

**Code patterns**
```python
# src/extract/tree_sitter_extract.py
TS_NODES_QUERY = dataset_query("ts_nodes_v1")
TS_NODES_SCHEMA = dataset_schema("ts_nodes_v1")
TS_NODES_ROW_SCHEMA = dataset_row_schema("ts_nodes_v1")

metadata = dataset_metadata_with_options("ts_nodes_v1", options=options)
table = materialize_plan(plan, metadata_spec=metadata, attach_ordering_metadata=True)
```

**Target files**
- Update: `src/extract/ast_extract.py`
- Update: `src/extract/cst_extract.py`
- Update: `src/extract/scip_extract.py`
- Update: `src/extract/symtable_extract.py`
- Update: `src/extract/bytecode_extract.py`
- Update: `src/extract/tree_sitter_extract.py`
- Update: `src/extract/runtime_inspect_extract.py`
- Update: `src/extract/repo_scan.py`

**Implementation checklist**
- [x] Replace inline schema/query/metadata constants with registry lookups.
- [x] Keep only extractor logic + row emission in each module.
- [x] Ensure derived IDs use registry HashSpecs.

**Status**
Completed.

---

### Scope 7: Cleanup + Validation
**Description**
Remove obsolete static constants and ensure programmatic registry outputs match
current schemas/metadata.

**Code patterns**
```python
def validate_registry() -> None:
    for name in registry.dataset_names():
        spec = registry.dataset(name)
        _ = spec.schema()  # ensures schema validity
```

**Target files**
- Update: `src/extract/helpers.py`
- Update: `src/extract/__init__.py`
- Remove: `src/extract/hash_specs.py` (if fully replaced)
- Remove: inline `_FIELDS`/`*_METADATA` constants from extractors

**Implementation checklist**
- [x] Verify schema/metadata equivalence with current outputs.
- [x] Delete obsolete constants + registry duplicates.
- [x] Run ruff/pyright/pyrefly.

**Status**
Completed.
