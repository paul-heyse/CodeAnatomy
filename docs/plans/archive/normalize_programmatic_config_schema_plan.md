## Normalize Programmatic Configuration + Schema Plan

### Goals
- Make normalize configuration, schema, and metadata fully programmatic and registry-driven.
- Replace inline schema/query/contract declarations with row-driven builders.
- Centralize ID/HashSpec definitions and schema policies for consistent alignment/encoding.
- Reduce stringly-typed wiring for normalize plan catalogs and dataset references.
- Preserve all existing normalized outputs, column names, and metadata semantics.

### Constraints
- Preserve current normalized outputs and dataset contracts.
- Keep strict typing + Ruff compliance (no suppressions).
- Avoid relative imports; keep modules fully typed and pyright clean.
- Keep kernel-lane conversions (text index lookups, span derivation) in kernel lane.

---

### Scope 1: Field + Bundle Catalogs
**Description**
Create centralized catalogs for reusable `ArrowFieldSpec` entries and `FieldBundle`s.
Normalize schemas reference catalog keys instead of defining inline field lists.

**Code patterns**
```python
# src/normalize/registry_fields.py
FIELD_CATALOG = {
    "type_expr_id": ArrowFieldSpec(name="type_expr_id", dtype=pa.string()),
    "type_id": ArrowFieldSpec(name="type_id", dtype=pa.string()),
    "expr_kind": dict_field("expr_kind"),
}

def field(name: str) -> ArrowFieldSpec:
    return FIELD_CATALOG[name]


# src/normalize/registry_bundles.py
BUNDLE_CATALOG = {
    "file_identity": file_identity_bundle(include_sha256=False),
    "span": span_bundle(),
}

def bundle(name: str) -> FieldBundle:
    return BUNDLE_CATALOG[name]
```

**Target files**
- Add: `src/normalize/registry_fields.py`
- Add: `src/normalize/registry_bundles.py`
- Update: `src/normalize/schemas.py`

**Implementation checklist**
- [x] Define `FIELD_CATALOG` entries for all normalize fields.
- [x] Define `BUNDLE_CATALOG` for standard bundles (file identity, spans, etc.).
- [x] Replace inline `ArrowFieldSpec` lists with catalog lookups.

**Status**
Completed.

---

### Scope 2: HashSpec + SpanIdSpec Registry
**Description**
Move ID definitions into a registry to ensure consistent use across plan-lane and
kernel-lane helpers.

**Code patterns**
```python
# src/normalize/registry_ids.py
_HASH_SPECS = {
    "type_expr_id": hash_spec_factory(
        prefix="cst_type_expr",
        cols=("path", "bstart", "bend"),
        null_sentinel="None",
    ),
    "type_id": hash_spec_factory(prefix="type", cols=("type_repr",), null_sentinel="None"),
}

def hash_spec(name: str) -> HashSpec:
    return _HASH_SPECS[name]
```

**Target files**
- Add: `src/normalize/registry_ids.py`
- Update: `src/normalize/utils.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Define `HashSpec` registry entries for all normalize IDs.
- [x] Wire `MaskedHashExprSpec` and `HashExprSpec` to registry helpers.
- [x] Remove inline HashSpec definitions from normalize modules.

**Status**
Completed.

---

### Scope 3: Dataset Rows + Templates
**Description**
Define each normalize dataset as a compact row spec that references bundles/fields
and inherits defaults from templates (ordering + metadata).

**Code patterns**
```python
# src/normalize/registry_rows.py
@dataclass(frozen=True)
class DatasetRow:
    name: str
    version: int
    bundles: tuple[str, ...]
    fields: tuple[str, ...]
    join_keys: tuple[str, ...] = ()
    derived: tuple[DerivedFieldSpec, ...] = ()
    contract: ContractRow | None = None
    template: str | None = None


# src/normalize/registry_templates.py
TEMPLATES = {
    "normalize": RegistryTemplate(
        stage="normalize",
        ordering_level=OrderingLevel.IMPLICIT,
    )
}
```

**Target files**
- Add: `src/normalize/registry_rows.py`
- Add: `src/normalize/registry_templates.py`
- Update: `src/normalize/schemas.py`

**Implementation checklist**
- [x] Add `DatasetRow` entries for type exprs, type nodes, CFG, def/use, reaches, diagnostics.
- [x] Capture `join_keys` to drive ordering keys programmatically.
- [x] Define template defaults for stage metadata.
- [ ] Define template defaults for determinism tier metadata (not implemented).

**Status**
Partially completed.

---

### Scope 4: Programmatic Dataset Builder + Registry API
**Description**
Build `TableSchemaSpec`, `QuerySpec`, `ContractSpec`, and `SchemaMetadataSpec` from
dataset rows and templates, replacing inline schemas/queries/contracts.

**Code patterns**
```python
# src/normalize/registry_builders.py
def build_dataset_spec(row: DatasetRow) -> DatasetSpec:
    table_spec = make_table_spec(
        name=row.name,
        version=row.version,
        bundles=tuple(bundle(name) for name in row.bundles),
        fields=tuple(field(name) for name in row.fields),
    )
    contract_spec = build_contract_spec(row.contract, table_spec=table_spec)
    metadata_spec = build_metadata_spec(row)
    return register_dataset(
        table_spec=table_spec,
        contract_spec=contract_spec,
        query_spec=build_query_spec(row),
        metadata_spec=metadata_spec,
    )


# src/normalize/registry_specs.py
def dataset_spec(name: str) -> DatasetSpec:
    return DATASET_SPECS[name]
```

**Target files**
- Add: `src/normalize/registry_builders.py`
- Add: `src/normalize/registry_specs.py`
- Update: `src/normalize/schemas.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_cfg.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Implement builders for query, contract, and metadata specs.
- [x] Expose registry accessors (`dataset_schema`, `dataset_query`, `dataset_contract`).
- [x] Replace `*_SPEC`, `*_SCHEMA`, and `*_QUERY` constants with registry lookups.

**Status**
Completed.

---

### Scope 5: SchemaPolicy + Encoding Integration
**Description**
Drive alignment/encoding from dataset specs using `SchemaPolicy`, removing per-module
manual encoding/projection logic.

**Code patterns**
```python
# src/normalize/registry_specs.py
def dataset_schema_policy(name: str, *, ctx: ExecutionContext) -> SchemaPolicy:
    spec = dataset_spec(name).table_spec
    return schema_policy_factory(spec, ctx=ctx)
```

**Target files**
- Update: `src/normalize/registry_specs.py`
- Update: `src/normalize/runner.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_cfg.py`
- Update: `src/normalize/bytecode_dfg.py`
- Update: `src/normalize/diagnostics.py`

**Implementation checklist**
- [x] Add registry accessors for `SchemaPolicy`.
- [ ] Replace ad-hoc encoding logic with `SchemaPolicy.apply(...)`.
- [ ] Ensure pipelines use `dataset_schema_policy(...)` where alignment/encoding is needed.

**Status**
Partially completed.

---

### Scope 6: Plan Catalog Registry (Programmatic Wiring)
**Description**
Replace stringly-typed normalize plan refs with a registry of plan rows so derived
plans (types, cfg, reaches, diagnostics) are wired programmatically.

**Code patterns**
```python
# src/normalize/registry_plans.py
@dataclass(frozen=True)
class PlanRow:
    name: str
    inputs: tuple[str, ...]
    derive: Callable[[PlanCatalog, ExecutionContext], Plan | None]

PLAN_ROWS = (
    PlanRow(name="type_exprs_norm", inputs=("cst_type_exprs",), derive=derive_type_exprs_norm),
)
```

**Target files**
- Add: `src/normalize/registry_plans.py`
- Update: `src/normalize/catalog.py`

**Implementation checklist**
- [x] Encode normalize plan refs as `PlanRow` entries.
- [x] Replace inline plan ref constants with registry lookups.
- [x] Ensure plan dependencies are discoverable from `PlanRow.inputs`.

**Status**
Completed.

---

### Scope 7: QuerySpec Base Columns From Registry
**Description**
Remove static base column lists from normalize modules and derive them from registry
rows or query specs for consistent projections.

**Code patterns**
```python
# src/normalize/registry_specs.py
def dataset_input_columns(name: str) -> tuple[str, ...]:
    return tuple(dataset_input_schema(name).names)


# src/normalize/types.py
columns = dataset_input_columns("type_exprs_norm_v1")
plan = plan_source(cst_type_exprs, ctx=ctx, columns=columns)
```

**Target files**
- Update: `src/normalize/registry_specs.py`
- Update: `src/normalize/types.py`
- Update: `src/normalize/bytecode_cfg.py`
- Update: `src/normalize/bytecode_dfg.py`

**Implementation checklist**
- [x] Add registry helper for input column lists.
- [x] Replace local `_BASE_*` column lists with registry lookups.
- [x] Keep kernel-lane conversions unchanged.

**Status**
Completed.

---

### Scope 8: Table-backed Schema/Contract Tables (Optional)
**Description**
Expose normalize schemas/contracts as Arrow spec tables for programmatic auditing
and external tooling, using `arrowdsl.spec.tables.schema`.

**Code patterns**
```python
# src/normalize/spec_tables.py
SCHEMA_TABLES = schema_spec_tables_from_dataset_specs(DATASET_SPECS.values())
FIELD_TABLE = SCHEMA_TABLES.field_table
CONTRACT_TABLE = SCHEMA_TABLES.contract_table
```

**Target files**
- Add: `src/normalize/spec_tables.py`
- Update: `src/normalize/registry_specs.py`

**Implementation checklist**
- [x] Generate schema/contract tables from dataset specs.
- [x] Expose read-only tables for registry inspection.
- [x] Keep this optional and non-invasive for runtime behavior.

**Status**
Completed.
