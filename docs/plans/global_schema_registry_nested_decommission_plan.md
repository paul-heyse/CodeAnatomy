# Global Schema Registry Nested Decommission Plan

## Objective
Fully remove nested dataset schemas from `schema_spec.system.GLOBAL_SCHEMA_REGISTRY` and make
nested schema resolution authoritative through `datafusion_engine.schema_registry`, including
all derived nested datasets.

## Canonical nested dataset list (exact)
These names come directly from `datafusion_engine.schema_registry.NESTED_DATASET_INDEX`:

```
ast_edges
ast_errors
ast_nodes
bytecode_errors
bytecode_exception_table
cst_callsites
cst_defs
cst_edges
cst_imports
cst_name_refs
cst_nodes
cst_parse_errors
cst_parse_manifest
cst_type_exprs
py_bc_blocks
py_bc_cfg_edges
py_bc_code_units
py_bc_instructions
scip_diagnostics
scip_documents
scip_external_symbol_information
scip_metadata
scip_occurrences
scip_symbol_information
scip_symbol_relationships
symtable_scope_edges
symtable_scopes
symtable_symbols
ts_errors
ts_missing
ts_nodes
```

Registry-row presence check (exact):
- No `DatasetRow` definitions in `src/extract/registry_rows.py`, `src/normalize/registry_rows.py`,
  or `src/cpg/registry_rows.py` match any of the nested names above.
- The only references in registry rows are metadata-only (e.g., evidence source names), not dataset
  row names.

## Scope Item 1: Treat all nested datasets as DataFusion-native (intrinsic + derived)
Route every nested dataset name through DataFusion schema resolution, including derived datasets.

Representative code pattern:
```python
from datafusion_engine.schema_registry import is_nested_dataset, nested_schema_for

def dataset_schema(name: str) -> SchemaLike:
    if is_nested_dataset(name):
        return nested_schema_for(name, allow_derived=True)
    return dataset_spec(name).schema()
```

Target file list:
- `src/extract/registry_specs.py`
- `src/normalize/registry_specs.py`
- `src/cpg/registry_specs.py`

Implementation checklist:
- [x] Update extract/normalize/cpg `dataset_schema` helpers to resolve nested datasets via
      `nested_schema_for(..., allow_derived=True)`.
- [x] Ensure fallbacks still resolve non-nested dataset names via dataset specs.
- [x] Add a simple guard to confirm `is_nested_dataset` is used before spec fallback.

## Scope Item 2: Block nested datasets from registering in GLOBAL_SCHEMA_REGISTRY
Prevent nested dataset specs from being registered into the global registry in all paths
(`register_dataset_spec`, `register_from_tables`, and any registry builder flows).

Representative code pattern:
```python
from datafusion_engine.schema_registry import is_nested_dataset

def register_dataset(self, spec: DatasetSpec) -> DatasetSpec:
    if self is GLOBAL_SCHEMA_REGISTRY and is_nested_dataset(spec.name):
        return spec
    ...
```

Target file list:
- `src/schema_spec/system.py`

Implementation checklist:
- [x] Add a nested-dataset filter used by `SchemaRegistry.register_dataset` for the global registry.
- [x] Ensure `SchemaRegistry.register_from_tables` prunes nested entries for the global registry.
- [x] Use a direct import of `is_nested_dataset` (no import cycle observed).

## Scope Item 3: Prune any existing nested entries from GLOBAL_SCHEMA_REGISTRY
Add a cleanup function to remove nested dataset specs that may have already been registered.

Representative code pattern:
```python
def prune_nested_dataset_specs(registry: SchemaRegistry) -> None:
    from datafusion_engine.schema_registry import is_nested_dataset
    for name in list(registry.dataset_specs):
        if is_nested_dataset(name):
            registry.dataset_specs.pop(name, None)
```

Target file list:
- `src/schema_spec/system.py`
- `src/hamilton_pipeline/modules/cpg_build.py`

Implementation checklist:
- [x] Add `prune_nested_dataset_specs` helper in `schema_spec.system`.
- [x] Call the prune helper after global registry bootstraps (table-based registration and
      relspec schema registry assembly).
- [x] Ensure pruning is idempotent and safe if no entries exist.

## Scope Item 4: Update GLOBAL_SCHEMA_REGISTRY consumers to skip nested datasets
Any logic that consults `GLOBAL_SCHEMA_REGISTRY` for schema inference must be guarded so that
nested datasets are resolved via the DataFusion registry instead.

Representative code pattern:
```python
from datafusion_engine.schema_registry import is_nested_dataset, schema_for

if is_nested_dataset(name):
    return schema_for(name)
return GLOBAL_SCHEMA_REGISTRY.dataset_specs[name].schema()
```

Target file list:
- `src/normalize/schema_infer.py`
- `src/hamilton_pipeline/modules/cpg_build.py`
- `src/hamilton_pipeline/modules/outputs.py`
- `src/relspec/rules/validation.py`

Implementation checklist:
- [x] Identify direct `GLOBAL_SCHEMA_REGISTRY.dataset_specs.get(...)` lookups in schema inference
      and validation paths.
- [x] Guard those lookups with `is_nested_dataset(name)` and route to nested schema resolution
      where needed.
- [x] Confirm no nested dataset name is required for contract validation logic.

## Scope Item 5: Tests and verification
Add unit-level checks to lock the new behavior and prevent regression.

Representative code pattern:
```python
def test_global_registry_excludes_nested() -> None:
    from schema_spec.system import GLOBAL_SCHEMA_REGISTRY
    from datafusion_engine.schema_registry import nested_dataset_names

    for name in nested_dataset_names():
        assert name not in GLOBAL_SCHEMA_REGISTRY.dataset_specs
```

Target file list:
- `tests/unit/test_global_schema_registry_nested.py` (new)

Implementation checklist:
- [x] Add a test that ensures nested datasets are not present in `GLOBAL_SCHEMA_REGISTRY`.
- [x] Add a test verifying `dataset_schema` resolves derived nested datasets via DataFusion.
- [x] Add a regression test for registry pruning (idempotent behavior).

## Deliverables
- Nested datasets are always resolved from `datafusion_engine.schema_registry`.
- `GLOBAL_SCHEMA_REGISTRY` no longer contains nested dataset specs.
- Registry consumers explicitly bypass global schema specs for nested dataset names.
- Tests prove decommissioning is enforced and stable.
