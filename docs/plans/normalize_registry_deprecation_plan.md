# Normalize Registry Decommission Plan

## Goals
- Remove the legacy `normalize/registry_*` stack and make DataFusion the source of truth.
- Keep normalize dataset discovery, schema access, and validation consistent with runtime catalogs.
- Consolidate diagnostic schema types and ID specs into aligned, reusable modules.
- Delete legacy registry files once all call sites are migrated.

## Preconditions / gates
- `scripts/bootstrap_codex.sh` then `uv sync`
- Lint and type gates before final merge:
  - `uv run ruff check --fix`
  - `uv run pyrefly check`
  - `uv run pyright --warnings --pythonversion=3.13`

## Scope 1: DataFusion-backed normalize registry adapter
Objective: replace registry_specs-driven dataset discovery and aliasing with DataFusion catalog
metadata.
Status: Complete.

Representative code
```python
# src/normalize/registry_runtime.py
from __future__ import annotations

from datafusion import SessionContext

import normalize.dataset_specs as static_dataset_specs
from datafusion_engine.schema_introspection import table_names_snapshot
from datafusion_engine.sql_options import sql_options_for_profile

NORMALIZE_STAGE_META = "normalize_stage"
NORMALIZE_ALIAS_META = "normalize_alias"


def dataset_names(ctx: SessionContext | None = None) -> tuple[str, ...]:
    session = _resolve_session_context(ctx)
    names = table_names_snapshot(session, sql_options=sql_options_for_profile(None))
    normalize_names = {name for name in names if _is_normalize_dataset(session, name)}
    static_names = set(static_dataset_specs.dataset_names())
    return tuple(sorted(normalize_names | static_names))


def dataset_alias(name: str, *, ctx: SessionContext | None = None) -> str:
    session = _resolve_session_context(ctx)
    meta = _metadata_for_table(session, name)
    alias = meta.get(NORMALIZE_ALIAS_META)
    if alias:
        return alias
    return _strip_version(name)
```

Target files
- [x] `src/normalize/registry_runtime.py` (new)
- [x] `src/normalize/runner.py` (register normalize metadata on outputs)
- [x] `src/normalize/dataset_builders.py` (schema metadata defaults)

Implementation checklist
- [x] Add `normalize_stage` / `normalize_alias` metadata to normalize datasets at registration time.
- [x] Implement runtime dataset discovery, alias resolution, and metadata helpers.
- [x] Ensure DataFusion catalog tables/views expose schema metadata consistently.

## Scope 2: Migrate call sites off registry_specs
Objective: move all `normalize.registry_specs` usage to the new DataFusion-backed adapter.
Status: Complete.

Representative code
```python
# src/normalize/op_specs.py
from normalize.registry_runtime import normalize_dataset_alias, normalize_dataset_names
from ibis_engine.registry import datafusion_context

def _rule_op_specs(definitions: tuple[RuleDefinition, ...]) -> tuple[NormalizeOpSpec, ...]:
    ctx = datafusion_context()
    known = frozenset(normalize_dataset_names(ctx))
    specs: list[NormalizeOpSpec] = []
    for rule in definitions:
        output = normalize_dataset_alias(ctx, rule.output)
        inputs = tuple(
            normalize_dataset_alias(ctx, name) if name in known else name
            for name in rule.inputs
        )
        specs.append(NormalizeOpSpec(name=output, inputs=inputs, outputs=(output,)))
    return tuple(specs)
```

Target files
- [x] `src/normalize/op_specs.py`
- [x] `src/normalize/rule_defaults.py`
- [x] `src/normalize/contracts.py`
- [x] `src/normalize/runner.py`
- [x] `src/normalize/ibis_api.py`
- [x] `src/normalize/ibis_plan_builders.py`
- [x] `src/normalize/schemas.py`
- [x] `src/hamilton_pipeline/modules/normalization.py`
- [x] `src/incremental/normalize_update.py`
- [x] `src/incremental/relspec_update.py`
- [x] `src/relspec/incremental.py`
- [x] `src/normalize/__init__.py` (export map cleanup)

Implementation checklist
- [x] Replace `dataset_names`, `dataset_schema`, `dataset_spec`, and alias helpers with runtime
      adapter equivalents.
- [x] Remove `normalize.registry_specs` exports from `normalize.__init__`.
- [x] Update tests to use DataFusion-backed dataset discovery (if any).

## Scope 3: Consolidate diagnostic schema types
Objective: move DIAG_* types from registry_fields into the unified schema registry.
Status: Complete.

Representative code
```python
# src/datafusion_engine/schema_registry.py
import pyarrow as pa

from arrowdsl.schema.build import list_view_type, struct_type

DIAG_TAGS_TYPE = list_view_type(pa.string(), large=True)
DIAG_DETAIL_STRUCT = struct_type(
    {
        "detail_kind": pa.string(),
        "error_type": pa.string(),
        "source": pa.string(),
        "tags": DIAG_TAGS_TYPE,
    }
)
DIAG_DETAILS_TYPE = list_view_type(DIAG_DETAIL_STRUCT, large=True)
```

Target files
- [x] `src/datafusion_engine/schema_registry.py`
- [x] `src/normalize/ibis_plan_builders.py`
- [x] `src/normalize/schemas.py`
- [x] `src/normalize/dataset_fields.py`

Implementation checklist
- [x] Relocate DIAG_* types to the DataFusion schema registry module.
- [x] Update imports to the new location.
- [x] Remove `src/normalize/registry_fields.py`.

## Scope 4: Centralize normalize ID specs
Objective: move `registry_ids` into a DataFusion-aligned ID spec module.
Status: Complete.

Representative code
```python
# src/datafusion_engine/normalize_ids.py
from ibis_engine.hashing import HashExprSpec
from ibis_engine.hashing import hash_expr_spec_factory as hash_spec_factory

TYPE_EXPR_ID_SPEC = hash_spec_factory(
    prefix="cst_type_expr",
    cols=("path", "bstart", "bend"),
    null_sentinel="None",
)
```

Target files
- [x] `src/datafusion_engine/normalize_ids.py` (new)
- [x] `src/normalize/ibis_plan_builders.py`
- [x] `src/normalize/utils.py`
- [x] `src/normalize/dataset_rows.py`
- [x] `src/normalize/__init__.py`

Implementation checklist
- [x] Add `datafusion_engine.normalize_ids` module and move constants there.
- [x] Update imports in normalize plan builders and utils.
- [x] Remove `src/normalize/registry_ids.py`.

## Scope 5: Replace registry_validation with SchemaIntrospector checks
Objective: validate normalize rule specs against DataFusion metadata instead of static registries.
Status: Complete.

Representative code
```python
# src/normalize/runtime_validation.py
from datafusion_engine.schema_introspection import SchemaIntrospector
from datafusion_engine.sql_options import sql_options_for_profile

def validate_rule_specs(
    rules: Sequence[RuleDefinition],
    *,
    ctx: SessionContext,
) -> None:
    introspector = SchemaIntrospector(ctx, sql_options=sql_options_for_profile(None))
    known = set(introspector.table_names())
    # ... validate evidence outputs and metadata against runtime schemas
```

Target files
- [x] `src/normalize/runtime_validation.py` (new)
- [x] `src/normalize/runner.py`

Implementation checklist
- [x] Replace `normalize.registry_validation.validate_rule_specs` with runtime validation.
- [x] Wire validation through `normalize.runner`.
- [x] Remove `src/normalize/registry_validation.py`.

## Scope 6: Delete legacy registry modules
Objective: delete registry modules after all migrations are complete.
Status: Complete.

Target files
- [x] `src/normalize/registry_builders.py` (replaced by `src/normalize/dataset_builders.py`)
- [x] `src/normalize/registry_rows.py` (replaced by `src/normalize/dataset_rows.py`)
- [x] `src/normalize/registry_bundles.py` (replaced by `src/normalize/dataset_bundles.py`)
- [x] `src/normalize/registry_templates.py` (replaced by `src/normalize/dataset_templates.py`)
- [x] `src/normalize/registry_specs.py` (replaced by `src/normalize/dataset_specs.py`)
- [x] `src/normalize/registry_fields.py` (replaced by `src/normalize/dataset_fields.py`)
- [x] `src/normalize/registry_ids.py` (replaced by `src/datafusion_engine/normalize_ids.py`)
- [x] `src/normalize/registry_validation.py` (replaced by `src/normalize/runtime_validation.py`)

Implementation checklist
- [x] Remove imports and exports that point at registry modules.
- [x] Delete registry modules and update any remaining tests or docs.
- [x] Confirm DataFusion runtime supplies dataset names, schemas, and metadata in all call paths.

## Legacy decommission list
Removed in Scope 6:
- `src/normalize/registry_builders.py` (replaced by `src/normalize/dataset_builders.py`)
- `src/normalize/registry_rows.py` (replaced by `src/normalize/dataset_rows.py`)
- `src/normalize/registry_bundles.py` (replaced by `src/normalize/dataset_bundles.py`)
- `src/normalize/registry_templates.py` (replaced by `src/normalize/dataset_templates.py`)
- `src/normalize/registry_specs.py` (replaced by `src/normalize/dataset_specs.py`)
- `src/normalize/registry_fields.py` (replaced by `src/normalize/dataset_fields.py`)
- `src/normalize/registry_ids.py` (replaced by `src/datafusion_engine/normalize_ids.py`)
- `src/normalize/registry_validation.py` (replaced by `src/normalize/runtime_validation.py`)
