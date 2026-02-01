# DataFusion Engine Semantic Alignment Opportunities Plan

> **Goal**: Extend the semantic-first architecture by tightening the execution/definition boundary, eliminating legacy pathways, and aligning materialization + caching behavior with semantic metadata.

---

## Executive Summary

This plan covers the next set of alignment improvements between `src/datafusion_engine` (execution authority) and `src/semantics` (definition authority). Each scope item below includes:
- Representative code snippets of the expected architectural patterns
- Target files
- Deprecations/deletions enabled by completion
- A concrete implementation checklist

---

## Scope Index

1. **Scope 1** - Registry Specs Consume `SemanticRuntimeConfig`
2. **Scope 2** - Semantic Catalog as Single Source of Dataset Specs
3. **Scope 3** - Cache Policy Derived from `SemanticDatasetRow` Metadata
4. **Scope 4** - Canonical Naming Enforcement at All Registration Boundaries
5. **Scope 5** - Remove Legacy View Registration Flows
6. **Scope 6** - Centralize Semantic Input Validation
7. **Scope 7** - Materialization Policies Driven by Semantic Metadata
8. **Scope 8** - Architecture Docs + Plan References Cleanup

---

## Scope 1 - Registry Specs Consume SemanticRuntimeConfig

### Goal
Remove semantic-runtime decisions from `datafusion_engine` by building a `SemanticRuntimeConfig` adapter once and threading it through view registration.

### Representative Pattern

```python
# src/datafusion_engine/views/registry_specs.py
from datafusion_engine.semantics_runtime import semantic_runtime_from_profile
from semantics.runtime import SemanticRuntimeConfig

def view_graph_nodes(..., runtime_profile: DataFusionRuntimeProfile, ...) -> tuple[ViewNode, ...]:
    runtime_config: SemanticRuntimeConfig = semantic_runtime_from_profile(runtime_profile)
    return _semantics_view_nodes(..., runtime_config=runtime_config)
```

```python
# src/datafusion_engine/views/registry_specs.py
def _semantics_view_nodes(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object],
    runtime_profile: DataFusionRuntimeProfile,
    runtime_config: SemanticRuntimeConfig,
) -> list[ViewNode]:
    view_specs = _semantic_view_specs_for_registration(
        ctx,
        runtime_profile=runtime_profile,
        runtime_config=runtime_config,
    )
```

### Target Files
- Modify: `src/datafusion_engine/views/registry_specs.py`
- Modify: `src/datafusion_engine/views/registration.py`
- Modify: `src/datafusion_engine/semantics_runtime.py`

### Deprecations/Deletions
- None (refactor only)

### Implementation Checklist
- [x] Build `SemanticRuntimeConfig` once per registration flow
- [x] Thread `runtime_config` into semantic view node building
- [x] Use `runtime_config.cache_policy_overrides` where applicable
- [x] Remove `DataFusionRuntimeProfile` branching for semantic-only options
- [ ] Update tests that assert view graph node config

**Status (2026-02-01):** Implementation complete; tests not yet updated.

---

## Scope 2 - Semantic Catalog as Single Source of Dataset Specs

### Goal
Ensure all dataset contract/spec lookups for semantic outputs use `semantics.catalog.dataset_specs` only.

### Representative Pattern

```python
# src/datafusion_engine/views/registry_specs.py
from semantics.catalog.dataset_specs import dataset_specs

def _semantic_dataset_specs() -> dict[str, DatasetSpec]:
    return {spec.name: spec for spec in dataset_specs()}
```

### Target Files
- Modify: `src/datafusion_engine/views/registry_specs.py`
- Modify: `src/datafusion_engine/schema/contracts.py`
- Modify: `src/datafusion_engine/dataset/semantic_catalog.py`

### Deprecations/Deletions
- Deprecate: `schema_spec.relationship_specs` usage for semantic outputs
- Remove: any semantic dataset spec adapters in `datafusion_engine` that shadow semantic catalog

### Implementation Checklist
- [x] Replace all semantic dataset spec lookups with semantic catalog
- [x] Delete any redundant spec builders in `datafusion_engine`
- [ ] Add tests to ensure semantic outputs get catalog-based contracts

**Status (2026-02-01):** Core lookup alignment complete; tests pending.

---

## Scope 3 - Cache Policy Derived from SemanticDatasetRow Metadata

### Goal
Drive cache policy decisions from semantic metadata (CDF support, category, merge keys) instead of view-name heuristics.

### Representative Pattern

```python
# src/datafusion_engine/views/registry_specs.py
from semantics.catalog.dataset_rows import dataset_row

def _semantic_cache_policy_for_row(row: SemanticDatasetRow) -> CachePolicy:
    if row.supports_cdf and row.merge_keys:
        return "delta_staging"
    if row.category in {"semantic", "analysis"} and row.register_view:
        return "delta_staging"
    return "none"
```

### Target Files
- Modify: `src/datafusion_engine/views/registry_specs.py`
- Modify: `src/semantics/catalog/dataset_rows.py` (if additional metadata needed)

### Deprecations/Deletions
- Remove: view-name-based cache policy branches for semantic outputs

### Implementation Checklist
- [x] Implement row-based cache policy function
- [x] Replace cache policy selection logic for semantic view nodes
- [ ] Update tests that assert cache policy outcomes

**Status (2026-02-01):** Cache policy wiring complete; tests pending.

---

## Scope 4 - Canonical Naming Enforcement at All Registration Boundaries

### Goal
Reject non-canonical names at registration and materialization boundaries; use aliases only for backward compatibility surfaces.

### Representative Pattern

```python
# src/datafusion_engine/dataset/semantic_catalog.py
from semantics.naming import canonical_output_name

canonical = canonical_output_name(row.name)
if canonical != row.name:
    raise ValueError(
        f"Semantic dataset rows must use canonical output names. Got {row.name!r}."
    )
```

### Target Files
- Modify: `src/datafusion_engine/dataset/semantic_catalog.py`
- Modify: `src/datafusion_engine/views/registry_specs.py`
- Modify: `src/semantics/pipeline.py`

### Deprecations/Deletions
- Remove: alias-based registration in semantic view nodes (retain aliasing only in shims)

### Implementation Checklist
- [x] Enforce canonical naming in catalog registration
- [x] Canonicalize view specs before view node creation
- [x] Gate aliasing at compatibility layers only

**Status (2026-02-01):** Complete.

---

## Scope 5 - Remove Legacy View Registration Flows

### Goal
Eliminate `register_view_specs` and other legacy pathways that bypass semantic view graph registration.

### Representative Pattern

```python
# src/datafusion_engine/session/runtime.py (remove legacy path)
def register_view_specs(...):
    raise RuntimeError("Legacy view spec registration is removed; use ensure_view_graph.")
```

### Target Files
- Modify: `src/datafusion_engine/session/runtime.py`
- Modify: `src/schema_spec/view_specs.py`
- Modify: `tests/unit/...` (legacy view-spec tests)

### Deprecations/Deletions
- Delete: `register_view_specs` API if unused
- Delete: tests asserting legacy registry view expressions

### Implementation Checklist
- [x] Identify remaining call sites of legacy view registration
- [x] Remove or hard-fail on legacy entry points
- [ ] Update tests to assert shims/deprecations instead of functionality

**Status (2026-02-01):** Legacy entry points now hard-fail; tests pending.

---

## Scope 6 - Centralize Semantic Input Validation

### Goal
Provide a single validation helper in `semantics.validation` and route all DataFusion entrypoints through it.

### Representative Pattern

```python
# src/semantics/validation/__init__.py
def require_semantic_inputs(
    ctx: SessionContext,
    *,
    input_mapping: Mapping[str, str],
) -> None:
    validation = validate_semantic_input_columns(ctx, input_mapping=input_mapping)
    if not validation.valid:
        raise ValueError(...)
```

```python
# src/datafusion_engine/views/registry_specs.py
from semantics.validation import require_semantic_inputs

require_semantic_inputs(ctx, input_mapping=input_mapping)
```

### Target Files
- Modify: `src/semantics/validation/*`
- Modify: `src/datafusion_engine/views/registry_specs.py`
- Modify: `src/semantics/pipeline.py`

### Deprecations/Deletions
- Remove: duplicated validation logic in datafusion_engine

### Implementation Checklist
- [x] Introduce a single semantic input validation helper
- [x] Route all semantic view registration through the helper
- [x] Record validation artifacts in one place

**Status (2026-02-01):** Complete.

---

## Scope 7 - Materialization Policies Driven by Semantic Metadata

### Goal
Materialization should respect semantic dataset metadata (partitioning, merge keys, schema evolution).

### Representative Pattern

```python
# src/semantics/pipeline.py
from semantics.catalog.dataset_specs import dataset_spec

spec = dataset_spec(view_name)
write_policy = spec.delta_write_policy
schema_policy = spec.delta_schema_policy
```

### Target Files
- Modify: `src/semantics/pipeline.py`
- Modify: `src/datafusion_engine/io/write.py`

### Deprecations/Deletions
- Remove: local defaults that override semantic dataset write policies

### Implementation Checklist
- [x] Pull write policy from semantic dataset spec
- [x] Respect semantic schema evolution settings
- [ ] Add tests for policy propagation to materialization pipeline

**Status (2026-02-01):** Materialization policy wiring complete; tests pending.

---

## Scope 8 - Architecture Docs + Plan References Cleanup

### Goal
Ensure all docs reflect the semantics-first reality and remove references to deleted view registry/DSL.

### Representative Pattern

```markdown
# docs/architecture/...
- Remove references to view registry and DSL
- Point to semantics catalog and view builders as definition authority
```

### Target Files
- Modify: `docs/architecture/*.md`
- Modify: `docs/plans/*` (obsolete references)

### Deprecations/Deletions
- Remove stale plan references to deleted modules

### Implementation Checklist
- [ ] Remove legacy registry/DSL mentions from architecture docs
- [ ] Update plan references to semantic catalog + registry_specs bridge
- [ ] Confirm documentation consistency with target state

**Status (2026-02-01):** Partial; key architecture docs updated, but additional legacy references remain.

---

## Completion Criteria

- [x] `datafusion_engine` consumes `SemanticRuntimeConfig` for semantic flow decisions
- [x] Semantic dataset specs are sourced exclusively from `semantics.catalog`
- [x] Cache policy and materialization rules reflect semantic metadata
- [x] Canonical naming is enforced at registration and materialization boundaries
- [x] Legacy view registration APIs are removed or hard-deprecated
- [x] All semantic input validation routes through a single helper
- [ ] Architecture docs match the new semantic-first boundary

---

## Suggested Verification Commands

```bash
uv run ruff check --fix
uv run pyrefly check
uv run pyright
uv run pytest tests/unit/ -v
uv run pytest tests/integration/ -v
```
