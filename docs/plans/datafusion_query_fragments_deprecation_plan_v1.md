# DataFusion Query Fragments Deprecation Plan v1

## Goal
Fully remove static fragment payloads (`query_fragments.py` / `query_fragments_ast.py`) and replace them with a **dynamic, AST-first, schema-driven view registry** built at runtime from the canonical schema registry and DataFusion capabilities.

## Design end-state (best-in-class)
- View definitions are **generated dynamically** from schema specs and feature gates.
- All view construction uses **SQLGlot AST builders** (or Ibis IR) and renders through policy.
- No serialized AST payloads or static fragment SQL remain in the codebase.
- View registration is **centralized**, deterministic, and schema-validated.

---

# Scope Items

## 1) Introduce a dynamic view registry (replaces query fragments)

**Why**
The static fragment payloads are brittle, duplicate schema intent, and break the AST-first contract. A registry derived from `schema_registry` + SQLGlot/Ibis is canonical and self-healing.

**Representative pattern (target)**
```python
from datafusion_engine.schema_registry import nested_view_specs
from datafusion_engine.view_registry import attrs_view_specs, diagnostics_view_specs

def registry_view_specs(ctx, *, ast_optional_disabled=()) -> tuple[ViewSpec, ...]:
    views = []
    views.extend(nested_view_specs())
    views.extend(attrs_view_specs(ctx, exclude=ast_optional_disabled))
    views.extend(diagnostics_view_specs(ctx))
    return tuple(views)
```

**Target files**
- New: `src/datafusion_engine/view_registry.py` (or similar)
- Update: `src/datafusion_engine/runtime.py` to call the new registry
- Update: `src/datafusion_engine/schema_registry.py` if helpers are shared

**Deletes**
- None (this introduces new runtime sources of truth)

**Implementation checklist**
- [ ] Create a new `registry_view_specs(...)` function that composes nested + attrs + diagnostics view specs.
- [ ] Ensure view ordering is deterministic and stable.
- [ ] Validate schemas via `ViewSpec.validate()` post-registration.

---

## 2) Replace static “attrs” fragments with generic AST builders

**Why**
`*_attrs` views are systematic projections of `attrs` maps and can be generated from the schema registry with a single builder.

**Representative pattern (target)**
```python
from sqlglot_tools.compat import exp
from sqlglot_tools.optimizer import resolve_sqlglot_policy, sqlglot_emit

def attrs_view_sql(base: str, *, id_cols: tuple[str, ...]) -> str:
    policy = resolve_sqlglot_policy(name="datafusion_compile")
    entries = exp.Anonymous(this="map_entries", expressions=[exp.column("attrs")])
    unnest = exp.Anonymous(this="unnest", expressions=[entries])
    from_ = exp.From(
        expressions=[
            exp.Table(this=exp.to_identifier(base)),
            exp.Lateral(this=unnest, alias=exp.TableAlias(this=exp.to_identifier("entry"))),
        ]
    )
    select_cols = [exp.column(col) for col in id_cols] + [
        exp.column("key", table="entry").as_("attr_key"),
        exp.column("value", table="entry").as_("attr_value"),
    ]
    query = exp.Select(expressions=select_cols, from_=from_)
    return sqlglot_emit(query, policy=policy)
```

**Target files**
- New: `src/datafusion_engine/view_registry.py` (attrs view builders)
- Update: `src/datafusion_engine/schema_registry.py` to map view → base table + id cols

**Deletes**
- `cst_*_attrs_sql` helpers in `src/datafusion_engine/query_fragments.py`
- `*_attrs` payloads in `query_fragments_ast.py`

**Implementation checklist**
- [ ] Define a single `attrs_view_spec(name, base, id_cols)` builder.
- [ ] Build a map of attr views (AST + CST + bytecode + symtable).
- [ ] Gate on `map_entries` availability (existing runtime gates).
- [ ] Ensure view schemas match `*_attrs` schema definitions.

---

## 3) Replace static diagnostics/check fragments with dynamic builders

**Why**
Diagnostics/check views should be generated from the canonical schema and runtime feature gates to avoid drift.

**Representative pattern (target)**
```python
def diagnostics_view_specs(ctx) -> tuple[ViewSpec, ...]:
    return (
        view_spec_from_sql(
            ctx,
            name="cst_schema_diagnostics",
            sql=build_cst_schema_diagnostics_sql(),
        ),
        # tree-sitter checks, span metadata, etc.
    )
```

**Target files**
- New: `src/datafusion_engine/view_registry.py`
- Update: `src/datafusion_engine/schema_registry.py` if shared helpers are needed

**Deletes**
- Corresponding fragment payloads in `query_fragments_ast.py`

**Implementation checklist**
- [ ] Enumerate diagnostics/check view names and build SQLGlot AST builders for each.
- [ ] Use `resolve_sqlglot_policy(name="datafusion_compile")` for rendering.
- [ ] Maintain existing view names to preserve downstream contracts.

---

## 4) Replace runtime registration wiring

**Why**
`DataFusionRuntimeProfile._install_schema_registry` currently imports fragment views; this must be routed through the new dynamic registry.

**Representative pattern (target)**
```python
from datafusion_engine.view_registry import registry_view_specs

fragment_views = registry_view_specs(ctx, ast_optional_disabled=ast_optional_disabled)
self._register_schema_views(ctx, fragment_views=fragment_views)
```

**Target files**
- Update: `src/datafusion_engine/runtime.py`

**Deletes**
- Import of `fragment_view_specs` from `query_fragments`

**Implementation checklist**
- [ ] Replace fragment view registration with registry-based view specs.
- [ ] Preserve `ast_optional_disabled` behavior.
- [ ] Keep diagnostics hooks unchanged (validation, telemetry).

---

## 5) Delete query fragments modules

**Why**
Static payloads are no longer needed and are actively counter to the AST-first architecture.

**Deletes**
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/query_fragments_ast.py`

**Target files**
- Update: any imports or references (search for `query_fragments`).

**Implementation checklist**
- [ ] Remove all imports and references to `query_fragments`.
- [ ] Delete both files.
- [ ] Ensure view registration still passes validation.

---

## 6) Update tests and diagnostics expectations

**Why**
Dynamic builders change the source of truth for view definitions; tests must validate behavior, not static payloads.

**Representative pattern (target)**
```python
def test_registry_view_specs_covers_cst_attrs() -> None:
    names = {spec.name for spec in registry_view_specs(ctx)}
    assert "cst_defs_attrs" in names
```

**Target files**
- Update: tests that assert fragment registration or specific SQL payloads
- Add: tests covering registry view spec coverage

**Deletes**
- Any tests tied directly to static fragment payloads.

**Implementation checklist**
- [ ] Add tests for registry coverage and schema validation.
- [ ] Remove tests that couple to serialized AST payloads.
- [ ] Validate `CST_VIEW_NAMES` / `AST_VIEW_NAMES` coverage with dynamic registry.

---

# Deletions Summary

**Files**
- `src/datafusion_engine/query_fragments.py`
- `src/datafusion_engine/query_fragments_ast.py`

**Functions**
- `fragment_view_specs`
- `_fragment_df_builder`
- `_normalize_fragment_expr`
- `_fragment_sql`
- `cst_*_attrs_sql` helpers
- `FRAGMENT_AST_PAYLOADS`

---

# Verification Checklist
- [ ] Schema registry installs all views in `AST_VIEW_NAMES`, `CST_VIEW_NAMES`, `TREE_SITTER_VIEW_NAMES`, `SCIP_VIEW_NAMES`, `BYTECODE_VIEW_NAMES`, `SYMTABLE_VIEW_NAMES`.
- [ ] View schema validation passes via `ViewSpec.validate()`.
- [ ] Diagnostics hooks continue to emit expected artifacts.
- [ ] No references to `query_fragments` remain.
- [ ] `uv run ruff check --fix`, `uv run pyrefly check`, `uv run pyright --warnings --pythonversion=3.13` pass.
