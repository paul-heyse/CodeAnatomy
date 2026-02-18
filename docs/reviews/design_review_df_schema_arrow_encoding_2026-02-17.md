# Design Review: DF Schema + Arrow + Encoding

**Date:** 2026-02-17
**Scope:** `src/datafusion_engine/schema/`, `src/datafusion_engine/arrow/`, `src/datafusion_engine/encoding/`, `src/schema_spec/`
**Focus:** Knowledge (7-11), Boundaries (1-6)
**Depth:** moderate
**Files reviewed:** 20 (representative sample across all four sub-scopes)

---

## Executive Summary

The schema, arrow, and encoding layers are well-architected in the large: `schema_spec/` provides serializable domain types, `arrow/` provides interop shims, and `schema/` provides the validation and alignment pipeline. Information hiding and dependency direction are largely respected. The three most significant issues are: (1) a parallel type-normalization system in `contracts.py` that duplicates knowledge already encoded in `type_normalization.py`; (2) an ephemeral `SessionContext()` created inside `encoding/policy.py::_datafusion_context()` on every encoding call, bypassing the canonical session lifecycle; and (3) the `SchemaContract._arrow_type_to_sql` + `_normalize_type_string` pair implementing type-string comparison logic that belongs in `type_normalization.py` and `type_resolution.py`. No DF52 migration blockers were found in the Python surface, but the `DFSchema::field()` return-type change (`&FieldRef` vs `Field`) is worth tracking against `coerce_arrow_schema` usage in `interop.py`.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `extraction_schemas.py` exports `_field_from_container` (private-by-convention) to `nested_views.py` |
| 2 | Separation of concerns | 2 | small | low | `validation.py` mixes schema policy resolution, DataFusion session management, and error aggregation in one large module |
| 3 | SRP | 2 | medium | low | `SchemaIntrospector` (662 LOC) conflates catalog reflection, query description, settings, function catalog, and fingerprinting |
| 4 | High cohesion, low coupling | 2 | small | low | `contracts.py` pulls from constraints, divergence, builders, type_normalization, type_resolution, schema_spec — 15 first-party imports |
| 5 | Dependency direction | 3 | — | — | N/A — `schema_spec/` depends on nothing internal; `arrow/` depends on `schema_spec/`; `schema/` depends on both |
| 6 | Ports & Adapters | 2 | medium | medium | `encoding/policy.py::_datafusion_context()` silently creates a naked `SessionContext()` — the session port is not injected |
| 7 | DRY | 1 | medium | medium | Type-string normalization exists in three places; type-hint-to-Arrow mapping duplicated between `field_types.py` and `type_resolution.py` |
| 8 | Design by contract | 3 | — | — | `finalize.py` contract model is strong; preconditions and postconditions are explicit |
| 9 | Parse, don't validate | 2 | small | low | `introspection_cache.py::schema_map_fingerprint` uses `getattr` duck-typing instead of a typed protocol parameter |
| 10 | Make illegal states unrepresentable | 2 | small | low | `EncodingPolicy.__post_init__` mutates frozen dataclass fields via `object.__setattr__`; the derived fields are not canonical inputs |
| 11 | CQS | 2 | small | low | `register_semantic_extension_types()` both registers side effects AND is called as a precondition inside `_extension_type()` to answer a query |

---

## Detailed Findings

### Category: Knowledge (Principles 7-11)

#### P7. DRY (knowledge, not lines) — Alignment: 1/3

**Current state:**

Type-to-string normalization exists in at least three separate locations:

1. `src/datafusion_engine/schema/type_normalization.py` — `normalize_contract_type()` (87 LOC): canonical normalizer for nested type strings (`list<...>`, `map`, `struct`).

2. `src/datafusion_engine/schema/contracts.py:492–499` — `SchemaContract._normalize_type_string()`: a private static method that calls into (1) but additionally applies its own string rewrites: `largeutf8→string`, `utf8→string`, `non-null→""`, quote stripping. These rewrites are not present in `normalize_contract_type`.

3. `src/datafusion_engine/schema/type_resolution.py:12–23` — `TYPE_HINT_TO_ARROW`: a dict mapping `"str"→pa.string()`, `"string"→pa.string()`, `"int"→pa.int64()`, etc. This is a parallel source of truth for canonical type aliases. `src/datafusion_engine/schema/field_types.py:27–66` — `resolve_field_type()` also resolves `"list<struct>"` and `"struct"` as special cases, duplicating knowledge that partly overlaps with `type_normalization.py`.

**Findings:**
- `src/datafusion_engine/schema/contracts.py:492–499`: `_normalize_type_string` implements Arrow-alias normalization (`largeutf8`, `utf8`) independently of `type_normalization.py`. Neither module knows about the other's aliases.
- `src/datafusion_engine/schema/field_types.py:51–59`: handles `"list<struct>"` and `"struct"` as special cases that `type_normalization.py` also handles (mapping `struct(...)` to `"struct"`) — the boundary between what each module owns is unclear.
- `src/datafusion_engine/schema/type_resolution.py:12–23`: `TYPE_HINT_TO_ARROW` encodes primitive type aliases. `field_types.py:17–24` maintains `_IDENTITY_FIELD_TYPES` for a subset of the same types.

**Suggested improvement:**

Consolidate all type-alias knowledge into `type_normalization.py`. Move the Arrow-alias rewrites (`largeutf8`, `utf8`, `non-null`) currently in `SchemaContract._normalize_type_string` into `normalize_contract_type`. Make `_normalize_type_string` a one-liner delegation. Remove the duplicated alias handling in `field_types.py::resolve_field_type` by delegating composite type detection to a shared helper in `type_normalization.py`.

**Effort:** medium
**Risk if unaddressed:** medium — when Arrow type names change (e.g., DF52 type-system changes), there are at least three places to update and they will drift independently.

---

#### P9. Parse, don't validate — Alignment: 2/3

**Current state:**

`src/datafusion_engine/schema/introspection_cache.py:37–54` — `schema_map_fingerprint(introspector)` accepts `object` as its parameter type and uses `getattr(introspector, "schema_map", None)` followed by a `callable` check instead of accepting a typed `SnapshotIntrospector`-style protocol.

`src/datafusion_engine/schema/introspection_core.py:108–111` — `_stable_cache_key` similarly accepts `Mapping[str, object]` and calls `to_builtins(payload)`, which may silently succeed or fail depending on the payload's actual structure.

**Findings:**
- `src/datafusion_engine/schema/introspection_cache.py:37`: `schema_map_fingerprint(introspector: object)` — the `object` type annotation means the structural contract is not expressed; the function validates via `getattr` rather than parsing at the boundary.
- `src/datafusion_engine/schema/snapshot_collector.py:13–30`: `SnapshotIntrospector` protocol exists and correctly expresses the needed interface, but `introspection_cache.py` does not use it.

**Suggested improvement:**

Change `schema_map_fingerprint(introspector: object)` to `schema_map_fingerprint(introspector: SnapshotIntrospector | SchemaIntrospector)` using the existing `SnapshotIntrospector` protocol from `snapshot_collector.py`. Remove the `getattr` duck-typing chain; the type system then enforces the interface at the call boundary.

**Effort:** small
**Risk if unaddressed:** low — the current pattern is safe at runtime but loses static-type enforcement and makes the contract invisible.

---

#### P10. Make illegal states unrepresentable — Alignment: 2/3

**Current state:**

`src/datafusion_engine/arrow/encoding.py:32–48` — `EncodingPolicy` is a `frozen=True` dataclass, but `__post_init__` mutates it with `object.__setattr__` to derive `dictionary_cols`, `dictionary_index_types`, and `dictionary_ordered_flags` from `specs`. This means the same logical state can be reached via two incompatible construction paths: provide `specs` (and have derived fields computed) or provide the derived fields directly. The `__post_init__` only populates missing derived fields, creating a possible inconsistency if callers set `specs` and also partially set `dictionary_cols`.

**Findings:**
- `src/datafusion_engine/arrow/encoding.py:32–48`: `__post_init__` uses `object.__setattr__` to bypass `frozen=True`, which is a code smell for frozen dataclasses. It also conditionally mutates only when the derived field is empty, meaning that a caller who sets `specs=` and also sets a partially overlapping `dictionary_cols=` will see an inconsistent combined state.

**Suggested improvement:**

Either: (a) make `specs` the sole canonical input and make `dictionary_cols`/`dictionary_index_types`/`dictionary_ordered_flags` computed properties rather than stored fields; or (b) introduce a named factory function `EncodingPolicy.from_specs(specs: tuple[EncodingSpec, ...]) -> EncodingPolicy` that builds the derived fields before constructing the frozen dataclass, and remove `__post_init__` mutation. Option (b) is lower churn.

**Effort:** small
**Risk if unaddressed:** low — practically speaking the construction paths are not mixed, but the dataclass invariant is violated by the `object.__setattr__` pattern.

---

#### P11. CQS — Alignment: 2/3

**Current state:**

`src/datafusion_engine/arrow/semantic.py:119–135` — `register_semantic_extension_types()` is a command (registers side effects with PyArrow's global registry). It is called inside `_extension_type(info)`, which is itself called by queries like `span_type()`, `byte_span_type()`, etc. Callers asking "give me the span type" trigger a global registration as a side effect.

**Findings:**
- `src/datafusion_engine/arrow/semantic.py:134`: `_extension_type(info)` calls `register_semantic_extension_types()` on every invocation of any type accessor. Registration is idempotent but each call enters `contextlib.suppress` blocks for every type in the registry — this is unnecessary overhead and mixes query and command semantics.
- The pattern obscures whether extension types are registered: callers cannot tell if they need to call `register_semantic_extension_types()` explicitly, so some may call it redundantly.

**Suggested improvement:**

Use a module-level `_registered: bool = False` flag (or `functools.lru_cache` on a `_ensure_registered()` helper) to make `register_semantic_extension_types()` a true one-time setup command. Type accessors like `span_type()` then call `_ensure_registered()` and return their type — separating the concern but keeping convenience. Alternatively, make the top-level module `__init__` of `arrow/` call `register_semantic_extension_types()` once.

**Effort:** small
**Risk if unaddressed:** low — functionally correct due to `contextlib.suppress`, but adds repeated overhead and violates CQS clarity.

---

### Category: Boundaries (Principles 1-6)

#### P1. Information hiding — Alignment: 2/3

**Current state:**

`src/datafusion_engine/schema/extraction_schemas.py` exports `_field_from_container` (named with a leading underscore, signaling it is private-by-convention) but it is imported by `src/datafusion_engine/schema/nested_views.py:19`:

```python
from datafusion_engine.schema.extraction_schemas import (
    _field_from_container,
    extract_schema_for,
)
```

This breaks the information-hiding contract: `nested_views.py` directly depends on an internal implementation detail of `extraction_schemas.py`.

**Findings:**
- `src/datafusion_engine/schema/nested_views.py:19`: imports `_field_from_container` — a private helper — from a sibling module in the same package.
- `src/datafusion_engine/schema/extraction_schemas.py`: does not export `_field_from_container` in its `__all__`, confirming it is intentionally private.

**Suggested improvement:**

Promote `_field_from_container` to the public interface of `extraction_schemas.py` (rename to `field_from_container`, add to `__all__`) if it is genuinely part of the module's stable surface; or move the functionality to a shared `_schema_helpers.py` within `datafusion_engine/schema/` that both modules can depend on.

**Effort:** small
**Risk if unaddressed:** low — a refactor of `extraction_schemas.py` internals may unintentionally break `nested_views.py`.

---

#### P2. Separation of concerns — Alignment: 2/3

**Current state:**

`src/datafusion_engine/schema/validation.py` (752 LOC) mixes three distinct concerns in a single module:
1. Session management (creating `SessionContext`, registering/deregistering temp tables).
2. DataFusion query expression construction (null checks, cast checks, duplicate detection using DataFusion `col`/`lit`/`f.sum`).
3. Schema-level validation report construction (`_build_validation_report`, `SchemaValidationReport`).

The DataFusion query execution and the schema validation report are readable only together, making it difficult to test the report-building logic independently of a live DataFusion session.

**Findings:**
- `src/datafusion_engine/schema/validation.py:112–115`: `_session_context()` creates bare `SessionContext()` when no runtime profile is available — this is the same hidden-session creation problem as in `encoding/policy.py`.
- `src/datafusion_engine/schema/validation.py:596–623`: `_prepare_validation_context` both creates a session and registers temp tables — these are two separable responsibilities.
- The report-building code at lines 505–530 is pure (no IO) but is buried inside a module whose primary job is session-dependent query execution.

**Suggested improvement:**

Extract `_build_validation_report` and `SchemaValidationReport` to `schema/validation_report.py` (pure, no DataFusion). Keep DataFusion session logic in `schema/validation.py`. This enables testing report construction without a live session. The session-creation concern (bare `SessionContext()` fallback) should be moved to a shared helper or require an injected session.

**Effort:** medium
**Risk if unaddressed:** low — the current code is correct; testability cost is the primary risk.

---

#### P3. SRP — Alignment: 2/3

**Current state:**

`src/datafusion_engine/schema/introspection_core.py` (662 LOC) — `SchemaIntrospector` is a frozen dataclass that serves as the primary interface for:
- Table inventory (`tables_snapshot`, `schemata_snapshot`, `columns_snapshot`)
- Column metadata (`table_columns`, `table_columns_with_ordinal`, `table_column_names`, `table_column_defaults`)
- Function/routine catalog (`routines_snapshot`, `parameters_snapshot`, `function_catalog_snapshot`, `function_names`)
- Session settings (`settings_snapshot`)
- Schema fingerprinting (`schema_map`, delegated to `introspection_cache.py`)
- Constraint inspection (`table_constraints`)
- Query description (`describe_query`)
- Disk caching (`invalidate_cache`)

That is at least five distinct responsibilities. The class changes when table-inventory logic changes, when function-catalog logic changes, when caching policy changes, or when constraint handling changes.

**Findings:**
- `src/datafusion_engine/schema/introspection_core.py:172–639`: `SchemaIntrospector` has 14 public methods spanning catalog, routine, settings, constraints, and fingerprinting concerns.
- The split into `introspection_helpers.py`, `introspection_routines.py`, `introspection_delta.py`, `introspection_cache.py`, `snapshot_collector.py` was a good first step — these helpers are extracted — but `SchemaIntrospector` still acts as a God object that wires them all together.

**Suggested improvement:**

Split `SchemaIntrospector` into focused facades:
- `CatalogIntrospector`: tables, schemata, columns, settings (thin wrapper over `IntrospectionSnapshot`).
- `RoutineIntrospector`: routines, parameters, function catalog (thin wrapper over `introspection_routines.py`).
- `ConstraintIntrospector`: constraints, key fields (thin wrapper over `introspection_delta.py`).

Each facade receives a `snapshot: IntrospectionSnapshot` and optionally a `ctx: SessionContext`. This aligns with the existing helper-module decomposition.

**Effort:** medium
**Risk if unaddressed:** low — the class works correctly; the cost is increased change-blast-radius when any one of the five concerns evolves.

---

#### P4. High cohesion, low coupling — Alignment: 2/3

**Current state:**

`src/datafusion_engine/schema/contracts.py` (656 LOC) has 15 first-party imports at module level:
`ConstraintSpec`, `ConstraintType`, `TableConstraints`, `constraint_key_fields`, `delta_check_constraints`, `delta_constraints_for_location`, `merge_constraint_expressions`, `normalize_column_names`, `table_constraint_definitions`, `table_constraints_from_location`, `table_constraints_from_spec` (from `constraints.py`); plus `SchemaDivergence`, `compute_schema_divergence` (from `divergence.py`); plus builders, introspection, type_normalization, type_resolution, schema_spec, validation.

The `__all__` re-exports all of these, making `contracts.py` act as an omnibus re-export module for the entire schema subsystem rather than a focused contract-definition module.

**Findings:**
- `src/datafusion_engine/schema/contracts.py:634–656`: `__all__` re-exports 22 symbols from 7 different source modules, making it a "barrel" module that increases coupling surface.
- Callers who `from datafusion_engine.schema.contracts import delta_check_constraints` now depend on `contracts.py` even though the symbol lives in `constraints.py`.

**Suggested improvement:**

Restrict `contracts.py::__all__` to the symbols that `contracts.py` defines directly: `SchemaContract`, `ContractRegistry`, `EvolutionPolicy`. Remove the re-exports of constraint and divergence symbols; callers should import from their authoritative modules. This reduces `contracts.py`'s coupling surface and makes import paths intention-revealing.

**Effort:** small (may require updating callers)
**Risk if unaddressed:** low — the re-exports are stable but inflate the apparent surface of `contracts.py`.

---

#### P6. Ports & Adapters — Alignment: 2/3

**Current state:**

`src/datafusion_engine/encoding/policy.py:138–139`:

```python
def _datafusion_context() -> SessionContext:
    return SessionContext()
```

This private helper creates a brand-new, unconfigured `SessionContext` every time `apply_encoding` is called. The session lifecycle (UDF registration, catalog bindings, memory pool, runtime config) is completely bypassed. This is the same anti-pattern noted in `validation.py:112–115`.

**Findings:**
- `src/datafusion_engine/encoding/policy.py:83`: `apply_encoding` calls `_datafusion_context()` to get a session for the `temp_table` context manager and `df_ctx.table(table_name)` query. The session has no UDFs, no catalog, and uses default memory limits.
- `src/datafusion_engine/encoding/policy.py:90–93`: the temp-table encode path uses `df.select(*selections).to_arrow_table()` — a DataFusion query — on a completely default session. For dictionary encoding (a pure cast), this could instead use PyArrow's `pa.Table.cast` or `pc.cast`, avoiding the DataFusion session entirely.
- The same pattern appears in `validation.py:112–115`.

**Suggested improvement for encoding:**

For `apply_encoding`, the DataFusion session is used only to perform a `SELECT col, CAST(col AS DICT(...))` operation. This is equivalent to `pa.Table.cast` with a dictionary target schema, which is a pure PyArrow operation requiring no session. Replace the DataFusion path with a PyArrow-native dictionary cast:

```python
# Replace the temp_table + df.select path with:
arrays = []
for field in resolved.schema:
    col_array = resolved.column(field.name)
    if field.name in policy.dictionary_cols and not patypes.is_dictionary(field.type):
        dict_type = pa.dictionary(index_type, field.type, ordered=ordered)
        col_array = col_array.dictionary_encode().cast(dict_type)
    arrays.append(col_array)
return pa.table(dict(zip(resolved.column_names, arrays)), schema=target_schema)
```

This eliminates the hidden `SessionContext()` entirely.

**Effort:** small
**Risk if unaddressed:** medium — the hidden `SessionContext()` bypasses runtime configuration (memory pool, object store, UDFs) and creates overhead on every encoding call. If encoding is called in a hot path, session creation cost accumulates.

---

## DF52 Migration Impact

**No breaking-change blockers found in the Python surface.** However, two areas are worth tracking:

1. **`DFSchema::field()` now returns `&FieldRef` (DF52 change J.1).** The Python binding surfaces this as returning schema fields; `src/datafusion_engine/arrow/interop.py:50–69` — `coerce_arrow_schema` — calls `df.schema()` and then iterates fields. If the Python binding begins returning `FieldRef` objects instead of `Field` objects, the iteration in `coerce_arrow_schema` would need `Arc::clone` semantics. Monitor when `datafusion-python` updates its bindings to the DF52 Rust core.

2. **`TableSchema::new(file_schema, table_partition_cols)` (DF52 schema plumbing refactor).** The scope does not implement a custom `TableProvider`, so the `TableSchema` constructor change (DF52 deep dive section L) does not directly apply. However, if partition-column handling is ever added to the extraction table providers, the DF52 `FilePruner::try_new()` change (dropping `partition_fields`) and the `replace_columns_with_literals()` migration path will apply.

3. **`CoalesceBatchesExec` removal (DF52 section H).** The `ChunkPolicy` in `src/datafusion_engine/arrow/chunking.py` may have been defensive-coded against `CoalesceBatchesExec` behavior. Verify that chunking behavior matches expectations after DF52 upgrade, since coalescing is now operator-embedded rather than explicit.

---

## Planning-Object Consolidation

**P6-finding expanded:** The primary bespoke-vs-built-in tension in this scope is `apply_encoding` in `src/datafusion_engine/encoding/policy.py`. It uses DataFusion (a query engine) to perform what is a pure Arrow array operation: dictionary-encoding selected columns.

Assessment per the audit criteria:
- (b) **DF52 features make custom code unnecessary**: PyArrow `pa.ChunkedArray.dictionary_encode()` and `pa.Array.cast(pa.dictionary(...))` already handle this. No DataFusion involvement needed.
- (c) **DataFusion's built-in planning/environment objects replace bespoke code**: Not applicable here — the fix is to use PyArrow directly, not to use more DataFusion.
- (d) **Custom caching vs `df.cache()`**: Not applicable.

**LOC reduction estimate:** Replacing the `temp_table` + `_datafusion_context()` + `df.select` path with a pure PyArrow cast path eliminates approximately 30 LOC in `encoding/policy.py` and removes the `_datafusion_context` function entirely.

For `validation.py`, the DataFusion session usage (null-check queries, duplicate-count queries, row filtering) is appropriate — these are genuine relational operations. The issue there is not the DataFusion usage but the hidden session creation. A `ctx: SessionContext` parameter should be passed in rather than created internally.

---

## Rust Migration Candidates

**No dedicated Rust schema layer exists** in the current codebase (`codeanatomy_engine/src/schema/` referenced in the agent brief does not exist under `rust/datafusion_ext/src/`). The Rust crate (`rust/datafusion_ext/`) focuses on UDF registration, delta operations, and expression planning.

The following Python functions are candidates for future migration to Rust if throughput becomes a concern:

1. **`src/datafusion_engine/schema/alignment.py::align_to_schema`** — Schema alignment (column reordering, type casting) is currently delegated to `align_table_to_schema` in `session/runtime_dataset_io.py`. A Rust `SchemaAdapter` (DataFusion `PhysicalExprAdapter` pattern) would perform this at the `TableProvider` boundary without Python overhead.

2. **`src/datafusion_engine/schema/type_normalization.py::normalize_contract_type`** — Pure string parsing logic; a good Rust migration candidate if type normalization becomes a hot path (unlikely currently).

3. **`src/datafusion_engine/arrow/coercion.py::storage_type` + `storage_schema`** — Recursive Arrow type normalization (extension → storage, list-view → list, etc.) is well-suited to Rust where `DataType` is `Clone + Eq`.

None of these are urgent — Python overhead is not the bottleneck in schema validation paths.

---

## Cross-Cutting Themes

### Theme 1: Hidden Session Creation (Affects P2, P6)

Three places in the scope create bare `SessionContext()` without configuration:
- `src/datafusion_engine/encoding/policy.py:139`
- `src/datafusion_engine/schema/validation.py:115`
- `src/datafusion_engine/schema/validation.py` (line 603 via `_session_context`)

**Root cause:** DataFusion operations that could be done in a caller-provided session are instead given an ephemeral local session. This bypasses memory pool configuration, UDF registration, and runtime profile settings.

**Approach:** Define a `session_context_for_schema_ops(runtime_profile: DataFusionRuntimeProfile | None) -> SessionContext` utility in `datafusion_engine/session/` that either extracts the session from a profile or creates a minimal default. All three sites then call this single utility, making the fallback explicit rather than silently scattered.

### Theme 2: Type-String Knowledge Scattered Across Four Modules (Affects P7)

`type_normalization.py`, `type_resolution.py`, `field_types.py`, and `contracts.py::_normalize_type_string` all encode overlapping knowledge about Arrow/SQL type names and their canonical aliases. This is a DRY violation at the knowledge level: when Arrow renames types or when the type system evolves, the fix must be applied in multiple locations.

**Approach:** Establish `type_normalization.py` as the single canonical module for type-string normalization, expand its alias table to cover the Arrow-alias cases currently in `contracts.py`, and have `type_resolution.py::arrow_type_to_sql` and `field_types.py::resolve_field_type` delegate their alias resolution to it.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P6 Ports & Adapters | Replace `_datafusion_context()` + temp-table path in `encoding/policy.py` with PyArrow-native dictionary cast | small | Eliminates hidden session creation + reduces LOC |
| 2 | P7 DRY | Merge Arrow-alias rewrites from `contracts.py::_normalize_type_string` into `type_normalization.py::normalize_contract_type` | small | Single source of truth for type aliases |
| 3 | P1 Info hiding | Rename `_field_from_container` to public `field_from_container` and add to `extraction_schemas.py::__all__` | small | Resolves private-symbol import |
| 4 | P9 Parse/validate | Change `introspection_cache.py::schema_map_fingerprint(introspector: object)` to use `SnapshotIntrospector` protocol | small | Type-safe boundary |
| 5 | P4 Cohesion/coupling | Remove re-exports from `contracts.py::__all__`; expose only `SchemaContract`, `ContractRegistry`, `EvolutionPolicy` | small | Reduces coupling surface |

---

## Recommended Action Sequence

1. **[P6, encoding]** Replace `encoding/policy.py::apply_encoding`'s DataFusion temp-table path with a pure PyArrow dictionary-cast loop. Remove `_datafusion_context`. Verify with existing tests.

2. **[P7, type normalization]** Move the Arrow alias rewrites (`largeutf8→string`, `utf8→string`, `non-null`) from `contracts.py::_normalize_type_string` into `type_normalization.py::normalize_contract_type`. Update `_normalize_type_string` to be a one-liner delegation. Update `type_resolution.py::arrow_type_to_sql` to delegate alias resolution to the same function.

3. **[P1, info hiding]** Make `_field_from_container` public in `extraction_schemas.py` (rename and `__all__` entry). Update `nested_views.py` import.

4. **[P4, coupling]** Shrink `contracts.py::__all__` to the three types that module defines. Update any callers that relied on the re-exports to import from the authoritative source modules.

5. **[P9, parse boundary]** Update `introspection_cache.py::schema_map_fingerprint` signature to accept `SnapshotIntrospector` protocol from `snapshot_collector.py`.

6. **[P11, CQS]** Add a `_ensure_registered()` guard in `arrow/semantic.py` (module-level bool or `functools.lru_cache`) so type accessors do not trigger repeated registration side effects.

7. **[P6, session]** Extract `_session_context(runtime_profile)` from `validation.py` into a shared `session/` utility (`session_context_for_schema_ops`). Update `validation.py` and any other call sites to use it.

8. **[P3, SRP — deferred]** Split `SchemaIntrospector` into `CatalogIntrospector`, `RoutineIntrospector`, `ConstraintIntrospector` after validating that the smaller facades cover all existing call sites.
