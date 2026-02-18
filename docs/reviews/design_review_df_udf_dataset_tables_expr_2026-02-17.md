# Design Review: src/datafusion_engine/udf/, dataset/, tables/, expr/

**Date:** 2026-02-17
**Scope:** `src/datafusion_engine/udf/`, `src/datafusion_engine/dataset/`, `src/datafusion_engine/tables/`, `src/datafusion_engine/expr/`
**Focus:** Boundaries (1-6), Knowledge (7-11), Composition (12-15)
**Depth:** deep
**Files reviewed:** 42 (all Python files in scope)

---

## Executive Summary

The four directories form a competent, modular DataFusion integration layer that is generally well-decomposed. The `udf/` package is the most structurally complex: it evolved through extraction of modules from a single large file, leaving a layered re-export pattern (`extension_core` → `extension_snapshot_runtime` → `extension_validation` → back through `extension_core`) that creates a circular import structure managed via deferred local imports — a pattern that hides coupling and violates information-hiding boundaries. The `dataset/registration_core.py` acts as an aggregator facade with 30+ public aliases pointing to symbols imported from seven other modules, blurring ownership. The `expr/spec.py` is a high-quality functional core but is large (775 LOC) and mixes two distinct concerns: the literal/IR type system and the call-dispatch lookup tables. DF52 impact is real but confined: `AggregateUDFImpl` opt-in methods (`supports_within_group_clause`, `supports_null_handling_clause`) are not surfaced in Python-side code, which is correct since UDF implementation lives in Rust, but the Python snapshot normalization layer does not document this gap. `ExprPlanner` is in use and correctly wired; `RelationPlanner` (new in DF52) is mentioned in `platform.py` docstring but not yet implemented. The highest-priority improvements are: (1) resolving the circular import chain in `udf/`, (2) eliminating the `registration_core` alias-only facade, and (3) splitting `expr/spec.py` into IR types and dispatch tables.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | medium | `extension_core.py` re-exports via bottom-of-file import from `extension_snapshot_runtime`; `registration_core.py` exposes 30+ aliases pointing at internals of seven other modules |
| 2 | Separation of concerns | 2 | small | low | `expr/spec.py` mixes IR type definitions with call-dispatch tables; `dataset/registration_core.py` mixes registration logic with alias aggregation |
| 3 | SRP | 2 | small | low | `udf/metadata.py` (1229 LOC) combines UDF spec types, FunctionCatalog, planner snapshot, and rewrite-tag index — at least three distinct concerns |
| 4 | High cohesion, low coupling | 1 | medium | medium | Circular import chain across `extension_core` ↔ `extension_snapshot_runtime` ↔ `extension_validation` enforced via deferred local imports; `registration_core` imports from 7+ modules and re-exports their private internals |
| 5 | Dependency direction | 2 | small | low | Core UDF snapshot logic (`extension_validation.py`) depends on `extension_core.py` for `extension_module_with_capabilities`, creating mild inward-dependency bleed |
| 6 | Ports & Adapters | 2 | small | low | `expr/planner.py` correctly uses an adapter (dynamically loaded extension module) with a stable port; `RelationPlanner` mentioned but no port declared |
| 7 | DRY | 2 | small | low | Arrow-to-DDL type mapping is split between `dataset/ddl_types.py` and `udf/extension_ddl.py::_ddl_type_name_from_arrow`; two independent lookup paths for the same mapping knowledge |
| 8 | Design by contract | 2 | small | low | `ExprIR.__post_init__` and `ExprSpec.__post_init__` enforce construction contracts well; `udf_expr` kwargs-to-positional coercion (`call_args.extend(kwargs.values())`) silently loses keyword semantics |
| 9 | Parse, don't validate | 2 | small | low | `normalize_runtime_install_snapshot` is a clean boundary parser; scattered `isinstance`-chains in snapshot normalization are partially duplicated across `_normalize_registry_snapshot` in `extension_core` and `_empty_registry_snapshot` in `extension_snapshot_runtime` |
| 10 | Make illegal states unrepresentable | 2 | small | low | `ExprSpec` enforces `sql or expr_ir` via `__post_init__`; `UdfTier = Literal["builtin"]` — a single-value literal type — signals dead-code paths in `resolve_by_tier` and `list_functions_by_tier` |
| 11 | CQS | 1 | small | medium | `register_dataset_df` both registers a table (command) and returns a DataFrame (query); `register_rust_udfs` mutates registries and returns a snapshot; `register_table` creates side-effects and returns `DataFrame` |
| 12 | Dependency inversion + explicit composition | 2 | small | low | `ensure_rust_udfs` constructs its `InstallRustUdfPlatformRequestV1` from `msgspec.to_builtins(RustUdfPlatformOptions(...))` — serializes then deserializes to bypass the typed API |
| 13 | Prefer composition over inheritance | 3 | — | — | No problematic inheritance; `StructBaseCompat`/`StructBaseStrict` used appropriately for msgspec boundaries |
| 14 | Law of Demeter | 2 | small | low | `_build_registration_context` accesses `runtime_profile.policies.schema_hardening.enable_view_types` (three hops); `_schema_hardening_view_types` correctly encapsulates one layer but callers chain further |
| 15 | Tell, don't ask | 2 | small | low | `UdfCatalog.resolve_function` exposes internal `FunctionCatalog` inspection logic rather than delegating resolution to `FunctionCatalog` itself |

---

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information Hiding — Alignment: 1/3

**Current state:**
Two distinct violations form the core of this category's gap.

**Findings:**

- `src/datafusion_engine/udf/extension_core.py:360-380`: The bottom of `extension_core.py` contains module-level import statements that import from `extension_snapshot_runtime`, which itself imports from `extension_core` at module level. This creates a deferred-import cycle: the module's body imports from a module that imports from it. The pattern is currently "managed" by placing the importing statements at the very bottom of `extension_core.py` and using deferred (`from ... import`) calls inside function bodies in `extension_snapshot_runtime.py` (lines 43-230). This means the public API of `extension_core` is only fully available after the entire module body has executed — a hidden startup ordering constraint that is not visible to callers.

- `src/datafusion_engine/dataset/registration_core.py:616-647`: The module defines a block of 30+ module-level alias assignments (e.g., `DataFusionCacheSettings = _DataFusionCacheSettings`, `register_dataset_df` is a genuine function but `record_delta_cdf_artifact = _record_delta_cdf_artifact` re-publishes a private symbol imported from `registration_delta_helpers.py`). These aliases expose internal helpers from six other submodules as if they were owned by `registration_core`. This blurs module ownership: callers importing from `registration_core` receive implementation details that belong to seven different modules, making it impossible to understand the module boundary without reading the entire alias block.

**Suggested improvement:**
For `udf/`: consolidate `extension_snapshot_runtime.py` and `extension_core.py` into a single `extension_runtime.py` module, placing all the mutually-dependent logic together and eliminating the cycle. `extension_validation.py` then depends cleanly on `extension_runtime.py` for the `extension_module_with_capabilities` accessor, which is its only genuine dependency on `extension_core`. For `dataset/`: eliminate the alias block entirely. Any module that truly needs the helpers from `registration_delta_helpers.py` should import them directly from that module. Reserve `registration_core.py` for the public registration API (`register_dataset_df`, `DataFusionRegistrationContext`, `DataFusionRegistryOptions`, and their direct dependencies).

**Effort:** medium
**Risk if unaddressed:** medium — the circular import is currently stable but is a maintenance trap: adding any import at module level in `extension_core` that transitively imports `extension_snapshot_runtime` will produce a silent ImportError or partially-initialized module error at runtime.

---

#### P2. Separation of Concerns — Alignment: 2/3

**Findings:**

- `src/datafusion_engine/expr/spec.py:616-723`: The module correctly encapsulates the expression IR type system (`ExprIR`, `ExprSpec`, `ScalarLiteralSpec` hierarchy) in its first ~340 lines. The second half (~400 lines) is a flat dispatch registry: two dicts (`_EXACT_CALL_COUNTS`, `_MIN_CALL_COUNTS`), one large dict (`_EXPR_CALLS`), one smaller dict (`_SQL_CALLS`), and ~30 private handler functions. The dispatch registry's domain is "which Python function implements each named operator" — a routing concern that changes for different reasons than the type system. If a new operator is added, both the type system and the dispatch table change; if the expression compiler changes backends, only the dispatch changes.

- `src/datafusion_engine/dataset/registration_core.py` is also a concern-mixing module: it contains the data model for `DataFusionRegistrationContext`, the resolution logic (`_resolve_registry_options_for_location`), the cache policy resolution logic, and a 30-alias re-export block. These are three different concerns.

**Suggested improvement:**
Extract `_EXPR_CALLS`, `_SQL_CALLS`, `_EXACT_CALL_COUNTS`, `_MIN_CALL_COUNTS`, and all `_expr_*` / `_sql_*` private functions from `expr/spec.py` into a new `expr/dispatch.py`. `ExprIR.to_expr()` and `ExprIR.to_sql()` delegate to `_call_expr` and `_render_call` respectively — make these the boundary. The type system (`ScalarLiteralSpec`, `ExprIR`, `ExprSpec`) stays in `spec.py` and imports from `dispatch.py`.

**Effort:** small
**Risk if unaddressed:** low — the current design works and the file is readable, but the 775-LOC file will continue growing as new operators are added.

---

#### P3. SRP — Alignment: 2/3

**Findings:**

- `src/datafusion_engine/udf/metadata.py` (1229 LOC) owns at minimum three distinct concerns: (a) the `DataFusionUdfSpec` / `DataFusionUdfSpecSnapshot` value types and their Arrow round-trip logic (lines 49-165); (b) the `FunctionCatalog` and `UdfCatalog` runtime resolution classes (lines 312-918); (c) planner snapshot utilities (`udf_planner_snapshot`, `rewrite_tag_index`, `datafusion_udf_specs`, `_registry_*` helpers) used by the FunctionFactory policy path (lines 920-1155). These change for independent reasons: spec serialization changes when the Arrow type system changes; resolution classes change when tier policy evolves; planner snapshot changes when the Rust extension protocol changes.

**Suggested improvement:**
Split into: `udf/spec_types.py` (DataFusionUdfSpec, DataFusionUdfSpecSnapshot, serialization), `udf/catalog.py` (FunctionCatalog, UdfCatalog, UdfCatalogAdapter, and factory functions), `udf/planner_metadata.py` (udf_planner_snapshot, rewrite_tag_index, datafusion_udf_specs). `metadata.py` becomes a compatibility re-export shim.

**Effort:** medium
**Risk if unaddressed:** low — the file is internally coherent, but it is the largest single file in scope and will attract further growth.

---

#### P4. High Cohesion, Low Coupling — Alignment: 1/3

**Findings:**

- The circular dependency between `extension_core`, `extension_snapshot_runtime`, and `extension_validation` is the most significant cohesion/coupling violation in the entire scope. The import graph is:
  - `extension_core.py` (bottom of file): imports `_register_udf_aliases`, `_register_udf_specs` from `extension_ddl.py` and 16 symbols from `extension_snapshot_runtime.py`.
  - `extension_snapshot_runtime.py` (module top): imports from `extension_core.py` (`EXTENSION_MODULE_LABEL`, `AsyncUdfPolicy`, `ExtensionRegistries`, `_datafusion_internal`, `_invoke_runtime_entrypoint`, `_resolve_registries`).
  - `extension_snapshot_runtime.py` (inside function bodies): imports from `extension_validation.py` (12 different locations, e.g. lines 43, 49, 55, 61, 67, 75, 81, 87, 93, 99, 183, 199, 211, 227).
  - `extension_validation.py` (module top): imports from `extension_core.py` (`expected_plugin_abi`, `extension_module_with_capabilities`, `invoke_runtime_entrypoint`).

  This forms a triangle: `extension_core` ↔ `extension_snapshot_runtime` ↔ `extension_validation` ↔ `extension_core`. The deferred local imports in `extension_snapshot_runtime.py` are the workaround for what would otherwise be a Python `ImportError`. The coupling is real even if Python's import system tolerates it.

- `registration_core.py` is coupled to seven other modules via its alias block: `registration_delta_helpers`, `registration_provider`, `registration_projection`, `registration_validation`, `registration_scan`, `registration_cache`, `registration_listing`. These are not its collaborators — they are modules whose internals it re-publishes.

**Suggested improvement:**
See P1 suggestion for `udf/`. For `dataset/`, remove the alias block from `registration_core.py` and let each submodule own its exports directly.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P5. Dependency Direction — Alignment: 2/3

**Findings:**

- `src/datafusion_engine/udf/extension_validation.py:12-18`: imports `expected_plugin_abi`, `extension_module_with_capabilities`, `invoke_runtime_entrypoint` from `extension_core.py`. These three functions ultimately load the Rust extension module and are I/O-adjacent (they perform dynamic module loading). Validation logic should depend on pure types and stable contracts, not on module-loading infrastructure. The validation function `validate_extension_capabilities` (lines 232-273) would be better served by receiving a pre-loaded module or capability snapshot as an argument rather than calling `extension_module_with_capabilities()` internally.

**Suggested improvement:**
Change `validate_extension_capabilities` to accept an already-loaded `capabilities_snapshot: Mapping[str, object]` parameter. Move the module-loading call to the caller (which already has the loaded module). This makes `extension_validation.py` a pure-validator that depends only on Python builtins and the `extension_core` constants for error messages.

**Effort:** small
**Risk if unaddressed:** low — the current design is not broken, but it makes `extension_validation` harder to test in isolation.

---

#### P6. Ports & Adapters — Alignment: 2/3

**Findings:**

- `src/datafusion_engine/udf/platform.py:4-7` (module docstring): mentions `RelationPlanner` as a planning-critical component to install alongside ExprPlanner. `platform.py:9` describes `RelationPlanner` as a supported extension type. However, no `RelationPlanner` port exists anywhere in the Python scope: `expr/planner.py` handles ExprPlanner installation via `install_expr_planners` / `_install_native_expr_planners`, but there is no corresponding `install_relation_planner` or `RelationPlannerPolicy`. The DF52 `RelationPlanner` hook (for FROM-clause extensions) is a direct relevance: if dataset scan registration is to be simplified via `RelationPlanner` (as the agent instructions suggest), the port needs to be declared before an adapter can be built.

- `src/datafusion_engine/expr/planner.py:70-92`: the `_load_extension()` function hard-codes a single candidate module name (`"datafusion_engine.extensions.datafusion_ext"`) in a `for` loop that iterates over a one-element tuple. This is vestigial iteration — either remove the loop or document that the list is an extension point for multiple backends.

**Suggested improvement:**
Declare a `RelationPlannerPort` protocol in `expr/planner.py` (or a new `expr/relation_planner.py`) that mirrors the `ExprPlannerPolicy` / `install_expr_planners` pattern. Until an adapter is implemented, the port documents the intended seam clearly. Simplify `_load_extension()` to remove the vestigial loop.

**Effort:** small
**Risk if unaddressed:** low — no active breakage, but the undeclared `RelationPlanner` seam means DF52's new capability will be added ad-hoc rather than cleanly.

---

### Category: Knowledge (7-11)

#### P7. DRY — Alignment: 2/3

**Findings:**

- Arrow-to-DDL type name mapping is implemented in two places:
  1. `src/datafusion_engine/dataset/ddl_types.py`: `_DDL_TYPE_ALIASES` dict + `ddl_type_alias()` function, covering primitive type strings.
  2. `src/datafusion_engine/udf/extension_ddl.py:75-127`: `_ddl_type_name_from_arrow()` and `_ddl_type_name_from_string()` perform Arrow-DataType-to-DDL-string conversion, calling `ddl_type_alias()` as a fallback but adding additional logic for timestamp, date, struct, list, map, binary, dictionary types.

  These two paths encode the same knowledge (how to represent an Arrow/DataFusion type as a DDL string) but via different entry-points with different capabilities. A caller needing to convert `pa.timestamp("us")` to `"TIMESTAMP"` must use `extension_ddl._ddl_type_name()` — the version in `ddl_types.py` is insufficient.

**Suggested improvement:**
Move `_ddl_type_name_from_arrow`, `_ddl_type_name_from_string`, and `_ddl_type_name` from `extension_ddl.py` into `ddl_types.py`, making it the single authority for Arrow→DDL type rendering. `extension_ddl.py` imports from `ddl_types.py` only. Both `registration_core._sql_type_name` and `extension_ddl._ddl_type_name` then share one implementation.

**Effort:** small
**Risk if unaddressed:** low — currently the two paths produce consistent results, but future changes to one will silently diverge from the other.

---

#### P8. Design by Contract — Alignment: 2/3

**Findings:**

- `src/datafusion_engine/udf/expr.py:136-145`: `udf_expr(name, *args, ctx=None, **kwargs)` silently converts all keyword argument *values* to positional arguments (`call_args.extend(kwargs.values())`). This means `udf_expr("stable_hash_any", col_expr, canonical=True, null_sentinel="__null__")` passes `[col_expr, True, "__null__"]` — the keyword names are discarded. The docstring does not warn of this, and the function signature implies keyword arguments are supported. This is an implicit contract violation: callers relying on keyword-argument semantics receive silent positional behavior.

- `src/datafusion_engine/udf/extension_validation.py:366-395`: `validate_rust_udf_snapshot` wraps `TypeError` and `ValueError` in new exceptions with a prefix — a contract enforcement pattern. However, the `validate_required_udfs` function (lines 398-433) has a subtle precondition: it calls `_alias_to_canonical` which validates that alias targets exist in the main snapshot names — but this can silently skip unknown canonical names if the alias resolution falls through, producing false-negative validation for mistyped aliases.

**Suggested improvement:**
Change `udf_expr` to not accept `**kwargs` at all if kwargs are only used positionally. Or, if named UDF arguments are intentional, document the contract explicitly and add an assertion that kwargs keys match the UDF's declared `parameter_names` snapshot. For `validate_required_udfs`, add an assertion that each `canonical` alias target resolves to a known name; raise `ValueError` explicitly if not.

**Effort:** small
**Risk if unaddressed:** medium — `udf_expr` silently misorders arguments if callers use kwargs with non-trivially-ordered Rust UDF signatures.

---

#### P9. Parse, Don't Validate — Alignment: 2/3

**Findings:**

- `src/datafusion_engine/udf/extension_core.py:140-174`: `_install_codeanatomy_runtime_snapshot` both normalizes (setdefault calls on lines 153-230 in `_normalize_registry_snapshot`) and also calls `normalize_runtime_install_snapshot` from `runtime_snapshot_types.py`. The result is two normalization passes over the same payload: one inside `_install_codeanatomy_runtime_snapshot` (building `payload_mapping` and calling `setdefault` on it) and one in `normalize_runtime_install_snapshot`. The boundary between "what the Rust runtime returns" and "what Python operates on" is crossed twice, with redundant coercions.

- `src/datafusion_engine/udf/extension_snapshot_runtime.py:21-39`: `_empty_registry_snapshot()` defines the default empty-snapshot shape in a separate function, while `_normalize_registry_snapshot` in `extension_core.py` also populates the same default keys via `setdefault`. The canonical shape of an empty snapshot is thus encoded in two places.

**Suggested improvement:**
Unify the snapshot normalization into a single `normalize_rust_snapshot(raw: object) -> dict[str, object]` function in `runtime_snapshot_types.py` that produces a fully canonical snapshot from any Mapping or None input. Remove the `setdefault` calls from `_normalize_registry_snapshot` and the separate `_empty_registry_snapshot`. The `normalize_runtime_install_snapshot` function becomes the single parse boundary.

**Effort:** small
**Risk if unaddressed:** low — the current two-pass approach produces correct output, but any schema evolution requires updating two locations.

---

#### P10. Make Illegal States Unrepresentable — Alignment: 2/3

**Findings:**

- `src/datafusion_engine/udf/metadata.py:35-36`: `UdfTier = Literal["builtin"]` is a single-value literal type. Functions `list_functions_by_tier` (line 721-737) and `resolve_by_tier` (lines 695-719) both contain branches for `tier != "builtin"` that raise `ValueError` or return `None`. Since `UdfTier` can only be `"builtin"`, these branches are dead code reachable only via `cast` or `type: ignore`. The `DataFusionUdfSpec.__post_init__` (lines 94-102) enforces the same constraint with a runtime check. The type system already prevents the invalid state; the runtime checks and dead-code branches add noise.

- `src/datafusion_engine/expr/spec.py:726-764`: `ExprSpec` has two optional fields (`sql` and `expr_ir`) with the constraint "at least one must be present." This could be modeled as a union of two concrete types: `ExprSpecSQL(sql: str)` and `ExprSpecIR(expr_ir: ExprIR)` — but the current dataclass-with-post-init approach is a reasonable pragmatic choice given msgspec struct constraints. Noted as minor.

**Suggested improvement:**
For `UdfTier`: if the type is intentionally narrowed to `"builtin"` only (no custom UDF tier is intended), remove the `tier != "builtin"` branches from `list_functions_by_tier` and `resolve_by_tier`, and remove the `DataFusionUdfSpec.__post_init__` tier check. Document the design decision in a comment. Alternatively, define a proper `UdfTierEnum` with future values explicitly listed to make future extension intentional rather than accidental.

**Effort:** small
**Risk if unaddressed:** low — the dead branches are harmless but mislead readers into believing tier variation is intended.

---

#### P11. CQS — Alignment: 1/3

**Findings:**

- `src/datafusion_engine/dataset/registration_core.py:351-418`: `register_dataset_df` registers a table (a command that has side-effects: catalog mutation, introspection cache invalidation, artifact recording) and returns a `DataFrame` (a query result). Callers cannot safely call it for its return value alone without triggering the registration side-effects, nor can they trigger registration without receiving the DataFrame.

- `src/datafusion_engine/udf/extension_registry.py:29-71`: `register_rust_udfs` installs UDFs into the session (command), mutates `registries` (command), and returns the validated snapshot (query). Callers who want the snapshot but have already registered must call `rust_udf_snapshot` instead — but the function docstring does not make this clear.

- `src/datafusion_engine/tables/registration.py:76-134`: `register_table` registers the table and returns a `DataFrame`.

  The combined registration-and-return pattern is pervasive (8+ instances across all four directories). This is partly driven by DataFusion's API (`ctx.table(name)` after registration), but the Python wrapper can and should separate the concerns.

**Suggested improvement:**
Introduce a two-step surface for dataset registration: `register_dataset(ctx, ...)` (command, returns `None`) and `get_dataset_df(ctx, name)` (query, returns `DataFrame`). Retain `register_dataset_df` as a convenience wrapper that calls both — but document it explicitly as a combined operation. Similarly for `register_rust_udfs`: rename to `install_rust_udfs` (command returning `None`) and expose `get_udf_snapshot(ctx, registries)` (pure query). The existing `rust_udf_snapshot` is already the right query surface; `register_rust_udfs` should delegate to it after installing.

**Effort:** medium
**Risk if unaddressed:** medium — the pattern makes retry logic, caching, and testing harder. Any caller that needs only the snapshot must call a function with hidden side-effects.

---

### Category: Composition (12-15)

#### P12. Dependency Inversion + Explicit Composition — Alignment: 2/3

**Findings:**

- `src/datafusion_engine/udf/platform.py:458-478`: `ensure_rust_udfs` constructs a `RustUdfPlatformOptions` dataclass, serializes it to builtins via `msgspec.to_builtins(...)`, wraps it in an `InstallRustUdfPlatformRequestV1`, then passes it to `install_rust_udf_platform`, which immediately deserializes it back to `RustUdfPlatformOptions` via `msgspec.convert(request.options, type=RustUdfPlatformOptions)`. This round-trip through serialization (`to_builtins` → dict → `msgspec.convert`) exists because `InstallRustUdfPlatformRequestV1.options` is typed as `dict[str, object] | None`. The `ensure_rust_udfs` function could instead construct the `RustUdfPlatformOptions` directly and pass it to a lower-level helper that bypasses the serialization round-trip.

- `src/datafusion_engine/udf/extension_core.py:404-412`: `register_rust_udfs` is implemented by importing `_register_rust_udfs` from `extension_registry` inside the function body. The function body is a one-liner that delegates to the import. This deferred-import delegation pattern is used to break circular imports — the same root cause as P4.

**Suggested improvement:**
Add an overload or internal constructor to `install_rust_udf_platform` that accepts `RustUdfPlatformOptions` directly (skipping serialization). `ensure_rust_udfs` calls this directly. The `InstallRustUdfPlatformRequestV1` envelope is for serialized cross-process requests only and should not be used for in-process Python calls.

**Effort:** small
**Risk if unaddressed:** low — the round-trip is functionally correct but adds unnecessary allocation and hides the type.

---

#### P13. Prefer Composition Over Inheritance — Alignment: 3/3

No issues. `StructBaseCompat` and `StructBaseStrict` are used only at msgspec serialization boundaries. `UdfCatalogAdapter` uses composition correctly (holds a `UdfCatalog` reference, implements `Registry` protocol via delegation). No deep inheritance chains exist.

---

#### P14. Law of Demeter — Alignment: 2/3

**Findings:**

- `src/datafusion_engine/dataset/registration_core.py:303-310`: `_schema_hardening_view_types(runtime_profile)` accesses `runtime_profile.policies.schema_hardening.enable_view_types` (three hops) and falls back to `runtime_profile.policies.schema_hardening_name == "arrow_performance"` (two hops with string comparison). This function is a single-level encapsulation wrapper — but the caller `_scan_hardening_defaults` calls it correctly via one hop. The issue is that `runtime_profile.policies` is a fat object that exposes a nested `schema_hardening` object, which callers reach through two levels of the profile.

- `src/datafusion_engine/udf/parity.py:157-161`: `introspection_cache_for_ctx(ctx).snapshot.routines` — three hops to reach `routines`. If the introspection cache changes its internal representation, `parity.py` must change.

**Suggested improvement:**
For the profile: add a `DataFusionRuntimeProfile.schema_hardening_view_types() -> bool` method that encapsulates the two-path fallback. For introspection: add a `get_routines_table() -> pa.Table | None` method to the introspection cache that hides the `.snapshot.routines` hop.

**Effort:** small
**Risk if unaddressed:** low — concrete chaining but not structurally dangerous.

---

#### P15. Tell, Don't Ask — Alignment: 2/3

**Findings:**

- `src/datafusion_engine/udf/metadata.py:631-693`: `UdfCatalog.resolve_function` asks `self._runtime_catalog` (a `FunctionCatalog`) whether a function is builtin, then extracts its signature, then asks again for the case where builtin is not preferred. The logic for "is builtin + what is its spec" is implemented in `resolve_function` rather than in `FunctionCatalog`. `FunctionCatalog.is_builtin` (line 447-460) is a thin check that returns a boolean; the caller then does the signature lookup manually. `FunctionCatalog` could expose `resolve(func_id) -> ResolvedFunction | None` instead of `is_builtin` + raw `function_signatures` dict access.

- `src/datafusion_engine/dataset/registration_core.py:464-480`: `_build_registration_context` asks `location.resolved.table_spec` for its schema, then builds `metadata` from it. The `TableProviderMetadata` construction logic (pulling constraints, defaults, storage location, file format, partition columns) is all done outside the `DatasetLocation` type, which "knows" all these fields. A `DatasetLocation.to_provider_metadata(...)` method could encapsulate this.

**Suggested improvement:**
Add `FunctionCatalog.resolve(func_id: str) -> ResolvedFunction | None` that encapsulates the `is_builtin` + `function_signatures.get` logic. `UdfCatalog.resolve_function` delegates to this. Remove `FunctionCatalog.function_signatures` from the public interface (currently used directly by `UdfCatalog`).

**Effort:** small
**Risk if unaddressed:** low — the current code is correct; this is a cohesion improvement.

---

## Cross-Cutting Themes

### Theme 1: Circular Import Infrastructure Tax

The `extension_core` ↔ `extension_snapshot_runtime` ↔ `extension_validation` triangle is not an accident — it reflects the history of extracting code from a single large file without resolving the dependency graph first. The consequence is that 14 functions in `extension_snapshot_runtime.py` are implemented as single-expression wrappers that perform a deferred local import and delegate to `extension_validation`. This accounts for approximately 200 LOC of pure indirection (lines 42-270 of `extension_snapshot_runtime.py`). The root cause is that `extension_core` was treated as the stable hub, but it accumulated dependencies on snapshot normalization (which depends on validation, which depends on extension loading — all three should be layered). Affected principles: P1, P4, P11, P12.

### Theme 2: Command-Query Fusion at Registration Boundaries

Every `register_*` function in the scope returns a DataFrame or snapshot. This is a consistent architectural pattern — not an isolated decision. The DataFusion API style (register, then `ctx.table(name)`) encourages it, but the wrappers could decouple. The pattern makes it impossible to call the registration for its side effects alone (e.g., in tests), and it couples the registration contract to the DataFrame surface. Affected principles: P11, P8, P23 (testability).

### Theme 3: `registration_core.py` as a Structural Liability

The alias block (lines 616-647) in `registration_core.py` was added to avoid private imports from submodules, but it inverts the dependency direction: instead of submodules importing from `core`, `core` imports from submodules and re-publishes them. This creates a star-shaped import hub that makes the module boundaries invisible to static analysis and linters. Callers that import from `registration_core` are effectively importing from seven different modules without knowing it. Affected principles: P1, P2, P4.

### Theme 4: `UdfTier` Literal Type + Dead Branches

The `UdfTier = Literal["builtin"]` design locks in "builtin-only" at the type level while preserving branches that handle hypothetical additional tiers. This is a YAGNI violation — the dead branches signal future extensibility that is not needed and creates false expectations that the tier system is active. Affected principles: P10, P20 (YAGNI).

---

## DF52 Migration Impact

| Area | Impact | Status | Required Action |
|------|--------|--------|-----------------|
| `AggregateUDFImpl::supports_within_group_clause()` | UDAFs using `WITHIN GROUP` will error in DF52 unless Rust impl opts in | Not surfaced in Python layer (correct — UDF impl is in Rust) | Audit `udaf_builtin.rs` in Rust layer; document in `udf/metadata.py` as a known gap in `DataFusionUdfSpec` |
| `AggregateUDFImpl::supports_null_handling_clause()` | `IGNORE/RESPECT NULLS` will error unless Rust impl opts in | Same as above | Same as above |
| `RelationPlanner` (new FROM-clause hook) | Could simplify dataset scan registration if adopted | No port declared in Python scope | Declare `RelationPlannerPort` in `expr/relation_planner.py`; note in `platform.py` |
| `ExprPlanner` extensibility | Already in use via `install_expr_planners` | Correctly wired in `expr/planner.py` and `udf/platform.py` | No action required |
| `FileSource::with_projection` removed → `try_pushdown_projection` | Scan pushdown API change affects `registration_listing.py` if using FileSource directly | Likely handled in Rust layer; `_register_listing_table_native` uses DataFusion's Python `register_csv_file` / `ListingTableConfig` abstraction | Verify `datafusion_ext`/`DataFusionIOAdapter` uses DF52-compatible scan pushdown path |
| `ListingTableProvider` caching (new TTL semantics) | `DataFusionScanOptions.list_files_cache_ttl` / `list_files_cache_limit` now actively used by `_apply_scan_settings` | Settings are already written via `SET datafusion.runtime.*` — the version-gated skip logic in `registration.py:186-213` handles DF52 runtime namespace changes | Verify skip version constant (`DATAFUSION_RUNTIME_SETTINGS_SKIP_VERSION`) matches DF52 major version |
| `CoalesceBatchesExec` removed | No Python-side reference found | Not affected | None |

---

## Rust Migration Candidates

| Component | Python LOC | Rust Candidate? | Rationale |
|-----------|-----------|-----------------|-----------|
| `udf/signature.py` (type string parser) | 205 | High | Parses Rust-native type signature strings into PyArrow types. All inputs come from the Rust snapshot. A Rust-side serialization that emits `pa.DataType`-compatible pyarrow capsules directly would eliminate this entire module. |
| `udf/extension_ddl.py` (DDL name generation) | 178 | Medium | Converts Arrow types to DDL type names for `CREATE FUNCTION` statements. The same conversion exists in the Rust layer (which knows Arrow types natively). The Rust snapshot could include pre-computed DDL type strings, eliminating `_ddl_type_name_from_arrow`. |
| `udf/extension_validation.py` (snapshot validation) | 481 | Medium | Validates the Rust-produced snapshot. If the Rust layer emits a typed contract (e.g., a msgpack-encoded struct) instead of an untyped `Mapping[str, object]`, the Python validation layer becomes unnecessary. Estimated LOC reduction: ~300. |
| `udf/extension_snapshot_runtime.py` (snapshot normalization helpers) | 495 | High | Primarily wraps calls to `extension_validation.py` via deferred imports. If snapshot validation moves to Rust, this module collapses to ~50 LOC (snapshot caching, docs retrieval). |
| `expr/spec.py` (`_expr_*` call dispatchers for Rust UDFs) | ~300 (dispatch only) | Medium | The 30+ `_expr_*` functions that call `udf_expr(name, ...)` are one-liners that delegate to the Rust extension. If `ExprIR.to_expr()` used a single `udf_expr(self.name, *[arg.to_expr() for arg in self.args])` dispatch for "call" nodes that are Rust UDFs, these individual Python wrappers would not be needed. Requires distinguishing Rust-UDF calls from DF-builtin calls in the IR. |

**Priority Rust pivot recommendation:** The highest LOC-reduction opportunity is unifying the snapshot contract: emit a typed, msgpack-encoded snapshot from Rust (instead of a generic Python dict) so that `extension_validation.py`, `extension_snapshot_runtime.py`, and `udf/signature.py` can be replaced by a single `msgspec.convert(raw_bytes, type=RustUdfSnapshot)` call. Estimated Python LOC reduction: ~800-900 LOC across the three files.

---

## Planning-Object Consolidation

| Bespoke Python Code | DF Built-in Replacement | LOC |
|--------------------|------------------------|-----|
| `_sql_type_name` in `registration_core.py` + `_ddl_type_name_from_arrow` in `extension_ddl.py` | No direct DF built-in; consolidate into single `ddl_types.py` function | ~40 |
| `udf/signature.py::parse_type_signature` (custom type-string parser) | `pa.DataType` from PyArrow's `from_numpy_dtype`/`deserialize` or Rust-side type emission | ~150 |
| `_expr_row_number_window`, `_expr_lag_window`, `_expr_lead_window` in `expr/spec.py` | `datafusion.functions.row_number()`, `datafusion.functions.lag()`, `datafusion.functions.lead()` — already called inside the handlers; the handlers are one-liners that already use DF built-ins. The `ExprIR` dispatch table entry could be replaced by a direct `datafusion.functions.*` lookup table, eliminating the named wrapper functions | ~30 |
| `_apply_scan_settings` via `SET key = value` SQL (listing table registration) | `ListingTableConfig` or `ListingOptions` builder API (DF Python) — eliminates SQL round-trip for config knobs | ~60 |

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Move `_ddl_type_name_from_arrow/string` from `extension_ddl.py` into `ddl_types.py` | small | Eliminates duplicated Arrow→DDL knowledge; `extension_ddl.py` and `registration_core.py` share one path |
| 2 | P8 (Design by Contract) | Remove `**kwargs` from `udf_expr` or document and enforce keyword-to-position semantics | small | Prevents silent argument misorder for any future Rust UDF added with a different parameter order |
| 3 | P10 (Illegal states) | Remove dead `tier != "builtin"` branches from `UdfCatalog.list_functions_by_tier` and `resolve_by_tier`; remove `DataFusionUdfSpec.__post_init__` tier check | small | Reduces LOC; eliminates false signal that tier variety is implemented |
| 4 | P2 (SoC) | Extract `_EXPR_CALLS`, `_SQL_CALLS`, `_EXACT_CALL_COUNTS`, `_MIN_CALL_COUNTS`, and all `_expr_*`/`_sql_*` handlers from `expr/spec.py` into `expr/dispatch.py` | small | `spec.py` becomes a pure type module; dispatch table becomes independently testable |
| 5 | P5 (Dependency direction) | Change `validate_extension_capabilities` to accept a pre-loaded `capabilities_snapshot` mapping; remove `extension_module_with_capabilities` call from inside the validation function | small | `extension_validation.py` becomes a pure validator; testable without a live Rust extension |

---

## Recommended Action Sequence

1. **Consolidate DDL type mapping** (P7, Quick Win 1): Merge `_ddl_type_name_from_arrow`, `_ddl_type_name_from_string`, and `_ddl_type_name` into `src/datafusion_engine/dataset/ddl_types.py`. Update `extension_ddl.py` to import from `ddl_types.py`. Update `registration_core._sql_type_name` to call the unified function.

2. **Fix `udf_expr` contract** (P8, Quick Win 2): Remove `**kwargs` from `udf_expr` signature in `src/datafusion_engine/udf/expr.py:123`. Update all call sites that use keyword arguments (the `stable_hash_any`, `utf8_normalize`, `qname_normalize`, `map_get_default`, `map_normalize`, `span_id` handlers in `expr/spec.py`) to use positional arguments. Confirm call sites produce the same Rust argument order.

3. **Remove dead `UdfTier` branches** (P10, Quick Win 3): In `src/datafusion_engine/udf/metadata.py`, remove the `if tier != "builtin": raise ValueError` branch from `list_functions_by_tier` (line 733-735) and the `return resolved if resolved.tier in tiers else None` gating in `resolve_by_tier` when `allowed_tiers` reduces to `("builtin",)`. Remove `DataFusionUdfSpec.__post_init__` tier check (lines 94-102).

4. **Split `expr/spec.py`** (P2, Quick Win 4): Create `src/datafusion_engine/expr/dispatch.py`. Move `_EXPR_CALLS`, `_SQL_CALLS`, `_EXACT_CALL_COUNTS`, `_MIN_CALL_COUNTS`, `_validate_call_name`, `_render_call`, `_call_expr`, `_bitwise_fold`, and all 30 `_expr_*` / `_sql_*` private functions. `spec.py` imports `_render_call` and `_call_expr` from `dispatch.py`.

5. **Decouple `validate_extension_capabilities`** (P5, Quick Win 5): In `src/datafusion_engine/udf/extension_validation.py:232-273`, change the function signature to `validate_extension_capabilities(snapshot: Mapping[str, object], *, strict: bool = True, ctx: SessionContext | None = None) -> Mapping[str, object]`. Remove the internal call to `extension_module_with_capabilities()`. Move all module-loading to the caller in `platform.py`.

6. **Resolve the circular import triangle** (P1, P4 — medium effort): Merge `extension_core.py` and `extension_snapshot_runtime.py` into a single `extension_runtime.py`. Eliminate the bottom-of-file imports in `extension_core.py` (lines 360-380). `extension_validation.py` depends on `extension_runtime.py` cleanly without deferred imports. `extension_registry.py` imports from `extension_runtime.py` directly.

7. **Remove `registration_core.py` alias block** (P1, P4 — medium effort): Delete lines 616-647 from `src/datafusion_engine/dataset/registration_core.py`. Update all callers that currently import aliases from `registration_core` to import directly from the owning module (e.g., `registration_delta_helpers`, `registration_provider`, `registration_projection`).

8. **Split `udf/metadata.py`** (P3 — large effort): Create `udf/spec_types.py` (DataFusionUdfSpec, DataFusionUdfSpecSnapshot, ArrowTypeSpec round-trip), `udf/catalog.py` (FunctionCatalog, UdfCatalog, UdfCatalogAdapter, factory functions), `udf/planner_metadata.py` (udf_planner_snapshot, rewrite_tag_index, datafusion_udf_specs). Retain `metadata.py` as a compatibility re-export shim for one release cycle.
