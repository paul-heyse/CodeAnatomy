# Design Review: src/datafusion_engine/udf/, extensions/, catalog/

**Date:** 2026-02-16
**Scope:** `src/datafusion_engine/udf/`, `src/datafusion_engine/extensions/`, `src/datafusion_engine/catalog/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 20

## Executive Summary

The UDF/extensions/catalog subsystem is a well-structured adapter layer mediating between Python-side DataFusion sessions and a Rust native extension module. It demonstrates strong contract awareness (frozen dataclasses, validation at boundaries, explicit `__all__` exports) and good separation between snapshot capture, validation, and registration lifecycle stages. The primary structural concerns are: (1) heavy reliance on module-level `WeakSet`/`WeakKeyDictionary` globals as the sole state management mechanism, creating a hidden implicit registry that makes testing and reasoning difficult; (2) duplicated extension module loading patterns scattered across 5+ files with near-identical `importlib.import_module("datafusion_engine.extensions.datafusion_ext")` calls; and (3) `extension_runtime.py` at 1,596 lines carrying too many responsibilities (snapshot capture, normalization, validation, DDL generation, hashing, and runtime installation). Catalog provider and runtime capabilities modules are well-aligned.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | high | 6 module-level WeakSet/WeakKeyDictionary globals exposed as implicit registry |
| 2 | Separation of concerns | 1 | large | medium | `extension_runtime.py` mixes snapshot capture, validation, DDL gen, hashing |
| 3 | SRP | 1 | large | medium | `extension_runtime.py` has 5+ reasons to change (1,596 LOC) |
| 4 | High cohesion, low coupling | 2 | medium | low | `metadata.py` tightly coupled to snapshot dict shape; catalog provider well-cohesive |
| 5 | Dependency direction | 2 | small | low | Core UDF types correctly independent; `factory.py` deferred imports acceptable |
| 6 | Ports & Adapters | 2 | medium | medium | `context_adaptation.py` is a proper port; but `datafusion_ext.py` import duplicated in 5 files |
| 7 | DRY (knowledge) | 1 | medium | medium | Extension module name string duplicated 7+ times; ABI error messages duplicated 11 times |
| 8 | Design by contract | 2 | small | low | Good validation at boundaries; some docstrings lack parameter descriptions |
| 9 | Parse, don't validate | 2 | medium | low | Snapshot normalized once at boundary; but downstream still does isinstance checks |
| 10 | Make illegal states unrepresentable | 1 | medium | medium | Async UDF policy is a raw tuple, not a typed struct; snapshot is untyped dict |
| 11 | CQS | 2 | small | low | `register_rust_udfs` both mutates and returns; `register_function` has side effect + cache invalidation |
| 12 | DI + explicit composition | 1 | large | high | Global WeakSet/WeakKeyDictionary state with no injection; factory uses importlib.import_module |
| 13 | Composition over inheritance | 3 | - | - | Inheritance is minimal and appropriate (DataFusion CatalogProvider/SchemaProvider) |
| 14 | Law of Demeter | 2 | small | low | `getattr(ctx, "ctx", None)` pattern in 8 files reaches into internal wrapper |
| 15 | Tell, don't ask | 2 | small | low | Snapshot queried externally via dict access; `UdfCatalog` encapsulates well |
| 16 | Functional core, imperative shell | 2 | medium | low | Pure validation/normalization functions exist; but IO-bound install mixed into same module |
| 17 | Idempotency | 3 | - | - | WeakSet guards ensure registration happens once per context |
| 18 | Determinism | 3 | - | - | `rust_udf_snapshot_hash` and normalization ensure deterministic hashing |
| 19 | KISS | 2 | small | low | `_rewrite_variadic_hash_call` in `expr.py` adds non-obvious rewrite logic |
| 20 | YAGNI | 2 | small | low | `create_default_catalog` and `create_strict_catalog` are identical implementations |
| 21 | Least astonishment | 2 | small | low | `register_rust_udfs` returns snapshot (query) while mutating global state (command) |
| 22 | Public contracts | 2 | small | low | Explicit `__all__` in every module; `InstallRustUdfPlatformRequestV1` is versioned |
| 23 | Testability | 1 | large | high | Module-level global state not injectable; importlib.import_module hardcoded |
| 24 | Observability | 2 | small | low | Structured logging in `runtime_capabilities.py`; `platform.py` has logging but sparse |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
`extension_runtime.py` exposes its internal registration state through 6 module-level globals (`_RUST_UDF_CONTEXTS`, `_RUST_UDF_SNAPSHOTS`, `_RUST_UDF_DOCS`, `_RUST_RUNTIME_PAYLOADS`, `_RUST_UDF_POLICIES`, `_RUST_UDF_VALIDATED`, `_RUST_UDF_DDL`) at lines 34-45. While underscore-prefixed, these dictionaries constitute the entire state model for UDF lifecycle management and are directly mutated by multiple functions throughout the module.

Similarly, `catalog/introspection.py:373` uses `_INTROSPECTION_CACHE_BY_CONTEXT: dict[int, IntrospectionCache]` keyed by `id(ctx)`, which is fragile (object IDs can be reused after GC) compared to the WeakSet/WeakKeyDictionary pattern used elsewhere.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:34-45` -- Six module-level `WeakSet`/`WeakKeyDictionary` globals constitute a hidden implicit registry with no encapsulation
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/platform.py:103-104` -- Two additional `WeakSet` globals (`_FUNCTION_FACTORY_CTXS`, `_EXPR_PLANNER_CTXS`) tracking install state
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/catalog/introspection.py:373` -- `_INTROSPECTION_CACHE_BY_CONTEXT` keyed by `id(ctx)` which is unsafe after GC (IDs can be reused)

**Suggested improvement:**
Encapsulate the 6 `extension_runtime.py` globals into a `RustUdfSessionRegistry` class that owns the WeakSet/WeakKeyDictionary state. Expose a module-level singleton for backward compatibility, but allow injection for testing. For `introspection.py`, switch from `dict[int, ...]` to `WeakKeyDictionary[SessionContext, ...]` to match the pattern used in `extension_runtime.py` and avoid stale entries.

**Effort:** medium
**Risk if unaddressed:** high -- The scattered module globals make it impossible to reset state between tests or run concurrent test suites. The `id(ctx)` key can cause stale cache entries to be served after GC reclaims and reallocates the same address.

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`extension_runtime.py` (1,596 LOC) handles at least five distinct responsibilities: (a) Rust extension module loading and ABI validation, (b) registry snapshot capture and normalization, (c) snapshot structural validation, (d) DDL generation and registration, (e) snapshot hashing and serialization. These responsibilities are interleaved rather than separated.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:1-100` -- Module constants, ABI version checks, DDL type aliases, and snapshot key definitions all cohabit
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:356-405` -- Snapshot normalization interleaves policy injection (`_RUST_UDF_POLICIES`) with data transformation
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:1352-1384` -- DDL registration uses deferred imports from `factory.py` and `metadata.py`, suggesting this code belongs closer to those modules
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:1216-1270` -- Hashing/serialization helpers (`rust_udf_snapshot_payload`, `rust_udf_snapshot_bytes`, `rust_udf_snapshot_hash`) are a self-contained concern

**Suggested improvement:**
Extract into 3 focused modules: (a) `udf/snapshot_validation.py` for `validate_rust_udf_snapshot`, `_require_snapshot_metadata`, `_validate_udf_entries`, etc. (b) `udf/snapshot_normalization.py` for `_normalize_registry_snapshot`, `_supplement_expr_surface_snapshot`, normalization helpers. (c) Keep `extension_runtime.py` focused on runtime lifecycle (install, register, capability probing). Move DDL-related functions (`register_udfs_via_ddl`, `_ddl_*`) near `factory.py` where `CreateFunctionConfig` is defined.

**Effort:** large
**Risk if unaddressed:** medium -- The monolithic file makes it difficult to understand the UDF lifecycle, increases merge conflict risk, and makes targeted testing of individual concerns impractical.

---

#### P3. SRP -- Alignment: 1/3

**Current state:**
`extension_runtime.py` changes for at least five different reasons: (1) Rust ABI version bumps, (2) snapshot schema evolution, (3) DDL dialect changes, (4) async UDF policy changes, (5) hashing algorithm changes. This violates the "one reason to change" heuristic.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py` -- 1,596 LOC with ~50 functions; the largest module in the reviewed scope
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/catalog/introspection.py` -- 730 LOC mixing snapshot capture, cache management, and cache diagnostics (4 separate `*_cache_snapshot` functions plus `capture_cache_diagnostics`)

**Suggested improvement:**
For `extension_runtime.py`, see the extraction plan in P2 above. For `introspection.py`, extract cache diagnostics (`CacheConfigSnapshot`, `CacheStateSnapshot`, `capture_cache_diagnostics`, the 4 `*_cache_snapshot` functions, `register_cache_introspection_functions`) into a dedicated `catalog/cache_diagnostics.py` module.

**Effort:** large
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Within the scope, cohesion is generally good. `catalog/provider.py` is highly cohesive around the catalog/schema provider abstraction. `udf/signature.py` is tightly focused on type parsing. `udf/parity.py` is well-contained as a parity reporting concern.

The coupling concern is that `extension_runtime.py`, `factory.py`, `platform.py`, `expr.py`, and `catalog/provider.py` all independently call `importlib.import_module("datafusion_engine.extensions.datafusion_ext")` to load the native extension. This creates implicit coupling to the module name string.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:428` (`_datafusion_internal`), `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/factory.py:222` (`_load_extension`), `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/expr.py:21` (`_require_callable`), `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/catalog/provider.py:66` -- Four independent implementations for loading the same extension module
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/metadata.py` -- `FunctionCatalog`, `UdfCatalog`, `UdfCatalogAdapter`, `DataFusionUdfSpec`, `DataFusionUdfSpecSnapshot`, `BuiltinFunctionSpec`, `FunctionSignature`, `RoutineMeta`, `ResolvedFunction` -- 9 types in one module (1,215 LOC), though they are thematically related

**Suggested improvement:**
Consolidate extension module loading into `context_adaptation.py` (which already has `resolve_extension_module`) and have all callers use it. The string `"datafusion_engine.extensions.datafusion_ext"` should be a single constant in one location.

**Effort:** medium
**Risk if unaddressed:** low -- The duplication is annoying but manageable since the module name is unlikely to change.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The dependency direction is generally correct. Core UDF types (`DataFusionUdfSpec`, `FunctionCatalog`) in `metadata.py` have no dependency on the Rust extension runtime. `extension_runtime.py` depends on `factory.py` and `metadata.py` via deferred imports, which is acceptable for avoiding circular dependencies. `contracts.py` (the most fundamental type) is dependency-free.

The concern is that `extension_runtime.py` imports from `datafusion_engine.expr.domain_planner` and `datafusion_engine.expr.planner` at line 226-228, meaning the UDF installation layer depends on the expression planner layer. Ideally this would be inverted.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:225-228` -- Deferred imports of `domain_planner_names_from_snapshot` and `install_expr_planners` create a dependency from UDF runtime to expression planner subsystem
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/contracts.py` -- Correctly dependency-free, defining only the request envelope

**Suggested improvement:**
The deferred imports are an acceptable pragmatic pattern given the orchestration nature of `extension_runtime.py`. However, the planner-related orchestration in `_install_runtime_via_modular_entrypoints` could be extracted to `platform.py`, which already handles this orchestration at a higher level.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`context_adaptation.py` is a well-designed port/adapter for extension entrypoint invocation. It defines clean contracts (`ExtensionContextPolicy`, `ExtensionContextProbe`, `ExtensionContextSelection`, `ExtensionEntrypointInvocation`) and provides `invoke_entrypoint_with_adapted_context` as the canonical way to call into Rust extensions. `datafusion_ext.py` acts as a thin adapter that normalizes SessionContext wrappers.

However, this port is not consistently used. `factory.py:243-257` uses `invoke_entrypoint_with_adapted_context` correctly, but `expr.py:21` and `catalog/provider.py:66` bypass it entirely with direct `importlib.import_module` calls and manual `getattr` dispatching.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/extensions/context_adaptation.py` -- Clean port definition with proper contracts
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/expr.py:19-31` -- `_require_callable` bypasses the context adaptation layer entirely
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/catalog/provider.py:62-83` -- `_table_from_dataset` directly imports and inspects `datafusion_ext.catalog.RawTable` without going through adaptation
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/catalog/introspection.py:661-703` -- `register_cache_introspection_functions` manually tries multiple context candidates instead of using `invoke_entrypoint_with_adapted_context`

**Suggested improvement:**
Route all extension module interactions through `context_adaptation.resolve_extension_module` and `invoke_entrypoint_with_adapted_context`. For `expr.py`, create a small `ExprBuilderPort` that the context adaptation layer can resolve. For `introspection.py:661-703`, replace the manual context candidate loop with `invoke_entrypoint_with_adapted_context`.

**Effort:** medium
**Risk if unaddressed:** medium -- Inconsistent adaptation increases the risk of ABI mismatch bugs affecting some code paths but not others.

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge) -- Alignment: 1/3

**Current state:**
The module name `"datafusion_engine.extensions.datafusion_ext"` is hardcoded in at least 7 locations across the scope. The ABI mismatch error message ("Rebuild and install matching datafusion/datafusion_ext wheels (scripts/build_datafusion_wheels.sh + uv sync).") appears 11 times across 6 files. The snapshot shape (keys like `"scalar"`, `"aggregate"`, `"window"`, `"table"`, `"aliases"`, etc.) is implicitly encoded in `_empty_registry_snapshot`, `_normalize_registry_snapshot`, `_REQUIRED_SNAPSHOT_KEYS`, and `_snapshot_names` -- four places that must stay synchronized.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:575-593` (`_empty_registry_snapshot`) and lines 47-60 (`_REQUIRED_SNAPSHOT_KEYS`) and lines 362-405 (`_normalize_registry_snapshot`) -- Three places encoding snapshot key knowledge
- ABI mismatch message duplicated at: `extension_runtime.py:299,317,432,443,553`, `factory.py:279`, `catalog/introspection.py:699`, `extensions/schema_evolution.py:83` -- 8+ occurrences of near-identical error text
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/extensions/runtime_capabilities.py:16,162,194,198` and `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:428,449` -- Extension module name string duplicated

**Suggested improvement:**
(a) Define a constant `DATAFUSION_EXT_MODULE` in `context_adaptation.py` and import it everywhere. (b) Define a single `abi_mismatch_error(detail: str) -> str` function in `context_adaptation.py`. (c) Define the snapshot key schema as a frozen dataclass or TypedDict in a single location (e.g., `udf/contracts.py`) rather than encoding it implicitly in three parallel data structures.

**Effort:** medium
**Risk if unaddressed:** medium -- When the error message or snapshot keys evolve, developers must hunt across multiple files to make consistent updates.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The scope has good contract enforcement overall. `validate_rust_udf_snapshot` performs comprehensive structural validation. `_async_udf_policy` validates parameter combinations. `DataFusionUdfSpec.__post_init__` enforces the `udf_tier == "builtin"` invariant. `CreateFunctionConfig` preconditions are checked in `build_create_function_sql`.

The gap is in docstring quality: several public functions have placeholder descriptions ("Args: snapshot: Description.") that provide no useful contract information.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:1038` -- `validate_rust_udf_snapshot` docstring: "Args: snapshot: Description." -- no useful description
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:1073,1076` -- `validate_required_udfs` docstring: "Args: snapshot: Description. required: Description."
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/factory.py:533-540` -- `register_function` documents `config` as "Function configuration payload." but does not describe what "register" means in terms of catalog visibility

**Suggested improvement:**
Replace placeholder docstring parameter descriptions with actual contract descriptions that state what the parameter must satisfy and what the function guarantees. Priority: public functions listed in `__all__`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
The scope partially follows this principle. `_normalize_registry_snapshot` at `extension_runtime.py:356-405` converts untyped Rust payloads into a normalized dict with defaults at the boundary, which is the right idea. `signature.py:parse_type_signature` converts string type names into `pa.DataType` once. However, the normalized snapshot remains an untyped `Mapping[str, object]`, forcing downstream code to re-validate structure via `isinstance` checks.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:356-405` -- Good: normalizes snapshot once with defaults
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/metadata.py:919-987` -- `_registry_volatility`, `_registry_rewrite_tags`, `_registry_docs`, `_registry_bool_map`, `_registry_signature_inputs` -- all re-parse the same `Mapping[str, object]` with repeated `isinstance` checks on the same keys
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/parity.py:176-178` -- Re-does `isinstance(name, str)` checks on routine names from already-typed PyArrow tables

**Suggested improvement:**
Define a `RegistrySnapshot` frozen dataclass (or msgspec Struct) with typed fields for `scalar: tuple[str, ...]`, `aggregate: tuple[str, ...]`, `volatility: Mapping[str, str]`, etc. Parse the untyped Rust payload into this typed struct once in `_normalize_registry_snapshot`, then pass the typed struct everywhere downstream. This eliminates the scattered re-validation.

**Effort:** medium
**Risk if unaddressed:** low -- The current approach works, but the repeated parsing is noisy and error-prone.

---

#### P10. Make illegal states unrepresentable -- Alignment: 1/3

**Current state:**
The async UDF policy is represented as a raw `tuple[bool, int | None, int | None]` stored in `_RUST_UDF_POLICIES`. This encoding allows illegal combinations (e.g., `enable_async=True` with `None` timeout) that are only caught at runtime in `_async_udf_policy`. The registry snapshot is an untyped `Mapping[str, object]` throughout, allowing structurally invalid snapshots to flow through the system until `validate_rust_udf_snapshot` is called.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:40-43` -- `_RUST_UDF_POLICIES: WeakKeyDictionary[SessionContext, tuple[bool, int | None, int | None]]` -- illegal state representable (True, None, None)
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:62` -- `RustUdfSnapshot = Mapping[str, object]` -- type alias provides no structural guarantee
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/metadata.py:34` -- `UdfTier = Literal["builtin"]` -- single-value literal works but the `DataFusionUdfSpec.__post_init__` re-validates at runtime what the type system already guarantees

**Suggested improvement:**
(a) Replace the raw async policy tuple with a frozen dataclass `AsyncUdfPolicy(enabled: bool, timeout_ms: int, batch_size: int)` that is only constructable when all fields are valid, plus a `NoAsyncPolicy` sentinel. (b) Replace `RustUdfSnapshot = Mapping[str, object]` with a typed struct (see P9).

**Effort:** medium
**Risk if unaddressed:** medium -- The raw tuple is easy to construct incorrectly, and the untyped snapshot passes invalid structures silently.

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. Query functions (`snapshot_function_names`, `snapshot_parameter_names`, etc.) are pure. Validation functions (`validate_rust_udf_snapshot`, `validate_required_udfs`) are pure assertions. However, a few functions mix command and query:

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:1305-1349` -- `register_rust_udfs` both mutates 3 global caches (`_RUST_UDF_CONTEXTS`, `_RUST_UDF_POLICIES`, `_RUST_UDF_SNAPSHOTS`) AND returns the snapshot
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/factory.py:528-562` -- `register_function` executes DDL (command) and calls `invalidate_introspection_cache` (side effect)
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:1136-1164` -- `rust_udf_snapshot` mutates `_RUST_UDF_SNAPSHOTS`, `_RUST_UDF_VALIDATED`, and `_RUST_UDF_DOCS` while returning the snapshot

**Suggested improvement:**
For `register_rust_udfs`, accept the mixed return as pragmatic (the caller needs the snapshot for downstream use). Document the dual nature explicitly. For `rust_udf_snapshot`, the lazy-caching pattern is acceptable but should be documented as a cache-fill query.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 1/3

**Current state:**
The most significant design concern in this scope. Extension module loading uses `importlib.import_module` with hardcoded module names in at least 5 locations, making it impossible to substitute implementations for testing. The 6 module-level global registries in `extension_runtime.py` and 2 in `platform.py` are never injected -- they are silently created at import time and mutated by module-level functions.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:34-45` -- 6 globals created at import; no DI seam
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/platform.py:103-104` -- 2 more globals created at import
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/catalog/introspection.py:373` -- Global dict keyed by object id
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/factory.py:212-229` -- `_load_extension()` uses hardcoded `importlib.import_module`
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/expr.py:19-31` -- `_require_callable` uses hardcoded `importlib.import_module`
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/extensions/schema_runtime.py:22-48` -- `load_schema_runtime` uses `@lru_cache(maxsize=1)` with hardcoded module import

**Suggested improvement:**
Introduce a `RustUdfSessionRegistry` class that encapsulates the WeakSet/WeakKeyDictionary state and accepts the extension module (or a protocol) as a constructor parameter. The module-level singleton pattern can remain for backward compatibility, but tests can create isolated instances. For extension loading, establish a `NativeExtensionLoader` protocol and pass it via DI rather than hardcoding `importlib.import_module`.

**Effort:** large
**Risk if unaddressed:** high -- Without DI, unit testing the UDF lifecycle requires monkey-patching module globals, which is fragile and non-deterministic.

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
The scope uses composition almost exclusively. `RegistrySchemaProvider` and `RegistryCatalogProvider` inherit from DataFusion's `SchemaProvider` and `CatalogProvider` respectively, which is required by the framework's design. `UdfCatalogAdapter` composes a `UdfCatalog` rather than inheriting from it. `ProviderRegistry` composes `MutableRegistry` rather than inheriting. No deep hierarchies exist.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
The `getattr(ctx, "ctx", None)` pattern appears in 8 files across the broader `datafusion_engine` scope, reaching into SessionContext's internal wrapper. Within the reviewed scope, this appears in `extension_runtime.py:169`, `factory.py:248`, `catalog/introspection.py:675`, and `extensions/schema_evolution.py:75`.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:169` -- `internal_ctx=getattr(ctx, "ctx", None)` reaches into wrapper internals
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/catalog/introspection.py:675` -- Same pattern in cache table registration
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/parity.py:157` -- `introspection_cache_for_ctx(ctx).snapshot` -- two-step traversal is acceptable (navigating to a cached property)

**Suggested improvement:**
The `getattr(ctx, "ctx", None)` pattern should be centralized in `datafusion_ext.py:_normalize_ctx` (which already exists for exactly this purpose). Callers should not need to perform this unwrapping themselves.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The UDF snapshot is queried via dict key access throughout `metadata.py`, `parity.py`, and `extension_runtime.py` (e.g., `snapshot.get("scalar")`, `snapshot.get("volatility")`). This is the primary "ask" pattern. `UdfCatalog` encapsulates its rules well (`resolve_function`, `resolve_by_tier`). `FunctionCatalog` encapsulates function lookup via `is_builtin`.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/metadata.py:1003-1037` -- `udf_planner_snapshot` queries 11 different snapshot keys externally
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:804-831` -- `_snapshot_names` iterates multiple snapshot keys

**Suggested improvement:**
If the snapshot were a typed struct (see P9/P10), these accessors would become methods on the struct, bringing the logic closer to the data. Until then, the pattern is acceptable given the untyped dict nature of the Rust bridge.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The scope has a reasonable separation. Pure functions exist for validation (`validate_rust_udf_snapshot`), normalization (`_normalize_registry_snapshot` is mostly pure but reads `_RUST_UDF_POLICIES`), type parsing (`parse_type_signature`), DDL generation (`build_create_function_sql`), and hashing (`rust_udf_snapshot_hash`). Imperative registration and installation lives in `register_rust_udfs`, `install_function_factory`, `install_rust_udf_platform`.

The concern is that `_normalize_registry_snapshot` at line 398-404 reads from the global `_RUST_UDF_POLICIES` to inject async policy into the snapshot, mixing IO-adjacent state lookup into what should be a pure transformation.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:398-404` -- `_normalize_registry_snapshot` reads from `_RUST_UDF_POLICIES[ctx]` -- impure dependency
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:643-661` -- `_probe_expr_surface_udf` makes runtime calls to DataFusion during normalization, adding IO to what is conceptually a data transformation

**Suggested improvement:**
Pass the async policy tuple as an explicit parameter to `_normalize_registry_snapshot` instead of reading it from the global dict. For `_probe_expr_surface_udf`, consider making expr surface probing a separate post-normalization step rather than embedding it in normalization.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
The `WeakSet` guard pattern in `register_rust_udfs` (checking `_RUST_UDF_CONTEXTS` before install) and `register_udfs_via_ddl` (checking `_RUST_UDF_DDL`) ensures idempotent registration. `_FUNCTION_FACTORY_CTXS` and `_EXPR_PLANNER_CTXS` in `platform.py` provide the same guard. `IntrospectionCache.snapshot` property auto-refreshes when invalidated. All registration paths check "already done?" before acting.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
`rust_udf_snapshot_hash` produces deterministic SHA-256 hashes via `_normalize_snapshot_value` which sorts dict keys and normalizes collection types. `config_fingerprint` in `FunctionFactoryPolicy.fingerprint` provides deterministic policy hashes. The snapshot normalization ensures consistent key ordering. `CacheStateSnapshot.event_time_unix_ms` is the only nondeterministic field, which is appropriate for diagnostic timestamps.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are straightforward. The one area of non-obvious complexity is `udf/expr.py:_rewrite_variadic_hash_call` (lines 65-86), which silently rewrites function call names and arguments based on argument count thresholds. This implicit rewriting is surprising and would benefit from clearer documentation.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/expr.py:65-86` -- `_rewrite_variadic_hash_call` silently rewrites `span_id` to `stable_id` and `stable_id_parts` to `stable_id` based on argument count

**Suggested improvement:**
Add a docstring to `_rewrite_variadic_hash_call` explaining the rationale (likely: variadic-to-binary UDF normalization for the Rust runtime). Consider making the rewrite rules data-driven (a mapping of source->target with conditions) rather than hardcoded conditionals.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
`create_default_catalog` and `create_strict_catalog` in `metadata.py` (lines 883-916) are identical implementations that both return `UdfCatalog(udf_specs=udf_specs)` with no policy difference. This appears to be speculative scaffolding for a future tier-based policy distinction that does not yet exist.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/metadata.py:883-916` -- `create_default_catalog` and `create_strict_catalog` are functionally identical
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/metadata.py:34` -- `UdfTier = Literal["builtin"]` -- single-value literal suggests the tier system was designed for expansion that has not materialized

**Suggested improvement:**
Collapse `create_default_catalog` and `create_strict_catalog` into a single `create_udf_catalog` factory. If/when differentiation is needed, add the parameter at that time. Keep the `UdfTier` type as-is since it is used in interfaces and documents intent.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
The API is mostly unsurprising. `install_rust_udf_platform` takes a `InstallRustUdfPlatformRequestV1` rather than `RustUdfPlatformOptions` directly, requiring `msgspec.convert` internally (line 360-363). This is surprising because callers of `ensure_rust_udfs` must construct a request envelope with `msgspec.to_builtins` wrapping of the options (lines 422-434), which is more ceremony than expected for a direct installation call.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/platform.py:342-363` -- `install_rust_udf_platform` takes a `InstallRustUdfPlatformRequestV1` envelope but immediately unwraps it
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/platform.py:421-434` -- `ensure_rust_udfs` constructs a request envelope with `msgspec.to_builtins(RustUdfPlatformOptions(...))` -- verbose ceremony for what is conceptually a simple call

**Suggested improvement:**
Accept `RustUdfPlatformOptions` directly as an optional parameter to `install_rust_udf_platform` alongside the `InstallRustUdfPlatformRequestV1` for backward compatibility. `ensure_rust_udfs` should be a thin convenience wrapper that constructs options directly without the envelope ceremony.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
Every module in scope defines `__all__` explicitly, which is good practice. `InstallRustUdfPlatformRequestV1` is versioned (V1 suffix). `DataFusionUdfSpecSnapshot` uses `StructBaseCompat` for serialization stability. `POLICY_PAYLOAD_VERSION` in `factory.py:32` versions the policy payload format.

The gap is that there is no explicit stability marker distinguishing stable public API from internal utilities. The `__all__` lists mix stable contracts (e.g., `register_rust_udfs`) with diagnostic utilities (e.g., `udf_audit_payload`).

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:1575-1596` -- `__all__` lists 18 names mixing core API (`register_rust_udfs`) with diagnostics (`udf_audit_payload`) and internals (`RustUdfSnapshot` type alias)

**Suggested improvement:**
Consider grouping `__all__` entries with comments distinguishing stable API from diagnostic/internal exports. This does not require code changes, just documentation.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
The most impactful testability barrier is the module-level global state described in P1 and P12. Testing `register_rust_udfs` requires either (a) a real Rust extension module installed and a real `SessionContext`, or (b) monkey-patching multiple module-level globals and `importlib.import_module`. There is no DI seam for the extension loader, the session registry, or the snapshot cache.

Pure functions (`parse_type_signature`, `build_create_function_sql`, `_normalize_registry_snapshot` modulo the global read, `validate_rust_udf_snapshot`) are testable in isolation, which is good.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py:34-45` -- 6 globals require monkey-patching to test lifecycle
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/factory.py:212-229` -- `_load_extension()` hardcodes `importlib.import_module`, untestable without patching
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/extensions/schema_runtime.py:22-48` -- `@lru_cache(maxsize=1)` on `load_schema_runtime` means once called, it cannot be overridden in tests without `cache_clear()`
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/catalog/introspection.py:373` -- Global dict not clearable via API

**Suggested improvement:**
(a) Encapsulate `extension_runtime.py` globals into a `RustUdfSessionRegistry` class with a `reset()` method for testing. (b) Add a `_clear_caches()` function to `introspection.py` for test cleanup. (c) Accept an optional `extension_loader` parameter in `_load_extension` and `_datafusion_internal` for test substitution. (d) Expose `load_schema_runtime.cache_clear` or accept a factory parameter.

**Effort:** large
**Risk if unaddressed:** high -- Without testability improvements, the UDF lifecycle can only be tested via expensive E2E tests with real Rust extensions.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
`runtime_capabilities.py` provides structured diagnostic snapshots with timestamps, version info, and compatibility status. `platform.py` uses `logging.getLogger(__name__)`. `capture_cache_diagnostics` in `introspection.py` produces structured cache metrics. `RegistrationMetadata` includes `registration_time_ms` and `table_spec_hash` for traceability.

The gap is that `extension_runtime.py` (the largest and most complex module) has no logging at all. Installation success/failure, ABI probing results, and snapshot normalization events are silent.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/extension_runtime.py` -- No `logging.getLogger` or any structured logging despite being the core runtime installation module
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/extensions/runtime_capabilities.py:16,113-128` -- Good: structured debug logging for plan capability detection
- `/Users/paulheyse/CodeAnatomy/src/datafusion_engine/udf/platform.py:105` -- Logger defined but minimally used

**Suggested improvement:**
Add structured debug logging to `extension_runtime.py` for: (a) Rust extension module resolution (`_datafusion_internal`), (b) snapshot capture and normalization, (c) UDF registration success with snapshot hash, (d) DDL registration. Use the existing `_LOGGER = logging.getLogger(__name__)` pattern.

**Effort:** small
**Risk if unaddressed:** low -- But lack of observability in the most complex module makes operational debugging harder than necessary.

---

## Cross-Cutting Themes

### Theme 1: Module-level global state as implicit registry

**Root cause:** The UDF lifecycle state management was designed around module-level globals (`WeakSet`, `WeakKeyDictionary`, `dict`) as a simple caching mechanism. As the scope grew to 8 global registries across 3 modules, this became an implicit, non-injectable, non-resettable state management system.

**Affected principles:** P1 (information hiding), P12 (DI), P23 (testability), P16 (functional core)

**Suggested approach:** Extract all module-level state into a `RustUdfSessionRegistry` class. Expose a module-level singleton instance for backward compatibility. Accept the registry as an optional constructor parameter for DI. Add a `reset()` method for test cleanup.

### Theme 2: Duplicated extension module loading

**Root cause:** Each module independently loads `datafusion_engine.extensions.datafusion_ext` because they were developed incrementally and the centralized adapter pattern (`context_adaptation.py`) was added later.

**Affected principles:** P7 (DRY), P6 (ports & adapters), P4 (coupling)

**Suggested approach:** Consolidate all extension module loading through `resolve_extension_module` in `context_adaptation.py`. Define the module name constant once.

### Theme 3: Untyped snapshot flowing through the system

**Root cause:** The Rust extension returns `Mapping[str, object]` payloads that are never parsed into a typed representation. The `RustUdfSnapshot = Mapping[str, object]` type alias provides no structural guarantees.

**Affected principles:** P9 (parse don't validate), P10 (illegal states), P15 (tell don't ask)

**Suggested approach:** Define a `RegistrySnapshot` msgspec Struct with typed fields. Parse the Rust payload into this struct once in `_normalize_registry_snapshot`. All downstream code operates on the typed struct. This also eliminates the need for `_require_mapping`, `_require_sequence`, `_require_bool_mapping` helper families.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 DRY | Centralize extension module name and ABI error message as constants | small | Eliminates 11+ duplicated error messages and 7+ duplicated module name strings |
| 2 | P24 Observability | Add structured logging to `extension_runtime.py` for installation and ABI probing | small | Makes the most complex module observable |
| 3 | P14 Demeter | Centralize `getattr(ctx, "ctx", None)` through `datafusion_ext._normalize_ctx` | small | Removes 8 instances of internal wrapper unwrapping |
| 4 | P8 Contracts | Replace placeholder docstring descriptions ("Args: snapshot: Description.") with real contracts | small | Improves discoverability and contract documentation |
| 5 | P20 YAGNI | Collapse identical `create_default_catalog` / `create_strict_catalog` | small | Removes dead abstraction |

## Recommended Action Sequence

1. **Centralize constants (P7):** Define `DATAFUSION_EXT_MODULE` and `abi_mismatch_error()` in `context_adaptation.py`. Update all 7+ import sites and 11+ error message sites. No behavioral change; purely mechanical.

2. **Add logging to extension_runtime.py (P24):** Add `_LOGGER = logging.getLogger(__name__)` and structured debug logging at key lifecycle points (module resolution, snapshot capture, registration, DDL install). No behavioral change.

3. **Centralize context unwrapping (P14):** Replace all `getattr(ctx, "ctx", None)` calls in the reviewed scope with calls to `context_adaptation` helpers or direct use of `datafusion_ext._normalize_ctx`.

4. **Introduce typed RegistrySnapshot struct (P9, P10, P15):** Define in `udf/contracts.py`. Parse once in `_normalize_registry_snapshot`. Update `metadata.py` and `parity.py` consumers. This is the highest-impact structural change.

5. **Extract RustUdfSessionRegistry class (P1, P12, P23):** Encapsulate the 6+2 module-level globals into a class. Expose `reset()` for testing. Preserve module-level singleton for backward compatibility.

6. **Split extension_runtime.py (P2, P3):** Extract snapshot validation, snapshot normalization, and DDL generation into focused modules. Keep `extension_runtime.py` focused on the runtime lifecycle.

7. **Route all extension loading through context_adaptation (P6):** Replace direct `importlib.import_module` calls in `factory.py`, `expr.py`, and `provider.py` with `resolve_extension_module`.

8. **Fix introspection cache key safety (P1):** Replace `dict[int, IntrospectionCache]` keyed by `id(ctx)` with `WeakKeyDictionary[SessionContext, IntrospectionCache]` to prevent stale entries after GC.
