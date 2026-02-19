# Design Review: DataFusion UDF, Views, Catalog, Compilation, Expr, SQL, Tables

**Date:** 2026-02-18
**Scope:** `src/datafusion_engine/udf/`, `src/datafusion_engine/views/`, `src/datafusion_engine/catalog/`, `src/datafusion_engine/compile/`, `src/datafusion_engine/expr/`, `src/datafusion_engine/sql/`, `src/datafusion_engine/tables/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 43

## Executive Summary

The DataFusion integration layer is architecturally ambitious and handles genuinely difficult coordination problems — installing Rust UDF platforms, managing view graph registration, enforcing schema contracts, and providing SQL execution safety. Most subsystems achieve their goals and several modules (`sql/guard.py`, `expr/query_spec.py`, `tables/spec.py`, `udf/runtime_snapshot_types.py`) are exemplary. However, three systemic problems undercut the layer's overall quality. First, `udf/extension_runtime.py` (1221 lines) has accreted responsibilities for six distinct concerns, culminating in two definitions of `register_rust_udfs` in the same file where the second silently shadows the first — a latent correctness hazard. Second, three independent CQS violations (`IntrospectionCache.snapshot`, `ParamTableRegistry.datafusion_tables`, `RegistrySchemaProvider.table`) allow state mutation to happen invisibly during reads. Third, the `create_strict_catalog` / `create_default_catalog` distinction makes a behavioral promise it does not keep: both functions are identical. Fixing these three themes would substantially raise the layer's correctness and maintainability profile.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | medium | `extension_registry.py` imports 10 private-named (`_`-prefixed) symbols from `extension_runtime.py` across the module boundary |
| 2 | Separation of concerns | 1 | large | medium | `register_view_graph` (graph.py) orchestrates 8+ distinct concerns; `extension_runtime.py` mixes 6 responsibility domains in 1221 lines |
| 3 | SRP | 1 | large | medium | `extension_runtime.py` changes for snapshot mgmt, validation, DDL, policy, lifecycle, and docs independently |
| 4 | High cohesion, low coupling | 2 | medium | low | Bottom-of-file import in `extension_runtime.py` and deferred imports in `views/registration.py` signal circular tension |
| 5 | Dependency direction | 2 | small | low | `views/registry_specs.py` instantiates `DataFusionIOAdapter` directly rather than receiving it via port |
| 6 | Ports and adapters | 2 | medium | low | `DataFusionIOAdapter` is instantiated at multiple call sites (views, tables, catalog) rather than injected |
| 7 | DRY | 1 | medium | medium | `create_default_catalog` and `create_strict_catalog` are identical; `_load_extension` duplicated between `factory.py` and `planner.py`; `_EXPR_HANDLER_NAMES` must stay in sync with handlers in `spec.py` |
| 8 | Design by contract | 1 | medium | high | `create_strict_catalog` promises strict-builtin-only enforcement and delivers nothing different; `udf_parity_report` always returns empty — a stub with a non-stub signature |
| 9 | Parse, don't validate | 3 | — | — | `udf/runtime_snapshot_types.py` decodes msgpack directly to typed structs; `tables/spec.py` parses `DatasetLocation` at boundary |
| 10 | Make illegal states unrepresentable | 1 | medium | medium | `create_default_catalog` / `create_strict_catalog` are indistinguishable at runtime; `UdfCatalog.list_functions_by_tier` ignores its `tier` argument |
| 11 | CQS | 1 | medium | high | Three violations: `IntrospectionCache.snapshot` property clears `_invalidated` on read; `ParamTableRegistry.datafusion_tables` registers tables as side effect; `RegistrySchemaProvider.table` records metadata during read |
| 12 | Dependency inversion + explicit composition | 2 | medium | low | `DataFusionIOAdapter` created ad-hoc at call sites throughout tables, views, and catalog modules rather than composed at construction |
| 13 | Prefer composition over inheritance | 3 | — | — | No significant inheritance hierarchies; composition via dataclass + protocol is the dominant pattern |
| 14 | Law of Demeter | 2 | small | low | `_resolve_registries` and `register_registry_catalog(profile=None)` access propagated state through helper chains |
| 15 | Tell, don't ask | 2 | small | low | `_install_function_factory_status` in `platform.py` inspects `state.runtime_payload` dict keys before deciding what to call |
| 16 | Functional core, imperative shell | 2 | medium | low | `register_view_graph` in `graph.py` mixes pure topology logic with IO side-effects; most leaf computations are pure |
| 17 | Idempotency | 2 | small | low | `register_rust_udfs` guards with `WeakSet`; `tables/registration.py` creates a fresh `ProviderRegistry` per call — ephemeral, but also unreusable |
| 18 | Determinism / reproducibility | 3 | — | — | Snapshot hashing, fingerprinting, and `WeakKeyDictionary`-based session isolation all support determinism |
| 19 | KISS | 1 | large | medium | `DataFusionCompileOptions` has 30+ fields; `udf/metadata.py` is 1214 lines; `extension_runtime.py` is 1221 lines |
| 20 | YAGNI | 2 | small | low | `extension_lifecycle.py` adds an indirection layer around functions that are already one call deep |
| 21 | Least astonishment | 1 | small | medium | `RegistrationPhase(validate=install_udf_platform)` — the `validate` parameter performs a side-effecting install action, not validation; `_CACHE_SNAPSHOT_ERRORS` includes bare `Exception` violating project policy |
| 22 | Declare and version public contracts | 2 | small | low | `extension_ddl.py` has `__all__: list[str] = []` (empty) while its private functions are called cross-module; `udf/parity.py` exports a stub as a public API |
| 23 | Design for testability | 2 | medium | low | `ExtensionRegistries` and `IntrospectionCaches` are injectable; the largest modules are hard to unit-test due to implicit side-effects inside queries |
| 24 | Observability | 2 | small | low | Artifact recording is inconsistent — `register_view_graph` records multiple artifacts but other paths (e.g., `register_table`) record nothing |

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding — Alignment: 1/3

**Current state:**
`extension_registry.py` imports ten private-named symbols (underscore-prefixed) directly from `extension_runtime.py`, bypassing `extension_runtime`'s public surface. This makes refactoring the internals of `extension_runtime.py` a breaking change for `extension_registry.py`, even though the two modules are nominally peers.

**Findings:**
- `src/datafusion_engine/udf/extension_registry.py:9-22` — imports `_async_udf_policy`, `_build_docs_snapshot`, `_build_registry_snapshot`, `_install_rust_udfs`, `_iter_snapshot_values`, `_notify_udf_snapshot`, `_registered_snapshot`, `_resolve_registries`, `_snapshot_alias_names`, `_validated_snapshot` — all underscore-prefixed private names from `extension_runtime`
- `src/datafusion_engine/catalog/provider_registry.py` — `_do_registration` calls `_build_registration_context` and `_register_dataset_with_context`, which are leading-underscore names from `datafusion_engine.dataset.registration_core`, repeated across `register_spec`, `register_delta`, `register_df`, `register_location`
- `src/datafusion_engine/udf/extension_runtime.py:477` — bottom-of-file import: `from datafusion_engine.udf.extension_ddl import _register_udf_aliases, _register_udf_specs` — private names consumed at module level, breaking the top-of-file import convention and indicating a circular-import workaround that was never cleaned up

**Suggested improvement:**
Promote the ten private functions in `extension_runtime.py` that `extension_registry.py` depends on into a dedicated internal submodule (e.g., `udf/_runtime_core.py`) with a documented public surface. Expose them from there without underscore prefix. This gives `extension_registry.py` a stable import surface and allows `extension_runtime.py`'s implementation to evolve independently. Similarly, the two functions imported from `extension_ddl.py` at line 477 should be moved to the top of the file or, better, promoted to a shared helper module.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P2. Separation of concerns — Alignment: 1/3

**Current state:**
Two modules concentrate unrelated concerns that change for different reasons: `extension_runtime.py` (1221 lines) and `views/graph.py`'s `register_view_graph` function.

**Findings:**
- `src/datafusion_engine/udf/extension_runtime.py` — in a single file: snapshot caching, ABI version validation, async UDF policy construction, DDL-based registration forwarding, documentation snapshot retrieval, runtime payload normalization, extension capability validation, `WeakKeyDictionary`-based lifecycle management, and msgpack serialization. Any one of these domains could change independently.
- `src/datafusion_engine/views/graph.py` — `register_view_graph` handles: topological sort of view nodes, cross-node dependency validation, required-UDF presence checks, schema contract enforcement per node, lineage recording, artifact emission for fingerprints and parity, scan-unit context propagation, and explain-analyze threshold configuration. This is at minimum six independent change drivers in one function.
- `src/datafusion_engine/views/registration.py:61-133` — `_ViewGraphRegistrationContext` is a mutable orchestration object whose methods include validation, platform installation, scan override application, and view node construction — four different phases of a pipeline mixed into one class

**Suggested improvement:**
For `extension_runtime.py`: extract three modules — `udf/_snapshot_cache.py` (WeakKeyDictionary snapshot caching + validation), `udf/_runtime_policy.py` (async policy + capability validation), and `udf/_ddl_bridge.py` (DDL-based registration forwarding). `extension_runtime.py` becomes a thin public re-export facade.

For `register_view_graph`: introduce a `ViewGraphRegistrationPipeline` dataclass that holds the steps as named callables, making each stage independently testable. The function orchestrates the steps rather than implementing them inline.

**Effort:** large
**Risk if unaddressed:** medium

---

#### P3. SRP — Alignment: 1/3

**Current state:**
`extension_runtime.py` fails the "one reason to change" test conclusively. Its change drivers include: the Rust ABI version changes, the async UDF policy semantics change, the DDL CREATE FUNCTION syntax changes, the snapshot caching strategy changes, the documentation format changes, or the capability validation rules change. Each is an independent axis.

**Findings:**
- `src/datafusion_engine/udf/extension_runtime.py` — 1221 lines; the bottom-of-file import at line 477 (`from datafusion_engine.udf.extension_ddl import _register_udf_aliases, _register_udf_specs`) followed immediately by a redefinition of `register_rust_udfs` at lines 480-509 exposes this accreted structure most clearly
- `src/datafusion_engine/udf/metadata.py` — 1214 lines; contains `DataFusionUdfSpec`, `FunctionCatalog`, `UdfCatalog`, `BuiltinFunctionSpec`, `FunctionSignature`, `RoutineMeta`, `ResolvedFunction`, `UdfCatalogAdapter`, and eight standalone helper functions for snapshot parsing — these are at least four distinct concepts
- `src/datafusion_engine/catalog/introspection.py` — 759 lines; mixes `IntrospectionSnapshot` (point-in-time capture), `IntrospectionCache` (invalidation management), `CacheConfigSnapshot` (configuration reading), `CacheStateSnapshot` (runtime state), and `capture_cache_diagnostics` (multi-cache aggregation)

**Suggested improvement:**
Split `metadata.py` into at minimum: `udf/_udf_spec.py` (DataFusionUdfSpec + snapshot parsing helpers), `udf/_udf_catalog.py` (UdfCatalog + FunctionCatalog), and `udf/_builtin_spec.py` (BuiltinFunctionSpec + BuiltinCategory). The existing `metadata.py` becomes a re-export module for backward compatibility.

Split `introspection.py` into `catalog/_introspection_snapshot.py` (IntrospectionSnapshot capture logic) and `catalog/_introspection_cache.py` (IntrospectionCache + invalidation helpers).

**Effort:** large
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling — Alignment: 2/3

**Current state:**
The bottom-of-file import in `extension_runtime.py` and the proliferation of deferred imports in `views/registration.py` are reliable indicators of circular-import tension. The pattern of instantiating `DataFusionIOAdapter` directly in `tables/param.py`, `views/registration.py`, and `catalog/provider.py` creates scattered construction points that are difficult to replace.

**Findings:**
- `src/datafusion_engine/udf/extension_runtime.py:477` — `from datafusion_engine.udf.extension_ddl import ...` placed after 470 lines of module body; this is a circular-import workaround that signals structural coupling between the two modules
- `src/datafusion_engine/views/registration.py:62-64, 79, 118, 211, 226` — six different deferred imports inside method bodies of `_ViewGraphRegistrationContext`, each hiding a dependency that is actually always required
- `src/datafusion_engine/tables/param.py:202, 273` — two independent instantiation sites of `DataFusionIOAdapter(ctx=ctx, profile=None)` within the same module

**Suggested improvement:**
Move the `extension_ddl` import to the top of `extension_runtime.py` and resolve the circular dependency by extracting a shared base module. Collapse the deferred imports in `_ViewGraphRegistrationContext` into top-level imports once the circularity is resolved — or make the dependencies explicit constructor parameters.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P5. Dependency direction — Alignment: 2/3

**Current state:**
`views/registry_specs.py` (the view registration authority) instantiates `DataFusionIOAdapter` directly inside `_semantics_view_nodes` and `_nested_view_nodes`. This makes the view graph depend on a concrete adapter class rather than expressing its need as a port.

**Findings:**
- `src/datafusion_engine/views/registry_specs.py` — `_semantics_view_nodes(ctx, ...)` and `_nested_view_nodes(ctx, ...)` both call `DataFusionIOAdapter(ctx=ctx, profile=runtime_profile)` internally, coupling the view node building layer to the concrete IO adapter

**Suggested improvement:**
Express the view graph's IO need as a parameter typed to a `DataFusionIOPort` protocol that `DataFusionIOAdapter` satisfies. `view_graph_nodes(ctx, *, adapter: DataFusionIOPort, ...)` would allow tests to inject a lightweight fake adapter and allow future alternate adapters without touching registry logic.

**Effort:** small
**Risk if unaddressed:** low

---

#### P6. Ports and adapters — Alignment: 2/3

**Current state:**
`DataFusionIOAdapter` is the project's primary port implementation but it is constructed in-place at call sites rather than composed at wiring time. Three independent modules (`tables/param.py`, `catalog/provider.py`, `views/registration.py`) each create their own adapter instances with `profile=None`, losing the runtime context.

**Findings:**
- `src/datafusion_engine/tables/param.py:202` — `DataFusionIOAdapter(ctx=ctx, profile=None)` inside `ParamTableRegistry.datafusion_tables`
- `src/datafusion_engine/tables/param.py:273` — second `DataFusionIOAdapter(ctx=ctx, profile=None)` inside `register_table_params`
- `src/datafusion_engine/catalog/provider.py` — `register_registry_catalog` creates `DataFusionIOAdapter(ctx=ctx, profile=None)`, dropping the caller's runtime profile
- `src/datafusion_engine/views/registration.py:68` — `install_rust_udf_platform(..., ctx=self.ctx)` does not pass the adapter; platform installation is isolated from the view registration adapter

**Suggested improvement:**
Pass a pre-constructed `DataFusionIOAdapter` into `ParamTableRegistry.datafusion_tables` and `register_table_params` as a parameter. This makes the dependency explicit and allows callers to provide an adapter carrying the full runtime profile context.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Knowledge

#### P7. DRY — Alignment: 1/3

**Current state:**
Three independent cases of duplicated knowledge across the scope.

**Findings:**
- `src/datafusion_engine/udf/metadata.py:868-902` — `create_default_catalog` (line 868) and `create_strict_catalog` (line 887) are byte-for-byte identical function bodies: both return `UdfCatalog(udf_specs=udf_specs)`. The "strict" behavioral distinction promised by the name is never implemented.
- `src/datafusion_engine/udf/factory.py` and `src/datafusion_engine/expr/planner.py` — both contain a private `_load_extension()` function that follows the same `importlib.import_module(EXTENSION_MODULE_PATH)` pattern with identical try/except structure
- `src/datafusion_engine/expr/dispatch.py` — `_EXPR_HANDLER_NAMES` (the authoritative list of handler names) and the actual handler functions in `spec.py` are maintained as two independent structures. Adding or removing a handler requires editing both files.
- `src/datafusion_engine/compile/options.py` — `DataFusionDmlOptions` repeats seven fields that also appear in `DataFusionCompileOptions`: `sql_options`, `sql_policy`, `sql_policy_name`, `params`, `named_params`, `runtime_profile`, `dialect`

**Suggested improvement:**
Delete `create_default_catalog` or implement the strict policy genuinely (e.g., by adding a `strict: bool` field to `UdfCatalog` that rejects registrations outside the builtin tier). Extract `_load_extension` into a shared `udf/_extension_loader.py` module. Make `_EXPR_HANDLER_NAMES` in `dispatch.py` derived automatically from the handler registry rather than maintained manually (e.g., using a decorator that registers handler functions). Extract the shared SQL option fields from both options classes into a `SqlExecutionOptions` base dataclass.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P8. Design by contract — Alignment: 1/3

**Current state:**
Two explicit contract violations: `create_strict_catalog` documents a guarantee it never enforces, and `udf_parity_report` documents a behavior (detecting mismatches) it never exercises.

**Findings:**
- `src/datafusion_engine/udf/metadata.py:887-902` — docstring states "Create a UDF catalog with strict builtin-only policy" and "Catalog with strict builtin-only configuration." The implementation is `return UdfCatalog(udf_specs=udf_specs)` — identical to the default catalog. Any caller relying on the strictness guarantee is silently wrong.
- `src/datafusion_engine/udf/parity.py:109-127` — `udf_parity_report` validates the snapshot then immediately returns `UdfParityReport(missing_in_rust=(), param_name_mismatches=(), volatility_mismatches=())` — always empty regardless of actual state. The docstring says "Return parity mismatches" but the function always returns no mismatches.
- `src/datafusion_engine/catalog/introspection.py:25-32` — `_CACHE_SNAPSHOT_ERRORS` includes bare `Exception` as its last entry. Project policy (`.claude/rules/python-quality.md`) explicitly prohibits `except Exception:`. This is a contract violation against the project's own quality rules.
- `src/datafusion_engine/views/graph.py` — `_validate_required_functions` catches `(RuntimeError, TypeError, ValueError)` broadly, which may silently swallow genuine programming errors rather than expected missing-function conditions

**Suggested improvement:**
Either implement the strict enforcement in `create_strict_catalog` (add a `strict: bool` to `UdfCatalog` that raises on non-builtin resolution) or delete the function and redirect callers to `create_default_catalog`. For `udf_parity_report`, either implement the comparison against `information_schema` or mark the stub explicitly with `raise NotImplementedError("parity comparison not yet implemented")` so callers are not misled. Remove `Exception` from `_CACHE_SNAPSHOT_ERRORS` and replace with specific exception types.

**Effort:** medium
**Risk if unaddressed:** high

---

#### P9. Parse, don't validate — Alignment: 3/3

The scope is well-aligned with this principle. `udf/runtime_snapshot_types.py` decodes Rust msgpack payloads directly into typed `RustUdfSnapshot` and `RuntimeInstallSnapshot` structs at the boundary, with coercion functions (`coerce_rust_udf_snapshot`, `normalize_runtime_install_snapshot`) that convert untyped mappings to typed contracts. `tables/spec.py`'s `table_spec_from_location` converts a `DatasetLocation` into a `TableSpec` at ingestion time. `expr/query_spec.py` builds structured `QuerySpec` and `ProjectionSpec` objects from raw column lists. No validation-in-place anti-patterns observed.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P10. Make illegal states unrepresentable — Alignment: 1/3

**Current state:**
The `create_default_catalog` / `create_strict_catalog` API creates a false distinction that cannot be enforced or detected at the type level. Code that depends on strict enforcement has no way to verify it received the strict variant.

**Findings:**
- `src/datafusion_engine/udf/metadata.py:868-902` — both `create_default_catalog` and `create_strict_catalog` return the same concrete type `UdfCatalog` with no distinguishing field, flag, or type tag. The "strict" semantic is invisible to the type system and unverifiable at runtime.
- `src/datafusion_engine/udf/metadata.py` — `UdfCatalog.list_functions_by_tier` (inferred from its signature) ignores the `tier` parameter (`_ = tier`), meaning the tier concept exists in the type signature but is not enforced at runtime. Callers cannot rely on tier-based filtering.
- `src/datafusion_engine/udf/extension_runtime.py` — the `RustUdfSnapshot` type alias (line 70) is `Mapping[str, object]` — a fully untyped alias. Callers that receive this type get no structural guarantees. The typed `RustUdfSnapshot` msgspec struct exists in `runtime_snapshot_types.py` but is not used consistently as the return type of the public snapshot API.

**Suggested improvement:**
Add a `strict: bool = False` field to `UdfCatalog` and enforce it in the resolution path. Alternatively, use `Literal["strict"] | Literal["default"]` as a type tag. Replace the `Mapping[str, object]` type alias with the typed `RustUdfSnapshot` msgspec struct as the authoritative return type for snapshot-returning functions.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P11. CQS — Alignment: 1/3

**Current state:**
Three functions/properties in the scope mix state mutation with data retrieval, violating CQS.

**Findings:**
- `src/datafusion_engine/catalog/introspection.py:346-363` — `IntrospectionCache.snapshot` (property, i.e., a query) clears `self._invalidated = False` at line 362 as a side effect of reading. A caller that reads `snapshot` twice in sequence gets different behavior from one that reads once — the first read silently resets the invalidation flag.

  ```python
  @property
  def snapshot(self) -> IntrospectionSnapshot:
      if self._snapshot is None or self._invalidated:
          self._snapshot = IntrospectionSnapshot.capture(self._ctx, ...)
          self._invalidated = False  # mutation inside a property getter
      return self._snapshot
  ```

- `src/datafusion_engine/tables/param.py:195-208` — `ParamTableRegistry.datafusion_tables` (named like a query) registers Arrow tables into the `DataFusionIOAdapter` as a side effect before returning the DataFrame dict. The method mutates the session catalog while appearing to only read tables.

  ```python
  def datafusion_tables(self, ctx: SessionContext) -> dict[str, DataFrame]:
      adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
      tables: dict[str, DataFrame] = {}
      for logical_name, artifact in self.artifacts.items():
          adapter.register_arrow_table(physical_name, artifact.table, overwrite=True)  # side effect
          tables[logical_name] = ctx.table(physical_name)
      return tables
  ```

- `src/datafusion_engine/catalog/provider.py` — `RegistrySchemaProvider.table` records metadata as a side effect of returning a table reference.

**Suggested improvement:**
For `IntrospectionCache.snapshot`: separate the recapture into an explicit `refresh()` command method; the `snapshot` property should be a pure getter that returns `self._snapshot` (possibly `None`). Callers check for `None` and call `refresh()` if needed.

For `ParamTableRegistry.datafusion_tables`: rename to `register_and_get_tables(ctx)` to signal the combined intent, or — better — split into `register_tables(ctx) -> None` (command) and `table_names() -> tuple[str, ...]` (query that returns logical names only). Callers then retrieve DataFrames via `ctx.table(name)`.

**Effort:** medium
**Risk if unaddressed:** high

---

### Category: Composition

#### P12. Dependency inversion + explicit composition — Alignment: 2/3

**Current state:**
`DataFusionIOAdapter` is the primary adapter for session-level IO, but it is constructed ad-hoc at four independent call sites inside the scope with `profile=None`, losing the runtime profile context that higher-level orchestrators have available. This is not a full inversion problem — the adapter exists and callers do depend on it — but the construction is not composed at the wiring layer.

**Findings:**
- `src/datafusion_engine/tables/param.py:202` — `DataFusionIOAdapter(ctx=ctx, profile=None)`
- `src/datafusion_engine/tables/param.py:273` — second independent `DataFusionIOAdapter(ctx=ctx, profile=None)`
- `src/datafusion_engine/catalog/provider.py` — `DataFusionIOAdapter(ctx=ctx, profile=None)` inside `register_registry_catalog`

**Suggested improvement:**
Accept a pre-constructed adapter as a parameter in `ParamTableRegistry.datafusion_tables(ctx, *, adapter: DataFusionIOAdapter)` and `register_table_params(ctx, bindings, *, adapter: DataFusionIOAdapter)`. This allows callers that already have an adapter with full profile context to pass it through, avoiding the `profile=None` loss.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P13. Prefer composition over inheritance — Alignment: 3/3

The scope uses composition consistently. `UdfCatalog` holds a `FunctionCatalog`. `RustUdfPlatform` holds `ExtensionInstallStatus` values. `IntrospectionCaches` holds `WeakKeyDictionary[SessionContext, IntrospectionCache]`. No deep class hierarchies or mixin-based designs observed. Score 3 — well aligned.

---

#### P14. Law of Demeter — Alignment: 2/3

**Current state:**
A few places chain through intermediate objects to reach final state.

**Findings:**
- `src/datafusion_engine/views/registration.py:268-279` — `profile.features.enable_udfs`, `profile.features.enable_async_udfs`, `profile.policies.async_udf_timeout_ms`, `profile.policies.async_udf_batch_size` — `_platform_options` traverses two levels of nested objects (`profile.features.X`, `profile.policies.X`) for every field
- `src/datafusion_engine/views/graph.py` — multiple sites traverse `node.contract_builder(...)` then inspect the resulting `SchemaContract` fields, chaining through builder → contract → violation in a single expression chain

**Suggested improvement:**
Add accessor methods to `DataFusionRuntimeProfile` that return the UDF platform options directly (e.g., `profile.udf_platform_options() -> RustUdfPlatformOptions`) instead of requiring callers to traverse `profile.features` and `profile.policies` independently.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask — Alignment: 2/3

**Current state:**
`_install_function_factory_status` in `platform.py` interrogates `state.runtime_payload` dict keys before deciding which installation path to take, rather than delegating the decision to the state object.

**Findings:**
- `src/datafusion_engine/udf/platform.py:314-315` — `if state.snapshot is not None and "function_factory_installed" in state.runtime_payload:` followed by `installed = bool(state.runtime_payload.get("function_factory_installed"))` — the caller asks about internal payload structure to make a routing decision that `_PlatformInstallState` could make itself

**Suggested improvement:**
Add a `function_factory_installed(self) -> bool | None` method to `_PlatformInstallState` that encapsulates the dict-key inspection. `_install_function_factory_status` then calls `state.function_factory_installed()` and branches on the result.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness

#### P16. Functional core, imperative shell — Alignment: 2/3

**Current state:**
The leaf computation modules (`expr/query_spec.py`, `sql/helpers.py`, `tables/spec.py`, `udf/signature.py`) are effectively pure. The orchestration layer (`views/graph.py`) mixes topology sorting (pure) with schema enforcement and artifact recording (impure) inside the same function body.

**Findings:**
- `src/datafusion_engine/views/graph.py` — `register_view_graph` interleaves pure logic (topological sort validation, schema contract building) with IO operations (calling `record_artifact`, `register_view_graph`, `ctx.table()`) in a flat sequence, making the pure parts non-testable without a live session context

**Suggested improvement:**
Extract the pure topology validation and schema contract computation into a standalone `compute_view_registration_plan(nodes, snapshot) -> ViewRegistrationPlan` function that returns a plan object (pure, testable). The imperative shell then executes the plan against a live session context. This follows the functional-core / imperative-shell split.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency — Alignment: 2/3

**Current state:**
The UDF registration path (`register_rust_udfs`) is idempotent — the `WeakSet` guard at `extension_runtime.py` prevents double-registration. `register_view_graph` is not idempotent by design (calling it twice produces duplicate views). `tables/registration.py` creates a new `ProviderRegistry` on each call.

**Findings:**
- `src/datafusion_engine/tables/registration.py` — `register_table` creates `ProviderRegistry(ctx=ctx, runtime_profile=...)` fresh on each invocation. While this is not a correctness hazard on its own (each call is isolated), it means any caching or deduplication must happen at the caller level.
- `src/datafusion_engine/views/graph.py` — `register_view_graph` has no idempotency guard. Double-calling with the same nodes would attempt to re-register views that already exist, likely producing errors that bubble up as `SchemaContractViolationError` or DataFusion registration errors.

**Suggested improvement:**
The view graph registration could add an idempotency check using the view fingerprint hash — if all fingerprints match the current registration state, skip. This is particularly important for callers that invoke `ensure_view_graph` defensively.

**Effort:** small
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility — Alignment: 3/3

The scope handles determinism carefully. `RustUdfPlatform.snapshot_hash` provides a stable identity for the installed platform state. `view_fingerprint_payload` records per-view fingerprints. `FunctionFactoryPolicy` participates in the plan hash. `WeakKeyDictionary`-based session isolation prevents cross-session contamination. `DataFusionCompileOptions` supports `policy_hash` field for plan reproducibility. No random or time-based ordering observed in core logic paths. Score 3 — well aligned.

---

### Category: Simplicity

#### P19. KISS — Alignment: 1/3

**Current state:**
Three modules are significantly over-large and represent unnecessary complexity in the sense that the complexity serves no KISS-irreducible need.

**Findings:**
- `src/datafusion_engine/compile/options.py` — `DataFusionCompileOptions` has 30+ fields. Many are rarely used defaults that could be grouped into sub-option dataclasses (`SqlCompileOptions`, `AsyncCompileOptions`, `CachingCompileOptions`) to make the common path simpler.
- `src/datafusion_engine/udf/metadata.py` — 1214 lines in a single file. The file contains four conceptually distinct entity groups (UDF specs, catalogs, builtins, snapshot parsers) that are navigated by scrolling.
- `src/datafusion_engine/udf/extension_runtime.py` — 1221 lines. The two `register_rust_udfs` definitions (one at the top and a forwarding wrapper at lines 480-509) demonstrate that the file has outgrown coherent navigation.
- `src/datafusion_engine/udf/extension_runtime.py:480-509` — the second definition of `register_rust_udfs` silently shadows the first. Any static analysis that resolves names to their first definition would resolve to the wrong function. This is a latent correctness hazard in addition to a complexity violation.

**Suggested improvement:**
For `DataFusionCompileOptions`: group fields into three nested option structs and reduce the top-level dataclass to those nested structs plus the most commonly-used top-level flags. For the large UDF modules: split as described under P3. Remove the duplicate `register_rust_udfs` definition immediately — this is the highest-priority simplicity fix.

**Effort:** large
**Risk if unaddressed:** medium

---

#### P20. YAGNI — Alignment: 2/3

**Current state:**
`extension_lifecycle.py` is a thin wrapper around functions that are already simple.

**Findings:**
- `src/datafusion_engine/udf/extension_lifecycle.py` — 42 lines; `install_udfs` delegates to `register_rust_udfs`, `install_udf_ddl` delegates to `register_udfs_via_ddl`. This is an indirection layer with no additional value over calling the underlying functions directly. If the only purpose is stable naming, the module should export the underlying functions directly under the stable names rather than wrapping them.

**Suggested improvement:**
Replace `extension_lifecycle.py` with a direct re-export: `from datafusion_engine.udf.extension_runtime import register_rust_udfs as install_udfs, register_udfs_via_ddl as install_udf_ddl`. This preserves the stable name surface without adding an indirection layer.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment — Alignment: 1/3

**Current state:**
Two naming decisions violate reasonable expectations for their callers.

**Findings:**
- `src/datafusion_engine/views/registration.py:140-178` — `RegistrationPhase(validate=registration.install_udf_platform)` — the `validate` parameter is named as if it performs validation, but it actually performs the installation side effect. All six uses of `RegistrationPhase` pass fully side-effecting functions (install, apply, build, register) to this `validate` parameter. A reader who encounters a `RegistrationPhase` in test output or error traces expects the phase to have validated something, not installed a UDF platform.

  ```python
  RegistrationPhase(
      name="udf_platform",
      requires=("semantic_registry",),
      validate=registration.install_udf_platform,  # installs, does not validate
  )
  ```

- `src/datafusion_engine/catalog/introspection.py:25-32` — `_CACHE_SNAPSHOT_ERRORS` includes bare `Exception` as its final entry. This is a policy violation per `.claude/rules/python-quality.md` ("No `except Exception:` — Too broad for most cases") and is also surprising because the explicit preceding entries (`AttributeError`, `KeyError`, etc.) suggest precision, while the trailing `Exception` negates all that precision.

- `src/datafusion_engine/udf/metadata.py:857-902` — `create_strict_catalog` implies behavioral difference from `create_default_catalog`. Finding them identical is a violation of the principle of least surprise for any caller who chose `create_strict_catalog` specifically for its documented behavior.

**Suggested improvement:**
Rename `validate` in `RegistrationPhase` to `action` or `execute`. Update the `RegistrationPhaseOrchestrator` accordingly. For `_CACHE_SNAPSHOT_ERRORS`: remove `Exception` from the tuple and use only specific exceptions. Add a comment explaining why each exception is included.

**Effort:** small
**Risk if unaddressed:** medium

---

#### P22. Declare and version public contracts — Alignment: 2/3

**Current state:**
`extension_ddl.py` declares `__all__: list[str] = []` (an empty export list) while its private functions are called cross-module. `parity.py` exports a stub as if it were a complete API.

**Findings:**
- `src/datafusion_engine/udf/extension_ddl.py` — `__all__: list[str] = []` — the module deliberately hides all exports, but `_register_udf_aliases` and `_register_udf_specs` are imported directly from it by `extension_runtime.py:477`. The module is both "private" (empty `__all__`) and used cross-module. This ambiguity makes it impossible to know what the module's intended surface is.
- `src/datafusion_engine/udf/parity.py:109-127` — `udf_parity_report` is in `__all__` (inferred from `__all__` pattern elsewhere in the module) and documented as a public API, but it is a permanent stub that always returns empty results. There is no deprecation marker, `NotImplementedError`, or comment indicating its stub status.

**Suggested improvement:**
For `extension_ddl.py`: either promote `_register_udf_aliases` and `_register_udf_specs` to public names and add them to `__all__`, or delete the module and inline its logic into its one caller. For `udf_parity_report`: add a `# TODO: implement comparison` comment at minimum, or raise `NotImplementedError` so callers discover the stub status rather than receiving silently empty results.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality

#### P23. Design for testability — Alignment: 2/3

**Current state:**
The injectable registries (`ExtensionRegistries`, `IntrospectionCaches`, `RustUdfPlatformRegistries`) are a strong testability design. The primary testability gap is that CQS violations (P11) make it impossible to test reads in isolation from state changes.

**Findings:**
- `src/datafusion_engine/udf/extension_runtime.py` — the `ExtensionRegistries` dataclass (lines 46-63) is injectable and enables unit testing of the registration lifecycle without a live Rust extension
- `src/datafusion_engine/catalog/introspection.py` — `IntrospectionCaches` is injectable, enabling test control of the cache state
- However, `IntrospectionCache.snapshot` (P11 violation) means tests that read `snapshot` cannot observe the invalidation state without also triggering a recapture — making testing of the invalidation logic awkward
- `src/datafusion_engine/views/graph.py` — `register_view_graph` requires a live `SessionContext`, live view nodes, and a live snapshot to test any part of it. The mixing of pure and impure logic (P16 finding) is the root cause.

**Suggested improvement:**
The testability improvements follow directly from fixing P11 and P16. Separating the invalidation check from the recapture (P11 fix) and extracting pure topology validation (P16 fix) would each independently improve unit testability without infrastructure requirements.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P24. Observability — Alignment: 2/3

**Current state:**
`register_view_graph` records multiple artifacts (UDF snapshot, view fingerprints, UDF parity, contract violations), but the artifact recording pattern is inconsistent across the scope. Table registration, UDF DDL registration, and catalog snapshot capture produce no structured artifacts.

**Findings:**
- `src/datafusion_engine/views/graph.py` and `views/registration.py:232-250` — `ensure_view_graph` records three artifacts: `RUST_UDF_SNAPSHOT_SPEC`, `VIEW_UDF_PARITY_SPEC`, `VIEW_FINGERPRINTS_SPEC`. This is good structured observability.
- `src/datafusion_engine/tables/registration.py` — `register_table` and `register_listing_table` produce no artifacts and no structured log events beyond what DataFusion itself emits
- `src/datafusion_engine/catalog/introspection.py:600-650` (inferred from `capture_cache_diagnostics`) — cache diagnostics exist but are aggregated into a flat payload rather than emitting named metrics per cache
- `src/datafusion_engine/udf/extension_runtime.py` — `register_rust_udfs` does not emit a structured artifact recording which UDFs were registered or what hash they carried; that information is only available via the returned snapshot

**Suggested improvement:**
Add a `_LOGGER.debug(...)` structured event at the end of `register_table` and `register_listing_table` logging the table name, location, and schema fingerprint. For the UDF registration path, log the snapshot hash and the count of registered UDF names as a debug event. This provides correlatable observability without requiring artifact storage.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: The Duplicate `register_rust_udfs` — Latent Correctness Hazard

`src/datafusion_engine/udf/extension_runtime.py` contains two definitions of `register_rust_udfs` in the same file. The first (from `extension_registry.py` via internal usage) establishes the real implementation. At line 480, the module defines a second `register_rust_udfs` that delegates to `extension_registry.register_rust_udfs` via a deferred import. In Python, the second definition silently replaces the first in the module's namespace. Any caller importing `register_rust_udfs` from `extension_runtime` receives the forwarding wrapper, which adds one indirection hop but does not change the behavior.

This is currently harmless (the wrapper delegates correctly) but is a time bomb: any future edit to the module that mistakenly calls the local name `register_rust_udfs` before line 480 (e.g., in a function added in the 1-480 range) would invoke the first definition, not the wrapper. It is also confusing to any reader who tries to understand the function by reading its definition in the file. The fix is to remove the bottom-of-file redefinition and replace it with a deferred import alias at the module level:

```python
# In extension_runtime.py, at the bottom:
# REMOVE the second def register_rust_udfs(...) definition
# REPLACE with:
def register_rust_udfs(ctx, *, enable_async=False, ...):
    from datafusion_engine.udf.extension_registry import register_rust_udfs as _impl
    return _impl(ctx, ...)
# OR: resolve the circular import and just use a top-level import alias
```

**Affected principles:** P3, P7, P19, P8
**Suggested approach:** Remove the duplicate definition. Resolve the circular import between `extension_runtime.py` and `extension_registry.py` by extracting shared low-level helpers to `udf/_runtime_core.py`.

---

### Theme 2: CQS Violations Enable Silent State Corruption

Three independent CQS violations in the scope (`IntrospectionCache.snapshot`, `ParamTableRegistry.datafusion_tables`, `RegistrySchemaProvider.table`) share a common root: session-catalog state is mutated inside methods that are named and typed as queries. This creates two practical risks:

1. **Logging/tracing adds a side effect**: If any monitoring code reads `cache.snapshot` for observability purposes, it triggers catalog recapture and resets the invalidation flag — an effect that should only happen when the caller explicitly intends to refresh.

2. **Test isolation is impossible**: A test that reads `snapshot` before asserting invalidation state will silently change the state it is trying to observe.

**Affected principles:** P11, P23
**Suggested approach:** Separate every query from its mutation. For `IntrospectionCache`: add `refresh() -> None` (command) and change `snapshot` to return `self._snapshot` (raises if `None`). For `ParamTableRegistry.datafusion_tables`: split into `register_tables(ctx) -> None` and a query for the logical names.

---

### Theme 3: The False Strict/Default Catalog Distinction

`create_strict_catalog` and `create_default_catalog` are functionally identical (`metadata.py:868-902`). This false distinction propagates into callers: any code that chose `create_strict_catalog` specifically to enforce tier isolation is silently unprotected. Because both functions return the same concrete type with no distinguishing field, a downstream audit cannot determine whether a given `UdfCatalog` instance was intended to be strict. The "strict" documentation creates a false sense of security.

**Affected principles:** P7, P8, P10, P21
**Suggested approach:** Either genuinely implement strict enforcement (add `strict: bool` to `UdfCatalog` and enforce it in `resolve`), or delete `create_strict_catalog` and direct all callers to `create_default_catalog` with a comment explaining the single tier.

---

### Theme 4: File Size as a Maintenance Risk

Two files (`extension_runtime.py` at 1221 lines, `metadata.py` at 1214 lines) are approaching the size where a competent reader cannot hold the entire file in working memory. The duplicate function definition in `extension_runtime.py` is a concrete consequence of this: it is the kind of mistake that happens when a file is large enough that an author adding a new function at the bottom does not realize the name already exists at the top.

**Affected principles:** P2, P3, P19
**Suggested approach:** Apply the module-splitting recommendations from P3. Size alone is not a problem, but when combined with mixed responsibilities, file size amplifies the difficulty of recognizing errors.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P21 / P8 | Rename `RegistrationPhase.validate` to `action` in `views/registration.py` to match its side-effecting semantics | small | Immediate clarity for all readers of registration phases |
| 2 | P8 | Remove bare `Exception` from `_CACHE_SNAPSHOT_ERRORS` in `catalog/introspection.py:31` and replace with specific exception types | small | Eliminates policy violation; prevents accidental swallowing of programming errors |
| 3 | P7 / P10 | Delete `create_strict_catalog` in `udf/metadata.py:887-902` or implement the strict enforcement — the current stub creates false security guarantees | small | Eliminates a dangerous false API contract |
| 4 | P22 | Mark `udf_parity_report` in `udf/parity.py:109-127` as a stub with `# NOTE: stub — always returns empty` comment, or raise `NotImplementedError` | small | Prevents callers from relying on empty results as accurate data |
| 5 | P19 / correctness | Remove the duplicate `register_rust_udfs` definition at `extension_runtime.py:480-509` — resolve the underlying circular import to eliminate the need for the bottom-of-file import at line 477 | medium | Eliminates a latent correctness hazard and reduces cognitive load in the largest file in scope |

## Recommended Action Sequence

1. **Remove the duplicate `register_rust_udfs` definition** (`extension_runtime.py:480-509`) and the bottom-of-file import at line 477. This is the highest-correctness-risk item. Resolve the circular dependency between `extension_runtime.py` and `extension_registry.py` by extracting shared low-level helpers to a new `udf/_runtime_core.py` module. (P3, P8, P19)

2. **Rename `RegistrationPhase.validate` to `action`** in `datafusion_engine/registry_facade.py` (the definition) and update all six call sites in `views/registration.py`. (P21)

3. **Remove `Exception` from `_CACHE_SNAPSHOT_ERRORS`** in `catalog/introspection.py:31`. Replace with the specific exceptions that are actually expected (document why each is included). (P8, P21)

4. **Fix the CQS violation in `IntrospectionCache.snapshot`** (`catalog/introspection.py:346-363`). Add an explicit `refresh()` command method; make `snapshot` a pure getter that returns `self._snapshot` (possibly `None` when not yet captured or invalidated). Update callers to call `refresh()` explicitly before accessing `snapshot`. (P11)

5. **Eliminate or implement `create_strict_catalog`** (`udf/metadata.py:887-902`). Decision: either add `strict: bool` to `UdfCatalog` and enforce it, or delete `create_strict_catalog` and redirect callers. (P7, P8, P10)

6. **Mark `udf_parity_report` as a stub** (`udf/parity.py:109-127`) with an explicit comment or `NotImplementedError`. (P8, P22)

7. **Split `IntrospectionCache.datafusion_tables` from `ParamTableRegistry.datafusion_tables`** (`tables/param.py:195-208`). Rename to `register_tables(ctx) -> None` (command) and provide a separate query for the logical table names. (P11)

8. **Extract shared `_load_extension()` helper** from `udf/factory.py` and `expr/planner.py` into `udf/_extension_loader.py`. (P7)

9. **Split `extension_runtime.py`** (1221 lines) into `udf/_snapshot_cache.py`, `udf/_runtime_policy.py`, and `udf/_ddl_bridge.py` with `extension_runtime.py` as a re-export facade. (P2, P3, P19)

10. **Split `metadata.py`** (1214 lines) into `udf/_udf_spec.py`, `udf/_udf_catalog.py`, and `udf/_builtin_spec.py` with `metadata.py` as a re-export facade for backward compatibility. (P2, P3, P19)
