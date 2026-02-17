# Design Review: DataFusion Dataset + UDF + Catalog + Tables + Extract

**Date:** 2026-02-17
**Scope:** `src/datafusion_engine/dataset`, `src/datafusion_engine/udf`, `src/datafusion_engine/tables`, `src/datafusion_engine/extract`, `src/datafusion_engine/catalog`
**Focus:** All principles (1-24) with emphasis on composition (12-15) and knowledge (7-11)
**Depth:** moderate
**Files reviewed:** 50 (16,427 LOC)

## Executive Summary

The DataFusion engine layer exhibits strong alignment in dependency direction, determinism, and contract declaration. The five sub-packages collectively provide a well-layered adapter system that properly wraps DataFusion's native SessionContext, CatalogProvider, and UDF registration APIs. The primary design concerns center on: (1) duplicated DDL type-alias knowledge across `dataset/registration_core.py` and `udf/extension_core.py`, (2) module-level mutable global state in `tables/metadata.py` that undermines testability and CQS, (3) several Law-of-Demeter violations in the `registry_snapshot()` function where deeply chained attribute access spans 3-4 levels, and (4) an overly large public alias surface in `registration_core.py` that re-exports ~30 private symbols from extracted helper modules, creating a facade that obscures true module boundaries.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `registration_core.py` re-exports ~30 private symbols as public aliases |
| 2 | Separation of concerns | 2 | medium | medium | Policy resolution, DDL generation, and provider registration interleaved in `registration_core.py` |
| 3 | SRP | 2 | medium | medium | `extension_core.py` (695 LOC) handles loading, snapshot normalization, modular/unified install, and DDL registration |
| 4 | High cohesion, low coupling | 2 | medium | low | `DatasetCaches` uses 3 separate WeakKeyDictionary fields; `ExtensionRegistries` uses 7 |
| 5 | Dependency direction | 2 | medium | medium | `semantic_catalog.py` imports from `semantics.*` (inner ring) creating bidirectional engine-semantics coupling |
| 6 | Ports & Adapters | 3 | - | - | `DataFusionIOAdapter` cleanly wraps DataFusion API; Rust extension module loaded via capability-gated adapter pattern |
| 7 | DRY (knowledge) | 1 | small | high | `_DDL_TYPE_ALIASES` duplicated between `registration_core.py:130` and `extension_core.py:64` |
| 8 | Design by contract | 2 | small | low | `DatasetPolicyRequest` has explicit fields but some functions accept untyped `Mapping[str, object]` snapshots |
| 9 | Parse, don't validate | 2 | medium | low | UDF snapshot is a `Mapping[str, object]` dict threaded everywhere; `_normalize_registry_snapshot` does validation inline |
| 10 | Make illegal states unrepresentable | 2 | medium | low | `DatasetLocation` allows `delta_version` and `delta_timestamp` simultaneously; runtime check in `provider.py:94` |
| 11 | CQS | 2 | small | low | `_install_rust_udfs` both mutates `registries.udf_snapshots` and returns the snapshot |
| 12 | DI + explicit composition | 2 | medium | medium | `_TABLE_PROVIDER_METADATA_BY_CONTEXT` is module-level global; `DatasetCaches` and `ExtensionRegistries` are injectable but fallback to fresh instances |
| 13 | Composition over inheritance | 3 | - | - | All catalog providers use dataclass composition; `UdfCatalogAdapter` wraps `UdfCatalog` via delegation |
| 14 | Law of Demeter | 1 | medium | medium | `registry_snapshot()` accesses `resolved.delta_write_policy.parquet_writer_policy.statistics_enabled` (4 levels) |
| 15 | Tell, don't ask | 2 | medium | low | `registry_snapshot()` manually extracts 20+ fields from resolved location rather than delegating serialization |
| 16 | Functional core, imperative shell | 2 | medium | low | Policy resolution in `policies.py` is pure; but `registration_core.py` mixes pure option resolution with imperative registration |
| 17 | Idempotency | 3 | - | - | Registration checks `catalog.has(name)` before re-registration; UDF install checks `_registered_snapshot` |
| 18 | Determinism | 3 | - | - | `cache_key()`, `fingerprint()`, `schema_identity_hash()` provide deterministic identity; snapshot hashes are canonical |
| 19 | KISS | 2 | small | low | `_resolve_cached_location` just calls `resolve_dataset_location` with no caching despite its name |
| 20 | YAGNI | 2 | small | low | `create_default_catalog` and `create_strict_catalog` produce identical results (both return `UdfCatalog(udf_specs=...)`) |
| 21 | Least astonishment | 2 | small | low | `_resolve_cached_location` name implies caching but provides none; `DataFusionCacheSettings` vs `DataFusionCachePolicy` naming is confusing |
| 22 | Public contracts | 2 | small | low | Explicit `__all__` on every module; `DataFusionUdfSpecSnapshot` provides versioned serialization; but snapshot version is hardcoded constant |
| 23 | Testability | 2 | medium | medium | `_TABLE_PROVIDER_METADATA_BY_CONTEXT` global forces tests to manage shared state; `_datafusion_internal()` does direct `importlib.import_module` without injection |
| 24 | Observability | 3 | - | - | Structured artifact recording via `record_artifact`; `CacheStateSnapshot.to_row()` provides consistent diagnostics payloads |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The `registration_core.py` module underwent decomposition into several helper modules (`registration_delta_helpers.py`, `registration_provider.py`, `registration_projection.py`, etc.) but retains a block of ~30 public aliases at `registration_core.py:634-665` that re-export private symbols.

**Findings:**
- `registration_core.py:634-665`: Public aliases like `DeltaProviderRegistration = _DeltaProviderRegistration`, `table_provider_capsule = _table_provider_capsule` create a compatibility facade that exposes implementation structure. Callers depend on the facade rather than importing from the actual modules, hiding the real module boundary.
- `extension_core.py:538-563`: Top-level imports from `extension_ddl` and `extension_snapshot_runtime` at module scope (not inside functions) tightly couple the module to its extracted parts.

**Suggested improvement:**
Migrate external callers to import directly from the actual helper modules (`registration_delta_helpers`, `registration_provider`) and deprecate the re-export block in `registration_core.py`. This makes the decomposition visible to callers and reduces the surface area of the facade module.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
`registration_core.py` (706 LOC, annotated with `size-exception` at line 29) mixes provider resolution, cache policy computation, DDL type mapping, runtime profile preparation, object store registration, and information schema invalidation.

**Findings:**
- `registration_core.py:29`: Self-documents as "temporarily >800 LOC during hard-cutover decomposition" but still bundles at least 4 distinct concerns (DDL, caching, registration, profile preparation).
- `registration_core.py:369-436` (`register_dataset_df`): Contains conditional delegation to `registry_facade_for_context`, `deregister_table`, `register_table`, and listing refresh artifact recording all within a single function.

**Suggested improvement:**
Complete the tracked decomposition. The DDL type mapping (`_sql_type_name`, `_DDL_TYPE_ALIASES`, `DDL_IDENTIFIER_RE`) belongs in a shared `ddl_utils` module. Cache policy resolution (`_resolve_cache_policy`, `DataFusionCachePolicy`, `DataFusionCacheSettings`) is already partially extracted to `registration_cache.py` and should move entirely.

**Effort:** medium
**Risk if unaddressed:** medium -- ongoing size makes the module a contributor-friction point.

---

#### P3. Single Responsibility -- Alignment: 2/3

**Current state:**
`extension_core.py` (695 LOC) serves as the primary UDF extension hub, handling at least four distinct responsibilities: native module loading, runtime installation (both modular and unified paths), registry snapshot normalization, and DDL alias registration.

**Findings:**
- `extension_core.py:142-176`: `_invoke_runtime_entrypoint` and `_module_supports_runtime_install` handle extension module capability probing.
- `extension_core.py:206-275`: `_install_runtime_via_modular_entrypoints` orchestrates multi-step install (UDF config, UDF registration, snapshot capture, function factory, expr planners).
- `extension_core.py:370-431`: `_normalize_registry_snapshot` is a 60-line function that normalizes, defaults, and enriches snapshot data.
- `extension_core.py:598-633`: `register_udfs_via_ddl` handles DDL-based catalog registration.

**Suggested improvement:**
Extract snapshot normalization into a dedicated `extension_snapshot_normalize.py` module. The module loading and capability probing (`_datafusion_internal`, `_extension_module_with_capabilities`, `_module_supports_runtime_install`) could form a standalone `extension_loader.py` concern. The existing `extension_loader.py` file at `udf/extension_loader.py` appears to exist already and could absorb this.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Cache containers (`DatasetCaches`, `ExtensionRegistries`, `IntrospectionCaches`) are injectable dataclasses using `WeakKeyDictionary` -- a good pattern. However, `ExtensionRegistries` at `extension_core.py:38-56` bundles 7 separate `WeakKeyDictionary` fields, some with semantically opaque types like `tuple[bool, int | None, int | None]`.

**Findings:**
- `extension_core.py:51-52`: `udf_policies: WeakKeyDictionary[SessionContext, tuple[bool, int | None, int | None]]` -- the tuple type carries no semantic meaning. Accessed at `extension_core.py:360` as `policy[0]`, `policy[1]`, `policy[2]`.
- `registration_core.py:232-244`: `DatasetCaches` has 3 `WeakKeyDictionary` fields for tracking cached datasets, registered catalogs, and registered schemas per context.

**Suggested improvement:**
Replace the `tuple[bool, int | None, int | None]` in `udf_policies` with a named dataclass or `NamedTuple` (e.g., `AsyncUdfPolicy(enabled: bool, timeout_ms: int | None, batch_size: int | None)`). This eliminates positional indexing and makes the semantics explicit.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The engine ring (`datafusion_engine/`) correctly does not import from outer-ring modules (`cli`, `graph`). However, `semantic_catalog.py` imports directly from the middle ring (`semantics.*`), creating a coupling where the engine layer depends on semantics layer internals.

**Findings:**
- `semantic_catalog.py:17-19`: Top-level imports `from semantics.catalog.dataset_rows import get_all_dataset_rows`, `from semantics.catalog.spec_builder import build_dataset_spec`, `from semantics.naming import canonical_output_name`.
- `semantic_catalog.py:167`: Deferred import `from semantics.catalog.dataset_rows import dataset_row`.
- `resolution.py:40`: TYPE_CHECKING import `from semantics.program_manifest import ManifestDatasetResolver`.
- `registration_core.py:393`: Deferred import `from semantics.program_manifest import ManifestDatasetBindings`.

**Suggested improvement:**
The `semantic_catalog.py` module acts as a bridge between semantics and the DataFusion engine. Consider moving it to the semantics layer (where it logically belongs as it builds semantic dataset catalogs from semantic rows) or introduce a protocol/interface in the engine layer that the semantics layer implements. The TYPE_CHECKING imports in `resolution.py` are acceptable since they don't create runtime dependencies.

**Effort:** medium
**Risk if unaddressed:** medium -- as the semantics layer evolves, changes ripple into the engine layer.

---

#### P6. Ports & Adapters -- Alignment: 3/3

**Current state:**
The Ports & Adapters pattern is well-applied. `DataFusionIOAdapter` provides a clean port for DataFusion operations. The Rust extension module is loaded via capability-gated adapter pattern with fallback paths. `RegistrySchemaProvider` and `RegistryCatalogProvider` implement DataFusion's `SchemaProvider` and `CatalogProvider` interfaces as adapters over the internal `DatasetCatalog`.

**Findings:**
- `provider.py:156-296`: `RegistrySchemaProvider` properly adapts `DatasetCatalog` to DataFusion's `SchemaProvider` interface.
- `provider.py:300-378`: `RegistryCatalogProvider` wraps schema providers in DataFusion's `CatalogProvider`.
- `extension_core.py:17-26`: Rust extension loaded via `invoke_entrypoint_with_adapted_context` which handles SessionContext ABI adaptation.

No action needed.

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
The DDL type-alias mapping -- the knowledge of how Arrow types map to SQL DDL type names -- is duplicated in two separate locations with subtly different content.

**Findings:**
- `registration_core.py:130-144`: `_DDL_TYPE_ALIASES` maps Arrow type names like `"Int8"` to DDL names like `"TINYINT"` using PascalCase keys (e.g., `"Int8"`, `"Utf8"`).
- `extension_core.py:64-82`: `_DDL_TYPE_ALIASES` maps similar Arrow types but uses lowercase keys (e.g., `"int8"`, `"utf8"`) and includes additional entries (`"large_string"`, `"string"`, `"null"`, `"bool"`).
- These represent the same business truth (Arrow-to-SQL type mapping) but with different key casing conventions and different coverage. Any change to type mapping rules requires updating both.

**Suggested improvement:**
Consolidate into a single canonical `ddl_type_aliases()` function in a shared module (e.g., `datafusion_engine/schema/ddl_types.py`) that normalizes input keys to a canonical case. Both `registration_core.py` and `extension_core.py` should call this single source of truth.

**Effort:** small
**Risk if unaddressed:** high -- drift between the two mappings can cause silent DDL generation inconsistencies.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Many types have explicit frozen dataclass contracts with typed fields. `DatasetPolicyRequest`, `ResolvedDatasetPolicies`, and `DataFusionRegistryOptions` use explicit fields. However, UDF registry snapshots flow as `Mapping[str, object]` throughout the entire UDF subsystem.

**Findings:**
- `extension_core.py:62`: `RustUdfSnapshot = Mapping[str, object]` -- the snapshot type alias provides no structural contract.
- `extension_core.py:370-431`: `_normalize_registry_snapshot` accepts `object`, casts to `Mapping`, then uses `setdefault` extensively to fill in missing keys, building the contract at runtime rather than at construction time.
- `metadata.py:1056-1102`: `datafusion_udf_specs()` accepts `registry_snapshot: Mapping[str, object]` and manually extracts typed data from it.

**Suggested improvement:**
Define a `RegistrySnapshotV1` msgspec Struct that declares all expected fields with their types and defaults. Parse the raw mapping into this struct at the boundary (`_normalize_registry_snapshot`) and thread the typed struct downstream instead of the raw mapping.

**Effort:** medium
**Risk if unaddressed:** low -- the current runtime validation catches most issues.

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`_normalize_registry_snapshot` at `extension_core.py:370-431` is a 60-line function that validates and mutates a raw snapshot mapping in-place using `setdefault`, `pop`, and conditional key insertions. This "validate-as-you-go" pattern scatters boundary enforcement across the function body.

**Findings:**
- `extension_core.py:376-380`: Version validation done inline (`if not isinstance(raw_version, int)`, `if raw_version != _REGISTRY_SNAPSHOT_VERSION`).
- `extension_core.py:391-408`: 18 `setdefault` calls to fill in missing keys with empty defaults.
- `factory.py:367-427`: `_policy_from_payload` performs similar "unpack and default" logic for function factory payloads.

**Suggested improvement:**
Replace `_normalize_registry_snapshot` with a `msgspec.convert(snapshot, RegistrySnapshotV1)` call that parses the raw mapping into a typed struct in one step. Invalid or missing fields would produce clear errors at construction rather than requiring scattered `setdefault` and `isinstance` checks.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`DatasetLocation` at `registry.py:75-153` allows `delta_version` and `delta_timestamp` to be set simultaneously, which is semantically invalid (you can time-travel by version OR timestamp, not both).

**Findings:**
- `registry.py:89-90`: `delta_version: int | None = None` and `delta_timestamp: str | None = None` are independent optional fields.
- `provider.py:94-96`: Runtime check `if location.delta_version is not None and location.delta_timestamp is not None: raise ValueError(...)`.
- `spec.py:75-104`: `with_delta_version` manually sets `delta_timestamp=None` when pinning a version, encoding the mutual-exclusion invariant in application code rather than the type.

**Suggested improvement:**
Replace the two optional fields with a discriminated union: `delta_time_travel: DeltaVersionPin | DeltaTimestampPin | None = None`. This makes the mutual exclusion unrepresentable by construction and eliminates the runtime check.

**Effort:** medium
**Risk if unaddressed:** low -- the runtime check catches it, but callers must remember the invariant.

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. Pure queries (`resolve_dataset_policies`, `dataset_schema`, `introspection_cache_for_ctx`) are separated from commands (`register_rust_udfs`, `invalidate_introspection_cache`). However, several functions both mutate state and return values.

**Findings:**
- `extension_core.py:434-451`: `_install_rust_udfs` mutates `registries.udf_snapshots[ctx]`, calls `registries.udf_validated.add(ctx)`, AND calls `_notify_udf_snapshot`. It is void-returning, which is acceptable, but the internal `_install_codeanatomy_runtime_snapshot` at `extension_core.py:278-352` both mutates `registries.runtime_payloads[ctx]` and returns the normalized snapshot.
- `provider_registry.py:95-142`: `ProviderRegistry.register_spec` both registers metadata AND returns `RegistrationMetadata`, mixing command and query.
- `tables/metadata.py:120-138`: `record_table_provider_metadata` mutates the global `_TABLE_PROVIDER_METADATA_BY_CONTEXT`.

**Suggested improvement:**
For `ProviderRegistry.register_spec`, separate the metadata creation (query) from the registration (command). Have callers create the metadata first, then register it. For `_install_codeanatomy_runtime_snapshot`, return the snapshot but move the `registries.runtime_payloads[ctx] = payload_mapping` mutation to the caller.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
The injectable cache containers (`DatasetCaches`, `ExtensionRegistries`, `IntrospectionCaches`) are a good pattern for DI. However, `tables/metadata.py` uses a module-level global `_TABLE_PROVIDER_METADATA_BY_CONTEXT` that is not injectable.

**Findings:**
- `tables/metadata.py:114-117`: `_TABLE_PROVIDER_METADATA_BY_CONTEXT: WeakKeyDictionary[SessionContext, dict[str, TableProviderMetadata]] = WeakKeyDictionary()` -- module-level mutable state shared across all callers. Functions `record_table_provider_metadata`, `table_provider_metadata`, `all_table_provider_metadata`, `clear_table_provider_metadata` all operate on this global.
- `extension_core.py:58-59`: `_resolve_registries` creates a fresh `ExtensionRegistries()` when None is passed, meaning callers that forget to pass registries get isolated state rather than shared state. This is safe but can cause surprising duplication of UDF installation work.

**Suggested improvement:**
Introduce a `TableProviderMetadataRegistry` injectable dataclass mirroring the pattern established by `DatasetCaches`, `ExtensionRegistries`, and `IntrospectionCaches`. Thread it via the registration context rather than using module-level state.

**Effort:** medium
**Risk if unaddressed:** medium -- tests must use `clear_table_provider_metadata` for cleanup, creating implicit coupling.

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
Composition is used consistently throughout. `ProviderRegistry` delegates to `MutableRegistry`. `UdfCatalogAdapter` wraps `UdfCatalog`. `RegistryCatalogProvider` composes `RegistrySchemaProvider`. There is no deep inheritance hierarchy.

**Findings:**
- `provider.py:155-296`: `RegistrySchemaProvider` extends `SchemaProvider` (a DataFusion framework class) -- this is appropriate adapter inheritance, not design hierarchy.
- `provider_registry.py:72-73`: `ProviderRegistry` extends `Registry` and `SnapshotRegistry` protocols rather than concrete classes.
- `udf/metadata.py:751-882`: `UdfCatalogAdapter` wraps `UdfCatalog` via a `catalog` field, implementing `Registry` protocol through delegation.

No action needed.

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
The `registry_snapshot()` function at `registry.py:235-361` contains extensive chains of attribute access spanning 3-4 levels deep on resolved policy objects.

**Findings:**
- `registry.py:293-312`: `resolved.delta_write_policy.parquet_writer_policy.statistics_enabled` -- 4-level chain. This pattern repeats for `bloom_filter_enabled`, `bloom_filter_fpp`, `bloom_filter_ndv`, `dictionary_enabled`, `statistics_level`.
- `registry.py:256-282`: `resolved.datafusion_scan.partition_cols_pyarrow()`, `resolved.datafusion_scan.file_sort_order`, `resolved.datafusion_scan.parquet_pruning`, etc. -- 14 separate attribute accesses through `resolved.datafusion_scan`.
- `registration_core.py:478`: `location.resolved.table_spec` -- double indirection through a property that triggers lazy resolution.

**Suggested improvement:**
Add `to_snapshot_payload()` methods to `ResolvedDatasetLocation`, `DataFusionScanOptions`, `DeltaWritePolicy`, and `DeltaScanOptions` that produce their own dict representations. `registry_snapshot()` would then call `resolved.to_snapshot_payload()` instead of manually extracting every field. This moves serialization knowledge to the owning type.

**Effort:** medium
**Risk if unaddressed:** medium -- any field addition to these policy types requires updating the snapshot function, which violates Open-Closed.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Several functions extract raw data from objects and perform logic externally rather than delegating to the object. The `registry_snapshot()` function is the most significant example.

**Findings:**
- `registry.py:235-361`: `registry_snapshot()` manually extracts and restructures every field from `ResolvedDatasetLocation`, `DataFusionScanOptions`, `DeltaWritePolicy`, and `DeltaScanOptions`. The object does not participate in its own serialization.
- `metadata.py:1056-1102`: `datafusion_udf_specs()` manually extracts kinds, parameter names, volatilities, rewrite tags, and documentation from a raw snapshot mapping rather than asking the snapshot to produce typed specs.

**Suggested improvement:**
Same as P14 suggestion. Additionally, for the UDF snapshot, converting to a typed `RegistrySnapshotV1` struct early would allow `datafusion_udf_specs()` to operate on typed accessors rather than manual dict traversal.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Policy resolution functions in `policies.py` are pure and deterministic -- good functional core. However, `registration_core.py` mixes pure option computation with imperative operations (object store registration, cache invalidation, metadata recording).

**Findings:**
- `policies.py:53-89`: `resolve_dataset_policies()` is pure -- takes a request, returns resolved policies. No side effects.
- `registration_core.py:439-500`: `_build_registration_context()` mixes pure option resolution (`_resolve_registry_options`) with imperative actions (`_register_object_store_for_location`, `_prepare_runtime_profile`, `record_table_provider_metadata`).

**Suggested improvement:**
Split `_build_registration_context` into a pure function that computes the `DataFusionRegistrationContext` from inputs, and a separate imperative function that performs the side effects (object store registration, metadata recording). This would make the context construction testable without mocking DataFusion.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Registration operations are idempotent. `_register_location` checks `catalog.has(name)` before registration. `register_rust_udfs` checks `_registered_snapshot` for an existing snapshot. Cache invalidation is inherently idempotent.

No action needed.

---

#### P18. Determinism / Reproducibility -- Alignment: 3/3

**Current state:**
Deterministic identity is well-supported. `TableSpec.cache_key()` produces stable hashes. `schema_identity_hash()` provides fingerprints. `param_signature_from_array()` generates canonical signatures. `FunctionFactoryPolicy.fingerprint()` uses `config_fingerprint`.

No action needed.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The `_resolve_cached_location` function at `registry.py:720-721` implies caching in its name but simply delegates to `resolve_dataset_location` with no caching behavior.

**Findings:**
- `registry.py:720-721`: `def _resolve_cached_location(location): return resolve_dataset_location(location)` -- a trivial wrapper that adds indirection without adding functionality.
- `registry.py:98-146`: The `DatasetLocation.resolved` property calls `_resolve_cached_location(self)`, which calls `resolve_dataset_location`. Multiple delta-policy properties (`delta_scan`, `delta_cdf_policy`, `delta_write_policy`, etc.) each call `_resolve_cached_location`, causing redundant resolution on repeated access.

**Suggested improvement:**
Either implement actual caching (e.g., `@functools.lru_cache` or a `_resolved` slot on the frozen struct) or rename `_resolve_cached_location` to `_resolve_location` to avoid misleading the reader.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
`create_default_catalog` and `create_strict_catalog` in `metadata.py` produce identical results despite different names.

**Findings:**
- `metadata.py:884-899`: `create_default_catalog` returns `UdfCatalog(udf_specs=udf_specs)`.
- `metadata.py:902-917`: `create_strict_catalog` returns `UdfCatalog(udf_specs=udf_specs)` -- same implementation.
- The `UdfTier` type is `Literal["builtin"]` (only one value), making the "tier" abstraction unnecessary at present.

**Suggested improvement:**
Collapse `create_default_catalog` and `create_strict_catalog` into a single `create_udf_catalog` until a genuine second tier is needed. The "strict" vs "default" distinction can be reintroduced when a second `UdfTier` value is added.

**Effort:** small
**Risk if unaddressed:** low -- but the dead abstraction misleads readers into thinking tier selection matters.

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Naming is mostly clear but a few names are misleading.

**Findings:**
- `registry.py:720`: `_resolve_cached_location` does not cache, violating reader expectations.
- `registration_core.py:178-208`: `DataFusionCachePolicy` (input policy) vs `_DataFusionCacheSettings` (resolved settings) -- the difference between "policy" and "settings" is subtle and easily confused given the project's config naming conventions (`Policy` = runtime behavior control, `Settings` = initialization parameters).
- `registry.py:764-778`: Module-level `__getattr__` lazily imports types from `schema_spec.system`, which is a surprising discovery mechanism for someone reading the module.

**Suggested improvement:**
Rename `_resolve_cached_location` to `_resolve_location`. Consider renaming `DataFusionCachePolicy` to `DataFusionCacheOptions` (aligning with the project convention where `Options` = optional parameter bundles) to distinguish it from the resolved `_DataFusionCacheSettings`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
Every module has an explicit `__all__` list, which is good. `DataFusionUdfSpecSnapshot` uses `StructBaseCompat` for versioned serialization. However, the UDF registry snapshot version is a hardcoded constant (`_REGISTRY_SNAPSHOT_VERSION = 1`) without a formal evolution mechanism.

**Findings:**
- `extension_core.py:99`: `_REGISTRY_SNAPSHOT_VERSION = 1` -- version checked but no migration path defined for version 2.
- `registration_core.py:668-706`: Explicit `__all__` with 35 entries -- comprehensive but very large, suggesting the module surface is broader than ideal.

**Suggested improvement:**
No immediate action needed. When version 2 becomes necessary, define a `RegistrySnapshotMigrator` protocol. For now, the version check is sufficient.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
The injectable cache containers are test-friendly. However, the module-level global in `tables/metadata.py` and the direct `importlib.import_module` calls for Rust extension loading make isolated testing difficult.

**Findings:**
- `tables/metadata.py:114-117`: `_TABLE_PROVIDER_METADATA_BY_CONTEXT` -- module-level mutable global. Tests must call `clear_table_provider_metadata(ctx)` for cleanup, which is a hidden coupling.
- `extension_core.py:454-469`: `_datafusion_internal()` uses `importlib.import_module(EXTENSION_MODULE_PATH)` directly. Tests that don't have the Rust extension installed must mock at the module level.
- `factory.py:217-234`: `_load_extension()` similarly does direct `importlib.import_module`.

**Suggested improvement:**
For `_TABLE_PROVIDER_METADATA_BY_CONTEXT`, convert to an injectable `TableProviderMetadataRegistry` dataclass as suggested in P12. For extension loading, consider an `ExtensionModuleLoader` protocol that can be injected, making tests able to provide fake extension modules without `monkeypatch`.

**Effort:** medium
**Risk if unaddressed:** medium -- test fragility increases as the Rust extension surface grows.

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
Observability is thorough. Structured artifact payloads are recorded via `record_artifact`. `CacheStateSnapshot.to_row()` provides consistent diagnostics. `udf_audit_payload` captures volatility and fast-path coverage. `capture_cache_diagnostics` provides unified cache state.

**Findings:**
- `catalog/introspection.py:642-668`: `capture_cache_diagnostics` produces a comprehensive diagnostics payload with config and cache snapshots.
- `extension_core.py:635-666`: `udf_audit_payload` provides structured volatility analysis and fast-path audit.
- `resolution.py:345-358`: `_record_override_artifact` records scan override events with structured payloads including hash-based identity.

No action needed.

---

## Cross-Cutting Themes

### Theme 1: Untyped Snapshot Pipelines

**Description:** The UDF registry snapshot flows as `Mapping[str, object]` through the entire UDF subsystem -- from Rust extension capture, through normalization, to policy derivation, DDL generation, and UDF spec construction. This untyped pipeline means every consumer must independently validate and extract fields, duplicating boundary-parsing logic across `_normalize_registry_snapshot`, `_policy_from_payload`, `datafusion_udf_specs`, `snapshot_function_names`, `udf_audit_payload`, and `udf_planner_snapshot`.

**Root cause:** The snapshot originates from a Rust FFI boundary where the Python type is inherently `object`. No parse-once-at-boundary pattern was applied.

**Affected principles:** P8 (Design by contract), P9 (Parse, don't validate), P14 (Law of Demeter), P15 (Tell, don't ask).

**Suggested approach:** Define a typed `RegistrySnapshotV1` struct. Parse at the `_normalize_registry_snapshot` boundary. Thread the struct downstream. This addresses P8, P9, P14, and P15 simultaneously.

### Theme 2: Module-Level Mutable State

**Description:** Three modules in scope use module-level `WeakKeyDictionary` globals: `tables/metadata.py` (`_TABLE_PROVIDER_METADATA_BY_CONTEXT`), and implicitly the default-constructed `DatasetCaches` and `ExtensionRegistries` when callers pass `None`. While the `WeakKeyDictionary` avoids memory leaks, the module-level state creates hidden coupling between test cases and complicates reasoning about state isolation.

**Root cause:** The metadata tracking was introduced before the injectable cache container pattern was established.

**Affected principles:** P12 (DI), P23 (Testability), P11 (CQS).

**Suggested approach:** Convert `_TABLE_PROVIDER_METADATA_BY_CONTEXT` to an injectable `TableProviderMetadataRegistry` dataclass following the established pattern from `DatasetCaches`, `ExtensionRegistries`, and `IntrospectionCaches`.

### Theme 3: Decomposition In Progress

**Description:** `registration_core.py` has been partially decomposed into helper modules but retains a large re-export surface (30+ aliases). `extension_core.py` similarly bundles multiple responsibilities. Both files self-document their size exceptions. The decomposition direction is correct but incomplete.

**Root cause:** Tracked in `docs/plans/src_design_improvements_implementation_plan_v1_2026-02-16.md`.

**Affected principles:** P1 (Information hiding), P2 (Separation of concerns), P3 (SRP).

**Suggested approach:** Complete the planned decomposition. Migrate external callers to import from the actual helper modules. Remove the re-export block once all callers are migrated.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Consolidate `_DDL_TYPE_ALIASES` from `registration_core.py:130` and `extension_core.py:64` into a shared `ddl_types.py` | small | Eliminates highest-risk knowledge duplication |
| 2 | P4 (Cohesion) | Replace `tuple[bool, int \| None, int \| None]` in `ExtensionRegistries.udf_policies` with a named `AsyncUdfPolicy` dataclass | small | Eliminates positional indexing and improves readability |
| 3 | P19/P21 (KISS/Astonishment) | Rename `_resolve_cached_location` to `_resolve_location` or implement actual caching | small | Removes misleading name |
| 4 | P20 (YAGNI) | Collapse `create_default_catalog`/`create_strict_catalog` into `create_udf_catalog` | small | Removes dead abstraction |
| 5 | P10 (Illegal states) | Add `__post_init__` check to `DatasetLocation` preventing simultaneous `delta_version` and `delta_timestamp` | small | Fails at construction rather than at provider resolution |

## Recommended Action Sequence

1. **Consolidate DDL type aliases** (P7): Create `src/datafusion_engine/schema/ddl_types.py` with a single canonical `ddl_type_name()` function. Update `registration_core.py` and `extension_core.py` to use it.

2. **Name the async UDF policy tuple** (P4): Create `AsyncUdfPolicy` dataclass in `extension_core.py`. Replace `tuple[bool, int | None, int | None]` throughout `ExtensionRegistries` and its consumers.

3. **Fix misleading `_resolve_cached_location` name** (P19, P21): Rename to `_resolve_location` or add actual caching via a `_resolved` slot.

4. **Eliminate dead catalog factory distinction** (P20): Collapse `create_default_catalog` and `create_strict_catalog` into `create_udf_catalog` until a genuine second tier exists.

5. **Make delta time-travel mutually exclusive** (P10): Add `__post_init__` validation to `DatasetLocation` or introduce a discriminated union type.

6. **Convert `_TABLE_PROVIDER_METADATA_BY_CONTEXT` to injectable registry** (P12, P23): Follow the `DatasetCaches` pattern. Thread through `DataFusionRegistrationContext`.

7. **Type the UDF registry snapshot** (P8, P9, P14, P15): Define `RegistrySnapshotV1` struct. Parse at `_normalize_registry_snapshot`. Thread downstream.

8. **Complete `registration_core.py` decomposition** (P1, P2, P3): Extract DDL and cache policy concerns. Migrate callers from re-export aliases to direct imports.
