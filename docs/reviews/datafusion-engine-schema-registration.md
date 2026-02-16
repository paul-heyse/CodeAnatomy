# Design Review: datafusion_engine schema/catalog/views/tables

**Date:** 2026-02-16
**Scope:** `src/datafusion_engine/schema/`, `src/datafusion_engine/catalog/`, `src/datafusion_engine/views/`, `src/datafusion_engine/tables/`
**Focus:** DRY across schema/catalog/views overlap, Ports & Adapters for catalog abstraction, dependency direction between registration and consumption. All 24 principles scored.
**Depth:** moderate
**Files reviewed:** 33 (all Python files in scope; 17 deep-read, 16 header-read)
**Total LOC:** ~13,948

## Executive Summary

The schema-registration subsystem has strong structural foundations: well-defined contract types (`SchemaContract`, `TableConstraints`, `ViewNode`, `TableSpec`), a clear introspection cache layer, and deterministic fingerprinting throughout. However, three systemic issues reduce alignment: (1) duplicated introspection and schema-resolution knowledge split across `schema/introspection.py`, `catalog/introspection.py`, and `views/graph.py`; (2) the `SchemaIntrospector` class at 1305 LOC acts as a God class that mixes query execution, caching, and schema reflection; (3) the `tables/metadata.py` module uses manual copy-on-write `with_*` methods instead of `dataclasses.replace()`, creating brittle boilerplate. The top improvements are consolidating the `_schema_from_table` / `schema_from_table` duplication and extracting the introspection cache into a first-class port.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `SchemaIntrospector` exposes `ctx`, `cache`, `snapshot` as public attrs |
| 2 | Separation of concerns | 1 | large | medium | `schema/introspection.py` mixes SQL execution, caching, constraint parsing, fingerprinting |
| 3 | SRP | 1 | medium | medium | `views/graph.py` (1425 LOC) handles topo-sort, caching, validation, diagnostics, plan artifacts |
| 4 | High cohesion, low coupling | 2 | medium | medium | `SchemaIntrospector` instantiated in 25+ callsites across 10+ modules |
| 5 | Dependency direction | 2 | medium | low | `schema/introspection.py` imports from `catalog/introspection.py` and `tables/metadata.py` (lateral) |
| 6 | Ports & Adapters | 1 | large | medium | No port abstraction for introspection; all callsites directly construct `SchemaIntrospector` |
| 7 | DRY | 1 | small | high | `_schema_from_table` duplicated in `views/graph.py:1352` and `schema/introspection.py:695` |
| 8 | Design by contract | 2 | small | low | `SchemaContract` has explicit validation; but `SchemaIntrospector` postconditions are implicit |
| 9 | Parse, don't validate | 2 | small | low | `IntrospectionSnapshot.capture()` parses at boundary; but many dict-based intermediaries |
| 10 | Make illegal states unrepresentable | 2 | medium | low | `TableProviderMetadata` allows impossible combos (ddl + no table_name) |
| 11 | CQS | 2 | small | low | `SchemaIntrospector.__post_init__` mutates state while constructing (query) |
| 12 | Dependency inversion | 1 | medium | medium | `SchemaIntrospector` is concrete; no protocol; callers tightly coupled |
| 13 | Composition over inheritance | 3 | - | low | No deep inheritance hierarchies |
| 14 | Law of Demeter | 2 | small | low | `cache.runtime.runtime_profile` chains in `views/graph.py` |
| 15 | Tell, don't ask | 2 | small | low | `_register_view_node` queries multiple context fields rather than delegating |
| 16 | Functional core, imperative shell | 2 | medium | low | Schema validation is pure; but `finalize.py` mixes registration with transforms |
| 17 | Idempotency | 3 | - | low | Registration uses overwrite flags; deterministic fingerprints |
| 18 | Determinism | 3 | - | low | Strong fingerprinting; sorted outputs; canonical hashing |
| 19 | KISS | 1 | medium | medium | `__init__.py` has 68-symbol `__all__` + lazy-load dispatch for entire schema package |
| 20 | YAGNI | 2 | small | low | `extraction_schemas_core.py` and `extraction_schemas_extended.py` are thin wrappers |
| 21 | Least astonishment | 2 | small | low | `_normalized_aliases` and `_normalized_parameter_names` are identical functions |
| 22 | Public contracts | 2 | small | low | `__all__` declared consistently; but internal helpers like `_field_from_container` exported |
| 23 | Testability | 2 | medium | medium | `SchemaIntrospector` requires a live `SessionContext`; no protocol for faking |
| 24 | Observability | 3 | - | low | Structured diagnostics via `record_artifact` throughout |

## Detailed Findings

### Category: Boundaries (P1-P6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
`SchemaIntrospector` at `schema/introspection.py:740` is a frozen dataclass that exposes `ctx`, `cache`, `cache_prefix`, `cache_ttl`, `introspection_cache`, and `snapshot` as public attributes. Callers across the codebase directly access `introspector.snapshot` (e.g., `views/graph.py:1244`, `schema/observability_schemas.py:531`) to reach into the underlying `IntrospectionSnapshot`.

**Findings:**
- `schema/introspection.py:740-749`: `SchemaIntrospector` fields `ctx`, `cache`, `cache_prefix`, `cache_ttl`, `introspection_cache`, `snapshot` are all public, exposing implementation details.
- `views/graph.py:1244`: `introspector = SchemaIntrospector(ctx); snapshot = introspector.snapshot` -- direct snapshot access bypasses any introspector abstraction.
- `catalog/introspection.py:27`: `IntrospectionSnapshot` is a plain mutable dataclass with 8 public `pa.Table` fields, exposing raw Arrow tables directly.

**Suggested improvement:**
Make `SchemaIntrospector` fields prefixed with underscore where they are implementation details (`_cache`, `_cache_prefix`, `_cache_ttl`, `_introspection_cache`). Expose `snapshot` through a method or property with a clear contract about freshness.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`schema/introspection.py` (1305 LOC) conflates five distinct responsibilities: (a) SQL query execution against information_schema, (b) schema fingerprinting/hashing, (c) constraint row parsing, (d) routine/parameter merging from registry snapshots, and (e) schema-to-contract conversion. These concerns are interleaved within a single module.

**Findings:**
- `schema/introspection.py:48-79`: Fingerprinting functions (`schema_map_fingerprint`, `schema_map_fingerprint_from_mapping`) are schema-identity concerns, not introspection.
- `schema/introspection.py:158-307`: Constraint row construction (`_constraint_rows_from_snapshot`, `_constraint_rows_from_metadata`, `_constraint_name`) is constraint parsing, distinct from catalog reflection.
- `schema/introspection.py:82-97`: `schema_contract_from_table()` is a contract builder that sits in the introspection module.
- `schema/introspection.py:558-631`: `_merge_registry_routines`, `_merge_registry_parameters` are empty stubs that remain as dead code after refactoring.

**Suggested improvement:**
Extract three focused modules: (a) `schema/fingerprint.py` for `schema_map_fingerprint*` and identity hash functions, (b) `schema/constraint_parsing.py` for constraint row construction and parsing, (c) keep `schema/introspection.py` focused on `SchemaIntrospector` and its catalog reflection methods. Remove the dead `_merge_registry_*` stubs.

**Effort:** large
**Risk if unaddressed:** medium -- Changes to constraint parsing logic currently force edits in a 1305-LOC file that also owns SQL execution.

---

#### P3. SRP -- Alignment: 1/3

**Current state:**
`views/graph.py` (1425 LOC) is the most SRP-violating file: it handles topological sorting (`_topo_sort_nodes`, `_topo_sort_nodes_kahn`), delta staging cache write logic (`_register_delta_staging_cache`, 185 LOC), delta output cache logic (`_register_delta_output_cache`, 187 LOC), schema contract validation (`_validate_schema_contract`), plan artifact persistence (`_persist_plan_artifacts`), UDF parity/audit recording, and view registration orchestration. It changes for at least 6 independent reasons: cache policy changes, validation rule changes, topo-sort algorithm changes, artifact format changes, UDF audit changes, and view graph shape changes.

**Findings:**
- `views/graph.py:663-847`: `_register_delta_staging_cache` is 185 lines of Delta-specific cache orchestration with inline import statements.
- `views/graph.py:850-1036`: `_register_delta_output_cache` is 187 lines that largely duplicate the staging cache pattern (resolve hit, enforce schema, write, register, record inventory).
- `views/graph.py:1369-1416`: Kahn's algorithm implementation is a pure algorithm with no view-specific logic, embedded in a registration module.

**Suggested improvement:**
Extract `_register_delta_staging_cache` and `_register_delta_output_cache` into a dedicated `views/cache_registration.py` module. Extract `_topo_sort_nodes` into a utility (possibly `utils/graph.py`). This would reduce `views/graph.py` to ~600 LOC focused on registration orchestration.

**Effort:** medium
**Risk if unaddressed:** medium -- The delta staging and output cache paths are nearly identical (violates DRY) and any fix requires touching a 1425-LOC file.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
`SchemaIntrospector` is instantiated directly in 25+ callsites across 10+ modules (views, schema, session, plan, dataset, lineage). This creates high fan-in coupling to a concrete class. The `ProviderRegistry` in `catalog/provider_registry.py` is better: it centralizes registration through a single interface.

**Findings:**
- Grep results show `SchemaIntrospector(` appears in: `session/runtime_extensions.py:76`, `session/runtime_session.py:532`, `session/introspection.py:59`, `lineage/diagnostics.py:768`, `session/runtime.py:1035`, `schema/finalize.py:661`, `schema/catalog_contracts.py:40,67`, `schema/observability_schemas.py:531,596,641`, `plan/bundle_artifact.py:1640,2447,2490`, `dataset/registration.py:1083,2788,2835`, `views/graph.py:556,1243,1321`, `session/facade.py:900`.
- Each callsite constructs with slightly different `sql_options` and `cache` parameters, making the coupling pattern inconsistent.

**Suggested improvement:**
Introduce a factory function or session-scoped singleton pattern for `SchemaIntrospector` creation, so that callers request an introspector from the runtime profile or session rather than constructing one manually.

**Effort:** medium
**Risk if unaddressed:** medium -- Inconsistent construction parameters across callsites risk subtle behavioral differences.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Dependencies generally flow inward (schema contracts are core; providers depend on contracts). However, there are lateral dependencies: `schema/introspection.py` imports from `catalog/introspection.py` (line 44, TYPE_CHECKING) and `tables/metadata.py` (line 37, runtime import). `schema/catalog_contracts.py` imports from both `schema/contracts.py` and `schema/introspection.py`. These create a web of co-dependencies within the schema layer.

**Findings:**
- `schema/introspection.py:37`: `from datafusion_engine.tables.metadata import table_provider_metadata` -- introspection depends on table metadata (should be the reverse).
- `schema/introspection.py:44`: TYPE_CHECKING import of `IntrospectionCache, IntrospectionSnapshot` from `catalog/introspection.py`.
- `views/graph.py:20-26`: imports from `schema/contracts`, `schema/introspection`, `udf/extension_runtime`, `views/artifacts`, `views/bundle_extraction` -- 5 direct schema-layer dependencies.

**Suggested improvement:**
Invert the `schema/introspection.py` -> `tables/metadata.py` dependency: have `table_provider_metadata` exposed through a port or passed in as a parameter, rather than imported at the call site.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 1/3

**Current state:**
There is no port abstraction for the introspection subsystem. `SchemaIntrospector` is a concrete dataclass that directly queries DataFusion's `SessionContext`. There is no protocol or interface that would allow substituting a fake introspector for testing or an alternative backend. The `ProviderRegistry` has a similar gap: it directly depends on `SessionContext` and imports internal registration functions.

Additionally, `IntrospectionSnapshot` at `catalog/introspection.py:27` is consumed by both `schema/introspection.py` and `schema/contracts.py`, but there is no abstract port defining what "introspection" means -- it is a concrete dataclass with Arrow tables.

**Findings:**
- `schema/introspection.py:740-749`: `SchemaIntrospector` directly stores `ctx: SessionContext` with no indirection.
- `catalog/introspection.py:27-61`: `IntrospectionSnapshot` is a concrete dataclass, not a protocol.
- `catalog/provider_registry.py:71-93`: `ProviderRegistry` stores `ctx: SessionContext` directly.
- `views/graph.py:54-82`: `ViewNode` stores `builder: Callable[[SessionContext], DataFrame]` -- tightly coupled to DataFusion.

**Suggested improvement:**
Define a `SchemaIntrospectionPort` protocol (with methods like `table_exists()`, `get_table_columns()`, `table_schema()`, `function_names()`) and have `SchemaIntrospector` implement it. Callers depend on the protocol. This enables lightweight test fakes without needing a full DataFusion `SessionContext`.

**Effort:** large
**Risk if unaddressed:** medium -- Testing `SchemaContract.validate_against_introspection()` currently requires a full `IntrospectionSnapshot`, which in turn requires a live `SessionContext`.

---

### Category: Knowledge (P7-P11)

#### P7. DRY -- Alignment: 1/3

**Current state:**
Several pieces of knowledge are duplicated across the reviewed scope.

**Findings:**
- `views/graph.py:1352-1366`: `_schema_from_table()` is a 15-line function that duplicates `schema_from_table()` at `schema/introspection.py:695-710`. Both resolve a DataFusion table schema and handle the `to_arrow()` fallback pattern, but the graph version has a slightly different error path.
- `schema/introspection.py:458-483`: `_normalized_aliases()` and `_normalized_parameter_names()` are structurally identical functions (same body, same types, same logic) that both normalize a `Mapping[str, object]` into `Mapping[str, Sequence[str]]`. This is classic copy-paste duplication.
- `views/graph.py:663-847` and `views/graph.py:850-1036`: The delta staging and delta output cache registration functions share ~80% of their structure (resolve cache hit, enforce schema evolution, write pipeline, register, record inventory, record artifact). The only differences are path resolution and `allow_evolution`.
- `schema/extraction_schemas_core.py` and `schema/extraction_schemas_extended.py` are thin 25-line wrappers that just re-export subsets of `extraction_schemas.py` -- the grouping knowledge is duplicated between the wrappers and the main module.

**Suggested improvement:**
1. Delete `_schema_from_table` in `views/graph.py` and use `schema_from_table` from `schema/introspection.py`.
2. Unify `_normalized_aliases` and `_normalized_parameter_names` into a single `_normalize_name_mapping()` function.
3. Extract a shared `_register_delta_cache` function parameterized by path resolution strategy and evolution policy to DRY up the two 185-line cache registration functions.

**Effort:** small (items 1-2), medium (item 3)
**Risk if unaddressed:** high -- The `_schema_from_table` duplication can silently diverge, causing different error behavior in graph registration vs introspection.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
`SchemaContract` has explicit validation methods (`validate_against_introspection`, `validate_against_schema`) with documented violation types. `TableConstraints` provides normalized accessors. However, `SchemaIntrospector` methods return `list[dict[str, object]]` with no postcondition guarantees about key presence or value types.

**Findings:**
- `schema/introspection.py:824-851`: `table_columns()` returns `list[dict[str, object]]` with no guarantee that `column_name` or `data_type` keys exist.
- `schema/contracts.py:470-500`: `SchemaContract` dataclass has documented attributes and explicit `enforce_columns` flag, which is a well-designed contract mechanism.

**Suggested improvement:**
Replace `list[dict[str, object]]` return types in `SchemaIntrospector` with typed dataclass or `TypedDict` return values (e.g., `ColumnMetadataRow`).

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`IntrospectionSnapshot.capture()` at `catalog/introspection.py:62` correctly parses information_schema tables into structured Arrow tables at the boundary. However, downstream consumers like `SchemaIntrospector.table_columns()` re-parse these Arrow tables back into `list[dict[str, object]]` via `.to_pylist()`, losing the type safety.

**Findings:**
- `catalog/introspection.py:62`: `capture()` properly parses at the boundary into Arrow tables.
- `schema/introspection.py:836`: `snapshot.columns.to_pylist()` converts typed Arrow data back into untyped dicts.

**Suggested improvement:**
Expose typed accessors on `IntrospectionSnapshot` (e.g., `columns_for_table(name) -> Sequence[ColumnRow]`) rather than having consumers re-parse via `to_pylist()`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
`TableProviderMetadata` at `tables/metadata.py:48` allows combinations that should be mutually exclusive: `ddl` can be set alongside `storage_location` and `file_format` even though DDL-registered tables derive their location from the DDL. `ConstraintSpec` at `schema/contracts.py:141` allows both `column` and `columns` to be set simultaneously or both to be `None`.

**Findings:**
- `tables/metadata.py:48-111`: `TableProviderMetadata` has 16 optional fields with no validation of impossible combinations.
- `schema/contracts.py:141-159`: `ConstraintSpec` allows `column` and `columns` simultaneously.

**Suggested improvement:**
Add `__post_init__` validation to `ConstraintSpec` ensuring exactly one of `column`/`columns` is set for column-targeted constraints. For `TableProviderMetadata`, consider splitting into `DdlProviderMetadata` and `StorageProviderMetadata` variants, or add validation.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS well. The primary exception is `SchemaIntrospector.__post_init__()` at `schema/introspection.py:751-760`, which mutates `self.introspection_cache` and `self.snapshot` via `object.__setattr__` during construction, making it a side-effecting constructor that also queries the session for cached state.

**Findings:**
- `schema/introspection.py:751-760`: `__post_init__` uses `object.__setattr__` on a frozen dataclass to lazily initialize `introspection_cache` and `snapshot`, mixing construction with query.
- `catalog/provider_registry.py:95-142`: `register_spec()` returns `RegistrationMetadata` while also mutating internal registry state (command+query).

**Suggested improvement:**
Use a factory classmethod `SchemaIntrospector.from_context(ctx)` that computes the introspection cache and snapshot before constructing the frozen dataclass, separating construction from query.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (P12-P15)

#### P12. Dependency inversion -- Alignment: 1/3

**Current state:**
All 25+ callsites construct `SchemaIntrospector` directly, with no abstraction layer. The `ProviderRegistry` directly imports internal registration functions (`_build_registration_context`, `_register_dataset_with_context`) from `dataset/registration.py` at `catalog/provider_registry.py:366-369`.

**Findings:**
- `catalog/provider_registry.py:366-369`: `from datafusion_engine.dataset.registration import _build_registration_context, _register_dataset_with_context` -- imports private functions from another module.
- No protocol exists for `SchemaIntrospector`; high-level modules like `views/graph.py` depend on the concrete class.

**Suggested improvement:**
Define a `SchemaIntrospectionProtocol` and inject it. Stop importing private `_`-prefixed functions across module boundaries; expose them via a registration service.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
The scope uses composition throughout. `RegistrySchemaProvider`, `RegistryCatalogProvider`, and `MultiRegistryCatalogProvider` in `catalog/provider.py` compose `DatasetCatalog` instances rather than inheriting from them. `ViewNode` composes `builder` callables rather than using template method patterns.

No action needed.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Several chain-access patterns exist in the view registration code.

**Findings:**
- `views/graph.py:397`: `runtime_profile.catalog.enable_information_schema` -- two levels deep into runtime profile internals.
- `views/graph.py:469`: `profile.diagnostics.explain_analyze_threshold_ms` -- reaching through `profile` into nested config.
- `views/graph.py:1091`: `location.resolved.delta_write_policy.partition_by` -- three levels of attribute access.

**Suggested improvement:**
Add convenience methods on `DataFusionRuntimeProfile` like `information_schema_enabled()` and `explain_analyze_threshold_ms()` to avoid chain-traversal.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`_register_view_node` at `views/graph.py:280-308` asks multiple questions about `context` fields before deciding what to do. The function calls 7 `_maybe_*` helpers, each of which re-checks `context.runtime.runtime_profile is None`.

**Findings:**
- `views/graph.py:280-308`: `_register_view_node` queries `context.options.validate_schema`, `node.contract_builder is not None`, `context.runtime.runtime_profile is None` repeatedly across helper calls.

**Suggested improvement:**
Push the "should I do this?" checks into the `ViewGraphContext` object itself, e.g., `context.should_validate_schema(node)`, `context.should_capture_scan_units(node)`.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (P16-P18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Schema validation logic (`SchemaContract.validate_against_schema`, `validate_against_introspection`) is largely pure -- it accepts data and returns violations without side effects. The imperative shell exists in `views/graph.py` (registration, caching, artifact recording). However, `schema/finalize.py` mixes schema alignment transforms (pure) with DataFusion session registration (imperative) within the same function calls.

**Findings:**
- `schema/finalize.py:43`: Large import list pulling in session helpers, UDF utilities, and registration functions alongside pure schema transforms.
- `schema/contracts.py:470-840`: `SchemaContract` validation methods are well-separated pure functions.

**Suggested improvement:**
In `finalize.py`, separate the pure schema-transform pipeline from the session-registration calls.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Registration functions use `overwrite=True` flags. Fingerprinting is deterministic. Cache hit resolution checks identity hashes before recomputing. `ProviderRegistry.register_spec()` explicitly checks for existing registrations.

No action needed.

---

#### P18. Determinism -- Alignment: 3/3

**Current state:**
Excellent determinism throughout. `SchemaContract.from_arrow_schema()` computes deterministic `schema_identity_hash`. `TableSpec.cache_key()` uses canonical JSON serialization + SHA-256. `IntrospectionSnapshot.capture()` sorts column results by ordinal position. The topo-sort in `views/graph.py:1391-1416` uses stable `sorted()` for deterministic output.

No action needed.

---

### Category: Simplicity (P19-P22)

#### P19. KISS -- Alignment: 1/3

**Current state:**
`schema/__init__.py` has a 68-symbol `__all__` list with a lazy-loading `__getattr__` dispatch mechanism using `_MODULE_ALIASES` and `_EXPORT_MAP` dictionaries. This is complex infrastructure for import convenience that makes it difficult to understand which module owns which symbol.

**Findings:**
- `schema/__init__.py:1-80`: 68 symbols in `__all__`, 3 module aliases, and a large `_EXPORT_MAP` for lazy resolution.
- `schema/extraction_schemas_core.py` and `schema/extraction_schemas_extended.py` are 25-line wrapper modules that add a thin grouping layer over `extraction_schemas.py` without meaningful abstraction.

**Suggested improvement:**
Remove the lazy-loading `__init__.py` dispatch and have callers import directly from submodules. Remove `extraction_schemas_core.py` and `extraction_schemas_extended.py` if they have no consumers beyond re-export convenience.

**Effort:** medium
**Risk if unaddressed:** medium -- The 68-symbol facade masks where knowledge actually lives and makes it hard for new contributors to find the source of truth.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
`EvolutionPolicy` at `schema/contracts.py:419-444` defines three policies (`STRICT`, `ADDITIVE`, `RELAXED`), but only `STRICT` appears to be checked in validation logic (line 672, 704). `ADDITIVE` and `RELAXED` exist as enum values with no behavioral implementation.

**Findings:**
- `schema/contracts.py:419-424`: `EvolutionPolicy.ADDITIVE` and `EvolutionPolicy.RELAXED` are defined but never checked in the `validate_against_*` methods.
- `schema/extraction_schemas_core.py` and `schema/extraction_schemas_extended.py` are speculative grouping modules with minimal usage.

**Suggested improvement:**
Document `ADDITIVE`/`RELAXED` as planned but unimplemented, or remove them if there is no near-term use case. Remove unused facade modules.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
`_normalized_aliases` and `_normalized_parameter_names` at `schema/introspection.py:458-483` are two separate functions with identical bodies. A reader encountering them would expect different behavior given the different names, but they do exactly the same thing. Additionally, `SchemaContract._arrow_type_to_sql` at line 857 is a `@staticmethod` that could be a standalone function -- callers must reference it as `SchemaContract._arrow_type_to_sql()`, which is surprising for a private static method.

**Findings:**
- `schema/introspection.py:458-483`: `_normalized_aliases` and `_normalized_parameter_names` have identical implementations.
- `schema/contracts.py:857-894`: `_arrow_type_to_sql` is a static method that self-references via `SchemaContract._arrow_type_to_sql(storage_type)` at line 893 -- unusual pattern.

**Suggested improvement:**
Merge the two identical functions into `_normalize_name_mapping()`. Extract `_arrow_type_to_sql` as a module-level function.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
`__all__` is declared consistently in most modules. However, `schema/__init__.py` exports `_resolve_nested_row_schema_authority` and `_semantic_validation_tables` (underscore-prefixed names) in its `__all__`, violating the convention that `_`-prefixed symbols are internal.

**Findings:**
- `schema/__init__.py:33-34`: `_resolve_nested_row_schema_authority` and `_semantic_validation_tables` exported in `__all__` despite underscore prefix.
- `catalog/provider_registry.py:366-369`: Imports `_build_registration_context` and `_register_dataset_with_context` -- cross-module private function access.

**Suggested improvement:**
Either rename the exported functions to remove the underscore prefix, or stop exporting them via `__all__`. Stop importing private functions across module boundaries.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (P23-P24)

#### P23. Testability -- Alignment: 2/3

**Current state:**
Testing `SchemaContract` validation requires constructing an `IntrospectionSnapshot` (which needs a live `SessionContext` to `capture()`). Testing view registration requires a full `ViewGraphContext` with a live session. There is no lightweight fake or protocol for substitution. The `tables/metadata.py` module uses a module-level `WeakKeyDictionary` global, which complicates isolated testing.

**Findings:**
- `tables/metadata.py:246-249`: `_TABLE_PROVIDER_METADATA_BY_CONTEXT` is a module-level mutable global.
- `catalog/introspection.py:378-379`: `IntrospectionCacheRegistry` uses a `WeakKeyDictionary` global.
- `schema/contracts.py:602-685`: `validate_against_introspection()` takes an `IntrospectionSnapshot` -- testable if you can construct a snapshot, but `capture()` requires a session.

**Suggested improvement:**
Add `IntrospectionSnapshot.from_dicts()` or `IntrospectionSnapshot.empty()` factory methods for test construction. Define a `MetadataStore` protocol to replace the global `WeakKeyDictionary` pattern, enabling DI in tests.

**Effort:** medium
**Risk if unaddressed:** medium -- Tests for schema validation and view registration are heavyweight and slow.

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
Excellent observability throughout. `views/graph.py` records artifacts for schema contract violations, UDF parity, UDF audit, cache events, plan artifacts, and explain-analyze thresholds. All use structured `record_artifact()` calls with typed payloads via `serde_artifact_specs`. `ProviderRegistry._emit_registration_diagnostic()` records registration events. `views/graph.py:573-629` uses `cache_span()` for OpenTelemetry-integrated cache observability.

No action needed.

---

### Category: Boilerplate (cross-cutting, not a numbered principle)

**Additional finding:**
`tables/metadata.py:113-243` contains four `with_*` methods (`with_ddl`, `with_constraints`, `with_schema_identity_hash`, `with_schema_adapter`) that each manually reconstruct the `TableProviderMetadata` dataclass by copying all 16 fields. This is 130 lines of brittle boilerplate that would be a single `dataclasses.replace(self, ddl=ddl)` call. Because the class is `frozen=True`, `replace()` is the idiomatic pattern.

**Suggested improvement:**
Replace all `with_*` methods with `dataclasses.replace()` at call sites, or provide a single generic `evolve(**kwargs)` method.

**Effort:** small
**Risk if unaddressed:** medium -- Adding a new field to `TableProviderMetadata` requires updating all four `with_*` methods.

---

## Cross-Cutting Themes

### Theme 1: SchemaIntrospector as a God Class

`SchemaIntrospector` (1305 LOC module, ~500 LOC class) is the single most coupling-intensive entity in the scope. It is constructed directly in 25+ callsites, mixes SQL execution with caching with fingerprinting, and has no protocol for substitution. This pattern simultaneously degrades P2 (separation of concerns), P4 (coupling), P6 (ports & adapters), P12 (dependency inversion), and P23 (testability).

**Root cause:** `SchemaIntrospector` was likely a natural growth point -- whenever someone needed schema metadata, they added a method to the introspector rather than extracting a focused service.

**Suggested approach:** Define a `SchemaIntrospectionPort` protocol, extract focused modules for constraint parsing and fingerprinting, and provide a session-scoped factory for introspector creation.

### Theme 2: Duplicated Delta Cache Registration Logic

The `_register_delta_staging_cache` (185 LOC) and `_register_delta_output_cache` (187 LOC) functions in `views/graph.py` share ~80% of their structure. This simultaneously violates P7 (DRY), P3 (SRP -- graph.py changes for cache reasons), and P19 (KISS -- 370 LOC of cache logic in a view module).

**Root cause:** The two cache policies evolved independently with slightly different path resolution and evolution semantics, but the core write-check-register-record flow is identical.

**Suggested approach:** Extract a shared `DeltaCacheWriter` that accepts a `CacheResolutionStrategy` (staging vs output), consolidating the common orchestration.

### Theme 3: Untyped Intermediate Representations

Multiple methods in `SchemaIntrospector` return `list[dict[str, object]]` rather than typed structures. This forces downstream code to use string-keyed dict access (`row.get("column_name")`), which provides no compile-time safety. This theme affects P8 (design by contract), P9 (parse don't validate), and P10 (illegal states).

**Root cause:** The dict-based pattern mirrors the natural shape of Arrow `to_pylist()` output and was likely the path of least resistance.

**Suggested approach:** Define `TypedDict` or `NamedTuple` types for common row shapes (`ColumnMetadataRow`, `RoutineMetadataRow`, `ConstraintRow`) and convert at the boundary.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Delete `_schema_from_table` in `views/graph.py:1352` and use `schema_from_table` from `schema/introspection.py` | small | Eliminates silently-divergable duplicate |
| 2 | P7/P21 (DRY/Astonishment) | Merge `_normalized_aliases` and `_normalized_parameter_names` at `schema/introspection.py:458-483` into one function | small | Removes copy-paste confusion |
| 3 | Boilerplate | Replace 4 `with_*` methods (130 LOC) in `tables/metadata.py:113-243` with `dataclasses.replace()` | small | Eliminates field-copy drift risk |
| 4 | P22 (Contracts) | Remove underscore-prefixed symbols from `schema/__init__.py __all__` | small | Cleaner public surface |
| 5 | P11 (CQS) | Extract `SchemaIntrospector.from_context()` factory to separate construction from lazy query | small | Eliminates `object.__setattr__` hack on frozen dataclass |

## Recommended Action Sequence

1. **[P7, small]** Delete `_schema_from_table` in `views/graph.py:1352-1366` and replace its sole callsite with `schema_from_table` from `schema/introspection.py`. Merge the two identical `_normalized_*` functions.

2. **[Boilerplate, small]** Replace `with_ddl`, `with_constraints`, `with_schema_identity_hash`, `with_schema_adapter` in `tables/metadata.py` with `dataclasses.replace()` at call sites.

3. **[P22, small]** Clean up `schema/__init__.py`: remove underscore-prefixed symbols from `__all__`, and stop importing private `_`-prefixed functions across module boundaries in `catalog/provider_registry.py`.

4. **[P3/P7, medium]** Extract `_register_delta_staging_cache` and `_register_delta_output_cache` from `views/graph.py` into a shared `views/cache_registration.py` module with a parameterized common path.

5. **[P2, medium]** Extract `schema/fingerprint.py` and `schema/constraint_parsing.py` from `schema/introspection.py` to reduce it from 1305 LOC to ~700 LOC focused on catalog reflection.

6. **[P6/P12/P23, large]** Define `SchemaIntrospectionPort` protocol. Provide a factory on the runtime profile. Migrate the 25+ callsites to use the factory rather than direct construction.

7. **[P19, medium]** Simplify `schema/__init__.py` by removing the lazy-load dispatch mechanism and having callers import from submodules directly. Remove `extraction_schemas_core.py` and `extraction_schemas_extended.py` if unused.
