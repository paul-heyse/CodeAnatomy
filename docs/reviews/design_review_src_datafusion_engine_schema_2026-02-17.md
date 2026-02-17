# Design Review: DataFusion Schema + Arrow

**Date:** 2026-02-17
**Scope:** `src/datafusion_engine/schema/`, `src/datafusion_engine/arrow/`, `src/datafusion_engine/encoding/`, `src/datafusion_engine/generated/`
**Focus:** All principles (1-24), with special emphasis on knowledge (7-11) and simplicity (19-22)
**Depth:** moderate
**Files reviewed:** 44 (16,103 LOC)

## Executive Summary

The DataFusion schema and Arrow interop layer is architecturally sound in its separation of protocol definitions from concrete implementations, and demonstrates strong contract-based validation through `SchemaContract`, `SchemaPolicy`, and the finalize pipeline. However, three systemic issues reduce overall alignment: (1) duplicated type-mapping knowledge across `field_types.py`, `contracts.py:875`, and `encoding/policy.py:148`, (2) the `SchemaIntrospector` class at 731 LOC accumulates too many responsibilities (introspection, caching, function catalog, settings, constraints), and (3) duplicated helper functions (`_read_only_sql_options`, `_introspection_cache_for_ctx`) appear verbatim in both `introspection_core.py` and `introspection_routines.py`. The `arrow/interop.py` module is an excellent example of ports-and-adapters design, but its 909-line surface (protocols + re-exported type aliases + compute module protocol) could benefit from narrower sub-module splits.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `SchemaIntrospector` exposes `ctx` directly; `SchemaContract._arrow_type_to_sql` is static but encodes mapping knowledge as a local dict |
| 2 | Separation of concerns | 2 | medium | medium | `finalize.py` mixes invariant checking, error aggregation, DataFusion SQL ops, and schema alignment in one 941-LOC module |
| 3 | SRP | 1 | medium | medium | `SchemaIntrospector` (731 LOC) changes for schema queries, function catalogs, settings, constraints, and plan introspection |
| 4 | High cohesion, low coupling | 2 | medium | medium | `contracts.py` (1263 LOC) combines constraint specs, evolution policy, contract validation, divergence detection, and factory functions |
| 5 | Dependency direction | 3 | - | low | No outer-ring (cli/graph) imports found; arrow/interop.py is correctly inner-ring |
| 6 | Ports & Adapters | 3 | - | low | Protocol definitions in `interop.py` and `spec_protocol.py` cleanly separate needs from implementations |
| 7 | DRY (knowledge) | 1 | small | high | Type mapping duplicated in 3 locations; `SchemaMapping` type alias defined twice; `_read_only_sql_options` duplicated |
| 8 | Design by contract | 3 | - | low | `SchemaContract`, `TableConstraints`, `ConstraintSpec` provide explicit contracts with validation |
| 9 | Parse, don't validate | 2 | medium | medium | `_normalize_contract_type` parses type strings ad-hoc; no structured type representation at boundary |
| 10 | Make illegal states unrepresentable | 2 | small | low | `ConstraintSpec` allows `column` and `columns` to both be None simultaneously; `SchemaPolicyOptions` has all-optional fields |
| 11 | CQS | 2 | small | low | `SchemaIntrospector.describe_query` both queries schema AND writes to disk cache; `finalize()` returns result AND raises on strict errors |
| 12 | DI + explicit composition | 2 | medium | medium | `encoding/policy.py:141` creates `DataFusionRuntimeProfile()` internally; `finalize.py:648` hides session creation |
| 13 | Prefer composition | 3 | - | low | No deep inheritance hierarchies; behavior composed via `SchemaPolicy`, `SchemaTransform`, `FinalizeContext` |
| 14 | Law of Demeter | 2 | small | low | `introspection_cache.py:48` calls `schema_map()` on opaque `object` via `getattr`; `validation.py:112` chains `profile.session_runtime().ctx` |
| 15 | Tell, don't ask | 2 | small | low | `SchemaIntrospector` exposes raw dicts from `to_pylist()` -- callers must extract and interpret fields |
| 16 | Functional core, imperative shell | 2 | medium | medium | `finalize()` mixes pure invariant evaluation with DataFusion session operations and temp table lifecycle |
| 17 | Idempotency | 3 | - | low | Schema alignment, validation, and finalize produce same outputs for same inputs |
| 18 | Determinism | 3 | - | low | Fingerprinting via `schema_identity_hash`, `config_fingerprint` ensures reproducible contracts |
| 19 | KISS | 2 | small | low | `__init__.py` lazy-loading system (307 LOC) is complex for what is essentially a re-export facade |
| 20 | YAGNI | 2 | small | low | `SchemaEvolutionSpec` has `rename_map` and `allow_casts` but no evidence of usage beyond defaults |
| 21 | Least astonishment | 2 | small | medium | `align_to_schema` ignores `safe_cast` parameter (`_ = safe_cast` at line 57); `finalize` can both return and raise |
| 22 | Declare/version contracts | 3 | - | low | `METADATA_PAYLOAD_VERSION`, `ORDERING_KEYS_VERSION`, `EXTRACTOR_DEFAULTS_VERSION` explicitly version payloads |
| 23 | Design for testability | 2 | medium | medium | `_datafusion_context()` in `encoding/policy.py:141` creates session internally; `finalize` requires full runtime |
| 24 | Observability | 2 | small | low | `arrow/interop.py` has logger but sparse usage; `metadata.py` has good logging; schema validation lacks structured metrics |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The protocol-based design in `arrow/interop.py` and `spec_protocol.py` provides excellent abstraction boundaries. However, `SchemaIntrospector` at `introspection_core.py:242` exposes its `ctx: SessionContext` as a public attribute, allowing callers to bypass the introspection interface entirely.

**Findings:**
- `introspection_core.py:242-251`: `SchemaIntrospector` exposes `ctx`, `sql_options`, `cache`, `cache_prefix`, `cache_ttl`, `introspection_cache`, and `snapshot` as public fields on a frozen dataclass. Only `ctx` and optionally `sql_options` should be public; the rest are implementation details.
- `contracts.py:857-894`: `SchemaContract._arrow_type_to_sql` is a static method that encodes a type mapping as a local dictionary. This mapping is an internal decision that should be hidden behind a single authoritative type conversion function.

**Suggested improvement:**
Narrow `SchemaIntrospector`'s public surface to `ctx` (or better, hide it behind a protocol) and move cache-related fields to a private `_CacheConfig` or pass them via a factory function. The type mapping in `_arrow_type_to_sql` should delegate to the existing `schema_spec.arrow_types` module.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
`finalize.py` (941 LOC) is the most concerning module for separation of concerns. It combines: (a) schema alignment policy resolution, (b) invariant evaluation (null checks, custom invariants), (c) error table construction with DataFusion aggregation, (d) deduplication, (e) canonical sorting, and (f) DataFusion temp table lifecycle management.

**Findings:**
- `finalize.py:736-824`: The `finalize()` function orchestrates 8 distinct phases in a single function body, mixing pure logic (invariant evaluation) with imperative DataFusion operations (temp table registration/deregistration, aggregation queries).
- `validation.py:623-672`: `validate_table()` similarly mixes alignment, null checking, key field resolution, cast checking, and row filtering in one orchestration function with 6 internal helper calls.

**Suggested improvement:**
Extract the pure invariant evaluation into a `InvariantEvaluator` that returns results without touching DataFusion. The DataFusion error aggregation (`_aggregate_error_detail_lists_df`) should be a separate adapter. The `finalize()` function should become a thin orchestrator composing these pieces.

**Effort:** medium
**Risk if unaddressed:** medium -- the current monolithic structure makes it difficult to test invariant logic without a full DataFusion session.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`SchemaIntrospector` at `introspection_core.py:242-708` has at least 6 distinct responsibilities: table column queries, schema reflection, routine/function catalog queries, settings queries, constraint queries, and schema map construction. It changes whenever any of these concerns evolves.

**Findings:**
- `introspection_core.py:242-708`: `SchemaIntrospector` has 17 public methods spanning table columns, routines, parameters, function catalogs, settings, constraints, schema maps, table definitions, and logical plans. Each method family would change for a different reason.
- `contracts.py:1-1263`: This module contains `ConstraintSpec`, `TableConstraints`, `EvolutionPolicy`, `SchemaContract`, `ContractRegistry`, `SchemaDivergence`, plus 7 factory functions. It changes for constraint logic, evolution policy, contract validation, divergence detection, or factory construction.

**Suggested improvement:**
Split `SchemaIntrospector` into focused facades: `TableIntrospector` (table/column queries), `FunctionIntrospector` (routines/parameters), and `SettingsIntrospector` (settings/config). The `SchemaIntrospector` becomes a thin composite. Similarly, extract `ConstraintSpec`/`TableConstraints` and `SchemaDivergence` from `contracts.py` into their own modules.

**Effort:** medium
**Risk if unaddressed:** medium -- as DataFusion's information_schema surface expands, `SchemaIntrospector` will continue to accumulate methods.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Internal cohesion within `arrow/interop.py` is strong -- all Arrow protocol definitions live together. However, `contracts.py` (1263 LOC) bundles weakly-related concepts: constraint specifications, schema contracts, evolution policies, divergence detection, and multiple factory functions that each pull from different parts of the codebase.

**Findings:**
- `contracts.py:141-235`: `ConstraintSpec` and `TableConstraints` are constraint-domain types that exist alongside `SchemaContract` which is a validation-domain type. These rarely change together.
- `contracts.py:1173-1238`: `SchemaDivergence` and `compute_schema_divergence` are schema-comparison utilities unrelated to the contract validation logic above them.

**Suggested improvement:**
Extract constraint types (`ConstraintSpec`, `TableConstraints`, constraint factory functions) into `schema/constraints.py`. Move `SchemaDivergence` into `schema/divergence.py`. This would reduce `contracts.py` to ~700 LOC of pure contract logic.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P5. Dependency direction -- Alignment: 3/3

**Current state:**
No outer-ring imports (cli, graph) were found in any scope module. Arrow interop protocols (`interop.py`) sit at the inner ring. Schema modules depend inward on `schema_spec`, `core`, `utils`, and `validation` -- all appropriately inner-ring.

**Findings:**
- Clean dependency direction confirmed across all 44 files.
- `arrow/interop.py` correctly defines protocols that callers depend on, not the reverse.

**Effort:** N/A
**Risk if unaddressed:** low

---

#### P6. Ports & Adapters -- Alignment: 3/3

**Current state:**
The protocol system in `arrow/interop.py` (`TableLike`, `SchemaLike`, `ArrayLike`, `DataTypeLike`, etc.) and `spec_protocol.py` (`ArrowFieldSpec`, `TableSchemaSpec`) provide clean port definitions. Concrete PyArrow types are adapted through these protocols. The `_SchemaContractAdapter` in `validation.py:42-62` is a textbook adapter.

**Findings:**
- `interop.py:19-468`: Comprehensive protocol definitions for all Arrow types used in the codebase.
- `validation.py:42-62`: `_SchemaContractAdapter` adapts `SchemaContract` to the `TableSchemaSpec` protocol.
- `spec_protocol.py:1-70`: Clean port definitions for schema specification.

**Effort:** N/A
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
Type-mapping knowledge is the primary DRY violation. Three separate locations encode overlapping knowledge about Arrow-to-string type mappings, and two utility functions are copy-pasted between introspection modules.

**Findings:**
- `field_types.py:28-35` (`_TYPE_HINT_MAP`), `contracts.py:875-882` (`type_map` in `_arrow_type_to_sql`), and `encoding/policy.py:148-154` (`_ensure_arrow_dtype`) each encode their own version of Arrow type resolution. The first maps strings to `pa.DataType`, the second maps `pa.DataType` to SQL strings, and the third converts spec types to `pa.DataType`. All three encode the same fundamental knowledge about the Arrow type system but in incompatible directions.
- `introspection_core.py:40` and `introspection_cache.py:11`: `SchemaMapping` type alias is defined identically in both files (`dict[str, dict[str, dict[str, dict[str, str]]]]`).
- `introspection_core.py:109-123` and `introspection_routines.py:19-30`: Both `_read_only_sql_options()` and `_introspection_cache_for_ctx()` are duplicated verbatim.
- `introspection_core.py:47-64` wraps `schema_map_fingerprint_from_mapping` from `introspection_cache.py` as a delegate -- the function exists in both modules.

**Suggested improvement:**
1. Create a single `schema/type_mapping.py` module with bidirectional Arrow type resolution (string-to-DataType and DataType-to-string).
2. Move `SchemaMapping` to a single location (e.g., `introspection_cache.py`) and import it elsewhere.
3. Extract `_read_only_sql_options()` and `_introspection_cache_for_ctx()` to a shared `introspection_common.py` module.
4. Remove the delegate wrappers in `introspection_core.py:47-84`.

**Effort:** small
**Risk if unaddressed:** high -- type mapping drift between `field_types.py` and `contracts.py` can cause silent schema mismatches during validation.

---

#### P8. Design by contract -- Alignment: 3/3

**Current state:**
This is a strength of the reviewed modules. `SchemaContract` at `contracts.py:470-925` provides explicit declarative contracts with `validate_against_introspection()` and `validate_against_schema()`. `TableConstraints` encodes NOT NULL, PK, UNIQUE, and CHECK constraints. `Contract` in `finalize.py:88-132` provides output contracts with invariants.

**Findings:**
- `contracts.py:602-685`: `validate_against_introspection()` checks table existence, column presence, type compatibility, and extra columns based on evolution policy.
- `finalize.py:88-132`: `Contract` bundles schema, key fields, required non-null columns, invariant functions, and determinism constraints.
- `policy.py:23-76`: `SchemaPolicy` provides a unified contract for alignment, encoding, and validation.

**Effort:** N/A
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Type string normalization in `contracts.py` operates on raw strings rather than parsing into a structured type representation. The `_normalize_contract_type` function at `contracts.py:124-138` applies ad-hoc string transformations to compare types.

**Findings:**
- `contracts.py:82-138`: `_extract_inner_type`, `_split_top_level_types`, `_strip_type_prefix`, and `_normalize_contract_type` implement a mini type-string parser through string manipulation. This ad-hoc parsing is fragile -- it handles `list(...)` and `list<...>` syntax but could miss edge cases.
- `contracts.py:897-904`: `_normalize_type_string` applies case folding and string replacements (`largeutf8` -> `string`, `utf8` -> `string`) before calling `_normalize_contract_type`. This is validation-style processing rather than parsing into a structured representation.

**Suggested improvement:**
Parse type strings into a structured `TypeDescriptor` enum/dataclass once at the boundary (e.g., when building `SchemaContract.from_arrow_schema`), then compare structured representations rather than normalized strings.

**Effort:** medium
**Risk if unaddressed:** medium -- the string-based normalization could produce false positive or false negative type compatibility results for complex nested types.

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Most data types use frozen dataclasses or msgspec Structs, which prevents mutation. However, some types allow semantically invalid combinations.

**Findings:**
- `contracts.py:141-159`: `ConstraintSpec` allows `column=None` and `columns=None` simultaneously, which means a constraint can have no target. The `constraint_type` field should constrain which of `column`/`columns`/`expression` is populated (e.g., `check` requires `expression`, `pk`/`not_null` require `column` or `columns`).
- `policy.py:132-188`: `SchemaPolicyOptions` has every field optional (`schema: SchemaLike | None`, `encoding: EncodingPolicy | None`, etc.). The resolved `SchemaPolicy` always has concrete values, so the options type allows a state that downstream code cannot handle.
- `alignment.py:193-199`: `SchemaEvolutionSpec` has `allow_missing: bool = False` and `allow_extra: bool = True` but these are not enforced -- the `unify_and_cast` method does not check them.

**Suggested improvement:**
Model `ConstraintSpec` as a union of typed variants: `PrimaryKeyConstraint(columns)`, `CheckConstraint(expression)`, `NotNullConstraint(column)`, `UniqueConstraint(columns)`. Use `typing.Required` or separate builder for `SchemaPolicyOptions` to ensure the factory always produces valid state.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. Two notable exceptions exist.

**Findings:**
- `introspection_core.py:288-324`: `SchemaIntrospector.describe_query()` both queries the schema (query) AND writes to disk cache (command). The caching side effect is hidden from callers.
- `finalize.py:787`: Inside `finalize()`, `_raise_on_errors_if_strict(raw_errors, mode=options.mode, contract=contract)` is called after building the result -- meaning the function both constructs a return value AND conditionally raises. A caller cannot predict whether they will get a result or an exception.

**Suggested improvement:**
For `describe_query`, separate the caching concern via a decorator or explicit cache-aside pattern. For `finalize`, make the strict-mode check happen before result construction, or return a `FinalizeResult` that includes a `valid` flag and let callers decide whether to raise.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
Several modules create internal dependencies rather than accepting them.

**Findings:**
- `encoding/policy.py:141-145`: `_datafusion_context()` internally creates a `DataFusionRuntimeProfile()` and calls `session_runtime().ctx`. This hidden construction prevents testing without a live DataFusion session.
- `finalize.py:648-654`: `_datafusion_context(runtime_profile)` similarly hides session creation. If `runtime_profile` is None, it returns None, but if present it calls `runtime_profile.session_context()` -- the session is not injected but derived.
- `validation.py:108-112`: `_session_context()` creates a `DataFusionRuntimeProfile` if none is provided, hiding the dependency.

**Suggested improvement:**
Accept `SessionContext` (or a protocol representing it) as an explicit parameter rather than creating it internally. For `encoding/policy.py`, accept a `SessionContext` parameter in `apply_encoding()` and let the caller provide it.

**Effort:** medium
**Risk if unaddressed:** medium -- hinders unit testing and creates hidden coupling to session lifecycle.

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
No deep inheritance hierarchies. All behavior is composed through dataclass composition (`SchemaPolicy` composes `SchemaTransform`, `EncodingPolicy`; `FinalizeContext` composes `Contract`, `ErrorArtifactSpec`, `ChunkPolicy`).

**Effort:** N/A
**Risk if unaddressed:** low

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
A few chain-call patterns reach through collaborators.

**Findings:**
- `validation.py:112`: `profile.session_runtime().ctx` chains through an intermediate object.
- `encoding/policy.py:144-145`: `DataFusionRuntimeProfile().session_runtime().ctx` is a 3-level chain.
- `introspection_cache.py:48-53`: `getattr(introspector, "schema_map", None)` uses duck-typing via `getattr` on an opaque `object` parameter, then calls the result.

**Suggested improvement:**
Use direct injection of `SessionContext` instead of chaining through `RuntimeProfile -> SessionRuntime -> ctx`. For `introspection_cache.py`, accept a typed protocol instead of `object`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`SchemaIntrospector` returns raw `list[dict[str, object]]` from most methods, forcing callers to extract and interpret fields. The `to_pylist()` -> dict pattern is used extensively.

**Findings:**
- `introspection_core.py:326-397`: `table_columns`, `table_columns_with_ordinal`, and `tables_snapshot` all return `list[dict[str, object]]`. Callers must know the dict keys (`column_name`, `data_type`, etc.) to use the data.
- `introspection_core.py:479-530`: `routines_snapshot` and `parameters_snapshot` similarly return raw dict lists.

**Suggested improvement:**
Define typed result dataclasses (e.g., `ColumnInfo`, `RoutineInfo`, `ParameterInfo`) and return those instead of raw dicts. The `SchemaIntrospector` methods would then "tell" callers the data rather than exposing raw structure.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The core type definitions (`interop.py`, `types.py`, `encoding.py`, `metadata_codec.py`) are pure and deterministic. However, `finalize.py` and `validation.py` interleave pure logic with DataFusion session operations.

**Findings:**
- `finalize.py:310-379`: `_required_non_null_results` and `_collect_invariant_results` are pure functions operating on Arrow arrays -- good functional core.
- `finalize.py:560-614`: `_row_id_for_errors` and `_aggregate_error_detail_lists` require a live DataFusion session for hash computation and aggregation -- imperative shell that is mixed into the same module.
- `validation.py:236-275`: `_cast_failure_count` and `_duplicate_row_count` execute DataFusion SQL to validate data -- imperative operations embedded in the validation module.

**Suggested improvement:**
Keep invariant evaluation pure (as it already is). Extract the DataFusion-dependent operations (`_row_id_for_errors`, `_aggregate_error_detail_lists_df`, `_cast_failure_count`) into a `finalize_runtime.py` adapter module that `finalize()` calls through an injected interface.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Schema alignment, validation, and finalize are all idempotent -- applying the same operation to the same input produces the same output. Temp tables are registered with unique prefixes and cleaned up in `finally` blocks.

**Effort:** N/A
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Fingerprinting is comprehensive and deterministic. `schema_identity_hash`, `config_fingerprint`, and `options_hash` use canonical msgpack serialization with sorted keys. Metadata payload versioning (`METADATA_PAYLOAD_VERSION`, `ORDERING_KEYS_VERSION`, `EXTRACTOR_DEFAULTS_VERSION`) ensures reproducibility across versions.

**Findings:**
- `metadata.py:280-290`: `options_hash` normalizes values (sorts dicts, converts paths to POSIX) before hashing.
- `metadata_codec.py:165-180`: `encode_metadata_payload` uses base64-encoded msgpack with a prefix for safe round-tripping through Arrow metadata (bytes -> UTF-8 safe string -> bytes).

**Effort:** N/A
**Risk if unaddressed:** low

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The lazy-loading `__init__.py` in `schema/` is 307 LOC of boilerplate mapping 66+ exports through `_EXPORT_MAP` and `_MODULE_ALIASES`. This is mechanically correct but imposes a maintenance cost: every new export requires updating three places (the actual module, `_EXPORT_MAP`, and `__all__`).

**Findings:**
- `schema/__init__.py:1-307`: The `__getattr__` lazy-loading pattern duplicates every export name three times: in `__all__`, in `_EXPORT_MAP`, and in the `TYPE_CHECKING` block. This is 307 LOC of pure wiring.
- `metadata.py:1210-1219`: Module-level `__getattr__` and `__dir__` overrides to lazily expose `TableSchemaSpec` -- adding complexity to avoid a circular import.

**Suggested improvement:**
Consider using explicit submodule imports at the call site (e.g., `from datafusion_engine.schema.contracts import SchemaContract`) rather than routing everything through `schema/__init__.py`. This would eliminate the 307-LOC facade entirely and make import dependencies explicit.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
A few abstractions appear to have unused capabilities.

**Findings:**
- `alignment.py:193-199`: `SchemaEvolutionSpec` has `rename_map`, `allow_missing`, `allow_extra`, and `allow_casts` fields, but `unify_and_cast` does not check `allow_missing`, `allow_extra`, or `allow_casts`. These fields are speculative extension points that are not enforced.
- `interop.py:711-777`: `ComputeModule` protocol mirrors ~40 PyArrow compute functions. This exhaustive protocol is rarely type-checked against -- it is cast from `_pc` at line 831. The protocol duplicates PyArrow's own type stubs.

**Suggested improvement:**
Remove unused fields from `SchemaEvolutionSpec` or implement their enforcement. For `ComputeModule`, consider whether the protocol provides value beyond what PyArrow's type stubs already offer; if not, simplify to `pc = _pc` with appropriate type annotations.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Two API behaviors would surprise a competent reader.

**Findings:**
- `alignment.py:57`: `align_to_schema` accepts a `safe_cast: bool` parameter but immediately discards it with `_ = safe_cast`. A caller passing `safe_cast=False` would expect unsafe casts, but the parameter is silently ignored. This is a "lying API."
- `finalize.py:787`: `finalize()` can both return a `FinalizeResult` AND raise `ValueError` when `mode="strict"` and errors exist. The raise happens after the result is already constructed, making the control flow surprising.
- `alignment.py:293-294`: `from serde_msgspec import StructBaseStrict` appears at module bottom, after `__all__`. This is a duplicate import (already at line 9) that violates Python conventions.

**Suggested improvement:**
Either remove the `safe_cast` parameter from `align_to_schema` or implement its semantics. For `finalize`, move strict-mode checking before result construction, or document clearly that strict mode raises. Remove the duplicate import at line 294.

**Effort:** small
**Risk if unaddressed:** medium -- the ignored `safe_cast` parameter could lead to data corruption if callers rely on it for safety guarantees.

---

#### P22. Declare and version public contracts -- Alignment: 3/3

**Current state:**
Metadata payloads are explicitly versioned. `__all__` exports are declared in every module. The `SchemaContract` type and `EvolutionPolicy` enum provide clear contract boundaries.

**Findings:**
- `metadata_codec.py:12`: `METADATA_PAYLOAD_VERSION = 1` versioning for metadata codec payloads.
- `metadata.py:89-91`: `ORDERING_KEYS_VERSION`, `OPTIONS_HASH_VERSION`, `EXTRACTOR_DEFAULTS_VERSION` version specific payload formats.
- Every module in scope declares `__all__` for explicit public surface.

**Effort:** N/A
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Pure types and protocols are easily testable. However, modules that create DataFusion sessions internally are difficult to unit test.

**Findings:**
- `encoding/policy.py:141-145`: `_datafusion_context()` creates a session internally. Unit tests for `apply_encoding` must stand up a full DataFusion runtime.
- `finalize.py:648-654`: `_datafusion_context(runtime_profile)` requires a `DataFusionRuntimeProfile` for error aggregation. Testing the finalize pipeline requires either a real profile or mocking this internal function.
- `validation.py:108-112`: `_session_context()` creates a session internally. The `validate_table()` function cannot be tested with a lightweight stub.
- `introspection_core.py:253-262`: `SchemaIntrospector.__post_init__` creates an `IntrospectionCache` in `__post_init__`, making it impossible to construct a test instance without a live session.

**Suggested improvement:**
Accept `SessionContext` as a constructor parameter for `SchemaIntrospector` (already done) but also for `apply_encoding()`, `validate_table()`, and `finalize()`. Create a `NullSessionContext` or protocol for testing that returns empty results.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Logging is present but inconsistent. `arrow/interop.py` and `arrow/metadata.py` have good debug-level logging. `encoding/policy.py` has structured logging. However, the schema validation and finalize modules produce no structured telemetry about their operations.

**Findings:**
- `encoding/policy.py:84-96`: Good structured logging for encoding operations (column count, row count).
- `validation.py`: No logging at all -- validation results are returned silently. A caller has no telemetry about how many null checks were performed, how many cast failures were detected, etc.
- `finalize.py`: No logging for the finalize pipeline stages. Alignment info, invariant results, and error counts are computed but not logged.

**Suggested improvement:**
Add structured logging to `validate_table()` (validation summary: table name, violation count, row count) and `finalize()` (pipeline stage timings, invariant result summary, error count). Use the existing logger pattern from `encoding/policy.py`.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Type Mapping Knowledge Fragmentation

**Description:** Arrow type resolution knowledge is scattered across three modules (`field_types.py`, `contracts.py`, `encoding/policy.py`), each encoding a different direction of the same fundamental mapping. This creates a maintenance burden where adding support for a new Arrow type requires changes in multiple locations that may not be discovered simultaneously.

**Root cause:** Each module was developed to solve its own immediate type resolution need without establishing a shared type mapping authority.

**Affected principles:** P7 (DRY), P9 (Parse don't validate), P10 (Illegal states)

**Suggested approach:** Create a canonical `schema/type_resolution.py` module with bidirectional type resolution functions: `arrow_type_to_string(pa.DataType) -> str` and `string_to_arrow_type(str) -> pa.DataType`. All three current locations would delegate to this authority.

### Theme 2: Introspection Module Duplication

**Description:** The introspection subsystem (`introspection_core.py`, `introspection_cache.py`, `introspection_helpers.py`, `introspection_routines.py`, `introspection_delta.py`) contains duplicated helper functions and delegate wrappers that add indirection without adding value. `_read_only_sql_options()` and `_introspection_cache_for_ctx()` appear in two modules. `schema_map_fingerprint_from_mapping` is wrapped in `introspection_core.py` delegating to `introspection_cache.py`.

**Root cause:** The module was decomposed from a single large file (noted in the `# NOTE(size-exception)` comment at `introspection_core.py:17`) but helper functions were copied rather than extracted to a shared location.

**Affected principles:** P7 (DRY), P3 (SRP), P4 (Cohesion)

**Suggested approach:** Extract shared utilities (`_read_only_sql_options`, `_introspection_cache_for_ctx`, `_normalized_rows`, `_sortable_text`, `_sortable_int`, `_row_sort_key`) to an `introspection_common.py` module. Remove delegate wrappers in `introspection_core.py` for functions that already live in `introspection_cache.py`.

### Theme 3: Hidden Session Construction

**Description:** Multiple modules create DataFusion sessions internally (`encoding/policy.py:141`, `validation.py:108`, `finalize.py:648`), making these modules impossible to unit test without a full runtime and creating hidden coupling to the session lifecycle.

**Root cause:** Convenience over testability -- it is easier to create a session inside the function than to thread it through the call stack.

**Affected principles:** P12 (DI), P14 (Demeter), P16 (Functional core), P23 (Testability)

**Suggested approach:** Accept `SessionContext` as an explicit parameter with a `None` default. When `None`, create a default session. This preserves backward compatibility while enabling test injection.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Deduplicate `_read_only_sql_options` / `_introspection_cache_for_ctx` across `introspection_core.py` and `introspection_routines.py` | small | Eliminates maintenance risk and reduces confusion |
| 2 | P7 (DRY) | Unify `SchemaMapping` type alias to single definition in `introspection_cache.py` | small | Removes duplicated type definition |
| 3 | P21 (Least astonishment) | Either implement `safe_cast` semantics in `align_to_schema` or remove the parameter | small | Prevents silent safety guarantee violations |
| 4 | P21 (Least astonishment) | Remove duplicate `from serde_msgspec import StructBaseStrict` at `alignment.py:294` | small | Eliminates confusing duplicate import |
| 5 | P24 (Observability) | Add structured logging to `validate_table()` and `finalize()` | small | Provides operational visibility into schema pipeline |

## Recommended Action Sequence

1. **Deduplicate introspection helpers (P7, P3):** Extract `_read_only_sql_options`, `_introspection_cache_for_ctx`, and shared sorting utilities from `introspection_core.py` and `introspection_routines.py` into a shared `introspection_common.py`. Remove delegate wrappers for `schema_map_fingerprint_from_mapping`. Unify `SchemaMapping` type alias.

2. **Fix lying API (P21):** Either implement `safe_cast` in `align_to_schema` at `alignment.py:57` or remove the parameter. Remove duplicate import at `alignment.py:294`.

3. **Create canonical type mapping (P7, P9):** Build `schema/type_resolution.py` with bidirectional type resolution. Refactor `field_types.py:_TYPE_HINT_MAP`, `contracts.py:_arrow_type_to_sql`, and `encoding/policy.py:_ensure_arrow_dtype` to delegate to it.

4. **Add observability (P24):** Add structured logging to `validate_table()` and `finalize()` following the pattern established in `encoding/policy.py`.

5. **Accept SessionContext explicitly (P12, P23):** Modify `encoding/policy.py:apply_encoding()`, `validation.py:validate_table()`, and `finalize.py:finalize()` to accept an optional `SessionContext` parameter, enabling test injection.

6. **Decompose SchemaIntrospector (P3, P4):** Split the 17-method `SchemaIntrospector` into focused facades: `TableIntrospector`, `FunctionIntrospector`, `SettingsIntrospector`, with `SchemaIntrospector` as a thin composite.

7. **Extract constraint types (P4):** Move `ConstraintSpec`, `TableConstraints`, and constraint factory functions from `contracts.py` into `schema/constraints.py`. Move `SchemaDivergence` to `schema/divergence.py`.

8. **Separate finalize concerns (P2, P16):** Extract DataFusion-dependent operations from `finalize.py` into `finalize_runtime.py`, keeping invariant evaluation pure and composable.
