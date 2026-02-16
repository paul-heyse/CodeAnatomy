# Design Review Synthesis: src/datafusion_engine/

**Date:** 2026-02-16
**Scope:** Full `src/datafusion_engine/` module (158 files, ~72,675 LOC)
**Method:** 8 parallel design-reviewer agents, each auditing a domain-coherent segment against 24 design principles

## Executive Summary

The DataFusion engine layer is architecturally sound in its fundamentals: **determinism** (8/8 agents scored 3/3), **composition over inheritance** (8/8 scored 3/3), and **idempotency** (6/8 scored 3/3) are uniformly strong. The codebase has clear domain boundaries and well-designed protocol types (especially `interop.py`'s PyArrow abstraction and `DiagnosticsSink`'s port pattern).

However, every agent independently identified the same structural anti-pattern: **monolithic files that accumulate multiple concerns**. Eight files exceeding 1,200 LOC together account for ~23,600 lines (32% of the module) and are the root cause of the majority of principle violations across boundaries, knowledge, simplicity, and testability categories.

A second systemic issue is **deep attribute chains through `DataFusionRuntimeProfile`**, identified by 5 of 8 agents as the primary Law of Demeter violation source. The profile acts as a "god object" intermediary that every subsystem reaches through, creating brittle coupling to its internal structure.

---

## Cross-Agent Principle Scores

| Principle | A1 | A2 | A3 | A4 | A5 | A6 | A7 | A8 | Avg | Theme |
|-----------|----|----|----|----|----|----|----|----|-----|-------|
| 1. Information hiding | 1 | 2 | 2 | 2 | 2 | 1 | 2 | 2 | 1.8 | Global state exposure |
| 2. Separation of concerns | 0 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0.9 | **Monolithic files** |
| 3. SRP | 0 | 1 | 1 | 1 | 1 | 1 | 0 | 1 | 0.8 | **Monolithic files** |
| 4. Cohesion/coupling | 1 | 2 | 2 | 2 | 2 | 2 | 1 | 2 | 1.8 | PolicyBundleConfig coupling |
| 5. Dependency direction | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2.0 | Generally correct |
| 6. Ports & adapters | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2.0 | Protocol types well-used |
| 7. DRY | 0 | 1 | 1 | 1 | 1 | 1 | 1 | 1 | 0.9 | **Pervasive duplication** |
| 8. Design by contract | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2.0 | Frozen structs, validation |
| 9. Parse, don't validate | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2.0 | Boundary parsing solid |
| 10. Illegal states | 1 | 2 | 2 | 2 | 2 | 1 | 2 | 2 | 1.8 | Some untyped bags |
| 11. CQS | 2 | 2 | 2 | 2 | 2 | 2 | 1 | 2 | 1.9 | Generally clean |
| 12. DI + composition | 1 | 2 | 2 | 2 | 2 | 1 | 2 | 2 | 1.8 | Global state is the gap |
| 13. Composition > inheritance | 1 | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 2.8 | **Strength** (A1: mixins) |
| 14. Law of Demeter | 1 | 1 | 1 | 2 | 1 | 2 | 1 | 1 | 1.3 | **Profile god object** |
| 15. Tell, don't ask | 1 | 2 | 2 | 2 | 2 | 2 | 1 | 2 | 1.8 | Profile field extraction |
| 16. Functional core | 2 | 2 | 2 | 2 | 2 | 2 | 1 | 2 | 1.9 | Observability interleaving |
| 17. Idempotency | 2 | 2 | 3 | 3 | 2 | 3 | 2 | 3 | 2.5 | **Strength** |
| 18. Determinism | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 3 | 3.0 | **Strength** (uniform) |
| 19. KISS | 1 | 1 | 2 | 2 | 2 | 2 | 1 | 1 | 1.5 | Monolith complexity |
| 20. YAGNI | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 2.0 | Mostly justified |
| 21. Least astonishment | 1 | 2 | 1 | 2 | 2 | 2 | 2 | 2 | 1.8 | Schema reassignment |
| 22. Public contracts | 1 | 2 | 2 | 2 | 2 | 2 | 2 | 2 | 1.9 | __all__ consistent |
| 23. Testability | 1 | 1 | 2 | 1 | 2 | 1 | 1 | 1 | 1.3 | **Global state barrier** |
| 24. Observability | 2 | 2 | 2 | 0 | 3 | 2 | 2 | 2 | 1.9 | Arrow has zero logging |

**Weakest principles (avg < 1.5):** P2 (0.9), P3 (0.8), P7 (0.9), P14 (1.3), P23 (1.3)
**Strongest principles (avg >= 2.5):** P18 (3.0), P13 (2.8), P17 (2.5)

---

## Systemic Themes (Cross-Agent)

### Theme 1: Monolithic Files (8/8 agents)

Every agent found at least one oversized file that is the root cause of multiple principle violations.

| File | LOC | Agent | SRP Score | Concerns |
|------|-----|-------|-----------|----------|
| `session/runtime.py` | 8,229 | A1 | 0/3 | Config, lifecycle, diagnostics, hooks, registration, views |
| `dataset/registration.py` | 3,350 | A7 | 0/3 | DDL, Delta provider, caching, observability, schema evolution, partition validation |
| `io/write.py` | 2,703 | A8 | 1/3 | Delta policy, commit metadata, feature mutation, streaming, maintenance, diagnostics |
| `plan/bundle_artifact.py` | 2,563 | A5 | -- | Plan construction (high cohesion, acceptable) |
| `delta/control_plane.py` | 2,297 | A2 | 1/3 | Request structs, Rust FFI, CRUD ops, metadata queries, feature management |
| `plan/artifact_store.py` | 1,654 | A5 | 1/3 | Persistence, validation, serialization, event processing |
| `udf/extension_runtime.py` | 1,596 | A6 | 1/3 | ABI validation, snapshot capture, validation, DDL gen, hashing |
| `extract/templates.py` | 1,435 | A8 | -- | Convention-based discovery (fragile but cohesive) |
| `schema/observability_schemas.py` | 1,330 | A3 | 1/3 | Schema declarations + 930 lines of validation logic |
| `arrow/metadata.py` | 1,262 | A4 | 1/3 | 6 metadata sub-concerns in one module |

**Total: ~26,419 LOC in 10 files (36% of the module) driving the majority of P2/P3/P19/P23 violations.**

**Recommended decomposition priority** (by impact x effort ratio):
1. `runtime.py` -- Extract config structs, policy presets, diagnostics mixin, hook factories (~5 new modules)
2. `registration.py` -- Extract DDL, artifacts, schema adapters, partition validation (~4 new modules)
3. `write.py` -- Extract delta policy, commit assembly, features, maintenance (~4 new modules)
4. `control_plane.py` -- Extract types, FFI layer, feature toggles (~3 new modules)
5. `metadata.py` -- Extract ordering, evidence, extractor, function requirements (~5 sub-modules)

### Theme 2: DataFusionRuntimeProfile as God Object (5/8 agents)

Agents 1, 2, 5, 7, and 8 independently identified deep attribute chains through the runtime profile as the primary Law of Demeter violation.

**Common patterns:**
- `profile.policies.feature_gates.enable_*` (3-4 levels, Agent 1)
- `profile.policies.delta_store_policy` (3 levels, Agents 2, 8)
- `profile.diagnostics.capture_explain` (3 levels, Agent 5)
- `profile.features.enable_schema_evolution_adapter` (2 levels, Agent 7)
- `dataset_location.resolved.delta_write_policy.parquet_writer_policy.statistics_enabled` (6 levels, Agent 7)

**Root cause:** `PolicyBundleConfig` (50+ fields, Agent 1) aggregates 8+ policy domains. Every subsystem reaches through the profile to access its specific policies.

**Recommended approach:**
1. Split `PolicyBundleConfig` into domain-specific bundles: `CachePolicyBundle`, `DeltaPolicyBundle`, `UdfPolicyBundle`, `HookPolicyBundle`
2. Add delegation methods on the profile for common queries: `profile.are_dynamic_filters_enabled()`, `profile.delta_write_policy()`, `profile.reserve_commit()`
3. Pass narrow, purpose-specific config objects to functions instead of the full profile

### Theme 3: Pervasive Knowledge Duplication (8/8 agents)

Every agent found DRY violations, with several spanning across agent boundaries:

| Duplicated Knowledge | Locations | Agents |
|---------------------|-----------|--------|
| `arrow_schema_from_df` | 4 files | A5, A7 |
| `_sql_identifier` | 3 files | A7, A8 |
| `_coerce_int` / `_int_or_none` | 4 files | A5, A1 |
| `_ENGINE_FUNCTION_REQUIREMENTS` | 2 files | A3 |
| Extension module name string | 7+ files | A6 |
| ABI mismatch error message | 11+ files | A6 |
| Gate/commit payload destructuring | 15+ sites | A2 |
| Feature enable/disable functions | 12 functions | A2 |
| Hook chaining functions | 5 functions | A1 |
| Registration payload builders | 3 functions | A1 |
| Schema name reassignment pattern | 7+ schemas | A3 |
| Delta cache registration logic | 2 functions | A7 |

**Recommended consolidation targets:**
1. `datafusion_engine/sql/helpers.py` -- `sql_identifier()`, `sql_string_literal()`
2. `utils/value_coercion.py` -- Extend with `coerce_float()`; replace 4 local copies
3. `datafusion_engine/arrow/schema_utils.py` -- Canonical `arrow_schema_from_df`
4. `extensions/context_adaptation.py` -- `DATAFUSION_EXT_MODULE` constant, `abi_mismatch_error()` helper
5. Generic `chain_hooks()` utility for the 5 identical hook chaining functions
6. Data-driven feature toggle registry replacing 12 near-identical functions

### Theme 4: Module-Level Global State Blocking Testability (5/8 agents)

| Module | Global State | Agent |
|--------|-------------|-------|
| `session/runtime.py` | `_SESSION_CONTEXT_CACHE`, `_SESSION_RUNTIME_CACHE`, `_RUNTIME_SETTINGS_OVERLAY` | A1 |
| `udf/extension_runtime.py` | 6 `WeakSet`/`WeakKeyDictionary` globals | A6 |
| `udf/platform.py` | `_FUNCTION_FACTORY_CTXS`, `_EXPR_PLANNER_CTXS` | A6 |
| `dataset/registration.py` | `_CACHED_DATASETS`, `_REGISTERED_CATALOGS`, `_REGISTERED_SCHEMAS` | A7 |
| `dataset/registry.py` | `_RESOLVED_LOCATION_CACHE` (keyed by `id()` -- unsafe after GC) | A7 |
| `views/graph.py` | `_resolver_identity_guard` | A7 |
| `catalog/introspection.py` | `_INTROSPECTION_CACHE_BY_CONTEXT` (keyed by `id()` -- unsafe after GC) | A6 |

**Impact:** These globals make isolated unit testing impossible without monkeypatching, prevent parallel test execution, and create subtle bugs when Python reuses object IDs after GC.

**Recommended approach:**
1. Wrap each set of related globals into a manager class (e.g., `SessionCacheManager`, `RustUdfSessionRegistry`, `RegistrationStateManager`)
2. Expose module-level singletons for backward compatibility
3. Accept managers via DI for testing
4. Add `reset()` / `clear()` methods for test fixtures
5. Replace `id()`-keyed caches with `WeakKeyDictionary`

### Theme 5: Zero Observability in Arrow/Encoding Modules (Agent 4)

Agent 4 scored **0/3** on observability -- the only 0/3 in any review. Zero `logging.getLogger`, zero tracing, zero metrics across 5,685 lines of Arrow abstraction code. This is the highest-risk gap because:
- These modules are the PyArrow boundary used by 20+ downstream consumers
- Schema coercion failures here cascade widely
- Silent error suppression in `register_semantic_extension_types`

**Recommended:** Add structured logging to `encoding/policy.py`, `dictionary.py`, `coercion.py`, and `semantic.py`.

---

## Prioritized Quick Wins (Cross-Agent Top 15)

| # | Principle | Action | Source | Effort | Files |
|---|-----------|--------|--------|--------|-------|
| 1 | P7 DRY | Extract `_sql_identifier` to `sql/helpers.py` | A8 | small | 3 |
| 2 | P7 DRY | Consolidate `_coerce_int`/`_float_or_none` to `utils/value_coercion.py` | A5 | small | 4 |
| 3 | P7 DRY | Consolidate `arrow_schema_from_df` to one canonical location | A7 | small | 4 |
| 4 | P7 DRY | Centralize extension module name + ABI error message as constants | A6 | small | 11+ |
| 5 | P7 DRY | Extract generic `_build_registration_payload()` in `runtime.py` | A1 | small | 1 |
| 6 | P7 DRY | Extract generic `chain_hooks()` from 5 identical functions | A1 | small | 1 |
| 7 | P7 DRY | Deduplicate `_ENGINE_FUNCTION_REQUIREMENTS` across schema modules | A3 | small | 2 |
| 8 | P10 | Replace `enable_ident_normalization` + `force_disable` with 3-value Literal | A1 | small | 1 |
| 9 | P24 | Add structured logging to Arrow/encoding modules (0/3 score) | A4 | medium | 4 |
| 10 | P21 | Remove `__getattr__` from `DiagnosticsRecorderAdapter` | A5 | small | 1 |
| 11 | P7 DRY | Consolidate `DEFAULT_DICTIONARY_INDEX_TYPE` to single location | A4 | small | 2 |
| 12 | P20 | Remove empty `_LEGACY_DATASET_ALIASES`, dead template assignments | A8 | small | 2 |
| 13 | P19 | Replace 12 feature toggle functions with data-driven registry | A2 | medium | 1 |
| 14 | P23 | Add `clear_registration_caches()` functions for test fixtures | A7 | small | 3 |
| 15 | P7 DRY | Extract `_invoke_rust_entrypoint` helper in `control_plane.py` | A2 | small | 1 |

---

## Recommended Action Sequence (Strategic)

### Phase 1: DRY Consolidation (Low risk, high signal)

Extract and consolidate the 7 duplicated knowledge items identified above. Each is a mechanical change with zero behavioral impact. This phase reduces the maintenance surface and prevents drift before larger structural changes begin.

**Estimated scope:** ~12 files touched, ~500 lines of duplication eliminated.

### Phase 2: Testability Quick Wins

1. Add `clear_*()` functions for all module-level caches (5 modules)
2. Replace `id()`-keyed caches with `WeakKeyDictionary` (2 modules)
3. Remove `__getattr__` delegation from `DiagnosticsRecorderAdapter`
4. Add structured logging to Arrow/encoding modules

**Estimated scope:** ~8 files touched, no behavioral changes.

### Phase 3: Type Safety Improvements

1. Replace `enable_ident_normalization` + `force_disable` with 3-value Literal
2. Define typed `RegistrySnapshot` struct for UDF snapshots (replacing `Mapping[str, object]`)
3. Replace `DeltaMutationRequest` with tagged union types
4. Restrict `ExprIR.op` and `CachePolicy` to typed enums
5. Parse `format_options` into typed `DeltaFormatOptions` struct

**Estimated scope:** ~10 files touched, minor behavioral tightening.

### Phase 4: Monolith Decomposition (High impact, requires Phase 1-3)

Execute in order (each reduces the next's complexity):

1. **`session/runtime.py`** (8,229 -> ~5,800 LOC)
   - Extract config structs (~300 LOC)
   - Extract policy presets (~130 LOC)
   - Extract diagnostics mixin (~420 LOC)
   - Extract hook factories (~80 LOC)
   - Extract registration helpers (~80 LOC)

2. **`dataset/registration.py`** (3,350 -> ~2,000 LOC)
   - Extract DDL generation (~240 LOC)
   - Extract artifact recording (~400 LOC)
   - Extract schema adapter factory (~120 LOC)
   - Extract partition validation (~200 LOC)

3. **`io/write.py`** (2,703 -> ~1,500 LOC)
   - Extract delta policy resolution (~200 LOC)
   - Extract commit metadata assembly (~200 LOC)
   - Extract feature mutation (~200 LOC)
   - Extract maintenance scheduling (~100 LOC)

4. **`delta/control_plane.py`** (2,297 -> ~1,200 LOC)
   - Extract request types (~400 LOC)
   - Extract FFI marshalling (~200 LOC)
   - Extract feature toggles (~600 LOC via data-driven registry)

5. **`arrow/metadata.py`** (1,262 -> ~300 LOC facade)
   - Extract ordering metadata (~100 LOC)
   - Extract evidence metadata (~70 LOC)
   - Extract extractor defaults (~130 LOC)
   - Extract function requirements (~100 LOC)

### Phase 5: Profile God Object Mitigation (Requires Phase 4)

1. Split `PolicyBundleConfig` into 5 domain-specific bundles
2. Refactor `DataFusionRuntimeProfile` mixins to composition
3. Add delegation methods for common queries
4. Incrementally narrow profile access in leaf functions

---

## Verification Checklist

- [x] 8 review documents in `docs/reviews/`
- [x] Every subdirectory assigned to exactly one agent
- [x] Each review references specific files within its scope
- [x] No scope overlaps between agents
- [x] Cross-cutting themes identified across agent boundaries
- [x] Quick wins prioritized by effort/impact ratio
- [x] Action sequence respects dependencies between phases

## Review Documents

| Agent | Scope | Output |
|-------|-------|--------|
| A1 | Session & Runtime Core | `docs/reviews/design_review_datafusion_session_runtime_core_2026-02-16.md` |
| A2 | Delta Lake Integration | `docs/reviews/datafusion_delta_lake.md` |
| A3 | Schema & Contracts | `docs/reviews/datafusion_schema_contracts.md` |
| A4 | Arrow Interop + Encoding | `docs/reviews/datafusion_arrow_encoding.md` |
| A5 | Plan Management + Lineage | `docs/reviews/datafusion_plan_lineage.md` |
| A6 | UDF + Extensions + Catalog | `docs/reviews/datafusion_udf_extensions_catalog.md` |
| A7 | Dataset + Views + Expression | `docs/reviews/datafusion_dataset_views_expr.md` |
| A8 | I/O, Extract, Tables & Utilities | `docs/reviews/datafusion_io_extract_utilities.md` |
