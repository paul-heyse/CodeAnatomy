# Design Review: src/utils

**Date:** 2026-02-17
**Scope:** `src/utils/` (shared utilities) + `src/cache/` (disk cache) + `src/arrow_utils/` (Arrow helpers)
**Focus:** All principles (1-24), with emphasis on boundaries (1-6), simplicity (19-22), quality (23-24)
**Depth:** moderate
**Files reviewed:** 18

## Executive Summary

The `src/utils/` package is one of the cleanest inner-ring modules in the codebase, providing focused utility functions for hashing, environment parsing, UUID generation, validation, and registry protocols. Most files have zero external dependencies beyond stdlib. The single exception is `utils/value_coercion.py` which imports from `datafusion_engine.arrow.interop`, creating a dependency direction violation. The `cache/` module provides a well-structured disk cache factory with profile-based configuration. The `arrow_utils/` module is minimal and cleanly positioned as inner ring with pure constant definitions and data types.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 3 | - | - | Internal helpers well-hidden; public API via `__all__` |
| 2 | Separation of concerns | 3 | - | - | Each file addresses one concern; clean module boundaries |
| 3 | SRP | 3 | - | - | `hashing.py` = hashing, `env_utils.py` = env parsing, etc. |
| 4 | High cohesion, low coupling | 2 | small | low | One file (`value_coercion.py`) couples to engine ring |
| 5 | Dependency direction | 2 | small | medium | `value_coercion.py` imports from `datafusion_engine` |
| 6 | Ports & Adapters | 3 | - | - | `Registry` protocol is a clean port definition |
| 7 | DRY | 3 | - | - | Single authority for hashing, UUIDs, env parsing |
| 8 | Design by contract | 2 | small | low | `ensure_*` functions enforce preconditions; `validate_*` functions enforce invariants |
| 9 | Parse, don't validate | 3 | - | - | `env_utils.py` parses env vars to typed values at boundary |
| 10 | Make illegal states unrepresentable | 2 | small | low | `ImmutableRegistry` is frozen; `UUID7_HEX_LENGTH` is `Final` |
| 11 | CQS | 3 | - | - | Hash functions are pure queries; registry `register()` is command |
| 12 | DI + explicit composition | 3 | - | - | `MutableRegistry` composed via DI; no singletons |
| 13 | Composition over inheritance | 3 | - | - | No inheritance; `MappingRegistryAdapter` uses composition |
| 14 | Law of Demeter | 3 | - | - | Direct collaborator access only |
| 15 | Tell, don't ask | 3 | - | - | `ensure_*` validates and returns; `coerce_*` transforms |
| 16 | Functional core, imperative shell | 3 | - | - | All functions are pure or have explicit side effects |
| 17 | Idempotency | 3 | - | - | Hash functions idempotent; coercion functions idempotent |
| 18 | Determinism | 3 | - | - | All hash/parse/coerce functions are deterministic |
| 19 | KISS | 3 | - | - | Simple, focused modules; no unnecessary complexity |
| 20 | YAGNI | 2 | small | low | `MappingRegistryAdapter` may overlap with `MutableRegistry` |
| 21 | Least astonishment | 3 | - | - | Function names match behavior; overloaded type signatures |
| 22 | Public contracts | 3 | - | - | `__all__` defined; `Registry` protocol is explicit contract |
| 23 | Testability | 3 | - | - | Pure functions; no global state; injectable dependencies |
| 24 | Observability | 3 | - | - | N/A for utility module; hash functions provide traceability |

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding -- Alignment: 3/3

**Current state:**
Each utility module exposes a well-defined public API via `__all__`. Helper functions use `_` prefix convention.

**Findings:**
- `src/utils/hashing.py` exports 12 public functions, all documented and typed
- `src/utils/env_utils.py` exports 9 public functions with proper overloaded type signatures
- `src/utils/validation.py` exports 8 validation helpers
- `src/utils/__init__.py` re-exports only `schema_from_struct` and `uuid_factory` -- selective public surface

**Suggested improvement:**
No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Nearly all `utils/` files depend only on stdlib. One file violates inner-ring constraints.

**Findings:**
- `src/utils/value_coercion.py:10-13` imports `RecordBatchReader` and `RecordBatchReaderLike` from `datafusion_engine.arrow.interop` -- this is an inner-ring module importing from engine ring
- `src/utils/value_coercion.py:8` also imports `pyarrow` directly, which is acceptable for a utility that operates on Arrow types
- All other `utils/` files (`hashing.py`, `env_utils.py`, `validation.py`, `uuid_factory.py`, `storage_options.py`, `schema_from_struct.py`, `registry_protocol.py`) have zero engine-ring dependencies -- clean inner ring

**Suggested improvement:**
Move `value_coercion.py` to `datafusion_engine/utils/` or create a `utils/arrow/` sub-package that is explicitly documented as engine-ring. Alternatively, the `coerce_to_recordbatch_reader()` function could be moved to `datafusion_engine/arrow/coercion.py` since it is the only function that requires engine imports.

**Effort:** small
**Risk if unaddressed:** medium -- Inner-ring consumers that import `utils.value_coercion` will unknowingly pull in the engine ring.

---

#### P6. Ports & Adapters -- Alignment: 3/3

**Current state:**
`registry_protocol.py` defines clean port abstractions via protocols.

**Findings:**
- `src/utils/registry_protocol.py:20-41` `Registry[K, V]` is a `runtime_checkable` protocol defining the canonical registry port: `register()`, `get()`, `__contains__()`, `__iter__()`, `__len__()`
- `src/utils/registry_protocol.py:44-54` `SnapshotRegistry[K, V]` extends with `snapshot()`/`restore()` capabilities
- `MutableRegistry`, `ImmutableRegistry`, and `MappingRegistryAdapter` are concrete adapters implementing these ports
- This is textbook Ports & Adapters

**Suggested improvement:**
No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Knowledge

#### P7. DRY -- Alignment: 3/3

**Current state:**
Each utility module is the single authority for its domain.

**Findings:**
- `src/utils/hashing.py` is the single authority for all hashing operations. The CLAUDE.md documents this: "Explicit, semantics-preserving hash helpers (msgpack, JSON, storage options)"
- `src/utils/env_utils.py` is the single authority for environment variable parsing
- `src/utils/uuid_factory.py` is the single authority for UUID generation
- `src/utils/storage_options.py` is the single authority for storage option normalization
- `src/utils/registry_protocol.py` is the single authority for registry patterns

**Suggested improvement:**
No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P9. Parse, don't validate -- Alignment: 3/3

**Current state:**
`env_utils.py` is a model implementation of parse-don't-validate. Raw string environment variables are parsed once into typed values at the boundary.

**Findings:**
- `src/utils/env_utils.py` provides `env_value()`, `env_bool()`, `env_int()`, `env_float()`, `env_enum()`, `env_list()`, `env_text()` that convert `os.environ` string values to typed Python values
- Functions return `T | None` when parsing fails, allowing callers to handle missing/invalid values
- `env_bool_strict()` variant raises on invalid values for cases where failure should be explicit
- Proper `@overload` signatures provide type-safe defaults

**Suggested improvement:**
No action needed. This is exemplary boundary parsing.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Composition

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies in `utils/`. All behavior is composed via standalone functions and dataclasses.

**Findings:**
- `MutableRegistry` is a dataclass with dict storage -- composition, not inheritance
- `ImmutableRegistry` is a frozen dataclass with tuple storage -- composition
- `MappingRegistryAdapter` wraps a dict with configurable read-only/overwrite behavior -- composition
- `CacheKeyBuilder` in `hashing.py` uses builder pattern -- composition

**Suggested improvement:**
No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Simplicity

#### P19. KISS -- Alignment: 3/3

**Current state:**
Utility modules are focused and minimal. No unnecessary abstractions.

**Findings:**
- `src/utils/hashing.py` (~316 LOC) provides exactly the hashing functions documented in CLAUDE.md
- `src/utils/env_utils.py` (~387 LOC) provides exactly the env parsing functions needed
- `src/utils/validation.py` (~228 LOC) provides focused validation helpers
- `src/utils/storage_options.py` (~73 LOC) is minimal and focused
- `src/utils/uuid_factory.py` (~178 LOC) handles UUID generation with thread safety and fallback

**Suggested improvement:**
No action needed.

**Effort:** -
**Risk if unaddressed:** -

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most utilities are actively used. `MappingRegistryAdapter` may overlap with `MutableRegistry`.

**Findings:**
- `src/utils/registry_protocol.py:236-335` `MappingRegistryAdapter` provides `read_only` and `allow_overwrite` configuration options, similar to `MutableRegistry` which has `overwrite` parameter
- Both classes serve the same basic purpose (mutable key-value registry) with slightly different APIs
- `src/utils/coercion.py` exists alongside `src/utils/value_coercion.py` -- potential overlap

**Suggested improvement:**
Audit whether `MappingRegistryAdapter` has unique callers that cannot use `MutableRegistry`. If not, consolidate. Also verify `coercion.py` vs `value_coercion.py` for overlap.

**Effort:** small
**Risk if unaddressed:** low

---

### Cache Module

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
`cache/__init__.py` re-exports all public symbols from `diskcache_factory.py`. The module-level `_CACHE_POOL` dict manages singleton cache instances.

**Findings:**
- `src/cache/__init__.py:1-33` re-exports 13 symbols from `cache.diskcache_factory`
- `src/cache/diskcache_factory.py` uses `_CACHE_POOL: dict[str, DiskCache]` at module level for singleton cache management
- The `cache_for_kind()` function both creates and caches `DiskCache` instances -- singleton factory pattern

**Suggested improvement:**
Document the singleton pattern explicitly. Consider providing a `clear_cache_pool()` function for test isolation rather than requiring tests to know about `_CACHE_POOL`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
`cache_for_kind()` both returns a cache instance (query) and potentially creates/caches it (command). This is a justified CQS violation for a factory-cache pattern.

**Findings:**
- `src/cache/diskcache_factory.py` `cache_for_kind()` returns a `DiskCache` but also mutates `_CACHE_POOL` on first call -- factory pattern justifies CQS exception
- `bulk_cache_set()` is a proper command
- `diskcache_stats_snapshot()` is a proper query

**Suggested improvement:**
No action needed. The factory-cache pattern is a standard justified CQS exception.

**Effort:** -
**Risk if unaddressed:** -

---

#### P23. Testability -- Alignment: 2/3

**Current state:**
The module-level `_CACHE_POOL` singleton makes test isolation require manual cleanup.

**Findings:**
- Tests must manage the `_CACHE_POOL` state to avoid cross-test contamination
- `DiskCacheProfile` and `DiskCacheSettings` are testable as pure data (frozen structs)
- `default_diskcache_profile()` is a pure factory

**Suggested improvement:**
Add a `reset_cache_pool()` function for test teardown. Consider making the cache pool injectable rather than module-level global.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Exemplary Inner-Ring Design

**Root cause:** `utils/` was designed from the start as a foundational layer with no outward dependencies. The CLAUDE.md documents each module's purpose, creating accountability.

**Affected principles:** P1-P6, P7, P9, P11, P13, P19 -- all positively

**Approach:** Use `utils/` as the reference architecture for other inner-ring modules. When refactoring `obs/` or `schema_spec/`, measure success against the `utils/` standard.

### Theme 2: Single Dependency Direction Violation

**Root cause:** `value_coercion.py` was placed in `utils/` for convenience but requires `datafusion_engine` Arrow types for RecordBatch coercion.

**Affected principles:** P5 (dependency direction)

**Approach:** Move `coerce_to_recordbatch_reader()` to `datafusion_engine/` where it belongs. Keep the primitive coercion functions (`coerce_int`, `coerce_float`, etc.) in `utils/value_coercion.py`.

### Theme 3: Registry Protocol Quality

**Root cause:** `registry_protocol.py` was designed as a reusable protocol with multiple adapter implementations.

**Affected principles:** P6, P13 -- positively

**Approach:** Promote the registry protocol pattern to other modules. Consider whether `SnapshotRegistry` is used or if it can be simplified.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P5 Dependency | Move `coerce_to_recordbatch_reader()` out of `utils/value_coercion.py` | small | Fixes the only dependency violation in utils |
| 2 | P20 YAGNI | Audit `MappingRegistryAdapter` vs `MutableRegistry` overlap | small | Reduce API surface if redundant |
| 3 | P23 Testability | Add `reset_cache_pool()` to `cache/diskcache_factory.py` | small | Better test isolation |
| 4 | P20 YAGNI | Audit `utils/coercion.py` vs `utils/value_coercion.py` overlap | small | Consolidate if redundant |
| 5 | P1 Info hiding | Document `_CACHE_POOL` singleton pattern in cache module | small | Prevents misuse |

## Recommended Action Sequence

1. **Move RecordBatch coercion** (P5) -- Extract `coerce_to_recordbatch_reader()` from `utils/value_coercion.py` to `datafusion_engine/arrow/coercion.py`. Update all callers. This makes `utils/` a truly dependency-free inner ring.

2. **Audit registry implementations** (P20) -- Check if `MappingRegistryAdapter` and `MutableRegistry` serve distinct callers. If not, consolidate into `MutableRegistry` with `read_only` option.

3. **Audit coercion modules** (P20) -- Compare `utils/coercion.py` and `utils/value_coercion.py` for functional overlap. Consolidate if they cover the same domain.

4. **Add cache pool reset** (P23) -- Add `reset_cache_pool()` function to `cache/diskcache_factory.py` for test isolation.

5. **Maintain inner-ring purity** (P5) -- Establish a CI check that verifies `utils/` files (excluding any explicitly documented exceptions) have no imports from `datafusion_engine`, `relspec`, `semantics`, `storage`, or `extraction`.
