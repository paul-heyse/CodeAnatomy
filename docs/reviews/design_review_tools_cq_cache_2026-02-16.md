# Design Review: tools/cq/core/cache/

**Date:** 2026-02-16
**Scope:** `tools/cq/core/cache/`
**Focus:** Boundaries (1-6), Composition (12-15), Correctness (16-18), Quality (23-24)
**Depth:** deep
**Files reviewed:** 25 (3,902 LOC total)

## Executive Summary

The CQ cache subsystem is well-structured overall, with a clean protocol-based abstraction (`CqCacheBackend`), consistent fail-open error handling, and strong msgspec-based serialization contracts. The primary weaknesses are: (a) several modules bypass the `CqCacheBackend` protocol by reaching through to the underlying `FanoutCache` via `getattr(backend, "cache", ...)`, which violates information hiding and couples callers to diskcache internals; (b) duplicated decode/persist patterns across `tree_sitter_cache_store.py` and `search_artifact_store.py`; and (c) environment variable parsing logic that duplicates the existing `env_bool` utility. The coordination module's triple-lock acquisition pattern (`semaphore + lock + rlock`) has a potential resource leak on partial acquisition failure.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | high | 3 modules bypass CqCacheBackend to access raw FanoutCache internals |
| 2 | Separation of concerns | 2 | small | low | Policy resolution mixes env parsing and domain logic |
| 3 | SRP | 2 | small | low | `diskcache_backend.py` handles adapter, singleton lifecycle, and factory |
| 4 | High cohesion, low coupling | 2 | medium | medium | Stores tightly coupled to diskcache-specific APIs (Deque, Index, read) |
| 5 | Dependency direction | 2 | small | low | Core contracts are clean; stores depend inward but reach through protocol |
| 6 | Ports & Adapters | 1 | medium | high | Protocol defined but routinely circumvented for advanced ops |
| 12 | DI + explicit composition | 2 | medium | medium | Module-level singletons and `default_cache_policy()` scattered across callers |
| 13 | Prefer composition over inheritance | 3 | - | - | No inheritance; function bundles and dataclasses throughout |
| 14 | Law of Demeter | 1 | medium | high | `getattr(backend, "cache", None)` chains are pervasive |
| 15 | Tell, don't ask | 2 | small | low | Callers probe backend attributes before deciding behavior |
| 16 | Functional core, imperative shell | 2 | small | low | Fragment engine is pleasantly pure; stores mix IO |
| 17 | Idempotency | 3 | - | - | All writes use content-addressed keys; re-writes are safe |
| 18 | Determinism / reproducibility | 3 | - | - | SHA256-based keys, sorted file signatures, canonical payloads |
| 23 | Design for testability | 2 | medium | medium | Module-level singletons and `default_cache_policy()` calls hinder isolation |
| 24 | Observability | 3 | - | - | Rich namespace-scoped telemetry with counters, key cardinality tracking |

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
The `CqCacheBackend` protocol in `interface.py` defines a clean 14-method contract. However, three modules routinely bypass this protocol to access the underlying `FanoutCache` object directly.

**Findings:**
- `tools/cq/core/cache/coordination.py:24` -- `_fanout_cache()` reaches into `getattr(backend, "cache", None)` to extract the raw diskcache handle, then uses it to construct `BoundedSemaphore`, `Lock`, `RLock`, and `barrier` primitives directly. This means any alternative backend implementation must also expose a `.cache` attribute compatible with diskcache's API, defeating the protocol's purpose.
- `tools/cq/core/cache/tree_sitter_blob_store.py:61` -- `_cache_object()` does the same `getattr(backend, "cache", None)` extraction to use `cache.set(..., read=True)` and `cache.read(...)` (the streaming read API), plus `cache.create_tag_index()` and `cache.drop_tag_index()`. These are diskcache-specific operations not represented in `CqCacheBackend`.
- `tools/cq/core/cache/search_artifact_store.py:300` -- `_touch_cached_entry()` bypasses the backend protocol and calls `cache.touch(...)` directly on the extracted FanoutCache. The irony is that `CqCacheBackend` already declares `touch()` as a method, making this bypass unnecessary.
- `tools/cq/core/cache/cache_runtime_tuning.py:63-83` -- `apply_cache_runtime_tuning()` takes a raw `cache: object` and uses `getattr(cache, "reset", None)` plus `getattr(cache, "create_tag_index", None)` to poke into diskcache internals.

**Suggested improvement:**
Extend `CqCacheBackend` protocol with optional "capability" methods for the operations that stores actually need: `read_streaming(key) -> bytes | None`, `set_streaming(key, payload, ...) -> bool`, and `create_tag_index() -> None`. These should have no-op defaults in the protocol. The `coordination.py` primitives should accept `CqCacheBackend` and check for coordination capabilities via a `supports_coordination: bool` property rather than extracting the raw cache object. The existing `search_artifact_store.py:300` bypass should just call `backend.touch(cache_key, expire=ttl_seconds)`.

**Effort:** medium
**Risk if unaddressed:** high -- Any attempt to swap or mock the cache backend (e.g., for Redis, in-memory, or tests) silently fails because the bypasses access attributes that only `DiskcacheBackend` exposes.

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
Policy resolution in `policy.py` handles both domain-level defaults and environment variable parsing within a single function. The `_resolve_cache_scalar_settings` function (lines 136-218) interleaves 13 `os.getenv` calls with `getattr(runtime, ...)` fallback chains.

**Findings:**
- `tools/cq/core/cache/policy.py:136-218` -- `_resolve_cache_scalar_settings()` mixes three concerns: (a) reading env vars, (b) applying defaults from `runtime`, and (c) coercing/clamping values. Each scalar follows the same pattern but is expanded inline.
- `tools/cq/core/cache/cache_runtime_tuning.py:19-60` -- `resolve_cache_runtime_tuning()` duplicates the same env-override-or-policy-default pattern for 7 settings, with its own inline boolean parsing at lines 46-51 instead of using the `env_bool` utility imported by `policy.py`.

**Suggested improvement:**
Extract a small helper `_env_or_policy(env_key, policy_field, minimum, coerce_fn)` to eliminate the repetitive pattern. This would reduce `_resolve_cache_scalar_settings` from ~80 lines to ~25 while keeping the resolution logic readable.

**Effort:** small
**Risk if unaddressed:** low -- The current code is correct but repetitive; the main risk is drift between the two env-resolution paths.

---

#### P3. SRP (one reason to change) -- Alignment: 2/3

**Current state:**
`diskcache_backend.py` (522 LOC) handles three distinct responsibilities: the `DiskcacheBackend` adapter class (lines 76-398), the process-global singleton lifecycle (`_BackendState`, `_BACKEND_LOCK`, `get_cq_cache_backend`, `set_cq_cache_backend`, `close_cq_cache_backend` at lines 400-514), and the factory function `_build_diskcache_backend` (lines 434-466).

**Findings:**
- `tools/cq/core/cache/diskcache_backend.py:400-514` -- The singleton lifecycle code (registering `atexit`, managing `_BACKEND_STATE`, collecting stale backends) is a separate concern from the adapter implementation. It changes for different reasons (e.g., changing backend scoping strategy vs. changing how timeouts are handled).
- `tools/cq/core/cache/diskcache_backend.py:434-466` -- The factory function `_build_diskcache_backend` couples FanoutCache construction parameters to the policy struct and runtime tuning. This is construction logic, not adapter logic.

**Suggested improvement:**
Extract the singleton lifecycle management (lines 400-514) into a dedicated `backend_lifecycle.py` module, keeping `diskcache_backend.py` focused on the adapter implementation. The factory function could remain with the lifecycle or be separated into a builder module.

**Effort:** small
**Risk if unaddressed:** low -- The file is manageable at 522 LOC but mixes responsibilities that change independently.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The fragment layer (`fragment_contracts.py`, `fragment_engine.py`, `fragment_orchestrator.py`, `fragment_codecs.py`) is highly cohesive and well-decomposed. The store modules (`search_artifact_store.py`, `tree_sitter_cache_store.py`, `tree_sitter_blob_store.py`) are cohesive individually but coupled to diskcache-specific concepts.

**Findings:**
- `tools/cq/core/cache/search_artifact_store.py:34-38` -- Uses `diskcache.Deque` and `diskcache.Index` directly, coupling the artifact index implementation to diskcache's persistent data structures. These are not abstracted through `CqCacheBackend`.
- `tools/cq/core/cache/tree_sitter_blob_store.py:88-125` -- `_persist_to_cache()` uses the raw `cache.set(..., read=True)` streaming API with a `NamedTemporaryFile`, which is a diskcache-specific optimization not available through the protocol.
- The coupling between `search_artifact_store.py` and `tree_sitter_blob_store.py` is tight: the search store depends on blob store for large payloads (lines 23-27, 189-191), which is reasonable but means blob storage is shared infrastructure rather than tree-sitter-specific despite the name.

**Suggested improvement:**
Rename `tree_sitter_blob_store.py` to `blob_store.py` since it serves search artifacts too. The `Deque`/`Index` usage could be abstracted behind a protocol for the artifact index, though this is lower priority since diskcache is the only planned backend.

**Effort:** medium
**Risk if unaddressed:** medium -- The diskcache-specific coupling makes it harder to swap backends and creates confusion about the blob store's intended scope.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Contract types in `base_contracts.py` and `fragment_contracts.py` have zero outward dependencies (they depend only on `CqStruct`), which is correct. The interface protocol in `interface.py` is similarly clean. Domain logic flows inward.

**Findings:**
- `tools/cq/core/cache/snapshot_fingerprint.py:16` -- Imports `get_cq_cache_backend` (a concrete factory) rather than accepting a `CqCacheBackend` as a parameter. The `build_scope_snapshot_fingerprint` function (line 247) calls this internally, making it a mixed domain-and-infrastructure function.
- The same pattern appears in `diagnostics.py:8`, `run_lifecycle.py:7`, `search_artifact_store.py:13`, `tree_sitter_cache_store.py:11`, and `tree_sitter_blob_store.py:13` -- all directly import `get_cq_cache_backend` from the concrete backend module rather than accepting the backend as a parameter.

**Suggested improvement:**
For the public API functions in these modules, accept `CqCacheBackend` as a parameter (injected by the caller) instead of calling `get_cq_cache_backend()` internally. This would make the dependency direction explicit and improve testability. The `build_scope_snapshot_fingerprint` function is a good candidate since it is called from `scope_services.py` which already has access to the root path.

**Effort:** small
**Risk if unaddressed:** low -- The current approach works but creates hidden coupling and makes unit testing require monkeypatching the global backend.

---

#### P6. Ports & Adapters -- Alignment: 1/3

**Current state:**
The port (`CqCacheBackend` protocol) and primary adapter (`DiskcacheBackend`) are well-defined. The `NoopCacheBackend` serves as a null adapter. However, the port is incomplete -- it does not cover all operations that modules actually need.

**Findings:**
- The protocol declares 14 methods. But modules use at least 4 additional diskcache-specific operations not in the protocol: `cache.read()` (streaming read), `cache.set(..., read=True)` (streaming write), `cache.reset(...)` (SQLite pragma tuning), and diskcache coordination primitives (`Lock`, `RLock`, `BoundedSemaphore`, `barrier`).
- `tools/cq/core/cache/coordination.py:15-20` -- Imports `Lock`, `RLock`, `BoundedSemaphore`, `barrier` directly from `diskcache`, creating direct adapter-level dependency in what should be infrastructure-neutral coordination logic.
- `tools/cq/core/cache/interface.py:175-338` -- `NoopCacheBackend` duplicates every method signature from the protocol with identical no-op implementations. This is 163 lines of boilerplate that exactly mirrors the protocol defaults. Since the protocol already provides default return values in its method bodies, `NoopCacheBackend` is technically redundant -- any object satisfying the protocol already behaves as a no-op.

**Suggested improvement:**
1. Add streaming read/write methods to the protocol (with no-op defaults) to cover blob store needs.
2. Introduce a `CqCoordinationBackend` protocol (or extend `CqCacheBackend` with optional coordination capabilities) to formally represent the lock/semaphore surface.
3. Consider whether `NoopCacheBackend` can be replaced with a simple class that just satisfies the protocol via inheritance/composition, eliminating the duplicated method bodies.

**Effort:** medium
**Risk if unaddressed:** high -- The incomplete port means the hexagonal boundary is a facade; real backend substitution would require auditing and fixing all bypass sites.

---

### Category: Composition

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
The system uses a process-global singleton pattern (`_BACKEND_STATE`) with `get_cq_cache_backend()` as the composition root. Callers throughout the system call `default_cache_policy(root=root)` independently to resolve policy, creating redundant policy resolution.

**Findings:**
- `default_cache_policy(root=root)` is called 18 times across the codebase (in `tools/cq/`), each time constructing a fresh `CqCachePolicyV1` from environment variables. Since env vars don't change within a process, these repeated resolutions are wasteful.
- `tools/cq/core/cache/search_artifact_store.py:174,228,303` -- Calls `default_cache_policy(root=root)` three times within the same module, in three different functions. Each call reconstructs the full policy from scratch.
- `tools/cq/core/cache/tree_sitter_blob_store.py:96` -- Calls `default_cache_policy(root=root)` inside `_persist_to_cache`, which is already called from functions that could pass the policy down.

**Suggested improvement:**
Cache the resolved policy per workspace (similar to how the backend is cached) or thread it through from the top-level composition site. Alternatively, bundle policy + backend into a "cache session" object that is resolved once per workspace and threaded through.

**Effort:** medium
**Risk if unaddressed:** medium -- Repeated policy resolution is not a correctness issue but creates unnecessary overhead and makes the dependency graph implicit.

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
The subsystem consistently uses composition. The fragment engine uses function bundles (`FragmentProbeRuntimeV1`, `FragmentPersistRuntimeV1`) rather than class hierarchies. Contract types use `msgspec.Struct` inheritance only for serialization mechanics, not behavior polymorphism. No deep inheritance trees exist.

**Findings:** None -- well aligned.

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
Multiple modules reach through the `CqCacheBackend` protocol to access the underlying `FanoutCache` object's methods and then further reach into those objects' APIs.

**Findings:**
- `tools/cq/core/cache/coordination.py:24-33` -- `_fanout_cache(backend)` gets `backend.cache`, then probes `cache.get` and `cache.set` to verify it looks like a FanoutCache. This is `backend -> cache -> get/set` -- two levels of indirection through undeclared interfaces.
- `tools/cq/core/cache/tree_sitter_blob_store.py:88-125` -- `_persist_to_cache()` does `backend -> cache -> set(key, tmp_file, read=True, ...)` -- operating on a file handle passed through the cache's streaming API, which is three levels deep from the caller's perspective.
- `tools/cq/core/cache/search_artifact_store.py:128-139` -- `_transaction_context(store)` does `store -> transact -> retry=True`, probing whether the unknown object supports `transact(retry=True)` or `transact()` with fallback. This is structural duck-typing through multiple levels.

**Suggested improvement:**
These operations should be methods on the `CqCacheBackend` protocol or on a dedicated coordination/blob adapter. The caller should say "persist this blob" to the backend, not "give me your underlying cache so I can use its streaming API."

**Effort:** medium
**Risk if unaddressed:** high -- Changes to diskcache's API (version upgrades, deprecations) would ripple through multiple modules rather than being contained in the adapter.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The fragment engine is a good example of "tell, don't ask" -- callers provide runtime function bundles and the engine drives the workflow. However, several modules probe object capabilities before acting.

**Findings:**
- `tools/cq/core/cache/cache_runtime_tuning.py:65-83` -- `apply_cache_runtime_tuning()` does `getattr(cache, "reset", None)` then `if callable(reset_fn)` before calling. This "ask first" pattern is repeated 6 times in the function for different attributes.
- `tools/cq/core/cache/tree_sitter_blob_store.py:64-71` -- `_ensure_tag_index()` probes for `create_tag_index` capability before calling. Same pattern at line 79 for `drop_tag_index`.

**Suggested improvement:**
Move capability probing into the adapter class itself. `DiskcacheBackend` could expose an `apply_tuning(tuning: CacheRuntimeTuningV1)` method that knows how to apply settings to its owned cache, eliminating the need for external code to probe capabilities.

**Effort:** small
**Risk if unaddressed:** low -- The current pattern works but spreads diskcache knowledge across multiple modules.

---

### Category: Correctness

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The fragment engine (`fragment_engine.py`) is admirably functional: `partition_fragment_entries()` and `persist_fragment_writes()` are pure orchestration functions that take injectable function bundles and return structured results. The key builder (`key_builder.py`) is fully deterministic and side-effect-free.

**Findings:**
- `tools/cq/core/cache/snapshot_fingerprint.py:247-300` -- `build_scope_snapshot_fingerprint()` mixes pure logic (sorting stats, computing digests) with impure operations (stat calls, cache get/set). The pure fingerprinting logic could be separated from the caching layer.
- `tools/cq/core/cache/content_hash.py:25-62` -- `file_content_hash()` mixes IO (file stat, read_bytes) with caching (process-local memo dict) in a single function. The hashing logic is deterministic but entangled with IO.

**Suggested improvement:**
In `snapshot_fingerprint.py`, separate `_compute_scope_fingerprint(stats, language, scope_globs, ...) -> ScopeSnapshotFingerprintV1` (pure) from the cache-check-and-persist wrapper. This would make the fingerprinting logic testable without any cache involvement.

**Effort:** small
**Risk if unaddressed:** low -- The mixed functions are well-contained and the fail-open semantics mean cache failures don't affect correctness.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All cache keys are content-addressed via SHA256 digests of canonicalized payloads. Re-running the same operation with the same inputs produces the same key and overwrites with the same value. The `add()` method (write-if-absent) is available for cases where atomic insert semantics are needed.

**Findings:** None -- well aligned. The `canonicalize_cache_payload()` function in `key_builder.py:61-70` ensures deterministic key generation regardless of dict ordering.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Cache keys are generated from deterministically sorted and canonicalized payloads (`canonicalize_payload` from `tools/cq/core/id`). Scope snapshots use sorted file stats. Content hashes use SHA256. Blob IDs use BLAKE2b. The entire key generation pipeline is reproducible.

**Findings:** None -- well aligned.

---

### Category: Quality

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
The fragment engine layer is highly testable due to its function-bundle design. The protocol/no-op backend pattern supports basic test substitution. However, the pervasive use of `get_cq_cache_backend()` and `default_cache_policy()` as module-level singletons makes isolated unit testing harder.

**Findings:**
- `tools/cq/core/cache/snapshot_fingerprint.py:267` -- `build_scope_snapshot_fingerprint()` internally calls `get_cq_cache_backend()`, meaning tests must either monkeypatch the global backend or use `set_cq_cache_backend()` as a test fixture, which leaks state between tests if cleanup is missed.
- `tools/cq/core/cache/diskcache_backend.py:401-411` -- The `_BackendState` singleton holds a mutable dict that persists across the process. Tests must carefully clean this up.
- `tools/cq/core/cache/content_hash.py:21-22` -- Process-local `_CONTENT_HASH_CACHE` dict persists across tests; `reset_file_content_hash_cache()` exists but must be called explicitly.
- `tools/cq/core/cache/telemetry.py:36-37` -- `_TELEMETRY` and `_SEEN_KEYS` are module-level mutable dicts; `reset_cache_telemetry()` exists for cleanup.

The existence of explicit `reset_*()` functions demonstrates awareness of the testability concern, but the design requires every test to remember to call them.

**Suggested improvement:**
For the highest-impact improvement, make `build_scope_snapshot_fingerprint()` and similar public functions accept an optional `backend: CqCacheBackend | None = None` parameter that defaults to calling `get_cq_cache_backend()`. This allows tests to inject a `NoopCacheBackend()` without touching global state.

**Effort:** medium
**Risk if unaddressed:** medium -- Test flakiness from shared mutable state between test cases, especially under parallel test execution.

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
The telemetry module (`telemetry.py`) provides comprehensive namespace-scoped counters covering gets, hits, misses, sets, failures, evictions, decode failures, timeouts, aborts, volume, cull operations, and key cardinality/size distribution. Every cache operation in the subsystem records telemetry.

**Findings:**
- `tools/cq/core/cache/telemetry.py:10-32` -- `CacheNamespaceTelemetry` captures 20 distinct metrics per namespace. This is thorough and well-structured.
- `tools/cq/core/cache/diskcache_backend.py:113-120` -- Timeout and abort events are recorded via dedicated functions that map to distinct telemetry buckets, enabling fine-grained operational diagnosis.
- `tools/cq/core/cache/diagnostics.py:13-51` -- `_namespace_metrics_payload()` computes hit/miss ratios and aggregates all namespace metrics into a structured diagnostics payload.

The observability layer is well-designed and covers all meaningful operational signals.

---

## Cross-Cutting Themes

### Theme 1: Protocol Bypass Pattern

**Root cause:** The `CqCacheBackend` protocol was designed for basic CRUD operations but the subsystem grew to require streaming read/write, coordination primitives (locks, semaphores, barriers), SQLite tuning, and persistent data structures (Deque, Index). Rather than extending the protocol, modules reached through to the underlying implementation.

**Affected principles:** P1 (Information hiding), P6 (Ports & Adapters), P14 (Law of Demeter)

**Suggested approach:** Extend the protocol in two tiers:
1. Add optional methods with no-op defaults for streaming blob operations and TTL-refresh (the `touch()` bypass in `search_artifact_store.py` is the simplest fix since the method already exists on the protocol).
2. Define a separate `CqCacheCoordinationBackend` protocol for lock/semaphore/barrier operations, keeping basic cache users decoupled from coordination concerns.

### Theme 2: Duplicated Decode Patterns

**Root cause:** Both `_decode_tree_sitter_payload` (`tree_sitter_cache_store.py:115-136`) and `_decode_bundle_payload` (`search_artifact_store.py:311-332`) follow the identical pattern: check for bytes -> try msgpack decode -> check for dict -> try blob pointer decode -> try mapping convert. This is the same logic parameterized only by the target type.

**Affected principles:** P7 (DRY -- knowledge not lines)

**Suggested approach:** Extract a generic `decode_cached_payload[T](root, payload, type_) -> tuple[T | None, bool]` function in `typed_codecs.py` that encapsulates the bytes-or-dict-or-blob decision tree. Both stores would call this single function with their respective target types.

### Theme 3: Env Var Boolean Parsing Duplication

**Root cause:** `cache_runtime_tuning.py:47,51` and `diagnostics.py:69` manually parse boolean env vars with `.strip().lower() in {"1", "true", "yes", "on"}` despite `env_bool` from `tools.cq.core.runtime.env_namespace` being available and already used by `policy.py`.

**Affected principles:** P7 (DRY)

**Suggested approach:** Replace the 3 inline boolean parsing sites with `env_bool()` calls. This is a direct one-line-per-site fix.

### Theme 4: Coordination Correctness Risk

**Root cause:** In `coordination.py:66-77`, the `tree_sitter_lane_guard` acquires three locks sequentially (`semaphore.acquire()`, `lock.acquire()`, `rlock.acquire()`) without try/except around the acquisition sequence. If `lock.acquire()` raises an exception after `semaphore.acquire()` succeeds, the semaphore is never released, potentially deadlocking future lane acquisitions.

**Affected principles:** P16 (Functional core, imperative shell), P17 (Idempotency)

**Suggested approach:** Wrap the acquisition sequence in a try/except that releases any successfully acquired primitives on partial failure. Alternatively, nest the acquisitions:
```python
semaphore.acquire()
try:
    lock.acquire()
    try:
        rlock.acquire()
        try:
            yield
        finally:
            rlock.release()
    finally:
        lock.release()
finally:
    semaphore.release()
```

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P14 (Demeter) | `search_artifact_store.py:300` bypasses protocol for `touch()` which already exists on `CqCacheBackend` | small | Removes one unnecessary protocol bypass |
| 2 | P7 (DRY) | `cache_runtime_tuning.py:47,51` and `diagnostics.py:69` duplicate `env_bool` logic | small | Eliminates 3 sites of duplicated boolean parsing |
| 3 | P17 (Correctness) | `coordination.py:66-68` triple lock acquisition without partial-failure cleanup | small | Prevents potential semaphore leak on partial acquisition failure |
| 4 | P7 (DRY) | Identical decode patterns in `tree_sitter_cache_store.py:115-136` and `search_artifact_store.py:311-332` | small | Consolidates ~40 lines of duplicated decode logic |
| 5 | P4 (Cohesion) | `tree_sitter_blob_store.py` is used by search artifacts too; name is misleading | small | Rename to `blob_store.py` to reflect actual usage scope |

## Recommended Action Sequence

1. **Fix coordination correctness risk** (P17): Wrap the triple-lock acquisition in `coordination.py:66-68` with nested try/finally blocks to ensure partial acquisition cleanup. This is a small change with high correctness impact.

2. **Replace inline boolean parsing with `env_bool`** (P7): Update `cache_runtime_tuning.py:44-51` and `diagnostics.py:69` to use `env_bool()` from `tools.cq.core.runtime.env_namespace`. Three sites, each a one-line fix.

3. **Fix the `touch()` protocol bypass** (P14): In `search_artifact_store.py:298-308`, replace `getattr(backend, "cache", None)` + `cache.touch(...)` with `backend.touch(cache_key, expire=ttl_seconds)`.

4. **Extract generic decode function** (P7): Create `decode_cached_payload[T](root, payload, type_) -> tuple[T | None, bool]` in `typed_codecs.py` and use it in both `tree_sitter_cache_store.py` and `search_artifact_store.py`.

5. **Rename `tree_sitter_blob_store.py` to `blob_store.py`** (P4): Update imports in `search_artifact_store.py` and `tree_sitter_cache_store.py`.

6. **Extend `CqCacheBackend` protocol for streaming operations** (P1, P6): Add `read_streaming(key) -> bytes | None` and `set_streaming(key, payload, ...) -> bool` with no-op defaults. Update `tree_sitter_blob_store.py` to use the protocol methods instead of extracting the raw cache.

7. **Extract singleton lifecycle from `diskcache_backend.py`** (P3): Move `_BackendState`, `get_cq_cache_backend`, `set_cq_cache_backend`, `close_cq_cache_backend` into a `backend_lifecycle.py` module.

8. **Add optional backend injection to public functions** (P23): Make `build_scope_snapshot_fingerprint()`, `persist_tree_sitter_payload()`, and similar functions accept an optional `backend` parameter to improve testability without requiring global state manipulation.
