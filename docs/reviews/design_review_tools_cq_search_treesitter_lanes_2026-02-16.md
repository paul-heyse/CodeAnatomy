# Design Review: tools/cq/search tree-sitter + language lanes

**Date:** 2026-02-16
**Scope:** `tools/cq/search/tree_sitter/` (core, python_lane, rust_lane, query, contracts, structural, schema) + `tools/cq/search/python/` + `tools/cq/search/rust/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 72

## Executive Summary

This subsystem is architecturally well-structured with strong contract boundaries (frozen msgspec Structs), a clean core/lane separation, and robust fail-open degradation discipline. The primary design debt lies in **duplicated knowledge across the ast-grep and tree-sitter extraction tiers** -- the Rust enrichment functions (`_scope_chain`, `_find_scope`, `_extract_visibility`, `_extract_function_signature`, `_extract_struct_shape`, `_extract_enum_shape`, `_extract_call_target`) are semantically duplicated between `rust/enrichment.py` (ast-grep tier) and `tree_sitter/rust_lane/enrichment_extractors.py` + `rust_lane/runtime.py` (tree-sitter tier), and the `ENRICHMENT_ERRORS` tuple is redefined in four locations. The highest-impact improvements are (1) consolidating duplicated Rust extraction logic behind a shared protocol, (2) unifying the error boundary tuple, and (3) extracting the `_try_extract`/`_merge_result` pattern into a shared combinator.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `_SESSIONS` dict is module-global but properly lock-guarded |
| 2 | Separation of concerns | 2 | medium | medium | `rust_lane/runtime.py` mixes enrichment orchestration + query pack execution + scope walking |
| 3 | SRP | 2 | medium | medium | `rust_lane/runtime.py` (1097 LOC) changes for scope logic, query pack execution, and payload assembly |
| 4 | High cohesion, low coupling | 2 | small | low | Lane payloads share canonicalization logic; core contracts are well-cohesive |
| 5 | Dependency direction | 3 | - | - | Core contracts have no outward deps; lanes depend on core |
| 6 | Ports & Adapters | 2 | medium | low | No explicit port protocol for tree-sitter/ast-grep parser backends |
| 7 | DRY (knowledge) | 1 | medium | high | Rust extractors duplicated across ast-grep and tree-sitter tiers; ENRICHMENT_ERRORS x4 |
| 8 | Design by contract | 3 | - | - | Frozen msgspec Structs with V1 versioning throughout |
| 9 | Parse, don't validate | 2 | small | low | Payload dicts use `dict[str, object]` instead of typed Structs in enrichment return paths |
| 10 | Illegal states | 2 | small | low | `QueryWindowV1` allows `start_byte > end_byte` by construction |
| 11 | CQS | 2 | small | low | `record_runtime_sample` is a command; `runtime_snapshot` is a query; but `_merge_capture_map` mutates + returns via side effect |
| 12 | DI + explicit composition | 2 | small | low | `ParseSession` takes `parser_factory` callable; `adaptive_runtime` imports `get_cq_cache_backend` directly |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; pure composition throughout |
| 14 | Law of Demeter | 2 | small | low | `getattr(node, "start_byte", 0)` chains are defensive but indicate missing typed access |
| 15 | Tell, don't ask | 2 | small | low | `_apply_extractors` inspects `payload.get("attributes")` after setting it |
| 16 | Functional core, imperative shell | 2 | medium | low | Core transforms are mostly pure; `record_runtime_sample` and `_SESSIONS` are side-effectful singletons |
| 17 | Idempotency | 3 | - | - | Parse sessions are cache-friendly; re-parsing same source yields same tree |
| 18 | Determinism | 3 | - | - | Query windows sorted deterministically; match fingerprints deduplicate |
| 19 | KISS | 2 | small | low | Adaptive runtime autotuning adds complexity for marginal benefit in a dev tool context |
| 20 | YAGNI | 2 | small | low | `QueryPointWindowV1` infrastructure exists but appears lightly used |
| 21 | Least astonishment | 2 | small | low | `enrich_rust_context` takes (line, col) while `enrich_rust_context_by_byte_range` takes byte offsets -- inconsistent API surface |
| 22 | Public contracts | 3 | - | - | `__all__` lists present in all modules; V1-suffixed frozen Structs |
| 23 | Design for testability | 2 | medium | medium | `adaptive_runtime` hard-codes `get_cq_cache_backend(root=Path.cwd())`; not injectable in tests |
| 24 | Observability | 2 | small | low | Logging present; `stage_timings_ms` tracked; but no structured span correlation across lanes |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
Internal parse session state is properly hidden behind `get_parse_session()` and `clear_parse_session()` functions. The `_SESSIONS` module-global dict in `tools/cq/search/tree_sitter/core/parse.py:270` is guarded by `_LANGUAGE_SESSION_LOCK`.

**Findings:**
- `tools/cq/search/tree_sitter/core/infrastructure.py:140-152` re-exports symbols from `parser_controls`, `streaming_parse`, and `node_schema` via `__all__`, acting as a proper facade. However, callers like `rust_lane/runtime.py:32` import from `core.infrastructure` while also importing from `core.lane_support` and `core.node_utils` separately, bypassing the facade.

**Suggested improvement:**
Consolidate the infrastructure facade to include `lane_support` and `node_utils` exports so lane modules have a single import target for core utilities.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
The tree-sitter core layer cleanly separates parsing (`parse.py`), query execution (`runtime.py`), adaptive budgeting (`adaptive_runtime.py`), and parallel execution (`parallel.py`).

**Findings:**
- `tools/cq/search/tree_sitter/rust_lane/runtime.py` (1097 lines) mixes three distinct concerns: (a) scope/node walking utilities (lines 237-303), (b) query pack execution orchestration (lines 620-694), and (c) enrichment payload assembly (lines 560-604, 811-859). These change for different reasons.
- `tools/cq/search/python/extractors.py` (1443 lines) similarly mixes ast-grep extraction, Python AST extraction, import extraction, and payload budgeting in a single file.

**Suggested improvement:**
Extract scope/node walking from `rust_lane/runtime.py` into a dedicated `rust_lane/scope_utils.py`. Extract payload assembly into `rust_lane/payload_assembly.py`. The public entry points (`enrich_rust_context`, `enrich_rust_context_by_byte_range`) remain in `runtime.py` as orchestration.

**Effort:** medium
**Risk if unaddressed:** medium -- difficulty grows as more Rust grammar features are added.

---

#### P3. SRP -- Alignment: 2/3

**Current state:**
Most modules have a single clear responsibility. The contracts module (`contracts/core_models.py`, 318 lines) is appropriately a "contract bucket" -- it changes only when a structural contract changes.

**Findings:**
- `tools/cq/search/tree_sitter/rust_lane/runtime.py` changes for: scope traversal logic, enrichment extractor dispatch, query pack execution strategy, injection plan handling, tag event building, and public API shape. Six reasons to change.
- `tools/cq/search/python/extractors.py` changes for: ast-grep extraction, Python AST extraction, import extraction, payload budgeting, cross-source agreement, and enrichment state management.

**Suggested improvement:**
Same as P2 recommendation. Additionally, extract `_PythonEnrichmentState` and its stage runners from `extractors.py` into a separate `extractors_pipeline.py`.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P5. Dependency direction -- Alignment: 3/3

**Current state:**
The dependency graph flows correctly inward: lane modules (`python_lane/`, `rust_lane/`) depend on `core/` and `contracts/`. The `contracts/core_models.py` module has no outward dependencies beyond `tools.cq.core.structs` (the CQ contract base). The `core/` modules depend only on contracts and infrastructure, never on lanes.

No action needed.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
Parser construction is partially abstracted via `parser_factory: Callable[[], Parser]` in `ParseSession.__init__`. The `load_tree_sitter_language()` function in `language_registry.py:223-240` dispatches to grammar bundles via if/elif branches.

**Findings:**
- `tools/cq/search/tree_sitter/core/language_registry.py:232-240`: Language loading is a hardcoded if/elif chain (`if normalized == "python"` / `if normalized == "rust"`). Adding a third language requires editing this function.
- No explicit protocol for the parsing backend. Both tree-sitter and ast-grep are accessed directly rather than through a port. While this is pragmatic for two backends, the Rust enrichment tier in `rust/enrichment.py` and `tree_sitter/rust_lane/runtime.py` would benefit from a shared extraction protocol.

**Suggested improvement:**
Replace the if/elif chain in `load_tree_sitter_language` with a registry dict mapping language names to grammar loader callables. This preserves simplicity while enabling extension.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge) -- Alignment: 1/3

**Current state:**
This is the most significant gap in the subsystem. Multiple pieces of domain knowledge are duplicated.

**Findings:**

1. **Rust extraction functions duplicated across tiers.** The following functions exist in near-identical form in both `tools/cq/search/rust/enrichment.py` (ast-grep tier) and `tools/cq/search/tree_sitter/rust_lane/enrichment_extractors.py` + `rust_lane/runtime.py` (tree-sitter tier):
   - `_scope_name`: `rust/enrichment.py:109` vs `rust_lane/runtime.py:237`
   - `_scope_chain`: `rust/enrichment.py:140` vs `rust_lane/runtime.py:251`
   - `_find_scope`: `rust/enrichment.py:118` vs `rust_lane/runtime.py:268`
   - `_extract_visibility`: `rust/enrichment.py:154` vs `rust_lane/enrichment_extractors.py:201`
   - `_extract_function_signature`: `rust/enrichment.py:165` vs `rust_lane/enrichment_extractors.py:157`
   - `_extract_struct_shape`: `rust/enrichment.py:220` vs `rust_lane/enrichment_extractors.py:386`
   - `_extract_enum_shape`: `rust/enrichment.py:245` vs `rust_lane/enrichment_extractors.py:422`
   - `_extract_call_target`: `rust/enrichment.py:268` vs `rust_lane/enrichment_extractors.py:352`
   These encode the same Rust semantic knowledge (what constitutes visibility, what a scope chain looks like) but with slightly different signatures (SgNode vs tree-sitter Node + source_bytes).

2. **`ENRICHMENT_ERRORS` tuple defined in four locations:**
   - `tools/cq/search/tree_sitter/core/lane_support.py:20-28`
   - `tools/cq/search/python/extractors.py:119-126`
   - `tools/cq/search/rust/enrichment.py:40`
   - `tools/cq/search/pipeline/smart_search.py:165`

3. **`_SCOPE_KINDS` defined in two locations:**
   - `tools/cq/search/tree_sitter/rust_lane/runtime.py:96-104`
   - `tools/cq/search/rust/enrichment.py:52-62`

**Suggested improvement:**
Define a `RustExtractionSpec` protocol with methods like `scope_name(node, source) -> str | None`, `visibility(node, source) -> str`, etc. Implement it twice: once wrapping SgNode (in `rust/enrichment.py`) and once wrapping tree-sitter Node (in `rust_lane/enrichment_extractors.py`). The actual extraction logic (what constitutes "pub(crate)", what scope kinds exist) lives once in the spec, with node-access adapters. For `ENRICHMENT_ERRORS`, export a single canonical tuple from `lane_support.py` and import it everywhere.

**Effort:** medium
**Risk if unaddressed:** high -- when Rust grammar semantics change (e.g., a new visibility modifier), one tier gets updated but the other drifts silently.

---

#### P8. Design by contract -- Alignment: 3/3

**Current state:**
Excellent use of frozen `msgspec.Struct` contracts throughout. `QueryWindowV1`, `QueryExecutionSettingsV1`, `QueryExecutionTelemetryV1`, `TreeSitterInputEditV1`, and many others provide explicit structural contracts. The `V1` suffix convention enables future evolution.

No action needed.

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
Source bytes are parsed into tree-sitter `Tree` objects at the boundary. Query results are returned as typed capture maps.

**Findings:**
- Enrichment payloads are returned as `dict[str, object]` throughout (`python_lane/runtime.py:556`, `rust_lane/runtime.py:940`, `python/extractors.py:1278`). While `canonicalize_python_lane_payload` in `contracts/lane_payloads.py:39-52` validates via `msgspec.convert`, it validates and discards the result (`_ = msgspec.convert(...)`) rather than returning the typed struct. Downstream consumers continue working with untyped dicts.

**Suggested improvement:**
Have the canonicalization functions return the typed `PythonTreeSitterPayloadV1`/`RustTreeSitterPayloadV1` struct alongside the dict, or convert the public API to return the struct directly.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Most contracts use frozen Structs with sensible defaults. `QueryExecutionSettingsV1.window_mode` uses a `Literal` union of three valid strings.

**Findings:**
- `tools/cq/search/tree_sitter/contracts/core_models.py:55-59`: `QueryWindowV1` allows construction where `start_byte > end_byte`, which is an invalid window. The normalization logic in `runtime.py:82-86` filters these out at runtime rather than preventing construction.

**Suggested improvement:**
Add a `__post_init__` check or use `msgspec.Struct.__init_subclass__` to validate `start_byte <= end_byte` at construction time.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. `runtime_snapshot()` is a pure query. `record_runtime_sample()` is a pure command.

**Findings:**
- `tools/cq/search/tree_sitter/core/runtime.py:363-381`: `_merge_capture_map` mutates `merged` in place (command) while also filtering nodes (query-like behavior). This is acceptable for performance but the function name suggests a command, which is correct.
- `tools/cq/search/tree_sitter/query/registry.py:171-224`: `load_query_pack_sources` both loads sources (query) and updates `runtime_state.last_drift_reports` (command) at line 217.

**Suggested improvement:**
Separate drift report recording from query pack loading. Return the drift report as part of the result rather than mutating runtime state inside a load function.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
`ParseSession` takes an injectable `parser_factory`. The query compiler uses the language registry as a service locator pattern.

**Findings:**
- `tools/cq/search/tree_sitter/core/adaptive_runtime.py:19-23`: `_cache()` hard-codes `get_cq_cache_backend(root=Path.cwd())` as the default. While `cache_backend` is an optional parameter throughout, the default creates a hidden dependency on the filesystem.
- `tools/cq/search/tree_sitter/core/parse.py:31-34`: `_parser_controls()` does a deferred import of `SettingsFactory` and calls a class method. This hidden dependency makes the parse module depend on the settings subsystem implicitly.

**Suggested improvement:**
Thread `cache_backend` through from the top-level call sites rather than defaulting to `Path.cwd()` resolution. For `_parser_controls()`, accept controls as a parameter to `ParseSession.parse()`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies anywhere in scope. All behavior is composed via function calls, dataclass composition, and protocol typing (`NodeLike` protocol in `core_models.py:16-51`).

No action needed.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most code talks to direct collaborators. The `NodeLike` protocol provides a clean interface.

**Findings:**
- `tools/cq/search/tree_sitter/core/runtime.py:169-170`: `int(getattr(node, "start_byte", 0))` patterns are pervasive throughout the runtime module. These defensive getattr chains indicate the code doesn't trust the node type contract, reaching through an untyped interface.
- `tools/cq/search/tree_sitter/core/runtime.py:487`: `bool(getattr(state.cursor, "did_exceed_match_limit", False))` -- reaching into cursor internals defensively.

**Suggested improvement:**
Wrap `QueryCursor` in a thin typed facade that exposes `did_exceed_match_limit` as a property, and use the `NodeLike` protocol consistently to eliminate getattr patterns. The library reference confirms `QueryCursor.did_exceed_match_limit` is a stable API since 0.25.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The `_try_extract`/`_merge_result` pattern in `rust_lane/runtime.py:324-354` is a good "tell" pattern -- extractors are told to run and results are merged.

**Findings:**
- `tools/cq/search/tree_sitter/rust_lane/runtime.py:487-488`: After setting `payload["attributes"]` via `_enrich_visibility_and_attrs`, the code immediately inspects `payload.get("attributes")` to pass to `_classify_item_role`. This is an "ask" pattern -- the payload is treated as a data bag rather than an object that encapsulates its state.

**Suggested improvement:**
Have `_enrich_visibility_and_attrs` return the extracted attributes list alongside updating payload, so `_classify_item_role` receives attributes directly rather than re-extracting from the payload dict.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Core transforms (`_compute_input_edit`, `_byte_offset_to_point`, `build_tag_events`, `_match_fingerprint`) are pure functions. Side effects concentrate at the edges in `ParseSession`, `record_runtime_sample`, and the cache singletons.

**Findings:**
- `tools/cq/search/tree_sitter/core/parse.py:270`: `_SESSIONS: dict[str, ParseSession] = {}` is a mutable module-global singleton. While properly lock-guarded, it makes the parsing subsystem stateful.
- `tools/cq/search/tree_sitter/core/adaptive_runtime.py:72-101`: `record_runtime_sample` mutates diskcache state as a side effect.

**Suggested improvement:**
No immediate change needed -- the singleton pattern is pragmatic for a process-local parse cache. The lock discipline is correct.

**Effort:** N/A
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Parsing the same source bytes with the same file key yields the same tree. `_match_fingerprint` in `runtime.py:271-280` deduplicates matches deterministically. Re-running enrichment on the same input produces the same payload.

No action needed.

---

#### P18. Determinism -- Alignment: 3/3

**Current state:**
Query windows are processed in a deterministic order. Match fingerprints use sorted capture names. Tag events and evidence rows are built from ordered match sequences. The adaptive runtime uses cached averages that converge rather than random seeds.

No action needed.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The bounded query execution in `core/runtime.py` is necessarily complex -- windowed execution with deduplication, budget tracking, and adaptive autotuning. The complexity is justified by the need to handle large files without timeout.

**Findings:**
- `tools/cq/search/tree_sitter/core/runtime.py:218-240`: `_split_window` adds window-splitting complexity for the autotuning path. The split target is derived from `QueryAutotunePlanV1`, adding an indirection layer. In practice, for a developer tool, a simpler fixed-window strategy would suffice.
- The adaptive runtime (`adaptive_runtime.py`, 200 lines) uses diskcache `Averager`, `incr`, and `memoized_value` for latency tracking. This is significant infrastructure for what could be a simple in-memory moving average.

**Suggested improvement:**
Consider whether the diskcache-backed adaptive runtime is worth its complexity. An in-memory exponential moving average with a process-lifetime would be simpler and avoid filesystem dependencies. The diskcache backing is only useful if latency data needs to persist across process restarts, which is unlikely in a CLI tool context.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most abstractions have clear current use cases.

**Findings:**
- `tools/cq/search/tree_sitter/contracts/core_models.py:109-120`: `QueryWindowSourceV1` and `QueryWindowSetV1` appear to be redundant with `QueryWindowV1`. They have the same fields but different names.
- `tools/cq/search/tree_sitter/contracts/core_models.py:62-68`: `QueryPointWindowV1` infrastructure exists in the contracts and runtime but point-window execution appears to be a niche path -- most callers use byte windows.

**Suggested improvement:**
Remove `QueryWindowSourceV1`/`QueryWindowSetV1` if they serve no distinct serialization purpose. Keep `QueryPointWindowV1` but document it as an extension point.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Most APIs are intuitive. The `enrich_*` functions return `dict[str, object] | None` consistently.

**Findings:**
- `tools/cq/search/tree_sitter/rust_lane/runtime.py:940-1011`: `enrich_rust_context` takes `(source, *, line, col, ...)` with 1-based line, 0-based col. `enrich_rust_context_by_byte_range` takes `(source, *, byte_start, byte_end, ...)`. The asymmetric parameter naming (line/col vs byte_start/byte_end) is mildly surprising but documented.
- `tools/cq/search/tree_sitter/contracts/lane_payloads.py:51`: `_ = msgspec.convert(payload, type=PythonTreeSitterPayloadV1, strict=False)` validates and discards. A reader would expect this to be the return value.

**Suggested improvement:**
Add a brief inline comment explaining the "validate-and-discard" pattern in `canonicalize_python_lane_payload`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Public contracts -- Alignment: 3/3

**Current state:**
Every module has an explicit `__all__` list. All serialized contracts use `V1`-suffixed frozen `CqStruct`/`CqCacheStruct` types. The `NodeLike` protocol provides a stable structural interface.

No action needed.

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
`ParseSession` accepts an injectable `parser_factory`. Pure functions like `_compute_input_edit`, `build_tag_events`, and `_match_fingerprint` are trivially testable.

**Findings:**
- `tools/cq/search/tree_sitter/core/adaptive_runtime.py:19-23`: `_cache()` defaults to `get_cq_cache_backend(root=Path.cwd())`. Testing adaptive runtime behavior requires either monkeypatching or passing `cache_backend` to every function. The parameter is available but the default is hard to override in tests.
- `tools/cq/search/tree_sitter/core/parse.py:31-34`: `_parser_controls()` does a deferred import and calls `SettingsFactory.parser_controls()`. This makes `ParseSession.parse()` depend on settings infrastructure that cannot be easily substituted in tests.
- `tools/cq/search/tree_sitter/python_lane/runtime.py:86-92`: `_pack_source_rows()` is decorated with `@lru_cache(maxsize=1)`, making it effectively a singleton. Tests that need different pack configurations must call `_pack_source_rows.cache_clear()`.

**Suggested improvement:**
Accept `parser_controls` as a parameter to `ParseSession.parse()` with a default that calls the factory. This eliminates the hidden dependency without changing callers.

**Effort:** medium
**Risk if unaddressed:** medium -- test fragility increases as the adaptive runtime and settings infrastructure evolves.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
`ParseSessionStatsV1` provides structured counters. `QueryExecutionTelemetryV1` tracks window counts, match limits, and degrade reasons. `stage_timings_ms` in `PythonAnalysisSession` records per-stage latency. Logger usage is consistent with `logging.getLogger(__name__)`.

**Findings:**
- No span correlation across lanes. When a Python enrichment triggers both ast-grep, Python AST, resolution, and tree-sitter stages, each stage logs independently with no shared correlation ID.
- `tools/cq/search/tree_sitter/core/adaptive_runtime.py:88-101`: `record_runtime_sample` suppresses all errors from `incr`. If the cache is corrupted, the failure is completely silent.

**Suggested improvement:**
Add a `request_id` parameter to the enrichment entry points and propagate it through stage timings and telemetry. This enables correlating stages when debugging enrichment pipeline issues.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: Duplicated Rust Extraction Knowledge

The most significant structural issue. Eight extraction functions are semantically duplicated between the ast-grep tier (`rust/enrichment.py`) and the tree-sitter tier (`rust_lane/enrichment_extractors.py` + `rust_lane/runtime.py`). Both tiers encode the same Rust language knowledge (scope kinds, visibility modifiers, function signature structure) but operate on different node types (SgNode vs tree-sitter Node).

**Root cause:** The two tiers were developed independently, each building its own traversal logic for the same grammar constructs.

**Affected principles:** P7 (DRY), P3 (SRP), P2 (separation of concerns).

**Suggested approach:** Define a `RustNodeAccess` protocol that abstracts node field access, text extraction, and child traversal. Implement it for both SgNode and tree-sitter Node. Write the extraction logic once against the protocol. The ast-grep `SgNode.field()` and tree-sitter `child_by_field_name()` APIs are structurally equivalent, making this protocol natural.

### Theme 2: Pervasive `getattr` Defensive Access

Throughout `core/runtime.py` and lane modules, nodes are accessed via `getattr(node, "start_byte", 0)` and `int(getattr(...))` patterns rather than through typed protocols. This defensive coding style suggests the `NodeLike` protocol defined in `contracts/core_models.py:16-51` is not being used to its full potential.

**Root cause:** The `NodeLike` protocol was added after the runtime code was written. The runtime still uses `Any`-typed cursor and node references.

**Affected principles:** P14 (Law of Demeter), P8 (Design by contract).

**Suggested approach:** Type the `cursor` field in `_BoundedQueryStateV1` and the node parameters in runtime functions using `NodeLike` instead of `Any`/`object`. Remove `getattr` patterns in favor of direct property access.

### Theme 3: Error Boundary Tuple Fragmentation

The "fail-open" error boundary pattern is correctly applied throughout the subsystem, but the error tuple itself is defined independently in four locations with slight variations (some include `SyntaxError`, some include `KeyError`/`IndexError`, some don't).

**Root cause:** Each module defined its own error boundary during development.

**Affected principles:** P7 (DRY).

**Suggested approach:** Use the canonical `ENRICHMENT_ERRORS` from `core/lane_support.py` everywhere. If a module needs additional exceptions (e.g., `SyntaxError` for Python AST parsing), define it as `PYTHON_ENRICHMENT_ERRORS = (*ENRICHMENT_ERRORS, SyntaxError)`.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Unify `ENRICHMENT_ERRORS` to single source in `core/lane_support.py` | small | Prevents drift when new exception types need handling |
| 2 | P7 (DRY) | Unify `_SCOPE_KINDS` between `rust/enrichment.py:52` and `rust_lane/runtime.py:96` | small | Single authority for Rust scope semantics |
| 3 | P14 (LoD) | Replace `getattr(node, "start_byte", 0)` with `NodeLike` protocol usage in `core/runtime.py` | small | Typed access catches field renames at type-check time |
| 4 | P21 (Least astonishment) | Add comment explaining validate-and-discard pattern in `lane_payloads.py:51` | small | Prevents confusion for maintainers |
| 5 | P10 (Illegal states) | Add `start_byte <= end_byte` validation to `QueryWindowV1` construction | small | Prevents invalid windows from propagating |

## Recommended Action Sequence

1. **Unify error boundary tuples (P7).** Single `ENRICHMENT_ERRORS` in `core/lane_support.py`, with language-specific extensions. This is zero-risk and removes 4-way drift. Import the canonical tuple in `python/extractors.py`, `rust/enrichment.py`, and `pipeline/smart_search.py`.

2. **Unify `_SCOPE_KINDS` and other Rust grammar constants (P7).** Move to a shared `rust_lane/constants.py` (or `rust/constants.py` if both tiers need it). Import from both `rust/enrichment.py` and `rust_lane/runtime.py`.

3. **Type runtime node access with `NodeLike` protocol (P14, P8).** Replace `getattr` chains in `core/runtime.py` with direct `NodeLike` property access. This is a mechanical change with type-checker verification.

4. **Extract scope/node utilities from `rust_lane/runtime.py` (P2, P3).** Move `_scope_name`, `_scope_chain`, `_find_scope`, `_find_ancestor` into `rust_lane/scope_utils.py`. Reduces `runtime.py` from 1097 to ~900 lines and isolates scope-traversal changes.

5. **Define `RustNodeAccess` protocol to unify extraction logic (P7).** This is the highest-effort, highest-impact change. Abstract `field()`, `text()`, `children()`, `kind()` behind a protocol. Implement for SgNode and tree-sitter Node. Rewrite extraction functions once against the protocol. Eliminates 8 duplicated functions.

6. **Improve testability of adaptive runtime (P23).** Thread `cache_backend` through `ParseSession.parse()` instead of defaulting to `Path.cwd()` resolution. Accept `parser_controls` as a parameter.

7. **Add request-level correlation to enrichment pipeline (P24).** Thread a `request_id` through stage timings, telemetry, and logging for cross-stage debugging.
