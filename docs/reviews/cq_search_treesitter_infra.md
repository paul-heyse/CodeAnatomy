# Design Review: CQ Search Infrastructure (tree_sitter / rg / generated / _shared)

**Date:** 2026-02-16
**Scope:** `tools/cq/search/tree_sitter/`, `tools/cq/search/rg/`, `tools/cq/search/generated/`, `tools/cq/search/_shared/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 65

## Executive Summary

The CQ search infrastructure is architecturally well-structured with correct dependency direction (lanes depend on core, never vice versa), strong contract definitions via versioned `msgspec.Struct` types, and consistent fail-open degradation patterns. The primary weaknesses are concentrated in **knowledge duplication** across the `python_lane/` module (duplicated constants, helper functions, and type sets in `runtime.py`, `facts.py`, and `fallback_support.py`) and a **mixed-responsibility** `_shared/core.py` module that bundles five distinct concerns. The rg module is a model of clean separation. Addressing the duplication cluster in `python_lane/` and decomposing `_shared/core.py` would yield the highest alignment improvement with modest effort.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `fallback_support.py` exports private-prefixed names in `__all__` |
| 2 | Separation of concerns | 2 | medium | medium | `_shared/core.py` mixes 5 concerns; `query_models.py` mixes contracts with runtime lint logic |
| 3 | SRP | 2 | medium | medium | `_shared/core.py` has 5+ reasons to change; `infrastructure.py` mixes parallel execution, streaming parsing, parser controls, and language helpers |
| 4 | High cohesion, low coupling | 2 | small | low | Lane modules have high cohesion; `_shared/core.py` has low cohesion across its mixed responsibilities |
| 5 | Dependency direction | 3 | - | - | Lanes depend on core; core never imports lanes; contracts are leaf modules |
| 6 | Ports & Adapters | 2 | small | low | `tree_sitter` and `diskcache` dependencies are properly gated but not abstracted behind ports |
| 7 | DRY (knowledge) | 1 | medium | high | `_pack_source_rows` in 3 places; `_PYTHON_LIFT_ANCHOR_TYPES` in 2; `get_python_field_ids` in 2; `node_text` in 2; constants duplicated across runtime/facts |
| 8 | Design by contract | 3 | - | - | Versioned contracts with `CqStruct`; explicit enrichment contract docstrings |
| 9 | Parse, don't validate | 3 | - | - | `rg/codec.py` decodes JSON into typed structs; `canonicalize_*_lane_payload` validates via `msgspec.convert` |
| 10 | Make illegal states unrepresentable | 2 | small | low | `PythonTreeSitterPayloadV1` and `RustTreeSitterPayloadV1` are near-identical with only a `language` default difference; union would prevent mismatch |
| 11 | CQS | 2 | small | low | `ParseSession.parse` both mutates internal cache and returns tree; `record_runtime_sample` returns `None` but is mutation-only (clean) |
| 12 | DI + explicit composition | 2 | medium | low | `ParseSession` accepts `parser_factory` (good DI); but `_cache()` in `adaptive_runtime.py` uses hidden global lookup |
| 13 | Prefer composition over inheritance | 3 | - | - | No inheritance hierarchies; behavior composed via protocols and callbacks |
| 14 | Law of Demeter | 2 | small | low | `_averager()` chains through `get_cq_cache_backend().cache` then `getattr(avg, "add")` |
| 15 | Tell, don't ask | 2 | small | low | Multiple `getattr`/`callable` guard patterns probe collaborators before acting |
| 16 | Functional core, imperative shell | 2 | small | low | Enrichment functions are largely deterministic transforms; IO (diskcache, subprocess) at edges but not fully separated |
| 17 | Idempotency | 3 | - | - | `enrich_python_context_by_byte_range` and rg searches are idempotent given same inputs |
| 18 | Determinism / reproducibility | 3 | - | - | `lru_cache` on compiled queries; deterministic JSON encoding; sorted outputs in rg adapter |
| 19 | KISS | 2 | small | low | Mostly simple; `_shared/core.py` accumulates incidental complexity from mixed concerns |
| 20 | YAGNI | 3 | - | - | No speculative generality observed; all code paths are actively used |
| 21 | Least astonishment | 2 | small | low | Two `make_parser` functions with different signatures (one takes `str`, one takes `Language`); `__all__` with private names |
| 22 | Declare and version public contracts | 3 | - | - | Versioned `V1` structs; explicit `__all__` exports; `contracts/` package with consolidated re-exports |
| 23 | Design for testability | 2 | medium | medium | Module-level `_SESSIONS` dict and `_cache()` global lookup hinder isolated testing |
| 24 | Observability | 1 | medium | medium | No logging, no metrics, no tracing anywhere in the 65-file scope |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The contract layer (`contracts/`) provides a clean public surface with consolidated re-exports in `contracts/__init__.py`. The rg module hides internal event types behind accessor functions (`as_match_data`, `match_path`, etc.). However, naming conventions leak implementation details.

**Findings:**
- `tools/cq/search/tree_sitter/python_lane/fallback_support.py:349-356`: `__all__` exports private-prefixed names (`_capture_binding_candidates`, `_capture_import_alias_chain`, `_capture_named_definition`, `_capture_qualified_name_candidates`, `_default_parse_quality`, `_fallback_resolution_fields`). These are consumed by `python_lane/runtime.py:67-74`. Exporting underscore-prefixed names as a module's public interface is contradictory -- the names signal "private" while the `__all__` signals "public."
- `tools/cq/search/tree_sitter/rust_lane/runtime.py:53-69`: Imports private-prefixed names from `enrichment_extractors.py` (`_extract_attributes_dict`, `_extract_call_target`, etc.) and from `fact_extraction.py` (`_extend_rust_fact_lists_from_rows`, `_import_rows_from_matches`, etc.). The same naming inconsistency applies.

**Suggested improvement:**
Drop the underscore prefix from functions that are consumed cross-module, or introduce a thin public facade in each sub-module that re-exports them without the underscore prefix. For example, rename `_capture_binding_candidates` to `capture_binding_candidates` in `fallback_support.py`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
Most modules have clear responsibilities. The rg sub-package is exemplary: `codec.py` (decode), `runner.py` (execute), `collector.py` (collect), `adapter.py` (high-level API), `prefilter.py` (ast-grep bridge). Two modules violate this principle.

**Findings:**
- `tools/cq/search/_shared/core.py:1`: Module docstring reads "Consolidated shared helpers, contracts, runtime handles, requests, and timeout utilities" -- acknowledging five concerns in the title. It contains: (a) node text extraction (`node_text` at line 99, `sg_node_text` at line 113), (b) byte-offset conversion (`line_col_to_byte_offset` at line 50), (c) JSON encoding/decoding (`encode_mapping` at line 69, `decode_mapping` at line 78), (d) source hashing (`source_hash` at line 87), (e) enrichment request/settings/runtime contracts (many `CqStruct` classes starting around line 130+), (f) timeout/async execution wrappers (`search_sync_with_timeout`, `search_async_with_timeout`), (g) `RgRunRequest` dataclass.
- `tools/cq/search/tree_sitter/contracts/query_models.py:128-273`: Mixes contract definitions (pure data structs like `QueryPackPlanV1`, `QueryPatternPlanV1`) with runtime logic (`load_pack_rules` at line 128 performs filesystem I/O, `lint_query_pack_source` at line 150 compiles queries and inspects them). A contracts module should contain only data definitions.

**Suggested improvement:**
1. Split `_shared/core.py` into: `_shared/node_helpers.py` (node text), `_shared/encoding.py` (JSON encode/decode, hashing), `_shared/rg_request.py` (RgRunRequest), `_shared/timeout.py` (timeout wrappers), keeping enrichment contracts in `_shared/core.py` or moving to `_shared/enrichment_contracts.py`.
2. Move `load_pack_rules` and `lint_query_pack_source` from `contracts/query_models.py` to `query/lint.py` (which already exists and is a natural home).

**Effort:** medium
**Risk if unaddressed:** medium -- As `_shared/core.py` grows, changes to unrelated concerns risk collateral breakage.

---

#### P3. SRP (one reason to change) -- Alignment: 2/3

**Current state:**
Language lanes (`python_lane/`, `rust_lane/`) are each focused on one language's enrichment. The core runtime modules are mostly single-purpose. Two modules have multiple reasons to change.

**Findings:**
- `tools/cq/search/_shared/core.py`: Changes to JSON encoding policy, tree-sitter node handling, ripgrep request shape, or async timeout strategy all require touching this single file. Five distinct change drivers.
- `tools/cq/search/tree_sitter/core/infrastructure.py:1-302`: Combines four responsibilities: parallel execution (`run_file_lanes_parallel` at line 36), streaming source parsing (`build_stream_reader` at line 67, `parse_streaming_source` at line 91), parser controls (`ParserControlSettingsV1` at line 123, `apply_parser_controls` at line 154), and language runtime helpers (`load_language` at line 174, `make_parser` at line 264, `cached_field_ids` at line 225). Any one of these areas could change independently.

**Suggested improvement:**
For `infrastructure.py`, the four sections are already cleanly separated with comments (`# -- Parallel Execution --`, etc.). Extract into `core/parallel.py`, `core/streaming_parse.py`, `core/parser_controls.py`, and keep `core/infrastructure.py` as the language runtime helpers module. This makes each file's change reason singular.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Lane modules are highly cohesive (each focused on one language). The `contracts/` package is a cohesive collection of typed boundaries. Coupling between lanes and core is narrow and well-defined.

**Findings:**
- `tools/cq/search/_shared/core.py`: Low cohesion -- node text helpers, JSON encoding, source hashing, RgRunRequest, enrichment contracts, and timeout utilities share no conceptual core.
- `tools/cq/search/tree_sitter/python_lane/facts.py:34-36`: Imports from sibling module `python_lane/runtime.py` (`is_tree_sitter_python_available`, `parse_python_tree_with_ranges`). This creates coupling between two large modules (652 + 743 lines) within the same package. If `runtime.py` changes its parsing API, `facts.py` must change too.

**Suggested improvement:**
Extract the shared Python parsing surface (`is_tree_sitter_python_available`, `parse_python_tree_with_ranges`, `get_python_field_ids`) into a dedicated `python_lane/shared.py` module that both `runtime.py` and `facts.py` depend on, eliminating the sibling coupling.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction -- Alignment: 3/3

**Current state:**
Dependencies flow correctly: `python_lane/` and `rust_lane/` depend on `core/` and `contracts/`. Core modules never import from lanes. The `contracts/` package depends only on `tools.cq.core.structs` (framework base). The `rg/` module is fully independent of `tree_sitter/`. The `_shared/` module provides foundation types consumed by both `tree_sitter/` and `rg/`.

No findings. Dependency direction is well-maintained.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
External dependencies (`tree_sitter`, `diskcache`, `rg` subprocess) are gated with try/except import guards throughout the codebase, enabling graceful degradation when optional packages are unavailable.

**Findings:**
- `tools/cq/search/tree_sitter/core/adaptive_runtime.py:13-16`: `diskcache.Averager` import is guarded. The module accesses cache operations via `getattr` chains (`getattr(cache, "get")`, `getattr(avg, "add")`). While this provides runtime safety, it does not define a port/protocol for the caching interface -- the module directly probes the cache object's API surface.
- `tools/cq/search/rg/runner.py:228-262`: Direct `subprocess.Popen` call. This is appropriate for a ripgrep adapter -- the runner IS the adapter. No issue.

**Suggested improvement:**
Define a minimal `CacheLike` protocol (with `get`, `set`, `incr` methods) and use it as the type annotation for the cache interface in `adaptive_runtime.py`. This would make the expected API surface explicit without adding any runtime overhead.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
This is the weakest area. Multiple business-meaningful constants and helper functions are duplicated across the `python_lane/` package, and the `node_text` function is duplicated between `_shared/core.py` and `tree_sitter/core/node_utils.py`.

**Findings:**
- **`_pack_source_rows` in 3 places:**
  - `tools/cq/search/tree_sitter/python_lane/runtime.py:177`
  - `tools/cq/search/tree_sitter/python_lane/facts.py:240`
  - `tools/cq/search/tree_sitter/rust_lane/query_cache.py:18`
  Each loads query pack sources, compiles them, builds plans, and sorts by priority. The logic is structurally identical; only the language name and profile name differ. This is knowledge duplication (the "how to load and compile query packs" invariant).

- **`_PYTHON_LIFT_ANCHOR_TYPES` in 2 places:**
  - `tools/cq/search/tree_sitter/python_lane/runtime.py:457-467`
  - `tools/cq/search/tree_sitter/python_lane/facts.py:53-63`
  Identical frozenset defining which Python AST node types constitute semantic anchors. This is a language-specific policy that should live in one place.

- **`get_python_field_ids` in 2 places:**
  - `tools/cq/search/tree_sitter/python_lane/runtime.py:62-64`
  - `tools/cq/search/tree_sitter/python_lane/fallback_support.py:17-23`
  Both are trivial wrappers around `cached_field_ids("python")`, but each carries its own `@lru_cache` decorator, creating two independent caches for the same data.

- **`node_text` in 2 places:**
  - `tools/cq/search/_shared/core.py:99-110` (simpler version, no `max_len` parameter)
  - `tools/cq/search/tree_sitter/core/node_utils.py:13-58` (canonical version with `max_len`, documented, `NodeLike` protocol)
  The `node_utils.py` version explicitly documents itself as superseding local duplicates (line 4: "It supersedes local `_node_text` duplicates scattered across the codebase"), yet the `_shared/core.py` duplicate persists.

- **Duplicated constants across `runtime.py` and `facts.py`:**
  - `_MAX_CAPTURE_ITEMS = 8`: `runtime.py:55` and implicitly in `fallback_support.py:13`
  - `_DEFAULT_MATCH_LIMIT = 4_096`: `runtime.py:57` and `facts.py:51`
  - `_STOP_CONTEXT_KINDS`: `runtime.py:58` and `facts.py:52`

- **Two `make_parser` with incompatible signatures:**
  - `tools/cq/search/tree_sitter/core/infrastructure.py:264`: `make_parser(language: str) -> Parser` -- takes a language name string
  - `tools/cq/search/tree_sitter/core/lane_support.py:106`: `make_parser(language: Language) -> Parser` -- takes a `Language` object
  These represent the same concept (constructing a parser) but with different contracts. Callers must know which one to use based on what they have available.

**Suggested improvement:**
1. Extract a `_load_pack_source_rows(language: str, profile_name: str)` function into `core/query_pack_executor.py` or a new `core/pack_loading.py`, parameterized by language and profile. Replace all three copies.
2. Move `_PYTHON_LIFT_ANCHOR_TYPES` to `python_lane/constants.py` (or inline at `python_lane/__init__.py`). Import from both `runtime.py` and `facts.py`.
3. Consolidate `get_python_field_ids` to a single location in `python_lane/shared.py` or `python_lane/constants.py`.
4. Delete `node_text` from `_shared/core.py`; update all callers to use `tree_sitter/core/node_utils.py`.
5. Consolidate to one `make_parser` -- the `infrastructure.py` version (takes `str`) is the higher-level one. Remove the `Language`-accepting version from `lane_support.py` or rename it `make_parser_from_language`.

**Effort:** medium
**Risk if unaddressed:** high -- Divergence between duplicated copies will inevitably cause subtle behavioral inconsistencies (e.g., one copy's `_MAX_CAPTURE_ITEMS` is changed but the other is forgotten).

---

#### P8. Design by contract -- Alignment: 3/3

**Current state:**
Contracts are explicit and well-versioned throughout. The `contracts/` package defines typed boundaries using `msgspec.Struct` subclasses with `frozen=True`. Enrichment modules declare their behavioral contract explicitly in module docstrings (e.g., `rust_lane/runtime.py:6-11`: "All fields produced by this module are strictly additive. They never affect: confidence scores, match counts, category classification, or relevance ranking.").

No findings requiring improvement.

---

#### P9. Parse, don't validate -- Alignment: 3/3

**Current state:**
The codebase converts messy inputs into structured types at boundaries:
- `rg/codec.py:1-381`: Decodes raw ripgrep JSON output lines into typed `RgMatchData`, `RgContextData`, `RgSummaryData` structs via `msgspec.json.decode`. Invalid lines return `None` (fail-open).
- `contracts/lane_payloads.py:39-52`: `canonicalize_python_lane_payload` validates payloads via `msgspec.convert(..., strict=False)`, converting raw dicts into checked contract types at the boundary.

No findings requiring improvement.

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Most data structures use frozen dataclasses and `msgspec.Struct(frozen=True)` to prevent mutation after construction. `QueryMode` is an enum, preventing invalid mode strings.

**Findings:**
- `tools/cq/search/tree_sitter/contracts/lane_payloads.py:13-30`: `PythonTreeSitterPayloadV1` and `RustTreeSitterPayloadV1` are structurally identical except for the `language` default (`"python"` vs `"rust"`). Nothing prevents constructing a `PythonTreeSitterPayloadV1(language="rust")` or vice versa. A `Literal["python"]` / `Literal["rust"]` constraint on the `language` field, or a single generic payload parameterized by language, would prevent this mismatch.

**Suggested improvement:**
Change `language` field types to `Literal["python"]` and `Literal["rust"]` respectively, so `msgspec` validates the value at decode time.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS (Command-Query Separation) -- Alignment: 2/3

**Current state:**
Most functions follow CQS. Pure query functions return data without side effects. Mutation functions (like `record_runtime_sample`) return `None`.

**Findings:**
- `tools/cq/search/tree_sitter/core/parse.py:130-163`: `ParseSession.parse()` both returns a tree (query) and mutates internal `_entries` cache, counters, and stats (command). While this is a common pattern for caching parse sessions, it means the method has two roles. The cache mutation is hidden behind the return-value interface.

**Suggested improvement:**
This is a pragmatic violation for caching. The internal mutation is encapsulated and does not affect callers. No change recommended -- documenting the mixed nature in the docstring is sufficient. Score remains 2 because the pattern exists even if it is justified.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 2/3

**Current state:**
`ParseSession` demonstrates good DI -- it accepts a `parser_factory` callable rather than constructing its own parser. `QueryExecutionCallbacksV1` provides an explicit callback bundle. The rg module passes request objects rather than individual parameters.

**Findings:**
- `tools/cq/search/tree_sitter/core/adaptive_runtime.py:19-21`: `_cache()` performs a hidden global lookup via `get_cq_cache_backend(root=Path.cwd())`. The `Path.cwd()` call introduces an implicit environment dependency. Callers of `record_runtime_sample` and `adaptive_query_budget_ms` cannot inject a test cache.
- `tools/cq/search/tree_sitter/core/parse.py:267`: Module-level `_SESSIONS: dict[str, ParseSession] = {}` is a global mutable registry. While `get_parse_session` and `clear_parse_session` provide controlled access, there is no way to inject a custom session store for testing.

**Suggested improvement:**
1. For `_cache()`, accept an optional `cache` parameter that defaults to the global lookup. This enables test injection without changing the default behavior.
2. For `_SESSIONS`, consider making it an attribute of a `ParseSessionRegistry` class that can be instantiated in tests with isolated state.

**Effort:** medium
**Risk if unaddressed:** low (testing ergonomics, not correctness)

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies exist in the reviewed scope. All behavior is composed through:
- Protocols (`NodeLike`, `RuntimeLanguage`, `SupportsQuerySpecialization`, `_LanguageLike`)
- Callbacks (`QueryExecutionCallbacksV1.progress_callback`, `parser_factory`)
- Dataclass composition (`_BoundedQueryStateV1` composes `QueryExecutionSettingsV1`)

No findings requiring improvement.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most modules interact with direct collaborators. A few locations reach through collaborator interfaces.

**Findings:**
- `tools/cq/search/tree_sitter/core/adaptive_runtime.py:19-21`: `_cache()` reaches through `get_cq_cache_backend(root=Path.cwd())` to access `.cache`, then further probes the returned object via `getattr(cache, "get")`, `getattr(cache, "set")`, `getattr(cache, "incr")`. This is a three-level reach-through (backend -> cache -> methods).
- `tools/cq/search/tree_sitter/core/adaptive_runtime.py:77-80`: `add_fn = getattr(avg, "add", None)` followed by `callable(add_fn)` and `add_fn(sample)`. The module probes the `Averager` object's internal API rather than receiving a pre-configured collaborator.

**Suggested improvement:**
Wrap the diskcache interaction behind a small helper class or protocol that exposes the three needed operations (`get`, `set`, `record_sample`), and inject it. This collapses the reach-through into direct interaction.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
Most enrichment logic follows tell-don't-ask: callers pass data to enrichment functions and receive structured payloads. The `RgCollector.handle_event` method delegates to typed handlers (good tell pattern).

**Findings:**
- `tools/cq/search/tree_sitter/core/adaptive_runtime.py:69-93`: `record_runtime_sample` asks the `Averager` object "do you have an `add` method?" and "is it callable?" before telling it to add. This is an ask-then-tell pattern driven by the lack of a typed interface for the external `diskcache` dependency.
- `tools/cq/search/tree_sitter/core/infrastructure.py:235-261`: `child_by_field` asks the node "do you have `child_by_field_id`?" and "is it callable?" before using it, with a fallback to `child_by_field_name`. This is defensive but structurally ask-heavy. The `NodeLike` protocol in `contracts/core_models.py` could be extended to include child access methods to enable direct telling.

**Suggested improvement:**
For `child_by_field`, this is a pragmatic pattern needed for cross-version tree-sitter compatibility. Document the rationale. For `adaptive_runtime.py`, the protocol suggestion from P14 would also address this.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Enrichment functions are largely deterministic transforms: given source bytes and byte ranges, they produce enrichment payloads. IO (subprocess for rg, diskcache for adaptive runtime) is concentrated in specific modules rather than scattered through core logic.

**Findings:**
- `tools/cq/search/tree_sitter/python_lane/runtime.py:554-640`: `enrich_python_context_by_byte_range` is mostly a pure pipeline (parse -> window -> capture -> fallback -> canonicalize), but it directly calls `enqueue_windows` (side effect: writes to work queue at line 605) and `collect_tree_sitter_diagnostics` (pure) within the same function. The work queue side effect could be separated.
- `tools/cq/search/tree_sitter/core/adaptive_runtime.py:69-93`: `record_runtime_sample` is correctly in the imperative shell. But `adaptive_query_budget_ms` (line 128) is a query that reads from the cache -- a side-effect-dependent query mixed into what could be a pure computation given the cached average as input.

**Suggested improvement:**
Factor `adaptive_query_budget_ms` into two parts: a pure `compute_budget_ms(average_ms: float | None, fallback: int) -> int` and a shell function that reads from cache and delegates. This makes the budget logic independently testable.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All query/search operations are idempotent. Running `enrich_python_context_by_byte_range` with the same source and byte range produces the same output. Ripgrep searches are inherently idempotent. The `lru_cache` on query compilation and language loading ensures stable results.

No findings requiring improvement.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
- `_shared/core.py:46`: `_JSON_ENCODER = msgspec.json.Encoder(order="deterministic")` ensures deterministic serialization.
- `rg/adapter.py`: All result lists are `sorted()` before return.
- `core/runtime.py`: Query execution uses fixed match limits and deterministic window ordering.
- Query compilation uses `@lru_cache` keyed on source text, ensuring stable compilation.

No findings requiring improvement.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are straightforward. The rg module is particularly clean. The main complexity concern is `_shared/core.py`'s accumulated breadth.

**Findings:**
- `tools/cq/search/_shared/core.py:1`: The module's docstring ("Consolidated shared helpers, contracts, runtime handles, requests, and timeout utilities") describes a grab-bag rather than a focused component. This makes it harder for new contributors to understand where to add new shared functionality or where to find existing helpers.
- `tools/cq/search/tree_sitter/core/infrastructure.py:91-117`: `parse_streaming_source` has a triple-fallback pattern (try streaming with encoding kwarg, try streaming without, try bytes parse). While necessary for cross-version tree-sitter compatibility, a comment explaining the version-specific reasons would help.

**Suggested improvement:**
Decompose `_shared/core.py` as recommended in P2/P3. Add a brief comment to `parse_streaming_source` explaining the tree-sitter API version differences that motivate the fallback chain.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 3/3

**Current state:**
No speculative generality detected. Every module, contract, and function is actively used. The `generated/` node type snapshots serve as fallbacks when runtime grammar assets are unavailable (used, not speculative). The adaptive runtime's budget computation is actively called from lane enrichment paths.

No findings requiring improvement.

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Most APIs behave as expected. A few naming inconsistencies create potential confusion.

**Findings:**
- **Two `make_parser` functions with incompatible signatures:**
  - `tools/cq/search/tree_sitter/core/infrastructure.py:264`: `make_parser(language: str) -> Parser`
  - `tools/cq/search/tree_sitter/core/lane_support.py:106`: `make_parser(language: Language) -> Parser`
  A developer importing `make_parser` might get either version depending on the import source. The name is identical but the parameter type differs (`str` vs `Language`), violating least astonishment.

- **Private names in `__all__`:**
  - `tools/cq/search/tree_sitter/python_lane/fallback_support.py:349-356`: All six exports start with `_`. A reader inspecting `__all__` expects public API surface; underscore-prefixed names suggest otherwise.

- **`_pack_source_rows` vs `_pack_sources`:**
  - `tools/cq/search/tree_sitter/rust_lane/query_cache.py:18` and `:56`: Two functions with near-identical names. `_pack_source_rows` returns `tuple[tuple[str, str, QueryPackPlanV1], ...]` while `_pack_sources` returns `tuple[tuple[str, str], ...]` (dropping the plan). The naming does not clearly convey the difference.

**Suggested improvement:**
1. Rename `lane_support.make_parser` to `make_parser_from_language` to disambiguate.
2. Drop underscore prefixes from `fallback_support.py` exports.
3. Rename `_pack_sources` to `_pack_name_and_source_pairs` or similar to clarify the data shape difference.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts -- Alignment: 3/3

**Current state:**
The codebase excels here:
- All contract types use `V1` suffix convention (`QueryWindowV1`, `TreeSitterDiagnosticV1`, `RgRunSettingsV1`, etc.).
- `contracts/__init__.py` provides a consolidated import surface.
- Every module declares `__all__` explicitly.
- `rg/contracts.py` defines the serializable boundary between rg runtime and its consumers.

No findings requiring improvement.

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Many components are testable via DI: `ParseSession` accepts `parser_factory`, `RgCollector` accepts `match_factory`, enrichment functions accept configuration parameters. Frozen dataclasses and pure functions are inherently testable.

**Findings:**
- `tools/cq/search/tree_sitter/core/parse.py:267`: `_SESSIONS: dict[str, ParseSession] = {}` is a module-level global. Tests must call `clear_parse_session()` for cleanup, and there is no way to isolate concurrent tests that use different session configurations.
- `tools/cq/search/tree_sitter/core/adaptive_runtime.py:19-21`: `_cache()` calls `get_cq_cache_backend(root=Path.cwd())` -- the `Path.cwd()` call is an implicit environment dependency. Tests must either monkeypatch `Path.cwd` or ensure the working directory has a cache backend, adding setup friction.
- `tools/cq/search/tree_sitter/python_lane/runtime.py:45-52`: Module-level try/except imports of `tree_sitter.Parser`, `tree_sitter.Query`, `tree_sitter.QueryCursor` set module globals to `None` when unavailable. Tests checking degradation paths must monkeypatch these module globals.

**Suggested improvement:**
1. Make `_SESSIONS` injectable by adding an optional `registry` parameter to `get_parse_session`.
2. Add an optional `cache` parameter to `adaptive_query_budget_ms` and `record_runtime_sample`.
3. For the module-level try/except, this is a standard Python pattern and is acceptable. The monkeypatching cost is low.

**Effort:** medium
**Risk if unaddressed:** medium -- As the test suite grows, global state management becomes increasingly brittle.

---

#### P24. Observability -- Alignment: 1/3

**Current state:**
There is **no logging, no metrics collection, and no tracing** anywhere in the 65-file scope. The `adaptive_runtime.py` module records latency samples to diskcache but this is runtime autotuning, not observability. Enrichment functions return `enrichment_status` and `degrade_reason` fields in their payloads, which is a form of structured observability output -- but there are no log emissions, no OpenTelemetry spans, and no structured metric counters.

**Findings:**
- `tools/cq/search/tree_sitter/python_lane/runtime.py`: No `import logging` or `getLogger` anywhere. When enrichment degrades (e.g., `_mark_degraded` at line 490), the reason is embedded in the output payload but never logged.
- `tools/cq/search/rg/runner.py:221-262`: `run_rg_json` spawns subprocess, handles timeout, and returns results -- but timeout events and non-zero return codes are not logged.
- `tools/cq/search/tree_sitter/core/parse.py:90-265`: `ParseSession` tracks counters (`_cache_hits`, `_cache_misses`, `_parse_count`, `_reparse_count`, `_edit_failures`) and exposes them via `stats()`. This is telemetry-ready but not connected to any observability pipeline.

**Suggested improvement:**
1. Add a module-level logger to `python_lane/runtime.py`, `rust_lane/runtime.py`, `rg/runner.py`, and `core/parse.py`. Log at `DEBUG` level for normal operations and `WARNING` for degradation/timeout events.
2. Consider adding OpenTelemetry span instrumentation to the main enrichment entry points (`enrich_python_context_by_byte_range`, `enrich_rust_context_by_byte_range`, `run_rg_json`) to integrate with the project's existing `src/obs/` observability layer.
3. Connect `ParseSession.stats()` to an observable metrics surface.

**Effort:** medium
**Risk if unaddressed:** medium -- Without logging, debugging production enrichment failures requires reproducing the exact input, which is difficult for intermittent tree-sitter issues.

---

## Cross-Cutting Themes

### Theme 1: Python Lane Duplication Cluster

**Description:** The `python_lane/` package (3 files: `runtime.py`, `facts.py`, `fallback_support.py`) contains a concentrated cluster of duplicated constants, helper functions, and type definitions. This affects P7 (DRY), P4 (cohesion), and P19 (KISS).

**Root cause:** `facts.py` was likely developed as an alternative enrichment path alongside `runtime.py`, and shared knowledge was copied rather than extracted to a common location. The two modules grew in parallel without a consolidation pass.

**Affected principles:** P7, P3, P4, P19, P21

**Suggested approach:** Create `python_lane/constants.py` for shared constants (`_PYTHON_LIFT_ANCHOR_TYPES`, `_MAX_CAPTURE_ITEMS`, `_DEFAULT_MATCH_LIMIT`, `_STOP_CONTEXT_KINDS`) and `python_lane/shared.py` for shared functions (`get_python_field_ids`, parsing utilities). Extract a parameterized `_load_pack_source_rows(language, profile)` to `core/` to deduplicate across both lanes.

### Theme 2: `_shared/core.py` as an Accumulation Point

**Description:** `_shared/core.py` has become a catch-all for shared functionality that does not have a natural home. It contains node helpers, encoding utilities, hashing, timeout wrappers, and request/contract types. This affects P2 (separation of concerns), P3 (SRP), P4 (cohesion), and P19 (KISS).

**Root cause:** When a function needs to be shared between `tree_sitter/` and `rg/`, the easiest path is to add it to `_shared/core.py`. Without a policy for when to create new modules, the file grows.

**Affected principles:** P2, P3, P4, P19

**Suggested approach:** Split into 3-4 focused modules based on the natural groupings already visible in the code's section structure. Establish a convention: `_shared/` modules should each have a single-concept name (e.g., `_shared/encoding.py`, `_shared/timeout.py`).

### Theme 3: Silent Failure Without Observability

**Description:** The entire scope follows a fail-open pattern (enrichment degradation, timeout handling, unavailable dependencies). This is architecturally correct for an enrichment layer. However, the absence of any logging means that when failures occur, they are invisible to operators. The `enrichment_status: "degraded"` field in payloads is the only signal, and it is only visible when the output is inspected.

**Root cause:** The CQ tool is primarily a CLI tool, and enrichment is best-effort. Logging was likely deferred as non-essential. As the system matures and enrichment becomes depended on for confidence scoring, the absence of observability becomes a larger gap.

**Affected principles:** P24

**Suggested approach:** Add structured `logging.getLogger(__name__)` to the 4-5 key modules that handle degradation or subprocess execution. Log at DEBUG for success paths and WARNING for degradation events. This is a small, low-risk change with high diagnostic value.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Move `_PYTHON_LIFT_ANCHOR_TYPES`, `_MAX_CAPTURE_ITEMS`, `_DEFAULT_MATCH_LIMIT`, `_STOP_CONTEXT_KINDS` to `python_lane/constants.py` | small | Eliminates 4 duplicated knowledge items across 3 files |
| 2 | P7 (DRY) | Delete `node_text` from `_shared/core.py`; redirect callers to `core/node_utils.py` canonical version | small | Removes documented-but-unfixed duplication |
| 3 | P21 (Least astonishment) | Rename `lane_support.make_parser` to `make_parser_from_language`; drop underscore prefixes from `fallback_support.py` exports | small | Eliminates two naming confusions |
| 4 | P10 (Illegal states) | Change `PythonTreeSitterPayloadV1.language` to `Literal["python"]` and `RustTreeSitterPayloadV1.language` to `Literal["rust"]` | small | Prevents language field mismatch at decode time |
| 5 | P24 (Observability) | Add `logging.getLogger(__name__)` to `python_lane/runtime.py`, `rust_lane/runtime.py`, `rg/runner.py` with WARNING-level degradation logging | small | Makes enrichment failures visible without code changes |

## Recommended Action Sequence

1. **Create `python_lane/constants.py`** -- Extract `_PYTHON_LIFT_ANCHOR_TYPES`, `_MAX_CAPTURE_ITEMS`, `_DEFAULT_MATCH_LIMIT`, `_STOP_CONTEXT_KINDS`, and `get_python_field_ids` into a single authoritative module. Update imports in `runtime.py`, `facts.py`, and `fallback_support.py`. (Addresses P7, P4, P21)

2. **Delete duplicate `node_text` from `_shared/core.py`** -- Update all callers in `_shared/` to import from `tree_sitter/core/node_utils.py`. (Addresses P7)

3. **Extract parameterized `load_pack_source_rows`** -- Create a generic pack-loading function in `core/query_pack_executor.py` parameterized by language and profile. Replace the three `_pack_source_rows` copies. (Addresses P7)

4. **Rename `lane_support.make_parser` and fix `fallback_support.py` exports** -- Disambiguate the two `make_parser` signatures. Drop underscore prefixes from exported names. (Addresses P21, P1)

5. **Add `Literal` type constraints to lane payload `language` fields** -- Tighten `PythonTreeSitterPayloadV1.language` to `Literal["python"]`. (Addresses P10)

6. **Decompose `_shared/core.py`** -- Split into `_shared/encoding.py`, `_shared/timeout.py`, `_shared/rg_request.py`, and a slim `_shared/core.py` for enrichment contracts. (Addresses P2, P3, P4, P19)

7. **Move `lint_query_pack_source` and `load_pack_rules` out of `contracts/query_models.py`** -- Relocate to `query/lint.py`. Keep `contracts/` as pure data definitions. (Addresses P2)

8. **Add structured logging to key modules** -- `logging.getLogger(__name__)` with WARNING for degradation and DEBUG for normal flow in `python_lane/runtime.py`, `rust_lane/runtime.py`, `rg/runner.py`, `core/parse.py`. (Addresses P24)

9. **Inject cache dependency in `adaptive_runtime.py`** -- Add optional `cache` parameter to public functions. Extract `CacheLike` protocol. (Addresses P12, P14, P23)

10. **Decompose `core/infrastructure.py`** -- Split four sections into `core/parallel.py`, `core/streaming_parse.py`, `core/parser_controls.py`, keeping language helpers in `core/infrastructure.py`. (Addresses P3)
