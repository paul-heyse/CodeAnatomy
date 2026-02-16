# Design Review: tools/cq/search/ (Language Lanes and Shared Infrastructure)

**Date:** 2026-02-15
**Scope:** `tools/cq/search/` excluding `pipeline/` and `tree_sitter/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 35

Subdirectories in scope: `_shared/`, `objects/`, `python/`, `rg/`, `rust/`, `semantic/`, `enrichment/`, `generated/`, and `search/__init__.py`.

## Executive Summary

The search lane infrastructure demonstrates strong boundary design within the ripgrep layer (`rg/`), good fail-open resilience throughout enrichment, and solid use of typed contracts for serializable payloads. However, three systemic issues lower overall alignment: (1) enrichment payloads flow as `dict[str, object]` rather than typed structs, forcing extensive `isinstance` checking in downstream consumers like `objects/resolve.py`; (2) three sets of utility functions are duplicated across modules (`_node_byte_span`/`_ast_node_priority`/`_iter_nodes_with_parents`, `_as_int`/`_as_optional_str`, and `fail_open`/`enrich_semantics`); and (3) language lanes and `_shared/core.py` depend upward on `pipeline/` types (`QueryMode`, `SearchLimits`), creating a reverse dependency that couples leaf modules to their orchestrator. The top priority improvements are extracting shared AST helpers into a single module, promoting enrichment payloads to typed structs, and inverting the `pipeline/` dependency direction.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `_RUNTIME_ONLY_ATTR_NAMES` exposed in `_shared/core.py`; LRU caches are module globals |
| 2 | Separation of concerns | 2 | medium | medium | `extractors.py` mixes pipeline orchestration, AST traversal, and payload formatting |
| 3 | SRP | 2 | medium | medium | `extractors.py` (2251 LOC) has 5+ reasons to change |
| 4 | High cohesion, low coupling | 2 | medium | medium | `objects/resolve.py` couples to `python/evidence.py`; `rg/` couples to `pipeline/` |
| 5 | Dependency direction | 1 | medium | high | `_shared/core.py`, `rg/*`, `rust/enrichment.py` all import upward from `pipeline/` |
| 6 | Ports & Adapters | 2 | medium | low | `rg/` is a good adapter; semantic providers could use a protocol |
| 7 | DRY | 1 | small | medium | Three distinct sets of duplicated utilities across modules |
| 8 | Design by contract | 2 | small | low | Enrichment contracts exist but `data: dict[str, object]` is too loose |
| 9 | Parse, don't validate | 1 | large | medium | Enrichment payloads are untyped dicts; consumers do repetitive isinstance checks |
| 10 | Illegal states unrepresentable | 2 | medium | low | `EnrichmentStatus = str` allows arbitrary values; `data: dict` allows invalid shapes |
| 11 | CQS | 2 | small | low | `attach_rust_evidence` mutates and returns; `append_source` mutates in place (correct) |
| 12 | DI + explicit composition | 2 | medium | low | `PythonAnalysisSession.ensure_sg_root` uses deferred import instead of DI |
| 13 | Composition over inheritance | 3 | - | - | No inheritance hierarchies; all composition-based |
| 14 | Law of Demeter | 2 | small | low | Chained access in `resolve.py` on nested dict payloads |
| 15 | Tell, don't ask | 1 | medium | medium | `resolve.py` interrogates raw dict payloads extensively instead of typed methods |
| 16 | Functional core, imperative shell | 2 | medium | low | Pure transform functions exist but mixed with cache/IO in `extractors.py` |
| 17 | Idempotency | 3 | - | - | Enrichment is additive and re-runnable |
| 18 | Determinism / reproducibility | 2 | small | low | `_truncation_tracker` global can leak state between calls |
| 19 | KISS | 2 | small | low | `enrichment/core.py` partitioning uses 4 `frozenset` key groups with fallback bucket |
| 20 | YAGNI | 3 | - | - | No speculative abstractions observed |
| 21 | Least astonishment | 2 | small | low | `fail_open` / `enrich_semantics` in two modules with same name but different behavior |
| 22 | Declare and version contracts | 2 | small | low | `V1` suffix convention followed; `__all__` present everywhere |
| 23 | Design for testability | 2 | medium | medium | Module-level caches and deferred imports hinder unit testing |
| 24 | Observability | 2 | small | low | Stage timings in `PythonAnalysisSession`; no structured logging for errors |

## Detailed Findings

### Category: Boundaries (Principles 1-6)

#### P1. Information Hiding -- Alignment: 2/3

**Current state:**
The `rg/` subpackage provides excellent information hiding with `codec.py` encapsulating JSON event structure, `runner.py` encapsulating process management, and `contracts.py` providing clean serializable output types. Module-level caches (`_SESSION_CACHE`, `_AST_CACHE`) are behind accessor functions (`get_python_analysis_session`, `clear_python_analysis_sessions`).

**Findings:**
- `tools/cq/search/_shared/core.py:34` exposes `_RUNTIME_ONLY_ATTR_NAMES` as a module-level set used by `assert_no_runtime_only_keys`. The name suggests it is internal but it is used cross-module and could be part of a public contract instead.
- `tools/cq/search/python/analysis_session.py:22` exposes the raw `_SESSION_CACHE` dict. While accessor functions exist (lines 268-311), the cache dict itself is not name-mangled or otherwise protected from direct access by other modules.
- `tools/cq/search/rust/enrichment.py:40` exposes `_AST_CACHE` as a plain module-level dict with the same pattern.

**Suggested improvement:**
Wrap `_SESSION_CACHE` and `_AST_CACHE` behind a small `_CacheStore` class or closure that encapsulates the LRU eviction logic (which is currently duplicated between Python and Rust). The eviction pattern at `analysis_session.py:297-299` and `rust/enrichment.py:90-92` is identical and could be a single shared implementation.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of Concerns -- Alignment: 2/3

**Current state:**
The module decomposition generally separates concerns well: `rg/` handles process execution, `python/` handles Python-specific enrichment, `rust/` handles Rust-specific enrichment, and `objects/` handles identity resolution. However, `extractors.py` is an outlier.

**Findings:**
- `tools/cq/search/python/extractors.py` (2251 LOC) mixes five distinct concerns: (1) AST traversal and node classification (lines 100-180), (2) ast-grep stage execution (lines 300-800), (3) Python AST stage execution (lines 800-1200), (4) import normalization (lines 1200-1600), (5) payload finalization and budget enforcement (lines 2050-2100). Each of these could be a separate module.
- `tools/cq/search/enrichment/core.py` mixes payload normalization utilities (lines 19-109) with Python-specific field partitioning knowledge (lines 136-259). The `_PY_RESOLUTION_KEYS`, `_PY_BEHAVIOR_KEYS`, `_PY_STRUCTURAL_KEYS` constants at lines 136-205 are Python-specific domain knowledge embedded in a module named "core" that is supposed to be shared.

**Suggested improvement:**
Split `extractors.py` into stage-specific modules (e.g., `python/stages/ast_grep.py`, `python/stages/python_ast.py`, `python/stages/import_detail.py`) with a thin orchestrator in `extractors.py`. Move the `_PY_*_KEYS` constants and `_partition_python_payload_fields` from `enrichment/core.py` into `enrichment/python_normalization.py` or `python/payload_schema.py`.

**Effort:** medium
**Risk if unaddressed:** medium -- the 2251-line `extractors.py` is the highest-churn file and its breadth makes reasoning about changes difficult.

---

#### P3. SRP (One Reason to Change) -- Alignment: 2/3

**Current state:**
Most modules follow SRP well. `rg/codec.py` changes only when ripgrep JSON format changes. `rg/runner.py` changes only when process execution requirements change.

**Findings:**
- `tools/cq/search/python/extractors.py` has at least five reasons to change: (1) ast-grep API changes, (2) Python `ast` module changes, (3) import normalization logic changes, (4) tree-sitter enrichment integration changes, (5) payload budget/format changes. This is the primary SRP violation in scope.
- `tools/cq/search/objects/resolve.py` (698 LOC) has two reasons to change: (1) identity resolution logic and (2) payload field extraction. The `_resolve_object_ref` function at line 30 (estimated) is responsible for both interpreting diverse payload shapes and building `ResolvedObjectRef` identity keys.

**Suggested improvement:**
Same as P2: decompose `extractors.py` into stage-specific modules. For `resolve.py`, extract payload field accessors into a typed adapter that converts `dict[str, object]` payloads into a typed intermediate form before identity resolution.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P4. High Cohesion, Low Coupling -- Alignment: 2/3

**Current state:**
Within each language lane, cohesion is high. The `rg/` subpackage is well-decomposed with each module serving a clear role. The `python/` subpackage has good internal cohesion between `analysis_session.py`, `resolution_support.py`, and `resolution_index.py`.

**Findings:**
- `tools/cq/search/objects/resolve.py:19` imports `evaluate_python_semantic_signal_from_mapping` from `tools/cq/search/python/evidence.py`. This creates a cross-lane coupling: the language-agnostic `objects/` module depends on the Python-specific `python/` module.
- `tools/cq/search/rg/collector.py:32` imports `RawMatch` from `tools/cq/search/pipeline/smart_search` (TYPE_CHECKING only), coupling the low-level ripgrep collector to the high-level search pipeline.
- `tools/cq/search/rg/adapter.py:17-18` imports `QueryMode` and `SearchLimits` directly from `pipeline/`, coupling the adapter to the pipeline layer's types.

**Suggested improvement:**
Move `evaluate_python_semantic_signal_from_mapping` to `_shared/` or `enrichment/` since it evaluates language-agnostic semantic signal patterns. Define `QueryMode` and `SearchLimits` in `_shared/` (or re-export them) so that leaf modules do not import from `pipeline/`.

**Effort:** medium
**Risk if unaddressed:** medium -- cross-lane coupling makes it harder to evolve language lanes independently.

---

#### P5. Dependency Direction -- Alignment: 1/3

**Current state:**
The intended layering is: `_shared/` (foundation) <- language lanes (`python/`, `rust/`, `rg/`) <- `objects/` <- `pipeline/` (orchestrator). Several modules violate this by importing upward from `pipeline/`.

**Findings:**
- `tools/cq/search/_shared/core.py:26-27` imports `QueryMode` from `pipeline/classifier` and `SearchLimits` from `pipeline/profiles` (TYPE_CHECKING block). These types are fundamental to all lane modules and should live in `_shared/` or be defined independently.
- `tools/cq/search/rg/runner.py:12-13` imports `QueryMode` and `SearchLimits` directly (runtime) from `pipeline/`.
- `tools/cq/search/rg/adapter.py:17-18` imports `QueryMode`, `SearchLimits`, `DEFAULT`, `INTERACTIVE` from `pipeline/`.
- `tools/cq/search/rg/prefilter.py:11-12` imports `QueryMode` and `SearchLimits` from `pipeline/`.
- `tools/cq/search/rg/collector.py:31-32` imports `SearchLimits` and `RawMatch` from `pipeline/` (TYPE_CHECKING).
- `tools/cq/search/rust/enrichment.py:24` imports `get_node_index` from `pipeline/classifier` at runtime.
- `tools/cq/search/python/analysis_session.py:116,141` imports `get_sg_root` and `get_node_index` from `pipeline/classifier` via deferred import.
- `tools/cq/search/semantic/front_door.py:34` imports `get_sg_root` from `pipeline/classifier` at runtime.

This means 8 modules across `_shared/`, `rg/`, `rust/`, `python/`, and `semantic/` all depend upward on `pipeline/`, inverting the intended dependency direction.

**Suggested improvement:**
Move `QueryMode` (enum) and `SearchLimits` (dataclass) from `pipeline/classifier.py` and `pipeline/profiles.py` into `_shared/core.py` or a new `_shared/types.py`. For `get_sg_root` and `get_node_index`, define a protocol in `_shared/` that `pipeline/classifier` implements, and inject it into the lane modules rather than importing the concrete implementation.

**Effort:** medium
**Risk if unaddressed:** high -- every lane change risks cascading through the `pipeline/` layer, and the `pipeline/` cannot be refactored without considering all lane modules.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
The `rg/` subpackage is an excellent adapter: it wraps the external `rg` process behind typed contracts (`RgRunRequest`, `RgProcessResult`, `RgRunSettingsV1`). The `semantic/` subpackage defines `LanguageSemanticEnrichmentRequest` and `SemanticOutcomeV1` as port contracts.

**Findings:**
- The semantic provider dispatch in `tools/cq/search/semantic/front_door.py:340-380` (estimated) uses hardcoded `if language == "python"` / `elif language == "rust"` branching rather than a protocol-based dispatch. Adding a new language requires modifying the front door.
- `tools/cq/search/python/analysis_session.py:116` calls `get_sg_root` via deferred import from `pipeline/classifier`. This is an implicit adapter -- it would be cleaner as an explicit port (a callable parameter or protocol) injected at construction time.

**Suggested improvement:**
Define a `LanguageEnrichmentProvider` protocol in `semantic/models.py` with a single `enrich(request) -> outcome` method. Register providers in a dict at initialization time. This makes the semantic front door open-closed for new languages.

**Effort:** medium
**Risk if unaddressed:** low -- only two languages exist today, so the conditional dispatch works.

---

### Category: Knowledge (Principles 7-11)

#### P7. DRY (Knowledge, Not Lines) -- Alignment: 1/3

**Current state:**
Three distinct sets of utility functions are duplicated across modules, representing duplicated knowledge about how to perform common operations.

**Findings:**
- **AST traversal helpers:** `_node_byte_span` (line-col to byte offset conversion for AST nodes), `_ast_node_priority` (node type priority ranking), and `_iter_nodes_with_parents` (iterative parent-chain traversal) are duplicated verbatim between `tools/cq/search/python/analysis_session.py:36-78` and `tools/cq/search/python/resolution_support.py:62-100`.
- **Type coercion helpers:** `_as_int` is defined at `tools/cq/search/objects/resolve.py:662` and `tools/cq/search/python/evidence.py:53` with slightly different signatures (one returns `int | None`, the other returns `int` with a default). `_as_optional_str` is defined at `tools/cq/search/objects/resolve.py:658` and `tools/cq/search/python/resolution_index.py:56`.
- **Semantic facade functions:** `fail_open` and `enrich_semantics` are defined in both `tools/cq/search/semantic/models.py:506,511` and `tools/cq/search/semantic/front_door.py:501,506`. Both create `SemanticOutcomeV1` instances but the front-door version wraps the model version with caching logic.
- **Trivial wrappers:** `tools/cq/search/rust/enrichment.py:73-74` defines `_source_hash` as a one-line wrapper around `_shared_source_hash`. Similarly, `_node_text` at line 102-103 wraps `_shared_sg_node_text`. These wrappers add indirection without value.
- **LRU eviction logic:** The pattern of checking cache size, evicting oldest entry, and inserting is duplicated between `tools/cq/search/python/analysis_session.py:297-300` and `tools/cq/search/rust/enrichment.py:90-93`.

**Suggested improvement:**
1. Extract `_node_byte_span`, `_ast_node_priority`, `_iter_nodes_with_parents` into `python/ast_helpers.py` and import from both `analysis_session.py` and `resolution_support.py`.
2. Consolidate `_as_int` and `_as_optional_str` into `_shared/core.py` or `enrichment/core.py` as public utilities.
3. Remove the `fail_open`/`enrich_semantics` from `models.py` (or make them the canonical source that `front_door.py` calls).
4. Remove trivial wrappers in `rust/enrichment.py` and import the shared functions directly.
5. Extract LRU eviction into a `_shared/lru_dict.py` utility or use a standard `OrderedDict` subclass.

**Effort:** small (most are mechanical moves)
**Risk if unaddressed:** medium -- divergent evolution of duplicated helpers can introduce subtle bugs (e.g., differing `_as_int` signatures already show this).

---

#### P8. Design by Contract -- Alignment: 2/3

**Current state:**
Contracts are explicit for serializable boundaries: `RgRunSettingsV1`, `RgProcessResultV1`, `EnrichmentMeta`, `PythonEnrichmentPayload`, `RustEnrichmentPayload`, `SemanticOutcomeV1`, `SearchSummaryContract`. Preconditions are checked (e.g., `assert_no_runtime_only_keys`).

**Findings:**
- `tools/cq/search/enrichment/contracts.py:28,35` both define `data: dict[str, object]` -- the contract exists but the data payload is untyped. The `PythonEnrichmentPayload` and `RustEnrichmentPayload` wrappers carry typed metadata but their main payload is an opaque dict.
- `tools/cq/search/enrichment/contracts.py:9` defines `EnrichmentStatus = str` as a plain type alias rather than a `Literal["applied", "degraded", "skipped"]` or enum. This allows arbitrary string values.
- `tools/cq/search/_shared/core.py:114-130` (estimated) defines `PythonNodeEnrichmentSettingsV1` with typed fields but the runtime companion `PythonNodeRuntimeV1` uses `sg_root: object` and `node_index: object` -- a conscious choice documented as "runtime-only" but it weakens the contract.

**Suggested improvement:**
Define `EnrichmentStatus` as `Literal["applied", "degraded", "skipped"]` instead of `str`. For the enrichment data payloads, define typed structs (e.g., `PythonResolutionData`, `PythonBehaviorData`, `PythonStructuralData`) that replace the `data: dict[str, object]` field on the enrichment wrappers.

**Effort:** small (for `EnrichmentStatus`), medium (for typed data payloads)
**Risk if unaddressed:** low

---

#### P9. Parse, Don't Validate -- Alignment: 1/3

**Current state:**
The ripgrep layer does this well: `rg/codec.py` parses raw JSON into typed `RgMatchData`, `RgContextData`, etc. via msgspec, with accessor functions that return typed values. However, the enrichment pipeline does not follow this principle.

**Findings:**
- Enrichment payloads flow through the entire system as `dict[str, object]`. The Python enrichment pipeline in `tools/cq/search/python/extractors.py` builds payloads by setting string keys on a mutable dict (e.g., `state.payload["signature"] = ...` at many points). These dicts are never parsed into typed structs before being consumed.
- `tools/cq/search/objects/resolve.py` is the primary consumer and must defensively interrogate every field: `_as_int(value)`, `_as_optional_str(value)`, `isinstance(value, dict)`, `isinstance(value, list)` checks appear on nearly every line of `_resolve_object_ref` (lines 30-650 approximately). This is pure "validate" rather than "parse."
- `tools/cq/search/enrichment/core.py:112-133` (`_meta_from_flat`) does the same defensive extraction from flat dict payloads with extensive `isinstance` checking to build the typed `EnrichmentMeta`.
- `tools/cq/search/enrichment/core.py:212-241` (`_partition_python_payload_fields`) partitions a flat dict into 5 sub-dicts by checking membership in 4 `frozenset` key registries. This is classification-by-validation rather than parsing into a typed schema.

**Suggested improvement:**
Define typed structs for the enrichment data sections (`PythonResolutionFacts`, `PythonBehaviorFacts`, `PythonStructuralFacts`, `RustEnrichmentFacts`). Build these structs at the boundary in `extractors.py` when the enrichment pipeline completes, rather than building `dict[str, object]`. Consumers like `resolve.py` would then access typed fields directly. The `enrichment/core.py` partitioning logic would become a msgspec decode call.

**Effort:** large
**Risk if unaddressed:** medium -- every new field requires updating the dict-building code, the key set constants, and the dict-reading code in `resolve.py`.

---

#### P10. Make Illegal States Unrepresentable -- Alignment: 2/3

**Current state:**
`rg/codec.py` uses tagged unions (`RgMatchEvent`, `RgContextEvent`, etc.) which prevent illegal event type combinations. `rg/contracts.py` uses frozen dataclasses and msgspec structs.

**Findings:**
- `tools/cq/search/enrichment/contracts.py:9`: `EnrichmentStatus = str` allows any string, but only `"applied"`, `"degraded"`, `"skipped"` are meaningful.
- `tools/cq/search/enrichment/contracts.py:28`: `PythonEnrichmentPayload.data` is `dict[str, object]`, allowing arbitrary key-value combinations including invalid ones.
- `tools/cq/search/python/analysis_session.py:90`: `node_index: Any | None = None` uses `Any` rather than a protocol or typed handle. Same for `tree_sitter_tree: Any | None = None` at line 93.
- `tools/cq/search/rg/runner.py:40-41`: `RgCountRequest.mode` is typed as `QueryMode` (good), but `RgFilesWithMatchesRequest.mode` at line 54 is also `QueryMode` -- consistent, which is correct.

**Suggested improvement:**
Replace `EnrichmentStatus = str` with a `Literal` type or enum. Define typed protocols for `node_index` and `tree_sitter_tree` fields in `PythonAnalysisSession` to constrain allowed shapes. For enrichment `data`, define typed structs per the P9 suggestion.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P11. CQS (Command-Query Separation) -- Alignment: 2/3

**Current state:**
Most functions follow CQS. Query functions like `match_path`, `match_line_text`, `submatch_text` in `rg/codec.py` are pure accessors. Command functions like `append_source`, `set_degraded` in `enrichment/core.py` mutate and return `None`.

**Findings:**
- `tools/cq/search/rust/evidence.py:239` defines `attach_rust_evidence(payload: dict[str, object]) -> dict[str, object]` which mutates the input dict in place AND returns it. This violates CQS by being both a command (mutation) and a query (return).
- `tools/cq/search/enrichment/core.py:85-109` defines `enforce_payload_budget` which mutates the payload dict via `payload.pop()` and returns `tuple[list[str], int]`. This is a mixed command+query, though the return value reports what was changed, which is a reasonable pattern for budget enforcement.

**Suggested improvement:**
Change `attach_rust_evidence` to either (a) return `None` (command style, since it mutates in place) or (b) return a new dict without mutating the input (query style). The `enforce_payload_budget` pattern is acceptable as-is since the return describes the side effect.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (Principles 12-15)

#### P12. Dependency Inversion + Explicit Composition -- Alignment: 2/3

**Current state:**
The `_shared/core.py` module defines request dataclasses that act as value objects for DI. The `semantic/front_door.py` accepts `LanguageSemanticEnrichmentRequest` as an explicit input. However, several modules use deferred imports instead of DI.

**Findings:**
- `tools/cq/search/python/analysis_session.py:116` uses `from tools.cq.search.pipeline.classifier import get_sg_root` as a deferred import inside `ensure_sg_root()`. This hides the dependency and makes it impossible to substitute in tests without monkeypatching.
- Same pattern at `analysis_session.py:141` for `get_node_index`.
- `tools/cq/search/python/extractors.py:2176` uses the same deferred import pattern for `get_node_index`.
- `tools/cq/search/semantic/front_door.py:34` imports `get_sg_root` at module level from `pipeline/classifier`.

**Suggested improvement:**
Accept `sg_root_factory` and `node_index_factory` as optional callable parameters on `PythonAnalysisSession` (or on `get_python_analysis_session`). When not provided, fall back to the deferred import. This enables test injection without monkeypatching while preserving the current default behavior.

**Effort:** medium
**Risk if unaddressed:** low -- deferred imports work but are harder to test and reason about.

---

#### P13. Prefer Composition over Inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies exist in the reviewed scope. All behavior is composed via dataclass aggregation, function composition, and module-level delegation. `PythonAnalysisSession` uses composition (holds references to `SgRoot`, `ast.Module`, etc.). `RgCollector` composes with a `match_factory` callable.

**Findings:**
No violations found.

**Effort:** -
**Risk if unaddressed:** -

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
Most modules talk only to direct collaborators. The `rg/` subpackage's layered design (codec -> collector -> adapter -> runner) follows Demeter well.

**Findings:**
- `tools/cq/search/objects/resolve.py` extensively traverses nested dict structures. For example, accessing `payload.get("python_resolution", {}).get("binding_candidates", [])` (pattern observed throughout). This is reaching through the collaborator's internal structure.
- `tools/cq/search/enrichment/core.py:252-253` accesses `structural.get("structural_context")` and uses it to derive boolean flags in `_derive_behavior_flags`, reaching into the structural dict's shape.

**Suggested improvement:**
This is a symptom of the P9 issue. If enrichment payloads were typed structs, the Law of Demeter violations would largely resolve themselves since consumers would access direct attributes rather than nested dict keys.

**Effort:** small (if P9 is addressed)
**Risk if unaddressed:** low

---

#### P15. Tell, Don't Ask -- Alignment: 1/3

**Current state:**
The enrichment pipeline largely follows tell-don't-ask within each stage. However, `objects/resolve.py` is a major violation point.

**Findings:**
- `tools/cq/search/objects/resolve.py` (698 LOC) is almost entirely "ask" code. The entire `_resolve_object_ref` function extracts data from payload dicts via `.get()` calls, checks types with `isinstance`, and builds identity objects. The payload dicts do not encapsulate any behavior -- they are pure data bags that external code interrogates.
- `tools/cq/search/enrichment/core.py:223-241` (`_partition_python_payload_fields`) iterates over all keys in a flat dict and classifies them into 5 buckets. The dict does not know its own structure; the classification knowledge lives in the partitioner.

**Suggested improvement:**
Move classification knowledge into the payload structs themselves. A typed `PythonEnrichmentResult` struct could have `.resolution`, `.behavior`, `.structural` properties that expose pre-partitioned data. The `_resolve_object_ref` function in `resolve.py` should receive a typed struct and access its attributes directly.

**Effort:** medium
**Risk if unaddressed:** medium -- every consumer of enrichment payloads must independently know how to interpret the dict structure.

---

### Category: Correctness (Principles 16-18)

#### P16. Functional Core, Imperative Shell -- Alignment: 2/3

**Current state:**
Many functions in the enrichment pipeline are pure transforms: `extract_literal_fragments` in `prefilter.py`, `build_rg_command` in `runner.py`, and the codec accessor functions in `codec.py`. However, the enrichment stages mix pure logic with cache access and I/O.

**Findings:**
- `tools/cq/search/python/extractors.py` mixes pure enrichment logic (building payload dicts from AST nodes) with imperative concerns (cache lookups via `_AST_CACHE`, file I/O via `PythonAnalysisSession`, timer management via `perf_counter`).
- `tools/cq/search/python/analysis_session.py:99-265` mixes state management (lazy `ensure_*` methods that mutate `self`) with error recording (`stage_errors`) and timing (`stage_timings_ms`). The analysis artifacts are correctly memoized but the timing/error concerns are interleaved.
- `tools/cq/search/rg/runner.py:224-265` (`run_rg_json`) properly separates I/O (subprocess execution) from decoding (event parsing), which is a good functional core pattern.

**Suggested improvement:**
Factor pure payload-building functions out of `extractors.py` stages so they can be tested without cache or file system. Each stage's pure logic (AST traversal -> payload fragment) should be a standalone function that receives parsed artifacts as parameters.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Enrichment is explicitly documented as additive and stateless per invocation. The docstring at `extractors.py:1-16` states: "All fields produced by this module are strictly additive." Running enrichment multiple times on the same match produces the same payload. The `merge_gap_fill_payload` in `enrichment/core.py` only fills missing values, preserving existing ones.

**Findings:**
No violations found.

**Effort:** -
**Risk if unaddressed:** -

---

#### P18. Determinism / Reproducibility -- Alignment: 2/3

**Current state:**
Most operations are deterministic: `build_rg_command` produces deterministic command lines, enrichment stages apply the same logic to the same inputs.

**Findings:**
- `tools/cq/search/python/extractors.py:187` defines `_truncation_tracker: list[str] = []` as module-level mutable state. This tracker accumulates across calls and is cleared at `line 2070` and `line 2101`. If a function call fails between accumulation and clearing, the tracker leaks state into subsequent calls, potentially producing non-deterministic `truncated_fields` values.
- `tools/cq/search/python/extractors.py:215` appends to `_truncation_tracker` during truncation. If two enrichment calls execute concurrently (e.g., in a thread pool), the tracker would produce corrupted results.

**Suggested improvement:**
Move `_truncation_tracker` into `_PythonEnrichmentState` (the per-call mutable state dataclass) instead of using module-level state. This makes truncation tracking scoped to a single enrichment invocation and eliminates the concurrency hazard.

**Effort:** small
**Risk if unaddressed:** low (concurrency is not currently used for enrichment, but the global mutable state is a latent bug).

---

### Category: Simplicity (Principles 19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are straightforward. The `rg/` subpackage is especially clean: each module has a clear purpose and simple implementations. The `generated/` module provides static fallback data in the simplest possible format (tuples of tuples).

**Findings:**
- `tools/cq/search/enrichment/core.py:136-205` defines four large `frozenset` constants (`_PY_RESOLUTION_KEYS`, `_PY_BEHAVIOR_KEYS`, `_PY_STRUCTURAL_KEYS`, `_PY_FLAT_EXCLUDED_KEYS`) that replicate the field schema knowledge. This is a manual classification approach that could be simplified by using typed structs whose field names ARE the classification.
- `tools/cq/search/enrichment/core.py:212-241` (`_partition_python_payload_fields`) is a 30-line function that iterates over dict keys and classifies them into 5 buckets using set membership checks with a fallback to `structural`. This classification-by-exclusion pattern is fragile: adding a new field requires updating the correct frozenset.

**Suggested improvement:**
If typed enrichment structs are adopted (P9), the classification constants and partitioning function become unnecessary. The field names on each struct implicitly define their category.

**Effort:** small (contingent on P9)
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 3/3

**Current state:**
No speculative abstractions observed. All code serves current needs. The `generated/` fallback data is a minimal subset. The enrichment contract wrappers (`PythonEnrichmentPayload`, `RustEnrichmentPayload`) are thin and purposeful. There is no unused infrastructure or over-designed abstraction layer.

**Findings:**
No violations found.

**Effort:** -
**Risk if unaddressed:** -

---

#### P21. Least Astonishment -- Alignment: 2/3

**Current state:**
Most APIs behave as expected. The `rg/` functions have clear names (`find_files_with_pattern`, `find_call_candidates`, `search_content`). The enrichment functions have descriptive names (`enrich_python_context`, `enrich_python_context_by_byte_range`).

**Findings:**
- `tools/cq/search/semantic/models.py:506` and `tools/cq/search/semantic/front_door.py:501` both define public functions named `fail_open` and `enrich_semantics`. A reader importing from `semantic` would encounter name collisions. The `models.py` version creates a simple default outcome; the `front_door.py` version runs the full caching pipeline. Same name, very different behavior.
- `tools/cq/search/python/extractors.py:187`: `_truncation_tracker` is a global list that affects the output of otherwise-independent function calls. A reader would not expect that calling `enrich_python_context` on match A could affect the `truncated_fields` output of a subsequent call on match B.

**Suggested improvement:**
Rename `models.py`'s `fail_open` to `make_empty_outcome` or `default_outcome` to distinguish it from `front_door.py`'s `fail_open`. Make `_truncation_tracker` per-call (as suggested in P18).

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and Version Public Contracts -- Alignment: 2/3

**Current state:**
All modules define `__all__` exports. Serializable contracts use the `V1` suffix convention (`RgRunSettingsV1`, `RgProcessResultV1`, `PythonNodeEnrichmentSettingsV1`, `SemanticOutcomeV1`). The `CqOutputStruct` base ensures msgspec integration.

**Findings:**
- `tools/cq/search/enrichment/contracts.py:12`: `EnrichmentMeta` does not carry a version suffix despite being a serializable contract. This inconsistency could cause confusion about stability guarantees.
- `tools/cq/search/enrichment/contracts.py:24,31`: `PythonEnrichmentPayload` and `RustEnrichmentPayload` also lack version suffixes.
- `tools/cq/search/rg/collector.py:36`: `MatchPayload` is a public export (in `__all__`) but is a runtime-only dataclass, not a versioned contract. This is acceptable but the naming could be clearer.

**Suggested improvement:**
Add `V1` suffixes to `EnrichmentMeta`, `PythonEnrichmentPayload`, and `RustEnrichmentPayload` to align with the codebase convention for serializable contracts. Alternatively, document the versioning policy for these types.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (Principles 23-24)

#### P23. Design for Testability -- Alignment: 2/3

**Current state:**
The `rg/codec.py` accessor functions are trivially testable (pure functions on typed inputs). The `enrichment/core.py` utilities are pure and testable. Request dataclasses are easily constructable for testing.

**Findings:**
- `tools/cq/search/python/analysis_session.py:22`: `_SESSION_CACHE` is module-level global state. Testing requires calling `clear_python_analysis_sessions()` in setup/teardown, and parallel tests can interfere with each other.
- `tools/cq/search/rust/enrichment.py:40`: Same issue with `_AST_CACHE`.
- `tools/cq/search/python/extractors.py:187`: `_truncation_tracker` is module-level mutable state that leaks between tests.
- `tools/cq/search/python/analysis_session.py:116,141`: Deferred imports of `get_sg_root` and `get_node_index` from `pipeline/classifier` make it difficult to test `PythonAnalysisSession` in isolation without monkeypatching the import target.
- `tools/cq/search/python/extractors.py:252`: `_AST_CACHE` is another module-level dict cache that must be cleared between tests.

**Suggested improvement:**
Accept cache and factory dependencies as parameters (with defaults) instead of hardcoding module-level dicts. For `PythonAnalysisSession`, accept a `sg_root_factory: Callable | None = None` parameter. For `_truncation_tracker`, move to per-call state as recommended in P18.

**Effort:** medium
**Risk if unaddressed:** medium -- module-level caches cause test isolation issues and make it harder to add concurrent enrichment.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
`PythonAnalysisSession` records `stage_timings_ms` and `stage_errors` per session, providing per-file observability into enrichment performance. The enrichment contracts include `enrichment_status`, `enrichment_sources`, and `degrade_reason` for degradation tracking. `_shared/search_contracts.py` defines `EnrichmentTelemetry` for pipeline-level observability.

**Findings:**
- Error handling throughout the enrichment pipeline uses bare `except` tuples (e.g., `tools/cq/search/python/extractors.py:91-98` defines `_ENRICHMENT_ERRORS` and catches them silently) without structured logging. Errors are recorded in `stage_errors` as type names only (e.g., `"RuntimeError"`) without messages or tracebacks.
- `tools/cq/search/semantic/diagnostics.py` defines cross-language diagnostic contracts but these are assembled post-hoc rather than emitted at the point of failure.
- `tools/cq/search/rg/runner.py:262-263` captures stderr from ripgrep but does not log it; it is only available as a field on the result object.

**Suggested improvement:**
Add structured error recording that captures exception messages (not just type names) in `stage_errors`. Consider emitting `structlog` or `logging` entries at `WARNING` level when enrichment degrades, referencing the file path and stage. This aids debugging without requiring access to the full result object.

**Effort:** small
**Risk if unaddressed:** low -- the current level of observability is adequate for development but could be improved for production debugging.

---

## Cross-Cutting Themes

### Theme 1: Untyped Enrichment Payloads as the Central Design Debt

**Description:** The most impactful systemic issue is that enrichment payloads flow as `dict[str, object]` throughout the system. This single decision cascades into violations of P7 (DRY -- key set constants duplicate schema knowledge), P8 (loose contracts), P9 (validate rather than parse), P10 (illegal states representable), P14 (Demeter violations on nested dicts), P15 (ask rather than tell), and P19 (complex partitioning logic).

**Root cause:** The enrichment pipeline was likely designed for flexibility during rapid iteration, where adding a new field to a dict is zero-cost. The typed envelope (`EnrichmentMeta`) was added later but the core `data` field remained untyped.

**Affected principles:** P7, P8, P9, P10, P14, P15, P19

**Suggested approach:** Define typed msgspec structs for each enrichment data section (resolution, behavior, structural). Build these structs at the enrichment boundary in `extractors.py` and `rust/enrichment.py`. Consumers receive typed structs instead of dicts. This is a large effort but resolves the highest number of principle violations simultaneously.

### Theme 2: Reverse Dependency on Pipeline Layer

**Description:** Eight modules across `_shared/`, `rg/`, `rust/`, `python/`, and `semantic/` import types or functions from `pipeline/`. This inverts the intended layering where `pipeline/` orchestrates the lanes, not the other way around.

**Root cause:** `QueryMode` and `SearchLimits` are natural "vocabulary types" that belong in the foundation layer but were defined in the pipeline layer. Similarly, `get_sg_root` and `get_node_index` are utility functions that happen to live in `pipeline/classifier.py`.

**Affected principles:** P4, P5, P12

**Suggested approach:** Move `QueryMode` and `SearchLimits` to `_shared/types.py`. Define a `SgRootProvider` protocol in `_shared/` and inject it into lane modules. This can be done incrementally without changing any behavior.

### Theme 3: Module-Level Mutable State

**Description:** Four module-level mutable caches exist: `_SESSION_CACHE` in `analysis_session.py`, `_AST_CACHE` in both `extractors.py` and `rust/enrichment.py`, and `_truncation_tracker` in `extractors.py`. These create test isolation issues, concurrency hazards, and non-deterministic behavior.

**Root cause:** LRU caching was added for performance without introducing a cache abstraction. Each module independently implemented the same eviction pattern.

**Affected principles:** P1, P7, P18, P23

**Suggested approach:** Extract a simple `BoundedCache[K, V]` class into `_shared/` that encapsulates the eviction logic. This resolves the DRY violation (P7), improves information hiding (P1), enables test injection (P23), and eliminates the `_truncation_tracker` global (P18).

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Extract `_node_byte_span`, `_ast_node_priority`, `_iter_nodes_with_parents` into `python/ast_helpers.py` | small | Eliminates 50+ lines of exact duplication |
| 2 | P18 (Determinism) | Move `_truncation_tracker` from module-level into `_PythonEnrichmentState` | small | Eliminates global mutable state and concurrency hazard |
| 3 | P21 (Least Astonishment) | Rename `models.py:fail_open` to `make_empty_outcome` | small | Removes name collision with `front_door.py:fail_open` |
| 4 | P11 (CQS) | Change `attach_rust_evidence` to return `None` since it mutates in place | small | Clarifies mutation intent |
| 5 | P7 (DRY) | Remove trivial wrappers `_source_hash` and `_node_text` in `rust/enrichment.py` | small | Eliminates unnecessary indirection |

## Recommended Action Sequence

1. **Extract shared AST helpers** (P7) -- Create `tools/cq/search/python/ast_helpers.py` with `node_byte_span`, `ast_node_priority`, `iter_nodes_with_parents`. Update imports in `analysis_session.py` and `resolution_support.py`. No behavioral change.

2. **Scope truncation tracker** (P18, P23) -- Move `_truncation_tracker` into `_PythonEnrichmentState` dataclass. Clear it per-call. Eliminates global mutable state.

3. **Consolidate type coercion helpers** (P7) -- Move `_as_int`, `_as_optional_str` into `_shared/core.py` or `enrichment/core.py`. Update imports in `objects/resolve.py`, `python/evidence.py`, `python/resolution_index.py`.

4. **Move vocabulary types to shared layer** (P5) -- Relocate `QueryMode` and `SearchLimits` definitions (or re-export aliases) to `_shared/types.py`. Update all 8 importing modules to use the shared location. This inverts the dependency direction.

5. **Extract LRU cache utility** (P7, P1, P23) -- Create `_shared/bounded_cache.py` with a `BoundedCache[K, V]` class. Replace manual eviction in `analysis_session.py`, `extractors.py`, and `rust/enrichment.py`.

6. **Fix naming collisions** (P21) -- Rename `semantic/models.py:fail_open` and `enrich_semantics` to `make_empty_outcome` and `enrich_semantics_simple` (or remove them if `front_door.py` is the canonical entry point).

7. **Type enrichment status** (P10) -- Change `EnrichmentStatus = str` to `Literal["applied", "degraded", "skipped"]`. Add version suffixes to `EnrichmentMeta`, `PythonEnrichmentPayload`, `RustEnrichmentPayload`.

8. **Define typed enrichment data structs** (P9, P15, P19) -- Define `PythonResolutionFacts`, `PythonBehaviorFacts`, `PythonStructuralFacts` as msgspec structs. Build them at the enrichment boundary. This is the largest change and should be done last, after items 1-7 are stable.

9. **Inject SgRoot/NodeIndex providers** (P5, P12, P23) -- Define a `SgRootProvider` protocol in `_shared/`. Accept it as a parameter in `PythonAnalysisSession` and `semantic/front_door.py`. Default to the current deferred import behavior.

10. **Decompose extractors.py** (P2, P3) -- Split into stage-specific modules under `python/stages/`. This should be done after the typed enrichment structs (item 8) to avoid rework.
