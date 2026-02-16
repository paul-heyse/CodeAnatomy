# Design Review: CQ Smart Search Pipeline and Enrichment

**Date:** 2026-02-16
**Scope:** `tools/cq/search/pipeline/`, `tools/cq/search/python/`, `tools/cq/search/enrichment/`, `tools/cq/search/rust/`, `tools/cq/search/semantic/`, `tools/cq/search/objects/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 44

## Executive Summary

The Smart Search pipeline demonstrates strong architectural layering with clear phase boundaries (candidate -> classification -> enrichment -> assembly -> rendering) and robust fail-open behavior throughout. Type contracts via `msgspec.Struct` are well-established at boundaries. However, the system suffers from three systemic issues: (1) pervasive use of `dict[str, object]` as the internal payload representation undermines the typed contracts defined in `enrichment/contracts.py` and `enrichment/python_facts.py`/`rust_facts.py`, (2) language-specific dispatch is handled via string comparison (`lang == "python"` / `lang == "rust"`) at 24+ callsites rather than through a port/adapter pattern, and (3) several private helper functions are duplicated across modules (`_normalize_python_semantic_degradation_reason`, `_count_mapping_rows`, `_resolve_search_worker_count`, `_string`). The highest-impact improvements would be introducing a `LanguageEnrichmentPort` protocol to consolidate language dispatch and progressively replacing `dict[str, object]` payloads with the typed fact structs already defined but unused.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | medium | Module-level mutable caches exposed as globals; `__init__.py` lazy proxy leaks internals |
| 2 | Separation of concerns | 2 | medium | low | `smart_search.py` mixes orchestration, building, and rendering; pipeline phases otherwise clear |
| 3 | SRP (one reason to change) | 1 | large | medium | `smart_search.py` at ~2049 LOC is the God module; changes for 5+ different reasons |
| 4 | High cohesion, low coupling | 2 | medium | low | Well-cohesive sub-packages; coupling through `dict[str, object]` payloads is too loose |
| 5 | Dependency direction | 2 | small | low | Core types in `smart_search_types.py` are clean; but `front_door.py` pulls in classifier |
| 6 | Ports & Adapters | 1 | medium | medium | No language enrichment port; 24+ string-comparison dispatch sites |
| 7 | DRY (knowledge, not lines) | 1 | small | medium | 4 distinct function duplications across modules |
| 8 | Design by contract | 2 | medium | low | Enrichment status ternary well-defined; postconditions implicit in most enrichment functions |
| 9 | Parse, don't validate | 1 | large | medium | `dict[str, object]` payloads repeatedly validated with `isinstance` checks deep in call chains |
| 10 | Make illegal states unrepresentable | 1 | large | medium | Typed fact structs exist but unused; `EnrichedMatch` allows contradictory field combinations |
| 11 | CQS | 2 | small | low | `attach_rust_evidence` and `accumulate_*` mutate-and-return; most other functions clean |
| 12 | DI + explicit composition | 2 | medium | low | `BoundedCache` injected properly; module-level singletons for caches less explicit |
| 13 | Prefer composition over inheritance | 3 | - | - | No inheritance hierarchies; all composition via dataclasses and structs |
| 14 | Law of Demeter | 1 | medium | low | Deep payload navigation: `payload.get("meta").get("stage_status")` chains throughout |
| 15 | Tell, don't ask | 1 | medium | medium | Callers constantly inspect payload internals to make decisions |
| 16 | Functional core, imperative shell | 2 | small | low | Enrichment functions are mostly pure transforms; caches are the impure shell |
| 17 | Idempotency | 3 | - | - | Re-enrichment produces same results; caches keyed by content hash |
| 18 | Determinism / reproducibility | 3 | - | - | Sorted outputs; content-hash keying; deterministic merge policy |
| 19 | KISS | 2 | medium | low | Complexity within budget overall; `smart_search.py` exceeds it |
| 20 | YAGNI | 2 | small | low | `python_facts.py` / `rust_facts.py` structs defined but unused in pipeline |
| 21 | Least astonishment | 2 | small | low | `_LanguageSearchResult` exported as `LanguageSearchResult` alias creates naming confusion |
| 22 | Declare and version public contracts | 2 | small | low | V1 suffixes on output structs; `__all__` everywhere; some private names in `__all__` |
| 23 | Design for testability | 2 | medium | low | Pure functions testable; module-level caches require monkeypatching to test |
| 24 | Observability | 2 | small | low | Enrichment telemetry comprehensive; no structured logging within individual stages |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
Module-level mutable caches are exposed as module globals in `classifier_runtime.py`. The `pipeline/__init__.py` uses a lazy `__getattr__` pattern that maps names to `(module, attribute)` tuples, creating an implicit contract surface. Module-level mutable state in `search_object_view_store.py` exposes a global registry dict.

**Findings:**
- `tools/cq/search/pipeline/classifier_runtime.py:22-27`: Six module-level mutable caches (`_sg_cache`, `_source_cache`, `_def_lines_cache`, `_symtable_cache`, `_record_context_cache`, `_node_index_cache`) are `dict` globals accessible to any importer. No encapsulation behind a class or factory.
- `tools/cq/search/pipeline/search_object_view_store.py:12`: `_SEARCH_OBJECT_VIEW_REGISTRY: dict[str, SearchObjectResolvedViewV1]` is a module-level mutable global with `register_search_object_view` and `pop_search_object_view_for_run` as its only accessors -- but the dict itself is importable.
- `tools/cq/search/pipeline/__init__.py:8-31`: Lazy `__getattr__` maps attribute names to `(module_path, attr_name)` tuples. The mapped types include `SearchContext` aliased to `SmartSearchContext` from `contracts`, which creates an extra indirection layer without clear benefit.
- `tools/cq/search/rust/enrichment.py:41-43`: `_AST_CACHE: BoundedCache[str, tuple[SgRoot, str]]` is a module-level mutable cache. While `BoundedCache` provides better encapsulation than raw dicts, it is still a module global.
- `tools/cq/search/pipeline/smart_search.py`: Exports private-prefixed names in `__all__` (e.g., `_resolve_search_worker_count` is available via the telemetry module, creating ambiguity about the true owner).

**Suggested improvement:**
Wrap `classifier_runtime.py` caches in a `ClassifierRuntimeCache` class with `get_*`/`clear_*` methods, and inject it via the `SmartSearchContext` or a pipeline-level factory. Replace `_SEARCH_OBJECT_VIEW_REGISTRY` with a scoped registry passed through the pipeline context. This consolidates lifecycle management and makes cache invalidation explicit.

**Effort:** medium
**Risk if unaddressed:** medium -- Module-level mutable state creates hidden coupling between pipeline runs and makes concurrent usage unsafe.

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
The pipeline has clear macro-level separation: `pipeline/` for orchestration, `python/` for Python evidence, `rust/` for Rust evidence, `enrichment/` for shared normalization, `semantic/` for front-door insight, and `objects/` for resolution/rendering. However, within `pipeline/`, the separation breaks down.

**Findings:**
- `tools/cq/search/pipeline/smart_search.py` (~2049 lines): This single file contains candidate collection, classification orchestration, summary building, section building, follow-up suggestion generation, and language partition merging. It serves as both the orchestrator and the implementor of multiple pipeline phases.
- `tools/cq/search/pipeline/assembly.py:1-720`: Assembly is well-separated from orchestration, handling insight card construction, neighborhood preview, and primary target resolution.
- `tools/cq/search/semantic/models.py:1-441`: Combines semantic contract definitions, path resolution helpers, retry logic, and workspace root detection in a single module.

**Suggested improvement:**
Extract from `smart_search.py`: (1) summary building into `smart_search_summary.py`, (2) section building into `smart_search_sections.py`, (3) follow-up suggestions into `smart_search_followups.py`. The main `smart_search.py` should be a thin orchestrator that calls these phases.

**Effort:** medium
**Risk if unaddressed:** low -- Code is functional but the large module makes navigation and targeted changes difficult.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`smart_search.py` changes for at least 5 distinct reasons: candidate generation logic, classification tuning, summary format changes, section output changes, and follow-up suggestion logic. Other modules have reasonable SRP.

**Findings:**
- `tools/cq/search/pipeline/smart_search.py`: ~2049 LOC, changes for candidate collection, classification dispatch, summary schema, section rendering, and follow-up command generation.
- `tools/cq/search/pipeline/python_semantic.py`: ~855 LOC, handles both prefetch orchestration and enrichment attachment -- two distinct responsibilities with different change drivers.
- `tools/cq/search/semantic/models.py`: ~441 LOC, mixes contract definitions (structs), semantic plane building, path resolution, and retry utilities.

**Suggested improvement:**
Split `smart_search.py` into 3-4 focused modules as described in P2. Split `models.py` into `contracts.py` (structs), `planes.py` (semantic plane building), and `workspace.py` (path resolution + retry).

**Effort:** large
**Risk if unaddressed:** medium -- The monolithic module makes it harder to reason about change impact and increases merge conflict probability.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Sub-packages are cohesive: `python/` groups all Python evidence, `rust/` groups all Rust evidence, `objects/` groups resolution and rendering. Coupling between packages is moderate but mediated through `dict[str, object]` payloads, which is simultaneously too loose (no type safety) and too tight (callers must know internal key names).

**Findings:**
- `tools/cq/search/pipeline/smart_search_telemetry.py:50-88`: `empty_enrichment_telemetry()` defines a deeply nested dict structure with 30+ keys hardcoded as string literals. Any change to this structure requires coordinated updates in `accumulate_python_enrichment`, `accumulate_rust_enrichment`, and `build_enrichment_telemetry`.
- `tools/cq/search/objects/resolve.py:143-163`: `_payload_views` navigates `EnrichedMatch` fields to extract `python_enrichment.resolution`, `python_enrichment.structural`, `python_enrichment.agreement` -- coupling the resolution module to the internal structure of the enrichment payload.
- 99 occurrences of `dict[str, object]` in `pipeline/` alone (measured via grep).

**Suggested improvement:**
Define a `TelemetryAccumulator` class that encapsulates the telemetry structure and provides typed `accumulate_python`, `accumulate_rust`, and `finalize` methods. Replace the most frequently traversed `dict[str, object]` payloads with the existing typed structs from `enrichment/python_facts.py` and `enrichment/rust_facts.py`.

**Effort:** medium
**Risk if unaddressed:** low -- Current approach works but makes refactoring risky and increases the chance of runtime key-mismatch bugs.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Core types flow outward correctly: `smart_search_types.py` defines the data types, other modules import from it. `enrichment/contracts.py` defines boundary contracts. However, some reverse dependencies exist.

**Findings:**
- `tools/cq/search/semantic/front_door.py:34-36`: Imports `get_sg_root` from `tools.cq.search.pipeline.classifier` and `enrich_python_context_by_byte_range` from `tools.cq.search.python.extractors`. The semantic front-door (a higher-level concern) depends directly on lower-level pipeline internals and language-specific extractors.
- `tools/cq/search/rust/enrichment.py:25`: Imports `get_node_index` from `tools.cq.search.pipeline.classifier`. The language-specific enrichment module depends on the pipeline classifier, inverting the expected direction (pipeline should depend on enrichment, not vice versa).
- `tools/cq/search/objects/resolve.py:19`: Imports `evaluate_python_semantic_signal_from_mapping` from `tools.cq.search.python.evidence`. Object resolution (general) depends on Python-specific evidence evaluation.

**Suggested improvement:**
Move `get_node_index` and `get_sg_root` to a shared `_shared/ast_cache.py` module that both pipeline and language-specific enrichment can import without circular dependency. Abstract `evaluate_python_semantic_signal_from_mapping` behind a protocol in objects that the Python evidence module implements.

**Effort:** small
**Risk if unaddressed:** low -- Current lazy imports prevent circular import errors, but the conceptual dependency direction is inverted.

---

#### P6. Ports & Adapters -- Alignment: 1/3

**Current state:**
There is no language enrichment port/protocol. Language dispatch is handled via string comparison at 24+ callsites across 9 files. Each callsite independently checks `lang == "python"` or `language == "rust"` and calls language-specific functions directly.

**Findings:**
- `tools/cq/search/pipeline/smart_search.py`: 6 instances of language string comparison for dispatching to Python vs Rust enrichment paths.
- `tools/cq/search/pipeline/smart_search_telemetry.py:308,313-316`: `build_enrichment_telemetry` dispatches `accumulate_python_enrichment` vs `accumulate_rust_enrichment` via `match.language == "python"`.
- `tools/cq/search/semantic/front_door.py:215-217`: `_execute_provider` dispatches `_execute_python_provider` vs `_execute_rust_provider` via `request.language == "python"`.
- `tools/cq/search/semantic/models.py:234-235`: `build_static_semantic_planes` dispatches `_python_diagnostics` vs `_rust_diagnostics` via `language == "python"`.
- `tools/cq/search/pipeline/search_semantic.py`: 5 instances of language dispatch within semantic state resolution.

**Suggested improvement:**
Define a `LanguageEnrichmentPort` protocol in `enrichment/contracts.py`:
```python
class LanguageEnrichmentPort(Protocol):
    def enrich_by_byte_range(self, source: str, *, byte_start: int, byte_end: int) -> dict[str, object]: ...
    def accumulate_telemetry(self, lang_bucket: dict[str, object], payload: dict[str, object]) -> None: ...
    def build_diagnostics(self, payload: Mapping[str, object]) -> list[dict[str, object]]: ...
```
Register Python and Rust adapters in a `LANGUAGE_ADAPTERS: dict[QueryLanguage, LanguageEnrichmentPort]` mapping. Replace the 24+ dispatch sites with adapter lookup.

**Effort:** medium
**Risk if unaddressed:** medium -- Adding a third language (e.g., TypeScript, Go) would require touching 24+ files. The current approach also makes it easy to miss one dispatch site when modifying language-specific behavior.

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
Four distinct private helper functions are duplicated across modules, and a payload key schema (`_string` helper) is independently defined in two modules.

**Findings:**
- `tools/cq/search/pipeline/python_semantic.py` and `tools/cq/search/pipeline/search_semantic.py`: Both define `_normalize_python_semantic_degradation_reason` and `_count_mapping_rows` with identical logic.
- `tools/cq/search/pipeline/smart_search.py` and `tools/cq/search/pipeline/smart_search_telemetry.py`: Both define `_resolve_search_worker_count`.
- `tools/cq/search/semantic/models.py:91` and `tools/cq/search/rust/extensions.py:89`: Both define `_string(value: object) -> str | None` with identical `strip()`-and-return-or-None logic.
- `tools/cq/search/pipeline/classifier.py` and `tools/cq/search/pipeline/classifier_runtime.py`: `_record_contains` (classifier.py) and `_span_contains` (classifier_runtime.py) implement similar containment check logic.
- `tools/cq/search/enrichment/core.py:199-211` and `tools/cq/search/rust/enrichment.py:337-345`: Both define the same set of excluded metadata keys for Rust payload normalization (`language`, `enrichment_status`, `enrichment_sources`, `degrade_reason`, `payload_size_hint`, `dropped_fields`, `truncated_fields`).

**Suggested improvement:**
Move the duplicated functions to their natural shared home:
1. `_normalize_python_semantic_degradation_reason` and `_count_mapping_rows` into a new `pipeline/_semantic_helpers.py`.
2. `_resolve_search_worker_count` into one canonical location (likely `smart_search.py`, removing it from `smart_search_telemetry.py`).
3. `_string` into `_shared/core.py` or `enrichment/core.py`.
4. Define `_ENRICHMENT_METADATA_KEYS` once in `enrichment/contracts.py` and import it.

**Effort:** small
**Risk if unaddressed:** medium -- Divergent evolution of duplicated logic leads to subtle behavioral inconsistencies.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
The enrichment status ternary (`applied` / `degraded` / `skipped`) is well-defined as `EnrichmentStatus` in `enrichment/contracts.py`. Preconditions are generally checked (e.g., byte range validation in `enrich_rust_context_by_byte_range`). However, most enrichment functions lack explicit postcondition documentation.

**Findings:**
- `tools/cq/search/enrichment/contracts.py:7`: `EnrichmentStatus = Literal["applied", "degraded", "skipped"]` -- well-defined contract.
- `tools/cq/search/rust/enrichment.py:572-576`: `enrich_rust_context_by_byte_range` validates `byte_start < 0`, `byte_end <= byte_start`, and `byte_end > len(source_bytes)` -- good precondition checking.
- `tools/cq/search/enrichment/core.py:87-111`: `enforce_payload_budget` clearly documents its return contract (dropped keys + resulting size).
- `tools/cq/search/pipeline/classifier.py:145-180`: `classify_from_node` returns `NodeClassification | None` -- the None case is documented but callers must handle it defensively.

**Suggested improvement:**
Add postcondition assertions or documentation to key enrichment functions: `normalize_python_payload` should guarantee all returned payloads contain `meta.enrichment_status`; `build_enrichment_telemetry` should guarantee all language buckets are present.

**Effort:** medium
**Risk if unaddressed:** low -- Implicit contracts work today but make debugging harder when payloads drift.

---

#### P9. Parse, don't validate -- Alignment: 1/3

**Current state:**
Payloads flow through the pipeline as `dict[str, object]` and are repeatedly validated with `isinstance` checks at every consumption site. The `enrichment/contracts.py` and `enrichment/python_facts.py` / `enrichment/rust_facts.py` define typed representations, but the pipeline never converts to them.

**Findings:**
- `tools/cq/search/pipeline/smart_search_telemetry.py:103-108`: `accumulate_stage_status` validates every key-value pair with `isinstance(stage, str)`, `isinstance(stage_state, str)`, `isinstance(stage_bucket, dict)`, and a membership check in `{"applied", "degraded", "skipped"}` -- all of which could be guaranteed by a typed struct.
- `tools/cq/search/objects/resolve.py:143-163`: `_payload_views` performs 6 `isinstance` checks to extract typed views from `EnrichedMatch.python_enrichment` dict -- these checks recur everywhere the payload is consumed.
- `tools/cq/search/enrichment/core.py:114-139`: `_meta_from_flat` manually extracts and validates 7 fields from a flat dict to construct an `EnrichmentMeta` struct, then the struct is immediately decomposed back into a dict in `normalize_python_payload`.
- `tools/cq/search/enrichment/python_facts.py` and `tools/cq/search/enrichment/rust_facts.py`: Define comprehensive typed fact structs (`PythonEnrichmentFacts`, `RustEnrichmentFacts`) that are **never used** in the actual pipeline.

**Suggested improvement:**
At the enrichment boundary (after each language's enrichment function returns), parse the `dict[str, object]` into the typed fact struct once. Pass the typed struct through the pipeline. This eliminates the 100+ downstream `isinstance` checks and leverages the fact structs already defined.

**Effort:** large
**Risk if unaddressed:** medium -- The current approach works but is fragile: misspelling a key name produces a silent None, not a type error.

---

#### P10. Make illegal states unrepresentable -- Alignment: 1/3

**Current state:**
`EnrichedMatch` allows contradictory field combinations: a match with `language="rust"` can have `python_enrichment` populated and `rust_tree_sitter` as None. The typed fact structs exist to prevent this but are unused.

**Findings:**
- `tools/cq/search/pipeline/smart_search_types.py:174-221`: `EnrichedMatch` has fields for both `rust_tree_sitter: dict[str, object] | None` and `python_enrichment: dict[str, object] | None`, plus `python_semantic_enrichment: dict[str, object] | None`. Nothing prevents a Rust match from having Python enrichment or vice versa.
- `tools/cq/search/pipeline/smart_search_types.py:84-96`: `SearchSummaryInputs` has both `stats` and `language_stats` with no constraint that `stats` should be the aggregate of `language_stats`.
- `tools/cq/search/pipeline/smart_search_types.py:141-171`: `SearchStats` has `truncated`, `max_files_hit`, and `max_matches_hit` as independent booleans, though `truncated` should be the OR of the other two.

**Suggested improvement:**
Use a discriminated union for language-specific enrichment on `EnrichedMatch`: either a `PythonEnrichment` or `RustEnrichment` dataclass keyed by `language`, rather than parallel optional fields. Derive `truncated` from `max_files_hit | max_matches_hit` as a property.

**Effort:** large
**Risk if unaddressed:** medium -- Contradictory state combinations create subtle bugs that are hard to trace.

---

#### P11. CQS (Command Query Separation) -- Alignment: 2/3

**Current state:**
Most functions follow CQS well. A few enrichment functions both mutate their input and return it, creating ambiguity about ownership.

**Findings:**
- `tools/cq/search/rust/evidence.py:153-161`: `attach_macro_expansion_evidence` mutates `payload` in-place AND returns it: `payload["macro_expansions"] = ...; return payload`. Callers cannot tell if they should use the return value or rely on the mutation.
- `tools/cq/search/rust/evidence.py:228-236`: `attach_rust_module_graph` same pattern: mutates and returns.
- `tools/cq/search/rust/evidence.py:239-245`: `attach_rust_evidence` chains two mutate-and-return calls: `return attach_rust_module_graph(attach_macro_expansion_evidence(payload))`.
- `tools/cq/search/enrichment/core.py:87-111`: `enforce_payload_budget` mutates `payload` via `pop()` AND returns `(dropped, size)` -- mixed command/query. The mutation is documented but the dual return is unexpected.

**Suggested improvement:**
Choose one pattern: either mutate in-place and return `None` (command), or create a new dict and return it (query). For `attach_*` functions, return `None` since the mutation is the point. For `enforce_payload_budget`, the dual return is acceptable since it returns metadata about the mutation, but document clearly that the input is modified.

**Effort:** small
**Risk if unaddressed:** low -- The current pattern works but can lead to double-mutation bugs if callers chain operations.

---

### Category: Composition (12-15)

#### P12. DI + explicit composition -- Alignment: 2/3

**Current state:**
`BoundedCache` is injected properly in the Rust enrichment module. Pipeline context (`SmartSearchContext` / `SearchConfig`) serves as a value-based configuration carrier. However, module-level cache singletons bypass DI.

**Findings:**
- `tools/cq/search/rust/enrichment.py:41-43`: `_AST_CACHE` is a module-level `BoundedCache` singleton. Not injectable; tests must call `clear_rust_enrichment_cache()`.
- `tools/cq/search/pipeline/classifier_runtime.py:22-27`: Six module-level `dict` caches with no injection point. `clear_caches()` function exists but must be called explicitly.
- `tools/cq/search/semantic/front_door.py:52-67`: `_PipelineContext` is a well-structured DI container that carries policy, cache backend, budget, and TTL. This is the pattern the other modules should follow.

**Suggested improvement:**
Follow the `_PipelineContext` pattern from `front_door.py` for the classifier runtime caches. Create a `ClassifierContext` or `EnrichmentSession` that owns the caches and is created per-pipeline-run.

**Effort:** medium
**Risk if unaddressed:** low -- Module-level caches work in single-threaded use but complicate testing and concurrent execution.

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
The codebase uses no inheritance hierarchies in the reviewed scope. All type composition is via dataclasses, `msgspec.Struct`, and function composition. `CqStruct` and `CqOutputStruct` are base structs for serialization behavior, not behavioral inheritance.

**Findings:**
None -- the principle is well-satisfied.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
The `dict[str, object]` payload representation forces deep navigation chains throughout the codebase. Callers must know the internal structure of payloads they receive.

**Findings:**
- `tools/cq/search/pipeline/smart_search_telemetry.py:143-153`: `accumulate_python_enrichment` navigates `payload.get("meta")`, then `meta.get("stage_status")`, then `lang_bucket.get("stages")`, then `stages_bucket.get(stage)`, then `stage_bucket.get(stage_state, 0)` -- 5 levels of dict navigation.
- `tools/cq/search/objects/resolve.py:154-163`: `_payload_views` navigates `match.python_enrichment.get("resolution")`, `python_enrichment.get("structural")`, `python_enrichment.get("agreement")` -- reaching through the enrichment payload to extract sub-dicts.
- `tools/cq/search/semantic/models.py:112-141`: `_python_diagnostics` navigates `payload.get("parse_quality")`, then `parse_quality.get("error_nodes")`, then `payload.get("cst_diagnostics")` in a single function -- reaching into multiple unrelated payload sub-structures.
- `tools/cq/search/objects/resolve.py:496-507`: `_resolution_has_tree_sitter_source` navigates `resolution_payload.get("qualified_name_candidates")`, iterates rows, checks `row.get("source") == "tree_sitter"` -- 3 levels of traversal.

**Suggested improvement:**
Define accessor methods or typed wrappers for the most commonly traversed payload shapes. For example, the Python enrichment payload's `resolution`, `structural`, `behavior`, `agreement`, and `meta` sub-dicts should be accessible via typed accessors on a `PythonEnrichmentView` class. This reduces navigation depth from 4-5 to 1-2.

**Effort:** medium
**Risk if unaddressed:** low -- The current approach works but makes the code harder to read and refactor.

---

#### P15. Tell, don't ask -- Alignment: 1/3

**Current state:**
Callers frequently inspect payload internals to decide what to do next, rather than asking the payload to perform the action.

**Findings:**
- `tools/cq/search/pipeline/smart_search_telemetry.py:304-316`: `build_enrichment_telemetry` inspects `match.language` to decide which enrichment field to read, then inspects the payload to decide which accumulator to call. The match could encapsulate this dispatch.
- `tools/cq/search/objects/resolve.py:411-456`: `_coverage_for_match` inspects `semantic_payload.get("coverage")`, then `coverage.get("reason")`, then calls `evaluate_python_semantic_signal_from_mapping`, then inspects the resolution payload -- all to determine a `coverage_level` string. The enrichment payload should provide a `coverage_level` property.
- `tools/cq/search/objects/resolve.py:598-655`: `_infer_object_kind` chains through 3 fallback functions (`_kind_from_definition_target`, `_kind_from_structural_payload`, `_kind_from_category`), each of which asks the payload for raw data and interprets it locally.

**Suggested improvement:**
Add computed properties to the enrichment payload wrappers: `coverage_level`, `inferred_kind`, `primary_qualified_name`. Move the inference logic into the data structure rather than scattering it across consumers.

**Effort:** medium
**Risk if unaddressed:** medium -- Inference logic scattered across consumers leads to inconsistent interpretation of the same data.

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Enrichment functions are mostly pure transforms. The impure shell (caching, file I/O, process pool dispatch) is concentrated at the pipeline level. Some impurity leaks into enrichment via module-level caches.

**Findings:**
- `tools/cq/search/pipeline/smart_search_telemetry.py:165-184`: `attach_enrichment_cache_stats` performs lazy imports and calls runtime cache stats functions -- impure I/O injected into what is otherwise a pure telemetry aggregation module.
- `tools/cq/search/rust/enrichment.py:80-94`: `_get_sg_root` reads from and writes to a module-level cache -- impure state mixed into what looks like a pure parse function.
- `tools/cq/search/semantic/front_door.py:69-119`: `run_language_semantic_enrichment` properly isolates I/O (file reads, cache probe, cache write, worker pool) in the shell. Good pattern.

**Suggested improvement:**
Extract `attach_enrichment_cache_stats` to the pipeline shell layer rather than embedding it in the telemetry module. Pass cache stats as parameters to `build_enrichment_telemetry` rather than fetching them internally.

**Effort:** small
**Risk if unaddressed:** low -- The impurity is contained and documented.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Re-enrichment produces the same results given the same source and byte range. Cache keys include content hashes. Telemetry accumulation is additive. Object resolution produces deterministic IDs via `stable_digest24`.

**Findings:**
None -- the principle is well-satisfied.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Outputs are sorted deterministically (occurrences by file/line/col, summaries by count/symbol/id). Content-hash keying ensures cache consistency. The merge policy in `merge_gap_fill_payload` is deterministic (primary wins).

**Findings:**
- `tools/cq/search/objects/resolve.py:116-131`: Summaries sorted by `(-occurrence_count, symbol.lower(), object_id)` and occurrences sorted by `(file, line, col, object_id, occurrence_id)` -- fully deterministic.
- `tools/cq/search/enrichment/core.py:66-84`: `merge_gap_fill_payload` has deterministic merge semantics: primary values always win.

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Most modules are appropriately simple. The main exception is `smart_search.py` which handles too many concerns. The enrichment normalization in `core.py` is well-factored with clear single-purpose functions.

**Findings:**
- `tools/cq/search/pipeline/smart_search.py`: ~2049 LOC. Exceeds reasonable module complexity.
- `tools/cq/search/python/extractors.py`: ~1437 LOC. Large but well-structured with clear stage boundaries.
- `tools/cq/search/enrichment/core.py`: ~373 LOC. Well-factored with focused functions. Good example.
- `tools/cq/search/objects/render.py`: ~405 LOC. Clean section builders with minimal logic.

**Suggested improvement:**
Split `smart_search.py` as recommended in P2/P3.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Two modules define comprehensive typed fact structs that are not used in the pipeline. These structs represent a good future direction but are currently speculative generality.

**Findings:**
- `tools/cq/search/enrichment/python_facts.py`: Defines 9 typed fact structs (`PythonResolutionFacts`, `PythonBehaviorFacts`, etc.) and an aggregator `PythonEnrichmentFacts`. None of these are imported or used in the pipeline.
- `tools/cq/search/enrichment/rust_facts.py`: Defines 4 typed fact structs (`RustStructureFacts`, `RustDefinitionFacts`, etc.) and an aggregator `RustEnrichmentFacts`. None used.
- `tools/cq/search/rust/evidence.py:32`: `RustModuleEdgeV1 = RustImportEdgeV1` -- backward-compatible alias for legacy tests. If no tests use it, it is speculative.

**Suggested improvement:**
Either integrate the typed fact structs into the pipeline (recommended -- this addresses P9/P10 simultaneously) or remove them until they are needed. If keeping them, add a comment explaining they are staged for future integration.

**Effort:** small
**Risk if unaddressed:** low -- Unused code increases cognitive load but does not affect runtime behavior.

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
APIs generally behave as expected. A few naming conventions create minor confusion.

**Findings:**
- `tools/cq/search/pipeline/smart_search_types.py:341`: `LanguageSearchResult = _LanguageSearchResult` and `PythonSemanticPrefetchResult = _PythonSemanticPrefetchResult` -- public aliases for private-prefixed types. The underscore prefix suggests these are internal, but the aliases export them publicly.
- `tools/cq/search/pipeline/contracts.py:79`: `SmartSearchContext = SearchConfig` -- type alias creates confusion about whether there are two types or one.
- `tools/cq/search/rust/evidence.py:142-150`: `build_macro_expansion_evidence` is a thin wrapper around `build_macro_evidence` with no additional logic -- the two names suggest different functionality.

**Suggested improvement:**
Remove the private-prefixed types and use the public names directly. Remove the `SmartSearchContext` alias if it adds no value. Consolidate `build_macro_expansion_evidence` into `build_macro_evidence` or make the distinction clear.

**Effort:** small
**Risk if unaddressed:** low -- Naming confusion slows onboarding but does not cause bugs.

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
V1 suffixes on output structs (`SearchObjectResolvedViewV1`, `SemanticOutcomeV1`, `RustFactPayloadV1`) provide clear versioning. `__all__` is defined in every module. Some private-prefixed names leak into `__all__`.

**Findings:**
- All 44 files define `__all__` -- good practice consistently applied.
- `tools/cq/search/pipeline/smart_search_telemetry.py:369`: `_resolve_search_worker_count` is not in `__all__` but is imported directly by smart_search.py -- the boundary is respected.
- `tools/cq/search/pipeline/smart_search_types.py:345-359`: `__all__` properly exports only the public types.
- `tools/cq/search/enrichment/contracts.py`: V1 contracts with clean `__all__`.

**Suggested improvement:**
No immediate action needed. The versioning and `__all__` discipline is good.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 2/3

**Current state:**
Pure functions (classification, enrichment normalization, telemetry accumulation) are easily testable without setup. Module-level caches require `monkeypatch` or explicit `clear_caches()` calls. The `_PipelineContext` pattern in `front_door.py` is test-friendly.

**Findings:**
- `tools/cq/search/pipeline/classifier_runtime.py:22-27`: Six module-level caches require `clear_caches()` before each test to avoid cross-test pollution.
- `tools/cq/search/semantic/front_door.py:52-67`: `_PipelineContext` dataclass provides all dependencies as constructor arguments -- easily mockable for testing.
- `tools/cq/search/pipeline/partition_pipeline.py`: Uses `Any` type for several parameters, reducing IDE support during test development.

**Suggested improvement:**
Convert classifier runtime caches into an injectable `ClassifierCache` object passed through context. This eliminates the need for global `clear_caches()` calls in tests.

**Effort:** medium
**Risk if unaddressed:** low -- Tests work today but are more fragile due to shared mutable state.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Enrichment telemetry is comprehensive: per-language status counts, per-stage timing, cache hit rates, and drift metrics are all tracked. However, there is no structured logging within individual enrichment stages.

**Findings:**
- `tools/cq/search/pipeline/smart_search_telemetry.py:288-319`: `build_enrichment_telemetry` aggregates per-language status counts, stage status, stage timings, query runtime flags, and cache statistics. Comprehensive.
- `tools/cq/search/semantic/front_door.py:397-436`: `_persist_outcome` records cache set success and tags. Good observability.
- `tools/cq/search/rust/enrichment.py:39`: `_ENRICHMENT_ERRORS` tuple defines the fail-open boundary, but errors caught by this tuple are silently swallowed -- no logging of degraded enrichment attempts.
- `tools/cq/search/python/extractors.py`: `_ENRICHMENT_ERRORS` similarly swallows errors silently.

**Suggested improvement:**
Add structured warning logs when enrichment stages degrade (catch `_ENRICHMENT_ERRORS` and log with stage name, error type, and file context). This would make debugging enrichment failures much easier without changing the fail-open behavior.

**Effort:** small
**Risk if unaddressed:** low -- Enrichment failures are currently invisible unless the user notices missing enrichment in output.

---

## Cross-Cutting Themes

### Theme 1: The `dict[str, object]` Payload Trap

**Description:** The most pervasive design issue across the reviewed scope is the use of `dict[str, object]` as the universal payload representation. This single choice creates cascading violations of P9 (Parse, don't validate), P10 (Make illegal states unrepresentable), P14 (Law of Demeter), and P15 (Tell, don't ask). Typed contracts exist in `enrichment/contracts.py`, `enrichment/python_facts.py`, and `enrichment/rust_facts.py` but are never integrated into the pipeline. Every consumer re-validates the same keys with `isinstance` checks, creating 100+ redundant validation sites.

**Root cause:** The pipeline was likely built incrementally, with `dict[str, object]` as a convenient initial representation. The typed structs were added later as a design improvement that was never integrated.

**Affected principles:** 4, 9, 10, 14, 15

**Suggested approach:** Progressively introduce typed wrappers. Start with the most-consumed payload shape (Python enrichment) and convert at the enrichment boundary. Downstream consumers can migrate one at a time.

### Theme 2: Missing Language Abstraction Layer

**Description:** Language-specific dispatch is scattered across 24+ callsites in 9 files, all using string comparison (`lang == "python"`). This violates P6 (Ports & Adapters) and concentrates P7 (DRY) violations. The absence of a language abstraction makes adding a new language a shotgun surgery operation.

**Root cause:** The system started as Python-only; Rust support was added by inserting conditionals rather than extracting a shared interface.

**Affected principles:** 6, 7, 3

**Suggested approach:** Define a `LanguageEnrichmentPort` protocol and register language adapters. Migrate dispatch sites incrementally, starting with the enrichment pipeline (highest concentration).

### Theme 3: `smart_search.py` as God Module

**Description:** At ~2049 LOC, `smart_search.py` is the central orchestration hub that also implements summary building, section building, and follow-up generation. It changes for 5+ distinct reasons, violating P3 (SRP) and P19 (KISS). This module is the most-imported, most-modified file in the scope.

**Root cause:** Organic growth without periodic extraction.

**Affected principles:** 2, 3, 19

**Suggested approach:** Extract summary, sections, and follow-ups into separate modules. Keep `smart_search.py` as a thin orchestrator that calls phase functions.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Consolidate 4 duplicated helper functions across pipeline modules | small | Eliminates divergent evolution risk for `_normalize_python_semantic_degradation_reason`, `_count_mapping_rows`, `_resolve_search_worker_count`, `_string` |
| 2 | P11 (CQS) | Make `attach_*` functions in `rust/evidence.py` return `None` instead of mutate-and-return | small | Clarifies mutation semantics at 3 callsites |
| 3 | P21 (Least astonishment) | Remove `LanguageSearchResult = _LanguageSearchResult` aliases; use public names directly | small | Reduces naming confusion |
| 4 | P24 (Observability) | Add structured warning logs in `_ENRICHMENT_ERRORS` catch blocks | small | Makes enrichment degradation visible |
| 5 | P16 (Functional core) | Extract `attach_enrichment_cache_stats` to pipeline shell; pass stats as parameters | small | Removes impure I/O from telemetry module |

## Recommended Action Sequence

1. **Consolidate duplicated helpers (P7).** Move `_normalize_python_semantic_degradation_reason`, `_count_mapping_rows`, `_resolve_search_worker_count`, and `_string` to canonical locations. Minimal risk, immediate DRY improvement.

2. **Fix CQS violations in `rust/evidence.py` (P11).** Change `attach_*` functions to return `None`. Update 3-5 callsites. Minimal risk.

3. **Add enrichment degradation logging (P24).** Add `logger.warning(...)` in `_ENRICHMENT_ERRORS` catch blocks in `python/extractors.py` and `rust/enrichment.py`. No behavioral change.

4. **Split `smart_search.py` (P2, P3, P19).** Extract summary building, section building, and follow-up generation into separate modules. Keep orchestration in `smart_search.py`. Medium effort, high readability improvement.

5. **Define `LanguageEnrichmentPort` protocol (P6).** Create the protocol in `enrichment/contracts.py`. Implement Python and Rust adapters. Migrate the 5 highest-traffic dispatch sites first (front_door, telemetry, pipeline). Medium effort, major extensibility improvement.

6. **Integrate typed fact structs (P9, P10, P14, P15).** Parse `dict[str, object]` payloads into `PythonEnrichmentFacts` / `RustEnrichmentFacts` at the enrichment boundary. This is the highest-impact change but requires the most coordination. Start with Python enrichment as a pilot.

7. **Encapsulate classifier runtime caches (P1, P12, P23).** Wrap the 6 module-level caches in a `ClassifierCache` class. Inject via pipeline context. Improves testability and information hiding simultaneously.

8. **Move `get_node_index`/`get_sg_root` to `_shared/` (P5).** Eliminate reverse dependency from `rust/enrichment.py` and `semantic/front_door.py` on `pipeline/classifier.py`. Small effort, clean dependency direction.
