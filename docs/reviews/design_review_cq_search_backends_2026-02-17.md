# Design Review: CQ Search Backends

**Date:** 2026-02-17
**Scope:** `tools/cq/search/python/`, `tools/cq/search/rust/`, `tools/cq/search/semantic/`, `tools/cq/search/objects/`, `tools/cq/search/rg/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 33

## Executive Summary

The five CQ search backend directories contain approximately 10,000 LOC across 33 files, implementing the language-specific enrichment, ripgrep integration, semantic front-door pipeline, and object resolution layers. The architecture exhibits strong patterns in its ripgrep codec (`rg/codec.py`), Rust node-access protocol (`rust/node_access.py`), and well-decomposed extraction submodules (`python/extractors_classification.py`, `python/extractors_analysis.py`). However, the scope suffers from three systemic issues: (1) a 1795-LOC God module in `python/extractors.py` that conflates five enrichment stages, caching, agreement tracking, and payload budgeting; (2) pervasive use of `dict[str, object]` as the intermediate payload type (~112 occurrences across the scope), preventing static type safety and forcing defensive coercion at every boundary; and (3) a missing `LanguageEnrichmentPort` protocol in `semantic/` that would replace the hardcoded Python/Rust dispatch table. Addressing the God module decomposition and introducing typed enrichment payloads would yield the highest alignment improvement across multiple principles simultaneously.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | medium | `_SESSION_CACHE` and `_AST_CACHE` module-level singletons; private names in `__all__` |
| 2 | Separation of concerns | 1 | large | high | `extractors.py` (1795 LOC) conflates 5 enrichment stages, caching, agreement, budgeting |
| 3 | SRP | 1 | large | high | `extractors.py` changes for any of 5+ reasons; `models.py` mixes contracts/state/helpers |
| 4 | High cohesion, low coupling | 2 | medium | medium | Good submodule decomposition in `python/`; but `extractors.py` couples everything |
| 5 | Dependency direction | 2 | medium | medium | `front_door.py` depends on concrete Python/Rust enrichment functions, not abstractions |
| 6 | Ports & Adapters | 1 | medium | medium | Rust `node_access.py` exemplary; Python has no equivalent; semantic/ lacks enrichment port |
| 7 | DRY | 1 | small | medium | `_iter_nodes_with_parents` duplicated; `_MAX_PARENT_DEPTH` duplicated; pattern repetition in rg/ |
| 8 | Design by contract | 2 | medium | low | Good frozen msgspec contracts in rg/codec.py, objects/render.py; weak in extractors.py payloads |
| 9 | Parse, don't validate | 1 | large | medium | `dict[str, object]` traversed everywhere with defensive `isinstance` checks instead of typed parse |
| 10 | Make illegal states unrepresentable | 1 | medium | medium | `PythonAnalysisSession` uses `Any` for 3 fields; mutable despite holding cached artifacts |
| 11 | CQS | 2 | small | low | `attach_rust_evidence()` mutates payload in-place; `_merge_import_payload()` mutates |
| 12 | DI + explicit composition | 1 | medium | medium | `front_door.py:215` hardcodes dispatch dict; module-level cache singletons not injected |
| 13 | Composition over inheritance | 3 | - | low | No inheritance misuse; composition used throughout |
| 14 | Law of Demeter | 1 | medium | medium | `resolve.py` reaches through 3+ dict layers; `_payload_views()` reconstructs from raw dicts |
| 15 | Tell, don't ask | 1 | medium | medium | Extensive `isinstance` + `.get()` interrogation patterns in `resolve.py` and `models.py` |
| 16 | Functional core, imperative shell | 2 | medium | low | Pure extractors well separated; but enrichment state is mutable and side-effectful |
| 17 | Idempotency | 3 | - | low | Cache-verified re-runs produce same results; content-hash keying ensures correctness |
| 18 | Determinism | 2 | small | low | Sorted outputs throughout; `perf_counter` timing introduces non-deterministic metadata |
| 19 | KISS | 2 | medium | low | Well-intentioned but complex; 5-stage pipeline, 3-way agreement, payload budgeting |
| 20 | YAGNI | 2 | small | low | `_CROSSCHECK_METADATA_KEYS` and crosscheck env gating appear unused in production |
| 21 | Least astonishment | 2 | small | low | `resolution_support.py` exports private `_AstAnchor`, `_DefinitionSite` in `__all__` |
| 22 | Declare and version public contracts | 2 | small | low | Good `__all__` discipline; but `V1` contracts not all frozen or versioned |
| 23 | Design for testability | 1 | medium | high | Module-level singletons require monkeypatch; `extractors.py` untestable without full pipeline |
| 24 | Observability | 2 | small | low | Stage timings, degrade reasons tracked; but no structured logging or OpenTelemetry spans |

## Detailed Findings

### Category: Boundaries (P1-P6)

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
Module-level mutable singletons expose internal cache state to any importer. Private names are leaked through public APIs.

**Findings:**
- `tools/cq/search/python/analysis_session.py:26-28`: `_SESSION_CACHE` is a module-level `BoundedCache` singleton. Any module importing this file can indirectly depend on its existence and clear it.
- `tools/cq/search/python/extractors.py:275-278`: `_AST_CACHE` is another module-level singleton registered with `CACHE_REGISTRY` at import time. This couples cache lifecycle to module import order.
- `tools/cq/search/rust/enrichment.py:82-85`: Third module-level `_AST_CACHE` singleton with identical pattern, also registered at import time.
- `tools/cq/search/python/resolution_support.py:565-583` (in `__all__`): Exports private names `_AstAnchor`, `_DefinitionSite`, `_ResolutionPayloadInputs` through `__all__`, breaking the convention that underscore-prefixed names are internal.

**Suggested improvement:**
Move cache instances behind factory functions or inject them via constructor parameters. For `resolution_support.py`, either promote the types to public names (remove underscore prefix) or remove them from `__all__` and make consuming modules import from a separate public types module.

**Effort:** medium
**Risk if unaddressed:** medium -- cache lifecycle surprises during testing and concurrent use.

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`extractors.py` is a 1795-LOC God module that conflates at least five distinct concerns: enrichment stage orchestration, individual extractor logic, agreement tracking, payload budgeting, and fact-to-dict marshalling.

**Findings:**
- `tools/cq/search/python/extractors.py:1-1795`: This single file contains:
  - 5 stage runners (`_run_ast_grep_stage`, `_run_python_ast_stage`, `_run_import_stage`, `_run_python_resolution_stage`, `_run_tree_sitter_stage`)
  - Agreement building logic (lines 997-1067)
  - Payload budgeting and enforcement (lines 1575-1634)
  - Fact normalization and marshalling (lines 1105-1200)
  - 2 public entrypoints (`enrich_python_context` at line 1642, `enrich_python_context_by_byte_range` at line 1710)
  - Cache infrastructure (lines 275-310)
  - Mutable enrichment state dataclass `_PythonEnrichmentState` (line 1070)
- `tools/cq/search/semantic/models.py:1-448`: Consolidation module mixing contracts (4 frozen structs, lines 58-103), state helpers (glob compilation, lines 107-118), language detection (root markers, lines 31-40), retry logic (`call_with_retry`, referenced from front_door), and front-door adapter functions (`build_static_semantic_planes`, line 218).

**Suggested improvement:**
Split `extractors.py` into:
1. `extractors_orchestrator.py` -- stage sequencing, agreement building, public entrypoints
2. `extractors_ast_grep.py` -- ast-grep specific extraction logic (currently lines ~315-650)
3. `extractors_agreement.py` -- cross-source agreement tracking (lines 997-1067)
4. `extractors_budget.py` -- payload budgeting and enforcement (lines 1575-1634)

Split `models.py` into `contracts.py` (frozen structs) and `helpers.py` (adapter functions).

**Effort:** large
**Risk if unaddressed:** high -- every enrichment change touches this file; merge conflicts and cognitive overhead scale with size.

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
`extractors.py` changes for at least 5 independent reasons: adding a new enrichment stage, modifying agreement logic, changing payload budget, adjusting cache policy, or updating fact marshalling.

**Findings:**
- `tools/cq/search/python/extractors.py:1070-1081`: `_PythonEnrichmentState` is mutable state that accumulates results from all 5 stages. Changes to any stage's output shape require modifying this shared class.
- `tools/cq/search/python/extractors.py:1083-1102`: Field set constants (`_PY_RESOLUTION_FIELDS`, `_PY_BEHAVIOR_FIELDS`, etc.) are defined inline, coupling fact-type knowledge to the orchestrator.
- `tools/cq/search/rust/enrichment.py:1-667`: At 667 LOC, this file is approaching God module territory. It contains the same pattern: cache management, ast-grep stage, tree-sitter stage, agreement building, and public entrypoints all in one file. However, it delegates well to `extractors_shared.py` for actual extraction logic.

**Suggested improvement:**
Extract agreement tracking into its own module. Move field-set constants to the `enrichment/python_facts.py` module where the fact structs are defined, so fact shape changes stay localized.

**Effort:** large
**Risk if unaddressed:** high -- any enrichment pipeline change risks regressions across unrelated concerns.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The submodule decomposition within `python/` is well-structured, with `extractors_classification.py` (357 LOC), `extractors_analysis.py` (293 LOC), `extractors_structure.py` (329 LOC), and `evidence.py` (119 LOC) each owning a single responsibility. The coupling problem is concentrated in `extractors.py`, which depends on all of them.

**Findings:**
- `tools/cq/search/python/extractors.py:69-101`: 17 imports from sibling modules within `python/`, plus 12 imports from `_shared/` and `enrichment/`. This file is the dependency bottleneck.
- `tools/cq/search/python/extractors_classification.py:1-357`: Good cohesion. Only classification logic, dispatch tables, and role mapping. Clean dependency surface.
- `tools/cq/search/python/extractors_analysis.py:1-293`: Good cohesion. Pure behavior/import analysis with no external state.
- `tools/cq/search/rg/codec.py:1-380`: Strong cohesion. Typed JSON event decoding with msgspec structs. Clean boundary between raw ripgrep output and typed events.

**Suggested improvement:**
Reduce `extractors.py` fan-in by introducing an orchestrator pattern that receives stage runners as a sequence of callables rather than importing them directly.

**Effort:** medium
**Risk if unaddressed:** medium -- high coupling makes isolated testing and changes difficult.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
Core enrichment logic generally depends on abstractions, with the notable exception of `front_door.py`, which imports concrete Python and Rust enrichment functions directly.

**Findings:**
- `tools/cq/search/semantic/front_door.py:35-36`: Direct imports of `enrich_python_context_by_byte_range` from `python.extractors` and `enrich_rust_context_by_byte_range` from `rust.enrichment`. The semantic front-door (higher-level orchestrator) depends directly on concrete language-specific implementations.
- `tools/cq/search/semantic/front_door.py:215-218`: Hardcoded dispatch dict `{"python": _execute_python_provider, "rust": _execute_rust_provider}` with a fallback to Rust. This is a closed set that must be modified to add any new language.
- `tools/cq/search/rust/enrichment.py:130-149`: Thin wrappers like `_scope_name`, `_find_scope`, `_find_ancestor` delegate to shared extractors via `SgRustNodeAccess` adapter -- good dependency direction through the `RustNodeAccess` protocol.

**Suggested improvement:**
Introduce a `LanguageEnrichmentProvider` protocol in `semantic/contracts.py` with a `enrich_by_byte_range()` method. Register Python and Rust providers via a registry. Have `front_door.py` depend on the protocol, not concrete implementations.

**Effort:** medium
**Risk if unaddressed:** medium -- adding a third language requires modifying the orchestrator.

---

#### P6. Ports & Adapters -- Alignment: 1/3

**Current state:**
The Rust subsystem has an exemplary Ports & Adapters implementation with `RustNodeAccess` protocol and two concrete adapters. The Python subsystem has no equivalent. The semantic front-door lacks a language enrichment port entirely.

**Findings:**
- `tools/cq/search/rust/node_access.py:13-35`: `RustNodeAccess` protocol with 5 methods (`kind`, `text`, `child_by_field_name`, `children`, `parent`). This is the gold standard for this codebase.
- `tools/cq/search/rust/node_access.py:37-67`: `SgRustNodeAccess` adapter wrapping `ast_grep_py.SgNode`.
- `tools/cq/search/rust/node_access.py:70-106`: `TreeSitterRustNodeAccess` adapter wrapping `tree_sitter.Node`.
- `tools/cq/search/python/extractors.py:315-650`: Python extraction functions receive raw `SgNode` directly from ast-grep with no adapter indirection. When tree-sitter is used later (line 1699), it goes through a completely different code path with no shared protocol.
- `tools/cq/search/semantic/front_door.py:210-219`: No port/protocol for language enrichment. Direct function dispatch.

**Suggested improvement:**
1. Create a `PythonNodeAccess` protocol mirroring `RustNodeAccess` to unify ast-grep and tree-sitter node access in Python extraction.
2. Create a `LanguageEnrichmentPort` protocol in `semantic/` that `front_door.py` depends on, with Python and Rust adapters registered at composition time.

**Effort:** medium
**Risk if unaddressed:** medium -- Python enrichment cannot cleanly swap backends; new languages require invasive changes.

---

### Category: Knowledge (P7-P11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
Two concrete instances of duplicated knowledge and one pattern-level duplication.

**Findings:**
- `tools/cq/search/python/analysis_session.py:42-50` and `tools/cq/search/python/resolution_support.py:62-68`: `_iter_nodes_with_parents` is duplicated verbatim. The `analysis_session.py` version returns a `list`, while the `resolution_support.py` version yields via `Iterator`. Same algorithm, different return protocols.
- `tools/cq/search/python/extractors.py:131` and `tools/cq/search/python/extractors_structure.py:24`: `_MAX_PARENT_DEPTH = 20` is duplicated. This is a shared business constant (max AST traversal depth) that should have a single source of truth.
- `tools/cq/search/rg/adapter.py:93-347`: Functions `find_files_with_pattern`, `find_call_candidates`, `search_content`, `find_symbol_candidates`, and `find_symbol_definition_files` all follow the same pattern: build `RgRunRequest`, call `search_sync_with_timeout(run_rg_json, ...)`, iterate events, filter by language scope, collect results. This is knowledge duplication at the pattern level -- the "run ripgrep and collect typed results" workflow is repeated 5 times with minor variations.

**Suggested improvement:**
1. Extract `_iter_nodes_with_parents` to `ast_utils.py` (which already exists and is the natural home for shared AST traversal utilities).
2. Extract `_MAX_PARENT_DEPTH` to a shared constants module or `ast_utils.py`.
3. In `rg/adapter.py`, extract a generic `_run_and_collect()` higher-order function that takes a result-extraction callback, eliminating the repeated boilerplate.

**Effort:** small (items 1-2), medium (item 3)
**Risk if unaddressed:** medium -- divergent copies will accumulate subtle behavioral differences.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Good use of frozen msgspec structs for boundary contracts in `rg/codec.py`, `objects/render.py`, and `rust/contracts.py`. Weak in the Python enrichment pipeline where `dict[str, object]` serves as the de facto contract.

**Findings:**
- `tools/cq/search/rg/codec.py:13-60`: Excellent typed contracts: `RgText`, `RgPath`, `RgSubmatch`, `RgMatchData`, `RgContextData`, `RgSummaryStats` -- all frozen msgspec structs with explicit field types.
- `tools/cq/search/objects/render.py:18-87`: Strong contracts: `OccurrenceGroundingV1`, `ResolvedObjectRef`, `SearchOccurrenceV1`, `SearchObjectSummaryV1`, `SearchObjectResolvedViewV1` -- all frozen with explicit fields.
- `tools/cq/search/python/extractors.py:1642-1653`: `enrich_python_context()` returns `dict[str, object] | None`. No postcondition on what keys exist or their types. Callers must defensively parse.
- `tools/cq/search/semantic/models.py:58-96`: Good frozen contracts (`SemanticOutcomeV1`, `LanguageSemanticEnrichmentRequest`, etc.) but `payload: dict[str, object] | None` on line 61 and 83 pushes the untyped boundary inward.

**Suggested improvement:**
Define a `PythonEnrichmentResult` frozen struct (or reuse `PythonEnrichmentFacts` from `enrichment/python_facts.py`) as the return type of `enrich_python_context()`, replacing `dict[str, object]`. Callers would get static type safety and IDE support.

**Effort:** medium
**Risk if unaddressed:** low -- current defensive parsing works, but is verbose and error-prone.

---

#### P9. Parse, don't validate -- Alignment: 1/3

**Current state:**
The enrichment pipeline produces `dict[str, object]` payloads that are subsequently "validated" via `isinstance` checks at every consumption point rather than parsed into typed representations once at the boundary.

**Findings:**
- `tools/cq/search/objects/resolve.py:185-219`: `_payload_views()` reconstructs typed views from raw dicts using `msgspec.convert()` and then falls back to manual dict construction if the typed view lacks signal. This is validate-then-reconstruct rather than parse-once.
- `tools/cq/search/objects/resolve.py:60-74`: `_struct_to_mapping()` converts structs back to dicts, then `_mapping_has_signal()` iterates values checking types. Round-tripping through dicts.
- `tools/cq/search/semantic/models.py:121-203`: Functions like `_string_list`, `_mapping_list`, `_python_diagnostics`, `_locals_preview` all perform repeated `isinstance` + `.get()` validation on untyped payloads.
- `tools/cq/search/python/extractors.py:1105-1159`: `_parse_struct_or_none`, `_facts_dict`, `_merge_string_key_mapping`, `_merge_import_payload`, `_normalize_resolution_fact_rows` -- all defensive coercion helpers that exist because the pipeline uses untyped dicts.

**Suggested improvement:**
Parse enrichment payloads into `PythonEnrichmentFacts` (which already exists as a msgspec struct in `enrichment/python_facts.py`) at the enrichment boundary. Downstream consumers would receive typed structs, eliminating the need for ~20 defensive coercion helper functions.

**Effort:** large
**Risk if unaddressed:** medium -- each new consumer duplicates defensive parsing; type errors are caught at runtime, not statically.

---

#### P10. Make illegal states unrepresentable -- Alignment: 1/3

**Current state:**
Several data models allow states that should be structurally impossible.

**Findings:**
- `tools/cq/search/python/analysis_session.py:63-68`: `PythonAnalysisSession` uses `Any` for `node_index` (line 63), `tree_sitter_tree` (line 67), and `resolution_index` (line 68). These should be typed to their actual types (with TYPE_CHECKING imports if needed for the tree-sitter and ast-grep types).
- `tools/cq/search/python/analysis_session.py:53`: `PythonAnalysisSession` is `@dataclass(slots=True)` but NOT frozen, despite holding cached analysis artifacts. This allows callers to mutate cached state after retrieval, which could corrupt shared cache entries.
- `tools/cq/search/objects/render.py:75-76`: `code_facts: dict[str, object]` and `module_graph: dict[str, object]` on `SearchObjectSummaryV1` are untyped bags. The actual shapes are known and could be typed.
- `tools/cq/search/semantic/models.py:61,83`: `payload: dict[str, object] | None` on `SemanticOutcomeV1` and `LanguageSemanticEnrichmentOutcome` allows any shape. A discriminated union of Python/Rust outcome types would be more precise.

**Suggested improvement:**
1. Replace `Any` fields on `PythonAnalysisSession` with proper types under `TYPE_CHECKING`.
2. Make `PythonAnalysisSession` use a builder pattern that produces a frozen snapshot, preventing mutation of cached artifacts.
3. Type `code_facts` and `module_graph` on `SearchObjectSummaryV1` with specific struct types.

**Effort:** medium
**Risk if unaddressed:** medium -- `Any` types defeat static analysis; mutable cached state can cause subtle corruption.

---

#### P11. CQS (Command Query Separation) -- Alignment: 2/3

**Current state:**
Most query functions are pure, but several enrichment functions mutate their arguments in-place while also returning information.

**Findings:**
- `tools/cq/search/rust/evidence.py:229-232`: `attach_rust_evidence(payload)` mutates `payload` dict in-place by adding `"rust_module_graph"` and `"macro_expansion_evidence"` keys. Pure command, no return -- acceptable CQS.
- `tools/cq/search/python/extractors.py:1121-1124`: `_merge_string_key_mapping(target, payload)` mutates `target` dict in-place. Command with no return -- acceptable.
- `tools/cq/search/python/extractors.py:1127-1138`: `_merge_import_payload(import_fields, payload)` mutates `import_fields` in-place. Command with no return -- acceptable.
- `tools/cq/search/python/analysis_session.py:271-301`: `get_python_analysis_session()` both queries the cache AND mutates it (via `_SESSION_CACHE.put()`). This is a get-or-create pattern, which is a common CQS exception but could be separated into `try_get` + `create_and_cache`.

**Suggested improvement:**
The CQS violations are minor and follow common patterns (cache get-or-create, in-place mutation helpers). The `attach_rust_evidence` pattern is actually clean CQS (command only). No high-priority changes needed.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (P12-P15)

#### P12. DI + explicit composition -- Alignment: 1/3

**Current state:**
Dependencies are resolved via module-level singletons and hardcoded dispatch tables rather than injected at construction time.

**Findings:**
- `tools/cq/search/semantic/front_door.py:215-218`: Hardcoded dispatch dict for language providers. No injection point for custom or test providers.
- `tools/cq/search/python/analysis_session.py:26-28`: `_SESSION_CACHE` created at module import time with no injection mechanism.
- `tools/cq/search/python/extractors.py:275-278`: `_AST_CACHE` created at module import time and registered with `CACHE_REGISTRY` via side effect.
- `tools/cq/search/rust/enrichment.py:82-85`: Same pattern for Rust `_AST_CACHE`.
- `tools/cq/search/semantic/front_door.py:10-30`: 12 imports from `tools.cq.core.cache.*` -- the front-door pipeline is tightly coupled to the specific cache implementation.

**Suggested improvement:**
Accept cache instances as constructor/function parameters with module-level singletons as defaults. For language dispatch, use a registry pattern: `LANGUAGE_ENRICHMENT_PROVIDERS: dict[QueryLanguage, LanguageEnrichmentProvider]` that can be populated at startup and overridden in tests.

**Effort:** medium
**Risk if unaddressed:** medium -- testing requires monkeypatching module globals; adding new language providers requires modifying the orchestrator.

---

#### P13. Composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance is used in the scope. All behavior is composed via function calls, protocols, and dataclass composition. The `RustNodeAccess` protocol with concrete adapter classes is a textbook example.

**Findings:**
No violations found. This principle is well-satisfied.

**Effort:** --
**Risk if unaddressed:** low

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
`resolve.py` extensively reaches into nested dict structures, accessing collaborator's collaborator's data. The `_payload_views` function and its consumers traverse 3+ dict layers.

**Findings:**
- `tools/cq/search/objects/resolve.py:185-219`: `_payload_views()` calls `incremental_enrichment_payload(match.incremental_enrichment)`, then `incremental.get("semantic")`, then checks `isinstance(semantic_raw, dict)`, then passes through `_payload_view(python)` which calls `EnrichmentPayloadView.from_raw(payload)` which internally accesses nested dict keys. This is a train wreck of accessor chains.
- `tools/cq/search/objects/resolve.py:193-211`: After getting `python_view`, the code accesses `python_view.resolution`, converts it to a mapping with `_struct_to_mapping()`, checks signal with `_mapping_has_signal()`, then falls back to `python_facts.resolution.qualified_name_candidates` -- 4 levels of indirection.
- `tools/cq/search/semantic/models.py:177-203`: `_locals_preview()` accesses `payload.get("qualified_name_candidates")` -> iterates items -> `item.get("name")` -> filters strings. Three levels of dict traversal.
- `tools/cq/search/semantic/models.py:192-203`: `locals_payload.get("index")` -> iterate `index_rows` -> `item.get("name")` -> filter. Same 3-level pattern.

**Suggested improvement:**
Create a typed `EnrichmentPayload` facade class with methods like `qualified_names() -> list[str]`, `binding_candidates() -> list[BindingCandidate]`, `resolution_quality() -> ResolutionQuality`. Consumers would call `payload.qualified_names()` instead of traversing nested dicts. This directly follows from resolving P9.

**Effort:** medium
**Risk if unaddressed:** medium -- deep dict traversals are fragile and break silently when upstream changes payload structure.

---

#### P15. Tell, don't ask -- Alignment: 1/3

**Current state:**
Objects expose raw data through `dict[str, object]` and callers perform external logic on the exposed data rather than asking the object to compute the answer.

**Findings:**
- `tools/cq/search/objects/resolve.py:60-74`: `_struct_to_mapping()` serializes a struct back to a dict, then `_mapping_has_signal()` iterates all values checking types. The struct itself should know whether it has signal -- `has_signal()` method on the struct.
- `tools/cq/search/objects/resolve.py:48-57`: `_payload_view()` and `_resolution_view()` are external factories that reconstruct typed views from raw dicts. The enrichment module should return typed views directly.
- `tools/cq/search/semantic/models.py:165-174`: `_semantic_tokens_preview()` reaches into payload via `.get()` for 4 keys, checks each for None, truncates signature. The payload should provide its own preview.
- `tools/cq/search/python/extractors.py:1141-1158`: `_normalize_resolution_fact_rows()` and `_normalize_resolution_fact_fields()` reach into dicts to normalize values. The resolution facts struct should handle its own normalization on construction.

**Suggested improvement:**
Add behavior methods to enrichment structs: `PythonEnrichmentFacts.has_signal() -> bool`, `PythonResolutionFacts.qualified_names() -> list[str]`, `SemanticOutcomeV1.preview() -> dict[str, object]`. Move interrogation logic from consumers into the types that own the data.

**Effort:** medium
**Risk if unaddressed:** medium -- external logic scattered across consumers leads to inconsistent handling.

---

### Category: Correctness (P16-P18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
The individual extractor functions in `extractors_classification.py`, `extractors_analysis.py`, `extractors_structure.py`, and `ast_utils.py` are pure and deterministic. The impure shell (cache management, I/O, timing) is concentrated in `extractors.py` and `front_door.py`. The boundary is clear but could be cleaner.

**Findings:**
- `tools/cq/search/python/extractors_classification.py:1-357`: Pure functions. No side effects. Good.
- `tools/cq/search/python/extractors_analysis.py:1-293`: Pure functions. No side effects. Good.
- `tools/cq/search/python/evidence.py:1-119`: Pure signal evaluation. Good.
- `tools/cq/search/python/extractors.py:1070-1081`: `_PythonEnrichmentState` is mutable state threaded through all 5 stages. Each stage mutates it via field assignments. This is an imperative accumulator pattern inside what should be the functional core.
- `tools/cq/search/semantic/front_door.py:69-100`: `run_language_semantic_enrichment()` mixes cache probe, lock acquisition, provider execution, and cache writeback in one function. The imperative shell is well-localized here.

**Suggested improvement:**
Convert `_PythonEnrichmentState` mutation to a functional pipeline where each stage returns its own result, and a final combiner merges them into the output payload. This would eliminate mutable state accumulation.

**Effort:** medium
**Risk if unaddressed:** low -- current approach works but couples stage outputs through shared mutable state.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Enrichment operations are content-hash keyed and produce the same output for the same input. Cache lookups verify content hashes before returning cached results.

**Findings:**
- `tools/cq/search/python/analysis_session.py:286-291`: `get_python_analysis_session()` checks `cached.content_hash == content_hash` before reusing a cached session. Content change invalidates the cache entry.
- `tools/cq/search/semantic/front_door.py:82-91`: Cache probe verifies content via `snapshot_digest`. Re-runs with same content skip enrichment.
- `tools/cq/search/rg/adapter.py:93-142`: Ripgrep searches are stateless -- same pattern + same files produce same matches.

No violations found. This principle is well-satisfied.

**Effort:** --
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 2/3

**Current state:**
Outputs are sorted deterministically. However, timing metadata introduces non-deterministic fields in output payloads.

**Findings:**
- `tools/cq/search/objects/resolve.py:158-173`: Summaries sorted by `(-occurrence_count, symbol.lower(), object_id)`. Occurrences sorted by `(file, line, col, object_id, occurrence_id)`. Deterministic.
- `tools/cq/search/rg/adapter.py:90`: `sorted(set(lines))` ensures deterministic output.
- `tools/cq/search/python/analysis_session.py:73-76`: `_mark_stage()` records `perf_counter()` timing in `stage_timings_ms`. This introduces non-deterministic floating-point values into the enrichment metadata.
- `tools/cq/search/python/extractors.py:1625`: `payload["stage_timings_ms"] = state.stage_timings_ms` propagates non-deterministic timing into the output payload. If payloads are compared for equality (e.g., in tests), timing fields cause spurious differences.

**Suggested improvement:**
Separate timing telemetry from the enrichment payload. Return a `(payload, telemetry)` tuple so that payload comparison is deterministic and telemetry is opt-in.

**Effort:** small
**Risk if unaddressed:** low -- timing fields are informational; payload equality is not typically checked in production.

---

### Category: Simplicity (P19-P22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The 5-stage enrichment pipeline with 3-way agreement tracking is necessarily complex given the multi-source evidence requirement. However, some complexity is accidental.

**Findings:**
- `tools/cq/search/python/extractors.py:997-1067`: Agreement building computes a 3-way comparison between ast-grep, Python resolution, and tree-sitter fields. The algorithm iterates all three field sets, finds intersections, and classifies conflicts. This is essential complexity for the cross-source agreement feature.
- `tools/cq/search/python/extractors.py:1105-1159`: 6 helper functions (`_parse_struct_or_none`, `_facts_dict`, `_merge_string_key_mapping`, `_merge_import_payload`, `_normalize_resolution_fact_rows`, `_normalize_resolution_fact_fields`) exist solely to marshal between `dict[str, object]` and typed structs. This is accidental complexity from the untyped payload decision.
- `tools/cq/search/rg/adapter.py:175-229`: `find_call_candidates()` tries identifier mode first, falls back to regex mode. The dual-mode fallback adds complexity but is justified by the identifier-mode precision benefit.

**Suggested improvement:**
Eliminating `dict[str, object]` as the intermediate type (P9) would remove ~6 marshalling helpers and simplify the overall pipeline. The agreement tracking complexity is justified and should remain.

**Effort:** medium
**Risk if unaddressed:** low -- the pipeline works; complexity just increases maintenance cost.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most features appear actively used. A few constructs look like speculative generality.

**Findings:**
- `tools/cq/search/python/extractors.py:125`: `_PYTHON_ENRICHMENT_CROSSCHECK_ENV = "CQ_PY_ENRICHMENT_CROSSCHECK"` -- an environment variable that enables crosscheck mismatch logging. This appears to be a debugging aid that should be temporary but persists in production code.
- `tools/cq/search/rust/enrichment.py:88`: `_CROSSCHECK_ENV = "CQ_RUST_ENRICHMENT_CROSSCHECK"` -- same pattern for Rust.
- `tools/cq/search/rust/enrichment.py:91-93`: `_CROSSCHECK_METADATA_KEYS` -- a frozenset of metadata keys used only for crosscheck logging. Speculative if crosscheck is never enabled in production.
- `tools/cq/search/semantic/diagnostics.py:33-56`: `CAPABILITY_MATRIX` is a module-level dict (not frozen). 24 entries mapping features to language support levels. Well-used but the matrix is open for mutation despite being logically constant.

**Suggested improvement:**
If crosscheck env vars are purely for development, move them to a debug/diagnostic module or gate them behind a `__debug__` check. Freeze the `CAPABILITY_MATRIX` (wrap in `types.MappingProxyType` or convert to frozen struct).

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
APIs generally behave as expected. One naming surprise and one return-type inconsistency.

**Findings:**
- `tools/cq/search/python/resolution_support.py:565-583`: `__all__` exports names with underscore prefixes: `_AstAnchor`, `_DefinitionSite`, `_ResolutionPayloadInputs`. The underscore convention signals "private", but `__all__` export signals "public". This is confusing -- a reader expects `__all__` items to be public API.
- `tools/cq/search/python/analysis_session.py:42-50` vs `tools/cq/search/python/resolution_support.py:62-68`: The same function `_iter_nodes_with_parents` returns `list[tuple[...]]` in one module and `Iterator[tuple[...]]` in the other. A caller moving between modules would be surprised by the different return protocol.

**Suggested improvement:**
Remove underscore prefixes from `_AstAnchor`, `_DefinitionSite`, `_ResolutionPayloadInputs` since they are exported in `__all__`. Unify `_iter_nodes_with_parents` (resolves both P7 and P21).

**Effort:** small
**Risk if unaddressed:** low -- naming confusion, not a runtime bug.

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
Good `__all__` discipline across all 33 files. Most boundary types use `V1` versioning. Some contracts lack version suffixes.

**Findings:**
- `tools/cq/search/rg/contracts.py:1-81`: `RgRunSettingsV1` and `RgProcessResultV1` properly versioned.
- `tools/cq/search/objects/render.py:18-87`: `OccurrenceGroundingV1`, `SearchOccurrenceV1`, `SearchObjectSummaryV1`, `SearchObjectResolvedViewV1` properly versioned.
- `tools/cq/search/rust/node_access.py:13-108`: `RustNodeAccess` protocol and adapters `SgRustNodeAccess`, `TreeSitterRustNodeAccess` lack version suffixes. As protocols, versioning is less critical but should be considered for stability.
- `tools/cq/search/python/analysis_session.py:31-39`: `AstSpanEntry` -- no version suffix. Used as internal cache structure.
- `tools/cq/search/rg/runner.py:24-33`: `RgProcessResult` -- no version suffix despite being part of the public API (returned by `run_rg_json`).

**Suggested improvement:**
Add `V1` suffix to `RgProcessResult` and `AstSpanEntry` to align with the codebase convention for serialized boundary types. Protocols like `RustNodeAccess` can remain unversioned as they represent stable behavioral contracts.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (P23-P24)

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
Module-level mutable singletons and the God module pattern in `extractors.py` make isolated unit testing difficult. Testing any single enrichment stage requires either the full pipeline context or heavy monkeypatching.

**Findings:**
- `tools/cq/search/python/analysis_session.py:26-28`: `_SESSION_CACHE` singleton. Testing `get_python_analysis_session()` without affecting other tests requires clearing or replacing the global cache.
- `tools/cq/search/python/extractors.py:275-278`: `_AST_CACHE` singleton. Same issue.
- `tools/cq/search/python/extractors.py:1642-1710`: `enrich_python_context()` and `enrich_python_context_by_byte_range()` require a `PythonNodeEnrichmentRequest` / `PythonByteRangeEnrichmentRequest` with real source bytes, a real `SgNode`, and optionally a `PythonAnalysisSession`. Testing a single extraction function (e.g., classify_item_role) requires constructing this full context.
- `tools/cq/search/semantic/front_door.py:10-30`: 12 cache-related imports. The `run_language_semantic_enrichment()` function cannot be tested without either a real cache backend or extensive monkeypatching of 5+ cache functions.
- Positive: `tools/cq/search/python/extractors_classification.py`, `extractors_analysis.py`, `extractors_structure.py`, and `evidence.py` are all pure functions that are trivially testable in isolation.

**Suggested improvement:**
1. Accept cache instances as parameters with defaults (enables test injection).
2. Extract stage runners into standalone functions that take explicit inputs and return explicit outputs, testable without the full `_PythonEnrichmentState` context.
3. Create a `SemanticFrontDoorConfig` that bundles cache, policy, and provider dependencies, injectable in tests.

**Effort:** medium
**Risk if unaddressed:** high -- as complexity grows, the untestable portions become the most likely sources of regressions.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
Stage-level timing and degrade reasons are tracked throughout the enrichment pipeline. However, there is no structured logging or OpenTelemetry integration in the search backends.

**Findings:**
- `tools/cq/search/python/analysis_session.py:73-76`: `_mark_stage()` accumulates timing via `perf_counter()`. Per-stage timing is available in the output payload.
- `tools/cq/search/python/extractors.py:1620-1634`: `stage_status`, `stage_timings_ms`, `degrade_reason`, and `truncated_fields` all propagated to the output payload. Good observability surface.
- `tools/cq/search/rust/enrichment.py:89`: `logger = logging.getLogger(__name__)` -- standard logging available but used sparingly (only for cache clearing and warnings).
- `tools/cq/search/semantic/front_door.py:27-30`: Cache telemetry via `record_cache_get`, `record_cache_set`, `record_cache_decode_failure`. Good.
- Missing: No OpenTelemetry spans (`SCOPE_SEMANTICS` or similar) in the search backends, unlike `src/semantics/` which is instrumented. No structured log events for enrichment lifecycle (start, stage completion, budget exceeded).

**Suggested improvement:**
Add OpenTelemetry spans for the enrichment pipeline: one parent span per `enrich_python_context_by_byte_range()` call, child spans per stage. This would integrate with the existing observability infrastructure in `src/obs/`.

**Effort:** small
**Risk if unaddressed:** low -- current telemetry is adequate for debugging; structured tracing would improve production observability.

---

## Cross-Cutting Themes

### Theme 1: The `dict[str, object]` Anti-Pattern

**Description:** The most pervasive design issue across all five backend directories is the use of `dict[str, object]` as the canonical intermediate payload type. This affects principles P8, P9, P10, P14, P15, P19, and P23 simultaneously. The root cause is that the enrichment pipeline was designed around a flexible, schema-less payload that could accommodate any combination of fields from multiple enrichment sources. Over time, the actual field sets have stabilized (as evidenced by the typed `PythonEnrichmentFacts` struct in `enrichment/python_facts.py`), but the pipeline still passes raw dicts.

**Root cause:** Historical -- enrichment fields were added incrementally; typed structs were introduced later but not threaded through the full pipeline.

**Affected principles:** P8, P9, P10, P14, P15, P19, P23

**Suggested approach:** Replace `dict[str, object]` with the existing `PythonEnrichmentFacts` and `RustFactPayloadV1` structs at the enrichment boundary. Modify `enrich_python_context()` and `enrich_rust_context_by_byte_range()` to return typed structs. Introduce a `EnrichmentPayload` protocol that both language-specific types implement. This single change propagates type safety through `objects/resolve.py`, `semantic/models.py`, and all downstream consumers, eliminating ~20 defensive coercion helpers.

### Theme 2: God Module Concentration

**Description:** `python/extractors.py` at 1795 LOC and `rust/enrichment.py` at 667 LOC concentrate too many concerns. The Python submodule decomposition (`extractors_classification.py`, `extractors_analysis.py`, `extractors_structure.py`) shows that the codebase already knows how to decompose well -- the same discipline just needs to be applied to the orchestrator and agreement layers.

**Root cause:** Incremental feature addition without periodic refactoring. Each new enrichment stage was added to the existing orchestrator rather than split out.

**Affected principles:** P2, P3, P4, P23

**Suggested approach:** Apply the same decomposition pattern used for `extractors_classification.py` et al. to the remaining monolith: extract agreement, budget, and stage orchestration into separate modules.

### Theme 3: Missing Language Abstraction Layer

**Description:** The semantic front-door (`front_door.py`) hardcodes Python and Rust dispatch. The Python subsystem has no node-access protocol equivalent to Rust's `RustNodeAccess`. Adding a third language (e.g., TypeScript) would require modifying `front_door.py:215-218`, `models.py:33-40`, and creating new parallel infrastructure.

**Root cause:** Two-language scope made explicit abstraction feel premature. The Rust team introduced `RustNodeAccess` for their dual-backend needs (ast-grep + tree-sitter), but no equivalent pressure existed on the Python side.

**Affected principles:** P5, P6, P12

**Suggested approach:** Introduce `LanguageEnrichmentProvider` protocol and registration mechanism. Even if a third language is not imminent, the abstraction pays for itself in testability (inject mock providers) and in isolating the front-door from language-specific implementation details.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Extract `_iter_nodes_with_parents` to `ast_utils.py`; unify `_MAX_PARENT_DEPTH` | small | Eliminates 2 concrete duplications, prevents divergence |
| 2 | P21 (Least astonishment) | Remove underscore prefix from `_AstAnchor`, `_DefinitionSite` in `resolution_support.py` (they are in `__all__`) | small | Aligns naming convention with export semantics |
| 3 | P18 (Determinism) | Separate `stage_timings_ms` from enrichment payload into a sidecar telemetry object | small | Enables deterministic payload comparison in tests |
| 4 | P20 (YAGNI) | Freeze `CAPABILITY_MATRIX` in `diagnostics.py`; gate crosscheck env vars behind `__debug__` | small | Prevents accidental mutation; clarifies dev-only features |
| 5 | P24 (Observability) | Add `logging.getLogger(__name__)` and basic structured log events to `objects/resolve.py` and `semantic/front_door.py` | small | Improves production debuggability at zero runtime cost |

## Recommended Action Sequence

1. **[P7] Consolidate duplications (small).** Move `_iter_nodes_with_parents` to `python/ast_utils.py` and `_MAX_PARENT_DEPTH` to a shared constants location. This is a safe, isolated change with no API impact.

2. **[P21] Fix naming inconsistencies (small).** Remove underscore prefixes from exported types in `resolution_support.py`. Update all importers.

3. **[P18, P24] Separate telemetry from payload (small).** Return `(payload, telemetry)` tuples from enrichment functions. Add structured logging to `front_door.py` and `resolve.py`.

4. **[P6, P12] Introduce LanguageEnrichmentProvider protocol (medium).** Define protocol in `semantic/contracts.py`. Register Python and Rust providers. Update `front_door.py` to dispatch via registry. This enables step 5.

5. **[P9, P14, P15] Thread typed structs through enrichment pipeline (large).** Change `enrich_python_context()` and `enrich_rust_context_by_byte_range()` to return typed structs instead of `dict[str, object]`. Update `objects/resolve.py` and `semantic/models.py` to consume typed structs. This is the highest-impact change, resolving the `dict[str, object]` anti-pattern across 7 principles.

6. **[P2, P3] Decompose extractors.py God module (large).** Split into `extractors_orchestrator.py`, `extractors_agreement.py`, `extractors_budget.py`, and an ast-grep specific extraction module. This depends on step 5 being complete (or at least started), as typed structs simplify the interfaces between the new modules.

7. **[P10, P23] Improve testability (medium).** Accept cache instances as parameters. Type `Any` fields on `PythonAnalysisSession`. Create test fixtures that provide minimal enrichment contexts without full pipeline setup.
