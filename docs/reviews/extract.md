# Design Review: src/extract/

**Date:** 2026-02-16
**Scope:** `src/extract/` and all subdirectories
**Focus:** All principles (1-24), with emphasis on: Ports & Adapters (P6), SRP (P3), Information Hiding (P1), Dependency Direction (P5)
**Depth:** Deep (exhaustive file-by-file)
**Files reviewed:** 50

## Executive Summary

The `src/extract/` module (~20K LOC) is a well-structured extraction subsystem with clear subpackage boundaries (coordination, extractors, infrastructure, git, python, scanning) and consistent patterns across its 9+ extractors. The primary structural weakness is the absence of a formal `Extractor` protocol -- extractors follow convention rather than contract, leading to duplicated boilerplate (session setup, option normalization, plan building, materialization) across every extractor. Each extractor file is monolithic (800-1900 lines), mixing traversal logic, row construction, parallelism, caching, and plan materialization in a single module. The coordination layer provides good shared infrastructure but couples extractors tightly to DataFusion internals. Quick wins include extracting a formal `Extractor` protocol, consolidating duplicated `_line_offsets()` helpers, and splitting monolithic extractor files into focused sub-modules.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | medium | Extractors expose internal row shapes; no stable public API boundary |
| 2 | Separation of concerns | 1 | large | medium | Each extractor mixes traversal, row building, parallelism, caching, and materialization |
| 3 | SRP (one reason to change) | 1 | large | medium | Monolithic 800-1900 line extractor files change for many reasons |
| 4 | High cohesion, low coupling | 2 | medium | low | Good subpackage boundaries but extractors coupled to DataFusion plan internals |
| 5 | Dependency direction | 2 | medium | medium | Extractors depend on engine internals rather than stable abstractions |
| 6 | Ports & Adapters | 1 | medium | high | No formal Extractor protocol; convention-only plugin model |
| 7 | DRY (knowledge) | 1 | small | medium | `_line_offsets()` duplicated across 3 extractors; session/plan boilerplate repeated |
| 8 | Design by contract | 2 | small | low | Frozen dataclasses provide structural contracts; missing postcondition guarantees |
| 9 | Parse, don't validate | 2 | small | low | `FileContext.from_repo_row()` is good boundary parsing; some scattered checks remain |
| 10 | Make illegal states unrepresentable | 2 | small | low | Good use of frozen dataclasses; some Optional fields could be structured better |
| 11 | CQS | 2 | small | low | Most functions are queries; `_drain_worklist_queue` mutates and yields |
| 12 | DI + explicit composition | 1 | medium | medium | Options assembled ad-hoc in each extractor; no composition root |
| 13 | Composition over inheritance | 2 | small | low | Option mixins use MI but are simple; no deep hierarchies |
| 14 | Law of Demeter | 1 | medium | medium | Deep chain access through session/engine/profile objects |
| 15 | Tell, don't ask | 2 | small | low | `FileContext` mostly encapsulates; some extractors re-derive data from raw fields |
| 16 | Functional core, imperative shell | 2 | medium | low | Row-building logic is mostly pure; IO/materialization at edges |
| 17 | Idempotency | 3 | - | - | SHA-based worklists and cache keys ensure idempotent re-runs |
| 18 | Determinism / reproducibility | 3 | - | - | Stable IDs, SHA256 hashing, determinism tier tracking throughout |
| 19 | KISS | 2 | small | low | Reasonable complexity given the domain; some over-engineering in SCIP streaming |
| 20 | YAGNI | 2 | small | low | Most abstractions earn their keep; TypedDict overloads are borderline speculative |
| 21 | Least astonishment | 2 | small | low | Consistent naming conventions; `extract_X_tables` vs `extract_X` inconsistency |
| 22 | Public contracts | 1 | medium | medium | No versioned protocol; `__init__.py` exports 60+ symbols without stability marking |
| 23 | Design for testability | 1 | medium | high | Extractors hard to test in isolation due to coupled session/plan/cache dependencies |
| 24 | Observability | 3 | - | - | Consistent OTel spans, event recording, and diagnostics throughout |

## Detailed Findings

### Category: Boundaries (P1-P6)

#### P1. Information hiding -- Alignment: 1/3

**Current state:**
The `src/extract/__init__.py:1-269` exports over 60 symbols from all subpackages via a lazy `__getattr__` pattern. Internal implementation types like `ExtractMaterializeOptions`, `ExtractPlanOptions`, `_RowBatcher`, and `ExtractNormalizeOptions` leak through module boundaries. Downstream consumers can (and likely do) depend on coordination-layer internals.

**Findings:**
- `src/extract/__init__.py:16-100` exports coordination internals (`ExtractMaterializeOptions`, `ExtractPlanOptions`, `extract_plan_from_rows`, `materialize_extract_plan`) that should be internal to the extract subsystem
- `src/extract/coordination/materialization.py:1-1073` exposes `ExtractMaterializeOptions` and `ExtractPlanOptions` -- these are implementation details of how extractors build plans, not concepts downstream consumers should use
- `src/extract/extractors/cst_extract.py` uses global mutable state `_CST_WORKER_STATE` (a module-level dict) for worker process repo managers, exposing cross-process implementation details
- `src/extract/extractors/bytecode_extract.py` defines 10+ internal dataclasses (`BytecodeFileContext`, `BytecodeCacheResult`, `InstructionData`, `CodeUnitKey`, etc.) that are implementation-private but share a flat namespace

**Suggested improvement:**
Restructure `src/extract/__init__.py` to export only the public surface: `ExtractSession`, `ExtractExecutionContext`, `FileContext`, and the `extract_*_tables`/`scan_*` entry points. Move coordination types like `ExtractMaterializeOptions` behind a `_coordination` prefix or stop exporting them. Document which symbols are stable public API.

**Effort:** medium
**Risk if unaddressed:** medium -- downstream consumers couple to internal types, making refactoring risky

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
Each extractor file mixes multiple concerns: source traversal/parsing, row construction, parallelism orchestration, DiskCache integration, worklist management, DataFusion plan building, and materialization. For example, `cst_extract.py` (1901 lines) contains the `CSTCollector` visitor, row accumulation, cache lookup, parallel dispatch, plan building, and table materialization all in one file.

**Findings:**
- `src/extract/extractors/cst_extract.py:1-1901` mixes LibCST traversal (visitor), row construction, FQN resolution via `FullRepoManager`, parallel worker setup, cache integration, plan building, and materialization
- `src/extract/extractors/bytecode_extract.py:1-1855` mixes dis/bytecode analysis, CFG/DFG derivation, cache integration, thread pool execution, plan building, and materialization
- `src/extract/extractors/ast_extract.py:1-1389` mixes AST walking, row building, parallel dispatch, cache integration, and plan materialization
- `src/extract/extractors/tree_sitter/extract.py:1-1578` mixes tree-sitter parsing, query execution, incremental cache, parallel workers, and plan building (though tree-sitter is better decomposed with `cache.py` and `queries.py` as separate modules)

**Suggested improvement:**
Split each monolithic extractor into three focused modules: (1) a `_traversal.py` containing the pure parsing/walking logic that produces row dictionaries, (2) a `_plan.py` handling plan building and materialization, and (3) the main entry point `extract.py` that wires them together. The tree-sitter subpackage already demonstrates this pattern partially with `cache.py` and `queries.py`.

**Effort:** large
**Risk if unaddressed:** medium -- monolithic files accumulate complexity and resist independent testing

---

#### P3. SRP (one reason to change) -- Alignment: 1/3

**Current state:**
Each extractor file changes for multiple independent reasons: schema changes, traversal algorithm changes, parallelism strategy changes, cache key format changes, DataFusion API changes, and OTel instrumentation changes.

**Findings:**
- `src/extract/extractors/ast_extract.py` changes when: (a) Python AST node types change, (b) row schema evolves, (c) parallel worker strategy changes, (d) cache key format changes, (e) DataFusion plan API changes
- `src/extract/extractors/bytecode_extract.py` changes when: (a) CPython bytecode format changes, (b) CFG/DFG derivation logic changes, (c) thread pool strategy changes, (d) cache format changes
- `src/extract/extractors/cst_extract.py` changes when: (a) LibCST visitor API changes, (b) FQN resolution changes, (c) parallel worker state management changes, (d) row schema changes
- `src/extract/scanning/repo_scan.py:1-1210` has over 1200 lines handling: file enumeration, scope resolution, blame tracking, pathspec tracing, diagnostics recording, cache management, and plan building

**Suggested improvement:**
Extract parallelism and caching into `infrastructure/` helpers that extractors call, rather than each extractor reimplementing these patterns. `repo_scan.py` should split blame/trace recording into a separate `repo_diagnostics.py` module.

**Effort:** large
**Risk if unaddressed:** medium -- changes to shared concerns (cache, parallel, plan building) require modifying every extractor

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
The subpackage structure (coordination, extractors, infrastructure, git, python, scanning) creates meaningful boundaries. Within each subpackage, modules are mostly cohesive. However, extractors are tightly coupled to DataFusion plan artifact types and session internals.

**Findings:**
- `src/extract/extractors/tree_sitter/extract.py:29-60` imports 15+ symbols from `datafusion_engine`, `extract.coordination`, and `extract.infrastructure` -- a wide coupling surface
- `src/extract/extractors/scip/extract.py:22-47` imports from 12 different modules including `datafusion_engine.arrow`, `datafusion_engine.encoding`, `datafusion_engine.expr`, `datafusion_engine.extract`, `datafusion_engine.plan`, `datafusion_engine.schema`, `datafusion_engine.session`
- Good: `src/extract/coordination/line_offsets.py` (66 lines) is a highly cohesive, self-contained module with zero coupling to DataFusion
- Good: `src/extract/git/` subpackage has clean boundaries -- modules depend on `pygit2` and each other but not on DataFusion

**Suggested improvement:**
Introduce a thin `ExtractPlanBuilder` abstraction in `coordination/` that encapsulates the DataFusion plan creation pattern, so extractors depend on one coordination-layer type instead of importing 15+ DataFusion symbols directly.

**Effort:** medium
**Risk if unaddressed:** low -- current coupling is manageable but adds friction to DataFusion API evolution

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The general dependency flow is correct: extractors depend on coordination, which depends on infrastructure. However, extractors directly import DataFusion engine internals (`DataFusionPlanArtifact`, `DataFusionRuntimeProfile`, `SessionContext`, schema alignment functions) rather than going through stable abstractions.

**Findings:**
- `src/extract/extractors/scip/extract.py:24-25` directly imports `DataFusionPlanArtifact` and `DataFusionRuntimeProfile` from `datafusion_engine` -- these are engine implementation types, not extraction contracts
- `src/extract/infrastructure/worklists.py:10-11` imports `DataFrame`, `SessionContext`, `col` directly from `datafusion` (the raw library), bypassing any abstraction layer
- `src/extract/session.py:1-61` is a thin wrapper that partially addresses this, but extractors often bypass it to access `engine_session.datafusion_profile` directly
- Good: `src/extract/coordination/context.py` defines `FileContext` and `ExtractExecutionContext` as stable domain types that extractors depend on

**Suggested improvement:**
Have extractors depend only on `ExtractSession` and `ExtractExecutionContext` for session/runtime needs. Push all `DataFusionPlanArtifact` and `DataFusionRuntimeProfile` usage into coordination-layer helpers that extractors call via a narrow interface.

**Effort:** medium
**Risk if unaddressed:** medium -- DataFusion API changes ripple through all extractors

---

#### P6. Ports & Adapters -- Alignment: 1/3

**Current state:**
There is no formal `Extractor` protocol or port definition. Each extractor follows a convention (Options dataclass -> collect rows -> build plan -> materialize) but this convention is implicit. There is no way to discover extractors programmatically, swap implementations, or verify conformance at type-check time.

**Findings:**
- No `Extractor` protocol or ABC exists anywhere in `src/extract/`
- Each extractor defines its own entry point with slightly different signatures: `extract_ast_tables(**kwargs)`, `extract_cst_tables(**kwargs)`, `extract_ts_tables(**kwargs)`, `extract_scip_tables(*, context, options)`, `scan_repo(repo_root, options)`, `scan_repo_blobs(repo_files, options)`, `scan_file_line_index(repo_files, options)`
- `src/extract/coordination/evidence_plan.py:1-220` references extractors by string name (`"ast"`, `"cst"`, `"bytecode"`, etc.) without any type-safe dispatch mechanism
- The `spec_helpers.py:1-202` module handles extractor-specific option derivation with CST-specific logic (`_apply_cst_derivations`) hardcoded into a supposedly generic helper

**Suggested improvement:**
Define an `ExtractorPort` protocol in `src/extract/coordination/ports.py`:

```python
class ExtractorPort(Protocol):
    name: str
    output_datasets: tuple[str, ...]

    def extract_plans(
        self,
        repo_files: TableLike,
        *,
        options: object,
        context: ExtractExecutionContext,
    ) -> Mapping[str, DataFusionPlanArtifact]: ...

    def extract_tables(
        self,
        repo_files: TableLike,
        *,
        options: object,
        context: ExtractExecutionContext,
    ) -> Mapping[str, TableLike | RecordBatchReaderLike]: ...
```

Register extractors in a catalog rather than importing each by name. This enables: (1) programmatic discovery, (2) conformance checking, (3) test doubles, (4) future extension without modifying coordination code.

**Effort:** medium
**Risk if unaddressed:** high -- adding a new extractor requires modifying coordination code in multiple places; testing extractors in isolation is difficult

---

### Category: Knowledge (P7-P11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
Several pieces of domain knowledge are duplicated across extractors.

**Findings:**
- **Line offset computation** is duplicated: `src/extract/coordination/line_offsets.py` provides `LineOffsets`, but `ast_extract.py`, `bytecode_extract.py`, and `symtable_extract.py` each have their own `_line_offsets()` helper that builds similar byte-to-line mappings from source text rather than using the shared `LineOffsets` class
- **Session/plan/materialization boilerplate** is repeated in every extractor: each has nearly identical blocks of code that normalize options, ensure a session, get a runtime profile, get a determinism tier, build plans, and materialize them. This pattern appears in `ast_extract.py`, `cst_extract.py`, `bytecode_extract.py`, `symtable_extract.py`, `tree_sitter/extract.py`, `scip/extract.py`, `file_index/line_index.py`, `git/blobs.py`, and `scanning/repo_scan.py`
- **Worklist construction** is repeated: every file-per-file extractor builds a `WorklistRequest` with nearly identical parameters (repo_files, output_table, runtime_profile, file_contexts, queue_name)
- **SpanSpec** appears as `SpanSpec` in `coordination/context.py:77` and as `SpanTemplateSpec` in `row_builder.py` -- near-duplicate representations of the same concept

**Suggested improvement:**
(1) Ensure all extractors use `LineOffsets` from `coordination/line_offsets.py` instead of private helpers. (2) Extract the session/plan/materialize boilerplate into a single `extract_and_materialize()` helper in coordination that each extractor calls. (3) Consolidate `SpanSpec` and `SpanTemplateSpec` into a single authoritative type.

**Effort:** small (line offsets), medium (boilerplate extraction)
**Risk if unaddressed:** medium -- semantic drift between duplicate implementations of the same concept

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Good use of frozen dataclasses for options and contexts, which enforces structural invariants. `FileContext.from_repo_row()` acts as a boundary parser. However, postconditions on extractor outputs are not enforced -- there is no contract that says "an extractor must return tables matching a specific schema."

**Findings:**
- `src/extract/coordination/schema_ops.py:1-274` provides `validate_extract_output` for schema validation, but this is applied during materialization, not as a postcondition on extractor output
- `src/extract/coordination/evidence_plan.py:1-220` defines `EvidenceRequirement` with `required_datasets` but does not validate that the extractor actually produces those datasets
- Good: `src/extract/extractors/scip/extract.py:488-503` has `assert_scip_index_health()` as an explicit precondition check

**Suggested improvement:**
Add a postcondition check in the materialization path that verifies the returned table schemas match the expected dataset schema. This could be a debug-mode assertion in `materialize_extract_plan()`.

**Effort:** small
**Risk if unaddressed:** low -- schema mismatches are caught by DataFusion at query time, but earlier detection saves debugging

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`FileContext.from_repo_row()` is a good example of parse-don't-validate: it converts a raw row mapping into a structured frozen dataclass at the boundary. However, some extractors still perform scattered validation on raw fields.

**Findings:**
- Good: `src/extract/coordination/context.py:48-93` `FileContext.from_repo_row()` parses raw rows into typed dataclass at the boundary
- Good: `src/extract/scanning/scope_rules.py:76-102` `check_scope_path()` returns a typed `ScopeRuleDecision` rather than raw booleans
- Minor: `src/extract/extractors/scip/extract.py:506-528` has multiple `_int_or_none()`, `_string_value()` parsing helpers that could be consolidated

**Suggested improvement:**
No major action needed. Minor consolidation of type-coercion helpers across SCIP extract.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Good use of frozen dataclasses throughout. `WorklistRequest`, `FileContext`, `ExtractExecutionContext` are all frozen. However, some types use `| None` optionals where more structured representations would prevent invalid combinations.

**Findings:**
- `src/extract/coordination/context.py:145-175` `ExtractExecutionContext` has 7 optional fields, many of which have co-dependencies (e.g., `session` and `runtime_spec` are alternatives, but both can be None or both set)
- `src/extract/extractors/tree_sitter/extract.py:74-99` `TreeSitterExtractOptions` has 18 fields, some mutually constraining (e.g., `incremental` requires `incremental_cache_size > 0`, `parse_callback_threshold_bytes` requires `parse_callback_chunk_size > 0`) -- these invariants are checked at runtime in `_resolve_parse_callback_options`
- Good: `src/extract/scanning/scope_manifest.py:17-48` `ScopeManifestEntry` clearly models the inclusion/exclusion decision with explicit boolean and index fields

**Suggested improvement:**
Consider splitting `ExtractExecutionContext` into a `ResolvedExtractContext` (where session is guaranteed present) vs the current builder-style context. This eliminates the need for `ensure_session()` calls scattered through extractors.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions cleanly separate queries from commands. Row-building functions are pure queries. Plan materialization functions are commands with clear side effects.

**Findings:**
- `src/extract/infrastructure/worklists.py:318-327` `_drain_worklist_queue()` both mutates the queue (popleft, pop) and yields items -- it is simultaneously a command and a query
- `src/extract/extractors/cst_extract.py` mutates global `_CST_WORKER_STATE` dict as a side effect of worker initialization, which can surprise callers
- Good: Row construction functions throughout (e.g., `_node_entry`, `_occurrence_row`, `_build_repo_file_row`) are pure queries

**Suggested improvement:**
Split `_drain_worklist_queue` into a "read queue items" step and a "clear queue" step. For the CST global state, document the side effect clearly or switch to an explicit initialization function.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (P12-P15)

#### P12. DI + explicit composition -- Alignment: 1/3

**Current state:**
Each extractor assembles its own dependency graph ad-hoc. There is no composition root or factory that wires extractors with their dependencies. Options are normalized individually in each extractor via `normalize_options()`, sessions are created on-demand via `ensure_session()`, and caches are obtained inline.

**Findings:**
- Every extractor calls `normalize_options()`, `context.ensure_session()`, `context.ensure_runtime_profile()`, and `context.determinism_tier()` in an identical sequence -- this is assembly logic duplicated 9+ times
- `src/extract/coordination/context.py:145-175` `ExtractExecutionContext.ensure_session()` creates sessions lazily, making dependency injection difficult (the session is constructed internally rather than injected)
- `src/extract/infrastructure/cache_utils.py:1-235` cache construction is done inline via `cache_for_extract(diskcache_profile_from_ctx(runtime_profile))` -- repeated in every extractor that uses caching

**Suggested improvement:**
Introduce a `ResolvedExtractContext` that is fully assembled (session, runtime_profile, determinism_tier, caches all resolved) and passed to extractors. Create a factory function `resolve_extract_context(exec_context) -> ResolvedExtractContext` that performs all the initialization once. Each extractor then receives a fully-built context instead of repeating the assembly.

**Effort:** medium
**Risk if unaddressed:** medium -- duplicated assembly logic is error-prone and hard to test

---

#### P13. Composition over inheritance -- Alignment: 2/3

**Current state:**
The codebase correctly avoids deep inheritance hierarchies. Option types use multiple inheritance through dataclass mixins, which is borderline but acceptable given the shallow depth.

**Findings:**
- `src/extract/infrastructure/options.py:1-51` defines `RepoOptions`, `WorklistQueueOptions`, `WorkerOptions`, `BatchOptions`, `ParallelOptions` as mixin dataclasses. Extractors compose them: `TreeSitterExtractOptions(RepoOptions, WorklistQueueOptions, ParallelOptions)`
- `src/extract/git/remotes.py:99-173` `RemoteAuthCallbacks(pygit2.RemoteCallbacks)` inherits from a third-party class -- acceptable for framework integration
- No deep hierarchies found; composition is the default pattern

**Suggested improvement:**
No significant action needed. The mixin pattern is the lightest-weight use of inheritance.

**Effort:** small
**Risk if unaddressed:** low

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
Extractors frequently reach deep into collaborator chains to access DataFusion primitives.

**Findings:**
- `src/extract/extractors/scip/extract.py:158` `self.ensure_session().engine_session.datafusion_profile` -- three-level chain to reach the runtime profile
- `src/extract/infrastructure/worklists.py:192-193` `runtime_profile.session_runtime().ctx` -- chain through runtime to get DataFusion context
- `src/extract/scanning/repo_scan.py:982-983` `session.engine_session.datafusion_profile` -- two-level chain
- `src/extract/git/blobs.py:336-337` `session.engine_session.datafusion_profile` -- same pattern
- This pattern appears in nearly every module that needs a DataFusion context or runtime profile

**Suggested improvement:**
Add convenience properties to `ExtractSession` (e.g., `session.runtime_profile`, `session.df_ctx`) that eliminate chain access. The session wrapper already provides `df_ctx` but extractors often bypass it.

**Effort:** medium
**Risk if unaddressed:** medium -- changes to intermediate object structure cascade widely

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`FileContext` mostly encapsulates file identity logic. However, some extractors pull raw data from contexts and re-derive information.

**Findings:**
- Good: `src/extract/coordination/context.py:48-93` `FileContext` encapsulates file identity with `from_repo_row()`, `text_from_file_ctx()`, `bytes_from_file_ctx()` helpers
- Minor: Some extractors access `file_ctx.text`, `file_ctx.data`, `file_ctx.encoding` directly to reconstruct source bytes, duplicating the logic in `bytes_from_file_ctx()`

**Suggested improvement:**
Ensure all extractors use `bytes_from_file_ctx()` and `text_from_file_ctx()` rather than manually accessing raw fields.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (P16-P18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Row-building functions (the "core" of each extractor) are mostly pure: they take a node/context and return a dictionary. IO (file reading, plan materialization, cache access) is concentrated at the edges of each extractor's `extract_*_tables()` function.

**Findings:**
- Good: `src/extract/extractors/tree_sitter/extract.py:572-607` `_node_entry()` is a pure function: node in, row dict out
- Good: `src/extract/extractors/scip/extract.py:590-607` `_metadata_row()` is a pure function
- Mixed: `src/extract/extractors/cst_extract.py` `CSTCollector` is a mutable visitor class that accumulates rows as a side effect of traversal -- this is inherent to the LibCST visitor pattern but could be wrapped in a function that returns the accumulated rows

**Suggested improvement:**
Where possible, wrap mutable visitors (CST, AST walk) in functions that return the accumulated result, keeping the mutable state internal. This is already done well for tree-sitter with `_collect_queries()` returning `_QueryRows`.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
Well aligned. SHA256-based file identity, stable cache keys, worklist deduplication, and `memoize_stampede` all ensure that re-running extraction with the same inputs produces the same results without corruption.

**Findings:**
- Good: `src/extract/infrastructure/worklists.py:150-172` `_stream_with_queue()` deduplicates by file_id + sha match
- Good: `src/extract/infrastructure/cache_utils.py:1-235` `stable_cache_key()` produces deterministic cache keys
- Good: `src/extract/scanning/repo_scan.py:726-736` `memoize_stampede` prevents thundering herd on concurrent scans

**Effort:** N/A
**Risk if unaddressed:** N/A

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Excellent. The extraction layer is built around deterministic identifiers and reproducible outputs.

**Findings:**
- Good: `src/extract/scanning/repo_scan.py:345` uses `stable_id("file", repo_id, rel_posix)` for deterministic file IDs
- Good: `src/extract/infrastructure/schema_cache.py:1-127` provides per-dataset schema fingerprints that change when schemas change
- Good: `DeterminismTier` is tracked and propagated through the extraction pipeline

**Effort:** N/A
**Risk if unaddressed:** N/A

---

### Category: Simplicity (P19-P22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
The overall design is appropriate for the domain. Some areas are more complex than necessary.

**Findings:**
- `src/extract/extractors/scip/extract.py:1-1466` is complex but justified by the SCIP protobuf format's complexity; the streaming/in-memory dual path adds necessary complexity for large indexes
- `src/extract/extractors/tree_sitter/extract.py:1468-1502` defines three near-identical `TypedDict` classes (`_TreeSitterTablesKwargs`, `_TreeSitterTablesKwargsTable`, `_TreeSitterTablesKwargsReader`) for overload support -- this could be simplified
- `src/extract/coordination/materialization.py:1-1073` is the most complex single file; it handles multiple code paths for streaming, batched, and in-memory materialization with extensive error handling

**Suggested improvement:**
Reduce the TypedDict overload pattern in `extract_ts_tables` and similar functions. The `prefer_reader: bool` overload pattern can be handled with a single return type `TableLike | RecordBatchReaderLike`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
Most abstractions are justified by current usage. The option mixins pattern is well-scoped.

**Findings:**
- `src/extract/infrastructure/options.py:1-51` defines 5 option mixins, all currently used by at least one extractor
- `src/extract/extractors/scip/extract.py:287-316` the `_worklist_queue` and persistent queue mechanism is a real feature, not speculative
- Minor: `src/extract/extractors/tree_sitter/extract.py:413-425` `_parse_callback` with mmap support for large files is justified but might not be needed for most Python files under 5MB

**Suggested improvement:**
No significant action needed.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
Naming is mostly consistent but there are some surprising inconsistencies.

**Findings:**
- Entry point naming is inconsistent: `extract_ast_tables()`, `extract_cst_tables()`, `extract_ts_tables()`, but `scan_repo()`, `scan_repo_blobs()`, `scan_file_line_index()` -- the `extract_*` vs `scan_*` distinction is not documented
- `src/extract/extractors/tree_sitter/extract.py:901-950` `extract_ts()` returns `ExtractResult[TableLike]` while `extract_ts_tables()` returns `Mapping[str, TableLike]` -- two different shapes for essentially the same operation
- Good: Options classes consistently use `*Options` suffix and are frozen dataclasses

**Suggested improvement:**
Document the `extract_*` vs `scan_*` naming convention. `extract_*` should mean "parse source and produce evidence rows" while `scan_*` should mean "enumerate/read files." Make this distinction explicit in module docstrings.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Public contracts -- Alignment: 1/3

**Current state:**
No explicit contract versioning or stability marking exists. `__init__.py` exports everything; there is no distinction between stable and unstable API surface.

**Findings:**
- `src/extract/__init__.py:1-269` exports 60+ symbols with no stability annotation
- No `__all__` list at the top-level `__init__.py` -- instead uses a lazy `__getattr__` that can export anything
- Individual subpackage `__init__.py` files do define `__all__`, which is good, but these are not hierarchically enforced
- Dataset names like `"ast_files_v1"`, `"repo_files_v1"` have version suffixes, which is good practice for schema evolution

**Suggested improvement:**
Add explicit `__all__` to `src/extract/__init__.py` limiting it to the intended public surface. Add a `_private` or `_internal` prefix to modules/types that are not part of the public API.

**Effort:** medium
**Risk if unaddressed:** medium -- consumers depend on implementation details without knowing they are unstable

---

### Category: Quality (P23-P24)

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
Extractors are difficult to test in isolation because they couple to DataFusion sessions, runtime profiles, disk caches, and file system access. Testing an extractor requires constructing a full `ExtractSession` with a `DataFusionRuntimeProfile`, which requires a `SessionContext`.

**Findings:**
- `src/extract/coordination/context.py:145-175` `ExtractExecutionContext.ensure_session()` creates a session internally, making it hard to inject a test double
- Each extractor's `extract_*_tables()` function requires a live DataFusion session to build plans and materialize results -- there is no way to test just the row-building logic in isolation
- `src/extract/extractors/cst_extract.py` relies on global mutable state `_CST_WORKER_STATE` for parallel workers, complicating test isolation
- The pure row-building functions (e.g., `_node_entry`, `_occurrence_row`) are independently testable, but they are private and not exposed for testing

**Suggested improvement:**
(1) Make `extract_*_plans()` functions (which are separate from `extract_*_tables()`) the primary testing surface -- they produce plan artifacts without requiring materialization. (2) Expose the row-building layer as a testable unit: `collect_ast_rows(file_ctx, options) -> list[Row]` as a public function. (3) Accept `ExtractSession` as a required parameter rather than using `ensure_session()` to create it internally.

**Effort:** medium
**Risk if unaddressed:** high -- difficult to test extractors confidently, leading to regression risk

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
Excellent observability coverage. Every extractor and coordination function is instrumented with OpenTelemetry spans, event recording, and diagnostics.

**Findings:**
- Good: `src/extract/extractors/scip/extract.py:269-277` `stage_span("extract.scip_index", ...)` with structured attributes
- Good: `src/extract/scanning/repo_scan.py:975-1019` `_record_repo_scope_stats()` records detailed scope statistics as structured artifacts
- Good: `src/extract/extractors/scip/extract.py:796-818` `_record_scip_health_events()` records quality events via `EngineEventRecorder`
- Good: `src/extract/extractors/tree_sitter/extract.py:1378-1404` `_file_attrs()` captures parse timing, node counts, query stats as structured metadata

**Effort:** N/A
**Risk if unaddressed:** N/A

---

## Cross-Cutting Themes

### Theme 1: Convention Over Contract in Extractor Plugin Model

**Description:** All 9+ extractors follow the same general pattern (Options -> collect rows -> build plan -> materialize) but this pattern is encoded as convention, not as a protocol or interface. This is the root cause of violations in P6 (no Extractor port), P7 (duplicated boilerplate), P12 (no composition root), and P22 (no contract boundary).

**Root cause:** The extraction layer grew organically with each extractor copy-pasting the same structure, rather than starting from a formal protocol.

**Affected principles:** P1, P3, P6, P7, P12, P22, P23

**Suggested approach:** Define an `ExtractorPort` protocol, extract shared assembly logic into a `ResolvedExtractContext`, and create a composition root that wires extractors with dependencies. This single structural change addresses multiple principle violations simultaneously.

### Theme 2: Monolithic Extractor Files

**Description:** Each extractor is a single 800-1900 line file that mixes traversal, row construction, parallelism, caching, and materialization. This concentrates multiple concerns and multiple reasons to change in one place.

**Root cause:** Initial extraction was straightforward enough to fit in one file; complexity grew without decomposition.

**Affected principles:** P2, P3, P19

**Suggested approach:** Split each extractor into `_traversal.py` (pure parsing/walking), `_plan.py` (plan building), and `extract.py` (entry point). The tree-sitter extractor already partially demonstrates this with `cache.py` and `queries.py`.

### Theme 3: Deep Chain Access to DataFusion Internals

**Description:** Extractors and coordination modules access DataFusion primitives through long accessor chains: `session.engine_session.datafusion_profile.session_runtime().ctx`. This violates the Law of Demeter and creates tight coupling to the DataFusion session hierarchy.

**Root cause:** `ExtractSession` is a thin wrapper that does not fully encapsulate the DataFusion session hierarchy.

**Affected principles:** P5, P14

**Suggested approach:** Enrich `ExtractSession` with convenience properties (`runtime_profile`, `determinism_tier`, `df_ctx`) that eliminate the need to chain through intermediate objects.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Consolidate duplicated `_line_offsets()` helpers in ast/bytecode/symtable extractors to use `LineOffsets` from `coordination/line_offsets.py` | small | Eliminates semantic duplication of byte/line conversion logic |
| 2 | P14 (Demeter) | Add `runtime_profile` and `determinism_tier` properties to `ExtractSession` to eliminate chain access | small | Reduces coupling and simplifies extractor code |
| 3 | P22 (Contracts) | Add explicit `__all__` to `src/extract/__init__.py` limiting exports to stable public API | small | Establishes clear API boundary for downstream consumers |
| 4 | P21 (Astonishment) | Document `extract_*` vs `scan_*` naming convention in module docstrings | small | Reduces confusion for new contributors |
| 5 | P11 (CQS) | Split `_drain_worklist_queue()` into separate read and clear operations | small | Cleaner separation of query/command responsibilities |

## Recommended Action Sequence

1. **Add convenience properties to `ExtractSession`** (P14, small) -- Add `runtime_profile`, `determinism_tier`, and ensure `df_ctx` is always used. This immediately simplifies every extractor file and reduces coupling.

2. **Consolidate `_line_offsets()` helpers** (P7, small) -- Replace private line-offset helpers in ast/bytecode/symtable extractors with the shared `LineOffsets.from_bytes()`. This is a safe refactor with no API change.

3. **Define explicit `__all__` for `src/extract/__init__.py`** (P22, small) -- Audit current exports and limit to the intended public surface. This establishes an API boundary.

4. **Define `ExtractorPort` protocol** (P6, medium) -- Create the formal extractor interface in `src/extract/coordination/ports.py`. Implement it for 1-2 extractors first to validate the design.

5. **Extract `ResolvedExtractContext`** (P12, medium) -- Create a fully-assembled context type and factory function that eliminates the repeated `ensure_session()` / `ensure_runtime_profile()` / `determinism_tier()` sequence in every extractor.

6. **Split monolithic extractors** (P2/P3, large) -- Start with the largest (`cst_extract.py` at 1901 lines) by extracting the `CSTCollector` visitor and row builders into a `_traversal.py` module. Apply the same pattern to other extractors incrementally.

7. **Extract shared plan/materialize helper** (P7, medium) -- Create a coordination-layer function that encapsulates the normalize-options -> build-plan -> materialize-plan sequence, reducing each extractor's boilerplate to a single call.

8. **Improve testability** (P23, medium) -- Expose row-building functions as testable units and accept sessions as required parameters rather than creating them internally.
