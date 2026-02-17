# Design Review: src/extract + src/extraction

**Date:** 2026-02-17
**Scope:** `src/extract/` (68 files, ~20.4K LOC) + `src/extraction/` (13 files, ~3.6K LOC)
**Focus:** All principles (1-24) with emphasis on boundaries, composition, quality
**Depth:** moderate
**Files reviewed:** 26

## Executive Summary

The extract/extraction layer demonstrates strong knowledge consolidation (line offsets unified, row builders canonical, schema derivation centralized) and good separation between evidence-layer extractors and orchestration plumbing. However, the `ExtractorPort` protocol is defined but never implemented -- extractors follow a structural convention but do not satisfy a formal interface. The orchestrator (`extraction/orchestrator.py`) contains 4x duplicated session-bootstrap boilerplate and exhibits a pattern of reconstructing `resolve_runtime_profile("default") -> build_engine_session -> ExtractSession` sequences that should be hoisted to a single factory. The coordination/materialization module at 1075 LOC is the most complex file in scope and handles too many concerns (plan building, normalization, streaming, writing, view registration, artifact recording, schema validation). Dependency direction is mostly correct but `extraction/` reaches into `relspec.pipeline_policy` and `semantics.registry` at import time, creating circular-adjacent dependencies that place the extraction shell at the engine ring rather than the middle ring.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | low | `__init__.py` exports ~60 symbols including internals prefixed with `_` |
| 2 | Separation of concerns | 1 | medium | medium | `coordination/materialization.py` mixes plan building, normalization, IO writing, view registration, and diagnostics |
| 3 | SRP | 1 | medium | medium | `orchestrator.py` handles session setup, scanning, parallel dispatch, delta writes, and contract resolution |
| 4 | High cohesion, low coupling | 2 | small | low | Extractors are cohesive internally; coupling to `datafusion_engine` is broad (25 files import from it) |
| 5 | Dependency direction | 1 | medium | high | `extraction/` imports from `relspec`, `semantics` -- engine-ring reaching inward |
| 6 | Ports & Adapters | 1 | medium | medium | `ExtractorPort` defined but never implemented; no adapter boundary for Delta/DataFusion IO |
| 7 | DRY | 2 | small | medium | Session bootstrap duplicated 4x in orchestrator; `_line_offsets` duplication resolved |
| 8 | Design by contract | 2 | small | low | `RunExtractionRequestV1` uses msgspec; schema contracts validated post-materialization |
| 9 | Parse, don't validate | 2 | small | low | `normalize_extraction_options()` parses at boundary; some `getattr` probing remains |
| 10 | Illegal states | 2 | small | low | Options structs are frozen; `_validate_diff_options` guards inconsistent state |
| 11 | CQS | 2 | small | low | Most functions follow CQS; `_ExtractionRunState` mutation methods are clearly separated |
| 12 | DI + explicit composition | 1 | medium | medium | Orchestrator creates sessions inline 4x rather than accepting injected session |
| 13 | Composition over inheritance | 2 | small | low | Option mixins use dataclass inheritance; no deep hierarchies |
| 14 | Law of Demeter | 1 | small | medium | Chained access like `engine_session.engine_runtime.datafusion_profile.session_runtime().ctx` |
| 15 | Tell, don't ask | 2 | small | low | `ExtractExecutionContext.ensure_session()` encapsulates fallback logic |
| 16 | Functional core | 2 | small | low | Visitors/builders are mostly pure transforms; IO at orchestrator edge |
| 17 | Idempotency | 2 | small | low | Delta writes use `mode="overwrite"`; repo scan is repeatable |
| 18 | Determinism | 3 | - | - | Byte spans canonical; `DeterminismTier` threaded through all plans |
| 19 | KISS | 2 | small | low | Most extractors are straightforward; materialization adds unnecessary complexity |
| 20 | YAGNI | 2 | small | low | `_coerce_to_table` probes for multiple interfaces speculatively |
| 21 | Least astonishment | 1 | small | medium | Two top-level packages `extract/` vs `extraction/` with unclear boundary |
| 22 | Public contracts | 2 | small | low | `__all__` declared everywhere; `_v1` naming convention for versioned outputs |
| 23 | Design for testability | 1 | medium | medium | Orchestrator requires full DataFusion session; no DI seams for unit testing |
| 24 | Observability | 3 | - | - | Consistent OTel spans, structured diagnostics, `EngineEventRecorder` pattern |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The `src/extract/__init__.py` (lines 112-192) defines a `_EXPORTS` lazy-loading map with ~60 entries. While lazy-loading is good engineering, the `__all__` list (lines 209-268) exports private-prefixed names from internal modules (e.g., `_extract_ast_for_context` from `extract.extractors.ast`).

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extract/__init__.py:14-17` exports `_AstWalkAccumulator` and `_AstWalkResult` -- private types leaked through the `__init__` re-exports of `extract.extractors.ast.__init__.py:12`.
- `/Users/paulheyse/CodeAnatomy/src/extract/extractors/ast/__init__.py:18-19` re-exports `_extract_ast_for_context` and `_AstWalkAccumulator` as public.
- `/Users/paulheyse/CodeAnatomy/src/extract/extractors/cst/__init__.py:6-7` re-exports `_qname_keys` (a private helper).

**Suggested improvement:**
Remove underscore-prefixed names from extractor `__init__.py` `__all__` lists. These are internal implementation details that should only be accessed within their own module package. External callers should use the public `extract_ast_tables` / `extract_cst_tables` entry points.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns -- Alignment: 1/3

**Current state:**
`src/extract/coordination/materialization.py` (1075 LOC) handles plan construction, query projection, evidence filtering, normalization, streaming materialization, table materialization, Delta writes, view registration, artifact recording, UDF parity checks, and schema contract validation -- all in a single module.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extract/coordination/materialization.py:589-664`: `_write_and_record_extract_output` combines Delta write, view registration, artifact recording, and schema validation in a single 75-line function with 3 sequential try/except blocks.
- `/Users/paulheyse/CodeAnatomy/src/extract/coordination/materialization.py:694-781`: `materialize_extract_plan` is 87 lines mixing streaming/table decision logic, normalization, execution, writing, and reader conversion.
- `/Users/paulheyse/CodeAnatomy/src/extract/coordination/materialization.py:1040-1056`: `_record_extract_udf_parity` introduces UDF parity diagnostics into the materialization module -- an unrelated concern.

**Suggested improvement:**
Split `materialization.py` into at least three modules: (1) `plan_building.py` for plan construction and projection logic (lines 149-378), (2) `plan_execution.py` for execution, normalization, and materialization (lines 380-857), (3) `plan_observability.py` for view registration, artifact recording, schema validation, and UDF parity (lines 899-1057). The `_write_and_record_extract_output` function should be decomposed into a pipeline of independent steps.

**Effort:** medium
**Risk if unaddressed:** medium -- the module is already at the complexity threshold where adding new functionality risks breaking existing behavior.

---

#### P3. SRP -- Alignment: 1/3

**Current state:**
`src/extraction/orchestrator.py` (975 LOC) orchestrates the entire extraction pipeline but also handles session construction, Delta IO, and table coercion. It changes for multiple reasons: adding a new extractor, changing session construction, changing the Delta write strategy, or changing the contract mapping.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:318-341`: `_write_delta` is an inline Delta writer that duplicates functionality available through the `extraction.materialize_pipeline` module.
- `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:933-969`: `_coerce_to_table` uses `getattr`-based duck typing to handle multiple result types -- this is a generic utility mixed into the orchestrator.
- `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:562-601`: `_extract_ast` function bodies are nearly identical to `_extract_cst`, `_extract_tree_sitter`, `_extract_bytecode`, and `_extract_symtable` -- each follows the same call-get-coerce pattern.

**Suggested improvement:**
Extract `_write_delta` and `_coerce_to_table` into a shared utility module (e.g., `extraction.io_helpers`). Consolidate the `_extract_*` functions into a single `_run_extractor` generic function parameterized by extractor name and entry point, reducing the 5 near-identical functions to one.

**Effort:** medium
**Risk if unaddressed:** medium -- each new extractor requires copy-pasting a ~40-line function.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
Individual extractors (AST, CST, bytecode, symtable, tree-sitter) are internally cohesive -- each has a clean `builders.py` + `builders_runtime.py` + `visitors.py` + `setup.py` structure. However, 25 files in `src/extract/` import from `datafusion_engine`, creating broad coupling to the query engine layer.

**Findings:**
- Extractor builders_runtime files (`ast/builders_runtime.py`, `cst/builders_runtime.py`, etc.) each import 10-15 symbols from `datafusion_engine` sub-packages.
- `/Users/paulheyse/CodeAnatomy/src/extract/coordination/materialization.py:17-52` imports from 12 distinct `datafusion_engine` sub-modules.
- The `extract.session.ExtractSession` class (at `/Users/paulheyse/CodeAnatomy/src/extract/session.py:21`) wraps `EngineSession` and proxies 4 properties through it, suggesting the wrapping adds little value.

**Suggested improvement:**
Consider introducing a thin adapter interface (e.g., `ExtractExecutionSurface`) that captures the minimal DataFusion capabilities needed by extractors (register table, execute plan, materialize), reducing the 12-import fan-in from `datafusion_engine` to a single protocol import.

**Effort:** medium
**Risk if unaddressed:** low -- existing code works; this is a maintainability improvement.

---

#### P5. Dependency direction -- Alignment: 1/3

**Current state:**
Per the dependency direction map, extract/extraction should be inner-to-middle ring. However, `extraction/` reaches into engine-ring modules at import time.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extraction/engine_session_factory.py:23`: `from relspec.pipeline_policy import DiagnosticsPolicy` -- extraction directly imports from `relspec` (middle ring), which is acceptable.
- `/Users/paulheyse/CodeAnatomy/src/extraction/semantic_boundary.py:23`: `from semantics.registry import SEMANTIC_MODEL` -- extraction reaches into the semantics module. This creates a dependency from the extraction orchestration shell upward into the semantic layer, which is in the same ring but logically downstream.
- `/Users/paulheyse/CodeAnatomy/src/extract/coordination/materialization.py:67-68`: TYPE_CHECKING imports from `semantics.compile_context` and `semantics.program_manifest` -- these are acceptable as type-only but indicate the coordination layer has semantic-layer awareness.

**Suggested improvement:**
Move `extraction/semantic_boundary.py` into the `semantics/` module where it logically belongs -- it queries `SEMANTIC_MODEL` which is a semantics-layer concept. The `extraction/` package should not need to know about semantic view names. If orchestration needs a "are required views registered" check, inject the view name list as a parameter rather than importing the semantic model.

**Effort:** medium
**Risk if unaddressed:** high -- as semantics evolves, changes to `SEMANTIC_MODEL` will cascade into the extraction shell, creating fragile coupling.

---

#### P6. Ports & Adapters -- Alignment: 1/3

**Current state:**
An `ExtractorPort` protocol exists at `/Users/paulheyse/CodeAnatomy/src/extract/protocols.py:13` defining `name`, `output_names`, and `extract()`. However, no extractor implementation satisfies this protocol. The orchestrator invokes extractors directly through per-extractor functions (`_extract_ast`, `_extract_cst`, etc.) rather than through the port.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extract/protocols.py:13-33`: `ExtractorPort` defines `extract(file_contexts, execution_context) -> Mapping[str, pa.Table]`, but no class in `src/extract/extractors/` implements it.
- Grep for `ExtractorPort` across all of `src/` returns only the definition file -- zero usages.
- Delta IO in the orchestrator (`_write_delta` at line 318) directly calls `deltalake.write_deltalake` with no adapter abstraction. This makes testing the orchestrator impossible without a Delta Lake installation.

**Suggested improvement:**
Either (a) implement `ExtractorPort` on each extractor and use it in the orchestrator's `_build_stage1_extractors` to iterate over a registry of ports, or (b) remove the protocol if it serves no purpose. For Delta IO, introduce a `StoragePort` protocol that the orchestrator depends on, with a `DeltaStorageAdapter` implementation and a `MemoryStorageAdapter` for testing.

**Effort:** medium
**Risk if unaddressed:** medium -- the protocol creates a false promise of pluggability. The lack of a storage adapter makes the orchestrator untestable in isolation.

---

### Category: Knowledge (7-11)

#### P7. DRY -- Alignment: 2/3

**Current state:**
The `_line_offsets` duplication issue has been resolved -- `LineOffsets` is now consolidated in `extract.coordination.line_offsets`. However, session bootstrap code is duplicated 4 times in the orchestrator.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:381-383`, `:535-537`, `:840-842`, `:891-893`: The pattern `resolve_runtime_profile("default") -> build_engine_session(runtime_spec=runtime_spec) -> ExtractSession(engine_session=engine_session)` is repeated verbatim 4 times.
- `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:562-601` vs `:603-641` vs `:644-682`: `_extract_ast`, `_extract_cst`, `_extract_tree_sitter` share identical structure with only the import and output key differing.

**Suggested improvement:**
Extract a `_build_default_session() -> ExtractSession` factory and call it once, threading the session into downstream stages. For the extractor wrappers, create a generic `_run_table_extractor(extractor_fn, output_key, ...)` function parameterized by the entry point and output table name.

**Effort:** small
**Risk if unaddressed:** medium -- drift between the 4 session-construction sites or the 5 extractor wrapper functions will cause subtle behavioral inconsistencies.

---

#### P8. Design by contract -- Alignment: 2/3

**Current state:**
Contracts are enforced at key boundaries: `RunExtractionRequestV1` uses `msgspec.Struct` with frozen=True, `ExtractionRunOptions` validates diff option consistency, and schema contracts are validated post-materialization. However, `scip_index_config` in the orchestrator is typed as `object | None` with `getattr` probing.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:788-791`: `scip_index_path = getattr(scip_index_config, "index_path_override", None)` followed by a fallback `getattr(..., "scip_index_path", None)` -- this is structural duck typing that should be a protocol or typed struct.
- `/Users/paulheyse/CodeAnatomy/src/extraction/contracts.py:19`: `scip_index_config: object | None = None` -- the `object` type provides no contract whatsoever.

**Suggested improvement:**
Define a `ScipIndexConfig` protocol or msgspec struct with explicit `index_path_override` and `scip_index_path` fields. Update `RunExtractionRequestV1` to use it instead of `object | None`.

**Effort:** small
**Risk if unaddressed:** low -- the current `getattr` probing works but defeats static analysis.

---

#### P9. Parse, don't validate -- Alignment: 2/3

**Current state:**
`normalize_extraction_options()` at `/Users/paulheyse/CodeAnatomy/src/extraction/options.py:46` is a good example of parse-don't-validate: it accepts heterogeneous input (mapping, struct, or None) and produces a canonical `ExtractionRunOptions` struct. `FileContext.from_repo_row()` similarly parses row data into a typed context.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:955-958`: `_coerce_to_table` uses `getattr(value, "to_arrow_table", None)` for duck-typing coercion -- this is validation-style probing rather than parsing into a typed boundary.
- `/Users/paulheyse/CodeAnatomy/src/extract/coordination/context.py:60-65`: `_payload_from_row` properly parses a raw mapping into a typed `RepoFileRow` via `msgspec.convert`.

**Suggested improvement:**
Replace `_coerce_to_table` with explicit type-checking at the boundary where the result is produced, or use a `TableLike` union type with a proper conversion function in `datafusion_engine.arrow.interop`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Illegal states -- Alignment: 2/3

**Current state:**
Options structs (`AstExtractOptions`, `CstExtractOptions`, etc.) are frozen dataclasses, preventing post-construction mutation. `ExtractionRunOptions` is a frozen `msgspec.Struct`. The `_validate_diff_options` function at `/Users/paulheyse/CodeAnatomy/src/extraction/options.py:161-169` prevents the illegal state of `changed_only=True` without both refs.

**Findings:**
- The `options: Mapping[str, object] | None` field on `RunExtractionRequestV1` (at `/Users/paulheyse/CodeAnatomy/src/extraction/contracts.py:23`) allows arbitrary untyped data, which could contain invalid keys silently ignored.
- `_ExtractionRunState` at `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:47-52` is a mutable dataclass used as an accumulator -- acceptable for orchestration but its fields allow inconsistent intermediate states (e.g., `semantic_input_locations` populated before `delta_locations` are finalized).

**Suggested improvement:**
Consider making the `options` field on `RunExtractionRequestV1` an `ExtractionRunOptions | None` instead of `Mapping[str, object] | None`, moving parsing responsibility to the caller.

**Effort:** small
**Risk if unaddressed:** low

---

#### P11. CQS -- Alignment: 2/3

**Current state:**
Most functions follow CQS. Pure queries like `dataset_schema()`, `extract_metadata()` are separated from commands like `_write_delta()`. The mutable `_ExtractionRunState` accumulates results but mutations are contained in the orchestrator.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extract/coordination/materialization.py:694-781`: `materialize_extract_plan` both executes plans (side-effecting) and returns results, but this is a natural materialization pattern where the return value is the primary intent.
- `/Users/paulheyse/CodeAnatomy/src/extract/row_builder.py:459-461`: `ExtractionBatchBuilder.clear()` is a clean command; `build()` is a clean query. `build_and_clear()` at line 483 combines both but is clearly named.

**Suggested improvement:**
No critical issues. The `build_and_clear()` method could be split into separate calls at the call site, but the current naming is sufficiently clear.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 1/3

**Current state:**
The orchestrator creates dependencies inline rather than accepting them. Each `_extract_*` and `_run_python_*` function independently constructs `resolve_runtime_profile("default") -> build_engine_session -> ExtractSession`, making it impossible to inject test doubles.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:381-383`: `_run_repo_scan` constructs its own session.
- `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:535-537`: `_build_stage1_extractors` constructs its own session.
- `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:840-842`: `_run_python_imports` constructs its own session.
- `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:891-893`: `_run_python_external` constructs its own session.
- The public entry point `run_extraction(request)` at line 65 does not accept a session parameter.

**Suggested improvement:**
Add an optional `session: ExtractSession | None = None` parameter to `RunExtractionRequestV1` or `run_extraction()`. Construct the session once at the top of `run_extraction()` and thread it to all stages. This eliminates 4 duplicate constructions and enables test injection.

**Effort:** medium
**Risk if unaddressed:** medium -- testing the orchestrator currently requires a full DataFusion runtime.

---

#### P13. Composition over inheritance -- Alignment: 2/3

**Current state:**
Extractor option classes use multiple inheritance from option mixins (`RepoOptions`, `WorklistQueueOptions`, `WorkerOptions`, `ParallelOptions`), which is a shallow, mixin-style pattern. No deep class hierarchies exist.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extract/extractors/symtable_extract.py:62`: `SymtableExtractOptions(RepoOptions, WorklistQueueOptions, WorkerOptions)` -- 3-deep mixin chain.
- `/Users/paulheyse/CodeAnatomy/src/extract/extractors/ast/setup.py:18`: `AstExtractOptions(RepoOptions, WorklistQueueOptions, ParallelOptions)` -- `ParallelOptions` inherits from `WorkerOptions` and `BatchOptions`.
- These are frozen dataclasses used as configuration structs, so the inheritance is benign composition of fields.

**Suggested improvement:**
No change needed. The mixin pattern is appropriate for accumulating configuration fields without deep behavior hierarchies.

**Effort:** small
**Risk if unaddressed:** low

---

#### P14. Law of Demeter -- Alignment: 1/3

**Current state:**
Several access chains traverse 3+ levels of object structure, especially around session and profile resolution.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extraction/engine_session.py:45`: `self.engine_runtime.datafusion_profile.session_runtime().ctx` -- 4 levels of chained access.
- `/Users/paulheyse/CodeAnatomy/src/extraction/engine_session.py:55`: `self.engine_runtime.datafusion_profile.session_runtime()` -- 3 levels.
- `/Users/paulheyse/CodeAnatomy/src/extract/session.py:57`: `self.engine_session.surface_policy.determinism_tier` -- 3 levels.
- `/Users/paulheyse/CodeAnatomy/src/extraction/diagnostics.py:136`: `self.runtime_profile.diagnostics.diagnostics_sink` -- 3 levels.

**Suggested improvement:**
Add direct-access methods to `EngineSession` for commonly needed surfaces: `def session_context(self) -> SessionContext`, `def determinism_tier(self) -> DeterminismTier`. This is partially done already (`df_ctx()`, `df_runtime()`) but the property at line 57 of `session.py` still traverses through `engine_session`.

**Effort:** small
**Risk if unaddressed:** medium -- refactoring the internal structure of `EngineRuntime` or `DataFusionRuntimeProfile` will cascade through many call sites.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
`ExtractExecutionContext.ensure_session()` is a good example of tell-don't-ask: it encapsulates the fallback logic for session resolution. `ExtractionBatchBuilder` similarly encapsulates row accumulation and batch building.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extract/coordination/context.py:124-135`: `ensure_session()` handles the "use provided or create default" logic internally.
- `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:593-600`: `_extract_ast` asks for `outputs.get("ast_files")`, checks if None, and raises -- this pattern is repeated 5 times. A `_require_output(outputs, key)` helper exists at line 344 for repo scan but is not reused.

**Suggested improvement:**
Generalize `_require_repo_scan_table` at line 344 into a `_require_output(outputs: dict, key: str, extractor: str) -> pa.Table` that is used by all `_extract_*` functions.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 2/3

**Current state:**
Extractor visitors (`ast/visitors.py`, `cst/visitors.py`, `bytecode/visitors.py`, `tree_sitter/visitors.py`) contain mostly pure transformations -- they walk data structures and produce row dicts. The IO shell is concentrated in `builders_runtime.py` files and the orchestrator. This is a good separation.

**Findings:**
- `_walk_symtable` at `/Users/paulheyse/CodeAnatomy/src/extract/extractors/symtable_extract.py:402-443` is a pure tree-walking function producing data.
- `_extract_symtable_for_context` at line 320 mixes pure computation with caching IO (lines 337-369).
- The orchestrator's `_run_parallel_stage1_extractors` at line 222 is appropriately shell-level code handling IO, timing, and error recording.

**Suggested improvement:**
Separate caching from extraction in `_extract_symtable_for_context`: have the pure extraction as one function and a caching decorator or wrapper as another. This pattern exists in other extractors (e.g., `_ast_row_worker`) and could be applied consistently.

**Effort:** small
**Risk if unaddressed:** low

---

#### P17. Idempotency -- Alignment: 2/3

**Current state:**
Delta writes use `mode="overwrite"`, making re-runs idempotent. The orchestrator accumulates state in `_ExtractionRunState` which is constructed fresh each run. Cache operations use stable keys based on file identity and content hash.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:339`: `deltalake.write_deltalake(loc_str, table, mode="overwrite")` -- correct idempotent behavior.
- Cache keys at `/Users/paulheyse/CodeAnatomy/src/extract/extractors/symtable_extract.py:128-131` include `file_sha256` and `symtable_files_fingerprint()`, ensuring content-addressed caching.

**Suggested improvement:**
No significant issues.

**Effort:** small
**Risk if unaddressed:** low

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
Determinism is well-handled. `DeterminismTier` is threaded through all plan construction and materialization paths. Byte spans are canonical. Plan fingerprints are computed and recorded for reproducibility auditing.

**Findings:**
- `DeterminismTier` appears in `ExtractSession`, `ExtractMaterializeOptions`, `materialize_extract_plan`, and all builders_runtime files.
- File-level extraction keys include `file_sha256` for content-addressed determinism.

**Suggested improvement:**
None needed. This is well-aligned with the codebase's core determinism contract.

**Effort:** -
**Risk if unaddressed:** -

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 2/3

**Current state:**
Individual extractors are straightforward -- each follows a predictable `setup -> visitors -> builders -> builders_runtime` module structure. The materialization path, however, is more complex than necessary.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extract/coordination/materialization.py:667-691`: `_materialize_streaming_output` executes the stream twice -- once for writing and once for the return reader -- adding complexity for the streaming path.
- `/Users/paulheyse/CodeAnatomy/src/extract/coordination/materialization.py:814-856`: `materialize_extract_plan_reader` reconstructs options to force `prefer_reader=True`, duplicating option handling.

**Suggested improvement:**
Evaluate whether the double-stream execution is necessary or if the write can share the reader with the return path via tee/buffering.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
The `ExtractorPort` protocol at `/Users/paulheyse/CodeAnatomy/src/extract/protocols.py:13` defines an interface that no extractor implements. This is speculative generality -- the protocol exists but serves no purpose.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extract/protocols.py:13-33`: `ExtractorPort` is defined with 3 abstract members but has zero implementations and zero call sites.
- `/Users/paulheyse/CodeAnatomy/src/extract/row_builder.py:449-457`: `ExtractionBatchBuilder.extend()` is documented as "alias for add_rows" -- a convenience method with no distinct behavior.

**Suggested improvement:**
Either implement `ExtractorPort` across extractors (making the protocol useful) or remove it until there is a concrete use case requiring polymorphic extractor dispatch. The `extend()` alias is harmless but unnecessary.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment -- Alignment: 1/3

**Current state:**
The existence of two separate top-level packages `src/extract/` and `src/extraction/` is the most astonishing aspect of this codebase region. Their names differ by only 3 characters, and their responsibilities overlap.

**Findings:**
- `src/extract/` contains the core extractors, coordination, scanning, and infrastructure (~20.4K LOC).
- `src/extraction/` contains the orchestrator, engine session, runtime profile, diagnostics, and contracts (~3.6K LOC).
- `src/extract/session.py` imports from `src/extraction/engine_session.py` and `src/extraction/runtime_profile.py` -- the dependency goes from `extract` to `extraction`.
- `src/extraction/orchestrator.py` imports from `src/extract/` in 15+ places -- the dependency goes from `extraction` back to `extract`.
- A new contributor would not know which package to look in without reading both.

**Suggested improvement:**
This is a structural naming issue. Consider renaming `src/extraction/` to `src/extraction_runtime/` or `src/engine_orchestration/` to make the distinction clear. Alternatively, merge the 13 `extraction/` files into `extract/` as a sub-package (e.g., `extract/runtime/`), since the orchestrator already depends heavily on `extract/` internals.

**Effort:** medium
**Risk if unaddressed:** medium -- naming confusion slows onboarding and increases the likelihood of changes landing in the wrong package.

---

#### P22. Public contracts -- Alignment: 2/3

**Current state:**
All modules declare `__all__`. The `_v1` suffix convention for versioned output tables (`repo_files_v1`, `symtable_files_v1`, `bytecode_files_v1`) provides clear versioning. `RunExtractionRequestV1` uses msgspec with explicit field names.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extraction/contracts.py:14-24`: `RunExtractionRequestV1` is a well-defined versioned contract.
- `/Users/paulheyse/CodeAnatomy/src/extraction/contracts.py:27-39`: `_COMPAT_ALIASES` provides explicit legacy-to-canonical name mapping.
- `/Users/paulheyse/CodeAnatomy/src/extract/__init__.py:209-268`: The `__all__` list is comprehensive with 58 entries.

**Suggested improvement:**
No critical issues. The `_COMPAT_ALIASES` mapping should include a deprecation timeline or migration plan documentation.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 1/3

**Current state:**
The orchestrator and materialization modules are difficult to test in isolation because they directly construct sessions and call Delta Lake at execution time. The test suite for `extract/` has 12 test files, focusing on spans, schemas, and protocols -- but no orchestrator unit tests exist.

**Findings:**
- `/Users/paulheyse/CodeAnatomy/tests/unit/extract/test_protocols.py` tests the protocol definition but there are no tests for `src/extraction/orchestrator.py`.
- `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:318-341`: `_write_delta` directly calls `deltalake.write_deltalake` with no injection point.
- All `_extract_*` functions import their dependencies inline (e.g., `from extract.extractors.ast import extract_ast_tables` at line 586) -- this enables lazy loading but makes mocking difficult.
- Pure functions like `_walk_symtable`, `_scope_row`, `_symbol_rows_for_scope` are testable and tested (via `test_symtable_spans.py`).

**Suggested improvement:**
(1) Accept `ExtractSession` as a parameter in `run_extraction()` rather than constructing it 4 times internally. (2) Abstract the Delta write behind a `StoragePort` protocol that can be replaced with an in-memory fake for testing. (3) Add unit tests for `run_extraction` with injected dependencies.

**Effort:** medium
**Risk if unaddressed:** medium -- the orchestrator is the primary integration point but has zero unit test coverage.

---

#### P24. Observability -- Alignment: 3/3

**Current state:**
Observability is excellent. Every stage of the extraction pipeline is instrumented with OpenTelemetry spans (`stage_span`), structured error recording (`record_error`), timing metrics (`record_stage_duration`), and diagnostic artifacts (`EngineEventRecorder`).

**Findings:**
- `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:166-170`: Repo scan wrapped in `stage_span("extraction.repo_scan", ...)`.
- `/Users/paulheyse/CodeAnatomy/src/extraction/orchestrator.py:238-243`: Each parallel extractor wrapped in `stage_span(f"extraction.{name}", ...)`.
- `/Users/paulheyse/CodeAnatomy/src/extraction/diagnostics.py:122-260`: `EngineEventRecorder` provides typed, structured recording methods for plan execution, extract writes, quality events, cache stats, and Delta maintenance.
- `/Users/paulheyse/CodeAnatomy/src/extract/extractors/symtable_extract.py:907-912`: Extractor-level span with `SCOPE_EXTRACT`.

**Suggested improvement:**
None needed. The observability pattern is consistent and well-structured across both modules.

**Effort:** -
**Risk if unaddressed:** -

---

## Cross-Cutting Themes

### Theme 1: Orchestrator Boilerplate and Session Duplication

**Root cause:** `run_extraction()` does not accept a session parameter, forcing each stage to independently construct a `RuntimeProfileSpec -> EngineSession -> ExtractSession` chain.

**Affected principles:** P3 (SRP), P7 (DRY), P12 (DI), P23 (Testability)

**Suggested approach:** Thread a single `ExtractSession` through the pipeline by constructing it once at the top of `run_extraction()`. Add it as an optional field on `RunExtractionRequestV1` or as a separate parameter.

### Theme 2: Materialization Module Overload

**Root cause:** `coordination/materialization.py` grew organically as the single place where plans become tables. It now handles 6 distinct concerns in 1075 LOC.

**Affected principles:** P2 (SoC), P3 (SRP), P19 (KISS)

**Suggested approach:** Decompose into plan_building, plan_execution, and plan_observability modules. Each function should have a single responsibility axis.

### Theme 3: Package Naming Confusion (extract vs extraction)

**Root cause:** The `extraction/` package was likely created as an orchestration layer above `extract/`, but the names are too similar and the dependency relationship is bidirectional.

**Affected principles:** P21 (Least astonishment), P5 (Dependency direction)

**Suggested approach:** Rename `extraction/` to something clearly distinct (e.g., `extract_runtime/` or `engine_orchestration/`) or merge it as a sub-package of `extract/`.

### Theme 4: Phantom Protocol (ExtractorPort)

**Root cause:** `ExtractorPort` was designed as a future unification point for extractors but was never implemented.

**Affected principles:** P6 (Ports & Adapters), P20 (YAGNI)

**Suggested approach:** Either implement the protocol across all extractors and use it in the orchestrator, or remove it to reduce conceptual overhead.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Extract `_build_default_session()` to eliminate 4x session construction duplication in orchestrator | small | Eliminates 4 duplicate code blocks, reduces maintenance risk |
| 2 | P14 (Demeter) | Add `session_context()` and `determinism_tier()` direct methods to `EngineSession` | small | Reduces 4-level chained access patterns across 6+ call sites |
| 3 | P8 (Contract) | Define `ScipIndexConfig` protocol to replace `object | None` typing | small | Enables static analysis for SCIP config fields |
| 4 | P1 (Hiding) | Remove `_`-prefixed names from extractor `__init__.py` `__all__` lists | small | Reduces accidental coupling to internal implementation details |
| 5 | P15 (Tell) | Generalize `_require_repo_scan_table` into `_require_output` and reuse in all `_extract_*` functions | small | Eliminates 5 near-identical null-check-and-raise patterns |

## Recommended Action Sequence

1. **Session deduplication (P7, P12, P23):** Add `ExtractSession` factory method and thread it through `run_extraction()`. This is the highest-value change because it simultaneously reduces duplication, enables DI, and improves testability.

2. **Orchestrator consolidation (P3, P7):** After session deduplication, consolidate the 5 `_extract_*` functions into a single generic `_run_extractor(name, entry_point, output_key)`. This depends on step 1 because the generic function needs the shared session.

3. **Materialization decomposition (P2, P3):** Split `coordination/materialization.py` into plan_building, plan_execution, and plan_observability. This is independent of steps 1-2 and can proceed in parallel.

4. **ExtractorPort decision (P6, P20):** Decide whether to implement the protocol or remove it. If implementing, this depends on step 2 (the generic extractor dispatch is the natural place to use the port).

5. **Package naming (P21):** Rename `extraction/` to a clearly distinct name. This should be done last as it is a mechanical rename with broad import changes. Depends on steps 1-2 being complete to minimize the blast radius.

6. **Dependency direction fix (P5):** Move `semantic_boundary.py` into `semantics/` where it belongs. This is independent and can be done at any point.
