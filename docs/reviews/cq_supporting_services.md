# Design Review: CQ Supporting Services

**Date:** 2026-02-16
**Scope:** `tools/cq/neighborhood/`, `tools/cq/index/`, `tools/cq/introspection/`, `tools/cq/astgrep/`, `tools/cq/utils/`, `tools/cq/perf/`
**Focus:** All principles (1-24)
**Depth:** deep
**Files reviewed:** 31

## Executive Summary

The six supporting-service modules demonstrate strong structural discipline overall, with clean data contracts, well-separated concerns in most subsystems, and good testability design. The principal weaknesses are: (1) a duplicated `plan_feasible_slices` function between `neighborhood/contracts.py` and `neighborhood/bundle_builder.py`; (2) leakage of a private symbol (`_SELF_CLS`) across module boundaries in `index/`; (3) the `introspection/` module packing three unrelated analysis domains into a single package without internal cohesion boundaries; and (4) the `utils/interval_index.py` module holding an upward dependency on `astgrep/sgpy_scanner.py`, coupling a generic data structure to a domain-specific record type. Quick wins center on eliminating the `plan_feasible_slices` duplication, promoting `_SELF_CLS` to a public constant, and removing the `IntervalIndex` -> `SgRecord` coupling.

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 2 | small | medium | `_SELF_CLS` private symbol imported across module boundaries |
| 2 | Separation of concerns | 2 | medium | low | `introspection/` mixes bytecode, symtable, and CFG without separation |
| 3 | SRP | 2 | medium | low | `files.py` handles tabulation, filtering, untracked scanning, and scope normalization |
| 4 | High cohesion, low coupling | 2 | small | medium | `utils/interval_index.py` couples upward to `astgrep/sgpy_scanner.py` |
| 5 | Dependency direction | 2 | small | medium | `interval_index.py` depends on domain record type instead of abstracting |
| 6 | Ports & Adapters | 2 | small | low | `gitignore.py` has good port/adapter separation; `target_resolution.py` directly invokes rg adapter |
| 7 | DRY | 1 | small | medium | `plan_feasible_slices` duplicated between `contracts.py:65` and `bundle_builder.py:48` |
| 8 | Design by contract | 3 | - | low | Strong typed contracts throughout (CqStruct, frozen dataclasses) |
| 9 | Parse, don't validate | 3 | - | low | `TargetSpecV1` parsed at boundary; `ResolvedTarget` carries structured result |
| 10 | Illegal states | 2 | medium | low | `ResolvedTarget.resolution_kind` is a plain `str`, not an enum |
| 11 | CQS | 3 | - | low | Functions are cleanly query-only or command-only throughout |
| 12 | DI + explicit composition | 3 | - | low | `SymbolIndex` protocol enables DI; `Toolchain` injected into `perf/` |
| 13 | Composition over inheritance | 3 | - | low | No inheritance hierarchies; all composition-based |
| 14 | Law of Demeter | 2 | small | low | `snb_renderer.py:91` reaches into `view.key_findings` and `view.sections` deeply |
| 15 | Tell, don't ask | 2 | small | low | `_populate_summary` probes bundle internals field-by-field |
| 16 | Functional core, imperative shell | 3 | - | low | IO at edges; transforms are pure |
| 17 | Idempotency | 3 | - | low | All functions are side-effect-free or idempotent (hash-based artifact IDs) |
| 18 | Determinism | 3 | - | low | `SECTION_ORDER` ensures deterministic layout; hash-based bundle IDs |
| 19 | KISS | 3 | - | low | Implementations are straightforward without over-engineering |
| 20 | YAGNI | 2 | small | low | `file_hash.py` is never imported outside its own package |
| 21 | Least astonishment | 2 | small | low | `plan_feasible_slices` exists in two modules with subtly different signatures |
| 22 | Public contracts | 2 | small | low | `__all__` exports are declared; `resolution_kind` not formalized |
| 23 | Design for testability | 3 | - | low | Pure functions, injectable dependencies, minimal global state |
| 24 | Observability | 2 | small | low | `DegradeEventV1` provides structured diagnostics; no logging in most modules |

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding -- Alignment: 2/3

**Current state:**
The `_SELF_CLS` constant in `tools/cq/index/def_index.py:16` is prefixed with an underscore (marking it as private), yet it is imported by two other modules within the `index/` package.

**Findings:**
- `tools/cq/index/call_resolver.py:10` imports `_SELF_CLS` from `def_index`
- `tools/cq/index/arg_binder.py:11` imports `_SELF_CLS` from `def_index`
- The underscore prefix signals "private to this module" but cross-module use makes it a de facto public API

**Suggested improvement:**
Rename `_SELF_CLS` to `SELF_CLS_NAMES` (or similar public name) and add it to the `__all__` export list of `def_index.py`. This makes the cross-module dependency explicit and prevents confusion about its stability.

**Effort:** small
**Risk if unaddressed:** medium -- future refactors of `def_index.py` may inadvertently remove this "private" constant without checking downstream consumers.

---

#### P2. Separation of concerns -- Alignment: 2/3

**Current state:**
The `introspection/` package bundles three conceptually distinct analysis domains into a single package: bytecode instruction indexing (`bytecode_index.py`), symbol table extraction (`symtable_extract.py`), and control flow graph construction (`cfg_builder.py`). While each lives in a separate file, the `__init__.py` re-exports all 16 symbols from all three modules, presenting them as a single undifferentiated surface.

**Findings:**
- `tools/cq/introspection/__init__.py:1-50` re-exports all symbols from all three submodules
- The only external consumer is `tools/cq/query/enrichment.py:16` which imports only `ScopeGraph` and `extract_scope_graph` from `symtable_extract`
- `cfg_builder.py` depends on `bytecode_index.py` (legitimate), but `symtable_extract.py` is entirely independent

**Suggested improvement:**
No code change needed immediately, but the `__init__.py` re-exports could be trimmed to only the externally-consumed symbols. The three submodules are already well-separated internally; the concern is the flat public surface that implies all three are a single cohesive unit.

**Effort:** medium
**Risk if unaddressed:** low -- the current structure works but makes the actual dependency surface unclear to consumers.

---

#### P3. SRP (one reason to change) -- Alignment: 2/3

**Current state:**
`tools/cq/index/files.py` at 309 lines handles multiple distinct responsibilities: repo file index building, file tabulation with scope/glob filtering, untracked file scanning, gitignore-based ignore decisions, path normalization, and filter decision recording.

**Findings:**
- `tools/cq/index/files.py:56-71` (`build_repo_file_index`) -- index construction concern
- `tools/cq/index/files.py:74-147` (`tabulate_files`) -- tabulation/filtering concern
- `tools/cq/index/files.py:150-173` (`_scan_untracked_scope_files`) -- untracked file scanning concern
- `tools/cq/index/files.py:212-256` (`_collect_untracked_tree`) -- filesystem traversal concern
- `tools/cq/index/files.py:259-273` (`_record_ignore_decision`) -- diagnostics concern
- `tools/cq/index/files.py:286-298` (`_scope_prefixes_for_repo`) -- path normalization concern

**Suggested improvement:**
Consider extracting the untracked file scanning (`_scan_untracked_scope_files`, `_collect_untracked_*`) into a separate `untracked_scan.py` module. The scope/path normalization helpers (`_scope_prefixes_for_repo`, `_normalize_relative_path`, `_is_within_scope`) could consolidate with `core/pathing.py`.

**Effort:** medium
**Risk if unaddressed:** low -- the file is manageable at its current size, but future growth would exacerbate the mixed responsibilities.

---

#### P4. High cohesion, low coupling -- Alignment: 2/3

**Current state:**
`tools/cq/utils/interval_index.py` imports `SgRecord` and `group_records_by_file` directly from `tools/cq/astgrep/sgpy_scanner.py` (line 7). This creates an upward dependency from a generic utility module to a domain-specific scanner module.

**Findings:**
- `tools/cq/utils/interval_index.py:7` -- `from tools.cq.astgrep.sgpy_scanner import SgRecord, group_records_by_file`
- `IntervalIndex` is a generic `[T]` parameterized data structure, but `from_records` (line 72) and `FileIntervalIndex` (line 158) are hardcoded to `SgRecord`
- `FileIntervalIndex.from_records` (line 164) calls `group_records_by_file` directly, tightly coupling generic interval logic to ast-grep's record type

**Suggested improvement:**
Split `IntervalIndex[T]` (generic, no domain imports) from `FileIntervalIndex` (SgRecord-specific). Move `FileIntervalIndex` into `tools/cq/query/scan.py` or `tools/cq/astgrep/` where `SgRecord` naturally lives. The generic `IntervalIndex[T]` should remain in `utils/` with no domain imports.

**Effort:** small
**Risk if unaddressed:** medium -- any refactoring of `SgRecord` fields (e.g., `start_line`, `end_line`, `file`) would cascade into the generic utility module.

---

#### P5. Dependency direction -- Alignment: 2/3

**Current state:**
The dependency direction is generally good (supporting services depend on `core/`, not the reverse), but two cases violate the "utilities should be leaf nodes" principle.

**Findings:**
- `tools/cq/utils/interval_index.py:7` depends upward on `tools/cq/astgrep/sgpy_scanner.py` (a domain module)
- `tools/cq/neighborhood/target_resolution.py:13` imports `find_symbol_candidates` from `tools/cq/search/rg/adapter.py`, coupling target resolution to the ripgrep adapter implementation rather than an abstraction

**Suggested improvement:**
For `interval_index.py`, see P4 above. For `target_resolution.py`, the coupling to `find_symbol_candidates` is acceptable as a pragmatic choice (it is the only caller), but if target resolution were to support alternative backends, a protocol or callback parameter would be cleaner.

**Effort:** small
**Risk if unaddressed:** medium -- the `interval_index.py` case creates a surprising dependency that makes the utils package harder to use independently.

---

#### P6. Ports & Adapters -- Alignment: 2/3

**Current state:**
`tools/cq/index/gitignore.py` demonstrates good port/adapter thinking: it abstracts gitignore semantics behind `GitIgnoreSpec` and isolates `pygit2` interactions. The `RepoContext` dataclass in `repo.py` serves as a clean port boundary. However, there is no protocol/port for the symbol resolution in `target_resolution.py`.

**Findings:**
- `tools/cq/index/gitignore.py:22-38` -- `load_gitignore_spec` is a clean adapter composing `pygit2` and `pathspec`
- `tools/cq/index/repo.py:33-73` -- `resolve_repo_context` properly isolates `pygit2.discover_repository` behind `RepoContext`
- `tools/cq/neighborhood/target_resolution.py:125` -- directly invokes ripgrep adapter with no abstraction layer

**Suggested improvement:**
For `target_resolution.py`, consider accepting a callable `symbol_resolver: Callable[[Path, str, str], list[tuple[str, int, str]]] | None` parameter instead of directly importing `find_symbol_candidates`. This would improve testability and allow alternative symbol resolution strategies.

**Effort:** small
**Risk if unaddressed:** low -- the current approach works and is well-tested; the coupling is narrow.

---

### Category: Knowledge (7-11)

#### P7. DRY (knowledge, not lines) -- Alignment: 1/3

**Current state:**
`plan_feasible_slices` is implemented twice with subtly different signatures and behavior, encoding the same business rule (capability-gated slice planning) in two locations.

**Findings:**
- `tools/cq/neighborhood/contracts.py:65-92` -- accepts `Mapping[str, object] | None`, normalizes via `normalize_capability_snapshot`, returns individual unavailability messages
- `tools/cq/neighborhood/bundle_builder.py:48-89` -- accepts `Mapping[str, object] | object | None`, has different empty-capabilities behavior (returns all unavailable at once), subtly different `bool()` vs `.get()` check
- The `contracts.py` version uses `snapshot.get(kind, False)` while `bundle_builder.py` uses `bool(capabilities.get(kind))`
- Both produce identical `DegradeEventV1` messages but differ in edge-case behavior when `capabilities` is empty vs absent

**Suggested improvement:**
Delete `plan_feasible_slices` from `bundle_builder.py` and use the version in `contracts.py` (which is already exported in `__all__`). If the empty-mapping edge case matters, update the canonical version to handle it.

**Effort:** small
**Risk if unaddressed:** medium -- the two implementations may drift further apart, causing inconsistent capability gating behavior depending on which code path is invoked.

---

#### P8. Design by contract -- Alignment: 3/3

**Current state:**
Contracts are strong throughout these modules. Typed request/response pairs (`TreeSitterNeighborhoodCollectRequest`/`Result`, `BundleBuildRequest`, `RenderSnbRequest`) enforce preconditions via `frozen=True` and typed fields. The `SymbolIndex` protocol in `index/protocol.py` provides explicit interface contracts.

**Findings:**
- `tools/cq/neighborhood/contracts.py:24-46` -- frozen CqStruct request/response types with typed fields
- `tools/cq/index/protocol.py:13-148` -- comprehensive Protocol definition with docstrings
- `tools/cq/astgrep/sgpy_scanner.py:47-57` -- `RuleSpec.__post_init__` validates config type at construction

No action needed.

---

#### P9. Parse, don't validate -- Alignment: 3/3

**Current state:**
The boundary between raw user input and structured data is clean. `TargetSpecV1` (from `core/target_specs.py`) parses raw target strings at the boundary, and `resolve_target` in `target_resolution.py` converts them to typed `ResolvedTarget` structs. The `DefIndexVisitor` pattern converts raw AST trees into structured `FnDecl`/`ClassDecl` models.

**Findings:**
- `tools/cq/neighborhood/target_resolution.py:29-114` -- `resolve_target` converts a parsed `TargetSpecV1` to a `ResolvedTarget`, collapsing all validation into the transformation
- `tools/cq/index/def_index.py:286-402` -- `DefIndexVisitor` converts AST to structured `ModuleInfo`
- `tools/cq/astgrep/rulepack_loader.py:43-76` -- `load_cli_rule_file` parses YAML into typed `CliRuleFile` then converts to `RuleSpec`

No action needed.

---

#### P10. Make illegal states unrepresentable -- Alignment: 2/3

**Current state:**
Most data models use typed fields effectively, but several string-typed fields could be narrowed to enums or Literal types to prevent invalid values.

**Findings:**
- `tools/cq/neighborhood/target_resolution.py:25` -- `resolution_kind: str = "unresolved"` accepts any string; valid values are `"anchor"`, `"file_symbol"`, `"symbol_fallback"`, `"unresolved"` (could be a `Literal` or enum)
- `tools/cq/neighborhood/contracts.py:69` -- `stage: str = "semantic.planning"` is a free-form string
- `tools/cq/introspection/symtable_extract.py:17-26` -- `ScopeType` enum is well-defined (positive example)
- `tools/cq/index/call_resolver.py:14` -- `CallConfidence = Literal["exact", "likely", "ambiguous", "unresolved"]` is well-constrained (positive example)

**Suggested improvement:**
Define `ResolutionKind = Literal["anchor", "file_symbol", "symbol_fallback", "unresolved"]` and use it for `ResolvedTarget.resolution_kind`. Similarly, `DegradeEventV1.severity` and `DegradeEventV1.category` would benefit from Literal type constraints (though these are defined in `core/snb_schema.py`, outside the review scope).

**Effort:** medium
**Risk if unaddressed:** low -- the current strings are consistently used in practice but could accept invalid values without warning.

---

#### P11. CQS -- Alignment: 3/3

**Current state:**
Functions cleanly separate queries from commands. Collection functions return results without side effects. The only stateful operation is `_store_artifacts_with_preview` in `bundle_builder.py`, which writes to disk and returns pointers -- acceptable as a command at the IO boundary.

**Findings:**
- `tools/cq/neighborhood/tree_sitter_collector.py:596` -- `collect_tree_sitter_neighborhood` reads files and returns a result (query)
- `tools/cq/neighborhood/bundle_builder.py:233-278` -- `_store_artifacts_with_preview` writes files (command), clearly separated from assembly logic

No action needed.

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition -- Alignment: 3/3

**Current state:**
The `SymbolIndex` protocol in `index/protocol.py` provides clean dependency inversion for symbol lookup operations. The `Toolchain` object is injected into `perf/smoke_report.py`. Dependencies are explicitly composed rather than hidden.

**Findings:**
- `tools/cq/index/protocol.py:13` -- `SymbolIndex(Protocol)` enables DI for testing
- `tools/cq/perf/smoke_report.py:63` -- `Toolchain.detect()` is the only implicit creation, and it is at the top-level entry point

No action needed.

---

#### P13. Prefer composition over inheritance -- Alignment: 3/3

**Current state:**
No inheritance hierarchies exist in any of the reviewed modules. All behavior is built through composition: `_AnchorNeighborhood` composes node lists, `BundleBuildRequest` composes parameters, `CFG` composes blocks and edges.

No action needed.

---

#### P14. Law of Demeter -- Alignment: 2/3

**Current state:**
The `snb_renderer.py` module reaches several levels deep into the `BundleViewV1` structure to extract findings and sections.

**Findings:**
- `tools/cq/neighborhood/snb_renderer.py:91-112` -- `_populate_findings` reaches into `view.key_findings[i].category`, `view.key_findings[i].label`, `view.key_findings[i].value`, and `view.sections[i].items`
- `tools/cq/neighborhood/snb_renderer.py:60-78` -- `_populate_summary` accesses `bundle.subject.file_path`, `bundle.subject.name`, `bundle.graph.node_count`, `bundle.graph.edge_count`

**Suggested improvement:**
Consider adding a `to_cq_findings()` method on `BundleViewV1` or a standalone `convert_view_to_cq_findings(view: BundleViewV1) -> list[Finding]` function in `section_layout.py`. This would let the renderer delegate the structural traversal to the view object itself.

**Effort:** small
**Risk if unaddressed:** low -- the current approach works but couples the renderer to the internal structure of `BundleViewV1`.

---

#### P15. Tell, don't ask -- Alignment: 2/3

**Current state:**
The `_populate_summary` function in `snb_renderer.py` extracts individual fields from the bundle to populate the summary dict, rather than delegating summary construction to the bundle itself.

**Findings:**
- `tools/cq/neighborhood/snb_renderer.py:55-78` -- `_populate_summary` reads 10+ individual fields from `request` and `bundle` to build `result.summary` entries
- `tools/cq/neighborhood/section_layout.py:262-310` -- `_build_summary_section` similarly interrogates `bundle.graph.node_count`, `bundle.graph.edge_count`, etc.

**Suggested improvement:**
Add a `summary_dict() -> dict[str, object]` method to `SemanticNeighborhoodBundleV1` (or a standalone function in the bundle module) that encapsulates the summary extraction logic. Callers would then merge this dict rather than probing individual fields.

**Effort:** small
**Risk if unaddressed:** low -- the current pattern works but would require updates in multiple places if the bundle schema evolves.

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell -- Alignment: 3/3

**Current state:**
The modules demonstrate clean separation between pure transforms and IO operations. Tree-sitter parsing (IO) happens at the boundary in `_parse_tree_for_request`; all neighborhood collection and slice building is pure. File hashing in `file_hash.py` is purely functional per-call. The `perf/smoke_report.py` is correctly placed as an imperative shell entry point.

No action needed.

---

#### P17. Idempotency -- Alignment: 3/3

**Current state:**
All operations produce the same output for the same input. Artifact storage uses deterministic content-based hashing (`_store_artifacts_with_preview` in `bundle_builder.py:261` uses `sha256` of JSON content), ensuring repeated runs produce identical artifacts. Bundle IDs are derived from request parameters, not timestamps.

**Findings:**
- `tools/cq/neighborhood/bundle_builder.py:199-202` -- `_generate_bundle_id` uses `sha256` of `language:file:name:line:col`
- `tools/cq/neighborhood/bundle_builder.py:261` -- artifact paths include content hash

No action needed.

---

#### P18. Determinism / reproducibility -- Alignment: 3/3

**Current state:**
The `SECTION_ORDER` tuple in `section_layout.py:18-36` enforces deterministic section ordering. Slice merging in `bundle_builder.py:196` sorts by key. The `_build_meta` function records elapsed time (inherently non-deterministic) but this is appropriately a metadata field, not a logic input.

No action needed.

---

### Category: Simplicity (19-22)

#### P19. KISS -- Alignment: 3/3

**Current state:**
Implementations are straightforward. The interval tree in `utils/interval_index.py` is a clean generic data structure. The CFG builder in `introspection/cfg_builder.py` decomposes naturally into block identification, edge construction, and exception edge addition. No unnecessary abstractions or over-engineering observed.

No action needed.

---

#### P20. YAGNI -- Alignment: 2/3

**Current state:**
`tools/cq/index/file_hash.py` provides four functions (`compute_file_hash`, `compute_file_mtime`, `hash_files_batch`, `files_changed_since`) that are never imported outside the `index/` package itself. Additionally, the `files_changed_since` function appears to be unused entirely.

**Findings:**
- `tools/cq/index/file_hash.py` -- zero imports from any other CQ module (confirmed by grep)
- `tools/cq/index/__init__.py` does not re-export any symbols from `file_hash.py`
- The module appears to be infrastructure prepared for cache invalidation that was removed

**Suggested improvement:**
If `file_hash.py` is genuinely unused, consider removing it or marking it as deprecated. If it supports test infrastructure or future plans, add a comment explaining its purpose.

**Effort:** small
**Risk if unaddressed:** low -- dead code adds to maintenance burden and confuses readers about the module's contract surface.

---

#### P21. Least astonishment -- Alignment: 2/3

**Current state:**
The existence of `plan_feasible_slices` in both `contracts.py` and `bundle_builder.py` with subtly different signatures is the primary surprise.

**Findings:**
- `tools/cq/neighborhood/contracts.py:65` -- `plan_feasible_slices(requested_slices, capabilities, *, stage=...)` where `capabilities: Mapping[str, object] | None`
- `tools/cq/neighborhood/bundle_builder.py:48` -- `plan_feasible_slices(requested_slices, capabilities, *, stage=...)` where `capabilities: Mapping[str, object] | object | None`
- The `bundle_builder.py` version accepts `object` in the union, meaning it handles non-Mapping objects differently (returns all-unavailable), while the `contracts.py` version returns an empty feasible set for non-Mapping inputs
- A consumer importing from either location would get different edge-case behavior

**Suggested improvement:**
Remove the duplicate in `bundle_builder.py` and standardize on the `contracts.py` implementation. See P7.

**Effort:** small
**Risk if unaddressed:** medium -- developers may import the "wrong" one and get unexpected behavior.

---

#### P22. Declare and version public contracts -- Alignment: 2/3

**Current state:**
Most modules declare `__all__` exports, which is good. However, the `ResolvedTarget.resolution_kind` field uses free-form strings with no declared vocabulary, and some contract types lack explicit versioning.

**Findings:**
- `tools/cq/neighborhood/target_resolution.py:183-186` -- `__all__` is declared
- `tools/cq/neighborhood/contracts.py:95-99` -- `__all__` is declared
- `tools/cq/neighborhood/target_resolution.py:25` -- `resolution_kind: str` has no declared valid values
- `tools/cq/introspection/cfg_builder.py:15` -- `CfgEdgeType` is a well-declared `Literal` type (positive example)

**Suggested improvement:**
Add a `ResolutionKind` type alias to `target_resolution.py` and reference it in `ResolvedTarget`. This documents the valid vocabulary without changing runtime behavior.

**Effort:** small
**Risk if unaddressed:** low -- the current strings are used consistently but the contract is implicit.

---

### Category: Quality (23-24)

#### P23. Design for testability -- Alignment: 3/3

**Current state:**
The modules are highly testable by design. Pure functions dominate. The `SymbolIndex` protocol enables DI for `DefIndex` consumers. `Toolchain` is injectable. Request/response structs are `frozen=True`, preventing accidental mutation in tests.

**Findings:**
- `tools/cq/index/protocol.py:13-148` -- `SymbolIndex` protocol enables fake index implementations for testing
- `tools/cq/neighborhood/tree_sitter_collector.py:596-663` -- `collect_tree_sitter_neighborhood` takes a request struct and returns a result struct, trivially testable
- `tools/cq/introspection/bytecode_index.py:126-141` -- `BytecodeIndex.from_code` class method provides clean test entry point

No action needed.

---

#### P24. Observability -- Alignment: 2/3

**Current state:**
The neighborhood module uses `DegradeEventV1` consistently for structured diagnostics, which is excellent. However, the `index/`, `introspection/`, and `astgrep/` modules have minimal observability -- errors are silently swallowed or returned as empty results without any structured diagnostic signal.

**Findings:**
- `tools/cq/neighborhood/tree_sitter_collector.py:511-563` -- Multiple `_*_result` functions produce structured `DegradeEventV1` diagnostics for every failure mode (file missing, parse error, anchor unresolved)
- `tools/cq/index/def_index.py:472-473` -- `SyntaxError, OSError, UnicodeDecodeError` are caught and silently `continue`d with no diagnostic output
- `tools/cq/astgrep/sgpy_scanner.py:211-214` -- `OSError` on file read is silently swallowed
- `tools/cq/introspection/symtable_extract.py:14` -- Uses `logger` but only at `debug`/`warning` level for parse failures
- `tools/cq/index/gitignore.py:104` -- `OSError` returns empty list with no diagnostic

**Suggested improvement:**
Consider adding a diagnostic callback or return channel (similar to `DegradeEventV1`) to the `DefIndex.build` method and `scan_files` function. This would let callers understand which files were skipped and why, without breaking the current API.

**Effort:** small
**Risk if unaddressed:** low -- silent failures are acceptable for a code analysis tool (graceful degradation), but diagnostic reporting would aid debugging scan issues.

---

## Cross-Cutting Themes

### Theme 1: Duplicated `plan_feasible_slices` (Root cause: organic growth)

The `plan_feasible_slices` function was likely initially defined in `contracts.py` and then independently reimplemented in `bundle_builder.py` with slightly different semantics. This is the most significant DRY violation in the scope: the same business rule (capability-gated slice feasibility) is encoded in two places with subtly different behavior around empty capability maps.

**Affected principles:** P7 (DRY), P21 (Least astonishment)
**Suggested approach:** Consolidate to the `contracts.py` version. Update `bundle_builder.py` to import from `contracts.py`.

### Theme 2: Generic utility coupled to domain types

The `IntervalIndex[T]` class in `utils/interval_index.py` is a genuinely generic data structure, but its `from_records` class method and `FileIntervalIndex` subclass hardcode a dependency on `SgRecord` from `astgrep/sgpy_scanner.py`. This violates the principle that utility modules should be dependency-free leaves.

**Affected principles:** P4 (Cohesion/coupling), P5 (Dependency direction)
**Suggested approach:** Move `FileIntervalIndex` and the `SgRecord`-specific `from_records` class method to a module that already depends on `astgrep/` (such as `query/scan.py`). Keep the generic `IntervalIndex[T]` and `from_intervals` in `utils/`.

### Theme 3: Silent error swallowing pattern

Multiple modules across `index/`, `astgrep/`, and `introspection/` catch exceptions and return empty results without any structured diagnostic signal. While this follows the codebase's "graceful degradation" philosophy, it makes it difficult to diagnose why certain files or symbols are missing from analysis results.

**Affected principles:** P24 (Observability)
**Suggested approach:** Introduce a lightweight diagnostic accumulator (e.g., a `list[DegradeEventV1]` parameter or a returned diagnostics tuple) to `DefIndex.build` and `scan_files`. The `DegradeEventV1` pattern already exists in `neighborhood/` and could be reused.

### Theme 4: `introspection/` lacks internal cohesion boundaries

The `introspection/__init__.py` re-exports 16 symbols from three unrelated submodules (bytecode, symtable, CFG). Only `symtable_extract` has external consumers. The flat surface suggests these are a single cohesive unit when they are three independent analysis capabilities.

**Affected principles:** P2 (Separation of concerns), P4 (Cohesion)
**Suggested approach:** Trim `__init__.py` re-exports to only the externally consumed symbols (`ScopeGraph`, `extract_scope_graph`, `ScopeFact`, `SymbolFact`, `ScopeType`). Consumers needing bytecode or CFG functionality should import directly from the submodule.

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Remove duplicate `plan_feasible_slices` from `bundle_builder.py:48-89`; use the canonical version from `contracts.py:65-92` | small | Eliminates semantic drift between two capability-gating implementations |
| 2 | P1 (Information hiding) | Rename `_SELF_CLS` to `SELF_CLS_NAMES` in `def_index.py:16` and add to `__all__` | small | Makes cross-module usage explicit and safe |
| 3 | P4/P5 (Cohesion/Direction) | Move `FileIntervalIndex` and `IntervalIndex.from_records` out of `utils/interval_index.py` into `query/scan.py` | small | Removes upward dependency from utility module to domain module |
| 4 | P10 (Illegal states) | Define `ResolutionKind = Literal[...]` for `ResolvedTarget.resolution_kind` in `target_resolution.py:25` | small | Prevents invalid resolution kind strings at type-check time |
| 5 | P20 (YAGNI) | Remove or deprecate `tools/cq/index/file_hash.py` which has zero external imports | small | Reduces dead code and clarifies the package's public surface |

## Recommended Action Sequence

1. **Eliminate `plan_feasible_slices` duplication (P7, P21).** Delete the function body from `tools/cq/neighborhood/bundle_builder.py:48-89` and replace with an import from `tools/cq/neighborhood/contracts.py`. Verify edge-case behavior for empty vs None capabilities. This is the highest-priority fix because it prevents semantic drift in a capability-gating decision path.

2. **Promote `_SELF_CLS` to public (P1).** Rename to `SELF_CLS_NAMES` in `tools/cq/index/def_index.py:16`, add to `__all__`, and update imports in `call_resolver.py:10` and `arg_binder.py:11`. Simple mechanical change with no behavioral impact.

3. **Decouple `IntervalIndex` from `SgRecord` (P4, P5).** Move `FileIntervalIndex` and the `SgRecord`-typed `from_records` classmethod to `tools/cq/query/scan.py` (which already imports `SgRecord`). Keep the generic `IntervalIndex[T]` with `from_intervals` in `utils/interval_index.py`.

4. **Constrain `resolution_kind` to Literal type (P10, P22).** Define `ResolutionKind = Literal["anchor", "file_symbol", "symbol_fallback", "unresolved"]` in `target_resolution.py` and use it for the `ResolvedTarget.resolution_kind` field.

5. **Audit `file_hash.py` usage (P20).** Confirm it has no test consumers. If confirmed unused, remove. If it supports future cache invalidation, add a docstring explaining its purpose and intended consumers.

6. **Trim `introspection/__init__.py` re-exports (P2).** Reduce to only externally consumed symbols. This clarifies the package's actual dependency surface without breaking any existing consumers.

7. **Add diagnostic channel to `DefIndex.build` (P24).** Return a `tuple[DefIndex, list[DegradeEventV1]]` or accept an optional diagnostics accumulator to surface which files were skipped during indexing and why.
