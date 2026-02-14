# CQ LSP Deprecation + Static Analysis Cutover Implementation Plan v2 (2026-02-14)

**Status:** Proposed (Design Phase, Hard Cut)  
**Date:** 2026-02-14  
**Scope:** `tools/cq` runtime, contracts, enrichment, neighborhood, rendering, tests, docs

---

## 1) Executive Summary

This v2 plan replaces all `pyrefly`/`rust-analyzer` LSP functionality in `tools/cq` with a contract-first static semantic architecture using:

1. `ast-grep-py` / `ast-grep`
2. Python `ast` + `symtable`
3. `LibCST` metadata providers
4. `tree-sitter` (Python + Rust) query packs and parser diagnostics

The plan is aggressive by design: no compatibility shims, no dual-emission, no LSP feature flags retained.

---

## 2) Hard-Cut Design Policy

1. No runtime LSP processes, sessions, or protocol clients remain.
2. No LSP-named summary keys remain (`pyrefly_*`, `rust_lsp_*`, `lsp_advanced_planes`).
3. No LSP knobs remain (`CQ_ENABLE_LSP`, `--no-lsp`, `no_lsp`).
4. No compatibility wrappers for old LSP contracts.
5. Fail-open command semantics are preserved for static enrichment.

---

## 3) Inputs and Basis

### 3.1 Plans reviewed

1. `docs/plans/deprecate_lsp_enrichment.md`
2. `docs/plans/deprecate_lsp_enrichment_best_in_class_target_state_v1_2026-02-14.md`

### 3.2 Library references

1. `docs/python_library_reference/ast-grep-py.md`
2. `docs/python_library_reference/ast-grep-py_rust_deepdive.md`
3. `docs/python_library_reference/python_ast_libraries_and_cpg_construction.md`
4. `docs/python_library_reference/tree-sitter.md`
5. `docs/python_library_reference/tree-sitter_advanced.md`
6. `docs/python_library_reference/tree-sitter_outputs_overview.md`
7. `docs/python_library_reference/tree-sitter-rust.md`
8. `docs/python_library_reference/libcst.md`
9. `docs/python_library_reference/libcst-advanced.md`

### 3.3 Current CQ code surfaces

1. LSP providers/adapters in `tools/cq/search/*lsp*` and `tools/cq/search/lsp/*`
2. LSP consumers in `tools/cq/search/smart_search.py`, `tools/cq/macros/calls.py`, `tools/cq/query/entity_front_door.py`, `tools/cq/query/merge.py`, `tools/cq/neighborhood/bundle_builder.py`
3. Existing static substrate in:
   - `tools/cq/search/python_analysis_session.py`
   - `tools/cq/search/python_enrichment.py`
   - `tools/cq/search/libcst_python.py`
   - `tools/cq/search/tree_sitter_python.py`
   - `tools/cq/search/rust_enrichment.py`
   - `tools/cq/search/tree_sitter_rust.py`
   - `tools/cq/neighborhood/structural_collector.py`

---

## 4) Target-State Architecture

## 4.1 Static semantic front door (contract-first)

Replace language-LSP front door with language-static front door:

1. `LanguageSemanticEnrichmentRequest`
2. `LanguageSemanticEnrichmentOutcome`
3. `SemanticContractStateV1` (replacing `LspContractStateV1`)

New modules:

1. `tools/cq/search/language_front_door_contracts.py`
2. `tools/cq/search/language_front_door_pipeline.py`
3. `tools/cq/search/language_front_door_adapter.py`
4. `tools/cq/search/semantic_contract_state.py`

## 4.2 Semantic planes v2 (explicit schema)

Replace `lsp_advanced_planes` with `semantic_planes`:

1. `semantic_tokens`
   - source: tree-sitter highlights captures
2. `locals`
   - source: tree-sitter locals captures + scope association
3. `diagnostics`
   - source: tree-sitter `(ERROR)`/`(MISSING)` + Python AST parse errors
4. `injections`
   - source: tree-sitter injections planning/execution metadata

Required top-level envelope:

1. `version`
2. `language`
3. `counts`
4. `preview`
5. `degradation`
6. `sources`

## 4.3 Canonical output naming (hard rename)

1. `python_semantic_overview`
2. `python_semantic_telemetry`
3. `rust_semantic_telemetry`
4. `semantic_planes`
5. `python_semantic_diagnostics`

## 4.4 Agent-ready structural artifact contract

Adopt deterministic artifacts aligned with tree-sitter outputs guidance:

1. `cst_nodes`
2. `cst_edges`
3. `cst_tokens` (optional)
4. `cst_diagnostics`
5. `cst_query_hits`

All anchored by byte spans and joinable IDs.

---

## 5) Capability Boundaries and Parity Targets

## 5.1 LSP-to-static parity matrix

| Previous insight | Static replacement | Parity tier |
|---|---|---|
| Python definition/declaration grounding | LibCST assignments + tree-sitter defs + ast-grep anchors | high |
| Python references/same-scope symbols | LibCST accesses/referents + symtable scope graph | high |
| Python import alias resolution | LibCST import alias chain + qualified names | high |
| Python callable/type contract (annotated) | AST annotations + signature extraction + optional LibCST FQN/type metadata | medium-high |
| Python callable/type contract (unannotated inferred) | Not fully replaceable without external type engine | low |
| Python call graph preview | structural collector + call captures + scope-aware normalization | medium-high |
| Rust symbol grounding | rust defs/refs packs + structural collector | medium-high |
| Rust call graph preview | rust calls packs + structural neighborhood edges | medium-high |
| Rust type hierarchy | trait/impl relation extraction (partial) | medium |
| Rust diagnostics | tree-sitter error/missing diagnostics | medium-high |
| Rust macro expansion/runnables | macro invocation/token-tree/injection facts only | low (intentional) |
| LSP semantic overlays | highlights/locals/diagnostics/injections planes | medium |

## 5.2 Non-negotiable boundaries

1. No false claims of Rust macro expansion semantics from parser-only stack.
2. No false claims of deep Python inference for unannotated code.
3. Every degraded output must include explicit static degradation reason(s).

---

## 6) Static Degradation Taxonomy (required)

Canonical reasons used across search/calls/entity/neighborhood:

1. `source_unavailable`
2. `parse_error`
3. `query_pack_compile_error`
4. `query_pack_no_signal`
5. `scope_resolution_partial`
6. `annotation_absent`
7. `macro_semantics_unavailable`
8. `source_too_large`
9. `budget_exceeded`
10. `crosscheck_mismatch`

---

## 7) Implementation Workstreams

## Workstream 0: Contract Freeze and Parity Harness

Objectives:

1. Freeze baseline LSP-era artifacts and summaries.
2. Define `SemanticContractV1` fields and parity tiers.
3. Add field-level parity diff harness for `high` and `medium-high` tiers.

Deliverables:

1. Baseline fixtures for `search`, `calls`, `entity`, `neighborhood`, `run`.
2. Parity test matrix with explicit expected deltas for low-tier fields.

## Workstream 1: Static Front Door Introduction

Objectives:

1. Land static contracts and pipeline.
2. Migrate consumers to static contracts before deleting providers.

Primary edits:

1. `tools/cq/search/smart_search.py`
2. `tools/cq/macros/calls.py`
3. `tools/cq/query/entity_front_door.py`
4. `tools/cq/query/merge.py`
5. `tools/cq/core/front_door_insight.py`

Notes:

1. Keep cache/single-flight behavior.
2. Rename cache namespace from `lsp_front_door` to `semantic_front_door`.

## Workstream 2: Python Deep Semantic Provider

Objectives:

1. Make Python static provider parity-quality for high-tier fields.
2. Introduce two operating levels.

Levels:

1. Core mode: `ast-grep` + AST + symtable + LibCST + tree-sitter (single file).
2. Repo mode: optional `FullRepoManager`/FQN mode for stronger cross-file identity.

Primary edits:

1. `tools/cq/search/python_enrichment.py`
2. `tools/cq/search/python_analysis_session.py`
3. `tools/cq/search/libcst_python.py`
4. `tools/cq/search/tree_sitter_python.py`

Required payload sections:

1. `symbol_grounding_static`
2. `scope_and_bindings`
3. `import_aliases`
4. `call_shape`
5. `parser_diagnostics`
6. `semantic_planes`
7. `source_attribution`

## Workstream 3: Rust Deep Semantic Provider + Pack Infrastructure

Objectives:

1. Build Rust replacement on query packs + structural extraction.
2. Make pack governance mandatory.

Primary edits:

1. `tools/cq/search/rust_enrichment.py`
2. `tools/cq/search/tree_sitter_rust.py`
3. `tools/cq/search/queries/rust/*.scm` (new)

Required Rust projections:

1. defs
2. refs
3. calls
4. types
5. attrs/docs
6. errors
7. macro invocation facts

Macro policy:

1. Invocation-site indexing is required.
2. Expansion semantics are out-of-scope in this cut.
3. Injections are allowed for known macro-input languages as auxiliary evidence.

## Workstream 4: Semantic Planes v2 and Diagnostics Contracts

Objectives:

1. Remove LSP advanced-plane pipeline.
2. Standardize static semantic plane output and diagnostics payloads.

Primary edits:

1. `tools/cq/search/semantic_planes_static.py` (new)
2. `tools/cq/core/diagnostics_contracts.py`
3. `tools/cq/core/report.py`
4. `tools/cq/run/runner.py`

## Workstream 5: Neighborhood Static Cutover

Objectives:

1. Remove LSP slice planning/collection.
2. Keep deterministic slice quality with static evidence.

Primary edits:

1. `tools/cq/neighborhood/bundle_builder.py`
2. `tools/cq/neighborhood/snb_renderer.py`
3. `tools/cq/neighborhood/capability_gates.py`
4. `tools/cq/neighborhood/section_layout.py`
5. `tools/cq/core/snb_schema.py`

New adapters:

1. `tools/cq/neighborhood/python_static_adapter.py`
2. `tools/cq/neighborhood/rust_static_adapter.py`

## Workstream 6: Runtime + Scheduler Refactor (not blind deletion)

Objectives:

1. Remove LSP runtime policy concepts.
2. Preserve useful bounded execution patterns for semantic work.

Primary edits:

1. `tools/cq/core/runtime/execution_policy.py`
2. `tools/cq/core/runtime/worker_scheduler.py`
3. `tools/cq/core/runtime/__init__.py`

Required changes:

1. Remove `LspRuntimePolicy` and `lsp_request_workers`.
2. Replace `submit_lsp` with `submit_semantic` or generic bounded execution lane.
3. Replace `CQ_RUNTIME_LSP_*` envs with semantic-neutral equivalents when needed.

## Workstream 7: LSP Module Deletion and Rehoming

Delete modules that are truly LSP-only.

Rehome before delete where utility is generic:

1. `tools/cq/search/lsp/root_resolution.py` -> `tools/cq/search/language_root_resolution.py`
2. `tools/cq/search/lsp_request_budget.py` -> `tools/cq/search/semantic_request_budget.py` (if still used)

Delete list after consumer cutover:

1. `tools/cq/search/pyrefly_lsp.py`
2. `tools/cq/search/pyrefly_contracts.py`
3. `tools/cq/search/pyrefly_signal.py`
4. `tools/cq/search/rust_lsp.py`
5. `tools/cq/search/rust_lsp_contracts.py`
6. `tools/cq/search/lsp_front_door_adapter.py`
7. `tools/cq/search/lsp_front_door_contracts.py`
8. `tools/cq/search/lsp_front_door_pipeline.py`
9. `tools/cq/search/lsp_advanced_planes.py`
10. `tools/cq/search/semantic_overlays.py`
11. `tools/cq/search/diagnostics_pull.py`
12. `tools/cq/search/rust_extensions.py`
13. `tools/cq/search/lsp_contract_state.py`
14. `tools/cq/search/lsp/__init__.py`
15. `tools/cq/search/lsp/capabilities.py`
16. `tools/cq/search/lsp/contracts.py`
17. `tools/cq/search/lsp/position_encoding.py`
18. `tools/cq/search/lsp/request_queue.py`
19. `tools/cq/search/lsp/session_manager.py`
20. `tools/cq/search/lsp/status.py`

## Workstream 8: CLI, Run-Step, and Config Cleanup

Primary edits:

1. `tools/cq/cli_app/commands/neighborhood.py`
2. `tools/cq/cli_app/step_types.py`
3. `tools/cq/run/spec.py`
4. `tools/cq/run/runner.py`
5. `tests/e2e/cq/conftest.py`

Required changes:

1. Remove `--no-lsp` and `no_lsp`.
2. Remove `CQ_ENABLE_LSP` handling.
3. Add optional `--no-semantic-enrichment` only if needed for debugging.

## Workstream 9: Contracts, Rendering, and Report Renames

Primary edits:

1. `tools/cq/search/contracts.py`
2. `tools/cq/core/diagnostics_contracts.py`
3. `tools/cq/core/report.py`
4. `tools/cq/core/front_door_insight.py`
5. `tools/cq/core/cache/namespaces.py`

Required changes:

1. Remove all LSP-named structs/keys.
2. Add static semantic counterparts.
3. Update compact rendering and diagnostic derivations.

## Workstream 10: Tests, Goldens, and Docs

Retire LSP-only tests and add static semantic suites.

Add/expand tests:

1. static front-door contracts/pipeline
2. semantic planes schema and degradation
3. query-pack lint and drift checks
4. parity matrix tests
5. neighborhood static slice quality tests

Docs updates:

1. `tools/cq/README.md`
2. CQ skill docs where LSP behavior is currently documented
3. plan references and design docs to static-only language

---

## 8) Query-Pack Governance (Mandatory Gate)

## 8.1 Python and Rust pack checks

1. Query compile checks.
2. Node/field validity checks against grammar metadata.
3. Capture-schema checks against registry (stable `cq.kind` contracts).
4. Windowability checks for incremental scopes where required.

## 8.2 Grammar/runtime drift controls

1. ABI compatibility gate for runtime and language artifacts.
2. `node-types.json` hash/diff gate with classified drift report.
3. Explicit migration required for breaking pack drift.

---

## 9) Sequencing (Contract-First)

1. Wave 0: Baseline freeze + parity harness.
2. Wave 1: Static front-door contracts and consumer migration.
3. Wave 2: Python deep provider (core + repo mode).
4. Wave 3: Rust deep provider + pack infrastructure.
5. Wave 4: Semantic planes + neighborhood cutover.
6. Wave 5: Runtime/CLI/config cleanup.
7. Wave 6: Hard delete LSP modules.
8. Wave 7: Goldens/docs finalization and full gate.

---

## 10) Risks and Mitigations

## R1: Rust macro-heavy repos lose semantic depth

Mitigations:

1. Treat macro invocations as first-class indexed facts.
2. Add known-macro injection parsing as auxiliary evidence.
3. Emit explicit `macro_semantics_unavailable` degradation.

## R2: Python inferred-type regression

Mitigations:

1. Strong annotation/signature extraction path.
2. Scope/reference accuracy via LibCST + symtable.
3. Optional repo-mode FQN/type enrichment track.

## R3: Grammar/query drift causes silent regressions

Mitigations:

1. Mandatory query-pack lint + drift checks.
2. ABI gate + `node-types.json` diff gate.
3. Golden suites keyed by `cq.kind` and byte spans.

## R4: Performance regressions

Mitigations:

1. Reuse session caches.
2. Incremental parse windows with bounded queries.
3. Match-limit and timeout telemetry with enforced thresholds.

---

## 11) Acceptance Criteria

1. No runtime references to pyrefly/rust-analyzer/LSP remain in `tools/cq`.
2. No LSP-named summary/report/artifact keys remain.
3. `high` and `medium-high` parity-tier fields meet baseline expectations in parity harness.
4. `semantic_planes` schema is present, stable, and bounded in search/calls/entity outputs.
5. Neighborhood bundles contain no `lsp_servers` metadata and no LSP slice sources.
6. Query-pack governance gates pass for Python and Rust packs.
7. Full quality gate passes:
   - `uv run ruff format`
   - `uv run ruff check --fix`
   - `uv run pyrefly check`
   - `uv run pyright`
   - `uv run pytest -q`

---

## 12) Deliverables

1. Static semantic front-door contract and pipeline.
2. Static semantic planes v2.
3. Python and Rust deep static enrichment providers.
4. Full deletion of LSP providers/sessions/adapters.
5. Updated neighborhood, runtime policy, scheduler, report, and diagnostics contracts.
6. Query-pack governance and drift tooling.
7. Updated tests/goldens/docs for static-only CQ semantics.
