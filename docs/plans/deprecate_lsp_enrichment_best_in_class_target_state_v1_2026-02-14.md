# Deprecate LSP Enrichment: Best-in-Class Target-State Review and Upgrade Plan (v1, 2026-02-14)

## Objective

Upgrade the design in `docs/plans/deprecate_lsp_enrichment.md` to a best-in-class implementation target that:

1. Fully deprecates and deletes LSP runtime in `tools/cq`.
2. Recovers as much actionable insight as possible using `ast-grep-py`, Python AST/symtable, `tree-sitter`, and `LibCST`.
3. Improves correctness, determinism, observability, and upgrade safety beyond the current plan.

---

## Inputs Reviewed

### Existing plans

1. `docs/plans/deprecate_lsp_enrichment.md`
2. `docs/plans/cq_lsp_deprecation_static_analysis_cutover_implementation_plan_v1_2026-02-14.md`

### Current implementation surfaces

1. LSP front door and providers in `tools/cq/search/*lsp*`, `tools/cq/search/lsp/*`
2. LSP consumers in:
   - `tools/cq/search/smart_search.py`
   - `tools/cq/macros/calls.py`
   - `tools/cq/query/entity_front_door.py`
   - `tools/cq/query/merge.py`
   - `tools/cq/neighborhood/bundle_builder.py`
3. Existing static enrichment substrate:
   - `tools/cq/search/python_analysis_session.py`
   - `tools/cq/search/python_enrichment.py`
   - `tools/cq/search/libcst_python.py`
   - `tools/cq/search/tree_sitter_python.py`
   - `tools/cq/search/rust_enrichment.py`
   - `tools/cq/search/tree_sitter_rust.py`
   - `tools/cq/neighborhood/structural_collector.py`

### Library documentation (source basis for upgrades)

1. `docs/python_library_reference/ast-grep-py.md` (notably §12 practical boundaries, §A multi-edit robustness)
2. `docs/python_library_reference/ast-grep-py_rust_deepdive.md` (Rust structural taxonomy and macro surfaces)
3. `docs/python_library_reference/python_ast_libraries_and_cpg_construction.md` (symtable scope/binding model; byte-span invariants)
4. `docs/python_library_reference/tree-sitter.md` (incremental parsing, query cursor controls, diagnostics nodes)
5. `docs/python_library_reference/tree-sitter_advanced.md` (query-pack architecture, locals/highlights/injections, range/match-limit mechanics)
6. `docs/python_library_reference/tree-sitter_outputs_overview.md` (agent-ready CST output contract)
7. `docs/python_library_reference/tree-sitter-rust.md` (macro limitations, rust index artifacts, drift/ABI governance)
8. `docs/python_library_reference/libcst.md` (Scope/QualifiedName/FQN capabilities)
9. `docs/python_library_reference/libcst-advanced.md` (metadata identity rules, FullRepoManager, scope limitations)

---

## Findings on the Parallel Plan

## What is strong

1. Correct strategic direction: delete LSP runtime and make structural enrichment authoritative.
2. Good inventory of delete candidates and primary mixed-file callsites.
3. Acknowledges known non-replaceable LSP surfaces (macro expansion, full type inference).

## What is incomplete for a best-in-class target

1. It is delete-first, not contract-first.
   - High risk of breaking consumers before replacement outputs are stable.
2. No explicit static replacement contract equivalent to current front-door payload semantics.
   - It lists removals but does not define the new canonical schema boundary.
3. Python replacement is under-specified for repository-scale symbol identity.
   - LibCST `FullRepoManager`/FQN mode is not planned as a first-class track.
4. Rust replacement is under-specified for pack governance.
   - No query-pack registry/lint strategy tied to `node-types.json` drift.
5. No parity matrix with confidence tiers per LSP field.
   - Needed to avoid accidental silent capability loss.
6. No static semantic-plane architecture replacing `lsp_advanced_planes`.
   - Needs explicit highlights/locals/injections/diagnostics model.
7. Validation strategy is too thin for a high-risk architectural cutover.
   - Needs contract/golden parity suites, not only smoke commands.
8. Performance and drift controls are missing as first-class gates.
   - Should include incremental parsing windows, match limits, ABI checks, grammar drift CI.

---

## Best-in-Class Target Design

## 1) Contract-First Semantic Front Door

Replace LSP-specific interfaces with a language-agnostic static semantic contract before deleting any provider code.

### New contract family

1. `LanguageSemanticEnrichmentRequest`
2. `LanguageSemanticEnrichmentOutcome`
3. `SemanticContractStateV1` (replacing `LspContractStateV1`)

### New summary namespace (hard cut naming)

1. `python_semantic_overview`
2. `python_semantic_telemetry`
3. `rust_semantic_telemetry`
4. `semantic_planes`
5. `python_semantic_diagnostics`

This prevents carrying LSP semantics in names after LSP removal and keeps report/rendering clean.

## 2) Semantic Planes v2 (Static)

Replace `lsp_advanced_planes` with deterministic parser-based planes:

1. `semantic_tokens`: from tree-sitter highlights captures.
2. `locals`: from tree-sitter locals captures and scope association.
3. `diagnostics`: from:
   - tree-sitter `(ERROR)` and `(MISSING)`
   - Python `ast` syntax errors
4. `injections`: from tree-sitter injections planning and execution metadata.

This aligns with `tree-sitter_advanced.md` §9 and `tree-sitter_outputs_overview.md` §§2-3.

## 3) Python Deep Semantic Stack

### Required capability layers

1. Structural capture: ast-grep.
2. Syntax semantics: Python `ast`.
3. Lexical truth: `symtable`.
4. Name/scope/reference semantics: LibCST metadata (`ScopeProvider`, `QualifiedNameProvider`).
5. Optional repo-global identity mode: LibCST `FullyQualifiedNameProvider` via `FullRepoManager`.
6. Query projections and diagnostics: tree-sitter query packs.

### Design requirement

Represent definitions/references as byte-span-anchored rows, and keep source attribution per field (`ast_grep`, `python_ast`, `symtable`, `libcst`, `tree_sitter`).

## 4) Rust Deep Semantic Stack

### Required capability layers

1. Structural extraction: ast-grep + current `rust_enrichment.py`.
2. Parser-backed shape extraction: `tree_sitter_rust.py`.
3. Query-pack based artifacts:
   - defs
   - refs
   - calls
   - types
   - attrs/docs
   - errors
4. Optional injections for known macro inputs.

### Macro stance (explicitly enforced)

1. Do not claim expansion semantics from parser-only data.
2. Index macro invocation sites as first-class facts with stable IDs.
3. Keep expansion-derived semantics out-of-scope unless a separate optional enrichment is introduced later.

This follows `tree-sitter-rust.md` §G and §I.

---

## LSP-to-Static Parity Matrix (Design Target)

| Previous LSP insight | Static replacement target | Parity tier |
|---|---|---|
| Python definition/declaration grounding | LibCST scope assignments + tree-sitter definition captures + ast-grep anchor context | `high` |
| Python references/same-scope symbols | LibCST accesses/referents + symtable scope graph | `high` |
| Python import alias resolution | LibCST import alias chain + qualified names | `high` |
| Python type contract (annotated code) | AST signature + annotation extraction + optional LibCST FQN/type metadata | `medium-high` |
| Python type contract (unannotated inferred) | Not fully replaceable without external type engine | `low` |
| Python call graph preview | structural collector + call captures + scope-aware target normalization | `medium-high` |
| Rust symbol grounding | rust defs/refs packs + structural collector | `medium-high` |
| Rust call graph preview | rust calls packs + structural neighborhood edges | `medium-high` |
| Rust type hierarchy | trait/impl relation extraction (partial) | `medium` |
| Rust diagnostics | tree-sitter error/missing diagnostics | `medium-high` |
| Rust macro expansion/runnables | invocation + token-tree/injection facts only | `low` (intentional) |
| LSP semantic tokens/inlay hints | highlights/locals/diagnostics/injections planes | `medium` |

Parity tier guidance:
1. `high`: credible replacement for primary user workflows.
2. `medium-high`: good front-door utility, some precision loss.
3. `medium`: useful but partial.
4. `low`: intentionally accepted gap.

---

## Improvements to Implementation Plan (Delta)

## Delta A: Re-sequence to Contract-First

Change from delete-first to this sequence:

1. Introduce static contract + adapter/pipeline.
2. Switch consumers (`smart_search`, `calls`, `entity`, `merge`, `neighborhood`) to new contract.
3. Validate parity and goldens.
4. Delete LSP modules only after all consumers are on static contract.

## Delta B: Formal Query-Pack Governance

Add mandatory pack governance for both Python and Rust:

1. Compile-time pack lint.
2. Node/field validation against grammar metadata.
3. `node-types.json` drift report.
4. ABI compatibility gate for tree-sitter runtime vs grammar.
5. Golden fixtures keyed by `cq.kind` and spans.

This mirrors `tree-sitter-rust.md` §J and existing Python pack lint pattern in `tools/cq/search/tree_sitter_python.py`.

## Delta C: Static Confidence and Degradation Taxonomy

Introduce deterministic static reasons and confidence buckets:

1. `source_unavailable`
2. `parse_error`
3. `query_pack_compile_error`
4. `query_pack_no_signal`
5. `scope_resolution_partial`
6. `macro_semantics_unavailable`
7. `annotation_absent`

This replaces current LSP-oriented degradation semantics with actionable static diagnostics.

## Delta D: Repository-Scale Python Semantics Track

Plan two Python modes:

1. Core mode: single-file LibCST + AST + symtable.
2. Repo mode: enable `FullRepoManager` for FQN-resolved identities and stronger cross-file symbol coherence.

This avoids over-promising parity while giving a clear path to better-than-current structural output quality.

## Delta E: Explicit Artifact Contract for Agent Workloads

Adopt the agent-ready artifact shape from `tree-sitter_outputs_overview.md`:

1. `cst_nodes`
2. `cst_edges`
3. `cst_tokens` (optional)
4. `cst_diagnostics`
5. `cst_query_hits`

Then build semantic projections from these deterministic tables.

## Delta F: Stronger Validation Harness

Add mandatory validation layers:

1. Contract tests for static enrichment payloads.
2. Field-level parity diff tests against pre-cutover goldens (for high/medium-high tier fields).
3. Dataset with macro-heavy Rust and nested-scope Python cases.
4. Performance regressions for:
   - parse times
   - query times
   - cache hit rates
   - match-limit exceed counts

---

## Best-in-Class Execution Plan

## Wave 0: Baseline and Contract Freeze

1. Freeze current LSP-era output artifacts and summary keys.
2. Define `SemanticContractV1` target fields and parity tiers.
3. Add parity harness with fail-on-regression for `high` tier fields.

## Wave 1: Static Front Door Foundation

1. Implement static front-door adapter/pipeline/contracts.
2. Wire `smart_search`, `calls`, `entity`, `merge` to static adapter behind a temporary internal switch.
3. Keep LSP code present but unreachable in default path for one migration wave.

## Wave 2: Python Deep Parity

1. Strengthen LibCST/symtable resolution outputs.
2. Expand Python query packs (highlights/locals/diagnostics).
3. Add repo-mode FQN option via `FullRepoManager`.

## Wave 3: Rust Deep Parity

1. Add Rust query-pack infrastructure and linting.
2. Emit defs/refs/calls/types/attrs/docs/errors projections.
3. Add macro invocation and injection artifact streams.

## Wave 4: Neighborhood + Rendering + Diagnostics Cutover

1. Replace LSP slices in neighborhood with static slices.
2. Replace LSP summary keys/report sections with semantic equivalents.
3. Remove `--no-lsp` and `no_lsp`; optionally add `--no-semantic-enrichment` debug flag.

## Wave 5: Hard Delete and Cleanup

1. Delete all LSP provider/session/front-door/plane modules.
2. Remove LSP runtime policy and scheduler lane.
3. Remove LSP fields from SNB metadata and report contracts.
4. Update docs and CQ guidance to static-only model.

---

## Quality and Operations Gates

## Correctness gates

1. No imports of LSP modules remain in `tools/cq`.
2. No summary keys include `pyrefly`, `rust_lsp`, or `lsp_advanced`.
3. Parity harness passes for `high` and `medium-high` tier fields.

## Drift and upgrade gates

1. Tree-sitter ABI gate.
2. `node-types.json` drift diff gate.
3. Query-pack lint and compile gate.

## Performance gates

1. Bounded query execution with match limits.
2. Incremental parse/window usage where applicable.
3. Parse/query timeout and degradation telemetry emitted consistently.

---

## Conclusions

1. The existing `deprecate_lsp_enrichment.md` plan is strong on deletion mechanics but not yet best-in-class on replacement architecture.
2. The key upgrade is contract-first migration with a formal parity matrix and static semantic planes, not immediate module deletion.
3. Python parity can be very high when LibCST + symtable + AST are combined with explicit source attribution and optional repo-mode FQN.
4. Rust parity should target robust structural semantics with honest macro limitations, backed by query-pack governance and drift controls.
5. With the upgraded sequencing and governance above, final state quality will exceed a simple LSP removal and provide a stable, maintainable semantic platform for CQ.

