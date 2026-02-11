# CQ Comprehensive Testing + Realistic Golden Dataset Plan (Python + Rust)

## Summary
This document proposes what must be added to reach comprehensive CQ test coverage and a realistic, durable golden dataset for both Python and Rust.

Assessment inputs reviewed:
- Skill contract: `.codex/skills/cq/SKILL.md`
- CQ implementation surface: `tools/cq/*` (search, query, run, neighborhood, ldmd, LSP planes)
- Current tests: `tests/unit/cq/*` and `tests/e2e/cq/*`

Current state is strong at unit-level breadth, but e2e/golden coverage is still narrow and not yet representative of the full capability contract in `SKILL.md`.

## Current Gaps That Block “Comprehensive”

### 1. E2E coverage is heavily query-centric
Current e2e tests focus on `q` behavior and parser/query semantics. There is little or no command-level e2e coverage for:
- `search` macro output contracts
- `neighborhood` macro and `run` neighborhood steps
- `chain` macro
- `ldmd` end-to-end behavior (`index/search/get/neighbors`)
- advanced LSP evidence planes in realistic flows

### 2. Golden dataset is too small and repo-coupled
Current goldens in `tests/e2e/cq/fixtures` are few and reference live repo files (for example `tools/cq/core/toolchain.py`). This causes churn when repo internals change and does not model realistic Python/Rust projects.

### 3. Rust fixture realism is limited
`tests/e2e/cq/_fixtures/rust_sample.rs` is useful but too small to represent real rust-analyzer surfaces (multi-module crates, traits + impls across files, macro expansion breadth, runnables, diagnostics contexts).

### 4. Advanced-plane tests still include stub-era assertions
`tests/unit/cq/search/test_advanced_planes.py` still contains explicit “stub returns None/empty” tests alongside implemented-path tests. That leaves ambiguity in expected behavior and weakens regression signal.

### 5. No transcript-style LSP lifecycle validation at integration level
Unit tests cover many normalization paths, but there is no cohesive harness that validates full JSON-RPC session behavior (initialize/capabilities/notifications/quiescence/shutdown) with scripted servers in a realistic sequence.

## What To Add

## A. Build a Hermetic Golden Workspace Dataset
Create an isolated fixture workspace under:
- `tests/e2e/cq/_golden_workspace/python_project/`
- `tests/e2e/cq/_golden_workspace/rust_workspace/`
- `tests/e2e/cq/_golden_workspace/mixed_workspace/`

Design principles:
- Hermetic: no dependency on CodeAnatomy source files for core goldens.
- Realistic: model patterns CQ claims to support (not toy single-file snippets).
- Deterministic: stable symbol names, line anchors, and deterministic ordering.
- Multi-file: force cross-file references/implementations/import behavior.

### Python dataset minimum content
- Package with modules for:
  - inheritance + protocols + generics
  - decorators + async/await
  - dynamic dispatch (`getattr`, forwarding)
  - imports and re-exports across subpackages
  - intentional diagnostics cases (type mismatch, unresolved symbols)
- Include at least one ambiguous symbol name across files for resolver tie-break testing.

### Rust dataset minimum content
- Cargo workspace with at least 2 crates.
- Features to include:
  - traits + impl blocks across modules
  - macro definition + invocation
  - runnables (`#[test]`, binary target)
  - references/implementations split across files
  - intentional diagnostics cases (borrow/type errors) in non-critical fixture files

### Mixed dataset minimum content
- Python + Rust roots in one workspace to validate:
  - `lang=auto` behavior
  - include-glob scoping under `src/`, `tools/`, `rust/`-like layouts
  - language filtering safety (no silent cross-language leakage)

## B. Expand Golden Coverage to All Public CQ Macros
Add golden/spec tests for:
- `search`
- `q` (entity/pattern/composite/relational)
- `run` (single step, multi-step, neighborhood step, mixed-language)
- `chain`
- `neighborhood` and `nb`
- `ldmd` (`index`, `search`, `get`, `neighbors`)

Use two validation modes:
- Snapshot goldens: normalized `CqResult`/LDMD output for deterministic behavior.
- Spec goldens: declarative expectations (`min_findings`, required categories, required degrade events, required section titles) to reduce brittleness.

## C. Replace Stub-Era Advanced Plane Assertions With Contract Assertions
Refactor `tests/unit/cq/search/test_advanced_planes.py` into focused files:
- `tests/unit/cq/search/test_semantic_overlays.py`
- `tests/unit/cq/search/test_diagnostics_pull.py`
- `tests/unit/cq/search/test_refactor_actions.py`
- `tests/unit/cq/search/test_rust_extensions.py`

Required behaviors to test:
- Capability-gated success paths
- Capability-gated skip paths (explicit `None`/empty where expected)
- Resolve flows (`inlayHint/resolve`, `codeAction/resolve`)
- Diagnostics normalization (`textDocument/diagnostic`, `workspace/diagnostic`, relatedDocuments)
- Fail-open behavior on malformed server payloads

## D. Add Scripted LSP Integration Harness Tests
Add a lightweight scripted JSON-RPC stdio test server harness to validate real session flow for both Pyrefly and Rust LSP clients:
- initialize -> initialized
- capability extraction
- document open/update versioning
- request/response framing
- notifications (`publishDiagnostics`, rust status events)
- quiescence gating
- graceful shutdown idempotence

Suggested location:
- `tests/unit/cq/search/lsp_harness/` (server + transcript fixtures)

Suggested tests:
- `tests/unit/cq/search/test_pyrefly_lsp_integration_transcript.py`
- `tests/unit/cq/search/test_rust_lsp_integration_transcript.py`

## E. Add Command-Level E2E Matrix Against Golden Workspace
Create dedicated command e2e tests:
- `tests/e2e/cq/test_search_command_e2e.py`
- `tests/e2e/cq/test_neighborhood_command_e2e.py`
- `tests/e2e/cq/test_run_command_e2e.py`
- `tests/e2e/cq/test_chain_command_e2e.py`
- `tests/e2e/cq/test_ldmd_command_e2e.py`

Required scenario matrix:
1. Python-only searches/queries.
2. Rust-only searches/queries.
3. Auto-language mixed workspace queries.
4. Symbol-only neighborhood target resolution.
5. `file:line[:col]` neighborhood targeting.
6. Partial capability/degrade scenarios (no LSP / limited caps).
7. Run-step parity: CLI neighborhood vs run-plan neighborhood.
8. LDMD depth/mode extraction on real output.

## F. Upgrade Golden Normalization + Tooling
Extend `tests/e2e/cq/_support/goldens.py` normalization to scrub:
- machine/local absolute paths
- variable toolchain paths/binary locations
- unstable telemetry counters/timings
- optional server-specific metadata fields that are nondeterministic

Add a single update command script:
- `scripts/update_cq_goldens.sh`

Script should:
1. run focused e2e golden tests with update flags
2. validate no unstabilized fields remain
3. print changed fixture list for review

## Proposed New File/Folder Additions
- `tests/e2e/cq/_golden_workspace/python_project/...`
- `tests/e2e/cq/_golden_workspace/rust_workspace/...`
- `tests/e2e/cq/_golden_workspace/mixed_workspace/...`
- `tests/e2e/cq/fixtures/golden_specs/*.json`
- `tests/e2e/cq/test_search_command_e2e.py`
- `tests/e2e/cq/test_neighborhood_command_e2e.py`
- `tests/e2e/cq/test_run_command_e2e.py`
- `tests/e2e/cq/test_chain_command_e2e.py`
- `tests/e2e/cq/test_ldmd_command_e2e.py`
- `tests/unit/cq/search/test_semantic_overlays.py`
- `tests/unit/cq/search/test_diagnostics_pull.py`
- `tests/unit/cq/search/test_refactor_actions.py`
- `tests/unit/cq/search/test_rust_extensions.py`
- `tests/unit/cq/search/lsp_harness/*`
- `scripts/update_cq_goldens.sh`

## Acceptance Criteria For “Comprehensive”
1. Every documented CQ macro in `.codex/skills/cq/SKILL.md` has at least one command-level e2e test on the hermetic golden workspace.
2. Python and Rust each have realistic multi-file golden datasets with cross-file symbol resolution and diagnostics scenarios.
3. Advanced-plane tests no longer rely on “stub behavior” assertions as primary coverage.
4. LSP lifecycle is validated by transcript-based integration tests for both Pyrefly and Rust clients.
5. Golden snapshots are stable across machines after normalization (no path/time/toolchain drift).
6. `uv run pytest -q tests/unit/cq tests/e2e/cq` passes with deterministic results.

## Recommended Delivery Sequence
1. Build golden workspace datasets (Python, Rust, mixed).
2. Add command-level e2e tests and spec goldens for `search/q/run/chain/neighborhood/ldmd`.
3. Split and harden advanced-plane unit tests; remove stub-era ambiguity.
4. Add LSP transcript integration harness tests.
5. Finalize golden normalization + update script + CI lane wiring.

## Notes
- Keep fail-open semantics for LSP-dependent planes; test for explicit degrade signals rather than hard failures.
- Keep repo-source-dependent tests as supplemental smoke checks, not as canonical goldens.
- Prefer small, scenario-specific goldens over single giant snapshots to reduce churn and improve reviewability.
