# CQ Enhancement Plan v1 Review and Best-in-Class Scope Expansion

Date: 2026-02-11  
Author: Codex (review pass)

## Executive Conclusion

`docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md` is a strong starting structure, but it is not yet implementation-safe against the current CQ codebase and it does not yet fully leverage the highest-value Pyrefly and rust-analyzer LSP surfaces.

The plan should be revised before implementation. There are several blocking integration mismatches (CLI contract, run-step wiring, and scan-context construction), plus major opportunity to expand scope into capability-gated LSP evidence planes (diagnostics, symbol navigation, semantic overlays, and edit/refactor outputs) while preserving CQ's fail-open and targeted execution philosophy.

## Inputs Reviewed

- CQ architecture overviews:
  - `docs/architecture/cq/00_overview.md`
  - `docs/architecture/cq/01_core_infrastructure.md`
  - `docs/architecture/cq/02_search_subsystem.md`
  - `docs/architecture/cq/03_query_subsystem.md`
  - `docs/architecture/cq/04_analysis_commands.md`
  - `docs/architecture/cq/05_multi_step_execution.md`
  - `docs/architecture/cq/06_data_models.md`
  - `docs/architecture/cq/07_ast_grep_and_formatting.md`
- Design + implementation docs:
  - `docs/plans/cq_enhancement_design.md`
  - `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md`
- Reference docs:
  - `docs/python_library_reference/pyrefly_lsp_data.md`
  - `docs/python_library_reference/rust_lsp.md`
  - `docs/python_library_reference/rich_md_overview.md`
- Current CQ implementation checkpoints:
  - `tools/cq/cli_app/commands/search.py`
  - `tools/cq/cli_app/result.py`
  - `tools/cq/cli_app/types.py`
  - `tools/cq/query/executor.py`
  - `tools/cq/run/spec.py`
  - `tools/cq/run/runner.py`
  - `tools/cq/search/pyrefly_lsp.py`
  - `tools/cq/search/pyrefly_contracts.py`
  - `tools/cq/search/rust_enrichment.py`
  - `tools/cq/core/schema.py`
  - `tools/cq/core/enrichment_facts.py`

## Blocking Corrections (Must Fix)

### 1) CLI command contract mismatch in S7

Issue:
- The v1 sample `cmd_neighborhood()` returns `CqResult` directly.
- Current CQ command handlers return `CliResult` and rely on `handle_result()` for rendering, filtering, and artifact handling.

Evidence:
- `tools/cq/cli_app/commands/search.py` returns `CliResult`.
- `tools/cq/cli_app/result.py` expects `CliResult` for output handling.

Impact:
- Returning `CqResult` directly from command handlers can bypass standard render/save/filter flow.

Correction:
- Implement `neighborhood` command with the same command pattern as existing commands:
  - parse options via `params/options`
  - return `CliResult(result=..., context=ctx, filters=...)`

### 2) Invalid scan-context usage in S7

Issue:
- v1 sample calls `_build_scan_context(repo_root, language)`.
- Current `_build_scan_context` signature is `_build_scan_context(records: list[SgRecord])`.

Evidence:
- `tools/cq/query/executor.py`.

Impact:
- Proposed code path will not run.

Correction:
- Build scan context via existing query scan pipeline (`sg_scan` + `_build_scan_context`) or through batch helpers (`build_batch_session`) rather than inventing a new root/lang constructor.

### 3) Non-existent API reference in S7

Issue:
- v1 references `smart_search_impl` in `_resolve_target()`.
- No such function exists.

Evidence:
- `tools/cq/search/smart_search.py` exports `smart_search` only.

Correction:
- Use `smart_search(...)` and parse returned `CqResult` for target resolution.

### 4) Output format integration conflict (`ldmd`)

Issue:
- v1 introduces `--format ldmd` behavior but current `OutputFormat` enum and renderer dispatch do not support `ldmd`.

Evidence:
- `tools/cq/cli_app/types.py` and `tools/cq/cli_app/result.py`.

Correction:
- Add `ldmd` as a first-class `OutputFormat` value and renderer dispatch target.
- Do not shadow global format behavior with a command-local `format: str` argument.

### 5) Run step type expansion incomplete in v1 detail

Issue:
- v1 states adding a `"neighborhood"` step type, but current run model requires updates in multiple places beyond simple dispatch.

Evidence:
- `tools/cq/run/spec.py`: tagged union `RunStep`, `RUN_STEP_TYPES`, `_STEP_TAGS`.
- `tools/cq/run/runner.py`: `_execute_non_q_step` branching.

Correction:
- Add full tagged step struct + union inclusion + step tag map + loader compatibility + runner execution + summary metadata coverage tests.

### 6) S6 ordering logic does not enforce stated section order

Issue:
- v1 declares explicit `SECTION_ORDER` (01-12), but sample `materialize_section_layout()` appends slices in traversal order.

Impact:
- Output ordering will drift by collector return order, not design intent.

Correction:
- Introduce deterministic layout assembly keyed by slice kind + fallback slots, then emit in fixed order.

### 7) `details.kind` registry shape is too weak in S1

Issue:
- v1 sample registry maps kind string to string type names (`dict[str, str]`), not actual struct types or validators.

Impact:
- No runtime validation value.

Correction:
- Use `dict[str, type[msgspec.Struct]]` or an explicit resolver API; integrate it where findings are created or decoded.

### 8) Rust LSP scope in S2 is too narrow for "best-in-class" target

Issue:
- S2 parity claim is broader than actual proposed payload. It omits critical environment and quality surfaces (health/quiescence gating, diagnostics semantics, config fingerprinting, refresh events).

Evidence:
- Rust reference requirements in `docs/python_library_reference/rust_lsp.md`.

Correction:
- Add a required "session/environment" envelope before symbol payloads:
  - negotiated capabilities
  - position encoding
  - server status/quiescence
  - config fingerprint
  - refresh/invalidation events

## Major Optimization Recommendations

### 1) Reuse existing typed payload discipline (do not fork pattern)

Current CQ already has a good typed normalization boundary for Pyrefly:
- `tools/cq/search/pyrefly_contracts.py`
- `coerce_pyrefly_payload(...)` + `pyrefly_payload_to_dict(...)`

Recommendation:
- Create `rust_lsp_contracts.py` with the same normalization and conversion pattern.
- Avoid ad hoc dict plumbing in assemblers.

### 2) Avoid hard coupling to private query internals

The structural collector should not require unstable private helpers from `query/executor.py` without a stable adapter.

Recommendation:
- Add a small public "scan snapshot" adapter module that exposes only what neighborhood needs:
  - defs
  - calls
  - interval index
  - file index

### 3) Capability-gated LSP planning (required)

Architecture docs already call this out as an improvement vector.

Recommendation:
- For each requested neighborhood slice, compute capability prerequisites.
- Execute only feasible requests; mark unavailable slices with structured degrade records.

### 4) Structured degradation model for bundle assembly

Current degradation is string-based in many places.

Recommendation:
- Add typed degrade event rows with:
  - stage
  - severity
  - category
  - message
  - correlation key
- Surface these in `bundle.diagnostics` and provenance section.

### 5) Prefer preview/artifact split universally

This is consistent with CQ architecture and the design doc.

Recommendation:
- Every heavy LSP surface should use:
  - small preview in section details
  - full payload in artifact
  - deterministic artifact IDs and sizes in provenance

## Scope Expansion Required for True Best-in-Class LSP Leverage

The current v1 scope is centered on structural neighborhood + limited Rust LSP. To fully leverage valuable Pyrefly and Rust LSP outputs, expand into the following evidence planes.

### A) Shared environment plane (Python + Rust)

Persist per session/workspace:
- negotiated capabilities
- position encoding
- server identity/version
- workspace health + quiescence
- configuration fingerprint
- refresh/invalidation events

Why:
- This is the quality gate for interpreting all downstream spans and graph edges.

### B) Symbol grounding plane

Keep and standardize:
- definition/declaration/typeDefinition/implementation targets
- references with explicit `includeDeclaration` policy
- document symbols (hierarchical preferred)
- workspace symbol search + lazy resolve policy

Why:
- This yields stable node/edge primitives for neighborhood graphing.

### C) Behavioral plane

Expand and normalize:
- incoming/outgoing call hierarchy (depth-capped)
- type hierarchy (super/sub)
- polymorphism/override expansions

Why:
- Directly supports “functional context where called” and impact reasoning.

### D) Diagnostics and correctness plane

Add explicit per-language diagnostics bundles:
- span, severity, code, source, related info
- version association (when available)
- `Diagnostic.data` passthrough for code-action bridging

Why:
- Enables safe automated edits and confidence-aware ranking.

### E) Semantic overlay plane (on-demand)

Add on-demand support:
- semantic tokens (`range` first, full/delta cached)
- inlay hints (`range` first, resolve-capability aware)

Why:
- High semantic density for summarization/disambiguation with bounded payload cost.

### F) Edit/refactor action plane (on-demand)

Add normalized contracts for:
- rename/prepareRename
- codeAction/codeActionResolve
- workspace edits (including file ops)
- language-specific extension edits (RA `SnippetTextEdit` support awareness)

Why:
- Moves CQ from analysis-only toward safe, evidence-backed refactor planning.

### G) Rust-analyzer deep extension plane (optional, high leverage)

Add targeted integrations with strict gating:
- macro expansion
- runnables
- SSR
- parent module
- analyzer status dump

Why:
- These are uniquely valuable in Rust and provide leverage unavailable via generic LSP alone.

## LDMD/Rich/Textual Strategy Corrections

### 1) LDMD markers are the right base, but parser must be strict

Required hardening:
- explicit stack validation for BEGIN/END nesting
- duplicate ID detection
- deterministic byte offset indexing from raw bytes
- safe truncation preserving UTF-8 boundaries

### 2) Add protocol endpoints as CQ commands/tools, not only format mode

Recommended command surface:
- `cq ldmd index <path>`
- `cq ldmd get <path> --id ... --mode ... --depth ... --limit-bytes ...`
- `cq ldmd search <path> --query ...`
- `cq ldmd neighbors <path> --id ...`

### 3) Optional Textual viewer should be a separate UX layer

Do not couple primary pipeline to TUI state. Keep LDMD retrieval protocol authoritative.

## Revised Implementation Roadmap (v2)

### R0: Correctness alignment patch (before any new features)

- Fix S7 contract issues (CliResult pattern, format integration, scan path correctness).
- Add `ldmd` to global output format model.
- Add stable kind-registry typing and usage points.

### R1: Contract foundation

- Add SNB schema + typed registry + artifact pointer schemas.
- Add typed degrade event model.

### R2: Rust LSP session foundation

- Implement rust session with:
  - capabilities + position encoding persistence
  - server status/quiescence capture
  - config fingerprint capture
- Implement base symbol grounding + diagnostics normalization.

### R3: Pyrefly expansion alignment

- Reuse existing `pyrefly_contracts` directly in neighborhood assembler.
- Add missing high-value optional surfaces (document/workspace symbol, semantic overlays) behind capability gates.

### R4: Structural + semantic assembler

- Merge structural collector + language payloads into deterministic slice model.
- Enforce section order + collapse policy by layout spec.

### R5: LDMD services

- Implement strict LDMD parser/indexer/get/search/neighbors.
- Implement writer with preview/body separation and manifest.

### R6: CLI and run integration

- Add `neighborhood` command with standard CQ command architecture.
- Add run-step support for neighborhood with full schema/update coverage.

### R7: Advanced planes (on-demand)

- semantic tokens / inlay hints
- rename/code-action pipelines
- rust-analyzer extensions (macro/runnable/SSR)

### R8: Optional Textual UX layer

- Viewer-level navigation and selective expansion linked to LDMD protocol.

## Expanded Test Strategy

### Contract tests

- msgspec roundtrip tests for every new struct
- strict union normalization tests (`Location | Location[] | LocationLink[] | null`)
- kind-registry validation tests

### Session/protocol tests

- mock JSON-RPC transcript tests for Pyrefly and Rust sessions
- capability negotiation and position-encoding tests
- health/quiescence gating behavior tests

### Assembler tests

- deterministic ordering tests
- structural-only fallback tests
- mixed-language merge tests
- degrade-event aggregation tests

### LDMD tests

- parser validation error cases (mismatched markers, duplicate IDs)
- byte-offset correctness tests
- truncation/cursor continuation tests
- marker invisibility rendering tests with Rich markdown

### CLI/run tests

- command-level output flow via `CliResult`
- output format dispatch (`md`, `json`, `ldmd`, etc.)
- run-step integration with neighborhood steps

### Golden tests

- add golden snapshots for neighborhood markdown and ldmd outputs

## Final Recommendation

Proceed with a **v2 plan rewrite** before implementation. Keep the seven-scope decomposition concept, but apply:

- immediate integration fixes listed in Blocking Corrections
- capability-gated LSP evidence planes (not a single monolithic fetch)
- strict contract-first normalization for all new payload families
- deterministic LDMD protocol as first-class CQ retrieval tooling

This path preserves CQ's current strengths (fail-open, typed contracts, targeted scans) while materially upgrading semantic depth across both Python and Rust to a best-in-class, agent-ready architecture.
