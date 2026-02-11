# CQ Enhancement Implementation Plan v2 Review and Best-in-Class Scope Expansion

Date: 2026-02-11  
Source plan reviewed: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md`  
Reference docs reviewed:
- `docs/python_library_reference/pyrefly_lsp_data.md`
- `docs/python_library_reference/rust_lsp.md`
- `docs/python_library_reference/rich_md_overview.md`

Codebase cross-check targets:
- `tools/cq/cli_app/context.py`
- `tools/cq/cli_app/result.py`
- `tools/cq/cli_app/types.py`
- `tools/cq/cli_app/app.py`
- `tools/cq/query/sg_parser.py`
- `tools/cq/astgrep/sgpy_scanner.py`
- `tools/cq/query/executor.py`
- `tools/cq/run/spec.py`
- `tools/cq/run/loader.py`
- `tools/cq/run/runner.py`
- `tools/cq/search/pyrefly_lsp.py`
- `tools/cq/search/pyrefly_contracts.py`
- `tools/cq/search/rust_enrichment.py`
- `tools/cq/core/schema.py`

---

## Executive Conclusion

The updated plan is materially stronger than the earlier revision, but it is **not yet implementation-safe**. There are still multiple blocking API mismatches, capability-model errors, and internal contract inconsistencies that will cause implementation churn or broken behavior if started as-is.

Primary conclusion:
- Keep the current R0-R8 structure, but apply the blocking corrections below before coding.
- Add the proposed scope expansions to fully leverage Pyrefly + Rust LSP evidence planes and to align LDMD with Rich/Textual constraints.

---

## Blocking Corrections (Must Fix Before Implementation)

### B1) CLI type/import contract mismatches in snippets

Issue:
- Plan snippets import `CliResult` and `CliContext` from incorrect modules and use non-existent helper constructors.

Evidence:
- Plan imports `CliResult` from `tools.cq.cli_app.result` and `CliContext` from `tools.cq.cli_app.types`: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2402`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2403`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2082`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2083`
- Plan uses `CliContext.default()` which does not exist: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2421`
- Plan uses `CliResult.success/error` helpers that do not exist: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2098`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2118`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2154`
- Actual definitions live in `tools/cq/cli_app/context.py`: `tools/cq/cli_app/context.py:55`, `tools/cq/cli_app/context.py:135`

Required correction:
- Import `CliContext`/`CliResult` from `tools.cq.cli_app.context`.
- Use `ctx` injection pattern already used by commands (`ctx is None -> RuntimeError`) rather than `CliContext.default()`.
- For ldmd/admin-like commands, return `int` where appropriate (as in `tools/cq/cli_app/commands/admin.py:28`), or return `CliResult(result=<CqResult>, ...)` only when you actually have a `CqResult`.

---

### B2) `sg_scan` and `SgRecord` path/signature are incorrect in plan snippets

Issue:
- Plan references non-existent modules and wrong function signatures.

Evidence:
- Non-existent modules referenced: `tools.cq.search.sg_scan`, `tools.cq.search.sg_types`: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2429`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:1406`
- Actual `sg_scan` is in `tools/cq/query/sg_parser.py`: `tools/cq/query/sg_parser.py:42`
- Actual `SgRecord` is in `tools/cq/astgrep/sgpy_scanner.py`: `tools/cq/astgrep/sgpy_scanner.py:75`
- `sg_scan` requires `paths` and uses `lang`, not `language`: `tools/cq/query/sg_parser.py:43`, `tools/cq/query/sg_parser.py:47`

Required correction:
- Update all snippets and checklists to use `tools.cq.query.sg_parser.sg_scan` and `tools.cq.astgrep.sgpy_scanner.SgRecord`.
- Update invocation signatures (`paths=[root]`, `lang=...`, optional `record_types`, `globs`, `root`).

---

### B3) Capability gate algorithm is wrong for LSP capability shapes

Issue:
- Plan checks capabilities against incorrect dictionary structure and mixes server vs client capability semantics.

Evidence:
- Capability lookup assumes `negotiated["textDocument"][method]`: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:1068`
- Plan treats `textDocument/publishDiagnostics` as a server capability to gate slices: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:1015`
- Rust LSP doc explicitly places diagnostics gates under **client capability** `textDocument.publishDiagnostics.*` and `dataSupport`: `docs/python_library_reference/rust_lsp.md:358`, `docs/python_library_reference/rust_lsp.md:364`

Required correction:
- Introduce a typed `CapabilitySnapshot` split into:
  - `server_caps` (`InitializeResult.capabilities`)
  - `client_caps` (what CQ advertised)
  - `experimental_caps` (including extension opt-ins)
- Gate methods by provider fields (`definitionProvider`, `referencesProvider`, `callHierarchyProvider`, etc.) and gate diagnostic-bridge features by client `publishDiagnostics.dataSupport`.

---

### B4) Rust session health/quiescence design needs protocol-safe behavior

Issue:
- Quiescence strategy depends on server status extension without explicit opt-in and currently returns hard `None` before ready, conflicting with fail-open expectations.

Evidence:
- Plan waits on `experimental/serverStatus`: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:833`
- Rust extension requires client opt-in `experimental.serverStatusNotification`: `docs/python_library_reference/rust_lsp.md:204`
- Plan enforces `return None` when not quiescent: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:896`
- Existing Rust enrichment pipeline is fail-open + degrade-aware, not hard-fail: `tools/cq/search/rust_enrichment.py:523`, `tools/cq/search/rust_enrichment.py:563`, `tools/cq/search/rust_enrichment.py:638`
- Health has 3 states (`ok|warning|error`), not binary: `docs/python_library_reference/rust_lsp.md:211`

Required correction:
- Explicitly advertise `experimental.serverStatusNotification=true` when starting Rust session.
- Use staged gating:
  - always allow structural output,
  - allow cheap LSP calls during non-quiescent states with degraded confidence,
  - delay only high-cost/high-risk surfaces.
- Model health tri-state and map to typed degrade events; avoid silent `None` unless session is fully unavailable.

---

### B5) Pyrefly adapter snippet uses fields not present in current typed contracts

Issue:
- Proposed adapter code reads fields that do not exist in `PyreflyEnrichmentPayload` and uses wrong edge field names.

Evidence:
- Plan uses `sg.references` and `payload.type_hierarchy`: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:1225`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:1256`
- Current payload sections are `symbol_grounding`, `type_contract`, `call_graph`, `class_method_context`, `local_scope_context`, `import_alias_resolution`, `anchor_diagnostics`, `coverage`: `tools/cq/search/pyrefly_contracts.py:396`
- Current call graph edge fields are `symbol/file/line/col` (not `name/uri`): `tools/cq/search/pyrefly_contracts.py:107`

Required correction:
- Either:
  1. Rewrite adapter for current payload model, or
  2. Expand contracts first (versioned) and then update adapter/checklists accordingly.
- Do not include pseudo-fields in implementation snippets unless contracts are updated in the same phase.

---

### B6) Internal SNB contract drift across R1 vs R5/R6 snippets

Issue:
- Later sections assume bundle/slice fields not defined in R1 schema.

Evidence:
- R1 slice defines `preview` + `edges`, no `nodes`: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:245`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:246`
- R6 writer iterates `s.nodes`: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2292`
- R1 bundle defines `bundle_id/subject_node_id/subject_label/slices/node_index/diagnostics/metadata`: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:271`
- R5/R6 snippets expect `bundle.meta`, `bundle.subject`, `bundle.graph`, `bundle.artifacts`: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:1751`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2268`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2316`

Required correction:
- Freeze one canonical SNB schema in R1, then force R5/R6/R7 snippets to use only those fields.
- Add a schema compatibility checklist item: every later snippet must compile against R1 contracts with no shadow structs.

---

### B7) LDMD parser algorithm and marker grammar are inconsistent with the reference contract

Issue:
- Current parser sketch is not structurally safe and diverges from documented marker grammar.

Evidence:
- Plan does two independent passes over BEGIN then END markers with stack logic that breaks normal stream semantics: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:1943`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:1954`
- Plan computes “byte offsets” from decoded text slices: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:1950`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:1963`
- Plan marker regex uses simplified `<!-- LDMD:BEGIN:... -->`: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:1905`
- Rich LDMD reference uses attribute-based markers with `id="..."`: `docs/python_library_reference/rich_md_overview.md:523`, `docs/python_library_reference/rich_md_overview.md:659`

Required correction:
- Parse in a single forward pass over bytes.
- Use marker grammar aligned with attribute-based IDs (`BEGIN id=...`, `END id=...`) and validate matching IDs.
- Keep offsets in byte coordinates from raw content.

---

### B8) UTF-8 truncation helper is insufficiently safe

Issue:
- Backing up over continuation bytes alone can still return invalid UTF-8 boundaries in edge cases.

Evidence:
- Current helper logic only inspects continuation-bit pattern: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2015`

Required correction:
- Add strict decode validation loop (`candidate.decode("utf-8")`) after boundary adjustment.
- Add tests with 2/3/4-byte code points and truncation at each byte position.

---

### B9) `NeighborhoodStep` must conform to existing tagged run-step model

Issue:
- Step snippet does not follow current `RunStepBase` tagging model, which impacts loader/type guards.

Evidence:
- Plan defines bare `msgspec.Struct` step: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2505`
- Existing run-step base has tag/tag_field semantics: `tools/cq/run/spec.py:21`
- Loader checks step instances with `is_run_step` (`RunStepBase`): `tools/cq/run/spec.py:133`, `tools/cq/run/loader.py:74`

Required correction:
- Define `NeighborhoodStep(RunStepBase, tag="neighborhood", frozen=True)`.
- Update `RunStep`, `RUN_STEP_TYPES`, `_STEP_TAGS`, and dispatch in `tools/cq/run/runner.py:709`.

---

### B10) R0 snippet imports `CqResult` from a non-existent module

Issue:
- `tools.cq.core.result` is not present.

Evidence:
- Plan snippet import: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:78`
- Actual `CqResult` definition: `tools/cq/core/schema.py:314`

Required correction:
- Replace references with `from tools.cq.core.schema import CqResult`.

---

## High-Value Optimizations

### O1) Capability snapshot normalization (single abstraction for Python + Rust)

Implement a shared contract:
- `LspCapabilitySnapshotV1`:
  - `server_info`
  - `server_caps`
  - `client_caps`
  - `experimental_caps`
  - `position_encoding`
  - `negotiation_timestamp`

Why:
- Removes repeated hand-written capability checks.
- Enables deterministic gate diagnostics and telemetry for both pyrefly and rust sessions.

---

### O2) Health-aware progressive execution policy

Use a tiered gating matrix keyed by `health` + `quiescent`:
- Tier A (always): structural + low-cost lookups
- Tier B (`health in {ok,warning}`): definitions/references/hovers
- Tier C (`health=ok` and preferably quiescent): expensive graph expansion/refactor planning

Why:
- Aligns with rust-analyzer status semantics (`ok|warning|error`): `docs/python_library_reference/rust_lsp.md:211`
- Preserves fail-open UX while keeping confidence explicit.

---

### O3) Unified diagnostics-to-actions bridge

Add common normalized shapes for both LSP backends:
- diagnostics with `relatedInformation`, `codeDescription`, opaque `data`
- code actions with `kind`, `disabled.reason`, `data`, `edit`, `command`

Why:
- Rust reference explicitly identifies `Diagnostic.data` bridging to `codeAction`: `docs/python_library_reference/rust_lsp.md:364`, `docs/python_library_reference/rust_lsp.md:414`
- Pyrefly reference also leans on prepareRename/codeAction/resolve for safe edits: `docs/python_library_reference/pyrefly_lsp_data.md:4698`, `docs/python_library_reference/pyrefly_lsp_data.md:4797`

---

### O4) Semantic overlay persistence should be atomic (legend + encoding + payload)

Store semantic tokens with:
- negotiated position encoding
- server legend
- raw stream (`data`/`edits`)
- decoded rows
- cache keys (`resultId`, previous id)

Why:
- Required decode contract in Rust reference: `docs/python_library_reference/rust_lsp.md:1679`, `docs/python_library_reference/rust_lsp.md:1867`
- Same requirement called out in Pyrefly reference: `docs/python_library_reference/pyrefly_lsp_data.md:3802`, `docs/python_library_reference/pyrefly_lsp_data.md:3819`

---

## Scope Expansions for Best-in-Class LSP Leverage

### E1) Expand Pyrefly contract planes beyond current payload

Current payload in repo (`tools/cq/search/pyrefly_contracts.py:396`) is missing several high-value planes documented in the reference. Add versioned optional sections:
- `semantic_overlay` (legend + tokens + inlay hints)
- `structural_discovery` (document symbols + workspace symbols)
- `edit_actions` (prepareRename, rename, codeAction, resolve)
- `local_mutation_signal` (documentHighlight read/write)

Reference anchors:
- semantic tokens: `docs/python_library_reference/pyrefly_lsp_data.md:3802`
- legend requirement: `docs/python_library_reference/pyrefly_lsp_data.md:3819`
- documentHighlight read/write value: `docs/python_library_reference/pyrefly_lsp_data.md:1734`
- rename/codeAction surfaces: `docs/python_library_reference/pyrefly_lsp_data.md:4698`, `docs/python_library_reference/pyrefly_lsp_data.md:4797`

---

### E2) Add Rust environment/configuration invalidation plane

Add persistent environment records and invalidation events:
- `serverStatus` stream
- refresh events (`workspace/inlayHint/refresh`, `workspace/semanticTokens/refresh`, `workspace/codeLens/refresh`)
- config pull/change events (`workspace/configuration`, `workspace/didChangeConfiguration`)

Reference anchors:
- server status + quiescence: `docs/python_library_reference/rust_lsp.md:202`
- refresh events: `docs/python_library_reference/rust_lsp.md:281`, `docs/python_library_reference/rust_lsp.md:297`, `docs/python_library_reference/rust_lsp.md:307`
- configuration model: `docs/python_library_reference/rust_lsp.md:2668`

---

### E3) Explicit top-K/range-first LSP query strategy in plan

Adopt a consistent request budget model for both languages:
- top-K anchor selection
- range-scoped inlay and semantic token pulls first
- promote to full/delta only when needed
- content-hash + config-fingerprint cache keys

Reference anchors:
- Pyrefly minimal request plan, top-K/range inlay and cache keys: `docs/python_library_reference/pyrefly_lsp_data.md:917`, `docs/python_library_reference/pyrefly_lsp_data.md:940`, `docs/python_library_reference/pyrefly_lsp_data.md:946`
- Rust semantic token full/range/delta negotiation: `docs/python_library_reference/rust_lsp.md:1689`, `docs/python_library_reference/rust_lsp.md:1751`

---

### E4) LDMD protocol expansion aligned with Rich/Textual realities

Expand LDMD design to include:
- attribute-based marker IDs (`BEGIN id=...`/`END id=...`)
- optional TLDR markers per section
- explicit note that Rich markdown is static; interactive collapse requires Textual

Reference anchors:
- Rich static rendering limitation: `docs/python_library_reference/rich_md_overview.md:402`
- marker format with attributes: `docs/python_library_reference/rich_md_overview.md:523`
- Rich/Textual interoperability model: `docs/python_library_reference/rich_md_overview.md:709`

---

## Recommended Plan Adjustments by Release Slice

### Phase P0 (Plan Repair)
- Fix B1-B10 in the document text/snippets/checklists.
- Freeze canonical SNB schema and propagate it through all later sections.

### Phase P1 (Core Contracts + Session Envelope)
- Land R0/R1 with corrected imports and type paths.
- Implement shared capability snapshot + degrade taxonomy.

### Phase P2 (LSP Foundations)
- Rust session with status/config/refresh capture and partial-fallback behavior.
- Pyrefly session capability exposure + versioned payload expansion hooks.

### Phase P3 (Neighborhood + LDMD)
- Structural snapshot adapter and assembler on stable contracts.
- LDMD parser/writer with byte-accurate extraction and marker grammar parity.

### Phase P4 (Advanced Planes)
- Semantic overlays (tokens/inlay), edit/refactor action plane, Rust extension plane.
- Integrate on-demand policy and budgeted query strategy.

---

## Definition of Done Additions

Add these acceptance checks to the implementation plan:
- Every snippet compiles against current module paths.
- Capability gates distinguish server-capability vs client-capability checks.
- Non-quiescent Rust sessions return partial payload + degrade events (not hard `None`, unless transport unavailable).
- Semantic token artifacts always include legend + position encoding + decode provenance.
- Diagnostic `data` passthrough verified into codeAction request contexts.
- LDMD parser validated against attribute-based markers and byte-accurate offsets.

---

## Final Assessment

After the blocking corrections, the updated plan can become a strong foundation. The best-in-class gap is now mostly about breadth and persistence discipline:
- richer LSP evidence planes (semantic overlays, structural discovery, edit actions),
- environment/config invalidation tracking,
- strict capability semantics,
- and fully deterministic LDMD extraction aligned with Rich/Textual behavior.

With those additions, CQ can support a genuinely high-fidelity, agent-grade semantic substrate for both Python and Rust.
