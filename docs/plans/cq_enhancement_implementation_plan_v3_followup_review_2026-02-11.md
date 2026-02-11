# CQ Enhancement Plan v3 Follow-up Review (2026-02-11)

Reviewed artifacts:
- `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md`
- `docs/python_library_reference/pyrefly_lsp_data.md`
- `docs/python_library_reference/rust_lsp.md`
- `docs/python_library_reference/rich_md_overview.md`

Cross-checked against current code:
- `tools/cq/cli_app/context.py`
- `tools/cq/cli_app/result.py`
- `tools/cq/run/spec.py`
- `tools/cq/search/pyrefly_contracts.py`

---

## Summary

The plan is substantially improved and most prior blockers were addressed. Remaining issues are now concentrated in:
- LDMD command/output integration,
- residual schema inconsistencies between R1 and downstream snippets,
- one unresolved Pyrefly contract mismatch,
- and a few capability/health model correctness gaps.

Conclusion:
- The plan is close, but **not yet implementation-safe**.
- Fixing the P0 items below should be done before execution starts.

---

## Findings (Ordered by Severity)

### P0-1) LDMD CLI return model is still incompatible with current CLI runtime

Problem:
- R6 still uses `CliResult.success(...)` / `CliResult.error(...)`, but those APIs do not exist.
- Even if replaced with plain `CliResult(result=<str>, ...)`, current `handle_result` does not print non-`CqResult` payloads.

Evidence:
- Plan still uses non-existent helpers: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2254`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2274`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2310`.
- `CliResult` has no success/error helpers: `tools/cq/cli_app/context.py:135`.
- Non-`CqResult` branch exits without rendering output: `tools/cq/cli_app/result.py:125`.

Correction:
- Choose one explicit model and use it consistently:
  1. `ldmd` commands return `int` and write output directly (admin-command style), or
  2. extend `CliResult` + `handle_result` to support text payload rendering for non-`CqResult` results.

---

### P0-2) LDMD parser/writer marker grammar remains internally inconsistent

Problem:
- Parser is now attribute-based (`<!--LDMD:BEGIN id="..."-->`) but writer examples still emit old colon-style markers (`<!-- LDMD:BEGIN:... -->`).
- This means parser and writer won’t round-trip.

Evidence:
- Parser regex expects attribute markers: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2041`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2049`.
- Writer still emits colon markers: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2415`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2431`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2468`.
- Rich LDMD reference is attribute-based: `docs/python_library_reference/rich_md_overview.md:523`, `docs/python_library_reference/rich_md_overview.md:659`.

Correction:
- Standardize all writer output to attribute-based markers (`BEGIN id=...`, `END id=...`).
- Add a parser/writer roundtrip golden test as a required acceptance check.

---

### P0-3) R1 canonical SNB schema still drifts from R5/R6 snippets

Problem:
- R1 defines canonical fields, but downstream snippets still reference fields that do not exist in those canonical types.

Evidence:
- Canonical node fields: `name`, `file_path`, no `symbol/file/line/location`: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:191`.
- Canonical bundle meta fields do not include `elapsed_ms`: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:266`.
- Canonical artifact pointer fields are `byte_size`/`storage_path`, not `size_bytes`/`path`: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:403`.
- Writer still uses non-canonical fields: `bundle.subject.symbol/file/line`, `node.symbol/location`, `art.path/size_bytes`: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2416`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2449`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2475`.

Correction:
- Enforce R1 schema compatibility rule literally for every downstream snippet.
- Add a “snippet compile check” gate (all code blocks in R5-R7 type-check against R1 dataclasses).

---

### P0-4) Pyrefly adapter still references a non-existent payload field

Problem:
- R3 still reads `sg.references`, but `PyreflySymbolGrounding` has no `references` field.

Evidence:
- Plan still uses `sg.references`: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:1372`.
- Actual `PyreflySymbolGrounding` fields: definition/declaration/type_definition/implementation only: `tools/cq/search/pyrefly_contracts.py:73`.
- Reference locations currently live in `local_scope_context.reference_locations`: `tools/cq/search/pyrefly_contracts.py:277`.

Correction:
- Move reference slice derivation to `payload.local_scope_context.reference_locations`.
- Keep `symbol_grounding` strictly for target grounding edges.

---

### P1-1) Capability model still has a client/server split leak

Problem:
- `_capability_present()` currently hard-codes `textDocument/publishDiagnostics` as always true.
- This is still semantically wrong for flows that depend on client-advertised diagnostics features (`dataSupport`, `relatedInformation`, etc.).

Evidence:
- Plan marks publishDiagnostics always true: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:1228`.
- Rust LSP explicitly treats diagnostics options as client capabilities, including `dataSupport`: `docs/python_library_reference/rust_lsp.md:358`, `docs/python_library_reference/rust_lsp.md:364`.

Correction:
- Replace boolean shortcut with explicit `client_caps.publish_diagnostics.*` checks.
- Use a typed split snapshot (`server_caps`, `client_caps`, `experimental_caps`) for all gate decisions.

---

### P1-2) Rust health/quiescence update logic discards actual server health

Problem:
- `_wait_for_quiescence()` sets `workspace_health="ok"` whenever `quiescent=true`, ignoring reported `health` values (`warning`/`error`).

Evidence:
- Plan assignment to `ok` on quiescent: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:939`.
- `serverStatus` health is tri-state and meaningful: `docs/python_library_reference/rust_lsp.md:211`.

Correction:
- Persist and update health from each `experimental/serverStatus` payload independently of quiescence.
- Gate Tier B/C on `(health, quiescent)` policy without collapsing health state.

---

### P1-3) Config fingerprint model is underspecified vs actual LSP config pull model

Problem:
- Plan computes config fingerprint via `_get_workspace_configuration()` but does not model the real protocol direction (`workspace/configuration` server->client requests and `didChangeConfiguration` notifications).

Evidence:
- Plan placeholder for config fingerprint: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:900`.
- Rust LSP config pull model: `docs/python_library_reference/rust_lsp.md:2668`.

Correction:
- Fingerprint should derive from the effective config responses the client serves (and invalidation events), not an out-of-band pull.
- Add explicit persistence for `workspace/configuration` items/results + `workspace/didChangeConfiguration` events.

---

### P2-1) LDMD byte-offset logic still has an edge-case bug on final line and CRLF

Problem:
- `line_byte_len = len(raw_line) + 1` overcounts when file does not end with `\n`.
- CRLF lines may fail strict marker regex unless `\r` is normalized.

Evidence:
- Plan algorithm: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2089`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:2090`.

Correction:
- Compute byte offsets from actual separator boundaries, not fixed `+1` assumption.
- Normalize `\r` before marker matching and add explicit CRLF/no-final-newline tests.

---

### P2-2) Cross-cutting policy sections contain contradictions

Problem:
- The document states both “return None at fail-open boundary” and “probe() never returns None”.
- Health-aware section says Tier B is quiescent-only, while R2 probe says Tier B depends on `health in {ok, warning}`.

Evidence:
- Fail-open `return None` guidance: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:3015`.
- R2 says probe never None: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:992`, `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:1088`.
- Health-aware section says Tier B quiescent-only: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:3061`.

Correction:
- Declare one canonical policy and apply it across all sections:
  - transport/session fatal failure => `None` at outer boundary,
  - non-fatal capability/health degradation => partial typed payload + degrade events.

---

## Additional Optimization Recommendations

1. Capture rich provider payloads, not only boolean provider flags.
- Current `LspCapabilitySnapshotV1` loses `semanticTokensProvider.legend/full/range/delta`, codeAction resolve capability details, etc.: `docs/plans/cq_enhancement_implementation_plan_v1_2026-02-11.md:541`.
- This limits precise gating for advanced planes.

2. Add an explicit `CliTextResult` path (or equivalent) for protocol/tool commands.
- Avoid forcing non-analysis commands into `CqResult` envelopes while still preserving unified CLI output handling.

3. Add protocol-invariant assertions for semantic token decode.
- Rust reference explicitly calls out overlap/multiline and legend/encoding dependence: `docs/python_library_reference/rust_lsp.md:1679`, `docs/python_library_reference/rust_lsp.md:1867`.

---

## Further Scope Expansions for Best-in-Class LSP Leverage

1. Add diagnostics pull plane (`textDocument/diagnostic`, `workspace/diagnostic`) in addition to push diagnostics.
- Rust reference: `docs/python_library_reference/rust_lsp.md:430`.

2. Add full diagnostic action-bridge fidelity fields.
- Persist/propagate `relatedInformation`, `tags`, `codeDescription`, `version`, and opaque `data`.
- Rust reference: `docs/python_library_reference/rust_lsp.md:356`, `docs/python_library_reference/rust_lsp.md:412`.

3. Expand refactor action plane with resolve + execute command semantics.
- Include `codeAction/resolve`, `workspace/executeCommand`, disabled reasons, and snippet text edits.
- Rust reference: `docs/python_library_reference/rust_lsp.md:1322`, `docs/python_library_reference/rust_lsp.md:1381`, `docs/python_library_reference/rust_lsp.md:1411`.
- Pyrefly reference: `docs/python_library_reference/pyrefly_lsp_data.md:4797`.

4. Add dedicated structural discovery plane for pyrefly.
- Document symbols, workspace symbols, and workspaceSymbol/resolve are high-value and should be first-class, not optional stubs.
- Pyrefly reference: `docs/python_library_reference/pyrefly_lsp_data.md:3892`.

5. Add explicit Textual integration contract as a separate UX layer.
- Rich markdown is static; interactive reveal is Textual-only.
- Rich/Textual references: `docs/python_library_reference/rich_md_overview.md:402`, `docs/python_library_reference/rich_md_overview.md:421`.

---

## Final Conclusion

The plan has advanced significantly, but the remaining P0 issues are real implementation blockers and the P1 items materially affect correctness. After those are addressed, the architecture will be in strong shape for execution and for genuinely best-in-class Pyrefly + Rust LSP leverage.
