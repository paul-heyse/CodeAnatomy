# CQ Front-Door Enrichment Embedding Plan v3 (Front-Door Contract + Artifact-First Diagnostics)

**Status:** Proposed
**Date:** 2026-02-12
**Builds on:** `docs/plans/cq_enrichment_embedding_plan_v2.md`
**Inputs reviewed:** `docs/plans/cq_enrichment_embedding_plan_v2.md`, `docs/plans/cq_more_upfront_enrichment.md`, CQ architecture docs, `tools/cq` implementation, and live `./cq` command runs.

---

## 1. Executive Conclusion

v2 is directionally correct and should remain the baseline. The highest-ROI path is to keep v2's core idea (embed neighborhood/risk into front-door commands) but tighten it around four concrete alterations:

1. **Make a single front-door contract mandatory for `search`, `calls`, and `q entity`** so agents always see the same high-signal block first.
2. **Reuse existing CQ structures and telemetry paths** (summary/sections/key_findings, SNB preview model, existing enrichment payloads) instead of introducing parallel output channels.
3. **Move diagnostics out of in-band markdown by renderer policy** (not per-command ad hoc), while preserving full fidelity in JSON/LDMD artifacts.
4. **Prioritize free and cheap enrichments first** (scan-state structural signals), then bounded LSP enrichments only for top targets.

This gives stronger single-call orientation for LLM agents and lowers follow-up query pressure without introducing material latency risk.

---

## 2. Review Findings vs Current CQ Capabilities

### 2.1 What v2 gets right and should be kept

- Front-door focus on `search`, `calls`, and `entity` is correct.
- “Target identity + neighborhood + risk” is the right information shape for agents.
- SNB-style progressive disclosure (`totals + preview + artifact pointer`) is the right output discipline.
- Budgeted enrichment and fail-open behavior should remain mandatory.

### 2.2 Gaps in v2 that should be corrected

- v2 under-specifies a **single cross-command output contract**. It proposes ideas per command, but agents need one consistent top block.
- v2 proposes new keys/models where existing pipelines can already carry the data. This increases surface area and maintenance cost.
- v2 does not fully address that diagnostics currently leak into the markdown summary renderer globally.
- v2 does not make cross-language parity explicit; sparse Rust enrichments can otherwise produce unstable top-of-output semantics.

### 2.3 Observed current-state behavior that drives this v3

From implementation and command output review:

- `search` already computes substantial enrichment, but high-signal facts are diluted by long per-finding context blocks.
- `calls` provides strong callsite evidence but lacks immediate target dependency context (callee-side neighborhood).
- `entity` has enough in-memory scan state to add mini-neighborhood counts cheaply, but those counts are not promoted as front-door signals.
- Diagnostic metadata is still emitted in-band in rendered summary sections; this burns tokens that agents should spend on code insight.

---

## 3. v3 Design Principles

1. **Front-load decisions, not evidence.** The first screen answers: what target, what neighborhood, what risk.
2. **Additive compatibility only.** Existing keys/sections remain; new fields are additive.
3. **Single contract, three commands.** Same top schema for `search`, `calls`, and `entity`.
4. **Cost-tiered enrichment.**
   - Tier 0: free from existing scan state.
   - Tier 1: cheap bounded file-level extraction.
   - Tier 2: bounded LSP enrichment (top N only).
5. **Artifact-first diagnostics.** In-band diagnostics are compact status lines; detail goes to artifact.
6. **Stable cross-language semantics.** Keys are always present with explicit `availability`/`source` markers.

---

## 4. Alterations to v2 (Normative Deltas)

### Delta A: Replace command-specific cards with one **FrontDoorInsightV1** contract

Use one shared contract for all front-door commands instead of slightly different per-command card shapes.

Required top-level fields:

- `target`
- `neighborhood`
- `risk`
- `confidence`
- `degradation`
- `budget`
- `artifact_refs`

This contract is rendered as the first block in markdown and emitted in `summary` for JSON/LDMD.

### Delta B: Do not add parallel output lanes

Do not create competing summary namespaces. Use existing CQ result envelope with one additive sub-key:

- `summary.front_door_insight`

Retain `key_findings` as headline strings derived from the same object.

### Delta C: Move diagnostics to artifacts by renderer policy

Move detailed diagnostics out of markdown summary in `report`/LDMD rendering policy, not only in command logic.

In-band keep:

- one-line enrichment status
- one-line scope/filter status
- one-line degradation status

Artifact-only:

- full telemetry payloads
- parse quality details
- full capability matrices
- full per-stage timing and cache stats
- full pyrefly diagnostic lists

### Delta D: Reuse neighborhood collector with adapter, not duplicate logic

If structural neighborhood logic is reused across commands, introduce a lightweight adapter from query/calls scan structures to the `ScanSnapshot` shape expected by structural collectors.

### Delta E: Explicit cross-language contract behavior

Define `availability` semantics so Rust or degraded contexts still emit stable keys:

- `availability: full | partial | unavailable`
- `source: structural | lsp | heuristic | none`

No missing keys in front-door contract.

### Delta F: Add context-efficiency objective to success criteria

Primary KPI is not only latency but also **follow-up query reduction** and **output token efficiency**.

---

## 5. FrontDoorInsightV1 (Proposed High-Level Schema)

```text
front_door_insight:
  target:
    symbol
    kind
    location
    signature
    qualname
    selection_reason
  neighborhood:
    callers: { total, preview[], availability, source }
    callees: { total, preview[], availability, source }
    references: { total, preview[], availability, source }
    hierarchy_or_scope: { total, preview[], availability, source }
  risk:
    level
    drivers[]
    counters:
      callers
      callees
      files_with_calls
      arg_shape_count
      forwarding_count
      hazard_count
      closure_capture_count
  confidence:
    evidence_kind
    score
    bucket
  degradation:
    lsp
    scan
    scope_filter
    notes[]
  budget:
    top_candidates
    preview_per_slice
    lsp_targets
  artifact_refs:
    diagnostics
    neighborhood_overflow
    telemetry
```

Rendering rule:

- Markdown first section: `## Insight Card` from `front_door_insight`.
- JSON/LDMD: full structured object under `summary.front_door_insight`.

---

## 6. Command-Specific Embedding Plan

### 6.1 `search` (identifier-mode front door)

Primary objective: pick a likely definition and immediately show its neighborhood/risk.

Embed order:

1. `Insight Card` (for selected target).
2. `Target Candidates` (top 3 definitions only).
3. `Neighborhood Preview` (totals + bounded previews).
4. `Risk` headline.
5. Existing evidence sections (`Top Contexts`, `Definitions`, `Imports`, `Callsites`, etc.).

Data strategy:

- Use current enrichment payload first (structural + pyrefly when available).
- Backfill missing counts via scan-state structural adapter.
- Do not run extra expensive scans for all matches.

Budget defaults:

- candidates: 3
- preview per slice: 5
- optional LSP target count: 1 (top target only)

### 6.2 `calls` (refactor-surface front door)

Primary objective: combine incoming call surface with target dependency context.

Embed order:

1. `Insight Card` (target + call surface).
2. `Target Callees Preview` (what target depends on).
3. Existing `Argument Shape Histogram`.
4. Existing `Hazards` promoted above long callsite list.
5. Existing `Calling Contexts` and `Call Sites`.

Data strategy:

- Reuse existing calls census and hazard analysis.
- Add bounded callee extraction for the resolved target definition body (cheap path).
- Optional single LSP enrichment for typed signature/call graph if available.

Budget defaults:

- target candidates: up to 3 when ambiguous
- callee preview: 10
- optional LSP target count: 1

### 6.3 `q entity=...` (semantic selection front door)

Primary objective: enrich each returned definition with mini-neighborhood counts by default.

Embed order:

1. `Insight Card` for top result (or aggregate card for multi-result).
2. Result list with per-result mini-neighborhood counters.
3. Expansion sections (`callers`, `callees`, etc.) only when requested by query options.

Data strategy:

- Always compute counts from existing in-memory scan structures.
- Only top 3 results eligible for optional LSP augmentation.
- Preserve existing `fields`/`expand` semantics; no hidden heavy expansions.

Budget defaults:

- enriched top results: 3
- mini-neighborhood counts: all returned definitions
- optional LSP target count: 3

---

## 7. Artifact-First Diagnostics Policy (Global)

### 7.1 In-band markdown policy

Keep only concise status lines in-band:

- `Enrichment: applied/skipped/degraded`
- `Scope: dropped_by_scope=<n>` when relevant
- `Pyrefly: attempted/applied/failed`

### 7.2 Artifact policy

Persist detailed diagnostics in artifacts and reference them from top-level `artifact_refs`:

- telemetry and stage timings
- parser quality details
- diagnostics arrays
- language capability detail
- overflow neighborhood slices

### 7.3 LDMD policy alignment

LDMD top summary should match markdown compactness; full diagnostics remain retrievable through LDMD retrieval paths, not eagerly printed in top blocks.

---

## 8. Implementation Roadmap (High Level)

### Batch 1: Contract + Rendering + Reordering (highest ROI)

Scope:

- Introduce `front_door_insight` object and renderer support.
- Reorder front-door sections to put insight first.
- Add compact diagnostic status lines.
- Keep existing evidence sections intact.

Expected impact:

- Immediate improvement in first-screen signal density.
- Minimal compute change.

### Batch 2: Structural mini-neighborhood everywhere

Scope:

- Add shared adapter to build structural neighborhood slices from existing scan state.
- Populate neighborhood counters for search/calls/entity with bounded previews.
- Add per-result mini-neighborhood counters in entity.

Expected impact:

- Major single-call orientation lift without meaningful latency risk.

### Batch 3: Bounded LSP augmentation

Scope:

- `calls`: optional single-target LSP augmentation.
- `entity`: optional top-3 LSP augmentation.
- unified degradation reporting into `front_door_insight.degradation`.

Expected impact:

- Better typed and call-graph confidence for top targets.
- Controlled cost via strict N caps/timeouts.

### Batch 4: Artifact-first hardening + LDMD alignment

Scope:

- Ensure markdown/LDMD compact summaries do not dump full diagnostics.
- Route all heavy diagnostic detail to artifacts with stable refs.
- Add tests for compact rendering invariants.

Expected impact:

- Lower context consumption and better agent focus.

---

## 9. Validation and Success Metrics

### 9.1 Functional acceptance

- `search`, `calls`, and `entity` always emit `Insight Card` first when a target exists.
- `summary.front_door_insight` present and schema-stable for all three commands.
- No removals of existing public keys/sections.

### 9.2 Efficiency acceptance

- p95 latency guardrails:
  - `search`: +<= 200ms
  - `calls`: +<= 500ms
  - `entity`: +<= 150ms
- output size guardrail:
  - markdown tokens in top block <= fixed budget (define and enforce in tests)

### 9.3 Agent effectiveness acceptance

- Reduce median follow-up command count after initial front-door command by >= 25% in benchmark tasks.
- Increase “single-call orientation success” to >= 80% for definition-centric tasks.

### 9.4 Reliability acceptance

- Degraded or LSP-unavailable runs still emit complete `front_door_insight` with explicit `availability` and `source` fields.
- Rust scope runs keep schema stability with partial/unavailable markings rather than omitted fields.

---

## 10. Testing and Rollout Strategy

1. Add/update golden tests for compact top-of-output structure for `search`, `calls`, `entity`.
2. Add JSON schema assertions for `summary.front_door_insight` presence and required fields.
3. Add regression tests for diagnostics offloading (no full telemetry blobs in markdown summary).
4. Roll out behind a feature flag for one iteration if needed, then flip default once golden baseline is stable.

---

## 11. Risks and Mitigations

- **Risk:** contract drift across commands.
  - **Mitigation:** one shared builder and one shared renderer path.
- **Risk:** hidden compute growth.
  - **Mitigation:** explicit budget counters emitted in `front_door_insight.budget` and CI guardrails.
- **Risk:** diagnostic data loss perception.
  - **Mitigation:** explicit artifact refs in insight card and stable JSON/LDMD access paths.
- **Risk:** parity issues in Rust scope.
  - **Mitigation:** mandatory availability/source markers for every neighborhood slice.

---

## 12. Recommended Execution Order

1. Implement Batch 1 first and ship.
2. Implement Batch 2 with strict preview budgets.
3. Add Batch 3 LSP augmentation only after measuring Batch 1/2 impact.
4. Complete Batch 4 renderer/artifact hardening and lock golden outputs.

This sequencing delivers immediate agent UX gains while minimizing refactor risk and preserving backward compatibility.
