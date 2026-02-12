# CQ LSP + msgspec Hardening Execution Plan v1 (Design-Phase Hard Cutover)

**Status:** Proposed  
**Date:** 2026-02-12  
**Scope:** `tools/cq` Python implementation, renderers, artifacts, and CQ unit/e2e goldens  
**Builds on:** `docs/plans/cq_enrichment_embedding_plan_v3_front_door_contract_2026-02-12.md`  

## 1. Executive Summary

This plan hardens CQ's Python and Rust LSP integration and increases msgspec leverage so the `search`, `calls`, and `q entity` front door is both reliable and contract-stable under degraded runtime conditions.

It resolves the observed gaps:

1. Pyrefly "applied" status can be overstated when payloads contain no actionable semantic signal.
2. Rust LSP availability is healthy in neighborhood flows but not consistently reflected in front-door insight for `search`/`calls`/`q entity`.
3. Advanced LSP planes exist but are not systematically wired into top-of-output contracts.
4. LSP status semantics are inconsistent and too coarse for deterministic downstream behavior.
5. CQ hot paths still over-convert typed payloads to builtins instead of preserving msgspec contracts internally.

Hard-cutover policy for this plan:

1. No compatibility shims for old LSP status semantics.
2. No feature flags; contract changes are applied directly.
3. No legacy diagnostics payload rendering in-band; full diagnostics stay artifact-first.

---

## 2. Target Architecture

### 2.1 Unified LSP Contract-State Model

```python
from typing import Literal

from tools.cq.core.structs import CqStruct

LspProvider = Literal["pyrefly", "rust_analyzer", "none"]
LspStatus = Literal["unavailable", "skipped", "failed", "partial", "ok"]


class LspContractStateV1(CqStruct, frozen=True):
    provider: LspProvider = "none"
    available: bool = False
    attempted: int = 0
    applied: int = 0
    failed: int = 0
    timed_out: int = 0
    status: LspStatus = "unavailable"
    reasons: tuple[str, ...] = ()
```

### 2.2 Typed-Internal / Boundary-Only Builtins

```python
def write_front_door_summary(result: CqResult, insight: FrontDoorInsightV1) -> None:
    # Keep typed object in orchestration code; serialize only at boundary.
    result.summary["front_door_insight"] = to_public_front_door_insight_dict(insight)
```

### 2.3 Signal-Driven LSP Application

```python
def has_meaningful_pyrefly_signal(payload: PyreflyEnrichmentPayload) -> bool:
    return bool(
        payload.symbol_grounding.definition_targets
        or payload.symbol_grounding.declaration_targets
        or payload.call_graph.incoming_total
        or payload.call_graph.outgoing_total
        or payload.local_scope_context.reference_locations
        or payload.anchor_diagnostics
    )
```

---

## 3. Workstreams

## Workstream 1: Shared LSP Contract-State and Degradation Semantics

### Scope

Create one canonical status derivation path consumed by `search`, `calls`, `q entity`, and merge/orchestration flows.

### Representative Code Snippets

```python
def derive_lsp_contract_state(
    *,
    provider: LspProvider,
    available: bool,
    attempted: int,
    applied: int,
    failed: int,
    timed_out: int = 0,
    reasons: tuple[str, ...] = (),
) -> LspContractStateV1:
    if not available:
        return LspContractStateV1(provider=provider, available=False, status="unavailable", reasons=reasons)
    if attempted <= 0:
        return LspContractStateV1(provider=provider, available=True, status="skipped", reasons=reasons)
    if applied <= 0:
        return LspContractStateV1(provider=provider, available=True, attempted=attempted, failed=failed, timed_out=timed_out, status="failed", reasons=reasons)
    status = "ok" if failed == 0 and applied >= attempted else "partial"
    return LspContractStateV1(
        provider=provider,
        available=True,
        attempted=attempted,
        applied=applied,
        failed=failed,
        timed_out=timed_out,
        status=status,
        reasons=reasons,
    )
```

```python
insight = msgspec.structs.replace(
    insight,
    degradation=msgspec.structs.replace(
        insight.degradation,
        lsp=lsp_state.status,
        notes=tuple(dict.fromkeys([*insight.degradation.notes, *lsp_state.reasons])),
    ),
)
```

### Target File Edits

1. `tools/cq/core/front_door_insight.py`
2. `tools/cq/search/smart_search.py`
3. `tools/cq/macros/calls.py`
4. `tools/cq/query/executor.py`
5. `tools/cq/core/multilang_orchestrator.py`

### New Files

1. `tools/cq/search/lsp_contract_state.py`
2. `tests/unit/cq/search/test_lsp_contract_state.py`

### Decommission and Deletes

1. Remove `derive_lsp_status` from `tools/cq/core/front_door_insight.py` after callers migrate.
2. Remove `_python_lsp_available` from `tools/cq/core/front_door_insight.py`.
3. Remove command-local ad hoc `lsp_available/lsp_attempted/lsp_applied` status branching once centralized.

### Implementation Checklist

- [ ] Add `LspContractStateV1` and deterministic derivation helper.
- [ ] Thread state through front-door builders for `search`, `calls`, `entity`.
- [ ] Ensure merged auto-scope results preserve canonical state.
- [ ] Update compact summary status rendering to use canonical state.

---

## Workstream 2: LSP Session Lifecycle Hardening (Pyrefly + Rust)

### Scope

Harden startup and request behavior (timeouts, retries, health capture, fail-open reasoning) while preserving non-blocking CQ command execution.

### Representative Code Snippets

```python
class LspRequestBudgetV1(CqStruct, frozen=True):
    startup_timeout_seconds: float = 3.0
    probe_timeout_seconds: float = 1.0
    max_attempts: int = 2
    retry_backoff_ms: int = 100
```

```python
def _request_with_budget(request_fn: Callable[[str, object], object], method: str, params: object, budget: LspRequestBudgetV1) -> object | None:
    for attempt in range(budget.max_attempts):
        try:
            return request_fn(method, params)
        except TimeoutError:
            if attempt + 1 == budget.max_attempts:
                return None
            time.sleep((budget.retry_backoff_ms / 1000.0) * (attempt + 1))
        except Exception:
            return None
    return None
```

```python
# Pipeline requests over one warm session (no JSON-RPC batch).
request_ids = {
    "hover": self._request("textDocument/hover", tdp),
    "definition": self._request("textDocument/definition", tdp),
    "references": self._request("textDocument/references", refs_params),
}
responses = self._collect_responses(request_ids, timeout_seconds=budget.probe_timeout_seconds)
```

### Target File Edits

1. `tools/cq/search/pyrefly_lsp.py`
2. `tools/cq/search/rust_lsp.py`
3. `tools/cq/search/rust_lsp_contracts.py`
4. `tools/cq/search/diagnostics_pull.py`

### New Files

1. `tools/cq/search/lsp_request_budget.py`
2. `tests/unit/cq/search/test_lsp_session_recovery.py`

### Decommission and Deletes

1. Replace per-module single constants (`_DEFAULT_TIMEOUT_SECONDS`) with profile-based budgets where possible.
2. Delete duplicated restart/recreate session blocks after shared helper adoption.

### Implementation Checklist

- [ ] Add request budget profiles for `search`, `calls`, and `entity`.
- [ ] Add bounded retry for timeout-only errors, still fail-open.
- [ ] Capture structured failure reasons (`startup_failed`, `request_timeout`, `request_failed`).
- [ ] Ensure Pyrefly and Rust sessions expose consistent telemetry counters.

---

## Workstream 3: Pyrefly Signal Correctness and Telemetry Integrity

### Scope

Make Pyrefly "applied" status reflect real semantic value, not payload truthiness.

### Representative Code Snippets

```python
def _compute_pyrefly_signal(payload: PyreflyEnrichmentPayload) -> tuple[bool, tuple[str, ...]]:
    reasons: list[str] = []
    if not payload.symbol_grounding.definition_targets and not payload.symbol_grounding.declaration_targets:
        reasons.append("no_grounding")
    if payload.call_graph.incoming_total <= 0 and payload.call_graph.outgoing_total <= 0:
        reasons.append("no_call_graph")
    if not payload.local_scope_context.reference_locations and not payload.anchor_diagnostics:
        reasons.append("no_local_context")
    return (len(reasons) < 3, tuple(reasons))
```

```python
typed = coerce_pyrefly_payload(payload)
is_applied, reasons = _compute_pyrefly_signal(typed)
telemetry["attempted"] += 1
if is_applied:
    telemetry["applied"] += 1
else:
    telemetry["failed"] += 1
    diagnostics.append({"code": "PYREFLY002", "severity": "info", "reason": ",".join(reasons)})
```

### Target File Edits

1. `tools/cq/search/pyrefly_lsp.py`
2. `tools/cq/search/pyrefly_contracts.py`
3. `tools/cq/search/smart_search.py`
4. `tools/cq/core/front_door_insight.py`

### New Files

1. `tools/cq/search/pyrefly_signal.py`
2. `tests/unit/cq/search/test_pyrefly_signal.py`

### Decommission and Deletes

1. Delete `_coverage_reason` from `tools/cq/search/pyrefly_lsp.py`.
2. Delete dict-truthiness success checks in `_merge_match_with_pyrefly_payload` in `tools/cq/search/smart_search.py`.
3. Delete "applied by default unless explicitly not_resolved" logic from Pyrefly payload assembly.

### Implementation Checklist

- [ ] Add typed signal evaluator on top of `PyreflyEnrichmentPayload`.
- [ ] Gate "applied" telemetry increments on signal evaluator.
- [ ] Emit deterministic reason notes for non-applied attempts.
- [ ] Update front-door degradation notes to consume these reasons.

---

## Workstream 4: Rust LSP Front-Door Parity for `search`, `calls`, and `q entity`

### Scope

Wire Rust LSP enrichment into all front-door command paths with explicit availability/source semantics, matching Python path behavior.

### Representative Code Snippets

```python
def enrich_with_language_lsp(
    *,
    language: QueryLanguage,
    root: Path,
    file_path: Path,
    line: int,
    col: int,
    symbol_hint: str | None,
) -> dict[str, object] | None:
    if language == "python":
        return enrich_with_pyrefly_lsp(PyreflyLspRequest(root=root, file_path=file_path, line=line, col=col, symbol_hint=symbol_hint))
    if language == "rust":
        return enrich_with_rust_lsp(
            RustLspRequest(file_path=str(file_path), line=max(0, line - 1), col=max(0, col), query_intent="symbol_grounding"),
            root=root,
        )
    return None
```

```python
insight = augment_insight_with_lsp(insight, payload, preview_per_slice=5)
insight = msgspec.structs.replace(
    insight,
    neighborhood=msgspec.structs.replace(
        insight.neighborhood,
        callers=msgspec.structs.replace(insight.neighborhood.callers, source="lsp"),
        callees=msgspec.structs.replace(insight.neighborhood.callees, source="lsp"),
    ),
)
```

### Target File Edits

1. `tools/cq/search/smart_search.py`
2. `tools/cq/macros/calls.py`
3. `tools/cq/query/executor.py`
4. `tools/cq/core/front_door_insight.py`
5. `tools/cq/core/multilang_orchestrator.py`
6. `tools/cq/search/rust_lsp.py`

### New Files

1. `tools/cq/search/lsp_front_door_adapter.py`
2. `tests/unit/cq/search/test_lsp_front_door_adapter.py`

### Decommission and Deletes

1. Remove Python-only LSP availability checks from front-door degradation derivation.
2. Remove Rust-insight preservation fallback hacks in `tools/cq/macros/calls.py` once unified merge behavior is authoritative.
3. Remove suffix-only (`.py`, `.pyi`) gating where command is language-agnostic.

### Implementation Checklist

- [ ] Route top-target LSP enrichment by selected target language.
- [ ] Ensure Rust front-door insight sets `degradation.lsp != unavailable` when Rust LSP is available.
- [ ] Ensure merged auto-scope insight preserves partial markers when one language is missing.
- [ ] Add counters for attempted/applied/failed for Rust parity with Pyrefly.

---

## Workstream 5: Activate Advanced LSP Planes with Artifact-First Diagnostics

### Scope

Promote dormant advanced planes (semantic tokens, inlay hints, pull diagnostics, rust extensions) into bounded front-door enrichments and artifact-only detail payloads.

### Representative Code Snippets

```python
semantic_tokens = fetch_semantic_tokens_range(session, uri, start_line=max(0, line - 5), end_line=line + 5)
inlay_hints = fetch_inlay_hints_range(session, uri, start_line=max(0, line - 5), end_line=line + 5)
doc_diags = pull_text_document_diagnostics(session, uri=uri)
ws_diags = pull_workspace_diagnostics(session)
```

```python
macro = expand_macro(session, uri=uri, line=max(0, line - 1), col=max(0, col))
runnables = get_runnables(session, uri=uri)
advanced_payload = {
    "semantic_tokens_count": len(semantic_tokens or ()),
    "inlay_hints_count": len(inlay_hints or ()),
    "workspace_diagnostics_count": len(ws_diags or ()),
    "macro_expansion_available": macro is not None,
    "runnables_count": len(runnables),
}
```

### Target File Edits

1. `tools/cq/search/pyrefly_lsp.py`
2. `tools/cq/search/rust_lsp.py`
3. `tools/cq/search/diagnostics_pull.py`
4. `tools/cq/search/semantic_overlays.py`
5. `tools/cq/search/refactor_actions.py`
6. `tools/cq/search/rust_extensions.py`
7. `tools/cq/core/artifacts.py`
8. `tools/cq/cli_app/result.py`
9. `tools/cq/core/report.py`
10. `tools/cq/ldmd/writer.py`

### New Files

1. `tools/cq/search/lsp_advanced_planes.py`
2. `tests/unit/cq/search/test_lsp_advanced_planes.py`

### Decommission and Deletes

1. Delete `tools/cq/search/pyrefly_expansion.py` after equivalent advanced-plane wiring is in `lsp_advanced_planes.py`.
2. Delete `tools/cq/search/pyrefly_capability_gates.py` after capabilities are unified under shared gating.
3. Delete method-support fallbacks that always return true for unsupported methods in `tools/cq/search/diagnostics_pull.py`.

### Implementation Checklist

- [ ] Wire bounded semantic overlays (top target only for `search`/`calls`, top 3 for `entity`).
- [ ] Wire diagnostic pull paths with true capability checks.
- [ ] Wire Rust macro/runnable extension output as compact counters only.
- [ ] Save full advanced-plane payloads to diagnostics artifacts; keep markdown/LDMD compact.

---

## Workstream 6: msgspec Utilization Uplift and Boundary Discipline

### Scope

Increase typed-contract persistence in CQ hot paths and remove repeated typed<->builtins churn.

### Representative Code Snippets

```python
_FRONT_DOOR_DECODER = msgspec.json.Decoder(type=FrontDoorInsightV1, strict=True)


def decode_front_door_json(payload: bytes) -> FrontDoorInsightV1:
    return _FRONT_DOOR_DECODER.decode(payload)
```

```python
def write_diagnostics_artifact(contract: DiagnosticsArtifactV1, path: Path) -> None:
    encoder = msgspec.json.Encoder(order="deterministic")
    path.write_bytes(msgspec.json.format(encoder.encode(contract), indent=2))
```

```python
# Hot-path boundary rule:
# 1) typed contracts in orchestration
# 2) to_builtins only in renderer/artifact boundary function
summary_payload = to_public_front_door_insight_dict(insight)
```

### Target File Edits

1. `tools/cq/core/front_door_insight.py`
2. `tools/cq/core/serialization.py`
3. `tools/cq/core/contracts.py`
4. `tools/cq/search/enrichment/core.py`
5. `tools/cq/search/smart_search.py`
6. `tools/cq/query/executor.py`
7. `tools/cq/macros/calls.py`
8. `tools/cq/core/artifacts.py`
9. `tools/cq/core/report.py`

### New Files

1. `tools/cq/core/diagnostics_contracts.py`
2. `tests/unit/cq/core/test_diagnostics_contracts.py`

### Decommission and Deletes

1. Remove repeated per-call `msgspec.to_builtins(...)` conversions in hot loops where typed contracts are sufficient.
2. Remove ad hoc `dict[str, object]` normalization code paths once typed contracts exist.
3. Decommission mutable struct-only intermediates in touched CQ modules in favor of `CqStruct` where practical.

### Implementation Checklist

- [ ] Add typed diagnostics artifact contract structs.
- [ ] Migrate enrichment summaries to typed intermediate contracts.
- [ ] Restrict builtins conversion to rendering and artifact write boundaries.
- [ ] Add profiling notes for payload encode/decode hot paths.

---

## Workstream 7: Regression Harness Expansion (Unit + E2E Golden)

### Scope

Lock behavior with focused unit tests and expanded goldens on existing fixture datasets.

### Representative Code Snippets

```python
def _project_front_door_insight(summary: dict[str, object]) -> dict[str, Any] | None:
    insight = coerce_front_door_insight(summary.get("front_door_insight"))
    if insight is None:
        return None
    return {
        "source": insight.source,
        "target": {"symbol": insight.target.symbol, "kind": insight.target.kind},
        "degradation": {"lsp": insight.degradation.lsp, "notes": list(insight.degradation.notes)},
        "artifact_ref_presence": {"diagnostics": bool(insight.artifact_refs.diagnostics)},
    }
```

```bash
uv run pytest tests/e2e/cq --update-golden
uv run pytest tests/cli_golden --update-golden
```

### Target File Edits

1. `tests/unit/cq/search/test_pyrefly_lsp.py`
2. `tests/unit/cq/search/test_rust_lsp_session.py`
3. `tests/unit/cq/search/test_smart_search.py`
4. `tests/unit/cq/macros/test_calls.py`
5. `tests/unit/cq/test_executor_sections.py`
6. `tests/unit/cq/test_run_merge.py`
7. `tests/unit/cq/core/test_front_door_insight.py`
8. `tests/unit/cq/test_report.py`
9. `tests/unit/cq/ldmd/test_writer.py`
10. `tests/e2e/cq/_support/projections.py`
11. `tests/e2e/cq/test_search_command_e2e.py`
12. `tests/e2e/cq/test_q_command_e2e.py`
13. `tests/e2e/cq/test_chain_command_e2e.py`
14. `tests/e2e/cq/test_ldmd_command_e2e.py`
15. Existing golden snapshots under `tests/e2e/cq/_fixtures/` and `tests/cli_golden/`

### New Files

1. `tests/unit/cq/search/test_lsp_contract_state.py`
2. `tests/unit/cq/search/test_lsp_front_door_adapter.py`
3. `tests/unit/cq/search/test_pyrefly_signal.py`
4. `tests/unit/cq/search/test_lsp_advanced_planes.py`

### Decommission and Deletes

1. Delete or rewrite tests that assert legacy Pyrefly applied semantics based on payload truthiness.
2. Delete or rewrite tests expecting Rust front-door `degradation.lsp="unavailable"` in healthy Rust LSP scenarios.
3. Delete stale golden assertions that expect raw diagnostics payload in markdown/LDMD output.

### Implementation Checklist

- [ ] Expand projection contract to include canonical LSP status and availability fields.
- [ ] Add unit regression tests for all five canonical statuses.
- [ ] Add E2E regression for Rust front-door parity and auto-scope partial semantics.
- [ ] Regenerate goldens and commit updated projections/snapshots.

---

## 4. Global File Plan

### High-Probability Edits (Core)

1. `tools/cq/core/front_door_insight.py`
2. `tools/cq/search/smart_search.py`
3. `tools/cq/macros/calls.py`
4. `tools/cq/query/executor.py`
5. `tools/cq/core/multilang_orchestrator.py`
6. `tools/cq/search/pyrefly_lsp.py`
7. `tools/cq/search/rust_lsp.py`
8. `tools/cq/search/diagnostics_pull.py`
9. `tools/cq/core/report.py`
10. `tools/cq/ldmd/writer.py`
11. `tools/cq/core/artifacts.py`
12. `tools/cq/cli_app/result.py`
13. `tools/cq/search/enrichment/core.py`

### Planned New Files

1. `tools/cq/search/lsp_contract_state.py`
2. `tools/cq/search/lsp_request_budget.py`
3. `tools/cq/search/pyrefly_signal.py`
4. `tools/cq/search/lsp_front_door_adapter.py`
5. `tools/cq/search/lsp_advanced_planes.py`
6. `tools/cq/core/diagnostics_contracts.py`

---

## 5. Decommission Matrix (Post-Migration Deletes)

### Modules to Delete

1. `tools/cq/search/pyrefly_expansion.py`
2. `tools/cq/search/pyrefly_capability_gates.py`

### Functions/Paths to Remove

1. `tools/cq/core/front_door_insight.py`: `derive_lsp_status`, `_python_lsp_available`
2. `tools/cq/search/pyrefly_lsp.py`: `_coverage_reason` and related applied/not_resolved shortcut logic
3. `tools/cq/search/smart_search.py`: dict-truthiness applied counting in `_merge_match_with_pyrefly_payload`
4. `tools/cq/search/diagnostics_pull.py`: capability fallback returning true for `workspace/diagnostic` without negotiation evidence
5. `tools/cq/macros/calls.py`: stale insight preservation fallback path after merged insight handling is canonical

---

## 6. Execution Sequence

1. Implement Workstream 1 (shared LSP state contract) first.
2. Implement Workstream 2 (session lifecycle hardening) second.
3. Implement Workstream 3 (Pyrefly signal correctness) third.
4. Implement Workstream 4 (Rust front-door parity) fourth.
5. Implement Workstream 5 (advanced plane wiring + artifacts) fifth.
6. Implement Workstream 6 (msgspec boundary discipline) sixth.
7. Implement Workstream 7 (tests + goldens), then delete decommissioned modules.

---

## 7. Acceptance Criteria

1. Pyrefly telemetry marks `applied` only when meaningful semantic signal exists.
2. Rust `search`/`calls`/`q entity` front-door insight reflects Rust LSP availability and attempt outcomes.
3. `degradation.lsp` status is derived from one canonical function and supports `unavailable/skipped/failed/partial/ok`.
4. Markdown and LDMD show compact diagnostics statuses and artifact refs only (no raw payload dumps).
5. `summary["front_door_insight"]` remains explicit and fully populated by the public serializer.
6. Advanced LSP plane outputs are bounded and artifact-backed.
7. Updated CQ unit/e2e/golden suites pass for touched areas.

---

## 8. Quality Gate (After Implementation)

```bash
uv run ruff format && uv run ruff check --fix && uv run pyrefly check && uv run pyright && uv run pytest -q
```

