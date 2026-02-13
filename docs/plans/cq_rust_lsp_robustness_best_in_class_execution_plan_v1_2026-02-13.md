# CQ Rust LSP Robustness + Best-in-Class Execution Plan v1 (2026-02-13)

## Summary
This document assesses `tools/cq` Rust LSP integration against `docs/python_library_reference/rust_lsp.md` and defines a hardening roadmap to reach best-in-class reliability and capability utilization under parallel CQ workloads.

Current implementation is directionally strong (capability-gated, fail-open, advanced planes wired, scheduler-backed LSP lane), but it is **not yet fully robust/compliant** for concurrent/shared-session operation and full protocol correctness.

## Scope Reviewed
1. Reference: `docs/python_library_reference/rust_lsp.md`.
2. Runtime implementation:
`tools/cq/search/rust_lsp.py`,
`tools/cq/search/rust_extensions.py`,
`tools/cq/search/lsp_advanced_planes.py`,
`tools/cq/search/diagnostics_pull.py`,
`tools/cq/search/semantic_overlays.py`,
`tools/cq/search/lsp/request_queue.py`,
`tools/cq/search/lsp/session_manager.py`,
`tools/cq/search/lsp_front_door_adapter.py`,
`tools/cq/core/runtime/worker_scheduler.py`,
`tools/cq/search/lsp/capabilities.py`,
`tools/cq/search/lsp/contracts.py`.
3. Relevant tests in `tests/unit/cq/search/*` and `tests/unit/cq/core/test_worker_scheduler.py`.

## Current-State Assessment

### What is already strong
1. Workspace-keyed warm sessions exist (`LspSessionManager`) with restart-on-failure.
2. Tiered probing is health/quiescence-aware (`tier A/B/C` in `rust_lsp.py`).
3. Advanced planes are capability-gated and fail-open.
4. `lsp_request_workers` is operational at adapter scheduling level (`submit_lsp` used in `lsp_front_door_adapter.py`).
5. Rust extensions (macro expansion + runnables) are implemented and normalized.
6. Explicit session environment is persisted (`server`, `health`, `quiescent`, `position_encoding`, capability snapshot).

### Key robustness/compliance gaps

#### P0 (must fix first)
1. **No per-session transport lock in Rust session**.
`_RustLspSession` has no `RLock`; read/write/id/buffer lifecycle is unguarded under concurrent callers.
Risk: framing corruption, response misrouting, startup/shutdown races.

2. **Client does not properly answer server-initiated requests** (`workspace/*/refresh` class).
Current flow treats non-matching messages mostly as notifications and does not send JSON-RPC responses for server requests with `id`.
Risk: protocol deadlocks or server-side request backlog.

3. **Startup race window in shared-root session acquisition**.
`LspSessionManager.for_root()` can call `ensure_started()` concurrently on the same session instance; Rust session has no startup serialization.
Risk: duplicate `_start()` on one object, process/socket state corruption.

#### P1 (high value robustness)
1. **Position encoding not applied end-to-end for request coordinates**.
Rust probe requests use raw CQ `col` as LSP `character`; no conversion utility usage (contrast with pyrefly path).
Risk: wrong anchors for multibyte/UTF-16-sensitive lines.

2. **Single fixed timeout in `_send_request`**.
All methods currently share one timeout default; no per-method budgeting.
Risk: unstable behavior across cheap vs expensive methods (`workspace/diagnostic`, hierarchy, macro ops).

3. **Initialize capability surface is incomplete for refresh-oriented workflow**.
Missing explicit workspace refresh support declarations (`workspace.inlayHint.refreshSupport`, `workspace.semanticTokens.refreshSupport`, `workspace.codeLens.refreshSupport`) and broader capability matrix persistence.
Risk: lower interoperability and weaker refresh semantics.

4. **Advanced-plane collection not explicitly health-gated**.
Planes run even when status is degraded/non-quiescent.
Risk: noisy failures/timeouts and low signal.

#### P2 (best-in-class uplift)
1. **No request pipelining helper (`_request_many`) for Rust session**.
Current pattern is mostly request-at-a-time; compatible but slower.
2. **Degradation taxonomy lacks rich Rust-specific causes**.
Need deterministic reasons: `server_request_unhandled`, `position_encoding_mismatch`, `capability_not_advertised`, `workspace_not_quiescent`, `timeout_method=<x>`.
3. **Limited recovery hooks** for `rust-analyzer/reloadWorkspace` and status dump (`rust-analyzer/analyzerStatus`) in controlled fallback workflows.

## Parallelization Compliance Verdict
1. **Partially compliant** today.
2. Positive: global LSP lane (`submit_lsp`) limits aggregate parallel pressure.
3. Non-compliant risk: intra-session thread safety is not guaranteed in Rust path.
4. Requirement for full compliance: serialize all stdio session transport operations per session and treat parallelism as inter-session/inter-work-item scheduling, not concurrent raw socket access.

## Best-in-Class Target Architecture
1. **Concurrency model**:
one workspace session per root, one lock-serialized transport channel per session, bounded concurrent work across sessions via scheduler.
2. **Protocol model**:
fully support client obligations for server requests and refresh semantics.
3. **Encoding model**:
negotiated `positionEncoding` drives all request/response coordinate conversions.
4. **Capability model**:
single normalized matrix for gating all planes and Rust extensions.
5. **Observability model**:
structured telemetry by method, timeout class, capability gate, and workspace health state.

## Execution Plan

### Workstream 1: Session Transport Safety (P0)
1. Add `threading.RLock` to `_RustLspSession`.
2. Lock-protect: `ensure_started`, `_start`, `_send_request`, `_request`, `_send`, `_wait_for_response`, `_read_message`, `_read_pending_notifications`, `shutdown`, `close`.
3. Add startup idempotency guard to prevent duplicate `_start()` when concurrent callers race.
4. Keep fail-open behavior unchanged at adapter boundary.

Target files:
`tools/cq/search/rust_lsp.py`,
`tools/cq/search/lsp/session_manager.py`.

### Workstream 2: Server-Request Compliance (P0)
1. Detect server requests (`id` + `method`) distinctly from notifications.
2. Handle refresh requests (`workspace/inlayHint/refresh`, `workspace/semanticTokens/refresh`, `workspace/codeLens/refresh`) and send proper JSON-RPC response.
3. Add generic unsupported-method responder (`MethodNotFound` or null result strategy aligned to contract policy).
4. Preserve refresh-event tracking in session env.

Target files:
`tools/cq/search/rust_lsp.py`.

### Workstream 3: Position Encoding Correctness (P1)
1. Reuse `tools/cq/search/lsp/position_encoding.py` in Rust path.
2. Convert CQ column -> LSP character before request emission.
3. Convert returned LSP positions back to CQ semantics in normalization.
4. Persist encoding evidence in telemetry payload.

Target files:
`tools/cq/search/rust_lsp.py`,
`tools/cq/search/rust_lsp_contracts.py`,
`tools/cq/search/lsp/position_encoding.py` (if API expansion needed).

### Workstream 4: Capability Negotiation + Refresh Support (P1)
1. Expand initialize client capabilities to include workspace refresh support and richer document capabilities where supported.
2. Persist full negotiated capability snapshot (raw + normalized fields) used by shared helpers.
3. Align capability helpers with Rust extension surfaces.

Target files:
`tools/cq/search/rust_lsp.py`,
`tools/cq/search/lsp/capabilities.py`,
`tools/cq/search/rust_lsp_contracts.py`.

### Workstream 5: Timeout and Request Budget Hardening (P1)
1. Add optional `timeout_seconds` arg to Rust `_send_request` contract.
2. Apply method-class budgets (cheap/read-heavy/expensive workspace) rather than one static timeout.
3. Add timeout telemetry by method and plane.

Target files:
`tools/cq/search/rust_lsp.py`,
`tools/cq/search/lsp_request_budget.py`,
`tools/cq/search/lsp/contracts.py`.

### Workstream 6: Advanced Plane Robustness + Pipelining (P2)
1. Add `_request_many` in Rust session for keyed, non-batch pipelined requests (send many, collect by id).
2. Use keyed mapping in advanced-plane collection with deterministic handling of partial completion.
3. Gate high-cost planes on `health/quiescent` policy.
4. Keep rust extension fallbacks and fail-open semantics.

Target files:
`tools/cq/search/rust_lsp.py`,
`tools/cq/search/lsp_advanced_planes.py`,
`tools/cq/search/lsp/request_queue.py`,
`tools/cq/search/rust_extensions.py`.

### Workstream 7: Degradation and Telemetry Clarity (P2)
1. Expand deterministic reason taxonomy:
`session_unavailable`,
`request_timeout`,
`unsupported_capability`,
`server_request_unhandled`,
`workspace_not_quiescent`,
`encoding_conversion_error`,
`no_signal`.
2. Standardize Rust reporting with `lsp_contract_state` semantics.

Target files:
`tools/cq/search/rust_lsp.py`,
`tools/cq/search/lsp_contract_state.py`,
`tools/cq/search/smart_search.py` (only where summary wiring is needed).

### Workstream 8: Test Coverage Expansion
1. Add concurrent same-session stress test (multiple concurrent probes against one root/session).
2. Add server-request/refresh response test to verify compliant request replies.
3. Add UTF-16-sensitive coordinate tests for request and normalized output.
4. Add method-budget timeout tests (slow fake server path).
5. Add advanced-plane health gating tests.

Target tests:
`tests/unit/cq/search/test_rust_lsp_session.py`,
`tests/unit/cq/search/test_rust_lsp_integration_transcript.py`,
`tests/unit/cq/search/test_lsp_advanced_planes.py`,
`tests/unit/cq/search/test_lsp_request_queue.py`,
`tests/unit/cq/search/lsp_harness/fake_stdio_lsp_server.py`.

## Decommission / Simplification Opportunities
1. Remove ad-hoc notification-only handling paths that ignore server requests.
2. Remove Rust-only coordinate shortcuts once shared encoding utilities are used.
3. Consolidate duplicate capability probing logic behind shared LSP contract helpers.

## Acceptance Criteria
1. Parallel probes against a shared Rust workspace session are deterministic and transport-safe.
2. Rust session correctly responds to server-initiated refresh requests.
3. Request/response coordinates are correct under UTF-8/UTF-16 negotiation.
4. Advanced planes populate when supported and degrade with explicit reason codes when not.
5. Timeout behavior is method-budgeted and telemetry-rich.
6. No behavior regression to core CQ fail-open guarantees.

## Recommended Sequencing
1. Workstream 1
2. Workstream 2
3. Workstream 3
4. Workstream 4
5. Workstream 5
6. Workstream 6
7. Workstream 7
8. Workstream 8

## Conclusion
CQ’s Rust LSP integration is solidly structured but not yet best-in-class for concurrent, protocol-strict operation. The most important gap is session transport safety + server-request compliance; once closed, the remaining work is largely capability depth, encoding correctness, and telemetry precision. This plan reaches a robust, standards-compliant, and high-signal Rust LSP implementation without changing CQ’s fail-open user contract.
