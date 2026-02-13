# CQ Pyrefly LSP Robustness Gap-Closure Execution Plan v1 (Concurrency-Safe + Protocol-Compliant)

**Status:** Proposed  
**Date:** 2026-02-13  
**Scope:** `tools/cq` pyrefly LSP integration, shared LSP plane plumbing, CQ runtime scheduling and telemetry, targeted CQ unit/integration coverage

## 1. Executive Summary

This plan resolves the remaining pyrefly LSP robustness gaps in CQ by enforcing the documented integration model in `docs/python_library_reference/pyrefly_lsp_data.md`:

1. One warm workspace session.
2. Standards-compliant pipelined requests (no JSON-RPC batch).
3. Capability-gated feature usage.
4. Negotiated position-encoding correctness.
5. Deterministic fail-open degradation reporting.

Primary outcome: pyrefly-backed enrichment remains fail-open for command success semantics, while becoming deterministic, thread-safe, and observably correct under concurrent CQ workloads.

---

## 2. Baseline Gaps To Close

1. Shared-session stdio request/response lifecycle is vulnerable to concurrency races without strict serialization boundaries.
2. Advanced-plane request contract shape is inconsistent across pyrefly/rust clients, causing wiring drift and partial plane loss.
3. Advanced-plane assembly still relies on positional result assumptions instead of deterministic keyed mapping.
4. Column handling is not uniformly tied to negotiated `positionEncoding`, causing span drift in multibyte cases.
5. `lsp_request_workers` policy exists but is not consistently enforced for all LSP-bound execution paths.
6. Degradation semantics are too coarse (`failed/skipped`) and not explicit enough for operator and agent decision-making.
7. Regression coverage does not fully protect session safety, keyed timeout behavior, or position-encoding correctness.

---

## 3. Locked Decisions

1. Hard cutover for internal pyrefly plumbing. No compatibility shim for old request-shape assumptions.
2. No JSON-RPC batching.
3. Keep fail-open behavior for `search`, `calls`, and `q entity`.
4. Keep workspace-keyed warm session model; serialize intra-session transport operations.
5. Treat `lsp_request_workers` as an enforced runtime limit, not metadata.
6. Run quality checks only at end stage after all implementation workstreams are complete.

---

## 4. Design Rules (from `pyrefly_lsp_data.md`)

1. Session discipline: one warm server per workspace, many requests pipelined through that session.
2. Capability discipline: only call methods that server capabilities advertise.
3. Encoding discipline: honor negotiated encoding from initialize result (`utf-8` preferred, fallback `utf-16`).
4. Plane discipline: each plane is independently fail-open and must not corrupt other plane outputs.
5. Output discipline: expose explicit degradation reason taxonomy to avoid ambiguous "no data" interpretation.

---

## 5. Public Interface and Contract Changes

1. Add shared LSP request protocol in `tools/cq/search/lsp/contracts.py`.
2. Add pyrefly session methods:
   - `_send_request(method, params, timeout_seconds=...) -> object`
   - `_request_many(requests, timeout_seconds=...) -> dict[str, object]`
3. Switch advanced-plane aggregation to named-key mapping (no index-coupled contract).
4. Add position encoding utilities in `tools/cq/search/lsp/position_encoding.py`.
5. Extend worker scheduler with `submit_lsp(...)` bounded by `policy.lsp_request_workers`.
6. Standardize degradation reasons across pyrefly paths:
   - `unsupported_capability`
   - `request_interface_unavailable`
   - `session_unavailable`
   - `timeout`
   - `no_signal`

Representative contract snippet:

```python
class LspRequestClient(Protocol):
    def _send_request(
        self,
        method: str,
        params: dict[str, object],
        *,
        timeout_seconds: float | None = None,
    ) -> object: ...

    def capabilities_snapshot(self) -> dict[str, object]: ...
```

---

## 6. Workstreams

## Workstream 1: Session Concurrency Safety

### Scope

Serialize pyrefly stdio lifecycle and transport operations with reentrant lock protection and safe manager startup/restart semantics.

### Representative code pattern

```python
with self._lock:
    request_id = self._request(method, params)
    response = self._wait_for_response(request_id, timeout_seconds=timeout_seconds)
```

### Target file edits

1. `tools/cq/search/pyrefly_lsp.py`
2. `tools/cq/search/lsp/session_manager.py`

### Decommission/delete

1. Any session-access path that can call request/read/close without lock serialization.
2. Startup flow that holds manager lock while blocking on subprocess initialization.

### Implementation checklist

- [ ] Add/verify per-session `RLock` around request/read/notify/close flow.
- [ ] Ensure ID generation and response collection are lock-serialized.
- [ ] Harden manager `for_root` startup/restart race handling.
- [ ] Keep fail-open exception boundaries unchanged.

---

## Workstream 2: Protocol-Unified Request/Capability Wiring

### Scope

Unify request-callable and capability snapshot discovery across pyrefly/rust clients.

### Representative code pattern

```python
request_fn = resolve_request_callable(session)
caps = resolve_capabilities_snapshot(session)
if request_fn is None:
    return {}
```

### Target file edits

1. `tools/cq/search/lsp/contracts.py` (new)
2. `tools/cq/search/pyrefly_lsp.py`
3. `tools/cq/search/diagnostics_pull.py`
4. `tools/cq/search/semantic_overlays.py`

### Decommission/delete

1. Pyrefly-incompatible ad hoc request discovery logic.
2. Duplicated capability probing branches that ignore `capabilities_snapshot`.

### Implementation checklist

- [ ] Land shared protocol + helpers.
- [ ] Wire diagnostics and semantic overlays through shared helpers.
- [ ] Preserve rust fallback behavior.
- [ ] Validate pyrefly advanced planes use the same request contract path.

---

## Workstream 3: Keyed Advanced-Plane Request Queue

### Scope

Replace positional/index-driven advanced-plane request handling with keyed deterministic mapping and explicit timeout-key reporting.

### Representative code pattern

```python
result = run_lsp_requests(
    callables={
        "semantic_tokens": lambda: ...,
        "inlay_hints": lambda: ...,
        "document_diagnostics": lambda: ...,
    },
    timeout_seconds=timeout_seconds,
)
```

### Target file edits

1. `tools/cq/search/lsp/request_queue.py`
2. `tools/cq/search/lsp_advanced_planes.py`
3. `tools/cq/search/pyrefly_lsp.py`

### Decommission/delete

1. Index-coupled constants and positional unpacking for advanced-plane results.
2. Implicit timeout behavior that loses method identity.

### Implementation checklist

- [ ] Return keyed result map plus timed-out key list.
- [ ] Make sequential execution default for single-session safety.
- [ ] Keep optional worker-backed mode for non-session-bound requests.
- [ ] Emit timeout method names into advanced-plane telemetry.

---

## Workstream 4: Position Encoding Correctness

### Scope

Apply negotiated position encoding for outbound request coordinates and normalize inbound coordinates back to CQ semantics.

### Representative code pattern

```python
character = to_lsp_character(
    line_text=line_text,
    column=request.col,
    encoding=self._position_encoding,
)
```

### Target file edits

1. `tools/cq/search/lsp/position_encoding.py` (new)
2. `tools/cq/search/pyrefly_lsp.py`
3. `tools/cq/search/pyrefly_contracts.py` (if payload contract update is needed)

### Decommission/delete

1. Direct column passthrough as LSP character without encoding conversion.
2. Response normalizers that assume UTF-16 or UTF-8 unconditionally.

### Implementation checklist

- [ ] Read and store negotiated `capabilities.positionEncoding`.
- [ ] Convert CQ column to LSP character before request send.
- [ ] Convert LSP response character to CQ column in normalized targets/diagnostics.
- [ ] Keep encoding in coverage metadata for observability.

---

## Workstream 5: Initialization + Capability Negotiation Hardening

### Scope

Align initialize capability declarations with pyrefly-supported advanced planes and robust negotiation flow.

### Representative code pattern

```python
"general": {"positionEncodings": ["utf-8", "utf-16"]},
"textDocument": {
    "inlayHint": {"dynamicRegistration": False},
    "semanticTokens": {"requests": {"range": True, "full": True}},
    "diagnostic": {"dynamicRegistration": False},
},
```

### Target file edits

1. `tools/cq/search/pyrefly_lsp.py`
2. `tools/cq/search/lsp/capabilities.py`

### Decommission/delete

1. Capability checks that treat missing provider detail as implicit support.
2. Divergent method support checks across pyrefly and shared helpers.

### Implementation checklist

- [ ] Use UTF-8-first encoding preference order.
- [ ] Ensure diagnostics/inlay/semantic token capabilities are declared.
- [ ] Normalize capability snapshot shape.
- [ ] Gate probe and advanced-plane calls with shared `supports_method`.

---

## Workstream 6: Make `lsp_request_workers` Operational

### Scope

Enforce LSP-specific bounded concurrency at runtime and route LSP enrichment through that lane.

### Representative code pattern

```python
future = scheduler.submit_lsp(_run_enrichment)
payload = future.result(timeout=timeout_seconds)
```

### Target file edits

1. `tools/cq/core/runtime/worker_scheduler.py`
2. `tools/cq/search/lsp_front_door_adapter.py`
3. `tools/cq/search/lsp/request_queue.py`

### Decommission/delete

1. LSP call paths that run on generic IO pool without LSP-specific backpressure.
2. Dead policy fields unused in execution logic.

### Implementation checklist

- [ ] Add LSP semaphore bounded by `lsp_request_workers`.
- [ ] Route pyrefly/rust LSP front-door calls through `submit_lsp`.
- [ ] Keep non-LSP IO work on generic IO pool.
- [ ] Add deterministic concurrency-limit tests.

---

## Workstream 7: Degradation and Telemetry Semantics

### Scope

Make degradation reasons explicit and consistent for pyrefly request lifecycle outcomes.

### Representative code pattern

```python
if not supports_required:
    reason = "unsupported_capability"
elif payload is None:
    reason = "session_unavailable"
elif timed_out:
    reason = "timeout"
```

### Target file edits

1. `tools/cq/search/pyrefly_lsp.py`
2. `tools/cq/search/smart_search.py`
3. `tools/cq/search/lsp_contract_state.py` (if shared mapping expansion is needed)

### Decommission/delete

1. Coarse or ambiguous reason strings that collapse distinct failure modes.
2. Logic that treats all not-applied outcomes as identical.

### Implementation checklist

- [ ] Emit explicit reasons in coverage/degradation fields.
- [ ] Preserve compact summary shape while improving diagnostic precision.
- [ ] Ensure reason mapping is deterministic across prefetch and in-place attempts.

---

## Workstream 8: Regression Test Expansion

### Scope

Add focused tests to lock session safety, keyed request behavior, encoding correctness, and runtime limit enforcement.

### Target test edits/additions

1. `tests/unit/cq/search/test_pyrefly_lsp.py`
2. `tests/unit/cq/search/test_pyrefly_lsp_integration_transcript.py`
3. `tests/unit/cq/search/test_lsp_advanced_planes.py`
4. `tests/unit/cq/search/test_diagnostics_pull.py`
5. `tests/unit/cq/search/test_semantic_overlays.py`
6. `tests/unit/cq/search/test_lsp_request_queue.py` (new)
7. `tests/unit/cq/search/test_position_encoding.py` (new)
8. `tests/unit/cq/core/test_worker_scheduler.py` (new/expand)
9. `tests/unit/cq/search/lsp_harness/fake_stdio_lsp_server.py`

### Decommission/delete

1. Tests that only validate positional list semantics for advanced planes.
2. Tests that do not assert method-level timeout identity.

### Implementation checklist

- [ ] Add concurrent shared-session stability test.
- [ ] Assert advanced planes are non-empty when fake server advertises capability.
- [ ] Assert keyed timeout behavior preserves non-timed-out planes.
- [ ] Add UTF-16-sensitive multibyte coordinate roundtrip coverage.
- [ ] Verify `lsp_request_workers=1` serializes active LSP tasks.

---

## Workstream 9: Documentation Alignment

### Scope

Update CQ architecture and README docs to match the hardened pyrefly runtime model.

### Target docs

1. `docs/architecture/cq/01_core_infrastructure.md`
2. `docs/architecture/cq/02_search_subsystem.md`
3. `tools/cq/README.md`

### Implementation checklist

- [ ] Document single-session serialization rule.
- [ ] Document no-batch pipelined request rule.
- [ ] Document position encoding negotiation and conversion rules.
- [ ] Document LSP lane concurrency control and degradation reason taxonomy.

---

## 7. End-to-End Execution Sequence

1. Workstream 1 (session safety).
2. Workstream 2 and 3 together (request contract + keyed planes).
3. Workstream 4 (position encoding correctness).
4. Workstream 5 and 6 (capability negotiation + runtime lane enforcement).
5. Workstream 7 (degradation semantics).
6. Workstream 8 (tests/regressions).
7. Workstream 9 (docs).
8. Plan-vs-code audit.
9. End-stage quality checks.

---

## 8. Test Scenarios

1. Single-anchor pyrefly probe returns non-null payload with coverage metadata.
2. Advertised advanced planes return semantic/inlay/diagnostic signals in transcript harness.
3. Parallel same-workspace probes do not corrupt framing or randomly drop responses.
4. Timeout in one keyed plane does not null out other planes.
5. UTF-16 multibyte request columns map correctly to returned normalized columns.
6. Capability-absent path yields `unsupported_capability` without exceptions.
7. Request interface missing path yields `request_interface_unavailable`.
8. `lsp_request_workers=1` enforces serialized LSP execution.

---

## 9. Acceptance Criteria

1. Pyrefly enrichment is deterministic under parallel same-root load.
2. Advanced planes are wired and populated when server supports methods.
3. No JSON-RPC batch usage is introduced.
4. Position encoding negotiation is honored end-to-end for request and response mapping.
5. `lsp_request_workers` is enforced in runtime LSP entrypoints.
6. Degradation/coverage semantics distinguish unsupported vs interface-missing vs session-unavailable vs timeout vs no-signal.
7. Targeted CQ unit/integration coverage for pyrefly hardening passes.

---

## 10. Quality Freeze and End-Stage Validation

Do not run lint/type/test quality assessments during intermediate implementation.

Run only after all workstreams are complete and audited:

1. `uv run ruff format`
2. `uv run ruff check --fix`
3. `uv run pyrefly check`
4. `uv run pyright`
5. `uv run pytest tests/unit/cq tests/e2e/cq -q`

---

## 11. Assumptions and Defaults

1. Internal API changes are acceptable in this design-phase branch.
2. Fail-open behavior remains mandatory for CQ command success semantics.
3. Session serialization is preferred over intra-session thread parallelism.
4. UTF-8 preference is used during negotiation with fallback to server-selected encoding.
5. This scope does not redesign rust-analyzer internals beyond shared-helper compatibility.
