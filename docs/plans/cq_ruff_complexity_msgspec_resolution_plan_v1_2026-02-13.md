# CQ Ruff/Complexity Hardening Resolution Plan v1 (No-Threshold Relaxation)

## Summary
This plan resolves the current `tools/cq` Ruff backlog **without** changing lint complexity thresholds and without reintroducing `noqa` suppressions.

Current baseline after threshold reversion:
- `54` Ruff errors
- Dominant categories: `C901`, `PLR0912/13/14/15`, `BLE001`, plus a small set of docs/import hygiene findings.

Primary objective: refactor runtime modules to reduce complexity via modular pipelines and shared helpers while increasing msgspec contract discipline and preserving CQ behavior.

## Locked Decisions
1. No lint-threshold changes in `pyproject.toml`.
2. No `noqa` suppressions in `tools/cq` runtime.
3. No compatibility shims; refactors are authoritative cutover.
4. Keep LSP behavior fail-open for command success semantics.
5. Keep diskcache default-on behavior and workspace-scoped backends.

## Design Principles

### 1) Complexity Reduction via Decomposition and Shared Primitives
- Replace large procedural functions with pipeline steps and typed context objects.
- Move repeated logic into shared helper modules.
- Keep command modules orchestration-thin; business logic lives in dedicated modules.

### 2) Msgspec-First Contract Boundaries
- Use `msgspec.Struct` (`CqStruct`) for serializable settings/results crossing module boundaries.
- Keep runtime-only entities (LSP process handles, thread pools, AST nodes, open file handles) out of serializable structs.
- Use centralized boundary serializers only (`to_public_dict`, `contract_to_builtins`) and avoid ad-hoc conversion in hot paths.

### 3) LSP Hardening by Explicit Capability/State Contracts
Based on `pyrefly_lsp_data.md` and `rust_lsp.md`:
- Treat LSP as multi-request protocol (no “super request” assumptions).
- Normalize provider state (`unavailable/skipped/failed/partial/ok`) via shared status derivation.
- Separate transport concerns from enrichment assembly.
- Record server/capability/encoding status deterministically in telemetry/degradation.

### 4) Cache Discipline and Throughput
Based on `diskcache.md`:
- Keep cache operations fail-open and bounded.
- Prefer namespace/versioned key builders and explicit typed cache payload structs.
- Reuse workspace-keyed backend instances and tag-based invalidation for deterministic cleanup.

## Error-to-Workstream Mapping

### Workstream A: Fast Hygiene Fixes (5 errors)
Targets:
- `tests/unit/cq/search/test_parallel_pipeline.py` (`I001`, `PT013`)
- Missing `DOC201` in lightweight wrapper methods

Actions:
1. Normalize imports (`import pytest` + typed `pytest.MonkeyPatch`).
2. Add explicit `Returns:` blocks to wrapper/helper docstrings.

Acceptance:
- No `I001/PT013/DOC201` remains in touched files.

---

### Workstream B: Blind-Except Elimination (`BLE001`) via Shared Error Buckets
Targets:
- `tools/cq/run/runner.py`
- `tools/cq/search/smart_search.py`
- `tools/cq/search/pyrefly_lsp.py`
- `tools/cq/search/rust_lsp.py`
- `tools/cq/neighborhood/bundle_builder.py`

Actions:
1. Introduce shared exception tuples per subsystem:
   - `LSP_NON_FATAL_EXCEPTIONS`
   - `SCAN_NON_FATAL_EXCEPTIONS`
   - `RUN_STEP_NON_FATAL_EXCEPTIONS`
2. Replace `except Exception` with explicit exception families.
3. Route caught failures through typed degrade/telemetry helpers.

Representative pattern:
```python
class LspAttemptErrorV1(CqStruct, frozen=True):
    provider: str
    reason: str
    timed_out: bool = False

LSP_NON_FATAL_EXCEPTIONS = (
    OSError,
    RuntimeError,
    TimeoutError,
    ValueError,
    TypeError,
)

try:
    payload = request_lsp(...)
except LSP_NON_FATAL_EXCEPTIONS as exc:
    error = LspAttemptErrorV1(provider="pyrefly", reason=type(exc).__name__)
    return fail_open(error)
```

Acceptance:
- `BLE001` count is zero.

---

### Workstream C: Front-Door Builder Complexity Cut (`front_door_insight.py`)
Targets:
- `render_insight_card`
- `build_search_insight`
- `build_calls_insight`
- `build_entity_insight`

Actions:
1. Replace high-arg builders with typed request structs:
   - `SearchInsightBuildRequestV1`
   - `CallsInsightBuildRequestV1`
   - `EntityInsightBuildRequestV1`
2. Split card rendering into section renderers:
   - `_render_target_line`
   - `_render_neighborhood_lines`
   - `_render_risk_line`
   - `_render_confidence_line`
   - `_render_degradation_line`
3. Extract defaulting logic (`_default_search_budget`, `_default_calls_budget`, etc.) into shared helpers.

Representative pattern:
```python
class CallsInsightBuildRequestV1(CqStruct, frozen=True):
    function_name: str
    signature: str | None
    location: InsightLocationV1 | None
    neighborhood: InsightNeighborhoodV1
    files_with_calls: int
    arg_shape_count: int
    forwarding_count: int
    hazard_counts: dict[str, int]
    confidence: InsightConfidenceV1
```

Acceptance:
- No `PLR0913/PLR0914` in `front_door_insight.py`.

---

### Workstream D: Calls Path Refactor (`calls.py`, `calls_target.py`)
Targets:
- `tools/cq/macros/calls.py` (`_build_calls_result`)
- `tools/cq/macros/calls_target.py` (`resolve_target_definition`, `scan_target_callees`)

Actions:
1. Introduce calls result assembly context struct:
   - `CallsResultAssemblyContextV1`
2. Split `_build_calls_result` into explicit phases:
   - `_init_calls_result`
   - `_analyze_sites`
   - `_attach_target_metadata_and_sections`
   - `_attach_calls_front_door`
3. In `calls_target.py`, split target resolution and callee extraction into composable helpers:
   - `_resolve_target_sequential`
   - `_resolve_target_parallel`
   - `_find_target_node`
   - `_count_callees_in_node`

Acceptance:
- No `PLR0911/C901/PLR0914/DOC201` in calls modules.

---

### Workstream E: Neighborhood Subsystem Decomposition
Targets:
- `bundle_builder.py`
- `pyrefly_adapter.py`
- `snb_renderer.py`
- `structural_collector.py`
- `target_resolution.py`

Actions:
1. Introduce typed request structs for high-arg functions (`...RequestV1`).
2. Decompose target resolution into pure stages:
   - parse target spec
   - resolve file-position target
   - resolve symbol target
   - tie-breaker + degradation events
3. Split rust slice collection and structural collector into bounded helper pipelines.
4. Centralize neighborhood degrade-note normalization.

Acceptance:
- No `PLR0912/13/14/C901/BLE001` in neighborhood modules.

---

### Workstream F: Query Executor Complexity Cut
Targets:
- `tools/cq/query/executor.py` (`_process_def_query` + doc hygiene)

Actions:
1. Replace `_process_def_query` monolith with staged pipeline:
   - `_resolve_def_candidates`
   - `_compute_relationship_mode`
   - `_build_definition_findings`
   - `_append_optional_sections`
   - `_finalize_definition_summary`
2. Move relationship detail decisions into typed policy struct:
   - `DefQueryRelationshipPolicyV1`

Acceptance:
- No `C901/PLR0914/DOC201` in `query/executor.py`.

---

### Workstream G: LSP Adapter Hardening (Pyrefly + Rust)
Targets:
- `tools/cq/search/pyrefly_lsp.py`
- `tools/cq/search/rust_lsp.py`

Actions:
1. Split large routines (`probe`, `_extract_signature_list`, `_normalize_call_links`) into stage helpers:
   - request preparation
   - capability gating
   - request dispatch
   - payload normalization
   - telemetry/degrade update
2. Consolidate repeated try/catch callsites through one wrapper:
   - `_run_lsp_call_fail_open(...)`
3. Normalize output through msgspec contract structs before summary/public serialization.
4. Ensure rust-analyzer extension surfaces are capability-gated and status-labeled.

Representative pattern:
```python
class LspCallAttemptV1(CqStruct, frozen=True):
    method: str
    attempted: bool
    applied: bool
    timed_out: bool
    reason: str | None = None
```

Acceptance:
- No `C901/PLR0912/PLR0915/PLR0914/BLE001` in pyrefly/rust lsp modules.

---

### Workstream H: Smart Search Assembly Complexity Cut
Targets:
- `tools/cq/search/smart_search.py` (`_assemble_smart_search_result`, BLE callsites)

Actions:
1. Introduce `SearchAssemblyStateV1` struct holding summary, sections, candidates, insight intermediates.
2. Split assembly into dedicated steps:
   - `_assemble_sections`
   - `_assemble_target_candidates`
   - `_assemble_neighborhood`
   - `_assemble_insight`
   - `_assemble_result`
3. Replace blind catches with explicit non-fatal exception tuples.

Acceptance:
- No `PLR0914/BLE001` in `smart_search.py`.

---

## Shared Msgspec Contract Expansion

Add/expand contracts in:
- `tools/cq/core/contracts.py`
- `tools/cq/search/contracts.py`
- `tools/cq/neighborhood/*_contracts.py` (new)
- `tools/cq/macros/calls_contracts.py` (new)
- `tools/cq/query/contracts.py` (expand)

New contract families:
1. Builder request structs for high-arg functions.
2. Stage result structs for pipeline decomposition.
3. LSP call attempt/error telemetry structs.
4. Degradation note structs (serializable) for deterministic merge/reporting.

Boundary rule:
- Only boundary modules serialize to builtins:
  - `core/public_serialization.py`
  - `core/report.py`
  - `core/artifacts.py`
  - `ldmd/writer.py`

## Diskcache and Parallelization Alignment
1. Continue workspace-keyed backend reuse.
2. Keep cache payloads msgspec-typed and schema-versioned.
3. Add typed cache payloads when decomposition introduces new intermediate outputs.
4. Preserve deterministic ordering after parallel execution via stable keys/order arrays.

## File-Level Implementation Queue

1. `tests/unit/cq/search/test_parallel_pipeline.py`
2. `tools/cq/core/front_door_insight.py`
3. `tools/cq/macros/calls_target.py`
4. `tools/cq/macros/calls.py`
5. `tools/cq/neighborhood/target_resolution.py`
6. `tools/cq/neighborhood/structural_collector.py`
7. `tools/cq/neighborhood/bundle_builder.py`
8. `tools/cq/neighborhood/pyrefly_adapter.py`
9. `tools/cq/neighborhood/snb_renderer.py`
10. `tools/cq/query/executor.py`
11. `tools/cq/run/runner.py`
12. `tools/cq/search/pyrefly_lsp.py`
13. `tools/cq/search/rust_lsp.py`
14. `tools/cq/search/smart_search.py`
15. `tools/cq/core/report.py`

## Test and Validation Plan

### Unit
1. Add/refine tests for each decomposed pipeline stage.
2. Add strict tests for fail-open exception routing and degradation notes.
3. Add msgspec contract tests for new request/result structs.
4. Expand LSP state tests for pyrefly/rust parity.

### E2E/Golden
1. Update CQ e2e projections only where section shape changes.
2. Refresh affected CQ golden fixtures after refactors.
3. Verify stable deterministic ordering and front-door payload integrity.

### Final Gate (after all implementation only)
1. `uv run ruff format`
2. `uv run ruff check --fix`
3. `uv run pyrefly check`
4. `uv run pyright`
5. `uv run pytest -q`
6. `uv run pytest tests/e2e/cq --update-golden`
7. `uv run pytest tests/cli_golden --update-golden`
8. Re-run full gate after golden updates.

## Acceptance Criteria
1. Ruff passes with current threshold settings (no threshold relaxation).
2. `tools/cq` runtime contains zero `noqa` suppressions.
3. All listed complexity and blind-exception errors are eliminated by refactor.
4. New shared helpers/contracts reduce duplicated logic in front-door, calls, neighborhood, and LSP flows.
5. msgspec is used for serialized settings/output contracts; runtime-only objects remain outside serializable boundaries.
6. LSP fail-open behavior remains intact with clearer deterministic status/degradation semantics.
