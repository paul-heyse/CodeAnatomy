# CQ Design Improvements Implementation Plan v1 (2026-02-17)

## Scope Summary

This plan synthesizes findings from 7 deep design reviews covering all ~82K LOC / 390 files in `tools/cq/`. It addresses 6 systemic themes confirmed across every review scope: (1) frozen struct integrity breaches, (2) dependency direction violations, (3) knowledge duplication, (4) God module concentration, (5) the `dict[str, object]` payload anti-pattern (913 occurrences across 140 files), and (6) module-level mutable singletons. The plan is organized as 12 scope items ordered by dependency and risk, progressing from safe structural fixes through module decomposition to the large-scale typed payload migration.

**Design stance:** Incremental migration with no compatibility shims. Each scope item produces a working codebase. No `# deprecated` stubs or re-export facades for removed code.

## Design Principles

1. **Frozen by default**: All `msgspec.Struct` data-transfer types must use `frozen=True` unless they are intentional accumulators (e.g., `TaintState`).
2. **Copy-on-write, not mutation**: Use `msgspec.structs.replace()` for all struct updates. Never bypass frozen with `object.__setattr__`.
3. **Parse at the boundary, flow typed data**: Convert `dict[str, object]` to typed structs at enrichment boundaries; downstream code receives typed structs.
4. **Dependency direction**: Lower layers never import from higher layers. Shared contracts live in `_shared/` or `core/`.
5. **No facade chains**: If a module only re-exports, eliminate it and update callers to import from the source.
6. **Injectable context over module singletons**: Mutable runtime state lives in explicit context objects, not module-level globals.

## Current Baseline

- `Section` at `schema.py:290` is mutable while its container `CqResult` at `schema.py:362` is `frozen=True` — violates immutability invariant.
- `report.py:540-543` directly assigns to `result.key_findings`, `result.evidence`, and `section.findings` on frozen structs — bypasses frozen contract.
- 8 unfrozen data-transfer structs in `macros/` (`BytecodeSurface`, `RaiseSite`, `CatchSite`, `SideEffect`, `ImportInfo`, `ModuleDeps`, `ScopeInfo`, plus `TaintState` which is intentionally mutable).
- `core/services.py` has 12+ imports from `search/`, `macros/`, `query/`, and `orchestration/` — core depends on higher layers.
- `enrichment_contracts.py` lives in `pipeline/` but is imported by `enrichment/` adapters — layer inversion.
- `sgpy_scanner.py:452` imports from `query/executor_ast_grep.py` — astgrep depends on query layer.
- `_extract_provenance` duplicated in 3 files, `_coerce_count` in 2 files, `_pack_source_rows` in 3 files, scope filter in 2 files, schema projection in 11 pairs, file-scan-visit in 6 macros, status derivation in 7 copies.
- 913 `dict[str, object]` occurrences across 140 files. Typed fact structs (`PythonEnrichmentFacts`, `RustEnrichmentFacts`) exist in `enrichment/python_facts.py` and `enrichment/rust_facts.py` but are not threaded through the pipeline.
- God modules: `runtime_core.py` (1,209 LOC), `extractors.py` (1,795 LOC), `executor_runtime_impl.py` (1,001 LOC), `calls/analysis.py` (771 LOC), `front_door_assembly.py` (769 LOC), `executor_definitions.py` (715 LOC), `tree_sitter_collector.py` (661 LOC), `summary_contract.py` (663 LOC), `report.py` (614 LOC).
- Module-level mutable singletons: `_SESSIONS`, `_GLOBAL_STATE_HOLDER`, `_TREE_CACHE`, `_AST_CACHE` (2 copies), `console`, `error_console`, `app.config`, `_SEARCH_OBJECT_VIEW_REGISTRY`, `_LANGUAGE_ADAPTERS`, `_TELEMETRY`, `_SEEN_KEYS`, `_SCHEDULER_STATE`, `_BACKEND_STATE`.

---

## S1. Frozen Struct Integrity

### Goal

Restore the frozen contract invariant across the entire `tools/cq/` codebase by making all data-transfer structs frozen and replacing the `object.__setattr__` bypass in `report.py` with copy-on-write.

### Representative Code Snippets

```python
# tools/cq/core/schema.py — Make Section frozen, tuple findings
class Section(msgspec.Struct, frozen=True, kw_only=True):
    """A logical grouping of findings with a heading."""
    title: str
    findings: tuple[Finding, ...] = ()
    collapsed: bool = False
```

```python
# tools/cq/core/report.py — Replace in-place mutation with copy-on-write
def _apply_render_enrichment(
    result: CqResult,
    *,
    root: Path,
    cache: dict[tuple[str, int, int, str], dict[str, object]],
    allowed_files: set[str] | None,
    port: RenderEnrichmentPort | None,
) -> CqResult:
    """Return a new CqResult with render enrichment applied to findings."""
    def _apply(finding: Finding) -> Finding:
        return _maybe_attach_render_enrichment_orchestrator(
            finding, root=root, cache=cache, allowed_files=allowed_files, port=port,
        )

    new_key_findings = tuple(_apply(f) for f in result.key_findings)
    new_evidence = tuple(_apply(f) for f in result.evidence)
    new_sections = tuple(
        msgspec.structs.replace(s, findings=tuple(_apply(f) for f in s.findings))
        for s in result.sections
    )
    return msgspec.structs.replace(
        result,
        key_findings=new_key_findings,
        evidence=new_evidence,
        sections=new_sections,
    )
```

```python
# tools/cq/macros/bytecode.py — Freeze data-transfer struct
class BytecodeSurface(msgspec.Struct, frozen=True, kw_only=True):
    """Bytecode analysis surface for a single code object."""
    file: str
    function: str
    ...
```

### Files to Edit

- `tools/cq/core/schema.py` — Make `Section` frozen; change `findings: list[Finding]` to `findings: tuple[Finding, ...]`
- `tools/cq/core/report.py` — Replace `_apply_render_enrichment_in_place` with pure `_apply_render_enrichment` returning new `CqResult`; update `render_markdown` caller
- `tools/cq/macros/bytecode.py` — Add `frozen=True` to `BytecodeSurface`
- `tools/cq/macros/exceptions.py` — Add `frozen=True` to `RaiseSite` and `CatchSite`
- `tools/cq/macros/side_effects.py` — Add `frozen=True` to `SideEffect`
- `tools/cq/macros/imports.py` — Add `frozen=True` to `ImportInfo`; refactor `ModuleDeps` construction to pass `depends_on` as constructor arg
- `tools/cq/macros/scopes.py` — Add `frozen=True` to `ScopeInfo`
- `tools/cq/search/python/analysis_session.py` — Type `Any` fields on `PythonAnalysisSession` with proper types under `TYPE_CHECKING`

### New Files to Create

- `tests/unit/cq/core/test_section_frozen.py` — Verify `Section` is frozen and `findings` is a tuple

### Legacy Decommission/Delete Scope

- Delete `_apply_render_enrichment_in_place` from `tools/cq/core/report.py` — superseded by the pure `_apply_render_enrichment` function.

---

## S2. Dependency Direction Corrections

### Goal

Eliminate all dependency direction violations: core importing from higher layers (12+ sites), enrichment importing from pipeline, and astgrep importing from query.

### Representative Code Snippets

```python
# tools/cq/search/_shared/enrichment_contracts.py — Relocated from pipeline/
# (exact same content, just moved to the shared foundation layer)
class RustTreeSitterEnrichmentV1(msgspec.Struct, frozen=True, omit_defaults=True, forbid_unknown_fields=True):
    schema_version: int = 1
    payload: dict[str, object] = msgspec.field(default_factory=dict)
...
```

```python
# tools/cq/core/services.py — Replace deferred imports with port-based DI
class EntityService:
    """Adapter implementing EntityServicePort."""

    def __init__(self, attach_fn: Callable[..., None]) -> None:
        self._attach_fn = attach_fn

    def attach_front_door_insight(self, ...) -> None:
        self._attach_fn(...)
```

```python
# tools/cq/astgrep/metavar_extract.py — Moved from query/executor_ast_grep_impl.py
def extract_match_metavars(
    node: SgNode,
    rule: Rule,
) -> dict[str, str]:
    """Extract metavariable bindings from an ast-grep match."""
    ...
```

### Files to Edit

- `tools/cq/search/pipeline/enrichment_contracts.py` — Delete (relocated)
- `tools/cq/search/_shared/enrichment_contracts.py` — New location for enrichment contracts
- `tools/cq/search/enrichment/python_adapter.py` — Update imports to `_shared.enrichment_contracts`
- `tools/cq/search/enrichment/rust_adapter.py` — Update imports to `_shared.enrichment_contracts`
- `tools/cq/search/pipeline/classification.py` — Update imports to `_shared.enrichment_contracts`
- `tools/cq/search/pipeline/smart_search_sections.py` — Update imports
- `tools/cq/search/pipeline/smart_search_telemetry.py` — Update imports
- `tools/cq/core/services.py` — Replace deferred imports with DI-based adapters; move `CallsRequest` to `core/contracts.py`
- `tools/cq/core/contracts.py` — Add `CallsRequest` and `SearchSummaryContractProtocol`
- `tools/cq/core/bootstrap.py` — Wire concrete implementations at composition root
- `tools/cq/core/settings_factory.py` — Replace deferred import from `tree_sitter.core.infrastructure`
- `tools/cq/core/render_context.py` — Replace deferred import from `orchestration`
- `tools/cq/core/scoring.py` — Replace TYPE_CHECKING import from `macros.contracts`
- `tools/cq/astgrep/sgpy_scanner.py` — Update import from `astgrep/metavar_extract` instead of `query/executor_ast_grep`
- `tools/cq/query/executor_ast_grep_impl.py` — Remove `extract_match_metavars` (moved to astgrep)

### New Files to Create

- `tools/cq/search/_shared/enrichment_contracts.py` — Relocated from `pipeline/`
- `tools/cq/astgrep/metavar_extract.py` — Metavariable extraction utilities (moved from query)
- `tests/unit/cq/astgrep/test_metavar_extract.py` — Tests for relocated metavar extraction

### Legacy Decommission/Delete Scope

- Delete `tools/cq/search/pipeline/enrichment_contracts.py` — relocated to `_shared/`
- Delete `extract_match_metavars` from `tools/cq/query/executor_ast_grep_impl.py` — moved to `astgrep/metavar_extract.py`
- Remove all deferred imports from `tools/cq/core/services.py` that reach into `macros/`, `query/`, `search/`, and `orchestration/`

---

## S3. DRY Consolidation — Small Wins

### Goal

Eliminate verbatim code duplication and trivial knowledge duplication across the codebase. Each item is a small, independent change.

### Representative Code Snippets

```python
# tools/cq/search/_shared/helpers.py — Consolidated coercion helpers
def coerce_count(value: object) -> int:
    """Safely coerce a value to an integer count, returning 0 for non-coercible values."""
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def safe_int_counter(value: object) -> int:
    """Coerce to int, excluding booleans. For telemetry counter accumulation."""
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return value
    return 0
```

```python
# tools/cq/search/tree_sitter/core/language_registry.py — Single canonical _extract_provenance
# (Remove duplicates from query/planner.py and schema/node_schema.py, import from here)
```

```python
# tools/cq/core/scope_filter.py — Consolidated scope filtering
def filter_findings_by_scope(
    result: CqResult,
    *,
    root: Path,
    in_dir: str | None = None,
    exclude: tuple[str, ...] = (),
) -> CqResult:
    """Filter a CqResult's findings to those within a directory scope."""
    ...
```

### Files to Edit

- `tools/cq/search/_shared/helpers.py` — Add `coerce_count` and `safe_int_counter`
- `tools/cq/search/enrichment/incremental_compound_plane.py` — Replace local `_coerce_count` with import from `_shared.helpers`
- `tools/cq/search/enrichment/incremental_dis_plane.py` — Replace local `_coerce_count` with import from `_shared.helpers`
- `tools/cq/search/pipeline/smart_search_telemetry.py` — Replace local `int_counter` with import from `_shared.helpers`
- `tools/cq/search/enrichment/rust_adapter.py` — Replace local `_counter` with import from `_shared.helpers`
- `tools/cq/search/tree_sitter/query/planner.py` — Remove local `_extract_provenance`; import from `core/language_registry.py`
- `tools/cq/search/tree_sitter/schema/node_schema.py` — Remove local `_extract_provenance`; import from `core/language_registry.py`
- `tools/cq/search/tree_sitter/structural/export.py` — Remove `_build_node_id` duplicate
- `tools/cq/search/tree_sitter/structural/exports.py` — Import `build_node_id` from `export.py`
- `tools/cq/search/python/analysis_session.py` — Remove `_iter_nodes_with_parents`; import from `ast_utils.py`
- `tools/cq/search/python/resolution_support.py` — Remove `_iter_nodes_with_parents`; import from `ast_utils.py`
- `tools/cq/search/python/extractors.py` — Remove `_MAX_PARENT_DEPTH`; import from shared constant
- `tools/cq/search/python/extractors_structure.py` — Remove `_MAX_PARENT_DEPTH`; import from shared constant
- `tools/cq/search/tree_sitter/python_lane/fallback_support.py` — Rename exported functions to drop underscore prefix
- `tools/cq/core/render_summary.py` — Replace 7 `_derive_*_status()` functions with generic `_derive_status_from_summary()`
- `tools/cq/query/symbol_resolver.py` — Replace hardcoded `_is_builtin` set with `name in dir(builtins)`
- `tools/cq/cli_app/schema_projection.py` — Replace 11 function pairs with 2 generic functions

### New Files to Create

- `tools/cq/core/scope_filter.py` — Consolidated scope filtering logic
- `tests/unit/cq/core/test_scope_filter.py` — Tests for scope filter

### Legacy Decommission/Delete Scope

- Delete `_coerce_count` from `tools/cq/search/enrichment/incremental_compound_plane.py:8-16`
- Delete `_coerce_count` from `tools/cq/search/enrichment/incremental_dis_plane.py:23-30`
- Delete `_counter` from `tools/cq/search/enrichment/rust_adapter.py`
- Delete `int_counter` from `tools/cq/search/pipeline/smart_search_telemetry.py`
- Delete `_extract_provenance` from `tools/cq/search/tree_sitter/query/planner.py:22-40`
- Delete `_extract_provenance` from `tools/cq/search/tree_sitter/schema/node_schema.py:92-109`
- Delete `_build_node_id` from `tools/cq/search/tree_sitter/structural/exports.py:23-35`
- Delete `_iter_nodes_with_parents` from `tools/cq/search/python/analysis_session.py:42-50`
- Delete `_iter_nodes_with_parents` from `tools/cq/search/python/resolution_support.py:62-68`
- Delete `_is_builtin` 50-line hardcoded set from `tools/cq/query/symbol_resolver.py:358-438`
- Delete 9 of 11 `project_*_params` / `*_options_from_projected_params` pairs from `tools/cq/cli_app/schema_projection.py` (replaced by 2 generics)
- Delete `_apply_run_scope_filter` from `tools/cq/run/step_executors.py:383-424` — use `core/scope_filter.py`
- Delete `filter_result_by_scope` from `tools/cq/orchestration/bundles.py` — use `core/scope_filter.py`
- Delete 7 `_derive_*_status()` functions from `tools/cq/core/render_summary.py` — replaced by generic

---

## S4. Telemetry Accumulation Consolidation

### Goal

Consolidate the 8 duplicated telemetry accumulation functions across 4 modules into a single authority in `enrichment/telemetry.py`.

### Representative Code Snippets

```python
# tools/cq/search/enrichment/telemetry.py — New canonical telemetry module
from __future__ import annotations

from tools.cq.search._shared.helpers import coerce_count, safe_int_counter

__all__ = [
    "accumulate_stage_status",
    "accumulate_stage_timings",
    "accumulate_runtime_flags",
    "accumulate_rust_bundle_drift",
]


def accumulate_stage_status(
    stages_bucket: dict[str, object],
    stage_status: dict[str, object],
) -> None:
    """Accumulate stage status counters from a single enrichment payload."""
    for stage_name, status in stage_status.items():
        if not isinstance(status, str):
            continue
        if status in {"applied", "degraded", "skipped"}:
            key = f"{stage_name}_{status}"
            stages_bucket[key] = coerce_count(stages_bucket.get(key, 0)) + 1


def accumulate_stage_timings(
    timings_bucket: dict[str, object],
    stage_timings_ms: dict[str, object],
) -> None:
    """Accumulate stage timing totals from a single enrichment payload."""
    for stage_name, elapsed_ms in stage_timings_ms.items():
        if isinstance(elapsed_ms, (int, float)):
            current = timings_bucket.get(stage_name, 0.0)
            if isinstance(current, (int, float)):
                timings_bucket[stage_name] = current + elapsed_ms
```

### Files to Edit

- `tools/cq/search/enrichment/python_adapter.py` — Remove local `_accumulate_stage_status`, `_accumulate_stage_timings`; delegate to `telemetry.py`
- `tools/cq/search/enrichment/rust_adapter.py` — Remove local `_accumulate_stage_timings`, `_accumulate_bundle_drift`, `_counter`; delegate to `telemetry.py`
- `tools/cq/search/pipeline/smart_search_telemetry.py` — Remove `accumulate_stage_status`, `accumulate_stage_timings`, `accumulate_rust_bundle`, `accumulate_rust_runtime`, `int_counter`; delegate to `enrichment/telemetry.py`

### New Files to Create

- `tools/cq/search/enrichment/telemetry.py` — Canonical telemetry accumulation helpers
- `tests/unit/cq/search/enrichment/test_telemetry.py` — Tests for consolidated accumulation

### Legacy Decommission/Delete Scope

- Delete `_accumulate_stage_status` from `tools/cq/search/enrichment/python_adapter.py:93-105`
- Delete `_accumulate_stage_timings` from `tools/cq/search/enrichment/python_adapter.py:108-117`
- Delete `_accumulate_stage_timings` from `tools/cq/search/enrichment/rust_adapter.py:119-138`
- Delete `_accumulate_bundle_drift` from `tools/cq/search/enrichment/rust_adapter.py:93-116`
- Delete `accumulate_stage_status` from `tools/cq/search/pipeline/smart_search_telemetry.py:101-118`
- Delete `accumulate_stage_timings` from `tools/cq/search/pipeline/smart_search_telemetry.py:121-138`
- Delete `accumulate_rust_bundle` from `tools/cq/search/pipeline/smart_search_telemetry.py:255-295`

---

## S5. CQS Violations and Public Surface Cleanup

### Goal

Fix the most impactful command-query separation violations and clean up public API surfaces that export private names or create naming confusion.

### Representative Code Snippets

```python
# tools/cq/query/finding_builders.py — Pure function replacing void mutator
def build_call_evidence(
    evidence: dict[str, object],
    call_target: str,
) -> dict[str, object]:
    """Build call evidence fields from enrichment data. Returns a new dict."""
    result: dict[str, object] = {}
    result["resolved_globals"] = evidence.get("resolved_globals")
    result["bytecode_calls"] = evidence.get("bytecode_calls")
    result["globals_has_target"] = call_target in (evidence.get("resolved_globals") or [])
    result["bytecode_has_target"] = call_target in (evidence.get("bytecode_calls") or [])
    return result
```

```python
# tools/cq/core/summary_contracts.py — Rename to disambiguate
class RunCommandSummaryV1(CqStruct, frozen=True, kw_only=True):
    """Summary payload for run command results (distinct from RunSummaryV1 variant)."""
    ...
```

### Files to Edit

- `tools/cq/query/finding_builders.py` — Replace `apply_call_evidence` (void mutator) with `build_call_evidence` (returns dict)
- `tools/cq/query/executor_runtime_impl.py` — Update caller to merge returned dict
- `tools/cq/query/executor_runtime.py` — Remove underscore-prefixed names from `__all__`
- `tools/cq/search/pipeline/smart_search.py` — Remove `_run_candidate_phase` from `__all__`
- `tools/cq/core/summary_contracts.py` — Rename `RunSummaryV1` to `RunCommandSummaryV1`
- `tools/cq/core/serialization.py` — Replace bare `Any` with `object` at lines 47, 58, 80
- `tools/cq/core/contract_codec.py` — Remove encoder/decoder singletons from `__all__`
- `tools/cq/search/tree_sitter/query/registry.py` — Split `load_query_pack_sources()` into pure loader + separate drift state setter

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete `apply_call_evidence` from `tools/cq/query/finding_builders.py:164-193` — replaced by `build_call_evidence`
- Rename `RunSummaryV1` to `RunCommandSummaryV1` in `tools/cq/core/summary_contracts.py:49` — update all import sites

---

## S6. God Module Decomposition — Tree-Sitter

### Goal

Decompose `rust_lane/runtime_core.py` (1,209 LOC) into focused modules and extract shared query-pack pipeline logic used by both Python and Rust lanes.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/rust_lane/availability.py
from __future__ import annotations

__all__ = ["is_tree_sitter_rust_available"]

def is_tree_sitter_rust_available() -> bool:
    """Check whether tree-sitter Rust grammar is importable."""
    ...
```

```python
# tools/cq/search/tree_sitter/rust_lane/query_pack_execution.py
from __future__ import annotations

__all__ = [
    "collect_query_pack_captures",
    "collect_query_pack_payload",
]

def collect_query_pack_captures(...) -> ...:
    """Collect captures from query pack execution. Public API for orchestration."""
    ...
```

### Files to Edit

- `tools/cq/search/tree_sitter/rust_lane/runtime_core.py` — Decompose into sub-modules; keep as thin re-export facade
- `tools/cq/search/tree_sitter/rust_lane/query_orchestration.py` — Replace `__dict__` access with normal imports from new modules
- `tools/cq/search/tree_sitter/rust_lane/payload_assembly.py` — Replace `__dict__` access with normal imports from new modules

### New Files to Create

- `tools/cq/search/tree_sitter/rust_lane/availability.py` — `is_tree_sitter_rust_available()`
- `tools/cq/search/tree_sitter/rust_lane/scope_utils.py` — `_scope_name`, `_scope_chain`, `_find_scope`
- `tools/cq/search/tree_sitter/rust_lane/extractor_dispatch.py` — `_try_extract`, `_apply_extractors`, `_build_enrichment_payload`
- `tools/cq/search/tree_sitter/rust_lane/query_pack_execution.py` — `collect_query_pack_captures`, `collect_query_pack_payload` (now public)
- `tools/cq/search/tree_sitter/rust_lane/pipeline.py` — `_run_rust_enrichment_pipeline`, `_collect_payload_with_timings`
- `tools/cq/search/tree_sitter/core/query_pack_pipeline.py` — Shared generic `collect_query_pack_captures()` parameterized by language
- `tests/unit/cq/search/tree_sitter/rust_lane/test_query_pack_execution.py` — Tests for extracted module
- `tests/unit/cq/search/tree_sitter/core/test_query_pack_pipeline.py` — Tests for shared pipeline

### Legacy Decommission/Delete Scope

- Delete `__dict__["_collect_query_pack_captures"]` access from `tools/cq/search/tree_sitter/rust_lane/query_orchestration.py:41`
- Delete `__dict__["_collect_query_pack_payload"]` access from `tools/cq/search/tree_sitter/rust_lane/payload_assembly.py:27`
- Reduce `runtime_core.py` from 1,209 LOC to a ~100 LOC re-export facade

---

## S7. God Module Decomposition — Query Engine

### Goal

Decompose `executor_runtime_impl.py` (1,001 LOC) into focused execution modules and collapse the 3-layer facade chain.

### Representative Code Snippets

```python
# tools/cq/query/executor_plan_dispatch.py
from __future__ import annotations

__all__ = ["execute_plan"]

def execute_plan(ctx: QueryExecutionContext) -> CqResult:
    """Dispatch a compiled query plan to the appropriate executor."""
    ...
```

### Files to Edit

- `tools/cq/query/executor_runtime_impl.py` — Decompose into sub-modules
- `tools/cq/query/executor_entity.py` — Replace with actual entity execution logic (or delete)
- `tools/cq/query/executor_pattern.py` — Replace with actual pattern execution logic (or delete)
- `tools/cq/query/executor_runtime.py` — Delete (facade eliminated)
- `tools/cq/query/__init__.py` — Update exports
- `tools/cq/query/planner.py` — Move entity-pattern maps to use `ENTITY_KINDS` registry
- `tools/cq/core/entity_kinds.py` — Extend with `pattern_for_entity()` and Rust entity kinds

### New Files to Create

- `tools/cq/query/executor_plan_dispatch.py` — Plan dispatch and auto-scope orchestration
- `tools/cq/query/executor_entity_impl.py` — Entity query execution
- `tools/cq/query/executor_pattern_impl.py` — Pattern query execution
- `tools/cq/query/executor_call_decorator.py` — Decorator and call query processing
- `tests/unit/cq/query/test_executor_plan_dispatch.py` — Tests for plan dispatch
- `tests/unit/cq/query/test_executor_entity_impl.py` — Tests for entity execution

### Legacy Decommission/Delete Scope

- Delete `tools/cq/query/executor_runtime.py` — facade eliminated; callers import directly from implementation modules
- Delete `tools/cq/query/executor_entity.py` — replaced by `executor_entity_impl.py`
- Delete `tools/cq/query/executor_pattern.py` — replaced by `executor_pattern_impl.py`
- Reduce `executor_runtime_impl.py` from 1,001 LOC to ~100 LOC re-export facade (or delete entirely)
- Delete local `entity_patterns` and `entity_kinds` dicts from `tools/cq/query/planner.py:377-399` — use `ENTITY_KINDS`

---

## S8. God Module Decomposition — Search Backends and Core

### Goal

Decompose `python/extractors.py` (1,795 LOC) and `core/front_door_assembly.py` (769 LOC) into focused modules.

### Representative Code Snippets

```python
# tools/cq/search/python/extractors_orchestrator.py
from __future__ import annotations

__all__ = ["enrich_python_context", "enrich_python_context_by_byte_range"]

def enrich_python_context(...) -> dict[str, object] | None:
    """Orchestrate the 5-stage Python enrichment pipeline."""
    ...
```

### Files to Edit

- `tools/cq/search/python/extractors.py` — Decompose into sub-modules
- `tools/cq/core/front_door_assembly.py` — Split by macro type
- `tools/cq/core/report.py` — Extract render enrichment orchestration
- `tools/cq/core/render_summary.py` — Already addressed in S3 (status derivation consolidation)
- `tools/cq/macros/calls/analysis.py` — Split Rust processing and enrichment into separate modules

### New Files to Create

- `tools/cq/search/python/extractors_orchestrator.py` — Stage sequencing, public entrypoints
- `tools/cq/search/python/extractors_agreement.py` — Cross-source agreement tracking
- `tools/cq/search/python/extractors_budget.py` — Payload budgeting and enforcement
- `tools/cq/core/front_door_search.py` — Search insight assembly
- `tools/cq/core/front_door_entity.py` — Entity insight assembly
- `tools/cq/core/front_door_calls.py` — Calls insight assembly
- `tools/cq/core/render_enrichment_apply.py` — Render enrichment orchestration (from report.py)
- `tools/cq/macros/calls/rust_calls.py` — Rust call site processing
- `tools/cq/macros/calls/enrichment.py` — Call site enrichment with symtable/bytecode
- `tests/unit/cq/search/python/test_extractors_agreement.py` — Tests for agreement tracking
- `tests/unit/cq/search/python/test_extractors_budget.py` — Tests for budget enforcement

### Legacy Decommission/Delete Scope

- Reduce `tools/cq/search/python/extractors.py` from 1,795 LOC to ~400 LOC orchestrator
- Reduce `tools/cq/core/front_door_assembly.py` from 769 LOC to ~200 LOC thin dispatcher
- Reduce `tools/cq/macros/calls/analysis.py` from 771 LOC to ~450 LOC after Rust extraction

---

## S9. Typed Enrichment Payload Migration

### Goal

Replace `dict[str, object]` enrichment payloads with the existing typed fact structs (`PythonEnrichmentFacts`, `RustEnrichmentFacts`) at the enrichment boundary. This addresses 7 design principles simultaneously (P8, P9, P10, P14, P15, P19, P23) and eliminates the single largest source of fragility (913 occurrences across 140 files).

### Representative Code Snippets

```python
# tools/cq/search/_shared/enrichment_contracts.py — Typed payload wrappers
class RustTreeSitterEnrichmentV1(msgspec.Struct, frozen=True, omit_defaults=True, forbid_unknown_fields=True):
    schema_version: int = 1
    payload: RustEnrichmentFacts | None = None  # was: dict[str, object]


class PythonEnrichmentV1(msgspec.Struct, frozen=True, omit_defaults=True, forbid_unknown_fields=True):
    schema_version: int = 1
    payload: PythonEnrichmentFacts | None = None  # was: dict[str, object]


class IncrementalEnrichmentV1(msgspec.Struct, frozen=True, omit_defaults=True, forbid_unknown_fields=True):
    schema_version: int = 1
    mode: IncrementalEnrichmentModeV1 = DEFAULT_INCREMENTAL_ENRICHMENT_MODE
    payload: IncrementalFacts | None = None  # was: dict[str, object]
```

```python
# tools/cq/search/enrichment/contracts.py — Typed port signatures
class LanguageEnrichmentPort(Protocol):
    language: QueryLanguage

    def payload_from_match(self, match: object) -> PythonEnrichmentFacts | RustEnrichmentFacts | None:
        ...

    def accumulate_telemetry(
        self,
        lang_bucket: EnrichmentTelemetryBucket,
        facts: PythonEnrichmentFacts | RustEnrichmentFacts,
    ) -> None:
        ...
```

```python
# tools/cq/search/pipeline/smart_search_telemetry.py — Typed telemetry schema
class StageTelemetryBucket(msgspec.Struct, frozen=True, kw_only=True):
    """Per-stage telemetry counters."""
    applied: int = 0
    degraded: int = 0
    skipped: int = 0


class EnrichmentTelemetryV1(msgspec.Struct, frozen=True, kw_only=True):
    """Typed telemetry schema replacing dict[str, object] factory."""
    python_stages: dict[str, StageTelemetryBucket] = msgspec.field(default_factory=dict)
    rust_stages: dict[str, StageTelemetryBucket] = msgspec.field(default_factory=dict)
    python_stage_timings_ms: dict[str, float] = msgspec.field(default_factory=dict)
    rust_stage_timings_ms: dict[str, float] = msgspec.field(default_factory=dict)
    ...
```

### Files to Edit

- `tools/cq/search/_shared/enrichment_contracts.py` — Replace `payload: dict[str, object]` with typed fact references
- `tools/cq/search/enrichment/contracts.py` — Update `LanguageEnrichmentPort` signatures to use typed facts
- `tools/cq/search/enrichment/python_adapter.py` — Return `PythonEnrichmentFacts` from `payload_from_match`
- `tools/cq/search/enrichment/rust_adapter.py` — Return `RustEnrichmentFacts` from `payload_from_match`
- `tools/cq/search/enrichment/core.py` — Remove `_partition_python_payload_fields` (no longer needed with typed facts)
- `tools/cq/search/pipeline/classification.py` — Update to flow typed facts through classification
- `tools/cq/search/pipeline/smart_search_telemetry.py` — Replace `empty_enrichment_telemetry()` dict factory with `EnrichmentTelemetryV1()` struct
- `tools/cq/search/pipeline/smart_search_sections.py` — Update to use typed facts
- `tools/cq/search/objects/resolve.py` — Replace deep dict traversal with typed fact access
- `tools/cq/search/semantic/models.py` — Replace `_string_list`, `_mapping_list` etc. with typed fact access
- `tools/cq/search/python/extractors.py` (or `extractors_orchestrator.py` after S8) — Return typed `PythonEnrichmentFacts` instead of `dict[str, object]`
- `tools/cq/search/tree_sitter/rust_lane/runtime_core.py` (or sub-modules after S6) — Return typed facts instead of `dict[str, object]`
- `tools/cq/search/tree_sitter/contracts/lane_payloads.py` — Return typed payload structs instead of `dict[str, Any]`

### New Files to Create

- `tools/cq/search/enrichment/telemetry_schema.py` — `EnrichmentTelemetryV1`, `StageTelemetryBucket`, `RuntimeFlagsBucket`
- `tests/unit/cq/search/enrichment/test_telemetry_schema.py` — Tests for typed telemetry
- `tests/unit/cq/search/pipeline/test_typed_enrichment_flow.py` — Integration tests for typed fact flow

### Legacy Decommission/Delete Scope

- Delete `_partition_python_payload_fields` from `tools/cq/search/enrichment/core.py:266-295` — typed facts make field partitioning automatic
- Delete `_string_list`, `_mapping_list`, `_python_diagnostics`, `_locals_preview` from `tools/cq/search/semantic/models.py` — replaced by typed fact accessors
- Delete `_parse_struct_or_none`, `_facts_dict`, `_merge_string_key_mapping`, `_merge_import_payload`, `_normalize_resolution_fact_rows`, `_normalize_resolution_fact_fields` from `tools/cq/search/python/extractors.py` — no longer needed
- Delete `empty_enrichment_telemetry()` from `tools/cq/search/pipeline/smart_search_telemetry.py:44-98` — replaced by `EnrichmentTelemetryV1()` struct

---

## S10. Mutable Singleton Elimination

### Goal

Replace module-level mutable singletons with injectable context objects, enabling test isolation and dependency injection.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/core/runtime_context.py
from __future__ import annotations

import msgspec

__all__ = ["TreeSitterRuntimeContext", "get_default_context"]


class TreeSitterRuntimeContext(msgspec.Struct):
    """Aggregated mutable runtime state for tree-sitter subsystem.

    Replaces module-level _SESSIONS, _GLOBAL_STATE_HOLDER, _TREE_CACHE, _TREE_CACHE_EVICTIONS.
    """
    parse_sessions: dict[str, object] = msgspec.field(default_factory=dict)
    tree_cache: object | None = None
    query_runtime_state: object | None = None
    eviction_count: int = 0


_DEFAULT_CONTEXT: TreeSitterRuntimeContext | None = None


def get_default_context() -> TreeSitterRuntimeContext:
    """Return the process-level default context, creating it lazily."""
    global _DEFAULT_CONTEXT
    if _DEFAULT_CONTEXT is None:
        _DEFAULT_CONTEXT = TreeSitterRuntimeContext()
    return _DEFAULT_CONTEXT
```

```python
# tools/cq/cli_app/protocols.py — ConsolePort protocol
from __future__ import annotations

from typing import IO, Protocol

__all__ = ["ConsolePort"]


class ConsolePort(Protocol):
    """Protocol for console output, enabling test injection."""

    def print(self, *args: object, **kwargs: object) -> None: ...

    @property
    def file(self) -> IO[str]: ...
```

### Files to Edit

- `tools/cq/search/tree_sitter/core/parse.py` — Replace `_SESSIONS` module global with context-based access
- `tools/cq/search/tree_sitter/query/runtime_state.py` — Replace `_GLOBAL_STATE_HOLDER` with context-based access
- `tools/cq/search/tree_sitter/rust_lane/runtime_cache.py` — Replace `_TREE_CACHE`, `_TREE_CACHE_EVICTIONS` with context; make `CACHE_REGISTRY` registration lazy
- `tools/cq/cli_app/app.py` — Make `console`, `error_console` lazily initialized; provide injection point
- `tools/cq/cli_app/context.py` — Add `ConsolePort` to `CliContext`
- `tools/cq/cli_app/result_render.py` — Accept `ConsolePort` from context instead of importing singleton
- `tools/cq/cli_app/result_action.py` — Accept `ConsolePort` from context instead of importing singleton
- `tools/cq/core/bootstrap.py` — Add `reset_runtime_services()` for test isolation
- `tools/cq/core/worker_scheduler.py` — Add `reset_worker_scheduler()` for test isolation

### New Files to Create

- `tools/cq/search/tree_sitter/core/runtime_context.py` — `TreeSitterRuntimeContext` and `get_default_context()`
- `tools/cq/cli_app/protocols.py` — `ConsolePort` protocol definition
- `tests/unit/cq/search/tree_sitter/core/test_runtime_context.py` — Tests for context isolation
- `tests/unit/cq/cli_app/test_protocols.py` — Tests for ConsolePort

### Legacy Decommission/Delete Scope

- Delete `_SESSIONS` module-level dict from `tools/cq/search/tree_sitter/core/parse.py:270`
- Delete `_GLOBAL_STATE_HOLDER` from `tools/cq/search/tree_sitter/query/runtime_state.py:29`
- Delete `_TREE_CACHE_EVICTIONS` dict-wrapping-int pattern from `tools/cq/search/tree_sitter/rust_lane/runtime_cache.py:33`
- Remove import-time `CACHE_REGISTRY.register_cache()` calls from `tools/cq/search/tree_sitter/rust_lane/runtime_cache.py:34,132`

---

## S11. Shell-Layer Domain Logic Extraction

### Goal

Extract domain logic from CLI command handlers into testable core functions, making the shell a thin adapter.

### Representative Code Snippets

```python
# tools/cq/run/scope.py — Extracted from CLI command
def apply_in_dir_scope(
    in_value: str,
    root: Path,
    *,
    lang: str = "auto",
) -> list[str]:
    """Convert a --in directory argument to include glob patterns."""
    candidate = root / in_value
    if candidate.is_file():
        return [str(candidate)]
    return [f"{in_value}/**"]
```

```python
# tools/cq/query/router.py — Extracted from CLI command
def route_query_or_search(
    query_string: str,
    ctx: QueryExecutionContext,
) -> CqResult:
    """Route a user query string to either query or search execution."""
    try:
        query = parse_query(query_string)
        return execute_plan(compile_query(query, ctx))
    except QueryParseError:
        if _looks_like_search_query(query_string):
            return smart_search(query_string, ...)
        raise
```

### Files to Edit

- `tools/cq/cli_app/commands/search.py` — Remove `--in` dir-to-glob logic; delegate to `run/scope.py`
- `tools/cq/cli_app/commands/query.py` — Remove search-fallback routing; delegate to `query/router.py`
- `tools/cq/cli_app/result_action.py` — Split into `prepare_output()` (pure) and `emit_prepared_output()` (command)
- `tools/cq/macros/shared.py` — Add `scan_python_files()` shared helper for macro file scanning

### New Files to Create

- `tools/cq/run/scope.py` (if not existing) — `apply_in_dir_scope()` for `--in` flag handling
- `tools/cq/query/router.py` — `route_query_or_search()` for query/search fallback
- `tools/cq/macros/shared.py` — `scan_python_files(root, include, exclude, visitor_factory)` shared scan helper
- `tests/unit/cq/run/test_scope.py` — Tests for scope handling
- `tests/unit/cq/query/test_router.py` — Tests for query routing
- `tests/unit/cq/macros/test_shared.py` — Tests for shared scan helper

### Legacy Decommission/Delete Scope

- Delete `--in` directory handling logic from `tools/cq/cli_app/commands/search.py:57-64`
- Delete search-fallback routing from `tools/cq/cli_app/commands/query.py:55-89`
- Delete file-scan-visit boilerplate from 6 macro files (replaced by `scan_python_files`):
  - `tools/cq/macros/exceptions.py:277-293`
  - `tools/cq/macros/side_effects.py:238-268`
  - `tools/cq/macros/imports.py:234-266`
  - `tools/cq/macros/scopes.py:159-173`
  - `tools/cq/macros/bytecode.py:166-184`
  - `tools/cq/macros/impact.py:144-149`

---

## S12. Missing Abstraction Introduction

### Goal

Introduce `LanguageEnrichmentProvider` protocol in `semantic/` and `ConsolePort` in CLI (ConsolePort covered in S10), completing the hexagonal architecture boundary.

### Representative Code Snippets

```python
# tools/cq/search/semantic/contracts.py — New enrichment provider protocol
from __future__ import annotations

from typing import Protocol

__all__ = ["LanguageEnrichmentProvider"]


class LanguageEnrichmentProvider(Protocol):
    """Protocol for language-specific enrichment in the semantic front-door."""

    def enrich_by_byte_range(
        self,
        *,
        file_path: str,
        source_bytes: bytes,
        start_byte: int,
        end_byte: int,
    ) -> dict[str, object] | None:
        """Enrich a byte range with language-specific analysis."""
        ...
```

```python
# tools/cq/search/semantic/front_door.py — Use registry instead of hardcoded dispatch
_PROVIDERS: dict[str, LanguageEnrichmentProvider] = {}

def register_language_provider(language: str, provider: LanguageEnrichmentProvider) -> None:
    _PROVIDERS[language] = provider

def _get_provider(language: str) -> LanguageEnrichmentProvider | None:
    return _PROVIDERS.get(language)
```

### Files to Edit

- `tools/cq/search/semantic/front_door.py` — Replace hardcoded dispatch dict at line 215-218 with registry-based dispatch
- `tools/cq/search/semantic/models.py` — Split into `contracts.py` (structs) and `helpers.py` (adapters)

### New Files to Create

- `tools/cq/search/semantic/contracts.py` — `LanguageEnrichmentProvider` protocol
- `tests/unit/cq/search/semantic/test_contracts.py` — Tests for provider protocol

### Legacy Decommission/Delete Scope

- Delete hardcoded `{"python": ..., "rust": ...}` dispatch dict from `tools/cq/search/semantic/front_door.py:215-218`
- Delete direct imports of `enrich_python_context_by_byte_range` and `enrich_rust_context_by_byte_range` from `front_door.py:35-36`

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S1, S5)

- Delete `_apply_render_enrichment_in_place` from `report.py` — requires S1 (frozen Section) and S5 (CQS fix)
- Delete `render_summary_compact` alias from `report.py:604` — no callers after naming cleanup

### Batch D2 (after S2, S4)

- Delete `tools/cq/search/pipeline/enrichment_contracts.py` — relocated to `_shared/` in S2
- Delete all 8 duplicated telemetry accumulation functions — consolidated in S4

### Batch D3 (after S6, S7, S8)

- Delete `tools/cq/query/executor_runtime.py` facade — eliminated in S7
- Delete `tools/cq/query/executor_entity.py` facade — eliminated in S7
- Delete `tools/cq/query/executor_pattern.py` facade — eliminated in S7
- Reduce `executor_runtime_impl.py` to thin re-export or delete entirely — decomposed in S7
- Reduce `runtime_core.py` to thin re-export — decomposed in S6
- Reduce `extractors.py` to thin orchestrator — decomposed in S8

### Batch D4 (after S3, S11)

- Delete all trivial re-export modules in `_shared/`: `timeout.py`, `rg_request.py`, `encoding.py` — S3 consolidation
- Delete `generate_followup_suggestions` compatibility alias from `smart_search_followups.py` — S3 cleanup
- Delete file-scan-visit boilerplate from 6 macros — replaced by `scan_python_files` in S11

### Batch D5 (after S9)

- Delete `_partition_python_payload_fields` and ~20 defensive coercion helpers — typed facts make them unnecessary
- Delete `empty_enrichment_telemetry()` dict factory — replaced by `EnrichmentTelemetryV1` struct

### Batch D6 (after S10)

- Delete module-level `_SESSIONS`, `_GLOBAL_STATE_HOLDER`, `_TREE_CACHE_EVICTIONS` singletons — replaced by `TreeSitterRuntimeContext`
- Remove import-time `CACHE_REGISTRY` registrations — moved to lazy initialization

---

## Implementation Sequence

1. **S1. Frozen Struct Integrity** — Zero external dependencies. Foundation for all subsequent work. Ensures the immutability invariant that copy-on-write patterns depend on.

2. **S5. CQS Violations and Public Surface Cleanup** — Independent quick wins. Improves code quality immediately with minimal risk. Pairs well with S1 as a single "contract integrity" PR.

3. **S3. DRY Consolidation — Small Wins** — Independent of S1/S5. Many small, safe changes that reduce code surface area before larger refactors.

4. **S2. Dependency Direction Corrections** — Depends on understanding the import graph. Relocating `enrichment_contracts.py` unblocks S4 and S9. Core DI fixes unblock S10.

5. **S4. Telemetry Accumulation Consolidation** — Depends on S2 (enrichment contracts relocated) and S3 (`coerce_count` consolidated). Medium effort, high DRY impact.

6. **S11. Shell-Layer Domain Logic Extraction** — Independent of S2-S4. Can proceed in parallel. Extracts domain logic before God module decomposition reduces merge conflicts.

7. **S12. Missing Abstraction Introduction** — Depends on S2 (clean dependency direction). Introduces protocols that S6 and S9 will use.

8. **S6. God Module Decomposition — Tree-Sitter** — Depends on S3 (duplications resolved) and S12 (protocols available). Largest single structural change in tree-sitter.

9. **S7. God Module Decomposition — Query Engine** — Depends on S3 (entity-pattern maps consolidated). Can proceed in parallel with S6.

10. **S8. God Module Decomposition — Search Backends and Core** — Depends on S6 (shared query-pack pipeline extracted). Decomposes the remaining God modules.

11. **S9. Typed Enrichment Payload Migration** — Depends on S2 (contracts relocated), S4 (telemetry consolidated), S6/S8 (modules decomposed for clean integration points). This is the highest-impact change but requires the most preparation.

12. **S10. Mutable Singleton Elimination** — Depends on S6 (tree-sitter decomposition provides natural injection points) and S9 (typed payloads reduce the surface area of state that needs injection). Final structural improvement.

---

## Implementation Checklist

- [ ] S1. Frozen struct integrity (Section, report.py copy-on-write, 7 macro structs)
- [ ] S5. CQS violations and public surface cleanup
- [ ] S3. DRY consolidation — small wins (16 deduplication items)
- [ ] S2. Dependency direction corrections (enrichment contracts, core DI, astgrep)
- [ ] S4. Telemetry accumulation consolidation
- [ ] S11. Shell-layer domain logic extraction
- [ ] S12. Missing abstraction introduction (LanguageEnrichmentProvider)
- [ ] S6. God module decomposition — tree-sitter (runtime_core.py → 5 modules)
- [ ] S7. God module decomposition — query engine (executor_runtime_impl.py → 4 modules)
- [ ] S8. God module decomposition — search backends and core (extractors.py, front_door_assembly.py, calls/analysis.py)
- [ ] S9. Typed enrichment payload migration (dict[str, object] → typed facts)
- [ ] S10. Mutable singleton elimination (TreeSitterRuntimeContext, ConsolePort)
- [ ] D1. Cross-scope deletion batch (after S1, S5)
- [ ] D2. Cross-scope deletion batch (after S2, S4)
- [ ] D3. Cross-scope deletion batch (after S6, S7, S8)
- [ ] D4. Cross-scope deletion batch (after S3, S11)
- [ ] D5. Cross-scope deletion batch (after S9)
- [ ] D6. Cross-scope deletion batch (after S10)
