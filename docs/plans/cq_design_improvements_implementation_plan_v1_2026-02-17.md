# CQ Design Improvements Implementation Plan v1 (2026-02-17)

## Scope Summary

This plan synthesizes findings from 7 deep design reviews covering all ~82K LOC / 390 files in `tools/cq/`. It addresses 9 systemic themes confirmed across the reviews: (1) frozen struct integrity breaches, (2) dependency direction violations, (3) knowledge duplication, (4) God module concentration, (5) the `dict[str, object]` payload anti-pattern (913 occurrences across 140 files), (6) module-level mutable singletons, (7) facade/alias proliferation, (8) summary payload typing gaps, and (9) testability gaps caused by hidden construction and global registries. The plan is organized as 25 scope items (S1–S25) and 10 cross-scope deletion batches (D1–D10), ordered by dependency and risk, progressing from contract integrity and boundary fixes through decomposition and typed payload migration to final observability hardening.

**Design stance:** Incremental migration with no compatibility shims. Each scope item produces a working codebase. No `# deprecated` stubs or re-export facades for removed code.

## Design Principles

1. **Frozen by default**: All `msgspec.Struct` data-transfer types must use `frozen=True` unless they are intentional accumulators (e.g., `TaintState`).
2. **Copy-on-write, not mutation**: Use `msgspec.structs.replace()` for all struct updates. Never bypass frozen with `object.__setattr__`.
3. **Parse at the boundary, flow typed data**: Convert `dict[str, object]` to typed structs at enrichment boundaries; downstream code receives typed structs.
4. **Dependency direction**: Lower layers never import from higher layers. Shared contracts live in `_shared/` or `core/`.
5. **No facade chains**: If a module only re-exports, eliminate it and update callers to import from the source.
6. **Injectable context over module singletons**: Mutable runtime state lives in explicit context objects, not module-level globals.
7. **No typed-regression scope items**: Once a boundary is typed, no new or replacement API may reintroduce `dict[str, object]` payload flow in that boundary.
8. **Library-first implementation**: Prefer built-in library capabilities (`msgspec.convert`, `msgspec.to_builtins(order="deterministic")`, diskcache synchronization primitives, cyclopts result-action composition) before introducing custom glue layers.

## Current Baseline

- `Section` at `schema.py:290` is mutable while its container `CqResult` at `schema.py:362` is `frozen=True` — violates immutability invariant.
- `report.py:540-543` directly assigns to `result.key_findings`, `result.evidence`, and `section.findings` on frozen structs — bypasses frozen contract.
- 8 unfrozen data-transfer structs in `macros/` (`BytecodeSurface`, `RaiseSite`, `CatchSite`, `SideEffect`, `ImportInfo`, `ModuleDeps`, `ScopeInfo`, plus `TaintState` which is intentionally mutable).
- `core/services.py` has 12+ imports from `search/`, `macros/`, `query/`, and `orchestration/` — core depends on higher layers.
- `enrichment_contracts.py` lives in `pipeline/` but is imported by `enrichment/` adapters — layer inversion.
- `sgpy_scanner.py:452` imports from `query/executor_ast_grep.py` — astgrep depends on query layer.
- `_extract_provenance` duplicated in 3 files, `_coerce_count` in 2 files, `_pack_source_rows` in 3 files, scope filter in 2 files, schema projection in 11 pairs, file-scan-visit in 6 macros, status derivation in 7 copies.
- 913 `dict[str, object]` occurrences across 140 files. Typed fact structs (`PythonEnrichmentFacts`, `RustEnrichmentFacts`) exist in `enrichment/python_facts.py` and `enrichment/rust_facts.py` but are not threaded through the pipeline.
- God modules (>500 LOC, multiple change reasons): `extractors.py` (1,795 LOC), `runtime_core.py` (1,209 LOC), `executor_runtime_impl.py` (1,001 LOC), `calls/analysis.py` (771 LOC), `front_door_assembly.py` (769 LOC), `python_lane/facts.py` (738 LOC), `executor_definitions.py` (715 LOC), `diskcache_backend.py` (700 LOC), `python_lane/runtime.py` (685 LOC), `rust/enrichment.py` (667 LOC), `tree_sitter_collector.py` (661 LOC), `summary_contract.py` (663 LOC), `core/runtime.py` (644 LOC), `enrichment_facts.py` (642 LOC), `report.py` (614 LOC), `calls_target.py` (576 LOC), `calls/entry.py` (536 LOC), `multilang_orchestrator.py` (515 LOC).
- Module-level mutable singletons: `_SESSIONS`, `_GLOBAL_STATE_HOLDER`, `_TREE_CACHE`, `_AST_CACHE` (2 copies), `console`, `error_console`, `app.config`, `_SEARCH_OBJECT_VIEW_REGISTRY`, `_LANGUAGE_ADAPTERS`, `_TELEMETRY`, `_SEEN_KEYS`, `_SCHEDULER_STATE`, `_BACKEND_STATE`.
- Additional review-confirmed gaps now captured by expanded scopes S20–S25: `fragment_engine.py` + `fragment_orchestrator.py` layering complexity, mutable `DETAILS_KIND_REGISTRY`, query-side hidden constructor paths (`SymtableEnricher`, `load_default_rulepacks`), and CLI telemetry nesting complexity (`invoke_with_telemetry()`).

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
- `tools/cq/core/run_context.py` — Change `RunContext.argv: list[str]` to `argv: tuple[str, ...]` for frozen integrity (Review 7, P10)
- `tools/cq/neighborhood/snb_renderer.py` — Change `SemanticNeighborhoodBundleV1.node_index: dict[str, object]` to a frozen struct or `types.MappingProxyType` wrapper (Review 4, P10)

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
- `tools/cq/search/pipeline/assembly.py` — Consolidate `_pack_source_rows` (duplicated in 3 pipeline files) into `_shared/helpers.py` (Review 2, P7)
- `tools/cq/search/tree_sitter/core/adaptive_runtime.py` — Consolidate cache policy env-var resolution duplicated between `adaptive_runtime.py` and `partition_pipeline.py` into a single `_shared/cache_config.py` helper (Review 1, P7)
- `tools/cq/query/executor_definitions.py` — Consolidate import parsing logic duplicated between `executor_definitions.py:342-380` and `symbol_resolver.py:118-156` into `query/import_utils.py` (Review 5, P7)
- `tools/cq/macros/calls/entry.py` — Use `ScoringDetailsV1` struct in `build_detail_payload` to eliminate 6+ `asdict` scatter sites across macros (Review 6, P7)
- `tools/cq/macros/_rust_fallback.py` — Standardize Rust fallback pattern: `_rust_fallback.py` uses `try/except ImportError`, `result_builder.py` uses `shutil.which()` — pick one convention (Review 6, P7)
- `tools/cq/core/render_overview.py` — Consolidate `_safe_rel_path` (in `render_overview.py`) and `_to_rel_path` (in `render_summary.py`) into a single `core/path_utils.py` helper (Review 4, P7)

### New Files to Create

- `tools/cq/core/scope_filter.py` — Consolidated scope filtering logic
- `tests/unit/cq/core/test_scope_filter.py` — Tests for scope filter
- `tools/cq/search/_shared/cache_config.py` — Shared cache policy env resolution helper
- `tools/cq/query/import_utils.py` — Shared import parsing logic (from executor_definitions + symbol_resolver)
- `tools/cq/core/path_utils.py` — Consolidated `safe_rel_path()` helper
- `tests/unit/cq/query/test_import_utils.py` — Tests for shared import parsing
- `tests/unit/cq/core/test_path_utils.py` — Tests for path utilities

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
- Delete `_pack_source_rows` from `tools/cq/search/pipeline/assembly.py` and 2 other pipeline files — consolidated into `_shared/helpers.py`
- Delete cache policy env resolution from `tools/cq/search/tree_sitter/core/adaptive_runtime.py` and `partition_pipeline.py` — consolidated into `_shared/cache_config.py`
- Delete import parsing logic from `tools/cq/query/executor_definitions.py:342-380` and `symbol_resolver.py:118-156` — consolidated into `query/import_utils.py`
- Delete `_safe_rel_path` from `tools/cq/core/render_overview.py` and `_to_rel_path` from `render_summary.py` — consolidated into `core/path_utils.py`

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
- `tools/cq/query/executor_runtime_impl.py` — Replace `assert isinstance(...)` guards with `ValueError` raises at 8 sites (Review 5, P8)
- `tools/cq/query/symbol_resolver.py` — Split `SymbolTable.resolve()` CQS violation: currently appends to `self.unresolved` (command) and returns resolved symbol (query); split into `resolve()` (pure query) and `record_unresolved()` (command) (Review 5, P11)
- `tools/cq/macros/calls/semantic.py` — Fix in-place mutation at lines 94-97: `populate_semantic_evidence()` mutates its `evidence` dict argument; return new dict instead (Review 6, P11)
- `tools/cq/search/pipeline/__init__.py` — Rename `SearchContext` export alias (shadows stdlib `contextvars.Context` semantics) to `SearchPipelineContext`; keep `contracts.py` naming canonical (Review 2, P21)
- `tools/cq/cli_app/result_action.py` — Split `handle_result()` CQS violation into `prepare_output()` (pure query → output payload) and `emit_prepared_output()` (command → write to console) (Review 7, P11)
- `tools/cq/search/pipeline/assembly.py` — Fix `enforce_payload_budget()` CQS violation: currently mutates payload in-place and returns bool; split into `check_budget()` (query → bool) and `trim_to_budget()` (command → new payload) (Review 2, P11)

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete `apply_call_evidence` from `tools/cq/query/finding_builders.py:164-193` — replaced by `build_call_evidence`
- Rename `RunSummaryV1` to `RunCommandSummaryV1` in `tools/cq/core/summary_contracts.py:49` — update all import sites
- Delete `populate_semantic_evidence` from `tools/cq/macros/calls/semantic.py:94-97` — replaced by pure function returning new dict
- Delete void `handle_result()` from `tools/cq/cli_app/result_action.py` — replaced by `prepare_output()` + `emit_prepared_output()`

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

- `tools/cq/search/tree_sitter/rust_lane/runtime_core.py` — Decompose into sub-modules; migrate all callsites to extracted modules; remove facade-style pass-through exports
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
- Delete `runtime_core.py` once all imports have been migrated to extracted modules

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
- `tools/cq/query/executor_entity.py` — Migrate all callsites to `executor_entity_impl.py`, then delete module
- `tools/cq/query/executor_pattern.py` — Migrate all callsites to `executor_pattern_impl.py`, then delete module
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
- Delete `executor_runtime_impl.py` after extracting and migrating all execution paths to focused modules
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

def enrich_python_context(...) -> PythonEnrichmentFacts | None:
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

Wrapper-contract field migration in `_shared/enrichment_contracts.py` is completed in S2; S9 edits all downstream typed-fact consumers.

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
- `tools/cq/search/tree_sitter/rust_lane/runtime_core.py` — Pre-S6 integration point for typed Rust fact payload migration (migrated to extracted modules in S6)
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
- `tools/cq/core/runtime/worker_scheduler.py` — Add `reset_worker_scheduler()` for test isolation

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
    ) -> PythonEnrichmentFacts | RustEnrichmentFacts | None:
        """Enrich a byte range with language-specific analysis."""
        ...
```

```python
# tools/cq/search/semantic/registry.py — Injectable provider registry (no module singleton)
from __future__ import annotations

from dataclasses import dataclass, field

@dataclass
class LanguageProviderRegistry:
    providers: dict[str, LanguageEnrichmentProvider] = field(default_factory=dict)

    def register(self, language: str, provider: LanguageEnrichmentProvider) -> None:
        self.providers[language] = provider

    def get(self, language: str) -> LanguageEnrichmentProvider | None:
        return self.providers.get(language)
```

### Files to Edit

- `tools/cq/search/semantic/front_door.py` — Replace hardcoded dispatch dict with `LanguageProviderRegistry` passed from composition root/context
- `tools/cq/search/semantic/models.py` — Split into `contracts.py` (structs) and `helpers.py` (adapters)
- `tools/cq/search/tree_sitter/core/adaptive_runtime.py` — Introduce `CacheBackendProtocol` so adaptive runtime accepts any cache backend via DI, not just the concrete `diskcache` import (Review 1, P6)
- `tools/cq/search/python/extractors.py` — Introduce `PythonNodeAccess` protocol for the 14 `node.xxx` attribute accesses that assume a concrete tree-sitter node type; enables testing with stubs (Review 3, P6)
- `tools/cq/search/rg/adapter.py` — Extract `_run_and_collect()` helper from `RgAdapter.search()` (currently 4 inline subprocess invocations with identical error handling); single subprocess interaction seam (Review 3, P7)
- `tools/cq/core/bootstrap.py` — Register language providers once and pass registry into semantic front-door composition

### New Files to Create

- `tools/cq/search/semantic/contracts.py` — `LanguageEnrichmentProvider` protocol
- `tools/cq/search/semantic/registry.py` — `LanguageProviderRegistry` value object
- `tools/cq/search/tree_sitter/core/cache_protocol.py` — `CacheBackendProtocol` for injectable cache backends
- `tools/cq/search/python/node_protocol.py` — `PythonNodeAccess` protocol for tree-sitter node abstraction
- `tests/unit/cq/search/semantic/test_contracts.py` — Tests for provider protocol
- `tests/unit/cq/search/semantic/test_registry.py` — Tests for provider registry behavior
- `tests/unit/cq/search/tree_sitter/core/test_cache_protocol.py` — Tests for cache backend protocol
- `tests/unit/cq/search/python/test_node_protocol.py` — Tests for node access protocol

### Legacy Decommission/Delete Scope

- Delete hardcoded `{"python": ..., "rust": ...}` dispatch dict from `tools/cq/search/semantic/front_door.py`
- Delete module-level `_PROVIDERS` mutable singleton pattern from semantic front-door path
- Delete direct imports of `enrich_python_context_by_byte_range` and `enrich_rust_context_by_byte_range` from `front_door.py:35-36`
- Delete direct `diskcache` import from `tools/cq/search/tree_sitter/core/adaptive_runtime.py` — replaced by `CacheBackendProtocol`

---

## S13. God Module Decomposition — Tree-Sitter Lanes

### Goal

Decompose 3 God modules in the tree-sitter Python and Rust lane subsystems that were identified at deep review depth: `python_lane/facts.py` (738 LOC), `python_lane/runtime.py` (685 LOC), and `core/runtime.py` (644 LOC). These are distinct from the `rust_lane/runtime_core.py` (1,209 LOC) addressed in S6.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/python_lane/fact_extraction.py — Extracted from facts.py
from __future__ import annotations

__all__ = ["extract_python_facts", "extract_scope_facts"]

def extract_python_facts(node: object, *, source_bytes: bytes) -> PythonEnrichmentFacts:
    """Extract structural facts from a Python tree-sitter node."""
    ...

def extract_scope_facts(node: object, *, source_bytes: bytes) -> PythonScopeFactsV1:
    """Extract scope-related facts (closure, class, module level)."""
    ...
```

```python
# tools/cq/search/tree_sitter/python_lane/capture_processing.py — Extracted from runtime.py
from __future__ import annotations

__all__ = ["process_captures", "group_captures_by_scope"]

def process_captures(
    captures: list[object], *, source_bytes: bytes
) -> tuple[PythonCaptureFactV1, ...]:
    """Process raw tree-sitter captures into structured payloads."""
    ...
```

### Files to Edit

- `tools/cq/search/tree_sitter/python_lane/facts.py` — Decompose into `fact_extraction.py` (structural facts), `fact_formatting.py` (output formatting), `fact_contracts.py` (typed fact structs)
- `tools/cq/search/tree_sitter/python_lane/runtime.py` — Decompose into `capture_processing.py` (capture grouping), `enrichment_dispatch.py` (stage orchestration), `query_pack_runner.py` (query pack execution)
- `tools/cq/search/tree_sitter/core/runtime.py` — Decompose into `session_management.py` (parse session lifecycle), `language_dispatch.py` (lane routing), `scan_orchestration.py` (file scan coordination)

### New Files to Create

- `tools/cq/search/tree_sitter/python_lane/fact_extraction.py` — Structural fact extraction from nodes
- `tools/cq/search/tree_sitter/python_lane/fact_formatting.py` — Fact output formatting
- `tools/cq/search/tree_sitter/python_lane/fact_contracts.py` — Typed fact structs for Python lane
- `tools/cq/search/tree_sitter/python_lane/capture_processing.py` — Capture grouping and processing
- `tools/cq/search/tree_sitter/python_lane/enrichment_dispatch.py` — Stage orchestration
- `tools/cq/search/tree_sitter/python_lane/query_pack_runner.py` — Query pack execution
- `tools/cq/search/tree_sitter/core/session_management.py` — Parse session lifecycle
- `tools/cq/search/tree_sitter/core/language_dispatch.py` — Lane routing
- `tools/cq/search/tree_sitter/core/scan_orchestration.py` — File scan coordination
- `tests/unit/cq/search/tree_sitter/python_lane/test_fact_extraction.py` — Tests for fact extraction
- `tests/unit/cq/search/tree_sitter/python_lane/test_capture_processing.py` — Tests for capture processing
- `tests/unit/cq/search/tree_sitter/core/test_session_management.py` — Tests for session management

### Legacy Decommission/Delete Scope

- Delete `python_lane/facts.py` once all callsites are migrated to extracted modules
- Delete `python_lane/runtime.py` once all callsites are migrated to extracted modules
- Delete `core/runtime.py` once all callsites are migrated to extracted modules

---

## S14. God Module Decomposition — CLI + Orchestration

### Goal

Decompose 2 God modules in the CLI and orchestration shell: `tree_sitter_collector.py` (661 LOC) and `multilang_orchestrator.py` (515 LOC).

### Representative Code Snippets

```python
# tools/cq/neighborhood/collector_python.py — Extracted from tree_sitter_collector.py
from __future__ import annotations

__all__ = ["collect_python_neighborhood"]

def collect_python_neighborhood(
    file_path: str,
    *,
    source_bytes: bytes,
    anchor_byte: int,
    top_k: int = 10,
) -> SemanticNeighborhoodBundleV1:
    """Collect neighborhood context for a Python anchor point."""
    ...
```

```python
# tools/cq/orchestration/language_router.py — Extracted from multilang_orchestrator.py
from __future__ import annotations

__all__ = ["route_by_language", "detect_language"]

def route_by_language(
    request: object,
    *,
    lang: str = "auto",
) -> QueryLanguage:
    """Route an orchestration request to the appropriate language handler."""
    ...
```

### Files to Edit

- `tools/cq/neighborhood/tree_sitter_collector.py` — Decompose into `collector_python.py` (Python neighborhood), `collector_rust.py` (Rust neighborhood), `collector_shared.py` (shared anchor resolution)
- `tools/cq/orchestration/multilang_orchestrator.py` — Decompose into `language_router.py` (language detection and routing), `result_assembly.py` (multi-language result merging)

### New Files to Create

- `tools/cq/neighborhood/collector_python.py` — Python neighborhood collection
- `tools/cq/neighborhood/collector_rust.py` — Rust neighborhood collection
- `tools/cq/neighborhood/collector_shared.py` — Shared anchor resolution logic
- `tools/cq/orchestration/language_router.py` — Language detection and routing
- `tools/cq/orchestration/result_assembly.py` — Multi-language result merging
- `tests/unit/cq/neighborhood/test_collector_python.py` — Tests for Python collection
- `tests/unit/cq/orchestration/test_language_router.py` — Tests for language routing

### Legacy Decommission/Delete Scope

- Delete `tree_sitter_collector.py` once all callsites are migrated to extracted modules
- Delete `multilang_orchestrator.py` once all callsites are migrated to extracted modules

---

## S15. God Module Decomposition — Core Foundation

### Goal

Decompose 3 God modules in `core/`: `diskcache_backend.py` (700 LOC), `summary_contract.py` (663 LOC), and `enrichment_facts.py` (642 LOC).

### Representative Code Snippets

```python
# tools/cq/core/cache/eviction_policy.py — Extracted from diskcache_backend.py
from __future__ import annotations

__all__ = ["EvictionPolicy", "apply_eviction"]

class EvictionPolicy(msgspec.Struct, frozen=True, kw_only=True):
    """Cache eviction configuration."""
    max_size_bytes: int = 100 * 1024 * 1024
    ttl_seconds: int = 3600
    ...
```

```python
# tools/cq/core/summary_validation.py — Extracted from summary_contract.py
from __future__ import annotations

__all__ = ["validate_summary_envelope", "validate_summary_sections"]

def validate_summary_envelope(envelope: SummaryEnvelopeV1) -> list[str]:
    """Validate a summary envelope for structural completeness. Returns error messages."""
    ...
```

### Files to Edit

- `tools/cq/core/cache/diskcache_backend.py` — Decompose into `eviction_policy.py` (eviction logic), `serialization.py` (encode/decode), `backend_core.py` (get/set/delete)
- `tools/cq/core/summary_contract.py` — Decompose into `summary_validation.py` (validation logic), `summary_rendering.py` (render helpers), `summary_types.py` (struct definitions)
- `tools/cq/core/enrichment_facts.py` — Decompose into `fact_types.py` (struct definitions), `fact_builders.py` (factory functions), `fact_accessors.py` (typed accessor helpers)

### New Files to Create

- `tools/cq/core/cache/eviction_policy.py` — Eviction logic
- `tools/cq/core/cache/serialization.py` — Encode/decode helpers
- `tools/cq/core/cache/backend_core.py` — Core get/set/delete operations
- `tools/cq/core/summary_validation.py` — Summary validation logic
- `tools/cq/core/summary_rendering.py` — Summary render helpers
- `tools/cq/core/summary_types.py` — Summary struct definitions
- `tools/cq/core/fact_types.py` — Enrichment fact struct definitions
- `tools/cq/core/fact_builders.py` — Enrichment fact factory functions
- `tools/cq/core/fact_accessors.py` — Typed fact accessor helpers
- `tests/unit/cq/core/cache/test_eviction_policy.py` — Tests for eviction
- `tests/unit/cq/core/test_summary_validation.py` — Tests for summary validation
- `tests/unit/cq/core/test_fact_builders.py` — Tests for fact builders

### Legacy Decommission/Delete Scope

- Delete `diskcache_backend.py` once all callsites are migrated to extracted modules
- Delete `summary_contract.py` once all callsites are migrated to extracted modules
- Delete `enrichment_facts.py` once all callsites are migrated to extracted modules

---

## S16. God Module Decomposition — Macros + Query Periphery

### Goal

Decompose 3 God modules in the macros and query subsystems: `calls/entry.py` (536 LOC), `calls_target.py` (576 LOC), and `executor_definitions.py` (715 LOC, import extraction concern distinct from the main decomposition in S7).

### Representative Code Snippets

```python
# tools/cq/macros/calls/entry_dispatch.py — Extracted from entry.py
from __future__ import annotations

__all__ = ["dispatch_call_analysis"]

def dispatch_call_analysis(
    request: CallsRequest,
    *,
    root: Path,
) -> CallsResult:
    """Dispatch a call analysis request to the appropriate handler."""
    ...
```

```python
# tools/cq/macros/calls/target_resolution.py — Extracted from calls_target.py
from __future__ import annotations

__all__ = ["resolve_call_target", "classify_call_target"]

def resolve_call_target(
    name: str,
    *,
    scope: object,
    imports: dict[str, str],
) -> str | None:
    """Resolve a call target name to its fully qualified path."""
    ...
```

### Files to Edit

- `tools/cq/macros/calls/entry.py` — Decompose into `entry_dispatch.py` (request routing), `entry_output.py` (result formatting), keeping `entry.py` as the command boundary orchestrator only
- `tools/cq/macros/calls_target.py` — Decompose into `target_resolution.py` (resolution logic), `target_classification.py` (call-site classification)
- `tools/cq/query/executor_definitions.py` — Extract import handling logic into `query/import_utils.py` (partially covered by S3); extract entity kind registration into `core/entity_kinds.py` (partially covered by S7)
- `tools/cq/macros/calls/entry.py` — Fix private symbol imports: replace `from tools.cq.core._internal import _foo` with public API imports (Review 6, P4/P22)

### New Files to Create

- `tools/cq/macros/calls/entry_dispatch.py` — Request routing logic
- `tools/cq/macros/calls/entry_output.py` — Result formatting
- `tools/cq/macros/calls/target_resolution.py` — Call target resolution
- `tools/cq/macros/calls/target_classification.py` — Call-site classification
- `tests/unit/cq/macros/calls/test_entry_dispatch.py` — Tests for dispatch
- `tests/unit/cq/macros/calls/test_target_resolution.py` — Tests for resolution

### Legacy Decommission/Delete Scope

- Reduce `calls/entry.py` from 536 LOC to a focused command boundary module (<200 LOC)
- Delete `calls_target.py` once all callsites are migrated to `target_resolution.py` and `target_classification.py`
- Remove private symbol imports from `calls/entry.py` — replaced by public API imports

---

## S17. Domain Visitor Extraction to `analysis/`

### Goal

Move domain-specific AST visitor classes from `macros/` to `analysis/`, establishing `analysis/` as the home for reusable domain analysis logic. Currently, `ExceptionVisitor`, `SideEffectVisitor`, and `ImportVisitor` contain domain analysis logic that is only incidentally coupled to the macro execution shell.

### Representative Code Snippets

```python
# tools/cq/analysis/visitors/exception_visitor.py — Relocated from macros/exceptions.py
from __future__ import annotations

import ast

__all__ = ["ExceptionVisitor"]


class ExceptionVisitor(ast.NodeVisitor):
    """Visit AST nodes to collect exception handling patterns.

    Collects raise sites, catch sites, and bare except clauses.
    """

    def __init__(self) -> None:
        self.raise_sites: list[RaiseSite] = []
        self.catch_sites: list[CatchSite] = []
        ...
```

### Files to Edit

- `tools/cq/macros/exceptions.py` — Remove `ExceptionVisitor` class; import from `analysis/visitors/`
- `tools/cq/macros/side_effects.py` — Remove `SideEffectVisitor` class; import from `analysis/visitors/`
- `tools/cq/macros/imports.py` — Remove `ImportVisitor` class; import from `analysis/visitors/`

### New Files to Create

- `tools/cq/analysis/__init__.py` — Package init
- `tools/cq/analysis/visitors/__init__.py` — Package init
- `tools/cq/analysis/visitors/exception_visitor.py` — `ExceptionVisitor` (from macros/exceptions.py)
- `tools/cq/analysis/visitors/side_effect_visitor.py` — `SideEffectVisitor` (from macros/side_effects.py)
- `tools/cq/analysis/visitors/import_visitor.py` — `ImportVisitor` (from macros/imports.py)
- `tests/unit/cq/analysis/visitors/test_exception_visitor.py` — Tests for exception visitor
- `tests/unit/cq/analysis/visitors/test_side_effect_visitor.py` — Tests for side-effect visitor
- `tests/unit/cq/analysis/visitors/test_import_visitor.py` — Tests for import visitor

### Legacy Decommission/Delete Scope

- Delete `ExceptionVisitor` class from `tools/cq/macros/exceptions.py` — relocated to `analysis/visitors/`
- Delete `SideEffectVisitor` class from `tools/cq/macros/side_effects.py` — relocated to `analysis/visitors/`
- Delete `ImportVisitor` class from `tools/cq/macros/imports.py` — relocated to `analysis/visitors/`

---

## S18. Quick Wins — Naming and Contract Fixes

### Goal

Collect small, independent naming and contract fixes identified across all 7 reviews that don't warrant their own scope item but should not be lost. Each is a 1-5 line change.

### Representative Code Snippets

```python
# tools/cq/search/tree_sitter/contracts/core_models.py — @dataclass → CqStruct
# Before:
@dataclass
class TreeSitterInputEditV1:
    start_byte: int
    ...

# After:
class TreeSitterInputEditV1(CqStruct, frozen=True, kw_only=True):
    start_byte: int
    ...
```

### Files to Edit

- `tools/cq/search/tree_sitter/contracts/core_models.py` — Convert `TreeSitterInputEditV1` from `@dataclass` to `CqStruct` (frozen=True) (Review 1, P22)
- `tools/cq/search/python/resolution_support.py` — Remove leading underscore from `_AstAnchor` → `AstAnchor` (public-facing type) (Review 3, P21)
- `tools/cq/search/python/resolution_support.py` — Remove leading underscore from `_DefinitionSite` → `DefinitionSite` (public-facing type) (Review 3, P21)
- `tools/cq/search/rg/adapter.py` — Add V1 suffix to `RgProcessResult` → `RgProcessResultV1` for contract naming consistency (Review 3, P22)
- `tools/cq/search/semantic/diagnostics.py` — Freeze `CAPABILITY_MATRIX` dict as `types.MappingProxyType` (Review 3, P20)
- `tools/cq/neighborhood/snb_renderer.py` — Add V1 suffix to `NeighborhoodExecutionRequest` → `NeighborhoodExecutionRequestV1` (Review 7, P22)
- `tools/cq/cli_app/params.py` — Align `NeighborhoodParams` with schema-driven pattern used by other param structs (Review 7, P21)
- `tools/cq/query/planner.py` — Remove unused `python_path` parameter from `compile_query()` (Review 5, P20)
- `tools/cq/query/planner.py` — Remove unused `rulepack_path` parameter from `compile_query()` (Review 5, P20)

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete `@dataclass` decorator from `TreeSitterInputEditV1` — replaced by `CqStruct` base
- Rename `_AstAnchor` → `AstAnchor` in `resolution_support.py` — update all references and exports
- Rename `_DefinitionSite` → `DefinitionSite` in `resolution_support.py` — update all references and exports
- Delete unused `python_path` and `rulepack_path` parameters from `compile_query()` signature

---

## S19. Observability — Structured Logging

### Goal

Add structured logging to the ~55 pipeline files and 5 silent macros that currently have zero log statements. Without logging, failures in these modules produce no diagnostic output, forcing developers to add temporary print statements during debugging.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/assembly.py — Add module logger
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def assemble_search_result(...) -> ...:
    """Assemble search results from classified candidates."""
    logger.debug("Assembling %d candidates for query=%r", len(candidates), query)
    ...
    if dropped:
        logger.info("Dropped %d candidates by scope filter", len(dropped))
    ...
```

```python
# tools/cq/macros/exceptions.py — Add macro entry/exit logging
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def analyze_exceptions(root: Path, ...) -> list[RaiseSite]:
    """Analyze exception patterns in Python files."""
    logger.debug("Starting exception analysis in %s", root)
    ...
    logger.info("Exception analysis complete: %d raise sites, %d catch sites", len(raises), len(catches))
    return raises
```

### Files to Edit

- `tools/cq/search/pipeline/assembly.py` — Add `logging.getLogger(__name__)` and key entry/exit/error log points
- `tools/cq/search/pipeline/classification.py` — Add structured logging
- `tools/cq/search/pipeline/candidate_phase.py` — Add structured logging
- `tools/cq/search/pipeline/classify_phase.py` — Add structured logging
- `tools/cq/search/pipeline/partition_pipeline.py` — Add structured logging
- `tools/cq/search/pipeline/smart_search.py` — Add structured logging at pipeline entry/exit
- `tools/cq/search/pipeline/smart_search_sections.py` — Add structured logging
- `tools/cq/search/pipeline/smart_search_summary.py` — Add structured logging
- `tools/cq/search/pipeline/smart_search_followups.py` — Add structured logging
- `tools/cq/search/pipeline/target_resolution.py` — Add structured logging
- `tools/cq/search/pipeline/request_parsing.py` — Add structured logging
- `tools/cq/search/pipeline/neighborhood_preview.py` — Add structured logging
- `tools/cq/search/pipeline/search_runtime.py` — Add structured logging
- `tools/cq/search/python/extractors.py` — Add structured logging for 5-stage enrichment
- `tools/cq/search/rust/enrichment.py` — Add structured logging for Rust enrichment
- `tools/cq/search/rg/adapter.py` — Add structured logging for subprocess invocations
- `tools/cq/macros/exceptions.py` — Add entry/exit logging
- `tools/cq/macros/side_effects.py` — Add entry/exit logging
- `tools/cq/macros/imports.py` — Add entry/exit logging
- `tools/cq/macros/scopes.py` — Add entry/exit logging
- `tools/cq/macros/bytecode.py` — Add entry/exit logging
- Additional ~35 pipeline files with zero logging (see Review 2, P24 and Review 6, P24 for full list)

### New Files to Create

None. This scope item adds `import logging` and `logger = logging.getLogger(__name__)` to existing files.

### Legacy Decommission/Delete Scope

None. This is additive only.

---

## S20. Facade and Alias Eradication

### Goal

Enforce hard-cutover public surfaces by deleting facade chains, lazy re-export modules, and compatibility aliases. Post-refactor modules must be owned by one responsibility and imported directly.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/__init__.py — explicit exports, no Any=None sentinels
from __future__ import annotations

from tools.cq.search.pipeline.contracts import SearchConfig as SearchPipelineContext
from tools.cq.search.pipeline.orchestration import SearchPipeline, assemble_result
from tools.cq.search.pipeline.smart_search_types import SearchResultAssembly

__all__ = [
    "SearchPipeline",
    "SearchPipelineContext",
    "SearchResultAssembly",
    "assemble_result",
]
```

```python
# tools/cq/query/__init__.py — direct exports from canonical modules
from __future__ import annotations

from tools.cq.query.executor_plan_dispatch import execute_plan

__all__ = ["execute_plan"]
```

### Files to Edit

- `tools/cq/query/__init__.py` — Remove facade-style chained exports; expose canonical modules only
- `tools/cq/search/pipeline/__init__.py` — Remove lazy `Any = None` facade pattern and export canonical types directly
- `tools/cq/search/tree_sitter/rust_lane/query_orchestration.py` — Remove private `__dict__`-based lookup calls
- `tools/cq/search/tree_sitter/rust_lane/payload_assembly.py` — Remove private `__dict__`-based lookup calls
- `tools/cq/core/report.py` — Remove `render_summary_compact` compatibility alias
- `tools/cq/search/pipeline/smart_search_followups.py` — Remove compatibility alias and keep canonical function only

### New Files to Create

- `tests/unit/cq/query/test_public_import_surface.py` — Verify direct import surface and absence of facade chain exports
- `tests/unit/cq/search/pipeline/test_public_import_surface.py` — Verify explicit pipeline exports and no lazy `Any=None` sentinels

### Legacy Decommission/Delete Scope

- Delete `tools/cq/query/executor_runtime.py`, `tools/cq/query/executor_entity.py`, and `tools/cq/query/executor_pattern.py` facade modules once S7 migration completes
- Delete `render_summary_compact` alias from `tools/cq/core/report.py`
- Delete compatibility alias in `tools/cq/search/pipeline/smart_search_followups.py`

---

## S21. Mutable Singleton Elimination Completion and Registry Hardening

### Goal

Complete singleton removal for all remaining mutable module-level registries and counters not fully covered by S10. Replace globals with explicit registry/context objects that can be injected and reset in tests.

### Representative Code Snippets

```python
# tools/cq/search/pipeline/object_view_registry.py
from __future__ import annotations

import threading
from dataclasses import dataclass, field

from tools.cq.search.objects.resolve import SearchObjectResolvedViewV1


@dataclass
class SearchObjectViewRegistry:
    _rows: dict[str, SearchObjectResolvedViewV1] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def register(self, run_id: str, view: SearchObjectResolvedViewV1) -> None:
        with self._lock:
            self._rows[run_id] = view

    def pop(self, run_id: str) -> SearchObjectResolvedViewV1 | None:
        with self._lock:
            return self._rows.pop(run_id, None)
```

```python
# tools/cq/core/details_kinds.py — freeze read-only registry contract
from __future__ import annotations

from types import MappingProxyType

_DETAILS_KIND_REGISTRY_MUTABLE: dict[str, KindSpec] = {
    ...
}
DETAILS_KIND_REGISTRY = MappingProxyType(_DETAILS_KIND_REGISTRY_MUTABLE)
```

### Files to Edit

- `tools/cq/search/pipeline/search_object_view_store.py` — Replace `_SEARCH_OBJECT_VIEW_REGISTRY` with injectable `SearchObjectViewRegistry`
- `tools/cq/search/enrichment/language_registry.py` — Replace `_LANGUAGE_ADAPTERS` with explicit registry object
- `tools/cq/core/cache/telemetry.py` — Replace `_TELEMETRY` / `_SEEN_KEYS` with injected telemetry store object
- `tools/cq/core/cache/backend_lifecycle.py` — Replace `_BACKEND_STATE` with explicit backend registry object
- `tools/cq/search/python/extractors.py` — Remove module-global `_AST_CACHE`; resolve from injected runtime/cache context
- `tools/cq/search/rust/enrichment.py` — Remove module-global `_AST_CACHE`; resolve from injected runtime/cache context
- `tools/cq/core/details_kinds.py` — Freeze registry and remove mutable global exposure
- `tools/cq/core/bootstrap.py` — Compose registry/context instances and expose reset hooks for tests

### New Files to Create

- `tools/cq/search/pipeline/object_view_registry.py` — Thread-safe object-view registry
- `tools/cq/search/enrichment/adapter_registry.py` — Language adapter registry object
- `tools/cq/core/cache/backend_registry.py` — Backend lifecycle registry object
- `tools/cq/core/cache/telemetry_store.py` — Telemetry counters store object replacing module globals
- `tests/unit/cq/search/pipeline/test_object_view_registry.py` — Registry semantics tests
- `tests/unit/cq/search/enrichment/test_adapter_registry.py` — Adapter registry tests
- `tests/unit/cq/core/cache/test_backend_registry.py` — Backend registry lifecycle tests
- `tests/unit/cq/core/cache/test_telemetry_store.py` — Telemetry store mutation/reset tests

### Legacy Decommission/Delete Scope

- Delete `_SEARCH_OBJECT_VIEW_REGISTRY` from `tools/cq/search/pipeline/search_object_view_store.py`
- Delete `_LANGUAGE_ADAPTERS` from `tools/cq/search/enrichment/language_registry.py`
- Delete `_TELEMETRY` and `_SEEN_KEYS` from `tools/cq/core/cache/telemetry.py`
- Delete `_BACKEND_STATE` from `tools/cq/core/cache/backend_lifecycle.py`
- Delete module-global `_AST_CACHE` instances from `tools/cq/search/python/extractors.py` and `tools/cq/search/rust/enrichment.py`
- Delete mutable dict exposure of `DETAILS_KIND_REGISTRY` in `tools/cq/core/details_kinds.py`

---

## S22. Typed Summary and Macro Payload Contracts

### Goal

Replace high-traffic summary/update `dict[str, object]` payloads in query and macro paths with typed `msgspec.Struct` contracts to eliminate key-shape drift and reduce command/query mutation leakage.

### Representative Code Snippets

```python
# tools/cq/core/summary_update_contracts.py
from __future__ import annotations

from tools.cq.core.structs import CqStruct


class EntitySummaryUpdateV1(CqStruct, frozen=True, kw_only=True):
    matches: int
    total_defs: int
    total_calls: int
    total_imports: int
```

```python
# tools/cq/query/executor_runtime_impl.py — apply typed updates via deterministic builtins
def _entity_summary_updates(result: CqResult) -> EntitySummaryUpdateV1:
    summary = as_search_summary(result.summary)
    return EntitySummaryUpdateV1(
        matches=summary.matches,
        total_defs=summary.total_defs,
        total_calls=summary.total_calls,
        total_imports=summary.total_imports,
    )

summary = apply_summary_mapping(
    summary,
    msgspec.to_builtins(_entity_summary_updates(result), order="deterministic"),
)
```

### Files to Edit

- `tools/cq/core/summary_contract.py` — Accept typed summary-update structs at boundary points
- `tools/cq/core/scoring.py` — Accept `ScoringDetailsV1` directly in `build_detail_payload`
- `tools/cq/macros/impact.py` — Replace dict summary builders with typed summary contracts
- `tools/cq/macros/calls/entry.py` — Replace `_build_calls_summary()` dict return with typed contract
- `tools/cq/macros/calls/semantic.py` — Replace mutable semantic payload dict merges with typed summary payload contract
- `tools/cq/query/executor_runtime_impl.py` — Replace `dict[str, object]` summary update payloads with typed structs
- `tools/cq/query/executor_definitions.py` — Replace summary update dicts with typed structs

### New Files to Create

- `tools/cq/core/summary_update_contracts.py` — Typed summary update contract structs for query and macro paths
- `tests/unit/cq/core/test_summary_update_contracts.py` — Contract encode/decode and deterministic serialization tests
- `tests/unit/cq/macros/test_typed_summary_payloads.py` — Macro summary payload typing tests
- `tests/unit/cq/query/test_typed_summary_updates.py` — Query summary update typing tests

### Legacy Decommission/Delete Scope

- Delete dict-returning `_build_impact_summary()` from `tools/cq/macros/impact.py`
- Delete dict-returning `_build_calls_summary()` from `tools/cq/macros/calls/entry.py`
- Delete `msgspec.structs.asdict` scattering used only to feed summary dicts in macros where typed contracts supersede it
- Delete dict-returning `_entity_summary_updates()` and related dict update helpers from `tools/cq/query/executor_runtime_impl.py`

---

## S23. Query Testability and Rulepack Registry Injection

### Goal

Eliminate hidden construction in query execution paths by injecting enrichers and rulepack providers through explicit context contracts.

### Representative Code Snippets

```python
# tools/cq/query/enrichment.py — injectable source reader for SymtableEnricher
from collections.abc import Callable
from pathlib import Path

type SourceReader = Callable[[Path], str]


class SymtableEnricher:
    def __init__(self, root: Path, *, source_reader: SourceReader | None = None) -> None:
        self._root = root
        self._source_reader = source_reader or (lambda p: p.read_text(encoding="utf-8"))
```

```python
# tools/cq/astgrep/rulepack_registry.py
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from tools.cq.astgrep.sgpy_scanner import RuleSpec


@dataclass
class RulePackRegistry:
    _cache: dict[str, tuple[RuleSpec, ...]] = field(default_factory=dict)

    def load_default(self, base: Path) -> dict[str, tuple[RuleSpec, ...]]:
        if self._cache:
            return dict(self._cache)
        self._cache = _load_rulepacks_uncached(base)
        return dict(self._cache)
```

### Files to Edit

- `tools/cq/query/enrichment.py` — Add `source_reader` injection seam to `SymtableEnricher`
- `tools/cq/query/execution_context.py` — Require injected `symtable_enricher` in execution context composition
- `tools/cq/query/executor_runtime_impl.py` — Stop inline `SymtableEnricher(root)` construction; use injected dependency
- `tools/cq/query/executor_definitions.py` — Stop inline `SymtableEnricher(root)` construction; use injected dependency
- `tools/cq/query/section_builders.py` — Stop inline enricher construction; accept injected enricher/context
- `tools/cq/query/batch.py` — Compose injected enricher at boundary
- `tools/cq/astgrep/rulepack_loader.py` — Remove module-level `@lru_cache` singleton entrypoint
- `tools/cq/astgrep/rules.py` — Resolve rulepacks via injected registry instance

### New Files to Create

- `tools/cq/astgrep/rulepack_registry.py` — Injectable rulepack registry object replacing singleton loader behavior
- `tests/unit/cq/query/test_symtable_enricher_injection.py` — Query tests with fake source reader
- `tests/unit/cq/astgrep/test_rulepack_registry.py` — Rulepack registry caching/reset tests

### Legacy Decommission/Delete Scope

- Delete `@lru_cache(maxsize=1)` `load_default_rulepacks()` singleton pattern from `tools/cq/astgrep/rulepack_loader.py`
- Delete `clear_rulepack_cache()` test-only escape hatch from `tools/cq/astgrep/rulepack_loader.py`
- Delete inline `SymtableEnricher(root)` construction sites in query executor modules

---

## S24. CLI Telemetry Decomposition and Schema Alignment

### Goal

Simplify CLI telemetry control flow by extracting event-builder helpers and align neighborhood command params with the schema-driven command pattern used elsewhere.

### Representative Code Snippets

```python
# tools/cq/cli_app/telemetry_events.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CqInvokeEvent:
    ok: bool
    command: str | None
    parse_ms: float
    exec_ms: float
    exit_code: int
    error_class: str | None = None
    error_stage: str | None = None
    event_id: str | None = None
    event_uuid_version: int | None = None
    event_created_ms: int | None = None


def build_invoke_event(
    *,
    ok: bool,
    command: str | None,
    parse_ms: float,
    exec_ms: float,
    exit_code: int,
    error_class: str | None = None,
    error_stage: str | None = None,
    event_id: str | None = None,
    event_uuid_version: int | None = None,
    event_created_ms: int | None = None,
) -> CqInvokeEvent:
    return CqInvokeEvent(
        ok=ok,
        command=command,
        parse_ms=parse_ms,
        exec_ms=exec_ms,
        exit_code=exit_code,
        error_class=error_class,
        error_stage=error_stage,
        event_id=event_id,
        event_uuid_version=event_uuid_version,
        event_created_ms=event_created_ms,
    )
```

```python
# tools/cq/cli_app/command_schema.py — neighborhood command adopts schema-driven pattern
class NeighborhoodCommandSchema(CqStruct, frozen=True, kw_only=True):
    target: str
    lang: str = "python"
    top_k: int = 10
    no_lsp: bool = False
```

### Files to Edit

- `tools/cq/cli_app/telemetry.py` — Replace nested duplicated event construction with extracted event builder helper
- `tools/cq/cli_app/command_schema.py` — Add `NeighborhoodCommandSchema`
- `tools/cq/cli_app/params.py` — Align `NeighborhoodParams` generation with schema-driven pattern
- `tools/cq/cli_app/schema_projection.py` — Project neighborhood params via generic projection helpers
- `tools/cq/cli_app/context.py` — Thread any additional schema/config dependencies cleanly through context

### New Files to Create

- `tools/cq/cli_app/telemetry_events.py` — Shared telemetry event-builder helpers
- `tests/unit/cq/cli_app/test_telemetry_event_builder.py` — Event-builder behavior tests
- `tests/unit/cq/cli_app/test_neighborhood_schema_projection.py` — Neighborhood schema projection tests

### Legacy Decommission/Delete Scope

- Delete duplicated inline `CqInvokeEvent(...)` construction branches from `tools/cq/cli_app/telemetry.py`
- Delete neighborhood-specific parameter divergence in `tools/cq/cli_app/params.py` once schema-driven generation is in place

---

## S25. Fragment Cache Simplification and Search Artifact Index Extraction

### Goal

Reduce cache-layer complexity by collapsing fragment orchestration layers and extracting search artifact index/deque coordination into a focused module.

### Representative Code Snippets

```python
# tools/cq/core/cache/fragment_runtime.py
from __future__ import annotations

from collections.abc import Callable

from tools.cq.core.cache.interface import CqCacheBackend


def probe_or_persist_fragment(
    *,
    backend: CqCacheBackend,
    cache_key: str,
    compute: Callable[[], bytes],
    ttl_seconds: int,
) -> bytes:
    payload = backend.get(cache_key)
    if isinstance(payload, (bytes, bytearray, memoryview)):
        return bytes(payload)
    data = compute()
    backend.set(cache_key, data, expire=ttl_seconds)
    return data
```

```python
# tools/cq/core/cache/search_artifact_index.py
from __future__ import annotations

from pathlib import Path

from tools.cq.core.cache.policy import CqCachePolicyV1


def run_index_path(policy: CqCachePolicyV1, run_id: str) -> Path:
    return Path(policy.directory).expanduser() / "stores" / "search_artifacts" / "index" / f"run_{run_id}"
```

### Files to Edit

- `tools/cq/core/cache/fragment_engine.py` — Move probe/persist runtime orchestration into `fragment_runtime.py`
- `tools/cq/core/cache/fragment_orchestrator.py` — Remove duplicate orchestration layer and migrate callsites
- `tools/cq/core/cache/search_artifact_store.py` — Delegate index/deque path and index iteration logic to `search_artifact_index.py`
- `tools/cq/core/cache/__init__.py` — Export canonical cache runtime/index modules

### New Files to Create

- `tools/cq/core/cache/fragment_runtime.py` — Unified fragment probe/persist runtime helper
- `tools/cq/core/cache/search_artifact_index.py` — Search artifact index/deque paths and iteration helpers
- `tests/unit/cq/core/cache/test_fragment_runtime.py` — Fragment runtime behavior tests
- `tests/unit/cq/core/cache/test_search_artifact_index.py` — Artifact index helper tests

### Legacy Decommission/Delete Scope

- Delete `tools/cq/core/cache/fragment_orchestrator.py` after migrating all callsites to `fragment_runtime.py`
- Delete duplicated index/deque path helpers from `tools/cq/core/cache/search_artifact_store.py`
- Delete duplicate fragment probe/persist wrappers superseded by `probe_or_persist_fragment`

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
- Delete `tools/cq/query/executor_runtime_impl.py` after S7 callsite migration to focused execution modules
- Delete `tools/cq/search/tree_sitter/rust_lane/runtime_core.py` after S6 callsite migration to extracted runtime modules
- Delete `tools/cq/search/python/extractors.py` after S8 callsite migration to extracted orchestration modules

### Batch D4 (after S3, S11, S20)

- Delete all trivial re-export modules in `_shared/`: `timeout.py`, `rg_request.py`, `encoding.py` — S3 consolidation
- Delete `generate_followup_suggestions` compatibility alias from `smart_search_followups.py` — S20/S3 cleanup
- Delete file-scan-visit boilerplate from 6 macros — replaced by `scan_python_files` in S11

### Batch D5 (after S9)

- Delete `_partition_python_payload_fields` and ~20 defensive coercion helpers — typed facts make them unnecessary
- Delete `empty_enrichment_telemetry()` dict factory — replaced by `EnrichmentTelemetryV1` struct

### Batch D6 (after S10)

- Delete module-level `_SESSIONS`, `_GLOBAL_STATE_HOLDER`, `_TREE_CACHE_EVICTIONS` singletons — replaced by `TreeSitterRuntimeContext`
- Remove import-time `CACHE_REGISTRY` registrations — moved to lazy initialization

### Batch D7 (after S13, S14, S15, S16)

- Delete `tools/cq/search/tree_sitter/python_lane/facts.py` after S13 callsite migration
- Delete `tools/cq/search/tree_sitter/python_lane/runtime.py` after S13 callsite migration
- Delete `tools/cq/search/tree_sitter/core/runtime.py` after S13 callsite migration
- Delete `tools/cq/neighborhood/tree_sitter_collector.py` after S14 callsite migration
- Delete `tools/cq/orchestration/multilang_orchestrator.py` after S14 callsite migration
- Delete `tools/cq/core/cache/diskcache_backend.py` after S15 callsite migration
- Delete `tools/cq/core/summary_contract.py` after S15 callsite migration
- Delete `tools/cq/core/enrichment_facts.py` after S15 callsite migration
- Delete `tools/cq/macros/calls_target.py` after S16 callsite migration

### Batch D8 (after S17)

- Delete `ExceptionVisitor` class from `tools/cq/macros/exceptions.py` — relocated to `analysis/visitors/`
- Delete `SideEffectVisitor` class from `tools/cq/macros/side_effects.py` — relocated to `analysis/visitors/`
- Delete `ImportVisitor` class from `tools/cq/macros/imports.py` — relocated to `analysis/visitors/`

### Batch D9 (after S20, S22, S24)

- Delete lazy export facade in `tools/cq/search/pipeline/__init__.py` (`Any=None` sentinels + `__getattr__` indirection)
- Delete facade exports in `tools/cq/query/__init__.py` that no longer map to canonical modules
- Delete dict-based summary update helper functions superseded by typed `summary_update_contracts` structs
- Delete duplicated inline telemetry event construction branches in `tools/cq/cli_app/telemetry.py`

### Batch D10 (after S21, S23, S25)

- Delete mutable registry globals `_SEARCH_OBJECT_VIEW_REGISTRY`, `_LANGUAGE_ADAPTERS`, `_TELEMETRY`, `_SEEN_KEYS`, `_BACKEND_STATE`
- Delete `@lru_cache(maxsize=1)` rulepack singleton + `clear_rulepack_cache()` helper from `tools/cq/astgrep/rulepack_loader.py`
- Delete `tools/cq/core/cache/fragment_orchestrator.py` after callsite migration to `fragment_runtime.py`
- Delete duplicate search artifact index/deque helper functions from `tools/cq/core/cache/search_artifact_store.py` after `search_artifact_index.py` migration

---

## Implementation Sequence

1. **S1. Frozen Struct Integrity** — Foundation for all contract/copy-on-write work.
2. **S5. CQS Violations and Public Surface Cleanup** — Low-risk quality improvements that unblock later typing/decomposition.
3. **S3. DRY Consolidation — Small Wins** — Reduce duplication before deep structural edits.
4. **S2. Dependency Direction Corrections** — Unblocks clean layering for S4/S9/S12.
5. **S4. Telemetry Accumulation Consolidation** — Depends on S2/S3 helper and boundary moves.
6. **S20. Facade and Alias Eradication** — Enforce hard-cutover import surfaces before major module movement.
7. **S11. Shell-Layer Domain Logic Extraction** — Move domain logic inward to stabilize shell boundaries.
8. **S12. Missing Abstraction Introduction** — Introduce provider/cache protocols and registry seams needed by decomposition.
9. **S6. God Module Decomposition — Tree-Sitter** — First major decomposition in tree-sitter lane.
10. **S7. God Module Decomposition — Query Engine** — Parallel major decomposition in query lane.
11. **S8. God Module Decomposition — Search Backends and Core** — Decompose remaining high-fan-in search/core modules.
12. **S9. Typed Enrichment Payload Migration** — Highest-impact contract migration after decomposition boundaries are in place.
13. **S22. Typed Summary and Macro Payload Contracts** — Extend typing discipline from enrichment payloads to summary/update payloads.
14. **S23. Query Testability and Rulepack Registry Injection** — Remove hidden construction and singleton loaders in query/astgrep.
15. **S10. Mutable Singleton Elimination** — Tree-sitter/CLI singleton migration using newly-extracted seams.
16. **S21. Mutable Singleton Elimination Completion and Registry Hardening** — Finish singleton cleanup across remaining registries/caches.
17. **S24. CLI Telemetry Decomposition and Schema Alignment** — Simplify telemetry complexity and normalize neighborhood schema flow.
18. **S18. Quick Wins — Naming and Contract Fixes** — Safe consistency fixes after primary structural movement.
19. **S13. God Module Decomposition — Tree-Sitter Lanes** — Follow-on lane decomposition after S6 pattern is established.
20. **S14. God Module Decomposition — CLI + Orchestration** — Complete shell-layer decomposition after S11 extraction.
21. **S15. God Module Decomposition — Core Foundation** — Decompose core cache/summary/fact modules after typed contracts settle.
22. **S16. God Module Decomposition — Macros + Query Periphery** — Complete remaining macro/query decomposition.
23. **S17. Domain Visitor Extraction** — Move visitors after macro decomposition reduces merge risk.
24. **S25. Fragment Cache Simplification and Search Artifact Index Extraction** — Collapse residual cache-layer over-abstraction and split index concerns.
25. **S19. Observability — Structured Logging** — Apply logging to final module topology last.

## Implementation Progress Audit (2026-02-18, refreshed after codebase re-audit)

### Status Legend

- `Complete` — scope is fully landed per this plan.
- `Partial` — at least one material slice is landed, but scope is not yet complete.
- `Not Started` — no material implementation landed yet.

### Scope Status Snapshot

| Scope | Status | Notes |
|---|---|---|
| S1 | Complete | Frozen `Section`, tuple findings, copy-on-write report enrichment, macro struct freezing, `RunContext.argv` tuple migration, and SNB node-index hardening (`freeze_node_index` + immutable mapping at bundle construction) are all landed. |
| S2 | Complete | Core dependency direction corrections are landed: core no longer imports search/query/macro/orchestration implementation modules directly, summary-contract serialization is wired through core-owned seams, and semantic front-door provider execution is registry-driven through injected boundaries. |
| S3 | Partial | Counter/provenance/AST-parent dedup slices plus scope-filter wrapper consolidation landed; render-summary status derivation is consolidated, and `_pack_source_rows` dedup now uses canonical planner helper (`resolve_pack_source_rows`). Remaining DRY tail work is in broader module decomposition and residual helper duplication. |
| S4 | Complete | Canonical enrichment telemetry accumulation module is live and duplicate adapter/pipeline accumulation functions are removed. |
| S5 | Complete | CQS/public-surface cleanups are landed: `build_call_evidence` is pure, result-action is split into prepare/emit phases, summary contract rename is in place, symbol-resolver query/command split is landed, runtime guard asserts were replaced, and payload-budget flow is split into check/trim helpers. |
| S6 | Partial | Rust-lane extracted module surfaces are expanded (`runtime_query_execution.py`, `runtime_payload_builders.py`, `runtime_pipeline_stages.py`) and query/pipeline wrappers now route through them; remaining work is full ownership transfer out of the still-large `runtime_engine.py`. |
| S7 | Partial | Query facade modules and `executor_runtime_impl.py` are decommissioned; decomposition advanced with extracted entity/pattern/summary runtime surfaces (`executor_runtime_entity.py`, `executor_runtime_pattern.py`, `executor_runtime_summary.py`) and legacy `executor_entity_impl.py`/`executor_pattern_impl.py` alias modules removed. Remaining work is deeper extraction from still-large `executor_runtime.py`. |
| S8 | Partial | Search/core decomposition advanced with `front_door_dispatch.py`, extracted Python fact merge/normalize helpers, and calls-analysis scan wrappers; remaining work is deeper extraction from large ownership modules (`extractors_entrypoints.py`, `front_door_assembly.py`, `calls/analysis.py`). |
| S9 | Partial | Wrapper contracts and language adapters now use typed fact payloads, but downstream consumers still retain dict-centric enrichment handling in key paths (`semantic/models.py`, `objects/resolve.py`, lane payload canonicalization), so typed-fact propagation is not fully end-to-end yet. |
| S10 | Complete | Process-default singleton state-holder dict wrappers are eliminated from targeted runtime/registry/cache modules in favor of typed module-level singleton refs with lock-guarded getters/setters, and bootstrap reset hooks clear these defaults deterministically. |
| S11 | Complete | `apply_in_dir_scope()`, query/search routing extraction, result-action command/query split, and shared macro scan helper adoption are landed across planned command/macro surfaces. |
| S12 | Complete | Missing abstraction introduction is landed for planned boundaries: `LanguageEnrichmentProvider` protocol + registry wiring are canonical, and semantic provider execution now flows through these abstraction seams instead of direct concrete coupling. |
| S13 | Complete | Tree-sitter lane decomposition modules are in place and legacy god modules (`python_lane/facts.py`, `python_lane/runtime.py`, `core/runtime.py`) are decommissioned. |
| S14 | Complete | Collector/orchestration decomposition is landed and legacy modules (`neighborhood/tree_sitter_collector.py`, `orchestration/multilang_orchestrator.py`) are decommissioned. |
| S15 | Complete | Core foundation split modules are canonical and legacy modules (`diskcache_backend.py`, `summary_contract.py`, `enrichment_facts.py`) are decommissioned. |
| S16 | Partial | Periphery decomposition advanced with extracted modules (`executor_definitions_imports.py`, `executor_definitions_findings.py`, `entry_summary.py`, `entry_command.py`) and callsites migrated to new canonical imports; remaining work is further extraction from heavy `macros/calls/entry.py` and `query/executor_runtime.py`/`query/executor_definitions.py`. |
| S17 | Complete | Domain visitors moved to `analysis/visitors/` and removed from macro modules. |
| S18 | Complete | Quick-win contract/naming fixes are landed across the listed targets (`TreeSitterInputEditV1`, `RgProcessResultV1`, `AstAnchor`/`DefinitionSite`, frozen capability matrix, neighborhood schema/params alignment, and `compile_query()` param cleanup). |
| S19 | Complete | Structured logging is landed across planned pipeline surfaces and macro/enrichment lanes, including `search/rg/adapter.py` timeout/branch/row-count instrumentation and key pipeline stage entry/exit/degradation logging. |
| S20 | Complete | Pipeline lazy facade and compatibility aliases removed; query facade modules were deleted and rust-lane private indirection was removed. |
| S21 | Complete | Registry/context hardening is complete for the planned singleton slice: targeted registries/runtime contexts now use typed module-level singleton refs, lock-guarded accessors, and deterministic bootstrap reset seams. |
| S22 | Complete | Typed summary and macro payload contracts are landed for planned paths: summary updates flow through typed contracts (`SummaryUpdateV1` union, `set_summary_update()`, `summary_update_mapping()`) in query runtime and macro entry paths without fallback dict-mapping helpers. |
| S23 | Complete | Rulepack registry seam is canonical (`load_default_rulepacks()` removed) and execution entry paths now consume injected `symtable_enricher` dependencies instead of inline `SymtableEnricher(...)` construction. |
| S24 | Complete | CLI telemetry event construction is centralized via shared helpers and neighborhood command schema/params now follow the same schema-projection pattern as the rest of the CLI surface. |
| S25 | Complete | Fragment/runtime and search-artifact index consolidation are landed: fragment probe/persist ownership is runtime-scoped, and run/global index row iteration + decode helpers are owned by `search_artifact_index.py` with store/engine consuming canonical helpers. |

### Decommission Batch Status Snapshot

| Batch | Status | Notes |
|---|---|---|
| D1 | Complete | `_apply_render_enrichment_in_place` removed; `render_summary_compact` alias removed. |
| D2 | Complete | `pipeline/enrichment_contracts.py` deleted and telemetry accumulation duplicates consolidated under `search/enrichment/telemetry.py`. |
| D3 | Complete | Query facade cleanup and hard deletions are complete: `executor_runtime_impl.py`, `rust_lane/runtime_core.py`, and `search/python/extractors.py` are decommissioned with imports migrated to canonical modules. |
| D4 | Complete | Follow-up alias deletion is complete and `_shared` wrapper re-export files (`timeout.py`, `rg_request.py`, `encoding.py`) are deleted. |
| D5 | Partial | Major typed-payload helper cleanup is landed (obsolete coercion helpers removed and `extractors_entrypoints.py` merged through typed helpers), but remaining semantic/object-view dict helper paths (notably in `semantic/models.py` and `objects/resolve.py`) keep this batch open. |
| D6 | Complete | `_SESSIONS`, `_GLOBAL_STATE_HOLDER`, and `_TREE_CACHE_EVICTIONS` globals are removed; cache clear/cache registration is now lazy via `ensure_*` paths in runtime/enrichment modules (`runtime_cache.py`, `query_cache.py`, `python/extractors_entrypoints.py`, `rust/enrichment.py`) rather than import-time side effects. |
| D7 | Complete | S13/S14/S15/S16 deletion tranche is executed for planned hard-cut modules (`python_lane/facts.py`, `python_lane/runtime.py`, `core/runtime.py`, `tree_sitter_collector.py`, `multilang_orchestrator.py`, `diskcache_backend.py`, `summary_contract.py`, `enrichment_facts.py`, `calls_target.py`). |
| D8 | Complete | `ExceptionVisitor`, `SideEffectVisitor`, and `ImportVisitor` classes removed from macro modules after relocation to `analysis/visitors`. |
| D9 | Partial | Alias/facade cleanup is advanced (including query runtime alias-module removal and typed summary-update helpers), but duplicated inline telemetry event construction branches remain in `cli_app/telemetry.py`, so final D9 closure is pending. |
| D10 | Complete | `fragment_orchestrator.py` deletion is complete; singleton-state holder dict patterns are fully removed from targeted modules, and search-artifact index/store consolidation moved index/deque ownership into canonical `search_artifact_index.py` helpers. |

### Quality-Check Tracking

Quality-check failure tracking is intentionally omitted from this scope-status audit. This section is maintained separately from implementation-scope progress.

---

## Implementation Checklist

- [x] S1. Frozen struct integrity (frozen `Section`, COW enrichment flow, macro struct freezing, and SNB node-index immutability hardening landed)
- [x] S5. CQS violations and public surface cleanup (call evidence purity, result-action split, symbol-resolver split, runtime-guard cleanup, and payload-budget check/trim split landed)
- [ ] S3. DRY consolidation — small wins (partial: counter/builtin/AST-parent dedup plus scope-filter wrapper consolidation landed; render-summary status-derivation dedup and canonical `_pack_source_rows` helper path are now landed; remaining dedup set pending)
- [x] S2. Dependency direction corrections (core dependency inversion + summary serializer seam + semantic provider-registry wiring landed with registry-driven semantic provider execution)
- [x] S4. Telemetry accumulation consolidation
- [x] S20. Facade and alias eradication
- [x] S11. Shell-layer domain logic extraction
- [x] S12. Missing abstraction introduction (LanguageEnrichmentProvider protocol + registry seam are canonical)
- [ ] S6. God module decomposition — tree-sitter (runtime_core hard-cut completed and extracted runtime surfaces (`runtime_query_execution.py`, `runtime_payload_builders.py`, `runtime_pipeline_stages.py`) are landed; full ownership transfer from `runtime_engine.py` remains)
- [ ] S7. God module decomposition — query engine (facade + runtime_impl hard-cut completed, `executor_runtime_entity.py`/`executor_runtime_pattern.py`/`executor_runtime_summary.py` extracted, and alias modules removed; deeper split of `executor_runtime.py` remains)
- [ ] S8. God module decomposition — search backends and core (hard-cut from `extractors.py` completed; decomposition advanced with `front_door_dispatch.py`, `extractors_fact_merge.py`/`extractors_fact_normalize.py`, and calls-analysis scan wrappers; deeper split of ownership-heavy modules remains)
- [ ] S9. Typed enrichment payload migration (partial: typed wrappers/adapters are landed; downstream dict-centric enrichment handling remains in semantic/object-view flows)
- [x] S22. Typed summary and macro payload contracts (typed summary-update boundaries are centralized and applied across query/calls/impact flows)
- [x] S23. Query testability and rulepack registry injection (registry-only rulepack loading landed; inline execution-path `SymtableEnricher(...)` construction removed)
- [x] S10. Mutable singleton elimination (process-default state-holder dict wrappers removed in targeted modules; typed singleton refs + lock-guarded accessors are canonical)
- [x] S21. Mutable singleton elimination completion and registry hardening (registry/runtime context singleton hardening + bootstrap reset seams are landed for planned targets)
- [x] S24. CLI telemetry decomposition and schema alignment (telemetry event helper consolidation + neighborhood schema-projection alignment landed)
- [x] S18. Quick wins — naming and contract fixes
- [x] S13. God module decomposition — tree-sitter lanes (facts.py, runtime.py, core/runtime.py hard-cut completed)
- [x] S14. God module decomposition — CLI + orchestration (tree_sitter_collector.py, multilang_orchestrator.py hard-cut completed)
- [x] S15. God module decomposition — core foundation (diskcache_backend.py, summary_contract.py, enrichment_facts.py hard-cut completed)
- [ ] S16. God module decomposition — macros + query periphery (calls-target hard-cut plus extracted `entry_summary.py`/`entry_command.py` and `executor_definitions_imports.py`/`executor_definitions_findings.py` landed; deeper split of `entry.py` and runtime-heavy query modules remains)
- [x] S17. Domain visitor extraction to analysis/ (ExceptionVisitor, SideEffectVisitor, ImportVisitor)
- [x] S25. Fragment cache simplification and search artifact index extraction (fragment/runtime boundary and search artifact index ownership consolidation are landed)
- [x] S19. Observability — structured logging (planned pipeline/macro/enrichment logging coverage and `search/rg/adapter.py` instrumentation are landed)
- [x] D1. Cross-scope deletion batch (after S1, S5)
- [x] D2. Cross-scope deletion batch (after S2, S4)
- [x] D3. Cross-scope deletion batch (query facades + runtime_core/extractors/runtime_impl hard deletions completed)
- [x] D4. Cross-scope deletion batch (`_shared` wrapper modules deleted and followups alias cleanup complete)
- [ ] D5. Cross-scope deletion batch (partial: major typed-payload helper cleanup is landed; residual semantic/object-view dict-helper paths remain)
- [x] D6. Cross-scope deletion batch (singleton globals removed and runtime/enrichment cache callback registrations converted to lazy `ensure_*` paths)
- [x] D7. Cross-scope deletion batch (planned hard-cut module deletions completed)
- [x] D8. Cross-scope deletion batch (after S17)
- [ ] D9. Cross-scope deletion batch (partial: alias cleanup + typed-summary slices landed; duplicated inline telemetry event construction branches remain)
- [x] D10. Cross-scope deletion batch (singleton-state holder dict removal + search-artifact index consolidation closure are landed)
