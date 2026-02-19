# Extraction Pipeline Cleanup Implementation Plan v1 (2026-02-18)

## Scope Summary

Address the 8 highest-priority findings from the extraction pipeline design review
(`docs/reviews/design_review_extraction_pipeline_2026-02-18.md`) and integrate the
approved DataFusion-native expansion scope captured in
`docs/plans/extraction_pipeline_cleanup_plan_review_and_datafusion_alignment_v1_2026-02-19.md`.
Scope includes extraction orchestration plus DataFusion planning/runtime surfaces across:
`src/extract/`, `src/extraction/`, `src/datafusion_engine/`, `rust/datafusion_python/`,
`rust/codeanatomy_engine/src/session/`, `rust/datafusion_ext/`, and supporting test files.
Design stance: hard cutover with no compatibility shims.

Public API constraint:
- Preserve extractor entrypoint signatures (`extract_ast()`, `extract_cst()`, etc.).
- Preserve `run_extraction(request: RunExtractionRequestV1)` request-envelope entrypoint;
  injection seams are introduced via request fields or lower-level helpers, not a
  positional-argument API rewrite.

Key goals:
- Eliminate ~1,500 LOC of structural duplication across extractor entry points (P7)
- Unify dual span types `SpanSpec` / `SpanTemplateSpec` into a single canonical type (P7)
- Complete the `execution_bundle` migration and remove `inspect.signature` reflection (P6)
- Add injection seams for module-level `@cache` singletons to enable test isolation (P23)
- Separate pure domain logic from cache management in builder files (P16/P23)
- Replace bespoke runtime-cache wiring with DataFusion-native cache-manager surfaces (P7/P8)
- Promote builder-native planning composition as the canonical cross-language path (P12/P22)
- Enforce DF52 schema-forward pushdown contracts (`TableSchema`, `FileSource` hooks, adapter factory) (P8/P18)
- Standardize plan identity and environment snapshots (`df_settings`, info-schema hash, plan identity hash) (P18/P24)
- Rationalize custom optimizer/extension surfaces against DF52 ordering and safety constraints (P19/P22)

## Design Principles

1. **No compatibility shims** — `SpanSpec` is deleted, not deprecated. All callers migrate in one pass.
2. **Preserve stable public API contracts** — `extract_*` signatures are unchanged and `run_extraction` remains request-envelope based.
3. **Injection over patching** — Module-level singletons replaced with optional parameters defaulting to current behavior.
4. **Pure core, imperative shell** — Cache management wraps pure functions, not the reverse.
5. **DataFusion-native first** — Prefer DataFusion planning/runtime objects and extension points over bespoke orchestration when feature-equivalent.
6. **Single planning contract across languages** — Python and Rust settings/policy surfaces must be hashable and parity-checkable.
7. **Determinism by construction** — Plan/run artifacts must include stable environment and policy snapshots needed for replay/diff.

## Current Baseline

- `SpanSpec` defined at `src/extract/coordination/context.py:174-184` with 8 fields (all optional except first 6 positional).
- `SpanTemplateSpec` defined at `src/extract/row_builder.py:20-35` with identical 8 fields (all keyword with defaults).
- `span_dict()` adapter at `context.py:220-244` converts `SpanSpec` → `SpanTemplateSpec` field-by-field, then delegates to `make_span_spec_dict()`.
- `SpanSpec` imported by: `ast/builders.py`, `bytecode/builders.py`, `cst/builders.py`, `symtable_extract.py`, and `src/test_support/datafusion_ext_stub.py:683`.
- The runtime dispatch block (normalize options → ensure session → ensure runtime profile → determinism tier → normalize → collect rows → build plan → materialize) appears identically in:
  - `ast/builders.py:926-966` (`extract_ast`) and `ast/builders.py:967+` (`extract_ast_plans`)
  - `bytecode/builders.py:1563-1607` (`extract_bytecode`) and `bytecode/builders.py:1610+` (`extract_bytecode_plans`)
  - `cst/builders.py:1517-1583` (`extract_cst`) and `cst/builders.py:1586+` (`extract_cst_plans`)
  - `symtable_extract.py:759-806` (`extract_symtable`) and `symtable_extract.py:809+` (`extract_symtable_plans`)
- `run_extraction` currently requires a request envelope (`src/extraction/orchestrator.py:83`) with contract defined in `src/extraction/contracts.py:17`.
- `inspect.signature` reflection at `orchestrator.py:230,342,378` checks if `execution_bundle` exists in function parameters, despite all three target functions (`_run_repo_scan:457`, `_run_python_imports:819`, `_run_python_external:867`) already accepting it.
- `@cache` on `_delta_write_ctx()` at `orchestrator.py:193-198` creates an unresetable module-level singleton.
- `@cache` on `_feature_flag_rows()` and `_metadata_defaults()` at `spec_helpers.py:43-54` cache based on template name with no clear invalidation.
- `_extract_ast_for_context` at `ast/builders.py:857-914` interleaves cache lookup/lock/store with pure AST parsing logic.
- `_build_extract_execution_bundle()` at `orchestrator.py:175-190` is a zero-argument factory with no injection seam.
- Python runtime-policy bridge uses bespoke parsing for `datafusion.runtime.*` keys in `delta_session_builder.py` (`build_runtime_policy_options` + `_apply_cache_runtime_policy_settings` + `_runtime_policy_rules`) while config application path skips those keys (`context_pool.py:50`).
- Rust Python bridge already exposes DF52 cache-manager wiring via `CacheManagerConfig` (`rust/datafusion_python/src/codeanatomy_ext/delta_session_bridge.rs:104-125`).
- Rust runtime profile coverage currently marks `meta_fetch_concurrency`, `list_files_cache_*`, and `metadata_cache_limit` as reserved (`rust/codeanatomy_engine/src/session/profile_coverage.rs:67,141-152`) despite first-class DF52 cache/runtime knobs.
- Planning-surface composition primitives exist in Rust (`PlanningSurfaceSpec` + `apply_to_builder` at `rust/codeanatomy_engine/src/session/planning_surface.rs:43-131`) but parity is not yet fully contract-enforced across Python/Rust.
- `CodeAnatomyRelationPlanner` currently delegates all relations unchanged (`rust/datafusion_ext/src/relation_planner.rs:13-20`), and custom physical rule wraps plans with `CoalescePartitionsExec` (`rust/datafusion_ext/src/physical_rules.rs:118`), requiring explicit governance against DF52 rule ordering.

---

## S1. Unify Span Types (P7, P1, P10)

### Goal

Eliminate the dual `SpanSpec` / `SpanTemplateSpec` representation by making `SpanTemplateSpec` the single canonical span specification type. Delete `SpanSpec` and the adapter `span_dict()` from `context.py`.

### Representative Code Snippets

```python
# src/extract/extractors/ast/builders.py — BEFORE
from extract.coordination.context import SpanSpec, span_dict

def _span_spec_from_node(node: ast.AST, *, line_offsets: LineOffsets | None) -> SpanSpec:
    return SpanSpec(
        start_line0=node.lineno - 1,
        start_col=node.col_offset,
        end_line0=getattr(node, "end_lineno", node.lineno) - 1,
        end_col=getattr(node, "end_col_offset", node.col_offset),
        end_exclusive=True,
        col_unit="utf8",
        byte_start=...,
        byte_len=...,
    )

# src/extract/extractors/ast/builders.py — AFTER
from extract.row_builder import SpanTemplateSpec, make_span_spec_dict

def _span_spec_from_node(node: ast.AST, *, line_offsets: LineOffsets | None) -> SpanTemplateSpec:
    return SpanTemplateSpec(
        start_line0=node.lineno - 1,
        start_col=node.col_offset,
        end_line0=getattr(node, "end_lineno", node.lineno) - 1,
        end_col=getattr(node, "end_col_offset", node.col_offset),
        end_exclusive=True,
        col_unit="utf8",
        byte_start=...,
        byte_len=...,
    )
```

```python
# src/extract/coordination/context.py — AFTER (hard cutover, no shim)
# Remove SpanSpec class entirely.
# Remove span_dict() entirely.
# Keep only canonical span construction via SpanTemplateSpec + make_span_spec_dict.
```

### Files to Edit

- `src/extract/coordination/context.py` — delete `SpanSpec` class (lines 173-184), delete `span_dict()` function (lines 220-244), remove from `__all__`
- `src/extract/coordination/__init__.py` — remove `SpanSpec` and `span_dict` from imports and `__all__`; add `SpanTemplateSpec` and `make_span_spec_dict` re-exports from `row_builder`
- `src/extract/__init__.py` — remove `SpanSpec` / `span_dict` from `_EXPORTS` dict; add `SpanTemplateSpec` / `make_span_spec_dict` if needed
- `src/extract/extractors/ast/builders.py` — change `SpanSpec` → `SpanTemplateSpec`, `span_dict()` → `make_span_spec_dict()` throughout
- `src/extract/extractors/bytecode/builders.py` — same migration
- `src/extract/extractors/cst/builders.py` — same migration
- `src/extract/extractors/symtable_extract.py` — same migration
- `src/test_support/datafusion_ext_stub.py` — migrate `_tree_sitter_node_row()` from `SpanSpec`/`span_dict` to `SpanTemplateSpec`/`make_span_spec_dict`

### New Files to Create

- `tests/unit/extract/coordination/test_span_template_migration.py` — verify migrated span generation paths (including test-support tree-sitter row helper behavior).

### Legacy Decommission/Delete Scope

- Delete `SpanSpec` class from `src/extract/coordination/context.py:173-184` — superseded by `SpanTemplateSpec` in `row_builder.py`.
- Delete `span_dict()` function from `src/extract/coordination/context.py:220-244` — superseded by direct `make_span_spec_dict()` calls.
- Remove `"SpanSpec"` and `"span_dict"` from `__all__` in `src/extract/coordination/context.py`.
- Remove `SpanSpec` and `span_dict` from `src/extract/coordination/__init__.py` imports/exports.
- Delete legacy `SpanSpec`/`span_dict` import usage in `src/test_support/datafusion_ext_stub.py:683-708`.

---

## S2. Remove `inspect.signature` Reflection (P6, P19)

### Goal

Complete the `execution_bundle` migration by removing the three `inspect.signature` compatibility checks in `orchestrator.py`. All three target functions already accept `execution_bundle` as a keyword parameter — the reflection is dead code.

### Representative Code Snippets

```python
# src/extraction/orchestrator.py — BEFORE (line ~230)
scan_fn = _run_repo_scan
if "execution_bundle" in inspect.signature(scan_fn).parameters:
    outputs = scan_fn(repo_root, options=options, execution_bundle=execution_bundle)
else:
    outputs = scan_fn(repo_root, options=options)

# src/extraction/orchestrator.py — AFTER
outputs = _run_repo_scan(repo_root, options=options, execution_bundle=execution_bundle)
```

```python
# Same pattern at lines ~342 and ~378
# BEFORE:
imports_fn = _run_python_imports
if "execution_bundle" in inspect.signature(imports_fn).parameters:
    python_imports = imports_fn(state.delta_locations, execution_bundle=execution_bundle)
else:
    python_imports = imports_fn(state.delta_locations)

# AFTER:
python_imports = _run_python_imports(state.delta_locations, execution_bundle=execution_bundle)
```

### Files to Edit

- `src/extraction/orchestrator.py` — remove `inspect.signature` checks at lines ~230, ~342, ~378; replace with direct calls passing `execution_bundle`. Remove `import inspect` if no other usages remain.

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete the three `if "execution_bundle" in inspect.signature(...)` branches from `orchestrator.py:~230,342,378`.
- Remove `import inspect` from `orchestrator.py` if no other call sites remain.

---

## S3. Add Injection Seam for `_delta_write_ctx` Singleton (P23, P12)

### Goal

Replace the unresetable `@cache` singleton `_delta_write_ctx()` with an injectable parameter on `_write_delta`, so tests can supply a test-scope `SessionContext` without monkey-patching.

### Representative Code Snippets

```python
# src/extraction/orchestrator.py — BEFORE
@cache
def _delta_write_ctx() -> SessionContext:
    from extraction.rust_session_bridge import build_extraction_session, extraction_session_payload
    return build_extraction_session(extraction_session_payload())

def _write_delta(table: pa.Table, location: Path, name: str) -> str:
    ctx = _delta_write_ctx()
    ...

# src/extraction/orchestrator.py — AFTER
def _default_delta_write_ctx() -> SessionContext:
    """Build a native extraction session context for Delta writes."""
    from extraction.rust_session_bridge import build_extraction_session, extraction_session_payload
    return build_extraction_session(extraction_session_payload())

# Module-level lazy holder
_DELTA_WRITE_CTX: SessionContext | None = None

def _get_delta_write_ctx() -> SessionContext:
    global _DELTA_WRITE_CTX  # noqa: PLW0603 - intentional lazy init
    if _DELTA_WRITE_CTX is None:
        _DELTA_WRITE_CTX = _default_delta_write_ctx()
    return _DELTA_WRITE_CTX

def _write_delta(
    table: pa.Table,
    location: Path,
    name: str,
    *,
    write_ctx: SessionContext | None = None,
) -> str:
    ctx = write_ctx or _get_delta_write_ctx()
    ...
```

Note: The `global` approach preserves the lazy-init behavior without `@cache`'s unresettability. Tests can set `orchestrator._DELTA_WRITE_CTX = mock_ctx` or pass `write_ctx=` directly. Alternatively, simply adding the `write_ctx` parameter to `_write_delta` and threading it through callers may be cleaner.

### Files to Edit

- `src/extraction/orchestrator.py` — replace `@cache _delta_write_ctx()` with injectable `_write_delta(..., write_ctx=...)` parameter; thread `write_ctx` through all `_write_delta` call sites within the module.

### New Files to Create

- `tests/unit/extraction/test_delta_write_injection.py` — verify `_write_delta` uses injected `write_ctx` when provided.

### Legacy Decommission/Delete Scope

- Delete `@cache` decorator and `_delta_write_ctx()` function from `orchestrator.py:193-198` — replaced by `_get_delta_write_ctx()` + parameter injection.

---

## S4. Add Injection Seam for `_build_extract_execution_bundle` (P12, P23)

### Goal

Introduce an injection seam for execution-bundle construction that is compatible with the
current request-envelope API (`run_extraction(request: RunExtractionRequestV1)`). Tests
must be able to provide a lightweight bundle without rewriting `run_extraction` into a
positional-argument API.

### Representative Code Snippets

```python
# src/extraction/contracts.py — BEFORE
class RunExtractionRequestV1(msgspec.Struct, frozen=True):
    repo_root: str
    work_dir: str
    ...
    options: ExtractionRunOptions | Mapping | None = None

# src/extraction/contracts.py — AFTER
class RunExtractionRequestV1(msgspec.Struct, frozen=True):
    repo_root: str
    work_dir: str
    ...
    execution_bundle_override: object | None = None
    options: ExtractionRunOptions | Mapping | None = None
```

```python
# src/extraction/orchestrator.py — AFTER
def _resolve_execution_bundle(request: RunExtractionRequestV1) -> _ExtractExecutionBundle:
    override = request.execution_bundle_override
    if isinstance(override, _ExtractExecutionBundle):
        return override
    if override is not None:
        msg = "execution_bundle_override must be an _ExtractExecutionBundle instance."
        raise TypeError(msg)
    return _build_extract_execution_bundle()

def run_extraction(request: RunExtractionRequestV1) -> ExtractionResult:
    ...
    execution_bundle = _resolve_execution_bundle(request)
    ...
```

### Files to Edit

- `src/extraction/contracts.py` — add optional `execution_bundle_override` field to `RunExtractionRequestV1`.
- `src/extraction/orchestrator.py` — add `_resolve_execution_bundle(request)` and replace direct `_build_extract_execution_bundle()` call in `run_extraction`.

### New Files to Create

- `tests/unit/extraction/test_bundle_injection.py` — verify `run_extraction` uses injected bundle when provided.

### Legacy Decommission/Delete Scope

- Delete direct inlined bundle construction in `run_extraction` (`execution_bundle = _build_extract_execution_bundle()`) and replace with `_resolve_execution_bundle(request)`.

---

## S5. Extract Pure Parse/Walk Core from Cache Shell (P16, P23)

### Goal

Separate the pure AST parsing/walking logic in `_extract_ast_for_context` from the cache management shell, making the pure core unit-testable without diskcache infrastructure.

### Representative Code Snippets

```python
# src/extract/extractors/ast/builders.py — NEW pure function
def _run_ast_parse_walk(
    file_ctx: FileContext,
    *,
    options: AstExtractOptions,
) -> tuple[_AstWalkResult | None, list[dict[str, object]]]:
    """Pure AST parse and walk — no cache, no IO.

    Returns:
    -------
    tuple[_AstWalkResult | None, list[dict[str, object]]]
        Walk result and error rows.
    """
    text = text_from_file_ctx(file_ctx)
    if text is None:
        return None, []
    line_offsets = line_offsets_from_file_ctx(file_ctx)
    max_nodes, error_rows = _limit_errors(file_ctx, text=text, options=options)
    walk: _AstWalkResult | None = None
    if not error_rows:
        walk, parse_errors = _parse_and_walk(
            text,
            filename=str(file_ctx.path),
            options=options,
            max_nodes=max_nodes,
            line_offsets=line_offsets,
        )
        error_rows.extend(parse_errors)
    return walk, error_rows
```

```python
# src/extract/extractors/ast/builders.py — REFACTORED cache wrapper
def _extract_ast_for_context(
    file_ctx: FileContext,
    *,
    options: AstExtractOptions,
    cache: Cache | FanoutCache | None = None,
    cache_ttl: float | None = None,
) -> dict[str, object] | None:
    if not file_ctx.file_id or not file_ctx.path:
        return None
    cache_key = _cache_key(file_ctx, options=options)
    use_cache = cache is not None and cache_key is not None
    cache_key_str = stable_cache_key("ast", {"key": cache_key}) if use_cache else None
    if use_cache and cache_key_str is not None:
        cached = cache_get(cache, key=cache_key_str, default=None)
        if isinstance(cached, _AstWalkResult):
            return _ast_row_from_walk(file_ctx, options=options, walk=cached, errors=[])

    # Delegate to pure core
    walk, error_rows = _run_ast_parse_walk(file_ctx, options=options)

    if use_cache and cache is not None and cache_key_str is not None and walk is not None:
        cache_set(cache, key=cache_key_str, value=walk, options=CacheSetOptions(ttl=cache_ttl))
    return _ast_row_from_walk(file_ctx, options=options, walk=walk, errors=error_rows) if walk is not None or error_rows else None
```

### Files to Edit

- `src/extract/extractors/ast/builders.py` — extract `_run_ast_parse_walk()` from `_extract_ast_for_context()`. The cache wrapper calls the pure function.

### New Files to Create

- `tests/unit/extract/extractors/ast/test_ast_parse_walk.py` — unit tests for `_run_ast_parse_walk` with no cache dependency.

### Legacy Decommission/Delete Scope

None — `_extract_ast_for_context` is refactored in place. The inline parse/walk logic within it is replaced by a call to `_run_ast_parse_walk`.

---

## S6. Introduce Shared `run_extract_entry_point` Helper (P7, P2, P3)

### Goal

Create shared entry-point helpers that encapsulate repeated normalize → ensure session →
ensure runtime profile → determinism tier → normalize options → collect/plan logic for
both materializing entrypoints (`extract_*`) and plan-returning entrypoints
(`extract_*_plans`). Each extractor wrapper becomes thin and pattern-consistent.

### Representative Code Snippets

```python
# src/extract/coordination/entry_point.py — NEW MODULE
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import TYPE_CHECKING, TypeVar

from datafusion_engine.arrow.interop import TableLike
from extract.coordination.context import ExtractExecutionContext
from extract.coordination.materialization import (
    ExtractMaterializeOptions,
    materialize_extract_plan,
)
from extract.coordination.schema_ops import ExtractNormalizeOptions
from extract.infrastructure.result_types import ExtractResult

if TYPE_CHECKING:
    from collections.abc import Callable

    from datafusion_engine.plan.bundle_artifact import DataFusionPlanArtifact
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from extract.session import ExtractSession

T_Options = TypeVar("T_Options")


def run_extract_entry_point(
    extractor_name: str,
    dataset_name: str,
    repo_files: TableLike,
    options: T_Options,
    *,
    context: ExtractExecutionContext | None = None,
    plan_builder: Callable[
        [TableLike, T_Options, ExtractExecutionContext, ExtractSession, DataFusionRuntimeProfile],
        DataFusionPlanArtifact,
    ],
    normalize_kwargs: Mapping[str, object] | None = None,
    apply_post_kernels: bool = True,
) -> ExtractResult[TableLike]:
    """Run the standard extract entry-point sequence.

    Parameters
    ----------
    extractor_name
        Short name for the extractor (e.g., "ast", "bytecode").
    dataset_name
        Output dataset name (e.g., "ast_files_v1").
    repo_files
        Input repository file table.
    options
        Extractor-specific options (already normalized).
    context
        Execution context; constructed if not provided.
    plan_builder
        Callable that produces a DataFusionPlanArtifact from inputs.
    normalize_kwargs
        Extra keyword arguments for ExtractNormalizeOptions.
    apply_post_kernels
        Whether to apply post-processing kernels during materialization.

    Returns
    -------
    ExtractResult[TableLike]
        Materialized extraction result.
    """
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    determinism_tier = exec_context.determinism_tier()
    normalize = ExtractNormalizeOptions(options=options, **(normalize_kwargs or {}))

    plan = plan_builder(repo_files, options, exec_context, session, runtime_profile)

    table = materialize_extract_plan(
        dataset_name,
        plan,
        runtime_profile=runtime_profile,
        determinism_tier=determinism_tier,
        options=ExtractMaterializeOptions(
            normalize=normalize,
            apply_post_kernels=apply_post_kernels,
        ),
    )
    return ExtractResult(table=table, extractor_name=extractor_name)
```

```python
# src/extract/coordination/entry_point.py — NEW companion helper
def run_extract_plan_entry_point(
    repo_files: TableLike,
    options: T_Options,
    *,
    context: ExtractExecutionContext | None = None,
    plan_builder: Callable[
        [TableLike, T_Options, ExtractExecutionContext, ExtractSession, DataFusionRuntimeProfile],
        dict[str, DataFusionPlanArtifact],
    ],
) -> dict[str, DataFusionPlanArtifact]:
    exec_context = context or ExtractExecutionContext()
    session = exec_context.ensure_session()
    exec_context = replace(exec_context, session=session)
    runtime_profile = exec_context.ensure_runtime_profile()
    _ = runtime_profile  # ensures plan builder receives fully initialized context
    return plan_builder(repo_files, options, exec_context, session, runtime_profile)
```

```python
# src/extract/extractors/ast/builders.py — AFTER (thin wrapper)
def extract_ast(
    repo_files: TableLike,
    options: AstExtractOptions | None = None,
    *,
    context: ExtractExecutionContext | None = None,
) -> ExtractResult[TableLike]:
    """Extract a minimal AST fact set per file."""
    normalized_options = normalize_options("ast", options, AstExtractOptions)
    return run_extract_entry_point(
        "ast",
        "ast_files_v1",
        repo_files,
        normalized_options,
        context=context,
        plan_builder=_build_ast_plan,
    )

def _build_ast_plan(
    repo_files: TableLike,
    options: AstExtractOptions,
    exec_context: ExtractExecutionContext,
    session: ExtractSession,
    runtime_profile: DataFusionRuntimeProfile,
) -> DataFusionPlanArtifact:
    """Build the AST extraction plan artifact."""
    plans = extract_ast_plans(repo_files, options=options, context=exec_context)
    return plans["ast_files"]
```

### Files to Edit

- `src/extract/extractors/ast/builders.py` — refactor `extract_ast()` to delegate to `run_extract_entry_point`
- `src/extract/extractors/ast/builders.py` — refactor `extract_ast_plans()` to delegate to `run_extract_plan_entry_point`
- `src/extract/extractors/bytecode/builders.py` — refactor `extract_bytecode()` similarly
- `src/extract/extractors/bytecode/builders.py` — refactor `extract_bytecode_plans()` similarly
- `src/extract/extractors/cst/builders.py` — refactor `extract_cst()` similarly
- `src/extract/extractors/cst/builders.py` — refactor `extract_cst_plans()` similarly
- `src/extract/extractors/symtable_extract.py` — refactor `extract_symtable()` similarly
- `src/extract/extractors/symtable_extract.py` — refactor `extract_symtable_plans()` similarly

### New Files to Create

- `src/extract/coordination/entry_point.py` — the shared `run_extract_entry_point` helper
- `tests/unit/extract/coordination/test_entry_point.py` — tests for the shared helper
- `tests/unit/extract/coordination/test_entry_point_plans.py` — tests for `run_extract_plan_entry_point` and plan-key preservation.

### Legacy Decommission/Delete Scope

- The inline runtime dispatch blocks in each `extract_*()` and `extract_*_plans()` function
  (normalize → session → profile → tier → normalize options → collect → plan/materialize)
  are replaced by shared helper calls in `entry_point.py`.

---

## S7. Clear `@cache` on Spec Helpers for Test Isolation (P23)

### Goal

Expose `cache_clear()` on the `@cache`-decorated spec helper functions so tests can reset cached state between test runs, preventing test-ordering dependencies.

### Representative Code Snippets

```python
# src/extract/coordination/spec_helpers.py — BEFORE
from functools import cache

@cache
def _feature_flag_rows(template_name: str) -> Mapping[str, tuple[ExtractMetadata, ...]]:
    ...

@cache
def _metadata_defaults(template_name: str) -> dict[str, object]:
    ...

# src/extract/coordination/spec_helpers.py — AFTER
from functools import lru_cache

@lru_cache(maxsize=None)
def _feature_flag_rows(template_name: str) -> Mapping[str, tuple[ExtractMetadata, ...]]:
    ...

@lru_cache(maxsize=None)
def _metadata_defaults(template_name: str) -> dict[str, object]:
    ...

def clear_spec_caches() -> None:
    """Clear cached spec helper results (for test isolation)."""
    _feature_flag_rows.cache_clear()
    _metadata_defaults.cache_clear()
```

### Files to Edit

- `src/extract/coordination/spec_helpers.py` — switch from `@cache` to `@lru_cache(maxsize=None)`, add `clear_spec_caches()`.

### New Files to Create

- `tests/unit/extract/coordination/test_spec_helpers_cache.py` — verify `clear_spec_caches()` resets cached values.

### Legacy Decommission/Delete Scope

- Replace `from functools import cache` with `from functools import lru_cache` in `spec_helpers.py`.

---

## S8. Fix Parallel Timing in Orchestrator (P24)

### Goal

Fix the misleading parallel extractor timing in `orchestrator.py` so that each extractor records its own wall-clock elapsed time rather than time-since-stage-start.

### Representative Code Snippets

```python
# src/extraction/orchestrator.py — BEFORE (conceptual, ~line 302-324)
stage_start = time.monotonic()
for name, future in futures:
    result = future.result()
    state.timing[name] = time.monotonic() - stage_start  # wrong: measures from stage start

# src/extraction/orchestrator.py — AFTER
submission_times: dict[str, float] = {}
for name, fn in extractors:
    submission_times[name] = time.monotonic()
    futures[name] = executor.submit(fn)

for name, future in futures.items():
    result = future.result()
    state.timing[name] = time.monotonic() - submission_times[name]  # correct: per-extractor
```

### Files to Edit

- `src/extraction/orchestrator.py` — refactor the parallel stage 1 timing to record per-extractor submission timestamps and compute per-extractor elapsed times.

### New Files to Create

None.

### Legacy Decommission/Delete Scope

None — the timing computation is corrected in place.

---

## S9. Unify Runtime Cache Policy on DF52-Native Cache Surfaces (P7, P8, P22)

### Goal

Replace duplicated string-key runtime-cache parsing with a single typed cache-policy bridge
that maps cleanly to DF52 cache-manager surfaces in both Python and Rust paths.

### Representative Code Snippets

```python
# src/datafusion_engine/session/delta_session_builder.py — AFTER
def _apply_cache_runtime_policy_settings(
    *,
    options: object,
    runtime_settings: Mapping[str, str],
    consumed: dict[str, object],
    unsupported: dict[str, str],
) -> set[str]:
    from datafusion_engine.session.cache_manager_contract import (
        cache_manager_contract_from_settings,
    )

    contract = cache_manager_contract_from_settings(enabled=True, settings=runtime_settings)
    handled: set[str] = set()
    for key, attr, value in (
        ("datafusion.runtime.metadata_cache_limit", "metadata_cache_limit", contract.metadata_cache_limit_bytes),
        ("datafusion.runtime.list_files_cache_limit", "list_files_cache_limit", contract.list_files_cache_limit_bytes),
        ("datafusion.runtime.list_files_cache_ttl", "list_files_cache_ttl_seconds", contract.list_files_cache_ttl_seconds),
    ):
        if key not in runtime_settings:
            continue
        handled.add(key)
        if value is None or not hasattr(options, attr):
            unsupported[key] = runtime_settings[key]
            continue
        setattr(options, attr, value)
        consumed[key] = value
    return handled
```

```rust
// rust/datafusion_python/src/codeanatomy_ext/delta_session_bridge.rs — existing DF52-native path
let mut cache_manager = CacheManagerConfig::default();
if let Some(limit) = options.metadata_cache_limit {
    cache_manager = cache_manager.with_metadata_cache_limit(usize_from_u64(limit, "metadata_cache_limit")?);
}
if let Some(limit) = options.list_files_cache_limit {
    cache_manager = cache_manager.with_list_files_cache_limit(usize_from_u64(limit, "list_files_cache_limit")?);
}
if let Some(ttl_seconds) = options.list_files_cache_ttl_seconds {
    cache_manager = cache_manager.with_list_files_cache_ttl(Some(Duration::from_secs(ttl_seconds)));
}
builder = builder.with_cache_manager(cache_manager);
```

### Files to Edit

- `src/datafusion_engine/session/delta_session_builder.py` — consolidate runtime cache parsing around cache-manager contract and remove duplicate parser paths.
- `src/datafusion_engine/session/context_pool.py` — route runtime-prefixed keys through canonical runtime-policy bridge instead of unconditional skip.
- `rust/datafusion_python/src/codeanatomy_ext/delta_session_bridge.rs` — keep as canonical DF52 cache-manager wiring and align option-field semantics with Python bridge.
- `rust/codeanatomy_engine/src/session/factory.rs` — adopt the same cache-policy contract semantics for Rust-native session construction.

### New Files to Create

- `tests/unit/datafusion_engine/session/test_runtime_cache_policy_bridge.py` — verify runtime setting inputs map deterministically to cache-manager policy payload.
- `rust/datafusion_python/tests/codeanatomy_ext_delta_runtime_policy_tests.rs` — verify native bridge applies list/metadata cache limits and TTL consistently.

### Legacy Decommission/Delete Scope

- Delete `_runtime_policy_rules()` and `_parse_runtime_policy_value()` in `src/datafusion_engine/session/delta_session_builder.py` once canonical bridge coverage is complete.
- Delete duplicated ad-hoc runtime-key skip logic in `src/datafusion_engine/session/context_pool.py` that bypasses typed runtime policy application.

---

## S10. Reclassify Runtime Profile “Reserved” Cache Fields to Applied (P8, P22)

### Goal

Move DF52-supported runtime/cache profile fields from “reserved/ignored” to “applied,” and
update coverage/warning logic so warnings only represent truly unsupported knobs.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/session/factory.rs — AFTER
let opts = config.options_mut();
opts.execution.planning_concurrency = profile.planning_concurrency;
opts.execution.meta_fetch_concurrency = profile.meta_fetch_concurrency;
```

```rust
// rust/codeanatomy_engine/src/session/profile_coverage.rs — AFTER
RuntimeProfileCoverage {
    field: "meta_fetch_concurrency".into(),
    state: CoverageState::Applied,
    note: "opts.execution.meta_fetch_concurrency".into(),
},
RuntimeProfileCoverage {
    field: "list_files_cache_limit".into(),
    state: CoverageState::Applied,
    note: "RuntimeEnvBuilder cache manager config".into(),
},
```

### Files to Edit

- `rust/codeanatomy_engine/src/session/factory.rs` — wire supported profile fields into session/runtime options.
- `rust/codeanatomy_engine/src/session/profile_coverage.rs` — reclassify applied fields, trim reserved warnings, and update assertions.
- `rust/codeanatomy_engine/src/session/runtime_profiles.rs` — ensure field semantics/docs align with applied behavior.

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete reserved-warning emissions for fields that are newly applied in `reserved_profile_warnings(...)`.
- Delete stale “known reserved” test assertions for fields no longer reserved.

---

## S11. Make Planning-Surface Contract the Cross-Language Source of Truth (P1, P7, P12, P22)

### Goal

Define a versioned planning-surface policy contract shared across Python and Rust so session
composition is parity-checkable and hash-comparable.

### Representative Code Snippets

```python
# src/datafusion_engine/session/planning_surface_contract.py — NEW
import msgspec

class PlanningSurfacePolicyContractV1(msgspec.Struct, frozen=True):
    enable_default_features: bool = True
    expr_planner_names: tuple[str, ...] = ()
    relation_planner_enabled: bool = False
    type_planner_enabled: bool = False
    table_factory_allowlist: tuple[str, ...] = ()
```

```rust
// rust/codeanatomy_engine/src/session/factory.rs — AFTER
planning_surface.typed_policy = Some(PlanningSurfacePolicyV1 {
    enable_default_features: planning_surface.enable_default_features,
    expr_planner_names: if overrides.enable_domain_planner {
        vec!["codeanatomy_domain".to_string()]
    } else {
        Vec::new()
    },
    relation_planner_enabled: !planning_surface.relation_planners.is_empty(),
    type_planner_enabled: planning_surface.type_planner.is_some(),
});
```

### Files to Edit

- `src/datafusion_engine/session/runtime.py` — load/export canonical planning-surface policy payload from Python runtime construction.
- `src/datafusion_engine/session/runtime_config_policies.py` — map runtime/profile names to contract payloads.
- `rust/codeanatomy_engine/src/session/planning_surface.rs` — keep `PlanningSurfaceSpec` as canonical builder target for contract application.
- `rust/codeanatomy_engine/src/session/factory.rs` — enforce typed-policy installation and parity fields.
- `rust/codeanatomy_engine/src/session/planning_manifest.rs` — include contract identity/hash in manifests.

### New Files to Create

- `src/datafusion_engine/session/planning_surface_contract.py` — Python-side versioned policy contract.
- `tests/unit/datafusion_engine/session/test_planning_surface_contract.py` — contract serialization and hash stability tests.

### Legacy Decommission/Delete Scope

- Delete scattered one-off planning-policy dicts that duplicate contract semantics after policy contract adoption.

---

## S12. Enforce DF52 Schema-Forward Pushdown Contracts in Custom Scan Paths (P8, P18, P22)

### Goal

Adopt DF52’s schema-forward pushdown model as a hard contract for custom listing/file source
paths: projection pushdown is fallible, adapter registration is explicit, and failures are surfaced.

### Representative Code Snippets

```python
# src/datafusion_engine/session/runtime_extensions.py — AFTER
def _install_physical_expr_adapter_factory(
    profile: DataFusionRuntimeProfile, ctx: SessionContext
) -> None:
    factory = profile.policies.physical_expr_adapter_factory
    if factory is None and profile.features.enable_schema_evolution_adapter:
        factory = _load_schema_evolution_adapter_factory()
    register = getattr(ctx, "register_physical_expr_adapter_factory", None)
    if not callable(register):
        msg = "SessionContext does not expose physical expr adapter registration."
        raise TypeError(msg)
    register(factory)
```

```rust
// rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs — existing contract path
let config = ListingTableConfig::new(table_path)
    .with_listing_options(options)
    .with_schema(schema.clone())
    .with_expr_adapter_factory(adapter_factory);
let provider = ListingTable::try_new(config)?;
```

### Files to Edit

- `src/datafusion_engine/session/runtime_extensions.py` — fail loudly when adapter registration is required but unavailable.
- `rust/datafusion_python/src/codeanatomy_ext/schema_evolution.rs` — preserve/strengthen explicit adapter + listing-table configuration checks.
- `rust/codeanatomy_engine/src/session/format_policy.rs` — align format policy outputs with schema-forward pushdown assumptions.

### New Files to Create

- `rust/datafusion_python/tests/codeanatomy_ext_schema_pushdown_contract_tests.rs` — verify adapter registration and projection-pushdown failure signaling.

### Legacy Decommission/Delete Scope

- Delete silent fallback paths that suppress required adapter registration failures when schema-evolution adapter is enabled.

---

## S13. Pin Listing Partition-Inference Policy and Fold into Identity (P18, P22)

### Goal

Stop relying on default partition-inference behavior and treat listing partition inference as an
explicit policy knob included in identity/fingerprint payloads.

### Representative Code Snippets

```rust
// rust/codeanatomy_engine/src/session/factory.rs — AFTER
config = config.set_str(
    "datafusion.execution.listing_table_factory_infer_partitions",
    "false",
);
```

```python
# src/datafusion_engine/plan/plan_identity.py — AFTER
payload = {
    ...
    "df_settings_entries": df_settings_entries,
    "listing_partition_inference": dict(inputs.artifacts.df_settings).get(
        "datafusion.execution.listing_table_factory_infer_partitions",
        "",
    ),
}
```

### Files to Edit

- `rust/codeanatomy_engine/src/session/factory.rs` — set explicit listing-partition-inference policy in session config.
- `src/datafusion_engine/plan/plan_identity.py` — include listing-inference policy in identity payload.
- `src/datafusion_engine/plan/bundle_assembly.py` — ensure environment capture includes the pinned setting for bundle identity/fingerprint.

### New Files to Create

- `tests/unit/datafusion_engine/plan/test_listing_partition_policy_identity.py` — verify identity hash changes when listing-partition policy changes.

### Legacy Decommission/Delete Scope

- Delete implicit dependence on DataFusion default `listing_table_factory_infer_partitions` behavior for production/reproducibility paths.

---

## S14. Standardize Extraction Planning Artifacts on Existing Bundle Surfaces (P18, P24)

### Goal

Use existing bundle-environment/identity surfaces as the canonical extraction-planning artifact
envelope (logical/optimized metadata + settings snapshot + info-schema hash + identity hash).

### Representative Code Snippets

```python
# src/datafusion_engine/plan/bundle_assembly.py — existing primitives
df_settings = _df_settings_snapshot(ctx, session_runtime=session_runtime)
info_schema_snapshot = _information_schema_snapshot(ctx, session_runtime=session_runtime)
info_schema_hash = _information_schema_hash(info_schema_snapshot)
fingerprint = compute_plan_fingerprint(...)
identity = plan_identity_payload(...)
```

```python
# src/extraction/orchestrator.py — AFTER (conceptual)
artifact = build_extraction_plan_artifact(ctx=session_ctx, df=df_for_stage, stage=name)
state.plan_artifacts[name] = artifact
```

### Files to Edit

- `src/extraction/orchestrator.py` — capture plan artifact envelope for extraction-planning stages that require replay/debug determinism.
- `src/datafusion_engine/plan/bundle_assembly.py` — expose public helper(s) for extraction artifact collection without duplicating internals.
- `src/datafusion_engine/plan/plan_identity.py` — keep extraction-facing payload shape aligned with canonical plan identity path.

### New Files to Create

- `tests/unit/extraction/test_extraction_plan_artifact_envelope.py` — verify extraction-stage artifact includes `df_settings`, info-schema hash, and plan identity hash.

### Legacy Decommission/Delete Scope

- Delete any new extraction-local ad-hoc identity hashing introduced outside canonical plan bundle identity helpers.

---

## S15. Rationalize Materialization Boundaries with DataFusion DML/COPY Surfaces (P2, P7, P19)

### Goal

Define when extraction uses DataFusion materialization boundaries (COPY/DML/write-table) vs
custom Delta transaction path, and centralize that policy to reduce bespoke write orchestration.

### Representative Code Snippets

```python
# src/extraction/orchestrator.py — AFTER (conceptual policy gate)
def _materialize_stage_output(
    *,
    ctx: SessionContext,
    table_name: str,
    location: Path,
    mode: str,
) -> str:
    if mode == "datafusion_copy":
        ctx.sql(
            f"COPY (SELECT * FROM {table_name}) TO '{location}' STORED AS parquet"
        ).collect()
        return str(location)
    return _write_delta(_load_table_from_ctx(ctx, table_name), location, table_name)
```

```python
# Keep existing explicit Delta path for cases that require transaction-specific semantics
write_transaction(_delta_write_ctx(), request=request)
```

### Files to Edit

- `src/extraction/orchestrator.py` — introduce explicit materialization boundary policy and route stage outputs through it.
- `src/datafusion_engine/io/delta_write_handler.py` — align write-path policy hooks with orchestrator materialization decisions.

### New Files to Create

- `tests/unit/extraction/test_materialization_boundary_policy.py` — verify boundary policy chooses correct sink path per mode/profile.

### Legacy Decommission/Delete Scope

- Delete direct, unconditional `_write_delta(...)` callsites in extraction stages where policy now routes through `_materialize_stage_output(...)`.

---

## S16. Add Physical Optimizer Rule Governance and Ordering Safety Gates (P8, P19, P22)

### Goal

Ensure custom physical optimizer rules are installed and executed in a DF52-safe way,
especially around post-optimization dynamic-filter phases and sort-pushdown interactions.

### Representative Code Snippets

```rust
// rust/datafusion_ext/src/physical_rules.rs — AFTER (conceptual guard)
impl PhysicalOptimizerRule for CodeAnatomyPhysicalRule {
    fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
        let policy = CodeAnatomyPhysicalConfig::from_config(config);
        if !policy.enabled {
            return Ok(plan);
        }
        if policy.coalesce_partitions && config.optimizer.enable_dynamic_filter_pushdown {
            // skip unsafe post-filter structural rewrite path
            return Ok(plan);
        }
        Ok(if policy.coalesce_partitions {
            Arc::new(CoalescePartitionsExec::new(plan))
        } else {
            plan
        })
    }
}
```

### Files to Edit

- `rust/datafusion_ext/src/physical_rules.rs` — add rule-governance checks and explicit safety gates.
- `rust/codeanatomy_engine/src/session/factory.rs` — ensure custom physical-rule install policy is explicit and profile-driven.

### New Files to Create

- `rust/datafusion_ext/tests/physical_rule_ordering_tests.rs` — verify rule behavior under dynamic-filter/sort-pushdown config combinations.

### Legacy Decommission/Delete Scope

- Delete unconditional coalesce-partitions wrapping behavior when governance/safety policy disables it.

---

## S17. Require Concrete Value from Relation Planner Extension (P19, P20, P22)

### Goal

Avoid carrying a no-op relation-planner extension surface by requiring either concrete
domain planning behavior with tests or explicit disablement when no behavior is provided.

### Representative Code Snippets

```rust
// rust/datafusion_ext/src/relation_planner.rs — AFTER (example behavior gate)
impl RelationPlanner for CodeAnatomyRelationPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        if supports_codeanatomy_relation(&relation) {
            return plan_codeanatomy_relation(relation, context);
        }
        Ok(RelationPlanning::Original(relation))
    }
}
```

```rust
// rust/codeanatomy_engine/src/session/factory.rs — AFTER
if overrides.enable_domain_planner && overrides.enable_relation_planner {
    planning_surface.relation_planners = vec![Arc::new(
        datafusion_ext::relation_planner::CodeAnatomyRelationPlanner,
    )];
}
```

### Files to Edit

- `rust/datafusion_ext/src/relation_planner.rs` — implement concrete behavior gate or explicit unsupported state.
- `rust/codeanatomy_engine/src/session/factory.rs` — only install relation planner when enabled by dedicated override/policy.

### New Files to Create

- `rust/datafusion_ext/tests/relation_planner_behavior_tests.rs` — verify both delegated and custom-planned relation paths.

### Legacy Decommission/Delete Scope

- Delete unconditional no-op relation planner installation in domain-planner path.

---

## S18. Version and Enforce Cross-Language Policy Parity Hashes (P7, P18, P22)

### Goal

Make planning/runtime policy parity explicit and machine-checkable by computing stable
policy hashes in Python and Rust from the same versioned policy contract payload.

### Representative Code Snippets

```python
# src/datafusion_engine/session/planning_surface_contract.py — AFTER
from datafusion_engine.plan.hashing import hash_json_default

def planning_surface_policy_hash(policy: PlanningSurfacePolicyContractV1) -> str:
    return hash_json_default(msgspec.to_builtins(policy), str_keys=True)
```

```rust
// rust/codeanatomy_engine/src/session/planning_manifest.rs — AFTER (conceptual)
pub fn planning_surface_policy_hash(policy: &PlanningSurfacePolicyV1) -> [u8; 32] {
    let payload = serde_json::to_vec(policy).expect("policy serializable");
    *blake3::hash(&payload).as_bytes()
}
```

### Files to Edit

- `src/datafusion_engine/session/runtime_config_policies.py` — emit typed policy payloads for parity hashing.
- `src/datafusion_engine/plan/plan_identity.py` — include policy hash/version in identity payload.
- `rust/codeanatomy_engine/src/session/planning_manifest.rs` — include policy hash/version in planning manifests.
- `rust/codeanatomy_engine/src/session/factory.rs` — compute and emit policy parity telemetry/warnings on mismatch.

### New Files to Create

- `tests/unit/datafusion_engine/session/test_policy_parity_hash.py` — verify stable hash generation for equivalent policy payloads.

### Legacy Decommission/Delete Scope

- Delete implicit parity assumptions based solely on profile names or ad-hoc setting maps without contract version/hash checks.

---

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S1)

- Delete `SpanSpec` class from `src/extract/coordination/context.py:173-184` — all usages migrated to `SpanTemplateSpec`.
- Delete `span_dict()` function from `src/extract/coordination/context.py:220-244` — all usages migrated to `make_span_spec_dict()`.
- Remove `"SpanSpec"` and `"span_dict"` from `__all__` in `context.py`, `coordination/__init__.py`, and `extract/__init__.py`.
- Delete remaining `SpanSpec`/`span_dict` usage in `src/test_support/datafusion_ext_stub.py`.

### Batch D2 (after S2)

- Remove `import inspect` from `orchestrator.py` if no remaining usages exist.
- Delete the three `if "execution_bundle" in inspect.signature(...)` conditional blocks from `orchestrator.py`.

### Batch D3 (after S3)

- Delete `@cache` decorator and `_delta_write_ctx()` function from `orchestrator.py:193-198`.

### Batch D4 (after S5, S6)

- Delete the inline runtime dispatch blocks from `extract_ast()`, `extract_bytecode()`, `extract_cst()`, `extract_symtable()`, `extract_ast_plans()`, `extract_bytecode_plans()`, `extract_cst_plans()`, and `extract_symtable_plans()` — replaced by entry-point helper delegation.
- Delete the inline parse/walk logic from `_extract_ast_for_context()` — replaced by call to `_run_ast_parse_walk()`.

### Batch D5 (after S9, S10)

- Delete duplicated runtime-policy parser helpers (`_runtime_policy_rules`, `_parse_runtime_policy_value`) in `src/datafusion_engine/session/delta_session_builder.py` once typed cache-policy bridge is canonical.
- Delete reserved-warning entries for fields reclassified to applied in `rust/codeanatomy_engine/src/session/profile_coverage.rs`.

### Batch D6 (after S11, S18)

- Delete ad-hoc planning-policy maps that duplicate versioned contract semantics in Python/Rust session setup paths.
- Delete parity checks based only on profile name without policy payload hash/version.

### Batch D7 (after S12, S13)

- Delete silent schema-evolution adapter fallback behavior when adapter registration is required.
- Delete implicit dependence on default `listing_table_factory_infer_partitions` behavior in reproducibility-sensitive planning paths.

### Batch D8 (after S16, S17)

- Delete unconditional no-op relation planner installation in domain planner setup.
- Delete unconditional coalesce-partitions rewrite path when governance policy disables or guards it.

---

## Implementation Sequence

1. **S1 — Unify span types** — Mechanical hard-cut migration; prerequisite for clean decommission and dedup passes.
2. **S2 — Remove `inspect.signature` reflection** — Low-risk dead-code removal that simplifies orchestration flow.
3. **S8 — Fix parallel timing** — Independent observability correction with immediate value.
4. **S7 — Spec helper cache clearing** — Small isolation improvement that reduces test-order coupling.
5. **S3 — Delta write context injection** — Adds test seam before broader orchestration refactors.
6. **S4 — Request-envelope-compatible execution bundle injection** — Lands after S3 to stabilize orchestration seam strategy.
7. **S5 — Extract pure parse/walk core** — Establishes pure-core pattern required for generalized entrypoint refactor.
8. **S6 — Shared entry-point helpers** — Apply dedup pattern across both materializing and plan-returning extractor entrypoints.
9. **S9 — Runtime cache policy unification** — Start DataFusion-native expansion with lowest-risk runtime policy consolidation.
10. **S10 — Reclassify reserved runtime fields** — Apply profile coverage/runtime wiring updates on top of S9.
11. **S11 — Planning-surface contract canonicalization** — Introduce typed cross-language policy contract.
12. **S18 — Policy parity hash/version enforcement** — Add deterministic parity checks once contract exists.
13. **S12 — Schema-forward pushdown contract enforcement** — Tighten adapter/projection behavior on custom source paths.
14. **S13 — Listing partition policy pinning + identity integration** — Stabilize schema-affecting defaults and identity payload.
15. **S14 — Standardized extraction plan artifacts** — Reuse canonical bundle surfaces for replay/debug envelopes.
16. **S15 — Materialization boundary rationalization** — Route writes through explicit policy (DML/COPY vs Delta transaction).
17. **S16 — Physical rule governance gates** — Add ordering/safety constraints for custom physical optimizer behavior.
18. **S17 — Relation planner value gate** — Finalize extension governance by requiring concrete value or disablement.

Rationale: S1-S8 complete corrected extraction cleanup first. S9-S18 then expand DataFusion-native planning/runtime adoption in dependency order (runtime policy → contract/parity → pushdown/schema policy → artifact/materialization policy → optimizer/extension governance).

---

## Implementation Checklist

- [ ] S1. Unify `SpanSpec` / `SpanTemplateSpec` into single canonical type
- [ ] S2. Remove `inspect.signature` reflection from `orchestrator.py`
- [ ] S3. Add injection seam for `_delta_write_ctx` singleton
- [ ] S4. Add request-envelope-compatible injection seam for `_build_extract_execution_bundle`
- [ ] S5. Extract pure `_run_ast_parse_walk` from cache shell
- [ ] S6. Introduce shared entry-point helpers for `extract_*` and `extract_*_plans`
- [ ] S7. Expose `cache_clear()` on spec helper caches
- [ ] S8. Fix parallel timing in orchestrator
- [ ] S9. Unify runtime cache policy on DF52-native cache surfaces
- [ ] S10. Reclassify runtime profile cache/meta fields from reserved to applied
- [ ] S11. Make planning-surface contract the cross-language source of truth
- [ ] S12. Enforce DF52 schema-forward pushdown contracts in custom scan paths
- [ ] S13. Pin listing partition-inference policy and fold it into identity payloads
- [ ] S14. Standardize extraction planning artifacts on canonical bundle surfaces
- [ ] S15. Rationalize materialization boundaries with DataFusion DML/COPY policy
- [ ] S16. Add physical optimizer rule governance and ordering safety gates
- [ ] S17. Require concrete value from relation planner extension
- [ ] S18. Version and enforce cross-language policy parity hashes
- [ ] D1. Delete `SpanSpec`, `span_dict`, and related exports
- [ ] D2. Remove `import inspect` and conditional blocks
- [ ] D3. Delete `@cache _delta_write_ctx()`
- [ ] D4. Delete inline runtime dispatch blocks from all extractor entrypoints (materializing + plans)
- [ ] D5. Delete duplicated runtime-policy parser paths and stale reserved warnings
- [ ] D6. Delete ad-hoc policy/parity logic superseded by contract hash/version
- [ ] D7. Delete implicit listing-inference defaults and silent adapter fallback paths
- [ ] D8. Delete unconditional no-op extension installs and unsafe post-filter rewrites
