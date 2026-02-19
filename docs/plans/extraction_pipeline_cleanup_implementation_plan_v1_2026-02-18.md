# Extraction Pipeline Cleanup Implementation Plan v1 (2026-02-18)

## Scope Summary

Address the 8 highest-priority findings from the extraction pipeline design review
(`docs/reviews/design_review_extraction_pipeline_2026-02-18.md`). The scope is confined
to `src/extract/` and `src/extraction/` (~81 files). Design stance: hard cutover with no
backward-compatibility shims. All existing public API signatures are preserved; internal
restructuring only.

Key goals:
- Eliminate ~1,500 LOC of structural duplication across extractor entry points (P7)
- Unify dual span types `SpanSpec` / `SpanTemplateSpec` into a single canonical type (P7)
- Complete the `execution_bundle` migration and remove `inspect.signature` reflection (P6)
- Add injection seams for module-level `@cache` singletons to enable test isolation (P23)
- Separate pure domain logic from cache management in builder files (P16/P23)

## Design Principles

1. **No compatibility shims** — `SpanSpec` is deleted, not deprecated. All callers migrate in one pass.
2. **Preserve public API signatures** — `extract_ast()`, `extract_cst()`, etc. retain their existing signatures. Internal restructuring only.
3. **Injection over patching** — Module-level singletons replaced with optional parameters defaulting to current behavior.
4. **Pure core, imperative shell** — Cache management wraps pure functions, not the reverse.
5. **No scope creep** — Do not touch `src/semantics/`, `src/relspec/`, `src/datafusion_engine/`, or `rust/`.

## Current Baseline

- `SpanSpec` defined at `src/extract/coordination/context.py:174-184` with 8 fields (all optional except first 6 positional).
- `SpanTemplateSpec` defined at `src/extract/row_builder.py:20-35` with identical 8 fields (all keyword with defaults).
- `span_dict()` adapter at `context.py:220-244` converts `SpanSpec` → `SpanTemplateSpec` field-by-field, then delegates to `make_span_spec_dict()`.
- `SpanSpec` imported by: `ast/builders.py`, `bytecode/builders.py`, `cst/builders.py`, `symtable_extract.py`. Also exported from `coordination/__init__.py`.
- The runtime dispatch block (normalize options → ensure session → ensure runtime profile → determinism tier → normalize → collect rows → build plan → materialize) appears identically in:
  - `ast/builders.py:926-964` (`extract_ast`) and `:1175+` (`extract_ast_tables`)
  - `bytecode/builders.py:1563-1607` (`extract_bytecode`)
  - `cst/builders.py:1517-1564` (`extract_cst`)
  - `symtable_extract.py:759-806` (`extract_symtable`)
- `inspect.signature` reflection at `orchestrator.py:230,342,378` checks if `execution_bundle` exists in function parameters, despite all three target functions (`_run_repo_scan:457`, `_run_python_imports:819`, `_run_python_external:867`) already accepting it.
- `@cache` on `_delta_write_ctx()` at `orchestrator.py:193-198` creates an unresetable module-level singleton.
- `@cache` on `_feature_flag_rows()` and `_metadata_defaults()` at `spec_helpers.py:43-54` cache based on template name with no clear invalidation.
- `_extract_ast_for_context` at `ast/builders.py:857-914` interleaves cache lookup/lock/store with pure AST parsing logic.
- `_build_extract_execution_bundle()` at `orchestrator.py:175-190` is a zero-argument factory with no injection seam.

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
# src/extract/coordination/context.py — add backward-compat alias (temporary, deleted in D1)
# Delete SpanSpec class entirely
# Delete span_dict function entirely
# Keep only re-exports from row_builder if needed for coordination/__init__.py
```

### Files to Edit

- `src/extract/coordination/context.py` — delete `SpanSpec` class (lines 173-184), delete `span_dict()` function (lines 220-244), remove from `__all__`
- `src/extract/coordination/__init__.py` — remove `SpanSpec` and `span_dict` from imports and `__all__`; add `SpanTemplateSpec` and `make_span_spec_dict` re-exports from `row_builder`
- `src/extract/__init__.py` — remove `SpanSpec` / `span_dict` from `_EXPORTS` dict; add `SpanTemplateSpec` / `make_span_spec_dict` if needed
- `src/extract/extractors/ast/builders.py` — change `SpanSpec` → `SpanTemplateSpec`, `span_dict()` → `make_span_spec_dict()` throughout
- `src/extract/extractors/bytecode/builders.py` — same migration
- `src/extract/extractors/cst/builders.py` — same migration
- `src/extract/extractors/symtable_extract.py` — same migration

### New Files to Create

None.

### Legacy Decommission/Delete Scope

- Delete `SpanSpec` class from `src/extract/coordination/context.py:173-184` — superseded by `SpanTemplateSpec` in `row_builder.py`.
- Delete `span_dict()` function from `src/extract/coordination/context.py:220-244` — superseded by direct `make_span_spec_dict()` calls.
- Remove `"SpanSpec"` and `"span_dict"` from `__all__` in `src/extract/coordination/context.py`.
- Remove `SpanSpec` and `span_dict` from `src/extract/coordination/__init__.py` imports/exports.

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

Add an optional `bundle` parameter to `run_extraction` so tests can supply a lightweight extraction bundle without spinning up a full Rust-backed DataFusion session.

### Representative Code Snippets

```python
# src/extraction/orchestrator.py — BEFORE
def run_extraction(
    repo_root: str | Path,
    *,
    work_dir: str | Path | None = None,
    ...
) -> ExtractionResult:
    ...
    execution_bundle = _build_extract_execution_bundle()
    ...

# src/extraction/orchestrator.py — AFTER
def run_extraction(
    repo_root: str | Path,
    *,
    work_dir: str | Path | None = None,
    execution_bundle: _ExtractExecutionBundle | None = None,
    ...
) -> ExtractionResult:
    ...
    resolved_bundle = execution_bundle or _build_extract_execution_bundle()
    ...
```

### Files to Edit

- `src/extraction/orchestrator.py` — add `execution_bundle: _ExtractExecutionBundle | None = None` parameter to `run_extraction`; use `execution_bundle or _build_extract_execution_bundle()` at the construction site.

### New Files to Create

- `tests/unit/extraction/test_bundle_injection.py` — verify `run_extraction` uses injected bundle when provided.

### Legacy Decommission/Delete Scope

None — `_build_extract_execution_bundle` remains as the default factory. The change is additive.

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

Create a generic entry-point helper that encapsulates the repeated normalize → ensure session → ensure runtime profile → determinism tier → normalize options → collect/plan → materialize sequence. Each `extract_*` function becomes a thin wrapper delegating to this helper.

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
- `src/extract/extractors/bytecode/builders.py` — refactor `extract_bytecode()` similarly
- `src/extract/extractors/cst/builders.py` — refactor `extract_cst()` similarly
- `src/extract/extractors/symtable_extract.py` — refactor `extract_symtable()` similarly

### New Files to Create

- `src/extract/coordination/entry_point.py` — the shared `run_extract_entry_point` helper
- `tests/unit/extract/coordination/test_entry_point.py` — tests for the shared helper

### Legacy Decommission/Delete Scope

- The inline runtime dispatch blocks in each `extract_*()` function (normalize → session → profile → tier → normalize options → plan → materialize) are replaced by single calls to `run_extract_entry_point`. The ~30-40 line block in each function is reduced to ~5-8 lines.

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

## Cross-Scope Legacy Decommission and Deletion Plan

### Batch D1 (after S1)

- Delete `SpanSpec` class from `src/extract/coordination/context.py:173-184` — all usages migrated to `SpanTemplateSpec`.
- Delete `span_dict()` function from `src/extract/coordination/context.py:220-244` — all usages migrated to `make_span_spec_dict()`.
- Remove `"SpanSpec"` and `"span_dict"` from `__all__` in `context.py`, `coordination/__init__.py`, and `extract/__init__.py`.

### Batch D2 (after S2)

- Remove `import inspect` from `orchestrator.py` if no remaining usages exist.
- Delete the three `if "execution_bundle" in inspect.signature(...)` conditional blocks from `orchestrator.py`.

### Batch D3 (after S3)

- Delete `@cache` decorator and `_delta_write_ctx()` function from `orchestrator.py:193-198`.

### Batch D4 (after S5, S6)

- Delete the inline runtime dispatch blocks from `extract_ast()`, `extract_bytecode()`, `extract_cst()`, `extract_symtable()` — replaced by delegation to `run_extract_entry_point()`.
- Delete the inline parse/walk logic from `_extract_ast_for_context()` — replaced by call to `_run_ast_parse_walk()`.

---

## Implementation Sequence

1. **S1 — Unify span types** — Zero behavioral change. Mechanical find-and-replace. Must land first because S6 depends on a clean import surface.
2. **S2 — Remove `inspect.signature`** — Trivially safe; removes dead compatibility code. Independent of other items.
3. **S7 — Spec helper cache clearing** — Small, self-contained. Lands early to unblock test improvements.
4. **S8 — Fix parallel timing** — Small, self-contained. No dependency on other items.
5. **S3 — Delta write ctx injection** — Standalone injectable parameter addition.
6. **S4 — Execution bundle injection** — Standalone injectable parameter addition.
7. **S5 — Extract pure parse/walk** — Prerequisite refactoring for S6 (demonstrates the pattern).
8. **S6 — Shared entry-point helper** — Largest item; depends on S1 (clean imports) and S5 (pure-core pattern established). This is the capstone that eliminates ~1,000+ LOC of duplication.

Rationale: S1-S4 are independent quick wins that reduce noise. S5 establishes the pure-core pattern on one extractor. S6 generalizes it across all extractors and is the highest-risk, highest-reward item.

---

## Implementation Checklist

- [ ] S1. Unify `SpanSpec` / `SpanTemplateSpec` into single canonical type
- [ ] S2. Remove `inspect.signature` reflection from `orchestrator.py`
- [ ] S3. Add injection seam for `_delta_write_ctx` singleton
- [ ] S4. Add injection seam for `_build_extract_execution_bundle`
- [ ] S5. Extract pure `_run_ast_parse_walk` from cache shell
- [ ] S6. Introduce shared `run_extract_entry_point` helper
- [ ] S7. Expose `cache_clear()` on spec helper caches
- [ ] S8. Fix parallel timing in orchestrator
- [ ] D1. Delete `SpanSpec`, `span_dict`, and related exports
- [ ] D2. Remove `import inspect` and conditional blocks
- [ ] D3. Delete `@cache _delta_write_ctx()`
- [ ] D4. Delete inline runtime dispatch blocks from all extractors
