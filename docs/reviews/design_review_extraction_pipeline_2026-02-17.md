# Design Review: Extraction Pipeline

**Date:** 2026-02-17
**Scope:** `src/extract/`, `src/extraction/`
**Focus:** Boundaries (1–6), Simplicity (19–22), Quality (23–24)
**Depth:** moderate
**Files reviewed:** 20 (from 84 total; 71 in `src/extract/`, 13 in `src/extraction/`)

---

## Executive Summary

The extraction pipeline is architecturally sound and well-stratified: `src/extract/` owns evidence production, `src/extraction/` owns orchestration and session wiring, and the boundary between them is mostly clean. The primary systemic gaps are (1) a `builders.py`/`builders_runtime.py` split that forces each extractor to expose private symbols across a module boundary — a DRY and information-hiding violation that scales with extractor count, and (2) residual inspection-at-runtime patterns in `orchestrator.py` that couple the orchestrator to internal function signatures. The `src/extraction/` module is structurally sound as a thin orchestration layer and does not warrant consolidation into `src/extract/`; its concern (staged, parallelized, Delta-persisted orchestration) is meaningfully distinct. The highest-priority improvements are eliminating the cross-module private-symbol imports, removing `inspect.signature` gating, and replacing the inline `repo_files_v1` schema literal in the orchestrator with the canonical registry schema.

---

## Alignment Scorecard

| # | Principle | Alignment | Effort | Risk | Key Finding |
|---|-----------|-----------|--------|------|-------------|
| 1 | Information hiding | 1 | medium | medium | `builders_runtime.py` imports private symbols (`_cache_key`, `_AstWalkAccumulator`, `_def_row`, etc.) from `builders.py` across the module boundary |
| 2 | Separation of concerns | 2 | small | low | Orchestrator mixes stage sequencing with inline Delta-write logic; otherwise clean |
| 3 | SRP | 2 | small | low | `materialization.py` handles planning, execution, streaming, schema validation, and view registration — four distinct concerns |
| 4 | High cohesion, low coupling | 2 | small | low | `extraction/runtime_profile.py` imports pydantic alongside msgspec, creating a mixed-framework dependency in a hot path |
| 5 | Dependency direction | 2 | small | low | `extract/coordination/context.py` imports from `extraction/` (the orchestration layer), inverting direction slightly |
| 6 | Ports & Adapters | 3 | — | — | N/A; extractor-to-DataFusion adapter pattern is correct and well-bounded |
| 19 | KISS | 1 | medium | medium | `builders_runtime.py` files (AST: 792 LOC, CST: 1312 LOC, bytecode: 1324 LOC) are excessively large due to the split architecture |
| 20 | YAGNI | 2 | small | low | `_run_repo_scan_fallback` duplicates file-walking logic that `RepoScanOptions` already supports; the fallback is real but over-specified |
| 21 | Least astonishment | 1 | small | medium | `orchestrator.py` uses `inspect.signature` at runtime to gate parameter passing — callers cannot predict dispatch behavior from type signatures alone |
| 22 | Declare and version public contracts | 2 | small | low | `src/extraction/__init__.py` exports only options types; `ExtractionResult` and `RunExtractionRequestV1` are not re-exported at the package boundary |
| 23 | Design for testability | 2 | small | low | Core extractor logic in `builders.py` is pure and testable; runtime files require heavyweight session setup; unit tests monkeypatch module-level privates |
| 24 | Observability | 2 | small | low | Stage timing uses `time.monotonic()` anchored to `stage_start` not `t0` in `_run_parallel_stage1_extractors`, producing misleading relative timings |

---

## Detailed Findings

### Category: Boundaries

#### P1. Information hiding — Alignment: 1/3

**Current state:**

Every `builders_runtime.py` file imports private symbols directly from its sibling `builders.py`. In `src/extract/extractors/ast/builders_runtime.py:31–54`:

```python
from extract.extractors.ast.builders import (
    _cache_key,
    _call_row,
    _def_row,
    _docstring_row,
    _exception_error_row,
    _import_rows,
    _limit_errors,
    _node_name,
    _node_scalar_attrs,
    _node_value_repr,
    _parse_ast_text,
    _span_spec_from_node,
)
from extract.extractors.ast.setup import (
    AstExtractOptions,
    _format_feature_version,
    _resolve_feature_version,
)
from extract.extractors.ast.visitors import (
    AstLimitError,
    _AstWalkAccumulator,
    _AstWalkResult,
)
```

The CST runtime file (`src/extract/extractors/cst/builders_runtime.py:41–58`) imports 11 private symbols from `builders.py`, including `_collect_cst_nodes_edges`, `_row_map`, `_manifest_row`, `_matcher_counts`, and `_qname_keys`. The bytecode runtime (`builders_runtime.py`) follows the same pattern.

This means:
- The `_` prefix convention has no protective value at the per-extractor level — everything is effectively public to the paired runtime file.
- Any internal refactor of `builders.py` requires simultaneous edits to `builders_runtime.py`.
- The module split signals a boundary that does not exist in practice.

**Findings:**
- `src/extract/extractors/ast/builders_runtime.py:31–54` — 12 private symbol imports from `ast/builders.py`
- `src/extract/extractors/cst/builders_runtime.py:41–58` — 11 private symbol imports from `cst/builders.py`
- `src/extract/extractors/bytecode/builders_runtime.py` — follows same pattern; bytecode runtime is 1324 LOC
- `src/extract/extractors/ast/setup.py` — `_format_feature_version` and `_resolve_feature_version` imported across module boundary

**Suggested improvement:**

Merge each `builders_runtime.py` back into its corresponding `builders.py` and expose only the public API through the extractor's `__init__.py`. The split appears to have been introduced to separate "pure row construction" from "orchestration/parallelism," but this concern boundary does not align with actual usage: `builders_runtime.py` imports deeply into `builders.py` internals, making the modules inseparable in practice. After merging, the `__init__.py` for each extractor remains the clean public surface (`extract_ast`, `extract_ast_tables`, `AstExtractOptions`, `AstLimitError`).

Expected LOC reduction: approximately 30% per extractor from eliminating duplicated import scaffolding and boundary ceremony. The merged `ast/builders.py` would be ~1,200 LOC (vs two files summing to 1,290 LOC), but with no cross-module private imports.

**Effort:** medium (one merge per extractor × 4 extractors; careful with test monkeypatching)
**Risk if unaddressed:** medium — every internal rename requires two-file edits; the private symbol imports will drift if the two files evolve independently

---

#### P2. Separation of concerns — Alignment: 2/3

**Current state:**

The `src/extract/coordination/materialization.py` (692 LOC) is the clearest place where concerns blur. `_write_and_record_extract_output` (lines 326–401) performs five distinct operations in a single function:
1. Resolves a `ManifestDatasetResolver`
2. Calls `write_extract_outputs`
3. Registers the extract view via `_register_extract_view`
4. Records the view artifact via `_record_extract_view_artifact`
5. Validates the schema contract via `_validate_extract_schema_contract`

Each operation has its own `try/except` block recording a `ExtractQualityEvent`, making the function 75 lines of largely boilerplate error-handling. The operations are logically independent (view registration failure should not block schema validation recording) but share a mutable `EngineEventRecorder` that is created once and shared.

The `run_extraction` orchestrator in `src/extraction/orchestrator.py` is cleaner: `_run_repo_scan_with_fallback`, `_run_parallel_stage1_extractors`, `_run_python_imports_stage`, and `_run_python_external_stage` are well-separated. The remaining concern is that `_write_delta` (lines 407–443) mixes Delta transaction logic with orchestration — this is the only place in `src/extraction/` that touches the Delta write path directly.

**Findings:**
- `src/extract/coordination/materialization.py:326–401` — `_write_and_record_extract_output` performs five operations across three error-handling blocks
- `src/extraction/orchestrator.py:407–443` — `_write_delta` embeds Delta transaction construction inline rather than delegating to a shared write helper

**Suggested improvement:**

Extract `_write_and_record_extract_output` into three focused helpers: `_write_extract_output`, `_register_and_record_view`, and `_validate_extract_output_contract`. Each is independently testable and the three-step sequence remains explicit in the call site. For `_write_delta` in the orchestrator, delegate to `extraction.delta_tools` (which already exists as `src/extraction/delta_tools.py`) rather than constructing the Delta write request inline.

**Effort:** small
**Risk if unaddressed:** low — no correctness issue; ergonomic and maintenance burden only

---

#### P3. SRP — Alignment: 2/3

**Current state:**

`src/extract/coordination/materialization.py` has at least four reasons to change: (a) how extract plans are built, (b) how they are executed, (c) how outputs are written and registered, (d) how schema contracts are validated. As a coordination module this is largely acceptable, but the module size (692 LOC) and the number of private helpers suggest it is approaching a natural split point.

`src/extraction/runtime_profile.py` (613 LOC) mixes runtime profile resolution, Rust bridge interrogation, environment variable parsing, and profile hashing. The pydantic usage at line 13 (`from pydantic import BaseModel, ConfigDict, TypeAdapter`) exists for a single validation step (`_RUNTIME_PROFILE_ENV_ADAPTER`) that could be replaced by the msgspec `convert` pattern used throughout the rest of the codebase.

**Findings:**
- `src/extraction/runtime_profile.py:13` — pydantic imported for `_RuntimeProfileEnvPatchRuntime` validation; the equivalent msgspec `convert` pattern is already used in `extract/coordination/context.py:63`
- `src/extract/coordination/materialization.py` — four distinct concerns in one file; not yet a problem but approaching the threshold

**Suggested improvement:**

Replace `_RuntimeProfileEnvPatchRuntime` (a pydantic `BaseModel`) in `runtime_profile.py` with a msgspec `Struct` and the `convert` function already available in `serde_msgspec`. This removes the pydantic dependency from the hot path and aligns with the project's CLAUDE.md constraint ("Avoid pydantic in CQ hot paths"). This also applies to `ExtractionRunOptions` callers in the same file.

**Effort:** small (pydantic → msgspec swap for one class)
**Risk if unaddressed:** low — pydantic in a non-hot-path file; but it violates the architectural constraint and creates an inconsistent second validation framework

---

#### P4. High cohesion, low coupling — Alignment: 2/3

**Current state:**

The `src/extract/` and `src/extraction/` module boundary is coherent. `src/extract/` owns evidence production primitives; `src/extraction/` owns staged orchestration, session wiring, and Delta persistence. The boundary is crossed cleanly via typed interfaces (`ExtractSession`, `RuntimeProfileSpec`, `EngineSession`).

The weaker cohesion point is `src/extract/coordination/context.py`, which contains: `FileContext` (file identity), `RepoFileRow` (typed schema), `ExtractExecutionContext` (session construction), `SpanSpec` (span encoding), and utility functions (`file_identity_row`, `span_dict`, `attrs_map`, `text_from_file_ctx`). These are logically related but the file is the de facto "kitchen sink" for coordination utilities. It is currently 218 LOC — manageable, but adding more session logic here will blur cohesion further.

**Findings:**
- `src/extract/coordination/context.py` — 5 logically distinct groupings in one file (identity, schema, context, span spec, utilities)
- `src/extraction/engine_runtime.py` (not reviewed in depth) — imported by `engine_session.py`; assess whether `EngineRuntime` and `EngineSession` should be co-located

**Suggested improvement:**

Split `src/extract/coordination/context.py` into:
- `context_types.py` — `FileContext`, `RepoFileRow`, `SpanSpec` (pure data)
- `context_builders.py` — `ExtractExecutionContext`, `build_extract_session` helpers
- Keep the utility functions (`span_dict`, `attrs_map`, etc.) in `context.py` as a thin re-export

This prevents the file from becoming a dumping ground as the extraction layer evolves.

**Effort:** small
**Risk if unaddressed:** low

---

#### P5. Dependency direction — Alignment: 2/3

**Current state:**

`src/extract/coordination/context.py:22–27` imports from `src/extraction/`:

```python
from extract.session import ExtractSession, build_extract_session
from extraction.runtime_profile import RuntimeProfileSpec, resolve_runtime_profile
```

`src/extract/session.py:11–12` also imports from `src/extraction/`:

```python
from extraction.engine_session import EngineSession
from extraction.engine_session_factory import build_engine_session
```

The intended dependency direction is `extraction/` (orchestration, shell) depends on `extract/` (core evidence production). The reverse imports — `extract/` depending on `extraction/` — invert this direction. The concrete types leaking upward are `EngineSession`, `RuntimeProfileSpec`, and `resolve_runtime_profile`.

This is a moderate violation: `ExtractSession` in `src/extract/session.py` wraps `EngineSession` from `src/extraction/`. If `extraction/` is the "outer shell," `extract/` should not need to import from it.

**Findings:**
- `src/extract/session.py:11–12` — imports `EngineSession` and `build_engine_session` from `extraction/`
- `src/extract/coordination/context.py:22–27` — imports `RuntimeProfileSpec` and `resolve_runtime_profile` from `extraction/`

**Suggested improvement:**

Move `EngineSession`, `EngineRuntime`, and `RuntimeProfileSpec` into `src/extract/` (e.g., `src/extract/engine/`) or into a shared layer (e.g., `src/engine_core/`), with `src/extraction/` importing from there. Alternatively, define `ExtractSession` purely in terms of a `Protocol` interface for its engine dependencies, eliminating the concrete import of `EngineSession` into `src/extract/session.py`. The key invariant: `src/extract/` should not need `src/extraction/` at import time.

**Effort:** medium (affects session construction path across both modules)
**Risk if unaddressed:** medium — the inversion creates a circular-import risk as the two modules evolve; it also makes `src/extract/` harder to test in isolation

---

### Category: Simplicity

#### P19. KISS — Alignment: 1/3

**Current state:**

The `builders.py`/`builders_runtime.py` split across all four major extractors results in accumulated complexity:

| Extractor | `builders.py` LOC | `builders_runtime.py` LOC | Total |
|-----------|-------------------|---------------------------|-------|
| AST | 498 | 792 | 1,290 |
| CST | 511 | 1,312 | 1,823 |
| bytecode | 482 | 1,324 | 1,806 |
| tree-sitter | 765 | 786 | 1,551 |

The `builders_runtime.py` files contain: the same orchestration pattern (worklist iteration, parallel map, plan materialization, cache lookup), repeated four times. The per-extractor variation is in the row construction logic, which is already isolated in `builders.py`. The runtime scaffolding is essentially a template instantiated four times with minimal structural variation.

**Findings:**
- `src/extract/extractors/ast/builders_runtime.py` — 792 LOC duplicating the same worklist + parallel map + materialize + cache pattern as the other three runtime files
- `src/extract/extractors/cst/builders_runtime.py` — 1312 LOC; largest file in the extraction layer
- `src/extract/extractors/bytecode/builders_runtime.py` — 1324 LOC
- `src/extract/extractors/tree_sitter/builders_runtime.py` — 786 LOC

A shared `ExtractionRuntime` base or factory that takes the extractor's row-construction function as a parameter would reduce each `builders_runtime.py` to ~100–150 LOC of configuration, with the shared loop in one place.

**Suggested improvement:**

Extract the shared runtime orchestration into `src/extract/coordination/extraction_runtime_loop.py` (or similar) that implements: worklist iteration, parallel map dispatch, plan building from row batches, evidence plan gating, cache lookup/store, and telemetry spans. Each extractor's runtime file then becomes a thin adapter:

```python
# extract/extractors/ast/builders_runtime.py (target state)
from extract.coordination.extraction_runtime_loop import run_extractor_loop
from extract.extractors.ast.builders import AstExtractOptions, build_ast_rows_for_file

def extract_ast_tables(repo_files, session, ...) -> dict[str, TableLike]:
    return run_extractor_loop(
        name="ast_files",
        repo_files=repo_files,
        session=session,
        row_fn=build_ast_rows_for_file,
        options=AstExtractOptions(...),
    )
```

Estimated LOC reduction: ~2,500 LOC across the four runtime files (keeping ~600 LOC total for extractor-specific configuration).

**Effort:** large (requires identifying the common loop structure and parameterizing differences for all four extractors)
**Risk if unaddressed:** high — the duplicated runtime loop is a maintenance multiplier: any change to parallelism, caching, evidence plan gating, or observability must be replicated four times; drift is already visible (bytecode runtime is 1324 LOC vs AST runtime at 792 LOC despite similar feature sets)

---

#### P20. YAGNI — Alignment: 2/3

**Current state:**

`_run_repo_scan_fallback` in `src/extraction/orchestrator.py:518–601` duplicates the file-walking logic (glob expansion, exclude filtering, encoding detection, sha256 hashing, `repo_files_v1` schema construction) that `RepoScanOptions` in `src/extract/scanning/repo_scan.py` already implements for non-git directories. The fallback path was introduced for legitimately different behavior (filesystem walk without git), but it re-implements file metadata collection rather than delegating to the scanning layer.

The inline `repo_files_schema` literal at `orchestrator.py:586–596` is the most direct YAGNI/DRY violation: the canonical `repo_files_v1` Arrow schema lives in the registry (`datafusion_engine.extract.registry.dataset_schema("repo_files_v1")`).

**Findings:**
- `src/extraction/orchestrator.py:586–596` — inline `pa.schema([...])` definition that duplicates the canonical `repo_files_v1` schema
- `src/extraction/orchestrator.py:518–601` — 83-LOC fallback that duplicates file-walking and metadata logic from the scanning layer

**Suggested improvement:**

Replace the inline schema at `orchestrator.py:586` with `dataset_schema("repo_files_v1")` from the registry. For the fallback logic, delegate to a purpose-built `scan_filesystem_repo` function in `src/extract/scanning/` that wraps the non-git walk path — the orchestrator then calls the same interface for both git and fallback paths.

**Effort:** small
**Risk if unaddressed:** medium — the inline schema will drift from the canonical schema; the fallback walker will miss future fields added to `repo_files_v1`

---

#### P21. Least astonishment — Alignment: 1/3

**Current state:**

`src/extraction/orchestrator.py` uses `inspect.signature` at runtime to decide whether to pass `execution_bundle` to module-level private functions. This occurs in three places:

```python
# orchestrator.py:229–240
scan_fn = _run_repo_scan
if "execution_bundle" in inspect.signature(scan_fn).parameters:
    outputs = scan_fn(repo_root, options=options, execution_bundle=execution_bundle)
else:
    outputs = scan_fn(repo_root, options=options)
```

And at lines 341–348 (`_run_python_imports`) and 377–386 (`_run_python_external`).

This is a runtime arity check on module-level functions in the same file — functions whose signatures are visible at definition time. The callers cannot know from the type signatures alone which code path will be taken. A competent reader of the call site at line 229 sees `_run_repo_scan` being called, but the actual behavior depends on introspection of the callee's parameter list. This pattern originated as a migration shim (the comment structure suggests `execution_bundle` was added later), but it persists in the current codebase where all three functions already accept `execution_bundle` as an optional keyword argument (`*,  execution_bundle: _ExtractExecutionBundle | None = None`).

**Findings:**
- `src/extraction/orchestrator.py:229–240` — `inspect.signature` gating for `_run_repo_scan`
- `src/extraction/orchestrator.py:341–348` — `inspect.signature` gating for `_run_python_imports`
- `src/extraction/orchestrator.py:377–386` — `inspect.signature` gating for `_run_python_external`

All three target functions already have `execution_bundle` in their signatures (confirmed by reading lines 457–515, 819–864, 867–915), making the inspection dead code.

**Suggested improvement:**

Remove the `inspect.signature` checks and call the functions directly with `execution_bundle=execution_bundle`. The dead-code path (`else` branch) can be deleted.

```python
# Before (orchestrator.py:229–240)
scan_fn = _run_repo_scan
if "execution_bundle" in inspect.signature(scan_fn).parameters:
    outputs = scan_fn(repo_root, options=options, execution_bundle=execution_bundle)
else:
    outputs = scan_fn(repo_root, options=options)

# After
outputs = _run_repo_scan(repo_root, options=options, execution_bundle=execution_bundle)
```

**Effort:** small (three sites, straightforward deletion)
**Risk if unaddressed:** medium — the pattern signals to future readers that the function signature is unstable, encouraging more inspection-based dispatch; it also imports `inspect` for no purpose

---

#### P22. Declare and version public contracts — Alignment: 2/3

**Current state:**

`src/extraction/__init__.py` exports only `ExtractionRunOptions`, `RepoScanDiffOptions`, and `normalize_extraction_options`. The primary entry point `RunExtractionRequestV1` and `ExtractionResult` — the contract between callers and the orchestrator — are not part of the public package surface. A caller must know to import from `extraction.contracts` and `extraction.orchestrator` directly.

`src/extract/__init__.py` uses a lazy `__getattr__`/`_EXPORTS` pattern (lines 111–206) that correctly defers heavy imports. The `__all__` at lines 209–268 is comprehensive and well-maintained. This is a good pattern.

**Findings:**
- `src/extraction/__init__.py:1–15` — exports only option types; `RunExtractionRequestV1`, `ExtractionResult`, and `run_extraction` are absent from the package surface
- `src/extraction/contracts.py:17` — `RunExtractionRequestV1` is versioned (`V1`) but not re-exported at the `extraction` package level

**Suggested improvement:**

Add `RunExtractionRequestV1`, `ExtractionResult`, and `run_extraction` to `src/extraction/__init__.py`:

```python
from extraction.contracts import RunExtractionRequestV1
from extraction.orchestrator import ExtractionResult, run_extraction
```

This makes the package boundary explicit: callers import from `extraction` (stable surface) rather than from `extraction.orchestrator` (internal module).

**Effort:** small
**Risk if unaddressed:** low — no correctness issue; ergonomic friction for callers

---

### Category: Quality

#### P23. Design for testability — Alignment: 2/3

**Current state:**

The per-extractor `builders.py` files contain largely pure functions (row construction from AST nodes, CST nodes, bytecode instructions). These are well-isolated and directly testable — `test_ast_extract.py` confirms this approach is used. The problem is in the runtime files: `extract_ast_tables`, `extract_cst_tables`, etc. in `builders_runtime.py` require an `ExtractSession` (which requires a live DataFusion `SessionContext`) to test end-to-end.

The unit test for the orchestrator (`tests/unit/test_extraction_orchestrator.py`) relies heavily on `monkeypatch.setattr` against module-level private functions (`_run_repo_scan`, `_build_stage1_extractors`, `_write_delta`, etc.). This is a consequence of the orchestrator lacking injectable collaborators for its stage functions — it calls them by name rather than through a dispatch table or protocol.

The `_delta_write_ctx` function (orchestrator line 193–198) is cached with `@cache` and creates a `SessionContext` as a module-level singleton. This makes the Delta write path untestable without a real filesystem.

**Findings:**
- `src/extraction/orchestrator.py:193–198` — `@cache` on `_delta_write_ctx()` makes the Delta write path a hidden singleton; tests must patch `_write_delta` at the module level
- `src/extraction/orchestrator.py:83–163` — `run_extraction` has no injectable stage collaborators; all stage functions are module-level privates
- `src/extract/extractors/ast/builders_runtime.py` — `extract_ast_tables` requires live `ExtractSession`; no protocol/stub exists

**Suggested improvement:**

Introduce an `ExtractionStages` protocol or dataclass in `src/extraction/orchestrator.py` that bundles the stage callables:

```python
@dataclass(frozen=True)
class ExtractionStages:
    run_repo_scan: Callable[..., dict[str, pa.Table]]
    build_stage1_extractors: Callable[..., dict[str, Callable[[], pa.Table]]]
    run_python_imports: Callable[..., pa.Table]
    run_python_external: Callable[..., pa.Table]
    write_delta: Callable[[pa.Table, Path, str], str]
```

`run_extraction` accepts an optional `stages: ExtractionStages | None = None`, defaulting to the real implementations. Tests pass stub stages directly without monkeypatching. This eliminates the `@cache` singleton concern for the Delta context as well — `write_delta` becomes injectable.

**Effort:** medium
**Risk if unaddressed:** medium — the current monkeypatch approach couples test structure to internal function names; any renaming of `_run_repo_scan` or `_write_delta` silently breaks tests

---

#### P24. Observability — Alignment: 2/3

**Current state:**

Observability is present and structured: `stage_span`, `record_error`, `record_stage_duration` from `obs.otel` are used consistently across all stage functions in `orchestrator.py`. The `ExtractQualityEvent` pattern in `materialization.py` provides typed error recording.

One measurement error: in `_run_parallel_stage1_extractors` (orchestrator.py:290–324), individual extractor timing is recorded as:

```python
state.timing[name] = time.monotonic() - stage_start
```

where `stage_start` is set once before the `ThreadPoolExecutor` block (line 302). Since futures run in parallel, each extractor's elapsed time is measured from the start of the _entire parallel batch_, not from when that extractor began. The timing values for individual extractors will all be approximately equal to the wall-clock time of the longest extractor in the batch, rather than reflecting individual extractor durations.

**Findings:**
- `src/extraction/orchestrator.py:302–324` — `stage_start` shared across all parallel futures; individual extractor timings reflect wall clock from batch start, not individual execution time
- `src/extract/coordination/materialization.py:326–401` — `ExtractQualityEvent` records errors for three post-execution operations; the event list is created inline (not named), making correlation across events harder

**Suggested improvement:**

Capture per-extractor start time before submitting to the executor:

```python
start_times = {name: time.monotonic() for name in extractors}
futures = {name: executor.submit(fn) for name, fn in extractors.items()}
for name, future in futures.items():
    state.timing[name] = time.monotonic() - start_times[name]
```

For true parallel timing, capture `t0` per future result, not per batch start.

**Effort:** small (3-line change)
**Risk if unaddressed:** low — no correctness impact; misleading metrics in profiling output

---

## Cross-Cutting Themes

### Theme 1: The builders.py / builders_runtime.py split is the root cause of P1 and P19

The split was likely introduced to separate "pure data transformation" (row construction from parse trees) from "IO and orchestration" (parallel map, cache, DataFusion sessions). The intent is sound but the boundary is not enforced: `builders_runtime.py` imports 10–12 private symbols from `builders.py`, making the split fictional at the Python level. The consequence is 4,000+ LOC of near-duplicate orchestration scaffolding across four extractor pairs, with no shared loop infrastructure. Fixing this (merging the files and extracting a shared runtime loop) is the highest-impact structural improvement in this scope.

**Affected principles:** P1, P19, P23
**Root cause:** absence of a shared extraction runtime loop; file split attempted to compensate but created a worse information-hiding violation than the original monolithic file
**Suggested approach:** Phase 1 — merge `builders.py` + `builders_runtime.py` per extractor, exposing only public API. Phase 2 — extract the common worklist/parallel/materialize/cache loop into `src/extract/coordination/extraction_runtime_loop.py`.

---

### Theme 2: `src/extraction/orchestrator.py` carries migration residue

Three `inspect.signature` checks (P21), inline schema literals (P20), and the `@cache` singleton (P23) all indicate the orchestrator was assembled during a migration from a different architecture. The functions it introspects already have the parameters it checks for. Cleaning these residues is low-effort and high-signal for future readers.

**Affected principles:** P19, P20, P21, P23
**Root cause:** incremental migration left defensive checks that are no longer needed
**Suggested approach:** Remove `inspect.signature` gating, replace inline schema with registry lookup, and make `_write_delta` injectable.

---

### Theme 3: The `src/extract/` → `src/extraction/` dependency inversion (P5)

`src/extract/session.py` and `src/extract/coordination/context.py` import `EngineSession`, `RuntimeProfileSpec`, and `resolve_runtime_profile` from `src/extraction/`. If `src/extraction/` is the "orchestration shell" and `src/extract/` is the "core evidence production," the import direction is inverted. This is not yet causing problems but constrains the ability to test `src/extract/` in isolation.

**Affected principles:** P5, P23
**Root cause:** `EngineSession` and `RuntimeProfileSpec` are defined in `src/extraction/` but needed by `src/extract/` session wrappers
**Suggested approach:** Move `EngineSession` and `RuntimeProfileSpec` to a shared layer (`src/extract/engine/` or `src/engine_core/`) imported by both; or define `ExtractSession` in terms of protocols so it does not import concrete orchestration types.

---

### Theme 4: `src/extraction/` is correctly scoped and should NOT be consolidated

The agent-specific instructions flag `src/extraction/` (13 files, ~3.7K LOC) as a potential consolidation candidate. Based on this review, consolidation is not warranted. `src/extraction/` serves a clearly distinct concern: staged, parallelized, Delta-persisted orchestration with fallback handling, runtime profile management, and Rust bridge wiring. `src/extract/` owns evidence production primitives. The boundary is coherent; the primary issue is the P5 inversion at the session layer, not architectural redundancy.

---

## Rust Migration Candidates

**Tree-sitter extraction (`src/extract/extractors/tree_sitter/`)** is the strongest Rust migration candidate. Tree-sitter is a C library with first-class Rust bindings (`tree-sitter-python` crate). The Python `tree_sitter` bindings wrap the same C library. Migrating tree-sitter extraction to Rust would:
- Eliminate the GIL bottleneck for concurrent parsing (currently requires `ProcessPoolExecutor` or GIL-disabled Python)
- Use `tree-sitter-python` Rust bindings directly, removing the Python FFI overhead
- Allow the parse results to be emitted directly as Arrow `RecordBatch` via the `arrow2` or `arrow` Rust crates, skipping the Python dict accumulation in `_QueryCollector`

**Assessment:** `extract_ts_tables` is the clearest bottleneck target. The pure query/match logic in `builders.py` and `visitors.py` maps naturally to Rust structs. The `TreeSitterCache` (incremental reparse with `InputEdit`) would benefit significantly from Rust ownership semantics.

**Estimated LOC reduction in Python:** ~1,300 LOC (combined `builders.py` + `builders_runtime.py` + `visitors.py` for tree-sitter could become ~200 LOC of Rust FFI bridge + ~100 LOC Python entrypoint).

**AST and bytecode extraction** are Python-interpreter-native (`ast` and `dis` modules) and have no Rust affinity — these should remain in Python.

**SCIP extraction** is already a subprocess bridge to a Rust binary (`rust-analyzer`/`scip-python`); no migration needed.

**Priority ranking:**
1. Tree-sitter (highest ROI — bottleneck, natural Rust affinity, clear boundary)
2. CST/LibCST (possible Rust tree-sitter backend for CST extraction, but LibCST provides semantic analysis Python cannot replicate easily)
3. File index / line offset computation (trivial Rust win, already `mmap`-based in Python)

---

## DF52 Migration Impact

**Direct impact: Minimal.** The extraction pipeline does not implement custom `TableProvider` or `FileSource` adapters — it consumes DataFusion as a query engine over Arrow tables ingested via `datafusion_from_arrow`. The DF52 breaking changes to `FileSource::with_projection`, `FileScanConfig`, and `PhysicalExprAdapter` are in the DataFusion core and do not affect the extraction layer's DataFusion usage pattern.

**Indirect impact to check:**

1. **`scan_file_line_index_plan` in `src/extract/extractors/file_index/line_index.py`** — This function returns a `DataFusionPlanArtifact`. If it uses `FileScanConfig` builder patterns internally, the DF52 projection API change (`FileScanConfigBuilder::with_projection_indices` now returns `Result<Self>`) applies.

2. **`worklist_builder` in `src/extract/infrastructure/worklists.py`** — Uses `ctx.table(repo_table)` and join patterns. These are stable DataFrame-level APIs unaffected by DF52.

3. **DF52 file statistics cache** — The new `statistics_cache()` and `list_files_cache()` APIs in DF52 could benefit the `scan_repo` path if it registers file metadata tables. Not currently used; adding it would reduce repeated `LIST` operations in incremental extraction runs.

**Recommendation:** No immediate DF52 migration work required in the extraction layer. Track `scan_file_line_index_plan` for the projection API change if DF52 is adopted.

---

## Planning-Object Consolidation

**Finding:** The extraction layer does not contain bespoke planning objects that replicate DataFusion built-ins. The `DataFusionPlanArtifact` / `PlanBundleOptions` / `build_plan_artifact` pattern in `extract_plan_builder.py` is a thin wrapper over DF's logical plan, not a reimplementation. `apply_query_and_project` (lines 226–259) constructs column projections using `dataset_schema` and passes them to `apply_query_spec` — this is appropriate delegation.

**No consolidation required** for planning objects in this scope. The extraction layer correctly treats DataFusion plans as opaque artifacts built by the `datafusion_engine` layer.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P21 Least astonishment | Remove 3× `inspect.signature` checks in `orchestrator.py:230,342,378` — all target functions already have `execution_bundle` | small | Removes dead code, cleans up import of `inspect` |
| 2 | P20 YAGNI | Replace inline `repo_files_schema` literal at `orchestrator.py:586–596` with `dataset_schema("repo_files_v1")` from the registry | small | Eliminates schema drift risk |
| 3 | P24 Observability | Fix per-extractor timing in `_run_parallel_stage1_extractors`: capture `start_times` per extractor before submitting futures | small | Accurate per-extractor telemetry |
| 4 | P22 Public contracts | Add `RunExtractionRequestV1`, `ExtractionResult`, `run_extraction` to `src/extraction/__init__.py` | small | Clear stable package surface |
| 5 | P3 SRP | Replace pydantic `_RuntimeProfileEnvPatchRuntime` in `runtime_profile.py:95–114` with a msgspec `Struct` + `convert` | small | Removes pydantic from this module; aligns with project constraints |

---

## Recommended Action Sequence

1. **[Quick win] Remove `inspect.signature` checks** in `src/extraction/orchestrator.py:229–240, 341–348, 377–386`. Call `_run_repo_scan`, `_run_python_imports`, `_run_python_external` directly. Delete the `import inspect` at line 14.

2. **[Quick win] Replace inline schema** at `src/extraction/orchestrator.py:586–596` with `dataset_schema("repo_files_v1")` from `datafusion_engine.extract.registry`.

3. **[Quick win] Fix parallel extractor timing** in `_run_parallel_stage1_extractors` by capturing per-extractor start times before the `ThreadPoolExecutor` submits futures.

4. **[Quick win] Add `RunExtractionRequestV1`, `ExtractionResult`, `run_extraction`** to `src/extraction/__init__.py`.

5. **[Quick win] Replace pydantic** `_RuntimeProfileEnvPatchRuntime` in `runtime_profile.py` with an equivalent msgspec `Struct` and the `convert` helper.

6. **[Medium] Merge `builders.py` + `builders_runtime.py`** for all four extractors (AST, CST, bytecode, tree-sitter). Expose only the public API through each extractor's `__init__.py`. This eliminates the private-symbol cross-module imports (P1) and is a prerequisite for step 7.

7. **[Large] Extract shared runtime loop** into `src/extract/coordination/extraction_runtime_loop.py`. Reduce each extractor's runtime file to an adapter (~100 LOC) that provides the extractor-specific row function and options. This is the highest-impact structural improvement in this scope (P19, P23).

8. **[Medium] Resolve dependency inversion**: move `EngineSession` and `RuntimeProfileSpec` out of `src/extraction/` into a shared location (e.g., `src/extract/engine/`) so `src/extract/session.py` does not import from the orchestration layer (P5, P23).

9. **[Medium] Make `run_extraction` stages injectable** via an `ExtractionStages` dataclass, replacing the `@cache` singleton for `_delta_write_ctx` and eliminating monkeypatch-based test coupling (P23).
