# Design Review: Extraction Pipeline

**Date:** 2026-02-18
**Scope:** `src/extract/` AND `src/extraction/`
**Focus:** All principles (1-24)
**Depth:** deep (all 81 files)
**Files reviewed:** 81

---

## Executive Summary

The extraction pipeline is structurally sound and has made real progress toward
principled design: a shared `ExtractionRowBuilder` / `ExtractionBatchBuilder` pair
eliminates per-extractor row-construction boilerplate, the `EvidencePlan` + registry
gating pattern is clean and declarative, and the `ExtractionSchemaBuilder` fluent API
provides a uniform schema-construction surface. The coordination layer
(`extraction_runtime_loop.py`, `worklists.py`, `materialization.py`) is well-separated
from extractor-specific logic.

The most pressing issues are: (1) each builders file (`ast/builders.py`,
`bytecode/builders.py`, `cst/builders.py`, and friends) repeats the same ~150-200 line
runtime-dispatch/plan-building block verbatim, amounting to roughly 1,500-2,000 lines of
structural duplication across extractor implementations; (2) `SpanSpec` and
`SpanTemplateSpec` are two nearly-identical types in sibling modules
(`coordination/context.py:174` vs `row_builder.py:21`) with adapter glue bridging them,
a textbook DRY violation; (3) `extraction/orchestrator.py` uses `inspect.signature` at
runtime to detect whether optional parameters exist — a hard-coded compatibility shim
that should be a protocol or versioned adapter; (4) `extraction/runtime_profile.py`
imports Pydantic in what is otherwise a `msgspec`-only module boundary, violating the
hot-path convention stated in `AGENTS.md`. Addressing the structural duplication and the
dual-span type are the highest-leverage improvements.

---

## Alignment Scorecard

| #  | Principle                        | Alignment | Effort | Risk   | Key Finding                                                                   |
|----|----------------------------------|-----------|--------|--------|-------------------------------------------------------------------------------|
| 1  | Information hiding               | 2         | small  | low    | `SpanSpec` / `SpanTemplateSpec` adapter glue leaks internal span representation |
| 2  | Separation of concerns           | 2         | medium | low    | Runtime dispatch & plan-building mixed into every builders file               |
| 3  | SRP                              | 1         | medium | medium | `ast/builders.py` (1,200+ LOC) is simultaneously a row factory, a cache manager, a plan builder, and an API surface |
| 4  | High cohesion, low coupling      | 2         | medium | low    | Extractor `builders.py` files import from 8+ coordination modules each        |
| 5  | Dependency direction             | 3         | —      | —      | Core extraction logic has no upward dependencies; adapters depend on core    |
| 6  | Ports & Adapters                 | 2         | medium | low    | Rust bridge is cleanly wrapped, but extractor dispatch relies on `inspect.signature` duck-typing |
| 7  | DRY                              | 1         | large  | high   | `SpanSpec` duplicated; ~1,500 LOC of runtime dispatch/plan-building repeated across 4+ builders files |
| 8  | Design by contract               | 2         | small  | low    | `build_extraction_rows` preconditions implicit; output contracts verified post-hoc via `validate_extract_output` |
| 9  | Parse, don't validate            | 2         | small  | low    | `RepoFileRow` msgspec parse at boundary is correct; `normalize_extraction_options` handles messy mapping input well |
| 10 | Make illegal states unrepresentable | 2      | small  | low    | `SpanSpec` allows all-None fields without structural enforcement              |
| 11 | CQS                              | 2         | small  | low    | `_write_and_record_extract_output` both writes and registers a view as a side effect |
| 12 | Dependency inversion + explicit composition | 2 | medium | low | `_build_extract_execution_bundle` is a module-level factory with no injection seam |
| 13 | Prefer composition over inheritance | 2      | small  | low    | Options mixin chains (`ParallelOptions(WorkerOptions, BatchOptions)`) are shallow; acceptable |
| 14 | Law of Demeter                   | 2         | small  | low    | `exec_context.ensure_session().runtime_profile.diagnostics.capture_plan_artifacts` traverses four levels |
| 15 | Tell, don't ask                  | 2         | small  | low    | `materialize_extract_plan` asks for type via `isinstance(result, pa.RecordBatchReader)` branch logic |
| 16 | Functional core, imperative shell | 2        | medium | medium | Cache read/write smeared through `_extract_ast_for_context` alongside pure logic |
| 17 | Idempotency                      | 3         | —      | —      | Delta `overwrite` mode + SHA-based worklist ensures idempotent re-runs        |
| 18 | Determinism / reproducibility    | 3         | —      | —      | `stable_cache_key`, `policy_hash`, `ddl_fingerprint` pattern in place         |
| 19 | KISS                             | 2         | small  | low    | `_delta_write_ctx` is a `@cache`-decorated module-level singleton hidden inside `orchestrator.py:194` |
| 20 | YAGNI                            | 2         | small  | low    | `_AstTablesKwargs` / `_AstTablesKwargsTable` / `_AstTablesKwargsReader` — three TypedDicts for one overload pair |
| 21 | Least astonishment               | 2         | small  | low    | `extract_ast` returns `ExtractResult[TableLike]`; `extract_ast_tables` returns `Mapping[str, TableLike]` — two naming conventions for similar operations |
| 22 | Declare and version public contracts | 2     | small  | low    | `_SEMANTIC_INPUT_CANDIDATES` in `contracts.py` is undocumented and unversioned |
| 23 | Design for testability           | 1         | medium | medium | Builders file-level cache singletons and module-level `@cache` decorators make unit isolation impractical without monkey-patching |
| 24 | Observability                    | 2         | small  | low    | `EngineEventRecorder` centralizes events well; `extraction/orchestrator.py` timing uses `time.monotonic()` but does not emit structured spans per extractor |

---

## Detailed Findings

### Category: Boundaries (1-6)

#### P1. Information hiding — Alignment: 2/3

**Current state:**
The `extract/` package uses a lazy `__getattr__` dispatch pattern in `src/extract/__init__.py:195`
that is a well-executed information-hiding mechanism: callers never import sub-modules
directly, and the stable `__all__` list defines the public surface clearly.

**Findings:**
- `src/extract/coordination/context.py:174` defines `SpanSpec`, and
  `src/extract/row_builder.py:21` defines `SpanTemplateSpec`. These are nearly identical
  value objects (both hold `start_line0`, `start_col`, `end_line0`, `end_col`,
  `end_exclusive`, `col_unit`, `byte_start`, `byte_len`). The adapter function
  `span_dict` at `context.py:220-244` converts `SpanSpec` to `SpanTemplateSpec` before
  delegating to `row_builder`. This means the internal representation (`SpanSpec`) is
  not hidden — callers in `ast/builders.py`, `cst/builders.py`, and others must import
  `SpanSpec` and understand both types exist.
- `src/extract/extractors/ast/setup.py:32` exports `_format_feature_version` and
  `_resolve_feature_version` (leading-underscore names) from the package — these are
  imported by `ast/builders.py:520-522` via an explicit import rather than being
  package-internal, leaking private functions across module boundaries.

**Suggested improvement:**
Collapse `SpanSpec` and `SpanTemplateSpec` into a single canonical type (keep
`SpanTemplateSpec` in `row_builder.py` which is the more general one), remove the
adapter, and update `context.py` to re-export the single type. For the `_resolve_feature_version`
leak, either move the function to `builders.py` (it is only called there) or make it part
of `AstExtractOptions` as a method: `options.resolve_feature_version(contexts)`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P2. Separation of concerns — Alignment: 2/3

**Current state:**
The coordination layer (`extraction_runtime_loop.py`, `materialization.py`,
`extract_plan_builder.py`) successfully isolates the runtime-dispatch plumbing from
extractor domain logic. However, individual builders files conflate three distinct
concerns: (a) the domain-specific row construction logic, (b) the cache management
protocol, and (c) the plan/materialization orchestration. All three live in the same
file with no separation.

**Findings:**
- `src/extract/extractors/ast/builders.py` begins at line 1 with AST-specific row
  construction (`_span_spec_from_node`, `_def_row`, `_call_row`, etc.) and at line 488
  switches to importing the full runtime infrastructure (`ExtractExecutionContext`,
  `iter_runtime_rows`, `materialize_extract_plan`) to implement the public API. These
  two sections have very different reasons to change.
- The same split is visible in `src/extract/extractors/bytecode/builders.py:486` and
  `src/extract/extractors/cst/builders.py:511` — each file has a distinct "top half"
  (pure domain logic) and "bottom half" (IO/orchestration plumbing) but they are
  physically merged into a single module.
- `src/extraction/orchestrator.py:83-163` mixes staging logic (repo scan, parallel
  stage 1, python imports, python external) with Delta IO (`_write_delta` at line 407),
  error recording, and OTel span management within a single function chain. The IO
  concern (`_write_delta`) is entangled with the orchestration concern.

**Suggested improvement:**
Split each `builders.py` into two files: `{extractor}/extraction.py` (pure domain rows,
visitors, span logic — no IO) and `{extractor}/runtime.py` (the `extract_*_tables` entry
points, session wiring, plan building). The `visitors.py` / `builders.py` / `setup.py`
naming convention is already almost there; the runtime layer just needs its own file.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P3. SRP — Alignment: 1/3

**Current state:**
`src/extract/extractors/ast/builders.py` has at least five distinct responsibilities:
(1) AST node row construction (`_node_row`, `_edge_row`, `_docstring_row`, etc.),
(2) AST parsing orchestration (`_parse_ast_text`, `_parse_via_compile`, `_parse_via_ast_parse`),
(3) cache management (`_cache_key`, cache_get/set/lock calls),
(4) parallel row iteration (`_iter_ast_rows`, `_iter_ast_row_batches`), and
(5) public API surface (`extract_ast`, `extract_ast_tables`). The file has grown to over
1,200 lines. The same pattern repeats in `bytecode/builders.py` (750+ lines) and
`cst/builders.py` (900+ lines).

**Findings:**
- `src/extract/extractors/ast/builders.py:857-914`: `_extract_ast_for_context` mixes
  cache lookup, cache locking, text parsing, AST walking, and cache storing in a single
  60-line function. A change to the cache eviction policy requires editing the same
  function as a change to the AST parsing algorithm.
- `src/extract/extractors/ast/builders.py:926-964`: `extract_ast` is the simple entry
  point but repeats session setup, normalize option construction, and plan materialization
  that also appear identically in `extract_ast_tables` at lines 1175-1232. Two functions
  with the same session-setup preamble indicate that the preamble belongs in a shared
  helper.
- `src/extraction/runtime_profile.py` has at least three concerns: profile construction
  (`resolve_runtime_profile`), Rust bridge snapshot collection
  (`_collect_rust_profile_snapshot`), diagnostics event emission
  (`engine_runtime_artifact`), and environment override parsing
  (`_runtime_profile_env_patch`). It imports both `pydantic` and `msgspec` for what is
  nominally a profile-resolution concern.

**Suggested improvement:**
For builders files, extract a `_{extractor}_cache.py` module holding only the
cache-key derivation and cache read/write helpers, and move the runtime entry points
(`extract_*`, `extract_*_tables`) to a thin `_{extractor}_api.py` or keep them in
`builders.py` but remove everything except the public API. For `runtime_profile.py`,
move environment patching to a dedicated `env_profile.py` module and diagnostics
artifact emission to `extraction/diagnostics.py` (already exists and is the right home).

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P4. High cohesion, low coupling — Alignment: 2/3

**Current state:**
Each extractor `builders.py` imports from 8-10 modules across `coordination/`,
`infrastructure/`, `extraction/`, `datafusion_engine/`, and `obs/`. This is partly
unavoidable given the architecture, but some coupling is incidental.

**Findings:**
- `src/extract/extractors/ast/builders.py:488-546` imports 18 symbols from 9 modules
  in the "bottom half" block. This is a high fan-in that means any of 9 modules
  changing can force a re-parse of `ast/builders.py`.
- `src/extraction/orchestrator.py:194`: `@cache` on `_delta_write_ctx()` creates a
  module-level singleton that ties the orchestrator permanently to the native bridge
  at first call. There is no way to inject an alternate session (e.g., for testing)
  without patching `extraction.orchestrator._delta_write_ctx`.
- `src/extract/coordination/materialization.py:59`: `from extraction.diagnostics import
  EngineEventRecorder, ExtractQualityEvent` — the `coordination/` layer imports from
  `extraction/`, which is the outer orchestration layer. This is a reverse-direction
  dependency.

**Suggested improvement:**
Move `EngineEventRecorder` and `ExtractQualityEvent` from `extraction/diagnostics.py` to
`extract/coordination/diagnostics.py` so the coordination layer is self-contained.
Alternatively, define a protocol in `coordination/` that `EngineEventRecorder` satisfies
and pass it in, eliminating the dependency direction violation. For `_delta_write_ctx`,
accept a `SessionContext | None` parameter in `_write_delta`, defaulting to the cached
version only when `None`.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P5. Dependency direction — Alignment: 3/3

**Current state:**
Core extraction logic (`row_builder.py`, `coordination/context.py`, `schema_derivation.py`)
depends only on PyArrow and `schema_spec` / `datafusion_engine.arrow` — well-behaved
leaves. The Rust bridge (`rust_session_bridge.py`) wraps `datafusion_ext` as an adapter.
No inversion detected.

**Suggested improvement:** None required.

---

#### P6. Ports & Adapters — Alignment: 2/3

**Current state:**
`src/extraction/rust_session_bridge.py` is a clean adapter: it wraps the native Rust
`datafusion_ext` extension and normalizes its output to a standard `SessionContext`,
raising typed errors on unexpected payloads. This is well-designed.

**Findings:**
- `src/extraction/orchestrator.py:229-240` uses `inspect.signature` to detect whether
  extractor functions accept an `execution_bundle` parameter:
  ```python
  if "execution_bundle" in inspect.signature(scan_fn).parameters:
      outputs = scan_fn(repo_root, options=options, execution_bundle=execution_bundle)
  else:
      outputs = scan_fn(repo_root, options=options)
  ```
  This is a runtime compatibility shim: the orchestrator is checking whether the
  function it calls conforms to an old or new interface. The same pattern repeats at
  lines 342 and 378 for `_run_python_imports` and `_run_python_external`. This is
  duck-typing in lieu of a proper versioned port.
- `src/extraction/contracts.py:30-44`: `_SEMANTIC_INPUT_CANDIDATES` is a hardcoded
  dict mapping semantic input names to extraction output candidates. When a new extractor
  is added, this mapping must be updated manually, with no mechanism to detect that it
  is out of date. This is a brittle point-of-truth.

**Suggested improvement:**
Remove the `inspect.signature` branches. Since `_run_repo_scan`, `_run_python_imports`,
and `_run_python_external` are private module-level functions, simply update them to
always accept `execution_bundle` as a keyword argument (or always pass it as a
positional argument). The compatibility detection was presumably added to support an
old signature during migration; the migration should be completed. For
`_SEMANTIC_INPUT_CANDIDATES`, consider moving this mapping into the extractor registry
metadata (as an `output_aliases` field on `ExtractMetadata`) so it is co-located with
the extractor definition.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Knowledge (7-11)

#### P7. DRY — Alignment: 1/3

**Current state:**
This is the most significant structural problem in the codebase. There are two distinct
duplication clusters.

**Findings:**

**Cluster A — Span type duplication:**
`src/extract/coordination/context.py:174-184` defines `SpanSpec` with eight fields.
`src/extract/row_builder.py:21-36` defines `SpanTemplateSpec` with the identical eight
fields. The adapter function `span_dict` at `context.py:220-244` converts between them
with a 24-line field-by-field mapping. This is pure semantic duplication: both types
represent the same invariant (a multi-coordinate span). Any new field requires updating
both types and the adapter.

**Cluster B — Runtime dispatch block duplication:**
Each extractor `builders.py` contains a near-identical block of 120-180 lines
implementing the public `extract_*` and `extract_*_tables` entry points, with identical
structure:
1. `normalize_options(...)` call
2. `ExtractExecutionContext()` construction
3. `exec_context.ensure_session()` + `replace(exec_context, session=session)`
4. `exec_context.ensure_runtime_profile()` + `exec_context.determinism_tier()`
5. `ExtractNormalizeOptions(options=...)` construction
6. `extract_*_plans(...)` call
7. `materialize_extract_plan(...)` call

This block appears in:
- `src/extract/extractors/ast/builders.py:926-964` (`extract_ast`) and `1175-1232` (`extract_ast_tables`)
- `src/extract/extractors/bytecode/builders.py` (symmetric pattern, ~lines 530-650)
- `src/extract/extractors/cst/builders.py` (symmetric pattern, ~lines 511+)
- `src/extract/extractors/symtable_extract.py` (same pattern)
- `src/extract/extractors/tree_sitter/builders.py` (same pattern)

The total structural duplication across these blocks is conservatively 1,000-1,500 lines.

**Suggested improvement:**

For Cluster A: Delete `SpanSpec` from `context.py`. Move all `SpanTemplateSpec` usage to
be the canonical type. Update the three extractor builders that import `SpanSpec` to
import `SpanTemplateSpec` directly from `extract.row_builder`. The adapter
`span_dict` in `context.py` simply wraps `make_span_spec_dict` — collapse it to
a re-export alias.

For Cluster B: Introduce a generic helper function in `extract/coordination/`:
```python
def run_extract_entry_point(
    extractor_name: str,
    repo_files: TableLike,
    options: T_Options,
    options_type: type[T_Options],
    plan_builder: Callable[[TableLike, T_Options, ExtractExecutionContext], dict[str, DataFusionPlanArtifact]],
    *,
    context: ExtractExecutionContext | None = None,
    prefer_reader: bool = False,
    apply_post_kernels: bool = True,
) -> Mapping[str, TableLike | RecordBatchReaderLike]: ...
```
Each `extract_*_tables` function then becomes a 5-10 line thin wrapper that passes its
specific `plan_builder`. This reduces the duplication to a single implementation.

**Effort:** large
**Risk if unaddressed:** high — drift between extractor implementations is already
observable (e.g., `symtable_extract.py` uses `WorkerOptions` while others use
`ParallelOptions`; `ast` uses `batch_size=512` default while `symtable` has no batch
support) and will worsen as extractors evolve independently.

---

#### P8. Design by contract — Alignment: 2/3

**Current state:**
Schema contracts are validated post-hoc via `_validate_extract_schema_contract` in
`materialize_extract_plan`, and `validate_extract_output` provides a finalize-then-check
pattern. This is better than nothing but contracts are not stated at function entry.

**Findings:**
- `src/extract/row_builder.py:559-588`: `build_extraction_rows` has no precondition
  check that `file_ctx.file_id` and `file_ctx.path` are non-empty, yet downstream
  code (`_extract_ast_for_context:864`) does check `if not file_ctx.file_id or not
  file_ctx.path: return None`. The contract is implicitly enforced in callers rather
  than at the boundary.
- `src/extract/coordination/evidence_plan.py:105-147`: `compile_evidence_plan`
  silently accepts `rules=None` and returns an all-sources plan. There is no documented
  contract clarifying when `None` is valid vs when it is a caller error.
- `src/extraction/orchestrator.py:407-443`: `_write_delta` raises `TypeError` on bad
  `table` input but does not check that `location` is writable or that `name` is a
  valid table name — these are implicit preconditions.

**Suggested improvement:**
Add `assert file_ctx.file_id` / `assert file_ctx.path` guards (or explicit ValueError)
at `build_extraction_rows` entry and at `ExtractionRowBuilder.from_file_context`. Add a
docstring contract to `compile_evidence_plan` clarifying that `rules=None` means
"require all extractors." These are small, low-risk additions.

**Effort:** small
**Risk if unaddressed:** low

---

#### P9. Parse, don't validate — Alignment: 2/3

**Current state:**
`FileContext.from_repo_row` correctly uses `msgspec` to parse and validate the incoming
row mapping into `RepoFileRow` at `context.py:62-67` before constructing `FileContext`.
`normalize_extraction_options` in `extraction/options.py:46-148` is an exemplary parse
function that handles multiple legacy field shapes into a canonical `ExtractionRunOptions`.

**Findings:**
- `src/extract/extractors/ast/builders.py:864`: `_extract_ast_for_context` checks
  `if not file_ctx.file_id or not file_ctx.path: return None` rather than requiring
  these fields to be non-empty at parse time. This means a `FileContext` with empty
  `file_id` is representable and must be validated at every callsite.
- `src/extraction/orchestrator.py:794-796`: `scip_index_path = getattr(scip_index_config, "index_path_override", None)` followed by a fallback `getattr(..., "scip_index_path", None)` is late validation/parsing of a config object rather than parsing at the boundary where `scip_index_config` enters the system. `ScipIndexConfig` in `contracts.py:13` is declared as an empty Protocol with no required attributes, meaning the parse step is deferred to deep inside `_extract_scip`.

**Suggested improvement:**
Make `FileContext.__init__` or `from_repo_row` assert non-empty `file_id` and `path`,
converting the late check into a construction-time guarantee. For `ScipIndexConfig`,
add the required `index_path_override: str | None` attribute to the Protocol definition
so callers know the contract.

**Effort:** small
**Risk if unaddressed:** low

---

#### P10. Make illegal states unrepresentable — Alignment: 2/3

**Current state:**
The use of frozen dataclasses throughout (`FileContext`, `ExtractExecutionContext`,
`SpanTemplateSpec`, `ExtractionRowBuilder`) prevents post-construction mutation, which
is good. Option types are well-typed.

**Findings:**
- `src/extract/coordination/context.py:174-184`: `SpanSpec` allows all eight fields to
  be `None`. A span where `start_line0 is None and end_line0 is None and byte_start is
  None and byte_len is None` is logically meaningless but structurally valid.
- `src/extract/row_builder.py:21-36`: `SpanTemplateSpec` has the same all-None
  allowance. The function `_span_dict_from_spec` at `row_builder.py:50` explicitly
  handles the all-None case by returning `None`, meaning callers receive a nullable
  dict where they presumably expect a span. A `SpanSpec` with partial byte or line
  coverage (e.g., `byte_start` set but `byte_len=None`) is also representable.
- `src/extract/infrastructure/options.py:29-42`: `ParallelOptions` inherits from both
  `WorkerOptions` and `BatchOptions` using dataclass inheritance. Both parent classes
  define `batch_size: int | None = None`, and `ParallelOptions` overrides it to
  `batch_size: int | None = 512`. Python dataclass MRO handles this, but the three
  related classes define `batch_size` in two places — a subtle knowledge duplication.

**Suggested improvement:**
Introduce a minimal `ByteSpan` type with `bstart: int` and `bend: int` (both required,
non-nullable) and a separate `LineColSpan` type for line/column positions. These replace
the all-optional `SpanSpec` / `SpanTemplateSpec`. Callers that actually have byte spans
use `ByteSpan`; callers with line/col info use `LineColSpan`. This eliminates the
meaningless all-None state.

**Effort:** medium
**Risk if unaddressed:** low

---

#### P11. CQS — Alignment: 2/3

**Current state:**
Most query functions are pure. The clearest CQS violations are in the write path.

**Findings:**
- `src/extract/coordination/materialization.py:326-401`: `_write_and_record_extract_output`
  both writes data to Delta storage (a command) and registers a view in the DataFusion
  session (a second command) and records quality events (a third command) and records view
  artifacts (a fourth command), all in one function. It has four distinct side effects.
- `src/extract/coordination/worklists.py:150-172`: `_stream_with_queue` both yields
  `FileContext` items (a query) and writes them into a `diskcache.Deque` and `Index`
  (a command). The generator is a combined query/command.
- `src/extract/coordination/materialization.py:431-509`: `materialize_extract_plan`
  returns a value (query) while also writing to Delta storage via
  `_write_and_record_extract_output` (command). A caller cannot call this function to
  get the materialized table without triggering the write.

**Suggested improvement:**
Split `materialize_extract_plan` into two functions: `execute_extract_plan` (pure query,
returns the normalized table/reader) and `persist_extract_output` (command, writes to
Delta and registers the view). The current function becomes a composition of the two.
For `_stream_with_queue`, make the queue-population a separate phase after all contexts
are collected.

**Effort:** medium
**Risk if unaddressed:** low

---

### Category: Composition (12-15)

#### P12. Dependency inversion + explicit composition — Alignment: 2/3

**Current state:**
`ExtractSession` and `ExtractExecutionContext` are passed explicitly through the
extraction call chain, which is good. However, two factory patterns resist injection.

**Findings:**
- `src/extraction/orchestrator.py:175-190`: `_build_extract_execution_bundle()` is
  a zero-argument factory that creates a full `RuntimeProfileSpec` + `ExtractSession`
  internally. It is called as a default at multiple points: `line 127`, `line 847`,
  `line 898`. Any test that exercises `run_extraction` without patching this function
  will spin up a real Rust-backed DataFusion session.
- `src/extraction/orchestrator.py:193-198`: `@cache` on `_delta_write_ctx()` creates
  an implicit module-level singleton. There is no way to inject an alternate
  `SessionContext` for Delta writes without monkey-patching the module.
- `src/extract/coordination/worklists.py:176-182`: `_resolve_runtime_profile` calls
  `create_runtime_profile()` when no profile is provided — a hidden factory call. While
  it serves as a fallback, it means tests cannot easily exercise the worklist code with
  a lightweight stub profile.

**Suggested improvement:**
Add a `bundle_factory: Callable[[], _ExtractExecutionBundle] | None = None` parameter
to `run_extraction`, defaulting to the current `_build_extract_execution_bundle`. For
`_delta_write_ctx`, accept a `write_ctx: SessionContext | None = None` parameter in
`_write_delta` and use the cached singleton only when `None`. Both changes are
backward-compatible and make tests injectable.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P13. Prefer composition over inheritance — Alignment: 2/3

**Current state:**
Options types use shallow dataclass inheritance chains. These are acceptable given their
narrow scope.

**Findings:**
- `src/extract/infrastructure/options.py`: `ParallelOptions(WorkerOptions, BatchOptions)`
  is a three-level multiple-inheritance chain for dataclasses. Python dataclass MRO
  handles field ordering, but the `batch_size` field is defined in both `BatchOptions`
  (default `None`) and `ParallelOptions` (default `512`), making the intent non-obvious
  without reading both classes.
- `src/extract/extractors/ast/setup.py:18`: `AstExtractOptions(RepoOptions, WorklistQueueOptions, ParallelOptions)`
  — four base classes. `BytecodeExtractOptions(RepoOptions, WorklistQueueOptions, WorkerOptions)`
  — only three. `SymtableExtractOptions(RepoOptions, WorklistQueueOptions, WorkerOptions)` — three.
  `TreeSitterExtractOptions(RepoOptions, WorklistQueueOptions, ParallelOptions)` — four.
  The inconsistency (some use `ParallelOptions`, some use `WorkerOptions`) means that
  `SymtableExtractOptions` has no `batch_size` or `parallel` fields, while `AstExtractOptions`
  has both. This creates invisible capability differences between extractors.

**Suggested improvement:**
Consolidate to a single `ExtractorBaseOptions` that includes all common fields
(`repo_id`, `use_worklist_queue`, `max_workers`, `batch_size`, `parallel`), with each
extractor inheriting only from this one base. Fields that don't apply to a given
extractor can be documented as "ignored." This removes the invisible capability difference.

**Effort:** small
**Risk if unaddressed:** low

---

#### P14. Law of Demeter — Alignment: 2/3

**Current state:**
Most accessor chains are short (two levels). A few deeper chains appear in the context
and session management code.

**Findings:**
- `src/extract/coordination/context.py:139-156`: `ensure_runtime_profile` accesses:
  `self.ensure_session().runtime_profile.diagnostics.capture_plan_artifacts` — four
  levels of traversal. The knowledge that `diagnostics.capture_plan_artifacts` exists
  and must be reset to `False` for extraction leaks into `ExtractExecutionContext`.
- `src/extract/coordination/materialization.py:304`: `runtime_profile.session_runtime().ctx`
  — `session_runtime()` is a method call that returns a `SessionRuntime`, then `.ctx`
  accesses its context. Every caller of this pattern knows that `SessionRuntime` has a
  `.ctx` attribute. This two-hop chain is repeated across many files in `coordination/`
  and `infrastructure/`.
- `src/extraction/orchestrator.py:631`: `execution_bundle.runtime_spec` (first hop)
  then later `execution_bundle.extract_session` (second access) — technically fine, but
  the orchestrator accesses both components of the bundle independently rather than
  treating the bundle as an opaque object.

**Suggested improvement:**
Add a `session_context` property to `ExtractSession` (it already has one at `session.py:27`)
— callers should use `session.df_ctx` instead of `session.session_runtime.ctx`.
For `ensure_runtime_profile`, extract the `capture_plan_artifacts=False` override logic
to a method on `DataFusionRuntimeProfile` or `ExtractSession` named
`profile_for_extraction()`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P15. Tell, don't ask — Alignment: 2/3

**Current state:**
The `EvidencePlan.requires_dataset()` pattern is good "tell, don't ask" design — callers
ask the plan what is needed rather than inspecting its internals. However, some ask
patterns remain.

**Findings:**
- `src/extract/coordination/materialization.py:498-509`: The function asks the result
  type and branches:
  ```python
  if resolved.prefer_reader:
      if isinstance(normalized, pa.Table):
          ...
          return pa.RecordBatchReader.from_batches(...)
      return normalized
  ```
  The decision of how to wrap the output should be encapsulated in a helper that
  `normalized` knows how to produce, rather than having `materialize_extract_plan`
  inspect the type.
- `src/extraction/orchestrator.py:940-976`: `_coerce_to_table` checks three attribute
  names (`to_arrow_table`, `read_all`) sequentially. This "ask for capability, branch on
  result" pattern is duck-typing without a protocol, and duplicates similar logic that
  presumably exists in `datafusion_engine`.

**Suggested improvement:**
Define a `ToArrowTable` protocol (or use `datafusion_engine.arrow.interop.TableLike`'s
existing coerce path) and replace the `isinstance` + `getattr` chain in
`_coerce_to_table` with a single coerce call. For the reader wrapping, introduce
`as_record_batch_reader(result: TableLike | RecordBatchReaderLike) -> RecordBatchReader`
that encapsulates the isinstance check.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Correctness (16-18)

#### P16. Functional core, imperative shell — Alignment: 2/3

**Current state:**
The extractor core logic (row construction, span computation, AST walking) is largely
pure. However, the cache management is mixed into the pure core rather than separated to
the shell.

**Findings:**
- `src/extract/extractors/ast/builders.py:857-914`: `_extract_ast_for_context` is a
  60-line function that interleaves cache lookup, cache locking, text parsing, AST
  walking, and cache storing. The pure core (`_parse_and_walk`) is correct, but it is
  buried inside the cache-management shell rather than the shell wrapping the core.
  A test of pure AST parsing correctness cannot invoke `_parse_and_walk` directly
  without also understanding the `_AstWalkResult` caching contract.
- `src/extract/coordination/worklists.py:150-172`: `_stream_with_queue` is a generator
  that both reads from a DataFusion stream (imperative IO) and writes to a diskcache
  queue (imperative IO). The two effects are interspersed within a single generator,
  making it impossible to test the queue-population logic independently from the
  stream-reading logic.

**Suggested improvement:**
In `_extract_ast_for_context`, extract the pure core as:
```python
def _run_ast_parse_walk(text: str, *, filename: str, options: AstExtractOptions,
                        max_nodes: int | None, line_offsets: LineOffsets | None
                        ) -> tuple[_AstWalkResult | None, list[dict[str, object]]]: ...
```
Then the cache-management shell calls this pure function and handles the cache
read/write/lock. This makes the pure core unit-testable without cache infrastructure.

**Effort:** medium
**Risk if unaddressed:** medium

---

#### P17. Idempotency — Alignment: 3/3

**Current state:**
Delta `overwrite` mode in `_write_delta` and the SHA-based worklist query (which skips
files where `file_sha256` matches the already-extracted value) together ensure that
re-running extraction with the same inputs produces the same outputs without corruption.

**Note:** Idempotency is well-satisfied for this scope. No action needed.

---

#### P18. Determinism / reproducibility — Alignment: 3/3

**Current state:**
`stable_cache_key` at `cache_utils.py:31` includes a `CACHE_VERSION` integer and a
`msgpack`-canonical hash, ensuring cache keys are deterministic. The `policy_hash` and
`ddl_fingerprint` fields on plan artifacts anchor plans to their schema version. The
`RuntimeProfileSpec.runtime_profile_hash` provides a stable identity for the runtime
configuration.

**Note:** Determinism is well-satisfied. No action needed.

---

### Category: Simplicity (19-22)

#### P19. KISS — Alignment: 2/3

**Current state:**
The core extraction abstractions are appropriately simple. Two specific
complications are unnecessary.

**Findings:**
- `src/extraction/orchestrator.py:193-198`:
  ```python
  @cache
  def _delta_write_ctx() -> SessionContext:
      from extraction.rust_session_bridge import build_extraction_session, extraction_session_payload
      return build_extraction_session(extraction_session_payload())
  ```
  A module-level cached singleton for the Delta write session is the simplest form
  possible — but it is hidden inside a private function with no documentation explaining
  why this session is separate from the extraction session. A new reader would not know
  this exists or when it is used.
- `src/extract/extractors/ast/builders.py:1130-1161`: Three `TypedDict` classes
  (`_AstTablesKwargs`, `_AstTablesKwargsTable`, `_AstTablesKwargsReader`) exist solely
  to support the `@overload` dispatch in `extract_ast_tables`. The overload resolves
  `prefer_reader: bool` to the return type. This is significant boilerplate for a
  type-narrowing benefit that most callers won't notice. The same three-TypedDict pattern
  presumably repeats in CST and other builders.
- `src/extraction/runtime_profile.py:95-114`: `_RuntimeProfileEnvPatchRuntime` is a
  Pydantic `BaseModel` that mirrors `RuntimeProfileEnvPatch` (a msgspec Struct), created
  solely to enable `validate_python` with strict mode. Two type definitions for the same
  data is unnecessary complexity.

**Suggested improvement:**
For the Pydantic/msgspec duplication: replace `_RuntimeProfileEnvPatchRuntime` with
a direct `msgspec.convert(payload, RuntimeProfileEnvPatch, strict=False)` call —
`msgspec` supports this and eliminates the Pydantic dependency entirely from this module.
For the three TypedDicts: reduce to a single `_AstTablesKwargs` and express the overload
difference as a simpler `prefer_reader: bool = False` default, dropping the `Unpack`
pattern if the type narrowing isn't essential for downstream callers.

**Effort:** small
**Risk if unaddressed:** low

---

#### P20. YAGNI — Alignment: 2/3

**Current state:**
The `EXTRACTION_SCHEMA_TEMPLATES` registry in `schema_derivation.py` and the fluent
`ExtractionSchemaBuilder` are both justified by the 7 extractor types they serve. The
`derive_extraction_schema` function (DataFusion plan analysis path) is more speculative.

**Findings:**
- `src/extract/schema_derivation.py:542-604`: `derive_extraction_schema` performs full
  DataFusion plan lineage extraction to derive a schema from a source table. This is
  powerful machinery, but there is no call-site evidence that this function is used in
  production paths — all extractors use `build_schema_from_template` or the fluent
  builder directly. If this function is unused, it is speculative generality.
- `src/extract/schema_derivation.py:687-758`: Six private `_*_template_hook` functions
  (one per extractor type) plus the `builder_hook` callable field on
  `ExtractionSchemaTemplate` introduce a plugin mechanism. The hooks are called only
  from `build_schema_from_template`. If the hook pattern was introduced to allow
  runtime extension (e.g., user-registered extractors), it is speculative. If all hooks
  are known at compile time, a simpler approach is to inline them.
- `src/extract/extractors/ast/builders.py:1163-1232`: `extract_ast_tables` is
  documented as returning a `Mapping` with the key `"ast_files"` — a single-element
  mapping. A `Mapping` return type for a function that always returns exactly one entry
  is speculative generality: callers always do `result["ast_files"]`.

**Suggested improvement:**
Audit `derive_extraction_schema` for call sites. If unused, remove it. For the single-
element `Mapping` return, consider returning a named tuple or the table directly, or at
minimum document that the key is always `"ast_files"`. For the builder hook, if no
runtime extension case exists, inline the hooks into `build_schema_from_template`'s
template dict.

**Effort:** small
**Risk if unaddressed:** low

---

#### P21. Least astonishment — Alignment: 2/3

**Current state:**
The `extract_*` vs `extract_*_tables` naming convention is consistent across all
extractors, which is good. However, the two functions have different return types in a
non-obvious way.

**Findings:**
- `src/extract/extractors/ast/builders.py:926`: `extract_ast` returns
  `ExtractResult[TableLike]`. `extract_ast_tables` at line 1175 returns
  `Mapping[str, TableLike | RecordBatchReaderLike]`. A caller seeing `extract_ast` for
  the first time would not know that the "tables" variant returns a mapping while the
  non-tables variant wraps in `ExtractResult`. The `ExtractResult` wrapper adds only an
  `extractor_name` field; the naming does not communicate this.
- `src/extract/extractors/symtable_extract.py:62`: `SymtableExtractOptions` inherits
  `WorkerOptions` (no `parallel` or `batch_size` fields). The AST, CST, and tree-sitter
  extractors use `ParallelOptions` (which adds `parallel: bool = True`). A caller passing
  `parallel=True` to `SymtableExtractOptions()` would get a `TypeError` — the behavior
  difference is invisible from the naming.
- `src/extraction/orchestrator.py:386-388`: The Python external stage stores its output
  under key `"python_external_interfaces"` in `state.delta_locations`, while the
  extractor produces output keyed as `"python_external_interfaces"` from
  `extract_python_external_tables`. The key rename is consistent but undocumented.

**Suggested improvement:**
Standardize the return type of `extract_*` and `extract_*_tables` — either both return
`Mapping[str, TableLike]` or both return a typed result wrapper. Drop `ExtractResult` if
it adds no meaningful information beyond the extractor name (which the caller already
knows). Document the `python_external_interfaces` key rename in `contracts.py`.

**Effort:** small
**Risk if unaddressed:** low

---

#### P22. Declare and version public contracts — Alignment: 2/3

**Current state:**
The `_v1` suffix naming convention (e.g., `ast_files_v1`, `libcst_files_v1`) is
consistently applied to dataset names, indicating awareness of contract versioning.
`SCHEMA_VERSION = 2` in `repo_scan.py` anchors the repo-scan schema version.

**Findings:**
- `src/extraction/contracts.py:30-44`: `_SEMANTIC_INPUT_CANDIDATES` is a private
  dict with no version number, no docstring, and no migration mechanism. It maps
  semantic input names to extraction output candidates and is the authoritative
  source for how extraction outputs map to semantic pipeline inputs. Any new extractor
  that adds a dataset must manually edit this dict, but there is no enforcement that
  this happens.
- `src/extract/__init__.py:112-192`: The `_EXPORTS` dict is the public surface of the
  `extract` package, but it is not annotated as such — it looks like implementation
  detail. There is no clear separation between "stable surface" (functions callers should
  use) and "migration surface" (functions that may change).
- `src/extraction/orchestrator.py` exports only `ExtractionResult` and `run_extraction`
  in its `__all__`, which is correct and minimal. But `ExtractionResult.delta_locations`
  contains string keys (e.g., `"ast_files"`, `"libcst_files"`) that are effectively a
  public contract — they are consumed by `contracts.py` — but are undocumented as such.

**Suggested improvement:**
Document `_SEMANTIC_INPUT_CANDIDATES` as a versioned mapping (add `# version: 1` comment
and a docstring explaining how to extend it). Add an enum or `Literal` type for the known
`delta_locations` keys in `ExtractionResult` so that consumers get type-checked access
rather than raw string keys.

**Effort:** small
**Risk if unaddressed:** low

---

### Category: Quality (23-24)

#### P23. Design for testability — Alignment: 1/3

**Current state:**
The pure domain logic (row construction, span computation) in the builders files is
in principle testable in isolation. However, the practical unit test story is impaired
by three structural issues.

**Findings:**
- `src/extraction/orchestrator.py:193-198`: `@cache` on `_delta_write_ctx` creates an
  unresetable module-level singleton. Any test of `_write_delta` will call the Rust
  `build_extraction_session` and `extraction_session_payload` unless the test patches
  `extraction.orchestrator._delta_write_ctx`. Since `functools.cache` stores results,
  the patch must happen before any import of the module.
- `src/extract/extractors/ast/builders.py:857-914`: `_extract_ast_for_context` mixes
  cache operations with the pure `_parse_and_walk`. A unit test for the parsing logic
  must either inject a `None` cache (which skips the cache path) or supply a real
  diskcache instance. There is no interface through which a test can supply a stub cache.
- `src/extraction/orchestrator.py:175-190`: `_build_extract_execution_bundle` has no
  injection seam. Tests of `run_extraction` either run the full Rust session stack or
  must monkey-patch the private function. Neither approach is lightweight.
- `src/extract/coordination/spec_helpers.py:43-59`: `@cache` decorators on
  `_feature_flag_rows` and `_metadata_defaults` cache results at module load time based
  on `extract_metadata_specs()` output. In tests that register different datasets,
  these caches will return stale values from previous tests unless explicitly cleared,
  causing test ordering dependency.

**Suggested improvement:**
For `_delta_write_ctx`: replace `@cache` with an injected parameter or a thread-local
context, so tests can supply a test-scope session without patching.
For cache mixing in builders: extract `_run_ast_parse_walk` as a pure function (no
cache access) and wrap it with `_cached_ast_parse_walk(cache, ...) -> ...` that handles
cache mechanics. Unit tests invoke the pure function; integration tests use the cached
wrapper.
For `_build_extract_execution_bundle`: accept an optional `bundle` argument in
`run_extraction` as suggested under P12.
For `@cache` on spec helpers: provide a `_clear_spec_cache()` function or use
`functools.lru_cache` with `maxsize=None` and expose `.cache_clear()` for test teardown.

**Effort:** medium
**Risk if unaddressed:** medium — the existing test suite presumably works around these
issues, but the barriers prevent writing new isolated unit tests for the core logic.

---

#### P24. Observability — Alignment: 2/3

**Current state:**
`EngineEventRecorder` in `extraction/diagnostics.py` centralizes structured event
recording for plan execution, Delta writes, extract quality, and diskcache stats. OTel
spans are emitted at `extract_ast_tables` and similar entry points via `stage_span`.

**Findings:**
- `src/extraction/orchestrator.py:302-324`: The parallel stage 1 extractor loop records
  `state.timing[name] = time.monotonic() - stage_start` using a shared `stage_start`
  timestamp. The timing for each extractor is measured from `stage_start` (the loop
  start), not from when that specific future began executing. In a parallel execution,
  this means extractor timings are wall-clock time from stage start, not per-extractor
  execution time. The timing data is structurally misleading.
- `src/extraction/orchestrator.py:311-312`: The span is created *after* `future.result()`
  blocks — so the span covers the `result()` wait time plus the span creation overhead,
  not the extractor's actual execution time. The span and the timing counter measure
  different things.
- `src/extract/coordination/materialization.py:326-401`: `_write_and_record_extract_output`
  records quality events for `register_view_failed`, `view_artifact_failed`, and
  `schema_contract_failed` separately with individual try/except blocks, but there is no
  structured event for the successful path. Observability is asymmetric (failures are
  recorded; successes are not).
- `src/extract/coordination/extraction_runtime_loop.py` has no observability at all.
  The parallel map that processes file contexts across extractors emits no spans, no row
  counts, and no timing information.

**Suggested improvement:**
For parallel timing: record `t0 = time.monotonic()` before `executor.submit(fn)` for
each extractor and record `time.monotonic() - t0` after `future.result()`. For the span,
wrap the `executor.submit(fn)` + `future.result()` pair in a single `stage_span`. For
success observability in `_write_and_record_extract_output`, record a structured success
event alongside the failure-only events. For `iter_runtime_rows`, add a span around the
parallel map call logging the row count and worker count.

**Effort:** small
**Risk if unaddressed:** low

---

## Cross-Cutting Themes

### Theme 1: The "builder pattern in two halves" anti-pattern

**Root cause:** Each extractor `builders.py` grew organically from a pure row-construction
module to include the full runtime entry-point wiring. The result is files with a clean
functional top half (AST walking, span extraction, row dict construction) and an imperative
bottom half (session wiring, cache management, plan building, OTel instrumentation).

**Affected principles:** P2 (SoC), P3 (SRP), P7 (DRY), P16 (functional core), P23
(testability).

**Suggested approach:** Apply the "anti-corruption layer" pattern: keep `builders.py`
as the pure domain layer, extract the runtime wiring to a new thin `api.py` file per
extractor. The `api.py` files will be nearly identical and can each delegate to a single
shared `run_extract_entry_point` helper in `coordination/`.

---

### Theme 2: Dual span representation

**Root cause:** `SpanSpec` was defined in `context.py` as a coordination primitive, then
`SpanTemplateSpec` emerged in `row_builder.py` as a more general form. The adapter glue
was added to bridge them rather than unifying the types.

**Affected principles:** P1 (information hiding), P7 (DRY), P10 (illegal states), P21
(least astonishment).

**Suggested approach:** Audit all `SpanSpec` usages (confined to `ast/builders.py`,
`bytecode/builders.py`, `cst/builders.py`, `symtable_extract.py`, and `file_index/line_index.py`)
and replace with `SpanTemplateSpec`. Remove `SpanSpec` from `context.py` and delete
the adapter. This is a mechanical refactor with no behavioral change.

---

### Theme 3: `inspect.signature` runtime compatibility detection

**Root cause:** During a migration adding `execution_bundle` to extractor function
signatures, a compatibility shim was added to `orchestrator.py` using `inspect.signature`
to detect the old vs new interface.

**Affected principles:** P6 (Ports & Adapters), P21 (least astonishment), P19 (KISS).

**Suggested approach:** Complete the migration. The three affected private functions
(`_run_repo_scan`, `_run_python_imports`, `_run_python_external`) all exist in the same
module. Updating them to always accept `execution_bundle` as a keyword-only parameter
with a default of `None` would eliminate all three `inspect.signature` calls without
changing any external interface.

---

### Theme 4: Testability impaired by module-level caching singletons

**Root cause:** `@cache` / `@functools.cache` is used for performance on hot paths
(`_delta_write_ctx`, `_feature_flag_rows`, `_metadata_defaults`), but the cached
objects are session-bound and stateful, making test isolation require monkey-patching.

**Affected principles:** P12 (DI), P16 (functional core), P23 (testability).

**Suggested approach:** Reserve `@cache` for truly pure, stateless computations (e.g.,
`_qname_keys` in `cst/setup.py` is fine — it reads a schema, which is stable). For
session-bound factories, use explicit injection rather than implicit singleton caching.

---

## Quick Wins (Top 5)

| Priority | Principle | Finding | Effort | Impact |
|----------|-----------|---------|--------|--------|
| 1 | P7 (DRY) | Delete `SpanSpec` from `context.py`, make `SpanTemplateSpec` canonical, remove adapter glue in `span_dict` | small | Eliminates dual-type confusion; removes 30 lines of adapter code |
| 2 | P6 (Ports) | Remove three `inspect.signature` checks in `orchestrator.py:230,342,378`; always pass `execution_bundle` | small | Removes runtime reflection; makes interface explicit |
| 3 | P19 (KISS) | Replace `_RuntimeProfileEnvPatchRuntime` Pydantic model with direct `msgspec.convert` call | small | Removes Pydantic from `runtime_profile.py`; aligns with AGENTS.md convention |
| 4 | P24 (Obs) | Fix parallel extractor timing in `orchestrator.py:302-324` to measure per-extractor wall time | small | Timing data becomes accurate |
| 5 | P23 (Test) | Extract pure `_run_ast_parse_walk` from `_extract_ast_for_context` so AST parsing is unit-testable | small | Enables isolated tests for the core parsing logic |

---

## Recommended Action Sequence

1. **Unify span types (P7, P1, P10):** Replace `SpanSpec` with `SpanTemplateSpec`
   everywhere in the extractors layer. Delete `SpanSpec` from `context.py`. Remove the
   adapter function `span_dict` (or make it a simple alias for `make_span_spec_dict`).
   This is a mechanical, zero-behavioral-change refactor.

2. **Complete the `execution_bundle` migration (P6, P19):** Remove the three
   `inspect.signature` branches in `orchestrator.py`. Update `_run_repo_scan`,
   `_run_python_imports`, `_run_python_external` to always accept
   `execution_bundle: _ExtractExecutionBundle | None = None`.

3. **Remove Pydantic from `runtime_profile.py` (P19, P3):** Replace
   `_RuntimeProfileEnvPatchRuntime` / `_RUNTIME_PROFILE_ENV_ADAPTER.validate_python`
   with `msgspec.convert(payload, RuntimeProfileEnvPatch, strict=False)`.

4. **Extract pure parse/walk core from builder cache wrappers (P16, P23):** For each
   extractor, separate the pure row-construction logic from the cache management shell.
   Start with `ast/builders.py` as the reference implementation.

5. **Introduce a shared `run_extract_entry_point` helper (P7, P2, P3):** Create a
   generic entry-point helper in `extract/coordination/entry_point.py` parameterized
   by extractor name, options type, and plan-builder callable. Migrate each
   `extract_*_tables` function to use it. This eliminates the ~1,000 line structural
   duplication across builders files.

6. **Move `EngineEventRecorder` import direction (P4):** Move
   `extraction/diagnostics.py:EngineEventRecorder` to
   `extract/coordination/diagnostics.py` so that `coordination/` does not depend on
   `extraction/`. Alternatively, inject the recorder via a protocol.

7. **Add injection seam to `_build_extract_execution_bundle` (P12, P23):** Add
   `bundle: _ExtractExecutionBundle | None = None` parameter to `run_extraction`. When
   `None`, construct the bundle as today. This makes the orchestrator testable without
   Rust session infrastructure.

8. **Fix parallel timing in orchestrator (P24):** Record `t0 = time.monotonic()` per
   extractor before submission and compute elapsed at `future.result()` completion.
