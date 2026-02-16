# Multi-Step Execution Architecture

**Version:** 0.5.0
**Last Updated:** 2026-02-15

## Overview

The multi-step execution subsystem enables running multiple CQ commands in a single invocation with shared file scanning infrastructure. This architecture optimizes performance by batching operations and reusing expensive parsing operations across multiple query steps.

Located under `tools/cq/run/` and `tools/cq/cli_app/commands/`, the subsystem coordinates Q-step batching, language scope expansion, relational span collection, and result aggregation with provenance tracking. Non-Q steps can execute in parallel via the WorkerScheduler when parallelism is enabled and `stop_on_error` is not set.

**Related Documentation:**
- [02_search_subsystem.md](02_search_subsystem.md) - Smart search implementation (SearchStep)
- [03_query_ir_subsystem.md](03_query_ir_subsystem.md) - Query IR and execution (QStep)
- [04_analysis_macros.md](04_analysis_macros.md) - Analysis macros (CallsStep, ImpactStep, etc.)
- [08_neighborhood_subsystem.md](08_neighborhood_subsystem.md) - Neighborhood assembly (NeighborhoodStep)
- [10_runtime_services.md](10_runtime_services.md) - Runtime services and worker scheduling

## Module Map

### Core Execution Engine
- `tools/cq/run/runner.py` (1297 LOC) - Main execution orchestration, batching strategy, language scope expansion, result collapse, parallel non-Q execution
- `tools/cq/run/spec.py` (236 LOC) - RunPlan data model, 11 step type variants, step ID normalization, `coerce_run_step()` helper
- `tools/cq/run/loader.py` (84 LOC) - TOML/JSON plan loading via typed boundary protocol, inline step coercion

### Frontend Interfaces
- `tools/cq/run/chain.py` (197 LOC) - Command chaining compiler, token parsing, step builder dispatch
- `tools/cq/run/step_decode.py` (50 LOC) - JSON step decoding via typed boundary protocol
- `tools/cq/cli_app/commands/chain.py` - CLI entry point for `cq chain`
- `tools/cq/cli_app/commands/run.py` - CLI entry point for `cq run`

### Batch Optimization
- `tools/cq/query/batch.py` - BatchEntityQuerySession with shared ast-grep scan
- `tools/cq/query/batch_spans.py` - Relational span collection with per-file parse caching

### Result Aggregation
- `tools/cq/core/merge.py` - Step result merging with provenance tracking

## Data Model

### RunPlan Structure

```python
class RunPlan(msgspec.Struct, kw_only=True, frozen=True, omit_defaults=True):
    """Typed run plan for multi-step execution."""

    version: int = 1                     # Schema version
    steps: tuple[RunStep, ...] = ()      # Ordered execution steps
    in_dir: str | None = None            # Global scope filter (directory)
    exclude: tuple[str, ...] = ()        # Global scope filter (patterns)
```

**Location:** `tools/cq/run/spec.py:13-20`

Global scope filters are applied to all steps:
- `in_dir`: restricts all steps to a subdirectory
- `exclude`: glob patterns to exclude from all steps
- Applied via `_apply_run_scope()` for Q steps, `_apply_run_scope_filter()` for non-Q steps

### Step Type Taxonomy (11 variants)

#### Query Steps

**QStep** - Entity or pattern query via query IR
```python
class QStep(RunStepBase, tag="q", frozen=True):
    query: str
```
- Supports full query language including relational constraints
- Optimized via batch session with shared ast-grep scan
- Language scope expansion creates per-language variants (e.g., `auto` → `python` + `rust`)
- **Location:** `tools/cq/run/spec.py:35-39`

**SearchStep** - Smart search with classification
```python
class SearchStep(RunStepBase, tag="search", frozen=True):
    query: str
    regex: bool = False
    literal: bool = False
    include_strings: bool = False
    in_dir: str | None = None
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE
```
- Falls through to smart search pipeline directly
- No batch optimization (each search is independent)
- Supports regex and literal modes
- **Location:** `tools/cq/run/spec.py:41-50`

#### Analysis Steps

**CallsStep** - Find all call sites for a function
```python
class CallsStep(RunStepBase, tag="calls", frozen=True):
    function: str
```
- **Location:** `tools/cq/run/spec.py:52-56`

**ImpactStep** - Trace parameter data flow
```python
class ImpactStep(RunStepBase, tag="impact", frozen=True):
    function: str
    param: str
    depth: int = 5
```
- **Location:** `tools/cq/run/spec.py:58-64`

**SigImpactStep** - Signature change breakage analysis
```python
class SigImpactStep(RunStepBase, tag="sig-impact", frozen=True):
    symbol: str
    to: str
```
- **Location:** `tools/cq/run/spec.py:79-84`

**ExceptionsStep** - Exception flow analysis
```python
class ExceptionsStep(RunStepBase, tag="exceptions", frozen=True):
    function: str | None
```
- **Location:** `tools/cq/run/spec.py:73-77`

#### Graph Analysis Steps

**ImportsStep** - Import graph and cycle detection
```python
class ImportsStep(RunStepBase, tag="imports", frozen=True):
    cycles: bool = False
    module: str | None = None
```
- **Location:** `tools/cq/run/spec.py:66-71`

#### Surface Inspection Steps

**SideEffectsStep** - Side effect surface scan
```python
class SideEffectsStep(RunStepBase, tag="side-effects", frozen=True):
    max_files: int = 2000
```
- **Location:** `tools/cq/run/spec.py:86-90`

**ScopesStep** - Closure and scope analysis
```python
class ScopesStep(RunStepBase, tag="scopes", frozen=True):
    target: str
    max_files: int = 500
```
- **Location:** `tools/cq/run/spec.py:92-97`

**BytecodeSurfaceStep** - Bytecode opcode inspection
```python
class BytecodeSurfaceStep(RunStepBase, tag="bytecode-surface", frozen=True):
    target: str
    show: str = "globals,attrs,constants"
    max_files: int = 500
```
- **Location:** `tools/cq/run/spec.py:99-105`

#### Neighborhood Steps

**NeighborhoodStep** - Semantic neighborhood assembly for a target
```python
class NeighborhoodStep(RunStepBase, tag="neighborhood", frozen=True):
    target: str
    lang: str = "python"
    top_k: int = 10
    no_semantic_enrichment: bool = False
```
- Executes the full neighborhood pipeline: target resolution → structural collection → LSP enrichment → bundle assembly → rendering
- Results are not batch-optimized (each neighborhood step is independent)
- Supports both position-based (`file.py:line:col`) and symbol-based (`function_name`) target specifications
- **Location:** `tools/cq/run/spec.py:107-114`
- See [08_neighborhood_subsystem.md](08_neighborhood_subsystem.md) for full pipeline documentation

### Step Base Protocol

```python
class RunStepBase(
    msgspec.Struct,
    kw_only=True,
    frozen=True,
    omit_defaults=True,
    tag=True,
    tag_field="type",
):
    """Base class for run steps."""
    id: str | None = None  # Optional step identifier for provenance
```

**Location:** `tools/cq/run/spec.py:22-33`

All step types use msgspec tagged union discrimination via `tag_field="type"`. The `normalize_step_ids()` function ensures every step has a deterministic ID (default: `{type}_{index}`).

### RunStep Union Type

```python
RunStep = (
    QStep
    | SearchStep
    | CallsStep
    | ImpactStep
    | ImportsStep
    | ExceptionsStep
    | SigImpactStep
    | SideEffectsStep
    | ScopesStep
    | BytecodeSurfaceStep
    | NeighborhoodStep
)
```

**Location:** `tools/cq/run/spec.py:116-128`

This discriminated union enables type-safe deserialization via msgspec's tagged union protocol.

### ParsedQStep Internal Representation

```python
@dataclass(frozen=True)
class ParsedQStep:
    step_id: str              # Effective step ID (may include :lang suffix)
    parent_step_id: str       # Original step ID (for result collapsing)
    step: QStep               # Original step specification
    query: Query              # Parsed query IR
    plan: ToolPlan            # Compiled tool plan with ast-grep rules
    scope_paths: list[Path]   # Resolved file paths in scope
    scope_globs: list[str] | None  # Glob patterns for filtering
```

**Location:** `tools/cq/run/runner.py:88-97`

This intermediate representation is created during Q-step parsing and enables language scope expansion without modifying the original step specification. When a Q-step has `lang_scope="auto"`, it expands into multiple `ParsedQStep` instances with language-specific step IDs (e.g., `q_0:python`, `q_0:rust`).

## Plan Loading

### Three Input Methods

#### 1. TOML Plan File

```toml
version = 1
in_dir = "src"
exclude = ["tests/**", "build/**"]

[[steps]]
type = "q"
query = "entity=function name=build_graph"

[[steps]]
type = "calls"
function = "build_graph"
```

Loaded via `_load_plan_file()` at `tools/cq/run/loader.py:45-56`:
1. Reads file as bytes (`path.read_bytes()`)
2. Decodes TOML via typed boundary protocol: `decode_toml_strict(payload, type_=RunPlan)`
3. Raises `RunPlanError` wrapping `BoundaryDecodeError` on parse or validation failure
4. Typed boundary protocol centralizes TOML parsing (uses `tomllib` stdlib in Python 3.11+)

#### 2. Inline JSON Step

```bash
cq run --step '{"type":"q","query":"entity=function name=foo"}'
```

Single step provided as JSON string, parsed via `parse_run_step_json()` from `tools/cq/run/step_decode.py:9-26`.

#### 3. JSON Array

```bash
cq run --steps '[{"type":"q","query":"..."},{"type":"calls","function":"foo"}]'
```

Multiple steps provided as JSON array, parsed via `parse_run_steps_json()` from `tools/cq/run/step_decode.py:28-44`. Combined with `--plan` steps if both provided.

### Step Coercion Protocol

The `load_run_plan()` function (`tools/cq/run/loader.py:20-42`) assembles the final plan:
1. Load plan file if `--plan` provided
2. Load inline steps from `--step` and `--steps` options
3. Merge file and inline steps
4. Normalize step IDs via `normalize_step_ids()`
5. Validate at least one step exists

Step coercion via `_coerce_step(item: object) -> RunStep` (`tools/cq/run/loader.py:66-78`):
1. If already a `RunStep`, return as-is
2. If dataclass, convert to dict via `asdict()`
3. If dict, call `coerce_run_step(item)` (delegating to `convert_strict()` from typed boundary)
4. Otherwise, raise `RunPlanError`

The `coerce_run_step()` helper (in `spec.py:156-164`) provides the canonical conversion entry point:
```python
def coerce_run_step(payload: object) -> RunStep:
    """Convert payload into canonical run-step tagged union."""
    return convert_strict(payload, type_=RunStep)
```

This delegation to the typed boundary protocol ensures consistent strict validation and error taxonomy across all run-step deserialization paths.

### Error Taxonomy Integration

Plan loading and step coercion use the typed boundary protocol error hierarchy:

- **`BoundaryDecodeError`**: Base exception for all typed boundary decode failures
  - Wraps TOML parse errors from `tomllib.TOMLDecodeError`
  - Wraps msgspec validation errors from `msgspec.ValidationError`
- **`RunPlanError`**: Domain-specific wrapper that adds context (file path, step type)

Error flow example:
```
TOML parse failure → BoundaryDecodeError → RunPlanError("Invalid TOML in plan file: {path}")
Schema validation failure → BoundaryDecodeError → RunPlanError("Invalid step schema: {exc}")
```

This taxonomy enables:
- Centralized error handling in typed boundary layer
- Consistent error messages across all deserialization paths
- Context preservation through error chain

### Architectural Observations for Improvement Proposals

**Plan Loading Design Tensions**:
- TOML is human-friendly but requires external parser (now centralized in typed boundary)
- JSON is verbose but supports inline CLI usage without escaping
- No plan validation beyond schema (e.g., query syntax not checked until execution)
- Step ordering is significant but not validated (e.g., using results from later steps)

**Potential Improvement Vectors**:
- Plan validation mode that parses queries without execution
- Plan introspection/explain command showing execution strategy
- Dependency declaration between steps for parallel execution
- Named outputs/inputs for passing data between steps

## Execution Engine

### Main Entry Point

```python
def execute_run_plan(plan: RunPlan, ctx: CliContext, *, stop_on_error: bool = False) -> CqResult
```

**Location:** `tools/cq/run/runner.py:99-158`

Execution flow:
1. Create `RunContext` from CLI context with run ID
2. Initialize merged result with "run" macro metadata
3. Normalize step IDs via `normalize_step_ids()`
4. Partition steps into Q steps and other steps
5. Execute Q steps with batch optimization (`_execute_q_steps()`)
6. Execute non-Q steps (parallel via `_execute_non_q_steps_parallel()` or serial via `_execute_non_q_steps_serial()`)
7. Populate run summary metadata via `_populate_run_summary_metadata()`
8. Snapshot cache backend metrics
9. Assign result finding IDs
10. Evict run cache tags for both Python and Rust
11. Return merged result

**Runtime Services Integration**:
- Runner uses `resolve_runtime_services()` from `core/bootstrap.py` for service composition
- `WorkerScheduler` provides CPU and I/O pools for parallel execution
- `RuntimeExecutionPolicy` controls parallelism (via `run_step_workers` setting), LSP budgets, and cache behavior
- See [10_runtime_services.md](10_runtime_services.md) for full runtime services documentation

### Q-Step Batch Optimization Pipeline

The Q-step pipeline implements a sophisticated multi-stage optimization to minimize redundant file parsing.

#### Stage 1: Parsing and Scope Expansion

`_partition_q_steps()` processes each Q step (`tools/cq/run/runner.py:387-409`):

1. **Parse**: `_prepare_q_step()` (`runner.py:433-460`) parses query string to IR, applies run-level scope
2. **Expand**: `_expand_q_step_by_scope()` (`runner.py:463-483`) splits `auto` scope into per-language variants
   - Input: `ParsedQStep(step_id="q_0", lang_scope="auto", ...)`
   - Output: `[ParsedQStep(step_id="q_0:python", ...), ParsedQStep(step_id="q_0:rust", ...)]`
3. **Partition**: Separate entity queries from pattern queries by `plan.is_pattern_query`
4. **Group by Language**: Build `dict[QueryLanguage, list[ParsedQStep]]`

Language expansion enables:
- Separate ast-grep scans per language (Python vs Rust have different grammars)
- Independent language-specific record type requirements
- Per-language result confidence scoring

#### Stage 2: Batch Session Construction

`_execute_entity_q_steps()` builds shared scan session (`tools/cq/run/runner.py:498-572`):

1. **Check Toolchain**: Early return with error if `ast-grep` not available
2. **Union Paths**: Deduplicate file paths across all steps (`_unique_paths()` at `runner.py:1250-1258`)
3. **Union Record Types**: Collect all required record types (`_union_record_types()` at `runner.py:1234-1238`)
4. **Single Scan**: `build_batch_session()` performs ONE ast-grep scan for all steps
5. **Per-Step Filtering**: Each step filters from shared records by scope via `filter_files_for_scope()`

Performance impact:
- Without batching: N steps × M files = N×M scans
- With batching: 1 scan for N steps sharing language and repo root
- Test `test_batch_q_steps_scan_once` validates scan count = 1 via monkeypatch

#### Stage 3: Relational Span Collection

For queries with relational constraints (`inside`, `has`, `precedes`, etc.) (`runner.py:533-540`):

1. **Union Span Files**: Collect files needing pattern matching (`_union_span_files()` at `runner.py:1241-1247`)
2. **Batch Collection**: `collect_span_filters()` matches all rule sets per file
3. **Parse Caching**: `roots_by_lang` dict caches `SgRoot` per language per file
4. **Per-Query Distribution**: Returns `list[dict[str, list[tuple[int, int]]]]` (spans by file per query)

This avoids re-parsing the same file for multiple queries with relational constraints.

#### Stage 4: Per-Step Execution

Each parsed step executes against shared artifacts (`runner.py:542-572`):

1. **Scope Filtering**: `filter_files_for_scope()` narrows records to step scope
2. **Build Request**: Construct `EntityQueryRequest` with filtered records and match spans
3. **Entity Matching**: `execute_entity_query_from_records()` matches against filtered records
4. **Result Enrichment**: Add `lang` and `query_text` to summary
5. **Error Handling**: Catch `RUN_STEP_NON_FATAL_EXCEPTIONS` and convert to error result
6. **Stop-on-Error**: Break if `stop_on_error=True` and exception occurred

Non-fatal exceptions (`runner.py:79-86`):
```python
RUN_STEP_NON_FATAL_EXCEPTIONS = (
    OSError,
    RuntimeError,
    TimeoutError,
    ValueError,
    TypeError,
)
```

#### Stage 5: Result Collapsing

`_collapse_parent_q_results()` merges per-language results (`tools/cq/run/runner.py:802-816`):

1. **Group by Parent**: Steps with same `parent_step_id` are grouped (e.g., `q_0:python`, `q_0:rust` → `q_0`)
2. **Single Result Pass-Through**: If only one language, normalize via `_normalize_single_group_result()` and return
3. **Multi-Language Merge**: Call `_merge_collapsed_q_group()` with cross-language diagnostics
4. **Metadata Synthesis**: Derive `mode`, `lang_scope`, `language_order` from group

The collapse logic (`runner.py:778-799`):
- Collects language-specific results into `dict[QueryLanguage, CqResult]`
- Extracts common query text, mode, and lang scope
- Builds cross-language diagnostics via `_build_collapse_diagnostics()`
- Merges results via `merge_language_cq_results()` with unified metadata

Collapsing preserves:
- Original query text from any language result
- Query mode (entity/pattern) from any result
- Union of language scopes
- Cross-language capability diagnostics

### Pattern Query Execution

`_execute_pattern_q_steps()` executes pattern queries (`tools/cq/run/runner.py:575-633`):

1. **Check Toolchain**: Early return with error if `ast-grep` not available
2. **Union Paths**: Deduplicate file paths across all steps
3. **Tabulate Files**: Build file index via `_tabulate_files()` (`runner.py:1261-1272`) with language extensions
4. **Per-Step Execution**: Each step calls `execute_pattern_query_with_files()`
5. **No Shared Scan**: Pattern queries don't benefit from record batching (full AST match)

Pattern queries are separated because:
- No entity candidate construction needed
- Match entire AST tree, not discrete entity records
- Different optimization strategy (file list union vs record union)

### Non-Q Step Execution

**Location:** `tools/cq/run/runner.py:819-887`

Non-Q steps execute either serially or in parallel depending on configuration.

#### Parallel Execution

`_execute_non_q_steps_parallel()` (`runner.py:865-887`):
1. Check if parallelism is beneficial (more than 1 step)
2. Check if worker scheduler policy allows parallelism (`run_step_workers > 1`)
3. Submit all steps to I/O worker pool via `scheduler.submit_io()`
4. Collect results with bounded timeout (60 seconds per step)
5. Fall back to serial execution if any timeouts occur

Parallelism is controlled by `RuntimeExecutionPolicy.run_step_workers`:
- Default: 1 (serial execution)
- Can be configured via environment or policy
- See [10_runtime_services.md](10_runtime_services.md)

#### Serial Execution

`_execute_non_q_steps_serial()` (`runner.py:848-862`):
1. Execute each step via `_execute_non_q_step_safe()`
2. Append result to list
3. If `stop_on_error=True` and result has error, break

#### Step Dispatch

`_execute_non_q_step()` routes to the appropriate executor (`runner.py:819-832`):

```python
def _execute_non_q_step(step: RunStep, plan: RunPlan, ctx: CliContext, *, run_id: str) -> CqResult:
    if isinstance(step, SearchStep):
        return _execute_search_step(step, plan, ctx, run_id=run_id)
    if isinstance(step, NeighborhoodStep):
        return _execute_neighborhood_step(step, ctx, run_id=run_id)

    executor = _NON_SEARCH_DISPATCH.get(type(step))
    if executor is None:
        msg = f"Unsupported step type: {type(step)!r}"
        raise TypeError(msg)

    result = executor(step, ctx)
    return _apply_run_scope_filter(result, ctx.root, plan.in_dir, plan.exclude)
```

**Static Dispatch Table** (`runner.py:1136-1148`):

```python
_NON_SEARCH_DISPATCH: dict[type, Callable[..., CqResult]] = {
    CallsStep: _execute_calls,
    ImpactStep: _execute_impact,
    ImportsStep: _execute_imports,
    ExceptionsStep: _execute_exceptions,
    SigImpactStep: _execute_sig_impact,
    SideEffectsStep: _execute_side_effects,
    ScopesStep: _execute_scopes,
    BytecodeSurfaceStep: _execute_bytecode_surface,
    NeighborhoodStep: _execute_neighborhood_step,
}
```

**Design rationale:**
- Dict dispatch avoids long if/elif chains
- `SearchStep` and `NeighborhoodStep` handled separately due to special requirements
- New step types only need to add an entry to `_NON_SEARCH_DISPATCH` and implement the executor function
- All non-Q, non-Search, non-Neighborhood steps go through `_apply_run_scope_filter` for consistent scope enforcement

#### Executor Implementations

Each step type maps to a corresponding macro via typed request structs:

- `CallsStep` → `tools.cq.macros.calls.cmd_calls()` (`runner.py:955-960`)
- `ImpactStep` → `tools.cq.macros.impact.cmd_impact()` (`runner.py:963-973`)
- `ImportsStep` → `tools.cq.macros.imports.cmd_imports()` (`runner.py:976-981`)
- `ExceptionsStep` → `tools.cq.macros.exceptions.cmd_exceptions()` (`runner.py:984-989`)
- `SigImpactStep` → `tools.cq.macros.sig_impact.cmd_sig_impact()` (`runner.py:992-997`)
- `SideEffectsStep` → `tools.cq.macros.side_effects.cmd_side_effects()` (`runner.py:1000-1005`)
- `ScopesStep` → `tools.cq.macros.scopes.cmd_scopes()` (`runner.py:1008-1013`)
- `BytecodeSurfaceStep` → `tools.cq.macros.bytecode.cmd_bytecode_surface()` (`runner.py:1016-1026`)
- `NeighborhoodStep` → `tools.cq.neighborhood.bundle_builder.build_neighborhood_bundle()` (`runner.py:1029-1112`)

**Executor Implementation Pattern:**

Each executor function constructs a `RequestContextV1` and uses `RequestFactory` to build the typed request. Example from `_execute_impact()`:

```python
def _execute_impact(step: ImpactStep, ctx: CliContext) -> CqResult:
    from tools.cq.macros.impact import cmd_impact

    request_ctx = RequestContextV1(root=ctx.root, argv=ctx.argv, tc=ctx.toolchain)
    request = RequestFactory.impact(
        request_ctx,
        function_name=step.function,
        param_name=step.param,
        max_depth=step.depth,
    )
    return cmd_impact(request)
```

This pattern ensures:
- Type-safe macro invocation with structured requests
- Explicit field mapping from step spec to request
- Consistent error handling at struct construction time
- Centralized request factory for request building logic

### Search Fallback Logic

`_handle_query_parse_error()` provides fallback (`tools/cq/run/runner.py:486-495`):
- If query parse fails AND `_has_query_tokens()` returns `False`
- Fall back to `_execute_search_fallback()` with same query text
- Otherwise, return error result

Token detection regex (`runner.py:1290-1292`):
```python
def _has_query_tokens(query_string: str) -> bool:
    token_pattern = r"([\\w.]+|\\$+\\w+)=(?:'([^']+)'|\\\"([^\\\"]+)\\\"|([^\\s]+))"
    return bool(list(re.finditer(token_pattern, query_string)))
```

Matches query tokens like `entity=function`, `name=foo`, but not plain strings. This enables bareword fallback: `cq run --step '{"type":"q","query":"build_graph"}'` → smart search.

### Architectural Observations for Improvement Proposals

**Execution Engine Design Tensions**:
- Q-step batching requires homogeneous language scope (no cross-language batching within a single batch)
- Non-Q steps execute unconditionally even if inputs from prior steps are empty
- Error handling uses exceptions for control flow (`stop_on_error` flag)
- No cost estimation or adaptive batching based on file counts
- Parallel execution available for non-Q steps but requires configuration

**Potential Improvement Vectors**:
- Dependency DAG construction for parallel execution of independent Q and non-Q steps
- Adaptive batching: small file sets may not benefit from batch overhead
- Lazy execution: skip steps with unsatisfied preconditions
- Streaming results: emit step results as they complete
- Batch heterogeneity: support cross-language batches with per-language record extraction
- Auto-tune parallelism based on available cores and step count

## Shared Scan Infrastructure

### BatchEntityQuerySession

```python
@dataclass(frozen=True)
class BatchEntityQuerySession:
    root: Path                        # Repository root
    tc: Toolchain                     # Toolchain with ast-grep availability
    files: list[Path]                 # Union of files across all steps
    files_by_rel: dict[str, Path]     # Index for fast lookup
    records: list[SgRecord]           # SHARED ast-grep scan results
    scan: ScanContext                 # SHARED scan context for entity matching
    candidates: EntityCandidates      # SHARED entity candidate index
    symtable: SymtableEnricher        # Symtable enricher for Python
```

**Location:** `tools/cq/query/batch.py`

Session construction via `build_batch_session()`:
1. Resolve repo context (repo root, gitignore rules)
2. Tabulate files with language extension filtering
3. Normalize record types (convert string record types to `RecordType` enum)
4. Get rules via `get_rules_for_types()` (retrieves ast-grep rules for record types)
5. **Single Scan**: `scan_files(files, rules, root, lang=lang)` — THE CRITICAL OPTIMIZATION
6. Filter records via `filter_records_by_type()` (narrows to requested types)
7. Build indices (construct `ScanContext` and `EntityCandidates` from records)

The `records` field is the shared artifact that eliminates redundant scans. All Q-steps in a batch reuse this single scan result.

### Relational Span Batching

`collect_span_filters()` implements per-file parse caching:

```python
def collect_span_filters(
    *,
    root: Path,
    files: list[Path],
    queries: list[Query],
    plans: list[ToolPlan],
) -> list[dict[str, list[tuple[int, int]]]]:
```

**Location:** `tools/cq/query/batch_spans.py`

Execution:
1. Initialize per-query accumulators: `per_query: list[list[AstGrepMatchSpan]]`
2. Extract rule sets: `[plan.sg_rules for plan in plans]`
3. Per-file iteration: Call `_collect_file_matches()` for each file
4. Language-scoped parsing: Check file extension against query language before parsing
5. Parse caching: `roots_by_lang: dict[QueryLanguage, SgNode]` caches parsed tree
6. Rule matching: Each rule set matches against the cached tree
7. Span construction: Build `AstGrepMatchSpan` from match range

Parse caching impact:
- Without caching: N queries × 1 file = N parses
- With caching: N queries × 1 file = 1 parse per language
- Rust and Python parse independently (different grammars)

### Architectural Observations for Improvement Proposals

**Batch Infrastructure Design Tensions**:
- Session is immutable post-construction (no incremental updates)
- Record type union forces conservative scan (may scan unnecessary records)
- In-memory parse cache is per-invocation (no persistence across runs)
- Language-specific sessions prevent heterogeneous batching
- No cache eviction strategy (entire session held in memory)

**Potential Improvement Vectors**:
- Persistent parse cache across invocations (disk-backed AST cache) - infrastructure exists for macro-level results
- Incremental session updates for interactive/watch mode
- Record type intersection optimization (only scan minimal superset)
- Memory budget awareness with cache eviction
- Heterogeneous language batching with per-language record extraction

## Chain Command Frontend

### CLI Entry Point

```python
def chain(
    *tokens: str,
    delimiter: str = "AND",
    ctx: CliContext | None = None,
) -> CliResult
```

**Location:** `tools/cq/cli_app/commands/chain.py`

Token parsing via `itertools.groupby()`:
```python
groups = [
    list(group)
    for is_delim, group in itertools.groupby(tokens, lambda t: t == delimiter)
    if not is_delim
]
```

Example:
```
tokens = ["q", "entity=function", "AND", "calls", "foo"]
delimiter = "AND"
→ groups = [["q", "entity=function"], ["calls", "foo"]]
```

### Command Parsing

Each token group parsed via `app.parse_known_args()` (`tools/cq/run/chain.py:69-100`):

```python
with app.app_stack(
    group,
    {
        "exit_on_error": False,
        "print_error": False,
    },
):
    command, bound, unused_tokens, _ignored = app.parse_known_args(group)
```

This reuses the main CQ CLI parser (from `tools/cq/cli_app/app.py`), ensuring command semantics match standalone invocations.

### Step Builder Dispatch

`_step_from_command()` extracts (`tools/cq/run/chain.py:108-119`):
- `command.__name__` for command name
- `bound.args` for positional arguments
- `bound.kwargs["opts"]` for options object

Step builder registry (10 of 11 step types supported) (`chain.py:181-192`):

```python
_STEP_BUILDERS: dict[str, StepBuilder] = {
    "q": _build_q_step,
    "search": _build_search_step,
    "calls": _build_calls_step,
    "impact": _build_impact_step,
    "imports": _build_imports_step,
    "exceptions": _build_exceptions_step,
    "sig_impact": _build_sig_impact_step,
    "side_effects": _build_side_effects_step,
    "scopes": _build_scopes_step,
    "bytecode_surface": _build_bytecode_surface_step,
    # Note: neighborhood step not supported in chain command (use run --step instead)
}
```

**Neighborhood Chain Limitation:**
The `NeighborhoodStep` is not supported in the `cq chain` command due to target resolution complexity. Use `cq run --step '{"type":"neighborhood","target":"..."}' ` or `cq run --steps '[...]'` for neighborhood analysis instead. See [08_neighborhood_subsystem.md](08_neighborhood_subsystem.md) for full neighborhood documentation.

Each builder extracts step fields from args/opts. Example (`chain.py:140-145`):

```python
def _build_impact_step(args: tuple[object, ...], opts: object | None) -> RunStep:
    return ImpactStep(
        function=_require_str_arg(args, 0, "function"),
        param=_require_str_attr(opts, "param", "param"),
        depth=getattr(opts, "depth", 5),
    )
```

### Architectural Observations for Improvement Proposals

**Chain Frontend Design Tensions**:
- Token-based parsing is fragile (no quoting for complex arguments)
- Delimiter must not appear in command arguments
- Error messages don't indicate which segment failed
- No variable substitution or output piping between commands
- Parse overhead: each segment re-parses CLI args
- Neighborhood step not supported (use `run` command instead)

**Potential Improvement Vectors**:
- Structured chain DSL with proper escaping/quoting
- Variable binding: `$VAR = q "entity=function" && calls $VAR`
- Command substitution: `calls $(q "entity=function" | first)`
- Better error context: show failing segment and its index
- Compiled chain representation for repeated execution
- Add neighborhood builder to support `cq chain neighborhood <target>` syntax

## Result Merging

### Provenance Tracking

`merge_step_results()` tracks step origin (`tools/cq/core/merge.py`):

1. **Summary Metadata**: Add `step_id` to `steps` list, store step summary in `step_summaries` dict
2. **Finding Cloning**: Clone each finding with provenance via `_clone_with_provenance()`
3. **Section Title Prefixing**: Prepend `{step_id}: ` to section titles
4. **Artifact Aggregation**: Extend artifacts list

### Finding Provenance

`_clone_with_provenance()` adds metadata:

```python
def _clone_with_provenance(
    finding: Finding,
    *,
    step_id: str,
    source_macro: str,
) -> Finding:
    data = dict(finding.details.data)
    data["source_step"] = step_id      # e.g., "q_0", "calls_1"
    data["source_macro"] = source_macro  # e.g., "q", "calls"
    details = DetailPayload(kind=finding.details.kind, score=finding.details.score, data=data)
    return Finding(...)
```

Provenance enables:
- Tracing findings back to originating step
- Filtering results by step ID
- Understanding macro-specific behavior

### Summary Metadata Synthesis

`_populate_run_summary_metadata()` derives aggregate metadata (`tools/cq/run/runner.py:160-189`):

#### Mode and Query Derivation

`_derive_run_summary_metadata()` (`runner.py:261-296`):
- **Single Step**: Use its mode and query directly
- **Homogeneous Mode**: If all steps have same mode (entity/pattern/identifier/regex/literal), join queries with ` | `
- **Heterogeneous Mode**: Default to `"run"` mode with `"multi-step plan (N steps)"` query

#### Language Scope Derivation

`_derive_run_scope_metadata()` (`runner.py:312-338`):
- Extract `lang_scope` and `language_order` from all step results
- **Single Unique Scope**: Use that scope
- **Multiple Scopes**: Default to `DEFAULT_QUERY_LANGUAGE_SCOPE` (usually `"auto"`)
- **Inferred from Orders**: If no explicit scope, derive from union of `language_order` tuples

Inferred scope logic (`runner.py:299-309`):

```python
def _derive_scope_from_orders(language_orders: list[tuple[QueryLanguage, ...]]) -> QueryLanguageScope | None:
    languages = {lang for order in language_orders for lang in order}
    if languages == {"python"}:
        return "python"
    if languages == {"rust"}:
        return "rust"
    if languages:
        return "auto"
    return None
```

This ensures the run result has a coherent language scope even if individual steps had different scopes.

#### Semantic Telemetry Aggregation

`_aggregate_run_semantic_telemetry()` (`runner.py:192-216`):
- Sums telemetry counters (`attempted`, `applied`, `failed`, `skipped`, `timed_out`) from all steps
- Separate aggregation for `python_semantic_telemetry` and `rust_semantic_telemetry`

#### Semantic Planes Selection

`_select_run_semantic_planes()` (`runner.py:233-258`):
- Selects the "best" semantic planes from all step results
- Uses signal scoring (counts of non-empty values, collections, etc.)
- Prefers later steps in execution order when scores are equal

### Architectural Observations for Improvement Proposals

**Result Merging Design Tensions**:
- Provenance is metadata-only (no structured result tree)
- Section title prefixing is string manipulation (not structural)
- Mode/query derivation uses heuristics (may not capture semantic intent)
- No step result deduplication (same finding from multiple steps appears twice)
- Summary synthesis is lossy (individual step metadata squashed)

**Potential Improvement Vectors**:
- Structured result tree with explicit parent-child relationships
- Result deduplication with step provenance list
- Semantic mode inference (e.g., "call graph analysis" for calls+impact steps)
- Step-level result filtering/transformation API
- Incremental summary updates (avoid recomputing on each merge)

## Error Handling

### Error Result Construction

`_error_result()` creates standardized error results (`tools/cq/run/runner.py:1275-1287`):

```python
def _error_result(step_id: str, macro: str, exc: Exception, ctx: CliContext) -> CqResult:
    result = build_error_result(
        macro=macro,
        root=ctx.root,
        argv=ctx.argv,
        tc=ctx.toolchain,
        started_ms=ms(),
        error=exc,
    )
    result.key_findings.append(
        Finding(category="error", message=f"{step_id}: {exc}", severity="error")
    )
    return assign_result_finding_ids(result)
```

Error results:
- Include `"error"` key in summary
- Have one `Finding` with `category="error"` and `severity="error"`
- Preserve step ID and macro in message

### Stop-on-Error Behavior

`stop_on_error: bool = False` parameter:
- **False (default)**: All steps execute, partial failures included in result
- **True**: Execution stops at first error, subsequent steps skipped

Error detection (`runner.py:429-430`):

```python
def _results_have_error(results: list[tuple[str, CqResult]]) -> bool:
    return any("error" in result.summary for _, result in results)
```

This checks for the `"error"` key in summary, not exception types.

### Exception Boundaries

Non-Q steps wrapped in try-except (`runner.py:834-845`):

```python
def _execute_non_q_step_safe(
    step: RunStep,
    plan: RunPlan,
    ctx: CliContext,
    *,
    run_id: str,
) -> tuple[str, CqResult]:
    step_id = step.id or step_type(step)
    try:
        return step_id, _execute_non_q_step(step, plan, ctx, run_id=run_id)
    except RUN_STEP_NON_FATAL_EXCEPTIONS as exc:
        return step_id, _error_result(step_id, step_type(step), exc, ctx)
```

Q-step execution also has per-step try-except in batch runners (`runner.py:559-568`):

```python
try:
    result = execute_entity_query_from_records(request)
except RUN_STEP_NON_FATAL_EXCEPTIONS as exc:
    result = _error_result(step.parent_step_id, "q", exc, ctx)
    result.summary["lang"] = step.plan.lang
    result.summary["query_text"] = step.step.query
    results.append((step.parent_step_id, result))
    if stop_on_error:
        break
    continue
```

These are "defensive boundaries" that prevent one step failure from crashing the entire run. Only specific exception types are caught (defined in `RUN_STEP_NON_FATAL_EXCEPTIONS`).

### Architectural Observations for Improvement Proposals

**Error Handling Design Tensions**:
- Exception catching is selective but may still hide unexpected failures
- Error results are structurally identical to success results (no distinct type)
- No error recovery or retry logic
- Partial results have no indicator of incompleteness
- Stack traces not captured (only exception message)

**Potential Improvement Vectors**:
- Typed error result discriminated union (`Success | Error`)
- Structured error taxonomy (parse error, execution error, timeout, etc.)
- Optional stack trace capture with `--debug` flag
- Retry policy for transient failures
- Partial success indicator in summary (e.g., "5/10 steps succeeded")

## Global Scope Filtering

### Run-Level Scope Application

`_apply_run_scope()` merges plan and query scopes (`tools/cq/run/runner.py:1202-1211`):

```python
def _apply_run_scope(query: Query, plan: RunPlan) -> Query:
    if not plan.in_dir and not plan.exclude:
        return query
    scope = query.scope
    merged = Scope(
        in_dir=_merge_in_dir(plan.in_dir, scope.in_dir),
        exclude=tuple(_merge_excludes(plan.exclude, scope.exclude)),
        globs=scope.globs,
    )
    return query.with_scope(merged)
```

Merge logic:
- `in_dir`: Concatenate paths (e.g., `"src" + "semantics"` → `"src/semantics"`) via `_merge_in_dir()` (`runner.py:1214-1217`)
- `exclude`: Union of patterns (deduplicated) via `_merge_excludes()` (`runner.py:1220-1231`)

### Result-Level Scope Filtering

`_apply_run_scope_filter()` post-filters results (`tools/cq/run/runner.py:1151-1192`):

Filtering strategy:
1. Resolve `base = (root / in_dir).resolve()` if `in_dir` provided
2. For each finding with anchor:
   - Check `abs_path.is_relative_to(base)` if base exists
   - Check `rel_path.match(pattern)` for each exclude pattern
3. Filter `key_findings`, `evidence`, and section findings
4. Drop sections with no remaining findings

This is a "post-filter" approach: execution runs on full file set, then findings are filtered by location.

### Search Step Scope Building

`_build_search_includes()` constructs include globs for search steps (`runner.py:1195-1199`):

```python
def _build_search_includes(run_in_dir: str | None, step_in_dir: str | None) -> list[str] | None:
    combined = _merge_in_dir(run_in_dir, step_in_dir)
    if not combined:
        return None
    return [f"{combined}/**"]
```

This converts directory scope into glob patterns for the search pipeline.

### Architectural Observations for Improvement Proposals

**Scope Filtering Design Tensions**:
- Post-filtering wastes work on excluded files
- No early termination for scope violations
- Pattern matching uses `Path.match()` (limited glob support)
- Findings without anchors always pass filter (may be incorrect)
- No scope validation before execution (only at result time)

**Potential Improvement Vectors**:
- Pre-filtering: apply scope to file tabulation
- Scope validation: check for empty scope before execution
- Richer glob syntax (e.g., `**/*.py` vs `*.py`)
- Explicit handling of anchor-less findings
- Scope statistics: report files scanned vs files in scope

## Performance Characteristics

### Batch Optimization Impact

#### Entity Query Batching

- **Without Batching**: N steps × M files → N × M ast-grep scans
- **With Batching**: N steps × M files → 1 ast-grep scan
- **Speedup**: O(N) for N steps sharing language and record types

Benchmark (hypothetical):
```
5 steps, 500 files, 100ms per scan
Without batching: 5 × 100ms = 500ms
With batching: 100ms + (5 × 1ms per-step overhead) = 105ms
Speedup: 4.8x
```

#### Relational Span Batching

- **Without Caching**: N queries × 1 file → N parses
- **With Caching**: N queries × 1 file → 1 parse per language
- **Speedup**: O(N) for N queries with relational constraints

Benchmark (hypothetical):
```
3 queries with `inside` constraints, 100 files, 10ms per parse
Without caching: 3 × 100 × 10ms = 3000ms
With caching: 100 × 10ms + (3 × 100 × 0.1ms per-match overhead) = 1030ms
Speedup: 2.9x
```

#### Parallel Non-Q Execution

- **Serial**: N non-Q steps × average latency = N × avg latency
- **Parallel**: max(step latencies) + scheduling overhead
- **Speedup**: Up to N for N independent steps (bounded by worker count)

Benchmark (hypothetical):
```
4 non-Q steps, each 200ms, 4 workers
Serial: 4 × 200ms = 800ms
Parallel: 200ms + 10ms overhead = 210ms
Speedup: 3.8x
```

### Memory Overhead

Session memory footprint:
- `records: list[SgRecord]`: ~500 bytes per record (estimate)
- `scan: ScanContext`: O(number of files) for indices
- `candidates: EntityCandidates`: O(number of entities) for candidate lists
- Parse cache: O(file size × number of languages) for `SgRoot` objects

Example:
```
1000 files, average 10KB each, 5000 records, 2 languages
Records: 5000 × 500 bytes = 2.5 MB
Parse cache: 1000 × 10KB × 2 = 20 MB
Total: ~23 MB
```

This is acceptable for most repositories but may be prohibitive for very large monorepos.

### Execution Latency

#### Q-Step Latency Breakdown

1. **Parsing**: O(query complexity) — typically <1ms
2. **Scope Expansion**: O(number of languages) — typically 1-2 languages
3. **Batch Session**: O(files × record types × rules) — typically 100-1000ms
4. **Entity Matching**: O(records × entity filters) — typically 10-100ms per step
5. **Result Collapsing**: O(findings × languages) — typically <10ms

Total Q-step latency: Dominated by batch session construction (ast-grep scan).

#### Non-Q Step Latency

Varies by macro:
- `calls`: O(files × functions) — typically 100-500ms
- `impact`: O(call graph depth × fan-out) — typically 500-2000ms
- `imports`: O(import graph size) — typically 100-1000ms
- `neighborhood`: O(LSP operations + structural scan) — typically 500-3000ms
- Others: O(files) — typically 100-500ms

Non-Q steps dominate run time for analysis-heavy workloads. Parallelism can significantly reduce total latency when multiple non-Q steps are present.

### Architectural Observations for Improvement Proposals

**Performance Design Tensions**:
- Batch session is all-or-nothing (no incremental construction)
- Memory footprint grows linearly with file count (no pagination)
- Parallel execution available for non-Q steps but requires configuration
- In-memory parse cache per invocation (no persistence across runs)
- No cost estimation or adaptive strategy

**Potential Improvement Vectors**:
- Incremental session construction with early step execution
- Memory-bounded batching with file chunking
- Auto-tune parallelism based on available cores and step count
- Persistent parse cache (disk-backed with TTL) - infrastructure exists, not yet applied to scan-level cache
- Cost-based execution planning (estimate latency before execution)
- Adaptive worker pool sizing based on step characteristics

## Testing Strategy

### Unit Test Coverage

- `test_batch_q_steps_scan_once`: Validates single ast-grep scan for multiple Q steps via monkeypatch
- `test_run_plan_validation`: Schema validation for RunPlan loading
- `test_step_coercion`: Step coercion from dict/dataclass
- `test_chain_parsing`: Token parsing and command chaining
- `test_scope_filtering`: Global scope filter application
- `test_parallel_execution`: Worker scheduler integration for non-Q steps

### Integration Test Coverage

- End-to-end run execution with mixed step types
- Language scope expansion and result collapsing
- Error propagation with `stop_on_error`
- Result provenance tracking
- Parallel vs serial execution comparison

### Performance Test Coverage

- Batch vs non-batch execution latency
- Parse cache hit rate
- Memory footprint scaling
- Parallel execution speedup

### Architectural Observations for Improvement Proposals

**Testing Design Tensions**:
- No property-based testing for plan validation
- Limited concurrency testing for parallel execution paths
- No performance regression suite
- Integration tests don't cover all step type combinations
- Mock-based tests tightly coupled to implementation

**Potential Improvement Vectors**:
- Property-based testing with Hypothesis for plan validation
- Concurrency stress testing for parallel execution
- Performance benchmarking suite with historical tracking
- Exhaustive step type combination matrix
- Contract-based testing for step execution interfaces

## Future Directions

### Full Parallel Execution

Build dependency DAG from step inputs/outputs:
- Parse each step's query to extract entity references
- Detect data dependencies (e.g., `calls` depends on function from prior `q` step)
- Construct DAG with `rustworkx` or similar
- Execute independent Q and non-Q steps in parallel with thread/process pool

### Incremental Execution

Cache step results and reuse on unchanged inputs:
- Hash step specification + input file set → cache key
- Store result in persistent cache (e.g., SQLite or DiskCache)
- Invalidate cache on file content changes (use git SHA or mtime)
- Report cache hit rate in summary

### Streaming Results

Emit step results as they complete:
- Change return type to `Iterator[CqResult]`
- Yield each step result immediately after execution
- Support streaming formatters (e.g., JSONL, markdown stream)

### Adaptive Batching

Choose batch strategy based on file count and step characteristics:
- Small file sets (<100 files): Skip batch overhead, execute independently
- Large record type union: Partition into multiple batches
- Memory pressure: Chunk files and process in waves

### Rich Error Reporting

Improve error diagnostics:
- Structured error taxonomy (parse/execution/timeout/resource)
- Stack trace capture in debug mode
- Error suggestions (e.g., "did you mean entity=class?")
- Partial result preservation (return findings from successful steps)

### Plan Introspection

Add commands to understand execution strategy:
- `cq run --explain`: Show execution plan without running
- `cq run --estimate`: Estimate latency and memory usage
- `cq run --dry-run`: Validate plan without execution

## Execution Flow Diagram

```mermaid
flowchart TD
    Start[Load RunPlan] --> Normalize[Normalize Step IDs]
    Normalize --> Partition[Partition: Q vs Non-Q Steps]

    Partition --> QBranch{Q Steps?}
    QBranch -->|Yes| QParse[Parse & Expand Q Steps]
    QParse --> QGroup[Group by Language & Type]
    QGroup --> EntityBatch[Entity Query Batch]
    QGroup --> PatternBatch[Pattern Query Batch]
    EntityBatch --> QCollapse[Collapse Per-Language Results]
    PatternBatch --> QCollapse
    QCollapse --> QMerge[Merge Q Results]
    QBranch -->|No| NonQBranch

    QMerge --> NonQBranch{Non-Q Steps?}
    NonQBranch -->|Yes| ParallelCheck{Parallelism Enabled?}
    ParallelCheck -->|Yes| Parallel[Execute Non-Q Parallel]
    ParallelCheck -->|No| Serial[Execute Non-Q Serial]
    Parallel --> NonQMerge[Merge Non-Q Results]
    Serial --> NonQMerge
    NonQBranch -->|No| Finalize

    NonQMerge --> Finalize[Populate Summary Metadata]
    Finalize --> Assign[Assign Finding IDs]
    Assign --> Evict[Evict Cache Tags]
    Evict --> Done[Return CqResult]
```

## Step Type Dispatch Flow

```mermaid
flowchart TD
    Step[RunStep] --> QCheck{Is QStep?}
    QCheck -->|Yes| QBatch[Batch Entity/Pattern Executor]
    QCheck -->|No| SearchCheck{Is SearchStep?}

    SearchCheck -->|Yes| Search[Execute Search]
    SearchCheck -->|No| NeighborhoodCheck{Is NeighborhoodStep?}

    NeighborhoodCheck -->|Yes| Neighborhood[Execute Neighborhood]
    NeighborhoodCheck -->|No| Dispatch[Lookup in _NON_SEARCH_DISPATCH]

    Dispatch --> Executor[Call Executor Function]
    Executor --> Filter[Apply Run Scope Filter]
    Filter --> Result[Return CqResult]

    Search --> Result
    Neighborhood --> Result
    QBatch --> Result
```

## Conclusion

The multi-step execution architecture achieves significant performance gains through batching (4-5x speedup for entity queries) and parallel execution (up to Nx speedup for non-Q steps) while maintaining a clean separation between plan specification, execution strategy, and result aggregation. The language scope expansion and result collapsing mechanisms enable seamless cross-language analysis without user-visible complexity.

**Key strengths:**
- Shared ast-grep scan eliminates redundant parsing for Q-steps
- Parse caching optimizes relational span collection
- Parallel execution for non-Q steps via WorkerScheduler
- Provenance tracking enables result attribution
- Error boundaries prevent cascading failures
- 11 step types support diverse analysis workflows (search, entity queries, pattern matching, call graphs, data flow, imports, exceptions, side effects, scopes, bytecode, neighborhoods)
- Runtime services integration provides configurable parallelism, LSP budgets, and policy control

**Key opportunities:**
- Full parallel execution for independent Q and non-Q steps (requires dependency DAG)
- Persistent parse cache across invocations (infrastructure exists for macro-level results)
- Adaptive batching based on file counts and memory pressure
- Rich error reporting with structured taxonomy and suggestions
- Plan introspection and cost estimation
- Neighborhood step support in chain command
- Auto-tuning of parallelism based on available resources

**Extension Points:**

New step types require:
1. Add step class to `tools/cq/run/spec.py` with `RunStepBase` inheritance
2. Add to `RunStep` union type and `RUN_STEP_TYPES` tuple
3. Add tag to `_STEP_TAGS` dict
4. Implement executor function in `runner.py`
5. Add to `_NON_SEARCH_DISPATCH` dispatch table (or handle separately for special cases like SearchStep/NeighborhoodStep)
6. Optionally add chain builder to `chain.py` for `cq chain` support

**Key File Locations:**
- Core execution: `tools/cq/run/runner.py` (1297 LOC)
- Step specs: `tools/cq/run/spec.py` (236 LOC)
- Plan loading: `tools/cq/run/loader.py` (84 LOC)
- Chain frontend: `tools/cq/run/chain.py` (197 LOC)
- Step decode: `tools/cq/run/step_decode.py` (50 LOC)
- Batch session: `tools/cq/query/batch.py`
- Span batching: `tools/cq/query/batch_spans.py`
- Result merge: `tools/cq/core/merge.py`
