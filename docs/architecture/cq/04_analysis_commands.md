# Analysis Commands Architecture

**Version**: 0.4.0
**Document Status**: Phase 3
**Last Updated**: 2026-02-15

## Overview

CQ analysis commands provide impact analysis, call site census, scope capture analysis, bytecode inspection, signature change simulation, and exception handling pattern analysis. The macro system operates at the intersection of static AST analysis, bytecode inspection, symtable introspection, tree-sitter structural queries, and heuristic search fallbacks.

Analysis commands (primarily `calls`) embed a `FrontDoorInsightV1` contract providing target identity, neighborhood preview, and risk assessment as the first output block. The Insight Card offers concise front-door grounding: target selection with location and signature, neighborhood slice totals with preview nodes, risk level with explicit drivers and counters, confidence scoring from evidence kind, degradation status, and budget controls.

This document targets advanced LLM programmers seeking to propose architectural improvements. All line references are stable as of 2026-02-15.

## Module Map

### Core Analysis Commands

**Primary Macros** (`tools/cq/macros/`):
- `calls.py` (2311 lines) - Call site census with argument shape analysis and FrontDoor insight
- `calls_target.py` (741 lines) - Target resolution with persistent cache, callee extraction
- `impact.py` (880 lines) - Taint/data flow propagation with inter-procedural analysis
- `sig_impact.py` (427 lines) - Signature change impact simulation with compatibility classification
- `scopes.py` (361 lines) - Closure/scope capture analysis via symtable introspection
- `bytecode.py` (441 lines) - Bytecode surface analysis for hidden dependencies
- `side_effects.py` (463 lines) - Import-time side effect detection
- `imports.py` (619 lines) - Import structure and cycle analysis
- `exceptions.py` (592 lines) - Exception handling pattern analysis
- `scope_filters.py` (51 lines) - Shared scope filtering helpers for macro file enumeration

**Shared Macro Infrastructure** (`tools/cq/macros/`):
- `contracts.py` (64 lines) - Base request contracts: `MacroRequestBase`, `ScopedMacroRequestBase`
- `scan_utils.py` (30 lines) - File scan helper with glob filters
- `scoring_utils.py` (67 lines) - Normalized scoring payload builders
- `target_resolution.py` (44 lines) - File/symbol target resolution
- `common/` - Re-export package for shared contracts, scoring, targets

**Cross-Language Fallback** (`tools/cq/macros/`):
- `multilang_fallback.py` (82 lines) - Shared multilang summary assembly and diagnostics
- `_rust_fallback.py` (94 lines) - Shared Rust fallback search helper

### Supporting Infrastructure

**Index Subsystem** (`tools/cq/index/`):
- `def_index.py` (676 lines) - DefIndex: function/class definition index with import tracking
- `call_resolver.py` (352 lines) - Call expression → FnDecl resolution with confidence scoring
- `arg_binder.py` (360 lines) - Argument → parameter binding for taint propagation
- `graph_utils.py` - Strongly connected component detection for cycle analysis

**CLI Layer**:
- `cli_app/commands/analysis.py` (361 lines) - CLI dispatch layer for all analysis macros

**Scoring and Caching**:
- `core/scoring.py` (200 lines) - Impact and confidence scoring with evidence-kind mapping
- `core/cache.py` - Persistent disk cache infrastructure (used by calls_target)

## Shared Macro Infrastructure

All analysis macros have been refactored to use common infrastructure for request handling, target resolution, file scanning, and scoring. This eliminates code duplication and establishes consistent patterns across macro implementations.

### Base Request Contracts

**MacroRequestBase** (`macros/contracts.py:16-22`):
```python
class MacroRequestBase(CqStruct, frozen=True):
    """Base request envelope shared by macro entry points."""
    tc: Toolchain
    root: Path
    argv: list[str]
```

All macro request types extend `MacroRequestBase` to ensure consistent access to toolchain, repository root, and command-line arguments.

**ScopedMacroRequestBase** (`macros/contracts.py:24-28`):
```python
class ScopedMacroRequestBase(MacroRequestBase, frozen=True):
    """Macro request base with shared include/exclude scope filters."""
    include: list[str] = msgspec.field(default_factory=list)
    exclude: list[str] = msgspec.field(default_factory=list)
```

Macros that scan multiple files extend `ScopedMacroRequestBase` to inherit standard scope filtering capability.

**Usage Hierarchy**:
- `MacroRequestBase` ← `BytecodeSurfaceRequest`, `ScopeRequest`
- `ScopedMacroRequestBase` ← `ExceptionsRequest`, `ImportRequest`, `SideEffectsRequest`
- `MacroRequestBase` ← `ImpactRequest`, `SigImpactRequest` (no scope filtering needed)

### Target Resolution

**resolve_target_files** (`macros/target_resolution.py:10-42`):
```python
def resolve_target_files(
    *,
    root: Path,
    target: str,
    max_files: int,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    extensions: tuple[str, ...] = (".py",),
) -> list[Path]:
    """Resolve explicit path targets or symbol-hint file candidates."""
```

**Resolution Strategy**:
1. **Explicit Path**: Check if target is absolute or relative path to existing file
2. **Rooted Path**: Try `root / target` as file path
3. **Symbol Search**: Scan files matching include/exclude/extensions for `def {target}` or `class {target}`

**Used By**:
- `bytecode.py:373` - Resolve bytecode analysis targets
- `scopes.py:283` - Resolve scope analysis targets

### File Scanning

**iter_files** (`macros/scan_utils.py:10-27`):
```python
def iter_files(
    *,
    root: Path,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
    extensions: tuple[str, ...] = (".py",),
    max_files: int | None = None,
) -> list[Path]:
    """Resolve files for macro scans with optional include/exclude globs."""
```

Wraps `resolve_macro_files` from `scope_filters.py` with optional max_files truncation.

**Used By**:
- `exceptions.py:284` - File iteration for exception analysis
- `imports.py:285` - File iteration for import analysis
- `side_effects.py:257` - File iteration for side-effect analysis

### Scoring Utilities

**macro_scoring_details** (`macros/scoring_utils.py:17-43`):
```python
def macro_scoring_details(
    *,
    sites: int,
    files: int,
    depth: int = 0,
    breakages: int = 0,
    ambiguities: int = 0,
    evidence_kind: str = "resolved_ast",
) -> dict[str, object]:
    """Build a normalized scoring-details mapping for macro findings."""
```

Constructs scoring payload from `ImpactSignals` and `ConfidenceSignals` with normalized bucket classification.

**macro_score_payload** (`macros/scoring_utils.py:46-64`):
```python
def macro_score_payload(*, files: int, findings: int) -> MacroScorePayloadV1:
    """Build a normalized macro scoring payload from simple counters."""
```

Convenience builder for simple file/finding count payloads.

**Used By** (all macros):
- `bytecode.py:400` - Bytecode scoring with `evidence_kind="bytecode"`
- `exceptions.py:542` - Exception scoring with breakages count
- `imports.py:555` - Import scoring with depth from cycle length
- `scopes.py:309` - Scope scoring with `evidence_kind="resolved_ast"`
- `side_effects.py:414` - Side-effect scoring
- `impact.py:766` - Impact scoring with cross-file taint evidence
- `sig_impact.py:376` - Signature impact scoring with breakages/ambiguities

### Common Re-Exports

The `macros/common/` package provides clean re-export surfaces for shared infrastructure:

**contracts.py** (`macros/common/contracts.py:1-19`):
- Re-exports: `MacroRequestBase`, `ScopedMacroRequestBase`, `MacroExecutionRequestV1`, `MacroTargetResolutionV1`, `MacroScorePayloadV1`

**scoring.py** (`macros/common/scoring.py:1-7`):
- Re-exports: `macro_score_payload`

**targets.py** (`macros/common/targets.py:1-7`):
- Re-exports: `resolve_target_files`

**Usage**: These re-exports allow future macro implementations to import from `tools.cq.macros.common` without knowing the internal module structure.

## Architecture Patterns

### 1. Two-Stage Collection

All analysis commands use a fast pre-filter stage followed by precise AST parsing:

1. **Fast Scan**: ripgrep or ast-grep identifies candidate files
2. **Precise Parse**: AST parsing only on candidates

**Example** (`calls.py:1453-1524`):
```python
def _scan_call_sites(root_path: Path, function_name: str) -> CallScanResult:
    # Fast file-level filter with ripgrep
    pattern = rf"\b{search_name}\s*\("
    candidate_files = find_files_with_pattern(root_path, pattern, limits=INTERACTIVE)

    # Try ast-grep first, fall back to ripgrep on error
    if candidate_files:
        try:
            call_records = sg_scan(paths=candidate_files, record_types={"call"}, root=root_path)
            used_fallback = False
        except (OSError, RuntimeError, ValueError):
            used_fallback = True

    if used_fallback:
        candidates = _rg_find_candidates(function_name, root_path)
        all_sites = _collect_call_sites(root_path, by_file, function_name)
```

### 2. Visitor Pattern for AST Traversal

Analysis commands use `ast.NodeVisitor` subclasses for structured code inspection:

**CallFinder** (`calls.py:792-838`):
- Visits all `ast.Call` nodes
- Matches target expression by name/qualified name
- Analyzes argument shape (positional, keyword, starred)
- Detects hazards (star_args, dynamic dispatch, eval/exec)

**TaintVisitor** (`impact.py:125-270`):
- Tracks tainted variable set
- Propagates taint through assignments, calls, returns
- Records taint sites for each propagation
- Collects call sites with tainted arguments for inter-procedural analysis

**ExceptionVisitor** (`exceptions.py:115-266`):
- Extracts raise statements with exception types
- Captures exception handlers with caught types
- Detects bare except clauses and reraises
- Tracks context (function/class) for each site

### 3. Target Resolution with Persistent Caching

The `calls_target.py` module provides shared target resolution infrastructure with cache-backed performance optimization.

**Cache Contract** (`tools/cq/core/cache/contracts.py:CallsTargetCacheV1`):
```python
class CallsTargetCacheV1(CqStruct):
    target_location: tuple[str, int] | None
    target_callees: dict[str, int]
    snapshot_digest: str | None
```

**Resolution Flow** (`calls_target.py:680-731`):
1. **Cache Lookup**: Check DiskCache for target metadata (900s TTL)
2. **Snapshot Validation**: Verify file digest matches cached snapshot
3. **Invalidation**: Re-resolve if snapshot changed or missing
4. **Cache Write**: Persist resolved target location and callees
5. **Tagging**: Associate cache entries with run_id for eviction

**Language Detection** (`calls_target.py:84-111`):
- Python: Search for `\bdef {base_name}\s*\(` pattern
- Rust: Search for `\bfn {base_name}\s*\(` with visibility/async/unsafe modifiers
- Returns `QueryLanguage | None` for inference

**Parallel Resolution** (`calls_target.py:210-229`):
- Worker pool submission for multi-file scanning
- Bounded timeout (2.0s per file)
- Fallback to sequential on timeout

### 4. Cross-Module Integration

**DefIndex** (`def_index.py:412-676`):
- Repository-wide symbol index (functions, classes, methods)
- Import alias tracking (module and symbol imports)
- Built on-demand via full repo scan
- No serialization or persistence (rebuilt per invocation)
- Supports both simple name and qualified name lookup

**CallResolver** (`call_resolver.py:255-316`):
- Resolves call expressions to FnDecl targets
- Handles: simple calls, self/cls method calls, qualified calls, typed receiver calls
- Returns confidence: "exact", "likely", "ambiguous", "unresolved"
- Resolution path labels for diagnostics

**ArgBinder** (`arg_binder.py:260-305`):
- Maps call-site arguments to callee parameter names
- Handles positional, keyword, *args, **kwargs
- Tracks unbound args and params (missing required params)
- Returns binding results for taint propagation

### 5. Scope Filtering

All macros that scan multiple files support `--include` and `--exclude` globs for scope control.

**Shared Infrastructure** (`scope_filters.py:20-48`):
```python
def resolve_macro_files(
    *,
    root: Path,
    include: Sequence[str] | None,
    exclude: Sequence[str] | None,
    extensions: tuple[str, ...],
) -> list[Path]:
    # Uses tabulate_files with gitignore-aware semantics
    # Include globs: direct patterns
    # Exclude globs: negated patterns (prefixed with "!")
```

**Used By**:
- `side_effects.py` - `--include src/ --exclude tests/`
- `imports.py` - `--include src/ tools/`
- `exceptions.py` - `--exclude scripts/`

**Summary Telemetry**:
All scoped macros report `scope_file_count` and `scope_filter_applied` in summary.

### 6. Multilang Fallback Pattern

Analysis commands append Rust fallback findings for cross-language coverage.

**Shared Helper** (`_rust_fallback.py:19-94`):
```python
def rust_fallback_search(
    root: Path,
    pattern: str,
    *,
    macro_name: str,
) -> tuple[list[Finding], list[Finding], dict[str, object]]:
    # ripgrep search in Rust files (lang_scope="rust")
    # Returns: (rust_findings, capability_diagnostics, partition_stats)
```

**Application** (`multilang_fallback.py:34-77`):
```python
def apply_rust_macro_fallback(
    *,
    result: CqResult,
    root: Path,
    pattern: str,
    macro_name: str,
    fallback_matches: int = 0,
    query: str | None = None,
) -> CqResult:
    # Merges Rust findings into result.evidence
    # Builds multilang summary with Python + Rust partition stats
```

**Rust Search Patterns**:
- `calls`: Function name
- `impact`: Function name
- `sig-impact`: Symbol name
- `scopes`: Target symbol/file
- `side-effects`: `static mut \|lazy_static\|thread_local\|unsafe `
- `imports`: `use ` or module filter
- `exceptions`: `panic!\|unwrap\|expect\|Result<\|Err(`

### 7. Scoring System

**ImpactSignals** (`scoring.py:51-73`):
- sites (45% weight, normalized by 100)
- files (25% weight, normalized by 20)
- depth (15% weight, normalized by 10)
- breakages (10% weight, normalized by 10)
- ambiguities (5% weight, normalized by 10)

**ConfidenceSignals** (`scoring.py:75-88`):
- Evidence kind determines confidence score
- resolved_ast: 0.95
- bytecode: 0.90
- resolved_ast_heuristic: 0.75
- cross_file_taint: 0.70
- heuristic: 0.60
- rg_only: 0.45
- unresolved: 0.30

**Bucket Classification**:
- high: >= 0.7
- med: >= 0.4
- low: < 0.4

## Command Deep Dive

### calls: Call Site Census

**Purpose**: Find all call sites for a function with argument shape analysis, hazard detection, and neighborhood context.

#### Data Structures

**CallSite** (`calls.py:84-157`):
```python
class CallSite(msgspec.Struct):
    file: str
    line: int
    col: int
    num_args: int
    num_kwargs: int
    kwargs: list[str]
    has_star_args: bool
    has_star_kwargs: bool
    context: str                      # Containing function
    arg_preview: str                  # Truncated arg preview
    callee: str                       # Resolved callee name
    receiver: str | None              # Receiver for method calls
    resolution_confidence: str        # exact/likely/ambiguous/unresolved
    resolution_path: str              # Resolution strategy label
    binding: str                      # ok/ambiguous/would_break/unresolved
    target_names: list[str]           # Resolved target names
    call_id: str                      # Stable UUID7 identifier
    hazards: list[str]                # Dynamic hazards
    symtable_info: dict | None        # Symtable analysis
    bytecode_info: dict | None        # Bytecode analysis
    context_window: dict | None       # Line range for containing def
    context_snippet: str | None       # Source code snippet
```

#### Algorithm Pipeline

**1. Target Resolution and Caching** (`calls_target.py:680-731`):

**Cache Lookup**:
```python
cached = context.cache.get(context.cache_key) if context.cache_enabled else None
if isinstance(cached, dict):
    cached_payload = msgspec.convert(cached, type=CallsTargetCacheV1)
    # Validate snapshot digest
    current_snapshot = _target_scope_snapshot_digest(...)
    if snapshot_digest == current_snapshot:
        # Use cached target_location and target_callees
```

**Target Resolution** (`calls_target.py:60-82`):
```python
def resolve_target_definition(
    root: Path,
    function_name: str,
    *,
    target_language: QueryLanguage | None = None,
) -> tuple[str, int] | None:
    # Returns (relative_file, line) for target definition
    # Supports Python and Rust
```

**Python Resolution** (`calls_target.py:113-135`):
- ripgrep scan for `\bdef {base_name}\s*\(`
- Parallel AST parsing with worker pool
- Returns first match (file, line)

**Rust Resolution** (`calls_target.py:137-194`):
- ripgrep scan with visibility/async/unsafe modifiers
- ast-grep scan with `record_types={"def"}`
- Regex fallback for def line extraction

**Callee Extraction** (`calls_target.py:260-323`):
- Python: Walk target function AST, extract Call nodes
- Rust: ast-grep scan for call records within target bounds
- Returns `Counter[str]` of callee names

**Cache Persistence** (`calls_target.py:647-678`):
```python
cache_payload = CallsTargetCacheV1(
    target_location=target_location,
    target_callees=dict(target_callees),
    snapshot_digest=snapshot_digest,
)
tag = resolve_write_cache_tag(...)  # Includes run_id
context.cache.set(cache_key, contract_to_builtins(cache_payload), expire=ttl, tag=tag)
```

**2. Candidate Scan** (`calls.py:1453-1524`):
- Use ripgrep for fast file-level filtering: `\b{function_name}\s*\(`
- Try ast-grep on candidate files for structural matching
- Fall back to ripgrep if ast-grep fails
- Track fallback status for confidence scoring

**3. Call Site Collection** (`calls.py:892-983`):
- Parse candidate files into AST
- Visit all `ast.Call` nodes with CallFinder visitor
- Match target expression by name or qualified name
- Extract argument shape: positional, keyword, *args, **kwargs

**4. Argument Analysis** (`calls.py:278-321`):
- Count positional args (non-starred)
- Count keyword args (non-**kwargs)
- Detect star_args (`ast.Starred` in args)
- Detect star_kwargs (`kw.arg is None` in keywords)
- Build arg preview (truncate to 20 chars per arg, max 3 args + 2 kwargs)

**5. Hazard Detection** (`calls.py:440-464`):
- star_args/star_kwargs forwarding
- Dynamic dispatch: getattr, setattr, delattr, hasattr
- Code execution: eval, exec, __import__
- Introspection: globals(), locals(), vars()
- Dynamic imports: importlib.import_module
- Higher-order functions: operator.attrgetter/methodcaller, functools.partial

**6. Enrichment** (`calls.py:467-522`):
- **Symtable analysis**: Extract free_vars, globals_used, nested_scopes
- **Bytecode analysis**: Extract LOAD_GLOBALS, LOAD_ATTRS, CALL_FUNCTIONS opcodes
- Best-effort compilation (silently skip on syntax errors)

**7. Context Window Extraction** (`calls.py:524-568`):
- Find containing def by indentation hierarchy
- Compute start_line/end_line for containing function
- Extract source snippet with intelligent truncation (max 30 lines)
- Skip docstrings when extracting snippet

**Context Snippet Algorithm** (`calls.py:570-778`):
- Collect function header indices (def line + body-level statements)
- Skip docstrings via triple-quote detection
- Collect anchor block indices (match line + enclosing block)
- Select mandatory indices (header + anchor)
- Fill remaining budget with surrounding lines (max 30 total)
- Render with omission markers for skipped lines

**8. Scoring** (`calls.py:1526-1568`):
- ImpactSignals: sites, files, forwarding_count as ambiguities
- ConfidenceSignals:
  - "rg_only" if fallback to ripgrep
  - "resolved_ast_closure" if closures detected
  - "resolved_ast_heuristic" if hazards detected
  - "resolved_ast_bytecode" if bytecode analysis succeeded
  - "resolved_ast" otherwise

#### Calls Insight Card

The `calls` command embeds a `FrontDoorInsightV1` contract as the first output block.

**Build Process** (`calls.py:2040-2099`):
```python
insight = build_calls_insight(
    CallsInsightBuildRequestV1(
        function_name=function_name,
        signature=signature_info or None,
        location=InsightLocationV1(file=..., line=..., col=0),
        neighborhood=neighborhood,
        files_with_calls=files_with_calls,
        arg_shape_count=len(arg_shapes),
        forwarding_count=forwarding_count,
        hazard_counts=dict(hazard_counts),
        confidence=confidence,
        budget=InsightBudgetV1(...),
        degradation=InsightDegradationV1(...),
    )
)
```

**Risk Computation** (`front_door_insight.py:risk_from_counters`):
- **Drivers**: `high_call_surface` (callers >= 10), `medium_call_surface` (callers >= 4), `argument_forwarding`, `dynamic_hazards`, `arg_shape_variance`, `closure_capture`
- **Risk Level**:
  - "high" if callers > 10 OR hazards > 0 OR (forwarding > 0 AND callers > 0)
  - "med" if callers > 3 OR arg_shape_count > 3 OR files_with_calls > 3 OR closure_capture_count > 0
  - "low" otherwise
- **Counters**: callers, callees, files_with_calls, arg_shape_count, forwarding_count, hazard_count, closure_capture_count

**Neighborhood Construction** (`calls.py:1640-1752`):
- Structural neighborhood via tree-sitter collector
- Fallback to heuristic neighborhood from call census contexts
- Callees from target body extraction (bounded to preview limit)
- Semantic overlay via language-specific LSP integration (Python/Rust)

**Budget Defaults**:
- top_candidates: 3 (when ambiguous)
- preview_per_slice: 5 nodes per neighborhood slice
- semantic_targets: 1

**Output Sections**:
- Insight Card (FrontDoorInsightV1) - Rendered first, before section reordering
- Neighborhood Preview
- Target Callees Preview
- Argument Shape Histogram
- Hazards
- Keyword Argument Usage
- Calling Contexts
- Call Sites

### impact: Taint/Data Flow Analysis

**Purpose**: Trace how data flows from a specific parameter through function calls.

**Request Type**: Extends `MacroRequestBase` (`impact.py:86-91`)

**Shared Infrastructure Usage**:
- Uses `macro_scoring_details` with cross-file taint evidence (`impact.py:766`)

#### Data Structures

**TaintedSite** (`impact.py:41-65`):
```python
class TaintedSite(msgspec.Struct):
    file: str
    line: int
    kind: str                # "source", "call", "return", "assign"
    description: str
    param: str | None        # Tainted parameter
    depth: int               # Propagation depth
```

**TaintState** (`impact.py:68-83`):
```python
class TaintState(msgspec.Struct):
    tainted_vars: set[str]          # Currently tainted variables
    tainted_sites: list[TaintedSite] # Recorded taint sites
    visited: set[str]               # Visited function keys (cycle prevention)
```

#### Algorithm

**1. Index Construction** (`impact.py:883-902`):
- Build DefIndex from repository
- Resolve function by name or qualified name
- Filter functions containing target parameter

**2. Intra-Procedural Taint Tracking** (`impact.py:114-259`):
- Initialize tainted set with target parameter name
- Visit assignments: propagate taint from RHS to LHS
- Visit calls: record tainted arguments for inter-procedural analysis
- Visit returns: record tainted return values

**3. Taint Propagation Handlers** (`impact.py:261-432`):

Dictionary mapping AST node types to taint checking functions:

- **Attribute/Subscript**: Taint if sub-expression tainted
- **BinOp/UnaryOp**: Taint if operands tainted
- **Call**: Taint if any argument or receiver tainted
- **IfExp/Compare**: Taint if any branch/operand tainted
- **BoolOp**: Taint if any value tainted
- **Lambda**: Never tainted (conservative)
- **Comprehensions**: Taint if iterating over tainted data

**4. Inter-Procedural Propagation** (`impact.py:489-545`):
```python
def _analyze_function(fn: FnDecl, tainted_params: set[str], ...):
    # Run TaintVisitor on function body
    visitor = TaintVisitor(fn.file, tainted_params, current_depth)
    visitor.visit(fn_node)

    # For each call with tainted args:
    for call_info, tainted_args in visitor.calls:
        resolved = resolve_call_targets(index, call_info)
        for target in resolved.targets:
            bound = bind_call_to_params(call_info.args, call_info.keywords, target)
            new_tainted = tainted_params_from_bound_call(bound, tainted_indices)
            _analyze_function(target, new_tainted, context, current_depth + 1)
```

**5. External Callers** (`impact.py:548-581`):
- Use ripgrep to find potential callers
- Convert absolute paths to relative paths

**Scoring** (`impact.py:760-772`):
- ImpactSignals: sites, files, max_depth
- ConfidenceSignals:
  - "cross_file_taint" if max_depth > 0
  - "resolved_ast" if max_depth == 0

**Output Sections**:
- Key findings: Total taint sites, depth distribution
- Taint Assign/Call/Return Sites: Grouped by kind
- Callers: External callers found via ripgrep

### sig-impact: Signature Change Impact

**Purpose**: Simulate a signature change and classify call sites as would_break, ambiguous, or ok.

**Request Type**: Extends `MacroRequestBase` (`sig_impact.py:60-64`)

**Shared Infrastructure Usage**:
- Uses `macro_scoring_details` with breakages and ambiguities counts (`sig_impact.py:376`)

#### Data Structures

**SigParam** (`sig_impact.py:36-57`):
```python
class SigParam(msgspec.Struct):
    name: str
    has_default: bool
    is_kwonly: bool
    is_vararg: bool          # *args
    is_kwarg: bool           # **kwargs
```

#### Algorithm

**1. Signature Parsing** (`sig_impact.py:67-147`):
```python
def _parse_signature(sig: str) -> list[SigParam]:
    # Extract inside parens via regex
    # Parse as synthetic function def: `def _tmp({inside}): pass`
    # Extract params: positional, *args, keyword-only, **kwargs
    # Track defaults for positional and keyword-only params
```

**2. Call Site Collection** (`sig_impact.py:208-232`):
- Reuse calls.py infrastructure: `rg_find_candidates`, `collect_call_sites`

**3. Call Classification** (`sig_impact.py:150-205`):

**Ambiguous**: Call uses *args or **kwargs

**Would Break**:
- Missing required param (no default, not covered by position or keyword)
- Too many positional args (no *args in new sig)
- Unknown keyword arg (not in param names, no **kwargs in new sig)

**OK**: Compatible call

**4. Classification Logic**:
```python
# Build required param list (no defaults, not keyword-only, not variadic)
required_params = [p.name for p in new_params if not p.has_default and ...]

# For each required param:
for i, param_name in enumerate(required_params):
    # Check coverage by position or keyword
    if i < site.num_args or param_name in kwargs_set:
        continue
    return ("would_break", f"parameter '{param_name}' not provided")

# Check excess positional args
if not has_vararg and site.num_args > max_positional:
    return ("would_break", f"too many positional args")

# Check unknown keyword args
for kw in site.kwargs:
    if kw not in param_names and not has_kwarg:
        return ("would_break", f"keyword '{kw}' not in new signature")
```

**Scoring**:
- ImpactSignals: sites, files, breakages, ambiguities
- ConfidenceSignals: "resolved_ast" (always, since AST-based)

**Output Sections**:
- Would Break Sites: Calls that would fail (severity: error)
- Ambiguous Sites: Calls needing manual review (severity: warning)
- OK Sites: Compatible calls (severity: info)

### scopes: Closure/Scope Analysis

**Purpose**: Analyze scope capture for closures and nested functions via symtable introspection.

**Request Type**: Extends `MacroRequestBase` (`scopes.py:67-71`)

**Shared Infrastructure Usage**:
- Uses `resolve_target_files` for file/symbol target resolution (`scopes.py:283`)
- Uses `macro_scoring_details` with `evidence_kind="resolved_ast"` (`scopes.py:309`)

#### Data Structures

**ScopeInfo** (`scopes.py:31-64`):
```python
class ScopeInfo(msgspec.Struct):
    name: str                # Scope name (qualified)
    kind: str                # "function", "class", "module"
    file: str
    line: int
    free_vars: list[str]     # Captured from enclosing scopes
    cell_vars: list[str]     # Provided to nested scopes
    globals_used: list[str]  # Global references
    nonlocals: list[str]     # Declared nonlocal
    locals: list[str]        # Local variables
```

#### Algorithm

**1. Target Resolution** (`scopes.py:283-289`):
- Uses shared `resolve_target_files` helper
- Resolves explicit paths or searches for symbol definitions
- Returns list of candidate files for scope analysis

**2. Scope Extraction** (`scopes.py:110-157`):
```python
def _extract_scopes(st: symtable.SymbolTable, file: str, parent_name: str = ""):
    # Build symtable via symtable.symtable(source, file, "exec")
    # Recursively walk symtable tree
    for sym in st.get_symbols():
        if sym.is_free():         # Captured from enclosing scope
            free_vars.append(sym_name)
        if is_cell():             # Provided to nested scope
            cell_vars.append(sym_name)
        if sym.is_global():       # Global reference
            globals_used.append(sym_name)
        if is_nonlocal():         # Declared nonlocal
            nonlocals.append(sym_name)
        if sym.is_local():        # Local variable
            locals_list.append(sym_name)

    # Filter: Only report functions with captures
    if kind == "function" and (free_vars or cell_vars or nonlocals):
        scopes.append(ScopeInfo(...))
```

**Scoring**:
- ImpactSignals: sites (closures), files
- ConfidenceSignals: "resolved_ast" (always)

**Output Sections**:
- Key findings: Closures, providers, nonlocal users
- Scope Capture Details: Function-by-function breakdown

### bytecode-surface: Bytecode Analysis

**Purpose**: Analyze compiled bytecode for hidden dependencies without execution.

**Request Type**: Extends `MacroRequestBase` (`bytecode.py:74-79`)

**Shared Infrastructure Usage**:
- Uses `resolve_target_files` for file/symbol target resolution (`bytecode.py:373`)
- Uses `macro_scoring_details` with `evidence_kind="bytecode"` (`bytecode.py:400`)

#### Data Structures

**BytecodeSurface** (`bytecode.py:44-72`):
```python
class BytecodeSurface(msgspec.Struct):
    qualname: str            # Qualified name
    file: str
    line: int
    globals: list[str]       # Global variables accessed
    attrs: list[str]         # Attributes accessed
    constants: list[str]     # Interesting constants
    opcodes: dict[str, int]  # Opcode frequency counts
```

#### Algorithm

**1. Code Object Walking** (`bytecode.py:95-121`):
```python
def _walk_code_objects(co: CodeType, prefix: str = ""):
    # Recursively yield (qualname, CodeType) pairs
    # Walk co_consts for nested code objects
    # Build qualified names via prefix concatenation
```

**2. Code Object Analysis** (`bytecode.py:123-172`):
```python
def _analyze_code_object(co: CodeType, file: str):
    for instr in dis.get_instructions(co):
        # Track opcode frequencies
        surface.opcodes[instr.opname] += 1

        # Extract globals: LOAD_GLOBAL, STORE_GLOBAL, DELETE_GLOBAL
        if instr.opname in _GLOBAL_OPS:
            surface.globals.append(instr.argval)

        # Extract attributes: LOAD_ATTR, STORE_ATTR, DELETE_ATTR
        if instr.opname in _ATTR_OPS:
            surface.attrs.append(instr.argval)

    # Extract constants: strings (< 100 chars), non-trivial numbers
    for const in co.co_consts:
        if isinstance(const, str) and len(const) < 100:
            surface.constants.append(const)
```

**3. Surface Collection** (`bytecode.py:212-231`):
- Compile source: `compile(source, rel_path, "exec")`
- Walk all code objects
- Filter: Only include code objects with globals, attrs, or constants

**Scoring**:
- ImpactSignals: sites (code objects), files
- ConfidenceSignals: "bytecode" (always)

**Output Sections**:
- Bytecode Surfaces: Per-function breakdown
- Global References: Aggregated global usage
- Opcode Summary: Most common opcodes

### side-effects: Import-Time Side Effect Detection

**Purpose**: Detect function calls at module top-level, global state writes, and ambient reads.

**Request Type**: Extends `ScopedMacroRequestBase` (`side_effects.py:239-242`)

**Shared Infrastructure Usage**:
- Uses `iter_files` for scoped file scanning with max_files limit (`side_effects.py:257`)
- Uses `scope_filter_applied` for summary telemetry (`side_effects.py:402`)
- Uses `macro_scoring_details` with `evidence_kind="resolved_ast"` (`side_effects.py:414`)

#### Data Structures

**SideEffect** (`side_effects.py:73-92`):
```python
class SideEffect(msgspec.Struct):
    file: str
    line: int
    kind: str                # "top_level_call", "global_write", "ambient_read"
    description: str
```

#### Algorithm

**Visitor Pattern** (`side_effects.py:135-236`):

```python
class SideEffectVisitor(ast.NodeVisitor):
    # Track nesting depth (_in_def) and main guard status

    def visit_If(self, node):
        # Skip if __name__ == "__main__": blocks
        if _is_main_guard(node):
            self._in_main_guard = True

    def visit_Call(self, node):
        # Record top-level calls (skip decorators/type-related)
        if self._in_def == 0 and not self._in_main_guard:
            if callee not in SAFE_TOP_LEVEL:
                self.effects.append(SideEffect(..., kind="top_level_call"))

    def visit_Assign(self, node):
        # Detect dict/list mutations at module level
        if isinstance(target, ast.Subscript):
            self.effects.append(SideEffect(..., kind="global_write"))

    def visit_Attribute(self, node):
        # Detect ambient state reads (os.environ, sys.argv, Path.cwd)
        for pattern, category in AMBIENT_PATTERNS.items():
            if pattern in full:
                self.effects.append(SideEffect(..., kind="ambient_read"))
```

**Known Safe Patterns** (`side_effects.py:42-70`):
- Type annotations: TypeVar, Generic, Protocol
- Dataclass decorators: dataclass, field, NamedTuple
- ABC patterns: abstractmethod, property, staticmethod
- Typing annotations: overload, final, runtime_checkable

**Scoring**:
- ImpactSignals: sites (effects), files
- ConfidenceSignals: "resolved_ast"

**Output Sections**:
- Top Level Calls
- Global Writes
- Ambient Reads

### imports: Import Graph and Cycle Detection

**Purpose**: Analyze module import structure, identify cycles, and map dependencies.

**Request Type**: Extends `ScopedMacroRequestBase` (`imports.py:135-139`)

**Shared Infrastructure Usage**:
- Uses `iter_files` for scoped file scanning (`imports.py:285`)
- Uses `scope_filter_applied` for summary telemetry (`imports.py:513`)
- Uses `macro_scoring_details` with max_cycle_len as depth (`imports.py:555`)

#### Data Structures

**ImportInfo** (`imports.py:84-114`):
```python
class ImportInfo(msgspec.Struct):
    file: str
    line: int
    module: str
    names: list[str]        # Imported names (for from imports)
    is_from: bool           # Whether this is a from import
    is_relative: bool       # Whether this is a relative import
    level: int              # Relative import level (dots)
    alias: str | None       # Import alias if present
```

**ModuleDeps** (`imports.py:117-133`):
```python
class ModuleDeps(msgspec.Struct):
    file: str
    imports: list[ImportInfo]
    depends_on: set[str]    # Direct dependencies (module names)
```

#### Algorithm

**1. Relative Import Resolution** (`imports.py:164-191`):
```python
def _resolve_relative_import(importing_file: str, level: int, module: str | None):
    # Go up 'level' directories
    # level 1 = current package
    # level 2 = parent package
    # Resolve to absolute module name
```

**2. Import Visitor** (`imports.py:194-238`):
```python
class ImportVisitor(ast.NodeVisitor):
    def visit_Import(self, node):
        # Record direct import statements
        for alias in node.names:
            self.imports.append(ImportInfo(module=alias.name, ...))

    def visit_ImportFrom(self, node):
        # Record from-import statements
        # Resolve relative imports to absolute
        if is_relative:
            resolved = _resolve_relative_import(self.file, node.level, node.module)
```

**3. Cycle Detection** (`imports.py:244-273`):
```python
def _find_import_cycles(deps: dict[str, ModuleDeps], internal_prefix: str):
    # Build adjacency graph using only internal modules
    graph: dict[str, set[str]] = defaultdict(set)
    for mod_file, mod_deps in deps.items():
        mod_name = _file_to_module(mod_file)
        for dep in mod_deps.depends_on:
            if dep.startswith(internal_prefix):
                graph[mod_name].add(dep)

    # Use rustworkx for strongly connected component detection
    return find_sccs(dict(graph))
```

**Scoring**:
- ImpactSignals: sites (imports), files, max_cycle_len as depth
- ConfidenceSignals: "resolved_ast"

**Output Sections**:
- Import Cycles (if --cycles flag enabled)
- External Dependencies
- Relative Imports
- Module Focus (if --module filter specified)

### exceptions: Exception Handling Pattern Analysis

**Purpose**: Analyze exception handling patterns, identify uncaught exceptions, and map exception propagation.

**Request Type**: Extends `ScopedMacroRequestBase` (`exceptions.py:100-103`)

**Shared Infrastructure Usage**:
- Uses `iter_files` for scoped file scanning (`exceptions.py:284`)
- Uses `scope_filter_applied` for summary telemetry (`exceptions.py:533`)
- Uses `macro_scoring_details` with bare_excepts as breakages (`exceptions.py:542`)

#### Data Structures

**RaiseSite** (`exceptions.py:37-65`):
```python
class RaiseSite(msgspec.Struct):
    file: str
    line: int
    exception_type: str
    in_function: str
    in_class: str | None
    message: str | None      # Exception message if extractable
    is_reraise: bool         # Whether this is a bare raise
```

**CatchSite** (`exceptions.py:67-98`):
```python
class CatchSite(msgspec.Struct):
    file: str
    line: int
    exception_types: list[str]
    in_function: str
    in_class: str | None
    has_handler: bool        # Whether there's actual handling logic
    is_bare_except: bool     # Whether this is a bare except:
    reraises: bool           # Whether the handler re-raises
```

#### Algorithm

**1. Exception Visitor** (`exceptions.py:113-264`):

```python
class ExceptionVisitor(ast.NodeVisitor):
    def visit_Raise(self, node):
        # Bare raise (reraise)
        if node.exc is None:
            self.raises.append(RaiseSite(..., is_reraise=True))
        else:
            exc_type = self._extract_exception_type(node.exc)
            message = self._extract_message(node.exc)
            self.raises.append(RaiseSite(exception_type=exc_type, message=message))

    def visit_Try(self, node):
        for handler in node.handlers:
            # Bare except:
            if handler.type is None:
                exc_types = ["<any>"]
            # Tuple of exceptions
            elif isinstance(handler.type, ast.Tuple):
                exc_types = [self._get_name(elt) for elt in handler.type.elts]
            # Single exception
            else:
                exc_types = [self._get_name(handler.type)]

            # Check for reraise in handler
            reraises = any(isinstance(stmt, ast.Raise) for stmt in handler.body)

            self.catches.append(CatchSite(exception_types=exc_types, reraises=reraises))
```

**2. Uncaught Detection** (`exceptions.py:361-372`):
```python
def _has_matching_catch(raise_site: RaiseSite, catches: list[CatchSite]):
    for caught in catches:
        # Same file and function
        if caught.file != raise_site.file or caught.in_function != raise_site.in_function:
            continue
        # Exact match, bare except, or Exception/BaseException
        if (raise_site.exception_type in caught.exception_types or
            "<any>" in caught.exception_types or
            {"Exception", "BaseException"} & set(caught.exception_types)):
            return True
    return False
```

**Scoring**:
- ImpactSignals: sites (raises), files, bare_excepts as breakages
- ConfidenceSignals: "resolved_ast"

**Output Sections**:
- Raised Exception Types
- Caught Exception Types
- Potentially Uncaught Exceptions
- Bare Except Clauses

## Supporting Infrastructure Deep Dive

### DefIndex: Repository-Wide Symbol Index

**Purpose**: Index all function and class definitions with import alias tracking.

#### Data Structures

**ParamInfo** (`def_index.py:18-38`):
```python
@dataclass
class ParamInfo:
    name: str
    annotation: str | None
    default: str | None
    kind: str  # POSITIONAL_ONLY, POSITIONAL_OR_KEYWORD, VAR_POSITIONAL, ...
```

**FnDecl** (`def_index.py:41-97`):
```python
@dataclass
class FnDecl:
    name: str
    file: str
    line: int
    params: list[ParamInfo]
    is_async: bool
    is_method: bool
    class_name: str | None
    decorators: list[str]

    @property
    def qualified_name(self) -> str:
        return f"{self.class_name}.{self.name}" if self.class_name else self.name

    @property
    def key(self) -> str:
        return f"{self.file}::{self.qualified_name}"
```

**ModuleInfo** (`def_index.py:138-160`):
```python
@dataclass
class ModuleInfo:
    file: str
    functions: list[FnDecl]
    classes: list[ClassDecl]
    module_aliases: dict[str, str]       # alias -> module (e.g., "np" -> "numpy")
    symbol_aliases: dict[str, tuple[str, str]]  # alias -> (module, name)
```

#### Build Process

**DefIndex.build** (`def_index.py:427-483`):
1. Iterate source files via glob patterns (`**/*.py`)
2. Exclude patterns: `.*`, `__pycache__`, `node_modules`, `venv`, `build`, `dist`
3. Parse each file into AST
4. Visit with DefIndexVisitor to extract definitions
5. Store ModuleInfo in modules dict (keyed by relative path)

**DefIndexVisitor** (`def_index.py:293-409`):
- Visit `Import`: Record module_aliases (e.g., `import numpy as np`)
- Visit `ImportFrom`: Record symbol_aliases (e.g., `from pathlib import Path`)
- Visit `FunctionDef`/`AsyncFunctionDef`: Extract params, decorators, method detection
- Visit `ClassDef`: Extract bases, decorators, recursively visit methods

#### Lookup Methods

- `find_function_by_name(name)`: All functions with given name
- `find_function_by_qualified_name(qname)`: Class.method or function
- `find_class_by_name(name)`: All classes with given name
- `resolve_import_alias(file, name)`: Resolve alias to (module, symbol)

**No Caching**: DefIndex is rebuilt on every invocation. No serialization or persistence.

### CallResolver: Call Target Resolution

**Purpose**: Link call expressions to FnDecl targets with confidence scoring.

#### Resolution Strategies

**resolve_call_targets** (`call_resolver.py:255-316`):

**1. Simple Call** (not method call):
```python
# Resolve import alias
alias_result = index.resolve_import_alias(file, callee_name)
if alias_result:
    targets = find_function_by_name(symbol)
    path = f"import:{module}.{symbol}"
else:
    targets = find_function_by_name(callee_name)
    path = f"direct:{callee_name}"

confidence = "exact" if len(targets) == 1 else "ambiguous" if targets else "unresolved"
```

**2. Self/Cls Method Call**:
```python
# Get ModuleInfo for file
module_info = index.get_module_info(file)
targets = [method for cls in module_info.classes for method in cls.methods if method.name == callee_name]
confidence = "exact" if len(targets) == 1 else "likely"
path = f"self:{callee_name}" or f"cls:{callee_name}"
```

**3. Qualified Call** (e.g., `Class.method`):
```python
# Split on first dot
prefix, name = callee_name.split(".", 1)
# Find classes with prefix, then methods with name
targets = [method for cls in index.find_class_by_name(prefix) if method.name == name]
if targets:
    confidence = "exact" if len(targets) == 1 else "ambiguous"
    path = f"class:{prefix}.{name}"
else:
    # Try import alias resolution
    alias_result = index.resolve_import_alias(file, prefix)
    targets = find_function_by_name(name)
    confidence = "exact" if len(targets) == 1 else "likely"
    path = f"module:{module}.{symbol}"
```

**4. Typed Receiver** (with var_types hint):
```python
# Lookup receiver in var_types dict
type_name = var_types.get(receiver_name)
targets = [method for cls in index.find_class_by_name(type_name) if method.name == method_name]
confidence = "exact" if len(targets) == 1 else "likely"
path = f"typed:{type_name}.{method_name}"
```

### ArgBinder: Argument-Parameter Binding

**Purpose**: Map call-site arguments to callee parameter names for taint propagation.

#### Algorithm

**bind_call_to_params** (`arg_binder.py:260-305`):

**1. Categorize Parameters**:
```python
positional_params = [p for p in params if p.kind in {POSITIONAL_ONLY, POSITIONAL_OR_KEYWORD}]
keyword_only_params = [p for p in params if p.kind == KEYWORD_ONLY]
var_positional = [p for p in params if p.kind == VAR_POSITIONAL][0] if ... else None
var_keyword = [p for p in params if p.kind == VAR_KEYWORD][0] if ... else None
param_offset = 1 if is_method else 0  # Skip self/cls
```

**2. Bind Positional Args** (`arg_binder.py:130-161`):
```python
for i, arg in enumerate(args):
    if isinstance(arg, ast.Starred):
        starred_args.append(arg)
    elif i < len(positional_params):
        param = positional_params[i + param_offset]
        bindings.append(Binding(arg_index=i, param_name=param.name))
    elif var_positional:
        bindings.append(Binding(arg_index=i, param_name=var_positional.name, is_starred=True))
    else:
        unbound_args.append(arg)
```

**3. Bind Starred Args** (`arg_binder.py:164-176`):
```python
for arg in starred_args:
    if var_positional:
        bindings.append(Binding(arg_value=ast.unparse(arg), param_name=var_positional.name))
    else:
        unbound_args.append(arg)
```

**4. Bind Keyword Args** (`arg_binder.py:190-228`):
```python
for kw in keywords:
    if kw.arg is None:  # **kwargs
        double_starred.append(kw)
    else:
        param = param_by_name.get(kw.arg)
        if param and kw.arg not in used_params:
            bindings.append(Binding(arg_value=kw.arg, param_name=kw.arg))
            used_params.add(kw.arg)
        elif var_keyword:
            bindings.append(Binding(arg_value=kw.arg, param_name=var_keyword.name, is_starred=True))
        else:
            unbound_args.append(kw)
```

**5. Bind Double-Starred Args** (`arg_binder.py:231-246`):
```python
for kw in double_starred:
    if var_keyword:
        bindings.append(Binding(arg_value=ast.unparse(kw.value), param_name=var_keyword.name))
    else:
        unbound_args.append(kw)
```

**6. Collect Unbound Params** (`arg_binder.py:249-257`):
```python
for param in positional_params[param_offset:]:
    if param.default is None and param.name not in used_params:
        unbound_params.append(param.name)
for param in keyword_only_params:
    if param.default is None and param.name not in used_params:
        unbound_params.append(param.name)
```

#### Taint Propagation

**tainted_params_from_bound_call** (`arg_binder.py:308-336`):
```python
def tainted_params_from_bound_call(bound: BindResult, tainted_args: set[int | str]):
    tainted_params: set[str] = set()
    for binding in bound.bindings:
        # Check if arg_index in tainted_args
        if binding.arg_index is not None and binding.arg_index in tainted_args:
            tainted_params.add(binding.param_name)
        # Check if arg_value in tainted_args
        if binding.arg_value is not None and binding.arg_value in tainted_args:
            tainted_params.add(binding.param_name)
    return tainted_params
```

### Scoring System

#### Impact Score Calculation

**impact_score** (`scoring.py:90-138`):
```python
norm_sites = min(sites / 100, 1.0)
norm_files = min(files / 20, 1.0)
norm_depth = min(depth / 10, 1.0)
norm_breakages = min(breakages / 10, 1.0)
norm_ambiguities = min(ambiguities / 10, 1.0)

score = (
    0.45 * norm_sites +
    0.25 * norm_files +
    0.15 * norm_depth +
    0.10 * norm_breakages +
    0.05 * norm_ambiguities
)

if severity == "error":
    score *= 1.5
elif severity == "info":
    score *= 0.5

return clamp(score, 0.0, 1.0)
```

#### Confidence Score Mapping

**confidence_score** (`scoring.py:141-154`):
- Fixed mapping from evidence_kind to score
- No parameterization or dynamic adjustment
- Default to 0.30 for unknown evidence kinds

#### Bucket Classification

**bucket** (`scoring.py:157-174`):
- Simple threshold-based classification
- No hysteresis or fuzzy boundaries

#### Insight Card Risk Scoring

The `calls` command uses a separate risk scoring model in the Insight Card (`risk_from_counters` in `front_door_insight.py`). This is explicit driver/counter based computation rather than the general `ImpactSignals` model:

**Drivers**: Explicit conditions
- `high_call_surface` (callers >= 10)
- `medium_call_surface` (callers >= 4)
- `argument_forwarding` (forwarding_count > 0)
- `dynamic_hazards` (hazard_count > 0)
- `arg_shape_variance` (arg_shape_count > threshold)
- `closure_capture` (closure_capture_count > 0)

**Risk Level**: Deterministic thresholds
- high: callers > 10 OR hazards > 0 OR (forwarding > 0 AND callers > 0)
- med: callers > 3 OR arg_shape_count > 3 OR files_with_calls > 3 OR closure_capture_count > 0
- low: otherwise

**Counters**: Explicit metrics
- callers, callees, files_with_calls, arg_shape_count
- forwarding_count, hazard_count, closure_capture_count

This separate risk model provides more transparent, actionable risk assessment for the front-door card compared to the normalized `ImpactSignals` scoring used for overall impact ranking.

## Architectural Observations for Improvement Proposals

### 1. Index Construction Strategy

**Current Design**:
- DefIndex rebuilds on every invocation (within-command state only)
- Persistent caching implemented for calls target metadata via DiskCache layer
- Higher-level result caching reduces redundant work across invocations
- Full repo scan still occurs per invocation for DefIndex construction

**Tensions**:
- Simplicity: No cache invalidation complexity for DefIndex itself
- Performance: Persistent cache layer partially addresses repeated scan overhead
- Accuracy: Always fresh DefIndex, cached results with TTL (900s)

**Improvement Vectors**:
- Session-scoped DefIndex caching: Share index across chain/run commands (partially addressed by result cache)
- Incremental indexing: File-level invalidation with timestamps
- Persistent DefIndex with invalidation: On-disk cache with mtime checks (infrastructure exists, not yet applied to DefIndex)

### 2. Taint Propagation Depth Control

**Current Design**:
- Fixed max_depth (default: 5)
- No adaptive depth based on complexity
- No cycle detection per call chain

**Tensions**:
- Completeness: Deep propagation finds distant impacts
- Performance: Deep recursion is expensive
- Precision: Deeper = more false positives

**Improvement Vectors**:
- Adaptive depth: Stop early if no new taint sites
- Complexity-based depth: Reduce for high-fanout functions
- Cycle-aware depth: Track per-path depth, not global depth

### 3. Confidence Scoring Precision

**Current Design**:
- Fixed evidence_kind → score mapping
- No adjustment for resolution path quality
- No aggregation for multiple evidence sources

**Tensions**:
- Simplicity: Easy to understand and debug
- Precision: Coarse buckets miss nuanced differences
- Calibration: Fixed weights may not match reality

**Improvement Vectors**:
- Multi-factor confidence: Combine resolution_path, hazards, enrichment
- Calibration feedback: Track prediction accuracy over time
- Contextual confidence: Adjust for code complexity, test coverage

### 4. Context Window Extraction Heuristics

**Current Design**:
- Indentation-based def detection
- Fixed max 30 lines
- Docstring skipping via regex

**Tensions**:
- Robustness: Indentation heuristics fail on unusual formatting
- Relevance: Fixed budget may omit critical context
- Parsing: Regex docstring detection is brittle

**Improvement Vectors**:
- AST-based bounds: Use actual node.end_lineno
- Adaptive budget: Expand for small functions, contract for large
- Semantic selection: Prioritize lines with tainted variables, call sites

### 5. Hazard Detection Coverage

**Current Design**:
- Hardcoded hazard list (getattr, eval, etc.)
- No severity classification
- No dataflow tracking for hazard arguments

**Tensions**:
- Completeness: Missing hazards (e.g., `__getattribute__`, type(), compile())
- Precision: All hazards treated equally
- Actionability: No guidance on mitigation

**Improvement Vectors**:
- Extensible hazard registry: Configurable hazard patterns
- Severity classification: Critical (eval) vs. medium (getattr) vs. low (partial)
- Dataflow tracking: Trace hazard arguments to constants

### 6. Signature Change Classification Precision

**Current Design**:
- Binary classification: would_break, ambiguous, ok
- No partial compatibility scoring
- No migration guidance

**Tensions**:
- Clarity: Simple buckets are easy to interpret
- Precision: Misses gradations (e.g., "would break in rare edge case")
- Actionability: No concrete migration steps

**Improvement Vectors**:
- Compatibility score: 0.0 (definitely breaks) to 1.0 (fully compatible)
- Edge case detection: Identify calls that break only with *args/**kwargs
- Migration suggestions: Generate code edits for simple fixes

### 7. Cross-Command Integration

**Current Design**:
- Each command is independent
- No shared state or results
- Manual chaining via `cq run` or `cq chain`

**Tensions**:
- Simplicity: Commands are self-contained
- Efficiency: Repeated work (e.g., DefIndex builds)
- Composability: No typed interfaces for command pipelines

**Improvement Vectors**:
- Shared context: Pass DefIndex, enrichment data between commands
- Pipeline API: Type-safe command composition
- Result reuse: Cache and reuse call sites, taint sites across commands

### 8. Bytecode Analysis Depth

**Current Design**:
- Opcode-level analysis only
- No control flow graph reconstruction
- No stack effect tracking

**Tensions**:
- Simplicity: Opcode inspection is straightforward
- Precision: Missing indirect calls, exception handlers
- Performance: CFG reconstruction is expensive

**Improvement Vectors**:
- CFG construction: Identify basic blocks, branches, exception handlers
- Stack effect tracking: Understand argument passing in complex calls
- Selective depth: Full analysis only for high-impact functions

### 9. Enrichment Uniformity

**Current Design**:
- Symtable and bytecode enrichment only in calls.py
- No enrichment in impact.py, scopes.py, etc.
- Inconsistent enrichment across commands

**Tensions**:
- Performance: Enrichment is expensive
- Consistency: Users expect similar detail across commands
- Utility: Some enrichment (e.g., closures) is broadly useful

**Improvement Vectors**:
- Shared enrichment layer: Extract to `tools/cq/enrichment/`
- Lazy enrichment: Compute on-demand per finding
- Enrichment registry: Commands declare enrichment needs

### 10. Fallback Strategy Transparency

**Current Design**:
- Silent fallback to ripgrep on ast-grep failure
- Fallback status tracked in used_fallback flag
- Affects confidence scoring but not prominently reported

**Tensions**:
- Reliability: Graceful degradation is user-friendly
- Transparency: Users may not know they got lower-quality results
- Debugging: Hard to diagnose ast-grep failures

**Improvement Vectors**:
- Explicit fallback reporting: Key finding or warning
- Fallback diagnostics: Capture ast-grep error messages
- Configurable fallback: Allow disabling fallback for strict mode

### 11. Calls Target Cache Management

**Current Design** (`calls_target.py:680-731`):
- DiskCache backend with 900s TTL
- Snapshot digest validation for cache hits
- Run-id tagging for bulk eviction
- Cache key includes language, target, and preview_limit

**Tensions**:
- Performance: Reduces redundant DefIndex scans across invocations
- Freshness: Snapshot validation ensures cache invalidation on file changes
- Complexity: Cache layer adds state and potential failure modes

**Improvement Vectors**:
- Adaptive TTL: Shorter for frequently-changing files
- Cache warming: Preemptive resolution for common targets
- Metrics collection: Track cache hit rate and eviction frequency

### 12. Scope Filter Coverage

**Current Design** (`scope_filters.py:20-48`):
- Shared `resolve_macro_files` with include/exclude globs
- Used by side-effects, imports, exceptions macros
- Not used by calls, impact, sig-impact (no scope filtering)

**Tensions**:
- Consistency: Some macros support scope filtering, others don't
- Discoverability: Users may expect --include/--exclude everywhere
- Performance: Scope filtering reduces scan surface

**Improvement Vectors**:
- Uniform scope filtering: Add to all macros that scan multiple files
- Scope presets: Predefined filters like --scope=tests or --scope=src
- Scope validation: Warn when filters exclude all files

## Shared Infrastructure Benefits

The refactoring to shared macro infrastructure provides several architectural improvements:

**Code Deduplication**:
- Eliminated ~200 lines of duplicated request handling code across 7 macros
- Centralized target resolution logic in single authoritative implementation
- Unified scoring payload construction with consistent signal mapping

**Consistency Guarantees**:
- All macros inherit standard `tc`, `root`, `argv` contract
- Scope-filtered macros share identical `include`/`exclude` semantics
- Scoring details use consistent `ImpactSignals`/`ConfidenceSignals` mapping

**Maintenance Simplification**:
- Request contract changes propagate automatically to all macros
- Target resolution bug fixes apply to all consumers
- Scoring formula adjustments centralized in single module

**Extensibility**:
- New macros can extend `MacroRequestBase` or `ScopedMacroRequestBase`
- Re-export package (`macros/common/`) provides clean import surface
- Future request fields (e.g., timeout, cache_policy) added in single location

**Migration Path**:
- Remaining macros (`calls`, `calls_target`) can adopt shared infrastructure incrementally
- No breaking changes to CLI or run-plan interfaces
- Backward-compatible with existing request payloads

## Design Tensions Summary

| Tension | Current Choice | Alternative | Tradeoff |
|---------|---------------|-------------|----------|
| Index caching | Persistent result cache only | Session/persistent DefIndex | Simplicity vs. performance |
| Taint depth | Fixed max | Adaptive | Predictability vs. precision |
| Confidence scoring | Fixed mapping | Multi-factor | Simplicity vs. accuracy |
| Context extraction | Heuristic | AST-based | Performance vs. robustness |
| Hazard detection | Hardcoded | Registry | Stability vs. extensibility |
| Signature classification | Binary | Continuous | Clarity vs. precision |
| Command integration | Independent | Pipeline | Simplicity vs. efficiency |
| Bytecode analysis | Opcode-level | CFG | Performance vs. depth |
| Enrichment | Command-specific | Shared layer | Targeted vs. consistent |
| Fallback strategy | Silent | Explicit | Reliability vs. transparency |
| Scope filtering | Macro-specific | Uniform | Simplicity vs. consistency |
| Target resolution | Cached with snapshot | Always fresh | Performance vs. correctness |

All design choices reflect deliberate tradeoffs favoring simplicity, reliability, and predictability over maximum precision or extensibility. Proposals should explicitly address how they shift these tradeoffs and what new failure modes they introduce.

## Cross-References

- **Doc 06 (Front Door Insight)**: FrontDoorInsightV1 contract structure, risk computation model
- **Doc 07 (Tree-Sitter)**: Structural neighborhood assembly for Insight Card neighborhood slices
- **Doc 10 (Runtime Services)**: DiskCache infrastructure, worker pool scheduling, cache eviction
