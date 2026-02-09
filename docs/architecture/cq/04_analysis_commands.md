# Analysis Commands Architecture

## Overview

CQ analysis commands provide impact analysis, call site census, scope capture analysis, bytecode inspection, and signature change simulation. The macro system operates at the intersection of static AST analysis, bytecode inspection, symtable introspection, and heuristic search fallbacks.

This document targets advanced LLM programmers seeking to propose architectural improvements. All line references are stable as of 2026-02-09.

## Module Map

**Core Analysis Commands** (`tools/cq/macros/`):
- `calls.py` (1429 lines) - Call site census with argument shape analysis
- `impact.py` (901 lines) - Taint/data flow propagation
- `sig_impact.py` (446 lines) - Signature change impact simulation
- `scopes.py` (414 lines) - Closure/scope capture analysis
- `bytecode.py` (494 lines) - Bytecode surface analysis
- `side_effects.py` (264 lines) - Import-time side effect detection
- `imports.py` (449 lines) - Import structure and cycle analysis
- `exceptions.py` (478 lines) - Exception handling pattern analysis

**Supporting Infrastructure**:
- `cli_app/commands/analysis.py` (361 lines) - CLI dispatch layer
- `index/def_index.py` (676 lines) - DefIndex: function/class definition index
- `index/call_resolver.py` (189 lines) - Call expression → FnDecl resolution
- `index/arg_binder.py` (242 lines) - Argument → parameter binding
- `core/scoring.py` (200 lines) - Impact and confidence scoring

## Architecture Patterns

### 1. Two-Stage Collection

All analysis commands use a fast pre-filter stage followed by precise AST parsing:

1. **Fast Scan**: ripgrep or ast-grep identifies candidate files
2. **Precise Parse**: AST parsing only on candidates

**Example** (`calls.py:1159-1216`):
```python
def _scan_call_sites(root_path: Path, function_name: str) -> CallScanResult:
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

Both calls and impact use `ast.NodeVisitor` subclasses:

**CallFinder** (`calls.py:722-762`):
- Visits all `ast.Call` nodes
- Matches target expression by name/qualified name
- Analyzes argument shape (positional, keyword, starred)
- Detects hazards (star_args, dynamic dispatch, eval/exec)

**TaintVisitor** (`impact.py:124-269`):
- Tracks tainted variable set
- Propagates taint through assignments, calls, returns
- Records taint sites for each propagation
- Collects call sites with tainted arguments for inter-procedural analysis

### 3. Cross-Module Integration

**DefIndex** (`def_index.py:412-676`):
- Repository-wide symbol index (functions, classes, methods)
- Import alias tracking (module and symbol imports)
- Built on-demand via full repo scan (no caching)
- Supports both simple name and qualified name lookup

**CallResolver** (`call_resolver.py:1-352`):
- Resolves call expressions to FnDecl targets
- Handles: simple calls, self/cls method calls, qualified calls, typed receiver calls
- Returns confidence: "exact", "likely", "ambiguous", "unresolved"

**ArgBinder** (`arg_binder.py:1-360`):
- Maps call-site arguments to callee parameter names
- Handles positional, keyword, *args, **kwargs
- Tracks unbound args and params (missing required params)

### 4. Scoring System

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

**Purpose**: Find all call sites for a function with argument shape analysis and hazard detection.

**Data Structures**:

**CallSite** (`calls.py:65-137`):
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

**Algorithm Pipeline**:

1. **Candidate Scan** (`_scan_call_sites`, lines 1159-1216):
   - Use ripgrep for fast file-level filtering: `\b{function_name}\s*\(`
   - Try ast-grep on candidate files for structural matching
   - Fall back to ripgrep if ast-grep fails
   - Track fallback status for confidence scoring

2. **Call Site Collection** (`_collect_call_sites`, lines 810-863):
   - Parse candidate files into AST
   - Visit all `ast.Call` nodes with CallFinder visitor
   - Match target expression by name or qualified name
   - Extract argument shape: positional, keyword, *args, **kwargs

3. **Argument Analysis** (`_analyze_call`, lines 220-262):
   - Count positional args (non-starred)
   - Count keyword args (non-**kwargs)
   - Detect star_args (`ast.Starred` in args)
   - Detect star_kwargs (`kw.arg is None` in keywords)
   - Build arg preview (truncate to 20 chars per arg, max 3 args + 2 kwargs)

4. **Hazard Detection** (`_detect_hazards`, lines 382-406):
   - star_args/star_kwargs forwarding
   - Dynamic dispatch: getattr, setattr, delattr, hasattr
   - Code execution: eval, exec, __import__
   - Introspection: globals(), locals(), vars()
   - Dynamic imports: importlib.import_module
   - Higher-order functions: operator.attrgetter/methodcaller, functools.partial

5. **Enrichment** (`_enrich_call_site`, lines 409-463):
   - **Symtable analysis**: Extract free_vars, globals_used, nested_scopes
   - **Bytecode analysis**: Extract LOAD_GLOBALS, LOAD_ATTRS, CALL_FUNCTIONS opcodes
   - Best-effort compilation (silently skip on syntax errors)

6. **Context Window Extraction** (`_compute_context_window`, lines 466-509):
   - Find containing def by indentation hierarchy
   - Compute start_line/end_line for containing function
   - Extract source snippet with intelligent truncation (max 30 lines)
   - Skip docstrings when extracting snippet

**Context Snippet Algorithm** (`_extract_context_snippet`, lines 512-719):
- Collect function header indices (def line + body-level statements)
- Skip docstrings via triple-quote detection
- Collect anchor block indices (match line + enclosing block)
- Select mandatory indices (header + anchor)
- Fill remaining budget with surrounding lines (max 30 total)
- Render with omission markers for skipped lines

**Scoring** (`_build_call_scoring`, lines 1219-1260):
- ImpactSignals: sites, files, forwarding_count as ambiguities
- ConfidenceSignals:
  - "rg_only" if fallback to ripgrep
  - "resolved_ast_closure" if closures detected
  - "resolved_ast_heuristic" if hazards detected
  - "resolved_ast_bytecode" if bytecode analysis succeeded
  - "resolved_ast" otherwise

**Output Sections**:
- Key findings: Total calls, forwarding warnings
- Argument Shape Histogram: Distribution of (args, kwargs, *, **)
- Keyword Argument Usage: Most common keyword args
- Calling Contexts: Most common containing functions
- Hazards: Dynamic dispatch and execution patterns
- Call Sites: Full list with context snippets

### impact: Taint/Data Flow Analysis

**Purpose**: Trace how data flows from a specific parameter through function calls.

**Data Structures**:

**TaintedSite** (`impact.py:48-72`):
```python
class TaintedSite(msgspec.Struct):
    file: str
    line: int
    kind: str                # "source", "call", "return", "assign"
    description: str
    param: str | None        # Tainted parameter
    depth: int               # Propagation depth
```

**TaintState** (`impact.py:75-90`):
```python
class TaintState(msgspec.Struct):
    tainted_vars: set[str]          # Currently tainted variables
    tainted_sites: list[TaintedSite] # Recorded taint sites
    visited: set[str]               # Visited function keys (cycle prevention)
```

**Algorithm**:

1. **Index Construction** (`cmd_impact`, lines 882-900):
   - Build DefIndex from repository
   - Resolve function by name or qualified name
   - Filter functions containing target parameter

2. **Intra-Procedural Taint Tracking** (`TaintVisitor`, lines 124-269):
   - Initialize tainted set with target parameter name
   - Visit assignments: propagate taint from RHS to LHS
   - Visit calls: record tainted arguments for inter-procedural analysis
   - Visit returns: record tainted return values

3. **Taint Propagation Handlers** (lines 271-442):
   - Dictionary mapping AST node types to taint checking functions
   - Attribute/Subscript: Taint if sub-expression tainted
   - BinOp/UnaryOp: Taint if operands tainted
   - Call: Taint if any argument or receiver tainted
   - IfExp/Compare: Taint if any branch/operand tainted
   - BoolOp: Taint if any value tainted
   - Lambda: Never tainted (conservative)
   - Comprehensions: Taint if iterating over tainted data

4. **Inter-Procedural Propagation** (`_analyze_function`, lines 499-556):
   - Read source file and find function AST node
   - Run TaintVisitor on function body
   - For each call with tainted args:
     - Resolve call targets via CallResolver
     - Bind arguments to parameters via ArgBinder
     - Propagate taint to callee parameters
     - Recursively analyze callee (up to max_depth)
   - Cycle prevention via visited set

5. **External Callers** (`_find_callers_via_search`, lines 558-592):
   - Use ripgrep to find potential callers
   - Convert absolute paths to relative paths

**Scoring** (`_build_impact_scoring`, lines 770-793):
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

**Data Structures**:

**SigParam** (`sig_impact.py:42-64`):
```python
class SigParam(msgspec.Struct):
    name: str
    has_default: bool
    is_kwonly: bool
    is_vararg: bool          # *args
    is_kwarg: bool           # **kwargs
```

**Algorithm**:

1. **Signature Parsing** (`_parse_signature`, lines 76-156):
   - Extract inside parens via regex
   - Parse as synthetic function def: `def _tmp({inside}): pass`
   - Extract params: positional, *args, keyword-only, **kwargs
   - Track defaults for positional and keyword-only params

2. **Call Site Collection** (`_collect_sites`, lines 217-241):
   - Reuse calls.py infrastructure: `_rg_find_candidates`, `_collect_call_sites`

3. **Call Classification** (`_classify_call`, lines 159-214):
   - **Ambiguous**: Call uses *args or **kwargs
   - **Would Break**:
     - Missing required param (no default, not covered by position or keyword)
     - Too many positional args (no *args in new sig)
     - Unknown keyword arg (not in param names, no **kwargs in new sig)
   - **OK**: Compatible call

4. **Classification Logic**:
   - Build required param list (no defaults, not keyword-only, not variadic)
   - For each required param:
     - Check coverage by positional index
     - Check coverage by keyword arg
   - Check excess positional args
   - Check unknown keyword args

**Scoring**:
- ImpactSignals: sites, files, breakages, ambiguities
- ConfidenceSignals: "resolved_ast" (always, since AST-based)

**Output Sections**:
- Would Break Sites: Calls that would fail (severity: error)
- Ambiguous Sites: Calls needing manual review (severity: warning)
- OK Sites: Compatible calls (severity: info)

### scopes: Closure/Scope Analysis

**Purpose**: Analyze scope capture for closures and nested functions.

**Data Structures**:

**ScopeInfo** (`scopes.py:41-74`):
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

**Algorithm**:

1. **File Resolution** (`_resolve_target_files`, lines 185-203):
   - Check if target is file path (absolute or relative)
   - Otherwise search for files containing `def {target}` or `class {target}`
   - Limit to max_files

2. **Scope Extraction** (`_extract_scopes`, lines 123-170):
   - Build symtable via `symtable.symtable(source, file, "exec")`
   - Recursively walk symtable tree
   - For each symbol:
     - is_free(): Captured from enclosing scope
     - is_cell(): Provided to nested scope
     - is_global(): Global reference
     - is_nonlocal(): Declared nonlocal
     - is_local(): Local variable
   - Filter: Only report functions with captures (free_vars or cell_vars or nonlocals)

**Scoring**:
- ImpactSignals: sites (closures), files
- ConfidenceSignals: "resolved_ast" (always)

**Output Sections**:
- Key findings: Closures, providers, nonlocal users
- Scope Capture Details: Function-by-function breakdown

### bytecode-surface: Bytecode Analysis

**Purpose**: Analyze compiled bytecode for hidden dependencies without execution.

**Data Structures**:

**BytecodeSurface** (`bytecode.py:54-82`):
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

**Algorithm**:

1. **Code Object Walking** (`_walk_code_objects`, lines 95-120):
   - Recursively yield (qualname, CodeType) pairs
   - Walk co_consts for nested code objects
   - Build qualified names via prefix concatenation

2. **Code Object Analysis** (`_analyze_code_object`, lines 123-171):
   - Iterate instructions via `dis.get_instructions(co)`
   - Track opcode frequencies
   - Extract globals: LOAD_GLOBAL, STORE_GLOBAL, DELETE_GLOBAL
   - Extract attributes: LOAD_ATTR, STORE_ATTR, DELETE_ATTR
   - Extract constants: strings (< 100 chars), non-trivial numbers

3. **Surface Collection** (`_collect_surfaces`, lines 212-230):
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

## Supporting Infrastructure Deep Dive

### DefIndex: Repository-Wide Symbol Index

**Purpose**: Index all function and class definitions with import alias tracking.

**Data Structures**:

**ParamInfo** (`def_index.py:18-38`):
```python
@dataclass
class ParamInfo:
    name: str
    annotation: str | None
    default: str | None
    kind: str  # POSITIONAL_ONLY, POSITIONAL_OR_KEYWORD, VAR_POSITIONAL, KEYWORD_ONLY, VAR_KEYWORD
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

**Build Process** (`DefIndex.build`, lines 427-483):
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

**Lookup Methods**:
- `find_function_by_name(name)`: All functions with given name
- `find_function_by_qualified_name(qname)`: Class.method or function
- `find_class_by_name(name)`: All classes with given name
- `resolve_import_alias(file, name)`: Resolve alias to (module, symbol)

**No Caching**: DefIndex is rebuilt on every invocation. No serialization or persistence.

### CallResolver: Call Target Resolution

**Purpose**: Link call expressions to FnDecl targets with confidence scoring.

**Resolution Strategies** (`resolve_call_targets`, lines 255-316):

1. **Simple Call** (not method call):
   - Resolve import alias: `index.resolve_import_alias(file, callee_name)`
   - If alias resolves: `find_function_by_name(symbol)`, path: `import:{module}.{symbol}`
   - Otherwise: `find_function_by_name(callee_name)`, path: `direct:{callee_name}`
   - Confidence: "exact" if 1 target, "ambiguous" if multiple, "unresolved" if none

2. **Self/Cls Method Call**:
   - Get ModuleInfo for file
   - Find methods in classes with matching name
   - Confidence: "exact" if 1 target, "likely" if multiple
   - Path: `self:{callee_name}` or `cls:{callee_name}`

3. **Qualified Call** (e.g., `Class.method`):
   - Split on first dot: prefix.name
   - Find classes with prefix, then methods with name
   - If found: confidence "exact"/"ambiguous", path: `class:{prefix}.{name}`
   - Otherwise resolve import alias and search
   - Confidence: "exact"/"likely", path: `module:{module}.{symbol}`

4. **Typed Receiver** (with var_types hint):
   - Lookup receiver in var_types dict
   - Find methods in class with receiver's type
   - Confidence: "exact"/"likely", path: `typed:{type_name}.{method_name}`

### ArgBinder: Argument-Parameter Binding

**Purpose**: Map call-site arguments to callee parameter names for taint propagation.

**Algorithm** (`bind_call_to_params`, lines 260-305):

1. **Categorize Parameters**:
   - positional_params: POSITIONAL_ONLY, POSITIONAL_OR_KEYWORD
   - keyword_only_params: KEYWORD_ONLY
   - var_positional: VAR_POSITIONAL (*args)
   - var_keyword: VAR_KEYWORD (**kwargs)
   - param_offset: 1 if method (skip self/cls), 0 otherwise

2. **Bind Positional Args** (`_bind_positional_args`, lines 130-161):
   - For each positional arg:
     - If starred: defer to starred_args list
     - If within positional_params range: bind to param at (index + offset)
     - Elif var_positional exists: bind to *args (is_starred=True)
     - Else: add to unbound_args

3. **Bind Starred Args** (`_bind_starred_args`, lines 164-176):
   - For each starred arg:
     - If var_positional exists: bind to *args
     - Else: add to unbound_args

4. **Bind Keyword Args** (`_bind_keyword_args`, lines 190-228):
   - For each keyword arg:
     - If kw.arg is None (**kwargs): defer to double_starred list
     - Lookup param in positional_params or keyword_only_params
     - If found and not already used: bind to param
     - Elif var_keyword exists: bind to **kwargs (is_starred=True)
     - Else: add to unbound_args

5. **Bind Double-Starred Args** (`_bind_double_starred_args`, lines 231-246):
   - For each **kwargs arg:
     - If var_keyword exists: bind to **kwargs
     - Else: add to unbound_args

6. **Collect Unbound Params** (`_collect_unbound_params`, lines 249-257):
   - For positional params (after offset) with no default: add to unbound_params
   - For keyword-only params with no default: add to unbound_params

**Taint Propagation** (`tainted_params_from_bound_call`, lines 308-336):
- For each binding:
  - If arg_index in tainted_args: mark param as tainted
  - If arg_value in tainted_args: mark param as tainted
- Return set of tainted parameter names

### Scoring System

**Impact Score Calculation** (`impact_score`, lines 90-138):
```
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

**Confidence Score Mapping** (`confidence_score`, lines 141-154):
- Fixed mapping from evidence_kind to score
- No parameterization or dynamic adjustment
- Default to 0.30 for unknown evidence kinds

**Bucket Classification** (`bucket`, lines 157-174):
- Simple threshold-based classification
- No hysteresis or fuzzy boundaries

## Architectural Observations for Improvement Proposals

### 1. Index Construction Strategy

**Current Design**:
- DefIndex rebuilds on every invocation
- No caching or persistence
- Full repo scan for every command

**Tensions**:
- Simplicity: No cache invalidation complexity
- Performance: Repeated scans for multi-command workflows
- Accuracy: Always fresh, never stale

**Improvement Vectors**:
- Session-scoped caching: Share index across chain/run commands
- Incremental indexing: File-level invalidation with timestamps
- Persistent index with invalidation: On-disk cache with mtime checks

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

## Design Tensions Summary

| Tension | Current Choice | Alternative | Tradeoff |
|---------|---------------|-------------|----------|
| Index caching | None | Session/persistent | Simplicity vs. performance |
| Taint depth | Fixed max | Adaptive | Predictability vs. precision |
| Confidence scoring | Fixed mapping | Multi-factor | Simplicity vs. accuracy |
| Context extraction | Heuristic | AST-based | Performance vs. robustness |
| Hazard detection | Hardcoded | Registry | Stability vs. extensibility |
| Signature classification | Binary | Continuous | Clarity vs. precision |
| Command integration | Independent | Pipeline | Simplicity vs. efficiency |
| Bytecode analysis | Opcode-level | CFG | Performance vs. depth |
| Enrichment | Command-specific | Shared layer | Targeted vs. consistent |
| Fallback strategy | Silent | Explicit | Reliability vs. transparency |

All design choices reflect deliberate tradeoffs favoring simplicity, reliability, and predictability over maximum precision or extensibility. Proposals should explicitly address how they shift these tradeoffs and what new failure modes they introduce.
