# Query Subsystem Architecture

**Version:** 0.4.0
**Document Status:** Phase 2 (Active)
**Last Updated:** 2026-02-15

## Overview

The CQ query subsystem (`tools/cq/query/`) implements a declarative code query system for Python and Rust codebases. It provides a token-based DSL for expressing semantic code queries that compile to executable plans using ast-grep-py for structural pattern matching.

The subsystem follows a classic compiler architecture with three distinct phases: **parse** (DSL to IR), **compile** (IR to execution plan), and **execute** (plan to results).

## Module Map

| Module | Responsibility | Lines | Key Types |
|--------|---------------|-------|-----------|
| `parser.py` | Query DSL tokenization and parsing to IR | 992 | `parse_query()`, `_tokenize()`, `_EntityQueryState` |
| `ir.py` | Intermediate representation type definitions | 765 | `Query`, `PatternSpec`, `RelationalConstraint`, `CompositeRule` |
| `planner.py` | IR compilation to executable ToolPlan | 658 | `compile_query()`, `ToolPlan`, `AstGrepRule` |
| `executor.py` | Plan execution and result generation | 3235 | `execute_plan()`, `ScanContext`, entity/pattern handlers |
| `entity_front_door.py` | Entity front-door insight assembly | 468 | `attach_entity_front_door_insight()`, `EntitySemanticTelemetry` |
| `enrichment.py` | Symtable/bytecode enrichment | 629 | `SymtableEnricher`, `BytecodeInfo`, `SymtableInfo` |
| `batch.py` | Batch query execution with shared scans | 170 | `BatchEntityQuerySession`, `build_batch_session()` |
| `merge.py` | Multi-language result merging | 205 | `merge_auto_scope_query_results()` |
| `symbol_resolver.py` | Symbol resolution and import tracking | 484 | `SymbolTable`, `SymbolKey`, `ImportBinding` |
| `sg_parser.py` | ast-grep parser and file inventory | 409 | `sg_scan()`, `list_scan_files()`, `FileInventoryCacheV1` |
| `batch_spans.py` | Batch relational span collection | 122 | `collect_span_filters()` |
| `language.py` | Multi-language scope resolution | 310 | `QueryLanguage`, `QueryLanguageScope`, extension mappings |
| `metavar.py` | Metavariable parsing and filtering | 162 | `parse_metavariables()`, `apply_metavar_filters()` |
| `execution_requests.py` | Request payloads for shared execution | 65 | `EntityQueryRequest`, `PatternQueryRequest` |
| `execution_context.py` | Bundled execution context | 26 | `QueryExecutionContext` |

**Total LOC:** ~8,700 lines

## CLI Entry Point

Location: `tools/cq/cli_app/commands/query.py`

The `q()` function handles query execution with smart fallback: plain identifier queries (no `key=value` tokens) fall back to smart_search, while tokenized queries use the parse-compile-execute pipeline. Token detection pattern: `([\w.]+|\$+\w+)=(?:'([^']+)'|"([^"]+)"|([^\s]+))`

## Query DSL Grammar

The query language supports two primary modes: **entity queries** (search by semantic entity type) and **pattern queries** (search by AST structure).

### Token Categories

All queries use `key=value` token pairs. Values may be quoted for strings with spaces.

#### Core Tokens

| Token | Purpose | Example |
|-------|---------|---------|
| `entity=<type>` | Entity type filter | `entity=function`, `entity=class` |
| `name=<pattern>` | Name filter (prefix `~` for regex) | `name=foo`, `name=~^build.*` |
| `pattern='<ast-pattern>'` | Structural pattern match | `pattern='def $F($$$)'` |
| `in=<dir>` | Directory scope | `in=src/` |
| `exclude=<glob>` | Exclusion pattern | `exclude=tests/**` |
| `expand=<kind>(depth=N)` | Graph expansion | `expand=callers(depth=2)` |
| `fields=<f1>,<f2>` | Output fields | `fields=def,callers,imports` |
| `limit=<N>` | Result limit | `limit=10` |
| `explain` | Show plan explanation | `explain=true` |
| `lang=<scope>` | Language scope | `lang=auto`, `lang=python`, `lang=rust` |

#### Relational Constraint Tokens

| Token | Semantics | Example |
|-------|-----------|---------|
| `inside='<pattern>'` | Match inside ancestor matching pattern | `inside='class Config'` |
| `has='<pattern>'` | Match containing descendant matching pattern | `has='await $X'` |
| `precedes='<pattern>'` | Match preceding sibling matching pattern | `precedes='return $X'` |
| `follows='<pattern>'` | Match following sibling matching pattern | `follows='if $COND:'` |

**Modifiers**:
- `<op>.stopBy=<mode>` - When to stop search: `neighbor` (default), `end`, or custom pattern
- `<op>.field=<name>` - Constrain search to specific AST field (only `inside`/`has`)

#### Composite Logic Tokens

| Token | Logic | Example |
|-------|-------|---------|
| `all='<p1>,<p2>'` | AND - all patterns must match | `all='await $X,return $Y'` |
| `any='<p1>,<p2>'` | OR - at least one pattern must match | `any='logger.$M,print'` |
| `not='<pattern>'` | NOT - pattern must not match (single pattern only) | `not='pass'` |

#### Scope Filter Tokens

| Token | Purpose | Example |
|-------|---------|---------|
| `scope=<type>` | Scope type filter | `scope=closure` |
| `captures=<var>` | Filter by captured variables | `captures=x` |
| `has_cells=<bool>` | Filter by cell variable presence | `has_cells=true` |

#### Decorator Filter Tokens

| Token | Purpose | Example |
|-------|---------|---------|
| `decorated_by=<name>` | Filter by decorator presence | `decorated_by=property` |
| `decorator_count_min=<N>` | Minimum decorator count | `decorator_count_min=2` |
| `decorator_count_max=<N>` | Maximum decorator count | `decorator_count_max=5` |

#### Metavariable Filter Tokens

Post-filter pattern matches by captured metavariable content.

Syntax: `$<NAME>=~<regex>` or `$<NAME>!=~<regex>`

Examples:
- `$$OP=~'^[<>=]'` - Binary comparison operators only
- `$X!=~debug` - Exclude matches where `$X` captures "debug"

#### Pattern Disambiguation Tokens

For ambiguous patterns that cannot parse standalone.

| Token | Purpose | Example |
|-------|---------|---------|
| `pattern.context='...'` | Surrounding code for parsing | `pattern.context='{ "$K": $V }'` |
| `pattern.selector=<kind>` | Node kind to extract | `pattern.selector=pair` |
| `pattern.strictness=<mode>` | Matching strictness | `pattern.strictness=relaxed` |

**Strictness modes**:
- `cst` - Match all nodes including whitespace
- `smart` - Match all nodes except trivial source nodes (default)
- `ast` - Match only named AST nodes
- `relaxed` - Match AST nodes, ignore comments
- `signature` - Match structure without comparing text

#### Positional Matching Tokens

| Token | Purpose | Example |
|-------|---------|---------|
| `nthChild=<N>` | Exact position (1-indexed) | `nthChild=2` |
| `nthChild='<formula>'` | Formula (e.g., '2n+1' for odd) | `nthChild='2n'` |
| `nthChild.reverse=<bool>` | Count from end | `nthChild.reverse=true` |
| `nthChild.ofRule='<rule>'` | Filter which siblings count | `nthChild.ofRule='kind=identifier'` |

## Query IR (Intermediate Representation)

Location: `tools/cq/query/ir.py` (765 lines)

The IR layer provides frozen, immutable structs representing parsed queries. All types use `msgspec.Struct` for serialization compatibility and performance.

### Core Types

```python
EntityType = Literal["function", "class", "method", "module", "callsite", "import", "decorator"]
ExpanderKind = Literal["callers", "callees", "imports", "raises", "scope", "bytecode_surface"]
FieldType = Literal["def", "loc", "callers", "callees", "evidence", "imports", "decorators", "decorated_functions"]
StrictnessMode = Literal["cst", "smart", "ast", "relaxed", "signature"]
RelationalOp = Literal["inside", "has", "precedes", "follows"]
CompositeOp = Literal["all", "any", "not"]
MetaVarKind = Literal["single", "multi", "unnamed"]
```

### Key Structs

**Query** - Top-level parsed query
- Must specify either `entity` OR `pattern_spec` (mutually exclusive, validated in `__post_init__`)
- Fields: entity, name, expand, scope, fields, limit, explain, pattern_spec, relational, scope_filter, decorator_filter, joins, metavar_filters, composite, nth_child, lang_scope

**PatternSpec** - Pattern specification with disambiguation
- Fields: pattern, context (for ambiguous patterns), selector (node kind to extract), strictness
- `requires_yaml_rule()` returns True if context, selector, or non-smart strictness present
- `to_yaml_dict()` converts to ast-grep YAML format

**RelationalConstraint** - Structural relationship
- Fields: operator (inside/has/precedes/follows), pattern, stop_by (neighbor/end/custom), field_name (only valid for inside/has)
- `to_ast_grep_dict()` converts to ast-grep rule format

**CompositeRule** - Boolean composition
- Fields: operator (all/any/not), patterns (tuple), metavar_order (optional)
- Validates 'not' has single pattern in `to_ast_grep_dict()`

**Expander** - Graph expansion operator
- Fields: kind (ExpanderKind), depth (≥1, enforced by msgspec.Meta)

**Scope** - File scope constraints
- Fields: in_dir, exclude (tuple), globs (tuple)

**MetaVarCapture** - Captured metavariable
- Fields: name, kind (single/multi/unnamed), text, nodes (list for multi captures)

**MetaVarFilter** - Metavariable post-filter
- Fields: name, pattern (regex), negate (bool)
- `matches(capture)` applies regex filter to capture text with optional negation

## Query Parser

Location: `tools/cq/query/parser.py` (992 lines)

The parser converts query strings to Query IR using a stateful token handler pattern.

### Parsing Pipeline

```python
def parse_query(query_string: str) -> Query:
    tokens = _tokenize(query_string)

    # Dispatch based on query type (pattern vs entity)
    if "pattern" in tokens or "pattern.context" in tokens:
        return _parse_pattern_query(tokens)
    return _parse_entity_query(tokens)
```

### Tokenization

The `_tokenize()` function uses regex to extract key-value pairs:

```python
pattern = r"([\w.]+|\$+\w+)=(?:'([^']+)'|\"([^\"]+)\"|([^\s]+))"
```

This pattern handles:
- Dot-notation keys: `pattern.context`, `inside.stopBy`
- Metavar filter keys: `$NAME`, `$$NAME`, `$$$NAME`
- Single-quoted strings: `'value with spaces'`
- Double-quoted strings: `"another value"`
- Unquoted values: `value`

### State-Based Parsing

Entity queries use `_EntityQueryState` and pattern queries use `_PatternQueryState` to accumulate parsed components.

Token handlers process tokens in defined order:
```python
_ENTITY_HANDLER_ORDER = ("name", "expand", "scope", "fields", "limit", "explain", "lang")
```

Each handler mutates the state object. After all handlers run, the state builds the final `Query` object.

### Expander Parsing

Handles complex expander syntax like `callers(depth=2),callees`:
1. Split by comma, respecting nested parentheses
2. Parse each part: `kind(depth=N)` or just `kind`
3. Validate expander kind and extract depth parameter
4. Build Expander tuple

### Relational Constraint Parsing

Supports both dot notation and global fallbacks:
- Try `{op}.stopBy` first (e.g., `inside.stopBy`)
- Fall back to `{op}_stop_by` (legacy underscore notation)
- Fall back to global `stopBy` token
- Same pattern for `field` modifiers

## Query Planner

Location: `tools/cq/query/planner.py` (658 lines)

The planner compiles Query IR into an executable ToolPlan that specifies which tools to run and how.

### ToolPlan Structure

```python
class ToolPlan(msgspec.Struct, frozen=True):
    scope: Scope
    sg_record_types: frozenset[str]  # e.g., {"def", "call", "import"}
    need_symtable: bool  # For scope/callers/callees analysis
    need_bytecode: bool  # For bytecode_surface expander
    expand_ops: tuple[Expander, ...]
    explain: bool
    sg_rules: tuple[AstGrepRule, ...]
    is_pattern_query: bool
    lang: QueryLanguage
    lang_scope: QueryLanguageScope
```

### AstGrepRule Structure

```python
class AstGrepRule(msgspec.Struct, frozen=True):
    pattern: str
    kind: str | None  # AST node kind constraint
    context: str | None  # Surrounding code for parsing
    selector: str | None  # Node kind to extract
    strictness: StrictnessMode
    inside: str | None  # Relational: inside ancestor
    has: str | None  # Relational: contains descendant
    precedes: str | None  # Relational: precedes sibling
    follows: str | None  # Relational: follows sibling
    inside_stop_by: str | None
    has_stop_by: str | None
    inside_field: str | None
    has_field: str | None
    composite: CompositeRule | None
    nth_child: NthChildSpec | None

    def requires_inline_rule(self) -> bool:
        # True if features beyond simple pattern present

    def to_yaml_dict(self) -> dict[str, object]:
        # Convert to ast-grep YAML format
```

### Compilation Strategy

```python
def compile_query(query: Query) -> ToolPlan:
    if query.is_pattern_query:
        return _compile_pattern_query(query)
    return _compile_entity_query(query)
```

**Entity Query Compilation**:
1. Determine required record types via `_determine_record_types()` - union of:
   - Entity type records (e.g., function → `{"def"}`)
   - Expander records (e.g., callers → `{"def", "call"}`)
   - Field records (e.g., imports → `{"import"}`)
2. Check if symtable needed - True for scope expander, callers/callees expanders, or scope_filter present
3. Check if bytecode needed - True for bytecode_surface expander or evidence field
4. Generate ast-grep rules from relational constraints

**Pattern Query Compilation**:
1. Build AstGrepRule from PatternSpec
2. Apply relational constraints
3. Set empty sg_record_types (not using standard record scanning)
4. Mark `is_pattern_query=True`

### Record Type Inference

```python
_ENTITY_RECORDS: dict[str, set[str]] = {
    "function": {"def"},
    "class": {"def"},
    "method": {"def"},
    "module": {"def"},
    "callsite": {"call"},
    "import": {"import"},
    "decorator": {"def"},
}

_EXPANDER_RECORDS: dict[str, set[str]] = {
    "callers": {"def", "call"},
    "callees": {"def", "call"},
    "imports": {"import"},
    "raises": {"raise", "except"},
    "scope": {"def"},
}
```

### Multi-Language Rule Generation

For Rust queries, `_rust_entity_to_ast_grep_rules()` generates language-specific rules:

```python
if entity == "class":
    # Rust types map to struct/enum/trait
    type_kinds = ("struct_item", "enum_item", "trait_item")
    return tuple(
        _apply_relational_constraints(AstGrepRule(pattern="$TYPE", kind=kind), query.relational)
        for kind in type_kinds
    )

if entity == "method":
    # Rust methods are function_item nodes inside impl blocks
    base = AstGrepRule(pattern="$METHOD", kind="function_item", inside="impl $TYPE { $$$ }")
    return (_apply_relational_constraints(base, query.relational),)
```

### Relational Constraint Compilation

Merges relational constraints into AstGrepRule:
1. Build `_RelationalState` with current rule constraints
2. Apply each constraint via `_apply_relational_constraint()`
3. Merge state back into rule via `_merge_relational_state()`

Heuristic: For class/function `inside` constraints, use `stop_by="end"` to search to root rather than stopping at nearest neighbor.

## Query Executor

Location: `tools/cq/query/executor.py` (3235 lines)

The executor implements the runtime that executes ToolPlans and produces CqResult objects. It handles two execution paths: entity queries and pattern queries.

### Execution Entry Point

```python
def execute_plan(
    plan: ToolPlan,
    query: Query,
    tc: Toolchain,
    root: Path,
    argv: list[str] | None = None,
    query_text: str | None = None,
) -> CqResult:
    if query.lang_scope == "auto":
        # Multi-language execution with result merging
        return _execute_auto_scope_plan(query, tc=tc, root=root, argv=argv or [], query_text=query_text)

    # Single-language execution
    ctx = QueryExecutionContext(plan=plan, query=query, tc=tc, root=root, argv=argv or [], started_ms=ms(), query_text=query_text)
    return _execute_single_context(ctx)
```

### Entity Query Execution Flow

**Preparation Phase** (`_prepare_entity_state()`):
1. Validate ast-grep availability via toolchain
2. Resolve paths from scope constraints via `scope_to_paths()`
3. Scan records via `sg_scan()` - runs ast-grep for required record types
4. Build scan context via `_build_scan_context()` - creates interval index, assigns calls to defs
5. Build entity candidates via `_build_entity_candidates()` - partitions by record type
6. Apply rule spans via `_apply_rule_spans()` - filters candidates using relational constraint matches

**Execution Phase** (`_execute_entity_query()`):
1. Build result structure with RunMeta
2. Apply entity-specific handlers via `_apply_entity_handlers()`
3. Add file scan statistics
4. Add explain metadata if requested
5. Finalize multi-language summary if needed

### ScanContext Structure

```python
@dataclass
class ScanContext:
    def_records: list[SgRecord]
    call_records: list[SgRecord]
    interval_index: IntervalIndex[SgRecord]  # Spatial index for containment queries
    file_index: FileIntervalIndex  # Per-file interval indices
    calls_by_def: dict[SgRecord, list[SgRecord]]  # Call assignment
    all_records: list[SgRecord]
```

The interval indices enable O(log n) containment queries for finding which definition contains a given call or other record.

### Entity Handler Dispatch

```python
if query.entity == "import":
    _process_import_query(candidates.import_records, query, result, root, symtable=symtable)
elif query.entity == "decorator":
    _process_decorator_query(state.scan, query, result, root, candidates.def_records)
elif query.entity == "callsite":
    _process_call_query(state.scan, query, result, root)
else:
    # function, class, method, module
    _process_def_query(def_ctx, query, candidates.def_records)
```

### Definition Query Processing

The `_process_def_query()` function implements the core definition handler:

1. Filter candidates by name pattern via `_filter_to_matching()`
2. Convert records to findings via `_def_to_finding()`
3. Apply scope filter if present (closure detection)
4. Build sections for requested fields:
   - `callers` section: Find all calls to these definitions
   - `callees` section: Extract calls within these definitions
   - `imports` section: Show imports in files containing these definitions
5. Append expander sections (raises, scope, bytecode_surface)

### Pattern Query Execution Flow

**Preparation Phase** (`_prepare_pattern_state()`):
1. Validate ast-grep availability
2. Resolve paths from scope
3. Tabulate files via `_tabulate_scope_files()` - applies extension filtering and glob rules
4. Return error if no files match

**Execution Phase** (`_execute_pattern_query()`):
1. Execute ast-grep rules via `_execute_ast_grep_rules()`
2. Build result with findings
3. Apply scope filter if present (symtable enrichment)
4. Apply limit if specified
5. Add file scan statistics and explain metadata

### AST-Grep Rule Execution

The `_execute_ast_grep_rules()` function uses ast-grep-py directly:
1. Create execution context with rules, paths, root, query, language
2. For each file path:
   - Read source text
   - Parse with SgRoot
   - For each rule:
     - Iterate matches via `_iter_rule_matches()`
     - Build match data with rule_id and relative path
     - Apply filters (scope, metavar)
     - Convert to finding and record
     - Apply metavar details
     - Append to results
3. Return findings, records, raw matches

### Multi-Language Execution

The `_execute_auto_scope_plan()` function orchestrates parallel language execution:
1. Execute by language scope via `execute_by_language_scope()`
2. Each language runs independently with scoped queries
3. Results merged via `merge_language_cq_results()` with cross-language diagnostics

### Relational Span Collection

For entity queries with relational constraints, `_collect_match_spans()` pre-computes spans:

1. Tabulate files for language scope
2. Collect ast-grep match spans via `_collect_ast_grep_match_spans()`
3. Filter by metavariable constraints if present
4. Group by file

The `_filter_records_by_spans()` function then filters entity candidates to those overlapping matched spans.

### Caller/Callee Analysis

**Callers** (`_build_callers_section()`):
1. Build target context via `_build_call_target_context()`:
   - Extract target names from definitions
   - Classify as function vs method (based on enclosing class)
   - Build class-to-method mapping for qualified lookup
2. Collect call contexts via `_collect_call_contexts()`:
   - Iterate all call records
   - Extract call target and receiver
   - Check if call matches target (handles `self.method()` vs `func()` disambiguation)
3. Build evidence map via `_build_def_evidence_map()`:
   - Enrich containing definitions with symtable/bytecode info
4. Build caller findings with evidence attachment

**Callees** (`_build_callees_section()`):
1. Iterate target definitions
2. Extract calls within each definition (from `calls_by_def` mapping)
3. Build callee findings with evidence

### Scope Enrichment

The `_build_scope_section()` function provides closure analysis using SymtableEnricher:
- Enrich each function finding with scope info
- Extract free_vars, cell_vars from symtable analysis
- Label as "closure" or "toplevel" based on `is_closure` flag
- Attach scope details to findings

### Bytecode Surface Analysis

The `_build_bytecode_surface_section()` function extracts bytecode patterns:
- Enrich target definitions with bytecode info via `enrich_records()`
- Extract load_globals, load_attrs, call_functions from BytecodeInfo
- Build findings with bytecode details attached

## Entity Front-Door Insight Assembly

Location: `tools/cq/query/entity_front_door.py` (468 lines)

The entity front-door module builds and attaches `FrontDoorInsightV1` cards to entity query results, providing target identity, neighborhood preview, risk assessment, and confidence scoring.

### Purpose

Entity queries need a structured summary that captures:
- **Target Identity** - What was found (definitions, primary candidate)
- **Neighborhood Context** - Immediate relationships (callers, callees, scope)
- **Risk Indicators** - Complexity signals (relationship counts, closure captures)
- **Confidence Metrics** - Evidence quality and completeness
- **Degradation Signals** - LSP/semantic availability, budget constraints, language gaps

The front-door insight provides this in a canonical `FrontDoorInsightV1` contract that downstream tooling (formatters, UIs) can consume without re-parsing entity-specific fields.

### Integration Point

The executor calls `attach_entity_front_door_insight()` after entity query execution completes:

```python
# In executor.py, after building entity result
if not query.is_pattern_query:
    attach_entity_front_door_insight(
        result,
        relationship_detail_max_matches=RELATIONSHIP_DETAIL_MAX_MATCHES,
    )
```

### Integration Flow

1. Extract definition findings from `result.key_findings`
2. Build candidate neighborhood from top 3 definitions via `_build_candidate_neighborhood()`
3. Compute risk from neighborhood counters (callers, callees, scope) via `risk_from_counters()`
4. Build degradation status from result summary via `_build_degradation()`
5. Build base insight via `build_entity_insight()`
6. Execute language-aware semantic enrichment for candidates (budget-gated)
7. Derive semantic contract state from telemetry
8. Mark partial slices for missing languages
9. Attach insight to `result.summary["front_door_insight"]`

### Supporting Structs

**EntitySemanticTelemetry**:
```python
@dataclass(slots=True)
class EntitySemanticTelemetry:
    semantic_attempted: int = 0
    semantic_applied: int = 0
    semantic_failed: int = 0
    semantic_timed_out: int = 0
    semantic_provider: SemanticProvider = "none"
    py_attempted: int = 0
    py_applied: int = 0
    py_failed: int = 0
    py_timed_out: int = 0
    rust_attempted: int = 0
    rust_applied: int = 0
    rust_failed: int = 0
    rust_timed_out: int = 0
    reasons: list[str] = dataclass_field(default_factory=list)
```

**CandidateNeighborhood**:
```python
@dataclass(slots=True)
class CandidateNeighborhood:
    primary_target: Finding | None
    candidates: list[Finding]
    neighborhood: InsightNeighborhoodV1
    confidence: InsightConfidenceV1
```

### Neighborhood Building

Aggregates heuristic data from definition findings:

**Callers slice**:
- Sum `caller_count` from all candidates
- Build preview nodes for candidates with callers

**Callees slice**:
- Sum `callee_count` from all candidates
- Build preview nodes for candidates with callees

**Hierarchy/scope slice**:
- Extract unique `enclosing_scope` values from candidates
- Filter out empty and `<module>` scopes
- Build preview nodes sorted by scope name

**Result**: `InsightNeighborhoodV1` with four canonical slices (callers, callees, references, hierarchy_or_scope).

### Semantic Enrichment Integration

**Language-aware routing**:
1. For each candidate finding:
   - Resolve target context (file, language)
   - Skip if context unresolved
   - Enrich via language-specific adapter (Python → pyrefly, Rust → rust-analyzer)
   - Augment insight with semantic data (preview_per_slice=5)

**Provider mapping**:
- Python files (`.py`, `.pyi`) → `python_static` provider (pyrefly)
- Rust files (`.rs`) → `rust_static` provider (rust-analyzer)

**Budget controls**:
- Semantic enrichment skipped when `match_count > relationship_detail_max_matches`
- Semantic enrichment skipped when `semantic_runtime_enabled()` returns False
- Per-candidate semantic requests use `budget_for_mode("entity")` timeouts

### Semantic Contract State Derivation

Canonical semantic status embedded in insight degradation:

```python
semantic_state = derive_semantic_contract_state(
    SemanticContractStateInputV1(
        provider=telemetry.semantic_provider,
        available=telemetry.semantic_provider != "none",
        attempted=telemetry.semantic_attempted,
        applied=telemetry.semantic_applied,
        failed=max(
            telemetry.semantic_failed,
            telemetry.semantic_attempted - telemetry.semantic_applied
        ),
        timed_out=telemetry.semantic_timed_out,
        reasons=tuple(dict.fromkeys(telemetry.reasons)),
    )
)

insight = msgspec.structs.replace(
    insight,
    degradation=msgspec.structs.replace(
        insight.degradation,
        semantic=semantic_state.status,
        notes=tuple(dict.fromkeys([*insight.degradation.notes, *semantic_state.reasons])),
    ),
)
```

**Output**: Canonical `SemanticStatus` embedded in `insight.degradation.semantic` field.

### Confidence Scoring

Confidence derived from finding score metadata:

```python
def _confidence_from_candidates(candidates: list[Finding]) -> InsightConfidenceV1:
    confidence = InsightConfidenceV1(evidence_kind="resolved_ast", score=0.8, bucket="high")
    for finding in candidates:
        score = finding.details.score
        if score is None:
            continue
        return InsightConfidenceV1(
            evidence_kind=score.evidence_kind or confidence.evidence_kind,
            score=float(score.confidence_score) if score.confidence_score is not None else 0.8,
            bucket=score.confidence_bucket or confidence.confidence_bucket,
        )
    return confidence
```

### Degradation Status Building

Captures scan issues, scope filtering, and budget constraints:

```python
def _build_degradation(summary: dict[str, object]) -> InsightDegradationV1:
    dropped = summary.get("dropped_by_scope")
    scope_filter_status = "dropped" if isinstance(dropped, dict) and dropped else "none"
    notes: list[str] = []
    if isinstance(dropped, dict) and dropped:
        notes.append(f"dropped_by_scope={dropped}")
    return InsightDegradationV1(
        semantic="skipped",  # Updated later by semantic contract state
        scan=(
            "timed_out"
            if bool(summary.get("timed_out"))
            else "truncated"
            if bool(summary.get("truncated"))
            else "ok"
        ),
        scope_filter=scope_filter_status,
        notes=tuple(notes),
    )
```

### Cross-Reference

**FrontDoor Insight V1 Contract**: See [06_output_schema.md § FrontDoor Insight V1](06_output_schema.md#frontdoor-insight-v1) for the complete schema specification.

**Bootstrap integration**: Entity queries invoke `resolve_runtime_services()` to obtain `EntityService` for entity scan caching via `QueryEntityScanCacheV1`.

**See also**: [10_runtime_services.md](10_runtime_services.md) for runtime services documentation.

## Batch Query Execution

Location: `tools/cq/query/batch.py` (170 lines)

For multi-query workflows (e.g., `cq run`), batch execution amortizes parse and scan costs by sharing ast-grep scans across multiple queries.

### BatchEntityQuerySession Structure

```python
@dataclass(frozen=True)
class BatchEntityQuerySession:
    """Shared scan session for multiple entity queries."""

    root: Path
    tc: Toolchain
    files: list[Path]
    files_by_rel: dict[str, Path]
    records: list[SgRecord]
    scan: ScanContext
    candidates: EntityCandidates
    symtable: SymtableEnricher
```

### Session Building

The `build_batch_session()` function creates a shared scan session:

1. Tabulate files via `list_scan_files()`
2. Index files by relative path
3. Scan records:
   - Normalize record types
   - Get rules for types
   - Scan files with rules
   - Filter by record types
4. Build scan context via `_build_scan_context()`
5. Build entity candidates via `_build_entity_candidates()`
6. Return session with shared symtable enricher

### Scope Filtering

The `filter_files_for_scope()` function filters pre-scanned files by scope constraints:

1. Resolve scope paths via `scope_to_paths()`
2. Return empty set if no paths
3. Extract globs via `scope_to_globs()`
4. For each file:
   - Check if within scope paths
   - Check if matches globs (if present)
   - Add relative path to allowed set
5. Return allowed paths

## Multi-Language Result Merging

Location: `tools/cq/query/merge.py` (205 lines)

The merge module implements canonical auto-scope result merging for multi-language queries.

### Merge Strategy

The `merge_auto_scope_query_results()` function merges per-language results:

1. Build cross-language diagnostics via `build_cross_language_diagnostics()`:
   - Count Python and Rust matches
   - Detect if query is Python-oriented
   - Generate diagnostic messages
2. Add capability diagnostics via `build_capability_diagnostics()`:
   - Extract features from query
   - Check feature support per language
   - Generate capability warnings
3. Merge results via `merge_language_cq_results()`:
   - Combine findings from all languages
   - Merge run metadata
   - Attach diagnostics and payloads
4. Attach front-door insight for entity queries:
   - Check if insight already present (from individual language)
   - If not, resolve runtime services and attach fresh insight
   - Mark partial slices for missing languages

### Semantic Telemetry Merging

The `_merge_semantic_contract_inputs()` function aggregates per-language telemetry:

```python
py_attempted, py_applied, py_failed, py_timed_out = _coerce_semantic_telemetry(
    summary.get("python_semantic_telemetry")
)
rust_attempted, rust_applied, rust_failed, rust_timed_out = _coerce_semantic_telemetry(
    summary.get("rust_semantic_telemetry")
)

provider = "none"
if rust_attempted > 0 and py_attempted <= 0:
    provider = "rust_static"
elif py_attempted > 0:
    provider = "python_static"

attempted = py_attempted + rust_attempted
applied = py_applied + rust_applied
failed = py_failed + rust_failed
timed_out = py_timed_out + rust_timed_out
```

This creates a unified semantic contract input for cross-language insights.

## Symbol Resolution

Location: `tools/cq/query/symbol_resolver.py` (484 lines)

The symbol resolver builds symbol tables and resolves references across files for advanced query features.

### SymbolTable Structure

```python
@dataclass
class SymbolTable:
    """Symbol table for a codebase.

    Provides resolution of names to definitions and tracking of imports.
    """

    # Mapping from symbol key to definition record
    definitions: dict[str, SgRecord] = field(default_factory=dict)

    # Mapping from (file, local_name) to import binding
    imports: dict[tuple[str, str], ImportBinding] = field(default_factory=dict)

    # Unresolved references with reasons
    unresolved: list[tuple[str, str]] = field(default_factory=list)
```

### Symbol Key Format

```python
@dataclass
class SymbolKey:
    """Unique identifier for a symbol.

    Format: module_path:qualname
    Example: src/cli/app:meta_launcher
    """

    module_path: str
    qualname: str

    def __str__(self) -> str:
        return f"{self.module_path}:{self.qualname}"
```

### Import Tracking

```python
@dataclass
class ImportBinding:
    """Represents an import binding.

    Tracks what name is bound and where it comes from.
    """

    local_name: str  # Name as used in this module
    source_module: str  # Module imported from
    source_name: str | None  # Original name if different (for 'as' imports)
    is_from_import: bool  # True for 'from x import y'
```

### Resolution Algorithm

The `resolve()` method implements name resolution:

```python
def resolve(self, file: str, name: str) -> SgRecord | None:
    # Check if it's an imported name
    binding = self.imports.get((file, name))
    if binding:
        return self._resolve_import(binding)

    # Try local definitions in same module
    module_path = _file_to_module_path(file)
    key = f"{module_path}:{name}"
    if key in self.definitions:
        return self.definitions[key]

    # Track as unresolved
    self.unresolved.append((file, name))
    return None
```

## AST-Grep Parser and File Inventory

Location: `tools/cq/query/sg_parser.py` (409 lines)

The sg_parser module provides ast-grep-py integration with cache-backed file inventory.

### sg_scan Function

```python
def sg_scan(
    paths: list[Path],
    record_types: Iterable[str] | Iterable[RecordType] | None = None,
    root: Path | None = None,
    globs: list[str] | None = None,
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE,
) -> list[SgRecord]:
    """Run ast-grep-py scan and return parsed records."""
    # 1. Tabulate files (with caching)
    files = _tabulate_scan_files(paths, root, globs, lang=lang)

    # 2. Build ast-grep rules
    normalized_record_types = normalize_record_types(record_types)
    rules = get_rules_for_types(normalized_record_types, lang=lang)

    # 3. Scan files
    records = scan_files(files, rules, root, lang=lang)

    # 4. Filter by record types
    return filter_records_by_type(records, normalized_record_types)
```

### File Inventory Caching

The `_tabulate_scan_files()` function implements cache-backed file inventory:

1. Resolve scope via `resolve_scope()`:
   - List files via `_list_files_for_inventory()`
   - Generate inventory token via `_inventory_token_for_root()`
2. Build cache key via `build_cache_key()`:
   - Namespace: "file_inventory"
   - Version: "v1"
   - Workspace: scope.root
   - Language: scope.language
   - Target: scope.scope_hash or scope.language
   - Extras: scope_globs, inventory_token
3. Read from cache if enabled:
   - Deserialize FileInventoryCacheV1 payload
   - Validate inventory token (file count stability)
   - Return cached files if valid
4. Tabulate files from resolved scope
5. Write to cache if enabled:
   - Serialize FileInventoryCacheV1 payload
   - Store with cache key

**Cache payload**:
```python
class FileInventoryCacheV1(CqStruct, frozen=True):
    """Cached file inventory payload for ast-grep scans."""

    files: list[str]
    snapshot_digest: str = ""
    inventory_token: dict[str, int] = msgspec.field(default_factory=dict)
```

## Batch Span Collection

Location: `tools/cq/query/batch_spans.py` (122 lines)

For multi-query workflows (e.g., `cq run`), batch span collection amortizes parse costs by reusing parsed AST trees across queries.

### Collection Strategy

The `collect_span_filters()` function implements shared parsing:

```python
per_query: list[list[AstGrepMatchSpan]] = [[] for _ in queries]
rule_sets = [plan.sg_rules if plan.sg_rules else () for plan in plans]
query_langs: list[QueryLanguage] = [query.primary_language for query in queries]

for file_path in files:
    _collect_file_matches(
        file_path=file_path,
        root=root,
        rule_sets=rule_sets,
        query_langs=query_langs,
        per_query=per_query,
    )

return _build_spans_by_query(per_query=per_query, queries=queries)
```

### Per-File Matching

The `_collect_file_matches()` function caches SgRoot by language:

```python
roots_by_lang: dict[QueryLanguage, SgNode] = {}

for idx, rules in enumerate(rule_sets):
    if not rules:
        continue
    lang = query_langs[idx]
    if file_path.suffix not in file_extensions_for_language(lang):
        continue

    # Cache parsed tree per language
    if lang not in roots_by_lang:
        roots_by_lang[lang] = SgRoot(src, lang).root()
    node = roots_by_lang[lang]

    for rule in rules:
        for match in _iter_rule_matches_for_spans(node, rule):
            per_query[idx].append(AstGrepMatchSpan(...))
```

This ensures a file with Python and Rust queries is parsed once per language, not once per query.

## Enrichment Subsystem

Location: `tools/cq/query/enrichment.py` (629 lines)

The enrichment module provides symtable and bytecode analysis for query results. It's lazy-loaded only when needed (scope filters, evidence fields, scope expanders).

### SymtableInfo

```python
@dataclass(frozen=True)
class SymtableInfo:
    locals: tuple[str, ...]
    globals_used: tuple[str, ...]
    free_vars: tuple[str, ...]  # Closure variables
    nested_scopes: int
```

Extracted via `analyze_symtable()` which uses Python's `symtable` module:
1. Parse source with `symtable.symtable()`
2. Walk symbol table tree
3. For each function/class scope:
   - Extract local symbols (is_local())
   - Extract global symbols (is_global())
   - Extract free variables (is_free())
   - Count nested scopes (get_children())
4. Build SymtableInfo mapping

### BytecodeInfo

```python
@dataclass(frozen=True)
class BytecodeInfo:
    load_globals: tuple[str, ...]
    load_attrs: tuple[str, ...]
    call_functions: tuple[str, ...]
```

Extracted via `analyze_bytecode()` which uses Python's `dis` module:
1. Disassemble code object with `dis.get_instructions()`
2. Iterate instructions:
   - LOAD_GLOBAL, LOAD_NAME → add to load_globals
   - LOAD_ATTR → add to load_attrs
   - CALL, CALL_FUNCTION, CALL_METHOD → extract previous instruction's target, add to call_functions
3. Build BytecodeInfo

### SymtableEnricher

The `SymtableEnricher` class provides stateful enrichment with file-level caching:

```python
class SymtableEnricher:
    def __init__(self, root: Path):
        self._root = root
        self._cache: dict[str, dict[str, SymtableInfo]] = {}

    def enrich_function_finding(self, finding: Finding, record: SgRecord) -> dict[str, object] | None:
        # Extract function name, read source, analyze symtable
        # Returns scope info: locals, globals, free_vars, cell_vars, is_closure
```

Scope filtering via `filter_by_scope()`:
1. Iterate findings with records
2. Enrich each finding with symtable info
3. Check scope_type filter (closure vs toplevel)
4. Check captures filter (free_vars contains specified variable)
5. Check has_cells filter (cell_vars present or not)
6. Append passing findings to filtered list

## Execution Requests

Location: `tools/cq/query/execution_requests.py` (65 lines)

Request payloads enable shared execution workflows (e.g., `cq run` with pre-scanned records).

### EntityQueryRequest

```python
@dataclass(frozen=True)
class EntityQueryRequest:
    plan: ToolPlan
    query: Query
    tc: Toolchain
    root: Path
    records: list[SgRecord]  # Pre-scanned records
    paths: list[Path]
    scope_globs: list[str] | None
    argv: list[str]
    query_text: str | None = None
    match_spans: dict[str, list[tuple[int, int]]] | None = None  # Pre-computed relational spans
    symtable: SymtableEnricher | None = None  # Shared symtable enricher
```

Used by `execute_entity_query_from_records()` to skip scanning phase.

### PatternQueryRequest

```python
@dataclass(frozen=True)
class PatternQueryRequest:
    plan: ToolPlan
    query: Query
    tc: Toolchain
    root: Path
    files: list[Path]  # Pre-tabulated files
    argv: list[str]
    query_text: str | None = None
    decisions: list[FileFilterDecision] | None = None  # File filter audit trail
```

Used by `execute_pattern_query_with_files()` to skip file tabulation phase.

## Execution Context

Location: `tools/cq/query/execution_context.py` (26 lines)

Bundled context for query execution.

```python
class QueryExecutionContext(CqStruct, frozen=True):
    query: Query
    plan: ToolPlan
    tc: Toolchain
    root: Path
    argv: list[str]
    started_ms: float
    query_text: str | None = None
```

This struct is threaded through the execution pipeline to provide consistent access to runtime parameters.

## Multi-Language Support

Location: `tools/cq/query/language.py` (310 lines)

The language module implements scope-based multi-language query execution.

### Core Types

```python
QueryLanguage = Literal["python", "rust"]
QueryLanguageScope = Literal["auto", "python", "rust"]

DEFAULT_QUERY_LANGUAGE: QueryLanguage = "python"
DEFAULT_QUERY_LANGUAGE_SCOPE: QueryLanguageScope = "auto"
```

### Scope Expansion

```python
def expand_language_scope(scope: QueryLanguageScope) -> tuple[QueryLanguage, ...]:
    if scope == "auto":
        return ("python", "rust")
    return (scope,)
```

### Extension Mapping

```python
LANGUAGE_FILE_EXTENSIONS: dict[QueryLanguage, tuple[str, ...]] = {
    "python": (".py", ".pyi"),
    "rust": (".rs",),
}
```

### Scope Enforcement

Extension filtering is **authoritative**:
- `lang=rust`: Only `.rs` files are scanned
- `lang=python`: Only `.py`/`.pyi` files are scanned
- `lang=auto`: Union is scanned and results are partitioned by detected language

```python
def is_path_in_lang_scope(path: str | Path, scope: QueryLanguageScope) -> bool:
    lang = infer_language_for_path(path)
    if lang is None:
        return False
    return lang in expand_language_scope(scope)
```

## Metavariable System

Location: `tools/cq/query/metavar.py` (162 lines)

Metavariables enable pattern capture and post-filtering.

### Metavariable Kinds

```python
MetaVarKind = Literal["single", "multi", "unnamed"]

def get_metavar_kind(metavar: str) -> MetaVarKind:
    if metavar.startswith("$$$"):
        return "multi"
    if metavar.startswith("$$"):
        return "unnamed"
    return "single"
```

**Conventions**:
- `$NAME`: Named single capture (enforces equality on reuse in pattern)
- `$$$NAME`: Zero-or-more nodes (non-greedy)
- `$$NAME`: Unnamed node capture (operators, punctuation)
- `$_NAME`: Non-capturing wildcard (no equality enforcement)

### Capture Parsing

The `parse_metavariables()` function extracts captures from ast-grep JSON output:

```python
meta_vars = match_result.get("metaVariables")

# Single captures ($NAME)
for name, capture_info in meta_vars.get("single", {}).items():
    if name.startswith("_"):  # Skip non-capturing wildcards
        continue
    captures[name] = MetaVarCapture(name=name, kind="single", text=capture_info.get("text", ""))

# Multi captures ($$$NAME)
for name, capture_list in meta_vars.get("multi", {}).items():
    if name.startswith("_"):
        continue
    if isinstance(capture_list, list):
        node_texts = [c.get("text", "") for c in capture_list if isinstance(c, dict)]
        captures[name] = MetaVarCapture(name=name, kind="multi", text=", ".join(node_texts), nodes=node_texts)
```

### Filter Application

The `apply_metavar_filters()` function validates captures against filter specs:

```python
for filter_spec in filters:
    capture = captures.get(filter_spec.name)
    if capture is None:
        return False  # Filter references missing metavar

    if not filter_spec.matches(capture):
        return False  # Regex match failed

return True  # All filters passed
```

The `MetaVarFilter.matches()` method applies regex with optional negation:

```python
def matches(self, capture: MetaVarCapture) -> bool:
    match = bool(re.search(self.pattern, capture.text))
    return not match if self.negate else match
```

## Architectural Observations

### Parse-Compile-Execute Pipeline

The three-phase pipeline provides clean separation but introduces coupling at phase boundaries:

**Coupling Points**:
1. Parser produces IR that planner must exhaustively handle
2. Planner produces ToolPlan that executor must route correctly
3. Executor produces CqResult that formatter must render

**Improvement Opportunities**:
- Introduce capability-based dispatching: let IR declare required capabilities, planner computes capability closure, executor validates capability availability before execution
- Add query optimization pass between parse and compile: constant folding, dead-field elimination, scope constraint propagation
- Implement query explain mode that shows optimization decisions and estimated costs

### Token-Based DSL vs Proper Grammar

The current token-based approach (`key=value` pairs) is simple but has limitations:

**Tradeoffs**:

| Approach | Advantages | Disadvantages |
|----------|-----------|---------------|
| Token-based (current) | Simple to parse, forgiving syntax, easy CLI composition | Limited nesting, ambiguous precedence, verbose for complex queries |
| Formal grammar (PEG/LALR) | Precise semantics, composable operators, better error messages | Steeper learning curve, requires parser generator, harder to extend |

**Observations**:
- Token order doesn't matter except for handler order (`_ENTITY_HANDLER_ORDER`)
- No token-level validation of mutual exclusivity (enforced in `Query.__post_init__()`)
- Relational constraints are flat (no nesting: can't express "inside X AND inside Y")

**Improvement Opportunities**:
- Add precedence rules for composite operators (current: flat all/any/not)
- Support parenthesized sub-queries for clarity
- Add query macros/aliases for common patterns
- Implement query builder API for programmatic construction

### Entity vs Pattern Query Bifurcation

The executor has two separate paths (entity and pattern) that share minimal code:

**Why Bifurcation Exists**:
- Entity queries scan for known record types then filter by entity semantics
- Pattern queries execute arbitrary ast-grep patterns then post-filter
- Different enrichment strategies: entity queries join calls to defs, pattern queries report raw matches

**Coupling Issues**:
- `_apply_entity_handlers()` has 4 entity-specific branches
- Expander sections are only available for entity queries
- Pattern queries can't use decorator/scope filters (symtable enrichment applies but no pre-filtering)

**Improvement Opportunities**:
- Unify via abstract query executor protocol with entity/pattern implementations
- Extract shared scan-filter-enrich pipeline
- Support hybrid queries: pattern to identify entities, then apply entity enrichment
- Add pattern-based expanders (e.g., find patterns calling a target pattern)

### ScanContext as Mutable State Holder

The `ScanContext` dataclass bundles indexed scan results but doesn't enforce immutability:

```python
@dataclass  # Not frozen
class ScanContext:
    def_records: list[SgRecord]  # Mutable list
    call_records: list[SgRecord]
    interval_index: IntervalIndex[SgRecord]
    file_index: FileIntervalIndex
    calls_by_def: dict[SgRecord, list[SgRecord]]  # Mutable dict
    all_records: list[SgRecord]
```

**Risks**:
- Handlers could mutate shared scan context
- Interval indices could become stale if records are modified

**Improvement Opportunities**:
- Make `ScanContext` frozen with immutable collections
- Use persistent data structures (pyrsistent) for zero-copy mutations
- Implement copy-on-write semantics for scan context forks
- Add validation that records haven't changed after indexing

### Language Scope Expansion Patterns

The multi-language execution strategy (`lang_scope=auto`) runs queries in parallel per language then merges:

**Current Architecture**:
1. `execute_by_language_scope()` spawns per-language queries
2. Each query runs with scoped `Query` (`lang_scope="python"` or `lang_scope="rust"`)
3. Results merged via `merge_language_cq_results()` with diagnostics

**Advantages**:
- Clean separation: each language query is independent
- Parallel execution potential (currently sequential but parallelizable)
- Diagnostic generation can explain language-specific issues

**Limitations**:
- No cross-language relationship detection (e.g., Python calling Rust via FFI)
- Duplicate work if files contain both languages (unlikely but possible)
- Result merging is additive (no deduplication of findings with same location)

**Improvement Opportunities**:
- Add cross-language relationship detection via FFI analysis
- Implement language-specific query preprocessing (e.g., Rust-specific pattern simplifications)
- Support language-aware scope constraints: `in=rust/` matches only Rust modules
- Add language affinity scoring for auto-scope queries to prioritize primary language

### Query Composition and Reuse

The current system doesn't support query composition or named query definitions:

**Missing Capabilities**:
- No query variables or parameterization
- No query library/registry
- No query composition operators (union, intersection, difference)
- No incremental query refinement

**Improvement Opportunities**:
- Add query definition language: `let funcs = entity=function in=src/`
- Support query composition: `funcs & uses(dangerous_api)`
- Implement query templates: `@deprecated = entity=function decorated_by=deprecated`
- Add query pipelines: `entity=function | filter(calls > 10) | sort(calls desc) | limit(5)`

### Error Recovery and Diagnostics

The parser has limited error recovery:

**Current Behavior**:
- First parse error aborts query (fallback to smart_search for plain queries)
- No partial results on error
- No suggestions for typos

**Improvement Opportunities**:
- Implement error recovery: continue parsing after first error, collect all errors
- Add did-you-mean suggestions for token keys
- Provide query examples in error messages
- Implement query linting (warn about inefficient patterns)

### Performance and Caching

The executor doesn't cache intermediate results:

**Current Performance Characteristics**:
- Each query scans from scratch (no scan result reuse)
- `cq run` amortizes span collection but not entity scanning
- Symtable/bytecode enrichment re-analyzes files per query

**Improvement Opportunities**:
- Add scan result caching keyed by (root, record_types, lang)
- Cache symtable/bytecode enrichment per file
- Implement incremental query evaluation (reuse previous results when query changes minimally)
- Add query cost estimation and adaptive execution (switch strategies based on estimated cost)

## Cross-References

- **FrontDoor Insight V1 Schema**: [06_output_schema.md § FrontDoor Insight V1](06_output_schema.md#frontdoor-insight-v1)
- **AST-Grep Pattern Matching**: [07_analysis_engines.md § AST-Grep Engine](07_analysis_engines.md#ast-grep-engine)
- **Tree-Sitter Queries**: [07_analysis_engines.md § Tree-Sitter Engine](07_analysis_engines.md#tree-sitter-engine)
- **Runtime Services**: [10_runtime_services.md](10_runtime_services.md)
- **Multi-Language Orchestration**: [05_multilang_orchestration.md](05_multilang_orchestration.md)

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 0.1.0 | 2025-01-15 | Initial architecture documentation |
| 0.2.0 | 2025-02-01 | Added multi-language support, semantic enrichment |
| 0.3.0 | 2025-02-10 | Added batch execution, symbol resolution |
| 0.4.0 | 2026-02-15 | Added entity front-door insight assembly, updated LOC counts, new modules |
