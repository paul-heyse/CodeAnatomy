# Query Subsystem Architecture

## Overview

The CQ query subsystem (`tools/cq/query/`) implements a declarative code query system for Python and Rust codebases. It provides a token-based DSL for expressing semantic code queries that compile to executable plans using ast-grep-py for structural pattern matching.

The subsystem follows a classic compiler architecture with three distinct phases: **parse** (DSL to IR), **compile** (IR to execution plan), and **execute** (plan to results). This separation enables query optimization, caching, and composability while maintaining deterministic semantics.

## Module Map

| Module | Responsibility | Lines | Key Types |
|--------|---------------|-------|-----------|
| `parser.py` | Query DSL tokenization and parsing to IR | 965 | `parse_query()`, `_tokenize()`, `_EntityQueryState` |
| `ir.py` | Intermediate representation type definitions | 766 | `Query`, `PatternSpec`, `RelationalConstraint`, `CompositeRule` |
| `planner.py` | IR compilation to executable ToolPlan | 659 | `compile_query()`, `ToolPlan`, `AstGrepRule` |
| `executor.py` | Plan execution and result generation | 2473 | `execute_plan()`, `ScanContext`, entity/pattern handlers |
| `batch_spans.py` | Batch relational span collection | 122 | `collect_span_filters()` |
| `enrichment.py` | Symtable/bytecode enrichment | 600+ | `SymtableEnricher`, `BytecodeInfo`, `SymtableInfo` |
| `execution_requests.py` | Request payloads for shared execution | 65 | `EntityQueryRequest`, `PatternQueryRequest` |
| `execution_context.py` | Bundled execution context | 26 | `QueryExecutionContext` |
| `language.py` | Multi-language scope resolution | 310 | `QueryLanguage`, `QueryLanguageScope`, extension mappings |
| `metavar.py` | Metavariable parsing and filtering | 162 | `parse_metavariables()`, `apply_metavar_filters()` |

## CLI Entry Point

Location: `tools/cq/cli_app/commands/query.py`

The `q()` function handles user-facing query command execution with fallback behavior:

```python
def q(query_string: str, opts: QueryParams, ctx: CliContext) -> CliResult:
    has_tokens = _has_query_tokens(query_string)  # Regex check for key=value

    try:
        parsed_query = parse_query(query_string)
    except QueryParseError as e:
        if not has_tokens:
            # Fallback to smart_search for plain identifier queries
            return smart_search(query_string, ...)
        # Report parse error for tokenized queries
        return error_result(str(e))

    plan = compile_query(parsed_query)
    result = execute_plan(plan, parsed_query, ...)
    return CliResult(result=result, context=ctx, filters=opts)
```

**Token detection pattern**: `([\w.]+|\$+\w+)=(?:'([^']+)'|"([^"]+)"|([^\s]+))`

This regex matches:
- Dot-notation keys: `pattern.context=`, `inside.stopBy=`
- Metavariable filter keys: `$NAME=`, `$$OP=`, `$$$ARGS=`
- Quoted values (single/double quotes) or unquoted values

## Query DSL Grammar

The query language supports two primary modes: **entity queries** (search by semantic entity type) and **pattern queries** (search by AST structure).

### Token Syntax

All queries use `key=value` token pairs. Values may be quoted (single or double) for strings containing spaces or special characters.

### Core Tokens

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

### Relational Constraint Tokens

Relational operators enable context-aware queries using ast-grep's relational rules.

| Token | Semantics | Example |
|-------|-----------|---------|
| `inside='<pattern>'` | Match inside ancestor matching pattern | `inside='class Config'` |
| `has='<pattern>'` | Match containing descendant matching pattern | `has='await $X'` |
| `precedes='<pattern>'` | Match preceding sibling matching pattern | `precedes='return $X'` |
| `follows='<pattern>'` | Match following sibling matching pattern | `follows='if $COND:'` |

**Modifiers**:
- `<op>.stopBy=<mode>` - When to stop search: `neighbor` (default), `end`, or custom pattern
- `<op>.field=<name>` - Constrain search to specific AST field (only `inside`/`has`)

### Composite Logic Tokens

Composite rules combine multiple patterns with boolean operators.

| Token | Logic | Example |
|-------|-------|---------|
| `all='<p1>,<p2>'` | AND - all patterns must match | `all='await $X,return $Y'` |
| `any='<p1>,<p2>'` | OR - at least one pattern must match | `any='logger.$M,print'` |
| `not='<pattern>'` | NOT - pattern must not match (single pattern only) | `not='pass'` |

### Scope Filter Tokens

Symtable-driven scope filtering for closure detection.

| Token | Purpose | Example |
|-------|---------|---------|
| `scope=<type>` | Scope type filter | `scope=closure` |
| `captures=<var>` | Filter by captured variables | `captures=x` |
| `has_cells=<bool>` | Filter by cell variable presence | `has_cells=true` |

### Decorator Filter Tokens

Decorator-based filtering.

| Token | Purpose | Example |
|-------|---------|---------|
| `decorated_by=<name>` | Filter by decorator presence | `decorated_by=property` |
| `decorator_count_min=<N>` | Minimum decorator count | `decorator_count_min=2` |
| `decorator_count_max=<N>` | Maximum decorator count | `decorator_count_max=5` |

### Metavariable Filter Tokens

Post-filter pattern matches by captured metavariable content.

Syntax: `$<NAME>=~<regex>` or `$<NAME>!=~<regex>`

Examples:
- `$$OP=~'^[<>=]'` - Binary comparison operators only
- `$X!=~debug` - Exclude matches where `$X` captures "debug"

### Pattern Disambiguation Tokens

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

### Positional Matching Tokens

Match nodes by position among siblings.

| Token | Purpose | Example |
|-------|---------|---------|
| `nthChild=<N>` | Exact position (1-indexed) | `nthChild=2` |
| `nthChild='<formula>'` | Formula (e.g., '2n+1' for odd) | `nthChild='2n'` |
| `nthChild.reverse=<bool>` | Count from end | `nthChild.reverse=true` |
| `nthChild.ofRule='<rule>'` | Filter which siblings count | `nthChild.ofRule='kind=identifier'` |

## Query IR (Intermediate Representation)

Location: `tools/cq/query/ir.py` (766 lines)

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

**Query** (lines 553-658) - Top-level parsed query
```python
class Query(msgspec.Struct, frozen=True):
    entity: EntityType | None = None
    name: str | None = None
    expand: tuple[Expander, ...] = ()
    scope: Scope = msgspec.field(default_factory=Scope)
    fields: tuple[FieldType, ...] = ("def",)
    limit: int | None = None
    explain: bool = False
    pattern_spec: PatternSpec | None = None
    relational: tuple[RelationalConstraint, ...] = ()
    scope_filter: ScopeFilter | None = None
    decorator_filter: DecoratorFilter | None = None
    joins: tuple[JoinConstraint, ...] = ()
    metavar_filters: tuple[MetaVarFilter, ...] = ()
    composite: CompositeRule | None = None
    nth_child: NthChildSpec | None = None
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE

    def __post_init__(self):
        # Validate: must specify either entity OR pattern_spec (mutually exclusive)
```

**PatternSpec** (lines 245-328) - Pattern specification with disambiguation
```python
class PatternSpec(msgspec.Struct, frozen=True):
    pattern: str
    context: str | None = None
    selector: str | None = None
    strictness: StrictnessMode = "smart"

    def requires_yaml_rule(self) -> bool:
        # True if context, selector, or non-smart strictness present

    def to_yaml_dict(self) -> dict[str, object]:
        # Convert to ast-grep YAML rule format
```

**RelationalConstraint** (lines 331-396) - Structural relationship
```python
class RelationalConstraint(msgspec.Struct, frozen=True):
    operator: RelationalOp
    pattern: str
    stop_by: StopByMode | str = "neighbor"
    field_name: str | None = None  # Only valid for inside/has

    def to_ast_grep_dict(self) -> dict[str, object]:
        # Convert to ast-grep rule format
```

**CompositeRule** (lines 403-460) - Boolean composition
```python
class CompositeRule(msgspec.Struct, frozen=True):
    operator: CompositeOp
    patterns: tuple[str, ...]
    metavar_order: tuple[str, ...] | None = None

    def to_ast_grep_dict(self) -> dict[str, object]:
        # Validates 'not' has single pattern, builds rule dict
```

**Expander** (lines 112-124) - Graph expansion operator
```python
class Expander(msgspec.Struct, frozen=True):
    kind: ExpanderKind
    depth: Annotated[int, msgspec.Meta(ge=1)] = 1
```

**Scope** (lines 127-142) - File scope constraints
```python
class Scope(msgspec.Struct, frozen=True):
    in_dir: str | None = None
    exclude: tuple[str, ...] = ()
    globs: tuple[str, ...] = ()
```

**MetaVarCapture** (lines 145-174) - Captured metavariable
```python
class MetaVarCapture(msgspec.Struct, frozen=True):
    name: str
    kind: MetaVarKind
    text: str
    nodes: list[str] | None = None  # For multi captures, individual node texts
```

**MetaVarFilter** (lines 177-224) - Metavariable post-filter
```python
class MetaVarFilter(msgspec.Struct, frozen=True):
    name: str
    pattern: str  # Regex pattern
    negate: bool = False

    def matches(self, capture: MetaVarCapture) -> bool:
        # Apply regex filter to capture text
```

## Query Parser

Location: `tools/cq/query/parser.py` (965 lines)

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

The `_tokenize()` function (lines 262-292) uses a regex to extract key-value pairs:

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

Entity queries use `_EntityQueryState` (lines 93-128) and pattern queries use `_PatternQueryState` (lines 131-156) to accumulate parsed components.

Token handlers (`_ENTITY_TOKEN_HANDLERS` at line 201) process tokens in a defined order (`_ENTITY_HANDLER_ORDER` at line 210):

```python
_ENTITY_HANDLER_ORDER = ("name", "expand", "scope", "fields", "limit", "explain", "lang")
```

Each handler mutates the state object. After all handlers run, the state builds the final `Query` object.

### Expander Parsing

The `_parse_expanders()` function (lines 589-625) handles complex expander syntax:

```python
# Split by comma, respecting nested parentheses
parts = _split_expanders(expand_str)

for part in parts:
    # Parse: kind(depth=N) or just kind
    match = re.match(r"(\w+)(?:\(([^)]*)\))?", part)
    kind = _parse_expander_kind(match.group(1))
    depth = _parse_expander_params(match.group(2) or "")
    expanders.append(Expander(kind=kind, depth=depth))
```

### Relational Constraint Parsing

The `_parse_relational_constraints()` function (lines 454-496) supports both dot notation and global fallbacks:

```python
global_stop_by = tokens.get("stopBy") or "neighbor"
global_field = tokens.get("field")

for op in ("inside", "has", "precedes", "follows"):
    pattern = tokens.get(op)
    if pattern:
        # Try dot notation first, then underscore, then global
        stop_by = tokens.get(f"{op}.stopBy") or tokens.get(f"{op}_stop_by") or global_stop_by
        field_name = tokens.get(f"{op}.field") or tokens.get(f"{op}_field") or global_field

        constraints.append(RelationalConstraint(
            operator=op, pattern=pattern, stop_by=stop_by, field_name=field_name
        ))
```

## Query Planner

Location: `tools/cq/query/planner.py` (659 lines)

The planner compiles Query IR into an executable ToolPlan that specifies which tools to run and how.

### ToolPlan Structure

```python
class ToolPlan(msgspec.Struct, frozen=True):
    scope: Scope = msgspec.field(default_factory=Scope)
    sg_record_types: frozenset[str] = frozenset({"def", "call", "import"})
    need_symtable: bool = False
    need_bytecode: bool = False
    expand_ops: tuple[Expander, ...] = ()
    explain: bool = False
    sg_rules: tuple[AstGrepRule, ...] = ()
    is_pattern_query: bool = False
    lang: QueryLanguage = DEFAULT_QUERY_LANGUAGE
    lang_scope: QueryLanguageScope = DEFAULT_QUERY_LANGUAGE_SCOPE
```

### AstGrepRule Structure

```python
class AstGrepRule(msgspec.Struct, frozen=True):
    pattern: str
    kind: str | None = None
    context: str | None = None
    selector: str | None = None
    strictness: StrictnessMode = "smart"
    inside: str | None = None
    has: str | None = None
    precedes: str | None = None
    follows: str | None = None
    inside_stop_by: str | None = None
    has_stop_by: str | None = None
    inside_field: str | None = None
    has_field: str | None = None
    composite: CompositeRule | None = None
    nth_child: NthChildSpec | None = None

    def requires_inline_rule(self) -> bool:
        # True if features beyond simple pattern present

    def to_yaml_dict(self) -> dict[str, object]:
        # Convert to ast-grep YAML format
```

### Compilation Strategy

The `compile_query()` function (line 260) dispatches based on query type:

```python
def compile_query(query: Query) -> ToolPlan:
    if query.is_pattern_query:
        return _compile_pattern_query(query)
    return _compile_entity_query(query)
```

**Entity Query Compilation** (lines 282-313):
1. Determine required record types via `_determine_record_types()` - union of entity type records, expander records, and field records
2. Check if symtable needed via `_needs_symtable()` - True for scope expander, callers/callees expanders, or scope_filter present
3. Check if bytecode needed via `_needs_bytecode()` - True for bytecode_surface expander or evidence field
4. Generate ast-grep rules from relational constraints via `_entity_to_ast_grep_rules()`

**Pattern Query Compilation** (lines 316-350):
1. Build AstGrepRule from PatternSpec
2. Apply relational constraints via `_apply_relational_constraints()`
3. Set empty sg_record_types (not using standard record scanning)
4. Mark `is_pattern_query=True`

### Record Type Inference

Record types determine which ast-grep record patterns to scan for:

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
_ENTITY_EXTRA_RECORDS: dict[str, set[str]] = {
    "function": {"call"},  # Need calls for caller/callee analysis
    "class": {"call"},
    "method": {"call"},
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

For Rust queries, `_rust_entity_to_ast_grep_rules()` (lines 400-443) generates language-specific rules:

```python
if entity == "class":
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

The `_apply_relational_constraints()` function (lines 458-491) merges constraints into AstGrepRule:

```python
state = _RelationalState(
    inside=rule.inside,
    has=rule.has,
    precedes=rule.precedes,
    follows=rule.follows,
    inside_stop_by=rule.inside_stop_by,
    has_stop_by=rule.has_stop_by,
    inside_field=rule.inside_field,
    has_field=rule.has_field,
)

for constraint in constraints:
    _apply_relational_constraint(state, constraint)

return _merge_relational_state(rule, state)
```

The `_normalize_stop_by()` function (lines 521-529) applies heuristics:

```python
# For class/function inside constraints, use "end" to search to root
if constraint.operator in {"inside", "has"}:
    pattern = constraint.pattern.strip()
    if pattern.startswith(("class ", "def ", "async def ")):
        return "end"
```

## Query Executor

Location: `tools/cq/query/executor.py` (2473 lines)

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

### Single-Context Execution

The `_execute_single_context()` function (line 571) dispatches by query type:

```python
def _execute_single_context(ctx: QueryExecutionContext) -> CqResult:
    if ctx.plan.is_pattern_query:
        return _execute_pattern_query(ctx)
    return _execute_entity_query(ctx)
```

### Entity Query Execution Flow

**Preparation Phase** (`_prepare_entity_state()`, lines 379-399):
1. Validate ast-grep availability via toolchain
2. Resolve paths from scope constraints via `scope_to_paths()`
3. Scan records via `sg_scan()` - runs ast-grep for required record types
4. Build scan context via `_build_scan_context()` - creates interval index, assigns calls to defs
5. Build entity candidates via `_build_entity_candidates()` - partitions by record type
6. Apply rule spans via `_apply_rule_spans()` - filters candidates using relational constraint matches

**Execution Phase** (`_execute_entity_query()`, lines 682-700):
1. Build result structure with RunMeta
2. Apply entity-specific handlers via `_apply_entity_handlers()`
3. Add file scan statistics
4. Add explain metadata if requested
5. Finalize multi-language summary if needed

### ScanContext Structure

The `ScanContext` dataclass (lines 83-92) bundles indexed scan results:

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

The `_apply_entity_handlers()` function (lines 452-477) routes by entity type:

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

The `_process_def_query()` function (lines 1345-1400) implements the core definition handler:

1. Filter candidates by name pattern via `_filter_to_matching()`
2. Convert records to findings via `_def_to_finding()`
3. Apply scope filter if present (closure detection)
4. Build sections for requested fields:
   - `callers` section: Find all calls to these definitions
   - `callees` section: Extract calls within these definitions
   - `imports` section: Show imports in files containing these definitions
5. Append expander sections (raises, scope, bytecode_surface)

### Pattern Query Execution Flow

**Preparation Phase** (`_prepare_pattern_state()`, lines 421-449):
1. Validate ast-grep availability
2. Resolve paths from scope
3. Tabulate files via `_tabulate_scope_files()` - applies extension filtering and glob rules
4. Return error if no files match

**Execution Phase** (`_execute_pattern_query()`, lines 746-786):
1. Execute ast-grep rules via `_execute_ast_grep_rules()`
2. Build result with findings
3. Apply scope filter if present (symtable enrichment)
4. Apply limit if specified
5. Add file scan statistics and explain metadata

### AST-Grep Rule Execution

The `_execute_ast_grep_rules()` function (lines 851-889) uses ast-grep-py directly:

```python
ctx = AstGrepExecutionContext(rules=rules, paths=paths, root=root, query=query, lang=query.primary_language if query is not None else DEFAULT_QUERY_LANGUAGE)
state = AstGrepExecutionState(findings=[], records=[], raw_matches=[])

for file_path in ctx.paths:
    src = file_path.read_text(encoding="utf-8")
    sg_root = SgRoot(src, ctx.lang)
    node = sg_root.root()
    rel_path = _normalize_match_file(str(file_path), ctx.root)

    for idx, rule in enumerate(ctx.rules):
        for match in _iter_rule_matches(node, rule):
            match_data = _build_match_data(match, rule_id=f"pattern_{idx}", rel_path=rel_path)
            if not _match_passes_filters(ctx, match):
                continue
            finding, record = _match_to_finding(match_data)
            if finding:
                _apply_metavar_details(ctx, match, finding)
                state.findings.append(finding)
            if record:
                state.records.append(record)

return state.findings, state.records, state.raw_matches
```

### Multi-Language Execution

The `_execute_auto_scope_plan()` function (lines 577-603) orchestrates parallel language execution:

```python
results = execute_by_language_scope(
    query.lang_scope,
    lambda lang: _run_scoped_auto_query(
        query=query, lang=lang, tc=tc, root=root, argv=argv
    ),
)

return _merge_auto_scope_results(
    query, results, root=root, argv=argv, tc=tc, query_text=query_text
)
```

Each language runs independently with scoped queries (`msgspec.structs.replace(query, lang_scope=cast("QueryLanguageScope", lang))`). Results are merged via `merge_language_cq_results()` with cross-language diagnostics.

### Relational Span Collection

For entity queries with relational constraints, `_collect_match_spans()` (lines 1095-1127) pre-computes spans:

```python
# 1. Tabulate files for language scope
file_result = tabulate_files(repo_index, paths, globs, extensions=file_extensions_for_scope(query.lang_scope))

# 2. Collect ast-grep match spans
matches = _collect_ast_grep_match_spans(file_result.files, rules, root, query.primary_language)

# 3. Filter by metavariable constraints if present
if query.metavar_filters:
    return _filter_match_spans_by_metavars(matches, query.metavar_filters)

# 4. Group by file
return _group_match_spans(matches)
```

The `_filter_records_by_spans()` function (lines 1194-1217) then filters entity candidates to those overlapping matched spans.

### Caller/Callee Analysis

The `_build_callers_section()` function (lines 2012-2032) implements transitive caller discovery:

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

The `_build_callees_section()` function (lines 2130-2175) is simpler:

1. Iterate target definitions
2. Extract calls within each definition (from `calls_by_def` mapping)
3. Build callee findings with evidence

### Scope Enrichment

The `_build_scope_section()` function (lines 2252-2296) provides closure analysis:

```python
enricher = SymtableEnricher(root)

for def_record in target_defs:
    base_finding = _def_to_finding(def_record, calls_by_def.get(def_record, []))
    scope_info = enricher.enrich_function_finding(base_finding, def_record)

    if scope_info:
        free_vars = scope_info.get("free_vars", [])
        cell_vars = scope_info.get("cell_vars", [])
        label = "closure" if scope_info.get("is_closure") else "toplevel"
        findings.append(...)
```

### Bytecode Surface Analysis

The `_build_bytecode_surface_section()` function (lines 2299-2352) extracts bytecode patterns:

```python
enrichment = enrich_records(target_defs, root)

for record in target_defs:
    location = f"{record.file}:{record.start_line}:{record.start_col}"
    info = enrichment.get(location, {})
    bytecode_info = info.get("bytecode_info")

    if isinstance(bytecode_info, BytecodeInfo):
        details = {
            "globals": list(bytecode_info.load_globals),
            "attrs": list(bytecode_info.load_attrs),
            "calls": list(bytecode_info.call_functions),
        }
        findings.append(...)
```

## Batch Span Collection

Location: `tools/cq/query/batch_spans.py` (122 lines)

For multi-query workflows (e.g., `cq run`), batch span collection amortizes parse costs by reusing parsed AST trees across queries.

### Collection Strategy

The `collect_span_filters()` function (lines 25-52) implements shared parsing:

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

The `_collect_file_matches()` function (lines 55-93) caches SgRoot by language:

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

Location: `tools/cq/query/enrichment.py` (600+ lines)

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

```python
table = symtable.symtable(source, filename, "exec")

def _process_symbol_table(st: symtable.SymbolTable) -> None:
    if st.get_type() in {"function", "class"}:
        name = st.get_name()
        local_syms = tuple(sorted(sym.get_name() for sym in st.get_symbols() if sym.is_local()))
        global_syms = tuple(sorted(sym.get_name() for sym in st.get_symbols() if sym.is_global()))
        free_syms = tuple(sorted(sym.get_name() for sym in st.get_symbols() if sym.is_free()))
        nested_count = len(st.get_children())

        result[name] = SymtableInfo(locals=local_syms, globals_used=global_syms, free_vars=free_syms, nested_scopes=nested_count)

    for child in st.get_children():
        _process_symbol_table(child)
```

### BytecodeInfo

```python
@dataclass(frozen=True)
class BytecodeInfo:
    load_globals: tuple[str, ...]
    load_attrs: tuple[str, ...]
    call_functions: tuple[str, ...]
```

Extracted via `analyze_bytecode()` which uses Python's `dis` module:

```python
instructions = list(dis.get_instructions(code_object))

for i, instr in enumerate(instructions):
    if instr.opname in {"LOAD_GLOBAL", "LOAD_NAME"} and instr.argval:
        load_globals.add(str(instr.argval))

    elif instr.opname == "LOAD_ATTR" and instr.argval:
        load_attrs.add(str(instr.argval))

    elif instr.opname in {"CALL", "CALL_FUNCTION", "CALL_METHOD"} and i > 0:
        prev_instr = instructions[i - 1]
        if prev_instr.opname in {"LOAD_ATTR", "LOAD_GLOBAL", "LOAD_NAME"} and prev_instr.argval:
            call_functions.add(str(prev_instr.argval))
```

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

```python
def filter_by_scope(
    findings: list[Finding],
    scope_filter: ScopeFilter,
    enricher: SymtableEnricher,
    records: list[SgRecord],
) -> list[Finding]:
    filtered: list[Finding] = []

    for finding, record in zip(findings, records):
        scope_info = enricher.enrich_function_finding(finding, record)

        if scope_filter.scope_type and not _matches_scope_type(scope_info, scope_filter.scope_type):
            continue
        if scope_filter.captures and not _matches_captures(scope_info, scope_filter.captures):
            continue
        if scope_filter.has_cells is not None and not _matches_has_cells(scope_info, scope_filter.has_cells):
            continue

        filtered.append(finding)

    return filtered
```

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

Used by `execute_entity_query_from_records()` (lines 703-743) to skip scanning phase.

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

Used by `execute_pattern_query_with_files()` (lines 789-848) to skip file tabulation phase.

## Execution Context

Location: `tools/cq/query/execution_context.py` (26 lines)

Bundled context for query execution:

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

Extension filtering is **authoritative**: when `lang=rust`, only `.rs` files are scanned. When `lang=python`, only `.py`/`.pyi` files are scanned. When `lang=auto`, the union is scanned and results are partitioned by detected language.

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

The `parse_metavariables()` function (lines 16-80) extracts captures from ast-grep JSON output:

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

The `apply_metavar_filters()` function (lines 113-141) validates captures against filter specs:

```python
for filter_spec in filters:
    capture = captures.get(filter_spec.name)
    if capture is None:
        return False  # Filter references missing metavar

    if not filter_spec.matches(capture):
        return False  # Regex match failed

return True  # All filters passed
```

The `MetaVarFilter.matches()` method (lines 208-224) applies regex with optional negation:

```python
def matches(self, capture: MetaVarCapture) -> bool:
    match = bool(re.search(self.pattern, capture.text))
    return not match if self.negate else match
```

## Architectural Observations for Improvement Proposals

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