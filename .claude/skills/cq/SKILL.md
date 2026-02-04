---
name: cq
description: High-signal code queries (impact, calls, imports, exceptions, sig-impact, side-effects, scopes, bytecode-surface, pattern queries, scope filtering, visualization)
allowed-tools: Bash
---

# Code Query (cq) Skill

Use this skill for high-recall, structured repository analysis before proposing changes.
The cq tool provides markdown-formatted analysis injected directly into context.

## Quick Reference

| Task | Command |
|------|---------|
| Find callers | `/cq calls <fn>` |
| Trace parameter | `/cq impact <fn> --param <p>` |
| Check signature change | `/cq sig-impact <fn> --to "<sig>"` |
| Find entity | `/cq q "entity=function name=<name>"` |
| Pattern search | `/cq q "pattern='<ast-grep-pattern>'"` |
| Context search | `/cq q "entity=function inside='class <C>'"` |
| Find closures | `/cq q "entity=function scope=closure"` |
| Visualize calls | `/cq q "entity=function name=<fn> expand=callers" --format mermaid` |
| Security patterns | `/cq q "pattern='eval(\$X)'"` |

## Global Options

All commands support these global options:

| Option | Env Var | Default | Description |
|--------|---------|---------|-------------|
| `--root PATH` | `CQ_ROOT` | Auto-detect | Repository root |
| `--config PATH` | `CQ_CONFIG` | `.cq.toml` | Config file path |
| `--no-config` | `CQ_NO_CONFIG` | `false` | Skip config loading |
| `--verbose N` | `CQ_VERBOSE` | `0` | Verbosity (0-3) |
| `--format FMT` | `CQ_FORMAT` | `md` | Output format |
| `--artifact-dir PATH` | `CQ_ARTIFACT_DIR` | `.cq/artifacts` | Artifact directory |
| `--no-save-artifact` | `CQ_NO_SAVE_ARTIFACT` | `false` | Skip saving artifacts |

### Output Formats

| Format | Description |
|--------|-------------|
| `md` | Markdown (default) - optimized for Claude context |
| `json` | Full JSON - for programmatic use |
| `both` | Markdown followed by JSON |
| `summary` | Condensed single-line - for CI |
| `mermaid` | Mermaid flowchart - call graphs |
| `mermaid-class` | Mermaid class diagram |
| `mermaid-cfg` | Control flow graph |
| `dot` | Graphviz DOT format |

### Examples with Global Options

```bash
# Use JSON output
/cq calls build_graph --format json

# Specify custom root
/cq q "entity=function" --root /path/to/repo

# Verbose output for debugging
/cq impact foo --param bar --verbose 2

# Skip config file (use defaults only)
/cq calls foo --no-config
```

## Configuration

### Config File

Create `.cq.toml` in your repo root:

```toml
[cq]
format = "md"
verbose = 0
artifact_dir = ".cq/artifacts"
save_artifact = true
```

### Environment Variables

All options can be set via environment variables with `CQ_` prefix:

```bash
export CQ_FORMAT=json
export CQ_VERBOSE=1
export CQ_ROOT=/path/to/repo
```

### Precedence

1. CLI flags (highest)
2. Environment variables
3. Config file
4. Defaults (lowest)

## Reference Documentation

For detailed information on architecture, scoring, filtering, and troubleshooting:
- Complete reference: `reference/cq_reference.md`

## Quick Command Reference

### Analysis Commands

| Command | Purpose | Example |
|---------|---------|---------|
| `impact` | Trace data flow from a parameter | `/cq impact build_graph_product --param repo_root` |
| `calls` | Census all call sites for a function | `/cq calls build_graph_product` |
| `sig-impact` | Test signature change viability | `/cq sig-impact foo --to "foo(a, *, b=None)"` |
| `imports` | Analyze import structure/cycles | `/cq imports --cycles` |
| `exceptions` | Analyze exception handling | `/cq exceptions` |
| `side-effects` | Detect import-time side effects | `/cq side-effects` |
| `scopes` | Analyze closure captures | `/cq scopes path/to/file.py` |
| `bytecode-surface` | Analyze bytecode dependencies | `/cq bytecode-surface file.py` |
| `q` | Declarative entity queries | `/cq q "entity=import name=Path"` |

## Command Details

### impact - Parameter Flow Analysis

Traces how data flows from a parameter through the codebase.

Results: !`./scripts/cq impact "$1" --param "$2" --root .`
Usage: /cq impact <FUNCTION_NAME> --param <PARAM_NAME> [--depth N]

Example: /cq impact build_graph_product --param repo_root

### calls - Call Site Census

Finds all call sites with argument shape analysis, forwarding detection, and context enrichment.

Results: !`./scripts/cq calls "$1" --root .`
Usage: /cq calls <FUNCTION_NAME>

Example: /cq calls build_graph_product

**Output Sections:**
- Summary: total sites, files, signature preview
- Argument Shape Histogram: distribution of arg patterns
- Keyword Argument Usage: which kwargs used how often
- Calling Contexts: which functions call this one
- Hazards: dynamic dispatch patterns detected
- Call Sites: detailed list with previews and context

**Context Window Enrichment:**

Each call site includes a context window showing the containing function:

| Field | Description |
|-------|-------------|
| `context_window` | Line range (`start_line`, `end_line`) of containing function |
| `context_snippet` | Source code snippet of the containing function (truncated if >30 lines) |

The context snippet provides immediate visibility into how each call site is used, making it easier to understand argument patterns and refactoring impact without needing to read the full file.

### sig-impact - Signature Change Analysis

Classifies call sites as would_break, ambiguous, or ok for a proposed signature change.

Results: !`./scripts/cq sig-impact "$1" --to "$2" --root .`
Usage: /cq sig-impact <FUNCTION_NAME> --to "<new_signature>"

Example: /cq sig-impact _find_repo_root --to "_find_repo_root(start: Path | None = None, *, strict: bool = False)"

### imports - Import Structure Analysis

Analyzes module import structure and detects import cycles.

Results: !`./scripts/cq imports --cycles --root .`
Usage: /cq imports [--cycles] [--module <MODULE>]

Example: /cq imports --cycles

### exceptions - Exception Handling Analysis

Analyzes exception handling patterns, uncaught exceptions, and bare except clauses.

Results: !`./scripts/cq exceptions --root .`
Usage: /cq exceptions [--function <FUNCTION>]

Example: /cq exceptions

### side-effects - Import-Time Side Effects

Detects side effects at module import time (top-level calls, global mutations).

Results: !`./scripts/cq side-effects --root .`
Usage: /cq side-effects [--max-files <N>]

Example: /cq side-effects --max-files 500

### scopes - Closure Scope Analysis

Uses symtable to analyze scope capture: free vars, cell vars, globals, nonlocals.

Results: !`./scripts/cq scopes "$1" --root .`
Usage: /cq scopes <FILE_OR_SYMBOL>

Example: /cq scopes tools/cq/macros/impact.py

### bytecode-surface - Bytecode Dependency Analysis

Analyzes bytecode for hidden dependencies: globals, attributes, constants.

Results: !`./scripts/cq bytecode-surface "$1" --root .`
Usage: /cq bytecode-surface <FILE_OR_SYMBOL> [--show <globals,attrs,constants,opcodes>]

Example: /cq bytecode-surface tools/cq/macros/calls.py --show globals,attrs

### Bytecode Queries (dis Module Integration)

Query bytecode instructions directly using Python's `dis` module.

**Instruction Filters:**

| Filter | Description | Example |
|--------|-------------|---------|
| `bytecode.opname=X` | Exact opcode match | `bytecode.opname=LOAD_GLOBAL` |
| `bytecode.opname=~regex` | Opcode regex match | `bytecode.opname=~^CALL` |
| `bytecode.is_jump_target=true` | Jump target instructions | Branch points |
| `bytecode.stack_effect>=N` | Stack effect filter | Net stack changes |

**CFG Visualization:**

| Format | Description |
|--------|-------------|
| `--format mermaid-cfg` | Control flow graph as Mermaid |
| `fields=basic_block_count` | Number of basic blocks |

**Examples:**
```bash
# Find functions using LOAD_GLOBAL
/cq q "entity=function bytecode.opname=LOAD_GLOBAL"

# Find functions with any CALL opcode
/cq q "entity=function bytecode.opname=~^CALL"

# Find complex control flow (many basic blocks)
/cq q "entity=function fields=basic_block_count" | grep -E "blocks: [0-9]{2,}"

# Visualize control flow graph
/cq q "entity=function name=complex_fn" --format mermaid-cfg
```

### q - Declarative Entity Queries

The `q` command provides a composable query DSL for finding code entities.

Results: !`./scripts/cq q "$1" --root .`
Usage: /cq q "<query_string>"

**Query Syntax:** `entity=TYPE [name=PATTERN] [in=DIR] [expand=KIND] [fields=FIELDS]`

**Entity Types:**
| Entity | Finds |
|--------|-------|
| `function` | Function definitions |
| `class` | Class definitions |
| `import` | Import statements (all forms) |
| `callsite` | Function/method call sites |

**Name Patterns:**
- Exact: `name=build_graph_product`
- Regex: `name=~^test_.*` (prefix with `~`)

**Scope:**
- `in=src/semantics/` - Search only in directory
- `exclude=tests,__pycache__` - Exclude directories

**Expanders:**
- `expand=callers` - Find functions that call the target
- `expand=callees` - Find functions called by target
- `expand=callers(depth=2)` - Transitive callers

**Fields:**
- `fields=def` - Include definition metadata
- `fields=callers` - Include caller section

**Examples:**
```bash
# Find all imports of Path
/cq q "entity=import name=Path"

# Find functions matching pattern with callers
/cq q "entity=function name=~^build expand=callers in=src/"

# Find class definitions with definition metadata
/cq q "entity=class in=src/semantics/ fields=def"

# Show query plan explanation
/cq q "entity=function name=compile explain=true"
```

### Pattern Queries (Structural Search)

Pattern queries use ast-grep syntax for structural code matching without false positives from strings/comments.

| Syntax | Description |
|--------|-------------|
| `pattern='$X = getattr($Y, $Z)'` | Match getattr assignments |
| `pattern='def $F($$$)'` | Match any function definition |
| `pattern='async def $F($$$)'` | Match async functions |
| `strictness=signature` | Match signatures only |

**Examples:**
```bash
# Find dynamic attribute access
/cq q "pattern='getattr(\$X, \$Y)'"

# Find all f-string usages
/cq q "pattern='f\"\$\$\$\"'"

# Find specific decorator usage
/cq q "pattern='@dataclass'"

# Find eval/exec (security-sensitive pattern)
/cq q "pattern='eval(\$X)'"

# Find pickle.load (unsafe deserialization pattern)
/cq q "pattern='pickle.load(\$X)'"
```

### Pattern Objects (Disambiguation)

When a simple pattern has multiple interpretations (e.g., `{ "k": v }` could match dict literals or JSON pair assignments), use pattern objects with `context` and `selector` to disambiguate.

**Syntax:**
```bash
/cq q "pattern.context='<containing_pattern>' pattern.selector=<node_kind>"
```

| Field | Description |
|-------|-------------|
| `pattern.context` | The outer pattern providing context |
| `pattern.selector` | AST node kind to extract (e.g., `pair`, `argument`) |

**Examples:**
```bash
# Find JSON-style key-value pairs (not dict literals)
/cq q "pattern.context='{ \"\$K\": \$V }' pattern.selector=pair"

# Find specific argument positions in calls
/cq q "pattern.context='func(\$A, \$B)' pattern.selector=argument"

# Extract decorators from decorated functions
/cq q "pattern.context='@\$D def \$F(\$$$): \$$$' pattern.selector=decorator"
```

### Strictness Modes

Control how precisely patterns match code structure.

| Mode | Description | Use Case |
|------|-------------|----------|
| `cst` | Exact CST match (whitespace-sensitive) | Formatting-aware queries |
| `smart` | Balances precision and recall (default) | General-purpose |
| `ast` | Pure AST match (ignores whitespace, parens) | Structure-only matching |
| `relaxed` | Looser matching for exploration | Finding similar patterns |
| `signature` | Match function signatures only | API shape queries |

**Examples:**
```bash
# Match signatures only (ignore body)
/cq q "pattern='def \$F(\$A, \$B)' strictness=signature"

# AST-only (ignore formatting differences)
/cq q "pattern='x + y' strictness=ast"
```

### Meta-Variable Enhancements

Meta-variables capture code structure in patterns.

| Syntax | Name | Description |
|--------|------|-------------|
| `$X` | Single | Match exactly one AST node |
| `$$X` | Multi-named | Match zero or more nodes, bind to X |
| `$$$` | Multi-anonymous | Match zero or more nodes (no binding) |
| `$_X` | Non-capturing | Match one node, no equality enforcement |

**Equality Enforcement:**
When the same named meta-variable appears multiple times, all captures must match:
```bash
# Find self-assignment (x = x)
/cq q "pattern='\$X = \$X'"

# Find swaps (a, b = b, a)
/cq q "pattern='\$A, \$B = \$B, \$A'"
```

**Meta-Variable Filtering:**
Filter captures with regex patterns:

| Syntax | Description |
|--------|-------------|
| `$X=~pattern` | Capture must match regex |
| `$X=!~pattern` | Capture must NOT match regex |

**Examples:**
```bash
# Find getattr with string literal attribute names
/cq q "pattern='getattr(\$X, \$Y)' \$Y=~'^\"'"

# Find operators that aren't equality
/cq q "pattern='\$A \$\$OP \$B' \$\$OP=!~'^[=!]'"

# Find function calls starting with 'test_'
/cq q "pattern='\$F(\$$$)' \$F=~'^test_'"
```

### Composite Queries (all/any/not)

Combine multiple patterns with logical operators.

| Operator | Syntax | Description |
|----------|--------|-------------|
| `all` | `all='p1,p2'` | AND - all patterns must match |
| `any` | `any='p1,p2'` | OR - at least one pattern matches |
| `not` | `not='pattern'` | Exclude matches |

**Examples:**
```bash
# Find functions with both await and return
/cq q "entity=function all='await \$X,return \$Y'"

# Find any form of logging
/cq q "any='logger.\$M(\$$$),print(\$$$),console.log(\$$$)'"

# Find functions without docstrings
/cq q "entity=function not.has='\"\"\"'"

# Exclude test functions
/cq q "entity=function name=~^build not.has='@pytest'"

# Complex: async functions that don't use await
/cq q "entity=function has='async def' not.has='await'"
```

**Ordered Capture with all:**
Patterns in `all=` preserve capture order for meta-variables:
```bash
# Find where $A is used before being assigned
/cq q "all='use(\$A)', '\$A = \$B'"
```

### nthChild Positional Matching

Match elements by their position within a parent.

| Parameter | Description |
|-----------|-------------|
| `nthChild=N` | Match exactly the Nth child (1-indexed) |
| `nthChild='2n+1'` | Match odd-positioned children |
| `nthChild.reverse=true` | Count from end instead of start |
| `nthChild.ofRule='...'` | Count only matching siblings |

**Examples:**
```bash
# Find first argument in function calls
/cq q "pattern='\$F(\$ARG)' nthChild=1"

# Find last parameter in definitions
/cq q "pattern='def \$F(\$$$, \$LAST)' nthChild.reverse=true nthChild=1"

# Find every other decorator
/cq q "pattern='@\$D' nthChild='2n'"
```

### Relational Constraints (Contextual Search)

Find code in specific structural contexts.

| Constraint | Description |
|------------|-------------|
| `inside='class Config'` | Within a class |
| `has='return $X'` | Contains a pattern |
| `precedes='$X'` | Before a pattern |
| `follows='$X'` | After a pattern |

**stopBy Control:**
Control how far to search for containment:

| Mode | Syntax | Description |
|------|--------|-------------|
| `neighbor` | Default | Stop at immediate neighbors |
| `end` | `inside.stopBy=end` | Search to end of scope |
| Custom | `inside.stopBy='def \$F'` | Stop at matching pattern |

**Field-Scoped Searches:**
Limit searches to specific AST fields (only for `inside`/`has`):

| Syntax | Description |
|--------|-------------|
| `inside.field=body` | Search only in function body |
| `has.field=arguments` | Search only in function arguments |
| `has.field=returns` | Search only in return type annotation |

**Examples:**
```bash
# Find methods inside Config classes
/cq q "entity=function inside='class Config'"

# Find functions that contain getattr
/cq q "entity=function has='getattr(\$X, \$Y)'"

# Find functions inside async context managers
/cq q "entity=function inside='async with \$X'"

# Search to end of containing scope
/cq q "pattern='return \$X' inside='def \$F' inside.stopBy=end"

# Find only in function body (not decorators/annotations)
/cq q "pattern='yield \$X' inside='def \$F' inside.field=body"

# Find patterns in decorator arguments only
/cq q "pattern='\$X' has.field=arguments inside='@\$D(\$$$)'"
```

**Validation Note:** The `field` parameter only works with `inside` and `has`, not with `precedes` or `follows`.

### Scope Filtering (Closure Analysis)

Filter functions by scope characteristics using Python's symtable.

| Filter | Description |
|--------|-------------|
| `scope=closure` | Functions that capture variables |
| `scope=module` | Module-level functions |
| `scope=class` | Methods in classes |
| `captures=var_name` | Functions capturing specific variable |

**Examples:**
```bash
# Find all closures
/cq q "entity=function scope=closure"

# Find closures in a specific directory
/cq q "entity=function scope=closure in=src/semantics/"

# Find functions capturing a specific variable
/cq q "entity=function captures=config"
```

### Decorator Queries

Find decorated functions or analyze decorator usage.

| Syntax | Description |
|--------|-------------|
| `entity=decorator` | Find decorator definitions |
| `decorated_by=dataclass` | Functions with @dataclass |
| `decorator_count_min=2` | Multiple decorators |

**Examples:**
```bash
# Find all @pytest.fixture decorated functions
/cq q "entity=function decorated_by=fixture"

# Find heavily decorated functions
/cq q "entity=function decorator_count_min=3"

# Find @dataclass decorated classes
/cq q "entity=class decorated_by=dataclass"
```

### Join Queries (Cross-Entity Relationships)

Find entities by their relationships to other entities.

| Join | Description |
|------|-------------|
| `used_by=function:main` | Called by main function |
| `defines=class:Config` | Modules defining Config class |
| `raises=class:ValueError` | Functions raising ValueError |
| `exports=function:main` | Modules exporting main |

**Examples:**
```bash
# Find functions called by main
/cq q "entity=function used_by=function:main"

# Find modules that define Config class
/cq q "entity=module defines=class:Config"

# Find functions that raise ValueError
/cq q "entity=function raises=class:ValueError"
```

### Visualization Outputs

Generate visual representations of code structure.

| Format | Description | Use Case |
|--------|-------------|----------|
| `--format mermaid` | Mermaid flowchart | Call graphs |
| `--format mermaid-class` | Mermaid class diagram | Class hierarchies |
| `--format dot` | Graphviz DOT | Complex graphs |

**Examples:**
```bash
# Generate call graph
/cq q "entity=function name=build_graph expand=callers" --format mermaid

# Generate class diagram
/cq q "entity=class in=src/semantics/" --format mermaid-class

# Export for Graphviz
/cq q "entity=function expand=callees" --format dot > graph.dot

# Control flow graph visualization
/cq q "entity=function name=complex_fn" --format mermaid-cfg
```

### Security Pattern Queries

Use pattern queries to find security-sensitive constructs without a dedicated scanner.

```bash
# Dynamic code execution
/cq q "pattern='eval(\$X)'"
/cq q "pattern='exec(\$X)'"

# Unsafe deserialization
/cq q "pattern='pickle.load(\$X)'"
/cq q "pattern='yaml.load(\$X)'"

# Shell injection risks
/cq q "pattern='subprocess.\$M(\$$$, shell=True)'"
```

## Filtering & Output

### Filter Options (all commands)

| Option | Description |
|--------|-------------|
| `--impact high,med,low` | Filter by impact bucket |
| `--confidence high,med,low` | Filter by confidence bucket |
| `--severity error,warning,info` | Filter by severity |
| `--include <pattern>` | Include files (glob or `~regex`) |
| `--exclude <pattern>` | Exclude files (glob or `~regex`) |
| `--limit N` | Max findings |

### Output Formats

| Format | Flag | Use Case |
|--------|------|----------|
| Markdown | `--format md` | Claude context (default) |
| JSON | `--format json` | Programmatic use |
| Summary | `--format summary` | CI integration |

### Filtering Examples

```bash
# High-impact findings in src/
/cq impact build_graph_product --param repo_root --impact high --include "src/"

# Exclude tests
/cq exceptions --exclude "tests/" --limit 100

# Multiple filters
/cq calls build_graph_product --impact med,high --include "src/relspec/" --exclude "*test*"

# Regex pattern
/cq side-effects --include "~src/(extract|normalize)/.*\\.py$"
```

## Scoring System

### Impact Score (0.0-1.0)

Weighted formula:
- Sites affected (45%)
- Files affected (25%)
- Propagation depth (15%)
- Breaking changes (10%)
- Ambiguous cases (5%)

### Confidence Score (0.0-1.0)

Based on evidence quality:
- `resolved_ast` = 0.95 (full AST)
- `bytecode` = 0.90
- `heuristic` = 0.60
- `rg_only` = 0.45
- `unresolved` = 0.30

### Buckets

| Bucket | Threshold |
|--------|-----------|
| high | >= 0.7 |
| med | >= 0.4 |
| low | < 0.4 |

## When to Use

| Scenario | Command |
|----------|---------|
| Refactoring a function | `/cq calls <function>` |
| Modifying a parameter | `/cq impact <function> --param <param>` |
| Changing a signature | `/cq sig-impact <function> --to "<new_sig>"` |
| Import cycle issues | `/cq imports --cycles` |
| Improving error handling | `/cq exceptions` |
| Test isolation issues | `/cq side-effects` |
| Extracting nested functions | `/cq scopes <file>` |
| Finding hidden deps | `/cq bytecode-surface <file>` |
| Finding specific imports | `/cq q "entity=import name=pandas"` |
| Finding functions by pattern | `/cq q "entity=function name=~^test_"` |
| Quick entity census | `/cq q "entity=class in=src/"` |
| Finding structural patterns | `/cq q "pattern='getattr(\$X, \$Y)'"` |
| Context-aware search | `/cq q "entity=function inside='class Config'"` |
| Closure investigation | `/cq q "entity=function scope=closure"` |
| Understanding code flow | `/cq q "entity=function expand=callers" --format mermaid` |
| Decorator analysis | `/cq q "entity=function decorated_by=fixture"` |
| Security pattern queries | `/cq q "pattern='eval(\$X)'"` |

## Artifacts

JSON artifacts saved to `.cq/artifacts/` by default.

Options:
- `--artifact-dir <path>` - Custom location
- `--no-save-artifact` - Skip saving
