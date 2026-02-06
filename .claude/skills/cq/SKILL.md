---
name: cq
description: High-signal code queries for Python and Rust (impact, calls, imports, exceptions, sig-impact, side-effects, scopes, bytecode-surface, pattern queries, scope filtering, visualization)
allowed-tools: Bash
---

# Code Query (cq) Skill

Use this skill for high-recall, structured repository analysis before proposing changes.
The cq tool provides markdown-formatted analysis injected directly into context.
Canonical behavior and output semantics are documented in
`/home/paul/CodeAnatomy/.claude/skills/cq/reference/cq_reference.md`.

## Quick Reference

| Task | Command |
|------|---------|
| **Search for code** | `/cq search <query>` |
| Search with regex | `/cq search "pattern.*" --regex` |
| Search in directory | `/cq search <query> --in src` |
| Search Rust code | `/cq search <query> --lang rust` |
| Find callers | `/cq calls <fn>` |
| Trace parameter | `/cq impact <fn> --param <p>` |
| Check signature change | `/cq sig-impact <fn> --to "<sig>"` |
| Report bundle | `/cq report refactor-impact --target function:foo` |
| Multi-step run | `/cq run --steps '[{"type":"q","query":"entity=function name=foo"},{"type":"calls","function":"foo"}]'` |
| Plan file execution | `/cq run --plan analysis.toml` |
| Command chaining | `/cq chain q "entity=function name=foo" AND calls foo` |
| Pattern search (AST) | `/cq q "pattern='getattr(\$X, \$Y)'"` |
| Entity query | `/cq q "entity=function name=<name>"` |
| Rust entity query | `/cq q "entity=function name=<name> lang=rust"` |
| Context search | `/cq q "entity=function inside='class <C>'"` |
| Find closures | `/cq q "entity=function scope=closure"` |
| Visualize calls | `/cq q "entity=function name=<fn> expand=callers" --format mermaid` |
| Security patterns | `/cq q "pattern='eval(\$X)'"` |

## Decision Guide

Use CQ first for code discovery and structural analysis:
- Use `/cq search` for code discovery instead of `rg`/`grep`.
- Use `/cq q "pattern=..."` for AST-exact matching.
- Use `/cq calls` or `/cq impact` before refactors.
- Use `/cq run` for multi-step workflows to avoid repeated scans.
- Use `rg` only for non-Python/non-Rust assets or explicit raw text needs.

## Rust Language Support

cq supports searching and querying Rust code (`.rs` files) alongside Python.
Language scope defaults to `auto` (search both Python and Rust); use `--lang rust` or
`lang=rust` to narrow scope.

Scope is extension-authoritative:
- `python` => `.py`, `.pyi`
- `rust` => `.rs`
- `auto` => union

## Contract and Runtime Policy

- Use `msgspec.Struct` for CQ serialized contracts that move between modules
  (`tools/cq/search/contracts.py`, `tools/cq/search/enrichment/contracts.py`).
- Keep runtime parser/cache objects as runtime-only types; do not force them
  into serialized contract models.
- In CQ hot paths (`tools/cq/search`, `tools/cq/query`, `tools/cq/run`), avoid
  `pydantic`; use typed runtime objects and boundary serialization instead.
- Use frozen `CqStruct` request objects (`tools/cq/search/requests.py`,
  `tools/cq/core/requests.py`) for internal operation input contracts.
- `PythonAnalysisSession` (`tools/cq/search/python_analysis_session.py`) caches
  per-file analysis artifacts (ast-grep, AST, symtable, LibCST, tree-sitter) keyed
  by content hash. Maximum 64 cached sessions.

### Supported Commands for Rust

| Command | Rust Support | How to Use |
|---------|-------------|------------|
| `search` | Yes | `--lang rust` flag |
| `q` (entity) | Yes | `lang=rust` in query string |
| `q` (pattern) | Yes | `lang=rust` in query string |
| `run` / `chain` | Yes | `lang` field in step specs |
| `calls` | No | Python-only (AST/symtable) |
| `impact` | No | Python-only (AST/symtable) |
| `sig-impact` | No | Python-only (AST/symtable) |
| `imports` | No | Python-only (ast module) |
| `scopes` | No | Python-only (symtable) |
| `bytecode-surface` | No | Python-only (dis module) |
| `side-effects` | No | Python-only |
| `exceptions` | No | Python-only |

### Rust Entity Types

| Query Entity | Matches in Rust |
|-------------|-----------------|
| `function` | `fn` declarations (`function_item`) |
| `class` | `struct`, `enum`, `trait` definitions |
| `method` | `fn` inside `impl` blocks |
| `callsite` | Function/method call expressions |
| `import` | `use` declarations |
| `decorator` | Not applicable (empty) |

### Rust Examples

```bash
# Search Rust code for a symbol
/cq search register_udf --lang rust

# Find all Rust function definitions
/cq q "entity=function lang=rust"

# Find Rust structs/enums/traits by name
/cq q "entity=class name=~^Arrow lang=rust"

# Find Rust use declarations
/cq q "entity=import lang=rust in=rust"

# Pattern search in Rust code
/cq q "pattern='pub fn \$F(\$$$) -> \$R' lang=rust"

# Find impl blocks with pattern queries
/cq q "pattern='impl \$T for \$Trait' lang=rust"

# Scoped Rust search
/cq search ScalarUDF --lang rust --in rust

# Multi-step with Rust
/cq run --steps '[{"type":"q","query":"entity=function lang=rust"},{"type":"q","query":"entity=import lang=rust in=rust"}]'
```

### Language Parity Matrix

| Feature | Python | Rust | Notes |
|---------|--------|------|-------|
| entity=function | Full | Full | |
| entity=class | Full | Full | struct/enum/trait in Rust |
| entity=import | Full | Full | use declarations |
| entity=callsite | Full | Full | incl. macro invocations |
| entity=method | Full | Full | impl-scoped in Rust |
| entity=module | None | Partial | Rust mod_item only |
| entity=decorator | Full | None | Rust has no decorators |
| Pattern queries | Full | Full | |
| Relational constraints | Full | Full | |
| Scope filtering | Full | None | Python symtable only |
| Bytecode queries | Full | None | Python dis only |
| calls macro | Full | Partial | Rust: location only |
| impact/sig-impact | Full | None | Python AST only |
| imports/exceptions | Full | None | Python only |
| side-effects/scopes | Full | None | Python only |
| bytecode-surface | Full | None | Python only |

## Smart Search (Default for Code Discovery)

Smart Search (`/cq search`) is the **primary tool for finding code**. It provides semantically-enriched results that are superior to grep for code work.

### Basic Usage

```bash
# Simple identifier search (auto-detected)
/cq search build_graph

# Regex pattern
/cq search "config.*path" --regex

# Literal string
/cq search "hello world" --literal

# Scoped to directory
/cq search CqResult --in tools/cq/core

# Include strings/comments/docstrings
/cq search build_graph --include-strings
```

### Why Smart Search > grep

| Aspect | Smart Search | grep/rg |
|--------|--------------|---------|
| Classification | Defs, calls, imports, refs | Text only |
| Grouping | By containing function | By file |
| Non-code | Hidden by default | Mixed in |
| Follow-ups | Suggested next commands | None |
| Ranking | By relevance (code > tests) | None |

### Output Sections

- **Top Contexts**: Representative match per containing function
- **Definitions**: Function/class definitions (identifier mode)
- **Imports**: Import statements
- **Callsites**: Function calls
- **Uses by Kind**: Category breakdown
- **Non-Code Matches**: Strings/comments (collapsed)
- **Hot Files**: Files with most matches
- **Suggested Follow-ups**: Next commands to explore
- **Code Facts**: Per-finding enrichment clusters (Identity, Scope, Interface, Behavior, Structure)
- **Code Overview**: Top-level query-focused summary (query/mode/scope/top symbols/top files/categories)
- **Summary/Footer diagnostics**: Capability + telemetry data (including `dropped_by_scope`)

### Enrichment Pipeline

Each Python match undergoes a 5-stage enrichment pipeline extracting complementary semantic information:

| Stage | Source | Provides |
|-------|--------|----------|
| `ast_grep` | ast-grep-py | Node kind, enclosing scope, symbol role |
| `python_ast` | Python `ast` | AST node type, scope nesting, decorator context |
| `import_detail` | Import visitor | Module path, alias resolution |
| `libcst` | LibCST providers | Qualified names, scope analysis, binding candidates |
| `tree_sitter` | tree-sitter queries | Parse quality metrics, structural patterns |

Enrichment data is organized into structured sections in the output:

| Section | What It Tells You |
|---------|-------------------|
| **meta** | Pipeline status and per-stage timing |
| **resolution** | What a symbol resolves to (qualified names, bindings, call targets) |
| **behavior** | Runtime characteristics (async, generator, exception handling) |
| **structural** | Code structure (enclosing scope, nesting depth, decorators) |
| **parse_quality** | Source file health (syntax validity, parse errors) |
| **agreement** | Cross-source confidence ("full"/"partial"/"conflict") |

**Cross-source agreement** compares results from ast_grep, libcst, and tree_sitter:
- `full` = high confidence (all sources agree)
- `partial` = reasonable confidence (some sources unavailable)
- `conflict` = manual review recommended (sources disagree)

### Code Facts in Markdown

When using `--format md`, findings include a compact **Code Facts** block before the
context snippet. Values are grouped by cluster:

- **Identity**: language, role, qualified name, binding candidates
- **Scope**: enclosing callable/class, alias chain, visibility
- **Interface**: signature, parameters, return type, attributes/decorators
- **Behavior**: async/generator, await/yield, raise/control-flow context
- **Structure**: shape-oriented fields (for example struct/enum metadata when available)

Missing values are explicit:
- `N/A — not applicable`
- `N/A — not resolved`
- `N/A — enrichment unavailable`

Interpretation guidance:
- `not applicable`: expected for this language/kind.
- `not resolved`: applicable but unresolved; narrow scope or run a structural follow-up query.
- `enrichment unavailable`: fail-open/missing enrichment; retry with focused scope if needed.

### Parallel Classification

Smart search classification runs in parallel for large result sets:
- Matches partitioned by file across up to 4 worker processes
- Uses multiprocessing `spawn` context for safety in multi-threaded parents
- Fail-open: falls back to sequential on any worker error
- Transparent to output (same results, faster execution)

### Render-Time Enrichment

For findings missing enrichment (e.g., from macro commands), cq performs on-demand enrichment at markdown render time:
- Enriches up to 9 unique files in parallel (4 workers)
- Uses multiprocessing `spawn` context
- Results cached by `(file, line, col, language)` for deduplication
- Findings beyond the file limit render without render-time enrichment

### Plain Query Fallback

Plain queries in `/cq q` automatically use Smart Search:
```bash
/cq q build_graph  # Same as: /cq search build_graph
```

### When to Use Smart Search vs Pattern Queries

| Need | Use |
|------|-----|
| Find where symbol is used | `/cq search <symbol>` |
| Find structural pattern | `/cq q "pattern='...'"` |
| Find in strings/comments | `/cq search --include-strings` |
| Find AST-exact matches | `/cq q "pattern='...'"` |

## Multi-Step Execution

Execute multiple queries with a single repo scan for improved performance.

**Run/Chain Notes**
- `--step` and `--steps` accept JSON in a single token (quote the entire JSON).
- `cq run` continues on error by default; `--stop-on-error` fails fast.
- Merged results include `details.data["source_step"]` and `details.data["source_macro"]`.
- `cq chain` uses a delimiter token (default `AND`); quote multi-word queries.

### cq run (JSON/TOML Plans)

```bash
# Inline JSON steps (agent-friendly)
/cq run --steps '[{"type":"search","query":"build_graph"},{"type":"q","query":"entity=function name=build_graph"},{"type":"calls","function":"build_graph"}]'

# From TOML plan file
/cq run --plan docs/plans/refactor_check.toml

# Mixed: plan + inline steps
/cq run --plan base.toml --step '{"type":"impact","function":"foo","param":"x"}'
```

### cq chain (Command Chaining)

```bash
# Default AND delimiter
/cq chain q "entity=function name=build_graph" AND calls build_graph AND search build_graph

# Custom delimiter
/cq chain q "..." OR calls foo --delimiter OR
```

### Step Types

| Type | Required Fields | Optional Fields |
|------|-----------------|-----------------|
| `q` | `query` | `id` |
| `search` | `query` | `regex`, `literal`, `include_strings`, `in_dir`, `id` |
| `calls` | `function` | `id` |
| `impact` | `function`, `param` | `depth`, `id` |
| `imports` | — | `cycles`, `module`, `id` |
| `exceptions` | — | `function`, `id` |
| `sig-impact` | `symbol`, `to` | `id` |
| `side-effects` | — | `max_files`, `id` |
| `scopes` | `target` | `max_files`, `id` |
| `bytecode-surface` | `target` | `show`, `max_files`, `id` |

### When to Use Multi-Step

| Scenario | Approach |
|----------|----------|
| Ad-hoc multi-query | `/cq chain cmd1 AND cmd2 AND cmd3` |
| Repeatable workflow | `/cq run --plan workflow.toml` |
| Agent automation | `/cq run --steps '[...]'` |
| Pre-refactoring checklist | Create TOML plan, run before changes |

### TOML Plan File Format

```toml
version = 1
in_dir = "src/"           # Optional global scope
exclude = ["tests/"]      # Optional global excludes

[[steps]]
type = "search"
query = "build_graph"

[[steps]]
type = "q"
query = "entity=function name=build_graph expand=callers"

[[steps]]
type = "calls"
function = "build_graph"
```

### Performance Benefit

Multiple `q` steps share a **single repo scan** instead of scanning once per query. This provides significant speedup when running multiple entity queries against the same codebase.

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

### Filters

- `--include`, `--exclude` (glob or `~regex`)
- `--impact` (low,med,high)
- `--confidence` (low,med,high)
- `--severity` (error,warning,info)
- `--limit` (max findings)

### Output Formats

| Format | Description |
|--------|-------------|
| `md` | Markdown (default) - optimized for Claude and Codex context |
| `json` | Full JSON - for programmatic use |
| `both` | Markdown followed by JSON |
| `summary` | Condensed single-line - for CI |
| `mermaid` | Mermaid flowchart - call graphs |
| `mermaid-class` | Mermaid class diagram |
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
export CQ_PY_ENRICHMENT_CROSSCHECK=1
export CQ_RUST_ENRICHMENT_CROSSCHECK=1
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
| `search` | Smart code search with enrichment | `/cq search build_graph` |
| `impact` | Trace data flow from a parameter | `/cq impact build_graph_product --param repo_root` |
| `calls` | Census all call sites for a function | `/cq calls build_graph_product` |
| `sig-impact` | Test signature change viability | `/cq sig-impact foo --to "foo(a, *, b=None)"` |
| `imports` | Analyze import structure/cycles | `/cq imports --cycles` |
| `exceptions` | Analyze exception handling | `/cq exceptions` |
| `side-effects` | Detect import-time side effects | `/cq side-effects` |
| `scopes` | Analyze closure captures | `/cq scopes path/to/file.py` |
| `bytecode-surface` | Analyze bytecode dependencies | `/cq bytecode-surface file.py` |
| `q` | Declarative entity queries | `/cq q "entity=import name=Path"` |
| `run` | Multi-step execution (shared scan) | `/cq run --steps '[{"type":"q","query":"entity=function name=foo"},{"type":"calls","function":"foo"}]'` |
| `chain` | Command chaining frontend | `/cq chain q "entity=function name=foo" AND calls foo` |

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

The context snippet uses smart block selection to show the most relevant code:
- **Function header**: The `def` line and signature are always included
- **Docstring skipping**: Leading docstrings are omitted to focus on logic
- **Anchor block**: Lines around the actual call site are prioritized
- **Omission markers**: Gaps show `# ... omitted (N lines) ...` instead of truncating

This ensures the call site is always visible in context, even in long functions, rather than being truncated away.

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

Query bytecode instructions directly using Python's `dis` module (Python-only).

**Instruction Filters:**

| Filter | Description | Example |
|--------|-------------|---------|
| `bytecode.opname=X` | Exact opcode match | `bytecode.opname=LOAD_GLOBAL` |
| `bytecode.opname=~regex` | Opcode regex match | `bytecode.opname=~^CALL` |
| `bytecode.is_jump_target=true` | Jump target instructions | Branch points |
| `bytecode.stack_effect>=N` | Stack effect filter | Net stack changes |

**Examples:**
```bash
# Find functions using LOAD_GLOBAL
/cq q "entity=function bytecode.opname=LOAD_GLOBAL"

# Find functions with any CALL opcode
/cq q "entity=function bytecode.opname=~^CALL"

# Find complex control flow (many basic blocks)
/cq q "entity=function fields=basic_block_count" | grep -E "blocks: [0-9]{2,}"
```

### q - Declarative Entity Queries

The `q` command provides a composable query DSL for finding code entities.

Results: !`./scripts/cq q "$1" --root .`
Usage: /cq q "<query_string>"

**Query Syntax:** `entity=TYPE [name=PATTERN] [in=DIR] [expand=KIND] [fields=FIELDS] [limit=N]`

> **Note:** `limit=N` is enforced for pattern queries only. Entity queries do not currently cap results via `limit`.

**Entity Types (Python, default):**
| Entity | Finds | Notes |
|--------|-------|-------|
| `function` | Function definitions | |
| `class` | Class definitions | |
| `import` | Import statements (all forms) | |
| `callsite` | Function/method call sites | |
| `method` | Function definitions (not class-context validated) | Matches all functions, not just methods |
| `module` | Not implemented | Always returns empty results |
| `decorator` | Decorated definitions | See Decorator Queries section |

**Entity Types (Rust, with `lang=rust`):**
| Entity | Finds |
|--------|-------|
| `function` | `fn` declarations |
| `class` | `struct`, `enum`, `trait` definitions |
| `method` | `fn` inside `impl` blocks |
| `import` | `use` declarations |
| `callsite` | Call expressions |

**Name Patterns:**
- Exact: `name=build_graph_product`
- Regex: `name=~^test_.*` (prefix with `~`)

**Scope:**
- `in=src/semantics` - Search only in directory
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
/cq q "entity=function name=~^build expand=callers in=src"

# Find class definitions with definition metadata
/cq q "entity=class in=src/semantics fields=def"

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

Filter functions by scope characteristics using Python's symtable (Python-only).

| Filter | Description |
|--------|-------------|
| `scope=closure` | Functions that capture variables |
| `scope=nested` | Functions nested inside other functions |
| `scope=toplevel` | Top-level (module-level) functions |
| `captures=var_name` | Functions capturing specific variable |

**Examples:**
```bash
# Find all closures
/cq q "entity=function scope=closure"

# Find closures in a specific directory
/cq q "entity=function scope=closure in=src/semantics"

# Find functions capturing a specific variable
/cq q "entity=function captures=config"
```

### Decorator Queries

Find decorated definitions using `entity=decorator`.

> **Note:** `decorated_by` and `decorator_count_*` filters only take effect with `entity=decorator`. They are silently ignored for `entity=function` or `entity=class`.

| Syntax | Description |
|--------|-------------|
| `entity=decorator` | Find decorated definitions |
| `decorated_by=dataclass` | Filter to specific decorator |
| `decorator_count_min=2` | Filter by decorator count |

**Examples:**
```bash
# Find all decorated definitions
/cq q "entity=decorator"

# Find definitions with @pytest.fixture
/cq q "entity=decorator decorated_by=fixture"

# Find definitions with 3+ decorators
/cq q "entity=decorator decorator_count_min=3"

# Alternative: use pattern queries for decorator search on functions
/cq q "pattern='@dataclass' inside='class \$C'"
```

### Join Queries (Cross-Entity Relationships) — Planned

> **Status: Planned.** Join syntax is parsed but join filters are not yet enforced at runtime. Queries with join clauses will return unfiltered results. Use `expand=callers` or pattern queries for relationship analysis until joins are implemented.

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
/cq q "entity=class in=src/semantics" --format mermaid-class

# Export for Graphviz
/cq q "entity=function expand=callees" --format dot > graph.dot
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

## Enrichment Telemetry

Smart search summary includes pipeline performance data:

| Metric | Description |
|--------|-------------|
| `summary.enrichment_telemetry.python.applied` | Python findings that received enrichment |
| `summary.enrichment_telemetry.python.degraded` | Python findings with partial enrichment |
| `summary.enrichment_telemetry.python.skipped` | Python findings where enrichment was skipped |
| `summary.enrichment_telemetry.python.stages.*` | Per-stage applied/degraded/skipped counts and timing |
| `summary.enrichment_telemetry.rust.applied` | Rust findings that received enrichment |
| `summary.enrichment_telemetry.rust.degraded` | Rust findings with partial enrichment |
| `summary.enrichment_telemetry.rust.skipped` | Rust findings where enrichment was skipped |

Per-stage breakdown covers: `ast_grep`, `python_ast`, `import_detail`, `libcst`, `tree_sitter`.

Use telemetry to diagnose enrichment issues: high degraded counts suggest parse errors, high skipped counts suggest missing dependencies, and stage timing identifies bottlenecks.

## Markdown Output Structure

The markdown output follows this ordering:

1. **Title** - Command and target
2. **Code Overview** - Query-focused overview (code-informative, not diagnostics-first)
3. **Key Findings** - Top-level actionable insights
4. **Sections** - Organized finding groups with per-finding Code Facts
5. **Evidence** - Supporting details
6. **Artifacts** - Saved JSON artifact references
7. **Summary** - Single ordered JSON line with priority-keyed metrics

The summary appears at the end as a compact JSON line:
```
{"macro":"search","total_matches":45,"definitions":3,"callsites":12,...}
```

Summary keys are ordered by priority: core metrics first, then classification breakdown, enrichment telemetry, and timing data.

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
| Quick entity census | `/cq q "entity=class in=src"` |
| Finding structural patterns | `/cq q "pattern='getattr(\$X, \$Y)'"` |
| Context-aware search | `/cq q "entity=function inside='class Config'"` |
| Closure investigation | `/cq q "entity=function scope=closure"` |
| Understanding code flow | `/cq q "entity=function expand=callers" --format mermaid` |
| Decorator analysis | `/cq q "entity=decorator decorated_by=fixture"` |
| Security pattern queries | `/cq q "pattern='eval(\$X)'"` |
| Searching Rust code | `/cq search <query> --lang rust` |
| Finding Rust entities | `/cq q "entity=function lang=rust"` |
| Rust structural patterns | `/cq q "pattern='impl \$T' lang=rust"` |

## Artifacts

JSON artifacts saved to `.cq/artifacts/` by default.

Options:
- `--artifact-dir <path>` - Custom location
- `--no-save-artifact` - Skip saving
