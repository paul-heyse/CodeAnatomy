---
name: ast-grep
description: Structural code search and transformation using AST patterns - zero false positives from strings/comments
allowed-tools: Bash, Read
---

# ast-grep Skill

Use this skill for **structural** code search and transformation. Unlike text-based tools, ast-grep understands code syntax and won't match strings, comments, or unrelated code.

Installed version: **0.40.5**

## When to Use ast-grep

**Use ast-grep instead of grep/ripgrep when:**
- Finding function calls (avoids string literals, comments)
- Finding patterns with variable parts (arguments, receivers)
- Refactoring/renaming with structural awareness
- Enforcing code patterns across codebase

**Use ripgrep when:**
- Simple text search (faster for literals)
- Searching in non-code files
- Regex patterns that don't need AST awareness

## Quick Start

**Ad-hoc search:**
```bash
ast-grep run -p 'function_name($$$ARGS)' -l python .
```

**Search with rewrite preview:**
```bash
ast-grep run -p 'old_func($A)' -r 'new_func($A)' -l python .
```

**Apply all rewrites:**
```bash
ast-grep run -p 'old_func($A)' -r 'new_func($A)' -l python -U .
```

## Pattern Syntax

### Metavariables

| Pattern | Matches | Example |
|---------|---------|---------|
| `$VAR` | Single named AST node | `foo($X)` matches `foo(1)`, `foo(a+b)` |
| `$$$VAR` | Zero or more nodes | `foo($$$)` matches `foo()`, `foo(a,b,c)` |
| `$_VAR` | Non-capturing (faster, no equality) | `$_FUNC($_ARG)` matches any 1-arg call |
| `$$VAR` | Unnamed nodes (operators, punctuation) | Use in rules to capture `+`, `:`, etc. |

**Metavariable equality:** Same captured name = same value
```bash
# Matches: a == a, x.y == x.y (but NOT: a == b)
ast-grep run -p '$A == $A' -l python .
```

**Non-capturing `$_` does NOT enforce equality:**
```bash
# Matches ANY one-arg call, even foo(bar) where callee != arg
ast-grep run -p '$_FUNC($_ARG)' -l python .
```

### Multi-meta `$$$` behavior

`$$$` matches **zero or more** nodes in list positions (arguments, parameters, statements). It uses non-greedy anchor-based matching: it consumes nodes until the next AST node in your pattern can match. Place an "anchor" node after `$$$` to limit consumption.

## Core Recipes

### Find function calls
```bash
# Any call to subprocess.run with any arguments
ast-grep run -p 'subprocess.run($$$)' -l python .

# Specific method call pattern
ast-grep run -p '$OBJ.execute($$$)' -l python .

# Dynamic attribute access
ast-grep run -p 'getattr($X, $Y)' -l python .
```

### Find function definitions

**Important:** `def $NAME($$$):` does not reliably parse in tree-sitter Python because `$` in identifier position causes parse errors. Use these approaches instead:

```bash
# Find a specific function by name
ast-grep run -p 'def build_cpg($$$)' -l python .

# Find ALL function definitions using kind (most reliable)
ast-grep scan --inline-rules "$(cat <<'YAML'
id: find-funcs
language: Python
rule:
  kind: function_definition
severity: info
message: Found function
YAML
)" .

# Find functions matching a name pattern
ast-grep scan --inline-rules "$(cat <<'YAML'
id: find-build-funcs
language: Python
rule:
  kind: function_definition
  has:
    field: name
    regex: ^build
severity: info
message: Found build function
YAML
)" .
```

### Rename across codebase
```bash
ast-grep run -p 'old_name($$$ARGS)' -r 'new_name($$$ARGS)' -l python -U .
```

### Find exception handlers
```bash
# Single-exception except clauses
ast-grep run -p 'except $EXC:' -l python .

# Note: except $EXC: matches only single exceptions (e.g. except ValueError:)
# It will NOT match except (TypeError, ValueError): because $EXC is one node.
# For multi-exception, use kind-based matching:
ast-grep scan --inline-rules "$(cat <<'YAML'
id: find-except
language: Python
rule:
  kind: except_clause
severity: info
message: Found except clause
YAML
)" .
```

### Find raise statements
```bash
# Any raise with arguments
ast-grep run -p 'raise $EXC($$$)' -l python .

# Raise without arguments (re-raise)
ast-grep run -p 'raise' -l python .
```

### Find relational patterns (X inside Y)
```bash
# Blocking calls inside async functions
ast-grep scan --inline-rules "$(cat <<'YAML'
id: blocking-in-async
language: Python
rule:
  pattern: time.sleep($$$)
  inside:
    pattern: async def $FUNC($$$)
    stopBy: end
severity: warning
message: Blocking call in async function
YAML
)" .
```

### Find with context constraints
```bash
# Assert statements inside functions
ast-grep scan --inline-rules "$(cat <<'YAML'
id: find-asserts
language: Python
rule:
  pattern: assert $COND
  inside:
    kind: function_definition
    stopBy: end
severity: info
message: Assert in function
YAML
)" .
```

## Path Scoping with `--globs`

Use `--globs` to include/exclude paths. Multiple globs allowed; `!` prefix excludes; later globs win.

```bash
# Scope to a specific directory
ast-grep run -p 'raise $EXC($$$)' -l python --globs 'src/utils/**/*.py' .

# Exclude test directories
ast-grep run -p 'raise $EXC($$$)' -l python --globs '*.py' --globs '!tests/**' .

# Multiple includes + excludes
ast-grep run -p '$OBJ.execute($$$)' -l python \
  --globs 'src/**/*.py' \
  --globs '!src/**/vendor/**' \
  .
```

`--globs` always overrides `.gitignore` and other ignore logic.

## Match Strictness

`--strictness` controls how exact the match is. Default is `smart`.

| Value | Behavior | Use When |
|-------|----------|----------|
| `cst` | Match all nodes exactly | Need exact punctuation/whitespace matching |
| `smart` | Match all except trivial source nodes | **Default** - good for most searches |
| `ast` | Match only named AST nodes | Getting too many matches from trivial diffs |
| `relaxed` | Match AST nodes except comments | Want to ignore comment differences |
| `signature` | Match AST nodes, ignore text content | Structural shape matching only |
| `template` | Match text only, ignore node kinds | Loose text-level matching (0.40+) |

```bash
# Tighten matching if over-matching
ast-grep run -p 'foo($X)' --strictness ast -l python .

# Loosen matching for fuzzy structural search
ast-grep run -p 'foo($X)' --strictness relaxed -l python .
```

## Rule Building Blocks

Inline rules (`scan --inline-rules`) give access to the full rule object model. A rule object is an **implicit AND** over its fields: the node must satisfy ALL present fields.

### Atomic Rules (properties of a node)

#### `kind` - Match by AST node type
The most reliable way to find Python constructs:
```yaml
rule:
  kind: function_definition    # functions (sync and async)
  # kind: class_definition     # classes
  # kind: decorator            # decorators
  # kind: return_statement     # return statements
  # kind: except_clause        # except handlers
  # kind: import_statement     # import statements
  # kind: call                 # function calls
```

Use `--debug-query=ast` to discover node kinds for any code pattern.

#### `regex` - Filter by node text
Applies a Rust regex to the node's full text. Combine with `kind` for precision:
```yaml
rule:
  kind: identifier
  regex: ^_[a-z]              # match private identifiers
```

**Note:** Rust regex lacks lookaround and backreferences.

#### `pattern` - Match by code structure
Either a string or an object with `context` + `selector` + `strictness`:
```yaml
# Simple string pattern
rule:
  pattern: getattr($X, $Y)

# Object pattern (when string is ambiguous/incomplete)
rule:
  pattern:
    context: 'class A { $FIELD = $INIT }'
    selector: field_definition
    strictness: relaxed
```

Use pattern objects when your snippet doesn't parse correctly as standalone code. The `context` provides surrounding syntax; `selector` picks the sub-node you want to match.

### Relational Rules (node relative to another)

#### `inside` - Target is inside (ancestor of) a matching node
```yaml
rule:
  pattern: time.sleep($$$)
  inside:
    pattern: async def $FUNC($$$)
    stopBy: end
```

#### `has` - Target has a descendant matching the sub-rule
```yaml
rule:
  kind: function_definition
  has:
    field: name           # scope to the 'name' field only
    regex: ^build
```

#### `precedes` / `follows` - Sibling order
```yaml
# Match return statements that follow a raise (dead code)
rule:
  kind: return_statement
  follows:
    kind: raise_statement
    stopBy: neighbor
```

#### `field` option (inside/has only)
Scopes the search to a specific named child field of the node. Prevents matching in the wrong position:
```yaml
# Match only function NAMES starting with _, not body content
rule:
  kind: function_definition
  has:
    field: name
    regex: ^_[a-z]
```

#### `stopBy` - Control search depth
All relational rules accept `stopBy`:

| Value | Behavior |
|-------|----------|
| `neighbor` | **Default** - look only one step (direct child/parent/sibling) |
| `end` | Search all the way to root/leaf/edge |
| `{rule}` | Stop when this rule matches a node |

```yaml
# Search all descendants (not just direct children)
has:
  stopBy: end
  pattern: await $X

# Stop at function boundaries when searching ancestors
inside:
  stopBy: { kind: function_definition }
  pattern: class $CLS
```

### Composite Rules (logic + reuse)

#### `all` - AND with guaranteed order
```yaml
rule:
  all:
    - pattern: $X = $Y        # captures $X first
    - inside:                   # then uses $X in context
        pattern: def $X($$$)
        stopBy: end
```

**Critical:** Use `all:` (not bare fields) when metavariable capture in one sub-rule is referenced by a later sub-rule. Bare rule fields are unordered; `all:` guarantees evaluation order.

#### `any` - OR alternatives
```yaml
rule:
  any:
    - pattern: eval($$$)
    - pattern: exec($$$)
    - pattern: compile($$$)
```

#### `not` - Negation
```yaml
# Find raises that are NOT ValueError
rule:
  pattern: raise $EXC($$$)
  not:
    pattern: raise ValueError($$$)
```

#### `matches` - Reuse utility rules by ID
```yaml
rule:
  matches: some.utility.rule.id
```

### `nthChild` - Positional matching (1-based, named nodes only)
```yaml
# Match the second argument
rule:
  nthChild: 2
```

Supports `An+B` formula strings, `reverse`, and `ofRule` filter. See reference for details.

## Common Pitfalls

**1. One node at a time:** Each rule tests ONE node. This never matches:
```yaml
# WRONG: asks one node to be both number AND string
has:
  all:
    - { kind: number }
    - { kind: string }
```
Correct encoding:
```yaml
# RIGHT: target has a number descendant AND a string descendant
all:
  - has: { kind: number }
  - has: { kind: string }
```

**2. `$NAME` in Python `def` position:** `def $NAME($$$)` relies on error recovery. It works for matching but `--selector function_definition` will fail. Use `kind: function_definition` in inline rules instead.

**3. Bare rule fields are unordered:** If `$A` is captured in `pattern:` and referenced in `inside:`, the `inside:` may evaluate first. Wrap in `all:` to guarantee order.

**4. `--json` requires `=` syntax:** Use `--json=stream`, NOT `--json stream`. The latter is parsed incorrectly.

**5. `except $EXC:` is single-exception only:** `$EXC` matches one AST node. For `except (A, B):` use `kind: except_clause`.

## Debugging Patterns

### Three-step convergence workflow

**Step 1: Lock language + dump parse tree**
```bash
ast-grep run -l python -p 'your_pattern' --debug-query=cst
```

**Step 2: Choose the right debug format**

| Format | Shows | Use When |
|--------|-------|----------|
| `ast` | Named nodes only | Identify node kinds for `kind:` rules |
| `cst` | Named + unnamed nodes | Suspect punctuation/operators matter |
| `sexp` | S-expression (compact) | Quick shape comparison between parses |
| `pattern` | ast-grep internal format | Debug metavariable binding |

**Step 3: Escalate if pattern parses wrong**
If `--debug-query` shows unexpected structure (ERROR nodes, wrong kind), escalate to a **pattern object** in an inline rule with `context` + `selector`:
```yaml
rule:
  pattern:
    context: 'def foo(x: int = 0): pass'
    selector: default_parameter
```

### Quick diagnostic commands
```bash
# See how a pattern parses (requires -l)
ast-grep run -l python -p 'your_pattern' --debug-query=ast

# Why are files being skipped?
ast-grep run -p 'foo($X)' -l python --inspect=summary .

# Per-file/per-rule tracing
ast-grep scan --inspect=entity .
```

`--inspect` outputs to **stderr** and does not affect match results. Capture it separately:
```bash
ast-grep scan --json=stream --inspect entity 2>inspect.log | jq '.file'
```

## Exit Codes

| Mode | Exit 0 | Exit 1 |
|------|--------|--------|
| `run` | At least one match found | No matches |
| `scan` | No error-severity findings | Error-severity findings exist |

**Note:** `scan` returns 0 for warning/info/hint findings. Only `error` severity triggers exit 1. The exit codes are inverted between `run` and `scan`.

## Project Scanning

**Run single rule file (no sgconfig.yml needed):**
```bash
ast-grep scan --rule rules/no-eval.yml .
```

**Filter project rules by ID regex:**
```bash
ast-grep scan --filter '^security\.' .
```

**Multi-rule inline rules (separate with `---`):**
```bash
ast-grep scan --inline-rules "$(cat <<'YAML'
id: find-raise-typeerror
language: Python
rule:
  pattern: raise TypeError($$$)
severity: warning
message: Found TypeError raise
---
id: find-raise-valueerror
language: Python
rule:
  pattern: raise ValueError($$$)
severity: info
message: Found ValueError raise
YAML
)" src/
```

**Runtime severity overrides (no YAML editing needed):**
```bash
# Promote rules to error for CI gating
ast-grep scan --error=security.no-eval --error=security.no-sql-injection

# Suppress a noisy rule
ast-grep scan --off=noisy.rule.id

# Set all rules to warning
ast-grep scan --warning
```

Note: severity override flags require `=` syntax.

## Output Formats

| Flag | Use Case |
|------|----------|
| `--json=stream` | NDJSON - one object per line for pipelines |
| `--json=pretty` | Human-readable JSON array |
| `--json=compact` | Single-line JSON array |
| `--format github` | GitHub Actions annotations (scan only) |
| `--format sarif` | SARIF for security tools (scan only) |
| `--files-with-matches` | File paths only (no match content) |
| `-C NUM` | Show NUM context lines around each match |
| `-A NUM` / `-B NUM` | Show lines after / before each match |
| `--report-style rich\|medium\|short` | Scan terminal verbosity (default: rich) |

### JSON match object fields

Each match in JSON output contains:
- `text` - matched source text
- `range` - `{byteOffset: {start, end}, start: {line, column}, end: {line, column}}`
- `file` - file path
- `lines` - full source line(s) containing the match
- `language` - detected language
- `metaVariables` - `{single: {NAME: {text, range}}, multi: {}, transformed: {}}`
- `replacement` - (when `--rewrite` used) the replacement text

```bash
# Extract file paths from JSON stream
ast-grep run -p 'getattr($X, $Y)' -l python --json=stream . | jq -r '.file'

# Extract captured metavariable values
ast-grep run -p 'getattr($X, $Y)' -l python --json=stream . \
  | jq '{file: .file, obj: .metaVariables.single.X.text, attr: .metaVariables.single.Y.text}'
```

## `--selector` Workflow

`--selector <KIND>` extracts a sub-node from your pattern to become the actual matcher. Use when your pattern needs context but you want matches anchored on a specific node type.

**Workflow:**
1. Dump the pattern's parse tree: `ast-grep run -l python -p '<pattern>' --debug-query=cst`
2. Identify the node kind you want from the CST output
3. Re-run with `--selector <KIND>`

```bash
ast-grep run -l python -p 'import $X' --selector import_statement .
```

**Limitation:** `--selector` in `run` mode fails when the pattern contains parse errors (e.g., `def $FUNC`). For those cases, use the inline-rule `pattern.selector` approach or `kind:` matching instead.

## Integration with cq

ast-grep provides structural precision where cq uses heuristic matching:

| cq Command | ast-grep Enhancement |
|------------|---------------------|
| `calls` | Zero false positives from strings/comments |
| `async-hazards` | Relational patterns (inside async def) |
| `exceptions` | Direct pattern matching for raise/except |
| `sig-impact` | Structural call site detection |

**Example workflow:**
```bash
# 1. Use cq for high-recall discovery
./scripts/cq calls subprocess.run --root .

# 2. Use ast-grep for precise structural matching
ast-grep run -p 'subprocess.run($$$, shell=True)' -l python .
```

## Reference

For deep dives, see `reference/ast-grep.md`:
- Section B: CLI subcommands (run, scan, test, new, lsp)
- Section C: Run mode options (pattern, rewrite, strictness, selectors)
- Section D: Scan mode options (config, rules, severity, output formats)
- Section F: Pattern syntax (metavariables, multi-meta, context/selector)
- Section G: Rule object model (atomic, relational, composite rules)
- Section O: Debugging and troubleshooting
