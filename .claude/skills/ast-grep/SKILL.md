---
name: ast-grep
description: Structural code search and transformation using AST patterns - zero false positives from strings/comments
allowed-tools: Bash, Read
---

# ast-grep Skill

Use this skill for **structural** code search and transformation. Unlike text-based tools, ast-grep understands code syntax and won't match strings, comments, or unrelated code.

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

| Pattern | Matches | Example |
|---------|---------|---------|
| `$VAR` | Single AST node | `foo($X)` matches `foo(1)`, `foo(a+b)` |
| `$$$VAR` | Zero or more nodes | `foo($$$)` matches `foo()`, `foo(a,b,c)` |
| `$_VAR` | Non-capturing (faster) | `$_FUNC($_ARG)` |

**Metavariable equality:** Same name = same value
```bash
# Matches: a == a, x.y == x.y (but NOT: a == b)
ast-grep run -p '$A == $A' -l python .
```

## Core Recipes

### Find function calls
```bash
# Any call to subprocess.run with any arguments
ast-grep run -p 'subprocess.run($$$)' -l python .

# Specific method call pattern
ast-grep run -p '$OBJ.execute($$$)' -l python .
```

### Find function definitions
```bash
ast-grep run -p 'def $NAME($$$PARAMS): $$$BODY' -l python --selector function_definition .
```

### Rename across codebase
```bash
ast-grep run -p 'old_name($$$ARGS)' -r 'new_name($$$ARGS)' -l python -U .
```

### Find exception handlers
```bash
ast-grep run -p 'except $EXC:' -l python .
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
    kind: async_function_definition
    stopBy: end
severity: warning
message: Blocking call in async function
YAML
)" .
```

### Find with context constraints
```bash
# Assert statements with specific patterns
ast-grep scan --inline-rules "$(cat <<'YAML'
id: find-asserts
language: Python
rule:
  pattern: assert $COND
  inside:
    kind: function_definition
YAML
)" .
```

## Debugging Patterns

**When pattern doesn't match - verify parse:**
```bash
ast-grep run -l python -p 'your_pattern' --debug-query=cst
```

**Common fixes:**
1. **Pattern incomplete?** Use inline-rules with `context` + `selector`
2. **Wrong node kind?** Check with `--debug-query=ast`
3. **Language mismatch?** Explicitly set `-l <lang>`

## Project Scanning

**Run single rule file:**
```bash
ast-grep scan --rule rules/no-eval.yml .
```

**Filter project rules:**
```bash
ast-grep scan --filter '^security\.' .
```

**JSON output for pipelines:**
```bash
ast-grep scan --json=stream | jq '.file'
```

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

## Output Formats

| Flag | Use Case |
|------|----------|
| `--json=stream` | Pipe to jq for processing |
| `--json=pretty` | Human-readable JSON |
| `--format github` | GitHub Actions annotations |
| `--format sarif` | SARIF for security tools |

## Reference

For deep dives, see `reference/ast-grep.md`:
- Section B: CLI subcommands (run, scan, test, new, lsp)
- Section C: Run mode options (pattern, rewrite, strictness, selectors)
- Section D: Scan mode options (config, rules, severity, output formats)
- Section F: Pattern syntax (metavariables, multi-meta, context/selector)
- Section G: Rule object model (atomic, relational, composite rules)
- Section O: Debugging and troubleshooting
