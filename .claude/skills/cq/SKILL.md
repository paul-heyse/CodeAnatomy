---
name: cq
description: High-signal code queries (impact, calls, imports, exceptions, sig-impact, side-effects, scopes, async-hazards, bytecode-surface)
allowed-tools: Bash
---

# Code Query (cq) Skill

Use this skill for high-recall, structured repository analysis before proposing changes.
The cq tool provides markdown-formatted analysis injected directly into context.

## Reference Documentation

For detailed information on architecture, scoring, filtering, and troubleshooting:
- Complete reference: `reference/cq_reference.md`

## Quick Command Reference

### Analysis Commands

| Command | Purpose | Example |
|---------|---------|---------|
| `impact` | Trace data flow from a parameter | `/cq impact build_graph_product --param repo_root` |
| `calls` | Census all call sites for a function | `/cq calls DefIndex.build` |
| `sig-impact` | Test signature change viability | `/cq sig-impact foo --to "foo(a, *, b=None)"` |
| `imports` | Analyze import structure/cycles | `/cq imports --cycles` |
| `exceptions` | Analyze exception handling | `/cq exceptions` |
| `side-effects` | Detect import-time side effects | `/cq side-effects` |
| `scopes` | Analyze closure captures | `/cq scopes path/to/file.py` |
| `async-hazards` | Find blocking in async | `/cq async-hazards` |
| `bytecode-surface` | Analyze bytecode dependencies | `/cq bytecode-surface file.py` |

## Command Details

### impact - Parameter Flow Analysis

Traces how data flows from a parameter through the codebase.

Results: !`./scripts/cq impact "$1" --param "$2" --root .`
Usage: /cq impact <FUNCTION_NAME> --param <PARAM_NAME> [--depth N]

Example: /cq impact build_graph_product --param repo_root

### calls - Call Site Census

Finds all call sites with argument shape analysis and forwarding detection.

Results: !`./scripts/cq calls "$1" --root .`
Usage: /cq calls <FUNCTION_NAME>

Example: /cq calls DefIndex.build

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

### async-hazards - Blocking in Async Detection

Finds blocking calls (time.sleep, requests.*, subprocess.*) inside async functions.

Results: !`./scripts/cq async-hazards --root .`
Usage: /cq async-hazards [--profiles "<blocking_patterns>"]

Example: /cq async-hazards --profiles "redis.get,mysql.execute"

### bytecode-surface - Bytecode Dependency Analysis

Analyzes bytecode for hidden dependencies: globals, attributes, constants.

Results: !`./scripts/cq bytecode-surface "$1" --root .`
Usage: /cq bytecode-surface <FILE_OR_SYMBOL> [--show <globals,attrs,constants,opcodes>]

Example: /cq bytecode-surface tools/cq/macros/calls.py --show globals,attrs

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
/cq calls DefIndex.build --impact med,high --include "src/relspec/" --exclude "*test*"

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
| Async performance issues | `/cq async-hazards` |
| Finding hidden deps | `/cq bytecode-surface <file>` |

## Artifacts

JSON artifacts saved to `.cq/artifacts/` by default.

Options:
- `--artifact-dir <path>` - Custom location
- `--no-save-artifact` - Skip saving
