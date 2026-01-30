---
name: cq
description: High-signal code queries (impact, calls, imports, exceptions, sig-impact, side-effects, scopes, async-hazards, bytecode-surface)
allowed-tools: Bash
---

# Code Query (cq) Skill

Use this skill for high-recall, structured repository analysis before proposing changes.
The cq tool provides markdown-formatted analysis injected directly into context.

## Phase 1 Commands

### Impact Analysis
Traces data flow from a function parameter to identify downstream consumers and impacts.

Results: !`./scripts/cq impact "$1" --param "$2" --root .`
Usage: /cq impact <FUNCTION_NAME> --param <PARAM_NAME>

Example: /cq impact build_graph_product --param repo_root

### Call Census
Finds all call sites for a function with argument shape analysis, keyword usage, and forwarding patterns.

Results: !`./scripts/cq calls "$1" --root .`
Usage: /cq calls <FUNCTION_NAME>

Example: /cq calls DefIndex.build

### Import Analysis
Analyzes module import structure. Use --cycles to detect import cycles.

Results: !`./scripts/cq imports --cycles --root .`
Usage: /cq imports [--cycles] [--module <MODULE>]

Example: /cq imports --cycles

### Exception Analysis
Analyzes exception handling patterns, identifies uncaught exceptions and bare except clauses.

Results: !`./scripts/cq exceptions --root .`
Usage: /cq exceptions [--function <FUNCTION>]

Example: /cq exceptions

## Phase 2 Commands

### Signature Impact Analysis
Simulates a signature change and classifies call sites as would_break, ambiguous, or ok.

Results: !`./scripts/cq sig-impact "$1" --to "$2" --root .`
Usage: /cq sig-impact <FUNCTION_NAME> --to "<new_signature>"

Example: /cq sig-impact _find_repo_root --to "_find_repo_root(start: Path | None = None, *, strict: bool = False)"

### Side Effects Analysis
Detects import-time side effects: top-level function calls, global mutations, ambient state access.

Results: !`./scripts/cq side-effects --root .`
Usage: /cq side-effects [--max-files <N>]

Example: /cq side-effects --max-files 500

### Scopes Analysis
Uses symtable to analyze scope capture for closures: free vars, cell vars, globals, nonlocals.

Results: !`./scripts/cq scopes "$1" --root .`
Usage: /cq scopes <FILE_OR_SYMBOL>

Example: /cq scopes tools/cq/macros/impact.py

### Async Hazards Analysis
Finds blocking calls (time.sleep, requests.*, subprocess.*) inside async functions.

Results: !`./scripts/cq async-hazards --root .`
Usage: /cq async-hazards [--profiles "<blocking_patterns>"]

Example: /cq async-hazards --profiles "redis.get,mysql.execute"

### Bytecode Surface Analysis
Analyzes bytecode via dis module for hidden dependencies: globals, attributes, constants.

Results: !`./scripts/cq bytecode-surface "$1" --root .`
Usage: /cq bytecode-surface <FILE_OR_SYMBOL> [--show <globals,attrs,constants,opcodes>]

Example: /cq bytecode-surface tools/cq/macros/calls.py --show globals,attrs

## Output Format

All commands output:
- **Summary**: Key metrics at a glance
- **Key Findings**: Actionable insights
- **Sections**: Organized findings by category
- **Evidence**: Supporting details (truncated)
- **Artifacts**: JSON artifacts saved to .cq/artifacts/

## Options

All commands support:
- `--root <PATH>`: Repository root (default: auto-detect)
- `--format <md|json|both>`: Output format (default: md)
- `--no-save-artifact`: Skip saving JSON artifact
- `--impact <low,med,high>`: Filter by impact bucket (comma-separated)
- `--confidence <low,med,high>`: Filter by confidence bucket (comma-separated)
- `--include <pattern>`: Include files matching pattern (glob or ~regex, repeatable)
- `--exclude <pattern>`: Exclude files matching pattern (glob or ~regex, repeatable)
- `--limit <N>`: Maximum number of findings

## Scoring

Each finding includes impact and confidence scores:

**Impact** (0.0-1.0): Weighted formula based on:
- Sites affected (45%)
- Files affected (25%)
- Propagation depth (15%)
- Breaking changes (10%)
- Ambiguous cases (5%)

**Confidence** (0.0-1.0): Based on evidence kind:
- `resolved_ast` = 0.95 (full AST analysis)
- `bytecode` = 0.90 (bytecode inspection)
- `heuristic` = 0.60 (pattern matching)
- `rg_only` = 0.45 (grep-only results)
- `unresolved` = 0.30 (unverified)

**Buckets**: high (≥0.7), med (≥0.4), low (<0.4)

## Filtering Examples

```bash
# High-impact, high-confidence findings in core modules
/cq impact build_graph_product --param repo_root --impact high --confidence high --include "src/"

# Exclude tests and limit results
/cq exceptions --exclude "tests/" --limit 100

# Multiple filters combined
/cq calls DefIndex.build --impact med,high --include "src/relspec/" --exclude "*test*"

# Use regex pattern (prefix with ~)
/cq side-effects --include "~src/(extract|normalize)/.*\\.py$"

# Filter by multiple impact levels
/cq async-hazards --impact high,med --confidence high
```

## When to Use

Use `/cq` before:
- Refactoring a function (check callers with `/cq calls`)
- Modifying a parameter (trace impact with `/cq impact`)
- Changing a function signature (test viability with `/cq sig-impact`)
- Investigating import issues (detect cycles with `/cq imports --cycles`)
- Improving error handling (analyze with `/cq exceptions`)
- Understanding test isolation issues (check `/cq side-effects`)
- Extracting nested functions (analyze captures with `/cq scopes`)
- Reviewing async code for blocking (use `/cq async-hazards`)
- Finding hidden dependencies (use `/cq bytecode-surface`)
