# Code Query (cq) Reference Documentation

> Version 0.2.0 | Schema: cq.v1

## Overview

Code Query (cq) is a high-signal code analysis tool designed for Claude Code. It provides structured, markdown-formatted analysis that integrates directly into the conversation context. Unlike simple grep searches, cq performs semantic analysis using Python's AST, symtable, bytecode inspection, and intelligent call resolution.

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                           CLI Layer                               │
│  scripts/cq → tools/cq/cli.py → argparse + dispatch               │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                         Macro Layer                               │
│  macros/{impact,calls,imports,exceptions,sig_impact,...}.py       │
│  Each macro: Request dataclass → analysis → CqResult              │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                         Index Layer                               │
│  index/def_index.py   - Repo-wide symbol index (FnDecl, ClassDecl)│
│  index/call_resolver.py - Multi-strategy call target resolution  │
│  index/arg_binder.py  - Argument → parameter binding              │
└──────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                          Core Layer                               │
│  core/schema.py       - CqResult, Finding, Section, Anchor        │
│  core/scoring.py      - Impact/Confidence signal computation      │
│  core/findings_table.py - Polars-based filtering + rehydration    │
│  core/report.py       - Markdown rendering                        │
│  core/artifacts.py    - JSON artifact persistence                 │
│  core/toolchain.py    - External tool detection (rg, sg)          │
└──────────────────────────────────────────────────────────────────┘
```

## Schema Definitions

### CqResult (Top-level structure)

Every cq command returns a `CqResult` with these components:

| Field | Type | Description |
|-------|------|-------------|
| `run` | `RunMeta` | Invocation metadata (macro, argv, root, timing, toolchain) |
| `summary` | `dict[str, Any]` | Key metrics at a glance |
| `key_findings` | `list[Finding]` | Top-level actionable insights |
| `sections` | `list[Section]` | Organized finding groups |
| `evidence` | `list[Finding]` | Supporting details (often truncated in display) |
| `artifacts` | `list[Artifact]` | Saved JSON artifact references |

### Finding

Individual analysis findings:

| Field | Type | Description |
|-------|------|-------------|
| `category` | `str` | Finding type (e.g., "call_site", "hazard", "scope") |
| `message` | `str` | Human-readable description |
| `anchor` | `Anchor \| None` | Source location: `file:line[:col]` |
| `severity` | `str` | One of: "error", "warning", "info" |
| `details` | `dict[str, Any]` | Structured metadata including scoring |

### Anchor

Source code location:

| Field | Type | Description |
|-------|------|-------------|
| `file` | `str` | Relative file path from repo root |
| `line` | `int` | 1-indexed line number |
| `col` | `int \| None` | 0-indexed column offset |
| `end_line` | `int \| None` | End line for spans |
| `end_col` | `int \| None` | End column for spans |

---

## Scoring System

### Impact Score (0.0 - 1.0)

Computed as weighted sum of normalized signals:

| Signal | Weight | Normalizer | Description |
|--------|--------|------------|-------------|
| `sites` | 45% | /100 | Number of affected call/usage sites |
| `files` | 25% | /20 | Number of affected files |
| `depth` | 15% | /10 | Propagation depth (taint analysis) |
| `breakages` | 10% | /10 | Breaking changes count |
| `ambiguities` | 5% | /10 | Uncertain/forwarding cases |

**Severity multiplier** (optional):
- `error`: 1.5x
- `warning`: 1.0x
- `info`: 0.5x

### Confidence Score (0.0 - 1.0)

Based on evidence quality:

| Evidence Kind | Score | Description |
|---------------|-------|-------------|
| `resolved_ast` | 0.95 | Full AST analysis with symbol resolution |
| `bytecode` | 0.90 | Bytecode inspection |
| `resolved_ast_heuristic` | 0.75 | AST with heuristic matching |
| `bytecode_heuristic` | 0.75 | Bytecode with heuristics |
| `cross_file_taint` | 0.70 | Multi-file taint propagation |
| `heuristic` | 0.60 | Pattern matching only |
| `rg_only` | 0.45 | Ripgrep text search only |
| `unresolved` | 0.30 | Unverified/fallback |

### Bucket Thresholds

| Bucket | Threshold |
|--------|-----------|
| `high` | >= 0.7 |
| `med` | >= 0.4 |
| `low` | < 0.4 |

---

## Commands Reference

### impact - Parameter Taint Analysis

Traces data flow from a function parameter to identify downstream consumers.

```bash
/cq impact <FUNCTION_NAME> --param <PARAM_NAME> [--depth N]
```

**How it works:**
1. Builds `DefIndex` of all functions/classes in repo
2. Finds function by name or qualified name (`Class.method`)
3. Initializes taint set with the specified parameter
4. Walks function AST, propagating taint through:
   - Assignments (`x = param` → x is tainted)
   - Calls with tainted args → records call site
   - Returns with tainted values → marks return as tainted
5. Recursively analyzes callees where tainted args are passed
6. Also finds callers via ripgrep for broader impact

**Taint propagation rules:**
- `ast.Name` → tainted if identifier in taint set
- `ast.Attribute` → tainted if receiver is tainted
- `ast.Subscript` → tainted if base or slice is tainted
- `ast.BinOp`, `ast.UnaryOp` → tainted if operand(s) tainted
- `ast.Call` → tainted if any arg tainted
- `ast.IfExp`, `ast.Compare`, `ast.BoolOp` → tainted if any branch tainted
- `ast.JoinedStr` (f-strings) → tainted if any value tainted
- Comprehensions → tainted if iterating over tainted data

**Output sections:**
- Summary: sites, files, max depth
- Key Findings: taint propagation summary by depth
- Taint Sites by kind (assign, call, return)
- Callers via ripgrep
- Evidence

**Example:**
```bash
/cq impact build_graph_product --param repo_root --depth 5
```

---

### calls - Call Site Census

Finds all call sites for a function with argument shape analysis.

```bash
/cq calls <FUNCTION_NAME>
```

**How it works:**
1. Uses ripgrep to find candidate lines matching `function_name(`
2. Parses candidate files with AST
3. `CallFinder` visitor matches calls by name or qualified name
4. Analyzes each call for:
   - Number of positional/keyword arguments
   - Keyword argument names used
   - `*args`/`**kwargs` forwarding
   - Calling context (containing function)
   - Argument preview string

**Output sections:**
- Summary: total sites, files, signature preview
- Argument Shape Histogram: distribution of arg patterns
- Keyword Argument Usage: which kwargs used how often
- Calling Contexts: which functions call this one
- Call Sites: detailed list with previews
- Evidence

**Example:**
```bash
/cq calls DefIndex.build
/cq calls render_markdown
```

---

### imports - Import Structure Analysis

Analyzes module import structure with optional cycle detection.

```bash
/cq imports [--cycles] [--module <MODULE>]
```

**How it works:**
1. Scans all Python files for import statements
2. Builds import graph (module → imported modules)
3. If `--cycles`: runs DFS-based cycle detection
4. If `--module`: focuses analysis on specific module

**Output sections:**
- Summary: modules scanned, imports found
- Import Cycles (if detected)
- Module Dependencies
- Evidence

**Example:**
```bash
/cq imports --cycles
/cq imports --module src.relspec
```

---

### exceptions - Exception Handling Analysis

Analyzes exception handling patterns across the codebase.

```bash
/cq exceptions [--function <FUNCTION>]
```

**How it works:**
1. Walks AST for all files (or focused function)
2. Records `raise` statements with exception types
3. Records `try/except` handlers with caught types
4. Identifies bare `except:` clauses (potential hazard)
5. Detects exception chaining (`raise ... from ...`)

**Output sections:**
- Summary: raises, catches, bare excepts
- Raised Exceptions by type
- Caught Exceptions by type
- Bare Except Clauses (warnings)
- Exception Chaining
- Evidence

**Example:**
```bash
/cq exceptions
/cq exceptions --function cmd_impact
```

---

### sig-impact - Signature Change Analysis

Simulates a signature change and classifies call sites.

```bash
/cq sig-impact <FUNCTION_NAME> --to "<new_signature>"
```

**How it works:**
1. Parses new signature into `SigParam` list
2. Finds all call sites using `calls` infrastructure
3. Classifies each call site:
   - `would_break`: Missing required param, excess positional args, unknown keyword
   - `ambiguous`: Uses `*args`/`**kwargs` (can't verify statically)
   - `ok`: Compatible with new signature

**Signature parsing:**
- Extracts parameter names, defaults, keyword-only status
- Handles `*args`, `**kwargs`, positional-only (`/`)

**Output sections:**
- Summary: call sites, would_break, ambiguous, ok counts
- Key Findings: breakage/compatibility summary
- Would Break Sites (with specific reasons)
- Ambiguous Sites
- OK Sites
- Evidence

**Example:**
```bash
/cq sig-impact _find_repo_root --to "_find_repo_root(start: Path | None = None, *, strict: bool = False)"
```

---

### side-effects - Import-Time Side Effects

Detects side effects at module import time.

```bash
/cq side-effects [--max-files <N>]
```

**How it works:**
1. Scans module-level code (outside functions/classes)
2. Detects:
   - Top-level function calls
   - Global variable mutations
   - Ambient state access (sys.path, os.environ, etc.)
   - Module-level I/O operations

**Output sections:**
- Summary: files scanned, effects found
- Side Effects by type
- Files with Effects
- Evidence

**Example:**
```bash
/cq side-effects --max-files 500
```

---

### scopes - Closure Scope Analysis

Uses Python's `symtable` to analyze scope capture for closures.

```bash
/cq scopes <FILE_OR_SYMBOL>
```

**How it works:**
1. Resolves target to file(s) containing symbol
2. Builds symbol table with `symtable.symtable()`
3. Recursively extracts scope info:
   - `free_vars`: Variables captured from enclosing scope
   - `cell_vars`: Variables provided to nested scopes
   - `nonlocals`: Explicit `nonlocal` declarations
   - `globals_used`: Global variable references

**Use case:** Before extracting a nested function, check if it captures variables that would break when moved.

**Output sections:**
- Summary: files analyzed, scopes with captures
- Key Findings: closure count, provider count, nonlocal usage
- Scope Capture Details
- Evidence

**Example:**
```bash
/cq scopes tools/cq/macros/impact.py
/cq scopes TaintVisitor
```

---

### async-hazards - Blocking in Async Detection

Finds blocking calls inside async functions.

```bash
/cq async-hazards [--profiles "<blocking_patterns>"]
```

**Default blocking patterns:**
- `time.sleep`, `sleep`
- `requests.{get,post,put,delete,head,patch,request}`
- `subprocess.{run,call,check_call,check_output,Popen}`
- `os.system`, `os.popen`
- `urllib.request.urlopen`, `urlopen`
- `socket.socket.{recv,send,connect}`
- `open` (file I/O)

**How it works:**
1. Scans files for `async def` functions
2. Within async scopes, detects calls matching blocking patterns
3. Records hazards with async function name and blocking call

**Output sections:**
- Summary: files scanned, async functions, hazards
- Key Findings: hazard count, most common blocker
- Async Hazards list
- By Blocking Call Type
- Evidence

**Example:**
```bash
/cq async-hazards
/cq async-hazards --profiles "redis.get,mysql.execute"
```

---

### bytecode-surface - Bytecode Analysis

Analyzes Python bytecode for hidden dependencies.

```bash
/cq bytecode-surface <FILE_OR_SYMBOL> [--show <categories>]
```

**Show categories:**
- `globals`: Global name loads (`LOAD_GLOBAL`)
- `attrs`: Attribute access (`LOAD_ATTR`, `STORE_ATTR`)
- `constants`: Constant values (`LOAD_CONST`)
- `opcodes`: Opcode frequency analysis

Default: `globals,attrs,constants`

**How it works:**
1. Compiles source to bytecode
2. Uses `dis` module to disassemble
3. Extracts:
   - Global variable references (including builtins)
   - Attribute access patterns
   - Constant values embedded in bytecode
   - Opcode distribution

**Use case:** Finding hidden dependencies that aren't visible in import statements (e.g., globals accessed via `eval`, dynamic attribute access).

**Output sections:**
- Summary: file, opcodes analyzed
- Global References
- Attribute Access
- Constants
- Evidence

**Example:**
```bash
/cq bytecode-surface tools/cq/macros/calls.py --show globals,attrs
```

---

## Filtering Options

All commands support these filtering options:

### File Pattern Filters

| Option | Description |
|--------|-------------|
| `--include <pattern>` | Include files matching pattern (repeatable) |
| `--exclude <pattern>` | Exclude files matching pattern (repeatable) |

**Pattern syntax:**
- Glob patterns: `*.py`, `src/**/*.py`, `*test*`
- Regex patterns: `~src/(extract\|normalize)/.*\.py$` (prefix with `~`)

### Score Filters

| Option | Description |
|--------|-------------|
| `--impact <buckets>` | Filter by impact: `low`, `med`, `high` (comma-separated) |
| `--confidence <buckets>` | Filter by confidence: `low`, `med`, `high` (comma-separated) |
| `--severity <levels>` | Filter by severity: `error`, `warning`, `info` (comma-separated) |

### Output Limits

| Option | Description |
|--------|-------------|
| `--limit <N>` | Maximum number of findings |

### Filtering Examples

```bash
# High-impact, high-confidence findings in src/
/cq impact build_graph_product --param repo_root --impact high --confidence high --include "src/"

# Exclude tests, limit to 100 findings
/cq exceptions --exclude "tests/" --limit 100

# Multiple includes with regex
/cq side-effects --include "~src/(extract|normalize)/.*\.py$"

# Combine filters
/cq calls DefIndex.build --impact med,high --include "src/relspec/" --exclude "*test*"
```

---

## Output Formats

| Format | Description |
|--------|-------------|
| `--format md` | Markdown (default) - optimized for Claude context |
| `--format json` | Full JSON - for programmatic use |
| `--format both` | Markdown followed by JSON |
| `--format summary` | Condensed single-line - for CI integration |

### Summary Format

```
calls: 45 total sites [3 error, 12 warning, 30 info] [impact:high confidence:high]
```

---

## Artifact Persistence

By default, cq saves JSON artifacts to `.cq/artifacts/`:

```
.cq/artifacts/
├── impact_20240115_143022.json
├── calls_20240115_143145.json
└── ...
```

**Options:**
- `--artifact-dir <path>`: Custom artifact directory
- `--no-save-artifact`: Skip artifact saving

---

## Toolchain Requirements

cq detects and uses external tools:

| Tool | Required | Purpose |
|------|----------|---------|
| `rg` (ripgrep) | Yes | Fast file search for candidates |
| `sg` (ast-grep) | Optional | Structural code search (future) |
| Python | Yes | AST parsing, symtable, bytecode |

**Installation:**
```bash
# macOS
brew install ripgrep

# Linux
apt install ripgrep

# ast-grep (optional)
pip install ast-grep-py
# or
cargo install ast-grep
```

---

## Definition Index (DefIndex)

The `DefIndex` is cq's core data structure for repo-wide symbol resolution.

### What it captures:

**FnDecl (Function Declaration):**
- `name`: Function name
- `file`: Relative file path
- `line`: Definition line number
- `params`: List of `ParamInfo` (name, annotation, default, kind)
- `is_async`: Whether async
- `is_method`: Whether method (has self/cls)
- `class_name`: Containing class (if method)
- `decorators`: Decorator names
- `qualified_name`: `Class.method` or `function`
- `key`: `file::qualified_name`

**ClassDecl (Class Declaration):**
- `name`: Class name
- `file`: Relative file path
- `line`: Definition line number
- `bases`: Base class names
- `methods`: List of `FnDecl`
- `decorators`: Decorator names

**ModuleInfo:**
- `functions`: Top-level functions
- `classes`: Class definitions
- `module_aliases`: `import numpy as np` → `{"np": "numpy"}`
- `symbol_aliases`: `from pathlib import Path` → `{"Path": ("pathlib", "Path")}`

### Building the index:

```python
index = DefIndex.build(
    root="/path/to/repo",
    max_files=10000,
    include_patterns=["**/*.py"],
    exclude_patterns=["**/venv/**", "**/__pycache__/**"],
)
```

### Querying:

```python
# Find by name
functions = index.find_function_by_name("build")

# Find by qualified name
functions = index.find_function_by_qualified_name("DefIndex.build")

# Resolve import alias
module, symbol = index.resolve_import_alias("src/foo.py", "np.array")
# → ("numpy", "array")
```

---

## Findings Table (Polars-Based Filtering)

cq uses Polars DataFrames for efficient filtering:

### Column Schema

| Column | Type | Description |
|--------|------|-------------|
| `macro` | `Utf8` | Macro name |
| `group` | `Utf8` | "key_findings", "evidence", or section title |
| `category` | `Utf8` | Finding category |
| `message` | `Utf8` | Finding message |
| `file` | `Utf8` | File path (nullable) |
| `line` | `Int64` | Line number (nullable) |
| `col` | `Int64` | Column (nullable) |
| `impact_score` | `Float64` | Computed impact |
| `impact_bucket` | `Utf8` | high/med/low |
| `confidence_score` | `Float64` | Computed confidence |
| `confidence_bucket` | `Utf8` | high/med/low |
| `evidence_kind` | `Utf8` | Evidence source |
| `severity` | `Utf8` | error/warning/info |

### Filter flow:

```
CqResult → flatten_result() → FindingRecord[] → build_frame() → pl.DataFrame
    → apply_filters(df, include=..., exclude=..., impact=...) → filtered_df
    → rehydrate_result(original, filtered_df) → filtered CqResult
```

---

## Best Practices

### Before Refactoring

1. **Check callers first:**
   ```bash
   /cq calls function_to_modify
   ```

2. **Verify signature change safety:**
   ```bash
   /cq sig-impact function_name --to "function_name(new, signature)"
   ```

3. **Trace parameter impact:**
   ```bash
   /cq impact function_name --param param_to_change
   ```

### Before Extracting Nested Functions

```bash
/cq scopes file_with_nested_function.py
```

Check for captured variables that would break extraction.

### Investigating Async Issues

```bash
/cq async-hazards --impact high
```

### Understanding Module Dependencies

```bash
/cq imports --cycles
/cq imports --module your.module.path
```

### CI Integration

```bash
# Fail if high-impact breaking changes detected
./scripts/cq sig-impact foo --to "foo(new_sig)" --format summary --impact high
```

---

## Troubleshooting

### "ripgrep (rg) is required but not found"

Install ripgrep:
```bash
brew install ripgrep  # macOS
apt install ripgrep   # Debian/Ubuntu
```

### "Function 'X' not found in index"

- Check spelling and qualified name format
- Try both `function_name` and `Class.method`
- Increase `--max-files` if repo is large
- Check if file is in excluded directory

### Empty results

- Verify file patterns with `--include`/`--exclude`
- Lower impact/confidence thresholds
- Check if function exists in scanned files

### Slow performance

- Use `--max-files` to limit scope
- Add `--include` patterns to narrow search
- Ensure ripgrep is installed (much faster than pure Python)

### JSON artifact location

Default: `.cq/artifacts/`
Override: `--artifact-dir /custom/path`
Disable: `--no-save-artifact`
