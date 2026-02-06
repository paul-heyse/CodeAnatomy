# cq tool

High-signal code queries for LLM agents. Supports Python (default) and Rust.

## Quick Start

```bash
# Smart search (recommended for code discovery)
/cq search build_graph

# Find all callers
/cq calls build_graph_product

# Trace parameter impact
/cq impact build_graph --param root

# Direct invocation (preferred entrypoints)
./cq calls build_graph_product
uv run python -m tools.cq.cli calls build_graph_product

# Multi-step execution (shared scan for q steps)
/cq run --steps '[{"type":"search","query":"build_graph_product"},{"type":"q","query":"entity=function name=build_graph_product"},{"type":"calls","function":"build_graph_product"}]'

# Chained command frontend
/cq chain q "entity=function name=build_graph_product" AND calls build_graph_product AND search build_graph_product

# Rust code search (requires CQ_ENABLE_RUST_QUERY=1)
/cq search register_udf --lang rust
/cq q "entity=function lang=rust in=rust/"
```

## Global Options

All commands support these global options:

| Option | Env Var | Default | Description |
|--------|---------|---------|-------------|
| `--root` | `CQ_ROOT` | Auto-detect | Repository root path |
| `--config` | `CQ_CONFIG` | `.cq.toml` | Config file path |
| `--no-config` | `CQ_NO_CONFIG` | `false` | Skip config file loading |
| `--verbose`, `-v` | `CQ_VERBOSE` | `0` | Verbosity level (0-3) |
| `--format` | `CQ_FORMAT` | `md` | Output format |
| `--artifact-dir` | `CQ_ARTIFACT_DIR` | `.cq/artifacts` | Artifact output directory |
| `--no-save-artifact` | `CQ_NO_SAVE_ARTIFACT` | `false` | Skip artifact saving |
| — | `CQ_ENABLE_RUST_QUERY` | `false` | Enable Rust language queries |

### Filters

- `--include`, `--exclude` (glob or `~regex`)
- `--impact` (low,med,high)
- `--confidence` (low,med,high)
- `--severity` (error,warning,info)
- `--limit` (max findings)

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

## Configuration

Config precedence (highest to lowest):
1. CLI flags
2. Environment variables (`CQ_*`)
3. Config file (`.cq.toml` or specified via `--config`)
4. Defaults

### Config File

Create `.cq.toml` in your repo root:

```toml
[cq]
format = "md"
verbose = 0
artifact_dir = ".cq/artifacts"
save_artifact = true
```

## Dependencies

Smart search uses ripgrep via rpygrep. Ensure `rg` is installed and on `PATH`.

### Rust Language Support

Rust queries are feature-gated. Enable with:

```bash
export CQ_ENABLE_RUST_QUERY=1
```

Supported for Rust: `search` (`--lang rust`), `q` (`lang=rust`), `run`/`chain`.
Not supported for Rust: `calls`, `impact`, `sig-impact`, `imports`, `scopes`, `bytecode-surface`, `side-effects`, `exceptions` (Python-only).

## Command Groups

### Analysis Commands

| Command | Purpose | Example |
|---------|---------|---------|
| `search` | Smart code search with enrichment | `/cq search build_graph` |
| | ↳ Rust: `--lang rust` | `/cq search register_udf --lang rust` |
| `impact` | Trace data flow from a parameter | `/cq impact build_graph --param root` |
| `calls` | Census all call sites for a function | `/cq calls DefIndex.build` |
| `sig-impact` | Test signature change viability | `/cq sig-impact foo --to "foo(a, *, b=None)"` |
| `imports` | Analyze import structure/cycles | `/cq imports --cycles` |
| `exceptions` | Analyze exception handling | `/cq exceptions` |
| `side-effects` | Detect import-time side effects | `/cq side-effects` |
| `scopes` | Analyze closure captures | `/cq scopes path/to/file.py` |
| `bytecode-surface` | Analyze bytecode dependencies | `/cq bytecode-surface file.py` |
| `q` | Declarative entity queries | `/cq q "entity=import name=Path"` |
| `report` | Target-scoped report bundles | `/cq report refactor-impact --target function:foo` |
| `run` | Multi-step execution (shared scan) | `/cq run --plan docs/plans/cq_run_example.toml` |
| `chain` | Command chaining frontend | `/cq chain q "entity=function" AND calls foo` |

### Run/Chain Notes

- `--step` and `--steps` accept JSON in a single token (quote the entire JSON).
- `cq run` continues on error by default; `--stop-on-error` fails fast.
- Merged results include `details.data["source_step"]` and `details.data["source_macro"]`.
- `cq chain` uses a delimiter token (default `AND`); quote multi-word queries.

### Administration Commands

| Command | Purpose | Example |
|---------|---------|---------|
| `index` | *Deprecated* - Index management removed | - |
| `cache` | *Deprecated* - Cache management removed | - |
| `schema` | Show JSON schema for artifacts | `/cq schema` |

### Calls Output Enrichment

The `calls` command provides enrichment data for each call site:

| Field | Description |
|-------|-------------|
| `context_window` | Line range (`start_line`, `end_line`) of containing function |
| `context_snippet` | Source code snippet of the containing function (truncated if >30 lines) |
| `symtable_info` | Symbol table analysis: `is_closure`, `free_vars`, `globals_used`, `nested_scopes` |
| `bytecode_info` | Bytecode analysis: `load_globals`, `load_attrs`, `call_functions` |

**Context Window & Snippet:**

The context window identifies the line range of the function containing each call site. The context snippet provides the actual source code, making it easy to understand how the function is called without opening the file. Long snippets (>30 lines) are truncated to show the first 15 and last 5 lines with a marker.

**Performance:**

The `calls` command uses on-demand signature lookup, parsing only the file containing the function definition rather than building a full repository index. This significantly improves performance for large codebases.

## Artifacts

JSON artifacts are saved by default to `.cq/artifacts`. Use `--no-save-artifact` to skip.

## Examples

```bash
# Use JSON output
/cq calls build_graph --format json

# Specify custom root
/cq q "entity=function" --root /path/to/repo

# Verbose output for debugging
/cq impact foo --param bar --verbose 2

# Skip config file (use defaults only)
/cq calls foo --no-config

# Security-sensitive pattern query
/cq q "pattern='eval(\$X)'"
```
