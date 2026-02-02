# cq tool

High-signal code queries for LLM agents.

## Quick Start

```bash
# Via skill (recommended)
/cq calls build_graph_product

# Direct invocation
uv run python -c "from tools.cq.cli_app import main; main()" calls build_graph_product
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

## Command Groups

### Analysis Commands

| Command | Purpose | Example |
|---------|---------|---------|
| `impact` | Trace data flow from a parameter | `/cq impact build_graph --param root` |
| `calls` | Census all call sites for a function | `/cq calls DefIndex.build` |
| `sig-impact` | Test signature change viability | `/cq sig-impact foo --to "foo(a, *, b=None)"` |
| `imports` | Analyze import structure/cycles | `/cq imports --cycles` |
| `exceptions` | Analyze exception handling | `/cq exceptions` |
| `side-effects` | Detect import-time side effects | `/cq side-effects` |
| `scopes` | Analyze closure captures | `/cq scopes path/to/file.py` |
| `async-hazards` | Find blocking in async | `/cq async-hazards` |
| `bytecode-surface` | Analyze bytecode dependencies | `/cq bytecode-surface file.py` |
| `q` | Declarative entity queries | `/cq q "entity=import name=Path"` |
| `report` | Target-scoped report bundles | `/cq report refactor-impact --target function:foo` |

### Administration Commands

| Command | Purpose | Example |
|---------|---------|---------|
| `index` | Manage ast-grep scan index cache | `/cq index --rebuild` |
| `cache` | Manage query result cache | `/cq cache --stats` |

## Caching

Query and report runs use caching by default. Disable with `--no-cache`.

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
```
