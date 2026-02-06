# cq tool

High-signal code queries for LLM agents.

## Quick Start

```bash
# Smart search (recommended for discovery)
/cq search build_graph

# Rust-only scope
/cq search register_udf --lang rust

# Entity query (default scope is auto = python+rust)
/cq q "entity=function name=build_graph_product"

# Rust entity query
/cq q "entity=function lang=rust in=rust"

# Multi-step execution (shared q-scan)
/cq run --steps '[{"type":"search","query":"build_graph"},{"type":"q","query":"entity=function name=build_graph"},{"type":"calls","function":"build_graph"}]'
```

## Scope Model

- `search`, `q`, and `run` use a language scope selector: `auto | python | rust`.
- Default scope is `auto`.
- Use `--lang rust` (search) or `lang=rust` (q queries) to narrow to Rust.
- Python-first ordering is preserved for merged results.

## Global Options

| Option | Env Var | Default | Description |
|--------|---------|---------|-------------|
| `--root` | `CQ_ROOT` | Auto-detect | Repository root path |
| `--config` | `CQ_CONFIG` | `.cq.toml` | Config file path |
| `--no-config` | `CQ_NO_CONFIG` | `false` | Skip config file loading |
| `--verbose`, `-v` | `CQ_VERBOSE` | `0` | Verbosity level (0-3) |
| `--format` | `CQ_FORMAT` | `md` | Output format |
| `--artifact-dir` | `CQ_ARTIFACT_DIR` | `.cq/artifacts` | Artifact output directory |
| `--no-save-artifact` | `CQ_NO_SAVE_ARTIFACT` | `false` | Skip artifact saving |

### Filters

- `--include`, `--exclude` (glob or `~regex`)
- `--impact` (low,med,high)
- `--confidence` (low,med,high)
- `--severity` (error,warning,info)
- `--limit` (max findings)

## Output Formats

| Format | Description |
|--------|-------------|
| `md` | Markdown (default) |
| `json` | Full JSON |
| `both` | Markdown followed by JSON |
| `summary` | Condensed single-line output |
| `mermaid` | Mermaid flowchart |
| `mermaid-class` | Mermaid class diagram |
| `dot` | Graphviz DOT |

## Dependencies

Smart search uses native `rg` process execution plus AST enrichment.
Ensure `rg` is installed and available on `PATH`.

## Model Boundaries

- Use `msgspec.Struct` for serialized CQ contracts that cross module boundaries
  (for example `tools/cq/search/contracts.py` and
  `tools/cq/search/enrichment/contracts.py`).
- Keep parser handles, AST nodes, and cache state as runtime-only objects
  (protocols/dataclasses/regular classes), not serialized contract types.
- Serialize typed contracts at boundary points only (for example when building
  `CqResult.summary`), so internal code stays strongly typed and output remains
  mapping-compatible.
- Do not introduce `pydantic` in CQ hot paths (`tools/cq/search`, `tools/cq/query`,
  `tools/cq/run`); reserve it for explicit external-input adapters if needed.

## Command Coverage

- Cross-language (`auto/python/rust` scope): `search`, `q`, `run`, `chain`.
- Python-only deep analyses: `calls`, `impact`, `sig-impact`, `imports`, `scopes`,
  `bytecode-surface`, `side-effects`, `exceptions`.

## Artifacts

JSON artifacts are saved by default to `.cq/artifacts`.
Use `--no-save-artifact` to skip.
