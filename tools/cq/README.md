# cq tool

High-signal code queries for LLM agents.
Canonical detailed behavior reference:
`.claude/skills/cq/reference/cq_reference.md`.

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

# Top-level composite pattern query (no entity= required)
/cq q "any='logger.$M($$$),print($$$)'"

# Multi-step execution (shared q-scan)
/cq run --steps '[{"type":"search","query":"build_graph"},{"type":"q","query":"entity=function name=build_graph"},{"type":"calls","function":"build_graph"}]'
```

## Scope Model

- `search`, `q`, and `run` use a language scope selector: `auto | python | rust`.
- Default scope is `auto`.
- Use `--lang rust` (search) or `lang=rust` (q queries) to narrow to Rust.
- Scope is extension-authoritative:
  - `python` => `.py`, `.pyi`
  - `rust` => `.rs`
  - `auto` => union of Python and Rust extensions
- Python-first ordering is preserved for merged results.
- Scope drops are surfaced in summary diagnostics via `dropped_by_scope`.
- `imports`, `exceptions`, and `side-effects` apply `--include/--exclude`
  at scan time (not only post-render filtering).

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
| `ldmd` | LDMD marker format for progressive disclosure |

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

## Enrichment Pipeline

Smart search results include multi-source enrichment from a 5-stage pipeline:

| Stage | Source | Provides |
|-------|--------|----------|
| `ast_grep` | ast-grep-py | Node kind, symbol role, structural context |
| `python_ast` | Python `ast` | AST node type, scope nesting |
| `import_detail` | Import visitor | Module path, alias resolution |
| `libcst` | LibCST metadata | Qualified names, scope analysis, binding candidates |
| `tree_sitter` | tree-sitter queries | Parse quality, structural patterns |

Enrichment payloads are structured into sections: `meta`, `resolution`, `behavior`,
`structural`, `parse_quality`, `agreement`. Cross-source agreement tracking compares
ast_grep, libcst, and tree_sitter results ("full"/"partial"/"conflict").

### Markdown Code Facts

In `--format md`, findings render enrichment as a **Code Facts** block (before context)
with clustered fields:

- `Identity`
- `Scope`
- `Interface`
- `Behavior`
- `Structure`

Missing values are explicit (`N/A — not applicable`, `N/A — not resolved`,
`N/A — enrichment unavailable`).

Python findings render Python enrichment payloads; Rust findings render Rust payloads.

### Render-Time Enrichment

Findings missing enrichment (e.g., from macro commands) receive on-demand enrichment
at markdown render time, parallelized across up to 4 workers for up to 9 files
(anchor file + next 8). Worker pools use multiprocessing `spawn` context.

### Markdown Output Structure

`render_markdown` is code-first and ordered as:

1. Title
2. Insight Card (FrontDoorInsightV1, when a target exists — rendered before section reordering)
3. `Code Overview`
4. Key Findings
5. Sections (reordered per `_SECTION_ORDER_MAP` for search/calls)
6. Evidence
7. Artifacts
8. Summary (compact JSON line, with heavy diagnostics offloaded to artifacts)
9. Footer

Code Overview guarantees best-effort `Query` and `Mode` values:
- `search`/`q`: taken from result summary.
- `run`: synthesized when needed (`mode="run"`, `query="multi-step plan (<n> steps)"` for mixed plans).
- macro commands: standardized to `mode="macro:<macro_name>"` with macro-specific query text.

### Front-Door Insight

Front-door commands (`search`, `calls`, `entity`) embed a `FrontDoorInsightV1` contract
in `summary["front_door_insight"]`. This provides immediate target identity, neighborhood
context, risk assessment, and confidence. The Insight Card is rendered first in markdown
output. See `cq_reference.md` for full schema details.

### Parallel Classification

Smart search classification runs in parallel (up to 4 workers, partitioned by file)
with fail-open fallback to sequential processing. Worker pools use multiprocessing
`spawn` context for safety in multi-threaded parents.

## Enrichment Debug Env Vars

- `CQ_PY_ENRICHMENT_CROSSCHECK=1`: include Python cross-source mismatch details.
- `CQ_RUST_ENRICHMENT_CROSSCHECK=1`: include Rust ast-grep/tree-sitter mismatch details.

## Request Objects

Internal operations use frozen `CqStruct` request types for clean input contracts:

- `RgRunRequest` - ripgrep execution input
- `CandidateCollectionRequest` - candidate collection input
- `PythonNodeEnrichmentRequest` / `PythonByteRangeEnrichmentRequest` - Python enrichment
- `RustEnrichmentRequest` - Rust enrichment
- `SummaryBuildRequest` / `MergeResultsRequest` - orchestration contracts

Request structs live in `tools/cq/search/requests.py` and `tools/cq/core/requests.py`.

## Analysis Session Caching

`PythonAnalysisSession` caches per-file analysis artifacts (ast-grep root, AST tree,
symtable, LibCST wrapper, tree-sitter tree) keyed by content hash. Maximum 64 cached
entries. Multiple findings in the same file share a single session.

## Pyrefly LSP Runtime Notes

- Pyrefly integration uses one warm session per workspace root.
- Requests are pipelined (no JSON-RPC batch) and session access is serialized
  to keep stdio framing deterministic under concurrent CQ workloads.
- Position encoding is negotiated at initialize (`utf-8` preferred, `utf-16`
  fallback) and CQ anchor columns are converted accordingly for request/response
  normalization.
- Advanced planes (semantic tokens, inlay hints, text/workspace diagnostics) are
  capability-gated and fail-open.

## Artifacts

JSON artifacts are saved by default to `.cq/artifacts`.
Use `--no-save-artifact` to skip.

Artifact types:

| Suffix | Content | When Saved |
|--------|---------|------------|
| `result` | Full `CqResult` JSON | Always (unless `--no-save-artifact`) |
| `diagnostics` | Offloaded diagnostic keys | When any offloaded key is present |
| `neighborhood_overflow` | Large neighborhood bundles | When bundle exceeds inline size budget |

Filename pattern: `{macro}_{suffix}_{timestamp}_{run_id}.json`
