# CodeAnatomy CLI

## Quick start

```bash
./scripts/bootstrap_codex.sh
uv sync
```

Run the CLI (choose one):

```bash
# Preferred once entry points are installed
uv run codeanatomy --help

# Always works in a dev checkout
uv run python -m cli --help
```

## Command overview

```
codeanatomy
├── build              # Main CPG pipeline (primary entry point)
├── plan               # Show/validate execution plan without running
├── delta              # Delta Lake maintenance operations
│   ├── vacuum         # Run Delta vacuum
│   ├── checkpoint     # Create Delta checkpoint
│   ├── cleanup-log    # Cleanup expired log files
│   ├── export         # Clone Delta snapshot to target
│   └── restore        # Restore table to prior version/timestamp
├── diag               # Diagnostics and reporting
├── config             # Configuration management
│   ├── show           # Show effective configuration
│   ├── validate       # Validate configuration file
│   └── init           # Initialize configuration template
└── version            # Show version and engine information
```

## Configuration files

Configuration is layered and merged in this order:

1. CLI flags (highest priority)
2. Environment variables (explicit `env_var=` per parameter)
3. `codeanatomy.toml` (searched in the current directory and parents)
4. `pyproject.toml` under `[tool.codeanatomy]`
5. Python defaults (lowest priority)

### Config file discovery

By default, the CLI searches for configuration files in the current directory and parent directories:

- `codeanatomy.toml`
- `pyproject.toml` with `[tool.codeanatomy]`

To force a specific config file, use the global `--config` flag:

```bash
codeanatomy --config ./codeanatomy.toml build .
```

### Config structure

Configuration is **nested-only** (no flat-key normalization). Use TOML sections
like `[plan]`, `[cache]`, `[graph_adapter]`, etc. to set values.

Use `codeanatomy config show` to inspect the effective payload:

```bash
codeanatomy config show
```

To include value origins (CLI/env/config/default), use:

```bash
codeanatomy config show --with-sources
```

## Environment variables

The CLI uses explicit `CODEANATOMY_` environment variables for high-value parameters.
Use `codeanatomy config show --with-sources`
to see where each configuration value is coming from.

### High-value overrides

| Environment Variable | Purpose | CLI Flag |
|---|---|---|
| `CODEANATOMY_LOG_LEVEL` | Logging verbosity | `--log-level` |
| `CODEANATOMY_RUNTIME_PROFILE` | Runtime profile selection | `--runtime-profile` |
| `CODEANATOMY_OUTPUT_DIR` | Output directory | `--output-dir` |
| `CODEANATOMY_WORK_DIR` | Working directory | `--work-dir` |
| `CODEANATOMY_EXECUTION_MODE` | Execution mode | `--execution-mode` |
| `CODEANATOMY_DETERMINISM_TIER` | Determinism tier | `--determinism-tier` |
| `CODEANATOMY_DISABLE_SCIP` | Disable SCIP indexing | `--disable-scip` |
| `CODEANATOMY_SCIP_OUTPUT_DIR` | SCIP output directory | `--scip-output-dir` |
| `CODEANATOMY_INCLUDE_GLOBS` | Include globs | `--include-globs` |
| `CODEANATOMY_EXCLUDE_GLOBS` | Exclude globs | `--exclude-globs` |
| `CODEANATOMY_STATE_DIR` | Incremental state directory | `--incremental-state-dir` |
| `CODEANATOMY_REPO_ID` | Repository identifier | `--incremental-repo-id` |
| `CODEANATOMY_INCREMENTAL_IMPACT_STRATEGY` | Impact strategy | `--incremental-impact-strategy` |
| `CODEANATOMY_GIT_BASE_REF` | Git base ref | `--git-base-ref` |
| `CODEANATOMY_GIT_HEAD_REF` | Git head ref | `--git-head-ref` |
| `CODEANATOMY_GIT_CHANGED_ONLY` | Changed-only mode | `--git-changed-only` |
| `CODEANATOMY_PLAN_OUTPUT_FORMAT` | Plan output format | `--output-format` |
| `CODEANATOMY_PLAN_OUTPUT_FILE` | Plan output file | `--output-file` |

### Observability

| Environment Variable | Purpose | CLI Flag |
|---|---|---|
| `CODEANATOMY_ENABLE_TRACES` | Enable OpenTelemetry traces | `--enable-traces` |
| `CODEANATOMY_ENABLE_METRICS` | Enable OpenTelemetry metrics | `--enable-metrics` |
| `CODEANATOMY_ENABLE_LOGS` | Enable OpenTelemetry logs | `--enable-logs` |
| `CODEANATOMY_OTEL_TEST_MODE` | In-memory OTel exporters (tests) | `--otel-test-mode` |

### Advanced environment toggles

The pipeline honors a few runtime toggles defined in the engine layers:

- `CODEANATOMY_FORCE_TIER2` (forces canonical determinism)
- `CODEANATOMY_PIPELINE_MODE` (incremental/streaming mode)

## Advanced SCIP config-only fields

When `--disable-scip` is enabled, CLI-only SCIP parameters are ignored and the
CLI will warn if any disabled SCIP options are still provided.

The following SCIP settings are **config-only** (set in `[scip]`):

- `scip_cli_bin`
- `generate_env_json`
- `use_incremental_shards`
- `shards_dir`
- `shards_manifest_path`
- `run_scip_print`
- `scip_print_path`
- `run_scip_snapshot`
- `scip_snapshot_dir`
- `scip_snapshot_comment_syntax`
- `run_scip_test`
- `scip_test_args`

Example:

```toml
[scip]
scip_cli_bin = "scip"
generate_env_json = false
use_incremental_shards = false
shards_dir = "build/scip/shards"
shards_manifest_path = "build/scip/shards/manifest.json"
run_scip_print = false
scip_print_path = "build/scip/print.json"
run_scip_snapshot = false
scip_snapshot_dir = "build/scip/snapshots"
scip_snapshot_comment_syntax = "#"
run_scip_test = false
scip_test_args = ["--check-documents"]
```

## Shell completion

Install completion scripts dynamically:

```bash
codeanatomy --install-completion
```

Generate static completion scripts for packaging:

```bash
uv run python scripts/generate_completion_scripts.py --output-dir ./dist/completions
```

## Examples

```bash
# Build the full CPG
codeanatomy build . --output-dir ./build

# Inspect plan (JSON)
codeanatomy plan . --output-format json

# Delta maintenance
codeanatomy delta vacuum --path ./build/cpg/nodes

# Diagnostics
codeanatomy diag --output-dir ./build
```

## Deprecated scripts

The legacy scripts in `scripts/` remain available but are deprecated in favor of the CLI:

| Script | Replacement |
|---|---|
| `scripts/run_full_pipeline.py` | `codeanatomy build` |
| `scripts/delta_maintenance.py` | `codeanatomy delta vacuum/checkpoint/cleanup-log` |
| `scripts/delta_export_snapshot.py` | `codeanatomy delta export` |
| `scripts/delta_restore_table.py` | `codeanatomy delta restore` |
| `scripts/e2e_diagnostics_report.py` | `codeanatomy diag` |
