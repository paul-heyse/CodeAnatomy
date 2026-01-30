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

### Config normalization

Nested sections in TOML are normalized into the flat keys expected by `driver_factory.py`.
Examples:

| Nested TOML | Flat key | Purpose |
|---|---|---|
| `[plan] allow_partial = true` | `plan_allow_partial = true` | Partial plan compilation |
| `[plan] requested_tasks = ["..."]` | `plan_requested_tasks = [...]` | Explicit task allowlist |
| `[cache] policy_profile = "aggressive"` | `cache_policy_profile = "aggressive"` | Cache policy profile |
| `[graph_adapter] kind = "ray"` | `graph_adapter_kind = "ray"` | Graph adapter selection |
| `[incremental] enabled = true` | `incremental_enabled = true` | Enable incremental mode |

Use `codeanatomy config show` to inspect the normalized payload:

```bash
codeanatomy config show
```

## Environment variable precedence

The CLI intentionally maps only the existing environment variables from `inputs.py`:

| Environment Variable | Purpose | CLI Flag |
|---|---|---|
| `CODEANATOMY_RUNTIME_PROFILE` | Runtime profile selection | `--runtime-profile` |
| `CODEANATOMY_DETERMINISM_TIER` | Determinism tier | `--determinism-tier` (overrides) |
| `CODEANATOMY_FORCE_TIER2` | Force canonical tier | `--determinism-tier canonical` |
| `CODEANATOMY_PIPELINE_MODE` | Incremental/streaming mode | `--incremental` (overrides) |
| `CODEANATOMY_STATE_DIR` | Incremental state directory | `--incremental-state-dir` |
| `CODEANATOMY_REPO_ID` | Repository identifier | `--incremental-repo-id` |
| `CODEANATOMY_INCREMENTAL_IMPACT_STRATEGY` | Impact strategy | `--incremental-impact-strategy` |
| `CODEANATOMY_GIT_BASE_REF` | Git base ref | `--git-base-ref` |
| `CODEANATOMY_GIT_HEAD_REF` | Git head ref | `--git-head-ref` |
| `CODEANATOMY_GIT_CHANGED_ONLY` | Changed-only mode | `--git-changed-only` |
| `CODEANATOMY_LOG_LEVEL` | Logging verbosity | `--log-level` |

## Advanced SCIP config-only fields

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
