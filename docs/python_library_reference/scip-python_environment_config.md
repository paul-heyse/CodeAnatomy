# scip-python environment configuration (uv projects)

This guide covers deterministic scip-python indexing when using uv. The key points are:

- scip-python uses pip by default to discover installed packages unless you provide an
  explicit `--environment` JSON.
- Pyright must know the correct venv and import roots for accurate symbol resolution.

## Checklist

1. Ensure the project venv exists and dependencies are synced:
   - `uv sync` (use `--locked` or `--frozen` in CI).
2. Ensure pip is available inside the venv, or generate an environment JSON.
3. Add a `pyrightconfig.json` so scip-python resolves imports the same way as your runtime.

## Recommended files

### pyrightconfig.json

```json
{
  "venvPath": ".",
  "venv": ".venv",
  "pythonVersion": "3.12",
  "extraPaths": ["src"],
  "verboseOutput": true
}
```

- Use `extraPaths` if you have a `src/` layout.
- Set `pythonVersion` to the version you actually run in the venv.

### .python-version (optional, but helpful)

uv uses `.python-version` to select the interpreter, which keeps indexing deterministic.

### Environment JSON (optional)

If you cannot rely on pip, generate a scip-python environment JSON and pass it via
`--environment`.

## Running scip-python with uv

Preferred:

```bash
uv run -- scip-python index . --project-name <name>
```

Alternative (activate venv first):

```bash
source .venv/bin/activate
scip-python index . --project-name <name>
```

## Generating an environment JSON

This repo provides a helper script:

```bash
uv run -- python scripts/gen_scip_env.py --output build/scip/env.json
uv run -- scip-python index . --project-name <name> --environment build/scip/env.json
```

## Pipeline configuration mapping

ScipIndexSettings controls scip-python execution inside the pipeline:

- `scip_python_bin`: path to scip-python (default `scip-python`).
- `env_json_path` / `generate_env_json`: use or generate an environment JSON.
- `extra_args`: additional CLI args for scip-python.
- `use_incremental_shards`, `shards_dir`, `shards_manifest_path`: shard reuse for incremental
  indexing.
- `run_scip_print`, `run_scip_snapshot`, `run_scip_test`: optional scip CLI hooks.
- `scip_cli_bin`: path to the scip CLI (only needed when those hooks are enabled).

## Troubleshooting

- Missing external imports: enable `verboseOutput` in `pyrightconfig.json` and re-run.
- pip missing: add `pip` as a dev dependency or switch to an environment JSON.
- Node OOM: increase `node_max_old_space_mb` or set `NODE_OPTIONS=--max-old-space-size=...`.
