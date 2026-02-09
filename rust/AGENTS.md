# rust/AGENTS.md

Rust build and artifact expectations.

## Build Command

```bash
bash scripts/rebuild_rust_artifacts.sh
```

**When to rebuild:** After any `*.rs` changes in `rust/`.

## Crate Structure

| Crate | Purpose |
|-------|---------|
| `codeanatomy_engine` | Core Rust execution engine (no PyO3 bindings) |
| `codeanatomy_engine_py` | Python bindings crate for `codeanatomy_engine` |
| `datafusion_ext` | Core UDFs and DataFusion extensions |
| `datafusion_ext_py` | Python bindings via PyO3 |
| `datafusion_python` | Additional Python-facing APIs |
| `df_plugin_api` | ABI-stable plugin interface |
| `df_plugin_codeanatomy` | CodeAnatomy-specific plugin |
| `df_plugin_host` | Plugin host runtime |

## Expected Artifacts

After a successful build:

```
dist/wheels/
  datafusion-*.whl
  datafusion_ext-*.whl
  codeanatomy_engine-*.whl

build/
  datafusion_plugin_manifest.json

rust/datafusion_ext_py/plugin/
  *.so (Linux) / *.dylib (macOS)
```

## Key Files

- `Cargo.toml` - Workspace manifest
- `Cargo.lock` - Pinned dependencies (checked in)
- `scripts/rebuild_rust_artifacts.sh` - Build orchestration

## Common Issues

1. **Plugin path mismatch** - Manifest path must match actual binary location
2. **ABI version** - `df_plugin_api` defines the stable ABI; bump carefully
3. **PyO3 version** - Must match Python version (3.13)

## Testing Rust Code

```bash
cd rust && cargo test
```

For integration with Python, use the e2e tests after rebuilding artifacts.
