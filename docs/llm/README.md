# LLM Rust Docs And API Baselines

This directory stores pre-implementation artifacts used for the DF52.1 + Delta 0.31 pivot.

## Public API snapshots

Pre-upgrade snapshots are stored under:

- `docs/llm/public_api/2026-02-18/pre_df52/`

Crates captured:

- `datafusion_ext`
- `codeanatomy-engine`
- `df_plugin_api`
- `df_plugin_codeanatomy`

Post-upgrade snapshot outputs are stored under:

- `docs/llm/public_api/2026-02-18/post_df52/`
- `docs/llm/public_api/2026-02-18/diff/`

Current status:

- `df_plugin_api` post snapshot succeeded.
- `datafusion_ext`, `codeanatomy-engine`, and `df_plugin_codeanatomy` are blocked by
  DF52 FFI migration errors in `rust/df_plugin_host/src/registry_bridge.rs`.

## Rustdoc markdown bundle

Generate a DF52.1 markdown bundle with:

```bash
scripts/generate_df52_rustdoc_md.sh
```

Default output:

- `docs/llm/rustdoc-md/df-52.1.0/`

Useful overrides:

```bash
OUT_DIR="$PWD/.artifacts/rustdoc-md" scripts/generate_df52_rustdoc_md.sh
DOC_MODE=with_deps scripts/generate_df52_rustdoc_md.sh
INCLUDE_PRIVATE=1 scripts/generate_df52_rustdoc_md.sh
NIGHTLY_TOOLCHAIN=nightly-2026-02-10 scripts/generate_df52_rustdoc_md.sh
```
