# src/datafusion_engine/AGENTS.md

DataFusion integration guidance.

## Before You Start

**Use the `datafusion-and-deltalake-stack` skill** before touching DataFusion/DeltaLake/UDF APIs:

```
/datafusion-and-deltalake-stack
```

This skill provides version-correct API references for both DataFusion (query engine) and DeltaLake (storage layer). Do not guess APIs.

## Key Modules

| Module | Purpose |
|--------|---------|
| `session/runtime.py` | Runtime profiles and configuration |
| `session/factory.py` | Session construction |
| `session/facade.py` | High-level session interface |
| `delta/control_plane.py` | Plugin loading and Delta integration |
| `delta/scan_config.py` | Delta scan configuration |
| `udf/` | UDF registration and runtime |
| `views/` | View graph and registry |
| `schema/` | Schema introspection and validation |

## Common Footguns

1. **Plugin path** - Must match `datafusion_plugin_manifest.json`
2. **UDF registry** - Initialize before query execution
3. **Session lifecycle** - Don't share sessions across threads
4. **Delta maintenance** - Use `scripts/` for compaction/vacuum

## Configuration

- `RuntimeProfile` in `session/runtime.py` - Memory, parallelism settings
- `DeltaScanConfig` in `delta/scan_config.py` - Pruning, caching options
- Environment variables documented in module docstrings

## Reference Docs

- `docs/architecture/datafusion_engine_core.md`
- Skill reference: `.claude/skills/datafusion-and-deltalake-stack/`
