# CodeAnatomy

## Tooling prerequisites
- Python 3.14+ (use `scripts/bootstrap_codex.sh` + `uv sync` for setup).
- SCIP CLI (`scip`) and `scip-python` available on `PATH`.
- `protoc` for generating `scip_pb2` bindings.
- GitHub CLI (`gh`) authenticated via `gh auth login` for project identity.

## SCIP artifacts
- `build/scip/` is the canonical location for `scip.proto`, `scip_pb2`, and `index.scip`.
- Regenerate protobuf bindings with `scripts/scip_proto_codegen.py`.

## DataFusion catalog autoload
- Set `CODEANATOMY_DATAFUSION_CATALOG_LOCATION` and `CODEANATOMY_DATAFUSION_CATALOG_FORMAT` to
  configure `datafusion.catalog.location` and `datafusion.catalog.format` for catalog autoload.

## Recommended API

Use the graph product entrypoint instead of calling the pipeline directly:

```python
from graph import GraphProductBuildRequest, build_graph_product

result = build_graph_product(
    GraphProductBuildRequest(
        repo_root=".",
    )
)
print(result.output_dir)
```

## CLI

The Cyclopts-based CLI is available via the `codeanatomy` entrypoint. In a dev checkout:

```bash
uv run codeanatomy --help
uv run python -m cli --help
```

See `docs/guide/cli.md` for full usage, configuration, and examples.
