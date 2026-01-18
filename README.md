# CodeAnatomy

## Tooling prerequisites
- Python 3.14+ (use `scripts/bootstrap_codex.sh` + `uv sync` for setup).
- SCIP CLI (`scip`) and `scip-python` available on `PATH`.
- `protoc` for generating `scip_pb2` bindings.
- GitHub CLI (`gh`) authenticated via `gh auth login` for project identity.

## SCIP artifacts
- `build/scip/` is the canonical location for `scip.proto`, `scip_pb2`, and `index.scip`.
- Regenerate protobuf bindings with `scripts/scip_proto_codegen.py`.

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
