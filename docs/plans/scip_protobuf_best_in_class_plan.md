# SCIP + Protobuf Best-In-Class Implementation Plan

## Goals
- Generate `index.scip` inside the pipeline by default, with an override path for prebuilt indexes.
- Use `build/scip/` as the canonical location for `scip.proto`, generated bindings, and `index.scip`.
- Persist `scip_pb2` by allowlisting `build/scip/` for version control.
- Compute `project_name`, `project_version`, and `project_namespace` programmatically via the GitHub CLI.
- Use protobuf parsing as the primary path; JSON is a strict fallback only.
- Enrich SCIP extraction for richer CPG and more reliable joins.

## Scope 1: Pipeline-integrated SCIP indexing

**Intent**
Create a pipeline node that decides whether to run scip-python and produces a stable `scip_index_path`.
This keeps SCIP indexing inside the pipeline while still supporting explicit overrides and opt-out.

**Code patterns**

```python
@dataclass(frozen=True)
class ScipIndexConfig:
    """Configuration for scip-python indexing inside the pipeline."""

    enabled: bool = True
    index_path_override: str | None = None
    output_dir: str = "build/scip"
    env_json_path: str | None = None  # default to build/scip/scip_env.json when generated
    scip_python_bin: str = "scip-python"
    target_only: str | None = None
    node_max_old_space_mb: int | None = 8192
    timeout_s: int | None = None
```

```python
@tag(layer="extract", artifact="scip_index", kind="path")
def scip_index_path(
    repo_root: str,
    scip_index_config: ScipIndexConfig,
    scip_identity: ScipIdentity,
    ctx: ExecutionContext,
) -> str | None:
    if not scip_index_config.enabled:
        return None
    if scip_index_config.index_path_override:
        return scip_index_config.index_path_override
    opts = build_scip_index_options(
        repo_root=repo_root,
        identity=scip_identity,
        config=scip_index_config,
    )
    return str(run_scip_python_index(opts))
```

**Target files**
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/pipeline_types.py`
- `src/hamilton_pipeline/modules/extraction.py`
- `src/extract/scip_extract.py`
- `src/extract/scip_indexer.py` (new)

**Implementation checklist**
- [ ] Add `ScipIndexConfig` to pipeline config types.
- [ ] Default `enabled=True` and document opt-out behavior.
- [ ] Add a pipeline node that produces `scip_index_path`.
- [ ] Thread `scip_index_path` into `scip_bundle`.
- [ ] Support override path without re-indexing.
- [ ] Always pass explicit `--project-name`, `--project-version`, and `--project-namespace`.
- [ ] Ensure `scip_python_bin` and timeout are configurable.
- [ ] Ensure `index.scip` is written under `build/scip/index.scip`.
- [ ] Ensure `build/scip/` exists before any SCIP artifact creation.
- [ ] If `env_json_path` is generated, place it under `build/scip/`.

## Scope 2: Programmatic project identity (repo slug, git SHA, org prefix)

**Intent**
Ensure stable symbol identity using `project_name`, `project_version`, and `project_namespace`,
computed automatically via the GitHub CLI and configurable overrides.

**Code patterns**

```python
@dataclass(frozen=True)
class ScipIdentity:
    """Stable identity values for SCIP symbol namespaces."""

    project_name: str
    project_version: str
    project_namespace: str | None
```

```python
def gh_repo_name_with_owner(repo_root: Path) -> str:
    proc = subprocess.run(
        ["gh", "repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def gh_repo_head_sha(repo_root: Path, name_with_owner: str) -> str:
    proc = subprocess.run(
        [
            "gh",
            "api",
            f"repos/{name_with_owner}/commits/HEAD",
            "--jq",
            ".sha",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()


def resolve_scip_identity(
    repo_root: Path,
    *,
    project_name_override: str | None,
    project_version_override: str | None,
    project_namespace_override: str | None,
) -> ScipIdentity:
    repo = gh_repo_name_with_owner(repo_root)
    sha = gh_repo_head_sha(repo_root, repo)
    default_prefix = "github.com/org"
    org_prefix = project_namespace_override or default_prefix
    return ScipIdentity(
        project_name=project_name_override or repo,
        project_version=project_version_override or sha,
        project_namespace=org_prefix,
    )
```

**Target files**
- `src/extract/scip_identity.py` (new)
- `src/extract/scip_indexer.py` (new)
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/pipeline_types.py`

**Implementation checklist**
- [ ] Implement GitHub CLI queries for repo slug and HEAD SHA.
- [ ] Treat missing `gh` auth as a hard error unless explicit overrides are set.
- [ ] Document the required `gh auth login` setup for CI or local use.
- [ ] Default `project_namespace` to `github.com/org` when not provided.
- [ ] Support explicit overrides via pipeline config.
- [ ] Add unit tests for identity derivation and defaults.

## Scope 3: Programmatic scip.proto discovery and `scip_pb2` regeneration

**Intent**
Ship generated `scip_pb2.py` in-repo for fast protobuf parsing, while discovering `scip.proto`
programmatically at build time. The canonical path is `build/scip/`. If a local `scip.proto` is
found under `build/scip/`, use it; otherwise download the version that matches the installed
`scip` CLI into `build/scip/`.

**Code patterns**

```python
# scripts/scip_proto_codegen.py
def scip_cli_version(scip_bin: str) -> str:
    proc = subprocess.run(
        [scip_bin, "--version"],
        capture_output=True,
        text=True,
        check=True,
    )
    return proc.stdout.strip()

def resolve_scip_proto_path(
    *,
    scip_bin: str,
    search_paths: Sequence[Path],
    cache_dir: Path,
) -> Path:
    env_override = os.getenv("SCIP_PROTO_PATH")
    if env_override:
        candidate = Path(env_override)
        if candidate.exists():
            return candidate
    for path in search_paths:
        if path.exists():
            return path
    version = scip_cli_version(scip_bin)
    url = f"https://raw.githubusercontent.com/sourcegraph/scip/{version}/scip.proto"
    cache_dir.mkdir(parents=True, exist_ok=True)
    target = cache_dir / "scip.proto"
    download_to(url, target)
    return target
```

```python
def generate_scip_pb2(proto_path: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "protoc",
            f"-I{proto_path.parent}",
            f"--python_out={out_dir}",
            f"--pyi_out={out_dir}",
            str(proto_path),
        ],
        check=True,
    )
```

```python
# build/scip/scip_pb2.py is imported via an explicit path loader.
def load_scip_pb2_from_build(build_dir: Path) -> ModuleType:
    module_path = build_dir / "scip_pb2.py"
    if not module_path.exists():
        raise FileNotFoundError(module_path)
    spec = importlib.util.spec_from_file_location("scip_pb2", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load scip_pb2 module spec.")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
```

**Target files**
- `scripts/scip_proto_codegen.py` (new)
- `build/scip/scip.proto` (generated cache)
- `build/scip/scip_pb2.py` (generated, persisted)
- `build/scip/scip_pb2.pyi` (generated, persisted)
- `src/extract/scip_extract.py`
- `src/extract/scip_proto_loader.py` (new)
- `.gitignore` (allowlist `build/scip/` for persistence)

**Implementation checklist**
- [ ] Ensure `build/scip/` exists for all SCIP artifacts.
- [ ] Implement `SCIP_PROTO_PATH` override and search-path discovery scoped to `build/scip/`.
- [ ] If not found, fetch `scip.proto` that matches `scip --version` into `build/scip/`.
- [ ] Generate `scip_pb2.py` and `scip_pb2.pyi` under `build/scip/`.
- [ ] Add a dynamic import loader for `build/scip/scip_pb2.py`.
- [ ] Add a regen script that uses `protoc`.
- [ ] Default `parse_index_scip` to the build-path loader unless overridden.
- [ ] Document how to regenerate and verify in the plan or README.
- [ ] Update `.gitignore` to allowlist `build/scip/` for persisted bindings.

## Scope 4: Protobuf-first parsing with strict JSON fallback

**Intent**
Use protobuf parsing as the primary and preferred method, with JSON parsing only as a last-resort
fallback when `scip_pb2` is unavailable or parsing fails.

**Code patterns**

```python
@dataclass(frozen=True)
class SCIPParseOptions:
    prefer_protobuf: bool = True
    allow_json_fallback: bool = False
    scip_pb2_import: str | None = None
    scip_cli_bin: str = "scip"
```

```python
def parse_index_scip(index_path: Path, parse_opts: SCIPParseOptions | None = None) -> object:
    opts = parse_opts or SCIPParseOptions()
    scip_pb2 = _load_scip_pb2(opts, build_dir=Path("build/scip"))
    if scip_pb2 is not None and opts.prefer_protobuf:
        return _parse_index_protobuf(index_path, scip_pb2)
    if opts.allow_json_fallback:
        return _parse_index_json(index_path, opts.scip_cli_bin)
    raise RuntimeError("SCIP protobuf bindings not available and JSON fallback disabled.")
```

**Target files**
- `src/extract/scip_extract.py`
- `src/extract/scip_parse_json.py` (new)

**Implementation checklist**
- [ ] Implement `_load_scip_pb2` with import override support.
- [ ] Default `_load_scip_pb2` to `build/scip/scip_pb2.py` via the path loader.
- [ ] Implement JSON fallback via `scip print --json` streaming only.
- [ ] Default `allow_json_fallback` to `False`.
- [ ] Emit clear error messages when protobuf is unavailable.

## Scope 5: Range integrity and span normalization

**Intent**
Avoid spurious `(0,0,0,0)` ranges and ensure byte spans are only computed from valid SCIP ranges.
Use `range_len` as a validity gate and preserve `enclosing_range` for definition bodies.

**Code patterns**

```python
def _normalize_range(rng: Sequence[int]) -> tuple[int, int, int, int] | None:
    if len(rng) == RANGE_LEN_SHORT:
        line, start_c, end_c = rng
        return int(line), int(start_c), int(line), int(end_c)
    if len(rng) >= RANGE_LEN_FULL:
        sl, sc, el, ec = rng[:RANGE_LEN_FULL]
        return int(sl), int(sc), int(el), int(ec)
    return None
```

```python
main_range = _normalize_range(list(getattr(occ, "range", [])))
if main_range is None:
    start_line = start_char = end_line = end_char = None
    range_len = 0
```

**Target files**
- `src/extract/scip_extract.py`
- `src/normalize/spans.py`

**Implementation checklist**
- [ ] Treat invalid ranges as `None` (do not emit zero coordinates).
- [ ] Gate byte-span computation on `range_len > 0`.
- [ ] Preserve `enclosing_range` fields and compute separate byte spans for it.
- [ ] Add span error reasons for invalid range data.

## Scope 6: Richer SCIP extraction tables

**Intent**
Preserve more SCIP data so symbol relationships, signatures, and syntax kinds are available to
build richer CPG nodes, props, and edges.

**Code patterns**

```python
SCIP_OCCURRENCES_SCHEMA = pa.schema(
    [
        ("syntax_kind", pa.string()),
        ("override_documentation", pa.list_(pa.string())),
        ("symbol_roles", pa.int32()),
        # existing span fields...
    ]
)
```

```python
SCIP_SYMBOL_RELATIONSHIPS_SCHEMA = pa.schema(
    [
        ("symbol", pa.string()),
        ("related_symbol", pa.string()),
        ("is_reference", pa.bool_()),
        ("is_implementation", pa.bool_()),
        ("is_type_definition", pa.bool_()),
        ("is_definition", pa.bool_()),
    ]
)
```

**Target files**
- `src/extract/scip_extract.py`
- `src/normalize/types.py`
- `src/cpg/build_props.py`
- `src/cpg/build_edges.py`

**Implementation checklist**
- [ ] Add columns for `syntax_kind` and `override_documentation` to occurrences.
- [ ] Add a new relationships table extracted from `SymbolInformation.relationships`.
- [ ] Add a new table for `external_symbols` from `Index.external_symbols`.
- [ ] Store `signature_documentation` as structured JSON or a derived table.
- [ ] Update CPG props to surface new symbol metadata.

## Scope 7: Role and diagnostic completeness

**Intent**
Preserve all `SymbolRole` and `Severity` values to improve filtering, diagnostics, and edge
semantics.

**Code patterns**

```python
SCIP_ROLE_DEFINITION = 1
SCIP_ROLE_IMPORT = 2
SCIP_ROLE_WRITE = 4
SCIP_ROLE_READ = 8
SCIP_ROLE_GENERATED = 16
SCIP_ROLE_TEST = 32
SCIP_ROLE_FORWARD_DEFINITION = 64
```

```python
def _scip_severity_value(value: int) -> str:
    mapping = {
        1: "ERROR",
        2: "WARNING",
        3: "INFO",
        4: "HINT",
    }
    return mapping.get(value, "INFO")
```

**Target files**
- `src/cpg/kinds.py`
- `src/normalize/diagnostics.py`
- `src/cpg/build_edges.py`
- `src/cpg/build_props.py`

**Implementation checklist**
- [ ] Add missing role constants and export them.
- [ ] Preserve roles in edges and add props for Generated/Test/ForwardDefinition.
- [ ] Add Hint severity support and preserve diagnostic tags.
- [ ] Add tests that verify role bitmask handling.

## Scope 8: Quality gates and observability (protobuf-first)

**Intent**
Add deterministic checks for index health, while keeping JSON and CLI tools as fallback or
debug-only paths.

**Code patterns**

```python
def assert_scip_index_health(index: Index) -> None:
    if not index.documents:
        raise ValueError("SCIP index has no documents.")
    if not any(doc.occurrences for doc in index.documents):
        raise ValueError("SCIP index has no occurrences.")
```

```bash
# Optional debug-only (not in normal execution):
scip print --json index.scip | jq '.documents | length'
```

**Target files**
- `src/extract/scip_extract.py`
- `src/hamilton_pipeline/modules/extraction.py`
- `tests/` (new tests for health checks)

**Implementation checklist**
- [ ] Add an optional health check step in the extraction pipeline.
- [ ] Surface counts and error diagnostics in logs or metrics.
- [ ] Keep JSON usage behind an explicit debug flag.

## Scope 9: Documentation and developer workflows

**Intent**
Ensure developers know how to regenerate `scip_pb2`, run scip-python inside the pipeline, and
override identity values when needed.

**Code patterns**

```markdown
## SCIP toolchain
- Regenerate protobuf bindings: `scripts/scip_proto_codegen.py`
- Run pipeline indexing: `uv run -- <pipeline entry>`
- Override identity: set `ScipIndexConfig.project_*_override`
- Ensure `build/scip/` is available and tracked for persisted bindings.
```

**Target files**
- `docs/plans/scip_protobuf_best_in_class_plan.md`
- `README.md` (optional)

**Implementation checklist**
- [ ] Document regen steps and scip.proto discovery/download behavior.
- [ ] Document identity override knobs and defaults.
- [ ] Document fallback behavior and how to opt in.
- [ ] Document the `build/scip/` artifact location and git ignore exceptions.

## Acceptance gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.14`
