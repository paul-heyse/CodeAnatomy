# SCIP + Protobuf Best-In-Class Implementation Plan

## Goals
- Generate `index.scip` inside the pipeline by default, with an override path for prebuilt indexes.
- Use `build/scip/` as the canonical location for `scip.proto`, generated bindings, and `index.scip`.
- Persist `scip_pb2` by allowlisting `build/scip/` for version control.
- Compute `project_name`, `project_version`, and `project_namespace` programmatically via the GitHub CLI.
- Use protobuf parsing as the primary path; JSON is a strict fallback only when explicitly enabled.
- Enrich SCIP extraction for richer CPG and more reliable joins.
- Align SCIP extraction and normalization with Arrow/Acero columnar strategy.

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
    env_json_path: str | None = None
    scip_python_bin: str = "scip-python"
    target_only: str | None = None
    node_max_old_space_mb: int | None = 8192
    timeout_s: int | None = None
```

```python
@tag(layer="extract", artifact="scip_index", kind="path")
def scip_index_path(
    repo_root: str,
    scip_identity_overrides: ScipIdentityOverrides,
    scip_index_config: ScipIndexConfig,
    ctx: ExecutionContext,
) -> str | None:
    _ = ctx
    repo_root_path = Path(repo_root).resolve()
    build_dir = ensure_scip_build_dir(repo_root_path, scip_index_config.output_dir)

    if scip_index_config.index_path_override:
        override = Path(scip_index_config.index_path_override)
        override_path = override if override.is_absolute() else repo_root_path / override
        target = build_dir / "index.scip"
        if override_path.resolve() != target.resolve():
            target.write_bytes(override_path.read_bytes())
        return str(target)

    if not scip_index_config.enabled:
        return None

    identity = resolve_scip_identity(
        repo_root_path,
        project_name_override=scip_identity_overrides.project_name_override,
        project_version_override=scip_identity_overrides.project_version_override,
        project_namespace_override=scip_identity_overrides.project_namespace_override,
    )
    opts = build_scip_index_options(
        repo_root=repo_root_path,
        identity=identity,
        config=scip_index_config,
    )
    return str(run_scip_python_index(opts))
```

**Target files**
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/pipeline_types.py`
- `src/hamilton_pipeline/modules/extraction.py`
- `src/extract/scip_extract.py`
- `src/extract/scip_indexer.py`

**Implementation checklist**
- [x] Add `ScipIndexConfig` to pipeline config types.
- [x] Default `enabled=True` and document opt-out behavior.
- [x] Add a pipeline node that produces `scip_index_path`.
- [x] Thread `scip_index_path` into `scip_bundle`.
- [x] Support override path without re-indexing (copy into `build/scip/index.scip`).
- [x] Always pass explicit `--project-name`, `--project-version`, and `--project-namespace`.
- [x] Ensure `scip_python_bin` and timeout are configurable.
- [x] Ensure `index.scip` is written under `build/scip/index.scip` by default.
- [x] Ensure `build/scip/` exists before any SCIP artifact creation.
- [x] Resolve `env_json_path` relative to repo root when provided.

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
@dataclass(frozen=True)
class ScipIdentityOverrides:
    """Optional overrides for SCIP project identity."""

    project_name_override: str | None
    project_version_override: str | None
    project_namespace_override: str | None
```

```python
def resolve_scip_identity(
    repo_root: Path,
    *,
    project_name_override: str | None,
    project_version_override: str | None,
    project_namespace_override: str | None,
) -> ScipIdentity:
    repo = project_name_override or gh_repo_name_with_owner(repo_root)
    sha = project_version_override or gh_repo_head_sha(repo_root, repo)
    namespace = project_namespace_override or DEFAULT_ORG_PREFIX
    return ScipIdentity(
        project_name=repo,
        project_version=sha,
        project_namespace=namespace,
    )
```

**Target files**
- `src/extract/scip_identity.py`
- `src/extract/scip_indexer.py`
- `src/hamilton_pipeline/modules/inputs.py`
- `src/hamilton_pipeline/pipeline_types.py`

**Implementation checklist**
- [x] Implement GitHub CLI queries for repo slug and HEAD SHA.
- [x] Raise on `gh` failures; bypass `gh` when both name/version overrides are provided.
- [x] Document the required `gh auth login` setup for CI or local use.
- [x] Default `project_namespace` to `github.com/org` when not provided.
- [x] Support explicit overrides via pipeline config.
- [x] Add unit tests for identity derivation and defaults.

## Scope 3: Programmatic scip.proto discovery and `scip_pb2` regeneration

**Intent**
Ship generated `scip_pb2.py` in-repo for fast protobuf parsing, while discovering `scip.proto`
programmatically at build time. The canonical path is `build/scip/`. If a local `scip.proto` is
found under `build/scip/`, use it; otherwise download the version that matches the installed
`scip` CLI into `build/scip/`.

**Code patterns**

```python
# scripts/scip_proto_codegen.py
def resolve_scip_proto_path(
    *,
    scip_bin: str,
    build_dir: Path,
) -> Path:
    target = build_dir / "scip.proto"
    if target.exists():
        return target

    env_override = os.getenv("SCIP_PROTO_PATH")
    if env_override:
        override_path = Path(env_override)
        if override_path.exists():
            target.write_bytes(override_path.read_bytes())
            return target

    version = scip_cli_version(scip_bin)
    url = f"https://raw.githubusercontent.com/sourcegraph/scip/{version}/scip.proto"
    download_to(url, target)
    return target
```

```python
def generate_scip_pb2(proto_path: Path, out_dir: Path) -> None:
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
- `scripts/scip_proto_codegen.py`
- `build/scip/scip.proto`
- `build/scip/scip_pb2.py`
- `build/scip/scip_pb2.pyi`
- `src/extract/scip_extract.py`
- `src/extract/scip_proto_loader.py`
- `.gitignore` (allowlist `build/scip/` for persistence)

**Implementation checklist**
- [x] Ensure `build/scip/` exists for all SCIP artifacts.
- [x] Implement `SCIP_PROTO_PATH` override copied into `build/scip/scip.proto`.
- [x] If not found, fetch `scip.proto` that matches `scip --version` into `build/scip/`.
- [x] Generate `scip_pb2.py` and `scip_pb2.pyi` under `build/scip/`.
- [x] Add a dynamic import loader for `build/scip/scip_pb2.py`.
- [x] Add a regen script that uses `protoc`.
- [x] Default `parse_index_scip` to the build-path loader unless overridden.
- [x] Document how to regenerate and verify in README.
- [x] Update `.gitignore` to allowlist `build/scip/` for persisted bindings.

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
    build_dir: Path | None = None
    health_check: bool = False
    log_counts: bool = False
    dictionary_encode_strings: bool = False
```

```python
def parse_index_scip(index_path: Path, parse_opts: SCIPParseOptions | None = None) -> object:
    opts = parse_opts or SCIPParseOptions()
    if opts.build_dir is None:
        opts = replace(opts, build_dir=index_path.parent)
    scip_pb2 = _load_scip_pb2(opts)
    if scip_pb2 is not None and hasattr(scip_pb2, "Index") and opts.prefer_protobuf:
        return _parse_index_protobuf(index_path, scip_pb2)
    if opts.allow_json_fallback:
        return parse_index_json(index_path, opts.scip_cli_bin)
    if scip_pb2 is not None and hasattr(scip_pb2, "Index"):
        return _parse_index_protobuf(index_path, scip_pb2)
    raise RuntimeError("SCIP protobuf bindings not available and JSON fallback disabled.")
```

**Target files**
- `src/extract/scip_extract.py`
- `src/extract/scip_parse_json.py`

**Implementation checklist**
- [x] Implement `_load_scip_pb2` with import override support.
- [x] Default `_load_scip_pb2` to `build/scip/scip_pb2.py` via the path loader.
- [x] Implement JSON fallback via `scip print --json` streaming only.
- [x] Default `allow_json_fallback` to `False`.
- [x] Emit clear error messages when protobuf is unavailable.

## Scope 5: Range integrity and span normalization

**Intent**
Avoid spurious `(0,0,0,0)` ranges and ensure byte spans are only computed from valid SCIP ranges.
Preserve `enclosing_range` for definition bodies.

**Code patterns**

```python
def _normalize_range(rng: Sequence[int]) -> tuple[int, int, int, int, int] | None:
    if len(rng) == RANGE_LEN_SHORT:
        line, start_c, end_c = rng
        return int(line), int(start_c), int(line), int(end_c), RANGE_LEN_SHORT
    if len(rng) >= RANGE_LEN_FULL:
        sl, sc, el, ec = rng[:RANGE_LEN_FULL]
        return int(sl), int(sc), int(el), int(ec), RANGE_LEN_FULL
    return None
```

```python
norm = _normalize_range(list(getattr(occ, "range", [])))
if norm is None:
    start_line = start_char = end_line = end_char = None
    range_len = 0
```

**Target files**
- `src/extract/scip_extract.py`
- `src/normalize/spans.py`

**Implementation checklist**
- [x] Treat invalid ranges as `None` (do not emit zero coordinates).
- [x] Preserve `enclosing_range` fields and compute separate byte spans for it.
- [x] Span errors capture missing range data in `scip_span_errors`.
- [x] Gate byte-span computation on `range_len > 0`.

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
- `src/hamilton_pipeline/modules/extraction.py`
- `src/hamilton_pipeline/modules/cpg_build.py`
- `src/cpg/build_props.py`

**Implementation checklist**
- [x] Add columns for `syntax_kind` and `override_documentation` to occurrences.
- [x] Add a new relationships table extracted from `SymbolInformation.relationships`.
- [x] Add a new table for `external_symbols` from `Index.external_symbols`.
- [x] Store `signature_documentation` as a struct with nested occurrences.
- [x] Update CPG props to surface new symbol metadata.
- [x] Emit `scip_symbol_relationships` as CPG edges.

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
- [x] Add missing role constants and export them.
- [x] Preserve roles in edges and add props for Generated/Test/ForwardDefinition.
- [x] Add Hint severity support and preserve diagnostic tags.
- [x] Add tests that verify role bitmask handling.

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
- `tests/`

**Implementation checklist**
- [x] Add an optional health check step in the extraction pipeline.
- [x] Surface counts and error diagnostics in logs or metrics.
- [x] Keep JSON usage behind an explicit debug flag.

## Scope 9: Documentation and developer workflows

**Intent**
Ensure developers know how to regenerate `scip_pb2`, run scip-python inside the pipeline, and
override identity values when needed.

**Code patterns**

```markdown
## SCIP toolchain
- Regenerate protobuf bindings: `scripts/scip_proto_codegen.py`
- Run pipeline indexing: `uv run -- <pipeline entry>`
- Override identity: set `ScipIdentityOverrides(...)`
- Ensure `build/scip/` is available and tracked for persisted bindings.
```

**Target files**
- `docs/plans/scip_protobuf_best_in_class_plan.md`
- `README.md` (optional)

**Implementation checklist**
- [x] Document regen steps and scip.proto discovery/download behavior in this plan.
- [x] Document identity override knobs and defaults in this plan.
- [x] Document fallback behavior and how to opt in (JSON fallback).
- [x] Document the `build/scip/` artifact location and git ignore exceptions.
- [x] Add README updates for tooling prerequisites (`gh auth login`, `scip`, `protoc`).

## Scope 10: Arrow/Acero alignment for SCIP outputs

**Intent**
Bring SCIP extraction and normalization fully in line with the Arrow/Acero columnar plan:
vectorized IDs, shared empty table utilities, shared schema alignment, and finalize-only
ordering.

**Code patterns**

```python
from arrowdsl.ids import hash64_from_columns

hashes = hash64_from_columns(scip_occurrences, cols=["path", "bstart", "bend"], prefix="scip_occ")
occ_id = pc.binary_join_element_wise(pa.scalar("scip_occ"), pc.cast(hashes, pa.string()), ":")
```

```python
from arrowdsl.empty import empty_table

return empty_table(SCIP_OCCURRENCES_SCHEMA)
```

```python
from arrowdsl.schema import align_to_schema

aligned, info = align_to_schema(
    scip_occurrences,
    schema=SCIP_OCCURRENCES_SCHEMA,
    safe_cast=True,
    on_error="keep",
)
```

**Target files**
- `src/extract/scip_extract.py`
- `src/normalize/spans.py`
- `src/normalize/diagnostics.py`
- `src/cpg/build_edges.py`
- `src/cpg/build_props.py`
- `src/arrowdsl/empty.py`
- `src/arrowdsl/ids.py`
- `src/arrowdsl/schema.py`

**Implementation checklist**
- [x] Replace `stable_id` loops with vectorized `hash64_from_arrays` for SCIP tables.
- [x] Replace `_empty` helper in `scip_extract` with `arrowdsl.empty.empty_table`.
- [x] Align SCIP tables to schemas via `arrowdsl.schema.align_to_schema` where needed.
- [x] Ensure no canonical ordering is introduced pre-finalize for SCIP outputs.
- [x] Add tests that validate vectorized ID stability for SCIP tables.

## Scope 11: Columnar enrichment upgrades (PyArrow-intensive)

**Intent**
Push SCIP extraction and enrichment deeper into columnar compute to improve performance,
reduce Python loops, and align with Acero plan-lane execution.

**Code patterns**

```python
# Example: role flag derivation with compute kernels
roles = pc.cast(scip_occurrences["symbol_roles"], pa.int64())
scip_role_generated = pc.not_equal(
    pc.bit_wise_and(roles, pa.scalar(SCIP_ROLE_GENERATED)),
    pa.scalar(0),
)
```

```python
# Example: signature_documentation as struct with nested occurrences
occ_struct = pa.StructArray.from_arrays(
    [symbols, roles, ranges],
    names=["symbol", "symbol_roles", "range"],
)
signature_doc = pa.StructArray.from_arrays(
    [texts, languages, occ_struct],
    names=["text", "language", "occurrences"],
)
```

**Target files**
- `src/extract/scip_extract.py`
- `src/cpg/build_props.py`
- `src/cpg/build_edges.py`
- `src/hamilton_pipeline/modules/cpg_build.py`
- `src/relspec/compiler.py` (if scip relationships are surfaced via relspec)

**Implementation checklist**
- [ ] Convert scip extraction row building to columnar array construction where practical (documents,
      occurrences, and diagnostics now columnar; symbol/external/relationship rows still list-based).
- [x] Promote `signature_documentation` to a struct with nested occurrences.
- [x] Apply dictionary encoding to repeated SCIP string columns for memory savings.
- [x] Replace role flag derivation in props with Arrow compute kernels.
- [x] Integrate `scip_symbol_relationships` into CPG edges.

## Acceptance gates
- `uv run ruff check --fix`
- `uv run pyrefly check`
- `uv run pyright --warnings --pythonversion=3.14`
