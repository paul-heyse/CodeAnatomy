# Part IX: Public API Reference

## Overview

CodeAnatomy's public API surface consists of a single entry point function `build_graph_product()` that accepts a request object and returns a typed result. This design intentionally abstracts the internal Hamilton pipeline orchestration, providing a stable contract for external callers.

The API follows a request-response pattern where all execution parameters are bundled into `GraphProductBuildRequest` and all outputs are returned via `GraphProductBuildResult`. This architecture enables comprehensive configuration while maintaining backward compatibility as internal implementation details evolve.

**Primary Module:** `src/graph/product_build.py`

**Core Philosophy:**
- **Single Entry Point** - All builds go through `build_graph_product()`
- **Typed Contracts** - Dataclass-based configuration and results
- **Output Stability** - Result fields never reference internal Hamilton node names
- **Graceful Defaults** - Minimal configuration required, extensive customization available

## Complete API Surface

### Primary Entry Point

```python
from graph.product_build import build_graph_product, GraphProductBuildRequest

result = build_graph_product(GraphProductBuildRequest(repo_root="/path/to/repo"))
```

**Function Signature:**
```python
def build_graph_product(request: GraphProductBuildRequest) -> GraphProductBuildResult:
    """Build the requested graph product and return typed outputs.

    Returns
    -------
    GraphProductBuildResult
        Typed outputs for the requested graph product.
    """
```

**Invocation Lifecycle:**
1. Resolve and validate paths (repo root, output directory)
2. Apply configuration overrides and generate run ID
3. Configure OpenTelemetry with repository context
4. Execute Hamilton pipeline via `execute_pipeline()`
5. Parse pipeline outputs into stable result structure
6. Return `GraphProductBuildResult` with all output paths and metadata

## GraphProductBuildRequest

**Definition:** `src/graph/product_build.py` (lines 92-123)

The request object bundles all execution parameters for a graph build.

### Field Reference

| Field Name | Type | Default | Description |
|------------|------|---------|-------------|
| `repo_root` | `PathLike` | (required) | Absolute or relative path to repository root. Resolved to absolute path before execution. |
| `product` | `GraphProduct` | `"cpg"` | Graph product to build. Currently only `"cpg"` is supported. |
| `execution_mode` | `ExecutionMode` | `PLAN_PARALLEL` | Hamilton execution strategy. See [ExecutionMode](#executionmode). |
| `executor_config` | `ExecutorConfig \| None` | `None` | Executor configuration for parallel execution. See [ExecutorConfig](#executorconfig). |
| `graph_adapter_config` | `GraphAdapterConfig \| None` | `None` | Graph adapter for non-dynamic backends (Dask, Ray). See [GraphAdapterConfig](#graphadapterconfig). |
| `output_dir` | `PathLike \| None` | `None` | Output directory for Delta tables. Defaults to `{repo_root}/build`. |
| `work_dir` | `PathLike \| None` | `None` | Working directory for intermediate files. Defaults to `None` (no intermediate persistence). |
| `runtime_profile_name` | `str \| None` | `None` | Runtime profile name for DataFusion configuration presets. |
| `determinism_override` | `DeterminismTier \| None` | `None` | Override determinism tier. See [DeterminismTier](#determinismtier). |
| `scip_index_config` | `ScipIndexConfig \| None` | `None` | SCIP indexing configuration. See [ScipIndexConfig](#scipindexconfig). |
| `scip_identity_overrides` | `ScipIdentityOverrides \| None` | `None` | Override SCIP project identity. See [ScipIdentityOverrides](#scipidentityoverrides). |
| `writer_strategy` | `WriterStrategy \| None` | `None` | Writer strategy for Delta table writes: `"arrow"` or `"datafusion"`. |
| `incremental_config` | `IncrementalConfig \| None` | `None` | Incremental processing configuration. See [IncrementalConfig](#incrementalconfig). |
| `incremental_impact_strategy` | `ImpactStrategy \| None` | `None` | Impact analysis strategy: `"hybrid"`, `"symbol_closure"`, or `"import_closure"`. |
| `include_quality` | `bool` | `True` | Include CPG quality validation tables in outputs. |
| `include_extract_errors` | `bool` | `True` | Include extraction error artifacts in outputs. |
| `include_manifest` | `bool` | `True` | Include run manifest metadata in outputs. |
| `include_run_bundle` | `bool` | `True` | Include full run bundle directory with diagnostics. |
| `config` | `Mapping[str, JsonValue]` | `{}` | Additional configuration key-value pairs passed to Hamilton. |
| `overrides` | `Mapping[str, object] \| None` | `None` | Direct Hamilton input overrides (advanced usage). |
| `use_materialize` | `bool` | `True` | Use Hamilton's `materialize()` API instead of `execute()`. |

### Field Details

#### repo_root

**Type:** `PathLike` (str or Path)

**Required:** Yes

**Description:** Repository root directory to analyze. Can be absolute or relative. Relative paths are resolved against the current working directory before execution.

**Resolution Logic:**
```python
repo_root_path = ensure_path(request.repo_root).resolve()
```

**Example:**
```python
GraphProductBuildRequest(repo_root="/home/user/my-project")
GraphProductBuildRequest(repo_root="./my-project")  # Resolved to absolute path
```

#### product

**Type:** `GraphProduct` (Literal["cpg"])

**Default:** `"cpg"`

**Description:** The graph product to build. Currently only `"cpg"` (Code Property Graph) is supported.

**Product Version:** Derived as `f"cpg_ultimate_v{SCHEMA_VERSION}"` where `SCHEMA_VERSION` is defined in `src/cpg/schemas.py`.

#### execution_mode

**Type:** `ExecutionMode` (enum)

**Default:** `ExecutionMode.PLAN_PARALLEL`

**Description:** Determines Hamilton orchestration strategy. See [ExecutionMode](#executionmode) for detailed mode descriptions.

**Valid Values:**
- `ExecutionMode.DETERMINISTIC_SERIAL` - Single-threaded sequential execution
- `ExecutionMode.PLAN_PARALLEL` - Plan-aware parallel execution with local workers
- `ExecutionMode.PLAN_PARALLEL_REMOTE` - Plan-aware parallel with remote workers (Dask/Ray)

#### executor_config

**Type:** `ExecutorConfig | None`

**Default:** `None`

**Description:** Configuration for parallel execution backends. Only relevant when `execution_mode` is `PLAN_PARALLEL` or `PLAN_PARALLEL_REMOTE`.

See [ExecutorConfig](#executorconfig) for field details.

#### output_dir

**Type:** `PathLike | None`

**Default:** `None` (resolves to `{repo_root}/build`)

**Description:** Directory where Delta Lake tables and final outputs are written.

**Resolution Logic:**
```python
if output_dir is None:
    return repo_root / "build"
resolved = ensure_path(output_dir)
return resolved if resolved.is_absolute() else repo_root / resolved
```

**Example:**
```python
# Default: /home/user/my-project/build
GraphProductBuildRequest(repo_root="/home/user/my-project")

# Custom absolute path
GraphProductBuildRequest(
    repo_root="/home/user/my-project",
    output_dir="/mnt/storage/outputs"
)

# Custom relative path (resolved against repo_root)
GraphProductBuildRequest(
    repo_root="/home/user/my-project",
    output_dir="custom_output"  # -> /home/user/my-project/custom_output
)
```

#### work_dir

**Type:** `PathLike | None`

**Default:** `None`

**Description:** Working directory for intermediate materialization. When `None`, intermediate results are kept in memory. Setting a path enables disk-based intermediate persistence.

**Use Cases:**
- Large repositories that exceed available memory
- Debugging pipeline stages
- Incremental reruns where intermediate caching is beneficial

#### runtime_profile_name

**Type:** `str | None`

**Default:** `None`

**Description:** Named runtime profile for DataFusion configuration presets. Profiles control memory limits, batch sizes, and execution parameters.

**Example:**
```python
GraphProductBuildRequest(
    repo_root="/path/to/repo",
    runtime_profile_name="high_memory"
)
```

#### determinism_override

**Type:** `DeterminismTier | None`

**Default:** `None`

**Description:** Override the determinism tier for plan execution. See [DeterminismTier](#determinismtier).

#### scip_index_config

**Type:** `ScipIndexConfig | None`

**Default:** `None`

**Description:** Configuration for SCIP (SCIP Code Intelligence Protocol) indexing. See [ScipIndexConfig](#scipindexconfig).

#### scip_identity_overrides

**Type:** `ScipIdentityOverrides | None`

**Default:** `None`

**Description:** Override SCIP project identity fields. See [ScipIdentityOverrides](#scipidentityoverrides).

#### writer_strategy

**Type:** `WriterStrategy | None`

**Default:** `None`

**Description:** Writer implementation for Delta table writes.

**Valid Values:**
- `"arrow"` - PyArrow-based writer (default, recommended)
- `"datafusion"` - DataFusion-based writer (experimental)

#### incremental_config

**Type:** `IncrementalConfig | None`

**Default:** `None`

**Description:** Configuration for incremental processing mode. See [IncrementalConfig](#incrementalconfig).

#### incremental_impact_strategy

**Type:** `ImpactStrategy | None`

**Default:** `None`

**Description:** Strategy for computing impact scope in incremental mode.

**Valid Values:**
- `"hybrid"` - Combine symbol and import closures (default)
- `"symbol_closure"` - Track symbol-level dependencies only
- `"import_closure"` - Track module-level import dependencies only

#### include_quality

**Type:** `bool`

**Default:** `True`

**Description:** Include quality validation tables (`cpg_nodes_quality`, `cpg_props_quality`) in pipeline outputs.

**Effect:** When `False`, quality tables are omitted from `GraphProductBuildResult`:
```python
result.cpg_nodes_quality  # None when include_quality=False
result.cpg_props_quality  # None when include_quality=False
```

#### include_extract_errors

**Type:** `bool`

**Default:** `True`

**Description:** Include extraction error artifacts table in outputs.

**Effect:** When `False`:
```python
result.extract_error_artifacts  # None when include_extract_errors=False
```

#### include_manifest

**Type:** `bool`

**Default:** `True`

**Description:** Include run manifest metadata in outputs.

**Effect:** When `False`:
```python
result.manifest_path  # None when include_manifest=False
```

#### include_run_bundle

**Type:** `bool`

**Default:** `True`

**Description:** Include run bundle directory with diagnostics, telemetry, and execution metadata.

**Effect:** When `False`:
```python
result.run_bundle_dir  # None when include_run_bundle=False
```

#### config

**Type:** `Mapping[str, JsonValue]`

**Default:** `{}`

**Description:** Additional configuration parameters passed to the Hamilton pipeline. Common keys include cache configuration and diagnostic settings.

**Common Configuration Keys:**

| Key | Type | Description |
|-----|------|-------------|
| `cache_path` | `str` | Path to Hamilton disk cache directory |
| `cache_log_to_file` | `bool` | Enable cache operation logging |
| `cache_policy_profile` | `str` | Cache eviction policy profile name |

**Example:**
```python
GraphProductBuildRequest(
    repo_root="/path/to/repo",
    config={
        "cache_path": "/tmp/hamilton_cache",
        "cache_log_to_file": True,
        "cache_policy_profile": "aggressive",
    }
)
```

#### overrides

**Type:** `Mapping[str, object] | None`

**Default:** `None`

**Description:** Direct overrides for Hamilton inputs. Advanced feature for custom pipeline integration.

**Warning:** This is an advanced feature that bypasses normal configuration processing. Only use when integrating custom Hamilton drivers or debugging.

**Example:**
```python
GraphProductBuildRequest(
    repo_root="/path/to/repo",
    overrides={
        "run_id": "custom-run-id-2024-01-15",
        "custom_param": custom_value,
    }
)
```

#### use_materialize

**Type:** `bool`

**Default:** `True`

**Description:** Use Hamilton's `materialize()` API instead of `execute()`. Materialization provides richer output metadata.

**Effect:** When `False`, falls back to Hamilton's `execute()` API. Generally should remain `True` unless debugging.

## GraphProductBuildResult

**Definition:** `src/graph/product_build.py` (lines 64-89)

The result object contains all output paths, metadata, and metrics from a successful graph build.

### Field Reference

| Field Name | Type | Description |
|------------|------|-------------|
| `product` | `GraphProduct` | The product type built (`"cpg"`). |
| `product_version` | `str` | Product version string (e.g., `"cpg_ultimate_v1"`). |
| `engine_versions` | `Mapping[str, str]` | Engine dependency versions (`pyarrow`, `datafusion`). |
| `run_id` | `str \| None` | Unique run identifier (UUID7). |
| `repo_root` | `Path` | Resolved absolute repository root path. |
| `output_dir` | `Path` | Resolved absolute output directory path. |
| `cpg_nodes` | `FinalizeDeltaReport` | CPG nodes table output. See [FinalizeDeltaReport](#finalizeddeltareport). |
| `cpg_edges` | `FinalizeDeltaReport` | CPG edges table output. |
| `cpg_props` | `FinalizeDeltaReport` | CPG properties table output. |
| `cpg_props_map` | `TableDeltaReport` | CPG property map table output. See [TableDeltaReport](#tableddeltareport). |
| `cpg_edges_by_src` | `TableDeltaReport` | CPG edges indexed by source node. |
| `cpg_edges_by_dst` | `TableDeltaReport` | CPG edges indexed by destination node. |
| `cpg_nodes_quality` | `TableDeltaReport \| None` | CPG nodes quality validation table (when `include_quality=True`). |
| `cpg_props_quality` | `TableDeltaReport \| None` | CPG properties quality validation table (when `include_quality=True`). |
| `extract_error_artifacts` | `JsonDict \| None` | Extraction error artifacts metadata (when `include_extract_errors=True`). |
| `manifest_path` | `Path \| None` | Run manifest Delta table path (when `include_manifest=True`). |
| `run_bundle_dir` | `Path \| None` | Run bundle directory path (when `include_run_bundle=True`). |
| `pipeline_outputs` | `Mapping[str, JsonDict \| None]` | Raw Hamilton pipeline outputs (advanced usage). |

### Field Details

#### product and product_version

**product Type:** `GraphProduct` (Literal["cpg"])

**product_version Type:** `str`

**Description:** Product type and version identifier.

**Version Format:** `"{product}_ultimate_v{SCHEMA_VERSION}"` where `SCHEMA_VERSION` is from `src/cpg/schemas.py`.

**Example:**
```python
result.product          # "cpg"
result.product_version  # "cpg_ultimate_v1"
```

#### engine_versions

**Type:** `Mapping[str, str]`

**Description:** Dictionary mapping engine component names to their installed versions.

**Common Keys:**
- `"pyarrow"` - PyArrow version
- `"datafusion"` - DataFusion (datafusion-python) version

**Example:**
```python
result.engine_versions
# {"pyarrow": "18.1.0", "datafusion": "50.1.0"}
```

**Resolution Logic:** Attempts to resolve package versions via `importlib.metadata.version()`, returning empty dict if packages are not installed.

#### run_id

**Type:** `str | None`

**Description:** Unique identifier for this execution run. Generated as UUID7 (time-ordered UUID) unless overridden via `overrides`.

**Format:** UUID7 string representation

**Example:**
```python
result.run_id  # "01935a1e-4b4c-7890-abcd-0123456789ab"
```

**Sources (priority order):**
1. `run_bundle_dir.name` (when `include_run_bundle=True`)
2. `manifest.run_id` (when `include_manifest=True`)
3. `overrides["run_id"]`
4. Auto-generated UUID7

#### repo_root and output_dir

**Types:** `Path`

**Description:** Resolved absolute paths for repository root and output directory.

**Example:**
```python
result.repo_root   # Path("/home/user/my-project")
result.output_dir  # Path("/home/user/my-project/build")
```

#### cpg_nodes, cpg_edges, cpg_props

**Type:** `FinalizeDeltaReport`

**Description:** Core CPG table outputs with finalization metadata. See [FinalizeDeltaReport](#finalizeddeltareport).

**Tables:**
- `cpg_nodes` - CPG node entities (files, modules, classes, functions, etc.)
- `cpg_edges` - CPG relationships between nodes
- `cpg_props` - CPG node properties (attributes, metadata, annotations)

**Example:**
```python
result.cpg_nodes.paths.data  # Path to cpg_nodes Delta table
result.cpg_nodes.rows        # Number of nodes emitted
result.cpg_nodes.error_rows  # Number of rows with validation errors

result.cpg_edges.paths.data  # Path to cpg_edges Delta table
result.cpg_edges.rows        # Number of edges emitted

result.cpg_props.paths.data  # Path to cpg_props Delta table
result.cpg_props.rows        # Number of properties emitted
```

#### cpg_props_map, cpg_edges_by_src, cpg_edges_by_dst

**Type:** `TableDeltaReport`

**Description:** Auxiliary index tables for efficient CPG queries. See [TableDeltaReport](#tableddeltareport).

**Tables:**
- `cpg_props_map` - Property name to property ID mapping
- `cpg_edges_by_src` - Edge index keyed by source node
- `cpg_edges_by_dst` - Edge index keyed by destination node

**Example:**
```python
result.cpg_props_map.path  # Path to property map Delta table
result.cpg_props_map.rows  # Number of property mappings

result.cpg_edges_by_src.path  # Path to source-keyed edge index
result.cpg_edges_by_dst.path  # Path to destination-keyed edge index
```

#### cpg_nodes_quality and cpg_props_quality

**Type:** `TableDeltaReport | None`

**Description:** Quality validation tables containing schema conformance checks and validation errors. Only present when `include_quality=True`.

**Schema:** Validation failure records with rule identifiers and error messages.

**Example:**
```python
if result.cpg_nodes_quality:
    print(f"Quality table: {result.cpg_nodes_quality.path}")
    print(f"Validation failures: {result.cpg_nodes_quality.rows}")
```

#### extract_error_artifacts

**Type:** `JsonDict | None`

**Description:** Extraction error artifacts metadata. Only present when `include_extract_errors=True`.

**Schema:** JSON dictionary with extraction failure details, file paths, and error messages.

**Example:**
```python
if result.extract_error_artifacts:
    error_path = result.extract_error_artifacts.get("path")
    error_count = result.extract_error_artifacts.get("rows", 0)
```

#### manifest_path

**Type:** `Path | None`

**Description:** Path to run manifest Delta table. Only present when `include_manifest=True`.

**Manifest Schema:** Run metadata including configuration snapshot, timestamps, and execution parameters.

**Example:**
```python
if result.manifest_path:
    # Read manifest with DuckDB or Delta Lake reader
    manifest_df = duckdb.read_delta(str(result.manifest_path))
```

#### run_bundle_dir

**Type:** `Path | None`

**Description:** Run bundle directory containing execution artifacts. Only present when `include_run_bundle=True`.

**Bundle Contents:**
- Diagnostics logs
- OpenTelemetry traces
- Execution metrics
- Pipeline metadata

**Example:**
```python
if result.run_bundle_dir:
    diagnostics_path = result.run_bundle_dir / "diagnostics.jsonl"
    telemetry_path = result.run_bundle_dir / "telemetry"
```

#### pipeline_outputs

**Type:** `Mapping[str, JsonDict | None]`

**Description:** Raw Hamilton pipeline output metadata. Advanced field for debugging and custom integrations.

**Keys:** Hamilton node names (e.g., `"write_cpg_nodes_delta"`, `"write_cpg_edges_delta"`)

**Example:**
```python
# Advanced usage: inspect raw Hamilton outputs
for node_name, metadata in result.pipeline_outputs.items():
    if metadata:
        print(f"{node_name}: {metadata}")
```

## Supporting Types

### FinalizeDeltaReport

**Definition:** `src/graph/product_build.py` (lines 47-53)

Report structure for finalized Delta table outputs with error tracking.

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `paths` | `FinalizeDeltaPaths` | Output paths bundle. |
| `rows` | `int` | Number of valid rows written. |
| `error_rows` | `int` | Number of rows with validation errors. |

**Example:**
```python
report: FinalizeDeltaReport = result.cpg_nodes

print(f"Data table: {report.paths.data}")
print(f"Valid rows: {report.rows}")
print(f"Error rows: {report.error_rows}")
```

### FinalizeDeltaPaths

**Definition:** `src/graph/product_build.py` (lines 37-44)

Bundle of paths returned by finalized Delta writes.

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `data` | `Path` | Main data table path. |
| `errors` | `Path` | Validation errors table path. |
| `stats` | `Path` | Table statistics path. |
| `alignment` | `Path` | Schema alignment metadata path. |

**Example:**
```python
paths: FinalizeDeltaPaths = result.cpg_nodes.paths

data_path = paths.data          # Main Delta table
errors_path = paths.errors      # Validation failures
stats_path = paths.stats        # Statistics summary
alignment_path = paths.alignment # Schema alignment log
```

### TableDeltaReport

**Definition:** `src/graph/product_build.py` (lines 56-61)

Simplified report structure for single-table Delta outputs.

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `path` | `Path` | Delta table path. |
| `rows` | `int` | Number of rows written. |

**Example:**
```python
report: TableDeltaReport = result.cpg_props_map

print(f"Table path: {report.path}")
print(f"Row count: {report.rows}")
```

### ExecutionMode

**Definition:** `src/hamilton_pipeline/pipeline_types.py` (lines 184-189)

Enum controlling Hamilton orchestration strategy.

**Values:**

| Mode | Description | Use Case |
|------|-------------|----------|
| `DETERMINISTIC_SERIAL` | Single-threaded sequential execution. No parallelism. | Debugging, minimal overhead, deterministic output order. |
| `PLAN_PARALLEL` | Plan-aware parallel execution with local workers (ThreadPool or ProcessPool). | Most common mode. Balances performance and resource usage. |
| `PLAN_PARALLEL_REMOTE` | Plan-aware parallel execution with remote workers (Dask, Ray). | Large-scale distributed execution. Requires `graph_adapter_config`. |

**Selection Guidelines:**

**DETERMINISTIC_SERIAL:**
- Use for debugging pipeline issues
- Guarantee deterministic execution order
- Single-file or small codebases
- CI/CD environments with strict resource constraints

**PLAN_PARALLEL (Recommended):**
- Default for production workloads
- Automatically schedules tasks based on dependency graph
- Respects `executor_config.max_tasks` for parallelism
- Best for medium to large repositories (100-10,000 files)

**PLAN_PARALLEL_REMOTE:**
- Use for very large repositories (>10,000 files)
- Requires Dask or Ray cluster setup
- Configure via `graph_adapter_config`
- Best for distributed compute environments

**Example:**
```python
from hamilton_pipeline.pipeline_types import ExecutionMode

# Serial execution (debugging)
GraphProductBuildRequest(
    repo_root="/path/to/repo",
    execution_mode=ExecutionMode.DETERMINISTIC_SERIAL
)

# Parallel execution (default, recommended)
GraphProductBuildRequest(
    repo_root="/path/to/repo",
    execution_mode=ExecutionMode.PLAN_PARALLEL,
    executor_config=ExecutorConfig(kind="multiprocessing", max_tasks=8)
)

# Remote parallel execution (large scale)
GraphProductBuildRequest(
    repo_root="/path/to/repo",
    execution_mode=ExecutionMode.PLAN_PARALLEL_REMOTE,
    graph_adapter_config=GraphAdapterConfig(kind="dask")
)
```

### ExecutorConfig

**Definition:** `src/hamilton_pipeline/pipeline_types.py` (lines 193-204)

Configuration for plan-aware parallel execution.

**Fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `kind` | `ExecutorKind` | `"multiprocessing"` | Local executor type: `"threadpool"`, `"multiprocessing"`, `"dask"`, `"ray"`. |
| `max_tasks` | `int` | `4` | Maximum parallel workers. |
| `remote_kind` | `ExecutorKind \| None` | `None` | Remote executor type (for `PLAN_PARALLEL_REMOTE` mode). |
| `remote_max_tasks` | `int \| None` | `None` | Maximum remote workers. |
| `cost_threshold` | `float \| None` | `None` | Task cost threshold for local vs remote execution. |
| `ray_init_config` | `Mapping[str, JsonValue] \| None` | `None` | Ray initialization parameters. |
| `dask_scheduler` | `str \| None` | `None` | Dask scheduler address. |
| `dask_client_kwargs` | `Mapping[str, JsonValue] \| None` | `None` | Dask client keyword arguments. |

**Executor Kind Values:**

| Kind | Description | Best For |
|------|-------------|----------|
| `"threadpool"` | Thread-based parallelism. | I/O-bound tasks, lightweight operations. |
| `"multiprocessing"` | Process-based parallelism (default). | CPU-bound tasks, Python GIL avoidance. |
| `"dask"` | Dask distributed backend. | Large-scale distributed execution. |
| `"ray"` | Ray distributed backend. | Large-scale distributed execution with advanced scheduling. |

**Example:**
```python
from hamilton_pipeline.pipeline_types import ExecutorConfig

# Default multiprocessing
ExecutorConfig(kind="multiprocessing", max_tasks=8)

# Thread pool for I/O-bound tasks
ExecutorConfig(kind="threadpool", max_tasks=16)

# Dask distributed
ExecutorConfig(
    kind="dask",
    max_tasks=32,
    dask_scheduler="tcp://scheduler:8786"
)

# Ray distributed
ExecutorConfig(
    kind="ray",
    max_tasks=64,
    ray_init_config={"address": "auto", "num_cpus": 64}
)
```

### GraphAdapterConfig

**Definition:** `src/hamilton_pipeline/pipeline_types.py` (lines 207-212)

Configuration for non-dynamic graph execution backends (Dask, Ray).

**Fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `kind` | `GraphAdapterKind` | (required) | Adapter type: `"threadpool"`, `"dask"`, `"ray"`. |
| `options` | `Mapping[str, JsonValue] \| None` | `None` | Backend-specific options. |

**Adapter Kind Values:**

| Kind | Description |
|------|-------------|
| `"threadpool"` | Thread-based execution (local). |
| `"dask"` | Dask distributed backend. |
| `"ray"` | Ray distributed backend. |

**Example:**
```python
from hamilton_pipeline.pipeline_types import GraphAdapterConfig

# Dask adapter
GraphAdapterConfig(
    kind="dask",
    options={"scheduler": "tcp://scheduler:8786"}
)

# Ray adapter
GraphAdapterConfig(
    kind="ray",
    options={"address": "auto", "num_cpus": 32}
)
```

### DeterminismTier

**Definition:** `src/core_types.py` (lines 33-41)

Determinism budget controlling output ordering and stability guarantees.

**Values:**

| Tier | Description | Sorting Overhead |
|------|-------------|------------------|
| `CANONICAL` | Fully deterministic output with complete sorting. | High (full sort on all keys). |
| `STABLE_SET` | Set-stable output with partial sorting. | Medium (partial sort on primary keys). |
| `BEST_EFFORT` | Best-effort determinism with minimal sorting. | Low (no sorting overhead). |

**Aliases:**
- `FAST` = `BEST_EFFORT`
- `STABLE` = `STABLE_SET`

**Selection Guidelines:**

**CANONICAL:**
- Use for reproducible builds requiring exact output order
- Testing and CI/CD where output diffs must be stable
- Archival and compliance scenarios
- **Trade-off:** Highest overhead due to complete sorting

**STABLE_SET:**
- Use for reproducible builds where element sets must match
- Order within sets may vary but set membership is stable
- **Trade-off:** Medium overhead with partial sorting

**BEST_EFFORT (Default):**
- Use for performance-critical builds
- Output correctness guaranteed but order may vary
- **Trade-off:** Minimal overhead, fastest execution

**Example:**
```python
from core_types import DeterminismTier

# Maximum determinism (slowest)
GraphProductBuildRequest(
    repo_root="/path/to/repo",
    determinism_override=DeterminismTier.CANONICAL
)

# Balanced determinism (recommended for testing)
GraphProductBuildRequest(
    repo_root="/path/to/repo",
    determinism_override=DeterminismTier.STABLE_SET
)

# Fastest execution (default)
GraphProductBuildRequest(
    repo_root="/path/to/repo",
    determinism_override=DeterminismTier.BEST_EFFORT
)
```

### IncrementalConfig

**Definition:** `src/incremental/types.py` (lines 11-23)

Configuration for incremental processing mode.

**Fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `False` | Enable incremental processing. |
| `state_dir` | `Path \| None` | `None` | State directory for incremental metadata. Defaults to `{output_dir}/state`. |
| `repo_id` | `str \| None` | `None` | Repository identifier for state isolation. |
| `impact_strategy` | `Literal["hybrid", "symbol_closure", "import_closure"]` | `"hybrid"` | Impact analysis strategy. |
| `git_base_ref` | `str \| None` | `None` | Git base reference for diff computation. |
| `git_head_ref` | `str \| None` | `None` | Git head reference for diff computation. |
| `git_changed_only` | `bool` | `False` | Process only changed files (no impact closure). |

**Impact Strategy Details:**

| Strategy | Description | Use Case |
|----------|-------------|----------|
| `"hybrid"` | Combine symbol and import closures. | Default. Balances accuracy and scope. |
| `"symbol_closure"` | Track symbol-level dependencies. | Fine-grained incremental updates. |
| `"import_closure"` | Track module-level import dependencies. | Coarse-grained incremental updates. |

**Example:**
```python
from pathlib import Path
from incremental.types import IncrementalConfig

# Basic incremental mode
IncrementalConfig(
    enabled=True,
    state_dir=Path("/tmp/codeanatomy_state"),
    repo_id="my-project"
)

# Git diff-based incremental
IncrementalConfig(
    enabled=True,
    state_dir=Path("/tmp/codeanatomy_state"),
    repo_id="my-project",
    git_base_ref="main",
    git_head_ref="HEAD",
    impact_strategy="hybrid"
)

# Changed files only (no closure)
IncrementalConfig(
    enabled=True,
    state_dir=Path("/tmp/codeanatomy_state"),
    repo_id="my-project",
    git_changed_only=True
)
```

### ScipIndexConfig

**Definition:** `src/hamilton_pipeline/pipeline_types.py` (lines 20-44)

Configuration for SCIP (SCIP Code Intelligence Protocol) indexing.

**Fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | `bool` | `True` | Enable SCIP indexing. |
| `index_path_override` | `str \| None` | `None` | Override SCIP index file path. |
| `output_dir` | `str` | `"build/scip"` | SCIP output directory. |
| `env_json_path` | `str \| None` | `None` | Path to environment JSON for SCIP. |
| `generate_env_json` | `bool` | `False` | Auto-generate environment JSON. |
| `scip_python_bin` | `str` | `"scip-python"` | Path to scip-python binary. |
| `scip_cli_bin` | `str` | `"scip"` | Path to scip CLI binary. |
| `target_only` | `str \| None` | `None` | Index only specific target path. |
| `node_max_old_space_mb` | `int \| None` | `8192` | Node.js max old space size (MB). |
| `timeout_s` | `int \| None` | `None` | SCIP indexing timeout (seconds). |
| `extra_args` | `tuple[str, ...]` | `()` | Additional scip-python arguments. |
| `use_incremental_shards` | `bool` | `False` | Use incremental shard-based indexing. |
| `shards_dir` | `str \| None` | `None` | Directory for incremental shards. |
| `shards_manifest_path` | `str \| None` | `None` | Shard manifest file path. |
| `run_scip_print` | `bool` | `False` | Run scip print after indexing. |
| `scip_print_path` | `str \| None` | `None` | Output path for scip print. |
| `run_scip_snapshot` | `bool` | `False` | Run scip snapshot after indexing. |
| `scip_snapshot_dir` | `str \| None` | `None` | Snapshot output directory. |
| `scip_snapshot_comment_syntax` | `str` | `"#"` | Comment syntax for snapshots. |
| `run_scip_test` | `bool` | `False` | Run scip test validation. |
| `scip_test_args` | `tuple[str, ...]` | `("--check-documents",)` | scip test arguments. |

**Example:**
```python
from hamilton_pipeline.pipeline_types import ScipIndexConfig

# Basic SCIP indexing
ScipIndexConfig(
    enabled=True,
    output_dir="build/scip"
)

# Custom SCIP binary paths
ScipIndexConfig(
    enabled=True,
    scip_python_bin="/opt/scip-python/bin/scip-python",
    scip_cli_bin="/opt/scip/bin/scip",
    node_max_old_space_mb=16384
)

# Incremental SCIP indexing
ScipIndexConfig(
    enabled=True,
    use_incremental_shards=True,
    shards_dir="build/scip/shards",
    shards_manifest_path="build/scip/manifest.json"
)

# SCIP with validation
ScipIndexConfig(
    enabled=True,
    run_scip_test=True,
    scip_test_args=("--check-documents", "--check-references")
)
```

### ScipIdentityOverrides

**Definition:** `src/hamilton_pipeline/pipeline_types.py` (lines 249-255)

Override SCIP project identity fields.

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `project_name_override` | `str \| None` | Override project name in SCIP index. |
| `project_version_override` | `str \| None` | Override project version in SCIP index. |
| `project_namespace_override` | `str \| None` | Override project namespace in SCIP index. |

**Example:**
```python
from hamilton_pipeline.pipeline_types import ScipIdentityOverrides

ScipIdentityOverrides(
    project_name_override="my-custom-project",
    project_version_override="1.2.3",
    project_namespace_override="com.example"
)
```

## Complete Usage Examples

### Minimal Usage

Simplest possible invocation with all defaults:

```python
from graph.product_build import build_graph_product, GraphProductBuildRequest

result = build_graph_product(
    GraphProductBuildRequest(repo_root="/path/to/repository")
)

print(f"CPG nodes: {result.cpg_nodes.rows} rows")
print(f"CPG edges: {result.cpg_edges.rows} rows")
print(f"Output directory: {result.output_dir}")
```

**Result:**
- CPG built in `/path/to/repository/build`
- All output tables included (nodes, edges, props, quality)
- Parallel execution with default worker count
- Run bundle and manifest generated

### Production Configuration

Comprehensive production configuration with custom settings:

```python
from pathlib import Path
from graph.product_build import build_graph_product, GraphProductBuildRequest
from hamilton_pipeline.pipeline_types import (
    ExecutionMode,
    ExecutorConfig,
    ScipIndexConfig,
)
from incremental.types import IncrementalConfig
from core_types import DeterminismTier

result = build_graph_product(
    GraphProductBuildRequest(
        # Core paths
        repo_root="/home/user/my-project",
        output_dir="/mnt/storage/cpg_outputs",
        work_dir="/mnt/storage/work",

        # Execution strategy
        execution_mode=ExecutionMode.PLAN_PARALLEL,
        executor_config=ExecutorConfig(
            kind="multiprocessing",
            max_tasks=16,
        ),

        # Determinism
        determinism_override=DeterminismTier.STABLE_SET,

        # SCIP indexing
        scip_index_config=ScipIndexConfig(
            enabled=True,
            output_dir="build/scip",
            node_max_old_space_mb=16384,
            run_scip_test=True,
        ),

        # Incremental processing
        incremental_config=IncrementalConfig(
            enabled=True,
            state_dir=Path("/mnt/storage/state"),
            repo_id="my-project-id",
            impact_strategy="hybrid",
        ),

        # Output control
        include_quality=True,
        include_extract_errors=True,
        include_manifest=True,
        include_run_bundle=True,

        # Cache configuration
        config={
            "cache_path": "/mnt/storage/cache",
            "cache_log_to_file": True,
            "cache_policy_profile": "aggressive",
        },
    )
)

# Access outputs
print(f"Run ID: {result.run_id}")
print(f"Product version: {result.product_version}")
print(f"Engine versions: {result.engine_versions}")
print(f"\nCPG Statistics:")
print(f"  Nodes: {result.cpg_nodes.rows:,} ({result.cpg_nodes.error_rows} errors)")
print(f"  Edges: {result.cpg_edges.rows:,}")
print(f"  Properties: {result.cpg_props.rows:,}")

# Check quality
if result.cpg_nodes_quality:
    print(f"\nQuality Validation:")
    print(f"  Node failures: {result.cpg_nodes_quality.rows}")
    print(f"  Quality table: {result.cpg_nodes_quality.path}")

# Access Delta tables
print(f"\nDelta Table Paths:")
print(f"  Nodes: {result.cpg_nodes.paths.data}")
print(f"  Edges: {result.cpg_edges.paths.data}")
print(f"  Props: {result.cpg_props.paths.data}")
```

### Incremental Build Example

Incremental processing with Git diff-based change detection:

```python
from pathlib import Path
from graph.product_build import build_graph_product, GraphProductBuildRequest
from incremental.types import IncrementalConfig

# Initial full build
initial_result = build_graph_product(
    GraphProductBuildRequest(
        repo_root="/path/to/repo",
        incremental_config=IncrementalConfig(
            enabled=True,
            state_dir=Path("/tmp/state"),
            repo_id="my-repo",
        ),
    )
)

print(f"Initial build: {initial_result.cpg_nodes.rows} nodes")

# Incremental update (after code changes)
incremental_result = build_graph_product(
    GraphProductBuildRequest(
        repo_root="/path/to/repo",
        incremental_config=IncrementalConfig(
            enabled=True,
            state_dir=Path("/tmp/state"),
            repo_id="my-repo",
            git_base_ref="HEAD~1",  # Compare against previous commit
            git_head_ref="HEAD",
            impact_strategy="hybrid",
        ),
    )
)

print(f"Incremental build: {incremental_result.cpg_nodes.rows} nodes")
print(f"Run ID: {incremental_result.run_id}")
```

### Distributed Execution Example

Large-scale distributed execution with Dask:

```python
from graph.product_build import build_graph_product, GraphProductBuildRequest
from hamilton_pipeline.pipeline_types import (
    ExecutionMode,
    ExecutorConfig,
    GraphAdapterConfig,
)

result = build_graph_product(
    GraphProductBuildRequest(
        repo_root="/path/to/large/repository",
        output_dir="/mnt/distributed/outputs",

        execution_mode=ExecutionMode.PLAN_PARALLEL_REMOTE,

        executor_config=ExecutorConfig(
            kind="dask",
            max_tasks=64,
            dask_scheduler="tcp://dask-scheduler:8786",
        ),

        graph_adapter_config=GraphAdapterConfig(
            kind="dask",
            options={
                "scheduler": "tcp://dask-scheduler:8786",
            },
        ),
    )
)

print(f"Distributed build completed: {result.cpg_nodes.rows} nodes")
```

### Custom Run ID Example

Override run ID for external correlation:

```python
from graph.product_build import build_graph_product, GraphProductBuildRequest

result = build_graph_product(
    GraphProductBuildRequest(
        repo_root="/path/to/repo",
        overrides={
            "run_id": "pipeline-build-2024-01-15-prod",
        },
    )
)

assert result.run_id == "pipeline-build-2024-01-15-prod"
print(f"Run bundle: {result.run_bundle_dir}")
```

### Minimal Output Example

Build with minimal outputs (no quality, no manifest):

```python
from graph.product_build import build_graph_product, GraphProductBuildRequest

result = build_graph_product(
    GraphProductBuildRequest(
        repo_root="/path/to/repo",
        include_quality=False,
        include_extract_errors=False,
        include_manifest=False,
        include_run_bundle=False,
    )
)

# Only core CPG tables are present
assert result.cpg_nodes_quality is None
assert result.cpg_props_quality is None
assert result.extract_error_artifacts is None
assert result.manifest_path is None
assert result.run_bundle_dir is None

print(f"Minimal build: {result.cpg_nodes.rows} nodes")
```

### Debugging Example

Serial execution with full diagnostics:

```python
from graph.product_build import build_graph_product, GraphProductBuildRequest
from hamilton_pipeline.pipeline_types import ExecutionMode
from core_types import DeterminismTier

result = build_graph_product(
    GraphProductBuildRequest(
        repo_root="/path/to/repo",

        # Serial execution for debugging
        execution_mode=ExecutionMode.DETERMINISTIC_SERIAL,

        # Canonical determinism for reproducible outputs
        determinism_override=DeterminismTier.CANONICAL,

        # Enable all diagnostics
        include_quality=True,
        include_extract_errors=True,
        include_manifest=True,
        include_run_bundle=True,
    )
)

# Inspect diagnostics
if result.run_bundle_dir:
    print(f"Diagnostics in: {result.run_bundle_dir}")
    diagnostics_file = result.run_bundle_dir / "diagnostics.jsonl"
    if diagnostics_file.exists():
        print(f"Diagnostics log: {diagnostics_file}")

# Check for errors
if result.extract_error_artifacts:
    print("Extraction errors detected")

if result.cpg_nodes_quality and result.cpg_nodes_quality.rows > 0:
    print(f"Quality failures: {result.cpg_nodes_quality.rows}")
```

## Error Handling

### Return Type Guarantees

`build_graph_product()` always returns `GraphProductBuildResult` on success. Errors are raised as exceptions.

### Common Exception Types

| Exception | Cause | Mitigation |
|-----------|-------|------------|
| `FileNotFoundError` | `repo_root` does not exist | Verify path before calling |
| `PermissionError` | Cannot write to `output_dir` | Check directory permissions |
| `ValueError` | Invalid configuration parameter | Validate config values |
| `RuntimeError` | Pipeline execution failure | Check logs in `run_bundle_dir` |
| `ImportError` | Missing SCIP binaries (when SCIP enabled) | Install scip-python or disable SCIP |

### Error Handling Pattern

```python
from graph.product_build import build_graph_product, GraphProductBuildRequest
from pathlib import Path

try:
    result = build_graph_product(
        GraphProductBuildRequest(repo_root="/path/to/repo")
    )
except FileNotFoundError as exc:
    print(f"Repository not found: {exc}")
except PermissionError as exc:
    print(f"Permission denied: {exc}")
except ValueError as exc:
    print(f"Invalid configuration: {exc}")
except RuntimeError as exc:
    print(f"Pipeline execution failed: {exc}")
    # Check diagnostics if available
    if Path("/path/to/repo/build").exists():
        print("Check logs in build directory")
else:
    # Success path
    print(f"Build succeeded: {result.cpg_nodes.rows} nodes")

    # Check for soft failures
    if result.cpg_nodes.error_rows > 0:
        print(f"Warning: {result.cpg_nodes.error_rows} validation errors")
        print(f"Error table: {result.cpg_nodes.paths.errors}")
```

### Diagnostics Access

When execution fails, diagnostics are available in the run bundle:

```python
from graph.product_build import build_graph_product, GraphProductBuildRequest
from pathlib import Path
import json

try:
    result = build_graph_product(
        GraphProductBuildRequest(
            repo_root="/path/to/repo",
            include_run_bundle=True,
        )
    )
except Exception as exc:
    print(f"Build failed: {exc}")

    # Try to access diagnostics
    bundle_dir = Path("/path/to/repo/build/run_bundle")
    if bundle_dir.exists():
        diagnostics_file = bundle_dir / "diagnostics.jsonl"
        if diagnostics_file.exists():
            with diagnostics_file.open() as f:
                for line in f:
                    event = json.loads(line)
                    print(f"[{event['timestamp']}] {event['message']}")
```

## Cross-References

**Related Architecture Documentation:**
- [Part II: Extraction Stage](./part_ii_extraction.md) - Details on extraction pipeline configuration
- [Part IV: Normalization and Scheduling](./part_iv_normalization_and_scheduling.md) - Scheduling and dependency inference
- [Part V: Storage and Incremental](./part_v_storage_and_incremental.md) - Delta Lake configuration and incremental processing
- [Part VI: CPG Build and Orchestration](./part_vi_cpg_build_and_orchestration.md) - Hamilton pipeline orchestration
- [Part VII: Observability](./part_vii_observability.md) - OpenTelemetry integration and diagnostics
- [Configuration Reference](./configuration_reference.md) - Environment variable configuration
- [Utilities Reference](./utilities_reference.md) - Shared utility modules

**Source Files:**
- `src/graph/product_build.py` - Public API entry point
- `src/hamilton_pipeline/execution.py` - Pipeline execution
- `src/hamilton_pipeline/pipeline_types.py` - Configuration types
- `src/core_types.py` - Base type definitions
- `src/incremental/types.py` - Incremental processing types
