# System Overview & Public API

## Purpose

This document provides the entry point for understanding CodeAnatomy's architecture. It covers the system's goals, the four-stage pipeline model, key invariants, technology stack, module organization, and the public API for building Code Property Graphs.

## Key Concepts

- **Multi-Source Evidence Fusion** - Different parsing technologies (AST, CST, symtable, bytecode, SCIP, tree-sitter) capture complementary aspects of code structure
- **Inference-Driven Dependencies** - Task dependencies are automatically inferred from DataFusion query plans via native lineage extraction
- **Deterministic Contract-Validated Transformations** - All pipeline stages produce reproducible outputs via plan fingerprinting and schema contracts
- **Single Entry Point** - All builds go through `build_graph_product()` with typed request/response objects
- **Polars + Arrow Only** - Public API surfaces exchange tabular data via Polars or Arrow; pandas is not supported

---

## Four-Stage Pipeline

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            CodeAnatomy Pipeline                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Stage 1: EXTRACTION                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  repo_scan() → extract_ast() → extract_cst() → extract_symtable()       │   │
│  │  → extract_bytecode() → extract_scip() → extract_tree_sitter()          │   │
│  │                                                                          │   │
│  │  Output: Evidence tables (repo_files_v1, ast_files_v1, libcst_files_v1, │   │
│  │          symtable_files_v1, bytecode_files_v1, scip_files_v1, ...)      │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                      │                                          │
│                                      ▼                                          │
│  Stage 2: NORMALIZATION                                                         │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  df_view_builders.py → Canonical byte spans, stable IDs, join-ready     │   │
│  │                                                                          │   │
│  │  Output: Normalized views (type_exprs_norm_v1, cst_defs_norm_v1, ...)   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                      │                                          │
│                                      ▼                                          │
│  Stage 3: TASK/PLAN CATALOG + SCHEDULING                                        │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Plan Bundle Compilation → Lineage Extraction → Task Graph (rustworkx)  │   │
│  │  → Generation-Based Scheduling → Evidence Catalog Validation             │   │
│  │                                                                          │   │
│  │  Output: TaskSchedule with parallelizable generations                    │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                      │                                          │
│                                      ▼                                          │
│  Stage 4: CPG BUILD                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  build_cpg_nodes_df() → build_cpg_edges_df() → build_cpg_props_df()     │   │
│  │  → cpg_props_map/edges_by_* → Delta Lake writes                          │   │
│  │                                                                          │   │
│  │  Output: cpg_nodes, cpg_edges, cpg_props, accelerators (Delta Lake)     │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Architectural Invariants

CodeAnatomy enforces five architectural invariants across all subsystems:

1. **Byte Spans Are Canonical** - All normalizations anchor to byte offsets (`bstart`, `bend`). Line/column coordinates vary by parser, but byte spans provide a universal coordinate system.

2. **Determinism Contract** - All Acero/DataFusion plans must be reproducible. Plans include `policy_hash` and `ddl_fingerprint`. Plan bundles capture Substrait bytes, environment snapshots, UDF hashes, and Delta input pins.

3. **Inference-Driven** - Dependencies are auto-inferred from DataFusion lineage. Don't specify intermediate schemas—only strict boundaries (relationship outputs, final CPG).

4. **Graceful Degradation** - Missing optional inputs produce correct-schema empty outputs, not exceptions. Views attempt enhanced data (SCIP) but fall back to base data (CST) if unavailable.

5. **No Monkeypatching** - Tests use dependency injection and configuration, never `unittest.mock` or `monkeypatch` for production code paths.

---

## Technology Stack

| Component | Technology | Version | Purpose |
|-----------|------------|---------|---------|
| Query Engine | Apache DataFusion | 51.0.0+ | SQL execution, plan optimization, lineage extraction |
| Graph Engine | rustworkx | 0.17.1+ | Task DAG construction, scheduling |
| Pipeline Orchestration | Hamilton | 1.89.0+ | DAG execution, caching, materialization |
| Data Format | PyArrow | Latest | Columnar data interchange |
| Storage | Delta Lake (deltalake) | 1.3.2+ | Versioned table storage with CDF |
| Serialization | msgspec | 0.20.0+ | Fast binary serialization |
| Python Parsing | LibCST, ast, symtable | Latest | CST/AST/symbol extraction |
| Python Bytecode | dis module | 3.13 | Instruction/CFG extraction |
| Tree Parsing | tree-sitter | Latest | Error-tolerant parsing |
| Cross-Repo Symbols | SCIP | - | Semantic code intelligence |
| Git Integration | pygit2 | 1.19.1+ | Git repository access |
| Observability | OpenTelemetry | 0.60b1+ | Metrics, tracing, logging |
| Runtime | Python | 3.13.11 | Pinned Python version (exact) |
| Package Manager | uv | Latest | Fast dependency resolution |

---

## Module Map

```
src/
├── extract/           # Stage 1: Multi-source extraction
│   ├── extractors/          # Evidence layer implementations (AST, CST, bytecode, SCIP)
│   ├── coordination/        # Execution context and materialization
│   ├── scanning/            # Repository scanning and scope filtering
│   ├── git/                 # Git repository integration
│   └── infrastructure/      # Caching, parallelization, utilities

├── semantics/         # Stage 2: Semantic catalog + normalization (canonical)
│   ├── catalog/            # Dataset rows/specs + view builders
│   ├── incremental/        # CDF cursors, readers, merge strategies
│   ├── compiler.py         # SemanticCompiler (normalize/relate/union)
│   └── spec_registry.py    # Declarative normalization/relationship specs

├── relspec/           # Stage 3: Task catalog + scheduling
│   ├── inferred_deps.py     # Dependency inference from DataFusion lineage
│   ├── rustworkx_graph.py   # Bipartite task graph construction
│   ├── rustworkx_schedule.py # Generation-based scheduling
│   └── graph_edge_validation.py # Column-level validation

├── datafusion_engine/ # DataFusion integration (consolidated query engine)
│   ├── plan/                # Plan bundles, execution, caching
│   ├── session/             # Session management, runtime, streaming
│   ├── schema/              # Schema contracts, inference, alignment
│   ├── lineage/             # Plan lineage extraction
│   ├── views/               # View graph registration
│   ├── udf/                 # UDF catalog and runtime
│   └── delta/               # Delta Lake integration

├── cpg/               # Stage 4: CPG schema + emission
│   ├── kind_catalog.py      # Node/edge kinds
│   ├── prop_catalog.py      # Property specifications
│   └── view_builders_df.py  # CPG emission builders

├── hamilton_pipeline/ # Orchestration
│   ├── driver_factory.py    # Hamilton driver construction
│   ├── task_module_builder.py # Dynamic module generation
│   └── execution.py         # Pipeline execution

├── storage/           # Delta Lake integration
│   └── deltalake/           # Read/write operations, file pruning

├── obs/               # Observability layer
│   ├── diagnostics.py       # DiagnosticsCollector + event recording
│   └── otel/                # OpenTelemetry bootstrap + instrumentation

├── graph/             # Public API
│   └── product_build.py     # Entry point (build_graph_product)

└── utils/             # Cross-cutting utilities
    ├── hashing.py           # Deterministic hashing
    └── registry_protocol.py # Registry abstractions

rust/                  # Rust components
├── datafusion_ext/    # Core DataFusion extensions (UDFs, Delta integration)
├── datafusion_ext_py/ # PyO3 wrapper for datafusion_ext
├── datafusion_python/ # DataFusion Python bindings with CodeAnatomy extensions
└── df_plugin_*/       # ABI-stable plugin system
```

---

## Public API

### Entry Point

```python
from graph.product_build import build_graph_product, GraphProductBuildRequest

result = build_graph_product(GraphProductBuildRequest(repo_root="/path/to/repo"))
```

**Function Signature:**
```python
def build_graph_product(request: GraphProductBuildRequest) -> GraphProductBuildResult:
    """Build the requested graph product and return typed outputs."""
```

**Invocation Lifecycle:**
1. Resolve and validate paths (repo root, output directory)
2. Apply configuration overrides and generate run ID
3. Configure OpenTelemetry with repository context
4. Execute Hamilton pipeline via `execute_pipeline()`
5. Parse pipeline outputs into stable result structure
6. Return `GraphProductBuildResult` with all output paths and metadata

### GraphProductBuildRequest

**Primary Fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `repo_root` | `PathLike` | (required) | Repository root directory to analyze |
| `product` | `GraphProduct` | `"cpg"` | Graph product to build |
| `output_dir` | `PathLike \| None` | `None` | Output directory (defaults to `{repo_root}/build`) |
| `execution_mode` | `ExecutionMode` | `PLAN_PARALLEL` | Hamilton execution strategy |
| `executor_config` | `ExecutorConfig \| None` | `None` | Parallel execution configuration |
| `determinism_override` | `DeterminismTier \| None` | `None` | Override determinism tier |
| `scip_index_config` | `ScipIndexConfig \| None` | `None` | SCIP indexing configuration |
| `incremental_config` | `IncrementalConfig \| None` | `None` | Incremental processing configuration |

**Output Control:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `include_quality` | `bool` | `True` | Include quality validation tables |
| `include_extract_errors` | `bool` | `True` | Include extraction error artifacts |
| `include_manifest` | `bool` | `True` | Include run manifest metadata |
| `include_run_bundle` | `bool` | `True` | Include diagnostics bundle |

### GraphProductBuildResult

**Core Outputs:**

| Field | Type | Description |
|-------|------|-------------|
| `product` | `GraphProduct` | Product type built (`"cpg"`) |
| `product_version` | `str` | Product version string |
| `run_id` | `str \| None` | Unique run identifier (UUID7) |
| `cpg_nodes` | `FinalizeDeltaReport` | CPG nodes table |
| `cpg_edges` | `FinalizeDeltaReport` | CPG edges table |
| `cpg_props` | `FinalizeDeltaReport` | CPG properties table |

**Index Tables:**

| Field | Type | Description |
|-------|------|-------------|
| `cpg_props_map` | `TableDeltaReport` | Property name to ID mapping |
| `cpg_edges_by_src` | `TableDeltaReport` | Edges indexed by source node |
| `cpg_edges_by_dst` | `TableDeltaReport` | Edges indexed by destination node |

### ExecutionMode

| Mode | Description | Use Case |
|------|-------------|----------|
| `DETERMINISTIC_SERIAL` | Single-threaded sequential | Debugging, deterministic output order |
| `PLAN_PARALLEL` | Plan-aware parallel (local) | Default for most workloads |
| `PLAN_PARALLEL_REMOTE` | Plan-aware parallel (remote) | Large-scale distributed execution |

### DeterminismTier

| Tier | Description | Overhead |
|------|-------------|----------|
| `CANONICAL` | Fully deterministic with complete sorting | High |
| `STABLE_SET` | Set-stable with partial sorting | Medium |
| `BEST_EFFORT` | Minimal sorting, fastest execution | Low |

---

## Usage Examples

### Minimal Build

```python
from graph.product_build import build_graph_product, GraphProductBuildRequest

result = build_graph_product(
    GraphProductBuildRequest(repo_root="/path/to/repository")
)

print(f"CPG nodes: {result.cpg_nodes.rows} rows")
print(f"CPG edges: {result.cpg_edges.rows} rows")
print(f"Output: {result.output_dir}")
```

### Production Configuration

```python
from graph.product_build import build_graph_product, GraphProductBuildRequest
from hamilton_pipeline.types import ExecutionMode, ExecutorConfig, ScipIndexConfig
from semantics.incremental import IncrementalConfig
from core_types import DeterminismTier

result = build_graph_product(
    GraphProductBuildRequest(
        repo_root="/home/user/my-project",
        output_dir="/mnt/storage/cpg_outputs",

        execution_mode=ExecutionMode.PLAN_PARALLEL,
        executor_config=ExecutorConfig(kind="multiprocessing", max_tasks=16),
        determinism_override=DeterminismTier.STABLE_SET,

        scip_index_config=ScipIndexConfig(enabled=True, run_scip_test=True),

        incremental_config=IncrementalConfig(
            enabled=True,
            state_dir=Path("/mnt/storage/state"),
            impact_strategy="hybrid",
        ),
    )
)
```

### Incremental Build

```python
from graph.product_build import build_graph_product, GraphProductBuildRequest
from semantics.incremental import IncrementalConfig

result = build_graph_product(
    GraphProductBuildRequest(
        repo_root="/path/to/repo",
        incremental_config=IncrementalConfig(
            enabled=True,
            state_dir=Path("/tmp/state"),
            git_base_ref="main",
            git_head_ref="HEAD",
            impact_strategy="hybrid",
        ),
    )
)
```

---

## Command-Line Interface

```bash
# Basic build
codeanatomy build /path/to/repo

# Custom output directory
codeanatomy build /path/to/repo --output-dir ./cpg_output

# Parallel execution with 16 workers
codeanatomy build /path/to/repo \
  --execution-mode plan_parallel \
  --executor-max-tasks 16

# Incremental build
codeanatomy build /path/to/repo \
  --incremental \
  --git-base-ref main \
  --git-head-ref HEAD

# Debug mode (serial, canonical determinism)
codeanatomy build /path/to/repo \
  --execution-mode deterministic_serial \
  --determinism-tier canonical \
  --log-level DEBUG
```

---

## Error Handling

| Exception | Cause | Mitigation |
|-----------|-------|------------|
| `FileNotFoundError` | `repo_root` does not exist | Verify path before calling |
| `PermissionError` | Cannot write to `output_dir` | Check directory permissions |
| `ValueError` | Invalid configuration parameter | Validate config values |
| `RuntimeError` | Pipeline execution failure | Check logs in `run_bundle_dir` |

```python
try:
    result = build_graph_product(GraphProductBuildRequest(repo_root="/path/to/repo"))
except FileNotFoundError as exc:
    print(f"Repository not found: {exc}")
except RuntimeError as exc:
    print(f"Pipeline failed: {exc}")
else:
    if result.cpg_nodes.error_rows > 0:
        print(f"Warning: {result.cpg_nodes.error_rows} validation errors")
```

---

## Cross-References

- **[02_pipeline_stages.md](02_pipeline_stages.md)** - Detailed pipeline stage documentation
- **[03_semantic_compiler.md](03_semantic_compiler.md)** - Semantic normalization and compilation
- **[04_datafusion_integration.md](04_datafusion_integration.md)** - Query engine integration
- **[09_configuration.md](09_configuration.md)** - Configuration reference
- **[10_debugging_guide.md](10_debugging_guide.md)** - Debugging and troubleshooting

**Source Files:**
- `src/graph/product_build.py` - Public API entry point
- `src/hamilton_pipeline/execution.py` - Pipeline execution
- `src/hamilton_pipeline/types/` - Configuration types
