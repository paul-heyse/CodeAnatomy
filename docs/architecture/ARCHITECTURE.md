# CodeAnatomy Architecture Documentation

## Part I: System Overview

### 1.1 Introduction and Goals

CodeAnatomy is an inference-driven Code Property Graph (CPG) builder for Python. It extracts multiple evidence layers from Python source code—LibCST, AST, symtable, bytecode, SCIP, tree-sitter, and introspection—and compiles them into a rich, queryable graph representation. The system is designed around three core principles:

1. **Multi-Source Evidence Fusion**: Different parsing technologies capture complementary aspects of code structure (syntax, semantics, types, runtime behavior). CodeAnatomy combines them into a unified graph model.

2. **Inference-Driven Dependencies**: Task dependencies are automatically inferred from DataFusion query plans via SQLGlot lineage analysis. No manual `inputs=` declarations required.

3. **Deterministic, Contract-Validated Transformations**: All pipeline stages produce reproducible outputs via plan fingerprinting, schema contracts, and Delta Lake versioning.

### 1.2 Four-Stage Pipeline Overview

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
│  │  → Delta Lake writes                                                     │   │
│  │                                                                          │   │
│  │  Output: cpg_nodes, cpg_edges, cpg_props (Delta Lake tables)            │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 1.3 Key Architectural Invariants

CodeAnatomy enforces five architectural invariants across all subsystems:

1. **Byte Spans Are Canonical**: All normalizations anchor to byte offsets (`bstart`, `bend`). Line/column coordinates vary by parser (AST uses byte columns, LibCST uses UTF-32 columns), but byte spans provide a universal coordinate system.

2. **Determinism Contract**: All Acero/DataFusion plans must be reproducible. Plans include `policy_hash` and `ddl_fingerprint`. Plan bundles capture Substrait bytes, environment snapshots, UDF hashes, and Delta input pins.

3. **Inference-Driven**: Dependencies are auto-inferred from SQLGlot/DataFusion lineage. Don't specify intermediate schemas—only strict boundaries (relationship outputs, final CPG).

4. **Graceful Degradation**: Missing optional inputs produce correct-schema empty outputs, not exceptions. Views attempt enhanced data (SCIP) but fall back to base data (CST) if unavailable.

5. **No Monkeypatching**: Tests use dependency injection and configuration, never `unittest.mock` or `monkeypatch`.

### 1.4 Technology Stack

| Component | Technology | Version | Purpose |
|-----------|------------|---------|---------|
| Query Engine | Apache DataFusion | 50.1+ | SQL execution, plan optimization |
| Graph Engine | rustworkx | 0.17+ | Task DAG construction, scheduling |
| Expression DSL | Ibis | 11.0+ | Relational expression building |
| SQL Analysis | SQLGlot | 28.1+ | Lineage extraction, SQL normalization |
| Pipeline Orchestration | Hamilton | 1.89+ | DAG execution, caching, materialization |
| Data Format | PyArrow | - | Columnar data interchange |
| Storage | Delta Lake (deltalake) | 1.3+ | Versioned table storage with CDF |
| Python Parsing | LibCST, ast, symtable | - | CST/AST/symbol extraction |
| Python Bytecode | dis module | 3.13 | Instruction/CFG extraction |
| Tree Parsing | tree-sitter | - | Error-tolerant parsing |
| Cross-Repo Symbols | SCIP | - | Semantic code intelligence |
| Runtime | Python | 3.13.11 | Pinned Python version |
| Package Manager | uv | - | Fast dependency resolution |

### 1.5 Core Module Map

```
src/
├── extract/           # Stage 1: Multi-source extraction
│   ├── repo_scan.py       # Repository file discovery
│   ├── ast_extract.py     # AST parsing
│   ├── cst_extract.py     # LibCST parsing with metadata
│   ├── symtable_extract.py # Symbol table extraction
│   ├── bytecode_extract.py # Bytecode/CFG/DFG
│   ├── scip_extract.py    # SCIP index processing
│   └── tree_sitter_extract.py # Tree-sitter integration
│
├── normalize/         # Stage 2: Normalization
│   ├── df_view_builders.py  # DataFusion view definitions
│   └── dataset_builders.py  # Schema construction
│
├── relspec/           # Stage 3: Task catalog + scheduling
│   ├── inferred_deps.py     # Dependency inference
│   ├── rustworkx_graph.py   # Task graph construction
│   ├── rustworkx_schedule.py # Generation-based scheduling
│   └── evidence.py          # Evidence catalog
│
├── datafusion_engine/ # DataFusion integration
│   ├── plan_bundle.py       # Plan bundle artifacts
│   ├── execution_facade.py  # Execution API
│   ├── lineage_datafusion.py # Plan lineage extraction
│   ├── schema_contracts.py  # Schema validation
│   ├── scan_planner.py      # Delta scan planning
│   └── udf_*.py             # UDF management
│
├── cpg/               # Stage 4: CPG schema + emission
│   ├── kind_catalog.py      # Node/edge kinds
│   ├── prop_catalog.py      # Property specs
│   └── view_builders_df.py  # CPG emission
│
├── hamilton_pipeline/ # Orchestration
│   ├── execution.py         # Pipeline execution
│   ├── driver_factory.py    # Hamilton driver
│   └── task_module_builder.py # Dynamic module generation
│
├── engine/            # Runtime management
│   ├── session.py           # Engine session
│   └── materialize_pipeline.py # Materialization
│
├── storage/           # Delta Lake integration
│   └── deltalake/           # Read/write operations
│
├── incremental/       # Incremental processing
│   ├── invalidations.py     # Change detection
│   ├── cdf_*.py             # Change Data Feed
│   └── state_store.py       # State management
│
└── graph/             # Public API
    └── product_build.py     # Entry point
```

### 1.6 Entry Point

```python
from graph import GraphProductBuildRequest, build_graph_product

result = build_graph_product(
    GraphProductBuildRequest(repo_root=".")
)

# Access outputs
print(f"Nodes: {result.cpg_nodes.paths.data} ({result.cpg_nodes.rows} rows)")
print(f"Edges: {result.cpg_edges.paths.data} ({result.cpg_edges.rows} rows)")
print(f"Props: {result.cpg_props.paths.data} ({result.cpg_props.rows} rows)")
```

---

## Part II: Extraction Stage

*For the complete extraction stage documentation, see: [docs/architecture/part_ii_extraction.md](part_ii_extraction.md)*

### Overview

The Extraction Stage transforms Python source files into structured evidence tables. It employs multiple parsing strategies—AST, LibCST, symtable, bytecode, SCIP, and tree-sitter—to capture complementary views of code structure.

### Key Concepts

- **FileContext**: Shared state for per-file extraction including path, content, and SHA-256 hash
- **SpanSpec**: Universal coordinate system anchored to byte offsets
- **Parallel Execution**: Fork-based parallelism with ProcessPoolExecutor
- **Evidence Plan Gating**: Skip unnecessary extractors based on downstream requirements

### Critical Files

| File | Lines | Purpose |
|------|-------|---------|
| `src/extract/helpers.py` | ~600 | Core patterns (FileContext, SpanSpec, ExtractExecutionContext) |
| `src/extract/repo_scan.py` | ~620 | Repository file discovery with git integration |
| `src/extract/ast_extract.py` | ~1200 | AST extraction with node/edge/docstring capture |
| `src/extract/cst_extract.py` | ~1700 | LibCST with metadata providers (qualified names, scopes) |
| `src/extract/symtable_extract.py` | ~770 | Symbol table with scope walk |
| `src/extract/bytecode_extract.py` | ~1700 | Bytecode, CFG, DFG extraction |
| `src/extract/parallel.py` | ~100 | Parallel execution utilities |

### Output Evidence Tables

- `repo_files_v1`: File inventory with SHA-256 hashes
- `ast_files_v1`: AST nodes, edges, docstrings, imports, defs, calls
- `libcst_files_v1`: CST with refs, imports, callsites, defs, type expressions
- `symtable_files_v1`: Scopes and symbols with binding flags
- `bytecode_files_v1`: Instructions, CFG blocks, DFG edges
- `scip_files_v1`: Cross-repository symbol information
- `tree_sitter_files_v1`: Error-tolerant parse trees

---

## Part III: DataFusion Engine

*For the complete DataFusion engine documentation, see: [docs/architecture/datafusion_engine_core.md](datafusion_engine_core.md)*

### Overview

The DataFusion Engine provides query planning and execution infrastructure built on Apache Arrow DataFusion (v50.1+). It bridges high-level relational specifications and efficient, deterministic query execution.

### Key Concepts

- **DataFusionPlanBundle**: Central artifact containing DataFrame, logical/optimized plans, Substrait bytes, and fingerprint
- **SessionRuntime**: Lifecycle management for DataFusion SessionContext
- **Substrait-First Execution**: Plans execute from serialized Substrait for determinism
- **Two-Pass Delta Pin Planning**: Extract lineage first, then pin Delta versions

### Critical Data Structures

```python
@dataclass(frozen=True)
class DataFusionPlanBundle:
    df: DataFrame
    logical_plan: object
    optimized_logical_plan: object
    execution_plan: object | None
    substrait_bytes: bytes | None
    plan_fingerprint: str
    artifacts: PlanArtifacts
    delta_inputs: tuple[DeltaInputPin, ...]
    required_udfs: tuple[str, ...]
    required_rewrite_tags: tuple[str, ...]
```

### Critical Files

| File | Lines | Purpose |
|------|-------|---------|
| `src/datafusion_engine/plan_bundle.py` | ~1900 | Plan bundle with fingerprinting |
| `src/datafusion_engine/execution_facade.py` | ~770 | Unified execution API |
| `src/datafusion_engine/planning_pipeline.py` | ~285 | Two-pass Delta planning |
| `src/datafusion_engine/lineage_datafusion.py` | ~300 | Plan tree lineage extraction |
| `src/datafusion_engine/schema_contracts.py` | ~615 | Schema validation |
| `src/datafusion_engine/udf_catalog.py` | ~300 | UDF registration |
| `src/datafusion_engine/scan_planner.py` | ~800 | Delta-aware scan planning |

---

## Part IV: Normalization and Scheduling

*For the complete normalization and scheduling documentation, see: [docs/architecture/part_iv_normalization_and_scheduling.md](part_iv_normalization_and_scheduling.md)*

### Overview

The Normalization and Scheduling subsystem transforms raw extraction outputs into canonical forms and orchestrates execution through inference-driven dependency resolution. Dependencies are automatically inferred by analyzing DataFusion plan artifacts—no manual declarations required.

### Key Concepts

- **InferredDeps**: Dependency record with inputs, required_columns, required_types
- **Bipartite Task Graph**: Evidence nodes (datasets) + Task nodes (computations)
- **Generation-Based Scheduling**: Parallelizable execution waves
- **Column-Level Validation**: Edge requirements checked at schedule time

### Critical Data Structures

```python
@dataclass(frozen=True)
class InferredDeps:
    task_name: str
    output: str
    inputs: tuple[str, ...]
    required_columns: Mapping[str, tuple[str, ...]]
    required_types: Mapping[str, tuple[tuple[str, str], ...]]
    plan_fingerprint: str
    required_udfs: tuple[str, ...]
    scans: tuple[ScanLineage, ...]

@dataclass(frozen=True)
class TaskSchedule:
    ordered_tasks: tuple[str, ...]
    generations: tuple[tuple[str, ...], ...]
    missing_tasks: tuple[str, ...] = ()
```

### Critical Files

| File | Lines | Purpose |
|------|-------|---------|
| `src/normalize/df_view_builders.py` | ~650 | DataFusion view definitions |
| `src/relspec/inferred_deps.py` | ~250 | Dependency inference from plans |
| `src/relspec/rustworkx_graph.py` | ~1200 | Bipartite task graph |
| `src/relspec/rustworkx_schedule.py` | ~320 | Generation scheduling |
| `src/relspec/evidence.py` | ~160 | Evidence catalog |
| `src/relspec/graph_edge_validation.py` | ~350 | Column-level validation |

---

## Part V: Storage and Incremental Processing

*For the complete storage and incremental documentation, see: [docs/architecture/part_v_storage_and_incremental.md](part_v_storage_and_incremental.md)*

### Overview

Delta Lake provides versioned storage with ACID guarantees and Change Data Feed (CDF) for incremental processing. The system tracks plan fingerprints, runtime hashes, and dataset versions to detect when cached artifacts are stale.

### Key Concepts

- **ScanUnit**: Deterministic, pruned scan description with version pins
- **File Pruning**: Partition and statistics-based file elimination
- **CDF Cursors**: Track last processed Delta version per dataset
- **Invalidation Snapshots**: Plan fingerprints + metadata hashes for change detection

### Critical Data Structures

```python
@dataclass(frozen=True)
class ScanUnit:
    key: str
    dataset_name: str
    delta_version: int | None
    delta_timestamp: str | None
    candidate_file_count: int
    pruned_file_count: int
    pushed_filters: tuple[str, ...]
    projected_columns: tuple[str, ...]

@dataclass(frozen=True)
class InvalidationSnapshot:
    version: int
    plan_fingerprints: tuple[PlanFingerprint, ...]
    metadata_hash: str | None
    runtime_profile_hash: str | None
```

### Critical Files

| File | Lines | Purpose |
|------|-------|---------|
| `src/datafusion_engine/scan_planner.py` | ~810 | Scan planning with pruning |
| `src/datafusion_engine/delta_protocol.py` | ~150 | Protocol compatibility |
| `src/incremental/invalidations.py` | ~400 | Change detection |
| `src/incremental/cdf_cursors.py` | ~150 | CDF version tracking |
| `src/incremental/state_store.py` | ~180 | State layout |
| `src/storage/deltalake/file_pruning.py` | ~450 | File-level pruning |

---

## Part VI: CPG Build and Orchestration

*For the complete CPG documentation, see: [docs/architecture/part_vi_cpg_build_and_orchestration.md](part_vi_cpg_build_and_orchestration.md)*

### Overview

The CPG Build subsystem transforms normalized evidence into a queryable Code Property Graph with nodes, edges, and properties materialized as Delta Lake tables. Hamilton orchestrates the full DAG with caching, incremental execution, and diagnostics.

### Key Concepts

- **Node/Edge Kind Catalogs**: Type-safe identifiers for CPG entities
- **Property Catalog**: 200+ property specs with types and enum constraints
- **Entity Family Specs**: Declarative node/property emission rules
- **Hamilton DAG**: Dynamic module generation for task execution

### CPG Output Schema

**Nodes**: `node_id, node_kind, path, bstart, bend, file_id`

**Edges**: `edge_id, edge_kind, src_node_id, dst_node_id, path, bstart, bend, origin, resolution_method, confidence, score`

**Properties**: `entity_kind, entity_id, node_kind, prop_key, value_type, value_string, value_int, value_float, value_bool, value_json`

### Critical Files

| File | Lines | Purpose |
|------|-------|---------|
| `src/graph/product_build.py` | ~330 | Public API entry point |
| `src/cpg/kind_catalog.py` | ~280 | Node/edge kind registry |
| `src/cpg/prop_catalog.py` | ~430 | Property specifications |
| `src/cpg/view_builders_df.py` | ~500 | CPG emission builders |
| `src/hamilton_pipeline/driver_factory.py` | ~830 | Hamilton driver |
| `src/hamilton_pipeline/task_module_builder.py` | ~200 | Dynamic modules |
| `src/engine/materialize_pipeline.py` | ~510 | View materialization |

---

## Appendices

### A. Glossary

| Term | Definition |
|------|------------|
| **CPG** | Code Property Graph - unified graph representation of code structure |
| **Evidence** | Structured data extracted from source code (e.g., AST nodes, CST refs) |
| **Plan Bundle** | DataFusion plan artifact with fingerprint for caching |
| **Substrait** | Cross-platform IR for query plan serialization |
| **CDF** | Change Data Feed - Delta Lake feature for tracking row-level changes |
| **Generation** | Group of tasks with no inter-dependencies, executable in parallel |
| **Bipartite Graph** | Graph with two node types (Evidence, Task) and edges only between types |
| **Scan Unit** | Deterministic scan description with Delta version pin |

### B. Data Flow Diagram

```
Repository Files
       │
       ▼
┌──────────────────────────────────────────────────────────────┐
│                     EXTRACTION LAYER                          │
│  ┌─────────┬─────────┬──────────┬──────────┬────────────────┐ │
│  │   AST   │  LibCST │ Symtable │ Bytecode │ SCIP/TreeSitter│ │
│  └────┬────┴────┬────┴────┬─────┴────┬─────┴───────┬────────┘ │
└───────│─────────│─────────│──────────│─────────────│──────────┘
        │         │         │          │             │
        ▼         ▼         ▼          ▼             ▼
    ┌─────────────────────────────────────────────────────────┐
    │                  Evidence Tables (Arrow)                 │
    │  ast_files_v1, libcst_files_v1, symtable_files_v1, ...  │
    └────────────────────────┬────────────────────────────────┘
                             │
                             ▼
┌──────────────────────────────────────────────────────────────┐
│                    NORMALIZATION LAYER                        │
│              df_view_builders.py (DataFusion)                │
│    - Stable ID generation (SHA-256 hashes)                   │
│    - Byte span canonicalization                              │
│    - Schema alignment                                        │
└────────────────────────┬────────────────────────────────────-┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                    SCHEDULING LAYER                           │
│  Plan Bundle Compilation → Lineage Extraction → Task Graph   │
│  rustworkx bipartite graph → generation-based scheduling     │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                      CPG EMISSION                             │
│  build_cpg_nodes_df() → build_cpg_edges_df() → props         │
│  Entity family specs → Delta Lake writes                     │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────┐
│                     OUTPUT (Delta Lake)                       │
│    cpg_nodes    │    cpg_edges    │    cpg_props             │
└──────────────────────────────────────────────────────────────┘
```

### C. Configuration Reference

**Environment Variables:**
- None required; configuration via Python objects

**Runtime Profile Settings:**
- `target_partitions`: Parallelism for DataFusion execution
- `batch_size`: Arrow record batch size
- `determinism_tier`: CANONICAL (reproducible) / STABLE_SET / BEST_EFFORT
- `delta_protocol_mode`: Delta protocol compatibility level

**GraphProductBuildRequest Options:**
- `repo_root`: Repository path (required)
- `output_dir`: Delta Lake output directory
- `runtime_profile_name`: Engine configuration profile
- `determinism_override`: Override determinism tier
- `incremental_config`: Incremental execution settings
- `include_quality`: Emit quality metrics tables
- `include_manifest`: Emit run manifest
- `include_run_bundle`: Emit diagnostics bundle

---

*This documentation was generated by analyzing the CodeAnatomy codebase. For implementation details, refer to the source files referenced throughout.*
