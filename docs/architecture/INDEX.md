# Architecture Documentation Index

This index provides an overview of CodeAnatomy's architecture documentation, including document summaries, reading order, and quick reference links.

CodeAnatomy is an inference-driven Code Property Graph (CPG) builder that uses a three-phase pipeline: Python extraction, Python semantic IR compilation, and Rust engine execution. The architecture documentation reflects the current Rust-first execution model where the `codeanatomy_engine` Rust crate owns compilation, scheduling, rule application, and materialization.

---

## Document Overview

| Document | Purpose | Lines |
|----------|---------|-------|
| [01_overview.md](01_overview.md) | System introduction, three-phase pipeline, invariants, public API | ~650 |
| [02_extraction.md](02_extraction.md) | Multi-source evidence extraction (Python) | ~780 |
| [03_semantic_compiler.md](03_semantic_compiler.md) | Semantic IR compilation, view kinds, entity model (Python) | ~980 |
| [04_boundary_contract.md](04_boundary_contract.md) | `SemanticExecutionSpec` contract between Python and Rust | ~1,090 |
| [05_rust_engine.md](05_rust_engine.md) | Rust compiler, rules system, session management, scheduling | ~550 |
| [06_execution.md](06_execution.md) | Rust execution pipeline, Delta writer, metrics, tracing | ~1,010 |
| [07_datafusion_and_udfs.md](07_datafusion_and_udfs.md) | DataFusion integration, UDF platform, plugin system | ~700 |
| [08_storage.md](08_storage.md) | Delta Lake storage layer (Python + Rust), incremental processing | ~680 |
| [09_observability_and_config.md](09_observability_and_config.md) | Observability, configuration, debugging workflows | ~730 |

**Total:** ~7,170 lines across 9 documents

---

## Reading Order

### Recommended Path

```
01_overview.md                     <- Start here (three-phase pipeline introduction)
    |
02_extraction.md                   <- Phase 1: how evidence is extracted
    |
04_boundary_contract.md            <- The Python-Rust boundary (read before 03)
    |
03_semantic_compiler.md            <- Phase 2: what Python produces for Rust
    |
+---------------------------------------------------+
|  Parallel reading (no strict dependencies)         |
+---------------------------------------------------+
|  05_rust_engine.md     (compiler, rules, session)  |
|  06_execution.md       (execution, materialization)|
|  07_datafusion_and_udfs.md (query engine, UDFs)    |
|  08_storage.md         (Delta Lake, incremental)   |
+---------------------------------------------------+
    |
09_observability_and_config.md     <- Cross-cutting concerns + debugging
```

### Why 04 Before 03

The boundary contract (`SemanticExecutionSpec`) is the architectural keystone.
Reading it before the semantic compiler clarifies *what* Python must produce and
*why* the semantic IR is shaped the way it is.

### By Topic

| Topic | Primary Document | Related Documents |
|-------|------------------|-------------------|
| Getting started | 01_overview.md | 02_extraction.md, 04_boundary_contract.md |
| Evidence extraction | 02_extraction.md | 03_semantic_compiler.md |
| Semantic IR | 03_semantic_compiler.md | 04_boundary_contract.md |
| Python-Rust boundary | 04_boundary_contract.md | 03_semantic_compiler.md, 05_rust_engine.md |
| Rust compilation | 05_rust_engine.md | 04_boundary_contract.md, 06_execution.md |
| Execution + materialization | 06_execution.md | 05_rust_engine.md, 08_storage.md |
| Query engine + UDFs | 07_datafusion_and_udfs.md | 05_rust_engine.md, 08_storage.md |
| Storage | 08_storage.md | 06_execution.md, 07_datafusion_and_udfs.md |
| Incremental processing | 08_storage.md | 04_boundary_contract.md |
| Observability | 09_observability_and_config.md | 06_execution.md |
| Configuration | 09_observability_and_config.md | 01_overview.md |
| Debugging | 09_observability_and_config.md | All |

---

## Document Summaries

### 01_overview.md - System Overview & Pipeline Architecture

Introduction to CodeAnatomy's three-phase CPG pipeline. Covers:
- Three-phase architecture: Extraction -> Semantic IR -> Rust Engine
- Technology stack (DataFusion 51.0, PyArrow, Delta Lake, msgspec)
- Module map and entry points
- Five architectural invariants (byte-span canonicalization, determinism, inference-driven scheduling, graceful degradation, spec-driven boundary)
- Public API (`GraphProductBuildRequest`, `build_graph_product`)

**Key Concepts:** Three-phase pipeline, `SemanticExecutionSpec` boundary, Rust-first execution

### 02_extraction.md - Extraction Pipeline (Python)

Multi-source evidence extraction from Python source code:
- Six extractors: LibCST, AST, symtable, bytecode, SCIP, tree-sitter
- Four-stage extraction flow: discovery -> per-file -> cross-file -> output mapping
- Extraction orchestration via `run_extraction()`
- Output contracts and semantic input mapping

**Key Concepts:** Evidence layers, byte-span anchoring, extraction options, semantic input registry

### 03_semantic_compiler.md - Semantic IR Compilation (Python)

The Python-owned phase that compiles extraction evidence into semantic IR:
- Four-stage IR pipeline: build -> compile -> validate -> package
- SemanticIR data model (views, dataset_rows, join_groups)
- 14 view kinds across 4 categories
- 10 composable semantic rules
- Entity model (nodes, edges, properties)

**Key Concepts:** View kinds, rule composition, entity model, `build_semantic_ir()` pipeline

### 04_boundary_contract.md - The Python-Rust Boundary Contract

The architectural keystone defining the immutable contract between Python and Rust:
- `SemanticExecutionSpec` structure (version 4, msgspec Struct)
- Seven spec components: InputRelation, ViewDefinition, JoinGraph, OutputTarget, RuleIntent, RuntimeConfig, TracingConfig
- Python-side building via `SemanticPlanCompiler.build_spec_json()`
- Rust-side consumption via `run_build()` (PyO3 entry point)
- Output contracts and naming conventions
- JSON wire format and serialization
- Error boundary (Python errors vs Rust errors)
- Two-layer determinism (spec_hash + envelope_hash via BLAKE3)

**Key Concepts:** Spec immutability, JSON wire format, determinism contract, error boundary

### 05_rust_engine.md - Rust Execution Engine Architecture

The Rust engine (`codeanatomy_engine`, ~21K LOC) that owns post-boundary execution:
- Compiler subsystem: `SemanticPlanCompiler`, graph validation, view compilation
- Rules system: 4 rule classes, 4 rulepack profiles, intent compiler
- Session management: `SessionFactory`, environment profiles, session envelope
- Scheduling and cost model
- Optimizer pipeline with pass tracing

**Key Concepts:** Plan compilation flow, rulepack profiles, session envelope, cost model

### 06_execution.md - Execution and Materialization Layer

The Rust execution pipeline that materializes CPG outputs:
- Six-stage pipeline: preflight -> compilation -> plan bundle -> compliance -> materialization -> maintenance
- Delta writer with schema mapping and output table lifecycle
- `RunResult` contract returned to Python
- Metrics collection (`CollectedMetrics`, scan selectivity)
- Warning system with `WarningCode` taxonomy
- OTel tracing integration
- Adaptive tuner with bounded adjustment
- Delta maintenance schedule (compact -> checkpoint -> vacuum -> cleanup -> constraints)

**Key Concepts:** Execution pipeline stages, Delta writer, RunResult, adaptive tuning

### 07_datafusion_and_udfs.md - DataFusion Integration and UDF Platform

DataFusion's dual role across both Python and Rust:
- 8-crate Rust workspace structure
- UDF platform: 29+ scalar, 11+ aggregate, 3+ window, 5+ table functions
- Plugin system: ABI-stable `df_plugin_api`, plugin host, CodeAnatomy plugin
- Python-side DataFusion: session management, pooling, schema contracts, lineage extraction
- Delta control plane with feature gating

**Key Concepts:** Dual-role DataFusion, UDF registration flow, plugin ABI stability

### 08_storage.md - Storage Layer Architecture

Delta Lake storage for both Python extraction and Rust materialization:
- Python storage layer: core operations, snapshot identity, mutation safety, file pruning
- Rust Delta operations: Delta writer, table provider registration, scan configuration
- Incremental processing: CDF cursors, merge strategies (UPSERT, APPEND, REPLACE, DELETE_INSERT)
- State directory layout and cursor tracking
- Storage options for local and cloud (S3, Azure, GCS)

**Key Concepts:** Dual write paths, CDF cursors, merge strategies, scan configuration

### 09_observability_and_config.md - Observability, Configuration, and Debugging

Cross-cutting concerns spanning the full pipeline:
- DiagnosticsCollector and event recording
- Engine metrics bridge (Rust -> Python)
- OpenTelemetry integration (bootstrap, processors, instrumentation scopes)
- Rust tracing integration with `TracingConfig`
- Configuration philosophy: Policy/Settings/Config/Spec/Options hierarchy
- Runtime profiles and determinism tiers
- Environment variables reference
- Debugging workflows: plan inspection, fingerprint debugging, data flow tracing

**Key Concepts:** Two-layer observability (domain + transport), engine metrics bridge, configuration hierarchy

---

## Quick Reference

### Entry Points

```python
# Primary API
from graph import GraphProductBuildRequest, build_graph_product
result = build_graph_product(GraphProductBuildRequest(repo_root="."))

# Three-phase orchestration (internal)
from graph.build_pipeline import orchestrate_build
# Phase 1: _run_extraction_phase()
# Phase 2: _compile_semantic_phase()  -> SemanticExecutionSpec
# Phase 3: _execute_engine_phase()    -> RunResult (via Rust run_build())
```

### Key Environment Variables

| Variable | Purpose | Document |
|----------|---------|----------|
| `CODEANATOMY_RUNTIME_PROFILE` | Runtime profile name | 09 |
| `CODEANATOMY_DETERMINISM_TIER` | Determinism level (STRICT, BEST_EFFORT) | 09 |
| `CODEANATOMY_ENGINE_PROFILE` | Rust engine profile (Small, Medium, Large) | 05, 06 |
| `OTEL_SERVICE_NAME` | Service name for telemetry | 09 |
| `OTEL_SDK_DISABLED` | Disable OpenTelemetry | 09 |

### Source File Locations

| Concern | Location |
|---------|----------|
| Extraction (Python) | `src/extract/`, `src/extraction/` |
| Semantic pipeline (Python) | `src/semantics/` |
| Boundary contract (Python) | `src/planning_engine/spec_contracts.py` |
| Rust engine | `rust/codeanatomy_engine/` |
| Rust PyO3 bindings | `rust/codeanatomy_engine_py/` |
| DataFusion engine (Python) | `src/datafusion_engine/` |
| DataFusion extensions (Rust) | `rust/datafusion_ext/` |
| UDF plugins (Rust) | `rust/df_plugin_api/`, `rust/df_plugin_host/`, `rust/df_plugin_codeanatomy/` |
| Storage (Python) | `src/storage/` |
| Observability | `src/obs/` |
| Pipeline orchestration | `src/graph/build_pipeline.py` |

### Technology Stack

| Technology | Version | Role |
|------------|---------|------|
| DataFusion | 51.0 | Query engine (Python + Rust) |
| PyArrow | - | In-memory columnar data |
| Delta Lake | - | Versioned table storage |
| msgspec | - | Serialization (spec contract) |
| PyO3 | - | Python-Rust bindings |
| OpenTelemetry | - | Observability transport |

---

## Related Files

- **CLAUDE.md** - Agent instructions and project overview
- **AGENTS.md** - Agent operating protocols
- **pyproject.toml** - Project configuration and dependencies
- **rust/Cargo.toml** - Rust workspace configuration
