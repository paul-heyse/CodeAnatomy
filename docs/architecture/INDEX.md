# Architecture Documentation Index

This index provides an overview of CodeAnatomy's architecture documentation, including document summaries, reading order, and quick reference links.

---

## Document Overview

| Document | Purpose | Lines |
|----------|---------|-------|
| [01_overview.md](01_overview.md) | System introduction, four-stage pipeline, invariants, public API | ~600 |
| [02_pipeline_stages.md](02_pipeline_stages.md) | Detailed extraction → normalization → scheduling → CPG build | ~650 |
| [03_semantic_compiler.md](03_semantic_compiler.md) | SemanticCompiler, 10 composable rules, view catalog | ~500 |
| [04_datafusion_integration.md](04_datafusion_integration.md) | Plan bundles, sessions, lineage extraction, schema contracts | ~550 |
| [05_rust_and_udfs.md](05_rust_and_udfs.md) | Crate structure, UDF platform, quality gates | ~450 |
| [06_hamilton_orchestration.md](06_hamilton_orchestration.md) | Hamilton DAG, driver factory, execution modes | ~450 |
| [07_storage_and_incremental.md](07_storage_and_incremental.md) | Delta Lake integration, CDF, incremental processing | ~550 |
| [08_observability.md](08_observability.md) | OpenTelemetry, diagnostics, metrics, quality tables | ~600 |
| [09_configuration.md](09_configuration.md) | Runtime profiles, policies, utilities, fingerprinting | ~500 |
| [10_debugging_guide.md](10_debugging_guide.md) | Plan inspection, data flow tracing, common issues | ~650 |

**Total:** ~5,500 lines (consolidated from ~24,000 lines)

---

## Reading Order

### Recommended Path

```
01_overview.md                     ← Start here (system introduction)
    ↓
02_pipeline_stages.md              ← Understand the four-stage flow
    ↓
┌───────────────────────────────────────────────────┐
│  Parallel reading (no dependencies between these) │
├───────────────────────────────────────────────────┤
│  03_semantic_compiler.md    (normalization)       │
│  04_datafusion_integration.md (query engine)      │
│  05_rust_and_udfs.md        (native extensions)   │
│  06_hamilton_orchestration.md (DAG execution)     │
│  07_storage_and_incremental.md (persistence)      │
└───────────────────────────────────────────────────┘
    ↓
08_observability.md                ← Cross-cutting concern
09_configuration.md                ← Cross-cutting concern
    ↓
10_debugging_guide.md              ← Practical reference (read as needed)
```

### By Topic

| Topic | Primary Document | Related Documents |
|-------|------------------|-------------------|
| Getting started | 01_overview.md | 02_pipeline_stages.md |
| Extraction | 02_pipeline_stages.md | - |
| Normalization | 02_pipeline_stages.md, 03_semantic_compiler.md | - |
| Query engine | 04_datafusion_integration.md | 05_rust_and_udfs.md |
| UDF development | 05_rust_and_udfs.md | 04_datafusion_integration.md |
| Pipeline execution | 06_hamilton_orchestration.md | 04_datafusion_integration.md |
| Storage | 07_storage_and_incremental.md | - |
| Incremental processing | 07_storage_and_incremental.md | 03_semantic_compiler.md |
| Telemetry | 08_observability.md | 09_configuration.md |
| Configuration | 09_configuration.md | - |
| Troubleshooting | 10_debugging_guide.md | All |

---

## Document Summaries

### 01_overview.md - System Overview & Public API

Introduction to CodeAnatomy's inference-driven CPG builder. Covers:
- Four-stage pipeline architecture
- Technology stack (DataFusion, Hamilton, PyArrow, Delta Lake)
- Module map and entry points
- Key architectural invariants
- Public API (`GraphProductBuildRequest`, `build_graph_product`)

**Key Concepts:** Byte span canonicalization, determinism contract, inference-driven scheduling

### 02_pipeline_stages.md - The Four-Stage Pipeline

Detailed coverage of each pipeline stage:
- **Stage 1: Extraction** - LibCST, AST, symtable, bytecode, SCIP, tree-sitter evidence
- **Stage 2: Normalization** - Byte span alignment, ID generation, schema enforcement
- **Stage 3: Scheduling** - Task graph inference, rustworkx scheduling, plan compilation
- **Stage 4: CPG Build** - Node/edge/property emission, Delta Lake output

**Key Concepts:** Evidence layers, semantic compilation, task graph inference

### 03_semantic_compiler.md - Semantic Pipeline

The SemanticCompiler and its 10 composable rules:
- Input/spec registries
- Normalize/relate/union operations
- View catalog with fingerprints
- Schema-driven join inference

**Key Concepts:** Rule composition, view registration authority, graceful degradation

### 04_datafusion_integration.md - DataFusion Engine

DataFusion query engine integration:
- Plan bundle compilation and fingerprinting
- Session management and configuration
- Lineage extraction from logical plans
- Schema contracts and validation
- Two-pass Delta pin planning

**Key Concepts:** Plan bundles, Substrait serialization, lineage inference

### 05_rust_and_udfs.md - Rust Architecture & UDFs

Rust extensions and UDF platform:
- Crate workspace structure
- 28+ scalar UDFs, 11+ aggregate UDFs
- Plugin architecture with ABI stability
- PyO3 bindings and Arrow IPC bridge
- UDF quality gate requirements

**Key Concepts:** UDF implementation patterns, plugin capabilities, API compliance

### 06_hamilton_orchestration.md - Pipeline Orchestration

Hamilton DAG orchestration:
- Driver factory and caching
- Dynamic task module generation
- Execution modes (serial, parallel, remote)
- Lifecycle hooks for diagnostics
- Cache lineage tracking

**Key Concepts:** Task routing, configuration fingerprinting, execution manager

### 07_storage_and_incremental.md - Storage Layer

Delta Lake integration and incremental processing:
- Read/write operations with version pinning
- Two-pass scan planning
- File pruning (partition, stats, row-group)
- CDF-based incrementality
- Cursor tracking and merge strategies

**Key Concepts:** Version pinning, CDF cursors, merge strategies (UPSERT, REPLACE, APPEND)

### 08_observability.md - Observability

Diagnostics and telemetry:
- DiagnosticsCollector for event recording
- Dataset and column statistics
- Quality tables for invalid entities
- OpenTelemetry bootstrap and processors
- Metrics catalog (histograms, counters, gauges)

**Key Concepts:** Two-layer architecture (domain + transport), run correlation

### 09_configuration.md - Configuration & Utilities

Configuration system and utilities:
- Runtime profiles (dev, prod, CST autoload)
- Determinism tiers (CANONICAL, STABLE_SET, BEST_EFFORT)
- Environment variables reference
- Hashing utilities (BLAKE2b, SHA-256)
- Registry protocol and implementations

**Key Concepts:** Configuration hierarchy (Policy/Settings/Config/Spec/Options)

### 10_debugging_guide.md - Debugging & Troubleshooting

Practical debugging workflows:
- Plan bundle inspection
- Fingerprint debugging
- Data flow tracing with examples
- Hamilton execution debugging
- Delta Lake and UDF troubleshooting
- Common issues and solutions

**Key Concepts:** Diagnostic checklist, data flow walkthrough, debugging workflows

---

## Quick Reference

### Entry Points

```python
# Primary API
from graph import GraphProductBuildRequest, build_graph_product
result = build_graph_product(GraphProductBuildRequest(repo_root="."))

# Direct pipeline access
from hamilton_pipeline import execute_pipeline
result = execute_pipeline(repo_root=".", options=options)
```

### Key Environment Variables

| Variable | Purpose |
|----------|---------|
| `CODEANATOMY_RUNTIME_PROFILE` | Runtime profile name |
| `CODEANATOMY_DETERMINISM_TIER` | Determinism level |
| `CODEANATOMY_DISKCACHE_DIR` | DiskCache root |
| `OTEL_SERVICE_NAME` | Service name for telemetry |

### Source File Locations

| Concern | Location |
|---------|----------|
| Extraction | `src/extract/` |
| Semantic pipeline | `src/semantics/` |
| DataFusion engine | `src/datafusion_engine/` |
| Hamilton pipeline | `src/hamilton_pipeline/` |
| Rust extensions | `rust/` |
| Storage | `src/storage/` |
| Observability | `src/obs/` |

---

## Related Files

- **CLAUDE.md** - Agent instructions and project overview
- **AGENTS.md** - Agent operating protocols
- **pyproject.toml** - Project configuration and dependencies
