# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CodeAnatomy is an inference-driven Code Property Graph (CPG) builder for Python. It extracts multiple evidence layers from Python source code (LibCST, AST, symtable, bytecode, SCIP, tree-sitter, introspection) and compiles them into a rich, queryable graph representation using a Hamilton-driven DAG pipeline with PyArrow Acero for deterministic, contract-validated transformations.

## Build & Development Commands

```bash
# One-time setup (REQUIRED)
scripts/bootstrap_codex.sh && uv sync

# Quality gates (run in order)
uv run ruff check --fix          # Lint + autofix
uv run pyrefly check             # Type/contract validation
uv run pyright --warnings --pythonversion=3.13  # Strict type checking

# Testing
uv run pytest tests/unit/                    # Unit tests only
uv run pytest tests/ -m "not e2e"           # Exclude slow E2E tests
uv run pytest tests/unit/test_foo.py -v     # Single test file
uv run pytest --cov=src --cov-report=html   # With coverage

# Format code
uv run ruff format
```

**Critical:** Always use `uv run` for Python commands. Direct `python` calls will fail due to missing module imports.

## Architecture

**Four-Stage Pipeline:**
1. **Extraction** → Evidence tables from multiple sources (LibCST, AST, symtable, bytecode, SCIP, tree-sitter)
2. **Normalization** → Canonical byte spans, stable IDs, join-ready shape
3. **Task/Plan Catalog + Scheduling** → TaskSpec builders → PlanCatalog → inferred TaskGraph → schedule
4. **CPG Build** → Node/edge/property emission → final outputs

**Core Modules (`src/`):**
- `extract/` - Multi-source extractors (repo scan, AST, CST, symtable, bytecode, SCIP)
- `normalize/` - Byte-span canonicalization, stable ID generation
- `relspec/` - Task/plan catalog, inferred deps, rustworkx graph inference + scheduling
- `cpg/` - CPG schema definitions, node/edge/property contracts
- `engine/` - Execution runtime: sessions, plans, materialization
- `hamilton_pipeline/` - Hamilton DAG orchestration
- `datafusion_engine/` - DataFusion integration (query planning, UDFs, lineage, schema contracts)
- `obs/` - Observability layer (diagnostics collection, metrics, scan telemetry)
- `storage/` - Delta Lake integration with file pruning
- `incremental/` - Incremental processing, CDF cursors, invalidation detection

**Primary Entry Point:**
```python
from graph import GraphProductBuildRequest, build_graph_product

result = build_graph_product(
    GraphProductBuildRequest(repo_root=".")
)
```

## Calculation-Driven Scheduling

Dependencies are automatically inferred from DataFusion plan lineage. No manual `inputs=` declarations required. The system:

1. Compiles view builders to DataFusion DataFrames
2. Extracts lineage from optimized logical plans via `lineage_datafusion.py`
3. Extracts column-level requirements via plan tree walking
4. Builds rustworkx graph with inferred edges via `build_task_graph_from_inferred_deps()`
5. Generates Hamilton DAGs for orchestration

**Key modules:**
- `src/relspec/task_catalog.py` - TaskSpec definitions + TaskCatalog
- `src/relspec/plan_catalog.py` - PlanArtifact compilation + PlanCatalog
- `src/relspec/inferred_deps.py` - Dependency inference from DataFusion lineage
- `src/relspec/rustworkx_graph.py` - TaskGraph construction from inferred deps
- `src/relspec/graph_edge_validation.py` - Column-level edge validation
- `src/relspec/rustworkx_schedule.py` - Inference-driven scheduling

**Feature flags** (in `src/relspec/config.py`):
- `USE_INFERRED_DEPS` - Use inferred dependencies for scheduling (default: True)
- `COMPARE_DECLARED_INFERRED` - Legacy comparison hook if declared inputs exist
- `HAMILTON_DAG_OUTPUT` - Generate Hamilton DAG modules (default: True)

**Deprecated patterns:**
- Rule/spec registry based scheduling has been removed
- Do not add manual `inputs=` declarations or rule definitions

## Semantic Pipeline Architecture

The semantic module (`src/semantics/`) provides centralized, rule-based CPG relationship building.

**Data Flow:**
```
Extraction → Input Registry → SemanticCompiler → View Graph → Hamilton DAG → CPG Outputs
```

**Key Modules:**
- `semantics/compiler.py` - Core `SemanticCompiler` class for normalize/relate/union operations
- `semantics/pipeline.py` - `build_cpg()` entry point that orchestrates the full pipeline
- `semantics/input_registry.py` - Maps extraction outputs to semantic inputs with validation
- `semantics/spec_registry.py` - Declarative normalization + relationship specs and spec index
- `semantics/naming.py` - Canonical output naming policy (all `_v1` suffixes)
- `semantics/catalog/` - View catalog with metadata and fingerprints
- `semantics/joins/` - Schema-driven join strategy inference
- `semantics/types/` - Semantic type system and annotated schemas
- `semantics/adapters.py` - Legacy schema compatibility adapters

**Configuration:**
- `SemanticConfig` in `semantics/config.py` controls pipeline behavior
- `CpgBuildOptions` in `semantics/pipeline.py` for build-time options

**Observability:**
- All operations instrumented with OpenTelemetry spans (`SCOPE_SEMANTICS`)
- Metrics collection via `semantics/metrics.py`
- Plan fingerprints via `semantics/plans/`

**Execution Authority:**
- View graph (`datafusion_engine/views/registry_specs.py`) is the sole registration authority
- Hamilton DAG only consumes pre-registered semantic views
- No duplicate registration allowed

**Generated Docs:**
- `docs/architecture/semantic_pipeline_graph.md` (auto-generated Mermaid graph)

## Key Architectural Invariants

- **Byte Spans Are Canonical**: All normalizations anchor to byte offsets (`bstart`, `bend`)
- **Determinism Contract**: All Acero plans must be reproducible; include `policy_hash` and `ddl_fingerprint`
- **Inference-Driven**: Don't specify intermediate schemas—only strict boundaries (relationship outputs, final CPG)
- **Graceful Degradation**: Missing optional inputs produce correct-schema empty outputs, not exceptions
- **No Monkeypatching**: Tests use dependency injection and configuration, never `unittest.mock` or `monkeypatch`

## Code Style Requirements

- **Imports**: Absolute imports only; type-only imports in `if TYPE_CHECKING:` blocks
- **Formatting**: 100-char lines, 4-space indent, double quotes, trailing commas
- **Typing**: All functions fully typed; no bare `Any`; strict Optional handling
- **Docstrings**: NumPy convention; imperative mood summary; Parameters/Returns/Raises sections
- **Complexity**: Cyclomatic ≤ 10, max 12 branches, max 6 return statements

**Every module must have:**
```python
from __future__ import annotations
```

## Consolidated Utilities

Use the shared utilities before introducing new helpers:

- `src/utils/hashing.py` - Explicit, semantics-preserving hash helpers (msgpack, JSON, storage options).
- `src/utils/env_utils.py` - Canonical environment parsing helpers for bool/int/float/string.
- `src/utils/storage_options.py` - Normalization + merge helpers for storage/log storage options.
- `src/utils/registry_protocol.py` - Registry protocol + `MutableRegistry`/`ImmutableRegistry`.
- `src/datafusion_engine/hash_utils.py` - Thin re-export wrapper for DataFusion callers.

## Configuration + Registry Conventions

**Config naming**
- `Policy`: runtime behavior control (e.g., `DeltaWritePolicy`)
- `Settings`: initialization parameters (e.g., `DiskCacheSettings`)
- `Config`: request/command parameters (e.g., `OtelConfig`)
- `Spec`: declarative schema definitions (e.g., `TableSpec`)
- `Options`: optional parameter bundles (e.g., `CompileOptions`)

**Registry usage**
- Prefer `Registry` protocol types for simple key/value registries.
- Use `ImmutableRegistry` for static module-level registries.
- Use `MutableRegistry` only when registry behavior is pure key/value.
- Avoid forcing inheritance for rich registries (`ProviderRegistry`, `ParamTableRegistry`, etc.).

## Test Markers

- `@pytest.mark.smoke` - Fast sanity checks
- `@pytest.mark.e2e` - End-to-end pipeline tests
- `@pytest.mark.integration` - Multi-subsystem tests
- `@pytest.mark.benchmark` - Performance tests (non-gating)
- `@pytest.mark.serial` - Must run single-threaded

## Git Protocol

- Do not use `git checkout` or destructive git operations
- Do not worry about git cleanliness—`.gitignore` is properly configured
- The user handles uncommitted file changes independently

## Environment

- **Python**: 3.13.11 (pinned)
- **Package Manager**: uv
- **Key Dependencies**: DataFusion 50.1+, Rustworkx 0.17+, Hamilton 1.89+, PyArrow, LibCST, deltalake 1.3+
