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
3. **Relationship Compilation** → Rule-driven Acero plans → relationship tables
4. **CPG Build** → Node/edge/property emission → final outputs

**Core Modules (`src/`):**
- `arrowdsl/` - Arrow DSL for relational transforms → PyArrow Acero plans
- `extract/` - Multi-source extractors (repo scan, AST, CST, symtable, bytecode, SCIP)
- `normalize/` - Byte-span canonicalization, stable ID generation
- `relspec/` - Relationship rule registry + compiler for deterministic joins
- `cpg/` - CPG schema definitions, node/edge/property contracts
- `engine/` - Execution runtime: sessions, plans, materialization
- `hamilton_pipeline/` - Hamilton DAG orchestration
- `datafusion_engine/` - DataFusion SessionContext integration
- `ibis_engine/` - Ibis expression generation and compilation
- `sqlglot_tools/` - SQL normalization, compilation, AST serialization

**Primary Entry Point:**
```python
from graph import GraphProductBuildRequest, build_graph_product

result = build_graph_product(
    GraphProductBuildRequest(repo_root=".")
)
```

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
- **Key Dependencies**: DataFusion 50.1+, Rustworkx 0.17+, Ibis 11.0+, SQLGlot 28.1+, Hamilton, PyArrow, LibCST, Polars
