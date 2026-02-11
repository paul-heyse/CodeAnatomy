# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Follow @AGENTS.md for agent-specific protocols.

## Project Overview

CodeAnatomy is an inference-driven Code Property Graph (CPG) builder for Python. It extracts multiple evidence layers from Python source code (LibCST, AST, symtable, bytecode, SCIP, tree-sitter, introspection) and compiles them into a rich, queryable graph representation using a Hamilton-driven DAG pipeline with PyArrow Acero for deterministic, contract-validated transformations.

## Skills (Use Before Guessing)

**IMPORTANT:** Always use these skills to understand code before proposing changes. They provide high-signal analysis that prevents incorrect assumptions.
Canonical CQ behavior/output semantics are documented in
`.claude/skills/cq/reference/cq_reference.md`.

| Skill | When to Use | Why |
|-------|-------------|-----|
| `/cq search` | Finding code patterns, symbols, identifiers | Fast semantic search with classification (Python default, `--lang rust` for Rust) |
| `/cq` | Before ANY refactor, rename, or signature change | Finds all callsites, impact, and breaking changes |
| `/cq q "entity=..."` | Finding functions/classes/imports | Entity-based code discovery |
| `/cq q "pattern=..."` | Structural code patterns | Zero false positives from strings/comments |
| `/cq q "...inside=..."` | Context-aware search | Find code in specific structural contexts |
| `/cq q "...scope=closure"` | Before extracting nested functions | Identifies variable capture issues |
| `/cq q "all=..." / "any=..."` | Combined/alternative patterns | Logical composition of queries |
| `/cq q "pattern='eval(\$X)'"` | Security pattern review | Identify security-sensitive constructs |
| `/cq q --format mermaid` | Understanding code flow | Visual call graphs and class diagrams |
| `/cq neighborhood` / `/cq nb` | Targeted semantic neighborhood analysis | Anchor/symbol resolution + structural/LSP neighborhood slices |
| `/cq run` | Multi-step execution with shared scan | Batch analysis workflows |
| `/cq run` with `type="neighborhood"` | Neighborhood in automation plans | Parity with CLI neighborhood behavior |
| `/cq ldmd` | Progressive disclosure of LDMD artifacts | Index/search/get/neighbors for long outputs |
| `/cq chain` | Command chaining frontend | Quick multi-command analysis |
| `/ast-grep` | Structural search/rewrites (not regex) | Pattern-based code transformation |
| `/dfdl_ref` | DataFusion + DeltaLake operations (query engine, storage, UDFs) | Don't guess APIs - probe versions |

### Mandatory cq Usage

**Before searching for code:** Use `/cq search <query>` for semantic code discovery. It classifies matches and groups by containing function, enriches with a 5-stage pipeline (ast_grep, python_ast, import_detail, libcst, tree_sitter), and tracks cross-source agreement for confidence. Output includes per-finding Code Facts clusters (Identity, Scope, Interface, Behavior, Structure) plus a top Code Overview block; summary/footer includes diagnostics such as `dropped_by_scope`.

**Before searching Rust code:** Use `/cq search <query> --lang rust` to narrow scope to Rust files. Use `/cq q "entity=... lang=rust"` for Rust entity queries. Scope is extension-authoritative (`python` => `.py/.pyi`, `rust` => `.rs`).

CQ parallel worker pools use multiprocessing `spawn` context (not `fork`) and fail open to sequential execution on worker errors.

**Before deep localized analysis:** Use `/cq neighborhood <target>` (or `/cq nb <target>`) with `target` as `file.py:line[:col]` or `symbol`. Resolution is deterministic and shared between CLI and run-plan execution.

**Validated reliability baseline (2026-02-11):**
- `cq run --step` and `cq run --steps` accept `type="neighborhood"` JSON payloads.
- `cq search --in <dir>` works reliably for `src/`, `tools/`, and `rust/` directory scopes.
- Directory include-glob constraining remains language-safe.

Known-good commands:
```bash
/cq run --step '{"type":"neighborhood","target":"tools/cq/search/python_analysis_session.py:1"}' --format summary
/cq run --steps '[{"type":"neighborhood","target":"tools/cq/search/python_analysis_session.py:1"}]' --format summary
/cq search PythonAnalysisSession --in tools/cq --format summary
/cq search compile --in rust --lang rust --format summary
/cq search RuntimeProfile --in src --format summary
```

**Before processing long CQ markdown artifacts:** Use `/cq ldmd index|get|neighbors|search` for progressive section retrieval, with `get` controls `--mode full|preview|tldr` and `--depth`.

**For LSP-rich evidence collection:** CQ now includes implemented advanced planes under `tools/cq/search`:
- semantic tokens + inlay hints overlays (`semantic_overlays.py`)
- pull diagnostics normalization (`diagnostics_pull.py`)
- code-action resolve/execute bridge (`refactor_actions.py`)
- rust-analyzer macro/runnables extensions (`rust_extensions.py`)

These planes are capability-gated and fail-open; they enrich outputs without blocking core search/query/run execution.

**Before modifying a function:** Run `/cq calls <function>` to find all call sites.

**Before changing a parameter:** Run `/cq impact <function> --param <param>` to trace data flow.

**Before changing a signature:** Run `/cq sig-impact <function> --to "<new_sig>"` to classify breakages.

**Before refactoring imports:** Run `/cq q "entity=import in=<dir>"` to understand import structure.

**Before extracting closures:** Run `/cq q "entity=function scope=closure in=<dir>"` to find variable captures.

**Before security review:** Use pattern queries like `/cq q "pattern='eval(\$X)'"` or `/cq q "pattern='pickle.load(\$X)'"`.

**Before combining conditions:** Use composite queries: `/cq q "all='pattern1,pattern2'"` (AND) or `/cq q "any='p1,p2'"` (OR).

**Before disambiguating patterns:** Use pattern objects: `/cq q "pattern.context='...' pattern.selector=<kind>"`.

**Rationale:** cq uses AST analysis, not text search. It catches indirect callers, forwarding patterns, and provides confidence scores. Guessing based on grep leads to missed call sites and broken code.

### Why Pattern Queries > Grep

Pattern queries (`/cq q "pattern=..."`) use ast-grep for structural matching:
- **No false positives** from strings, comments, or variable names
- **Metavariables** (`$X`, `$$$`, `$$X`) capture and enforce code structure
- **Meta-variable filtering** (`$X=~pattern`) constrains captures with regex
- **Context-aware** via relational constraints (`inside`, `has`, `precedes`, `follows`)
- **Composite logic** via `all`/`any`/`not` for complex queries
- **Disambiguation** via `pattern.context` and `pattern.selector`
- **Positional matching** via `nthChild` for argument/parameter positions

**Example - Finding dynamic dispatch:**
```bash
# Grep would match: log("getattr example")  # FALSE POSITIVE
# Pattern query matches only actual getattr calls:
/cq q "pattern='getattr(\$X, \$Y)'"

# Find with meta-variable filter (string literal attrs only):
/cq q "pattern='getattr(\$X, \$Y)' \$Y=~'^\"'"
```

**Example - Searching Rust code:**
```bash
# Find Rust function definitions matching a pattern
/cq q "entity=function name=~^register lang=rust"

# Structural pattern search in Rust
/cq q "pattern='pub fn \$F(\$$$) -> \$R' lang=rust"
```

### Understanding Code with Visualization

Use `--format mermaid`, `--format mermaid-class`, or `--format dot` to visualize:
- Call graphs before refactoring
- Class hierarchies before inheritance changes
- Import dependencies before restructuring

| Format | Use Case |
|--------|----------|
| `--format mermaid` | Call graphs, data flow |
| `--format mermaid-class` | Class hierarchies, inheritance |
| `--format dot` | Complex graphs for Graphviz |

```bash
# Call graph with transitive callers
/cq q "entity=function name=build_cpg expand=callers(depth=2)" --format mermaid

# Class hierarchy diagram
/cq q "entity=class in=src/semantics" --format mermaid-class

```

### cq Global Options

All `/cq` commands support `--format` for output control:
- `md` (default) - Markdown for Claude context
- `json` - Structured JSON
- `both` / `summary` - Combined or compact output
- `mermaid` / `mermaid-class` / `dot` - Visual diagrams
- `ldmd` - LDMD marker-preserving output for progressive disclosure tools

Config via `.cq.toml` or `CQ_*` environment variables. See `tools/cq/README.md` for full options.

CQ model boundary policy:
- Use `msgspec.Struct` for serialized CQ contracts crossing module boundaries.
- Keep parser/cache handles as runtime-only objects.
- Avoid `pydantic` in CQ hot paths (`tools/cq/search`, `tools/cq/query`, `tools/cq/run`).

## Build & Development Commands

```bash
# One-time setup (REQUIRED)
scripts/bootstrap_codex.sh && uv sync

# Quality gates (run AFTER task completion, not during implementation)
uv run ruff format               # Auto-format
uv run ruff check --fix          # Lint + autofix
uv run pyrefly check             # Type/contract validation (the strict gate)
uv run pyright                   # IDE-level type checking (basic mode)

# Testing
uv run pytest tests/unit/                    # Unit tests only
uv run pytest tests/ -m "not e2e"           # Exclude slow E2E tests
uv run pytest tests/unit/test_foo.py -v     # Single test file
uv run pytest --cov=src --cov-report=html   # With coverage

# Format code
uv run ruff format
```

**Critical:** Always use `uv run` for Python commands. Direct `python` calls will fail due to missing module imports.
**Exception:** Use `./cq` or `/cq` for the CQ tool; use `uv run` for all other Python invocations.

**Quality gate timing:** Complete the implementation first, then run quality checks. Don't interrupt workflow with mid-task linting or type checking.

## Architecture

**Four-Stage Pipeline:**
1. **Extraction** - Evidence tables from multiple sources (LibCST, AST, symtable, bytecode, SCIP, tree-sitter)
2. **Semantic Catalog + Normalization** - Canonical byte spans, stable IDs, join-ready shape
3. **Task/Plan Catalog + Scheduling** - TaskSpec builders - PlanCatalog - inferred TaskGraph - schedule
4. **CPG Build** - Node/edge/property emission - final outputs

**Core Modules (`src/`):**
- `extract/` - Multi-source extractors (repo scan, AST, CST, symtable, bytecode, SCIP)
- `semantics/` - Semantic catalog + normalization + incremental protocol (canonical)
- `normalize/` - Deprecated facade (re-exports semantics.catalog)
- `incremental/` - Deprecated facade (re-exports semantics.incremental)
- `relspec/` - Task/plan catalog, inferred deps, rustworkx graph inference + scheduling
- `cpg/` - CPG schema definitions, node/edge/property contracts
- `engine/` - Execution runtime: sessions, plans, materialization
- `hamilton_pipeline/` - Hamilton DAG orchestration
- `datafusion_engine/` - DataFusion integration (query planning, UDFs, lineage, schema contracts)
- `obs/` - Observability layer (diagnostics collection, metrics, scan telemetry)
- `storage/` - Delta Lake integration with file pruning

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
Extraction - Input Registry - SemanticCompiler - View Graph - Hamilton DAG - CPG Outputs
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
- **Inference-Driven**: Don't specify intermediate schemas-only strict boundaries (relationship outputs, final CPG)
- **Graceful Degradation**: Missing optional inputs produce correct-schema empty outputs, not exceptions

## Code Style Requirements

- **Imports**: Absolute imports only; type-only imports in `if TYPE_CHECKING:` blocks
- **Formatting**: 100-char lines, 4-space indent, double quotes, trailing commas
- **Typing**: All functions fully typed; no bare `Any`; strict Optional handling
- **Docstrings**: NumPy convention; imperative mood summary; Parameters/Returns/Raises sections
- **Complexity**: Cyclomatic <= 10, max 12 branches, max 6 return statements

**Every module must have:**
```python
from __future__ import annotations
```

## Project-Specific Rules

See `.claude/rules/` for detailed policies:
- `.claude/rules/python-quality.md` - Quality rules beyond ruff
- `.claude/rules/testing-policy.md` - Testing philosophy, allowed mocking patterns

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
- Do not worry about git cleanliness-`.gitignore` is properly configured
- The user handles uncommitted file changes independently

## Environment

- **Python**: 3.13.12 (pinned)
- **Package Manager**: uv
- **Key Dependencies**: DataFusion 50.1+, Rustworkx 0.17+, Hamilton 1.89+, PyArrow, LibCST, deltalake 1.3+
