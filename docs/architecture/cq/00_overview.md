# CQ Architecture Overview

This document provides a fully integrated architectural review of the CQ tool (`tools/cq/`, version 0.4.0), a multi-language code query and analysis system. It synthesizes the ten subsystem documents in this directory into a coherent picture of the system's design, data flow, cross-cutting concerns, and primary vectors for architectural improvement.

**Target audience:** Advanced LLM programmers with deep Python expertise, seeking to propose architectural improvements.

**Document map:**

| # | Title | File | Lines |
|---|-------|------|-------|
| 00 | CQ Architecture Overview | `00_overview.md` | 800+ |
| 01 | CLI & Command Framework | `01_cli_command_framework.md` | 1,370 |
| 02 | Search Pipeline | `02_search_subsystem.md` | 2,837 |
| 03 | Query Subsystem | `03_query_subsystem.md` | 1,378 |
| 04 | Analysis Commands | `04_analysis_commands.md` | 1,004 |
| 05 | Multi-Step Execution | `05_multi_step_execution.md` | 890 |
| 06 | Cross-Cutting Contracts & Orchestration | `06_cross_cutting_contracts.md` | 2,860 |
| 07 | Tree-Sitter & Structural Parsing Engine | `07_tree_sitter_engine.md` | 1,612 |
| 08 | Neighborhood Subsystem | `08_neighborhood_subsystem.md` | 1,994 |
| 09 | LDMD Format & Protocol | `09_ldmd_format.md` | 1,505 |
| 10 | Runtime Services | `10_runtime_services.md` | 1,794 |

---

## 1. System Identity

CQ is a code query tool that occupies a specific niche: **AST-aware code analysis with agent-friendly output**. It is not a language server, not a linter, and not a build tool. Its design center is providing high-signal, structurally-precise code intelligence to an LLM agent (or advanced developer) performing refactoring, impact analysis, or security review.

**Core value proposition:**
- Structural precision: AST-based matching eliminates false positives from strings, comments, and variable names
- Enrichment depth: Multi-source evidence (ast-grep, Python AST, import detail, LibCST, tree-sitter, symtable, bytecode, Pyrefly LSP, rust-analyzer LSP) with cross-source agreement tracking
- Impact awareness: Call graph traversal, data flow taint analysis, and signature change simulation
- Workflow composition: Multi-step execution with shared scan infrastructure
- Contextual neighborhoods: Semantic neighborhood assembly with capability-gated LSP enrichment and progressive disclosure via LDMD
- Front-door orientation: Single `FrontDoorInsightV1` contract across search/calls/entity commands providing target identity, neighborhood, risk, and confidence in the first output block
- Artifact-first diagnostics: Heavy diagnostic payloads offloaded to artifacts with compact in-band status lines

**Technology stack:**
- CLI: cyclopts for command routing
- Serialization: msgspec for zero-copy struct (de)serialization
- Structural matching: ast-grep-py (library binding, not subprocess)
- Text search: ripgrep for fast candidate generation
- Tree-sitter: Multi-language AST parsing with bounded query execution
- Parallelism: multiprocessing with `spawn` context (not `fork`)
- Type enrichment: Pyrefly LSP for Python semantic hover/type data, rust-analyzer for Rust enrichment
- Progressive disclosure: LDMD format for long document navigation
- DiskCache: Persistent workspace-scoped caching (diskcache FanoutCache)
- Hexagonal ports: Protocol-based service boundaries (SearchServicePort, EntityServicePort, etc.)
- WorkerScheduler: Dual-pool CPU/IO worker scheduling

**System scale (LOC estimates, 2026-02-15):**
- CLI framework: ~3,700 LOC
- Search subsystem: ~8,000 LOC
- Query subsystem: ~3,500 LOC
- Analysis commands: ~6,000 LOC
- Multi-step execution: ~1,800 LOC
- Cross-cutting contracts: ~3,000 LOC
- Tree-sitter engine: ~8,100 LOC
- Neighborhood subsystem: ~2,500 LOC
- LDMD format: ~1,200 LOC
- Runtime services: ~2,000 LOC
- **Total: ~40,000 LOC** across 10 major subsystems

---

## 2. Architectural Topology

CQ is organized as a layered system with four execution tiers, cross-cutting infrastructure, and a runtime services foundation.

```
                          CLI Layer (cyclopts)
                    config / context / rendering
                              |
         +--------------------+--------------------+-------------------+
         |                    |                    |                   |
    Search (rg +         Query (DSL +         Analysis          Neighborhood
    enrichment)          ast-grep)          (macros)          (SNB assembly)
         |                    |                    |                   |
         +--------------------+--------------------+-------------------+
                              |
                    Front-Door Insight Layer
              (FrontDoorInsightV1 contract, risk,
               neighborhood preview, degradation)
                              |
                    Shared Infrastructure
              (tree-sitter, ast-grep, DefIndex, scoring,
               multi-lang, serialization, LDMD)
                              |
                    Multi-Step Execution
                  (run / chain / batch)
                              |
                    Runtime Services
          (execution policy, cache, workers, LSP runtime)
```

### 2.1 Execution Tiers

**Tier 1: Search** (`tools/cq/search/`, ~8,000 LOC)
Entry point for code discovery. Ripgrep generates candidates; a 3-tier classification pipeline (heuristic → AST node → record-based) categorizes matches; a 5-stage enrichment pipeline (ast-grep → Python AST → import detail → LibCST → tree-sitter) adds structural metadata. Output groups findings by containing function with cross-source agreement indicators. Parallel classification uses multiprocessing `spawn` context (up to 4 workers, fail-open to sequential). Render-time enrichment for findings missing enrichment during initial collection.

**Tier 2: Query** (`tools/cq/query/`, ~3,500 LOC)
Declarative code queries via a token-based DSL. Follows classic compiler architecture: parse (DSL → Query IR), compile (IR → ToolPlan), execute (ToolPlan → CqResult). Supports entity queries (`entity=function name=~^build`), structural pattern queries (`pattern='getattr($X, $Y)'`), relational constraints (`inside`, `has`, `precedes`, `follows`), and composite logic (`all`, `any`, `not`). Batch execution via `BatchEntityQuerySession` for shared scans across multiple Q-steps.

**Tier 3: Analysis** (`tools/cq/macros/`, ~6,000 LOC)
Pre-built analysis commands: `calls` (call site census), `impact` (taint/data flow), `sig-impact` (signature change simulation), `scopes` (closure capture), `bytecode-surface` (bytecode dependencies), `side-effects` (import-time effects), `imports` (structure/cycles), `exceptions` (handling patterns). Each uses two-stage collection (fast ripgrep pre-filter → precise AST parse). Python-centric with Rust fallback stubs for limited support.

**Tier 4: Neighborhood** (`tools/cq/neighborhood/`, ~2,500 LOC)
Targeted semantic neighborhood analysis around code anchors. A 4-phase pipeline resolves a target (file:line:col or symbol), collects structural AST neighbors via tree-sitter, enriches with LSP evidence (capability-gated: Pyrefly for Python, rust-analyzer for Rust), and emits a typed `SemanticNeighborhoodBundleV1`. Output is rendered to markdown with a deterministic 17-slot section layout or to LDMD for progressive disclosure.

### 2.2 Cross-Cutting Infrastructure

**Tree-Sitter Engine** (`tools/cq/search/tree_sitter/`, ~8,100 LOC)
Multi-language AST parsing engine providing bounded query execution, language-specific enrichment lanes (Python/Rust), diagnostic collection, and structural exports. Serves as the foundation for search enrichment stage 5 and neighborhood structural collection. Features include windowing, autotune, budget management, injection runtime for Rust macro expansion, and comprehensive telemetry. Python lane includes locals index, facts collector, and fallback support. Rust lane includes injection runtime for docstrings, function signatures, and `macro_rules!` patterns.

**FrontDoor Insight** (`tools/cq/core/front_door_insight.py`, ~1,200 LOC)
Canonical cross-subsystem contract providing unified high-level analysis results across search/calls/entity commands. Embeds target identity, neighborhood preview (callers, callees, references, hierarchy/scope with total + bounded preview + availability + source), risk assessment (level, drivers, counters), confidence metrics (evidence kind, score, bucket), degradation status (per-subsystem: lsp, scan, scope_filter), budget controls (top_candidates, preview_per_slice, semantic_targets), and artifact references (diagnostics, neighborhood_overflow, telemetry) in a single compact schema.

**LDMD Progressive Disclosure** (`tools/cq/ldmd/`, ~1,200 LOC)
Structured markdown format with embedded section markers (`<!--LDMD:BEGIN-->` / `<!--LDMD:END-->`), byte-offset indexing, and protocol commands (index/get/search/neighbors). Enables random-access retrieval of large analysis artifacts without loading entire documents. Supports three extraction modes (full/preview/tldr) with depth control. Stack-validated parsing prevents malformed nesting.

**Runtime Services** (`tools/cq/core/runtime/`, ~2,000 LOC)
Process-global infrastructure providing execution policy management, dual-pool worker scheduling (CPU process-based via multiprocessing `spawn`, I/O thread-based), persistent disk-backed caching with fail-open semantics (workspace-scoped DiskCache FanoutCache, TTL-based eviction with default 900s), and LSP runtime coordination (Pyrefly for Python, rust-analyzer for Rust). Central `CqRuntimeServices` composition root wires dependencies. Fail-open architecture: cache/LSP unavailability never blocks execution.

### 2.3 Multi-Step Composition

The `run` and `chain` subsystems (`tools/cq/run/`, ~1,800 LOC) compose tiers into workflows. A `RunPlan` contains ordered steps that can mix search, query, analysis, and neighborhood commands (11 step types total). Multiple Q-steps sharing the same language scope are batched into a single ast-grep scan via `BatchEntityQuerySession`, avoiding redundant file I/O. NeighborhoodStep enables targeted semantic neighborhood analysis within automated workflows. TOML plan files provide human-readable workflow definitions; inline JSON steps support agent-driven automation.

---

## 3. Data Flow: End-to-End

Understanding the full data flow from user input to rendered output reveals the system's integration points and boundaries.

### 3.1 Single Command Flow

```
User Input (CLI args)
    |
    v
Config Resolution (CLI → env → .cq.toml → defaults)
    |
    v
CliContext Construction (root, toolchain, format, options)
    |
    v
Runtime Services Bootstrap (policy, cache, workers, LSP)
    |
    v
Command Dispatch (cyclopts @app.command routing)
    |
    v
+-- Search Path --------+-- Query Path ----------+-- Analysis Path ------+
| ripgrep candidates     | DSL parse → Query IR  | ripgrep pre-filter    |
| 3-tier classification  | compile → ToolPlan    | ast-grep/AST parse    |
| 5-stage enrichment     | execute → scan        | DefIndex lookup       |
| parallel ProcessPool   | entity/pattern match  | CallResolver binding  |
| cross-source agreement | relational filtering  | taint propagation     |
| Pyrefly LSP hover      | metavar extraction    | scoring               |
| tree-sitter stage 5    | tree-sitter queries   | front-door insight    |
| render-time enrichment | batch optimization    | neighborhood preview  |
+------------------------+------------------------+-----------------------+
                              |
                              v
                    CqResult Construction
                (RunMeta + findings + sections + summary)
                (summary.front_door_insight for search/calls/entity)
                              |
                              v
                    Render Dispatch
          (md | json | mermaid | mermaid-class | dot | ldmd | summary)
                              |
                              v
                    Output (stdout) + Optional Artifact (.cq/artifacts/)
```

### 3.2 Multi-Step Flow

```
RunPlan (TOML/JSON/inline steps)
    |
    v
Step Classification (Q-steps vs analysis steps vs search steps vs neighborhood)
    |
    v
Q-Step Batching (same lang-scope steps → BatchEntityQuerySession)
    |
    v
Single ast-grep scan (shared across batched steps)
    |
    v
Per-Step Execution (query IR → ToolPlan → results)
    |
    v
Result Merging (provenance tracking, step_id tagging)
    |
    v
Merged CqResult (aggregated findings, per-step sections)
```

### 3.3 Key Data Types at Boundaries

| Boundary | Type | Direction |
|----------|------|-----------|
| User → System | CLI args, `SearchRequest`, `Query` string | Input |
| Config → Runtime | `CqConfig`, `CliContext`, `RuntimeExecutionPolicy` | Internal |
| Search → Enrichment | `RgCandidate`, `PythonNodeEnrichmentRequest` | Internal |
| Query Parser → Planner | `Query` (IR) | Internal |
| Planner → Executor | `ToolPlan` | Internal |
| Executor → Scanner | `AstGrepRule`, `RuleSpec` | Internal |
| Scanner → Executor | `SgRecord` | Internal |
| Tree-sitter → Enrichment | `QueryExecutionTelemetryV1`, capture nodes | Internal |
| Analysis → Index | `DefIndex`, `CallResolver`, `ArgBinder` | Internal |
| Neighborhood → LSP | `TargetCoordinatesV1`, capability gates | Internal |
| Any front-door command → Output | `FrontDoorInsightV1` in `summary.front_door_insight` | Output |
| Any Command → Output | `CqResult` | Output |
| Output → Disk | `ContractEnvelope` (msgpack) or JSON | Persistence |

---

## 4. Contract Architecture

CQ enforces a strict boundary protocol for data types, documented fully in [06_cross_cutting_contracts.md](06_cross_cutting_contracts.md).

### 4.1 Three-Tier Type System

1. **Serialized Contracts** (msgspec.Struct): Cross module boundaries. Base classes `CqStruct`, `CqOutputStruct`, `CqSettingsStruct`, `CqCacheStruct` provide `frozen=True`, `kw_only=True`, `omit_defaults=True`. Used for: `CqResult`, `Finding`, `SearchSummaryContract`, request types, `CqConfig`, `RuntimeExecutionPolicy`.

2. **Runtime-Only Objects** (`@dataclass` or plain class): In-process state only. Used for: `ScanContext`, `EntityExecutionState`, `RunContext`, `DefIndex`.

3. **External Handles**: Parser/cache objects never serialized. Used for: `SgRoot`, `SgNode`, `pygit2.Repository`, tree-sitter `Query`/`Node` objects.

### 4.2 Canonical Output: CqResult

Every command produces `CqResult`, the universal output contract:

```python
class CqResult(msgspec.Struct):
    run: RunMeta           # Timing, command, toolchain, schema version
    summary: dict          # Command-specific summary metrics
    key_findings: list[Finding]  # Priority findings
    evidence: list[Finding]      # Supporting findings
    sections: list[Section]      # Grouped output blocks
    artifacts: list[Artifact]    # Generated file references
```

Priority ordering: `key_findings` → `sections` → `evidence`.

### 4.3 Enrichment Fact System

The Code Facts cluster system (6 clusters, 50+ fields) provides structured enrichment metadata per finding:

1. **Identity & Grounding** (8 fields): Language, symbol role, qualified name, binding targets
2. **Type Contract** (8 fields): Signature, parameters, return type, async/generator flags, visibility
3. **Call Graph** (2 fields, Python-only): Incoming callers, outgoing callees
4. **Class/Method Context** (6 fields): Enclosing class, bases, overrides, Rust struct/enum fields
5. **Local Scope Context** (5 fields, Python-only): Enclosing callable, assignments, narrowing hints
6. **Imports/Aliases** (2 fields): Alias chain, resolved import path

Each field has a `FactFieldSpec` with multi-level key paths for fallback resolution, language/kind applicability filters, and an `NAReason` for unavailable data (not applicable, not resolved, enrichment unavailable).

### 4.4 Front-Door Insight Contract

The `FrontDoorInsightV1` contract (`tools/cq/core/front_door_insight.py`) provides a single shared output schema for all front-door commands (`search`, `calls`, `entity`). Embedded in `CqResult.summary["front_door_insight"]`, it ensures agents always see the same high-signal block first:

- **target**: Symbol identity, kind, location, signature, selection reason
- **neighborhood**: Callers, callees, references, hierarchy/scope (each with total + bounded preview + availability + source)
- **risk**: Level (low/med/high), risk drivers, counters (callers, callees, hazards, forwarding, arg_shape_count, closure_capture_count, files_with_calls)
- **confidence**: Evidence kind, score, bucket
- **degradation**: Per-subsystem status (semantic, scan, scope_filter with notes)
- **budget**: Output bounds (top_candidates, preview_per_slice, semantic_targets)
- **artifact_refs**: Pointers to diagnostics, neighborhood_overflow, telemetry artifacts

Builder functions (`build_search_insight()`, `build_calls_insight()`, `build_entity_insight()`) construct the contract from command-specific data structures. `render_insight_card()` produces the markdown "Insight Card" section.

### 4.5 Artifact-First Diagnostics

CQ follows an artifact-first diagnostics policy: heavy diagnostic payloads are offloaded to `.cq/artifacts/` while compact status lines remain in-band.

**In-band (markdown summary):**
- One-line enrichment status (e.g., "Python semantic: 6/10 enriched")
- One-line scope/filter status (e.g., "Scope: python, 42 files")
- One-line degradation status (e.g., "Degradation: scan=truncated, semantic=partial")

**Artifact-only (JSON files):**
- `enrichment_telemetry`: Per-stage applied/degraded/skipped counts and timing
- `pyrefly_telemetry`: LSP hover/type enrichment stats
- `pyrefly_diagnostics`: Detailed enrichment failure messages
- `language_capabilities`: Per-language capability snapshot
- `cross_language_diagnostics`: Multi-language merge diagnostics
- Full per-stage timing and cache stats

Artifact references are included in `front_door_insight.artifact_refs` for retrieval. The artifact retrieval sub-app (`cq artifact`) provides cache-backed access to search bundles and diagnostics.

---

## 5. Cross-Cutting Concerns

### 5.1 Multi-Language Support

CQ supports Python and Rust with extension-authoritative scope enforcement:

- `python` scope: `.py`, `.pyi`
- `rust` scope: `.rs`
- `auto` scope: union of all

Language scope flows through the entire stack: file enumeration, candidate collection, ast-grep rule selection, tree-sitter query selection, enrichment pipeline selection, result merging. The multi-language orchestrator (`core/multilang_orchestrator.py`) partitions execution by language and merges results with deterministic ordering (Python priority, deduplicated by span overlap).

**Asymmetry:** Python has full enrichment (5-stage pipeline, Pyrefly LSP, symtable, bytecode, tree-sitter). Rust has 2-stage enrichment (ast-grep + tree-sitter) plus rust-analyzer LSP enrichment (hover, diagnostics, type hierarchy, macro expansion) with injection runtime for macro pattern extraction. Analysis macros are Python-only with partial Rust support (calls location-only, other macros not supported). This asymmetry is fundamental to the architecture, not a gap to fill uniformly.

**Rust enrichment capabilities (rust-analyzer):**
- Hover data with type information
- Pull diagnostics (textDocument/diagnostic, workspace/diagnostic)
- References and implementations
- Type hierarchy (supertypes, subtypes)
- Macro expansion (`experimental/expandMacro`)
- Runnables (`experimental/runnables`)

**Python enrichment capabilities (Pyrefly):**
- Hover data with type information
- Symbol grounding (definition, declaration, type definition, implementation)
- Type contracts (resolved type, callable signature, parameters, return type)
- References and implementations
- Type hierarchy (supertypes, subtypes)

### 5.2 Error Handling: Fail-Open Architecture

CQ follows a consistent fail-open philosophy across all subsystems:

- **Enrichment failures**: Degraded findings (partial enrichment) rather than errors. `degrade_reasons` list tracks which stages failed.
- **Classification failures**: Fallback to lower-tier classifier. Record-based classification is the terminal fallback.
- **External tool failures**: ast-grep errors fall back to ripgrep-only results. Ripgrep failures produce empty candidate sets.
- **Multi-step failures**: `stop_on_error=False` (default) continues execution; errors accumulated in per-step results.
- **Parallel worker failures**: ProcessPool with `spawn` context fails open to sequential execution.
- **Cache failures**: All cache operations fail open; cache unavailability never blocks execution.
- **LSP failures**: Capability-gated enrichment degrades gracefully; missing LSP never blocks core results.
- **Tree-sitter failures**: Budget exhaustion and parse errors produce partial results with telemetry.

**Degradation tracking:** Three layers. Findings retain `degrade_reasons: list[str]` for backward compatibility. The `FrontDoorInsightV1.degradation` field provides structured per-subsystem status (semantic, scan, scope_filter with notes). The SNB schema provides typed `DegradeEventV1` events with stage/category/severity/correlation_key. Neighborhood and insight artifacts carry the most detailed degradation records.

### 5.3 Performance Architecture

**Candidate generation:** Ripgrep provides O(n) file scanning with regex pre-filtering, narrowing the set before expensive AST parsing.

**Shared scan context:** `ScanContext` bundles ast-grep scan results (definition records, call records, interval index) for reuse across multiple queries in the same invocation. Typical speedup: 5-10x for multi-step plans.

**Tree-sitter bounded execution:** Query execution uses configurable match limits, depth limits, and time budgets. Window-based execution processes bounded byte ranges. Autotune adjusts limits dynamically based on query performance. Budget management prevents runaway queries on pathological files.

**Parallel enrichment:** `ProcessPoolExecutor` with `spawn` context (not `fork`, avoiding CPython GIL issues), max 4 workers, fail-open to sequential on worker errors. Dual-pool architecture separates CPU-intensive (process pool) from I/O-bound (thread pool) workloads via `WorkerScheduler`.

**Batch optimization:** Multiple Q-steps with the same language scope share a single ast-grep scan via `BatchEntityQuerySession`.

**Persistent caching:** Workspace-scoped DiskCache backend with TTL-based eviction (default 900s) provides persistent caching for calls target metadata, search partitions, and entity scans. The cache uses fail-open semantics, tag-based eviction, and is configurable via `CQ_CACHE_*` environment variables. In-memory per-invocation caches still exist alongside the persistent layer for short-lived data. DefIndex per-invocation rebuild remains, but result caching reduces redundant work.

**Render-time enrichment:** For findings missing enrichment (e.g., from macro commands), CQ performs on-demand enrichment at markdown render time. Enriches up to 9 unique files in parallel (4 workers) with multiprocessing `spawn` context. Results cached by `(file, line, col, language)` for deduplication. Findings beyond the file limit render without render-time enrichment.

### 5.4 Scoring System

Two-dimensional scoring (impact + confidence) applied to findings:

**ImpactSignals:** `sites` (call count), `files` (affected file count), `depth` (call chain depth), `breakages` (potential breaks), `ambiguities` (ambiguous references). Weighted combination → `impact_score` → bucketed to `high`/`med`/`low`.

**ConfidenceSignals:** Evidence quality tracking. Values: `resolved_ast`, `resolved_ast_bytecode`, `bytecode`, `resolved_ast_heuristic`, `bytecode_heuristic`, `cross_file_taint`, `heuristic`, `rg_only`, `unresolved`. Score ranges: 0.95 (full AST) down to 0.30 (unresolved).

The scoring model is underdeveloped relative to the rest of the architecture. Impact scoring has implicit formulas with no documentation; confidence scoring relies on evidence kind classification with fixed score mappings.

---

## 6. Subsystem Integration Map

This section maps how subsystems interact at their boundaries, revealing coupling points and potential improvement seams.

### 6.1 Search <-> Query Integration

The query command (`q`) falls back to search when the input lacks query DSL tokens:

```python
# cli_app/commands/query.py
has_tokens = _has_query_tokens(query_string)
try:
    parsed_query = parse_query(query_string)
except QueryParseError:
    if not has_tokens:
        return smart_search(query_string, ...)  # Fallback
```

This creates a bidirectional dependency: the query module imports search as a fallback. The search module, conversely, uses ast-grep rules from the astgrep module (shared with query). The coupling is functional rather than structural -- both consume the same `CqResult` output contract.

### 6.2 Analysis <-> Index Integration

All analysis macros depend on the index infrastructure:

- `DefIndex`: Built once, shared across `calls`, `impact`, `sig-impact`, `scopes`
- `CallResolver`: Uses `DefIndex` for call site → declaration resolution (3-strategy: local → import → global)
- `ArgBinder`: Uses `FnDecl` parameters from `DefIndex` for argument binding

The index is rebuilt from scratch on every invocation. For analysis commands that need the full index (e.g., `calls` scanning the entire repo), this means O(n) startup cost proportional to repository size. However, persistent caching of calls target metadata significantly reduces redundant work for repeated queries.

### 6.3 Multi-Step <-> All Subsystems

The `run` subsystem can invoke any other subsystem via `RunStep` types:

| Step Type | Target Subsystem | Shared Scan |
|-----------|-----------------|-------------|
| `QStep` | Query | Yes (batched) |
| `SearchStep` | Search | No |
| `CallsStep` | Analysis (calls) | No |
| `ImpactStep` | Analysis (impact) | No |
| `SigImpactStep` | Analysis (sig-impact) | No |
| `ScopesStep` | Analysis (scopes) | No |
| `BytecodeSurfaceStep` | Analysis (bytecode) | No |
| `ImportsStep` | Analysis (imports) | No |
| `ExceptionsStep` | Analysis (exceptions) | No |
| `SideEffectsStep` | Analysis (side-effects) | No |
| `NeighborhoodStep` | Neighborhood | No |

Only Q-steps benefit from shared scan optimization. Analysis and search steps are executed independently, each rebuilding their own indexes and candidate sets. This is the most significant performance opportunity in the multi-step system.

### 6.4 Tree-Sitter as Shared Infrastructure

Tree-sitter is consumed by three subsystems:

1. **Search enrichment**: Stage 5 of the Python enrichment pipeline uses tree-sitter queries for structural metadata extraction
2. **Neighborhood collection**: Structural collector uses tree-sitter queries for caller/callee/reference extraction
3. **Rust enrichment**: Rust lane uses tree-sitter with injection runtime for macro expansion and syntax analysis

The tree-sitter engine provides language-specific query selection, bounded execution with budgets/limits, windowing for large files, autotune for dynamic adjustment, and comprehensive telemetry. See [07_tree_sitter_engine.md](07_tree_sitter_engine.md) for full architecture.

### 6.5 LDMD as Progressive Disclosure Infrastructure

LDMD (`tools/cq/ldmd/`) provides progressive disclosure markdown format for long outputs, with byte-offset indexing, stack-validated parsing, and a 4-command protocol (index/get/search/neighbors). It enables large documents (e.g., neighborhood outputs) to be navigated section-by-section rather than as monolithic text blocks. The LDMD format is integrated into the output rendering pipeline via `OutputFormat.LDMD`, allowing any CQ command to emit LDMD-formatted artifacts for subsequent exploration.

**LDMD commands:**
- `index`: Build section index with byte offsets
- `get`: Extract section by ID with mode (full/preview/tldr) and depth control
- `search`: Search within sections
- `neighbors`: Navigate to prev/next sections

---

## 7. Architectural Patterns

### 7.1 Two-Stage Collection

Every subsystem that touches source code follows the same pattern:

1. **Fast scan**: Ripgrep or ast-grep identifies candidate files/locations
2. **Precise parse**: Full AST parsing only on candidates

This is the fundamental performance strategy. It trades recall (ripgrep may miss candidates) for speed (avoids parsing non-matching files). The trade-off is acceptable because ripgrep patterns are conservative (broad regex matching) and ast-grep provides the precision layer.

### 7.2 Compiler Architecture in Query

The query subsystem follows a textbook compiler pipeline:

```
DSL string → Tokenizer → Parser → Query IR → Planner → ToolPlan → Executor → CqResult
```

This separation enables:
- Query validation at parse time (syntax errors before execution)
- Plan optimization at compile time (rule complexity routing)
- Batch execution sharing (multiple plans against one scan)

### 7.3 Immutable Data Flow

All cross-boundary types are frozen (`frozen=True`). CqResult, Finding, Query, ToolPlan, RunMeta -- all immutable. Mutation happens only in runtime-only objects (`ScanContext`, `EntityExecutionState`).

This enables:
- Safe parallel processing (no shared mutable state)
- Deterministic serialization (no mutation between encode calls)
- Cache safety (frozen objects are hashable)

### 7.4 Fail-Open with Degradation Tracking

Rather than exception-driven error handling, CQ returns degraded results:

```
Success path:  Input → Enrichment → Full Finding
Failure path:  Input → Partial Enrichment → Degraded Finding + degrade_reasons
Fatal path:    Input → Error CqResult(success=False)
```

The degradation tracking is evolving. Legacy `degrade_reasons: list[str]` provides backward compatibility. `InsightDegradationV1` provides compact per-subsystem status. `DegradeEventV1` in SNB provides typed events with stage/category/severity/correlation_key for detailed diagnostics.

### 7.5 Hexagonal Service Layer

Runtime services use port/adapter architecture:

```python
class SearchServicePort(Protocol):
    def execute_search(self, request: SearchRequest) -> CqResult: ...

class SearchService(SearchServicePort):
    def __init__(self, runtime: CqRuntimeServices): ...
```

This pattern enables:
- Clean dependency injection via composition root
- Protocol-based boundaries (no concrete dependencies)
- Testability (mock services via protocol)
- Swappable implementations

---

## 8. Systemic Improvement Vectors

The subsystem documents identify numerous per-module improvement opportunities. This section synthesizes them into systemic themes that span multiple subsystems.

### 8.1 Persistent Index / Scan Cache

**Status:** **PARTIALLY IMPLEMENTED**

**Affected subsystems:** Search, Query, Analysis, Multi-Step

**Current state:** Persistent DiskCache-backed caching implemented in `core/cache/` with TTL-based eviction (default 900s), workspace-scoped singletons, and fail-open semantics. Caching covers calls target metadata, search partitions, and entity scans. DefIndex per-invocation rebuild remains, but result caching reduces redundant work. See [10_runtime_services.md](10_runtime_services.md) for comprehensive documentation.

**Remaining improvement opportunities:** Persistent DefIndex caching with file-mtime-based invalidation, tree-sitter parse result caching, shared scan context persistence. Key design decisions: cache granularity (per-file SgRecord sets vs. full ScanContext), invalidation strategy (mtime vs. content hash), and cache format (msgpack for speed, JSON for debuggability).

### 8.2 Structured Error/Degradation Model

**Affected subsystems:** All (error handling is cross-cutting)

**Current state:** Degradation tracked via three layers: legacy `list[str]` in `Finding.degrade_reasons`, compact `InsightDegradationV1` in front-door outputs, and typed `DegradeEventV1` in SNB. Error results use `CqResult(success=False)` with error message in summary. Structured degradation is implemented in SNB/neighborhood but not fully propagated to all subsystems.

**Systemic impact:** Consumers cannot uniformly react to specific failure modes across all subsystems. Same root cause (e.g., Pyrefly timeout) may appear as string entries in some contexts, typed events in others.

**Improvement direction:** Consolidate on `DegradeEventV1` across all subsystems, replacing flat `degrade_reasons`. Aggregate related events with correlation keys. Support partial result recovery with explicit degradation metadata.

### 8.3 Request/Config Type Consolidation

**Affected subsystems:** Search (5 request types), Query (3+ execution context types), Config (CqConfig + CLI params + env)

**Current state:** Search has `SearchRequest`, `SearchConfig`, `CandidateSearchRequest`, `CandidateCollectionRequest`, `RgRunRequest` -- five types with significant field overlap. Query has `QueryExecutionContext`, `EntityExecutionState`, `PatternExecutionState`, `AstGrepExecutionContext`. Runtime has `RuntimeExecutionPolicy` with hierarchical policy structs. Config resolution produces `CqConfig` that partially duplicates CLI parameter definitions.

**Systemic impact:** Unclear which type is authoritative at each boundary. Field additions require updating multiple types. No provenance tracking for resolved values.

**Improvement direction:** Consolidate to one request type per subsystem with clear lifecycle: `UserInput` → `ResolvedConfig` → `ExecutionContext`. Add resolution provenance (`ConfigSource` annotations).

### 8.4 Scoring Model Maturation

**Affected subsystems:** Analysis (primary consumer), Search (secondary consumer)

**Current state:** `ImpactSignals` has 5 fields (sites, files, depth, breakages, ambiguities). `ConfidenceSignals` has evidence_kind classification with fixed score mappings. Scoring formulas are implicit. No scorer protocol or strategy pattern.

**Systemic impact:** Scores are black-box numbers with no documentation of how they are computed. Confidence is effectively determined by evidence kind classification. No way to calibrate or customize scoring for different use cases.

**Improvement direction:** Document scoring formulas explicitly. Expand `ConfidenceSignals` with tool agreement, contradiction count, coverage metrics. Define `Scorer` protocol for pluggable scoring strategies.

### 8.5 Language Extensibility

**Affected subsystems:** Multi-language orchestration, ast-grep rules, tree-sitter queries, file enumeration, enrichment pipeline

**Current state:** Two languages (Python, Rust) with hard-coded extension mapping, hard-coded enrichment pipeline selection, hard-coded merge priority. Adding a language requires changes in 6+ locations: `language.py`, `rules.py`, `multilang_orchestrator.py`, `smart_search.py`, `files.py`, enrichment modules, tree-sitter lane dispatch.

**Systemic impact:** The language abstraction is incomplete. Each subsystem reimplements language dispatch. No single registration point for a new language.

**Improvement direction:** `LanguagePlugin` protocol defining: extensions, ast-grep rules, tree-sitter queries, enrichment pipeline, merge priority. Central `LanguageRegistry` replaces scattered if/else dispatch. Not necessarily plugin-based (static registration is fine), but unified dispatch.

### 8.6 Renderer Modularity

**Affected subsystems:** Output formatting (core/renderers, core/report.py)

**Current state:** Markdown renderer is 1383 lines in a single module. Mermaid and DOT renderers duplicate graph extraction logic. Format dispatch is a hardcoded dict. No plugin mechanism for custom formats.

**Systemic impact:** Individual renderer components (finding formatting, context blocks, enrichment facts display) cannot be tested independently. Adding a new format requires core code changes.

**Improvement direction:** Extract shared graph extraction into `core/graph_extraction.py` returning `GraphModel(nodes, edges)`. Define `Renderer` protocol. Extract sub-renderers (finding formatter, context formatter, enrichment formatter) as composable units.

### 8.7 Analysis Macro Scan Sharing

**Affected subsystems:** Multi-step execution, Analysis macros

**Current state:** Only Q-steps benefit from shared scan optimization via `BatchEntityQuerySession`. Analysis steps (calls, impact, sig-impact, etc.) each perform independent scans, rebuilding DefIndex and running ripgrep pre-filters separately.

**Systemic impact:** A `cq run` plan with 5 analysis steps performs 5 independent repository scans. The shared infrastructure exists (ScanContext, DefIndex) but is not wired into the multi-step framework for non-Q steps.

**Improvement direction:** Lift DefIndex construction into the RunPlan execution frame. Share a single DefIndex across all analysis steps in a run. Requires refactoring analysis macros to accept injected DefIndex rather than building their own.

### 8.8 Tree-Sitter Query Optimization

**Affected subsystems:** Tree-sitter engine, Search enrichment, Neighborhood collection

**Current state:** Tree-sitter queries execute with conservative default limits. Autotune adjusts dynamically but starts from scratch per invocation. Query compilation happens per-file. No persistent query plan cache.

**Systemic impact:** Large files trigger frequent limit exhaustion. Query compilation overhead adds up across many files. No learning across invocations.

**Improvement direction:** Persistent autotune profiles keyed by file characteristics. Compiled query cache. Adaptive windowing based on file/query characteristics. Budget pooling across related queries.

---

## 9. Module Size and Complexity Profile

Understanding where code mass concentrates reveals maintenance hotspots and decomposition opportunities.

| Module | Lines | Subsystem | Complexity Note |
|--------|-------|-----------|-----------------|
| `search/pipeline/smart_search.py` | 3769 | Search | Orchestration + parallel pools + result assembly |
| `query/executor.py` | 3235 | Query | Entity + pattern execution + inline rules |
| `search/python/extractors.py` | 2236 | Search | 5-stage pipeline + agreement validation |
| `macros/calls.py` | 2311 | Analysis | Call census + argument shape + scoring + insight |
| `core/report.py` | 1383 | Rendering | Markdown + enrichment facts + context |
| `core/front_door_insight.py` | 1171 | Cross-cutting | Insight contract + builders + rendering |
| `run/runner.py` | 1297 | Multi-step | Batching + scope + language expansion |
| `query/parser.py` | 992 | Query | DSL tokenizer + parser + IR construction |
| `macros/impact.py` | 902 | Analysis | Taint propagation + inter-procedural |
| `query/ir.py` | 765 | Query | 17-field Query struct + PatternSpec + RelationalConstraint |
| `neighborhood/tree_sitter_collector.py` | 696 | Neighborhood | Tree-sitter structural collection |
| `index/def_index.py` | 676 | Index | Full-repo function/class index |
| `query/planner.py` | 658 | Query | IR → ToolPlan compilation |
| `query/enrichment.py` | 629 | Query | Symtable/bytecode enrichment |

Eight modules exceed 1000 lines. The top three (`smart_search.py`, `executor.py`, `extractors.py`) each carry multiple responsibilities that could be decomposed into smaller, independently testable units.

---

## 10. Dependency Graph

External dependencies and their roles:

| Dependency | Role | Subsystem |
|------------|------|-----------|
| `ast_grep_py` | Structural AST matching (library binding) | Query, Search, Analysis |
| `msgspec` | Zero-copy serialization, contract types | All (core) |
| `cyclopts` | CLI framework with parameter groups | CLI |
| `pygit2` | Git index access (libgit2 bindings) | File enumeration |
| `pathspec` | Gitignore pattern matching | File enumeration |
| `pyrefly` | LSP-based type/symbol enrichment (Python) | Search, Neighborhood |
| `tree-sitter` | Multi-language AST parsing | Search, Neighborhood, Tree-sitter engine |
| `diskcache` | Persistent disk-backed cache | Runtime services |
| `uuid6` | UUID v7 generation for run IDs | Telemetry |
| `rustworkx` | Graph algorithms (removed from active use) | Deprecated |
| ripgrep (`rg`) | Fast text search (subprocess) | Search, Analysis |
| rust-analyzer | LSP-based Rust enrichment (subprocess) | Search, Neighborhood (Rust) |

**Notable:** ast-grep is used as a Python library binding (`from ast_grep_py import ...`), not via subprocess. Tree-sitter is also used as a library via Python bindings. This is a deliberate design choice: library-level access enables direct node manipulation, metavariable extraction, and shared AST reuse. Ripgrep and rust-analyzer, conversely, are invoked via subprocess -- their native speed makes IPC overhead negligible relative to processing time.

---

## 11. Design Decisions and Trade-Offs

### 11.1 Library vs. Subprocess for External Tools

**Decision:** Use library bindings for ast-grep and tree-sitter; use subprocess for ripgrep and rust-analyzer.

**Rationale:** Library bindings enable shared AST reuse, direct node manipulation, and metavariable extraction. Subprocess invocation is acceptable when IPC overhead is negligible relative to tool execution time (ripgrep's native speed, rust-analyzer's analysis latency).

**Trade-off:** Library bindings create tighter coupling and version dependencies. Subprocess invocation adds IPC overhead and complicates error handling.

### 11.2 Fail-Open vs. Fail-Fast

**Decision:** Fail-open for all optional enrichment; fail-fast for core execution.

**Rationale:** Enrichment failures should not block core analysis results. Users get degraded but usable results rather than errors. Core execution failures (parse errors, file not found) are fatal because recovery is not meaningful.

**Trade-off:** Fail-open can hide bugs. Degradation tracking is essential but adds complexity.

### 11.3 Per-Invocation Index vs. Persistent Cache

**Decision:** Per-invocation DefIndex rebuild with persistent result caching.

**Rationale:** Simpler lifecycle management, no invalidation logic. Persistent cache reduces redundant work for repeated queries. Full persistent DefIndex requires complex invalidation and increases cache size.

**Trade-off:** O(n) startup cost for analysis commands that need full index. Persistent result cache mitigates but does not eliminate this cost.

### 11.4 Python/Rust Asymmetry

**Decision:** Full enrichment for Python, structural enrichment + LSP for Rust.

**Rationale:** Python analysis is the primary use case. Rust enrichment provides LSP-backed capabilities (hover, diagnostics, type hierarchy, macro expansion) without duplicating Python's bytecode/symtable analysis (which has no Rust equivalent).

**Trade-off:** Analysis macros are Python-only. Rust users get search/query/neighborhood but not calls/impact/sig-impact.

### 11.5 Multiprocessing Spawn vs. Fork

**Decision:** Use `spawn` context for all multiprocessing.

**Rationale:** `fork` in multi-threaded Python processes causes GIL deadlocks and unpredictable behavior. `spawn` is slower to start workers but safe in all contexts.

**Trade-off:** Higher worker startup overhead. Mitigated by worker pooling and fail-open to sequential execution.

---

## 12. Observability and Telemetry

CQ instruments execution with multiple telemetry layers:

**RunMeta:** Every `CqResult` includes execution metadata (command, timing, toolchain, schema version, run ID).

**Run ID:** UUID v7 for deterministic timestamp-ordered run identification. Enables artifact correlation and cache key generation.

**Front-Door Insight:** Embedded in search/calls/entity results, provides high-level analysis summary with degradation status, artifact references, and budget controls.

**Telemetry dictionaries:** Per-subsystem telemetry counters (files_scanned, matches_found, enrichment_applied, etc.) in `CqResult.summary`.

**Artifact diagnostics:** Offloaded diagnostic payloads in `.cq/artifacts/` with references in `front_door_insight.artifact_refs`.

**Tree-sitter telemetry:** Query execution metrics (matches, timeouts, budget_exhausted, parse_errors) in `QueryExecutionTelemetryV1`.

**LSP runtime telemetry:** Capability snapshots, request/response timing, failure counts for Pyrefly and rust-analyzer.

**Worker scheduler telemetry:** Task submission counts, worker pool utilization, failure modes.

---

## 13. Testing Strategy (Implied)

While testing is not explicitly documented, the architecture implies a testing strategy:

**Unit testing:** Frozen contracts (CqStruct) enable deterministic serialization testing. Protocol boundaries enable service mocking.

**Integration testing:** Shared scan context and batch optimization require multi-query integration tests.

**Golden testing:** CLI commands (e.g., neighborhood, ldmd) likely use golden file comparison for output stability.

**Property testing:** Parser and planner components could benefit from property-based testing (all valid inputs parse, all IR compiles).

**Performance testing:** Tree-sitter autotune and batch optimization require benchmarking infrastructure.

---

## 14. Documentation Coverage

The architecture is documented across 10 subsystem documents:

- [01_cli_command_framework.md](01_cli_command_framework.md): CLI layer (cyclopts, commands, config, rendering)
- [02_search_subsystem.md](02_search_subsystem.md): Search pipeline (ripgrep, classification, enrichment)
- [03_query_subsystem.md](03_query_subsystem.md): Query DSL (parser, planner, executor)
- [04_analysis_commands.md](04_analysis_commands.md): Analysis macros (calls, impact, sig-impact, etc.)
- [05_multi_step_execution.md](05_multi_step_execution.md): Multi-step execution (run, chain, batch)
- [06_cross_cutting_contracts.md](06_cross_cutting_contracts.md): Contracts (CqResult, Finding, enrichment)
- [07_tree_sitter_engine.md](07_tree_sitter_engine.md): Tree-sitter engine (queries, lanes, budgets)
- [08_neighborhood_subsystem.md](08_neighborhood_subsystem.md): Neighborhood assembly (SNB, LSP, collectors)
- [09_ldmd_format.md](09_ldmd_format.md): LDMD progressive disclosure (format, protocol, commands)
- [10_runtime_services.md](10_runtime_services.md): Runtime services (cache, workers, LSP, policy)

Each document follows a consistent structure: module map, data flow, key abstractions, integration points, improvement vectors.

---

## 15. Future Directions

Based on the improvement vectors and architectural patterns, future development should focus on:

1. **Persistent scan/index caching** with file-mtime invalidation to eliminate O(n) startup costs
2. **Unified degradation model** (`DegradeEventV1`) across all subsystems with correlation keys
3. **Language plugin registry** to enable third-language support without core changes
4. **Analysis macro scan sharing** to enable multi-step plans with shared DefIndex
5. **Renderer modularity** with protocol-based boundaries and composable sub-renderers
6. **Scoring model documentation** with explicit formulas and calibration strategies
7. **Tree-sitter query optimization** with persistent autotune profiles and compiled query cache
8. **Request/config consolidation** with clear lifecycle and provenance tracking

---

This overview synthesizes the current state of the CQ architecture as of 2026-02-15. It reflects the actual implementation, not aspirational design. All file paths, LOC counts, and module references are based on active code inspection. For detailed subsystem information, consult the referenced subsystem documents.
