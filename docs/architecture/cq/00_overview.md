# CQ Architecture Overview

This document provides a fully integrated architectural review of the CQ tool (`tools/cq/`, version 0.3.0), a multi-language code query and analysis system. It synthesizes the nine subsystem documents in this directory into a coherent picture of the system's design, data flow, cross-cutting concerns, and primary vectors for architectural improvement.

**Target audience:** Advanced LLM programmers with deep Python expertise, seeking to propose architectural improvements.

**Document map:**

| Document | Scope | Lines |
|----------|-------|-------|
| [01_core_infrastructure.md](01_core_infrastructure.md) | CLI, config, toolchain, result pipeline, multi-language orchestration, error handling, serialization, code indexing | 1077 |
| [02_search_subsystem.md](02_search_subsystem.md) | Smart search, 3-tier classification, 5-stage enrichment, parallel pools, Pyrefly LSP, cross-source agreement | 1819 |
| [03_query_subsystem.md](03_query_subsystem.md) | Query DSL grammar, IR, parser, planner, executor, batch spans, metavariables, multi-language | 1242 |
| [04_analysis_commands.md](04_analysis_commands.md) | 8 macro commands (calls, impact, sig-impact, scopes, bytecode, side-effects, imports, exceptions), DefIndex, scoring | 803 |
| [05_multi_step_execution.md](05_multi_step_execution.md) | RunPlan model, 11 step types, TOML/JSON plans, shared scan, chain command, result merging, provenance | 846 |
| [06_data_models.md](06_data_models.md) | Contract architecture, CqStruct, schema types, enrichment facts, FrontDoorInsightV1, serialization codecs, boundary protocol | ~2400 |
| [07_ast_grep_and_formatting.md](07_ast_grep_and_formatting.md) | ast-grep-py integration, rule system, output renderers, file scanning, gitignore, shared scan context | 1124 |
| [08_neighborhood_subsystem.md](08_neighborhood_subsystem.md) | Semantic neighborhood assembly, SNB schema, 4-phase pipeline, capability gating, section layout, CLI/run integration | 1957 |
| [09_ldmd_format.md](09_ldmd_format.md) | LDMD progressive disclosure format, parser/writer architecture, protocol commands, OutputFormat integration | 1505 |

---

## 1. System Identity

CQ is a code query tool that occupies a specific niche: **AST-aware code analysis with agent-friendly output**. It is not a language server, not a linter, and not a build tool. Its design center is providing high-signal, structurally-precise code intelligence to an LLM agent (or advanced developer) performing refactoring, impact analysis, or security review.

**Core value proposition:**
- Structural precision: AST-based matching eliminates false positives from strings, comments, and variable names
- Enrichment depth: Multi-source evidence (ast-grep, Python AST, LibCST, tree-sitter, symtable, bytecode, Pyrefly LSP) with cross-source agreement tracking
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
- Parallelism: multiprocessing with `spawn` context (not `fork`)
- Type enrichment: Pyrefly LSP for semantic hover data
- Progressive disclosure: LDMD format for long document navigation

---

## 2. Architectural Topology

CQ is organized as a layered system with four execution tiers and a shared infrastructure layer.

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
              (ast-grep, DefIndex, scoring,
               multi-lang, serialization, LDMD)
                              |
                    Multi-Step Execution
                  (run / chain / batch)
```

### 2.1 Execution Tiers

**Tier 1: Search** (`tools/cq/search/`)
Entry point for code discovery. Ripgrep generates candidates; a 3-tier classification pipeline (heuristic -> AST node -> record-based) categorizes matches; a 5-stage enrichment pipeline (ast-grep -> Python AST -> import detail -> LibCST -> tree-sitter) adds structural metadata. Output groups findings by containing function with cross-source agreement indicators.

**Tier 2: Query** (`tools/cq/query/`)
Declarative code queries via a token-based DSL. Follows classic compiler architecture: parse (DSL -> Query IR), compile (IR -> ToolPlan), execute (ToolPlan -> CqResult). Supports entity queries (`entity=function name=~^build`), structural pattern queries (`pattern='getattr($X, $Y)'`), relational constraints (`inside`, `has`, `precedes`, `follows`), and composite logic (`all`, `any`, `not`).

**Tier 3: Analysis** (`tools/cq/macros/`)
Pre-built analysis commands: `calls` (call site census), `impact` (taint/data flow), `sig-impact` (signature change simulation), `scopes` (closure capture), `bytecode` (bytecode surface), `side-effects` (import-time effects), `imports` (structure/cycles), `exceptions` (handling patterns). Each uses two-stage collection (fast ripgrep pre-filter -> precise AST parse).

**Tier 4: Neighborhood** (`tools/cq/neighborhood/`)
Targeted semantic neighborhood analysis around code anchors. A 4-phase pipeline resolves a target (file:line:col or symbol), collects structural AST neighbors, enriches with LSP evidence (capability-gated), and emits a typed `SemanticNeighborhoodBundleV1`. Output is rendered to markdown with a deterministic 17-slot section layout or to LDMD for progressive disclosure.

### 2.2 Multi-Step Composition

The `run` and `chain` subsystems (`tools/cq/run/`) compose tiers into workflows. A `RunPlan` contains ordered steps that can mix search, query, analysis, and neighborhood commands (11 step types total). Multiple Q-steps sharing the same language scope are batched into a single ast-grep scan via `BatchEntityQuerySession`, avoiding redundant file I/O. NeighborhoodStep is the newest addition, enabling targeted semantic neighborhood analysis within automated workflows.

---

## 3. Data Flow: End-to-End

Understanding the full data flow from user input to rendered output reveals the system's integration points and boundaries.

### 3.1 Single Command Flow

```
User Input (CLI args)
    |
    v
Config Resolution (CLI -> env -> .cq.toml -> defaults)
    |
    v
CliContext Construction (root, toolchain, format, options)
    |
    v
Command Dispatch (cyclopts @app.command routing)
    |
    v
+-- Search Path --------+-- Query Path ----------+-- Analysis Path ------+
| ripgrep candidates     | DSL parse -> Query IR  | ripgrep pre-filter    |
| 3-tier classification  | compile -> ToolPlan    | ast-grep/AST parse    |
| 5-stage enrichment     | execute -> scan        | DefIndex lookup       |
| parallel ProcessPool   | entity/pattern match   | CallResolver binding  |
| cross-source agreement | relational filtering   | taint propagation     |
| Pyrefly LSP hover      | metavar extraction     | scoring               |
+------------------------+------------------------+-----------------------+
                              |
                              v
                    CqResult Construction
                (RunMeta + findings + sections + summary)
                (summary.front_door_insight for search/calls/entity)
                              |
                              v
                    Render Dispatch
          (md | json | mermaid | mermaid-class | dot | summary)
                              |
                              v
                    Output (stdout) + Optional Artifact (.cq/artifacts/)
```

### 3.2 Multi-Step Flow

```
RunPlan (TOML/JSON/inline steps)
    |
    v
Step Classification (Q-steps vs analysis steps vs search steps)
    |
    v
Q-Step Batching (same lang-scope steps -> BatchEntityQuerySession)
    |
    v
Single ast-grep scan (shared across batched steps)
    |
    v
Per-Step Execution (query IR -> ToolPlan -> results)
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
| User -> System | CLI args, `SearchRequest`, `Query` string | Input |
| Config -> Runtime | `CqConfig`, `CliContext` | Internal |
| Search -> Enrichment | `RgCandidate`, `PythonNodeEnrichmentRequest` | Internal |
| Query Parser -> Planner | `Query` (IR) | Internal |
| Planner -> Executor | `ToolPlan` | Internal |
| Executor -> Scanner | `AstGrepRule`, `RuleSpec` | Internal |
| Scanner -> Executor | `SgRecord` | Internal |
| Analysis -> Index | `DefIndex`, `CallResolver`, `ArgBinder` | Internal |
| Any front-door command -> Output | `FrontDoorInsightV1` in `summary.front_door_insight` | Output |
| Any Command -> Output | `CqResult` | Output |
| Output -> Disk | `ContractEnvelope` (msgpack) | Persistence |

---

## 4. Contract Architecture

CQ enforces a strict boundary protocol for data types, documented fully in [06_data_models.md](06_data_models.md).

### 4.1 Three-Tier Type System

1. **Serialized Contracts** (msgspec.Struct): Cross module boundaries. Base class `CqStruct` provides `frozen=True`, `kw_only=True`, `omit_defaults=True`. Used for: `CqResult`, `Finding`, `SearchSummaryContract`, request types, `CqConfig`.

2. **Runtime-Only Objects** (`@dataclass` or plain class): In-process state only. Used for: `ScanContext`, `EntityExecutionState`, `RunContext`, `DefIndex`.

3. **External Handles**: Parser/cache objects never serialized. Used for: `SgRoot`, `SgNode`, `pygit2.Repository`.

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

Priority ordering: `key_findings` -> `sections` -> `evidence`.

### 4.3 Enrichment Fact System

The Code Facts cluster system (6 clusters, 50+ fields) provides structured enrichment metadata per finding:

1. **Identity & Grounding** (8 fields): Language, symbol role, qualified name, binding targets
2. **Type Contract** (8 fields): Signature, parameters, return type, async/generator flags
3. **Call Graph** (2 fields, Python-only): Incoming callers, outgoing callees
4. **Class/Method Context** (6 fields): Enclosing class, bases, overrides, Rust struct/enum fields
5. **Local Scope Context** (5 fields, Python-only): Enclosing callable, assignments, narrowing hints
6. **Imports/Aliases** (2 fields): Alias chain, resolved import path

Each field has a `FactFieldSpec` with multi-level key paths for fallback resolution, language/kind applicability filters, and an `NAReason` for unavailable data.

### 4.4 Front-Door Insight Contract

The `FrontDoorInsightV1` contract (`tools/cq/core/front_door_insight.py`) provides a single shared output schema for all front-door commands (`search`, `calls`, `entity`). Embedded in `CqResult.summary["front_door_insight"]`, it ensures agents always see the same high-signal block first:

- **target**: Symbol identity, kind, location, signature, selection reason
- **neighborhood**: Callers, callees, references, hierarchy/scope (each with total + bounded preview + availability + source)
- **risk**: Level (low/med/high), risk drivers, counters
- **confidence**: Evidence kind, score, bucket
- **degradation**: Per-subsystem status (lsp, scan, scope_filter)
- **budget**: Output bounds (top_candidates, preview_per_slice, lsp_targets)
- **artifact_refs**: Pointers to diagnostics, neighborhood overflow, telemetry artifacts

Builder functions (`build_search_insight()`, `build_calls_insight()`, `build_entity_insight()`) construct the contract from command-specific data structures. `render_insight_card()` produces the markdown "Insight Card" section.

### 4.5 Artifact-First Diagnostics

CQ follows an artifact-first diagnostics policy: heavy diagnostic payloads are offloaded to `.cq/artifacts/` while compact status lines remain in-band.

**In-band (markdown summary):**
- One-line enrichment status
- One-line scope/filter status
- One-line degradation status

**Artifact-only:**
- enrichment_telemetry
- pyrefly_telemetry
- pyrefly_diagnostics
- language_capabilities
- cross_language_diagnostics
- full per-stage timing and cache stats

Artifact references are included in `front_door_insight.artifact_refs` for retrieval.

---

## 5. Cross-Cutting Concerns

### 5.1 Multi-Language Support

CQ supports Python and Rust with extension-authoritative scope enforcement:

- `python` scope: `.py`, `.pyi`
- `rust` scope: `.rs`
- `auto` scope: union of all

Language scope flows through the entire stack: file enumeration, candidate collection, ast-grep rule selection, enrichment pipeline selection, result merging. The multi-language orchestrator (`core/multilang_orchestrator.py`) partitions execution by language and merges results with deterministic ordering (Python priority, deduplicated by span overlap).

**Asymmetry:** Python has full enrichment (5-stage pipeline, Pyrefly LSP, symtable, bytecode). Rust has 2-stage enrichment (tree-sitter syntax analysis). Analysis macros are Python-only. This asymmetry is fundamental to the architecture, not a gap to fill uniformly.

### 5.2 Error Handling: Fail-Open Architecture

CQ follows a consistent fail-open philosophy across all subsystems:

- **Enrichment failures**: Degraded findings (partial enrichment) rather than errors. `degrade_reasons` list tracks which stages failed.
- **Classification failures**: Fallback to lower-tier classifier. Record-based classification is the terminal fallback.
- **External tool failures**: ast-grep errors fall back to ripgrep-only results. Ripgrep failures produce empty candidate sets.
- **Multi-step failures**: `stop_on_error=False` (default) continues execution; errors accumulated in per-step results.
- **Parallel worker failures**: ProcessPool with `spawn` context fails open to sequential execution.

**Degradation tracking:** Two layers. Findings retain `degrade_reasons: list[str]` for backward compatibility. The `FrontDoorInsightV1.degradation` field provides structured per-subsystem status (lsp, scan, scope_filter). The SNB schema provides typed `DegradeEventV1` events with stage/category/severity/correlation_key. Neighborhood and insight artifacts carry the most detailed degradation records.

### 5.3 Performance Architecture

**Candidate generation:** Ripgrep provides O(n) file scanning with regex pre-filtering, narrowing the set before expensive AST parsing.

**Shared scan context:** `ScanContext` bundles ast-grep scan results (definition records, call records, interval index) for reuse across multiple queries in the same invocation. Typical speedup: 5-10x for multi-step plans.

**Parallel enrichment:** `ProcessPoolExecutor` with `spawn` context (not `fork`, avoiding CPython GIL issues), max 4 workers, fail-open to sequential on worker errors.

**Batch optimization:** Multiple Q-steps with the same language scope share a single ast-grep scan via `BatchEntityQuerySession`.

**No persistent caching:** All indexes and scan results are in-memory, rebuilt per invocation. No disk cache, no incremental index updates.

### 5.4 Scoring System

Two-dimensional scoring (impact + confidence) applied to findings:

**ImpactSignals:** `sites` (call count), `files` (affected file count), `depth` (call chain depth), `breakages` (potential breaks), `ambiguities` (ambiguous references). Weighted combination -> `impact_score` -> bucketed to `high`/`med`/`low`.

**ConfidenceSignals:** Currently minimal (`evidence_kind: str` only). Values: `ast`, `bytecode`, `scip`, `static_analysis`, `unresolved`.

The scoring model is underdeveloped relative to the rest of the architecture. Impact scoring has implicit formulas with no documentation; confidence scoring carries a single field.

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
- `CallResolver`: Uses `DefIndex` for call site -> declaration resolution (3-strategy: local -> import -> global)
- `ArgBinder`: Uses `FnDecl` parameters from `DefIndex` for argument binding

The index is rebuilt from scratch on every invocation. For analysis commands that need the full index (e.g., `calls` scanning the entire repo), this means O(n) startup cost proportional to repository size.

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
| `BytecodeStep` | Analysis (bytecode) | No |
| `ImportsStep` | Analysis (imports) | No |
| `ExceptionsStep` | Analysis (exceptions) | No |
| `SideEffectsStep` | Analysis (side-effects) | No |
| `NeighborhoodStep` | Neighborhood | No |

Only Q-steps benefit from shared scan optimization. Analysis and search steps are executed independently, each rebuilding their own indexes and candidate sets. This is the most significant performance opportunity in the multi-step system.

### 6.4 AST-Grep as Shared Infrastructure

ast-grep-py is consumed by three subsystems:

1. **Query executor**: Pattern matching via `SgRoot.find_all()` and inline `Rule` objects
2. **Search classifier**: AST node classification for candidate enrichment
3. **Analysis macros**: Entity scanning via `sg_scan()` for definition/call records

The `RuleSpec` system (`tools/cq/astgrep/sgpy_scanner.py`) provides the canonical rule representation. Language-dispatched rule loading (`rules.py` -> `rules_py.py` / `rules_rust.py`) selects rules per language. Python has 23 rules covering 6 record types; Rust has 8 rules.

### 6.5 LDMD as Progressive Disclosure Infrastructure

LDMD (`tools/cq/ldmd/`) provides progressive disclosure markdown format for long outputs, with byte-offset indexing, stack-validated parsing, and a 4-command protocol (index/get/search/neighbors). It enables large documents (e.g., neighborhood outputs) to be navigated section-by-section rather than as monolithic text blocks. The LDMD format is integrated into the output rendering pipeline via `OutputFormat.LDMD`, allowing any CQ command to emit LDMD-formatted artifacts for subsequent exploration.

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
DSL string -> Tokenizer -> Parser -> Query IR -> Planner -> ToolPlan -> Executor -> CqResult
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
Success path:  Input -> Enrichment -> Full Finding
Failure path:  Input -> Partial Enrichment -> Degraded Finding + degrade_reasons
Fatal path:    Input -> Error CqResult(success=False)
```

The degradation tracking is the weakest part of this pattern. `degrade_reasons` is `list[str]`, providing no structure for downstream consumers to react to specific failure modes.

---

## 8. Systemic Improvement Vectors

The subsystem documents identify numerous per-module improvement opportunities. This section synthesizes them into systemic themes that span multiple subsystems.

### 8.1 Persistent Index / Scan Cache

**Affected subsystems:** Search, Query, Analysis, Multi-Step

**Current state:** All indexes (DefIndex), scan contexts (ScanContext), and enrichment caches are in-memory, rebuilt per invocation. Multi-step execution shares scans within a single run, but separate invocations start from scratch.

**Systemic impact:** Repository scan is O(n) per invocation. For large repos (>50k files), this dominates execution time. Analysis macros that need full DefIndex pay startup cost even for targeted queries.

**Improvement direction:** Persistent scan cache (`.cq-cache/`) with file-mtime-based invalidation. Key design decisions: cache granularity (per-file SgRecord sets vs. full ScanContext), invalidation strategy (mtime vs. content hash), and cache format (msgpack for speed, JSON for debuggability).

### 8.2 Structured Error/Degradation Model

**Affected subsystems:** All (error handling is cross-cutting)

**Current state:** Degradation tracked as `list[str]` in `Finding.degrade_reasons`. Error results use `CqResult(success=False)` with error message in summary. No structured error types, severity levels, correlation IDs, or partial result recovery.

**Systemic impact:** Consumers cannot programmatically react to specific failure modes. Same root cause (e.g., Pyrefly timeout) appears as N independent string entries. No way to distinguish "no results found" from "error prevented results."

**Improvement direction:** `DegradeEventV1` is now implemented in the SNB and neighborhood subsystems. `InsightDegradationV1` provides compact per-subsystem status in front-door outputs. Remaining work: propagate structured degradation to all findings (replacing flat `degrade_reasons`), aggregate related events with correlation keys, and support partial result recovery.

### 8.3 Request/Config Type Consolidation

**Affected subsystems:** Search (5 request types), Query (3+ execution context types), Config (CqConfig + CLI params + env)

**Current state:** Search has `SearchRequest`, `SearchConfig`, `CandidateSearchRequest`, `CandidateCollectionRequest`, `RgRunRequest` -- five types with significant field overlap. Query has `QueryExecutionContext`, `EntityExecutionState`, `PatternExecutionState`, `AstGrepExecutionContext`. Config resolution produces `CqConfig` that partially duplicates CLI parameter definitions.

**Systemic impact:** Unclear which type is authoritative at each boundary. Field additions require updating multiple types. No provenance tracking for resolved values.

**Improvement direction:** Consolidate to one request type per subsystem with clear lifecycle: `UserInput` -> `ResolvedConfig` -> `ExecutionContext`. Add resolution provenance (`ConfigSource` annotations).

### 8.4 Scoring Model Maturation

**Affected subsystems:** Analysis (primary consumer), Search (secondary consumer)

**Current state:** `ImpactSignals` has 5 fields (sites, files, depth, breakages, ambiguities). `ConfidenceSignals` has 1 field (evidence_kind). Scoring formulas are implicit. No scorer protocol or strategy pattern.

**Systemic impact:** Scores are black-box numbers with no documentation of how they are computed. Confidence is effectively binary (resolved vs. unresolved). No way to calibrate or customize scoring for different use cases.

**Improvement direction:** Document scoring formulas explicitly. Expand `ConfidenceSignals` with tool agreement, contradiction count, coverage metrics. Define `Scorer` protocol for pluggable scoring strategies.

### 8.5 Language Extensibility

**Affected subsystems:** Multi-language orchestration, ast-grep rules, file enumeration, enrichment pipeline

**Current state:** Two languages (Python, Rust) with hard-coded extension mapping, hard-coded enrichment pipeline selection, hard-coded merge priority. Adding a language requires changes in 6+ locations: `language.py`, `rules.py`, `multilang_orchestrator.py`, `smart_search.py`, `files.py`, enrichment modules.

**Systemic impact:** The language abstraction is incomplete. Each subsystem reimplements language dispatch. No single registration point for a new language.

**Improvement direction:** `LanguagePlugin` protocol defining: extensions, ast-grep rules, enrichment pipeline, merge priority. Central `LanguageRegistry` replaces scattered if/else dispatch. Not necessarily plugin-based (static registration is fine), but unified dispatch.

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

---

## 9. Module Size and Complexity Profile

Understanding where code mass concentrates reveals maintenance hotspots and decomposition opportunities.

| Module | Lines | Subsystem | Complexity Note |
|--------|-------|-----------|-----------------|
| `search/smart_search.py` | 2514 | Search | Orchestration + parallel pools + result assembly |
| `search/python_enrichment.py` | 2174 | Search | 5-stage pipeline + agreement validation |
| `query/executor.py` | 2473 | Query | Entity + pattern execution + inline rules |
| `macros/calls.py` | 1429 | Analysis | Call census + argument shape + scoring |
| `core/report.py` | 1383 | Rendering | Markdown + enrichment facts + context |
| `run/runner.py` | 1038 | Multi-step | Batching + scope + language expansion |
| `search/classifier.py` | 1025 | Search | 3-tier classification pipeline |
| `macros/impact.py` | 901 | Analysis | Taint propagation + inter-procedural |
| `query/parser.py` | 965 | Query | DSL tokenizer + parser + IR construction |
| `query/ir.py` | 766 | Query | 17-field Query struct + PatternSpec + RelationalConstraint |
| `index/def_index.py` | 676 | Index | Full-repo function/class index |
| `query/planner.py` | 659 | Query | IR -> ToolPlan compilation |
| `query/enrichment.py` | 600+ | Query | Symtable/bytecode enrichment |

Six modules exceed 1000 lines. The top three (`smart_search.py`, `python_enrichment.py`, `executor.py`) each carry multiple responsibilities that could be decomposed into smaller, independently testable units.

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
| `pyrefly` | LSP-based type/symbol enrichment | Search (Python) |
| ripgrep (`rg`) | Fast text search (subprocess) | Search, Analysis |

**Notable:** ast-grep is used as a Python library binding (`from ast_grep_py import ...`), not via subprocess. This is a deliberate design choice: library-level access enables direct `SgNode` manipulation, metavariable extraction, and shared AST reuse. Ripgrep, conversely, is invoked via subprocess -- its Rust-native speed makes IPC overhead negligible relative to scan time.

---

## 11. Design Decisions and Trade-Offs

### 11.1 Library vs. Subprocess for AST Tools

**Decision:** ast-grep as library, ripgrep as subprocess.
**Rationale:** ast-grep results require deep integration (node traversal, metavar extraction, multi-rule sharing). Ripgrep results are flat text lines that need no post-processing beyond JSON parsing.
**Trade-off:** Library binding ties CQ to ast-grep-py's Python API surface. Subprocess isolation would allow swapping implementations but at the cost of stdout parsing overhead.

### 11.2 msgspec vs. Pydantic

**Decision:** msgspec for all serialized contracts.
**Rationale:** 10-50x faster serialization, frozen/kw_only defaults prevent mutation and argument ordering bugs, `omit_defaults=True` produces compact JSON.
**Trade-off:** Less ecosystem support than Pydantic. No built-in validation beyond type checking. No OpenAPI schema generation.

### 11.3 spawn vs. fork for Parallel Workers

**Decision:** `spawn` context for ProcessPoolExecutor.
**Rationale:** `fork` is unsafe with CPython's GIL in multi-threaded contexts. `spawn` avoids inheriting parent process state.
**Trade-off:** Higher process creation overhead (full Python interpreter startup per worker). Mitigated by limiting to max 4 workers and fail-open semantics.

### 11.4 No Persistent Index

**Decision:** Rebuild all indexes per invocation.
**Rationale:** Simplicity. No cache invalidation bugs. No stale index risk. No disk I/O for cache management.
**Trade-off:** O(n) startup cost per invocation, proportional to repository size. Acceptable for repos <100k files; becomes bottleneck beyond that.

### 11.5 Flat Finding Model

**Decision:** `CqResult.findings` is a flat list, not a tree.
**Rationale:** Simpler rendering, serialization, and merging. Flat lists compose well across multi-step execution.
**Trade-off:** Cannot natively represent hierarchical relationships (class -> method -> callsite). Hierarchy is encoded in `Section` grouping or `DetailPayload` metadata.

---

## 12. Testing Surface

CQ's test infrastructure is distributed across:

- `tools/cq/core/tests/` - Core unit tests (schema, scoring, serialization)
- Integration tests embedded in subsystem test directories
- The tool itself is used for self-analysis via `/cq` skill invocations

**Notable gap:** No golden snapshot tests for CQ output (unlike the main CodeAnatomy project which has `tests/cli_golden/` and `tests/plan_golden/`). Output format stability is enforced by convention rather than snapshots.

---

## 13. Reading Order for Improvement Proposals

For someone planning architectural improvements, the recommended reading order depends on the target:

**For performance improvements:**
1. [07_ast_grep_and_formatting.md](07_ast_grep_and_formatting.md) - Shared scan context, file discovery
2. [05_multi_step_execution.md](05_multi_step_execution.md) - Batch optimization, scan sharing limits
3. [02_search_subsystem.md](02_search_subsystem.md) - Parallel pools, caching architecture

**For extensibility improvements (new languages, formats):**
1. [01_core_infrastructure.md](01_core_infrastructure.md) - Multi-language orchestration, render dispatch
2. [07_ast_grep_and_formatting.md](07_ast_grep_and_formatting.md) - Rule system, format renderers
3. [03_query_subsystem.md](03_query_subsystem.md) - Language scope, metavariable system

**For data model improvements:**
1. [06_data_models.md](06_data_models.md) - Contract architecture, boundary protocol
2. [01_core_infrastructure.md](01_core_infrastructure.md) - CqResult schema, error handling
3. [02_search_subsystem.md](02_search_subsystem.md) - Enrichment fact system, agreement tracking

**For analysis capability improvements:**
1. [04_analysis_commands.md](04_analysis_commands.md) - Macro architecture, scoring, taint analysis
2. [03_query_subsystem.md](03_query_subsystem.md) - Query IR, relational constraints
3. [06_data_models.md](06_data_models.md) - Scoring models, confidence signals

**For contextual analysis and progressive disclosure:**
1. [08_neighborhood_subsystem.md](08_neighborhood_subsystem.md) - Semantic neighborhood assembly, 4-phase pipeline, LSP enrichment
2. [09_ldmd_format.md](09_ldmd_format.md) - LDMD format specification, parser architecture, protocol commands
3. [01_core_infrastructure.md](01_core_infrastructure.md) - OutputFormat integration, rendering pipeline

---

## 14. Summary of Architectural Strengths

1. **Structural precision**: AST-based analysis eliminates false positives. The two-stage collection pattern (ripgrep -> ast-grep) balances speed with accuracy.

2. **Immutable contracts**: Frozen msgspec structs throughout the pipeline enable safe parallelism and deterministic serialization.

3. **Fail-open resilience**: No single enrichment stage failure can abort the pipeline. Degraded results are always preferred over no results.

4. **Compositional execution**: The multi-step framework with shared scan infrastructure enables complex analysis workflows without redundant I/O.

5. **Agent-friendly output**: CqResult is designed for LLM consumption with priority ordering (key_findings -> sections -> evidence), Code Facts clusters, and structured metadata.

6. **Compiler-inspired query system**: The parse -> compile -> execute pipeline enables validation, optimization, and composability.

## 15. Summary of Systemic Improvement Opportunities

| Priority | Theme | Subsystem Scope | Key Documents |
|----------|-------|-----------------|---------------|
| High | Persistent index/scan cache | All | 01, 02, 05, 07 |
| High | Analysis macro scan sharing | Multi-step, Analysis | 04, 05 |
| Medium | Structured error/degradation model | All | 01, 02, 04 |
| Medium | Request/config type consolidation | Search, Query, Config | 01, 02, 06 |
| Medium | Scoring model maturation | Analysis, Search | 04, 06 |
| Low | Language extensibility (plugin model) | All | 01, 03, 07 |
| Low | Renderer modularity | Output | 01, 07 |

These priorities reflect estimated impact-to-effort ratio. Persistent caching and analysis scan sharing yield the largest performance improvements for the least architectural disruption. Language plugins and renderer refactoring offer long-term extensibility at higher implementation cost.
