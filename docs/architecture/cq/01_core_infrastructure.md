# Core Infrastructure Architecture

## Overview

CQ is a multi-language code query and analysis tool providing AST-based search, structural pattern matching, call graph analysis, impact analysis, and multi-step workflow execution. Version 0.3.0. Located at `tools/cq/`.

**Technology Stack:**
- **CLI**: cyclopts for command routing and parameter parsing
- **Serialization**: msgspec for zero-copy struct (de)serialization
- **Structural Matching**: ast-grep-py for AST pattern queries
- **Text Search**: ripgrep for fast regex/literal search
- **Parallelism**: multiprocessing with spawn context for classification/enrichment

**Primary Use Cases:**
1. Pre-refactoring impact analysis (call sites, data flow, signature changes)
2. Security pattern detection (eval/exec/pickle.load/subprocess.shell)
3. Architecture comprehension (call graphs, class hierarchies, import structures)
4. Multi-step analysis workflows (shared scan infrastructure)

## Package Structure

```
tools/cq/
├── __init__.py
├── astgrep/                # ast-grep library integration
│   ├── __init__.py
│   ├── sgpy_scanner.py     # Core scanner (489 lines)
│   ├── rules.py            # Language-dispatched rule loading
│   ├── rules_py.py         # Python ast-grep rules (23 rules)
│   └── rules_rust.py       # Rust ast-grep rules (8 rules)
├── cli_app/                # CLI application layer (cyclopts)
│   ├── __init__.py
│   ├── app.py              # Root App, global options, meta-app launcher
│   ├── config.py           # Config chain building (pyproject.toml, env, CLI)
│   ├── config_types.py     # CqConfig struct
│   ├── context.py          # CliContext, CliResult
│   ├── options.py          # QueryOptions, options_from_params
│   ├── params.py           # QueryParams (cyclopts parameter group)
│   ├── result.py           # Result rendering dispatch
│   ├── step_types.py       # Step type definitions for run/chain
│   ├── types.py            # OutputFormat enum
│   ├── validators.py       # Input validators
│   └── commands/           # CLI command implementations
│       ├── __init__.py
│       ├── admin.py        # Deprecated admin commands (index, cache)
│       ├── analysis.py     # Analysis macro commands (calls, impact, etc.)
│       ├── chain.py        # Chain command entry
│       ├── ldmd.py         # LDMD protocol commands (index, get, search, neighbors)
│       ├── neighborhood.py # Neighborhood CLI command
│       ├── query.py        # Query (q) command entry
│       ├── report.py       # Report command
│       ├── run.py          # Run command entry
│       └── search.py       # Search command entry
├── core/                   # Shared infrastructure
│   ├── __init__.py         # Re-exports: Anchor, Artifact, CqResult, Finding, RunMeta, Section, Toolchain
│   ├── artifacts.py        # Artifact storage (diagnostics, neighborhood overflow, results)
│   ├── bundles.py          # TargetSpec, TargetScope, BundleContext
│   ├── codec.py            # JSON/msgpack encoders/decoders
│   ├── contracts.py        # ContractEnvelope, contract serialization
│   ├── enrichment_facts.py # Code Facts cluster system (6 clusters, 50+ fields)
│   ├── findings_table.py   # FindingRecord (tabular flattening for filtering)
│   ├── front_door_insight.py  # FrontDoorInsightV1 contract, builders, rendering (904 lines)
│   ├── locations.py        # SourceSpan, byte-to-char conversion
│   ├── merge.py            # Step result merging with provenance
│   ├── multilang_orchestrator.py # Multi-language execution and merge
│   ├── multilang_summary.py     # Multi-language summary building
│   ├── renderers/          # Output format renderers
│   │   ├── __init__.py
│   │   ├── dot.py          # Graphviz DOT renderer
│   │   └── mermaid.py      # Mermaid flowchart/class diagram renderer
│   ├── report.py           # Markdown report renderer
│   ├── requests.py         # SummaryBuildRequest, MergeResultsRequest
│   ├── run_context.py      # RunContext (runtime-only)
│   ├── schema.py           # Core schema: CqResult, Finding, Section, Anchor, RunMeta, DetailPayload, ScoreDetails
│   ├── scoring.py          # Impact/confidence scoring system
│   ├── serialization.py    # High-level JSON/msgpack API
│   ├── structs.py          # CqStruct base class
│   ├── tests/              # Core unit tests
│   └── toolchain.py        # Toolchain detection (rg, sgpy, python versions)
├── index/                  # Code indexing infrastructure
│   ├── __init__.py
│   ├── arg_binder.py       # Argument → parameter binding
│   ├── call_resolver.py    # Call expression → FnDecl resolution
│   ├── def_index.py        # DefIndex (function/class definition index, 676 lines)
│   ├── files.py            # File tabulation with gitignore
│   └── gitignore.py        # .gitignore parsing
├── introspection/          # Runtime introspection utilities
│   └── __init__.py
├── macros/                 # Analysis command implementations
│   ├── __init__.py         # Re-exports all cmd_* functions
│   ├── bytecode.py         # Bytecode surface analysis
│   ├── calls.py            # Call site census (1429 lines)
│   ├── exceptions.py       # Exception pattern analysis
│   ├── impact.py           # Taint/data flow analysis (901 lines)
│   ├── imports.py          # Import structure analysis
│   ├── scopes.py           # Scope/closure analysis
│   ├── side_effects.py     # Import-time side effects
│   └── sig_impact.py       # Signature change analysis
├── query/                  # Declarative query DSL
│   ├── __init__.py         # Re-exports: Query, parse_query, etc.
│   ├── batch_spans.py      # Batch relational span collection
│   ├── enrichment.py       # SymtableEnricher
│   ├── execution_context.py # QueryExecutionContext
│   ├── execution_requests.py # EntityQueryRequest, PatternQueryRequest
│   ├── executor.py         # Query execution engine
│   ├── ir.py               # Query IR (all type definitions)
│   ├── language.py         # Language types, scopes, extensions
│   ├── metavar.py          # Metavariable parsing/filtering
│   ├── parser.py           # Query DSL parser
│   └── planner.py          # Query → ToolPlan compiler
├── run/                    # Multi-step execution
│   ├── __init__.py
│   ├── batch.py            # BatchEntityQuerySession
│   ├── chain.py            # Chain command implementation
│   ├── plan.py             # RunPlan, step types, TOML/JSON loading
│   └── runner.py           # Execution engine
├── search/                 # Smart search subsystem
│   ├── __init__.py         # Re-exports: SearchConfig, classifier types, adapter functions
│   ├── adapter.py          # Ripgrep adapter functions
│   ├── classifier.py       # 3-tier classification pipeline
│   ├── collector.py        # RgCollector for streaming JSON events
│   ├── context_window.py   # Context snippet extraction
│   ├── contracts.py        # Search summary contracts
│   ├── enrichment/         # Enrichment sub-modules
│   │   ├── __init__.py
│   │   ├── contracts.py    # EnrichmentMeta, PythonEnrichmentPayload
│   │   └── core.py         # Enrichment normalization
│   ├── models.py           # SearchConfig, SearchRequest
│   ├── profiles.py         # SearchLimits presets
│   ├── python_enrichment.py # 5-stage Python enrichment (2174 lines)
│   ├── requests.py         # Search request contracts
│   ├── rust_enrichment.py  # Rust enrichment (2 stages)
│   ├── smart_search.py     # Main search orchestration (2514 lines)
│   └── timeout.py          # Timeout wrappers
├── neighborhood/           # Semantic neighborhood assembly (R0-R4)
│   ├── __init__.py
│   ├── bundle_builder.py   # 4-phase assembly orchestration
│   ├── structural_collector.py  # AST-based neighborhood collection
│   ├── section_layout.py   # 17-slot SECTION_ORDER, dynamic collapse
│   ├── capability_gates.py # LSP capability gating
│   ├── scan_snapshot.py    # ScanSnapshot adapter
│   ├── target_resolution.py # Target symbol resolution
│   ├── snb_renderer.py     # SNB markdown rendering
│   └── pyrefly_adapter.py  # Pyrefly integration for type data
├── ldmd/                   # LDMD progressive disclosure format (R6)
│   ├── __init__.py
│   ├── format.py           # Strict parser with stack validation
│   └── writer.py           # Document writer, preview/body split
└── utils/                  # Shared utilities
    └── __init__.py
```

**Module Responsibilities:**
- `cli_app/`: Command routing, parameter parsing, config resolution, output dispatch
- `core/`: Shared types (CqResult, Finding, Section), serialization, rendering, multi-language orchestration
- `index/`: Definition indexing, call resolution, argument binding, gitignore-aware file enumeration
- `macros/`: Pre-built analysis commands (calls, impact, sig-impact, scopes, etc.)
- `query/`: Declarative query DSL (entity queries, pattern queries, relational constraints)
- `run/`: Multi-step workflow execution (shared scan, step composition)
- `search/`: Smart search (ripgrep integration, classification, enrichment)
- `neighborhood/`: Semantic neighborhood assembly (4-phase orchestration, LSP integration)
- `ldmd/`: LDMD progressive disclosure format (strict parsing, preview/body split)
- `astgrep/`: AST pattern matching via ast-grep-py (Python and Rust rules)

## CLI Architecture

### Meta-App Launcher Pattern

Built on cyclopts with a meta-app launcher pattern for extensibility:

```python
# cli_app/app.py
app = App(
    name="cq",
    default_command=lambda: None,
    version_flags=["--version"],
)

@app.default
def main(
    *,
    root: Annotated[Path | None, Parameter(...)] = None,
    verbose: Annotated[int, Parameter(...)] = 0,
    format: Annotated[OutputFormat, Parameter(...)] = OutputFormat.MD,
    artifact_dir: Annotated[Path | None, Parameter(...)] = None,
    no_save_artifact: Annotated[bool, Parameter(...)] = False,
) -> int:
    """CQ code query tool."""
    ...
```

**Global Options:**
- `--root`: Repository root path (default: cwd)
- `--verbose`: Verbosity level 0-3 (default: 0)
- `--format`: Output format (md, json, summary, mermaid, mermaid-class, dot, ldmd)
- `--artifact-dir`: Directory for saved artifacts (default: .cq/artifacts)
- `--no-save-artifact`: Disable artifact persistence

**Output Formats:**
```python
class OutputFormat(StrEnum):
    md = "md"                    # Markdown (default)
    json = "json"                # JSON
    both = "both"                # JSON + Markdown
    summary = "summary"          # Compact summary
    mermaid = "mermaid"          # Mermaid flowchart
    mermaid_class = "mermaid-class"  # Mermaid class diagram
    dot = "dot"                  # Graphviz DOT
    ldmd = "ldmd"                # LLM-friendly progressive disclosure markdown
```

### Config Resolution Chain

Configuration resolved via three-layer chain with precedence ordering:

```python
# cli_app/config.py
def build_config_chain(root: Path, cli_overrides: dict[str, Any]) -> CqConfig:
    """Build config from chain: CLI → env → pyproject.toml → defaults."""
    base = load_typed_config(root)  # pyproject.toml [tool.cq]
    env_layer = load_typed_env_config()  # CQ_* environment variables
    merged = {**base, **env_layer, **cli_overrides}
    return CqConfig(**merged)
```

**Precedence (highest to lowest):**
1. CLI arguments (explicit flags like `--verbose 2`)
2. Environment variables (CQ_VERBOSE=2)
3. `.cq.toml` or `pyproject.toml` [tool.cq] section
4. Hard-coded defaults in `config_types.py`

**Config Structure:**
```python
class CqConfig(CqStruct, frozen=True):
    root: Path
    verbose: int
    format: OutputFormat
    artifact_dir: Path
    no_save_artifact: bool
    max_findings: int
    timeout: float | None
    parallel_workers: int
```

### Command Registration

Commands registered as sub-commands via `@app.command()`:

```python
# cli_app/commands/search.py
from cli_app.app import app

@app.command
def search(
    query: Annotated[str, Parameter(...)],
    *,
    regex: Annotated[bool, Parameter(...)] = False,
    include_strings: Annotated[bool, Parameter(...)] = False,
    in_: Annotated[str | None, Parameter(name="in")] = None,
    lang: Annotated[str | None, Parameter(...)] = None,
) -> int:
    """Smart search with classification and enrichment."""
    ...
```

**Registered Commands:**
- `q`: Declarative query DSL (entity queries, pattern queries)
- `search`: Smart search with classification
- `calls`: Call site census for function
- `impact`: Data flow/taint analysis for parameter
- `sig-impact`: Signature change impact analysis
- `scopes`: Scope/closure analysis
- `bytecode-surface`: Bytecode pattern analysis
- `side-effects`: Import-time side effects detection
- `imports`: Import structure analysis
- `exceptions`: Exception pattern analysis
- `run`: Multi-step workflow execution
- `chain`: Command chaining frontend
- `neighborhood` / `nb`: Semantic neighborhood assembly (anchor/symbol resolution + LSP slices)
- `ldmd`: LDMD progressive disclosure protocol (index, get, search, neighbors)

### Neighborhood Command Registration

**Location:** `cli_app/app.py:326-330`

```python
# Neighborhood command
from tools.cq.cli_app.commands.neighborhood import nb_app, neighborhood_app

app.command(neighborhood_app)
app.command(nb_app)
```

Two apps are registered for the same handler: `neighborhood` (full name) and `nb` (alias). Both route to the same `neighborhood()` function, allowing users to use the shorter `nb` form for quick invocations.

### LDMD Command Registration

**Location:** `cli_app/app.py:323-324`

```python
from tools.cq.cli_app.commands.ldmd import ldmd_app
app.command(ldmd_app)
```

Registers the `ldmd` subcommand group with sub-commands: `index`, `get`, `search`, `neighbors`. These commands implement progressive disclosure operations over LDMD-formatted artifacts, allowing tools like Claude to retrieve specific sections without loading entire documents.

### CliContext Injection

All commands receive `CliContext` with shared runtime state:

```python
# cli_app/context.py
class CliContext(CqStruct, frozen=True):
    root: Path
    toolchain: Toolchain
    argv: list[str]
    output_format: OutputFormat
    verbose: int
    artifact_dir: Path
    no_save_artifact: bool
```

Context constructed at app entry and passed to all commands.

### CliTextResult

**Location:** `cli_app/context.py:135-147`

Text payload for non-analysis protocol commands (e.g., LDMD index/get/search/neighbors):

```python
class CliTextResult(CqStruct, frozen=True):
    text: str
    media_type: str = "text/plain"
```

Used by protocol commands that return raw text or JSON rather than `CqResult`:

```python
# In result.py handle_result()
if not cli_result.is_cq_result:
    if isinstance(cli_result.result, CliTextResult):
        sys.stdout.write(f"{cli_result.result.text}\n")
    return cli_result.get_exit_code()
```

This enables LDMD commands to return structured text outputs without forcing them into the `CqResult` schema, maintaining clean separation between analysis results and protocol responses.

### Architectural Observations for Improvement Proposals

**Configuration Chain Coupling:**
- Three-layer config resolution (CLI → env → file) creates non-obvious precedence bugs
- `CqConfig` struct duplicates CLI parameter definitions, requiring manual sync
- Config validation scattered across multiple modules (validators.py, config.py, config_types.py)

**Improvement Vector:** Consider unifying config resolution into a single typed config builder with explicit precedence rules and automatic CLI parameter derivation from config schema.

**Command Registration Brittleness:**
- Commands manually import and decorate from `app` singleton
- No compile-time validation that command signatures match `CliContext` expectations
- Command discovery is implicit (decorator side effects at import time)

**Improvement Vector:** Consider explicit command registry with signature validation, or migrate to a more structured command framework with typed command handlers.

## Toolchain Detection

### Toolchain Struct

```python
# core/toolchain.py
class Toolchain(CqStruct, frozen=True):
    rg_available: bool
    rg_version: str | None
    sgpy_available: bool
    sgpy_version: str | None
    py_path: str
    py_version: str
```

### Detection Logic

Toolchain detected once at application startup:

```python
@staticmethod
def detect() -> Toolchain:
    """Detect available tools at runtime."""
    rg_available, rg_version = _probe_ripgrep()
    sgpy_available, sgpy_version = _probe_ast_grep()
    py_path = sys.executable
    py_version = platform.python_version()

    return Toolchain(
        rg_available=rg_available,
        rg_version=rg_version,
        sgpy_available=sgpy_available,
        sgpy_version=sgpy_version,
        py_path=py_path,
        py_version=py_version,
    )
```

**Probe Implementation:**
- `_probe_ripgrep()`: Runs `rg --version`, parses version string
- `_probe_ast_grep()`: Attempts `import ast_grep_py`, captures version if available
- Both probes fail gracefully (return `False, None` on failure)

**Toolchain Usage:**
- Injected into `CliContext` at startup
- Commands check availability before invoking external tools
- Missing tools degrade gracefully (e.g., search falls back to identifier mode if ripgrep unavailable)

### Architectural Observations for Improvement Proposals

**Toolchain Probing Overhead:**
- Toolchain detection runs unconditionally at every invocation
- Version parsing for rg involves subprocess spawn even for commands that don't use ripgrep
- No memoization or caching across invocations

**Improvement Vector:** Consider lazy toolchain detection (probe only when tool is actually needed) or persistent toolchain cache with invalidation on tool updates.

## Result Pipeline

### Core Schema

All commands produce `CqResult` as unified output contract:

```python
# core/schema.py
class CqResult(CqStruct, frozen=True):
    success: bool
    summary: str
    sections: list[Section]
    findings: list[Finding]
    meta: RunMeta
    detail_payload: DetailPayload | None = None
```

**Front-Door Insight Integration:**

Front-door commands (search, calls, entity) embed `FrontDoorInsightV1` in `summary["front_door_insight"]`, which is rendered as the first markdown block ("Insight Card"). The insight provides:
- Target grounding (symbol, kind, location, signature)
- Neighborhood preview slices (callers, callees, references, hierarchy/scope)
- Risk assessment with explicit drivers and counters
- Confidence scoring
- Degradation status per subsystem
- Budget tracking and artifact references

**Supporting Types:**

```python
class Section(CqStruct, frozen=True):
    title: str
    content: str
    level: int = 2

class Finding(CqStruct, frozen=True):
    path: str
    name: str
    kind: str
    span: SourceSpan
    context: str
    score_details: ScoreDetails | None = None
    anchors: list[Anchor]
    degrade_reasons: list[str]
    code_facts: dict[str, Any]  # Enrichment facts

class Anchor(CqStruct, frozen=True):
    label: str
    span: SourceSpan

class RunMeta(CqStruct, frozen=True):
    timestamp: str
    duration_ms: float
    command: str
    args: list[str]
    root: str
    total_findings: int
    toolchain: Toolchain

class DetailPayload(CqStruct, frozen=True):
    payload_type: str
    data: dict[str, Any]

class ScoreDetails(CqStruct, frozen=True):
    impact_score: float
    confidence_score: float
    breakdown: dict[str, float]
```

### Rendering Dispatch

All output routed through `render_result()` dispatcher:

```python
# cli_app/result.py
def render_result(result: CqResult, format: OutputFormat) -> str:
    """Dispatch result rendering based on output format."""
    renderers = {
        OutputFormat.JSON: lambda r: dumps_json(r),
        OutputFormat.MD: render_markdown,
        OutputFormat.SUMMARY: render_summary,
        OutputFormat.MERMAID: render_mermaid_flowchart,
        OutputFormat.MERMAID_CLASS: render_mermaid_class_diagram,
        OutputFormat.DOT: render_dot,
    }
    renderer = renderers.get(format)
    if not renderer:
        raise ValueError(f"Unknown output format: {format}")
    return renderer(result)
```

**Renderer Implementations:**
- `dumps_json`: msgspec JSON encoder with custom hooks for Path, datetime
- `render_markdown`: Default format (sections + findings table + metadata)
- `render_summary`: Condensed format (summary + counts only)
- `render_mermaid_flowchart`: Call graph visualization
- `render_mermaid_class_diagram`: Class hierarchy visualization
- `render_dot`: Graphviz DOT format for complex graphs
- `render_ldmd_from_cq_result`: LDMD progressive disclosure format (lazy import)

### LDMD Renderer Dispatch

**Location:** `cli_app/result.py:100-104`

LDMD uses a lazy import pattern separate from the main renderer dict:

```python
# LDMD format uses lazy import (implemented in R6)
if format_value == "ldmd":
    from tools.cq.ldmd.writer import render_ldmd_from_cq_result
    return render_ldmd_from_cq_result(result)
```

**Rationale:** Lazy import prevents circular dependencies and allows the LDMD subsystem (R6) to evolve independently of the core renderer dispatch. The LDMD writer depends on neighborhood infrastructure that may not be needed for standard CQ operations.

### Markdown Rendering Structure

Default markdown output follows structured template:

```markdown
# [Command Title]

## Insight Card (if present)

[FrontDoorInsightV1 rendering: target, confidence, risk, neighborhood preview, budget, artifacts]

[Summary paragraph]

## Section 1
[Section content]

## Section 2
[Section content]

## Findings

| Path | Name | Kind | Line | Score | Details |
|------|------|------|------|-------|---------|
| ... | ... | ... | ... | ... | ... |

## Metadata

- Command: [command]
- Root: [root path]
- Duration: [duration ms]
- Total Findings: [count]
- Toolchain: [versions]
```

**Insight Card Rendering (Front-Door Commands):**

When `summary["front_door_insight"]` is present (search, calls, entity), the markdown renderer emits a structured "## Insight Card" section as the first content block. The card includes:
- **Target**: Symbol, kind, location, signature (with confidence badge)
- **Risk**: Level (low/med/high) with explicit drivers and counters
- **Neighborhood Preview**: Preview slices for callers, callees, references, hierarchy/scope (with overflow artifact references)
- **Budget**: Token counts and limits
- **Artifacts**: Links to diagnostic and overflow artifact files

**Diagnostic Offloading:**

Large diagnostic payloads (enrichment_telemetry, pyrefly_telemetry, pyrefly_diagnostics, language_capabilities, cross_language_diagnostics) are offloaded to artifact files rather than emitted in-band. The markdown output shows compact status lines instead of verbose diagnostic blocks. Full diagnostics are available in `.cq/artifacts/*_diagnostics_*.json` files.

**Finding Table Columns:**
- Path (relative to root)
- Name (function/class/symbol name)
- Kind (definition, callsite, import, usage, etc.)
- Line (span.start_line)
- Impact/Confidence scores (if available)
- Degrade reasons (if enrichment failed)

### Artifact Persistence

Results optionally persisted as msgpack artifacts:

```python
# cli_app/result.py
def save_artifact(result: CqResult, artifact_dir: Path) -> Path:
    """Save result as msgpack artifact with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    cmd = result.meta.command.replace(" ", "_")
    filename = f"{cmd}_{timestamp}.msgpack"
    path = artifact_dir / filename
    path.parent.mkdir(parents=True, exist_ok=True)

    envelope = ContractEnvelope(
        version="0.3.0",
        schema="CqResult",
        data=result,
    )
    with path.open("wb") as f:
        f.write(msgspec.msgpack.encode(envelope))

    return path
```

**Artifact Structure:**
- Location: `.cq/artifacts/` by default
- Filename patterns:
  - Results: `{macro}_result_{timestamp}_{run_id}.json`
  - Diagnostics: `{macro}_diagnostics_{timestamp}_{run_id}.json`
  - Neighborhood overflow: `{macro}_neighborhood_overflow_{timestamp}_{run_id}.json`
- Format: JSON with structured payload (diagnostics, neighborhood slices, or full CqResult)
- Disabled via `--no-save-artifact` flag

**Artifact Types:**

1. **Result artifacts** (`save_artifact_json`): Full CqResult as JSON
2. **Diagnostic artifacts** (`save_diagnostics_artifact`): Offloaded diagnostic payloads (enrichment_telemetry, pyrefly_telemetry, pyrefly_diagnostics, language_capabilities, cross_language_diagnostics)
3. **Neighborhood overflow artifacts** (`save_neighborhood_overflow_artifact`): Full neighborhood slices when insight preview is truncated

Artifact references are included in `FrontDoorInsightV1.artifacts` and markdown "## Artifacts" sections for easy retrieval of offloaded data.

### Architectural Observations for Improvement Proposals

**Result Schema Rigidity:**
- `CqResult` is a monolithic struct containing all possible output types
- `DetailPayload` is a weakly-typed escape hatch (dict[str, Any])
- Findings list cannot represent nested hierarchies (e.g., class → methods → callsites)

**Improvement Vector:** Consider adopting a more flexible result schema with discriminated unions for different result types, or a hierarchical finding tree structure.

**Rendering Coupling:**
- Renderers tightly coupled to `CqResult` structure
- Adding new output formats requires modifying multiple renderer functions
- No extension points for custom renderers

**Improvement Vector:** Consider a renderer plugin system with explicit contracts, or a intermediate representation layer that decouples analysis results from output formatting.

## Multi-Language Orchestration

### Language Scope Model

CQ supports multi-language analysis with explicit scope control:

```python
# query/language.py
class LanguageScope(enum.Enum):
    PYTHON = "python"
    RUST = "rust"
    AUTO = "auto"  # Union of Python + Rust
```

**Extension Mapping:**
```python
LANGUAGE_EXTENSIONS = {
    LanguageScope.PYTHON: {".py", ".pyi"},
    LanguageScope.RUST: {".rs"},
}
```

**Scope Enforcement:**
- Extension-authoritative: file must match language's extension set
- `AUTO` scope: union of all language extension sets
- Scope filtering: candidates dropped if extension doesn't match (tracked in `dropped_by_scope` diagnostic)

### Orchestration Flow

Multi-language queries execute via partitioned execution and merge:

```python
# core/multilang_orchestrator.py
def execute_by_language_scope(
    scope: LanguageScope,
    callback: Callable[[LanguageScope], T],
) -> list[tuple[LanguageScope, T]]:
    """Execute callback once per language in scope."""
    if scope == LanguageScope.AUTO:
        langs = [LanguageScope.PYTHON, LanguageScope.RUST]
    else:
        langs = [scope]

    results = []
    for lang in langs:
        result = callback(lang)
        results.append((lang, result))

    return results
```

**Merge Strategy:**
```python
def merge_language_cq_results(
    partitioned_results: list[tuple[LanguageScope, CqResult]],
) -> CqResult:
    """Merge per-language results into unified result."""
    # Merge findings with language priority (Python first, Rust second)
    merged_findings = merge_partitioned_items(
        [(lang, r.findings) for lang, r in partitioned_results],
        key=lambda f: (f.path, f.name, f.span.start_byte),
    )

    # Merge sections (deduplicate by title)
    merged_sections = merge_sections([r.sections for _, r in partitioned_results])

    # Build multi-language summary
    summary = build_multilang_summary(partitioned_results)

    return CqResult(
        success=all(r.success for _, r in partitioned_results),
        summary=summary,
        sections=merged_sections,
        findings=merged_findings,
        meta=merge_meta([r.meta for _, r in partitioned_results]),
    )
```

**Merge Determinism:**
- Findings sorted by `(path, name, start_byte)` for stable ordering
- Language priority ordering: Python first, Rust second
- Duplicate findings deduplicated by span overlap detection

### Multi-Language Summary Building

```python
# core/multilang_summary.py
def build_multilang_summary(
    partitioned_results: list[tuple[LanguageScope, CqResult]],
) -> str:
    """Build summary paragraph covering all languages."""
    lang_summaries = []
    for lang, result in partitioned_results:
        count = result.meta.total_findings
        lang_summaries.append(f"{lang.value}: {count} findings")

    combined = ", ".join(lang_summaries)
    return f"Multi-language analysis results: {combined}"
```

### Architectural Observations for Improvement Proposals

**Language Scope Rigidity:**
- Only two languages supported (Python, Rust)
- Extension mapping is hard-coded, no plugin mechanism
- AUTO scope always includes all languages (no selective auto-detection)

**Improvement Vector:** Consider language detection via file content analysis (shebang, syntax probing) or a plugin system for adding new languages without modifying core orchestrator.

**Merge Strategy Limitations:**
- Merge priority is hard-coded (Python > Rust)
- No support for merge conflict resolution beyond "first wins"
- Duplicate detection via span overlap is O(n²) for large result sets

**Improvement Vector:** Consider configurable merge strategies, or interval tree-based duplicate detection for scalability.

## Error Handling Philosophy

CQ follows a fail-open architecture with graceful degradation tracking.

### Fail-Open Semantics

**Core Principle:** Individual component failures should not abort the entire analysis. Instead, failures are tracked and degraded results are returned.

**Implementation:**

```python
# search/python_enrichment.py
def _enrich_finding_safe(finding: Finding, stages: list[str]) -> Finding:
    """Enrich finding with fail-open semantics."""
    degrade_reasons = []
    enriched_facts = {}

    for stage in stages:
        try:
            facts = run_enrichment_stage(finding, stage)
            enriched_facts.update(facts)
        except Exception as e:
            degrade_reasons.append(f"{stage}: {e}")
            continue  # Continue to next stage

    return Finding(
        **{**finding.__dict__, "code_facts": enriched_facts, "degrade_reasons": degrade_reasons}
    )
```

### Degradation Tracking

Failures accumulated in `Finding.degrade_reasons`:

```python
class Finding(CqStruct, frozen=True):
    ...
    degrade_reasons: list[str]  # Stage-specific failure messages
```

**Degradation Sources:**
- Enrichment stage failures (parse errors, AST traversal exceptions)
- Classification failures (ambiguous patterns, timeout)
- External tool failures (ripgrep crash, ast-grep error)

**Degradation Reporting:**
- Included in markdown finding tables (separate column)
- Preserved in JSON/msgpack artifacts
- Surfaced in summary statistics

**Insight-Level Degradation:**

For front-door commands, `FrontDoorInsightV1` includes an `InsightDegradationV1` payload that tracks per-subsystem degradation status alongside the existing `Finding.degrade_reasons` list. This provides structured status tracking for:
- Enrichment subsystems (5-stage pipeline status)
- LSP integration (capability gate failures)
- Neighborhood assembly (structural/LSP slice availability)
- Pyrefly integration (type analysis failures)

### Error Result Pattern

Commands that encounter unrecoverable errors produce error results rather than raising exceptions:

```python
def cmd_calls(function: str, root: Path, ...) -> CqResult:
    """Find all call sites for function."""
    try:
        findings = find_all_call_sites(function, root)
        return CqResult(
            success=True,
            summary=f"Found {len(findings)} call sites",
            findings=findings,
            ...
        )
    except Exception as e:
        return CqResult(
            success=False,
            summary=f"Error finding call sites: {e}",
            findings=[],
            ...
        )
```

**Benefits:**
- Commands always return `CqResult`, simplifying caller contracts
- Errors are data, not control flow
- Multi-step workflows can continue after individual step failures

### Configurable Stop-On-Error

Multi-step workflows support configurable error handling:

```python
# run/runner.py
class RunPlan(CqStruct, frozen=True):
    steps: list[StepSpec]
    stop_on_error: bool = False  # Default: continue on error
```

**Behavior:**
- `stop_on_error=False`: All steps execute, errors accumulated in results
- `stop_on_error=True`: First error aborts workflow, returns error result

### Timeout Handling

Long-running operations (ripgrep, enrichment) support timeouts:

```python
# search/timeout.py
def with_timeout(func: Callable[..., T], timeout: float, default: T) -> T:
    """Execute function with timeout, return default on timeout."""
    try:
        return func_timeout(timeout, func)
    except FunctionTimedOut:
        return default
```

**Timeout Strategy:**
- Enrichment: per-finding timeout (default: 5s)
- Ripgrep: global timeout (default: 30s)
- Classification: per-finding timeout (default: 2s)
- Timeouts result in degraded findings rather than errors

### Architectural Observations for Improvement Proposals

**Degradation Tracking Opacity:**
- `degrade_reasons` is a flat list of strings, no structured error representation
- No error severity levels (warning vs critical failure)
- No error correlation across findings (same root cause appears N times)

**Improvement Vector:** Consider structured error types with severity, category, and correlation IDs. Aggregate related errors in summary rather than repeating per-finding.

**Timeout Granularity:**
- Timeouts are global or per-finding, no per-stage granularity
- No adaptive timeout based on repository size or complexity
- No timeout budget allocation across stages

**Improvement Vector:** Consider hierarchical timeout budgets (total → per-stage → per-finding) or adaptive timeouts based on repository characteristics.

**Error Result Uniformity:**
- Error results contain empty findings lists (loses partial progress)
- No distinction between "no results" and "error prevented results"
- Error messages in summary field overload success case semantics

**Improvement Vector:** Consider explicit error result variant with error details separate from summary, or partial result support (some findings collected before error).

## Serialization Infrastructure

### CqStruct Base Class

All data types derive from `CqStruct`:

```python
# core/structs.py
class CqStruct(msgspec.Struct, frozen=True, kw_only=True):
    """Base class for all CQ data structures."""
    pass
```

**Benefits:**
- Zero-copy serialization via msgspec
- Immutable by default (frozen=True)
- Keyword-only construction prevents positional argument errors
- Automatic equality, hashing, repr

### JSON Encoding

```python
# core/codec.py
def dumps_json(obj: Any) -> str:
    """Serialize object to JSON string."""
    encoder = msgspec.json.Encoder(enc_hook=_json_enc_hook)
    return encoder.encode(obj).decode("utf-8")

def _json_enc_hook(obj: Any) -> Any:
    """Custom encoder for non-JSON types."""
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, enum.Enum):
        return obj.value
    raise NotImplementedError(f"Cannot encode {type(obj)}")
```

### Msgpack Encoding

```python
# core/codec.py
def dumps_msgpack(obj: Any) -> bytes:
    """Serialize object to msgpack bytes."""
    encoder = msgspec.msgpack.Encoder(enc_hook=_msgpack_enc_hook)
    return encoder.encode(obj)

def _msgpack_enc_hook(obj: Any) -> Any:
    """Custom encoder for non-msgpack types."""
    if isinstance(obj, Path):
        return {"__type__": "Path", "value": str(obj)}
    if isinstance(obj, datetime):
        return {"__type__": "datetime", "value": obj.isoformat()}
    if isinstance(obj, enum.Enum):
        return {"__type__": "Enum", "value": obj.value}
    raise NotImplementedError(f"Cannot encode {type(obj)}")
```

### Contract Envelopes

Serialized artifacts wrapped in versioned contract envelope:

```python
# core/contracts.py
class ContractEnvelope(CqStruct, frozen=True):
    version: str  # Tool version
    schema: str   # Schema name (e.g., "CqResult")
    data: Any     # Actual payload
```

**Benefits:**
- Schema evolution tracking
- Version compatibility checks
- Type-safe deserialization

**Usage:**
```python
envelope = ContractEnvelope(
    version="0.3.0",
    schema="CqResult",
    data=result,
)
bytes = dumps_msgpack(envelope)
```

### Architectural Observations for Improvement Proposals

**Serialization Hook Fragility:**
- Custom encoders registered via enc_hook callbacks
- No compile-time verification that all types in schema are encodable
- msgpack round-trip requires symmetric dec_hook (not implemented)

**Improvement Vector:** Consider code generation for encoders/decoders from msgspec schema, or runtime schema validation at serialization boundaries.

**Contract Envelope Limitations:**
- No schema registry or validation
- No migration support for older versions
- Schema field is string, not enum or structured type

**Improvement Vector:** Consider JSON Schema or msgspec schema definitions for contracts, with automatic validation and migration support.

## Code Indexing Infrastructure

### DefIndex Architecture

Central index for function and class definitions:

```python
# index/def_index.py (676 lines)
class DefIndex:
    """Index of all function and class definitions in repository."""

    def __init__(self, root: Path, language_scope: LanguageScope):
        self.root = root
        self.language_scope = language_scope
        self._fn_decls: dict[str, list[FnDecl]] = {}
        self._class_decls: dict[str, list[ClassDecl]] = {}
        self._file_defs: dict[Path, FileDefIndex] = {}

    def build(self) -> None:
        """Scan repository and build index."""
        files = enumerate_files(self.root, self.language_scope)
        for file in files:
            self._index_file(file)

    def lookup_function(self, name: str) -> list[FnDecl]:
        """Find all function definitions by name."""
        return self._fn_decls.get(name, [])

    def lookup_class(self, name: str) -> list[ClassDecl]:
        """Find all class definitions by name."""
        return self._class_decls.get(name, [])
```

**Index Structures:**

```python
class FnDecl(CqStruct, frozen=True):
    name: str
    path: Path
    span: SourceSpan
    params: list[ParamDecl]
    return_type: str | None
    is_async: bool
    is_method: bool
    parent_class: str | None

class ClassDecl(CqStruct, frozen=True):
    name: str
    path: Path
    span: SourceSpan
    bases: list[str]
    methods: list[str]  # Method names
```

**Indexing Strategy:**
- Scans all files matching language scope
- Parses AST via ast-grep or Python ast module
- Extracts function/class signatures
- Builds name → declarations mapping

### Call Resolution

Resolves call expressions to function declarations:

```python
# index/call_resolver.py
class CallResolver:
    """Resolve call sites to function declarations."""

    def __init__(self, def_index: DefIndex):
        self.def_index = def_index

    def resolve_call(self, call: CallExpr, context: FileContext) -> list[FnDecl]:
        """Find candidate function declarations for call expression."""
        # Try local resolution first (same file)
        local_decls = context.lookup_local(call.name)
        if local_decls:
            return local_decls

        # Try import resolution
        import_decls = self._resolve_via_imports(call.name, context)
        if import_decls:
            return import_decls

        # Fall back to global name search
        return self.def_index.lookup_function(call.name)
```

**Resolution Strategies:**
1. Local resolution (same file definitions)
2. Import resolution (follow import chain)
3. Global name search (all definitions with matching name)

**Ambiguity Handling:**
- Returns list of candidate declarations
- Caller decides how to handle multiple candidates (e.g., report all)
- Confidence scoring based on resolution strategy

### Argument Binding

Binds call arguments to function parameters:

```python
# index/arg_binder.py
class ArgBinder:
    """Bind call arguments to function parameters."""

    def bind(self, call: CallExpr, fn_decl: FnDecl) -> dict[str, ArgBinding]:
        """Bind arguments to parameters."""
        bindings = {}

        # Bind positional arguments
        for i, arg in enumerate(call.positional_args):
            if i < len(fn_decl.params):
                param = fn_decl.params[i]
                bindings[param.name] = ArgBinding(
                    param=param,
                    arg=arg,
                    binding_type="positional",
                )

        # Bind keyword arguments
        for name, arg in call.keyword_args.items():
            param = fn_decl.lookup_param(name)
            if param:
                bindings[name] = ArgBinding(
                    param=param,
                    arg=arg,
                    binding_type="keyword",
                )

        return bindings
```

**Binding Types:**
- Positional: argument matched to parameter by position
- Keyword: argument matched to parameter by name
- Var-positional: *args binding
- Var-keyword: **kwargs binding

### File Enumeration

Gitignore-aware file enumeration:

```python
# index/files.py
def enumerate_files(root: Path, language_scope: LanguageScope) -> list[Path]:
    """Enumerate files matching language scope, respecting .gitignore."""
    gitignore = load_gitignore(root)
    extensions = LANGUAGE_EXTENSIONS[language_scope]

    files = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if gitignore.is_ignored(path):
            continue
        if path.suffix not in extensions:
            continue
        files.append(path)

    return files
```

**Gitignore Support:**
- Parses `.gitignore` files (top-level and nested)
- Applies glob patterns for ignore rules
- Respects negation rules (`!pattern`)

### Architectural Observations for Improvement Proposals

**DefIndex Scalability:**
- Full repository scan on every invocation (no persistent index)
- In-memory index structure (no disk cache)
- No incremental index updates (must rebuild on file changes)

**Improvement Vector:** Consider persistent index with file modification tracking, or lazy index building (only scan files relevant to query).

**Call Resolution Ambiguity:**
- Import resolution is shallow (doesn't follow re-exports)
- No type-based disambiguation (only name matching)
- Multiple candidates result in combinatorial explosion for transitive analysis

**Improvement Vector:** Consider type-aware resolution using stubs or runtime introspection, or confidence-based pruning of low-probability candidates.

**Argument Binding Limitations:**
- No handling of *args unpacking
- No handling of **kwargs unpacking
- No validation that call is well-formed (missing required params)

**Improvement Vector:** Consider full signature conformance checking with error reporting, or pattern-based argument analysis for unpacking expressions.

## Conclusion

The CQ core infrastructure provides a robust foundation for multi-language code analysis with fail-open semantics and extensible output formats. Recent additions (R0-R8) introduce semantic neighborhood assembly and progressive disclosure capabilities via LDMD format.

**Core Capabilities:**
1. Multi-language analysis (Python, Rust) with deterministic scope enforcement
2. Fail-open architecture with granular degradation tracking
3. Multiple output formats including visual diagrams and progressive disclosure
4. Semantic neighborhood assembly with LSP integration (R0-R4)
5. LDMD progressive disclosure protocol for large artifacts (R6)

The architecture exhibits several areas of coupling and rigidity that present opportunities for improvement:

1. **Configuration and CLI coupling**: Config resolution chain and command registration could be more declarative
2. **Result schema rigidity**: Monolithic CqResult struct limits extensibility (partially addressed by CliTextResult for protocol commands)
3. **Language scope limitations**: Hard-coded two-language support without plugin mechanism
4. **Index scalability**: Full rebuild on every invocation without persistent caching
5. **Error handling opacity**: Flat string-based degradation tracking without structured errors

These observations are documented not as deficiencies but as design decision points that could be revisited for specific use cases or scale requirements.

**File Locations:**
- Core schema: `tools/cq/core/schema.py`
- Front-door insight: `tools/cq/core/front_door_insight.py`
- Artifacts: `tools/cq/core/artifacts.py`
- Report rendering: `tools/cq/core/report.py`
- CLI app: `tools/cq/cli_app/app.py`
- Toolchain: `tools/cq/core/toolchain.py`
- Indexing: `tools/cq/index/def_index.py`
- Multi-language: `tools/cq/core/multilang_orchestrator.py`
- Serialization: `tools/cq/core/codec.py`, `tools/cq/core/contracts.py`
- Neighborhood: `tools/cq/neighborhood/bundle_builder.py`, `tools/cq/neighborhood/section_layout.py`
- LDMD: `tools/cq/ldmd/format.py`, `tools/cq/ldmd/writer.py`

**Related Documentation:**
- Neighborhood Architecture: [08_neighborhood_subsystem.md](08_neighborhood_subsystem.md)
- LDMD Format: [09_ldmd_format.md](09_ldmd_format.md)
