# 01 — CLI & Command Framework

**Version:** 0.4.0
**Scope:** `tools/cq/cli_app/` — Command-line interface, routing, configuration, output rendering
**Phase:** Phase 2 documentation (Command Interface Layer)

---

## Overview

The CLI & Command Framework provides the external interface to CQ's analysis capabilities. Built on cyclopts, it implements a meta-app launcher pattern with global options, command registration, config resolution, output format dispatch, and result action handling. This layer translates user commands into structured execution requests and renders analysis results in multiple output formats.

**Core Responsibilities:**

1. Command routing and parameter parsing via cyclopts
2. Three-layer configuration resolution (CLI → env → file)
3. Context injection for all commands (root, toolchain, verbosity)
4. Output format dispatch (markdown, JSON, mermaid, dot, LDMD)
5. Result filtering and artifact persistence
6. Telemetry collection (parse/execution timing)

**Technology Stack:**

- **cyclopts**: Command routing, parameter binding, help generation
- **msgspec**: Zero-copy serialization for structured options
- **Rich**: Console output rendering (with deterministic width control)
- **Polars**: Findings table filtering and rehydration

---

## Module Map

```
tools/cq/cli_app/                    (3,831 LOC total)
├── __init__.py                      (49 LOC) - Package re-exports
├── app.py                           (290 LOC) - Root App, launcher, command registration
├── config.py                        (35 LOC) - Config provider chain (TOML, env)
├── context.py                       (189 LOC) - CliContext, CliResult, CliTextResult
├── decorators.py                    (42 LOC) - Context injection decorators
├── dispatch.py                      (42 LOC) - Dispatch pipeline (async support)
├── groups.py                        (44 LOC) - Cyclopts command groups
├── options.py                       (127 LOC) - Typed option structs
├── params.py                        (280 LOC) - Parameter groups
├── result.py                        (381 LOC) - Result rendering dispatch, artifact persistence
├── result_action.py                 (81 LOC) - Cyclopts result action handlers
├── step_types.py                    (151 LOC) - Run step type definitions
├── telemetry.py                     (156 LOC) - Telemetry capture and error classification
├── types.py                         (288 LOC) - Output format enums, custom converters
├── validators.py                    (97 LOC) - Input validators
├── completion.py                    (69 LOC) - Shell completion scripts
└── commands/                        (1,510 LOC)
    ├── __init__.py                  (7 LOC)
    ├── admin.py                     (107 LOC) - Deprecated admin commands
    ├── analysis.py                  (325 LOC) - Analysis macros
    ├── artifact.py                  (160 LOC) - Artifact protocol commands
    ├── chain.py                     (47 LOC) - Command chaining frontend
    ├── ldmd.py                      (311 LOC) - LDMD protocol
    ├── neighborhood.py              (145 LOC) - Neighborhood assembly command
    ├── query.py                     (118 LOC) - Query command entry
    ├── repl.py                      (78 LOC) - Interactive REPL mode
    ├── report.py                    (79 LOC) - Unified report command
    ├── run.py                       (53 LOC) - Multi-step execution
    └── search.py                    (80 LOC) - Smart search command
```

---

## Cyclopts Meta-App Architecture

### Root App Configuration

**Location:** `cli_app/app.py:156-173`

```python
app = App(
    name="cq",
    help="Code Query - High-signal code analysis macros",
    version=VERSION,
    help_format="rich",
    name_transform=lambda s: s.replace("_", "-"),
    default_parameter=Parameter(show_default=True, show_env_var=False),
    result_action=CQ_DEFAULT_RESULT_ACTION,
    config=build_config_chain(),
    console=console,
    error_console=error_console,
    exit_on_error=False,
)
```

**Key Properties:**

- `help_format="rich"`: Rich-formatted help text with syntax highlighting
- `name_transform`: Automatic hyphenation (`side_effects` → `side-effects`)
- `result_action`: Custom result action pipeline for CliResult handling
- `config`: Layered config providers (env + TOML)
- `exit_on_error=False`: Commands return exit codes instead of calling `sys.exit()`

### Launcher Pattern

**Location:** `cli_app/app.py:206-226`

```python
@app.meta.default(validator=validate_launcher_invariants)
def launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    global_opts: GlobalOptionArgs | None = None,
    config_opts: ConfigOptionArgs | None = None,
) -> int:
    """Handle global options and dispatch the selected command."""
    resolved_global_opts = global_opts if global_opts is not None else GlobalOptionArgs()
    resolved_config_opts = config_opts if config_opts is not None else ConfigOptionArgs()
    launch = _build_launch_context(argv=sys.argv[1:], config_opts=resolved_config_opts, global_opts=resolved_global_opts)
    ctx = _build_cli_context(launch)
    exit_code, _event = invoke_with_telemetry(app, list(tokens), ctx=ctx)
    return exit_code
```

**Execution Flow:**

1. Parse global options (`--root`, `--verbose`, `--format`, etc.)
2. Build `LaunchContext` from config chain
3. Construct `CliContext` (resolve root, detect toolchain)
4. Invoke command via telemetry wrapper
5. Return exit code to shell

---

## Config Resolution Chain

### Three-Layer Precedence

**Location:** `cli_app/config.py:9-30`

Configuration resolved via layered providers with strict precedence ordering:

**Precedence Order (highest to lowest):**

1. **CLI flags**: Explicit arguments like `--verbose 2`
2. **Environment variables**: `CQ_VERBOSE=2`
3. **Config file**: `.cq.toml` or `pyproject.toml` `[tool.cq]` section
4. **Defaults**: Hard-coded defaults in parameter definitions

**Environment Variable Mapping:**

- `CQ_ROOT` → `--root`
- `CQ_FORMAT` → `--format`
- `CQ_VERBOSE` → `--verbose`
- `CQ_ARTIFACT_DIR` → `--artifact-dir`
- `CQ_SAVE_ARTIFACT` → `--save-artifact`

**Config File Format:**

```toml
# .cq.toml or pyproject.toml
[tool.cq]
verbose = 1
format = "md"
artifact_dir = ".cq/artifacts"
save_artifact = true
```

### Global Options

**Location:** `cli_app/app.py:72-123`

Core global option fields (see source for full parameter annotations):

- `root`: Repository root path (default: auto-detect via git)
- `verbose`: Verbosity level (count flag: `-vv` → 2)
- `output_format`: Output format (md, json, both, summary, mermaid, mermaid-class, dot, ldmd)
- `artifact_dir`: Custom artifact directory (default: `.cq/artifacts`)
- `save_artifact`: Toggle artifact persistence

---

## Dispatch Pipeline

### Command Binding and Dispatch

**Location:** `cli_app/dispatch.py:12-35`

```python
def dispatch_bound_command(command: Callable[..., Any], bound: BoundArguments) -> Any:
    """Execute a parsed command using only public Python/Cyclopts primitives."""
    result = command(*bound.args, **bound.kwargs)
    if not inspect.isawaitable(result):
        return result

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        awaitable: Awaitable[Any] = result
        return asyncio.run(_await_result(awaitable))

    # Prevent un-awaited coroutine warnings when sync dispatch is used in a running loop.
    if inspect.iscoroutine(result):
        result.close()
    msg = "Awaitable command dispatched from sync path while an event loop is running."
    raise RuntimeError(msg)
```

**Dispatch Mechanics:**

1. Accept parsed command and bound arguments
2. Invoke command with `*args, **kwargs`
3. If result is awaitable and no loop is running, create event loop via `asyncio.run()`
4. If result is awaitable and loop is running, raise error (prevents nested loop creation)
5. Return command result for result action processing

**Async Support:**

CQ commands can be async functions. The dispatcher automatically detects awaitables and runs them in a new event loop if needed.

---

## Telemetry Pipeline

### Invoke Wrapper

**Location:** `cli_app/telemetry.py:58-153`

```python
@dataclass(frozen=True, slots=True)
class CqInvokeEvent:
    """Structured CQ invocation telemetry payload."""
    ok: bool
    command: str | None
    parse_ms: float
    exec_ms: float
    exit_code: int
    error_class: str | None = None
    error_stage: str | None = None
    event_id: str | None = None
    event_uuid_version: int | None = None
    event_created_ms: int | None = None
```

**Telemetry Stages:**

1. **Parse**: Cyclopts parsing (parameter binding, validation)
2. **Execute**: Command execution
3. **Error Classification**: Categorize errors (command_resolve, binding, coercion, validation, execution)

**Error Stage Classification:**

- `command_resolve`: Unknown command error
- `binding`: Missing/repeat arguments
- `coercion`: Type conversion failure
- `validation`: Validator rejection
- `execution`: Runtime exception

**Event Identity:**

Each invocation receives a unique run ID via `resolve_run_identity_contract()` which provides:
- `run_id`: Deterministic UUID (UUIDv7 with timestamp + random components)
- `run_uuid_version`: UUID version (7)
- `run_created_ms`: Timestamp in milliseconds

---

## Context Pipeline

### CliContext Construction

**Location:** `cli_app/context.py:55-129`

```python
class CliContext(CqStruct, frozen=True):
    """Runtime context for CLI commands."""
    argv: list[str]
    root: Path
    toolchain: Toolchain
    verbose: int = 0
    output_format: OutputFormat | None = None
    artifact_dir: Path | None = None
    save_artifact: bool = True

    @classmethod
    def build(cls, argv: list[str], options: CliContextOptions | None = None, **kwargs: object) -> CliContext:
        """Build a CLI context from arguments."""
        # ... root resolution and toolchain detection
        return cls(argv=argv, root=root, toolchain=toolchain, ...)
```

**Root Resolution:**

1. If `--root` provided, use that path
2. Else auto-detect via `resolve_repo_context()` which searches for `.git` directory upward
3. Resolve to absolute path

**Toolchain Detection:**

Probes for external tools at startup:
- **ripgrep**: Run `rg --version`, parse version string
- **ast-grep-py**: Attempt `import ast_grep_py`, capture version
- **Python**: `sys.executable` and `platform.python_version()`

Toolchain probing fails gracefully; missing tools result in `available=False` flags.

### Context Injection

**Location:** `cli_app/decorators.py:12-39`

Commands receive `CliContext` via injection. The telemetry wrapper checks for `"ctx"` in `ignored` parameters and injects context into `bound.arguments["ctx"]`.

**Usage in Commands:**

```python
@app.command
def search(query: str, *, ctx: CliContext) -> CliResult:
    root = ctx.root
    toolchain = ctx.toolchain
    ...
```

---

## CliResult and CliTextResult

### CliResult Wrapper

**Location:** `cli_app/context.py:150-189`

```python
class CliResult(CqStruct, frozen=True):
    """Result from a CLI command execution."""
    result: Any
    context: CliContext
    exit_code: int | None = None
    filters: FilterConfig | None = None
```

**CliResult Contract:**

- `result`: Analysis result (`CqResult`) or exit code (`int`)
- `context`: CLI context used for execution
- `exit_code`: Explicit exit code override
- `filters`: Filter configuration for result post-processing

### CliTextResult for Protocol Commands

**Location:** `cli_app/context.py:135-147`

```python
class CliTextResult(CqStruct, frozen=True):
    """Text payload for non-analysis protocol commands."""
    text: str
    media_type: str = "text/plain"
```

Protocol commands (LDMD, artifact) that return raw text or JSON use `CliTextResult` instead of `CqResult`.

---

## Result Action Pipeline

### Default Result Action

**Location:** `cli_app/result_action.py:58-74`

```python
def cq_result_action(result: Any) -> int:
    """Normalize CQ command return values to process exit codes."""
    if isinstance(result, CliResult):
        return handle_result(result, result.filters or FilterConfig())
    if isinstance(result, int):
        return result
    return 0
```

**Handle Result Pipeline:**

**Location:** `cli_app/result.py:143-197`

**Processing Stages:**

1. **Non-CqResult handling**: Handle `CliTextResult` or int exit codes
2. **Filter application**: Apply file/impact/confidence/severity filters
3. **Artifact persistence**: Save results + diagnostics + overflow artifacts
4. **Rendering**: Dispatch to format-specific renderer
5. **Output emission**: Write to console (verbatim for JSON/LDMD, Rich-wrapped for MD)

---

## Output Rendering Dispatch

### Format Routing

**Location:** `cli_app/result.py:99-140`

**Output Formats:**

| Format | Renderer | Use Case |
|--------|----------|----------|
| `md` | `render_markdown()` | Default format for Claude context |
| `json` | `dumps_json()` | Machine-readable output |
| `both` | Both renderers | Combined markdown + JSON |
| `summary` | `render_summary()` | Condensed single-line output for CI |
| `mermaid` | `render_mermaid_flowchart()` | Call graph visualization |
| `mermaid-class` | `render_mermaid_class_diagram()` | Class hierarchy visualization |
| `dot` | `render_dot()` | Graphviz DOT format |
| `ldmd` | `render_ldmd_from_cq_result()` | LLM-friendly progressive disclosure markdown |

**Lazy Import for LDMD:**

LDMD uses a lazy import to prevent circular dependencies and allow the LDMD subsystem to evolve independently.

### Markdown Renderer

**Location:** `tools/cq/core/report.py:1597-1677`

The markdown renderer applies render-time enrichment for findings missing context snippets, offloads large diagnostic payloads to artifacts, and reorders sections per command for optimal presentation.

**Markdown Structure:**

```markdown
# cq {macro}

## Insight Card (if present)
[FrontDoorInsightV1 rendering: target, confidence, risk, neighborhood preview]

## Code Overview
- Query: `{query}`
- Mode: `{mode}`
- Language Scope: `{lang_scope}`
- Top Symbols: `symbol1, symbol2, ...`
- Top Files: `file1, file2, ...`
- Match Categories: `category:count, ...`

## Key Findings
- [impact:high] [conf:high] Finding message (anchor)

### Section Title
- Finding in section
  Code Facts:
  - Identity: ...
  - Scope: ...

## Evidence
- Additional evidence findings

## Artifacts
- `path/to/artifact.json` (json)

## Summary
- {compact summary payload}

---
_Completed in {elapsed}ms | Schema: {version}_
```

**Render-Time Enrichment:**

For findings missing context snippets or enrichment facts, the renderer performs lazy enrichment:

1. Select target files (top 9 files by finding priority)
2. Build enrichment tasks for findings in target files
3. Run enrichment in parallel (up to 4 workers) via `WorkerScheduler`
4. Cache enrichment results by `(file, line, col, language)` key
5. Merge enrichment details into findings before rendering

**Compact Diagnostics:**

Large diagnostic payloads (enrichment telemetry, LSP diagnostics, etc.) are offloaded to artifacts. Compact status lines appear in markdown output. Full payloads are saved in `.cq/artifacts/*_diagnostics_*.json` files.

---

## Findings Table Filtering

### Flatten/Filter/Rehydrate Pipeline

**Location:** `tools/cq/core/findings_table.py`

Findings are flattened into a Polars DataFrame for filtering, then rehydrated back into `CqResult` structure.

**FindingRecord Schema:**

```python
@dataclass
class FindingRecord:
    macro: str
    group: str  # "key_findings", "evidence", or section title
    category: str
    message: str
    file: str | None
    line: int | None
    col: int | None
    impact_score: float
    impact_bucket: str
    confidence_score: float
    confidence_bucket: str
    evidence_kind: str
    severity: str
    details: dict[str, Any]
    _section_title: str | None
    _section_idx: int | None
    _finding_idx: int | None
```

**Filtering with Polars:**

Filters applied via Polars DataFrame operations:

- **Impact bucket filter**: `pl.col("impact_bucket").is_in(buckets)`
- **Confidence bucket filter**: `pl.col("confidence_bucket").is_in(buckets)`
- **Severity filter**: `pl.col("severity").is_in(levels)`
- **Include file patterns**: OR logic (match any pattern)
- **Exclude file patterns**: AND logic (exclude if matches any pattern)
- **Limit**: `df.head(options.limit)`

**Pattern Matching:**

```python
def _match_pattern(value: str | None, pattern: str) -> bool:
    """Match a value against a glob or regex pattern."""
    if value is None:
        return False
    # Regex pattern (starts with ~)
    if pattern.startswith("~"):
        return bool(re.search(pattern[1:], value))
    # Glob pattern
    return fnmatch.fnmatch(value, pattern)
```

**Filter Examples:**

```bash
# Include only files in src/ directory
cq search build_graph --include "src/**/*.py"

# High-impact findings only
cq calls build_graph --impact high

# Combine filters
cq search Config --include "src/**/*.py" --exclude "src/deprecated/**" --impact high --limit 20
```

---

## FrontDoor Insight Rendering

### Insight Card in Markdown Output

**Location:** `tools/cq/core/report.py:1360-1381`

Front-door commands (search, calls, entity) embed `FrontDoorInsightV1` in `summary["front_door_insight"]`, which is rendered as the first markdown block after the title.

**Insight Card Structure:**

- **Target**: Symbol, kind, location, signature (with confidence badge)
- **Risk**: Level (low/med/high) with explicit drivers and counters
- **Neighborhood Preview**: Preview slices for callers, callees, references, hierarchy/scope
- **Budget**: Token counts and limits
- **Artifacts**: Links to diagnostic and overflow artifact files

**Cross-Reference:**

For the FrontDoor Insight contract and assembly logic, see:
- **Doc 06**: Front-Door Subsystem Architecture
- `tools/cq/core/front_door_insight.py`: Insight builder and renderer

---

## Command Registration Map

### Core Analysis Commands

**Location:** `cli_app/app.py:229-260`

| Command | Module | Description |
|---------|--------|-------------|
| `search` | `commands.search:search` | Smart search with classification and enrichment |
| `q` | `commands.query:q` | Declarative query DSL |
| `calls` | `commands.analysis:calls` | Call site census for function |
| `impact` | `commands.analysis:impact` | Data flow/taint analysis for parameter |
| `sig-impact` | `commands.analysis:sig_impact` | Signature change impact analysis |
| `imports` | `commands.analysis:imports` | Import structure analysis |
| `exceptions` | `commands.analysis:exceptions` | Exception pattern analysis |
| `side-effects` | `commands.analysis:side_effects` | Import-time side effects detection |
| `scopes` | `commands.analysis:scopes` | Scope/closure analysis |
| `bytecode-surface` | `commands.analysis:bytecode_surface` | Bytecode pattern analysis |
| `report` | `commands.report:report` | Unified report command |
| `run` | `commands.run:run` | Multi-step workflow execution |
| `chain` | `commands.chain:chain` | Command chaining frontend |
| `neighborhood` / `nb` | `commands.neighborhood:neighborhood` | Semantic neighborhood assembly |

### Protocol Commands

**Location:** `cli_app/app.py:262-275`

| Command | Module | Description |
|---------|--------|-------------|
| `ldmd` | `commands.ldmd:ldmd_app` | LDMD progressive disclosure protocol |
| `artifact` | `commands.artifact:artifact_app` | Artifact protocol (list, get, purge) |

**LDMD Sub-Commands:**

- `ldmd index <file>`: Build LDMD section index
- `ldmd get <file> --id <section_id>`: Extract section
- `ldmd search <file> --query <text>`: Search section titles/content
- `ldmd neighbors <file> --id <section_id>`: Get sibling/child sections

**Artifact Sub-Commands:**

- `artifact list`: List saved artifacts
- `artifact get <id>`: Load artifact by ID
- `artifact purge --before <timestamp>`: Delete old artifacts

### Registration Pattern

Commands registered via `app.command()` with module path strings (lazy import):

```python
app.command("tools.cq.cli_app.commands.search:search", group=analysis_group)
app.command("tools.cq.cli_app.commands.query:q", name="q", group=analysis_group)
```

**Benefits:**

- Lazy loading: Commands not loaded until invoked
- Name transformation: `sig_impact` → `sig-impact`
- Group organization: Commands grouped in help output
- Alias support: `neighborhood` has alias `nb`

---

## REPL Command

### Interactive Mode

**Location:** `cli_app/commands/repl.py:25-78`

**REPL Features:**

- **Interactive prompt**: `cq> ` prompt for command input
- **Shell-like parsing**: Uses `shlex.split()` for proper quoting
- **Built-in commands**: `help`, `exit`, `quit`
- **Persistent context**: Same `CliContext` across all commands
- **Error handling**: Non-zero exit codes reported but don't terminate REPL
- **Telemetry**: All commands wrapped with telemetry collection

---

## Error Handling and Exit Codes

### Exit Code Semantics

| Exit Code | Meaning | Source |
|-----------|---------|--------|
| 0 | Success | Command completed successfully |
| 1 | Runtime error | Exception during command execution |
| 2 | Parse/validation error | Cyclopts parse error, missing arguments, validation failure |

### Error Classification

**Location:** `cli_app/telemetry.py:45-55`

**Error Stages:**

- **command_resolve**: Unknown command (e.g., `cq foobar`)
- **binding**: Parameter binding errors (missing required arguments, repeated arguments)
- **coercion**: Type conversion failures (e.g., `--limit abc`)
- **validation**: Validator rejections (e.g., `--limit -5`)
- **execution**: Runtime exceptions during command execution

---

## Validators

### Input Validation Helpers

**Location:** `cli_app/validators.py`

**Built-In Cyclopts Validators:**

- `validators.Path`: Path existence/type checks
- `validators.Number`: Numeric range checks
- `validators.LimitedChoice`: Min/max choice count
- `validators.mutually_exclusive`: Mutually exclusive options

---

## Shell Completion

### Completion Script Generation

**Location:** `cli_app/completion.py:12-62`

**Supported Shells:**

- `bash`
- `zsh`
- `fish`

**Installation:**

```bash
# Generate completion scripts
cq --install-completion
```

**Completion Features:**

- Command name completion
- Option name completion
- Enum value completion (output formats, severity levels, etc.)
- File path completion for file arguments

---

## Artifact Persistence

### Artifact Types

**Location:** `cli_app/result.py:247-309`

**Artifact Categories:**

1. **Result artifacts**: Full `CqResult` as JSON (for all macros except search)
2. **Diagnostic artifacts**: Offloaded diagnostic payloads (enrichment telemetry, LSP diagnostics, etc.)
3. **Neighborhood overflow artifacts**: Full neighborhood slices when insight preview is truncated
4. **Search artifact bundles**: Structured search result cache (for search macro only)

**SearchArtifactBundleV1 Contract:**

```python
class SearchArtifactBundleV1(CqStruct, frozen=True):
    run_id: str
    query: str
    macro: str
    summary: dict[str, object]
    object_summaries: list[ObjectSummaryV1]
    occurrences: list[OccurrenceV1]
    diagnostics: dict[str, object]
    snippets: dict[str, str]
    created_ms: int | float
```

This structured bundle allows search results to be cached and reloaded without re-running analysis.

**Artifact Path Structure:**

```
.cq/artifacts/
├── {macro}_result_{timestamp}_{run_id}.json
├── {macro}_diagnostics_{timestamp}_{run_id}.json
└── {macro}_neighborhood_overflow_{timestamp}_{run_id}.json
```

**Artifact Metadata:**

```python
class Artifact(CqStruct, frozen=True):
    path: str
    format: str  # "json", "msgpack", "ldmd"
    kind: str    # "result", "diagnostics", "neighborhood_overflow", "search_bundle"
```

---

## Architectural Observations

### CLI Configuration Coupling

**Issue:**

Three-layer config resolution (CLI → env → file) creates non-obvious precedence bugs. `GlobalOptionArgs` dataclass duplicates CLI parameter definitions, requiring manual sync. Config validation scattered across multiple modules.

**Improvement Vector:**

Consider unifying config resolution into a single typed config builder with explicit precedence rules and automatic CLI parameter derivation from config schema.

### Command Registration Brittleness

**Issue:**

Commands manually import and register via `app.command()` decorator side effects. No compile-time validation that command signatures match `CliContext` expectations.

**Improvement Vector:**

Consider explicit command registry with signature validation, or migrate to a more structured command framework with typed command handlers.

### Toolchain Probing Overhead

**Issue:**

Toolchain detection runs unconditionally at every invocation. Version parsing for rg involves subprocess spawn even for commands that don't use ripgrep.

**Improvement Vector:**

Consider lazy toolchain detection (probe only when tool is actually needed) or persistent toolchain cache with invalidation on tool updates.

### Result Schema Rigidity

**Issue:**

`CqResult` is a monolithic struct containing all possible output types. `DetailPayload` is a weakly-typed escape hatch. Findings list cannot represent nested hierarchies.

**Improvement Vector:**

Consider adopting a more flexible result schema with discriminated unions for different result types, or a hierarchical finding tree structure.

### Rendering Coupling

**Issue:**

Renderers tightly coupled to `CqResult` structure. Adding new output formats requires modifying multiple renderer functions. No extension points for custom renderers.

**Improvement Vector:**

Consider a renderer plugin system with explicit contracts, or an intermediate representation layer that decouples analysis results from output formatting.

---

## Related Documentation

- **Doc 02**: Smart Search Subsystem (search command internals)
- **Doc 03**: Query DSL Subsystem (query command internals)
- **Doc 04**: Analysis Macros (calls, impact, sig-impact command internals)
- **Doc 05**: Multi-Step Execution (run command internals)
- **Doc 06**: Front-Door Subsystem (FrontDoorInsightV1 contract and assembly)
- **Doc 08**: Neighborhood Subsystem (neighborhood command internals)
- **Doc 09**: LDMD Format (LDMD rendering and protocol commands)
- **Doc 10**: Runtime Services (caching, worker scheduling, service layer)

---

## File Locations

- **Root App**: `tools/cq/cli_app/app.py` (290 LOC)
- **Config Chain**: `tools/cq/cli_app/config.py` (35 LOC)
- **Context Types**: `tools/cq/cli_app/context.py` (189 LOC)
- **Dispatch Pipeline**: `tools/cq/cli_app/dispatch.py` (42 LOC)
- **Telemetry**: `tools/cq/cli_app/telemetry.py` (156 LOC)
- **Result Rendering**: `tools/cq/cli_app/result.py` (381 LOC)
- **Result Actions**: `tools/cq/cli_app/result_action.py` (81 LOC)
- **Output Formats**: `tools/cq/cli_app/types.py` (288 LOC)
- **Validators**: `tools/cq/cli_app/validators.py` (97 LOC)
- **Markdown Renderer**: `tools/cq/core/report.py` (1,774 LOC)
- **Findings Table**: `tools/cq/core/findings_table.py` (494 LOC)
- **Commands Directory**: `tools/cq/cli_app/commands/` (1,510 LOC)

---

**End of Document**
