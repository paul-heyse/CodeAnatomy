# 01 — CLI & Command Framework

**Version:** 0.5.1
**Scope:** `tools/cq/cli_app/` — Command-line interface, routing, configuration, output rendering
**Phase:** Phase 2 documentation (Command Interface Layer)
**Last Updated:** 2026-02-15 — Cyclopts-based CLI with unified request/result pipeline

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
tools/cq/cli_app/                    (3,722 LOC total)
├── __init__.py                      (49 LOC) - Package re-exports, main() entry
├── app.py                           (290 LOC) - Root App, launcher, command registration
├── context.py                       (189 LOC) - CliContext, CliResult, CliTextResult
├── infrastructure.py                (154 LOC) - Config chain, decorators, dispatch, groups
├── params.py                        (314 LOC) - Parameter groups for cyclopts
├── result.py                        (381 LOC) - Result rendering dispatch, artifact persistence
├── types.py                         (288 LOC) - Output format enums, custom converters
├── telemetry.py                     (156 LOC) - Telemetry capture and error classification
├── options.py                       (127 LOC) - Typed option structs
├── protocol_output.py               (111 LOC) - Text/JSON helpers for protocol commands
├── validators.py                    (97 LOC) - Input validators
├── result_action.py                 (81 LOC) - Cyclopts result action handlers
├── completion.py                    (69 LOC) - Shell completion scripts
├── contracts.py                     (5 LOC) - Placeholder for future CLI-specific contracts
└── commands/                        (1,357 LOC)
    ├── __init__.py                  (7 LOC)
    ├── analysis.py                  (304 LOC) - 8 analysis commands
    ├── ldmd.py                      (274 LOC) - LDMD protocol sub-app
    ├── neighborhood.py              (145 LOC) - Neighborhood assembly command
    ├── artifact.py                  (140 LOC) - Artifact protocol sub-app
    ├── query.py                     (121 LOC) - Query command entry
    ├── search.py                    (85 LOC) - Smart search command
    ├── admin.py                     (80 LOC) - Deprecated admin commands
    ├── report.py                    (78 LOC) - Unified report command
    ├── repl.py                      (77 LOC) - Interactive REPL mode
    ├── run.py                       (53 LOC) - Multi-step execution launcher
    └── chain.py                     (47 LOC) - Command chaining frontend
```

---

## Cyclopts Meta-App Architecture

### Root App Configuration

**Location:** `cli_app/app.py:156-172`

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
- `version="0.4.0"`: CQ version string

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

**LaunchContext Structure:**

**Location:** `cli_app/app.py:123-143`

```python
@dataclass(frozen=True, slots=True)
class LaunchContext:
    """Resolved launch-time parameters."""
    argv: list[str]
    root: Path | None
    verbose: int
    output_format: OutputFormat | None
    artifact_dir: Path | None
    save_artifact: bool
    config_file: str | None
    use_config: bool
```

---

## Config Resolution Chain

### Three-Layer Precedence

**Location:** `cli_app/infrastructure.py:20-56`

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

**Config Chain Builder:**

**Location:** `cli_app/infrastructure.py:20-56`

```python
def build_config_chain(
    config_file: str | None = None,
    *,
    use_config: bool = True,
) -> list[Any]:
    """Build Cyclopts config providers with `CLI > env > config > defaults` precedence."""
    from cyclopts.config import Env, Toml

    providers: list[Any] = []

    # Always include environment provider (CQ_* prefix)
    providers.append(Env(prefix="CQ_"))

    # Conditionally add config file provider
    if use_config:
        toml_path = config_file or "pyproject.toml"
        providers.append(Toml(toml_path, root_keys=("tool", "cq")))

    return providers
```

### Global Options

**Location:** `cli_app/app.py:46-118`

**ConfigOptionArgs:**

```python
@dataclass(frozen=True, slots=True)
class ConfigOptionArgs:
    """Parsed CLI config options."""
    config: str | None = None      # --config path/to/config.toml
    use_config: bool = True        # --no-config to disable
```

**GlobalOptionArgs:**

```python
@dataclass(frozen=True, slots=True)
class GlobalOptionArgs:
    """Parsed CLI global options."""
    root: Path | None = None
    verbose: int = 0
    output_format: OutputFormat | None = None
    artifact_dir: Path | None = None
    save_artifact: bool = True
```

Core global option fields with cyclopts parameter annotations:

- `root`: Repository root path (default: auto-detect via git)
- `verbose`: Verbosity level (count flag: `-vv` → 2)
- `output_format`: Output format (md, json, both, summary, mermaid, mermaid-class, dot, ldmd)
- `artifact_dir`: Custom artifact directory (default: `.cq/artifacts`)
- `save_artifact`: Toggle artifact persistence

---

## Dispatch Pipeline

### Command Binding and Dispatch

**Location:** `cli_app/infrastructure.py:110-133`

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

**Invocation Wrapper:**

**Location:** `cli_app/telemetry.py:90-153`

```python
def invoke_with_telemetry(
    app: App,
    tokens: list[str] | None,
    *,
    ctx: CliContext,
) -> tuple[int, CqInvokeEvent]:
    """Invoke a command with parse/exec timing and error classification."""
    # Parse phase: app.parse_args(tokens)
    # Context injection: bound.arguments["ctx"] = ctx
    # Execute phase: dispatch_bound_command(command, bound)
    # Error handling: classify by stage, build telemetry event
    # Return: (exit_code, event)
```

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

**Location:** `cli_app/infrastructure.py:59-86`

Commands receive `CliContext` via injection. The telemetry wrapper checks for `"ctx"` in function signature and injects context into `bound.arguments["ctx"]`.

**Context Injection Decorator:**

```python
def require_ctx(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator asserting ctx kwarg is a CliContext instance."""
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        ctx = kwargs.get("ctx")
        if ctx is not None and not isinstance(ctx, CliContext):
            msg = f"Expected CliContext for 'ctx', got {type(ctx).__name__}"
            raise TypeError(msg)
        return func(*args, **kwargs)
    return wrapper
```

**Helper Function:**

```python
def require_context(ctx: CliContext | None) -> CliContext:
    """Extract non-optional context or raise."""
    if ctx is None:
        msg = "CliContext is required but was None"
        raise RuntimeError(msg)
    return ctx
```

**Usage in Commands:**

```python
@app.command
@require_ctx
def search(query: str, *, ctx: CliContext) -> CliResult:
    ctx = require_context(ctx)
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

    @property
    def is_cq_result(self) -> bool:
        """Check if result is a CqResult."""
        from tools.cq.core.structs import CqResult
        return isinstance(self.result, CqResult)
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

## Parameter Groups and Options Conversion

### Parameter Groups for Cyclopts

**Location:** `cli_app/params.py:48-314`

Parameter groups are dataclass-based reusable parameter bundles that can be flattened into command signatures via cyclopts `Parameter(name="*")`. Each command has a corresponding `*Params` dataclass that defines its parameter set.

**Parameter Group Examples:**

- `FilterParams` - Common filters (include, exclude, impact, confidence, severity, limit)
- `QueryParams(FilterParams)` - Query command parameters (extends `FilterParams`)
- `SearchParams(FilterParams)` - Search command parameters (extends `FilterParams`)
- `RunParams(FilterParams)` - Run command parameters with step/steps fields
- `ImpactParams(FilterParams)` - Impact analysis parameters
- `ImportsParams(FilterParams)` - Import analysis parameters
- `SigImpactParams(FilterParams)` - Signature impact parameters
- `SideEffectsParams(FilterParams)` - Side effects parameters
- `ScopesParams` - Scope analysis parameters
- `BytecodeSurfaceParams` - Bytecode analysis parameters
- `ReportParams(FilterParams)` - Report command parameters

**FilterParams Base:**

```python
@dataclass
class FilterParams:
    """Common filter parameters."""
    include: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)
    impact: list[ImpactBucket] = field(default_factory=list)
    confidence: list[ConfidenceBucket] = field(default_factory=list)
    severity: list[SeverityLevel] = field(default_factory=list)
    limit: int = 0  # 0 = unlimited
```

**RunParams Step Fields:**

**Location:** `cli_app/params.py:254-273`

```python
step: Annotated[
    list[dict[str, object]],
    Parameter(
        name="--step",
        group=run_input,
        n_tokens=1,
        accepts_keys=False,
        help='Repeatable JSON step object (e.g., \'{"type":"q","query":"..."}\')',
        converter=_run_step_converter,
    ),
] = field(default_factory=list)

steps: Annotated[
    list[dict[str, object]],
    Parameter(
        name="--steps",
        group=run_input,
        n_tokens=1,
        accepts_keys=False,
        help='JSON array of steps (e.g., \'[{"type":"q",...},{"type":"calls",...}]\')',
        converter=_run_steps_converter,
    ),
] = field(default_factory=list)
```

**Key Observations:**

- `RunParams.step` and `RunParams.steps` are typed as `list[dict[str, object]]`
- Custom converters parse JSON payloads from CLI arguments
- The `run` command converts these dicts into typed `RunStep` instances via msgspec

**Cyclopts Groups:**

```python
search_mode = Group("Search Mode", mutually_exclusive=True, sort_key=1)
filter_group = Group("Filters", sort_key=2)
run_input = Group("Run Input (mutually exclusive)", mutually_exclusive=True, sort_key=1, validator=validators.LimitedChoice(min_selection=1, max_selection=3))
```

**Validators:**

```python
_LIMIT_VALIDATOR = validators.Number(gte=1, lte=1_000_000)
_DEPTH_VALIDATOR = validators.Number(gte=1, lte=10_000)
_MAX_FILES_VALIDATOR = validators.Number(gte=1, lte=1_000_000)
```

**Custom Converters:**

```python
def comma_separated_list(type_: type) -> Callable[[str], list[Any]]:
    """Parse comma-separated values into typed list."""
    ...

def comma_separated_enum(enum_type: type[Enum]) -> Callable[[str], list[Enum]]:
    """Parse comma-separated enum values."""
    ...

def _run_step_converter(value: str) -> dict[str, object]:
    """Parse JSON step object."""
    ...

def _run_steps_converter(value: str) -> list[dict[str, object]]:
    """Parse JSON array of steps."""
    ...
```

### Options Conversion from Params

**Location:** `cli_app/options.py:119-127`

```python
def options_from_params[T](params: Any, *, type_: type[T]) -> T:
    """Convert a CLI params dataclass into a CQ options struct.

    Returns
    -------
    T
        Parsed options struct of the requested type.
    """
    return convert_strict(params, type_=type_, from_attributes=True)
```

**Conversion Mechanism:**

The `options_from_params()` helper uses the typed boundary protocol (`convert_strict()`) with `from_attributes=True` to transform cyclopts param dataclasses into typed option structs. This approach:

1. **Eliminates manual dict conversion** - No need for `asdict()` calls
2. **Provides type safety** - Validation at the CLI boundary
3. **Enables field transformation** - Automatic conversion between compatible types
4. **Supports nested structures** - Recursive conversion for complex payloads

**Example Conversion Flow:**

```
User Input (JSON)
  → Cyclopts Parameter Binding
  → XxxParams (dataclass instance)
  → options_from_params()
  → convert_strict(..., from_attributes=True)
  → XxxOptions (msgspec struct)
  → Request Factory
  → Service Layer
```

**Why Two Type Layers:**

- **CLI Layer (dataclasses)** - Cyclopts requires dataclasses for parameter binding
- **Execution Layer (msgspec structs)** - Msgspec provides zero-copy serialization and better performance
- **Typed Boundary** - `convert_strict()` bridges the two representations with validation

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

def _return_int_as_exit_code_else_zero(result: Any) -> int:
    """Cyclopts second-stage result action (normalize bool/int/None → exit code)."""
    if isinstance(result, bool):
        return 0 if result else 1
    if isinstance(result, int):
        return result
    return 0

CQ_DEFAULT_RESULT_ACTION = (cq_result_action, _return_int_as_exit_code_else_zero)
```

**Two-Stage Pipeline:**

1. **Stage 1** (`cq_result_action`): Dispatch on `CliResult`, call `handle_result()`
2. **Stage 2** (`_return_int_as_exit_code_else_zero`): Normalize bool/int/None to exit code

This pattern allows commands to return `CliResult`, `int`, or `bool`, and all are normalized to shell exit codes.

### Handle Result Pipeline

**Location:** `cli_app/result.py:143-197`

**Processing Stages:**

1. **Non-CqResult handling**: Handle `CliTextResult` or int exit codes
2. **Filter application**: Apply file/impact/confidence/severity filters
3. **Artifact persistence**: Save results + diagnostics + overflow artifacts
4. **Rendering**: Dispatch to format-specific renderer
5. **Output emission**: Write to console (verbatim for JSON/LDMD, Rich-wrapped for MD)

```python
def handle_result(cli_result: CliResult, filters: FilterConfig | None = None) -> int:
    """Main CLI result handler."""
    # Non-CqResult path
    if not cli_result.is_cq_result:
        return _handle_non_cq_result(cli_result)

    # Apply filters
    result = apply_result_filters(cli_result.result, filters)

    # Persist artifacts
    _handle_artifact_persistence(result, cli_result.context)

    # Render output
    output_format = cli_result.context.output_format or OutputFormat.md
    output = render_result(result, output_format)

    # Emit output
    _emit_output(output, output_format)

    return cli_result.exit_code or 0
```

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

```python
def render_result(result: CqResult, output_format: OutputFormat) -> str:
    """Dispatch rendering based on output format."""
    if output_format == OutputFormat.json:
        return dumps_json(result)
    elif output_format == OutputFormat.md:
        return render_markdown(result)
    elif output_format == OutputFormat.summary:
        return render_summary(result)
    elif output_format == OutputFormat.mermaid:
        return render_mermaid_flowchart(result)
    elif output_format == OutputFormat.mermaid_class:
        return render_mermaid_class_diagram(result)
    elif output_format == OutputFormat.dot:
        return render_dot(result)
    elif output_format == OutputFormat.ldmd:
        from tools.cq.ldmd.export import render_ldmd_from_cq_result
        return render_ldmd_from_cq_result(result)
    elif output_format == OutputFormat.both:
        md = render_markdown(result)
        json_output = dumps_json(result)
        return f"{md}\n\n---\n\n```json\n{json_output}\n```"
    else:
        return render_markdown(result)  # default fallback
```

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
| `artifact` | `commands.artifact:artifact_app` | Artifact protocol (list, get) |

**LDMD Sub-Commands:**

- `ldmd index <file>`: Build LDMD section index
- `ldmd get <file> --id <section_id>`: Extract section
- `ldmd search <file> --query <text>`: Search section titles/content
- `ldmd neighbors <file> --id <section_id>`: Get sibling/child sections

**Artifact Sub-Commands:**

- `artifact list`: List saved artifacts
- `artifact get <id>`: Load artifact by ID

### Admin Commands (Deprecated)

**Location:** `cli_app/commands/admin.py:80`

| Command | Status | Description |
|---------|--------|-------------|
| `index` | Deprecated | Cache index management (stub) |
| `cache` | Deprecated | Cache admin commands (stub) |
| `schema` | Deprecated | Schema export commands (stub) |

### Registration Pattern

Commands registered via `app.command()` with module path strings (lazy import):

```python
app.command("tools.cq.cli_app.commands.search:search", group=analysis_group)
app.command("tools.cq.cli_app.commands.query:q", name="q", group=analysis_group)
app.command("tools.cq.cli_app.commands.neighborhood:neighborhood", name=["neighborhood", "nb"], group=analysis_group)
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

```python
def repl(*, ctx: CliContext) -> int:
    """Launch interactive REPL mode."""
    ctx = require_context(ctx)
    print("CQ Interactive REPL. Type 'help' or 'exit'.")

    while True:
        try:
            line = input("cq> ").strip()
            if not line:
                continue
            if line in ("exit", "quit"):
                break
            if line == "help":
                print(app.help)
                continue

            tokens = shlex.split(line)
            exit_code, _event = invoke_with_telemetry(app, tokens, ctx=ctx)

            if exit_code != 0:
                print(f"Command exited with code {exit_code}")
        except (KeyboardInterrupt, EOFError):
            break

    return 0
```

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

**Error Classification Logic:**

```python
def _classify_error_stage(exc: Exception) -> tuple[str | None, str | None]:
    """Classify error by stage and class."""
    from cyclopts import (
        BindingError,
        CoercionError,
        CommandNotFoundError,
        ValidationError,
    )

    if isinstance(exc, CommandNotFoundError):
        return "command_resolve", exc.__class__.__name__
    elif isinstance(exc, BindingError):
        return "binding", exc.__class__.__name__
    elif isinstance(exc, CoercionError):
        return "coercion", exc.__class__.__name__
    elif isinstance(exc, ValidationError):
        return "validation", exc.__class__.__name__
    else:
        return "execution", f"runtime.{exc.__class__.__name__}"
```

---

## Validators

### Input Validation Helpers

**Location:** `cli_app/validators.py`

**Built-In Cyclopts Validators:**

- `validators.Path`: Path existence/type checks
- `validators.Number`: Numeric range checks
- `validators.LimitedChoice`: Min/max choice count
- `validators.mutually_exclusive`: Mutually exclusive options

**Custom Validators:**

```python
def validate_path_exists(path: Path, name: str = "path") -> Path:
    """Validate that a path exists."""
    if not path.exists():
        msg = f"{name} does not exist: {path}"
        raise ValueError(msg)
    return path

def validate_positive_int(value: int, name: str = "value") -> int:
    """Validate that an integer is positive."""
    if value <= 0:
        msg = f"{name} must be positive, got {value}"
        raise ValueError(msg)
    return value

def validate_target_spec(value: str) -> tuple[str, str]:
    """Parse and validate target spec (kind:value format)."""
    if ":" not in value:
        return "symbol", value
    kind, target = value.split(":", 1)
    valid_kinds = {"function", "class", "method", "module", "path"}
    if kind not in valid_kinds:
        msg = f"Invalid target kind: {kind}. Valid: {valid_kinds}"
        raise ValueError(msg)
    return kind, target

def validate_launcher_invariants(**kwargs: object) -> None:
    """Cross-option invariant checking for launcher."""
    config_opts = kwargs.get("config_opts")
    if config_opts and not config_opts.use_config and config_opts.config:
        msg = "Cannot specify --config with --no-config"
        raise ValueError(msg)
```

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

**Implementation:**

Cyclopts provides built-in completion script generation via `app.completion()`. The CLI wraps this with a custom `--install-completion` flag.

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

**Artifact Persistence Handler:**

**Location:** `cli_app/result.py:247-309`

```python
def _handle_artifact_persistence(result: CqResult, ctx: CliContext) -> None:
    """Save artifacts and attach metadata to front-door insight."""
    if not ctx.save_artifact:
        return

    artifact_dir = ctx.artifact_dir or Path(".cq/artifacts")

    # Search macro: save via cache-backed artifact store
    if result.macro == "search":
        _save_search_artifact_bundle(result, artifact_dir)
    else:
        # Other macros: save JSON result + diagnostics
        _save_result_artifact(result, artifact_dir)
        _save_diagnostics_artifact(result, artifact_dir)
        _save_neighborhood_overflow_artifact(result, artifact_dir)
```

---

## Protocol Output Helpers

### Text/JSON Dual-Mode Output

**Location:** `cli_app/protocol_output.py:111`

Protocol commands (LDMD, artifact, admin) use helpers for consistent text/JSON output:

```python
def wants_json(ctx: CliContext) -> bool:
    """Check if JSON output is requested."""
    return ctx.output_format == OutputFormat.json

def text_result(ctx: CliContext, text: str, media_type: str = "text/plain", exit_code: int = 0) -> CliResult:
    """Build CliTextResult wrapped in CliResult."""
    return CliResult(
        result=CliTextResult(text=text, media_type=media_type),
        context=ctx,
        exit_code=exit_code,
    )

def json_result(ctx: CliContext, payload: Any, exit_code: int = 0) -> CliResult:
    """Serialize payload as JSON and wrap in CliResult."""
    from tools.cq.core.structs import dumps_json
    text = dumps_json(payload)
    return text_result(ctx, text, media_type="application/json", exit_code=exit_code)

def emit_payload(ctx: CliContext, payload: Any, text_fallback: str | None = None, exit_code: int = 0) -> CliResult:
    """Dual-mode: JSON if wants_json(ctx), else text fallback."""
    if wants_json(ctx):
        return json_result(ctx, payload, exit_code=exit_code)
    return text_result(ctx, text_fallback or "", exit_code=exit_code)
```

**Usage Example (LDMD):**

```python
@ldmd_app.command
def index(path: str, *, ctx: CliContext) -> CliResult:
    """Build LDMD index from document."""
    ctx = require_context(ctx)

    # Build index
    index_payload = build_ldmd_index(path)

    # Emit JSON or text
    if wants_json(ctx):
        return json_result(ctx, index_payload)

    # Text fallback: pretty-print section list
    text = format_index_as_text(index_payload)
    return text_result(ctx, text)
```

---

## Architectural Observations

### Typed Boundary Integration

**Strength:**

The typed boundary protocol (`convert_strict()`) successfully decouples the CLI layer (cyclopts dataclasses) from the execution layer (msgspec structs). This enables:

- Type validation at the CLI ingress point
- Zero-copy serialization in the execution layer
- Clear separation between parameter binding and execution
- Graceful error handling with `BoundaryDecodeError`

**Observation:**

The dual-type-layer pattern (CLI dataclasses → typed boundary → execution structs) adds complexity but provides strong guarantees. The `from_attributes=True` conversion eliminates the need for manual `asdict()` calls and enables field-level transformations.

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

**Entry Points:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli.py` (12 LOC) - Thin wrapper
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/__init__.py` (49 LOC) - main() and main_async()

**Core CLI Infrastructure:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/app.py` (290 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/context.py` (189 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/infrastructure.py` (154 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/params.py` (314 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result.py` (381 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/types.py` (288 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/telemetry.py` (156 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/options.py` (127 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/protocol_output.py` (111 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/validators.py` (97 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/result_action.py` (81 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/completion.py` (69 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/contracts.py` (5 LOC)

**Command Modules:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/analysis.py` (304 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/ldmd.py` (274 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/neighborhood.py` (145 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/artifact.py` (140 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/query.py` (121 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/search.py` (85 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/admin.py` (80 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/report.py` (78 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/repl.py` (77 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/run.py` (53 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/cli_app/commands/chain.py` (47 LOC)

**Request/Result Infrastructure:**
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/request_factory.py` (396 LOC)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/result_factory.py` (referenced)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/report.py` (1597-1677, markdown rendering)
- `/Users/paulheyse/CodeAnatomy/tools/cq/core/findings_table.py` (filter/rehydrate logic)

---

**Total CLI Layer:** 3,722 LOC across 24 files in `tools/cq/cli_app/`
