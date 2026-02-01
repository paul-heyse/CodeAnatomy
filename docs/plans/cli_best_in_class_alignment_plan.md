# CLI Best-in-Class Alignment Plan

This document summarizes findings from a comprehensive review of the CodeAnatomy CLI implementation (`src/cli/`) against the full capabilities of the Cyclopts library and the broader codebase architecture. It provides specific, prioritized recommendations for making the CLI implementation best-in-class.

## Executive Summary

The current CLI implementation demonstrates solid foundational patterns:
- Meta-app launcher with RunContext injection
- Parameter groups with help ordering
- Lazy command loading for startup performance
- OpenTelemetry telemetry integration
- TOML configuration file support

However, significant opportunities remain to leverage underutilized Cyclopts features and better align the CLI with the codebase's inference-driven, semantic pipeline architecture. Key remaining gaps center on test coverage (golden fixtures, contract tests), plan output snapshots, and verifying version/help output behaviors.

---

## Cyclopts Capability Matrix

**Pinned Version**: `cyclopts>=4.4.1`

### Behavioral Model (Contract)

Cyclopts executes the CLI in this sequence:

1. `argv` normalization and tokenization
2. Parameter binding + coercion (`Annotated` + `Parameter`)
3. Parameter + group validators
4. Command dispatch
5. `result_action` (post-success only)
6. Exception rendering + exit behavior

**Implications**:
- `result_action` only handles successful returns; exceptions must be mapped in the invoke/telemetry layer.
- Help generation may import lazy commands; command modules must keep import surfaces light.
- Validation must be done via Cyclopts validators (not ad-hoc runtime checks) to guarantee UX consistency.

### Features: Active Use

| Feature | Status | Notes |
|---------|--------|-------|
| `App` with TOML config sources | ✅ Active | `codeanatomy.toml`, `pyproject.toml` |
| `@app.meta.default` launcher | ✅ Active | Session parameter injection |
| `Parameter` with groups | ✅ Active | 10 groups with sort keys |
| `Parameter(parse=False)` | ✅ Active | RunContext injection |
| Lazy command loading (import strings) | ✅ Active | All commands use deferred imports |
| `validators.MutuallyExclusive` | ✅ Active | `restore_target_group` only |
| `validators.Number` | ✅ Active | Range validation on numeric params |
| `validators.Path` | ✅ Active | Path existence validation |
| Rich help format + epilogue | ✅ Active | `help_format="rich"` with help epilogue |
| `version_flags` | ✅ Active | `--version`, `-V` |
| `result_action` pipeline | ✅ Active | `cli_result_action` registered |
| `default_parameter` policy | ✅ Active | Global defaults (show defaults/env) |
| Command aliases | ✅ Active | Build/plan/diag/config/version aliases |
| `Parameter(name="*")` flattening | ✅ Active | All option dataclasses |
| `env_var` expansion | ✅ Active | High-value params use `CODEANATOMY_*` |

### Features: Planned Adoption (This Plan)

| Feature | Target Tier | Notes |
|---------|-------------|-------|
| Explicit env var mappings | Tier 1 | Cyclopts lacks `env_var_prefix`; rely on per-parameter `env_var` |
| Group validators (conditional) | Tier 2 | Incremental, SCIP group validation |
| Rich result output formatting | Tier 2 | Use Rich console in result_action |

### Features: Intentionally Deferred

| Feature | Reason |
|---------|--------|
| `AutoRegistry` plugin system | Complexity; no current need for dynamic command discovery |
| Shell completion docs/UX | Install command exists; user-facing docs deferred to Tier 4 |

### Features: Do Not Use

| Feature | Reason |
|---------|--------|
| Interactive shell/REPL | Out of scope; CLI is batch-oriented |
| Meta-app chaining (`app.meta` nesting) | Single meta-launcher is sufficient; chaining adds complexity |
| Lazy command strings in tests | Tests must use direct function imports for debuggability |
| `Parameter(negative=...)` auto-generation | Explicit `--no-*` flags preferred for clarity (currently violated by default_parameter) |
| `call_if_callable` result action | All commands return `int` or `CliResult`; no callable returns |

### Implementation Status Notes (Current Codebase)

- Cyclopts does **not** expose `env_var_prefix` in the pinned version; env vars are configured per-parameter.
- `cli_result_action` now uses Rich output formatting for summaries/artifacts/metrics.
- Shell completion install command exists; documentation and UX remain deferred.
- Version alias (`-v`) is registered alongside `--version`/`-V`.

---

## CLI Contract

This section defines the formal contract for all CLI options, flags, and behaviors.

### Flag Naming Conventions

| Pattern | Example | Usage |
|---------|---------|-------|
| `--kebab-case` | `--output-dir` | All multi-word flags |
| `--no-<flag>` | `--no-enforce-retention` | Boolean negation (explicit, not auto-generated) |
| `-<letter>` | `-o`, `-v` | Single-letter shortcuts for common flags only |
| `--<noun>` | `--version`, `--timestamp` | Target/selector flags |
| `--<verb>-<noun>` | `--show-graph`, `--emit-metrics` | Action flags |

### Required Groups

Every parameter must belong to exactly one group. Groups are defined in `src/cli/groups.py`:

| Group | Sort Key | Purpose |
|-------|----------|---------|
| Session | 0 | Run context: `--config`, `--run-id`, `--log-level` |
| Output | 1 | Artifacts: `--output-dir`, `--report-path` |
| Execution | 2 | Pipeline control: `--execution-mode`, `--parallel` |
| SCIP Configuration | 3 | SCIP indexer options |
| Incremental Processing | 4 | Incremental processing options |
| Repository Scope | 5 | File inclusion/exclusion patterns |
| Graph Adapter | 6 | Hamilton graph adapter backend |
| Advanced | 7 | Advanced configuration options |
| Observability | 8 | OTel configuration (new) |
| Restore Target | 99 | Mutually exclusive version/timestamp |
| Admin | 99 | Administrative commands |

### Parameter Binding and Coercion Contract

Cyclopts resolves parameters based on type hints + `Parameter` metadata. The CLI must obey:

- All CLI-facing parameters use `Annotated[..., Parameter(...)]` with explicit `help` and `group`.
- `Parameter(parse=False)` is reserved for injected runtime objects (RunContext only).
- Any parameter requiring non-trivial parsing must use a Cyclopts validator, not ad-hoc runtime checks.

### Boolean Flag Semantics

- Flags use explicit `--flag` and `--no-flag` names. Auto-negative flags are not used.
- Boolean defaults must be explicit in signatures (no implicit `False`).
- Countable flags and repeated booleans are disallowed unless a command explicitly documents them.

**Status note:** `app.default_parameter` no longer sets `negative="no-"`, so auto-negative
flag generation is disabled and explicit `--no-*` flags are required.

### Structured Types and Dataclasses

- Option dataclasses are the preferred pattern for command configuration.
- All dataclass parameters must be flattened (`Parameter(name="*")`) unless a collision forces an explicit prefix.
- Dataclass parsing must not trigger heavy imports at module import time.

### Lazy Loading Constraints

- Command registration uses import strings for startup performance.
- Command modules must avoid heavy imports at module import time; import inside command functions.
- Tests must import command callables directly to avoid lazy-load indirection.

### Help Formatting Contract

- Help format is **rich** for end users and for golden tests.
- Rich output must be stable: fixed width, deterministic ordering, no time-based content.
- Help output examples must be included via help epilogue or command-specific `help` strings.

### Environment Variable Conventions

All environment variables use the `CODEANATOMY_` prefix. Cyclopts does not expose
an `env_var_prefix` option in the pinned version, so the CLI relies on explicit
per-parameter `env_var` mappings for high-value parameters.

```
CODEANATOMY_LOG_LEVEL=DEBUG
CODEANATOMY_OUTPUT_DIR=/tmp/out
CODEANATOMY_OTEL_ENDPOINT=http://localhost:4317
```

### Parameter Flattening Policy

**Rule**: All option dataclasses are flattened using `Parameter(name="*")` unless there is a naming collision between dataclasses in the same command signature.

```python
# REQUIRED: Flatten all option dataclasses
options: Annotated[BuildOptions, Parameter(name="*")] = _DEFAULT_BUILD_OPTIONS

# EXCEPTION: Naming collision requires explicit prefix
build_opts: Annotated[BuildOptions, Parameter(name="--build-*")] = ...
delta_opts: Annotated[DeltaOptions, Parameter(name="--delta-*")] = ...
```

**Rationale**: Flattening provides cleaner CLI syntax (`--output-dir` vs `--options-output-dir`). Collisions are rare and handled explicitly.

### App Default Parameter Policy

Use `App.default_parameter` to enforce consistent defaults across commands:

```python
app = App(
    default_parameter=Parameter(
        show_default=True,
        show_env_var=True,
    ),
)
```

**Rules**:
- Only global defaults belong here (help formatting, env visibility, default group).
- Command-specific behavior must remain explicit in the parameter definition.

---

## Configuration Layering and Precedence

### Config Provider Chain (Cyclopts `App.config`)

The CLI uses Cyclopts config providers as a chain of mutators over token sets.
Providers are evaluated in order, and later sources only apply to parameters
not already resolved by higher-precedence sources.

```python
app = App(
    config=[
        Toml("codeanatomy.toml", must_exist=False, search_parents=True),
        Toml("pyproject.toml", root_keys=("tool", "codeanatomy"), must_exist=False, search_parents=True),
    ],
)
```

### Precedence Order (Highest to Lowest)

1. **CLI flags** - Explicit command-line arguments
2. **Environment variables** - `CODEANATOMY_*` prefixed
3. **Config file** - `codeanatomy.toml` or `pyproject.toml [tool.codeanatomy]`
4. **Defaults** - Hardcoded in parameter definitions

### Conflict Resolution

**Policy**: First-wins (highest precedence source wins).

```python
# Example: --log-level resolution
# CLI: codeanatomy build --log-level=DEBUG
# Env: CODEANATOMY_LOG_LEVEL=INFO
# Config: log_level = "WARNING"
# Default: "INFO"
# Result: DEBUG (CLI wins)
```

### Failure Behavior

| Scenario | Behavior |
|----------|----------|
| Config file not found | Silent (files marked `must_exist=False`) |
| Config file parse error | Exit with error, show file path and parse error |
| Invalid value in config | Exit with error, show key and validation message |
| Env var invalid type | Exit with error, show env var name and expected type |

### Show Effective Config Subcommand

Add `codeanatomy config show --with-sources` to display resolved configuration:

```bash
$ codeanatomy config show --with-sources
# Effective configuration (CLI > env > config > defaults)
log_level: DEBUG          # source: CLI
output_dir: /tmp/out      # source: env (CODEANATOMY_OUTPUT_DIR)
execution_mode: parallel  # source: config (codeanatomy.toml)
determinism_tier: full    # source: default
```

---

## Result Contract

### Canonical Return Types

All command handlers must return one of:

1. **`int`** - Exit code (0 = success, non-zero = failure)
2. **`CliResult`** - Structured result with exit code, summary, and artifacts

Any other return type is treated as a contract violation and mapped to
`ExitCode.GENERAL_ERROR` by the result action.

```python
@dataclass(frozen=True)
class CliResult:
    """Canonical CLI command result."""

    exit_code: int
    summary: str | None = None
    artifacts: Mapping[str, Path] | None = None
    metrics: Mapping[str, float] | None = None
```

### Result Action Pipeline

```python
def cli_result_action(app: App, cmd: Command, result: int | CliResult) -> int:
    """Process CLI results with artifact emission.

    Note: Exceptions are handled before result_action (Cyclopts behavior).
    This handler only receives successful return values.
    """
    if isinstance(result, int):
        return result

    if not isinstance(result, CliResult):
        from cli.exit_codes import ExitCode

        return int(ExitCode.GENERAL_ERROR)

    if result.summary:
        console.print(result.summary)

    if result.artifacts:
        for name, path in result.artifacts.items():
            console.print(f"  {name}: {path}")

    return result.exit_code

app = App(
    result_action=cli_result_action,
)
```

---

## Exit Code Taxonomy

| Code | Category | Description | Example |
|------|----------|-------------|---------|
| 0 | Success | Command completed successfully | Build completed |
| 1 | General error | Unclassified failure | Unexpected exception |
| 2 | Parse error | CLI argument parsing failed | Invalid flag syntax |
| 3 | Validation error | Input validation failed | Missing required option |
| 4 | Config error | Configuration loading failed | Invalid TOML syntax |
| 10 | Extraction error | Extraction stage failed | File not found |
| 11 | Normalization error | Normalization stage failed | Schema mismatch |
| 12 | Scheduling error | Task scheduling failed | Cycle detected |
| 13 | Execution error | Pipeline execution failed | Hamilton DAG failure |
| 20 | Backend error | External system failure | Delta Lake write failed |
| 21 | DataFusion error | Query engine failure | Invalid expression |

**Cyclopts alignment**:
- `CycloptsError` subclasses map to Parse/Validation errors.
- All other exceptions are mapped via `ExitCode.from_exception` in telemetry/invoke paths.
- Rich-formatted exceptions are enabled in app configuration; result_action does not handle exceptions.

---

## DataFusion Query Constraint

### No Raw SQL Policy

**Rule**: CLI features that interact with DataFusion must NOT expose raw SQL input. All query operations use:

1. **Expression builders** - Programmatic query construction
2. **Predefined query templates** - Named queries with parameter substitution
3. **High-level query interface** - Domain-specific methods

### Rationale

- Consistent with codebase's inference-driven architecture
- Prevents SQL injection risks
- Enables query plan caching and optimization
- Maintains type safety

---

## Testing Requirements

### Golden File Location

```
tests/cli_golden/
├── fixtures/
│   ├── help_root.txt
│   ├── help_build.txt
│   ├── help_delta.txt
│   ├── help_plan.txt
│   ├── help_config.txt
│   ├── error_unknown_command.txt
│   └── error_missing_repo_root.txt
├── test_cli_help_output.py
└── test_cli_error_output.py

tests/unit/cli/
├── test_cli_invoke.py
├── test_cli_validation.py
├── test_cli_config_loader.py
├── test_cli_delta_commands.py
├── test_cli_result_contract.py      # NEW
├── test_cli_exit_codes.py           # NEW
└── test_cli_observability.py        # NEW
```

### Update Procedure

```bash
# Regenerate golden files
uv run pytest tests/cli_golden/ --update-golden

# Review changes
git diff tests/cli_golden/fixtures/
```

### Rich Help Golden Capture

Golden help output must capture the **rich** formatter deterministically:

```python
from cyclopts.testing import invoke
from rich.console import Console

def render_help(tokens: list[str]) -> str:
    console = Console(force_terminal=False, width=100, color_system=None, record=True)
    result = invoke(app, tokens, console=console)
    assert result.exit_code == 0
    return console.export_text()
```

### Contract Tests Per Command

Each top-level command requires:

| Test | Purpose |
|------|---------|
| `test_<cmd>_help_output` | Golden test for `--help` output |
| `test_<cmd>_exit_code_success` | Verify exit 0 on valid input |
| `test_<cmd>_exit_code_validation` | Verify exit 3 on invalid input |
| `test_<cmd>_result_type` | Verify returns `int` or `CliResult` |

---

# Implementation Scope Items

Each scope item below contains:
- Representative code snippets showing key architectural elements
- List of target files to create/modify
- Code/modules to deprecate after completion
- Implementation checklist

---

## Scope Item 1: Result Contract and Exit Code Taxonomy

### Overview

Implement a unified `CliResult` contract and `ExitCode` taxonomy that all commands use. This is foundational infrastructure that enables structured output handling and consistent error reporting.

### Key Architectural Elements

**1. Exit Code Enum (`src/cli/exit_codes.py`)**

```python
"""Exit code taxonomy for CLI commands."""

from __future__ import annotations

from enum import IntEnum


class ExitCode(IntEnum):
    """Standardized exit codes for CLI commands.

    Exit codes are grouped by category:
    - 0: Success
    - 1-9: CLI-level errors (parse, validation, config)
    - 10-19: Pipeline stage errors (extraction, normalization, scheduling, execution)
    - 20-29: Backend errors (storage, DataFusion)
    """

    SUCCESS = 0
    GENERAL_ERROR = 1
    PARSE_ERROR = 2
    VALIDATION_ERROR = 3
    CONFIG_ERROR = 4
    EXTRACTION_ERROR = 10
    NORMALIZATION_ERROR = 11
    SCHEDULING_ERROR = 12
    EXECUTION_ERROR = 13
    BACKEND_ERROR = 20
    DATAFUSION_ERROR = 21

    @classmethod
    def from_exception(cls, exc: BaseException) -> ExitCode:
        """Map exception type to exit code."""
        from cyclopts import CycloptsError

        exc_name = type(exc).__name__

        # Cyclopts errors
        if isinstance(exc, CycloptsError):
            if "Unknown" in exc_name or "Missing" in exc_name:
                return cls.PARSE_ERROR
            if "Validation" in exc_name or "Coercion" in exc_name:
                return cls.VALIDATION_ERROR
            return cls.PARSE_ERROR

        # Config errors
        if "TOML" in exc_name or "Config" in exc_name:
            return cls.CONFIG_ERROR

        # Pipeline stage errors
        if "Extraction" in exc_name:
            return cls.EXTRACTION_ERROR
        if "Normalization" in exc_name or "Schema" in exc_name:
            return cls.NORMALIZATION_ERROR
        if "Schedule" in exc_name or "Cycle" in exc_name:
            return cls.SCHEDULING_ERROR
        if "Hamilton" in exc_name or "DAG" in exc_name:
            return cls.EXECUTION_ERROR

        # Backend errors
        if "Delta" in exc_name:
            return cls.BACKEND_ERROR
        if "DataFusion" in exc_name or "Arrow" in exc_name:
            return cls.DATAFUSION_ERROR

        return cls.GENERAL_ERROR


__all__ = ["ExitCode"]
```

**2. CliResult Dataclass (`src/cli/result.py`)**

```python
"""CLI result contract for structured command output."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Mapping

if TYPE_CHECKING:
    from cli.exit_codes import ExitCode


@dataclass(frozen=True)
class CliResult:
    """Canonical CLI command result.

    All command handlers should return either:
    - `int` for simple exit codes
    - `CliResult` for structured results with metadata

    Parameters
    ----------
    exit_code
        Exit status code (0 = success).
    summary
        Human-readable summary of what was done.
    artifacts
        Mapping of artifact names to file paths.
    metrics
        Timing and count metrics from execution.
    """

    exit_code: int
    summary: str | None = None
    artifacts: Mapping[str, Path] = field(default_factory=dict)
    metrics: Mapping[str, float] = field(default_factory=dict)

    @classmethod
    def success(
        cls,
        summary: str | None = None,
        artifacts: Mapping[str, Path] | None = None,
        metrics: Mapping[str, float] | None = None,
    ) -> CliResult:
        """Create a success result."""
        return cls(
            exit_code=0,
            summary=summary,
            artifacts=artifacts or {},
            metrics=metrics or {},
        )

    @classmethod
    def error(
        cls,
        exit_code: int | ExitCode,
        summary: str | None = None,
    ) -> CliResult:
        """Create an error result."""
        code = int(exit_code)
        return cls(exit_code=code, summary=summary)


__all__ = ["CliResult"]
```

**3. Result Action (`src/cli/result_action.py`)**

```python
"""Result action pipeline for CLI output handling."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rich.console import Console

if TYPE_CHECKING:
    from cyclopts import App, Command

    from cli.result import CliResult


def cli_result_action(app: App, cmd: Command, result: Any) -> int:
    """Process CLI results with structured output.

    Handles both int and CliResult return types:
    - int: Pass through as exit code
    - CliResult: Print summary, list artifacts, return exit code
    - Unknown: Return ExitCode.GENERAL_ERROR

    Parameters
    ----------
    app
        The Cyclopts application.
    cmd
        The command that was executed.
    result
        The result returned by the command handler.

    Returns
    -------
    int
        Exit code to return to the shell.
    """
    from cli.result import CliResult

    console = Console()

    if isinstance(result, int):
        return result

    if not isinstance(result, CliResult):
        from cli.exit_codes import ExitCode

        return int(ExitCode.GENERAL_ERROR)

    # Print summary if provided
    if result.summary:
        console.print(result.summary)

    # List artifacts if any
    if result.artifacts:
        console.print("Artifacts:")
        for name, path in sorted(result.artifacts.items()):
            console.print(f"  {name}: {path}")

    # Print key metrics if any
    if result.metrics:
        duration = result.metrics.get("duration_ms")
        if duration is not None:
            console.print(f"Duration: {duration:.1f}ms")

    return result.exit_code


__all__ = ["cli_result_action"]
```

**4. App Default Parameter Policy (`src/cli/app.py`)**

```python
from cyclopts import App, Parameter

app = App(
    default_parameter=Parameter(
        show_default=True,
        show_env_var=True,
    ),
)
```

**5. Updated Telemetry Integration (`src/cli/telemetry.py` modifications)**

```python
# Add to existing telemetry.py

def _classify_exit_code(exc: BaseException) -> int:
    """Map exception to exit code using taxonomy."""
    from cli.exit_codes import ExitCode
    return int(ExitCode.from_exception(exc))


# Modify invoke_with_telemetry to use ExitCode
def invoke_with_telemetry(
    app: App,
    tokens: list[str] | None,
    *,
    run_context: RunContext | None,
) -> tuple[int, CliInvokeEvent]:
    """Execute CLI with telemetry capture."""
    from cli.exit_codes import ExitCode

    # ... existing code ...

    except CycloptsError as exc:
        exit_code = int(ExitCode.from_exception(exc))
        # ... rest of error handling ...

    except Exception as exc:
        exit_code = int(ExitCode.from_exception(exc))
        # ... rest of error handling ...
```

### Target Files

| Action | File |
|--------|------|
| CREATE | `src/cli/exit_codes.py` |
| CREATE | `src/cli/result.py` |
| CREATE | `src/cli/result_action.py` |
| MODIFY | `src/cli/app.py` (add result_action + default_parameter policy) |
| MODIFY | `src/cli/telemetry.py` (use ExitCode taxonomy) |
| MODIFY | `src/cli/__init__.py` (export new modules) |

### Deprecations After Completion

| Item | Reason |
|------|--------|
| Hardcoded exit codes in commands | Replace with `ExitCode` enum |
| Ad-hoc error classification in `_classify_error_stage` | Consolidate into `ExitCode.from_exception` |

### Implementation Checklist

- [x] Create `src/cli/exit_codes.py` with `ExitCode` enum
- [x] Create `src/cli/result.py` with `CliResult` dataclass
- [x] Create `src/cli/result_action.py` with `cli_result_action`
- [x] Update `src/cli/app.py` to use `result_action=cli_result_action`
- [x] Apply `default_parameter` policy in `src/cli/app.py`
- [x] Update `src/cli/telemetry.py` to use `ExitCode.from_exception`
- [x] Update `src/cli/__init__.py` exports
- [x] Create `tests/unit/cli/test_cli_exit_codes.py`
- [ ] Create `tests/unit/cli/test_cli_contract.py` (return type enforcement)
- [x] Create `tests/unit/cli/test_cli_result.py`
- [x] Switch `cli_result_action` output to Rich console + strict unknown return handling
- [ ] Verify all commands still work with new result handling
- [ ] Update golden fixtures if help output changes

---

## Scope Item 2: Version Flag, Command Aliases, and Rich Help Formatting

### Overview

Add `--version` / `-V` flag support, short aliases for common commands, and rich help formatting
to improve CLI usability and consistency with Cyclopts capabilities.

### Key Architectural Elements

**1. Version Module (`src/cli/commands/version.py` update)**

```python
"""Version information for CLI."""

from __future__ import annotations

from importlib.metadata import version as pkg_version


def get_version() -> str:
    """Get the package version string."""
    try:
        return pkg_version("codeanatomy")
    except Exception:
        return "0.0.0-dev"


def get_version_info() -> dict[str, str]:
    """Get detailed version information."""
    import platform
    import sys

    return {
        "codeanatomy": get_version(),
        "python": sys.version.split()[0],
        "platform": platform.platform(),
    }


__all__ = ["get_version", "get_version_info"]
```

**2. App Configuration Update (`src/cli/app.py`)**

```python
from cli.commands.version import get_version
from cli.result_action import cli_result_action

app = App(
    name="codeanatomy",
    help_format="rich",
    help_epilogue="""
Examples:
  codeanatomy build ./repo --output-dir ./out
  codeanatomy plan ./repo --show-schedule
  codeanatomy delta vacuum --path ./table --apply
""".strip(),
    version=get_version(),
    version_flags=["--version", "-V"],
    result_action=cli_result_action,
    config=[
        Toml("codeanatomy.toml", must_exist=False, search_parents=True),
        Toml(
            "pyproject.toml",
            root_keys=("tool", "codeanatomy"),
            must_exist=False,
            search_parents=True,
        ),
    ],
)

# Command registration with aliases
app.command(name=["build", "b"])("cli.commands.build:build_command")
app.command(name=["plan", "p"])("cli.commands.plan:plan_command")
app.command(name=["diag", "d"])("cli.commands.diag:diag_command")
app.command(name="delta", help="Delta Lake maintenance commands.")(delta_app)
app.command(name=["config", "cfg"])("cli.commands.config:config_app")
app.command(name=["version", "v"])("cli.commands.version:version_command")
```

### Target Files

| Action | File |
|--------|------|
| MODIFY | `src/cli/app.py` (add version_flags, aliases, rich help + epilogue) |
| MODIFY | `src/cli/commands/version.py` (add get_version helper) |

### Deprecations After Completion

None - this is additive.

### Implementation Checklist

- [x] Update `src/cli/commands/version.py` with `get_version()` helper
- [x] Update `src/cli/app.py` with `version=get_version()` and `version_flags`
- [x] Enable `help_format="rich"` and add epilogue examples
- [x] Add command aliases to all `app.command()` calls (version alias added)
- [ ] Update `tests/cli_golden/fixtures/help_root.txt` (aliases and rich formatting)
- [x] Update help golden capture to use Rich console export
- [ ] Add `tests/unit/cli/test_cli_version.py`
- [ ] Verify `codeanatomy --version` and `codeanatomy -V` work
- [x] Verify `codeanatomy b` works as alias for `codeanatomy build`

---

## Scope Item 3: Environment Variable Expansion

### Overview

Cyclopts does not expose `env_var_prefix`, so the CLI relies on explicit `env_var`
overrides for high-value parameters and documents the supported `CODEANATOMY_*`
environment variables.

### Key Architectural Elements

**App Configuration Baseline**

```python
app = App(
    # Cyclopts lacks env_var_prefix; per-parameter env_var overrides are required.
)
```

**Environment Variable Mapping Table**

| Parameter | Flag | Env Var | Group |
|-----------|------|---------|-------|
| output_dir | `--output-dir` | `CODEANATOMY_OUTPUT_DIR` | Output |
| work_dir | `--work-dir` | `CODEANATOMY_WORK_DIR` | Output |
| execution_mode | `--execution-mode` | `CODEANATOMY_EXECUTION_MODE` | Execution |
| determinism_tier | `--determinism-tier` | `CODEANATOMY_DETERMINISM_TIER` | Execution |
| disable_scip | `--disable-scip` | `CODEANATOMY_DISABLE_SCIP` | SCIP |
| scip_output_dir | `--scip-output-dir` | `CODEANATOMY_SCIP_OUTPUT_DIR` | SCIP |
| include_globs | `--include-globs` | `CODEANATOMY_INCLUDE_GLOBS` | Repo Scope |
| exclude_globs | `--exclude-globs` | `CODEANATOMY_EXCLUDE_GLOBS` | Repo Scope |

**Example Parameter Updates (`src/cli/commands/build.py`)**

```python
# Before
output_dir: Annotated[
    Path | None,
    Parameter(
        name="--output-dir",
        help="Directory for build artifacts.",
        group=output_group,
    ),
] = None

# After
output_dir: Annotated[
    Path | None,
    Parameter(
        name="--output-dir",
        help="Directory for build artifacts.",
        env_var="CODEANATOMY_OUTPUT_DIR",
        group=output_group,
    ),
] = None
```

### Target Files

| Action | File |
|--------|------|
| MODIFY | `src/cli/commands/build.py` (add env_var to ~20 parameters) |
| MODIFY | `src/cli/commands/plan.py` (add env_var to output parameters) |
| MODIFY | `src/cli/commands/delta.py` (add env_var to common parameters) |

### Deprecations After Completion

None - this is additive.

### Implementation Checklist

- [x] Update `src/cli/commands/build.py` output group parameters with env_var
- [x] Update `src/cli/commands/build.py` execution group parameters with env_var
- [x] Update `src/cli/commands/build.py` SCIP group parameters with env_var
- [x] Update `src/cli/commands/build.py` repo scope group parameters with env_var
- [x] Update `src/cli/commands/plan.py` parameters with env_var
- [x] Update `src/cli/commands/delta.py` common parameters with env_var
- [x] Rely on explicit `env_var` mappings (Cyclopts has no `env_var_prefix`)
- [x] Create `tests/unit/cli/test_cli_env_vars.py`
- [x] Document env var mappings in help text or README

---

## Scope Item 4: Observability CLI Integration

### Overview

Expose OpenTelemetry configuration through CLI flags that map to `OtelBootstrapOptions`, allowing users to control tracing, metrics, and logging from the command line.

### Key Architectural Elements

**1. Observability Group (`src/cli/groups.py`)**

```python
observability_group = Group(
    "Observability",
    help="Configure OpenTelemetry tracing, metrics, and logging.",
    sort_key=8,
)
```

**2. RunContext Extension (`src/cli/context.py`)**

```python
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Mapping

if TYPE_CHECKING:
    from opentelemetry.trace import Span

    from core_types import JsonValue
    from obs.otel.bootstrap import OtelBootstrapOptions


@dataclass(frozen=True)
class RunContext:
    """Injected run context for CLI commands."""

    run_id: str
    log_level: str
    config_contents: Mapping[str, JsonValue] = field(default_factory=dict)
    span: Span | None = None
    otel_options: OtelBootstrapOptions | None = None  # NEW


__all__ = ["RunContext"]
```

**3. Meta Launcher OTel Parameters (`src/cli/app.py`)**

```python
@app.meta.default
def meta_launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    # Session parameters
    config_file: Annotated[str | None, Parameter(name="--config", group=session_group)] = None,
    run_id: Annotated[str | None, Parameter(name="--run-id", group=session_group)] = None,
    log_level: Annotated[
        Literal["DEBUG", "INFO", "WARNING", "ERROR"],
        Parameter(name="--log-level", env_var="CODEANATOMY_LOG_LEVEL", group=session_group),
    ] = "INFO",
    # Observability parameters (NEW)
    enable_traces: Annotated[
        bool,
        Parameter(
            name="--enable-traces",
            help="Enable OpenTelemetry trace export.",
            env_var="CODEANATOMY_ENABLE_TRACES",
            group=observability_group,
        ),
    ] = True,
    enable_metrics: Annotated[
        bool,
        Parameter(
            name="--enable-metrics",
            help="Enable OpenTelemetry metrics export.",
            env_var="CODEANATOMY_ENABLE_METRICS",
            group=observability_group,
        ),
    ] = True,
    enable_logs: Annotated[
        bool,
        Parameter(
            name="--enable-logs",
            help="Enable OpenTelemetry logs export.",
            env_var="CODEANATOMY_ENABLE_LOGS",
            group=observability_group,
        ),
    ] = True,
    otel_test_mode: Annotated[
        bool,
        Parameter(
            name="--otel-test-mode",
            help="Use in-memory exporters for testing.",
            env_var="CODEANATOMY_OTEL_TEST_MODE",
            group=observability_group,
        ),
    ] = False,
) -> int:
    """Meta launcher with observability configuration."""
    from obs.otel.bootstrap import OtelBootstrapOptions

    # ... existing config loading ...

    otel_options = OtelBootstrapOptions(
        enable_traces=enable_traces,
        enable_metrics=enable_metrics,
        enable_logs=enable_logs,
        test_mode=otel_test_mode,
    )

    run_context = RunContext(
        run_id=effective_run_id,
        log_level=log_level,
        config_contents=config_contents,
        otel_options=otel_options,
    )

    exit_code, _event = invoke_with_telemetry(
        app,
        list(tokens),
        run_context=run_context,
    )
    return exit_code
```

**4. Session Factory Integration (`src/engine/session_factory.py`)**

```python
def build_engine_session(
    *,
    runtime_spec: RuntimeProfileSpec,
    diagnostics: DiagnosticsCollector | None = None,
    surface_policy: MaterializationPolicy | None = None,
    diagnostics_policy: DiagnosticsPolicy | None = None,
    semantic_config: SemanticRuntimeConfig | None = None,
    build_options: SemanticBuildOptions | None = None,
    otel_options: OtelBootstrapOptions | None = None,  # NEW PARAMETER
) -> EngineSession:
    """Build an EngineSession with optional OTel configuration."""
    from obs.otel.bootstrap import OtelBootstrapOptions, configure_otel

    effective_options = otel_options or OtelBootstrapOptions()
    # Merge resource overrides
    resource_overrides = dict(effective_options.resource_overrides or {})
    resource_overrides["codeanatomy.runtime_profile"] = runtime_spec.name

    configure_otel(
        service_name="codeanatomy",
        options=OtelBootstrapOptions(
            enable_traces=effective_options.enable_traces,
            enable_metrics=effective_options.enable_metrics,
            enable_logs=effective_options.enable_logs,
            test_mode=effective_options.test_mode,
            resource_overrides=resource_overrides,
        ),
    )
    # ... rest of session building ...
```

### Target Files

| Action | File |
|--------|------|
| MODIFY | `src/cli/groups.py` (add observability_group) |
| MODIFY | `src/cli/context.py` (add otel_options field) |
| MODIFY | `src/cli/app.py` (add OTel parameters to meta_launcher) |
| MODIFY | `src/cli/commands/build.py` (pass otel_options to session) |
| MODIFY | `src/engine/session_factory.py` (accept otel_options parameter) |

### Deprecations After Completion

| Item | Reason |
|------|--------|
| Hardcoded `configure_otel()` call in session_factory | Replace with parameterized call |

### Implementation Checklist

- [x] Add `observability_group` to `src/cli/groups.py`
- [x] Add `otel_options` field to `RunContext` in `src/cli/context.py`
- [x] Add OTel parameters to `meta_launcher` in `src/cli/app.py`
- [x] Update `build_command` to pass `otel_options` through
- [x] Update `build_engine_session` signature to accept `otel_options`
- [x] Create `tests/unit/cli/test_cli_observability.py`
- [ ] Update golden fixtures for help output
- [x] Document OTel CLI flags in README or docs

---

## Scope Item 5: Config Show with Sources

### Overview

Enhance `codeanatomy config show` to display the source of each configuration value (CLI, env, config file, default).

### Key Architectural Elements

**1. Config Source Tracking (`src/cli/config_source.py`)**

```python
"""Configuration source tracking for CLI."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from core_types import JsonValue


class ConfigSource(StrEnum):
    """Source of a configuration value."""

    CLI = "cli"
    ENV = "env"
    CONFIG_FILE = "config_file"
    DEFAULT = "default"
    DERIVED = "derived"


@dataclass(frozen=True)
class ConfigValue:
    """A configuration value with source tracking."""

    key: str
    value: JsonValue
    source: ConfigSource
    location: str | None = None  # e.g., "codeanatomy.toml:15" or "CODEANATOMY_LOG_LEVEL"


@dataclass(frozen=True)
class ConfigWithSources:
    """Configuration mapping with source tracking for each value."""

    values: dict[str, ConfigValue]

    def to_display_dict(self) -> dict[str, dict[str, object]]:
        """Convert to display format for JSON output."""
        return {
            cv.key: {
                "value": cv.value,
                "source": cv.source.value,
                "location": cv.location,
            }
            for cv in self.values.values()
        }

    def to_flat_dict(self) -> dict[str, JsonValue]:
        """Convert to flat dict (values only)."""
        return {cv.key: cv.value for cv in self.values.values()}


__all__ = ["ConfigSource", "ConfigValue", "ConfigWithSources"]
```

**2. Enhanced Config Loader (`src/cli/config_loader.py` additions)**

```python
def load_effective_config_with_sources(
    config_file: str | None,
) -> ConfigWithSources:
    """Load config and track the source of each value.

    Parameters
    ----------
    config_file
        Explicit config file path, or None to search.

    Returns
    -------
    ConfigWithSources
        Configuration with source tracking for each value.
    """
    from cli.config_source import ConfigSource, ConfigValue, ConfigWithSources

    values: dict[str, ConfigValue] = {}

    # Load from config file
    raw_config, config_path = _load_config_with_path(config_file)
    normalized = normalize_config_contents(raw_config)

    for key, value in normalized.items():
        values[key] = ConfigValue(
            key=key,
            value=value,
            source=ConfigSource.CONFIG_FILE,
            location=str(config_path) if config_path else None,
        )

    # Check for env var overrides
    env_mappings = _get_env_var_mappings()
    for key, env_var in env_mappings.items():
        env_value = os.environ.get(env_var)
        if env_value is not None:
            values[key] = ConfigValue(
                key=key,
                value=_parse_env_value(env_value, key),
                source=ConfigSource.ENV,
                location=env_var,
            )

    return ConfigWithSources(values=values)


def _get_env_var_mappings() -> dict[str, str]:
    """Return mapping of config keys to environment variable names."""
    return {
        "log_level": "CODEANATOMY_LOG_LEVEL",
        "output_dir": "CODEANATOMY_OUTPUT_DIR",
        "runtime_profile_name": "CODEANATOMY_RUNTIME_PROFILE",
        "incremental_state_dir": "CODEANATOMY_STATE_DIR",
        "incremental_repo_id": "CODEANATOMY_REPO_ID",
        # ... more mappings ...
    }
```

**3. Enhanced Config Show Command (`src/cli/commands/config.py`)**

```python
def show_config(
    with_sources: Annotated[
        bool,
        Parameter(
            name="--with-sources",
            help="Include source information for each value.",
        ),
    ] = False,
    *,
    run_context: Annotated[RunContext | None, Parameter(parse=False)] = None,
) -> int:
    """Show the effective configuration payload."""
    if with_sources:
        from cli.config_loader import load_effective_config_with_sources

        config = load_effective_config_with_sources(None)
        payload = json.dumps(config.to_display_dict(), indent=2, sort_keys=True)
    else:
        if run_context is None:
            config_contents = load_effective_config(None)
            normalized = normalize_config_contents(config_contents)
        else:
            normalized = dict(run_context.config_contents)
        validate_config_mutual_exclusion(normalized)
        payload = json.dumps(normalized, indent=2, sort_keys=True)

    sys.stdout.write(payload + "\n")
    return 0
```

### Target Files

| Action | File |
|--------|------|
| CREATE | `src/cli/config_source.py` |
| MODIFY | `src/cli/config_loader.py` (add load_effective_config_with_sources) |
| MODIFY | `src/cli/commands/config.py` (add --with-sources flag) |

### Deprecations After Completion

None - this is additive.

### Implementation Checklist

- [x] Create `src/cli/config_source.py` with source tracking types
- [x] Add `load_effective_config_with_sources` to `src/cli/config_loader.py`
- [x] Add `--with-sources` flag to `show_config` command
- [ ] Create `tests/unit/cli/test_cli_config_sources.py`
- [ ] Add golden fixture for `config show --with-sources` output
- [x] Document config precedence in help text

---

## Scope Item 6: Group Validators for Conditional Parameters

### Overview

Add conditional validators to parameter groups that enforce logical constraints (e.g., SCIP parameters only valid when SCIP is enabled).

### Key Architectural Elements

**1. Custom Validators (`src/cli/validators.py`)**

```python
"""Custom validators for CLI parameter groups."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

from cyclopts import validators

if TYPE_CHECKING:
    from cyclopts import Group


class ConditionalRequired:
    """Validator that requires parameters when a condition is met.

    Parameters
    ----------
    condition_key
        The parameter name to check.
    condition_value
        The value that triggers the requirement.
    required_keys
        Parameters that must be set when condition is met.
    """

    def __init__(
        self,
        condition_key: str,
        condition_value: Any,
        required_keys: list[str],
    ) -> None:
        self.condition_key = condition_key
        self.condition_value = condition_value
        self.required_keys = required_keys

    def __call__(self, values: dict[str, Any]) -> None:
        """Validate the parameter group."""
        condition_met = values.get(self.condition_key) == self.condition_value

        if condition_met:
            missing = [k for k in self.required_keys if values.get(k) is None]
            if missing:
                msg = (
                    f"When {self.condition_key}={self.condition_value}, "
                    f"the following parameters are required: {', '.join(missing)}"
                )
                raise ValueError(msg)


class ConditionalDisabled:
    """Validator that disables parameters when a condition is met.

    Useful for flags like --disable-scip where related params should be ignored.
    """

    def __init__(
        self,
        disable_key: str,
        disable_value: bool,
        disabled_keys: list[str],
    ) -> None:
        self.disable_key = disable_key
        self.disable_value = disable_value
        self.disabled_keys = disabled_keys

    def __call__(self, values: dict[str, Any]) -> None:
        """Validate the parameter group."""
        disabled = values.get(self.disable_key) == self.disable_value

        if disabled:
            provided = [k for k in self.disabled_keys if values.get(k) is not None]
            if provided:
                msg = (
                    f"When {self.disable_key}={self.disable_value}, "
                    f"the following parameters are ignored: {', '.join(provided)}"
                )
                # Log warning instead of raising - allow but warn
                import logging
                logging.getLogger("cli").warning(msg)


__all__ = ["ConditionalRequired", "ConditionalDisabled"]
```

**2. Updated Groups with Validators (`src/cli/groups.py`)**

```python
from cyclopts import Group, Parameter, validators

from cli.validators import ConditionalDisabled

# SCIP group with conditional validator
scip_group = Group(
    "SCIP Configuration",
    help="Configure SCIP semantic index extraction.",
    sort_key=3,
    validator=ConditionalDisabled(
        disable_key="disable_scip",
        disable_value=True,
        disabled_keys=[
            "scip_output_dir",
            "scip_python_bin",
            "scip_target_only",
            "scip_timeout_s",
            "scip_env_json",
            "scip_extra_args",
        ],
    ),
)

# Incremental group - git params require incremental=True
incremental_group = Group(
    "Incremental Processing",
    help="Configure incremental/CDF-based processing.",
    sort_key=4,
    # Note: Conditional validation handled in command logic
    # because incremental flag is in a different group
)

# Restore target - mutually exclusive (existing)
restore_target_group = Group(
    "Restore Target",
    help="Specify exactly one of version or timestamp.",
    validator=validators.MutuallyExclusive(),
    default_parameter=Parameter(show_default=False),
)
```

### Target Files

| Action | File |
|--------|------|
| CREATE | `src/cli/validators.py` |
| MODIFY | `src/cli/groups.py` (add validators to groups) |

### Deprecations After Completion

None - this is additive.

### Implementation Checklist

- [x] Create `src/cli/validators.py` with custom validators
- [x] Add `ConditionalDisabled` validator to `scip_group`
- [x] Add tests for validator behavior in `tests/unit/cli/test_cli_validators.py`
- [x] Verify warning messages appear when disabled params are provided
- [x] Document validation behavior in help text

---

## Scope Item 7: Comprehensive Test Coverage

### Overview

Expand CLI test coverage with golden tests, contract tests, and validation tests for all commands.

### Key Architectural Elements

**1. Golden Test Infrastructure (`tests/cli_golden/conftest.py`)**

```python
"""Pytest fixtures for CLI golden tests."""

from __future__ import annotations

import os
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from cyclopts import CycloptsError
from rich.console import Console

if TYPE_CHECKING:
    from collections.abc import Sequence

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture
def golden_dir() -> Path:
    """Return the golden fixtures directory."""
    return FIXTURES_DIR


def capture_help(app, tokens: Sequence[str] | None) -> str:
    """Capture help output for golden comparison."""
    buffer = StringIO()
    console = Console(file=buffer, force_terminal=False, color_system=None, width=120)
    app.help_print(tokens=list(tokens) if tokens is not None else [], console=console)
    return buffer.getvalue()


def capture_error(app, tokens: Sequence[str]) -> str:
    """Capture error output for golden comparison."""
    from contextlib import suppress

    buffer = StringIO()
    error_console = Console(file=buffer, force_terminal=False, color_system=None, width=120)
    with suppress(CycloptsError):
        app.parse_args(tokens, exit_on_error=False, print_error=True, error_console=error_console)
    return buffer.getvalue()


@pytest.fixture
def update_golden(request) -> bool:
    """Return True if --update-golden flag was passed."""
    return request.config.getoption("--update-golden", default=False)


def pytest_addoption(parser):
    """Add --update-golden option to pytest."""
    parser.addoption(
        "--update-golden",
        action="store_true",
        default=False,
        help="Update golden fixtures instead of comparing",
    )
```

**2. Contract Test Pattern (`tests/unit/cli/test_cli_contract.py`)**

```python
"""Contract tests for CLI commands."""

from __future__ import annotations

import inspect
from typing import Union, get_args, get_origin

import pytest

from cli.result import CliResult


def _get_return_type(func):
    """Get the return type annotation from a function."""
    hints = getattr(func, "__annotations__", {})
    return hints.get("return")


class TestCommandContracts:
    """Verify all commands follow the result contract."""

    @pytest.mark.parametrize(
        "command_path",
        [
            "cli.commands.build:build_command",
            "cli.commands.plan:plan_command",
            "cli.commands.diag:diag_command",
            "cli.commands.config:show_config",
            "cli.commands.config:validate_config",
            "cli.commands.config:init_config",
            "cli.commands.delta:vacuum_command",
            "cli.commands.delta:checkpoint_command",
            "cli.commands.delta:cleanup_log_command",
            "cli.commands.delta:export_command",
            "cli.commands.delta:restore_command",
        ],
    )
    def test_command_return_type(self, command_path: str) -> None:
        """Verify command returns int or CliResult."""
        module_path, func_name = command_path.rsplit(":", 1)
        module = __import__(module_path, fromlist=[func_name])
        func = getattr(module, func_name)

        return_type = _get_return_type(func)

        # Check if return type is int, CliResult, or Union[int, CliResult]
        valid_types = (int, CliResult)

        if return_type is None:
            pytest.fail(f"{command_path} has no return type annotation")

        if return_type in valid_types:
            return

        # Check for Union type
        origin = get_origin(return_type)
        if origin is Union:
            args = get_args(return_type)
            if all(arg in valid_types for arg in args):
                return

        pytest.fail(
            f"{command_path} return type {return_type} is not int or CliResult"
        )
```

**3. Exit Code Tests (`tests/unit/cli/test_cli_exit_codes.py`)**

```python
"""Tests for CLI exit code taxonomy."""

from __future__ import annotations

import pytest

from cli.exit_codes import ExitCode


class TestExitCodeFromException:
    """Test exception to exit code mapping."""

    def test_cyclopts_validation_error(self) -> None:
        from cyclopts import ValidationError

        exc = ValidationError("test")
        assert ExitCode.from_exception(exc) == ExitCode.VALIDATION_ERROR

    def test_cyclopts_missing_argument(self) -> None:
        from cyclopts import MissingArgumentError

        exc = MissingArgumentError("test")
        assert ExitCode.from_exception(exc) == ExitCode.PARSE_ERROR

    def test_file_not_found(self) -> None:
        exc = FileNotFoundError("test.txt")
        assert ExitCode.from_exception(exc) == ExitCode.GENERAL_ERROR

    def test_config_error(self) -> None:
        # Simulate a TOML parse error
        class TOMLDecodeError(Exception):
            pass

        exc = TOMLDecodeError("Invalid TOML")
        assert ExitCode.from_exception(exc) == ExitCode.CONFIG_ERROR


class TestExitCodeValues:
    """Test exit code value assignments."""

    def test_success_is_zero(self) -> None:
        assert ExitCode.SUCCESS == 0

    def test_cli_errors_under_ten(self) -> None:
        assert ExitCode.PARSE_ERROR < 10
        assert ExitCode.VALIDATION_ERROR < 10
        assert ExitCode.CONFIG_ERROR < 10

    def test_pipeline_errors_in_teens(self) -> None:
        assert 10 <= ExitCode.EXTRACTION_ERROR < 20
        assert 10 <= ExitCode.NORMALIZATION_ERROR < 20
        assert 10 <= ExitCode.SCHEDULING_ERROR < 20
        assert 10 <= ExitCode.EXECUTION_ERROR < 20

    def test_backend_errors_in_twenties(self) -> None:
        assert 20 <= ExitCode.BACKEND_ERROR < 30
        assert 20 <= ExitCode.DATAFUSION_ERROR < 30
```

### Target Files

| Action | File |
|--------|------|
| MODIFY | `tests/cli_golden/conftest.py` (add --update-golden option) |
| CREATE | `tests/unit/cli/test_cli_contract.py` |
| CREATE | `tests/unit/cli/test_cli_exit_codes.py` |
| CREATE | `tests/unit/cli/test_cli_result.py` |
| CREATE | `tests/unit/cli/test_cli_validators.py` |
| CREATE | `tests/unit/cli/test_cli_env_vars.py` |
| CREATE | `tests/unit/cli/test_cli_observability.py` |
| MODIFY | `tests/cli_golden/fixtures/` (add missing golden files) |

### Deprecations After Completion

None - this is additive.

### Implementation Checklist

- [x] Add `--update-golden` option to pytest configuration
- [ ] Create `tests/unit/cli/test_cli_contract.py` for return type validation
- [x] Create `tests/unit/cli/test_cli_exit_codes.py` for exit code mapping
- [x] Create `tests/unit/cli/test_cli_result.py` for CliResult behavior
- [x] Create `tests/unit/cli/test_cli_validators.py` for custom validators
- [x] Create `tests/unit/cli/test_cli_env_vars.py` for env var precedence
- [x] Create `tests/unit/cli/test_cli_observability.py` for OTel integration
- [ ] Add golden fixtures for `help_plan.txt`, `help_config.txt`, `help_diag.txt`
- [ ] Achieve >80% CLI module coverage
- [x] Document test patterns in `tests/cli_golden/README.md`

---

## Scope Item 8: Plan Command Output Formats

### Overview

Enhance the `plan` command to expose inference-driven scheduling details with multiple output formats (JSON, table, DOT).

### Key Architectural Elements

**1. Enhanced Plan Options (`src/cli/commands/plan.py`)**

```python
@dataclass(frozen=True)
class PlanOptions:
    """CLI options for plan command."""

    show_graph: Annotated[
        bool,
        Parameter(help="Display task dependency graph."),
    ] = False
    show_schedule: Annotated[
        bool,
        Parameter(help="Display computed execution schedule."),
    ] = False
    show_inferred_deps: Annotated[
        bool,
        Parameter(help="Show inferred dependencies from DataFusion lineage."),
    ] = False
    show_task_graph: Annotated[
        bool,
        Parameter(help="Show rustworkx task graph structure."),
    ] = False
    validate: Annotated[
        bool,
        Parameter(help="Validate plan consistency and report issues."),
    ] = False
    output_format: Annotated[
        Literal["text", "json", "dot"],
        Parameter(help="Output format for plan display."),
    ] = "text"
    output_file: Annotated[
        Path | None,
        Parameter(name="-o", help="Write plan to file instead of stdout."),
    ] = None
    execution_mode: Annotated[
        ExecutionMode,
        Parameter(
            name="--execution-mode",
            help="Execution mode to use when compiling the plan.",
            group=execution_group,
        ),
    ] = ExecutionMode.PLAN_PARALLEL
```

**2. Enhanced Payload Builder**

```python
def _build_payload(
    *,
    plan: ExecutionPlan,
    plan_bundle: PlanArtifactBundle,
    options: _PlanPayloadOptions,
) -> object:
    """Build plan payload based on output options."""
    if options.output_format == "dot":
        return plan.diagnostics.dot

    payload: dict[str, object] = {
        "plan_signature": plan.plan_signature,
        "reduced_plan_signature": plan.reduced_task_dependency_signature,
        "task_count": len(plan.active_tasks),
    }

    if options.show_graph:
        payload["graph"] = {
            "dot": plan.diagnostics.dot,
            "critical_path": list(plan.critical_path_task_names),
            "critical_path_length": plan.critical_path_length_weighted,
        }

    if options.show_inferred_deps:
        # Extract inferred dependencies from plan
        inferred_deps = {}
        for task in plan.active_tasks:
            deps = [dep.name for dep in task.dependencies]
            inferred_deps[task.name] = deps
        payload["inferred_deps"] = inferred_deps

    if options.show_task_graph:
        # Extract task graph structure
        nodes = [task.name for task in plan.active_tasks]
        edges = []
        for task in plan.active_tasks:
            for dep in task.dependencies:
                edges.append([dep.name, task.name])
        payload["task_graph"] = {"nodes": nodes, "edges": edges}

    if options.show_schedule:
        schedule_payload = to_builtins(plan_bundle.schedule_envelope)
        payload["schedule"] = schedule_payload

    if options.validate:
        validation_payload = to_builtins(plan_bundle.validation_envelope)
        payload["validation"] = validation_payload

    if options.output_format == "json":
        return payload

    return _format_text(payload, options)
```

### Target Files

| Action | File |
|--------|------|
| MODIFY | `src/cli/commands/plan.py` (add new options and payload builder) |

### Deprecations After Completion

None - this is additive.

### Implementation Checklist

- [x] Add `--show-inferred-deps` flag to PlanOptions
- [x] Add `--show-task-graph` flag to PlanOptions
- [x] Update `_build_payload` to include inferred deps and task graph
- [x] Update `_format_text` for new payload fields
- [ ] Add golden fixtures for plan output formats
- [ ] Create `tests/unit/cli/test_cli_plan_output.py`

---

## Prioritized Implementation Plan

### Tier 1: Foundation (High Impact, Enables Later Tiers)

| Scope Item | Description | Risk | Est. Effort |
|------------|-------------|------|-------------|
| 1. Result Contract | `CliResult`, `ExitCode`, result action, default_parameter | Medium | 2-3 days |
| 2. Version + Rich Help | `--version`, aliases, `help_format="rich"` | Low | 1 day |
| 3. Env Var Expansion | explicit env_var mappings on high-value params | Low | 1 day |

**Tier 1 Acceptance Criteria:**
- [x] `ExitCode` enum exists with all codes documented
- [x] `CliResult` dataclass exists with factory methods
- [x] Result action processes `CliResult` and emits artifacts
- [x] `default_parameter` policy applied in `App`
- [ ] `codeanatomy --version` returns package version (not verified)
- [x] `codeanatomy b` works as alias for `build`
- [x] Rich help is enabled with epilogue examples
- [x] All high-value parameters have `CODEANATOMY_*` env vars via explicit mappings
- [ ] Help output golden tests pass (missing help_plan/config/diag fixtures)

### Tier 2: Enhanced Validation and Observability (High Impact, Medium Effort)

| Scope Item | Description | Risk | Est. Effort |
|------------|-------------|------|-------------|
| 4. Observability | OTel CLI integration | Medium | 2 days |
| 5. Config Sources | `config show --with-sources` | Low | 1 day |
| 6. Group Validators | Conditional validators | Medium | 1 day |

**Tier 2 Acceptance Criteria:**
- [x] `--enable-traces`, `--enable-metrics`, `--enable-logs` flags exist
- [x] OTel options thread through to session factory
- [x] `config show --with-sources` shows value origins
- [x] SCIP group warns when disabled params provided
- [ ] Validation error messages are clear and actionable (no golden/error snapshot)

### Tier 3: Advanced Features (Medium Impact, Higher Effort)

| Scope Item | Description | Risk | Est. Effort |
|------------|-------------|------|-------------|
| 7. Test Coverage | Comprehensive CLI tests | Low | 2-3 days |
| 8. Plan Output | Enhanced plan command | Medium | 1-2 days |

**Tier 3 Acceptance Criteria:**
- [ ] CLI test coverage >80%
- [ ] All commands have golden help tests
- [ ] Contract tests verify return types
- [x] Plan command shows inferred deps and task graph
- [x] All output formats (text, json, dot) work correctly

---

## Implementation Guidelines

### Code Style

Follow existing patterns:
```python
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Literal

from cyclopts import Parameter, validators

if TYPE_CHECKING:
    from pathlib import Path
```

### Testing Pattern

Use Cyclopts testing utilities with golden files:
```python
from pathlib import Path
from cyclopts.testing import invoke
from rich.console import Console

GOLDEN_DIR = Path(__file__).parent / "golden"

def test_command_help(golden_dir, update_golden):
    console = Console(force_terminal=False, width=100, color_system=None, record=True)
    result = invoke(app, ["command", "--help"], console=console)
    golden_path = golden_dir / "help_command.txt"
    output = console.export_text()

    if update_golden:
        golden_path.write_text(output)
    else:
        assert output == golden_path.read_text()
```

### Documentation Requirements

Every new option must have:
- Clear `help` text (imperative mood)
- Appropriate `group` assignment
- `env_var` with `CODEANATOMY_` prefix where applicable
- Golden test coverage
- Entry in configuration reference

### Architectural Guardrails

- Avoid raw SQL in CLI-visible features; use expression builders or predefined query templates.
- Keep command modules light at import time; heavy imports must be inside command handlers.

---

## Conclusion

The CodeAnatomy CLI has a solid foundation but significant opportunities exist to:

1. **Establish contracts** - Result types, exit codes, flag naming, config precedence
2. **Leverage Cyclopts features** - Group validators, result actions, env vars, aliases
3. **Better expose codebase capabilities** - Inference scheduling, semantic pipeline, structured inspection
4. **Improve testability** - Golden tests, contract tests, validation tests

Implementing Tier 1 establishes the foundational contracts that enable Tiers 2 and 3. Result actions are in Tier 1 because they are an architectural seam that is painful to retrofit later.
