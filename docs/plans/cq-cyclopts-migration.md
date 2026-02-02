# CQ Cyclopts Migration Plan

> **Status:** Proposal (Revised)
> **Author:** Claude
> **Date:** 2026-02-02
> **Scope:** tools/cq CLI modernization

---

## Executive Summary

This document proposes migrating the `tools/cq` CLI from `argparse` to `cyclopts` to leverage type-hint-driven parsing, structured validation, and superior help generation. The current ~1,100 line `cli.py` can be reduced to ~400 lines while gaining shell completion, rich error messages, and composable parameter patterns.

**Key Benefits:**
1. **Type-Safety:** Type hints become the source of truth for parsing
2. **DRY Parameters:** Shared parameter groups via dataclasses eliminate `_add_common_args()` repetition
3. **Rich UX:** Automatic shell completion, formatted errors, markdown help
4. **Testability:** Structured `CliResult` envelope with full execution context
5. **Extensibility:** Lazy loading for macro commands, context injection via meta-app

---

## Design Principles

These principles guide all design decisions:

| Principle | Description |
|-----------|-------------|
| **Single Source of Truth** | Type hints and dataclasses define the CLI contract; no duplication |
| **Single Execution Envelope** | All commands return `CliResult` with full context (result, argv, timing, errors) |
| **Zero Global Mutation** | No global state; all context passed via `CliContext` injection |
| **Compatibility** | CLI tokens unchanged; existing scripts work without modification |
| **Fast Startup** | Lazy command loading; <100ms for `cq --help` |
| **Deterministic Help** | Fixed console width, no color bleeding, stable group ordering |
| **Extensible Subcommands** | Easy to add new commands without touching core wiring |
| **One Entrypoint** | Single `main()` in `__main__.py`; no legacy fallback paths |

---

## Current Architecture Analysis

### Current CLI Structure (`tools/cq/cli.py`)

```
cli.py (1,094 lines)
├── build_parser()           # argparse setup (~350 lines)
├── _add_common_args()       # Repeated filter options (~60 lines)
├── _cmd_*_cli()             # 11 command handlers (~400 lines)
├── _apply_cli_filters()     # Post-processing (~50 lines)
├── _output_result()         # Rendering dispatch (~80 lines)
└── main()                   # Entry point (~70 lines)
```

### Current Commands

| Command | Type | Positional Args | Key Options |
|---------|------|-----------------|-------------|
| `impact` | Macro | `function` | `--param`, `--depth` |
| `calls` | Macro | `function` | - |
| `imports` | Macro | - | `--cycles`, `--module` |
| `exceptions` | Macro | - | `--function` |
| `sig-impact` | Macro | `symbol` | `--to` |
| `side-effects` | Macro | - | `--max-files` |
| `scopes` | Macro | `target` | - |
| `async-hazards` | Macro | - | `--profiles` |
| `bytecode-surface` | Macro | `target` | `--show` |
| `q` | Query | `query_string` | `--cache/--no-cache`, `--explain-files` |
| `report` | Bundle | `preset` | `--target`, `--in`, `--param`, `--to`, `--bytecode-show` |
| `index` | Admin | - | `--rebuild`, `--stats`, `--clear` |
| `cache` | Admin | - | `--stats`, `--clear` |

### Current Functions to Replace

| Function | Lines | Replacement |
|----------|-------|-------------|
| `build_parser()` | 678-1010 | `cli_app/app.py` lazy command registration |
| `_add_common_args()` | 180-241 | `cli_app/params.py` `@Parameter(name="*")` dataclasses |
| `_cmd_impact_cli()` | 244-261 | `cli_app/commands/macros.py::impact()` |
| `_cmd_calls_cli()` | 264-278 | `cli_app/commands/macros.py::calls()` |
| `_cmd_imports_cli()` | 281-297 | `cli_app/commands/macros.py::imports()` |
| `_cmd_exceptions_cli()` | 300-314 | `cli_app/commands/macros.py::exceptions()` |
| `_cmd_sig_impact_cli()` | 317-333 | `cli_app/commands/macros.py::sig_impact()` |
| `_cmd_side_effects_cli()` | 336-351 | `cli_app/commands/macros.py::side_effects()` |
| `_cmd_scopes_cli()` | 354-369 | `cli_app/commands/macros.py::scopes()` |
| `_cmd_async_hazards_cli()` | 372-387 | `cli_app/commands/macros.py::async_hazards()` |
| `_cmd_bytecode_cli()` | 390-406 | `cli_app/commands/macros.py::bytecode_surface()` |
| `_cmd_index_cli()` | 409-519 | `cli_app/commands/admin.py::index()` |
| `_cmd_cache_cli()` | 522-545 | `cli_app/commands/admin.py::cache()` |
| `_cmd_query_cli()` | 548-625 | `cli_app/commands/query.py::q()` |
| `_cmd_report_cli()` | 628-675 | `cli_app/commands/report.py::report()` |
| `_apply_cli_filters()` | 51-103 | `cli_app/result.py::apply_filters()` |
| `_output_result()` | 106-177 | `cli_app/result.py::render_result()` |
| `_find_repo_root()` | 39-48 | `CliContext.build()` via `resolve_repo_context()` |
| `main()` | 1012-1094 | `cli_app/app.py::main()` |

---

## Proposed Cyclopts Architecture

### Module Structure

**IMPORTANT:** The package is named `cli_app/` (not `cli/`) to avoid collision with the existing `cli.py` module file.

```
tools/cq/
├── cli_app/                    # Package name avoids collision with cli.py
│   ├── __init__.py             # Re-export app, main()
│   ├── app.py                  # Root App + meta-app (~150 lines)
│   ├── context.py              # CliContext + CliResult dataclasses (~80 lines)
│   ├── params.py               # Shared parameter dataclasses (~100 lines)
│   ├── types.py                # Enums + converters (~100 lines)
│   ├── validators.py           # Parameter validators (~50 lines)
│   ├── result.py               # Result action pipeline (~80 lines)
│   └── commands/
│       ├── __init__.py
│       ├── macros.py           # Macro commands (lazy-loaded)
│       ├── query.py            # Query command
│       ├── report.py           # Report command
│       └── admin.py            # Index + cache commands
├── cli.py                      # Entry point (thin wrapper, imports from cli_app/)
└── __main__.py                 # `python -m tools.cq` entrypoint
```

---

## Implementation Scope Items

Each scope item includes:
- **Key Architectural Elements**: Core patterns and code structures
- **Representative Code Snippets**: Canonical implementation examples
- **Files to Create**: New files for this scope
- **Code to Deprecate/Delete**: What to remove after completion
- **Implementation Checklist**: Step-by-step verification

---

## Scope 1: Foundation Layer

### Overview

Create the core infrastructure for the new CLI architecture: context injection, type definitions, shared parameters, and validators.

### Key Architectural Elements

1. **Execution Envelope Pattern**: `CliContext` + `CliResult` provide immutable context and structured results
2. **Flattened Dataclass Parameters**: `@Parameter(name="*")` enables DRY parameter sharing
3. **Enum Coercion with Name Matching**: Enum member names = CLI tokens
4. **Comma-Separated Converter**: Custom converter for `--impact high,med` syntax

### Representative Code Snippets

#### 1.1 Execution Context (`context.py`)

```python
from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult
    from tools.cq.core.toolchain import Toolchain
    from tools.cq.index.repo import RepoContext


@dataclass(frozen=True, slots=True)
class CliContext:
    """
    Immutable execution context injected into all commands.

    Provides shared dependencies without global state.
    """

    toolchain: Toolchain
    """Detected toolchain (ast-grep, ripgrep versions)."""

    repo: RepoContext
    """Repository context (root, git info)."""

    argv: tuple[str, ...]
    """Original command-line arguments (for logging/artifacts)."""

    started_ns: int = field(default_factory=time.perf_counter_ns)
    """Monotonic start time for duration tracking."""

    @classmethod
    def build(cls, argv: list[str], root: Path | None = None) -> CliContext:
        """Build context from CLI invocation."""
        from tools.cq.core.toolchain import Toolchain
        from tools.cq.index.repo import resolve_repo_context

        toolchain = Toolchain.detect()
        repo = resolve_repo_context(root)

        return cls(
            toolchain=toolchain,
            repo=repo,
            argv=tuple(argv),
        )


@dataclass(slots=True)
class CliResult:
    """
    Structured result envelope for all CLI commands.

    Captures full execution context for logging, testing, and observability.
    """

    result: CqResult | int | None
    """Command result: CqResult for analysis, int for admin exit codes, None for errors."""

    context: CliContext
    """Execution context."""

    error: Exception | None = None
    """Exception if command failed."""

    @property
    def elapsed_ms(self) -> float:
        """Elapsed time in milliseconds."""
        return (time.perf_counter_ns() - self.context.started_ns) / 1_000_000

    @property
    def exit_code(self) -> int:
        """Compute exit code from result."""
        if self.error is not None:
            return 1
        if isinstance(self.result, int):
            return self.result
        if hasattr(self.result, "summary") and self.result.summary.get("error"):
            return 1
        return 0

    @property
    def is_success(self) -> bool:
        """True if command succeeded."""
        return self.exit_code == 0
```

#### 1.2 Type Definitions with Correct Enum Coercion (`types.py`)

```python
from __future__ import annotations

from enum import Enum
from typing import Annotated, TypeVar

from cyclopts import Parameter, validators

T = TypeVar("T", bound=Enum)


class OutputFormat(str, Enum):
    """
    Output format for cq results.

    CRITICAL: Member NAMES are the CLI tokens (cyclopts matches names, not values).
    """

    md = "md"
    json = "json"
    both = "both"
    summary = "summary"
    mermaid = "mermaid"
    mermaid_class = "mermaid-class"
    dot = "dot"
    mermaid_cfg = "mermaid-cfg"


class ImpactBucket(str, Enum):
    """Impact score buckets."""

    low = "low"
    med = "med"
    high = "high"


class ConfidenceBucket(str, Enum):
    """Confidence score buckets."""

    low = "low"
    med = "med"
    high = "high"


class SeverityLevel(str, Enum):
    """Finding severity levels."""

    error = "error"
    warning = "warning"
    info = "info"


class ReportPreset(str, Enum):
    """Report bundle presets."""

    refactor_impact = "refactor-impact"
    safety_reliability = "safety-reliability"
    change_propagation = "change-propagation"
    dependency_health = "dependency-health"


class BytecodeShow(str, Enum):
    """Bytecode surface display categories."""

    globals = "globals"
    attrs = "attrs"
    constants = "constants"
    opcodes = "opcodes"


def comma_separated_list(type_: type[T]):
    """
    Create a converter for comma-separated enum values.

    Allows: --impact high,med  OR  --impact high --impact med

    Parameters
    ----------
    type_
        Enum type to convert to.

    Returns
    -------
    Callable
        Converter function for cyclopts.
    """

    def convert(value: str | list[str]) -> list[T]:
        if isinstance(value, list):
            items = value
        else:
            items = [v.strip() for v in value.split(",") if v.strip()]

        result: list[T] = []
        for item in items:
            for sub in item.split(","):
                sub = sub.strip()
                if sub:
                    try:
                        result.append(type_[sub])
                    except KeyError:
                        valid = ", ".join(m.name for m in type_)
                        msg = f"Invalid value '{sub}'. Must be one of: {valid}"
                        raise ValueError(msg) from None
        return result

    convert.__cyclopts_converter__ = True
    return convert


# Type aliases for validated parameters
PositiveInt = Annotated[int, Parameter(validator=validators.Number(gte=1))]
AnalysisDepth = Annotated[int, Parameter(validator=validators.Number(gte=1, lte=10))]
```

#### 1.3 Shared Parameter Dataclasses (`params.py`)

```python
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter, validators

from tools.cq.cli_app.types import (
    ConfidenceBucket,
    ImpactBucket,
    OutputFormat,
    SeverityLevel,
    comma_separated_list,
)


def _get_default_root() -> Path | None:
    """Get default root from CQ_ROOT env var."""
    env_root = os.environ.get("CQ_ROOT")
    return Path(env_root) if env_root else None


def _get_default_artifact_dir() -> Path | None:
    """Get default artifact dir from CQ_ARTIFACT_DIR env var."""
    env_dir = os.environ.get("CQ_ARTIFACT_DIR")
    return Path(env_dir) if env_dir else None


@Parameter(name="*")
@dataclass
class CommonOptions:
    """Common options for all analysis commands."""

    root: Annotated[
        Path | None,
        Parameter(
            name=["--root", "-r"],
            help="Repository root (default: auto-detect from cwd, or CQ_ROOT env)",
        ),
    ] = field(default_factory=_get_default_root)

    output_format: Annotated[
        OutputFormat,
        Parameter(
            name=["--format", "-f"],
            help="Output format for results",
        ),
    ] = OutputFormat.md

    artifact_dir: Annotated[
        Path | None,
        Parameter(
            name="--artifact-dir",
            help="Directory for JSON artifacts (default: .cq/artifacts, or CQ_ARTIFACT_DIR env)",
        ),
    ] = field(default_factory=_get_default_artifact_dir)

    no_save_artifact: Annotated[
        bool,
        Parameter(
            name="--no-save-artifact",
            help="Skip saving JSON artifact",
            negative="",
        ),
    ] = False


@Parameter(name="*")
@dataclass
class FilterOptions:
    """Filtering options for analysis results."""

    impact: Annotated[
        list[ImpactBucket],
        Parameter(
            name="--impact",
            help="Filter by impact bucket (low, med, high). Comma-separated or repeated.",
            converter=comma_separated_list(ImpactBucket),
        ),
    ] = field(default_factory=list)

    confidence: Annotated[
        list[ConfidenceBucket],
        Parameter(
            name="--confidence",
            help="Filter by confidence bucket (low, med, high). Comma-separated or repeated.",
            converter=comma_separated_list(ConfidenceBucket),
        ),
    ] = field(default_factory=list)

    severity: Annotated[
        list[SeverityLevel],
        Parameter(
            name="--severity",
            help="Filter by severity (error, warning, info). Comma-separated or repeated.",
            converter=comma_separated_list(SeverityLevel),
        ),
    ] = field(default_factory=list)

    include: Annotated[
        list[str],
        Parameter(
            name=["--include", "-i"],
            help="Include files matching pattern (glob or ~regex, repeatable)",
        ),
    ] = field(default_factory=list)

    exclude: Annotated[
        list[str],
        Parameter(
            name=["--exclude", "-e"],
            help="Exclude files matching pattern (glob or ~regex, repeatable)",
        ),
    ] = field(default_factory=list)

    limit: Annotated[
        int | None,
        Parameter(
            name=["--limit", "-n"],
            help="Maximum number of findings",
            validator=validators.Number(gte=1),
        ),
    ] = None


@Parameter(name="*")
@dataclass
class CacheOptions:
    """Caching options for query commands."""

    use_cache: Annotated[
        bool,
        Parameter(
            name="--cache/--no-cache",
            help="Enable/disable query result caching (default: enabled)",
        ),
    ] = True
```

#### 1.4 Validators (`validators.py`)

```python
from __future__ import annotations

from pathlib import Path
from typing import Annotated

from cyclopts import Parameter, validators


class ExistingPath(validators.Path):
    """Validator for paths that must exist."""

    def __init__(self) -> None:
        super().__init__(exists=True)


class ExistingDirectory(validators.Path):
    """Validator for directories that must exist."""

    def __init__(self) -> None:
        super().__init__(exists=True, file_okay=False)


class QueryDepth(validators.Number):
    """Validator for query expansion depth."""

    def __init__(self) -> None:
        super().__init__(gte=1, lte=10)


class PositiveLimit(validators.Number):
    """Validator for positive limit values."""

    def __init__(self) -> None:
        super().__init__(gte=1)


# Type aliases for common validated parameters
RepoRoot = Annotated[Path | None, Parameter(validator=ExistingDirectory())]
AnalysisDepth = Annotated[int, Parameter(validator=QueryDepth())]
FindingLimit = Annotated[int | None, Parameter(validator=PositiveLimit())]
```

#### 1.5 Package Init (`__init__.py`)

```python
"""
CQ CLI Application.

This package provides the cyclopts-based CLI for cq.
"""
from __future__ import annotations

from tools.cq.cli_app.app import app, main
from tools.cq.cli_app.context import CliContext, CliResult

__all__ = ["app", "main", "CliContext", "CliResult"]
```

### Files to Create

| File | Purpose | Lines (est.) |
|------|---------|--------------|
| `tools/cq/cli_app/__init__.py` | Package exports | 15 |
| `tools/cq/cli_app/context.py` | `CliContext`, `CliResult` | 80 |
| `tools/cq/cli_app/types.py` | Enums, converters | 100 |
| `tools/cq/cli_app/params.py` | Shared dataclasses | 120 |
| `tools/cq/cli_app/validators.py` | Custom validators | 50 |

### Code to Deprecate/Delete After Scope 1

None yet - this scope only creates new files.

### Implementation Checklist

- [ ] Create `tools/cq/cli_app/` directory
- [ ] Add `cyclopts>=3.0` to `pyproject.toml` dependencies
- [ ] Implement `context.py` with `CliContext` and `CliResult`
- [ ] Implement `types.py` with all enums (member names = CLI tokens)
- [ ] Implement `comma_separated_list()` converter
- [ ] Implement `params.py` with `CommonOptions`, `FilterOptions`, `CacheOptions`
- [ ] Implement `validators.py` with custom validator classes
- [ ] Create `__init__.py` with public exports
- [ ] Unit tests for `CliContext.build()`
- [ ] Unit tests for `CliResult.exit_code` and `elapsed_ms`
- [ ] Unit tests for `comma_separated_list()` converter
- [ ] Unit tests for enum member name matching
- [ ] Run quality gate: `uv run ruff format && uv run ruff check --fix && uv run pyrefly check`

---

## Scope 2: App Core and Meta-App

### Overview

Create the root application with meta-app pattern for context injection, deterministic console settings, and lazy command registration.

### Key Architectural Elements

1. **Meta-App Pattern**: `@app.meta.default` intercepts all commands to inject `CliContext`
2. **Explicit `name_transform`**: Lambda for underscore → hyphen conversion
3. **Deterministic Console**: Fixed width, controlled color for reproducible help
4. **Lazy Loading**: String import paths for fast startup
5. **Command Groups**: Stable sort order via `sort_key`

### Representative Code Snippets

#### 2.1 Root Application (`app.py`)

```python
from __future__ import annotations

import os
import sys
from typing import Annotated

from cyclopts import App, Group, Parameter
from rich.console import Console

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.result import cq_result_action


def _make_console() -> Console:
    """
    Create console with deterministic settings for stable help output.

    Force width=100 and no color for CI/testing reproducibility.
    """
    force_color = os.environ.get("CQ_FORCE_COLOR", "").lower() in ("1", "true")

    return Console(
        width=100,
        force_terminal=force_color,
        color_system="auto" if force_color else None,
        highlight=False,
    )


app = App(
    name="cq",
    help="Code Query - High-signal code analysis for Python",
    version="0.3.0",
    version_flags=["--version", "-V"],
    help_format="rich",
    console=_make_console(),
    result_action=cq_result_action,
    exit_on_error=True,
    print_error=True,
    name_transform=lambda s: s.replace("_", "-"),
)

# Groups for help organization with stable sort order
analysis_group = Group("Analysis Commands", sort_key=0)
admin_group = Group("Admin Commands", sort_key=100)


@app.meta.default
def meta_launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
) -> CliResult:
    """
    Meta-app launcher that injects CliContext into commands.

    Commands declare `ctx: Annotated[CliContext, Parameter(parse=False)]` to receive
    the shared execution context.
    """
    command, bound, ignored = app.parse_args(tokens)

    argv = list(tokens)

    # Extract root override if present in bound args
    root_override = None
    if "common" in bound.kwargs and hasattr(bound.kwargs["common"], "root"):
        root_override = bound.kwargs["common"].root

    context = CliContext.build(argv=argv, root=root_override)

    # Inject context if command expects it
    extra_kwargs: dict[str, object] = {}
    if "ctx" in ignored:
        extra_kwargs["ctx"] = context

    try:
        result = command(*bound.args, **bound.kwargs, **extra_kwargs)

        if isinstance(result, CliResult):
            return result
        return CliResult(result=result, context=context)

    except Exception as e:
        return CliResult(result=None, context=context, error=e)


# -----------------------------------------------------------------------------
# Command Registration (Lazy Loading)
# -----------------------------------------------------------------------------

# Analysis macros
app.command("tools.cq.cli_app.commands.macros:impact", group=analysis_group)
app.command("tools.cq.cli_app.commands.macros:calls", group=analysis_group)
app.command("tools.cq.cli_app.commands.macros:imports", group=analysis_group)
app.command("tools.cq.cli_app.commands.macros:exceptions", group=analysis_group)
app.command(
    "tools.cq.cli_app.commands.macros:sig_impact",
    name="sig-impact",
    group=analysis_group,
)
app.command(
    "tools.cq.cli_app.commands.macros:side_effects",
    name="side-effects",
    group=analysis_group,
)
app.command("tools.cq.cli_app.commands.macros:scopes", group=analysis_group)
app.command(
    "tools.cq.cli_app.commands.macros:async_hazards",
    name="async-hazards",
    group=analysis_group,
)
app.command(
    "tools.cq.cli_app.commands.macros:bytecode_surface",
    name="bytecode-surface",
    group=analysis_group,
)

# Query command
app.command("tools.cq.cli_app.commands.query:q", group=analysis_group)

# Report bundles
app.command("tools.cq.cli_app.commands.report:report", group=analysis_group)

# Admin commands
app.command("tools.cq.cli_app.commands.admin:index", group=admin_group)
app.command("tools.cq.cli_app.commands.admin:cache", group=admin_group)


def main(argv: list[str] | None = None) -> int:
    """
    Main entry point for cq CLI.

    Returns exit code (0 for success, non-zero for errors).
    """
    if argv is None:
        argv = sys.argv[1:]

    result = app(argv)

    if isinstance(result, CliResult):
        return result.exit_code
    if isinstance(result, int):
        return result
    return 0
```

#### 2.2 Result Action Pipeline (`result.py`)

```python
from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult

from tools.cq.cli_app.context import CliResult
from tools.cq.cli_app.types import OutputFormat


def render_result(result: CqResult, format_: OutputFormat) -> str:
    """Render a CqResult to the specified format."""
    from tools.cq.core.renderers import (
        render_dot,
        render_mermaid_cfg,
        render_mermaid_class_diagram,
        render_mermaid_flowchart,
    )
    from tools.cq.core.report import render_markdown, render_summary

    match format_:
        case OutputFormat.json:
            return json.dumps(result.to_dict(), indent=2)
        case OutputFormat.md:
            return render_markdown(result)
        case OutputFormat.summary:
            return render_summary(result)
        case OutputFormat.both:
            md = render_markdown(result)
            js = json.dumps(result.to_dict(), indent=2)
            return f"{md}\n\n---\n\n{js}"
        case OutputFormat.mermaid:
            return render_mermaid_flowchart(result)
        case OutputFormat.mermaid_class:
            return render_mermaid_class_diagram(result)
        case OutputFormat.mermaid_cfg:
            return render_mermaid_cfg(result)
        case OutputFormat.dot:
            return render_dot(result)
        case _:
            return render_markdown(result)


def apply_filters(result: CqResult, filters) -> CqResult:
    """Apply FilterOptions to CqResult findings."""
    from dataclasses import replace

    from tools.cq.cli_app.params import FilterOptions

    if not isinstance(filters, FilterOptions):
        return result

    findings = list(result.findings)

    if filters.impact:
        impact_values = {b.value for b in filters.impact}
        findings = [f for f in findings if f.get("impact_bucket") in impact_values]

    if filters.confidence:
        conf_values = {b.value for b in filters.confidence}
        findings = [f for f in findings if f.get("confidence_bucket") in conf_values]

    if filters.severity:
        sev_values = {s.value for s in filters.severity}
        findings = [f for f in findings if f.get("severity") in sev_values]

    if filters.limit and len(findings) > filters.limit:
        findings = findings[: filters.limit]

    return replace(result, findings=findings)


def cq_result_action(result: Any) -> int:
    """
    Custom result action for cq commands.

    Handles CliResult envelope by:
    1. Applying filters from FilterOptions
    2. Rendering to requested format
    3. Printing to stdout
    4. Returning exit code
    """
    if isinstance(result, CliResult):
        if result.error is not None:
            sys.stderr.write(f"Error: {result.error}\n")
            return result.exit_code

        inner = result.result
    else:
        inner = result

    if isinstance(inner, int):
        return inner

    if inner is None:
        return 0

    # Check if it's a CqResult by duck typing
    if not hasattr(inner, "to_dict"):
        if inner is not None:
            print(inner)
        return 0

    output = render_result(inner, OutputFormat.md)
    sys.stdout.write(f"{output}\n")

    if hasattr(inner, "summary") and inner.summary.get("error"):
        return 1

    return 0
```

### Files to Create

| File | Purpose | Lines (est.) |
|------|---------|--------------|
| `tools/cq/cli_app/app.py` | Root app, meta-app, command registration | 150 |
| `tools/cq/cli_app/result.py` | Result action pipeline | 100 |
| `tools/cq/cli_app/commands/__init__.py` | Commands package | 5 |

### Code to Deprecate/Delete After Scope 2

None yet - commands not implemented.

### Implementation Checklist

- [ ] Implement `app.py` with `App` configuration
- [ ] Add `_make_console()` with deterministic settings
- [ ] Implement `@app.meta.default` meta-launcher
- [ ] Add command registration with lazy loading strings
- [ ] Add `name_transform` for kebab-case
- [ ] Add `Group` objects with `sort_key`
- [ ] Implement `result.py` with `render_result()` and `apply_filters()`
- [ ] Implement `cq_result_action()` for result pipeline
- [ ] Create `commands/__init__.py`
- [ ] Unit tests for `_make_console()` determinism
- [ ] Unit tests for meta-launcher context injection
- [ ] Unit tests for `render_result()` format dispatch
- [ ] Test `--help` output is deterministic
- [ ] Benchmark startup time: `time cq --help` < 100ms
- [ ] Run quality gate

---

## Scope 3: Admin Commands

### Overview

Migrate the `index` and `cache` admin commands with mutually exclusive action groups.

### Key Architectural Elements

1. **Admin Group**: Mutually exclusive `--rebuild`/`--stats`/`--clear` flags
2. **Return Type**: `int` for exit codes, not `CqResult`
3. **Context Injection**: `ctx: Annotated[CliContext, Parameter(parse=False)]`

### Representative Code Snippets

#### 3.1 Admin Commands (`commands/admin.py`)

```python
from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

from cyclopts import Group, Parameter

from tools.cq.cli_app.context import CliContext

admin_actions = Group("Actions", default_parameter=Parameter(negative=""))


def index(
    *,
    rebuild: Annotated[
        bool, Parameter(group=admin_actions, help="Force full rebuild")
    ] = False,
    stats: Annotated[
        bool, Parameter(group=admin_actions, help="Show statistics")
    ] = False,
    clear: Annotated[
        bool, Parameter(group=admin_actions, help="Clear the index")
    ] = False,
    root: Annotated[
        Path | None, Parameter(help="Repository root (default: auto-detect)")
    ] = None,
    ctx: Annotated[CliContext, Parameter(parse=False)],
) -> int:
    """
    Manage the ast-grep scan index cache.

    The index caches ast-grep scan results and only rescans files that have changed.

    Examples:
        cq index              # Update index for changed files
        cq index --rebuild    # Full rebuild
        cq index --stats      # Show statistics
        cq index --clear      # Clear index
    """
    from tools.cq.index.diskcache_index_cache import IndexCache

    actual_root = root or ctx.repo.repo_root
    rule_version = ctx.toolchain.sg_version or "unknown"

    with IndexCache(actual_root, rule_version) as cache:
        if clear:
            cache.clear()
            sys.stdout.write("Index cleared.\n")
            return 0

        if stats:
            s = cache.get_stats()
            sys.stdout.write("Index Statistics:\n")
            sys.stdout.write(f"  Files cached: {s.total_files}\n")
            sys.stdout.write(f"  Total records: {s.total_records}\n")
            sys.stdout.write(f"  Rule version: {s.rule_version}\n")
            sys.stdout.write(f"  Database size: {s.database_size_bytes:,} bytes\n")
            return 0

        if rebuild:
            cache.clear()
            sys.stdout.write("Index cleared for rebuild.\n")

        # Full index update logic (imported lazily)
        from tools.cq.index.files import build_repo_file_index, tabulate_files
        from tools.cq.query.sg_parser import ALL_RECORD_TYPES, group_records_by_file, sg_scan

        repo_index = build_repo_file_index(ctx.repo)
        file_result = tabulate_files(
            repo_index,
            [actual_root],
            None,
            extensions=(".py",),
        )
        py_files = file_result.files

        sys.stdout.write(f"Found {len(py_files)} Python files.\n")

        files_to_scan = [p for p in py_files if cache.needs_rescan(p)]

        if not files_to_scan:
            sys.stdout.write("Index is up to date.\n")
            return 0

        sys.stdout.write(f"Scanning {len(files_to_scan)} changed files...\n")

        batch_size = 100
        total_records = 0

        for i in range(0, len(files_to_scan), batch_size):
            batch = files_to_scan[i : i + batch_size]
            records = sg_scan(batch, root=actual_root)
            records_by_file = group_records_by_file(records)

            for file_path_str, file_records in records_by_file.items():
                records_data = [
                    {
                        "record": r.record,
                        "kind": r.kind,
                        "file": r.file,
                        "start_line": r.start_line,
                        "start_col": r.start_col,
                        "end_line": r.end_line,
                        "end_col": r.end_col,
                        "text": r.text,
                        "rule_id": r.rule_id,
                    }
                    for r in file_records
                ]
                file_path = Path(file_path_str)
                cache.store(file_path, records_data)
                total_records += len(records_data)

            # Mark files with no matches
            for file_path in batch:
                if str(file_path) not in records_by_file:
                    cache.store(file_path, [])

        sys.stdout.write(f"Indexed {total_records} records from {len(files_to_scan)} files.\n")
        return 0


def cache(
    *,
    stats: Annotated[
        bool, Parameter(group=admin_actions, help="Show statistics")
    ] = False,
    clear: Annotated[
        bool, Parameter(group=admin_actions, help="Clear the cache")
    ] = False,
    root: Annotated[
        Path | None, Parameter(help="Repository root (default: auto-detect)")
    ] = None,
    ctx: Annotated[CliContext, Parameter(parse=False)],
) -> int:
    """
    Manage the query result cache.

    The cache stores query results and invalidates them when source files change.

    Examples:
        cq cache --stats    # Show statistics
        cq cache --clear    # Clear cache
    """
    from tools.cq.index.diskcache_query_cache import QueryCache

    actual_root = root or ctx.repo.repo_root
    cache_dir = actual_root / ".cq" / "cache"

    with QueryCache(cache_dir) as qcache:
        if clear:
            qcache.clear()
            sys.stdout.write("Query cache cleared.\n")
            return 0

        s = qcache.stats()
        sys.stdout.write("Query Cache Statistics:\n")
        sys.stdout.write(f"  Total entries: {s.total_entries}\n")
        sys.stdout.write(f"  Unique files: {s.unique_files}\n")
        sys.stdout.write(f"  Database size: {s.database_size_bytes:,} bytes\n")
        return 0
```

### Files to Create

| File | Purpose | Lines (est.) |
|------|---------|--------------|
| `tools/cq/cli_app/commands/admin.py` | Admin commands | 150 |

### Code to Deprecate After Scope 3

After admin commands work via cyclopts:

| Code | Location | Replacement |
|------|----------|-------------|
| `_cmd_index_cli()` | `cli.py:409-519` | `commands/admin.py::index()` |
| `_cmd_cache_cli()` | `cli.py:522-545` | `commands/admin.py::cache()` |
| Index subparser | `cli.py:942-979` | Lazy loading in `app.py` |
| Cache subparser | `cli.py:981-1009` | Lazy loading in `app.py` |

### Implementation Checklist

- [ ] Implement `commands/admin.py` with `index()` and `cache()` functions
- [ ] Add mutually exclusive group for admin actions
- [ ] Use `Parameter(parse=False)` for context injection
- [ ] Port full index rebuild logic from `_cmd_index_cli()`
- [ ] Port cache stats/clear from `_cmd_cache_cli()`
- [ ] Unit tests for `index --stats`
- [ ] Unit tests for `index --clear`
- [ ] Unit tests for `cache --stats`
- [ ] Unit tests for `cache --clear`
- [ ] Integration test: `cq index --rebuild` on test repo
- [ ] Verify output matches original CLI
- [ ] Run quality gate

---

## Scope 4: Macro Commands

### Overview

Migrate all 9 macro commands (impact, calls, imports, exceptions, sig-impact, side-effects, scopes, async-hazards, bytecode-surface).

### Key Architectural Elements

1. **Shared Dataclasses**: `CommonOptions`, `FilterOptions` flattened via `@Parameter(name="*")`
2. **Context Injection**: `ctx: Annotated[CliContext, Parameter(parse=False)]`
3. **Lazy Imports**: Macro implementations imported inside function bodies
4. **Type Aliases**: `AnalysisDepth` for validated depth parameter

### Representative Code Snippets

#### 4.1 Macro Commands (`commands/macros.py`)

```python
from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext
from tools.cq.cli_app.params import CommonOptions, FilterOptions
from tools.cq.cli_app.types import AnalysisDepth, BytecodeShow, comma_separated_list


def impact(
    function: Annotated[str, Parameter(help="Function name to analyze")],
    *,
    param: Annotated[str, Parameter(help="Parameter name to trace")],
    depth: AnalysisDepth = 5,
    common: CommonOptions,
    filters: FilterOptions,
    ctx: Annotated[CliContext, Parameter(parse=False)],
):
    """
    Trace data flow from a function parameter.

    Performs taint analysis to identify downstream consumers of a parameter.

    Examples:
        cq impact build_graph_product --param repo_root
        cq impact DefIndex.build --param root --depth 10
    """
    from tools.cq.macros.impact import ImpactRequest, cmd_impact

    request = ImpactRequest(
        tc=ctx.toolchain,
        root=ctx.repo.repo_root,
        argv=list(ctx.argv),
        function_name=function,
        param_name=param,
        max_depth=depth,
    )
    return cmd_impact(request)


def calls(
    function: Annotated[str, Parameter(help="Function name to find calls for")],
    *,
    common: CommonOptions,
    filters: FilterOptions,
    ctx: Annotated[CliContext, Parameter(parse=False)],
):
    """
    Census all call sites for a function.

    Finds all locations where a function is called, with argument analysis.

    Examples:
        cq calls DefIndex.build
        cq calls render_markdown --format json
    """
    from tools.cq.macros.calls import cmd_calls

    return cmd_calls(
        tc=ctx.toolchain,
        root=ctx.repo.repo_root,
        argv=list(ctx.argv),
        function_name=function,
    )


def imports(
    *,
    cycles: Annotated[bool, Parameter(help="Detect import cycles")] = False,
    module: Annotated[str | None, Parameter(help="Focus on specific module")] = None,
    common: CommonOptions,
    filters: FilterOptions,
    ctx: Annotated[CliContext, Parameter(parse=False)],
):
    """
    Analyze import structure and dependencies.

    Examples:
        cq imports --cycles
        cq imports --module src.semantics
    """
    from tools.cq.macros.imports import ImportRequest, cmd_imports

    request = ImportRequest(
        tc=ctx.toolchain,
        root=ctx.repo.repo_root,
        argv=list(ctx.argv),
        cycles=cycles,
        module=module,
    )
    return cmd_imports(request)


def exceptions(
    *,
    function: Annotated[str | None, Parameter(help="Focus on specific function")] = None,
    common: CommonOptions,
    filters: FilterOptions,
    ctx: Annotated[CliContext, Parameter(parse=False)],
):
    """
    Analyze exception handling patterns.

    Examples:
        cq exceptions
        cq exceptions --function build_graph
    """
    from tools.cq.macros.exceptions import cmd_exceptions

    return cmd_exceptions(
        tc=ctx.toolchain,
        root=ctx.repo.repo_root,
        argv=list(ctx.argv),
        function=function,
    )


def sig_impact(
    symbol: Annotated[str, Parameter(help="Function name to analyze")],
    *,
    to: Annotated[str, Parameter(help='New signature (e.g., "foo(a, b, *, c=None)")')],
    common: CommonOptions,
    filters: FilterOptions,
    ctx: Annotated[CliContext, Parameter(parse=False)],
):
    """
    Analyze impact of a signature change.

    Classifies call sites as would_break, ambiguous, or ok for a proposed change.

    Examples:
        cq sig-impact foo --to "foo(a, *, b=None)"
        cq sig-impact Config.validate --to "validate(self, strict: bool = True)"
    """
    from tools.cq.macros.sig_impact import SigImpactRequest, cmd_sig_impact

    request = SigImpactRequest(
        tc=ctx.toolchain,
        root=ctx.repo.repo_root,
        argv=list(ctx.argv),
        symbol=symbol,
        to=to,
    )
    return cmd_sig_impact(request)


def side_effects(
    *,
    max_files: Annotated[int, Parameter(help="Maximum files to analyze")] = 100,
    common: CommonOptions,
    filters: FilterOptions,
    ctx: Annotated[CliContext, Parameter(parse=False)],
):
    """
    Detect functions with side effects.

    Examples:
        cq side-effects
        cq side-effects --max-files 50
    """
    from tools.cq.macros.side_effects import SideEffectsRequest, cmd_side_effects

    request = SideEffectsRequest(
        tc=ctx.toolchain,
        root=ctx.repo.repo_root,
        argv=list(ctx.argv),
        max_files=max_files,
    )
    return cmd_side_effects(request)


def scopes(
    target: Annotated[str, Parameter(help="File path or symbol name to analyze")],
    *,
    common: CommonOptions,
    filters: FilterOptions,
    ctx: Annotated[CliContext, Parameter(parse=False)],
):
    """
    Analyze scope capture (closures).

    Uses symtable to analyze free vars, cell vars, nonlocals, and globals.

    Examples:
        cq scopes tools/cq/macros/impact.py
        cq scopes TaintVisitor
    """
    from tools.cq.macros.scopes import ScopeRequest, cmd_scopes

    request = ScopeRequest(
        tc=ctx.toolchain,
        root=ctx.repo.repo_root,
        argv=list(ctx.argv),
        target=target,
    )
    return cmd_scopes(request)


def async_hazards(
    *,
    profiles: Annotated[str, Parameter(help="Additional blocking patterns")] = "",
    common: CommonOptions,
    filters: FilterOptions,
    ctx: Annotated[CliContext, Parameter(parse=False)],
):
    """
    Detect async/await hazards and anti-patterns.

    Examples:
        cq async-hazards
        cq async-hazards --profiles time.sleep,requests.get
    """
    from tools.cq.macros.async_hazards import AsyncHazardsRequest, cmd_async_hazards

    request = AsyncHazardsRequest(
        tc=ctx.toolchain,
        root=ctx.repo.repo_root,
        argv=list(ctx.argv),
        profiles=profiles,
    )
    return cmd_async_hazards(request)


def bytecode_surface(
    target: Annotated[str, Parameter(help="File path or symbol name to analyze")],
    *,
    show: Annotated[
        str,
        Parameter(help="What to show (globals,attrs,constants,opcodes)"),
    ] = "globals,attrs,constants",
    common: CommonOptions,
    filters: FilterOptions,
    ctx: Annotated[CliContext, Parameter(parse=False)],
):
    """
    Analyze bytecode for hidden dependencies.

    Inspects bytecode for globals, attributes, and constants not visible in imports.

    Examples:
        cq bytecode-surface tools/cq/macros/calls.py
        cq bytecode-surface DefIndex.build --show globals,attrs
    """
    from tools.cq.macros.bytecode import BytecodeSurfaceRequest, cmd_bytecode_surface

    request = BytecodeSurfaceRequest(
        tc=ctx.toolchain,
        root=ctx.repo.repo_root,
        argv=list(ctx.argv),
        target=target,
        show=show,
    )
    return cmd_bytecode_surface(request)
```

### Files to Create

| File | Purpose | Lines (est.) |
|------|---------|--------------|
| `tools/cq/cli_app/commands/macros.py` | All 9 macro commands | 300 |

### Code to Deprecate After Scope 4

| Code | Location | Replacement |
|------|----------|-------------|
| `_cmd_impact_cli()` | `cli.py:244-261` | `commands/macros.py::impact()` |
| `_cmd_calls_cli()` | `cli.py:264-278` | `commands/macros.py::calls()` |
| `_cmd_imports_cli()` | `cli.py:281-297` | `commands/macros.py::imports()` |
| `_cmd_exceptions_cli()` | `cli.py:300-314` | `commands/macros.py::exceptions()` |
| `_cmd_sig_impact_cli()` | `cli.py:317-333` | `commands/macros.py::sig_impact()` |
| `_cmd_side_effects_cli()` | `cli.py:336-351` | `commands/macros.py::side_effects()` |
| `_cmd_scopes_cli()` | `cli.py:354-369` | `commands/macros.py::scopes()` |
| `_cmd_async_hazards_cli()` | `cli.py:372-387` | `commands/macros.py::async_hazards()` |
| `_cmd_bytecode_cli()` | `cli.py:390-406` | `commands/macros.py::bytecode_surface()` |
| All macro subparsers | `cli.py:702-834` | Lazy loading in `app.py` |

### Implementation Checklist

- [ ] Implement all 9 macro functions in `commands/macros.py`
- [ ] Verify docstrings have Examples section
- [ ] Unit tests for each command's parsing
- [ ] Test `common: CommonOptions` flattening works
- [ ] Test `filters: FilterOptions` flattening works
- [ ] Test depth validator rejects invalid values
- [ ] Integration test: `cq impact foo --param x` matches original output
- [ ] Integration test: `cq calls Toolchain.detect` matches original output
- [ ] Integration test: `cq sig-impact foo --to "foo(a, b)"` matches original
- [ ] Run quality gate

---

## Scope 5: Query and Report Commands

### Overview

Migrate the `q` (query) and `report` commands with their complex option sets.

### Key Architectural Elements

1. **Extended Help**: Multi-line docstrings with query syntax documentation
2. **Cache Options**: `CacheOptions` dataclass for `--cache/--no-cache`
3. **Error Handling**: Parse errors return `CqResult` with error in summary
4. **Lazy Cache Init**: Only initialize caches when caching enabled

### Representative Code Snippets

#### 5.1 Query Command (`commands/query.py`)

```python
from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext
from tools.cq.cli_app.params import CacheOptions, CommonOptions, FilterOptions


def q(
    query_string: Annotated[
        str,
        Parameter(
            help='Query string (e.g., "entity=function name=foo")',
            show=True,
        ),
    ],
    *,
    explain_files: Annotated[
        bool,
        Parameter(
            name="--explain-files",
            help="Include file filtering diagnostics in summary",
        ),
    ] = False,
    common: CommonOptions,
    filters: FilterOptions,
    cache: CacheOptions,
    ctx: Annotated[CliContext, Parameter(parse=False)],
):
    """
    Declarative code query using ast-grep.

    Query syntax: key=value pairs separated by spaces.

    Required:
        entity=TYPE    Entity type (function, class, method, module, callsite, import)

    Optional:
        name=PATTERN   Name to match (exact or ~regex)
        expand=KIND    Graph expansion (callers, callees, imports, raises, scope)
                       Use KIND(depth=N) to set depth (default: 1)
        in=DIR         Search only in directory
        exclude=DIRS   Exclude directories (comma-separated)
        fields=FIELDS  Output fields (def,loc,callers,callees,evidence,hazards)
        limit=N        Maximum results
        explain=true   Include query plan explanation
        pattern='...'  ast-grep pattern for structural search
        inside='...'   Containing context pattern
        has='...'      Nested pattern requirement
        scope=TYPE     Scope filter (module, function, class, closure)

    Examples:
        cq q "entity=function name=build_graph_product"
        cq q "entity=function name=detect expand=callers(depth=2) in=tools/cq/"
        cq q "entity=class in=src/relspec/ fields=def,hazards"
        cq q "pattern='getattr(\\$X, \\$Y)'"
        cq q "entity=function scope=closure in=src/semantics/"
    """
    from dataclasses import replace

    from tools.cq.index.diskcache_query_cache import QueryCache
    from tools.cq.index.diskcache_index_cache import IndexCache
    from tools.cq.query.executor import execute_plan
    from tools.cq.query.parser import QueryParseError, parse_query
    from tools.cq.query.planner import compile_query

    try:
        query = parse_query(query_string)
    except QueryParseError as e:
        from tools.cq.core.schema import mk_result, mk_runmeta, ms

        started_ms = ms()
        run = mk_runmeta(
            macro="q",
            argv=list(ctx.argv),
            root=str(ctx.repo.repo_root),
            started_ms=started_ms,
            toolchain=ctx.toolchain.to_dict(),
        )
        result = mk_result(run)
        result.summary["error"] = str(e)
        return result

    if explain_files and not query.explain:
        query = replace(query, explain=True)

    plan = compile_query(query)

    if not cache.use_cache:
        return execute_plan(
            plan=plan,
            query=query,
            tc=ctx.toolchain,
            root=ctx.repo.repo_root,
            argv=list(ctx.argv),
            use_cache=False,
        )

    rule_version = ctx.toolchain.sg_version or "unknown"
    with (
        IndexCache(ctx.repo.repo_root, rule_version) as index_cache,
        QueryCache(ctx.repo.repo_root / ".cq" / "cache") as query_cache,
    ):
        return execute_plan(
            plan=plan,
            query=query,
            tc=ctx.toolchain,
            root=ctx.repo.repo_root,
            argv=list(ctx.argv),
            index_cache=index_cache,
            query_cache=query_cache,
            use_cache=True,
        )
```

#### 5.2 Report Command (`commands/report.py`)

```python
from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext
from tools.cq.cli_app.params import CacheOptions, CommonOptions, FilterOptions
from tools.cq.cli_app.types import ReportPreset


def report(
    preset: Annotated[ReportPreset, Parameter(help="Report preset to run")],
    *,
    target: Annotated[
        str,
        Parameter(
            help="Target spec (function:foo, class:Bar, module:pkg.mod, path:src/...)",
        ),
    ],
    in_dir: Annotated[
        str | None,
        Parameter(
            name="--in",
            help="Restrict analysis to a directory",
        ),
    ] = None,
    param: Annotated[
        str | None,
        Parameter(help="Parameter name for impact analysis"),
    ] = None,
    signature: Annotated[
        str | None,
        Parameter(
            name="--to",
            help="Proposed signature for sig-impact analysis",
        ),
    ] = None,
    bytecode_show: Annotated[
        str | None,
        Parameter(
            name="--bytecode-show",
            help="Bytecode surface fields (globals,attrs,constants,opcodes)",
        ),
    ] = None,
    common: CommonOptions,
    filters: FilterOptions,
    cache: CacheOptions,
    ctx: Annotated[CliContext, Parameter(parse=False)],
):
    """
    Run target-scoped report bundles.

    Report presets combine multiple analysis commands for comprehensive reports.

    Examples:
        cq report refactor-impact --target function:build_graph --param repo_root
        cq report safety-reliability --target module:src.extract
        cq report change-propagation --target class:SemanticCompiler
    """
    from tools.cq.core.bundles import BundleContext, parse_target_spec, run_bundle
    from tools.cq.index.diskcache_query_cache import QueryCache
    from tools.cq.index.diskcache_index_cache import IndexCache

    try:
        target_spec = parse_target_spec(target)
    except ValueError as exc:
        from tools.cq.core.schema import mk_result, mk_runmeta, ms

        started_ms = ms()
        run = mk_runmeta(
            "report",
            argv=list(ctx.argv),
            root=str(ctx.repo.repo_root),
            started_ms=started_ms,
            toolchain=ctx.toolchain.to_dict(),
        )
        result = mk_result(run)
        result.summary["error"] = str(exc)
        return result

    use_cache = cache.use_cache
    index_cache: IndexCache | None = None
    query_cache: QueryCache | None = None

    if use_cache:
        rule_version = ctx.toolchain.sg_version or "unknown"
        index_cache = IndexCache(ctx.repo.repo_root, rule_version)
        index_cache.initialize()
        query_cache = QueryCache(ctx.repo.repo_root / ".cq" / "cache")

    bundle_ctx = BundleContext(
        tc=ctx.toolchain,
        root=ctx.repo.repo_root,
        argv=list(ctx.argv),
        target=target_spec,
        in_dir=in_dir,
        param=param,
        signature=signature,
        bytecode_show=bytecode_show,
        use_cache=use_cache,
        index_cache=index_cache,
        query_cache=query_cache,
    )

    if index_cache is None or query_cache is None:
        return run_bundle(preset.value, bundle_ctx)

    with index_cache, query_cache:
        return run_bundle(preset.value, bundle_ctx)
```

### Files to Create

| File | Purpose | Lines (est.) |
|------|---------|--------------|
| `tools/cq/cli_app/commands/query.py` | Query command | 100 |
| `tools/cq/cli_app/commands/report.py` | Report command | 100 |

### Code to Deprecate After Scope 5

| Code | Location | Replacement |
|------|----------|-------------|
| `_cmd_query_cli()` | `cli.py:548-625` | `commands/query.py::q()` |
| `_cmd_report_cli()` | `cli.py:628-675` | `commands/report.py::report()` |
| Query subparser | `cli.py:837-886` | Lazy loading in `app.py` |
| Report subparser | `cli.py:888-939` | Lazy loading in `app.py` |

### Implementation Checklist

- [ ] Implement `commands/query.py` with `q()` function
- [ ] Implement `commands/report.py` with `report()` function
- [ ] Verify query docstring has full syntax documentation
- [ ] Test parse error handling returns proper `CqResult`
- [ ] Test `--cache/--no-cache` toggle
- [ ] Test `--explain-files` flag
- [ ] Integration test: `cq q "entity=function name=main"` matches original
- [ ] Integration test: report command with all preset values
- [ ] Run quality gate

---

## Scope 6: Entry Points and Cleanup

### Overview

Create thin entry point wrappers and remove deprecated code from `cli.py`.

### Key Architectural Elements

1. **Thin Wrappers**: `cli.py` and `__main__.py` just import and call `main()`
2. **Single Entrypoint**: All logic in `cli_app/app.py::main()`
3. **Clean Deletion**: Remove all deprecated functions and argparse setup

### Representative Code Snippets

#### 6.1 New `cli.py` (Thin Wrapper)

```python
"""
CQ CLI entry point.

This module provides backward-compatible entry point that delegates to cli_app/.
"""
from __future__ import annotations

from tools.cq.cli_app import main

if __name__ == "__main__":
    raise SystemExit(main())
```

#### 6.2 New `__main__.py`

```python
"""
Entry point for `python -m tools.cq`.
"""
from __future__ import annotations

from tools.cq.cli_app import main

if __name__ == "__main__":
    raise SystemExit(main())
```

### Files to Modify

| File | Change |
|------|--------|
| `tools/cq/cli.py` | Replace 1,094 lines with 12-line wrapper |
| `tools/cq/__main__.py` | Update to import from `cli_app` |

### Code to Delete in Scope 6

**Delete from `cli.py`:**

| Function/Code | Lines | Reason |
|---------------|-------|--------|
| All imports | 1-36 | Replaced by cli_app imports |
| `_find_repo_root()` | 39-48 | Replaced by `CliContext.build()` |
| `_apply_cli_filters()` | 51-103 | Replaced by `result.py::apply_filters()` |
| `_output_result()` | 106-177 | Replaced by `result.py::render_result()` |
| `_add_common_args()` | 180-241 | Replaced by `params.py` dataclasses |
| `_cmd_impact_cli()` | 244-261 | Replaced by `macros.py::impact()` |
| `_cmd_calls_cli()` | 264-278 | Replaced by `macros.py::calls()` |
| `_cmd_imports_cli()` | 281-297 | Replaced by `macros.py::imports()` |
| `_cmd_exceptions_cli()` | 300-314 | Replaced by `macros.py::exceptions()` |
| `_cmd_sig_impact_cli()` | 317-333 | Replaced by `macros.py::sig_impact()` |
| `_cmd_side_effects_cli()` | 336-351 | Replaced by `macros.py::side_effects()` |
| `_cmd_scopes_cli()` | 354-369 | Replaced by `macros.py::scopes()` |
| `_cmd_async_hazards_cli()` | 372-387 | Replaced by `macros.py::async_hazards()` |
| `_cmd_bytecode_cli()` | 390-406 | Replaced by `macros.py::bytecode_surface()` |
| `_cmd_index_cli()` | 409-519 | Replaced by `admin.py::index()` |
| `_cmd_cache_cli()` | 522-545 | Replaced by `admin.py::cache()` |
| `_cmd_query_cli()` | 548-625 | Replaced by `query.py::q()` |
| `_cmd_report_cli()` | 628-675 | Replaced by `report.py::report()` |
| `build_parser()` | 678-1009 | Replaced by cyclopts App |
| `main()` | 1012-1094 | Replaced by `app.py::main()` |

### Implementation Checklist

- [ ] Backup original `cli.py` to `cli.py.bak` (temporary)
- [ ] Replace `cli.py` with thin wrapper
- [ ] Update `__main__.py` to use `cli_app.main`
- [ ] Verify `python -m tools.cq --help` works
- [ ] Verify `uv run cq --help` works
- [ ] Run full test suite
- [ ] Compare help output with original (golden test)
- [ ] Delete `cli.py.bak` after verification
- [ ] Run quality gate

---

## Scope 7: Testing and Documentation

### Overview

Port existing tests, add new tests for cyclopts-specific features, and update documentation.

### Key Architectural Elements

1. **Deterministic Testing**: Use fixed console width for golden tests
2. **Parse Testing**: Test `app.parse_args()` without execution
3. **Integration Testing**: Full command execution with `result_action="return_value"`
4. **Documentation Updates**: SKILL.md, README, etc.

### Representative Code Snippets

#### 7.1 Unit Tests

```python
import pytest
from rich.console import Console

from tools.cq.cli_app.app import app
from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.types import ImpactBucket, OutputFormat


@pytest.fixture
def deterministic_console():
    """Deterministic console for golden tests."""
    return Console(
        width=100,
        force_terminal=False,
        highlight=False,
        color_system=None,
    )


class TestParsing:
    """Test argument parsing without execution."""

    def test_impact_parses_correctly(self):
        cmd, bound, ignored = app.parse_args(
            ["impact", "foo", "--param", "x", "--depth", "3"],
            exit_on_error=False,
            print_error=False,
        )

        assert bound.kwargs["function"] == "foo"
        assert bound.kwargs["param"] == "x"
        assert bound.kwargs["depth"] == 3
        assert "ctx" in ignored

    def test_query_with_comma_separated_filters(self):
        cmd, bound, ignored = app.parse_args(
            ["q", "entity=function", "--impact", "high,med", "--limit", "10"],
            exit_on_error=False,
            print_error=False,
        )

        assert bound.kwargs["query_string"] == "entity=function"
        filters = bound.kwargs["filters"]
        assert ImpactBucket.high in filters.impact
        assert ImpactBucket.med in filters.impact
        assert filters.limit == 10

    def test_output_format_enum_names(self):
        cmd, bound, ignored = app.parse_args(
            ["calls", "foo", "--format", "mermaid-class"],
            exit_on_error=False,
            print_error=False,
        )

        assert bound.kwargs["common"].output_format == OutputFormat.mermaid_class

    def test_context_injection(self):
        cmd, bound, ignored = app.parse_args(
            ["calls", "foo"],
            exit_on_error=False,
            print_error=False,
        )

        assert "ctx" in ignored


class TestHelp:
    """Test help output determinism."""

    def test_help_stable(self, deterministic_console, capsys):
        app(["--help"], console=deterministic_console, exit_on_error=False)
        captured = capsys.readouterr()

        assert "Code Query - High-signal code analysis" in captured.out
        assert "Analysis Commands" in captured.out
        assert "Admin Commands" in captured.out

    def test_command_help_has_examples(self, deterministic_console, capsys):
        app(["impact", "--help"], console=deterministic_console, exit_on_error=False)
        captured = capsys.readouterr()

        assert "Examples:" in captured.out


class TestEnvVars:
    """Test environment variable configuration."""

    def test_cq_root_env_var(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CQ_ROOT", str(tmp_path))

        cmd, bound, ignored = app.parse_args(
            ["calls", "foo"],
            exit_on_error=False,
            print_error=False,
        )

        assert bound.kwargs["common"].root == tmp_path


@pytest.mark.integration
class TestIntegration:
    """Integration tests with real command execution."""

    def test_index_stats(self, tmp_path):
        (tmp_path / "test.py").write_text("def foo(): pass")

        result = app(
            ["index", "--stats", "--root", str(tmp_path)],
            result_action="return_value",
        )

        assert isinstance(result, CliResult)
        assert result.is_success
```

### Files to Create

| File | Purpose | Lines (est.) |
|------|---------|--------------|
| `tests/unit/cq/test_cli_parsing.py` | Parse tests | 150 |
| `tests/unit/cq/test_cli_types.py` | Type/enum tests | 80 |
| `tests/unit/cq/test_cli_context.py` | Context tests | 60 |
| `tests/integration/cq/test_cli_commands.py` | Integration tests | 200 |
| `tests/cli_golden/cq/help_output.golden` | Help golden file | - |

### Documentation to Update

| File | Changes |
|------|---------|
| `.claude/skills/cq/SKILL.md` | Update CLI examples if any changed |
| `tools/cq/README.md` | Update installation/usage if needed |
| `docs/plans/cq-cyclopts-migration.md` | Mark as implemented |

### Implementation Checklist

- [ ] Create `tests/unit/cq/test_cli_parsing.py` with parse tests
- [ ] Create `tests/unit/cq/test_cli_types.py` with enum/converter tests
- [ ] Create `tests/unit/cq/test_cli_context.py` with context tests
- [ ] Create `tests/integration/cq/test_cli_commands.py`
- [ ] Create golden test for help output
- [ ] Port any existing CLI tests from `tests/`
- [ ] Verify all tests pass: `uv run pytest tests/unit/cq/ tests/integration/cq/ -v`
- [ ] Update SKILL.md if CLI examples changed
- [ ] Update README.md if installation/usage changed
- [ ] Mark plan as implemented
- [ ] Run full quality gate

---

## Environment Variable Configuration

| Variable | Purpose | Default |
|----------|---------|---------|
| `CQ_ROOT` | Default repository root | Auto-detect from cwd |
| `CQ_ARTIFACT_DIR` | Default artifact output directory | `.cq/artifacts` |
| `CQ_FORCE_COLOR` | Force colored output (for interactive use) | `false` |

---

## Success Metrics

1. **Line Count:** Reduce cli.py from 1,100 to ~400 lines total across cli_app/
2. **Test Coverage:** Maintain or improve coverage
3. **Startup Time:** < 100ms for `cq --help`
4. **Shell Completion:** Working for all commands and options
5. **Help Quality:** Deterministic, grouped, with examples
6. **Type Safety:** All parameters statically typed
7. **No Global State:** All context passed via CliContext
8. **Clean Cut:** No legacy fallback code paths

---

## Complete File Summary

### Files to Create (Total: 12)

| File | Scope | Lines (est.) |
|------|-------|--------------|
| `tools/cq/cli_app/__init__.py` | 1 | 15 |
| `tools/cq/cli_app/context.py` | 1 | 80 |
| `tools/cq/cli_app/types.py` | 1 | 100 |
| `tools/cq/cli_app/params.py` | 1 | 120 |
| `tools/cq/cli_app/validators.py` | 1 | 50 |
| `tools/cq/cli_app/app.py` | 2 | 150 |
| `tools/cq/cli_app/result.py` | 2 | 100 |
| `tools/cq/cli_app/commands/__init__.py` | 2 | 5 |
| `tools/cq/cli_app/commands/admin.py` | 3 | 150 |
| `tools/cq/cli_app/commands/macros.py` | 4 | 300 |
| `tools/cq/cli_app/commands/query.py` | 5 | 100 |
| `tools/cq/cli_app/commands/report.py` | 5 | 100 |

**Total new code:** ~1,270 lines (vs 1,094 in original cli.py, but with much better structure)

### Files to Delete/Replace (Total: 1)

| File | Action | Lines Removed |
|------|--------|---------------|
| `tools/cq/cli.py` | Replace with 12-line wrapper | ~1,082 lines |

### Net Effect

- **Before:** 1 file, 1,094 lines, monolithic
- **After:** 12 files, ~1,270 lines, modular and testable
- **Gain:** Type safety, shell completion, DRY parameters, lazy loading, deterministic help

---

## Appendix: Cyclopts Features Used

| Feature | Documentation Reference | Usage in cq |
|---------|-------------------------|-------------|
| `App` + `@app.command` | Commands docs | Command registration |
| `Parameter(name="*")` | User Classes docs | Shared options dataclasses |
| `Parameter(parse=False)` | Meta App docs | CliContext injection |
| Lazy loading strings | Lazy Loading docs | Fast startup |
| `App.meta` | Meta App docs | Dependency injection |
| `Group` with `sort_key` | Groups docs | Stable help ordering |
| `validators.Number` | Validators docs | Depth/limit validation |
| `Enum` coercion | Coercion Rules | Output format, buckets |
| Custom `converter` | Coercion docs | Comma-separated lists |
| `name_transform` | App docs | Kebab-case control |
| `result_action` pipeline | App Calling docs | Result handling |
| Rich console | Help docs | Deterministic output |

---

## References

- [Cyclopts Documentation](https://cyclopts.readthedocs.io/)
- [Cyclopts API Reference](https://cyclopts.readthedocs.io/en/v4.4.1/api.html)
- [cq CLI Source](../tools/cq/cli.py)
- [cq SKILL.md](../.claude/skills/cq/SKILL.md)
