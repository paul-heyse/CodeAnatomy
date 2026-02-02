# CQ Cyclopts Feature Enhancement Plan

> **Status:** Design Document (Revised)
> **Created:** 2026-02-02
> **Revised:** 2026-02-02
> **Reference:** `docs/python_library_reference/cyclopts.md`

---

## Executive Summary

This document defines the target architecture for a "best-in-class" cyclopts deployment for the CQ CLI. The design prioritizes:

1. **Single source of truth** - All options defined via type hints + `Annotated` + `Parameter`
2. **Global options in meta-app** - `--root`, `--config`, `--no-config`, `--verbose`, `--format` appear once in help
3. **Structured result envelopes** - Commands return `CliResult` for centralized post-processing
4. **Preserved CLI tokens** - No breaking changes to existing `--format` or `--severity` values
5. **Fast startup** - Lazy command imports, sub-50ms for `--help`
6. **Deterministic output** - Pinned `help_format`, stable ordering, golden tests
7. **Explicit config precedence** - CLI > env > config file > defaults, with `--no-config` escape
8. **First-class completion and tests** - Installable shell completion + golden help tests

---

## 1. Current Implementation Audit

### 1.1 Features We ARE Using

| Feature | Location | Notes |
|---------|----------|-------|
| Basic `App` with name/help/version | `app.py:46-51` | Core setup done correctly |
| `Group` with `sort_key` | `app.py:24-25` | Analysis/Admin groups organized |
| `Parameter` annotations | Throughout | Help text, name overrides |
| Enum types for choices | `types.py` | `OutputFormat`, `ImpactBucket`, etc. |
| Custom converters | `types.py` | `comma_separated_list()`, `comma_separated_enum()` |
| Explicit `name=` for CLI token overrides | Throughout | e.g., `--format`, `--artifact-dir` |

### 1.2 Issues with Current Implementation

| Issue | Problem | Impact |
|-------|---------|--------|
| Global `negative=""` | `default_parameter=Parameter(negative="")` breaks `--no-cache` and any `--flag/--no-flag` pairs | High |
| No config file support | Users cannot set defaults in `pyproject.toml` or environment | High |
| Duplicated context building | Every command has `ctx = CliContext.build(...)` boilerplate | Medium |
| No result envelope | Commands return `int` directly; filters/artifacts handled per-command | Medium |
| Unpinned `help_format` | Help output may drift across cyclopts versions | Low |

### 1.3 Features We ARE NOT Using

| Feature | Impact | Priority |
|---------|--------|----------|
| Config file integration (`Env`, `Toml`) with `--no-config` escape | High | P0 |
| Meta-app pattern for global options | High | P0 |
| Structured `CliResult` envelope with centralized `result_action` | High | P0 |
| Namespace flattening (`name="*"`) for shared options | High | P1 |
| Lazy loading for commands | Medium | P1 |
| Parameter validators (`validators.Path`, `validators.Number`) | Medium | P1 |
| Group validators (`MutuallyExclusive`) | Medium | P2 |
| Shell completion | Medium | P2 |
| Counting flags (`-vvv`) | Low | P2 |
| Rich formatted exceptions | Low | P3 |

---

## 2. Critical Fixes (Before New Features)

### 2.1 Remove Global `negative=""` Setting

**Problem:** The current `default_parameter=Parameter(negative="")` globally disables all `--no-*` flags, breaking `--no-cache` and any future `--flag/--no-flag` pairs.

**Solution:** Remove the global setting; apply `negative=""` only to specific booleans where negatives don't make sense.

**Before (WRONG):**
```python
app = App(
    name="cq",
    default_parameter=Parameter(negative=""),  # BREAKS --no-cache!
)
```

**After (CORRECT):**
```python
app = App(
    name="cq",
    # No global negative="" - let --no-* flags work by default
)

# Only suppress negatives on specific flags where they don't make sense
@app.command
def index(
    *,
    rebuild: Annotated[bool, Parameter(negative="", help="Force rebuild")] = False,
    stats: Annotated[bool, Parameter(negative="", help="Show stats")] = False,
    # But --no-cache DOES make sense, so no negative="" here:
    # cache: Annotated[bool, Parameter(help="Use cache")] = True,
):
    ...
```

---

### 2.2 Ensure Enum Coercion Matches Existing CLI Tokens

**Problem:** Cyclopts maps enums by member **name** (not value) by default. If enum member names don't match CLI tokens, existing commands break.

**Current Implementation (CORRECT - verify and preserve):**
```python
class OutputFormat(str, Enum):
    md = "md"                    # name = "md" → CLI token "md" ✓
    json = "json"                # name = "json" → CLI token "json" ✓
    mermaid_class = "mermaid-class"  # name = "mermaid_class" → needs name_transform!
```

**Solution:** The `mermaid_class` member name differs from CLI token `mermaid-class`. Either:

1. **Option A (Recommended):** Set `name_transform` on the App to convert underscores to hyphens:
```python
app = App(
    name="cq",
    name_transform=lambda s: s.replace("_", "-"),
)
```

2. **Option B:** Use `Literal[...]` for CLI tokens where enum names don't match:
```python
output_format: Annotated[
    Literal["md", "json", "mermaid-class", ...],
    Parameter(help="Output format"),
] = "md"
```

**Verification test:**
```python
def test_enum_tokens_preserved():
    """Ensure CLI tokens match existing documentation."""
    for token in ["md", "json", "mermaid", "mermaid-class", "dot", "both", "summary"]:
        _, bound, _ = app.parse_args(["calls", "foo", "--format", token])
        # Should not raise
```

---

## 3. P0 Features (Must Have)

### 3.1 Config File Integration with Escape Hatch

**Problem:** Users cannot set defaults in `pyproject.toml` or environment variables, and there's no way to bypass config if it causes issues.

**Solution:** Implement `App.config` chain with explicit providers, `--config` override, and `--no-config` escape.

**Implementation:**

```python
# app.py
from cyclopts import App
from cyclopts.config import Env, Toml

def _build_config_chain(config_file: str | None = None, no_config: bool = False):
    """Build config provider chain based on meta-app options."""
    if no_config:
        return []  # Skip all config providers

    providers = [
        Env(prefix="CQ_"),  # Environment variables: CQ_ROOT, CQ_FORMAT, etc.
    ]

    if config_file:
        # User-specified config file takes precedence
        providers.append(Toml(config_file, must_exist=True))
    else:
        # Default: look for pyproject.toml
        providers.append(
            Toml("pyproject.toml", root_keys=("tool", "cq"), must_exist=False)
        )

    return providers
```

**Config Precedence (highest to lowest):**
1. **CLI arguments** - Always win
2. **Environment variables** - `CQ_ROOT`, `CQ_FORMAT`, etc.
3. **Config file** - `--config <file>` or `pyproject.toml [tool.cq]`
4. **Python defaults** - Function signature defaults

**Example pyproject.toml:**
```toml
[tool.cq]
root = "/home/user/myproject"
format = "json"
artifact_dir = ".cq-artifacts"
verbose = 1
```

**Example environment:**
```bash
export CQ_ROOT="/path/to/repo"
export CQ_FORMAT="json"
```

---

### 3.2 Meta-App Pattern for Global Options

**Problem:** Every command duplicates context building and global options like `--root` appear in every command signature.

**Solution:** Use meta-app pattern where **global options are meta-app params** (not in every command signature). They appear once in help under "Global Options" and are injected into all commands.

**Architecture:**

```
┌─────────────────────────────────────────────────────────────┐
│                      Meta-App Launcher                       │
│  Global Options: --root, --config, --no-config, --verbose   │
│  --format, --artifact-dir, --no-save-artifact               │
├─────────────────────────────────────────────────────────────┤
│                       Primary App                            │
│  Commands: impact, calls, imports, q, index, cache, ...     │
│  Each command has command-specific params only               │
└─────────────────────────────────────────────────────────────┘
```

**Implementation:**

```python
# app.py
from typing import Annotated
from cyclopts import App, Group, Parameter
from pathlib import Path

from tools.cq.cli_app.context import CliContext
from tools.cq.cli_app.types import OutputFormat

# Command groups (defined in always-imported module for lazy loading compatibility)
global_group = Group("Global Options", sort_key=0)
analysis_group = Group("Analysis", sort_key=1)
admin_group = Group("Administration", sort_key=2)

app = App(
    name="cq",
    help="Code Query - High-signal code analysis macros",
    version=VERSION,
    help_format="markdown",  # Pin for version stability
    name_transform=lambda s: s.replace("_", "-"),  # Enum underscore→hyphen
)

# Configure meta-app help panel
app.meta.group_parameters = global_group


@app.meta.default
def launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    # ─── Global Options (appear ONCE in help, injected into all commands) ───
    root: Annotated[
        str | None,
        Parameter(help="Repository root directory"),
    ] = None,
    config: Annotated[
        str | None,
        Parameter(help="Path to config file (default: pyproject.toml)"),
    ] = None,
    no_config: Annotated[
        bool,
        Parameter(
            name="--no-config",
            negative="",  # No --no-no-config
            help="Skip config file and environment variable loading",
        ),
    ] = False,
    verbose: Annotated[
        int,
        Parameter(
            name=("-v", "--verbose"),
            count=True,
            help="Increase verbosity (-v, -vv, -vvv)",
        ),
    ] = 0,
    output_format: Annotated[
        OutputFormat,
        Parameter(name="--format", help="Output format"),
    ] = OutputFormat.md,
    artifact_dir: Annotated[
        str | None,
        Parameter(name="--artifact-dir", help="Directory for saved artifacts"),
    ] = None,
    no_save_artifact: Annotated[
        bool,
        Parameter(
            name="--no-save-artifact",
            negative="",
            help="Don't save result artifacts",
        ),
    ] = False,
) -> int:
    """CQ CLI - Code Query for high-signal code analysis."""
    # Build config chain based on options
    config_chain = _build_config_chain(config, no_config)
    app.config = config_chain

    # Parse remaining tokens to find the command
    command, bound, ignored = app.parse_args(tokens)

    # Build context once
    ctx = CliContext.build(
        argv=list(tokens),
        root=Path(root) if root else None,
        verbose=verbose,
        output_format=output_format,
        artifact_dir=Path(artifact_dir) if artifact_dir else None,
        save_artifact=not no_save_artifact,
    )

    # Inject context into commands that request it
    extra = {}
    if "ctx" in ignored:
        extra["ctx"] = ctx

    # Execute command
    result = command(*bound.args, **bound.kwargs, **extra)

    # Centralized result handling
    return _handle_result(result, ctx)


def main() -> int:
    """Entry point."""
    return app.meta()
```

**Command signature (simplified):**

```python
@app.command(group=analysis_group)
def impact(
    function: Annotated[str, Parameter(help="Function name to analyze")],
    *,
    param: Annotated[str, Parameter(help="Parameter name to trace")],
    depth: Annotated[int, Parameter(help="Maximum call depth")] = 5,
    # Command-specific filters (not global)
    include: Annotated[list[str] | None, Parameter(help="Include patterns")] = None,
    exclude: Annotated[list[str] | None, Parameter(help="Exclude patterns")] = None,
    # Context injected by meta-app, NOT parsed from CLI
    ctx: Annotated[CliContext, Parameter(parse=False)],
) -> CliResult:
    """Trace data flow from a function parameter."""
    from tools.cq.macros.impact import ImpactRequest, cmd_impact

    request = ImpactRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        function_name=function,
        param_name=param,
        max_depth=depth,
    )
    cq_result = cmd_impact(request)

    # Return structured envelope for centralized handling
    return CliResult(
        result=cq_result,
        context=ctx,
        filters=FilterConfig(include=include, exclude=exclude),
    )
```

**Key design points:**
- **Global options live in meta-app only** - `--root`, `--config`, `--verbose`, `--format` appear once in help
- **Commands return `CliResult` envelope** - Not raw `int`; enables centralized post-processing
- **Context injection via `Parameter(parse=False)`** - No boilerplate in commands
- **`--no-config` escape hatch** - Bypasses all config providers

---

### 3.3 Structured Result Envelope with Centralized `result_action`

**Problem:** Each command currently handles filters, output format, artifact saving, and exit codes independently. This leads to inconsistency and duplication.

**Solution:** All commands return a `CliResult` envelope. The meta-app launcher applies filters, saves artifacts, renders output, and sets exit code centrally.

**CliResult Design:**

```python
# context.py
from dataclasses import dataclass, field
from typing import Any

from tools.cq.core.schema import CqResult


@dataclass
class FilterConfig:
    """Filter configuration for result post-processing."""

    include: list[str] | None = None
    exclude: list[str] | None = None
    impact: list[str] | None = None
    confidence: list[str] | None = None
    severity: list[str] | None = None
    limit: int | None = None

    @property
    def has_filters(self) -> bool:
        return any([
            self.include, self.exclude, self.impact,
            self.confidence, self.severity, self.limit,
        ])


@dataclass
class CliResult:
    """Structured result envelope returned by all commands."""

    result: CqResult | int | str | None
    context: CliContext
    filters: FilterConfig = field(default_factory=FilterConfig)
    exit_code: int | None = None  # Explicit override if needed

    @property
    def is_cq_result(self) -> bool:
        return isinstance(self.result, CqResult)

    def get_exit_code(self) -> int:
        if self.exit_code is not None:
            return self.exit_code
        if isinstance(self.result, int):
            return self.result
        if isinstance(self.result, bool):
            return 0 if self.result else 1
        return 0
```

**Centralized Result Handler:**

```python
# result.py
def handle_result(result: CliResult | int) -> int:
    """Centralized result handling for all commands.

    Applies filters, saves artifacts, renders output, returns exit code.
    """
    # Handle simple int returns (admin commands)
    if isinstance(result, int):
        return result

    ctx = result.context

    # Apply filters if result is CqResult with findings
    if result.is_cq_result and result.filters.has_filters:
        result.result = apply_filters(result.result, result.filters)

    # Render output in requested format
    output = render_result(result.result, ctx.output_format)
    if output:
        console.print(output)

    # Save artifact if configured
    if ctx.save_artifact and result.is_cq_result:
        save_artifact(result.result, ctx)

    return result.get_exit_code()
```

**Benefits:**
- **Consistent filtering** - All commands use same filter logic
- **Consistent artifact saving** - All commands save to same location with same format
- **Consistent exit codes** - Centralized logic, predictable behavior
- **Testable** - Can test result handling in isolation

---

## 4. P1 Features (Should Have)

### 4.1 Namespace Flattening for Shared Filter Options

**Problem:** Filter options (`--include`, `--exclude`, `--impact`, etc.) are repeated in every command signature.

**Solution:** Use `@Parameter(name="*")` to flatten shared dataclasses.

```python
# params.py
from dataclasses import dataclass
from typing import Annotated
from cyclopts import Parameter

from tools.cq.cli_app.types import (
    ImpactBucket, ConfidenceBucket, SeverityLevel,
    comma_separated_list, comma_separated_enum,
)


@Parameter(name="*")  # Flatten: --include not --filters.include
@dataclass
class FilterOptions:
    """Common filter options for analysis commands."""

    include: Annotated[
        list[str] | None,
        Parameter(help="Include patterns (comma-separated)", converter=comma_separated_list(str)),
    ] = None

    exclude: Annotated[
        list[str] | None,
        Parameter(help="Exclude patterns", converter=comma_separated_list(str)),
    ] = None

    impact: Annotated[
        list[ImpactBucket] | None,
        Parameter(help="Impact buckets (low,med,high)", converter=comma_separated_enum(ImpactBucket)),
    ] = None

    confidence: Annotated[
        list[ConfidenceBucket] | None,
        Parameter(help="Confidence buckets", converter=comma_separated_enum(ConfidenceBucket)),
    ] = None

    severity: Annotated[
        list[SeverityLevel] | None,
        Parameter(help="Severity levels", converter=comma_separated_enum(SeverityLevel)),
    ] = None

    limit: Annotated[
        int | None,
        Parameter(help="Maximum findings", validator=validators.Number(gte=1)),
    ] = None

    def to_filter_config(self) -> FilterConfig:
        """Convert to FilterConfig for result handling."""
        return FilterConfig(
            include=self.include,
            exclude=self.exclude,
            impact=[b.value for b in self.impact] if self.impact else None,
            confidence=[b.value for b in self.confidence] if self.confidence else None,
            severity=[s.value for s in self.severity] if self.severity else None,
            limit=self.limit,
        )
```

**Command signature becomes:**

```python
@app.command(group=analysis_group)
def impact(
    function: Annotated[str, Parameter(help="Function name")],
    *,
    param: Annotated[str, Parameter(help="Parameter name")],
    depth: Annotated[int, Parameter(help="Maximum call depth")] = 5,
    filters: FilterOptions | None = None,  # All filter flags in one!
    ctx: Annotated[CliContext, Parameter(parse=False)],
) -> CliResult:
    ...
    return CliResult(
        result=cq_result,
        context=ctx,
        filters=filters.to_filter_config() if filters else FilterConfig(),
    )
```

---

### 4.2 Lazy Loading for Commands

**Problem:** Startup time is ~175ms due to importing all macro modules at CLI import time.

**Solution:** Register commands by import path string. **Groups must be defined in the always-imported module** (critical for validators and help consistency).

**Implementation:**

```python
# app.py - Root app with lazy registrations only (no macro imports!)

from cyclopts import App, Group

# Groups defined here (always imported) - critical for lazy loading
global_group = Group("Global Options", sort_key=0)
analysis_group = Group("Analysis", sort_key=1)
admin_group = Group("Administration", sort_key=2)

app = App(
    name="cq",
    help_format="markdown",
    name_transform=lambda s: s.replace("_", "-"),
)

# Lazy-load commands by import path
app.command("tools.cq.cli_app.commands.impact:impact", group=analysis_group)
app.command("tools.cq.cli_app.commands.calls:calls", group=analysis_group)
app.command("tools.cq.cli_app.commands.imports:imports", group=analysis_group)
app.command("tools.cq.cli_app.commands.query:q", group=analysis_group)
app.command("tools.cq.cli_app.commands.report:report", group=analysis_group)
app.command("tools.cq.cli_app.commands.admin:index", group=admin_group)
app.command("tools.cq.cli_app.commands.admin:cache", group=admin_group)
```

**Directory structure:**

```
tools/cq/cli_app/
├── __init__.py
├── app.py              # Root app + lazy registrations (no macro imports!)
├── context.py          # CliContext, CliResult, FilterConfig
├── types.py            # Enums and converters
├── params.py           # Shared option dataclasses
├── result.py           # Centralized result handling
└── commands/
    ├── __init__.py
    ├── impact.py
    ├── calls.py
    ├── imports.py
    ├── query.py
    ├── report.py
    └── admin.py
```

**Target:** `cq --help` in <50ms (down from ~175ms)

**Verification tests:**

```python
def test_lazy_import_correctness():
    """Ensure all lazy imports resolve without errors."""
    for name in ["impact", "calls", "imports", "q", "report", "index", "cache"]:
        cmd = app[name]  # Forces resolution
        assert callable(cmd)


def test_help_generation_loads_commands():
    """Ensure help works even with lazy loading."""
    with pytest.raises(SystemExit):
        app(["--help"])
```

---

### 4.3 Parameter and Group Validators

**Implementation:**

```python
from cyclopts import validators

# Parameter validators
@app.command(group=admin_group)
def index(
    *,
    root: Annotated[
        Path | None,
        Parameter(
            help="Repository root",
            validator=validators.Path(exists=True, dir_okay=True, file_okay=False),
        ),
    ] = None,
    ctx: Annotated[CliContext, Parameter(parse=False)],
) -> CliResult:
    ...


# Group validator for mutually exclusive options
index_actions = Group(
    "",  # Hidden group (no panel in help)
    validator=validators.MutuallyExclusive(),
    default_parameter=Parameter(negative=""),  # No --no-rebuild, etc.
)

@app.command(group=admin_group)
def index(
    *,
    rebuild: Annotated[bool, Parameter(group=index_actions, help="Force full rebuild")] = False,
    stats: Annotated[bool, Parameter(group=index_actions, help="Show index statistics")] = False,
    clear: Annotated[bool, Parameter(group=index_actions, help="Clear the index")] = False,
    ctx: Annotated[CliContext, Parameter(parse=False)],
) -> CliResult:
    ...
```

---

## 5. P2 Features (Nice to Have)

### 5.1 Shell Completion

```python
# Register in admin group, don't auto-modify shell startup files
app.register_install_completion_command(
    name="--install-completion",
    add_to_startup=False,  # User must source manually
    group=admin_group,
    help="Install shell completion scripts (bash/zsh/fish)",
)
```

**Usage:**
```bash
cq --install-completion --shell zsh
# Output shows path to completion script
# User adds to .zshrc: source ~/.zsh/completions/_cq
```

---

### 5.2 Rich Formatted Exceptions

For better error presentation:

```python
# app.py
from rich.console import Console
from rich.traceback import install as install_rich_traceback

error_console = Console(stderr=True)
install_rich_traceback(console=error_console, show_locals=False)

app = App(
    error_console=error_console,
    # ...
)
```

---

## 6. Testing Strategy

### 6.1 Deterministic Console for Golden Tests

```python
@pytest.fixture
def deterministic_console():
    """Console with fixed settings for reproducible output."""
    return Console(
        width=80,
        force_terminal=True,
        highlight=False,
        color_system=None,
        legacy_windows=False,
    )


def test_help_output_stability(deterministic_console, capsys):
    """Pin help output with golden test."""
    with pytest.raises(SystemExit):
        app(["--help"], console=deterministic_console)

    actual = capsys.readouterr().out
    assert_golden("help_output.txt", actual)
```

### 6.2 Config Precedence Tests

```python
def test_cli_overrides_env(monkeypatch):
    """CLI args take precedence over environment."""
    monkeypatch.setenv("CQ_ROOT", "/from/env")

    _, bound, _ = app.parse_args(["calls", "foo", "--root", "/from/cli"])
    assert bound.kwargs["root"] == "/from/cli"


def test_env_overrides_file(tmp_path, monkeypatch):
    """Environment takes precedence over config file."""
    config = tmp_path / "pyproject.toml"
    config.write_text('[tool.cq]\nroot = "/from/file"')

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("CQ_ROOT", "/from/env")

    _, bound, _ = app.parse_args(["calls", "foo"])
    assert bound.kwargs["root"] == "/from/env"


def test_no_config_bypasses_all(tmp_path, monkeypatch):
    """--no-config skips env and file loading."""
    monkeypatch.setenv("CQ_ROOT", "/from/env")

    _, bound, _ = app.parse_args(["calls", "foo", "--no-config"])
    assert bound.kwargs["root"] is None  # Falls to Python default
```

### 6.3 Enum Token Preservation Tests

```python
@pytest.mark.parametrize("token,expected", [
    ("md", OutputFormat.md),
    ("json", OutputFormat.json),
    ("mermaid", OutputFormat.mermaid),
    ("mermaid-class", OutputFormat.mermaid_class),
    ("dot", OutputFormat.dot),
])
def test_format_tokens_preserved(token, expected):
    """Ensure CLI tokens match existing documentation."""
    _, bound, _ = app.parse_args(["calls", "foo", "--format", token])
    assert bound.kwargs["output_format"] == expected
```

### 6.4 Result Envelope Tests

```python
def test_result_filters_applied():
    """Verify filters are applied centrally."""
    result = CliResult(
        result=make_cq_result_with_10_findings(),
        context=make_context(),
        filters=FilterConfig(limit=5),
    )

    handled = handle_result(result)
    # Verify only 5 findings rendered
```

---

## 7. Implementation Roadmap

### Phase 1: Critical Fixes
1. Remove global `negative=""` setting
2. Verify enum coercion matches CLI tokens
3. Add `name_transform` for underscore→hyphen

### Phase 2: Core Architecture (P0)
1. Implement `CliResult` envelope
2. Build meta-app launcher with global options
3. Add config chain with `--config`/`--no-config`
4. Centralize result handling

### Phase 3: Cleanup (P1)
1. Create shared `FilterOptions` dataclass with `name="*"`
2. Convert commands to lazy loading
3. Add parameter validators

### Phase 4: Polish (P2)
1. Add group validators for mutually exclusive options
2. Add shell completion
3. Add counting verbosity (`-v`, `-vv`, `-vvv`)
4. Add rich formatted exceptions

---

## 8. Target Help Output Structure

```
Usage: cq [OPTIONS] COMMAND [ARGS]...

Code Query - High-signal code analysis macros

╭─ Global Options ───────────────────────────────────────────────────╮
│ --root          Repository root directory                         │
│ --config        Path to config file (default: pyproject.toml)     │
│ --no-config     Skip config file and env var loading              │
│ -v, --verbose   Increase verbosity (-v, -vv, -vvv)                 │
│ --format        Output format [md|json|mermaid|...]               │
│ --artifact-dir  Directory for saved artifacts                     │
│ --no-save-artifact  Don't save result artifacts                   │
│ --help          Show this help and exit                           │
│ --version       Show version and exit                             │
╰────────────────────────────────────────────────────────────────────╯
╭─ Analysis ─────────────────────────────────────────────────────────╮
│ impact      Trace data flow from a function parameter             │
│ calls       Census all call sites for a function                  │
│ imports     Analyze import structure and cycles                   │
│ exceptions  Analyze exception handling patterns                   │
│ q           Declarative code query using ast-grep                 │
│ ...                                                                │
╰────────────────────────────────────────────────────────────────────╯
╭─ Administration ───────────────────────────────────────────────────╮
│ index       Manage the code index                                 │
│ cache       Manage query cache                                    │
│ --install-completion  Install shell completion scripts            │
╰────────────────────────────────────────────────────────────────────╯
```

---

## 9. Success Metrics

| Metric | Current | Target |
|--------|---------|--------|
| Startup time (`cq --help`) | ~175ms | <50ms |
| Lines per command | ~30 | ~15 |
| Config sources | 1 (CLI) | 3 (CLI + env + file) |
| Global options in help | Duplicated per-command | Once under "Global Options" |
| Negative flag behavior | All disabled globally | Default enabled, per-flag override |
| Result handling | Per-command | Centralized |

---

## 10. Key Invariants

1. **Enum member names = CLI tokens** (with `name_transform` for underscore→hyphen)
2. **Global options in meta-app only** - Never duplicated in command signatures
3. **Commands return `CliResult`** - Not raw int; enables central handling
4. **Config precedence is fixed** - CLI > env > file > defaults
5. **`--no-config` always available** - Escape hatch for debugging
6. **Groups defined in root module** - Required for lazy loading + validators
7. **Help output is deterministic** - Pinned format, stable ordering, golden tests

---

## References

- Cyclopts documentation: `docs/python_library_reference/cyclopts.md`
- Current implementation: `tools/cq/cli_app/`
- Original migration plan: `docs/plans/cq-cyclopts-migration.md`

---

## 11. Implementation Specifications

This section provides detailed implementation specifications for each scope item, including
code snippets, target files, deprecation targets, and checklists.

---

### 11.1 Scope: Remove Global `negative=""` Setting (Section 2.1)

#### 11.1.1 Key Code Patterns

**Current problematic code to remove:**
```python
# app.py - REMOVE THIS
app = App(
    name="cq",
    help="Code Query - High-signal code analysis macros",
    version=VERSION,
    default_parameter=Parameter(negative=""),  # ← DELETE THIS LINE
)
```

**New targeted application (only where negatives don't make sense):**
```python
# In admin commands only - apply to action flags that are "do X" not "don't do X"
@app.command(group=admin_group)
def index(
    *,
    # These are actions, not toggles - negatives don't make sense
    rebuild: Annotated[bool, Parameter(negative="", help="Force full rebuild")] = False,
    stats: Annotated[bool, Parameter(negative="", help="Show statistics")] = False,
    clear: Annotated[bool, Parameter(negative="", help="Clear the index")] = False,
    # --root is not a boolean, no change needed
    root: Annotated[str | None, Parameter(help="Repository root")] = None,
) -> int:
    ...

@app.command(group=admin_group)
def cache(
    *,
    stats: Annotated[bool, Parameter(negative="", help="Show statistics")] = False,
    clear: Annotated[bool, Parameter(negative="", help="Clear the cache")] = False,
    root: Annotated[str | None, Parameter(help="Repository root")] = None,
) -> int:
    ...
```

**Preserved working patterns (--no-cache must work):**
```python
# query command - --no-cache MUST work
@app.command(name="q", group=analysis_group)
def query(
    query_string: Annotated[str, Parameter(help='Query string')],
    *,
    # NO negative="" here - --no-cache is valid and useful!
    no_cache: Annotated[bool, Parameter(name="--no-cache", help="Disable caching")] = False,
    ...
) -> int:
    ...
```

#### 11.1.2 Target Files

| Action | File | Changes |
|--------|------|---------|
| Modify | `tools/cq/cli_app/app.py` | Remove `default_parameter=Parameter(negative="")` from App() |
| Modify | `tools/cq/cli_app/app.py` | Add `negative=""` to `index()` action flags (rebuild, stats, clear) |
| Modify | `tools/cq/cli_app/app.py` | Add `negative=""` to `cache()` action flags (stats, clear) |
| Create | `tests/unit/cq/test_cli_negative_flags.py` | Test `--no-cache` works, `--no-rebuild` does not |

#### 11.1.3 Deprecation / Deletion

| What | Location | Action |
|------|----------|--------|
| `default_parameter=Parameter(negative="")` | `app.py:50` | Delete |

#### 11.1.4 Implementation Checklist

- [ ] Remove `default_parameter=Parameter(negative="")` from `App()` constructor
- [ ] Add `negative=""` to `index` command's `rebuild`, `stats`, `clear` parameters
- [ ] Add `negative=""` to `cache` command's `stats`, `clear` parameters
- [ ] Verify `cq q "entity=function" --no-cache` parses successfully
- [ ] Verify `cq index --no-rebuild` fails (help error)
- [ ] Create unit test for negative flag behavior
- [ ] Run full test suite: `uv run pytest tests/unit/cq/test_cli_*.py -v`

---

### 11.2 Scope: Ensure Enum Coercion Matches Existing CLI Tokens (Section 2.2)

#### 11.2.1 Key Code Patterns

**Add `name_transform` to App:**
```python
# app.py - Add name_transform for underscore→hyphen conversion
app = App(
    name="cq",
    help="Code Query - High-signal code analysis macros",
    version=VERSION,
    help_format="markdown",  # Pin for version stability
    name_transform=lambda s: s.replace("_", "-"),  # mermaid_class → mermaid-class
)
```

**Enum definition remains unchanged (already correct):**
```python
# types.py - Member names use underscore, CLI tokens use hyphen
class OutputFormat(str, Enum):
    md = "md"
    json = "json"
    both = "both"
    summary = "summary"
    mermaid = "mermaid"
    mermaid_class = "mermaid-class"  # name_transform: "mermaid_class" → "mermaid-class"
    mermaid_cfg = "mermaid-cfg"      # name_transform: "mermaid_cfg" → "mermaid-cfg"
    dot = "dot"

    def __str__(self) -> str:
        return self.value
```

**Comprehensive token verification test:**
```python
# tests/unit/cq/test_cli_enum_tokens.py
import pytest
from tools.cq.cli_app.app import app
from tools.cq.cli_app.types import OutputFormat

# All CLI tokens that must be preserved (from documentation)
DOCUMENTED_FORMAT_TOKENS = [
    "md", "json", "both", "summary", "mermaid", "mermaid-class", "mermaid-cfg", "dot"
]

@pytest.mark.parametrize("token", DOCUMENTED_FORMAT_TOKENS)
def test_format_token_accepted(token: str) -> None:
    """Ensure all documented CLI tokens are accepted."""
    _cmd, bound, _extra = app.parse_args(
        ["calls", "foo", "--format", token],
        exit_on_error=False,
        print_error=False,
    )
    # Should parse without error and return valid OutputFormat
    assert isinstance(bound.kwargs["output_format"], OutputFormat)


@pytest.mark.parametrize("token,expected", [
    ("md", OutputFormat.md),
    ("json", OutputFormat.json),
    ("mermaid-class", OutputFormat.mermaid_class),
    ("mermaid-cfg", OutputFormat.mermaid_cfg),
])
def test_format_token_to_enum(token: str, expected: OutputFormat) -> None:
    """Ensure CLI tokens map to correct enum members."""
    _cmd, bound, _extra = app.parse_args(
        ["calls", "foo", "--format", token],
        exit_on_error=False,
    )
    assert bound.kwargs["output_format"] == expected
```

#### 11.2.2 Target Files

| Action | File | Changes |
|--------|------|---------|
| Modify | `tools/cq/cli_app/app.py` | Add `name_transform=lambda s: s.replace("_", "-")` to App() |
| Modify | `tools/cq/cli_app/app.py` | Add `help_format="markdown"` to App() |
| Create | `tests/unit/cq/test_cli_enum_tokens.py` | Comprehensive enum token tests |

#### 11.2.3 Deprecation / Deletion

None - this is additive.

#### 11.2.4 Implementation Checklist

- [ ] Add `name_transform=lambda s: s.replace("_", "-")` to `App()` constructor
- [ ] Add `help_format="markdown"` to `App()` constructor (version stability)
- [ ] Create `tests/unit/cq/test_cli_enum_tokens.py` with parametrized token tests
- [ ] Verify `cq calls foo --format mermaid-class` parses to `OutputFormat.mermaid_class`
- [ ] Verify `cq calls foo --format mermaid-cfg` parses to `OutputFormat.mermaid_cfg`
- [ ] Run: `uv run pytest tests/unit/cq/test_cli_enum_tokens.py -v`

---

### 11.3 Scope: Config File Integration with Escape Hatch (Section 3.1)

#### 11.3.1 Key Code Patterns

**Config chain builder:**
```python
# config.py (NEW FILE)
"""Configuration providers for cq CLI."""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from cyclopts.config import ConfigFromFile


def build_config_chain(
    config_file: str | None = None,
    no_config: bool = False,
) -> list[ConfigFromFile]:
    """Build config provider chain based on CLI options.

    Parameters
    ----------
    config_file
        Explicit config file path (--config).
    no_config
        If True, skip all config providers (--no-config).

    Returns
    -------
    list[ConfigFromFile]
        Config providers in precedence order (first = highest priority).
    """
    from cyclopts.config import Env, Toml

    if no_config:
        return []  # Skip all config providers

    providers: list[ConfigFromFile] = [
        # Environment variables: CQ_ROOT, CQ_FORMAT, CQ_VERBOSE, etc.
        Env(prefix="CQ_"),
    ]

    if config_file:
        # User-specified config file takes precedence
        config_path = Path(config_file)
        providers.append(Toml(config_path, must_exist=True))
    else:
        # Default: look for pyproject.toml [tool.cq] section
        providers.append(
            Toml("pyproject.toml", root_keys=("tool", "cq"), must_exist=False)
        )

    return providers


# Environment variable name mappings (for documentation)
ENV_VAR_MAP = {
    "root": "CQ_ROOT",
    "format": "CQ_FORMAT",
    "verbose": "CQ_VERBOSE",
    "artifact_dir": "CQ_ARTIFACT_DIR",
    "no_save_artifact": "CQ_NO_SAVE_ARTIFACT",
}
```

**Integration with meta-app launcher:**
```python
# app.py - In meta-app launcher
from tools.cq.cli_app.config import build_config_chain

@app.meta.default
def launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    config: Annotated[
        str | None,
        Parameter(
            name="--config",
            help="Path to config file (default: pyproject.toml [tool.cq])",
        ),
    ] = None,
    no_config: Annotated[
        bool,
        Parameter(
            name="--no-config",
            negative="",  # No --no-no-config
            help="Skip config file and environment variable loading",
        ),
    ] = False,
    # ... other global options
) -> int:
    """CQ CLI - Code Query for high-signal code analysis."""
    # Build and apply config chain
    config_chain = build_config_chain(config, no_config)
    app.config = config_chain

    # ... rest of launcher
```

**Example pyproject.toml [tool.cq] section:**
```toml
# pyproject.toml
[tool.cq]
root = "/home/user/myproject"
format = "json"
artifact_dir = ".cq/artifacts"
verbose = 1
```

**Example environment usage:**
```bash
# Environment variables (CQ_ prefix)
export CQ_ROOT="/path/to/repo"
export CQ_FORMAT="json"
export CQ_VERBOSE="2"

# Override with CLI
cq calls foo --format md  # CLI wins over env

# Bypass all config
cq calls foo --no-config  # Uses Python defaults only
```

#### 11.3.2 Target Files

| Action | File | Changes |
|--------|------|---------|
| Create | `tools/cq/cli_app/config.py` | Config chain builder function |
| Modify | `tools/cq/cli_app/app.py` | Add `--config` and `--no-config` to meta-app |
| Modify | `tools/cq/cli_app/app.py` | Apply config chain in launcher |
| Create | `tests/unit/cq/test_cli_config.py` | Config precedence tests |

#### 11.3.3 Deprecation / Deletion

| What | Location | Action |
|------|----------|--------|
| `_get_default_root()` in params.py | `params.py:26-35` | Keep but document that Env provider supersedes |

#### 11.3.4 Implementation Checklist

- [ ] Create `tools/cq/cli_app/config.py` with `build_config_chain()`
- [ ] Add `--config` parameter to meta-app launcher
- [ ] Add `--no-config` parameter to meta-app launcher (with `negative=""`)
- [ ] Call `build_config_chain()` in launcher and set `app.config`
- [ ] Create test: CLI overrides env (`CQ_ROOT=/env cq calls foo --root /cli`)
- [ ] Create test: env overrides pyproject.toml
- [ ] Create test: `--no-config` bypasses all config
- [ ] Create test: `--config custom.toml` uses specified file
- [ ] Document env vars in help text and README
- [ ] Run: `uv run pytest tests/unit/cq/test_cli_config.py -v`

---

### 11.4 Scope: Meta-App Pattern for Global Options (Section 3.2)

#### 11.4.1 Key Code Patterns

**Global group definition:**
```python
# app.py - Groups for command organization
from cyclopts import App, Group, Parameter

# Groups defined at module level (required for lazy loading)
global_group = Group("Global Options", sort_key=0)
analysis_group = Group("Analysis", sort_key=1)
admin_group = Group("Administration", sort_key=2)

app = App(
    name="cq",
    help="Code Query - High-signal code analysis macros",
    version=VERSION,
    help_format="markdown",
    name_transform=lambda s: s.replace("_", "-"),
)

# Configure meta-app help panel
app.meta.group_parameters = global_group
```

**Meta-app launcher with global options:**
```python
# app.py - Meta-app launcher
from tools.cq.cli_app.config import build_config_chain
from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.result import handle_result
from tools.cq.cli_app.types import OutputFormat

@app.meta.default
def launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    # ─── Global Options (appear ONCE in help under "Global Options") ───
    root: Annotated[
        str | None,
        Parameter(help="Repository root directory"),
    ] = None,
    config: Annotated[
        str | None,
        Parameter(help="Path to config file (default: pyproject.toml)"),
    ] = None,
    no_config: Annotated[
        bool,
        Parameter(name="--no-config", negative="", help="Skip config/env loading"),
    ] = False,
    verbose: Annotated[
        int,
        Parameter(name=("-v", "--verbose"), count=True, help="Verbosity (-v, -vv, -vvv)"),
    ] = 0,
    output_format: Annotated[
        OutputFormat,
        Parameter(name="--format", help="Output format"),
    ] = OutputFormat.md,
    artifact_dir: Annotated[
        str | None,
        Parameter(name="--artifact-dir", help="Artifact output directory"),
    ] = None,
    no_save_artifact: Annotated[
        bool,
        Parameter(name="--no-save-artifact", negative="", help="Don't save artifacts"),
    ] = False,
) -> int:
    """CQ CLI - Code Query for high-signal code analysis."""
    from pathlib import Path

    # Build and apply config chain
    config_chain = build_config_chain(config, no_config)
    app.config = config_chain

    # Parse remaining tokens to find the command
    command, bound, ignored = app.parse_args(tokens)

    # Build context once (not duplicated per-command)
    ctx = CliContext.build(
        argv=list(tokens),
        root=Path(root) if root else None,
        verbose=verbose,
        output_format=output_format,
        artifact_dir=Path(artifact_dir) if artifact_dir else None,
        save_artifact=not no_save_artifact,
    )

    # Inject context into commands that request it via Parameter(parse=False)
    extra_kwargs = {}
    if "ctx" in ignored:
        extra_kwargs["ctx"] = ctx

    # Execute command and get CliResult
    result = command(*bound.args, **bound.kwargs, **extra_kwargs)

    # Centralized result handling
    return handle_result(result, ctx)


def main() -> int:
    """Entry point."""
    return app.meta()
```

**Simplified command signature (no global options):**
```python
# commands/impact.py
from tools.cq.cli_app.context import CliContext, CliResult, FilterConfig

@app.command(group=analysis_group)
def impact(
    function: Annotated[str, Parameter(help="Function name to analyze")],
    *,
    param: Annotated[str, Parameter(help="Parameter name to trace")],
    depth: Annotated[int, Parameter(help="Maximum call depth")] = 5,
    # Command-specific filter options (not global)
    include: Annotated[list[str] | None, Parameter(help="Include patterns")] = None,
    exclude: Annotated[list[str] | None, Parameter(help="Exclude patterns")] = None,
    # Context injected by meta-app, NOT parsed from CLI
    ctx: Annotated[CliContext, Parameter(parse=False)],
) -> CliResult:
    """Trace data flow from a function parameter."""
    from tools.cq.macros.impact import ImpactRequest, cmd_impact

    request = ImpactRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        function_name=function,
        param_name=param,
        max_depth=depth,
    )
    cq_result = cmd_impact(request)

    # Return envelope for centralized handling
    return CliResult(
        result=cq_result,
        context=ctx,
        filters=FilterConfig(include=include, exclude=exclude),
    )
```

**Context class update:**
```python
# context.py - Enhanced CliContext
@dataclass
class CliContext:
    """CLI execution context."""

    root: Path
    argv: list[str]
    toolchain: Toolchain
    verbose: int = 0
    output_format: OutputFormat = OutputFormat.md
    artifact_dir: Path | None = None
    save_artifact: bool = True

    @classmethod
    def build(
        cls,
        *,
        argv: list[str],
        root: Path | None = None,
        verbose: int = 0,
        output_format: OutputFormat = OutputFormat.md,
        artifact_dir: Path | None = None,
        save_artifact: bool = True,
    ) -> CliContext:
        """Build context from CLI arguments and global options."""
        resolved_root = root or _resolve_root()
        toolchain = load_toolchain(resolved_root)

        return cls(
            root=resolved_root,
            argv=argv,
            toolchain=toolchain,
            verbose=verbose,
            output_format=output_format,
            artifact_dir=artifact_dir,
            save_artifact=save_artifact,
        )
```

#### 11.4.2 Target Files

| Action | File | Changes |
|--------|------|---------|
| Modify | `tools/cq/cli_app/app.py` | Add `global_group`, configure `app.meta.group_parameters` |
| Modify | `tools/cq/cli_app/app.py` | Create `@app.meta.default` launcher function |
| Modify | `tools/cq/cli_app/app.py` | Move global options (root, format, etc.) to launcher |
| Modify | `tools/cq/cli_app/app.py` | Simplify all command signatures to remove global options |
| Modify | `tools/cq/cli_app/context.py` | Add verbose, output_format, artifact_dir, save_artifact to CliContext |
| Modify | `tools/cq/cli_app/context.py` | Update `CliContext.build()` to accept global options |
| Create | `tests/unit/cq/test_cli_meta_app.py` | Test global option injection |

#### 11.4.3 Deprecation / Deletion

| What | Location | Action |
|------|----------|--------|
| `root` param in each command | All commands in `app.py` | Remove (now in launcher) |
| `output_format` param in each command | All commands in `app.py` | Remove (now in launcher) |
| `artifact_dir` param in each command | All commands in `app.py` | Remove (now in launcher) |
| `no_save_artifact` param in each command | All commands in `app.py` | Remove (now in launcher) |
| `ctx = CliContext.build(...)` in each command | All commands in `app.py` | Remove (injected by launcher) |

#### 11.4.4 Implementation Checklist

- [ ] Add `global_group = Group("Global Options", sort_key=0)` at module level
- [ ] Set `app.meta.group_parameters = global_group`
- [ ] Create `@app.meta.default` launcher function with all global options
- [ ] Add counting verbosity: `Parameter(name=("-v", "--verbose"), count=True)`
- [ ] Move global options from commands to launcher
- [ ] Update `CliContext.build()` to accept all global options
- [ ] Update each command to accept `ctx: Annotated[CliContext, Parameter(parse=False)]`
- [ ] Update each command to return `CliResult` envelope
- [ ] Remove per-command context building boilerplate
- [ ] Test: `cq --help` shows "Global Options" panel once
- [ ] Test: `cq impact foo --param bar` receives injected context
- [ ] Test: `-v`, `-vv`, `-vvv` increment verbose level
- [ ] Run: `uv run pytest tests/unit/cq/test_cli_meta_app.py -v`

---

### 11.5 Scope: Structured Result Envelope with Centralized `result_action` (Section 3.3)

#### 11.5.1 Key Code Patterns

**Enhanced CliResult:**
```python
# context.py - Updated CliResult
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult
    from tools.cq.cli_app.types import OutputFormat


@dataclass
class FilterConfig:
    """Filter configuration for result post-processing."""

    include: list[str] = field(default_factory=list)
    exclude: list[str] = field(default_factory=list)
    impact: list[str] = field(default_factory=list)  # Store as strings for simplicity
    confidence: list[str] = field(default_factory=list)
    severity: list[str] = field(default_factory=list)
    limit: int | None = None

    @property
    def has_filters(self) -> bool:
        return bool(
            self.include or self.exclude or self.impact or
            self.confidence or self.severity or self.limit
        )


@dataclass
class CliResult:
    """Structured result envelope returned by all commands.

    All analysis commands return this envelope. The meta-app launcher
    applies filters, renders output, saves artifacts, and sets exit code
    centrally via handle_result().
    """

    result: CqResult | int | str | None
    context: CliContext
    filters: FilterConfig = field(default_factory=FilterConfig)
    exit_code: int | None = None  # Explicit override if needed

    @property
    def is_cq_result(self) -> bool:
        from tools.cq.core.schema import CqResult
        return isinstance(self.result, CqResult)

    def get_exit_code(self) -> int:
        """Get exit code from result or explicit override."""
        if self.exit_code is not None:
            return self.exit_code
        if isinstance(self.result, int):
            return self.result
        if isinstance(self.result, bool):
            return 0 if self.result else 1
        return 0
```

**Centralized result handler:**
```python
# result.py - Updated handle_result
def handle_result(result: CliResult | int, ctx: CliContext) -> int:
    """Centralized result handling for all commands.

    Applies filters, renders output, saves artifacts, returns exit code.

    Parameters
    ----------
    result
        Command result (CliResult envelope or int for admin commands).
    ctx
        CLI context with global options.

    Returns
    -------
    int
        Exit code.
    """
    # Handle simple int returns (admin commands)
    if isinstance(result, int):
        return result

    # Apply filters if result is CqResult with findings
    if result.is_cq_result and result.filters.has_filters:
        result.result = apply_result_filters(result.result, result.filters)

    # Render output in format from global context
    output = render_result(result.result, ctx.output_format)
    if output:
        console.print(output)

    # Save artifact if enabled in global context
    if ctx.save_artifact and result.is_cq_result:
        save_artifact(result.result, ctx.artifact_dir)

    return result.get_exit_code()
```

**Command returning CliResult:**
```python
# Example: calls command
@app.command(group=analysis_group)
def calls(
    function: Annotated[str, Parameter(help="Function name")],
    *,
    include: Annotated[list[str] | None, Parameter(help="Include patterns")] = None,
    exclude: Annotated[list[str] | None, Parameter(help="Exclude patterns")] = None,
    limit: Annotated[int | None, Parameter(help="Max findings")] = None,
    ctx: Annotated[CliContext, Parameter(parse=False)],
) -> CliResult:
    """Census all call sites for a function."""
    from tools.cq.macros.calls import cmd_calls

    cq_result = cmd_calls(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        function_name=function,
    )

    return CliResult(
        result=cq_result,
        context=ctx,
        filters=FilterConfig(
            include=include or [],
            exclude=exclude or [],
            limit=limit,
        ),
    )
```

#### 11.5.2 Target Files

| Action | File | Changes |
|--------|------|---------|
| Modify | `tools/cq/cli_app/context.py` | Update `FilterConfig` fields, add docstrings |
| Modify | `tools/cq/cli_app/context.py` | Update `CliResult` with comprehensive docstring |
| Modify | `tools/cq/cli_app/result.py` | Refactor `handle_result()` to use ctx for global options |
| Modify | `tools/cq/cli_app/result.py` | Remove `output_result()` (superseded by handle_result) |
| Modify | `tools/cq/cli_app/app.py` | Update launcher to call `handle_result(result, ctx)` |
| Modify | `tools/cq/cli_app/app.py` | Update all commands to return `CliResult` |
| Create | `tests/unit/cq/test_cli_result_handling.py` | Test filter application, artifact saving |

#### 11.5.3 Deprecation / Deletion

| What | Location | Action |
|------|----------|--------|
| `output_result()` function | `result.py:106-153` | Delete (replaced by handle_result) |
| `cq_result_action()` function | `result.py:155-185` | Delete (not used) |
| `_build_filters()` helper | `app.py:783-843` | Delete (filters built inline) |
| Per-command output_result calls | All commands in `app.py` | Remove (centralized in launcher) |

#### 11.5.4 Implementation Checklist

- [ ] Update `FilterConfig` to use `list[str]` for impact/confidence/severity (simpler)
- [ ] Update `CliResult` docstring to document envelope pattern
- [ ] Refactor `handle_result(result, ctx)` to use ctx for output_format, save_artifact
- [ ] Delete `output_result()` function
- [ ] Delete `cq_result_action()` function
- [ ] Delete `_build_filters()` helper
- [ ] Update all analysis commands to return `CliResult` envelope
- [ ] Update launcher to call `handle_result(result, ctx)`
- [ ] Test: filters applied centrally
- [ ] Test: artifact saved when ctx.save_artifact=True
- [ ] Test: exit code from result.get_exit_code()
- [ ] Run: `uv run pytest tests/unit/cq/test_cli_result_handling.py -v`

---

### 11.6 Scope: Namespace Flattening for Shared Filter Options (Section 4.1)

#### 11.6.1 Key Code Patterns

**Shared filter options with namespace flattening:**
```python
# params.py - Updated FilterOptions with @Parameter(name="*")
from dataclasses import dataclass, field
from typing import Annotated

from cyclopts import Parameter, validators

from tools.cq.cli_app.types import (
    ImpactBucket,
    ConfidenceBucket,
    SeverityLevel,
    comma_separated_enum,
    comma_separated_list,
)


@Parameter(name="*")  # Flatten: --include not --filters.include
@dataclass
class FilterOptions:
    """Common filter options for analysis commands.

    Decorated with @Parameter(name="*") to flatten into parent namespace.
    This means --include not --filters.include in CLI.
    """

    include: Annotated[
        list[str],
        Parameter(
            help="Include files matching pattern (glob or ~regex)",
            converter=comma_separated_list(str),
        ),
    ] = field(default_factory=list)

    exclude: Annotated[
        list[str],
        Parameter(
            help="Exclude files matching pattern",
            converter=comma_separated_list(str),
        ),
    ] = field(default_factory=list)

    impact: Annotated[
        list[ImpactBucket],
        Parameter(
            help="Filter by impact (low,med,high)",
            converter=comma_separated_enum(ImpactBucket),
        ),
    ] = field(default_factory=list)

    confidence: Annotated[
        list[ConfidenceBucket],
        Parameter(
            help="Filter by confidence (low,med,high)",
            converter=comma_separated_enum(ConfidenceBucket),
        ),
    ] = field(default_factory=list)

    severity: Annotated[
        list[SeverityLevel],
        Parameter(
            help="Filter by severity (info,warning,error)",
            converter=comma_separated_enum(SeverityLevel),
        ),
    ] = field(default_factory=list)

    limit: Annotated[
        int | None,
        Parameter(
            help="Maximum findings",
            validator=validators.Number(gte=1),
        ),
    ] = None

    def to_filter_config(self) -> FilterConfig:
        """Convert to FilterConfig for result handling."""
        from tools.cq.cli_app.context import FilterConfig

        return FilterConfig(
            include=self.include,
            exclude=self.exclude,
            impact=[str(b) for b in self.impact],
            confidence=[str(b) for b in self.confidence],
            severity=[str(s) for s in self.severity],
            limit=self.limit,
        )
```

**Simplified command using FilterOptions:**
```python
# commands/impact.py
@app.command(group=analysis_group)
def impact(
    function: Annotated[str, Parameter(help="Function name")],
    *,
    param: Annotated[str, Parameter(help="Parameter name to trace")],
    depth: Annotated[int, Parameter(help="Maximum call depth")] = 5,
    filters: FilterOptions = FilterOptions(),  # All filter flags in one!
    ctx: Annotated[CliContext, Parameter(parse=False)],
) -> CliResult:
    """Trace data flow from a function parameter."""
    from tools.cq.macros.impact import ImpactRequest, cmd_impact

    request = ImpactRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        function_name=function,
        param_name=param,
        max_depth=depth,
    )
    cq_result = cmd_impact(request)

    return CliResult(
        result=cq_result,
        context=ctx,
        filters=filters.to_filter_config(),
    )
```

#### 11.6.2 Target Files

| Action | File | Changes |
|--------|------|---------|
| Modify | `tools/cq/cli_app/params.py` | Add `@Parameter(name="*")` to `FilterOptions` |
| Modify | `tools/cq/cli_app/params.py` | Add `validators.Number(gte=1)` to limit |
| Modify | `tools/cq/cli_app/params.py` | Add `to_filter_config()` method |
| Modify | `tools/cq/cli_app/app.py` | Update all analysis commands to use `filters: FilterOptions` |
| Create | `tests/unit/cq/test_cli_filter_options.py` | Test namespace flattening |

#### 11.6.3 Deprecation / Deletion

| What | Location | Action |
|------|----------|--------|
| Individual filter params per command | All commands in `app.py` | Replace with `filters: FilterOptions` |
| ~70 lines per command of filter params | `app.py` | ~630 lines removed total |

#### 11.6.4 Implementation Checklist

- [ ] Add `@Parameter(name="*")` to `FilterOptions` dataclass
- [ ] Add `validators.Number(gte=1)` to limit field
- [ ] Add `to_filter_config()` method to `FilterOptions`
- [ ] Update `impact` command to use `filters: FilterOptions`
- [ ] Update `calls` command to use `filters: FilterOptions`
- [ ] Update `imports` command to use `filters: FilterOptions`
- [ ] Update `exceptions` command to use `filters: FilterOptions`
- [ ] Update `sig_impact` command to use `filters: FilterOptions`
- [ ] Update `side_effects` command to use `filters: FilterOptions`
- [ ] Update `scopes` command to use `filters: FilterOptions`
- [ ] Update `async_hazards` command to use `filters: FilterOptions`
- [ ] Update `bytecode_surface` command to use `filters: FilterOptions`
- [ ] Update `query` command to use `filters: FilterOptions`
- [ ] Update `report` command to use `filters: FilterOptions`
- [ ] Verify `cq calls foo --include src/` parses correctly (not `--filters.include`)
- [ ] Run: `uv run pytest tests/unit/cq/test_cli_filter_options.py -v`

---

### 11.7 Scope: Lazy Loading for Commands (Section 4.2)

#### 11.7.1 Key Code Patterns

**Directory structure after refactoring:**
```
tools/cq/cli_app/
├── __init__.py           # main() entry point
├── app.py                # Root app + lazy registrations (no macro imports!)
├── config.py             # Config chain builder
├── context.py            # CliContext, CliResult, FilterConfig
├── types.py              # Enums and converters
├── params.py             # Shared option dataclasses
├── result.py             # Centralized result handling
└── commands/
    ├── __init__.py       # Package init (empty)
    ├── analysis.py       # impact, calls, imports, exceptions, sig_impact, etc.
    ├── query.py          # q command
    ├── report.py         # report command
    └── admin.py          # index, cache commands
```

**Root app with lazy registrations:**
```python
# app.py - Only groups and app defined here, no macro imports!
from cyclopts import App, Group, Parameter

VERSION = "0.3.0"

# Groups defined at module level (required for lazy loading + validators)
global_group = Group("Global Options", sort_key=0)
analysis_group = Group("Analysis", sort_key=1)
admin_group = Group("Administration", sort_key=2)

app = App(
    name="cq",
    help="Code Query - High-signal code analysis macros",
    version=VERSION,
    help_format="markdown",
    name_transform=lambda s: s.replace("_", "-"),
)

app.meta.group_parameters = global_group

# ─── Lazy Command Registration ───
# Commands registered by import path, not imported until invoked

# Analysis commands
app.command("tools.cq.cli_app.commands.analysis:impact", group=analysis_group)
app.command("tools.cq.cli_app.commands.analysis:calls", group=analysis_group)
app.command("tools.cq.cli_app.commands.analysis:imports", group=analysis_group)
app.command("tools.cq.cli_app.commands.analysis:exceptions", group=analysis_group)
app.command("tools.cq.cli_app.commands.analysis:sig_impact", name="sig-impact", group=analysis_group)
app.command("tools.cq.cli_app.commands.analysis:side_effects", name="side-effects", group=analysis_group)
app.command("tools.cq.cli_app.commands.analysis:scopes", group=analysis_group)
app.command("tools.cq.cli_app.commands.analysis:async_hazards", name="async-hazards", group=analysis_group)
app.command("tools.cq.cli_app.commands.analysis:bytecode_surface", name="bytecode-surface", group=analysis_group)

# Query and report
app.command("tools.cq.cli_app.commands.query:q", group=analysis_group)
app.command("tools.cq.cli_app.commands.report:report", group=analysis_group)

# Admin commands
app.command("tools.cq.cli_app.commands.admin:index", group=admin_group)
app.command("tools.cq.cli_app.commands.admin:cache", group=admin_group)
```

**Command module (loaded on demand):**
```python
# commands/analysis.py - Imported only when command is invoked
from __future__ import annotations

from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext, CliResult
from tools.cq.cli_app.params import FilterOptions


def impact(
    function: Annotated[str, Parameter(help="Function name to analyze")],
    *,
    param: Annotated[str, Parameter(help="Parameter name to trace")],
    depth: Annotated[int, Parameter(help="Maximum call depth")] = 5,
    filters: FilterOptions = FilterOptions(),
    ctx: Annotated[CliContext, Parameter(parse=False)],
) -> CliResult:
    """Trace data flow from a function parameter."""
    # Macro import inside function - only when command runs
    from tools.cq.macros.impact import ImpactRequest, cmd_impact

    request = ImpactRequest(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        function_name=function,
        param_name=param,
        max_depth=depth,
    )
    cq_result = cmd_impact(request)

    return CliResult(
        result=cq_result,
        context=ctx,
        filters=filters.to_filter_config(),
    )


def calls(
    function: Annotated[str, Parameter(help="Function name")],
    *,
    filters: FilterOptions = FilterOptions(),
    ctx: Annotated[CliContext, Parameter(parse=False)],
) -> CliResult:
    """Census all call sites for a function."""
    from tools.cq.macros.calls import cmd_calls

    cq_result = cmd_calls(
        tc=ctx.toolchain,
        root=ctx.root,
        argv=ctx.argv,
        function_name=function,
    )

    return CliResult(
        result=cq_result,
        context=ctx,
        filters=filters.to_filter_config(),
    )

# ... other analysis commands
```

**Startup time benchmark test:**
```python
# tests/unit/cq/test_cli_startup.py
import subprocess
import time

def test_help_startup_time() -> None:
    """Verify --help completes in under 100ms."""
    start = time.perf_counter()
    result = subprocess.run(
        ["uv", "run", "cq", "--help"],
        capture_output=True,
        text=True,
    )
    elapsed_ms = (time.perf_counter() - start) * 1000

    assert result.returncode == 0
    assert elapsed_ms < 100, f"Startup took {elapsed_ms:.1f}ms, expected <100ms"


def test_lazy_import_correctness() -> None:
    """Ensure all lazy imports resolve without errors."""
    from tools.cq.cli_app.app import app

    command_names = [
        "impact", "calls", "imports", "exceptions", "sig-impact",
        "side-effects", "scopes", "async-hazards", "bytecode-surface",
        "q", "report", "index", "cache",
    ]

    for name in command_names:
        cmd = app[name]  # Forces resolution
        assert callable(cmd), f"Command {name} is not callable"
```

#### 11.7.2 Target Files

| Action | File | Changes |
|--------|------|---------|
| Create | `tools/cq/cli_app/commands/analysis.py` | Move analysis commands (impact, calls, etc.) |
| Create | `tools/cq/cli_app/commands/query.py` | Move query command |
| Create | `tools/cq/cli_app/commands/report.py` | Move report command |
| Create | `tools/cq/cli_app/commands/admin.py` | Move admin commands (index, cache) |
| Modify | `tools/cq/cli_app/app.py` | Replace inline commands with lazy registrations |
| Create | `tests/unit/cq/test_cli_startup.py` | Startup time benchmark test |

#### 11.7.3 Deprecation / Deletion

| What | Location | Action |
|------|----------|--------|
| Inline command definitions | `app.py:61-776` | Move to `commands/` modules |
| `_build_filters()` helper | `app.py:783-843` | Already deleted in 11.5 |

#### 11.7.4 Implementation Checklist

- [ ] Create `tools/cq/cli_app/commands/analysis.py` with 9 analysis commands
- [ ] Create `tools/cq/cli_app/commands/query.py` with `q` command
- [ ] Create `tools/cq/cli_app/commands/report.py` with `report` command
- [ ] Create `tools/cq/cli_app/commands/admin.py` with `index`, `cache` commands
- [ ] Update `tools/cq/cli_app/commands/__init__.py` (can remain empty)
- [ ] Replace inline commands in `app.py` with `app.command("path:func")` registrations
- [ ] Verify Groups are defined in `app.py` (required for lazy loading)
- [ ] Create startup time benchmark test
- [ ] Measure: `time uv run cq --help` < 100ms
- [ ] Test all commands still work after refactoring
- [ ] Run: `uv run pytest tests/unit/cq/test_cli_startup.py -v`

---

### 11.8 Scope: Parameter and Group Validators (Section 4.3)

#### 11.8.1 Key Code Patterns

**Parameter validators:**
```python
# commands/admin.py - Path validator example
from cyclopts import Parameter, validators
from pathlib import Path

def index(
    *,
    root: Annotated[
        Path | None,
        Parameter(
            help="Repository root",
            validator=validators.Path(exists=True, dir_okay=True, file_okay=False),
        ),
    ] = None,
    # ... other params
) -> int:
    ...
```

**Number validator (already added in 11.6):**
```python
# params.py - In FilterOptions
limit: Annotated[
    int | None,
    Parameter(
        help="Maximum findings",
        validator=validators.Number(gte=1),  # Must be >= 1
    ),
] = None
```

**Group validator for mutually exclusive options:**
```python
# commands/admin.py - MutuallyExclusive validator
from cyclopts import Group, Parameter, validators

# Hidden group (no panel in help) for mutual exclusion
index_actions = Group(
    "",  # Empty name = hidden group
    validator=validators.MutuallyExclusive(),
    default_parameter=Parameter(negative=""),  # No --no-rebuild, etc.
)

def index(
    *,
    rebuild: Annotated[bool, Parameter(group=index_actions, help="Force full rebuild")] = False,
    stats: Annotated[bool, Parameter(group=index_actions, help="Show statistics")] = False,
    clear: Annotated[bool, Parameter(group=index_actions, help="Clear the index")] = False,
    root: Annotated[Path | None, Parameter(help="Repository root")] = None,
    ctx: Annotated[CliContext, Parameter(parse=False)],
) -> int:
    """Manage the ast-grep scan index cache."""
    # Only one of rebuild/stats/clear can be True
    ...


cache_actions = Group(
    "",
    validator=validators.MutuallyExclusive(),
    default_parameter=Parameter(negative=""),
)

def cache(
    *,
    stats: Annotated[bool, Parameter(group=cache_actions, help="Show statistics")] = False,
    clear: Annotated[bool, Parameter(group=cache_actions, help="Clear the cache")] = False,
    root: Annotated[Path | None, Parameter(help="Repository root")] = None,
    ctx: Annotated[CliContext, Parameter(parse=False)],
) -> int:
    """Manage the query result cache."""
    ...
```

**Validator test:**
```python
# tests/unit/cq/test_cli_validators.py
import pytest
from tools.cq.cli_app.app import app

def test_mutually_exclusive_index_actions() -> None:
    """Verify --rebuild and --stats cannot be used together."""
    with pytest.raises(SystemExit):
        app.parse_args(
            ["index", "--rebuild", "--stats"],
            exit_on_error=True,
            print_error=False,
        )


def test_limit_must_be_positive() -> None:
    """Verify --limit requires positive integer."""
    with pytest.raises(SystemExit):
        app.parse_args(
            ["calls", "foo", "--limit", "0"],
            exit_on_error=True,
            print_error=False,
        )


def test_root_path_must_exist() -> None:
    """Verify --root path must exist."""
    with pytest.raises(SystemExit):
        app.parse_args(
            ["index", "--root", "/nonexistent/path"],
            exit_on_error=True,
            print_error=False,
        )
```

#### 11.8.2 Target Files

| Action | File | Changes |
|--------|------|---------|
| Modify | `tools/cq/cli_app/commands/admin.py` | Add `index_actions`, `cache_actions` groups with `MutuallyExclusive` |
| Modify | `tools/cq/cli_app/commands/admin.py` | Add `validators.Path(exists=True)` to root param |
| Modify | `tools/cq/cli_app/params.py` | Already has `validators.Number(gte=1)` from 11.6 |
| Create | `tests/unit/cq/test_cli_validators.py` | Validator tests |

#### 11.8.3 Deprecation / Deletion

None - this is additive.

#### 11.8.4 Implementation Checklist

- [ ] Create `index_actions` group with `validators.MutuallyExclusive()`
- [ ] Create `cache_actions` group with `validators.MutuallyExclusive()`
- [ ] Apply groups to `rebuild`, `stats`, `clear` params in `index` command
- [ ] Apply groups to `stats`, `clear` params in `cache` command
- [ ] Add `validators.Path(exists=True, dir_okay=True, file_okay=False)` to root params
- [ ] Test: `cq index --rebuild --stats` fails with mutual exclusion error
- [ ] Test: `cq cache --clear --stats` fails with mutual exclusion error
- [ ] Test: `cq calls foo --limit 0` fails with validation error
- [ ] Run: `uv run pytest tests/unit/cq/test_cli_validators.py -v`

---

### 11.9 Scope: Shell Completion (Section 5.1)

#### 11.9.1 Key Code Patterns

**Register completion command:**
```python
# app.py - Register shell completion
app.register_install_completion_command(
    name="--install-completion",
    add_to_startup=False,  # User must source manually (safer)
    group=admin_group,
    help="Install shell completion scripts (bash/zsh/fish)",
)
```

**Usage:**
```bash
# Install completion
cq --install-completion --shell zsh

# User adds to .zshrc:
source ~/.zsh/completions/_cq

# Now tab completion works:
cq imp<TAB>  # → impact
cq impact --<TAB>  # → --param, --depth, --format, ...
```

#### 11.9.2 Target Files

| Action | File | Changes |
|--------|------|---------|
| Modify | `tools/cq/cli_app/app.py` | Add `app.register_install_completion_command()` |
| Create | `docs/shell_completion.md` | Documentation for shell completion setup |

#### 11.9.3 Deprecation / Deletion

None - this is additive.

#### 11.9.4 Implementation Checklist

- [ ] Add `app.register_install_completion_command()` call
- [ ] Set `add_to_startup=False` (user must opt-in)
- [ ] Set `group=admin_group` for proper help organization
- [ ] Test: `cq --install-completion --shell zsh` works
- [ ] Test: `cq --install-completion --shell bash` works
- [ ] Create documentation for shell completion setup
- [ ] Test completion in interactive shell

---

### 11.10 Scope: Rich Formatted Exceptions (Section 5.2)

#### 11.10.1 Key Code Patterns

**Install rich traceback handler:**
```python
# app.py - Rich traceback for better error display
from rich.console import Console
from rich.traceback import install as install_rich_traceback

# Error console for stderr
error_console = Console(stderr=True, force_terminal=True)

# Install rich traceback handler (only shows in verbose mode)
install_rich_traceback(
    console=error_console,
    show_locals=False,  # Don't expose locals (security)
    width=100,
    suppress=[],  # Don't suppress any frames
)

app = App(
    name="cq",
    error_console=error_console,
    # ... other options
)
```

**Verbose-aware exception display:**
```python
# result.py - Handle exceptions based on verbosity
def handle_exception(exc: Exception, ctx: CliContext) -> int:
    """Handle exception with appropriate verbosity.

    Parameters
    ----------
    exc
        Exception that occurred.
    ctx
        CLI context with verbosity level.

    Returns
    -------
    int
        Exit code (always 1 for exceptions).
    """
    if ctx.verbose >= 2:
        # Full traceback in verbose mode
        raise exc
    else:
        # Clean error message for normal mode
        error_console.print(f"[red]Error:[/red] {exc}")
        if ctx.verbose >= 1:
            error_console.print(f"[dim]Use -vv for full traceback[/dim]")
        return 1
```

#### 11.10.2 Target Files

| Action | File | Changes |
|--------|------|---------|
| Modify | `tools/cq/cli_app/app.py` | Add `error_console`, `install_rich_traceback()`, `error_console=` arg |
| Modify | `tools/cq/cli_app/result.py` | Add `handle_exception()` function |
| Modify | `tools/cq/cli_app/app.py` | Wrap command execution in try/except using handle_exception |

#### 11.10.3 Deprecation / Deletion

None - this is additive.

#### 11.10.4 Implementation Checklist

- [ ] Create `error_console = Console(stderr=True)`
- [ ] Call `install_rich_traceback(console=error_console, show_locals=False)`
- [ ] Pass `error_console=error_console` to `App()` constructor
- [ ] Add `handle_exception(exc, ctx)` function to `result.py`
- [ ] Wrap command execution in launcher with try/except
- [ ] Test: normal error shows clean message
- [ ] Test: `-vv` shows full rich traceback
- [ ] Ensure no sensitive data (locals) exposed in tracebacks

---

## 12. Execution Order

Execute scopes in this order to minimize conflicts and enable incremental testing:

| Phase | Scope | Section | Dependencies |
|-------|-------|---------|--------------|
| 1 | Remove Global `negative=""` | 11.1 | None |
| 2 | Enum Coercion Fix | 11.2 | None |
| 3 | Config Integration | 11.3 | None |
| 4 | Meta-App Pattern | 11.4 | 11.1, 11.2, 11.3 |
| 5 | Result Envelope | 11.5 | 11.4 |
| 6 | Namespace Flattening | 11.6 | 11.4, 11.5 |
| 7 | Lazy Loading | 11.7 | 11.6 |
| 8 | Validators | 11.8 | 11.7 |
| 9 | Shell Completion | 11.9 | 11.7 |
| 10 | Rich Exceptions | 11.10 | 11.4 |

**Critical path:** 11.1 → 11.4 → 11.5 → 11.6 → 11.7

---

## 13. Post-Implementation Verification

### 13.1 Quality Gate

```bash
# Format and lint
uv run ruff format && uv run ruff check --fix

# Type checking
uv run pyrefly check && uv run pyright

# Unit tests
uv run pytest tests/unit/cq/test_cli_*.py -v

# Integration tests
uv run pytest tests/integration/cq/ -v

# Full test suite
uv run pytest -m "not e2e" -v
```

### 13.2 Performance Verification

```bash
# Startup time (target: <100ms)
time uv run cq --help

# Command execution
uv run cq calls main
uv run cq q "entity=function name=main"
```

### 13.3 Behavioral Verification

```bash
# Global options appear once in help
uv run cq --help | grep -A20 "Global Options"

# Config precedence
CQ_FORMAT=json uv run cq calls foo --format md  # Should use md (CLI wins)

# --no-config escape hatch
CQ_ROOT=/bad/path uv run cq calls foo --no-config  # Should auto-detect root

# Negative flags work correctly
uv run cq q "entity=function" --no-cache  # Should work
uv run cq index --no-rebuild 2>&1 | grep -i error  # Should fail

# Mutual exclusion
uv run cq index --rebuild --stats 2>&1 | grep -i "mutually exclusive"

# Enum tokens preserved
uv run cq calls foo --format mermaid-class  # Should parse correctly
```

### 13.4 Files Summary

**Files to Create:**
| File | Lines (est.) |
|------|-------------|
| `tools/cq/cli_app/config.py` | ~50 |
| `tools/cq/cli_app/commands/analysis.py` | ~300 |
| `tools/cq/cli_app/commands/query.py` | ~100 |
| `tools/cq/cli_app/commands/report.py` | ~120 |
| `tools/cq/cli_app/commands/admin.py` | ~150 |
| `tests/unit/cq/test_cli_negative_flags.py` | ~30 |
| `tests/unit/cq/test_cli_enum_tokens.py` | ~40 |
| `tests/unit/cq/test_cli_config.py` | ~80 |
| `tests/unit/cq/test_cli_meta_app.py` | ~60 |
| `tests/unit/cq/test_cli_result_handling.py` | ~70 |
| `tests/unit/cq/test_cli_filter_options.py` | ~50 |
| `tests/unit/cq/test_cli_startup.py` | ~40 |
| `tests/unit/cq/test_cli_validators.py` | ~50 |
| `docs/shell_completion.md` | ~30 |

**Files to Modify:**
| File | Changes |
|------|---------|
| `tools/cq/cli_app/app.py` | Major refactor (~844 lines → ~150 lines) |
| `tools/cq/cli_app/context.py` | Add global option fields to CliContext |
| `tools/cq/cli_app/params.py` | Add `@Parameter(name="*")`, `to_filter_config()` |
| `tools/cq/cli_app/result.py` | Refactor `handle_result()`, delete `output_result()` |
| `tools/cq/cli_app/types.py` | No changes (already correct) |

**Net line change:** ~-500 lines (844 → ~150 in app.py, +720 in commands/, +450 in tests)
