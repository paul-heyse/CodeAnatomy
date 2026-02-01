# CodeAnatomy Cyclopts CLI Design Plan

## Executive Summary

This document proposes a **best-in-class cyclopts-based CLI** for CodeAnatomy, leveraging the library's type-hint-driven parsing, configuration composition, and structured error handling to expose the full power of the CPG pipeline while maintaining a clean, discoverable user experience.

### Key Design Decisions

1. **Cyclopts as Foundation**: Type-hint-driven CLI binding matches our frozen dataclass architecture
2. **Reuse Existing Types via User Class Flattening**: Direct mapping to `GraphProductBuildRequest`, `ScipIndexConfig`, `ExecutorConfig`, `IncrementalConfig`, `RepoScopeConfig`, and `ScipIdentityOverrides` using Cyclopts' `Parameter(name="*")` flattening—no bespoke `BuildOptions` to avoid drift
3. **Layered Configuration with Explicit Precedence**: CLI args > environment variables > config file > Python defaults; explicit `env_var=` mapping per parameter to avoid inventing new env names; config file contents forwarded (and normalized) to `GraphProductBuildRequest.config`
4. **Full Parity with Existing Scripts**: All `run_full_pipeline.py` flags including `--disable-scip`, SCIP, executor remote, writer strategy; all delta maintenance ops; determinism tier aliases; advanced `ScipIndexSettings` fields documented as config-only
5. **Advanced Cyclopts Features**: Lazy loading, meta-app config chaining, keyword-only `parse=False` injection, group validators, `result_action` variants, config-level mutual exclusion validation, dynamic completion install commands
6. **Observability-First**: Telemetry-instrumented invocation wrapper with structured error classification
7. **Path Resolution Contract**: Relative paths for `incremental_state_dir` and SCIP paths resolved against `repo_root` before building config types

**Estimated Scope**: ~2,500 lines of CLI code across 12-15 modules.

---

## Design Principles

### 1. Type Reuse via User Class Flattening

Reuse existing frozen dataclasses directly using Cyclopts' `Parameter(name="*")` flattening:

```python
# DO: Use Parameter(name="*") to flatten config types
@Parameter(name="*")
@dataclass(frozen=True)
class CliExecutorConfig:
    """Wrapper for CLI flattening of ExecutorConfig fields."""
    kind: ExecutorKind = "multiprocessing"
    max_tasks: int = 4
    remote_kind: ExecutorKind | None = None
    remote_max_tasks: int | None = None
    cost_threshold: float | None = None

def build_command(
    repo_root: Path,
    executor: Annotated[CliExecutorConfig | None, Parameter(name="*")] = None,
) -> int:
    # Flattening produces --kind, --max-tasks, etc. instead of --executor.kind
    ...

# DON'T: Create parallel BuildOptions that drifts from product_build.py
```

### 2. Explicit Environment Variable Mapping

**Do not use `env_var=True` in `App.default_parameter`**—this silently invents new env names like `CODEANATOMY_INCREMENTAL_STATE_DIR` that don't exist in `inputs.py` and can confuse precedence.

Instead, map env vars explicitly per parameter to match existing `CODEANATOMY_*` names:

```python
# DO: Explicit env_var mapping to existing pipeline env vars
incremental_state_dir: Annotated[
    Path | None,
    Parameter(
        name="--incremental-state-dir",
        env_var="CODEANATOMY_STATE_DIR",  # Matches inputs.py
    ),
] = None

# DON'T: Global env_var=True creates CODEANATOMY_INCREMENTAL_STATE_DIR
app = App(default_parameter=Parameter(env_var=True))  # BAD
```

### 3. Config File Contents Forwarded to Pipeline

TOML config layers fill CLI parameter defaults, but pipeline config keys (cache policy, plan hooks, graph adapter options, etc.) **must also reach the engine** via `GraphProductBuildRequest.config`. This is a separate config payload from CLI-bound values and must be loaded even when `--config` is not specified:

```python
# Meta-app captures config file contents and forwards to request.config
@app.meta.default
def meta_launcher(*tokens, config_file: str | None = None) -> int:
    # Load full config file (not just CLI-mapped keys).
    # Use the same default search rules as cyclopts config:
    #   codeanatomy.toml or pyproject.toml[tool.codeanatomy]
    config_contents = load_effective_config(config_file)

    # Normalize nested config to flat keys expected by driver_factory
    config_contents = normalize_config_contents(config_contents)

    # Forward to request.config for pipeline-internal config keys
    request = GraphProductBuildRequest(
        ...,
        config=config_contents,  # Includes cache_policy, plan_hooks, etc.
    )

### 3.1 Config Normalization (Nested TOML → Flat Keys)

`driver_factory.py` expects **flat config keys** like `plan_allow_partial`, `plan_requested_tasks`, `cache_policy_profile`, etc. If you keep nested TOML sections (e.g., `[plan] allow_partial = true`), normalize them before forwarding:

```python
def load_effective_config(config_file: str | None) -> dict[str, object]:
    """Load config contents from codeanatomy.toml / pyproject.toml or explicit --config."""
    if config_file:
        path = Path(config_file)
        if path.exists():
            with path.open("rb") as f:
                return tomllib.load(f)
        return {}
    # Default search (mirrors cyclopts.config.Toml in app.config):
    # 1) codeanatomy.toml in repo root or parents
    # 2) pyproject.toml with [tool.codeanatomy]
    cfg = {}
    codeanatomy_path = find_in_parents("codeanatomy.toml")
    if codeanatomy_path:
        with codeanatomy_path.open("rb") as f:
            cfg = tomllib.load(f)
    pyproject_path = find_in_parents("pyproject.toml")
    if pyproject_path:
        with pyproject_path.open("rb") as f:
            pyproject = tomllib.load(f)
        cfg = pyproject.get("tool", {}).get("codeanatomy", cfg)
    return cfg


def normalize_config_contents(config: dict[str, object]) -> dict[str, object]:
    """Flatten nested config keys to match driver_factory expectations."""
    flat: dict[str, object] = {}

    # Pass through already-flat keys
    flat.update(config)

    # [plan] → plan_* keys
    plan = config.get("plan")
    if isinstance(plan, dict):
        _copy_key(flat, plan, "allow_partial", "plan_allow_partial")
        _copy_key(flat, plan, "requested_tasks", "plan_requested_tasks")
        _copy_key(flat, plan, "impacted_tasks", "plan_impacted_tasks")
        _copy_key(flat, plan, "enable_metric_scheduling", "enable_metric_scheduling")
        _copy_key(flat, plan, "enable_plan_diagnostics", "enable_plan_diagnostics")
        _copy_key(flat, plan, "enable_plan_task_submission_hook", "enable_plan_task_submission_hook")
        _copy_key(flat, plan, "enable_plan_task_grouping_hook", "enable_plan_task_grouping_hook")
        _copy_key(flat, plan, "enforce_plan_task_submission", "enforce_plan_task_submission")

    # [cache] → cache_* keys
    cache = config.get("cache")
    if isinstance(cache, dict):
        _copy_key(flat, cache, "policy_profile", "cache_policy_profile")
        _copy_key(flat, cache, "path", "cache_path")
        _copy_key(flat, cache, "log_to_file", "cache_log_to_file")

    # [graph_adapter] → graph_adapter_* keys
    graph_adapter = config.get("graph_adapter")
    if isinstance(graph_adapter, dict):
        _copy_key(flat, graph_adapter, "kind", "graph_adapter_kind")
        _copy_key(flat, graph_adapter, "options", "graph_adapter_options")

    return flat


def _copy_key(
    target: dict[str, object],
    source: dict[str, object],
    source_key: str,
    dest_key: str,
) -> None:
    value = source.get(source_key)
    if value is not None:
        target[dest_key] = value
```
```

### 4. Determinism Contract Exposure

The CLI must make the determinism tier visible and controllable:
- Expose `--determinism-tier` with clear semantics
- Support legacy aliases (tier2/tier1/tier0, canonical/stable/fast)
- Default to `PLAN_PARALLEL` with `STABLE_SET` guarantees

### 5. Environment Variable Precedence

Many pipeline inputs already read `CODEANATOMY_*` directly in `inputs.py`. The CLI must clearly define:
- CLI flags override environment variables (via `GraphProductBuildRequest.overrides`)
- Environment variables fill missing CLI values (explicit `env_var=` per param)
- Config file provides base defaults (forwarded to `GraphProductBuildRequest.config`)
- **Critical**: CLI-specified values go into `overrides` to take precedence over env vars that `inputs.py` would otherwise read

**Important**: Do **not** use `Env("CODEANATOMY")` in `app.config`. It manufactures new env var names and can override CLI values unexpectedly. Restrict env lookup to explicit `env_var=` on parameters.

### 6. Path Resolution Contract

Relative paths for `incremental_state_dir` and SCIP path args (`scip_output_dir`, `scip_index_path_override`, `env_json_path`) must be resolved against `repo_root` before building config types:

```python
def _resolve_path(repo_root: Path, path: Path | str | None) -> Path | None:
    """Resolve relative path against repo_root."""
    if path is None:
        return None
    p = Path(path)
    return p if p.is_absolute() else repo_root / p
```

### 7. Repo-Scope + Tree-Sitter Overrides Must Flow Through `overrides`

Repo-scope flags (`include_globs`, `exclude_globs`, `include_submodules`, etc.) and `enable_tree_sitter`
are **scalar inputs** in `inputs.py` and only change via `execute(overrides={...})`. Ensure these are
placed in `GraphProductBuildRequest.overrides`:

```python
overrides: dict[str, object] = {}
overrides["include_globs"] = list(include_globs)
overrides["exclude_globs"] = list(exclude_globs)
overrides["include_untracked"] = include_untracked
overrides["include_submodules"] = include_submodules
overrides["include_worktrees"] = include_worktrees
overrides["follow_symlinks"] = follow_symlinks
overrides["external_interface_depth"] = external_interface_depth
overrides["enable_tree_sitter"] = enable_tree_sitter
```

---

## Command Hierarchy

```
codeanatomy
├── build              # Main CPG pipeline (primary entry point)
├── plan               # Show/validate execution plan without running
├── delta              # Delta Lake maintenance operations
│   ├── vacuum         # Run Delta vacuum
│   ├── checkpoint     # Create Delta checkpoint
│   ├── cleanup-log    # Cleanup expired log files
│   ├── export         # Clone Delta snapshot to target
│   └── restore        # Restore table to prior version/timestamp
├── diag               # Diagnostics and reporting
├── config             # Configuration management
│   ├── show           # Show effective configuration
│   ├── validate       # Validate configuration file
│   └── init           # Initialize configuration template
└── version            # Show version and engine information
```

---

## Core Cyclopts Application Architecture

### App Configuration with Advanced Features

```python
from __future__ import annotations

from typing import Annotated, Literal
from cyclopts import App, Group, Parameter, validators
from cyclopts.config import Toml

# === Help Panel Groups ===
# Organize meta params into "Session" panel, standard admin into "Admin" panel

session_group = Group(
    "Session",
    help="Session and run context options.",
    sort_key=0,
)

admin_group = Group(
    "Admin",
    help="Administrative commands and help.",
    sort_key=99,
)

# === Global Parameter Defaults ===
# Use App.default_parameter for consistent flag semantics
# IMPORTANT: Do NOT use env_var=True globally—map env vars explicitly per parameter

app = App(
    name="codeanatomy",
    help="CodeAnatomy CPG Builder - Inference-driven Code Property Graph generation.",
    help_format="markdown",
    # NOTE: version is handled by a custom version_command to avoid collision
    # with --version flag in certain cyclopts versions
    version=None,
    version_flags=[],
    # Global defaults for all parameters (no env_var=True!)
    default_parameter=Parameter(
        # Consistent negative flag prefix
        negative="no-",
        # Do NOT set env_var=True here—explicit mapping per parameter
    ),
    # Layered configuration: CLI > TOML > defaults
    # NOTE: env vars are mapped explicitly per-parameter (do not use Env("CODEANATOMY"))
    config=[
        Toml("codeanatomy.toml", must_exist=False, search_parents=True),
        Toml(
            "pyproject.toml",
            root_keys=("tool", "codeanatomy"),
            must_exist=False,
            search_parents=True,
        ),
    ],
    # Error handling
    exit_on_error=True,
    print_error=True,
    help_on_error=False,
)

# === Version Command (avoids --version collision) ===
@app.command(group=admin_group)
def version() -> None:
    """Show version and engine information."""
    from cli.version import print_version_info
    print_version_info()

# === Lazy Loading for Fast Startup ===
# Heavy imports (DataFusion, Hamilton) are deferred until command execution

app.command("build", "cli.commands.build:build_command")
app.command("plan", "cli.commands.plan:plan_command")
app.command("diag", "cli.commands.diag:diag_command")

# Delta subapp with lazy-loaded commands
delta_app = App(name="delta", help="Delta Lake maintenance operations.")
delta_app.command("vacuum", "cli.commands.delta:vacuum_command")
delta_app.command("checkpoint", "cli.commands.delta:checkpoint_command")
delta_app.command("cleanup-log", "cli.commands.delta:cleanup_log_command")
delta_app.command("export", "cli.commands.delta:export_command")
delta_app.command("restore", "cli.commands.delta:restore_command")
app.command(delta_app)

# Config subapp (light imports, no lazy loading needed)
config_app = App(name="config", help="Configuration management.")
app.command(config_app)
```

### Meta-App for Config File Selection and Context Injection

```python
from cyclopts import App, Parameter
from cyclopts.config import Toml
from typing import Annotated
from pathlib import Path
import tomllib

# === Meta-App Pattern ===
# Capture --config flag and inject run context (logger, tracer, run_id)
# before dispatching to actual commands
# Session params go in session_group for organized help panels

@app.meta.default
def meta_launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    config_file: Annotated[
        str | None,
        Parameter(
            name="--config",
            help="Path to configuration file (overrides default search).",
            group=session_group,
        ),
    ] = None,
    run_id: Annotated[
        str | None,
        Parameter(
            name="--run-id",
            help="Explicit run identifier (UUID7 generated if not provided).",
            group=session_group,
        ),
    ] = None,
    log_level: Annotated[
        Literal["DEBUG", "INFO", "WARNING", "ERROR"],
        Parameter(
            name="--log-level",
            help="Logging verbosity level.",
            env_var="CODEANATOMY_LOG_LEVEL",
            group=session_group,
        ),
    ] = "INFO",
) -> int:
    """Meta launcher for config selection and context injection."""
    import logging
    from utils.uuid_factory import uuid7_str

    # Configure logging
    logging.basicConfig(level=log_level.upper())

    # Load and parse config file contents (for forwarding to request.config).
    # Always load defaults when --config is not provided.
    config_contents = load_effective_config(config_file)
    config_contents = normalize_config_contents(config_contents)

    # Override cyclopts config chain for CLI param defaults when explicit config provided
    if config_file:
        app.config = [Toml(config_file, must_exist=True)]

    # Build run context for injection (includes config_contents for forwarding)
    effective_run_id = run_id or uuid7_str()
    run_context = RunContext(
        run_id=effective_run_id,
        log_level=log_level,
        config_contents=config_contents,  # Forward to request.config
    )

    # Parse and dispatch with context injection
    command, bound, ignored = app.parse_args(tokens, exit_on_error=False)

    # Inject run context for commands that declared keyword-only parse=False param
    if "run_context" in ignored:
        return command(*bound.args, **bound.kwargs, run_context=run_context)

    return command(*bound.args, **bound.kwargs)
```

---

## Build Command — Full Parity with `run_full_pipeline.py`

### Parameter Groups

```python
from cyclopts import Group, validators

# Organized help panels with explicit sort order
output_group = Group(
    "Output",
    help="Configure output directory and artifact selection.",
    sort_key=1,
)

execution_group = Group(
    "Execution",
    help="Control pipeline execution strategy and parallelism.",
    sort_key=2,
)

scip_group = Group(
    "SCIP Configuration",
    help="Configure SCIP semantic index extraction.",
    sort_key=3,
)

incremental_group = Group(
    "Incremental Processing",
    help="Configure incremental/CDF-based processing.",
    sort_key=4,
)

repo_scope_group = Group(
    "Repository Scope",
    help="Control which files are included in analysis.",
    sort_key=5,
)

graph_adapter_group = Group(
    "Graph Adapter",
    help="Configure Hamilton graph adapter backend.",
    sort_key=6,
)

advanced_group = Group(
    "Advanced",
    help="Advanced configuration options (most set via config file).",
    sort_key=7,
)
```

### Build Command Implementation

```python
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Literal

from cyclopts import Parameter, validators
from core_types import DeterminismTier, JsonValue
from graph import GraphProductBuildRequest, build_graph_product
from hamilton_pipeline.types import (
    ExecutionMode,
    ExecutorConfig,
    GraphAdapterConfig,
    RepoScopeConfig,
    ScipIndexConfig,
    ScipIdentityOverrides,
    TreeSitterConfig,
)
from semantics.incremental import IncrementalConfig


# === Run Context (injected via keyword-only parse=False) ===
@dataclass(frozen=True)
class RunContext:
    """Injected run context from meta-app."""
    run_id: str
    log_level: str
    config_contents: Mapping[str, JsonValue] = field(default_factory=dict)


# === Determinism Tier with Aliases ===
# Support legacy aliases: tier2/tier1/tier0, canonical/stable/fast

DETERMINISM_ALIASES: dict[str, DeterminismTier] = {
    "tier2": DeterminismTier.CANONICAL,
    "canonical": DeterminismTier.CANONICAL,
    "tier1": DeterminismTier.STABLE_SET,
    "stable": DeterminismTier.STABLE_SET,
    "stable_set": DeterminismTier.STABLE_SET,
    "tier0": DeterminismTier.BEST_EFFORT,
    "fast": DeterminismTier.BEST_EFFORT,
    "best_effort": DeterminismTier.BEST_EFFORT,
}

DeterminismTierChoice = Literal[
    "tier2", "canonical",
    "tier1", "stable", "stable_set",
    "tier0", "fast", "best_effort",
]


# === Path Resolution Helper ===
def _resolve_path(repo_root: Path, path: Path | str | None) -> Path | None:
    """Resolve relative path against repo_root."""
    if path is None:
        return None
    p = Path(path)
    return p if p.is_absolute() else repo_root / p


def build_command(
    # === Required ===
    repo_root: Annotated[
        Path,
        Parameter(
            help="Root directory of the repository to analyze.",
            validator=validators.Path(exists=True, dir_okay=True, file_okay=False),
        ),
    ],

    # === Output Control (group=output_group) ===
    output_dir: Annotated[
        Path | None,
        Parameter(
            name=["--output-dir", "-o"],
            help="Directory for CPG outputs. Defaults to <repo_root>/build.",
            group=output_group,
        ),
    ] = None,

    work_dir: Annotated[
        Path | None,
        Parameter(
            name="--work-dir",
            help="Working directory for intermediate artifacts.",
            group=output_group,
        ),
    ] = None,

    include_quality: Annotated[
        bool,
        Parameter(
            name="--include-quality",
            help="Include quality metrics in output.",
            group=output_group,
        ),
    ] = True,

    include_errors: Annotated[
        bool,
        Parameter(
            name="--include-errors",
            help="Include extraction error artifacts.",
            group=output_group,
        ),
    ] = True,

    include_manifest: Annotated[
        bool,
        Parameter(
            name="--include-manifest",
            help="Include run manifest in output.",
            group=output_group,
        ),
    ] = True,

    include_run_bundle: Annotated[
        bool,
        Parameter(
            name="--include-run-bundle",
            help="Include run bundle directory.",
            group=output_group,
        ),
    ] = True,

    # === Execution Configuration (group=execution_group) ===
    execution_mode: Annotated[
        ExecutionMode,
        Parameter(
            name="--execution-mode",
            help="Pipeline execution strategy: deterministic_serial, plan_parallel, plan_parallel_remote.",
            group=execution_group,
        ),
    ] = ExecutionMode.PLAN_PARALLEL,

    executor_kind: Annotated[
        Literal["threadpool", "multiprocessing", "dask", "ray"] | None,
        Parameter(
            name="--executor-kind",
            help="Primary executor backend for parallel execution.",
            group=execution_group,
        ),
    ] = None,

    executor_max_tasks: Annotated[
        int | None,
        Parameter(
            name="--executor-max-tasks",
            help="Maximum concurrent tasks for primary executor.",
            validator=validators.Number(gte=1),
            group=execution_group,
        ),
    ] = None,

    executor_remote_kind: Annotated[
        Literal["threadpool", "multiprocessing", "dask", "ray"] | None,
        Parameter(
            name="--executor-remote-kind",
            help="Remote executor backend for scan/high-cost tasks.",
            group=execution_group,
        ),
    ] = None,

    executor_remote_max_tasks: Annotated[
        int | None,
        Parameter(
            name="--executor-remote-max-tasks",
            help="Maximum concurrent tasks for remote executor.",
            validator=validators.Number(gte=1),
            group=execution_group,
        ),
    ] = None,

    executor_cost_threshold: Annotated[
        float | None,
        Parameter(
            name="--executor-cost-threshold",
            help="Cost threshold for routing tasks to remote executor.",
            validator=validators.Number(gt=0.0),
            group=execution_group,
        ),
    ] = None,

    # === Determinism Control (group=execution_group) ===
    determinism_tier: Annotated[
        DeterminismTierChoice | None,
        Parameter(
            name="--determinism-tier",
            help="Determinism level: canonical/tier2, stable_set/tier1/stable, best_effort/tier0/fast.",
            group=execution_group,
        ),
    ] = None,

    runtime_profile: Annotated[
        str | None,
        Parameter(
            name="--runtime-profile",
            help="Named runtime profile from configuration.",
            env_var="CODEANATOMY_RUNTIME_PROFILE",
            group=execution_group,
        ),
    ] = None,

    writer_strategy: Annotated[
        Literal["arrow", "datafusion"] | None,
        Parameter(
            name="--writer-strategy",
            help="Writer strategy for materialization.",
            group=execution_group,
        ),
    ] = None,

    # === SCIP Configuration (group=scip_group) ===
    disable_scip: Annotated[
        bool,
        Parameter(
            name="--disable-scip",
            help="Disable SCIP indexing (not recommended; produces less useful outputs).",
            group=scip_group,
        ),
    ] = False,

    scip_output_dir: Annotated[
        str | None,
        Parameter(
            name="--scip-output-dir",
            help="SCIP output directory (relative to repo root unless absolute). Default: build/scip.",
            group=scip_group,
        ),
    ] = None,

    scip_index_path_override: Annotated[
        str | None,
        Parameter(
            name="--scip-index-path-override",
            help="Override the index.scip path.",
            group=scip_group,
        ),
    ] = None,

    scip_python_bin: Annotated[
        str,
        Parameter(
            name="--scip-python-bin",
            help="scip-python executable to use.",
            group=scip_group,
        ),
    ] = "scip-python",

    scip_target_only: Annotated[
        str | None,
        Parameter(
            name="--scip-target-only",
            help="Optional target file/module to index.",
            group=scip_group,
        ),
    ] = None,

    scip_timeout_s: Annotated[
        int | None,
        Parameter(
            name="--scip-timeout-s",
            help="Timeout in seconds for scip-python.",
            validator=validators.Number(gte=1),
            group=scip_group,
        ),
    ] = None,

    scip_env_json: Annotated[
        str | None,
        Parameter(
            name="--scip-env-json",
            help="Path to SCIP environment JSON.",
            group=scip_group,
        ),
    ] = None,

    node_max_old_space_mb: Annotated[
        int | None,
        Parameter(
            name="--node-max-old-space-mb",
            help="Node.js memory cap (MB) for scip-python.",
            validator=validators.Number(gte=256),
            group=scip_group,
        ),
    ] = None,

    scip_extra_args: Annotated[
        tuple[str, ...],
        Parameter(
            name="--scip-extra-arg",
            help="Extra arguments passed to scip-python (repeatable).",
            group=scip_group,
        ),
    ] = (),

    # === SCIP Identity Overrides (group=scip_group) ===
    scip_project_name: Annotated[
        str | None,
        Parameter(
            name="--scip-project-name",
            help="Override SCIP project name.",
            group=scip_group,
        ),
    ] = None,

    scip_project_version: Annotated[
        str | None,
        Parameter(
            name="--scip-project-version",
            help="Override SCIP project version.",
            group=scip_group,
        ),
    ] = None,

    scip_project_namespace: Annotated[
        str | None,
        Parameter(
            name="--scip-project-namespace",
            help="Override SCIP project namespace.",
            group=scip_group,
        ),
    ] = None,

    # === Incremental Processing (group=incremental_group) ===
    incremental: Annotated[
        bool,
        Parameter(
            name="--incremental",
            help="Enable incremental processing using cached state.",
            group=incremental_group,
        ),
    ] = False,

    incremental_state_dir: Annotated[
        Path | None,
        Parameter(
            name="--incremental-state-dir",
            help="Directory for incremental processing state (relative to repo_root).",
            env_var="CODEANATOMY_STATE_DIR",
            group=incremental_group,
        ),
    ] = None,

    incremental_repo_id: Annotated[
        str | None,
        Parameter(
            name="--incremental-repo-id",
            help="Repository identifier for incremental state.",
            env_var="CODEANATOMY_REPO_ID",
            group=incremental_group,
        ),
    ] = None,

    incremental_impact_strategy: Annotated[
        Literal["hybrid", "symbol_closure", "import_closure"] | None,
        Parameter(
            name="--incremental-impact-strategy",
            help="Strategy for computing incremental impact.",
            env_var="CODEANATOMY_INCREMENTAL_IMPACT_STRATEGY",
            group=incremental_group,
        ),
    ] = None,

    git_base_ref: Annotated[
        str | None,
        Parameter(
            name="--git-base-ref",
            help="Git base ref for incremental diff.",
            env_var="CODEANATOMY_GIT_BASE_REF",
            group=incremental_group,
        ),
    ] = None,

    git_head_ref: Annotated[
        str | None,
        Parameter(
            name="--git-head-ref",
            help="Git head ref for incremental diff.",
            env_var="CODEANATOMY_GIT_HEAD_REF",
            group=incremental_group,
        ),
    ] = None,

    git_changed_only: Annotated[
        bool,
        Parameter(
            name="--git-changed-only",
            help="Process only changed files (no closure expansion).",
            env_var="CODEANATOMY_GIT_CHANGED_ONLY",
            group=incremental_group,
        ),
    ] = False,

    # === Repository Scope (group=repo_scope_group) ===
    include_globs: Annotated[
        tuple[str, ...],
        Parameter(
            name="--include-glob",
            help="Glob patterns for files to include (repeatable).",
            group=repo_scope_group,
        ),
    ] = (),

    exclude_globs: Annotated[
        tuple[str, ...],
        Parameter(
            name="--exclude-glob",
            help="Glob patterns for files to exclude (repeatable).",
            group=repo_scope_group,
        ),
    ] = (),

    include_untracked: Annotated[
        bool,
        Parameter(
            name="--include-untracked",
            help="Include untracked (not git-ignored) files.",
            group=repo_scope_group,
        ),
    ] = True,

    include_submodules: Annotated[
        bool,
        Parameter(
            name="--include-submodules",
            help="Include files from git submodules.",
            group=repo_scope_group,
        ),
    ] = False,

    include_worktrees: Annotated[
        bool,
        Parameter(
            name="--include-worktrees",
            help="Include files from git worktrees.",
            group=repo_scope_group,
        ),
    ] = False,

    follow_symlinks: Annotated[
        bool,
        Parameter(
            name="--follow-symlinks",
            help="Follow symbolic links when scanning.",
            group=repo_scope_group,
        ),
    ] = False,

    external_interface_depth: Annotated[
        Literal["metadata", "full"],
        Parameter(
            name="--external-interface-depth",
            help="Depth for external interface extraction.",
            group=repo_scope_group,
        ),
    ] = "metadata",

    # === Graph Adapter (group=graph_adapter_group) ===
    graph_adapter_kind: Annotated[
        Literal["threadpool", "dask", "ray"] | None,
        Parameter(
            name="--graph-adapter-kind",
            help="Hamilton graph adapter backend (non-dynamic execution).",
            group=graph_adapter_group,
        ),
    ] = None,

    graph_adapter_option: Annotated[
        tuple[str, ...],
        Parameter(
            name="--graph-adapter-option",
            help="Graph adapter option key=value (repeatable).",
            group=graph_adapter_group,
        ),
    ] = (),

    graph_adapter_option_json: Annotated[
        tuple[str, ...],
        Parameter(
            name="--graph-adapter-option-json",
            help="Graph adapter option key=json (repeatable; JSON values).",
            group=graph_adapter_group,
        ),
    ] = (),

    # === Advanced (group=advanced_group) ===
    enable_tree_sitter: Annotated[
        bool,
        Parameter(
            name="--enable-tree-sitter",
            help="Enable tree-sitter extraction for syntax nodes.",
            group=advanced_group,
        ),
    ] = True,

    plan_allow_partial: Annotated[
        bool | None,
        Parameter(
            name="--plan-allow-partial",
            help="Allow partial execution plans (advanced).",
            group=advanced_group,
        ),
    ] = None,

    plan_requested_task: Annotated[
        tuple[str, ...],
        Parameter(
            name="--plan-requested-task",
            help="Explicit task names to request in plan compilation (repeatable).",
            group=advanced_group,
        ),
    ] = (),

    plan_impacted_task: Annotated[
        tuple[str, ...],
        Parameter(
            name="--plan-impacted-task",
            help="Explicit task names to mark as impacted in plan compilation (repeatable).",
            group=advanced_group,
        ),
    ] = (),

    enable_metric_scheduling: Annotated[
        bool | None,
        Parameter(
            name="--enable-metric-scheduling",
            help="Enable metric-based scheduling in plan compilation (advanced).",
            group=advanced_group,
        ),
    ] = None,

    enable_plan_diagnostics: Annotated[
        bool | None,
        Parameter(
            name="--enable-plan-diagnostics",
            help="Enable plan diagnostics emission (advanced).",
            group=advanced_group,
        ),
    ] = None,

    enable_plan_task_submission_hook: Annotated[
        bool | None,
        Parameter(
            name="--enable-plan-task-submission-hook",
            help="Enable plan task submission hook (advanced).",
            group=advanced_group,
        ),
    ] = None,

    enable_plan_task_grouping_hook: Annotated[
        bool | None,
        Parameter(
            name="--enable-plan-task-grouping-hook",
            help="Enable plan task grouping hook (advanced).",
            group=advanced_group,
        ),
    ] = None,

    enforce_plan_task_submission: Annotated[
        bool | None,
        Parameter(
            name="--enforce-plan-task-submission",
            help="Enforce plan task submission policy (advanced).",
            group=advanced_group,
        ),
    ] = None,

    # === Keyword-Only Injected Context (parse=False) ===
    # IMPORTANT: parse=False params MUST be keyword-only for Cyclopts meta injection
    *,
    run_context: Annotated[
        RunContext | None,
        Parameter(parse=False),
    ] = None,

) -> int:
    """Build the Code Property Graph for a repository.

    This is the primary entry point for CPG generation. It runs the full
    four-stage pipeline: extraction, normalization, task scheduling, and
    CPG emission.

    Examples
    --------
    Minimal invocation:
        codeanatomy build .

    With parallel Ray execution:
        codeanatomy build . --executor-kind ray --executor-max-tasks 16

    Canonical determinism for CI:
        codeanatomy build . --determinism-tier canonical --execution-mode deterministic_serial

    Incremental build:
        codeanatomy build . --incremental --incremental-state-dir .codeanatomy/state
    """
    import logging

    logger = logging.getLogger("codeanatomy.pipeline")
    resolved_repo_root = repo_root.resolve()

    # === Resolve relative paths against repo_root ===
    resolved_state_dir = _resolve_path(resolved_repo_root, incremental_state_dir)
    resolved_scip_output = str(_resolve_path(resolved_repo_root, scip_output_dir) or "build/scip")
    resolved_scip_index = str(_resolve_path(resolved_repo_root, scip_index_path_override)) if scip_index_path_override else None
    resolved_env_json = str(_resolve_path(resolved_repo_root, scip_env_json)) if scip_env_json else None

    # === Build ScipIndexConfig (respects --disable-scip) ===
    scip_config = ScipIndexConfig(
        enabled=not disable_scip,
        output_dir=resolved_scip_output,
        index_path_override=resolved_scip_index,
        env_json_path=resolved_env_json,
        scip_python_bin=scip_python_bin,
        target_only=scip_target_only,
        node_max_old_space_mb=node_max_old_space_mb,
        timeout_s=scip_timeout_s,
        extra_args=scip_extra_args,
        # Advanced fields set via config file only (see ScipIndexSettings):
        # generate_env_json, scip_cli_bin, use_incremental_shards,
        # shards_dir, shards_manifest_path, run_scip_print, scip_print_path,
        # run_scip_snapshot, scip_snapshot_dir, scip_snapshot_comment_syntax,
        # run_scip_test, scip_test_args
    )

    # === Build ScipIdentityOverrides ===
    scip_identity = None
    if scip_project_name or scip_project_version or scip_project_namespace:
        scip_identity = ScipIdentityOverrides(
            project_name_override=scip_project_name,
            project_version_override=scip_project_version,
            project_namespace_override=scip_project_namespace,
        )

    # === Build ExecutorConfig ===
    executor_config = None
    if any([
        executor_kind,
        executor_max_tasks,
        executor_remote_kind,
        executor_remote_max_tasks,
        executor_cost_threshold,
    ]):
        executor_config = ExecutorConfig(
            kind=executor_kind or "multiprocessing",
            max_tasks=executor_max_tasks or 4,
            remote_kind=executor_remote_kind,
            remote_max_tasks=executor_remote_max_tasks,
            cost_threshold=executor_cost_threshold,
        )

    # === Build GraphAdapterConfig ===
    graph_adapter_config = None
    if graph_adapter_kind:
        adapter_options = _parse_kv_pairs(graph_adapter_option) if graph_adapter_option else None
        adapter_options_json = (
            _parse_kv_pairs_json(graph_adapter_option_json) if graph_adapter_option_json else None
        )
        if adapter_options_json:
            adapter_options = {**(adapter_options or {}), **adapter_options_json}
        graph_adapter_config = GraphAdapterConfig(
            kind=graph_adapter_kind,
            options=adapter_options,
        )

    # === Build RepoScopeConfig ===
    repo_scope_config = None
    if any([include_globs, exclude_globs, not include_untracked, include_submodules,
            include_worktrees, follow_symlinks, external_interface_depth != "metadata"]):
        repo_scope_config = RepoScopeConfig(
            include_untracked=include_untracked,
            include_globs=include_globs,
            exclude_globs=exclude_globs,
            include_submodules=include_submodules,
            include_worktrees=include_worktrees,
            follow_symlinks=follow_symlinks,
            external_interface_depth=external_interface_depth,
        )

    # === Build IncrementalConfig (with resolved path) ===
    incremental_config = None
    if incremental:
        incremental_config = IncrementalConfig(
            enabled=True,
            state_dir=resolved_state_dir,
            repo_id=incremental_repo_id,
            impact_strategy=incremental_impact_strategy or "hybrid",
            git_base_ref=git_base_ref,
            git_head_ref=git_head_ref,
            git_changed_only=git_changed_only,
        )

    # === Resolve determinism tier with aliases ===
    resolved_tier = None
    if determinism_tier:
        resolved_tier = DETERMINISM_ALIASES.get(determinism_tier)

    # === Build overrides dict (only CLI-specified values) ===
    overrides: dict[str, object] = {}
    if run_context:
        overrides["run_id"] = run_context.run_id
    if runtime_profile:
        overrides["runtime_profile_name"] = runtime_profile
    if resolved_tier:
        overrides["determinism_override"] = resolved_tier
    if writer_strategy:
        overrides["writer_strategy"] = writer_strategy

    # Repo-scope + tree-sitter overrides (inputs.py expects these in overrides)
    overrides["include_globs"] = list(include_globs)
    overrides["exclude_globs"] = list(exclude_globs)
    overrides["include_untracked"] = include_untracked
    overrides["include_submodules"] = include_submodules
    overrides["include_worktrees"] = include_worktrees
    overrides["follow_symlinks"] = follow_symlinks
    overrides["external_interface_depth"] = external_interface_depth
    overrides["enable_tree_sitter"] = enable_tree_sitter

    # === Get config file contents from run_context for forwarding ===
    config_contents = dict(run_context.config_contents) if run_context else {}

    # CLI plan/config overrides should win over config file values
    if plan_allow_partial is not None:
        config_contents["plan_allow_partial"] = plan_allow_partial
    if plan_requested_task:
        config_contents["plan_requested_tasks"] = list(plan_requested_task)
    if plan_impacted_task:
        config_contents["plan_impacted_tasks"] = list(plan_impacted_task)
    if enable_metric_scheduling is not None:
        config_contents["enable_metric_scheduling"] = enable_metric_scheduling
    if enable_plan_diagnostics is not None:
        config_contents["enable_plan_diagnostics"] = enable_plan_diagnostics
    if enable_plan_task_submission_hook is not None:
        config_contents["enable_plan_task_submission_hook"] = enable_plan_task_submission_hook
    if enable_plan_task_grouping_hook is not None:
        config_contents["enable_plan_task_grouping_hook"] = enable_plan_task_grouping_hook
    if enforce_plan_task_submission is not None:
        config_contents["enforce_plan_task_submission"] = enforce_plan_task_submission

    # === Build request ===
    request = GraphProductBuildRequest(
        repo_root=resolved_repo_root,
        output_dir=output_dir,
        work_dir=work_dir,
        execution_mode=execution_mode,
        executor_config=executor_config,
        graph_adapter_config=graph_adapter_config,
        scip_index_config=scip_config,
        scip_identity_overrides=scip_identity,
        incremental_config=incremental_config,
        incremental_impact_strategy=incremental_impact_strategy,
        include_quality=include_quality,
        include_extract_errors=include_errors,
        include_manifest=include_manifest,
        include_run_bundle=include_run_bundle,
        config=config_contents,  # Forward config file contents to pipeline
        overrides=overrides or None,
    )

    # === Execute ===
    result = build_graph_product(request)

    logger.info(
        "Build complete. Output dir=%s bundle=%s",
        result.output_dir,
        result.run_bundle_dir,
    )
    logger.info("Pipeline outputs: %s", sorted(result.pipeline_outputs))

    return 0
```

### Helper: Parse Key-Value Pairs

```python
def _parse_kv_pairs(values: tuple[str, ...]) -> dict[str, str]:
    """Parse key=value pairs from CLI repeatable options."""
    parsed: dict[str, str] = {}
    for item in values:
        key, sep, value = item.partition("=")
        if not sep or not key:
            msg = f"Expected key=value, got {item!r}."
            raise ValueError(msg)
        parsed[key] = value
    return parsed
```

### Helper: Parse Key-Value Pairs with JSON Values

```python
import json

def _parse_kv_pairs_json(values: tuple[str, ...]) -> dict[str, object]:
    """Parse key=json pairs from CLI repeatable options."""
    parsed: dict[str, object] = {}
    for item in values:
        key, sep, value = item.partition("=")
        if not sep or not key:
            msg = f"Expected key=json, got {item!r}."
            raise ValueError(msg)
        parsed[key] = json.loads(value)
    return parsed
```

---

## Advanced ScipIndexSettings Fields (Config-Only)

The following `ScipIndexSettings` fields are **config-file only** and not exposed as CLI flags. They're used for specialized workflows and are documented here for completeness:

```toml
# codeanatomy.toml — advanced SCIP configuration
[scip]
# Generate environment JSON (used for reproducible scip-python runs)
generate_env_json = false

# Alternative scip CLI binary (for snapshot/test operations)
scip_cli_bin = "scip"

# Incremental sharding for large repos
use_incremental_shards = false
shards_dir = "build/scip/shards"
shards_manifest_path = "build/scip/shards_manifest.json"

# SCIP print output (for debugging symbol graph)
run_scip_print = false
scip_print_path = "build/scip/print_output.txt"

# SCIP snapshot testing
run_scip_snapshot = false
scip_snapshot_dir = "build/scip/snapshots"
scip_snapshot_comment_syntax = "#"

# SCIP validation testing
run_scip_test = false
scip_test_args = ["--check-documents"]
```

These map directly to `ScipIndexSettings` fields in `src/hamilton_pipeline/types/repo_config.py:10-34`.

---

## Config-Level Validation for Mutual Exclusion

Config files can set both `version` and `timestamp` for delta operations, bypassing CLI-only validators. Add config-level validation:

```python
from cyclopts import validators

def validate_config_mutual_exclusion(config: dict[str, object]) -> None:
    """Validate config-level mutual exclusion rules.

    Config files can bypass CLI validators like MutuallyExclusive().
    This function enforces the same rules at config load time.
    """
    # Delta restore: version XOR timestamp
    delta_config = config.get("delta", {}) or {}
    restore_config = delta_config.get("restore", {}) or {}
    if restore_config.get("version") and restore_config.get("timestamp"):
        msg = "Config error: delta.restore cannot specify both 'version' and 'timestamp'."
        raise ValueError(msg)

    # Delta export: version XOR timestamp (optional, both None is allowed)
    export_config = delta_config.get("export", {}) or {}
    if export_config.get("version") and export_config.get("timestamp"):
        msg = "Config error: delta.export cannot specify both 'version' and 'timestamp'."
        raise ValueError(msg)


# Integrate into meta-app
@app.meta.default
def meta_launcher(*tokens, config_file: str | None = None, ...) -> int:
    config_contents = load_toml_config(config_file) if config_file else {}

    # Validate config-level mutual exclusion rules
    validate_config_mutual_exclusion(config_contents)

    # Continue with dispatch...
```

---

## Delta Commands — Full Parity with Existing Scripts

### Group Validators for Mutual Exclusion

```python
from cyclopts import Group, validators

# === Restore: version XOR timestamp (mutually exclusive) ===
restore_target_group = Group(
    "Restore Target",
    help="Specify exactly one of version or timestamp.",
    validator=validators.MutuallyExclusive(),
    default_parameter=Parameter(show_default=False),
)
```

### Delta Vacuum Command (from `delta_maintenance.py`)

```python
def vacuum_command(
    path: Annotated[
        str,
        Parameter(
            name="--path",
            help="Delta table path.",
            required=True,
        ),
    ],
    storage_option: Annotated[
        tuple[str, ...],
        Parameter(
            name="--storage-option",
            help="Storage option key=value (repeatable).",
        ),
    ] = (),
    retention_hours: Annotated[
        int | None,
        Parameter(
            name="--retention-hours",
            help="Retention window in hours for vacuum.",
        ),
    ] = None,
    apply: Annotated[
        bool,
        Parameter(
            name="--apply",
            help="Apply vacuum deletions (not a dry-run).",
        ),
    ] = False,
    no_enforce_retention: Annotated[
        bool,
        Parameter(
            name="--no-enforce-retention",
            help="Disable minimum retention duration checks during vacuum.",
        ),
    ] = False,
    full: Annotated[
        bool,
        Parameter(
            name="--full",
            help="Run a full vacuum (including active files where supported).",
        ),
    ] = False,
    keep_versions: Annotated[
        str | None,
        Parameter(
            name="--keep-versions",
            help="Comma-separated Delta versions to retain during vacuum.",
        ),
    ] = None,
    commit_metadata: Annotated[
        tuple[str, ...],
        Parameter(
            name="--commit-metadata",
            help="Commit metadata key=value entries for vacuum (repeatable).",
        ),
    ] = (),
    report_path: Annotated[
        str | None,
        Parameter(
            name="--report-path",
            help="Optional path for JSON report (default: stdout).",
        ),
    ] = None,
    artifact_path: Annotated[
        str | None,
        Parameter(
            name="--artifact-path",
            help="Optional path for a run-bundle artifact payload.",
        ),
    ] = None,
) -> int:
    """Run Delta vacuum to remove expired files."""
    ...
```

### Delta Checkpoint Command

```python
def checkpoint_command(
    path: Annotated[str, Parameter(name="--path", required=True)],
    storage_option: Annotated[tuple[str, ...], Parameter(name="--storage-option")] = (),
    report_path: Annotated[str | None, Parameter(name="--report-path")] = None,
) -> int:
    """Create a Delta checkpoint."""
    ...
```

### Delta Cleanup-Log Command

```python
def cleanup_log_command(
    path: Annotated[str, Parameter(name="--path", required=True)],
    storage_option: Annotated[tuple[str, ...], Parameter(name="--storage-option")] = (),
    report_path: Annotated[str | None, Parameter(name="--report-path")] = None,
) -> int:
    """Cleanup expired Delta log files."""
    ...
```

### Delta Export Command (from `delta_export_snapshot.py`)

```python
def export_command(
    path: Annotated[str, Parameter(name="--path", required=True, help="Delta table path.")],
    target: Annotated[str, Parameter(name="--target", required=True, help="Target Delta table path.")],
    version: Annotated[
        int | None,
        Parameter(
            name="--version",
            help="Delta version to clone.",
            group=restore_target_group,
        ),
    ] = None,
    timestamp: Annotated[
        str | None,
        Parameter(
            name="--timestamp",
            help="Timestamp to clone (as accepted by Delta Lake).",
            group=restore_target_group,
        ),
    ] = None,
    storage_option: Annotated[tuple[str, ...], Parameter(name="--storage-option")] = (),
    mode: Annotated[
        Literal["overwrite", "append", "error"],
        Parameter(name="--mode", help="Delta write mode."),
    ] = "overwrite",
    schema_mode: Annotated[
        Literal["overwrite", "merge"] | None,
        Parameter(name="--schema-mode", help="Delta schema mode."),
    ] = "overwrite",
    report_path: Annotated[str | None, Parameter(name="--report-path")] = None,
) -> int:
    """Clone a Delta snapshot into Delta storage."""
    ...
```

### Delta Restore Command (from `delta_restore_table.py`)

```python
def restore_command(
    path: Annotated[str, Parameter(name="--path", required=True, help="Delta table path.")],
    version: Annotated[
        int | None,
        Parameter(
            name="--version",
            help="Delta table version to restore.",
            group=restore_target_group,
        ),
    ] = None,
    timestamp: Annotated[
        str | None,
        Parameter(
            name="--timestamp",
            help="Timestamp to restore (as accepted by Delta Lake).",
            group=restore_target_group,
        ),
    ] = None,
    storage_option: Annotated[tuple[str, ...], Parameter(name="--storage-option")] = (),
    allow_unsafe: Annotated[
        bool,
        Parameter(
            name="--allow-unsafe",
            help="Allow unsafe restore operations when supported.",
        ),
    ] = False,
    report_path: Annotated[str | None, Parameter(name="--report-path")] = None,
) -> int:
    """Restore a Delta table to a prior version or timestamp.

    Raises
    ------
    ValueError
        If neither or both of version/timestamp are specified.
    """
    ...
```

---

## Diag Command — Aligned with `e2e_diagnostics_report.py`

```python
def diag_command(
    output_dir: Annotated[
        str,
        Parameter(
            name="--output-dir",
            help="Output directory containing e2e artifacts.",
        ),
    ] = "build/e2e_full_pipeline",
    run_bundle: Annotated[
        str | None,
        Parameter(
            name="--run-bundle",
            help="Specific run bundle directory (default: latest under run_bundles).",
        ),
    ] = None,
    report_path: Annotated[
        str | None,
        Parameter(
            name="--report-path",
            help="Path to write JSON report (default: <output_dir>/diagnostics_report.json).",
        ),
    ] = None,
    summary_path: Annotated[
        str | None,
        Parameter(
            name="--summary-path",
            help="Path to write summary markdown (default: <output_dir>/diagnostics_report.md).",
        ),
    ] = None,
    validate_delta: Annotated[
        bool,
        Parameter(
            name="--validate-delta",
            help="Read Delta tables and validate arrays (slower, more thorough).",
        ),
    ] = False,
) -> int:
    """Generate a diagnostic report for pipeline outputs.

    Validates Delta table schemas, row counts, and optionally performs
    full array validation.
    """
    ...
```

---

## Plan Command — Execution Plan Inspection

```python
def plan_command(
    repo_root: Annotated[
        Path,
        Parameter(validator=validators.Path(exists=True, dir_okay=True)),
    ],
    show_graph: Annotated[
        bool,
        Parameter(help="Display task dependency graph."),
    ] = False,
    show_schedule: Annotated[
        bool,
        Parameter(help="Display computed execution schedule."),
    ] = False,
    validate: Annotated[
        bool,
        Parameter(help="Validate plan consistency and report issues."),
    ] = False,
    output_format: Annotated[
        Literal["text", "json", "dot"],
        Parameter(help="Output format for plan display."),
    ] = "text",
    output_file: Annotated[
        Path | None,
        Parameter(name="-o", help="Write plan to file instead of stdout."),
    ] = None,
) -> int:
    """Show the computed execution plan without running it.

    Useful for debugging task dependencies and understanding what
    the pipeline will execute.
    """
    ...
```

---

## Configuration Precedence

### Environment Variables Already Read in `inputs.py`

The following environment variables are read directly by pipeline inputs. **Do not invent new env var names**—map CLI params explicitly to these existing names:

| Environment Variable | Purpose | CLI Override | Parameter `env_var=` |
|---------------------|---------|--------------|----------------------|
| `CODEANATOMY_RUNTIME_PROFILE` | Runtime profile selection | `--runtime-profile` | `env_var="CODEANATOMY_RUNTIME_PROFILE"` |
| `CODEANATOMY_DETERMINISM_TIER` | Determinism tier | `--determinism-tier` | *(via overrides)* |
| `CODEANATOMY_FORCE_TIER2` | Force canonical tier | `--determinism-tier canonical` | *(via overrides)* |
| `CODEANATOMY_PIPELINE_MODE` | Incremental/streaming mode | `--incremental` | *(via overrides)* |
| `CODEANATOMY_STATE_DIR` | Incremental state directory | `--incremental-state-dir` | `env_var="CODEANATOMY_STATE_DIR"` |
| `CODEANATOMY_REPO_ID` | Repository identifier | `--incremental-repo-id` | `env_var="CODEANATOMY_REPO_ID"` |
| `CODEANATOMY_INCREMENTAL_IMPACT_STRATEGY` | Impact strategy | `--incremental-impact-strategy` | `env_var="CODEANATOMY_INCREMENTAL_IMPACT_STRATEGY"` |
| `CODEANATOMY_GIT_BASE_REF` | Git base ref | `--git-base-ref` | `env_var="CODEANATOMY_GIT_BASE_REF"` |
| `CODEANATOMY_GIT_HEAD_REF` | Git head ref | `--git-head-ref` | `env_var="CODEANATOMY_GIT_HEAD_REF"` |
| `CODEANATOMY_GIT_CHANGED_ONLY` | Changed-only mode | `--git-changed-only` | `env_var="CODEANATOMY_GIT_CHANGED_ONLY"` |
| `CODEANATOMY_HAMILTON_CACHE_PATH` | Hamilton cache path | Via config file | *(config only)* |
| `CODEANATOMY_CACHE_POLICY_PROFILE` | Cache policy | Via config file | *(config only)* |
| `CODEANATOMY_ENABLE_STREAMING_TABLES` | Streaming tables | Not exposed | *(internal)* |
| `CODEANATOMY_LOG_LEVEL` | Logging verbosity | `--log-level` | `env_var="CODEANATOMY_LOG_LEVEL"` |

### Precedence Rules

```
1. CLI flags (highest priority)
   ↓ GraphProductBuildRequest.overrides receives CLI-specified values
   ↓ Overrides take precedence over env vars read by inputs.py
2. Environment variables (via explicit env_var= mapping per parameter)
   ↓ Cyclopts fills CLI params from env vars when not specified on command line
   ↓ inputs.py also reads env vars directly for some values
3. Configuration file contents (codeanatomy.toml / pyproject.toml)
   ↓ Cyclopts config layer fills unspecified CLI defaults
   ↓ Full config normalized and forwarded to GraphProductBuildRequest.config
4. Python dataclass defaults (lowest priority)
```

### How It Works

```python
# 1. CLI flag specified: goes into overrides (highest precedence)
#    codeanatomy build . --runtime-profile ci
#    → overrides["runtime_profile_name"] = "ci"

# 2. Env var via explicit mapping (when CLI not specified)
#    export CODEANATOMY_STATE_DIR=/tmp/state
#    codeanatomy build .
#    → Cyclopts fills incremental_state_dir from env var

# 3. Config file (fills defaults, forwarded to request.config)
#    codeanatomy.toml: [cache] policy = "aggressive"
#    → request.config = {"cache": {"policy": "aggressive"}}

# Key: CLI values in `overrides` override what inputs.py reads from env
```

**Critical Warning**: Do NOT use `App.default_parameter=Parameter(env_var=True)`. This invents new env var names like `CODEANATOMY_INCREMENTAL_STATE_DIR` that don't exist in `inputs.py` and creates precedence confusion.

---

## Testing Strategy with `result_action` Variants

### Embedding Mode for Tests

```python
from cyclopts import App

# For testing: no sys.exit, return values directly
test_app = App(
    name="codeanatomy",
    result_action="return_value",
    exit_on_error=False,
    print_error=False,
)

def test_build_minimal(tmp_path: Path) -> None:
    """Test minimal build invocation."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "example.py").write_text("x = 1")

    # parse_args returns (command, bound_args, ignored)
    command, bound, ignored = test_app.parse_args(
        ["build", str(repo)],
        exit_on_error=False,
        print_error=False,
    )

    assert bound.arguments["repo_root"] == repo
    assert bound.arguments["execution_mode"] == ExecutionMode.PLAN_PARALLEL
    # parse=False keyword-only params appear in ignored dict
    assert "run_context" in ignored


def test_build_with_config_override(tmp_path: Path) -> None:
    """Test config file override via meta-app."""
    config_file = tmp_path / "test.toml"
    config_file.write_text("""
[build]
execution-mode = "deterministic_serial"
""")

    command, bound, _ = test_app.parse_args(
        ["--config", str(config_file), "build", str(tmp_path)],
    )

    assert bound.arguments["execution_mode"] == ExecutionMode.DETERMINISTIC_SERIAL


def test_parse_false_keyword_only_injection(tmp_path: Path) -> None:
    """Test keyword-only parse=False context injection pattern."""
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "example.py").write_text("x = 1")

    command, bound, ignored = test_app.parse_args(
        ["build", str(repo)],
        exit_on_error=False,
    )

    # Keyword-only parse=False params must be in ignored
    assert "run_context" in ignored

    # Simulate meta-app injection
    run_context = RunContext(run_id="test-123", log_level="INFO", config_contents={})

    # Call command with injected keyword-only context
    # (In production, meta_launcher handles this)
    result = command(*bound.args, **bound.kwargs, run_context=run_context)
    assert result == 0
```

### Dataclass Command Pattern

```python
from cyclopts import App, Parameter
from dataclasses import dataclass
from typing import Annotated

@dataclass
class BuildResult:
    """Build result that can be called for output."""
    output_dir: Path
    run_id: str

    def __call__(self) -> str:
        return f"Build complete: {self.output_dir} (run={self.run_id})"

# Use result_action pipeline for dataclass commands
app = App(
    result_action=["call_if_callable", "print_non_int_sys_exit"],
)

@app.command
def build_returning_result(...) -> BuildResult:
    """Build command returning callable result."""
    result = build_graph_product(request)
    return BuildResult(
        output_dir=result.output_dir,
        run_id=result.run_id or "unknown",
    )
```

### User Class Flattening with `Parameter(name="*")`

Use Cyclopts' namespace flattening to bind existing config types directly, reducing manual mapping and drift:

```python
from cyclopts import Parameter
from dataclasses import dataclass
from typing import Annotated

# === Define CLI wrapper with flattening ===
# @Parameter(name="*") removes the wrapper namespace from CLI flags
# --executor.kind becomes --executor-kind

@Parameter(name="*")
@dataclass(frozen=True)
class CliExecutorConfig:
    """Flattened CLI wrapper for ExecutorConfig fields."""
    kind: Literal["threadpool", "multiprocessing", "dask", "ray"] = "multiprocessing"
    max_tasks: int = 4
    remote_kind: Literal["threadpool", "multiprocessing", "dask", "ray"] | None = None
    remote_max_tasks: int | None = None
    cost_threshold: float | None = None


@Parameter(name="*")
@dataclass(frozen=True)
class CliRepoScopeConfig:
    """Flattened CLI wrapper for RepoScopeConfig fields."""
    include_untracked: bool = True
    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = ()
    include_submodules: bool = False
    include_worktrees: bool = False
    follow_symlinks: bool = False
    external_interface_depth: Literal["metadata", "full"] = "metadata"


# === Use in command signature ===
def build_command(
    repo_root: Path,
    # Flattened types appear as top-level flags
    executor: Annotated[CliExecutorConfig | None, Parameter(name="*")] = None,
    repo_scope: Annotated[CliRepoScopeConfig | None, Parameter(name="*")] = None,
    *,
    run_context: Annotated[RunContext | None, Parameter(parse=False)] = None,
) -> int:
    # Convert CLI wrappers to actual config types
    executor_config = None
    if executor:
        executor_config = ExecutorConfig(
            kind=executor.kind,
            max_tasks=executor.max_tasks,
            remote_kind=executor.remote_kind,
            remote_max_tasks=executor.remote_max_tasks,
            cost_threshold=executor.cost_threshold,
        )
    ...
```

**Benefits of flattening**:
- Reduces manual field-by-field mapping
- Keeps CLI flag names in sync with config type fields
- Type changes in config types surface as CLI changes
- Works with JSON dict parsing for complex inputs

**When NOT to flatten**:
- When you need different default values than the config type
- When you need additional validators per-field
- When the config type has fields that shouldn't be CLI-exposed

---

## Telemetry Integration with Keyword-Only `parse=False` Injection

### Run Context Dataclass

```python
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from opentelemetry import trace

from core_types import JsonValue

tracer = trace.get_tracer("codeanatomy.cli")


@dataclass(frozen=True)
class RunContext:
    """Injected run context for commands.

    Commands declare this as a KEYWORD-ONLY parameter with parse=False
    to receive it from the meta-app rather than CLI parsing.

    IMPORTANT: parse=False params MUST be keyword-only (after `*,`) for
    Cyclopts meta injection to work correctly.
    """
    run_id: str
    log_level: str
    config_contents: Mapping[str, JsonValue] = field(default_factory=dict)
    span: trace.Span | None = None

    def child_span(self, name: str) -> trace.Span:
        """Create a child span for command operations."""
        return tracer.start_span(name, parent=self.span)
```

### Command Using Keyword-Only Injected Context

```python
def build_command(
    repo_root: Path,
    # ... other params ...
    # CRITICAL: parse=False must be KEYWORD-ONLY (after *,) for meta injection
    *,
    run_context: Annotated[RunContext | None, Parameter(parse=False)] = None,
) -> int:
    """Build command with keyword-only injected run context."""
    # Use injected context for telemetry
    if run_context and run_context.span:
        run_context.span.set_attribute("codeanatomy.repo_root", str(repo_root))

    # Forward config file contents to pipeline
    config_contents = run_context.config_contents if run_context else {}

    # Execute build with config forwarding...
    request = GraphProductBuildRequest(
        repo_root=repo_root,
        config=config_contents,  # Pipeline-internal config keys
        ...
    )
    return 0
```

---

## Module Structure

```
src/cli/
├── __init__.py              # Package exports + main() entry
├── __main__.py              # Entry point: python -m codeanatomy
├── app.py                   # Main App definition, lazy loading, meta-app
├── context.py               # RunContext dataclass (with config_contents)
├── converters.py            # Type converters (determinism tier aliases)
├── groups.py                # Parameter groups (output, execution, scip, etc.)
├── config_loader.py         # Config loading + normalization helpers
├── validation.py            # Config-level mutual exclusion validation
├── path_utils.py            # Path resolution helpers (_resolve_path)
├── kv_parser.py             # Key-value pair parsing helpers
├── commands/
│   ├── __init__.py
│   ├── build.py             # build command (lazy loaded)
│   ├── plan.py              # plan command (lazy loaded)
│   ├── delta.py             # delta subcommands (lazy loaded)
│   ├── diag.py              # diag command (lazy loaded)
│   ├── config.py            # config subcommands
│   └── version.py           # version command (avoids --version collision)
├── telemetry.py             # Telemetry integration, invoke_with_telemetry
└── completion.py            # Shell completion utilities
```

---

## Shell Completion (Dynamic Install + Static Scripts)

Expose Cyclopts’ built-in install commands in addition to generating static scripts:

```python
# Enable `--install-completion` and `--show-completion` entrypoints
app.register_install_completion_command(
    name="--install-completion",
    add_to_startup=False,
    group=admin_group,
    help="Install shell completion scripts.",
)
```

Static script generation remains available for packaging; dynamic install is more
ergonomic for local users and matches Cyclopts’ native CLI behavior.

---

## Implementation Phases

### Phase 1: Core Infrastructure
- [ ] Create `src/cli/` module structure
- [ ] Define main `App` with lazy loading (no `env_var=True` in default_parameter)
- [ ] Implement meta-app with config file loading and forwarding to `request.config`
- [ ] Add config normalization helper (nested TOML → flat keys)
- [ ] Implement Session/Admin help panel groups
- [ ] Implement `version` command (avoids --version flag collision)
- [ ] Add keyword-only `parse=False` context injection pattern
- [ ] Write path resolution helpers (`_resolve_path` for relative paths vs repo_root)

### Phase 2: Build Command
- [ ] Implement `build` command with full parameter parity
- [ ] Add `--disable-scip` flag
- [ ] Add repo scope controls (include/exclude globs, submodules, symlinks, etc.)
- [ ] Add graph adapter options (`--graph-adapter-kind`, `--graph-adapter-option`)
- [ ] Add `--enable-tree-sitter` flag
- [ ] Explicit `env_var=` mapping to existing `CODEANATOMY_*` names only
- [ ] Path resolution for incremental_state_dir and SCIP paths
- [ ] Config contents forwarding to `GraphProductBuildRequest.config`
- [ ] Write unit tests for parsing and context injection

### Phase 3: Delta Commands
- [ ] Implement `delta vacuum` with all flags from `delta_maintenance.py`
- [ ] Implement `delta checkpoint` and `delta cleanup-log`
- [ ] Implement `delta export` from `delta_export_snapshot.py`
- [ ] Implement `delta restore` with CLI mutual exclusion validator
- [ ] Add config-level mutual exclusion validation
- [ ] Write delta command tests

### Phase 4: Supporting Commands
- [ ] Implement `plan` command
- [ ] Implement `diag` command from `e2e_diagnostics_report.py`
- [ ] Implement `config show/validate/init` subcommands
- [ ] Add help panel organization (Session, Output, Execution, SCIP, etc.)

### Phase 5: Configuration & Polish
- [ ] Complete configuration file support (TOML) with full content forwarding
- [ ] Document advanced config-only ScipIndexSettings fields
- [ ] Document environment variable precedence table
- [ ] Add telemetry wrapper
- [ ] Register dynamic shell completion install command
- [ ] Generate shell completion scripts
- [ ] Write integration tests
- [ ] Add golden test snapshots for help and error output

### Phase 6: Documentation & Release
- [ ] Write CLI user documentation
- [ ] Add examples to README
- [ ] Create packaging entry point
- [ ] Verify parity with existing scripts (run_full_pipeline.py, delta_*.py)
- [ ] Deprecation notices for old scripts

---

## Dependencies

### Required

```toml
[project.dependencies]
cyclopts = ">=4.4.1"
rich = ">=13.0.0"  # Already a cyclopts dependency
```

### Entry Point

```toml
# pyproject.toml
[project.scripts]
codeanatomy = "cli:main"
```

---

## Example Invocations

### Build Commands

```bash
# Minimal invocation
codeanatomy build .

# With output directory
codeanatomy build . --output-dir ./analysis

# Parallel Ray execution with remote tasks
codeanatomy build . \
    --executor-kind ray \
    --executor-max-tasks 8 \
    --executor-remote-kind ray \
    --executor-remote-max-tasks 16 \
    --executor-cost-threshold 10.0

# Canonical determinism (legacy alias)
codeanatomy build . --determinism-tier tier2 --execution-mode deterministic_serial

# Incremental build with git refs
codeanatomy build . \
    --incremental \
    --incremental-state-dir .codeanatomy/state \
    --git-base-ref main \
    --git-head-ref HEAD

# Custom SCIP configuration
codeanatomy build . \
    --scip-output-dir ./scip_output \
    --scip-timeout-s 600 \
    --node-max-old-space-mb 16384 \
    --scip-project-name "my-project"

# Disable SCIP (not recommended, produces less useful outputs)
codeanatomy build . --disable-scip

# With repository scope controls
codeanatomy build . \
    --include-glob "src/**/*.py" \
    --exclude-glob "**/*_test.py" \
    --include-submodules \
    --external-interface-depth full

# With graph adapter (non-dynamic execution)
codeanatomy build . \
    --graph-adapter-kind dask \
    --graph-adapter-option scheduler=distributed \
    --graph-adapter-option address=localhost:8786

# Using config file for advanced options
codeanatomy --config ci-config.toml build .
```

### Delta Commands

```bash
# Vacuum with dry run (default)
codeanatomy delta vacuum --path ./build/cpg/nodes

# Apply vacuum with retention
codeanatomy delta vacuum --path ./build/cpg/nodes \
    --retention-hours 168 \
    --apply \
    --report-path ./vacuum_report.json

# Create checkpoint
codeanatomy delta checkpoint --path ./build/cpg/nodes

# Export snapshot at specific version
codeanatomy delta export \
    --path ./build/cpg/nodes \
    --target ./export/nodes \
    --version 5 \
    --mode overwrite

# Restore to timestamp
codeanatomy delta restore \
    --path ./build/cpg/nodes \
    --timestamp "2024-01-15T00:00:00Z" \
    --allow-unsafe
```

### Diagnostics

```bash
# Generate diagnostic report
codeanatomy diag --output-dir ./build

# Validate Delta tables thoroughly
codeanatomy diag --output-dir ./build --validate-delta

# Specific run bundle
codeanatomy diag --run-bundle ./build/run_bundles/2024-01-15_abc123
```

---

## References

### Codebase Types
- [GraphProductBuildRequest](../src/graph/product_build.py:92)
- [ScipIndexSettings / ScipIndexConfig](../src/hamilton_pipeline/types/repo_config.py:10)
- [RepoScopeConfig](../src/hamilton_pipeline/types/repo_config.py:40)
- [ScipIdentityOverrides](../src/hamilton_pipeline/types/repo_config.py:77)
- [TreeSitterConfig](../src/hamilton_pipeline/types/repo_config.py:95)
- [ExecutorConfig](../src/hamilton_pipeline/types/execution.py:28)
- [GraphAdapterConfig](../src/hamilton_pipeline/types/execution.py:42)
- [IncrementalConfig](../src/semantics/incremental/config.py)

### Existing Scripts (Parity Targets)
- [run_full_pipeline.py](../scripts/run_full_pipeline.py)
- [delta_maintenance.py](../scripts/delta_maintenance.py)
- [delta_export_snapshot.py](../scripts/delta_export_snapshot.py)
- [delta_restore_table.py](../scripts/delta_restore_table.py)
- [e2e_diagnostics_report.py](../scripts/e2e_diagnostics_report.py)

### External Documentation
- [Cyclopts Documentation](https://cyclopts.readthedocs.io/)
- [Cyclopts User Classes (Flattening)](https://cyclopts.readthedocs.io/en/latest/user_classes.html)
- [Cyclopts Meta App](https://cyclopts.readthedocs.io/en/latest/meta_app.html)
- [Cyclopts Config Files](https://cyclopts.readthedocs.io/en/latest/config_file.html)

---

## Appendix A — Config Loader + Normalization (Driver Factory Mapping)

`driver_factory.py` reads **flat keys** from `config`. This appendix shows concrete mappings so
`normalize_config_contents(...)` can turn nested TOML into the exact keys the pipeline expects.

### A.1 Mapping Examples (Nested → Flat)

| Nested TOML | Flat Key (driver_factory.py) | Notes |
|------------|------------------------------|-------|
| `[plan] allow_partial = true` | `plan_allow_partial = true` | Controls partial plan compilation. |
| `[plan] requested_tasks = ["..."]` | `plan_requested_tasks = [...]` | Explicit task allowlist. |
| `[plan] impacted_tasks = ["..."]` | `plan_impacted_tasks = [...]` | Impacted tasks for pruning. |
| `[plan] enable_metric_scheduling = false` | `enable_metric_scheduling = false` | Overrides default scheduling. |
| `[plan] enable_plan_diagnostics = false` | `enable_plan_diagnostics = false` | Plan diagnostics emission. |
| `[plan] enable_plan_task_submission_hook = false` | `enable_plan_task_submission_hook = false` | Submission hook toggle. |
| `[plan] enable_plan_task_grouping_hook = false` | `enable_plan_task_grouping_hook = false` | Grouping hook toggle. |
| `[plan] enforce_plan_task_submission = false` | `enforce_plan_task_submission = false` | Enforce submission policy. |
| `[cache] policy_profile = "aggressive"` | `cache_policy_profile = "aggressive"` | Cache policy profile. |
| `[cache] path = "/tmp/cache"` | `cache_path = "/tmp/cache"` | Cache path override. |
| `[cache] log_to_file = true` | `cache_log_to_file = true` | Cache logging toggle. |
| `[graph_adapter] kind = "ray"` | `graph_adapter_kind = "ray"` | Non-dynamic execution adapter. |
| `[graph_adapter] options = { ... }` | `graph_adapter_options = { ... }` | Adapter options dict. |
| `[incremental] enabled = true` | `incremental_enabled = true` | Enables incremental mode in driver factory. |
| `[incremental] state_dir = "build/state"` | `incremental_state_dir = "build/state"` | Driver uses repo_root to resolve. |

**Additional flat keys consumed by `driver_factory.py` (non-exhaustive):**
`runtime_profile_name`, `determinism_override`, `cache_opt_in`, `hamilton_cache_path`,
`enable_hamilton_tracker`, `hamilton_tags`, `hamilton_capture_data_statistics`,
`hamilton_max_list_length_capture`, `hamilton_max_dict_length_capture`,
`enable_hamilton_type_checker`, `enable_hamilton_node_diagnostics`,
`enable_otel_node_tracing`, `enable_structured_run_logs`, `enable_otel_plan_tracing`.

### A.2 Stub Module Spec: `src/cli/config_loader.py`

Use a small helper module to centralize config loading + normalization. This is intentionally
minimal and keeps logic out of command handlers.

```python
from __future__ import annotations

from pathlib import Path
import tomllib
from typing import Any


def load_effective_config(config_file: str | None) -> dict[str, object]:
    """Load config contents from codeanatomy.toml / pyproject.toml or explicit --config."""
    if config_file:
        path = Path(config_file)
        if path.exists():
            with path.open("rb") as f:
                return tomllib.load(f)
        return {}

    cfg: dict[str, object] = {}
    codeanatomy_path = _find_in_parents("codeanatomy.toml")
    if codeanatomy_path:
        with codeanatomy_path.open("rb") as f:
            cfg = tomllib.load(f)

    pyproject_path = _find_in_parents("pyproject.toml")
    if pyproject_path:
        with pyproject_path.open("rb") as f:
            pyproject = tomllib.load(f)
        cfg = pyproject.get("tool", {}).get("codeanatomy", cfg)

    return cfg


def normalize_config_contents(config: dict[str, object]) -> dict[str, object]:
    """Normalize nested TOML sections into flat keys used by driver_factory."""
    flat: dict[str, object] = {}
    flat.update(config)

    plan = config.get("plan")
    if isinstance(plan, dict):
        _copy_key(flat, plan, "allow_partial", "plan_allow_partial")
        _copy_key(flat, plan, "requested_tasks", "plan_requested_tasks")
        _copy_key(flat, plan, "impacted_tasks", "plan_impacted_tasks")
        _copy_key(flat, plan, "enable_metric_scheduling", "enable_metric_scheduling")
        _copy_key(flat, plan, "enable_plan_diagnostics", "enable_plan_diagnostics")
        _copy_key(flat, plan, "enable_plan_task_submission_hook", "enable_plan_task_submission_hook")
        _copy_key(flat, plan, "enable_plan_task_grouping_hook", "enable_plan_task_grouping_hook")
        _copy_key(flat, plan, "enforce_plan_task_submission", "enforce_plan_task_submission")

    cache = config.get("cache")
    if isinstance(cache, dict):
        _copy_key(flat, cache, "policy_profile", "cache_policy_profile")
        _copy_key(flat, cache, "path", "cache_path")
        _copy_key(flat, cache, "log_to_file", "cache_log_to_file")

    graph_adapter = config.get("graph_adapter")
    if isinstance(graph_adapter, dict):
        _copy_key(flat, graph_adapter, "kind", "graph_adapter_kind")
        _copy_key(flat, graph_adapter, "options", "graph_adapter_options")

    incremental = config.get("incremental")
    if isinstance(incremental, dict):
        _copy_key(flat, incremental, "enabled", "incremental_enabled")
        _copy_key(flat, incremental, "state_dir", "incremental_state_dir")
        _copy_key(flat, incremental, "repo_id", "incremental_repo_id")
        _copy_key(flat, incremental, "impact_strategy", "incremental_impact_strategy")

    return flat


def _copy_key(
    target: dict[str, object],
    source: dict[str, object],
    source_key: str,
    dest_key: str,
) -> None:
    value = source.get(source_key)
    if value is not None:
        target[dest_key] = value


def _find_in_parents(filename: str) -> Path | None:
    """Walk parents from cwd to find a filename, return first hit."""
    path = Path.cwd()
    while True:
        candidate = path / filename
        if candidate.exists():
            return candidate
        if path.parent == path:
            return None
        path = path.parent
```
