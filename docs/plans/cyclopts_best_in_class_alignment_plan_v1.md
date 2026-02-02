# Cyclopts Best-in-Class Alignment Plan v1

Status: Proposed  
Owner: Codex  
Scope: Cyclopts CLI alignment with public API + config unification

## Objectives
- Make Cyclopts the single, canonical entry point for public API invocation.
- Eliminate config drift between Cyclopts config providers and pipeline config payload.
- Ensure CLI parameter mapping matches `GraphProductBuildRequest` fields directly.
- Strengthen validation and UX via Cyclopts group validators and error handling.

---

## Scope Item 1 — Single Source of Truth for Config Resolution

### Why
The CLI currently reads configuration twice: Cyclopts config providers (`app.config`) and `load_effective_config()` in `build_command`. This can cause drift and user confusion.

### Design (best-in-class choice)
Replace ad-hoc config merging with a single config pipeline that produces:
1) Cyclopts parameter defaults
2) The `GraphProductBuildRequest.config` payload

Implement a shared config provider that uses `load_effective_config()` and returns normalized values to Cyclopts (and optionally `ConfigWithSources` for `config show`).

### Representative snippet
```python
# src/cli/config_provider.py
from cyclopts.config import Config

class CodeAnatomyConfig(Config):
    def __call__(self, tokens, *, app, command, ctx):
        config = load_effective_config(ctx.config_file)
        return normalize_config_contents(config)
```

### Target files
- `src/cli/app.py`
- `src/cli/config_loader.py`
- `src/cli/commands/build.py`
- `src/cli/commands/config.py`
- (New) `src/cli/config_provider.py`

### Deprecations / deletions
- Remove redundant config reads in `build_command` (use the shared config provider output).

### Implementation checklist
- Build a custom Cyclopts config provider using `load_effective_config()`.
- Wire it into `app.config`.
- Remove duplicated config-loading logic in `build_command`.
- Ensure `config show` reads from the same provider output.

---

## Scope Item 2 — Route Execution Through Cyclopts Result Pipeline

### Why
`invoke_with_telemetry()` calls the command directly, bypassing `result_action`. This skips standardized rendering and can diverge from Cyclopts expected behavior.

### Design (best-in-class choice)
Invoke `app(...)` within the telemetry wrapper so `result_action` is always applied.

### Representative snippet
```python
# src/cli/telemetry.py
result = app(tokens, exit_on_error=False, print_error=True)
exit_code = cli_result_action(app, command, result)
```

### Target files
- `src/cli/telemetry.py`
- `src/cli/result_action.py`

### Deprecations / deletions
- Remove direct `command(*bound.args, **bound.kwargs)` invocation.

### Implementation checklist
- Route execution via `app(...)`.
- Preserve telemetry timing and error classification.
- Ensure `CliResult` still renders correctly.

---

## Scope Item 3 — Map CLI Directly to Public API Fields

### Why
`GraphProductBuildRequest` exposes explicit fields such as `runtime_profile_name` and `determinism_override`. Today these are routed through `overrides`, which hides intent and increases coupling.

### Design (best-in-class choice)
Populate request fields directly and reserve `overrides` for truly dynamic/driver-level options.

### Representative snippet
```python
request = GraphProductBuildRequest(
    repo_root=resolved_repo_root,
    runtime_profile_name=options.runtime_profile,
    determinism_override=resolved_tier,
    writer_strategy=options.writer_strategy,
    overrides=overrides or None,
)
```

### Target files
- `src/cli/commands/build.py`
- `src/graph/product_build.py`

### Deprecations / deletions
- Remove forwarding of these keys through `overrides`.

### Implementation checklist
- Map CLI values directly to `GraphProductBuildRequest` fields.
- Keep `overrides` for non-API driver config only.

---

## Scope Item 4 — User-Class Flattening for Request/Config Types

### Why
Cyclopts supports flattening dataclasses (`Parameter(name="*")`) which reduces duplication and keeps CLI aligned with API dataclasses.

### Design (best-in-class choice)
Flatten `GraphProductBuildRequest` (or a smaller CLI-specific request dataclass) and nested configs like `ExecutorConfig` and `GraphAdapterConfig`. Avoid manually re-listing equivalent flags.

### Representative snippet
```python
@dataclass(frozen=True)
class BuildRequestCli:
    request: Annotated[GraphProductBuildRequest, Parameter(name="*")]

@app.command
def build(request: BuildRequestCli, ...):
    return build_graph_product(request.request)
```

### Target files
- `src/cli/commands/build.py`
- `src/graph/product_build.py`

### Deprecations / deletions
- Reduce or remove duplicated CLI-only structs (e.g., `BuildOptions`).

### Implementation checklist
- Identify fields safe to flatten (avoid mixing CLI-only vs request-only).
- Update help/labels using `Parameter` metadata where needed.
- Ensure defaults match `GraphProductBuildRequest` defaults.

---

## Scope Item 5 — Strengthen Validation With Group Validators

### Why
Some options have implied dependencies. Cyclopts group validators improve UX and prevent invalid runs.

### Design
Add `ConditionalRequired` and `MutuallyExclusive` validators for incremental and SCIP modes.

### Representative snippet
```python
incremental_group = Group(
    "Incremental Processing",
    validator=ConditionalRequired(
        condition_param="incremental",
        condition_value=True,
        required_params=("incremental_state_dir",),
    ),
)
```

### Target files
- `src/cli/groups.py`
- `src/cli/validators.py`
- `src/cli/commands/build.py`

### Deprecations / deletions
- None.

### Implementation checklist
- Add conditional validators for incremental + SCIP usage.
- Ensure warnings for ignored options remain.

---

## Scope Item 6 — Explicit `--config pyproject.toml` Support

### Why
Passing `--config pyproject.toml` currently ignores `tool.codeanatomy` root keys.

### Design
Detect pyproject configs and apply root keys automatically when `--config` is used.

### Representative snippet
```python
if session.config_file.endswith("pyproject.toml"):
    app.config = [Toml(session.config_file, root_keys=("tool", "codeanatomy"), must_exist=True)]
else:
    app.config = [Toml(session.config_file, must_exist=True)]
```

### Target files
- `src/cli/app.py`
- `src/cli/config_loader.py`

### Deprecations / deletions
- None.

### Implementation checklist
- Add explicit pyproject handling for `--config`.
- Validate against existing config loader behavior.

---

## Cross-Cutting Acceptance Gates
- CLI and API configuration are sourced from a single, consistent pipeline.
- `build` command uses `GraphProductBuildRequest` fields directly.
- Cyclopts result_action always runs, even when telemetry wrapper is used.
- `--config pyproject.toml` respects `tool.codeanatomy` root keys.

