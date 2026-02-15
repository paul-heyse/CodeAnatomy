# CQ Cyclopts CLI Tools-CQ-Only Implementation Plan v1 (2026-02-15)

## 2. Scope Summary
This plan implements the approved Cyclopts improvement scope for CQ items 1 through 12 only, with a strict boundary: all changes are confined to `tools/cq`, CQ tests, and CQ documentation. No integration, shared runtime coupling, or refactor dependency on `src/` is permitted. The design stance is incremental hardening with direct cutover in CQ (no compatibility shim with `src/cli`).

## 3. Design Principles
1. CQ/`src` independence is non-negotiable. CQ CLI logic remains fully self-contained under `tools/cq`.
2. Prefer documented public Cyclopts APIs (`App.__call__`, `run_async`, `parse_args`, `parse_known_args`, `result_action`) over private module imports.
3. Parse-time validation is preferred to runtime validation wherever Cyclopts types/validators can express the constraint.
4. Help, completion, and docs are contractual outputs and must be covered by CQ-specific tests/goldens.
5. Execution policy must be explicit and deterministic: parse stage, dispatch stage, result-action stage, and exit-code normalization are each intentional.
6. Changes should preserve existing CQ command semantics unless explicitly called out as a CLI contract change.

## 4. Current Baseline
- CQ app uses a meta launcher with forwarding tokens and global options in `tools/cq/cli_app/app.py`.
- CQ telemetry imports Cyclopts private internals in `tools/cq/cli_app/telemetry.py` (`cyclopts._result_action`, `cyclopts._run`).
- CQ admin commands are present and parseable in `tools/cq/cli_app/commands/admin.py`, but they currently emit directly to stdout and bypass the unified `CliResult` rendering path.
- CQ uses both Cyclopts config providers and separate typed config/env loaders in `tools/cq/cli_app/config.py`, then merges manually in `tools/cq/cli_app/app.py`.
- CQ help/docs currently show duplicated env var labels and iterable negative flags (`--empty-*`), including run-input flags (`--empty-step`, `--empty-steps`), in `docs/reference/cq_cli.md`.
- CQ already has CQ-specific generation scripts for docs/completion (`scripts/generate_cq_cli_docs.py`, `scripts/generate_cq_completion.py`) that should be hardened instead of replaced.
- CQ chain execution now compiles to `RunPlan` in `tools/cq/run/chain.py` and executes through `tools/cq/run/runner.py` via `tools/cq/cli_app/commands/chain.py`.
- Shared group constants are mostly centralized, but `tools/cq/cli_app/commands/neighborhood.py` still constructs an inline `Group("Analysis", sort_key=1)`.
- CQ has broad `Parameter(parse=False)` use for context injection in command signatures, and uses `app.meta` as orchestration surface.

## 5. Per-Scope-Item Plan

## S1. Replace Private Cyclopts Invocation Internals with Public Dispatch Surface
### Goal
Remove dependency on Cyclopts private modules in CQ invocation telemetry while preserving parse/exec timing and context injection behavior.

### Representative Code Snippets
```python
# tools/cq/cli_app/dispatch.py
from __future__ import annotations

import asyncio
import inspect
from collections.abc import Callable
from inspect import BoundArguments
from typing import Any


def dispatch_bound_command(command: Callable[..., Any], bound: BoundArguments) -> Any:
    """Execute a parsed command using public APIs and explicit async policy."""
    result = command(*bound.args, **bound.kwargs)
    if not inspect.isawaitable(result):
        return result
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(result)
    msg = "Awaitable command dispatched from sync invocation path; use main_async/run_async."
    raise RuntimeError(msg)
```

```python
# tools/cq/cli_app/telemetry.py
from tools.cq.cli_app.dispatch import dispatch_bound_command

command, bound, ignored = app.parse_args(
    normalized,
    exit_on_error=False,
    print_error=True,
)
if "ctx" in ignored:
    bound.arguments["ctx"] = ctx
result = dispatch_bound_command(command, bound)
```

### Files to Edit
- `tools/cq/cli_app/telemetry.py`
- `tests/unit/cq/test_cli_result_handling.py`

### New Files to Create
- `tools/cq/cli_app/dispatch.py`
- `tests/unit/cq/test_cli_dispatch.py`

### Legacy Decommission/Delete Scope
- Delete private Cyclopts imports from `tools/cq/cli_app/telemetry.py`:
- `from cyclopts._result_action import handle_result_action`
- `from cyclopts._run import _run_maybe_async_command`
- Delete private-internal execution helpers superseded by explicit public dispatch and result-action policy.

---

## S2. Adopt Explicit CQ Result-Action Pipelines
### Goal
Standardize CQ return handling through explicit Cyclopts `result_action` pipeline contracts for launcher invocation, telemetry-wrapped execution, and REPL behavior.

### Representative Code Snippets
```python
# tools/cq/cli_app/result_action.py
from __future__ import annotations

from typing import Any

from tools.cq.cli_app.context import CliResult, FilterConfig
from tools.cq.cli_app.result import handle_result

# CQ canonical app-level policy: normalize CQ result object, then normalize exit code.
CQ_DEFAULT_RESULT_ACTION = (
    lambda result: handle_result(result, result.filters or FilterConfig())
    if isinstance(result, CliResult)
    else result,
    "return_int_as_exit_code_else_zero",
)


def cq_result_action(result: Any) -> int:
    if isinstance(result, CliResult):
        return handle_result(result, result.filters or FilterConfig())
    if isinstance(result, int):
        return result
    return 0
```

```python
# tools/cq/cli_app/app.py
app = App(
    ...,
    result_action=CQ_DEFAULT_RESULT_ACTION,
)
```

### Files to Edit
- `tools/cq/cli_app/result_action.py`
- `tools/cq/cli_app/app.py`
- `tools/cq/cli_app/telemetry.py`
- `tools/cq/cli_app/commands/repl.py`
- `tests/unit/cq/test_cli_result_handling.py`
- `tests/unit/cq/cli/test_admin_output_contracts.py`

### New Files to Create
- None.

### Legacy Decommission/Delete Scope
- Delete manual `int(...) if isinstance(processed, int)` coercion fallback in `tools/cq/cli_app/telemetry.py` once pipeline policy is authoritative.
- Delete duplicated result-normalization branches split across launcher, telemetry, and command-local stdout emitters once `CQ_DEFAULT_RESULT_ACTION` is adopted everywhere.

---

## S3. Add Async-Safe CQ Entry Point (`run_async` Path)
### Goal
Provide a CQ async entrypoint for embedding in existing event loops without creating a new loop, using Cyclopts `run_async`.

### Representative Code Snippets
```python
# tools/cq/cli_app/__init__.py
from __future__ import annotations


def main() -> int:
    from tools.cq.cli_app.app import app
    return int(app.meta())


async def main_async(tokens: list[str] | None = None) -> int:
    from tools.cq.cli_app.app import app
    out = await app.meta.run_async(
        tokens=tokens,
        exit_on_error=False,
        print_error=True,
        result_action="return_int_as_exit_code_else_zero",
    )
    return int(out) if isinstance(out, int) else 0
```

```python
# tools/cq/cli.py
from tools.cq.cli_app import main

if __name__ == "__main__":
    raise SystemExit(main())
```

### Files to Edit
- `tools/cq/cli_app/__init__.py`
- `tools/cq/cli.py`
- `tests/unit/cq/test_cli_context.py`

### New Files to Create
- `tests/unit/cq/test_cli_async_entrypoint.py`

### Legacy Decommission/Delete Scope
- Delete any CQ-local ad hoc async loop bootstrapping introduced for command dispatch if `main_async` becomes the canonical async entrypoint.

---

## S4. REPL Dispatcher with Context Injection + In-Shell Help Command
### Goal
Make CQ REPL compatible with `ctx`-injected commands and provide natural `help` UX inside interactive shell sessions.

### Representative Code Snippets
```python
# tools/cq/cli_app/commands/repl.py
from __future__ import annotations

from typing import Annotated, Any

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext
from tools.cq.cli_app.decorators import require_context, require_ctx


def _dispatch_with_ctx(
    ctx: CliContext,
    command: Any,
    bound: Any,
    ignored: dict[str, Any],
) -> Any:
    if "ctx" in ignored:
        bound.arguments["ctx"] = ctx
    return command(*bound.args, **bound.kwargs)


@require_ctx
def repl(
    *,
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> int:
    from tools.cq.cli_app.app import app

    resolved_ctx = require_context(ctx)
    app.interactive_shell(
        prompt="cq> ",
        exit_on_error=False,
        result_action="print_non_int_return_int_as_exit_code",
        dispatcher=lambda command, bound, ignored: _dispatch_with_ctx(
            resolved_ctx, command, bound, ignored
        ),
    )
    return 0
```

```python
# tools/cq/cli_app/commands/repl.py
def repl_help(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
) -> int:
    from tools.cq.cli_app.app import app

    app.help_print(tokens=list(tokens))
    return 0
```

### Files to Edit
- `tools/cq/cli_app/commands/repl.py`
- `tools/cq/cli_app/app.py`
- `tests/unit/cq/test_cli_parsing.py`
- `tests/unit/cq/test_cli_result_handling.py`

### New Files to Create
- `tests/unit/cq/test_cli_repl_dispatch.py`

### Legacy Decommission/Delete Scope
- Delete REPL invocation path that calls `interactive_shell` without a dispatcher, because it cannot inject `ctx` into `parse=False` command parameters.

---

## S5. Normalize Admin Command Contracts Through CQ Result Pipeline
### Goal
Keep existing admin command registrations but migrate command outputs to unified `CliResult`/`result_action` handling, removing direct stdout writes from admin command bodies.

### Representative Code Snippets
```python
# tools/cq/cli_app/commands/admin.py
from __future__ import annotations

import json
from typing import Annotated

from cyclopts import Parameter

from tools.cq.cli_app.context import CliContext, CliResult, CliTextResult
from tools.cq.cli_app.decorators import require_context, require_ctx
from tools.cq.cli_app.types import OutputFormat, SchemaKind


def _emit_payload(ctx: CliContext, payload: dict[str, object], *, text_fallback: str) -> CliResult:
    text = json.dumps(payload, indent=2) if ctx.output_format == OutputFormat.json else text_fallback
    return CliResult(
        result=CliTextResult(
            text=text,
            media_type="application/json" if ctx.output_format == OutputFormat.json else "text/plain",
        ),
        context=ctx,
        filters=None,
    )


@require_ctx
def index(*, ctx: Annotated[CliContext | None, Parameter(parse=False)] = None) -> CliResult:
    resolved = require_context(ctx)
    return _emit_payload(
        resolved,
        {"deprecated": True, "message": "Index management has been removed. Caching is no longer used."},
        text_fallback="Index management has been removed. Caching is no longer used.",
    )
```

```python
# tests/unit/cq/cli/test_admin_output_contracts.py
from pathlib import Path

from tools.cq.cli_app.commands import admin
from tools.cq.cli_app.context import CliContext, CliResult, CliTextResult
from tools.cq.cli_app.types import OutputFormat


def test_admin_index_returns_cli_result(tmp_path: Path) -> None:
    ctx = CliContext.build(argv=["cq", "index"], root=tmp_path, output_format=OutputFormat.json)
    result = admin.index(ctx=ctx)
    assert isinstance(result, CliResult)
    assert isinstance(result.result, CliTextResult)
```

### Files to Edit
- `tools/cq/cli_app/commands/admin.py`
- `tests/unit/cq/cli/test_admin_output_contracts.py`
- `tests/unit/cq/test_cli_result_handling.py`

### New Files to Create
- None.

### Legacy Decommission/Delete Scope
- Delete direct stdout JSON/text emission branches inside admin command implementations.
- Delete admin-only output normalization that duplicates shared CQ result pipeline behavior.

---

## S6. Consolidate CQ Config Semantics Around `App.config` Chain
### Goal
Reduce duplicated config parsing/merge logic while preserving current CQ precedence semantics (`CLI > env > config-file > defaults`) and existing `--config`/`--no-config` behavior.

### Representative Code Snippets
```python
# tools/cq/cli_app/config.py
from cyclopts.config import Env, Toml


def build_config_chain(
    config_file: str | None = None,
    *,
    use_config: bool = True,
) -> list[object]:
    providers: list[object] = [Env(prefix="CQ_", command=False)]
    if not use_config:
        return providers
    if config_file:
        providers.append(Toml(config_file, must_exist=True))
    else:
        providers.append(Toml("pyproject.toml", root_keys=("tool", "cq"), must_exist=False))
    return providers
```

```python
# tools/cq/cli_app/app.py
def _build_launch_context(
    argv: list[str],
    config_opts: ConfigOptionArgs,
    global_opts: GlobalOptionArgs,
) -> LaunchContext:
    app.config = build_config_chain(
        config_file=config_opts.config,
        use_config=config_opts.use_config,
    )
    return LaunchContext(
        argv=argv,
        root=global_opts.root,
        verbose=global_opts.verbose,
        output_format=global_opts.output_format,
        artifact_dir=global_opts.artifact_dir,
        save_artifact=global_opts.save_artifact,
    )
```

### Files to Edit
- `tools/cq/cli_app/config.py`
- `tools/cq/cli_app/app.py`
- `tools/cq/cli_app/config_types.py`
- `tests/unit/cq/test_cli_config.py`
- `tests/unit/cq/test_cli_meta_app.py`

### New Files to Create
- None.

### Legacy Decommission/Delete Scope
- Phase decommission after parity tests land:
- Delete typed config duplication in `tools/cq/cli_app/config.py` only when precedence parity is proven:
- `load_typed_config`
- `load_typed_env_config`
- `_coerce_config_data`
- `_convert_config_data`
- Delete manual config merge helpers in `tools/cq/cli_app/app.py` only when equivalent behavior is covered by tests:
- `_apply_config_overrides`
- `_resolve_global_options`

---

## S7. Upgrade Help Presentation with App/Group `help_formatter` Controls
### Goal
Use Cyclopts help formatter capabilities for clearer CQ panels, accessibility fallback, deterministic panel layout, and elimination of duplicated group-description rendering in CQ help output.

### Representative Code Snippets
```python
# tools/cq/cli_app/groups.py
from cyclopts import Group

global_group = Group(
    "Global Options",
    help="Options applied to every CQ command.",
    sort_key=0,
    help_formatter="default",
)
setup_group = Group(
    "Setup",
    help="Shell and developer setup commands.",
    sort_key=4,
    help_formatter="plain",
)
```

```python
# tools/cq/cli_app/app.py
app = App(
    ...,
    help_format="rich",
    help_formatter="default",
    group_parameters=global_group,
)
```

### Files to Edit
- `tools/cq/cli_app/groups.py`
- `tools/cq/cli_app/app.py`
- `tests/cli_golden/test_cq_help_output.py`
- `tests/cli_golden/fixtures/cq_help_root.txt`
- `docs/reference/cq_cli.md`

### New Files to Create
- None.

### Legacy Decommission/Delete Scope
- Delete CQ help panel formatting assumptions that require default formatter behavior only.
- Delete duplicated group-description text in CQ help goldens once formatter/group wiring is finalized.

---

## S8. Reduce Help Noise from Iterable Negative Flags and Env Duplication
### Goal
Simplify CQ help surface by removing low-value iterable negative flags (`--empty-*`) for filters and run-input arrays, and eliminating duplicated env var rendering.

### Representative Code Snippets
```python
# tools/cq/cli_app/params.py
from cyclopts import Group, Parameter, validators

filter_group = Group(
    "Filters",
    default_parameter=Parameter(
        show_choices=True,
        negative_iterable=(),  # disable --empty-<filter>
        show_env_var=False,
    ),
)

run_input = Group(
    "Run Input",
    validator=validators.LimitedChoice(min=1, max=3),
    default_parameter=Parameter(
        negative_iterable=(),  # disable --empty-step / --empty-steps
        show_env_var=False,
    ),
)
```

```python
# tools/cq/cli_app/app.py
app = App(
    ...,
    default_parameter=Parameter(show_default=True, show_env_var=False),
)

class GlobalOptionArgs:
    root: Annotated[
        Path | None,
        Parameter(name="--root", env_var="CQ_ROOT", show_env_var=True, group=global_group),
    ] = None
```

### Files to Edit
- `tools/cq/cli_app/params.py`
- `tools/cq/cli_app/app.py`
- `docs/reference/cq_cli.md`
- `tests/cli_golden/fixtures/cq_help_root.txt`
- `tests/cli_golden/fixtures/run_help.txt`
- `tests/cli_golden/test_cq_help_output.py`
- `tests/cli_golden/test_run_golden.py`
- `tests/unit/cq/test_cli_parsing.py`

### New Files to Create
- None.

### Legacy Decommission/Delete Scope
- Delete `--empty-include`, `--empty-exclude`, `--empty-impact`, `--empty-confidence`, `--empty-severity` from CQ filter help surface.
- Delete `--empty-step` and `--empty-steps` from CQ run-input help surface.
- Delete duplicated env annotations like `CQ_ROOT, CQ_ROOT` from generated CQ docs/goldens.

---

## S9. Move Verbosity to Counted Flag Semantics
### Goal
Adopt Cyclopts counted flag behavior (`-v`, `-vv`, `-vvv`) for CQ verbosity while preserving env var override.

### Representative Code Snippets
```python
# tools/cq/cli_app/app.py
class GlobalOptionArgs:
    verbose: Annotated[
        int,
        Parameter(
            name=["--verbose", "-v"],
            count=True,
            env_var="CQ_VERBOSE",
            group=global_group,
            help="Verbosity level; repeat flag for higher verbosity",
        ),
    ] = 0
```

```python
# tests/unit/cq/test_cli_meta_app.py
_cmd, bound, _extra = app.meta.parse_args(["calls", "foo", "-vv"])
assert bound.kwargs["global_opts"].verbose == 2
```

### Files to Edit
- `tools/cq/cli_app/app.py`
- `tests/unit/cq/test_cli_meta_app.py`
- `tests/unit/cq/test_cli_parsing.py`
- `docs/reference/cq_cli.md`
- `tests/cli_golden/fixtures/cq_help_root.txt`

### New Files to Create
- None.

### Legacy Decommission/Delete Scope
- Delete tests/docs requiring numeric argument style for `-v` (`-v 2`) as primary UX.

---

## S10. Add CQ App-Level Validation for Cross-Option Invariants
### Goal
Introduce app-level validator hooks for constraints that can be bypassed by config-injected defaults or cross-option interactions.

### Representative Code Snippets
```python
# tools/cq/cli_app/validators.py
from __future__ import annotations

from tools.cq.cli_app.app import ConfigOptionArgs, GlobalOptionArgs


def validate_launcher_invariants(**kwargs: object) -> None:
    config_opts = kwargs.get("config_opts")
    global_opts = kwargs.get("global_opts")

    if isinstance(config_opts, ConfigOptionArgs):
        if not config_opts.use_config and config_opts.config is not None:
            raise ValueError("--config cannot be combined with --no-config")

    if isinstance(global_opts, GlobalOptionArgs):
        if global_opts.verbose < 0:
            raise ValueError("--verbose cannot be negative")
```

```python
# tools/cq/cli_app/app.py
@app.meta.default(validator=validate_launcher_invariants)
def launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    global_opts: GlobalOptionArgs | None = None,
    config_opts: ConfigOptionArgs | None = None,
) -> int:
    ...
```

### Files to Edit
- `tools/cq/cli_app/validators.py`
- `tools/cq/cli_app/app.py`
- `tests/unit/cq/test_cli_config.py`
- `tests/unit/cq/test_cli_meta_app.py`

### New Files to Create
- `tests/unit/cq/test_cli_invariants.py`

### Legacy Decommission/Delete Scope
- Delete late-stage runtime checks in launcher/command handlers that duplicate invariant checks already enforced by app-level validators.

---

## S11. Enforce Shared Group Objects Across CQ Command Graph
### Goal
Eliminate the remaining inline `Group(...)` construction in CQ leaf modules (currently neighborhood) and enforce central group constants for stable help panel behavior.

### Representative Code Snippets
```python
# tools/cq/cli_app/commands/neighborhood.py
from tools.cq.cli_app.groups import analysis_group

neighborhood_app = App(
    name="neighborhood",
    help="Analyze semantic neighborhood of a target",
    group=analysis_group,
)
```

```python
# tests/unit/cq/test_cli_lazy_loading.py
from tools.cq.cli_app.groups import analysis_group
from tools.cq.cli_app.commands.neighborhood import neighborhood_app


def test_neighborhood_group_identity() -> None:
    assert neighborhood_app.group is analysis_group
```

### Files to Edit
- `tools/cq/cli_app/commands/neighborhood.py`
- `tests/unit/cq/test_cli_lazy_loading.py`

### New Files to Create
- None.

### Legacy Decommission/Delete Scope
- Delete inline group instance `Group("Analysis", sort_key=1)` in `tools/cq/cli_app/commands/neighborhood.py`.

---

## S12. Automate CQ CLI Asset Generation (Docs + Completion) with Freshness Checks
### Goal
Harden existing CQ-only generation workflow for CLI docs and completion scripts with deterministic output controls and freshness checks, without introducing redundant replacement scripts.

### Representative Code Snippets
```python
# scripts/generate_cq_cli_docs.py
from __future__ import annotations

from pathlib import Path

from tools.cq.cli_app.app import app


def generate_cq_reference(output_path: Path) -> str:
    docs = app.generate_docs(output_format="markdown", recursive=True, include_hidden=False)
    output_path.write_text(docs, encoding="utf-8")
    return docs


def main() -> None:
    generate_cq_reference(Path("docs/reference/cq_cli.md"))
```

```python
# tests/unit/cq/test_cli_asset_generation.py
from pathlib import Path

from scripts.generate_cq_cli_docs import generate_cq_reference


def test_generate_cq_reference(tmp_path: Path) -> None:
    out = tmp_path / "cq_cli.md"
    text = generate_cq_reference(out)
    assert "Code Query - High-signal code analysis macros" in text
```

### Files to Edit
- `scripts/generate_cq_cli_docs.py`
- `scripts/generate_cq_completion.py`
- `scripts/generate_cq_completion.sh`
- `tools/cq/cli_app/completion.py`
- `docs/reference/cq_cli.md`
- `tests/unit/cq/test_cli_completion.py`
- `tests/cli_golden/fixtures/cq_help_root.txt`
- `tests/cli_golden/fixtures/run_help.txt`
- `tests/cli_golden/test_cq_help_output.py`

### New Files to Create
- `tests/unit/cq/test_cli_asset_generation.py`

### Legacy Decommission/Delete Scope
- Delete manually maintained CQ CLI reference drift workflow (hand-edited help docs).
- Delete ad hoc completion/doc generation instructions that are not backed by `scripts/generate_cq_cli_docs.py` and `scripts/generate_cq_completion.py`.

---

## 6. Cross-Scope Legacy Decommission and Deletion Plan
### Batch D1 (after S1, S2, S3)
- Delete private Cyclopts execution dependencies in `tools/cq/cli_app/telemetry.py`.
- Delete CQ-specific fallback code that emulates result-action behavior now covered by explicit pipeline policy.

### Batch D2 (after S5, S12)
- Delete direct admin-command stdout emission paths that bypass unified CQ output handling.
- Delete tests that assert raw stdout side effects from admin command internals instead of `CliResult` contracts.

### Batch D3 (after S6, S10)
- Delete duplicate typed config/env coercion path in `tools/cq/cli_app/config.py` after precedence parity tests pass.
- Delete manual config precedence merge helpers from `tools/cq/cli_app/app.py` after equivalent behavior is covered by CQ config tests.

### Batch D4 (after S7, S8, S9, S11, S12)
- Delete old CQ help/doc snapshots containing duplicate env var labels, duplicated group-description text, and iterable `--empty-*` flags.
- Delete inline group construction patterns superseded by centralized group constants.

## 7. Implementation Sequence
1. Implement S1 first to remove private Cyclopts dependency and establish a stable CQ dispatch seam.
2. Implement S2 to standardize result handling contracts across launcher/telemetry/REPL policy.
3. Implement S5 to align admin command outputs with the unified CQ result pipeline.
4. Implement S4 to make REPL context injection and in-shell help fully functional.
5. Implement S3 to add async-safe entrypoint once dispatch/result policies are stable.
6. Implement S6 to consolidate config precedence while preserving current behavior.
7. Implement S10 to enforce post-config cross-option invariants at the app/meta layer.
8. Implement S11 to remove remaining inline group construction.
9. Implement S7 to improve panel rendering and deterministic help structure.
10. Implement S8 to reduce help noise and clean env/negative-flag presentation.
11. Implement S9 to finalize verbosity UX and update parse/help contracts.
12. Implement S12 last to regenerate CQ docs/completions and lock new output contracts in tests.

## 8. Implementation Checklist
- [ ] S1. Replace private Cyclopts invocation internals with public dispatch surface.
- [ ] S2. Adopt explicit CQ result-action pipelines.
- [ ] S3. Add async-safe CQ entrypoint (`run_async` path).
- [ ] S4. Add REPL dispatcher with context injection and in-shell help command.
- [ ] S5. Normalize admin command contracts through the CQ result pipeline.
- [ ] S6. Consolidate CQ config semantics around `App.config` chain.
- [ ] S7. Upgrade help presentation with App/Group `help_formatter` controls.
- [ ] S8. Reduce help noise from iterable negative flags and env duplication.
- [ ] S9. Move verbosity to counted flag semantics.
- [ ] S10. Add CQ app-level validation for cross-option invariants.
- [ ] S11. Enforce shared group objects across CQ command graph.
- [ ] S12. Automate CQ CLI asset generation (docs + completion) with freshness checks.
- [ ] D1. Decommission private Cyclopts internals and redundant result fallback code.
- [ ] D2. Decommission admin command direct-stdout output paths/tests.
- [ ] D3. Decommission duplicate typed config merge/coercion path.
- [ ] D4. Decommission outdated help/docs/group snapshots and inline group patterns.
