# CQ Cyclopts CLI Best-in-Class Comprehensive Implementation Plan v1 (2026-02-15)

## Objective
Implement the full CQ Cyclopts upgrade scope end-to-end (all 20 items from the prior review) so `tools/cq/cli_app` reaches a production-grade CLI architecture with:
- predictable parsing contracts,
- cleaner help and completion UX,
- explicit execution/error/result policy,
- tighter alignment with existing best practices already present in `src/cli/app.py`,
- and first-class integration with existing tree-sitter-backed CQ capabilities.

## Non-Goals
- No behavioral changes to query semantics themselves unless required for parse/validation correctness.
- No extractor/runtime redesign for tree-sitter internals (existing runtime primitives are reused).
- No CQ-skill tooling usage in implementation; discovery remains `rg` + `ast-grep`.

## Success Criteria
- CQ help output is stable, clear, and free of double-negative flags.
- CQ command validation errors are parse-time where possible (Cyclopts-native).
- CQ execution path uses explicit result-action policy (no ad hoc finalize path).
- CQ command graph supports aliases/completion/docs-generation patterns consistently.
- CQ tests/goldens cover new CLI contracts and telemetry/error policy.

## Scope Inventory
1. Double-negative flag removal.
2. Parse-time enum/literal validation.
3. Console/error_console policy wiring.
4. Result-action based execution pipeline.
5. Alias modernization (`nb` via alias).
6. Completion install command for CQ.
7. Help UX and panel quality upgrades.
8. Explicit group assignment for all CQ command surfaces.
9. Explicit global env-var naming.
10. Default-parameter layering and policy centralization.
11. Numeric/range validators.
12. Iterable UX (`consume_multiple`) standardization.
13. Tolerant parsing where forwarding/chaining needs it.
14. Lazy-loading sub-app graph completion.
15. Context injection consolidation.
16. Telemetry-first invocation wrapper.
17. CI/release completion script generation.
18. Cyclopts docs generation pipeline.
19. Optional interactive shell mode.
20. Golden/contract test expansion.

---

## SI-01: Normalize Boolean Flag Semantics (Remove Double Negatives)
### Goal
Replace negative field names (`no_config`, `no_save_artifact`) with positive booleans and explicit negative aliases so help output does not expose `--no-no-*` patterns.

### Representative snippet
```python
# tools/cq/cli_app/app.py
from typing import Annotated
from cyclopts import Parameter

class GlobalOptionArgs:
    save_artifact: Annotated[
        bool,
        Parameter(name="--save-artifact", negative="--no-save-artifact", help="Save artifacts"),
    ] = True

class ConfigOptionArgs:
    use_config: Annotated[
        bool,
        Parameter(name="--config", negative="--no-config", help="Enable config loading"),
    ] = True

# Existing tree-sitter parse path remains unchanged.
tree, changed_ranges = parse_python_tree_with_ranges(source, cache_key=rel_path)
# parse_python_tree_with_ranges uses Parser.parse + Tree.changed_ranges under the hood.
```

### Tree-sitter library functions leveraged
- `Parser.parse(...)`
- `Tree.changed_ranges(...)`
- `Tree.edit(...)`

### Files to edit
- `tools/cq/cli_app/app.py`
- `tools/cq/cli_app/config.py`
- `tools/cq/cli_app/config_types.py`
- `tests/unit/cq/test_cli_meta_app.py`
- `tests/unit/cq/test_cli_config.py`
- `tests/cli_golden/fixtures/search_help.txt`
- `tests/cli_golden/fixtures/run_help.txt`

### New files
- None required.

### Legacy/deletion targets
- Remove legacy negative field names and inversion logic paths:
- `GlobalOptionArgs.no_save_artifact`
- `ConfigOptionArgs.no_config`
- `_coerce_config_data(... no_save_artifact ...)` compatibility branch after migration window.

---

## SI-02: Move Runtime Choice Checks to Cyclopts Parse-Time Types
### Goal
Use `Literal`/`Enum`/typed aliases in command signatures instead of runtime `if value not in set(...)` checks.

### Representative snippet
```python
# tools/cq/cli_app/types.py
from enum import StrEnum

class ReportPreset(StrEnum):
    refactor_impact = "refactor-impact"
    safety_reliability = "safety-reliability"
    change_propagation = "change-propagation"
    dependency_health = "dependency-health"

# tools/cq/cli_app/commands/report.py
def report(
    preset: ReportPreset,
    *,
    opts: Annotated[ReportParams, Parameter(name="*")],
    ctx: Annotated[CliContext | None, Parameter(parse=False)] = None,
) -> CliResult:
    # no manual preset set-membership branch needed
    ...

# Parse-time validated command still drives tree-sitter diagnostics downstream.
diags = collect_tree_sitter_diagnostics(
    language="python",
    root=tree.root_node,
    windows=(QueryWindowV1(start_byte=0, end_byte=len(source_bytes)),),
)
```

### Tree-sitter library functions leveraged
- `QueryCursor.matches(...)` (via `run_bounded_query_matches`)
- `QueryCursor.set_byte_range(...)`
- `Query(...)`

### Files to edit
- `tools/cq/cli_app/types.py`
- `tools/cq/cli_app/commands/report.py`
- `tools/cq/cli_app/commands/admin.py`
- `tools/cq/cli_app/commands/neighborhood.py`
- `tools/cq/cli_app/commands/ldmd.py`
- `tests/unit/cq/test_cli_parsing.py`
- `tests/unit/cq/test_cli_enum_tokens.py`

### New files
- `tools/cq/cli_app/choices.py` (optional shared literal/enum aliases if we want type segregation).

### Legacy/deletion targets
- Manual runtime validation branches:
- `report.py` preset `valid_presets` block.
- `admin.py` schema-kind `if/elif` fallback error branch for invalid string.
- `neighborhood.py` lang fallback branch (`resolved_lang = ...`).

---

## SI-03: Wire `console` / `error_console` Policy Explicitly
### Goal
Bind CQ `App` output channels explicitly and align parse/exec invocations with a deterministic console policy.

### Representative snippet
```python
# tools/cq/cli_app/app.py
from rich.console import Console

console = Console(width=100, highlight=False)
error_console = Console(stderr=True, width=100, highlight=False)

app = App(
    name="cq",
    console=console,
    error_console=error_console,
    help_on_error=True,
    ...,
)

# Errors discovered while running tree-sitter diagnostics flow to error_console.
matches, telemetry = run_bounded_query_matches(query, tree.root_node, settings=settings)
if telemetry.cancelled:
    app.error_console.print("[yellow]tree-sitter query budget cancelled[/yellow]")
```

### Tree-sitter library functions leveraged
- `QueryCursor.matches(...)`
- `QueryCursor.did_exceed_match_limit`

### Files to edit
- `tools/cq/cli_app/app.py`
- `tests/cli_golden/test_cli_error_output.py`
- `tests/cli_golden/fixtures/error_unknown_command.txt`

### New files
- None required.

### Legacy/deletion targets
- Unused local console variable path in CQ launcher.
- Any ad hoc stdout/stderr divergence bypassing Cyclopts error channel for parse/runtime errors.

---

## SI-04: Adopt Result-Action Execution Pipeline
### Goal
Replace ad hoc `_execute_command` + `_finalize_result` flow with explicit Cyclopts `result_action` policy.

### Representative snippet
```python
# tools/cq/cli_app/result_action.py
from tools.cq.cli_app.context import CliResult, FilterConfig
from tools.cq.cli_app.result import handle_result


def cq_result_action(result: object) -> int:
    if isinstance(result, CliResult):
        return handle_result(result, result.filters or FilterConfig())
    if isinstance(result, int):
        return result
    return 0

# tools/cq/cli_app/app.py
app = App(..., result_action=cq_result_action)

@app.meta.default
def launcher(*tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)], ...):
    command, bound, ignored = app.parse_args(tokens, exit_on_error=True, print_error=True)
    return command(*bound.args, **bound.kwargs, **({"ctx": ctx} if "ctx" in ignored else {}))
```

### Tree-sitter library functions leveraged
- `Parser.parse(...)` (through commands executed under result action)
- `QueryCursor.captures(...)`
- `QueryCursor.matches(...)`

### Files to edit
- `tools/cq/cli_app/app.py`
- `tools/cq/cli_app/result.py`
- `tools/cq/cli_app/__init__.py`
- `tests/unit/cq/test_cli_result_handling.py`

### New files
- `tools/cq/cli_app/result_action.py`

### Legacy/deletion targets
- Delete launcher helpers once result-action path is stable:
- `_execute_command`
- `_finalize_result`
- duplicated return coercion logic in launcher.

---

## SI-05: Replace Dual-App `nb` Alias with Cyclopts `alias=`
### Goal
Use Cyclopts alias registration instead of maintaining duplicate neighborhood app objects and duplicate decorators.

### Representative snippet
```python
# tools/cq/cli_app/commands/neighborhood.py
neighborhood_app = App(name="neighborhood", help="Analyze semantic neighborhood of a target")

@neighborhood_app.default

def neighborhood(...):
    ...

# tools/cq/cli_app/app.py
app.command(neighborhood_app, alias="nb", group=analysis_group)

# neighborhood path continues to call parser.parse for source anchoring.
tree = parser.parse(source_bytes)  # tree_sitter.Parser.parse
```

### Tree-sitter library functions leveraged
- `Parser.parse(...)`
- `Node.parent`, `Node.children` traversal in collector

### Files to edit
- `tools/cq/cli_app/commands/neighborhood.py`
- `tools/cq/cli_app/app.py`
- `tests/unit/cq/test_cli_parsing.py`
- `tests/unit/cq/test_cli_lazy_loading.py`

### New files
- None required.

### Legacy/deletion targets
- `nb_app` duplicate sub-app object.
- Dual `@nb_app.default` decorator path.

---

## SI-06: Add `--install-completion` Command to CQ App
### Goal
Expose Cyclopts completion installation directly in CQ CLI.

### Representative snippet
```python
# tools/cq/cli_app/app.py
completion_group = Group("Setup", sort_key=50)

app.register_install_completion_command(
    name="--install-completion",
    add_to_startup=False,
    group=completion_group,
    help="Install CQ shell completion scripts.",
)

# Completion safety probe command keeps tree-sitter runtime check visible.
status = {
    "python": is_tree_sitter_python_available(),
    "rust": is_tree_sitter_rust_available(),
}
```

### Tree-sitter library functions leveraged
- `tree_sitter_python.language()` / `Language(...)` availability probe path
- `tree_sitter_rust.language()` / `Language(...)` availability probe path

### Files to edit
- `tools/cq/cli_app/app.py`
- `tests/unit/cq/test_cli_parsing.py`
- `tests/cli_golden/fixtures/search_help.txt`

### New files
- `tools/cq/cli_app/completion.py` (optional helper wrappers for generation/install).

### Legacy/deletion targets
- None immediate.

---

## SI-07: Help UX and Panel Quality Improvements
### Goal
Add richer group help text, optional epilogue, cleaner usage/ordering, and a stable help contract.

### Representative snippet
```python
# tools/cq/cli_app/app.py
analysis_group = Group("Analysis", help="Static + structural analysis commands.", sort_key=1)
admin_group = Group("Administration", help="Maintenance and schema export commands.", sort_key=2)

app = App(
    ...,
    help_epilogue="Examples:\n  cq search build_graph --lang python\n  cq run --steps '[{"type":"q",...}]'",
)

# Help examples reference tree-sitter-backed neighborhood and diagnostics behavior.
# Internally these rely on QueryCursor.set_point_range + QueryCursor.matches.
```

### Tree-sitter library functions leveraged
- `QueryCursor.set_point_range(...)`
- `QueryCursor.matches(...)`

### Files to edit
- `tools/cq/cli_app/app.py`
- `tools/cq/cli_app/params.py`
- `tests/cli_golden/test_search_golden.py`
- `tests/cli_golden/fixtures/search_help.txt`
- `tests/cli_golden/fixtures/run_help.txt`

### New files
- `tools/cq/cli_app/help_text.py` (optional central example/epilogue text).

### Legacy/deletion targets
- Sparse/inconsistent group help with no panel context.

---

## SI-08: Explicit Group Assignment for Every Command Surface
### Goal
Ensure all sub-app and leaf commands land in explicit help panels and maintain ordering consistency.

### Representative snippet
```python
# tools/cq/cli_app/app.py
protocol_group = Group("Protocols", help="Protocol/data access commands.", sort_key=3)

app.command("tools.cq.cli_app.commands.search:search", group=analysis_group)
app.command("tools.cq.cli_app.commands.query:q", group=analysis_group)
app.command("tools.cq.cli_app.commands.admin:schema", group=admin_group)
app.command("tools.cq.cli_app.commands.ldmd:ldmd_app", group=protocol_group)
app.command("tools.cq.cli_app.commands.artifact:artifact_app", group=protocol_group)
```

### Tree-sitter library functions leveraged
- `Query.pattern_settings(...)` (protocol commands include tree-sitter metadata payloads)

### Files to edit
- `tools/cq/cli_app/app.py`
- `tests/cli_golden/fixtures/search_help.txt`

### New files
- None required.

### Legacy/deletion targets
- Implicit/ungrouped root command entries for `ldmd`, `artifact`, and neighborhood aliases.

---

## SI-09: Explicit Env-Var Naming for Global Option Contract
### Goal
Pin global option env vars to CQ-wide names instead of command-scoped derived variants.

### Representative snippet
```python
# tools/cq/cli_app/app.py
root: Annotated[
    Path | None,
    Parameter(name="--root", env_var="CQ_ROOT", group=global_group),
] = None

output_format: Annotated[
    OutputFormat,
    Parameter(name="--format", env_var="CQ_FORMAT", group=global_group),
] = OutputFormat.md

# Tree-sitter runtime toggles can then be wired with stable env defaults.
settings = QueryExecutionSettingsV1(match_limit=int(os.getenv("CQ_TS_MATCH_LIMIT", "4096")))
```

### Tree-sitter library functions leveraged
- `QueryCursor(..., match_limit=...)`
- `QueryCursor.matches(...)`

### Files to edit
- `tools/cq/cli_app/app.py`
- `tools/cq/cli_app/params.py`
- `tests/unit/cq/test_cli_meta_app.py`
- `tests/cli_golden/fixtures/search_help.txt`

### New files
- None required.

### Legacy/deletion targets
- Command-scoped global env var contract for root/global flags (e.g., `CQ_SEARCH_ROOT` for shared global fields).

---

## SI-10: Centralize Parameter Policy with `default_parameter` / Group Defaults
### Goal
Reduce repeated `Parameter(...)` metadata by setting app/group defaults and overriding only exceptions.

### Representative snippet
```python
# tools/cq/cli_app/app.py
app = App(
    ...,
    default_parameter=Parameter(show_default=True, show_env_var=True),
)

filter_group = Group(
    "Filters",
    default_parameter=Parameter(show_choices=True),
)

# Tree-sitter-related diagnostics flags inherit defaults but can opt out.
max_nodes: Annotated[int, Parameter(name="--max-nodes", show_default=True)] = 1024
```

### Tree-sitter library functions leveraged
- `QueryCursor.captures(...)` (filter/choice defaults apply to tree-sitter-backed filtering flags)

### Files to edit
- `tools/cq/cli_app/app.py`
- `tools/cq/cli_app/params.py`

### New files
- `tools/cq/cli_app/groups.py` (optional if group objects are split from app bootstrap).

### Legacy/deletion targets
- Repeated per-parameter formatting metadata where app/group defaults should own policy.

---

## SI-11: Add Numeric Validators for Limits and Depths
### Goal
Convert runtime implicit assumptions into parse-time numeric validation.

### Representative snippet
```python
# tools/cq/cli_app/params.py
from cyclopts import validators

top_k: Annotated[
    int,
    Parameter(name="--top-k", validator=validators.Number(gte=1, lte=10_000)),
] = 10

match_limit: Annotated[
    int,
    Parameter(name="--match-limit", validator=validators.Number(gte=1, lte=1_000_000)),
] = 4096

# Applied directly to tree-sitter query cursor settings.
settings = QueryExecutionSettingsV1(match_limit=match_limit)
```

### Tree-sitter library functions leveraged
- `QueryCursor(..., match_limit=...)`
- `QueryCursor.set_max_start_depth(...)`

### Files to edit
- `tools/cq/cli_app/params.py`
- `tools/cq/cli_app/commands/neighborhood.py`
- `tests/unit/cq/test_cli_parsing.py`
- `tests/unit/cq/test_run_validators.py`

### New files
- None required.

### Legacy/deletion targets
- Runtime fallback guards that sanitize invalid numeric args after parse.

---

## SI-12: Standardize Iterable UX with `consume_multiple`
### Goal
Support `--include a b c` alongside repeated flags and preserve existing comma-based compatibility during migration.

### Representative snippet
```python
# tools/cq/cli_app/params.py
include: Annotated[
    list[str],
    Parameter(
        name="--include",
        consume_multiple=True,
        converter=comma_separated_list(str),
        help="Include globs (repeatable or multi-token)",
    ),
] = field(default_factory=list)

# Include windows flow into tree-sitter bounded queries for targeted scans.
matches, telemetry = run_bounded_query_matches(
    query,
    tree.root_node,
    windows=windows_from_includes(include),
)
```

### Tree-sitter library functions leveraged
- `QueryCursor.set_byte_range(...)`
- `QueryCursor.matches(...)`

### Files to edit
- `tools/cq/cli_app/params.py`
- `tools/cq/cli_app/types.py`
- `tests/unit/cq/test_cli_filter_options.py`

### New files
- None required.

### Legacy/deletion targets
- Strict repeated-flag-only assumptions for include/exclude parsing.

---

## SI-13: Use `parse_known_args` in Forwarding/Chaining Flows
### Goal
Preserve strict parsing for direct command execution while using tolerant parsing where forwarding and token segmentation are intended.

### Representative snippet
```python
# tools/cq/run/chain.py
command, bound, unused_tokens, ignored = app.parse_known_args(
    group,
    exit_on_error=False,
    print_error=False,
)
if unused_tokens:
    raise RuntimeError(f"Unused chain tokens: {unused_tokens}")

# Forwarded commands still execute tree-sitter query paths as normal.
rows, telemetry = run_bounded_query_matches(query, tree.root_node, settings=settings)
```

### Tree-sitter library functions leveraged
- `QueryCursor.matches(...)`
- `QueryCursor.captures(...)`

### Files to edit
- `tools/cq/run/chain.py`
- `tools/cq/cli_app/app.py`
- `tools/cq/cli_app/commands/chain.py`
- `tests/unit/cq/test_cli_meta_app.py`

### New files
- None required.

### Legacy/deletion targets
- Strict `parse_args` usage in chain paths that were intended to be tolerant by design.

---

## SI-14: Complete Lazy Loading for Sub-App Graph
### Goal
Eliminate eager imports for sub-app modules in `app.py` and register sub-apps via lazy import paths/factories.

### Representative snippet
```python
# tools/cq/cli_app/app.py
app.command("tools.cq.cli_app.commands.neighborhood:get_app", group=analysis_group)
app.command("tools.cq.cli_app.commands.ldmd:get_app", group=protocol_group)
app.command("tools.cq.cli_app.commands.artifact:get_app", group=protocol_group)

# commands/neighborhood.py

def get_app() -> App:
    return neighborhood_app

# First invocation triggers lazy import, then parser.parse executes on demand.
```

### Tree-sitter library functions leveraged
- `Parser.parse(...)` (deferred to command execution stage)

### Files to edit
- `tools/cq/cli_app/app.py`
- `tools/cq/cli_app/commands/neighborhood.py`
- `tools/cq/cli_app/commands/ldmd.py`
- `tools/cq/cli_app/commands/artifact.py`
- `tests/unit/cq/test_cli_lazy_loading.py`

### New files
- `tools/cq/cli_app/commands/_app_factories.py` (optional central factory module).

### Legacy/deletion targets
- Eager inline imports in `tools/cq/cli_app/app.py` for sub-app wiring.

---

## SI-15: Consolidate Context Injection and Remove Repeated Guards
### Goal
Replace repetitive `if ctx is None: raise RuntimeError` checks with a shared injection/decorator utility.

### Representative snippet
```python
# tools/cq/cli_app/decorators.py
from functools import wraps


def require_ctx(fn):
    @wraps(fn)
    def _wrapped(*args, **kwargs):
        ctx = kwargs.get("ctx")
        if ctx is None:
            raise RuntimeError("Context not injected")
        return fn(*args, **kwargs)
    return _wrapped

# tools/cq/cli_app/commands/search.py
@require_ctx
def search(..., ctx: Annotated[CliContext | None, Parameter(parse=False)] = None) -> CliResult:
    tree = parse_python_tree(source, cache_key=str(ctx.root))
    ...
```

### Tree-sitter library functions leveraged
- `Parser.parse(...)`
- `Tree.changed_ranges(...)`

### Files to edit
- `tools/cq/cli_app/commands/analysis.py`
- `tools/cq/cli_app/commands/search.py`
- `tools/cq/cli_app/commands/query.py`
- `tools/cq/cli_app/commands/run.py`
- `tools/cq/cli_app/commands/chain.py`
- `tools/cq/cli_app/commands/report.py`
- `tools/cq/cli_app/commands/admin.py`
- `tools/cq/cli_app/commands/ldmd.py`
- `tools/cq/cli_app/commands/artifact.py`
- `tools/cq/cli_app/commands/neighborhood.py`

### New files
- `tools/cq/cli_app/decorators.py`

### Legacy/deletion targets
- Repeated context-guard blocks in every command module.

---

## SI-16: Introduce Telemetry-First CQ Invocation Wrapper
### Goal
Add parse/execute/exit-code/error-stage telemetry around CQ invocation, mirroring main CLI pattern.

### Representative snippet
```python
# tools/cq/cli_app/telemetry.py
import time


def invoke_with_telemetry(app: App, tokens: list[str], *, ctx: CliContext) -> int:
    t0 = time.perf_counter()
    command, bound, ignored = app.parse_args(tokens, exit_on_error=False, print_error=True)
    parse_ms = (time.perf_counter() - t0) * 1000.0

    t1 = time.perf_counter()
    result = command(*bound.args, **bound.kwargs, **({"ctx": ctx} if "ctx" in ignored else {}))
    exec_ms = (time.perf_counter() - t1) * 1000.0

    # include tree-sitter runtime telemetry fields when present
    # e.g. did_exceed_match_limit from QueryCursor execution paths
    return cq_result_action(result)
```

### Tree-sitter library functions leveraged
- `QueryCursor.did_exceed_match_limit`
- `QueryCursor.matches(...)`
- `QueryCursor.captures(...)`

### Files to edit
- `tools/cq/cli_app/app.py`
- `tools/cq/cli_app/result.py`
- `tools/cq/search/smart_search.py` (ensure runtime telemetry is consistently surfaced)
- `tests/unit/cq/test_cli_result_handling.py`

### New files
- `tools/cq/cli_app/telemetry.py`

### Legacy/deletion targets
- Non-instrumented direct command invocation path in launcher.

---

## SI-17: Add Completion Generation to CI/Release Tooling
### Goal
Generate deterministic completion scripts (`bash`, `zsh`, `fish`) during release/CI and optionally publish as artifacts.

### Representative snippet
```python
# tools/cq/cli_app/completion.py
from pathlib import Path


def generate_completion_scripts(app: App, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for shell in ("bash", "zsh", "fish"):
        script = app.generate_completion(shell=shell)
        (output_dir / f"cq.{shell}").write_text(script, encoding="utf-8")

# Keep completion docs aligned with tree-sitter capability command examples.
```

### Tree-sitter library functions leveraged
- `Language(...)` availability checks exposed via completion docs/examples.

### Files to edit
- `tools/cq/cli_app/app.py`
- `scripts/` build/release scripts referencing CQ packaging (new helper invocation)
- `docs/` release docs for completion usage

### New files
- `scripts/generate_cq_completion.sh`
- `tools/cq/cli_app/completion.py`

### Legacy/deletion targets
- Manual ad hoc completion instructions without generated artifacts.

---

## SI-18: Add Cyclopts-Driven CLI Docs Generation
### Goal
Generate CQ CLI docs from the app graph to keep docs synchronized with live command signatures.

### Representative snippet
```python
# docs tooling script
from tools.cq.cli_app.app import app


def render_cq_cli_docs() -> str:
    return app.generate_docs(format="md")

# Generated docs should include tree-sitter command examples:
# cq neighborhood symbol_name --lang python
# cq search "pattern" --with-neighborhood
```

### Tree-sitter library functions leveraged
- `Parser.parse(...)` and `QueryCursor.matches(...)` represented in generated examples and option docs.

### Files to edit
- `docs/` CQ CLI documentation pages
- `pyproject.toml` (if docs tooling entrypoints/plugins are added)

### New files
- `scripts/generate_cq_cli_docs.py`
- `docs/reference/cq_cli.md` (generated target)

### Legacy/deletion targets
- Hand-maintained CQ CLI docs sections that drift from code signatures.

---

## SI-19: Add Optional Interactive Shell Command
### Goal
Provide a REPL-style CQ command for exploratory workflows without changing default non-interactive CLI behavior.

### Representative snippet
```python
# tools/cq/cli_app/commands/repl.py
from cyclopts import App


def repl(ctx: Annotated[CliContext | None, Parameter(parse=False)] = None) -> int:
    if ctx is None:
        raise RuntimeError("Context not injected")
    # non-exiting result action for REPL session
    return app.interactive_shell(result_action="print_non_int_return_int_as_exit_code")

# REPL workflows can execute tree-sitter-backed commands interactively:
# search, q pattern=..., neighborhood, etc.
```

### Tree-sitter library functions leveraged
- `QueryCursor.matches(...)`
- `QueryCursor.set_point_range(...)`

### Files to edit
- `tools/cq/cli_app/app.py`
- `tools/cq/cli_app/commands/__init__.py`
- `tests/unit/cq/test_cli_parsing.py`

### New files
- `tools/cq/cli_app/commands/repl.py`

### Legacy/deletion targets
- None required.

---

## SI-20: Expand Golden and Contract Tests for New CLI Policies
### Goal
Snapshot and contract-test help/errors/result-action/completion/telemetry behavior so upgrades remain deterministic.

### Representative snippet
```python
# tests/cli_golden/test_cq_help_output.py

def test_root_help_snapshot(update_golden: bool) -> None:
    text = capture_help(["--help"])
    assert_text_snapshot("cq_help_root.txt", text, update=update_golden)


def test_tree_sitter_error_channel_snapshot(update_golden: bool) -> None:
    # intentionally malformed query to exercise QueryCursor diagnostics path
    text = capture_error(["search", "(", "--lang", "python"])
    assert_text_snapshot("cq_error_tree_sitter_parse.txt", text, update=update_golden)
```

### Tree-sitter library functions leveraged
- `Parser.parse(...)`
- `QueryCursor.matches(...)`
- `Query.pattern_settings(...)`

### Files to edit
- `tests/unit/cq/test_cli_parsing.py`
- `tests/unit/cq/test_cli_meta_app.py`
- `tests/unit/cq/test_cli_result_handling.py`
- `tests/unit/cq/test_run_validators.py`
- `tests/unit/cq/test_cli_filter_options.py`
- `tests/cli_golden/test_search_golden.py`
- `tests/cli_golden/fixtures/search_help.txt`
- `tests/cli_golden/fixtures/run_help.txt`

### New files
- `tests/cli_golden/test_cq_help_output.py`
- `tests/cli_golden/test_cq_error_output.py`
- `tests/cli_golden/fixtures/cq_help_root.txt`
- `tests/cli_golden/fixtures/cq_error_tree_sitter_parse.txt`

### Legacy/deletion targets
- Outdated golden fixtures that encode pre-migration flag/help contracts.

---

## Aggregate File Plan
### Core files expected to change
- `tools/cq/cli.py`
- `tools/cq/cli_app/__init__.py`
- `tools/cq/cli_app/app.py`
- `tools/cq/cli_app/params.py`
- `tools/cq/cli_app/types.py`
- `tools/cq/cli_app/config.py`
- `tools/cq/cli_app/config_types.py`
- `tools/cq/cli_app/result.py`
- `tools/cq/cli_app/commands/analysis.py`
- `tools/cq/cli_app/commands/search.py`
- `tools/cq/cli_app/commands/query.py`
- `tools/cq/cli_app/commands/run.py`
- `tools/cq/cli_app/commands/chain.py`
- `tools/cq/cli_app/commands/report.py`
- `tools/cq/cli_app/commands/admin.py`
- `tools/cq/cli_app/commands/ldmd.py`
- `tools/cq/cli_app/commands/artifact.py`
- `tools/cq/cli_app/commands/neighborhood.py`
- `tools/cq/run/chain.py`

### New modules likely required
- `tools/cq/cli_app/result_action.py`
- `tools/cq/cli_app/telemetry.py`
- `tools/cq/cli_app/decorators.py`
- `tools/cq/cli_app/completion.py`
- `tools/cq/cli_app/help_text.py` (optional)
- `tools/cq/cli_app/groups.py` (optional)
- `tools/cq/cli_app/choices.py` (optional)
- `tools/cq/cli_app/commands/repl.py`

### Test/docs tooling likely required
- `tests/cli_golden/test_cq_help_output.py`
- `tests/cli_golden/test_cq_error_output.py`
- `scripts/generate_cq_completion.sh`
- `scripts/generate_cq_cli_docs.py`
- `docs/reference/cq_cli.md`

---

## Legacy/Deprecation Scope (Cross-Item Dependencies)
These deletions depend on multiple scope items and should be executed only after all prerequisite migrations are merged.

### D1: Remove manual launcher finalization path
Prerequisites:
- SI-03 (console policy)
- SI-04 (result action)
- SI-16 (telemetry wrapper)

Delete:
- `_execute_command(...)` in `tools/cq/cli_app/app.py`
- `_finalize_result(...)` in `tools/cq/cli_app/app.py`
- Any duplicate return coercion branches split between launcher and result handler

### D2: Remove duplicate alias app structures
Prerequisites:
- SI-05
- SI-08

Delete:
- `nb_app` and duplicate decorator paths in `tools/cq/cli_app/commands/neighborhood.py`
- redundant root registration lines for duplicate alias app objects

### D3: Remove runtime validation branches replaced by parse-time types
Prerequisites:
- SI-02
- SI-11

Delete:
- runtime membership checks for `report` preset
- runtime language fallback checks in neighborhood command
- runtime schema-kind invalid string branches where parse-time typing can reject

### D4: Remove repeated context guard boilerplate
Prerequisites:
- SI-15
- SI-16

Delete:
- per-command repeated `if ctx is None` blocks in command modules once shared decorator/injection contract is in place.

### D5: Remove stale help/golden artifacts
Prerequisites:
- SI-01
- SI-07
- SI-08
- SI-20

Delete/replace:
- golden fixture lines containing `--no-no-*` patterns
- stale root help snapshots that no longer match grouped/epilogue ordering.

---

## Implementation Checklist

## Phase 1: CLI Contract Normalization
- [ ] Implement SI-01 (boolean flag renaming + negative aliases).
- [ ] Implement SI-02 (parse-time choice typing).
- [ ] Implement SI-09 (explicit env var contract).
- [ ] Implement SI-11 (numeric validators).
- [ ] Implement SI-12 (consume_multiple list UX).

## Phase 2: Execution Architecture
- [ ] Implement SI-03 (console/error_console policy).
- [ ] Implement SI-04 (result-action pipeline).
- [ ] Implement SI-13 (parse_known_args in forwarding flows).
- [ ] Implement SI-15 (context injection consolidation).
- [ ] Implement SI-16 (telemetry wrapper).

## Phase 3: Command Graph + UX
- [ ] Implement SI-05 (alias modernization).
- [ ] Implement SI-06 (completion install command).
- [ ] Implement SI-07 (help panel and epilogue quality).
- [ ] Implement SI-08 (explicit grouping for all command surfaces).
- [ ] Implement SI-14 (full lazy-loading registration).
- [ ] Implement SI-19 (interactive shell command).

## Phase 4: Docs, Completion Artifacts, and QA
- [ ] Implement SI-17 (completion generation scripts in CI/release).
- [ ] Implement SI-18 (Cyclopts docs generation pipeline).
- [ ] Implement SI-20 (golden/contract tests).
- [ ] Execute cross-item deprecations D1-D5 in dependency order.

## Quality Gate (run after full implementation)
- [ ] `uv run ruff format`
- [ ] `uv run ruff check --fix`
- [ ] `uv run pyrefly check`
- [ ] `uv run pyright`
- [ ] `uv run pytest -q`

## Rollout Validation
- [ ] Verify `cq --help` has no `--no-no-*` flags.
- [ ] Verify `cq --install-completion` works with `add_to_startup=False`.
- [ ] Verify `cq search --help` and `cq run --help` goldens are updated and stable.
- [ ] Verify telemetry events include parse/exec/exit fields and tree-sitter runtime counters.
- [ ] Verify tree-sitter-backed commands (`search`, `q`, `neighborhood`) behave identically for core query semantics after CLI refactor.
