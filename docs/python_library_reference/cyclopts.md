Style/format target reference (internal “advanced reference” template): 

# Cyclopts advanced technical doc plan — feature-category catalog

Below is an **exhaustive section map** (the “Table of Contents”) for a *pyarrow-advanced.md–style* Cyclopts technical document. Each section is a deep-dive unit we can expand independently.

---

## 0) Scope, versioning, and the Cyclopts mental model

* What Cyclopts is optimizing for (type-hint-driven CLIs + docstring-driven UX)
* Compatibility + runtime expectations (Python version support; “stable” vs “latest” docs)
* End-to-end flow: `argv → tokenization → parameter binding/coercion → validation → function call → result action / exit` ([Cyclopts][1])

---

## 1) Installation, distribution, and project layout

* Install from PyPI / GitHub; development workflow tooling notes ([Cyclopts][2])
* Packaging patterns for CLIs (entry points / `__main__.py`) ([Cyclopts][3])

---

## 2) Core application model

* `cyclopts.run(...)` vs `App(...)` as the two “front doors” ([Cyclopts][1])
* `App` configuration surface (name, help, version, error/exit behavior, formatting hooks) ([Cyclopts][4])
* App recursion: composing apps-as-subcommands ([Cyclopts][4])

---

## 3) Command graph & dispatch

* Registering commands (`@app.command`) and defaults (`@app.default`) ([Cyclopts][4])
* Subcommands (apps registered as commands), nesting, and inheritance of configuration ([Cyclopts][4])
* Flattening subcommands (`name="*"`), collision rules, and tradeoffs ([Cyclopts][4])
* Command naming system: `name_transform`, explicit names, aliases ([Cyclopts][4])
* Async command support (event loop creation/backends) ([Cyclopts][4])

---

## 4) Parameter model (arguments, options, flags) and metadata

* How Cyclopts derives parameter behavior from **names + type hints + docstrings** ([Cyclopts][5])
* `Parameter` object + `typing.Annotated[...]` for explicit control (visibility, parsing knobs, etc.) ([Cyclopts][5])
* `*args` / `**kwargs` parsing rules and use cases ([Cyclopts][6])
* Global defaults via `App.default_parameter` (policy knobs applied app-wide) ([Cyclopts][7])

---

## 5) Type system and coercion engine

* Coercion “resolution order” (no-hint behavior, default-value-driven inference, etc.) ([Cyclopts][8])
* Built-in coercions across primitives, containers, unions/literals/enums, etc. ([Cyclopts][9])
* Flag semantics (e.g., counting flags; negative flags) ([Cyclopts][9])
* Library-provided types (e.g., bounded ints like `UInt16`, IO helpers like `StdioPath`) ([Cyclopts][10])

---

## 6) User-defined/structured types (“user classes”)

* Parsing user classes from CLI (including JSON dict/list parsing) ([Cyclopts][11])
* Namespace flattening strategies (e.g., `@Parameter(name="*")` on dataclasses) ([Cyclopts][10])
* Interop with dataclasses / attrs / pydantic (type-hint surface + behavior expectations) ([Cyclopts][1])

---

## 7) Validation framework

* Parameter validators (built-ins like Path/Number) ([Cyclopts][12])
* Group validators (e.g., LimitedChoice, MutuallyExclusive, all_or_none) ([Cyclopts][13])
* “Validation as UX”: how validation errors are rendered (plain vs rich) ([Cyclopts][14])

---

## 8) Groups and “presentation-aware” parameter organization

* Command groups vs parameter groups ([Cyclopts][15])
* Group-level validators and defaults ([Cyclopts][15])
* Group-level help formatting overrides ([Cyclopts][16])

---

## 9) Help system and output formatting

* Help generation pipeline (docstrings, parameter docs, command docs) ([Cyclopts][5])
* Markup formats supported in help (PlainText/Rich/RST/Markdown) ([Cyclopts][17])
* Help flags, epilogue, and customization entry points ([Cyclopts][17])
* Help formatter architecture: built-in formatters + custom formatters, panels/tables/columns ([Cyclopts][16])

---

## 10) Execution semantics, return values, and error/exit policy

* `App.__call__` inputs (string vs list vs default `sys.argv[1:]`) ([Cyclopts][18])
* Return value handling + exit behavior (sectioned in docs) ([Cyclopts][18])
* Rich formatted tracebacks vs standard tracebacks (how to enable/structure) ([Cyclopts][14])
* Result-action pipelines (e.g., “call_if_callable”, “print_non_int_sys_exit”) ([Cyclopts][19])

---

## 11) Configuration system (files, env vars, in-memory, composition)

* `App.config` as a chain of callables that mutate an argument/token collection ([Cyclopts][20])
* Built-in config providers (TOML/YAML/etc.), root keys, search-parent discovery ([Cyclopts][20])
* Env var injection (built-in `Env(...)`), JSON config, and combining sources ([Cyclopts][21])
* User-specified config file pattern (via Meta App) ([Cyclopts][20])

---

## 12) Shell completion and CLI tooling

* Shell completion support (bash/zsh/fish) ([Cyclopts][22])
* `cyclopts run` wrapper for development scripts (completion without packaging) ([Cyclopts][22])
* Programmatic vs manual completion installation paths ([Cyclopts][22])

---

## 13) Developer-UX extras: interactive shell and text editor integration

* Built-in interactive shell (`app.interactive_shell`) and help flows (`help_print`) ([Cyclopts][23])
* Text editor helper `cyclopts.edit()` + related exceptions for “didn’t save / didn’t change” workflows ([Cyclopts][24])

---

## 14) Performance and large-app architecture patterns

* Lazy loading commands via import-path strings (`"module:path_or_attr"`) and resolution points (exec/help/indexing) ([Cyclopts][25])
* Lazy-loading gotchas with shared objects like `Group` definitions ([Cyclopts][25])

---

## 15) Advanced composition patterns

* Meta App: launching an app from an app; merged help; “launch pipeline” control ([Cyclopts][26])
* Sharing parameters across commands (dataclass-based “context-ish” pattern; namespace flattening) ([Cyclopts][10])
* Dataclass-as-command pattern + callable result-action pipeline ([Cyclopts][19])
* Command chaining: not built-in, but implementable with token controls (e.g., allow leading hyphens) ([Cyclopts][27])
* AutoRegistry integration for “string → implementation” selection (common plugin/provider pattern) ([Cyclopts][28])

---

## 16) Docs generation integrations

* Sphinx directive and its option surface (recursive vs flat, include hidden, filters, heading controls, etc.) ([Cyclopts][29])
* MkDocs plugin (experimental) and analogous option surface ([Cyclopts][30])

---

## 17) Testing and QA

* Unit testing cookbook: mocking, exit codes, stdout checks, env vars, config files, help output assertions ([Cyclopts][21])
* Known issues / edge cases to codify as regression tests ([Cyclopts][31])

---

## 18) Migration and comparative framing

* Migration guide from Typer (mapping concepts + pitfalls) ([Cyclopts][32])
* “vs Typer / Fire / Arguably” comparison pages (design rationale + differentiators) ([Cyclopts][33])

---

If you want the next step, I can start with **Section 0 + Section 2 (mental model + core API map)** and write it in the same “dense, agent-ready, call-signature-first” style as your advanced library docs.

[1]: https://cyclopts.readthedocs.io/ "Cyclopts — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/stable/Installation.html "Installation — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/latest/packaging.html "Packaging — cyclopts"
[4]: https://cyclopts.readthedocs.io/en/latest/commands.html "Commands — cyclopts"
[5]: https://cyclopts.readthedocs.io/en/latest/parameters.html "Parameters — cyclopts"
[6]: https://cyclopts.readthedocs.io/en/latest/args_and_kwargs.html "Args & Kwargs — cyclopts"
[7]: https://cyclopts.readthedocs.io/en/latest/default_parameter.html "Default Parameter — cyclopts"
[8]: https://cyclopts.readthedocs.io/en/latest/getting_started.html "Getting Started — cyclopts"
[9]: https://cyclopts.readthedocs.io/en/latest/rules.html "Coercion Rules — cyclopts"
[10]: https://cyclopts.readthedocs.io/en/latest/cookbook/sharing_parameters.html "Sharing Parameters — cyclopts"
[11]: https://cyclopts.readthedocs.io/en/latest/user_classes.html "User Classes — cyclopts"
[12]: https://cyclopts.readthedocs.io/en/latest/parameter_validators.html "Parameter Validators — cyclopts"
[13]: https://cyclopts.readthedocs.io/en/latest/group_validators.html "Group Validators — cyclopts"
[14]: https://cyclopts.readthedocs.io/en/latest/cookbook/rich_formatted_exceptions.html "Rich Formatted Exceptions — cyclopts"
[15]: https://cyclopts.readthedocs.io/en/latest/groups.html "Groups — cyclopts"
[16]: https://cyclopts.readthedocs.io/en/latest/help_customization.html "Help Customization — cyclopts"
[17]: https://cyclopts.readthedocs.io/en/latest/help.html "Help — cyclopts"
[18]: https://cyclopts.readthedocs.io/en/latest/app_calling.html "App Calling & Return Values — cyclopts"
[19]: https://cyclopts.readthedocs.io/en/latest/cookbook/dataclass_commands.html "Dataclass Commands — cyclopts"
[20]: https://cyclopts.readthedocs.io/en/latest/config_file.html "Config Files — cyclopts"
[21]: https://cyclopts.readthedocs.io/en/latest/cookbook/unit_testing.html "Unit Testing — cyclopts"
[22]: https://cyclopts.readthedocs.io/en/latest/shell_completion.html "Shell Completion — cyclopts"
[23]: https://cyclopts.readthedocs.io/en/latest/cookbook/interactive_help.html "Interactive Shell & Help — cyclopts"
[24]: https://cyclopts.readthedocs.io/en/latest/text_editor.html "Text Editor — cyclopts"
[25]: https://cyclopts.readthedocs.io/en/latest/lazy_loading.html "Lazy Loading — cyclopts"
[26]: https://cyclopts.readthedocs.io/en/latest/meta_app.html "Meta App — cyclopts"
[27]: https://cyclopts.readthedocs.io/en/latest/command_chaining.html "Command Chaining — cyclopts"
[28]: https://cyclopts.readthedocs.io/en/latest/autoregistry.html "AutoRegistry — cyclopts"
[29]: https://cyclopts.readthedocs.io/en/latest/sphinx_integration.html "Sphinx Integration — cyclopts"
[30]: https://cyclopts.readthedocs.io/en/latest/mkdocs_integration.html "MkDocs Integration — cyclopts"
[31]: https://cyclopts.readthedocs.io/en/latest/known_issues.html "Known Issues — cyclopts"
[32]: https://cyclopts.readthedocs.io/en/latest/migration/typer.html "Migrating From Typer — cyclopts"
[33]: https://cyclopts.readthedocs.io/en/latest/vs_typer/README.html "Typer Comparison — cyclopts"

## Cyclopts Advanced (Section 0 + 2) — mental model + core API map (agent-ready)

### Version anchors used here

* API signatures + semantics referenced from Cyclopts **v4.4.1 API page** (stable signature snapshot). ([Cyclopts][1])
* “App calling / return values / error policy” referenced from **latest** docs page. ([Cyclopts][2])
* “Getting started” parsing/binding rules + type-hint reliance from **latest** docs page. ([Cyclopts][3])
* Repo README notes Python requirement. ([GitHub][4])

---

# 0) Mental model: Cyclopts is a “Python call binder” + “CLI app graph”

## 0.1 The core invariant: CLI binding follows Python call semantics

Cyclopts treats your callable’s **signature** (names, position/keyword-ability, defaults) + **type hints** as the spec, then binds CLI tokens as if calling the function in Python. It explicitly leans on type hints as first-class input schema. ([Cyclopts][3])

Implications for agent implementers:

* **Argument binding rules are Python’s**: you can pass values positionally or via `--kw`, and keyword order can float, but you can’t place a positional after you’ve “keyworded past” it (same as `f(name="Alice", 3)` being invalid). ([Cyclopts][3])
* The POSIX `"--"` delimiter forces remaining tokens to be positional (default). ([Cyclopts][3])
* If a parameter has **no type annotation**, Cyclopts tries the **type of the default value**, else falls back to `str`. ([Cyclopts][3])

## 0.2 “Everything is an App”: command tree is an App graph, not a flat registry

Internally, Cyclopts models commands as **sub-Apps**. You can retrieve a command’s subapp via `app[key]`. Crucially: “All commands get registered… as subapps” and the underlying handler lives at `app[key].default_command`. ([Cyclopts][1])

Agent takeaways:

* You can treat a CLI as a **navigable graph** of `App` nodes, each with its own defaults/config/errors. `App.__getitem__` is your “descend into subtree” primitive. ([Cyclopts][1])
* Lazy-loaded commands (registered via import path string) are resolved **on first access** through this subapp mechanism. ([Cyclopts][1])

## 0.3 Execution pipeline (what happens when `app(...)` runs)

A useful decomposition for “LLM agents generating code” is the following pipeline (each stage maps to a public method):

1. **Token source & normalization**

* If `tokens is None`, default is `sys.argv[1:]`. ([Cyclopts][2])
* If you pass a **string**, Cyclopts uses `shlex.split` to get tokens. ([Cyclopts][2])

2. **Command path extraction**

* `parse_commands(...)` separates “command tokens” (subapp traversal) from remaining tokens. ([Cyclopts][1])

3. **Argument model construction**

* `assemble_argument_collection(parse_docstring=...)` builds an `ArgumentCollection` (derived from signature + `Parameter` metadata). The `parse_docstring` flag is explicitly a perf switch. ([Cyclopts][1])

4. **Binding + conversion + validation**

* `parse_known_args(...)` returns `(command, bound_args, unused_tokens, ignored)`; `parse_args(...)` is the strict version that raises if tokens remain. ([Cyclopts][1])
* `ignored` is a map for parameters annotated `parse=False` (intended to simplify meta-apps / higher-level orchestrators). ([Cyclopts][1])

5. **Invoke + post-process result (`result_action`)**

* `__call__` executes the selected command and processes its return value via `App.result_action`. ([Cyclopts][2])
* Defaults are chosen to match behavior for **standalone scripts** and **installed entry points** (which themselves feed return values to `sys.exit`). For embedding/testing, switch to `result_action="return_value"`. ([Cyclopts][2])

6. **Error policy + consoles**
   Cyclopts defaults to “CLI-friendly” failure:

* runtime errors (coercion/validation/etc.) typically trigger formatted error output and `sys.exit(1)` unless you disable it. ([Cyclopts][2])
* Two separate consoles: `App.console` (normal output) vs `App.error_console` (errors; defaults to stderr). ([Cyclopts][2])
* These behaviors are inherited by child apps and overrideable per call. ([Cyclopts][2])

## 0.4 Async is a first-class dispatch mode (but still “same pipeline”)

If the resolved command is `async def`, Cyclopts can run it using an async backend; `backend` supports `"asyncio"` or `"trio"` (with an extra requirement for trio). The sync entrypoint is `__call__`, the async entrypoint is `run_async`. ([Cyclopts][1])

## 0.5 Minimal runtime constraints

Cyclopts requires Python **>= 3.10** (per project README). ([GitHub][4])

---

# 2) Core API map (call-signature-first)

Below is the “core surface area” you’ll want agents to internalize for implementation, testing, introspection, and embedding.

---

## 2.1 `cyclopts.run`: single-call “CLI wrapper” for one callable

```python
cyclopts.run(callable, /, *, result_action=None) -> Any
```

* Accepts a function **or coroutine function**; registers it as the default command on a fresh `App`, then runs the app. ([Cyclopts][1])
* Semantically equivalent to:

  * `app = App(); app.default(callable); app()` ([Cyclopts][1])
* `result_action` controls return-value handling (see §2.6). Setting `"return_value"` is the canonical embedding/testing mode. ([Cyclopts][1])

**Agent note:** `run()` is ideal for “one-function CLIs” and quick prototypes; migrate to `App` once you need subcommands, config layering, or stable error policy.

---

## 2.2 `App`: root object + node type for every subcommand

```python
class cyclopts.App(
    name=None, help=None, usage=None, *,
    alias=None,
    default_command=None,
    default_parameter: Parameter | None = None,
    config=None,
    version=None,
    version_flags=['--version'],
    show: bool = True,
    console: Console | None = None,
    error_console: Console | None = None,
    help_flags=['--help', '-h'],
    help_format: Literal['markdown','md','plaintext','restructuredtext','rst','rich'] | None = None,
    help_on_error: bool | None = None,
    help_epilogue: str | None = None,
    version_format: Literal['markdown','md','plaintext','restructuredtext','rst','rich'] | None = None,
    group=None,
    group_arguments=None, group_parameters=None, group_commands=None,
    validator=None,
    name_transform: Callable[[str], str] | None = None,
    sort_key=None,
    end_of_options_delimiter: str | None = None,
    print_error: bool | None = None,
    exit_on_error: bool | None = None,
    verbose: bool | None = None,
    suppress_keyboard_interrupt: bool = True,
    backend: Literal['asyncio','trio'] | None = None,
    help_formatter=None,
    result_action=None
)
```

(Exact signature above from API docs.) ([Cyclopts][1])

### Constructor “knobs” agents should treat as policy levers

* **Meta commands**: help/version flags are part of the command set and even show up in `list(app)` iteration. ([Cyclopts][1])
* **Name / alias**: `name` can be a single string or iterable; `alias` adds extra names. ([Cyclopts][1])
* **Formatting + consoles**: `console` vs `error_console` have a resolution order (call override → subapp → parent chain → default). ([Cyclopts][1])
* **Error policy**: `exit_on_error`, `print_error`, `help_on_error`, `verbose` are inherited through subapps and overridable per method call. ([Cyclopts][2])
* **Result policy**: `result_action` is the “return value contract” for the whole app unless overridden per call. ([Cyclopts][1])
* **Async backend**: choose `"asyncio"` (default) or `"trio"` (requires extra). ([Cyclopts][1])
* **`end_of_options_delimiter`**: defaults to `"--"` but can be disabled by setting `""` (empty string). ([Cyclopts][1])

---

## 2.3 Registering commands: `App.command` + `App.default`

### `App.command`: register a function / subapp / lazy import path

```python
App.command(obj, name=None, *, alias=None, **kwargs) -> None
```

* `obj` may be:

  * a **callable**
  * an **App** (subcommand group)
  * a **string import path** `"module.path:function_or_app_name"` (lazy loading) ([Cyclopts][1])
* `name` default behavior:

  * registering an `App` → uses `app.name`
  * registering a function or import-path attribute → uses name after `name_transform` ([Cyclopts][1])
* Special `name="*"`: **flatten** all sub-App commands into the parent app (App instances only). ([Cyclopts][1])
* `**kwargs` is “any argument that `App` can take” (i.e., you can tune help/version flags, grouping, etc. per subcommand node). ([Cyclopts][1])

**Agent pattern:** treat `App.command()` as adding a node to an App graph; commands are discoverable via `__iter__()` and addressable via `app["name"]`. ([Cyclopts][1])

### `App.default`: register default action handler (when no command is specified)

```python
App.default(obj: T, *, validator: Callable | None = None) -> T
# or:
App.default(obj: None = None, *, validator: Callable | None = None) -> Callable[[T], T]
```

The decorator registers the function to run when no command is provided. ([Cyclopts][1])

---

## 2.4 Navigating the command graph: `__getitem__`, `__iter__`, `parse_commands`

### `App.__getitem__`: descend into subapps by command name

```python
App.__getitem__(key: str) -> App
```

* Commands are subapps; handler is `app[key].default_command`.
* If the command was lazily registered, resolution happens on first access. ([Cyclopts][1])

### `App.__iter__`: enumerate commands *including meta commands*

```python
App.__iter__() -> Iterator[str]
```

Help and version flags are treated as commands and included in iteration output. ([Cyclopts][1])

### `App.parse_commands`: split command tokens from argument tokens

```python
App.parse_commands(tokens=None, *, include_parent_meta=True)
  -> tuple[tuple[str, ...], tuple[App, ...], list[str]]
```

* Returns the command token path, the traversed `App` chain, and leftover tokens. ([Cyclopts][1])
* `include_parent_meta` controls whether parent meta apps appear in the execution path (relevant once you adopt meta-app patterns). ([Cyclopts][1])

---

## 2.5 Parsing without executing: `assemble_argument_collection`, `parse_known_args`, `parse_args`

These are “LLM agent power tools” for testing, meta-apps, documentation generation, and custom execution.

### `App.assemble_argument_collection`: build derived argument model

```python
App.assemble_argument_collection(*, default_parameter=None, parse_docstring=False) -> ArgumentCollection
```

* `parse_docstring=False` is explicitly recommended when you don’t need help strings (performance). ([Cyclopts][1])

### `App.parse_known_args`: parse + bind + convert, but allow trailing tokens

```python
App.parse_known_args(tokens=None, *, console=None, error_console=None, end_of_options_delimiter=None)
  -> tuple[Callable, BoundArguments, list[str], dict[str, Any]]
```

* Returns:

  * `command`: the resolved bare function
  * `bound`: `inspect.BoundArguments` for that function
  * `unused_tokens`: leftover tokens (not consumed)
  * `ignored`: mapping of parameters marked `parse=False` (to simplify meta-app orchestration) ([Cyclopts][1])

### `App.parse_args`: strict parse + bind; errors if tokens remain

```python
App.parse_args(tokens=None, *, console=None, error_console=None,
              print_error=None, exit_on_error=None, help_on_error=None,
              verbose=None, end_of_options_delimiter=None)
  -> tuple[Callable, BoundArguments, dict[str, Any]]
```

* Raises `UnusedCliTokensError` if any tokens remain after parsing. ([Cyclopts][1])
* Use this when you want “single-command, fully consumed” semantics (typical CLIs). ([Cyclopts][1])

---

## 2.6 Executing: `App.__call__` and `App.run_async`

### `App.__call__`: the canonical sync entrypoint

```python
App.__call__(tokens=None, *,
             console=None, error_console=None,
             print_error=None, exit_on_error=None, help_on_error=None, verbose=None,
             end_of_options_delimiter=None, backend=None,
             result_action=...) -> Any
```

Signature includes many predefined `result_action` modes (strings), plus custom callables or pipelines of actions. ([Cyclopts][1])

Behavior highlights:

* `tokens` default is `sys.argv[1:]`; string tokens are split with `shlex.split`. ([Cyclopts][2])
* Default `result_action` is chosen to make return/exiting consistent with installed scripts. ([Cyclopts][2])
* For embedding/testing, set `result_action="return_value"` so the call returns without `sys.exit`. ([Cyclopts][2])

### `App.run_async`: async execution without creating a new loop

```python
async App.run_async(tokens=None, *,
                    console=None, error_console=None,
                    print_error=None, exit_on_error=None, help_on_error=None, verbose=None,
                    end_of_options_delimiter=None, backend=None,
                    result_action=...) -> Any
```

Use in already-async contexts (notebooks, async services). Backend can be overridden; trio requires extra install. ([Cyclopts][1])

---

# “Agent implementation recipes” (how to *use* the model above)

## Recipe A: embed Cyclopts as a library (no sys.exit, normal Python exceptions)

Use *two* policies:

1. don’t exit on Cyclopts errors
2. don’t sys.exit on return values

```python
app = App(result_action="return_value", exit_on_error=False, print_error=False)
```

* `exit_on_error=False` makes unknown-command / coercion / validation failures raise exceptions instead of exiting. ([Cyclopts][2])
* `result_action="return_value"` preserves the raw return for programmatic callers. ([Cyclopts][2])

## Recipe B: custom dispatcher (parse then call yourself)

If your orchestrator wants full control over invocation, do:

* strict: `command, bound, ignored = app.parse_args(tokens)`
* flexible/meta: `command, bound, unused, ignored = app.parse_known_args(tokens)`

…then call:

```python
result = command(*bound.args, **bound.kwargs)
```

`parse_known_args` is explicitly designed to return `ignored` for `parse=False` params, which is a meta-app facilitation hook. ([Cyclopts][1])

## Recipe C: introspection for docs / “LLM tool discovery”

* Build argument models with docstrings only when needed:

  * `args = app.assemble_argument_collection(parse_docstring=True)` for help metadata
  * `parse_docstring=False` for faster structural introspection ([Cyclopts][1])
* Enumerate commands, including meta commands:

  * `list(app)` yields `--help`, `-h`, `--version`, etc. ([Cyclopts][1])
* Descend to a subcommand node:

  * `sub = app["deploy"]` → handler is `sub.default_command` ([Cyclopts][1])

---


[1]: https://cyclopts.readthedocs.io/en/v4.4.1/api.html "API — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/latest/app_calling.html "App Calling & Return Values — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/latest/getting_started.html "Getting Started — cyclopts"
[4]: https://github.com/BrianPugh/cyclopts "GitHub - BrianPugh/cyclopts: Intuitive, easy CLIs based on python type hints."

## Cyclopts Advanced (core increment) — `result_action` deep dive + console/error_console resolution + structured logging/telemetry patterns

Primary reference: Cyclopts API doc for `App.result_action`, `App.console`, `App.error_console` (v4.4.1). ([Cyclopts][1])

---

# 1) `result_action`: return-value policy is a *first-class* execution stage

## 1.1 Contract surface (what accepts `result_action`, and when it runs)

**Where it applies**

* `App.result_action` is an `App` attribute controlling how `App.__call__()` and `App.run_async()` handle the command return value. ([Cyclopts][1])
* `App.__call__(..., result_action=...)` and `App.run_async(..., result_action=...)` can override app-level policy per invocation (same union type: literal strings, callable, or iterable/pipeline). ([Cyclopts][1])
* Default policy is chosen to match how installed entry points behave (scripts commonly `sys.exit(return_value)`), so Cyclopts defaults to a `sys.exit`-oriented mode. ([Cyclopts][2])

**Type**

```python
result_action: (
  Literal[
    "return_value",
    "call_if_callable",
    "print_non_int_return_int_as_exit_code",
    "print_str_return_int_as_exit_code",
    "print_str_return_zero",
    "print_non_none_return_int_as_exit_code",
    "print_non_none_return_zero",
    "return_int_as_exit_code_else_zero",
    "print_non_int_sys_exit",
    "sys_exit",
    "return_none",
    "return_zero",
    "print_return_zero",
    "sys_exit_zero",
    "print_sys_exit_zero",
  ]
  | Callable[[Any], Any]
  | Iterable[Literal[...] | Callable[[Any], Any]]
  | None
)
```

This exact literal set (and “callable or iterable pipeline” rule) is defined in the API docs. ([Cyclopts][1])

**Pipeline semantics**

* If `result_action` is a sequence (list/tuple), actions are applied **left-to-right**; each action consumes the previous action’s output. ([Cyclopts][1])
* Sequence entries can be any mix of predefined literal strings and custom callables. ([Cyclopts][1])
* Empty sequences raise `ValueError` at app init. ([Cyclopts][1])

---

## 1.2 Predefined modes: normalized taxonomy for agents

Think of each mode as picking **(A) printing behavior** × **(B) return behavior** × **(C) exit behavior**, with special casing for `bool` and `int`.

### A) “CLI-native” modes (may call `sys.exit`)

These are designed for real CLIs; they terminate the process and/or print for you.

1. **`"print_non_int_sys_exit"` (default)**

* If result is `bool`: success/failure exit code (True→0, False→1).
* If result is `int`: exit code is that integer.
* If result is non-`None` and non-`int`: print it, then exit 0.
* If result is `None`: exit 0. ([Cyclopts][1])

2. **`"sys_exit"`**

* Never prints output.
* If result is `bool`: True→0, False→1.
* If result is `int`: exit(result).
* Else: exit 0. ([Cyclopts][1])

3. **`"sys_exit_zero"`**

* Always exits 0, regardless of result. ([Cyclopts][1])

4. **`"print_sys_exit_zero"`**

* Always prints result (even `None`), then exits 0. ([Cyclopts][1])

**When to pick these (rule-of-thumb)**

* Your commands return **exit codes** or **messages** and you want Cyclopts to behave like a conventional CLI: default `"print_non_int_sys_exit"`. ([Cyclopts][1])
* Your commands **print all output themselves** (including JSON) and you want Cyclopts to only manage exit codes: `"sys_exit"`. ([Cyclopts][1])

---

### B) “Embedding/testing” modes (return an int-ish exit code, but do NOT `sys.exit`)

These are ideal inside Python tests or when embedding into a service process.

1. **`"return_value"`**

* Returns the command value unchanged (no printing, no sys.exit). Recommended for embedding/testing. ([Cyclopts][2])

2. **`"print_non_int_return_int_as_exit_code"`**

* Prints non-ints (excluding `None`), returns an int-ish exit code for bool/int, else returns 0. ([Cyclopts][1])

3. **`"print_str_return_int_as_exit_code"`**

* Prints only `str` results; returns exit code for bool/int; otherwise returns 0. ([Cyclopts][1])

4. **`"print_str_return_zero"`**

* Prints only `str` results; always returns 0. ([Cyclopts][1])

5. **`"print_non_none_return_int_as_exit_code"`**

* Prints any non-`None` (including ints), returns exit code for bool/int; otherwise returns 0. ([Cyclopts][1])

6. **`"print_non_none_return_zero"`**

* Prints any non-`None`; always returns 0. ([Cyclopts][1])

7. **`"return_int_as_exit_code_else_zero"`**

* Prints nothing; returns exit code for bool/int; otherwise returns 0. ([Cyclopts][1])

8. **`"return_none"` / `"return_zero"`**

* Ignores command result and returns `None` or `0`. ([Cyclopts][1])

9. **`"print_return_zero"`**

* Prints result (even `None`), returns 0. ([Cyclopts][1])

**When to pick these**

* You need Cyclopts to compute an “exit code” but you don’t want process termination: `return_int_as_exit_code_else_zero` (silent) or `print_*_return_int_as_exit_code` (visible). ([Cyclopts][1])
* You’re writing pytest assertions around stdout + exit behavior: these modes plus `capsys` are directly used in Cyclopts’ testing cookbook. ([Cyclopts][3])

---

### C) Transform-only mode used for composition

**`"call_if_callable"`**

* If the result is callable, call it with no args; else return as-is.
* Explicitly intended for composition/pipelines, especially the “dataclass command” pattern. ([Cyclopts][1])

Canonical example (from docs): a pipeline `["call_if_callable", "print_non_int_sys_exit"]` calls the dataclass instance’s `__call__`, then prints+exits with the resulting string. ([Cyclopts][4])

---

## 1.3 Custom callables and pipelines: where you attach observability

### Custom callable

A custom `result_action` callable is invoked with the command result and returns a processed value. ([Cyclopts][1])

You use this to:

* normalize output types (e.g., convert domain objects → JSON string),
* add *result-level* telemetry (latency and exceptions need separate handling),
* enforce “CLI contract” invariants (e.g., only `str|int|bool|None` escape).

### Pipeline composition

A pipeline is the most powerful primitive: mix built-ins and callables; left-to-right. ([Cyclopts][1])

**Agent invariant:** avoid placing a sys-exiting mode (`sys_exit*` or `print_*_sys_exit*`) before later stages; anything after `sys.exit` won’t run. (This is a design-level constraint, not a Cyclopts footgun.)

**Dataclass pattern requires pipeline**
Cyclopts explicitly documents this: without `call_if_callable`, you’ll print the dataclass instance representation rather than the computed string. ([Cyclopts][4])

---

# 2) `console` vs `error_console`: deterministic resolution + Unix-friendly separation

Cyclopts uses Rich `Console` objects for output and errors, with **separate resolution chains**.

## 2.1 `App.console` resolution order

Cyclopts resolves the “normal output console” in this order: ([Cyclopts][1])

1. explicit `console=` passed to the method call (`__call__`, `parse_args`, etc.)
2. the relevant subcommand’s `App.console` (if not `None`)
3. parent `App.console` chain (first non-`None`)
4. default Rich `Console` (stdout-oriented)

## 2.2 `App.error_console` resolution order

Cyclopts resolves “error output console” similarly, but with a stderr default: ([Cyclopts][1])

1. explicit `error_console=` passed to the method call
2. the relevant subcommand’s `App.error_console`
3. parent `App.error_console` chain
4. default Rich `Console(stderr=True)`

Cyclopts explicitly calls out why: Unix redirection allows users to split outputs (`program > out 2> err`). ([Cyclopts][1])

## 2.3 Rich Console fundamentals Cyclopts relies on (stderr + file redirection)

Rich’s own docs define:

* default console writes to `sys.stdout`
* `Console(stderr=True)` writes to `sys.stderr`
* `Console(file=...)` writes to a file-like object opened for text output. ([rich.readthedocs.io][5])

This matters because Cyclopts’ **default behavior** is: help/version/normal messages to `App.console`, and formatted errors to `App.error_console`. ([Cyclopts][2])

---

# 3) Error policy knobs (don’t fight them—choose a stance)

Cyclopts is mostly hands-off, but by default it:

* prints formatted CLI-friendly errors for Cyclopts runtime errors (e.g., coercion/validation)
* exits with code 1 to avoid raw tracebacks to CLI users. ([Cyclopts][2])

Key toggles (inherited by child apps, overrideable per call): ([Cyclopts][2])

* `exit_on_error`: default True, calls `sys.exit(1)` for Cyclopts runtime/parse errors
* `print_error`: default True, prints rich-formatted errors
* `help_on_error`: default False, prints help before errors
* `verbose`: default False, includes developer-centric detail

The app_calling doc shows the exact behavior shift:

* default: unknown command prints formatted error and exits
* with `exit_on_error=False` + `print_error=False`: Cyclopts raises exceptions like normal Python, allowing you to catch/log them. ([Cyclopts][2])

---

# 4) Wiring Cyclopts into structured logging & telemetry (agent-ready patterns)

Below are patterns that *compose with* Cyclopts instead of “forking” it.

## Pattern 1 — Service/embedded mode: no exits, no printing, full control

**Goal:** Cyclopts is your argument binder; your host process owns logging, tracing, and response formatting.

**App stance**

* `result_action="return_value"` so `app(...)` returns instead of `sys.exit`. ([Cyclopts][2])
* `exit_on_error=False`, `print_error=False` so Cyclopts errors raise normally. ([Cyclopts][2])

```python
from cyclopts import App
app = App(
    result_action="return_value",
    exit_on_error=False,
    print_error=False,
)
```

([Cyclopts][2])

**Execution wrapper with OpenTelemetry span**

```python
from opentelemetry import trace

tracer = trace.get_tracer("my.cli")

def invoke(tokens):
    # Instrument the entire parse+execute as one span.
    with tracer.start_as_current_span("cyclopts.invoke") as span:
        span.set_attribute("cli.tokens", str(tokens))
        try:
            return app(tokens)
        except Exception as e:
            # record_exception / status handling depends on your OTel SDK setup
            span.record_exception(e)
            raise
```

OpenTelemetry’s cookbook shows the `trace.get_tracer(...); with tracer.start_as_current_span(...): ...` pattern. ([OpenTelemetry][6])

**Why this works**

* You don’t fight Cyclopts’ default sys.exit or rich error printing because you’ve disabled them at the source. ([Cyclopts][2])

---

## Pattern 2 — Parse/execute split: use `parse_args` for structured “call metadata”

**Goal:** precise telemetry fields (command identity, bound args/kwargs) with your own exception + result handling.

Cyclopts exposes parse primitives returning the resolved `command` and `inspect.BoundArguments`. ([Cyclopts][1])

```python
def invoke_with_metadata(tokens):
    command, bound, ignored = app.parse_args(
        tokens,
        exit_on_error=False,
        print_error=False,
    )

    with tracer.start_as_current_span("cyclopts.command") as span:
        span.set_attribute("cli.command", getattr(command, "__name__", str(command)))
        span.set_attribute("cli.kwargs", str(bound.kwargs))
        span.set_attribute("cli.args", str(bound.args))
        try:
            return command(*bound.args, **bound.kwargs)
        except Exception as e:
            span.record_exception(e)
            raise
```

* `parse_args` exists explicitly and returns `(command, bound, ignored)`; it is the strict “no unused tokens” parser. ([Cyclopts][1])
* If you need tolerant parsing (for meta-apps or chaining), use `parse_known_args` to obtain `unused_tokens` as well. ([Cyclopts][1])

**Agent caveat:** be deliberate about logging raw args/kwargs (redaction / secrets) — Cyclopts supports env/config injection elsewhere, so sensitive values might arrive via non-CLI channels too.

---

## Pattern 3 — CLI mode with *dual channel*: human errors on stderr + machine logs elsewhere

**Goal:** keep Cyclopts’ rich UX for end users, while emitting structured logs for operators.

Leverage the console split:

* Cyclopts prints runtime errors to `error_console` (stderr by default). ([Cyclopts][2])
* Rich `Console(file=...)` allows redirecting output to an arbitrary file-like sink. ([rich.readthedocs.io][5])

**Option A: keep Cyclopts UX, add *result_action* telemetry**
Use a pipeline: “normalize result → emit telemetry → default behavior”.

```python
import logging
logger = logging.getLogger("mycli")

def log_result(result):
    logger.info("cli.result", extra={"result_type": type(result).__name__})
    return result

app = App(result_action=[log_result, "print_non_int_sys_exit"])
```

* Pipelines are supported and apply actions left-to-right. ([Cyclopts][1])
* Default mode semantics for `"print_non_int_sys_exit"` are documented. ([Cyclopts][1])

**Option B: suppress rich error printing, emit JSON logs, still return “exit code”**
If you want errors in structured logs (instead of rich panels), set:

* `print_error=False`, `exit_on_error=False`, catch `CycloptsError` and translate to your own exit code. ([Cyclopts][2])
  Then choose a non-exiting `result_action` (e.g., `return_int_as_exit_code_else_zero`) and have your entry point call `sys.exit(code)` itself.

---

## Pattern 4 — “JSON output CLI” pattern: don’t let Cyclopts print objects you didn’t serialize

If your CLI outputs machine-readable JSON to **stdout**, you generally want:

* **no incidental prints** from Cyclopts result handling,
* errors strictly on stderr.

Two common stances:

### Stance A: command prints JSON; Cyclopts handles exit codes only

Set `result_action="sys_exit"` (no printing; exit code from bool/int else 0). ([Cyclopts][1])

```python
app = App(result_action="sys_exit")

@app.command
def dump(...) -> int:
    print(json_payload)  # your JSON
    return 0
```

### Stance B: command returns JSON string; Cyclopts prints only strings

Set `result_action="print_str_return_int_as_exit_code"` (prints strings; int/bool as code). ([Cyclopts][1])

This avoids accidentally printing non-string objects (like dicts or dataclasses) which would corrupt downstream parsers.

---

## Pattern 5 — Capturing help/errors for tests or telemetry attachments

Rich supports `Console(file=StringIO())` to capture output (recommended for tests). ([rich.readthedocs.io][5])

Combine with Cyclopts method-level override:

* pass `console=` / `error_console=` directly (top priority in console resolution). ([Cyclopts][1])

---

## Pattern 6 — KeyboardInterrupt observability

Cyclopts defaults to `suppress_keyboard_interrupt=True` (graceful exit without noisy error). If you need to log Ctrl-C as a “canceled run,” set it to `False` so `KeyboardInterrupt` can propagate to your instrumentation layer. ([Cyclopts][1])

---

# 5) Implementation checklist for agents (fast correctness)

1. Pick a stance:

* CLI UX-first: keep default `print_non_int_sys_exit` + rich errors. ([Cyclopts][1])
* Embedding/service: `result_action="return_value"`, `exit_on_error=False`, `print_error=False`. ([Cyclopts][2])

2. If you need observability:

* wrap `app(tokens)` in a span and catch exceptions, OR
* do parse/execute split with `parse_args` to log command name + bound args. ([Cyclopts][1])

3. If you need post-result transforms or logging:

* use a `result_action` pipeline: callables + literals left-to-right. ([Cyclopts][1])

4. Keep stdout clean for machine outputs:

* use `sys_exit` (command prints) or `print_str_*` modes (command returns str only). ([Cyclopts][1])

---

If you want the next “core” increment after this: deep dive **error types** (CycloptsError hierarchy + which stage throws what), plus a concrete “telemetry-first wrapper” that records (a) parse latency, (b) execution latency, (c) exit code, (d) normalized error class, using `parse_args`/`parse_known_args` + a `result_action` pipeline.

[1]: https://cyclopts.readthedocs.io/en/v4.4.1/api.html "API — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/latest/app_calling.html "App Calling & Return Values — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/latest/cookbook/unit_testing.html?utm_source=chatgpt.com "Unit Testing - Cyclopts - Read the Docs"
[4]: https://cyclopts.readthedocs.io/en/stable/cookbook/dataclass_commands.html?utm_source=chatgpt.com "Dataclass Commands — cyclopts"
[5]: https://rich.readthedocs.io/en/latest/console.html "Console API — Rich 14.1.0 documentation"
[6]: https://opentelemetry.io/docs/languages/python/cookbook/?utm_source=chatgpt.com "Cookbook"

## Cyclopts Advanced (core increment) — exception taxonomy + stage mapping + telemetry-first wrapper

Primary refs: Cyclopts API v4.4.1 (exceptions + `parse_args`/`parse_known_args` + `result_action` semantics) and “App Calling & Return Values” (default exit/printing behavior). ([Cyclopts][1])

---

# 1) Exception taxonomy: hierarchy + payload surface

## 1.1 Root: `CycloptsError` (runtime/parse/coercion/validation class)

`CycloptsError` is the root exception for Cyclopts runtime errors; as it “bubbles up the Cyclopts call-stack, more information is added to it.” ([Cyclopts][1])

**Fields (structured payload you should harvest for telemetry):**

* `msg`: optional override of generated message ([Cyclopts][1])
* `verbose`: developer-oriented verbosity toggle ([Cyclopts][1])
* `root_input_tokens`: initial parsed CLI tokens fed into the `App` ([Cyclopts][1])
* `unused_tokens`: leftover tokens after parsing ([Cyclopts][1])
* `target`: python function associated with the parsed command ([Cyclopts][1])
* `argument`: matched `Argument` (binding-level object) ([Cyclopts][1])
* `command_chain`: command tokens leading to `target` ([Cyclopts][1])
* `app`: the `App` instance involved ([Cyclopts][1])
* `console`: Rich `Console` intended for error display ([Cyclopts][1])

> **Agent rule:** treat `CycloptsError` as the structured “error envelope.” The subclass tells you the *kind*; the base fields provide *context* (token provenance, target function, command chain).

## 1.2 Subclasses of `CycloptsError` (core parse/convert/validate failures)

### `UnknownCommandError : CycloptsError`

Raised when CLI token combination doesn’t map to a valid command. ([Cyclopts][1])

### `UnknownOptionError : CycloptsError`

Unknown/unregistered option token; docs note a nearest-neighbor suggestion *may* be printed. Fields:

* `token`: the unmatched `Token`
* `argument_collection`: plausible options for suggestion ([Cyclopts][1])

### `MissingArgumentError : CycloptsError`

Required argument not provided. Fields:

* `tokens_so_far`: partial tokens collected for multi-token args
* `keyword`: the keyword variant used (e.g. `-o` vs `--option`) ([Cyclopts][1])

### `RepeatArgumentError : CycloptsError`

Same parameter specified multiple times. Field:

* `token`: the repeated token ([Cyclopts][1])

### `MixedArgumentError : CycloptsError`

Cannot supply keywords and non-keywords to the same argument. ([Cyclopts][1])

### `CombinedShortOptionError : CycloptsError`

Cannot combine short, token-consuming options with short flags. ([Cyclopts][1])

### `UnusedCliTokensError : CycloptsError`

Not all CLI tokens were consumed as expected; `App.parse_args()` raises this if any tokens remain. ([Cyclopts][1])

### `CoercionError : CycloptsError`

Automatic type coercion failed. Fields:

* `token`: the input token that couldn’t be coerced (may be `None`)
* `target_type`: intended target type (may be `None`) ([Cyclopts][1])

### `ValidationError : CycloptsError`

Validator raised an exception. Fields:

* `exception_message`: message from parent `AssertionError`/`ValueError`/`TypeError` family
* `group`: group validator source (optional)
* `value`: converted value that failed validation ([Cyclopts][1])

And Cyclopts explicitly “re-interprets” validator-thrown `ValueError`, `TypeError`, `AssertionError` into prettier formatted messages. ([Cyclopts][2])

---

## 1.3 Non-`CycloptsError` exceptions you still must model

### `CommandCollisionError : Exception`

Raised if you register a command name that already exists in the app. **Registration-time** error, not parse-time. ([Cyclopts][1])

### Editor family (from `edit()`)

* `EditorError : Exception` (root)
* `EditorNotFoundError`, `EditorDidNotSaveError`, `EditorDidNotChangeError` ([Cyclopts][1])

### Config IO errors (built-in exceptions)

Cyclopts config sources can raise:

* `FileNotFoundError` if `must_exist=True` and file missing, or `search_parents=True` finds nothing. ([Cyclopts][1])
  And unknown keys in config can raise `UnknownOptionError` unless `allow_unknown=True`. ([Cyclopts][1])

---

# 2) “Which stage throws what”: pipeline → exception mapping

Cyclopts’ parse pipeline (for telemetry) is best treated as **phases**. Note config functions are executed “after parsing CLI tokens and environment variables” and “before any conversion and validation.” ([Cyclopts][1])

## Phase A — Registration (build-time, import-time)

* `CommandCollisionError` when registering duplicate command names. ([Cyclopts][1])

## Phase B — Command resolution (command-chain extraction)

* `UnknownCommandError` when the command chain doesn’t resolve. ([Cyclopts][1])

## Phase C — Option lexing / token structural constraints

* `CombinedShortOptionError` for invalid short-option combinations. ([Cyclopts][1])

## Phase D — Argument matching & binding (option ↔ parameter / positional binding)

* `UnknownOptionError` unmatched option token (including config-sourced unknown keys when `allow_unknown=False`). ([Cyclopts][1])
* `RepeatArgumentError` same parameter multiple times. ([Cyclopts][1])
* `MixedArgumentError` keywords and non-keywords mixed for same arg. ([Cyclopts][1])
* `MissingArgumentError` required param missing. ([Cyclopts][1])

## Phase E — Config injection (post-token / pre-convert)

* Any exception thrown by your config loader (user code)
* `FileNotFoundError` from config sources (`must_exist`, `search_parents`) ([Cyclopts][1])
* `UnknownOptionError` for unknown config keys if `allow_unknown=False` ([Cyclopts][1])

## Phase F — Coercion (token(s) → typed value)

* `CoercionError` when conversion fails. ([Cyclopts][1])

## Phase G — Validation (parameter/group/app validators)

* `ValidationError` when validator raises (including wrapped `ValueError`/`TypeError`/`AssertionError`). ([Cyclopts][1])

## Phase H — Strictness check (only for strict parse)

* `UnusedCliTokensError` if tokens remain after parse; this is exactly what `parse_args` enforces. ([Cyclopts][1])
  (Conversely, `parse_known_args` returns the `unused_tokens` list instead of raising.) ([Cyclopts][1])

## Phase I — Command execution (user function)

* arbitrary user exceptions (Cyclopts is “hands-off” for most exceptions; Cyclopts runtime errors default to formatted output + `sys.exit(1)` unless you disable it). ([Cyclopts][3])

## Phase J — Result handling (`result_action`)

Default is `"print_non_int_sys_exit"` and its semantics are spelled out in the API docs (bool→0/1, int→exit code, else print then exit 0). ([Cyclopts][1])
For telemetry-first wrappers, the key point is: **a result_action may call `sys.exit()`**; if you need guaranteed emission, run a non-exiting action that returns an int code (e.g. `"return_int_as_exit_code_else_zero"`). ([Cyclopts][1])

---

# 3) Default exit/print behavior (why wrappers must opt out)

By default, Cyclopts will `sys.exit(1)` for Cyclopts runtime errors like `CoercionError` / `ValidationError` to avoid uncaught ugly exceptions. This is controlled by `App.exit_on_error` and `App.print_error`. ([Cyclopts][3])

> **Telemetry rule:** for telemetry-first behavior, you almost always want `exit_on_error=False` and `print_error=False` during parse so you can (a) classify, (b) emit telemetry, then (c) choose your own exit strategy. ([Cyclopts][3])

---

# 4) Concrete telemetry-first wrapper (parse latency, exec latency, exit code, normalized error)

This wrapper:

* uses `parse_args` (strict) or `parse_known_args` (tolerant) ([Cyclopts][1])
* evaluates a Cyclopts-style `result_action` pipeline using the **documented per-mode pseudocode** ([Cyclopts][1])
* records:

  * parse latency (includes config injection + coercion + validation)
  * execution latency (user function)
  * exit code (including `SystemExit` interception)
  * normalized error class + stage

### 4.1 Code (drop-in utility)

```python
from __future__ import annotations

import inspect
import sys
import time
from dataclasses import dataclass, asdict
from typing import Any, Callable, Iterable, Literal, Optional, Sequence, Union

from cyclopts.exceptions import CycloptsError  # used in docs + stacktraces
from cyclopts import App  # type only; you pass your app in


# -----------------------------
# Telemetry event model
# -----------------------------

@dataclass(frozen=True)
class CliInvokeEvent:
    ok: bool
    command: Optional[str]
    parse_ms: float
    exec_ms: float
    exit_code: int

    error_class: Optional[str] = None      # e.g., "cyclopts.UnknownOptionError"
    error_stage: Optional[str] = None      # e.g., "binding", "coercion", "validation"
    error_message: Optional[str] = None

    root_tokens: Optional[list[str]] = None
    unused_tokens: Optional[list[str]] = None


# -----------------------------
# Result-action evaluation
# (mirrors Cyclopts documented pseudocode)
# -----------------------------

ResultActionLiteral = Literal[
    "return_value",
    "call_if_callable",
    "print_non_int_return_int_as_exit_code",
    "print_str_return_int_as_exit_code",
    "print_str_return_zero",
    "print_non_none_return_int_as_exit_code",
    "print_non_none_return_zero",
    "return_int_as_exit_code_else_zero",
    "print_non_int_sys_exit",
    "sys_exit",
    "return_none",
    "return_zero",
    "print_return_zero",
    "sys_exit_zero",
    "print_sys_exit_zero",
]
ResultAction = Union[ResultActionLiteral, Callable[[Any], Any], Sequence[Union[ResultActionLiteral, Callable[[Any], Any]]]]


def _as_exit_code(x: Any) -> int:
    # Matches Cyclopts' common bool/int conventions.
    if x is None:
        return 0
    if isinstance(x, bool):
        return 0 if x else 1
    if isinstance(x, int):
        return x
    return 0


def _apply_action(action: Union[ResultActionLiteral, Callable[[Any], Any]], result: Any) -> Any:
    if callable(action):
        return action(result)

    # Literal semantics derived from Cyclopts API pseudocode.
    if action == "return_value":
        return result

    if action == "call_if_callable":
        return result() if callable(result) else result

    if action == "print_non_int_return_int_as_exit_code":
        if isinstance(result, bool):
            return 0 if result else 1
        if isinstance(result, int):
            return result
        if result is not None:
            print(result)
        return 0

    if action == "print_str_return_int_as_exit_code":
        if isinstance(result, str):
            print(result)
            return 0
        if isinstance(result, bool):
            return 0 if result else 1
        if isinstance(result, int):
            return result
        return 0

    if action == "print_str_return_zero":
        if isinstance(result, str):
            print(result)
        return 0

    if action == "print_non_none_return_int_as_exit_code":
        if result is not None:
            print(result)
        if isinstance(result, bool):
            return 0 if result else 1
        if isinstance(result, int):
            return result
        return 0

    if action == "print_non_none_return_zero":
        if result is not None:
            print(result)
        return 0

    if action == "return_int_as_exit_code_else_zero":
        return _as_exit_code(result)

    if action == "return_none":
        return None

    if action == "return_zero":
        return 0

    if action == "print_return_zero":
        print(result)
        return 0

    if action == "sys_exit_zero":
        sys.exit(0)

    if action == "print_sys_exit_zero":
        print(result)
        sys.exit(0)

    if action == "sys_exit":
        sys.exit(_as_exit_code(result))

    if action == "print_non_int_sys_exit":
        # default Cyclopts CLI mode
        if isinstance(result, bool):
            sys.exit(0 if result else 1)
        if isinstance(result, int):
            sys.exit(result)
        if result is not None:
            print(result)
        sys.exit(0)

    raise ValueError(f"Unknown result_action literal: {action}")


def apply_result_action(result_action: ResultAction, result: Any) -> Any:
    if isinstance(result_action, (list, tuple)):
        if not result_action:
            raise ValueError("Empty result_action pipeline.")
        out = result
        for a in result_action:
            out = _apply_action(a, out)
        return out
    return _apply_action(result_action, result)


# -----------------------------
# Error normalization
# -----------------------------

def classify_error_stage(exc: BaseException) -> str:
    name = exc.__class__.__name__
    if name == "UnknownCommandError":
        return "command_resolve"
    if name == "UnknownOptionError":
        return "binding"
    if name in ("MissingArgumentError", "RepeatArgumentError", "MixedArgumentError"):
        return "binding"
    if name == "CombinedShortOptionError":
        return "lexing"
    if name == "UnusedCliTokensError":
        return "strictness"
    if name == "CoercionError":
        return "coercion"
    if name == "ValidationError":
        return "validation"
    if name.endswith("EditorError") or name.startswith("Editor"):
        return "editor"
    if isinstance(exc, FileNotFoundError):
        return "config_io"
    return "execution_or_unknown"


def normalize_error_class(exc: BaseException) -> str:
    # "cyclopts.X" for Cyclopts errors; otherwise module-qualified for user exceptions.
    if isinstance(exc, CycloptsError):
        return f"cyclopts.{exc.__class__.__name__}"
    return f"{exc.__class__.__module__}.{exc.__class__.__name__}"


# -----------------------------
# Telemetry-first invocation
# -----------------------------

def invoke_with_telemetry(
    app: App,
    tokens: Optional[Union[str, Iterable[str]]] = None,
    *,
    strict: bool = True,
    result_action: ResultAction = ("call_if_callable", "return_int_as_exit_code_else_zero"),
    # opt-out of cyclopts' default exit/print behavior for telemetry guarantees:
    parse_exit_on_error: bool = False,
    parse_print_error: bool = False,
    raise_exceptions: bool = False,
) -> tuple[int, CliInvokeEvent]:
    # Parse phase (includes coercion + validation; config injection happens after token/env parse and before convert/validate)
    t0 = time.perf_counter()
    command_name: Optional[str] = None
    unused_tokens: Optional[list[str]] = None

    try:
        if strict:
            command, bound, ignored = app.parse_args(
                tokens,
                exit_on_error=parse_exit_on_error,
                print_error=parse_print_error,
            )
        else:
            command, bound, unused_tokens, ignored = app.parse_known_args(tokens)

        parse_ms = (time.perf_counter() - t0) * 1000.0
        command_name = getattr(command, "__qualname__", repr(command))

        # Exec phase (user code)
        t1 = time.perf_counter()
        result = command(*bound.args, **bound.kwargs)
        if inspect.isawaitable(result):
            raise RuntimeError("Async command returned awaitable; use an async runner variant.")
        exec_ms = (time.perf_counter() - t1) * 1000.0

        # Result-action phase (may sys.exit -> SystemExit)
        try:
            ra_out = apply_result_action(result_action, result)
            exit_code = _as_exit_code(ra_out)
        except SystemExit as se:
            # normalize SystemExit.code the same way Python does (None->0; int->int; else->1)
            code = se.code
            exit_code = code if isinstance(code, int) else (0 if code is None else 1)
            raise  # re-raise to preserve behavior unless caller chooses otherwise

        evt = CliInvokeEvent(
            ok=True,
            command=command_name,
            parse_ms=parse_ms,
            exec_ms=exec_ms,
            exit_code=exit_code,
            unused_tokens=unused_tokens,
        )
        return exit_code, evt

    except BaseException as exc:
        parse_ms = (time.perf_counter() - t0) * 1000.0
        evt = CliInvokeEvent(
            ok=False,
            command=command_name,
            parse_ms=parse_ms,
            exec_ms=0.0,
            exit_code=1,
            error_class=normalize_error_class(exc),
            error_stage=classify_error_stage(exc),
            error_message=str(exc),
            root_tokens=getattr(exc, "root_input_tokens", None),
            unused_tokens=getattr(exc, "unused_tokens", unused_tokens),
        )
        if raise_exceptions:
            raise
        return 1, evt
```

**Why this matches Cyclopts core semantics (and stays stable):**

* `parse_args()` strictness (`UnusedCliTokensError` if remaining tokens) is Cyclopts-defined. ([Cyclopts][1])
* The `result_action` literal behaviors are implemented directly from Cyclopts’ own API pseudocode (including default `"print_non_int_sys_exit"`). ([Cyclopts][1])
* The wrapper opts out of Cyclopts’ default `sys.exit(1)` on runtime errors by passing `exit_on_error=False`, `print_error=False` during parsing, matching the documented behavior. ([Cyclopts][3])

---

# 5) Wiring to OpenTelemetry (spans + record_exception)

If you want spans for parse/exec (and structured exception events):

* OTel “cookbook” shows `trace.get_tracer(...)` + `with tracer.start_as_current_span(...)` and `span.set_attribute(...)`. ([OpenTelemetry][4])
* OTel Python `Span.record_exception(...)` exists and records an exception as a span event. ([opentelemetry-python.readthedocs.io][5])

### 5.1 Minimal integration sketch (sync)

```python
from opentelemetry import trace

tracer = trace.get_tracer("my.cli")

def run_cli(app, tokens):
    with tracer.start_as_current_span("cyclopts.invoke") as span:
        code, evt = invoke_with_telemetry(app, tokens, strict=True)

        span.set_attribute("cli.command", evt.command or "")
        span.set_attribute("cli.parse_ms", evt.parse_ms)
        span.set_attribute("cli.exec_ms", evt.exec_ms)
        span.set_attribute("cli.exit_code", evt.exit_code)
        span.set_attribute("cli.ok", evt.ok)

        if not evt.ok:
            span.set_attribute("cli.error_class", evt.error_class or "")
            span.set_attribute("cli.error_stage", evt.error_stage or "")
            # record_exception is standard OpenTelemetry Span API
            span.record_exception(Exception(evt.error_message or "cyclopts error"))

        return code
```

---

# 6) Practical “agent rules” for production-grade normalization

1. **Emit stage + class, not just message.**
   Messages can change with `verbose`; class names (`UnknownOptionError`, `CoercionError`, etc.) are the stable join keys. ([Cyclopts][1])

2. **Don’t lose CycloptsError envelope fields.**
   `root_input_tokens`, `unused_tokens`, `command_chain`, and `target` are precisely what you need for triage and reproducibility. ([Cyclopts][1])

3. **Treat config as part of parse.**
   Config hooks run after token/env parse and before convert/validate, so “parse latency” includes config IO and config-derived unknown-key errors. ([Cyclopts][1])

4. **Avoid sys.exit inside telemetry-critical paths.**
   Prefer non-exiting `result_action` modes returning exit codes (e.g., `"return_int_as_exit_code_else_zero"`), then exit in your outermost entry point after telemetry emission. ([Cyclopts][1])

---

If you want the next increment: a fully async-compatible version (`run_async` + awaiting coroutine commands), plus a “redaction policy layer” that strips secrets from `BoundArguments`/tokens before emitting telemetry (and a golden test matrix asserting each Cyclopts exception maps to the correct normalized stage + exit code).

[1]: https://cyclopts.readthedocs.io/en/v4.4.1/api.html "API — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/latest/parameters.html "Parameters — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/latest/app_calling.html "App Calling & Return Values — cyclopts"
[4]: https://opentelemetry.io/docs/languages/python/cookbook/?utm_source=chatgpt.com "Cookbook"
[5]: https://opentelemetry-python.readthedocs.io/en/latest/api/trace.html "opentelemetry.trace package — OpenTelemetry Python  documentation"

# 3) Command graph & dispatch (Cyclopts core)

Primary refs: **Commands** doc (registration, subcommands, flattening, naming, async) + **API v4.4.1** (exact overloads, defaults, backend semantics, name transform function). ([Cyclopts][1])

---

## 3.0 Mental model: commands are a recursive **App graph** (not “just functions”)

Cyclopts’ command tree is structurally **Apps all the way down**:

* Registering a command (function) creates a **sub-App node** whose handler lives at `sub_app.default_command`.
* Registering a subcommand (an `App`) attaches that `App` under the parent.
* The structure is fully recursive: any sub-App can itself have commands and/or its own `@default`. ([Cyclopts][1])

Two practical consequences for LLM agents:

1. **Dispatch = pick an App node via command tokens, then invoke its default_command** (or descend further). The “graph edge” is the command name token(s). ([Cyclopts][2])
2. **Config inheritance is App-level** (parent→child), and can be overridden per subcommand App or per call. ([Cyclopts][1])

---

## 3.1 Registering commands: `@app.command` / `app.command(...)` (overloads + semantics)

### 3.1.1 API surface (overloads; return types matter for composition)

`App.command` has three relevant overload shapes: ([Cyclopts][2])

```python
# Direct registration of a callable or App; returns the same object
command(obj: T, name: None|str|Iterable[str]=None, *, alias: None|str|Iterable[str]=None, **kwargs) -> T

# Decorator factory form
command(obj: None=None, name: None|str|Iterable[str]=None, *, alias: None|str|Iterable[str]=None, **kwargs) -> Callable[[T], T]

# Lazy registration via import-path string; returns None
command(obj: str, name: None|str|Iterable[str]=None, *, alias: None|str|Iterable[str]=None, **kwargs) -> None
```

Operational consequences:

* **Decorator form returns the original function unchanged** (Cyclopts does not wrap/replace it), so the decorated function remains directly callable and introspectable. ([Cyclopts][1])
* Passing an `App` returns that same `App`, enabling idioms like `child = root.command(App(name="child"))` as in docs. ([Cyclopts][1])
* Passing a lazy import path string registers a command without importing immediately; you can’t chain off the return value (it’s `None`). ([Cyclopts][2])

### 3.1.2 Default naming rule for commands (when `name=` is omitted)

When `name` isn’t provided, Cyclopts chooses it based on *what you registered*: ([Cyclopts][2])

* Registering an **App** → uses the app’s `name`
* Registering a **function** → uses `function.__name__` after applying `App.name_transform`
* Registering an **import path** → uses the attribute name after applying `App.name_transform`

Special case: `name="*"` means **flatten** (see §3.4). ([Cyclopts][2])

### 3.1.3 `**kwargs` when registering commands (sub-App configuration hook)

`command(..., **kwargs)` accepts **any argument that `App` can take** (help, error policy, grouping, etc.). Practically, Cyclopts can materialize an underlying sub-App node with those settings. ([Cyclopts][2])

> Agent rule: treat `@app.command(help=..., show=..., sort_key=..., ...)` as “configure the generated sub-App node,” not the raw function. ([Cyclopts][1])

---

## 3.2 Registering defaults: `@app.default` (root and sub-App entrypoints)

### 3.2.1 API surface (overloads + validator)

`App.default` has decorator / decorator-factory forms and can take a `validator`: ([Cyclopts][2])

```python
default(obj: T, *, validator: Callable[..., Any] | None = None) -> T
default(obj: None = None, *, validator: Callable[..., Any] | None = None) -> Callable[[T], T]
```

### 3.2.2 Behavioral rules

* `@app.default` registers the action when **no command token** is provided. ([Cyclopts][1])
* A **sub-App cannot be registered with `app.default`**. ([Cyclopts][1])
* If **no default** is registered, Cyclopts shows the help page. ([Cyclopts][1])
* Defaults are **per node**: subcommands may have their own default actions; the command graph is fully recursive. ([Cyclopts][1])

**Design pattern**

* Use defaults for “single-command CLIs” and for subcommand group entrypoints (e.g., `tool db` with no subcommand prints help or runs a safe read-only command).

---

## 3.3 Subcommands: Apps registered as commands (nesting + dispatch + inheritance)

### 3.3.1 Register a sub-App

Canonical pattern: create sub-App with a name, register it on parent, then register commands on the sub-App. ([Cyclopts][1])

```python
from cyclopts import App

app = App()
foo = App(name="foo")
app.command(foo)          # "foo" becomes a command on parent

@foo.command
def bar(n: int):
    ...
```

Alternative: treat `app` as a dict for subapps: ([Cyclopts][1])

```python
@app["foo"].command
def baz(n: int):
    ...
```

**Dispatch shape**

* CLI: `my-script foo bar 3` ⇒ select sub-App “foo”, then select command “bar” under it. ([Cyclopts][1])

### 3.3.2 Configuration inheritance: “subcommands inherit from parent Apps”

Cyclopts states directly: **subcommands inherit configuration from their parent apps**, and demonstrates inheritance for `exit_on_error`/`print_error`. ([Cyclopts][1])

Key inherited classes of settings (high-ROI for agents):

* **Error policy**: `exit_on_error`, `print_error`, `help_on_error`, `verbose` are inherited by child apps and overridable per call. ([Cyclopts][3])
* **Consoles**: output uses resolution order across call override → sub-App → parent chain → default. ([Cyclopts][2])
* **Async backend**: `App.backend` inherits from parent chain, defaulting to `"asyncio"` if unset. ([Cyclopts][2])

Practical engineering implication:

* Put your “global policy” (error behavior, output conventions, async backend) on the **root App**, and override only when needed on sub-App nodes.

---

## 3.4 Flattening subcommands: `name="*"` (collision rules + tradeoffs)

Flattening makes *commands from a sub-App* directly accessible from the parent, removing the intermediate command token in the CLI. ([Cyclopts][1])

### 3.4.1 Mechanism

```python
app.command(tools_app, name="*")
```

Only **App instances** can be flattened. ([Cyclopts][1])

### 3.4.2 Caveats (these are operationally important)

Cyclopts documents these explicitly: ([Cyclopts][1])

* **Collision precedence**: parent app commands take precedence over flattened commands if names collide.
* Multiple sub-apps can be flattened into the same parent.
* You **cannot supply additional configuration kwargs** when using `name="*"`.
* Only `App` instances can be flattened (not functions or import paths).

### 3.4.3 Tradeoffs: when flattening is worth it (and when it isn’t)

**Pros**

* User-facing CLI stays flat (`my-script compress ...`) while code remains modular (`tools_app` contains “tooling” commands). ([Cyclopts][1])

**Cons / constraints**

* **Namespace pressure** increases: collisions become more likely; parent wins, so flattened commands can become shadowed without error if you’re not careful. ([Cyclopts][1])
* Because `name="*"` forbids extra `**kwargs`, you can’t “wrap” the flattened import with extra config at registration time; you must configure the sub-App and/or each command where they are defined. ([Cyclopts][1])
* Flattening removes the “group token” from the CLI, so you lose a natural scope boundary for user mental model (this is a UX tradeoff; not a Cyclopts limitation).

**Agent recommendation**

* Prefer flattening for large CLIs where internal module organization matters but user-facing command depth must remain shallow.
* Enforce a naming convention or prefix strategy in flattened modules to prevent accidental shadowing.

---

## 3.5 Collision rules: registration-time collisions vs parse-time ambiguity

Two distinct collision/ambiguity classes:

### 3.5.1 Registration-time collision: `CommandCollisionError`

Cyclopts exposes `CommandCollisionError` (base `Exception`): “A command with the same name has already been registered to the app.” ([Cyclopts][2])

Use this mental model:

* **Explicit duplicate registration** should fail loudly (exception).
* **Flattening collisions** are a special case where “parent wins” can override flattened commands (shadowing risk). ([Cyclopts][1])

### 3.5.2 Meta command collisions (help/version tokens)

Help and version flags are treated as commands in iteration (e.g., `["--help", "-h", "--version", ...]`). This means some names are effectively reserved/occupied at the command namespace level. ([Cyclopts][2])

**Recent footgun note (version fuzzy-match bug)**
Cyclopts v4 had a bug where command `"version"` could be erroneously fuzzy-matched to `"--version"`; fixed in **v4.4.2** (Dec 27) per release notes. ([GitHub][4])
Practical guidance: avoid command names too close to meta flags, or customize `version_flags` if you need `version` as a real command in older pinned versions. ([GitHub][4])

---

## 3.6 Command naming system: `name_transform`, explicit names, aliases

### 3.6.1 Default transformation rules

Docs summary: by default, command names are derived from the Python function name with underscores replaced by hyphens and leading/trailing underscores stripped. ([Cyclopts][1])

The API defines the default transform more precisely (`default_name_transform`): ([Cyclopts][2])

1. PascalCase → snake_case
2. lowercase
3. `_` → `-`
4. strip leading/trailing `-` (and `_` via step 3)

### 3.6.2 `App.name_transform`: global control plane for derived names

Set at app construction:

```python
app = App(name_transform=lambda s: s)  # identity, preserves underscores
```

Then a function `foo_bar` registers as `foo_bar` (not `foo-bar`). ([Cyclopts][1])

### 3.6.3 Explicit `name=...`: bypass transform

Manually setting `name` in the decorator **overrides** derived naming and is **not subject to `App.name_transform`**. ([Cyclopts][1])

```python
@app.command(name="bar")
def foo(): ...
```

### 3.6.4 `alias=...`: additive names for backwards compatibility and UX

Aliases add additional names without overriding the derived one. ([Cyclopts][1])

```python
@app.command(alias="bar")
def foo():  # triggers on both "foo" and "bar"
    ...
```

Agent-use cases:

* Seamless rename migrations: keep old name as alias, new name as explicit `name`.
* Provide “short form” alias for long verbs (`deploy` alias `d`), but ensure it doesn’t collide with flags.

---

## 3.7 Async command support: event loop creation + backend selection + `run_async`

### 3.7.1 What Cyclopts does automatically

If an async command is encountered, Cyclopts automatically creates an event loop using the selected `backend` (default: `asyncio`). ([Cyclopts][1])

### 3.7.2 Backend resolution + installation constraints

`App.backend: Literal["asyncio","trio"] | None`:

* If unset, inherits from parent chain; otherwise defaults to `"asyncio"`. ([Cyclopts][2])
* Can be overridden per call: `app(backend="trio")`. ([Cyclopts][2])
* If using `backend="trio"`, docs note you must have the extra installed (e.g., `cyclopts[trio]`). ([Cyclopts][2])

### 3.7.3 Existing async context: must use `run_async`

Cyclopts explicitly instructs: when calling from within an existing async context, `await app.run_async([...])`; calling `app([...])` would raise `RuntimeError`. ([Cyclopts][1])

Agent guideline:

* **Synchronous CLI entrypoint**: call `app()` and let Cyclopts manage loop lifecycle.
* **Async host (FastAPI, notebooks, any running loop)**: call `await app.run_async(tokens)`.

---

## 3.8 “LLM agent” implementation checklist for command graph work

1. **Prefer explicit App nodes for modularity**
   Create `App(name="group")` per module; register on parent; optionally flatten. ([Cyclopts][1])

2. **Treat `@app.command(**kwargs)` as sub-App configuration**
   Use `help=`, `show=`, `sort_key=`, etc. on commands for help UX; root policy on root App. ([Cyclopts][2])

3. **Name safety**

* Use `name_transform` to enforce house style; use `name=` to pin stable CLI names; add `alias=` for migrations. ([Cyclopts][1])
* Avoid collisions with meta flags (`--help`, `--version`) and watch “close-name” pitfalls in pinned versions. ([Cyclopts][2])

4. **Async correctness**

* If async commands exist and you’re embedding Cyclopts into an async service, always use `run_async`. ([Cyclopts][1])

---

If you want the next increment, I’d move to **Section 4: Parameter model** but specifically from a command-graph perspective: how Cyclopts derives parameter namespaces for nested user classes, how `Parameter(parse=False)` interacts with meta-apps and `ignored`, and how command grouping interacts with help panels and sort order.

[1]: https://cyclopts.readthedocs.io/en/latest/commands.html "Commands — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/v4.4.1/api.html "API — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/latest/app_calling.html "App Calling & Return Values — cyclopts"
[4]: https://github.com/BrianPugh/cyclopts/releases "Releases · BrianPugh/cyclopts · GitHub"

# 4) Parameter model from a command-graph perspective (namespaces, `parse=False` / `ignored`, grouping + help ordering)

Key refs: User Classes (namespacing + flattening + sharing), Coercion Rules → User-Defined Classes (namespacing rules), Meta App (parse=False injection pattern), Groups (command/parameter panels), API v4.4.1 (exact semantics for `ignored`, `Parameter.parse`, `App.sort_key`, group ordering). ([Cyclopts][1])

---

## 4.0 Graph invariant: each command node owns its *own* argument tree (ArgumentCollection)

At dispatch time, Cyclopts selects a command node (an `App` subnode), then builds/uses an argument model derived from that node’s handler signature (plus group + default_parameter stack). Conceptually:

* **Command graph**: `root App` → `sub-App` → … → `leaf App` whose `default_command` is invoked.
* **Parameter graph** (per leaf command): root parameters + expanded children for user classes (dataclass-like, TypedDict, NamedTuple, etc.) show up as a *tree* whose leaf nodes correspond to concrete CLI tokens (`--x`, `--user.name`, etc.). The “namespace” problem is just deterministic naming of nodes in that tree. ([Cyclopts][1])

---

## 4.1 Namespace derivation for nested user classes (dotted hierarchy + override/flatten rules)

### 4.1.1 Default behavior: user-class parameters expand into dotted names

If a command has a structured parameter like `movie: Movie`, Cyclopts expands it into field parameters whose CLI names are prefixed with the parameter name:

* Help shows `--movie.title`, `--movie.year` (and required markers for required fields). ([Cyclopts][1])

This is your baseline rule:

> **Namespace = dotted path** `(<top-level param name> "." <field name> "." <nested-field name> ...)`

Cyclopts explicitly describes the names as “dotted-hierarchal” and uses `--player.years-young`, `--movie.title` as canonical examples. ([Cyclopts][2])

### 4.1.2 Per-node rename/override rules (how you *change* the dotted path)

Cyclopts gives two distinct “rename semantics” for a child field’s `Parameter(name=...)` inside a user class:

1. **Absolute override**: if the child name begins with `--`, it **completely overrides** the parenting parameter name (i.e., you break the dotted chain at that point). ([Cyclopts][2])
2. **Relative tack-on**: if the child name does *not* begin with `--`, it is appended under the parent (i.e., you keep the chain). ([Cyclopts][2])

The rules doc shows both in one example (field `name` becomes `--nickname` and field `age` becomes `--player.years-young` when parent was renamed to `player`). ([Cyclopts][2])

**Agent takeaway:** within nested types, you can decide *at each edge* whether a child stays under the parent namespace (relative tack-on) or “escapes” to the top level (absolute override).

### 4.1.3 Namespace flattening (the `name="*"` operator removes one namespace segment)

Cyclopts defines a special parameter name `"*"`:

* It “removes the immediate parameter’s name from the dotted-hierarchal name.” ([Cyclopts][2])
* In practice: `movie: Annotated[Movie, Parameter(name="*")]` turns `--movie.title` into `--title` and `--movie.year` into `--year`. ([Cyclopts][1])

Cyclopts supports applying this in two ways:

* **On the type annotation** (`Annotated[Movie, Parameter(name="*")]`). ([Cyclopts][1])
* **As a decorator on the class definition** (`@Parameter(name="*") @dataclass class Movie: ...`), which is terser and the configuration is inherited by subclasses. ([Cyclopts][1])

### 4.1.4 Deep nesting: how to reason about “nested user classes” (composition rule)

Cyclopts’ docs establish (a) dotted hierarchical naming, and (b) that `"*"` removes exactly one segment. The compositional inference for deep nesting is straightforward:

* A nested dataclass field under `movie` continues the dotted chain: `--movie.director.name` (conceptually).
* Applying `Parameter(name="*")` at different nodes removes different segments:

  * on `movie` removes the `movie` segment (so `--director.name`),
  * on `Director` removes `director` segment (so `--movie.name` for that subtree),
  * on both yields `--name`.

The “one segment removal” rule is explicit; the multi-level composition is the natural recursive application of it. ([Cyclopts][2])

### 4.1.5 Structured “escape hatch”: JSON dict/list parsing reduces namespace surface area

Namespace verbosity is often the reason you reach for JSON parsing:

* For dataclass-like parameters, Cyclopts will parse `--movie='{"title": "...", "year": 2024}'` when using the keyword option form `--movie` and certain conditions (e.g., not union’d with `str`, first char `{`). ([Cyclopts][1])
* Lists of dataclasses support JSON arrays (and mixed forms), configurable via `Parameter.json_list`. ([Cyclopts][1])

**Agent usage rule:** prefer JSON parsing when you want “single token → structured object” and keep the CLI namespace stable even as the class grows; prefer dotted fields when users want shell discoverability and per-field overrides.

---

## 4.2 `Parameter(parse=False)` and `ignored`: meta-apps + custom dispatch without signature pollution

### 4.2.1 The contract: `Parameter.parse` controls whether Cyclopts binds a parameter

In v4.4.1, `Parameter.parse` accepts:

* `True` parse normally,
* `False` do not parse → parameter appears in `ignored`,
* `None` default behavior,
* regex / compiled regex: parse only if name matches; otherwise skip (intended for app-wide patterns via `App.default_parameter`). ([Cyclopts][3])

Cyclopts explicitly states:

* annotated parameter must be **keyword-only or have a default value** (for parse control),
* and `parse=False` is intended for meta apps / injection. ([Cyclopts][3])

### 4.2.2 Where `ignored` comes from (parse API)

`App.parse_known_args()` returns `(command, bound, unused_tokens, ignored)` and `App.parse_args()` returns `(command, bound, ignored)`. In both cases:

* `ignored` is “a mapping of python-variable-name to annotated type of any parameter with annotation `parse=False` (Annotated resolved), intended to simplify meta apps.” ([Cyclopts][3])

This is a *deliberate* API affordance: it gives you a typed signal about which parameters the downstream command expects but Cyclopts did not bind.

### 4.2.3 Canonical meta-app injection pattern (from docs)

Cyclopts’ Meta App section shows the exact intended use case:

* downstream command declares a keyword-only parameter annotated with `Parameter(parse=False)`,
* launcher parses tokens for the downstream app,
* checks `ignored` for that param name,
* constructs the object and injects it into the final call. ([Cyclopts][4])

Key constraints explicitly noted:

* `parse=False` parameter must be **keyword-only**. ([Cyclopts][4])
* The meta launcher often uses `*tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)]` to collect remaining CLI tokens (including `-`-prefixed options) and forward them. ([Cyclopts][4])

**Agent-ready distilled template**

```python
from typing import Annotated
from cyclopts import App, Parameter

app = App()

class Session: ...

@app.command
def work(x: int, *, session: Annotated[Session, Parameter(parse=False)]):
    ...

@app.meta.default
def launcher(*tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)], user: str):
    command, bound, ignored = app.parse_args(tokens)  # strict by default
    extra = {}
    if "session" in ignored:
        extra["session"] = Session()  # build from meta params like user
    return command(*bound.args, **bound.kwargs, **extra)
```

This is exactly the mechanism Cyclopts documents; the only variability is your object construction logic. ([Cyclopts][4])

### 4.2.4 App-wide “skip private params” (regex parse patterns)

Instead of sprinkling `parse=False`, you can define an app-level rule:

* `app = App(default_parameter=Parameter(parse="^(?!_)"))` skips underscore-prefixed keyword-only parameters and makes them available in `ignored` (for injection). ([Cyclopts][3])

This pattern is called out as especially useful for meta apps. ([Cyclopts][4])

---

## 4.3 Groups across the command graph: help panels, default group routing, and ordering controls

Cyclopts “Groups” are the missing piece that ties **command graph structure** to **help UX**.

### 4.3.1 Group objects are metadata; commands/params reference them

* Groups organize **commands and parameters on the help page** into titled panels. ([Cyclopts][5])
* You can assign group(s) via:

  * `@app.command(group=...)` (command grouping),
  * `Parameter(group=...)` (parameter grouping). ([Cyclopts][5])
* Groups can be passed as:

  * explicit `Group(...)` instances, or
  * strings (implicitly creating `Group(name)` if not exists). Warning: typos create new groups. ([Cyclopts][5])
* Groups do **not** contain direct references to their members; they’re “just metadata,” so a group can be reused across commands. ([Cyclopts][5])

### 4.3.2 Default groups per App node (commands vs args vs params)

Cyclopts defines default groups on the registering app:

* `App.group_commands` (default `"Commands"`) for command listings. ([Cyclopts][5])
* `App.group_arguments` (default `"Arguments"`) for positional-only args. ([Cyclopts][5])
* `App.group_parameters` (default `"Parameters"`) for other parameters. ([Cyclopts][5])

For a command-graph, treat this as: **each App node defines the “routing table”** for where its parameters/commands appear in the help UI.

### 4.3.3 Command groups: grouping commands (including meta commands) into panels

Docs show:

* You can regroup meta commands like `--help` and `--version` into an “Admin” group by setting `app["--help"].group = "Admin"` and `app["--version"].group = "Admin"`. ([Cyclopts][5])
* Then `@app.command(group="Admin")` places your “info” command into that same panel. ([Cyclopts][5])

**Important nuance:** group validators are *not invoked* for command groups; validators apply to parameter groups only. ([Cyclopts][3])

### 4.3.4 Parameter groups: help panels + inter-parameter validators + hidden groups

Parameter groups:

* form help panels,
* can carry group validators (mutually exclusive, limited choice, etc.),
* can be hidden by giving empty name or `show=False` without impacting validation. ([Cyclopts][5])

For command-graph design: this is how you keep help structured even when multiple commands share “global” options and each command has its own local options.

### 4.3.5 Sorting: three independent layers (panel order, command order within panel, visibility)

#### (A) Panel order (Group panels)

Defaults + controls:

* Panels are alphabetical by default. ([Cyclopts][5])
* Override with `Group.sort_key`; `Group.create_ordered()` gives monotonically increasing sort_key to preserve definition order. ([Cyclopts][5])
* API nuance: the **default App groups** (`group_commands`, `group_arguments`, `group_parameters`) are displayed first; if you want to control their relative placement, replace them with custom `Group(..., sort_key=...)` and they’ll be treated like normal groups for sorting. ([Cyclopts][3])

#### (B) Command order within a commands panel (`App.sort_key`)

Each command is itself an App node, and `App.sort_key` modifies its display order in the help-page:

* `sort_key=None` commands sort alphabetically *after* non-None `sort_key` commands.
* `sort_key!=None` commands sort by `(sort_key, app.name)`.
* `sort_key` can be a generator (consumed via `next()`), or callable(s) invoked as `sort_key(app)`. ([Cyclopts][3])

This is the key mechanism for stable help ordering in large command graphs.

#### (C) Visibility (`show=False`)

* `App.show=False` hides a command from help but keeps it executable. ([Cyclopts][3])
* Groups with empty name or `show=False` do not render, but still exist for validation/organization. ([Cyclopts][5])

### 4.3.6 Meta-app specific: merging help + overriding default panels

Meta-app docs show a high-signal pattern:

* `app.meta` inherits config and its help-page is **merged** with the parent help.
* You can rename/reorder the meta app’s parameter panel by setting `app.meta.group_parameters = Group("Session Parameters", sort_key=0)`. ([Cyclopts][4])

This is the canonical “session/global parameters + normal commands” UX.

---

## 4.4 Agent implementation playbook (practical patterns)

### Pattern A — “shared config object” across many commands (namespace control)

Use a flattened dataclass (`@Parameter(name="*")`) as a keyword-only config parameter to avoid `--config.user` verbosity and to share across commands. ([Cyclopts][1])

### Pattern B — “inject expensive session object” (parse=False + meta app)

Declare `session: Annotated[Session, Parameter(parse=False)]` as keyword-only and inject it in a meta launcher by inspecting `ignored`. ([Cyclopts][4])

### Pattern C — Help UX at scale (groups + sort_key)

* Put “admin/meta” commands in a dedicated group.
* Use `App.sort_key` on critical commands to keep ordering stable.
* Use `Group.sort_key` / `Group.create_ordered()` to keep panel ordering stable and intentional. ([Cyclopts][5])

---

If you want the next increment, the natural continuation is **Section 4.5: Parameter resolution stack** (App.default_parameter → Group.default_parameter → Annotated Parameter → inference), including how group default parameters interact with per-field overrides and nested user class field metadata.

[1]: https://cyclopts.readthedocs.io/en/latest/user_classes.html "User Classes — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/latest/rules.html "Coercion Rules — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/v4.4.1/api.html "API — cyclopts"
[4]: https://cyclopts.readthedocs.io/en/latest/meta_app.html "Meta App — cyclopts"
[5]: https://cyclopts.readthedocs.io/en/latest/groups.html "Groups — cyclopts"

# 4.5 Parameter resolution stack (App → Group → Annotated → inference) + nested user-class metadata

This section is the “how Cyclopts decides *the effective Parameter config* for each argument node in the command’s ArgumentCollection” spec.

---

## 4.5.1 Two orthogonal precedence axes

Cyclopts resolves an argument’s effective `Parameter` config along **two** dimensions:

### A) **Source stack** (App / Group / signature, across the App call-chain)

Cyclopts defines an explicit resolution order for defaults (low → high) that is evaluated along the **parenting App call-chain** (root app → subapp → … → leaf app that registered the command). The documented order is:

1. **Highest**: function signature `Annotated[..., Parameter(...)]`
2. `Group.default_parameter` for the group(s) the parameter belongs to
3. `App.default_parameter` for the app that registered the command
4. `Group.default_parameter` of the registering app itself (i.e., the app’s default group for that param kind)
5. **Lowest**: repeat (2–4) recursively for parent apps in the chain ([Cyclopts][1])

### B) **Within-signature stacking** (`typing.Annotated` can contain multiple `Parameter(...)`)

Cyclopts can combine multiple `Parameter` annotations (because Python flattens nested `Annotated`). It searches **right-to-left** for each attribute: the *right-most* `Parameter(...)` wins for that attribute. ([Cyclopts][2])

> Think: **source stack decides which `Parameter` objects are in play**; **Annotated stacking decides which one wins per-field at the top of the stack**.

---

## 4.5.2 The merge primitive: `Parameter.combine()` (and why `Parameter.default()` exists)

Cyclopts exposes the same mental model in the API:

* `Parameter.combine(*parameters)` returns a new `Parameter` whose attributes are overridden in priority order; the args are “ordered from least-to-highest attribute priority.” ([Cyclopts][3])
* `Parameter.default()` creates a `Parameter` where **all Cyclopts-default values are recorded** and therefore override upstream values; it is explicitly *not the same* as `Parameter()` (which leaves many fields unset / `None`). ([Cyclopts][3])

**Practical implication (agent rule):**

* Use `Parameter()` when you want to set *a few* knobs and otherwise inherit.
* Use `Parameter.default()` when you want a “hard reset” node in the stack (force Cyclopts’ baseline values and block upstream defaults).

---

## 4.5.3 Source stack, made concrete (how Cyclopts builds the effective `Parameter`)

You can model Cyclopts’ effective-resolution algorithm as:

1. Start with an “unset” config (`Parameter()`), or a hard baseline (`Parameter.default()`) depending on how you want upstream to behave. ([Cyclopts][3])
2. Apply **parent app chain** defaults (lowest):

   * each ancestor app’s `Group.default_parameter` (for the relevant default group: `group_arguments` vs `group_parameters`) ([Cyclopts][1])
   * each ancestor app’s `App.default_parameter` ([Cyclopts][1])
3. Apply **registering app** defaults:

   * `App.default_parameter` ([Cyclopts][1])
   * `Group.default_parameter` for the app’s default group (`group_arguments` if positional-only; else `group_parameters`) ([Cyclopts][1])
4. Apply **explicit parameter groups’** defaults (higher than app defaults): `Group.default_parameter` for the group(s) the parameter belongs to. ([Cyclopts][1])
5. Apply **signature `Annotated` Parameters** (highest), using right-to-left per-field precedence if multiple. ([Cyclopts][1])

### The “Group.default_parameter sits *between* app defaults and signature” invariant

The API states this explicitly: `Group.default_parameter` is “between `App.default_parameter` and the function signature’s Annotated Parameter.” It also forbids setting `group` inside a `Group.default_parameter` (to avoid self-referential / cross-group weirdness). ([Cyclopts][3])

---

## 4.5.4 Group defaults vs per-field overrides (what actually happens)

### Group defaults: “bulk policy for a panel / validator set”

Groups commonly carry **behavioral defaults** that you don’t want to restate per parameter, e.g. disabling negative flags for all members in a mutually-exclusive group:

```python
vehicle = Group(
    "Vehicle (choose one)",
    default_parameter=Parameter(negative=""),   # disables --no-*
    validator=validators.MutuallyExclusive(),
)
```

This is the canonical pattern in the Groups docs. ([Cyclopts][4])

### Per-field overrides: signature wins

Because signature `Annotated[..., Parameter(...)]` is highest priority in the default stack, any explicit field override beats group/app defaults. ([Cyclopts][1])

Cyclopts also supports a “reset to true defaults” technique:

* **Any `Parameter` field can be set to `None`** to revert to Cyclopts’ original default for that field. ([Cyclopts][1])
* Or use `Parameter.default()` as a hard reset layer (overrides upstream defaults for *all* fields). ([Cyclopts][3])

---

## 4.5.5 Multiple groups: what you can rely on (and what to avoid)

Facts you can rely on:

* A parameter can belong to multiple groups (the `group` field accepts iterables). ([Cyclopts][3])
* Every parameter belongs to at least one group; if `Parameter.group` is `None`, Cyclopts assigns it to the app’s default groups: `App.group_arguments` for positional-only, otherwise `App.group_parameters`. ([Cyclopts][3])

What the docs **do not precisely specify**: the deterministic merge ordering if **multiple** groups each define `default_parameter` with conflicting attributes. Cyclopts must combine them somehow, but the public docs stop at “Group.default_parameter that the parameter belongs to.” ([Cyclopts][1])

**Agent-safe engineering rule:** treat group defaults as *orthogonal* (each group sets different knobs), or make conflicts impossible by putting the decisive override in the signature `Annotated Parameter(...)` (highest priority) ([Cyclopts][1]).

---

## 4.5.6 Nested user-class field metadata: how the same stack repeats for children

When Cyclopts expands a dataclass-like parameter into sub-arguments, **each leaf field** is still an “argument node” with its own `Parameter` config, resolved via the same mechanics:

### A) Naming + namespace decisions are `Parameter.name` decisions (parent + child interplay)

Cyclopts’ API shows the nested-name override rule:

* If you set a field name inside a nested structure beginning with `-` (e.g. `"--email"`), it **overrides hierarchical dot-notation**.
* If you set it without a leading hyphen (e.g. `"password"`), it remains in the parent namespace (`--user.password`). ([Cyclopts][3])

That’s why this works:

```python
@dataclass
class User:
    id: int                      # default => --user.id
    email: Annotated[str, Parameter(name="--email")]     # escapes => --email
    pwd: Annotated[str, Parameter(name="password")]      # stays => --user.password
```

…and the help output reflects exactly that. ([Cyclopts][3])

### B) Namespace flattening (`name="*"`) is just a Parameter layer

For user classes, Cyclopts documents `Parameter(name="*")` as the namespace-flattening operator: it removes the immediate parent segment (`--movie.title` → `--title`). ([Cyclopts][5])

You can apply it:

* on the *annotation* (`movie: Annotated[Movie, Parameter(name="*")]`) ([Cyclopts][5])
* or as a *class decorator* `@Parameter(name="*") @dataclass class Movie: ...`, which is inherited by subclasses. ([Cyclopts][5])

### C) “Are child fields keyword-addressable?” is controlled by `accepts_keys`

`Parameter.accepts_keys` controls whether a user-class behaves like a keyed structure (fields addressable by `--image.path`) or like a tuple (consume positional tokens, no `--image.path` addressing). ([Cyclopts][3])

This matters because it determines whether nested field Parameter metadata even comes into play for keyword parsing.

---

## 4.5.7 Inference: the bottom of the stack (what happens when fields remain unset)

After all explicit defaults + overrides, many `Parameter` fields may still be `None`—Cyclopts then infers behavior from signature/type/structure:

* `required`: inferred from the signature default (no default → required True; has default → required False). ([Cyclopts][3])
* `show`: defaults to whether the parameter is parsed (usually True). ([Cyclopts][3])
* `show_default`: defaults to “like True, but don’t show if the default is None” (and can be a formatter function). ([Cyclopts][3])
* `group`: inferred to `App.group_arguments` for positional-only, else `App.group_parameters`. ([Cyclopts][3])
* `negative`: has built-in default prefixes for bool/iterable negatives; setting it to an empty string/list disables negative flags. ([Cyclopts][3])
* `env_var`: if set, Cyclopts uses left-most defined env var, else falls back to function-signature default. ([Cyclopts][3])

This “inference layer” is why `App.default_parameter` and `Group.default_parameter` are so high leverage: they selectively override inference without requiring per-parameter verbosity.

---

## 4.5.8 Agent-ready pattern: “policy at root, UX policy per group, exceptions in signature”

A robust, scalable composition pattern that matches the stack:

1. **Root App.default_parameter** = global parsing policy (e.g., private injection parse regex, negative-none prefix, disable allow_leading_hyphen globally, etc.). ([Cyclopts][1])
2. **Group.default_parameter** = panel/validator semantics (e.g., “Mutually exclusive group” disables negative flags + hides defaults). ([Cyclopts][4])
3. **Signature Annotated Parameter** = local exception handling (one-off overrides; or `Parameter.default()` to hard-reset). ([Cyclopts][1])
4. **Nested user classes** = treat their field metadata as just “signature-level Parameter layers” applied at deeper nodes; use `--`-prefixed names when you need to escape dot namespaces. ([Cyclopts][3])


[1]: https://cyclopts.readthedocs.io/en/latest/default_parameter.html "Default Parameter — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/latest/parameters.html "Parameters — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/v4.4.1/api.html "API — cyclopts"
[4]: https://cyclopts.readthedocs.io/en/latest/groups.html "Groups — cyclopts"
[5]: https://cyclopts.readthedocs.io/en/v4.4.6/user_classes.html "User Classes — cyclopts"

# 5) Type system and coercion engine (Cyclopts)

This section is the “how tokens become typed Python objects” spec: effective type inference, token consumption, built-in coercions, and the knobs (`converter`, `n_tokens`, `consume_multiple`, `count`, `negative*`) that let you override the defaults.

---

## 5.0 Coercion pipeline and override points

### 5.0.1 Effective type hint resolution (“no hint” behavior)

Cyclopts computes an **effective type** for each parameter before converting tokens:

* **No annotation** ⇒

  * if the parameter has a **non-None default**, the effective type is `type(default)` (default-value-driven inference);
  * else the effective type is `str`.
* A standalone `Any` behaves the same as “no hint”. ([Cyclopts][1])

This is why a signature like `def f(x=5)` will coerce `x` as `int`, while `def f(x=None)` will treat `x` as `str` unless you annotate it.

### 5.0.2 Converter selection (built-in vs custom)

The coercion engine is *always* overrideable:

* If `Parameter.converter` is provided, it supersedes built-in coercion.
* Converters normally receive a **list of `Token`** objects (but can receive a **dict of keys→tokens** when dot-keyed input is used).
* Converter token arity is inferred from the type hint, but you can override consumption with `Parameter.n_tokens`. ([Cyclopts][1])

### 5.0.3 Token consumption inference (and when it surprises you)

Cyclopts infers how many CLI tokens a parameter consumes from its (effective) type hint (e.g., `int` → 1 token, `tuple[int,int]` → 2, `list[T]` → variable). You can override this with:

* `Parameter.n_tokens`: consume exactly N tokens, or `-1` for “consume all remaining”. This is crucial when your converter wants a different token shape than the hint would imply (e.g., loading a complex object from a **single path token**). ([Cyclopts][2])

---

## 5.1 Built-in coercions: scalar types

### `str`

No-op: CLI tokens are already strings. ([Cyclopts][1])

### `int`

Cyclopts’ `int` parsing is richer than `int(token)`:

* accepts decimal strings, and also accepts floating-point strings (rounded before casting),
* supports `0b` (binary), `0o` (octal), `0x` (hex). ([Cyclopts][1])

### `float` / `complex`

* `float(token)` and `complex(token)` respectively. ([Cyclopts][1])

### `bool`

Cyclopts has **three** bool modes depending on how the value is provided:

1. **Keyword flag**: `--my-flag` sets True; the default negative is `--no-my-flag` (configurable via `Parameter.negative`). ([Cyclopts][1])
2. **Positional**: strict token set; only `{"yes","y","1","true","t"}` → True and `{"no","n","0","false","f"}` → False (case-insensitive); otherwise a `CoercionError`. ([Cyclopts][1])
3. **Keyword with `=`**: `--my-flag=true` is parsed using the positional bool rules; note that `--no-my-flag=true` flips semantics (because it’s the negative variant). ([Cyclopts][1])

### `date` / `datetime` / `timedelta`

* `date`: uses `date.fromisoformat()`; primary format `%Y-%m-%d`, with broader ISO 8601 support on Python ≥ 3.11. ([Cyclopts][1])
* `datetime`: supports a defined set of timestamp formats (e.g., date-only, `T` separator, space separator, timezone, fractional seconds). ([Cyclopts][1])
* `timedelta`: supports compact duration strings like `30s`, `5m`, `2h`, `1d`, `3w`, plus approximate `6M` and `1y`; concatenation is supported (`1h30m`). ([Cyclopts][1])

---

## 5.2 Built-in coercions: containers and structured types

### `list[T]` (and friends)

Lists have distinct parsing semantics depending on positional vs keyword input.

#### Positional lists

* If `Parameter.allow_leading_hyphen=False` (default), parsing stops when an option-like token is encountered; if the consumed tokens don’t complete an element boundary (e.g., `list[tuple[int,str]]` needs 2 tokens per element), Cyclopts raises `MissingArgumentError`. ([Cyclopts][1])
* If `allow_leading_hyphen=True`, Cyclopts consumes tokens unconditionally (so option-looking tokens can become list elements). Since “known keyword args are parsed first”, use a bare `--` delimiter when you need to force ambiguous tokens to be treated positionally. ([Cyclopts][1])

#### Keyword lists (`--values ...`)

* Tokens are consumed until there’s enough data to build the hinted element type; the keyword can be repeated to append elements.
* If `allow_leading_hyphen=False`, encountering an option-like token before completing the element raises `MissingArgumentError`. ([Cyclopts][1])
* `Parameter.consume_multiple=True` changes keyword-list behavior to accept `--values 1 2 3` as “all remaining tokens” (subject to `allow_leading_hyphen`), instead of requiring repeats. ([Cyclopts][1])

#### Empty list flag (negative iterable)

Cyclopts supports “explicit empty list” via a generated negative flag: `--empty-<name>` (e.g., `--empty-extensions`). This is tied to the negative-flag system for iterables. ([Cyclopts][1])

#### `Iterable`, `Sequence`, `set`, `frozenset`

* `Iterable` and `Sequence` follow the same rules as `list` and yield a `list`.
* `set` follows list rules but yields a `set`; `frozenset` follows set rules and yields a `frozenset`. ([Cyclopts][1])

### `tuple[...]`

* Fixed-length tuples: inner hints are applied per element; enough tokens are consumed to populate the structure.
* Nested fixed-length tuples are allowed and consume the sum of their element tokens.
* Variable-length `tuple[T, ...]` is only supported at the root annotation level and behaves like list parsing. ([Cyclopts][1])

### `dict[K,V]`

Cyclopts populates dicts via **keyword dot-notation** (subkey parsing): `--mapping.key value`. Dicts can’t be populated positionally; docs recommend making dict parameters keyword-only. ([Cyclopts][1])

---

## 5.3 Built-in coercions: sum/choice types

### `Union[...]` and `Optional[...]`

* Union arms are attempted **left-to-right** until one coercion succeeds.
* `None` arms are ignored during the attempt loop (so `Union[None,int,str]` behaves as “try int then str”).
* `Optional[T]` is `Union[T, None]`. ([Cyclopts][1])

**Agent footgun:** union order defines what “wins” for ambiguous tokens (e.g., `Union[int,str]` will parse `"10"` as `int`, not `str`).

### `Literal[...]`

* Like `Union`, literal options are attempted **left-to-right**; Cyclopts coerces the token into the type of each literal candidate (so a literal `3` is tried as `int`, literal `"3"` as `str`). ([Cyclopts][1])

### `Enum`

Cyclopts Enum handling is name-centric:

* `Parameter.name_transform` is applied to both Enum member names and the CLI token, enabling case-insensitive matching and underscore↔hyphen normalization.
* Cyclopts matches CLI token to **Enum member name**, not value (explicitly contrasted with Typer’s value matching). ([Cyclopts][1])

### `Flag` / `IntFlag`

* Treated as a collection of boolean flags.
* Supports positional specification (`read write`) and dot-key form (`--permissions.write`).
* If you want `--read/--write` as direct booleans, the docs point you to namespace flattening. ([Cyclopts][1])

---

## 5.4 Flag semantics beyond `bool`: counting flags and negative flags

### 5.4.1 Negative flags (`Parameter.negative*`)

Cyclopts generates negative/empty variants as part of the type system:

* `Parameter.negative`: names for false boolean flags and empty iterable flags.

  * booleans default to `no-{name}` (via `negative_bool`, default prefix `"no-"`)
  * iterables default to `empty-{name}` (via `negative_iterable`, default prefix `"empty-"`)
* Set `negative` to `""` or `[]` to disable negative-flag generation.
* `negative_none` can be enabled (e.g., prefix `none-`) to generate flags that set optionals to `None` (e.g., `--none-path`). ([Cyclopts][2])

This is the mechanism behind `--no-flag` for `bool` and `--empty-items` for lists/sets.

### 5.4.2 Counting flags (`Parameter.count`)

Counting is an `int`-typed flag mode (classic verbosity pattern):

* `count=True` increments by 1 per occurrence.
* `-vvv` is equivalent to `-v -v -v`; long flags can be repeated (`--verbose --verbose` → 2).
* Negative variants are **not generated** for counted flags. ([Cyclopts][1])

---

## 5.5 Custom converters + token shaping (`converter`, `n_tokens`, `consume_multiple`, `allow_leading_hyphen`)

### `Parameter.converter` signature and token forms

Converter signature is:

```python
def converter(type_, tokens) -> Any: ...
```

Where `tokens` is usually:

* `list[Token]` (most common), or
* a `dict` of tokens when keys are specified via dot notation. ([Cyclopts][2])

### `Parameter.n_tokens`: override consumption to match your converter

`n_tokens` explicitly overrides inferred token count:

* `None`: infer from type hint
* non-negative int: consume exactly N tokens
* `-1`: consume all remaining tokens
  …and for `*args`, `n_tokens` means “tokens per element”. ([Cyclopts][2])

This is the core building block for patterns like “load a config object from one filepath token” even though the object has multiple fields. ([Cyclopts][2])

### `consume_multiple` (keyword iterable UX)

`consume_multiple=True` is the “single keyword, many values” UX (`--ext txt pdf`) and also defines “keyword present with no values => empty container”, equivalent to `--empty-ext`. ([Cyclopts][2])

### `allow_leading_hyphen`

Disabled by default to allow better “unknown option” errors; enable it to treat `-something` as a value (useful for paths like `-` or negative numbers in some contexts). ([Cyclopts][2])

---

## 5.6 Library-provided types (`cyclopts.types`): bounded ints, path contracts, IO helpers

Cyclopts ships a “typed convenience layer” implemented primarily as `typing.Annotated[...]` aliases combining a base type with validators and help formatting. ([Cyclopts][2])

### 5.6.1 Bounded ints (e.g., `UInt16`) + hex-default display

Examples from the `Number` type family:

* `UInt16` is `Annotated[int, Parameter(validator=(Number(... gte=0, lte=65535),))]`
* `HexUInt16` adds `Parameter(show_default=...)` so defaults display in hex. ([Cyclopts][2])

Also included: `UInt8/Int8`, `UInt32/Int32`, `UInt64/Int64`, plus sign/positivity constrained types like `PositiveInt`, `NonNegativeFloat`, etc. The docs note these “Number” types also apply to sequences of numbers. ([Cyclopts][2])

### 5.6.2 Path “contracts” (existence, type, resolution, extensions)

The `types.Path` family includes:

* existence contracts (Existing / NonExistent),
* file vs directory constraints,
* resolved variants,
* extension-specific types (`ExistingJsonPath`, `ExistingTomlPath`, etc.). ([Cyclopts][2])

### 5.6.3 `StdioPath`: Unix “`-` means stdin/stdout” IO helper

Cyclopts recommends `StdioPath` (a `Path` subclass) that treats `-` as stdin for reading or stdout for writing, following common Unix convention; it requires Python 3.12+ and is preconfigured with `allow_leading_hyphen=True` so `-` isn’t treated as an option. ([Cyclopts][3])

It supports:

* defaulting to stdin/stdout via `StdioPath("-")` default values,
* binary `read_bytes` / `write_bytes` and context-manager usage,
* an alternative `Optional[Path]` helper approach for Python < 3.12. ([Cyclopts][3])

---

## 5.7 Agent-grade “gotchas” and design rules

* **Union ordering is semantics**, not documentation: put the most specific/desired arm first. ([Cyclopts][1])
* **`int` rounding-from-float** is allowed; if you need strict integer tokens only, use a custom converter/validator. ([Cyclopts][1])
* **`allow_leading_hyphen=True` on positional iterables can swallow options**; use `--` delimiter when ambiguity matters. ([Cyclopts][1])
* **Empty containers are modeled as negative flags** (`--empty-x`) and as “keyword present with no values” when `consume_multiple=True`. ([Cyclopts][1])
* **Enums match names, not values**; normalize naming with `name_transform` for stable CLIs. ([Cyclopts][1])
* **Pydantic user classes**: Cyclopts defers conversion to pydantic’s engine (Cyclopts won’t do internal conversions for those classes). ([Cyclopts][1])

If you want the next increment, the natural continuation is: **custom coercion patterns at scale**—a catalog of “converter + n_tokens” idioms (file-backed config objects, lookups by ID, size-suffixed quantities, JSON-in-single-token vs dotted subkeys), and a golden-test matrix ensuring token consumption + error messages remain stable across versions.

[1]: https://cyclopts.readthedocs.io/en/latest/rules.html "Coercion Rules — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/v4.5.0/api.html "API — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/latest/cookbook/file_or_stdin_stdout.html "Reading/Writing From File or Stdin/Stdout — cyclopts"

# 5.8 Custom coercion patterns at scale (converter + `n_tokens` idioms) + golden-test matrix

This section is a practical “playbook” for when Cyclopts’ internal coercion is *almost* right, but you need **domain coercions** with **stable token consumption** and **stable error UX**.

---

## 5.8.1 Core primitives you compose

### Converter contract (tokens in, typed value out)

A `Parameter.converter` is invoked with `(type_, tokens)` and typically reads `Token.value` (string). Cyclopts’ docs show the canonical “size suffix” converter shape (`tokens[0].value`, parse → int) and attaching it with `Parameter(converter=...)`. ([Cyclopts][1])

### Tokens can be **list** or **dict** (dotted keys)

If keys are specified in the CLI (dot-notation like `--foo.key=val`), Cyclopts can pass the converter a **dict** keyed by the subkey, rather than a flat list. The API shows the dict form explicitly (e.g., `{"key1": ["val1"]}`). ([Cyclopts][2])

### `n_tokens`: hard-control token arity (the scaling lever)

Cyclopts documents `n_tokens` as the way to override inferred token count, especially for:

* custom converters needing a different token count than the type suggests,
* loading complex types from a single token (file path),
* selection/lookup patterns where one token identifies an object. ([Cyclopts][2])

### `accepts_keys`: decide whether dot-keys are even allowed

When you want “single token identifies the whole object,” combine `n_tokens=1` with `accepts_keys=False` to prevent `--obj.field=...` style input from entering your converter as keyed dicts (and to keep the interface stable). Cyclopts calls out `accepts_keys` as a useful companion to `n_tokens` for these patterns. ([Cyclopts][2])

### Reusable converter “decorators”

Cyclopts supports decorating converters with `@Parameter(...)` to package reusable conversion behavior (e.g., enforce `n_tokens=1`, `accepts_keys=False` on the converter itself). ([Cyclopts][2])

### Pydantic note (important “why didn’t my converter run?”)

If the annotated type is a pydantic model, Cyclopts disables its internal coercion engine “including this converter argument” and leaves coercion to pydantic. ([Cyclopts][2])

---

## 5.8.2 Idiom catalog: converter + `n_tokens` patterns you can standardize

### Idiom A — File-backed config object (single token → complex object)

**Use case:** `--config prod.toml` loads a structured config (host/port/credentials), but the CLI surface is one token.

**Mechanics:** `n_tokens=1`, `accepts_keys=False`, converter loads file.

Cyclopts’ API includes this exact pattern as a canonical `n_tokens` use: load complex types from a single token (e.g., file path). ([Cyclopts][2])

```python
from dataclasses import dataclass
from typing import Annotated
from cyclopts import App, Parameter, Token

@dataclass
class Config:
    host: str
    port: int

def load_config(type_, tokens: list[Token]) -> Config:
    path = tokens[0].value
    # parse toml/json/yaml here...
    return Config(host="example.com", port=8080)

app = App()

@app.default
def main(config: Annotated[Config, Parameter(n_tokens=1, accepts_keys=False, converter=load_config)]):
    ...
```

**Stability notes**

* Lock down keys: `accepts_keys=False` prevents `--config.host ...` from becoming a “second interface” you later have to support.
* Unit tests should assert: 1 token consumed, no unused tokens, and the config object fields correct.

---

### Idiom B — Registry / DB lookup by ID (single token → domain object)

**Use case:** `--user 123` loads a `User` from a cache/DB; this is explicitly cited as an `n_tokens` motivation (“selection/lookup patterns”). ([Cyclopts][2])

Cyclopts even shows the packaged form: decorate the converter with `@Parameter(n_tokens=1, accepts_keys=False)` for reuse across multiple commands. ([Cyclopts][2])

```python
from cyclopts import Parameter, Token

@Parameter(n_tokens=1, accepts_keys=False)
def load_from_id(type_, tokens: list[Token]):
    return fetch_from_db(tokens[0].value)

# usage:
# obj: Annotated[MyType, Parameter(converter=load_from_id)]
```

**Scaling variant (batch):** accept multiple IDs with `list[MyType]` and a converter that consumes 1 token per element; keep the converter itself “single token” and let list parsing handle repetition.

---

### Idiom C — Composite key lookup (fixed multi-token object)

**Use case:** `--artifact repo sha` or `--coord 40.7 -74.0` should consume **exactly N tokens**.

Cyclopts’ API shows a multi-token converter pattern where the converter asserts the `type_` and consumes a fixed token list. ([Cyclopts][2])

```python
from typing import Annotated
from cyclopts import App, Parameter, Token

def coord(type_, tokens: list[Token]):
    assert type_ == tuple[int, int]
    return tuple(2 * int(t.value) for t in tokens)  # example

@app.default
def main(coordinates: Annotated[tuple[int, int], Parameter(converter=coord)]):
    ...
```

**Hardening tip:** set `n_tokens=2` explicitly for composite keys to prevent future type-hint edits from changing token consumption.

---

### Idiom D — Size-suffixed quantities (“1kb”, “3mb”) (single token, custom parsing)

Cyclopts’ Parameters docs include the canonical size-suffix converter (token parsing + suffix mapping + `Parameter(converter=...)`). ([Cyclopts][1])

```python
from cyclopts import Parameter, Token
from typing import Annotated, Sequence

def byte_units(type_, tokens: Sequence[Token]) -> int:
    value = tokens[0].value.lower()
    # try pure int, else parse suffix…
    ...

def cmd(size: Annotated[int, Parameter(converter=byte_units)]):
    ...
```

**Scale it into a “Quantity” library**

* Standardize units (SI vs IEC), rounding rules, and error messages.
* Provide multiple target types: bytes as `int`, bandwidth as `float`, etc.

---

### Idiom E — “JSON-in-one-token” vs “dotted subkeys” for user classes (two interfaces; decide deliberately)

Cyclopts supports *both* styles for dataclass-like parameters:

1. **Dotted subkeys**: `--movie.title ... --movie.year ...` (always works; discoverable). ([Cyclopts][3])
2. **JSON dict in a single token**: `--movie '{"title": "...", "year": 2015}'`, but only when:

   * parameter specified as keyword option (`--movie`),
   * type is dataclass-like (has sub-arguments),
   * type is **not union’d with `str`**,
   * token starts with `{`. ([Cyclopts][3])

Also: Cyclopts explicitly notes JSON parsing “only works when using the keyword option format (`--movie`)”; dotted fields remain available. ([Cyclopts][3])

**Operational decision**

* If you want *one* stable interface: either

  * disable JSON parsing with `Parameter(json_dict=False)` and require dotted fields, or
  * enforce JSON-only by setting `accepts_keys=False` and giving the dataclass parameter a converter that parses JSON, so dotted keys become invalid by design.

**Version sensitivity**
Cyclopts has discussed changing the “avoid strings” rule (JSON parsing disabled when union’d with `str`) as a breaking-change candidate. That’s exactly the kind of behavior your golden tests should pin. ([GitHub][4])

---

### Idiom F — JSON list parsing for lists of dataclasses (single token array, repeated objects, or mixed)

Cyclopts’ User Classes docs show JSON list parsing for `list[Dataclass]`, including:

* one JSON array token,
* repeated JSON-object tokens,
* mixed mode; and it is automatically enabled with constraints (not union’d with `str`, JSON starts with `{` or `[`). ([Cyclopts][3])

This is “custom coercion without custom code” — but still belongs in your matrix because it is token-shape-sensitive and version-sensitive (controlled by `Parameter.json_list`). ([Cyclopts][3])

---

### Idiom G — Keyed dict converters (dot-keys → dict-of-lists tokens)

**Use case:** `--labels.env=prod --labels.team=core` should build a dict, maybe with custom normalization (lowercasing keys, splitting CSV values, enforcing allowlist).

Cyclopts’ API explicitly documents that when keys are specified, converters can receive a dict mapping keys to token lists. ([Cyclopts][2])

```python
def labels(type_, tokens_by_key: dict[str, list[str]]):
    # normalize keys/values, validate, return dict[str,str] or dict[str,list[str]]
    ...
```

**Hardening tip:** explicitly set `accepts_keys=True` for this parameter so future refactors don’t accidentally disable keyed parsing.

---

### Idiom H — “Promote converter to classmethod” (type owns its parse)

Cyclopts supports classmethod converters and allows referencing converters by string in `@Parameter(converter="from_env")` on the class. The API shows the expected signature `(cls, tokens)` and how to decorate the classmethod with its own `@Parameter(n_tokens=1, accepts_keys=False)`. ([Cyclopts][2])

This is ideal when you want the type to be “CLI-parsable” across multiple apps.

---

## 5.8.3 Golden-test matrix: pin token consumption + error UX across versions

You want two complementary layers:

### Layer 1 — Semantic tests (stable across formatting)

Assert:

* parsed value correctness,
* token consumption correctness (`parse_args` vs `parse_known_args`),
* exception class (CycloptsError subclass) and key structured fields.

Cyclopts provides `parse_args()` (strict; errors on unused tokens) and `parse_known_args()` (returns `unused_tokens`). ([Cyclopts][2])

**Matrix axes (minimum)**

* **Input style**: positional / keyword / dotted keys / JSON dict / JSON list
* **Token boundary**: fixed `n_tokens` (1,2,N) / inferred / `consume_multiple`
* **Option ambiguity**: leading hyphens on/off (paths like `-`)
* **Failure mode**: missing tokens, invalid unit, invalid JSON, unknown ID, invalid key
* **Version**: at least `lowest_supported`, `current_pinned`, `latest` (in CI)

### Layer 2 — Rendered output snapshots (Rich stabilization + golden strings)

Cyclopts uses Rich for pretty printing; Rich output varies with console capabilities, so Cyclopts recommends forcing deterministic console settings in tests (width, force_terminal, color_system=None, etc.). ([Cyclopts][5])

Cyclopts’ unit testing cookbook also notes you can capture Rich output via `capsys` (help printed to stdout) and mentions `Console.capture()` as another capture strategy. ([Cyclopts][5])

#### Recommended snapshot strategy

* For *errors* you care about, run in “CLI-like” mode (print_error=True, exit_on_error=False) and capture `error_console` output into a golden file.
* For “semantic stability,” assert exception type and structured fields even if formatting changes.

---

## 5.8.4 Concrete pytest scaffolding (drop-in patterns)

### Deterministic console fixture (from Cyclopts docs)

```python
import pytest
from rich.console import Console

@pytest.fixture
def console():
    return Console(
        width=70,
        force_terminal=True,
        highlight=False,
        color_system=None,
        legacy_windows=False,
    )
```

This is the exact pattern Cyclopts recommends to make output comparable against known-good strings. ([Cyclopts][5])

### Matrix template: consumption + errors

```python
import pytest

CASES = [
  # name, tokens, strict, expect_value, expect_unused, expect_exc
  ("config_file_ok", ["--config", "prod.toml"], True,  ("example.com", 8080), None, None),
  ("config_file_unused", ["--config", "prod.toml", "EXTRA"], False, ("example.com", 8080), ["EXTRA"], None),
  ("size_ok", ["--size", "3mb"], True, 3145728, None, None),
  ("size_bad_unit", ["--size", "3zz"], True, None, None, ValueError),  # or Cyclopts CoercionError depending on converter
  ("movie_json_ok", ["add", "--movie", '{"title":"Dune","year":2021}'], True, ("Dune", 2021), None, None),
  ("movie_dotted_ok", ["add", "--movie.title", "Dune", "--movie.year", "2021"], True, ("Dune", 2021), None, None),
]

@pytest.mark.parametrize("name,tokens,strict,expect,expect_unused,expect_exc", CASES)
def test_matrix(app, name, tokens, strict, expect, expect_unused, expect_exc, console, capsys):
    if expect_exc:
        with pytest.raises(expect_exc):
            if strict:
                app.parse_args(tokens, exit_on_error=False, print_error=False)
            else:
                app.parse_known_args(tokens)
        return

    if strict:
        cmd, bound, ignored = app.parse_args(tokens, exit_on_error=False, print_error=False)
        assert expect_unused is None
    else:
        cmd, bound, unused, ignored = app.parse_known_args(tokens)
        assert unused == expect_unused

    # assert on bound.arguments for your case…
```

### Snapshot error output (golden strings)

Use Cyclopts’ “deterministic console” fixture and pass it via `error_console=...`. Cyclopts shows `console=` being passed for predictable help output and capturing via `capsys`. ([Cyclopts][5])

---

## 5.8.5 What to pin vs what to allow to drift

**Pin hard**

* token consumption (unused tokens, strict failures),
* converter arity (`n_tokens`) and key acceptance (`accepts_keys`),
* JSON dict/list parsing enable/disable rules (notably union-with-str behavior). ([Cyclopts][3])

**Allow soft drift**

* exact Rich box drawing / spacing (unless your CLI is itself a UX product),
* verbose diagnostic text (toggleable), if you’re asserting the structured exception class instead.

---

If you want the next increment, we can turn this into a “house style” module: `coercions/` containing reusable decorated converters (`@Parameter(...)`) plus a ready-made pytest matrix generator that introspects command signatures and auto-emits “token-shape tests” for every parameter that declares `converter` or `n_tokens`.

[1]: https://cyclopts.readthedocs.io/en/latest/parameters.html "Parameters — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/v4.4.1/api.html "API — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/stable/user_classes.html "User Classes — cyclopts"
[4]: https://github.com/BrianPugh/cyclopts/issues/324?utm_source=chatgpt.com "v4 Breaking Change Wishlist · Issue #324"
[5]: https://cyclopts.readthedocs.io/en/latest/cookbook/unit_testing.html "Unit Testing — cyclopts"

# 6) User-defined / structured types (“user classes”) — parsing, JSON, namespacing, dataclass/attrs/pydantic interop

This section is the “how Cyclopts turns *one* typed parameter into a *tree* of sub-arguments” spec.

Cyclopts supports “classically defined user classes” plus dataclass-like classes from **attrs**, **dataclasses**, **pydantic**, **NamedTuple**, and **TypedDict**. ([Cyclopts][1])

---

## 6.1 Mental model: *subkey parsing* + “argument tree expansion”

### 6.1.1 Subkey parsing is the default UX for user classes

When a command parameter is a user class, Cyclopts exports **dot-notation subkeys** so users can set fields by keyword:

* `--user.name`, `--user.age`, `--user.region` etc. ([Cyclopts][2])

Cyclopts explicitly frames this as: “Subkey parsing allows for assigning values positionally and by keyword with a dot-separator.” ([Cyclopts][2])

### 6.1.2 Positionals still work (Python-call binding, but now across the class field order)

For a dataclass-like `User(name, age, region="us")`, Cyclopts supports:

* positional binding: `my-program 'Bob Smith' 30`
* keyword binding: `my-program --user.name 'Bob Smith' --user.age 30`
* mixed: `my-program --user.name 'Bob Smith' 30 --user.region=ca` ([Cyclopts][2])

**Agent framing:** treat a user-class parameter as an *implicit nested signature* that can be satisfied by a mixture of positional and keyed assignments, subject to the same “enough tokens to populate required fields” constraint.

---

## 6.2 How Cyclopts discovers + configures sub-arguments (recursive `Parameter` semantics)

### 6.2.1 Recursive `Parameter` annotations are honored on fields

Cyclopts “recursively search[es] for `Parameter` annotations and respect[s] them.” ([Cyclopts][2])

Two critical naming semantics for nested fields (these define your namespace contract):

* If a field’s `Parameter(name=...)` **begins with `--`**, it **completely overrides** the parent namespace (escapes dot hierarchy).
* If it **does not** begin with `--`, it is **tacked on** under the parent namespace. ([Cyclopts][2])

This is the mechanism behind patterns like:

* `name: Annotated[str, Parameter(name="--nickname")]` → `--nickname` (top-level)
* `age: Annotated[int, Parameter(name="years-young")]` with parent `user: Annotated[User, Parameter(name="player")]` → `--player.years-young` ([Cyclopts][2])

### 6.2.2 Help text source of truth (class docstrings vs command docstring)

Cyclopts uses **docstrings from the class** for help, but **command docstrings override class docstrings** when both are provided for the same field path (e.g., `user.age`). ([Cyclopts][2])

**Agent rule:** if you generate docs automatically, always treat the command docstring as the “final patch layer” over class field docstrings.

---

## 6.3 JSON dict parsing for user classes (`Parameter.json_dict`)

### 6.3.1 Activation conditions

Cyclopts will parse a **JSON object string** into a dataclass-like parameter when all of the following are true:

1. The parameter is provided as a **keyword option** (e.g., `--movie`)
2. The referenced parameter type is **dataclass-like** (has sub-arguments)
3. The parameter type is **not union’d with `str`**
4. The first character of the token is `{` ([Cyclopts][1])

This behavior is controlled by `Parameter.json_dict: bool | None`:

* `None` (default) behaves like **True** except when the annotated type is union’d with `str`. ([Cyclopts][3])

### 6.3.2 Important UX constraint: JSON dict parsing is keyword-only

Cyclopts notes explicitly: JSON parsing “only works when using the keyword option format (`--movie`)”; dotted fields still work normally (`--movie.title`, etc.). ([Cyclopts][1])

**Agent decision point:** you’re effectively choosing whether your structured type has *two* input syntaxes (JSON dict and dotted keys). If you want a single stable interface:

* disable JSON dict parsing (set `json_dict=False`), or
* disable subkeys (see `accepts_keys=False` below) and force JSON-only with a custom converter.

---

## 6.4 JSON list parsing for lists of user classes (`Parameter.json_list`)

Cyclopts supports JSON parsing for `list[DataclassLike]` and documents **three** user-facing syntaxes:

1. **JSON array token** containing multiple objects:
   `--movies '[{"title": "...", "year": ...}, {"title": "...", ...}]'`
2. **Repeated JSON objects**, one per flag occurrence:
   `--movies '{"title": "..."}' --movies '{"title": "..."}'`
3. **Mixed** array + individual objects. ([Cyclopts][1])

The docs also state:

* JSON list parsing is “automatically enabled for `list` types containing dataclasses.”
* The element type cannot be union’d with `str`.
* JSON objects must start with `{` or be arrays starting with `[`. ([Cyclopts][1])

`Parameter.json_list: bool | None` exists as the control surface. The API description emphasizes list-string parsing (token starts with `[`) and describes `None` default behavior as “acts like True” with element-type caveats. ([Cyclopts][3])

**Agent interpretation for implementation/testing:** Cyclopts’ list-of-dataclass JSON behavior is *token-shape sensitive* (object token vs array token), so pin it with golden tests if you rely on mixed mode.

---

## 6.5 Namespace flattening (`Parameter(name="*")`) — “remove one dot segment”

### 6.5.1 Flatten a single parameter instance (annotation-level)

Applying the special name `"*"` to a user-class parameter removes the **immediate parent segment**:

* `movie: Annotated[Movie, Parameter(name="*")]` turns `--movie.title` / `--movie.year` into `--title` / `--year`. ([Cyclopts][1])

Cyclopts calls out that `"*"` “remove[s] the immediate parameter’s name from the dotted-hierarchal name.” ([Cyclopts][2])

### 6.5.2 Flatten at the type definition (decorator-level; inherited by subclasses)

You can also decorate the class itself: `@Parameter(name="*") @dataclass class Movie: ...`. Cyclopts notes this is often cleaner and that the configuration is inherited by subclasses. ([Cyclopts][1])

### 6.5.3 “Flattening as architecture”: shared config objects across commands

Cyclopts documents a pattern where a flattened dataclass-like `Config` is injected into multiple commands as `*, config: Config`, giving top-level options like `--user`/`--server` rather than `--config.user`/`--config.server`. ([Cyclopts][1])

---

## 6.6 Turning off subkeys: `Parameter(accepts_keys=False)` (“opaque struct” mode)

If a user-class parameter is annotated with `Parameter(accepts_keys=False)`:

* Cyclopts exports **no dot-notation subkeys**; the CLI shows a single `--user` parameter.
* The parameter consumes enough positional tokens to populate the required positional arguments. ([Cyclopts][2])

This mode is explicitly demonstrated with a failure case:

* If the class needs 2 args but you provide 1, Cyclopts errors: `Parameter "--user" requires 2 arguments. Only got 1.` ([Cyclopts][2])

Cyclopts also notes the tradeoff: you may be unable to set optional fields (e.g., `region`) from the CLI in this mode. ([Cyclopts][2])

**Agent rule:** `accepts_keys=False` is how you enforce “single input form” (positional-only or converter-only) for a class parameter, at the cost of field-level override UX.

---

## 6.7 Interop expectations: dataclasses vs attrs vs pydantic (what Cyclopts converts, who validates)

### 6.7.1 Dataclasses / attrs / NamedTuple / TypedDict: Cyclopts does the coercion, then instantiates

Cyclopts treats these as “dataclass-like” for sub-argument expansion and parsing (listed as supported sources). ([Cyclopts][1])

Practical expectation:

* Cyclopts will parse tokens into the hinted Python types (int/float/Literal/etc.), apply Cyclopts validators, then instantiate the class (or build the structure) using those values.

For **attrs**, note that attrs itself supports per-field `converter=` functions that run during initialization. ([attrs.org][4])
**Implication:** if you rely on attrs converters, you can end up with *double conversion* (Cyclopts converts based on type hints; attrs converts again). Usually this is harmless if converters are idempotent, but it’s a design choice you should make explicit.

### 6.7.2 Pydantic is special: Cyclopts defers coercion (and even disables custom `converter`)

Cyclopts explicitly states:

* “For `pydantic` classes, Cyclopts will not internally perform type conversions and instead relies on pydantic’s coercion engine.” ([Cyclopts][2])
* The v4.4.5 API doc further clarifies: when a pydantic type hint is provided, Cyclopts disables its internal coercion engine **including the `Parameter.converter` hook** and leaves coercion to pydantic. ([Cyclopts][3])

**Practical consequences for agents:**

* Put validation/coercion policy in **pydantic** (validators, constrained types, strict mode), not Cyclopts converters, when the top-level parameter is a BaseModel.
* Be careful with **pydantic strict mode**: strict mode makes pydantic “much less lenient when coercing data” (strings → ints may fail), which can surprise CLIs where everything arrives as strings. ([docs.pydantic.dev][5])

### 6.7.3 Nested pydantic models: treat as a version-sensitive edge

There has been explicit discussion/requests around nested pydantic model parsing and CLI dot-notation conventions (e.g., `--f-arg.a 1`). ([GitHub][6])
If your design depends on nested BaseModel expansion, pin your Cyclopts version and add golden tests for the exact token forms you accept.

---

## 6.8 “Choose your interface” decision table (agent heuristics)

When exposing a structured type `T`:

1. **Dotted subkeys (default)**: best for discoverability + shell completion; stable per-field UX. ([Cyclopts][2])
2. **JSON dict/list parsing**: compact, good for automation, but token-shape sensitive and constrained (keyword-only; union-with-str disables defaults). ([Cyclopts][1])
3. **Flatten (`name="*"`)**: best when the wrapper type is “implementation detail” (config objects). ([Cyclopts][1])
4. **Opaque struct (`accepts_keys=False`)**: enforce one stable input form; useful for “selection/lookup” or “file-backed config” patterns where you don’t want field-level CLI surface. ([Cyclopts][2])

---

If you want the next increment inside Section 6, the high-leverage follow-on is: a “structured type contract checklist” (how to design dataclass/attrs/pydantic models so Cyclopts produces stable help, stable parsing, and stable error classes), plus a golden-test harness that pins (a) dotted keys, (b) JSON dict/list, (c) flattening, (d) `accepts_keys=False` token arity.

[1]: https://cyclopts.readthedocs.io/en/v4.4.6/user_classes.html "User Classes — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/latest/rules.html "Coercion Rules — cyclopts"
[3]: https://cyclopts.readthedocs.io/_/downloads/en/v4.4.5/pdf/ "cyclopts"
[4]: https://www.attrs.org/en/stable/examples.html?utm_source=chatgpt.com "attrs by Example - attrs 25.4.0 documentation"
[5]: https://docs.pydantic.dev/latest/concepts/strict_mode/?utm_source=chatgpt.com "Strict Mode - Pydantic Validation"
[6]: https://github.com/BrianPugh/cyclopts/issues/86 "[Feature request]: Nested pydantic validation · Issue #86 · BrianPugh/cyclopts · GitHub"

# 6.x Structured type contract checklist + golden-test harness (dotted keys, JSON dict/list, flattening, `accepts_keys=False` arity)

This is the “make structured types stable” playbook: **stable help**, **stable parsing**, **stable exception classes/messages**, across Cyclopts upgrades.

---

## A) Structured type contract checklist

### A1) Pick (and freeze) the input grammar(s) per structured parameter

Cyclopts gives you multiple syntaxes for a single structured parameter:

1. **Dotted subkeys**: `--movie.title … --movie.year …` (default) ([Cyclopts][1])
2. **JSON dict token**: `--movie '{"title": "...", "year": ...}'` — only under specific conditions ([Cyclopts][2])
3. **JSON list token(s)** for `list[DataclassLike]`: JSON array, repeated objects, or mixed ([Cyclopts][2])
4. **Namespace flattening**: remove one dotted segment via `Parameter(name="*")` (annotation or decorator) ([Cyclopts][2])
5. **Opaque/arity mode**: `Parameter(accepts_keys=False)` disables dotted keys and consumes required positional tokens ([Cyclopts][1])

**Contract rule (agent-grade):** decide *which* of these are supported for each parameter and encode the decision explicitly in `Parameter(...)` instead of relying on defaults.

#### JSON dict/list: don’t rely on `None` defaults if you care about backward compatibility

* `Parameter.json_dict=None` behaves like True **unless** the annotated type is `Union[..., str, ...]`; the parse only triggers when token starts with `{` and the parameter is provided as a **keyword option**. ([Cyclopts][2])
* `Parameter.json_list=None` behaves like True **unless** the iterable’s element type is `str` (default-avoid-strings behavior). ([Cyclopts][3])

There is an explicit “breaking change wishlist” item to change the “avoid strings” behavior for `json_dict/json_list` in the future. Treat that as a compatibility risk and pin with tests. ([GitHub][4])

**Concrete stability stance**

* If you want JSON parsing: set `json_dict=True` / `json_list=True` explicitly. (Then add a golden test proving it works for your types and token shapes.) ([Cyclopts][3])
* If you want *no JSON parsing ever*: set `json_dict=False` / `json_list=False` explicitly and test that `--movie '{...}'` is treated as a literal string token (or fails in a controlled way). ([Cyclopts][2])

---

### A2) Namespace contract: dotted names, flattening, and “escape hatches”

**Default:** Cyclopts generates dotted subkeys for dataclass-like parameters (e.g., `--movie.title`). ([Cyclopts][2])

**Flattening (`name="*"`) removes exactly one namespace segment**

* `movie: Annotated[Movie, Parameter(name="*")]` turns `--movie.title` into `--title`. ([Cyclopts][2])
* Using `@Parameter(name="*")` on the class is terser and is inherited by subclasses. ([Cyclopts][2])

**Checklist**

* If the wrapper type is an implementation detail (config/context objects): prefer flattening at the type definition (`@Parameter(name="*")`) so every usage is consistent. ([Cyclopts][5])
* If the wrapper type is user-facing: keep dotted names, and treat renames as breaking changes (or explicitly “escape” a field to the top-level by assigning it a `--field` name, then pin with help+parse snapshots). ([Cyclopts][6])

---

### A3) Field ordering & requiredness: what makes positional structured input stable

Cyclopts allows positional assignment into a user class (`add "Mad Max" 2015`). The positional behavior is inherently coupled to:

* the class’ initializer/field order (dataclass/attrs/NamedTuple semantics), and
* which fields are required vs have defaults. ([Cyclopts][2])

**Checklist**

* If you expect users to supply structured objects positionally, treat field order as a compatibility contract (don’t reorder fields; add only at the end with defaults). ([Cyclopts][2])
* If you don’t want to support positional structured objects: prefer keyword dotted keys or JSON dict input and document that in help (and/or enforce via `accepts_keys` + `n_tokens` + converter). ([Cyclopts][2])

---

### A4) Error-class stability: choose where conversion/validation errors originate

Cyclopts tries to preserve nice CLI errors and stable exception classes:

* Validators run **after** conversion; `AssertionError`, `TypeError`, or `ValidationError` raised by validators are promoted to `cyclopts.ValidationError` for nicer presentation. ([Cyclopts][7])
* Certain builtin error types (`ValueError`, `TypeError`, `AssertionError`) are re-interpreted and formatted into nicer CLI errors. ([Cyclopts][8])

**Checklist**

* Prefer “domain correctness” checks in validators, not converters (converters should only parse). This yields more consistent “Invalid value for X” style messages. ([Cyclopts][7])
* For `accepts_keys=False`, pin the “arity error” message; Cyclopts shows: `Parameter "--user" requires 2 arguments. Only got 1.` ([Cyclopts][1])
* In golden tests, assert **exception class** first (stable), and **message rendering** second (more fragile), unless your CLI UX is product-critical.

---

### A5) Help stability: docstrings, overrides, and format pinning

Cyclopts uses docstrings heavily and parses them via `docstring_parser`. ([Cyclopts][9])

**Help text precedence knobs**

* `Parameter(help="...")` overrides docstring-derived parameter help. ([Cyclopts][10])
* Cyclopts’ default help markup format has differed across versions (older docs: default reStructuredText; newer docs: default Markdown). To keep output stable across upgrades, set `App.help_format` explicitly. ([Cyclopts][11])

**Checklist**

* Set `App(help_format="plaintext")` for the most stable snapshots; or choose `"markdown"/"rst"` explicitly if you rely on markup rendering. ([Cyclopts][12])
* If you care about per-field help stability, avoid implicit docstring parsing drift: pin parameter help via `Parameter(help=...)` for the critical fields. ([Cyclopts][10])

---

### A6) Interop rules: dataclasses vs attrs vs pydantic

#### Dataclasses / attrs / NamedTuple / TypedDict

Cyclopts treats these as “dataclass-like libraries” for user class expansion. ([Cyclopts][2])

**attrs gotcha: converters can double-convert**
attrs fields may have a `converter` that runs during initialization (and possibly on set, depending on API). If Cyclopts already coerces strings→types, attrs converters can re-run conversion. Prefer idempotent converters or keep CLI models free of attrs converters. ([attrs.org][13])

#### Pydantic

Cyclopts explicitly says: for pydantic classes, Cyclopts does **not** perform internal type conversions and relies on pydantic’s coercion engine. ([Cyclopts][14])

**Pydantic strict mode caution**
CLI tokens are strings. Pydantic strict mode becomes “much less lenient” and may reject typical CLI string inputs (e.g., `'123'` for `int`). Decide lax vs strict consciously for CLI-facing models. ([docs.pydantic.dev][15])

**Nested BaseModel expansion is a risk area**
There’s an open design discussion about how nested pydantic models should be traversed and represented in help/CLI flags. If you depend on nested BaseModel dot-key behavior, pin version + add dedicated golden tests. ([GitHub][16])

---

## B) Golden-test harness: pin parsing + token arity + rendered help/errors

Below is a “single-file harness” you can drop into a repo. It pins:

* (a) dotted keys (`--movie.title`)
* (b) JSON dict/list parsing
* (c) flattening (`name="*"`)
* (d) `accepts_keys=False` token arity error text

### B1) Example CLI under test

```python
# cli_under_test.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Literal
import cyclopts
from cyclopts import App, Parameter

app = App(
    name="demo",
    help_format="plaintext",  # freeze markup parsing behavior across versions
)

@dataclass
class Movie:
    title: str
    year: int
    rating: float = 8.0

@Parameter(name="*")  # flatten config namespace at type-definition level
@dataclass
class Config:
    user: str
    server: str = "media.sqlite"

@dataclass
class User:
    name: str
    age: int
    region: Literal["us", "ca"] = "us"

@app.command
def add(movie: Movie) -> Movie:
    return movie

@app.command
def add_batch(movies: list[Movie]) -> list[Movie]:
    return movies

@app.command
def maintain(config: Config) -> Config:
    return config

@app.command
def opaque(user: Annotated[User, Parameter(accepts_keys=False)]) -> User:
    return user
```

This mirrors Cyclopts’ documented behaviors for JSON dict parsing, JSON list parsing, namespace flattening, and `accepts_keys=False` arity enforcement. ([Cyclopts][2])

---

### B2) Snapshot utilities (update-golden mode + deterministic consoles)

Cyclopts’ unit-testing cookbook recommends creating a deterministic Rich Console fixture for stable output comparisons, and shows capturing help via `SystemExit` + `capsys`. ([Cyclopts][17])
Rich recommends capturing output by writing the Console to a `StringIO` for unit tests. ([rich.readthedocs.io][18])

```python
# test_cli_structured_types.py
from __future__ import annotations

from io import StringIO
from pathlib import Path
import os
import pytest

from rich.console import Console
from cyclopts.exceptions import CycloptsError, UnknownOptionError, MissingArgumentError

from cli_under_test import app, Movie, Config, User

UPDATE = os.environ.get("UPDATE_GOLDENS") == "1"
GOLDEN_DIR = Path(__file__).parent / "goldens"

@pytest.fixture
def console() -> Console:
    # Cyclopts cookbook deterministic console settings (stable boxes/wrapping)
    return Console(width=70, force_terminal=True, highlight=False, color_system=None, legacy_windows=False)

def make_capture_console(width: int = 70) -> Console:
    # Rich: file=StringIO() recommended for test capture
    return Console(file=StringIO(), width=width, force_terminal=True, highlight=False, color_system=None, legacy_windows=False)

def assert_golden(name: str, actual: str) -> None:
    path = GOLDEN_DIR / name
    if UPDATE:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(actual)
        return
    expected = path.read_text()
    assert actual == expected
```

---

### B3) (a) Dotted keys parsing: `--movie.title`, `--movie.year`

```python
def test_dotted_keys_parse():
    command, bound, _ = app.parse_args(
        ["add", "--movie.title", "Dune", "--movie.year", "2021"],
        exit_on_error=False,
        print_error=False,
    )
    movie = bound.arguments["movie"]
    assert movie == Movie(title="Dune", year=2021, rating=8.0)
```

Dotted subkey parsing is explicitly the baseline structured-type behavior. ([Cyclopts][1])

---

### B4) (b) JSON dict parsing: `--movie '{...}'` + pin the enable/disable rules

Cyclopts documents JSON dict parsing conditions (keyword option, dataclass-like, not union’d with `str`, token starts with `{`) and notes dotted keys still work. ([Cyclopts][2])

```python
def test_json_dict_parse():
    command, bound, _ = app.parse_args(
        ["add", "--movie", '{"title": "Mad Max", "year": 2015, "rating": 8.1}'],
        exit_on_error=False,
        print_error=False,
    )
    movie = bound.arguments["movie"]
    assert movie == Movie(title="Mad Max", year=2015, rating=8.1)
```

**Optional hardening test (recommended):** add a second command with `movie: Movie | str` and explicitly set `Parameter(json_dict=True)` or `False`, then pin the behavior (because defaults around “avoid strings” are a known potential breaking-change area). ([Cyclopts][3])

---

### B5) (b) JSON list parsing for `list[Movie]` (array, repeated objects, mixed)

Cyclopts explicitly documents all three forms and states JSON list parsing is automatically enabled for lists of dataclasses with the same “not union’d with str; token starts with `{` or `[`” rules. ([Cyclopts][2])

```python
@pytest.mark.parametrize(
    "tokens,expected",
    [
        (
            ["add_batch", "--movies", '[{"title":"A","year":1},{"title":"B","year":2}]'],
            [Movie("A", 1, 8.0), Movie("B", 2, 8.0)],
        ),
        (
            ["add_batch", "--movies", '{"title":"A","year":1}', "--movies", '{"title":"B","year":2}'],
            [Movie("A", 1, 8.0), Movie("B", 2, 8.0)],
        ),
        (
            ["add_batch", "--movies", '{"title":"A","year":1}', "--movies", '[{"title":"B","year":2},{"title":"C","year":3}]'],
            [Movie("A", 1, 8.0), Movie("B", 2, 8.0), Movie("C", 3, 8.0)],
        ),
    ],
)
def test_json_list_parse(tokens, expected):
    command, bound, _ = app.parse_args(tokens, exit_on_error=False, print_error=False)
    assert bound.arguments["movies"] == expected
```

---

### B6) (c) Flattening contract: `@Parameter(name="*")` yields `--user`, `--server` (not `--config.user`)

Cyclopts documents flattening as removing the immediate dotted segment and also shows the “flattened config shared across commands” pattern. ([Cyclopts][2])

```python
def test_flattening_parse():
    command, bound, _ = app.parse_args(
        ["maintain", "--user", "alice", "--server", "db.sqlite"],
        exit_on_error=False,
        print_error=False,
    )
    cfg = bound.arguments["config"]
    assert cfg == Config(user="alice", server="db.sqlite")

def test_flattening_rejects_old_namespace():
    with pytest.raises(UnknownOptionError):
        app.parse_args(["maintain", "--config.user", "alice"], exit_on_error=False, print_error=False)
```

If you want to pin **rendered error text**, see the next test (error snapshotting).

---

### B7) (d) `accepts_keys=False` arity contract: pin the “requires N arguments” error

Cyclopts shows the exact user-facing message for insufficient tokens in `accepts_keys=False` mode: `Parameter "--user" requires 2 arguments. Only got 1.` ([Cyclopts][1])

```python
def test_accepts_keys_false_arity_error_snapshot(console):
    err_console = make_capture_console(width=70)

    with pytest.raises(CycloptsError):
        # 1 token for a User(name, age, ...) → should fail
        app(
            ["opaque", "--user", "Bob Smith"],
            console=console,
            error_console=err_console,
            exit_on_error=False,   # raise, don't sys.exit
            print_error=True,      # still render the nice message
            result_action="return_value",
        )

    actual_err = err_console.file.getvalue()
    assert_golden("opaque_arity_error.txt", actual_err)
```

Cyclopts’ unit-testing docs explicitly call out stdout vs stderr separation and recommend using `capsys.readouterr().err` or a custom `error_console` for error capture. ([Cyclopts][17])

---

### B8) Help-page snapshot pinning (optional, but powerful for CLI UX stability)

Cyclopts’ unit-testing cookbook shows how to snapshot help by passing a deterministic console and expecting `SystemExit`. ([Cyclopts][17])

```python
from textwrap import dedent

def test_help_add_snapshot(capsys, console):
    with pytest.raises(SystemExit):
        app(["add", "--help"], console=console)

    actual = capsys.readouterr().out
    assert_golden("help_add.txt", actual)
```

**Tip:** set `help_format="plaintext"` at `App(...)` creation time to reduce cross-version differences in markup parsing defaults (which have changed historically). ([Cyclopts][11])

---

## C) Minimal “stability matrix” you should actually run in CI

If structured-type behavior is core to your CLI, run the test suite against:

* your pinned Cyclopts version, and
* the newest Cyclopts in a periodic CI job.

The things most likely to drift are exactly the ones you’re pinning here: JSON dict/list enablement defaults (especially around `str` unions), nested pydantic behavior, and help rendering defaults. ([GitHub][4])

[1]: https://cyclopts.readthedocs.io/en/latest/rules.html "Coercion Rules — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/v4.4.6/user_classes.html "User Classes — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/v3.14.1/api.html "API — cyclopts"
[4]: https://github.com/BrianPugh/cyclopts/issues/324?utm_source=chatgpt.com "v4 Breaking Change Wishlist · Issue #324"
[5]: https://cyclopts.readthedocs.io/en/v3.2.1/user_classes.html "User Classes — cyclopts"
[6]: https://cyclopts.readthedocs.io/en/v3.14.1/api.html?utm_source=chatgpt.com "API — cyclopts - Read the Docs"
[7]: https://cyclopts.readthedocs.io/en/latest/parameter_validators.html?utm_source=chatgpt.com "Parameter Validators - Cyclopts - Read the Docs"
[8]: https://cyclopts.readthedocs.io/en/latest/parameters.html?utm_source=chatgpt.com "Parameters — cyclopts - Read the Docs"
[9]: https://cyclopts.readthedocs.io/en/latest/vs_typer/docstring/README.html "Docstring Parsing — cyclopts"
[10]: https://cyclopts.readthedocs.io/en/v3.4.1/parameters.html "Parameters — cyclopts"
[11]: https://cyclopts.readthedocs.io/en/v3.13.1/help.html "Help — cyclopts"
[12]: https://cyclopts.readthedocs.io/en/v5-develop/help.html "Help — cyclopts"
[13]: https://www.attrs.org/en/stable/examples.html "attrs by Example - attrs 25.4.0 documentation"
[14]: https://cyclopts.readthedocs.io/en/latest/rules.html?utm_source=chatgpt.com "Coercion Rules — cyclopts - Read the Docs"
[15]: https://docs.pydantic.dev/latest/concepts/strict_mode/ "Strict Mode - Pydantic Validation"
[16]: https://github.com/BrianPugh/cyclopts/issues/86?utm_source=chatgpt.com "Nested pydantic validation · Issue #86 · BrianPugh/cyclopts"
[17]: https://cyclopts.readthedocs.io/en/latest/cookbook/unit_testing.html "Unit Testing — cyclopts"
[18]: https://rich.readthedocs.io/en/latest/console.html "Console API — Rich 14.1.0 documentation"

# 7) Validation framework (Cyclopts) — parameter validators, group validators, and “validation as UX”

## 7.0 Validation lifecycle (where validators sit)

Cyclopts’ validation happens *after* coercion/conversion: tokens → typed value → **parameter validators** run; then **group validators** run across sets of parameters. The parameter-validator contract is any callable of the form `validator(type_, value) -> None` that raises if invalid; multiple validators may be supplied as a list. ([Cyclopts][1])

Cyclopts explicitly “promotes” validator-thrown `AssertionError`, `TypeError`, or `ValidationError` to `cyclopts.ValidationError` so it can present a nicer CLI error message. ([Cyclopts][1])

---

## 7.1 Parameter validators

### 7.1.1 Contract + composition (how to write validators that behave “Cyclopts-native”)

**Signature**

```python
def validator(type_, value) -> None:
    # raise to signal invalid value
    ...
```

Validation occurs after conversion; you can attach one validator or a list via `Parameter(validator=...)`. ([Cyclopts][1])

**Promotion rules (UX-critical)**
To get Cyclopts’ “nice error panel” rather than an uncaught raw exception, raise one of:

* `AssertionError`
* `TypeError`
* `ValidationError` (Cyclopts’ or your own)
  Cyclopts will wrap/promote these into `cyclopts.ValidationError` for presentation. ([Cyclopts][1])

**Design stance (agent rule)**

* Put *parsing* in converters; put *domain validity* in validators so failures map to `ValidationError` and render consistently (vs. accidental `CoercionError`-style failures). ([Cyclopts][1])

### 7.1.2 Built-in validator: `validators.Path(...)`

Cyclopts ships a `Path` validator that asserts properties of a `pathlib.Path` value—e.g., existence checks. ([Cyclopts][1])

**API (key args)**
`cyclopts.validators.Path(*, exists: bool=False, file_okay: bool=True, dir_okay: bool=True, ext: None|Any|Iterable[Any]=None)` ([Cyclopts][2])

**Canonical usage**

```python
from pathlib import Path
from typing import Annotated
from cyclopts import Parameter, validators

def main(path: Annotated[Path, Parameter(validator=validators.Path(exists=True))]):
    ...
```

This produces a formatted CLI error when the file doesn’t exist. ([Cyclopts][1])

**Scaling pattern**
Prefer **pre-annotated validated types** from `cyclopts.types` when you want consistency + terseness across many commands. The API lists many extension- and existence-specific aliases (e.g., `ExistingJsonPath`, `ExistingTomlPath`, `ImagePath`, etc.) built from the `Path` validator. ([Cyclopts][3])

### 7.1.3 Built-in validator: `validators.Number(...)`

Cyclopts’ `Number` validator constrains numeric input (min/max bounds and modulo). The docs show it used to enforce `0 <= n < 16`, producing a formatted error when violated. ([Cyclopts][1])

**API (key args)**
`cyclopts.validators.Number(*, lt=None, lte=None, gt=None, gte=None, modulo=None)` where each bound can be `int|float|None`. ([Cyclopts][2])

**Common properties**

* `lt`, `lte`, `gt`, `gte`: range constraints
* `modulo`: require “multiple of” constraint ([Cyclopts][2])

**Prebuilt numeric “contracts”**
Cyclopts exposes many `cyclopts.types.*` aliases for common constraints (e.g., `PositiveInt`, `NonNegativeInt`, fixed-width `UInt8/UInt16/...`, plus “HexUInt*” that changes default rendering). These are implemented as `Annotated[..., Parameter(validator=(Number(...),))]`. ([Cyclopts][3])

> **Agent heuristic:** if a constraint appears in >1 command, define a `types.py` alias (or reuse Cyclopts’) and make *help+error output* part of its contract via golden tests.

---

## 7.2 Group validators

### 7.2.1 Contract + attachment point

Group validators operate on *sets* of parameters to ensure they’re mutually compatible. You attach them via `Group.validator`. The validator signature is `validator(argument_collection: ArgumentCollection)` (raise on invalid). ([Cyclopts][4])

### 7.2.2 Built-in group validator: `LimitedChoice(min, max, allow_none)` → “N-of-K” constraints

The docs describe `LimitedChoice` as “limits the number of specified arguments within the group,” commonly used for mutual exclusion, and show the canonical help panel + error rendering. ([Cyclopts][4])

**API semantics (from Cyclopts API PDF)**

* `min`: minimum (inclusive) number of CLI parameters in the group that must be provided

  * special: if `min` is negative, then **all** parameters in the group must have CLI values provided ([Cyclopts][2])
* `max`: maximum (inclusive); defaults to `1` if `min==0`, else defaults to `min`
* `allow_none`: if True, also allow zero selections even if `min>0` ([Cyclopts][2])

**Operational meaning of “specified”**
“Specified arguments” is about parameters that received CLI input (not merely truthy defaults). This distinction is why `LimitedChoice` works for non-bool options too (e.g., one of `--file PATH` vs `--socket ADDR`).

**Typical group wiring**

```python
vehicle = Group(
    "Vehicle (choose one)",
    default_parameter=Parameter(negative=""),  # avoid --no-* noise for bools
    validator=validators.LimitedChoice(),      # defaults => mutually exclusive
)
```

The docs explicitly show disabling negative flags at the group default level in this pattern. ([Cyclopts][4])

### 7.2.3 Built-in alias: `MutuallyExclusive` (+ convenience instance)

Cyclopts provides:

* `class MutuallyExclusive`: alias of `LimitedChoice` “to make intentions more obvious,” enforcing only 1 argument supplied
* `cyclopts.validators.mutually_exclusive`: already-instantiated convenience object for `Group(validator=...)` ([Cyclopts][4])

### 7.2.4 Built-in: `all_or_none` (paired/atomic option sets)

Cyclopts provides `all_or_none` as a group validator that enforces: either **all** parameters in the group must be supplied, or **none** of them. ([Cyclopts][4])

The docs show the UX behavior: specifying `--foo` without `--bar` yields a formatted error “Missing argument: --bar”; similarly for multiple independent groups. ([Cyclopts][4])

> **Agent pattern:** `all_or_none` is the clean way to enforce “credentials come as a bundle” (e.g., `--user` + `--password`) or “either give all coordinates or none,” without pushing this logic into ad-hoc command code.

---

## 7.3 “Validation as UX”: rich panels vs plain exceptions/tracebacks

### 7.3.1 Cyclopts runtime errors (Coercion/Validation/etc.): default is “pretty error + exit”

Cyclopts’ default stance: for Cyclopts runtime errors like `CoercionError` or `ValidationError`, it prints a formatted error and then `sys.exit(1)`. You can control this via:

* `App.exit_on_error` (default True)
* `App.print_error` (default True)
* `App.help_on_error`, `App.verbose`
  …and these attributes are inherited by child apps. ([Cyclopts][5])

Cyclopts also separates output streams:

* `App.console` for normal output/help (stdout by default)
* `App.error_console` for errors (stderr by default) ([Cyclopts][5])

**Implication:** your validators should aim to raise errors that land in this “CycloptsError → rich panel” path (see promotion rules above). ([Cyclopts][1])

### 7.3.2 Plain exceptions (embedding / testing / “let it crash” debugging)

If you set `exit_on_error=False` and `print_error=False`, Cyclopts will raise exceptions normally (no formatted panel, no exit), which is ideal for embedding and for asserting exception classes/messages in tests. ([Cyclopts][5])

### 7.3.3 User-code exceptions: standard traceback by default; opt into Rich tracebacks

Cyclopts’ “Rich Formatted Exceptions” cookbook shows the distinction:

* Without extra setup, uncaught user exceptions produce a standard Python traceback.
* You can install Rich’s traceback handler (`rich.traceback.install`) using `error_console` so the traceback is rich-formatted. ([Cyclopts][6])

This is *orthogonal* to Cyclopts’ validator error panels:

* Validator failures should be presented as Cyclopts formatted errors (via promotion).
* Unexpected bugs in command code can be presented with rich tracebacks for developer UX. ([Cyclopts][6])

---

## Implementation checklist for agents

1. **Use built-in validators when possible** (`validators.Path`, `validators.Number`) and prefer `cyclopts.types.*` aliases to standardize constraints and help output across commands. ([Cyclopts][1])
2. **Raise “promotable” exceptions** in custom validators so Cyclopts renders them nicely (`AssertionError`/`TypeError`/`ValidationError`). ([Cyclopts][1])
3. **Enforce cross-parameter invariants with group validators**, not ad-hoc command logic (`LimitedChoice`/`MutuallyExclusive`, `all_or_none`). ([Cyclopts][4])
4. **Choose your UX mode explicitly**:

   * CLI UX: keep `print_error=True`, `exit_on_error=True`
   * Embedding/tests: set both False
   * Debug: install Rich tracebacks for user exceptions ([Cyclopts][5])

If you want the next increment, the natural follow-on is: a **validator design patterns catalog** (e.g., secret redaction in error messages, structured error codes, composing `Number` with custom validators, writing group validators that reason about “specified vs default”), plus a golden-snapshot harness that pins *both* the exception class and the rendered error panel text under a deterministic Rich console.

[1]: https://cyclopts.readthedocs.io/en/latest/parameter_validators.html "Parameter Validators — cyclopts"
[2]: https://cyclopts.readthedocs.io/_/downloads/en/v4.4.5/pdf/ "cyclopts"
[3]: https://cyclopts.readthedocs.io/en/v4.5.0/api.html "API — cyclopts"
[4]: https://cyclopts.readthedocs.io/en/latest/group_validators.html "Group Validators — cyclopts"
[5]: https://cyclopts.readthedocs.io/en/latest/app_calling.html "App Calling & Return Values — cyclopts"
[6]: https://cyclopts.readthedocs.io/en/latest/cookbook/rich_formatted_exceptions.html "Rich Formatted Exceptions — cyclopts"

# 8) Groups and “presentation-aware” parameter organization (Cyclopts)

## 8.0 Group = “help panel + policy layer + optional validator”

A `Group` is **pure metadata** you attach to *commands* and/or *parameters* to (a) form help-page panels and (b) provide an abstraction layer for validators/defaults. Groups can be created explicitly (`Group(...)`) or implicitly by string; **typos in string group names create new groups**, and groups don’t hold direct references to members (so they can be reused across commands). ([Cyclopts][1])

Core fields (v4.5.0 API):

* `name`: panel title + string-lookup key; unnamed groups aren’t shown. ([Cyclopts][2])
* `help`: text rendered *inside the panel above entries* (group-level doc). ([Cyclopts][2])
* `show`: `None` (default) means “show only if name is provided”; `show=False` hides. ([Cyclopts][2])
* `sort_key`: panel ordering control (callable/generator supported). ([Cyclopts][2])
* `default_parameter`: group-scoped `Parameter` inserted into the parameter resolution stack between `App.default_parameter` and signature `Annotated[...] Parameter`. Must not itself set `group`. ([Cyclopts][2])
* `validator`: group validator operating on an `ArgumentCollection` containing the group’s arguments; invoked even if no member has tokens; **not invoked for command groups**. ([Cyclopts][2])
* `help_formatter`: per-group help rendering override (inherits from `App.help_formatter` by default). ([Cyclopts][2])

---

## 8.1 Command groups vs parameter groups (same type, different semantics)

### 8.1.1 Command groups

Attach with `@app.command(group=...)` (or mutate an existing command node’s `group`). Cyclopts’ docs show regrouping meta commands (`--help`, `--version`) into an “Admin” panel and placing a custom `info` command there. The default command group is `App.group_commands` (defaults to `"Commands"`). ([Cyclopts][1])

**Key rule:** group validators are **not** invoked for command groups (they’re for parameter coherence, not command lists). ([Cyclopts][2])

```python
from cyclopts import App

app = App()

# Move meta commands into an Admin group (string creates/joins group by name).
app["--help"].group = "Admin"
app["--version"].group = "Admin"

@app.command(group="Admin")
def info():
    """Print debugging system information."""
    ...
```

([Cyclopts][1])

### 8.1.2 Parameter groups

Attach with `Parameter(group=...)`. Parameter groups do three things simultaneously:

1. organize help into panels,
2. provide a natural scope for **inter-parameter validators**,
3. provide a natural scope for group-wide parameter defaults via `Group.default_parameter`. ([Cyclopts][1])

Default routing for parameters:

* positional-only args → `App.group_arguments` (defaults to `"Arguments"`)
* all other parameters → `App.group_parameters` (defaults to `"Parameters"`) ([Cyclopts][1])

---

## 8.2 Group-level validators and defaults (the “policy layer”)

### 8.2.1 Validators: `Group.validator` runs on the group’s `ArgumentCollection`

`Group.validator` receives an `ArgumentCollection` containing the arguments in the group; Cyclopts documents that validators are invoked regardless of whether any argument has tokens, which matters for writing robust validators (they must tolerate defaults/unset state). Validators are not invoked for command groups. ([Cyclopts][2])

### 8.2.2 Defaults: `Group.default_parameter` is a resolution-stack layer

`Group.default_parameter` sits between `App.default_parameter` and the signature’s `Annotated Parameter`, and **the provided `Parameter` cannot have a `group` value** (to avoid recursion/ambiguity). ([Cyclopts][2])

Practical uses:

* disable negative flags for mutually-exclusive bools (`negative=""`),
* suppress meaningless default metadata in certain panels (`show_default=False`),
* enforce presentation defaults (e.g., show/hide defaults) consistently for a whole panel. ([Cyclopts][1])

### 8.2.3 “Validator-only groups” (hidden groups that don’t affect the help UI)

Cyclopts explicitly supports groups with an empty name or `show=False` so you can apply validator/default logic *without creating another help panel*. ([Cyclopts][1])

Canonical pattern: parameter belongs to two groups:

* its visible panel (often `app.group_parameters`)
* a hidden “policy group” carrying validator/defaults

```python
from typing import Annotated
from cyclopts import App, Group, Parameter, validators

app = App()

policy = Group(
    "",  # hidden (empty name)
    validator=validators.MutuallyExclusive(),
    default_parameter=Parameter(show_default=False, negative=""),
)

@app.command
def foo(
    car:   Annotated[bool, Parameter(group=(app.group_parameters, policy))] = False,
    truck: Annotated[bool, Parameter(group=(app.group_parameters, policy))] = False,
):
    ...
```

This exact “hidden group for validators/defaults” approach is documented (including the `show_default=False` + `negative=""` tuning). ([Cyclopts][1])

---

## 8.3 Panel formation, ordering, and “presentation-aware” sorting

### 8.3.1 Panels and visibility

Groups form titled panels on the help page; groups with empty `name` or `show=False` are not shown. ([Cyclopts][1])

### 8.3.2 Ordering rules: `Group.sort_key` + `Group.create_ordered()`

Default: panels are alphabetical. You can manipulate ordering via `Group.sort_key`; `Group.create_ordered()` produces a group with a monotonically increasing sort key so help panels appear in instantiation order. ([Cyclopts][1])

v4.5.0 API details worth pinning in tests:

* if `sort_key` is a **generator**, Cyclopts immediately consumes it with `next()`
* groups with `sort_key!=None` sort by `(sort_key, group.name)`
* groups with `sort_key==None` sort alphabetically **after** the default groups `App.group_commands/group_arguments/group_parameters` ([Cyclopts][2])

### 8.3.3 Reordering the default groups

Default groups are displayed first, but you can override them by passing custom `Group(...)` instances to `App(group_parameters=..., group_arguments=..., group_commands=...)`, which then behave like normal groups for sorting. ([Cyclopts][2])

---

## 8.4 Group-level help formatting overrides (panel styling + accessibility)

Cyclopts supports a **help formatter protocol** and exposes `help_formatter` at both the `App` and `Group` levels. Group help formatters override the app-level default. ([Cyclopts][3])

### 8.4.1 App-level vs group-level formatter selection

* `App(help_formatter=...)` sets the default for all panels.
* `Group(help_formatter=...)` overrides rendering for that group’s panel. ([Cyclopts][3])

Per v4.5.0 API, `help_formatter` can be:

* `None` (inherit), `"default"`, `"plain"` (no-frills plain text), or a custom callable implementing the HelpFormatter protocol. ([Cyclopts][2])

### 8.4.2 Group-level “panel docs”: `Group.help`

`Group.help` is additional documentation rendered inside the panel above entries—use this to encode constraints (“choose exactly one”), defaults policy, or operational notes. ([Cyclopts][2])

### 8.4.3 Practical styling patterns (per-group visual distinction)

Help customization docs show per-group formatters to visually distinguish groups (e.g., “Required Options” vs “Optional Settings”), and the `PlainFormatter` option exists for accessibility-focused plain output. ([Cyclopts][3])

Minimal pattern:

```python
from cyclopts import App, Group, Parameter
from cyclopts.help import DefaultFormatter, PanelSpec
from typing import Annotated
from rich.box import DOUBLE, MINIMAL

required = Group(
    "Required Options",
    help="These must be provided.",
    help_formatter=DefaultFormatter(panel_spec=PanelSpec(box=DOUBLE, border_style="red bold")),
)
optional = Group(
    "Optional Settings",
    help_formatter=DefaultFormatter(panel_spec=PanelSpec(box=MINIMAL, border_style="green")),
)

app = App()

@app.default
def main(
    input_file: Annotated[str, Parameter(group=required)],
    output_dir: Annotated[str, Parameter(group=required)],
    verbose: Annotated[bool, Parameter(group=optional)] = False,
):
    ...
```

The key mechanism—`help_formatter` on both App and Group, and group-specific overrides—is explicitly documented. ([Cyclopts][3])

---

## 8.5 Agent-grade design patterns (what to standardize in a large CLI)

1. **Treat groups as schema constants**

* define `Group` objects centrally (avoid strings to prevent accidental “new group by typo”). ([Cyclopts][1])

2. **Separate “policy groups” from “presentation groups”**

* visible panel group for UX
* hidden validator/default group for correctness and consistent metadata suppression (`show_default=False`, `negative=""`, etc.). ([Cyclopts][1])

3. **Pin ordering deterministically**

* use `Group.create_ordered()` for user-facing panel order, or explicit `sort_key` and override the default groups when you need them to interleave. ([Cyclopts][1])

4. **Accessibility switch**

* `App(help_formatter="plain")` (or per-group `"plain"`) gives no-frills plain output, suitable for screen readers and environments that can’t handle box drawing. ([Cyclopts][3])

[1]: https://cyclopts.readthedocs.io/en/latest/groups.html "Groups — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/v4.5.0/api.html "API — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/v4.4.5/help_customization.html "Help Customization — cyclopts"

# 9) Help system and output formatting (Cyclopts)

## 9.0 Help generation pipeline: “derive text + derive entries + render panels”

Cyclopts’ help output is the composition of three layers:

1. **Content derivation (text + metadata)**

   * Command short/long descriptions + per-parameter descriptions are extracted primarily from docstrings (parsed via `docstring_parser`). Cyclopts explicitly uses `docstring_parser` internally. ([Cyclopts][1])
   * Parameter help can be explicitly overridden via `Parameter(help=...)` (and then docstring-derived text is ignored for that parameter). ([Cyclopts][2])

2. **Argument/command inventory (what entries exist on this help page)**

   * Commands include your subcommands plus meta commands like `--help/-h` and `--version`. ([Cyclopts][1])
   * Parameters include positional “Arguments” and “Parameters” (options/flags), possibly grouped. ([Cyclopts][3])

3. **Rendering (how it looks)**

   * A **help formatter** (built-in or custom) is invoked per help panel (group). Default is Rich-based panels/tables (`DefaultFormatter`); an accessibility-focused plain output mode exists (`PlainFormatter`). ([Cyclopts][3])

---

## 9.1 Source resolution order for command help text (summary + long description)

Cyclopts explicitly documents the resolution order for “help string components” (per command node), in descending precedence:

1. `help=` field in the `@app.command` decorator **for functions**. ([Cyclopts][4])
2. `App.help` (app-level help string). ([Cyclopts][4])
3. The `__doc__` docstring of the function registered with `@app.default` on that command node; Cyclopts parses docstring to populate both command descriptions and parameter-level help. ([Cyclopts][4])
4. The same resolution order but applied to the **Meta App** node (`app.meta`). ([Cyclopts][4])

Two important constraints that affect how you structure code generators:

* When registering an `App` object as a subcommand, providing `help=` via `@app.command(... help=...)` is explicitly forbidden (to reduce ambiguity) and raises `ValueError`; use the subapp’s `App(help=...)` instead. ([Cyclopts][4])
* If `App.help` is not set, Cyclopts falls back to the *short description* (first block / first line behavior) of the registered `@app.default` docstring. ([Cyclopts][1])

---

## 9.2 Parameter help pipeline: docstring extraction + overrides + naming invariants

### 9.2.1 Docstring parsing for parameter help

Cyclopts can extract parameter help strings from the command’s docstring (e.g., NumpyDoc `Parameters` section). It explicitly notes that it parses docstrings to populate parameter-level help. ([Cyclopts][4])

**Naming invariant:** docstrings must use the **Python variable name** from the signature, even if you renamed the CLI flag via `Parameter(name=...)`. Cyclopts calls this out directly. ([Cyclopts][4])

### 9.2.2 Explicit override: `Parameter(help=...)`

If you set `Parameter(help="...")` for a parameter, Cyclopts uses that help text and ignores the docstring’s description for that parameter. ([Cyclopts][2])

**Agent implementation rule:** treat `Parameter(help=...)` as the top-precedence “documentation patch” for a single argument node; everything else (docstring parsing, autodoc) becomes optional.

---

## 9.3 Docstring style vs help markup: two distinct concerns

Cyclopts conflates two “docstring-ish” knobs that you should keep separate:

### 9.3.1 Docstring *style* (how to parse `Parameters`, etc.)

Cyclopts (via `docstring_parser`) can interpret multiple docstring styles: ReST, Google, Numpydoc, Epydoc. ([Cyclopts][1])

This governs **how parameter docs are extracted**, not how markup is rendered.

### 9.3.2 Help *markup format* (`App.help_format`) (how extracted text is rendered)

`App.help_format` controls how the extracted help text is interpreted/rendered. Cyclopts documents these options:

* `plaintext`: display as-is, no additional parsing/reflow; newlines are literal. ([Cyclopts][4])
* `rich`: treat text as Rich markup; newlines literal. ([Cyclopts][4])
* `restructuredtext` / `rst`: interpret docstrings as ReST and render (Cyclopts historically used this as the default in older releases). ([Cyclopts][4])
* `markdown` / `md`: interpret docstrings as Markdown and render (Cyclopts defaults to Markdown in newer releases). ([Cyclopts][5])

### 9.3.3 **Version drift alert**: default `help_format` changed across releases

* In v3.x docs, ReStructuredText is described as the default behavior. ([Cyclopts][6])
* In v4.x docs, Markdown is described as the default behavior. ([Cyclopts][5])

**Best practice:** always set `App(help_format=...)` explicitly in production CLIs if you want stable help rendering across Cyclopts upgrades. ([Cyclopts][7])

---

## 9.4 Help flags, epilogue, usage, and key customization entry points

### 9.4.1 Help flags (`App.help_flags`)

* Default help triggers are `--help` and `-h`. ([Cyclopts][7])
* You can set `help_flags` to a string or iterable of strings (e.g., `["--send-help", "-h"]`). ([Cyclopts][5])
* You can disable help entirely by setting `help_flags=""` or `help_flags=[]`. ([Cyclopts][5])

**Agent rule:** if you are building meta-CLIs (wrappers around wrappers), disabling the built-in help at one layer and delegating help to a parent layer can eliminate name collisions and confusion.

### 9.4.2 Help epilogue (`App.help_epilogue`)

`help_epilogue` is displayed at the end of the help screen after all panels; it’s intended for version/support/notes. ([Cyclopts][5])

Inheritance model:

* epilogues inherit parent→child like `help_format`; set one global epilogue on the root app to apply everywhere, override on specific subapps, or disable by setting empty string. ([Cyclopts][5])
* epilogue supports the **same formatting** as help (markdown/plaintext/rst/rich depending on `help_format`). ([Cyclopts][7])

### 9.4.3 Usage line (`App.usage`)

You can override the default “Usage: …” header; setting it to `""` disables showing the default usage. ([Cyclopts][8])

### 9.4.4 Help-on-error (`App.help_on_error`)

`help_on_error` prints the help page before printing an error (useful for UX on validation failures). It inherits parent→child and defaults to False. ([Cyclopts][8])

### 9.4.5 Command visibility (`show=False`)

Help is also affected by `@app.command(show=False)`; hidden commands remain executable but are not listed. ([Cyclopts][7])

---

## 9.5 Help formatter architecture: formatters, panels, tables, columns

### 9.5.1 The formatter selection surface (`help_formatter` on App and Group)

Cyclopts exposes help formatting through `help_formatter` on both `App` and `Group`. These accept formatters following the HelpFormatter protocol. ([Cyclopts][3])

`App(help_formatter=...)` accepts:

* `None` (inherit, eventually defaulting to `DefaultFormatter`)
* `"default"` (`DefaultFormatter`)
* `"plain"` (`PlainFormatter`)
* a callable/custom object following HelpFormatter protocol ([Cyclopts][7])

`Group(help_formatter=...)` overrides the app-level default for that group/panel. ([Cyclopts][3])

### 9.5.2 Built-in formatters

* `DefaultFormatter`: Rich-based panels/tables with structured layouts. ([Cyclopts][3])
* `PlainFormatter`: plain text output without colors/special characters (accessibility/screen readers). ([Cyclopts][3])

### 9.5.3 DefaultFormatter composition model: **PanelSpec + TableSpec + ColumnSpec**

Cyclopts’ “DefaultFormatter” is explicitly parameterized by three spec layers:

#### (A) PanelSpec — outer panel appearance

PanelSpec controls box style, border style, padding, expand-to-width, etc. ([Cyclopts][3])

```python
from cyclopts import App
from cyclopts.help import DefaultFormatter, PanelSpec
from rich.box import DOUBLE

app = App(
    help_formatter=DefaultFormatter(
        panel_spec=PanelSpec(
            box=DOUBLE,
            border_style="blue",
            padding=(1, 2),
            expand=True,
        )
    )
)
```

#### (B) TableSpec — table styling within panels

TableSpec controls header visibility, row lines, edges, border style, padding, box type, etc. ([Cyclopts][3])

```python
from cyclopts.help import DefaultFormatter, TableSpec
from rich.box import SQUARE

app = App(
    help_formatter=DefaultFormatter(
        table_spec=TableSpec(
            show_header=True,
            show_lines=True,
            show_edge=False,
            border_style="green",
            padding=(0, 2, 0, 0),
            box=SQUARE,
        )
    )
)
```

#### (C) ColumnSpec — column definition and rendering

You can define custom columns (including a “required marker” column), control widths, style, overflow, and provide renderers. Renderers can be:

* a callable `renderer(entry) -> str|Renderable`, or
* a string naming an attribute (e.g., `"description"`). ([Cyclopts][3])

```python
from cyclopts import App, Group, Parameter
from cyclopts.help import DefaultFormatter, ColumnSpec, TableSpec
from typing import Annotated

def names_renderer(e):
    return " ".join(e.names + e.shorts).strip()

custom = Group(
    "Custom Layout",
    help_formatter=DefaultFormatter(
        table_spec=TableSpec(show_header=True),
        column_specs=(
            ColumnSpec(renderer=lambda e: "★" if e.required else " ", header="", width=2, style="yellow bold"),
            ColumnSpec(renderer=names_renderer, header="Option", style="cyan", max_width=30),
            ColumnSpec(renderer="description", header="Description", overflow="fold"),
        ),
    ),
)

@app.default
def main(
    input_path: Annotated[str, Parameter(group=custom, help="Input file path")],
    output_path: Annotated[str, Parameter(group=custom, help="Output file path")],
):
    ...
```

([Cyclopts][3])

### 9.5.4 Dynamic column builders (adaptive to terminal width / panel contents)

Instead of a fixed `tuple[ColumnSpec,...]`, `column_specs` can be a function `(console, options, entries) -> tuple[ColumnSpec,...]` that adapts layout at runtime (e.g., hide required indicator column if none are required; compute max widths from console width). ([Cyclopts][3])

### 9.5.5 Creating a completely custom formatter (HelpFormatter protocol)

Cyclopts lets you fully replace panel rendering by implementing the HelpFormatter protocol. The docs demonstrate a custom formatter with:

* `__call__(console, options, panel)` to render a single help panel
* optional `render_usage(...)` and `render_description(...)` hooks for top matter ([Cyclopts][3])

This is the escape hatch when you want:

* no box drawing,
* JSON help output for tooling,
* hyperlinks/ANSI policies,
* deterministic output for golden tests.

### 9.5.6 Group-level formatting: different panels can look different

You can apply different `DefaultFormatter(panel_spec=...)` instances per group (e.g., “Required Options” with a red double border vs “Optional Settings” minimal border). ([Cyclopts][3])

---

## 9.6 Practical “LLM-agent rules” for stable, high-quality help

1. **Pin `help_format` explicitly** to avoid doc rendering drift (v3 default ReST; v4 default Markdown). ([Cyclopts][6])
2. Prefer docstrings for most help text, but use `Parameter(help=...)` for high-risk/critical flags where wording is a contract (and to avoid docstring parsing edge cases). ([Cyclopts][2])
3. If you rename CLI flags, keep docstrings using **Python variable names**; Cyclopts expects that and still renders the CLI names correctly. ([Cyclopts][4])
4. Adopt formatter layering:

   * Default: `DefaultFormatter` with a consistent PanelSpec/TableSpec
   * Accessibility mode: `help_formatter="plain"`
   * Highlight critical groups: group-level `help_formatter` overrides ([Cyclopts][3])
5. Use dynamic column builders for “big CLIs” so the help stays readable across narrow terminals and rich panels (columns adapt to width and requiredness). ([Cyclopts][3])

If you want the next increment: a “help contract” golden-test harness that snapshots (a) plain formatter output, (b) default formatter output with fixed console width, (c) per-group formatting overrides, and (d) help-on-error behavior—all using a deterministic Rich Console config.

[1]: https://cyclopts.readthedocs.io/en/latest/getting_started.html "Getting Started — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/latest/parameters.html "Parameters — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/v4.5.0/help_customization.html "Help Customization — cyclopts"
[4]: https://cyclopts.readthedocs.io/en/v4.0.0/help.html "Help — cyclopts"
[5]: https://cyclopts.readthedocs.io/en/v4.1.0/help.html "Help — cyclopts"
[6]: https://cyclopts.readthedocs.io/en/v3.11.1/help.html?utm_source=chatgpt.com "Help — cyclopts"
[7]: https://cyclopts.readthedocs.io/en/v4.4.1/api.html?utm_source=chatgpt.com "API — cyclopts"
[8]: https://cyclopts.readthedocs.io/en/v3.14.1/api.html?utm_source=chatgpt.com "API — cyclopts - Read the Docs"

# 10) Execution semantics, return values, and error/exit policy (Cyclopts)

## 10.1 Invocation surface: `App.__call__` token normalization + dispatch

### 10.1.1 Call signature (the knobs that matter)

`App.__call__(tokens=None | str | Iterable[str], *, console, error_console, print_error, exit_on_error, help_on_error, verbose, end_of_options_delimiter, backend, result_action) -> Any` ([Cyclopts][1])

Key semantics:

* **`tokens=None`** → defaults to `sys.argv[1:]` (everything after the program name). ([Cyclopts][1])
* **`tokens` as `list[str]`** → used directly (no additional splitting). ([Cyclopts][2])
* **`tokens` as `str`** → Cyclopts internally calls `shlex.split()` to produce a list. ([Cyclopts][2])

**Agent footgun:** `shlex.split()` is POSIX-shell oriented; Python’s own docs warn it’s designed for Unix shells and can be incorrect on Windows / non-POSIX shells. When you want deterministic behavior (tests, programmatic invocation, Windows paths), pass a list of tokens, not a string. ([Python documentation][3])

### 10.1.2 End-of-options delimiter (force remaining tokens positional)

Cyclopts supports a POSIX-style delimiter (default `"--"`) that forces all following tokens to be interpreted as positionals. You can override per call or via `App.end_of_options_delimiter`; set it to `""` to disable. ([Cyclopts][1])

### 10.1.3 Async commands: loop backend + in-loop execution

Execution of async commands is governed by `backend` (`"asyncio"` or `"trio"`), inheritable from `App.backend`, and overridable per call. ([Cyclopts][1])

* **Sync context**: `app()` will run an async command via the configured backend. ([Cyclopts][1])
* **Already in an event loop** (notebooks / async servers): use `await app.run_async(...)` (the explicit “don’t create a new loop” entrypoint). ([Cyclopts][1])
  If you choose `backend="trio"`, Cyclopts documents that you must have Trio installed via the extra (`cyclopts[trio]`). ([Cyclopts][4])

### 10.1.4 Ctrl-C policy (`suppress_keyboard_interrupt`)

By default, Cyclopts suppresses the error message on `KeyboardInterrupt` and exits “gracefully”; set `suppress_keyboard_interrupt=False` to let `KeyboardInterrupt` propagate (useful in debugging, embedding, or telemetry-first wrappers). ([Cyclopts][1])

---

## 10.2 Return value contract: `result_action` as a first-class post-exec stage

Cyclopts’ `result_action` controls how command return values are printed and/or converted into exit codes, for both `__call__()` and `run_async()`. It can be:

* a predefined literal mode,
* a custom callable `(result) -> processed`,
* a left-to-right pipeline `list[mode|callable]`. ([Cyclopts][1])

### 10.2.1 Why the default uses `sys.exit`

Installed entry points (PyPA `console_scripts`) are effectively `sys.exit(main())`, and they treat returning `None` as exit code 0. Cyclopts chooses a default `result_action` that matches this “entrypoint return == process exit semantics” expectation. ([packaging.python.org][5])

### 10.2.2 Built-in modes: exhaustive taxonomy (what each one *means*)

Cyclopts enumerates these literal modes (full list):
`return_value`, `call_if_callable`, `print_non_int_return_int_as_exit_code`, `print_str_return_int_as_exit_code`, `print_str_return_zero`, `print_non_none_return_int_as_exit_code`, `print_non_none_return_zero`, `return_int_as_exit_code_else_zero`, `print_non_int_sys_exit` (default), `sys_exit`, `return_none`, `return_zero`, `print_return_zero`, `sys_exit_zero`, `print_sys_exit_zero`. ([Cyclopts][1])

**Mental model:** you’re selecting two orthogonal behaviors:

1. **Output policy**: print nothing vs print strings vs print anything non-None vs print non-int, etc.
2. **Termination policy**: return an exit code vs call `sys.exit(...)`.

#### A) CLI-native (calls `sys.exit`)

* **`print_non_int_sys_exit` (default)**: bool → 0/1, int → exit(int), other non-None → print then exit(0), None → exit(0). ([Cyclopts][1])
* **`sys_exit`**: never prints; bool/int → exit code; everything else exits 0. ([Cyclopts][1])
* **`sys_exit_zero`**: always exit(0). ([Cyclopts][1])
* **`print_sys_exit_zero`**: print result (even None), then exit(0). ([Cyclopts][1])

#### B) Embedding/testing (returns a code or value; no `sys.exit`)

* **`return_value`**: returns raw command return (no printing). ([Cyclopts][1])
* **`return_int_as_exit_code_else_zero`**: return 0/1 for bool, return int, else 0; never prints. ([Cyclopts][1])
* **`print_non_int_return_int_as_exit_code`**: print non-int non-None; return int/bool as code; else 0. ([Cyclopts][1])
* **`print_str_return_int_as_exit_code`**: only prints strings; return int/bool as code; else 0. ([Cyclopts][1])
* **`print_str_return_zero`**: only prints strings; always returns 0. ([Cyclopts][1])
* **`print_non_none_return_int_as_exit_code`**: print any non-None; return int/bool as code; else 0. ([Cyclopts][1])
* **`print_non_none_return_zero`**: print any non-None; always returns 0. ([Cyclopts][1])
* **`return_none` / `return_zero`**: ignore result; always return None / 0. ([Cyclopts][1])
* **`print_return_zero`**: always print result; return 0. ([Cyclopts][1])

#### C) Transform-only (intended for composition)

* **`call_if_callable`**: if result is callable, call it with no args; else return unchanged. Intended to be combined with another mode (e.g., CLI printing/exit). ([Cyclopts][1])

### 10.2.3 Pipelines: deterministic post-processing chains

If `result_action` is a list/tuple, actions apply **left-to-right**, each receiving the prior step’s output. ([Cyclopts][1])
Canonical use: dataclass commands. The docs show `result_action=["call_if_callable", "print_non_int_sys_exit"]` so Cyclopts instantiates a dataclass command, calls it (via `__call__`), then prints+exits with standard CLI semantics. ([Cyclopts][6])

**Agent rule:** keep `sys_exit*` actions at the *end* of pipelines; anything after a `sys.exit()` won’t run.

---

## 10.3 Error/exit policy: Cyclopts errors vs user exceptions

### 10.3.1 Cyclopts runtime errors: default = “formatted error + exit(1)”

Cyclopts is mostly hands-off, but for Cyclopts runtime errors (e.g., coercion/validation), it will `sys.exit(1)` by default to avoid exposing unformatted exceptions to CLI users. ([Cyclopts][2])

Control knobs (App attributes or per-call overrides), inherited by sub-apps:

* `exit_on_error` (default True): exit(1) on Cyclopts errors vs raise. ([Cyclopts][2])
* `print_error` (default True): print formatted error output. ([Cyclopts][2])
* `help_on_error` (default False): print help before error. ([Cyclopts][2])
* `verbose` (default False): include developer-oriented details in error strings. ([Cyclopts][2])

Cyclopts uses separate consoles:

* `console` for normal output/help (stdout default),
* `error_console` for errors (stderr default). ([Cyclopts][2])

### 10.3.2 User-code exceptions: default is standard Python traceback

If your command function raises a normal exception, Cyclopts will surface a standard Python traceback (i.e., it doesn’t swallow it into CycloptsError). The cookbook shows this explicitly with a `TypeError` raised inside the command. ([Cyclopts][7])

### 10.3.3 How `sys.exit` behaves (why return types matter)

`sys.exit(arg)` raises `SystemExit`. If `arg` is an int, shells interpret 0 as success, nonzero as failure; `None` is equivalent to 0; non-int objects may be printed to stderr and yield exit code 1. ([Python documentation][8])
Cyclopts’ default `print_non_int_sys_exit` avoids passing arbitrary objects into `sys.exit` by printing non-int values itself and exiting 0. ([Cyclopts][1])

---

## 10.4 Tracebacks: standard vs rich-formatted (and how to wire it cleanly)

### 10.4.1 Cyclopts recipe: install Rich traceback handler on `error_console`

Cyclopts recommends installing Rich’s traceback handler and using the same `error_console` that Cyclopts uses for error output. ([Cyclopts][7])

```python
import sys
from cyclopts import App
from rich.console import Console
from rich.traceback import install as install_rich_traceback

error_console = Console(stderr=True)
app = App(error_console=error_console)

install_rich_traceback(console=error_console)
app()
```

This yields rich-rendered tracebacks for uncaught user exceptions. ([Cyclopts][7])

### 10.4.2 Rich-side knobs you’ll actually use

Rich’s docs highlight:

* `install(show_locals=True)` to include locals per frame,
* `Console.print_exception(show_locals=True)` for manual exception printing. ([rich.readthedocs.io][9])

**Agent guideline:** set `show_locals=False` in production CLIs that may handle secrets; enable it only in dev builds or behind a `--verbose` / env flag.

---

## 10.5 Agent-grade execution patterns (canonical “contracts”)

### Pattern A: “Real CLI” (entrypoint semantics)

* Keep defaults: `result_action="print_non_int_sys_exit"`, `exit_on_error=True`, `print_error=True`.
* Ensure commands return: `int` for explicit exit code, `bool` for success/failure, `str` for a user message, or `None` for success-with-no-output. ([Cyclopts][1])

### Pattern B: “Library embedding / tests”

* `result_action="return_value"` to avoid `sys.exit`.
* `exit_on_error=False`, `print_error=False` so Cyclopts raises exceptions you can assert on. ([Cyclopts][2])

### Pattern C: “Machine-readable stdout” (JSON CLI)

* Command prints JSON itself; set `result_action="sys_exit"` to suppress incidental printing and keep exit code handling. ([Cyclopts][1])
* Keep errors on stderr via `error_console` default separation. ([Cyclopts][2])

### Pattern D: “Dataclass command objects”

* `result_action=["call_if_callable", "print_non_int_sys_exit"]` so the dataclass instance is invoked and its return is printed/exited consistently. ([Cyclopts][6])

### Pattern E: “Async host”

* In an async application, always use `await app.run_async(...)` to avoid loop-creation issues. ([Cyclopts][1])

---

If you want the next increment after Section 10, the high-leverage continuation is: **config injection + execution composition** (how `App.config` functions interact with parsing, conversion/validation, and meta-app forwarding), and how to build “safe-by-default” command chaining without losing deterministic error/exit semantics.

[1]: https://cyclopts.readthedocs.io/en/latest/api.html "API — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/latest/app_calling.html "App Calling & Return Values — cyclopts"
[3]: https://docs.python.org/3/library/shlex.html "shlex — Simple lexical analysis — Python 3.14.2 documentation"
[4]: https://cyclopts.readthedocs.io/_/downloads/en/v4.5.0/pdf/?utm_source=chatgpt.com "Release 4.5.0 Brian Pugh"
[5]: https://packaging.python.org/specifications/entry-points/ "Entry points specification - Python Packaging User Guide"
[6]: https://cyclopts.readthedocs.io/en/stable/cookbook/dataclass_commands.html "Dataclass Commands — cyclopts"
[7]: https://cyclopts.readthedocs.io/en/latest/cookbook/rich_formatted_exceptions.html "Rich Formatted Exceptions — cyclopts"
[8]: https://docs.python.org/3/library/sys.html "sys — System-specific parameters and functions — Python 3.14.2 documentation"
[9]: https://rich.readthedocs.io/en/latest/traceback.html "Traceback — Rich 14.1.0 documentation"

# 11) Config injection + execution composition (App.config, meta-app forwarding, safe command chaining)

## 11.1 Where `App.config` sits in the execution pipeline

Cyclopts treats *configs* as a **post-parse, pre-convert** mutation stage:

* `App.config` is a callable (or list) executed **after parsing CLI tokens and environment variables**, but **before any conversion and validation**. ([Cyclopts][1])
* The config callable receives the *resolved command context* and an **`ArgumentCollection`** and must **modify it in-place**. ([Cyclopts][1])

**Signature (current, v4.4.x+)**

```python
def config(app: "App", commands: tuple[str, ...], arguments: ArgumentCollection) -> None:
    ...
```

([Cyclopts][1])

**Operational consequence:** configs are conceptually “default injectors” that operate on the parsed-but-not-yet-typed argument graph. They should *not* be thought of as “just another parse layer”—they mutate the argument model right before coercion/validation.

---

## 11.2 Built-in config classes: indexing algorithm + precedence semantics

Cyclopts ships config helpers intended to be passed directly to `App(config=...)`; the docs emphasize these are usually **classes implementing `__call__`**, not necessarily functions. ([Cyclopts][2])

### 11.2.1 Common indexing rules (Toml/Json/Yaml/Dict builtins)

All built-in config loaders apply the same key navigation algorithm:

1. Apply `root_keys` (if provided) to enter your “project namespace” within the config structure.
2. Apply the command chain (`commands`) to enter the *current command’s* namespace.
3. For each key/value pair in that namespace: apply it **only if CLI args were not provided** for that parameter. ([Cyclopts][1])

This implies a “defaults cascade” rather than “override everything”: CLI-specified values are sticky.

### 11.2.2 `Toml(...)` (the canonical pyproject pattern)

The API specifies:

```python
cyclopts.config.Toml(
    path,
    *,
    root_keys=(),
    allow_unknown=False,
    use_commands_as_keys=True,
    source=None,
    must_exist=False,
    search_parents=False,
)
```

…and documents:

* `root_keys` is how you enter `[tool.myproject]` or similar namespaces in `pyproject.toml`. ([Cyclopts][1])
* `must_exist=True` raises `FileNotFoundError` if missing. ([Cyclopts][1])
* `source` is used in error messages; defaults to absolute path. ([Cyclopts][1])

### 11.2.3 `Env(prefix)` (environment-variable injection)

`cyclopts.config.Env(prefix)` derives env var names by concatenating:

1. the provided prefix
2. the command/subcommand chain leading to the executed function
3. the parameter CLI name with `--` stripped and hyphens replaced by underscores ([Cyclopts][2])

This yields predictable per-command scoping (e.g., `CHAR_COUNTER_COUNT_CHARACTER`). ([Cyclopts][2])

### 11.2.4 `Dict(fetch_fn, source=...)` (non-file config sources)

Cyclopts provides an in-memory/dynamic config source that calls a function to fetch a dict, with optional `source` for error reporting. ([Cyclopts][2])

### 11.2.5 Multiple config sources: sequencing and “priority”

You can pass a sequence to `App.config`; configs are applied **sequentially**, and the docs explicitly state earlier entries can have priority (example uses `Env` before `Toml`). ([Cyclopts][2])

Cyclopts’ documented precedence ladder (for the example) is:

1. CLI args override everything
2. Env can override TOML
3. TOML provides base config
4. Python defaults are last ([Cyclopts][2])

> **Practical rule:** treat `App.config=[higher_priority, lower_priority, …]` where earlier injectors “win” by setting values first and later injectors only filling remaining unset values.

---

## 11.3 Writing a config injector: invariants you must preserve

### 11.3.1 Determinism + idempotency

Because config functions run every invocation (and may run multiple times in meta-app patterns), your config injector should be:

* **pure w.r.t. input arguments** (no time-based drift unless you explicitly want it),
* **idempotent** (running it twice doesn’t keep “stacking” overrides),
* **monotone**: only fill missing values unless you have an explicit override policy.

This aligns with the built-in algorithm (“apply key/value if CLI not provided”). ([Cyclopts][1])

### 11.3.2 “Injected defaults” vs “user-specified”: validator semantics footgun

Many *group validators* reason about whether arguments were “specified” (vs defaulted). A config injector that sets defaults may not count as “specified” for validators such as `LimitedChoice()`; there’s an explicit discussion noting config-loaded values can appear to bypass that style of validation. ([GitHub][3])

**Hardening patterns**

* Put cross-parameter invariants into an **App-level validator** that sees final converted kwargs, not “specified-ness” heuristics. Cyclopts documents an App-level validator that runs **after** `Parameter` and `Group` validators. ([Cyclopts][1])
* Alternatively, make your config injector *emit tokens* (conceptually) by setting the argument state in a way that Cyclopts treats as user-provided—this is internal-behavior-dependent, so the safest public approach is App-level validation.

---

## 11.4 Meta-app forwarding + config injection (composition pattern)

Cyclopts’ config docs show the canonical pattern for “user-specified config file”:

* A meta-app captures all remaining tokens via `*tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)]`
* It parses/sets the desired config source (e.g., `Toml(config_path, root_keys=..., search_parents=True)`)
* Then forwards into the main app by calling `app(tokens)` ([Cyclopts][2])

### 11.4.1 Why `allow_leading_hyphen=True` matters here

If meta-app token capture used the default `allow_leading_hyphen=False`, option-looking tokens (e.g., `--flag`) would terminate positional consumption and/or be treated as meta options instead of forwarded tokens. Cyclopts’ coercion rules describe how option-like tokens stop positional parsing when `allow_leading_hyphen` is False. ([Cyclopts][4])

**Meta-forwarding invariant:** *token capture must be “opaque”*—it should not interpret or stop on option-like tokens—otherwise forwarding is lossy.

### 11.4.2 “Safe” meta-forwarding policy knobs

If you’re composing meta-app forwarding with deterministic behavior:

* call `app(tokens, exit_on_error=False, print_error=False, result_action="return_int_as_exit_code_else_zero")` (or similar) so forwarding doesn’t `sys.exit` the host process on parse errors and returns a stable code. The `exit_on_error` semantics are documented in the API. ([Cyclopts][1])
* if you want CLI UX (pretty errors), keep `print_error=True` but still set `exit_on_error=False` and handle exceptions in the meta layer.

---

## 11.5 “Safe-by-default” command chaining (without nondeterministic exits)

Cyclopts explicitly does **not** natively support command chaining, preferring robust parsing; but it provides a meta-app-based reference implementation using a delimiter token (e.g., `"AND"`) that splits token streams into groups and calls `app(group)` per segment. ([Cyclopts][5])

That reference implementation is intentionally minimal; for production-grade chaining you typically need:

* deterministic exit behavior (no surprise `sys.exit` mid-chain),
* clear error attribution per segment,
* optional “stop-on-first-failure” semantics,
* correct forwarding of option-like tokens.

### 11.5.1 Baseline splitter (from docs)

Cyclopts’ example:

* meta captures tokens with `allow_leading_hyphen=True`,
* uses `itertools.groupby` to split on delimiter,
* calls `app(group)` for each group. ([Cyclopts][5])

### 11.5.2 Hardened chaining runner (recommended pattern)

Key changes vs docs:

1. **Disable `sys.exit`** inside each segment (`exit_on_error=False`).
2. **Make return codes explicit** by forcing a non-exiting `result_action`.
3. **Capture structured failures** per segment (CycloptsError class + tokens).
4. Optionally **stop** after first failure.

```python
import itertools
from dataclasses import dataclass
from typing import Annotated

from cyclopts import App, Parameter
from cyclopts.exceptions import CycloptsError

@dataclass(frozen=True)
class SegmentResult:
    tokens: list[str]
    ok: bool
    exit_code: int
    error_class: str | None = None
    error_msg: str | None = None

def run_segment(app: App, seg_tokens: list[str]) -> SegmentResult:
    try:
        code = app(
            seg_tokens,
            exit_on_error=False,
            print_error=False,  # flip True if you want rich errors per segment
            result_action="return_int_as_exit_code_else_zero",
        )
        # return_int_as_exit_code_else_zero => bool/int→code else 0
        return SegmentResult(tokens=seg_tokens, ok=(code == 0), exit_code=int(code))
    except CycloptsError as e:
        return SegmentResult(tokens=seg_tokens, ok=False, exit_code=1,
                             error_class=e.__class__.__name__, error_msg=str(e))
    except Exception as e:
        # user-code exception in the command
        return SegmentResult(tokens=seg_tokens, ok=False, exit_code=1,
                             error_class=e.__class__.__name__, error_msg=str(e))

def chained(app: App, tokens: list[str], delimiter: str = "AND", stop_on_error: bool = True) -> tuple[int, list[SegmentResult]]:
    groups = [list(g) for is_delim, g in itertools.groupby(tokens, lambda t: t == delimiter) if not is_delim] or [[]]

    results: list[SegmentResult] = []
    for seg in groups:
        r = run_segment(app, seg)
        results.append(r)
        if stop_on_error and not r.ok:
            break

    # Example aggregation policy: first nonzero else 0
    final = next((r.exit_code for r in results if r.exit_code != 0), 0)
    return final, results

app = App()

@app.command
def foo(val: int): ...

@app.command
def bar(flag: bool = False): ...

@app.meta.default
def main(*tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)]):
    code, results = chained(app, list(tokens))
    return code
```

**Why this is “safe-by-default”**

* It respects Cyclopts’ own guidance that chaining is a user-implemented composition and uses the documented token-capture approach. ([Cyclopts][5])
* It avoids mid-chain `sys.exit` by overriding `exit_on_error` and `result_action` per segment (both are explicit `__call__` policy knobs). ([Cyclopts][1])

### 11.5.3 Recommended CLI contract for chained mode

Decide and document:

* delimiter token (must not collide with real command names),
* whether `--help` inside a segment prints help for that command and whether chaining stops,
* aggregation strategy: *first failure*, *max exit code*, or *bitwise OR* (Unix-ish).

---

## 11.6 Putting it all together: config + forwarding + chaining without “policy leakage”

A robust “composed CLI” architecture usually does:

1. **Root App**: defines global error/exit policy defaults (or leaves defaults and overrides in meta).
2. **Meta App**: captures “launcher-level” concerns: config path selection, command chaining delimiter, telemetry toggles.
3. **Config injection**: meta sets `app.config` (single source or list) based on launcher-level args, then forwards. ([Cyclopts][2])
4. **Segment execution**: each forwarded segment executes with deterministic policy overrides (`exit_on_error=False`, non-exiting `result_action`). ([Cyclopts][1])
5. **Final validation**: if you rely on group-validator “specified” semantics, add an App-level validator for “final state correctness,” especially when configs can set combinations that weren’t specified on the CLI. ([Cyclopts][1])

If you want the next increment after this: I’d codify a “config injector contract” with a **golden test matrix** that pins (a) precedence order (CLI > env > file > default), (b) segment isolation in chaining, and (c) validator behavior with config-provided values vs CLI-provided values.

[1]: https://cyclopts.readthedocs.io/_/downloads/en/v4.4.4/pdf/ "cyclopts"
[2]: https://cyclopts.readthedocs.io/en/latest/config_file.html "Config Files — cyclopts"
[3]: https://github.com/BrianPugh/cyclopts/issues/87 "[Discussion] First-class config overrides via pyproject.toml? · Issue #87 · BrianPugh/cyclopts · GitHub"
[4]: https://cyclopts.readthedocs.io/en/latest/rules.html?utm_source=chatgpt.com "Coercion Rules — cyclopts - Read the Docs"
[5]: https://cyclopts.readthedocs.io/en/latest/command_chaining.html "Command Chaining — cyclopts"

# 12) Shell completion and CLI tooling (Cyclopts)

Cyclopts supports **bash / zsh / fish** completion, but you need to choose between two fundamentally different completion modes:

* **Dynamic completion** (development): shell completes `cyclopts …`, which then imports your script on each tab press.
* **Static completion** (production): you generate/install a shell script for your *actual* CLI executable; tab completion is instant and does not invoke Python. ([cyclopts.readthedocs.io][1])

---

## 12.1 Hard constraint: shells complete *executables in `$PATH`*

Bash/zsh/fish generally only provide completion for installed executables, not `python myapp.py`. Cyclopts calls this a fundamental shell limitation and uses it to motivate the wrapper workflow below. ([cyclopts.readthedocs.io][1])

---

## 12.2 Development workflow: `cyclopts run` (dynamic completion without packaging)

### CLI shape

```bash
cyclopts run SCRIPT [ARGS...]
```

Runs a Cyclopts application from a Python script **with dynamic shell completion**; everything after `SCRIPT` is forwarded to the loaded app. ([cyclopts.readthedocs.io][2])

### Script path resolution contract

* `cyclopts run script.py` → auto-detects an `App` object; errors if it can’t determine one. ([cyclopts.readthedocs.io][1])
* `cyclopts run script.py:app` → explicitly names the `App` object. ([cyclopts.readthedocs.io][1])

### Virtualenv / interpreter behavior (important for reproducibility)

`cyclopts run` **imports your script in-process** (no subprocess), using the **same interpreter** running `cyclopts`. Practically:

* `cyclopts` must be installed in the active venv,
* your script sees whatever packages are installed in that environment. ([cyclopts.readthedocs.io][1])

### Performance + side-effect warning (dynamic completion calls Python on *every tab press*)

Cyclopts warns that dynamic completion imports your script and executes Python **on every tab press**, so heavy imports can make completion noticeably slow; it recommends **Lazy Loading** to mitigate this during development and advises static completion for production. ([cyclopts.readthedocs.io][1])

### Install once: completion for `cyclopts` itself

Completion for your script “comes through the `cyclopts` CLI completion”; install it once via:

```bash
cyclopts --install-completion
```

([cyclopts.readthedocs.io][1])

---

## 12.3 Production workflow: static completion (generate/install scripts for *your* app)

Cyclopts exposes three related APIs:

### 12.3.1 `App.generate_completion()` — generate a script (no install)

**Signature (v4.4.1):**

```python
App.generate_completion(*, prog_name: str | None = None,
                        shell: Literal["zsh","bash","fish"] | None = None) -> str
```

* `prog_name`: defaults to the first name in `app.name`
* `shell=None`: auto-detect current shell
* Returns the full completion script text
* Raises `ValueError` if app has no name / unsupported shell; raises `ShellDetectionError` if auto-detect fails ([cyclopts.readthedocs.io][3])

This is your “build artifact” primitive (e.g., generate completion scripts at release time and ship them in packages or docs).

### 12.3.2 `App.install_completion()` — write script to disk + optionally enable in shell startup

**Signature (v4.4.1):**

```python
App.install_completion(*,
  shell: Literal["zsh","bash","fish"] | None = None,
  output: Path | None = None,
  add_to_startup: bool = True
) -> Path
```

Semantics:

* generates + writes the script to a shell-specific default path (unless `output` provided)
* if `add_to_startup=True`, writes a sourcing line into your shell rc file so completion loads automatically ([cyclopts.readthedocs.io][3])

Default install paths:

* **Zsh:** `~/.zsh/completions/_<app_name>`
* **Bash:** `~/.local/share/bash-completion/completions/<app_name>`
* **Fish:** `~/.config/fish/completions/<app_name>.fish` ([cyclopts.readthedocs.io][1])

### 12.3.3 `App.register_install_completion_command()` — add `--install-completion` to your CLI

**Signature (v4.4.1):**

```python
App.register_install_completion_command(
  name: str | Iterable[str] = "--install-completion",
  add_to_startup: bool = True,
  **kwargs
) -> None
```

* registers a command that calls `install_completion()`
* lets you rename the command (e.g., `"--setup-completion"`)
* lets you pass `**kwargs` through to `App.command()` (e.g., `help=`, `group=`, override flags) ([cyclopts.readthedocs.io][3])

Programmatic install is the recommended end-user UX:

```python
from cyclopts import App
app = App(name="myapp")
app.register_install_completion_command()  # enables: myapp --install-completion
```

([cyclopts.readthedocs.io][1])

---

## 12.4 Shell configuration behavior (how scripts become “active”)

When installing via `install_completion()` / the registered install command, Cyclopts defaults to modifying startup files:

* **Zsh:** adds to `~/.zshrc`
* **Bash:** adds to `~/.bashrc`
* **Fish:** no rc modification needed (fish autoloads from `~/.config/fish/completions/`) ([cyclopts.readthedocs.io][1])

To install *without* modifying shell rc files:

```python
app.register_install_completion_command(add_to_startup=False)
# or:
app.install_completion(add_to_startup=False)
```

([cyclopts.readthedocs.io][1])

---

## 12.5 CLI tooling shipped with Cyclopts (`cyclopts …`)

### `cyclopts --install-completion`

Installs completion for the `cyclopts` CLI itself:

```bash
cyclopts --install-completion [--shell {zsh,bash,fish}] [--output PATH]
```

* `--shell` optional (auto-detect)
* `--output` optional (default path) ([cyclopts.readthedocs.io][2])

### `cyclopts run`

As above, enables dynamic completion for scripts during development, and docs explicitly recommend installing `cyclopts` completion once for this to work. ([cyclopts.readthedocs.io][2])

---

## 12.6 “Safe-by-default” patterns (what to standardize in a serious CLI)

### Pattern A: production-grade install command + no rc edits (enterprise-friendly)

```python
app = App(name="myapp")
app.register_install_completion_command(add_to_startup=False, group="Setup",
                                        help="Install shell completion scripts.")
```

This keeps your installer command discoverable while avoiding surprise startup file edits by default. ([cyclopts.readthedocs.io][3])

### Pattern B: deterministic script generation in release tooling

Use `generate_completion(shell="zsh")`/`"bash"`/`"fish"` during CI to produce pinned scripts (commit them, ship them, or attach to releases). The signature/behavior is stable and returns raw script text. ([cyclopts.readthedocs.io][3])

### Pattern C: “dev fast path” with `cyclopts run`, but avoid heavy imports

If you rely on `cyclopts run` dynamic completion, keep your top-level imports light (or use lazy loading) because completion imports your script on each tab press. ([cyclopts.readthedocs.io][1])

---

If you want the next increment, the natural continuation is **value-level completions** (static choices vs stateful/dynamic completion) and how Cyclopts currently models completion candidates for parameter *values* (and what’s still open/under discussion for “dynamic completion sourced from runtime state”).

[1]: https://cyclopts.readthedocs.io/en/latest/shell_completion.html "Shell Completion — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/latest/cli_reference.html "CLI Reference — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/v4.4.1/api.html "API — cyclopts"

## 12.x Value-level completions (static choices vs stateful/dynamic) — current Cyclopts model + open gaps

### 12.x.1 Two completion execution models (what can run at `<TAB>` time)

Cyclopts has *two distinct* completion “deployment” modes:

1. **Static completion scripts (production)**
   You generate/install a shell script that is **pre-generated and does not call Python**, so it’s instantaneous. ([Cyclopts][1])
   *Implication:* value completions must be derivable **ahead of time** (finite sets, mostly).

2. **Dynamic completion via `cyclopts run` (development)**
   `cyclopts run` does “dynamic completion” by **importing your script and calling Python on every tab press**. ([Cyclopts][1])
   *Implication:* in principle, runtime state *could* be consulted (config files, filesystem, etc.), but Cyclopts still needs an API surface to ask your app “what candidates for this parameter value right now?”

---

# 12.x.2 Cyclopts’ current *value completion* model is `Argument.get_choices(force=True)`

Cyclopts’ **only explicitly documented value-candidate extraction hook** is:

```python
Argument.get_choices(force: bool = False) -> tuple[str, ...] | None
```

It “extract[s] completion choices from type hint” and supports:

* `Literal[...]`
* `Enum`
* `Union` containing those choice-bearing types

It also explicitly states that `force=True` is “used by shell completion to always provide choices,” ignoring `Parameter.show_choices`. ([Cyclopts][2])

### `Parameter.show_choices` is a **help rendering knob**, not a completion knob

`Parameter.show_choices: bool | None = True` controls whether choices are displayed on the help page. ([Cyclopts][2])
But `Argument.get_choices(force=True)` returns choices even when `show_choices=False`, specifically so completion can still offer them. ([Cyclopts][2])

**Agent invariant:**

* Help generation: typically calls `get_choices(force=False)` ⇒ respects `show_choices`.
* Shell completion: calls `get_choices(force=True)` ⇒ ignores `show_choices` and always emits finite choices. ([Cyclopts][2])

---

# 12.x.3 Choice extraction details (how “static value candidates” are derived)

## A) `Literal[...]` → finite candidates

Cyclopts treats `Literal` as the canonical “choices” type; `get_choices()` returns those literal values as strings. ([Cyclopts][2])

**Contract:** if you want stable value completion, express it as `Literal[...]` (or as a `Union` whose arms include Literals).

## B) `Enum` → name-based candidates, normalized by `name_transform`

Cyclopts matches enum **member names**, not values, and applies `Parameter.name_transform` to enum names and user tokens (case-insensitive, underscore↔hyphen normalization by default). ([Cyclopts][3])
This matters for completion because the candidates should match the *CLI-visible* names users can type.

**Contract:** prefer enums whose member names are already “CLI-friendly” (or set `name_transform` accordingly) so completion candidates don’t surprise users. ([Cyclopts][3])

## C) `Union[...]` containing `Literal` / `Enum` → merged choice set

`get_choices()` explicitly supports unions that “contain” Literals/Enums. ([Cyclopts][2])
**Practical interpretation:** use `Union[Literal[...], EnumType, ...]` when you want “finite choices plus some other parse arm,” and completion can still offer the finite subset.

> Parsing semantics (union arm order) and completion semantics (choice extraction) are related but not identical: union order governs conversion; choice extraction governs suggestions. Cyclopts documents conversion order separately; choice extraction is “types containing Literals/Enums.” ([Cyclopts][2])

---

# 12.x.4 What Cyclopts does *not* currently provide (publicly) for values

Cyclopts docs clearly cover:

* completion installation/generation + `cyclopts run` dynamic wrapper ([Cyclopts][1])
* *static* value candidates extracted from type hints via `get_choices(force=True)` ([Cyclopts][2])

But **there is no documented `Parameter(completion=...)` / “completion callback” API** (in docs/API excerpts above) that would let you compute candidates from runtime state (config files, repos, network, etc.) at completion time.

So: today, “value completion” is effectively **finite choice sets** inferred from type hints.

---

# 12.x.5 “Dynamic/stateful value completion” — what’s open / under discussion

There is an active feature request asking for **dynamic completion** where candidates depend on runtime state (e.g., config files): ([GitHub][4])

> “some of my parameters have partially non-static completions that rely on state (config files, etc).” ([GitHub][4])

This is the core gap: Cyclopts can *execute Python* at completion time (via `cyclopts run`, and possibly via future completion backends), but it needs:

* a **completion callback contract**
* a **completion context** (tokens so far, which parameter is being completed, current incomplete prefix)
* and a **stable shell transport** format for candidates

---

# 12.x.6 Workable patterns *today* (until a first-class dynamic API exists)

## Pattern 1: “Semi-static” state → generate Literals at startup, then regenerate completion scripts when state changes

If “state” changes infrequently (e.g., config-defined environments), you can:

* load config at import/startup,
* build `Literal[tuple(env_names)]`,
* then (re)install completion scripts when config changes.

This keeps you within Cyclopts’ current value completion model (`get_choices` from Literals). ([Cyclopts][2])

## Pattern 2: Use `cyclopts run` during development when you need live Python behavior

Because `cyclopts run` imports your script on every `<TAB>`, you can prototype “stateful completion logic” (even if you ultimately need a custom shell script to ship it). ([Cyclopts][1])

## Pattern 3: Ship a custom completion script that calls a hidden “completion endpoint” command

If you need *true* runtime-driven candidates now, the most robust approach is external to Cyclopts’ built-in static generator:

* add a hidden command (e.g., `__complete`) that:

  * accepts the current argv prefix + incomplete token
  * uses `App.parse_known_args` / `parse_commands` to locate the command node
  * prints newline-delimited candidates
* write a shell completion script that calls `myapp __complete ...`

This is exactly how other ecosystems implement dynamic completion (program is invoked during completion), but Cyclopts does not currently generate such scripts automatically. (This is also why the feature request exists.) ([GitHub][4])

---

If you want, the next increment can be a **concrete design spec** for a Cyclopts-native dynamic completion API (types, call signatures, context object, and how it composes with `Argument.get_choices(force=True)` as the fallback), plus a minimal “proof” implementation pattern that LLM agents can apply to your CLI immediately.

[1]: https://cyclopts.readthedocs.io/en/latest/shell_completion.html "Shell Completion — cyclopts"
[2]: https://cyclopts.readthedocs.io/_/downloads/en/v4.5.0/pdf/ "cyclopts"
[3]: https://cyclopts.readthedocs.io/en/latest/rules.html?utm_source=chatgpt.com "Coercion Rules — cyclopts - Read the Docs"
[4]: https://github.com/BrianPugh/cyclopts/issues/641 "Dyanmic shell completion · Issue #641 · BrianPugh/cyclopts · GitHub"

# 13) Developer-UX extras: interactive shell + text editor integration (Cyclopts)

---

## 13.1 Built-in interactive shell (`App.interactive_shell`) and help flows

### 13.1.1 API surface (call-signature-first)

```python
App.interactive_shell(
  prompt: str = "$ ",
  quit: str | Iterable[str] | None = None,
  dispatcher: Dispatcher | None = None,
  console: Console | None = None,
  exit_on_error: bool = False,
  result_action: ResultAction | None = None,
  **kwargs
) -> None
```

Semantics (the non-obvious parts):

* **Blocking REPL**: “Create a blocking, interactive shell” where “all registered commands can be executed in the shell.” ([Cyclopts][1])

* **Quit words**: `quit` is a string or list of strings that causes the shell to exit; default is `["q", "quit"]`. ([Cyclopts][1])

* **Dispatcher hook**: `dispatcher` is an optional function invoked after parsing; expected signature (v4.5) is:

  ```python
  def dispatcher(command: Callable, bound: inspect.BoundArguments, ignored: dict[str, Any]) -> Any:
      return command(*bound.args, **bound.kwargs)
  ```

  The shown body is the *default dispatcher*. ([Cyclopts][1])

* **REPL-specific error/return semantics**:

  * `exit_on_error` defaults to **False** (no `sys.exit()` on parsing errors inside the REPL). ([Cyclopts][1])
  * `result_action` defaults to **`"print_non_int_return_int_as_exit_code"`** inside the interactive shell (prints non-int results, returns int/bool as exit codes without calling `sys.exit`). If `None`, it inherits `App.result_action`. ([Cyclopts][1])

* `**kwargs` are passed through to `parse_args()` (so you can thread standard parsing knobs into the REPL). ([Cyclopts][1])

### 13.1.2 “How to wire it so your CLI still behaves like a CLI”

Cyclopts’ cookbook explicitly recommends: **don’t call `app.interactive_shell()` at module bottom** if you still want the script to behave normally as a CLI. Instead, register a command (or default) that launches it. ([Cyclopts][2])

Canonical pattern:

```python
@app.command
def shell():
    app.interactive_shell()
```

…then keep `app()` as your normal entrypoint. ([Cyclopts][2])

### 13.1.3 Help in the shell: meta flags work, but UX is awkward → add a `help` command

In the shell:

* Special flags like `--help` and `--version` work. ([Cyclopts][2])
* But the “root help” is a bit awkward because you’re typing `--help` as if it were a command. The cookbook shows the UX and then recommends adding a dedicated `help` command that calls `app.help_print()`. ([Cyclopts][2])

```python
@app.command
def help():
    """Display the help screen."""
    app.help_print()
```

This gives you `cyclopts> help` as a natural REPL action. ([Cyclopts][2])

### 13.1.4 `help_print()` is the primitive behind “help flows”

In v4.5, `help_print` is itself shaped like a “CLI-ish function”:

```python
App.help_print(
  tokens: None | str | Iterable[str] = None,
  *,
  console: Console | None = None
) -> None
```

* It “Print[s] the help page.” ([Cyclopts][1])
* `tokens` are interpreted as the traversal path through your command graph (so you can print help for root vs a subcommand path). ([Cyclopts][1])
* It defaults to `sys.argv` when tokens aren’t provided (per earlier API; the intent persists as “tokens to interpret for traversing the application command structure”). ([Cyclopts][3])
* `console` follows the normal `App.console` resolution if not supplied. ([Cyclopts][3])

**Agent note:** In v4.5 the method signature annotates `tokens` as `Parameter(show=False)` and `console` as `Parameter(parse=False)`—a strong hint that Cyclopts treats “help printing” as a reusable internal command-like primitive rather than a bespoke side-effect function. ([Cyclopts][1])

### 13.1.5 Dispatcher is the “execution composition” hook for REPLs

Because the REPL’s dispatcher receives:

* the resolved **callable** (`command`)
* its bound args/kwargs (`inspect.BoundArguments`)
* `ignored` (for `Parameter(parse=False)` injection patterns)

…it is the correct place to implement REPL-only policies: logging, telemetry, post-processing, guardrails, or meta injection. ([Cyclopts][1])

**High-leverage patterns for LLM agents**

1. **Telemetry-first dispatch (no `sys.exit`)**
   Keep `exit_on_error=False` and use a non-exiting `result_action` (the REPL default already does this). Then wrap the dispatcher call in your own span/timer and print results via the selected `result_action`. ([Cyclopts][1])

2. **Prevent “shell inside shell” recursion**
   There is an open documentation/UX concern that the shell launcher command is itself exposed inside the interactive shell, enabling infinite nesting. ([GitHub][4])
   A practical mitigation *today* is to implement a dispatcher that refuses to invoke the shell launcher callable when already in REPL mode (compare by function identity), returning a benign code/message instead. This keeps the command registered for normal CLI use while enforcing a REPL policy at the only stable hook Cyclopts exposes for “execution interception.” ([Cyclopts][1])

3. **REPL-only meta injection**
   If your command signatures use `Parameter(parse=False)` (meta-app style), the dispatcher gets the `ignored` map explicitly (v4.5). That enables a “REPL session object” injection (db handle, repo context, etc.) without polluting CLI tokens. ([Cyclopts][1])

### 13.1.6 REPL exit UX

The cookbook example prints: “Interactive shell. Press Ctrl-D to exit.” and shows a prompt loop. ([Cyclopts][2])
Additionally, `quit` keywords (`q`, `quit` by default) provide a shell-native exit path. ([Cyclopts][1])

---

## 13.2 Text editor helper (`cyclopts.edit`) + “didn’t save / didn’t change” workflow

### 13.2.1 API surface (call-signature-first)

```python
cyclopts.edit(
  initial_text: str = "",
  *,
  fallback_editors: Sequence[str] = ("nano", "vim", "notepad", "gedit"),
  editor_args: Sequence[str] = (),
  path: str | Path = "",
  encoding: str = "utf-8",
  save: bool = True,
  required: bool = True,
) -> str
```

Core semantics:

* Launches the user’s default editor to collect text input (blocks until editor closes). ([Cyclopts][1])
* Editor resolution: if a suitable editor can’t be determined from environment variable **`EDITOR`**, Cyclopts tries `fallback_editors` in order. ([Cyclopts][1])
* `path`: if provided, that file path is opened (often better UX because editors display the filename); otherwise Cyclopts uses a temporary file. ([Cyclopts][1])
* `save=True`: require the user to save; otherwise raises `EditorDidNotSaveError`. ([Cyclopts][1])
* `required=True`: require the saved text to differ from `initial_text`; otherwise raises `EditorDidNotChangeError`. ([Cyclopts][1])

### 13.2.2 Exception taxonomy (the “abort commit message” primitives)

`edit()` raises:

* `EditorError`: base editor error (also raised if the editor subcommand returns non-zero)
* `EditorNotFoundError`: no suitable editor found
* `EditorDidNotSaveError`: user exited without saving when `save=True`
* `EditorDidNotChangeError`: user did not change contents when `required=True` ([Cyclopts][1])

These map cleanly onto real CLI workflows:

* “abort operation because the user cancelled” (`DidNotSave`/`DidNotChange`)
* “abort because system is misconfigured” (`NotFound`)
* “abort because editor failed” (`EditorError`)

### 13.2.3 Canonical workflow: git-commit-style message editing

Cyclopts’ docs ship a “git commit” mimic that demonstrates the intended UX contract:

* Call `cyclopts.edit(...)` with a prefilled template
* Catch `EditorDidNotSaveError` and `EditorDidNotChangeError` → abort commit (exit 1)
* Post-process the returned text (strip comment lines, strip whitespace) ([Cyclopts][5])

This is the recommended pattern when the “empty message aborts the operation” semantics are desired.

### 13.2.4 Agent-grade usage patterns (stable UX + stable tests)

1. **Template + required message** (commit messages, PR descriptions)

   * Keep defaults (`save=True`, `required=True`) so “no save” and “no change” are distinguishable cancellation signals. ([Cyclopts][1])

2. **Optional editor** (e.g., user may optionally edit a long description)

   * Use `required=False` so “no edits” returns the original text instead of raising. ([Cyclopts][1])

3. **Custom file path for better UX**

   * Set `path` to an application-specific filename (`COMMIT_EDITMSG`, `RELEASE_NOTES.md`, etc.) so the editor UI is clearer; Cyclopts explicitly notes that editors display the file path. ([Cyclopts][1])

4. **Editor arguments**

   * Use `editor_args` to pass editor-specific flags (e.g., “wait” modes or filetype hints) while keeping the “editor selection” logic centralized. ([Cyclopts][1])

### 13.2.5 Composition with Cyclopts execution policy

Because `edit()` is just a regular blocking call returning `str`, it composes naturally with Cyclopts’ `result_action` machinery:

* CLI mode: return the edited string → let `print_non_int_sys_exit` print it and exit 0
* Embedded mode: set `result_action="return_value"` and use the returned string programmatically
  (Your earlier `result_action`/exit-policy section governs this integration.)

---

### Summary “what to standardize” for LLM agents

* Treat `interactive_shell(dispatcher=...)` as the **composition seam**: it’s where you enforce REPL-only policies (telemetry, guardrails, meta injection). ([Cyclopts][1])
* Treat `help_print(tokens=...)` as the **single primitive** for help flows, including “help command” inside REPL. ([Cyclopts][2])
* Treat `edit()` exceptions (`DidNotSave` / `DidNotChange`) as first-class **user cancellation signals** and encode them into deterministic exit semantics. ([Cyclopts][1])

[1]: https://cyclopts.readthedocs.io/en/v4.5.0/api.html "API — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/stable/cookbook/interactive_help.html "Interactive Shell & Help — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/v3.0.0/api.html "API — cyclopts"
[4]: https://github.com/BrianPugh/cyclopts/issues/49?utm_source=chatgpt.com "Interactive shell created according to docs also includes ..."
[5]: https://cyclopts.readthedocs.io/en/v4.2.0/text_editor.html "Text Editor — cyclopts"

# 14) Performance and large-app architecture patterns — Lazy Loading (Cyclopts)

## 14.1 Core capability: register commands by **import-path string** (defer imports)

### Call shape

`App.command()` accepts a **string import path** instead of a callable, which defers importing the command module until the command is actually needed. This is explicitly intended to improve startup time for large CLIs with expensive imports. ([Cyclopts][1])

### Import-path format (exact grammar)

The string is `"module.path:attribute_name"`:

* **module.path**: dot-qualified module to import
* **attribute_name**: retrieved via `getattr(module, attribute_name)` (the Python attribute name, not the CLI name) ([Cyclopts][1])

Examples (all valid):

* `app.command("myapp.commands:create_user")`
* `app.command("myapp.admin.database.operations:migrate")`
* lazy-load an **App instance** from a module: `app.command("myapp.admin:admin_app", name="admin")` ([Cyclopts][1])

### CLI name vs Python attribute name (must be treated as two different namespaces)

* The segment after `:` is the *actual* Python attribute.
* The **CLI-visible command name** is controlled by `name=...`; if omitted, Cyclopts derives it from the Python attribute name via `App.name_transform`. ([Cyclopts][1])

---

## 14.2 Resolution points: **when lazy commands are imported** (exec / help / indexing)

Cyclopts explicitly states that lazy commands are resolved/imported in these situations: ([Cyclopts][1])

1. **Command execution**
   When the user runs that specific command. ([Cyclopts][1])

2. **Help generation**
   When displaying help that **includes** the command. Practically: any operation that needs the command’s signature/docstring/parameters will pull it in. ([Cyclopts][1])

3. **Direct access / indexing**
   Accessing a command node via `app["command_name"]` triggers resolution. This is the canonical “force-import for tests/docs tooling” hook. ([Cyclopts][1])

### Performance corollary (non-negotiable)

To get startup wins, ensure those modules are **not imported by other means** during CLI import (e.g., avoid importing command modules in `__init__.py`, avoid eager “import all commands” registries). Cyclopts calls this out directly. ([Cyclopts][1])

---

## 14.3 Error handling + hardening strategy (lazy imports fail late unless you force them)

Lazy registration does **not** validate the import path at registration time; failures occur when the command is first resolved/executed. Cyclopts shows this explicitly and recommends a test that forces resolution via `app["create"]`. ([Cyclopts][1])

**Agent-grade hardening pattern**

* Add a unit test that enumerates the command tree and calls `app["cmd"]` (and nested subcommands) to ensure every import path is correct.
* Add an additional “help smoke” pass (see §14.7) so docstring/signature extraction doesn’t crash.

---

## 14.4 Large-app patterns (how to structure a fast CLI without losing UX)

### Pattern A — Lazy leaf commands (most common)

Root app imports *no command modules*; it only registers string paths.

```python
# myapp/cli.py
from cyclopts import App

app = App(name="myapp")

user = App(name="user")
user.command("myapp.commands.users:create")
user.command("myapp.commands.users:delete")
user.command("myapp.commands.users:list_users", name="list")
app.command(user)
```

This is exactly Cyclopts’ “Basic Usage” lazy-loading rewrite. ([Cyclopts][1])

**Operational characteristics**

* `myapp` startup is fast.
* First invocation of `myapp user list …` incurs one-time import of `myapp.commands.users`.

### Pattern B — Lazy-load an entire subcommand subtree by importing a sub-App

Expose an `App` instance from a feature module, and register it lazily:

```python
# myapp/admin/__init__.py
from cyclopts import App
admin_app = App(name="admin")
# admin_app.command(...) defined inside this module or imported lazily again

# myapp/cli.py
app.command("myapp.admin:admin_app", name="admin")
```

Cyclopts documents importing an App instance via import path. ([Cyclopts][1])

**When this is best**

* You want feature modules to “own” their subcommand graph.
* You want to lazy-load an entire feature slice at once.

### Pattern C — Two-stage deferral (“lazy command” + “defer heavy imports inside the command body”)

Even with Cyclopts lazy loading, `myapp.commands.users` still imports when that command/help is resolved. If that module itself imports huge dependencies at top-level, you can push those imports **inside** the actual command function to delay further.

This is a widely used CLI technique; Click explicitly documents this approach as “Further Deferring Imports” for lazy CLIs, and the same logic applies to Cyclopts because help is built from signature metadata rather than runtime imports inside the function body. ([Click Documentation][2])

**Rule:** keep “help-critical” metadata cheap (signature, annotations, docstring), and move heavy runtime-only imports under function execution if possible.

---

## 14.5 Lazy-loading gotcha: **Groups are shared objects** (define them in non-lazy modules)

Cyclopts has an explicit “Groups and Lazy Loading” warning:

> Define `Group` objects used by commands in your main CLI module, **NOT** in lazy-loaded modules. ([Cyclopts][1])

### Why this breaks (the failure mode)

If a `Group` object is only defined in an unresolved lazy module, it doesn’t exist until that module is imported. That creates a subtle split-brain behavior:

* If other commands reference the group **by string name** (e.g., `group="Admin Commands"`), Cyclopts can’t attach the real `Group` instance yet.
* Until the lazy module is imported, **validators** and `Group.default_parameter` associated with that group won’t apply to commands elsewhere (because the group object isn’t available). ([Cyclopts][1])
* After the lazy module is imported (by executing one of its commands), the group becomes available and subsequent operations use it. ([Cyclopts][1])

### Safe pattern (authoritative)

Define `Group` objects in an always-imported module (root `cli.py`) and pass the **Group object**, not just a string:

```python
# myapp/cli.py (always imported)
from cyclopts import App, Group, Parameter

admin_group = Group("Admin Commands", validator=require_admin_role)
db_group = Group("Database", default_parameter=Parameter(envvar_prefix="DB_"))

app = App()
app.command("myapp.admin:create_user", group=admin_group)
app.command("myapp.admin:delete_user", group=admin_group)
app.command("myapp.db:migrate", group=db_group)
```

This pattern is lifted directly from Cyclopts’ lazy loading docs. ([Cyclopts][1])

**Agent rule:** if you care about group validators/defaults being applied globally, group objects must be created before any lazy modules are imported; do not “discover groups” by importing feature modules.

---

## 14.6 Lazy loading + shell completion performance (development vs production)

`cyclopts run` dynamic completion imports your script and runs Python on every tab press; Cyclopts explicitly recommends using **Lazy Loading** to mitigate slow imports during development. ([Cyclopts][3])

**Architecture implication**

* If you want a responsive dev UX with `cyclopts run …`, you must keep your root module cheap and rely on lazy command registration for the heavy modules.

---

## 14.7 Test/CI hardening for lazy CLIs (what to pin so upgrades don’t regress you)

### Minimal hardening (Cyclopts-native)

* **Import-path correctness:** force resolution via `app["cmd"]` in tests (Cyclopts shows this exact technique). ([Cyclopts][1])

### Recommended hardening (industry best practice for lazy CLIs)

Lazy loading can yield order-dependent / circular import issues that only show up when resolving help or completion. Click’s guidance for lazy CLIs is to have tests that run `--help` on each subcommand to guarantee everything loads successfully. The same reasoning applies to Cyclopts because Cyclopts also resolves lazy commands during help generation. ([Cyclopts][1])

**Practical harness**

* Enumerate all command paths.
* For each path:

  * force import (`app["name"]` / traverse)
  * run help generation for that path (`help_print(tokens=path)` or equivalent) so docstring/signature parsing is validated.

---

## 14.8 Agent checklist: “make it fast, make it predictable”

1. **Root module must be cheap**
   No eager imports of command modules; only import Cyclopts + shared policy objects. ([Cyclopts][1])

2. **Register commands by import-path string**
   `"module.path:attr"`; use `name=` for CLI naming. ([Cyclopts][1])

3. **Know the three resolution points**
   exec / help / direct access; design tests around them. ([Cyclopts][1])

4. **Define shared `Group` objects outside lazy modules**
   Otherwise validators/defaults won’t apply consistently until some command happens to import the group. ([Cyclopts][1])

5. **If help must be instant, keep help metadata cheap**
   Consider two-stage deferral (lazy module + heavy imports inside command body) when possible. ([Click Documentation][2])

[1]: https://cyclopts.readthedocs.io/en/latest/lazy_loading.html "Lazy Loading — cyclopts"
[2]: https://click.palletsprojects.com/en/stable/complex/ "Complex Applications — Click Documentation (8.3.x)"
[3]: https://cyclopts.readthedocs.io/en/latest/shell_completion.html "Shell Completion — cyclopts"

# 15) Advanced composition patterns (Cyclopts)

## 15.0 Unifying mental model: “compose by intercepting *tokens → (command, bound, ignored) → invoke → result_action*”

Most “advanced composition” in Cyclopts comes from deliberately taking control of one of these seams:

1. **Launch seam**: run **`app.meta()`** instead of `app()` to introduce a wrapper CLI layer whose help is merged with the parent app, and which can forward tokens into the parent. ([Cyclopts][1])
2. **Invocation seam**: replicate the core `App.__call__` logic yourself—`parse_args()` gives you `(command, bound, ignored)` and then you decide how to invoke and what extra kwargs to inject. ([Cyclopts][1])
3. **Return seam**: `result_action` pipelines allow multi-step postprocessing (notably `call_if_callable` for dataclass commands). ([Cyclopts][2])

Everything below is basically a specialized application of those seams.

---

## 15.1 Meta App: launching an app from an app (merged help + launch-pipeline control)

### 15.1.1 What `app.meta` is (and why it exists)

Cyclopts’ **meta app** is “just like a normal Cyclopts App,” except its help page is **merged** with its parent’s help page. It’s the intended hook when you want “more control over the application launch process.” ([Cyclopts][1])

Canonical shape:

```python
from cyclopts import App, Group, Parameter
from typing import Annotated

app = App()

# Optional: rename the meta parameter panel and pin it higher in help ordering.
app.meta.group_parameters = Group("Session Parameters", sort_key=0)

@app.command
def foo(loops: int):
    ...

@app.meta.default
def launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    user: str,
):
    print(f"Hello {user}")
    app(tokens)  # forward remaining tokens into the primary app

if __name__ == "__main__":
    app.meta()
```

Key invariants:

* `*tokens` aggregates all remaining tokens, **including** those beginning with `-` (options), because you set `allow_leading_hyphen=True`. ([Cyclopts][1])
* `show=False` hides the forwarding catch-all from help, so you don’t expose “tokens” as a user-facing parameter. ([Cyclopts][1])
* The meta app is additionally scanned when generating help screens, producing merged help output. ([Cyclopts][1])

### 15.1.2 Meta commands: “bypass the launcher”

If you register commands on `app.meta`, those commands run **instead of** the meta default launcher, letting you create “setup/info” commands that don’t require session parameters like `--user`. ([Cyclopts][1])

```python
@app.meta.command
def info():
    print("Runs without requiring --user")
```

### 15.1.3 Launch pipeline control: custom invocation via `parse_args()` + `ignored`

Cyclopts spells out the core `__call__` logic:

```python
command, bound, ignored = self.parse_args(tokens, **kwargs)
return command(*bound.args, **bound.kwargs)
```

…and explicitly encourages using this to customize invocation. ([Cyclopts][1])

The meta-app doc shows the canonical pattern for **session object injection**:

* downstream command declares a keyword-only parameter annotated with `Parameter(parse=False)`
* Cyclopts doesn’t bind it; instead it appears in `ignored`
* the launcher checks `ignored` and injects the constructed object at call time ([Cyclopts][1])

```python
from cyclopts import App, Parameter
from typing import Annotated

app = App()

class User:
    def __init__(self, name: str):
        self.name = name

@app.command
def create(age: int, *, user_obj: Annotated[User, Parameter(parse=False)]):
    print(f"Creating user {user_obj.name} with age {age}.")

@app.meta.default
def launcher(
    *tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)],
    user: str,
):
    command, bound, ignored = app.parse_args(tokens)
    extra = {}
    if "user_obj" in ignored:
        extra["user_obj"] = ignored["user_obj"](user)
    return command(*bound.args, **bound.kwargs, **extra)

app.meta()
```

Critical constraints:

* `parse=False` parameters must be **keyword-only** (explicitly called out in the meta-app docs). ([Cyclopts][3])
* `ignored` maps **python-variable-name → type annotation** for `parse=False` params, which is why `ignored["user_obj"]` is the `User` class. ([Cyclopts][1])

**LLM-agent takeaway:** Meta App is the sanctioned way to build a “launcher pipeline” that (a) adds session/global flags, (b) merges help, (c) forwards into the real command graph, and (d) can inject expensive/shared objects without polluting every command signature.

---

## 15.2 Sharing parameters across commands: “context-ish dataclass” + namespace flattening

Cyclopts documents two approaches to “shared configuration” across commands:

1. via a meta app (powerful but “heavy-handed and clunky”), or
2. via a common dataclass passed to each command (simpler/terser). ([Cyclopts][4])

### 15.2.1 The canonical pattern: flattened dataclass `Common` + per-command kw-only param

Cyclopts’ cookbook shows:

* define a dataclass `Common` for shared flags (URL/port/verbosity)
* decorate it with `@Parameter(name="*")` to **flatten** the namespace so flags appear as `--url`, `--port` instead of `--common.url`, `--common.port`
* commands accept `*, common: Common | None = None` and instantiate defaults if not provided ([Cyclopts][4])

```python
from cyclopts import App, Parameter
from cyclopts.types import UInt16
from dataclasses import dataclass
from functools import cached_property
from httpx import Client

@Parameter(name="*")  # flatten: --url not --common.url
@dataclass
class Common:
    url: str = "http://cyclopts.readthedocs.io"
    port: UInt16 = 8080
    verbose: bool = False

    @property
    def base_url(self) -> str:
        return f"{self.url}:{self.port}"

    @cached_property
    def client(self) -> Client:
        return Client(base_url=self.base_url)

app = App()

@app.command
def create(name: str, age: int, *, common: Common | None = None):
    if common is None:
        common = Common()
    common.client.post("/users", json={"name": name, "age": age})
```

The help output for a command shows the flattened flags as top-level parameters (`--url`, `--port`, …), confirming the namespace flattening contract. ([Cyclopts][4])

### 15.2.2 Why this is “context-ish” (and when it beats Meta App)

This dataclass approach is ideal when:

* you want **shared options** and maybe a lazily-created client (`cached_property`) but don’t need cross-command session orchestration
* you want each command to remain independently callable/testing-friendly (just pass `common=Common(...)`)

When you *do* need cross-command session semantics (reuse a created client across multiple chained commands), jump back to Meta App + `parse=False` injection (§15.1.3). ([Cyclopts][1])

---

## 15.3 Dataclass-as-command pattern: command objects + `call_if_callable` pipeline

Cyclopts supports registering a **dataclass as a command** (instead of a function). The dataclass holds parsed state, and its `__call__` performs the action. ([Cyclopts][2])

### 15.3.1 The required `result_action` pipeline

Cyclopts recommends setting:

```python
app = App(result_action=["call_if_callable", "print_non_int_sys_exit"])
```

…and explains this pipeline:

1. Cyclopts parses CLI into an instance of the dataclass
2. `call_if_callable` calls the instance if it has `__call__`
3. `print_non_int_sys_exit` prints returned string (or exits with int/bool semantics) ([Cyclopts][2])

Canonical example: ([Cyclopts][2])

```python
from dataclasses import dataclass, KW_ONLY
from cyclopts import App

app = App(result_action=["call_if_callable", "print_non_int_sys_exit"])

@app.command
@dataclass
class Greet:
    name: str = "World"
    _: KW_ONLY
    formal: bool = False

    def __call__(self):
        greeting = "Hello" if self.formal else "Hey"
        return f"{greeting} {self.name}."
```

**Agent-grade uses**

* “Command object” that carries normalized state and can be reused programmatically
* Natural fit for richer commands with multiple methods (e.g., `validate()`, `run()`, `render()`), while CLI still targets `__call__`

---

## 15.4 Command chaining: compose your own multi-command scripting layer

Cyclopts explicitly does **not** natively support command chaining, preferring robust parsing. But it provides the tools to build it yourself, and documents a canonical “delimiter token” pattern. ([Cyclopts][5])

### 15.4.1 Canonical implementation (meta-app + `allow_leading_hyphen` capture)

Cyclopts’ documented pattern:

* meta default captures all remaining tokens with `allow_leading_hyphen=True`
* split tokens on a delimiter (e.g., `"AND"`)
* execute each group via `app(group)` ([Cyclopts][5])

```python
import itertools
from cyclopts import App, Parameter
from typing import Annotated

app = App()

@app.command
def foo(val: int): ...

@app.command
def bar(flag: bool): ...

@app.meta.default
def main(*tokens: Annotated[str, Parameter(show=False, allow_leading_hyphen=True)]):
    delimiter = "AND"
    groups = [list(g) for is_delim, g in itertools.groupby(tokens, lambda t: t == delimiter) if not is_delim] or [[]]
    for group in groups:
        app(group)

app.meta()
```

**Safe-by-default enhancements (recommended)**
The doc’s example calls `app(group)` directly (which may `sys.exit` depending on your policy). For deterministic chaining:

* override `exit_on_error=False` and choose a non-exiting `result_action` when invoking each segment (so one bad segment doesn’t terminate the whole launcher unless you want it to)
* record per-segment failures (CycloptsError subclass + token slice) for better UX/telemetry

(Those enhancements build on Cyclopts’ documented meta-app invocation seam and the fact that `App.__call__` is essentially `parse_args → invoke`.) ([Cyclopts][1])

---

## 15.5 AutoRegistry integration: “string → implementation” selection (plugin/provider pattern)

Cyclopts documents AutoRegistry as a companion library that “automatically creates string-to-functionality mappings,” making it trivial to select implementations from CLI parameters. ([Cyclopts][6])

### 15.5.1 Minimal pattern: registry keys become `Literal` choices

The Cyclopts AutoRegistry doc demonstrates turning a provider selector into:

```python
provider: Literal[tuple(_downloaders)] = "gcp"
```

So the CLI gets:

* **help** shows `[choices: gcp,s3,azure]` and a default
* selection is purely via a string key, then lookup the callable and invoke it ([Cyclopts][6])

It contrasts “manual dict mapping” with an AutoRegistry `Registry(prefix="_download_")` that registers functions via decorator and reduces duplication / central mapping boilerplate. ([Cyclopts][6])

### 15.5.2 Why this composes extremely well with Cyclopts

* Cyclopts already has first-class support for **`Literal` choice types** in help and completion pipelines (so registry keys surface naturally). ([Cyclopts][6])
* AutoRegistry makes the mapping self-contained: provider implementations register themselves, avoiding “update central dict in another file” failure modes. ([Cyclopts][6])

**Agent-grade extension**

* Use one registry per extension point (storage backends, auth providers, exporters)
* Use prefixes / naming conversion rules to enforce a CLI naming schema (AutoRegistry emphasizes name schema enforcement/conversion rules as configurable features). ([GitHub][7])

---

## 15.6 Composition test matrix (what to pin so agents don’t regress behavior)

If your CLI uses these patterns heavily, pin these invariants with golden tests:

* **Meta help merge**: root `--help` includes meta parameter panel (renamed) and commands list. ([Cyclopts][1])
* **Launcher forwarding**: `*tokens` capture includes `--flag` tokens due to `allow_leading_hyphen=True`. ([Cyclopts][1])
* **Injection contract**: `parse=False` params appear in `ignored` and can be injected by meta dispatcher. ([Cyclopts][1])
* **Flattened shared dataclass**: help shows `--url/--port` (not `--common.url`), and commands accept `common: Common|None`. ([Cyclopts][4])
* **Dataclass command pipeline**: without `call_if_callable`, you’d print the dataclass instance; with pipeline, you call `__call__`. ([Cyclopts][8])
* **AutoRegistry choices**: help shows provider choices derived from registry keys. ([Cyclopts][6])

[1]: https://cyclopts.readthedocs.io/en/v3.3.0/meta_app.html "Meta App — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/stable/cookbook/dataclass_commands.html "Dataclass Commands — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/v3.3.0/meta_app.html?utm_source=chatgpt.com "Meta App — cyclopts - Read the Docs"
[4]: https://cyclopts.readthedocs.io/en/v3.24.0/cookbook/sharing_parameters.html "Sharing Parameters — cyclopts"
[5]: https://cyclopts.readthedocs.io/en/latest/command_chaining.html "Command Chaining — cyclopts"
[6]: https://cyclopts.readthedocs.io/en/v4.5.0/autoregistry.html "AutoRegistry — cyclopts"
[7]: https://github.com/BrianPugh/autoregistry?utm_source=chatgpt.com "BrianPugh/autoregistry: Automatic registry design-pattern ..."
[8]: https://cyclopts.readthedocs.io/_/downloads/en/v4.2.2/pdf/?utm_source=chatgpt.com "Release 4.2.2 Brian Pugh"

# 16) Docs generation integrations (Sphinx + MkDocs)

## 16.1 Sphinx integration (`cyclopts.sphinx_ext` + `.. cyclopts:: ...`)

### 16.1.1 Enable the extension + directive

1. Add the extension in `docs/conf.py`:

```python
extensions = [
  "cyclopts.sphinx_ext",
  # optionally:
  "sphinx.ext.napoleon",  # NumPy-style docstrings (common with Cyclopts examples)
]
```

2. Use the directive in an `.rst` file:

```rst
.. cyclopts:: mypackage.cli:app
```

This is the supported “happy path” for Sphinx docgen. ([cyclopts.readthedocs.io][1])

### 16.1.2 Module path formats (explicit vs auto-discovery)

The directive accepts two formats: ([cyclopts.readthedocs.io][1])

* **Explicit**: `module.path:app_name` (documents that specific `App` object)
* **Auto-discovery**: `module.path` (extension scans the module for an `App` instance, looking for common names like `app`, `cli`, `main`)

**Agent-grade constraint:** auto-discovery is convenient, but explicit `module:app` is more stable under refactors and better for multi-App modules. ([cyclopts.readthedocs.io][1])

### 16.1.3 Directive option surface (exact knobs + semantics)

**Heading topology controls**

* `:heading-level:` start heading level (1–6, default 2). ([cyclopts.readthedocs.io][1])
* `:max-heading-level:` cap deepest heading level (1–6, default 6) to avoid tiny headings in deep command trees. ([cyclopts.readthedocs.io][1])
* `:flatten-commands:` render *all* commands/subcommands at the same heading level (flat hierarchy vs nested). ([cyclopts.readthedocs.io][1])

**Graph traversal**

* `:no-recursive:` disables recursive subcommand documentation (default is recursive). ([cyclopts.readthedocs.io][1])

**Visibility**

* `:include-hidden: true` includes commands marked `show=False` (default is to exclude). ([cyclopts.readthedocs.io][1])

**Command selection filters**

* `:commands:` filter to a subset of commands (supports nested paths via dot notation like `db.migrate`, and normalizes underscores/dashes). ([cyclopts.readthedocs.io][1])
* `:exclude-commands:` exclude specific commands (supports nested dot paths; intended for internal/debug commands). ([cyclopts.readthedocs.io][1])

**Preamble control**

* `:skip-preamble:` when filtering to a single command, suppresses the generated description/usage block so you can write your own intro and still auto-render parameters/subcommands. ([cyclopts.readthedocs.io][1])

**Title rendering**

* `:code-block-title:` renders command titles as inline code (monospace) to make command names visually distinct. ([cyclopts.readthedocs.io][1])

### 16.1.4 Automatic reference labels (cross-linkable anchors)

The directive automatically generates RST reference labels for every command. Anchor format:
`cyclopts-{app-name}-{command-path}` (e.g., `cyclopts-myapp-deploy-production`). You can then link with `:ref:`cyclopts-myapp-deploy``. ([cyclopts.readthedocs.io][1])

**Why this matters for large CLIs:** this gives you stable cross-page deep links even if you split docs across multiple pages/sections (and reduces anchor collisions when documenting multiple CLIs). ([cyclopts.readthedocs.io][1])

### 16.1.5 Page structuring patterns (what to generate, where)

Cyclopts’ own Sphinx guide recommends two common “information architecture” layouts:

* **Flat headings for navigability**: `:flatten-commands:` + `:code-block-title:` yields distinct command headings and easy cross-referencing. ([cyclopts.readthedocs.io][1])
* **Selective documentation**: use `:commands:` / `:exclude-commands:` to split docs by domain (e.g., `db`, `api`, `dev`), and keep internal commands out of user-facing docs. ([cyclopts.readthedocs.io][1])

### 16.1.6 Programmatic doc generation (`app.generate_docs`)

Sphinx integration also documents generating docs outside Sphinx via `app.generate_docs(output_format=...)` (examples include `"rst"`, `"markdown"`, `"html"`), useful for README generation or other doc systems. ([cyclopts.readthedocs.io][1])

---

## 16.2 MkDocs integration (experimental plugin + `::: cyclopts` blocks)

### 16.2.1 Install + enable plugin

* Install extra: `pip install cyclopts[mkdocs]`. ([cyclopts.readthedocs.io][2])
* Add plugin in `mkdocs.yml`:

```yaml
plugins:
  - search
  - cyclopts
```

Cyclopts explicitly warns the MkDocs plugin is **experimental** and may introduce breaking changes. ([cyclopts.readthedocs.io][2])

### 16.2.2 Directive syntax in Markdown

MkDocs pages use a block form:

```md
::: cyclopts
    module: myapp.cli:app
    heading_level: 2
    recursive: true
```

The `module` option is required. ([cyclopts.readthedocs.io][2])

### 16.2.3 Module path formats (same two-mode contract as Sphinx)

* Explicit: `module.path:app_name`
* Auto-discovery: `module.path` scanning for common `App` names like `app`, `cli`, `main` ([cyclopts.readthedocs.io][2])

### 16.2.4 MkDocs option surface (YAML keys; analogous to Sphinx)

MkDocs options are the “same idea, different spelling”:

* `heading_level` (1–6; default 2) ([cyclopts.readthedocs.io][2])
* `max_heading_level` (1–6; default 6) ([cyclopts.readthedocs.io][2])
* `recursive: true|false` (default true) ([cyclopts.readthedocs.io][2])
* `include_hidden: true|false` (include `show=False`) ([cyclopts.readthedocs.io][2])
* `flatten_commands: true|false` ([cyclopts.readthedocs.io][2])
* `generate_toc: true|false` (default true; suppress when you have your own nav or multiple directives per page) ([cyclopts.readthedocs.io][2])
* `code_block_title: true|false` (render headings as backticked inline code) ([cyclopts.readthedocs.io][2])
* `commands:` list (or inline YAML list) of command paths (supports dot paths like `db.migrate`, underscore/dash normalization) ([cyclopts.readthedocs.io][2])
* `exclude_commands:` list of command paths to omit ([cyclopts.readthedocs.io][2])
* `skip_preamble: true|false` when filtering to a single command and you provide your own Markdown heading/introduction ([cyclopts.readthedocs.io][2])

### 16.2.5 “Experimental” risk management (what to pin)

Because Cyclopts explicitly flags this plugin as experimental, treat the rendered output as a versioned contract:

* snapshot generated Markdown output (or at least section headers + command lists)
* pin `cyclopts` version in docs build environments
* maintain a tiny “mkdocs build smoke test” in CI that renders one `::: cyclopts` block for a representative command subtree ([cyclopts.readthedocs.io][2])

---

# 17) Testing and QA (unit testing cookbook + regression suite inventory)

## 17.1 Unit-testing cookbook: canonical patterns Cyclopts explicitly recommends

### 17.1.1 Mocking: isolate CLI glue from business logic

Cyclopts’ cookbook pattern: put real logic in a pure function, then unit-test the Cyclopts-decorated CLI wrapper by mocking the pure function. The example uses `pytest-mock` and a fixture that patches the business function. ([cyclopts.readthedocs.io][3])

### 17.1.2 Exit codes: default `result_action` raises `SystemExit`

Cyclopts notes that default `result_action="print_non_int_sys_exit"` calls `sys.exit()` based on return value (True→0, False→1). Therefore, tests should `pytest.raises(SystemExit)` and assert `.code`. ([cyclopts.readthedocs.io][3])

### 17.1.3 Stdout checks: override `result_action` to avoid `sys.exit`

To test printed output, Cyclopts recommends calling the app with `result_action="return_value"` (so you can assert both the return value and captured stdout) and using `capsys.readouterr().out`. ([cyclopts.readthedocs.io][3])

Cyclopts also explicitly reminds:

* normal output goes to `console` (stdout)
* errors go to `error_console` (stderr) ([cyclopts.readthedocs.io][3])

### 17.1.4 Env var injection tests: use `monkeypatch.setenv` + `parse_args([])`

If the app is configured with `cyclopts.config.Env`, you can unit-test env var parsing by setting env vars and calling `App.parse_args([])` (empty token list) to parse without invoking the command. ([cyclopts.readthedocs.io][3])

### 17.1.5 “No tokens” warning under pytest: always pass `[]` or `""`

Cyclopts warns that calling `app()` or `app.parse_args()` with no argument reads from `sys.argv` (rarely what you want in a test) and emits a pytest-oriented warning (“Did you mean `app([])`?”). Correct way to supply “no CLI arguments” is `app([])` (or an empty string/list). ([cyclopts.readthedocs.io][3])

### 17.1.6 File config tests: `tmp_path` + `monkeypatch.chdir`

To test config file loading, create config files in a temporary directory and change cwd to that directory (because discovery is relative to invocation cwd). Then call `app.parse_args([])` and assert bound values. ([cyclopts.readthedocs.io][3])

### 17.1.7 Help output assertions: deterministic Rich Console fixture + capsys (optional)

Cyclopts warns help-page snapshot testing is often overkill, but shows how to do it deterministically:

* define a `Console(width=70, force_terminal=True, highlight=False, color_system=None, legacy_windows=False)` fixture
* call `app("--help", console=console)` and capture stdout with `capsys` ([cyclopts.readthedocs.io][3])
  It also notes you can use `Console.capture()` as an alternative capture mechanism. ([cyclopts.readthedocs.io][3])

---

## 17.2 Regression suite inventory (what to codify as “don’t break this” tests)

### 17.2.1 Known Issues: `from __future__ import annotations` / stringized type hints

Cyclopts’ **Known Issues** page explicitly documents long-standing limitations with `from __future__ import annotations` (PEP 563), including breakage with cross-module dataclass inheritance, driven by CPython `typing.get_type_hints()` bugs and the shifting direction of PEP 649/749. ([cyclopts.readthedocs.io][4])

**Regression tests to add (if you use these patterns):**

* a minimal multi-module dataclass inheritance fixture under future annotations
* ensure either (a) Cyclopts works for your supported Python versions, or (b) you detect and fail fast with a clear error message (vs obscure NameError)

There is a concrete issue report demonstrating a `NameError: Annotated is not defined` failure mode specifically tied to `@Parameter @dataclass` + cross-file inheritance under `from __future__ import annotations`. ([GitHub][5])

### 17.2.2 Default command help UX edge: `@app.default` function name in help

There is an open issue noting that help output may show the name of the `@app.default` function (e.g., “Usage: main COMMAND”), which can be confusing for CLIs that intend the default entrypoint to be anonymous. If your docs/UX depend on hiding that name, pin help output behavior (or adopt the “underscore function name” workaround). ([GitHub][6])

### 17.2.3 Config + group-validator semantics (meta app / pyproject overrides)

A long-standing discussion highlights that config-loaded values (e.g., from `pyproject.toml`) can interact awkwardly with “specified argument” logic used by group validators like `LimitedChoice`/`MutuallyExclusive`, and that meta-app params often need nullable defaults to detect “was provided by CLI.” If you rely on config defaults + group validators, codify a regression test where config sets an invalid combination (e.g., both `verbose` and `quiet` true) and ensure your app catches it (often via an explicit post-config validation step). ([GitHub][7])

### 17.2.4 Version-specific behavior hazards you should pin

Even if not “known issues,” these are typical drift points worth snapshot/regression tests when you ship CLIs as products:

* help rendering snapshots under a deterministic console fixture (Cyclopts explicitly documents environment-sensitive Rich output) ([cyclopts.readthedocs.io][3])
* docs generation output for Sphinx directive options you rely on (`:flatten-commands:`, `:commands:`, anchor labels) ([cyclopts.readthedocs.io][1])
* MkDocs plugin output (plugin is explicitly experimental) ([cyclopts.readthedocs.io][2])

---

## 17.3 “Agent-ready” QA harness blueprint (what to automate)

If you want a single unified harness that LLM agents can keep stable across refactors:

1. **Parse-only contract tests**: `parse_args([])` / `parse_args(tokens)` for each command path; assert `command` identity + `bound.arguments` schema. ([cyclopts.readthedocs.io][3])
2. **Execution contract tests**: run representative commands with `result_action="return_value"` and `exit_on_error=False` so failures surface as exceptions. ([cyclopts.readthedocs.io][3])
3. **I/O contract tests**: assert stdout/stderr separation (capsys `.out` / `.err`), and optionally supply a custom `error_console` to capture rich errors. ([cyclopts.readthedocs.io][3])
4. **Config contract tests**: `monkeypatch.setenv` + tmp config file + chdir; assert precedence and that validators still enforce invariants when defaults originate from config. ([cyclopts.readthedocs.io][3])
5. **Docs contract tests**: for at least one representative subtree, snapshot:

   * Sphinx directive output headings + generated reference labels (`cyclopts-{app}-{path}`) ([cyclopts.readthedocs.io][1])
   * MkDocs directive output headings / TOC behavior (`generate_toc`) ([cyclopts.readthedocs.io][2])

[1]: https://cyclopts.readthedocs.io/en/latest/sphinx_integration.html "Sphinx Integration — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/latest/mkdocs_integration.html "MkDocs Integration — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/latest/cookbook/unit_testing.html "Unit Testing — cyclopts"
[4]: https://cyclopts.readthedocs.io/en/latest/known_issues.html "Known Issues — cyclopts"
[5]: https://github.com/BrianPugh/cyclopts/issues/439 "`NameError: name 'Annotated' is not defined` · Issue #439 · BrianPugh/cyclopts · GitHub"
[6]: https://github.com/BrianPugh/cyclopts/issues/308 "Name of @app.default function appears in help · Issue #308 · BrianPugh/cyclopts · GitHub"
[7]: https://github.com/BrianPugh/cyclopts/issues/87 "[Discussion] First-class config overrides via pyproject.toml? · Issue #87 · BrianPugh/cyclopts · GitHub"
