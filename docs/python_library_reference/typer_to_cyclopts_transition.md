Here’s a Cyclopts “field manual” tuned for an AI agent that:

* Already understands Typer and Click deeply
* Is operating inside a large, fully-typed Python codebase
* Must keep **function signatures simple** and **semantics explicit**

I’ll structure this around *how you should think and operate as an LLM refactoring Typer → Cyclopts*, not as a generic tutorial.

---

## 0. Cyclopts in one paragraph (for your internal model)

Cyclopts is a Typer-inspired CLI framework that:

* Uses **`typing.Annotated` + `Parameter` metadata** instead of proxy default values in function signatures.([cyclopts.readthedocs.io][1])
* Implements its **own parser** (not Click) with semantics that directly mirror Python’s function argument binding rules.([cyclopts.readthedocs.io][2])
* Has first-class support for complex type hints (including `Union`, `Literal`, Pydantic, dataclasses, attrs, TypedDict).([cyclopts.readthedocs.io][3])
* Provides rich help, validation, config files & environment variable integration, and test-friendly embedding.([cyclopts.readthedocs.io][4])

It is explicitly designed to fix the main “Typer pain points” around signatures, typing, and configurability.([cyclopts.readthedocs.io][1])

---

## 1. Core conceptual shift: from Typer-style proxies to Annotated metadata

### 1.1 What changes compared to Typer

Typer decorates functions and injects **proxy default values** (`Option(...)`, `Argument(...)`). That makes the function hard to call outside the CLI and confuses static analyzers.([cyclopts.readthedocs.io][1])

Cyclopts instead:

* Leaves default values alone.
* Attaches CLI metadata via `typing.Annotated[..., Parameter(...)]`.([cyclopts.readthedocs.io][5])

**As an AI agent**, your invariant during migration should be:

> After refactor, every CLI function must remain a *perfectly normal* Python function to call in tests or non-CLI code.

### 1.2 Minimal example of the pattern

Typer:

```python
# BEFORE (Typer)
from typing_extensions import Annotated
import typer

app = typer.Typer()

@app.command()
def run(
    path: Annotated[str, typer.Argument(help="Path to repo")],
    workers: Annotated[int, typer.Option("--workers", "-w", help="Worker count")] = 4,
) -> None:
    ...
```

Cyclopts:

```python
# AFTER (Cyclopts)
from typing import Annotated
from cyclopts import App, Parameter

app = App()

@app.command
def run(
    path: str,  # positional by signature
    workers: Annotated[int, Parameter(name=["--workers", "-w"], help="Worker count")] = 4,
) -> None:
    ...
```

Key points:

* No proxy defaults; `workers` has a literal default `4`.
* CLI metadata is entirely in `Annotated[..., Parameter(...)]`.
* Whether something is positional vs option is determined by **signature shape**, not what helper class you use.([cyclopts.readthedocs.io][6])

---

## 2. Cyclopts primitives you must master

### 2.1 `App`: the main orchestration object

`App` is the replacement for `typer.Typer`. The migration table explicitly maps `Typer()` → `App()`.([cyclopts.readthedocs.io][6])

Important constructor knobs for an advanced codebase:

* `name`: application name (affects help, env naming, etc.).([cyclopts.readthedocs.io][6])
* `help`: top-level help text if you don’t want to rely on default docstring extraction.([cyclopts.readthedocs.io][2])
* `default_parameter`: app-wide defaults for `Parameter` (ex: globally disabling `--no-flag`).([cyclopts.readthedocs.io][7])
* `config`: config source (files, env, in-memory dict).([cyclopts.readthedocs.io][4])
* `result_action`: how to interpret command return values (print, `sys.exit`, return, call if callable, etc.).([cyclopts.readthedocs.io][8])

LLM recommendation:

* For **normal CLI scripts**: keep the default `result_action` (prints, maps bools to exit codes, etc.).([cyclopts.readthedocs.io][8])
* For **embedding in tests or orchestrators**: construct a *second* `App` or call with `result_action="return_value"` so parsing doesn’t `sys.exit`.([cyclopts.readthedocs.io][8])
* Config precedence (CodeIntel): CLI flags > environment (`CODEINTEL_*`) > TOML file (`config/codeintel.toml` or `CODEINTEL_CONFIG_PATH`) > signature defaults. The root app wires both `Env("CODEINTEL_")` and an optional TOML loader.
* Quick harness patterns:
  ```python
  from codeintel.cli.cyclopts_app import app

  # Parse/execute without exiting (sync)
  ns = app(["build", "status", "--help"], result_action="return_value")

  # Async contexts
  # await app.run_async(["op", "function-summary", ...], result_action="return_value")
  ```

### 2.2 `@app.command`, `@app.default`, `@app.meta`

Mapping from Typer:

| Typer concept                                 | Cyclopts equivalent              | Notes                                                                                 |
| --------------------------------------------- | -------------------------------- | ------------------------------------------------------------------------------------- |
| `@app.command()`                              | `@app.command`                   | Always creates a named command.([cyclopts.readthedocs.io][6])                         |
| `@app.callback()` as “no command -> run this” | `@app.default`                   | Default action when no subcommand is given.([cyclopts.readthedocs.io][2])             |
| `@app.callback()` as global pre-hook          | `app.meta` + `@app.meta.default` | “Meta app” for custom pre-parsing or global behaviours.([cyclopts.readthedocs.io][9]) |

As an agent, you should *never* retain Typer callbacks. Systematically:

1. Replace “default action” callbacks with `@app.default`.
2. Replace “pre-execution logic” with `app.meta`/meta-commands only if truly required.

---

## 3. Parameters and `Annotated` – your main lever

The `Parameter` class is where almost all CLI-level control lives. It is always attached via `typing.Annotated`.([cyclopts.readthedocs.io][5])

### 3.1 Basic form

```python
from typing import Annotated
from cyclopts import Parameter

def cmd(
    path: Annotated[Path, Parameter(help="Codebase root")],
    verbose: Annotated[bool, Parameter()] = False,
) -> None:
    ...
```

Authority:

* The docs explicitly say `Parameter` “gives complete control” over how a parameter is processed and can be used both as an Annotated payload and as a decorator for classes.([cyclopts.readthedocs.io][5])

### 3.2 Positional vs option is purely signature-based

Typer:

* `Argument(...)` vs `Option(...)` choose positional vs option.

Cyclopts:

* Positional vs keyword is based on **standard Python rules**:

  * positional (no `*`), then
  * keyword-only (after `*`), etc.([cyclopts.readthedocs.io][2])

As migration logic:

* If Typer used `Argument`, make sure the parameter is **positional** in the function signature.
* If Typer used `Option`, make it keyword-only (put it after `*`) or give it a default.

Example:

```python
@app.command
def run(
    repo_root: Path,   # from typer.Argument
    *,                 # everything below is options
    snapshot: str,     # becomes --snapshot
    workers: int = 4,  # --workers
) -> None:
    ...
```

You only attach `Parameter` when you need more control than the default naming/coercion.

### 3.3 Naming, aliases, and flag behavior

`Parameter` handles:

* `name`: explicit long/short names (`["--workers", "-w"]`).([cyclopts.readthedocs.io][5])
* Aliases.
* `negative` / `negative_bool`: how “negative flags” such as `--no-foo` are generated or disabled.([cyclopts.readthedocs.io][6])

As the docs highlight:

* Typer’s behaviour for disabling the “false” flag is quite indirect.
* Cyclopts gives you explicit knobs (`negative=""`, `negative_bool="--disable-"` etc.).([cyclopts.readthedocs.io][6])

LLM migration pattern for bool options:

1. Maintain the original `true` flag name.
2. Inspect Typer’s `Option(..., "--flag/--no-flag")` etc.
3. Encode the same semantics using `Parameter(negative=...)` or `Parameter(negative_bool=...)`.

Example:

```python
from typing import Annotated
from cyclopts import Parameter

def cmd(
    *,
    my_flag: Annotated[
        bool,
        Parameter("--my-flag", negative_bool="--disable-"),
    ] = False,
) -> None:
    ...
```

### 3.4 Validation and converters

Cyclopts has a dedicated `validators` module and built-in convenience types.([cyclopts.readthedocs.io][10])

* **Per-parameter validation:** `Parameter(validator=validators.Path(exists=True))`, custom callables, etc.([cyclopts.readthedocs.io][10])
* **Converters:** `Parameter(converter=...)` for custom parsing.
* You can **compose multiple `Parameter` annotations** by stacking `Annotated`, and there are pre-aliased types such as “byte size” or `ExistingPath`.([cyclopts.readthedocs.io][11])

For a project like CodeIntel, this is a strong pattern:

```python
ByteSize = Annotated[int, Parameter(converter=parse_byte_units)]
ExistingDir = Annotated[
    Path,
    Parameter(validator=validators.Path(exists=True, is_dir=True)),
]
```

Then just:

```python
def cmd(
    *,
    cache_dir: ExistingDir,
    max_ram: ByteSize,
) -> None:
    ...
```

### 3.5 App-wide defaults for `Parameter`

`App.default_parameter` lets you set global defaults once (for example, for negative flags, requiredness, or shorthand behaviours).([cyclopts.readthedocs.io][7])

```python
app = App(
    default_parameter=Parameter(
        negative=(),          # turn off --no-foo globally
        show_default=True,    # always show defaults in help
    ),
)
```

As an LLM agent, prefer:

* Use `default_parameter` for **policy-level** decisions.
* Use per-parameter `Parameter(...)` only for deviations from policy.

This keeps your CLI behaviour consistent and your code base compressible.

---

## 4. User classes & dataclass commands: the answer to “too many parameters”

You explicitly want to keep **function complexity low** and move arguments into helper objects/configs. Cyclopts’ “User Classes” and “Dataclass Commands” are built exactly for this purpose.([cyclopts.readthedocs.io][12])

### 4.1 User classes (dataclasses, Pydantic, attrs, TypedDict…)

Cyclopts can parse a **dataclass / attrs class / Pydantic model / NamedTuple / TypedDict** from CLI arguments into a single typed object parameter.([cyclopts.readthedocs.io][3])

Example pattern:

```python
from dataclasses import dataclass
from pathlib import Path
from cyclopts import App

app = App(name="codeintel")

@dataclass
class PipelineConfig:
    repo_root: Path
    snapshot: str
    enable_graphs: bool = True
    enable_analytics: bool = True

@app.command
def run(cfg: PipelineConfig) -> None:
    run_pipeline(cfg)  # your core logic
```

Cyclopts automatically:

* Produces nested help like `CFG.REPO_ROOT`, `CFG.SNAPSHOT`, etc.
* Parses CLI input into a `PipelineConfig` instance.([cyclopts.readthedocs.io][3])

If you want **flat flags** (`--repo-root` instead of `--cfg.repo-root`), you can use `Parameter(name="*")` to flatten the namespace.([cyclopts.readthedocs.io][3])

```python
from typing import Annotated
from cyclopts import Parameter

@app.command
def run(cfg: Annotated[PipelineConfig, Parameter(name="*")]) -> None:
    run_pipeline(cfg)
```

This is ideal for your “single argument” requirement:

* The CLI command has many parameters.
* The Python function has **one** config object.
* All of it is explicitly typed and static-analysis-friendly.

### 4.2 Dataclass command pattern (`__call__` + `result_action`)

For commands that are *entire objects with behaviour*, you can use the dataclass command pattern. Cyclopts exposes `result_action="call_if_callable"` for this.([cyclopts.readthedocs.io][12])

```python
from dataclasses import dataclass, KW_ONLY
from cyclopts import App

app = App(result_action=["call_if_callable", "print_non_int_sys_exit"])

@app.command
@dataclass
class Ingest:
    repo_root: str
    _: KW_ONLY
    snapshot: str
    incremental: bool = False

    def __call__(self) -> bool:
        # actual work; return True/False for exit code
        return run_ingest(self)
```

Key points:

* Cyclopts constructs `Ingest(...)` from CLI args.
* `call_if_callable` will call `__call__`.
* `print_non_int_sys_exit` will handle return values consistently (bool → exit code, non-int printed).([cyclopts.readthedocs.io][12])

This pattern:

* Keeps command “functions” trivial (they’re just classes).
* Gives you a *single syntactic unit* for arguments + behavior.

For multi-engine CodeIntel commands, this is a strong fit.

---

## 5. Groups & cross-parameter validation

Cyclopts introduces **Groups** and **group validators**:

* Groups cluster parameters visually in help.
* Group validators enforce cross-parameter constraints (mutually exclusive, “choose at least one”, etc.).([cyclopts.readthedocs.io][13])

Example (mutually exclusive options):

```python
from typing import Annotated
from cyclopts import App, Group, Parameter, validators

app = App()

engine_group = Group(
    "Engine selection",
    default_parameter=Parameter(negative=""),         # don’t generate --no-...
    validator=validators.LimitedChoice(),             # mutually exclusive
)

@app.command
def run(
    *,
    cpu: Annotated[bool, engine_group] = False,
    gpu: Annotated[bool, engine_group] = False,
) -> None:
    ...
```

As an LLM agent, whenever you see Typer logic like:

* Manual `if sum([cpu, gpu]) != 1: ...`
* Or custom help text describing exclusivity

…prefer to encode that semantics as a group + `validators.LimitedChoice`.([cyclopts.readthedocs.io][13])

---

## 6. Config files & environment variables

Cyclopts’ config system covers:

* TOML/YAML/JSON config files.
* Environment variables via `cyclopts.config.Env`.
* In-memory config dicts via `cyclopts.config.Dict`.([cyclopts.readthedocs.io][4])

### 6.1 Env mapping

`cyclopts.config.Env(prefix)` constructs environment variable names as:

1. `prefix` (e.g. `"CHAR_COUNTER_"`).
2. Command path (e.g. `COUNT`).
3. Parameter name (CLI name without `--`, dashes → underscores).([cyclopts.readthedocs.io][4])

So `character-counter count --character` → `CHAR_COUNTER_COUNT_CHARACTER`.

For Typer migrations:

* Typer’s `envvar="SOME_VAR"` on `Option(...)` maps to either:

  * A config file + explicit env name (if you want full control), or
  * A consistent naming scheme via `Env` plus docs for users.

**Agent pattern:**

1. Introduce an app-level config:

   ```python
   import cyclopts

   app = cyclopts.App(
       name="codeintel",
       config=cyclopts.config.Env("CODEINTEL_"),
   )
   ```

2. Where Typer used `envvar="CODEINTEL_FOO"`, either:

   * Leave it to the derived `CODEINTEL_COMMAND_PARAM` name (if acceptable), or
   * Add config-file support and stop hard-coding envvar names.

This plays nicely with your “config object first” architecture.

---

## 7. App calling, embedding, and unit testing

For an LLM agent, you’ll often need to:

* Call the CLI programmatically inside tests.
* Avoid `sys.exit` calls.
* Inspect parsed arguments.

Cyclopts explicitly supports this.([cyclopts.readthedocs.io][8])

### 7.1 App invocation

* `app()` without args: parse `sys.argv` and run (suitable for `if __name__ == "__main__": app()`).
* `app("cmd --flag")` or `app(["cmd", "--flag"])`: parse explicit tokens.
* `app(..., result_action="return_value")`: return actual result instead of exiting.([cyclopts.readthedocs.io][8])

### 7.2 Testing patterns

The unit-testing cookbook lays out patterns you should emulate:

* Keep core business logic separate from Cyclopts functions.
* `app("args", result_action="return_value")` in tests to assert results.
* Use `app.parse_args([...])` to inspect parsed bindings without executing.
* Use `cyclopts.config.Env` + `parse_args` + pytest’s `monkeypatch` to test env-driven defaults.([cyclopts.readthedocs.io][4])

As an LLM agent migrating Typer → Cyclopts:

* Where Typer tests do `runner.invoke(app, ...)`, prefer:

  * `app("subcommand ...", result_action="return_value")` for outcome tests.
  * `app.parse_args([...])` for parse-only tests.

---

## 8. Concrete migration strategy for an AI agent

Cyclopts ships a “Migrating from Typer” guide with an API mapping table; you should treat that as normative.([cyclopts.readthedocs.io][6])

Here is a systematic migration process tailored to you as an agent.

### Phase 0: Discovery

For each Python module:

1. Detect `import typer` and `typer.Typer()`.
2. Detect `@app.command`, `@app.callback`, `app.add_typer`, `Annotated[..., typer.Option/Argument]`.
3. Collect all commands, their parameters, and docstrings.

Keep a mapping:

* `Typer` app object → new `App` object name (usually same variable).

### Phase 1: Replace Typer with Cyclopts minimally

For each app:

1. `from cyclopts import App, Parameter` (plus `Group`, `validators`, etc., as needed).
2. `app = typer.Typer(...)` → `app = App(name="...", help="...")`. For an initial pass, drop Typer arguments and reintroduce as needed later; Cyclopts provides comparable defaults (e.g. `no_args_is_help` behaviour).([cyclopts.readthedocs.io][6])
3. Replace `@app.callback()` used as default command with `@app.default`.

   * If callback was global pre-hook, migrate to `app.meta` pattern.([cyclopts.readthedocs.io][6])
4. Replace all `Annotated[..., typer.Argument(...)]` or `Annotated[..., typer.Option(...)]` with `Annotated[..., Parameter(...)]`, preserving:

   * `help` (or move into docstrings, which Cyclopts strongly supports).([cyclopts.readthedocs.io][6])
   * `envvar` (map to config.Env or `env_var` field if you choose to use it directly).([cyclopts.readthedocs.io][14])
   * Choices, constraints → `Literal` or validators.([cyclopts.readthedocs.io][10])

### Phase 2: Normalize function signatures

Your main goals:

* Functions are **small** and **callable with normal Python semantics**.
* No huge parameter lists.

Patterns to apply:

1. For commands with many related options, refactor to a **config dataclass** (user class pattern):

   ```python
   @dataclass
   class GraphRuntimeConfig:
       engine: Literal["cpu", "gpu"]
       max_nodes: int = 50_000
       max_depth: int = 5
       # ...

   @app.command
   def build_graphs(cfg: Annotated[GraphRuntimeConfig, Parameter(name="*")]) -> None:
       build_graphs_core(cfg)
   ```

2. For commands that embody a “task object”, use the **dataclass command** pattern:

   ```python
   @app.command
   @dataclass
   class Analyze:
       snapshot: str
       _: KW_ONLY
       with_coverage: bool = True
       # ...

       def __call__(self) -> bool:
           return run_analysis(self)
   ```

3. Use `Group` + validators for cross-parameter rules that Typer implemented manually.([cyclopts.readthedocs.io][13])

### Phase 3: Enforce app-wide policy

Once everything compiles and tests pass:

1. Configure `App.default_parameter` to encode global CLI norms:

   * Negative flag behaviour.
   * Whether to show defaults in help.
   * Repetition / aggregation defaults.([cyclopts.readthedocs.io][7])
2. Introduce `config.Env` and/or file config so you’re not hard-coding envvar names per option.([cyclopts.readthedocs.io][4])
3. Centralize shared arguments using the **“sharing parameters”** cookbook pattern: mark a dataclass with `@Parameter(name="*")` and reuse it across commands.([cyclopts.readthedocs.io][15])

### Phase 4: Documentation & shell integration

Cyclopts ships its own CLI:

* `cyclopts --install-completion` to enable shell completion.([cyclopts.readthedocs.io][16])
* `cyclopts generate-docs script.py:app` to auto-generate Markdown/HTML/RST help from your app.([cyclopts.readthedocs.io][16])

As an LLM agent:

* Prefer generating CLI docs from the **actual app** using `generate-docs` rather than hand-maintaining help text.
* Use docstrings as the authoritative source for parameter descriptions; Cyclopts parses multiple docstring styles.([cyclopts.readthedocs.io][2])

---

## 9. Design patterns tuned to your constraints

Given your constraints (strict typing, small functions, complex domain config), I’d recommend you treat Cyclopts as:

> A thin, type-driven adapter from CLI → structured config objects → your existing services.

A few concrete patterns:

### 9.1 “Edge-only” Cyclopts layer

* One module `codeintel/cli.py` (or similar) with:

  * `App` creation.
  * Command registration.
  * User classes / config dataclasses.
* All real work is in `codeintel.*` modules with zero Cyclopts imports.

This keeps tooling (pyright, ruff) very happy and ensures the CLI layer is swappable.

### 9.2 Typed config + flattening

For each major pipeline stage (ingest, graphs, analytics):

1. Define a dataclass config.
2. Add it as a single argument to the command.
3. Flatten it with `Parameter(name="*")` if you want top-level flags.

This yields:

* Clean function signatures.
* Discoverable, strongly-typed configs you can reuse in non-CLI orchestration (e.g., Prefect, internal pipeline orchestrator).

### 9.3 Testing via `result_action="return_value"`

For LLM-written tests:

* ALWAYS call `app("...", result_action="return_value")` when you want to assert on return values or stdout, instead of letting `sys.exit` fire.([cyclopts.readthedocs.io][8])
* Use `app.parse_args([...])` for introspecting parsed bindings when testing env/config behaviour.

---



[1]: https://cyclopts.readthedocs.io/en/latest/vs_typer/README.html "Typer Comparison — cyclopts"
[2]: https://cyclopts.readthedocs.io/en/latest/getting_started.html "Getting Started — cyclopts"
[3]: https://cyclopts.readthedocs.io/en/v3.6.0/user_classes.html "User Classes — cyclopts"
[4]: https://cyclopts.readthedocs.io/en/latest/config_file.html "Config Files — cyclopts"
[5]: https://cyclopts.readthedocs.io/en/latest/parameters.html "Parameters — cyclopts"
[6]: https://cyclopts.readthedocs.io/en/latest/migration/typer.html "Migrating From Typer — cyclopts"
[7]: https://cyclopts.readthedocs.io/en/latest/default_parameter.html?utm_source=chatgpt.com "Default Parameter — cyclopts - Read the Docs"
[8]: https://cyclopts.readthedocs.io/en/latest/app_calling.html "App Calling & Return Values — cyclopts"
[9]: https://cyclopts.readthedocs.io/en/v3.20.0/meta_app.html?utm_source=chatgpt.com "Meta App — cyclopts - Read the Docs"
[10]: https://cyclopts.readthedocs.io/en/stable/parameter_validators.html?utm_source=chatgpt.com "Parameter Validators - Cyclopts - Read the Docs"
[11]: https://cyclopts.readthedocs.io/en/latest/parameters.html?utm_source=chatgpt.com "Parameters — cyclopts - Read the Docs"
[12]: https://cyclopts.readthedocs.io/en/stable/cookbook/dataclass_commands.html "Dataclass Commands — cyclopts"
[13]: https://cyclopts.readthedocs.io/en/latest/group_validators.html?utm_source=chatgpt.com "Group Validators — cyclopts - Read the Docs"
[14]: https://cyclopts.readthedocs.io/en/v3.0.0/api.html?utm_source=chatgpt.com "API — cyclopts - Read the Docs"
[15]: https://cyclopts.readthedocs.io/en/v3.6.0/user_classes.html?utm_source=chatgpt.com "User Classes - Cyclopts - Read the Docs"
[16]: https://cyclopts.readthedocs.io/en/latest/cli_reference.html "CLI Reference — cyclopts"


### 10. Command trees, sub-apps, and flattened CLIs

Cyclopts encourages treating your CLI as a tree of `App` objects rather than a flat list of commands, which is a big upgrade over a typical monolithic Typer app.

#### 10.1 Sub-apps as first-class commands

You can register an entire `App` as a command of another `App`:

```python
from cyclopts import App

root = App(name="codeintel")

# A sub-application for ingestion commands
ingest_app = App(name="ingest")

root.command(ingest_app)  # registers under command name "ingest"

@ingest_app.command
def snapshot(repo_root: str, snapshot_id: str) -> None:
    ...

@ingest_app.command
def incremental(repo_root: str, base_snapshot: str, new_snapshot: str) -> None:
    ...
```

Users call:

```bash
codeintel ingest snapshot ...
codeintel ingest incremental ...
```

This pattern is fully recursive: sub-apps can have their own `@default`, parameters, config, etc.

#### 10.2 Flattening sub-apps

If you want your code organized into sub-apps but the UX flat (`codeintel snapshot` instead of `codeintel ingest snapshot`), you can **flatten** a sub-app using `name="*"`:

```python
tools = App(name="tools")

@tools.command
def compress(path: str) -> None: ...
@tools.command
def extract(path: str) -> None: ...

root.command(tools, name="*")  # flattens tools_* commands into root
```

Caveats:

* Parent commands win on name collisions.
* Only `App` instances can be flattened.
* You can’t pass extra config kwargs when using `name="*"`.

For a large system like CodeIntel, this lets you keep separate modules/apps for `ingest`, `graphs`, `analytics` while presenting a flat, ergonomic CLI.

#### 10.3 Sub-app configuration inheritance

Config like `exit_on_error`, `print_error`, `result_action`, `console`, and `default_parameter` are inherited by sub-apps unless explicitly overridden:

```python
root = App(exit_on_error=False, print_error=False)

child = root.command(App(name="child"))
grandchild = child.command(App(name="grandchild", exit_on_error=True))
```

Best practice:

* Set **global policy** (error handling, console, result_action) only on the root app.
* Override per sub-app only when you have a truly distinct behavior (e.g. experimental commands that should propagate tracebacks).

#### 10.4 Naming, aliases, and name transforms

Cyclopts normalizes command names by default:

* Function `foo_bar` → CLI command `foo-bar` (underscores to hyphens, leading/trailing underscores stripped).

You can control this at three levels:

1. **Global name policy** via `App.name_transform`:

   ```python
   app = App(name_transform=lambda s: s)  # preserve underscores
   ```

2. **Explicit command name** in decorator:

   ```python
   @app.command(name="run-ingest")
   def run_ingest(...): ...
   ```

3. **Aliases** (multiple names for same command):

   ```python
   @app.command(alias="ri")
   def run_ingest(...): ...
   ```

As an LLM agent refactoring from Typer:

* Preserve existing user-visible names by **encoding them explicitly** (`name`, `alias`), *then* optionally enable a global `name_transform`.
* Never rely on default transforms when reproducing an existing CLI contract.

---

### 11. Async commands and event-loop control

Cyclopts treats `async def` commands as first-class citizens.

#### 11.1 Async from the CLI

If a registered command is `async def`, `app()` will automatically create and run an event loop using the configured backend (default: `asyncio`).

```python
import asyncio
from cyclopts import App

app = App()

@app.command
async def ingest(snapshot: str) -> None:
    await asyncio.sleep(0.1)
    ...
    
app()
```

This is enough for a top-level CLI script (`python -m codeintel.cli` entrypoint).

#### 11.2 Async from existing event loops

In an *already async* context (ASGI app, tests, orchestrators), you must call `run_async`:

```python
async def main() -> None:
    result = await app.run_async(["ingest", "--snapshot", "s123"])
```

Calling `app([...])` directly inside a running event loop will raise `RuntimeError`.

Best practice for an LLM agent:

* In tests that are `async def`, call `await app.run_async(tokens, result_action="return_value")`.
* In sync tests, keep `app(tokens, result_action="return_value")`.

#### 11.3 result_action and async

By default, `App.call` / `App.run_async` are configured to behave like a “real” CLI: after running the command, they will `sys.exit(...)` with an exit code based on the result.

You can override via `result_action`:

```python
app = App(result_action=["call_if_callable", "return_value"])
```

* `call_if_callable` → call the result if it is callable (dataclass command pattern).
* `return_value` → just return whatever the command returns, no `sys.exit`.

This is critical for embedding Cyclopts into other systems or for LLM-generated test harnesses.

Watchout vs Typer:

* Typer’s `CliRunner` hides exit behavior in tests. With Cyclopts, *you* configure the policy; be explicit in test apps.

---

### 12. Coercion rules and complex types

Cyclopts has a rich coercion engine that maps CLI tokens → Python objects based on type hints.

High-level semantics:

1. **No type hint**:

   * If there’s a non-`None` default, infer the type from `type(default)`.
   * Otherwise, default to `str`.

2. **Simple types**: `int`, `float`, `bool`, `str`, `Path`, etc. are parsed as expected. Booleans become flags.

3. **Enums and Flags**: `Enum`, `IntEnum`, `Flag`, `IntFlag` are supported; CLI values are matched by `.name` (and/or `.value` where appropriate).

4. **Literal**: `Literal["cpu", "gpu"]` is treated like a choice; invalid value → validation error.

5. **Containers**:

   * `list[T]`, `set[T]`, `tuple[T, ...]` consume multiple tokens; repeated options generally extend the collection.
   * The converter sees multiple tokens when the annotated type is iterable, unless you constrain it with `n_tokens`.

6. **User classes**: dataclasses, attrs classes, Pydantic models, `TypedDict`, etc., are recursively populated from nested flags (`--cfg.foo`, `--cfg.bar`).

#### 12.1 Custom converters

When built-in coercion is not enough, use `Parameter(converter=...)`:

```python
from cyclopts import App, Parameter, Token
from typing import Annotated, Sequence
import json

app = App()

def json_list_converter(type_, *tokens: Token) -> list[str]:
    # tokens: raw CLI tokens (string-like)
    return json.loads(tokens[0])

@app.command
def run(
    include_ext: Annotated[
        list[str],
        Parameter(converter=json_list_converter),
    ],
) -> None:
    ...
```

Converter signature: `converter(type_, *tokens) -> Any`. You control how many tokens it sees via `n_tokens` if needed.

#### 12.2 Validators

Carve validation out of conversion with `Parameter(validator=...)` or group validators.

```python
from cyclopts import validators

def validate_exts(type_, value: list[str]) -> None:
    for ext in value:
        if ext not in {".py", ".pyi"}:
            raise ValueError(f"Unsupported extension {ext!r}")

ParamExtList = Annotated[list[str], Parameter(validator=validate_exts)]
```

For the kind of strongly validated configs you want, this separation keeps conversion pure and validation explicit.

#### 12.3 n_tokens and “shape-mismatch” signatures

When you have types like `tuple[int, int]` or custom objects that should consume a fixed number of tokens, use `n_tokens`:

```python
Coord = tuple[int, int]

@app.command
def plot(
    xy: Annotated[
        Coord,
        Parameter(n_tokens=2),
    ],
) -> None:
    ...
```

Without `n_tokens`, Cyclopts infers token grouping from container type alone; being explicit is safer for complex CLIs.

---

### 13. Container & multi-value arguments: patterns and gotchas

Complex CodeIntel commands will inevitably take lists or sets (e.g. multiple datasets, engines).

#### 13.1 Repeated options vs single JSONish argument

Cyclopts supports repeated options for list-like types:

```python
@app.command
def run(
    include: list[str],   # or Annotated[list[str], Parameter(...)]
) -> None:
    ...

# Works:
#   --include A --include B
#   --include=A --include=B
```

In practice:

* `--include A --include B` → `["A", "B"]`
* For some syntaxes (`--include A B`) you may need `n_tokens` or a custom converter; behaviour here has some open issues and you should design tests around it.

**Best practice for LLM agents**: prefer **repeated flags** over “space-separated list” semantics; they are less ambiguous and easier to test.

#### 13.2 Empty vs missing

For iterable params:

* Default `[]` (or `set()`) means “empty” vs `None` meaning “not provided”.
* Negative flags (`negative_iterable`) can give you a convenient `--empty-include` to explicitly set an empty list.

```python
IncludeList = Annotated[
    list[str],
    Parameter(negative_iterable="--no-include"),  # or custom name
]
```

Design tip: use `None` for “use default behavior” and `[]` / `set()` for “user explicitly requested empty” and document this clearly.

---

### 14. Docstring-driven help and parameter docs

Cyclopts leans heavily on docstrings and `docstring_parser` to populate help text.

#### 14.1 Command help from docstrings

For a command function:

* First line → short description for help.
* Body → long description in detailed help (`--help` on that command).

It supports common styles: reST / Sphinx, Google, NumPy, Epydoc.

```python
@app.command
def ingest(snapshot: str) -> None:
    """
    Ingest a snapshot into the CodeIntel store.

    Parameters
    ----------
    snapshot : str
        Snapshot identifier or path to snapshot config.
    """
```

You rarely need `Parameter(help=...)` unless you want to override docstring-derived help.

#### 14.2 Parameter help from docstrings

Parameter sections in your docstring (`Parameters`, `Args`) are parsed and associated with function parameters:

```python
def run(
    snapshot: str,
    workers: int = 4,
) -> None:
    """
    Run analytics for a snapshot.

    Parameters
    ----------
    snapshot : str
        Snapshot ID.
    workers : int
        Number of worker processes.
    """
```

Cyclopts will show `Snapshot ID.` and `Number of worker processes.` as parameter help in `--help`.

#### 14.3 Overriding with `Parameter(help=...)`

If you *also* use `Annotated[..., Parameter(help="...")]`, that explicit help wins over docstrings:

```python
def run(
    snapshot: Annotated[str, Parameter(help="Snapshot identifier")],
) -> None:
    """... docstring ..."""
```

LLM pattern:

* Prefer docstrings as canonical documentation.
* Use `Parameter(help=...)` only when:

  * You need different help for CLI vs internal docs, or
  * You’re auto-generating docstrings and want CLI-specific overrides.

#### 14.4 App-level help and version

* `App.help`: top-level description shown in root `--help`. If unset, falls back to `@app.default` docstring.
* Help and version flags `--help/-h` and `--version` are added by default; you can customize labels and formatting via `App.help_flags`, `App.version_flags`, `App.help_format`, `App.version_format`.

When migrating from Typer:

* Preserve existing banner text by porting it to `App.help` and command docstrings, not ad-hoc `print()`s.

---

### 15. Console and error output (Rich integration)

Cyclopts uses **Rich** for all help and CLI error formatting.

#### 15.1 App.console

`App.console` is a `rich.console.Console` used for:

* Help output.
* Parsing errors and validation errors.
* Optional traceback rendering (see “rich formatted exceptions” cookbook).

You can:

```python
from rich.console import Console
from cyclopts import App

console = Console()

app = App(console=console)
```

From v4/v5 discussions: `App.console` respects the app hierarchy; if set on a parent app, sub-apps share the same console unless overridden.

#### 15.2 Controlling error printing in tests

`App.call` / `App.run` have a `print_error` flag:

* `True` (default): show rich error panel on failure.
* `False`: suppress error output, raise exceptions (or return error objects depending on `result_action`).

For unit tests written by an LLM:

```python
def test_invalid_snapshot() -> None:
    app = App(print_error=False, result_action="return_value")
    with pytest.raises(SomeError):
        app(["run", "--snapshot", "bad"])
```

This avoids noisy Rich output in test logs while still preserving Cyclopts’ semantics in production.

#### 15.3 Rich-formatted exceptions

The cookbook shows how to wrap your entrypoint with Rich’s traceback handler for prettier errors:

* Wrap `app()` in `rich.traceback.install()` or use Rich’s exception handler.
* Keep business logic exceptions distinct from CLI parse errors (parse errors are already pretty-printed by Cyclopts).

Design rule:

* For library-like usage (embeddable CLI), prefer raising domain-specific exceptions and let the embedding layer decide how to render them (Rich vs logging, etc.)

---

### 16. Config files, env vars, and config pipeline

#### 16.1 config functions and precedence

`App(config=...)` accepts a *callable* (or list of callables) that manipulate an internal argument mapping after CLI tokens and environment variables are parsed but before conversion and validation.

Signature:

```python
def config(apps: list["App"], commands: tuple[str, ...], arguments):
    ...
```

Built-in config providers live under `cyclopts.config` and include:

* File-based configs (YAML/TOML, etc.).
* Environment variable reader (`Env`).

Typical precedence:

1. CLI flags
2. Config (files/env)
3. Defaults in function signatures

This mirrors common “CLI > config > env > defaults” expectations.

#### 16.2 TOML/YAML config

Example TOML config:

```toml
# config/codeintel.toml
[codeintel.run]
snapshot = "s123"
workers = 8
```

Wiring it:

```python
import cyclopts
from cyclopts.config import Toml

app = cyclopts.App(
    name="codeintel",
    config=Toml("config/codeintel.toml"),
)
```

Config functions can merge or override other sources; you can compose multiple config providers in a list.

#### 16.3 Environment variables via Env

`cyclopts.config.Env(prefix)` automatically maps environment variables to parameters.

Given:

```python
app = cyclopts.App(
    name="character-counter",
    config=cyclopts.config.Env("CHAR_COUNTER_"),
)

@app.command
def count(character: str, path: Path) -> None:
    ...
```

Environment variable names follow:

`PREFIX` + command path + param name, uppercased and `_`-separated. Example: `CHAR_COUNTER_COUNT_CHARACTER`.

Pattern for your stack:

* Use **config dataclasses** as the primary abstraction.
* Provide a TOML/YAML config file per environment.
* Overlay `Env(prefix)` for deployment-specific overrides.
* Leave CLI flags as the highest-precedence explicit control surface.

---

### 17. Parameter resolution precedence and stacking

Cyclopts lets you stack multiple `Parameter` annotations and apply defaults from three levels:

1. `App.default_parameter`
2. `Group(default_parameter=...)`
3. `Annotated[..., Parameter(...)]` on the parameter itself

Resolution is **right-to-left**:

* Rightmost `Parameter` wins for attributes it sets; unset attributes fall back to the next one to the left.

Example:

```python
CommonBool = Annotated[bool, Parameter(negative="--no-")]

@app.command
def run(
    *,
    verbose: Annotated[
        bool,
        Parameter(help="Verbose output"),
        CommonBool,  # left side
    ] = False,
) -> None:
    ...
```

If `CommonBool` sets `negative` but not `help`, and the explicit `Parameter` sets `help` but not `negative`, the final parameter has **both**.

Design patterns:

* Use type aliases (`CommonBool`, `PathDir`, `SnapshotId`) to encode reusable chunks of semantics.
* Use `Group(default_parameter=...)` for *semantic roles* (“graph toggles”, “output format”) and per-parameter `Parameter` for exceptions.
* Keep `App.default_parameter` for universal norms (show defaults, disable negative flags globally).

Watchout: when migrating from Typer, faithfully replicate envvar, help, and flag semantics by stacking `Parameter` pieces carefully; rely on the right-to-left precedence rule.

---

### 18. Meta app, command chaining, and advanced orchestration

#### 18.1 Meta app (custom invocation flow)

Cyclopts provides a “meta app” concept: an app that launches other apps, letting you customize invocation beyond standard `app()` semantics.

Use cases:

* Global login / authentication before running any subcommand.
* Context objects shared across commands (replacing Typer’s context pattern).
* Multi-phase flows (validate -> run -> summarize) with custom error handling.

At a high level:

* Define a meta `App` that calls `.call()`/`.run()`/`.run_async()` on your real `App` with custom `result_action`.
* You can bind context (DB handles, sessions, auth tokens) before dispatching.

This maps nicely to your pipeline orchestrator: the meta app can perform snapshot resolution, environment detection, and config assembly before invoking the “inner” CLI.

#### 18.2 Command chaining (by design, DIY)

Cyclopts explicitly **does not** support Click-style command chaining (e.g. `tool validate build upload` all in one go) because it prioritizes consistent, predictable parsing.

However, the docs provide a pattern:

* Choose a delimiter token (e.g. `AND` or `--`).
* Split `sys.argv` on that token.
* For each segment, invoke `app(segment_tokens)` manually.

As an LLM agent:

* For CodeIntel, if you need multi-stage flows, prefer explicit subcommands (`pipeline run`, `pipeline summarize`) instead of chained commands.
* If you must implement chaining, encapsulate it in **one carefully documented command**, not as global behavior.

---

### 19. Packaging, shell completion, and Sphinx integration

#### 19.1 Packaging and entry points

Cyclopts encourages the standard “call `app()` in `if __name__ == "__main__"`” pattern, but in production you’ll usually expose it via `console_scripts` entry points.

Pattern:

```python
# codeintel/cli.py
from cyclopts import App
app = App(name="codeintel")
...  # register commands

def main() -> None:
    app()

# pyproject.toml
[project.scripts]
codeintel = "codeintel.cli:main"
```

#### 19.2 Shell completion

Cyclopts has built-in shell completion support via its own CLI (`cyclopts` tool) and `Shell Completion` docs section.

Typical pattern:

* Run a generator command (e.g. `cyclopts completion bash codeintel.cli:app`) as part of installation.
* Install the generated completion script according to shell conventions.

#### 19.3 Sphinx / doc generation

Cyclopts has dedicated docs for Sphinx integration and a CLI to **generate docs** from an app (`generate-docs` command).

For a CodeIntel-style project:

* Treat the CLI app as a source of truth.
* Generate Markdown/RST help for mkdocs/sphinx, rather than duplicating argument doc tables by hand.

---

### 20. Putting it together: patterns for a large, strictly-typed CLI

For an AI agent working in your CodeIntel repo, these are the recommended patterns to apply consistently:

1. **Single responsibility commands:**

   * Each CLI command has a low-arity Python signature (1–3 parameters).
   * Heavy argument sets become config dataclasses or user classes flattened into flags.

2. **Docstring-first documentation:**

   * Use NumPy or Google-style docstrings for *all* commands.
   * Rely on Cyclopts to parse docs for help; only override with `Parameter(help=...)` when necessary.

3. **Strong types + validators:**

   * Use `Literal`, `Enum`, and user classes to model domain concepts (engines, backends, snapshot selectors).
   * Express invariants via validators and group validators, not ad-hoc checks in command bodies.

4. **Config pipeline:**

   * Per-environment TOML/YAML config + `Env(prefix)` + CLI flags with the familiar precedence.
   * No hard-coded envvar names except for backwards compatibility.

5. **Sub-app structure:**

   * `codeintel` root app.
   * Sub-apps for `ingest`, `graphs`, `analytics`, `pipeline`, `serve`, etc.
   * Flatten where you want a simpler UX (`name="*"`), keep hierarchical where it matches mental models.

6. **Async and orchestrators:**

   * Use `run_async` in async contexts and set `result_action` to `"return_value"` for programmatic usage.
   * Wrap the CLI in a meta app if you need cross-command context (connections, sessions, etc.).

7. **Testing discipline:**

   * For every non-trivial command, have:

     * Unit tests on the underlying business function (no Cyclopts).
     * At least one test that calls the Cyclopts `App` with tokens and asserts on results & parse behavior.
