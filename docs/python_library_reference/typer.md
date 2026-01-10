Below is a *field‑guide* to **Typer**—aimed at an AI programming agent refactoring custom CLI code into a maintainable, type‑safe Typer application. It’s structured to help you map common CLI patterns (manual `sys.argv`, `argparse`, ad‑hoc prompting/printing) to Typer’s primitives, with idiomatic examples and gotchas.

---

## 0) Snapshot (versions, scope, mental model)

* **What it is:** Typer is a Python library (built on Click) for writing CLIs with modern type hints. It generates `--help`, validates and converts values from annotations, and supports shell completion. It can also *run* simple scripts (even those not written with Typer) via the `typer` command. ([Typer][1])
* **Latest release:** `typer` **0.19.2** on PyPI (released **Sep 23, 2025**). The project previously announced 0.19.1 as the last to support Python 3.7 and noted 3.8 would be dropped soon after; treat **3.8+** as your baseline. ([PyPI][2])
* **Why refactor to Typer:** You get type‑driven parsing/validation, automatic help, completion, composable subcommands, testability via `CliRunner`, and good UX via Rich integration. ([Typer][3])

---

## 1) Installation & the `typer` command

```bash
pip install typer         # includes the `typer` console command
# or
pip install typer-slim    # no console command; still available via: python -m typer
```

**What the `typer` command does:**

* Run a module or file (even a plain function) with completion:
  `typer path_or.module run ...`
* Install shell completion for the `typer` command: `typer --install-completion`
* Generate Markdown docs for your Typer app:
  `typer <module> utils docs --name my-cli --output README.md` ([Typer][4])

---

## 2) Mental model (how Typer “clicks”)

* **Types drive parsing.** Annotate parameters (e.g., `int`, `Path`, `Enum`, `List[str]`); Typer converts CLI text into those types and shows helpful errors. Prefer `typing_extensions.Annotated` (or `typing.Annotated`) + `typer.Option(...)` / `typer.Argument(...)` for rich metadata. ([Typer][5])
* **Two entry styles:**

  1. *Single function*: `typer.run(main)`—fastest path for small scripts. ([Typer][6])
  2. *Multi‑command app*: `app = typer.Typer(); @app.command() ...; app()`. For subcommands, compose smaller `Typer()` apps with `app.add_typer(...)`. ([Typer][7])
* **Context & callbacks:** A root `@app.callback()` can define *global* options and initialize shared state (e.g., config, verbosity) via a `typer.Context` object. ([Typer][8])

---

## 3) Core API by example (upgrade your custom code)

### 3.1 A minimal one‑file CLI (arguments, options, help)

```python
import typer
from typing_extensions import Annotated

def main(
    name: Annotated[str, typer.Argument(help="Who to greet")],
    polite: Annotated[bool, typer.Option("--polite/--no-polite")] = False,
    times: Annotated[int, typer.Option(min=1, max=10, clamp=True)] = 1,
):
    for _ in range(times):
        print(f"{'Good day' if polite else 'Hello'}, {name}!")

if __name__ == "__main__":
    typer.run(main)
```

* `Argument` is *required* by default; `Option` is *optional* by default; you can make options required by omitting a default. Booleans automatically get `--flag/--no-flag`. Numeric bounds (`min`, `max`, `clamp`) are supported. ([Typer][9])

### 3.2 Types you’ll use most

* **Numbers** (`int`, `float`) with validation (min/max, clamp). **Counters**: `Option(..., count=True)` supports `-vvv` verbosity. ([Typer][10])
* **Paths & files**: annotate with `pathlib.Path`, or use file‑like parameter types when you need open handles. ([Typer][11])
* **Enums (choices)**: subclass `str, Enum` for nice strings and auto‑choices. ([Typer][12])
* **Collections & tuples**: `List[str]` with `multiple=True`‐style usage; tuples for fixed arity. ([Typer][13])

### 3.3 Options/arguments from environment variables

```python
from typing_extensions import Annotated
import typer

def main(
    token: Annotated[str, typer.Option(envvar=["MYAPP_TOKEN", "TOKEN"])],
):
    ...
```

Typer can read from one or more env vars if the CLI value wasn’t provided, and this is surfaced in `--help`. Use it to replace ad‑hoc `os.environ.get(...)` plumbing. ([Typer][14])

### 3.4 Prompts, confirmations, passwords

```python
force = typer.confirm("Delete all data?", abort=True)
password = typer.prompt("Enter password", hide_input=True)
```

`confirm(..., abort=True)` will politely exit if the user declines. For fancy UX and color, pair Typer with **Rich**. ([Typer][15])

### 3.5 Exceptions & termination

* Raise `typer.BadParameter(...)` from a callback to attach a clear error to a specific parameter.
* Use `raise typer.Exit(code=...)` or `raise typer.Abort()` to stop early. With Rich installed (`typer[all]`), tracebacks are cleaned up for you. ([Typer][16])

---

## 4) Subcommands & project layout

### 4.1 Multi‑command apps

```python
import typer

app = typer.Typer(help="Awesome CLI user manager.")
users = typer.Typer()
items = typer.Typer()
app.add_typer(users, name="users")
app.add_typer(items, name="items")

@users.command("create")
def users_create(name: str): ...

@items.command("delete")
def items_delete(item_id: int): ...

if __name__ == "__main__":
    app()
```

You compose independently testable apps, then mount them under names. Nest as deeply as needed. ([Typer][7])

### 4.2 Global options via `@app.callback()`

```python
state = {"verbose": 0}

@app.callback()
def main(verbose: int = typer.Option(0, "--verbose", "-v", count=True)):
    state["verbose"] = verbose
```

Callbacks let you put root‑level options (e.g., `--verbose`) and help. The `count=True` pattern gives `-v`, `-vv`, etc. ([Typer][8])

### 4.3 Organizing commands across modules

Use “**One file per command**” and `add_typer(...)` to keep modules small and dependencies local; Typer merges them into one UX. ([Typer][17])

---

## 5) Completion (what users love)

* **Shell completion** suggests commands, options, and typed values (e.g., paths, choices). It’s powered by Click’s shell‑completion and works for Bash, Zsh, Fish, etc. ([Click Documentation][18])
* **Installing completion:** packaged CLIs typically gain a `--install-completion` option; for loose scripts, the `typer` command provides completion automatically once you run `typer --install-completion`. ([Typer][4])
* **Custom value completion:** supply a completion function (or `shell_complete`) to filter candidates based on the *incomplete* token and even show per‑item help. ([Typer][19])

---

## 6) Printing, color, progress bars, and docs

* **Printing**: Use `print()` for simple logs; prefer **Rich** (`from rich import print`) for structured, colored output. Typer docs now recommend Rich for display; `typer.echo` still exists (from Click) but is rarely needed. ([Typer][20])
* **Progress**: `typer.progressbar(iterable)` is available (from Click); for richer widgets, prefer Rich progress. ([Typer][21])
* **Help formatting**: enable `rich_markup_mode` on `Typer()` for rich/markdown‑style help and use help panels to group options. ([Typer][22])
* **Open files/URLs**: `typer.launch("https://...")` opens with the OS default handler. ([Typer][23])
* **Generate Markdown docs** for your CLI: `typer <module> utils docs --name <cli> --output README.md`. Great for README automation. ([Typer][4])

---

## 7) State & configuration

* **Context object**: add `ctx: typer.Context` to command params; set `ctx.obj` in the callback (e.g., a `Settings` object) so subcommands can read it. ([Typer][24])
* **App directory**: `typer.get_app_dir("my-cli")` gives a per‑user config folder (OS‑aware). Use it to store JSON/INI config and caches. ([Typer][25])
* **Environment variables**: use `envvar=` for arguments or options to support “CLI or ENV” sourcing—`--flag` still overrides. ([Typer][14])

---

## 8) Testing

Use Typer’s `CliRunner` (Click‑compatible) to invoke the app as if from the shell:

```python
from typer.testing import CliRunner
from myapp.cli import app

runner = CliRunner()

def test_list_users():
    result = runner.invoke(app, ["users", "list", "--format", "json"])
    assert result.exit_code == 0
    assert "[]" in result.output
```

`CliRunner` supports input streams, filesystem isolation, and capturing stderr if you configure it. It’s a stable way to test command behavior without spawning subprocesses. ([Typer][3])

---

## 9) Packaging & entry points

Typer’s tutorial shows a minimal “package your CLI” workflow; in `pyproject.toml` define an entry point so end‑users can run `my-cli` instead of `python -m ...`. For small scripts, you can run them through the `typer` command without packaging and still get completion. ([Typer][26])

---

## 10) Interop with Click (when porting existing Click or custom code)

* Typer rides on Click; many advanced features (custom `ParamType`, file/Path handling, eager options like `--version`) come straight from Click. You can drop down to Click if needed, or embed Click commands in Typer (help styles may differ). ([Typer][27])
* To add a **root `--version`** today, implement it via a callback with `is_eager=True` (Typer docs show the pattern), or wrap Click’s `version_option`. ([Typer][28])

---

## 11) Refactor mapping (from custom code to Typer)

| If your custom code does this… | Replace with Typer…                                             | Notes                                                           |
| ------------------------------ | --------------------------------------------------------------- | --------------------------------------------------------------- |
| Parse `sys.argv` manually      | `typer.run(main)` or `app = Typer()` + `@app.command()`         | Types drive parsing; you get help/errors for free. ([Typer][6]) |
| Manual `print` menus / usage   | Docstrings + `help=` on `Option`/`Argument`; `rich_markup_mode` | `--help` generated; organize with help panels. ([Typer][22])    |
| Hand‑rolled boolean flags      | `bool` option → auto `--flag/--no-flag`                         | Override names if you don’t want the dual form. ([Typer][29])   |
| “-v/-vv/-vvv” verbosity        | `Option(count=True)` on an `int`                                | Often set globally in `@app.callback()`. ([Typer][10])          |
| Validate choices               | `Enum` subclass (`str, Enum`)                                   | Users see choices and completion. ([Typer][12])                 |
| Read files/paths               | Annotate `Path` or `File` types                                 | Click/ Typer handle existence & modes. ([Typer][11])            |
| Pull config from ENV           | `envvar=` on `Option`/`Argument`                                | Shows ENV in help; CLI still wins. ([Typer][14])                |
| Custom prompts/confirm         | `typer.prompt`, `typer.confirm(..., abort=True)`                | Rich improves UX. ([Typer][15])                                 |
| “Abort on error” exits         | `raise typer.BadParameter(...)`, `typer.Exit`, `typer.Abort`    | Clear errors; integrates with help. ([Typer][16])               |
| Multiple subcommands           | Structuring with `Typer()` + `add_typer(...)`                   | Compose apps; nest deeply if needed. ([Typer][7])               |

---

## 12) Advanced recipes you’ll likely need

### 12.1 Custom autocompletion for values

```python
import typer
from typing_extensions import Annotated

names = ["Camila", "Carlos", "Sebastian"]

def complete_name(incomplete: str):
    return [n for n in names if n.startswith(incomplete)]

@app.command()
def greet(name: Annotated[str, typer.Option(autocompletion=complete_name)]):
    print(f"Hi {name}")
```

Your completion receives the incomplete token; Click 8.x terms this `shell_complete` under the hood. ([Typer][19])

### 12.2 Multiple values / repeated options

```python
from typing_extensions import Annotated
from typing import List

@app.command()
def add_users(user: Annotated[List[str], typer.Option("--user", "-u")]):
    ...
```

Users can pass `-u one -u two -u three`; you’ll get `["one", "two", "three"]`. For fixed‑arity groups, use a typed tuple. ([Typer][13])

### 12.3 Path & file safety

```python
from pathlib import Path
from typing_extensions import Annotated

def main(config: Annotated[Path, typer.Option(exists=True, file_okay=True)]):
    ...
```

Typer/Click validate that the file exists (or is a directory), avoiding manual checks. ([Typer][11])

### 12.4 Group‑wide config & context

```python
import typer
app = typer.Typer()

class Settings:
    def __init__(self, verbose: int): self.verbose = verbose

@app.callback()
def root(ctx: typer.Context, verbose: int = typer.Option(0, "--verbose", "-v", count=True)):
    ctx.obj = Settings(verbose)
```

Subcommands accept `ctx: typer.Context` and read `ctx.obj`. ([Typer][24])

---

## 13) Testing patterns

* Prefer **unit‑style** testing with `typer.testing.CliRunner()`.
* Assert on `result.exit_code`, `result.output`; to separate stderr/stdout, configure the runner accordingly. Click’s testing docs apply directly. ([Typer][3])

---

## 14) UX polish checklist (small wins)

* **`--version`**: implement via a callback with `is_eager=True` (or wrap `click.version_option`). ([Typer][28])
* **Rich help/markup**: `Typer(rich_markup_mode="rich")`; group options via `rich_help_panel=`. ([Typer][22])
* **Completion**: ensure your packaged CLI exposes `--install-completion`; for scripts, teach users the `typer` command. ([Typer][4])
* **Progress bars**: `typer.progressbar` or Rich progress for multi‑task feedback. ([Typer][21])
* **App dir**: use `typer.get_app_dir("name")` for config/cache paths. ([Typer][25])

---

## 15) Common pitfalls (and how to avoid them)

* **Only one command?** Typer optimizes single‑command apps so you call `python app.py ARGS` (not `app.py cmd ARGS`). If you *need* a command label anyway, add an `@app.callback()` to keep the group shape. ([Typer][30])
* **`autocompletion` vs `shell_complete`:** With Click 8.x, `autocompletion` moved to `shell_complete`; Typer bridges this but warns if you use the old name. Prefer the modern pattern shown in the docs. ([GitHub][31])
* **Printing APIs:** Prefer `print()` or Rich; `typer.echo` is fine but typically unnecessary in modern Python. ([Typer][20])
* **Global state in tests:** `CliRunner` changes interpreter state; keep tests isolated and avoid cross‑test globals. (Guidance comes from Click testing docs.) ([Click Documentation][32])

---

## 16) Migration playbook (step‑by‑step)

1. **Identify commands** in your custom code; each becomes a `@app.command()` function.
2. **Move argument parsing into type hints** using `Annotated[..., Option/Argument]`.
3. **Replace `print`/color libs** with Rich (still call `print()`—but import from Rich). Split stdout/stderr only if needed. ([Typer][20])
4. **Prompts** → `typer.prompt` / `typer.confirm(abort=True)`; kill bespoke yes/no logic. ([Typer][15])
5. **Validation**: turn regex/if‑else validation into type conversions (e.g., `Enum`, `Path`, numeric bounds) or per‑param `callback` raising `BadParameter`. ([Typer][12])
6. **Config**: replace custom `~/.toolrc` discovery with `typer.get_app_dir(...)` and envvar‑backed options. ([Typer][25])
7. **Package**: expose an entry point; add a `--version` eager option; ship instructions for `--install-completion`. ([Typer][28])
8. **Test**: port shell scripts to `CliRunner` tests. ([Typer][3])

---

## 17) Extras you may want later

* **Files & paths**: leverage `exists=`, `dir_okay=`, `file_okay=` validations on path options. ([Typer][11])
* **Launching external apps**: `typer.launch("file-or-url")`. ([Typer][23])
* **Docs automation**: wire `typer ... utils docs` into your CI to refresh README usage blocks. ([Typer][4])

---

## 18) One last compact example (multi‑cmd, env, context, completion)

```python
# src/mycli/cli.py
import typer
from enum import Enum
from pathlib import Path
from typing import List
from typing_extensions import Annotated

app = typer.Typer(rich_markup_mode="rich")

class Format(str, Enum):
    json = "json"
    table = "table"

@app.callback()
def main(
    ctx: typer.Context,
    verbose: Annotated[int, typer.Option("--verbose", "-v", count=True)] = 0,
    profile: Annotated[str, typer.Option(envvar=["MYCLI_PROFILE", "PROFILE"])] = "default",
):
    ctx.obj = {"verbose": verbose, "profile": profile}

def complete_user(incomplete: str) -> List[str]:
    users = ["alice", "bob", "charlie"]
    return [u for u in users if u.startswith(incomplete)]

@app.command()
def ls(
    output: Annotated[Format, typer.Option(help="Output format")] = Format.table,
    data_dir: Annotated[Path, typer.Option(exists=True, dir_okay=True)] = Path("."),
    user: Annotated[str, typer.Option(autocompletion=complete_user)] = "",
):
    """List things from [bold]{data_dir}[/] for a given user."""
    print(output.value, data_dir, user)

@app.command()
def rm(
    path: Annotated[Path, typer.Argument(exists=True, file_okay=True)],
    force: Annotated[bool, typer.Option(prompt="Are you sure?", help="Force delete")] = False,
):
    if not force:
        raise typer.Abort()
    print(f"Deleting {path}")
```

This combines: callback state; envvar‑sourced option; Enum choices; path validation; completion; prompts; and a clean abort.

---

### Primary references (for further reading)

* **Typer homepage & tutorial** (features, types, options/args, subcommands, context, printing, testing): ([Typer][33])
* **`typer` command** (completion for scripts, `utils docs`, slim variant): ([Typer][4])
* **Shell completion** (Click’s engine): ([Click Documentation][18])
* **Autocompletion for values** (Typer tutorial): ([Typer][19])
* **Release notes & version info**: ([Typer][34])

If you share examples of your current custom CLI code, I can translate them 1:1 into Typer patterns and flag edge cases (e.g., mixed positional/option parsing, interactive flows, or bespoke validation) with precise drop‑in replacements.

[1]: https://typer.tiangolo.com/features/?utm_source=chatgpt.com "Features"
[2]: https://pypi.org/project/typer/?utm_source=chatgpt.com "typer"
[3]: https://typer.tiangolo.com/tutorial/testing/?utm_source=chatgpt.com "Testing"
[4]: https://typer.tiangolo.com/tutorial/typer-command/ "typer command - Typer"
[5]: https://typer.tiangolo.com/tutorial/parameter-types/?utm_source=chatgpt.com "CLI Parameter Types"
[6]: https://typer.tiangolo.com/tutorial/commands/?utm_source=chatgpt.com "Commands"
[7]: https://typer.tiangolo.com/tutorial/subcommands/add-typer/?utm_source=chatgpt.com "Add Typer"
[8]: https://typer.tiangolo.com/tutorial/commands/callback/?utm_source=chatgpt.com "Typer Callback"
[9]: https://typer.tiangolo.com/tutorial/options/required/?utm_source=chatgpt.com "Required CLI Options"
[10]: https://typer.tiangolo.com/tutorial/parameter-types/number/?utm_source=chatgpt.com "Number"
[11]: https://typer.tiangolo.com/tutorial/parameter-types/path/?utm_source=chatgpt.com "Path"
[12]: https://typer.tiangolo.com/tutorial/parameter-types/enum/?utm_source=chatgpt.com "Enum - Choices"
[13]: https://typer.tiangolo.com/tutorial/multiple-values/multiple-options/?utm_source=chatgpt.com "Multiple CLI Options"
[14]: https://typer.tiangolo.com/tutorial/arguments/envvar/?utm_source=chatgpt.com "CLI Arguments with Environment Variables"
[15]: https://typer.tiangolo.com/tutorial/prompt/?utm_source=chatgpt.com "Ask with Prompt"
[16]: https://typer.tiangolo.com/tutorial/options/callback-and-context/?utm_source=chatgpt.com "CLI Option Callback and Context"
[17]: https://typer.tiangolo.com/tutorial/one-file-per-command/?utm_source=chatgpt.com "One File Per Command"
[18]: https://click.palletsprojects.com/en/stable/shell-completion/?utm_source=chatgpt.com "Shell Completion — Click Documentation (8.3.x)"
[19]: https://typer.tiangolo.com/tutorial/options-autocompletion/?utm_source=chatgpt.com "CLI Option autocompletion"
[20]: https://typer.tiangolo.com/tutorial/printing/?utm_source=chatgpt.com "Printing and Colors"
[21]: https://typer.tiangolo.com/tutorial/progressbar/?utm_source=chatgpt.com "Progress Bar"
[22]: https://typer.tiangolo.com/tutorial/commands/help/?utm_source=chatgpt.com "Command Help - Typer"
[23]: https://typer.tiangolo.com/tutorial/launch/?utm_source=chatgpt.com "Launching Applications - Typer - Tiangolo.com"
[24]: https://typer.tiangolo.com/tutorial/commands/context/?utm_source=chatgpt.com "Using the Context"
[25]: https://typer.tiangolo.com/tutorial/app-dir/?utm_source=chatgpt.com "CLI Application Directory"
[26]: https://typer.tiangolo.com/tutorial/package/?utm_source=chatgpt.com "Building a Package"
[27]: https://typer.tiangolo.com/tutorial/using-click/?utm_source=chatgpt.com "Using Click"
[28]: https://typer.tiangolo.com/tutorial/options/version/?utm_source=chatgpt.com "Version CLI Option, is_eager"
[29]: https://typer.tiangolo.com/tutorial/parameter-types/bool/?utm_source=chatgpt.com "Boolean CLI Options"
[30]: https://typer.tiangolo.com/tutorial/commands/one-or-multiple/?utm_source=chatgpt.com "One or Multiple Commands"
[31]: https://github.com/fastapi/typer/issues/949?utm_source=chatgpt.com "`shell_complete` vs `autocompletion` · Issue #949"
[32]: https://click.palletsprojects.com/en/stable/testing/?utm_source=chatgpt.com "Testing Click Applications — Click Documentation (8.3.x)"
[33]: https://typer.tiangolo.com/?utm_source=chatgpt.com "Typer"
[34]: https://typer.tiangolo.com/release-notes/?utm_source=chatgpt.com "Release Notes"
