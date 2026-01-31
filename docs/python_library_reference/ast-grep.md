## B) CLI topology and subcommands (what exists)

### B0) Entry points, “default subcommand” behavior, and help discovery (Linux)

**Binary name on Linux:** use `ast-grep` (not `sg`). Linux already has an `sg` command (`setgroups`), so the docs explicitly recommend `ast-grep` and only using `sg` if you alias it yourself. ([Ast Grep][1])

```bash
# recommended on Linux
ast-grep --help

# optional convenience (your choice)
alias sg=ast-grep
```

**Default subcommand:** `run` is the default command. Anything that looks like `ast-grep -p ...` is actually `ast-grep run -p ...`. ([Ast Grep][2])

```bash
ast-grep -p 'foo()'          # == ast-grep run -p 'foo()'
ast-grep run -p 'foo()'
```

**Help inventory / drill-down:**

```bash
ast-grep --help              # canonical “what exists”
ast-grep help                # same intent as --help
ast-grep help run            # drill into a subcommand
ast-grep run --help          # ditto
```

The official CLI docs explicitly position `ast-grep --help` as the authoritative “up-to-date options” surface. ([Ast Grep][2])

---

## B1) `ast-grep run` — ad-hoc search / rewrite (default command)

### Contract / signature

```bash
ast-grep run [OPTIONS] --pattern <PATTERN> [PATHS]...
# PATHS defaults to "."
```

([Ast Grep][3])

### What `run` is for (operationally)

Use `run` when you want **one-off** structural search or codemod across paths *without* needing a project rule setup. It’s also the backbone of “CLI-first” workflows because `run` is the default subcommand. ([Ast Grep][3])

### Core knobs (the ones you actually reach for)

**Query + rewrite surface**

* `-p, --pattern <PATTERN>`: the AST pattern query (required). ([Ast Grep][3])
* `-r, --rewrite <FIX>`: replacement string (turns matches into candidate edits). ([Ast Grep][3])
* `-l, --lang <LANG>`: language for parsing the *pattern* (file language is inferred by extension if omitted). ([Ast Grep][2])
* `--selector <KIND>`: “match this sub-node kind” when your pattern needs context but you want a specific node matched. ([Ast Grep][3])
* `--strictness <cst|smart|ast|relaxed|signature>`: tune match strictness (defaults to `smart`). ([Ast Grep][3])
* `--debug-query[=<format>]`: print the query’s tree-sitter AST; **requires** `--lang` be set explicitly. ([Ast Grep][3])

**Input / traversal**

* `--globs <GLOBS>`: include/exclude paths; `!` excludes; later globs win. ([Ast Grep][3])
* `--no-ignore <hidden|dot|exclude|global|parent|vcs>`: bypass ignore layers (repeatable). ([Ast Grep][3])
* `--follow`: follow symlinks (with loop/broken-link handling). ([Ast Grep][3])
* `--stdin`: treat stdin as input (streaming one file-ish input). ([Ast Grep][3])

**Execution / output**

* `-i, --interactive`: interactive edit session; rewrites only happen *inside* the session. ([Ast Grep][3])
* `-U, --update-all`: apply all rewrites without confirmation. ([Ast Grep][3])
* `--json[=<pretty|stream|compact>]`: structured output; value must be passed as `--json=<STYLE>`; **conflicts with interactive**. ([Ast Grep][3])
* `--inspect <nothing|summary|entity>`: emit file/rule discovery tracing to stderr (doesn’t affect match results). ([Ast Grep][3])

### Copy/paste recipes (the “muscle memory” set)

**Search only (default PATHS = `.`):**

```bash
ast-grep -p 'foo($A)'
```

([Ast Grep][3])

**Search a specific subtree set with globs (include then exclude):**

```bash
ast-grep -p 'foo($A)' \
  --globs 'src/**/*.ts' \
  --globs '!src/**/__generated__/**'
```

([Ast Grep][3])

**Rewrite + apply-all (classic codemod shape):**

```bash
ast-grep -p 'var $X = $Y' -r 'let $X = $Y' -l js -U
```

(Example pattern/rewrite style is the documented canonical usage shape.) ([Ast Grep][4])

**Rewrite + interactive review/edit:**

```bash
ast-grep -p 'var $X = $Y' -r 'let $X = $Y' -l js -i
```

([Ast Grep][3])

**Debug why a pattern parses/matches unexpectedly (prints query AST):**

```bash
ast-grep -p 'obj.val && obj.val()' -l ts --debug-query=sexp
```

([Ast Grep][3])

**Run can still benefit from project config (when present):** `run` does *not* require `sgconfig.yml`, but project discovery / `--config` still apply (e.g., for custom languages / language globs). ([Ast Grep][5])

---

## B2) `ast-grep scan` — project scan / lint / batch rewrite via config or rule inputs

### Contract / signature

```bash
ast-grep scan [OPTIONS] [PATHS]...
# PATHS defaults to "."
```

([Ast Grep][6])

### What `scan` is for (operationally)

Use `scan` when you want **repeatable** analysis/rewrite over a repo: run many rules, apply severity, produce CI-friendly formats, and share rule packs via `sgconfig.yml`. ([Ast Grep][6])

### Config discovery (how `scan` decides what “project” means)

* `scan` **requires** an `sgconfig.yml` in the project root (or provided via `--config`). ([Ast Grep][5])
* It searches upward from the current directory to find `sgconfig.yml`. ([Ast Grep][5])
* Use inspection to confirm what it found:

```bash
ast-grep scan --inspect summary
```

([Ast Grep][5])

### Rule selection modes (scan’s “routing table”)

**1) Project mode (default):** run all rules under `ruleDirs` from `sgconfig.yml`. ([Ast Grep][5])

```bash
ast-grep scan
```

**2) Single rule file mode:** `--rule <RULE_FILE>` runs exactly one YAML rule file **without project setup**, but it **conflicts with `--config`**. ([Ast Grep][6])

```bash
ast-grep scan --rule rules/no-alert.yml src/
```

**3) Inline rule mode:** `--inline-rules <RULE_TEXT>` runs a rule passed as text; multiple rules can be separated with `---`; **incompatible with `--rule`**. ([Ast Grep][6])

```bash
ast-grep scan --inline-rules "$(cat <<'YAML'
id: no-alert
language: JavaScript
rule:
  pattern: alert($MSG)
severity: warning
message: Avoid alert.
YAML
)" src/
```

(Inline rules are explicitly supported, including multi-rule `---` separation.) ([Ast Grep][6])

**4) Filter subset of project rules:** `--filter <REGEX>` selects rule IDs matching a regex; **conflicts with `--rule`**. ([Ast Grep][6])

```bash
ast-grep scan --filter '^security\.' .
```

### Output + CI integration surface

* `--report-style <rich|medium|short>`: tune terminal verbosity (default `rich`). ([Ast Grep][2])
* `--format <github|sarif>`: emit diagnostics for GitHub annotations or SARIF pipelines. ([Ast Grep][2])
* `--json[=<pretty|stream|compact>]`: machine-readable matches. ([Ast Grep][2])
* `--include-metadata`: include rule metadata **only in JSON mode**. ([Ast Grep][6])

### Rewrite application surface (scan can codemod too)

* `--interactive`: interactive edit session. ([Ast Grep][6])
* `--update-all`: apply all rewrites without confirmation. ([Ast Grep][2])

### Severity overrides (runtime control plane)

`scan` has a CLI-level severity override layer:

* `--error [RULE_ID...]`
* `--warning [RULE_ID...]`
* `--info [RULE_ID...]`
* `--hint [RULE_ID...]`
* `--off [RULE_ID...]`

These let you “reshape” severity without editing rule YAML. ([Ast Grep][2])

```bash
# turn off a noisy rule at runtime
ast-grep scan --off no-alert

# promote some rules to error for CI
ast-grep scan --error security.no-eval security.no-sql-injection --format github
```

([Ast Grep][2])

---

## B3) `ast-grep test` — rule tests + snapshot workflow

### Contract / signature

```bash
ast-grep test [OPTIONS]
```

([Ast Grep][7])

### What `test` is for

`test` executes rule test cases and verifies expected outputs via snapshots (updateable en masse or interactively). ([Ast Grep][7])

### Key knobs

* `-c, --config <CONFIG>`: config path (default `sgconfig.yml`). ([Ast Grep][7])
* `-t, --test-dir <TEST_DIR>`: where to discover test YAMLs. ([Ast Grep][7])
* `--snapshot-dir <SNAPSHOT_DIR>`: snapshot directory name. ([Ast Grep][7])
* `--skip-snapshot-tests`: validate tests without checking rule output (**conflicts with** `--update-all`). ([Ast Grep][7])
* `-U, --update-all`: rewrite all changed snapshots (**conflicts with** `--skip-snapshot-tests`). ([Ast Grep][7])
* `-i, --interactive`: interactive review to update snapshots selectively. ([Ast Grep][7])
* `-f, --filter <FILTER>`: glob filter for selecting test cases. ([Ast Grep][7])
* `--include-off`: include `severity: off` rules in tests (normally skipped). ([Ast Grep][7])

### Copy/paste recipes

```bash
ast-grep test                 # run full suite (default config discovery)
ast-grep test -f 'python/**'  # narrow by glob
ast-grep test -U              # update all snapshots
ast-grep test -i              # update snapshots interactively
ast-grep test --skip-snapshot-tests
```

([Ast Grep][7])

---

## B4) `ast-grep new` — scaffolding (project + rule + test + util)

### Contract / signature

```bash
ast-grep new [COMMAND] [OPTIONS] [NAME]
```

([Ast Grep][8])

### Subcommands (what it can scaffold)

* `project`: create project scaffold (`sgconfig.yml`, `rules/`, `rule-tests/`, `utils/` by default; folder names are customizable). ([Ast Grep][8])
* `rule`: create a new rule in one of the configured `ruleDirs`; prompts you when multiple are configured; `--yes` auto-picks the first. ([Ast Grep][8])
* `test`: create a new test case in one of the configured test dirs; similar prompting / `--yes` behavior. ([Ast Grep][8])
* `util`: create a global utility rule in one of the configured util dirs; similar prompting / `--yes`. ([Ast Grep][8])

### Options you actually use

* `-b, --base-dir <BASE_DIR>`: create scaffold in a target directory (default `.`). ([Ast Grep][2])
* `-l, --lang <LANG>`: required for `new rule` and `new util`. ([Ast Grep][8])
* `-y, --yes`: non-interactive defaults (requires you to supply required args via CLI). ([Ast Grep][8])
* `-c, --config <CONFIG_FILE>`: point at a specific root config (documented in the `new` reference). ([Ast Grep][8])

### Copy/paste recipes

```bash
# scaffold a new repo setup (interactive)
ast-grep new project

# scaffold into a subfolder
ast-grep new project -b tooling/ast-grep

# create a rule non-interactively
ast-grep new rule no-alert -l JavaScript -y -c sgconfig.yml

# create a test case
ast-grep new test no-alert-basic -y -c sgconfig.yml
```

([Ast Grep][8])

---

## B5) `ast-grep lsp` — language server (editor diagnostics)

### Contract / signature

```bash
ast-grep lsp
ast-grep lsp -c <CONFIG_FILE>
```

([Ast Grep][2])

### Operational reality

* LSP-driven diagnostics/code actions require `sgconfig.yml` in your workspace/project root. ([Ast Grep][9])
* The docs explicitly show specifying config path via CLI: `ast-grep lsp -c <configPath>`. ([Ast Grep][9])

### Copy/paste recipes

```bash
# run from project root (where sgconfig.yml is discoverable)
cd /path/to/repo
ast-grep lsp

# explicit config path (editor running in a different root, monorepo subdir, etc.)
ast-grep lsp -c path/to/sgconfig.yml
```

([Ast Grep][9])

---

## B6) `ast-grep completions` — generate shell completion scripts

### Contract / signature

```bash
ast-grep completions [SHELL]
# SHELL ∈ {bash, elvish, fish, powershell, zsh}
# If omitted, inferred from environment
```

([Ast Grep][2])

### Copy/paste recipes (ephemeral)

```bash
# Bash (current shell session)
source <(ast-grep completions bash)

# Zsh
source <(ast-grep completions zsh)

# Fish
ast-grep completions fish | source
```

([Ast Grep][2])

*(Persisting completions is distro/shell-framework specific; the only guaranteed contract is “prints completion script to stdout for a given shell”.)* ([Ast Grep][2])

---

## B7) “Docs generation” surfaces (status: not part of current CLI reference)

Your earlier note about “docs generation” is real historically: some help inventories (e.g., packaging-related issues) list a `docs` subcommand described as “Generate rule docs… (Not Implemented Yet)”. ([GitHub][10])
However, the current official CLI reference page lists `run/scan/test/new/lsp/completions/help` and does **not** include `docs`. ([Ast Grep][2])

**Action:** treat `docs` as *version-dependent*. If you care about it, confirm against your installed binary:

```bash
ast-grep --help | sed -n '1,120p'
```

([Ast Grep][2])

[1]: https://ast-grep.github.io/guide/quick-start.html "Quick Start | ast-grep"
[2]: https://ast-grep.github.io/reference/cli.html "Command Line Reference | ast-grep"
[3]: https://ast-grep.github.io/reference/cli/run.html "ast-grep run | ast-grep"
[4]: https://ast-grep.github.io/guide/introduction.html?utm_source=chatgpt.com "What is ast-grep?"
[5]: https://ast-grep.github.io/guide/project/project-config.html "Project Configuration | ast-grep"
[6]: https://ast-grep.github.io/reference/cli/scan.html "ast-grep scan | ast-grep"
[7]: https://ast-grep.github.io/reference/cli/test.html "ast-grep test | ast-grep"
[8]: https://ast-grep.github.io/reference/cli/new.html "ast-grep new | ast-grep"
[9]: https://ast-grep.github.io/guide/tools/editors.html "Editor Integration | ast-grep"
[10]: https://github.com/microsoft/winget-pkgs/issues/233180?utm_source=chatgpt.com "[Package Issue]: ast-grep.ast-grep - microsoft/winget-pkgs"

## C) “Run mode” (ad-hoc) feature surface (`ast-grep run`)

### C0) Command contract + “what it does”

`run` is the **one-off** search/rewrite command (and the default subcommand). Syntax:

```bash
ast-grep run [OPTIONS] --pattern <PATTERN> [PATHS]...
# PATHS defaults to "."
```

([Ast Grep][1])

**Exit codes (scripting-friendly):**

* `0`: at least one match
* `1`: no matches
  ([Ast Grep][1])

---

## C1) Pattern query + rewrite: `--pattern/-p`, `--rewrite/-r`

### C1.1 Pattern is *code*, not text

`--pattern` is an AST pattern, written like normal code in the target language. The pattern must be valid code that tree-sitter can parse. ([Ast Grep][1])

```bash
# search only
ast-grep run -p 'console.log($X)' src/
```

### C1.2 Meta variables (the “wildcards” you use constantly)

Meta variables start with `$` and a name made of **A–Z / `_` / digits**; they match a **single AST node**. ([Ast Grep][2])

```bash
# match any argument expression
ast-grep run -p 'console.log($ARG)' .
```

Multi-meta `$$$` matches **zero or more** nodes (useful for args/params/statement lists). ([Ast Grep][2])

```bash
# match any number of args
ast-grep run -p 'console.log($$$ARGS)' .
```

### C1.3 Rewriting: `--rewrite` replaces the *matched node*

`--rewrite/-r` is a string replacement applied to the matched AST node. Minimal example (rename identifier): ([Ast Grep][1])

```bash
ast-grep run --pattern 'foo' --rewrite 'bar' --lang python .
```

Operational behavior:

* `--rewrite` shows a diff of proposed changes
* with `--interactive`, it prompts you to apply changes
* with `--update-all/-U`, it applies without confirmation ([Ast Grep][3])

---

## C2) Language selection: `--lang/-l` + extension inference

### C2.1 What `--lang` actually sets

`--lang/-l` is the **language of the pattern query**. If omitted, ast-grep infers language **based on the file extension**. ([Ast Grep][4])

```bash
# explicit language for pattern parsing
ast-grep run -p '$PROP && $PROP()' -l ts TypeScript/src
```

(Equivalent long/short forms are shown in the docs.) ([Ast Grep][5])

### C2.2 StdIn is special: you *must* provide a language

In `--stdin` mode, **there is no file extension**, so for `run` you must specify `--lang/-l`. ([Ast Grep][6])

```bash
echo "print('Hello world')" | ast-grep run --lang python --stdin -p 'print($X)'
```

---

## C3) Pattern parsing introspection: `--debug-query[=<format>]`

Use this when you’re asking: *“why didn’t it match?”* or *“what node kind should I target with `--selector`?”*

**Key constraint:** `--debug-query` requires `--lang` be set explicitly. ([Ast Grep][1])

```bash
ast-grep run -l ts -p 'a?.b()' --debug-query
```

**Formats (pick the one that helps):**

* `pattern`: parsed query in Pattern format
* `ast`: tree-sitter AST (named nodes only)
* `cst`: tree-sitter CST (named + unnamed nodes)
* `sexp`: S-expression form ([Ast Grep][1])

```bash
ast-grep run -l ts -p 'a?.b()' --debug-query=sexp
ast-grep run -l ts -p 'a?.b()' --debug-query=cst
```

---

## C4) Matching algorithm controls: `--strictness` + `--selector`

### C4.1 `--strictness`: tune how “exact” the match is

`--strictness` controls match strictness (default `smart`). ([Ast Grep][1])

Values:

* `cst`: match exact all nodes
* `smart`: match all nodes except “trivial” source nodes
* `ast`: match only AST nodes
* `relaxed`: match AST nodes except comments
* `signature`: match AST nodes except comments, **without text** ([Ast Grep][1])

Practical workflow:

```bash
# start permissive-ish
ast-grep run -p 'foo($X)' --strictness=smart .

# tighten if you’re getting “too many” matches
ast-grep run -p 'foo($X)' --strictness=ast .
```

### C4.2 `--selector`: match a sub-node kind inside a bigger pattern

`--selector <KIND>` extracts a specific AST kind from the pattern to become the actual matcher. Use it when your pattern needs context, but you want the match anchored on a particular node type. ([Ast Grep][1])

**Actionable workflow (repeatable):**

1. Get the pattern’s tree-sitter structure:

```bash
ast-grep run -l <lang> -p '<pattern>' --debug-query=cst
```

2. Identify the node kind you want.
3. Re-run with `--selector <KIND>`:

```bash
ast-grep run -l <lang> -p '<pattern>' --selector <KIND> .
```

---

## C5) Rewrite application UX: interactive, apply-all, concurrency

### C5.1 `--interactive/-i`: review & selectively apply

Starts an interactive edit session: confirm changes, apply selectively, or open an editor to tweak matched code. Rewrite only happens inside the session. ([Ast Grep][1])

```bash
ast-grep run -l js -p 'var $X = $Y' -r 'let $X = $Y' -i src/
```

### C5.2 `--update-all/-U`: apply everything without prompts

```bash
ast-grep run -l js -p 'var $X = $Y' -r 'let $X = $Y' -U src/
```

([Ast Grep][1])

### C5.3 `--threads/-j`: throughput knob

Sets approximate thread count; `0` (default) lets ast-grep pick via heuristics. ([Ast Grep][1])

```bash
ast-grep run -p 'foo($X)' -j 8 .
```

---

## C6) Output shaping: JSON + context + headings + color

### C6.1 JSON output: `--json[=<pretty|stream|compact>]`

Enables structured output; default is “pretty” if you don’t pass a value. It **conflicts with interactive**. ([Ast Grep][1])

```bash
# pretty array (default)
ast-grep run -p 'Some($A)' -r 'None' --json .

# NDJSON (one object per line) for pipelines
ast-grep run -p 'Some($A)' --json=stream .

# compact single-line JSON array
ast-grep run -p 'Some($A)' --json=compact .
```

**Critical gotcha:** you must use `--json=<STYLE>` (equals sign). `--json stream` is parsed incorrectly. ([Ast Grep][7])

**What’s in the match object (core fields):**
`text`, `range`, `file`, surrounding `lines`, optional `replacement`, and optional `metaVariables`. ([Ast Grep][7])

**Common pipeline pattern (`jq`):**

```bash
ast-grep run -p 'Some($A)' -r 'None' --json \
  | jq '.[].replacement'
```

([Ast Grep][6])

### C6.2 Context lines: `-A/-B/-C`

* `-A, --after <NUM>` lines after match (conflicts with `-C`)
* `-B, --before <NUM>` lines before match (conflicts with `-C`)
* `-C, --context <NUM>` lines around match (equivalent to `-A NUM -B NUM`) ([Ast Grep][1])

```bash
ast-grep run -p 'dangerous($X)' -C 2 .
```

### C6.3 Headings: `--heading <auto|always|never>`

Controls whether file name prints once as a heading vs prefixing each match line. Default `auto` (TTY-aware). ([Ast Grep][1])

```bash
ast-grep run -p 'foo($X)' --heading=always .
```

### C6.4 Color: `--color <auto|always|ansi|never>`

Default `auto` (TTY-aware). Mentions suppression when piped / `TERM=dumb` / `NO_COLOR`. ([Ast Grep][1])

```bash
# force ANSI colors even when piping (useful with fzf/bat previews)
ast-grep run -p 'foo($X)' --color=always .
```

---

## C7) File traversal controls (shared mental model with `scan`)

### C7.1 Ignore handling: `--no-ignore <FILE_TYPE>` (repeatable)

Disables ignore layers; pass multiple times to suppress multiple sources. Values include:

* `hidden`: include hidden files/dirs (normally skipped)
* `dot`: ignore `.ignore` files
* `exclude`: ignore repo excludes like `.git/info/exclude`
* `global`: ignore global git ignores (e.g. core.excludesFile)
* `parent`: ignore ignore-files in parent dirs
* `vcs`: ignore VCS ignore files (implies `parent` for VCS files; `.ignore` still respected) ([Ast Grep][1])

```bash
# include hidden + ignore .gitignore effects
ast-grep run -p 'foo($X)' --no-ignore hidden --no-ignore vcs .
```

### C7.2 Globs: `--globs <GLOBS>` (include/exclude, last-wins)

* Overrides other ignore logic
* Multiple globs allowed
* `!` prefix excludes
* later flags take precedence ([Ast Grep][1])

```bash
ast-grep run -p 'foo($X)' \
  --globs 'src/**/*.py' \
  --globs '!src/**/vendor/**' \
  .
```

### C7.3 Symlinks: `--follow`

Follows symlinks; detects loops; reports broken links. ([Ast Grep][1])

```bash
ast-grep run -p 'foo($X)' --follow .
```

### C7.4 StdIn mode: `--stdin` (pipes/subprocesses)

Enabled by passing `--stdin`; docs call out operational caveats:

* conflicts with `--interactive` (both read stdin)
* `run` requires `--lang` (no extension to infer language)
* if you run `--stdin` in a tty, it can “hang” until you send EOF (Ctrl-D) ([Ast Grep][6])

```bash
curl -s https://example.com \
  | ast-grep run --stdin --lang html -p '<div $$$> $$$ <i>$AUTHORS</i> </div>' --json=stream \
  | jq '.metaVariables.single.AUTHORS.text'
```

(StdIn + JSON pipelines are explicitly documented.) ([Ast Grep][6])

---

## C8) Debugging discovery itself: `--inspect <nothing|summary|entity>`

`--inspect` traces internal file filtering to **stderr** (doesn’t change match results). Useful when you suspect ignores/globs are excluding files unexpectedly. ([Ast Grep][1])

```bash
ast-grep run -p 'foo($X)' --inspect=summary .
ast-grep run -p 'foo($X)' --inspect=entity  .
```

---

If you want, the next deep-dive section after this is usually **D) scan mode** (because it reuses most of the above knobs but adds rule loading, formats like SARIF/GitHub, severity overrides, and `sgconfig.yml` routing).

[1]: https://ast-grep.github.io/reference/cli/run.html "ast-grep run | ast-grep"
[2]: https://ast-grep.github.io/guide/pattern-syntax.html "Pattern Syntax | ast-grep"
[3]: https://ast-grep.github.io/guide/rewrite-code.html "Rewrite Code | ast-grep"
[4]: https://ast-grep.github.io/reference/cli.html "Command Line Reference | ast-grep"
[5]: https://ast-grep.github.io/guide/quick-start.html?utm_source=chatgpt.com "Quick Start"
[6]: https://ast-grep.github.io/guide/tooling-overview.html "Command Line Tooling Overview | ast-grep"
[7]: https://ast-grep.github.io/guide/tools/json.html "JSON Mode | ast-grep"

## D) “Scan mode” (project linting) feature surface (`ast-grep scan`)

### D0) Contract + prerequisites (what makes `scan` different from `run`)

`scan` is the **project-mode linter/workflow**: it loads **rules from config** (or rule inputs), runs them over a codebase, and can rewrite code when fixes are defined. Usage:

```bash
ast-grep scan [OPTIONS] [PATHS]...
# PATHS defaults to "."
```

([Ast Grep][1])

**Minimum scaffolding:** `scan` requires (a) `sgconfig.yml` and (b) at least one rule directory (commonly `rules/`). The official “Scan Your Project” guide calls these out explicitly. ([Ast Grep][2])

**Exit codes (important for CI scripts):**

* `1` if **at least one rule matches**
* `0` if **no rules match**
  ([Ast Grep][1])

*(This is intentionally “grep-like gating”: “findings exist” → non-zero.)*

---

## D1) Config-driven scanning: `--config` and project discovery

### D1.1 Root config discovery rules

By default, `scan` looks for `sgconfig.yml` starting at CWD and walking **up** the directory tree until it finds one. You can override with `--config`. ([Ast Grep][3])

```bash
# default: find sgconfig.yml by walking upward from CWD
ast-grep scan

# explicitly point to a config (monorepo / tooling dir / CI checkout layout)
ast-grep scan --config path/to/sgconfig.yml
```

([Ast Grep][3])

### D1.2 “Where is it scanning from?”: `--inspect summary`

Use `--inspect summary` to print the discovered **projectDir** and config path. ([Ast Grep][3])

```bash
ast-grep scan --inspect summary
# stderr-style trace includes projectDir and isProject=true
```

([Ast Grep][3])

---

## D2) Rule selection modes (how `scan` decides what rules to run)

### D2.1 Full project ruleDirs (default)

Default mode loads **all rule YAML files** under directories listed in `sgconfig.yml`’s `ruleDirs`. ([Ast Grep][3])

```bash
# run the full project ruleset
ast-grep scan
```

### D2.2 Single rule file: `--rule <RULE_FILE>`

Runs **exactly one** rule file *without project setup*. This flag **conflicts with `--config`** (by design). ([Ast Grep][1])

```bash
ast-grep scan --rule rules/no-eval.yml src/
```

([Ast Grep][1])

### D2.3 Inline rules: `--inline-rules <RULE_TEXT>` (multi-rule supported via `---`)

Run a rule directly from CLI text (handy for scripting or rapid iteration). You can provide **multiple rules** separated by `---`. `--inline-rules` is **incompatible with `--rule`**. ([Ast Grep][1])

```bash
ast-grep scan --inline-rules "$(cat <<'YAML'
id: no-await-in-promise-all
language: TypeScript
rule:
  pattern: Promise.all($A)
  has:
    pattern: await $_
    stopBy: end
severity: warning
message: Avoid awaiting inside Promise.all.
YAML
)" src/
```

([Ast Grep][4])

**Multi-rule form (same invocation):**

```bash
ast-grep scan --inline-rules "$(cat <<'YAML'
id: rule-one
language: JavaScript
rule: { pattern: alert($MSG) }
severity: warning
message: Avoid alert.
---
id: rule-two
language: JavaScript
rule: { pattern: confirm($MSG) }
severity: warning
message: Avoid confirm.
YAML
)" src/
```

([Ast Grep][1])

### D2.4 Subset by rule id regex: `--filter <REGEX>`

Loads project rules but only runs those whose **`id` matches REGEX**. Conflicts with `--rule`. ([Ast Grep][1])

```bash
# run only security.* rules
ast-grep scan --filter '^security\.' .
```

([Ast Grep][1])

---

## D3) File discovery and traversal (the scan-time “input router”)

### D3.1 Ignore-stack behavior and overrides: `--no-ignore <FILE_TYPE>` (repeatable)

`scan` respects hidden-file skipping and ignore files (`.gitignore`, `.ignore`, etc.) unless overridden. You can suppress specific ignore layers by repeating `--no-ignore`. ([Ast Grep][1])

Values:

* `hidden`, `dot`, `exclude`, `global`, `parent`, `vcs` ([Ast Grep][1])

```bash
# include hidden files and ignore VCS ignore files
ast-grep scan --no-ignore hidden --no-ignore vcs .
```

([Ast Grep][1])

### D3.2 Glob include/exclude: `--globs <GLOBS>` (always overrides ignore logic)

`--globs` is the “hard override”:

* matches `.gitignore`-style globs
* prefix `!` excludes
* later globs win if multiple match
* always overrides other ignore logic ([Ast Grep][1])

```bash
ast-grep scan \
  --globs 'src/**/*.ts' \
  --globs '!src/**/__generated__/**' \
  .
```

([Ast Grep][1])

### D3.3 Follow symlinks with loop detection: `--follow`

Enables symlink traversal; ast-grep checks loops and reports broken links. ([Ast Grep][1])

```bash
ast-grep scan --follow .
```

([Ast Grep][1])

### D3.4 StdIn mode: `--stdin`

Consumes code from standard input (useful when ast-grep is in a pipe). ([Ast Grep][1])

```bash
cat src/foo.ts | ast-grep scan --stdin --rule rules/no-eval.yml
```

**Operational footgun:** in a TTY, `--stdin` will “hang” until you send EOF (Ctrl-D). ([Ast Grep][5])

---

## D4) Scan outputs & integrations (human + CI)

### D4.1 Report verbosity: `--report-style rich|medium|short`

* `rich`: formatted diagnostics with code previews
* `medium`: condensed (line, severity, message, notes)
* `short`: minimal (line, severity, message) ([Ast Grep][1])

```bash
ast-grep scan --report-style=medium
```

([Ast Grep][1])

### D4.2 CI/annotations formats: `--format github|sarif`

`scan` can emit diagnostics in:

* **GitHub Action** annotation format (`github`)
* **SARIF** (`sarif`) ([Ast Grep][1])

```bash
# GitHub annotations
ast-grep scan --format github

# SARIF output (feed to code scanning upload steps)
ast-grep scan --format sarif
```

([Ast Grep][1])

If you’re wiring this into GitHub Code Scanning: GitHub supports SARIF (subset of SARIF 2.1.0). ([GitHub Docs][6])

### D4.3 JSON output (and rule metadata): `--json[=pretty|stream|compact]` + `--include-metadata`

JSON modes:

* `--json` == `--json=pretty`
* `--json=stream` prints one JSON object per line (NDJSON-ish)
* `--json=compact` single-line array
  Also: `--json=<STYLE>` requires `=`; and JSON conflicts with `--interactive`. ([Ast Grep][1])

```bash
# stream mode for pipelines
ast-grep scan --json=stream | jq -c '.file + ":" + (.range.start.line|tostring)'
```

([Ast Grep][1])

`--include-metadata` attaches each rule’s `metadata` block into JSON output and **requires JSON mode**. ([Ast Grep][1])

```bash
ast-grep scan --json=stream --include-metadata
```

([Ast Grep][1])

---

## D5) Rewrite application UX in scan mode: `--interactive`, `--update-all/-U`, `--threads/-j`

Scan can apply fixes (from rules that define fix/rewrites):

* `-i, --interactive`: confirm/apply selectively; can open editor; rewrites only happen inside the session ([Ast Grep][1])
* `-U, --update-all`: apply all rewrites without confirmation ([Ast Grep][1])
* `-j, --threads <NUM>`: concurrency; `0` default uses heuristics ([Ast Grep][1])

```bash
# review + apply selectively
ast-grep scan -i

# apply everything (codemod mode)
ast-grep scan -U

# throttle/boost parallelism
ast-grep scan -j 8
```

([Ast Grep][1])

---

## D6) Runtime severity override knobs: `--error/--warning/--info/--hint/--off`

These flags let you override rule severities **per run**. Key behaviors from the scan reference:

* You can specify multiple rules by repeating flags (e.g., `--error=R1 --error=R2`)
* If you provide **no RULE_ID**, it applies to **all rules** (e.g., `--error` sets all rules to error)
* These flags **must use `=`** to pass values (`--error=rule-id`) ([Ast Grep][1])

```bash
# promote specific rules to error (CI gating)
ast-grep scan --error=security.no-eval --error=security.no-sql-injection

# demote a noisy rule
ast-grep scan --off=noisy.rule.id

# set ALL rules to warning for a one-off audit run
ast-grep scan --warning
```

([Ast Grep][1])

Severity semantics (what “error” means): the severity guide documents levels and notes that triggering an `error` is useful for failing scans in CI. ([Ast Grep][7])

---

## D7) Observability / debugging: `--inspect` (why files/rules were included/skipped)

`--inspect <nothing|summary|entity>` prints tracing to **stderr**, does **not** affect matching results, and is specifically intended to explain **how many and why files and rules are scanned/skipped**. Output format is documented as:

```
sg: <GRANULARITY>|<ENTITY_TYPE>|<ENTITY_IDENTIFIERS...>: KEY=VAL
```

([Ast Grep][1])

```bash
# quick “what project/config did you find?”
ast-grep scan --inspect summary

# per-file/per-rule tracing (debugging filters, globs, rule selection)
ast-grep scan --inspect entity
```

([Ast Grep][1])

[1]: https://ast-grep.github.io/reference/cli/scan.html "ast-grep scan | ast-grep"
[2]: https://ast-grep.github.io/guide/scan-project.html "Scan Your Project! | ast-grep"
[3]: https://ast-grep.github.io/guide/project/project-config.html "Project Configuration | ast-grep"
[4]: https://ast-grep.github.io/guide/rule-config.html?utm_source=chatgpt.com "Rule Essentials"
[5]: https://ast-grep.github.io/guide/tooling-overview.html "Command Line Tooling Overview | ast-grep"
[6]: https://docs.github.com/en/code-security/reference/code-scanning/sarif-support-for-code-scanning?utm_source=chatgpt.com "SARIF support for code scanning"
[7]: https://ast-grep.github.io/guide/project/severity.html "Handle Error Reports | ast-grep"

## F) Pattern syntax (the “query language” for `--pattern` and `rule.pattern`)

### F0) First principle: patterns are parsed as code (Tree-sitter), not text

ast-grep **parses your pattern into an AST** (via Tree-sitter) and matches that structure against the target code’s AST. Because of that:

* Your pattern snippet **must be valid code that Tree-sitter can parse** (for the target language). ([Ast Grep][1])
* If parsing is ambiguous or incomplete, your first escalation should be a **pattern object** (`context` + optional `selector` + optional `strictness`) to give the parser enough surrounding structure. ([Ast Grep][1])

**Fast verification loop (CLI):**

```bash
# 1) force language and visualize how the pattern parses
ast-grep run -l js -p 'a: 123' --debug-query=cst

# 2) if it parses “wrong”, add context via YAML pattern object (scan/rules),
# or change your pattern to include disambiguating syntax.
```

(`--debug-query` requires `--lang` and shows parse shape.) ([Ast Grep][2])

---

## F1) Meta variables: `$NAME` (single-node wildcard + capture)

### F1.1 Naming rules (strict)

A meta variable:

* starts with `$`
* followed by a name composed of **uppercase letters `A-Z`, underscore `_`, or digits `1-9`**
* matches **exactly one AST node** (think “AST wildcard”, not regex text). ([Ast Grep][1])

From the docs:

* **Valid**: `$META`, `$META_VAR`, `$META_VAR1`, `$_`, `$_123`
* **Invalid**: `$invalid`, `$Svalue`, `$123`, `$KEBAB-CASE`, `$` ([Ast Grep][1])

### F1.2 Single-node behavior (most common “why didn’t it match?”)

Because `$X` matches **one** node, patterns that imply multiple nodes won’t match.

Example:

```bash
# matches calls with exactly one argument node
ast-grep run -p 'console.log($ARG)' src/
```

This won’t match `console.log(a, b)` (too many args) or `console.log()` (missing arg) because `$ARG` is a single node. ([Ast Grep][1])

---

## F2) Multi meta variables: `$$$` and `$$$NAME` (zero-or-more consecutive nodes)

### F2.1 What `$$$` matches

`$$$` matches **zero or more AST nodes** in list-like positions (arguments, parameters, statements). ([Ast Grep][1])

Concrete examples (arguments):

```bash
# matches console.log() / console.log(x) / console.log(a, b, c) / console.log(...args)
ast-grep run -p 'console.log($$$)' src/
```

The docs explicitly call out that `$$$` can match zero nodes, one node, many nodes, and even spread. ([Ast Grep][1])

### F2.2 Capturing the list: `$$$NAME`

You can **name** multi metavars to capture the full list of matched nodes as a sequence (e.g., `$$$ARGS`). ([Ast Grep][1])

Example (function params + body statements):

```bash
ast-grep run -p 'function $FUNC($$$ARGS) { $$$ }' src/
```

In this pattern, `ARGS` is populated with a list of parameter nodes; the docs show it matching `noop()` (empty list) and multi-arg functions. ([Ast Grep][1])

### F2.3 How `$$$NAME` “chooses” how many nodes to eat

Multi metavars have special matching behavior: once they start matching, they keep matching nodes **until the first AST node after the metavariable in your pattern can match**. (The docs compare the behavior to “non-greedy” regex-ish mechanics and TypeScript `infer` intuition.) ([Ast Grep][3])

Practical implication:

* Put an “anchor” AST node **after** `$$$...` when you want to limit what it consumes.
* If there’s no anchor (e.g., `console.log($$$)`), it can consume the whole list position.

---

## F3) Capture semantics: equality, non-capturing, and unnamed nodes

### F3.1 Repeated metavars enforce equality (captured-by-name)

If you reuse the same captured metavariable name, it must match the **same AST subtree**.

```bash
ast-grep run -p '$A == $A' src/
```

This matches `a == a` and `1 + 1 == 1 + 1`, but not `a == b`. ([Ast Grep][1])

### F3.2 Non-capturing metavars: names starting with `_`

Any metavariable whose name starts with underscore (e.g., `$_FUNC`) is **not captured**, meaning:

* repeated occurrences do **not** enforce equality
* it can be faster (no capture bookkeeping). ([Ast Grep][1])

Example from the docs:

```bash
# both occurrences can match different things because it is non-capturing
ast-grep run -p '$_FUNC($_FUNC)' src/
```

This matches any one-arg call (and spread call) regardless of whether callee/arg are the same. ([Ast Grep][1])

### F3.3 Capturing unnamed nodes: `$$VAR`

`$META` captures **named** Tree-sitter nodes by default. To capture **unnamed** nodes, use `$$VAR`. ([Ast Grep][1])

Why you care: operators / punctuation / certain keywords are often **unnamed** nodes, so `$OP` won’t match them, but `$$OP` can.

Canonical example (operator field in a binary expression):

```yaml
rule:
  kind: binary_expression
  has:
    field: operator
    pattern: $$OP
```

The docs explicitly note `$OP` fails here because the operator is unnamed; `$$OP` is required. ([Ast Grep][3])

---

## F4) “Object-style patterns” (context + selector + strictness) to disambiguate parsing

### F4.1 When you need a pattern object

Use a pattern object when the “string pattern” is:

* **invalid/incomplete** without surrounding syntax (e.g., JSON `"a": 123` needs `{ ... }`) ([Ast Grep][3])
* **ambiguous** (grammar can parse it multiple ways) ([Ast Grep][3])
* parsed into the “wrong” node kind for your intent (common in JS/TS). ([Ast Grep][4])

### F4.2 The object fields and what they mean

In YAML, `pattern` can be either:

* a **String** (simple)
* an **Object** with:

  * `context` (required): surrounding code that parses correctly
  * `selector` (optional): the **node kind** inside the parsed context you want to treat as “the actual pattern”
  * `strictness` (optional): match algorithm tuning (`cst|smart|ast|relaxed|signature`) ([Ast Grep][5])

Rule reference confirms that patterns parse “as a standalone file” by default, and `selector` is the mechanism to “pull out” the subpart you actually want to match. ([Ast Grep][5])

### F4.3 Example: JS class field vs assignment expression

String pattern like `$FIELD = $INIT` is ambiguous in JS and will parse as an `assignment_expression` unless you provide class context. Pattern object fixes it by parsing a full class and selecting `field_definition`. ([Ast Grep][4])

```yaml
pattern:
  selector: field_definition
  context: class A { $FIELD = $INIT }
```

### F4.4 Example: JSON pair needs braces

Incomplete pattern:

```yaml
pattern: '"a": 123'    # fails to parse as JSON pair
```

Fix with context + selector:

```yaml
pattern:
  context: '{ "a": 123 }'
  selector: pair
```

(Shown in the pattern deep dive as the canonical solution for incomplete JSON snippets.) ([Ast Grep][3])

---

## F5) Recommended “pattern authoring” workflow (fast + reliable)

1. Start with a minimal `run` pattern:

```bash
ast-grep run -l <lang> -p '<pattern>' <paths>
```

2. If it doesn’t match, verify parse:

```bash
ast-grep run -l <lang> -p '<pattern>' --debug-query=cst
```

3. If parsing is wrong/ambiguous/incomplete, move the pattern into a **rule YAML** and switch to a **pattern object** (`context` + `selector`) as your first escalation. ([Ast Grep][1])

[1]: https://ast-grep.github.io/guide/pattern-syntax.html "Pattern Syntax | ast-grep"
[2]: https://ast-grep.github.io/reference/cli/run.html "ast-grep run | ast-grep"
[3]: https://ast-grep.github.io/advanced/pattern-parse.html "Deep Dive into ast-grep's Pattern Syntax | ast-grep"
[4]: https://ast-grep.github.io/guide/rule-config/atomic-rule.html "Atomic Rule | ast-grep"
[5]: https://ast-grep.github.io/reference/rule.html "Rule Object Reference | ast-grep"

## G) Rule object model (atomic + relational + composite matching)

### G0) Mental model: a rule object is “AND over named fields”, unless you force order with `all`

A **Rule object** is an (unordered) dictionary of fields drawn from **Atomic**, **Relational**, and **Composite** keys. The node matches the rule object iff **all present fields match**. The reference groups the available keys exactly this way. ([Ast Grep][1])

**Two consequences that bite in real rules:**

1. **Field order is not guaranteed** when you write:

```yaml
rule:
  pattern: foo($A)
  inside: { pattern: bar($A) }
```

Because the rule object is unordered, `inside` may evaluate before `pattern`. If your later checks depend on metavariables captured earlier, you should use `all:` to impose an order. ([Ast Grep][2])

2. `all:` is not “all nodes”; it’s “this **one node** satisfies all rules.” Each rule tests **one node at a time**. If you accidentally write a rule that would require a node to be two kinds at once, it will never match. ([Ast Grep][2])

**Try-it-now harness (fast iteration):**
Use `scan --inline-rules` so you can paste rule configs while prototyping the *rule object* under `rule:`. ([Ast Grep][3])

```bash
ast-grep scan --inline-rules "$(cat <<'YAML'
id: demo.rule.object
language: JavaScript
rule:
  pattern: console.log($X)
severity: warning
message: demo
YAML
)" src/
```

---

## G1) Atomic rule keys (match “properties of this node”)

### G1.1 `pattern`: string pattern vs object pattern (`context` / `selector` / `strictness`)

`pattern` accepts either:

* a **string** (matches a single AST node using pattern syntax), or
* an **object** `{context, selector?, strictness?}`. ([Ast Grep][1])

**String pattern (simple):**

```yaml
rule:
  pattern: console.log($ARG)
```

([Ast Grep][1])

**Object-style pattern (disambiguate parse):**

* By default, `pattern` parses as a **standalone file**; `selector` “pulls out” the sub-node inside the provided `context` that becomes the match target. ([Ast Grep][1])

```yaml
rule:
  pattern:
    context: class { $F }
    selector: field_definition
```

([Ast Grep][1])

**`strictness` (pattern match algorithm tuning):**
Supported values: `cst`, `smart`, `ast`, `relaxed`, `signature`. ([Ast Grep][1])

```yaml
rule:
  pattern:
    context: foo($BAR)
    strictness: relaxed
```

([Ast Grep][1])

---

### G1.2 `kind`: node kind name (+ limited ESQuery-style selectors in 0.39+)

`kind` matches an AST node by its **kind name** (discoverable in the playground). ([Ast Grep][1])

```yaml
rule:
  kind: call_expression
```

([Ast Grep][1])

**ESQuery-style `kind` (0.39+):** you can write limited CSS-like selectors directly in `kind`, e.g.:

```yaml
rule:
  kind: call_expression > identifier
```

This is documented as supported in **0.39+** and is equivalent (under the hood) to relational-rule formulations (e.g., “identifier inside call_expression”). ([Ast Grep][1])

**Currently supported selectors (limited):**

* `>` direct child
* space descendant
* `+` next sibling
* `~` following sibling ([Ast Grep][4])

The blog post shows explicit equivalences, including cases where descendant/sibling forms imply `stopBy: end`. ([Ast Grep][4])

---

### G1.3 `regex`: Rust regex over full node text (no lookaround/backrefs)

`regex` applies a **Rust regular expression** to the node’s **text**. The docs note Rust regex syntax is Perl-like but **lacks** features such as lookaround and backreferences. ([Ast Grep][1])

```yaml
rule:
  kind: identifier
  regex: ^[a-z]+$
```

([Ast Grep][1])

**Operational note:** the reference emphasizes rules should include at least one **positive** key; `regex` alone is not “positive” (because it can match any kind if the text matches). ([Ast Grep][1])

---

### G1.4 `nthChild`: CSS `An+B` positional matching (1-based; named nodes only; `ofRule` filters)

`nthChild` selects nodes by their index among siblings in the parent’s children list. It supports:

* number: exact position
* string: `An+B` formula
* object: `{position, reverse?, ofRule?}` ([Ast Grep][1])

**Rules (important):**

* index is **1-based** (CSS-like)
* only considers **named** nodes (unnamed nodes are excluded) ([Ast Grep][1])

Examples:

```yaml
rule:
  kind: number
  nthChild: 3
```

```yaml
rule:
  nthChild: 2n+1
```

```yaml
rule:
  nthChild:
    position: 2n+1
    reverse: true
    ofRule:
      kind: function_declaration
```

([Ast Grep][1])

**When to use `ofRule`:**
If your “sibling list” includes multiple node types and you only want to count a subset, `ofRule` filters the sibling list *before* applying the positional test. ([Ast Grep][1])

---

### G1.5 `range`: 0-based (line, column), inclusive start, exclusive end

`range` matches nodes by their **source span**:

* `line` and `column` are **0-based**, character-based
* `start` inclusive, `end` exclusive ([Ast Grep][1])

```yaml
rule:
  range:
    start: { line: 0, column: 0 }
    end:   { line: 0, column: 3 }
```

([Ast Grep][1])

The atomic-rule guide explicitly calls out `range` as useful for integrating external tools that report ranges (e.g., compilers/type checkers) so ast-grep can target and rewrite those nodes. ([Ast Grep][5])

---

## G2) Relational rules (match “this node relative to another node”)

### G2.0 Target vs surrounding: read it as “target RELATION surrounding”

Relational rules are easiest to read as: **the main rule matches the target**, while the relational sub-rule matches the **surrounding** node (ancestor/descendant/sibling) that constrains the target. The relational guide provides this mnemonic explicitly. ([Ast Grep][6])

Example (`follows`):

```yaml
pattern: console.log('hello');
follows:
  pattern: console.log('world');
```

Only the `hello` *after* `world` matches. ([Ast Grep][6])

---

### G2.1 `stopBy`: control search depth (`neighbor` default, `end`, or a rule)

All relational rules accept a `stopBy` option:

* `'neighbor'` (default): only look one “step” further (e.g., direct child for `has`)
* `'end'`: search until “the end” (root/leaf/edge siblings)
* **rule object**: stop when that rule matches a surrounding node ([Ast Grep][6])

```yaml
has:
  stopBy: end
  pattern: $MY_PATTERN
```

([Ast Grep][6])

Custom stop condition example (stop at any function boundary while searching ancestors):

```yaml
inside:
  stopBy: { kind: function }
  pattern: function test($$$) { $$$ }
```

`stopBy` is **inclusive**: if the stop rule and the relational match hit the same node, it still counts as a match. ([Ast Grep][6])

---

### G2.2 `field`: scope the search to a specific child field (only for `inside` / `has`)

For `inside` and `has`, you can add `field:` to search only within a specific named field of the surrounding node. ([Ast Grep][1])

Example (ensure we match object **keys** named `prototype`, not values):

```yaml
kind: pair
has:
  field: key
  regex: 'prototype'
```

This matches `{ prototype: anotherObject }` but not `{ normalKey: prototype }`. ([Ast Grep][6])

---

### G2.3 The four relational operators

**`inside`**: target appears inside (ancestor chain) a node matching the sub-rule. ([Ast Grep][1])
**`has`**: target has a descendant matching the sub-rule. ([Ast Grep][1])
**`precedes` / `follows`**: target appears before/after another node matching the sub-rule; these do **not** support `field`. ([Ast Grep][1])

```yaml
precedes:
  kind: function_declaration
  stopBy: end
```

```yaml
follows:
  kind: decorator
  stopBy: end
```

([Ast Grep][1])

---

## G3) Composite rules (logic + reuse): `all`, `any`, `not`, `matches`

### G3.1 `all`: AND list + **ordering guarantee** (metavariable-safe)

`all` matches nodes satisfying **all** listed rules. Crucially, it **guarantees rule matching order**, which matters when metavariable capture in one sub-rule is used by later sub-rules. ([Ast Grep][2])

```yaml
rule:
  all:
    - pattern: foo($A)
    - inside: { pattern: bar($A), stopBy: end }
```

*(Use this shape whenever `$A` binding must be established first.)* ([Ast Grep][2])

### G3.2 `any`: OR list (canonical “alternatives” encoding)

`any` matches if **any one** sub-rule matches. ([Ast Grep][2])

```yaml
rule:
  any:
    - pattern: var a = $A
    - pattern: const a = $A
    - pattern: let a = $A
```

([Ast Grep][2])

### G3.3 `not`: negation (usually paired with `all` / field-AND)

`not` matches nodes that **do not** satisfy its sub-rule; commonly used to exclude a special-case match. ([Ast Grep][2])

```yaml
rule:
  pattern: console.log($GREETING)
  not:
    pattern: console.log('Hello World')
```

([Ast Grep][2])

### G3.4 `matches`: rule-id indirection to utility rules (reuse + recursion)

`matches` takes a **rule-id string** and reuses a utility rule (local or global) by reference. The docs call out that it enables reuse and even recursive possibilities and point to a dedicated page. ([Ast Grep][2])

```yaml
rule:
  matches: some.utility.rule.id
```

([Ast Grep][2])

---

### G3.5 Composite semantics footgun: `all`/`any` refer to rules, not “multiple nodes”

The composite guide is explicit: a rule tests **one node** at a time. So:

```yaml
has:
  all: [ {kind: number}, {kind: string} ]
```

will never work (it asks for one descendant node that is simultaneously number and string). The correct encoding is to move `all` up and use two separate `has` constraints: ([Ast Grep][2])

```yaml
all:
  - has: { kind: number }
  - has: { kind: string }
```

([Ast Grep][2])

---

### G4) “Rule object fields” vs explicit `all:` — when to pick which

Because rule objects implicitly behave like “AND over fields”, this is equivalent structurally:

```yaml
pattern: this.foo
inside: { kind: class_body }
```

and

```yaml
all:
  - pattern: this.foo
  - inside: { kind: class_body }
```

…but only `all:` guarantees **evaluation order** when metavariables are involved. ([Ast Grep][2])

---

If you want, the next logical deep-dive after this is **H) Rule YAML surface (id/language/message/severity/files/ignores/utils/constraints/fix/transform/rewriters)**, because that’s where these rule objects get “deployed” as lint rules and codemods.

[1]: https://ast-grep.github.io/reference/rule.html "Rule Object Reference | ast-grep"
[2]: https://ast-grep.github.io/guide/rule-config/composite-rule.html "Composite Rule | ast-grep"
[3]: https://ast-grep.github.io/guide/rule-config/relational-rule.html?utm_source=chatgpt.com "Relational Rules - ast-grep"
[4]: https://ast-grep.github.io/blog/new-ver-39.html "ast-grep 0.39 is Here | ast-grep"
[5]: https://ast-grep.github.io/guide/rule-config/atomic-rule.html "Atomic Rule | ast-grep"
[6]: https://ast-grep.github.io/guide/rule-config/relational-rule.html "Relational Rules | ast-grep"

## O) Debugging, observability, and troubleshooting

### O0) The triage loop (fastest path to “why is this not working?”)

When something “doesn’t match” or “doesn’t scan”, force the problem into one of these buckets:

1. **Pattern parsing**: did tree-sitter parse your pattern the way you think? → `run --debug-query=…` ([ast-grep.github.io][1])
2. **Match logic**: is your rule/pattern structurally correct (selector/strictness/metavars)? → `run --debug-query` + pattern object escalation ([ast-grep.github.io][1])
3. **Discovery**: did scan even include the files + rules you think it did? → `scan --inspect summary|entity` ([ast-grep.github.io][2])

Everything below is the “muscle memory” to resolve each bucket.

---

## O1) Debug pattern parsing/matching with `--debug-query` (formats + workflow)

### O1.1 The exact flag contract

`--debug-query[=<format>]` prints the **tree-sitter parse** of your query pattern and **requires `--lang`** be set explicitly. ([ast-grep.github.io][1])

Supported formats: `pattern`, `ast`, `cst`, `sexp`. ([ast-grep.github.io][1])

```bash
# minimal: see how the pattern parses
ast-grep run -l ts -p 'a?.b()' --debug-query

# pick format explicitly (recommended while debugging)
ast-grep run -l ts -p 'a?.b()' --debug-query=ast
ast-grep run -l ts -p 'a?.b()' --debug-query=cst
ast-grep run -l ts -p 'a?.b()' --debug-query=sexp
ast-grep run -l ts -p 'a?.b()' --debug-query=pattern
```

([ast-grep.github.io][1])

### O1.2 Which format to use (practical guidance)

* `ast`: named nodes only (good for “what kind am I matching?”) ([ast-grep.github.io][1])
* `cst`: named + unnamed nodes (critical when you suspect punctuation/operators/keywords matter) ([ast-grep.github.io][1])
* `sexp`: compact “shape” view (good for quick diffs between parses) ([ast-grep.github.io][1])
* `pattern`: ast-grep’s internal “Pattern format” (good when debugging metavars) ([ast-grep.github.io][1])

### O1.3 Debugging flow that reliably converges

1. **Lock language** + dump parse:

```bash
ast-grep run -l <lang> -p '<pattern>' --debug-query=cst
```

([ast-grep.github.io][1])

2. If the parse is surprising, you now have two high-leverage moves:

* **Add context** (pattern object in a rule) and use `selector` to pick the node you actually want. ([ast-grep.github.io][3])
* If your intent is “match a sub-node within a larger snippet”, try `run --selector <KIND>` (discussed earlier) — the run reference explicitly ties `--selector` to pattern-object semantics. ([ast-grep.github.io][1])

### O1.4 “Works in Playground but not in CLI” (common, explainable)

The FAQ calls out two common reasons for CLI vs Playground inconsistencies:

* **Parser version skew** (Playground parsers may lag behind CLI)
* **Encoding differences** (CLI uses UTF-8; Playground uses UTF-16; can affect error recovery) ([ast-grep.github.io][3])

The recommended fix path is:

* use `--debug-query` to inspect what CLI parsed, and compare to Playground
* if mismatch is due to incomplete/wrong snippet, escalate to a **pattern object** with `context` + `selector` ([ast-grep.github.io][3])

---

## O2) Explain file/rule inclusion/exclusion with `scan --inspect` (and capture it cleanly)

### O2.1 `--inspect` contract (scan + run)

Both `run` and `scan` support `--inspect <GRANULARITY>`:

* prints “how many and why files and rules are scanned and skipped”
* outputs to **stderr**
* does **not** affect search results ([ast-grep.github.io][1])

Granularity values: `nothing`, `summary`, `entity`. ([ast-grep.github.io][1])

```bash
# high level “why did you scan N files / skip M files?”
ast-grep scan --inspect summary

# per-file / per-rule tracing (the real debugger)
ast-grep scan --inspect entity
```

([ast-grep.github.io][2])

### O2.2 Capture inspect output without polluting stdout pipelines

Because inspect is stderr, you can safely keep JSON on stdout while logging discovery trace:

```bash
# stdout: results; stderr: trace
ast-grep scan --json=stream --inspect entity \
  2> inspect.log \
  | jq -c '.file'
```

(Inspect → stderr is explicitly documented.) ([ast-grep.github.io][2])

### O2.3 Isolate variables: “one pattern” vs “one rule”

For debugging, ast-grep recommends these two isolated modes: ([ast-grep.github.io][4])

```bash
# test one pattern in isolation
ast-grep run -p 'YOUR_PATTERN' --debug-query

# test one rule in isolation (no full ruleset noise)
ast-grep scan -r path/to/your/rule.yml
```

([ast-grep.github.io][4])

---

## O3) Common failure classes (symptoms → commands → durable fix)

### O3.1 Wrong language inference / missing language mapping

#### Symptom

* files you expected to scan are skipped, or parse behaves “like the wrong language”
* extensionless files (e.g. `.eslintrc`) don’t behave like you expect

#### Why it happens

ast-grep uses **file extensions** to determine language by default. The language reference table explicitly documents extension mapping and that extension-based inference is the default behavior. ([ast-grep.github.io][5])

#### Fast “one-off” fixes (no config changes)

**(A) Force language in stdin workflows** (especially for extensionless inputs):

* `run --stdin` requires you to specify `--lang` because language can’t be inferred without an extension. ([ast-grep.github.io][4])

```bash
cat .eslintrc | ast-grep run --stdin -l json -p '"rules": $R'
```

([ast-grep.github.io][4])

**(B) Verify correct language alias**
Language aliases are what you pass to `--lang` and what you use in YAML `language:`. ([ast-grep.github.io][5])

#### Durable fix: `languageGlobs` in `sgconfig.yml`

`languageGlobs` lets you associate a language with **glob patterns**, supports extensionless files, and can even override the default parser for an extension (it “takes precedence”). ([ast-grep.github.io][6])

```yaml
# sgconfig.yml
ruleDirs: [rules]

languageGlobs:
  json: ['.eslintrc', '.prettierrc']
  html: ['*.vue', '*.svelte', '*.astro']
  tsx:  ['*.ts']   # example: treat TS as TSX for rule reuse
```

([ast-grep.github.io][6])

#### Missing language entirely: `customLanguages`

If the language isn’t shipped with the CLI, you can register a tree-sitter dynamic library via `customLanguages` (with `libraryPath` and `extensions`, plus optional overrides). ([ast-grep.github.io][6])

---

### O3.2 “Pattern snippet lacks context” (Tree-sitter can’t parse your fragment)

#### Symptom

* `--debug-query` shows an unexpected node kind (or error recovery junk)
* pattern matches “nothing” even though the snippet looks right
* CLI vs Playground differences appear

#### Why it happens

The FAQ explicitly calls out “incomplete or wrong code snippet in the pattern” as a common cause, and recommends fixing by supplying a **complete context** via a **pattern object**. ([ast-grep.github.io][3])

#### The fix: escalate to a pattern object (`context` + `selector`)

Use a YAML rule with:

```yaml
rule:
  pattern:
    context: 'int main() { return 0; }'
    selector: function
```

This is the FAQ’s canonical “provide complete context code via pattern object” move. ([ast-grep.github.io][3])

#### Debug workflow (repeatable)

1. Show parse shape in CLI:

```bash
ast-grep run -l <lang> -p '<your-snippet>' --debug-query=cst
```

([ast-grep.github.io][1])

2. If it’s malformed/ambiguous → move the pattern into a rule and add `context` + `selector`. ([ast-grep.github.io][3])

---

### O3.3 LSP started outside a configured project root (diagnostics “don’t work”)

#### Symptom

* VSCode diagnostics/code actions don’t appear
* LSP starts but reports nothing

#### Ground truth requirements (from the editor guide)

To get LSP diagnostics working, the editor guide requires:

1. ast-grep CLI installed + accessible in VSCode
2. a linting project in the **workspace root**: `sgconfig.yml` is required for diagnostics
3. LSP server starts in the **workspace root** by default; `sgconfig.yml` must be in that root ([ast-grep.github.io][7])

#### Fix options (pick the one that matches your layout)

**(A) Make your VSCode workspace root the repo root** (where `sgconfig.yml` lives). ([ast-grep.github.io][7])

**(B) Start LSP in a subfolder by pointing at config**
The guide says you can set `configPath` in extension settings to point to the `sgconfig.yml` (relative to workspace root). ([ast-grep.github.io][7])

**(C) Start the server with an explicit config path**
The editor guide also documents CLI support:

```bash
ast-grep lsp -c <configPath>
```

([ast-grep.github.io][7])

**(D) If VSCode “can’t find the CLI”**
The guide notes VSCode’s extension environment may not inherit your terminal PATH; it suggests either installing globally or setting `astGrep.serverPath` in extension settings. ([ast-grep.github.io][7])

---

If you want to keep this chapter “ops-grade”, the next two debugging topics that usually pay off are: (1) stdin caveats + TTY behavior, and (2) a “known-good minimal project scaffold” (sgconfig + one rule + one test) you can drop into any repo to validate your local install.

[1]: https://ast-grep.github.io/reference/cli/run.html "ast-grep run | ast-grep"
[2]: https://ast-grep.github.io/reference/cli/scan.html "ast-grep scan | ast-grep"
[3]: https://ast-grep.github.io/advanced/faq.html "Frequently Asked Questions | ast-grep"
[4]: https://ast-grep.github.io/guide/tooling-overview.html "Command Line Tooling Overview | ast-grep"
[5]: https://ast-grep.github.io/reference/languages.html "List of Languages with Built-in Support | ast-grep"
[6]: https://ast-grep.github.io/reference/sgconfig.html "sgconfig.yml Reference | ast-grep"
[7]: https://ast-grep.github.io/guide/tools/editors.html "Editor Integration | ast-grep"
