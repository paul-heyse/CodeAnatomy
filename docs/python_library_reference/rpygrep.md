
Below is a **version-stamped, clustered feature catalog** for **`rpygrep` v0.2.1** (latest on PyPI as of Oct 3, 2025). ([PyPI][1])

`rpygrep` is a **strongly-typed wrapper around the `rg` CLI**, with two main modes: **file finding** and **content searching**, and both **sync + async** execution. ([PyPI][1])

---

## A) Scope, intent, and core value proposition

### What `rpygrep` is

* **Programmatic construction** of `rg` commands for:

  * **Finding files** (`RipGrepFind` → yields `pathlib.Path`)
  * **Searching content** (`RipGrepSearch` → yields typed `RipGrepSearchResult`) ([PyPI][1])
* **Typed results**: it “automatically parses ripgrep’s JSON output into rich Python data classes” (Pydantic-backed). ([PyPI][1])
* **Execution styles**:

  * Synchronous: `run()`
  * Asynchronous: `arun()` ([PyPI][1])

### What it is *not*

* Not a full native ripgrep reimplementation; it depends on `rg` being available. ([PyPI][1])
* Not an attempt to wrap the entire ripgrep manpage as first-class Python methods—**it covers a focused “agent/search-tool” subset**, plus an “escape hatch” for extra flags (see section H).

---

## B) Installation & runtime dependencies

* **Requires the ripgrep command-line tool** to be installed and runnable. ([PyPI][1])
* Python package install: `uv add rpygrep` (docs). ([PyPI][1])

---

## C) Public surface & module layout

### Top-level imports

* `from rpygrep import RipGrepFind, RipGrepSearch` ([PyPI][1])

### Internal but useful modules (not re-exported at top level)

* `rpygrep.types`: typed event/data models (begin/match/context/end/summary + stats + unions)
* `rpygrep.helpers`: post-processing helpers for attaching context lines to matches (MatchedFile/MatchedLine)

---

## D) Core builder abstraction: shared option surface

Both `RipGrepFind` and `RipGrepSearch` share a common “builder” pattern (method chaining) described in the project features. ([PyPI][1])

### Shared configuration fields (conceptual)

* **working directory** (where `rg` is executed)
* **command** (binary name/path; default `rg`)
* **targets** (paths you search; defaults to “current dir semantics” if empty)

### Shared option families (methods)

These apply to both Find and Search:

#### 1) Scope & traversal controls

* `max_depth(depth: int)` (limit recursion depth) ([PyPI][1])
* `one_file_system()` (don’t cross filesystem boundaries)

#### 2) Include/exclude by glob

* `include_glob(glob: str)`
* `exclude_glob(glob: str)`
* Batch helpers: `include_globs([...])`, `exclude_globs([...])` (used in the docs’ chained example) ([PyPI][1])

#### 3) Include/exclude by ripgrep “file type”

* `include_type(type_name)` / `exclude_type(type_name)`
* Batch helpers: `include_types([...])`, `exclude_types([...])` (used in the docs’ chained example) ([PyPI][1])

#### 4) Case sensitivity and sorting

* `case_sensitive(bool)` (case-insensitive when False) ([PyPI][1])
* `sort(by=..., ascending=True)` (sort keys include `path/name/size/accessed/created/modified/none`) ([PyPI][1])

#### 5) Target selection

* Add directories/files to the target list (directory/file adders exist; see Find/Search sections for typical usage patterns)

---

## E) File discovery mode: `RipGrepFind`

### Purpose

* “Finding files based on various criteria.” ([PyPI][1])
* Backed by `rg --files` under the hood.

### Key features (documented)

* Filtering: `include_glob`, `exclude_glob`, `include_type`, `exclude_type` ([PyPI][1])
* Depth limiting: `max_depth` ([PyPI][1])
* Sorting: `sort(...)` ([PyPI][1])
* Execution:

  * `run() -> Iterator[Path]` ([PyPI][1])
  * `arun() -> AsyncIterator[Path]` ([PyPI][1])

### Typical usage patterns

* “Find all Python files” via `include_types(["py"])`. ([PyPI][1])
* “Limit search depth” via `max_depth(1)`. ([PyPI][1])

---

## F) Content search mode: `RipGrepSearch`

### Purpose

* Search within files; yields structured per-file results (`RipGrepSearchResult`). ([PyPI][1])
* Internally uses `rg --json` (enabled automatically by `run/arun`). ([PyPI][1])

### Key features (documented)

#### 1) Pattern specification

* `add_pattern(pattern: str)` (call multiple times for multiple patterns) ([PyPI][1])
* Batch helper: `add_patterns([ ... ])` (shown in docs) ([PyPI][1])

#### 2) Context lines

* `before_context(n)` and `after_context(n)` ([PyPI][1])

#### 3) Result limiting / safety

* `max_count(n)` (max matches per file) ([PyPI][1])
* `max_file_size(bytes)` (skip files larger than limit) ([PyPI][1])
* `add_safe_defaults()` exists and is used in the docs’ recommended chain. ([PyPI][1])

#### 4) Output mode

* `as_json()` (configures JSON output; auto-called by `run/arun`) ([PyPI][1])

#### 5) Execution

* `run() -> Iterator[RipGrepSearchResult]` ([PyPI][1])
* `arun() -> AsyncIterator[RipGrepSearchResult]` ([PyPI][1])

### Important underlying constraints (from ripgrep JSON mode)

Because `rpygrep` relies on `rg --json`, it inherits the JSON-mode constraints:

* JSON lines emits message types `begin/end/match/context/summary` and uses `{text|bytes}` encoding to handle non-UTF8 data. ([Ripgrepy][2])
* `--json` can’t be combined with `--files`, `--count`, etc. ([Ripgrepy][2])

---

## G) Result model: what you get back from `RipGrepSearch`

### `RipGrepSearchResult` (per file)

The docs describe a per-file result object with:

* `path`
* `begin`
* `matches: list[RipGrepMatch]`
* `context: list[RipGrepContext]`
* `end` ([PyPI][1])

### Event/message model (JSON-lines shaped)

`rpygrep` models the same conceptual “event stream” as ripgrep’s JSON output:

* `begin`: file started
* `match`: a matching line + offsets/submatches
* `context`: context lines around matches
* `end`: file finished + stats
* `summary`: overall summary at end of run ([Ripgrepy][2])

(Internally, `rpygrep` groups events into a per-file `RipGrepSearchResult` using the `begin`↔`end` boundary.)

---

## H) Extensibility & “escape hatches”

Even though `rpygrep` provides typed helpers for common options, the project explicitly frames itself as “comprehensive options” over core needs rather than a perfect 1:1 wrapper of every `rg` flag. ([PyPI][1])

Practically, your extension points are:

* **Extra raw CLI flags** (pass-through) for unsupported ripgrep knobs (e.g., smart-case, multiline, pcre2, hidden, no-ignore, etc.)
* **Direct JSON-line consumption** (consume the raw `rg --json` stream yourself if you want global `summary` handling, progress, or custom grouping)

---

## I) Helper layer: “match + context → ready-to-render structure”

`rpygrep` includes helper dataclasses intended for consumer ergonomics (particularly “search tools” that need to render context around matches):

* **MatchedLine**: `before`, `match`, `after` maps keyed by line number
* **MatchedFile**: `path` + list of `MatchedLine`
* Constructors to build from one or many `RipGrepSearchResult` objects, with logic to avoid stealing context lines across adjacent matches (useful when `before_context/after_context` windows overlap)

---

## J) Type system & safety features

### `RIPGREP_TYPE_LIST`

* A very large `Literal[...]` of ripgrep “file types” (languages + formats + compressed types), used to strongly-type `include_type(s)` / `exclude_type(s)`. ([PyPI][1])

### Pydantic-driven validation

* Results are validated/parsed with Pydantic models (“Type Safety” + “Structured Output”). ([PyPI][1])

---

## K) Notable gaps / sharp edges (useful for tool design)

These matter when you build an agent-facing search tool on top:

1. **JSON-mode semantics**

   * Many “pretty output” flags don’t apply in `--json` mode (ripgrep itself documents this). ([Ripgrepy][2])

2. **Global summary handling**

   * ripgrep emits a final `summary` message in JSON mode. ([Ripgrepy][2])
   * `rpygrep`’s main `run/arun` contract is **per-file results**; if you want the global summary, you’ll typically need to tap the raw stream.

3. **Docs vs package drift**

   * The PyPI/README examples reference `DEFAULT_EXCLUDED_TYPES` as a top-level import. ([PyPI][1])
   * If you rely on that, sanity-check it in your environment; if missing, you’ll want to define your own default exclude set (often: images, archives, parquet, sqlite/db, etc.) and apply via `exclude_types(...)`.

---

## Quick “feature → why it matters for agents” index

* **Find vs Search split** → lets you implement “file candidate” step vs “content match” step cleanly. ([PyPI][1])
* **Async iteration (`arun`)** → integrates with FastAPI/MCP tool calls without blocking. ([PyPI][1])
* **Typed JSON results** → makes it safe to post-process matches (snippets, offsets, context windows) without brittle stdout parsing. ([PyPI][1])
* **Include/exclude types + globs + depth** → your primary “don’t melt the repo” safety rails. ([PyPI][1])

---

If you want the **next increment** (like your PyArrow doc), I can expand this catalog into **deep-dive pages** per cluster (Find/Search/Result Models/Helper post-processing), including **power knobs**, **failure modes**, and **agent-grade “safe default profiles”** (e.g., “interactive query,” “batch audit,” “precision regex,” “literal quick scan”) anchored to the exact `rpygrep` calls.

[1]: https://pypi.org/project/rpygrep/0.2.1/ "rpygrep · PyPI"
[2]: https://ripgrepy.readthedocs.io/?utm_source=chatgpt.com "Welcome to ripgrepy's documentation! — ripgrepy ..."


Below are **deep-dive pages per cluster** for **`rpygrep` v0.2.1** (wrapper over `rg`), in the same “power knobs + failure modes + minimal patterns” style as your PyArrow doc. `rpygrep` gives you two primary builders (`RipGrepFind`, `RipGrepSearch`) plus typed result models (Pydantic-backed) and a small helper layer for “render-ready” match+context structures. ([PyPI][1])

---

## A) `RipGrepFind` (file discovery) — deep dive (rpygrep v0.2.1)

### Mental model: “enumerate candidate files” with *ripgrep’s traversal + filters*

`RipGrepFind` is effectively a programmatic `rg --files …` with:

* **Scope**: working directory (and/or explicit targets)
* **Filters**: globs (`--glob=…`), types (`--type=…` / `--type-not=…`), max depth (`--max-depth=…`), one filesystem (`--one-file-system`)
* **Ordering (optional)**: `--sort/--sortr` (but this disables parallelism in `rg`) ([Debian Manpages][2])

---

# A1) Full surface area (what you can *actually* dial)

## A1.1 Construction & execution

**Constructor pattern (docs):**

```python
from pathlib import Path
from rpygrep import RipGrepFind

rg_find = RipGrepFind(working_directory=Path("."))
for p in rg_find.run():
    print(p)

# async
async for p in rg_find.arun():
    print(p)
```

([PyPI][1])

**Execution primitives**

* `run() -> Iterator[Path]`
* `arun() -> AsyncIterator[Path]` ([PyPI][1])

## A1.2 File filtering primitives (the “candidate set” control plane)

**Type filters**

* `include_type("py")` / `include_types([...])` → adds `--type=…`
* `exclude_type("json")` / `exclude_types([...])` → adds `--type-not=…` ([PyPI][1])

**Glob filters**

* `include_glob("*.py")` / `include_globs([...])` → adds `--glob=…`
* `exclude_glob("node_modules/**")` / `exclude_globs([...])` → adds `--glob=!…` ([PyPI][1])

**Traversal**

* `max_depth(n)` → `--max-depth=n` ([PyPI][1])
* `one_file_system()` → `--one-file-system` (avoid cross-mount traversal)

**Sorting**

* `sort(by=..., ascending=True)` → `--sort=…` / `--sortr=…` ([PyPI][1])
  Sorting forces `rg` into **single-threaded mode** and can error if the sort key isn’t supported (e.g., `created` on ext4). ([Debian Manpages][3])

## A1.3 “Safety defaults”

`rpygrep` also documents a `DEFAULT_EXCLUDED_TYPES` list you can apply:

```python
from rpygrep import RipGrepFind, DEFAULT_EXCLUDED_TYPES
rg_find = RipGrepFind(working_directory=Path(".")).exclude_types(DEFAULT_EXCLUDED_TYPES)
```

([PyPI][1])

---

# A2) Minimal implementation snippets (canonical patterns)

## A2.1 Candidate list: “source files only”

```python
from pathlib import Path
from rpygrep import RipGrepFind, DEFAULT_EXCLUDED_TYPES

rg = (
    RipGrepFind(working_directory=Path("."))
    .exclude_types(DEFAULT_EXCLUDED_TYPES)
    .include_types(["py", "rs", "ts", "js", "go"])
    .exclude_globs(["**/node_modules/**", "**/.git/**", "**/dist/**", "**/build/**"])
    .max_depth(20)
)

paths = list(rg.run())
```

(Types are ripgrep “type-list” names; `rpygrep` exposes them as a big `Literal[...]` for type safety. ([PyPI][1]))

## A2.2 Deterministic file order (for CI reproducibility)

```python
rg = RipGrepFind(working_directory=Path(".")).sort(by="path", ascending=True)
```

Tradeoff: sorting disables parallelism and can slow large walks. ([Debian Manpages][2])

---

# A3) Power knobs (when you’re building an agent tool)

## A3.1 Fast vs stable ordering

* **Fastest**: don’t sort (default). `rg` can stay parallel. ([Debian Manpages][3])
* **Stable**: sort by `path` (or `modified`) if you need deterministic snapshots, accepting slower single-thread behavior. ([Debian Manpages][3])

## A3.2 “Hidden/vendor explosion” prevention

Ripgrep defaults are usually what you want for agent search: it respects `.gitignore`, skips hidden and binary content by default. ([Debian Manpages][2])
Your *agent layer* should still add explicit excludes for the usual repo sinks (`node_modules`, `.venv`, `dist`, `target`, `.git`), because ignore rules vary repo-to-repo.

## A3.3 Config-file interference

If `RIPGREP_CONFIG_PATH` is set, ripgrep will prepend args from the rc file to every invocation; you can disable this with `--no-config`. ([Debian Manpages][3])
If you want your agent tool to be deterministic across environments, consider always passing `--no-config` (see `RipGrepSearch.add_extra_options` in section B; Find doesn’t expose it in the PyPI summary, but you can still add equivalent behavior at the tool boundary by ensuring a clean environment).

---

# A4) Failure modes & diagnostics

## A4.1 “Why did it search stdin?”

Ripgrep can auto-detect readable stdin and search it; to avoid that, specify an explicit directory/path. ([Debian Manpages][2])
**Agent best practice**: always provide a concrete repo root (as working_directory or a target path) rather than relying on defaults.

## A4.2 Sorting errors on some filesystems

If you sort by `created` but the platform/filesystem doesn’t support it, `rg` will error and exit. ([Debian Manpages][3])
Mitigation: default to `path` or `modified` for deterministic runs.

---

## B) `RipGrepSearch` (content search) — deep dive (rpygrep v0.2.1)

### Mental model: build `rg --json` searches, then stream+group JSON events into per-file results

In JSON mode, ripgrep emits **JSON Lines** “messages” with types: `begin`, `match`, `context`, `end`, and final `summary`. ([Debian Manpages][3])
`rpygrep` sets `--json` (via `as_json()`, called by `run/arun`) and groups events into `RipGrepSearchResult` objects per file. ([PyPI][1])

---

# B1) Full surface area (option families)

## B1.1 Pattern entry

* `add_pattern(pat)` → adds `--regexp=pat` (call repeatedly)
* `add_patterns([...])` convenience ([PyPI][1])

### Literal vs regex semantics

* `patterns_are_not_regex()` → adds `--fixed-strings` (treat patterns as literals)
  In ripgrep, `--fixed-strings` forces literal matching (regex metacharacters lose meaning). ([Lean Crew][4])
* `auto_hybrid_regex()` → adds `--auto-hybrid-regex`
  This tells ripgrep to choose between its default engine and PCRE2 automatically based on pattern features (look-around/backrefs need PCRE2). ([GitHub][5])

## B1.2 Output/context shaping (but remember JSON constraints)

* `before_context(n)` / `after_context(n)` → `--before-context/--after-context`
* `max_count(n)` → `--max-count=n`
* `max_file_size(bytes)` → `--max-filesize=…`
* `case_sensitive(bool)` → if False, adds `--ignore-case` ([PyPI][1])

**Important:** In `--json` mode, many “human formatting” flags do nothing (e.g., `--heading`, `--only-matching`, `--replace`, etc.), and JSON can’t be combined with `--files`, `-l`, counts, etc. ([Debian Manpages][3])

## B1.3 Scope filters (same as Find)

* `include_type(s)`, `exclude_type(s)`
* `include_glob(s)`, `exclude_glob(s)`
* `max_depth(n)`, `one_file_system()`
* `sort(...)` (but sorting disables parallelism) ([PyPI][1])

## B1.4 Execution modes (critical for agent integration)

* `run()` / `arun()` → yields **typed per-file** `RipGrepSearchResult`
* `run_direct()` / `arun_direct()` → yields **raw JSON lines** (`str`) as they arrive
  This is your escape hatch when you want:

  * the final `summary` event (global stats),
  * custom grouping,
  * custom streaming UI.

## B1.5 Escape hatch for unsupported rg flags

* `add_extra_options([...])` → pass raw ripgrep flags (e.g., `--smart-case`, `--hidden`, `-j1`, `--no-mmap`, `--no-config`, `--line-buffered`, etc.).
  These are particularly important because JSON mode restricts which “output mode” flags you can combine. ([Debian Manpages][3])

---

# B2) Minimal implementation snippets (high-signal patterns)

## B2.1 “Interactive query” (fast, bounded, streaming)

```python
from pathlib import Path
from rpygrep import RipGrepSearch, DEFAULT_EXCLUDED_TYPES

def interactive_search(root: Path, pattern: str) -> RipGrepSearch:
    return (
        RipGrepSearch(working_directory=root)
        .add_safe_defaults()                # rpygrep defaults (depth cap + size/count caps)
        .exclude_types(DEFAULT_EXCLUDED_TYPES)
        .exclude_globs([
            "**/.git/**", "**/node_modules/**", "**/.venv/**", "**/dist/**", "**/build/**"
        ])
        .patterns_are_not_regex()           # literal by default (agent UX)
        .case_sensitive(False)              # typical “search box” behavior
        .before_context(1).after_context(1)
        .max_count(50)                      # tighter than defaults for responsiveness
        .max_file_size(2 * 1024 * 1024)     # 2 MiB cap for interactive
        .add_extra_options(["--line-buffered", "--no-config"])
    )

# async iteration
# async for result in interactive_search(Path("."), "DatasetContract").arun():
#     ...
```

Why `--line-buffered`: without a tty, ripgrep may use block buffering; `--line-buffered` flushes after each match. ([Debian Manpages][3])

## B2.2 “Precision regex” (when you need look-around/backrefs)

```python
rg = (
    RipGrepSearch(working_directory=Path("."))
    .add_pattern(r"(?s)foo.*bar")     # or any regex
    .auto_hybrid_regex()             # uses PCRE2 only if needed & available
    .case_sensitive(True)
    .max_count(200)
    .max_file_size(10 * 1024 * 1024)
    .add_extra_options(["--no-config"])
)
```

`--auto-hybrid-regex` makes engine choice automatic, using PCRE2 when the default engine can’t compile the pattern (if PCRE2 is available). ([GitHub][5])

## B2.3 “Batch audit” (reproducible, slower but stable ordering)

```python
rg = (
    RipGrepSearch(working_directory=Path("."))
    .add_pattern(r"eval\(")
    .exclude_types(DEFAULT_EXCLUDED_TYPES)
    .sort(by="path", ascending=True)       # stable ordering for snapshots
    .max_count(10_000)                     # audit mode: allow many hits
    .max_file_size(50 * 1024 * 1024)
    .add_extra_options(["--no-config"])
)
```

Sorting disables parallelism; use it only when determinism matters more than speed. ([Debian Manpages][3])

## B2.4 Getting global stats (the `summary` event)

`rpygrep`’s `run/arun` yields per-file results; if you need overall totals, consume `run_direct/arun_direct` and parse the final `{"type":"summary", ...}` row. The JSON message types and presence of `summary` are defined by ripgrep. ([Debian Manpages][3])

---

# B3) Power knobs (the ones that matter in production)

## B3.1 Output volume controls (prevent “agent melts the repo”)

* **`max_count`**: caps matches per file (your #1 lever).
* **`max_file_size`**: prevents large-file scans (your #2 lever).
* **`max_depth` + globs**: keep traversal bounded.

## B3.2 Memory / buffering / long-line hazards

Ripgrep’s manpage explicitly notes memory can spike with:

* parallel search buffering output per file (to avoid interleaving),
* very long lines (esp. binary/text modes),
* mmap behavior; use `--no-mmap` to avoid certain truncation-related aborts. ([Debian Manpages][3])

**Agent-grade mitigation** (when you see RAM spikes):

* pass `-j1` / `--threads=1` via `add_extra_options` to reduce buffering,
* add `--no-mmap` if you hit the truncation caveat,
* keep `max_file_size` low for interactive usage. ([Debian Manpages][3])

## B3.3 Ignore/hidden/binary policy

Ripgrep default filtering (gitignore + skip hidden + skip binary) is usually ideal for codebases. ([Debian Manpages][2])
If you must broaden scope:

* `--hidden`, `--binary`, `--no-ignore`, `-u/-uu/-uuu` (progressively reduce filtering). ([Debian Manpages][3])

## B3.4 Config-file determinism

Config files via `RIPGREP_CONFIG_PATH` can change behavior; `--no-config` disables config loading. ([Debian Manpages][3])

---

# B4) Failure modes & debugging checklist

## B4.1 “I added an option and now JSON parsing fails”

In `--json` mode, ripgrep forbids combining JSON with `--files`, `-l`, counts, etc., and many output formatting flags are ignored. ([Debian Manpages][3])
If you pass incompatible flags via `add_extra_options`, ripgrep will error. Solution: keep JSON mode “pure search” and use `RipGrepFind` for file listing.

## B4.2 “No output / missing results”

* Check ignore rules precedence (gitignore vs .ignore vs .rgignore) and hidden/binary policy. ([Debian Manpages][3])
* Check if config file is mutating defaults; temporarily add `--no-config`. ([Debian Manpages][3])
* Ensure you’re searching the intended directory (stdin detection can surprise you if no path is supplied and stdin is readable). ([Debian Manpages][2])

---

## C) Result models (`RipGrepSearchResult`, events, offsets) — deep dive

### Mental model: ripgrep emits an event stream; `rpygrep` groups it into “per-file bundles”

In JSON mode, ripgrep emits:

* `begin`: file started (and will contain at least one match)
* `match`: match line with offsets/submatches
* `context`: context line
* `end`: file finished + per-file stats
* `summary`: final global stats across the whole search ([Debian Manpages][3])

---

# C1) `RipGrepSearchResult` shape (what you can rely on)

From the `rpygrep` docs:

* `path: Path`
* `begin`
* `matches: list[...]`
* `context: list[...]`
* `end` ([PyPI][1])

**Important semantic detail:** `summary` exists in the underlying stream, but the primary `run/arun` interface is designed around **per-file** results. If you need cross-file totals, you must parse the raw stream (`run_direct/arun_direct`) or run your own aggregator. ([Debian Manpages][3])

---

# C2) Match payloads: text/bytes + submatches + offsets

## C2.1 `lines.text` vs `lines.bytes`

Ripgrep JSON must be valid Unicode, so file paths and line contents are emitted as either:

* `{ "text": "..." }` when valid UTF-8
* or `{ "bytes": "<base64>" }` when not valid UTF-8 ([Debian Manpages][3])

So in `rpygrep`, `match.data.lines.text` can be `None` and you may need `match.data.lines.bytes` (base64). ([Debian Manpages][3])

## C2.2 Offsets (`absolute_offset`, `start`, `end`)

The JSON match example shows:

* `line_number`
* `absolute_offset`
* `submatches: [{match.text, start, end}, ...]` ([Rust CLI][6])

Offsets can be tricky with encodings/BOMs: there’s a known issue where `absolute_offset` in JSON output didn’t account for BOM bytes in some cases. ([GitHub][7])
**Agent-grade guidance**:

* Treat offsets as **byte-oriented positions** unless you normalize to decoded text yourself.
* If you need “character columns,” compute them from the decoded line you present (and be explicit about the encoding policy).

---

# C3) Stats payloads (per-file and global)

In `--json` mode, `--stats` is implicitly enabled. ([Debian Manpages][3])
You’ll see per-file stats under each `end` message and global totals in the final `summary`. ([Rust CLI][6])

---

## D) Helper post-processing (`rpygrep.helpers`) — deep dive

### Mental model: turn “matches + context events” into render-ready snippets per match

For agent-facing output, you typically want:

* **per match**: `(before_lines, match_line, after_lines)` keyed by line number
* **per file**: a list of such match snippets

`rpygrep.helpers` provides:

* `MatchedLine`: `{before: {lineno: text}, match: {lineno: text}, after: {lineno: text}}`
* `MatchedFile`: `{path, matched_lines}` + constructors that attach context without “stealing” context lines from neighboring matches.

---

# D1) Minimal usage

```python
from rpygrep.helpers import MatchedFile

files = []
for result in rg.run():
    files.append(
        MatchedFile.from_search_result(
            result,
            before_context=1,
            after_context=1,
            include_empty_lines=False,
        )
    )
```

# D2) Why this helper matters

Context in `rg --json` arrives as separate `context` events. If you want clean UX (“show me the 2 lines around each match”), you either:

* implement your own windowing logic (and handle overlap),
* or use helpers that already:

  * group by line number,
  * avoid overlapping matches stealing each other’s context.

---

## E) Agent-grade “safe default profiles” (drop-in builders)

These profiles are **designed for AI agents**: bounded scope, bounded output, deterministic-enough behavior, and ergonomic match rendering.

### E0) Common defaults (recommended in *all* profiles)

* Always set `working_directory` to repo root (avoid stdin surprises). ([Debian Manpages][2])
* Add `--no-config` unless you explicitly want user rc behavior. ([Debian Manpages][3])
* Exclude repo sinks via globs (node_modules/.git/dist/build/etc.).
* Prefer JSON mode (what `rpygrep` does) for structured parsing; don’t mix with other output modes. ([Debian Manpages][3])

---

# E1) Profile: **Interactive query** (fast, low-latency, low-output)

**Goal:** “search box” feel; returns quickly; doesn’t scan huge files; doesn’t produce massive outputs.

```python
from pathlib import Path
from rpygrep import RipGrepSearch, DEFAULT_EXCLUDED_TYPES

def rg_profile_interactive(root: Path, pattern: str) -> RipGrepSearch:
    return (
        RipGrepSearch(working_directory=root)
        .exclude_types(DEFAULT_EXCLUDED_TYPES)
        .exclude_globs(["**/.git/**", "**/node_modules/**", "**/.venv/**", "**/dist/**", "**/build/**"])
        .patterns_are_not_regex()           # literal by default
        .case_sensitive(False)
        .before_context(1).after_context(1)
        .max_depth(20)
        .max_file_size(2 * 1024 * 1024)     # 2 MiB
        .max_count(50)                      # 50 matches/file
        .add_pattern(pattern)
        .add_extra_options(["--line-buffered", "--no-config"])
    )
```

Why `--line-buffered`: without a tty, ripgrep may block-buffer; line-buffered flushes immediately. ([Debian Manpages][3])

---

# E2) Profile: **Literal quick scan** (fastest “needle find”)

**Goal:** treat query as a literal string (no regex escaping surprises), scan broadly but keep caps.

```python
def rg_profile_literal_scan(root: Path, needle: str) -> RipGrepSearch:
    return (
        RipGrepSearch(working_directory=root)
        .patterns_are_not_regex()           # --fixed-strings
        .case_sensitive(False)
        .max_depth(30)
        .max_file_size(5 * 1024 * 1024)
        .max_count(200)
        .add_pattern(needle)
        .add_extra_options(["--no-config"])
    )
```

`--fixed-strings` forces literal matching (metacharacters don’t need escaping). ([Lean Crew][4])

---

# E3) Profile: **Precision regex** (power + correctness, bounded enough)

**Goal:** allow advanced regex features when needed; avoid “why didn’t lookbehind work?” surprises.

```python
def rg_profile_precision_regex(root: Path, regex: str) -> RipGrepSearch:
    return (
        RipGrepSearch(working_directory=root)
        .auto_hybrid_regex()                # chooses PCRE2 only if needed
        .case_sensitive(True)
        .max_depth(30)
        .max_file_size(10 * 1024 * 1024)
        .max_count(1000)
        .add_pattern(regex)
        .add_extra_options(["--no-config"])
    )
```

`--auto-hybrid-regex` switches engines automatically when patterns require PCRE2 features. ([GitHub][5])

---

# E4) Profile: **Batch audit** (thorough, reproducible, CI-friendly)

**Goal:** “scan the whole repo for a class of issues,” stable ordering, richer totals.

```python
def rg_profile_batch_audit(root: Path, regex: str) -> RipGrepSearch:
    return (
        RipGrepSearch(working_directory=root)
        .auto_hybrid_regex()
        .max_depth(100)
        .max_file_size(50 * 1024 * 1024)
        .max_count(50_000)
        .sort(by="path", ascending=True)    # stable output ordering
        .add_pattern(regex)
        .add_extra_options(["--no-config"])
    )
```

Sorting disables parallelism (slower) but gives deterministic snapshots. ([Debian Manpages][3])

If you hit memory spikes in audit mode, consider `-j1` (single-thread) and/or `--no-mmap` per ripgrep caveats. ([Debian Manpages][3])

---

If you want the **next increment after these deep dives** (closest to your CodeIntel workflow), I can turn these profiles into a **repo-ready “search contract pack”**:

* pytest fixtures that run each profile against a tiny corpus,
* golden JSON snapshots normalized for determinism,
* and a “result summarizer” that emits a single CI artifact with: match counts, file counts, bytes searched, and drift diffs across ripgrep upgrades.

[1]: https://pypi.org/project/rpygrep/0.2.1/ "rpygrep · PyPI"
[2]: https://manpages.debian.org/testing/ripgrep/rg.1.en.html?utm_source=chatgpt.com "rg(1) — ripgrep — Debian testing"
[3]: https://manpages.debian.org/testing/ripgrep/rg.1.en.html "rg(1) — ripgrep — Debian testing — Debian Manpages"
[4]: https://leancrew.com/all-this/man/man1/rg.html?utm_source=chatgpt.com "rg(1) man page"
[5]: https://github.com/BurntSushi/ripgrep/pull/1155?utm_source=chatgpt.com "add flag to enable \"hybrid\" regex mode · Issue #1155"
[6]: https://rust-cli.github.io/book/in-depth/machine-communication.html "Communicating with machines - Command Line Applications in Rust"
[7]: https://github.com/BurntSushi/ripgrep/issues/1627?utm_source=chatgpt.com "Offset in `--json` output does not take into account BOM bytes"


Below is a **repo-ready “rpygrep search contract pack”** you can drop into your project. It gives you:

* **pytest fixtures** that run multiple search profiles against a **tiny deterministic corpus**
* **golden JSON snapshots** (normalized for determinism) with an **UPDATE_GOLDENS=1** workflow
* a **single CI artifact** (`report.json`) capturing: **candidate file counts**, **bytes scanned (by enumerating candidates)**, **matched file counts**, **match counts**, plus **drift diffs** (current vs golden) and `rg`/`rpygrep` version stamps

This pack is anchored strictly to the documented `rpygrep` surface: `RipGrepFind`/`RipGrepSearch` + `include_*`/`exclude_*`/`max_depth`/`before_context`/`after_context`/`max_count`/`max_file_size`/`run`/`arun`. ([PyPI][1])

---

# 1) Drop-in layout

```text
tests/
  contracts/
    rpygrep_search_contract/
      __init__.py
      README.md
      corpus.py
      profiles.py
      runner.py
      normalize.py
      report.py
      test_contract_rpygrep_search.py
      goldens/
        .gitkeep
  artifacts/
    rpygrep_search_contract/
      (generated at test time; add to .gitignore)
```

**Git hygiene**

* Commit: `tests/contracts/rpygrep_search_contract/**` (including `goldens/*.json`)
* Ignore: `tests/artifacts/rpygrep_search_contract/**`

---

# 2) Contract corpus (tiny, deterministic)

Create `tests/contracts/rpygrep_search_contract/corpus.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


# Bump this whenever you change corpus contents/paths.
CORPUS_LAYOUT_VERSION = 1


@dataclass(frozen=True)
class CorpusSpec:
    """Deterministic corpus used by the rpygrep contract pack."""
    layout_version: int = CORPUS_LAYOUT_VERSION

    def materialize(self, root: Path) -> Path:
        """
        Create a tiny repo-like tree under `root`.

        We intentionally include:
        - multiple languages (py/txt/sh)
        - a JSON file (often excluded by DEFAULT_EXCLUDED_TYPES)
        - a large file to exercise max_file_size caps
        - a vendor-ish directory to exercise glob excludes
        """
        files: dict[str, str] = {
            "code_with_hello_world.py": (
                "def main():\n"
                "    print('hello, world!')\n"
                "    # TODO: DatasetContract integration\n"
                "    value = 'Hello, World!'\n"
                "    return value\n"
            ),
            "test_with_Hello_World.txt": (
                "Hello, World!\n"
                "This is a test file.\n"
                "hello appears here too.\n"
            ),
            "subdir/script_with_hello.sh": (
                "#!/usr/bin/env bash\n"
                "echo 'Hello'\n"
                "echo 'hello'\n"
            ),
            "should_be_ignored.env": (
                "HELLO=World\n"
                "SHOULD_BE_IGNORED=1\n"
            ),
            # Many teams exclude json by default as a “data” type.
            "data.json": '{ "hello": "world", "note": "json file" }\n',
            "subdir/nested.txt": (
                "Nested file\n"
                "hello hello hello\n"
            ),
            # Something you definitely want excluded by glob:
            "node_modules/ignored.js": (
                "console.log('hello from node_modules');\n"
            ),
        }

        for rel, content in files.items():
            p = root / rel
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(content, encoding="utf-8", newline="\n")

        # Large file to test max_file_size behavior.
        big = root / "big.txt"
        big.parent.mkdir(parents=True, exist_ok=True)
        big.write_text(("hello\n" * 15_000), encoding="utf-8", newline="\n")  # ~90KB

        return root
```

---

# 3) Profile specs (agent-grade “safe defaults”)

Create `tests/contracts/rpygrep_search_contract/profiles.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence
import re

from rpygrep import DEFAULT_EXCLUDED_TYPES, RipGrepFind, RipGrepSearch


@dataclass(frozen=True)
class SearchProfile:
    """
    Declarative profile that we can:
      (a) apply to RipGrepFind to enumerate candidate files deterministically
      (b) apply to RipGrepSearch to run the actual content search
    """
    name: str
    patterns: tuple[str, ...]
    # rpygrep does not expose --fixed-strings directly; we implement "literal"
    # by escaping patterns with re.escape.
    literal: bool = False

    case_sensitive: bool = True
    before_context: int = 0
    after_context: int = 0

    max_depth: int | None = None
    max_count: int | None = None
    max_file_size: int | None = None  # bytes

    include_types: tuple[str, ...] = ()
    exclude_types: tuple[str, ...] = tuple(DEFAULT_EXCLUDED_TYPES)

    include_globs: tuple[str, ...] = ()
    exclude_globs: tuple[str, ...] = (
        "**/.git/**",
        "**/node_modules/**",
        "**/.venv/**",
        "**/dist/**",
        "**/build/**",
    )


def _apply_common_filters_find(rg: RipGrepFind, profile: SearchProfile) -> RipGrepFind:
    for t in profile.include_types:
        rg.include_type(t)
    for t in profile.exclude_types:
        rg.exclude_type(t)
    for g in profile.include_globs:
        rg.include_glob(g)
    for g in profile.exclude_globs:
        rg.exclude_glob(g)
    if profile.max_depth is not None:
        rg.max_depth(profile.max_depth)
    return rg


def _apply_common_filters_search(rg: RipGrepSearch, profile: SearchProfile) -> RipGrepSearch:
    for t in profile.include_types:
        rg.include_type(t)
    for t in profile.exclude_types:
        rg.exclude_type(t)
    for g in profile.include_globs:
        rg.include_glob(g)
    for g in profile.exclude_globs:
        rg.exclude_glob(g)
    if profile.max_depth is not None:
        rg.max_depth(profile.max_depth)
    return rg


def build_find(root: Path, profile: SearchProfile) -> RipGrepFind:
    rg = RipGrepFind(working_directory=root)
    return _apply_common_filters_find(rg, profile)


def build_search(root: Path, profile: SearchProfile) -> RipGrepSearch:
    rg = RipGrepSearch(working_directory=root)
    _apply_common_filters_search(rg, profile)

    # Patterns first (so failures are “compile pattern” vs “run”).
    for pat in profile.patterns:
        rg.add_pattern(re.escape(pat) if profile.literal else pat)

    rg.case_sensitive(profile.case_sensitive)

    if profile.before_context:
        rg.before_context(profile.before_context)
    if profile.after_context:
        rg.after_context(profile.after_context)

    if profile.max_count is not None:
        rg.max_count(profile.max_count)
    if profile.max_file_size is not None:
        rg.max_file_size(profile.max_file_size)

    # JSON is the default output mode for rpygrep search (run/arun call as_json under the hood). :contentReference[oaicite:1]{index=1}
    return rg


# ---- Contract profiles ----
# Keep these stable: this pack is intended to detect drift (rg upgrades, rpygrep upgrades, filtering changes).
PROFILES: tuple[SearchProfile, ...] = (
    SearchProfile(
        name="interactive_query",
        patterns=("hello",),
        literal=False,
        case_sensitive=False,
        before_context=1,
        after_context=1,
        max_depth=10,
        max_count=50,
        # Tight cap ensures big.txt is skipped for responsiveness.
        max_file_size=8 * 1024,
    ),
    SearchProfile(
        name="literal_quick_scan",
        patterns=("hello, world!",),
        literal=True,               # implemented via re.escape
        case_sensitive=False,
        max_depth=10,
        max_count=50,
        max_file_size=2 * 1024 * 1024,
        include_types=("py",),      # docs show "py" as a canonical type example :contentReference[oaicite:2]{index=2}
    ),
    SearchProfile(
        name="precision_regex",
        patterns=(r"Hello,\s+World!",),
        literal=False,
        case_sensitive=True,
        before_context=1,
        after_context=1,
        max_depth=10,
        max_count=50,
        max_file_size=2 * 1024 * 1024,
    ),
    SearchProfile(
        name="batch_audit",
        patterns=(r"SHOULD_BE_IGNORED", r"TODO"),
        literal=False,
        case_sensitive=True,
        max_depth=50,
        max_count=50_000,
        max_file_size=50 * 1024 * 1024,
        # Sorting is intentionally NOT done here: we normalize ordering ourselves for determinism.
    ),
)
```

Notes:

* `rpygrep`’s key surface (find + search + typed results, sync/async) is documented on the PyPI page. ([PyPI][1])
* `DEFAULT_EXCLUDED_TYPES` is explicitly documented and used in examples. ([PyPI][1])

---

# 4) Normalization rules (goldens that don’t flap)

Create `tests/contracts/rpygrep_search_contract/normalize.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any


SCHEMA_ID = "rpygrep_search_contract/v1"


def _rel_posix(path: Path, root: Path) -> str:
    try:
        rel = path.relative_to(root)
    except Exception:
        rel = Path(Path(path).as_posix())  # fallback; best-effort
    return rel.as_posix()


def _clean_line(text: str) -> str:
    # Normalize line-endings + strip trailing newline (ripgrep’s JSON lines often include '\n').
    return text.replace("\r\n", "\n").rstrip("\n")


def _maybe_text_lines(lines_obj: Any) -> str | None:
    # Docs show match.data.lines.text can be None (bytes mode); handle that. :contentReference[oaicite:5]{index=5}
    txt = getattr(lines_obj, "text", None)
    if isinstance(txt, str):
        return _clean_line(txt)
    return None


def normalize_results(*, profile_name: str, corpus_root: Path, results: list[Any], meta: dict[str, Any]) -> dict[str, Any]:
    """
    Turn rpygrep’s typed objects into a stable JSON shape suitable for goldens.

    We deliberately keep only the high-signal, low-flake fields:
      - relative path
      - match line_number + line text (or "<BINARY>")
      - context line_number + line text
      - submatch spans (start/end) if present
    """
    files: list[dict[str, Any]] = []
    total_matches = 0
    total_context = 0

    for r in results:
        path = getattr(r, "path", None)
        path_s = _rel_posix(path, corpus_root) if isinstance(path, Path) else str(path)

        matches_out: list[dict[str, Any]] = []
        for m in getattr(r, "matches", []) or []:
            data = getattr(m, "data", None)
            ln = getattr(data, "line_number", None)
            lines = getattr(data, "lines", None)
            line_txt = _maybe_text_lines(lines) if lines is not None else None

            # Submatches are optional; we keep only stable span info.
            sub_out: list[dict[str, Any]] = []
            for sm in getattr(data, "submatches", []) or []:
                sub_out.append(
                    {
                        "start": getattr(sm, "start", None),
                        "end": getattr(sm, "end", None),
                    }
                )

            matches_out.append(
                {
                    "line_number": ln,
                    "line": line_txt if line_txt is not None else "<BINARY>",
                    "submatches": sub_out,
                }
            )

        context_out: list[dict[str, Any]] = []
        for c in getattr(r, "context", []) or []:
            data = getattr(c, "data", None)
            ln = getattr(data, "line_number", None)
            lines = getattr(data, "lines", None)
            line_txt = _maybe_text_lines(lines) if lines is not None else None
            context_out.append(
                {
                    "line_number": ln,
                    "line": line_txt if line_txt is not None else "<BINARY>",
                }
            )

        # Deterministic ordering:
        matches_out.sort(key=lambda x: (x["line_number"] or 0, (x["submatches"][0]["start"] if x["submatches"] else -1)))
        context_out.sort(key=lambda x: (x["line_number"] or 0))

        total_matches += len(matches_out)
        total_context += len(context_out)

        files.append(
            {
                "path": path_s,
                "matches": matches_out,
                "context": context_out,
            }
        )

    files.sort(key=lambda f: f["path"])

    payload: dict[str, Any] = {
        "schema": SCHEMA_ID,
        "profile": profile_name,
        "meta": meta,  # includes versions + candidate metrics (provided by runner)
        "matched": {
            "files": len(files),
            "matches": total_matches,
            "context_lines": total_context,
        },
        "results": files,
    }
    return payload


def stable_json_hash(payload: dict[str, Any]) -> str:
    import json
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
    return sha256(blob).hexdigest()
```

---

# 5) Runner (execute profiles, compute candidate bytes, stamp versions)

Create `tests/contracts/rpygrep_search_contract/runner.py`:

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import importlib.metadata
import shutil
import subprocess

from .profiles import SearchProfile, build_find, build_search


@dataclass(frozen=True)
class RunMetrics:
    candidate_files: int
    candidate_bytes: int
    matched_files: int
    match_count: int
    context_count: int


@dataclass(frozen=True)
class RunOutput:
    profile: str
    rg_version: str
    rpygrep_version: str
    candidate_relpaths: list[str]
    results: list[Any]
    metrics: RunMetrics


def _require_rg() -> str:
    rg = shutil.which("rg")
    if not rg:
        raise RuntimeError("ripgrep binary 'rg' not found on PATH (rpygrep requires rg).")
    return rg


def _rg_version(rg_bin: str) -> str:
    # Keep only first line for stable stamping.
    p = subprocess.run([rg_bin, "--version"], check=True, capture_output=True, text=True)
    return (p.stdout.splitlines()[0] if p.stdout else "rg <unknown>").strip()


def _rpygrep_version() -> str:
    try:
        return importlib.metadata.version("rpygrep")
    except Exception:
        return "rpygrep <unknown>"


def _to_relposix(p: Path, root: Path) -> str:
    try:
        return p.relative_to(root).as_posix()
    except Exception:
        return p.as_posix()


def run_profile(profile: SearchProfile, corpus_root: Path) -> RunOutput:
    rg_bin = _require_rg()
    rg_ver = _rg_version(rg_bin)
    rpy_ver = _rpygrep_version()

    # 1) Candidate enumeration (what *would* be searched under this profile).
    rg_find = build_find(corpus_root, profile)
    candidates: list[Path] = []
    for p in rg_find.run():
        abs_p = p if p.is_absolute() else (corpus_root / p)
        candidates.append(abs_p)

    # Deterministic candidate ordering is part of the contract surface.
    candidates.sort(key=lambda x: _to_relposix(x, corpus_root))
    candidate_rel = [_to_relposix(p, corpus_root) for p in candidates]

    candidate_bytes = 0
    for p in candidates:
        try:
            candidate_bytes += p.stat().st_size
        except FileNotFoundError:
            # Should not happen; corpus is deterministic.
            pass

    # 2) Actual search run.
    rg_search = build_search(corpus_root, profile)
    results = list(rg_search.run())  # yields RipGrepSearchResult objects :contentReference[oaicite:6]{index=6}

    matched_files = len(results)
    match_count = sum(len(getattr(r, "matches", []) or []) for r in results)
    context_count = sum(len(getattr(r, "context", []) or []) for r in results)

    metrics = RunMetrics(
        candidate_files=len(candidates),
        candidate_bytes=candidate_bytes,
        matched_files=matched_files,
        match_count=match_count,
        context_count=context_count,
    )

    return RunOutput(
        profile=profile.name,
        rg_version=rg_ver,
        rpygrep_version=rpy_ver,
        candidate_relpaths=candidate_rel,
        results=results,
        metrics=metrics,
    )
```

Key anchoring points:

* `rpygrep` is explicitly a wrapper over the `rg` CLI and requires `rg`. ([PyPI][1])
* `RipGrepFind.run()` yields `Path`; `RipGrepSearch.run()` yields `RipGrepSearchResult`. ([PyPI][1])

---

# 6) Report + diff writer (single CI artifact + drift diffs)

Create `tests/contracts/rpygrep_search_contract/report.py`:

```python
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import difflib
import json
import os

from .normalize import stable_json_hash


@dataclass
class ContractPaths:
    repo_root: Path
    contract_dir: Path
    goldens_dir: Path
    artifacts_dir: Path
    runs_dir: Path
    diffs_dir: Path
    report_path: Path

    @classmethod
    def from_repo_root(cls, repo_root: Path) -> "ContractPaths":
        contract_dir = repo_root / "tests" / "contracts" / "rpygrep_search_contract"
        goldens_dir = contract_dir / "goldens"

        artifacts_dir = repo_root / "tests" / "artifacts" / "rpygrep_search_contract"
        runs_dir = artifacts_dir / "runs"
        diffs_dir = artifacts_dir / "diffs"
        report_path = artifacts_dir / "report.json"

        runs_dir.mkdir(parents=True, exist_ok=True)
        diffs_dir.mkdir(parents=True, exist_ok=True)
        goldens_dir.mkdir(parents=True, exist_ok=True)

        return cls(
            repo_root=repo_root,
            contract_dir=contract_dir,
            goldens_dir=goldens_dir,
            artifacts_dir=artifacts_dir,
            runs_dir=runs_dir,
            diffs_dir=diffs_dir,
            report_path=report_path,
        )


def should_update_goldens() -> bool:
    return os.environ.get("UPDATE_GOLDENS") == "1" or os.environ.get("UPDATE_RPYGREP_GOLDENS") == "1"


def dump_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_diff(*, golden: dict[str, Any], current: dict[str, Any], diff_path: Path) -> None:
    golden_s = json.dumps(golden, ensure_ascii=False, sort_keys=True, indent=2).splitlines(keepends=True)
    current_s = json.dumps(current, ensure_ascii=False, sort_keys=True, indent=2).splitlines(keepends=True)
    diff = difflib.unified_diff(
        golden_s,
        current_s,
        fromfile="golden",
        tofile="current",
    )
    diff_path.parent.mkdir(parents=True, exist_ok=True)
    diff_path.write_text("".join(diff), encoding="utf-8")


@dataclass
class ProfileReport:
    profile: str
    status: str  # pass|fail|missing_golden|updated_golden|skipped
    rg_version: str
    rpygrep_version: str
    golden_hash: str | None
    current_hash: str | None
    diff_file: str | None
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass
class ContractReport:
    schema: str = "rpygrep_search_contract_report/v1"
    profiles: list[ProfileReport] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "profiles": [vars(p) for p in self.profiles],
        }
```

---

# 7) pytest harness (golden compare + CI artifact emission)

Create `tests/contracts/rpygrep_search_contract/test_contract_rpygrep_search.py`:

```python
from __future__ import annotations

from pathlib import Path
import pytest

from .corpus import CorpusSpec
from .profiles import PROFILES
from .runner import run_profile
from .normalize import normalize_results, stable_json_hash
from .report import ContractPaths, ContractReport, ProfileReport, dump_json, load_json, should_update_goldens, write_diff


@pytest.fixture(scope="session")
def repo_root(pytestconfig) -> Path:
    return Path(pytestconfig.rootpath)


@pytest.fixture(scope="session")
def contract_paths(repo_root: Path) -> ContractPaths:
    return ContractPaths.from_repo_root(repo_root)


@pytest.fixture(scope="session")
def corpus_root(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("rpygrep_contract_corpus")
    CorpusSpec().materialize(root)
    return root


@pytest.fixture(scope="session")
def contract_report(contract_paths: ContractPaths):
    report = ContractReport()
    yield report
    dump_json(contract_paths.report_path, report.to_json())


@pytest.mark.parametrize("profile", PROFILES, ids=lambda p: p.name)
def test_rpygrep_profile_contract(profile, corpus_root: Path, contract_paths: ContractPaths, contract_report: ContractReport):
    # 1) Run profile.
    try:
        out = run_profile(profile, corpus_root)
    except RuntimeError as e:
        # e.g., rg not found on PATH.
        contract_report.profiles.append(
            ProfileReport(
                profile=profile.name,
                status="skipped",
                rg_version="<missing>",
                rpygrep_version="<unknown>",
                golden_hash=None,
                current_hash=None,
                diff_file=None,
                metrics={"reason": str(e)},
            )
        )
        pytest.skip(str(e))

    meta = {
        "rg_version": out.rg_version,
        "rpygrep_version": out.rpygrep_version,
        "candidate": {
            "files": out.metrics.candidate_files,
            "bytes": out.metrics.candidate_bytes,
            "relpaths": out.candidate_relpaths,
        },
    }

    current = normalize_results(
        profile_name=profile.name,
        corpus_root=corpus_root,
        results=out.results,
        meta=meta,
    )
    current_hash = stable_json_hash(current)

    # Always write current run payload for CI inspection.
    run_path = contract_paths.runs_dir / f"{profile.name}.json"
    dump_json(run_path, current)

    golden_path = contract_paths.goldens_dir / f"{profile.name}.json"

    # 2) First-time bootstrap: missing golden.
    if not golden_path.exists():
        if should_update_goldens():
            dump_json(golden_path, current)
            contract_report.profiles.append(
                ProfileReport(
                    profile=profile.name,
                    status="updated_golden",
                    rg_version=out.rg_version,
                    rpygrep_version=out.rpygrep_version,
                    golden_hash=current_hash,
                    current_hash=current_hash,
                    diff_file=None,
                    metrics=vars(out.metrics),
                )
            )
            return
        else:
            contract_report.profiles.append(
                ProfileReport(
                    profile=profile.name,
                    status="missing_golden",
                    rg_version=out.rg_version,
                    rpygrep_version=out.rpygrep_version,
                    golden_hash=None,
                    current_hash=current_hash,
                    diff_file=None,
                    metrics={**vars(out.metrics), "run_payload": str(run_path)},
                )
            )
            pytest.fail(f"Missing golden: {golden_path}. Run with UPDATE_GOLDENS=1 to create it.")

    golden = load_json(golden_path)
    golden_hash = stable_json_hash(golden)

    # 3) Compare.
    if golden != current:
        diff_path = contract_paths.diffs_dir / f"{profile.name}.diff"
        write_diff(golden=golden, current=current, diff_path=diff_path)

        contract_report.profiles.append(
            ProfileReport(
                profile=profile.name,
                status="fail",
                rg_version=out.rg_version,
                rpygrep_version=out.rpygrep_version,
                golden_hash=golden_hash,
                current_hash=current_hash,
                diff_file=str(diff_path),
                metrics={**vars(out.metrics), "run_payload": str(run_path)},
            )
        )
        pytest.fail(f"Contract drift for profile={profile.name}. See diff: {diff_path}")

    contract_report.profiles.append(
        ProfileReport(
            profile=profile.name,
            status="pass",
            rg_version=out.rg_version,
            rpygrep_version=out.rpygrep_version,
            golden_hash=golden_hash,
            current_hash=current_hash,
            diff_file=None,
            metrics={**vars(out.metrics), "run_payload": str(run_path)},
        )
    )
```

This produces:

* `tests/artifacts/rpygrep_search_contract/runs/<profile>.json`
* `tests/artifacts/rpygrep_search_contract/diffs/<profile>.diff` (only on drift)
* `tests/artifacts/rpygrep_search_contract/report.json` (always)

---

# 8) README (how to run, how to update, what drift means)

Create `tests/contracts/rpygrep_search_contract/README.md`:

```md
# rpygrep search contract pack

This suite locks down the behavior of our "agent search profiles" built on top of rpygrep (and rg).

## What it does
- Materializes a tiny deterministic corpus
- Runs multiple profiles (interactive_query, literal_quick_scan, precision_regex, batch_audit)
- Normalizes results into a stable JSON shape
- Compares to committed goldens
- Emits CI artifacts:
  - tests/artifacts/rpygrep_search_contract/report.json
  - tests/artifacts/rpygrep_search_contract/runs/*.json
  - tests/artifacts/rpygrep_search_contract/diffs/*.diff (on drift)

## Prereqs
- `rg` must be installed (rpygrep shells out to ripgrep).

## First-time bootstrap
Generate goldens:
  UPDATE_GOLDENS=1 pytest -q tests/contracts/rpygrep_search_contract/test_contract_rpygrep_search.py

Commit:
  tests/contracts/rpygrep_search_contract/goldens/*.json

## Normal run (CI)
  pytest -q tests/contracts/rpygrep_search_contract/test_contract_rpygrep_search.py

## Interpreting drift
If a profile fails, inspect:
- tests/artifacts/rpygrep_search_contract/diffs/<profile>.diff
- tests/artifacts/rpygrep_search_contract/runs/<profile>.json
- tests/artifacts/rpygrep_search_contract/report.json

Common causes:
- `rg` upgraded (behavior/ignore semantics/output changes)
- `rpygrep` upgraded (parsing/model changes)
- profile specs changed (filters/caps/patterns)
- corpus layout changed (corpus.py layout_version bump)
```

---

# 9) What you get in CI (the single artifact)

`tests/artifacts/rpygrep_search_contract/report.json` looks like:

```json
{
  "schema": "rpygrep_search_contract_report/v1",
  "profiles": [
    {
      "profile": "interactive_query",
      "status": "pass",
      "rg_version": "ripgrep 14.1.0 ...",
      "rpygrep_version": "0.2.1",
      "golden_hash": "...",
      "current_hash": "...",
      "diff_file": null,
      "metrics": {
        "candidate_files": 5,
        "candidate_bytes": 12345,
        "matched_files": 3,
        "match_count": 12,
        "context_count": 6,
        "run_payload": "tests/artifacts/rpygrep_search_contract/runs/interactive_query.json"
      }
    }
  ]
}
```

* **Drift diffs across ripgrep upgrades** show up as `status="fail"` with a `diff_file`, and the version stamps inside each run’s `meta.rg_version` / `meta.rpygrep_version` make the upgrade correlation obvious. (rpygrep documents the versioned surface and that it depends on `rg`.) ([PyPI][1])

---

If you want one more increment after this pack (most useful in a CodeIntel-style agent tool), I can add:

* a “profile linter” that rejects dangerous profiles (no caps, no excludes),
* a small “search tool response adapter” that converts normalized results into your preferred agent payload schema (file → snippets → spans), and
* a regression benchmark harness (timings + output-size budget) alongside the contract.

[1]: https://pypi.org/project/rpygrep/0.2.1/ "rpygrep · PyPI"


Below is the **“one more increment”** as **drop-in additions** to the contract pack you already have:

1. **Profile linter** (rejects dangerous profiles: missing caps / missing excludes)
2. **Search tool response adapter** (normalized → agent payload: file → snippets → spans)
3. **Regression benchmark harness** (timings + output-size budgets, with baseline + CI artifact)

These are aligned with `rpygrep`’s documented surface (Find/Search builders, `run/arun`, typed results) and still assume `rg` is installed (a stated prerequisite). ([PyPI][1])

---

# 1) Profile linter (reject unsafe profiles)

### Add: `tests/contracts/rpygrep_search_contract/linter.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Sequence

from .profiles import SearchProfile


@dataclass(frozen=True)
class LintIssue:
    severity: str  # "error" | "warn"
    code: str
    profile: str
    message: str


class ProfileLintError(RuntimeError):
    def __init__(self, issues: Sequence[LintIssue]) -> None:
        msg = "rpygrep profile lint failed:\n" + "\n".join(
            f"- [{i.severity.upper()}] {i.profile} {i.code}: {i.message}" for i in issues
        )
        super().__init__(msg)
        self.issues = list(issues)


# ---- Policy knobs (tune to your org) ----
MAX_COUNT_HARD_LIMIT = 1_000_000
MAX_FILE_SIZE_HARD_LIMIT = 512 * 1024 * 1024  # 512 MiB
MAX_DEPTH_HARD_LIMIT = 500

RECOMMENDED_EXCLUDE_GLOBS = (
    "**/.git/**",
    "**/node_modules/**",
    "**/.venv/**",
    "**/dist/**",
    "**/build/**",
)


def lint_profile(p: SearchProfile) -> list[LintIssue]:
    issues: list[LintIssue] = []

    # ---- identity / structure ----
    if not p.name or any(ch.isspace() for ch in p.name):
        issues.append(LintIssue("error", "BAD_NAME", p.name or "<empty>", "Profile name must be non-empty and no spaces."))

    if not p.patterns:
        issues.append(LintIssue("error", "NO_PATTERNS", p.name, "At least one pattern is required."))

    # ---- safety rails: excludes ----
    has_any_excludes = bool(p.exclude_globs) or bool(p.exclude_types)
    if not has_any_excludes:
        issues.append(LintIssue("error", "NO_EXCLUDES", p.name, "Profile must exclude something (globs and/or types)."))

    # Recommend standard repo sink excludes (warn-only).
    missing = [g for g in RECOMMENDED_EXCLUDE_GLOBS if g not in set(p.exclude_globs)]
    if missing:
        issues.append(
            LintIssue(
                "warn",
                "MISSING_RECOMMENDED_EXCLUDES",
                p.name,
                f"Consider adding repo sink excludes: {missing}",
            )
        )

    # ---- safety rails: caps ----
    if p.max_depth is None:
        issues.append(LintIssue("error", "MISSING_MAX_DEPTH", p.name, "max_depth must be set (prevents repo explosions)."))
    elif p.max_depth <= 0:
        issues.append(LintIssue("error", "BAD_MAX_DEPTH", p.name, "max_depth must be > 0."))
    elif p.max_depth > MAX_DEPTH_HARD_LIMIT:
        issues.append(LintIssue("warn", "HIGH_MAX_DEPTH", p.name, f"max_depth={p.max_depth} is unusually high."))

    if p.max_count is None:
        issues.append(LintIssue("error", "MISSING_MAX_COUNT", p.name, "max_count must be set (caps output volume)."))
    elif p.max_count <= 0:
        issues.append(LintIssue("error", "BAD_MAX_COUNT", p.name, "max_count must be > 0."))
    elif p.max_count > MAX_COUNT_HARD_LIMIT:
        issues.append(LintIssue("error", "MAX_COUNT_TOO_HIGH", p.name, f"max_count={p.max_count} exceeds hard limit."))

    if p.max_file_size is None:
        issues.append(LintIssue("error", "MISSING_MAX_FILE_SIZE", p.name, "max_file_size must be set (caps scan cost)."))
    elif p.max_file_size <= 0:
        issues.append(LintIssue("error", "BAD_MAX_FILE_SIZE", p.name, "max_file_size must be > 0 bytes."))
    elif p.max_file_size > MAX_FILE_SIZE_HARD_LIMIT:
        issues.append(
            LintIssue("error", "MAX_FILE_SIZE_TOO_HIGH", p.name, f"max_file_size={p.max_file_size} exceeds hard limit.")
        )

    return issues


def lint_profiles(profiles: Iterable[SearchProfile]) -> list[LintIssue]:
    issues: list[LintIssue] = []
    seen: set[str] = set()
    for p in profiles:
        if p.name in seen:
            issues.append(LintIssue("error", "DUPLICATE_PROFILE_NAME", p.name, "Profile names must be unique."))
        seen.add(p.name)
        issues.extend(lint_profile(p))
    return issues


def assert_profiles_safe(profiles: Iterable[SearchProfile]) -> None:
    issues = lint_profiles(profiles)
    errors = [i for i in issues if i.severity == "error"]
    if errors:
        raise ProfileLintError(errors)
```

### Add: `tests/contracts/rpygrep_search_contract/test_profile_linter.py`

```python
from __future__ import annotations

from .linter import lint_profiles
from .profiles import PROFILES


def test_profiles_are_safe() -> None:
    issues = lint_profiles(PROFILES)
    errors = [i for i in issues if i.severity == "error"]
    assert not errors, "Unsafe rpygrep profiles:\n" + "\n".join(
        f"- {i.profile} {i.code}: {i.message}" for i in errors
    )
```

---

# 2) Response adapter (normalized → agent payload: file → snippets → spans)

### Add: `tests/contracts/rpygrep_search_contract/adapter.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable


AGENT_SCHEMA = "codeintel.search/v1"


@dataclass(frozen=True)
class AdapterOptions:
    snippet_before: int = 2
    snippet_after: int = 2
    max_snippets_per_file: int = 50
    max_files: int | None = None  # cap files in payload (agent safety)
    include_candidate_paths: bool = False


def _clamp_span(start: int | None, end: int | None, line: str) -> tuple[int, int] | None:
    if start is None or end is None:
        return None
    # rg JSON submatch offsets are byte offsets; we treat them as best-effort indices on decoded text.
    s = max(0, min(int(start), len(line)))
    e = max(0, min(int(end), len(line)))
    if e <= s:
        return None
    return (s, e)


def _group_spans_by_line(matches: list[dict[str, Any]]) -> dict[int, list[tuple[int, int]]]:
    out: dict[int, list[tuple[int, int]]] = {}
    for m in matches:
        ln = m.get("line_number")
        if ln is None:
            continue
        ln_i = int(ln)
        line = m.get("line") or ""
        if not isinstance(line, str):
            line = ""
        spans: list[tuple[int, int]] = []
        for sm in m.get("submatches") or []:
            span = _clamp_span(sm.get("start"), sm.get("end"), line)
            if span:
                spans.append(span)
        spans.sort()
        # Merge overlaps
        merged: list[tuple[int, int]] = []
        for s, e in spans:
            if not merged or s > merged[-1][1]:
                merged.append((s, e))
            else:
                merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        out[ln_i] = merged
    return out


def _lines_index(file_entry: dict[str, Any]) -> dict[int, str]:
    idx: dict[int, str] = {}
    for c in file_entry.get("context") or []:
        ln = c.get("line_number")
        line = c.get("line")
        if ln is not None and isinstance(line, str):
            idx[int(ln)] = line
    for m in file_entry.get("matches") or []:
        ln = m.get("line_number")
        line = m.get("line")
        if ln is not None and isinstance(line, str):
            idx[int(ln)] = line
    return idx


def adapt_normalized_to_agent_payload(normalized: dict[str, Any], *, options: AdapterOptions = AdapterOptions()) -> dict[str, Any]:
    """
    Convert normalize_results(...) output into a compact agent payload:
      file -> snippets -> spans

    Input shape is the JSON emitted by the contract pack runs (*.json in tests/artifacts/.../runs).
    """
    profile = normalized.get("profile", "<unknown>")
    meta = normalized.get("meta", {}) or {}
    results = normalized.get("results", []) or []

    out_files: list[dict[str, Any]] = []
    total_snippets = 0

    for file_entry in results:
        path = file_entry.get("path", "<unknown>")
        matches = file_entry.get("matches") or []
        if not matches:
            continue

        spans_by_line = _group_spans_by_line(matches)
        line_idx = _lines_index(file_entry)

        # One snippet per matching line_number (merged spans).
        line_numbers = sorted(spans_by_line.keys())
        snippets: list[dict[str, Any]] = []
        for ln in line_numbers[: options.max_snippets_per_file]:
            start_ln = ln - options.snippet_before
            end_ln = ln + options.snippet_after

            # Only include lines we actually have (context may be missing for edges / caps).
            lines: list[dict[str, Any]] = []
            for cur_ln in range(start_ln, end_ln + 1):
                if cur_ln in line_idx:
                    lines.append({"line_number": cur_ln, "text": line_idx[cur_ln]})

            if not lines:
                # Fallback: include just the match line if present
                if ln in line_idx:
                    lines = [{"line_number": ln, "text": line_idx[ln]}]

            snippet = {
                "start_line": lines[0]["line_number"] if lines else ln,
                "end_line": lines[-1]["line_number"] if lines else ln,
                "lines": lines,
                "matches": [
                    {
                        "line_number": ln,
                        "spans": [{"start": s, "end": e} for (s, e) in spans_by_line.get(ln, [])],
                    }
                ],
            }
            snippets.append(snippet)

        out_files.append(
            {
                "path": path,
                "match_lines": len(line_numbers),
                "snippets": snippets,
            }
        )

        total_snippets += sum(len(f["snippets"]) for f in out_files)
        if options.max_files is not None and len(out_files) >= options.max_files:
            break

    payload: dict[str, Any] = {
        "schema": AGENT_SCHEMA,
        "profile": profile,
        "meta": meta if options.include_candidate_paths else _strip_candidate_relpaths(meta),
        "totals": {
            "files": len(out_files),
            "snippets": sum(len(f["snippets"]) for f in out_files),
        },
        "results": out_files,
    }
    return payload


def _strip_candidate_relpaths(meta: dict[str, Any]) -> dict[str, Any]:
    # Keep counts/bytes; drop the potentially-large relpaths list unless explicitly requested.
    cand = (meta.get("candidate") or {}) if isinstance(meta.get("candidate"), dict) else {}
    if not cand:
        return meta
    pruned = dict(meta)
    pruned["candidate"] = {k: v for k, v in cand.items() if k != "relpaths"}
    return pruned
```

### Add: `tests/contracts/rpygrep_search_contract/test_adapter_smoke.py`

```python
from __future__ import annotations

from .adapter import adapt_normalized_to_agent_payload, AdapterOptions


def test_adapter_smoke() -> None:
    normalized = {
        "schema": "rpygrep_search_contract/v1",
        "profile": "interactive_query",
        "meta": {"candidate": {"files": 3, "bytes": 123, "relpaths": ["a.py", "b.txt", "c.md"]}},
        "results": [
            {
                "path": "a.py",
                "matches": [
                    {"line_number": 10, "line": "print('hello')", "submatches": [{"start": 7, "end": 12}]},
                ],
                "context": [
                    {"line_number": 9, "line": "def f():"},
                    {"line_number": 11, "line": "return 1"},
                ],
            }
        ],
    }

    payload = adapt_normalized_to_agent_payload(
        normalized,
        options=AdapterOptions(snippet_before=1, snippet_after=1, include_candidate_paths=False),
    )
    assert payload["schema"] == "codeintel.search/v1"
    assert payload["totals"]["files"] == 1
    assert payload["results"][0]["snippets"][0]["lines"][0]["line_number"] == 9
    assert payload["results"][0]["snippets"][0]["matches"][0]["spans"] == [{"start": 7, "end": 12}]
    # relpaths pruned by default:
    assert "relpaths" not in payload["meta"]["candidate"]
```

---

# 3) Regression benchmark harness (timings + output-size budgets)

This is **opt-in** via env var (so CI can enable it on stable runners, and dev machines don’t get flaky failures).

## 3.1 Add baseline + runner

### Add: `tests/contracts/rpygrep_search_contract/bench.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json
import os
import platform
import statistics
import time

from .adapter import adapt_normalized_to_agent_payload, AdapterOptions
from .normalize import normalize_results
from .profiles import PROFILES, SearchProfile
from .runner import run_profile
from .report import ContractPaths, dump_json, load_json


@dataclass(frozen=True)
class BenchPolicy:
    allowed_regression_factor: float = 1.50  # median_ms must be <= baseline * factor
    allowed_output_growth_factor: float = 1.10  # normalized bytes must be <= baseline * factor


@dataclass(frozen=True)
class BenchConfig:
    warmup_runs: int = 1
    timed_runs: int = 5
    policy: BenchPolicy = BenchPolicy()
    # Hard budgets (fail even if baseline updated).
    max_normalized_bytes: dict[str, int] | None = None


def bench_enabled() -> bool:
    return os.environ.get("RUN_BENCHMARKS") == "1" or os.environ.get("RUN_RPYGREP_BENCH") == "1"


def update_baseline() -> bool:
    return os.environ.get("UPDATE_BENCH_BASELINE") == "1"


def strict_bench() -> bool:
    # If unset, we still fail on regressions when bench is enabled;
    # but you can loosen locally by setting STRICT_BENCH=0.
    v = os.environ.get("STRICT_BENCH")
    if v is None:
        return True
    return v not in ("0", "false", "False")


def env_fingerprint(meta: dict[str, Any]) -> dict[str, Any]:
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "rg_version": meta.get("rg_version"),
        "rpygrep_version": meta.get("rpygrep_version"),
    }


def _json_size(obj: Any) -> int:
    return len(json.dumps(obj, ensure_ascii=False, sort_keys=True).encode("utf-8"))


def run_bench_for_profile(profile: SearchProfile, corpus_root: Path, *, config: BenchConfig) -> dict[str, Any]:
    # Warmups (don’t measure)
    for _ in range(config.warmup_runs):
        _ = run_profile(profile, corpus_root)

    times_ms: list[float] = []
    normalized_bytes: list[int] = []
    adapted_bytes: list[int] = []

    last_meta: dict[str, Any] | None = None
    last_metrics: dict[str, Any] | None = None

    for _ in range(config.timed_runs):
        t0 = time.perf_counter()
        out = run_profile(profile, corpus_root)
        t1 = time.perf_counter()

        meta = {
            "rg_version": out.rg_version,
            "rpygrep_version": out.rpygrep_version,
            "candidate": {
                "files": out.metrics.candidate_files,
                "bytes": out.metrics.candidate_bytes,
            },
        }
        normalized = normalize_results(profile_name=profile.name, corpus_root=corpus_root, results=out.results, meta=meta)
        agent_payload = adapt_normalized_to_agent_payload(normalized, options=AdapterOptions())

        times_ms.append((t1 - t0) * 1000.0)
        normalized_bytes.append(_json_size(normalized))
        adapted_bytes.append(_json_size(agent_payload))

        last_meta = meta
        last_metrics = {
            "candidate_files": out.metrics.candidate_files,
            "candidate_bytes": out.metrics.candidate_bytes,
            "matched_files": out.metrics.matched_files,
            "match_count": out.metrics.match_count,
            "context_count": out.metrics.context_count,
        }

    median_ms = statistics.median(times_ms)
    p90_ms = statistics.quantiles(times_ms, n=10)[8] if len(times_ms) >= 10 else max(times_ms)

    report = {
        "profile": profile.name,
        "timings_ms": {
            "median": median_ms,
            "p90": p90_ms,
            "runs": times_ms,
        },
        "sizes_bytes": {
            "normalized_median": statistics.median(normalized_bytes),
            "adapted_median": statistics.median(adapted_bytes),
            "normalized_runs": normalized_bytes,
            "adapted_runs": adapted_bytes,
        },
        "meta": last_meta or {},
        "metrics": last_metrics or {},
        "env": env_fingerprint(last_meta or {}),
    }
    return report


def baseline_path(repo_root: Path) -> Path:
    return repo_root / "tests" / "contracts" / "rpygrep_search_contract" / "bench" / "baseline.json"


def run_benchmarks(repo_root: Path, corpus_root: Path, paths: ContractPaths, *, config: BenchConfig) -> dict[str, Any]:
    bench_dir = (repo_root / "tests" / "contracts" / "rpygrep_search_contract" / "bench")
    bench_dir.mkdir(parents=True, exist_ok=True)

    profiles_out: dict[str, Any] = {}
    for p in PROFILES:
        profiles_out[p.name] = run_bench_for_profile(p, corpus_root, config=config)

        # Hard output-size budget (optional)
        if config.max_normalized_bytes and p.name in config.max_normalized_bytes:
            limit = config.max_normalized_bytes[p.name]
            cur = profiles_out[p.name]["sizes_bytes"]["normalized_median"]
            if cur > limit:
                raise AssertionError(f"[bench] profile={p.name} normalized_bytes={cur} exceeds budget={limit}")

    report = {
        "schema": "rpygrep_search_bench_report/v1",
        "policy": {
            "allowed_regression_factor": config.policy.allowed_regression_factor,
            "allowed_output_growth_factor": config.policy.allowed_output_growth_factor,
        },
        "profiles": profiles_out,
    }

    # Always emit artifact
    bench_artifacts = paths.artifacts_dir / "bench"
    bench_artifacts.mkdir(parents=True, exist_ok=True)
    dump_json(bench_artifacts / "report.json", report)

    return report


def compare_to_baseline(repo_root: Path, report: dict[str, Any], *, policy: BenchPolicy) -> list[str]:
    """
    Returns list of human-readable regression messages (empty = pass).
    """
    bp = baseline_path(repo_root)
    if not bp.exists():
        return [f"missing baseline: {bp}"]

    base = load_json(bp)
    msgs: list[str] = []
    for name, cur in (report.get("profiles") or {}).items():
        base_p = ((base.get("profiles") or {}).get(name)) if isinstance(base.get("profiles"), dict) else None
        if not base_p:
            msgs.append(f"missing baseline profile: {name}")
            continue

        cur_ms = float(cur["timings_ms"]["median"])
        base_ms = float(base_p["timings_ms"]["median"])
        if cur_ms > base_ms * policy.allowed_regression_factor:
            msgs.append(f"[time] {name}: median_ms={cur_ms:.2f} > baseline={base_ms:.2f} * {policy.allowed_regression_factor}")

        cur_sz = float(cur["sizes_bytes"]["normalized_median"])
        base_sz = float(base_p["sizes_bytes"]["normalized_median"])
        if cur_sz > base_sz * policy.allowed_output_growth_factor:
            msgs.append(f"[size] {name}: norm_bytes={cur_sz:.0f} > baseline={base_sz:.0f} * {policy.allowed_output_growth_factor}")

    return msgs


def write_baseline(repo_root: Path, report: dict[str, Any]) -> None:
    bp = baseline_path(repo_root)
    bp.parent.mkdir(parents=True, exist_ok=True)
    dump_json(bp, report)
```

### Add: `tests/contracts/rpygrep_search_contract/test_benchmark_rpygrep_search.py`

```python
from __future__ import annotations

from pathlib import Path
import pytest

from .bench import BenchConfig, BenchPolicy, bench_enabled, compare_to_baseline, run_benchmarks, strict_bench, update_baseline, write_baseline
from .corpus import CorpusSpec
from .report import ContractPaths


@pytest.fixture(scope="session")
def repo_root(pytestconfig) -> Path:
    return Path(pytestconfig.rootpath)


@pytest.fixture(scope="session")
def contract_paths(repo_root: Path) -> ContractPaths:
    return ContractPaths.from_repo_root(repo_root)


@pytest.fixture(scope="session")
def corpus_root(tmp_path_factory) -> Path:
    root = tmp_path_factory.mktemp("rpygrep_contract_corpus_bench")
    CorpusSpec().materialize(root)
    return root


def test_rpygrep_benchmarks(repo_root: Path, corpus_root: Path, contract_paths: ContractPaths) -> None:
    if not bench_enabled():
        pytest.skip("Benchmarks disabled. Set RUN_BENCHMARKS=1 to enable.")

    config = BenchConfig(
        warmup_runs=1,
        timed_runs=5,
        policy=BenchPolicy(allowed_regression_factor=1.50, allowed_output_growth_factor=1.10),
        # Optional hard budgets (tune to your agent payload constraints):
        max_normalized_bytes={
            "interactive_query": 40_000,
            "literal_quick_scan": 40_000,
            "precision_regex": 60_000,
            "batch_audit": 120_000,
        },
    )

    report = run_benchmarks(repo_root, corpus_root, contract_paths, config=config)

    if update_baseline():
        write_baseline(repo_root, report)
        return

    msgs = compare_to_baseline(repo_root, report, policy=config.policy)
    if msgs and strict_bench():
        raise AssertionError("Benchmark regression:\n" + "\n".join(f"- {m}" for m in msgs))
    if msgs:
        # Non-strict: don’t fail, but keep it visible in logs.
        print("Benchmark warnings:\n" + "\n".join(f"- {m}" for m in msgs))
```

### Add: `tests/contracts/rpygrep_search_contract/bench/baseline.json` (seed file)

Commit an initial baseline by running:

* `RUN_BENCHMARKS=1 UPDATE_BENCH_BASELINE=1 pytest -q tests/contracts/rpygrep_search_contract/test_benchmark_rpygrep_search.py`

If you want a placeholder file now (so the path exists), create:

```json
{
  "schema": "rpygrep_search_bench_report/v1",
  "policy": {
    "allowed_regression_factor": 1.5,
    "allowed_output_growth_factor": 1.1
  },
  "profiles": {}
}
```

---

# 4) README patch (tell people how to use it)

Append to `tests/contracts/rpygrep_search_contract/README.md`:

```md
## Profile linter
We lint profiles to prevent dangerous agent search configurations (no caps, no excludes).

Runs automatically in:
  pytest -q tests/contracts/rpygrep_search_contract/test_profile_linter.py

## Agent payload adapter
Convert normalized contract run JSON into an agent-friendly schema (file -> snippets -> spans):

  python -c '
  import json
  from pathlib import Path
  from tests.contracts.rpygrep_search_contract.adapter import adapt_normalized_to_agent_payload
  p = Path("tests/artifacts/rpygrep_search_contract/runs/interactive_query.json")
  data = json.loads(p.read_text())
  print(json.dumps(adapt_normalized_to_agent_payload(data), indent=2))
  '

## Benchmarks (opt-in)
Benchmarks are opt-in because timing can be flaky on dev machines.

Run:
  RUN_BENCHMARKS=1 pytest -q tests/contracts/rpygrep_search_contract/test_benchmark_rpygrep_search.py

Update baseline:
  RUN_BENCHMARKS=1 UPDATE_BENCH_BASELINE=1 pytest -q tests/contracts/rpygrep_search_contract/test_benchmark_rpygrep_search.py

Artifacts:
  tests/artifacts/rpygrep_search_contract/bench/report.json
```

---

## What you get (end state)

* **Safety gate**: if a profile loses `max_count`/`max_file_size`/`max_depth` or excludes, CI fails immediately (before you melt a repo).
* **Standard agent payload**: deterministic “file → snippets → spans” output you can return from an MCP tool (and it prunes candidate path lists unless explicitly requested).
* **Bench regression**: one opt-in test that produces a stable **bench report artifact** and checks against a committed baseline with a configurable regression factor + output-size budgets.

If you want, I can also give you a **thin `codeintel.search` runtime module** that reuses the exact same profile objects + linter + adapter (so your production MCP tool is guaranteed to match the contract suite).

[1]: https://pypi.org/project/rpygrep/0.2.1/ "rpygrep · PyPI"
