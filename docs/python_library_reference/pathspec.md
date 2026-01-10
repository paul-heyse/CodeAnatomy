Below is a **clustered feature inventory** for **`pathspec` (PyPI: 0.12.1)**, written in the same “dense surface-area catalog” style as your attached PyArrow reference. 

(Notes on versioning: PyPI’s latest release is **0.12.1 (Dec 10, 2023)**. ([PyPI][1]))

---

## 0) What `pathspec` is (scope + intent)

* **Purpose:** pattern matching for file paths, focused on **Git’s `.gitignore` / wildmatch-style** rules. ([PathSpec][2])
* **Core model:** compile *pattern lines* → a **spec** (ordered list of compiled patterns) → run match/check over files or directory trees. ([PathSpec][2])

---

## 1) “Front door” API (what’s exported at `import pathspec`)

These are intentionally re-exported from the package root:

* **Specs**

  * `pathspec.PathSpec`
  * `pathspec.GitIgnoreSpec` (also available as `pathspec.gitignore.GitIgnoreSpec`) ([PathSpec][3])
* **Pattern base types**

  * `pathspec.Pattern`
  * `pathspec.RegexPattern` ([PathSpec][3])
* **Utilities / errors**

  * `pathspec.RecursionError`
  * `pathspec.lookup_pattern()` ([PathSpec][3])
* **Deprecated re-exports (kept for backwards compatibility)**

  * `pathspec.util.iter_tree()` (alias)
  * `pathspec.util.match_files()` (old) ([PathSpec][3])

---

## 2) Pattern system (how matching is represented)

### 2.1 `Pattern` (abstract base)

* **Inclusion semantics:** `include ∈ {True, False, None}` meaning “include”, “exclude”, or “no-op / doesn’t affect result”. ([PathSpec][3])
* **Match API**

  * `Pattern.match_file(file: str) -> Optional[Any]` (returns a “match result” if matched, else `None`). ([PathSpec][3])
  * `Pattern.match(files)` is **deprecated** (use `match_file()` in a loop). ([PathSpec][3])

### 2.2 `RegexPattern` (regex-backed pattern implementation)

* **Construction:** `RegexPattern(pattern, include=None)` where `pattern` may be `str | bytes | re.Pattern | None`. ([PathSpec][3])
* **Conversion hook for subclasses:** `pattern_to_regex(pattern) -> (regex, include)` (the core “compile this syntax into a regex” contract). ([PathSpec][3])
* **Introspection**

  * `RegexPattern.pattern` stores the original/uncompiled pattern (added in 0.12.0). ([PyPI][1])
  * `RegexPattern.regex` holds the compiled `re.Pattern`. ([PathSpec][3])
* **Match result carrier:** `RegexMatchResult(match: re.Match)` (dataclass wrapper). ([PathSpec][3])

---

## 3) Built-in pattern languages shipped with the library

### 3.1 Git wildmatch (`.gitignore`-style) pattern compilation

* `pathspec.patterns.gitwildmatch.GitWildMatchPattern`

  * Inherits `RegexPattern`. ([PathSpec][3])
  * `GitWildMatchPattern.pattern_to_regex(pattern: str|bytes) -> (regex, include)` compiles wildmatch syntax → regex. ([PathSpec][3])
  * `GitWildMatchPattern.escape(s)` escapes special chars for building patterns safely. ([PathSpec][3])
* The docs/tutorial explicitly show wildmatch features like:

  * `#` comment lines,
  * directory-only patterns ending in `/`,
  * negation via leading `!`. ([PathSpec][2])

### 3.2 “gitignore” as a named pattern style (factory name)

`PathSpec.from_lines(...)` accepts a **registered pattern-factory name** (string), not just a class. ([PathSpec][3])

Two names you’ll see in practice:

* **`'gitwildmatch'`** (documented in the tutorial as the registered name for `GitWildMatchPattern`). ([PathSpec][2])
* **`'gitignore'`** (documented in the GitHub README for building a `PathSpec` using “gitignore patterns”). ([GitHub][4])

### 3.3 “Two gitignore behaviors”: basic vs “fully Git-faithful”

* `PathSpec.from_lines('gitignore', ...)` uses a **basic** gitignore implementation (“follows patterns as documented”). ([GitHub][4])
* `GitIgnoreSpec` exists specifically because **Git’s real behavior has edge cases** (e.g., Git allows including files from excluded directories); `GitIgnoreSpec` aims to replicate Git’s behavior more closely. ([GitHub][4])

---

## 4) Specs (compiled rule sets)

### 4.1 `PathSpec` (generic compiled spec)

* **Definition:** wrapper over an ordered list of compiled `Pattern` objects. ([PathSpec][3])
* **Constructors**

  * `PathSpec(patterns)` where `patterns` is `Collection|Iterable[Pattern]`. ([PathSpec][3])
  * `PathSpec.from_lines(pattern_factory, lines)` where `pattern_factory` is either:

    * a **registered name** (`str`), or
    * a callable/class that compiles a line to a `Pattern`. ([PathSpec][3])
* **Basic dunder surface**

  * `__len__()` and `__eq__()` are documented. ([PathSpec][3])
  * The project changelog notes **“addition operator” support** was added (combine specs). ([PathSpec][5])

### 4.2 `GitIgnoreSpec` (git-faithful spec)

* Subclasses `PathSpec` and is intended to “replicate `.gitignore` behavior”. ([PathSpec][3])
* `GitIgnoreSpec.from_lines(lines, pattern_factory=None)` defaults to `GitWildMatchPattern` when `pattern_factory` is not specified. ([PathSpec][3])

---

## 5) Matching API (what you run at query time)

### 5.1 Single file match

* `spec.match_file(file, separators=None) -> bool` ([PathSpec][3])

  * `separators` controls how paths are normalized (delegates to `normalize_file`). ([PathSpec][3])

### 5.2 Match a provided list of paths

* `spec.match_files(files, separators=None, *, negate=None) -> Iterator[paths]` ([PathSpec][3])
* `negate=True` flips the meaning: “a pattern match excludes instead of includes” (useful for “ignore like gitignore” workflows). ([PathSpec][3])

### 5.3 Match filesystem trees

* `spec.match_tree_files(root, on_error=None, follow_links=None, *, negate=None) -> Iterable[str]` ([PathSpec][3])
* `spec.match_tree_entries(root, on_error=None, follow_links=None, *, negate=None) -> Iterator[TreeEntry]` ([PathSpec][3])
* `spec.match_tree(...)` exists as an alias (historical name). ([PathSpec][3])

**Operational knobs for tree walks**

* `on_error(exc: OSError)` callback to handle filesystem errors; re-raise to abort. ([PathSpec][3])
* `follow_links` (defaults True when not specified). ([PathSpec][3])

### 5.4 Match “entries” (pre-walked) instead of raw strings

* `spec.match_entries(entries: Iterable[TreeEntry], separators=None, *, negate=None) -> Iterator[TreeEntry]` ([PathSpec][3])

---

## 6) Checking / diagnostics API (match + “why / which rule”)

### 6.1 Check methods on `PathSpec` (added in 0.12.0)

* `spec.check_file(file, separators=None) -> CheckResult`
* `spec.check_files(files, separators=None) -> Iterator[CheckResult]`
* `spec.check_tree_files(root, on_error=None, follow_links=None, negate=...) -> Iterator[CheckResult]` ([PathSpec][3])

The PyPI changelog describes these as returning more info (e.g. **`CheckResult.index` = last-matching pattern index**). ([PyPI][1])

### 6.2 `CheckResult` payload

* `CheckResult(file, include, index)` where:

  * `include`: `True|False|None` (“None” = no pattern matched)
  * `index`: `int|None` index of the last matching pattern ([PathSpec][3])

### 6.3 “Detailed matching”: which patterns matched which files

* `pathspec.util.detailed_match_files(patterns, files, all_matches=None) -> dict[file, MatchDetail]` ([PathSpec][3])
* `MatchDetail.patterns` lists the patterns that matched in encounter order. ([PathSpec][3])

---

## 7) Filesystem integration layer (`pathspec.util`)

### 7.1 Tree iteration primitives

* `iter_tree_files(root, on_error=None, follow_links=None) -> Iterator[str]` ([PathSpec][3])
* `iter_tree_entries(root, on_error=None, follow_links=None) -> Iterator[TreeEntry]`

  * walks files **and directories**, yields `TreeEntry` relative to `root` ([PathSpec][3])
  * raises `pathspec.RecursionError` when recursion is detected ([PathSpec][3])
* `iter_tree()` is a deprecated alias for `iter_tree_files()`. ([PathSpec][3])

### 7.2 `TreeEntry` (stat-aware entry object)

* Fields: `name`, `path` ([PathSpec][3])
* Predicates: `is_dir(follow_links)`, `is_file(follow_links)`, `is_symlink()` ([PathSpec][3])
* Cached stat retrieval: `stat(follow_links)` ([PathSpec][3])

### 7.3 Directory-vs-file disambiguation helper

* `append_dir_sep(path: pathlib.Path) -> str` (adds trailing separator for directories). ([PathSpec][3])

---

## 8) Path normalization & separator policy

### 8.1 Canonical normalization

* `normalize_file(file, separators=None) -> str`

  * normalizes to POSIX `/`
  * makes paths relative (removes leading `/`) ([PathSpec][3])
* `NORMALIZE_PATH_SEPS` lists OS separators that should be normalized. ([PathSpec][3])

### 8.2 Deprecated batch normalization

* `normalize_files(files, separators=None)` is deprecated (use `normalize_file` in a loop). ([PathSpec][3])

---

## 9) Pattern factory registry & extensibility

### 9.1 Lookup / registration

* `lookup_pattern(name) -> pattern_factory` (raises `KeyError` if missing). ([PathSpec][3])
* `register_pattern(name, pattern_factory, override=None)`

  * `override=False` (default) raises if the name already exists. ([PathSpec][3])

### 9.2 Registry errors

* `AlreadyRegisteredError(name, pattern_factory)` carries `.name`, `.pattern_factory`, `.message`. ([PathSpec][3])

---

## 10) “Ignore like gitignore” workflows (common footgun + built-in fix)

* `PathSpec` / `GitIgnoreSpec` **positively match** by default (they return “things that match the spec”). ([PathSpec][2])
* To use them as a traditional “ignore list” (i.e., return files to keep), you flip results with `negate=True` on tree/list matching. ([GitHub][4])

---

## 11) Compatibility / deprecations snapshot

From the docs + changelog:

* `Pattern.match()` deprecated → `Pattern.match_file()` loop. ([PathSpec][3])
* `util.match_files()` deprecated (old, returns set). ([PathSpec][3])
* `match_tree()` renamed to `match_tree_files()`; old name remains as alias. ([PathSpec][5])

---

## 12) Performance features (present on `main` / dev docs; verify against your pinned version)

The GitHub README and the “latest” ReadTheDocs API describe **optional regex backends** and a `backend=` selection knob:

* Backends described: `"simple"` (Python `re`), `"hyperscan"`, `"re2"`, with default `"best"` auto-selection. ([GitHub][4])
* Install extras: `pip install pathspec[hyperscan]`, `pip install pathspec[google-re2]`. ([GitHub][4])

If you want this catalog to be *version-pinned* to **exactly** what you’ll have in-prod (e.g., “0.12.1 only, no dev features”), tell me what version you’re pinning and I’ll emit the inventory as a strict “public API contract surface” for that version.

[1]: https://pypi.org/project/pathspec/ "pathspec · PyPI"
[2]: https://python-path-specification.readthedocs.io/en/stable/readme.html "PathSpec — PathSpec 0.12.1 documentation"
[3]: https://python-path-specification.readthedocs.io/en/stable/api.html "API — PathSpec 0.12.1 documentation"
[4]: https://github.com/cpburnz/python-pathspec "GitHub - cpburnz/python-pathspec: Utility library for gitignore style pattern matching of file paths."
[5]: https://python-path-specification.readthedocs.io/en/stable/changes.html "Change History — PathSpec 0.12.1 documentation"
