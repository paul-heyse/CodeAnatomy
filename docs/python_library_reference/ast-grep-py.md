Below is a **feature catalog** for **`ast-grep-py`** (Python bindings for ast-grep), clustered into logical sections and written in the same “surface area inventory” spirit as your PyArrow reference. 

---

# ast-grep-py — Comprehensive feature catalog (Python API + rule system surface)

## 0) Package + positioning

* **PyPI package**: `ast-grep-py` (latest shown: **0.40.3**, released **Dec 15, 2025**), **Requires Python ≥ 3.8**. ([PyPI][1])
* **What it is**: a **native (PyO3) Python binding** for ast-grep so you can **parse code to a syntax tree**, **search structurally**, and **generate text patches** programmatically. ([Ast Grep][2])

---

## 1) Core object model

### 1.1 `SgRoot` — parse entrypoint

* **Constructor**: `SgRoot(src: str, language: str)`
* **Root access**: `SgRoot.root() -> SgNode` ([Ast Grep][2])

Minimal flow (as documented):

```python
from ast_grep_py import SgRoot

root = SgRoot("print('hello world')", "python")
node = root.root()
print_stmt = node.find(pattern="print($A)")
print(print_stmt.get_match("A").text())
```

([Ast Grep][2])

### 1.2 `SgNode` — the main AST handle (immutable)

`SgNode` is the “query/traverse/edit” handle. It is **immutable**; edits are expressed as patch objects. ([Ast Grep][2])

---

## 2) Node inspection and source location primitives

### 2.1 Inspection methods

* `range() -> Range`
* `is_leaf() -> bool`
* `is_named() -> bool`
* `is_named_leaf() -> bool`
* `kind() -> str`
* `text() -> str` ([Ast Grep][2])

### 2.2 `Range` / `Pos` location model (0-indexed)

* `node.range()` returns start/end positions (`Pos`) with `line`, `column`, and an absolute `index` (offset). ([Ast Grep][2])

This is the canonical bridge to “patch application” and to mapping matches back to source. ([Ast Grep][2])

---

## 3) Search surface: `find` / `find_all`

### 3.1 Two search modes (overloads)

`SgNode.find(...)` / `find_all(...)` accept either:

1. **Rule kwargs** (simple, direct): `node.find(pattern="...", kind="...", ...)`
2. A **Config dict** (YAML-like config object): `node.find({...})` ([Ast Grep][2])

Key signatures: ([Ast Grep][2])

* `find(...) -> Optional[SgNode]`
* `find_all(...) -> List[SgNode]`

### 3.2 Rule-kwargs search

* Match by atomic rule keys like `pattern=...`, `kind=...`, etc. ([Ast Grep][2])

### 3.3 Config-based search (YAML rule-config subset)

* Lets you use a config object with additional capabilities like **constraints**, **utils**, **transform**. ([Ast Grep][2])

---

## 4) Captures: retrieving meta-variables from a match

Once you have a matched node:

* `get_match("A") -> Optional[SgNode]` (single metavariable like `$A`)
* `get_multiple_matches("ARGS") -> List[SgNode]` (multi metavariable like `$$$ARGS`)
* `node["A"] -> SgNode` (like `get_match`, but raises `KeyError` if missing; useful for type-checking) ([Ast Grep][2])

### 4.1 Transforms output access

* `get_transformed(name: str) -> Optional[str]` returns the **string output** of a `transform`-defined variable. ([Ast Grep][3])

(Transform definition lives in the Config surface; see §7.)

---

## 5) Refinement predicates (post-match filtering)

These are “boolean checks” on a node, using **Rule kwargs only**:

* `matches(**rule) -> bool`
* `inside(**rule) -> bool`
* `has(**rule) -> bool`
* `precedes(**rule) -> bool`
* `follows(**rule) -> bool` ([Ast Grep][2])

These correspond to the relational/composite rule ideas but used as **post-match predicates** in Python. ([Ast Grep][2])

---

## 6) Traversal primitives (AST navigation)

`SgNode` provides jQuery-like navigation helpers:

* `get_root() -> SgRoot`
* `field(name: str) -> Optional[SgNode]`
* `parent() -> Optional[SgNode]`
* `child(nth: int) -> Optional[SgNode]`
* `children() -> List[SgNode]`
* `ancestors() -> List[SgNode]`
* `next() / next_all()`
* `prev() / prev_all()` ([Ast Grep][2])

Use these when you need **context-aware rewrites** that exceed what a single structural pattern can express. ([Ast Grep][4])

---

## 7) Rule object surface (what `Rule` can express)

In Python, **`Rule`** is a `TypedDict` mirroring the YAML rule object fields. ([Ast Grep][3])

### 7.1 Atomic rule keys

* `pattern`: `str` **or** object-style pattern `{context, selector, strictness?}` ([Ast Grep][5])

  * Object-style `pattern` lets you provide **parsing context** and select a node kind from it (reduces ambiguity). ([Ast Grep][5])
  * `strictness` options include `cst`, `smart`, `ast`, `relaxed`, `signature`. ([Ast Grep][5])
* `kind`: node kind name; (ast-grep **0.39+** supports limited ESQuery syntax like `call_expression > identifier`). ([Ast Grep][5])
* `regex`: Rust regex matching the **whole node text** (with Rust regex limitations). ([Ast Grep][5])
* `nthChild`: match by sibling position (number / `An+B` / object with `position`, `reverse`, `ofRule`), **1-based**, **named nodes only**. ([Ast Grep][5])
* `range`: match nodes constrained to a `(start{line,column}, end{line,column})` region. ([Ast Grep][5])

### 7.2 Relational rule keys

These embed “relative position” checks:

* `inside`, `has`, `precedes`, `follows` — each is a relational rule object that extends `Rule` with:

  * `stopBy`: `"neighbor" | "end" | Rule`
  * `field`: (for some relations) limit traversal to a named field ([Ast Grep][5])

### 7.3 Composite rule keys

* `all: List[Rule]`
* `any: List[Rule]`
* `not: Rule` (note Python `TypedDict` can’t use `not` as an attribute name; but YAML supports it)
* `matches: str` (references a utility rule id) ([Ast Grep][3])

---

## 8) Pattern syntax (what you can put inside `pattern="..."`)

Patterns are parsed as code by tree-sitter; they must be valid code for the target language. ([Ast Grep][6])

### 8.1 Single meta variables (`$A`, `$FOO`, …)

* `$NAME` matches **one AST node**; names are uppercase/underscore/digits (not lowercase). ([Ast Grep][6])

### 8.2 Multi meta variables (`$$$`, `$$$ARGS`)

* `$$$` / `$$$NAME` matches **zero or more AST nodes** (arguments, parameters, statements, etc.). ([Ast Grep][6])

### 8.3 Capture semantics

* Reusing the same metavariable name (e.g. `$A == $A`) enforces “same subtree” capture matching. ([Ast Grep][6])
* Names starting with `_` are **non-capturing** (useful for speed/cleaner captures). ([Ast Grep][6])
* To capture **unnamed nodes**, use `$$VAR` (double-dollar) rather than `$VAR`. ([Ast Grep][6])

---

## 9) Config surface (what Python `find({...})` accepts)

`Config` is a `TypedDict` with (at least):

* `rule: Rule`
* `constraints: Dict[str, Mapping]`
* `utils: Dict[str, Rule]`
* `transform: Dict[str, Mapping]` ([Ast Grep][3])

### 9.1 `constraints` (post-rule metavariable filters)

* Applied **after** the main `rule` matches. ([Ast Grep][7])
* Keys are metavariable names **without `$`** and only apply to **single metavariables** (not `$$$ARGS`). ([Ast Grep][7])
* “Usually do not work inside `not`.” ([Ast Grep][7])

### 9.2 `utils` + `matches` (utility rule reuse)

* `utils` defines a map of named rules (local utility rules) that can be referenced by `matches`. ([Ast Grep][7])
* **No cyclic dependency** when using `matches` (to avoid infinite recursion). ([Ast Grep][8])

### 9.3 `transform` (metavariable string transforms)

* `transform` maps **new variable names** to transformation objects or string-style expressions (string-style supported for ast-grep 0.38.3+). ([Ast Grep][7])
* Supported transform operations include:

  * `replace` (regex replace) ([Ast Grep][9])
  * `substring` (Python-slice-like indices; unicode character indices) ([Ast Grep][9])
  * `convert` (case transforms: camel/snake/kebab/pascal/etc + separator rules) ([Ast Grep][9])
  * `rewrite` (experimental: applies rewriter rules to captured sub-AST) ([Ast Grep][9])

Access transform results from Python with `SgNode.get_transformed("NAME")`. ([Ast Grep][3])

---

## 10) Rewrite/edit primitives (what you actually do in Python)

### 10.1 Patch objects: `Edit`

* `Edit.start_pos: int`
* `Edit.end_pos: int`
* `Edit.inserted_text: str` ([Ast Grep][2])

### 10.2 Creating and applying edits

* `node.replace(new_text) -> Edit`
* `node.commit_edits([edits...]) -> str` produces new source text. ([Ast Grep][2])

**Metavariable substitution is NOT automatic in Python edits**: unlike the CLI’s `fix`/rewrite templating, the Python API’s `replace(new_text)` treats `new_text` as literal. You’re expected to build strings yourself from `get_match()` results. ([Ast Grep][2])

### 10.3 Multiple edits: ordering matters

The project’s RFC/tests demonstrate:

* You can generate many edits, then **sort edits in reverse `start_pos` order** before `commit_edits` to avoid shifting offsets. ([GitHub][10])
* `commit_edits` can be called on the **matched node** or the **root node** to produce the final rewritten source string. ([GitHub][10])

---

## 11) Language selection (what strings you can pass)

* `SgRoot(src, language)` takes a language identifier string (examples use `"python"` / `"javascript"`). ([Ast Grep][2])
* The official config reference lists valid language names like `Bash`, `C`, `Cpp`, `CSharp`, `Css`, `Go`, `Html`, `Java`, `JavaScript`, `Json`, `Kotlin`, `Lua`, `Nix`, `Php`, `Python`, `Ruby`, `Rust`, `Scala`, `Solidity`, `Swift`, `Tsx`, `TypeScript`, `Yaml`. ([Ast Grep][7])

---

## 12) Practical boundaries (important for agent-driven use)

* **API exists to do what rules can’t express well**: conditional rewrites, computed replacement strings, counting/order-sensitive transforms, context-dependent patching. ([Ast Grep][4])
* **Applying CLI-style `fix` automatically via JS/Python API is described as experimental**, but the supported building blocks are `replace()` + `commit_edits()`. ([Ast Grep][4])

---

If you want the next increment (like your PyArrow doc): I can expand each cluster above into “deep dive pages” with **power knobs + failure modes + minimal patterns**, especially for (a) robust multi-edit patch application (overlap detection + stable ordering), (b) a CodeIntel-grade rule/config layering strategy (local utils + constraints + transforms), and (c) performance patterns for large-file and multi-file workloads.

[1]: https://pypi.org/project/ast-grep-py/ "ast-grep-py · PyPI"
[2]: https://ast-grep.github.io/guide/api-usage/py-api.html "Python API | ast-grep"
[3]: https://ast-grep.github.io/reference/api.html "API Reference | ast-grep"
[4]: https://ast-grep.github.io/guide/api-usage.html "API Usage | ast-grep"
[5]: https://ast-grep.github.io/reference/rule.html "Rule Object Reference | ast-grep"
[6]: https://ast-grep.github.io/guide/pattern-syntax.html "Pattern Syntax | ast-grep"
[7]: https://ast-grep.github.io/reference/yaml.html "Configuration Reference | ast-grep"
[8]: https://ast-grep.github.io/guide/rule-config/utility-rule.html?utm_source=chatgpt.com "Reusing Rule as Utility"
[9]: https://ast-grep.github.io/reference/yaml/transformation.html "Transformation Object | ast-grep"
[10]: https://github.com/ast-grep/ast-grep/issues/1172 "[RFC] Using fix in JS/Python API · Issue #1172 · ast-grep/ast-grep · GitHub"


## A) Robust multi-edit patch application — deep dive (ast-grep-py)

### Mental model: edits are *patches over the original source string*

* `SgNode` is **immutable**; you don’t mutate the tree in-place. Instead, you generate an `Edit` with `replace()`, then apply edits with `commit_edits()`. ([Ast Grep][1])
* `Edit` is a 3-tuple: `start_pos`, `end_pos`, `inserted_text`. ([Ast Grep][1])
* **Important:** Python `replace()` does **not** substitute meta-variables for you; you must build replacement strings yourself from captured nodes. ([Ast Grep][1])
* Node locations are expressed with 0-indexed `Pos` objects; `range().start.index` / `range().end.index` give offsets you can use for diagnostics and invariants. ([Ast Grep][1])

---

### A1) Single-edit workflow (baseline)

**Power knobs**

* Prefer `replace()` to hand-rolled slicing when possible (it gives you a consistent `Edit` object). ([Ast Grep][1])
* Use `range()` to log exactly what you’re about to edit (helps with “why did we rewrite this?”). ([Ast Grep][1])

**Minimal snippet**

```python
from ast_grep_py import SgRoot

src = "print('hello world')\n"
root = SgRoot(src, "python").root()

node = root.find(pattern="print($A)")
edit = node.replace("logger.log('bye world')")
out = node.commit_edits([edit])
```

`replace()` → `Edit`, `commit_edits()` → new source string. ([Ast Grep][1])

**Failure modes**

* **Metavariable template surprise**: `"logger.log($A)"` stays literal in Python; you must build it. ([Ast Grep][1])
* **Scope ambiguity**: `commit_edits()` returns “a new source string”, but the docs don’t explicitly define whether it’s for the whole original source or the node’s slice. In practice, treat it as “apply edits to the same text coordinate space the `Edit` came from”, and lock this down with tests for multi-statement inputs. ([Ast Grep][1])

---

### A2) Multi-edit workflow (what you actually want in codemods)

#### A2.1 The invariant you must enforce

Edits are defined on the **original** coordinate space. If you apply one edit and then compute later edits on the modified string, offsets drift. So:

* **Compute all edits first**
* **Validate + deconflict**
* **Apply once** (or apply per-file once)

#### A2.2 Deterministic “edit plan” pipeline

**Suggested internal representation**

```python
from dataclasses import dataclass

@dataclass(frozen=True)
class PlannedEdit:
    start: int
    end: int
    text: str
    rule_id: str          # optional: provenance
    match_kind: str       # optional: diagnostics
```

**Algorithm (robust default)**

1. **Collect candidates** with `find_all(...)` (avoid per-node traversal loops; see §C). ([Ast Grep][1])
2. **Materialize edits** from each matched node:

   * build replacement string using captures (`node["A"].text()` etc.) ([Ast Grep][1])
   * `edit = node.replace(replacement)`
3. **Normalize into PlannedEdit**: `(edit.start_pos, edit.end_pos, edit.inserted_text)`
4. **Validate bounds**: `0 <= start <= end <= len(src)`
5. **De-overlap** using a policy (below)
6. **Sort in a stable order** (recommended: descending `start`, then descending `end`)
7. **Apply**: `commit_edits(sorted_edits)`

#### A2.3 Overlap detection + policies (pick one explicitly)

Overlaps happen constantly because AST matches nest.

**Detect**

* Sort ascending by `(start, end)`
* Walk and flag if `next.start < current.end`

**Recommended policies**

* **ERROR (strict codemod)**: fail the whole file if any overlap exists.
* **DROP_INNER**: if two edits overlap and one is fully contained in the other, drop the inner (you’re rewriting a larger syntactic unit).
* **DROP_OUTER**: keep the most specific rewrite; drop the outer.
* **PREFER_LONGEST / PREFER_SHORTEST**: deterministic, but less semantically grounded.
* **ALLOW_SAME_SPAN_IF_SAME_TEXT**: if same `(start,end)` edits produce identical `inserted_text`, dedupe; otherwise error.

**Code pattern**

```python
def resolve_overlaps(edits: list[PlannedEdit]) -> list[PlannedEdit]:
    edits = sorted(edits, key=lambda e: (e.start, e.end))
    out: list[PlannedEdit] = []
    for e in edits:
        if not out:
            out.append(e); continue
        prev = out[-1]
        if e.start >= prev.end:
            out.append(e); continue

        # overlap
        if (e.start, e.end) == (prev.start, prev.end) and e.text == prev.text:
            continue  # exact duplicate
        # default strict:
        raise ValueError(f"overlapping edits: {prev} vs {e}")
    return out
```

#### A2.4 Stable ordering (why “descending start” is the safest default)

Even if `commit_edits()` internally handles offset shifting, passing edits in descending start order is a **defensive invariant**: later (earlier-in-file) edits cannot invalidate earlier (later-in-file) spans.

---

### A3) “Python doesn’t template metavariables”: build a tiny renderer

Docs explicitly warn metavariables are not replaced in `replace()`. ([Ast Grep][1])

**Minimal pattern**

```python
def render_logger(node) -> str:
    # node matched print($A)
    a = node["A"].text()  # captured argument text
    return f"logger.log({a})"
```

When you need something closer to CLI `fix: "logger.log($$$ARGS)"`, implement a small interpolator over captured nodes (string-level, not AST-level).

---

### A4) Contract tests for patching (high leverage)

1. **Golden rewrite**: input → output exact match.
2. **Permutation invariance**: shuffle matches; your sorted/planned edit pipeline must yield the same output.
3. **No-overlap invariant**: assert `resolve_overlaps()` passes for all rule packs you ship.
4. **Idempotence**: apply rewrite twice; second pass yields identical output (or zero edits).

---

## B) CodeIntel-grade rule/config layering — deep dive (utils + constraints + “transforms”)

### Mental model: separate *matching* from *post-processing*

* Python `find/find_all` accept either:

  * **Rule kwargs** (simple), or
  * a **Config object** (YAML-like) which supports extra control like `constraints` and `utils`. ([Ast Grep][1])
* `constraints` are applied **after** the core `rule` matches, and constrained metavariables usually don’t work inside `not`. ([Ast Grep][2])
* Utility rules can be defined locally via `utils` and referenced via `matches`. ([Ast Grep][3])

---

### B1) Layer 0 — “Rule kwargs” (fast path, minimal surface)

Use when you need quick structural search and will filter in Python.

```python
nodes = root.find_all(pattern="print($A)")
```

**Failure modes**

* You can’t use `utils`/`constraints` in kwargs mode. ([Ast Grep][1])

---

### B2) Layer 1 — Config + constraints (most useful day-to-day)

Python API shows Config usage mirroring YAML, including constraints. ([Ast Grep][1])

```python
config = {
  "rule": {"pattern": "print($A)"},
  "constraints": {"A": {"regex": "hello"}}
}
node = root.find(config)
```

**Power knobs**

* Treat constraints as “match refinement” you want to keep *declarative* (instead of Python `if` chains).

**Failure modes**

* Constraints run after `rule`; so if your `rule` is too broad, you may pay for a large candidate set first. ([Ast Grep][2])
* Constraints + `not`: “usually do not work” inside `not`. ([Ast Grep][2])

---

### B3) Layer 2 — Local utility rules (“utils”) for reusable sub-matchers

Utility rules are explicitly designed for reuse via `matches`, and can be local (`utils:` dict). ([Ast Grep][3])

**Pattern**

```python
config = {
  "utils": {
    "is-literal": {
      "any": [
        {"kind": "string"},
        {"kind": "number"},
        {"kind": "true"},
        {"kind": "false"},
        {"kind": "null"},
      ]
    }
  },
  "rule": {
    "kind": "array",
    "has": {"matches": "is-literal"}
  }
}
```

**Power knobs**

* Use `utils` to encode “semantic atoms” (literal, logging-call, deprecated-api-use) once, then compose.

**Failure modes**

* Local utility rules are scoped to the config where they’re defined; and they can’t have separate `constraints`. ([Ast Grep][3])

---

### B4) Layer 3 — “Transforms” as a *Python-side* deterministic post-process

YAML `transform` exists to rewrite metavariables before `fix`. ([Ast Grep][4])
But Python `replace()` won’t do metavariable templating for you. ([Ast Grep][1])

So for a CodeIntel-grade approach: **store transforms as data** (same shape as YAML transform objects) and interpret them in Python to produce derived strings deterministically.

**Supported ops in the transform reference**

* `replace` (regex replace), `substring`, `convert` (case conversion), and `rewrite` (experimental). ([Ast Grep][5])

**Why do this**

* Lets you keep “rule packs” declarative + diffable, while still generating Python replacement strings.

**Minimal interpreter sketch (example: substring + convert)**

```python
def apply_transform(op: dict, value: str) -> str:
    if "substring" in op:
        s = op["substring"]
        start = s.get("startChar")
        end = s.get("endChar")
        return value[start:end]
    if "convert" in op:
        # implement a small subset you need (snake/camel/kebab)
        ...
    if "replace" in op:
        ...
    raise NotImplementedError(op)
```

**Failure mode**

* `rewrite` is explicitly experimental; don’t build critical invariants on it without golden tests. ([Ast Grep][6])

---

### B5) Layer 4 — Rule authoring knobs that prevent “match explosions”

#### Strictness (controls what can be skipped during matching)

ast-grep documents strictness levels: `cst`, `smart` (default), `ast`, `relaxed`, `signature`. ([Ast Grep][7])
Use stricter modes when patterns overmatch (especially around trivia/comments/quotes).

#### Non-capturing metavariables (`$_NAME`)

Metavariables starting with `_` suppress capture and can match independently (useful when you only care about structure). ([Ast Grep][8])

#### “pattern object” for ambiguous parsing

If you’re trying to combine `kind` and `pattern` to force parsing, FAQ notes that doesn’t work the way people expect; use a pattern object (`context` + `selector`) instead. ([Ast Grep][9])

#### Order-sensitive matching → force order with `all`

Rules are unordered dictionaries; application order is implementation-defined, and `all` can be used to force an order when metavariable interactions matter. ([Ast Grep][9])

---

## C) Performance patterns — large files + many files (Python)

### Mental model: cost centers

1. **Parse cost** (`SgRoot(src, lang)`) ([Ast Grep][1])
2. **Match cost** (pattern/rule evaluation)
3. **FFI call overhead** (Python ↔ Rust boundary)
4. **Python overhead** (filesystem walk, reading files, building strings, applying patches)

---

### C1) Minimize FFI calls: prefer `find_all` over manual traversal

The ast-grep performance guide (for NAPI) strongly emphasizes reducing cross-language calls and shows `findAll` is faster than recursing with `kind()` / `children()` because recursion incurs many FFI calls. ([Ast Grep][10])

**In Python, treat this as the same principle**:

* Avoid `for child in node.children(): ...` in hot paths
* Prefer:

  * `root.find_all(kind="...")`
  * `root.find_all({...config...})`

---

### C2) Reduce match explosions (the hidden killer)

**Power knobs**

* Use stricter matching when appropriate (see strictness levels). ([Ast Grep][7])
* Use non-capturing metavars when you don’t need captures. ([Ast Grep][8])
* Push filtering into `constraints` rather than Python post-filters (still post-rule, but keeps logic declarative). ([Ast Grep][2])
* Use `utils` + `matches` to keep your “semantic atoms” precise and reusable. ([Ast Grep][3])

**Failure mode**

* Over-broad `pattern` + late filtering = huge candidate lists and lots of `.text()` calls.

---

### C3) Multi-file scaling patterns (Python-side)

Because `ast-grep-py` exposes a string-parse API (`SgRoot(src, language)`) ([Ast Grep][1]), the scalable approach is usually:

**Per-file plan**

1. Read file → `src`
2. Parse once → `root`
3. Run *all* rules you care about (preferably a small pack)
4. Produce:

   * matches (for analytics), and/or
   * an edit plan (for rewrites)
5. Apply edits once → write back

**Parallelism**

* If you need throughput, use **multiprocessing** (process-per-core) rather than threads unless you’ve verified the binding releases the GIL in your workload.
* Chunk by file count/size so one huge file doesn’t starve a worker.

**Kill switches**

* Max file size cutoff (skip or “text-only prefilter then parse”)
* Max matches per file per rule (cap; emit a diagnostic)
* Time budget per file (soft timeout enforced at your worker level)

---

### C4) Incremental + cached workflows (CodeIntel-aligned)

* Hash file contents; only re-run ast-grep on changed files.
* Persist “match records” (file, rule_id, ranges, capture texts) so you can diff results across runs.
* Store rewrite outputs as *derived artifacts* and gate them with golden tests (§A4, §B5).

---

If you want the *next* increment after these pages (in your repo-agent style): I can turn A/B/C into a **repo-ready “ast-grep contract pack”**: a small `astgrep_contract/` module with (1) edit-planner + overlap policy, (2) YAML-shaped rule pack loader (utils/constraints + optional transform interpreter), and (3) pytest golden fixtures that produce a single CI JSON artifact summarizing matches/overlaps/idempotence/perf counters.

[1]: https://ast-grep.github.io/guide/api-usage/py-api.html "Python API | ast-grep"
[2]: https://ast-grep.github.io/reference/yaml.html?utm_source=chatgpt.com "Configuration Reference | ast-grep"
[3]: https://ast-grep.github.io/guide/rule-config/utility-rule.html "Reusing Rule as Utility | ast-grep"
[4]: https://ast-grep.github.io/cheatsheet/yaml.html "Config Cheat Sheet | ast-grep"
[5]: https://ast-grep.github.io/reference/yaml/transformation.html "Transformation Object | ast-grep"
[6]: https://ast-grep.github.io/reference/yaml/transformation.html?utm_source=chatgpt.com "Transformation Object"
[7]: https://ast-grep.github.io/advanced/match-algorithm.html "Deep Dive into ast-grep's Match Algorithm | ast-grep"
[8]: https://ast-grep.github.io/guide/pattern-syntax.html "Pattern Syntax | ast-grep"
[9]: https://ast-grep.github.io/advanced/faq.html "Frequently Asked Questions | ast-grep"
[10]: https://ast-grep.github.io/guide/api-usage/performance-tip.html "Performance Tip for napi usage | ast-grep"

Below is a **repo-ready “ast-grep contract pack”** you can drop into your codebase. It’s designed to be:

* **Deterministic** (stable ordering, explicit overlap policy, idempotence checks)
* **Diffable** (YAML-shaped rule packs, JSON report artifact)
* **Agent-friendly** (clear module boundaries + minimal magic)
* **Low-dep** (only requires `ast-grep-py`; YAML loading is optional via `PyYAML`)

---

## 1) Directory layout (drop-in)

```text
astgrep_contract/
  __init__.py
  types.py
  rulepack.py
  templating.py
  transforms.py
  planner.py
  apply.py
  runner.py
  report.py

tests/
  conftest.py
  test_astgrep_contract_basic.py
  rules/
    basic.yaml
  fixtures/
    sample_in.py
    sample_in.after.py
  artifacts/                 # generated by pytest sessionfinish
    astgrep_contract_report.json
```

---

## 2) `astgrep_contract/` package

### `astgrep_contract/types.py`

```python
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Sequence


class OverlapPolicy(str, Enum):
    ERROR = "error"
    DROP_INNER = "drop_inner"
    DROP_OUTER = "drop_outer"
    PREFER_LONGEST = "prefer_longest"
    PREFER_SHORTEST = "prefer_shortest"
    ALLOW_DUPLICATE_SAME_TEXT = "allow_duplicate_same_text"


@dataclass(frozen=True)
class ReplacementSpec:
    """
    A Python-side replacement specification.

    template supports:
      - $NAME          (single capture)
      - $$$NAME        (multiple capture)
      - $@NAME         (transform output, via node.get_transformed("NAME"))
    """
    template: str
    joiner: str = ", "


@dataclass(frozen=True)
class RuleSpec:
    """
    One rule in a rule pack.
    - config: YAML-shaped ast-grep config dict OR a "rule kwargs" dict.
      If it contains 'rule'/'utils'/'constraints'/'transform' we treat as full config.
      Otherwise we pass as kwargs to find_all(**config).
    """
    rule_id: str
    language: str
    config: Mapping[str, Any]
    replacement: Optional[ReplacementSpec] = None
    overlap_policy: Optional[OverlapPolicy] = None
    max_matches: Optional[int] = None
    enabled: bool = True
    tags: Sequence[str] = field(default_factory=tuple)


@dataclass(frozen=True)
class PlannedEdit:
    start: int
    end: int
    text: str
    rule_id: str
    # diagnostics
    kind: Optional[str] = None


@dataclass(frozen=True)
class RuleRunStats:
    rule_id: str
    matches: int
    edits: int
    errors: List[str]
    timing_ms: Dict[str, float]


@dataclass(frozen=True)
class FileRunStats:
    path: str
    language: str
    matches: int
    edits: int
    overlaps_detected: int
    idempotent: bool
    errors: List[str]
    timing_ms: Dict[str, float]
    per_rule: List[RuleRunStats]


@dataclass(frozen=True)
class ContractReport:
    tool: str
    version: int
    files: List[FileRunStats]
    totals: Dict[str, int]
    timing_ms: Dict[str, float]
```

---

### `astgrep_contract/rulepack.py`

```python
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

from .types import OverlapPolicy, ReplacementSpec, RuleSpec


def _maybe_load_yaml(path: Path) -> Mapping[str, Any]:
    try:
        import yaml  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "YAML rule packs require PyYAML. Install with: pip install pyyaml"
        ) from e
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Rule pack root must be a mapping: {path}")
    return data


def load_rule_pack(path: str | Path) -> Sequence[RuleSpec]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(str(p))

    if p.suffix.lower() in {".yaml", ".yml"}:
        raw = _maybe_load_yaml(p)
        return parse_rule_pack(raw, default_source=str(p))
    if p.suffix.lower() == ".json":
        raw = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError(f"Rule pack JSON root must be a mapping: {p}")
        return parse_rule_pack(raw, default_source=str(p))

    raise ValueError(f"Unsupported rule pack extension: {p.suffix} (use .yaml/.yml/.json)")


def parse_rule_pack(raw: Mapping[str, Any], *, default_source: str = "<memory>") -> Sequence[RuleSpec]:
    """
    Expected YAML/JSON shape:

    version: 1
    defaults:
      language: python
      overlap_policy: error
      joiner: ", "
    rules:
      - id: replace_print
        enabled: true
        config:
          rule: { pattern: "print($A)" }
        replacement:
          template: "logger.log($A)"
          joiner: ", "
        overlap_policy: drop_inner
        max_matches: 1000
        tags: ["demo"]
    """
    version = raw.get("version", 1)
    if version != 1:
        raise ValueError(f"Unsupported rule pack version={version} from {default_source}")

    defaults = raw.get("defaults", {})
    if defaults is None:
        defaults = {}
    if not isinstance(defaults, dict):
        raise ValueError(f"defaults must be a mapping in {default_source}")

    default_lang = str(defaults.get("language", "python"))
    default_policy = defaults.get("overlap_policy")
    default_joiner = str(defaults.get("joiner", ", "))

    rules = raw.get("rules")
    if not isinstance(rules, list):
        raise ValueError(f"rules must be a list in {default_source}")

    out: list[RuleSpec] = []
    for i, r in enumerate(rules):
        if not isinstance(r, dict):
            raise ValueError(f"rules[{i}] must be a mapping in {default_source}")

        rule_id = r.get("id") or r.get("rule_id")
        if not isinstance(rule_id, str) or not rule_id.strip():
            raise ValueError(f"rules[{i}] missing valid id in {default_source}")

        enabled = bool(r.get("enabled", True))
        language = str(r.get("language", default_lang))

        config = r.get("config")
        if not isinstance(config, dict):
            raise ValueError(f"rules[{i}] config must be a mapping in {default_source}")

        # replacement is optional: you can run match-only rules for reporting.
        repl = None
        repl_raw = r.get("replacement")
        if repl_raw is not None:
            if not isinstance(repl_raw, dict):
                raise ValueError(f"rules[{i}] replacement must be a mapping in {default_source}")
            template = repl_raw.get("template")
            if not isinstance(template, str):
                raise ValueError(f"rules[{i}] replacement.template must be a string in {default_source}")
            joiner = str(repl_raw.get("joiner", default_joiner))
            repl = ReplacementSpec(template=template, joiner=joiner)

        pol_raw = r.get("overlap_policy", default_policy)
        pol = OverlapPolicy(pol_raw) if pol_raw is not None else None

        max_matches = r.get("max_matches")
        if max_matches is not None and not isinstance(max_matches, int):
            raise ValueError(f"rules[{i}] max_matches must be int in {default_source}")

        tags = r.get("tags", ())
        if isinstance(tags, list):
            tags = tuple(str(t) for t in tags)
        elif isinstance(tags, tuple):
            tags = tuple(str(t) for t in tags)
        else:
            tags = (str(tags),) if tags else ()

        out.append(
            RuleSpec(
                rule_id=rule_id,
                language=language,
                config=config,
                replacement=repl,
                overlap_policy=pol,
                max_matches=max_matches,
                enabled=enabled,
                tags=tags,
            )
        )
    return out
```

---

### `astgrep_contract/transforms.py`

A small “optional transform interpreter” you can use **even if** you don’t rely on `node.get_transformed(...)`.

```python
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Mapping, Optional


_WORD_RE = re.compile(r"[A-Za-z0-9]+")


def _split_words(s: str) -> list[str]:
    parts = _WORD_RE.findall(s)
    if not parts:
        return []
    # also split camel segments inside each part
    out: list[str] = []
    for p in parts:
        # "FooBar99" -> ["Foo", "Bar", "99"]
        segs = re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?![a-z])|\d+", p)
        out.extend(segs if segs else [p])
    return out


def _to_snake(words: list[str]) -> str:
    return "_".join(w.lower() for w in words if w)


def _to_kebab(words: list[str]) -> str:
    return "-".join(w.lower() for w in words if w)


def _to_camel(words: list[str]) -> str:
    if not words:
        return ""
    head = words[0].lower()
    tail = "".join(w[:1].upper() + w[1:].lower() if w else "" for w in words[1:])
    return head + tail


def _to_pascal(words: list[str]) -> str:
    return "".join(w[:1].upper() + w[1:].lower() if w else "" for w in words)


def apply_transform_spec(value: str, spec: Mapping[str, Any]) -> str:
    """
    Supports a pragmatic subset of ast-grep transform ops:
      - replace: { regex: "...", replacement: "..." }
      - substring: { startChar: int?, endChar: int? }
      - convert: { toCase: "snake"|"kebab"|"camel"|"pascal"|"lower"|"upper" }
    """
    if "replace" in spec:
        rs = spec["replace"]
        if not isinstance(rs, dict):
            raise ValueError("replace must be a mapping")
        pattern = rs.get("regex")
        repl = rs.get("replacement", "")
        if not isinstance(pattern, str) or not isinstance(repl, str):
            raise ValueError("replace.regex and replace.replacement must be strings")
        return re.sub(pattern, repl, value)

    if "substring" in spec:
        ss = spec["substring"]
        if not isinstance(ss, dict):
            raise ValueError("substring must be a mapping")
        start = ss.get("startChar")
        end = ss.get("endChar")
        if start is not None and not isinstance(start, int):
            raise ValueError("substring.startChar must be int")
        if end is not None and not isinstance(end, int):
            raise ValueError("substring.endChar must be int")
        return value[start:end]

    if "convert" in spec:
        cs = spec["convert"]
        if not isinstance(cs, dict):
            raise ValueError("convert must be a mapping")
        to_case = cs.get("toCase")
        if not isinstance(to_case, str):
            raise ValueError("convert.toCase must be a string")
        if to_case == "lower":
            return value.lower()
        if to_case == "upper":
            return value.upper()
        words = _split_words(value)
        if to_case == "snake":
            return _to_snake(words)
        if to_case == "kebab":
            return _to_kebab(words)
        if to_case == "camel":
            return _to_camel(words)
        if to_case == "pascal":
            return _to_pascal(words)
        raise ValueError(f"Unsupported convert.toCase={to_case!r}")

    raise ValueError(f"Unsupported transform spec keys: {list(spec.keys())}")
```

---

### `astgrep_contract/templating.py`

```python
from __future__ import annotations

import re
from typing import Any, Optional

from .types import ReplacementSpec


# tokens: $NAME, $$$NAME, $@NAME
_TOKEN_RE = re.compile(r"(\$\$\$[A-Z0-9_]+|\$@[A-Z0-9_]+|\$[A-Z0-9_]+)")


def _capture_text(node: Any, name: str) -> str:
    cap = node.get_match(name)
    if cap is None:
        raise KeyError(f"Missing capture {name}")
    return cap.text()


def _captures_text(node: Any, name: str, joiner: str) -> str:
    caps = node.get_multiple_matches(name)
    if not caps:
        return ""
    return joiner.join(c.text() for c in caps)


def _transform_text(node: Any, name: str) -> str:
    # ast-grep-py exposes node.get_transformed(name) when a transform exists in config.
    v = node.get_transformed(name)
    if v is None:
        raise KeyError(f"Missing transformed value {name}")
    return v


def render_replacement(node: Any, spec: ReplacementSpec) -> str:
    """
    Render ReplacementSpec against a matched SgNode.
    """
    template = spec.template
    joiner = spec.joiner

    parts: list[str] = []
    last = 0
    for m in _TOKEN_RE.finditer(template):
        parts.append(template[last : m.start()])
        tok = m.group(0)
        if tok.startswith("$$$"):
            name = tok[3:]
            parts.append(_captures_text(node, name, joiner=joiner))
        elif tok.startswith("$@"):
            name = tok[2:]
            parts.append(_transform_text(node, name))
        else:
            name = tok[1:]
            parts.append(_capture_text(node, name))
        last = m.end()
    parts.append(template[last:])
    return "".join(parts)
```

---

### `astgrep_contract/apply.py`

```python
from __future__ import annotations

from dataclasses import replace
from typing import Iterable, List, Sequence

from .types import OverlapPolicy, PlannedEdit


def _span_len(e: PlannedEdit) -> int:
    return e.end - e.start


def resolve_overlaps(edits: Sequence[PlannedEdit], policy: OverlapPolicy) -> List[PlannedEdit]:
    """
    Normalize and resolve overlaps deterministically.

    Always dedup exact duplicates (same span + same text).
    """
    if not edits:
        return []

    # sort by (start, end, text) for stable overlap scanning
    ordered = sorted(edits, key=lambda e: (e.start, e.end, e.text, e.rule_id))

    out: list[PlannedEdit] = []
    for e in ordered:
        if not out:
            out.append(e)
            continue

        prev = out[-1]

        # non-overlapping or touching
        if e.start >= prev.end:
            out.append(e)
            continue

        # exact duplicate span
        if (e.start, e.end) == (prev.start, prev.end):
            if e.text == prev.text:
                # keep the earlier one (stable)
                continue
            if policy == OverlapPolicy.ALLOW_DUPLICATE_SAME_TEXT:
                # different text on same span is still ambiguous -> error
                raise ValueError(f"Conflicting edits on same span: {prev} vs {e}")
            raise ValueError(f"Conflicting edits on same span: {prev} vs {e}")

        # overlap: choose per policy
        if policy == OverlapPolicy.ERROR:
            raise ValueError(f"Overlapping edits: {prev} vs {e}")

        # containment checks
        prev_contains = prev.start <= e.start and prev.end >= e.end
        e_contains = e.start <= prev.start and e.end >= prev.end

        if policy == OverlapPolicy.DROP_INNER:
            # keep outer
            if prev_contains:
                continue  # drop e
            if e_contains:
                out[-1] = e
                continue
            # partial overlap -> still ambiguous
            raise ValueError(f"Partial overlap under DROP_INNER: {prev} vs {e}")

        if policy == OverlapPolicy.DROP_OUTER:
            # keep inner
            if prev_contains:
                out[-1] = e
                continue
            if e_contains:
                continue
            raise ValueError(f"Partial overlap under DROP_OUTER: {prev} vs {e}")

        if policy == OverlapPolicy.PREFER_LONGEST:
            if _span_len(e) > _span_len(prev):
                out[-1] = e
            # else keep prev
            continue

        if policy == OverlapPolicy.PREFER_SHORTEST:
            if _span_len(e) < _span_len(prev):
                out[-1] = e
            continue

        if policy == OverlapPolicy.ALLOW_DUPLICATE_SAME_TEXT:
            # allow overlap only if identical span+text (already handled above)
            raise ValueError(f"Overlap not allowed under ALLOW_DUPLICATE_SAME_TEXT: {prev} vs {e}")

        raise ValueError(f"Unknown policy: {policy}")

    return out


def apply_edits_to_text(src: str, edits: Sequence[PlannedEdit]) -> str:
    """
    Apply edits using original-coordinate slicing.
    Requires edits to be non-overlapping.

    We apply in descending start order so earlier edits don't shift later spans.
    """
    if not edits:
        return src

    ordered = sorted(edits, key=lambda e: (e.start, e.end), reverse=True)

    out = src
    for e in ordered:
        if e.start < 0 or e.end < e.start or e.end > len(out):
            raise ValueError(f"Edit out of bounds for current text: {e} len={len(out)}")
        out = out[: e.start] + e.text + out[e.end :]
    return out
```

---

### `astgrep_contract/planner.py`

```python
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ast_grep_py import SgRoot  # type: ignore

from .apply import apply_edits_to_text, resolve_overlaps
from .templating import render_replacement
from .types import (
    FileRunStats,
    OverlapPolicy,
    PlannedEdit,
    RuleRunStats,
    RuleSpec,
)


def _is_full_config(cfg: Mapping[str, Any]) -> bool:
    return any(k in cfg for k in ("rule", "utils", "constraints", "transform"))


@dataclass(frozen=True)
class PlannedFileResult:
    src_before: str
    src_after: str
    edits: List[PlannedEdit]
    overlaps_detected: int
    file_stats: FileRunStats


def plan_and_apply_file(
    *,
    src: str,
    path: str,
    language: str,
    rules: Sequence[RuleSpec],
    default_overlap_policy: OverlapPolicy = OverlapPolicy.ERROR,
) -> PlannedFileResult:
    """
    Parse once, run rules, build an edit plan, resolve overlaps, apply edits.
    Also computes an idempotence check: apply again should yield same output.
    """
    file_errors: list[str] = []
    t0 = time.perf_counter()

    # Parse
    t_parse0 = time.perf_counter()
    root = SgRoot(src, language).root()
    t_parse1 = time.perf_counter()

    planned: list[PlannedEdit] = []
    per_rule_stats: list[RuleRunStats] = []
    overlaps_detected = 0

    total_matches = 0
    total_edits = 0

    for rs in rules:
        if not rs.enabled:
            continue
        if rs.language != language:
            continue

        r_errors: list[str] = []
        timing: Dict[str, float] = {}

        t_rule0 = time.perf_counter()
        matches: list[Any] = []

        try:
            t_match0 = time.perf_counter()
            if _is_full_config(rs.config):
                matches = root.find_all(dict(rs.config))
            else:
                # rule kwargs mode
                matches = root.find_all(**dict(rs.config))
            t_match1 = time.perf_counter()
            timing["match_ms"] = (t_match1 - t_match0) * 1000.0
        except Exception as e:
            r_errors.append(f"match_error: {type(e).__name__}: {e}")
            matches = []

        if rs.max_matches is not None and len(matches) > rs.max_matches:
            r_errors.append(f"max_matches_exceeded: got={len(matches)} cap={rs.max_matches}")
            matches = matches[: rs.max_matches]

        total_matches += len(matches)

        t_edit0 = time.perf_counter()
        edits_for_rule = 0
        if rs.replacement is not None and matches:
            for n in matches:
                try:
                    rng = n.range()
                    start = int(rng.start.index)
                    end = int(rng.end.index)
                    text = render_replacement(n, rs.replacement)
                    planned.append(
                        PlannedEdit(
                            start=start,
                            end=end,
                            text=text,
                            rule_id=rs.rule_id,
                            kind=getattr(n, "kind", lambda: None)(),
                        )
                    )
                    edits_for_rule += 1
                except Exception as e:
                    r_errors.append(f"edit_error: {type(e).__name__}: {e}")

        t_edit1 = time.perf_counter()
        timing["edit_ms"] = (t_edit1 - t_edit0) * 1000.0

        t_rule1 = time.perf_counter()
        timing["rule_ms"] = (t_rule1 - t_rule0) * 1000.0

        total_edits += edits_for_rule
        per_rule_stats.append(
            RuleRunStats(
                rule_id=rs.rule_id,
                matches=len(matches),
                edits=edits_for_rule,
                errors=r_errors,
                timing_ms=timing,
            )
        )

        file_errors.extend(r_errors)

    # Resolve overlaps
    t_res0 = time.perf_counter()
    # Count overlaps deterministically by attempting resolution under each rule’s policy is messy;
    # instead: detect overlaps by sorting raw planned edits.
    overlaps_detected = _count_overlaps(planned)

    # Apply one global policy; optionally you can do per-rule buckets.
    resolved = resolve_overlaps(planned, policy=default_overlap_policy)
    t_res1 = time.perf_counter()

    # Apply edits
    t_apply0 = time.perf_counter()
    out = apply_edits_to_text(src, resolved)
    t_apply1 = time.perf_counter()

    # Idempotence check (re-parse the output and re-run; should yield same output)
    t_idem0 = time.perf_counter()
    idem_out = _apply_again(out, language, rules, default_overlap_policy)
    idempotent = idem_out == out
    t_idem1 = time.perf_counter()

    t1 = time.perf_counter()

    file_stats = FileRunStats(
        path=path,
        language=language,
        matches=total_matches,
        edits=len(resolved),
        overlaps_detected=overlaps_detected,
        idempotent=idempotent,
        errors=file_errors,
        timing_ms={
            "parse_ms": (t_parse1 - t_parse0) * 1000.0,
            "resolve_ms": (t_res1 - t_res0) * 1000.0,
            "apply_ms": (t_apply1 - t_apply0) * 1000.0,
            "idempotence_ms": (t_idem1 - t_idem0) * 1000.0,
            "file_total_ms": (t1 - t0) * 1000.0,
        },
        per_rule=per_rule_stats,
    )

    return PlannedFileResult(
        src_before=src,
        src_after=out,
        edits=resolved,
        overlaps_detected=overlaps_detected,
        file_stats=file_stats,
    )


def _count_overlaps(edits: Sequence[PlannedEdit]) -> int:
    if not edits:
        return 0
    ordered = sorted(edits, key=lambda e: (e.start, e.end))
    overlaps = 0
    prev = ordered[0]
    for e in ordered[1:]:
        if e.start < prev.end:
            overlaps += 1
            # extend prev to the max end so multiple overlaps are counted consistently
            prev = PlannedEdit(start=prev.start, end=max(prev.end, e.end), text=prev.text, rule_id=prev.rule_id)
        else:
            prev = e
    return overlaps


def _apply_again(out: str, language: str, rules: Sequence[RuleSpec], policy: OverlapPolicy) -> str:
    # Re-run plan/apply quickly; if rules are idempotent this should be stable.
    root = SgRoot(out, language).root()
    planned: list[PlannedEdit] = []

    for rs in rules:
        if not rs.enabled or rs.language != language or rs.replacement is None:
            continue
        try:
            if _is_full_config(rs.config):
                matches = root.find_all(dict(rs.config))
            else:
                matches = root.find_all(**dict(rs.config))
        except Exception:
            continue
        for n in matches:
            try:
                rng = n.range()
                start = int(rng.start.index)
                end = int(rng.end.index)
                text = render_replacement(n, rs.replacement)
                planned.append(PlannedEdit(start=start, end=end, text=text, rule_id=rs.rule_id))
            except Exception:
                continue

    resolved = resolve_overlaps(planned, policy=policy)
    return apply_edits_to_text(out, resolved)
```

---

### `astgrep_contract/report.py`

```python
from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Iterable, List

from .types import ContractReport, FileRunStats


def build_report(files: List[FileRunStats]) -> ContractReport:
    totals = {
        "files": len(files),
        "matches": sum(f.matches for f in files),
        "edits": sum(f.edits for f in files),
        "overlaps_detected": sum(f.overlaps_detected for f in files),
        "idempotent_files": sum(1 for f in files if f.idempotent),
        "error_files": sum(1 for f in files if f.errors),
    }
    timing_ms = {
        "parse_ms": sum(f.timing_ms.get("parse_ms", 0.0) for f in files),
        "resolve_ms": sum(f.timing_ms.get("resolve_ms", 0.0) for f in files),
        "apply_ms": sum(f.timing_ms.get("apply_ms", 0.0) for f in files),
        "idempotence_ms": sum(f.timing_ms.get("idempotence_ms", 0.0) for f in files),
        "file_total_ms": sum(f.timing_ms.get("file_total_ms", 0.0) for f in files),
    }
    return ContractReport(
        tool="astgrep_contract",
        version=1,
        files=files,
        totals=totals,
        timing_ms=timing_ms,
    )


def write_report(path: str | Path, report: ContractReport) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    data = asdict(report)
    p.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
```

---

### `astgrep_contract/runner.py`

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

from .planner import plan_and_apply_file
from .rulepack import load_rule_pack
from .types import FileRunStats, OverlapPolicy, RuleSpec


@dataclass(frozen=True)
class RunResult:
    files: List[FileRunStats]


def iter_files(root: Path, *, suffixes: Sequence[str] = (".py",), max_bytes: int = 2_000_000) -> Iterable[Path]:
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix not in suffixes:
            continue
        try:
            if p.stat().st_size > max_bytes:
                continue
        except OSError:
            continue
        yield p


def run_on_paths(
    *,
    paths: Sequence[str | Path],
    rule_pack_path: str | Path,
    language: str,
    overlap_policy: OverlapPolicy = OverlapPolicy.ERROR,
    write_back: bool = False,
) -> RunResult:
    rules: Sequence[RuleSpec] = load_rule_pack(rule_pack_path)

    file_stats: list[FileRunStats] = []
    for p0 in paths:
        p = Path(p0)
        if p.is_dir():
            candidates = list(iter_files(p, suffixes=(".py",) if language == "python" else ()))
        else:
            candidates = [p]

        for f in candidates:
            src = f.read_text(encoding="utf-8")
            res = plan_and_apply_file(
                src=src,
                path=str(f),
                language=language,
                rules=rules,
                default_overlap_policy=overlap_policy,
            )
            file_stats.append(res.file_stats)

            if write_back and res.src_after != src:
                f.write_text(res.src_after, encoding="utf-8")

    return RunResult(files=file_stats)
```

---

### `astgrep_contract/__init__.py`

```python
from .rulepack import load_rule_pack, parse_rule_pack
from .types import OverlapPolicy, PlannedEdit, RuleSpec
from .planner import plan_and_apply_file
from .runner import run_on_paths
from .report import build_report, write_report

__all__ = [
    "OverlapPolicy",
    "PlannedEdit",
    "RuleSpec",
    "load_rule_pack",
    "parse_rule_pack",
    "plan_and_apply_file",
    "run_on_paths",
    "build_report",
    "write_report",
]
```

---

## 3) Pytest harness (goldens + single CI artifact)

### `tests/conftest.py`

```python
from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
from typing import List

import pytest

from astgrep_contract.report import build_report, write_report
from astgrep_contract.types import FileRunStats


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--update-goldens",
        action="store_true",
        default=False,
        help="Update golden .after snapshots in-place.",
    )


@pytest.fixture
def update_goldens(pytestconfig: pytest.Config) -> bool:
    return bool(pytestconfig.getoption("--update-goldens"))


# Session-level accumulator
_COLLECTED: List[FileRunStats] = []


@pytest.fixture
def collect_stats() -> List[FileRunStats]:
    return _COLLECTED


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if not _COLLECTED:
        return
    report = build_report(_COLLECTED)
    out = Path("tests/artifacts/astgrep_contract_report.json")
    write_report(out, report)
```

---

### `tests/test_astgrep_contract_basic.py`

```python
from __future__ import annotations

from pathlib import Path

from astgrep_contract.planner import plan_and_apply_file
from astgrep_contract.rulepack import load_rule_pack
from astgrep_contract.types import OverlapPolicy


def test_basic_golden(collect_stats, update_goldens: bool) -> None:
    rules = load_rule_pack("tests/rules/basic.yaml")

    inp = Path("tests/fixtures/sample_in.py").read_text(encoding="utf-8")
    expected_path = Path("tests/fixtures/sample_in.after.py")

    res = plan_and_apply_file(
        src=inp,
        path="tests/fixtures/sample_in.py",
        language="python",
        rules=rules,
        default_overlap_policy=OverlapPolicy.ERROR,
    )

    collect_stats.append(res.file_stats)

    if update_goldens:
        expected_path.write_text(res.src_after, encoding="utf-8")
        return

    expected = expected_path.read_text(encoding="utf-8")
    assert res.src_after == expected

    # Idempotence is enforced in file_stats; also assert hard here for clarity.
    assert res.file_stats.idempotent, f"Non-idempotent rewrite: {res.file_stats.path}"
```

---

## 4) Example rule pack + fixtures

### `tests/rules/basic.yaml`

```yaml
version: 1
defaults:
  language: python
  overlap_policy: error
  joiner: ", "

rules:
  - id: replace_print_with_logger
    enabled: true
    config:
      rule:
        pattern: "print($A)"
    replacement:
      template: "logger.log($A)"

  - id: replace_print_many_args
    enabled: true
    config:
      rule:
        pattern: "print($$$ARGS)"
    replacement:
      template: "logger.log($$$ARGS)"
      joiner: ", "
```

### `tests/fixtures/sample_in.py`

```python
def f():
    print("hello")
    print("a", 1, True)
```

### `tests/fixtures/sample_in.after.py`

```python
def f():
    logger.log("hello")
    logger.log("a", 1, True)
```

---

## 5) How you run it

### Local

```bash
pytest -q
# writes tests/artifacts/astgrep_contract_report.json
```

### Updating goldens

```bash
pytest -q --update-goldens
```

### “Codemod mode” (optional write-back)

```python
from astgrep_contract import run_on_paths, OverlapPolicy, build_report, write_report

res = run_on_paths(
    paths=["src/"],
    rule_pack_path="rules/my_pack.yaml",
    language="python",
    overlap_policy=OverlapPolicy.ERROR,
    write_back=True,
)

report = build_report(res.files)
write_report("artifacts/astgrep_contract_report.json", report)
```

---

## 6) Notes + extension points (where to plug into CodeIntel)

* **Edit planner + overlap policy**: `apply.resolve_overlaps()` is the “contract”. If you want per-rule overlap semantics, bucket by `rule_id` first, resolve per bucket, then resolve globally.
* **Transforms**:

  * If you’re using ast-grep config `transform`, prefer `$@NAME` in templates (uses `node.get_transformed(NAME)`).
  * If you want to interpret transform objects yourself (for parity or determinism), wire `transforms.apply_transform_spec()` into `templating.render_replacement()`.
* **Perf counters**: you already get per-file and per-rule `timing_ms`; add file-size bytes and match caps if you want a richer CI artifact.
* **Match-only rules**: omit `replacement` to just record match counts/errors in the JSON artifact.

---

If you want one more increment after this pack (most useful once you integrate into your repo): I can adapt the YAML schema + report format to mirror your **RunRegistry / pipeline_steps** style (file → rule → matches → edits) and add a **“stable match record” emitter** (path, start/end, kind, capture texts) that you can ingest into DuckDB as a dataset contract.


Below is the **“RunRegistry-aligned” increment** to the contract pack:

1. **YAML schema v2** with an explicit `run:` section (pipeline/step/dataset/record policy)
2. **Report JSON v2** shaped like `pipeline_runs` + `pipeline_steps` + (targets/files) + (rules)
3. A **stable match-record emitter** (JSONL) + optional **edit-records** JSONL, designed to ingest cleanly into DuckDB as dataset contracts

---

## 1) Rule pack YAML v2 (RunRegistry-shaped)

### Schema (high level)

```yaml
version: 2

run:
  pipeline: codeintel              # RunRegistry pipeline name
  step: astgrep_contract           # RunRegistry step name
  language: python                 # default language for this pack
  dataset_namespace: astgrep       # prefix for emitted datasets/artifacts
  record:                          # stable record policy
    include_node_text: excerpt     # none|excerpt|full
    excerpt_chars: 200
    normalize_whitespace: true
    capture_mode: inferred         # inferred|allowlist
    capture_allowlist: []          # used if capture_mode=allowlist
    include_transforms: true
    max_capture_chars: 500
    max_multi_capture_elems: 50

defaults:
  overlap_policy: error
  joiner: ", "

rules:
  - id: replace_print
    enabled: true
    config:
      rule: { pattern: "print($A)" }
    replacement:
      template: "logger.log($A)"
```

### Example: `tests/rules/basic_v2.yaml`

```yaml
version: 2

run:
  pipeline: codeintel
  step: astgrep_contract
  language: python
  dataset_namespace: astgrep
  record:
    include_node_text: excerpt
    excerpt_chars: 200
    normalize_whitespace: true
    capture_mode: inferred
    include_transforms: true
    max_capture_chars: 500
    max_multi_capture_elems: 50

defaults:
  overlap_policy: error
  joiner: ", "

rules:
  - id: replace_print_with_logger
    enabled: true
    config:
      rule:
        pattern: "print($A)"
    replacement:
      template: "logger.log($A)"

  - id: replace_print_many_args
    enabled: true
    config:
      rule:
        pattern: "print($$$ARGS)"
    replacement:
      template: "logger.log($$$ARGS)"
      joiner: ", "
```

---

## 2) Report JSON v2 (pipeline_runs / pipeline_steps style)

### Shape (what your CI artifact writes)

```json
{
  "run": {
    "run_id": "…",
    "pipeline": "codeintel",
    "started_at": "2026-01-01T22:10:12Z",
    "ended_at": "2026-01-01T22:10:13Z",
    "status": "ok",
    "params": { "language": "python" }
  },
  "steps": [
    {
      "step": "astgrep_contract",
      "status": "ok",
      "metrics": { "files": 1, "matches": 2, "edits": 2, "overlaps_detected": 0, "idempotent_files": 1 },
      "targets": [
        {
          "path": "tests/fixtures/sample_in.py",
          "language": "python",
          "status": "ok",
          "idempotent": true,
          "overlaps_detected": 0,
          "metrics": { "matches": 2, "edits": 2 },
          "rules": [
            { "rule_id": "replace_print_with_logger", "matches": 1, "edits": 1, "errors": [], "timing_ms": { "match_ms": 0.4, "edit_ms": 0.1 } }
          ],
          "errors": []
        }
      ],
      "timing_ms": { "parse_ms": 1.2, "resolve_ms": 0.2, "apply_ms": 0.1, "idempotence_ms": 0.8 }
    }
  ],
  "artifacts": {
    "match_records_jsonl": "tests/artifacts/astgrep_match_records.jsonl",
    "edit_records_jsonl": "tests/artifacts/astgrep_edit_records.jsonl"
  }
}
```

---

## 3) Stable match records (JSONL) + DuckDB ingestion contract

### JSONL row (stable ordering)

Each line is a record:

```json
{
  "run_id": "...",
  "pipeline": "codeintel",
  "step": "astgrep_contract",
  "file_path": "tests/fixtures/sample_in.py",
  "language": "python",
  "rule_id": "replace_print_with_logger",
  "match_ord": 0,
  "start_index": 13,
  "end_index": 24,
  "start_line": 1,
  "start_col": 4,
  "end_line": 1,
  "end_col": 15,
  "kind": "call",
  "match_text_excerpt": "print(\"hello\")",
  "match_text_sha1": "…",
  "captures": [
    {"name": "A", "text": "\"hello\"", "text_sha1": "…"}
  ],
  "multi_captures": [],
  "transforms": []
}
```

### DuckDB “dataset contract” DDL (pragmatic)

You can ingest the JSONL and then **pin a stable schema** by `CREATE TABLE AS SELECT …`:

```sql
-- raw ingest (DuckDB infers nested structures)
CREATE OR REPLACE TABLE astgrep_match_records_raw AS
SELECT * FROM read_json_auto('tests/artifacts/astgrep_match_records.jsonl', format='newline_delimited');

-- “contracted” projection (pin the columns you care about)
CREATE OR REPLACE TABLE astgrep_match_records AS
SELECT
  run_id::VARCHAR,
  pipeline::VARCHAR,
  step::VARCHAR,
  file_path::VARCHAR,
  language::VARCHAR,
  rule_id::VARCHAR,
  match_ord::INTEGER,
  start_index::INTEGER,
  end_index::INTEGER,
  start_line::INTEGER,
  start_col::INTEGER,
  end_line::INTEGER,
  end_col::INTEGER,
  kind::VARCHAR,
  match_text_excerpt::VARCHAR,
  match_text_sha1::VARCHAR,
  captures,         -- keep nested (LIST<STRUCT>) or cast to JSON if you prefer
  multi_captures,
  transforms
FROM astgrep_match_records_raw;
```

If you prefer a **fully flat relational contract**, add a derived table:

```sql
CREATE OR REPLACE TABLE astgrep_capture_records AS
SELECT
  run_id,
  pipeline,
  step,
  file_path,
  rule_id,
  match_ord,
  c.name::VARCHAR AS capture_name,
  c.text::VARCHAR AS capture_text,
  c.text_sha1::VARCHAR AS capture_text_sha1
FROM astgrep_match_records_raw,
UNNEST(captures) AS t(c);
```

---

# 4) Repo-ready code update (v2)

This is an **in-place upgrade** of the earlier pack: `load_rule_pack()` now returns a `RulePack(run, rules)` and the runner emits:

* `tests/artifacts/astgrep_contract_report.json`
* `tests/artifacts/astgrep_match_records.jsonl`
* `tests/artifacts/astgrep_edit_records.jsonl`

## 4.1 `astgrep_contract/types.py` (replace)

```python
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple


class OverlapPolicy(str, Enum):
    ERROR = "error"
    DROP_INNER = "drop_inner"
    DROP_OUTER = "drop_outer"
    PREFER_LONGEST = "prefer_longest"
    PREFER_SHORTEST = "prefer_shortest"
    ALLOW_DUPLICATE_SAME_TEXT = "allow_duplicate_same_text"


@dataclass(frozen=True)
class ReplacementSpec:
    """
    template supports:
      - $NAME          (single capture)
      - $$$NAME        (multiple capture)
      - $@NAME         (transform output, via node.get_transformed("NAME"))
    """
    template: str
    joiner: str = ", "


@dataclass(frozen=True)
class MatchRecordPolicy:
    include_node_text: str = "excerpt"  # none|excerpt|full
    excerpt_chars: int = 200
    normalize_whitespace: bool = True
    capture_mode: str = "inferred"      # inferred|allowlist
    capture_allowlist: Tuple[str, ...] = ()
    include_transforms: bool = True
    max_capture_chars: int = 500
    max_multi_capture_elems: int = 50


@dataclass(frozen=True)
class RunSpec:
    pipeline: str = "astgrep_contract"
    step: str = "astgrep_contract"
    language: str = "python"
    dataset_namespace: str = "astgrep"
    record: MatchRecordPolicy = field(default_factory=MatchRecordPolicy)


@dataclass(frozen=True)
class RuleSpec:
    rule_id: str
    language: str
    config: Mapping[str, Any]
    replacement: Optional[ReplacementSpec] = None
    overlap_policy: Optional[OverlapPolicy] = None
    max_matches: Optional[int] = None
    enabled: bool = True
    tags: Sequence[str] = field(default_factory=tuple)


@dataclass(frozen=True)
class RulePack:
    run: RunSpec
    rules: Tuple[RuleSpec, ...]


@dataclass(frozen=True)
class PlannedEdit:
    start: int
    end: int
    text: str
    rule_id: str
    kind: Optional[str] = None


@dataclass(frozen=True)
class RuleRunStats:
    rule_id: str
    matches: int
    edits: int
    errors: List[str]
    timing_ms: Dict[str, float]


@dataclass(frozen=True)
class FileRunStats:
    path: str
    language: str
    matches: int
    edits: int
    overlaps_detected: int
    idempotent: bool
    errors: List[str]
    timing_ms: Dict[str, float]
    per_rule: List[RuleRunStats]
```

---

## 4.2 `astgrep_contract/rulepack.py` (replace)

```python
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence, Tuple

from .types import (
    MatchRecordPolicy,
    OverlapPolicy,
    ReplacementSpec,
    RulePack,
    RuleSpec,
    RunSpec,
)


def _maybe_load_yaml(path: Path) -> Mapping[str, Any]:
    try:
        import yaml  # type: ignore
    except Exception as e:  # pragma: no cover
        raise RuntimeError("YAML rule packs require PyYAML. Install with: pip install pyyaml") from e
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Rule pack root must be a mapping: {path}")
    return data


def load_rule_pack(path: str | Path) -> RulePack:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(str(p))

    if p.suffix.lower() in {".yaml", ".yml"}:
        raw = _maybe_load_yaml(p)
        return parse_rule_pack(raw, default_source=str(p))
    if p.suffix.lower() == ".json":
        raw = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError(f"Rule pack JSON root must be a mapping: {p}")
        return parse_rule_pack(raw, default_source=str(p))

    raise ValueError(f"Unsupported rule pack extension: {p.suffix} (use .yaml/.yml/.json)")


def parse_rule_pack(raw: Mapping[str, Any], *, default_source: str = "<memory>") -> RulePack:
    """
    version: 2
    run: { pipeline, step, language, dataset_namespace, record: {...} }
    defaults: { overlap_policy, joiner }
    rules: [...]
    """
    version = raw.get("version", 1)
    if version != 2:
        raise ValueError(f"Unsupported rule pack version={version} in {default_source} (expected 2)")

    run_raw = raw.get("run", {}) or {}
    if not isinstance(run_raw, dict):
        raise ValueError(f"run must be a mapping in {default_source}")

    record_raw = run_raw.get("record", {}) or {}
    if not isinstance(record_raw, dict):
        raise ValueError(f"run.record must be a mapping in {default_source}")

    record = MatchRecordPolicy(
        include_node_text=str(record_raw.get("include_node_text", "excerpt")),
        excerpt_chars=int(record_raw.get("excerpt_chars", 200)),
        normalize_whitespace=bool(record_raw.get("normalize_whitespace", True)),
        capture_mode=str(record_raw.get("capture_mode", "inferred")),
        capture_allowlist=tuple(str(x) for x in record_raw.get("capture_allowlist", []) or []),
        include_transforms=bool(record_raw.get("include_transforms", True)),
        max_capture_chars=int(record_raw.get("max_capture_chars", 500)),
        max_multi_capture_elems=int(record_raw.get("max_multi_capture_elems", 50)),
    )

    run = RunSpec(
        pipeline=str(run_raw.get("pipeline", "astgrep_contract")),
        step=str(run_raw.get("step", "astgrep_contract")),
        language=str(run_raw.get("language", "python")),
        dataset_namespace=str(run_raw.get("dataset_namespace", "astgrep")),
        record=record,
    )

    defaults = raw.get("defaults", {}) or {}
    if not isinstance(defaults, dict):
        raise ValueError(f"defaults must be a mapping in {default_source}")

    default_lang = str(run.language)
    default_policy = defaults.get("overlap_policy")
    default_joiner = str(defaults.get("joiner", ", "))

    rules = raw.get("rules")
    if not isinstance(rules, list):
        raise ValueError(f"rules must be a list in {default_source}")

    out: list[RuleSpec] = []
    for i, r in enumerate(rules):
        if not isinstance(r, dict):
            raise ValueError(f"rules[{i}] must be a mapping in {default_source}")

        rule_id = r.get("id") or r.get("rule_id")
        if not isinstance(rule_id, str) or not rule_id.strip():
            raise ValueError(f"rules[{i}] missing valid id in {default_source}")

        enabled = bool(r.get("enabled", True))
        language = str(r.get("language", default_lang))

        config = r.get("config")
        if not isinstance(config, dict):
            raise ValueError(f"rules[{i}] config must be a mapping in {default_source}")

        repl = None
        repl_raw = r.get("replacement")
        if repl_raw is not None:
            if not isinstance(repl_raw, dict):
                raise ValueError(f"rules[{i}] replacement must be a mapping in {default_source}")
            template = repl_raw.get("template")
            if not isinstance(template, str):
                raise ValueError(f"rules[{i}] replacement.template must be a string in {default_source}")
            joiner = str(repl_raw.get("joiner", default_joiner))
            repl = ReplacementSpec(template=template, joiner=joiner)

        pol_raw = r.get("overlap_policy", default_policy)
        pol = OverlapPolicy(pol_raw) if pol_raw is not None else None

        max_matches = r.get("max_matches")
        if max_matches is not None and not isinstance(max_matches, int):
            raise ValueError(f"rules[{i}] max_matches must be int in {default_source}")

        tags = r.get("tags", ())
        if isinstance(tags, list):
            tags = tuple(str(t) for t in tags)
        elif isinstance(tags, tuple):
            tags = tuple(str(t) for t in tags)
        else:
            tags = (str(tags),) if tags else ()

        out.append(
            RuleSpec(
                rule_id=rule_id,
                language=language,
                config=config,
                replacement=repl,
                overlap_policy=pol,
                max_matches=max_matches,
                enabled=enabled,
                tags=tags,
            )
        )

    return RulePack(run=run, rules=tuple(out))
```

---

## 4.3 `astgrep_contract/records.py` (new)

```python
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple

from .types import MatchRecordPolicy, RuleSpec


# Pattern tokens: $A, $$A, $$$ARGS
_PATTERN_TOKEN_RE = re.compile(r"(\$\$\$[A-Z0-9_]+|\$\$[A-Z0-9_]+|\$[A-Z0-9_]+)")
# Template tokens: $A, $$$ARGS, $@NAME
_TEMPLATE_TOKEN_RE = re.compile(r"(\$\$\$[A-Z0-9_]+|\$@[A-Z0-9_]+|\$[A-Z0-9_]+)")


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _norm_ws(s: str) -> str:
    # collapse all whitespace runs to a single space; strip ends
    return " ".join(s.split())


def _excerpt(s: str, n: int) -> str:
    if len(s) <= n:
        return s
    return s[: n - 1] + "…"


def _walk_patterns(cfg: Any) -> Iterable[str]:
    if isinstance(cfg, dict):
        for k, v in cfg.items():
            if k == "pattern" and isinstance(v, str):
                yield v
            else:
                yield from _walk_patterns(v)
    elif isinstance(cfg, list):
        for x in cfg:
            yield from _walk_patterns(x)


@dataclass(frozen=True)
class CapturePlan:
    single: Tuple[str, ...]
    multi: Tuple[str, ...]
    transforms: Tuple[str, ...]


def infer_capture_plan(rule: RuleSpec) -> CapturePlan:
    singles: Set[str] = set()
    multis: Set[str] = set()
    transforms: Set[str] = set()

    # 1) from patterns
    for p in _walk_patterns(rule.config):
        for tok in _PATTERN_TOKEN_RE.findall(p):
            if tok.startswith("$$$"):
                multis.add(tok[3:])
            elif tok.startswith("$$"):
                singles.add(tok[2:])
            else:
                singles.add(tok[1:])

    # 2) from constraints (keys are capture names without $)
    cfg = rule.config
    if isinstance(cfg, dict) and isinstance(cfg.get("constraints"), dict):
        for k in cfg["constraints"].keys():
            if isinstance(k, str):
                singles.add(k)

    # 3) from transforms in config
    if isinstance(cfg, dict) and isinstance(cfg.get("transform"), dict):
        for k in cfg["transform"].keys():
            if isinstance(k, str):
                transforms.add(k)

    # 4) from replacement template tokens (if present)
    if rule.replacement is not None:
        for tok in _TEMPLATE_TOKEN_RE.findall(rule.replacement.template):
            if tok.startswith("$@"):
                transforms.add(tok[2:])
            elif tok.startswith("$$$"):
                multis.add(tok[3:])
            else:
                singles.add(tok[1:])

    return CapturePlan(
        single=tuple(sorted(singles)),
        multi=tuple(sorted(multis)),
        transforms=tuple(sorted(transforms)),
    )


def _cap_text(node: Any, name: str) -> Optional[str]:
    cap = node.get_match(name)
    if cap is None:
        return None
    return cap.text()


def _caps_text(node: Any, name: str) -> List[str]:
    caps = node.get_multiple_matches(name)
    if not caps:
        return []
    return [c.text() for c in caps]


def _transformed(node: Any, name: str) -> Optional[str]:
    v = node.get_transformed(name)
    return v


def build_match_record(
    *,
    run_id: str,
    pipeline: str,
    step: str,
    file_path: str,
    language: str,
    rule_id: str,
    match_ord: int,
    node: Any,
    capture_plan: CapturePlan,
    policy: MatchRecordPolicy,
) -> Dict[str, Any]:
    rng = node.range()
    start_i = int(rng.start.index)
    end_i = int(rng.end.index)

    start_line = int(rng.start.line)
    start_col = int(rng.start.column)
    end_line = int(rng.end.line)
    end_col = int(rng.end.column)

    kind = node.kind()

    raw_text = node.text()
    text = _norm_ws(raw_text) if policy.normalize_whitespace else raw_text
    text_sha1 = _sha1(text)

    if policy.include_node_text == "none":
        excerpt = ""
    elif policy.include_node_text == "full":
        excerpt = text
    else:
        excerpt = _excerpt(text, policy.excerpt_chars)

    captures_out: List[Dict[str, Any]] = []
    multi_out: List[Dict[str, Any]] = []
    transforms_out: List[Dict[str, Any]] = []

    # capture selection mode
    if policy.capture_mode == "allowlist":
        single_names = tuple(n for n in capture_plan.single if n in set(policy.capture_allowlist))
        multi_names = tuple(n for n in capture_plan.multi if n in set(policy.capture_allowlist))
    else:
        single_names = capture_plan.single
        multi_names = capture_plan.multi

    for name in single_names:
        v = _cap_text(node, name)
        if v is None:
            continue
        v2 = _norm_ws(v) if policy.normalize_whitespace else v
        if len(v2) > policy.max_capture_chars:
            v2 = _excerpt(v2, policy.max_capture_chars)
        captures_out.append({"name": name, "text": v2, "text_sha1": _sha1(v2)})

    for name in multi_names:
        vals = _caps_text(node, name)
        if not vals:
            continue
        if len(vals) > policy.max_multi_capture_elems:
            vals = vals[: policy.max_multi_capture_elems]
        vals2: List[str] = []
        for v in vals:
            v2 = _norm_ws(v) if policy.normalize_whitespace else v
            if len(v2) > policy.max_capture_chars:
                v2 = _excerpt(v2, policy.max_capture_chars)
            vals2.append(v2)
        multi_out.append({"name": name, "texts": vals2})

    if policy.include_transforms:
        for name in capture_plan.transforms:
            v = _transformed(node, name)
            if v is None:
                continue
            v2 = _norm_ws(v) if policy.normalize_whitespace else v
            if len(v2) > policy.max_capture_chars:
                v2 = _excerpt(v2, policy.max_capture_chars)
            transforms_out.append({"name": name, "text": v2, "text_sha1": _sha1(v2)})

    # stable ordering for nested lists
    captures_out.sort(key=lambda x: x["name"])
    multi_out.sort(key=lambda x: x["name"])
    transforms_out.sort(key=lambda x: x["name"])

    return {
        "run_id": run_id,
        "pipeline": pipeline,
        "step": step,
        "file_path": file_path,
        "language": language,
        "rule_id": rule_id,
        "match_ord": match_ord,
        "start_index": start_i,
        "end_index": end_i,
        "start_line": start_line,
        "start_col": start_col,
        "end_line": end_line,
        "end_col": end_col,
        "kind": kind,
        "match_text_excerpt": excerpt,
        "match_text_sha1": text_sha1,
        "captures": captures_out,
        "multi_captures": multi_out,
        "transforms": transforms_out,
    }


def stable_sort_nodes(nodes: Sequence[Any]) -> List[Any]:
    # enforce deterministic order even if backend returns in different order
    keyed = []
    for n in nodes:
        r = n.range()
        keyed.append((int(r.start.index), int(r.end.index), n.kind(), _sha1(n.text()), n))
    keyed.sort(key=lambda t: (t[0], t[1], t[2], t[3]))
    return [t[4] for t in keyed]
```

---

## 4.4 `astgrep_contract/planner.py` (replace)

```python
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ast_grep_py import SgRoot  # type: ignore

from .apply import apply_edits_to_text, resolve_overlaps
from .records import build_match_record, infer_capture_plan, stable_sort_nodes
from .templating import render_replacement
from .types import (
    FileRunStats,
    MatchRecordPolicy,
    OverlapPolicy,
    PlannedEdit,
    RulePack,
    RuleRunStats,
    RuleSpec,
)


def _is_full_config(cfg: Mapping[str, Any]) -> bool:
    return any(k in cfg for k in ("rule", "utils", "constraints", "transform"))


@dataclass(frozen=True)
class PlannedFileResult:
    src_before: str
    src_after: str
    edits: List[PlannedEdit]
    overlaps_detected: int
    file_stats: FileRunStats
    match_records: List[Dict[str, Any]]
    edit_records: List[Dict[str, Any]]


def plan_and_apply_file(
    *,
    run_id: str,
    pack: RulePack,
    src: str,
    path: str,
    language: str,
    default_overlap_policy: OverlapPolicy = OverlapPolicy.ERROR,
) -> PlannedFileResult:
    """
    Parse once, run rules, emit stable match records, build edits, resolve overlaps, apply.
    Also computes idempotence.
    """
    file_errors: list[str] = []
    t0 = time.perf_counter()

    # Parse
    t_parse0 = time.perf_counter()
    root = SgRoot(src, language).root()
    t_parse1 = time.perf_counter()

    planned: list[PlannedEdit] = []
    per_rule_stats: list[RuleRunStats] = []
    match_records: list[Dict[str, Any]] = []

    total_matches = 0
    total_edits = 0

    for rs in pack.rules:
        if not rs.enabled or rs.language != language:
            continue

        r_errors: list[str] = []
        timing: Dict[str, float] = {}

        t_rule0 = time.perf_counter()
        matches: list[Any] = []

        try:
            t_match0 = time.perf_counter()
            if _is_full_config(rs.config):
                matches = root.find_all(dict(rs.config))
            else:
                matches = root.find_all(**dict(rs.config))
            matches = stable_sort_nodes(matches)
            t_match1 = time.perf_counter()
            timing["match_ms"] = (t_match1 - t_match0) * 1000.0
        except Exception as e:
            r_errors.append(f"match_error: {type(e).__name__}: {e}")
            matches = []

        if rs.max_matches is not None and len(matches) > rs.max_matches:
            r_errors.append(f"max_matches_exceeded: got={len(matches)} cap={rs.max_matches}")
            matches = matches[: rs.max_matches]

        total_matches += len(matches)

        # Emit stable match records (even for match-only rules)
        cap_plan = infer_capture_plan(rs)
        for i, n in enumerate(matches):
            try:
                match_records.append(
                    build_match_record(
                        run_id=run_id,
                        pipeline=pack.run.pipeline,
                        step=pack.run.step,
                        file_path=path,
                        language=language,
                        rule_id=rs.rule_id,
                        match_ord=i,
                        node=n,
                        capture_plan=cap_plan,
                        policy=pack.run.record,
                    )
                )
            except Exception as e:
                r_errors.append(f"record_error: {type(e).__name__}: {e}")

        # Build edits if replacement is present
        t_edit0 = time.perf_counter()
        edits_for_rule = 0
        if rs.replacement is not None and matches:
            for n in matches:
                try:
                    rng = n.range()
                    start = int(rng.start.index)
                    end = int(rng.end.index)
                    text = render_replacement(n, rs.replacement)
                    planned.append(
                        PlannedEdit(
                            start=start,
                            end=end,
                            text=text,
                            rule_id=rs.rule_id,
                            kind=n.kind(),
                        )
                    )
                    edits_for_rule += 1
                except Exception as e:
                    r_errors.append(f"edit_error: {type(e).__name__}: {e}")

        t_edit1 = time.perf_counter()
        timing["edit_ms"] = (t_edit1 - t_edit0) * 1000.0

        t_rule1 = time.perf_counter()
        timing["rule_ms"] = (t_rule1 - t_rule0) * 1000.0

        total_edits += edits_for_rule
        per_rule_stats.append(
            RuleRunStats(
                rule_id=rs.rule_id,
                matches=len(matches),
                edits=edits_for_rule,
                errors=r_errors,
                timing_ms=timing,
            )
        )

        file_errors.extend(r_errors)

    overlaps_detected = _count_overlaps(planned)

    # Resolve overlaps (global policy)
    t_res0 = time.perf_counter()
    resolved = resolve_overlaps(planned, policy=default_overlap_policy)
    t_res1 = time.perf_counter()

    # Apply edits
    t_apply0 = time.perf_counter()
    out = apply_edits_to_text(src, resolved)
    t_apply1 = time.perf_counter()

    # Edit records (stable)
    edit_records = _build_edit_records(
        run_id=run_id,
        pack=pack,
        file_path=path,
        language=language,
        edits=resolved,
    )

    # Idempotence
    t_idem0 = time.perf_counter()
    idem_out = _apply_again(run_id, pack, out, path, language, default_overlap_policy)
    idempotent = idem_out == out
    t_idem1 = time.perf_counter()

    t1 = time.perf_counter()

    file_stats = FileRunStats(
        path=path,
        language=language,
        matches=total_matches,
        edits=len(resolved),
        overlaps_detected=overlaps_detected,
        idempotent=idempotent,
        errors=file_errors,
        timing_ms={
            "parse_ms": (t_parse1 - t_parse0) * 1000.0,
            "resolve_ms": (t_res1 - t_res0) * 1000.0,
            "apply_ms": (t_apply1 - t_apply0) * 1000.0,
            "idempotence_ms": (t_idem1 - t_idem0) * 1000.0,
            "file_total_ms": (t1 - t0) * 1000.0,
        },
        per_rule=per_rule_stats,
    )

    return PlannedFileResult(
        src_before=src,
        src_after=out,
        edits=resolved,
        overlaps_detected=overlaps_detected,
        file_stats=file_stats,
        match_records=match_records,
        edit_records=edit_records,
    )


def _count_overlaps(edits: Sequence[PlannedEdit]) -> int:
    if not edits:
        return 0
    ordered = sorted(edits, key=lambda e: (e.start, e.end, e.rule_id))
    overlaps = 0
    prev_start, prev_end = ordered[0].start, ordered[0].end
    for e in ordered[1:]:
        if e.start < prev_end:
            overlaps += 1
            prev_end = max(prev_end, e.end)
        else:
            prev_start, prev_end = e.start, e.end
    return overlaps


def _apply_again(run_id: str, pack: RulePack, out: str, path: str, language: str, policy: OverlapPolicy) -> str:
    # quick idempotence check: re-run and apply
    root = SgRoot(out, language).root()
    planned: list[PlannedEdit] = []

    for rs in pack.rules:
        if not rs.enabled or rs.language != language or rs.replacement is None:
            continue
        try:
            if _is_full_config(rs.config):
                matches = root.find_all(dict(rs.config))
            else:
                matches = root.find_all(**dict(rs.config))
            matches = stable_sort_nodes(matches)
        except Exception:
            continue

        for n in matches:
            try:
                rng = n.range()
                start = int(rng.start.index)
                end = int(rng.end.index)
                text = render_replacement(n, rs.replacement)
                planned.append(PlannedEdit(start=start, end=end, text=text, rule_id=rs.rule_id, kind=n.kind()))
            except Exception:
                continue

    resolved = resolve_overlaps(planned, policy=policy)
    return apply_edits_to_text(out, resolved)


def _build_edit_records(*, run_id: str, pack: RulePack, file_path: str, language: str, edits: Sequence[PlannedEdit]) -> List[Dict[str, Any]]:
    import hashlib

    def sha1(s: str) -> str:
        return hashlib.sha1(s.encode("utf-8")).hexdigest()

    out: List[Dict[str, Any]] = []
    # stable order
    ordered = sorted(edits, key=lambda e: (e.start, e.end, e.rule_id, sha1(e.text)))
    for i, e in enumerate(ordered):
        txt = e.text
        if pack.run.record.normalize_whitespace:
            txt = " ".join(txt.split())
        excerpt = txt if len(txt) <= pack.run.record.excerpt_chars else txt[: pack.run.record.excerpt_chars - 1] + "…"
        out.append(
            {
                "run_id": run_id,
                "pipeline": pack.run.pipeline,
                "step": pack.run.step,
                "file_path": file_path,
                "language": language,
                "rule_id": e.rule_id,
                "edit_ord": i,
                "start_index": e.start,
                "end_index": e.end,
                "kind": e.kind,
                "inserted_text_excerpt": excerpt,
                "inserted_text_sha1": sha1(txt),
            }
        )
    return out
```

---

## 4.5 `astgrep_contract/report.py` (replace)

```python
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

from .types import FileRunStats, RulePack


def build_runregistry_report(
    *,
    run_id: str,
    pack: RulePack,
    started_at_iso: str,
    ended_at_iso: str,
    files: List[FileRunStats],
    artifacts: Mapping[str, str],
) -> Dict[str, Any]:
    # step-level totals
    matches = sum(f.matches for f in files)
    edits = sum(f.edits for f in files)
    overlaps = sum(f.overlaps_detected for f in files)
    idempotent_files = sum(1 for f in files if f.idempotent)
    error_files = sum(1 for f in files if f.errors)

    step_status = "ok" if error_files == 0 else "error"
    run_status = step_status

    step_targets: List[Dict[str, Any]] = []
    for f in files:
        target_status = "ok" if not f.errors else "error"
        step_targets.append(
            {
                "path": f.path,
                "language": f.language,
                "status": target_status,
                "idempotent": f.idempotent,
                "overlaps_detected": f.overlaps_detected,
                "metrics": {"matches": f.matches, "edits": f.edits},
                "rules": [
                    {
                        "rule_id": r.rule_id,
                        "matches": r.matches,
                        "edits": r.edits,
                        "errors": r.errors,
                        "timing_ms": r.timing_ms,
                    }
                    for r in f.per_rule
                ],
                "errors": f.errors,
            }
        )

    timing = {
        "parse_ms": sum(f.timing_ms.get("parse_ms", 0.0) for f in files),
        "resolve_ms": sum(f.timing_ms.get("resolve_ms", 0.0) for f in files),
        "apply_ms": sum(f.timing_ms.get("apply_ms", 0.0) for f in files),
        "idempotence_ms": sum(f.timing_ms.get("idempotence_ms", 0.0) for f in files),
        "file_total_ms": sum(f.timing_ms.get("file_total_ms", 0.0) for f in files),
    }

    return {
        "run": {
            "run_id": run_id,
            "pipeline": pack.run.pipeline,
            "started_at": started_at_iso,
            "ended_at": ended_at_iso,
            "status": run_status,
            "params": {"language": pack.run.language},
        },
        "steps": [
            {
                "step": pack.run.step,
                "status": step_status,
                "metrics": {
                    "files": len(files),
                    "matches": matches,
                    "edits": edits,
                    "overlaps_detected": overlaps,
                    "idempotent_files": idempotent_files,
                    "error_files": error_files,
                },
                "targets": step_targets,
                "timing_ms": timing,
            }
        ],
        "artifacts": dict(artifacts),
    }


def write_json(path: str | Path, obj: Mapping[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")
```

---

## 4.6 `astgrep_contract/runner.py` (replace)

```python
from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from .planner import plan_and_apply_file
from .rulepack import load_rule_pack
from .report import build_runregistry_report, write_json
from .types import FileRunStats, OverlapPolicy, RulePack


@dataclass(frozen=True)
class RunResult:
    run_id: str
    pack: RulePack
    files: List[FileRunStats]
    match_records: List[dict]
    edit_records: List[dict]
    report: dict


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def iter_files(root: Path, *, suffixes: Sequence[str], max_bytes: int = 2_000_000) -> Iterable[Path]:
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix not in suffixes:
            continue
        try:
            if p.stat().st_size > max_bytes:
                continue
        except OSError:
            continue
        yield p


def _suffixes_for_language(language: str) -> Tuple[str, ...]:
    # extend as needed
    if language == "python":
        return (".py",)
    if language in ("javascript", "typescript"):
        return (".js", ".ts", ".tsx")
    return ()


def run_on_paths(
    *,
    paths: Sequence[str | Path],
    rule_pack_path: str | Path,
    overlap_policy: OverlapPolicy = OverlapPolicy.ERROR,
    write_back: bool = False,
    artifacts_dir: str | Path = "tests/artifacts",
) -> RunResult:
    pack = load_rule_pack(rule_pack_path)

    run_id = os.environ.get("CODEINTEL_RUN_ID") or str(uuid.uuid4())

    started = _utc_iso_now()

    files: list[FileRunStats] = []
    match_records: list[dict] = []
    edit_records: list[dict] = []

    suffixes = _suffixes_for_language(pack.run.language)
    if not suffixes:
        raise ValueError(f"No suffix mapping for language={pack.run.language!r}")

    for p0 in paths:
        p = Path(p0)
        candidates: List[Path]
        if p.is_dir():
            candidates = list(iter_files(p, suffixes=suffixes))
        else:
            candidates = [p]

        for f in candidates:
            src = f.read_text(encoding="utf-8")
            res = plan_and_apply_file(
                run_id=run_id,
                pack=pack,
                src=src,
                path=str(f),
                language=pack.run.language,
                default_overlap_policy=overlap_policy,
            )
            files.append(res.file_stats)
            match_records.extend(res.match_records)
            edit_records.extend(res.edit_records)

            if write_back and res.src_after != src:
                f.write_text(res.src_after, encoding="utf-8")

    ended = _utc_iso_now()

    artifacts_dir = Path(artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    match_path = artifacts_dir / f"{pack.run.dataset_namespace}_match_records.jsonl"
    edit_path = artifacts_dir / f"{pack.run.dataset_namespace}_edit_records.jsonl"
    report_path = artifacts_dir / f"{pack.run.dataset_namespace}_contract_report.json"

    # stable write: sort match records deterministically
    match_records_sorted = sorted(
        match_records,
        key=lambda r: (
            r["file_path"],
            r["rule_id"],
            r["match_ord"],
            r["start_index"],
            r["end_index"],
            r.get("kind", ""),
            r.get("match_text_sha1", ""),
        ),
    )
    edit_records_sorted = sorted(
        edit_records,
        key=lambda r: (
            r["file_path"],
            r["rule_id"],
            r["edit_ord"],
            r["start_index"],
            r["end_index"],
            r.get("inserted_text_sha1", ""),
        ),
    )

    match_path.write_text("\n".join([json_dumps_line(x) for x in match_records_sorted]) + ("\n" if match_records_sorted else ""), encoding="utf-8")
    edit_path.write_text("\n".join([json_dumps_line(x) for x in edit_records_sorted]) + ("\n" if edit_records_sorted else ""), encoding="utf-8")

    report = build_runregistry_report(
        run_id=run_id,
        pack=pack,
        started_at_iso=started,
        ended_at_iso=ended,
        files=files,
        artifacts={
            "match_records_jsonl": str(match_path),
            "edit_records_jsonl": str(edit_path),
            "report_json": str(report_path),
        },
    )
    write_json(report_path, report)

    return RunResult(
        run_id=run_id,
        pack=pack,
        files=files,
        match_records=match_records_sorted,
        edit_records=edit_records_sorted,
        report=report,
    )


def json_dumps_line(obj: dict) -> str:
    import json
    return json.dumps(obj, separators=(",", ":"), sort_keys=True)
```

---

## 4.7 Pytest harness update (writes the same artifacts)

### `tests/conftest.py` (replace)

```python
from __future__ import annotations

from pathlib import Path
from typing import List

import pytest

from astgrep_contract.report import build_runregistry_report, write_json
from astgrep_contract.types import FileRunStats, RulePack


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption("--update-goldens", action="store_true", default=False)


@pytest.fixture
def update_goldens(pytestconfig: pytest.Config) -> bool:
    return bool(pytestconfig.getoption("--update-goldens"))


_COLLECTED_FILES: List[FileRunStats] = []
_COLLECTED_MATCH: List[dict] = []
_COLLECTED_EDITS: List[dict] = []
_PACK: RulePack | None = None
_RUN_ID: str = "pytest-run"


@pytest.fixture
def collect_files() -> List[FileRunStats]:
    return _COLLECTED_FILES


@pytest.fixture
def collect_match_records() -> List[dict]:
    return _COLLECTED_MATCH


@pytest.fixture
def collect_edit_records() -> List[dict]:
    return _COLLECTED_EDITS


@pytest.fixture
def set_pack() -> callable:
    def _set(p: RulePack) -> None:
        global _PACK
        _PACK = p
    return _set


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if _PACK is None or not _COLLECTED_FILES:
        return

    artifacts_dir = Path("tests/artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    match_path = artifacts_dir / f"{_PACK.run.dataset_namespace}_match_records.jsonl"
    edit_path = artifacts_dir / f"{_PACK.run.dataset_namespace}_edit_records.jsonl"
    report_path = artifacts_dir / f"{_PACK.run.dataset_namespace}_contract_report.json"

    # stable
    match_sorted = sorted(_COLLECTED_MATCH, key=lambda r: (r["file_path"], r["rule_id"], r["match_ord"], r["start_index"], r["end_index"], r.get("match_text_sha1", "")))
    edit_sorted = sorted(_COLLECTED_EDITS, key=lambda r: (r["file_path"], r["rule_id"], r["edit_ord"], r["start_index"], r["end_index"], r.get("inserted_text_sha1", "")))

    import json
    match_path.write_text("\n".join(json.dumps(x, separators=(",", ":"), sort_keys=True) for x in match_sorted) + ("\n" if match_sorted else ""), encoding="utf-8")
    edit_path.write_text("\n".join(json.dumps(x, separators=(",", ":"), sort_keys=True) for x in edit_sorted) + ("\n" if edit_sorted else ""), encoding="utf-8")

    report = build_runregistry_report(
        run_id=_RUN_ID,
        pack=_PACK,
        started_at_iso="pytest",
        ended_at_iso="pytest",
        files=_COLLECTED_FILES,
        artifacts={
            "match_records_jsonl": str(match_path),
            "edit_records_jsonl": str(edit_path),
            "report_json": str(report_path),
        },
    )
    write_json(report_path, report)
```

### `tests/test_astgrep_contract_basic.py` (replace)

```python
from __future__ import annotations

from pathlib import Path

from astgrep_contract.planner import plan_and_apply_file
from astgrep_contract.rulepack import load_rule_pack
from astgrep_contract.types import OverlapPolicy


def test_basic_golden(collect_files, collect_match_records, collect_edit_records, set_pack, update_goldens: bool) -> None:
    pack = load_rule_pack("tests/rules/basic_v2.yaml")
    set_pack(pack)

    inp = Path("tests/fixtures/sample_in.py").read_text(encoding="utf-8")
    expected_path = Path("tests/fixtures/sample_in.after.py")

    res = plan_and_apply_file(
        run_id="pytest-run",
        pack=pack,
        src=inp,
        path="tests/fixtures/sample_in.py",
        language=pack.run.language,
        default_overlap_policy=OverlapPolicy.ERROR,
    )

    collect_files.append(res.file_stats)
    collect_match_records.extend(res.match_records)
    collect_edit_records.extend(res.edit_records)

    if update_goldens:
        expected_path.write_text(res.src_after, encoding="utf-8")
        return

    expected = expected_path.read_text(encoding="utf-8")
    assert res.src_after == expected
    assert res.file_stats.idempotent
```

---

## What you get after this increment

* **RunRegistry-shaped JSON report**: `tests/artifacts/astgrep_contract_contract_report.json` (name depends on `dataset_namespace`)
* **Stable match dataset**: `tests/artifacts/astgrep_match_records.jsonl`
* **Stable edit dataset**: `tests/artifacts/astgrep_edit_records.jsonl`
* Clear DuckDB ingestion path + easy normalization into contract tables

If you want the next increment after this (the most CodeIntel-native): I can provide a ready-to-drop **DuckDB ingestion node** (SQL + Python) that materializes `astgrep_match_records` + `astgrep_capture_records` with primary keys, and emits a `pipeline_steps.artifact_uri` pointing at the report + JSONL paths (so your RunRegistry can track “contract drift” across upgrades).
s