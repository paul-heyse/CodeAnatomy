Below is a deep, **Python‑centric** overview of Tree‑sitter and its Python bindings (`tree‑sitter` / “py‑tree‑sitter”), with an emphasis on the features you can compose for advanced program analysis, refactoring, and multi‑language tooling.

---

## 0) What Tree‑sitter is (and why it’s useful)

Tree‑sitter is a **parser generator** and **incremental parsing** library. It builds concrete syntax trees (CSTs) and can *efficiently update only the affected parts* of the tree as the source changes—ideal for interactive tools, code intelligence, and large‑scale analysis. ([Tree-sitter][1])

The Python package `tree‑sitter` exposes this engine and its concepts (Parser/Tree/Node/Language/Query/QueryCursor, etc.) in a modern, typed API. The docs you see referenced below correspond to **py‑tree‑sitter 0.25.x**. ([Tree-sitter][2])

---

## 1) Installing and loading languages

**Install the core bindings:**

```bash
pip install tree-sitter
```

This provides prebuilt wheels on major platforms. ([PyPI][3])

**Get a language grammar:**

* *Per‑language wheel* (ex: Python): `pip install tree-sitter-python` (module name `tree_sitter_python`). ([PyPI][4])
* *Many languages at once:* third‑party convenience packs:

  * `tree-sitter-languages` – exposes `get_language('python')` / `get_parser('python')`. ([PyPI][5])
  * `tree-sitter-language-pack` – similar API (`get_binding`, `get_language`, `get_parser`). ([PyPI][6])

**Constructing a `Language` and `Parser` (0.25+):**

```python
from tree_sitter import Language, Parser
import tree_sitter_python as tspython  # from tree-sitter-python

PY_LANG = Language(tspython.language())  # 0.25+: Language() accepts a c-ext capsule
parser = Parser(PY_LANG)                 # 0.25+: language passed to constructor
```

In **0.25**, `Language()` accepts a *capsule* for the native binding, and `Parser` takes the `Language` in its constructor (older APIs like `Parser.set_language`, `Language.build_library`, `Language.load` are removed). ([GitHub][7])

---

## 2) Core object model (Python)

### `Parser`

* `Parser(language, *, included_ranges=None, timeout_micros=None)` constructs a parser. Key method:

  * `parse(source_or_callback, /, old_tree=None, encoding='utf8', progress_callback=None)`
    *Pass a `bytes` object* (or a callback that supplies slices); optionally pass the previous `Tree` via `old_tree` for **incremental** parsing; `encoding` may be `"utf8"`, `"utf16"`, `"utf16le"`, or `"utf16be"`. A `progress_callback` lets you cancel long parses. ([Tree-sitter][8])
* Attributes:

  * `included_ranges` (use with `Range` for partial parsing), `language`, `logger`. The legacy `timeout_micros` is deprecated in favor of the `progress_callback`. ([Tree-sitter][8])

### `Tree`

Represents the CST for a document.

* `edit(...)` updates the tree’s internal offsets in response to a text edit you describe in **bytes** and **row/column** form. ([Tree-sitter][9])
* `changed_ranges(new_tree)` returns **minimal regions** whose ancestor paths changed—perfect for editor diffs, invalidation, or targeted re‑analysis. ([Tree-sitter][9])
* `root_node`, `language`, `included_ranges`, and debugging via `print_dot_graph(file)`. Also: `root_node_with_offset()` for virtualized offsets (e.g., embedded fragments). ([Tree-sitter][9])

### `Node`

A node in the CST; cheap to copy, points into the shared tree.

* Navigation, selection, and field access:

  * `child(i)`, `named_child(i)`, `children`, `named_children`
  * `child_by_field_name(name)`, `children_by_field_name(name)`
  * Spatial queries: `descendant_for_byte_range`, `descendant_for_point_range`, and *byte‑based accelerators* (`first_child_for_byte`, `first_named_child_for_byte`). ([Tree-sitter][10])
* Semantics / diagnostics:

  * `type`, `kind_id`, `grammar_name`, `grammar_id`
  * `is_named`, `is_missing` (parser recovery inserted tokens), `is_error`, `has_error`, `has_changes` ([Tree-sitter][10])
* Source ranges: `start_byte/end_byte` *and* `start_point/end_point`; `range` and `byte_range`. For convenience, `text` returns the node’s bytes **if the tree has not been edited**. ([Tree-sitter][10])
* Parse‑engine hooks: `parse_state`, `next_parse_state` — pair with `Language.next_state` and `Language.lookahead_iterator` for completion / prediction features. ([Tree-sitter][10])

### `TreeCursor`

Efficient **cursor‑style** traversal without allocating Python lists. Methods like `goto_first_child`, `goto_last_child`, `goto_first_child_for_byte`, `goto_next_sibling`, etc. Use it for deep walks and streaming scans. ([Tree-sitter][11])

### `Language`

Introspection and advanced engine helpers:

* Node/field IDs and properties: `id_for_node_kind`, `node_kind_for_id`, `node_kind_is_named`, `field_id_for_name`, `field_name_for_id`, `supertypes()` (for supertype groups like `expression`). ([Tree-sitter][12])
* Predictive APIs:

  * `next_state(state, id)` and `lookahead_iterator(state)`—combine with `Node.parse_state` to enumerate legal tokens/symbols from a position (useful for code completion and robust recovery UX). ([Tree-sitter][12])
* Version/ABI: `LANGUAGE_VERSION` and `MIN_COMPATIBLE_LANGUAGE_VERSION` are exposed at the package root so you can check parser/grammar compatibility. ([Tree-sitter][2])

### `Query` + `QueryCursor`

The **query DSL** is an S‑expression language that matches tree shapes with named **captures** (e.g., `@function.name`). Create a `Query(language, source)`; then execute it with a `QueryCursor`. ([Tree-sitter][13])

* **Predicates**: built‑ins include `#eq?`, `#match?`, `#any-of?`, `#is?`, and `#set!`—use them to filter by text, set properties, and annotate results. ([Tree-sitter][13])
* **Execution**:

  * `cursor = QueryCursor(query, match_limit=..., timeout_micros=...)`
  * `cursor.set_byte_range(start, end)` / `set_point_range(start_point, end_point)` for scoped queries
  * `cursor.matches(node, predicate=None, progress_callback=None)` – returns (pattern_index, {capture_name: node}) tuples
  * `cursor.captures(node, predicate=None, progress_callback=None)` – returns `{capture_name: [nodes...]}` dict
    Also track `cursor.did_exceed_match_limit`. ([Tree-sitter][14])
* **Patterns & performance**:

  * **Non‑local** patterns (multiple roots/repeat contexts) disable some range optimizations—use sparingly for wide matches. ([Tree-sitter][13])
  * Strings in queries must use **double quotes** (applies across bindings). ([Davis Vaughan][15])

### Utility types

* `Point(row, column)` – 0‑based logical coordinates. ([Tree-sitter][16])
* `Range(start_point, end_point, start_byte, end_byte)` – used for `included_ranges`, etc. ([Tree-sitter][17])

---

## 3) The modern 0.25+ Python API at a glance

Significant changes shipped in **0.25** (current docs as of this writing):

* `Parser` now takes `Language` in the constructor; `Parser.set_language` *removed*.
* `Language.build_library` and `Language.load` *removed* (prefer prebuilt grammar wheels or third‑party packs).
* `QueryCursor` is promoted—timeouts, match limits, scoping, and the `captures/matches` runners live here (moved from `Query`).
* `parse()` and `QueryCursor` gained `progress_callback` for cooperative cancellation; `timeout_micros` moved/deprecated accordingly.
* `Language()` accepts a **capsule** (e.g., from `tree_sitter_python.language()`), simplifying grammar loading. ([GitHub][7])

---

## 4) End‑to‑end, **incremental** parsing loop (Python)

```python
from tree_sitter import Parser, Language, Point
import tree_sitter_python as tspython

lang = Language(tspython.language())
parser = Parser(lang)

source = b"def f(x):\n    return x + 1\n"
tree = parser.parse(source)                       # initial parse

# ... user inserts "2" after '+' on line 2, column 15 (0-based rows/cols)
edited = b"def f(x):\n    return x + 12\n"

# describe the edit both in bytes and in points (row/column)
start_byte = source.index(b"+ 1") + 2
old_end_byte = start_byte + 1
new_end_byte = start_byte + 2
start_point = Point(1, 18)
old_end_point = Point(1, 19)
new_end_point = Point(1, 20)

# update the old tree in-place to reflect the text change
tree.edit(start_byte, old_end_byte, new_end_byte,
          start_point, old_end_point, new_end_point)

# reparse incrementally by passing old_tree
new_tree = parser.parse(edited, old_tree=tree)

# find the smallest set of syntactic regions whose ancestor paths changed
for r in tree.changed_ranges(new_tree):
    print(r.start_point, r.end_point)
```

Key ideas: call `Tree.edit(...)` first (to shift offsets), then `Parser.parse(..., old_tree=tree)`; finally, compute changed regions via `changed_ranges`. ([Tree-sitter][9])

---

## 5) Included ranges (parse only the parts you care about)

If you have a document with large generated sections, hidden regions, or fenced code blocks that you want treated as a separate parse unit, set `parser.included_ranges` (list of `Range` objects) before parsing. Only those ranges will be considered, improving performance and supporting “projection” views of a file. ([Tree-sitter][8])

For truly **embedded languages** (e.g., JS inside HTML), pair parsing with **injection queries**—capture the injection content/language using `@injection.content`/`@injection.language` and properties like `injection.language`, `injection.combined`, `injection.include-children`. This yields nested trees with their own languages. ([Tree-sitter][18])

---

## 6) Querying—fast structural search with predicates

Write S‑expression patterns that mirror grammar structure; annotate sub‑nodes with capture names (e.g., `@def.name`). Example:

```scheme
; functions and their names (Python)
(function_definition
  name: (identifier) @func.name)
```

Then execute:

```python
from tree_sitter import Query, QueryCursor

q = Query(lang, '(function_definition name: (identifier) @func.name)')
cursor = QueryCursor(q)
caps = cursor.captures(new_tree.root_node)
print(list(caps['func.name']))
```

Use `cursor.set_byte_range`/`set_point_range` to limit scope, `match_limit` to keep memory bounded, and `progress_callback` to cancel for large files or complex queries. ([Tree-sitter][14])

**Predicates** refine matches by text or properties (e.g., filter function names with a regex using `#match?`, or set per‑match metadata via `#set!`). Remember: strings use **double quotes** in query text. ([Tree-sitter][13])

**Note on non‑local patterns:** patterns with multiple roots (spanning repeating contexts) disable certain range optimizations; prefer rooted/local patterns when you need windowed execution. ([Tree-sitter][13])

---

## 7) Performance, scale, and cancellation

* **Incrementality:** Always reuse the previous `Tree` in `parse(..., old_tree=...)`. Then use `changed_ranges` to decide what to re‑lint, re‑highlight, or re‑index. ([Tree-sitter][9])
* **Scoped queries:** Use `QueryCursor.set_byte_range`/`set_point_range` to execute inside just the changed regions. ([Tree-sitter][14])
* **Match throttling:** `QueryCursor.match_limit` and `did_exceed_match_limit` protect you from pathological patterns. ([Tree-sitter][14])
* **Cancellation:** Prefer `progress_callback` (on `Parser.parse`, `QueryCursor.captures/matches`) over the older `timeout_micros`. ([Tree-sitter][8])
* **Streaming input:** `parse` can accept a *callback* that returns slices on demand—useful for very large buffers. ([Tree-sitter][8])

---

## 8) Advanced capabilities for sophisticated tooling

* **Completions & syntactic expectations:** With `node.parse_state` and `language.next_state(...)` or `language.lookahead_iterator(...)`, you can enumerate what tokens or nonterminals are valid next—foundational for structured code completion even in the presence of errors. ([Tree-sitter][12])
* **Diagnostics & recovery:** Detect errors via `(ERROR)` and `(MISSING ...)` queries or `Node.is_error/has_error/is_missing`. The query syntax includes explicit support for `(ERROR)`/`(MISSING)` nodes so you can surface precise diagnostics even when parsing recovers. ([Tree-sitter][19])
* **Supertypes:** If the grammar defines supertypes (e.g., `expression`), you can query at that abstraction level and refine with `supertype/subtype` syntax. Combine with `Language.supertypes()` to discover the lattice. ([Tree-sitter][19])
* **Graph/debug output:** `Parser.print_dot_graphs(file)` logs the parser’s LR steps; `Tree.print_dot_graph(file)` dumps the resulting tree for visualization/debugging. ([Tree-sitter][8])
* **Encoding control:** `parse(..., encoding=...)` supports UTF‑8 and UTF‑16 variants; choose to match your in‑memory representation and keep `byte` vs `point` math straight. ([Tree-sitter][8])

---

## 9) Python grammar & ecosystem notes

* The official Python grammar is `tree-sitter-python` (grammar repo / wheel). Use it directly (`import tree_sitter_python as tspython`) or through a language bundle. ([GitHub][20])
* Multi‑language packs such as `tree-sitter-languages` and `tree-sitter-language-pack` remove the need to build grammars yourself and expose helpers like `get_language()`/`get_parser()`. (They are third‑party but widely used.) ([PyPI][5])

---

## 10) Putting it together — robust design patterns

**A. Live analyzer / linter loop**

1. Maintain `bytes` for each open file and a cached `Tree`.
2. On edit: compute byte + point deltas; call `tree.edit(...)`; re‑parse with `old_tree=tree`; compute `changed_ranges`.
3. For each range: run targeted queries (`set_point_range`) for rules that could be affected. Emit diagnostics only for those spans. ([Tree-sitter][9])

**B. Multi‑language documents**
Use **injection queries** (e.g., HTML with JS and CSS) to split embedded regions into child parses with their own `Language`. Use `injection.language`, `@injection.content`, and `injection.include-children` to fine‑tune. For strict host‑language‑only analysis, you can alternatively parse with `included_ranges` to filter. ([Tree-sitter][18])

**C. Structural search and refactoring**
Use `QueryCursor.matches` to bind related captures in one pass (e.g., a `function_definition` and all of its parameter identifiers). Keep `match_limit` under control; employ `progress_callback` for cancelability; and confine execution to changed regions. ([Tree-sitter][14])

**D. Completion & smart insertions**
At a cursor position, take the nearest node and its `parse_state`, feed `Language.lookahead_iterator(state)` to enumerate viable tokens—then blend with symbol tables (built from queries over `locals.scm` patterns) for context‑aware completions. ([Tree-sitter][12])

---

## 11) Query DSL essentials (cheat sheet)

* **Node match:** `(function_definition)`; anonymous literals use quotes: `"return"`. ([Tree-sitter][19])
* **Fields:** `name: (identifier) @id` targets a specific child. ([Tree-sitter][19])
* **Negated fields:** `!type_parameters` to require absence of a field. ([Tree-sitter][19])
* **Wildcards:** `_` (any), `(_)` (any **named** node). ([Tree-sitter][19])
* **ERROR/MISSING:** `(ERROR) @error`, `(MISSING identifier) @missing`. ([Tree-sitter][19])
* **Predicates:** `#eq?`, `#match?`, `#any-of?`, `#is?`, `#set!` (double‑quoted strings). ([Tree-sitter][13])
* **Non‑local:** multiple roots in one pattern—use judiciously. ([Tree-sitter][13])

---

## 12) Version/compatibility tips

* `tree_sitter.LANGUAGE_VERSION` and `tree_sitter.MIN_COMPATIBLE_LANGUAGE_VERSION` are exported in Python; ensure the grammars you load are compatible with the binding’s ABI version. ([Tree-sitter][2])
* If you are migrating from pre‑0.25:

  * Replace `Parser.set_language(...)` with `Parser(Language(...))`
  * Migrate `Query.*` execution/limits/timeouts to `QueryCursor`
  * Prefer `progress_callback` to `timeout_micros` (deprecated)
    The official release notes enumerate these changes. ([GitHub][7])

---

## 13) Where to read more (official docs)

* **Python API (0.25.x):** class pages for `Parser`, `Tree`, `Node`, `Language`, `Query`, `QueryCursor`, `Range`, `Point`, `TreeCursor`—these are authoritative for signatures and behaviors. ([Tree-sitter][8])
* **Query syntax & highlighting/injections:** See the “Using Parsers → Queries → Syntax” and the syntax highlighting & language injection chapters. ([Tree-sitter][19])
* **Python grammar:** `tree-sitter-python` repo/wheel. ([GitHub][20])

---

### Quick “gotchas” to avoid

* Pass **bytes** to `parse(...)` (or a callback returning bytes). Don’t pass `str`. ([Tree-sitter][8])
* `Node.text` is only valid **if the tree hasn’t been edited** since the node was obtained. After `Tree.edit`, reacquire nodes. ([Tree-sitter][10])
* Prefer `QueryCursor` over `Query.captures/matches` (the latter are deprecated/moved). ([GitHub][21])
* Don’t overuse non‑local patterns; they prevent some range optimizations. ([Tree-sitter][13])

---

### Minimal working snippet (all together)

```python
from tree_sitter import Language, Parser, Query, QueryCursor, Point
import tree_sitter_python as tspython

PY = Language(tspython.language())
parser = Parser(PY)

src = b"""
def greet(name):
    print("hi", name)
"""

tree = parser.parse(src)

# Find function names
q = Query(PY, '(function_definition name: (identifier) @fn.name)')
cursor = QueryCursor(q)
caps = cursor.captures(tree.root_node)
names = [n.text.decode('utf8') for n in caps['fn.name']]
print(names)  # ['greet']
```

This uses the **new** `Parser(Language(...))` and `QueryCursor` APIs and shows text extraction through `Node.text`. ([Tree-sitter][8])

---

If you’d like, I can tailor the above into a **scaffold** for your design task—e.g., an incremental, multi‑language analyzer skeleton with query packs, injection support, and a change‑driven pipeline (parsing → scoped queries → diagnostics/index), built around `changed_ranges`, `QueryCursor`, and optional `lookahead_iterator`.

[1]: https://tree-sitter.github.io/?utm_source=chatgpt.com "Tree-sitter: Introduction"
[2]: https://tree-sitter.github.io/py-tree-sitter/ "py-tree-sitter — py-tree-sitter 0.25.2 documentation"
[3]: https://pypi.org/project/tree-sitter/?utm_source=chatgpt.com "Python Tree-sitter"
[4]: https://pypi.org/project/tree-sitter-python/?utm_source=chatgpt.com "tree-sitter-python"
[5]: https://pypi.org/project/tree-sitter-languages/ "tree-sitter-languages · PyPI"
[6]: https://pypi.org/project/tree-sitter-language-pack/ "tree-sitter-language-pack · PyPI"
[7]: https://github.com/tree-sitter/py-tree-sitter/releases "Releases · tree-sitter/py-tree-sitter · GitHub"
[8]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Parser.html "Parser — py-tree-sitter 0.25.2 documentation"
[9]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Tree.html "Tree — py-tree-sitter 0.25.2 documentation"
[10]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Node.html "Node — py-tree-sitter 0.25.2 documentation"
[11]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.TreeCursor.html?utm_source=chatgpt.com "TreeCursor — py-tree-sitter 0.25.2 documentation"
[12]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Language.html "Language — py-tree-sitter 0.25.2 documentation"
[13]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Query.html "Query — py-tree-sitter 0.25.2 documentation"
[14]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.QueryCursor.html "QueryCursor — py-tree-sitter 0.25.2 documentation"
[15]: https://davisvaughan.github.io/r-tree-sitter/reference/query-matches-and-captures.html?utm_source=chatgpt.com "Query matches and captures"
[16]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Point.html?utm_source=chatgpt.com "Point — py-tree-sitter 0.25.2 documentation"
[17]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Range.html "Range — py-tree-sitter 0.25.2 documentation"
[18]: https://tree-sitter.github.io/tree-sitter/3-syntax-highlighting.html "Syntax Highlighting - Tree-sitter"
[19]: https://tree-sitter.github.io/tree-sitter/using-parsers/queries/1-syntax.html "Basic Syntax - Tree-sitter"
[20]: https://github.com/tree-sitter/tree-sitter-python?utm_source=chatgpt.com "Python grammar for tree-sitter"
[21]: https://github.com/tree-sitter/py-tree-sitter/releases?utm_source=chatgpt.com "Releases · tree-sitter/py-tree-sitter"
