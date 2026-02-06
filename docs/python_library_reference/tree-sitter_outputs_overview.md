Below is the “what you can *actually* get out of Tree-sitter for Python” map, split into:

1. **Direct Tree-sitter outputs** (the CST + grammar metadata + query results)
2. **Agent-ready derived artifacts** you can compute cheaply from those outputs (or from standard Tree-sitter query conventions)

---

## 1) Direct outputs from Tree-sitter (Python CST construction)

Tree-sitter is an **incremental parsing system** that builds a **concrete syntax tree** and can efficiently update it as the source changes; it’s designed to stay useful even with syntax errors. ([GitHub][1])

### 1.1 `Language` (grammar identity + introspection surface)

What you can extract *about the Python grammar* (not about a specific file):

* **Identity/versioning**

  * `Language.name`, `Language.semantic_version`, `Language.abi_version` ([tree-sitter.github.io][2])
  * ABI compatibility guidance is in the py-tree-sitter docs home (library supports a range of language ABIs). ([tree-sitter.github.io][3])

* **Node kinds + classification**

  * `Language.node_kind_count`, `Language.node_kind_for_id(id)`, `Language.id_for_node_kind(kind, named)` ([tree-sitter.github.io][2])
  * `Language.node_kind_is_named / is_visible / is_supertype` + `Language.supertypes` + `Language.subtypes(supertype)` ([tree-sitter.github.io][2])
    This is how you learn “what kinds exist”, which are “structural” vs “auxiliary”, and which abstract supertypes exist.

* **Field names**

  * `Language.field_count`, `Language.field_id_for_name(name)`, `Language.field_name_for_id(id)` ([tree-sitter.github.io][2])

* **Parse-state / completion primitives**

  * `Language.next_state(state, id)` and `Language.lookahead_iterator(state)` (used with node parse states) ([tree-sitter.github.io][2])

> Practical: `Language` lets you build **ID-based, allocation-light** traversals (store `kind_id`, `field_id` ints), and it’s also your bridge to “what tokens are valid here?” via parse states. ([tree-sitter.github.io][2])

---

### 1.2 `Parser` (turn bytes → `Tree`, optionally incremental)

Core outputs and controls:

* `Parser.parse(source, old_tree=None, encoding="utf8") -> Tree | None` ([tree-sitter.github.io][4])

  * `source` can be **bytes** *or* a **read callback** (byte offset + point → bytes). ([tree-sitter.github.io][4])
* `Parser.included_ranges` lets you parse only selected ranges (projection parsing). ([tree-sitter.github.io][4])
* Debug surfaces:

  * `Parser.logger` ([tree-sitter.github.io][4])
  * `Parser.print_dot_graphs(file)` emits DOT graphs of parsing steps. ([tree-sitter.github.io][4])
* Operational correctness:

  * If a parse times out, the parser can resume on the next parse unless you `reset()`; use `reset()` when switching documents. ([tree-sitter.github.io][4])
* The py-tree-sitter README shows the canonical Python grammar install + load pattern (`tree-sitter-python` wheel → `Language(tspython.language())`). ([GitHub][5])

---

### 1.3 `Tree` (the CST for one document)

What you can extract per parse:

* `Tree.root_node` (entrypoint), `Tree.language`, `Tree.included_ranges` ([tree-sitter.github.io][6])
* Incremental maintenance:

  * `Tree.edit(...)` updates internal offsets (you supply byte offsets **and** row/col points) ([tree-sitter.github.io][6])
  * `Tree.changed_ranges(new_tree)` returns ranges where ancestor structure changed (your “re-run analysis only here” primitive). ([tree-sitter.github.io][6])
* Traversal/debug:

  * `Tree.walk()` creates a `TreeCursor` ([tree-sitter.github.io][6])
  * `Tree.print_dot_graph(file)` emits a DOT graph of the syntax tree ([tree-sitter.github.io][6])
  * `Tree.root_node_with_offset(...)` is useful when you parse fragments but want virtualized positions. ([tree-sitter.github.io][6])

---

### 1.4 `Node` (the fundamental CST record)

This is the core “CST construction output”: each node gives you **shape + identity + coordinates + error signals**.

**Structural identity**

* `node.type` (string node kind) ([tree-sitter.github.io][7])
* `node.kind_id` vs `node.grammar_id` / `node.grammar_name` (alias-aware vs grammar symbol identity) ([tree-sitter.github.io][7])

**Coordinates (the anchor everything else should join on)**

* `node.start_byte`, `node.end_byte`, plus `node.start_point`, `node.end_point` ([tree-sitter.github.io][7])
* `node.byte_range` and `node.range` (convenience wrappers) ([tree-sitter.github.io][7])

**Tree navigation**

* Parent/sibling: `node.parent`, `node.next_sibling`, `node.prev_sibling` (and named variants) ([tree-sitter.github.io][7])
* Children:

  * positional: `child(i)`, `children`, `named_child(i)`, `named_children` ([tree-sitter.github.io][7])
  * fielded: `child_by_field_name`, `children_by_field_name`, and field-name reverse lookup `field_name_for_child(...)` ([tree-sitter.github.io][7])
* Spatial selection:

  * `descendant_for_byte_range`, `descendant_for_point_range`, and named-only variants ([tree-sitter.github.io][7])

**Error + recovery signals**

* `node.is_error`, `node.has_error` ([tree-sitter.github.io][7])
* `node.is_missing` (parser-inserted nodes for recovery) ([tree-sitter.github.io][7])
* `node.is_extra` (things that can appear anywhere, like whitespace/comments, depending on grammar) ([tree-sitter.github.io][7])

**Incremental stability hooks**

* `node.id` is unique within a tree and can persist across old/new trees when a node is reused in incremental parsing. ([tree-sitter.github.io][7])
* `node.has_changes` indicates edits. ([tree-sitter.github.io][7])

**Text**

* `node.text` returns the node’s bytes *if the tree has not been edited*. ([tree-sitter.github.io][7])
  In general, Tree-sitter is range-based: your safest “ground truth” is `source_bytes[node.start_byte:node.end_byte]`.

---

### 1.5 `TreeCursor` (fast traversal + field metadata)

For building your own serialized CST, the cursor is the “streaming walker”:

* Navigation: `goto_first_child`, `goto_next_sibling`, `goto_parent`, etc. ([tree-sitter.github.io][8])
* Outputs per cursor position:

  * `cursor.node`
  * `cursor.depth`
  * **field metadata**: `cursor.field_name` / `cursor.field_id` for the current node relative to its parent ([tree-sitter.github.io][8])

This is often the cleanest way to emit **(parent, child, field_name)** edges.

---

### 1.6 Query system (`Query` + `QueryCursor`) (structured extraction output)

Tree-sitter’s query system is “CST → matches/captures”, which is usually the highest ROI for building agent-facing indexes.

**Query language (what it can express)**

* Patterns are S-expressions matching node types and optionally their children; you can constrain via **fields**, match **anonymous tokens**, and query special nodes like `ERROR` and `MISSING`. ([tree-sitter.github.io][9])
* Supertypes can be used in queries to match any subtype; `supertype/subtype` syntax exists. ([tree-sitter.github.io][9])

**Predicates**

* py-tree-sitter documents built-in predicates like `#eq?`, `#match?`, `#any-of?`, `#is?`, `#set!`, etc. ([tree-sitter.github.io][10])

**Execution outputs**

* `QueryCursor.captures(node)` → `{capture_name: [Node, ...]}` ([tree-sitter.github.io][11])
* `QueryCursor.matches(node)` → `[(pattern_index, {capture_name: Node, ...}), ...]` ([tree-sitter.github.io][11])
* Range limiting:

  * `set_byte_range(start, end)` / `set_point_range(start, end)` (intersecting matches) ([tree-sitter.github.io][11])
  * “containing” variants restrict matches to be fully contained. ([tree-sitter.github.io][11])
* Safety/perf:

  * `match_limit`, `did_exceed_match_limit` ([tree-sitter.github.io][11])

---

### 1.7 “What becomes a node?” (tokens, extras, indentation)

This matters a lot for “CST construction expectations.”

From Tree-sitter’s grammar DSL:

* By default, each string/regex literal in a grammar is treated as a separate **token** and is returned as a **leaf node**. ([tree-sitter.github.io][12])
* Grammars can define `extras` (often whitespace/comments) that can appear anywhere. ([tree-sitter.github.io][12])
* Grammars can use an **external scanner** for lexical rules that aren’t regex-friendly (Python indentation is the canonical example class). ([tree-sitter.github.io][12])

So for Python:

* You should expect a mix of **named nodes** (grammar rules) and **anonymous token nodes** (punctuation/keywords/operators). ([tree-sitter.github.io][9])
* You may see **extra nodes** (e.g., comments) depending on how the Python grammar declares `extras`. ([tree-sitter.github.io][7])

---

### 1.8 `node-types.json` (exhaustive node/field schema for the Python grammar)

This is the “static schema” of the grammar (what nodes *can* exist).

Tree-sitter generates `node-types.json`, describing every node type, whether it’s named, its possible fields/children, and any supertypes/subtypes. ([tree-sitter.github.io][13])
This is the file you use to generate:

* typed wrappers,
* schema validators for your CST serializer,
* “which fields exist on `function_definition`?” without guessing. ([tree-sitter.github.io][13])

---

## 2) The agent-ready CST outputs you should export (recommended “contract”)

If you want an LLM agent to *reason over code structure reliably*, don’t hand it raw recursive objects. Export a small set of **joinable, span-anchored artifacts**:

### 2.1 `cst_nodes` (row per node)

Minimum columns:

* `file_path`
* `node_id` (Tree-sitter `Node.id`) ([tree-sitter.github.io][7])
* `parent_id` (or null for root) ([tree-sitter.github.io][7])
* `type` (`Node.type`)
* `is_named`, `is_extra`, `is_error`, `is_missing`, `has_error`, `has_changes` ([tree-sitter.github.io][7])
* `start_byte`, `end_byte`, `start_row`, `start_col`, `end_row`, `end_col` ([tree-sitter.github.io][7])
* `grammar_id` (optional but great for stability) ([tree-sitter.github.io][7])

### 2.2 `cst_edges` (parent→child with field metadata)

Use `TreeCursor.field_name` / `field_id` to emit:

* `parent_id`, `child_id`
* `field_name` / `field_id`
* `child_index` (positional order) ([tree-sitter.github.io][8])

### 2.3 `cst_tokens` (optional but very useful)

Emit leaf nodes (including anonymous tokens) with:

* `start_byte/end_byte`
* `token_text = source_bytes[start:end].decode(...)`
* `is_named` (most punctuation will be anonymous) ([tree-sitter.github.io][9])

### 2.4 `cst_diagnostics`

* all nodes where `is_error` or `is_missing`, plus enclosing context via ancestor chain. ([tree-sitter.github.io][7])

### 2.5 `cst_query_hits` (your “semantic projections”)

For each query pack you care about:

* `query_name`, `pattern_index`
* captures: `capture_name`, `node_id`, `start/end` spans ([tree-sitter.github.io][11])

---

## 3) High-leverage derived outputs (not “tree-sitter outputs”, but cheaply constructible)

These are the things an LLM agent typically *actually needs* to navigate a repo.

### 3.1 Outline + “what are the major units?”

Build using queries:

* modules
* classes
* functions / methods
* decorators
* docstrings (Python: string literal as first statement in a block)

This becomes: `definitions(def_kind, name, qualname_guess, span, body_span, decorators_span, docstring_span)`.

### 3.2 Imports → dependency graph

From import nodes, emit:

* `import_edge(from_file → module)`
* imported names + aliases
* `from X import Y as Z` edges

Even without semantic resolution, this gives agents a strong “where to look next” map.

### 3.3 Callsites table (shape-first, resolution-later)

For each call expression:

* callee “shape” (identifier vs attribute chain vs call-returned-call, etc.)
* argument count, keyword names present
* span of callee token(s)

This supports agent tasks like “find where X is called”, “find calls missing keyword timeout”, etc., even before you do name binding.

### 3.4 Scope/locals (Tree-sitter’s *query convention* approach)

Tree-sitter’s highlighting system defines a standard **locals query** idea:

* captures `@local.scope`, `@local.definition`, `@local.reference`
* the highlighter tracks scopes/definitions so references can be linked to definitions textually. ([tree-sitter.github.io][14])

For agents, you can reuse the same idea:

* it’s fast, works cross-language, and is “good enough” for many navigation tasks
  …but it is not compiler-accurate for Python’s full binding rules.

### 3.5 Incremental “what changed?” for fast re-index

If you keep `old_tree`:

1. apply `old_tree.edit(...)` with your text edit
2. `new_tree = parser.parse(new_source, old_tree=old_tree)`
3. `old_tree.changed_ranges(new_tree)` gives minimal re-analysis windows ([tree-sitter.github.io][6])

Then re-run only the impacted queries/hit extraction in those ranges.

### 3.6 Multi-language / embedded snippets (optional)

Tree-sitter standardizes **injection queries** (`queries/injections.scm`) for “parse this region as another language”; it’s part of the CLI/highlight ecosystem. ([tree-sitter.github.io][14])
For Python repos, this can matter if you want to parse embedded SQL in strings, regexes, etc. (design choice, not automatic).

---

## 4) A tiny “emit CST rows” skeleton (py-tree-sitter 0.25+)

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional

import tree_sitter_python as tspython
from tree_sitter import Language, Parser

PY = Language(tspython.language())
parser = Parser(PY)

@dataclass(frozen=True)
class CstRow:
    node_id: int
    parent_id: Optional[int]
    type: str
    is_named: bool
    is_extra: bool
    is_error: bool
    is_missing: bool
    start_byte: int
    end_byte: int
    start_row: int
    start_col: int
    end_row: int
    end_col: int

def iter_cst_rows(src: bytes) -> Iterator[CstRow]:
    tree = parser.parse(src)
    cursor = tree.walk()

    stack: list[tuple[object, Optional[int]]] = [(cursor, None)]  # (cursor-ish, parent_id)
    # We'll do manual DFS using TreeCursor methods (fast + field_name available).
    cursor = tree.walk()
    parent_stack: list[Optional[int]] = [None]
    visited_children = False

    while True:
        n = cursor.node
        row = CstRow(
            node_id=n.id,
            parent_id=parent_stack[-1],
            type=n.type,
            is_named=n.is_named,
            is_extra=n.is_extra,
            is_error=n.is_error,
            is_missing=n.is_missing,
            start_byte=n.start_byte,
            end_byte=n.end_byte,
            start_row=n.start_point.row,
            start_col=n.start_point.column,
            end_row=n.end_point.row,
            end_col=n.end_point.column,
        )
        yield row

        if cursor.goto_first_child():
            parent_stack.append(n.id)
            continue
        while True:
            if cursor.goto_next_sibling():
                break
            if not cursor.goto_parent():
                return
            parent_stack.pop()
```

API details used above: `Parser.parse`, `Tree.walk`, `Node.id/type/is_* flags`, start/end byte/point, and `TreeCursor.goto_*` traversal. ([tree-sitter.github.io][4])

---

If you tell me your preferred **canonical coordinate system** (bytes-only vs points-only vs both), I can propose an exact “CST table schema pack” (nodes/edges/tokens/query_hits) that’s optimized for joinability with your other evidence sources and for incremental updates via `changed_ranges`.

[1]: https://github.com/tree-sitter/tree-sitter "GitHub - tree-sitter/tree-sitter: An incremental parsing system for programming tools"
[2]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Language.html "Language — py-tree-sitter 0.25.2 documentation"
[3]: https://tree-sitter.github.io/py-tree-sitter/?utm_source=chatgpt.com "py-tree-sitter 0.25.2 documentation"
[4]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Parser.html "Parser — py-tree-sitter 0.25.2 documentation"
[5]: https://github.com/tree-sitter/py-tree-sitter "GitHub - tree-sitter/py-tree-sitter: Python bindings to the Tree-sitter parsing library"
[6]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Tree.html "Tree — py-tree-sitter 0.25.2 documentation"
[7]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Node.html "Node — py-tree-sitter 0.25.2 documentation"
[8]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.TreeCursor.html "TreeCursor — py-tree-sitter 0.25.2 documentation"
[9]: https://tree-sitter.github.io/tree-sitter/using-parsers/queries/1-syntax.html "Basic Syntax - Tree-sitter"
[10]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Query.html "Query — py-tree-sitter 0.25.2 documentation"
[11]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.QueryCursor.html "QueryCursor — py-tree-sitter 0.25.2 documentation"
[12]: https://tree-sitter.github.io/tree-sitter/creating-parsers/2-the-grammar-dsl.html "The Grammar DSL - Tree-sitter"
[13]: https://tree-sitter.github.io/tree-sitter/using-parsers/6-static-node-types "Static Node Types - Tree-sitter"
[14]: https://tree-sitter.github.io/tree-sitter/3-syntax-highlighting.html "Syntax Highlighting - Tree-sitter"
