# 2) Rust grammar contract for ast-grep: node kinds + fields you should treat as a “stable ABI”

This chapter is the **Rosetta Stone** between:

* what you write in `pattern=...` / `kind=...` (CLI + Python), and
* what actually exists in the Rust concrete syntax tree (tree-sitter-rust).

The contract lives in **`node-types.json`** (the canonical list of node kinds + their **field names** + child shapes). Tree-sitter defines exactly what this file contains and how to interpret it. ([tree-sitter.github.io][1])

---

## 2.0 “Stable ABI” mental model (why kinds + fields beat text regex)

### Named nodes are your stability boundary

Tree-sitter distinguishes:

* **named nodes** (grammar rule names like `function_item`, `impl_item`)
* **unnamed nodes** (string-literal tokens like `"+"`, keywords, punctuation)

`node-types.json` encodes both, and treats `(type, named)` as the unique identifier for a node type. ([tree-sitter.github.io][1])

### ast-grep defaults align with “named nodes first”

ast-grep’s default strictness (`smart`) *skips unnamed nodes in the target code* during matching, which makes your patterns robust to trivia (keywords/punctuation formatting) unless you explicitly include them. ([ast-grep.github.io][2])

**Implication for Rust:** if you want rules that survive refactors and rustfmt, anchor on:

* **node kind** (`kind="function_item"`) and/or
* **field names** (`name`, `body`, `parameters`, …),
  not brittle `text()` regex.

### Fields are the “child index that doesn’t lie”

Tree-sitter exposes APIs specifically to retrieve children by **field name** (`child_by_field_name`, `children_by_field_name`) and to inspect field names for children. ([Docs.rs][3])
So: **prefer `.field("name")` / field-aware logic** over “child #2 is probably the identifier”.

---

## 2.1 Node kind atlas from `tree-sitter-rust` `node-types.json`

### 2.1.1 Acquire the grammar contract (pin it!)

The tree-sitter-rust crate publishes `src/node-types.json` (here shown for v0.24.0). ([Docs.rs][4])

Recommended: **vendor + pin** this file in your repo (or pin the crate version) so your rule pack doesn’t silently drift.

Example download (docs.rs source path):

```bash
mkdir -p vendor/tree-sitter-rust
curl -L \
  'https://docs.rs/crate/tree-sitter-rust/latest/source/src/node-types.json' \
  -o vendor/tree-sitter-rust/node-types.json
```

(That URL corresponds to the same content you can browse on docs.rs.) ([Docs.rs][4])

### 2.1.2 Understand the JSON schema (you’ll query this constantly)

Tree-sitter defines the structure:

* each entry has `"type"` and `"named"`
* internal nodes may declare:

  * `"fields"`: map `field_name -> {required, multiple, types[]}`
  * `"children"`: same shape, but for “unfielded named children”
  * `"subtypes"`: union/supertype modeling (e.g., `_type`, `_expression`) ([tree-sitter.github.io][1])

### 2.1.3 Build an *atlas* object you can query (Python)

This is the core utility you’ll reuse in every deep dive:

```python
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

@dataclass(frozen=True)
class ChildSpec:
    required: bool
    multiple: bool
    types: list[tuple[str, bool]]  # (type, named)

@dataclass(frozen=True)
class NodeSpec:
    type: str
    named: bool
    fields: dict[str, ChildSpec]
    children: ChildSpec | None
    subtypes: list[tuple[str, bool]] | None

def load_node_types(path: str | Path) -> dict[tuple[str, bool], NodeSpec]:
    data = json.loads(Path(path).read_text())
    out: dict[tuple[str, bool], NodeSpec] = {}
    for entry in data:
        def parse_childspec(obj: Any) -> ChildSpec:
            return ChildSpec(
                required=bool(obj["required"]),
                multiple=bool(obj["multiple"]),
                types=[(t["type"], bool(t["named"])) for t in obj["types"]],
            )
        fields = {k: parse_childspec(v) for k, v in (entry.get("fields") or {}).items()}
        children = parse_childspec(entry["children"]) if "children" in entry else None
        subtypes = [(s["type"], bool(s["named"])) for s in entry.get("subtypes", [])] or None
        spec = NodeSpec(entry["type"], bool(entry["named"]), fields, children, subtypes)
        out[(spec.type, spec.named)] = spec
    return out

atlas = load_node_types("vendor/tree-sitter-rust/node-types.json")

def show(kind: str, named: bool = True) -> None:
    spec = atlas[(kind, named)]
    print(f"{spec.type} (named={spec.named})")
    if spec.fields:
        print("  fields:")
        for f, cs in sorted(spec.fields.items()):
            print(f"    - {f}: required={cs.required} multiple={cs.multiple} types={cs.types}")
    if spec.children:
        cs = spec.children
        print(f"  children: required={cs.required} multiple={cs.multiple} types={cs.types}")
    if spec.subtypes:
        print(f"  subtypes: {spec.subtypes}")

show("function_item")
```

### 2.1.4 Generate a “hot set” of kinds (minimal, high ROI)

You want two lists:

**A) Always-hot (structural anchors)**
Rule of thumb: include anything that:

* is a top-level “item” kind (`*_item`, `use_declaration`, `mod_item`)
* has **fields** (because fields are stable handles)
* is macro/attribute related (`macro_*`, `attribute_*`, `token_tree`)
* is a supertype you’ll use for broad matches (`_expression`, `_type`, `_pattern`)

**B) Repo-hot (frequency in your target codebase)**
Use ast-grep-py to parse `.rs` files and count node kinds:

```python
from ast_grep_py import SgRoot
from pathlib import Path
from collections import Counter

def walk(n):
    yield n
    for c in n.children():
        yield from walk(c)

counts = Counter()
for p in Path("path/to/repo").rglob("*.rs"):
    root = SgRoot(p.read_text(encoding="utf-8"), "rust").root()
    for n in walk(root):
        if n.is_named():
            counts[n.kind()] += 1

for kind, n in counts.most_common(60):
    print(n, kind)
```

This produces a **ranked hot list**; intersect it with “has fields” from `node-types.json` to get the most useful stable anchors.

### 2.1.5 Debug any Rust snippet into kinds (CLI workflow)

Use `ast-grep run` with `--debug-query` to see what your pattern parses into.

`--debug-query` formats: `ast` (named nodes only), `cst` (named + unnamed), `sexp` (S-expression), plus `pattern`. It requires `-l/--lang` explicitly. ([ast-grep.github.io][5])

Examples:

```bash
# Show named-node AST for a Rust pattern
sg run -l rust -p 'fn foo(x: i32) -> i32 { x + 1 }' --debug-query ast

# Show CST (includes punctuation/keywords tokens)
sg run -l rust -p 'fn foo(x: i32) -> i32 { x + 1 }' --debug-query cst

# Show S-expression form (great for quickly seeing nesting)
sg run -l rust -p 'impl Foo { fn bar(&self) {} }' --debug-query sexp
```

If your snippet isn’t parseable standalone, use `--selector <KIND>` (or YAML pattern object `context` + `selector`) to parse with context and select the subnode you actually want to match. ([ast-grep.github.io][5])

---

## 2.2 High-leverage Rust *item* kinds (top-level and nested)

Below are the **stable ABI nodes** you should treat as your “public interface” for Rust structural search. Every bullet is taken directly from tree-sitter-rust `node-types.json`.

### 2.2.1 `function_item`

**Fields (stable handles):**

* `name`: `identifier | metavariable` (**required**)
* `parameters`: `parameters` (**required**)
* `return_type`: `_type` (optional)
* `type_parameters`: `type_parameters` (optional)
* `body`: `block` (**required**) ([Docs.rs][4])

**Children (unfielded named children you’ll often filter on):**

* `function_modifiers`, `visibility_modifier`, `where_clause` (all optional as children) ([Docs.rs][4])

**Action patterns (CLI):**

```bash
# Find all functions (structural)
sg run -l rust -p 'fn $NAME($$$ARGS) $$$RET { $$$BODY }'

# Inspect the pattern’s AST (confirm the root matcher kind)
sg run -l rust -p 'fn $NAME($$$ARGS) $$$RET { $$$BODY }' --debug-query ast
```

**Action extraction (Python):**

```python
from ast_grep_py import SgRoot

root = SgRoot(src, "rust").root()
for fn in root.find_all(kind="function_item"):
    name = fn.field("name").text()           # identifier/metavariable
    params = fn.field("parameters").text()   # parameters
    ret = fn.field("return_type")
    body = fn.field("body")                  # block
```

### 2.2.2 `impl_item`

This is your “impl block” anchor.

**Fields:**

* `type`: `_type` (**required**)  ← the implemented type
* `trait`: `generic_type | scoped_type_identifier | type_identifier` (optional)  ← present for `impl Trait for Type`
* `type_parameters`: `type_parameters` (optional)
* `body`: `declaration_list` (optional) ([Docs.rs][4])

**Children:**

* optional `where_clause` ([Docs.rs][4])

**Action patterns:**

```bash
# Inherent impl
sg run -l rust -p 'impl $TYPE { $$$ITEMS }'

# Trait impl
sg run -l rust -p 'impl $TRAIT for $TYPE { $$$ITEMS }'
```

**Python discrimination (trait vs inherent):**

```python
for impl in root.find_all(kind="impl_item"):
    trait = impl.field("trait")   # None => inherent
    ty = impl.field("type").text()
```

### 2.2.3 `trait_item`

**Fields:**

* `name`: `type_identifier` (**required**)
* `body`: `declaration_list` (**required**)
* `bounds`: `trait_bounds` (optional)
* `type_parameters`: `type_parameters` (optional) ([Docs.rs][4])

**Children:**

* optional `visibility_modifier`, `where_clause` ([Docs.rs][4])

**Action patterns:**

```bash
sg run -l rust -p 'trait $NAME $$$REST { $$$ITEMS }'
# then use --debug-query ast to see what $$$REST binds to in your pattern
```

### 2.2.4 `struct_item`

**Fields:**

* `name`: `type_identifier` (**required**)
* `body`: `field_declaration_list | ordered_field_declaration_list` (optional)
* `type_parameters`: `type_parameters` (optional) ([Docs.rs][4])

**Children:**

* optional `visibility_modifier`, `where_clause` ([Docs.rs][4])

**Why two possible bodies?**
That’s a grammar-level distinction (tuple/record-style variations + ordering forms) that you should treat as *ABI* rather than branching on text. ([Docs.rs][4])

### 2.2.5 `enum_item`

**Fields:**

* `name`: `type_identifier` (**required**)
* `body`: `enum_variant_list` (**required**)
* `type_parameters`: `type_parameters` (optional) ([Docs.rs][4])

**Children:**

* optional `visibility_modifier`, `where_clause` ([Docs.rs][4])

### 2.2.6 `type_item` (type alias)

**Fields:**

* `name`: `type_identifier` (**required**)
* `type`: `_type` (**required**)
* `type_parameters`: `type_parameters` (optional) ([Docs.rs][4])

**Children:**

* optional `visibility_modifier`, `where_clause` ([Docs.rs][4])

### 2.2.7 `use_declaration`

This is a *high-variance syntax surface*; rely on the `argument` field kinds rather than parsing strings.

**Fields:**

* `argument` (**required**) can be:

  * `crate | identifier | metavariable | scoped_identifier`
  * `scoped_use_list | use_list | use_wildcard`
  * `use_as_clause`
  * `self | super` ([Docs.rs][4])

**Children:**

* optional `visibility_modifier` ([Docs.rs][4])

**Action patterns:**

```bash
# broad “any use”
sg run -l rust -p 'use $X;'

# visibility-sensitive
sg run -l rust -p 'pub use $X;'
```

When you need more precision, match specific `argument` shapes by combining `pattern` + `kind` or by inspecting `use_declaration.field("argument").kind()` in Python.

### 2.2.8 `mod_item`

**Fields:**

* `name`: `identifier` (**required**)
* `body`: `declaration_list` (optional)  ← present for inline `mod foo { ... }` ([Docs.rs][4])

**Children:**

* optional `visibility_modifier` ([Docs.rs][4])

---

## 2.3 Macro & attribute primitives (Rust must-haves)

Rust’s surface syntax is macro/attribute heavy. Treat these nodes as first-class “ABI”.

### 2.3.1 `macro_invocation` (call site)

**Fields:**

* `macro`: `identifier | scoped_identifier` (**required**) ([Docs.rs][4])

**Children:**

* `token_tree` (**required**) — the macro’s payload ([Docs.rs][4])

**Operational implication:** you can reliably extract the macro path via field access, but the arguments live under `token_tree` (often best handled with structural submatches when possible, and fallback `regex/text()` only when token trees get too free-form).

### 2.3.2 `macro_definition` (e.g., `macro_rules!`)

**Fields:**

* `name`: `identifier` (**required**) ([Docs.rs][4])

**Children:**

* `macro_rule` (0+) ([Docs.rs][4])

So **invocation** and **definition** are disjoint kinds:

* invocations: `macro_invocation`
* definitions: `macro_definition`

### 2.3.3 Token-tree internals (what you can safely assume)

Tree-sitter models token trees with dedicated nodes (`token_tree`, `token_repetition`, `token_tree_pattern`, etc.), and enumerates what kinds can occur under them (identifiers, literals, nested token trees, etc.). ([Docs.rs][4])

**Practical rule-author guidance:** macros are the #1 place where you’ll use the CLI `--debug-query` loop to understand what the grammar *actually* emits for your snippet. ([ast-grep.github.io][5])

### 2.3.4 Attributes: `attribute_item` vs `inner_attribute_item` vs `attribute`

Rust attributes are modeled as:

* `attribute_item` → contains one `attribute` ([Docs.rs][4])
* `inner_attribute_item` → contains one `attribute` ([Docs.rs][4])
* `attribute` has:

  * fields:

    * `arguments`: `token_tree` (optional)
    * `value`: `_expression` (optional)
  * children (attribute path head):

    * `crate | identifier | metavariable | scoped_identifier | self | super` ([Docs.rs][4])

**Why this matters:** attribute “names” are not a single string token; they’re structured as identifier/scoped_identifier/etc. If you want `#[cfg(...)]` vs `#[derive(...)]`, you either:

* structurally match the `attribute` subtree, or
* match `attribute.text()` with tight regex *after* you’ve structurally constrained the surrounding node (e.g., only attributes attached to `function_item`).

---

## 2.4 The core workflow you should standardize for Rust rule authors

1. **Pick the anchor kind** from `node-types.json` (e.g., `function_item`, `impl_item`). ([Docs.rs][4])
2. Write the smallest pattern that parses, then run:

   ```bash
   sg run -l rust -p '...' --debug-query ast
   ```

   (`--debug-query` is the fast path to “what kind is this?”) ([ast-grep.github.io][5])
3. Use `--debug-query cst` only when punctuation/keywords matter. ([ast-grep.github.io][5])
4. When snippets need surrounding context, use:

   * `--selector <KIND>` on the CLI, or
   * YAML `pattern: { context: ..., selector: ... }` ([ast-grep.github.io][5])
5. In Python, **prefer field access** (`node.field("name")`) over child indexing; it maps directly to tree-sitter field semantics. ([Docs.rs][3])

---

If you want, the next deep dive can be **“Rust fragment contexts for patterns”** (how to make Rust snippets parseable, when to use `context/selector`, and a cookbook of selectors for “match just the return type”, “match just the trait name”, etc.).

[1]: https://tree-sitter.github.io/tree-sitter/using-parsers/6-static-node-types "Static Node Types - Tree-sitter"
[2]: https://ast-grep.github.io/advanced/match-algorithm.html "Deep Dive into ast-grep's Match Algorithm | ast-grep"
[3]: https://docs.rs/tree-sitter/latest/tree_sitter/struct.Node.html "Node in tree_sitter - Rust"
[4]: https://docs.rs/crate/tree-sitter-rust/latest/source/src/node-types.json "tree-sitter-rust 0.24.0 - Docs.rs"
[5]: https://ast-grep.github.io/reference/cli/run.html "ast-grep run | ast-grep"

# 3) Pattern authoring for Rust: “make the parser happy” (fragment contexts)

Rust is where “pattern-as-code” hurts the most because you constantly want to match **fragments** (a type, an expression, a single `let`, a path), but ast-grep patterns are parsed by tree-sitter and **must be parseable code**. When you give an incomplete/ambiguous snippet, ast-grep will *try* to recover, but that recovery is **not guaranteed across versions**—so for Rust you should default to **pattern objects with `context + selector`** (or the CLI equivalent `--selector`). ([Ast Grep][1])

---

## 3.0 The “Rust pattern authoring loop” you should standardize

### 3.0.1 Invariants you’re fighting

* **Pattern must be valid code that tree-sitter can parse.** ([Ast Grep][1])
* ast-grep parses pattern code as a **standalone file** by default; if you want to “parse inside a larger snippet,” you must provide `context` and then `selector` the node you actually want to match. ([Ast Grep][2])
* ast-grep picks an **effective node** from your snippet via a builtin heuristic (often the innermost structural node), which is frequently *not* what you want (e.g., you want `expression_statement`, but it extracts `call_expression`). You override this with `selector`. ([Ast Grep][3])

### 3.0.2 CLI debug loop (fastest way to converge)

```bash
# 1) Parse/inspect what your Rust pattern becomes
sg run -l rust -p 'YOUR_SNIPPET' --debug-query ast

# 2) If it extracted the wrong effective node, force it:
sg run -l rust -p 'YOUR_SNIPPET' --selector <KIND> --debug-query ast

# 3) If matching is too brittle, adjust strictness:
sg run -l rust -p 'YOUR_SNIPPET' --selector <KIND> --strictness <smart|ast|relaxed|signature|cst>
```

* `--debug-query` shows you the pattern’s tree-sitter structure.
* `--selector <KIND>` tells ast-grep which AST kind is the “actual matcher.” ([Ast Grep][4])

### 3.0.3 Python rule-authoring loop (ast-grep-py)

In Python, you do the same thing via a config dict and a pattern object:

```python
rule = {
  "pattern": {
    "context": "fn __sg_ctx() { let $P = $V; }",
    "selector": "let_declaration",
    # optional: "strictness": "smart",
  }
}

hits = root.root().find_all({"rule": rule})
```

Pattern objects are part of the rule schema: `pattern` can be a string or `{context, selector, strictness?}`. ([Ast Grep][2])

---

## 3.1 Fragment taxonomy (Rust-specific)

The trick is: **decide what syntactic category you truly want to match**, then choose a wrapper `context` that makes it parseable, and finally select the **right effective kind**.

Tree-sitter-rust’s root is `source_file`, and it can contain top-level declarations and even `expression_statement` (useful for snippet parsing / error recovery), but relying on recovery is brittle—wrap fragments anyway. ([Docs.rs][5])

### 3.1.1 Item fragments (top-level / nested)

**Use when you’re matching items**: `fn`, `impl`, `trait`, `struct`, `enum`, `type`, `use`, `mod`, etc.

**Best practice:** prefer *direct item patterns* when possible; wrap only when you need to target a nested item precisely (e.g., a method inside an `impl`).

**Examples (direct item patterns):**

```bash
# All free functions
sg run -l rust -p 'fn $NAME($$$ARGS) $$$RET { $$$BODY }'

# All impl blocks (trait or inherent)
sg run -l rust -p 'impl $$$HEAD { $$$ITEMS }'
```

If you need “method inside impl” as the effective node, wrap and select `function_item`:

```bash
sg run -l rust -p 'impl $T { fn $NAME($$$ARGS) $$$RET { $$$BODY } }' --selector function_item
```

(You’re using the impl to make the method parse in-place, then selecting the `function_item` as matcher.)

### 3.1.2 Statement fragments (inside blocks)

Rust statements like `let ...;` **do not exist as standalone files in real Rust**, and any successful parsing you observe without a wrapper is recovery-dependent. Use a block-bearing wrapper.

#### A) `let ...;` statements

There is an explicit `let_declaration` node kind. ([Docs.rs][5])

**Wrapper template:**

* `context`: `fn __sg_ctx() { <STMT> }`
* `selector`: `let_declaration`

```bash
sg run -l rust -p 'fn __sg_ctx() { let $PAT = $VAL; }' --selector let_declaration
```

Python rule object:

```python
rule = {
  "pattern": {
    "context": "fn __sg_ctx() { let $PAT = $VAL; }",
    "selector": "let_declaration",
  }
}
```

#### B) Expression statements (the semicolon boundary)

Tree-sitter-rust has `expression_statement`, whose child is an `_expression`. ([Docs.rs][5])

**Key pitfall:** ast-grep’s builtin effective-node heuristic often extracts the inner expression (e.g., `call_expression`) rather than the statement wrapper; if you want the trailing `;` (or want to use `follows/precedes` at statement granularity), you **must select `expression_statement`**. ([Ast Grep][3])

```bash
# Match "foo(bar);" as a statement (not just the call expression)
sg run -l rust -p 'fn __sg_ctx() { foo($$$ARGS); }' --selector expression_statement
```

#### C) “return / break / continue” are expressions in Rust

They exist as node kinds: `return_expression`, `break_expression`, `continue_expression`. ([Docs.rs][5])

**Two choices:**

* select the **expression** kind (`return_expression`) when semicolons don’t matter,
* select `expression_statement` when statement boundaries matter.

```bash
# Expression-only match
sg run -l rust -p 'fn __sg_ctx() { return $X; }' --selector return_expression

# Statement-granularity match (includes semicolon position)
sg run -l rust -p 'fn __sg_ctx() { return $X; }' --selector expression_statement
```

### 3.1.3 Expression fragments (call, match, if, etc.)

These usually parse fine inside a function body. Useful node kinds include:

* `call_expression` (fields: `function`, `arguments`) ([Docs.rs][5])
* `match_expression` ([Docs.rs][5])
* `if_expression` ([Docs.rs][5])

**Wrapper templates:**

* “pure expression”: `fn __sg_ctx() { let _ = <EXPR>; }` → select the expression kind you want
* “expression as statement”: `fn __sg_ctx() { <EXPR>; }` → select `expression_statement`

Examples:

```bash
# Match call expressions anywhere
sg run -l rust -p 'fn __sg_ctx() { $F($$$ARGS); }' --selector call_expression

# Match match-expressions
sg run -l rust -p 'fn __sg_ctx() { match $X { $$$ARMS } }' --selector match_expression

# Match if-expressions (including let-chains/conditions)
sg run -l rust -p 'fn __sg_ctx() { if $COND { $$$ } }' --selector if_expression
```

### 3.1.4 Type fragments (the #1 “won’t parse” category)

Tree-sitter-rust has the supertype `_type`, plus concrete nodes like:

* `reference_type` (field `type: _type`) ([Docs.rs][5])
* `generic_type` and `generic_type_with_turbofish` (useful for `Vec<T>` vs `Vec::<T>`) ([Docs.rs][5])

**Preferred wrapper templates (pick one):**

1. `type __SgT = <TYPE>;` (best when you only care about types)
2. `fn __sg_ctx(_: <TYPE>) {}` (best when you want parameter context like `&self`, lifetimes, patterns)

**Selectors:**

* use `_type` for “any type”
* use `reference_type` / `generic_type` / `pointer_type` etc. when you care about the specific shape

Examples:

```bash
# Match any reference type (&T / &'a T / &mut T…) by selecting reference_type
sg run -l rust -p 'type __SgT = &$T;' --selector reference_type

# Match generic types like Option<T>
sg run -l rust -p 'type __SgT = Option<$T>;' --selector generic_type
```

---

## 3.2 Object-style `pattern` with `context + selector` (Rust-specific use cases)

### 3.2.1 Why “kind + pattern” is not how you control parsing

`kind` and `pattern` are independent rules; `kind` does **not** change how the pattern string is parsed. To force parsing outcomes, you must use `pattern: {context, selector}`. ([Ast Grep][6])

### 3.2.2 The Rust wrapper library (copy/paste templates)

#### A) Match a `let_declaration` anywhere

```yaml
rule:
  pattern:
    context: |
      fn __sg_ctx() {
        let $PAT = $VAL;
      }
    selector: let_declaration
```

(Use the same structure as a Python dict for ast-grep-py.)

#### B) Match an expression statement (keep semicolons)

```yaml
rule:
  pattern:
    context: |
      fn __sg_ctx() {
        $EXPR;
      }
    selector: expression_statement
```

This directly addresses the builtin heuristic that otherwise selects the inner expression node. ([Ast Grep][3])

#### C) Match a type node

```yaml
rule:
  pattern:
    context: |
      type __SgT = $TYPE;
    selector: _type
```

### 3.2.3 Selector strategy (how to pick the right kind)

1. **Decide your granularity:**

   * want statement boundaries? select `expression_statement` / `let_declaration`
   * want “inner expression shape”? select `call_expression`, `match_expression`, etc. ([Docs.rs][5])
2. **Make `context` contain exactly one node of your selector kind** (avoid “which one did it pick?” confusion).
3. Use CLI `--debug-query ast` to verify the extracted node, then port that selector into your Python rule. ([Ast Grep][4])

### 3.2.4 Rust-specific disambiguation: “same text, different node kind”

Classic Rust pain: **paths can parse as expression-ish vs type-ish** depending on context.

* `scoped_identifier` exists (expression-ish path form) ([Docs.rs][5])
* `scoped_type_identifier` exists (type path form) ([Docs.rs][5])

**Force “type context” vs “expr context” using wrappers:**

```yaml
# Treat Foo::Bar as a TYPE path
rule:
  pattern:
    context: 'type __SgT = Foo::Bar;'
    selector: scoped_type_identifier
```

```yaml
# Treat Foo::Bar as an EXPRESSION path
rule:
  pattern:
    context: |
      fn __sg_ctx() {
        let _ = Foo::Bar;
      }
    selector: scoped_identifier
```

This is the single most reliable way to avoid “why did tree-sitter pick that kind?” surprises.

### 3.2.5 Multi-metavariables in Rust fragments (blocks, args, arms)

`$$$NAME` matches multiple AST nodes and extends until the next AST node in the pattern matches; it behaves “lazy/non-greedy-ish” in that sense. ([Ast Grep][3])

**Rust-specific advice:** always put a **sentinel node after `$$$`** if possible.

Bad (can over-capture inside blocks):

```bash
sg run -l rust -p 'fn $F() { $$$ }'
```

Better:

```bash
# Require a trailing statement shape to stop $$$ early
sg run -l rust -p 'fn $F() { $$$STMTS let $X = $Y; $$$REST }'
```

---

## 3.3 Strictness strategy for Rust (smart vs cst vs relaxed vs signature)

Strictness controls which nodes can be skipped during matching. ast-grep defines:

* `cst`: match *all* nodes (no skipping)
* `smart`: skip **unnamed** nodes in target code (default)
* `ast`: skip all unnamed nodes (pattern + target)
* `relaxed`: ignore comments + unnamed nodes
* `signature`: match only **named node kinds**; ignore text, comments, unnamed nodes ([Ast Grep][7])

### 3.3.1 Default recommendations for Rust rule authors

#### A) `smart` as your default

* Best for “real Rust structure” matches that should survive rustfmt.
* Avoids common false negatives caused by punctuation trivia while still respecting what you explicitly wrote. ([Ast Grep][7])

CLI:

```bash
sg run -l rust -p 'fn $NAME($$$ARGS) $$$RET { $$$BODY }' --strictness smart
```

#### B) `relaxed` when comments break your match

Rust frequently has inline comments inside args/tuples.
`relaxed` ignores comments (and unnamed nodes). ([Ast Grep][7])

```bash
sg run -l rust -p '$F($X)' --strictness relaxed
```

#### C) `signature` for “shape-only” queries (use sparingly)

`signature` ignores text content and matches only the **kinds** of named nodes—so it will match “same shape, different names.” ([Ast Grep][7])

Use it for things like:

* “any 2-arg function call”
* “any function signature with N parameters”
* “impl headers of the same structural form”

But constrain it with `inside/has/kind` filters or it will explode match count.

```bash
# "Any call with 2 args", regardless of callee name (shape-only)
sg run -l rust -p '$F($A, $B)' --strictness signature
```

#### D) `cst` only when you truly need token-level exactness

Use `cst` when punctuation/trailing commas/formatting are part of the intent (e.g., codemods in macro-heavy zones, or places where you want to *exclude* `foo(bar,)`). `cst` matches exact nodes and is the most brittle across formatting. ([Ast Grep][7])

### 3.3.2 How to set strictness (CLI vs pattern object)

* CLI: `--strictness <...>` ([Ast Grep][4])
* Pattern object: `pattern: { context, selector, strictness }` ([Ast Grep][2])

```yaml
rule:
  pattern:
    context: 'fn __sg_ctx() { $EXPR; }'
    selector: expression_statement
    strictness: relaxed
```

---

If you want the next section, the natural continuation is: **“Rust metavariable cookbook (patterns that actually work)”**—especially `$` interactions in `macro_rules!`, `$$VAR` for unnamed operator tokens, and how to pattern-match `generic_type_with_turbofish`, `scoped_identifier` vs `scoped_type_identifier`, and semicolon-sensitive expressions.

[1]: https://ast-grep.github.io/guide/pattern-syntax.html "Pattern Syntax | ast-grep"
[2]: https://ast-grep.github.io/reference/rule.html "Rule Object Reference | ast-grep"
[3]: https://ast-grep.github.io/advanced/pattern-parse.html "Deep Dive into ast-grep's Pattern Syntax | ast-grep"
[4]: https://ast-grep.github.io/reference/cli/run.html "ast-grep run | ast-grep"
[5]: https://docs.rs/crate/tree-sitter-rust/latest/source/src/node-types.json "tree-sitter-rust 0.24.0 - Docs.rs"
[6]: https://ast-grep.github.io/advanced/faq.html "Frequently Asked Questions | ast-grep"
[7]: https://ast-grep.github.io/advanced/match-algorithm.html "Deep Dive into ast-grep's Match Algorithm | ast-grep"

# 4) Rust item-level search cookbook (deep-dive chapters)

This chapter is a **Rust-native recipe pack** for ast-grep / ast-grep-py. Every recipe is structured as:

* **(a) Canonical node kinds / fields** (tree-sitter-rust “ABI”)
* **(b) Minimal robust patterns** (CLI + rule-object form you can drop into ast-grep-py)
* **(c) Common false positives/negatives**
* **(d) Recommended refinements** using `inside / has / follows / precedes` (+ `stopBy`, `field`) ([Ast Grep][1])

---

## 4.1 Modules and exports

### 4.1.a Canonical nodes / fields (stable ABI)

#### `mod_item`

* `fields.name: identifier` (required)
* `fields.body: declaration_list` (optional) — **present for inline `mod foo { ... }`, absent for file module `mod foo;`**
* `children: visibility_modifier` (optional) ([Docs.rs][2])

#### `use_declaration`

* `fields.argument` (required) is one of:
  `crate | identifier | scoped_identifier | scoped_use_list | self | super | use_as_clause | use_list | use_wildcard | metavariable`
* `children: visibility_modifier` (optional) ([Docs.rs][2])

#### `use_as_clause` (for `use path as alias`)

* `fields.path` is a path-like node (`crate | identifier | scoped_identifier | self | super | metavariable`)
* `fields.alias: identifier` (required) ([Docs.rs][2])

### 4.1.b Minimal robust patterns

#### Find all module declarations (both forms)

**CLI**

```bash
sg run -l rust -p 'mod $M;'
sg run -l rust -p 'mod $M { $$$ITEMS }'
```

**Rule-object (ast-grep-py) — prefer `kind` for completeness**

```python
rule = {"kind": "mod_item"}   # catches both `mod foo;` and `mod foo { ... }`
```

**Python extraction**

```python
for m in root.root().find_all(kind="mod_item"):
    name = m.field("name").text()
    is_inline = m.field("body") is not None  # inline vs file module
```

(That `body` optionality is part of the grammar contract.) ([Docs.rs][2])

#### Find public module declarations (and do it *precisely*)

**Pitfall:** `pattern: pub mod $M;` will often also match `pub(crate) mod $M;` under `smart` matching, because unnamed trivia in the target can be skipped. ([Ast Grep][3])

**Robust approach: regex on `visibility_modifier`**

```yaml
rule:
  all:
    - kind: mod_item
    - has:
        kind: visibility_modifier
        regex: '^pub$'      # exactly `pub`
        stopBy: end
```

(`has` + `stopBy` semantics are defined in relational rules.) ([Ast Grep][1])

To include all “pub-ish” visibilities:

```yaml
rule:
  all:
    - kind: mod_item
    - has:
        kind: visibility_modifier
        regex: '^pub'
        stopBy: end
```

#### Find all `pub use ...;` re-exports

**CLI**

```bash
sg run -l rust -p 'pub use $X;'
```

**Rule-object**

```yaml
rule:
  all:
    - kind: use_declaration
    - has:
        kind: visibility_modifier
        regex: '^pub'
        stopBy: end
```

### 4.1.c Re-export hygiene: “Avoid Duplicated Exports” (official catalog)

ast-grep’s Rust catalog includes a canonical rule that detects:

```rust
pub mod foo;
pub use foo::Foo;  // Foo exposed as both Foo and foo::Foo
```

The official YAML (key ideas: `inside` + `has` + `stopBy`) is: ([Ast Grep][4])

```yaml
rule:
  all:
    - pattern: pub use $B::$C;
    - inside:
        kind: source_file
        has:
          pattern: pub mod $A;
    - has:
        pattern: $A
        stopBy: end
```

**What it’s doing (operationally):**

* Target is the `pub use ...` node (`pattern: pub use $B::$C;`)
* Filter to cases where, somewhere in the same `source_file`, there exists `pub mod $A;` (`inside … has …`)
* Then require that the `pub use` path references that `$A` (`has: pattern: $A stopBy: end`)
  (`stopBy: end` makes `has` search deeper than direct children). ([Ast Grep][1])

**Turn this into a Rust “export surface” policy pack**

* Add variants for inline modules: `pub mod $A { $$$ }` (same idea, different `mod_item` form)
* Add variants for `use_as_clause` re-exports (`pub use foo::Foo as Bar;`) by matching `use_as_clause` via `use_declaration.argument` shapes. ([Docs.rs][2])

### 4.1.d Refinements you’ll reuse constantly

#### “Only top-level exports”

Use `inside: kind: source_file stopBy: end` to force “crate root / file-level” interpretation. `stopBy` semantics are defined by relational rules. ([Ast Grep][1])

#### “Only exports inside module X”

```yaml
rule:
  all:
    - kind: use_declaration
    - inside:
        kind: mod_item
        stopBy: end
        has:
          field: name
          pattern: $MOD
```

(Use `field` to target the `name` field of the `mod_item` node.) ([Ast Grep][1])

---

## 4.2 Functions

### 4.2.a Canonical nodes / fields

#### `function_item` (has a body)

* `fields.name: identifier | metavariable` (required)
* `fields.parameters: parameters` (required)
* `fields.return_type: _type` (optional)
* `fields.type_parameters: type_parameters` (optional)
* `fields.body: block` (required)
* `children`: `function_modifiers`, `visibility_modifier`, `where_clause` (optional) ([Docs.rs][2])

#### `function_signature_item` (no body; common in traits / extern blocks)

Same signature fields, same child set:

* `fields.name/parameters/return_type/type_parameters`
* `children`: `function_modifiers`, `visibility_modifier`, `where_clause` ([Docs.rs][2])

#### `function_modifiers`

Has (named) `extern_modifier` child; other keywords often live as unnamed tokens inside the modifier span, so you frequently filter it via `regex` on the node text. ([Docs.rs][2])

### 4.2.b Minimal robust patterns

#### Find all functions (including `pub(crate)`, `async`, etc.)

**CLI**

```bash
sg run -l rust -p 'fn $NAME($$$ARGS) $$$RET { $$$BODY }'
```

Because `smart` matching skips unnamed nodes in the *target*, this `fn ...` pattern commonly matches `async fn` too unless you require `async` explicitly. ([Ast Grep][3])

**Rule-object (best “shape” anchor)**

```yaml
rule:
  kind: function_item
```

#### Match async functions only

**CLI**

```bash
sg run -l rust -p 'async fn $NAME($$$ARGS) $$$RET { $$$BODY }'
```

This works because when you spell `async` in the pattern, it becomes required. ([Ast Grep][3])

#### Match unsafe functions only (catalog-grade approach)

The Rust catalog’s “Unsafe Function Without Unsafe Block” uses:

* `kind: function_item`
* `has: kind: function_modifiers regex: "^unsafe"`
* `not: has: kind: unsafe_block stopBy: end` ([Ast Grep][4])

```yaml
rule:
  all:
    - kind: function_item
    - has:
        kind: function_modifiers
        regex: '^unsafe'
    - not:
        has:
          kind: unsafe_block
          stopBy: end
```

That rule is *structurally* robust because it anchors on `function_item` and checks for an `unsafe_block` node, not just string searches. ([Ast Grep][4])

#### Extern ABI functions (two common Rust surfaces)

1. **Extern modifier on a normal function**

```yaml
rule:
  all:
    - kind: function_item
    - has:
        kind: function_modifiers
        stopBy: end
        has:
          kind: extern_modifier
          stopBy: end
```

(`function_modifiers` can contain an `extern_modifier` child.) ([Docs.rs][2])

2. **Functions declared inside `extern "C" { ... }` blocks**
   The extern block itself is `foreign_mod_item` with `extern_modifier` and optional `declaration_list` body. ([Docs.rs][2])
   Within those blocks you’ll often see `function_signature_item` (no body). ([Docs.rs][2])

### 4.2.c Common false positives/negatives

* **Modifier ambiguity under `smart`:** `fn $NAME(...)` will match `async fn` unless you require `async` explicitly. ([Ast Grep][3])
* **Visibility nuance:** `pub fn` pattern often matches `pub(crate) fn` unless you constrain via `visibility_modifier` text/regex.
* **Trait methods vs free functions:** methods inside `impl`/`trait` are still `function_item` nodes; if you want only free functions, constrain by `inside: kind: source_file` or `not: inside: kind: impl_item` etc. ([Ast Grep][1])

### 4.2.d High-ROI refinements (inside/has/precedes/follows)

* **Only methods (functions inside impl blocks):**

```yaml
rule:
  all:
    - kind: function_item
    - inside:
        kind: impl_item
        stopBy: end
```

([Ast Grep][1])

* **Only trait methods:**

```yaml
rule:
  all:
    - kind: function_item
    - inside:
        kind: trait_item
        stopBy: end
```

([Ast Grep][1])

* **“fn that follows an attribute”** (e.g., `#[test] fn ...`)
  Use `follows` + an attribute pattern; `follows` semantics are defined in relational rules. ([Ast Grep][1])

---

## 4.3 Structs / enums / unions

### 4.3.a Canonical nodes / fields

#### `struct_item`

* `fields.name: type_identifier` (required)
* `fields.body: field_declaration_list | ordered_field_declaration_list` (optional)
* `fields.type_parameters: type_parameters` (optional)
* `children: visibility_modifier | where_clause` (optional) ([Docs.rs][2])

#### `enum_item`

* `fields.name: type_identifier` (required)
* `fields.body: enum_variant_list` (required)
* `fields.type_parameters: type_parameters` (optional)
* `children: visibility_modifier | where_clause` (optional) ([Docs.rs][2])

#### `union_item`

* `fields.name: type_identifier` (required)
* `fields.body: field_declaration_list` (required)
* `fields.type_parameters: type_parameters` (optional)
* `children: visibility_modifier | where_clause` (optional) ([Docs.rs][2])

### 4.3.b Minimal robust patterns

#### Find all structs/enums/unions by kind

```yaml
# structs
rule: { kind: struct_item }

# enums
rule: { kind: enum_item }

# unions
rule: { kind: union_item }
```

#### Find public structs only (precise “pub” vs “pub(crate)”)

```yaml
rule:
  all:
    - kind: struct_item
    - has:
        kind: visibility_modifier
        regex: '^pub$'
        stopBy: end
```

(`visibility_modifier` is a named child of `struct_item`.) ([Docs.rs][2])

#### Tuple-struct vs record-struct (pattern first, then confirm with `--debug-query`)

```bash
# record
sg run -l rust -p 'struct $S { $$$FIELDS }'

# tuple
sg run -l rust -p 'struct $S($$$FIELDS);'
```

### 4.3.c Common false positives/negatives

* **Struct expressions vs struct items:** `Foo { ... }` is `struct_expression`, not `struct_item`; avoid pure `pattern` matching on the literal text and anchor with `kind: struct_item` to avoid mis-hits. ([Docs.rs][2])
* **Attributes are not children of `struct_item`:** `struct_item` children are visibility + where_clause only; attributes are separate `attribute_item` nodes. If you want “structs with #[derive(...)]”, use `follows` or match a wider parent (`_declaration_statement`) that “has” both. ([Docs.rs][2])

### 4.3.d Refinements you’ll reuse

* **“Only types declared in module X”:** use `inside: kind: mod_item` (same pattern as §4.1).
* **“Enum variants with attribute X”**: target nodes in `enum_variant_list` which can contain `attribute_item` siblings (you typically match the variant and `has`/`follows` the attribute). ([Docs.rs][2])

---

## 4.4 Traits + impl blocks

### 4.4.a Canonical nodes / fields

#### `trait_item`

* `fields.name: type_identifier` (required)
* `fields.body: declaration_list` (required)
* `fields.bounds: trait_bounds` (optional)
* `fields.type_parameters: type_parameters` (optional)
* `children: visibility_modifier | where_clause` (optional) ([Docs.rs][2])

#### `impl_item`

* `fields.type: _type` (required)
* `fields.trait: generic_type | scoped_type_identifier | type_identifier` (optional)  ← present for `impl Trait for Type`
* `fields.body: declaration_list` (optional)
* `fields.type_parameters: type_parameters` (optional)
* `children: where_clause` (optional) ([Docs.rs][2])

### 4.4.b Minimal robust patterns

#### Inherent impl vs trait impl

**Inherent impl (`impl Type { ... }`)**

```yaml
rule:
  all:
    - kind: impl_item
    - not:
        has:
          field: trait
          stopBy: end
          kind: type_identifier
```

The key is `field: trait` (trait field is optional; if it’s absent, `has` fails). `field` is a relational-rule option. ([Docs.rs][2])

**Trait impl (`impl Trait for Type { ... }`)**

```yaml
rule:
  all:
    - kind: impl_item
    - has:
        field: trait
        stopBy: end
        kind: type_identifier
```

(Use `generic_type` / `scoped_type_identifier` too if you want all trait-path shapes.) ([Docs.rs][2])

#### Methods inside impl blocks

```yaml
rule:
  all:
    - kind: function_item
    - inside:
        kind: impl_item
        stopBy: end
```

([Docs.rs][2])

#### Associated consts / associated types (inside trait/impl)

`const_item` is a named declaration statement subtype, and it can occur inside declaration lists (traits/impls). ([Docs.rs][2])

```yaml
rule:
  all:
    - kind: const_item
    - inside:
        any:
          - kind: trait_item
          - kind: impl_item
        stopBy: end
```

([Docs.rs][2])

### 4.4.c Common false positives/negatives

* **Macros generating impls/traits:** if you want “hand-written impls only,” add:

  ```yaml
  not:
    inside:
      kind: macro_invocation
      stopBy: end
  ```

  (`macro_invocation` is its own node kind with `token_tree` payload.) ([Docs.rs][2])

* **Impl headers are formatting-heavy:** if you match by `pattern` with lots of explicit tokens, consider leaving the header loose and extracting `field("trait")` / `field("type")` in Python instead of spelling the full header.

### 4.4.d Refinements

* Use `has` with `stopBy: end` when the node you need is not a *direct* child. (Default relational behavior is more shallow unless you change `stopBy`.) ([Ast Grep][1])
* Use `precedes` / `follows` for “method ordering conventions” (e.g., constructor should precede other methods). ([Ast Grep][1])

---

## 4.5 Type aliases, const/static items

### 4.5.a Canonical nodes / fields

#### `type_item` (type alias)

* `fields.name: type_identifier` (required)
* `fields.type: _type` (required)
* `fields.type_parameters: type_parameters` (optional)
* `children: visibility_modifier | where_clause` (optional) ([Docs.rs][2])

#### `const_item`

* `fields.name: identifier` (required)
* `fields.type: _type` (required)
* `fields.value: _expression` (optional) — enables `const X: T;` in traits
* `children: visibility_modifier` (optional) ([Docs.rs][2])

#### `static_item`

* `fields.name: identifier` (required)
* `fields.type: _type` (required)
* `fields.value: _expression` (optional)
* `children`: `mutable_specifier` (for `static mut`) and `visibility_modifier` (optional) ([Docs.rs][2])

### 4.5.b Minimal robust patterns

#### Find all type aliases

```bash
sg run -l rust -p 'type $T = $U;'
```

or structural:

```yaml
rule: { kind: type_item }
```

([Docs.rs][2])

#### Find “public type aliases” (exact `pub`)

```yaml
rule:
  all:
    - kind: type_item
    - has:
        kind: visibility_modifier
        regex: '^pub$'
        stopBy: end
```

([Docs.rs][2])

#### Find associated consts with no default value (trait API surface)

Because `const_item.value` is optional, you can separate “decl-only” consts by checking absence of value in Python, or by requiring that `value` doesn’t exist via `not: has: field: value …` (if you wrap the value check as a pattern). The optionality is in the grammar. ([Docs.rs][2])

#### Find `static mut`

```yaml
rule:
  all:
    - kind: static_item
    - has:
        kind: mutable_specifier
        stopBy: end
```

(`static_item` children include `mutable_specifier`.) ([Docs.rs][2])

### 4.5.c Common false positives/negatives

* **`const` keyword appears elsewhere** (e.g., `const fn`, `const { ... }` blocks). Anchor on `kind: const_item` to avoid matching `const_block` and friends. ([Docs.rs][2])
* **Visibility nuance again:** prefer `visibility_modifier` regex filters instead of spelling `pub(...)` forms in patterns if you care about exact export surface.

### 4.5.d Refinements

* **“Only top-level const/static/type aliases”:**

```yaml
inside:
  kind: source_file
  stopBy: end
```

([Ast Grep][1])

* **“Only associated consts in impls”**:

```yaml
rule:
  all:
    - kind: const_item
    - inside: { kind: impl_item, stopBy: end }
```

([Docs.rs][2])

---

### Next deep-dive after this (recommended)

**“Rust attribute + derive cookbook”** (how to reliably match `#[derive(...)]`, `#[cfg(...)]`, `#[test]`, `#[tokio::test]` using `attribute_item`/`attribute` nodes and `follows/precedes` patterns), because attributes are Rust’s #1 “not-a-child-node” gotcha. ([Docs.rs][2])

[1]: https://ast-grep.github.io/guide/rule-config/relational-rule.html "Relational Rules | ast-grep"
[2]: https://docs.rs/crate/tree-sitter-rust/latest/source/src/node-types.json "tree-sitter-rust 0.24.0 - Docs.rs"
[3]: https://ast-grep.github.io/advanced/match-algorithm.html?utm_source=chatgpt.com "Deep Dive into ast-grep's Match Algorithm"
[4]: https://ast-grep.github.io/catalog/rust/ "Rust | ast-grep"

# 5) Rust expression and control-flow cookbook

Rust is where “structural search beats regex” becomes *obvious*: `match` arms, `let`-chains, `.await`, and macro invocations all have **distinct node kinds + stable fields** in tree-sitter-rust, and ast-grep can key off those directly.

---

## 5.0 Standard workflow for expression rules (you will use this constantly)

### 5.0.1 Always validate the *effective matcher kind*

```bash
sg run -l rust -p 'fn __sg_ctx() { match x { _ => y } }' --selector match_expression --debug-query ast
sg run -l rust -p 'fn __sg_ctx() { foo().await; }'       --selector await_expression --debug-query ast
```

* `--selector` forces which node kind becomes the matcher.
* `--debug-query ast` shows what ast-grep actually parsed. ([Ast Grep][1])

### 5.0.2 Prefer “kind + field extraction” over spelling every token

For Rust, many “keywords” are *unnamed tokens* and can be skipped under `smart` strictness. If you need keyword-sensitive behavior, either:

* require the keyword in the pattern, **or**
* filter using `kind`/`fields` and (if needed) a tight `regex` on a small node like `function_modifiers`. ([Ast Grep][2])

---

## 5.1 Match ecosystem

### 5.1.a Canonical nodes / fields (stable ABI)

#### `match_expression`

* `fields.value: _expression` (required) — the scrutinee
* `fields.body: match_block` (required) ([Docs.rs][3])

#### `match_block`

* children: `match_arm*` ([Docs.rs][3])

#### `match_arm`

* `fields.pattern: match_pattern` (required)
* `fields.value: _expression` (required)
* children: `attribute_item?`, `inner_attribute_item?` ([Docs.rs][3])

#### `match_pattern` (this is where guards live)

* children: `_pattern` (required)
* `fields.condition` (optional) can be:

  * `_expression` (normal guard: `if cond`)
  * `let_condition` / `let_chain` (guard can contain `let`-forms) ([Docs.rs][3])

#### `_pattern` (important subtypes you’ll actually see)

Wildcard `_` is an **unnamed** subtype, and common structural subtypes include:
`identifier`, `scoped_identifier`, `struct_pattern`, `tuple_pattern`, `tuple_struct_pattern`, `slice_pattern`, `range_pattern`, `or_pattern`, `mut_pattern`, `ref_pattern`, `reference_pattern`, `captured_pattern`, `remaining_field_pattern`, `generic_pattern`, and even `macro_invocation`. ([Docs.rs][3])

---

### 5.1.b Minimal robust patterns

#### Find all match expressions

```bash
sg run -l rust -p 'fn __sg_ctx() { match $X { $$$ARMS } }' --selector match_expression
```

YAML / rule-object equivalent (ast-grep-py friendly):

```yaml
rule:
  kind: match_expression
```

#### Match a specific arm shape (per-arm matching)

If you want *arms* as the returned matches (not whole `match`):

```bash
sg run -l rust -p 'fn __sg_ctx() { match $X { $P => $Y, } }' --selector match_arm
```

#### Match guarded arms (`pat if cond => expr`)

```bash
sg run -l rust -p 'fn __sg_ctx() { match $X { $P if $COND => $Y, } }' --selector match_arm
```

> In the AST, that `if $COND` becomes `match_pattern.condition`. ([Docs.rs][3])

#### Match multi-pattern arms (`A | B => ...`)

```bash
sg run -l rust -p 'fn __sg_ctx() { match $X { $A | $B => $Y, } }' --selector match_arm
```

This works because `_pattern` includes `or_pattern`. ([Docs.rs][3])

---

### 5.1.c Common false positives / negatives

* **Guard detection by text is brittle**: prefer `match_pattern.condition` existence (Python) or a structural guard pattern as above. `match_pattern.condition` explicitly allows `_expression`, `let_condition`, `let_chain`. ([Docs.rs][3])
* **Wildcard `_` is an unnamed subtype**. If you insist on “must contain a `_ => ...` arm”, you’ll usually do it via a pattern snippet rather than `kind` checks (since `_` isn’t a named node). ([Docs.rs][3])
* **Macros can appear in patterns** (`_pattern` includes `macro_invocation`). If you search for `macro_invocation` globally, you’ll catch *pattern macros* too. ([Docs.rs][3])

---

### 5.1.d High-ROI refinements (`inside / has / follows / precedes`)

Relational rules are first-class: `inside`, `has`, `follows`, `precedes` + `stopBy` + `field`. ([Ast Grep][4])

#### “Only match arms inside matches inside functions named X”

```yaml
rule:
  all:
    - kind: match_arm
    - inside:
        kind: match_expression
        stopBy: end
    - inside:
        kind: function_item
        stopBy: end
        has:
          field: name
          pattern: my_fn
```

(`field` and `stopBy` semantics come from relational rules.) ([Ast Grep][5])

#### “Find guarded arms”

```yaml
rule:
  all:
    - kind: match_arm
    - has:
        field: pattern
        stopBy: end
        has:
          field: condition
          stopBy: end
          kind: _expression
```

(You can widen `kind` to `let_condition` / `let_chain` if you want `let`-guards too.) ([Docs.rs][3])

---

## 5.2 Let-chains / if-let / while-let

### 5.2.a Canonical nodes / fields

#### `if_expression`

* `fields.condition` can be `_expression`, `let_condition`, or `let_chain` ([Docs.rs][3])

#### `while_expression`

* `fields.condition` can be `_expression`, `let_condition`, or `let_chain` ([Docs.rs][3])

#### `let_condition`

* `fields.pattern: _pattern` (required)
* `fields.value: _expression` (required) ([Docs.rs][3])

#### `let_chain`

* children: one or more of (`_expression`, `let_condition`) and is **required true + multiple true** (i.e., it’s a sequence container) ([Docs.rs][3])

---

### 5.2.b Minimal robust patterns

#### `if let PAT = EXPR { ... }`

```bash
sg run -l rust -p 'fn __sg_ctx() { if let $P = $E { $$$ } }' --selector if_expression
```

If you want the *let condition itself* as the match:

```bash
sg run -l rust -p 'fn __sg_ctx() { if let $P = $E { $$$ } }' --selector let_condition
```

#### `while let PAT = EXPR { ... }`

```bash
sg run -l rust -p 'fn __sg_ctx() { while let $P = $E { $$$ } }' --selector while_expression
```

#### Let-chains (`if let ... && ...`)

You usually want to target the `let_chain` node directly:

```bash
sg run -l rust -p 'fn __sg_ctx() { if let $P = $E && $C { $$$ } }' --selector let_chain
```

---

### 5.2.c Common false positives / negatives

* Searching `let_condition` alone will catch:

  * `if let` / `while let`
  * **match guards** (`match_pattern.condition` supports `let_condition`/`let_chain`) ([Docs.rs][3])
    If you mean only control-flow conditions, constrain with `inside: if_expression|while_expression`.
* Let-chains are a *container node* (`let_chain`) that mixes `_expression` and `let_condition` children—don’t assume a fixed “child index = first let”. ([Docs.rs][3])

---

### 5.2.d Refinements

#### “Only `if let` / `while let` (exclude match guards)”

```yaml
rule:
  all:
    - kind: let_condition
    - inside:
        any:
          - kind: if_expression
          - kind: while_expression
        stopBy: end
```

([Docs.rs][3])

#### “Find let-chains and extract each let_condition (Python workflow)”

* Match: `kind="let_chain"`
* Then in Python, iterate `lc.find_all(kind="let_condition")` and read `pattern/value` fields from each `let_condition`. (Those fields are stable.) ([Docs.rs][3])

---

## 5.3 Call vs macro call vs method call

### 5.3.a Canonical nodes / fields

#### Function/method calls: `call_expression`

* `fields.arguments: arguments` (required)
* `fields.function: <many expression types>` (required) — notably includes:

  * `identifier`, `scoped_identifier` (free function / path)
  * `field_expression` (method-like: `recv.method`)
  * `generic_function` (turbofish calls) ([Docs.rs][3])

#### Method selector: `field_expression`

* `fields.value: _expression` (receiver)
* `fields.field: field_identifier | integer_literal` (member name / tuple index) ([Docs.rs][3])

#### Turbofish call head: `generic_function`

* `fields.function: field_expression | identifier | scoped_identifier` (required)
* `fields.type_arguments: type_arguments` (required) ([Docs.rs][3])

#### Macros: `macro_invocation`

* `fields.macro: identifier | scoped_identifier` (required)
* children: `token_tree` (required) ([Docs.rs][3])

---

### 5.3.b Minimal robust patterns

#### Find all calls (includes free fn calls + method calls + turbofish)

```yaml
rule: { kind: call_expression }
```

([Docs.rs][3])

#### Free function calls only (structural filter)

**YAML idea:** keep `call_expression` where the `function` field is an identifier/path (and not a `field_expression`).

```yaml
rule:
  all:
    - kind: call_expression
    - has:
        field: function
        stopBy: end
        any:
          - kind: identifier
          - kind: scoped_identifier
          - kind: generic_function
```

Then, if you include `generic_function`, further require its inner `function` is *not* `field_expression` when you want “turbofish free fn” only. ([Docs.rs][3])

#### Method calls only (`recv.method(args)` and `recv.method::<T>(args)`)

```yaml
rule:
  all:
    - kind: call_expression
    - has:
        field: function
        stopBy: end
        any:
          - kind: field_expression
          - all:
              - kind: generic_function
              - has:
                  field: function
                  stopBy: end
                  kind: field_expression
```

([Docs.rs][3])

#### Macro invocations (e.g., `println!`, `format!`)

```yaml
rule: { kind: macro_invocation }
```

([Docs.rs][3])

If you only want macro *statements* (not type/pattern macros), constrain to `expression_statement`:

```yaml
rule:
  all:
    - kind: macro_invocation
    - inside:
        kind: expression_statement
        stopBy: end
```

(`source_file` can contain `expression_statement`, so this is the standard “statement macro” filter.) ([Docs.rs][3])

---

### 5.3.c Common false positives / negatives

* **Method calls are still `call_expression`** — there is no dedicated “method_call_expression” node; the distinction is `call_expression.function == field_expression` (or `generic_function(function=field_expression)`). ([Docs.rs][3])
* `macro_invocation` shows up in expressions, patterns, and types (it’s a subtype of `_pattern` and `_type`), so “find macros” may be broader than you expect. ([Docs.rs][3])

---

### 5.3.d High-ROI refinement: extract receiver + method name (Python)

For method calls:

* `call = <call_expression>`
* `fun = call.field("function")`
* if `fun.kind() == "field_expression"`:

  * receiver: `fun.field("value")`
  * method: `fun.field("field")` ([Docs.rs][3])

For turbofish method calls:

* `fun.kind() == "generic_function"`
* inner = `fun.field("function")` (may be `field_expression`) ([Docs.rs][3])

---

## 5.4 Closures, async blocks, awaits

### 5.4.a Canonical nodes / fields

#### `closure_expression`

* `fields.parameters: closure_parameters` (required)
* `fields.body: _expression | "_"` (required) — body can be an expression or placeholder `_`
* `fields.return_type: _type` (optional) ([Docs.rs][3])

#### `async_block`

* children: `block` (required) ([Docs.rs][3])

#### `await_expression`

* children: `_expression` (required) — the awaited expression ([Docs.rs][3])

---

### 5.4.b Minimal robust patterns

#### Match closures (return closure nodes)

```bash
sg run -l rust -p 'fn __sg_ctx() { let _ = |$$$P| $BODY; }' --selector closure_expression
```

Require `move` closure:

```bash
sg run -l rust -p 'fn __sg_ctx() { let _ = move |$$$P| $BODY; }' --selector closure_expression
```

(If you omit `move`, `smart` matching may still match move closures; include it when it matters.) ([Ast Grep][2])

#### Match async blocks

```bash
sg run -l rust -p 'fn __sg_ctx() { let _ = async { $$$ }; }' --selector async_block
```

([Docs.rs][3])

#### Match awaits (return await nodes)

```bash
sg run -l rust -p 'fn __sg_ctx() { $X.await; }' --selector await_expression
```

([Docs.rs][3])

---

### 5.4.c Common false positives / negatives

* `await_expression` is a postfix construct with a single `_expression` child. If you try to match `.await` using regex, you’ll hit comments/strings; match the node kind instead. ([Docs.rs][3])
* Closure bodies can be expression or block; treat `closure_expression.body` as “any `_expression`” (block is an expression in Rust) rather than assuming braces. ([Docs.rs][3])

---

### 5.4.d Refinements (control-flow + async)

#### “Find awaits inside loops”

```yaml
rule:
  all:
    - kind: await_expression
    - inside:
        any:
          - kind: for_expression
          - kind: while_expression
          - kind: loop_expression
        stopBy: end
```

([Docs.rs][3])

#### “Async blocks that contain awaits”

```yaml
rule:
  all:
    - kind: async_block
    - has:
        kind: await_expression
        stopBy: end
```

([Docs.rs][3])

---

## 5.5 Unsafe surfaces

### 5.5.a Canonical nodes / fields

#### `unsafe_block`

* children: `block` (required) ([Docs.rs][3])

(“Unsafe function” is not its own kind; it’s detected via `function_modifiers` text/structure around `function_item`, as in the Rust catalog example.) ([Ast Grep][6])

---

### 5.5.b Minimal robust patterns

#### Find all unsafe blocks

```yaml
rule: { kind: unsafe_block }
```

([Docs.rs][3])

#### Official catalog: “unsafe function without unsafe block”

From the Rust rule catalog (key pieces: `function_item` + `function_modifiers` regex + absence of `unsafe_block`). ([Ast Grep][6])

```yaml
rule:
  all:
    - kind: function_item
    - has:
        kind: function_modifiers
        regex: '^unsafe'
    - not:
        has:
          kind: unsafe_block
          stopBy: end
```

(Uses `has`/`not` relational filtering; `stopBy: end` makes descendant search deep.) ([Ast Grep][4])

---

### 5.5.c Common false positives / negatives

* This rule is intentionally *syntactic*: it doesn’t prove the function is “actually safe,” only that there’s no `unsafe {}` block. That’s exactly what the catalog rule claims. ([Ast Grep][6])
* Unsafe blocks can exist inside closures/macro expansions textually; tree-sitter sees only surface syntax—no macro expansion.

---

### 5.5.d Refinements

#### “Unsafe blocks inside *safe* functions”

```yaml
rule:
  all:
    - kind: unsafe_block
    - inside:
        kind: function_item
        stopBy: end
    - not:
        inside:
          has:
            kind: function_modifiers
            regex: '^unsafe'
          stopBy: end
```

(First constrain the unsafe block to be inside a function; then exclude unsafe-marked functions.) ([Docs.rs][3])

---

If you want the next chapter after this, the most natural continuation is: **“Rust attributes + derive cookbook”** (because attributes are siblings/preceding nodes, and most “real” Rust queries become “match item X that follows attribute Y”).

[1]: https://ast-grep.github.io/reference/cli/run.html?utm_source=chatgpt.com "ast-grep run"
[2]: https://ast-grep.github.io/advanced/match-algorithm.html?utm_source=chatgpt.com "Deep Dive into ast-grep's Match Algorithm"
[3]: https://docs.rs/crate/tree-sitter-rust/latest/source/src/node-types.json "tree-sitter-rust 0.24.0 - Docs.rs"
[4]: https://ast-grep.github.io/guide/rule-config/relational-rule.html?utm_source=chatgpt.com "Relational Rules"
[5]: https://ast-grep.github.io/reference/rule.html?utm_source=chatgpt.com "Rule Object Reference"
[6]: https://ast-grep.github.io/catalog/rust/?utm_source=chatgpt.com "Rust"

# 6) Rust type-system surface: generics, lifetimes, trait bounds

Rust’s “type surface” is *highly* structured in tree-sitter-rust, and you get huge leverage by anchoring on a small set of node kinds that behave like a **stable ABI**.

---

## 6.0 Core ABI anchors (types + bounds)

### 6.0.1 `_type` is the hub (treat this as your “type root”)

In tree-sitter-rust, `_type` is a **supertype** whose subtypes include (among others):

* `abstract_type` (used for `impl Trait`-style abstract types)
* `dynamic_type` (used for `dyn Trait`-style dynamic types)
* `bounded_type` (the “A + B + 'a” bound-list form)
* `pointer_type` (`*mut T` / `*const T`)
* `reference_type` (`&'a T` / `&mut T`)
* plus many others (arrays, generics, tuples, etc.) ([Docs.rs][1])

This means: if your query is “find types shaped like X”, start with `kind: _type` and then refine to a subtype, not `regex` over type text.

---

## 6.0.2 Your “fragment parsing” standard (required for type/bound queries)

### Rule schema you should standardize on

`pattern` can be a **string** or an **object** `{ context, selector, strictness? }`. The object form is the canonical way to make fragments parse and then “pluck out” the matcher node. ([Ast Grep][2])

### CLI equivalents you’ll use constantly

`sg run` supports:

* `-p/--pattern <PATTERN>`
* `-l/--lang <LANG>`
* `--selector <KIND>` (extract sub-part of pattern to match)
* `--debug-query[=<format>]` (prints query pattern’s AST; **requires lang set explicitly**)
* `--strictness <STRICTNESS>` ([Ast Grep][3])

**Debug loop template**

```bash
sg run -l rust -p 'type __SgT = &\'a T;' --selector reference_type --debug-query
```

---

# 6.1 Generics and lifetimes

## 6.1.a Canonical nodes/fields (stable ABI)

### `lifetime` (the `'a` atom)

* kind: `lifetime`
* children: exactly one `identifier` (required) ([Docs.rs][1])

### `lifetime_parameter` (generic lifetime param: `<'a: 'b + ...>`)

* fields:

  * `name: lifetime` (required)
  * `bounds: trait_bounds` (optional) ([Docs.rs][1])

### `type_parameter` (generic type param: `<T: Trait = Default>`)

* fields:

  * `name: type_identifier` (required)
  * `bounds: trait_bounds` (optional)
  * `default_type: _type` (optional) ([Docs.rs][1])

### `type_parameters` (the `<...>` list)

* children (multiple, required): can include

  * `lifetime_parameter`, `type_parameter`, `const_parameter`, `attribute_item`, `metavariable` ([Docs.rs][1])

### `reference_type` (borrow types: `&'a T`, `&mut T`)

* fields:

  * `type: _type` (required)
* children (multiple, optional): `lifetime`, `mutable_specifier` ([Docs.rs][1])

### `higher_ranked_trait_bound` (HRTB: `for<'a> Trait`)

* fields:

  * `type_parameters: type_parameters` (required)
  * `type: _type` (required) ([Docs.rs][1])

---

## 6.1.b Parse-safe fragment contexts (copy/paste)

### Match *lifetime parameter* nodes robustly

You want the `lifetime_parameter` node, not the whole `type_parameters` list.

**YAML (rule object)**

```yaml
rule:
  pattern:
    context: "fn __sg_ctx<'a>() {}"
    selector: lifetime_parameter
```

(pattern object + selector is defined by the rule schema) ([Ast Grep][2])

**CLI**

```bash
sg run -l rust -p "fn __sg_ctx<'a>() {}" --selector lifetime_parameter --debug-query
```

(`--selector` / `--debug-query` documented in CLI reference) ([Ast Grep][3])

### Match *reference types with explicit lifetime*

**YAML**

```yaml
rule:
  pattern:
    context: "type __SgT = &'a T;"
    selector: reference_type
```

Why `reference_type` works: it explicitly models `&...` and can include a `lifetime` child. ([Docs.rs][1])

---

## 6.1.c Minimal robust searches

### Find all lifetime *parameters*

```yaml
rule: { kind: lifetime_parameter }
```

([Docs.rs][1])

### Find all *explicit* lifetimes used in references (`&'a T`)

```yaml
rule:
  all:
    - kind: reference_type
    - has:
        kind: lifetime
        stopBy: end
```

(`reference_type` children can include `lifetime`; `has/stopBy` semantics are defined in rule reference) ([Docs.rs][1])

### Find HRTBs (`for<'a> ...`) anywhere in bounds/types

```yaml
rule: { kind: higher_ranked_trait_bound }
```

([Docs.rs][1])

---

## 6.1.d Common pitfalls (Rust-specific)

* **Elided lifetimes do not exist as `lifetime` nodes**. If the code is `&T` with elision, there is no `'a` token and no `lifetime` node to match—so queries must target `reference_type` + “has lifetime” to find only *explicit* lifetimes. ([Docs.rs][1])
* **Lifetime bounds use `trait_bounds`**, so don’t assume “bounds == only types”. `trait_bounds` can include `lifetime` nodes. ([Docs.rs][1])

---

# 6.2 Trait bounds + where clauses

## 6.2.a Canonical nodes/fields

### `trait_bounds` (the `: Trait1 + Trait2 + 'a` list)

* children (multiple, required):

  * `_type`
  * `higher_ranked_trait_bound`
  * `lifetime` ([Docs.rs][1])

### `where_clause` / `where_predicate`

* `where_clause` children: `where_predicate*` ([Docs.rs][1])
* `where_predicate` fields:

  * `left` (required): supports many type-ish forms **including** `reference_type`, `pointer_type`, and even `lifetime`
  * `bounds: trait_bounds` (required) ([Docs.rs][1])

---

## 6.2.b Matching “T: Trait” in different syntactic positions

### Case 1: Inline generic bounds (`fn f<T: Trait>()`)

Target the **parameter node**: `type_parameter` with `bounds`.

```yaml
rule:
  all:
    - kind: type_parameter
    - has:
        field: bounds
        kind: trait_bounds
        stopBy: end
```

* `type_parameter.bounds` is a `trait_bounds` node when present. ([Docs.rs][1])
* `has.field` and `stopBy` are defined for relational rules. ([Ast Grep][2])

### Case 2: Where-clause bounds (`where T: Trait`)

Target `where_predicate` nodes, then inspect `left` and `bounds`.

```yaml
rule:
  kind: where_predicate
```

To narrow to the “type-name on the left” style:

```yaml
rule:
  all:
    - kind: where_predicate
    - has:
        field: left
        kind: type_identifier
        stopBy: end
```

`where_predicate.left` explicitly allows `type_identifier` and many other type forms. ([Docs.rs][1])

### Case 3: Lifetime outlives bounds in where clauses (`where 'a: 'b`)

Because `where_predicate.left` can be a `lifetime`, and `bounds` is `trait_bounds` which can contain lifetimes:

```yaml
rule:
  all:
    - kind: where_predicate
    - has:
        field: left
        kind: lifetime
        stopBy: end
    - has:
        field: bounds
        has:
          kind: lifetime
          stopBy: end
        stopBy: end
```

This is mechanically justified by the grammar: `where_predicate.left` can be `lifetime`, and `trait_bounds` can contain `lifetime`. ([Docs.rs][1])

---

## 6.2.c Common false positives/negatives

* If you match `trait_bounds` directly, you’ll hit:

  * type parameter bounds,
  * lifetime parameter bounds,
  * where predicates,
  * trait object / impl trait bound lists (via `bounded_type`, below).
    So **always anchor the container** (`type_parameter`, `where_predicate`, etc.) and then descend into `bounds`. ([Docs.rs][1])
* `where_predicate.left` supports many shapes (reference, pointer, lifetime, generic types). Don’t overfit to only `type_identifier` unless that’s your intent. ([Docs.rs][1])

---

# 6.3 `impl Trait`, `dyn` types, references, pointers (formatting-robust)

## 6.3.a Canonical nodes/fields

### `abstract_type` (the “impl Trait” node)

* fields:

  * `trait` (required): can be `type_identifier`, `generic_type`, `bounded_type`, etc.
* children:

  * optional `type_parameters` ([Docs.rs][1])

### `dynamic_type` (the “dyn Trait” node)

* fields:

  * `trait` (required): can be `type_identifier`, `generic_type`, `higher_ranked_trait_bound`, etc. ([Docs.rs][1])
* note: the `dyn` keyword itself is an **unnamed** token in the grammar; the named node you match is `dynamic_type`. ([Docs.rs][1])

### `bounded_type` (the `A + B + 'a` form)

* children (multiple, required):

  * `_type`
  * `lifetime`
  * `use_bounds` ([Docs.rs][1])

This is the node you use to catch:

* `impl Trait + Send + 'a`
* `dyn Trait + Send + 'a`
* etc. (i.e., anything with `+` bounds), because it’s explicitly modeled as a multi-child list. ([Docs.rs][1])

### `pointer_type` / `reference_type`

* `pointer_type`: field `type: _type`; optional child `mutable_specifier` ([Docs.rs][1])
* `reference_type`: field `type: _type`; optional children `lifetime`, `mutable_specifier` ([Docs.rs][1])

---

## 6.3.b Minimal robust patterns (with parse-safe wrappers)

### Find `impl Trait` occurrences (match `abstract_type`)

```bash
sg run -l rust -p 'fn __sg_ctx() -> impl Trait { todo!() }' --selector abstract_type
```

(`--selector` is the supported way to extract the matcher node) ([Ast Grep][3])

### Find `dyn Trait` occurrences (match `dynamic_type`)

```bash
sg run -l rust -p 'type __SgT = Box<dyn Trait>;' --selector dynamic_type
```

(`dynamic_type` is the named node for dyn types) ([Docs.rs][1])

### Find “dyn/impl with `+` bounds” (match `bounded_type`)

**Broad bound-list finder**

```yaml
rule: { kind: bounded_type }
```

([Docs.rs][1])

**Only bound-lists that include a dyn type**

```yaml
rule:
  all:
    - kind: bounded_type
    - has:
        kind: dynamic_type
        stopBy: end
```

(`bounded_type` can contain `_type` children; `_type` includes `dynamic_type` as a subtype) ([Docs.rs][1])

### Reference types: `&'a T` / `&mut T`

```bash
sg run -l rust -p "type __SgT = &'a T;" --selector reference_type --debug-query
sg run -l rust -p "type __SgT = &mut T;" --selector reference_type --debug-query
```

(`reference_type` can contain `lifetime` and/or `mutable_specifier`) ([Docs.rs][1])

### Pointer types: `*mut T` / `*const T`

```bash
sg run -l rust -p "type __SgT = *mut T;"   --selector pointer_type --debug-query
sg run -l rust -p "type __SgT = *const T;" --selector pointer_type --debug-query
```

(`pointer_type` has `_type` field and optional `mutable_specifier`) ([Docs.rs][1])

---

## 6.3.c Common pitfalls

* If you only search `abstract_type` and `dynamic_type`, you will miss `impl/dyn` uses that add `+ Send + 'a` because the “plus list” is represented as `bounded_type`. ([Docs.rs][1])
* If you need to match “only explicit lifetime references,” require `reference_type has lifetime` (otherwise you’ll include elided lifetimes). ([Docs.rs][1])

---

# 7) Rust attributes + derive cookbook

Rust is attribute-heavy, and the key modeling detail is:

> Attributes are **their own nodes** (`attribute_item` / `inner_attribute_item`) that contain an `attribute` node, and you usually attach them to items via **sibling relations** (`follows` / `precedes`).

---

## 7.1 Canonical nodes/fields (stable ABI)

### `attribute_item` (outer attribute: `#[...]`)

* children: exactly one `attribute` (required) ([Docs.rs][1])

### `inner_attribute_item` (inner attribute: `#![...]`)

* children: exactly one `attribute` (required) ([Docs.rs][1])

### `attribute` (the payload)

* fields:

  * `arguments: token_tree` (optional) — e.g., `#[derive(...)]`
  * `value: _expression` (optional) — e.g., name/value forms ([Docs.rs][1])
* children (required): the *attribute path head* can be

  * `identifier`, `scoped_identifier`, `crate`, `self`, `super`, etc. ([Docs.rs][1])

**Implication:** `#[tokio::test]` and `#[test]` differ structurally because the `attribute`’s child can be `scoped_identifier` vs `identifier`. ([Docs.rs][1])

---

## 7.2 Parse-safe fragment contexts for attributes (standardize these)

Because `pattern` supports `{ context, selector, strictness? }`, you can always make attribute snippets parse and then select `attribute_item` / `inner_attribute_item`. ([Ast Grep][2])

### Outer attribute selector template

```yaml
pattern:
  context: |
    #[derive($$$DERIVES)]
    struct __SgT;
  selector: attribute_item
```

(Select `attribute_item`, not the struct.) ([Docs.rs][1])

### Inner attribute selector template

```yaml
pattern:
  context: |
    mod __sg_mod {
      #![allow(dead_code)]
    }
  selector: inner_attribute_item
```

([Docs.rs][1])

---

## 7.3 “Items that follow attribute Y” (the workhorse pattern)

Relational rules:

* `follows` / `precedes` accept a sub-rule and support `stopBy` (but **no `field`**) ([Ast Grep][2])

### Structs that have a derive (directly adjacent)

```yaml
rule:
  all:
    - kind: struct_item
    - follows:
        stopBy: neighbor
        pattern:
          context: |
            #[derive($$$DERIVES)]
            struct __SgT;
          selector: attribute_item
```

* Using `stopBy: neighbor` means “immediately preceded by”. ([Ast Grep][2])
* `#[derive(...)]` lives inside `attribute.arguments` (a `token_tree`), so capturing the whole list as `$$$DERIVES` is the stable move. ([Docs.rs][1])

### Functions that are `#[test]`

```yaml
rule:
  all:
    - kind: function_item
    - follows:
        stopBy: neighbor
        pattern:
          context: |
            #[test]
            fn __SgT() {}
          selector: attribute_item
```

### Async tests: `#[tokio::test] async fn ...`

```yaml
rule:
  all:
    - kind: function_item
    - follows:
        stopBy: neighbor
        pattern:
          context: |
            #[tokio::test]
            async fn __SgT() {}
          selector: attribute_item
```

(You’re matching the attribute structurally; the function modifiers are a separate concern.) ([Docs.rs][1])

---

## 7.4 Filtering attribute arguments (derive membership) without overfitting

Attribute args are token trees; tree-sitter does not “type” them like normal expressions. Your stable options are:

### Option A (preferred): capture the token_tree and post-filter in Python

* match: the `attribute_item` with a derive
* extract: `attribute.arguments` token tree text
* test: membership (`Serialize`, `Debug`, etc.) in your own logic

Why this is stable: `attribute.arguments` is explicitly a `token_tree` field. ([Docs.rs][1])

### Option B (rule-only): anchored regex on the attribute node text

Rule reference: `regex` matches the **whole node text** (so use `^...$` + `.*` as needed). ([Ast Grep][2])

Example: derive contains `Serialize` (allow multiline)

```yaml
rule:
  all:
    - kind: attribute_item
    - regex: '(?s)^#\\[derive\\(.*\\bSerialize\\b.*\\)\\]$'
```

Then attach it to items via `follows` as in §7.3. ([Ast Grep][2])

*(If you rely on regex, keep it anchored to `kind: attribute_item` so you’re not grepping arbitrary node text.)* ([Ast Grep][2])

---

## 7.5 Inner attributes: crate/module policies (`#![...]`)

To find crate-level lint policy:

```yaml
rule:
  pattern:
    context: |
      mod __sg_mod {
        #![deny($$$LINTS)]
      }
    selector: inner_attribute_item
```

Inner attributes are a distinct kind, not `attribute_item`. ([Docs.rs][1])

---

## 7.6 Practical correctness boundary: “attachment” means “adjacent”

Using `follows stopBy: neighbor` models “the attribute is attached to this item” in the usual Rust formatting style (attribute immediately preceding item). If you intentionally allow doc comments or other nodes between, you must widen `stopBy`—but that increases false positives because you’ll start matching non-attached attributes earlier in the sibling list. (`stopBy` behavior is explicitly defined.) ([Ast Grep][2])

[1]: https://docs.rs/crate/tree-sitter-rust/0.24.0/source/src/node-types.json "https://docs.rs/crate/tree-sitter-rust/0.24.0/source/src/node-types.json"
[2]: https://ast-grep.github.io/reference/rule.html "https://ast-grep.github.io/reference/rule.html"
[3]: https://ast-grep.github.io/reference/cli.html "https://ast-grep.github.io/reference/cli.html"

# 8) Attribute-driven Rust querying

Rust attributes are *the* high-leverage handle for “what code means” (cfg gates, derives, tests, lint scopes). The trick is to treat attributes as **first-class structural nodes**, not “text above a declaration”.

---

## 8.0 The contract: Rust semantics ↔ tree-sitter nodes ↔ ast-grep relations

### Rust semantics you must model

* **Outer attributes** `#[…]` apply to the thing that **follows** the attribute.
* **Inner attributes** `#![…]` apply to the item **declared within** (scope: crate/module/block/etc.). ([Rust Documentation][1])

Rust also allows attributes on many surfaces beyond items: match arms, fields, generic params, parameters, some statements/expressions, etc. ([Rust Documentation][1])

### Tree-sitter “ABI” nodes (what ast-grep will actually see)

* `attribute_item` → contains exactly one `attribute` child. ([Docs.rs][2])
* `inner_attribute_item` → contains exactly one `attribute` child. ([Docs.rs][2])
* `attribute` has:

  * `fields.arguments?: token_tree`
  * `fields.value?: _expression`
  * children for the *path head*: `identifier | scoped_identifier | crate | self | super | …` ([Docs.rs][2])
* `token_tree` is a free-form container (nested token trees, identifiers, literals, etc.). ([Docs.rs][2])

### ast-grep relational rules are the glue

* Use `follows` / `precedes` to model “outer attribute attaches to following node”.
* Use `inside` / `has` to model “inner attribute exists within a scope” or “this node literally contains attribute children”.
* `stopBy` is your precision knob: `"neighbor"` (default), `"end"`, or a **rule object** boundary. ([ast-grep.github.io][3])

---

## 8.1 Outer vs inner attributes

### 8.1.1 Outer attributes (`#[…]`) → “item follows attribute_item”

**Structural anchor**

* `attribute_item` is the node for an outer attribute. ([Docs.rs][2])

**Debug pattern parsing (CLI)**

```bash
# Inspect the attribute AST itself
sg run -l rust -p '#[cfg(test)] fn __x() {}' --selector attribute_item --debug-query ast
```

`--selector` and pattern objects exist specifically to make “fragment contexts” parseable and then select the intended subnode. ([ast-grep.github.io][4])

**Attach attribute → function**

```yaml
rule:
  all:
    - kind: function_item
    - follows:
        stopBy: neighbor
        pattern:
          context: |
            #[cfg(test)]
            fn __x() {}
          selector: attribute_item
```

* `stopBy: neighbor` means “immediately preceded by” in sibling order (the usual Rust “attached attribute” layout). ([ast-grep.github.io][3])
* Pattern object `{context, selector}` is the canonical way to match an attribute fragment. ([ast-grep.github.io][4])

**When outer attributes are not siblings**
Some nodes *contain* attributes as children (match arms, for example), so `has` is cleaner than `follows` there (see §8.2.5). Match arms explicitly allow both `attribute_item` and `inner_attribute_item` as children. ([Docs.rs][2])

---

### 8.1.2 Inner attributes (`#![…]`) → “inner_attribute_item inside scope”

**Structural anchor**

* `inner_attribute_item` contains an `attribute` child. ([Docs.rs][2])

**Crate / module policy lints**
Rust defines that inner attributes apply to the enclosing item and are written `#![...]`. ([Rust Documentation][1])

Examples to find:

* crate/module lints: `#![allow(...)]`, `#![deny(...)]`
* crate metadata: `#![crate_type = "lib"]` (uses attribute `value`) ([Rust Documentation][1])

**Find all inner attributes in a file/module**

```yaml
rule:
  kind: inner_attribute_item
```

**Find crate-level inner attributes (top-level only)**

```yaml
rule:
  all:
    - kind: inner_attribute_item
    - inside:
        kind: source_file
        stopBy: end
```

(`inside`/`stopBy` semantics are defined for relational rules.) ([ast-grep.github.io][5])

---

## 8.2 Common Rust attribute categories (cookbook)

### 8.2.1 `#[derive(...)]` (outer attribute; usually attached to types)

**Reality check**

* Tree-sitter exposes the derive argument list as `attribute.fields.arguments: token_tree`. ([Docs.rs][2])
  So “derive contains Serialize” is usually a **two-phase** operation:

1. structural match `#[derive(...)]`
2. optional regex/text filter on that attribute node (or post-filter in Python)

**Match derive attributes**

```yaml
rule:
  pattern:
    context: |
      #[derive($$$DERIVES)]
      struct __SgT;
    selector: attribute_item
```

Pattern object is explicitly designed for “needs context + select subnode” cases. ([ast-grep.github.io][4])

**Attach derive → struct/enum/union**

```yaml
rule:
  all:
    - any:
        - kind: struct_item
        - kind: enum_item
        - kind: union_item
    - follows:
        stopBy: neighbor
        matches: attr-derive
```

(Uses `matches` to reference a utility rule; see §8.3.) ([ast-grep.github.io][6])

**Derive membership filter (rule-only fallback)**

```yaml
utils:
  attr-derive-serialize:
    kind: attribute_item
    regex: '(?s)^#\\[derive\\(.*\\bSerialize\\b.*\\)\\]$'
rule:
  all:
    - kind: struct_item
    - follows: { stopBy: neighbor, matches: attr-derive-serialize }
```

`regex` matches the node’s text; anchoring it to `kind: attribute_item` keeps it scoped. ([Docs.rs][2])

---

### 8.2.2 `#[cfg(...)]` (outer attribute; gating compilation)

Rust reference examples include conditional compilation via `#[cfg(...)]`. ([Rust Documentation][1])
Tree-sitter exposes `cfg` arguments as `token_tree`, so treat complex cfg expressions as token trees, not full Rust expressions. ([Docs.rs][2])

**Match cfg attributes**

```yaml
rule:
  pattern:
    context: |
      #[cfg($$$COND)]
      fn __x() {}
    selector: attribute_item
```

([ast-grep.github.io][4])

**Attach cfg → module**

```yaml
rule:
  all:
    - kind: mod_item
    - follows: { stopBy: neighbor, matches: attr-cfg }
```

(`follows` is the correct relational primitive for “applies to the thing that follows”.) ([ast-grep.github.io][5])

---

### 8.2.3 `#[cfg_attr(...)]` (outer attribute; conditional attributes)

Same operational model: `cfg_attr` arguments are `token_tree`, so structural match first, then refine. ([Docs.rs][2])

**Match cfg_attr**

```yaml
rule:
  pattern:
    context: |
      #[cfg_attr($$$ARGS)]
      fn __x() {}
    selector: attribute_item
```

([ast-grep.github.io][4])

---

### 8.2.4 `#[test]` and `#[tokio::test]` (outer attribute; function identity)

Rust reference shows `#[test] fn ...` as a canonical example. ([Rust Documentation][1])

**Match `#[test]` attribute_item**

```yaml
utils:
  attr-test:
    pattern:
      context: |
        #[test]
        fn __x() {}
      selector: attribute_item
```

([ast-grep.github.io][4])

**Attach `#[test]` → function**

```yaml
rule:
  all:
    - kind: function_item
    - follows: { stopBy: neighbor, matches: attr-test }
```

`stopBy: neighbor` is the intended “attached attribute directly above item” interpretation. ([ast-grep.github.io][3])

**Match `#[tokio::test]` (scoped path)**

```yaml
utils:
  attr-tokio-test:
    pattern:
      context: |
        #[tokio::test]
        async fn __x() {}
      selector: attribute_item
```

Attribute path heads can be `scoped_identifier` (e.g., `tokio::test`) inside `attribute`. ([Docs.rs][2])

---

### 8.2.5 Lint attributes: `allow/deny/warn/forbid` (outer + inner)

Rust reference shows lint attributes like `#[allow(non_camel_case_types)]` and inner `#![allow(unused)]`. ([Rust Documentation][1])
Tree-sitter models both outer and inner forms as separate wrapper nodes around the same `attribute` node. ([Docs.rs][2])

**Outer lint (item-scoped)**

```yaml
utils:
  outer-lint-allow:
    pattern:
      context: |
        #[allow($$$LINTS)]
        fn __x() {}
      selector: attribute_item
```

**Inner lint (scope-scoped)**

```yaml
utils:
  inner-lint-allow:
    pattern:
      context: |
        mod __m {
          #![allow($$$LINTS)]
        }
      selector: inner_attribute_item
```

Pattern object usage is the supported mechanism here. ([ast-grep.github.io][4])

---

### 8.2.6 Attributes on match arms (use `has`, not `follows`)

Match arms can directly contain both outer/inner attribute items as children. ([Docs.rs][2])

**Find match arms with any attribute**

```yaml
rule:
  all:
    - kind: match_arm
    - has:
        stopBy: end
        any:
          - kind: attribute_item
          - kind: inner_attribute_item
```

(`has` + `stopBy` are relational-rule options.) ([ast-grep.github.io][5])

---

## 8.3 Attribute filters as reusable utils (utility-rule pack pattern)

You want attribute querying to be **composable**:

* define “this is a `cfg(test)` attribute”
* reuse it in “exclude tests”
* reuse it in “only test modules”
* reuse it in “lint policy enforcement”

### 8.3.1 Mechanics: `utils` + `matches`

* `utils` is a dictionary of reusable rule objects. ([ast-grep.github.io][7])
* You reference them via the `matches` composite rule. ([ast-grep.github.io][8])

### 8.3.2 A practical Rust “attribute utils” starter pack

```yaml
# rust-attr-utils.yml (can be local utils inside a rule, or global util file)

utils:
  # outer attributes
  attr-derive:
    pattern: { context: "#[derive($$$DERIVES)] struct __SgT;", selector: attribute_item }

  attr-cfg:
    pattern: { context: "#[cfg($$$COND)] fn __x() {}", selector: attribute_item }

  attr-cfg-attr:
    pattern: { context: "#[cfg_attr($$$ARGS)] fn __x() {}", selector: attribute_item }

  attr-test:
    pattern: { context: "#[test] fn __x() {}", selector: attribute_item }

  attr-tokio-test:
    pattern: { context: "#[tokio::test] async fn __x() {}", selector: attribute_item }

  # “cfg(test)” heuristic (token_tree is free-form; keep it anchored to attribute_item)
  attr-cfg-test-like:
    kind: attribute_item
    regex: '(?s)^#\\[cfg\\(.*\\btest\\b.*\\)\\]$'
```

Pattern object contract is `{context, selector, strictness?}`. ([ast-grep.github.io][4])

### 8.3.3 Reuse: “has attribute X” and “not cfg(test)”

**Example: all functions that are tests**

```yaml
rule:
  all:
    - kind: function_item
    - follows: { stopBy: neighbor, matches: attr-test }
```

(`follows` + `stopBy: neighbor` semantics.) ([ast-grep.github.io][3])

**Example: production functions (exclude test + cfg(test))**

```yaml
rule:
  all:
    - kind: function_item
    - not: { follows: { stopBy: neighbor, matches: attr-test } }
    - not: { follows: { stopBy: neighbor, matches: attr-tokio-test } }
    - not: { follows: { stopBy: neighbor, matches: attr-cfg-test-like } }
```

`not` is a composite rule; `matches` references your utils. ([ast-grep.github.io][8])

### 8.3.4 Promote to global utils (rule pack hygiene)

ast-grep supports **global utility rules** via `utilDirs` in `sgconfig.yml`, so you can share `attr-*` across multiple rule files. ([ast-grep.github.io][6])

---

If you want the next deep dive after this, the natural follow-on is **“Macro & token-tree driven querying”**: how to write robust rules when the payload is a `token_tree` (cfg expressions, derive lists, attribute macros, macro invocations), and when to switch from structural matching → text/regex post-filters. ([Docs.rs][2])

[1]: https://doc.rust-lang.org/stable/reference/attributes.html?utm_source=chatgpt.com "Attributes - The Rust Reference"
[2]: https://docs.rs/crate/tree-sitter-rust/latest/source/src/node-types.json "tree-sitter-rust 0.24.0 - Docs.rs"
[3]: https://ast-grep.github.io/reference/rule.html?utm_source=chatgpt.com "Rule Object Reference | ast-grep"
[4]: https://ast-grep.github.io/guide/rule-config/atomic-rule.html?utm_source=chatgpt.com "Atomic Rule | ast-grep"
[5]: https://ast-grep.github.io/guide/rule-config/relational-rule.html?utm_source=chatgpt.com "Relational Rules | ast-grep"
[6]: https://ast-grep.github.io/guide/rule-config/utility-rule.html?utm_source=chatgpt.com "Reusing Rule as Utility | ast-grep"
[7]: https://ast-grep.github.io/reference/yaml.html?utm_source=chatgpt.com "Configuration Reference | ast-grep"
[8]: https://ast-grep.github.io/guide/rule-config/composite-rule.html?utm_source=chatgpt.com "Composite Rule | ast-grep"

# 9) Rust literal edge cases (string-heavy Rust code)

This chapter is about **search correctness** (don’t miss matches; don’t match the wrong thing) and **safe rewrites** (don’t emit invalid Rust; don’t silently change semantics) when Rust code is dominated by string/byte literals and formatting macros.

---

## 9.0 Why literals are “special” in Rust AST search

1. **Raw strings are delimiter-parametric tokens** (`r#"…" #`, `r##"…"##`, …). The *same literal form* can appear with different `#` counts, and you must handle that if you want completeness. ([Rust Documentation][1])
2. **Byte strings are ASCII-only**; raw byte strings do **no escapes** and still forbid non-ASCII bytes. A rewrite that inserts Unicode into a `b"…" / br"…"` literal creates invalid Rust. ([Rust Documentation][1])
3. **Formatting macros** (`format!`, `println!`, etc.) enforce that the **first argument is a string literal** (compiler-checked), and the `{}` placeholder grammar has edge cases (brace escaping) that can break builds if you rewrite blindly. ([Rust Documentation][2])
4. ast-grep rewrites (`--rewrite` / `fix`) are **textual templates**: meta vars are substituted even if the result isn’t valid code, so your rule must guarantee validity. ([ast-grep.github.io][3])

---

## 9.1 Rust literal forms that matter for structural search

### 9.1.1 Raw string literals: `r#"…"#` and multi-`#`

**Rust lexer contract (what you must model):**

* Start: `r` + *<256* `#` + `"`
* End: `"` + *same number of* `#`
* **No escapes** are processed; the body is literal text; termination is only by the matching delimiter. ([Rust Documentation][1])

**Operational consequence:** the **body can contain quotes** freely as long as it doesn’t form the closing delimiter sequence for the chosen `#` count. ([Rust Documentation][1])

**Matching implication:** a pattern like `r#"$$$BODY"#` matches only the **1-#** delimiter form; you need a strategy for other counts.

---

### 9.1.2 Byte strings: `b"…"` and raw byte strings: `br#"…"#`

**Byte string `b"…"`**

* ASCII + escapes (byte escapes allowed)
* Line breaks are allowed; CR is disallowed; backslash immediately before a line break suppresses the line break in the produced string. ([Rust Documentation][4])

**Raw byte string `br#"…"#`**

* Start: `br` + `<256` `#` + `"`
* End: `"` + same `#` count
* **No escapes**, and **cannot contain non-ASCII** bytes. ([Rust Documentation][1])

**Rewrite hazard:** converting a Unicode raw string to `br"…"` can fail instantly if the content isn’t ASCII. ([Rust Documentation][1])

---

## 9.2 How tree-sitter + ast-grep typically surface these literals

Even if your main workflow is ast-grep patterns, it helps to know the common node names you’ll see when you debug:

* Many Rust tree-sitter consumers treat ordinary string literals as `string_literal` nodes (e.g., difftastic config for Rust explicitly lists `string_literal` as an atomic node type). ([Fossies][5])
* Raw strings often appear as `raw_string_literal` nodes in tree-sitter queries (example: matching a `raw_string_literal` inside a `token_tree` in a Rust macro invocation). ([Docs.rs][6])

**Actionable rule-authoring loop (mandatory for literal edge cases):**

```bash
# Always confirm what the snippet parses into
sg run -l rust -p 'fn __sg_ctx() { let _ = r#"hi"#; }' --debug-query ast
sg run -l rust -p 'fn __sg_ctx() { let _ = br#"hi"#; }' --debug-query ast
```

(Use this to decide whether you’ll anchor via `kind:` or via literal syntax patterns.) ([Rust Documentation][1])

---

## 9.3 Pattern recipes for raw strings / byte strings

### 9.3.1 “Hot set” matcher pack for raw strings (0–3 `#`)

In real code, 0–3 `#` covers most cases. Don’t fight the “variable delimiter count” problem inside a single pattern—just ship a small pack.

**YAML utils (drop-in)**

```yaml
utils:
  raw-str-0: { pattern: 'r"$$$S"' }
  raw-str-1: { pattern: 'r#"$$$S"#' }
  raw-str-2: { pattern: 'r##"$$$S"##' }
  raw-str-3: { pattern: 'r###"$$$S"###' }

rule:
  any:
    - matches: raw-str-0
    - matches: raw-str-1
    - matches: raw-str-2
    - matches: raw-str-3
```

* `$$$S` is the workhorse for multi-line bodies (as in the indoc example). ([ast-grep.github.io][7])

**Debug which forms exist in your repo**

```bash
sg run -l rust -p 'r#"$$$S"#' --debug-query ast
sg run -l rust -p 'r##"$$$S"##' --debug-query ast
```

(Validate parsing once, then reuse in utils.)

---

### 9.3.2 Byte strings (`b"…"`) and raw byte strings (`br#"…"#`)

**Byte string matcher pack**

```yaml
utils:
  bstr: { pattern: 'b"$$$B"' }         # may include escapes and line breaks
  rbstr-0: { pattern: 'br"$$$B"' }     # raw byte string, 0-# form
  rbstr-1: { pattern: 'br#"$$$B"#' }
  rbstr-2: { pattern: 'br##"$$$B"##' }

rule:
  any:
    - matches: bstr
    - matches: rbstr-0
    - matches: rbstr-1
    - matches: rbstr-2
```

**Correctness guardrails**

* Use these patterns to *find* candidates, then apply semantic checks in your tooling:

  * For `br...` forms, ensure body is ASCII. ([Rust Documentation][1])
  * For `b"..."`, remember escapes are processed and line continuations exist; direct text substitution inside the quotes can change the bytes produced. ([Rust Documentation][4])

---

## 9.4 Format strings & formatting macros (`format!`, `println!`)

### 9.4.1 Compiler constraints you can use as “free validation”

* `format!` requires the **first argument** to be a **string literal**, and `{}` placeholders drive interpolation. ([Rust Documentation][2])
* `println!` uses the same format syntax as `format!`. ([Rust Documentation][8])
* Literal `{` and `}` inside format strings must be escaped as `{{` and `}}`. ([Rust Documentation][9])

### 9.4.2 Search recipes

**Find `format!` calls (broad)**

```bash
sg run -l rust -p 'format!($$$TT)'
```

**Find `format!` where the format string is a raw string (targeted)**

```bash
sg run -l rust -p 'format!(r#"$$$FMT"#, $$$ARGS)'
sg run -l rust -p 'format!(r##"$$$FMT"##, $$$ARGS)'
```

**Find `println!` with raw-string format**

```bash
sg run -l rust -p 'println!(r#"$$$FMT"#, $$$ARGS)'
```

### 9.4.3 Rewrite safety rules for formatting macros

When rewriting the format string:

* Preserve brace-escaping rules (`{{` / `}}`) or you’ll introduce format parse errors. ([Rust Documentation][9])
* Preserve argument arity / naming (`{x}`, `{0}`, etc.) or compilation will fail (compiler checks format strings). ([Rust Documentation][2])

---

## 9.5 Safe rewrite playbook for raw/byte strings

### 9.5.1 Treat `fix` / `--rewrite` as “string templating”, not AST synthesis

ast-grep’s fix/rewrite is a textual template and is not parsed; metavariables are substituted wherever they appear, even if the result can’t parse. ([ast-grep.github.io][3])

**Therefore:**

* Use fix/rewrite only for transformations where **validity is guaranteed by construction** (e.g., “remove macro wrapper but keep the literal unchanged”).
* For delimiter-sensitive changes (changing raw string delimiters; converting between byte and Unicode strings), prefer a programmatic patcher (ast-grep-py + your own delimiter selection logic).

### 9.5.2 Raw string delimiter selection algorithm (must-have if you rewrite bodies)

Given a desired body `B`, choose the smallest `n` such that `B` does **not** contain the closing delimiter sequence `"` followed by `n` `#` characters. This is exactly how raw string termination works. ([Rust Documentation][1])

Pseudo:

```python
def choose_hash_count(body: str, max_n: int = 10) -> int:
    for n in range(max_n + 1):
        if ('"' + ('#' * n)) not in body:
            return n
    raise ValueError("Need larger delimiter count")
```

### 9.5.3 Byte-string ASCII gate (must-have if you rewrite `b"/br"`)

Raw byte strings cannot contain non-ASCII bytes; treat `body.isascii()` as a hard precondition before emitting `br...`. ([Rust Documentation][1])

---

## 9.6 Real-world example: `indoc!` macro rewrite (ast-grep Rust catalog)

ast-grep’s Rust catalog includes a worked refactor that removes `indoc!` while preserving the raw-string payload, using a pattern that captures the body inside a raw string and a rewrite template. ([ast-grep.github.io][7])

**Catalog pattern + rewrite (as published)**

```bash
ast-grep --pattern 'indoc! { r#"$$$A"# }' --rewrite '`$$$A`' sgtest.rs
```

([ast-grep.github.io][7])

### 9.6.1 What to learn from this example (generalizable)

* `$$$A` is the key: it can capture multi-line content inside a raw string literal delimiter. ([ast-grep.github.io][7])
* The pattern is delimiter-specific (`r#" … "#`). If your repo uses `r##" … "##`, you need additional variants (see §9.3.1). ([Rust Documentation][1])
* This is a *textual rewrite*; ensure the emitted replacement is valid in the context where the macro was used (indentation/dedenting semantics may change when removing `indoc!`). ([ast-grep.github.io][3])

### 9.6.2 Productionizing the `indoc!` matcher (recommended pack)

Add variants for `#` counts + spacing:

```yaml
utils:
  indoc-r1: { pattern: 'indoc! { r#"$$$S"# }' }
  indoc-r2: { pattern: 'indoc! { r##"$$$S"## }' }
  indoc-r3: { pattern: 'indoc! { r###"$$$S"### }' }

rule:
  any:
    - matches: indoc-r1
    - matches: indoc-r2
    - matches: indoc-r3

# fix/rewrite: only do this if your replacement is valid for your use case
fix: '`$$$S`'
```

(Still textual—see §9.5.1.) ([ast-grep.github.io][3])

[1]: https://doc.rust-lang.org/beta/reference/tokens.html?utm_source=chatgpt.com "Tokens - The Rust Reference"
[2]: https://doc.rust-lang.org/std/macro.format.html?utm_source=chatgpt.com "format in std - Rust"
[3]: https://ast-grep.github.io/guide/project/lint-rule.html?utm_source=chatgpt.com "Lint Rule | ast-grep"
[4]: https://doc.rust-lang.org/reference/tokens.html?utm_source=chatgpt.com "Tokens - The Rust Reference"
[5]: https://fossies.org/linux/difftastic/src/parse/tree_sitter_parser.rs?utm_source=chatgpt.com "difftastic: src/parse/tree_sitter_parser.rs | Fossies"
[6]: https://docs.rs/crate/tree-sitter-rigz/latest?utm_source=chatgpt.com "tree-sitter-rigz 0.5.0 - Docs.rs"
[7]: https://ast-grep.github.io/catalog/rust/ "Rust | ast-grep"
[8]: https://doc.rust-lang.org/nightly/std/macro.println.html?utm_source=chatgpt.com "println in std - Rust"
[9]: https://doc.rust-lang.org/alloc/fmt/index.html?utm_source=chatgpt.com "alloc::fmt - Rust"

# 10) Declarative refinement patterns tuned for Rust (constraints / utils / transforms)

This chapter is “how you turn a broad Rust structural match into a **policy-quality** match set”, and then (optionally) into a **safe codemod**.

---

## 10.0 Mental model: `rule` → `constraints` → `transform` → `fix`

In ast-grep’s find/patch model:

1. `rule` finds candidate nodes
2. `constraints` filters based on **captured single meta vars**
3. `transform` derives new strings (usually for rewriting)
4. `fix` is a **template string** that substitutes meta vars (and transformed vars) into replacement text ([ast-grep.github.io][1])

Two critical details that matter for Rust-heavy rules:

* `constraints` apply **after** `rule` matches, and only to **single** meta vars (e.g., `$ARG`, not `$$$ARGS`). ([ast-grep.github.io][2])
* `fix` is textual (not parsed); meta-var adjacency can be misinterpreted (e.g., `$VARName` becomes `$VARN` + `ame`). Use `transform` to build safe concatenations. ([ast-grep.github.io][3])

---

## 10.1 Constraints tuned for Rust

### 10.1.1 Constraints are for “one-node, post-match” filtering

Typical Rust example: “match any call, but only if the callee name starts with `crate::`”.

You **cannot** write `crate::$PATH` as a metavariable (`$PATH` must be the whole AST node), so you capture the node and constrain it via regex:

```yaml
rule:
  pattern: $CALLEE($$$ARGS)
constraints:
  CALLEE:
    regex: '^crate::'
```

This pattern is exactly the recommended workaround for “prefix/suffix cannot be part of a metavariable node”, and is the canonical use of `constraints` + `regex`. ([ast-grep.github.io][4])

> Reminder: `constraints` keys are the metavariable names **without** `$`. ([ast-grep.github.io][2])

---

### 10.1.2 Visibility constraints (`pub`, `pub(crate)`, `pub(super)`, `pub(in path)`)

#### Rust visibility syntax (what you must match)

Rust’s restricted visibility forms include `pub(crate)`, `pub(super)`, `pub(self)` and `pub(in path)`; and for Rust 2018+, `pub(in path)` paths must begin with `crate`, `self`, or `super`. ([Rust Documentation][5])

#### Tree-sitter emits a named `visibility_modifier` node

You can see `visibility_modifier` as a named node in tree-sitter’s Rust S-expression examples. ([Docs.rs][6])

#### Robust filtering approach: `has kind: visibility_modifier` + anchored regex

`regex` in ast-grep matches the **entire node text**, so anchor it (`^...$`) to distinguish `pub` vs `pub(crate)`. ([ast-grep.github.io][7])

**Exact `pub` only**

```yaml
rule:
  all:
    - kind: function_item
    - has:
        kind: visibility_modifier
        regex: '^pub$'
        stopBy: end
```

`stopBy: end` makes `has` search descendants, not just direct children. ([ast-grep.github.io][8])

**`pub(crate)` only**

```yaml
rule:
  all:
    - kind: function_item
    - has:
        kind: visibility_modifier
        regex: '^pub\\(crate\\)$'
        stopBy: end
```

**`pub(super)` only**

```yaml
rule:
  all:
    - kind: function_item
    - has:
        kind: visibility_modifier
        regex: '^pub\\(super\\)$'
        stopBy: end
```

**`pub(in crate::...)` (Rust 2018+ compliant head)**

```yaml
rule:
  all:
    - kind: function_item
    - has:
        kind: visibility_modifier
        regex: '^pub\\(in (crate|self|super)::'
        stopBy: end
```

The head restriction is from the Rust Reference. ([Rust Documentation][5])

> Practical Rust policy pattern: many teams lint “`pub(crate)` preferred unless truly public API”. You can detect “pub but not actually exported” only with semantic module export analysis (outside tree-sitter); syntactically, the above rules are the reliable layer.

---

### 10.1.3 Module/path-shape constraints (`crate::`, `super::`, `self::`)

Rust path “shape” appears in multiple contexts. Your highest ROI targets are:

1. `use` declarations
2. call/type paths inside expressions/types
3. visibility restrictions `pub(in path)` (already covered)

#### (A) `use` declarations: constrain the `argument` field

Typed wrappers examples show `use_declaration` has a field `"argument"`, and a scoped identifier can have a `"path"` field. ([Docs.rs][9])

So you can filter imports by argument text **at the argument field**, without guessing child indices:

```yaml
rule:
  all:
    - kind: use_declaration
    - has:
        field: argument
        stopBy: end
        regex: '^crate::'
```

`field` is supported on `has/inside` relational rules. ([ast-grep.github.io][8])

Variants:

```yaml
# super::...
regex: '^super::'

# self::...
regex: '^self::'
```

#### (B) Generic “path-ish” capture + constraints (works in many contexts)

When you can’t reliably pick the Rust node kind (`scoped_identifier` vs `scoped_type_identifier` vs others), use the capture+constraint idiom:

```yaml
rule:
  pattern: $PATH
constraints:
  PATH: { regex: '^(crate|self|super)::' }
```

This is the same “constraints for prefix rules” technique recommended in ast-grep docs/FAQ. ([ast-grep.github.io][4])

---

### 10.1.4 Excluding macro contexts (don’t match inside token trees)

Macro payloads are the easiest place to get **false positives** (especially stringy cfg/derive token trees and “code-like” DSLs).

**Best practice: derive the macro node kind once, then freeze it in utils.**

1. Determine macro invocation kind in your Rust grammar:

```bash
sg run -l rust -p 'foo!($$$)' --debug-query ast
```

(You’re looking for the named node kind that represents the macro call site.)

2. Exclude matches “inside macro invocations” (deep):

```yaml
rule:
  all:
    - <YOUR_TARGET_RULE>
    - not:
        inside:
          kind: <MACRO_INVOC_KIND>
          stopBy: end
```

`inside` + `stopBy: end` is the canonical “descendant-of” filter. ([ast-grep.github.io][8])

> If you also want to exclude attribute argument token trees (e.g. `#[cfg(...)]`), do the same introspection workflow on an attribute snippet, then exclude `inside: kind: token_tree` *only when it’s within an attribute node* (attributes are everywhere in Rust; don’t globally ban token trees).

---

## 10.2 Reusable `utils` packs for Rust rule authoring

You do **not** want to retype visibility/path/macro filters in every rule file. ast-grep’s `utils` + `matches` exist for exactly this. ([ast-grep.github.io][10])

### 10.2.1 A practical `rust-utils.yml` starter (local utils)

```yaml
utils:
  vis-pub:
    has: { kind: visibility_modifier, regex: '^pub$', stopBy: end }

  vis-pub-crate:
    has: { kind: visibility_modifier, regex: '^pub\\(crate\\)$', stopBy: end }

  vis-pub-super:
    has: { kind: visibility_modifier, regex: '^pub\\(super\\)$', stopBy: end }

  import-from-crate:
    all:
      - kind: use_declaration
      - has: { field: argument, regex: '^crate::', stopBy: end }

rule:
  all:
    - kind: function_item
    - matches: vis-pub-crate
```

* `utils` are reusable rule objects referenced via `matches`. ([ast-grep.github.io][10])
* Relational `field` / `stopBy` behavior is defined in relational rule docs. ([ast-grep.github.io][8])

### 10.2.2 Utility rules vs constraints: where each shines

* Use **utils** for “structural filters” (visibility, inside module X, not in macros, etc.). ([ast-grep.github.io][10])
* Use **constraints** for “meta-variable properties” (prefix name checks, regex on captured identifier, etc.), noting constraints are single-meta-var only. ([ast-grep.github.io][2])

---

## 10.3 Transforms for Rust naming conventions (snake_case ↔ UpperCamelCase)

ast-grep’s `transform` supports **string case conversions**, including:

* `snakeCase`
* `pascalCase` (this is Rust’s “UpperCamelCase” for types) ([ast-grep.github.io][11])

### 10.3.1 Canonical case conversion transform (codemod-friendly)

**Example: generate a module name from a type name**

```yaml
rule:
  pattern: struct $TYPE { $$$ }
constraints:
  TYPE: { kind: type_identifier }
transform:
  MOD_NAME:
    convert:
      source: $TYPE
      toCase: snakeCase
fix: |-
  mod $MOD_NAME;
  struct $TYPE { $$$ }
```

* `transform.convert` and supported cases are in the transformation reference. ([ast-grep.github.io][11])
* `fix` is textual; combining transform + fix is the standard find/patch pipeline. ([ast-grep.github.io][1])

**Example: generate a type name from a snake_case identifier**

```yaml
rule:
  pattern: let $VAR = $E;
constraints:
  VAR: { kind: identifier }
transform:
  TYPE_NAME:
    convert:
      source: $VAR
      toCase: pascalCase
fix: |-
  struct $TYPE_NAME;
  let $VAR = $E;
```

Supported `pascalCase` is documented. ([ast-grep.github.io][11])

### 10.3.2 Separator control (when your source names aren’t Rusty)

`convert` can be “separator sensitive” and can be configured with `separatedBy` (dash, underscore, case changes, etc.). This is useful if you’re migrating from env-var-ish or kebab-case identifiers into Rust. ([ast-grep.github.io][11])

### 10.3.3 “Don’t concatenate meta vars in `fix`” (Rust name suffix/prefix patterns)

If you want `Foo` → `FooError` you **cannot** reliably write `$TYPEError` in `fix` (it may parse as `$TYPEE` + `rror`). Use a transform (e.g. `replace`/rewrite/convert pipeline) as recommended in rewrite docs. ([ast-grep.github.io][3])

---

## 10.4 Python (ast-grep-py) mapping: same schema, dicts not YAML

Everything above is the same when passed to ast-grep-py: you supply the rule object as a dict (including `constraints`, `utils`, `transform`, `fix`) and run it against `SgRoot(..., "rust")`. The core semantics are defined by the ast-grep YAML/rule/transform references you’re implementing against. ([ast-grep.github.io][2])

[1]: https://ast-grep.github.io/guide/project/lint-rule.html?utm_source=chatgpt.com "Lint Rule | ast-grep"
[2]: https://ast-grep.github.io/reference/yaml.html?utm_source=chatgpt.com "Configuration Reference | ast-grep"
[3]: https://ast-grep.github.io/guide/rewrite-code.html?utm_source=chatgpt.com "Rewrite Code | ast-grep"
[4]: https://ast-grep.github.io/advanced/faq.html?utm_source=chatgpt.com "Frequently Asked Questions | ast-grep"
[5]: https://doc.rust-lang.org/beta/reference/visibility-and-privacy.html?utm_source=chatgpt.com "Visibility and privacy - The Rust Reference"
[6]: https://docs.rs/tree_sitter?utm_source=chatgpt.com "tree_sitter - Rust"
[7]: https://ast-grep.github.io/reference/rule.html?utm_source=chatgpt.com "Rule Object Reference | ast-grep"
[8]: https://ast-grep.github.io/guide/rule-config/relational-rule.html?utm_source=chatgpt.com "Relational Rules | ast-grep"
[9]: https://docs.rs/type-sitter "type_sitter - Rust"
[10]: https://ast-grep.github.io/guide/rule-config/utility-rule.html?utm_source=chatgpt.com "Reusing Rule as Utility | ast-grep"
[11]: https://ast-grep.github.io/reference/yaml/transformation.html?utm_source=chatgpt.com "Transformation Object | ast-grep"
