Below is a **high-lexical-density, systems-oriented technical guide** for:

1. **Entry points and “parse/compile knobs”**
2. **AST helper utilities (public functions)**

Format/style is intentionally modeled after your attached reference (dense “surface area + power knobs + footguns + minimal but clarifying code”). 
Baseline is **Python 3.14.x** docs (because several “advanced knobs” landed in 3.13–3.14). ([Python documentation][1])

---

# 1) Entry points and “parse/compile knobs”

## 1.0 Systems model (the real boundary)

Think of native `ast` as three tightly-coupled but separable planes:

1. **Front-end parse**: source → concrete parse → AST nodes (may succeed even when code is non-executable).
2. **(Optional) AST optimization**: compiler-side rewrites driven by `optimize` (when you ask for optimized AST).
3. **Compile to code object**: AST (or source) → code object (enforces semantic constraints such as scope rules). ([Python documentation][1])

Crucial implication: **“parse success” ≠ “compile success”**. `ast.parse()` can return an AST for source that cannot be compiled standalone (e.g., `return 42` at module level), because parse does not perform the later compiler checks. ([Python documentation][1])

---

## 1.1 `ast.parse(...)` is (now) a configurable `compile(..., flags=..., optimize=...)` wrapper

### Signature (3.14)

```python
ast.parse(
    source,
    filename="<unknown>",
    mode="exec",
    *,
    type_comments=False,
    feature_version=None,
    optimize=-1,
)
```

([Python documentation][1])

### The non-obvious semantic contract

`ast.parse()` is specified as equivalent to calling `compile(source, filename, mode, flags=FLAGS_VALUE, optimize=optimize)` where:

* if `optimize <= 0`: `FLAGS_VALUE = ast.PyCF_ONLY_AST`
* else: `FLAGS_VALUE = ast.PyCF_OPTIMIZED_AST` ([Python documentation][1])

This is a *big deal* because it means `optimize` is not “just metadata”: it changes whether you get a **raw AST** vs an **optimized AST** (via distinct compiler flags). ([Python documentation][1])

### Knob: `mode`

* `"exec"` → parse a file/module input (root is `ast.Module`)
* `"eval"` → parse a single expression input (root is `ast.Expression`)
* `"single"` → parse a single interactive statement (root is `ast.Interactive`)
* `"func_type"` → parse “signature type comment” syntax from PEP 484 (root is `ast.FunctionType`) ([Python documentation][1])

**Advanced agent hint:** `mode="func_type"` is an easy way to reuse the *parser* for a type-signature DSL that’s “Pythonic but not Python statements”.

### Knob: `type_comments=True`

Enables parsing/validation of:

* `# type: <type>`
* `# type: ignore[...optional text...]`

Effects:

* populates `type_comment` fields on certain nodes (otherwise always `None`)
* returns `# type: ignore` locations via `Module.type_ignores` (otherwise always empty)
* causes **syntax errors for misplaced** type comments (i.e., the parser becomes stricter). ([Python documentation][1])

Mechanically, `type_comments=True` is defined as “equivalent to passing `ast.PyCF_TYPE_COMMENTS` to `compile()`”. ([Python documentation][1])

### Knob: `feature_version=(major, minor)`

* “Best-effort” attempt to parse using that Python version’s grammar.
* `major` must be `3`.
* Supported range is `(3, 7)` up to `sys.version_info[0:2]` (minimum can increase in future). ([Python documentation][1])

**Key nuance:** “best-effort” is explicitly *not* a guarantee that parsing behaves exactly like the targeted runtime version (success/failure may differ). ([Python documentation][1])

**Practical use:** grammar-gating in tooling (e.g., reject `match` syntax by parsing with `(3, 9)`) rather than runtime simulation.

### Knob: `optimize`

For `ast.parse`, `optimize` does two separate things:

1. it is forwarded to the compiler front-end (same “optimize levels” semantics as `compile()`), and
2. it chooses **which AST-return flag** is used (`ONLY_AST` vs `OPTIMIZED_AST`). ([Python documentation][1])

Because default is `-1`, `ast.parse` will follow interpreter `-O` settings unless you override. ([Python documentation][2])

### Footgun: parse/compile stack depth hazards

The docs include explicit warnings that sufficiently large/complex inputs can crash the interpreter (C stack depth limits) when compiling to an AST or parsing. This is not theoretical for adversarial inputs. ([Python documentation][1])

---

## 1.2 `compile(...)` as your “AST ↔ runtime semantics” gate

### Signature + accepted `source` types

```python
compile(source, filename, mode, flags=0, dont_inherit=False, optimize=-1)
```

* `source` may be `str`, `bytes`, **or an AST object**.
* Returns a **code object** or (if flags request it) an **AST object**. ([Python documentation][2])

### The compilation knobs that matter for AST work

#### 1) `flags` and `dont_inherit` (future features + compiler options)

* `flags` adds to the current compilation context unless `dont_inherit` is nonzero.
* `dont_inherit != 0` means: **ignore surrounding frame flags**; use exactly `flags`. ([Python documentation][2])

This matters for tooling that runs under unpredictable host environments (REPLs, notebooks, plugin systems) where `__future__` imports might leak into compilation unless you isolate.

#### 2) `optimize` (levels)

The docs define:

* `-1`: inherit interpreter `-O`
* `0`: no optimization (`__debug__` true)
* `1`: remove asserts (`__debug__` false)
* `2`: remove asserts **and docstrings** ([Python documentation][2])

When you request **optimized AST** (via `ast.PyCF_OPTIMIZED_AST` or `ast.parse(..., optimize>0)`), the returned AST is “optimized according to optimize”. ([Python documentation][1])

#### 3) Mode alignment

`mode` is the grammar start symbol (`'exec'|'eval'|'single'`); it needs to match what the AST represents. The docs describe these modes for compilation and parsing. ([Python documentation][2])

### Compiler warnings relevant to AST tooling

* Raises `SyntaxError` or `ValueError` for invalid sources. ([Python documentation][2])
* Auditing event `compile` is raised (relevant if you run under audit hooks / hardened runtimes). ([Python documentation][2])
* Stack-depth crash warning also applies when compiling to AST. ([Python documentation][2])

---

## 1.3 Compiler flags exported by `ast` (the “AST-return + parser features” switches)

The `ast` module explicitly exports these `PyCF_*` flags for use with `compile()` (and `ast.parse` uses them internally). ([Python documentation][1])

### `ast.PyCF_ONLY_AST`

Return an AST instead of a code object. ([Python documentation][1])

### `ast.PyCF_OPTIMIZED_AST` (3.13+)

Return an **optimized** AST, “according to optimize”. ([Python documentation][1])

### `ast.PyCF_TYPE_COMMENTS`

Enable parsing and validation of PEP 484/526 type comments. ([Python documentation][1])

### `ast.PyCF_ALLOW_TOP_LEVEL_AWAIT`

Enable top-level `await`, `async for`, `async with`, async comprehensions at module scope. ([Python documentation][1])

### Canonical “AST entrypoint selection” pattern

If you need **precise control** (not the “smart wrapper” behavior of `ast.parse`), use `compile` directly:

```python
import ast

src = "async def f():\n    return 1\n"

# Raw AST (unoptimized)
tree0 = compile(src, "<mem>", "exec", flags=ast.PyCF_ONLY_AST, optimize=0)

# Optimized AST (optimizer may transform/strip based on optimize level)
tree1 = compile(src, "<mem>", "exec", flags=ast.PyCF_OPTIMIZED_AST, optimize=2)
```

This is the explicit version of the equivalence the docs give for `ast.parse`. ([Python documentation][1])

---

## 1.4 Two subtle parse-time behaviors that break naïve AST transforms

### 1) Operator nodes are singletons (per parse tree)

When parsing a string, operator nodes (e.g., instances of `ast.Add`, `ast.And`, `ast.Load`, etc.) are singletons within the returned tree; mutating one mutates all occurrences. In practice: treat these as immutable tokens. ([Python documentation][1])

### 2) `ast.parse` is syntax-only: no scoping checks

The docs explicitly call out that parse does not do scoping checks and compilation can raise additional `SyntaxError`s after parse. So “AST exists” is not proof of “code is compilable”. ([Python documentation][1])

**Tooling implication:** if you build “semantic” analyzers or codegen pipelines, decide explicitly whether you want:

* **accept-then-diagnose** (parse first, run your own validations), or
* **compile-gated correctness** (parse + compile test at boundary).

---

# 2) AST helper utilities (public functions)

## 2.0 Systems model: helpers cluster into 4 roles

1. **Round-tripping / surface rendering**: `dump`, `unparse`
2. **Safe-ish literal extraction**: `literal_eval`
3. **Source/metadata recovery**: `get_docstring`, `get_source_segment`
4. **Transform support**: `copy_location`, `fix_missing_locations`, `increment_lineno`, plus traversal helpers (`iter_*`, `walk`) and structural compare (`compare`) ([Python documentation][1])

---

## 2.1 `ast.dump(...)`: precise structural introspection (debug contract)

### Signature (3.14)

```python
ast.dump(
    node,
    annotate_fields=True,
    include_attributes=False,
    *,
    indent=None,
    show_empty=False,
)
```

([Python documentation][1])

### Semantics that matter

* `annotate_fields=True` prints `field=value` pairs; `False` omits unambiguous field names (more compact).
* `include_attributes=True` includes location attrs (line/col offsets, etc.), which are omitted by default. ([Python documentation][1])
* `indent` controls pretty-print format:

  * `None` → single-line
  * positive int → spaces per level
  * string → that string per indentation level
  * non-positive / empty string → newlines but no indentation ([Python documentation][1])
* `show_empty=False` omits optional empty lists by default; optional `None` values are always omitted. (`show_empty` added 3.13.) ([Python documentation][1])

### Practical “golden dump” pattern (stable-ish snapshots)

```python
import ast

tree = ast.parse("x: int = 1\n")
print(ast.dump(tree, indent=2, include_attributes=True, show_empty=True))
```

Use `include_attributes` + `show_empty` deliberately:

* `include_attributes=False` reduces churn from formatting/position changes.
* `show_empty=True` reduces ambiguity when you treat “missing vs empty list” as semantically distinct in tests. ([Python documentation][1])

---

## 2.2 `ast.unparse(ast_obj)`: AST → source generator with *equivalence*, not identity

### Contract

`unparse` generates code that would produce an **equivalent AST if parsed back**, not necessarily the original code string. ([Python documentation][1])

Warnings:

* Generated code not necessarily identical to original (compiler optimizations like constant tuple/frozenset folding may change representation). ([Python documentation][1])
* Very complex expressions may raise `RecursionError`. ([Python documentation][1])

### “Round-trip equivalence” harness (preferred over string equality)

```python
import ast

src = "a = (1, 2, 3)\n"
t0 = ast.parse(src)

src2 = ast.unparse(t0)
t1 = ast.parse(src2)

# Structural equivalence test (use ast.compare in 3.14+)
assert ast.compare(t0, t1)
```

This is the intended correctness model: *AST structural equivalence*, not “same pretty-printed string”. ([Python documentation][1])

---

## 2.3 `ast.literal_eval(node_or_string)`: literal-only evaluation (not a sandbox)

### Accepted structures (only)

* literals/containers: strings, bytes, numbers, tuples, lists, dicts, sets, booleans, `None`, `Ellipsis`
* explicitly **not** arbitrary expressions (no operators, no indexing, no name lookups). ([Python documentation][1])

### Security/robustness warnings (highly non-obvious to many experts)

Docs explicitly state:

* it is designed not to execute code (unlike `eval`), but
* it is **not safe on untrusted input**: small inputs can cause memory exhaustion, C stack exhaustion (crash), or excessive CPU (DoS). ([Python documentation][1])

It can raise `ValueError`, `TypeError`, `SyntaxError`, `MemoryError`, `RecursionError` depending on malformed input. ([Python documentation][1])

### Canonical “node path” usage (avoid re-parsing if you already have AST)

```python
import ast

expr = ast.parse("{'k': [1, 2, 3]}", mode="eval").body
val = ast.literal_eval(expr)
assert val["k"][1] == 2
```

This isolates evaluation to a subtree you control (vs handing a raw string to `literal_eval`). ([Python documentation][1])

---

## 2.4 `ast.get_docstring(node, clean=True)`: docstring extraction with indentation normalization

### Contract

* Works on `Module`, `ClassDef`, `FunctionDef`, `AsyncFunctionDef`.
* Returns docstring or `None`.
* `clean=True` applies `inspect.cleandoc()` indentation cleanup. ([Python documentation][1])

### “Docstring as semantic payload” caution

If you compile code with `optimize=2`, docstrings are removed at compile time (per `compile` docs). If your pipeline relies on docstrings, lock optimize level or operate pre-compile. ([Python documentation][2])

---

## 2.5 `ast.get_source_segment(source, node, *, padded=False)`: source recovery depends on full location spans

### Contract

Returns the exact substring of `source` corresponding to `node`, **only if** location info is present:

* `lineno`, `end_lineno`, `col_offset`, `end_col_offset`
  Otherwise returns `None`. ([Python documentation][1])

`padded=True` pads the first line of multi-line statements to preserve original horizontal alignment. ([Python documentation][1])

### Systems implication

If you synthesize nodes (transformers/codegen), you must propagate or synthesize **span** info (not just starting line/col) if you want `get_source_segment` to work.

---

## 2.6 Location hygiene for transforms: `copy_location` / `fix_missing_locations` / `increment_lineno`

### Why you care

When compiling an AST you generated or transformed, the compiler expects `lineno` and `col_offset` for nodes that support them; populating these by hand is tedious. `fix_missing_locations` fills missing location attrs recursively by inheriting from parent nodes. ([Python documentation][1])

### The 3 primitives

#### `ast.copy_location(new_node, old_node)`

Copies `(lineno, col_offset, end_lineno, end_col_offset)` where possible. ([Python documentation][1])

#### `ast.fix_missing_locations(node)`

Fills missing `lineno`/`col_offset` recursively. ([Python documentation][1])

#### `ast.increment_lineno(node, n=1)`

Shifts line numbers (and end line numbers) by `n` for “move code” scenarios. ([Python documentation][1])

### Canonical transformer snippet (location-correct)

```python
import ast

class RewriteName(ast.NodeTransformer):
    def visit_Name(self, node: ast.Name):
        # rewrite `x` -> data['x'] while preserving location spans
        new = ast.Subscript(
            value=ast.Name(id="data", ctx=ast.Load()),
            slice=ast.Constant(value=node.id),
            ctx=node.ctx,
        )
        return ast.copy_location(new, node)

tree = ast.parse("x + 1\n", mode="exec")
tree2 = ast.fix_missing_locations(RewriteName().visit(tree))
code = compile(tree2, "<mem>", "exec")
```

This pattern is exactly what the docs recommend conceptually: transform → fix missing locations → compile. ([Python documentation][1])

---

## 2.7 Tree iteration utilities: `iter_fields`, `iter_child_nodes`, `walk`

### `ast.iter_fields(node)`

Yields `(fieldname, value)` for each present field in `node._fields`. ([Python documentation][1])

### `ast.iter_child_nodes(node)`

Yields direct child nodes (nodes in fields + nodes inside lists-of-nodes). ([Python documentation][1])

### `ast.walk(node)`

Yields all descendants including the node itself, order unspecified; meant for in-place edits where context is irrelevant. ([Python documentation][1])

### “Fast structural scan” idiom

```python
import ast

tree = ast.parse("def f():\n    return x\n")

names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
assert "x" in names
```

---

## 2.8 `ast.compare(a, b, *, compare_attributes=False)` (3.14+): structural equality as a first-class op

### Contract

Recursively compares two ASTs.

* `compare_attributes=False` (default): ignores attributes such as line/column offsets.
* `compare_attributes=True`: requires attributes to match too, useful for distinguishing structural equivalence vs source-position equivalence. ([Python documentation][1])

### Golden-testing pattern (two tiers)

```python
import ast

t0 = ast.parse("x=1\n")
t1 = ast.parse("x = 1\n")  # different whitespace, same structure

assert ast.compare(t0, t1)                     # structure
assert not ast.compare(t0, t1, compare_attributes=True)  # likely differs in offsets
```

This is the “AST-native” way to do what people previously approximated with `dump(include_attributes=...)` and string compares. ([Python documentation][1])

---

If you want to keep the same momentum/structure, the natural next sections after these are:

* **Traversal & transformation framework** (`NodeVisitor` / `NodeTransformer` semantics + return-value protocol + list-return rules), and
* **Node attribute invariants + grammar drift** (e.g., `Constant` unification, `type_params`, pattern matching nodes, etc.).

[1]: https://docs.python.org/3/library/ast.html "ast — Abstract syntax trees — Python 3.14.2 documentation"
[2]: https://docs.python.org/3/library/functions.html "Built-in Functions — Python 3.14.2 documentation"


Below is the same **high-lexical-density, systems-oriented guide** style as your reference doc, applied to the next two sections.

---

# 3) Traversal & transformation framework (`NodeVisitor` / `NodeTransformer`)

## Mental model (what actually happens at runtime)

Traversal is **dynamic dispatch + optional recursion**:

* `visit(node)` computes a method name `visit_<ClassName>` and calls it if present; else calls `generic_visit(node)`. ([Python documentation][1])
* A custom `visit_X` **does not automatically recurse** into children unless you explicitly call `generic_visit` (or manually visit children). ([Python documentation][1])
* `NodeVisitor`: “read-only” *by convention* (it doesn’t apply replacements). ([Python documentation][1])
* `NodeTransformer`: “write-capable” traversal where each `visit_*` return value is interpreted as a **replacement protocol**. ([Python documentation][1])

---

## 3.1 `NodeVisitor`: dispatch + recursion contract + return-value forwarding

### What you get (surface area)

* `class ast.NodeVisitor`: base for walking AST and calling a visitor function per node. ([Python documentation][1])
* `visit(node)`: dispatches to `visit_<ClassName>` or falls back to `generic_visit(node)`. ([Python documentation][1])
* The visitor function **may return a value**, and `visit()` forwards that value to the caller. ([Python documentation][1])
* `generic_visit(node)`: visits all children by calling `visit()` on them. ([Python documentation][1])

### The core non-obvious invariant

> If you implement `visit_Foo`, you have *opted out* of recursion for that subtree unless you call `self.generic_visit(node)` (or do your own recursion). ([Python documentation][1])

This is the “pruning lever” (intentional or accidental).

### Systems patterns (dense)

**(A) Pre-order scan + prune**

```python
import ast

class FindCalls(ast.NodeVisitor):
    def __init__(self) -> None:
        self.calls: list[ast.Call] = []

    def visit_Call(self, node: ast.Call):
        self.calls.append(node)
        return self.generic_visit(node)  # keep walking deeper
```

**(B) Hard subtree boundary**

```python
class TopLevelOnly(ast.NodeVisitor):
    def visit_FunctionDef(self, node: ast.FunctionDef):
        return None  # NO generic_visit => function body not traversed
```

This is the common technique to enforce “do not inspect inside functions/classes” in indexers.

**(C) Return-value forwarding is *your* aggregation protocol**
You can use return values for compositional visitors, but the default traversal semantics are “visit side-effects”; Python’s docs only guarantee that the visit return is forwarded (not that it is aggregated automatically). ([Python documentation][1])
(So: either accumulate on `self`, or override `generic_visit` in your own subclass to collect child results.)

---

## 3.2 `NodeTransformer`: replacement protocol + recursion responsibility

### What you get (surface area)

* `class ast.NodeTransformer`: a `NodeVisitor` subclass that allows modification. ([Python documentation][1])
* Replacement protocol:

  * return `None` → remove the node from its location
  * return a node → replace the old node with that node
  * return the original node → no replacement ([Python documentation][1])
* **List-return rule** (critical): if the visited node was part of a **collection of statements** (i.e., any `stmt` in `body`/`orelse`/`finalbody` lists, etc.), you may return a **list of nodes** to splice multiple statements in place of one. ([Python documentation][1])
* If you introduce new nodes without location info, call `fix_missing_locations` on the new subtree. ([Python documentation][1])

### Recursion invariant (same as NodeVisitor, but more dangerous)

Transformers do **not** implicitly transform children of a node that you rewrite unless you either:

* call `self.generic_visit(node)` before returning (post-order), or
* explicitly visit children / generic-visit the replacement node (pre-order or “rewrite then descend”). ([Python documentation][1])

---

## 3.3 Return-value protocol matrix (practical “what can I return?”)

| Visitor           | Return value meaning                            | Where list returns are valid                                                                         |
| ----------------- | ----------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| `NodeVisitor`     | forwarded to caller; does not imply replacement | n/a                                                                                                  |
| `NodeTransformer` | replacement/removal protocol                    | only when the original node is in a `stmt*` list (statement collections) ([Python documentation][1]) |

**Practical guardrail:** list returns are structurally correct only when the parent field’s schema is `stmt*` (e.g., `Module.body`, `FunctionDef.body`, `If.orelse`, etc.). The docs phrase this as “collection of statements”. ([Python documentation][1])

---

## 3.4 The list-splice rule (statement splitting) — canonical example

Goal: rewrite a single expression statement `f(x)` into:

1. `_tmp = f(x)`
2. `use(_tmp)`

```python
import ast

class SplitExpr(ast.NodeTransformer):
    def visit_Expr(self, node: ast.Expr):
        # IMPORTANT: Expr is a stmt; safe to return a list of stmt nodes here. :contentReference[oaicite:17]{index=17}
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name) and node.value.func.id == "f":
            tmp_name = ast.Name(id="_tmp", ctx=ast.Store())
            assign = ast.Assign(targets=[tmp_name], value=node.value)

            use = ast.Expr(
                value=ast.Call(
                    func=ast.Name(id="use", ctx=ast.Load()),
                    args=[ast.Name(id="_tmp", ctx=ast.Load())],
                    keywords=[],
                )
            )
            # preserve source locations as a block (or use copy_location per node)
            assign = ast.copy_location(assign, node)
            use = ast.copy_location(use, node)
            return [assign, use]
        return self.generic_visit(node)

tree = ast.parse("f(1)\n")
tree2 = ast.fix_missing_locations(SplitExpr().visit(tree))
compile(tree2, "<mem>", "exec")
```

Why the `fix_missing_locations` step is a first-class part of the protocol: compilation expects `lineno`/`col_offset` on nodes that support them, and NodeTransformer explicitly calls this out for newly introduced nodes. ([Python documentation][1])

---

## 3.5 Deletion semantics (`return None`) and the “required-field” trap

`return None` means “remove from its location”. ([Python documentation][1])
That is clean when the parent field is a `stmt*` list (you’re just dropping an element). It is hazardous when the parent expects a **required single child** (e.g., `If.test`, `Return.value` when not optional, etc.), because compilation requires required fields to be present and valid. ([Python documentation][1])

**Rule of thumb:** only use `return None` for:

* statement deletion in statement lists, or
* truly optional node-valued fields (schema `expr?`, `stmt?`, etc.). ([Python documentation][1])

---

## 3.6 Drift hook: constants visitor methods

In 3.14+, the legacy `visit_Num/visit_Str/visit_Bytes/visit_NameConstant/visit_Ellipsis` are not called; implement `visit_Constant` instead. ([Python documentation][1])

---

# 4) Node attribute invariants + grammar drift (schema-level realities)

## Mental model

Native AST is a **versioned schema** (ASDL-derived). You should treat:

* node classes + fields
* constructor behavior
* location attribute rules
  as *API surface* that drifts across Python releases. The docs explicitly warn that the abstract syntax can change each release. ([Python documentation][1])

---

## 4.1 Schema invariants: `_fields`, `_field_types`, required-vs-optional, lists

### `_fields` (child-field enumeration)

Each concrete node class has:

* `Class._fields`: ordered names of child fields. ([Python documentation][1])
  Instances have one attribute per child field, typed per grammar:
* `?` → value may be `None`
* `*` → Python `list` (possibly empty) ([Python documentation][1])
  **Compilation invariant:** all required attributes must be present and valid when compiling an AST. ([Python documentation][1])

### `_field_types` (introspectable type schema; 3.13+)

Each concrete class also has:

* `Class._field_types: dict[str, type]` mapping field names to types (including unions like `ast.expr | None`). ([Python documentation][1])

This enables “schema-driven” tooling (auto-validators, generic walkers) without hardcoding field names.

---

## 4.2 Constructor invariants (and the 3.15 tightening)

Constructor parsing rules: positional args must match `_fields` length; keyword args set same-named fields. ([Python documentation][1])

Defaults when omitting fields:

* optional (`?`) omitted → `None`
* list (`*`) omitted → `[]`
* `expr_context` omitted → defaults to `Load()` ([Python documentation][1])

**Important tightening:** creating nodes missing *other required* fields triggers `DeprecationWarning` and yields a node missing that field; in 3.15 this becomes an error. ([Python documentation][1])
Also deprecated: arbitrary keyword args that don’t match fields (removal planned in 3.15). ([Python documentation][1])

**Implication for transformers:** “returning nodes with missing required fields” is now increasingly non-viable; treat schema correctness as mandatory, not “best effort”.

---

## 4.3 Location attributes: what exists, what’s required, what’s optional

For `ast.expr` and `ast.stmt` subclasses:

* `lineno`, `col_offset`, `end_lineno`, `end_col_offset`
* `col_offset` / `end_col_offset` are **UTF-8 byte offsets** (not codepoint offsets), because the parser uses UTF-8 internally. ([Python documentation][1])
* end positions are **optional** and not required by the compiler. ([Python documentation][1])

### Pattern nodes are different (harder invariant)

In the abstract grammar, `pattern` nodes specify attributes with **non-optional** end positions (`end_lineno`, `end_col_offset` without `?`). ([Python documentation][1])
This matters if you synthesize patterns (rare, but relevant for codegen tools): you must populate full spans.

### Type parameter nodes are also span-strict

`type_param` nodes (`TypeVar`, `ParamSpec`, `TypeVarTuple`) also specify non-optional end positions in the grammar. ([Python documentation][1])

---

## 4.4 Operator/context singleton invariant (mutation footgun)

When parsing a string, operator-ish nodes are **singletons** within the returned tree:

* `ast.operator`, `ast.unaryop`, `ast.cmpop`, `ast.boolop`, `ast.expr_context`
  Mutating one instance affects all occurrences of that operator value across the tree. ([Python documentation][1])

**Operational rule:** treat these nodes as immutable tokens; always replace rather than mutate.

---

## 4.5 Major grammar drift landmarks you should track

### (A) Constant unification + removal of legacy literal nodes (3.14)

* Since 3.8, `ast.Constant` represents all constants. ([Python documentation][1])
* In 3.14, `ast.Num`, `ast.Str`, `ast.Bytes`, `ast.NameConstant`, `ast.Ellipsis` were removed; use `ast.Constant`. ([Python documentation][1])
* Corresponding visitor hooks are gone; `visit_Constant` is the canonical handler. ([Python documentation][1])

### (B) Type parameter syntax surfaced into AST (3.12+)

The grammar adds `type_params` to:

* `FunctionDef`, `AsyncFunctionDef`, `ClassDef` (and `TypeAlias`) ([Python documentation][1])
  Docs explicitly note `type_params` added in 3.12 for these nodes. ([Python documentation][1])

The `type_param` node variants exist and have evolved:

* `TypeVar(name, bound, default_value)`
* `ParamSpec(name, default_value)`
* `TypeVarTuple(name, default_value)` ([Python documentation][1])
  …and `default_value` was added in 3.13 (per node docs). ([Python documentation][1])

### (C) Pattern matching introduces its own node family (3.10+)

Grammar-level presence:

* `Match(subject, cases)`
* `match_case(pattern, guard, body)`
* `pattern` variants (`MatchValue`, `MatchSingleton`, `MatchSequence`, `MatchMapping`, `MatchClass`, `MatchStar`, `MatchAs`, `MatchOr`) ([Python documentation][1])
  …and note the span strictness for patterns (above). ([Python documentation][1])

### (D) Exception groups (`except*`) add `TryStar`

`TryStar` exists alongside `Try` in the grammar. ([Python documentation][1])

### (E) Template strings add new node types (3.14)

Grammar shows `Interpolation` and `TemplateStr`, and node docs explicitly mark `TemplateStr` added in 3.14. ([Python documentation][1])

### (F) Representation / diagnostics drift (3.14)

`ast.AST.__repr__` changed in 3.14 to include node field values, affecting logs and snapshot tests that depended on older repr shape. ([Python documentation][1])

### (G) Subscripting drift (3.9+)

Simple indices are represented directly by their value; extended slices are tuples (deprecating older `Index` / `ExtSlice` shapes). ([Python documentation][1])
Those old classes remain deprecated. ([Python documentation][1])

---

If you want the next natural follow-on after these two sections, it’s usually:

* a **“schema-first manipulation cookbook”** (ctx correctness, statement vs expression replacement legality, span/offset propagation), and
* a **“version drift watchlist”** organized by *node family* (literals, typing, match, strings, exceptions), so an agent can quickly patch visitors/transformers when bumping Python versions.

[1]: https://docs.python.org/3/library/ast.html "ast — Abstract syntax trees — Python 3.14.2 documentation"


# 5) Node-class catalog (the “what exists” inventory)

## 5.0 Schema primitives (how to read the catalog fast)

* All concrete nodes inherit `ast.AST`; each concrete class exposes:

  * `T._fields` (ordered child-field names)
  * `T._field_types` (field → type; added 3.13) ([Python documentation][1])
* Field cardinality in the published ASDL grammar:

  * `X?` ⇒ optional (`None` allowed)
  * `X*` ⇒ Python list (possibly empty)
  * Required fields must be present/valid for `compile(ast_obj, ...)`. ([Python documentation][1])

### Programmatic inventory extraction (drift-proof)

Use this to mechanically derive “what node classes exist” for the running interpreter:

```python
import ast, inspect

nodes = sorted(
    (name, obj) for name, obj in vars(ast).items()
    if inspect.isclass(obj) and issubclass(obj, ast.AST)
)

# names only
print([name for name, _ in nodes])

# schema-ish view
print({name: getattr(obj, "_fields", ()) for name, obj in nodes})
print({name: getattr(obj, "_field_types", None) for name, obj in nodes})  # 3.13+
```

(You then layer the *semantic* grouping below on top.)

---

## 5.1 Root nodes (`mod`)

These are the only valid AST roots returned by `ast.parse(..., mode=...)` / `compile(..., flags=PyCF_ONLY_AST|PyCF_OPTIMIZED_AST)`.

* `Module(body: stmt*, type_ignores: type_ignore*)` — root for file input (`mode="exec"`). `type_ignores` is populated only when parsing type comments. ([Python documentation][1])
* `Expression(body: expr)` — root for expression input (`mode="eval"`). ([Python documentation][1])
* `Interactive(body: stmt*)` — root for REPL-ish “single” mode. ([Python documentation][1])
* `FunctionType(argtypes: expr*, returns: expr)` — root for PEP 484 “signature type comment” syntax (`mode="func_type"`). ([Python documentation][1])

---

## 5.2 Literals (constant-ish payload nodes)

### 5.2.1 Unified scalar literals

* `Constant(value)` where `value ∈ {str, bytes, int, float, complex, bool, None, Ellipsis}`. (Legacy `Num/Str/...` removed in 3.14.) ([Python documentation][1])

### 5.2.2 f-strings (AST-level formatting IR)

* `FormattedValue(value, conversion, format_spec)`:

  * `conversion ∈ {-1, ord('a'), ord('r'), ord('s')}`
  * `format_spec` is a `JoinedStr` or `None`. ([Python documentation][1])
* `JoinedStr(values)` where `values` is a sequence of `FormattedValue` and `Constant`. ([Python documentation][1])

### 5.2.3 Template strings (3.14+; distinct from f-strings)

* `TemplateStr(values, /)` — values are `Interpolation` and `Constant`, any order (not required to alternate). ([Python documentation][1])
* `Interpolation(value, str, conversion, format_spec=None)`:

  * `str` is the *text* of the interpolation expression (constant); if `str is None`, `value` is used for codegen during `ast.unparse()`, explicitly dropping “original source identity” guarantees. ([Python documentation][1])

### 5.2.4 Container literals

* `List(elts, ctx)` / `Tuple(elts, ctx)` — note `ctx` participates (Load/Store) and matters in assignment/unpacking. ([Python documentation][1])
* `Set(elts)` ([Python documentation][1])
* `Dict(keys, values)`:

  * `keys[i] is None` encodes dict-unpacking (`**expr`) at `values[i]`. ([Python documentation][1])

---

## 5.3 Variables, binding targets, and context tokens

* `Name(id, ctx)` where `ctx ∈ {Load, Store, Del}` distinguishes read vs write vs delete. ([Python documentation][1])
* `Starred(value, ctx)` — required to represent `*x` in assignment targets and call args; docs explicitly call out it “must be used” when building `Call` with `*args`. ([Python documentation][1])
* Context tokens: `Load`, `Store`, `Del`. ([Python documentation][1])

---

## 5.4 Expressions (`expr`) — compute-shaped nodes

Below is a coverage map; signatures follow the stdlib docs.

### 5.4.1 Statement-wrapper for bare expressions

* `Expr(value)` — a *statement* wrapper used when an expression appears as a statement (e.g., call result unused). ([Python documentation][1])

### 5.4.2 Operators (node + token nodes)

* `UnaryOp(op, operand)` with tokens: `UAdd`, `USub`, `Not`, `Invert`. ([Python documentation][1])
* `BinOp(left, op, right)` with tokens: `Add`, `Sub`, `Mult`, `Div`, `FloorDiv`, `Mod`, `Pow`, `LShift`, `RShift`, `BitOr`, `BitXor`, `BitAnd`, `MatMult`. ([Python documentation][1])
* `BoolOp(op, values)` with tokens: `And`, `Or`. ([Python documentation][1])
* `Compare(left, ops, comparators)` with tokens: `Eq`, `NotEq`, `Lt`, `LtE`, `Gt`, `GtE`, `Is`, `IsNot`, `In`, `NotIn`. ([Python documentation][1])

### 5.4.3 Calls, attributes, and named expressions

* `Call(func, args, keywords)`

  * `args` includes `Starred(...)` for `*args`
  * `keywords` contains `keyword(arg=None, value=...)` for `**kwargs`. ([Python documentation][1])
* `keyword(arg, value)` — `arg` is parameter name string or omitted/`None` for `**`. ([Python documentation][1])
* `Attribute(value, attr, ctx)` — attribute access is context-sensitive (Load/Store/Del). ([Python documentation][1])
* `NamedExpr(target, value)` — walrus; both sides are single nodes (unlike `Assign.targets`). ([Python documentation][1])

### 5.4.4 Conditional expression

* `IfExp(test, body, orelse)` — the ternary `a if b else c`. ([Python documentation][1])

### 5.4.5 Subscripting / slicing

* `Subscript(value, slice, ctx)` — `slice` can be:

  * an index/key expression node,
  * a `Slice(lower, upper, step)`,
  * or a `Tuple` that mixes `Slice` and index exprs for multidimensional slicing. ([Python documentation][1])
* `Slice(lower, upper, step)` — each field is optional expr. ([Python documentation][1])

### 5.4.6 Comprehensions (and their generator descriptor)

* `ListComp(elt, generators)`
* `SetComp(elt, generators)`
* `DictComp(key, value, generators)`
* `GeneratorExp(elt, generators)`
* `comprehension(target, iter, ifs, is_async)` — `is_async` encodes async comprehensions. ([Python documentation][1])

### 5.4.7 Function-ish expressions and suspension points

* `Lambda(args, body)`; uses `arguments(...)` (same schema as defs) but body is a single expr. ([Python documentation][1])
* `Yield(value)` / `YieldFrom(value)` — expression nodes; if used as a statement, they appear under `Expr(...)`. ([Python documentation][1])
* `Await(value)` — expression node; only valid in async function bodies. ([Python documentation][1])

---

## 5.5 Statements (`stmt`) — syntax that drives control flow / scope / binding

### 5.5.1 Assignments and type-bearing writes

* `Assign(targets, value, type_comment)`:

  * multiple targets = chained assignment
  * unpacking encoded by embedding `Tuple`/`List` inside `targets`. ([Python documentation][1])
* `AnnAssign(target, annotation, value, simple)`:

  * `simple ∈ {0,1}`; only `simple=1` contributes to module/class `__annotations__` (Name target, no parens). ([Python documentation][1])
* `AugAssign(target, op, value)` — `op` is one of the binary operator tokens. ([Python documentation][1])

### 5.5.2 Imports

* `Import(names: alias*)`
* `ImportFrom(module, names: alias*, level)`:

  * `module=None` for `from . import foo`
  * `level` is relative import level (0 absolute). ([Python documentation][1])
* `alias(name, asname)` — `asname=None` means no aliasing. ([Python documentation][1])

### 5.5.3 Control flow (blocky statements)

Docs-level invariant: optional clauses (`else`, etc.) are represented as empty lists when absent. ([Python documentation][1])

* `If(test, body, orelse)`:

  * `elif` is represented as an `If` nested inside the previous node’s `orelse`. ([Python documentation][1])
* `For(target, iter, body, orelse, type_comment)`:

  * `orelse` runs only on “normal completion” (i.e., no `break`). ([Python documentation][1])
* `While(test, body, orelse)` ([Python documentation][1])
* `Break`, `Continue` ([Python documentation][1])

### 5.5.4 Exceptions and context management

* `Try(body, handlers, orelse, finalbody)` where `handlers: ExceptHandler*`. ([Python documentation][1])
* `TryStar(body, handlers, orelse, finalbody)` — same shape as `Try`, but handlers are interpreted as `except*` blocks. ([Python documentation][1])
* `ExceptHandler(type, name, body)`:

  * `type=None` for bare `except:`
  * `name` is string or `None`. ([Python documentation][1])
* `Raise(exc, cause)` — `cause` represents `raise X from Y`. ([Python documentation][1])
* `Assert(test, msg)` ([Python documentation][1])
* `With(items, body, type_comment)`:

  * `items: withitem*` ([Python documentation][1])
* `withitem(context_expr, optional_vars)`:

  * `optional_vars=None` means no `as ...`. ([Python documentation][1])

### 5.5.5 Pattern matching (3.10+; separate `pattern` family)

* `Match(subject, cases: match_case*)` ([Python documentation][1])
* `match_case(pattern, guard, body)`:

  * patterns are **not expressions** even when syntax overlaps; guard is an expression. ([Python documentation][1])

Pattern nodes (`pattern`):

* `MatchValue(value)` — equality match against evaluated value expr. ([Python documentation][1])
* `MatchSingleton(value)` — identity match against `None/True/False`. ([Python documentation][1])
* `MatchSequence(patterns)` — sequence destructuring; variable-length via `MatchStar`. ([Python documentation][1])
* `MatchStar(name)` — captures “rest”; `name=None` also encodes bare-star wildcard cases. ([Python documentation][1])
* `MatchMapping(keys, patterns, rest)` — mapping destructuring; `rest` captures remainder mapping. ([Python documentation][1])
* `MatchClass(cls, patterns, kwd_attrs, kwd_patterns)` — class pattern; positional + keyword attribute matching. ([Python documentation][1])
* `MatchAs(pattern, name)` — capture / as-pattern / wildcard:

  * `pattern=None,name=<id>` ⇒ capture name (always succeeds)
  * `pattern=None,name=None` ⇒ `_` wildcard. ([Python documentation][1])
* `MatchOr(patterns)` — ordered OR across subpatterns. ([Python documentation][1])

### 5.5.6 Type-alias statement + type ignores

* `TypeAlias(name, type_params, value)` — produced by the `type` statement (3.12+). ([Python documentation][1])
* `TypeIgnore(lineno, tag)` — stored in `Module.type_ignores`; only produced when parsing type comments. ([Python documentation][1])

### 5.5.7 Type parameters (3.12+, defaults 3.13+)

These are the elements of `type_params` on `FunctionDef`/`ClassDef`/`TypeAlias`. ([Python documentation][1])

* `TypeVar(name, bound, default_value)`

  * `bound` is either a `Tuple` (constraints) or a single bound expr. ([Python documentation][1])
* `ParamSpec(name, default_value)` ([Python documentation][1])
* `TypeVarTuple(name, default_value)` ([Python documentation][1])

### 5.5.8 Function/class definitions and their “glue nodes”

* `FunctionDef(name, args, body, decorator_list, returns, type_comment, type_params)` ([Python documentation][1])
* `AsyncFunctionDef(...)` — same fields as `FunctionDef`. ([Python documentation][1])
* `ClassDef(name, bases, keywords, body, decorator_list, type_params)`:

  * `keywords` is mainly for `metaclass`, but is generic `keyword*`. ([Python documentation][1])
* `arguments(posonlyargs, args, vararg, kwonlyargs, kw_defaults, kwarg, defaults)`:

  * `kw_defaults[i] is None` ⇒ kw-only arg is required. ([Python documentation][1])
* `arg(arg, annotation, type_comment)` ([Python documentation][1])

### 5.5.9 Misc statements

* `Return(value)` ([Python documentation][1])
* `Delete(targets)` ([Python documentation][1])
* `Pass` ([Python documentation][1])
* `Global(names)`, `Nonlocal(names)` ([Python documentation][1])

### 5.5.10 Async control-flow statements

* `AsyncFor(target, iter, body, orelse, type_comment)` — same fields as `For`. ([Python documentation][1])
* `AsyncWith(items, body, type_comment)` — same fields as `With`. ([Python documentation][1])

---

## 5.6 Token-node families (operator/context atoms)

These are “leaf” nodes that act like enums:

* `expr_context`: `Load`, `Store`, `Del` ([Python documentation][1])
* `boolop`: `And`, `Or` ([Python documentation][1])
* `unaryop`: `UAdd`, `USub`, `Not`, `Invert` ([Python documentation][1])
* `operator`: `Add`, `Sub`, `Mult`, `Div`, `FloorDiv`, `Mod`, `Pow`, `LShift`, `RShift`, `BitOr`, `BitXor`, `BitAnd`, `MatMult` ([Python documentation][1])
* `cmpop`: `Eq`, `NotEq`, `Lt`, `LtE`, `Gt`, `GtE`, `Is`, `IsNot`, `In`, `NotIn` ([Python documentation][1])

---

# 6) Command-line interface (built-in)

## 6.1 Surface contract

`ast` is executable as a script:

```bash
python -m ast [-m <mode>] [-a] [infile]
```

* If `infile` is present: parse file contents; else read from stdin; dump AST to stdout. ([Python documentation][1])
* Added in 3.9; in 3.14 it gained `--feature-version`, `--optimize`, `--show-empty`. ([Python documentation][1])

## 6.2 Options (and what knob they map to)

The docs enumerate:

* `-m/--mode <mode>` — parse mode like `ast.parse(mode=...)` (`exec|eval|single|func_type`). ([Python documentation][1])
* `--no-type-comments` — disables parsing type comments. ([Python documentation][1])
* `-a/--include-attributes` — include location attributes in the dump output. ([Python documentation][1])
* `-i/--indent <indent>` — indentation spaces for pretty dump. ([Python documentation][1])
* `--feature-version 3.x` — parse using best-effort older grammar. ([Python documentation][1])
* `-O/--optimize <level>` — parser optimization level (exposes `ast.parse(..., optimize=...)` behavior). ([Python documentation][1])
* `--show-empty` — show empty lists and `None` fields (mirrors `ast.dump(show_empty=...)`). ([Python documentation][1])

## 6.3 Usage patterns (CLI as a “schema probe”)

### (A) “what did the parser produce?” with stable indentation

```bash
python -m ast -i 2 path/to/file.py
```

(Defaults: no attributes, no empty fields.) ([Python documentation][1])

### (B) Precise span debugging (line/col offsets)

```bash
python -m ast -a -i 2 path/to/file.py
```

Uses `--include-attributes` to surface `lineno/col_offset/...`. ([Python documentation][1])

### (C) Grammar-gating in pipelines (feature-version)

```bash
python -m ast --feature-version 3.9 -m exec path/to/file.py
```

A fast way to test “would this parse under 3.9 grammar?” without running a 3.9 interpreter. ([Python documentation][1])

### (D) Dump “full shape” (empty lists + None fields)

```bash
python -m ast --show-empty -i 2 path/to/file.py
```

Useful when you treat absence vs empty list as a meaningful distinction in snapshots. ([Python documentation][1])

[1]: https://docs.python.org/3/library/ast.html "ast — Abstract syntax trees — Python 3.14.2 documentation"


# 7) Version drift notes (things experts still trip on)

## 7.0 Treat `ast` as a *versioned schema*, not a stable IR

* The stdlib explicitly warns that the **abstract syntax can change with each Python release**; the only safe stance is “runtime-introspect the schema you’re executing against.” ([Python documentation][1])
* Practical consequence: any tool that hardcodes node-class sets, field lists, or dump/`repr` formats without version-gating eventually “silently rots.”

Minimal “drift sentry” (run at import time in tooling):

```python
import ast, sys

FEATURES = {
    "ast.compare": hasattr(ast, "compare"),
    "PyCF_OPTIMIZED_AST": hasattr(ast, "PyCF_OPTIMIZED_AST"),
    "TemplateStr": hasattr(ast, "TemplateStr"),
    "_field_types": hasattr(ast.AST, "_field_types") or hasattr(ast, "TypeVar") and hasattr(ast.TypeVar, "_field_types"),
}

print(sys.version_info[:3], FEATURES)
```

(Then branch your behavior off `FEATURES` rather than `sys.version_info` when possible.)

---

## 7.1 High-signal drift timeline (3.8 → 3.15)

### 3.8

* **Constant unification**: all constants represented as `ast.Constant`. ([Python documentation][1])

### 3.9

* **Subscript shape change**: “simple indices are represented by their value; extended slices are represented as tuples.” (This is why `ast.Index` / `ast.ExtSlice` became legacy.) ([Python documentation][1])
* `ast.unparse()` added. ([Python documentation][1])
* `ast.dump(..., indent=...)` added. ([Python documentation][1])
* `python -m ast` CLI added. ([Python documentation][1])

### 3.12

* **Type parameter syntax reaches AST**: `type_params` added to defs (functions/classes) and related nodes. ([Python documentation][1])

### 3.13

* **Optimized-AST pathway**: `ast.PyCF_OPTIMIZED_AST` added; `ast.parse(..., optimize=...)` added. ([Python documentation][1])
* `_field_types` added on node classes (schema reflection becomes first-class). ([Python documentation][1])
* `ast.dump(..., show_empty=...)` added. ([Python documentation][1])
* `feature_version` minimum supported version bumped to `(3, 7)` (and may increase later). ([Python documentation][1])
* Type-parameter nodes gained `default_value` in several cases. ([Python documentation][1])

### 3.14 (released **7 Oct 2025**)

* AST node `__repr__` now includes field values (log/snapshot churn). ([Python documentation][1])
* Deprecated literal nodes (`Num/Str/Bytes/NameConstant/Ellipsis`) **removed**; functionality replaced by `Constant`. ([Python documentation][1])
* Legacy visitor hooks for those literals **no longer called**; use `visit_Constant`. ([Python documentation][1])
* `ast.compare(...)` added (structural equality becomes first-class). ([Python documentation][1])
* `python -m ast` gained `--feature-version`, `--optimize`, `--show-empty`. ([Python documentation][1])
* Template string AST nodes added: `TemplateStr`, `Interpolation`. ([Python documentation][1])

### 3.15 (upcoming behavior cliff)

* Creating nodes **missing required fields** (previously tolerated) becomes an error in 3.15; arbitrary keyword attributes on node constructors also removed in 3.15. ([Python documentation][1])

---

## 7.2 Drift traps (symptom → root cause → robust pattern)

### Trap A: “Why did my visitor stop seeing numbers/strings in 3.14?”

**Symptom**: `visit_Num/visit_Str/...` never fires (and `ast.Num` doesn’t exist).
**Root cause**: those nodes were deprecated since 3.8 and removed in 3.14; constants are `ast.Constant`. ([Python documentation][1])

**Robust pattern** (cross-version, including user-constructed legacy nodes):

```python
import ast

class V(ast.NodeVisitor):
    def visit_Constant(self, node: ast.Constant):
        ...

    # harmless in 3.14+ (won’t be called), helpful if you ingest older / handcrafted ASTs:
    visit_Num = visit_Str = visit_Bytes = visit_NameConstant = visit_Ellipsis = visit_Constant
```

The docs explicitly recommend `visit_Constant` for 3.14+. ([Python documentation][1])

---

### Trap B: “My code expects `ast.Index` / `ast.ExtSlice` but I’m seeing raw expr / tuples”

**Symptom**: subscript handling breaks across 3.8–3.14; `slice` isn’t an `Index`.
**Root cause**: since 3.9, simple indices are “just the value” and extended slices are tuples; `ast.Index` and `ast.ExtSlice` are deprecated and instantiating them returns a different class. ([Python documentation][1])

**Robust pattern** (normalize to “modern slice model”):

```python
import ast

def normalize_subscript_slice(s: ast.expr) -> tuple[ast.expr | ast.Slice, ...]:
    # Modern shape: expr | Slice | Tuple(expr|Slice,...)
    if isinstance(s, ast.Tuple):
        return tuple(s.elts)
    return (s,)
```

---

### Trap C: “My AST node constructor hacks worked yesterday; now I get warnings / future errors”

**Symptom**: `DeprecationWarning` (and later hard errors) when creating nodes; code relied on missing required fields or passing extra keyword attributes.
**Root cause**: since 3.13 this behavior is deprecated and **scheduled for removal in 3.15**; missing non-optional fields becomes an error in 3.15. ([Python documentation][1])

**Robust pattern**: schema-driven construction (reject unknown kwargs; ensure required fields present).

```python
import ast

def strict_ctor(cls: type[ast.AST], /, **kwargs):
    allowed = set(getattr(cls, "_fields", ()))
    extra = set(kwargs) - allowed
    if extra:
        raise TypeError(f"{cls.__name__}: unknown fields: {sorted(extra)}")
    node = cls(**kwargs)  # relies on stdlib constructor rules
    # Optional: validate presence of all fields in _fields
    missing = [f for f in cls._fields if not hasattr(node, f)]
    if missing:
        raise TypeError(f"{cls.__name__}: missing fields: {missing}")
    return node
```

Constructor semantics and the 3.15 cliff are spelled out in the docs. ([Python documentation][1])

---

### Trap D: “My snapshot tests exploded in 3.14 without code changes”

**Symptom**: stable snapshots based on `repr(node)` start changing everywhere.
**Root cause**: AST `__repr__` changed in 3.14 to include field values. ([Python documentation][1])

**Robust pattern**:

* Use `ast.dump(..., include_attributes=False, show_empty=<explicit>)` for stable-ish structural snapshots; `show_empty` exists since 3.13. ([Python documentation][1])
* Prefer `ast.compare(...)` for equality assertions in 3.14+ (structure-first; optionally include attributes). ([Python documentation][1])

Compat shim:

```python
import ast

def ast_equal(a: ast.AST, b: ast.AST) -> bool:
    if hasattr(ast, "compare"):
        return ast.compare(a, b)  # 3.14+ :contentReference[oaicite:27]{index=27}
    # pre-3.14 fallback: dump-based structural compare (choose policy!)
    return ast.dump(a, include_attributes=False) == ast.dump(b, include_attributes=False)
```

---

### Trap E: “`ast.parse` is returning ‘different’ trees on newer versions / with optimize”

**Symptom**: instrumentation sees “missing docstrings/asserts” or folded constants; diffs between environments.
**Root cause**:

* 3.13 added `ast.parse(..., optimize=...)` and `PyCF_OPTIMIZED_AST`; optimized AST depends on `optimize`. ([Python documentation][1])

**Robust pattern**: pin `optimize=0` for “semantic AST” pipelines unless you *explicitly* want compiler-side normalization.

---

### Trap F: “I used `feature_version` to emulate old Python but behavior still differs”

**Symptom**: “should fail” constructs sometimes parse; “should succeed” constructs sometimes fail.
**Root cause**: docs guarantee only “best-effort,” and explicitly state it may not match real old-version parsing success/failure; minimum supported `feature_version` is now `(3, 7)` and may increase. ([Python documentation][1])

**Robust stance**:

* Use `feature_version` as a **syntax gate** (“disallow newer constructs”), not a compatibility emulator.

---

### Trap G: “My token mapping is off by N chars in Unicode-heavy code”

**Symptom**: slicing source by `col_offset`/`end_col_offset` misaligns on non-ASCII.
**Root cause**: offsets are **UTF-8 byte offsets**, not Unicode codepoint indices. ([Python documentation][1])

**Robust stance**:

* Treat offsets as byte indices into the UTF-8 encoded source (or use helpers like `ast.get_source_segment` that rely on span attrs). ([Python documentation][1])

---

### Trap H: “My tool never heard of Template Strings / Interpolation; now it breaks”

**Symptom**: new node classes appear (`TemplateStr`, `Interpolation`) and your exhaustive visitor throws / misses formatting nodes.
**Root cause**: template string literals landed in 3.14 and introduced these nodes. ([Python documentation][1])

**Robust pattern**: add “unknown-expression family” fallbacks + feature probes:

```python
import ast

def is_template_string(node: ast.AST) -> bool:
    return hasattr(ast, "TemplateStr") and isinstance(node, ast.TemplateStr)
```

---

### Trap I: “Type syntax became AST syntax: new fields, new nodes, new spans”

**Symptom**: `FunctionDef`/`ClassDef` suddenly have `type_params`; tools treating unknown fields as errors break.
**Root cause**: `type_params` added in 3.12; type-parameter nodes exist (`TypeVar`, `ParamSpec`, `TypeVarTuple`) and gained `default_value` in 3.13. ([Python documentation][1])

**Robust stance**:

* Never assume `_fields` is fixed; always iterate `node._fields` (or `iter_fields`) rather than hardcoding field lists. ([Python documentation][1])

---

## 7.3 Two “meta” gotchas that look like drift (but are really boundary confusion)

### Parse vs compile semantics (experts still conflate them)

* `ast.parse()` can succeed for code that cannot compile standalone (no scoping checks at parse time; compilation may raise additional `SyntaxError`). ([Python documentation][1])

### Stack-depth crash risk (not theoretical)

* The docs warn that sufficiently large/complex inputs can crash the interpreter (AST compiler recursion/stack depth), including in `ast.parse` and `literal_eval`. ([Python documentation][1])

[1]: https://docs.python.org/3/library/ast.html "ast — Abstract syntax trees — Python 3.14.2 documentation"
