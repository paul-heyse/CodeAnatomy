According to a document from your project files (date not specified), once you’re already extracting the “max tree-sitter surface” (CST structure + spans + queries/locals conventions), the **incremental** value of adding **`symtable` / `dis` / `inspect`** is that they give you three extra, non-overlapping semantics planes: **compile-time scope truth**, **post-compile execution substrate (bytecode + exception table)**, and **runtime reflection over live objects**. ([Python documentation][1])

Below is a prioritized “LLM programming agent” view: the highest-leverage new insights first, then the full inventory by module.

---

## Highest-leverage incremental insights for LLM programming agents

1. **Compiler-grade “what binding is this name?”**
   Deterministic per-identifier scope classification (local/param/global/declared_global/nonlocal/free/imported/annotated) and a scope tree that matches what CPython will compile, not what a syntax heuristic guesses.([Python documentation][1])

2. **A complete lexical binding graph (RESOLVES_TO / CAPTURES)**
   Explicit edges for `GLOBAL|NONLOCAL|FREE` resolution (and optional `CAPTURES`) so an agent can safely reason about closure capture, shadowing, and “rename impact” without re-deriving Python scoping rules from syntax.on that unifies: AST Name(Store/Load) edges, dis def/use events, and “SCIP missing” fallbacks—this is the backbone for safe refactors and high-confidence edits.

3. **Bytecode-grounded CFG including exception flow**
   co_exceptiontable` (3.11+), so try/except/finally/with control flow stops being guesswork—crucial for bug fixing, reasoning about invariants, and safe code motion.([Python documentation][2])

4. **Bytecode-grounded  events (FAST/NAME/GLOBAL/DEREF spaces) → reaching definitions over CFG → concrete `REACHES(def → use)` edges. This enables “what values can reach here?” queries and robust “is this edit safe?” checks far beyond syntax neighborhoods.

5. **Decorator reality: unwrap chain + effective si(`follow_wrapped=False/True`) so an agent can correctly update call sites and docs even when decorators lie to syntax-only tooling. ([Python documentation][3])

6. **Static-safe class/module on) plus descriptor classification (“property/data/method/getset/member/…”)—key for safe API changes, monkeypatch reasoning, and “where does this attribute come from?” answers.

7. **Annotations as data (safe-by-default)**
   `get_annotations(eval_str=False)` and signature extraction with explicit eval policy so agents can reason about types/annotations w

8. **Runtime-state-to-source joins (frames, tracebacks, generators/coros)**
   Frame/traceback/generator/coroutine local source spans.

---

## Full incremental insight inventory (by module)

### A) `symtable` adds compile-time semantic truth (beyond CST heuroperties like `is_nested()`, `is_optimized()`, etc.

* Modern typing/annotation meta-scopes: `ANNOTATION`, `TYPE_ALIAS`, `TYPE_PARAMETERS`, `TYPE_VARIABLE` (3.13 adds TYPE_VARI ([Python documentation][1])

**Per-name classification flags (compiler-grade)**

* `Symbol` flags: referenced/assigned/imported/ann

**Function partitions (agent-friendly)**

* Ordered partitions: `get_parameters/get_locals/get_globals/get_nonlocals/get_frees` — especially `get_frees()` as the “closure capture list”.pace binding for defs/classes**
* `is_namespace()` + `get_namespace(s)` lets you jump name → child table deterministically (core for building lexical scope graphs).me)`and explicit`RESOLVES_TO`edges for`GLOBAL/NONLOCAL/FREE`(+ optional`CAPTURES`).
* A single `resolve_binding(scope_id, name)` used by AST def/use attachment, bytecode def/use landing, and heuristic call stitching when SCIP is missing.racle / invariants**
* Validation gates that catch binder drift: e.g., `LOAD_FAST` names must be local/parameter; freevars must align; nonlocal/global must resolve appropriately. cannot get from syntax alone

**Instruction-level facts (normalized)**

* Stable “semantic opcode” via `baseopname`; model inline caches/specialization as debug/perf artifacts; avoid using raw of ([Python documentation][2])

**Exception table semantics (try regions → handlers)**

* CPython 3.11+ moves try/except mechanics to an exception table; parsing `co_exceptiontable` is mandatory normal + exceptional)**
* Basic blocks from labels + terminators; normal edges (fallthrough/jump/branch) with consistent polarity heuristics; plus exception edges `CFG_EXC` projected from exception table ranges into handler blocks.

**Def/use events → reaching defs**

* A derived def/use event table (USE_SLOT/DEF_SLOT/KILL_SLOT, plus attr-ish memory ops) including binding “spaces” FAST/NAME/GLOBAL/DEREF; then reaching-defs over CFG to produce `REACHES` edges.spaces (FAST/DEREF/GLOBAL) to the same binding nodes used by AST, making DFG coherent and queryable.

**Span aplus CFG/DFG sanity checklists (anchoring coverage, handler reachability, “every LOAD has defs or is flagged”).es, and “what exists”

**Object inventory (what *actually* exists)**

* A table of introspected objects (module/class/function/builtin/descriptor/etc.) with flags like `is_builtin`,

**Static member enumeration + descriptor classification**

* `getmembers_static`-style enumeration per `(owner, attr)` while preserving descriptors and classifying descriptor kinds—critical for attribute provenance and safe API edits.

**Wrapper chan), plus the “effective signature” semantics that may stop unwrapping when `__signature__` is present; store raw vs wrapped signature variants for queryable decorator effects.

**Signatures, parameters, binding simulation**

* `Signature.parameters`, `Parameter.kind`, `BoundArguments.bind/bind_partial`, `apply_defaults()`—lets an agent safely add/reorder parameters and update call sites with fewer mistakes than syntax-onl_annotations(eval_str=False)` + separate explicit unsafe mode if you ever evaluate strings; avoids unintended code execution while still extracting type metadata.

**Runtime state joins**

* Frames/onnect failures and live values back to code units/instructions and then to source anchors.

-(because they compose)

* **End-to-end name identity:** symtable makes every name map to a single binding slot (or explicit unresolved), and that same binding slot is what dis def/use eve
* **Callsite correctness under decorators:** tree-sitter tells you the syntactic call; inspect tells you the callable’s effective runtime signature and wrapper provenance; symtable helps classify whether the callee name is local/global/free/importe
* **Correctness backstops:** symtable partitions/flags provide “brutal invariants” to detect drift in your AST/DFG layers early (especially around closures and scope spaces).rn this into a **capability gate registry** (for cq) that decides, per question type (“rename safety”, “find callers”, “why this exception”, “update signature”), which minimal

[1]: https://docs.python.org/3/library/symtable.html?utm_source=chatgpt.com "symtable — Access to the compiler's symbol tables"
[2]: https://docs.python.org/3/library/dis.html?utm_source=chatgpt.com "dis — Disassembler for Python bytecode"
[3]: https://docs.python.org/3/library/inspect.html?utm_source=chatgpt.com "Inspect live objects - Python 3.14.1 documentation"

Below are **copy/paste-able** snippets for the **highest-leverage** incremental insights. Each example shows:

* the **tree-sitter basis** (which node(s)/spans you use), and
* the **symtable / dis / inspect** calls you make,
* plus the **join key(s)** that connect those semantics back to the tree-sitter CST.

Tree-sitter node primitives used here (`child_by_field_name`, `parent`, `id`, `named_descendant_for_point_range`, siblings/children) are part of `py-tree-sitter`’s `Node` API. ([Tree-sitter][1])
The `py-tree-sitter` upstream example demonstrates the Python grammar’s field names you’ll rely on (`function_definition name/body`, `call function/arguments`, etc.) and the `Query`/`QueryCursor` pattern. ([GitHub][2])

---

## Shared foundation (tree-sitter parse + node selection + text/spans)

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple, List

from tree_sitter import Language, Parser, Node, Query, QueryCursor
import tree_sitter_python as tspython  # pip install tree-sitter-python

PY_LANGUAGE = Language(tspython.language())
PARSER = Parser(PY_LANGUAGE)

@dataclass(frozen=True)
class Span:
    start_byte: int
    end_byte: int
    start_point: Tuple[int, int]  # (row0, col0)
    end_point: Tuple[int, int]
    node_type: str
    text: str

def ts_parse(src: bytes):
    return PARSER.parse(src)

def ts_text(src: bytes, node: Node) -> str:
    return src[node.start_byte:node.end_byte].decode("utf-8", errors="replace")

def ts_span(src: bytes, node: Node, max_len: int = 140) -> Span:
    s = ts_text(src, node).strip().replace("\n", " ")
    if len(s) > max_len:
        s = s[: max_len - 1] + "…"
    return Span(
        start_byte=node.start_byte,
        end_byte=node.end_byte,
        start_point=tuple(node.start_point),
        end_point=tuple(node.end_point),
        node_type=node.type,
        text=s,
    )

def ts_identifier_at_point(root: Node, line0: int, col0: int) -> Optional[Node]:
    """
    Tree-sitter basis: use named_descendant_for_point_range to get the smallest named node at a cursor. :contentReference[oaicite:2]{index=2}
    """
    pt = (line0, col0)
    n = root.named_descendant_for_point_range(pt, pt)
    if n is None:
        return None
    return n if n.type == "identifier" else None

def ts_enclosing_def(node: Node) -> Node:
    """
    Tree-sitter basis: walk parent chain to find enclosing function/class/module. :contentReference[oaicite:3]{index=3}
    """
    cur = node
    while cur.parent is not None:
        if cur.type in {"function_definition", "class_definition", "module"}:
            return cur
        cur = cur.parent
    return cur

def ts_def_name(def_node: Node) -> Optional[str]:
    """
    Uses grammar fields: function_definition name: (identifier), class_definition name: (identifier). :contentReference[oaicite:4]{index=4}
    """
    name_node = def_node.child_by_field_name("name")
    if name_node is None:
        return None
    return ts_text(SRC, name_node)  # relies on SRC global in examples below

def ts_qualname(node: Node, src: bytes) -> str:
    """
    Tree-sitter basis: build a qualname by collecting enclosing class/function names outward.
    Join key for inspect: module + qualname.
    """
    parts: List[str] = []
    cur = node
    while cur is not None:
        if cur.type in {"function_definition", "class_definition"}:
            name_node = cur.child_by_field_name("name")
            if name_node is not None:
                parts.append(ts_text(src, name_node))
        cur = cur.parent
    return ".".join(reversed(parts))
```

---

## 1) Compiler-grade binding kind for an identifier (tree-sitter → symtable)

**Incremental insight:** “This `x` is **local** vs **free** vs **declared global** vs **nonlocal** vs **parameter** vs **imported**” (compiler truth).
`symtable` is explicitly “responsible for calculating the scope of every identifier” and is generated just before bytecode generation. ([Python documentation][3])

**Join key:** tree-sitter scope node `(scope_type, scope_name, start_line)` ↔ symtable table `(get_type(), get_name(), get_lineno())`. ([Python documentation][3])

```python
import symtable

@dataclass(frozen=True)
class BindingKind:
    symbol: str
    scope_table_id: str
    scope_type: str
    scope_name: str
    flags: dict[str, bool]

def symtable_build(src_str: str, filename: str = "<mem>"):
    # symtable.symtable(code, filename, compile_type) :contentReference[oaicite:7]{index=7}
    return symtable.symtable(src_str, filename, "exec")

def symtable_index_tables(root_table):
    """
    Build parent pointers since symtable tables don’t expose parent directly.
    """
    parents = {}
    stack = [root_table]
    while stack:
        t = stack.pop()
        for ch in t.get_children():  # :contentReference[oaicite:8]{index=8}
            parents[ch] = t
            stack.append(ch)
    return parents

def symtable_find_table_for_ts_scope(root_table, ts_scope: Node, src: bytes):
    """
    Join: match table by (type, name, lineno).
    - get_lineno(): first line of the block :contentReference[oaicite:9]{index=9}
    """
    want_lineno = ts_scope.start_point[0] + 1  # symtable is 1-based lines
    want_type = "module" if ts_scope.type == "module" else ("function" if ts_scope.type == "function_definition" else "class")

    want_name = None
    if ts_scope.type in {"function_definition", "class_definition"}:
        name_node = ts_scope.child_by_field_name("name")
        want_name = ts_text(src, name_node) if name_node else None

    # BFS/DFS over tables
    stack = [root_table]
    while stack:
        t = stack.pop()
        t_type = str(t.get_type().value if hasattr(t.get_type(), "value") else t.get_type())
        if (t_type == want_type) and (t.get_lineno() == want_lineno):  # :contentReference[oaicite:10]{index=10}
            if want_name is None or t.get_name() == want_name:
                return t
        stack.extend(t.get_children())
    return None

def symtable_binding_kind(src: bytes, cursor_line0: int, cursor_col0: int) -> BindingKind:
    # Tree-sitter basis: identifier + enclosing def node.
    tree = ts_parse(src)
    root = tree.root_node
    ident = ts_identifier_at_point(root, cursor_line0, cursor_col0)
    if ident is None:
        raise ValueError("No identifier at cursor")

    scope = ts_enclosing_def(ident)
    symbol_name = ts_text(src, ident)

    st = symtable_build(src.decode("utf-8", errors="replace"))
    table = symtable_find_table_for_ts_scope(st, scope, src)
    if table is None:
        raise RuntimeError("Could not map tree-sitter scope to symtable table")

    sym = table.lookup(symbol_name)  # lookup(name) -> Symbol :contentReference[oaicite:11]{index=11}

    flags = {
        "is_local": sym.is_local(),
        "is_global": sym.is_global(),
        "is_declared_global": sym.is_declared_global(),
        "is_nonlocal": sym.is_nonlocal(),
        "is_free": sym.is_free(),
        "is_parameter": sym.is_parameter(),
        "is_imported": sym.is_imported(),
        "is_annotated": sym.is_annotated(),
        "is_assigned": sym.is_assigned(),
        "is_referenced": sym.is_referenced(),
    }  # Symbol flag APIs :contentReference[oaicite:12]{index=12}

    return BindingKind(
        symbol=symbol_name,
        scope_table_id=str(table.get_id()),
        scope_type=str(table.get_type()),
        scope_name=table.get_name(),
        flags=flags,
    )
```

---

## 2) Closure capture graph (tree-sitter occurrences → symtable get_frees + resolver)

**Incremental insight:** “This inner function **captures** `x` from outer scope; renaming/moving `x` affects closure semantics.”

**Join key:** inner function scope by `(name,start_line)` as above; captured names from `Function.get_frees()` (compiler truth). ([Python documentation][3])

```python
import symtable

@dataclass(frozen=True)
class CaptureEdge:
    inner_table_id: str
    outer_table_id: str
    name: str

def symtable_parent_map(root_table):
    parents = {}
    stack = [root_table]
    while stack:
        t = stack.pop()
        for ch in t.get_children():  # :contentReference[oaicite:14]{index=14}
            parents[ch] = t
            stack.append(ch)
    return parents

def symtable_find_outer_definer(table, parents, name: str):
    """
    Walk outward until a table contains a definition of name (assigned/parameter/import/global decl).
    This is a simplified “definer” heuristic; for a full resolver, store per-table symbol flags.
    """
    cur = parents.get(table)
    while cur is not None:
        if name in cur.get_identifiers():  # identifiers in table :contentReference[oaicite:15]{index=15}
            sym = cur.lookup(name)
            if sym.is_assigned() or sym.is_parameter() or sym.is_imported() or sym.is_declared_global():
                return cur
        cur = parents.get(cur)
    return None

def ts_occurrences_in_node(src: bytes, root: Node, within: Node, name: str) -> list[Span]:
    """
    Tree-sitter basis: query identifiers in a subtree.
    """
    q = Query(PY_LANGUAGE, "((identifier) @id (#eq? @id \"" + name.replace('"', '\\"') + "\"))")
    caps = QueryCursor(q).captures(within)
    nodes = caps.get("id", [])
    return [ts_span(src, n) for n in nodes]

def closure_capture_edges(src: bytes, inner_fn_start_line0: int, inner_fn_start_col0: int) -> tuple[list[CaptureEdge], list[Span]]:
    tree = ts_parse(src)
    root = tree.root_node

    # Tree-sitter basis: pick an identifier inside the inner function, then climb to its function_definition.
    any_ident = root.named_descendant_for_point_range((inner_fn_start_line0, inner_fn_start_col0),
                                                      (inner_fn_start_line0, inner_fn_start_col0))
    if any_ident is None:
        raise ValueError("No node at point")
    inner_fn = any_ident
    while inner_fn is not None and inner_fn.type != "function_definition":
        inner_fn = inner_fn.parent
    if inner_fn is None:
        raise ValueError("Not inside a function_definition")

    inner_name = ts_text(src, inner_fn.child_by_field_name("name"))
    st = symtable.symtable(src.decode("utf-8", errors="replace"), "<mem>", "exec")
    parents = symtable_parent_map(st)

    # Map tree-sitter inner_fn to symtable.Function by name+lineno
    want_lineno = inner_fn.start_point[0] + 1
    inner_table = None
    stack = [st]
    while stack:
        t = stack.pop()
        if str(t.get_type().value if hasattr(t.get_type(), "value") else t.get_type()) == "function" and t.get_lineno() == want_lineno and t.get_name() == inner_name:
            inner_table = t
            break
        stack.extend(t.get_children())
    if inner_table is None:
        raise RuntimeError("Could not find symtable for inner function")

    frees = list(inner_table.get_frees())  # compiler free vars :contentReference[oaicite:16]{index=16}

    edges: list[CaptureEdge] = []
    occ_spans: list[Span] = []
    for name in frees:
        outer = symtable_find_outer_definer(inner_table, parents, name)
        if outer is not None:
            edges.append(CaptureEdge(str(inner_table.get_id()), str(outer.get_id()), name))
        # Tree-sitter basis: show *where* captured name appears inside the inner fn subtree
        occ_spans.extend(ts_occurrences_in_node(src, root, inner_fn, name))

    return edges, occ_spans
```

---

## 3) Bytecode CFG + exception edges, anchored back to CST (tree-sitter → dis)

**Incremental insight:** “Actual control flow + exception paths (try/except/finally/with)”, which syntax neighborhoods cannot make fully faithful.

`dis.get_instructions()` yields `Instruction` objects with jump targets and **source positions** (lineno/col ranges) via `Instruction.positions` / `dis.Positions`. ([Python documentation][4])

**Join keys:**

* tree-sitter function node `(name, start_line)` ↔ code object `(co_name, co_firstlineno)`
* instruction source span (from `Instruction.positions`) ↔ tree-sitter node via `named_descendant_for_point_range`

```python
import dis
import types
from dataclasses import dataclass
from typing import Dict, Set

@dataclass(frozen=True)
class CFGEdge:
    src_off: int
    dst_off: int
    kind: str  # "jump" | "fallthrough" | "exc"

def walk_code_objects(co: types.CodeType):
    yield co
    for c in co.co_consts:
        if isinstance(c, types.CodeType):
            yield from walk_code_objects(c)

def find_function_codeobj(module_co: types.CodeType, fn_name: str, fn_firstlineno: int) -> types.CodeType:
    for co in walk_code_objects(module_co):
        if co.co_name == fn_name and co.co_firstlineno == fn_firstlineno:
            return co
    raise KeyError(f"Code object not found: {fn_name}@{fn_firstlineno}")

def ts_node_for_instruction(root: Node, instr: dis.Instruction) -> Optional[Node]:
    """
    Join dis.Instruction.positions -> tree-sitter node at that span. :contentReference[oaicite:18]{index=18}
    """
    pos = instr.positions
    if pos is None or pos.lineno is None or pos.col_offset is None:
        return None
    start = (pos.lineno - 1, pos.col_offset)
    end = (pos.end_lineno - 1, pos.end_col_offset) if (pos.end_lineno and pos.end_col_offset is not None) else start
    return root.named_descendant_for_point_range(start, end)

def cfg_edges_for_function(src: bytes, fn_line0: int, fn_col0: int) -> tuple[types.CodeType, list[CFGEdge], dict[int, Span]]:
    tree = ts_parse(src)
    root = tree.root_node

    # Tree-sitter basis: locate the function_definition node and read its name/body spans.
    n = root.named_descendant_for_point_range((fn_line0, fn_col0), (fn_line0, fn_col0))
    while n is not None and n.type != "function_definition":
        n = n.parent
    if n is None:
        raise ValueError("Not inside a function_definition")

    fn_name = ts_text(src, n.child_by_field_name("name"))
    fn_firstlineno = n.start_point[0] + 1

    module_co = compile(src.decode("utf-8", errors="replace"), "<mem>", "exec")
    fn_co = find_function_codeobj(module_co, fn_name, fn_firstlineno)

    ins = list(dis.get_instructions(fn_co))  # :contentReference[oaicite:19]{index=19}

    # Map instruction offset -> “best” CST node span for attribution
    off2span: dict[int, Span] = {}
    for i in ins:
        tn = ts_node_for_instruction(root, i)
        if tn is not None:
            off2span[i.offset] = ts_span(src, tn)

    # Basic CFG: jumps + fallthrough
    edges: list[CFGEdge] = []
    term_ops = {"RETURN_VALUE", "RAISE_VARARGS", "RERAISE"}
    jump_ops_prefix = ("JUMP_", "POP_JUMP_", "FOR_ITER", "SEND")  # pragmatic

    offsets = [i.offset for i in ins]
    offs_set = set(offsets)

    for idx, i in enumerate(ins):
        # jump_target is exposed on Instruction in docs :contentReference[oaicite:20]{index=20}
        jt = i.jump_target
        if jt is not None and jt in offs_set:
            edges.append(CFGEdge(i.offset, jt, "jump"))
            # conditional jumps also fall through
            if i.opname.startswith("POP_JUMP_") or i.opname.startswith("FOR_ITER") or i.opname.startswith("SEND"):
                if idx + 1 < len(ins):
                    edges.append(CFGEdge(i.offset, ins[idx + 1].offset, "fallthrough"))
        else:
            if i.opname not in term_ops and idx + 1 < len(ins):
                edges.append(CFGEdge(i.offset, ins[idx + 1].offset, "fallthrough"))

    # Exception edges (CPython-only detail):
    # Code objects carry co_exceptiontable; CPython’s codeobject.c exposes this field. :contentReference[oaicite:21]{index=21}
    # dis has internal exception-table parsing helpers in CPython (not in public docs).
    parse_exc = getattr(dis, "_parse_exception_table", None)
    if parse_exc:
        for e in parse_exc(fn_co):
            # entry has (start, end, target, depth, lasti) in CPython dis internals
            start, end, target = e.start, e.end, e.target
            # naive: connect any instruction within [start,end) to handler target
            for i in ins:
                if start <= i.offset < end:
                    edges.append(CFGEdge(i.offset, target, "exc"))

    return fn_co, edges, off2span
```

---

## 4) Slot-aware def/use events + reaching-defs DFG (tree-sitter spans + dis + symtable)

**Incremental insight:** “What definitions reach this use?” / “Is this rename/move safe?” using instruction-level `LOAD_*` / `STORE_*` events in **FAST / GLOBAL / DEREF** spaces.

* `dis.Instruction.argval` provides the resolved argument (often the variable name) and `Instruction.positions` gives the source span. ([Python documentation][4])
* `symtable` gives compiler-truth classification for names (`is_local/is_free/is_global/is_declared_global/...`). ([Python documentation][3])

**Join key:** variable name + symtable flags → canonical binding slot; instruction span → CST anchor via positions.

```python
import dis
import symtable
from dataclasses import dataclass
from typing import Literal, Iterable

SlotKind = Literal["FAST", "DEREF", "GLOBAL", "NAME", "ATTR", "SUBSCR"]

@dataclass(frozen=True)
class DefUseEvent:
    offset: int
    kind: Literal["DEF", "USE"]
    slot_kind: SlotKind
    name: str
    span: Optional[Span]  # attributed CST node span (best effort)

def classify_slot(sym: symtable.Symbol) -> SlotKind:
    # compiler-truth symbol flags :contentReference[oaicite:24]{index=24}
    if sym.is_free() or sym.is_nonlocal():
        return "DEREF"
    if sym.is_declared_global() or sym.is_global():
        return "GLOBAL"
    if sym.is_local() or sym.is_parameter():
        return "FAST"
    return "NAME"

def build_events_for_function(src: bytes, fn_line0: int, fn_col0: int) -> list[DefUseEvent]:
    tree = ts_parse(src)
    root = tree.root_node

    # Tree-sitter basis: locate function + name + scope mapping
    n = root.named_descendant_for_point_range((fn_line0, fn_col0), (fn_line0, fn_col0))
    while n is not None and n.type != "function_definition":
        n = n.parent
    if n is None:
        raise ValueError("Not inside function_definition")

    fn_name = ts_text(src, n.child_by_field_name("name"))
    fn_firstlineno = n.start_point[0] + 1

    module_co = compile(src.decode("utf-8", errors="replace"), "<mem>", "exec")
    fn_co = find_function_codeobj(module_co, fn_name, fn_firstlineno)

    # symtable table for this function (join by lineno+name)
    st = symtable.symtable(src.decode("utf-8", errors="replace"), "<mem>", "exec")
    fn_table = symtable_find_table_for_ts_scope(st, n, src)

    events: list[DefUseEvent] = []
    for ins in dis.get_instructions(fn_co):  # :contentReference[oaicite:25]{index=25}
        name = ins.argval if isinstance(ins.argval, str) else None
        if name is None:
            continue

        # Attribute spans from bytecode positions
        tn = ts_node_for_instruction(root, ins)
        sp = ts_span(src, tn) if tn is not None else None

        op = ins.baseopname  # baseopname in docs :contentReference[oaicite:26]{index=26}

        if op.startswith("LOAD_"):
            if op in {"LOAD_ATTR"}:
                events.append(DefUseEvent(ins.offset, "USE", "ATTR", name, sp))
            elif op in {"LOAD_SUBSCR"}:
                events.append(DefUseEvent(ins.offset, "USE", "SUBSCR", name, sp))
            else:
                sym = fn_table.lookup(name) if fn_table is not None else None
                slot = classify_slot(sym) if sym is not None else "NAME"
                events.append(DefUseEvent(ins.offset, "USE", slot, name, sp))

        elif op.startswith("STORE_") or op.startswith("DELETE_"):
            if op in {"STORE_ATTR", "DELETE_ATTR"}:
                events.append(DefUseEvent(ins.offset, "DEF", "ATTR", name, sp))
            elif op in {"STORE_SUBSCR"}:
                events.append(DefUseEvent(ins.offset, "DEF", "SUBSCR", name, sp))
            else:
                sym = fn_table.lookup(name) if fn_table is not None else None
                slot = classify_slot(sym) if sym is not None else "NAME"
                events.append(DefUseEvent(ins.offset, "DEF", slot, name, sp))

    return events

# (Optional) reaching-defs skeleton: uses CFG edges from example 3.
def reaching_defs(events: list[DefUseEvent], edges: list[CFGEdge]):
    """
    Dataflow core (sketch):
      - group events by basic block
      - compute GEN/KILL per block per (slot_kind,name)
      - iterate IN/OUT until fixpoint
    This becomes REACHES(def_event -> use_event).
    """
    ...
```

---

## 5) Decorator-aware callable surface: unwrap chain + effective signature (tree-sitter callsite → inspect)

**Incremental insight:** syntax sees `foo(...)`, but the runtime callable might be `@decorated` with a different “effective” signature.
`inspect.signature(..., follow_wrapped=...)` and `inspect.unwrap` exist specifically for wrapper chains; `unwrap` follows `__wrapped__` and `signature()` uses a stop condition when `__signature__` exists. ([Python documentation][5])

**Tree-sitter basis:** `call` node, `function:` and `arguments:` fields for the callee/args. ([GitHub][2])
**Join key:** module import path + callee qualname/attribute chain.

```python
import importlib
import inspect
from dataclasses import dataclass

@dataclass(frozen=True)
class CallsiteSignatureInsight:
    callee_text: str
    call_arg_count: int
    signature_follow_wrapped: str
    signature_no_unwrap: str
    unwrapped_qualname: str

def ts_call_at_point(root: Node, line0: int, col0: int) -> Optional[Node]:
    n = root.named_descendant_for_point_range((line0, col0), (line0, col0))
    while n is not None and n.type != "call":
        n = n.parent
    return n

def count_call_args(call_node: Node) -> int:
    args = call_node.child_by_field_name("arguments")  # call arguments: (argument_list) :contentReference[oaicite:29]{index=29}
    if args is None:
        return 0
    # conservative: count named children (skips commas/parentheses)
    return len(args.named_children)

def resolve_attr_chain(module_obj, dotted: str):
    cur = module_obj
    for part in dotted.split("."):
        cur = getattr(cur, part)
    return cur

def inspect_callsite_signature(src: bytes, module_name: str, line0: int, col0: int) -> CallsiteSignatureInsight:
    tree = ts_parse(src)
    root = tree.root_node

    call = ts_call_at_point(root, line0, col0)
    if call is None:
        raise ValueError("No call at point")

    callee_node = call.child_by_field_name("function")  # call function: (identifier) :contentReference[oaicite:30]{index=30}
    if callee_node is None:
        raise RuntimeError("Call has no function field")

    callee_text = ts_text(src, callee_node).strip()
    argc = count_call_args(call)

    # Runtime reflection (may execute module import side effects!)
    mod = importlib.import_module(module_name)
    fn_obj = resolve_attr_chain(mod, callee_text)

    sig_wrapped = str(inspect.signature(fn_obj, follow_wrapped=True))   # :contentReference[oaicite:31]{index=31}
    sig_raw = str(inspect.signature(fn_obj, follow_wrapped=False))      # follow_wrapped exists :contentReference[oaicite:32]{index=32}
    unwrapped = inspect.unwrap(fn_obj)                                   # :contentReference[oaicite:33]{index=33}

    return CallsiteSignatureInsight(
        callee_text=callee_text,
        call_arg_count=argc,
        signature_follow_wrapped=sig_wrapped,
        signature_no_unwrap=sig_raw,
        unwrapped_qualname=getattr(unwrapped, "__qualname__", repr(unwrapped)),
    )
```

---

## 6) Static-safe class/module member inventory + descriptor kinds (tree-sitter class → inspect.getmembers_static)

**Incremental insight:** “What attributes exist *at runtime* on this class/module, and which are **data descriptors** vs method descriptors vs properties vs slots?” — without triggering descriptor execution.

* `inspect.getmembers_static` enumerates members **without triggering** descriptor protocol / `__getattr__` / `__getattribute__`. ([Python documentation][5])
* Descriptor classifiers like `inspect.isdatadescriptor`, `ismethoddescriptor`, `isgetsetdescriptor`, `ismemberdescriptor` are documented. ([Python documentation][5])
* For single-attribute lookups, `inspect.getattr_static` is the static-safe primitive. ([Python documentation][5])

**Join key:** module import name + class name/qualname derived from tree-sitter.

```python
import importlib
import inspect
from dataclasses import dataclass

@dataclass(frozen=True)
class MemberFact:
    name: str
    kind: str  # "data_descriptor" | "method_descriptor" | "function" | "property" | ...
    value_type: str

def descriptor_kind(v) -> str:
    if isinstance(v, property):
        return "property"
    if inspect.isdatadescriptor(v):     # :contentReference[oaicite:37]{index=37}
        # can refine with isgetsetdescriptor/ismemberdescriptor if desired :contentReference[oaicite:38]{index=38}
        if inspect.isgetsetdescriptor(v):
            return "getset_descriptor"
        if inspect.ismemberdescriptor(v):
            return "member_descriptor"
        return "data_descriptor"
    if inspect.ismethoddescriptor(v):   # :contentReference[oaicite:39]{index=39}
        return "method_descriptor"
    if inspect.isfunction(v):
        return "function"
    if inspect.ismethod(v):
        return "bound_method"
    return "value"

def inspect_class_members_static(src: bytes, module_name: str, class_line0: int, class_col0: int) -> list[MemberFact]:
    tree = ts_parse(src)
    root = tree.root_node

    n = root.named_descendant_for_point_range((class_line0, class_col0), (class_line0, class_col0))
    while n is not None and n.type != "class_definition":
        n = n.parent
    if n is None:
        raise ValueError("Not inside class_definition")

    class_name = ts_text(src, n.child_by_field_name("name"))
    mod = importlib.import_module(module_name)
    cls = getattr(mod, class_name)

    out: list[MemberFact] = []
    for name, value in inspect.getmembers_static(cls):  # :contentReference[oaicite:40]{index=40}
        out.append(MemberFact(name=name, kind=descriptor_kind(value), value_type=type(value).__name__))

    return out

def inspect_static_attr(src: bytes, module_name: str, qual: str, attr: str):
    """
    For pinpoint attribution: getattr_static does not invoke descriptors. :contentReference[oaicite:41]{index=41}
    """
    mod = importlib.import_module(module_name)
    obj = resolve_attr_chain(mod, qual)
    return inspect.getattr_static(obj, attr)
```

---

## 7) Safe annotation extraction (tree-sitter annotation syntax ↔ inspect.get_annotations)

**Incremental insight:** “What annotations does Python believe exist for this object (post-decorator, post-future-annotations), safely?”

`inspect.get_annotations(..., eval_str=False)` exists and is documented; it warns about potential code execution depending on evaluation mode and points to security implications. ([Python documentation][5])

**Tree-sitter basis:** pick the `function_definition` / `class_definition` node and use spans to show where annotations are located syntactically; use inspect output as the “semantic” dict.

```python
import importlib
import inspect
from dataclasses import dataclass

@dataclass(frozen=True)
class AnnotationInsight:
    qualname: str
    annotations: dict
    ts_def_span: Span

def inspect_annotations_for_def(src: bytes, module_name: str, def_line0: int, def_col0: int) -> AnnotationInsight:
    tree = ts_parse(src)
    root = tree.root_node

    n = root.named_descendant_for_point_range((def_line0, def_col0), (def_line0, def_col0))
    while n is not None and n.type not in {"function_definition", "class_definition"}:
        n = n.parent
    if n is None:
        raise ValueError("Not inside function_definition/class_definition")

    qual = ts_qualname(n, src)  # tree-sitter-derived qualname
    mod = importlib.import_module(module_name)
    obj = resolve_attr_chain(mod, qual)

    ann = inspect.get_annotations(obj, eval_str=False)  # :contentReference[oaicite:43]{index=43}
    return AnnotationInsight(qualname=qual, annotations=ann, ts_def_span=ts_span(src, n))
```

---

### Why these joins work (and where the power comes from)

* **tree-sitter → symtable:** symtable is the compiler’s scope oracle (`symtable.symtable`, `SymbolTable.get_lineno/get_children/lookup`, `Symbol.is_local/is_free/...`). ([Python documentation][3])
* **tree-sitter → dis:** `dis.get_instructions` gives jump structure + `Instruction.positions` spans, which let you map bytecode facts back onto CST nodes via point-range lookup. ([Python documentation][4])
* **tree-sitter → inspect:** `inspect.signature(follow_wrapped=...)`, `inspect.unwrap`, `inspect.getmembers_static`, `inspect.getattr_static`, `inspect.get_annotations` provide runtime truth while giving you levers to avoid executing descriptors/wrappers inadvertently. ([Python documentation][5])

If you want, I can collapse these into a **single CQ “enrichment provider”** interface that takes `(file_path, cursor_point, mode)` and returns a structured bundle:

* `ts_anchor` (span + parents + peers),
* `sym_binding` (flags + capture edges),
* `dis_cfg/dfg` (edges + instruction→span attribution),
* `inspect_surface` (signature variants + wrapper chain + member facts),
  with materialization knobs for token budget.

[1]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Node.html "Node — py-tree-sitter 0.25.2 documentation"
[2]: https://raw.githubusercontent.com/tree-sitter/py-tree-sitter/master/examples/usage.py "raw.githubusercontent.com"
[3]: https://docs.python.org/3/library/symtable.html "symtable — Access to the compiler’s symbol tables — Python 3.14.3 documentation"
[4]: https://docs.python.org/3/library/dis.html "dis — Disassembler for Python bytecode — Python 3.14.3 documentation"
[5]: https://docs.python.org/3/library/inspect.html "inspect — Inspect live objects — Python 3.14.3 documentation"

Below is a single **CQ “enrichment provider”** module that takes:

`(file_path, cursor_point, mode)` → returns a structured `EnrichmentBundle` with:

* `ts_anchor` (span + parents + peers)
* `sym_binding` (flags + capture edges)
* `dis_cfg_dfg` (CFG edges + def/use + reaching-defs + instruction→CST attribution via `Instruction.positions`)
* `inspect_surface` (signature variants + unwrap chain + member facts)

It’s built directly on the documented primitives:

* Tree-sitter `Node` traversal (`parent`, `named_children`, siblings, `child_by_field_name`, point-range selection) and `Node.id` uniqueness/reuse semantics, plus `Node.text` caveat (only valid when tree not edited). ([tree-sitter.github.io][1])
* Incremental parse hook: `Parser.parse(..., old_tree=...)`, and invalidation surfaces `Tree.edit(...)` + `Tree.changed_ranges(new_tree)` (not strictly required for correctness here, but wired as optional cache hooks). ([tree-sitter.github.io][2])
* Query predicates like `#eq?` for targeted identifier capture. ([tree-sitter.github.io][3])
* `symtable.Symbol` flags (`is_local/is_free/is_parameter/is_declared_global/...`) for compiler-truth binding. ([Python documentation][4])
* `dis.get_instructions()` and `Instruction.positions` (`dis.Positions`) to attribute bytecode to source spans and build CFG/DFG. ([Python documentation][5])
* `inspect.signature(follow_wrapped=...)`, `getmembers_static`, `getattr_static`, `get_annotations(eval_str=False)` for runtime surfaces with explicit safety knobs. ([Python documentation][6])

---

## `cq_enrichment_provider.py`

````python
from __future__ import annotations

import dataclasses
import enum
import hashlib
import importlib
import inspect
import os
import symtable
import dis
import types
from typing import Dict, Iterable, List, Optional, Set, Tuple

from tree_sitter import Language, Parser, Node, Tree, Query, QueryCursor  # pip install tree-sitter
import tree_sitter_python as tspython  # pip install tree-sitter-python


# ============================================================
# Public API (what CQ calls)
# ============================================================

@dataclasses.dataclass(frozen=True)
class Point:
    row: int  # 0-based
    col: int  # 0-based


class EnrichmentMode(str, enum.Enum):
    TS_ONLY = "ts_only"
    TS_SYM = "ts_sym"
    TS_SYM_DIS = "ts_sym_dis"
    FULL = "full"  # includes inspect


@dataclasses.dataclass(frozen=True)
class Budget:
    # shared
    snippet_max_len: int = 120

    # tree-sitter materialization
    ts_parent_limit: int = 6
    ts_peer_limit: int = 6

    # symtable materialization
    sym_include_capture_edges: bool = True

    # dis materialization
    dis_max_instructions: int = 4000  # hard cap for huge functions
    dis_max_edges: int = 12000
    dis_include_exception_edges_if_available: bool = False  # uses private CPython helper if present

    # reaching-defs (DFG)
    dfg_enable: bool = True
    dfg_max_edges: int = 8000

    # inspect materialization
    inspect_enable: bool = False               # default off (imports can have side effects)
    inspect_member_limit: int = 80
    inspect_unwrap_limit: int = 12
    inspect_annotation_item_limit: int = 80
    inspect_follow_wrapped: bool = True        # signature(follow_wrapped=...)
    inspect_allow_import: bool = False         # must be true to import module
    inspect_module_name: Optional[str] = None  # if you want inspect to resolve dotted names


# ------------------------------------------------------------
# Output schema (structured bundle)
# ------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class Span:
    start_byte: int
    end_byte: int
    start_point: Point
    end_point: Point
    node_type: str
    text: str


@dataclasses.dataclass(frozen=True)
class TsAnchor:
    file_path: str
    file_hash: str
    cursor: Point
    node_id: Optional[int]
    anchor: Span
    parents: List[Span]
    peers_prev: List[Span]
    peers_next: List[Span]


@dataclasses.dataclass(frozen=True)
class SymBinding:
    scope_type: str
    scope_name: str
    scope_lineno: int
    symbol: str
    flags: Dict[str, bool]
    capture_edges: List[Tuple[str, str, str]]  # (inner_table_id, outer_table_id, name)


@dataclasses.dataclass(frozen=True)
class CFGEdge:
    src_off: int
    dst_off: int
    kind: str  # "jump" | "fallthrough" | "exc"


@dataclasses.dataclass(frozen=True)
class DefUseEvent:
    event_id: int
    offset: int
    kind: str      # "DEF" | "USE"
    slot_kind: str # "FAST" | "DEREF" | "GLOBAL" | "NAME"
    name: str
    span: Optional[Span]


@dataclasses.dataclass(frozen=True)
class DFGEdge:
    def_event_id: int
    use_event_id: int
    slot_kind: str
    name: str


@dataclasses.dataclass(frozen=True)
class DisCFGDFG:
    code_name: str
    firstlineno: int
    instruction_count: int
    edges: List[CFGEdge]
    events: List[DefUseEvent]
    dfg_edges: List[DFGEdge]


@dataclasses.dataclass(frozen=True)
class InspectMember:
    name: str
    kind: str
    value_type: str


@dataclasses.dataclass(frozen=True)
class InspectSurface:
    module_name: str
    dotted_expr: str
    resolved_qualname: str
    unwrap_chain: List[str]
    signature_follow_wrapped: Optional[str]
    signature_no_unwrap: Optional[str]
    annotations_keys: List[str]
    members: List[InspectMember]
    error: Optional[str] = None


@dataclasses.dataclass(frozen=True)
class EnrichmentBundle:
    ts_anchor: TsAnchor
    sym_binding: Optional[SymBinding]
    dis_cfg_dfg: Optional[DisCFGDFG]
    inspect_surface: Optional[InspectSurface]


# ============================================================
# Provider
# ============================================================

class CQEnrichmentProvider:
    def __init__(self) -> None:
        self._language = Language(tspython.language())
        self._parser = Parser(self._language)

        # Cheap caches keyed by (file_path -> (hash, artifact))
        self._ts_tree_cache: Dict[str, Tuple[str, Tree]] = {}
        self._sym_cache: Dict[str, Tuple[str, symtable.SymbolTable]] = {}
        self._code_cache: Dict[str, Tuple[str, types.CodeType]] = {}

    def enrich(self, file_path: str, cursor: Point, mode: EnrichmentMode, budget: Optional[Budget] = None) -> EnrichmentBundle:
        budget = budget or Budget(inspect_enable=(mode == EnrichmentMode.FULL))
        src = self._read_bytes(file_path)
        file_hash = _sha256(src)

        tree = self._get_tree(file_path, src, file_hash)
        root = tree.root_node

        anchor_node = _ts_anchor_node(root, cursor)
        ts_anchor = _build_ts_anchor(file_path, file_hash, cursor, src, anchor_node, budget)

        sym_binding: Optional[SymBinding] = None
        dis_cfg_dfg: Optional[DisCFGDFG] = None
        inspect_surface: Optional[InspectSurface] = None

        # ---- symtable plane
        if mode in {EnrichmentMode.TS_SYM, EnrichmentMode.TS_SYM_DIS, EnrichmentMode.FULL}:
            st = self._get_symtable(file_path, src, file_hash)
            sym_binding = _build_sym_binding(src, st, anchor_node, budget)

        # ---- dis plane
        if mode in {EnrichmentMode.TS_SYM_DIS, EnrichmentMode.FULL}:
            module_co = self._get_compiled_module(file_path, src, file_hash)
            dis_cfg_dfg = _build_dis_cfg_dfg(src, root, module_co, anchor_node, sym_binding, budget)

        # ---- inspect plane (runtime; opt-in)
        if mode == EnrichmentMode.FULL and budget.inspect_enable:
            inspect_surface = _build_inspect_surface(src, root, anchor_node, budget)

        return EnrichmentBundle(
            ts_anchor=ts_anchor,
            sym_binding=sym_binding,
            dis_cfg_dfg=dis_cfg_dfg,
            inspect_surface=inspect_surface,
        )

    # -------------------------
    # cache helpers
    # -------------------------

    def _get_tree(self, file_path: str, src: bytes, file_hash: str) -> Tree:
        hit = self._ts_tree_cache.get(file_path)
        if hit is not None and hit[0] == file_hash:
            return hit[1]
        tree = self._parser.parse(src)  # Parser.parse(source, old_tree=None, ...) exists for incremental usage
        self._ts_tree_cache[file_path] = (file_hash, tree)
        return tree

    def _get_symtable(self, file_path: str, src: bytes, file_hash: str) -> symtable.SymbolTable:
        hit = self._sym_cache.get(file_path)
        if hit is not None and hit[0] == file_hash:
            return hit[1]
        st = symtable.symtable(src.decode("utf-8", errors="replace"), file_path, "exec")
        self._sym_cache[file_path] = (file_hash, st)
        return st

    def _get_compiled_module(self, file_path: str, src: bytes, file_hash: str) -> types.CodeType:
        hit = self._code_cache.get(file_path)
        if hit is not None and hit[0] == file_hash:
            return hit[1]
        co = compile(src.decode("utf-8", errors="replace"), file_path, "exec")
        self._code_cache[file_path] = (file_hash, co)
        return co

    @staticmethod
    def _read_bytes(file_path: str) -> bytes:
        with open(file_path, "rb") as f:
            return f.read()


# ============================================================
# Tree-sitter plane
# ============================================================

def _sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _slice(src: bytes, start: int, end: int, max_len: int) -> str:
    s = src[start:end].decode("utf-8", errors="replace")
    s = " ".join(s.split())
    return s if len(s) <= max_len else s[: max_len - 1] + "…"


def _span(src: bytes, n: Node, max_len: int) -> Span:
    return Span(
        start_byte=n.start_byte,
        end_byte=n.end_byte,
        start_point=Point(*n.start_point),
        end_point=Point(*n.end_point),
        node_type=n.type,
        text=_slice(src, n.start_byte, n.end_byte, max_len),
    )


def _ts_anchor_node(root: Node, cursor: Point) -> Node:
    pt = (cursor.row, cursor.col)
    n = root.named_descendant_for_point_range(pt, pt)
    return n if n is not None else root


def _build_ts_anchor(file_path: str, file_hash: str, cursor: Point, src: bytes, anchor: Node, budget: Budget) -> TsAnchor:
    # parents
    parents: List[Span] = []
    cur = anchor.parent
    while cur is not None and len(parents) < budget.ts_parent_limit:
        parents.append(_span(src, cur, budget.snippet_max_len))
        cur = cur.parent

    # peers (named siblings)
    peers_prev: List[Span] = []
    cur = anchor
    for _ in range(budget.ts_peer_limit):
        cur = cur.prev_named_sibling
        if cur is None:
            break
        peers_prev.append(_span(src, cur, budget.snippet_max_len))

    peers_next: List[Span] = []
    cur = anchor
    for _ in range(budget.ts_peer_limit):
        cur = cur.next_named_sibling
        if cur is None:
            break
        peers_next.append(_span(src, cur, budget.snippet_max_len))

    return TsAnchor(
        file_path=file_path,
        file_hash=file_hash,
        cursor=cursor,
        node_id=getattr(anchor, "id", None),
        anchor=_span(src, anchor, budget.snippet_max_len),
        parents=parents,
        peers_prev=peers_prev,
        peers_next=peers_next,
    )


# ============================================================
# Symtable plane (binding truth + capture edges)
# ============================================================

def _ts_enclosing_scope_node(n: Node) -> Node:
    cur = n
    while cur.parent is not None:
        if cur.type in {"function_definition", "class_definition", "module"}:
            return cur
        cur = cur.parent
    return cur


def _ts_scope_key(src: bytes, scope: Node) -> Tuple[str, Optional[str], int]:
    # join key: (scope_type, scope_name, lineno)
    lineno = scope.start_point[0] + 1
    if scope.type == "module":
        return ("module", None, lineno)
    name_node = scope.child_by_field_name("name")
    name = _slice(src, name_node.start_byte, name_node.end_byte, 10_000) if name_node is not None else None
    return ("function" if scope.type == "function_definition" else "class", name, lineno)


def _symtable_parent_map(root: symtable.SymbolTable) -> Dict[symtable.SymbolTable, symtable.SymbolTable]:
    parents: Dict[symtable.SymbolTable, symtable.SymbolTable] = {}
    stack = [root]
    while stack:
        t = stack.pop()
        for ch in t.get_children():
            parents[ch] = t
            stack.append(ch)
    return parents


def _symtable_find_table(root: symtable.SymbolTable, want_type: str, want_name: Optional[str], want_lineno: int) -> Optional[symtable.SymbolTable]:
    stack = [root]
    while stack:
        t = stack.pop()
        t_type = t.get_type()
        # In CPython, table.get_type() returns an enum-ish value; str() is stable enough for matching.
        if str(t_type) == want_type and t.get_lineno() == want_lineno:
            if want_name is None or t.get_name() == want_name:
                return t
        stack.extend(t.get_children())
    return None


def _sym_flags(sym: symtable.Symbol) -> Dict[str, bool]:
    return {
        "is_local": sym.is_local(),
        "is_global": sym.is_global(),
        "is_declared_global": sym.is_declared_global(),
        "is_nonlocal": sym.is_nonlocal(),
        "is_free": sym.is_free(),
        "is_parameter": sym.is_parameter(),
        "is_imported": sym.is_imported(),
        "is_annotated": sym.is_annotated(),
        "is_assigned": sym.is_assigned(),
        "is_referenced": sym.is_referenced(),
    }


def _symtable_find_outer_definer(table: symtable.SymbolTable, parents: Dict[symtable.SymbolTable, symtable.SymbolTable], name: str) -> Optional[symtable.SymbolTable]:
    cur = parents.get(table)
    while cur is not None:
        if name in cur.get_identifiers():
            sym = cur.lookup(name)
            if sym.is_assigned() or sym.is_parameter() or sym.is_imported() or sym.is_declared_global():
                return cur
        cur = parents.get(cur)
    return None


def _build_sym_binding(src: bytes, st: symtable.SymbolTable, anchor: Node, budget: Budget) -> SymBinding:
    scope = _ts_enclosing_scope_node(anchor)
    want_type, want_name, want_lineno = _ts_scope_key(src, scope)

    table = _symtable_find_table(st, want_type, want_name, want_lineno) or st
    name = _slice(src, anchor.start_byte, anchor.end_byte, 10_000)

    sym = table.lookup(name)
    flags = _sym_flags(sym)

    capture_edges: List[Tuple[str, str, str]] = []
    if budget.sym_include_capture_edges and str(table.get_type()) == "function":
        parents = _symtable_parent_map(st)
        # If this function has frees, connect them to the nearest outer definer.
        for free_name in getattr(table, "get_frees", lambda: [])():
            outer = _symtable_find_outer_definer(table, parents, free_name)
            if outer is not None:
                capture_edges.append((str(table.get_id()), str(outer.get_id()), free_name))

    return SymBinding(
        scope_type=str(table.get_type()),
        scope_name=table.get_name(),
        scope_lineno=table.get_lineno(),
        symbol=name,
        flags=flags,
        capture_edges=capture_edges,
    )


# ============================================================
# dis plane (CFG + slot-level DFG)
# ============================================================

def _walk_code_objects(co: types.CodeType) -> Iterable[types.CodeType]:
    yield co
    for c in co.co_consts:
        if isinstance(c, types.CodeType):
            yield from _walk_code_objects(c)


def _find_enclosing_function_node(anchor: Node) -> Optional[Node]:
    cur = anchor
    while cur is not None:
        if cur.type == "function_definition":
            return cur
        cur = cur.parent
    return None


def _find_codeobj_for_ts_function(src: bytes, module_co: types.CodeType, fn_node: Node) -> types.CodeType:
    name_node = fn_node.child_by_field_name("name")
    fn_name = _slice(src, name_node.start_byte, name_node.end_byte, 10_000) if name_node is not None else "<lambda>"
    fn_firstlineno = fn_node.start_point[0] + 1
    for co in _walk_code_objects(module_co):
        if co.co_name == fn_name and co.co_firstlineno == fn_firstlineno:
            return co
    # fallback: best match by lineno only
    for co in _walk_code_objects(module_co):
        if co.co_firstlineno == fn_firstlineno:
            return co
    raise KeyError(f"Could not locate code object for {fn_name}@{fn_firstlineno}")


def _ts_node_for_dis_positions(root: Node, pos: dis.Positions) -> Optional[Node]:
    if pos is None or pos.lineno is None or pos.col_offset is None:
        return None
    start = (pos.lineno - 1, pos.col_offset)
    if pos.end_lineno is not None and pos.end_col_offset is not None:
        end = (pos.end_lineno - 1, pos.end_col_offset)
    else:
        end = start
    return root.named_descendant_for_point_range(start, end)


def _cfg_from_instructions(ins: List[dis.Instruction], max_edges: int) -> List[CFGEdge]:
    offs = [i.offset for i in ins]
    offs_set = set(offs)
    term_ops = {"RETURN_VALUE", "RERAISE", "RAISE_VARARGS"}
    edges: List[CFGEdge] = []
    for idx, i in enumerate(ins):
        jt = i.jump_target
        if jt is not None and jt in offs_set:
            edges.append(CFGEdge(i.offset, jt, "jump"))
            # conservative: conditional-ish ops fallthrough
            if i.opname.startswith("POP_JUMP") or i.opname in {"FOR_ITER", "SEND"}:
                if idx + 1 < len(ins):
                    edges.append(CFGEdge(i.offset, ins[idx + 1].offset, "fallthrough"))
        else:
            if i.opname not in term_ops and idx + 1 < len(ins):
                edges.append(CFGEdge(i.offset, ins[idx + 1].offset, "fallthrough"))
        if len(edges) >= max_edges:
            break
    return edges


def _slot_kind(name: str, sym: Optional[SymBinding]) -> str:
    # Use symtable-derived flags if available; otherwise "NAME".
    if sym is None or sym.symbol != name:
        return "NAME"
    if sym.flags.get("is_free") or sym.flags.get("is_nonlocal"):
        return "DEREF"
    if sym.flags.get("is_declared_global") or sym.flags.get("is_global"):
        return "GLOBAL"
    if sym.flags.get("is_local") or sym.flags.get("is_parameter"):
        return "FAST"
    return "NAME"


def _defuse_events(src: bytes, root: Node, ins: List[dis.Instruction], sym: Optional[SymBinding], budget: Budget) -> List[DefUseEvent]:
    out: List[DefUseEvent] = []
    eid = 0
    for i in ins:
        if not isinstance(i.argval, str):
            continue
        var = i.argval
        base = i.baseopname  # Instruction.baseopname
        if base.startswith("LOAD_"):
            kind = "USE"
        elif base.startswith("STORE_") or base.startswith("DELETE_"):
            kind = "DEF"
        else:
            continue

        tn = _ts_node_for_dis_positions(root, i.positions)
        sp = _span(src, tn, budget.snippet_max_len) if tn is not None else None

        out.append(
            DefUseEvent(
                event_id=eid,
                offset=i.offset,
                kind=kind,
                slot_kind=_slot_kind(var, sym),
                name=var,
                span=sp,
            )
        )
        eid += 1
        if eid >= budget.dfg_max_edges + 2000:  # soft cap
            break
    return out


def _basic_blocks(ins: List[dis.Instruction]) -> Tuple[List[int], Dict[int, int], Dict[int, List[int]]]:
    """
    Return: (leaders offsets, off->block_id, preds block graph)
    """
    offsets = [i.offset for i in ins]
    offs_set = set(offsets)

    leaders: Set[int] = set()
    if offsets:
        leaders.add(offsets[0])
    for idx, i in enumerate(ins):
        if i.jump_target is not None and i.jump_target in offs_set:
            leaders.add(i.jump_target)
            if idx + 1 < len(ins):
                leaders.add(ins[idx + 1].offset)

    leaders_list = sorted(leaders)
    off2block: Dict[int, int] = {}
    # leader boundaries
    leader_idx = 0
    current_leader = leaders_list[0] if leaders_list else (offsets[0] if offsets else 0)
    for off in offsets:
        while leader_idx + 1 < len(leaders_list) and off >= leaders_list[leader_idx + 1]:
            leader_idx += 1
            current_leader = leaders_list[leader_idx]
        off2block[off] = leader_idx

    # predecessors from CFG (jump + fallthrough)
    preds: Dict[int, List[int]] = {i: [] for i in range(len(leaders_list))}
    # build succs first
    succs: Dict[int, Set[int]] = {i: set() for i in range(len(leaders_list))}
    for idx, i in enumerate(ins):
        b = off2block[i.offset]
        if i.jump_target is not None and i.jump_target in offs_set:
            succs[b].add(off2block[i.jump_target])
            if i.opname.startswith("POP_JUMP") or i.opname in {"FOR_ITER", "SEND"}:
                if idx + 1 < len(ins):
                    succs[b].add(off2block[ins[idx + 1].offset])
        else:
            if idx + 1 < len(ins):
                succs[b].add(off2block[ins[idx + 1].offset])
    for b, ss in succs.items():
        for s in ss:
            preds[s].append(b)

    return leaders_list, off2block, preds


def _reaching_defs(events: List[DefUseEvent], leaders: List[int], off2block: Dict[int, int], preds: Dict[int, List[int]], budget: Budget) -> List[DFGEdge]:
    """
    Slot-level reaching defs (variables only). This is intentionally shallow:
      - tracks DEF for (slot_kind,name) and connects to USE at same key.
    """
    # index events by offset
    events_by_off: Dict[int, List[DefUseEvent]] = {}
    for e in events:
        events_by_off.setdefault(e.offset, []).append(e)

    # per block: GEN (last def id per key), KILL (keys defined)
    Key = Tuple[str, str]  # (slot_kind, name)
    gen: Dict[int, Dict[Key, int]] = {b: {} for b in range(len(leaders))}
    kill: Dict[int, Set[Key]] = {b: set() for b in range(len(leaders))}

    # need stable ordering of offsets within blocks
    offsets_sorted = sorted(off2block.keys())
    for off in offsets_sorted:
        b = off2block[off]
        for e in events_by_off.get(off, []):
            if e.kind == "DEF":
                k = (e.slot_kind, e.name)
                kill[b].add(k)
                gen[b][k] = e.event_id  # last def wins

    # dataflow state: IN/OUT maps key -> set(def_ids)
    IN: List[Dict[Key, Set[int]]] = [dict() for _ in range(len(leaders))]
    OUT: List[Dict[Key, Set[int]]] = [dict() for _ in range(len(leaders))]

    changed = True
    iters = 0
    while changed and iters < 2000:
        changed = False
        iters += 1
        for b in range(len(leaders)):
            # IN[b] = union OUT[p]
            in_map: Dict[Key, Set[int]] = {}
            for p in preds.get(b, []):
                for k, defs in OUT[p].items():
                    in_map.setdefault(k, set()).update(defs)

            # OUT[b] = GEN[b] ∪ (IN[b] - KILL[b])
            out_map: Dict[Key, Set[int]] = {}
            # start from IN, dropping killed keys
            for k, defs in in_map.items():
                if k in kill[b]:
                    continue
                out_map[k] = set(defs)
            # add GEN as singleton defs (overwrites any existing)
            for k, def_id in gen[b].items():
                out_map[k] = {def_id}

            if in_map != IN[b] or out_map != OUT[b]:
                IN[b] = in_map
                OUT[b] = out_map
                changed = True

    # Now produce def->use edges by simulating within each block
    dfg: List[DFGEdge] = []
    # Precompute offsets grouped by block
    block_offsets: Dict[int, List[int]] = {b: [] for b in range(len(leaders))}
    for off in offsets_sorted:
        block_offsets[off2block[off]].append(off)

    for b in range(len(leaders)):
        cur_map: Dict[Key, Set[int]] = {k: set(v) for k, v in IN[b].items()}
        for off in block_offsets[b]:
            for e in events_by_off.get(off, []):
                k = (e.slot_kind, e.name)
                if e.kind == "USE":
                    for def_id in cur_map.get(k, set()):
                        dfg.append(DFGEdge(def_id, e.event_id, e.slot_kind, e.name))
                        if len(dfg) >= budget.dfg_max_edges:
                            return dfg
                else:  # DEF
                    cur_map[k] = {e.event_id}
    return dfg


def _build_dis_cfg_dfg(src: bytes, root: Node, module_co: types.CodeType, anchor: Node, sym: Optional[SymBinding], budget: Budget) -> DisCFGDFG:
    fn_node = _find_enclosing_function_node(anchor)
    co = module_co if fn_node is None else _find_codeobj_for_ts_function(src, module_co, fn_node)

    ins = list(dis.get_instructions(co))
    if len(ins) > budget.dis_max_instructions:
        ins = ins[: budget.dis_max_instructions]

    edges = _cfg_from_instructions(ins, budget.dis_max_edges)

    # Optional exception edges (private helper; only if you explicitly opt-in)
    if budget.dis_include_exception_edges_if_available:
        parse_exc = getattr(dis, "_parse_exception_table", None)
        if parse_exc is not None:
            offs_set = {i.offset for i in ins}
            for e in parse_exc(co):
                start, end, target = e.start, e.end, e.target
                if target not in offs_set:
                    continue
                for i in ins:
                    if start <= i.offset < end:
                        edges.append(CFGEdge(i.offset, target, "exc"))
                        if len(edges) >= budget.dis_max_edges:
                            break

    events = _defuse_events(src, root, ins, sym, budget)

    dfg_edges: List[DFGEdge] = []
    if budget.dfg_enable and events:
        leaders, off2block, preds = _basic_blocks(ins)
        if leaders:
            dfg_edges = _reaching_defs(events, leaders, off2block, preds, budget)

    return DisCFGDFG(
        code_name=co.co_name,
        firstlineno=co.co_firstlineno,
        instruction_count=len(ins),
        edges=edges,
        events=events,
        dfg_edges=dfg_edges,
    )


# ============================================================
# inspect plane (runtime surface; opt-in)
# ============================================================

def _ts_lift_to_call_callee_expr(anchor: Node) -> Optional[Node]:
    """
    If cursor is inside call args, lift to (call).child_by_field_name("function") when possible.
    Otherwise return anchor.
    """
    cur = anchor
    while cur is not None:
        if cur.type == "call":
            fn = cur.child_by_field_name("function")
            return fn
        cur = cur.parent
    return None


def _ts_dotted_expr(src: bytes, anchor: Node) -> str:
    """
    Purely syntactic dotted expression (identifier or attribute chain text slice).
    """
    callee = _ts_lift_to_call_callee_expr(anchor)
    n = callee if callee is not None else anchor
    return _slice(src, n.start_byte, n.end_byte, 10_000).strip()


def _inspect_member_kind(v: object) -> str:
    if isinstance(v, property):
        return "property"
    if inspect.isdatadescriptor(v):
        if inspect.isgetsetdescriptor(v):
            return "getset_descriptor"
        if inspect.ismemberdescriptor(v):
            return "member_descriptor"
        return "data_descriptor"
    if inspect.ismethoddescriptor(v):
        return "method_descriptor"
    if inspect.isfunction(v):
        return "function"
    if inspect.ismethod(v):
        return "bound_method"
    if inspect.isclass(v):
        return "class"
    if inspect.ismodule(v):
        return "module"
    return "value"


def _build_inspect_surface(src: bytes, root: Node, anchor: Node, budget: Budget) -> InspectSurface:
    if not budget.inspect_allow_import or not budget.inspect_module_name:
        return InspectSurface(
            module_name=budget.inspect_module_name or "",
            dotted_expr=_ts_dotted_expr(src, anchor),
            resolved_qualname="",
            unwrap_chain=[],
            signature_follow_wrapped=None,
            signature_no_unwrap=None,
            annotations_keys=[],
            members=[],
            error="inspect disabled (set inspect_allow_import=True and inspect_module_name=...)",
        )

    module_name = budget.inspect_module_name
    dotted = _ts_dotted_expr(src, anchor)

    try:
        mod = importlib.import_module(module_name)

        # Resolve dotted expr using getattr_static to avoid descriptor execution where possible
        obj: object = mod
        for part in dotted.split("."):
            obj = inspect.getattr_static(obj, part)  # static fetch

        # unwrap chain (bounded)
        unwrap_chain: List[str] = []
        cur = obj
        for _ in range(budget.inspect_unwrap_limit):
            unwrap_chain.append(getattr(cur, "__qualname__", repr(cur)))
            nxt = getattr(cur, "__wrapped__", None)
            if nxt is None:
                break
            cur = nxt

        sig_follow = None
        sig_raw = None
        try:
            sig_follow = str(inspect.signature(obj, follow_wrapped=budget.inspect_follow_wrapped, eval_str=False))
        except Exception:
            sig_follow = None
        try:
            sig_raw = str(inspect.signature(obj, follow_wrapped=False, eval_str=False))
        except Exception:
            sig_raw = None

        ann_keys: List[str] = []
        try:
            ann = inspect.get_annotations(obj, eval_str=False)
            ann_keys = list(ann.keys())[: budget.inspect_annotation_item_limit]
        except Exception:
            ann_keys = []

        members: List[InspectMember] = []
        if inspect.isclass(obj) or inspect.ismodule(obj):
            for name, value in inspect.getmembers_static(obj)[: budget.inspect_member_limit]:
                members.append(InspectMember(name=name, kind=_inspect_member_kind(value), value_type=type(value).__name__))

        return InspectSurface(
            module_name=module_name,
            dotted_expr=dotted,
            resolved_qualname=getattr(obj, "__qualname__", repr(obj)),
            unwrap_chain=unwrap_chain,
            signature_follow_wrapped=sig_follow,
            signature_no_unwrap=sig_raw,
            annotations_keys=ann_keys,
            members=members,
        )
    except Exception as e:
        return InspectSurface(
            module_name=module_name,
            dotted_expr=dotted,
            resolved_qualname="",
            unwrap_chain=[],
            signature_follow_wrapped=None,
            signature_no_unwrap=None,
            annotations_keys=[],
            members=[],
            error=f"{type(e).__name__}: {e}",
        )


# ============================================================
# Compact renderer (token-budget friendly)
# ============================================================

def render_compact(bundle: EnrichmentBundle, width: int = 110) -> str:
    def clip(s: str) -> str:
        return s if len(s) <= width else s[: width - 1] + "…"

    lines: List[str] = []
    lines.append("```text")
    ta = bundle.ts_anchor
    lines.append(clip(f"TS anchor  {ta.anchor.node_type}@{ta.anchor.start_byte}:{ta.anchor.end_byte}  {ta.anchor.text}"))
    if ta.parents:
        chain = " <- ".join(p.node_type for p in ta.parents[:6])
        lines.append(clip(f"TS parents {chain}"))
    if ta.peers_prev or ta.peers_next:
        pv = ", ".join(p.node_type for p in ta.peers_prev[:4]) if ta.peers_prev else "-"
        nx = ", ".join(p.node_type for p in ta.peers_next[:4]) if ta.peers_next else "-"
        lines.append(clip(f"TS sib    prev[{pv}] next[{nx}]"))

    if bundle.sym_binding:
        sb = bundle.sym_binding
        keyflags = ["is_local", "is_parameter", "is_free", "is_nonlocal", "is_declared_global", "is_imported"]
        flags = " ".join(f"{k}={int(sb.flags.get(k, False))}" for k in keyflags)
        lines.append(clip(f"SYM {sb.scope_type}:{sb.scope_name}@{sb.scope_lineno}  {sb.symbol}  {flags}"))
        if sb.capture_edges:
            caps = ", ".join(f"{n}" for (_, _, n) in sb.capture_edges[:8])
            lines.append(clip(f"SYM captures {caps}"))

    if bundle.dis_cfg_dfg:
        d = bundle.dis_cfg_dfg
        lines.append(clip(f"DIS code {d.code_name}@{d.firstlineno}  ins={d.instruction_count}  cfg_edges={len(d.edges)}"))
        if d.dfg_edges:
            lines.append(clip(f"DFG edges {len(d.dfg_edges)}  sample=" +
                              ", ".join(f"{e.name}:{e.slot_kind}" for e in d.dfg_edges[:6])))

    if bundle.inspect_surface:
        r = bundle.inspect_surface
        if r.error:
            lines.append(clip(f"INSPECT error {r.error}"))
        else:
            lines.append(clip(f"INSPECT {r.module_name}:{r.dotted_expr} -> {r.resolved_qualname}"))
            if r.signature_follow_wrapped:
                lines.append(clip(f"INSPECT sig(wrapped) {r.signature_follow_wrapped}"))
            if r.signature_no_unwrap:
                lines.append(clip(f"INSPECT sig(raw)     {r.signature_no_unwrap}"))
            if r.members:
                lines.append(clip("INSPECT members " + ", ".join(f"{m.name}:{m.kind}" for m in r.members[:12])))

    lines.append("```")
    return "\n".join(lines)
````

---

## How this maps to your requested bundle parts

* **`ts_anchor`**: built from `named_descendant_for_point_range`, `parent`, `prev_named_sibling/next_named_sibling`, `child_by_field_name`, and stable `node_id` usage. ([tree-sitter.github.io][1])
* **`sym_binding`**: joins tree-sitter scope `(type,name,lineno)` → symtable table, then uses `Symbol.is_local/is_free/is_parameter/...` flags + optional capture edges. ([Python documentation][4])
* **`dis_cfg_dfg`**: `dis.get_instructions()` and `Instruction.positions` give source spans for each instruction; CFG from `jump_target`/fallthrough; DFG via reaching-defs on `(slot_kind,name)`. ([Python documentation][5])
* **`inspect_surface`**: guarded runtime import; dotted expr from tree-sitter; static attribute traversal via `getattr_static`; signature and annotations via `signature(... follow_wrapped=..., eval_str=False)` and `get_annotations(eval_str=False)`; member inventory via `getmembers_static`. ([Python documentation][6])

---

## Minimal CQ usage

```python
provider = CQEnrichmentProvider()

bundle = provider.enrich(
    file_path="path/to/foo.py",
    cursor=Point(row=120, col=17),
    mode=EnrichmentMode.TS_SYM_DIS,
    budget=Budget(snippet_max_len=90),
)

print(render_compact(bundle))
```

To enable runtime `inspect` (explicit opt-in):

```python
bundle = provider.enrich(
    "path/to/foo.py",
    Point(120, 17),
    EnrichmentMode.FULL,
    budget=Budget(
        inspect_enable=True,
        inspect_allow_import=True,
        inspect_module_name="my_pkg.foo",
        inspect_member_limit=40,
    ),
)
```


[1]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Node.html "Node — py-tree-sitter 0.25.2 documentation"
[2]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Parser.html "Parser — py-tree-sitter 0.25.2 documentation"
[3]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Query.html "Query — py-tree-sitter 0.25.2 documentation"
[4]: https://docs.python.org/3/library/symtable.html "symtable — Access to the compiler’s symbol tables — Python 3.14.3 documentation"
[5]: https://docs.python.org/3/library/dis.html "dis — Disassembler for Python bytecode — Python 3.14.3 documentation"
[6]: https://docs.python.org/3/library/inspect.html "inspect — Inspect live objects — Python 3.14.3 documentation"

Below are the **remaining enrichment opportunities** from your list that I **hadn’t already shown with code**. For each one, I’m explicit about:

* **tree-sitter basis** (which nodes/spans/fields you use),
* the **symtable / dis / inspect call(s)**,
* and the **join** from those semantics back to the CST (line numbers, names, bytecode positions → CST spans, etc.).

I’m using only **official + primary** surfaces where available (symtable / dis / inspect docs), and I call out when something is **CPython-internal** (exception table parsing). ([Python documentation][1])

---

## Shared helpers (tree-sitter parse + span + def lookup)

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Optional, Tuple, List

from tree_sitter import Language, Parser, Node
import tree_sitter_python as tspython  # pip install tree-sitter-python

PY_LANGUAGE = Language(tspython.language())
PARSER = Parser(PY_LANGUAGE)

@dataclass(frozen=True)
class Point:
    row: int  # 0-based
    col: int  # 0-based

@dataclass(frozen=True)
class Span:
    start_byte: int
    end_byte: int
    start_point: Point
    end_point: Point
    node_type: str
    text: str

def ts_parse(src: bytes):
    return PARSER.parse(src)

def ts_text(src: bytes, n: Node) -> str:
    return src[n.start_byte:n.end_byte].decode("utf-8", errors="replace")

def ts_span(src: bytes, n: Node, max_len: int = 160) -> Span:
    s = " ".join(ts_text(src, n).split())
    if len(s) > max_len:
        s = s[:max_len - 1] + "…"
    return Span(
        start_byte=n.start_byte,
        end_byte=n.end_byte,
        start_point=Point(*n.start_point),
        end_point=Point(*n.end_point),
        node_type=n.type,
        text=s,
    )

def ts_anchor_node(root: Node, pt: Point) -> Node:
    p = (pt.row, pt.col)
    n = root.named_descendant_for_point_range(p, p)
    return n if n is not None else root

def ts_enclosing(node: Node, types: set[str]) -> Optional[Node]:
    cur = node
    while cur is not None:
        if cur.type in types:
            return cur
        cur = cur.parent
    return None

def ts_walk_named(n: Node) -> Iterator[Node]:
    yield n
    for ch in n.named_children:
        yield from ts_walk_named(ch)

def ts_find_def_by_name_and_lineno(root: Node, def_type: str, name: str, lineno_1based: int) -> Optional[Node]:
    """
    Join key: (name, start_line). Symtable tables report first line of block. :contentReference[oaicite:1]{index=1}
    """
    for n in ts_walk_named(root):
        if n.type != def_type:
            continue
        name_node = n.child_by_field_name("name")
        if name_node is None:
            continue
        if ts_text(SRC, name_node) == name and (n.start_point[0] + 1) == lineno_1based:
            return n
    return None
```

> In snippets below I use a `SRC` global just to keep examples short; in real code pass `src` through.

---

# A) `symtable` enrichments you didn’t yet have code for

## A1) Scope graph + meta-scopes (ANNOTATION / TYPE_ALIAS / TYPE_PARAMETERS / TYPE_VARIABLE) + table properties

**Incremental insight:** enumerate the full **compiler scope tree**, including new **annotation/type parameter** scopes, with `is_nested()` and `is_optimized()` so an agent knows “this thing is a separate scope / closure / optimized locals”. ([Python documentation][1])

**Tree-sitter basis:** module root + (optionally) def/class nodes for anchoring spans.
**Join:** symtable `(type, name, lineno)` → tree-sitter `function_definition/class_definition` `(start_line, name)`.

```python
import symtable
from typing import Dict, Any

def sym_scope_graph_with_ts_spans(src: bytes) -> list[dict[str, Any]]:
    global SRC
    SRC = src

    tree = ts_parse(src)
    root = tree.root_node

    st = symtable.symtable(src.decode("utf-8", errors="replace"), "<mem>", "exec")
    # symtable types include MODULE/FUNCTION/CLASS and annotation/type-related flavors. :contentReference[oaicite:3]{index=3}

    # parent pointers for symtable traversal
    parents: Dict[object, object] = {}
    stack = [st]
    while stack:
        t = stack.pop()
        for ch in t.get_children():  # nested symbol tables :contentReference[oaicite:4]{index=4}
            parents[ch] = t
            stack.append(ch)

    out = []
    stack = [st]
    while stack:
        t = stack.pop()

        t_type = t.get_type()         # SymbolTableType enum (3.13+) :contentReference[oaicite:5]{index=5}
        t_name = t.get_name()         # includes underlying name for type scopes :contentReference[oaicite:6]{index=6}
        t_lineno = t.get_lineno()

        # Try anchor to TS only for function/class/module scopes
        ts_node = None
        if str(t_type).endswith("MODULE") or str(t_type) == "SymbolTableType.MODULE":
            ts_node = root
        elif str(t_type).endswith("FUNCTION") or str(t_type) == "SymbolTableType.FUNCTION":
            ts_node = ts_find_def_by_name_and_lineno(root, "function_definition", t_name, t_lineno)
        elif str(t_type).endswith("CLASS") or str(t_type) == "SymbolTableType.CLASS":
            ts_node = ts_find_def_by_name_and_lineno(root, "class_definition", t_name, t_lineno)

        out.append({
            "table_id": t.get_id(),                 # stable per-table id :contentReference[oaicite:7]{index=7}
            "type": str(t_type),
            "name": t_name,
            "lineno": t_lineno,
            "is_nested": t.is_nested(),             # :contentReference[oaicite:8]{index=8}
            "is_optimized": t.is_optimized(),       # :contentReference[oaicite:9]{index=9}
            "ts_span": ts_span(src, ts_node).text if ts_node else None,
            "parent_id": parents.get(t).get_id() if t in parents else None,
        })

        stack.extend(t.get_children())

    return out
```

---

## A2) Function partitions (parameters / locals / globals / nonlocals / frees) for an enclosing function

**Incremental insight:** a deterministic “rename impact” surface: `get_frees()` is your closure capture list; `get_nonlocals()` tells you explicit `nonlocal` declarations, etc. ([Python documentation][1])

**Tree-sitter basis:** locate enclosing `function_definition`.
**Join:** `(fn_name, fn_start_line)` → symtable `Function` table.

```python
import symtable
from typing import Any

def sym_function_partitions_at_cursor(src: bytes, cursor: Point) -> dict[str, Any]:
    global SRC
    SRC = src

    tree = ts_parse(src)
    root = tree.root_node
    anchor = ts_anchor_node(root, cursor)

    fn = ts_enclosing(anchor, {"function_definition"})
    if fn is None:
        raise ValueError("Cursor is not inside a function_definition")

    fn_name = ts_text(src, fn.child_by_field_name("name"))
    fn_lineno = fn.start_point[0] + 1

    st = symtable.symtable(src.decode("utf-8", errors="replace"), "<mem>", "exec")

    # Find the matching function table
    target = None
    stack = [st]
    while stack:
        t = stack.pop()
        if str(t.get_type()).endswith("FUNCTION") and t.get_name() == fn_name and t.get_lineno() == fn_lineno:
            target = t
            break
        stack.extend(t.get_children())

    if target is None:
        raise RuntimeError("Could not map TS function to symtable.Function")

    # Function partitions APIs :contentReference[oaicite:11]{index=11}
    return {
        "function": f"{fn_name}@{fn_lineno}",
        "parameters": list(target.get_parameters()),
        "locals": list(target.get_locals()),
        "globals": list(target.get_globals()),
        "nonlocals": list(target.get_nonlocals()),
        "frees": list(target.get_frees()),
    }
```

---

## A3) Namespace binding (is_namespace + get_namespace(s)) to build a **lexical scope graph** (defs → child tables)

**Incremental insight:** from a TS `def foo` / `class C`, jump to the exact symtable child scope(s) created by that binding. This is the **compiler-grade** “this name introduces a namespace”. ([Python documentation][1])

**Tree-sitter basis:** enumerate module-level `function_definition` and `class_definition`.
**Join:** TS def name → symtable module table `lookup(name)` → `Symbol.is_namespace()` / `get_namespaces()`.

```python
import symtable
from typing import Any

def sym_namespace_edges_from_module(src: bytes) -> list[dict[str, Any]]:
    global SRC
    SRC = src

    tree = ts_parse(src)
    root = tree.root_node

    st = symtable.symtable(src.decode("utf-8", errors="replace"), "<mem>", "exec")  # top-level table :contentReference[oaicite:13]{index=13}

    edges = []
    for n in ts_walk_named(root):
        if n.type not in {"function_definition", "class_definition"}:
            continue
        name_node = n.child_by_field_name("name")
        if name_node is None:
            continue
        name = ts_text(src, name_node)

        sym = st.lookup(name)                 # :contentReference[oaicite:14]{index=14}
        if not sym.is_namespace():            # :contentReference[oaicite:15]{index=15}
            continue

        # A single name can bind multiple namespaces; use get_namespaces() :contentReference[oaicite:16]{index=16}
        namespaces = sym.get_namespaces()
        for child in namespaces:
            edges.append({
                "ts_def": ts_span(src, n).text,
                "name": name,
                "sym_child_table_id": child.get_id(),
                "sym_child_type": str(child.get_type()),
                "sym_child_lineno": child.get_lineno(),
            })

    return edges
```

---

## A4) A single `resolve_binding(scope_table, name)` that returns a **canonical binding slot id**

**Incremental insight:** make *everything* (TS occurrences, dis def/use events, even “SCIP missing” calls) point at the same binding identity: `binding_id = f"{definer_table_id}:{name}"`.

**Tree-sitter basis:** for an identifier occurrence, use enclosing scope node (`function_definition`/`class_definition`/`module`).
**Join:** map that scope to a symtable table, then resolve using symbol flags. ([Python documentation][1])

```python
import symtable
from dataclasses import dataclass
from typing import Optional, Dict

@dataclass(frozen=True)
class Binding:
    binding_id: str
    definer_table_id: int
    name: str
    resolution: str  # "local"|"param"|"global"|"nonlocal"|"free"|"imported"|"unknown"

def sym_parent_map(root: symtable.SymbolTable) -> Dict[symtable.SymbolTable, symtable.SymbolTable]:
    parents = {}
    stack = [root]
    while stack:
        t = stack.pop()
        for ch in t.get_children():  # :contentReference[oaicite:18]{index=18}
            parents[ch] = t
            stack.append(ch)
    return parents

def find_sym_table_for_ts_scope(st: symtable.SymbolTable, scope_type: str, scope_name: Optional[str], lineno: int):
    stack = [st]
    while stack:
        t = stack.pop()
        if str(t.get_type()) == scope_type and t.get_lineno() == lineno:
            if scope_name is None or t.get_name() == scope_name:
                return t
        stack.extend(t.get_children())
    return None

def resolve_binding(st: symtable.SymbolTable, table: symtable.SymbolTable, name: str) -> Binding:
    sym = table.lookup(name)  # :contentReference[oaicite:19]{index=19}

    # declared global -> module
    if sym.is_declared_global() or sym.is_global():  # :contentReference[oaicite:20]{index=20}
        # find module/root table (walk parents until type=MODULE)
        return Binding(f"{st.get_id()}:{name}", st.get_id(), name, "global")

    # local / parameter / imported live in this table
    if sym.is_parameter():  # :contentReference[oaicite:21]{index=21}
        return Binding(f"{table.get_id()}:{name}", table.get_id(), name, "param")
    if sym.is_local() or sym.is_assigned():  # :contentReference[oaicite:22]{index=22}
        return Binding(f"{table.get_id()}:{name}", table.get_id(), name, "local")
    if sym.is_imported():  # :contentReference[oaicite:23]{index=23}
        return Binding(f"{table.get_id()}:{name}", table.get_id(), name, "imported")

    # free/nonlocal -> find outer definer
    if sym.is_free() or sym.is_nonlocal():  # :contentReference[oaicite:24]{index=24}
        parents = sym_parent_map(st)
        cur = parents.get(table)
        while cur is not None:
            if name in cur.get_identifiers():  # :contentReference[oaicite:25]{index=25}
                outer_sym = cur.lookup(name)
                if outer_sym.is_assigned() or outer_sym.is_parameter() or outer_sym.is_imported() or outer_sym.is_declared_global():
                    return Binding(f"{cur.get_id()}:{name}", cur.get_id(), name, "free/nonlocal")
            cur = parents.get(cur)
        # unresolved free
        return Binding(f"UNRESOLVED:{name}", -1, name, "free_unresolved")

    return Binding(f"{table.get_id()}:{name}", table.get_id(), name, "unknown")

def resolve_binding_for_ts_identifier(src: bytes, cursor: Point) -> Binding:
    global SRC
    SRC = src

    tree = ts_parse(src)
    root = tree.root_node
    anchor = ts_anchor_node(root, cursor)

    # Tree-sitter basis: target identifier text + enclosing scope
    ident = anchor if anchor.type == "identifier" else ts_enclosing(anchor, {"identifier"})
    if ident is None:
        raise ValueError("No identifier at cursor")
    name = ts_text(src, ident)

    scope = ts_enclosing(ident, {"function_definition", "class_definition", "module"})
    if scope is None:
        scope = root

    # Join to symtable by (type,name,lineno)
    st = symtable.symtable(src.decode("utf-8", errors="replace"), "<mem>", "exec")

    if scope.type == "module":
        table = st
    else:
        scope_name = ts_text(src, scope.child_by_field_name("name"))
        lineno = scope.start_point[0] + 1
        scope_type = "SymbolTableType.FUNCTION" if scope.type == "function_definition" else "SymbolTableType.CLASS"
        table = find_sym_table_for_ts_scope(st, scope_type, scope_name, lineno) or st

    return resolve_binding(st, table, name)
```

---

## A5) Correctness oracle / invariants: validate that dis “spaces” match symtable truth (and attribute failures back to CST spans)

**Incremental insight:** “brutal invariants” that catch binder drift. Example: if bytecode uses `LOAD_FAST*`, that name must be local/param. `Instruction.positions` lets you point at the exact sub-expression in source. ([Python documentation][2])

**Tree-sitter basis:** enclosing function span; and a root node to anchor failing instructions via positions.
**Join:** symtable partitions + code object varnames/freevars/cellvars + dis `Instruction.baseopname/argval/positions`. ([Python documentation][2])

```python
import symtable, dis, types
from dataclasses import dataclass
from typing import Any

@dataclass(frozen=True)
class InvariantViolation:
    kind: str
    message: str
    instr_offset: int
    instr_baseop: str
    name: Optional[str]
    ts_attribution: Optional[Span]

def ts_node_for_positions(root: Node, pos: dis.Positions) -> Optional[Node]:
    if pos is None or pos.lineno is None or pos.col_offset is None:
        return None
    start = (pos.lineno - 1, pos.col_offset)
    end = (pos.end_lineno - 1, pos.end_col_offset) if (pos.end_lineno and pos.end_col_offset is not None) else start
    return root.named_descendant_for_point_range(start, end)

def compile_module(src: bytes) -> types.CodeType:
    return compile(src.decode("utf-8", errors="replace"), "<mem>", "exec")

def walk_codeobjs(co: types.CodeType):
    yield co
    for c in co.co_consts:
        if isinstance(c, types.CodeType):
            yield from walk_codeobjs(c)

def find_fn_codeobj(module_co: types.CodeType, fn_name: str, fn_firstlineno: int) -> types.CodeType:
    for co in walk_codeobjs(module_co):
        if co.co_name == fn_name and co.co_firstlineno == fn_firstlineno:
            return co
    raise KeyError("function code object not found")

def validate_sym_dis_alignment(src: bytes, cursor: Point) -> list[InvariantViolation]:
    global SRC
    SRC = src

    tree = ts_parse(src)
    root = tree.root_node
    anchor = ts_anchor_node(root, cursor)

    fn = ts_enclosing(anchor, {"function_definition"})
    if fn is None:
        raise ValueError("Cursor must be inside a function_definition")

    fn_name = ts_text(src, fn.child_by_field_name("name"))
    fn_firstlineno = fn.start_point[0] + 1

    st = symtable.symtable(src.decode("utf-8", errors="replace"), "<mem>", "exec")
    # find matching symtable.Function
    fn_table = None
    stack = [st]
    while stack:
        t = stack.pop()
        if str(t.get_type()).endswith("FUNCTION") and t.get_name() == fn_name and t.get_lineno() == fn_firstlineno:
            fn_table = t
            break
        stack.extend(t.get_children())
    if fn_table is None:
        raise RuntimeError("Could not map TS function to symtable.Function")

    locals_set = set(fn_table.get_locals()) | set(fn_table.get_parameters())  # :contentReference[oaicite:28]{index=28}
    frees_set = set(fn_table.get_frees())  # :contentReference[oaicite:29]{index=29}

    module_co = compile_module(src)
    fn_co = find_fn_codeobj(module_co, fn_name, fn_firstlineno)

    # bytecode side truth
    co_varnames = set(fn_co.co_varnames)
    co_freevars = set(fn_co.co_freevars)
    co_cellvars = set(fn_co.co_cellvars)

    violations: list[InvariantViolation] = []
    for ins in dis.get_instructions(fn_co):  # Instruction has baseopname/argval/positions :contentReference[oaicite:30]{index=30}
        base = ins.baseopname
        name = ins.argval if isinstance(ins.argval, str) else None

        tn = ts_node_for_positions(root, ins.positions)
        sp = ts_span(src, tn) if tn is not None else None

        # FAST invariants: LOAD_FAST / STORE_FAST names should be local/param
        if base in {"LOAD_FAST", "STORE_FAST", "DELETE_FAST"} and name:
            if name not in locals_set:
                violations.append(InvariantViolation(
                    "FAST_NOT_LOCAL",
                    f"{base} uses {name!r} but symtable does not classify it as local/param",
                    ins.offset, base, name, sp
                ))
            if name not in co_varnames:
                violations.append(InvariantViolation(
                    "FAST_NOT_IN_COVARNAMES",
                    f"{base} uses {name!r} but name not in code.co_varnames",
                    ins.offset, base, name, sp
                ))

        # DEREF invariants: LOAD_DEREF / STORE_DEREF should be closure vars
        if base in {"LOAD_DEREF", "STORE_DEREF", "DELETE_DEREF"} and name:
            if name not in (co_freevars | co_cellvars):
                violations.append(InvariantViolation(
                    "DEREF_NOT_CLOSURE",
                    f"{base} uses {name!r} but not in code.co_freevars/co_cellvars",
                    ins.offset, base, name, sp
                ))
            # if it’s truly free, symtable should agree (best-effort)
            if name in co_freevars and name not in frees_set:
                violations.append(InvariantViolation(
                    "FREEVAR_SYM_MISMATCH",
                    f"{name!r} in code.co_freevars but not in symtable.get_frees()",
                    ins.offset, base, name, sp
                ))

    return violations
```

---

# B) `dis` enrichments you didn’t yet have code for

## B1) “Normalized instruction facts”: baseopname + cache_info + specialized/adaptive views

**Incremental insight:** record stable “semantic opcode” (`baseopname`) while optionally capturing specialization/caches for perf/debugging, not semantics. `Instruction.positions` supplies source locations. ([Python documentation][2])

```python
import dis
from typing import Any, Iterator

def iter_normalized_instructions(codeobj, *, adaptive: bool = False) -> Iterator[dict[str, Any]]:
    """
    dis.get_instructions(..., adaptive=...) yields Instruction objects. :contentReference[oaicite:32]{index=32}
    Instruction fields include baseopname/baseopcode/jump_target/positions/cache_info. :contentReference[oaicite:33]{index=33}
    """
    for ins in dis.get_instructions(codeobj, adaptive=adaptive):
        yield {
            "offset": ins.offset,
            "opname": ins.opname,
            "baseopname": ins.baseopname,
            "oparg": ins.oparg,
            "argval": ins.argval,
            "jump_target": ins.jump_target,
            "positions": None if ins.positions is None else {
                "lineno": ins.positions.lineno,
                "end_lineno": ins.positions.end_lineno,
                "col": ins.positions.col_offset,
                "end_col": ins.positions.end_col_offset,
            },
            "cache_info": ins.cache_info,  # cache triplets if present :contentReference[oaicite:34]{index=34}
        }
```

---

## B2) Exception table semantics (try regions → handlers) from `co_exceptiontable` (CPython) and mapping to CST spans

**Incremental insight:** when you *choose* to go CPython-specific, you can attribute “this bytecode range is covered by handler X” back to TS spans.

**Important:** parsing the exception table is not part of the public `dis` API; the most practical approach is to use CPython internals (`dis._parse_exception_table`) when present. The existence of the `co_exceptiontable` attribute in the code object interface is visible in CPython source (e.g., `code.replace(..., co_exceptiontable=...)`). ([GitHub][3])

**Tree-sitter basis:** root node; `Instruction.positions` → point-range lookup. ([Python documentation][2])

```python
import dis
from dataclasses import dataclass
from typing import Optional, Any

@dataclass(frozen=True)
class ExceptionRegion:
    start_off: int
    end_off: int
    target_off: int
    depth: int
    lasti: bool
    ts_span: Optional[Span]        # approximate source span for region
    handler_ts_span: Optional[Span]

def parse_exception_regions_to_ts(src: bytes, root: Node, codeobj) -> list[ExceptionRegion]:
    """
    CPython internal: dis._parse_exception_table(codeobj) (if available).
    Maps region offsets to approximate source spans via instruction positions.
    """
    parse_exc = getattr(dis, "_parse_exception_table", None)
    if parse_exc is None:
        return []

    ins_list = list(dis.get_instructions(codeobj))
    off2ins = {i.offset: i for i in ins_list}

    def ts_for_offset(off: int) -> Optional[Span]:
        i = off2ins.get(off)
        if i is None or i.positions is None:
            return None
        tn = ts_node_for_positions(root, i.positions)
        return ts_span(src, tn) if tn is not None else None

    out = []
    for e in parse_exc(codeobj):
        # CPython returns entries with attrs start/end/target/depth/lasti (shape may vary by version)
        start, end, target = e.start, e.end, e.target
        out.append(ExceptionRegion(
            start_off=start,
            end_off=end,
            target_off=target,
            depth=getattr(e, "depth", 0),
            lasti=bool(getattr(e, "lasti", False)),
            ts_span=ts_for_offset(start),
            handler_ts_span=ts_for_offset(target),
        ))
    return out
```

---

## B3) Span anchoring + sanity checklists (coverage + “unattributed hot spots”)

**Incremental insight:** you can tell an agent whether your bytecode/CFG/DFG attribution is *trustworthy*, and where it’s missing.

`Instruction.positions` / `dis.Positions` fields are explicit. ([Python documentation][2])

```python
import dis
from typing import Any

def bytecode_attribution_metrics(src: bytes, root: Node, codeobj) -> dict[str, Any]:
    ins = list(dis.get_instructions(codeobj))
    total = len(ins)

    with_pos = 0
    anchored = 0
    missing_offsets: list[int] = []

    for i in ins:
        if i.positions is not None and i.positions.lineno is not None and i.positions.col_offset is not None:
            with_pos += 1
            tn = ts_node_for_positions(root, i.positions)
            if tn is not None:
                anchored += 1
            else:
                missing_offsets.append(i.offset)

    return {
        "instruction_count": total,
        "with_positions": with_pos,
        "anchored_to_ts": anchored,
        "positions_coverage": (with_pos / total) if total else 0.0,
        "ts_anchor_coverage": (anchored / total) if total else 0.0,
        "unanchored_offsets_sample": missing_offsets[:30],
    }
```

---

# C) `inspect` enrichments you didn’t yet have code for

## C1) Object inventory (“what actually exists”) anchored to TS defs/imports

**Incremental insight:** build a table of runtime objects with flags (module/class/function/builtin/descriptor…) for the entities your agent is touching.

`inspect.getmembers_static` avoids triggering descriptor protocol / `__getattr__` / `__getattribute__`. ([Python documentation][4])
`inspect.isfunction/isclass/ismodule/...` gives object category tests. ([Python documentation][4])

**Tree-sitter basis:** discover top-level defs and (optional) imports; produce candidate dotted names.
**Join:** `module_name + qualname` (or `getattr_static` traversal) → inspect facts.

```python
import importlib
import inspect
from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class ObjectFact:
    dotted: str
    ts_def_span: Optional[Span]
    kind: str
    is_builtin: bool
    qualname: str
    type_name: str

def resolve_static(mod, dotted: str):
    obj = mod
    for part in dotted.split("."):
        obj = inspect.getattr_static(obj, part)  # :contentReference[oaicite:40]{index=40}
    return obj

def object_kind(o) -> str:
    if inspect.ismodule(o): return "module"
    if inspect.isclass(o): return "class"
    if inspect.isfunction(o): return "function"
    if inspect.ismethod(o): return "method"
    if inspect.isbuiltin(o): return "builtin"
    if inspect.isdatadescriptor(o): return "data_descriptor"
    if inspect.ismethoddescriptor(o): return "method_descriptor"
    return "value"

def ts_top_level_defs(root: Node) -> list[Node]:
    # simplistic: module's direct named children that are defs/classes
    return [ch for ch in root.named_children if ch.type in {"function_definition", "class_definition"}]

def inspect_object_inventory_for_file(src: bytes, module_name: str) -> list[ObjectFact]:
    global SRC
    SRC = src

    tree = ts_parse(src)
    root = tree.root_node
    mod = importlib.import_module(module_name)

    facts: list[ObjectFact] = []

    for d in ts_top_level_defs(root):
        name_node = d.child_by_field_name("name")
        if name_node is None:
            continue
        name = ts_text(src, name_node)
        dotted = name  # top-level; you can extend with qualname if you want nested

        try:
            obj = resolve_static(mod, dotted)
            facts.append(ObjectFact(
                dotted=dotted,
                ts_def_span=ts_span(src, d),
                kind=object_kind(obj),
                is_builtin=inspect.isbuiltin(obj),
                qualname=getattr(obj, "__qualname__", ""),
                type_name=type(obj).__name__,
            ))
        except Exception:
            facts.append(ObjectFact(
                dotted=dotted,
                ts_def_span=ts_span(src, d),
                kind="unresolved",
                is_builtin=False,
                qualname="",
                type_name="",
            ))

    return facts
```

---

## C2) Wrapper chain semantics: explicit `unwrap(stop=...)` that matches signature’s “stop at **signature**” behavior

`inspect.unwrap` supports `stop=`; docs note `signature()` uses this to stop if any object in the chain has `__signature__`. ([Python documentation][4])

**Tree-sitter basis:** call node callee expression text (`identifier` or attribute chain).
**Join:** resolve runtime object, then unwrap with stop.

```python
import inspect

def unwrap_chain_stop_on_signature(obj, max_hops: int = 16):
    chain = []
    cur = obj
    for _ in range(max_hops):
        chain.append(getattr(cur, "__qualname__", repr(cur)))
        nxt = getattr(cur, "__wrapped__", None)
        if nxt is None:
            break
        # stop if current has __signature__ (like inspect.signature does) :contentReference[oaicite:42]{index=42}
        if hasattr(cur, "__signature__"):
            break
        cur = nxt
    # canonical "unwrapped" per inspect.unwrap semantics
    unwrapped = inspect.unwrap(obj, stop=lambda o: hasattr(o, "__signature__"))
    return {"chain": chain, "unwrapped_qualname": getattr(unwrapped, "__qualname__", repr(unwrapped))}
```

---

## C3) Signature binding simulation from a tree-sitter callsite (bind / bind_partial / apply_defaults)

**Incremental insight:** for callsite edits (adding args, renaming parameters, moving keyword-only params), you can validate the **shape** against the runtime signature *without evaluating code*.

`inspect.signature(..., follow_wrapped=..., eval_str=...)` is documented. ([Python documentation][4])
`Signature.bind` and `bind_partial` are documented. ([Python documentation][4])

**Tree-sitter basis:** `call` node: `function` field + `arguments` field; parse out positional count + explicit keyword names.
**Join:** resolve runtime callable → signature → bind_partial using placeholder sentinels.

```python
import inspect
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

SENTINEL = object()

@dataclass(frozen=True)
class CallShape:
    callee_text: str
    positional_count: int
    keyword_names: List[str]
    has_star_args: bool
    has_starstar_kwargs: bool

def ts_call_at_point(root: Node, pt: Point) -> Optional[Node]:
    n = root.named_descendant_for_point_range((pt.row, pt.col), (pt.row, pt.col))
    return ts_enclosing(n, {"call"}) if n is not None else None

def ts_extract_call_shape(src: bytes, call: Node) -> CallShape:
    fn = call.child_by_field_name("function")
    args = call.child_by_field_name("arguments")
    callee_text = ts_text(src, fn).strip() if fn is not None else ""

    positional = 0
    kw_names: List[str] = []
    has_star = False
    has_starstar = False

    if args is not None:
        for a in args.named_children:
            t = ts_text(src, a).strip()
            if t.startswith("**"):
                has_starstar = True
                continue
            if t.startswith("*"):
                has_star = True
                continue

            # keyword_argument shape varies; simplest robust parse: look for "=" at top-level
            if "=" in t and not t.lstrip().startswith(("(", "[" , "{")):
                # naive: take token before "="
                kw = t.split("=", 1)[0].strip()
                if kw.isidentifier():
                    kw_names.append(kw)
                else:
                    # might be complex keyword target; ignore
                    pass
            else:
                positional += 1

    return CallShape(callee_text, positional, kw_names, has_star, has_starstar)

def bind_call_shape(sig: inspect.Signature, shape: CallShape) -> Tuple[bool, str, Optional[inspect.BoundArguments]]:
    # We cannot know the runtime contents of *args/**kwargs expansions; so we bind *only* explicit args.
    pos_args = [SENTINEL] * shape.positional_count
    kw_args = {k: SENTINEL for k in shape.keyword_names}

    try:
        ba = sig.bind_partial(*pos_args, **kw_args)  # :contentReference[oaicite:45]{index=45}
        ba.apply_defaults()                          # BoundArguments.apply_defaults() :contentReference[oaicite:46]{index=46}
        return True, "ok", ba
    except TypeError as e:
        return False, str(e), None

def check_callsite_against_runtime_signature(src: bytes, module_name: str, pt: Point) -> dict[str, Any]:
    global SRC
    SRC = src

    tree = ts_parse(src)
    root = tree.root_node
    call = ts_call_at_point(root, pt)
    if call is None:
        raise ValueError("No call at cursor")

    shape = ts_extract_call_shape(src, call)

    mod = importlib.import_module(module_name)
    obj = resolve_static(mod, shape.callee_text)  # getattr_static traversal :contentReference[oaicite:47]{index=47}

    sig_wrapped = inspect.signature(obj, follow_wrapped=True, eval_str=False)   # :contentReference[oaicite:48]{index=48}
    sig_raw = inspect.signature(obj, follow_wrapped=False, eval_str=False)      # :contentReference[oaicite:49]{index=49}

    ok_w, msg_w, ba_w = bind_call_shape(sig_wrapped, shape)
    ok_r, msg_r, ba_r = bind_call_shape(sig_raw, shape)

    return {
        "callee": shape.callee_text,
        "shape": dataclasses.asdict(shape),
        "sig_follow_wrapped": str(sig_wrapped),
        "sig_raw": str(sig_raw),
        "bind_follow_wrapped": {"ok": ok_w, "msg": msg_w, "bound": (dict(ba_w.arguments) if ba_w else None)},
        "bind_raw": {"ok": ok_r, "msg": msg_r, "bound": (dict(ba_r.arguments) if ba_r else None)},
        "ts_call_span": ts_span(src, call),
    }
```

---

## C4) Runtime-state → source joins (tracebacks + frames + generators/coros), mapped to CST spans

This was the big missing one from your earlier list.

### C4.1 Traceback → exact expression span (FrameInfo.positions / dis.Positions → tree-sitter)

`inspect.FrameInfo.positions` is a `dis.Positions` object containing start/end line/col for the *current instruction* in that frame. ([Python documentation][4])
Also, `dis.Bytecode.from_traceback(tb)` builds a Bytecode with `current_offset` set to the instruction responsible for the exception. ([Python documentation][2])

**Tree-sitter basis:** root node; point-range lookup for the positions.

```python
import inspect
import dis
from dataclasses import dataclass
from typing import Any, Optional

@dataclass(frozen=True)
class RuntimeAttribution:
    filename: str
    function: str
    lineno: int
    positions: Optional[dict[str, int]]
    ts_span: Optional[Span]
    tb_lasti: int

def frameinfo_to_ts_span(src: bytes, root: Node, frame) -> Optional[Span]:
    fi = inspect.getframeinfo(frame)  # FrameInfo has .positions :contentReference[oaicite:52]{index=52}
    pos = fi.positions
    if pos is None or pos.lineno is None or pos.col_offset is None:
        return None
    tn = root.named_descendant_for_point_range((pos.lineno - 1, pos.col_offset),
                                               (pos.end_lineno - 1, pos.end_col_offset))
    return ts_span(src, tn) if tn is not None else None

def attribute_exception_to_ts(src: bytes, exc: BaseException) -> list[RuntimeAttribution]:
    global SRC
    SRC = src
    tree = ts_parse(src)
    root = tree.root_node

    out: list[RuntimeAttribution] = []
    tb = exc.__traceback__
    while tb is not None:
        f = tb.tb_frame
        sp = frameinfo_to_ts_span(src, root, f)

        fi = inspect.getframeinfo(f)
        pos = fi.positions
        pos_dict = None
        if pos is not None:
            pos_dict = {"lineno": pos.lineno, "end_lineno": pos.end_lineno, "col": pos.col_offset, "end_col": pos.end_col_offset}

        out.append(RuntimeAttribution(
            filename=fi.filename,
            function=fi.function,
            lineno=fi.lineno,
            positions=pos_dict,
            ts_span=sp,
            tb_lasti=tb.tb_lasti,
        ))
        tb = tb.tb_next

    return out

def example_run_and_attribute(src: bytes, fn) -> list[RuntimeAttribution]:
    try:
        fn()
        return []
    except Exception as e:
        # Optional: use dis.Bytecode.from_traceback for bytecode-side context :contentReference[oaicite:53]{index=53}
        _ = dis.Bytecode.from_traceback(e.__traceback__)
        return attribute_exception_to_ts(src, e)
```

### C4.2 Generator / coroutine / asyncgen “where is it suspended?” mapped to CST

`inspect.getgeneratorstate`, `getcoroutinestate`, `getasyncgenstate` provide state; objects expose frames (`gi_frame`, `cr_frame`, `ag_frame`). ([Python documentation][4])
Then use `inspect.getframeinfo(frame).positions` → tree-sitter span as above. ([Python documentation][4])

```python
import inspect
from typing import Any

def attribute_generator_like_to_ts(src: bytes, genlike: Any) -> dict[str, Any]:
    global SRC
    SRC = src
    tree = ts_parse(src)
    root = tree.root_node

    # Determine type/state
    if inspect.isgenerator(genlike):
        state = inspect.getgeneratorstate(genlike)          # :contentReference[oaicite:56]{index=56}
        frame = genlike.gi_frame
    elif inspect.iscoroutine(genlike):
        state = inspect.getcoroutinestate(genlike)          # :contentReference[oaicite:57]{index=57}
        frame = genlike.cr_frame
    elif inspect.isasyncgen(genlike):
        state = inspect.getasyncgenstate(genlike)           # :contentReference[oaicite:58]{index=58}
        frame = genlike.ag_frame
    else:
        raise TypeError("not generator/coroutine/asyncgen")

    ts_sp = frameinfo_to_ts_span(src, root, frame) if frame else None
    return {"state": str(state), "ts_span": (dataclasses.asdict(ts_sp) if ts_sp else None)}
```

---

# D) Compound (“because they compose”) snippets you hadn’t yet gotten

## D1) End-to-end name identity: unify TS identifier occurrences + dis def/use events onto the same binding_id

**Incremental insight:** one joinable graph key across planes: every TS occurrence and every dis event resolves to the same `binding_id`.

**Tree-sitter basis:** identifier occurrences inside a function subtree.
**Join:** `resolve_binding` from A4 + `Instruction.positions` mapping.

```python
import dis, symtable
from dataclasses import dataclass
from typing import Any

@dataclass(frozen=True)
class TSBindingOccurrence:
    span: Span
    binding_id: str

@dataclass(frozen=True)
class DisBindingEvent:
    offset: int
    baseopname: str
    name: str
    binding_id: str
    span: Optional[Span]

def ts_identifiers_in_subtree(src: bytes, subtree: Node) -> list[Node]:
    return [n for n in ts_walk_named(subtree) if n.type == "identifier"]

def unify_bindings_for_function(src: bytes, cursor: Point) -> dict[str, Any]:
    global SRC
    SRC = src

    tree = ts_parse(src)
    root = tree.root_node
    anchor = ts_anchor_node(root, cursor)
    fn = ts_enclosing(anchor, {"function_definition"})
    if fn is None:
        raise ValueError("Cursor must be inside a function_definition")

    fn_name = ts_text(src, fn.child_by_field_name("name"))
    fn_lineno = fn.start_point[0] + 1

    st = symtable.symtable(src.decode("utf-8", errors="replace"), "<mem>", "exec")
    # find symtable table
    fn_table = find_sym_table_for_ts_scope(st, "SymbolTableType.FUNCTION", fn_name, fn_lineno) or st

    # --- TS occurrences -> binding ids
    ts_occ: list[TSBindingOccurrence] = []
    for ident in ts_identifiers_in_subtree(src, fn):
        name = ts_text(src, ident)
        b = resolve_binding(st, fn_table, name)
        ts_occ.append(TSBindingOccurrence(ts_span(src, ident), b.binding_id))

    # --- dis events -> binding ids
    module_co = compile_module(src)
    fn_co = find_fn_codeobj(module_co, fn_name, fn_lineno)

    dis_events: list[DisBindingEvent] = []
    for ins in dis.get_instructions(fn_co):
        if not isinstance(ins.argval, str):
            continue
        if not (ins.baseopname.startswith("LOAD_") or ins.baseopname.startswith("STORE_") or ins.baseopname.startswith("DELETE_")):
            continue
        name = ins.argval
        b = resolve_binding(st, fn_table, name)
        tn = ts_node_for_positions(root, ins.positions) if ins.positions else None
        sp = ts_span(src, tn) if tn is not None else None
        dis_events.append(DisBindingEvent(ins.offset, ins.baseopname, name, b.binding_id, sp))

    return {
        "function": f"{fn_name}@{fn_lineno}",
        "ts_occurrences": ts_occ[:200],     # budget
        "dis_events": dis_events[:400],     # budget
    }
```

---

## D2) Callsite correctness under decorators: TS callsite + symtable classification + inspect signature

**Incremental insight:** decide *how* to resolve the callee (local/free/imported/global), then inspect the right callable surface (wrapped/raw) and validate call shape.

This uses:

* symtable classification (`is_imported/is_local/is_free/...`). ([Python documentation][1])
* inspect signature follow_wrapped and bind_partial. ([Python documentation][4])

```python
def callsite_correctness_under_decorators(src: bytes, module_name: str, pt: Point) -> dict[str, Any]:
    global SRC
    SRC = src

    tree = ts_parse(src)
    root = tree.root_node

    call = ts_call_at_point(root, pt)
    if call is None:
        raise ValueError("No call at cursor")

    shape = ts_extract_call_shape(src, call)

    # symtable classify callee name *syntactically* (best for simple identifiers)
    st = symtable.symtable(src.decode("utf-8", errors="replace"), "<mem>", "exec")
    scope_node = ts_enclosing(call, {"function_definition", "class_definition", "module"}) or root
    if scope_node.type == "module":
        scope_table = st
    else:
        scope_name = ts_text(src, scope_node.child_by_field_name("name"))
        lineno = scope_node.start_point[0] + 1
        scope_type = "SymbolTableType.FUNCTION" if scope_node.type == "function_definition" else "SymbolTableType.CLASS"
        scope_table = find_sym_table_for_ts_scope(st, scope_type, scope_name, lineno) or st

    # only reliable if callee is a single identifier (not attribute chain)
    callee_ident = shape.callee_text.split(".")[0]
    sym = scope_table.lookup(callee_ident)
    flags = {
        "is_imported": sym.is_imported(),
        "is_local": sym.is_local(),
        "is_free": sym.is_free(),
        "is_global": sym.is_global(),
        "is_declared_global": sym.is_declared_global(),
    }  # :contentReference[oaicite:61]{index=61}

    # runtime resolve (opt-in: module import)
    mod = importlib.import_module(module_name)
    obj = resolve_static(mod, shape.callee_text)

    sig_wrapped = inspect.signature(obj, follow_wrapped=True, eval_str=False)   # :contentReference[oaicite:62]{index=62}
    ok, msg, ba = bind_call_shape(sig_wrapped, shape)                           # bind_partial :contentReference[oaicite:63]{index=63}

    return {
        "callee": shape.callee_text,
        "sym_flags_for_callee_ident": flags,
        "sig_follow_wrapped": str(sig_wrapped),
        "bind_ok": ok,
        "bind_msg": msg,
        "bound_args": (dict(ba.arguments) if ba else None),
        "ts_call_span": ts_span(src, call),
    }
```

---

## D3) Correctness backstops: “bytecode says FAST but symtable doesn’t” as a compact report for agents

This is essentially a **token-cheap** wrapper over A5:

````python
def correctness_backstops_report(src: bytes, cursor: Point) -> str:
    viols = validate_sym_dis_alignment(src, cursor)
    lines = ["```text", f"violations={len(viols)}"]
    for v in viols[:30]:
        loc = f"@{v.instr_offset} {v.instr_baseop}"
        at = f" | {v.ts_attribution.node_type} {v.ts_attribution.text}" if v.ts_attribution else ""
        lines.append(f"- {v.kind} {loc} name={v.name!r}: {v.message}{at}")
    lines.append("```")
    return "\n".join(lines)
````

---

## Quick mapping back to your bullet list (what this answer newly covered)

Newly covered with code (not already shown earlier):

* **Scope graph + meta-scopes + properties** (`SymbolTableType.*`, `is_nested`, `is_optimized`) ✅ ([Python documentation][1])
* **Function partitions** (`get_parameters/get_locals/get_globals/get_nonlocals/get_frees`) ✅ ([Python documentation][1])
* **Namespace binding** (`is_namespace + get_namespaces/get_namespace`) ✅ ([Python documentation][1])
* **Universal resolver (`resolve_binding`)** ✅ ([Python documentation][1])
* **Correctness oracle/invariants** with **CST attribution** ✅ ([Python documentation][2])
* **Normalized instruction facts** (`baseopname`, `cache_info`, `positions`) ✅ ([Python documentation][2])
* **Exception table semantics** (`co_exceptiontable`, CPython internal parsing) ✅ ([GitHub][3])
* **Span anchoring metrics/sanity checks** ✅ ([Python documentation][2])
* **Object inventory** (inspect classification, static resolution) ✅ ([Python documentation][4])
* **Signature binding simulation** (bind_partial/apply_defaults) ✅ ([Python documentation][4])
* **Runtime-state joins** (FrameInfo.positions / generator/coro states) ✅ ([Python documentation][4])
* **Compound joins** (end-to-end binding id; callsite correctness under decorators; correctness backstops) ✅

If you want the next increment: I can wrap these into a **`details.kind` registry** (`sym.scope_graph`, `sym.partitions`, `dis.exception_regions`, `inspect.callsite_bind_check`, `runtime.trace_attribution`, etc.) that maps cleanly onto your `Finding/Section` model so CQ can selectively materialize them behind “expand” commands.

[1]: https://docs.python.org/3/library/symtable.html "symtable — Access to the compiler’s symbol tables — Python 3.14.3 documentation"
[2]: https://docs.python.org/3/library/dis.html "dis — Disassembler for Python bytecode — Python 3.14.3 documentation"
[3]: https://raw.githubusercontent.com/python/cpython/main/Objects/codeobject.c "raw.githubusercontent.com"
[4]: https://docs.python.org/3/library/inspect.html "inspect — Inspect live objects — Python 3.14.3 documentation"

Here’s a **`details.kind` registry** that drops cleanly into your existing CQ schema:

* `Finding.details.kind` is the **kind string** (already supported by `DetailPayload.kind`).
* `Finding.details.data` carries:

  * a **compact preview** (always materialized),
  * an **expand handle** (minimal rehydration inputs),
  * optional **artifact ref** (when you choose to externalize heavy payloads).

This directly leverages:

* tree-sitter’s anchor selection (`named_descendant_for_point_range`) + `Node.id` stability within/ across incremental parses. ([tree-sitter.github.io][1])
* symtable’s compiler-truth scope system (incl. annotation/type-parameter scope flavors). ([Python documentation][2])
* dis’s `Instruction.positions`/`cache_info`/`baseopname` (useful for attribution + normalized facts). ([Python documentation][3])
* inspect’s static-safe reflection (`getmembers_static`, `getattr_static`) + callable introspection (`signature(follow_wrapped=...)`) + runtime state attribution (`FrameInfo.positions`). ([Python documentation][4])

---

## 1) Registry module: `tools/cq/core/details_kinds.py`

```python
# tools/cq/core/details_kinds.py
from __future__ import annotations

from typing import Literal

import msgspec

from tools.cq.core.structs import CqStruct
from tools.cq.core.schema import Anchor, Artifact, DetailPayload, Finding, Section
from tools.cq.core.scoring import build_detail_payload


# -----------------------------
# Conventions / protocol keys
# -----------------------------
# DetailPayload.kind == one of these stable strings.
#
# DetailPayload.data envelope:
# {
#   "preview": { ...small, always... },
#   "expand": {
#       "kind": "<details.kind>",
#       "handle": { ...minimal rehydration inputs... },
#       "cmd": "cq expand --kind <kind> --file ... --line ... --col ... [--module ...]",
#   },
#   "artifact": { "path": "...", "format": "json" }   # optional
# }

Materialization = Literal["inline", "artifact", "hybrid"]


class HandleField(CqStruct, frozen=True):
    """Single key in expand.handle."""
    key: str
    required: bool = True
    desc: str = ""


class KindSpec(CqStruct, frozen=True):
    """Registry spec for one details.kind."""
    # identity
    kind: str
    version: int = 1  # bump when payload schema changes
    rank: int = 1000  # stable ordering: never renumber existing; only append

    # mapping -> Finding/Section defaults
    category: str = "enrichment"
    section_title: str = "Enrichment"
    section_collapsed: bool = True

    # materialization policy
    materialization: Materialization = "hybrid"
    artifact_format: str = "json"

    # expand protocol
    expand_macro: str = "expand"
    handle_fields: tuple[HandleField, ...] = msgspec.field(default_factory=tuple)

    # preview contract (keys expected inside data["preview"])
    preview_keys: tuple[str, ...] = msgspec.field(default_factory=tuple)


def mk_expand_cmd(kind: str, handle: dict[str, object], *, macro: str = "expand") -> str:
    # Minimal generic command. Expand macro can accept extra flags if handle contains them.
    # Keep this string stable and tool-friendly.
    file = handle.get("file")
    line = handle.get("line")
    col = handle.get("col")
    module = handle.get("module")
    base = f"cq {macro} --kind {kind} --file {file} --line {line}"
    if col is not None:
        base += f" --col {col}"
    if module:
        base += f" --module {module}"
    return base


def build_enrichment_finding(
    *,
    spec: KindSpec,
    message: str,
    anchor: Anchor | None,
    preview: dict[str, object],
    handle: dict[str, object],
    severity: Literal["info", "warning", "error"] = "info",
    scoring: dict[str, object] | None = None,
    artifact: Artifact | None = None,
) -> Finding:
    """
    Create a Finding that:
      - stores only preview + expand handle by default
      - optionally points to an artifact for full payload
    """
    data: dict[str, object] = {
        "preview": preview,
        "expand": {
            "kind": spec.kind,
            "handle": handle,
            "cmd": mk_expand_cmd(spec.kind, handle, macro=spec.expand_macro),
        },
        "kind_version": spec.version,
    }
    if artifact is not None:
        data["artifact"] = {"path": artifact.path, "format": artifact.format}

    details = build_detail_payload(kind=spec.kind, data=data, scoring=scoring)

    return Finding(
        category=spec.category,
        message=message,
        anchor=anchor,
        severity=severity,
        details=details,
    )


# -----------------------------------------
# Canonical kind names (stable namespace)
# -----------------------------------------
# Prefixes:
#   sym.*      compiler-truth scoping & binding
#   dis.*      bytecode substrate / attribution / CFG/DFG
#   inspect.*  runtime reflection (opt-in)
#   runtime.*  live execution state (frames/tracebacks/generators)
#
# NOTE: Keep strings stable; do not rename. Append new kinds only.

DETAILS_KIND_REGISTRY: dict[str, KindSpec] = {
    # ---- symtable plane (scope truth) ----
    "sym.scope_graph": KindSpec(
        kind="sym.scope_graph",
        rank=100,
        category="scope",
        section_title="Enrichment: Symtable",
        section_collapsed=True,
        materialization="artifact",
        handle_fields=(
            HandleField("file", True, "Repo-relative file path"),
            HandleField("module", False, "Optional module name for later inspect joins"),
        ),
        preview_keys=("tables_count", "interesting_scopes", "has_type_scopes"),
    ),
    "sym.partitions": KindSpec(
        kind="sym.partitions",
        rank=110,
        category="scope",
        section_title="Enrichment: Symtable",
        materialization="hybrid",
        handle_fields=(
            HandleField("file", True, ""),
            HandleField("line", True, "1-based line of enclosing function/class"),
            HandleField("col", False, "0-based column"),
        ),
        preview_keys=("parameters", "locals_n", "globals_n", "nonlocals", "frees"),
    ),
    "sym.namespace_edges": KindSpec(
        kind="sym.namespace_edges",
        rank=120,
        category="scope",
        section_title="Enrichment: Symtable",
        materialization="artifact",
        handle_fields=(HandleField("file", True, ""),),
        preview_keys=("edges_n", "def_names"),
    ),
    "sym.binding_resolve": KindSpec(
        kind="sym.binding_resolve",
        rank=130,
        category="binding",
        section_title="Enrichment: Symtable",
        materialization="inline",
        handle_fields=(
            HandleField("file", True, ""),
            HandleField("line", True, ""),
            HandleField("col", True, ""),
        ),
        preview_keys=("name", "binding_id", "resolution", "flags"),
    ),
    "sym.invariants": KindSpec(
        kind="sym.invariants",
        rank=140,
        category="correctness",
        section_title="Enrichment: Symtable × dis",
        materialization="hybrid",
        handle_fields=(
            HandleField("file", True, ""),
            HandleField("line", True, "1-based line inside function"),
            HandleField("col", True, "0-based column inside function"),
        ),
        preview_keys=("violations_n", "kinds"),
    ),

    # ---- dis plane (bytecode substrate) ----
    "dis.instruction_facts": KindSpec(
        kind="dis.instruction_facts",
        rank=200,
        category="bytecode",
        section_title="Enrichment: dis",
        materialization="hybrid",
        handle_fields=(
            HandleField("file", True, ""),
            HandleField("line", True, "1-based line inside function"),
            HandleField("col", False, ""),
        ),
        preview_keys=("instruction_count", "has_positions", "has_cache_info", "top_baseopname"),
    ),
    "dis.anchor_metrics": KindSpec(
        kind="dis.anchor_metrics",
        rank=210,
        category="bytecode",
        section_title="Enrichment: dis",
        materialization="inline",
        handle_fields=(HandleField("file", True, ""), HandleField("line", True, ""), HandleField("col", False, "")),
        preview_keys=("positions_coverage", "ts_anchor_coverage", "unanchored_offsets_sample"),
    ),
    "dis.exception_regions": KindSpec(
        kind="dis.exception_regions",
        rank=220,
        category="exception",
        section_title="Enrichment: dis (exceptions)",
        materialization="artifact",
        handle_fields=(HandleField("file", True, ""), HandleField("line", True, ""), HandleField("col", False, "")),
        preview_keys=("regions_n", "handlers_n", "uses_private_parser"),
    ),
    "dis.cfg": KindSpec(
        kind="dis.cfg",
        rank=230,
        category="cfg",
        section_title="Enrichment: dis (CFG/DFG)",
        materialization="artifact",
        handle_fields=(HandleField("file", True, ""), HandleField("line", True, ""), HandleField("col", False, "")),
        preview_keys=("nodes_n", "edges_n", "exc_edges_n"),
    ),
    "dis.defuse_dfg": KindSpec(
        kind="dis.defuse_dfg",
        rank=240,
        category="dfg",
        section_title="Enrichment: dis (CFG/DFG)",
        materialization="artifact",
        handle_fields=(HandleField("file", True, ""), HandleField("line", True, ""), HandleField("col", False, "")),
        preview_keys=("events_n", "dfg_edges_n", "slot_kinds"),
    ),

    # ---- inspect plane (runtime reflection; opt-in) ----
    "inspect.object_inventory": KindSpec(
        kind="inspect.object_inventory",
        rank=300,
        category="runtime",
        section_title="Enrichment: inspect",
        materialization="artifact",
        handle_fields=(HandleField("module", True, "Importable module name"), HandleField("file", False, "")),
        preview_keys=("objects_n", "unresolved_n", "kinds"),
    ),
    "inspect.members_static": KindSpec(
        kind="inspect.members_static",
        rank=310,
        category="runtime",
        section_title="Enrichment: inspect",
        materialization="artifact",
        handle_fields=(
            HandleField("module", True, ""),
            HandleField("dotted", True, "Dotted expr to resolve statically (class/module)"),
        ),
        preview_keys=("members_n", "descriptor_kinds"),
    ),
    "inspect.wrapper_chain": KindSpec(
        kind="inspect.wrapper_chain",
        rank=320,
        category="runtime",
        section_title="Enrichment: inspect",
        materialization="hybrid",
        handle_fields=(HandleField("module", True, ""), HandleField("dotted", True, "")),
        preview_keys=("unwrap_chain", "unwrapped_qualname", "stops_on_signature"),
    ),
    "inspect.callsite_bind_check": KindSpec(
        kind="inspect.callsite_bind_check",
        rank=330,
        category="call",
        section_title="Enrichment: inspect (callsite)",
        materialization="hybrid",
        handle_fields=(
            HandleField("module", True, ""),
            HandleField("file", True, ""),
            HandleField("line", True, ""),
            HandleField("col", True, ""),
        ),
        preview_keys=("callee", "sig_follow_wrapped", "bind_ok", "bind_msg"),
    ),
    "inspect.annotations": KindSpec(
        kind="inspect.annotations",
        rank=340,
        category="types",
        section_title="Enrichment: inspect (types)",
        materialization="hybrid",
        handle_fields=(HandleField("module", True, ""), HandleField("dotted", True, "")),
        preview_keys=("annotations_keys", "count"),
    ),

    # ---- runtime state plane (frames/tracebacks/generators) ----
    "runtime.trace_attribution": KindSpec(
        kind="runtime.trace_attribution",
        rank=400,
        category="debug",
        section_title="Enrichment: runtime state",
        materialization="artifact",
        handle_fields=(
            HandleField("file", True, ""),
            HandleField("exception_type", False, "For display only"),
        ),
        preview_keys=("frames_n", "has_positions", "files"),
    ),
    "runtime.suspended_state": KindSpec(
        kind="runtime.suspended_state",
        rank=410,
        category="debug",
        section_title="Enrichment: runtime state",
        materialization="hybrid",
        handle_fields=(HandleField("file", True, ""), HandleField("object_id", True, "Opaque generator/coro id in your runtime harness")),
        preview_keys=("state", "ts_span"),
    ),
}
```

---

## 2) How CQ uses it: “preview now, expand later” (Finding/Section mapping)

This is the recommended wiring pattern:

* **In “search/calls/entity”**: emit *only* preview + expand handle.
* **In `cq expand` macro**: dispatch by `details.kind` to compute full payload and either:

  * return a new `CqResult` with a single Section, **or**
  * write an artifact and return an `Artifact` reference.

Tree-sitter gives you stable anchors from a cursor with `named_descendant_for_point_range`, and `Node.id` can be used as an internal cache key across incremental parses. ([tree-sitter.github.io][1])

Symtable provides compiler-truth scope graphs and annotation/type-parameter scope flavors (`ANNOTATION`, `TYPE_ALIAS`, `TYPE_PARAMETERS`, `TYPE_VARIABLE`). ([Python documentation][2])
dis provides instruction attribution via `Instruction.positions` and normalized fields like `baseopname` and `cache_info`. ([Python documentation][3])
inspect provides safe static reflection (`getmembers_static`, `getattr_static`), callable introspection (`signature(..., follow_wrapped=...)`), and runtime frame attribution (`FrameInfo.positions`). ([Python documentation][4])

---

## 3) Generic expand dispatcher skeleton: `tools/cq/macros/expand.py`

This shows the *protocol* (registry-driven), without forcing you into one compute strategy.

```python
# tools/cq/macros/expand.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from tools.cq.core.schema import Anchor, CqResult, Finding, Section, mk_result, ms
from tools.cq.core.scoring import build_detail_payload
from tools.cq.core.details_kinds import DETAILS_KIND_REGISTRY, KindSpec

# Your existing helpers (tree-sitter/symtable/dis/inspect implementations):
# - expand_sym_scope_graph(...)
# - expand_sym_partitions(...)
# - expand_dis_exception_regions(...)
# - expand_inspect_callsite_bind_check(...)
# - expand_runtime_trace_attribution(...)
# etc.

ExpandFn = Callable[[dict[str, object]], dict[str, object]]


@dataclass(frozen=True)
class ExpandRequest:
    root: Path
    argv: list[str]
    kind: str
    handle: dict[str, object]   # parsed from CLI flags into dict


EXPANDERS: dict[str, ExpandFn] = {
    # sym.*
    "sym.scope_graph": expand_sym_scope_graph,
    "sym.partitions": expand_sym_partitions,
    "sym.namespace_edges": expand_sym_namespace_edges,
    "sym.binding_resolve": expand_sym_binding_resolve,
    "sym.invariants": expand_sym_invariants,

    # dis.*
    "dis.instruction_facts": expand_dis_instruction_facts,
    "dis.anchor_metrics": expand_dis_anchor_metrics,
    "dis.exception_regions": expand_dis_exception_regions,
    "dis.cfg": expand_dis_cfg,
    "dis.defuse_dfg": expand_dis_defuse_dfg,

    # inspect.*
    "inspect.object_inventory": expand_inspect_object_inventory,
    "inspect.members_static": expand_inspect_members_static,
    "inspect.wrapper_chain": expand_inspect_wrapper_chain,
    "inspect.callsite_bind_check": expand_inspect_callsite_bind_check,
    "inspect.annotations": expand_inspect_annotations,

    # runtime.*
    "runtime.trace_attribution": expand_runtime_trace_attribution,
    "runtime.suspended_state": expand_runtime_suspended_state,
}


def cmd_expand(req: ExpandRequest) -> CqResult:
    started = ms()
    run = req  # plumb into your existing RunMeta builder
    result = mk_result(run=req.to_runmeta("expand"))  # assume you have to_runmeta on context; adapt as needed

    spec: KindSpec | None = DETAILS_KIND_REGISTRY.get(req.kind)
    if spec is None:
        result.key_findings.append(
            Finding(
                category="error",
                message=f"Unknown kind: {req.kind}",
                severity="error",
                details=build_detail_payload(kind="expand.error", data={"kind": req.kind}),
            )
        )
        return result

    expander = EXPANDERS.get(req.kind)
    if expander is None:
        result.key_findings.append(
            Finding(
                category="error",
                message=f"No expander registered for kind: {req.kind}",
                severity="error",
                details=build_detail_payload(kind="expand.error", data={"kind": req.kind}),
            )
        )
        return result

    payload = expander(req.handle)

    section = Section(title=f"Expanded: {req.kind}", collapsed=False)
    section.findings.append(
        Finding(
            category=spec.category,
            message=f"{req.kind} (expanded)",
            anchor=_anchor_from_handle(req.handle),
            severity="info",
            details=build_detail_payload(kind=req.kind, data={"payload": payload, "handle": req.handle}),
        )
    )
    result.sections.append(section)

    return result


def _anchor_from_handle(handle: dict[str, object]) -> Anchor | None:
    file = handle.get("file")
    line = handle.get("line")
    col = handle.get("col")
    if isinstance(file, str) and isinstance(line, int):
        return Anchor(file=file, line=line, col=(col if isinstance(col, int) else None))
    return None
```

---

## 4) “What to store in preview vs expand payload” (pragmatic defaults)

A good “LLM-agent friendly” split:

* **Preview (inline, always):**

  * counts, small lists (top N), one-liners, and an `expand.cmd`
* **Expand payload (artifact or expanded result):**

  * full scope graph, full CFG/DFG edges, full member lists, full traceback frame list with spans

This is especially valuable because:

* symtable scopes are compiler-derived just before bytecode. ([Python documentation][2])
* dis provides per-instruction source spans via `positions`, enabling precise CST attribution. ([Python documentation][3])
* inspect supports static-safe member access (`getattr_static`, `getmembers_static`) and signature unwrapping control (`follow_wrapped`). ([Python documentation][4])

---


[1]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Node.html "Node — py-tree-sitter 0.25.2 documentation"
[2]: https://docs.python.org/3/library/symtable.html "symtable — Access to the compiler’s symbol tables — Python 3.14.3 documentation"
[3]: https://docs.python.org/3/library/dis.html "dis — Disassembler for Python bytecode — Python 3.14.3 documentation"
[4]: https://docs.python.org/3/library/inspect.html "inspect — Inspect live objects — Python 3.14.3 documentation"

