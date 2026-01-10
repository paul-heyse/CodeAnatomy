
For an **LLM coding agent**, the “extra stuff CPython produces” (vs LibCST/tree-sitter) is mostly valuable **only insofar as it gives you higher-certainty *semantic facts***—not because the raw artifacts themselves are inherently more “understandable” to the model.

### Net value of the CPython-only outputs for code understanding

#### 1) **`ctx` (Load/Store/Del)**

**Incremental value:** *low* (but “clean correctness” value is real).
An agent can usually infer read vs write from syntax. The practical benefit of CPython’s explicit `ctx` is that it’s a **canonical, non-heuristic label** you can use as a truth source when generating def-use edges (“this is a write”, “this is a delete”) and when scoring confidence.

#### 2) **`type_comment` / `Module.type_ignores`**

**Incremental value:** *medium* for agent workflows that touch types.
The model benefits when you can say, precisely: “this assignment had a type comment”, or “this line is suppressed with `# type: ignore`.” Those are strong signals about **developer intent** and “known rough edges.” CPython elevates them into structured channels in the AST; LibCST/tree-sitter preserve them as comments but you’d have to associate them yourself.

#### 3) **Optimized AST (`PyCF_OPTIMIZED_AST`)**

**Incremental value:** *usually low* for understanding, *sometimes medium* for “what actually ships.”
Optimization levels matter for a few semantics: `optimize=1` removes `assert`s; `optimize=2` removes docstrings too. ([Python documentation][1])
Python 3.13 added `ast.PyCF_OPTIMIZED_AST` so you can get an AST that matches those compile-time eliminations. ([Python documentation][2])
For an LLM, this is mostly helpful if you’re doing production-vs-dev reasoning (“why is this check gone in prod?”), otherwise it’s rarely decisive.

#### 4) **Code objects + bytecode (compile output)**

**Incremental value:** *high potential, but only after you distill it.*
This is the big one: CPython can give you **executable compilation artifacts** (code objects / bytecode), which LibCST/tree-sitter fundamentally don’t produce. ([Python documentation][3])
Raw bytecode is *not* something you should dump into an LLM context window—it’s noisy and version-specific (even the `dis` docs warn bytecode is a CPython implementation detail and may change across releases). ([Python documentation][4])
But if you turn it into *structured facts*, it unlocks higher-confidence understanding of:

* **scope / closure capture** (what is local vs free vs cell),
* **function “shape”** (generator/coroutine/async generator flags, varargs/kwargs, docstring presence),
* **control flow** (basic blocks, jump edges, eliminated lines),
* **source↔execution mapping** (per-instruction source positions).

This is where CPython stops being “just another parser” and becomes an execution-model oracle.

---

## What analysis/tooling unlocks the value of CPython compile output?

### A) **Scope graph / name binding (huge leverage)**

Use `symtable` (stdlib) to extract the compiler’s scope decisions. Symbol tables are produced “just before bytecode is generated” and are responsible for “calculating the scope of every identifier.” ([Python documentation][5])
This gives you **authoritative bindings** that are painful to reproduce perfectly from syntax alone (especially around comprehensions, `global`/`nonlocal`, annotation scopes, etc.).

**LLM-facing payload ideas (per scope):**

* locals / globals / freevars / cellvars
* declared `global` / `nonlocal`
* “reads before writes” candidates (helpful bug-finding signal)
* closure capture summary (“inner fn captures: x, y”)

### B) **Code object tree extraction (structure without noise)**

Compile modules/functions and traverse nested code objects via `co_consts` (nested functions/lambdas/comprehensions show up as nested code objects). The language reference documents code object attributes like `co_consts`, `co_names`, `co_firstlineno`, `co_flags`, etc. ([Python documentation][6])
Then expose a compact “function capsule” to the LLM:

* flags: generator/coroutine/etc (inspect exposes the meaning of key flags, incl. new ones in recent versions) ([Python documentation][7])
* captured closure vars (from `co_freevars` / `co_cellvars`)
* constant strings used (often reveals config keys, error messages, SQL, etc.)

### C) **Bytecode → CFG (control-flow graph) + “eliminated code” detection**

Use `dis.get_instructions()` to get a stable(ish) instruction stream for your pinned CPython version. ([Python documentation][4])
Then build:

* **basic blocks** (leaders at jump targets + fallthrough boundaries),
* **edges** (conditional/unconditional jumps, fallthrough),
* optionally exception edges (version-dependent).

Why this matters to an LLM:

* you can summarize “this function has 7 blocks, 2 loops, 3 exits”
* you can compute **cyclomatic complexity** and “hot spots”
* you can spot **unreachable lines** / compiler-eliminated code. `co_lines()` explicitly allows *zero-width ranges* for lines “eliminated by the bytecode compiler.” ([Python documentation][6])

### D) **Precise source-position mapping for execution**

For many “explain this behavior / find the right span / patch safely” tasks, mapping semantics to exact spans is gold.

CPython provides:

* `codeobject.co_positions()` → per-instruction `(start_line, end_line, start_column, end_column)` with 0-indexed UTF-8 byte offsets. ([Python documentation][6])
  This gives you a robust way to connect compiled behavior back to source spans (and to detect when debug ranges are missing).

### E) **Def-use / effect summaries (the LLM-friendly payoff)**

Once you have (scope graph + CFG), you can compute the kinds of summaries that actually help an agent act safely:

* **Reads/Writes sets** (locals, nonlocals, globals)
* **“Mutates global/module state”** flag
* **Potentially-raising operations** clusters (coarse but helpful)
* **Callsite shapes** (direct global call vs attribute call vs dynamic callable)

These are the “code understanding” outputs an LLM benefits from—short, structural, and confidence-scored.

### F) Combine with *external* semantic resolvers for the last mile

CPython compile output still won’t tell you:

* what `obj.method()` resolves to,
* what types flow through a variable,
* what an import ultimately binds to.

That’s where you unlock the *full* value by pairing CPython’s ground-truth binding/control-flow with:

* **type checkers** (Pyright/mypy/pyre) for type + symbol resolution,
* **SCIP** (if you already have it) for cross-file symbol linking,
* optional **runtime traces** (coverage / profiling / tracing) for dynamic call target confirmation.

---

## Bottom line

If your goal is **LLM code understanding**, CPython’s “extra outputs” are not magic on their own. The net value comes from using them as **authoritative primitives** to produce **small, high-signal metadata**:

* **symtable → scope graph** (biggest bang-for-buck) ([Python documentation][5])
* **code objects + `dis` → CFG + execution↔source alignment** (high leverage, but version-pinned) ([Python documentation][4])
* **type_comment/type_ignores → intent/suppression channels** (medium, but useful) ([Python documentation][3])

If you tell me which agent tasks you’re optimizing (refactor planning, bug-finding, type fixes, blast-radius, etc.), I can recommend a minimal “CPython-derived fact surface” (a handful of JSON fields) that tends to give the best lift per byte/token.

[1]: https://docs.python.org/3/library/functions.html "Built-in Functions — Python 3.14.2 documentation"
[2]: https://docs.python.org/3/whatsnew/3.13.html "What’s New In Python 3.13 — Python 3.14.2 documentation"
[3]: https://docs.python.org/3/library/ast.html "ast — Abstract syntax trees — Python 3.14.2 documentation"
[4]: https://docs.python.org/3/library/dis.html "dis — Disassembler for Python bytecode — Python 3.14.2 documentation"
[5]: https://docs.python.org/3/library/symtable.html "symtable — Access to the compiler’s symbol tables — Python 3.14.2 documentation"
[6]: https://docs.python.org/3/reference/datamodel.html "3. Data model — Python 3.14.2 documentation"
[7]: https://docs.python.org/3/library/inspect.html "inspect — Inspect live objects — Python 3.14.2 documentation"


You can think of CPython “compiled output” as two layers:

1. **Code object header fields** (`co_*`) → already *structured* and relatively stable
2. **Bytecode stream** (`co_code` / `dis.Instruction`) → structured *per-instruction*, but the *instruction set + calling convention* shifts across Python versions (3.11+ especially), so you normalize it into “events” and “summaries” rather than storing raw opcodes.

The good news: there *are* dedicated fields for a lot of the high-value facts (names, locals, free/cell vars, flags, constants, source mapping). The variable part is mostly the opcode inventory and stack choreography. CPython exposes a stable-ish structured view over that via `dis.get_instructions()` / `dis.Bytecode()` which yields `Instruction` objects with fields like `opname`, `argval`, `offset`, `positions`, `jump_target` (newer), and cache metadata (newer).

Below is a concrete “compiled output → valuable facts” pattern you can scale.

---

## What “structured facts” you can get without heroics

### A) From **code object fields** (dedicated)

These are the “cheap, high-signal” facts you should always extract:

* **Signature shape:** `co_argcount`, `co_posonlyargcount`, `co_kwonlyargcount`
* **Locals & identifiers:** `co_varnames`, `co_names`
* **Closure binding:** `co_freevars`, `co_cellvars`
* **Constants:** `co_consts` (including *nested code objects* for inner funcs/lambdas/comprehensions)
* **Provenance & mapping:** `co_filename`, `co_firstlineno`, plus `co_lines()` and `co_positions()` for bytecode↔source mapping
* **Semantics flags:** `co_flags` (generator/coroutine/etc)

Those alone let you produce very useful facts like:

* “`outer` creates closure over `y,z`”
* “function is coroutine/generator”
* “constants include these error strings/config keys”
* “nested code objects: `outer.<locals>.inner`, `<listcomp>`, …”

### B) From **symbol tables** (compiler product, still cheap)

`symtable` gives authoritative scope classification (“local/global/free/cell/parameter/imported”). It’s generated by the compiler just before bytecode and “calculates the scope of every identifier”. ([Python documentation][1])
This is *the* easiest way to turn compilation into binding facts at scale.

### C) From **bytecode** (variable, but normalizable)

Use `dis.get_instructions(co)` and convert instruction sequences into *events*:

* **imports:** `IMPORT_NAME`, `IMPORT_FROM`
* **calls:** `CALL` (+ preceding loads)
* **reads/writes:** `LOAD_*` / `STORE_*`
* **attribute access:** `LOAD_ATTR` / `LOAD_METHOD`
* **control flow:** `*_JUMP_*`, `FOR_ITER`, etc.
* **source spans:** `instr.positions` (3.11+)

Don’t parse `dis.code_info()` strings—they’re explicitly “highly implementation dependent” and can change arbitrarily.

---

## Showcase: compiled output → facts (stdlib-only)

This is intentionally **lossy-but-useful**: it extracts stable facts and emits a small set of high-value events instead of dumping every opcode.

```python
from __future__ import annotations

import dis
import inspect
import symtable
import types
from dataclasses import dataclass, asdict
from typing import Any, Optional

# ----------------------------
# Helpers: stable-ish decoding
# ----------------------------

def decode_co_flags(flags: int) -> list[str]:
    """Turn co_flags bitmask into CO_* names (stdlib inspect defines them)."""
    out: list[str] = []
    for name in dir(inspect):
        if name.startswith("CO_"):
            val = getattr(inspect, name)
            if isinstance(val, int) and (flags & val):
                out.append(name)
    return sorted(set(out))

def summarize_const(c: Any) -> dict[str, Any]:
    if isinstance(c, types.CodeType):
        return {"kind": "code", "qualname": getattr(c, "co_qualname", c.co_name), "firstlineno": c.co_firstlineno}
    if isinstance(c, str):
        return {"kind": "str", "value": c if len(c) <= 80 else (c[:77] + "...")}
    if isinstance(c, (int, float, bool)) or c is None:
        return {"kind": type(c).__name__, "value": c}
    if isinstance(c, bytes):
        return {"kind": "bytes", "len": len(c)}
    if isinstance(c, tuple):
        return {"kind": "tuple", "len": len(c)}
    return {"kind": type(c).__name__}

# ----------------------------
# Bytecode → event extraction
# ----------------------------

@dataclass(frozen=True)
class Pos:
    lineno: Optional[int]
    end_lineno: Optional[int]
    col: Optional[int]
    end_col: Optional[int]

def pos_of(instr: dis.Instruction) -> Optional[Pos]:
    p = getattr(instr, "positions", None)
    if p is None:
        return None
    return Pos(p.lineno, p.end_lineno, p.col_offset, p.end_col_offset)

@dataclass(frozen=True)
class ImportEvent:
    module: str
    pos: Optional[Pos]

@dataclass(frozen=True)
class NameRead:
    name: str
    op: str
    pos: Optional[Pos]

@dataclass(frozen=True)
class NameWrite:
    name: str
    op: str
    pos: Optional[Pos]

@dataclass(frozen=True)
class AttrAccess:
    attr: str
    op: str
    pos: Optional[Pos]

@dataclass(frozen=True)
class CallEvent:
    callee_hint: str
    nargs: Optional[int]
    pos: Optional[Pos]

def extract_events(co: types.CodeType) -> dict[str, Any]:
    """
    Convert bytecode into a small set of stable-ish events.
    callee_hint is best-effort; for correctness you eventually build a proper stack model/CFG.
    """
    imports: list[ImportEvent] = []
    reads: list[NameRead] = []
    writes: list[NameWrite] = []
    attrs: list[AttrAccess] = []
    calls: list[CallEvent] = []
    opcode_counts: dict[str, int] = {}

    # Best-effort “callee hint”: track last loaded name/attr before CALL.
    last_callable: Optional[str] = None

    for ins in dis.get_instructions(co, adaptive=False):
        opcode_counts[ins.opname] = opcode_counts.get(ins.opname, 0) + 1
        p = pos_of(ins)

        if ins.opname in {"IMPORT_NAME"} and isinstance(ins.argval, str):
            imports.append(ImportEvent(module=ins.argval, pos=p))
            last_callable = None
            continue

        if ins.opname in {"LOAD_GLOBAL", "LOAD_NAME", "LOAD_FAST", "LOAD_DEREF"} and isinstance(ins.argval, str):
            reads.append(NameRead(name=ins.argval, op=ins.opname, pos=p))
            last_callable = ins.argval
            continue

        if ins.opname in {"STORE_GLOBAL", "STORE_NAME", "STORE_FAST", "STORE_DEREF"} and isinstance(ins.argval, str):
            writes.append(NameWrite(name=ins.argval, op=ins.opname, pos=p))
            last_callable = None
            continue

        if ins.opname in {"LOAD_ATTR", "LOAD_METHOD"} and isinstance(ins.argval, str):
            attrs.append(AttrAccess(attr=ins.argval, op=ins.opname, pos=p))
            # Turn "x" + ".y" into "x.y" if we can; otherwise just ".y"
            last_callable = f"{last_callable}.{ins.argval}" if last_callable else f".{ins.argval}"
            continue

        if ins.opname == "CALL":
            nargs = ins.arg if isinstance(ins.arg, int) else None
            calls.append(CallEvent(callee_hint=last_callable or "<?>", nargs=nargs, pos=p))
            last_callable = None
            continue

        # reset hint on unknown stack-affecting ops (conservative)
        if ins.opname in {"BINARY_OP", "BUILD_LIST", "BUILD_TUPLE", "BUILD_MAP", "CALL_FUNCTION_EX"}:
            last_callable = None

    return {
        "opcode_counts": opcode_counts,
        "imports": [asdict(x) for x in imports],
        "name_reads": [asdict(x) for x in reads],
        "name_writes": [asdict(x) for x in writes],
        "attr_accesses": [asdict(x) for x in attrs],
        "calls": [asdict(x) for x in calls],
    }

# ----------------------------
# Code object tree extraction
# ----------------------------

def walk_code_objects(root: types.CodeType) -> list[types.CodeType]:
    out: list[types.CodeType] = []
    stack = [root]
    while stack:
        co = stack.pop()
        out.append(co)
        for c in co.co_consts:
            if isinstance(c, types.CodeType):
                stack.append(c)
    return out

def code_object_facts(co: types.CodeType) -> dict[str, Any]:
    return {
        "qualname": getattr(co, "co_qualname", co.co_name),
        "name": co.co_name,
        "filename": co.co_filename,
        "firstlineno": co.co_firstlineno,
        "argcount": co.co_argcount,
        "posonlyargcount": getattr(co, "co_posonlyargcount", 0),
        "kwonlyargcount": getattr(co, "co_kwonlyargcount", 0),
        "nlocals": co.co_nlocals,
        "stacksize": co.co_stacksize,
        "flags": decode_co_flags(co.co_flags),
        "varnames": list(co.co_varnames),
        "names": list(co.co_names),
        "freevars": list(co.co_freevars),
        "cellvars": list(co.co_cellvars),
        "consts_summary": [summarize_const(c) for c in co.co_consts[:40]],
        "events": extract_events(co),
    }

# ----------------------------
# Compiler symbol table (scope facts)
# ----------------------------

def symtable_facts(source: str, filename: str) -> list[dict[str, Any]]:
    tbl = symtable.symtable(source, filename, "exec")

    rows: list[dict[str, Any]] = []
    def rec(t: symtable.SymbolTable, parent: Optional[str]) -> None:
        tid = f"{t.get_name()}@{t.get_lineno()}"
        syms = t.get_symbols()
        rows.append({
            "id": tid,
            "parent": parent,
            "type": t.get_type(),
            "name": t.get_name(),
            "lineno": t.get_lineno(),
            "locals": sorted(s.get_name() for s in syms if s.is_local()),
            "globals": sorted(s.get_name() for s in syms if s.is_global()),
            "free": sorted(s.get_name() for s in syms if s.is_free()),
            "cell": sorted(s.get_name() for s in syms if s.is_cell()),
            "parameters": sorted(s.get_name() for s in syms if s.is_parameter()),
            "imported": sorted(s.get_name() for s in syms if s.is_imported()),
        })
        for child in t.get_children():
            rec(child, tid)

    rec(tbl, None)
    return rows

# ----------------------------
# End-to-end: source → facts
# ----------------------------

def analyze_source(source: str, filename: str = "<mem>") -> dict[str, Any]:
    root = compile(source, filename, "exec", dont_inherit=True, optimize=0)
    return {
        "python": {"version": inspect.cleandoc(str(__import__("sys").version)), "impl": __import__("sys").implementation.name},
        "code_objects": [code_object_facts(co) for co in walk_code_objects(root)],
        "symtable_scopes": symtable_facts(source, filename),
    }
```

### What the emitted facts look like (example)

Given a module with a closure like:

```python
import os
def outer(y):
    z = 0
    def inner(a):
        nonlocal z
        z += a
        return os.path.join(y, str(z))
    return inner
```

You’ll get “structured facts” like:

* `outer` **cellvars** include `y` and `z` (because inner closes over them)
* `inner` **freevars** include `y` and `z`
* event stream includes:

  * import `os`
  * reads/writes of `z`
  * attribute chain hint `os.path.join`
  * call events with `nargs`

Those closure/binding facts are coming straight from `co_freevars/co_cellvars` and the compiler’s symbol tables—not heuristics. ([Python documentation][1])

---

## Are the fields “dedicated” or “highly variable”?

### Dedicated / stable(ish)

The **`co_*` header attributes** and methods are dedicated and are exactly what you want to key your schema on: locals/names/constants/freevars/cellvars/flags/source mapping.

### Variable

The **bytecode instruction set** and calling convention *is* version-sensitive (3.11+ added inline caches and adaptive/specialized behavior; instruction inventory evolves). CPython exposes a structured interface (`Instruction`) but even that gains fields over versions (e.g., extra cache metadata and jump-target convenience fields in newer releases).

**Implication for your schema:** make instruction-derived fields *optional*, and store **normalized events** rather than raw instruction streams.

---

## What you need to do to make this scalable

### 1) Pin the compiler reality

* Record: `{python_version, implementation, optimize_level, flags}` alongside facts.
* Ideally run analysis under **one pinned CPython version** in CI/infra to avoid “opcode drift”.

### 2) Treat compilation as an untrusted workload

Even though `compile()` doesn’t execute code, it can be expensive on pathological inputs.

* Run compilation in a **worker process** with CPU/memory limits (and kill on timeout).
* Always emit a parse/compile error record (never crash the pipeline).

### 3) Emit a small, stable “fact surface”

Recommended minimal tables/datasets (LLM-friendly + scalable):

* `code_object` (1 row per code object): header fields + `consts_summary`
* `code_object_contains` edges (parent→child from nested code objects)
* `sym_scope` (1 row per symtable scope): locals/globals/free/cell/params/imported
* `bytecode_events` (sparse): import/call/read/write/attr/jump with source positions

### 4) Upgrade path to “real” value: add CFG + better call reconstruction (optional)

The simple `callee_hint` trick above is okay for metadata, but if you want high-confidence call graphs:

* build basic blocks (CFG) from jumps
* do a small stack interpreter per block (or use `dis.stack_effect` to guide conservative modeling)
* only emit call targets when the stack model is confident; otherwise mark unknown and keep the positional span.

---

If you tell me your target “facts” (e.g., **call graph edges**, **global mutation detection**, **closure capture**, **import graph**, **CFG complexity**), I can propose a tight **canonical schema** (DuckDB tables + JSON payload shapes) and a version-pinned extractor plan that stays stable across CPython upgrades.

[1]: https://docs.python.org/3/library/symtable.html?utm_source=chatgpt.com "symtable — Access to the compiler's symbol tables"

According to documents dated **January 2, 2026**, the practical way to “join” CPython compiled facts to a LibCST CST is: **normalize everything to the same coordinate system (file path + UTF-8 byte spans), then do interval containment/overlap joins**.

## 1) The join key you want: `(path, byte_start, byte_len)`

LibCST’s indexer-grade posture is to treat **spans as primary keys** and persist a **byte span** (`ByteSpanPositionProvider`) for canonical slicing; it emits `(start_offset_bytes, length_bytes)` and the offsets are **bytes (UTF-8), not characters**. It also recommends stable IDs keyed on `(path, byte_start, byte_len, kind)` rather than persisting CST node identities. 

That is exactly the coordinate system CPython uses for columns too:

* `ast` node `col_offset/end_col_offset` are **UTF-8 byte offsets**.
* Code objects expose `co_positions()` with columns described as **0-indexed UTF-8 byte offsets** on the line.

So the “lowest-friction” join is: **convert CPython’s (line, col_byte) positions → absolute byte offsets**, then match against LibCST’s byte spans.

## 2) What you actually store from LibCST to make joining cheap

Per file:

1. **Read bytes and parse as bytes** so your byte spans remain meaningful end-to-end (recommended for fidelity). 
2. Build a `MetadataWrapper` and resolve `ByteSpanPositionProvider` and/or `PositionProvider` as needed. 
3. **Never persist CST nodes**; persist spans + stable IDs (wrapper deep-copies; node identity is not stable). 

Operationally: build an interval index per file:

* `node_span_index`: intervals for the node kinds you care about (`Call`, `Name`, `Attribute`, `Import`, `FunctionDef`, `ClassDef`, etc.)
* optionally two variants: “semantic span” (`PositionProvider`) vs “patching span” (whitespace-inclusive) if you care about ownership, but for joining compiled facts **byte spans** are the best canonical axis. 

## 3) What you extract from CPython to join

You *don’t* want to store raw opcodes as the join artifact (bytecode is explicitly version-unstable).

Instead, you use CPython’s **already-structured** position outputs:

* `dis.get_instructions(codeobj)` yields `Instruction` objects; each has `.positions` (a `dis.Positions` record) with `lineno/end_lineno/col_offset/end_col_offset` (may be `None`).
* Code objects provide `co_positions()` (per code unit) and `co_lines()` (bytecode ranges → line numbers), and `co_positions()` columns are **0-indexed UTF-8 byte offsets**; positions can be missing (e.g., `-X no_debug_ranges`).

### Convert (line, col_byte) → absolute byte offset

For each file, precompute `line_start_byte_offsets[]` from the raw file bytes (scan for `\n`). Then:

* `abs_start = line_start_byte_offsets[lineno-1] + col_offset`
* `abs_end   = line_start_byte_offsets[end_lineno-1] + end_col_offset`

Handle `None` fields by:

* falling back to line-only joins using `co_lines()`/`Instruction.line_number`, or
* skipping those instructions (often fine; you mostly care about “semantic” instructions anyway).

## 4) The actual join: “compiled span → best CST node”

Now you can join each compiled fact/event to CST by interval logic:

* For a compiled span `[abs_start, abs_end)`, find **all CST nodes whose byte span intersects/contains it**.
* Pick a deterministic “best node”:

  * prefer **containment** over intersection,
  * prefer **smallest containing node** (most specific),
  * filter by expected node types per event (e.g., CALL → `cst.Call`, LOAD_ATTR → `cst.Attribute`, IMPORT_NAME → `cst.Import`/`cst.ImportFrom`).

This yields a stable attachment point: `anchor_cst_id = stable_id(path, node_byte_start, node_byte_len, kind)` (as recommended). 

### Two joins you’ll use in practice

**(A) Function capsule join (high value, low noise)**
Attach each **code object**’s semantic header facts (`co_freevars/co_cellvars/co_flags/co_consts summary`) to the corresponding `FunctionDef`/`Lambda`/comprehension CST node. Code objects expose the necessary header fields and `co_positions()`/`co_firstlineno`.

**(B) Sparse event join (selective, for deeper tooling)**
Attach only a curated event stream (imports, call events, name reads/writes, jumps) to CST anchors. This is where `Instruction.positions` helps you localize events without doing “full opcode dumps”.

## 5) Is “proper stack model / CFG” where SCIP comes in?

Not really.

* A **stack model / CFG** is *intra-procedural*: it tells you, inside a single code object, what value is on the stack at `CALL`, how branches merge, what’s reachable, etc. That’s bytecode/dataflow work (you might use `dis.stack_effect`, basic blocks, etc.), and it exists even if you have zero repo-wide symbol info.
* **SCIP** is *inter-procedural / cross-file symbol resolution*: once you have a callsite anchored to a CST span, SCIP helps you map the **callee expression** to a **symbol identity / definition(s)** across the repo (and then build a call graph with confidence).

So the division of labor is usually:

1. **CST (LibCST) gives you exact syntax + spans** (best anchor surface). 
2. **CPython compile gives you authoritative scope/closure + execution-shaped facts** (attach to CST anchors).
3. **SCIP (plus optional type-checker info)** resolves anchored names/calls to real symbols across the repo.

If you already have CST, you often don’t need bytecode stack reconstruction to identify the callee text; you use CST for “what the code says” and reserve bytecode CFG for “what the code does” (reachability, closure capture, eliminated lines, etc.). (And you still may keep the CFG to compute higher-signal summaries for the LLM.)

If you want, I can sketch the **exact minimal schemas** for the three joinable tables you need (CST nodes, compiled events, code object capsules) and the deterministic join rules so you can run it repo-wide and get stable diffs.
