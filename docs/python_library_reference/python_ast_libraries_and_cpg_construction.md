Below is a **feature inventory + semantic clustering** for Python **3.13**’s `dis`, `inspect`, and `symtable` stdlib modules, in the same “catalog-first” style as your PyArrow reference. 

---

## 0) Mental model across the three modules

Think of these as **three different introspection planes**:

* **`symtable`**: *compile-time scope analysis* (names → scope classification) emitted from AST right before bytecode generation. ([Python documentation][1])
* **`dis`**: *post-compile codegen lens* (code objects → CPython bytecode → per-instruction facts). Bytecode is CPython implementation detail and changes across versions. ([Python documentation][2])
* **`inspect`**: *runtime reflection lens* over live objects (callables/classes/frames/tracebacks/signatures/source). ([Python documentation][3])

---

## 0.5) Implementation status in CodeIntel (current repo)

### Completed
- [x] dis extraction tables for code units, instructions, exception table, blocks, CFG edges, and def/use events with span anchoring and label mapping.
- [x] symtable extraction tables for scopes, symbols, scope edges, namespace edges, function partitions, derived bindings, and resolution edges with AST anchors (module/function/class plus annotation/type-alias/type-parameter/type-variable confidence mapping).
- [x] inspect extraction tables for objects, members (static), unwrap hops, signatures, signature params, annotations, source, and runtime state (frame/traceback/generator/coroutine/asyncgen) using an allowlist + subprocess/budgeted execution.
- [x] CPG nodes for SCOPE, BINDING, BC_CODE_UNIT, BC_INSTR, CFG_BLOCK, INSPECT_OBJECT, INSPECT_SIGNATURE, and INSPECT_SIGNATURE_PARAM.
- [x] CPG edges for OWNS_SCOPE/PARENT_SCOPE, BINDS_DEF/BINDS_USE, DEFINES_BINDING/USES_BINDING, REACHES, CFG/DFG stack + memory edges, bytecode callsite edges (to syntax calls + SCIP symbols), HAS_SIGNATURE/HAS_PARAM, ARG_TO_PARAM_INSPECT, INSPECT_ANCHORS_AST, INSPECT_SYMBOL, WRAPS, class MRO/attrs, and runtime-state-to-instruction joins.
- [x] Validation check for bytecode def/use binding-space mismatches.
- [x] Run-level compiler metadata on bytecode code units (python version, magic number, optimize, dont_inherit).

### Remaining checklists

### DIS / bytecode -> CPG
- [ ] Expand fixtures to assert stack-effect DFG + callsite wiring on if/loop/try/with patterns (beyond minimal cases).

### INSPECT overlay
- [ ] Improve inspect-to-AST anchoring using source spans and confidence metadata (not only qualname joins).
- [ ] If needed, distinguish DECORATES vs WRAPS edges from unwrap-hop metadata.

### SYMTABLE
- [ ] Add tests for type-alias/type-parameter scopes and annotation-only bindings.

### Cross-cutting and validation
- [ ] Track anchor coverage metrics for inspect->AST joins (and extend coverage metrics beyond instruction span anchoring as needed).
- [ ] Expand micro-fixtures for nested try/except/finally/with, comprehensions, and decorators.

---

## A) `dis` — Disassembler for Python bytecode (3.13)

### A1) Command-line interface (stdlib tool surface)

* `python -m dis [-h] [-C] [-O] [infile]` ([Python documentation][2])

  * `-C/--show-caches`: show inline caches (documented as added in 3.13). ([Python documentation][2])
  * `-O/--show-offsets`: show instruction offsets (added in 3.13). ([Python documentation][2])

### A2) High-level “print disassembly” entry points

* `dis.dis(x=None, *, file=None, depth=None, show_caches=False, adaptive=False, show_offsets=...)`

  * Accepts: module/class/method/function/generator/async gen/coroutine/code object/source string/raw bytecode bytes; recursively disassembles nested code objects. ([Python documentation][2])
  * `depth`: recursion bound (`0` = no recursion). ([Python documentation][2])
  * 3.13 change: output uses **logical labels** for jump targets/exception handlers; `show_offsets` added (and `-O`). ([Python documentation][2])

* `dis.distb(tb=None, *, file=None, show_caches=False, adaptive=False, show_offsets=...)`: disassemble from traceback (points at the faulting instruction). ([Python documentation][2])

* `dis.disassemble(code, lasti=-1, *, file=None, show_caches=False, adaptive=False, show_offsets=...)` / alias `disco(...)`: code-object–oriented disassembly. ([Python documentation][2])

### A3) Bytecode analysis object model (`Bytecode`)

* `class dis.Bytecode(x, *, first_line=None, current_offset=None, show_caches=False, adaptive=False, show_offsets=False)` ([Python documentation][2])

  * Iterable: iterating yields `Instruction` instances. ([Python documentation][2])
  * `current_offset`: marks a “current instruction” position (useful for traceback-centric views). ([Python documentation][2])
  * Methods/attrs:

    * `Bytecode.dis()` → formatted multiline string (same content as `dis.dis(...)` output). ([Python documentation][2])
    * `Bytecode.info()` → formatted multiline “code object info” view (like `code_info`). ([Python documentation][2])
    * `Bytecode.from_traceback(tb, *, show_caches=False)` constructor. ([Python documentation][2])
    * `Bytecode.codeobj`, `Bytecode.first_line` accessors. ([Python documentation][2])

### A4) “Analysis functions” (one-shot helpers)

* `dis.code_info(x)` → formatted multiline code-object metadata. ([Python documentation][2])
* `dis.show_code(x, *, file=None)` → print `code_info(x)` to a file/`stdout`. ([Python documentation][2])

### A5) Instruction stream API (per-op facts)

* `dis.get_instructions(x, *, first_line=None, show_caches=False, adaptive=False)`

  * 3.13: `show_caches` is **deprecated/no-op**; caches are surfaced via `Instruction.cache_info` instead of separate `CACHE` pseudo-instructions in the iterator. ([Python documentation][2])

* `class dis.Instruction` (the canonical “structured fact row” for one opcode) exposes: ([Python documentation][2])

  * opcode identity: `opcode`, `opname`
  * specialization identity: `baseopcode`, `baseopname`
  * operand: `arg` / alias `oparg`, `argval`, `argrepr`
  * layout + cache span: `offset`, `start_offset`, `cache_offset`, `end_offset`
  * source mapping: `starts_line`, `line_number`, `positions: dis.Positions`
  * control flow: `is_jump_target`, `jump_target`
  * inline cache payload: `cache_info` = `(name, size, data)` triplets (or `None`). ([Python documentation][2])

* `class dis.Positions`: `(lineno, end_lineno, col_offset, end_col_offset)` (fields can be `None`). ([Python documentation][2])

### A6) Source/CFG-ish helpers

* `dis.findlinestarts(code)` → `(offset, lineno)` pairs from `code.co_lines()`; in 3.13 line numbers can be `None` for bytecode without source mapping. ([Python documentation][2])
* `dis.findlabels(code)` → list of jump-target offsets in raw bytecode. ([Python documentation][2])

### A7) Stack model primitive (CFG/DFG prerequisite)

* `dis.stack_effect(opcode, oparg=None, *, jump=None)`

  * `jump=True/False/None` selects jump vs fallthrough vs max-of-both. ([Python documentation][2])
  * 3.13: omitting `oparg` now behaves as `oparg=0` (and passing `oparg` for non-arg opcodes is no longer an error). ([Python documentation][2])

### A8) Opcode collections (programmatic classification tables)

These are the “fast-path tables” you use for analysis pipelines (e.g., `hasjump` → treat as control-flow edge): ([Python documentation][2])

* `dis.opname`: index → opname
* `dis.opmap`: opname → opcode
* `dis.cmp_op`: comparison opname list
* `dis.hasarg`: opcodes that use arg (added 3.12)
* `dis.hasconst`, `dis.hasfree`, `dis.hasname`, `dis.haslocal`, `dis.hascompare`
* **3.13**:

  * `dis.hasjump`: opcodes with a jump target; all jumps are relative (added 3.13). ([Python documentation][2])
  * `dis.hasjrel`: deprecated in 3.13 (“all jumps relative; use hasjump”). ([Python documentation][2])
* `dis.hasexc`: opcodes that set an exception handler (added 3.12). ([Python documentation][2])
* `dis.hasjabs`: historical “absolute jump target” collection (kept for compatibility). ([Python documentation][2])

---

## B) `inspect` — Inspect live objects (3.13)

> The module organizes around **type checking**, **source retrieval**, **callable introspection**, and **stack/frame inspection**. ([Python documentation][3])

### B1) Types & members: discovery primitives

* Member enumeration:

  * `inspect.getmembers(obj, predicate=None)` → `(name, value)` sorted; predicate filters. ([Python documentation][3])
  * `inspect.getmembers_static(obj, predicate=None)` → member discovery **without** triggering descriptors / `__getattr__` / `__getattribute__` (useful for “don’t execute code while introspecting”). ([Python documentation][3])

* Core “is-*” predicates (selection tools for `getmembers` / classification):

  * Modules/classes/callables: `ismodule`, `isclass`, `isfunction`, `ismethod`, `isbuiltin`, `isroutine` ([Python documentation][3])
  * Generators/coroutines/async: `isgeneratorfunction`, `isgenerator`, `iscoroutinefunction`, `iscoroutine`, `isasyncgenfunction`, `isasyncgen`, `isawaitable` ([Python documentation][3])

    * 3.13 note: `isgeneratorfunction` now handles `functools.partialmethod()` wrappers. ([Python documentation][3])
  * Descriptors/ABCs: `isabstract`, `ismethoddescriptor`, `isdatadescriptor`, `isgetsetdescriptor`, `ismemberdescriptor` ([Python documentation][3])
  * Runtime artifacts: `iscode`, `isframe`, `istraceback` (and related “low-level” checks)

### B2) Retrieving source code (filesystem-backed reflection)

* “Where did this come from?”:

  * `getfile`, `getsourcefile`, `getabsfile`, `getmodule`, `getmodulename`
* “Give me the text”:

  * `findsource`, `getsourcelines`, `getsource`
  * docstrings/comments utilities: `getdoc`, `cleandoc`, `getcomments`

(These are the canonical surfaces used by debuggers, doc tools, and code browsing features.)

### B3) Signature & argument binding (PEP 362 plane)

* `inspect.signature(callable, *, follow_wrapped=True, globals=None, locals=None, eval_str=False)` ([Python documentation][3])

  * `follow_wrapped`: respects `__wrapped__` chain unless disabled. ([Python documentation][3])
  * `globals/locals/eval_str` (added 3.10) thread through to annotation handling/“un-stringizing”. ([Python documentation][3])
  * Implementation detail: `__signature__` may be honored if present (behavior not guaranteed stable). ([Python documentation][3])

* Core types:

  * `inspect.Signature` (immutable; stores ordered mapping of parameters) ([Python documentation][3])
  * `inspect.Parameter` (kind/default/annotation; supports `replace`) ([Python documentation][3])
  * `inspect.BoundArguments` (result of binding; `.args`, `.kwargs`, `.arguments`, `.apply_defaults()`) ([Python documentation][3])

* “Legacy” argument APIs (still present, sometimes deprecated):

  * `getfullargspec`, `getargvalues`, `formatargvalues`
  * `getcallargs` (deprecated in favor of `Signature.bind/bind_partial`). ([Python documentation][3])

### B4) Annotations resolution (safe access pattern)

* `inspect.get_annotations(obj, *, globals=None, locals=None, eval_str=False)` ([Python documentation][3])

  * Always returns a **fresh dict**; returns `{}` if absent; ignores inherited class annotations. ([Python documentation][3])
  * `eval_str=True` uses `eval()` to “un-stringize” string annotations; `globals/locals` default based on whether `obj` is module/class/callable. ([Python documentation][3])
  * Docs call it best practice for accessing annotation dicts in this era. ([Python documentation][3])

### B5) Class + attribute topology (structure discovery)

* Class layout helpers:

  * `getmro(cls)` (method resolution order) ([Python documentation][3])
  * `getclasstree(...)`, `classify_class_attrs(...)` (attribute classification / inheritance tracking)
* Static attribute access:

  * `getattr_static(obj, name, default=...)` (no descriptor execution)
  * `getmembers_static` (covered above)

### B6) Interpreter stack & traceback surfaces (debugger-grade)

* Frame/trace records:

  * `FrameInfo` and `Traceback` objects (3.11+ are class instances; carry `.positions` via `dis.Positions`). ([Python documentation][3])
* Core APIs:

  * `getframeinfo`, `getouterframes`, `getinnerframes`
  * `currentframe`, `stack`, `trace` ([Python documentation][3])
* Memory footgun:

  * Keeping frame references can create reference cycles; docs recommend breaking cycles (`del frame` / `frame.clear()`). ([Python documentation][3])

### B7) Generator / coroutine / asyncgen state inspection

* State + locals:

  * `getgeneratorstate`, `getgeneratorlocals`
  * `getcoroutinestate`, `getcoroutinelocals`
  * `getasyncgenstate`, `getasyncgenlocals`
* State constants typically returned:

  * `GEN_CREATED`, `GEN_RUNNING`, `GEN_SUSPENDED`, `GEN_CLOSED` (and coroutine/asyncgen analogs)

### B8) Low-level constants/enums exposed for tooling

* Code object flag constants (`CO_*`), e.g. `CO_GENERATOR`, `CO_COROUTINE`, `CO_ASYNC_GENERATOR`, etc. ([Python documentation][3])
* `inspect.BufferFlags` (`enum.IntFlag`) for the `__buffer__()` request plane. ([Python documentation][3])

### B9) Command-line interface

* `python -m inspect <module>[:qualname] [--details]` (prints module source by default; `--details` prints metadata instead). ([Python documentation][3])

---

## C) `symtable` — Access to the compiler’s symbol tables (3.13)

### C1) Symbol table generation

* `symtable.symtable(code, filename, compile_type)` → top-level `SymbolTable` for source (compile_type mirrors `compile(..., mode=...)`). ([Python documentation][1])
* Concept: symbol tables are generated from AST right before bytecode; they decide identifier scope classification. ([Python documentation][1])

### C2) Table typing (3.13 shifts to enums)

* `class symtable.SymbolTableType`: enum of table kinds. ([Python documentation][1])

  * Core: `MODULE`, `FUNCTION`, `CLASS` ([Python documentation][1])
  * Annotation/type-parameter era scopes: `ANNOTATION`, `TYPE_ALIAS`, `TYPE_PARAMETERS`, `TYPE_VARIABLE` (TYPE_VARIABLE added in 3.13). ([Python documentation][1])

### C3) `SymbolTable` (base) — namespace block facts

* `get_type()` → returns a `SymbolTableType` member (3.13). ([Python documentation][1])
* Identity/position:

  * `get_id()`, `get_name()`, `get_lineno()` ([Python documentation][1])
* Structural properties:

  * `is_optimized()`, `is_nested()`, `has_children()` ([Python documentation][1])
* Symbol access:

  * `get_identifiers()` (view object of names), `lookup(name) -> Symbol`, `get_symbols()` ([Python documentation][1])
* Tree navigation:

  * `get_children()` → nested tables (functions/classes/scopes). ([Python documentation][1])

### C4) `Function` table — scope partitions

* Inherits `SymbolTable`. ([Python documentation][1])
* Name partitions:

  * `get_parameters()`, `get_locals()`, `get_globals()`, `get_nonlocals()`, `get_frees()` ([Python documentation][1])

### C5) `Class` table — method-like extraction

* Inherits `SymbolTable`. ([Python documentation][1])
* `get_methods()` → names of `def`/`async def` defined directly in class body (ignores deeper scopes). ([Python documentation][1])

### C6) `Symbol` — per-identifier classification API

* Identity:

  * `get_name()` ([Python documentation][1])
* Usage/definition flags:

  * `is_referenced()`, `is_assigned()`, `is_imported()`, `is_annotated()` ([Python documentation][1])
* Scope flags:

  * `is_parameter()`, `is_global()`, `is_declared_global()`, `is_nonlocal()`, `is_local()`, `is_free()` ([Python documentation][1])
* Namespace binding:

  * `is_namespace()`, `get_namespaces()`, `get_namespace()` ([Python documentation][1])

### C7) Command-line usage (new in 3.13)

* `python -m symtable [infile...]` (reads stdin if no file; dumps symbol tables to stdout). ([Python documentation][1])

---

If you want the **next increment after this inventory** (matching how your PyArrow doc goes from “catalog” → “deep dives”), I can expand each module into 3–6 “deep dive pages” with: (1) power knobs + failure modes, (2) minimal but precise examples, and (3) “indexer-grade” extraction recipes (e.g., building CFG/DFG from `dis.Instruction` + `stack_effect`, and validating scope edges using `symtable`).

[1]: https://docs.python.org/uk/3.13/library/symtable.html "symtable — Access to the compiler’s symbol tables — Python 3.13.11 documentation"
[2]: https://docs.python.org/uk/3.13/library/dis.html "dis — Disassembler for Python bytecode — Python 3.13.11 documentation"
[3]: https://docs.python.org/3.13/library/inspect.html "inspect — Inspect live objects — Python 3.13.11 documentation"


Below is the **“deep dive pack”**: 3–6 “pages” per module, each with **power knobs**, **failure modes**, **minimal precise snippets**, and **indexer-grade extraction recipes** (CFG/DFG + scope validation).

---

# Deep dive pack index

## `dis` (3 pages)

* **DIS-1** Instruction facts: caches, specialization, offsets/labels, source positions
* **DIS-2** Control flow: basic blocks, jump semantics, exception table parsing + edges
* **DIS-3** Minimal dataflow: stack model + def/use extraction (DFG scaffolding)

## `inspect` (4 pages)

* **INSPECT-1** Safe introspection: type tests, member enumeration, static attribute fetch
* **INSPECT-2** Callable surfaces: `Signature`, binding, unwrapping, canonical signature facts
* **INSPECT-3** Annotations: `get_annotations` as the stable access layer
* **INSPECT-4** Stack/frames: FrameInfo, tracebacks, mapping runtime state to bytecode

## `symtable` (4 pages)

* **SYMTABLE-1** Scope model: tables, symbol flags, annotation/type scopes
* **SYMTABLE-2** Scope graph extraction: lexical scopes → graph edges
* **SYMTABLE-3** Cross-validation: `symtable` ↔ `code` ↔ `dis` consistency checks
* **SYMTABLE-4** 3.13-era typing scopes: TYPE_PARAMETERS / TYPE_ALIAS / TYPE_VARIABLE pitfalls

---

## DIS-1 — Instruction facts (what to extract, how to normalize)

### What you’re really looking at

* CPython bytecode is explicitly an **implementation detail**; opcodes can change between releases, and caches/specialization complicate “naive offsets.” ([Python documentation][1])
* Since 3.11, instructions can have **inline caches** (hidden by default) and the interpreter can **specialize/adapt** bytecode at runtime. ([Python documentation][1])
* In 3.13, text output uses **logical labels** for jump targets/exception handlers; offsets are optionally re-enabled via `show_offsets` / `-O`. ([Python documentation][1])

### Power knobs (disassembly fidelity)

* `adaptive=True`: show specialized/runtime-adapted opcodes (great for perf work; bad for deterministic indexing). ([Python documentation][1])
* `show_caches=True`: show cache entries in *text output*; programmatic iteration now carries cache payload via `Instruction.cache_info`. ([Python documentation][1])
* Prefer `adaptive=False` (default) for indexers; normalize specialized ops via `instr.baseopname` / `instr.baseopcode`. (These exist as properties in CPython’s `Instruction` implementation.) ([Chromium Git Repositories][2])

### Failure modes & drift hazards

* **Offsets are tricky around caches**:

  * Jump arguments are defined in terms of instruction offsets and caches (3.12+); you cannot reason correctly about backward jumps without accounting for caches. ([Python documentation][1])
* `dis.findlinestarts()` can yield `lineno=None` for bytecode that does not map to source lines. ([Python documentation][1])
* `get_instructions(..., show_caches=...)` (3.13) no longer emits separate CACHE pseudo-instructions; cache payload is attached to each `Instruction` (and `show_caches` is deprecated/no-op there). ([Python documentation][1])

### Minimal “instruction facts” schema (stable for a code index)

Recommended *normalized* fields (think “Arrow table row”):

* **identity**: `code_key`, `qualname`, `co_firstlineno`, `offset`
* **opcode**: `opname`, `baseopname`, `opcode`, `baseopcode`
* **operand**: `arg`, `oparg` (alias), `argval`, `argrepr`
* **control**: `jump_target` (bytecode offset or None), `label` (jump-target label or None)
* **source span**: `positions` (lineno/end_lineno/col/end_col), `line_number`
* **cache**: `cache_info` (triplets: `(name, size, data)`), plus derived `cache_offset/end_offset` if you want byte ranges

> CPython computes `cache_info` by slicing the code bytes according to `_cache_format` and cache sizes. ([Chromium Git Repositories][2])

### Minimal snippet: extract normalized instruction records

```python
from __future__ import annotations

import dis
import types
from dataclasses import dataclass
from typing import Any, Iterable

@dataclass(frozen=True)
class InstrFact:
    code_key: str
    offset: int
    opname: str
    baseopname: str
    opcode: int
    baseopcode: int
    arg: int | None
    argval: Any
    argrepr: str
    jump_target: int | None
    label: int | None
    line_number: int | None
    positions: dis.Positions | None
    cache_info: tuple[tuple[str, int, bytes], ...] | None

def iter_instr_facts(co: types.CodeType, *, code_key: str) -> Iterable[InstrFact]:
    for ins in dis.get_instructions(co, adaptive=False):
        cache = ins.cache_info
        cache_t = tuple(cache) if cache else None

        # “label” is the canonical 3.13 jump-target marker in CPython;
        # if you want a bool, use (ins.label is not None).
        label = getattr(ins, "label", None)

        yield InstrFact(
            code_key=code_key,
            offset=ins.offset,
            opname=ins.opname,
            baseopname=getattr(ins, "baseopname", ins.opname),
            opcode=ins.opcode,
            baseopcode=getattr(ins, "baseopcode", ins.opcode),
            arg=ins.arg,
            argval=ins.argval,
            argrepr=ins.argrepr,
            jump_target=getattr(ins, "jump_target", None),
            label=label,
            line_number=getattr(ins, "line_number", None),
            positions=getattr(ins, "positions", None),
            cache_info=cache_t,
        )
```

### Indexer-grade recipe: deterministic normalization

1. **Always store `baseopname`** and treat it as the semantic opcode. Specialized opnames are mostly perf artifacts. ([Chromium Git Repositories][2])
2. **Store offsets in bytes**, but treat them as *unstable join keys* across versions; prefer:

   * structural keys: `(qualname, co_firstlineno, instr_index)`
   * or label-based references for jumps in textual debug views (3.13 labels). ([Python documentation][1])
3. **Keep cache payload** for debugging/perf; don’t feed caches into correctness logic unless you’re explicitly analyzing specialization. ([Python documentation][1])

---

## DIS-2 — CFG reconstruction (jumps + exception table)

### The two CFG channels you must model

1. **Normal control flow**: fallthrough + jumps.
2. **Exceptional control flow**: exception table (“try regions” → handlers).

CPython 3.11+ moved most try/except mechanics into a compact **exception table** (printed as `ExceptionTable:` in disassembly). The dis module parses it from `code.co_exceptiontable`. ([Chromium Git Repositories][2])

### Power knobs

* Use **non-adaptive code** (`co.co_code`) when building label maps / walking bytecode; adaptive replacements (e.g., instrumentation/executor-related opcodes) can break the naive walk. ([Chromium Git Repositories][2])
* Use `dis.findlabels(co.co_code)` to get jump targets; then union with exception table boundaries. (That’s exactly how CPython builds label maps.) ([Chromium Git Repositories][2])

### Failure modes

* Jump targets are relative to “after cache entries” in 3.12+; for correctness, you should trust `instr.jump_target` (or replicate CPython’s `offset_from_jump_arg`) rather than re-deriving. ([Chromium Git Repositories][2])
* Exception table ranges are half-bytecode indices encoded as varints; parse errors → silently missing handler edges → *very wrong CFG* (especially for `try/finally`). ([Chromium Git Repositories][2])

### Minimal snippet: parse exception table (CPython-equivalent)

This is lifted directly from CPython’s logic: varints decoded, multiplied by 2. ([Chromium Git Repositories][2])

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator
import types

def _parse_varint(it: Iterator[int]) -> int:
    b = next(it)
    val = b & 63
    while b & 64:
        val <<= 6
        b = next(it)
        val |= b & 63
    return val

@dataclass(frozen=True)
class ExcEntry:
    start: int   # byte offset
    end: int     # byte offset (exclusive)
    target: int  # handler byte offset
    depth: int
    lasti: bool

def parse_exception_table(co: types.CodeType) -> list[ExcEntry]:
    it = iter(co.co_exceptiontable)
    out: list[ExcEntry] = []
    try:
        while True:
            start = _parse_varint(it) * 2
            length = _parse_varint(it) * 2
            end = start + length
            target = _parse_varint(it) * 2
            dl = _parse_varint(it)
            depth = dl >> 1
            lasti = bool(dl & 1)
            out.append(ExcEntry(start, end, target, depth, lasti))
    except StopIteration:
        return out
```

### CFG recipe (basic blocks + edges)

#### Step 1 — Materialize the instruction list + offset index

* `insns = list(dis.get_instructions(co, adaptive=False))`
* `by_off = {ins.offset: idx for idx, ins in enumerate(insns)}`

#### Step 2 — Compute block boundaries

Boundaries include:

* `0`
* every **jump target** offset (either from `ins.jump_target` or `dis.findlabels(co.co_code)`)
* the **instruction after** any terminator (return/raise/unconditional jump)
* exception-table boundaries: every `entry.start`, `entry.end`, `entry.target` ([Chromium Git Repositories][2])

#### Step 3 — Build normal edges

For each block ending at instruction `i_end`:

* If last instruction has `jump_target`:

  * add edge `block -> target_block`
  * if it’s a *conditional* jump, also add fallthrough `block -> next_block`
* Else if terminator (RETURN/RAISE): no fallthrough
* Else: fallthrough to next block

> If you don’t want to maintain opcode-classification tables, you can do a pragmatic rule:
>
> * “has jump_target” => jump edge exists
> * “RETURN_VALUE / RERAISE / RAISE_VARARGS” => terminator
> * otherwise fallthrough

#### Step 4 — Add exception edges (conservative but correct-ish)

For each `ExcEntry(start,end,target,...)`:

* Let `try_blocks = blocks_with_any_instruction_offset_in_[start,end)`
* Let `handler_block = block_containing(target)`
* Add `EXC_EDGE: try_block -> handler_block` (optionally tag with `depth/lasti`)

This matches how dis uses exception entries to build label maps (`start/end/target` become labeled locations). ([Chromium Git Repositories][2])

### Golden fixtures (CFG sanity)

Micro-programs that should be in your golden suite:

* `if/elif/else` (two conditionals; join block)
* `while` with `break` and `continue` (back edge + exits)
* `try/except/else/finally` (exception edges + finally paths)
* `with` (exception edges to cleanup)
* comprehension (implicit function-ish code object; scope weirdness)

---

## DIS-3 — DFG scaffolding via a stack model (minimal but useful)

### What “DFG from bytecode” means in practice

Bytecode is stack-based. A minimal DFG is:

* each instruction produces 0..n **value IDs** pushed to the stack
* other instructions **consume** value IDs by popping
* stores bind value IDs to **names/slots** (locals/globals/freevars)
* loads read from those bindings

You can validate your stack discipline with `dis.stack_effect(op, oparg, jump=...)`. In 3.13, omitting `oparg` now behaves like `oparg=0`, and passing `oparg` to non-arg opcodes is no longer an error. ([Python documentation][1])

### Power knobs

* Normalize semantics by `baseopname` (collapse specialized ops). ([Chromium Git Repositories][2])
* Keep an “unknown” value when you can’t model semantics (calls, dynamic attribute resolution, etc.)—don’t stop the pipeline.

### Failure modes (the common ones)

* Incorrect stack delta around conditional jumps if you ignore `jump=` effects.
* Attribute ops and CALL ops have nuanced stack conventions; if you treat them “one size fits all,” your DFG becomes garbage quickly.
* Exception edges: instructions inside try blocks may implicitly flow to handlers; you’ll miss major flows if you never add exception edges from DIS-2.

### Minimal DFG extractor (sketch)

```python
from __future__ import annotations
import dis
import types
from dataclasses import dataclass

@dataclass
class Value:
    vid: int
    produced_by: int  # instruction offset

@dataclass
class DefUseEdge:
    kind: str         # "USE_STACK", "DEF_LOCAL", "USE_LOCAL", ...
    src: int          # value id or symbol id
    dst: int          # instruction offset or symbol id

def build_min_dfg(co: types.CodeType) -> tuple[list[Value], list[DefUseEdge]]:
    vals: list[Value] = []
    edges: list[DefUseEdge] = []
    stack: list[Value] = []
    locals_map: dict[str, Value] = {}

    next_vid = 1
    def new_val(off: int) -> Value:
        nonlocal next_vid
        v = Value(next_vid, off)
        next_vid += 1
        vals.append(v)
        return v

    for ins in dis.get_instructions(co, adaptive=False):
        op = getattr(ins, "baseopname", ins.opname)

        if op == "LOAD_FAST":
            name = str(ins.argval)
            v = locals_map.get(name) or new_val(ins.offset)  # unknown initial
            edges.append(DefUseEdge("USE_LOCAL", v.vid, ins.offset))
            stack.append(v)

        elif op == "STORE_FAST":
            name = str(ins.argval)
            v = stack.pop()
            locals_map[name] = v
            edges.append(DefUseEdge("DEF_LOCAL", v.vid, ins.offset))

        elif op in {"BINARY_OP", "BINARY_ADD", "BINARY_SUBTRACT"}:
            r = stack.pop()
            l = stack.pop()
            edges.append(DefUseEdge("USE_STACK", l.vid, ins.offset))
            edges.append(DefUseEdge("USE_STACK", r.vid, ins.offset))
            stack.append(new_val(ins.offset))

        elif op in {"CALL", "CALL_FUNCTION", "CALL_METHOD"}:
            # best-effort: pop argcount + callable; push result
            # NOTE: exact pop count depends on opcode family; treat as conservative.
            # For correctness you’ll eventually model CALL conventions precisely.
            # Here: consume “some” stack then produce one value.
            if ins.arg is not None:
                argc = int(ins.arg)
                for _ in range(argc):
                    if stack: edges.append(DefUseEdge("USE_STACK", stack.pop().vid, ins.offset))
            if stack: edges.append(DefUseEdge("USE_STACK", stack.pop().vid, ins.offset))  # callable
            stack.append(new_val(ins.offset))

        else:
            # Fallback: apply stack_effect to keep stack depth consistent (optional).
            # Many ops are “transparent” for DFG purposes (NOP/RESUME/etc).
            pass

    return vals, edges
```

### Indexer-grade upgrades (what you add next)

* Track **stack depth** per instruction and assert consistency (use `stack_effect`).
* Add **memory edges**:

  * `LOAD_ATTR` / `STORE_ATTR` edges from object value → attribute slot
  * `LOAD_GLOBAL` edges keyed by `co.co_names` indices
* Merge with CFG (DIS-2): DFG is only meaningful per control-flow path.

---

# INSPECT deep dives

## INSPECT-1 — Safe introspection (don’t execute code while inspecting)

### Power knobs

* `inspect.getmembers(obj)` gives you a complete view but may trigger dynamic behavior.
* `inspect.getmembers_static(obj)` enumerates members **without** triggering descriptors or `__getattr__` / `__getattribute__`. It may miss dynamic attributes and may return descriptors rather than bound values. ([Python documentation][3])
* `inspect.getattr_static(obj, name)` is the targeted version: fetch an attribute without executing descriptor logic. ([Python documentation][3])

### Failure modes

* `getattr()` / `hasattr()` can execute arbitrary code via descriptors or `__getattr__`. `getattr_static` exists explicitly to avoid this. ([Python documentation][3])
* `getmembers_static` can return members that `getmembers` wouldn’t (e.g., descriptors that raise `AttributeError`) and vice versa. ([Python documentation][3])

### Minimal snippet: “safe member crawl” for an indexer

```python
import inspect

def safe_public_members(obj):
    for name, value in inspect.getmembers_static(obj):
        if name.startswith("_"):
            continue
        yield name, value  # may be descriptor objects; that’s *good* for passive tools
```

### Indexer recipe

Use this pattern for docs/indexers:

* enumerate with `getmembers_static`
* classify with `inspect.isfunction / isclass / ismodule / isroutine`
* only “activate” (call/instantiate/resolve descriptors) in a separate, sandboxed phase

---

## INSPECT-2 — Signatures & binding (canonical callable facts)

### The stable plane

* `inspect.signature(obj)` + `Signature.parameters` is the modern, structured surface for callables.
* Binding uses `Signature.bind` / `Signature.bind_partial` which yields `BoundArguments` (`.arguments`, `.args`, `.kwargs`). ([Python documentation][3])

### Power knobs

* Wrapper handling: `inspect.unwrap(func, stop=...)` follows `__wrapped__` chain; `signature()` uses this mechanism and stops unwrapping if something defines `__signature__`. ([Python documentation][3])

### Failure modes

* Builtins / C-extension callables may have incomplete signature metadata.
* Decorators can hide true call signatures unless `__wrapped__` / `__signature__` is preserved.
* “Implicit” parameters exist in some compiler-generated code objects (comprehensions); they show up in parameter naming conventions. ([Python documentation][3])

### Minimal snippet: canonical signature “fact record”

```python
import inspect
from dataclasses import dataclass

@dataclass(frozen=True)
class SignatureFact:
    qualname: str
    params: tuple[tuple[str, str, bool, bool], ...]  # (name, kind, has_default, has_annot)
    has_varargs: bool
    has_varkw: bool

def signature_fact(fn) -> SignatureFact:
    fn2 = inspect.unwrap(fn)
    sig = inspect.signature(fn2)
    params = []
    has_varargs = False
    has_varkw = False
    for p in sig.parameters.values():
        k = p.kind.name
        has_default = p.default is not p.empty
        has_annot = p.annotation is not p.empty
        params.append((p.name, k, has_default, has_annot))
        has_varargs |= (p.kind is p.VAR_POSITIONAL)
        has_varkw |= (p.kind is p.VAR_KEYWORD)
    return SignatureFact(getattr(fn2, "__qualname__", repr(fn2)), tuple(params), has_varargs, has_varkw)
```

### Indexer-grade recipe

Emit two linked datasets:

* `callable_signature_facts` (from `signature()`)
* `callable_code_facts` (from `fn.__code__`: arg counts, locals, freevars)
  …and assert they’re consistent where possible.

---

## INSPECT-3 — Annotations (treat `get_annotations` as the API boundary)

### Why `get_annotations` matters

`inspect.get_annotations(obj, ...)` is designed as the **stable way** to compute an annotations dict:

* only accepts callable/class/module
* returns a **new dict** each call
* handles multiple details for you (including string annotations depending on configuration) ([Python documentation][3])

### Failure modes

* If you read `__annotations__` directly you’ll hit edge cases around evaluation timing and ownership rules.
* Evaluating string annotations (`eval_str=True`) is a code execution boundary; treat it as “unsafe mode.”

### Minimal snippet: safe annotation extraction policy

```python
import inspect

def safe_annotations(obj) -> dict:
    # Do not eval strings by default (safe for indexers).
    return inspect.get_annotations(obj, eval_str=False)
```

---

## INSPECT-4 — Stack / frames / tracebacks (runtime mapping)

### Key surfaces

* `inspect.currentframe()` returns the caller’s frame in CPython; it may be `None` in other implementations. ([Python documentation][3])
* Frame-driven argument recovery exists via `inspect.getargvalues(frame)`; it was *inadvertently* marked deprecated in 3.5 (not a current deprecation). ([Python documentation][3])

### Indexer-grade usage pattern

When you need “runtime evidence” (profilers, debugging tooling) that ties back to static facts:

1. Frame gives `f_code` (code object), `f_lasti` (bytecode instruction index)
2. Use `dis.get_instructions(f_code)` and locate instruction by offset / lasti
3. Use `Instruction.positions` for source-span mapping (when available)

---

# SYMTABLE deep dives

## SYMTABLE-1 — The scope model (what symtable actually gives you)

### What symtable represents

* Symbol tables are generated by the compiler **from AST just before bytecode is generated**, and compute the **scope of every identifier**. ([Python documentation][4])

### Table kinds you must model (3.13)

* `SymbolTableType` includes the usual `MODULE/FUNCTION/CLASS`, plus annotation/type-scope flavors (including `TYPE_VARIABLE` added in 3.13). ([Python documentation][4])
* `SymbolTable.get_type()` returns members of this enum (3.13 change). ([Python documentation][4])

### Symbol flags (what you store per identifier)

Per name in a table you can query:

* usage/definition: referenced, assigned, imported, annotated
* scope classification: parameter/global/declared_global/nonlocal/local/free
* namespace binding: `is_namespace()` and `get_namespaces()`

### Power knobs

* Use `compile_type` (“exec”, “eval”, “single”) as an explicit input dimension; it changes top-level behavior.

---

## SYMTABLE-2 — Building a lexical scope graph (indexer recipe)

### Output graph primitives

Nodes:

* `Scope(table_id, type, name, lineno, is_nested, is_optimized, filename)`
* `Name(scope_id, identifier)`

Edges:

* `DECLARES(scope -> name)`
* `ASSIGNS(name -> scope)` (or annotate `DECLARES` with assigned flag)
* `REFS(scope -> name)`
* `CAPTURES(function_scope -> name)` for freevars
* `BINDS_NAMESPACE(name -> child_scope)` where `is_namespace()`

### Minimal extraction walk

```python
import symtable
from dataclasses import dataclass

@dataclass(frozen=True)
class ScopeFact:
    sid: int
    kind: str
    name: str
    lineno: int

@dataclass(frozen=True)
class NameFact:
    sid: int
    ident: str
    is_local: bool
    is_free: bool
    is_global: bool
    is_nonlocal: bool
    is_param: bool

def extract_symtable(code: str, filename: str) -> tuple[list[ScopeFact], list[NameFact]]:
    st = symtable.symtable(code, filename, "exec")
    scopes: list[ScopeFact] = []
    names: list[NameFact] = []

    def walk(t):
        scopes.append(ScopeFact(t.get_id(), t.get_type().name, t.get_name(), t.get_lineno()))
        for ident in t.get_identifiers():
            s = t.lookup(ident)
            names.append(NameFact(
                t.get_id(), ident,
                s.is_local(), s.is_free(), s.is_global(), s.is_nonlocal(), s.is_parameter()
            ))
        for ch in t.get_children():
            walk(ch)

    walk(st)
    return scopes, names
```

---

## SYMTABLE-3 — Cross-validation: symtable ↔ code object ↔ dis

This is the “certainty booster” phase: reject inconsistent interpretations early.

### Core invariants you can assert

For each **function scope** `T`:

* `T.get_frees()` should match the function code object’s `co_freevars` (names captured from outer scopes).
* `T.get_parameters()` should match the leading argument portion implied by the code object’s arg counts (posonly/argcount/kwonly).
* `T.get_locals()` should be a subset of code object locals (`co_varnames`)—with care for compiler-generated temporaries.

For nested code objects (including annotation/type scopes):

* disassembly recursively includes code objects used for annotation scopes. ([Python documentation][1])
* your crosswalk should treat those as “non-runtime-callable” scopes unless proven otherwise.

### Minimal validation skeleton

```python
import symtable, types

def validate_scope_vs_code(fn_code: types.CodeType, fn_table) -> list[str]:
    errs = []
    frees = set(fn_table.get_frees())
    if frees != set(fn_code.co_freevars):
        errs.append(f"freevars mismatch: symtable={sorted(frees)} code={sorted(fn_code.co_freevars)}")
    # Add more checks as you wish (params/locals/globals)
    return errs
```

---

## SYMTABLE-4 — 3.13 typing/annotation scopes (pitfalls)

### What changes in modern typing code

You will encounter nested tables of types:

* `ANNOTATION` (future annotations)
* `TYPE_PARAMETERS` (generic functions/classes)
* `TYPE_ALIAS` (`type` statements)
* `TYPE_VARIABLE` (bound/constraints/default of a TypeVar/ParamSpec/TypeVarTuple; added 3.13) ([Python documentation][4])

### Pitfalls

* These scopes may generate nested code objects / synthetic locals that do not behave like “regular” runtime locals.
* If you’re building a scope graph, **don’t assume** every scope corresponds 1:1 with a function a user can call.

### Practical indexer policy

* Treat non `FUNCTION/CLASS/MODULE` tables as **meta-scopes**:

  * still record symbols + resolution
  * keep them out of your “callable inventory”
  * still allow them to participate in name-resolution correctness checks

---

## Quick “next artifact” option (if you want it)

I can turn these pages into a **repo-ready contract harness** (your usual style):

* pytest fixtures that compile micro-programs,
* emit normalized `dis` instruction facts + CFG edges + symtable scope facts,
* compare against golden JSON snapshots with deterministic normalization (baseopname, label mapping, exception table edges).

[1]: https://docs.python.org/uk/3.13/library/dis.html "dis — Disassembler for Python bytecode — Python 3.13.11 documentation"
[2]: https://chromium.googlesource.com/external/github.com/python/cpython/%2B/refs/heads/3.13/Lib/dis.py "Lib/dis.py - external/github.com/python/cpython - Git at Google"
[3]: https://docs.python.org/3.13/library/inspect.html "inspect — Inspect live objects — Python 3.13.11 documentation"
[4]: https://docs.python.org/uk/3.13/library/symtable.html "symtable — Access to the compiler’s symbol tables — Python 3.13.11 documentation"

Absolutely — `dis` is worth a dedicated, “no ambiguities” deep dive. I’ll structure this as:

1. **What `dis` can emit** (3 output shapes)
2. **Every public entry point** (CLI + functions + classes), with **inputs → outputs** under different circumstances
3. **The emitted data objects** (`Bytecode`, `Instruction`, `Positions`) in “schema-level” detail
4. **The three major “gotchas” in 3.10–3.13** (offset units, caches, labels) and exactly how they affect outputs

Everything below is specific to **Python 3.13** docs. ([Python documentation][1])

---

## 1) The three output shapes `dis` can produce

### Shape A — **Formatted text disassembly** (human-facing)

Emitted by:

* `python -m dis ...` (CLI) ([Python documentation][1])
* `dis.dis(...)`, `dis.distb(...)`, `dis.disassemble(...)` / `dis.disco(...)` (write text to `file` or `sys.stdout`)
* `Bytecode(...).dis()` (returns the same text as `dis.dis()`, but as a **string**)

### Shape B — **Structured instruction stream**

Emitted by:

* `dis.get_instructions(...)` → iterator of `dis.Instruction` objects ([Python documentation][1])
* iterating a `dis.Bytecode(...)` instance → yields `dis.Instruction` objects ([Python documentation][1])

### Shape C — **Formatted code-object metadata string**

Emitted by:

* `dis.code_info(...)` → returns multi-line string (implementation-dependent)
* `dis.show_code(...)` → prints that string
* `Bytecode(...).info()` → returns similar string

---

## 2) The “big 3.10 → 3.13” context that changes how you interpret outputs

These version notes are the *reason* many `dis` outputs look “surprising” if you learned bytecode earlier:

### 2.1 Offsets and jump operands changed meaning

* 3.10: jump / exception / loop instruction **arguments** became **instruction offsets** rather than **byte offsets**. ([Python documentation][1])
* 3.12: jump arguments are **relative to the instruction after the jump’s CACHE entries**, meaning caches are “transparent” for forward jumps but must be accounted for for backward jumps. ([Python documentation][1])

**Practical consequence:** don’t treat `instr.arg` on a jump opcode as “bytes to target.” Use `Instruction.jump_target` (already resolved) for programmatic CFG building. `jump_target` is explicitly provided as “bytecode index of the jump target.” ([Python documentation][1])

### 2.2 Inline caches exist and can be shown/hidden

* 3.11: some instructions have inline caches which exist as `CACHE` pseudo-instructions in the bytecode; they’re hidden by default and can be shown with `show_caches=True`. ([Python documentation][1])

**Practical consequence:** apparent “gaps” in offsets (or unexpected jump math) often come from hidden cache slots. In 3.13, caches are also exposed in structured form via `Instruction.cache_info`. ([Python documentation][1])

### 2.3 3.13 switched text output to logical labels

* 3.13: text output uses **logical labels** for jump targets and exception handlers rather than printing raw offsets; the `-O` CLI option and `show_offsets` controls were introduced to re-include offsets. ([Python documentation][1])

**Practical consequence:** if you’re comparing disassemblies across versions/tools, you’ll often want “offsets on” for deterministic debugging views.

---

## 3) Command-line interface: `python -m dis`

### 3.1 Invocation and inputs

```
python -m dis [-h] [-C] [-O] [infile]
```

If `infile` is provided, it disassembles that file to stdout; otherwise it reads compiled source from stdin and disassembles. ([Python documentation][1])

### 3.2 Options and what they change in output

* `-C, --show-caches`: show inline cache entries (3.13+). ([Python documentation][1])
* `-O, --show-offsets`: show instruction offsets (3.13+). ([Python documentation][1])

**Output shape:** always **formatted text** (Shape A).

---

## 4) Bytecode analysis object: `dis.Bytecode`

### 4.1 Constructor (what inputs it accepts)

`class dis.Bytecode(x, *, first_line=None, current_offset=None, show_caches=False, adaptive=False, show_offsets=False)` ([Python documentation][1])

`x` can be a:

* function / method
* generator / async generator / coroutine
* string of source code
* code object (as from `compile()`) ([Python documentation][1])

**Notably:** this list does *not* include raw bytecode bytes (raw bytes are accepted by `dis.dis`, not documented for `Bytecode`). ([Python documentation][1])

### 4.2 What you can do with a `Bytecode` instance

#### (A) Iterate → structured instructions (Shape B)

Iterating yields `Instruction` instances. ([Python documentation][1])

```python
import dis

bc = dis.Bytecode(func)
for instr in bc:
    print(instr.opname, instr.argval)
```

**Important nuance:** iteration is a *single code object’s instruction stream*. If you want nested code objects, you explicitly recurse yourself (see `dis.dis()` below for recursive *formatted* output).

#### (B) `.dis()` → formatted text string (Shape A)

* `Bytecode.dis()` returns **the same formatted view** as `dis.dis()`, but as a string. ([Python documentation][1])

#### (C) `.info()` → formatted code object info string (Shape C)

* `Bytecode.info()` returns a formatted multi-line string like `dis.code_info()`. ([Python documentation][1])

#### (D) `.codeobj` → the underlying code object

* `Bytecode.codeobj` gives you the compiled code object. ([Python documentation][1])

### 4.3 The constructor knobs, precisely

#### `first_line`

Overrides what line number is reported for the first source line; else taken from the code object. ([Python documentation][1])

#### `current_offset`

Marks an instruction offset as the “current instruction” (the formatted output will show a marker on that opcode). ([Python documentation][1])

#### `show_caches`

If true, the **formatted** output displays inline cache entries. ([Python documentation][1])

#### `adaptive`

If true, formatted output displays specialized/adaptive bytecode that may differ from original. ([Python documentation][1])

#### `show_offsets`

If true, formatted output includes instruction offsets. ([Python documentation][1])

### 4.4 Traceback integration (pinpointing a failure instruction)

`Bytecode.from_traceback(tb, *, show_caches=False)` constructs a Bytecode and sets `current_offset` to the instruction responsible for the exception. ([Python documentation][1])

That’s the cleanest “give me the failing opcode” path if you want a **string** disassembly with a precise arrow marker.

---

## 5) Analysis functions (one-shot utilities)

### 5.1 `dis.code_info(x) → str`

Returns a formatted multi-line string with detailed code object information. The docs explicitly warn the contents are highly implementation-dependent and may change arbitrarily. ([Python documentation][1])

**Accepts:** function, generator, async generator, coroutine, method, source string, or code object.
**Emits:** Shape C (string)

### 5.2 `dis.show_code(x, *, file=None) → None`

Prints the same content as `code_info(x)` to `file` (or stdout). It’s a convenience shorthand for `print(code_info(x), file=file)`. ([Python documentation][1])

**Emits:** Shape C (printed)

---

## 6) Disassembly functions (formatted text)

### 6.1 `dis.dis(x=None, *, file=None, depth=None, show_caches=False, adaptive=False)`

Disassembles “almost anything”:

* module, class, method, function
* generator / async generator / coroutine
* code object
* source string (compiled with `compile()` first)
* raw bytecode (byte sequence)
* if `x` is omitted, it disassembles the **last traceback** ([Python documentation][1])

For modules, it disassembles all functions; for classes, all methods (including class/static methods).

It **recursively disassembles nested code objects**, including nested functions/classes and code objects used for annotation scopes.

**Output routing:** writes text to `file` if provided, else to `sys.stdout`.

#### `depth`

Caps recursion depth unless `None`; `depth=0` means no recursion.

#### `show_caches` / `adaptive`

* `show_caches=True` includes cache entries in the formatted output. ([Python documentation][1])
* `adaptive=True` shows specialized bytecode. ([Python documentation][1])

**What you get:** Shape A only (printed text)

> Note: In 3.13, dis output generally prefers logical labels for jump targets/handlers; offsets can be shown via the 3.13 “show offsets” controls mentioned in the version notes and CLI `-O`. ([Python documentation][1])

### 6.2 `dis.distb(tb=None, *, file=None, show_caches=False, adaptive=False, show_offset=False)`

Disassembles the top-of-stack function of a traceback (or last traceback if `tb` is not given) and **indicates the instruction causing the exception**. ([Python documentation][1])

Like `dis.dis`, it writes to `file` or stdout.

Docs note: 3.13 added “show offsets” support here as well. ([Python documentation][1])

**What you get:** Shape A only (printed text)

### 6.3 `dis.disassemble(code, lasti=-1, *, file=None, show_caches=False, adaptive=False, show_offsets=False)` (aka `dis.disco`)

This is the “code object only” disassembler with the most explicit column spec and a `lasti` marker. ([Python documentation][1])

* If `lasti` is provided, it indicates the last instruction (i.e., highlights the “current instruction”).
* Output is divided into columns: line number, current marker (`-->`), label marker (`>>`), address, opcode name, parameters, and interpretation in parentheses.
* Interpretation recognizes local/global names, constants, branch targets, compare operators.
* Writes to `file` or stdout.

**What you get:** Shape A only (printed text)

**When to use `disassemble` over `dis.dis`:**

* You already have a **code object** and you want a clear `lasti` “current instruction” marker with the canonical column layout.
* You want `show_offsets=True` on the formatter (explicitly supported here).

---

## 7) Structured instruction APIs (the actual “data objects”)

### 7.1 `dis.get_instructions(x, *, first_line=None, show_caches=False, adaptive=False) -> iterator[Instruction]`

Returns an iterator over instructions in:

* function, method, source string, or code object

Key 3.13 behavior:

* `show_caches` is deprecated and has **no effect** here.
* Cache entries are no longer represented as separate “CACHE instructions” in the iterator; instead, the returned `Instruction` objects have `cache_info` populated as appropriate.

**What you get:** Shape B (structured stream)

### 7.2 `dis.Instruction` — the “row object” you index

`Instruction` instances carry these fields/attributes (think “schema”):

Opcode identity:

* `opcode`, `opname` ([Python documentation][1])
* `baseopcode`, `baseopname` (if specialized; else equal to opcode/opname) ([Python documentation][1])

Operand interpretation:

* `arg` (or `None`), `oparg` (alias), `argval`, `argrepr` ([Python documentation][1])

Bytecode layout indices:

* `offset`: start index of operation within bytecode sequence ([Python documentation][1])
* `start_offset`: includes any prefixed `EXTENDED_ARG` ops if present ([Python documentation][1])
* `cache_offset`: start index of cache entries after the operation ([Python documentation][1])
* `end_offset`: end index of cache entries after the operation ([Python documentation][1])

Source mapping:

* `starts_line`: **bool** (“True if starts a source line”) — changed in 3.13 ([Python documentation][1])
* `line_number`: source line number or `None` ([Python documentation][1])
* `positions`: a `dis.Positions` span object ([Python documentation][1])

Control flow:

* `is_jump_target` (bool) ([Python documentation][1])
* `jump_target`: resolved bytecode index of jump target or `None` ([Python documentation][1])

Caches:

* `cache_info`: triplets `(name, size, data)` or `None` if none. ([Python documentation][1])

### 7.3 `dis.Positions` — source span object

Holds:

* `lineno`, `end_lineno`, `col_offset`, `end_col_offset`
  Any can be `None` if not available. ([Python documentation][1])

---

## 8) Helper functions (small APIs, very important)

### 8.1 `dis.findlinestarts(code) -> generator[(offset, lineno)]`

Uses `code.co_lines()` to emit the offsets that start source lines, as `(offset, lineno)` pairs. ([Python documentation][1])
In 3.13, `lineno` can be `None` for bytecode that doesn’t map to source lines. ([Python documentation][1])

### 8.2 `dis.findlabels(code_bytes) -> list[offset]`

Detects all offsets in raw compiled bytecode that are jump targets.

### 8.3 `dis.stack_effect(opcode, oparg=None, *, jump=None) -> int`

Computes stack effect; `jump=True/False/None` controls “jump vs no-jump vs max-of-both.” ([Python documentation][1])
3.13: if `oparg` is omitted/None, it is treated as `oparg=0` (and passing an integer oparg to a no-arg opcode is no longer an error). ([Python documentation][1])

---

## 9) Opcode collections (introspection tables you’ll actually use)

These are sequences/lists mapping opcode classes:

* `opname` (index → name), `opmap` (name → opcode), `cmp_op`, etc. ([Python documentation][1])
* 3.12: collections include pseudo + instrumented instructions as well (`>= MIN_PSEUDO_OPCODE`, `>= MIN_INSTRUMENTED_OPCODE`). ([Python documentation][1])
* 3.13: `hasjump` added; all jumps are relative. ([Python documentation][1])
* `hasjrel` and `hasjabs` are deprecated in 3.13; `hasjabs` is empty. ([Python documentation][1])

---

## 10) “Crystal clear” behavior matrix (inputs × APIs × outputs)

### A) If you want **printed disassembly**

Use:

* `dis.dis(x, ...)` (broadest input support; recursive; depth control)
* `dis.disassemble(codeobj, lasti=..., ...)` (codeobj-only; strongest column spec; `lasti` marker; supports `show_offsets`)
* `dis.distb(tb)` (traceback-focused; marks failing instruction)

**Inputs:**

* module/class → only `dis.dis` (disassembles contained functions/methods)
* function/method/generator/coroutine → `dis.dis`, `Bytecode`, and `code_info` paths ([Python documentation][1])
* traceback → `dis.distb` or `dis.dis()` with `x` omitted (last traceback)
* raw bytecode bytes → `dis.dis` (documented)

### B) If you want **a string** (not printed)

Use:

* `Bytecode(x, ...).dis()` → formatted disassembly string ([Python documentation][1])
* `dis.code_info(x)` or `Bytecode(x).info()` → metadata string ([Python documentation][1])

### C) If you want **structured facts**

Use:

* `dis.get_instructions(x)` or iterate `dis.Bytecode(x)` → `Instruction` objects ([Python documentation][1])

…and you can rely on:

* `Instruction.jump_target` for CFG targets ([Python documentation][1])
* `Instruction.positions` for span mapping (fields may be `None`) ([Python documentation][1])
* `Instruction.cache_info` for cache payloads (3.13 integrates caches here; `get_instructions(show_caches=...)` no longer changes that).

---

## 11) Two “canonical” patterns that remove ambiguity

### Pattern 1: “I want text output, but I need to capture it deterministically”

* Prefer `Bytecode(...).dis()` for a string. ([Python documentation][1])
* If you need recursion depth control, use `dis.dis(..., depth=...)` and capture via `file=` (since `dis.dis` writes to `file` / stdout).

### Pattern 2: “I want a complete structured dataset for indexing”

* Use `get_instructions()` (or `Bytecode` iteration) for the primary instruction rows. ([Python documentation][1])
* For CFG, treat jump operands as *not directly trustworthy* and use:

  * `Instruction.jump_target`
  * `Instruction.is_jump_target`
  * plus exception-handling boundaries by scanning for opcodes in `dis.hasexc` (or by parsing `co_exceptiontable` yourself if you need full fidelity). ([Python documentation][1])

---

If you want, I can go one level deeper still and provide a **fully pinned “dis → Arrow tables” extraction contract** (schemas + normalization rules) that explicitly handles: `EXTENDED_ARG` spans (`start_offset`), caches (`cache_offset/end_offset/cache_info`), labels-vs-offsets normalization, and stable cross-version keys.

[1]: https://docs.python.org/3.13/library/dis.html "dis — Disassembler for Python bytecode — Python 3.13.11 documentation"

Below is a **full, “no ambiguity” deep dive** on **`inspect` in Python 3.13**: every *documented* command (CLI + functions/classes) and **exactly what outputs you get**, including **what changes under different object types / wrappers / builtins / missing source / no-frame implementations**.

All details are grounded in the **Python 3.13.11 `inspect` docs**. ([Python documentation][1])

---

# 1) The output shapes `inspect` can emit

Think in **output-shape buckets**; almost every API in `inspect` lands in one of these:

1. **Booleans** (predicates): `isfunction`, `iscoroutine`, `isdatadescriptor`, … ([Python documentation][1])
2. **Lists of `(name, value)`** (member enumeration): `getmembers`, `getmembers_static` ([Python documentation][1])
3. **Strings** (docs/source/formatting): `getdoc`, `getcomments`, `getsource`, `cleandoc`, `formatargvalues` ([Python documentation][1])
4. **Paths / names**: `getfile`, `getsourcefile`, `getmodulename` (or `None`) ([Python documentation][1])
5. **Structured “introspection objects”**:

   * `Signature`, `Parameter`, `BoundArguments` (callable model) ([Python documentation][1])
   * `FrameInfo`, `Traceback` (stack model; includes `dis.Positions`) ([Python documentation][1])
   * `BufferFlags` (`enum.IntFlag`) ([Python documentation][1])
6. **Named tuples / dicts** (older-but-still-used structured outputs):

   * `FullArgSpec`, `ArgInfo`, `ClosureVars` ([Python documentation][1])
   * `get_annotations` returns a **fresh dict** each call ([Python documentation][1])

---

# 2) Command-line interface: `python -m inspect`

`inspect` has a basic CLI mode:

* **Default behavior:** takes a *module name* and prints the **source** of that module.
* You can target a **specific class/function** by appending `:<qualified.name>` to the module.
* `--details`: prints **information about the object** rather than its source. ([Python documentation][1])

That CLI always emits **text** (either source text or a “details” report).

---

# 3) Types & members: classification + member enumeration

## 3.1 `getmembers(object[, predicate]) -> list[(name, value)]`

* Returns **all members** as `(name, value)` pairs, sorted by name; optionally filtered by `predicate(value)`. ([Python documentation][1])
* **Metaclass gotcha:** for classes, metaclass-defined attributes are only returned if they appear in the metaclass’s custom `__dir__()`. ([Python documentation][1])

**Practical consequence:** `getmembers()` is “dynamic”: it can trigger descriptor logic / dynamic attribute generation (not spelled out here, but see the “static” alternative below).

## 3.2 `getmembers_static(object[, predicate]) -> list[(name, value)]`

* Same return shape as `getmembers`, **but** it avoids dynamic lookup via:

  * descriptor protocol
  * `__getattr__`
  * `__getattribute__` ([Python documentation][1])
* **Trade-offs (very important):**

  * may miss attributes that `getmembers` finds (e.g., dynamically created)
  * may find attributes `getmembers` can’t (e.g., descriptors that raise `AttributeError`)
  * may return **descriptor objects** instead of instance members ([Python documentation][1])

Use `getmembers_static` for **passive introspection** (indexers, docs tools, security-sensitive inspection).

## 3.3 `getmodulename(path) -> str | None`

* If the path ends with a suffix recognized by `importlib.machinery.all_suffixes()`, returns the final component without extension; else `None`. ([Python documentation][1])
* Returns meaningful names for **modules**, not packages (package paths still return `None`). ([Python documentation][1])

---

## 3.4 The `is*` predicate family (what they mean + key 3.13 nuances)

These are mostly **“type tests”** used as predicates for member lists.

### “Core object kind” tests

* `ismodule`, `isclass`, `ismethod` (bound Python method), `isfunction` (Python function, incl. lambda) ([Python documentation][1])
* `istraceback`, `isframe`, `iscode`, `isbuiltin` ([Python documentation][1])

### Generator / coroutine / async tests

* `isgeneratorfunction`: recognizes `functools.partial()` (3.8+) and **`functools.partialmethod()` in 3.13** ([Python documentation][1])
* `iscoroutinefunction`: true for `async def`, `functools.partial` wrapping, and sync functions marked by `markcoroutinefunction()`; **partialmethod wrappers are also recognized in 3.13** ([Python documentation][1])
* `markcoroutinefunction(func)`: decorator to force detection as coroutine-function (added 3.12); docs recommend `async def` when possible. ([Python documentation][1])
* `iscoroutine`, `isawaitable`, `isasyncgenfunction`, `isasyncgen` ([Python documentation][1])

### Routine / descriptor / ABC tests

* `isroutine` (user-defined or builtin function/method), `isabstract` (abstract base class) ([Python documentation][1])
* Descriptor classification:

  * `ismethoddescriptor`: special-case descriptor objects like `int.__add__`; **3.13 fixes misclassification when `__get__` + `__delete__` exist but `__set__` doesn’t** (those are data descriptors, not method descriptors). ([Python documentation][1])
  * `isdatadescriptor`: `__set__` or `__delete__` (properties, getsets, members) ([Python documentation][1])
  * `isgetsetdescriptor` / `ismemberdescriptor`: explicitly documented as **CPython implementation detail** (may always be `False` on other implementations). ([Python documentation][1])

**Indexer takeaway:** use descriptor predicates + `getmembers_static`/`getattr_static` when you want to build a “member index” without executing code.

---

# 4) Retrieving docs + source (exact outputs + failure modes)

## 4.1 `getdoc(object) -> str | None`

* Returns docstring cleaned via `cleandoc()`.
* If docstring is missing and object is class/method/property/descriptor, it searches inheritance hierarchy.
* Returns `None` if doc is invalid/missing. ([Python documentation][1])

## 4.2 `cleandoc(doc: str) -> str`

* Normalizes indentation/blank lines/tabs in docstrings. ([Python documentation][1])

## 4.3 `getcomments(object) -> str | None`

* Returns comments immediately preceding the object’s source (or top-of-file comments for modules).
* Returns `None` if source unavailable (e.g., defined in C or interactive shell). ([Python documentation][1])

## 4.4 `getfile(object) -> str` (or raises)

* Returns the name of the **file (text or binary)** where the object was defined.
* Raises `TypeError` for built-in module/class/function. ([Python documentation][1])

## 4.5 `getsourcefile(object) -> str | None` (or raises)

* Returns Python source filename, or `None` if no way to identify source.
* Raises `TypeError` for built-in module/class/function. ([Python documentation][1])

## 4.6 `getmodule(object) -> module | None`

* “Best effort” module inference; returns `None` if undetermined. ([Python documentation][1])

## 4.7 `getsourcelines(object) -> (list[str], int)` (or raises)

Accepts module/class/method/function/traceback/frame/code object.

* Returns: `(lines, starting_lineno)`
* Raises `OSError` if source can’t be retrieved.
* Raises `TypeError` for built-in module/class/function. ([Python documentation][1])

## 4.8 `getsource(object) -> str` (or raises)

Same acceptance + error behavior as `getsourcelines`, but returns one string. ([Python documentation][1])

**Crystal-clear rule:**

* **Builtins in C** → `TypeError` for `getfile/getsourcefile/getsource/getsourcelines`. ([Python documentation][1])
* **No retrievable source** (interactive, missing file, etc.) → usually `OSError` for `getsource/getsourcelines`, `None` for `getcomments`. ([Python documentation][1])

---

# 5) The callable model: `signature()` and its emitted objects

This is the “modern” introspection surface.

## 5.1 `inspect.signature(callable, *, follow_wrapped=True, globals=None, locals=None, eval_str=False) -> Signature`

### Inputs it supports

* “Wide range of callables”: functions, classes, `functools.partial()`, etc. ([Python documentation][1])

### Outputs

* Returns a **`Signature` object** (structured, immutable). ([Python documentation][1])

### Raises / failure modes

* Raises `ValueError` if no signature can be provided.
* Raises `TypeError` if object type not supported. ([Python documentation][1])
* Some builtins implemented in C may provide no argument metadata → signature may fail or be partial. ([Python documentation][1])

### Wrapper/annotation behavior (the part that changes outputs)

* If annotations are **stringized** (e.g., `from __future__ import annotations`), `signature()` tries to **un-stringize** using `get_annotations()`, passing through `globals/locals/eval_str`. ([Python documentation][1])
* If `eval_str` is not false, un-stringizing may call `eval()` and can raise arbitrary exceptions. ([Python documentation][1])
* `follow_wrapped=False` prevents unwrapping via `__wrapped__`. ([Python documentation][1])
* CPython may consult `__signature__` if present, but the semantics are explicitly an implementation detail. ([Python documentation][1])

---

## 5.2 `Signature` object (what it contains, exactly)

Key attributes/methods:

* `parameters`: ordered mapping name → `Parameter` (strict definition order) ([Python documentation][1])
* `return_annotation` and sentinel `Signature.empty` ([Python documentation][1])
* `.bind(*args, **kwargs)` / `.bind_partial(...)` → `BoundArguments` or raises `TypeError` ([Python documentation][1])
* `.replace(...)` → new Signature (immutability) ([Python documentation][1])
* `.format(*, max_width=None)` → string formatting helper **added in 3.13**; breaks long signatures into multiple lines when needed. ([Python documentation][1])
* `Signature.from_callable(...)` classmethod: builds a Signature (or subclass) for a callable; same behavior as `signature()`. ([Python documentation][1])

---

## 5.3 `Parameter` object (your per-argument “row”)

Fields:

* `name` (string), `default`, `annotation`, `kind`, sentinel `Parameter.empty` ([Python documentation][1])
* `kind` enum values and meanings:

  * `POSITIONAL_ONLY`, `POSITIONAL_OR_KEYWORD`, `VAR_POSITIONAL`, `KEYWORD_ONLY`, `VAR_KEYWORD` ([Python documentation][1])
* `.replace(...)` makes a modified copy; to remove defaults/annotations pass `Parameter.empty`. ([Python documentation][1])

Important CPython detail you’ll see in real indexing:

* Comprehensions / genexps have implicit arg names on code objects; `inspect` exposes these as `implicit0`, etc. ([Python documentation][1])

---

## 5.4 `BoundArguments` object (the result of binding)

Produced by `Signature.bind(...)` or `.bind_partial(...)`.

* `arguments`: mutable mapping param name → bound value; contains only explicitly bound args by default ([Python documentation][1])
* `args`: tuple of positional values (computed from `arguments`) ([Python documentation][1])
* `kwargs`: dict of keyword values (computed from `arguments`) ([Python documentation][1])
* `signature`: reference back to the Signature ([Python documentation][1])
* `.apply_defaults()`: fills defaults for missing args; uses empty tuple/dict defaults for `*args/**kwargs`. ([Python documentation][1])

**Varying circumstance you must account for:**
Arguments satisfied via defaults are **not included** in `.arguments` unless you call `.apply_defaults()`. ([Python documentation][1])

---

# 6) “Legacy” callable APIs (still documented in 3.13)

These are often used for compatibility, debugging, or bridging older tooling.

## 6.1 `getfullargspec(func) -> FullArgSpec(...)`

Returns named tuple:
`FullArgSpec(args, varargs, varkw, defaults, kwonlyargs, kwonlydefaults, annotations)` ([Python documentation][1])

Key “varying circumstances” notes:

* Recommended API is `signature()`; `getfullargspec` retained mainly for Python 2-style compatibility. ([Python documentation][1])
* It is based on `signature()`, **but** still ignores `__wrapped__` and **includes the already-bound first parameter for bound methods**. ([Python documentation][1])

That last line is exactly why you can see different outputs between:

* `signature(instance.method)` vs `getfullargspec(instance.method)` (if you’re comparing in tooling).

## 6.2 `getcallargs(func, /, *args, **kwds) -> dict` (deprecated)

* Returns dict mapping argument names (including `*` / `**` names) to values as-if the function were called.
* For bound methods, binds the first argument (typically `self`) to the instance.
* Raises the same type of exception you’d get by actually calling the function with bad args.
* Deprecated since 3.5 in favor of `Signature.bind` / `.bind_partial`. ([Python documentation][1])

---

# 7) Closure / free-name resolution

## `getclosurevars(func) -> ClosureVars(nonlocals, globals, builtins, unbound)`

* Returns a named tuple mapping external name references to their current values:

  * `nonlocals`: lexical closure variables
  * `globals`: module globals
  * `builtins`: visible builtins
  * `unbound`: referenced names that can’t be resolved given current globals/builtins
* Raises `TypeError` if not a Python function or method. ([Python documentation][1])

This is the “runtime value view” that complements `symtable`’s “compile-time scope classification.”

---

# 8) Wrappers

## `unwrap(func, *, stop=None) -> object`

* Follows the `__wrapped__` chain until the last object (or until `stop(obj)` returns true).
* `signature()` uses a stop condition to halt unwrapping when an object has `__signature__`. ([Python documentation][1])
* Raises `ValueError` if a cycle is encountered. ([Python documentation][1])

---

# 9) Annotations (and why `get_annotations` changes outcomes)

## `get_annotations(obj, *, globals=None, locals=None, eval_str=False) -> dict`

* Accepts only callable/class/module; else `TypeError`. ([Python documentation][1])
* Always returns a **fresh dict**; calling twice returns distinct-but-equivalent dicts. ([Python documentation][1])
* Returns `{}` if no annotations dict; ignores inherited class annotations if class lacks its own. ([Python documentation][1])
* If `eval_str=True`, string annotations are evaluated with `eval()` and exceptions propagate. ([Python documentation][1])
* Default `globals/locals` depend on `type(obj)`:

  * module → `globals = obj.__dict__`
  * class → `globals = sys.modules[obj.__module__].__dict__`, `locals = class namespace`
  * callable → `globals = obj.__globals__`, after unwrapping if it was wrapped via `functools.update_wrapper()` ([Python documentation][1])

**This matters for signatures** because `signature()` may call `get_annotations()` to un-stringize annotations, which can change the annotation objects you see (or raise exceptions) depending on `eval_str/globals/locals`. ([Python documentation][1])

---

# 10) The interpreter stack: frames, tracebacks, and what those objects contain

## 10.1 `FrameInfo` and `Traceback` objects (emitted data model)

Many stack APIs return `FrameInfo` objects; `getframeinfo` returns a `Traceback` object. ([Python documentation][1])

### `FrameInfo` fields

* `frame`: the frame object
* `filename`
* `lineno`
* `function`
* `code_context`: list of source lines around current line
* `index`: index of current line in `code_context`
* `positions`: a `dis.Positions` span for the *currently executing instruction* ([Python documentation][1])

Back-compat behavior:

* `FrameInfo` objects allow tuple-like operations for older code, except `positions`; this is deprecated. ([Python documentation][1])

### `Traceback` fields

* `filename`, `lineno`, `function`, `code_context`, `index`, `positions` (same shape for the point associated with the traceback frame) ([Python documentation][1])

## 10.2 Stack APIs and their exact outputs

* `getframeinfo(frame_or_tb, context=1) -> Traceback` ([Python documentation][1])
* `getouterframes(frame, context=1) -> list[FrameInfo]` (frame → callers up the stack) ([Python documentation][1])
* `getinnerframes(traceback, context=1) -> list[FrameInfo]` (tb → calls inward to where exception raised) ([Python documentation][1])
* `currentframe() -> frame | None`

  * CPython detail: returns `None` if the implementation lacks Python stack frame support. ([Python documentation][1])
* `stack(context=1) -> list[FrameInfo]` (caller’s stack) ([Python documentation][1])
* `trace(context=1) -> list[FrameInfo]` (stack between current frame and where currently handled exception was raised) ([Python documentation][1])

## 10.3 The memory-leak footgun (frames create reference cycles)

Holding frame references can create cycles and extend object lifetimes; docs recommend breaking cycles via `del frame` in a `finally` or by calling `frame.clear()` if you must keep it. ([Python documentation][1])

---

# 11) Fetching attributes statically (passive inspection)

## `getattr_static(obj, attr, default=None) -> value`

* Like `getattr`, but avoids executing code triggered by:

  * descriptor protocol
  * `__getattr__`, `__getattribute__` ([Python documentation][1])
* Same limitation pattern as `getmembers_static`:

  * may miss dynamic attrs
  * may return descriptor objects rather than resolved values ([Python documentation][1])
* Specific gotchas:

  * if instance `__dict__` is shadowed by another member (e.g., property), it can’t find instance members ([Python documentation][1])
  * it does not resolve built-in descriptors (slot/getset/wrapper descriptors); you may need to manually call `__get__` on known descriptor types, with the usual “could execute code” caveat. ([Python documentation][1])

---

# 12) Generator / coroutine / asyncgen state + locals

## State APIs (return *state constants*)

* `getgeneratorstate(gen)` → one of `GEN_CREATED`, `GEN_RUNNING`, `GEN_SUSPENDED`, `GEN_CLOSED` ([Python documentation][1])
* `getcoroutinestate(coro)` → `CORO_CREATED`, `CORO_RUNNING`, `CORO_SUSPENDED`, `CORO_CLOSED`

  * accepts coroutine-like objects with `cr_running` and `cr_frame` ([Python documentation][1])
* `getasyncgenstate(agen)` → `AGEN_CREATED`, `AGEN_RUNNING`, `AGEN_SUSPENDED`, `AGEN_CLOSED`

  * accepts asyncgen-like objects with `ag_running` and `ag_frame` ([Python documentation][1])

## Locals APIs (return dicts)

* `getgeneratorlocals(gen) -> dict[name, value]`

  * returns `{}` if generator has no associated frame
  * `TypeError` if not a Python generator
  * CPython detail: in implementations without frame exposure, always returns `{}` ([Python documentation][1])
* `getcoroutinelocals(coro) -> dict` (analogous to generator locals) ([Python documentation][1])
* `getasyncgenlocals(agen) -> dict` (analogous; added 3.12) ([Python documentation][1])

---

# 13) Code object bit flags + BufferFlags

## Code object flags: `CO_*` constants

`inspect` exposes CPython code object `co_flags` bits (e.g., generator/coroutine/async-gen markers). Docs warn these flags are CPython-specific/implementation detail and recommend using higher-level inspect APIs. ([Python documentation][1])

## `BufferFlags` (enum.IntFlag)

`inspect.BufferFlags` is an `IntFlag` for flags passed to `__buffer__()` for objects implementing the buffer protocol; added in 3.12. ([Python documentation][1])

---

# 14) A “choose the right API” cheat sheet (for deterministic tooling)

### If you are building an indexer / doc tool and must avoid executing code

* Enumerate: `getmembers_static`
* Fetch single attr: `getattr_static`
* Detect descriptors: `ismethoddescriptor` / `isdatadescriptor` / `isgetsetdescriptor` / `ismemberdescriptor` ([Python documentation][1])

### If you want a callable’s structured signature (recommended)

* `signature()` → `Signature(parameters: Parameter...)`
* bind arguments: `sig.bind(...)` → `BoundArguments`
* if you need defaults included: `ba.apply_defaults()` ([Python documentation][1])

### If you want annotations safely (and predictably)

* `get_annotations(obj, eval_str=False)` (fresh dict, no eval)
* only set `eval_str=True` in trusted contexts (since it may eval/raise) ([Python documentation][1])

### If you need runtime execution context mapping

* `stack()` / `trace()` / `getouterframes()` → list of `FrameInfo`
* use `.positions` (a `dis.Positions`) to connect frame state to bytecode/source spans
* break cycles (`del frame` / `frame.clear()`) ([Python documentation][1])

---

If you want, next I can do what we did for `dis`: a **fully pinned “inspect → facts” contract** (schemas + normalization rules) that explicitly covers the tricky cases you’ll hit in real repos: wrappers (`__wrapped__` / `__signature__`), stringized annotations, builtins with missing metadata, and safe static attribute enumeration that preserves descriptors rather than executing them.

[1]: https://docs.python.org/3.13/library/inspect.html "inspect — Inspect live objects — Python 3.13.11 documentation"

Absolutely — `symtable` is small on the surface, but *dense* in meaning. Below is a **“crystal clear”** deep dive on **what you can call**, **what objects you get back**, and **how the outputs change** depending on code shape (module vs function vs class vs new 3.12/3.13 typing scopes, etc.).

Everything here is anchored in the stdlib docs for `symtable` (and I’ll explicitly call out a couple of items that are **3.14+** so you can ignore them for **3.13**). ([Python documentation][1])

---

# 0) Mental model: what `symtable` *is* (and what it is not)

### What it is

* A view into **the compiler’s symbol tables**, generated from the **AST** *just before bytecode is generated*. ([Python documentation][1])
* Its job is to decide, for every identifier occurrence, **what scope category** it belongs to (local/global/nonlocal/free/etc.) and whether it is **assigned / referenced / imported / annotated**, plus whether a binding introduces a **new namespace** (def/class). ([Python documentation][1])

### What it is not

* It does **not** evaluate code.
* It does **not** compute “runtime name resolution” in the dynamic sense (imports executed, `globals()` contents, monkeypatching).
* It does **not** give you a full def-use graph; it gives you **compile-time scope classification**.

---

# 1) Output shapes: what `symtable` can emit

`symtable` has basically **two output modes**:

## Shape A — Text dump (CLI)

* `python -m symtable [infile...]`
* Reads files (or stdin if none) and **dumps symbol tables to stdout**. ([Python documentation][1])

## Shape B — Structured objects (programmatic API)

* `symtable.symtable(...)` → a **top-level `SymbolTable` object** that you traverse. ([Python documentation][1])

From there you get:

* `SymbolTableType` enum values ([Python documentation][1])
* `SymbolTable` / `Function` / `Class` objects ([Python documentation][1])
* `Symbol` objects for identifiers ([Python documentation][1])
* Tuples/lists of strings (names), plus “view objects” for identifiers ([Python documentation][1])

---

# 2) Command-line usage (3.13+)

## `python -m symtable [infile...]`

* **Added in 3.13**. ([Python documentation][1])
* Behavior:

  * If files are given: generate symbol tables for each and dump to stdout
  * If no files: read source from stdin and dump to stdout ([Python documentation][1])

### What the CLI output *is*

It’s a human dump of the internal structure. The docs don’t promise a stable textual format — treat it as a debugging aid, not an interchange format.

---

# 3) Programmatic entry point: `symtable.symtable(...)`

## `symtable.symtable(code, filename, compile_type) -> SymbolTable`

* Returns the **toplevel** `SymbolTable` for the given Python source code. ([Python documentation][1])
* `compile_type` is like the `mode=` argument to `compile()` (e.g., `"exec"`, `"eval"`, `"single"`). ([Python documentation][1])

### What can vary with `compile_type`

This matters because it changes:

* which grammar is allowed (statements vs expression-only)
* what the **top-level block “means”** (still a top-level table, but content differs)

### What exceptions to expect

The docs don’t list them explicitly here, but in practice this path uses the compiler front-end:

* syntax errors in `code` → `SyntaxError`
* invalid `compile_type` → typically `ValueError`/`TypeError`

---

# 4) The data model: every object you can get back (and what its methods return)

## 4.1 `SymbolTableType` (enum): what kinds of blocks exist

`SymbolTable.get_type()` returns a **member of `SymbolTableType`** (since 3.13). ([Python documentation][1])

### Core block kinds

* `MODULE`, `FUNCTION`, `CLASS` ([Python documentation][1])

### “Annotation scope flavors” (modern typing/language features)

The docs explicitly group these as “different flavors of annotation scopes”: ([Python documentation][1])

* `ANNOTATION`: used for annotations when `from __future__ import annotations` is active ([Python documentation][1])
* `TYPE_ALIAS`: used for `type` statement constructions ([Python documentation][1])
* `TYPE_PARAMETERS`: used for generic functions or classes ([Python documentation][1])
* `TYPE_VARIABLE`: used for the bound/constraints/default value scope for a single type variable (TypeVar / TypeVarTuple / ParamSpec); **added in 3.13** ([Python documentation][1])

> If you’re building an indexer: treat non-{MODULE, FUNCTION, CLASS} tables as **meta-scopes** (still important for name binding, but not “runtime blocks” you call).

---

## 4.2 `SymbolTable` (base class): a namespace table for a block

A `SymbolTable` represents a single block scope; you don’t construct it directly. ([Python documentation][1])

### Identity & classification

* `get_type() -> SymbolTableType`

  * **3.13 change:** return values are enum members, not strings ([Python documentation][1])
* `get_id() -> int` (table identifier) ([Python documentation][1])
* `get_name() -> str`

  * class block: class name
  * function block: function name
  * module/global table: `'top'`
  * type-parameter scopes: name of underlying class/function/type alias
  * type-alias scopes: name of the type alias
  * TypeVar “bound scope” naming is also described in the docs (historical/typing scope nuance) ([Python documentation][1])
* `get_lineno() -> int` (first line of the block) ([Python documentation][1])

### Structural booleans (very important for interpretation)

* `is_optimized() -> bool`
  “True if the locals in this table can be optimized.” ([Python documentation][1])
  *Typical mental model:* function-like scopes tend to be optimized; class/module behave differently (class body uses a mapping-like local namespace).
* `is_nested() -> bool`
  “True if the block is a nested class or function.” ([Python documentation][1])
* `has_children() -> bool`
  “True if the block has nested namespaces.” ([Python documentation][1])

### Symbol enumeration & lookup

* `get_identifiers() -> view`
  A view object containing the **names** in the table. ([Python documentation][1])
* `lookup(name) -> Symbol`
  Lookup a name and return a `Symbol` instance. ([Python documentation][1])
* `get_symbols() -> list[Symbol]`
  All symbols as `Symbol` objects. ([Python documentation][1])

### Nesting

* `get_children() -> list[SymbolTable]`
  Returns nested symbol tables (functions/classes/typing meta-scopes) inside this block. ([Python documentation][1])

---

## 4.3 `Function` (subclass of SymbolTable): function/method scopes

If `table.get_type() == FUNCTION`, you can treat it as `symtable.Function` and it supports:

* `get_parameters() -> tuple[str, ...]` ([Python documentation][1])
* `get_locals() -> tuple[str, ...]` ([Python documentation][1])
* `get_globals() -> tuple[str, ...]` ([Python documentation][1])
* `get_nonlocals() -> tuple[str, ...]` (explicit `nonlocal` declarations) ([Python documentation][1])
* `get_frees() -> tuple[str, ...]` (free/closure variables) ([Python documentation][1])

**Interpretation rule:** `get_frees()` is how you detect what the compiler thinks is captured from outer scopes.

---

## 4.4 `Class` (subclass of SymbolTable): class body scopes

If `table.get_type() == CLASS`, you can treat it as `symtable.Class` and it supports:

* `get_methods() -> tuple[str, ...]`
  Returns “method-like functions declared in the class body” via `def` or `async def`; **does not include deeper scopes**. ([Python documentation][1])

> Note: docs in newer versions mark `get_methods()` as deprecated (3.14+), but you asked for **3.13** and it’s part of the surfaced API there.

---

## 4.5 `Symbol`: per-identifier facts inside a table

A `Symbol` is “an entry in a SymbolTable corresponding to an identifier in the source”. ([Python documentation][1])

### Identity

* `get_name() -> str` ([Python documentation][1])

### Usage / origin flags

* `is_referenced() -> bool`
  “True if the symbol is used in its block.” ([Python documentation][1])
* `is_assigned() -> bool`
  “True if the symbol is assigned to in its block.” ([Python documentation][1])
* `is_imported() -> bool`
  “True if the symbol is created from an import statement.” ([Python documentation][1])
* `is_annotated() -> bool`
  “True if the symbol is annotated.” ([Python documentation][1])

### Scope classification flags

* `is_parameter() -> bool` ([Python documentation][1])
* `is_local() -> bool`
  “True if the symbol is local to its block.” ([Python documentation][1])
* `is_global() -> bool`
  “True if the symbol is global.” ([Python documentation][1])
* `is_declared_global() -> bool`
  “True if the symbol is declared global with a global statement.” ([Python documentation][1])
* `is_nonlocal() -> bool`
  “True if the symbol is nonlocal.” ([Python documentation][1])
* `is_free() -> bool`
  “True if the symbol is referenced in its block, but not assigned to.” ([Python documentation][1])

### Namespace binding (“def/class makes a namespace”)

* `is_namespace() -> bool`
  True if the name binding introduces a new namespace (e.g., function/class statement). ([Python documentation][1])
* `get_namespaces() -> list[SymbolTable]`
  List of namespaces bound to this name. ([Python documentation][1])
* `get_namespace() -> SymbolTable`
  Returns the single bound namespace; **raises ValueError** if more than one (or none) is bound. ([Python documentation][1])

> **Important nuance:** a single name can be bound to multiple objects; if `is_namespace()` is True, the name may also be bound to *other non-namespace values* (ints/lists/etc.). ([Python documentation][1])

### 3.14+ methods you should ignore for 3.13

If you happen to see these in newer docs, they are **not part of the 3.13 surface**:

* `is_type_parameter()` (3.14+) ([Python documentation][1])
* `is_free_class()` / comprehension-specific flags (3.14+) ([Python documentation][1])

---

# 5) “How to read” symtable results: practical interpretation rules

This is where most confusion happens, so here are concrete rules that make outputs predictable.

## 5.1 The scope flags are *block-relative*

A symbol being “global” is a property **in that table’s block**, not an absolute statement about the whole program. So `is_global()` is most meaningful inside **function blocks**, where the compiler is deciding whether `LOAD_FAST` vs `LOAD_GLOBAL`-style behavior applies.

## 5.2 Free vars vs locals: how to detect closure capture

A variable `x` is “captured” when:

* In an inner function’s table: `x` appears in `get_frees()` (and usually `lookup("x").is_free()` is true) ([Python documentation][1])
* In an outer function’s table: `x` will show up as local/assigned, and you infer “cell-ness” because it’s free in a child

`symtable` doesn’t directly expose “cell variable” as a separate flag in 3.13; you infer it via child freevar sets.

## 5.3 “Namespace binding” is how you jump from name → child table

If `table.lookup("f").is_namespace()` is True, then `table.lookup("f").get_namespace()` gives you the nested `SymbolTable` for that `def f...` (unless multiple). ([Python documentation][1])

That’s the *core mechanism* for building a lexical scope graph.

---

# 6) Varying circumstances: what changes depending on code shape

Below are the major “situations” and what you should expect the object graph to look like.

## 6.1 Module (top-level) code

* `symtable.symtable(..., compile_type="exec")` returns a **top table** whose `get_name()` is `'top'`. ([Python documentation][1])
* `get_type()` is `SymbolTableType.MODULE`. ([Python documentation][1])
* `get_children()` includes nested defs/classes/type scopes inside the module. ([Python documentation][1])

## 6.2 Function blocks

* `get_type() == FUNCTION` and you can call `get_parameters/get_locals/get_globals/get_nonlocals/get_frees`. ([Python documentation][1])
* `get_frees()` is the “closure capture” list. ([Python documentation][1])

## 6.3 Nested functions

* Inner function table: `is_nested() == True` ([Python documentation][1])
* Outer table: `has_children() == True`, and `get_children()` contains the inner function table(s). ([Python documentation][1])

## 6.4 Class blocks (class body semantics)

* `get_type() == CLASS`
* class bodies are special in Python (locals behave differently than in functions); `symtable` reflects that through the table type + `is_optimized()` differences. ([Python documentation][1])
* `get_methods()` enumerates “method-like defs” in the class body but not deeper ones. ([Python documentation][1])

## 6.5 `global` and `nonlocal`

* A name marked `is_declared_global()` means the compiler saw a `global x` statement in that block. ([Python documentation][1])
* A name marked `is_nonlocal()` means the compiler saw a `nonlocal x` statement in that block. ([Python documentation][1])

**Operationally**:

* `global` forces name resolution to module scope for that function block
* `nonlocal` forces name resolution to an enclosing function’s scope

## 6.6 Imports

* `is_imported()` tells you the symbol was introduced by `import ...` in that block. ([Python documentation][1])

This is useful for indexers: you can label “definition site is import statement” vs assignment/param.

## 6.7 Annotations

* `is_annotated()` reflects a binding being annotated (e.g., `x: int`, function parameter annotations, etc.). ([Python documentation][1])
* In modern typing modes, annotations can also induce **annotation scopes** at the symbol-table level (see `SymbolTableType.ANNOTATION`). ([Python documentation][1])

## 6.8 Type parameter / type alias / type variable scopes (3.12/3.13 era)

This is the big “new” category where people get surprised by extra symbol tables.

You may see `SymbolTableType` values such as:

* `TYPE_PARAMETERS` for `def f[T](...)` / `class C[T]: ...` generic scopes ([Python documentation][1])
* `TYPE_ALIAS` for `type Alias = ...` ([Python documentation][1])
* `TYPE_VARIABLE` for TypeVar bound/constraints/default scope; **added 3.13** ([Python documentation][1])

These tables typically show up in `get_children()` and their `get_name()` is defined to reflect the underlying construct (class/function/type alias) or the specific type variable name, depending on scope kind. ([Python documentation][1])

---

# 7) “Behavior matrix”: which call to use for which question

## “I need the scope graph (blocks + nesting)”

* Call `symtable.symtable(...)` → top `SymbolTable` ([Python documentation][1])
* Traverse:

  * `table.get_children()` recursively ([Python documentation][1])
  * Use `table.get_type()` to tag nodes ([Python documentation][1])

## “I need all identifiers in this block”

* `table.get_identifiers()` (names only) ([Python documentation][1])
* or `table.get_symbols()` (full `Symbol` objects) ([Python documentation][1])

## “For a name, what’s its classification in this block?”

* `sym = table.lookup(name)` ([Python documentation][1])
* then:

  * assignment/reference/import/annotation flags: `is_assigned/is_referenced/is_imported/is_annotated` ([Python documentation][1])
  * scope flags: `is_local/is_global/is_declared_global/is_nonlocal/is_free/is_parameter` ([Python documentation][1])

## “This name is a def/class — how do I reach its nested table?”

* `sym.is_namespace()` tells you if it introduces a namespace (def/class) ([Python documentation][1])
* `sym.get_namespace()` returns the single namespace or raises ValueError if ambiguous ([Python documentation][1])
* `sym.get_namespaces()` gives you all bound namespaces ([Python documentation][1])

---

# 8) A canonical “symtable report” extractor (indexer-friendly)

This is the minimal pattern that never surprises you:

```python
import symtable
from dataclasses import dataclass

@dataclass(frozen=True)
class TableRow:
    tid: int
    kind: str
    name: str
    lineno: int
    optimized: bool
    nested: bool

@dataclass(frozen=True)
class SymbolRow:
    tid: int
    name: str
    referenced: bool
    assigned: bool
    imported: bool
    annotated: bool
    param: bool
    local: bool
    global_: bool
    declared_global: bool
    nonlocal_: bool
    free: bool
    namespace: bool

def extract_symtable(code: str, filename: str = "<string>", mode: str = "exec"):
    top = symtable.symtable(code, filename, mode)

    tables: list[TableRow] = []
    symbols: list[SymbolRow] = []

    def walk(t):
        tables.append(TableRow(
            t.get_id(), t.get_type().name, t.get_name(), t.get_lineno(),
            t.is_optimized(), t.is_nested(),
        ))
        for s in t.get_symbols():
            symbols.append(SymbolRow(
                t.get_id(), s.get_name(),
                s.is_referenced(), s.is_assigned(), s.is_imported(), s.is_annotated(),
                s.is_parameter(), s.is_local(), s.is_global(), s.is_declared_global(),
                s.is_nonlocal(), s.is_free(), s.is_namespace(),
            ))
        for ch in t.get_children():
            walk(ch)

    walk(top)
    return tables, symbols
```

This gives you two clean datasets:

* table facts (block graph nodes)
* symbol facts (identifier rows keyed by table id)

And it uses only APIs described in the docs above. ([Python documentation][1])

---

If you want one more increment (in the same spirit as your `dis` deep dive): I can write a **“symtable semantics cookbook”** that enumerates ~20 micro-programs (`global`, `nonlocal`, nested classes, comprehensions, annotations, type parameters) and for each one lists **exactly which Symbol flags flip** and which child tables appear (including the 3.13 `TYPE_VARIABLE` cases).

[1]: https://docs.python.org/3/library/symtable.html "symtable — Access to the compiler’s symbol tables — Python 3.14.2 documentation"

Below is the “one level deeper” artifact: a **fully pinned `dis → Arrow tables` extraction contract** (schemas + normalization + stable keys), followed by **exactly how those tables become CFG/DFG/PDG edges inside your CPG build** (plugging into the layered pipeline you described). 

---

# 1) What “fully pinned dis → Arrow” means

A pinned contract is:

* **Deterministic inputs** (compile/disassemble in a repeatable way)
* **Stable identifiers** (so you can diff outputs across Python upgrades and across runs)
* **Lossless physical detail** for tricky bytecode realities:

  * `EXTENDED_ARG` prefix span (`start_offset`)
  * inline caches (`cache_offset/end_offset/cache_info`)
  * jump/handler **labels** that don’t depend on “pretty printing”
* **Explicit normalization rules** so you never argue about what a column means

This yields a **bytecode layer** that you can:

1. join back to AST/CST spans (byte offsets),
2. compute **CFG**, then **DFG** (reaching defs), then
3. project results into your CPG node/edge tables. 

---

# 2) Deterministic compilation & code-unit enumeration

## 2.1 Compilation knobs (pin the input)

You need a canonical “compiler boundary” per file:

* **Source bytes are canonical**: treat the *exact* file bytes (encoding/newlines) as the source of truth.
* Compile with a pinned mode:

  * module: `compile(src, filename, "exec", dont_inherit=True, optimize=0)`
  * expression/snippet use is optional; for repo files stick to `"exec"`.

Record these run facts once per run:

* `python_version` (`3.13.x`)
* `implementation` / `cache_tag`
* `bytecode_magic` (e.g., `importlib.util.MAGIC_NUMBER`)
* `compile_optimize` (0)
* `compile_dont_inherit` (true)

Those are your “environment keys” for drift tracking.

## 2.2 Code units (the root of everything)

You must treat every `types.CodeType` as a **code unit**:

* module top-level code object
* each `def` / `async def`
* lambdas
* comprehensions (they’re code objects)
* annotation/type scopes may also produce code objects in modern Python

The code unit table is the “parent index”; every instruction row must reference it.

---

# 3) Arrow tables: schemas + meaning

I’ll give you **a minimal set** that is both:

* *complete enough* to rebuild CFG/DFG correctly, and
* *stable enough* to diff across upgrades.

You can store these as Parquet with Arrow schemas; DuckDB can query them directly.

## 3.1 Table: `py_bc_code_units` (one row per code object)

**Purpose**: stable identity + metadata + nested structure.

**Primary key**: `code_unit_id`
**Foreign keys**: `file_id`, `parent_code_unit_id`

### Columns (recommended)

* `schema_version: int16`
* `file_id: string` (stable hash of repo-relative path + file content hash)
* `path: string`
* `code_unit_id: string` (**stable**; see §4)
* `parent_code_unit_id: string?`
* `qualpath: string` (your own stable path: `module::Class::func::<lambda#2>::<listcomp#1>`)
* `co_name: string`
* `co_qualname: string?` (if you choose to store it)
* `kind: string` enum: `module|function|class_body|lambda|comprehension|annotation|type_scope|other`
* `co_firstlineno: int32`
* `span_start_byte: int64?` (optional but *highly* recommended once you have AST/CST join)
* `span_end_byte: int64?`
* `flags: int32` (co_flags)
* `argcount: int16`, `posonlyargcount: int16`, `kwonlyargcount: int16`
* `nlocals: int32`, `stacksize: int32`
* `varnames: list<string>`
* `names: list<string>`
* `freevars: list<string>`
* `cellvars: list<string>`
* `bytecode_len: int32` (len(co_code))
* `exceptiontable_len: int32`
* `python_version: string`
* `bytecode_magic: binary`

**Why this matters for CPG**: code units become your **CFG domains** (per function/module), and their `qualpath/span` becomes your **stable anchor** for joins back to syntax. 

---

## 3.2 Table: `py_bc_instructions` (one row per instruction)

**Purpose**: the canonical structured facts you get from `dis.get_instructions(..., adaptive=False)` + computed span/labels.

You store both:

* **physical layout** (offsets, ext-arg span, caches)
* **semantic normalized identity** (base opcode family + stable join anchor)

### Columns (recommended)

**Identity & stability**

* `schema_version: int16`
* `code_unit_id: string`
* `instr_id: string` (**stable-ish**, see §4)
* `instr_physical_id: string` (`code_unit_id:OFF:{start_offset}`) (purely physical)
* `instr_index: int32` (0..N-1, in iteration order)

**Physical span in `co_code`**

* `start_offset: int32` (includes any `EXTENDED_ARG` prefixes)
* `offset: int32` (the opcode’s own offset)
* `cache_offset: int32` (start of inline caches after op)
* `end_offset: int32` (end of cache region)
* `ext_arg_len: int16` (= `offset - start_offset`)
* `op_len: int16` (= `cache_offset - offset`)
* `cache_len: int16` (= `end_offset - cache_offset`)

**Opcode identity**

* `opcode: int16`
* `opname: string`
* `baseopcode: int16`
* `baseopname: string`

**Operand**

* `arg: int32?`
* `argrepr: string`
* `argval_kind: string` (e.g., `str|int|float|none|bytes|tuple|code|other`)
* `argval_str: string?` (for `LOAD_* name`, attribute names, etc.)
* `argval_int: int64?` (for numeric constants / indices)
* `argval_repr: string` (repr of argval, always present)

*(You can optionally add an `argval_code_unit_id` if argval is a nested code object you also emitted into `py_bc_code_units`.)*

**Control-flow**

* `is_jump_target: bool`
* `jump_target_offset: int32?`
* `jump_target_label: string?` (your normalized label; see §4)
* `label: string?` (if *this* instruction’s offset is labeled)

**Source mapping (for AST/CST joins)**

* `starts_line: bool`
* `line_number: int32?`
* `pos: struct<lineno:int32?, end_lineno:int32?, col:int32?, end_col:int32?>`
* `span_start_byte: int64?` (computed via file line-map)
* `span_end_byte: int64?`

**Inline cache payload (lossless)**

* `cache_info: list<struct<name:string, size:int16, data:binary>>?`
* `cache_bytes: binary?` (slice `co_code[cache_offset:end_offset]`)
* `op_bytes: binary?` (slice `co_code[start_offset:cache_offset]`) *(optional but great for debugging)*

**Why this matters for CPG**:

* `jump_target_* + exception table` ⇒ CFG
* `pos/span_*` ⇒ join bytecode steps back to syntax nodes
* `baseopname + argval_*` ⇒ def/use classification and call-site modeling
* `cache_* + start_offset` ⇒ correctness under `EXTENDED_ARG` + caches 

---

## 3.3 Table: `py_bc_exception_table` (one row per exception-range entry)

**Purpose**: exceptional control-flow regions (“try region → handler target”).

### Columns (recommended)

* `schema_version: int16`
* `code_unit_id: string`
* `exc_entry_index: int32`
* `start_offset: int32`
* `end_offset: int32`
* `target_offset: int32`
* `depth: int16`
* `lasti: bool`
* `start_label: string?`
* `end_label: string?`
* `target_label: string?`

**Why this matters**: without these edges, CFG for `try/except/finally/with` is wrong in the exact ways you care about most. 

---

## 3.4 (Derived but strongly recommended) Table: `py_bc_blocks` and `py_bc_cfg_edges`

You can build these deterministically from `instructions + exception_table`.

### `py_bc_blocks`

* `block_id: string`
* `code_unit_id: string`
* `start_offset: int32` (first instruction offset in block)
* `end_offset: int32` (end offset exclusive)
* `start_label: string?`
* `kind: string` (`entry|exit|normal|handler|finally|loop_header|merge|other`)
* `anchor_span_start_byte: int64?` (first non-null instruction span)
* `anchor_span_end_byte: int64?`

### `py_bc_cfg_edges`

* `edge_id: string`
* `code_unit_id: string`
* `src_block_id: string`
* `dst_block_id: string`
* `kind: string` (`next|true|false|jump|exc`)
* `cond_instr_id: string?` (for branch edges)
* `exc_entry_index: int32?` (for exc edges)

These map directly to the CFG layer described in your CPG build overview. 

---

# 4) Normalization rules (the “pinned” part)

## 4.1 Canonical instruction stream selection

**Rule**: your canonical dataset is always built with:

* `adaptive=False` (so opnames aren’t runtime-specialized)
* `show_caches` is irrelevant for `get_instructions` in 3.13; you rely on `cache_info`.

You *may* optionally store a second variant (`adaptive=True`) in a separate table or with a `variant` column, but it must not pollute correctness logic.

## 4.2 EXTENDED_ARG handling

**What you store**

* `start_offset` (includes `EXTENDED_ARG`)
* `offset` (actual opcode)
* `ext_arg_len = offset - start_offset`

**Normalization**

* Treat the **physical instruction span** as `[start_offset, end_offset)`.
* Treat the **logical opcode anchor** as `offset` (and `instr_index`) for control/dataflow semantics.

This is what makes “instruction bytes” slicing consistent even when arg widths change.

## 4.3 Cache handling (inline caches are first-class)

**What you store**

* `cache_offset`, `end_offset`
* `cache_len`
* `cache_info` triplets
* `cache_bytes`

**Normalization**

* Never infer jump math from raw offsets alone; always trust `Instruction.jump_target_offset` (resolved) and then map that offset to a label.

The caches exist precisely to break naive “offset arithmetic.”

## 4.4 Labels-vs-offsets (make your own labels)

3.13’s *text* disassembly prefers logical labels; you should implement that as a deterministic mapping:

**Label set construction**
For each code unit:

* all `jump_target_offset` values
* all exception table `{start_offset, end_offset, target_offset}`
* plus `0` and “end of code” boundaries if you want entry/exit labeling

**Label mapping**

* sort unique offsets ascending
* assign label IDs: `L0000`, `L0001`, …

**Store**

* `label` on instruction rows where `offset` is in the label map
* `jump_target_label` on jump instructions (and on exception entries)

Now:

* CFG edges can be emitted in terms of labels (stable within compilation)
* UI/debug views can show labels without depending on `dis` formatting

## 4.5 Stable cross-version keys (what “stable” can realistically mean)

You need **two IDs** everywhere:

### A) Physical IDs (always correct, not stable across Python upgrades)

* `instr_physical_id = f"{code_unit_id}:OFF:{start_offset}"`

Use these for:

* reconstructing exact CFG/DFG over *this* compiled artifact

### B) Stable IDs (best-effort stable across minor version changes)

These must *not* rely on bytecode offsets. Use source anchoring:

* Compute instruction **source span** in bytes: `(span_start_byte, span_end_byte)` from `pos` via a per-file line-map.
* Define `site_key = f"{code_unit_id}:{span_start_byte}:{span_end_byte}"` when spans exist.
* Within the same `site_key`, order instructions by `instr_index` and assign an ordinal.

**Stable-ish instruction id**

* `instr_id = f"{site_key}:{baseopname}:{ordinal}"`

Fallbacks when span is null:

* `instr_id = f"{code_unit_id}:NOSPAN:{instr_index}:{baseopname}"`

### Stable code unit keys

Prefer AST/CST join anchoring (since you already have byte spans):

* `code_unit_id = f"{file_id}:CU:{span_start_byte}:{span_end_byte}:{qualpath}"`

If span isn’t available (rare in your pipeline):

* fallback to `co_firstlineno + qualpath`, but mark `key_quality="weak"`.

This aligns exactly with your CPG overview’s insistence that byte offsets are the coordinate system for “index-grade” joins. 

---

# 5) How these Arrow tables plug into end-to-end CPG construction

Your CPG build overview is layered:
**Syntax → Symbols/Scopes → CFG → DFG → interprocedural stitching → enrichment**. 

The `dis → Arrow` contract is the “bytecode semantics” substrate for the **CFG/DFG layers**, and a high-fidelity source of **evaluation order**.

## 5.1 Where the bytecode layer enters your pipeline

You already have:

* AST/CST with byte spans
* AST↔CST join
* SCIP occurrences anchored to syntax spans 

Add **one new ingestion product**:

* `py_bc_*` tables above (code units, instructions, exception table, blocks, cfg edges)

This is computed per file (compile + traverse code objects) and is *purely static* (no execution).

## 5.2 Joining bytecode back to syntax

This is the key move that makes bytecode usable in a CPG:

1. From `py_bc_instructions.pos` build `(span_start_byte, span_end_byte)` in file bytes.
2. Join `(file_id, span_start_byte/span_end_byte)` against:

   * CST tokens (exact covering tokens)
   * AST nodes (smallest AST node whose span fully covers the instruction span)
3. Emit one of:

   * `BYTECODE_ANCHOR(instr_id → ast_node_id)`
   * or `BYTECODE_COVERS(ast_node_id → instr_id)` (direction doesn’t matter if you standardize)

Now every bytecode step can be navigated back to concrete syntax and to SCIP symbol edges already attached to those syntax nodes.

## 5.3 Building CFG from the bytecode tables

Use:

* `py_bc_blocks` as `CFG_BLOCK` nodes
* `py_bc_cfg_edges` as `CFG_*` edges

Then emit into your property-graph edge table as:

* `CFG_NEXT/CFG_TRUE/CFG_FALSE/CFG_EXC` edges, exactly as your overview describes. 

Why this is so effective in Python:

* `try/finally/with` correctness is *hard* to get right from AST alone; bytecode + exception table makes it mechanical.

## 5.4 Building DFG (reaching defs) using bytecode + bindings

Your overview says DFG needs:

* bindings (what a name means)
* CFG (how execution flows) 

You already build bindings from AST + (optionally) SCIP. Then:

1. Classify instruction rows into **DEF** and **USE** events:

   * `STORE_FAST/STORE_NAME/...` ⇒ DEF of a binding slot
   * `LOAD_FAST/LOAD_NAME/...` ⇒ USE of a binding slot
   * `LOAD_ATTR/STORE_ATTR` ⇒ USE/DEF of attribute slot (object-sensitive; you can start with best-effort)
   * `LOAD_SUBSCR/STORE_SUBSCR` ⇒ USE/DEF of subscript slot
2. Materialize:

   * `DF_DEF` / `DF_USE` nodes *or* treat instruction nodes as the def/use site directly.
3. Run reaching definitions over `py_bc_cfg_edges`.
4. Emit `REACHES(def → use)` edges into your CPG edges table, matching the recipe in the overview. 

## 5.5 Calls: stitching to SCIP symbols

Bytecode will show call sites (`CALL`-family semantics). You can:

* create `CALLSITE` nodes for instruction(s) that perform calls
* join the call’s “callee expression” back to syntax via `BYTECODE_ANCHOR`
* use SCIP `REFERS_TO` edges from that syntax node to a `SYMBOL`
* emit `CALLS(callsite → SYMBOL, confidence=high)` when SCIP resolves it

This matches the overview’s interprocedural “call graph stitching” layer. 

---

# 6) What you end up materializing as the CPG

Your final property graph tables (`nodes`, `edges`) absorb bytecode as just another layer:

### Nodes you add (beyond syntax/symbol/scope)

* `BC_CODE_UNIT` (from `py_bc_code_units`)
* `BC_INSTR` (from `py_bc_instructions`) *(or collapse into `EVAL_STEP`)*
* `CFG_BLOCK` (from `py_bc_blocks`)
* optional `DF_DEF` / `DF_USE` (or reuse `BC_INSTR`)

### Edges you add

* `BYTECODE_ANCHOR` (instr → ast/cst)
* `CFG_*` (from cfg edges)
* `STEP_DEF` / `STEP_USE` (instr → binding/slot)
* `REACHES` (def → use)
* `CALLS` (callsite → symbol)

This fits perfectly into the “two-table property graph” model your overview recommends (`nodes` + `edges`) and keeps every semantic edge traceable back to byte spans. 

---

# 7) Minimal validation checklist (so this is not a pile of rows)

To lock correctness and prevent regressions:

1. **Span join sanity**

* % of instructions with non-null `(span_start_byte, span_end_byte)`
* % anchored to an AST node (should be high for non-synthetic ops)

2. **CFG sanity**

* each code unit has exactly 1 entry/exit block
* all blocks reachable (or accounted for as dead code)

3. **Exception-edge sanity**

* every exception table target corresponds to a block start label
* handlers reachable via `CFG_EXC`

4. **DFG sanity**

* every `LOAD_*` either has ≥0 reaching defs or is flagged “unbound / global / unknown”

---

If you want, I can also provide **the exact algorithms** for:

* block boundary detection from labels + terminators,
* exception-edge projection from `py_bc_exception_table` into `CFG_EXC`,
* and a first “Python bytecode def/use classifier” table keyed by `baseopname` (so DFG starts deterministic, then you refine).

Below are the **exact, implementation-ready algorithms** for the three items:

1. **Basic-block boundary detection** from **labels + terminators**
2. **Exception-edge projection** (`py_bc_exception_table → CFG_EXC`)
3. A first, deterministic **bytecode def/use classifier** keyed by **`baseopname`** (plus how to apply it)

I’m assuming the Arrow contracts you already have:

* `py_bc_instructions` (per code unit; includes `offset/start_offset/cache_offset/end_offset/baseopname/jump_target_offset/instr_id/instr_index/...`)
* `py_bc_exception_table` (per code unit; includes `start_offset/end_offset/target_offset/depth/lasti/exc_entry_index/...`)

And this plugs directly into the CFG/DFG phase sequence in your CPG build overview (CFG first, then DFG over CFG). 

---

# A) Block boundary detection from labels + terminators

## A1) Definitions (so “basic block” is unambiguous)

A **basic block** is a maximal contiguous instruction range with:

* **one entry** (block start),
* **no internal jump targets** (every jump target starts a new block),
* **one exit** (last instruction is a terminator/branch, or control falls through to the next block).

We’ll build blocks over **instruction offsets** (`Instruction.offset`), not physical byte spans. Physical spans come from `[first.start_offset, last.end_offset)`.

## A2) Inputs you need per code unit

From `py_bc_instructions` sorted by `instr_index`:

* `offset` (logical instruction start)
* `start_offset` (includes EXTENDED_ARG prefixes)
* `end_offset` (includes inline caches)
* `baseopname`
* `jump_target_offset` (nullable)
* `instr_id` (your stable-ish id)
* `instr_index`

From `py_bc_exception_table`:

* `start_offset`, `end_offset`, `target_offset` (all in the same “offset coordinate system” your extractor stored)

> If you generated label offsets earlier (L0000…), great; but the algorithm below *does not depend* on your labels—labels are *derived* from offsets.

---

## A3) Control-flow opcode classification (minimal but correct enough)

You need **two decisions** per instruction:

1. Does it **end the current block**?
2. If it has a jump target, does it **also fall through**?

### Terminators (no fallthrough)

Treat these as “path ends here”:

* returns: `RETURN_VALUE` (and `RETURN_CONST` if present in your build)
* raises: `RAISE_VARARGS`, `RERAISE`
* unconditional jumps: `JUMP_FORWARD`, `JUMP_BACKWARD`, `JUMP_BACKWARD_NO_INTERRUPT` (and any other `JUMP_*` that has no `IF` in the name)

### Branches (split flow: jump + fallthrough)

Anything with `jump_target_offset != None` **and** not classified as unconditional jump is a branch block terminator.

This includes:

* `POP_JUMP_*_IF_TRUE/FALSE`
* `JUMP_IF_TRUE_OR_POP`, `JUMP_IF_FALSE_OR_POP`
* `FOR_ITER` (loop-continue vs loop-exit)
* other conditional jump-like ops

### Yield/suspend points (recommended)

If you want a CFG that respects “suspend/resume” boundaries, treat:

* `YIELD_VALUE`, `YIELD_FROM`
  as block terminators **with fallthrough** (resume edge to next block).
  This is optional but very useful for correctness in generator-heavy code.

---

## A4) Boundary set construction (the deterministic recipe)

Let:

* `I = instructions` in ascending `instr_index`
* `Offsets = [i.offset for i in I]`
* `OffsetSet = set(Offsets)`

We build a boundary set `B` of offsets where blocks must start:

### (1) Always include entry

* `B ← {Offsets[0]}` (usually 0, but don’t assume)

### (2) Jump target starts

* For each instruction `i`:

  * if `i.jump_target_offset != None`: add that target offset to `B`

### (3) Exception region boundaries (strongly recommended)

For each exception entry `e`:

* add `e.start_offset`, `e.end_offset`, `e.target_offset` to `B`

Why include `start/end`?
Because it makes projecting exception edges trivial and avoids “block partially overlaps try-range” ambiguity later.

### (4) Fallthrough-after-terminator starts

For each instruction `i` that ends a block (branch, return/raise, yield, unconditional jump):

* add the offset of the **next instruction** (if any) to `B`

In code, “next instruction offset” is `I[k+1].offset`.

---

## A5) Snapping boundaries to real instruction starts

In practice, `exception_table` offsets should align, but you **must guard**:

* If `b ∉ OffsetSet`, snap `b` to the **next** instruction offset `>= b` (lower-bound).
* If `b` is beyond the last instruction, drop it.

This keeps blocks aligned to instruction starts.

---

## A6) Block emission algorithm (exact, linear-time)

Once you have `B`:

1. Sort: `starts = sorted(B)`
2. For each start offset `s` in `starts`:

   * Let `t = next start after s` (or sentinel “end”)
   * Block instructions are those with `offset ∈ [s, t)` (half-open)
3. Physical byte span:

   * `block_byte_start = first.start_offset`
   * `block_byte_end = last.end_offset`
4. Emit:

   * `block_id = f"{code_unit_id}:B:{label(s)}"` (or just `:B:{s}` if you prefer)
   * store `start_offset=s`, `end_offset=t` (logical), and physical `[byte_start, byte_end)`

### Reference implementation (Python)

```python
from __future__ import annotations
from dataclasses import dataclass
from bisect import bisect_left
from typing import Iterable

@dataclass(frozen=True)
class InstrRow:
    instr_id: str
    instr_index: int
    offset: int
    start_offset: int
    end_offset: int
    baseopname: str
    jump_target_offset: int | None

@dataclass(frozen=True)
class ExcRow:
    exc_entry_index: int
    start_offset: int
    end_offset: int
    target_offset: int
    depth: int
    lasti: bool

@dataclass(frozen=True)
class Block:
    block_id: str
    code_unit_id: str
    start_off: int         # logical start (instruction offset)
    end_off: int           # logical end (next block start or sentinel)
    byte_start: int        # physical
    byte_end: int          # physical
    first_instr_index: int
    last_instr_index: int

def _is_uncond_jump(op: str) -> bool:
    return op.startswith("JUMP_") and ("IF" not in op)

def _is_return_or_raise(op: str) -> bool:
    return op in {"RETURN_VALUE", "RETURN_CONST", "RAISE_VARARGS", "RERAISE"}

def _is_yield(op: str) -> bool:
    return op in {"YIELD_VALUE", "YIELD_FROM"}

def _ends_block(ins: InstrRow) -> bool:
    if ins.jump_target_offset is not None:
        return True
    if _is_return_or_raise(ins.baseopname):
        return True
    if _is_yield(ins.baseopname):
        return True
    return False

def _snap_to_instr_start(b: int, offsets: list[int]) -> int | None:
    j = bisect_left(offsets, b)
    return offsets[j] if j < len(offsets) else None

def build_blocks(code_unit_id: str, instrs: list[InstrRow], exc: list[ExcRow]) -> list[Block]:
    instrs = sorted(instrs, key=lambda r: r.instr_index)
    offsets = [r.offset for r in instrs]
    offset_set = set(offsets)

    B: set[int] = set()
    B.add(offsets[0])

    # jump targets
    for r in instrs:
        if r.jump_target_offset is not None:
            B.add(r.jump_target_offset)

    # exception boundaries (start/end/target)
    for e in exc:
        B.add(e.start_offset)
        B.add(e.end_offset)
        B.add(e.target_offset)

    # fallthrough after terminators/branches/yields
    for k, r in enumerate(instrs):
        if _ends_block(r) and k + 1 < len(instrs):
            B.add(instrs[k + 1].offset)

    # snap boundaries
    starts_unsnapped = sorted(B)
    starts: list[int] = []
    for b in starts_unsnapped:
        if b in offset_set:
            starts.append(b)
        else:
            s = _snap_to_instr_start(b, offsets)
            if s is not None:
                starts.append(s)
    starts = sorted(set(starts))

    # emit blocks (two-pointer scan)
    blocks: list[Block] = []
    cur_i = 0
    for bi, s in enumerate(starts):
        # advance cur_i to first instr at offset s
        while cur_i < len(instrs) and instrs[cur_i].offset < s:
            cur_i += 1
        if cur_i >= len(instrs) or instrs[cur_i].offset != s:
            continue

        t = starts[bi + 1] if bi + 1 < len(starts) else (offsets[-1] + 2)  # sentinel logical end
        start_i = cur_i
        end_i = start_i
        while end_i < len(instrs) and instrs[end_i].offset < t:
            end_i += 1
        last_i = end_i - 1
        if last_i < start_i:
            continue

        first = instrs[start_i]
        last = instrs[last_i]

        block_id = f"{code_unit_id}:B:{s}"
        blocks.append(Block(
            block_id=block_id,
            code_unit_id=code_unit_id,
            start_off=s,
            end_off=t,
            byte_start=first.start_offset,
            byte_end=last.end_offset,
            first_instr_index=first.instr_index,
            last_instr_index=last.instr_index,
        ))
        cur_i = end_i

    return blocks
```

---

# B) CFG edge emission (normal edges) + exception edges

Once blocks exist, you emit **two edge families**:

* normal edges: fallthrough + branches
* exception edges: `CFG_EXC` (from exception table projection)

## B1) Block lookup helpers (required)

You need:

* `block_for_offset(off)` → block containing instruction offset `off`
* `next_block(block)` → block starting at `block.end_off` (if present)

The easiest is to store blocks sorted by `start_off` and binary-search.

---

## B2) Normal edges: exact emission rules

For each block `b`:

* Let `t = last instruction in block` (the one with max `instr_index`)
* Compute:

  * `fallthrough = block starting at b.end_off` (if any)
  * `jump = block containing t.jump_target_offset` (if not None)

Then:

### Case 1: return/raise (no successors)

* emit nothing (or emit edge to a synthetic `EXIT` node if you use one)

### Case 2: unconditional jump

* emit edge `b → jump` labeled `CFG_JUMP` (no fallthrough)

### Case 3: conditional branch (has jump target + fallthrough)

* emit jump edge + fallthrough edge with polarity if known

**Polarity mapping (deterministic heuristic)**
If `baseopname` contains:

* `IF_FALSE` → jump is **false**, fallthrough is **true**
* `IF_TRUE` → jump is **true**, fallthrough is **false**
* `FOR_ITER` → fallthrough = loop continues (**true**), jump = loop exits (**false**)
  Otherwise:
* label as `CFG_BRANCH` with `props.polarity="unknown"`

### Case 4: normal fallthrough

* emit `b → fallthrough` labeled `CFG_NEXT`

---

## B3) Exception-edge projection: `py_bc_exception_table → CFG_EXC`

This is the second algorithm you requested, but it depends on blocks, so I’m placing it here.

### Baseline (sound, block-level)

For each exception entry `e = [start, end) -> target`:

1. `handler = block_for_offset(e.target_offset)`
2. `src_blocks = all blocks whose [start_off, end_off) intersects [e.start_offset, e.end_offset)`
3. For each `b in src_blocks`: emit `CFG_EXC` edge `b → handler` with props `{exc_entry_index, depth, lasti}`

This is **sound**: any instruction in the try-range can throw, so control can transfer to the handler.

### “Innermost-only” refinement (still simple, reduces edge spam)

Exception tables can overlap (nested try). To avoid emitting edges to multiple handlers from the same block:

* For each block `b`, consider all exception entries whose range intersects `b`
* Keep only those with **smallest range length** `(end-start)` (i.e., most specific), or keep top-K

This approximates “innermost handler wins” without modeling interpreter stack depth.

### Reference implementation (block-level CFG_EXC)

```python
from dataclasses import dataclass
from bisect import bisect_right

@dataclass(frozen=True)
class CfgEdge:
    edge_id: str
    code_unit_id: str
    src_block_id: str
    dst_block_id: str
    kind: str                # "next|jump|true|false|branch|exc"
    cond_instr_id: str | None = None
    exc_entry_index: int | None = None
    depth: int | None = None
    lasti: bool | None = None

def _block_for_offset(blocks: list[Block], off: int) -> Block | None:
    # blocks sorted by start_off
    starts = [b.start_off for b in blocks]
    i = bisect_right(starts, off) - 1
    if i < 0:
        return None
    b = blocks[i]
    return b if (b.start_off <= off < b.end_off) else None

def project_exception_edges(code_unit_id: str, blocks: list[Block], exc: list[ExcRow]) -> list[CfgEdge]:
    blocks = sorted(blocks, key=lambda b: b.start_off)
    out: list[CfgEdge] = []
    for e in exc:
        handler = _block_for_offset(blocks, e.target_offset)
        if handler is None:
            continue

        for b in blocks:
            # intersect [b.start_off, b.end_off) with [e.start, e.end)
            if b.end_off <= e.start_offset or b.start_off >= e.end_offset:
                continue
            edge_id = f"{code_unit_id}:EXC:{e.exc_entry_index}:{b.block_id}->{handler.block_id}"
            out.append(CfgEdge(
                edge_id=edge_id,
                code_unit_id=code_unit_id,
                src_block_id=b.block_id,
                dst_block_id=handler.block_id,
                kind="exc",
                exc_entry_index=e.exc_entry_index,
                depth=e.depth,
                lasti=e.lasti,
            ))
    return out
```

---

# C) First “Python bytecode def/use classifier” keyed by `baseopname`

This is the third deliverable: a deterministic classifier that turns `py_bc_instructions` rows into **def/use events** that your DFG builder can consume.

## C1) Event model (what the classifier emits)

For each instruction `ins`, emit zero or more of:

* `USE_SLOT(space, name)`
* `DEF_SLOT(space, name)`
* `KILL_SLOT(space, name)`
* `USE_ATTR(attr_name)` / `DEF_ATTR(attr_name)` / `KILL_ATTR(attr_name)`
* `USE_SUBSCR` / `DEF_SUBSCR` / `KILL_SUBSCR` (subscript is “memory slot” keyed by container+index)
* `CALL(argc, has_kwargs, ...)` (optional, for callsite nodes)
* `BRANCH(polarity=...)` (optional, for CFG annotation)

“space” values you care about in Python:

* `FAST` (locals)
* `GLOBAL` / `NAME` (globals, module scope lookups)
* `DEREF` (freevars/cellvars)

## C2) The classifier table (exact rows for the critical op families)

Store this as a tiny embedded dataset `py_bc_op_classifier` (Arrow/Parquet or literal list). **Key = baseopname**.

### Binding-slot ops (these directly create def/use edges to bindings)

| baseopname      | event     | space  | name source  |
| --------------- | --------- | ------ | ------------ |
| `LOAD_FAST`     | USE_SLOT  | FAST   | `argval_str` |
| `STORE_FAST`    | DEF_SLOT  | FAST   | `argval_str` |
| `DELETE_FAST`   | KILL_SLOT | FAST   | `argval_str` |
| `LOAD_NAME`     | USE_SLOT  | NAME   | `argval_str` |
| `STORE_NAME`    | DEF_SLOT  | NAME   | `argval_str` |
| `DELETE_NAME`   | KILL_SLOT | NAME   | `argval_str` |
| `LOAD_GLOBAL`   | USE_SLOT  | GLOBAL | `argval_str` |
| `STORE_GLOBAL`  | DEF_SLOT  | GLOBAL | `argval_str` |
| `DELETE_GLOBAL` | KILL_SLOT | GLOBAL | `argval_str` |
| `LOAD_DEREF`    | USE_SLOT  | DEREF  | `argval_str` |
| `STORE_DEREF`   | DEF_SLOT  | DEREF  | `argval_str` |
| `DELETE_DEREF`  | KILL_SLOT | DEREF  | `argval_str` |

> Notes:
>
> * `argval_str` is the `Instruction.argval` coerced to string for these ops.
> * This gives you deterministic binding events before you do scope resolution.

### Attribute “memory” ops

| baseopname    | event     | name source  |
| ------------- | --------- | ------------ |
| `LOAD_ATTR`   | USE_ATTR  | `argval_str` |
| `STORE_ATTR`  | DEF_ATTR  | `argval_str` |
| `DELETE_ATTR` | KILL_ATTR | `argval_str` |

### Subscript “memory” ops (container[index])

Depending on Python build, subscripting may appear as:

* `BINARY_SUBSCR` (load)
* `STORE_SUBSCR`, `DELETE_SUBSCR`

| baseopname      | event       |
| --------------- | ----------- |
| `BINARY_SUBSCR` | USE_SUBSCR  |
| `STORE_SUBSCR`  | DEF_SUBSCR  |
| `DELETE_SUBSCR` | KILL_SUBSCR |

### Branch ops (CFG annotation + “uses condition value”)

You don’t *need* these for def/use, but they’re useful to tag CFG edges and to record “condition value consumed”.

| baseopname             | branch polarity (jump edge)                          |
| ---------------------- | ---------------------------------------------------- |
| `POP_JUMP_*_IF_FALSE`  | jump=false, fallthrough=true                         |
| `POP_JUMP_*_IF_TRUE`   | jump=true, fallthrough=false                         |
| `JUMP_IF_FALSE_OR_POP` | jump=false                                           |
| `JUMP_IF_TRUE_OR_POP`  | jump=true                                            |
| `FOR_ITER`             | jump=false (loop exit), fallthrough=true (loop body) |

### Exit/terminator ops

| baseopname                      | meaning                                             |
| ------------------------------- | --------------------------------------------------- |
| `RETURN_VALUE` / `RETURN_CONST` | terminator (pops return value in RETURN_VALUE case) |
| `RAISE_VARARGS`                 | terminator                                          |
| `RERAISE`                       | terminator                                          |

### Calls (callsite creation; def/use still comes from loads/stores + stack)

Your DFG can be built without a call classifier at first (calls are just value transforms), but for the CPG you’ll almost certainly want `CALLSITE` nodes.

| baseopname         | call event | arity source                                        |
| ------------------ | ---------- | --------------------------------------------------- |
| `CALL`             | CALL       | `arg` (# positional+kw values depends on call form) |
| `CALL_KW`          | CALL       | `arg` + kwname tuple on stack                       |
| `CALL_FUNCTION_EX` | CALL       | flags in `arg`                                      |

> Treat these as “call-like transforms” in v1: pop callable + args, push result. Then join callsite to SCIP via syntax anchor.

---

## C3) Pattern fallback rules (so your classifier never breaks)

When `baseopname` isn’t in your explicit table, apply deterministic string rules:

1. If `baseopname.startswith("LOAD_")` and ends in `{FAST, NAME, GLOBAL, DEREF}` → `USE_SLOT` in that space
2. If `baseopname.startswith("STORE_")` and ends in `{FAST, NAME, GLOBAL, DEREF}` → `DEF_SLOT`
3. If `baseopname.startswith("DELETE_")` and ends in `{FAST, NAME, GLOBAL, DEREF}` → `KILL_SLOT`
4. If `baseopname.endswith("_ATTR")` → attr event
5. If `baseopname.endswith("_SUBSCR")` or contains `SUBSCR` → subscript event
6. Else: no binding/memory event (pure stack transform)

This keeps v1 deterministic even as opcode inventories shift.

---

## C4) Applying the classifier to emit DFG inputs

You convert instruction rows into **event rows**:

### Suggested derived table: `py_bc_defuse_events`

Columns:

* `code_unit_id`
* `instr_id`
* `instr_index`
* `event_kind` (`USE_SLOT|DEF_SLOT|KILL_SLOT|USE_ATTR|...`)
* `space` (nullable; FAST/NAME/GLOBAL/DEREF)
* `name` (nullable; var name or attr name)
* `confidence` (start at 1.0 for explicit slot ops, 0.6 for heuristic memory ops)

Then your DFG pipeline is:

1. Build CFG (blocks + edges)
2. Build def/use events (from classifier)
3. Resolve `USE_SLOT/DEF_SLOT` to actual `BINDING` nodes using your scope/binder (AST/symtable/SCIP phase) 
4. Run reaching-defs over CFG:

   * Each `DEF_*` is a definition site
   * Each `USE_*` is a use site
   * Each `KILL_*` ends a reaching set
5. Emit `REACHES(def_site → use_site)` edges (your DFG edges in the CPG) 

---

# D) Putting all three together in your CPG build

Mechanically (per code unit):

1. **Compute label offsets** (jump targets + exception boundaries + entry)
2. **Compute blocks** (A)
3. **Compute normal CFG edges** (B2)
4. **Project exception edges** (B3) and union with CFG edges
5. **Compute def/use events** (C)
6. **Run dataflow** over CFG:

   * `DEF_SLOT` reaches `USE_SLOT` via CFG
   * `DEF_ATTR` reaches `USE_ATTR` via CFG (object-sensitive later)
7. Emit the CPG nodes/edges:

   * `CFG_BLOCK` nodes from blocks
   * `CFG_*` edges from normal + exc edges
   * `DF_DEF/DF_USE` nodes (or reuse instruction nodes)
   * `REACHES` edges from dataflow

This exactly matches your overview’s “CFG → DFG → interprocedural stitching” layering, but now the CFG/DFG layers are **grounded in dis + exception table** (so try/with/finally stop being guesswork). 

---

If you want the next increment after *these* algorithms, the highest leverage is a **golden micro-fixture suite** that asserts:

* block boundaries and CFG edges for `if/loop/try/finally/with/comprehension`
* exception edges for nested try
* reaching-defs correctness for locals + closures (`LOAD_DEREF/STORE_DEREF`)


Below is a **fully pinned “inspect → facts” contract** for **Python 3.13**: **schemas + normalization rules + extraction algorithms**, with a **pyarrow/polars-first** storage model. It’s designed to be:

* **deterministic** (stable-ish keys, bounded repr, explicit variants),
* **non-executing by default** (static enumeration, no descriptor invocation, no annotation eval),
* **explicit about tricky cases**:

  * wrappers (`__wrapped__` / `inspect.unwrap` / `follow_wrapped` / `__signature__`)
  * stringized annotations (`from __future__ import annotations`, `get_annotations(eval_str=...)`)
  * builtins with missing metadata (signature/source failures)
  * safe static enumeration that preserves descriptors (`getmembers_static`, `getattr_static`) ([Python documentation][1])

---

## 0) Contract principles (what “pinned” means here)

### 0.1 Two extraction modes (explicit)

You always stamp a `mode` on rows:

* **`mode="static"` (canonical)**

  * member enumeration: `inspect.getmembers_static` (never triggers descriptor protocol / `__getattr__` / `__getattribute__`) ([Python documentation][1])
  * attribute fetch: `inspect.getattr_static` when needed (returns descriptors instead of resolving them; may miss dynamic attrs) ([Python documentation][1])
  * annotations: `inspect.get_annotations(eval_str=False)` (strings remain strings; no eval) ([Python documentation][1])
  * signatures: `inspect.signature(..., eval_str=False)` in both wrapped/raw variants ([Python documentation][1])

* **`mode="dynamic"` (optional, non-canonical)**

  * you may use `getmembers`/`getattr` and `eval_str=True`, but those can execute user code (descriptors + eval). You only do this in an explicit “unsafe” run.

### 0.2 Every “operation” produces either facts or a normalized failure record

Some `inspect` calls are expected to fail (builtins, no source, missing signature metadata). E.g. `signature()` can raise `ValueError`/`TypeError` and may be unavailable for some C-defined builtins. ([Python documentation][1])
So every table has: `ok`, `error_type`, `error_msg`.

---

## 1) Common encoding: stable-ish IDs + safe value representation

### 1.1 Object identity (stable-ish)

`inspect` is runtime introspection; you can’t get a perfect “global stable ID” for arbitrary runtime-created objects. For repo-indexing-style objects (modules/classes/functions), you can get **good stability** via `(module, qualname, kind)`.

**Canonical `object_id`**

* If `obj.__module__` and `obj.__qualname__` exist:
  `object_id = "{module}:{qualname}:{kind}"`

**Fallback when qualname missing**

* Use `{type_qualname}:{name}:{kind}` plus a short digest of `repr(type(obj)) + repr(obj)`.

**Always store (debug-only)**

* `object_addr = id(obj)` as `uint64` (not stable across runs; for de-duping within a run).

### 1.2 ValueRef: how you store defaults, annotations, member values *without pickling*

You should never pickle arbitrary objects in an indexer contract. Store a **lossy but deterministic** “value fingerprint”.

**Normalization rules**

* `type_qualname = type(v).__module__ + "." + type(v).__qualname__`
* `repr_trunc = repr(v)[:MAX_REPR]` (and store `repr_len`)
* `repr_hash = sha256(repr_trunc.encode("utf-8"))`
* `kind` is a small enum you control (primitive/callable/class/module/descriptor/other)
* **Never call** `__get__`, `__call__`, or any property access that could execute code.

---

## 2) Arrow schemas (pyarrow-first, polars-friendly)

### 2.1 Shared struct types

```python
import pyarrow as pa

ValueRef = pa.struct([
    ("kind", pa.string()),              # "none|bool|int|float|str|bytes|callable|class|module|descriptor|other"
    ("type_qualname", pa.string()),
    ("repr_trunc", pa.string()),
    ("repr_len", pa.int32()),
    ("repr_sha256", pa.binary(32)),     # sha256 over repr_trunc utf-8
    ("is_callable", pa.bool_()),
    ("is_descriptor", pa.bool_()),
    ("is_builtin", pa.bool_()),
])

OpStatus = pa.struct([
    ("ok", pa.bool_()),
    ("error_type", pa.string()),
    ("error_msg", pa.string()),
])
```

### 2.2 `py_inspect_objects` (inventory)

One row per “introspected object” (module/class/function/builtin/descriptor/etc).

```python
ObjectsSchema = pa.schema([
    ("schema_version", pa.int16()),
    ("mode", pa.string()),              # "static|dynamic"
    ("object_id", pa.string()),
    ("object_addr", pa.uint64()),
    ("kind", pa.string()),              # your enum (see §3.1)
    ("module_name", pa.string()),
    ("qualname", pa.string()),
    ("name", pa.string()),
    ("type_qualname", pa.string()),
    ("is_builtin", pa.bool_()),
    ("is_callable", pa.bool_()),
    ("is_descriptor", pa.bool_()),
    ("has_wrapped", pa.bool_()),
    ("has_signature_override", pa.bool_()),
    ("has_annotations", pa.bool_()),
    ("status", OpStatus),
])
```

### 2.3 `py_inspect_members_static` (static member enumeration; preserves descriptors)

One row per `(owner_object_id, attr_name)` produced by `getmembers_static`. ([Python documentation][1])

```python
MembersSchema = pa.schema([
    ("schema_version", pa.int16()),
    ("mode", pa.string()),              # almost always "static" here
    ("owner_object_id", pa.string()),
    ("owner_kind", pa.string()),
    ("attr_name", pa.string()),
    ("value_kind", pa.string()),        # coarse classification of the returned value
    ("value_object_id", pa.string()),   # nullable; only if value is also in Objects table
    ("value_ref", ValueRef),
    # descriptor classification (of the *returned value object*)
    ("desc_kind", pa.string()),         # "property|data|method|getset|member|function_descriptor|none"
    ("desc_is_data", pa.bool_()),
    ("desc_is_methoddesc", pa.bool_()),
    ("desc_is_getset", pa.bool_()),
    ("desc_is_member", pa.bool_()),
    ("status", OpStatus),
])
```

### 2.4 `py_inspect_unwrap_hops` (wrapper chain facts)

Built from `inspect.unwrap`, with cycle detection (`ValueError` on cycles). ([Python documentation][1])
Also captures where `signature()` *would* stop unwrapping due to `__signature__` (signature uses unwrap stop behavior). ([Python documentation][1])

```python
UnwrapSchema = pa.schema([
    ("schema_version", pa.int16()),
    ("mode", pa.string()),
    ("root_object_id", pa.string()),
    ("hop", pa.int16()),                # 0..N
    ("object_id", pa.string()),         # object at this hop
    ("has_wrapped", pa.bool_()),
    ("has_signature_override", pa.bool_()),
    ("stop_reason", pa.string()),       # only set on final row: "no_wrapped|signature_override|cycle|max_hops"
    ("status", OpStatus),
])
```

### 2.5 `py_inspect_signatures` + `py_inspect_signature_params` (relational form)

`inspect.signature(callable, follow_wrapped=..., globals=None, locals=None, eval_str=False)` is the canonical extractor; it attempts to un-stringize stringized annotations via `get_annotations`, and `follow_wrapped` controls unwrapping. It can raise `ValueError`/`TypeError`, and builtins may lack argument metadata. ([Python documentation][1])
`Signature.format(max_width=...)` exists in 3.13 and you can store it for UI/debug. ([Python documentation][1])

```python
SignaturesSchema = pa.schema([
    ("schema_version", pa.int16()),
    ("mode", pa.string()),
    ("signature_id", pa.string()),          # stable hash over (object_id, variant, sig_text)
    ("object_id", pa.string()),
    ("variant", pa.string()),               # "raw|wrapped"
    ("follow_wrapped", pa.bool_()),
    ("eval_str", pa.bool_()),               # pinned=False
    ("effective_object_id", pa.string()),   # for wrapped variant: last hop (or stop at __signature__)
    ("sig_text", pa.string()),              # str(sig)
    ("sig_format", pa.string()),            # sig.format(max_width=None) (optional)
    ("return_annotation", ValueRef),        # Signature.empty -> kind="none"
    ("has_varargs", pa.bool_()),
    ("has_varkw", pa.bool_()),
    ("status", OpStatus),
])

ParamsSchema = pa.schema([
    ("schema_version", pa.int16()),
    ("mode", pa.string()),
    ("signature_id", pa.string()),
    ("param_index", pa.int16()),
    ("name", pa.string()),
    ("kind", pa.string()),                  # POSITIONAL_ONLY, POSITIONAL_OR_KEYWORD, VAR_POSITIONAL, KEYWORD_ONLY, VAR_KEYWORD
    ("default_present", pa.bool_()),
    ("default_value", ValueRef),
    ("annotation_present", pa.bool_()),
    ("annotation_value", ValueRef),
    ("status", OpStatus),
])
```

### 2.6 `py_inspect_annotations_kv` (object annotations, safe/no-eval)

`inspect.get_annotations`:

* only callable/class/module accepted
* always returns a **fresh dict**
* `eval_str=True` calls `eval()` on string values and exceptions propagate
* determines default `globals/locals` based on object type and may unwrap wrapped callables for `__globals__`. ([Python documentation][1])

Pinned contract stores `eval_str=False` as canonical.

```python
AnnotationsSchema = pa.schema([
    ("schema_version", pa.int16()),
    ("mode", pa.string()),
    ("object_id", pa.string()),
    ("eval_str", pa.bool_()),               # pinned=False
    ("key", pa.string()),                   # annotation key (incl "return" if present)
    ("value", ValueRef),
    ("status", OpStatus),
])
```

### 2.7 `py_inspect_source` (source/file facts with normalized failures)

`getsource`/`getsourcelines` raise `TypeError` for built-in module/class/function and `OSError` if source can’t be retrieved. ([Python documentation][1])

```python
SourceSchema = pa.schema([
    ("schema_version", pa.int16()),
    ("mode", pa.string()),
    ("object_id", pa.string()),
    ("file_name", pa.string()),             # from getsourcefile/getfile; nullable
    ("start_line", pa.int32()),
    ("line_count", pa.int32()),
    ("source_sha256", pa.binary(32)),
    ("source_preview", pa.string()),        # first N chars (optional)
    ("status", OpStatus),
])
```

---

## 3) Normalization rules (the “pinned” behaviors)

### 3.1 Canonical `kind` taxonomy (keep it small)

Your `Objects.kind` should be a stable enum you control, e.g.:

* `module`
* `class`
* `function`
* `builtin_function_or_method` (inspect.isbuiltin / inspect.isroutine fallback)
* `method` (bound python method; inspect.ismethod)
* `descriptor_property`
* `descriptor_data`
* `descriptor_method`
* `descriptor_getset`
* `descriptor_member`
* `other_callable`
* `other`

### 3.2 Wrapper handling (this is the big one)

You want **two separate things**:

1. **The full `__wrapped__` chain** (for provenance)
2. The **effective callable used by signature()** (which may stop early if `__signature__` exists)

Rules:

* Build unwrap hops using `inspect.unwrap(func, stop=...)`; record `ValueError` as `stop_reason="cycle"`. ([Python documentation][1])
* Use the same `stop` semantics signature uses: stop if any object in chain defines `__signature__`. (Docs explicitly: signature uses unwrap to stop unwrapping if a `__signature__` attribute is defined.) ([Python documentation][1])
* For signatures you store both variants:

  * `variant="raw"`: `inspect.signature(obj, follow_wrapped=False, eval_str=False)` ([Python documentation][1])
  * `variant="wrapped"`: `inspect.signature(obj, follow_wrapped=True, eval_str=False)` ([Python documentation][1])
* Also store `effective_object_id`:

  * raw: `effective_object_id = object_id`
  * wrapped: `effective_object_id = unwrap_stop_at_signature_override(object_id)` (your hops table tells you which)

Why this matters: it makes “decorator hides signature” vs “decorator explicitly sets **signature**” visible and queryable. `__signature__` use is explicitly called out as an implementation detail, so you treat it as “best effort” metadata and record it explicitly rather than trusting it silently. ([Python documentation][1])

### 3.3 Stringized annotations (safe by default, explicit unsafe variant)

Rules:

* Canonical annotations extraction: `inspect.get_annotations(obj, eval_str=False)` (strings remain strings). ([Python documentation][1])
* Canonical signature extraction: `inspect.signature(..., eval_str=False)` (still may attempt to un-stringize using get_annotations; but with `eval_str=False`, strings remain strings). ([Python documentation][1])
* If you ever run `eval_str=True`, **it’s a separate run** (`mode="dynamic"` or `mode="unsafe_eval"`) and failures are captured as normal errors (`error_type`, `error_msg`), because exceptions propagate from `eval()`. ([Python documentation][1])

### 3.4 Builtins missing metadata (expected failures)

Rules:

* `inspect.signature` failures are *normal*:

  * store `ok=false`, `error_type=ValueError|TypeError`, `error_msg=...` ([Python documentation][1])
* Do **not** “invent” signatures from docstrings as part of the pinned contract; if you add that later, it must be a separate derived dataset with `confidence="low"` (because it’s not semantically guaranteed).

Source retrieval:

* `getsource/getfile` can raise `TypeError` for builtins and `OSError` for missing sources; capture those. ([Python documentation][1])

### 3.5 Safe static member enumeration (preserve descriptors; don’t execute)

Rules:

* Use `inspect.getmembers_static(obj)` for member enumeration:

  * it won’t trigger descriptor protocol / `__getattr__` / `__getattribute__`
  * it may return descriptor objects instead of resolved instance values
  * it may miss dynamic attributes or include descriptors that raise `AttributeError` under dynamic access ([Python documentation][1])
* If you need a single attribute, use `inspect.getattr_static(obj, name)`:

  * does not resolve descriptors (returns descriptor objects)
  * may fail if instance `__dict__` is shadowed by another member (e.g., property)
  * does not resolve certain builtin descriptor types unless you explicitly call `__get__` (which can execute code) ([Python documentation][1])

Pinned contract stance: **never call `__get__`**. You record the descriptor object and classify it.

---

## 4) Extraction algorithms (deterministic, repo-grade)

### 4.1 Core BFS/DFS walk (static-safe)

**Goal:** build an object inventory from a starting set (modules, classes) without executing descriptors.

Pseudo:

1. Seed queue with `{module objects}` you already have imported (or a curated allowlist).
2. For each `obj`:

   * emit `py_inspect_objects` row
   * if module/class:

     * enumerate `inspect.getmembers_static(obj)`
     * for each `(name, value)`:

       * emit `py_inspect_members_static` row
       * if value is introspectable kind (module/class/function/builtin):

         * enqueue it (unless seen)
   * if callable:

     * emit unwrap hops table for that callable (both “full chain” and “stop-at-**signature** chain”; you can store these as separate `root_object_id` variants or a `purpose` column)
     * emit signatures table (raw + wrapped)
     * emit signature params rows
     * emit annotations_kv rows from `get_annotations(eval_str=False)`
     * emit source row

Critical implementation detail: **cap your traversal** (`max_depth`, `max_members_per_object`, `include_private`) so this stays indexer-friendly.

### 4.2 Descriptor classification for member rows

For each returned `value` from `getmembers_static`:

* compute descriptor flags without invoking it:

  * `inspect.isdatadescriptor`, `inspect.ismethoddescriptor`, `inspect.isgetsetdescriptor`, `inspect.ismemberdescriptor`
  * special-case `isinstance(value, property)` as `desc_kind="property"`
* also store `value_kind` via inspect predicates (`ismodule/isclass/isfunction/isbuiltin/ismethod/...`)

This gives you a passive, queryable “attribute surface map” without executing code.

---

## 5) Minimal reference implementation skeleton (pyarrow + polars)

### 5.1 ValueRef encoder

```python
import hashlib
import inspect

MAX_REPR = 512

def _type_qualname(x) -> str:
    t = type(x)
    return f"{t.__module__}.{t.__qualname__}"

def value_ref(x) -> dict:
    if x is None:
        rep = "None"
        kind = "none"
    else:
        rep_full = repr(x)
        rep = rep_full[:MAX_REPR]
        kind = (
            "str" if isinstance(x, str) else
            "int" if isinstance(x, int) else
            "float" if isinstance(x, float) else
            "bool" if isinstance(x, bool) else
            "bytes" if isinstance(x, (bytes, bytearray)) else
            "module" if inspect.ismodule(x) else
            "class" if inspect.isclass(x) else
            "callable" if callable(x) else
            "descriptor" if (hasattr(x, "__get__") and not inspect.ismodule(x)) else
            "other"
        )

    h = hashlib.sha256(rep.encode("utf-8")).digest()
    return dict(
        kind=kind,
        type_qualname=_type_qualname(x) if x is not None else "builtins.NoneType",
        repr_trunc=rep,
        repr_len=len(rep),
        repr_sha256=h,
        is_callable=callable(x),
        is_descriptor=bool(getattr(x, "__get__", None)) if x is not None else False,
        is_builtin=inspect.isbuiltin(x),
    )
```

### 5.2 Building Arrow tables, then Polars

```python
import pyarrow as pa
import polars as pl

# rows_* are lists[dict] produced by your extractor
t_objects = pa.Table.from_pylist(rows_objects, schema=ObjectsSchema)
t_members = pa.Table.from_pylist(rows_members, schema=MembersSchema)
t_unwrap  = pa.Table.from_pylist(rows_unwrap,  schema=UnwrapSchema)
t_sigs    = pa.Table.from_pylist(rows_sigs,    schema=SignaturesSchema)
t_params  = pa.Table.from_pylist(rows_params,  schema=ParamsSchema)

# Polars views (nice for downstream transforms)
df_objects = pl.from_arrow(t_objects)
df_members = pl.from_arrow(t_members)

# Persist
import pyarrow.parquet as pq
pq.write_table(t_objects, "py_inspect_objects.parquet")
pq.write_table(t_members, "py_inspect_members_static.parquet")
```

---

## 6) “Tricky case” coverage checklist (what you can assert in tests)

You can write golden fixtures that assert:

1. **Decorators**

* `wrapped` vs `raw` signatures differ when `@wraps` used; unwrap hops record chain; cycle raises `ValueError` and is captured. ([Python documentation][1])

2. **`__signature__` override**

* unwrap-stop semantics trigger; `effective_object_id` points at the override-bearing object, not the leaf. (Signature may use `__signature__`.) ([Python documentation][1])

3. **Stringized annotations**

* `get_annotations(eval_str=False)` returns strings unchanged; `eval_str=True` is isolated to unsafe mode and failure is captured (exceptions propagate). ([Python documentation][1])

4. **Builtins**

* signature failures captured as `ok=false` with error info; source retrieval captured as TypeError/OSError as appropriate. ([Python documentation][1])

5. **Descriptors**

* `getmembers_static/getattr_static` return descriptor objects without invoking them; member rows classify descriptor kinds; no runtime side effects. ([Python documentation][1])

---

If you want the next increment after this contract: I can provide a **golden fixture suite** of ~12 micro-programs (wraps + custom **signature** + property/getset/member descriptors + stringized annotations + builtins) with **expected Parquet snapshots** and a small **normalizer** so diffs across Python 3.13.x patch upgrades are crisp and reviewable.

[1]: https://docs.python.org/3.13/library/inspect.html "inspect — Inspect live objects — Python 3.13.11 documentation"

Below is a **“dis-style” algorithm pack** for **using `inspect` end-to-end** as an optional but extremely high-leverage **runtime-reflection overlay** in your Python CPG pipeline: from **target selection → safe extraction → Arrow tables → joins → graph projection → call/param wiring → validation**.

This assumes you already have the **syntax backbone (AST/CST + byte-span join) + SCIP occurrences/symbols** as in your CPG build guide, and you’re layering semantics on top. 

---

# Where `inspect` fits in the CPG pipeline

`inspect` is **not** your CFG/DFG correctness hinge (that’s your `dis`/Eval-IR layer). It’s a **reflection augmentation pass** that boosts:

* **Symbol resolution confidence** (runtime object ↔ syntax def ↔ SCIP symbol)
* **Call modeling** (signatures, `__wrapped__` chains, `__signature__` overrides)
* **Actual→formal parameter wiring** (your optional Phase 8.3) 
* **Descriptor/class surface modeling** (properties/data descriptors/method descriptors)
* **Type annotation surfaces** (safe stringized annotations)

Because importing executes code, you treat this pass as **optional**, **isolated**, and **explicitly budgeted**.

---

# Inspect algorithm pack (start → finish)

## INS-ALG-0 — Target selection + isolation strategy (make reflection safe & bounded)

### Inputs

* Repo root + module discovery results (from your file index / AST scan)
* Optional allowlist/denylist rules (`tests.*`, scripts, known side-effect modules)
* Time/memory budgets

### Output tables

* `py_inspect_import_attempts` (module → ok/error, duration, stderr hash)
* `py_inspect_run_meta` (python version, sys.path policy, budgets, run_id)

### Algorithm

1. **Build module candidates**

   * Enumerate importable module names from filesystem:

     * package dirs with `__init__.py`
     * `.py` files (derive module path)
   * Optionally restrict to:

     * entrypoints, or
     * “import graph reachable” modules (from AST `Import/ImportFrom` scan)

2. **Run inspection in an isolated worker**

   * Spawn a subprocess “inspector” (strongly recommended), not in-process:

     * run with isolated flags (`-I`), controlled `sys.path`, and environment guards
   * For each module in candidate order:

     * attempt `importlib.import_module(modname)`
     * on failure, record and continue
   * Enforce budgets:

     * per-module timeout
     * max objects visited
     * max members per object
     * max unwrap hops per callable

**Key contract**: the introspection output is only “trusted” for modules that imported successfully, and each row records `ok/error_type/error_msg`.

---

## INS-ALG-1 — Static surface extraction (object inventory + member surface without executing descriptors)

This is your “backbone” extractor for inspect facts (canonical mode = **static**).

### Inputs

* Imported module objects from INS-ALG-0

### Output tables (from your inspect→facts contract)

* `py_inspect_objects`
* `py_inspect_members_static`

### Algorithm (BFS over objects)

Maintain:

* `queue = [(obj, depth)]`
* `seen = set(object_id)` (or `id(obj)` within a run)

For each `(obj, depth)`:

1. **Compute canonical identity**

   * `module_name = getattr(obj, "__module__", None)` (or module name for modules)
   * `qualname = getattr(obj, "__qualname__", getattr(obj, "__name__", None))`
   * `kind = classify(obj)` using `inspect.is*` predicates
   * `object_id = f"{module_name}:{qualname}:{kind}"` when possible
   * Record `object_addr = id(obj)` for within-run de-dupe debugging

2. **Emit `py_inspect_objects` row** with:

   * kind flags (`is_builtin`, `is_callable`, `is_descriptor`)
   * wrapper flags (`has_wrapped = hasattr(obj, "__wrapped__")`)
   * signature override (`has_signature_override = hasattr(obj, "__signature__")`)

3. **If obj is module/class: enumerate members statically**

   * Use `inspect.getmembers_static(obj)` (this avoids descriptor invocation / `__getattr__`)
   * For each `(name, value)`:

     * Emit `py_inspect_members_static` row with:

       * `owner_object_id`
       * `attr_name`
       * `value_ref` (lossy but deterministic repr fingerprint)
       * descriptor classification flags *on the returned value*:

         * `inspect.isdatadescriptor`, `inspect.ismethoddescriptor`, etc.
     * If `value` is an introspectable kind (module/class/function/builtin):

       * enqueue `(value, depth+1)` subject to budgets

**Budgets (must be explicit)**:

* `depth <= D`
* `members_per_obj <= M`
* `total_objects <= N`
* `include_private = False` by default

**This single algorithm produces your “runtime surface map”** for the repo and any successfully imported deps.

---

## INS-ALG-2 — Callable surface extraction (unwrap chain + raw/wrapped signature variants + annotations)

This is the inspect counterpart to “CFG correctness hinge” in `dis`: it gives you a deterministic, queryable callable surface.

### Inputs

* `py_inspect_objects` rows where `is_callable=True`

### Output tables

* `py_inspect_unwrap_hops`
* `py_inspect_signatures`
* `py_inspect_signature_params`
* `py_inspect_annotations_kv`

### Algorithm

For each callable object `f` (including methods/functions/builtins/partials):

1. **Unwrap hop extraction**

   * Build two unwrap traversals:

     * **Full chain**: repeatedly follow `__wrapped__` until none
     * **Signature-stop chain**: stop when any hop has `__signature__`
   * Record each hop as:

     * `(root_object_id, hop_index, hop_object_id, has_wrapped, has_signature_override)`
   * If cycle detected: record final hop with `stop_reason="cycle"` and `ok=false`

2. **Signature extraction variants**

   * Variant A (**raw**): `inspect.signature(f, follow_wrapped=False, eval_str=False)`
   * Variant B (**wrapped**): `inspect.signature(f, follow_wrapped=True, eval_str=False)`
   * For each variant:

     * If success:

       * store `sig_text = str(sig)`
       * store `return_annotation` as `ValueRef`
       * derive `has_varargs/has_varkw` from parameters
       * emit one `py_inspect_signatures` row
       * emit N `py_inspect_signature_params` rows:

         * `param_index`, `name`, `kind`, `default_present`, `default_value`, `annotation_present`, `annotation_value`
     * If failure (`ValueError`/`TypeError`):

       * emit a signatures row with `ok=false` and error info

3. **Annotations extraction (safe canonical)**

   * For `f`, also try `inspect.get_annotations(f, eval_str=False)`:

     * always emits a fresh dict; store each item as a row
   * If the object is a class/module, do the same (when you process those objects)

**Important normalization**:

* canonical run never uses `eval_str=True` (that can call `eval()` and is unsafe)
* store failures as first-class rows (builtins often lack signature/source)

---

## INS-ALG-3 — Class surface graph (descriptor-preserving attributes + inheritance + override structure)

This is how `inspect` helps you build a high-fidelity **object model** subgraph (useful for call resolution and attribute flows).

### Inputs

* `py_inspect_objects` where `kind="class"`

### Output tables (suggested derived datasets)

* `py_inspect_class_mro` (class → base list)
* `py_inspect_class_declared_members` (declaring_class → name → ValueRef)
* `py_inspect_attr_resolution` (class → name → defining_class)

### Algorithm

For each class `C`:

1. **Compute MRO**

   * `mro = inspect.getmro(C)`
   * Emit edges/rows: `C -> base` in order

2. **Declared members (no execution)**

   * For each class `K` in `mro`:

     * iterate `K.__dict__.items()` (pure data; does not invoke descriptors)
     * classify each value as:

       * function, builtin, property, data descriptor, etc.
     * emit `declared_member(K, name, value_ref, desc_kind, flags)`

3. **Resolution map**

   * For each attribute name in the union of all `K.__dict__` keys:

     * the defining class is the first `K` in MRO where `name in K.__dict__`
     * emit `attr_resolution(C, name, defining_class)`

This gives you:

* inheritance edges,
* override edges (defining class differs from the “owner”),
* descriptor nodes (properties, getset/member descriptors) without executing them.

---

## INS-ALG-4 — Source anchoring: join runtime objects back to AST/CST and SCIP

This is the critical bridge that turns inspect output into CPG edges.

### Inputs

* Inspect tables (`objects`, `source`)
* Your existing syntax datasets:

  * `ast_nodes` with `(path, kind, name/qualpath, start_line, end_line, span_start_byte, span_end_byte)`
  * CST token map
* SCIP symbol mapping anchored to syntax spans

### Output tables

* `py_inspect_object_anchors` (object_id → ast_node_id, confidence, reason)
* `py_inspect_object_symbol_map` (object_id → symbol_id, confidence)

### Algorithm: object → AST def anchor

For each inspect object `o` with source info:

1. Get:

   * `path = inspect.getsourcefile(o)` (or failure)
   * `(lines, start_line) = inspect.getsourcelines(o)` (or failure)
   * `end_line = start_line + len(lines) - 1`
   * `qualname = o.__qualname__` (best-effort)
2. Candidate AST nodes are those with:

   * same `path`
   * `start_line` match (strong) or within `[start_line, end_line]` (weak)
   * `kind` compatible (function/class/module)
   * name/qualpath match heuristics:

     * exact name match first
     * then suffix match for nested (`Outer.Inner.f`)
3. Choose best candidate by score:

   * +3 exact start_line match
   * +2 qualpath match
   * +1 smallest span
4. Emit `object_anchors` row with `confidence = score / max_score` and reason string

### Algorithm: object → SCIP symbol map

Once you have an AST def anchor for `o`:

* Use your existing `DEF(node → SYMBOL)` SCIP attachment edges (or symbol table) to map def node → symbol
* Emit `object_symbol_map(object_id → symbol_id)` with high confidence

If no AST anchor exists:

* fallback: map by `(module_name, qualname)` against your internal “FQN symbol table” if you maintain one; mark confidence lower

---

## INS-ALG-5 — Project inspect facts into CPG nodes/edges (the graph layer)

At this point, inspect facts become first-class CPG augmentation edges.

### CPG nodes you’ll add or enrich

* `RUNTIME_OBJECT` nodes (optional; often you instead attach properties directly to SYMBOL/DEF nodes)
* `SIGNATURE` nodes
* `PARAM` nodes
* `TYPE_ANNOT` nodes (string or structured ValueRef)

### CPG edges to emit

1. **Runtime binding edges**

   * `DEF_ANCHORS_RUNTIME(def_ast_node → object_id)`
   * or `SYMBOL_HAS_RUNTIME(symbol → object_id)`

2. **Wrapper edges**

   * from `py_inspect_unwrap_hops`:

     * `WRAPS(wrapper_object → wrapped_object, props={hop})`
   * If you’ve anchored both endpoints to AST defs/symbols, add:

     * `WRAPS_DEF(wrapper_def → wrapped_def)`

3. **Signature edges**

   * `HAS_SIGNATURE(callable_symbol_or_def → signature_id, props={variant})`
   * `SIG_HAS_PARAM(signature → param)`
   * `PARAM_DEFAULT(param → value_ref)` (optional; store as property)
   * `PARAM_ANNOT(param → type_annot)`
   * `RET_ANNOT(signature → type_annot)`

4. **Descriptor/class model edges**

   * `INHERITS(class → base_class)`
   * `DECLARES_ATTR(defining_class → attr_name_node, props={desc_kind})`
   * `RESOLVES_ATTR(class → defining_class, props={name})` (optional but powerful)

All of these are **static**, descriptor-preserving, and don’t require executing user code beyond import itself.

---

## INS-ALG-6 — Actual→formal parameter wiring using inspect signatures (Phase 8.3)

This is the inspect analogue to your “def/use classifier”: it turns a callsite into deterministic argument wiring when you have a signature.

### Inputs

* Callsites from your eval layer / AST:

  * `callsite_id`
  * list of argument expression node ids:

    * positional args in order
    * keyword args `{name → expr_id}`
    * starargs presence (`*args`, `**kwargs`)
* Callee symbol candidates (from SCIP and/or your call resolution)
* Inspect signature rows + param rows (`variant="wrapped"` usually preferred)

### Output edges

* `ARG_OF(arg_expr → callsite, props={position|kw})` (you likely already emit)
* `BINDS_ARG(arg_expr → param_binding, props={position|kw, confidence})` 

### Binding algorithm (pure, no execution)

For each `(callsite, callee_candidate)`:

1. Choose a signature:

   * prefer `variant="wrapped"` if it exists and `ok=true`
   * else fallback to `raw`
2. Let `P = [params...]` in order, with `kind`:

   * positional-only, positional-or-keyword, var-positional, keyword-only, var-keyword
3. Perform a deterministic mapping:

   * Assign positional args to the next available positional parameter until:

     * you exhaust positional params
     * or you hit keyword-only boundary
   * Map keyword args by name to matching parameters (pos-or-kw or kw-only)
   * If `*args` present:

     * if var-positional exists: bind the stararg expression to that param with lower confidence
     * else: mark binding error (still emit nothing or emit “unknown”)
   * If `**kwargs` present:

     * if var-keyword exists: bind it similarly with lower confidence
4. Special case: **method-like calls**

   * If your callsite is `obj.meth(...)` and the callee signature includes an initial parameter that looks like a receiver (first param is positional-only / or named `self`), then:

     * create an **implicit actual** for the receiver expression (`obj`)
     * bind it to the first parameter
     * mark `props={implicit_receiver:true, confidence:0.8}` unless you have type confirmation

### Confidence scoring (simple, deterministic)

Start at `1.0`, then subtract:

* `-0.3` if signature came from builtin and params lacked annotations
* `-0.2` for presence of `*args` or `**kwargs`
* `-0.2` if any keyword didn’t match a param name
* `-0.3` if receiver binding is heuristic (no type proof)

Emit `BINDS_ARG` edges with that confidence.

**Why this matters for your CPG**: once you have `BINDS_ARG`, you can project interprocedural dataflow:

* actual argument’s value → callee param binding → DFG inside callee
  This is the direct “CPG usefulness jump” your guide calls out. 

---

## INS-ALG-7 — Validation + drift gates (so inspect doesn’t silently rot)

### Inputs

* All inspect-derived tables and joins

### Output

* `py_inspect_quality_report` (single JSON/Arrow row per run)

### Algorithm (per run)

Compute:

1. Import success rate

   * modules attempted vs imported
2. Anchoring rate

   * `% objects with source` and `% anchored to AST def`
3. Signature coverage

   * `% callables with ok signature (raw/wrapped)`
4. Wrapper effectiveness

   * `% callables with unwrap hops` and `% where wrapped signature differs from raw`
5. Parameter wiring applicability

   * among callsites with resolved callee symbol and signature, `% with complete bind` (no starargs/kwargs) vs partial

Set thresholds; fail CI only on the metrics you care about (e.g., anchoring/signature coverage) rather than import success (which may depend on env).

---

# Minimal “start→finish” dependency graph (what consumes what)

1. **Imports** → module objects
2. **Static extraction** → `objects`, `members_static`
3. **Callable extraction** → `unwrap_hops`, `signatures`, `params`, `annotations`
4. **Source** → `source` table
5. **Anchoring join** → `object_anchors`, `object_symbol_map`
6. **Graph projection** → WRAPS/HAS_SIGNATURE/PARAM/TYPE edges
7. **Callsite wiring** → BINDS_ARG edges (actual→formal)
8. **Interprocedural DFG** (optional next pass) uses BINDS_ARG + your DFG layer

---

If you want, I can turn the above into a **repo-ready “inspect contract harness”** like your other packs:

* a tiny `inspect_contract/` module,
* pytest micro-fixtures for decorators/`__signature__`/string annotations/descriptors/builtins,
* deterministic Parquet snapshots + a drift report that gates upgrades.


Below is a **pyarrow-centric “symtable → facts” extraction contract** (schemas + normalization rules + extraction algorithms) and then **exactly how those outputs get used** inside your end-to-end CPG build (specifically Phase 4 “Scopes + bindings”, and then feeding CFG/DFG/calls). 

Everything about `symtable` itself is grounded in the Python 3.13.11 docs (notably: symbol tables are generated from AST before bytecode; `SymbolTableType` includes annotation/type scopes; `TYPE_VARIABLE` is added in 3.13; `get_type()` now returns enum members; and the `Symbol` flag API + `is_namespace/get_namespace(s)` behavior). ([Python documentation][1])

---

# 0) What “symtable → facts” is for in a CPG

In your CPG build guide, Phase 4 (“Scopes + bindings”) is the missing middle between “syntax + symbols” and any meaningful dataflow. 

`symtable` gives you **compiler-grade scope classification** per identifier per scope:

* local / parameter / global / declared_global / nonlocal / free
* referenced / assigned / imported / annotated
* and namespace binding (`def`/`class` introduces child scopes via `is_namespace` + `get_namespace(s)`) ([Python documentation][1])

That is exactly the information you need to:

* build **SCOPE** nodes and **BINDING** nodes deterministically,
* compute **binding resolution edges** (free/nonlocal/global resolve-to),
* and **validate** your AST binder and your `dis` def/use events.

---

# 1) Contract principles

## 1.1 Deterministic inputs

Per file:

* canonical file bytes → canonical decoded text (the same text you used for CST/AST)
* call: `symtable.symtable(code, filename, compile_type)` (typically `compile_type="exec"` for .py files). ([Python documentation][1])

## 1.2 Outputs are *structured*, not a CLI dump

3.13 adds a CLI (`python -m symtable …`) but the format is not a stable interchange contract. Use programmatic extraction to Arrow tables. ([Python documentation][1])

## 1.3 IDs: two layers

* **Compiler IDs** (`table.get_id()`) are useful for *within-run linking* but not stable across runs. ([Python documentation][1])
* Your contract must define **stable-ish IDs** derived from: `(file_id, scope_kind, qualpath, lineno, ordinal)`.

## 1.4 Coordinate system

`symtable` gives *line numbers* (`get_lineno()`), not byte spans. ([Python documentation][1])
Your pipeline requires byte spans as the “index-grade coordinate system.” 
So this contract is explicitly **two-stage**:

1. extract symtable facts (line/qualpath keyed),
2. **anchor** scopes/bindings to AST/CST spans via joins.

---

# 2) Arrow tables (schemas)

I’ll give a minimal set that covers 95% of CPG needs cleanly, plus two derived tables that make binding resolution mechanical.

## 2.1 `py_sym_scopes` (one row per SymbolTable)

**Purpose:** scope nodes + nesting + key attributes (`is_nested`, `is_optimized`, etc.).

Columns (recommended):

* `schema_version: int16`
* `file_id: string`
* `path: string`
* `compile_type: string` (`exec|eval|single`)
* `scope_id: string` (stable-ish)
* `scope_local_id: int32` (`get_id()`, within-run) ([Python documentation][1])
* `parent_scope_id: string?`
* `scope_type: string` (`MODULE|FUNCTION|CLASS|ANNOTATION|TYPE_ALIAS|TYPE_PARAMETERS|TYPE_VARIABLE`) ([Python documentation][1])
* `scope_name: string` (`get_name()`) ([Python documentation][1])
* `qualpath: string` (your constructed path, e.g., `top::A::f::<listcomp#1>`)
* `lineno: int32` (`get_lineno()`) ([Python documentation][1])
* `is_nested: bool` (`is_nested()`) ([Python documentation][1])
* `is_optimized: bool` (`is_optimized()`) ([Python documentation][1])
* `has_children: bool` (`has_children()`) ([Python documentation][1])
* **anchor fields (filled in stage 2):**

  * `anchor_ast_node_id: string?`
  * `span_start_byte: int64?`
  * `span_end_byte: int64?`
  * `anchor_confidence: float32?`

## 2.2 `py_sym_symbols` (one row per Symbol in a scope)

**Purpose:** per-identifier scope classification + def/ref/import/annot flags.

Columns:

* `schema_version: int16`
* `file_id: string`
* `scope_id: string`
* `symbol_row_id: string` (stable-ish: `scope_id + name`)
* `name: string` (`Symbol.get_name()`) ([Python documentation][1])
* flags (all bool):

  * `is_referenced` ([Python documentation][1])
  * `is_assigned` ([Python documentation][1])
  * `is_imported` ([Python documentation][1])
  * `is_annotated` ([Python documentation][1])
  * `is_parameter` ([Python documentation][1])
  * `is_local` ([Python documentation][1])
  * `is_global` ([Python documentation][1])
  * `is_declared_global` ([Python documentation][1])
  * `is_nonlocal` ([Python documentation][1])
  * `is_free` ([Python documentation][1])
  * `is_namespace` ([Python documentation][1])
* `namespace_count: int16` (len(get_namespaces()) if `is_namespace`) ([Python documentation][1])

## 2.3 `py_sym_scope_edges` (nesting tree from `get_children()`)

**Purpose:** the scope tree as explicit edges.

Columns:

* `schema_version`
* `file_id`
* `parent_scope_id`
* `child_scope_id`
* `edge_kind: string` = `"PARENT_SCOPE"`

This is basically `SymbolTable.get_children()` materialized. ([Python documentation][1])

## 2.4 `py_sym_namespace_edges` (bind name → child scope(s))

**Purpose:** link a *name binding* to the *namespace(s)* it introduces.

Columns:

* `schema_version`
* `file_id`
* `scope_id` (where the binding name appears)
* `symbol_row_id`
* `name`
* `child_scope_id`
* `edge_kind: string` = `"BINDS_NAMESPACE"`
* `is_ambiguous: bool` (True when `namespace_count != 1`)

This is built from `Symbol.is_namespace()` + `Symbol.get_namespaces()`; `get_namespace()` can raise `ValueError` if there isn’t exactly one, so you always prefer `get_namespaces()` and record count. ([Python documentation][1])

## 2.5 `py_sym_function_partitions` (only for FUNCTION scopes)

**Purpose:** store the function’s name partitions once (useful for validation + closure wiring).

Columns:

* `schema_version`
* `file_id`
* `scope_id`
* `parameters: list<string>` (`get_parameters()`) ([Python documentation][1])
* `locals: list<string>` (`get_locals()`) ([Python documentation][1])
* `globals: list<string>` (`get_globals()`) ([Python documentation][1])
* `nonlocals: list<string>` (`get_nonlocals()`) ([Python documentation][1])
* `frees: list<string>` (`get_frees()`) ([Python documentation][1])

## 2.6 Derived (recommended): `py_sym_bindings` and `py_sym_resolution_edges`

These two make Phase 4 of your CPG guide *mechanical*.

### `py_sym_bindings`

One row per `(scope_id, name)` binding slot you will turn into a `BINDING` node.

Columns:

* `binding_id: string` (`{scope_id}:BIND:{name}`)
* `scope_id`, `file_id`, `name`
* `binding_kind: string` = `local|param|import|namespace|annot_only|global_ref|nonlocal_ref|free_ref|unknown`
* `declared_here: bool` (true for local/param/import/namespace/annot_only)
* `referenced_here: bool` (from symbol flag)
* `assigned_here: bool`
* `annotated_here: bool`
* `scoping_class: string` = `LOCAL|GLOBAL|NONLOCAL|FREE|…` (your normalized classification)

### `py_sym_resolution_edges`

Edges from “reference binding” → “definition binding” for closure/global/nonlocal.

Columns:

* `edge_id`
* `file_id`
* `src_binding_id`
* `dst_binding_id`
* `kind: string` = `GLOBAL|NONLOCAL|FREE|UNKNOWN`
* `confidence: float32`
* `reason: string`

---

# 3) Normalization rules (the pinned semantics)

## 3.1 Store `SymbolTableType` as enum names, not strings

In 3.13, `get_type()` returns `SymbolTableType` enum members and the docs explicitly warn not to hard-code returned strings; use the enum members. ([Python documentation][1])
So your contract stores:

* `scope_type = table.get_type().name` (e.g., `FUNCTION`)
* (optional) `scope_type_value = table.get_type().value` (for debugging)

## 3.2 Stable-ish `scope_id`

Because `get_id()` isn’t stable across runs, use:

* `scope_id = f"{file_id}:SCOPE:{qualpath}:{lineno}:{scope_type}:{ordinal}"`

Where:

* `qualpath` is constructed from parent scopes:

  * module: `top`
  * class/function: append `scope_name`
  * anonymous/implicit scopes: append `"<{scope_name}#{ordinal_in_parent}>"`
* `ordinal` disambiguates duplicate `(scope_type, scope_name, lineno)` siblings.

## 3.3 Symbols are keyed by `(scope_id, name)`

Within a given scope, a name is unique in symtable; store:

* `symbol_row_id = f"{scope_id}:SYM:{name}"`

## 3.4 Type/annotation scopes are “meta scopes”

`symtable` includes `ANNOTATION`, `TYPE_ALIAS`, `TYPE_PARAMETERS`, `TYPE_VARIABLE` in 3.13. ([Python documentation][1])
Contract rule:

* Keep them in `py_sym_scopes` and in the scope tree
* But default `scope_role="type_meta"` and do not treat them as runtime code units unless your later stages prove they compile to code objects

---

# 4) Exact algorithms

## SYM-ALG-1 — Extract symtable facts to Arrow tables (per file)

**Inputs:** file text `code`, `filename`, `file_id`
**Outputs:** `py_sym_scopes`, `py_sym_symbols`, `py_sym_scope_edges`, `py_sym_namespace_edges`, `py_sym_function_partitions`

Algorithm:

1. `top = symtable.symtable(code, filename, "exec")` ([Python documentation][1])
2. DFS walk:

   * compute `scope_id` using rules above
   * emit scope row (`get_type/get_name/get_lineno/is_nested/is_optimized/has_children`) ([Python documentation][1])
   * emit parent→child edges from `get_children()` ([Python documentation][1])
3. For each table:

   * for each `Symbol s in table.get_symbols()` emit symbol row with all flags ([Python documentation][1])
   * if `s.is_namespace()`:

     * `namespaces = s.get_namespaces()` ([Python documentation][1])
     * for each namespace table `nt` in namespaces:

       * ensure that namespace table has a `scope_id` (you can assign on-demand or via pre-walk)
       * emit `py_sym_namespace_edges(scope_id, symbol_row_id, child_scope_id)`
4. If `scope_type == FUNCTION`:

   * emit `py_sym_function_partitions` row with `get_parameters/get_locals/get_globals/get_nonlocals/get_frees` ([Python documentation][1])

**Key detail:** because `get_namespaces()` returns symbol table objects, you can directly map name → child table without guessing ordering. ([Python documentation][1])

---

## SYM-ALG-2 — Anchor symtable scopes to AST/CST spans (byte offsets)

This is the step that makes symtable “index-grade” per your pipeline rule that byte offsets are the coordinate system. 

**Inputs**

* `py_sym_scopes` (line-based)
* `ast_nodes` dataset with: `(path, kind, name, lineno, end_lineno, span_start_byte, span_end_byte, ast_node_id)`
* CST token line map (or precomputed `line_start_byte[]`)

**Outputs**

* update `py_sym_scopes.anchor_ast_node_id/span_*_byte/anchor_confidence`

Algorithm (per scope row):

1. If `scope_type == MODULE`:

   * anchor to file root span: `(0, file_len_bytes)`
2. If `scope_type in {FUNCTION, CLASS}`:

   * find candidate AST nodes with:

     * same `path`
     * node kind in `{FunctionDef, AsyncFunctionDef}` (for FUNCTION) or `{ClassDef}` (for CLASS)
     * `ast.name == scope_name`
     * `ast.lineno == scope_lineno` (strong match)
   * pick best candidate:

     * prefer exact lineno match
     * then smallest span
3. For meta scopes (`TYPE_ALIAS`, `TYPE_PARAMETERS`, `TYPE_VARIABLE`, `ANNOTATION`):

   * anchor to the owning AST construct if available (TypeAlias node, type parameter list span, annotation expression span)
   * if you can’t isolate a precise span, anchor to the owning function/class node with lower confidence

Emit `anchor_confidence` and keep an `anchor_reason` string for debugging.

---

## SYM-ALG-3 — Build binding slots (`py_sym_bindings`) deterministically

This algorithm turns `py_sym_symbols` into the exact binding nodes your Phase 4 needs. 

**Inputs**

* `py_sym_scopes`, `py_sym_symbols`

**Output**

* `py_sym_bindings`

Rules (per `(scope_id, name)`):

1. Determine **scoping class**:

   * if `is_nonlocal`: `NONLOCAL_REF`
   * elif `is_declared_global` or `is_global`: `GLOBAL_REF`
   * elif `is_free`: `FREE_REF`
   * elif `is_parameter`: `PARAM_LOCAL`
   * elif `is_local`: `LOCAL`
   * else: `UNKNOWN` (still store; useful for diagnostics)
2. Determine **declared_here**:

   * True if any of:

     * `is_parameter`
     * `is_assigned`
     * `is_imported`
     * `is_namespace`
     * `is_annotated`  *(treat “annotation-only” as a binding slot; may be uninitialized at runtime)*
3. Assign `binding_kind`:

   * `param` if `is_parameter`
   * `import` if `is_imported`
   * `namespace` if `is_namespace`
   * `annot_only` if `is_annotated` and not `is_assigned`
   * `local` if `is_local` or `is_assigned`
   * `global_ref` / `nonlocal_ref` / `free_ref` if those flags apply
   * else `unknown`

This gives you a deterministic binding inventory per scope before you even touch AST stores/loads.

---

## SYM-ALG-4 — Resolve FREE/NONLOCAL/GLOBAL bindings to their defining binding

This emits `py_sym_resolution_edges`, i.e., the lexical resolution graph that lets you wire closure captures and global lookups.

**Inputs**

* `py_sym_scopes` with parent relationships
* `py_sym_bindings`

**Output**

* `py_sym_resolution_edges`

Algorithm (per binding `b = (scope_id, name)` where kind ends with `_ref`):

1. Let `ancestors = parent chain of scope_id` (nearest first)
2. If `kind == GLOBAL_REF`:

   * target scope = nearest ancestor with `scope_type == MODULE`
   * dst binding = `(module_scope_id, name)` (create if missing)
   * emit edge kind `GLOBAL`, confidence 1.0
3. If `kind == NONLOCAL_REF`:

   * scan ancestors, but only consider `scope_type == FUNCTION` scopes
   * find first ancestor where `(ancestor_scope_id, name)` has `declared_here=True` (local/param/import/namespace/annot_only)
   * emit edge kind `NONLOCAL`, confidence 1.0
4. If `kind == FREE_REF`:

   * scan ancestors for a declared binding with that name:

     * prefer nearest `FUNCTION` scope
     * then `CLASS` scope
     * then `MODULE`
   * emit edge kind `FREE`, confidence:

     * 1.0 if found in a non-module enclosing scope
     * 0.7 if it falls back to module (since “free vs global” can be subtle depending on construct)
5. If no target found:

   * emit `UNKNOWN` with confidence 0.0 and reason

This is the symtable analogue of “exception edges” in CFG: it makes closure/global resolution explicit and queryable.

---

## SYM-ALG-5 — Use symtable facts in end-to-end CPG construction

Now the “how it’s used” part, mapped to your pipeline phases. 

### Phase 4 — Scopes + bindings (core usage)

Use `py_sym_scopes` + anchors to emit:

* `SCOPE` nodes (one per scope row)
* edges:

  * `OWNS_SCOPE(ast_owner → scope)` (from `anchor_ast_node_id`) 
  * `PARENT_SCOPE(scope_parent → scope_child)` (from `py_sym_scope_edges`) 

Use `py_sym_bindings` to emit:

* `BINDING` nodes (`binding_id`, `name`, flags)
* `DECLARES(scope → binding)` where `declared_here=True` 

Use `py_sym_resolution_edges` to emit:

* `RESOLVES_TO(binding_ref → binding_def, props={kind, confidence})`
* and optionally `CAPTURES(scope → binding_def)` for FREE/NONLOCAL (nice for closure queries)

### Phase 5/6 — CFG/DFG (supporting role)

Your `dis` layer gives you instruction-level def/use events (FAST/GLOBAL/DEREF). Symtable helps you **map those events onto binding nodes** consistently:

* `LOAD_FAST/STORE_FAST` ↔ local/param binding in the current FUNCTION scope
* `LOAD_DEREF/STORE_DEREF` ↔ FREE/NONLOCAL resolution edge targets
* `LOAD_GLOBAL/STORE_GLOBAL` ↔ module binding via GLOBAL resolution edge

So in the DFG builder:

* when you emit `DEFINES_BINDING(df_def → binding)` / `USES_BINDING(df_use → binding)` (Phase 6), symtable resolution edges tell you which binding node is the target. 

### Phase 3 — SCIP attachment (unifying symbol and binding worlds)

Once you have `BINDING` nodes, join them to SCIP `SYMBOL` nodes:

* For each SCIP occurrence anchored to an AST `Name`:

  * if it’s a def site: connect `binding → SYMBOL`
  * if it’s a ref site: connect `Name(Load) → SYMBOL` and also `Name(Load) → binding`

This creates the “bridge” your guide recommends (`BINDING_SYMBOL`) so symbol-based and name-based analyses unify. 

### Phase 10 — Validation (symtable as a correctness oracle)

Use symtable partitions + flags as brutal invariants:

* in FUNCTION scopes:

  * every `LOAD_FAST` name must be `is_local` or `is_parameter`
  * every name in `get_frees()` should correspond to `FREE_REF` and have a resolution edge ([Python documentation][1])
* every `is_nonlocal` must resolve to an enclosing function binding (resolution edge exists)
* every `is_declared_global` must resolve to module binding

This catches binder drift early (exactly the “DFG sanity” principle in your guide). 

---

# 5) Minimal pyarrow implementation skeleton (writer/reader)

You said “pyarrow-centric,” so the typical pattern is:

* extract per file → build `pa.Table` per dataset → write Parquet
* later stages read Parquet and join in DuckDB/Polars.

Sketch:

```python
import pyarrow as pa
import pyarrow.parquet as pq

# After SYM-ALG-1 extraction
t_scopes = pa.Table.from_pylist(scope_rows, schema=scopes_schema)
t_symbols = pa.Table.from_pylist(symbol_rows, schema=symbols_schema)
t_edges = pa.Table.from_pylist(scope_edge_rows, schema=scope_edges_schema)
t_ns = pa.Table.from_pylist(namespace_edge_rows, schema=namespace_edges_schema)

pq.write_table(t_scopes, "py_sym_scopes.parquet")
pq.write_table(t_symbols, "py_sym_symbols.parquet")
pq.write_table(t_edges, "py_sym_scope_edges.parquet")
pq.write_table(t_ns, "py_sym_namespace_edges.parquet")
```

---

## If you want one more increment

The highest-leverage next artifact is a **golden micro-fixture suite** that asserts the *symtable contract outputs* for:

* `global` / `nonlocal`
* nested functions capturing outer locals
* class body name shadowing
* `type` aliases and type parameters (PEP 695)
  …and then cross-checks them against:
* `dis` op families (FAST/GLOBAL/DEREF)
* AST binder outputs (your `BINDS_DEF/BINDS_USE` edges)

That fixture suite is what makes symtable a permanent “correctness backstop” rather than a one-off extractor.

[1]: https://docs.python.org/zh-cn/3.13/library/symtable.html "symtable --- 访问编译器的符号表 — Python 3.13.11 文档"


Below is a **“dis-style” algorithm pack** for **how to use the `symtable` Arrow outputs end-to-end** across your entire CPG build—i.e., not just “Phase 4 scopes/bindings,” but how symtable facts become:

* **SCOPE/BINDING nodes + resolution edges** (compiler-grade),
* the **authoritative resolver** for AST Name(Store/Load) → binding,
* the **bridge** that makes `dis` def/use events land on the same binding nodes (so DFG is coherent),
* and the **validation oracle** that tells you when your binder/DFG is lying.

This is intentionally mechanistic and follows the phased layering in your CPG build guide. 

---

# SYM-CPG-ALG-0 — Inputs, products, and the one invariant

## Required upstream products (you already have)

* AST nodes with stable IDs + `(lineno/end_lineno)` + byte spans
* CST tokens + byte spans
* AST↔CST join by byte span
* SCIP occurrences/symbols mapped to syntax spans 

## Symtable products (from your pyarrow contract)

Per file:

* `py_sym_scopes`
* `py_sym_symbols`
* `py_sym_scope_edges`
* `py_sym_namespace_edges`
* `py_sym_function_partitions` (optional but highly recommended)
* derived:

  * `py_sym_bindings`
  * `py_sym_resolution_edges`

## The invariant (everything must reduce to byte spans)

Symtable is **line-based**. Your CPG is **bytes-first**. So symtable facts are only “index-grade” once you **anchor** them to AST/CST spans. 

---

# SYM-CPG-ALG-1 — Materialize SCOPE nodes and scope edges

This happens in your “Scopes + bindings” phase (Phase 4) but you should treat it as its own deterministic pass. 

## Inputs

* `py_sym_scopes`, `py_sym_scope_edges`
* `ast_nodes` (function/class/module root spans)

## Output (CPG)

* Nodes: `SCOPE`
* Edges: `OWNS_SCOPE(ast_owner → scope)`, `PARENT_SCOPE(scope_parent → scope_child)`

## Algorithm

1. **Create a scope node for every row in `py_sym_scopes`:**

   * `node_id = scope_id`
   * `label = "SCOPE"`
   * `file_id = file_id`
   * `props = {scope_type, scope_name, qualpath, lineno, is_nested, is_optimized}`

2. **Anchor each scope to an AST owner node (fill `span_start/span_end`):**

   * MODULE scope → file root AST node span
   * FUNCTION scope → match `FunctionDef/AsyncFunctionDef` by `(name, lineno)`; pick smallest span if ambiguous
   * CLASS scope → match `ClassDef` by `(name, lineno)`
   * Meta scopes (`ANNOTATION/TYPE_*`) → anchor to the owning class/func/type alias node if you can; else anchor to the nearest parent scope owner with lower confidence (still useful for resolution) 

3. Emit edges:

   * `OWNS_SCOPE(ast_owner_id → scope_id, props={confidence, reason})`
   * For each row in `py_sym_scope_edges`: `PARENT_SCOPE(parent_scope_id → child_scope_id)`

**Why this matters later:** every subsequent “resolve Name → binding” call needs a `scope_id` for the Name’s containing lexical scope.

---

# SYM-CPG-ALG-2 — Materialize BINDING nodes from symtable (don’t guess)

The CPG guide suggests scanning AST to create bindings; symtable gives you a compiler-grade inventory of which names are *in scope* and how they’re classified. Use it to create the canonical binding slots, then use AST to find def/use sites. 

## Inputs

* `py_sym_symbols`
* `py_sym_scopes`

## Outputs (CPG)

* Nodes: `BINDING`
* Edges: `DECLARES(scope → binding)`

## Algorithm

1. Build `py_sym_bindings` deterministically from `py_sym_symbols`:

   * One binding slot per `(scope_id, name)`.
   * `binding_id = f"{scope_id}:BIND:{name}"`
   * `binding_kind`:

     * `param` if `is_parameter`
     * `import` if `is_imported`
     * `namespace` if `is_namespace`
     * `annot_only` if `is_annotated` and not `is_assigned`
     * `local` if `is_local` or `is_assigned`
     * `global_ref` / `nonlocal_ref` / `free_ref` if those flags apply
2. Emit `BINDING` nodes:

   * `node_id = binding_id`
   * `label="BINDING"`
   * `file_id=file_id`
   * `props={name, binding_kind, flags...}` (all symtable booleans are valuable)
3. Emit `DECLARES(scope_id → binding_id)` **iff** `declared_here=True` (param/import/namespace/assigned/annot_only).

**Key point:** this gives you a canonical slot for every name the compiler cares about, including cases AST scanning often misses on day one.

---

# SYM-CPG-ALG-3 — Emit lexical resolution edges (GLOBAL/NONLOCAL/FREE)

This is the symtable equivalent of “exception edges” in CFG: it makes *name resolution* explicit and queryable, and it is the glue between:

* `dis` spaces (`FAST/GLOBAL/DEREF`) and
* your binding nodes.

## Inputs

* `py_sym_bindings`
* `py_sym_scopes` + `py_sym_scope_edges`

## Outputs (CPG)

* Edges: `RESOLVES_TO(binding_ref → binding_def, props={kind, confidence, reason})`
* Optional convenience: `CAPTURES(scope → binding_def)` for closures

## Algorithm (per binding that is a ref-kind)

Let `b = (scope_id, name)` where `binding_kind ∈ {global_ref, nonlocal_ref, free_ref}`.

1. Compute `ancestors(scope_id)` via `PARENT_SCOPE` chain, nearest-first.

2. If `global_ref`:

   * target scope = nearest ancestor with `scope_type == MODULE`
   * dst = `(module_scope_id, name)` binding (create if absent)
   * emit `RESOLVES_TO(b → dst, kind="GLOBAL", confidence=1.0)`

3. If `nonlocal_ref`:

   * scan ancestors where `scope_type == FUNCTION`
   * find first ancestor where `(ancestor_scope_id, name)` is **declared_here**
   * emit `RESOLVES_TO(b → dst, kind="NONLOCAL", confidence=1.0)`
   * if not found: emit `RESOLVES_TO(... kind="UNKNOWN", confidence=0.0)` (and flag for validation)

4. If `free_ref`:

   * scan ancestors, prefer:

     1. nearest FUNCTION where name declared_here
     2. then CLASS
     3. then MODULE
   * emit `RESOLVES_TO(b → dst, kind="FREE", confidence=1.0 if non-module else 0.7)`

5. Optional: emit `CAPTURES(scope_id → dst_binding_id)` when `kind in {"FREE","NONLOCAL"}`.

**Now you have a complete lexical binding graph** that later stages can follow without re-deriving Python’s scoping rules.

---

# SYM-CPG-ALG-4 — Resolve *any* Name occurrence to a binding_id (the core resolver)

This is the function you will call everywhere: AST defs/uses, `dis` def/use events, even some SCIP cases.

## Inputs

* `scope_id` for the Name’s containing scope
* `name` string
* `py_sym_bindings`, `py_sym_resolution_edges`

## Output

* `binding_id` (or “unresolved” with reason)

## Algorithm: `resolve_binding(scope_id, name) -> binding_id`

1. If binding `(scope_id, name)` exists:

   * If its kind is `global_ref/nonlocal_ref/free_ref`:

     * follow the unique `RESOLVES_TO` edge to get the destination binding
     * return destination binding_id
   * Else:

     * return `(scope_id, name)` binding_id
2. Else (no binding row):

   * Fallback policy (deterministic):

     * if current scope is MODULE: create `unknown` binding in module scope and return it (or mark unresolved)
     * else: attempt `global` resolution (module binding) with low confidence
   * record as `UNRESOLVED_BINDING(name, scope_id)` for validation

This is where symtable makes your entire CPG “tight”: every name maps to exactly one slot (or you explicitly record why it didn’t).

---

# SYM-CPG-ALG-5 — Attach AST def/use sites to bindings (BINDS_DEF / BINDS_USE)

This is Phase 4.3 in your guide, but symtable makes it deterministic and lets you avoid subtle scoping bugs. 

## Inputs

* AST nodes for Name occurrences (with ctx=Store/Load and containing scope owner)
* `OWNS_SCOPE` mapping (AST owner → scope_id)
* `resolve_binding(scope_id, name)` from SYM-CPG-ALG-4

## Outputs (CPG)

* `BINDS_DEF(ast_store_name → binding)`
* `BINDS_USE(ast_load_name → binding)`

## Algorithm

For each AST `Name` node:

1. Determine containing `scope_id`:

   * Walk AST ancestors to nearest scope owner (module/function/class/lambda/comprehension)
   * Map owner AST node → `scope_id` via `OWNS_SCOPE`
2. Let `name = ast.Name.id`
3. Let `binding_id = resolve_binding(scope_id, name)`
4. If ctx is Store:

   * emit `BINDS_DEF(name_node_id → binding_id)`
5. If ctx is Load:

   * emit `BINDS_USE(name_node_id → binding_id)`

**Important detail:** `global` and `nonlocal` now work automatically because `resolve_binding` follows the `RESOLVES_TO` edge.

---

# SYM-CPG-ALG-6 — Bridge BINDING ↔ SCIP SYMBOL (unify name-world and symbol-world)

This is Phase 4.3’s “bridge to SCIP” and is what makes symbol-based and name-based queries interchangeable. 

## Inputs

* SCIP `DEFINES/REFERS_TO` edges anchored to AST nodes
* `BINDS_DEF/BINDS_USE` edges (from SYM-CPG-ALG-5)

## Outputs (CPG)

* `BINDING_SYMBOL(binding → SYMBOL)` edges (and/or `SYMBOL_BINDING` reverse edges)

## Algorithm

For each AST anchor node `a` where SCIP attached a symbol `s`:

1. If `a` has outgoing `BINDS_DEF` or `BINDS_USE` edge to binding `b`:

   * emit `BINDING_SYMBOL(b → s, props={source:"scip", role:def|ref, confidence})`
2. If multiple SCIP symbols map to same binding (can happen under ambiguity):

   * keep all edges but lower confidence
   * mark `props.ambiguous=true`

This is the key join that later lets you answer:

* “this symbol’s binding slot”
* “this binding’s symbol (if any)”

---

# SYM-CPG-ALG-7 — Use symtable to *drive* DFG binding selection from `dis` def/use events

This is where symtable stops being “just Phase 4” and becomes the glue that makes `dis`-based DFG coherent.

## Inputs

* `py_bc_defuse_events` (from your `dis` classifier) with `(code_unit_id, instr_id, event_kind, space, name)`
* Mapping `code_unit_id → scope_id`:

  * via byte-span anchor: code unit AST owner ↔ `OWNS_SCOPE` scope (same function)
* `resolve_binding(scope_id, name)` from SYM-CPG-ALG-4

## Outputs (CPG)

* `DEFINES_BINDING(df_def → binding)`
* `USES_BINDING(df_use → binding)`

## Algorithm

For each def/use event row:

1. Determine `scope_id` for that `code_unit_id`:

   * code_unit anchored to AST function/lambda/comprehension node
   * that AST node owns a scope node → scope_id
2. Determine `name` (from `argval_str` in your bytecode tables)
3. Compute `binding_id = resolve_binding(scope_id, name)`:

   * this correctly maps:

     * `FAST` locals/params → local binding in this scope
     * `DEREF` free/nonlocal → resolved outer binding via `RESOLVES_TO`
     * `GLOBAL` → module binding via `RESOLVES_TO`
4. Emit:

   * if event is DEF: `DEFINES_BINDING(df_def → binding_id)`
   * if USE: `USES_BINDING(df_use → binding_id)`
   * kills can be represented as properties or special edges

**Result:** your AST name edges and bytecode def/use edges now talk about the *same* binding slots, so reaching-defs is meaningful. 

---

# SYM-CPG-ALG-8 — Parameter binding wiring (symtable-backed, AST-anchored)

Even if you use `inspect` for signatures, symtable is the compile-time truth for parameters in a scope. It’s also the best sanity check for your parameter binder.

## Inputs

* `py_sym_function_partitions.parameters`
* AST function signature nodes (args list)
* Existing binding nodes `(scope_id, name)` of kind `param`

## Outputs (CPG)

* `PARAM_OF(param_binding → function_def)` (optional)
* `BINDS_DEF(param_ast_node → param_binding)` at function entry (so DFG has a def site)

## Algorithm

For each FUNCTION scope:

1. Let `P = symtable parameters` (ordered tuple)
2. Let `A = AST params list` (posonly, args, kwonly, vararg, kwarg)
3. Align by:

   * name match first
   * order match second
4. For each matched param:

   * `binding_id = resolve_binding(scope_id, param_name)` (should return same scope binding)
   * emit `BINDS_DEF(param_ast_node → binding_id)` with `props={def_kind:"param"}`
5. If symtable has params not in AST (rare; can occur for synthetic code objects):

   * create synthetic param nodes or record as “implicit param” with low confidence

This ensures DFG always has initial definitions for parameters.

---

# SYM-CPG-ALG-9 — How symtable influences call stitching (even if SCIP does most resolution)

Symtable doesn’t resolve calls across modules, but it *does* give you the correct classification of the callee expression’s name:

* is `f` a local? global? free? nonlocal? imported?

You use that to:

* improve heuristics when SCIP is missing,
* rank candidate callees,
* and set confidence.

## Algorithm

For each callsite where callee is a simple `Name(id)`:

1. Let `binding_id = resolve_binding(scope_id, id)`
2. If `binding_kind == namespace` and you have an AST def anchor for that binding:

   * emit `CALLS(callsite → local_def_symbol, confidence=0.8)`
3. If `binding_kind == import`:

   * use import tables (from AST or SCIP) to propose module-qualified candidate symbols
4. Else fallback to SCIP or unknown

This is the “binder + heuristics” layer from Phase 8.2, but symtable makes it correct about lexical scope. 

---

# SYM-CPG-ALG-10 — Validation gates (symtable as the correctness oracle)

This is where symtable pays for itself: cheap, brutal checks.

## A) Binder completeness checks (AST ↔ symtable)

For each scope:

1. For every symbol row where `declared_here=True`:

   * assert there exists at least one `BINDS_DEF` edge to that binding, **or** it’s a parameter/import/namespace where def site is special-cased
2. For every `BINDS_USE` edge:

   * assert the target binding exists and is reachable via `resolve_binding`

## B) Resolution correctness checks (symtable invariants)

1. Every `nonlocal_ref` binding must have exactly one `RESOLVES_TO(kind="NONLOCAL")`
2. Every `global_ref` must resolve to MODULE scope binding
3. Every `free_ref` must resolve to some ancestor binding (or is flagged unresolved)

## C) Bytecode consistency checks (`dis` ↔ symtable)

Per FUNCTION code unit:

1. Every `LOAD_FAST`/`STORE_FAST` name must resolve to a binding that is not `global_ref/nonlocal_ref/free_ref`
2. Every `LOAD_DEREF` name must resolve via a `FREE/NONLOCAL` edge
3. If you store `co_freevars`, assert:

   * `set(co_freevars) == set(py_sym_function_partitions.frees)`

These are the exact kind of “DFG sanity” invariants your guide recommends. 

---

# Putting it together: the end-to-end symtable contribution map

1. **Phase 1–3 (identity/syntax/SCIP):** symtable doesn’t change what you emit, but it sets up stable scope IDs you’ll join to.
2. **Phase 4 (scopes/bindings):** symtable drives SCOPE/BINDING inventory + lexical RESOLVES_TO graph.
3. **Phase 5 (CFG):** symtable mostly irrelevant (CFG is from `dis`/IR), but scope anchors determine CFG “code unit” ownership.
4. **Phase 6 (DFG):** symtable is the resolver that maps both AST name uses and bytecode events onto the same binding nodes.
5. **Phase 7–8 (calls/arg wiring):** symtable improves fallback callee selection and gives parameter truth for wiring.
6. **Phase 10 (validation):** symtable provides compiler-grade invariants that catch drift.

---

If you want the next increment, I can turn these algorithms into a **concrete “symtable integration harness”**:

* a tiny `symtable_contract/` module,
* pytest micro-fixtures for `global/nonlocal/free` + comprehensions + class scopes,
* and golden Parquet snapshots that are cross-checked against your `dis` def/use events and your AST binder edges.
