Below is a compact “high-power query macro surface” I’d preprogram as a **single CLI** (e.g., `cq …`) and then teach Claude Code to use via a `SKILL.md`.

The design assumption is: **every macro emits structured JSON** (plus a short human summary), built by composing:

* `rg --json …` for **fast lexical narrowing + file lists + cheap cross-repo counts** ([iepathos.github.io][1])
* `ast-grep … --json=stream …` for **structural matches with captures + precise ranges** ([ast-grep.github.io][2])
* `python -m ast / symtable / dis …` (or equivalent `python -c …`) for **language-service introspection** ([Python documentation][3])
* and then use Claude Code skill “pre-run” (`!`command) to inject the JSON output directly into context. ([Claude Code][4])

---

## 1) Transitive “who depends on this input?” (impact graph, dataflow-ish)

**One-liner**

```bash
cq impact SYMBOL --param INPUT --root src --depth 6
```

**What it returns (high insight, still bounded)**

* `definitions[]`: all symbol defs (file/range/signature/decorators)
* `callers_direct[]`, `callers_transitive[]` (approx call graph)
* `uses_direct[]`: functions where `INPUT` (or derived vars) is read
* `flows[]`: “INPUT → local alias → call arg → downstream param” links (best-effort)
* `dynamic_hazards[]`: places where resolution is uncertain (e.g., `getattr`, passed-as-callback)
* `top_files_by_dependency_pressure[]`: most affected files

**How it works**

* `rg --json` to find candidate defs/refs quickly (and restrict scope) ([iepathos.github.io][1])
* `ast-grep --json` to extract *def sites* and *call sites* with captures + ranges ([ast-grep.github.io][2])
* Python AST pass to build intra-function “reads/assigns/returns/calls” chains (no execution) ([Python documentation][3])

This is the closest “single command” to your example request.

---

## 2) Call-site census (argument shapes, kwargs usage, starargs, partials)

**One-liner**

```bash
cq calls SYMBOL --root src --group-by argshape --top 50
```

**Returns**

* `call_sites[]`: each with `positional_count`, `kw_names[]`, `has_starargs`, `has_kwargs`
* `argshape_histogram[]`: counts per shape (e.g., `pos=2 kw=[timeout] **kwargs`)
* `high-risk_patterns[]`: heavy `**kwargs`, forwarded `*args`, dynamic callee

**Why it’s differentiated**
Claude is “familiar with grep for name”, but this gives you *signature-compatibility intelligence*.

**Core tooling**
ast-grep structural matching with metavars (and JSON ranges). ([ast-grep.github.io][5])

---

## 3) Signature-change viability (simulate a breaking change before touching code)

**One-liner**

```bash
cq sig-impact SYMBOL --to "new(a, b, *, c=None)" --root src
```

**Returns**

* `would_break[]`: call sites that become invalid under the new signature
* `needs_edit[]`: call sites that can be mechanically migrated
* `ambiguous[]`: calls that require semantic judgment (`**kwargs`, wrappers)
* `migration_plan[]`: suggested rewrite templates (not auto-applied unless asked)

**How it works**

* builds on `cq calls` census + parses the “new signature” string with Python’s parser (no import) ([Python documentation][3])

---

## 4) Import dependency graph + cycles + “blast radius of moving a module”

**One-liner**

```bash
cq imports --root src --cycles --focus MODULE_OR_PATH
```

**Returns**

* `import_edges[]`: `(from → to, kind=import/importfrom, names)`
* `cycles[]`: strongly connected components with concrete import lines
* `layering_violations[]`: if you provide optional “layer rules”
* `move_impact[]`: “if MODULE moves/renames, these imports break”

**Tooling**
Python AST extraction of import nodes; no execution. ([Python documentation][3])

---

## 5) Import-time side effects & global state mutation map

**One-liner**

```bash
cq side-effects --root src --severity high
```

**Returns**

* `import_time_calls[]`: function calls at module top-level (not under def/class)
* `global_writes[]`: assignments to globals / module state, with writer functions
* `env_reads[]` / `argv_reads[]` / `cwd_reads[]`: heuristically detected “ambient dependency” points
* `risk_summary`: “modules that are unsafe to import in tooling / tests”

**Why it’s differentiated**
This is *change-viability gold*: it tells you whether analysis/refactors will trip runtime behavior during imports.

**Tooling**
ast-grep structural patterns + symbol-table confirmation for globals. ([ast-grep.github.io][5])

---

## 6) Closure + scope capture analysis (refactor safety for nested funcs/classes)

**One-liner**

```bash
cq scopes FILE_OR_SYMBOL --root src
```

**Returns**

* `free_vars[]`, `cell_vars[]`, `globals_used[]`, `nonlocals[]`
* `nested_defs[]`: nested functions/classes and what they capture
* `refactor_hazards[]`: “moving this def will break due to closure/global reliance”

**Tooling**
`python -m symtable` (3.13+) or equivalent programmatic use. ([Python documentation][6])

---

## 7) Exception surface: raise/catch matrix + (approx) propagation

**One-liner**

```bash
cq exceptions --root src --focus SYMBOL --propagate-depth 4
```

**Returns**

* `raises[]`: exception types raised per function (best-effort)
* `catches[]`: exception types caught per function, including bare `except`
* `uncaught_paths[]`: call chains where exceptions likely escape
* `policy_violations[]`: e.g., “swallowing Exception”, “bare except”, etc. (if you enable rules)

**Tooling**
AST for `raise` + `try/except`, plus call edges from structural search. ([Python documentation][3])

---

## 8) Async + concurrency hazards (blocking-in-async, missing await, thread leaks)

**One-liner**

```bash
cq async-hazards --root src --profiles "requests,time.sleep,subprocess" --top 200
```

**Returns**

* `blocking_in_async[]`: async defs containing known-blocking calls
* `suspicious_calls[]`: sync I/O called in async context (heuristic)
* `await_misuse[]`: coroutine created but never awaited (best-effort)
* `hotspots[]`: functions most likely to deadlock/hang CI

**Tooling**
Primarily ast-grep structural queries + a small AST pass for context (inside async defs). ([GitHub][7])

---

## 9) Bytecode “semantic surface” (hidden dependencies: globals/attrs/constants/opcodes)

**One-liner**

```bash
cq bytecode-surface FILE_OR_SYMBOL --root src --show "globals,attrs,constants,calls"
```

**Returns**

* `loaded_globals[]`, `loaded_attrs[]`: names referenced at runtime
* `constants[]`: string/int literals used (often reveals config keys, env vars, SQL snippets)
* `opcode_stats[]`: quick complexity proxy (jumps/loops/exception handling)
* `compare_versions[]` (optional): if you point it at two git revs/files

**Tooling**
Compile-only + disassembly via `python -m dis` options (no execution required if you compile source yourself). ([Python documentation][8])

This is a big differentiator because it catches “behavioral coupling” that plain text search misses.

---

# How to make Claude Code actually use these (without bloating the skill surface)

In the `SKILL.md`, you want two things:

1. **A tiny, memorable verb set** (the 9 above).
2. Use **dynamic context injection** so Claude gets results immediately:

* Claude Code supports `!`command preprocessing and tool allowlists in skills. ([Claude Code][4])
* For structured outputs, prefer streaming JSON modes:

  * ast-grep `--json=stream` ([ast-grep.github.io][2])
  * ripgrep `--json` emits message types (`match`, `begin/end`, `summary`) suited for parsing and summaries. ([iepathos.github.io][1])



[1]: https://iepathos.github.io/ripgrep/output-formats/ "Output Formats - ripgrep User Guide"
[2]: https://ast-grep.github.io/reference/cli/scan.html "ast-grep scan | ast-grep"
[3]: https://docs.python.org/3/library/ast.html "ast — Abstract syntax trees — Python 3.14.2 documentation"
[4]: https://code.claude.com/docs/en/skills "Extend Claude with skills - Claude Code Docs"
[5]: https://ast-grep.github.io/guide/tools/json.html "JSON Mode | ast-grep"
[6]: https://docs.python.org/3/library/symtable.html "symtable — Access to the compiler’s symbol tables — Python 3.14.2 documentation"
[7]: https://github.com/ast-grep/ast-grep?utm_source=chatgpt.com "ast-grep/ast-grep: ⚡A CLI tool for code structural search, ..."
[8]: https://docs.python.org/3/library/dis.html "dis — Disassembler for Python bytecode — Python 3.14.2 documentation"

Yes — you can build a **single “cq” CLI** that runs `rg` + `ast-grep scan` + Python’s language-services (AST / symtable / dis) **and prints a Markdown report to stdout** (no JSON files to open). Internally it can still parse JSON streams for precision.

Below is a representative “core design” snippet + then, for **each of the 9 macros**, the **exact underlying CLI calls** (`rg`, `ast-grep scan`, and `python -m …` equivalents) the script would run.

---

## A. Core architecture snippet (shared by all 9 queries)

````python
#!/usr/bin/env python3
"""
cq: compositional code-query macros for Claude Code.

Goals:
- One-line user-facing macros: `cq impact foo --param x --root src`
- Internally compose: rg (fast narrowing) + ast-grep scan (structural) + Python AST/symtable/dis
- Output: Markdown report to stdout; optional JSON artifact saved automatically.
"""
from __future__ import annotations

import argparse
import ast
import dis
import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

# -------------------------
# Toolchain + subprocess
# -------------------------

@dataclass(frozen=True)
class Toolchain:
    rg: str
    sg: str  # ast-grep binary: "ast-grep" or "sg"
    py: str  # python binary to use for module CLIs (optional)

    @staticmethod
    def detect() -> "Toolchain":
        rg = shutil.which("rg") or "rg"
        sg = shutil.which("ast-grep") or shutil.which("sg") or "ast-grep"
        py = sys.executable  # or override via env CQ_PYTHON
        py = os.environ.get("CQ_PYTHON", py)
        return Toolchain(rg=rg, sg=sg, py=py)

def run_lines(cmd: Sequence[str], *, input_text: Optional[str]=None,
              ok_codes: Tuple[int, ...]=(0, 1)) -> Iterator[str]:
    """
    Stream stdout line-by-line.
    NOTE: ast-grep scan uses exit code 1 to mean "matches found" (not error).
    """
    p = subprocess.Popen(
        list(cmd),
        stdin=subprocess.PIPE if input_text is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if input_text is not None:
        assert p.stdin is not None
        p.stdin.write(input_text)
        p.stdin.close()

    assert p.stdout is not None
    for line in p.stdout:
        yield line.rstrip("\n")

    out_err = p.stderr.read() if p.stderr else ""
    rc = p.wait()
    if rc not in ok_codes:
        raise RuntimeError(f"command failed rc={rc}\ncmd={cmd}\n\nstderr:\n{out_err}")

# -------------------------
# rg: fast narrowing
# -------------------------

def rg_files(tc: Toolchain, pattern: str, root: Path, *, glob_py: bool=True) -> List[Path]:
    """
    Narrow candidate files quickly.
    Uses rg --json so we can derive files from 'begin' messages.
    (rg JSON emits begin/end/match/context/summary messages.)
    """
    cmd = [tc.rg, "--json"]
    if glob_py:
        cmd += ["-g", "*.py"]
    cmd += [pattern, str(root)]
    files: List[Path] = []
    seen: set[str] = set()
    for line in run_lines(cmd, ok_codes=(0, 1, 2)):  # rg: 0 no matches, 1 matches, 2 error
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if obj.get("type") == "begin":
            p = obj.get("data", {}).get("path", {})
            path_text = p.get("text")
            if path_text and path_text not in seen:
                seen.add(path_text)
                files.append(Path(path_text))
    return files

# -------------------------
# ast-grep scan: structural matches
# -------------------------

def sg_scan_inline(tc: Toolchain, inline_rules_yaml: str, paths: Sequence[Path]) -> Iterator[Dict[str, Any]]:
    """
    ast-grep scan with --inline-rules and JSON streaming output.
    Each line is a JSON object when using --json=stream.
    """
    cmd = [
        tc.sg, "scan",
        "--inline-rules", inline_rules_yaml,
        "--json=stream",
        "--include-metadata",
        "--globs", "*.py",
    ] + [str(p) for p in paths]
    for line in run_lines(cmd, ok_codes=(0, 1)):
        if not line:
            continue
        try:
            yield json.loads(line)
        except Exception:
            continue

# -------------------------
# Python parsing helpers
# -------------------------

@dataclass
class FnDef:
    file: Path
    name: str
    qual: str
    node: ast.AST
    params: List[str]

@dataclass
class CallSite:
    file: Path
    fn_qual: str
    lineno: int
    col: int
    callee_text: str
    callee_head: str  # last attr or name
    pos_args: int
    kw_names: List[str]
    has_starargs: bool
    has_kwargs: bool

def parse_py(file: Path) -> ast.Module:
    src = file.read_text(encoding="utf-8", errors="replace")
    return ast.parse(src, filename=str(file))

def fn_params(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> List[str]:
    # positional-only + args + kw-only (names only; you can extend to defaults/annotations)
    params = [a.arg for a in getattr(fn.args, "posonlyargs", [])]
    params += [a.arg for a in fn.args.args]
    params += [a.arg for a in fn.args.kwonlyargs]
    if fn.args.vararg:
        params.append("*" + fn.args.vararg.arg)
    if fn.args.kwarg:
        params.append("**" + fn.args.kwarg.arg)
    return params

def callee_head(expr: ast.AST) -> str:
    # return the “head” identifier: foo(...) -> foo ; obj.foo(...) -> foo
    if isinstance(expr, ast.Name):
        return expr.id
    if isinstance(expr, ast.Attribute):
        return expr.attr
    return "<dynamic>"

def unparse(expr: ast.AST) -> str:
    try:
        return ast.unparse(expr)  # py>=3.9
    except Exception:
        return expr.__class__.__name__

def index_defs_and_calls(files: Sequence[Path]) -> Tuple[List[FnDef], List[CallSite]]:
    defs: List[FnDef] = []
    calls: List[CallSite] = []
    for f in files:
        try:
            mod = parse_py(f)
        except SyntaxError:
            continue

        # Walk with a current function stack for call attribution
        stack: List[str] = []
        class Visitor(ast.NodeVisitor):
            def visit_ClassDef(self, node: ast.ClassDef):
                stack.append(node.name)
                self.generic_visit(node)
                stack.pop()

            def visit_FunctionDef(self, node: ast.FunctionDef):
                qual = ".".join(stack + [node.name]) if stack else node.name
                defs.append(FnDef(file=f, name=node.name, qual=qual, node=node, params=fn_params(node)))
                stack.append(node.name)
                self.generic_visit(node)
                stack.pop()

            def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
                qual = ".".join(stack + [node.name]) if stack else node.name
                defs.append(FnDef(file=f, name=node.name, qual=qual, node=node, params=fn_params(node)))
                stack.append(node.name)
                self.generic_visit(node)
                stack.pop()

            def visit_Call(self, node: ast.Call):
                fnq = ".".join(stack) if stack else "<module>"
                head = callee_head(node.func)
                kw_names = [k.arg for k in node.keywords if k.arg is not None]
                has_kwargs = any(k.arg is None for k in node.keywords)
                has_starargs = any(isinstance(a, ast.Starred) for a in node.args)
                calls.append(CallSite(
                    file=f,
                    fn_qual=fnq,
                    lineno=getattr(node, "lineno", 0),
                    col=getattr(node, "col_offset", 0),
                    callee_text=unparse(node.func),
                    callee_head=head,
                    pos_args=len(node.args),
                    kw_names=kw_names,
                    has_starargs=has_starargs,
                    has_kwargs=has_kwargs,
                ))
                self.generic_visit(node)

        Visitor().visit(mod)
    return defs, calls

# -------------------------
# Markdown reporting
# -------------------------

class MD:
    def __init__(self) -> None:
        self.lines: List[str] = []

    def h(self, level: int, s: str) -> None:
        self.lines.append("#" * level + " " + s)

    def p(self, s: str="") -> None:
        self.lines.append(s)

    def bullet(self, s: str) -> None:
        self.lines.append(f"- {s}")

    def code(self, s: str) -> None:
        self.lines.append("```")
        self.lines += s.splitlines()
        self.lines.append("```")

    def render(self) -> str:
        return "\n".join(self.lines) + "\n"

# -------------------------
# 9 macro entrypoints
# -------------------------

def cmd_calls(args, tc: Toolchain) -> str:
    # Narrow by symbol text; then parse AST for detailed arg-shapes
    files = rg_files(tc, rf"\b{re.escape(args.symbol)}\b", Path(args.root))
    defs, calls = index_defs_and_calls(files)

    hits = [c for c in calls if c.callee_head == args.symbol]
    hist: Dict[str, int] = {}
    for c in hits:
        key = f"pos={c.pos_args} kw={sorted(c.kw_names)} *={c.has_starargs} **={c.has_kwargs}"
        hist[key] = hist.get(key, 0) + 1

    md = MD()
    md.h(1, f"calls: {args.symbol}")
    md.bullet(f"candidate_files={len(files)} call_sites={len(hits)}")
    md.h(2, "Top arg-shapes")
    for k, v in sorted(hist.items(), key=lambda kv: kv[1], reverse=True)[: args.top]:
        md.bullet(f"{v:>5}  {k}")
    md.h(2, "Evidence (first N)")
    for c in hits[: args.evidence]:
        md.bullet(f"{c.file}:{c.lineno}:{c.col}  in {c.fn_qual}  callee={c.callee_text}")
    return md.render()

# The remaining 8 are similar “macro shells”:
# - each macro:
#   1) narrows files with rg
#   2) optionally runs sg_scan_inline for structural extraction
#   3) parses AST/symtable/dis as needed
#   4) prints Markdown report

# -------------------------
# CLI wiring
# -------------------------

def main(argv: Optional[Sequence[str]]=None) -> int:
    tc = Toolchain.detect()
    ap = argparse.ArgumentParser(prog="cq")
    ap.add_argument("--root", default=".", help="repo root/path to scan")
    sp = ap.add_subparsers(dest="cmd", required=True)

    p = sp.add_parser("calls", help="call-site census (arg shapes, kwargs, starargs)")
    p.add_argument("symbol")
    p.add_argument("--top", type=int, default=25)
    p.add_argument("--evidence", type=int, default=40)

    # (Add parsers for: impact, sig-impact, imports, side-effects, scopes, exceptions, async-hazards, bytecode-surface)

    args = ap.parse_args(argv)
    if args.cmd == "calls":
        sys.stdout.write(cmd_calls(args, tc))
        return 0
    raise SystemExit("unreachable")

if __name__ == "__main__":
    raise SystemExit(main())
````

**Key architecture elements shown**:

* `rg --json` used only to **derive candidate files quickly**, without requiring `-l`. (Ripgrep’s JSON mode emits typed messages including `begin/end/match/context/summary`.)
* `ast-grep scan --inline-rules … --json=stream` is supported and streams **one JSON object per line** (easy to parse in-memory). Also: `--json=stream` requires the `=` form.
* Markdown report builder `MD` emits a stable, Claude-friendly format to stdout.

(Reference: `ast-grep scan` supports `--inline-rules` and `--json=stream`; and the JSON stream format is one JSON object per line. Also note the `=` requirement for `--json=stream`.) ([ast-grep.github.io][1])
(Reference: ripgrep JSON output message types and constraints.) ([ripgrepy.readthedocs.io][2])

---

## B. For each of the 9 queries: “what cq runs under the hood”

For each macro below I’ll give:

1. the **user-facing one-liner** (`cq …`)
2. the **representative underlying CLI pipeline** (`rg` + `ast-grep scan` + `python -m …` equivalents)
3. the **minimal “core logic” snippet** (the part that makes the query meaningfully differentiated)

> Notes:
>
> * In practice you’ll call Python’s `ast/symtable/dis` modules directly (faster, no subprocess). I’ll still show the **equivalent `python -m …` commands** because you asked for exact CLI commands.
> * If your system uses `sg` instead of `ast-grep`, replace `ast-grep` with `sg`.

---

# 1) `impact`: “who depends on this input?” (approx taint + call propagation)

### One-liner

```bash
cq impact TARGET_FN --param INPUT --root src --depth 6
```

### Under-the-hood CLI pipeline

**Narrow likely files**

```bash
rg --json -g '*.py' '\bTARGET_FN\b' src
```

**Structural extraction (direct + attribute calls)**

```bash
ast-grep scan --inline-rules '
id: call-direct
language: python
rule:
  pattern: TARGET_FN($$$ARGS)
---
id: call-attr
language: python
rule:
  pattern: $OBJ.TARGET_FN($$$ARGS)
' --json=stream --include-metadata --globs '*.py' src
```

**Python language-services equivalents**

```bash
python -m ast path/to/file.py
python -m dis path/to/file.py
python -m symtable path/to/file.py
```

### Core differentiated logic (taint edges)

```python
def names_in_expr(e: ast.AST) -> set[str]:
    out: set[str] = set()
    class V(ast.NodeVisitor):
        def visit_Name(self, n: ast.Name): out.add(n.id)
    V().visit(e)
    return out

def call_taints_param(call: ast.Call, tainted: set[str]) -> bool:
    # conservative: if any arg expr references a tainted name, we treat it as dependent
    for a in call.args:
        if names_in_expr(a) & tainted:
            return True
    for k in call.keywords:
        if k.value and (names_in_expr(k.value) & tainted):
            return True
    return False

def propagate_impact(root_fn: FnDef, all_defs: Dict[str, FnDef], file_mod: ast.Module, param: str) -> List[Tuple[str,str]]:
    """
    Returns edges: (caller_qual -> callee_head) where caller passes tainted data.
    Extend this by binding tainted -> specific callee param names (pos/kw mapping).
    """
    edges: List[Tuple[str,str]] = []
    tainted = {param}

    class V(ast.NodeVisitor):
        def visit_Assign(self, n: ast.Assign):
            # if rhs is tainted, mark lhs names tainted
            rhs = names_in_expr(n.value)
            if rhs & tainted:
                for t in n.targets:
                    tainted.update(names_in_expr(t))
            self.generic_visit(n)
        def visit_Call(self, n: ast.Call):
            if call_taints_param(n, tainted):
                edges.append((root_fn.qual, callee_head(n.func)))
            self.generic_visit(n)

    V().visit(root_fn.node)
    return edges
```

**What makes this “complex”**: it gives you a *dataflow-ish* impact approximation (not just “calls”), and it can be extended to bind positional/keyword argument taint to callee parameter names.

---

# 2) `calls`: call-site census (arg shapes, kwargs, forwarding)

### One-liner

```bash
cq calls SYMBOL --root src --group-by argshape --top 50
```

### Under-the-hood CLI pipeline

```bash
rg --json -g '*.py' '\bSYMBOL\b' src
ast-grep scan --inline-rules '
id: calls-direct
language: python
rule:
  pattern: SYMBOL($$$ARGS)
---
id: calls-attr
language: python
rule:
  pattern: $X.SYMBOL($$$ARGS)
' --json=stream --include-metadata --globs '*.py' src
```

### Core logic (already shown in the shared snippet)

* count `(positional_count, kw_names, has_starargs, has_kwargs)`
* emit histogram + “evidence anchors”

---

# 3) `sig-impact`: simulate a signature change and classify breakage

### One-liner

```bash
cq sig-impact SYMBOL --to "new(a, b, *, c=None)" --root src
```

### Under-the-hood CLI pipeline

Same as `calls` (call-site census), plus parse the new signature.

```bash
rg --json -g '*.py' '\bSYMBOL\b' src
# (call extraction as in #2)
python -c 'import ast; ast.parse("def _tmp(a,b,*,c=None):\n  pass\n")'
```

### Core logic (positional/kw compatibility check)

```python
def parse_sig(sig: str) -> ast.arguments:
    # accept "new(a,b,*,c=None)" or "(a,b,*,c=None)"
    m = re.match(r"^\s*[A-Za-z_]\w*\s*\((.*)\)\s*$", sig)
    inside = m.group(1) if m else sig.strip().strip("()")
    tmp = f"def _tmp({inside}):\n  pass\n"
    fn = ast.parse(tmp).body[0]
    assert isinstance(fn, ast.FunctionDef)
    return fn.args

def call_compatible(call: ast.Call, new_args: ast.arguments) -> Tuple[bool, str]:
    # representative: you’ll extend for posonly/kwonly/defaults/varargs precisely
    pos = len(call.args)
    kws = {k.arg for k in call.keywords if k.arg}
    has_kwargs = any(k.arg is None for k in call.keywords)
    # quick fail: unknown kw when no **kwargs allowed
    allowed_kw = {a.arg for a in new_args.args} | {a.arg for a in new_args.kwonlyargs}
    if not has_kwargs and (kws - allowed_kw):
        return (False, f"unknown_kw={sorted(kws-allowed_kw)}")
    return (True, "ok")
```

**Differentiator**: the output is “would break / auto-fixable / ambiguous”, not just a list of occurrences.

---

# 4) `imports`: import dependency graph + cycles + move/rename impact

### One-liner

```bash
cq imports --root src --cycles --focus package.module
```

### Under-the-hood CLI pipeline

```bash
# optional narrowing for a focus module:
rg --json -g '*.py' '(^|\s)from\s+package\.module\s+import|(^|\s)import\s+package\.module' src
# for structural extraction (optional):
ast-grep scan --inline-rules '
id: import
language: python
rule: { pattern: import $A }
---
id: importfrom
language: python
rule: { pattern: from $A import $$$B }
' --json=stream --include-metadata --globs '*.py' src
```

### Core logic (AST import edges + cycle detection)

```python
def import_edges(mod: ast.Module, file: Path) -> List[Tuple[str,str,str]]:
    edges = []
    for n in mod.body:
        if isinstance(n, ast.Import):
            for a in n.names:
                edges.append((str(file), a.name, "import"))
        elif isinstance(n, ast.ImportFrom):
            if n.module:
                edges.append((str(file), n.module, "from"))
    return edges

def find_cycles(graph: Dict[str, List[str]]) -> List[List[str]]:
    # SCC-lite for representative snippet (replace with Tarjan/Kosaraju)
    cycles = []
    visiting, visited = set(), set()
    stack: List[str] = []
    def dfs(u: str):
        visiting.add(u); stack.append(u)
        for v in graph.get(u, []):
            if v in visiting:
                i = stack.index(v)
                cycles.append(stack[i:] + [v])
            elif v not in visited:
                dfs(v)
        stack.pop(); visiting.remove(u); visited.add(u)
    for u in graph:
        if u not in visited:
            dfs(u)
    return cycles
```

---

# 5) `side-effects`: import-time calls + global state writes + ambient reads

### One-liner

```bash
cq side-effects --root src --severity high
```

### Under-the-hood CLI pipeline

```bash
# high-recall narrowing (optional):
rg --json -g '*.py' '(^|\n)(import|from)\s|os\.environ|sys\.argv|Path\(|open\(' src
# structural pre-scan for obvious top-level calls (optional):
ast-grep scan --inline-rules '
id: suspicious-calls
language: python
rule:
  any:
    - { pattern: open($$$A) }
    - { pattern: requests.get($$$A) }
    - { pattern: time.sleep($A) }
' --json=stream --include-metadata --globs '*.py' src
```

### Core logic (module-top-level statements only)

```python
TOP_SIDE_EFFECT_NODES = (ast.Expr, ast.Assign, ast.AugAssign, ast.AnnAssign, ast.With, ast.Raise)

def module_side_effects(mod: ast.Module) -> List[Tuple[str,str,int]]:
    out = []
    for n in mod.body:
        # ignore def/class/imports; focus on executable statements
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Import, ast.ImportFrom)):
            continue
        if isinstance(n, TOP_SIDE_EFFECT_NODES):
            out.append((n.__class__.__name__, ast.get_source_segment("", n) or "", getattr(n, "lineno", 0)))
    return out
```

---

# 6) `scopes`: closure capture + globals/nonlocals (symtable-driven refactor hazards)

### One-liner

```bash
cq scopes FILE_OR_SYMBOL --root src
```

### Under-the-hood CLI pipeline

```bash
python -m symtable path/to/file.py
# or if you need it for many files:
rg --json -g '*.py' 'def\s|class\s' src
```

### Core logic (symtable traversal)

```python
import symtable

def symtable_report(file: Path) -> Dict[str, Any]:
    src = file.read_text(encoding="utf-8", errors="replace")
    st = symtable.symtable(src, str(file), "exec")
    def walk(t) -> List[Dict[str, Any]]:
        rows = [{
            "name": t.get_name(),
            "type": t.get_type(),
            "free": sorted([s.get_name() for s in t.get_symbols() if s.is_free()]),
            "globals": sorted([s.get_name() for s in t.get_symbols() if s.is_global()]),
            "nonlocals": sorted([s.get_name() for s in t.get_symbols() if s.is_nonlocal()]),
        }]
        for c in t.get_children():
            rows.extend(walk(c))
        return rows
    return {"tables": walk(st)}
```

---

# 7) `exceptions`: raise/catch matrix + (approx) propagation through call edges

### One-liner

```bash
cq exceptions --root src --focus SYMBOL --propagate-depth 4
```

### Under-the-hood CLI pipeline

```bash
rg --json -g '*.py' '\bSYMBOL\b|raise\s|except\s' src
ast-grep scan --inline-rules '
id: raises
language: python
rule: { pattern: raise $E }
---
id: excepts
language: python
rule: { pattern: except $E: $$$B }
' --json=stream --include-metadata --globs '*.py' src
```

### Core logic (extract raise/catch)

```python
def exc_name(e: ast.AST) -> str:
    if isinstance(e, ast.Name): return e.id
    if isinstance(e, ast.Attribute): return f"{unparse(e.value)}.{e.attr}"
    return "<dynamic>"

def exceptions_in_fn(fn: ast.AST) -> Tuple[List[str], List[str]]:
    raises, catches = [], []
    class V(ast.NodeVisitor):
        def visit_Raise(self, n: ast.Raise):
            if n.exc: raises.append(exc_name(n.exc))
        def visit_ExceptHandler(self, n: ast.ExceptHandler):
            if n.type: catches.append(exc_name(n.type))
            else: catches.append("<bare>")
            self.generic_visit(n)
    V().visit(fn)
    return raises, catches
```

---

# 8) `async-hazards`: blocking-in-async + missing awaits + “suspicious sync IO”

### One-liner

```bash
cq async-hazards --root src --profiles "time.sleep,requests.get,subprocess.run"
```

### Under-the-hood CLI pipeline

```bash
# Very effective structural filter: async defs that contain a known blocking call
ast-grep scan --inline-rules '
id: blocking-in-async
language: python
rule:
  pattern: async def $F($$$ARGS): $$$BODY
  has:
    any:
      - { pattern: time.sleep($A) }
      - { pattern: requests.get($$$A) }
      - { pattern: subprocess.run($$$A) }
    stopBy: end
' --json=stream --include-metadata --globs '*.py' src
```

### Core logic (AST confirms context + classifies)

```python
BLOCKING = {"time.sleep", "requests.get", "subprocess.run"}

def callee_fqn(expr: ast.AST) -> str:
    if isinstance(expr, ast.Name): return expr.id
    if isinstance(expr, ast.Attribute): return f"{callee_fqn(expr.value)}.{expr.attr}"
    return "<dynamic>"

def async_blocking_calls(fn: ast.AsyncFunctionDef) -> List[Tuple[int,str]]:
    out = []
    class V(ast.NodeVisitor):
        def visit_Call(self, n: ast.Call):
            fqn = callee_fqn(n.func)
            if fqn in BLOCKING:
                out.append((n.lineno, fqn))
            self.generic_visit(n)
    V().visit(fn)
    return out
```

---

# 9) `bytecode-surface`: globals/attrs/constants/opcodes (no execution)

### One-liner

```bash
cq bytecode-surface FILE_OR_SYMBOL --root src --show globals,attrs,constants,calls
```

### Under-the-hood CLI pipeline

```bash
# optional narrowing by symbol:
rg --json -g '*.py' '\bSYMBOL\b' src

# reference disassembly if you want a direct CLI view:
python -m dis path/to/file.py
```

### Core logic (compile → walk code objects → dis instructions)

```python
def walk_codeobjs(co, prefix="<module>") -> Iterator[Tuple[str, Any]]:
    yield (prefix, co)
    for c in co.co_consts:
        if hasattr(c, "co_code"):  # nested code object
            name = getattr(c, "co_qualname", None) or c.co_name
            yield from walk_codeobjs(c, f"{prefix}.{name}")

def bytecode_surface(file: Path) -> Dict[str, Any]:
    src = file.read_text(encoding="utf-8", errors="replace")
    co = compile(src, str(file), "exec")  # no execution
    out = {}
    for path, c in walk_codeobjs(co):
        g, a, consts, ops = set(), set(), set(), {}
        for ins in dis.get_instructions(c):
            ops[ins.opname] = ops.get(ins.opname, 0) + 1
            if ins.opname in {"LOAD_GLOBAL", "LOAD_NAME"} and isinstance(ins.argval, str):
                g.add(ins.argval)
            if ins.opname in {"LOAD_ATTR", "LOAD_METHOD"} and isinstance(ins.argval, str):
                a.add(ins.argval)
        for k in c.co_consts:
            if isinstance(k, (str, int, float, bytes)):
                consts.add(k)
        out[path] = {
            "globals": sorted(g),
            "attrs": sorted(a),
            "constants_sample": list(sorted(consts, key=lambda x: str(x)))[:50],
            "opcode_stats": dict(sorted(ops.items(), key=lambda kv: kv[1], reverse=True)[:25]),
        }
    return out
```

---

## What I’d do next (so this is actually usable immediately)

1. Implement each macro as a function returning:

* `report_md: str` (stdout)
* `artifact_json: dict` (optional, auto-saved to `.cq/artifacts/<cmd>-<ts>.json`)

2. Put the binary at:

* `scripts/cq` (bash wrapper that calls `python -m tools.cq …`)

3. In `SKILL.md`, your slash commands call:

* `!./scripts/cq impact …`
  so Claude gets the Markdown report injected directly (no file opens).

If you want, I can turn the above into a concrete repo-ready skeleton (files + minimal implementations for all 9 subcommands + consistent Markdown report sections + a stable JSON schema behind the scenes).

[1]: https://ast-grep.github.io/reference/cli/scan.html "ast-grep scan | ast-grep"
[2]: https://ripgrepy.readthedocs.io/ "Welcome to ripgrepy’s documentation! — ripgrepy  documentation"

Below is a **drop-in, repo-ready skeleton** with:

* **9 subcommands**: `impact`, `calls`, `sig-impact`, `imports`, `side-effects`, `scopes`, `exceptions`, `async-hazards`, `bytecode-surface`
* **Consistent Markdown report** to stdout (Claude-friendly)
* **Stable JSON artifact** saved under `.cq/artifacts/` by default (`schema_version = cq.v1`)
* Uses **system CLI tooling** when present (`rg`, optional `ast-grep`/`sg`) + Python stdlib (`ast`, `symtable`, `dis/compile`)

## 0) Install into your repo

Create these files:

```
scripts/cq
tools/__init__.py
tools/cq/__init__.py
tools/cq/__main__.py
tools/cq/cli.py
.claude/skills/cq/SKILL.md   (optional)
```

Then:

```bash
chmod +x scripts/cq
./scripts/cq --help
./scripts/cq calls myfunc --root src
```

---

## 1) `scripts/cq`

```bash
#!/usr/bin/env bash
set -euo pipefail

# Resolve repo root (prefer git), then run cq via project python.
ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$ROOT"

# Choose python interpreter:
# 1) explicit CQ_PYTHON
# 2) .venv/bin/python (common convention)
# 3) python3 / python
PY="${CQ_PYTHON:-}"
if [[ -z "${PY}" ]]; then
  if [[ -x "${ROOT}/.venv/bin/python" ]]; then
    PY="${ROOT}/.venv/bin/python"
  else
    PY="$(command -v python3 || command -v python)"
  fi
fi

exec "${PY}" -m tools.cq.cli "$@"
```

---

## 2) `tools/__init__.py`

```python
# package marker
```

---

## 3) `tools/cq/__init__.py`

```python
"""
cq: compositional code-query macros.

Entry point: python -m tools.cq.cli
"""
from .cli import SCHEMA_VERSION
```

---

## 4) `tools/cq/__main__.py`

```python
from .cli import main

if __name__ == "__main__":
    raise SystemExit(main())
```

---

## 5) `tools/cq/cli.py`  (single-file skeleton: schema + report + macros + CLI)

```python
from __future__ import annotations

import argparse
import ast
import dis
import json
import re
import symtable
import subprocess
import time
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple

# =============================================================================
# Stable JSON schema (v1)
# =============================================================================

SCHEMA_VERSION = "cq.v1"

@dataclass
class RunMeta:
    cmd: str
    argv: List[str]
    root: str
    started_at: str
    duration_ms: int
    tool_versions: Dict[str, str] = field(default_factory=dict)

@dataclass
class Anchor:
    path: str
    line: int
    col: int
    label: str = ""
    snippet: str = ""

@dataclass
class Finding:
    id: str
    title: str
    detail: str = ""
    severity: str = "info"  # info|warn|error
    anchors: List[Anchor] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)

@dataclass
class Section:
    title: str
    summary: str = ""
    findings: List[Finding] = field(default_factory=list)

@dataclass
class Artifact:
    kind: str   # json|html|other
    path: str
    description: str = ""

@dataclass
class CqResult:
    schema_version: str
    run: RunMeta
    summary: Dict[str, Any] = field(default_factory=dict)
    key_findings: List[str] = field(default_factory=list)
    evidence: List[Anchor] = field(default_factory=list)
    sections: List[Section] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)
    artifacts: List[Artifact] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

# =============================================================================
# Utilities: time / io / subprocess
# =============================================================================

def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def ms() -> int:
    return int(time.time() * 1000)

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def read_text(p: Path) -> str:
    return p.read_text(encoding="utf-8", errors="replace")

def which(cmd: str) -> Optional[str]:
    from shutil import which as _which
    return _which(cmd)

def run_cmd(
    cmd: Sequence[str],
    *,
    input_text: Optional[str] = None,
    ok_codes: Tuple[int, ...] = (0,),
    cwd: Optional[Path] = None,
) -> Tuple[int, str, str]:
    p = subprocess.Popen(
        list(cmd),
        stdin=subprocess.PIPE if input_text is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(cwd) if cwd else None,
    )
    if input_text is not None:
        assert p.stdin is not None
        p.stdin.write(input_text)
        p.stdin.close()
    out = p.stdout.read() if p.stdout else ""
    err = p.stderr.read() if p.stderr else ""
    rc = p.wait()
    if rc not in ok_codes:
        raise RuntimeError(f"command failed rc={rc}\ncmd={cmd}\n\nstderr:\n{err}")
    return rc, out, err

def iter_cmd_lines(
    cmd: Sequence[str],
    *,
    input_text: Optional[str] = None,
    ok_codes: Tuple[int, ...] = (0,),
    cwd: Optional[Path] = None,
) -> Iterator[str]:
    p = subprocess.Popen(
        list(cmd),
        stdin=subprocess.PIPE if input_text is not None else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(cwd) if cwd else None,
    )
    if input_text is not None:
        assert p.stdin is not None
        p.stdin.write(input_text)
        p.stdin.close()

    assert p.stdout is not None
    for line in p.stdout:
        yield line.rstrip("\n")

    err = p.stderr.read() if p.stderr else ""
    rc = p.wait()
    if rc not in ok_codes:
        raise RuntimeError(f"command failed rc={rc}\ncmd={cmd}\n\nstderr:\n{err}")

def short_path(p: Path, root: Path) -> str:
    try:
        return str(p.relative_to(root))
    except Exception:
        return str(p)

def json_dump(obj: Any, *, indent: int = 2) -> str:
    return json.dumps(obj, indent=indent, ensure_ascii=False, sort_keys=True)

# =============================================================================
# Toolchain: rg + ast-grep
# =============================================================================

@dataclass(frozen=True)
class Toolchain:
    rg: str
    sg: Optional[str]  # "ast-grep" or "sg"
    py: str

    @staticmethod
    def detect(py: str) -> "Toolchain":
        rg = which("rg") or "rg"
        sg = which("ast-grep") or which("sg")
        return Toolchain(rg=rg, sg=sg, py=py)

    def versions(self) -> Dict[str, str]:
        out: Dict[str, str] = {}
        try:
            _, rg_out, _ = run_cmd([self.rg, "--version"], ok_codes=(0,))
            out["rg"] = rg_out.splitlines()[0].strip()
        except Exception:
            out["rg"] = "unknown"
        if self.sg:
            try:
                _, sg_out, _ = run_cmd([self.sg, "--version"], ok_codes=(0,))
                out["ast_grep"] = sg_out.splitlines()[0].strip()
            except Exception:
                out["ast_grep"] = "unknown"
        else:
            out["ast_grep"] = "not_found"
        out["python"] = self.py
        return out

# =============================================================================
# Report renderer (consistent Markdown sections)
# =============================================================================

def _anchor_md(a: Anchor) -> str:
    loc = f"{a.path}:{a.line}:{a.col}"
    label = f" — {a.label}" if a.label else ""
    snip = f"\n    {a.snippet.strip()}" if a.snippet else ""
    return f"- `{loc}`{label}{snip}"

def render_markdown(res: CqResult, *, max_evidence: int = 80) -> str:
    lines: List[str] = []
    lines.append(f"# cq/{res.run.cmd}")
    lines.append("")
    lines.append("## Summary")
    if res.summary:
        for k, v in res.summary.items():
            lines.append(f"- **{k}**: {v}")
    else:
        lines.append("- (no summary)")
    lines.append("")
    if res.key_findings:
        lines.append("## Key findings")
        for s in res.key_findings:
            lines.append(f"- {s}")
        lines.append("")
    if res.evidence:
        lines.append("## Evidence index")
        for a in res.evidence[:max_evidence]:
            lines.append(_anchor_md(a))
        if len(res.evidence) > max_evidence:
            lines.append(f"- … ({len(res.evidence)-max_evidence} more)")
        lines.append("")
    if res.sections:
        lines.append("## Details")
        lines.append("")
        for sec in res.sections:
            lines.append(f"### {sec.title}")
            if sec.summary:
                lines.append(sec.summary)
            if not sec.findings:
                lines.append("- (none)")
                lines.append("")
                continue
            for f in sec.findings:
                sev = "" if f.severity == "info" else f"**[{f.severity}]** "
                lines.append(f"- {sev}**{f.title}**")
                if f.detail:
                    for ln in f.detail.strip().splitlines():
                        lines.append(f"  - {ln}")
                for a in f.anchors[:20]:
                    lines.append(f"  - `{a.path}:{a.line}:{a.col}` {a.label}".rstrip())
                if len(f.anchors) > 20:
                    lines.append(f"  - … ({len(f.anchors)-20} more anchors)")
            lines.append("")
    if res.notes:
        lines.append("## Notes / uncertainty")
        for n in res.notes:
            lines.append(f"- {n}")
        lines.append("")
    if res.artifacts:
        lines.append("## Artifacts")
        for a in res.artifacts:
            lines.append(f"- **{a.kind}**: `{a.path}` — {a.description}".rstrip())
        lines.append("")
    lines.append("## Run meta")
    lines.append(f"- started_at: {res.run.started_at}")
    lines.append(f"- duration_ms: {res.run.duration_ms}")
    if res.run.tool_versions:
        for k, v in res.run.tool_versions.items():
            lines.append(f"- {k}: {v}")
    return "\n".join(lines) + "\n"

# =============================================================================
# rg: fast narrowing
# =============================================================================

def rg_files_with_matches(
    rg: str,
    pattern: str,
    root: Path,
    *,
    globs: Sequence[str] = ("*.py",),
    max_files: Optional[int] = None,
) -> List[Path]:
    cmd = [rg, "--files-with-matches"]
    for g in globs:
        cmd += ["-g", g]
    cmd += [pattern, str(root)]
    # rg exit codes: 0 match, 1 no match, 2 error
    _, out, _ = run_cmd(cmd, ok_codes=(0, 1))
    files = [Path(p) for p in out.splitlines() if p.strip()]
    return files[:max_files] if max_files is not None else files

# =============================================================================
# ast-grep scan (optional): parse --json=stream
# =============================================================================

@dataclass
class SgMatch:
    file: str
    text: str
    lines: str
    language: str
    rule_id: str = ""
    start_line: int = 0
    start_col: int = 0
    end_line: int = 0
    end_col: int = 0
    meta: Dict[str, Any] = field(default_factory=dict)

def _sg_get(obj: Dict[str, Any], *keys: str, default=None):
    for k in keys:
        if k in obj:
            return obj[k]
    return default

def sg_scan_inline_rules(
    sg: str,
    inline_rules: str,
    paths: Sequence[Path],
    *,
    globs: Sequence[str] = ("*.py",),
    include_metadata: bool = True,
    context: int = 0,
    line_base: str = "auto",  # auto|0|1
) -> List[SgMatch]:
    """
    - ast-grep scan exit codes: 1 if any rule matches; 0 if none.
    - --json=stream prints one JSON object per line.
    """
    cmd = [sg, "scan", "--inline-rules", inline_rules, "--json=stream"]
    if include_metadata:
        cmd.append("--include-metadata")
    for g in globs:
        cmd += ["--globs", g]
    if context:
        cmd += ["-C", str(context)]
    cmd += [str(p) for p in paths]

    raw: List[Dict[str, Any]] = []
    for line in iter_cmd_lines(cmd, ok_codes=(0, 1)):
        if not line:
            continue
        try:
            raw.append(json.loads(line))
        except Exception:
            continue

    # auto-detect 0-based vs 1-based if desired
    add1 = False
    if line_base == "0":
        add1 = True
    elif line_base == "1":
        add1 = False
    else:
        min_line = min((_sg_get(r.get("range", {}).get("start", {}), "line", default=999999) for r in raw), default=999999)
        if min_line == 0:
            add1 = True

    out: List[SgMatch] = []
    for r in raw:
        rng = r.get("range", {}) or {}
        start = rng.get("start", {}) or {}
        end = rng.get("end", {}) or {}
        sl = int(_sg_get(start, "line", default=0))
        sc = int(_sg_get(start, "column", default=0))
        el = int(_sg_get(end, "line", default=0))
        ec = int(_sg_get(end, "column", default=0))
        if add1:
            sl += 1; sc += 1; el += 1; ec += 1

        out.append(SgMatch(
            file=str(_sg_get(r, "file", "path", default="")),
            text=str(_sg_get(r, "text", default="")),
            lines=str(_sg_get(r, "lines", "line", default="")),
            language=str(_sg_get(r, "language", default="")),
            rule_id=str(_sg_get(r, "ruleId", "rule_id", default="")),
            start_line=sl,
            start_col=sc,
            end_line=el,
            end_col=ec,
            meta=r.get("meta") or {},
        ))
    return out

# =============================================================================
# Python AST indexing (defs + calls)
# =============================================================================

@dataclass
class FnDef:
    file: Path
    qual: str
    name: str
    params: List[str]
    is_async: bool
    node: ast.AST

@dataclass
class CallSite:
    file: Path
    fn_qual: str
    lineno: int
    col: int
    callee_text: str
    callee_head: str
    pos_args: int
    kw_names: List[str]
    has_starargs: bool
    has_kwargs: bool

def safe_parse(file: Path) -> Tuple[Optional[ast.Module], str]:
    src = read_text(file)
    try:
        return ast.parse(src, filename=str(file)), src
    except SyntaxError:
        return None, src

def fn_params(fn: ast.FunctionDef | ast.AsyncFunctionDef) -> List[str]:
    args = fn.args
    params = [a.arg for a in getattr(args, "posonlyargs", [])]
    params += [a.arg for a in args.args]
    params += [a.arg for a in args.kwonlyargs]
    if args.vararg:
        params.append("*" + args.vararg.arg)
    if args.kwarg:
        params.append("**" + args.kwarg.arg)
    return params

def callee_head(expr: ast.AST) -> str:
    if isinstance(expr, ast.Name):
        return expr.id
    if isinstance(expr, ast.Attribute):
        return expr.attr
    return "<dynamic>"

def callee_fqn(expr: ast.AST) -> str:
    if isinstance(expr, ast.Name):
        return expr.id
    if isinstance(expr, ast.Attribute):
        return f"{callee_fqn(expr.value)}.{expr.attr}"
    return "<dynamic>"

def unparse(expr: ast.AST) -> str:
    try:
        return ast.unparse(expr)  # py>=3.9
    except Exception:
        return expr.__class__.__name__

def names_in_expr(e: ast.AST) -> Set[str]:
    out: Set[str] = set()
    class V(ast.NodeVisitor):
        def visit_Name(self, n: ast.Name):
            out.add(n.id)
    V().visit(e)
    return out

def index_defs_and_calls(files: Sequence[Path]) -> Tuple[List[FnDef], List[CallSite]]:
    defs: List[FnDef] = []
    calls: List[CallSite] = []

    for f in files:
        mod, _ = safe_parse(f)
        if mod is None:
            continue

        stack: List[str] = []

        class V(ast.NodeVisitor):
            def visit_ClassDef(self, node: ast.ClassDef):
                stack.append(node.name)
                self.generic_visit(node)
                stack.pop()

            def visit_FunctionDef(self, node: ast.FunctionDef):
                qual = ".".join(stack + [node.name]) if stack else node.name
                defs.append(FnDef(f, qual, node.name, fn_params(node), False, node))
                stack.append(node.name)
                self.generic_visit(node)
                stack.pop()

            def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
                qual = ".".join(stack + [node.name]) if stack else node.name
                defs.append(FnDef(f, qual, node.name, fn_params(node), True, node))
                stack.append(node.name)
                self.generic_visit(node)
                stack.pop()

            def visit_Call(self, node: ast.Call):
                fnq = ".".join(stack) if stack else "<module>"
                head = callee_head(node.func)
                kw_names = [k.arg for k in node.keywords if k.arg is not None]
                has_kwargs = any(k.arg is None for k in node.keywords)
                has_starargs = any(isinstance(a, ast.Starred) for a in node.args)
                calls.append(CallSite(
                    file=f,
                    fn_qual=fnq,
                    lineno=getattr(node, "lineno", 0),
                    col=getattr(node, "col_offset", 0),
                    callee_text=unparse(node.func),
                    callee_head=head,
                    pos_args=len(node.args),
                    kw_names=kw_names,
                    has_starargs=has_starargs,
                    has_kwargs=has_kwargs,
                ))
                self.generic_visit(node)

        V().visit(mod)

    return defs, calls

def iter_imports(mod: ast.Module) -> Iterator[Tuple[str, str, int]]:
    for n in mod.body:
        if isinstance(n, ast.Import):
            for a in n.names:
                yield ("import", a.name, getattr(n, "lineno", 0))
        elif isinstance(n, ast.ImportFrom):
            if n.module:
                yield ("from", n.module, getattr(n, "lineno", 0))

# =============================================================================
# symtable + bytecode helpers
# =============================================================================

@dataclass
class SymTableRow:
    name: str
    type: str
    free: List[str]
    cell: List[str]
    globals: List[str]
    nonlocals: List[str]
    locals: List[str]

def symtable_rows(file: Path) -> List[SymTableRow]:
    src = read_text(file)
    st = symtable.symtable(src, str(file), "exec")
    rows: List[SymTableRow] = []

    def walk(t):
        free, cell, glb, nonl, loc = [], [], [], [], []
        for s in t.get_symbols():
            n = s.get_name()
            if s.is_free(): free.append(n)
            if s.is_cell(): cell.append(n)
            if s.is_global(): glb.append(n)
            if s.is_nonlocal(): nonl.append(n)
            if s.is_local(): loc.append(n)
        rows.append(SymTableRow(
            name=t.get_name(),
            type=t.get_type(),
            free=sorted(free),
            cell=sorted(cell),
            globals=sorted(glb),
            nonlocals=sorted(nonl),
            locals=sorted(loc),
        ))
        for c in t.get_children():
            walk(c)

    walk(st)
    return rows

@dataclass
class BytecodeSurface:
    qual: str
    globals: List[str]
    attrs: List[str]
    constants_sample: List[Any]
    opcode_stats: Dict[str, int]

def walk_codeobjs(co, prefix: str = "<module>") -> Iterator[Tuple[str, Any]]:
    yield (prefix, co)
    for c in co.co_consts:
        if hasattr(c, "co_code"):
            name = getattr(c, "co_qualname", None) or c.co_name
            yield from walk_codeobjs(c, f"{prefix}.{name}")

def surface_for_file(file: Path) -> Dict[str, BytecodeSurface]:
    src = read_text(file)
    co = compile(src, str(file), "exec")  # no execution
    out: Dict[str, BytecodeSurface] = {}
    for qual, c in walk_codeobjs(co):
        g, a, consts = set(), set(), set()
        ops: Dict[str, int] = {}
        for ins in dis.get_instructions(c):
            ops[ins.opname] = ops.get(ins.opname, 0) + 1
            if ins.opname in {"LOAD_GLOBAL", "LOAD_NAME"} and isinstance(ins.argval, str):
                g.add(ins.argval)
            if ins.opname in {"LOAD_ATTR", "LOAD_METHOD"} and isinstance(ins.argval, str):
                a.add(ins.argval)
        for k in c.co_consts:
            if isinstance(k, (str, int, float, bytes)):
                consts.add(k)
        out[qual] = BytecodeSurface(
            qual=qual,
            globals=sorted(g),
            attrs=sorted(a),
            constants_sample=list(sorted(consts, key=lambda x: str(x)))[:50],
            opcode_stats=dict(sorted(ops.items(), key=lambda kv: kv[1], reverse=True)[:25]),
        )
    return out

# =============================================================================
# Context + result constructors
# =============================================================================

@dataclass
class Ctx:
    root: Path
    tc: Toolchain
    artifact_dir: Path
    save_artifact: bool
    fmt: str  # md|json|both

def mk_runmeta(cmd: str, argv: List[str], root: Path, started_ms: int, tc: Toolchain) -> RunMeta:
    return RunMeta(
        cmd=cmd,
        argv=argv,
        root=str(root),
        started_at=now_iso(),
        duration_ms=ms() - started_ms,
        tool_versions=tc.versions(),
    )

def mk_result(run: RunMeta) -> CqResult:
    return CqResult(schema_version=SCHEMA_VERSION, run=run)

# =============================================================================
# 9 MACROS
# =============================================================================

# 1) impact: approx taint edges
@dataclass
class DefInfo:
    file: Path
    name: str
    params: List[str]
    node: ast.AST

def _find_defs_top_level(ctx: Ctx, name: str, max_files: int = 3000) -> List[DefInfo]:
    pat = rf"^\s*(async\s+)?def\s+{re.escape(name)}\b"
    files = rg_files_with_matches(ctx.tc.rg, pat, ctx.root, max_files=max_files)
    out: List[DefInfo] = []
    for f in files[:200]:
        mod, _ = safe_parse(f)
        if mod is None:
            continue
        for n in mod.body:
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name:
                out.append(DefInfo(f, n.name, fn_params(n), n))
    return out

def _taint_edges(fn: DefInfo, tainted_param: str) -> List[Tuple[str, str, str, int, int]]:
    tainted: Set[str] = {tainted_param}
    edges: List[Tuple[str, str, str, int, int]] = []

    class V(ast.NodeVisitor):
        def visit_Assign(self, n: ast.Assign):
            rhs = names_in_expr(n.value)
            if rhs & tainted:
                for t in n.targets:
                    tainted.update(names_in_expr(t))
            self.generic_visit(n)

        def visit_Call(self, n: ast.Call):
            for a in n.args:
                if names_in_expr(a) & tainted:
                    edges.append((fn.name, callee_head(n.func), "tainted->pos", getattr(n, "lineno", 0), getattr(n, "col_offset", 0)))
                    break
            for k in n.keywords:
                if k.value and (names_in_expr(k.value) & tainted):
                    edges.append((fn.name, callee_head(n.func), f"tainted->kw:{k.arg or '**'}", getattr(n, "lineno", 0), getattr(n, "col_offset", 0)))
            self.generic_visit(n)

    V().visit(fn.node)
    return edges

def cmd_impact(ctx: Ctx, symbol: str, param: str, depth: int) -> CqResult:
    started = ms()
    roots = _find_defs_top_level(ctx, symbol)

    run = mk_runmeta("impact", ["impact", symbol, "--param", param, "--depth", str(depth)], ctx.root, started, ctx.tc)
    res = mk_result(run)
    res.summary = {"symbol": symbol, "param": param, "root_defs": len(roots), "depth": depth}

    if not roots:
        res.key_findings = ["No top-level defs found. Extend resolver for class methods / nested defs."]
        return res
    if len(roots) > 1:
        res.notes.append("Multiple top-level defs found; results aggregated (possible false positives).")

    impacted: Dict[str, Set[str]] = defaultdict(set)
    q = deque([(symbol, param, 0)])
    impacted[symbol].add(param)

    edges_out: List[Tuple[str, str, str, str, int, int]] = []
    seen = set()

    while q:
        fn_name, tparam, d = q.popleft()
        if d >= depth:
            continue
        key = (fn_name, tparam, d)
        if key in seen:
            continue
        seen.add(key)

        defs = _find_defs_top_level(ctx, fn_name)
        for df in defs[:5]:
            for (src, dst, reason, ln, col) in _taint_edges(df, tparam):
                edges_out.append((src, dst, reason, short_path(df.file, ctx.root), ln, col))
                if dst and dst != "<dynamic>":
                    impacted[dst].add("<unknown>")
                    q.append((dst, "<unknown>", d + 1))

    res.key_findings = [
        "Approx taint graph: if tainted data appears in call args, callee is marked impacted.",
        "Minimal resolver: top-level functions only; extend for methods + qualified resolution.",
    ]

    for (src, dst, reason, path, ln, col) in edges_out[:80]:
        res.evidence.append(Anchor(path=path, line=ln, col=col, label=f"{src} -> {dst} ({reason})"))

    sec1 = Section("Impact edges", "Edges where tainted data appears to flow into call arguments.")
    for (src, dst, reason, path, ln, col) in edges_out[:300]:
        sec1.findings.append(Finding(
            id="edge",
            title=f"{src} -> {dst}",
            detail=f"{reason} at {path}:{ln}:{col}",
            anchors=[Anchor(path=path, line=ln, col=col, label=reason)],
            severity="warn" if dst == "<dynamic>" else "info",
        ))
    res.sections.append(sec1)

    sec2 = Section("Impacted functions (name-level)", "Unique callee names reached by propagation.")
    for fn, tps in sorted(impacted.items()):
        sec2.findings.append(Finding(id="impacted", title=fn, detail=f"tainted_params={sorted(list(tps))[:6]}"))
    res.sections.append(sec2)

    res.notes.append("Extend: map arg position/kw to callee param names; resolve calls across modules/classes.")
    return res

# 2) calls: call-shape histogram
def cmd_calls(ctx: Ctx, symbol: str, top: int, evidence: int) -> CqResult:
    started = ms()
    files = rg_files_with_matches(ctx.tc.rg, rf"\b{re.escape(symbol)}\b", ctx.root, max_files=4000)
    _, calls = index_defs_and_calls(files)

    hits = [c for c in calls if c.callee_head == symbol]
    hist = Counter()
    for c in hits:
        key = f"pos={c.pos_args} kw={tuple(sorted(c.kw_names))} *={c.has_starargs} **={c.has_kwargs}"
        hist[key] += 1

    run = mk_runmeta("calls", ["calls", symbol], ctx.root, started, ctx.tc)
    res = mk_result(run)
    res.summary = {"symbol": symbol, "candidate_files": len(files), "call_sites": len(hits)}
    res.key_findings = [f"Call census: how `{symbol}` is invoked (pos/kw/*args/**kwargs)."]

    for c in hits[:evidence]:
        res.evidence.append(Anchor(
            path=short_path(c.file, ctx.root),
            line=c.lineno,
            col=c.col,
            label=f"in {c.fn_qual} callee={c.callee_text}",
        ))

    sec = Section("Arg-shape histogram", "Counts of distinct call shapes.")
    for k, v in hist.most_common(top):
        sec.findings.append(Finding(id="argshape", title=f"{v} × {k}"))
    res.sections.append(sec)

    if any(c.callee_head == "<dynamic>" for c in calls):
        res.notes.append("Some call sites are dynamic (callee cannot be resolved as Name/Attribute).")

    return res

# 3) sig-impact: signature change viability
def _parse_sig(sig: str) -> ast.arguments:
    m = re.match(r"^\s*[A-Za-z_]\w*\s*\((.*)\)\s*$", sig)
    inside = m.group(1) if m else sig.strip().strip("()")
    tmp = f"def _tmp({inside}):\n  pass\n"
    fn = ast.parse(tmp).body[0]
    assert isinstance(fn, ast.FunctionDef)
    return fn.args

def _allowed_kw(args: ast.arguments) -> set[str]:
    return {a.arg for a in args.args} | {a.arg for a in args.kwonlyargs} | {a.arg for a in getattr(args, "posonlyargs", [])}

def _has_kwargs_param(args: ast.arguments) -> bool:
    return args.kwarg is not None

def _min_pos_required(args: ast.arguments) -> int:
    pos = len(getattr(args, "posonlyargs", [])) + len(args.args)
    defaults = len(args.defaults)
    return max(0, pos - defaults)

def _max_pos(args: ast.arguments) -> int:
    pos = len(getattr(args, "posonlyargs", [])) + len(args.args)
    return 10**9 if args.vararg is not None else pos

def _classify_call(call: CallSite, new_args: ast.arguments) -> Tuple[str, str]:
    pos = call.pos_args
    kw = set(call.kw_names)

    min_pos = _min_pos_required(new_args)
    max_pos = _max_pos(new_args)

    if pos < min_pos:
        return ("break", f"too_few_positional (have {pos}, need >= {min_pos})")
    if pos > max_pos:
        return ("break", f"too_many_positional (have {pos}, max {max_pos})")

    allowed = _allowed_kw(new_args)
    if (kw - allowed) and not _has_kwargs_param(new_args):
        return ("break", f"unknown_kwargs={sorted(kw-allowed)}")

    if call.has_kwargs or call.has_starargs:
        return ("ambiguous", "uses *args/**kwargs forwarding")

    return ("ok", "compatible")

def cmd_sig_impact(ctx: Ctx, symbol: str, to: str, evidence: int) -> CqResult:
    started = ms()
    new_args = _parse_sig(to)

    files = rg_files_with_matches(ctx.tc.rg, rf"\b{re.escape(symbol)}\b", ctx.root, max_files=5000)
    _, calls = index_defs_and_calls(files)
    hits = [c for c in calls if c.callee_head == symbol]

    buckets: Dict[str, List[Tuple[CallSite, str]]] = defaultdict(list)
    for c in hits:
        bucket, reason = _classify_call(c, new_args)
        buckets[bucket].append((c, reason))

    run = mk_runmeta("sig-impact", ["sig-impact", symbol, "--to", to], ctx.root, started, ctx.tc)
    res = mk_result(run)
    res.summary = {
        "symbol": symbol,
        "new_signature": to,
        "call_sites": len(hits),
        "would_break": len(buckets.get("break", [])),
        "ambiguous": len(buckets.get("ambiguous", [])),
    }
    res.key_findings = [
        "Approx compatibility check based on call-shape + parsed signature.",
        "Calls using *args/**kwargs are classified ambiguous (manual review).",
    ]

    prioritized = buckets.get("break", []) + buckets.get("ambiguous", []) + buckets.get("ok", [])
    for (c, reason) in prioritized[:evidence]:
        res.evidence.append(Anchor(
            path=short_path(c.file, ctx.root),
            line=c.lineno,
            col=c.col,
            label=f"{reason} (in {c.fn_qual})",
        ))

    for bucket in ("break", "ambiguous", "ok"):
        sec = Section(f"{bucket} calls", f"Calls classified as {bucket}.")
        for (c, reason) in buckets.get(bucket, [])[:200]:
            sec.findings.append(Finding(
                id=bucket,
                title=f"{short_path(c.file, ctx.root)}:{c.lineno}:{c.col}",
                detail=f"{reason}; in {c.fn_qual}; callee={c.callee_text}",
                anchors=[Anchor(path=short_path(c.file, ctx.root), line=c.lineno, col=c.col, label=reason)],
                severity="error" if bucket == "break" else ("warn" if bucket == "ambiguous" else "info"),
            ))
        res.sections.append(sec)

    res.notes.append("Does not model overloads/typing stubs, dynamic dispatch, or runtime argument munging.")
    return res

# 4) imports: internal edges + SCC cycles
def _py_files(root: Path, max_files: int = 15000) -> List[Path]:
    out: List[Path] = []
    for p in root.rglob("*.py"):
        out.append(p)
        if len(out) >= max_files:
            break
    return out

def _module_name(root: Path, file: Path) -> str:
    rel = file.relative_to(root)
    parts = list(rel.parts)
    if parts[-1] == "__init__.py":
        parts = parts[:-1]
    else:
        parts[-1] = parts[-1].removesuffix(".py")
    return ".".join(parts)

def _tarjan_scc(graph: Dict[str, List[str]]) -> List[List[str]]:
    index = 0
    stack: List[str] = []
    onstack: Set[str] = set()
    idx: Dict[str, int] = {}
    low: Dict[str, int] = {}
    sccs: List[List[str]] = []

    def strongconnect(v: str):
        nonlocal index
        idx[v] = index
        low[v] = index
        index += 1
        stack.append(v)
        onstack.add(v)
        for w in graph.get(v, []):
            if w not in idx:
                strongconnect(w)
                low[v] = min(low[v], low[w])
            elif w in onstack:
                low[v] = min(low[v], idx[w])
        if low[v] == idx[v]:
            comp = []
            while True:
                w = stack.pop()
                onstack.remove(w)
                comp.append(w)
                if w == v:
                    break
            if len(comp) > 1:
                sccs.append(comp)

    for v in graph:
        if v not in idx:
            strongconnect(v)
    return sccs

def cmd_imports(ctx: Ctx, cycles: bool, focus: Optional[str]) -> CqResult:
    started = ms()
    files = _py_files(ctx.root, max_files=15000)
    module_by_file: Dict[Path, str] = {f: _module_name(ctx.root, f) for f in files}
    file_by_module: Dict[str, Path] = {m: f for f, m in module_by_file.items()}
    internal = set(file_by_module.keys())

    edges_internal: List[Tuple[str, str, str, str, int]] = []
    edges_external: List[Tuple[str, str, str, int]] = []

    for f in files:
        mod, _ = safe_parse(f)
        if mod is None:
            continue
        src_mod = module_by_file[f]
        for kind, target, lineno in iter_imports(mod):
            if target in internal:
                edges_internal.append((src_mod, target, kind, short_path(f, ctx.root), lineno))
            else:
                edges_external.append((src_mod, target, kind, lineno))

    g: Dict[str, List[str]] = defaultdict(list)
    for s, t, kind, path, lineno in edges_internal:
        g[s].append(t)

    comps = _tarjan_scc(g) if cycles else []

    run = mk_runmeta("imports", ["imports"] + (["--cycles"] if cycles else []) + (["--focus", focus] if focus else []),
                     ctx.root, started, ctx.tc)
    res = mk_result(run)
    res.summary = {
        "files": len(files),
        "internal_edges": len(edges_internal),
        "external_import_refs": len(edges_external),
        "cycles": len(comps) if cycles else "(not computed)",
        "focus": focus or "(none)",
    }
    res.key_findings = [
        "Internal import edges resolved by mapping file paths to module names.",
        "Cycles are SCCs size>1.",
    ]

    for (s, t, kind, path, lineno) in edges_internal[:60]:
        res.evidence.append(Anchor(path=path, line=lineno, col=0, label=f"{s} -> {t} ({kind})"))

    filt = edges_internal
    if focus:
        filt = [e for e in edges_internal if focus in e[0] or focus in e[1]]

    sec1 = Section("Internal import edges", "Resolved internal imports (module -> module).")
    for (s, t, kind, path, lineno) in filt[:400]:
        sec1.findings.append(Finding(
            id="edge",
            title=f"{s} -> {t}",
            detail=f"{kind} at {path}:{lineno}",
            anchors=[Anchor(path=path, line=lineno, col=0, label=f"{kind} {t}")],
        ))
    res.sections.append(sec1)

    if cycles:
        sec2 = Section("Cycles", "Strongly connected components (SCCs) size>1.")
        for comp in comps[:100]:
            sec2.findings.append(Finding(
                id="cycle",
                title=" ↔ ".join(comp[:6]) + (" …" if len(comp) > 6 else ""),
                detail=f"size={len(comp)}",
                severity="warn",
                meta={"members": comp},
            ))
        res.sections.append(sec2)

    res.notes.append("Import resolution is best-effort; package roots and editable installs can change module identity.")
    return res

# 5) side-effects: import-time calls + global writes + ambient reads
AMBIENT_PATTERNS = {
    "os.environ": "environment",
    "sys.argv": "argv",
    "pathlib.Path.cwd": "cwd",
    "Path.cwd": "cwd",
}
COMMON_SIDE_EFFECT_CALLS = {
    "open": "file_io",
    "requests.get": "network",
    "requests.post": "network",
    "subprocess.run": "subprocess",
    "subprocess.Popen": "subprocess",
    "time.sleep": "sleep",
}

def cmd_side_effects(ctx: Ctx, max_files: int) -> CqResult:
    started = ms()
    files = _py_files(ctx.root, max_files=max_files)

    top_calls: List[Tuple[Path, int, str, str]] = []
    global_writes: List[Tuple[Path, int, str]] = []
    ambient_reads: List[Tuple[Path, int, str, str]] = []

    for f in files:
        mod, _ = safe_parse(f)
        if mod is None:
            continue

        # module top-level only
        for n in mod.body:
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Import, ast.ImportFrom)):
                continue
            if isinstance(n, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
                global_writes.append((f, getattr(n, "lineno", 0), n.__class__.__name__))
            if isinstance(n, ast.Expr) and isinstance(n.value, ast.Call):
                fqn = callee_fqn(n.value.func)
                kind = COMMON_SIDE_EFFECT_CALLS.get(fqn, "call")
                top_calls.append((f, getattr(n, "lineno", 0), fqn, kind))

        # ambient reads anywhere (heuristic)
        class V(ast.NodeVisitor):
            def visit_Attribute(self, node: ast.Attribute):
                fqn = callee_fqn(node)
                for k, v in AMBIENT_PATTERNS.items():
                    if fqn.endswith(k):
                        ambient_reads.append((f, getattr(node, "lineno", 0), fqn, v))
                self.generic_visit(node)
        V().visit(mod)

    run = mk_runmeta("side-effects", ["side-effects", "--max-files", str(max_files)], ctx.root, started, ctx.tc)
    res = mk_result(run)
    res.summary = {
        "files": len(files),
        "module_top_calls": len(top_calls),
        "module_global_writes": len(global_writes),
        "ambient_reads": len(ambient_reads),
    }
    res.key_findings = [
        "Import-time side effects = module-top-level executable statements outside defs/classes.",
        "Ambient dependency reads (env/argv/cwd) are flagged heuristically.",
    ]

    for (f, ln, fqn, kind) in top_calls[:60]:
        res.evidence.append(Anchor(path=short_path(f, ctx.root), line=ln, col=0, label=f"top-level call {fqn} ({kind})"))

    sec1 = Section("Top-level calls", "Executable calls at module import time.")
    for (f, ln, fqn, kind) in top_calls[:300]:
        sev = "warn" if kind in {"network", "subprocess"} else "info"
        sec1.findings.append(Finding(
            id="import_time_call",
            title=f"{fqn} ({kind})",
            detail=f"{short_path(f, ctx.root)}:{ln}",
            anchors=[Anchor(path=short_path(f, ctx.root), line=ln, col=0, label=fqn)],
            severity=sev,
        ))
    res.sections.append(sec1)

    sec2 = Section("Global writes", "Assignments at module top-level.")
    for (f, ln, kind) in global_writes[:300]:
        sec2.findings.append(Finding(
            id="global_write",
            title=kind,
            detail=f"{short_path(f, ctx.root)}:{ln}",
            anchors=[Anchor(path=short_path(f, ctx.root), line=ln, col=0, label=kind)],
        ))
    res.sections.append(sec2)

    sec3 = Section("Ambient reads", "Heuristic detection of env/argv/cwd patterns.")
    for (f, ln, fqn, kind) in ambient_reads[:300]:
        sec3.findings.append(Finding(
            id="ambient_read",
            title=f"{fqn} ({kind})",
            detail=f"{short_path(f, ctx.root)}:{ln}",
            anchors=[Anchor(path=short_path(f, ctx.root), line=ln, col=0, label=fqn)],
            severity="warn",
        ))
    res.sections.append(sec3)

    res.notes.append("Heuristic: import-time effects can also occur indirectly via imported modules and metaclass side effects.")
    return res

# 6) scopes: symtable view for file or symbol (best-effort)
def _resolve_targets(ctx: Ctx, target: str, max_files: int = 2000) -> List[Path]:
    p = (ctx.root / target)
    if p.exists() and p.is_file():
        return [p]
    pat = rf"^\s*(async\s+)?def\s+{re.escape(target)}\b|^\s*class\s+{re.escape(target)}\b"
    return rg_files_with_matches(ctx.tc.rg, pat, ctx.root, max_files=max_files)

def cmd_scopes(ctx: Ctx, target: str) -> CqResult:
    started = ms()
    files = _resolve_targets(ctx, target)

    run = mk_runmeta("scopes", ["scopes", target], ctx.root, started, ctx.tc)
    res = mk_result(run)
    res.summary = {"target": target, "files": len(files)}
    res.key_findings = ["Uses Python symtable to report free/cell/global/nonlocal/local names."]

    if not files:
        res.notes.append("No files resolved for target.")
        return res

    sec = Section("Symbol tables", "Per-scope symbol classification.")
    for f in files[:50]:
        rows = symtable_rows(f)
        for r in rows:
            detail = f"type={r.type} free={len(r.free)} cell={len(r.cell)} globals={len(r.globals)} nonlocals={len(r.nonlocals)} locals={len(r.locals)}"
            sec.findings.append(Finding(
                id="symtable",
                title=f"{short_path(f, ctx.root)} :: {r.name}",
                detail=detail,
                anchors=[Anchor(path=short_path(f, ctx.root), line=1, col=0, label=r.name)],
                meta={
                    "free": r.free,
                    "cell": r.cell,
                    "globals": r.globals,
                    "nonlocals": r.nonlocals,
                    "locals": r.locals,
                },
            ))
    res.sections.append(sec)
    res.notes.append("Symtable is static (no execution), but runtime may differ under metaprogramming/dynamic exec.")
    return res

# 7) exceptions: raise/catch matrix + propagation candidates (heuristic)
def _exc_name(e: ast.AST) -> str:
    if isinstance(e, ast.Name):
        return e.id
    if isinstance(e, ast.Attribute):
        return f"{unparse(e.value)}.{e.attr}"
    return "<dynamic>"

def _exceptions_in_node(node: ast.AST) -> Tuple[Set[str], Set[str], int]:
    raises: Set[str] = set()
    catches: Set[str] = set()
    bare_except = 0

    class V(ast.NodeVisitor):
        def visit_Raise(self, n: ast.Raise):
            if n.exc:
                raises.add(_exc_name(n.exc))
        def visit_ExceptHandler(self, n: ast.ExceptHandler):
            nonlocal bare_except
            if n.type:
                catches.add(_exc_name(n.type))
            else:
                catches.add("<bare>")
                bare_except += 1
            self.generic_visit(n)

    V().visit(node)
    return raises, catches, bare_except

def cmd_exceptions(ctx: Ctx, focus: Optional[str], depth: int) -> CqResult:
    started = ms()
    pat = r"raise\s|except\s"
    if focus:
        pat += f"|\\b{re.escape(focus)}\\b"
    files = rg_files_with_matches(ctx.tc.rg, pat, ctx.root, max_files=8000)

    defs, calls = index_defs_and_calls(files)

    callees_of: Dict[str, Set[str]] = defaultdict(set)
    for c in calls:
        if c.callee_head != "<dynamic>":
            callees_of[c.fn_qual].add(c.callee_head)

    raises_by_fn: Dict[str, Set[str]] = defaultdict(set)
    catches_by_fn: Dict[str, Set[str]] = defaultdict(set)
    bare_by_fn: Dict[str, int] = defaultdict(int)

    for d in defs:
        r, c, b = _exceptions_in_node(d.node)
        raises_by_fn[d.qual] |= r
        catches_by_fn[d.qual] |= c
        bare_by_fn[d.qual] += b

    run = mk_runmeta("exceptions",
                     ["exceptions"] + (["--focus", focus] if focus else []) + ["--depth", str(depth)],
                     ctx.root, started, ctx.tc)
    res = mk_result(run)
    res.summary = {"files": len(files), "functions": len(defs), "focus": focus or "(none)", "depth": depth}
    res.key_findings = [
        "Raises/catches extracted syntactically (no type inference).",
        "Propagation is approximate; dynamic dispatch and re-raise patterns not fully modeled.",
    ]

    for d in defs:
        if focus and d.name != focus and d.qual != focus:
            continue
        if bare_by_fn.get(d.qual, 0) > 0:
            res.evidence.append(Anchor(
                path=short_path(d.file, ctx.root),
                line=getattr(d.node, "lineno", 1),
                col=0,
                label=f"bare excepts={bare_by_fn[d.qual]}",
            ))

    sec1 = Section("Per-function exception surface", "Raises and catches per function.")
    shown = 0
    for d in defs:
        if focus and d.name != focus and d.qual != focus:
            continue
        r = sorted(list(raises_by_fn.get(d.qual, set())))[:12]
        c = sorted(list(catches_by_fn.get(d.qual, set())))[:12]
        detail = f"raises={r} catches={c} bare_except={bare_by_fn.get(d.qual,0)}"
        sec1.findings.append(Finding(
            id="exc_surface",
            title=d.qual,
            detail=detail,
            anchors=[Anchor(path=short_path(d.file, ctx.root), line=getattr(d.node, "lineno", 1), col=0, label=d.qual)],
            severity="warn" if bare_by_fn.get(d.qual, 0) > 0 else "info",
        ))
        shown += 1
        if shown >= 300:
            break
    res.sections.append(sec1)

    sec2 = Section("Uncaught propagation candidates",
                   "Caller -> callee where callee raises and caller does not catch (heuristic).")
    count = 0
    for caller, callees in callees_of.items():
        caller_catches = catches_by_fn.get(caller, set())
        for callee in callees:
            raised = set()
            for qual, rset in raises_by_fn.items():
                if qual.endswith("." + callee) or qual == callee:
                    raised |= rset
            uncaught = sorted([e for e in raised if e not in caller_catches and "<bare>" not in caller_catches])[:8]
            if uncaught:
                sec2.findings.append(Finding(
                    id="uncaught",
                    title=f"{caller} -> {callee}",
                    detail=f"potential_uncaught={uncaught}",
                    severity="warn",
                ))
                count += 1
                if count >= 200:
                    break
        if count >= 200:
            break
    res.sections.append(sec2)

    res.notes.append("Dynamic exception expressions become <dynamic>; bare except treated as catching everything.")
    return res

# 8) async-hazards: uses ast-grep if available, else AST fallback
DEFAULT_BLOCKING = {"time.sleep", "requests.get", "requests.post", "subprocess.run", "subprocess.Popen"}

def cmd_async_hazards(ctx: Ctx, profiles: str) -> CqResult:
    started = ms()
    blocking = set(DEFAULT_BLOCKING)
    if profiles.strip():
        for p in profiles.split(","):
            blocking.add(p.strip())

    # Prefer ast-grep if available (structural + fast)
    hits: List[Tuple[str, int, int, str]] = []  # path, line, col, call_fqn
    used_sg = False

    if ctx.tc.sg:
        used_sg = True
        # Build one rule per blocking call; each rule matches the call inside an async def via `inside`.
        rule_docs = []
        for i, fqn in enumerate(sorted(blocking)):
            # Split into module + name patterns
            # Example patterns: time.sleep($A), requests.get($$$A), subprocess.run($$$A)
            # Use $$$ARGS for variadic calls.
            pat = f"{fqn}($$$ARGS)"
            rid = f"blocking_{i}"
            rule_docs.append(
f"""id: {rid}
language: python
rule:
  pattern: {pat}
  inside:
    pattern: async def $F($$$ARGS): $$$BODY
    stopBy: end
"""
            )
        inline_rules = "---\n".join(rule_docs)

        matches = sg_scan_inline_rules(ctx.tc.sg, inline_rules, [ctx.root], globs=("*.py",), include_metadata=False)
        for m in matches[:5000]:
            p = m.file or ""
            # if sg doesn't provide file for some reason, skip
            if not p:
                continue
            # m.text is the matched node text; we still store the intended fqn as meta label (best-effort)
            hits.append((p, m.start_line or 0, m.start_col or 0, m.text or "<call>"))

    # Fallback: parse python AST (slower on very large repos)
    if not hits and not used_sg:
        files = _py_files(ctx.root, max_files=10000)
        for f in files:
            mod, _ = safe_parse(f)
            if mod is None:
                continue

            class V(ast.NodeVisitor):
                def __init__(self):
                    self.fn_stack: List[str] = []
                    self.in_async = False

                def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
                    prev = self.in_async
                    self.in_async = True
                    self.fn_stack.append(node.name)
                    self.generic_visit(node)
                    self.fn_stack.pop()
                    self.in_async = prev

                def visit_FunctionDef(self, node: ast.FunctionDef):
                    return

                def visit_Call(self, node: ast.Call):
                    if self.in_async:
                        fqn = callee_fqn(node.func)
                        if fqn in blocking:
                            hits.append((short_path(f, ctx.root), getattr(node, "lineno", 0), getattr(node, "col_offset", 0), fqn))
                    self.generic_visit(node)

            V().visit(mod)

    run = mk_runmeta("async-hazards",
                     ["async-hazards"] + (["--profiles", profiles] if profiles.strip() else []),
                     ctx.root, started, ctx.tc)
    res = mk_result(run)
    res.summary = {"blocking_hits": len(hits), "used_ast_grep": bool(ctx.tc.sg)}
    res.key_findings = [
        "Flags known blocking calls inside async functions.",
        "If ast-grep is installed, query uses structural matching with `inside async def ...` (fast).",
    ]

    for (p, ln, col, call_txt) in hits[:80]:
        # p may be absolute from sg; try to relativize if possible
        pp = p
        try:
            pp = short_path(Path(p), ctx.root)
        except Exception:
            pass
        res.evidence.append(Anchor(path=pp, line=int(ln), col=int(col), label=f"blocking call: {call_txt}"))

    sec = Section("Blocking calls inside async functions", "Candidates for performance or deadlock hazards.")
    for (p, ln, col, call_txt) in hits[:400]:
        pp = p
        try:
            pp = short_path(Path(p), ctx.root)
        except Exception:
            pass
        sec.findings.append(Finding(
            id="blocking",
            title=str(call_txt),
            detail=f"{pp}:{ln}:{col}",
            anchors=[Anchor(path=pp, line=int(ln), col=int(col), label=str(call_txt))],
            severity="warn",
        ))
    res.sections.append(sec)
    res.notes.append("Extend `profiles` with project-specific blocking primitives (DB clients, SDKs, etc.).")
    return res

# 9) bytecode-surface: globals/attrs/constants/opcodes
def cmd_bytecode_surface(ctx: Ctx, target: str, show: str) -> CqResult:
    started = ms()
    files = _resolve_targets(ctx, target)

    run = mk_runmeta("bytecode-surface", ["bytecode-surface", target, "--show", show], ctx.root, started, ctx.tc)
    res = mk_result(run)
    res.summary = {"target": target, "files": len(files)}

    sec = Section("Bytecode surfaces", "Per code object: globals/attrs/constants/opcodes.")
    for f in files[:40]:
        surfaces = surface_for_file(f)
        items = list(surfaces.items())
        if not (ctx.root / target).exists():
            filt = [(k, v) for (k, v) in items if k.endswith("." + target) or k == target]
            items = filt if filt else items[:10]

        for qual, s in items[:25]:
            detail = f"globals={len(s.globals)} attrs={len(s.attrs)} consts~={len(s.constants_sample)}"
            sec.findings.append(Finding(
                id="bytecode",
                title=f"{short_path(f, ctx.root)} :: {qual}",
                detail=detail,
                anchors=[Anchor(path=short_path(f, ctx.root), line=1, col=0, label=qual)],
                meta={
                    "globals": s.globals[:120],
                    "attrs": s.attrs[:120],
                    "constants_sample": s.constants_sample,
                    "opcode_stats": s.opcode_stats,
                },
            ))
    res.sections.append(sec)

    res.key_findings = [
        "Bytecode surface approximates runtime coupling: loaded globals/attrs/constants and opcode mix.",
        "Useful for hidden dependencies (config keys, attribute names) that text search might miss.",
    ]
    res.notes.append("Compiles source but does not execute it.")
    return res

# =============================================================================
# CLI
# =============================================================================

def save_artifact_json(res: CqResult, artifact_dir: Path) -> str:
    ensure_dir(artifact_dir)
    ts = res.run.started_at.replace(":", "").replace("-", "")
    fn = f"{res.run.cmd}-{ts}-{abs(hash(' '.join(res.run.argv))) % 100000}.json"
    p = artifact_dir / fn
    p.write_text(json_dump(res.to_dict(), indent=2), encoding="utf-8")
    return str(p)

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="cq")
    ap.add_argument("--root", default=".", help="repo root path")
    ap.add_argument("--format", default="md", choices=["md", "json", "both"], help="output format")
    ap.add_argument("--artifact-dir", default=".cq/artifacts", help="where to save JSON artifacts")
    ap.add_argument("--no-save-artifact", action="store_true", help="disable artifact saving")
    sp = ap.add_subparsers(dest="cmd", required=True)

    p = sp.add_parser("impact", help="transitive 'who depends on this input' (approx taint)")
    p.add_argument("symbol")
    p.add_argument("--param", required=True)
    p.add_argument("--depth", type=int, default=4)

    p = sp.add_parser("calls", help="call-site census (arg shapes, kwargs, *args/**kwargs)")
    p.add_argument("symbol")
    p.add_argument("--top", type=int, default=25)
    p.add_argument("--evidence", type=int, default=40)

    p = sp.add_parser("sig-impact", help="signature-change viability (break / ambiguous / ok)")
    p.add_argument("symbol")
    p.add_argument("--to", required=True)
    p.add_argument("--evidence", type=int, default=60)

    p = sp.add_parser("imports", help="import graph + cycles")
    p.add_argument("--cycles", action="store_true")
    p.add_argument("--focus", default=None)

    p = sp.add_parser("side-effects", help="import-time calls + global writes + ambient reads")
    p.add_argument("--max-files", type=int, default=12000)

    p = sp.add_parser("scopes", help="symtable free/cell/global/nonlocal/local report")
    p.add_argument("target")

    p = sp.add_parser("exceptions", help="raise/catch matrix + propagation candidates")
    p.add_argument("--focus", default=None)
    p.add_argument("--depth", type=int, default=3)

    p = sp.add_parser("async-hazards", help="blocking-in-async (heuristic; uses ast-grep if present)")
    p.add_argument("--profiles", default="")

    p = sp.add_parser("bytecode-surface", help="globals/attrs/constants/opcodes per code object")
    p.add_argument("target")
    p.add_argument("--show", default="globals,attrs,constants,calls")

    return ap

def dispatch(ctx: Ctx, ns: argparse.Namespace) -> CqResult:
    if ns.cmd == "impact":
        return cmd_impact(ctx, ns.symbol, ns.param, ns.depth)
    if ns.cmd == "calls":
        return cmd_calls(ctx, ns.symbol, ns.top, ns.evidence)
    if ns.cmd == "sig-impact":
        return cmd_sig_impact(ctx, ns.symbol, ns.to, ns.evidence)
    if ns.cmd == "imports":
        return cmd_imports(ctx, ns.cycles, ns.focus)
    if ns.cmd == "side-effects":
        return cmd_side_effects(ctx, ns.max_files)
    if ns.cmd == "scopes":
        return cmd_scopes(ctx, ns.target)
    if ns.cmd == "exceptions":
        return cmd_exceptions(ctx, ns.focus, ns.depth)
    if ns.cmd == "async-hazards":
        return cmd_async_hazards(ctx, ns.profiles)
    if ns.cmd == "bytecode-surface":
        return cmd_bytecode_surface(ctx, ns.target, ns.show)
    raise SystemExit(f"unknown cmd: {ns.cmd}")

def main(argv: Optional[List[str]] = None) -> int:
    ap = build_parser()
    ns = ap.parse_args(argv)

    root = Path(ns.root).resolve()
    artifact_dir = (Path(ns.artifact_dir) if Path(ns.artifact_dir).is_absolute() else root / ns.artifact_dir).resolve()

    tc = Toolchain.detect(py=str(Path(__file__).resolve()))
    ctx = Ctx(
        root=root,
        tc=tc,
        artifact_dir=artifact_dir,
        save_artifact=not ns.no_save_artifact,
        fmt=ns.format,
    )

    res = dispatch(ctx, ns)

    if ctx.save_artifact:
        p = save_artifact_json(res, ctx.artifact_dir)
        res.artifacts.append(Artifact(kind="json", path=p, description="cq result artifact"))

    if ns.format in ("md", "both"):
        print(render_markdown(res), end="")
    if ns.format == "json":
        print(json_dump(res.to_dict(), indent=2))
    return 0
```

---

## 6) Optional: `.claude/skills/cq/SKILL.md`

This gives Claude Code “one-line” access to your macros while injecting the Markdown directly into its context (no JSON opening).

```markdown
---
name: cq
description: High-signal code queries (impact, calls, signature viability, imports, side effects, scopes, exceptions, async hazards, bytecode surface)
allowed-tools: ["Bash"]
---

# cq skill

Use this skill when you need high-recall, structured repository facts *before* proposing changes.

## Commands

### Impact (approx dependency / taint)
Results: !`./scripts/cq impact "$1" --param "$2" --root .`
Usage: /cq impact <FUNCTION_NAME> <PARAM_NAME>

### Call census
Results: !`./scripts/cq calls "$1" --root .`
Usage: /cq calls <FUNCTION_NAME>

### Signature change viability
Results: !`./scripts/cq sig-impact "$1" --to "$2" --root .`
Usage: /cq sig-impact <FUNCTION_NAME> "<new_sig_inside_parens>"

### Import graph / cycles
Results: !`./scripts/cq imports --cycles --root .`
Usage: /cq imports

### Import-time side effects
Results: !`./scripts/cq side-effects --root .`
Usage: /cq side-effects

### Scopes (symtable)
Results: !`./scripts/cq scopes "$1" --root .`
Usage: /cq scopes <FILE_OR_SYMBOL>

### Exceptions
Results: !`./scripts/cq exceptions --root .`
Usage: /cq exceptions

### Async hazards
Results: !`./scripts/cq async-hazards --root .`
Usage: /cq async-hazards

### Bytecode surface
Results: !`./scripts/cq bytecode-surface "$1" --root .`
Usage: /cq bytecode-surface <FILE_OR_SYMBOL>
```

---

### What you get immediately

* A consistent report format Claude can read **directly in stdout**
* A stable JSON artifact for “downstream automation” (and for Claude if you later build an MCP tool wrapper)
* A clean place to “harden” each macro into production-grade graph resolution over time

If you want the next increment, I’d do **two surgical upgrades** that massively increase correctness without exploding surface area:

1. **Definition resolver** (module + class method awareness) so `impact/exceptions` map `callee_head` → concrete defs reliably.
2. **Arg→param mapping** so `impact` propagates taint into the *correct downstream parameter names* (positional vs keyword, defaults, varargs).

Below are the **two surgical upgrades** implemented as **drop-in code** that keeps your **CLI surface unchanged** while making `impact` + `exceptions` far more correct:

1. **Definition resolver (module + class method awareness)**
2. **Arg→param binding (positional/keyword/*args/**kwargs) so taint propagates to the *right* callee parameter names**

I’m going to give you:

* **(A)** a new helper block to paste into `tools/cq/cli.py` (near the “Python AST indexing” area is a good spot), and
* **(B)** **replacement implementations** for `cmd_impact` and `cmd_exceptions` that use these helpers.

No new subcommands, no new flags — just much better fidelity.

---

# A) Add this block to `tools/cq/cli.py`

Paste this block **once** (suggested placement: after your existing `names_in_expr()` / `callee_fqn()` helpers, before the macro implementations).

```python
# =============================================================================
# Surgical upgrade helpers:
#  (1) Definition index + resolver (module + class methods + imports)
#  (2) Arg -> param binder for taint propagation
# =============================================================================

@dataclass(frozen=True)
class ImportAlias:
    kind: str  # "module" | "symbol"
    module: str
    name: Optional[str] = None  # for kind="symbol" -> imported symbol name

@dataclass
class ModuleInfo:
    module: str
    file: Path
    is_package: bool
    module_aliases: Dict[str, str] = field(default_factory=dict)       # alias -> module path
    symbol_aliases: Dict[str, ImportAlias] = field(default_factory=dict)  # alias -> ImportAlias(kind="symbol", module=..., name=...)

@dataclass
class FnDecl:
    key: str               # "pkg.mod:qual" where qual is "foo" or "Class.method"
    module: str            # "pkg.mod"
    qual: str              # "foo" or "Class.method"
    name: str              # "foo" or "method"
    class_name: Optional[str]
    file: Path
    lineno: int
    is_async: bool
    args: ast.arguments
    node: ast.AST          # FunctionDef/AsyncFunctionDef

    def all_param_names(self) -> List[str]:
        posonly = [a.arg for a in getattr(self.args, "posonlyargs", [])]
        pos = [a.arg for a in self.args.args]
        kwonly = [a.arg for a in self.args.kwonlyargs]
        out = posonly + pos + kwonly
        if self.args.vararg:
            out.append("*" + self.args.vararg.arg)
        if self.args.kwarg:
            out.append("**" + self.args.kwarg.arg)
        return out

    def positional_param_names(self) -> List[str]:
        posonly = [a.arg for a in getattr(self.args, "posonlyargs", [])]
        pos = [a.arg for a in self.args.args]
        return posonly + pos

    def kwonly_param_names(self) -> List[str]:
        return [a.arg for a in self.args.kwonlyargs]

    def vararg_name(self) -> Optional[str]:
        return self.args.vararg.arg if self.args.vararg else None

    def varkw_name(self) -> Optional[str]:
        return self.args.kwarg.arg if self.args.kwarg else None

@dataclass
class ClassDecl:
    key: str          # "pkg.mod:Class"
    module: str
    name: str
    file: Path
    lineno: int
    bases: List[str]
    node: ast.ClassDef

@dataclass
class DefIndex:
    root: Path
    modules: Dict[str, ModuleInfo] = field(default_factory=dict)
    functions: Dict[str, FnDecl] = field(default_factory=dict)  # key -> decl
    classes: Dict[str, ClassDecl] = field(default_factory=dict)  # key -> decl
    functions_by_name: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))
    classes_by_name: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))

    @staticmethod
    def module_name_from_file(root: Path, file: Path) -> Tuple[str, bool]:
        rel = file.relative_to(root)
        parts = list(rel.parts)
        is_pkg = (parts[-1] == "__init__.py")
        if is_pkg:
            parts = parts[:-1]
        else:
            parts[-1] = parts[-1].removesuffix(".py")
        mod = ".".join(parts) if parts else ""
        return mod, is_pkg

    @staticmethod
    def _resolve_relative_module(current_module: str, is_package: bool, level: int, module: Optional[str]) -> Optional[str]:
        # level=1 => current package; level=2 => parent; etc.
        if level <= 0:
            return module
        parts = current_module.split(".") if current_module else []
        pkg_parts = parts if is_package else parts[:-1]
        up = max(0, len(pkg_parts) - (level - 1))
        base = pkg_parts[:up]
        if module:
            base = base + module.split(".")
        full = ".".join(base)
        return full if full else None

    @staticmethod
    def _collect_import_aliases(mod: ast.Module, *, current_module: str, is_package: bool) -> Tuple[Dict[str, str], Dict[str, ImportAlias]]:
        module_aliases: Dict[str, str] = {}
        symbol_aliases: Dict[str, ImportAlias] = {}

        for n in mod.body:
            if isinstance(n, ast.Import):
                for a in n.names:
                    if a.asname:
                        module_aliases[a.asname] = a.name
                    else:
                        # "import a.b" binds name "a"
                        root_name = a.name.split(".")[0]
                        module_aliases[root_name] = root_name

            elif isinstance(n, ast.ImportFrom):
                base = n.module
                if n.level and n.level > 0:
                    base = DefIndex._resolve_relative_module(current_module, is_package, n.level, n.module)
                if not base:
                    continue
                for a in n.names:
                    alias = a.asname or a.name
                    # treat as symbol alias: alias() likely means base:a.name
                    symbol_aliases[alias] = ImportAlias(kind="symbol", module=base, name=a.name)
                    # ALSO treat as module alias: alias.attr likely means base.a.name:attr (common when importing submodules)
                    module_aliases.setdefault(alias, f"{base}.{a.name}")

        return module_aliases, symbol_aliases

    @classmethod
    def build(cls, root: Path, *, max_files: int = 15000) -> "DefIndex":
        idx = cls(root=root)
        files: List[Path] = []
        for p in root.rglob("*.py"):
            files.append(p)
            if len(files) >= max_files:
                break

        for f in files:
            mod_ast, _ = safe_parse(f)
            if mod_ast is None:
                continue

            module_name, is_pkg = cls.module_name_from_file(root, f)
            minfo = ModuleInfo(module=module_name, file=f, is_package=is_pkg)

            # imports alias maps
            ma, sa = cls._collect_import_aliases(mod_ast, current_module=module_name, is_package=is_pkg)
            minfo.module_aliases = ma
            minfo.symbol_aliases = sa
            idx.modules[module_name] = minfo

            # collect top-level defs
            for n in mod_ast.body:
                if isinstance(n, ast.ClassDef):
                    ckey = f"{module_name}:{n.name}"
                    idx.classes[ckey] = ClassDecl(
                        key=ckey,
                        module=module_name,
                        name=n.name,
                        file=f,
                        lineno=getattr(n, "lineno", 1),
                        bases=[unparse(b) for b in n.bases],
                        node=n,
                    )
                    idx.classes_by_name[n.name].append(ckey)

                    # methods (direct children only)
                    for b in n.body:
                        if isinstance(b, (ast.FunctionDef, ast.AsyncFunctionDef)):
                            qual = f"{n.name}.{b.name}"
                            fkey = f"{module_name}:{qual}"
                            decl = FnDecl(
                                key=fkey,
                                module=module_name,
                                qual=qual,
                                name=b.name,
                                class_name=n.name,
                                file=f,
                                lineno=getattr(b, "lineno", 1),
                                is_async=isinstance(b, ast.AsyncFunctionDef),
                                args=b.args,
                                node=b,
                            )
                            idx.functions[fkey] = decl
                            idx.functions_by_name[b.name].append(fkey)

                elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    qual = n.name
                    fkey = f"{module_name}:{qual}"
                    decl = FnDecl(
                        key=fkey,
                        module=module_name,
                        qual=qual,
                        name=n.name,
                        class_name=None,
                        file=f,
                        lineno=getattr(n, "lineno", 1),
                        is_async=isinstance(n, ast.AsyncFunctionDef),
                        args=n.args,
                        node=n,
                    )
                    idx.functions[fkey] = decl
                    idx.functions_by_name[n.name].append(fkey)

        return idx

    def find_function_keys(self, symbol: str) -> List[str]:
        # Accept "module:qual" exact form
        if ":" in symbol:
            return [symbol] if symbol in self.functions else []
        # Accept "Class.method" (still ambiguous across modules)
        if "." in symbol:
            # exact qual match in any module
            keys = [k for k, d in self.functions.items() if d.qual == symbol]
            return keys
        # Otherwise simple name
        return list(self.functions_by_name.get(symbol, []))

    def find_class_keys(self, name: str) -> List[str]:
        if ":" in name:
            return [name] if name in self.classes else []
        return list(self.classes_by_name.get(name, []))

# ---------- dotted-name helpers ----------

def dotted_name(expr: ast.AST) -> Optional[List[str]]:
    # Return list of parts for Name/Attribute chain; else None
    parts: List[str] = []
    cur = expr
    while True:
        if isinstance(cur, ast.Name):
            parts.append(cur.id)
            break
        if isinstance(cur, ast.Attribute):
            parts.append(cur.attr)
            cur = cur.value
            continue
        return None
    return list(reversed(parts))

# ---------- call target resolution ----------

def resolve_call_targets(
    index: DefIndex,
    *,
    caller_fn: FnDecl,
    call: ast.Call,
    var_types: Dict[str, str],  # local var -> class key "mod:Class"
) -> List[str]:
    """
    Return a list of candidate callee function keys.
    Resolution heuristics:
      - unqualified name() => local module def or imported symbol alias
      - mod.name() => imported module alias
      - self.name()/cls.name()/super().name() => current class method
      - ClassName.name() => local or imported class method
      - var.name() where var inferred from var = ClassName(...) => that class method
    """
    modinfo = index.modules.get(caller_fn.module)

    def exists(k: str) -> bool:
        return k in index.functions

    out: List[str] = []

    fn = call.func

    # 1) Name(...) call
    if isinstance(fn, ast.Name):
        nm = fn.id
        # imported symbol alias
        if modinfo and nm in modinfo.symbol_aliases:
            a = modinfo.symbol_aliases[nm]
            cand = f"{a.module}:{a.name}"
            if exists(cand):
                out.append(cand)
        # local function
        cand_local = f"{caller_fn.module}:{nm}"
        if exists(cand_local):
            out.append(cand_local)

        # fallback: any function with this simple name
        if not out:
            for k in index.functions_by_name.get(nm, [])[:30]:
                out.append(k)

        return list(dict.fromkeys(out))

    # 2) Attribute(...) call
    if isinstance(fn, ast.Attribute):
        attr = fn.attr
        base = fn.value

        # self / cls => method on current class
        if isinstance(base, ast.Name) and base.id in {"self", "cls"} and caller_fn.class_name:
            cand = f"{caller_fn.module}:{caller_fn.class_name}.{attr}"
            if exists(cand):
                out.append(cand)
            return list(dict.fromkeys(out))

        # super().method()
        if isinstance(base, ast.Call) and isinstance(base.func, ast.Name) and base.func.id == "super" and caller_fn.class_name:
            cand = f"{caller_fn.module}:{caller_fn.class_name}.{attr}"
            if exists(cand):
                out.append(cand)
            return list(dict.fromkeys(out))

        # dotted module alias: pkg.mod.fn()
        parts = dotted_name(base)
        if parts:
            # if first part is imported module alias, expand it
            if modinfo and parts[0] in modinfo.module_aliases:
                root_mod = modinfo.module_aliases[parts[0]]
                full_mod = ".".join([root_mod] + parts[1:])
                cand = f"{full_mod}:{attr}"
                if exists(cand):
                    out.append(cand)
                    return list(dict.fromkeys(out))
            # if parts themselves refer to module path, try directly
            cand = f"{'.'.join(parts)}:{attr}"
            if exists(cand):
                out.append(cand)
                return list(dict.fromkeys(out))

        # module alias: m.fn()
        if isinstance(base, ast.Name) and modinfo and base.id in modinfo.module_aliases:
            target_mod = modinfo.module_aliases[base.id]
            cand = f"{target_mod}:{attr}"
            if exists(cand):
                out.append(cand)
                return list(dict.fromkeys(out))

        # ClassName.method() where ClassName is local class or imported class symbol alias
        if isinstance(base, ast.Name):
            class_name = base.id

            # local class in same module
            ckey = f"{caller_fn.module}:{class_name}"
            if ckey in index.classes:
                cand = f"{caller_fn.module}:{class_name}.{attr}"
                if exists(cand):
                    out.append(cand)
                    return list(dict.fromkeys(out))

            # imported symbol alias might be a class
            if modinfo and class_name in modinfo.symbol_aliases:
                a = modinfo.symbol_aliases[class_name]
                # if the imported symbol is a class, method key is module:Class.method
                class_key = f"{a.module}:{a.name}"
                if class_key in index.classes:
                    cand = f"{a.module}:{a.name}.{attr}"
                    if exists(cand):
                        out.append(cand)
                        return list(dict.fromkeys(out))

            # module alias via "from x import y" treated as module alias too
            if modinfo and class_name in modinfo.module_aliases:
                target_mod = modinfo.module_aliases[class_name]
                cand = f"{target_mod}:{attr}"
                if exists(cand):
                    out.append(cand)
                    return list(dict.fromkeys(out))

            # local var inferred type: v.method()
            if class_name in var_types:
                class_key = var_types[class_name]  # "mod:Class"
                if ":" in class_key:
                    m, clsname = class_key.split(":", 1)
                    cand = f"{m}:{clsname}.{attr}"
                    if exists(cand):
                        out.append(cand)
                        return list(dict.fromkeys(out))

        return list(dict.fromkeys(out))

    return []

# ---------- constructor/type inference for var.method resolution ----------

def resolve_constructor_class_key(
    index: DefIndex,
    *,
    caller_fn: FnDecl,
    ctor_call: ast.Call,
) -> Optional[str]:
    """
    Given RHS like Foo(...) or mod.Foo(...), try to resolve Foo as a class key "mod:Foo".
    """
    modinfo = index.modules.get(caller_fn.module)
    fn = ctor_call.func

    # Foo(...)
    if isinstance(fn, ast.Name):
        nm = fn.id
        # local class
        local_ckey = f"{caller_fn.module}:{nm}"
        if local_ckey in index.classes:
            return local_ckey
        # imported symbol alias
        if modinfo and nm in modinfo.symbol_aliases:
            a = modinfo.symbol_aliases[nm]
            ckey = f"{a.module}:{a.name}"
            if ckey in index.classes:
                return ckey
        # fallback: any class with that name
        cands = index.classes_by_name.get(nm, [])
        return cands[0] if cands else None

    # mod.Foo(...)
    if isinstance(fn, ast.Attribute):
        attr = fn.attr
        base = fn.value
        parts = dotted_name(base)
        if parts:
            if modinfo and parts[0] in modinfo.module_aliases:
                root_mod = modinfo.module_aliases[parts[0]]
                full_mod = ".".join([root_mod] + parts[1:])
                ckey = f"{full_mod}:{attr}"
                if ckey in index.classes:
                    return ckey
            # direct dotted module
            ckey = f"{'.'.join(parts)}:{attr}"
            if ckey in index.classes:
                return ckey

        if isinstance(base, ast.Name) and modinfo and base.id in modinfo.module_aliases:
            target_mod = modinfo.module_aliases[base.id]
            ckey = f"{target_mod}:{attr}"
            if ckey in index.classes:
                return ckey

    return None

# ---------- arg -> param binding ----------

@dataclass
class BoundCall:
    by_param: Dict[str, ast.AST] = field(default_factory=dict)   # explicit bindings
    varargs_exprs: List[ast.AST] = field(default_factory=list)   # expressions from *args or extra positionals
    varkw_exprs: List[ast.AST] = field(default_factory=list)     # expressions from **kwargs
    hazards: List[str] = field(default_factory=list)

def bind_call_to_params(call: ast.Call, callee: FnDecl) -> BoundCall:
    """
    Best-effort Python call binding:
      - maps explicit positionals and explicit keywords
      - *args and **kwargs are recorded as hazards with expr buckets
    """
    b = BoundCall()
    pos_params = callee.positional_param_names()
    kwonly = set(callee.kwonly_param_names())
    vararg = callee.vararg_name()
    varkw = callee.varkw_name()

    # positional binding (including Starred)
    pi = 0
    for a in call.args:
        if isinstance(a, ast.Starred):
            b.varargs_exprs.append(a.value)
            b.hazards.append("uses *args (Starred) — positional-to-param mapping may be incomplete")
            continue
        if pi < len(pos_params):
            pname = pos_params[pi]
            # duplicate binding hazard
            if pname in b.by_param:
                b.hazards.append(f"duplicate binding for param {pname}")
            b.by_param[pname] = a
            pi += 1
        else:
            # extra positional => goes to *vararg if exists, else hazard
            if vararg:
                b.varargs_exprs.append(a)
            else:
                b.varargs_exprs.append(a)
                b.hazards.append("extra positional args but callee has no *args")

    # keyword binding
    for k in call.keywords:
        if k.arg is None:
            # **kwargs expansion
            b.varkw_exprs.append(k.value)
            b.hazards.append("uses **kwargs expansion — keyword-to-param mapping may be incomplete")
            continue
        pname = k.arg
        # If pname is positional param already bound by position, still ok but duplicates can occur
        if pname in b.by_param:
            b.hazards.append(f"duplicate binding for param {pname}")
        b.by_param[pname] = k.value

        # unknown keyword without **kwargs
        if pname not in pos_params and pname not in kwonly and not varkw:
            b.hazards.append(f"unknown keyword {pname} and callee has no **kwargs")

    return b

def tainted_params_from_bound_call(bound: BoundCall, callee: FnDecl, tainted_vars: Set[str]) -> Tuple[Set[str], List[str]]:
    """
    Convert BoundCall into a set of callee parameter names that are tainted.
    """
    out: Set[str] = set()
    hazards: List[str] = list(bound.hazards)

    # explicit param bindings
    for pname, expr in bound.by_param.items():
        if names_in_expr(expr) & tainted_vars:
            out.add(pname)

    # *args / extra positionals: map to *vararg name if present else "<unknown_positional>"
    if bound.varargs_exprs:
        for expr in bound.varargs_exprs:
            if names_in_expr(expr) & tainted_vars:
                if callee.vararg_name():
                    out.add(callee.vararg_name())
                else:
                    out.add("<unknown_positional>")
                hazards.append("taint flows through *args/extra positional — param mapping is conservative")

    # **kwargs: map to **kwarg name if present else "<unknown_keyword>"
    if bound.varkw_exprs:
        for expr in bound.varkw_exprs:
            if names_in_expr(expr) & tainted_vars:
                if callee.varkw_name():
                    out.add(callee.varkw_name())
                else:
                    out.add("<unknown_keyword>")
                hazards.append("taint flows through **kwargs — param mapping is conservative")

    return out, hazards

def expand_unknown_taint(callee: FnDecl, tainted_param: str) -> Set[str]:
    """
    If upstream taint couldn't determine the exact param, default to worst-case:
    taint ALL callee params (high recall, bounded explosion).
    """
    if tainted_param in {"<unknown>", "<unknown_positional>", "<unknown_keyword>"}:
        # include all named params plus vararg/varkw if present
        names = set([a.arg for a in getattr(callee.args, "posonlyargs", [])])
        names |= set([a.arg for a in callee.args.args])
        names |= set([a.arg for a in callee.args.kwonlyargs])
        if callee.args.vararg:
            names.add(callee.args.vararg.arg)
        if callee.args.kwarg:
            names.add(callee.args.kwarg.arg)
        return names
    return {tainted_param}
```

---

# B) Replace `cmd_impact` with this upgraded version

This version:

* Builds a **DefIndex** for the repo
* Resolves calls to **concrete callee defs** (`module:qual`, including `Class.method`)
* Uses **arg→param binding** to propagate taint to the *right* downstream parameter names
* Conservatively handles `*args/**kwargs` by emitting `<unknown_…>` and expanding taint as “worst case” (high recall, controlled)

```python
@dataclass
class ImpactEdge:
    src_key: str
    dst_key: str
    dst_params: List[str]
    reason: str
    path: str
    line: int
    col: int
    hazards: List[str] = field(default_factory=list)

def cmd_impact(ctx: Ctx, symbol: str, param: str, depth: int) -> CqResult:
    started = ms()
    index = DefIndex.build(ctx.root, max_files=15000)

    root_keys = index.find_function_keys(symbol)
    run = mk_runmeta("impact", ["impact", symbol, "--param", param, "--depth", str(depth)], ctx.root, started, ctx.tc)
    res = mk_result(run)
    res.summary = {"symbol": symbol, "param": param, "depth": depth, "root_defs": len(root_keys)}

    if not root_keys:
        res.key_findings = ["No defs found for symbol. Try module-qualified form like pkg.mod:foo or Class.method."]
        return res

    if len(root_keys) > 1:
        res.notes.append("Multiple defs match symbol; analysis aggregates them (possible over-approx).")
        res.notes.append(f"Roots: {root_keys[:10]}{' …' if len(root_keys)>10 else ''}")

    edges: List[ImpactEdge] = []
    visited: Set[Tuple[str, str, int]] = set()
    q = deque()

    for rk in root_keys:
        q.append((rk, param, 0))

    while q:
        fn_key, tainted_param, d = q.popleft()
        if d >= depth:
            continue
        vkey = (fn_key, tainted_param, d)
        if vkey in visited:
            continue
        visited.add(vkey)

        caller = index.functions.get(fn_key)
        if not caller:
            continue

        # Start taint set from the upstream tainted "parameter"
        tainted_vars: Set[str] = set(expand_unknown_taint(caller, tainted_param))

        # local var -> class key inference for resolving var.method()
        var_types: Dict[str, str] = {}

        class V(ast.NodeVisitor):
            def visit_FunctionDef(self, node: ast.FunctionDef):
                # do not descend into nested defs
                return
            def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
                return
            def visit_ClassDef(self, node: ast.ClassDef):
                return

            def visit_Assign(self, node: ast.Assign):
                # taint propagation through assignments
                rhs_names = names_in_expr(node.value)
                if rhs_names & tainted_vars:
                    for t in node.targets:
                        tainted_vars.update(names_in_expr(t))

                # type inference: x = ClassName(...)
                if isinstance(node.value, ast.Call):
                    ckey = resolve_constructor_class_key(index, caller_fn=caller, ctor_call=node.value)
                    if ckey:
                        for t in node.targets:
                            if isinstance(t, ast.Name):
                                var_types[t.id] = ckey
                # continue traversal
                self.generic_visit(node)

            def visit_AnnAssign(self, node: ast.AnnAssign):
                if node.value is not None:
                    rhs_names = names_in_expr(node.value)
                    if rhs_names & tainted_vars:
                        tainted_vars.update(names_in_expr(node.target))
                    if isinstance(node.value, ast.Call):
                        ckey = resolve_constructor_class_key(index, caller_fn=caller, ctor_call=node.value)
                        if ckey and isinstance(node.target, ast.Name):
                            var_types[node.target.id] = ckey
                self.generic_visit(node)

            def visit_Call(self, node: ast.Call):
                # if tainted vars appear in any argument, this call is "dependent"
                arg_is_tainted = False
                for a in node.args:
                    if isinstance(a, ast.Starred):
                        if names_in_expr(a.value) & tainted_vars:
                            arg_is_tainted = True
                    else:
                        if names_in_expr(a) & tainted_vars:
                            arg_is_tainted = True
                for k in node.keywords:
                    if k.value and (names_in_expr(k.value) & tainted_vars):
                        arg_is_tainted = True

                if arg_is_tainted:
                    callee_keys = resolve_call_targets(index, caller_fn=caller, call=node, var_types=var_types)
                    if not callee_keys:
                        # unresolved dynamic call; record as hazard but can't propagate
                        edges.append(ImpactEdge(
                            src_key=caller.key,
                            dst_key="<unresolved>",
                            dst_params=[],
                            reason="tainted arg flows into dynamic/unresolved call",
                            path=short_path(caller.file, ctx.root),
                            line=getattr(node, "lineno", 0),
                            col=getattr(node, "col_offset", 0),
                            hazards=["callee unresolved"],
                        ))
                    else:
                        for ck in callee_keys[:8]:  # small cap to avoid explosion on ambiguous resolution
                            callee = index.functions.get(ck)
                            if not callee:
                                continue
                            bound = bind_call_to_params(node, callee)
                            tparams, hazards = tainted_params_from_bound_call(bound, callee, tainted_vars)

                            # if no explicit param mapped but call was tainted, propagate conservatively
                            if not tparams:
                                tparams = {"<unknown>"}
                                hazards = hazards + ["call tainted but no param binding recognized; propagating as <unknown>"]

                            edges.append(ImpactEdge(
                                src_key=caller.key,
                                dst_key=callee.key,
                                dst_params=sorted(list(tparams))[:20],
                                reason="tainted arg -> bound callee params",
                                path=short_path(caller.file, ctx.root),
                                line=getattr(node, "lineno", 0),
                                col=getattr(node, "col_offset", 0),
                                hazards=hazards[:10],
                            ))

                            # enqueue propagation
                            for tp in tparams:
                                q.append((callee.key, tp, d + 1))

                self.generic_visit(node)

        V().visit(caller.node)

    res.key_findings = [
        "Uses repo-wide definition index to resolve calls to concrete defs (module + class methods).",
        "Propagates taint across calls via arg→param binding (positional/keyword), conservative on *args/**kwargs.",
    ]
    res.summary["edges"] = len(edges)

    # Evidence: top edges
    for e in edges[:80]:
        res.evidence.append(Anchor(
            path=e.path,
            line=e.line,
            col=e.col,
            label=f"{e.src_key} -> {e.dst_key} taints={e.dst_params[:6]}",
        ))

    sec = Section("Impact edges", "Edges where upstream taint flows into a specific callee parameter set.")
    for e in edges[:350]:
        sev = "warn" if (e.dst_key == "<unresolved>" or e.hazards) else "info"
        detail = f"{e.reason}; taints={e.dst_params}"
        if e.hazards:
            detail += f"; hazards={e.hazards}"
        sec.findings.append(Finding(
            id="edge",
            title=f"{e.src_key} -> {e.dst_key}",
            detail=detail,
            anchors=[Anchor(path=e.path, line=e.line, col=e.col, label="callsite")],
            severity=sev,
            meta={"dst_params": e.dst_params, "hazards": e.hazards},
        ))
    res.sections.append(sec)

    res.notes.append("Resolution is heuristic: dynamic dispatch/getattr/protocol-based calls may remain unresolved.")
    res.notes.append("Unknown taint (<unknown_*>) expands conservatively to all params in the callee (high recall).")
    return res
```

---

# C) Replace `cmd_exceptions` with this upgraded version

This version:

* Uses the **definition resolver** (no more “callee name endswith …” guessing)
* Builds real `caller_key -> callee_key` edges via AST traversal + local var type inference
* Computes “uncaught candidates” using resolved edges

```python
def cmd_exceptions(ctx: Ctx, focus: Optional[str], depth: int) -> CqResult:
    started = ms()
    index = DefIndex.build(ctx.root, max_files=15000)

    # Extract raises/catches per function
    raises_by_fn: Dict[str, Set[str]] = defaultdict(set)
    catches_by_fn: Dict[str, Set[str]] = defaultdict(set)
    bare_by_fn: Dict[str, int] = defaultdict(int)

    for fk, fn in index.functions.items():
        r, c, b = _exceptions_in_node(fn.node)
        raises_by_fn[fk] |= r
        catches_by_fn[fk] |= c
        bare_by_fn[fk] += b

    def exc_simple(x: str) -> str:
        return x.split(".")[-1]

    def catches_all(catches: Set[str]) -> bool:
        if "<bare>" in catches:
            return True
        simp = {exc_simple(x) for x in catches}
        return ("Exception" in simp) or ("BaseException" in simp)

    # Build resolved call edges per function (direct calls inside body; skip nested defs/classes)
    call_edges: List[Tuple[str, str, Anchor]] = []

    for caller_key, caller in index.functions.items():
        var_types: Dict[str, str] = {}

        class V(ast.NodeVisitor):
            def visit_FunctionDef(self, node: ast.FunctionDef): return
            def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef): return
            def visit_ClassDef(self, node: ast.ClassDef): return

            def visit_Assign(self, node: ast.Assign):
                if isinstance(node.value, ast.Call):
                    ckey = resolve_constructor_class_key(index, caller_fn=caller, ctor_call=node.value)
                    if ckey:
                        for t in node.targets:
                            if isinstance(t, ast.Name):
                                var_types[t.id] = ckey
                self.generic_visit(node)

            def visit_AnnAssign(self, node: ast.AnnAssign):
                if node.value is not None and isinstance(node.value, ast.Call) and isinstance(node.target, ast.Name):
                    ckey = resolve_constructor_class_key(index, caller_fn=caller, ctor_call=node.value)
                    if ckey:
                        var_types[node.target.id] = ckey
                self.generic_visit(node)

            def visit_Call(self, node: ast.Call):
                callees = resolve_call_targets(index, caller_fn=caller, call=node, var_types=var_types)
                if callees:
                    for ck in callees[:8]:
                        a = Anchor(
                            path=short_path(caller.file, ctx.root),
                            line=getattr(node, "lineno", 0),
                            col=getattr(node, "col_offset", 0),
                            label=f"{caller_key} -> {ck}",
                        )
                        call_edges.append((caller_key, ck, a))
                self.generic_visit(node)

        V().visit(caller.node)

    # Apply focus (optional)
    def focus_match(fn_key: str) -> bool:
        if not focus:
            return True
        if ":" in focus:
            return fn_key == focus
        # match simple name or suffix qual
        decl = index.functions.get(fn_key)
        if not decl:
            return False
        return decl.name == focus or decl.qual.endswith("." + focus) or fn_key.endswith(":" + focus)

    run = mk_runmeta("exceptions",
                     ["exceptions"] + (["--focus", focus] if focus else []) + ["--depth", str(depth)],
                     ctx.root, started, ctx.tc)
    res = mk_result(run)
    res.summary = {
        "functions_indexed": len(index.functions),
        "call_edges": len(call_edges),
        "focus": focus or "(none)",
        "depth": depth,
    }
    res.key_findings = [
        "Raises/catches extracted syntactically (no type inference).",
        "Propagation candidates use resolved call edges (module + class methods) rather than name matching.",
    ]

    # Evidence: show bare except hotspots for focused functions
    for fk, fn in list(index.functions.items())[:5000]:
        if not focus_match(fk):
            continue
        if bare_by_fn.get(fk, 0) > 0:
            res.evidence.append(Anchor(
                path=short_path(fn.file, ctx.root),
                line=fn.lineno,
                col=0,
                label=f"{fk} bare_except={bare_by_fn[fk]}",
            ))

    sec1 = Section("Per-function exception surface", "Raises and catches per function (syntactic).")
    shown = 0
    for fk, fn in index.functions.items():
        if not focus_match(fk):
            continue
        r = sorted(list(raises_by_fn.get(fk, set())))[:12]
        c = sorted(list(catches_by_fn.get(fk, set())))[:12]
        detail = f"raises={r} catches={c} bare_except={bare_by_fn.get(fk,0)}"
        sec1.findings.append(Finding(
            id="exc_surface",
            title=fk,
            detail=detail,
            anchors=[Anchor(path=short_path(fn.file, ctx.root), line=fn.lineno, col=0, label=fk)],
            severity="warn" if bare_by_fn.get(fk, 0) > 0 else "info",
        ))
        shown += 1
        if shown >= 350:
            break
    res.sections.append(sec1)

    # Uncaught candidates based on resolved edges
    sec2 = Section("Uncaught propagation candidates", "caller -> callee where callee raises and caller does not catch (heuristic).")
    count = 0
    for caller_key, callee_key, a in call_edges:
        if focus and not (focus_match(caller_key) or focus_match(callee_key)):
            continue

        callee_raises = raises_by_fn.get(callee_key, set())
        if not callee_raises:
            continue

        caller_catches = catches_by_fn.get(caller_key, set())
        if catches_all(caller_catches):
            continue

        caller_c_simp = {exc_simple(x) for x in caller_catches}
        uncaught = []
        for e in callee_raises:
            if exc_simple(e) not in caller_c_simp:
                uncaught.append(e)
        uncaught = sorted(uncaught)[:10]
        if uncaught:
            sec2.findings.append(Finding(
                id="uncaught",
                title=f"{caller_key} -> {callee_key}",
                detail=f"potential_uncaught={uncaught}",
                anchors=[a],
                severity="warn",
                meta={"uncaught": uncaught},
            ))
            count += 1
            if count >= 250:
                break
    res.sections.append(sec2)

    res.notes.append("Resolver is heuristic: dynamic dispatch/getattr/protocol-based calls may be missed or ambiguous.")
    res.notes.append("Exception matching uses simple-name equivalence (ValueError vs builtins.ValueError).")
    return res
```

---

# What you get from these two upgrades

### Definition resolver (impact/exceptions)

* `self.foo()` / `cls.foo()` / `super().foo()` → resolves to `module:Class.foo`
* `mod.foo()` where `mod` came from `import pkg.mod as mod` → resolves to `pkg.mod:foo`
* `from pkg.mod import Foo as Bar` + `Bar.baz()` → resolves to `pkg.mod:Foo.baz` (if class exists in repo index)
* `x = Foo(); x.bar()` → resolves to `Foo.bar` via lightweight local constructor inference

### Arg→param mapping (impact)

* `callee(a, b, c=...)` maps taint to *specific* callee params (not just “callee is affected”)
* `*args/**kwargs` are handled conservatively:

  * taint becomes `<unknown_positional>` / `<unknown_keyword>` and then expands to “taint all callee params” (high recall)

This is exactly the “massive correctness jump without surface-area explosion” you were aiming for.

---

If you want one more **very contained** follow-on that compounds these upgrades: add a tiny `--prefer-module pkg.mod` *optional* hint for `impact`/`exceptions` when `symbol` is ambiguous across many modules. It’s a single flag, but it eliminates most ambiguity explosions in large repos.
