
## 1) Parsing at scale: configuration, fidelity, and failure modes

### Mental model (indexer-grade invariants)

* **Only `parse_module()` is “lossless-by-construction” for a full file** (accepts leading/trailing whitespace, preserves encoding when parsing bytes). `parse_expression()` / `parse_statement()` are *convenience constructors* for insertion, but have representational holes (nowhere to store certain trivia). ([LibCST][1])
* **For repo indexing, prefer `bytes -> parse_module(bytes)`** so LibCST can infer/preserve PEP-263 encoding and you can round-trip as bytes without lossy decode/re-encode cycles. ([LibCST][1])
* **Treat parsing as an ingestion phase that emits a “parse manifest row”** per file: `{ok|error, encoding, default_newline, default_indent, future_imports, has_trailing_newline, parser_backend, libcst_version}` (the last two are ops-critical; see §1.5). ([LibCST][1])

---

### 1.1 Parser entrypoints (exact contracts + “don’t get cut” trivia)

#### `parse_module(source: str | bytes, config: PartialParserConfig = ...) -> Module`

* Accepts entire module **including all leading/trailing whitespace**. ([LibCST][1])
* If `source` is **bytes**: encoding is inferred/preserved; **use `Module.bytes`** to serialize. If `source` is **str**: encoding defaults to UTF-8 for byte serialization; **use `Module.code`** for string serialization. ([LibCST][1])

#### `parse_expression(source: str, config: ...) -> BaseExpression`

* Parses a **single-line expression**; **no leading/trailing whitespace/comments** (nowhere to store them). If you need exact trivia, embed it in a module and `parse_module`. ([LibCST][1])

#### `parse_statement(source: str, config: ...) -> SimpleStatementLine | BaseCompoundStatement`

* Parses a statement **followed by a trailing newline**; if missing, one is added. Leading/trailing comments (same line) are accepted, but **whitespace after the trailing newline is invalid** (no storage). Rendering a standalone statement via `Module([]).code_for_node(stmt)` will not include leading/trailing comments/empty lines because those trivia aren’t representable at statement-node level. ([LibCST][1])

**Operational rule:** If your indexing relies on *exact source slicing*, do *not* store `code_for_node(parse_statement(...))` as canonical evidence; store **byte spans from the original file** (metadata providers later) and slice raw bytes/text from disk.

---

### 1.2 `PartialParserConfig` (full surface area + determinism knobs)

`PartialParserConfig` is optional on all parse entrypoints; **unspecified fields are inferred from input source or environment**. ([LibCST][1])

Fields (with indexing relevance):

* `python_version: str | AutoConfig`

  * “Expected syntactic compatibility” target; if unspecified, defaults to the running interpreter (rounded down per LibCST’s supported list as documented). ([LibCST][1])
  * **Reality check:** LibCST project releases (e.g., 1.8.6) describe parsing Python 3.0→3.14 overall, but the ReadTheDocs page describing `python_version` is behind the PyPI claim; treat `python_version` primarily as a *compat hint* and validate by parse success in your own environment. ([LibCST][1])
* `parsed_python_version: PythonVersionInfo` (derived; don’t pass) ([LibCST][1])
* `encoding: str | AutoConfig`

  * bytes input: may be inferred from content; str input defaults `"utf-8"`. ([LibCST][1])
* `future_imports: FrozenSet[str] | AutoConfig` (detected `__future__` imports) ([LibCST][1])
* `default_indent: str | AutoConfig` (inferred tabs/spaces pattern) ([LibCST][1])
* `default_newline: str | AutoConfig` (`\n` / `\r\n` / `\r`, inferred) ([LibCST][1])

**Determinism pattern:**

* For **module parsing**: let LibCST infer `encoding/default_indent/default_newline` from the file once; record them in your parse manifest.
* For **insertion parsing** (`parse_expression/parse_statement`): always pass `config=module.config_for_parsing` to avoid “mixed newline/indent defaults” when creating nodes from templates. ([LibCST][2])

---

### 1.3 Fidelity primitives on `Module` (what to persist vs what to recompute)

Core `Module` properties/methods you treat as serialization APIs:

* `Module.code: str` — respects inferred indent/newline. ([LibCST][2])
* `Module.bytes: bytes` — **`code.encode(module.encoding)`**, respecting inferred indent/newline + current encoding. ([LibCST][2])
* `Module.code_for_node(node) -> str` — renders any `CSTNode` *in the context of this module* (needs module indent/newline defaults). ([LibCST][2])
* `Module.config_for_parsing: PartialParserConfig` — “clone the module’s inferred defaults” for downstream `parse_expression/parse_statement`, ensuring consistent newline/indent inference. ([LibCST][2])

**Indexer stance (token-efficient):**

* Persist: `encoding`, `default_newline`, `default_indent`, `has_trailing_newline`, `future_imports` (manifest); byte spans/line spans (later providers).
* Recompute on demand: `Module.code_for_node` snippets (for preview-only), but never treat them as authoritative slicing boundaries (use spans against original bytes). ([LibCST][2])

---

### 1.4 Failure modes: `ParserSyntaxError` as a structured artifact (not a log line)

LibCST parse entrypoints raise **`ParserSyntaxError`** (not `SyntaxError`) to avoid conflating with CPython’s broader `SyntaxError` causes. ([LibCST][1])

Useful fields (store them):

* `message: str` (no location; use `str(ex)` for full formatted) ([LibCST][1])
* `raw_line: int` (1-indexed), `raw_column: int` (0-indexed char offset) ([LibCST][1])
* `context: str | None` (line + caret, or `None` if no relevant non-empty line) ([LibCST][1])
* `editor_line`, `editor_column` (editor-friendly; tab expansion assumptions) ([LibCST][1])

**Canonical “best-effort parse” wrapper (index pipeline primitive):**

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union
import libcst as cst

@dataclass(frozen=True)
class ParseOk:
    path: str
    module: cst.Module
    encoding: str
    default_indent: str
    default_newline: str
    has_trailing_newline: bool
    future_imports: frozenset[str]

@dataclass(frozen=True)
class ParseErr:
    path: str
    message: str
    raw_line: int
    raw_column: int
    editor_line: int
    editor_column: int
    context: Optional[str]

def parse_file_best_effort(path: Path) -> Union[ParseOk, ParseErr]:
    data = path.read_bytes()  # bytes-first for encoding fidelity
    try:
        m = cst.parse_module(data)  # encoding inferred/preserved
        # NOTE: these are Module attrs; persist in your parse manifest row
        return ParseOk(
            path=str(path),
            module=m,
            encoding=m.encoding,
            default_indent=m.default_indent,
            default_newline=m.default_newline,
            has_trailing_newline=m.has_trailing_newline,
            future_imports=frozenset(m.future_imports),
        )
    except cst.ParserSyntaxError as ex:
        return ParseErr(
            path=str(path),
            message=ex.message,
            raw_line=ex.raw_line,
            raw_column=ex.raw_column,
            editor_line=ex.editor_line,
            editor_column=ex.editor_column,
            context=ex.context,
        )
```

This is the “LLM-agent usable” contract: downstream stages can branch on `ParseOk|ParseErr` and still emit partial indexes (“textual only”) for failing files.

---

### 1.5 Parser backend + native extension (ops-critical in 3.10+ repos)

**Packaging reality (current):**

* PyPI’s current release metadata (e.g., `libcst 1.8.6`, released Nov 3, 2025) describes **Python 3.0→3.14 parsing** and notes LibCST **ships with a native extension** (wheels; otherwise requires Rust toolchain). ([PyPI][3])
* The GitHub repo README repeats: native parsing ships as wheels; building from source requires Rust/cargo; `libcst.native` is the module. ([GitHub][4])

**Legacy/compat knob (environment variable):**

* Third-party ecosystem notes (Hypothesis changelog) indicate: **LibCST 1.0 uses the native parser by default**, so consumers stopped setting `LIBCST_PARSER_TYPE=native`; older LibCST versions may require explicitly setting it. ([Hypothesis Documentation][5])

**Practical guidance for indexers:**

* Treat “native parser availability” as a **startup invariant**: validate with a tiny parse of syntax your repo contains (e.g., `match/case` if applicable) and crash early if unavailable (better than silently downgrading parse coverage). PyPI’s “native extension + wheels” note is your deploy contract. ([PyPI][3])
* The ReadTheDocs parser page is **stale vs PyPI** on supported Python versions; do not use it as your sole source of “grammar ceiling.” Prefer your installed package metadata + smoke tests. ([LibCST][1])

---

### 1.6 Throughput patterns (what LibCST itself does at scale)

LibCST’s own codemod runner contains two “copy this” scale patterns:

1. **Warm the parser pre-fork** (avoid per-child cold init cost):

* Before creating a `ProcessPoolExecutor`, it calls `parse_module("", config=PartialParserConfig(...))`. ([LibCST][6])

2. **If doing repo-wide metadata, compute caches once and share read-only**:

* When a repo root is provided, it constructs a `FullRepoManager(...)` and calls `resolve_cache()` before parallel execution. (Metadata details belong to later sections, but the scale pattern is crucial.) ([LibCST][6])

Minimal “borrowed” skeleton:

```python
from concurrent.futures import ProcessPoolExecutor
import libcst as cst

def warm_parser(python_version: str | None = None) -> None:
    cst.parse_module(
        b"",  # bytes keeps the fast path consistent with repo ingest
        config=(cst.PartialParserConfig(python_version=python_version) if python_version else cst.PartialParserConfig()),
    )

def worker_parse(path: str):
    return parse_file_best_effort(Path(path))

# parent
warm_parser()
with ProcessPoolExecutor(max_workers=8) as ex:
    results = list(ex.map(worker_parse, files))
```

(If you’re building a persistent service, do the warmup at process startup, not per request.)

---

**Template source used for structure:** 

[1]: https://libcst.readthedocs.io/en/latest/parser.html "Parsing — LibCST  documentation"
[2]: https://libcst.readthedocs.io/en/latest/nodes.html "Nodes — LibCST  documentation"
[3]: https://pypi.org/project/libcst/ "libcst · PyPI"
[4]: https://github.com/Instagram/LibCST "GitHub - Instagram/LibCST: A concrete syntax tree parser and serializer library for Python that preserves many aspects of Python's abstract syntax tree"
[5]: https://hypothesis.readthedocs.io/en/latest/changelog.html?utm_source=chatgpt.com "Changelog - Hypothesis 6.148.7 documentation"
[6]: https://libcst.readthedocs.io/en/latest/_modules/libcst/codemod/_cli.html "libcst.codemod._cli — LibCST  documentation"

## 2) Template parsing & safe node construction (indexer-friendly refactor primitives)

### Mental model (why templates exist)

* **Templates = “string syntax + typed holes”**: you write valid Python surface syntax, then splice **real `CSTNode`s** into `{placeholders}` *before* parsing, yielding nodes that the parser would have produced (incl. parentheses/precedence handling) without you hand-assembling deep node graphs. ([LibCST][1])
* **Templates are *not* a bytes/encoding facility**: template helpers accept `str` only because they must preprocess `{…}` placeholders before parsing; use module bytes for file IO, then templates only for *construction* inside transforms. ([LibCST][2])
* **Config fidelity is the whole point**: always pass the current module’s parse config so newly constructed nodes inherit newline/indent conventions. LibCST explicitly calls this “best practice” in the template helper docs. ([LibCST][2])

---

### 2.1 Template APIs (exact contracts + type surface)

#### `libcst.helpers.parse_template_module(template: str, config=PartialParserConfig(), **repls) -> cst.Module`

* Accepts full module text incl. leading/trailing whitespace; `{name}` holes are replaced by CST nodes “f-string style.” ([LibCST][1])
* **No bytes input** (preprocessed before parsing). ([LibCST][1])

#### `libcst.helpers.parse_template_statement(template: str, config=..., **repls) -> SimpleStatementLine | BaseCompoundStatement`

* Accepts statement template; **ensures trailing newline** (adds one if missing). ([LibCST][2])
* Use when you need a syntactically-valid `BaseStatement` for insertion into a suite/module body.

#### `libcst.helpers.parse_template_expression(template: str, config=..., **repls) -> BaseExpression`

* Accepts a **single-line expression**; **no leading/trailing whitespace allowed** (“nowhere to store it on the expression node”). ([LibCST][2])

#### Replacement type surface (“what can legally fill a `{hole}`”)

The helper signatures enumerate permitted replacement node categories (representative set): `BaseExpression`, `Annotation`, `AssignTarget`, `Param`, `Parameters`, `Arg`, `BaseStatement`/`BaseSmallStatement`, `BaseSuite`, `BaseSlice`, `SubscriptElement`, `Decorator`. (This is intentionally *not* “anything,” so your holes stay syntax-context-valid.) ([LibCST][1])

#### Under-the-hood invariants (what to rely on)

* Implementation flow (relevant for debugging): template text is “mangled” → parsed via core `parse_*` → “unmangled” transform substitutes replacements → a checker walks the result and **raises if any template variable was not replaced**. ([LibCST][2])

---

### 2.2 Best practice: pass module config into templates (newline/indent determinism)

**Rule**: capture `module.config_for_parsing` (or equivalent) once per transform and thread it into every `parse_template_*` call.

```python
from __future__ import annotations

from dataclasses import dataclass
import libcst as cst
from libcst import helpers

@dataclass
class AddTypingImport(cst.CSTTransformer):
    _cfg: cst.PartialParserConfig | None = None

    def visit_Module(self, node: cst.Module) -> None:
        # “Config fingerprint” of the file: newline/indent/encoding defaults.
        self._cfg = node.config_for_parsing

    def leave_Module(self, original_node: cst.Module, updated_node: cst.Module) -> cst.Module:
        assert self._cfg is not None

        stmt = helpers.parse_template_statement(
            "from {mod} import {name}\n",
            config=self._cfg,
            mod=cst.Name("typing"),
            name=cst.Name("Optional"),
        )

        # Insert at top; in real systems you’d de-dup / respect shebang + encoding cookie.
        return updated_node.with_changes(body=(stmt, *updated_node.body))
```

Key points:

* `parse_template_*` is *construction*, not a replacement mechanism; you still insert via immutable `with_changes`.
* You avoid “mixed formatting defaults” by using the same parsing config the module was parsed with (explicitly recommended by the helper docs). ([LibCST][2])

---

### 2.3 Template-driven patch emission (index → refactor preview)

For advanced agents, the most useful pattern is: **emit findings** (index rows) and **emit deterministic patch previews** from the same matcher/template logic, without committing to “full codemod infra.”

**Minimal patch artifact contract**

* Store **(a)** file path, **(b)** finding span(s) (from metadata later), **(c)** original snippet (slice raw source by span), **(d)** replacement snippet (from template + `Module.code_for_node` or full rewritten module), **(e)** optional unified diff.

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import difflib
import libcst as cst

@dataclass(frozen=True)
class PatchPreview:
    path: str
    unified_diff: str  # small + token-cheap
    new_code: str      # optional; you can omit if diff is enough

def preview_transform(path: Path, transformer: cst.CSTTransformer) -> PatchPreview:
    old = path.read_text(encoding="utf-8")  # bytes-first preferred for fidelity; simplified here
    mod = cst.parse_module(old)
    new_mod = mod.visit(transformer)
    new = new_mod.code

    diff = "\n".join(
        difflib.unified_diff(
            old.splitlines(),
            new.splitlines(),
            fromfile=str(path),
            tofile=str(path),
            lineterm="",
        )
    )
    return PatchPreview(path=str(path), unified_diff=diff, new_code=new)
```

Design intent:

* Templates keep construction safe; spans (from metadata providers) keep anchoring safe; diffs keep review cheap.
* This scales because you can run “index-only,” “preview-only,” or “apply” from the same primitives.

---

## 3) Structural search with matchers (advanced patterns for indexing)

### Mental model (what matchers buy you)

* **Matchers are “shape queries” over CSTs**: for every node type there is a corresponding matcher; matcher attributes default to **`DoNotCare()`**, so you specify only the fields that matter. ([LibCST][3])
* You have two orthogonal axes:

  1. **Traversal strategy**: `findall` / visitor decorators / explicit visitors
  2. **Extraction strategy**: raw node hits vs `SaveMatchedNode`-driven structured captures via `extract`/`extractall`. ([LibCST][3])
* Matchers are **metadata-aware** when you use `MatchMetadata*` and supply a `MetadataWrapper` / resolver. ([LibCST][3])

---

### 3.1 Core matcher APIs (and when each is the indexing primitive)

#### `matches(node, matcher, *, metadata_resolver=None) -> bool`

* Tests a single node; root matcher must be a concrete matcher or `OneOf/AllOf`; cannot be `MatchIfTrue` / `DoesNotMatch` / sequence wildcards as root. ([LibCST][3])

#### `findall(tree_or_wrapper, matcher, *, metadata_resolver=None) -> Sequence[CSTNode]`

* Traverses node + descendants, returning matching nodes. Can accept a `MetadataWrapper` as the tree; if so and `metadata_resolver` is unset, the wrapper becomes the resolver automatically. ([LibCST][3])

#### `extract(node, matcher, *, metadata_resolver=None) -> dict | None`

* Like `matches`, but returns **captured values** keyed by `SaveMatchedNode(name=...)` if the node matches. Capture collision semantics: if the same name appears multiple times, **parent captures override child captures**, and later sequence elements override earlier ones. ([LibCST][3])

#### `extractall(tree_or_wrapper, matcher, *, metadata_resolver=None) -> Sequence[dict]`

* Like `findall`, but returns capture dicts for each match (conceptually “`findall` + `extract`”). Wrapper auto-resolver behavior matches `findall`. ([LibCST][3])

#### `replace(tree_or_wrapper, matcher, replacement, *, metadata_resolver=None) -> new_tree`

* Structural rewrite shortcut: replacement can be a fixed node or a callable `(matched_node, extracted_dict) -> replacement_node`; always returns a new tree. ([LibCST][3])

---

### 3.2 Advanced matcher constructs (special matchers + sequence wildcards + captures)

#### Special matchers (non-node logic)

* `OneOf` / `AllOf`: OR / AND composition (also supports `|` and `&` operator shorthands). ([LibCST][3])
* `TypeOf`: match any of several node types (also supports `|` type shorthand). ([LibCST][3])
* `DoesNotMatch` (or `~matcher`): invert a matcher (not root). ([LibCST][3])
* `MatchIfTrue` / `MatchRegex`: arbitrary predicate on an attribute, regex convenience for string attrs. ([LibCST][3])

#### Captures

* `SaveMatchedNode(matcher, name="...")`: bind the matched subnode (or matched sequence) into the `extract/extractall` dict. ([LibCST][3])

#### Sequence wildcard matchers (for list-ish attributes)

Crucial property: **LibCST does not partial-match sequences implicitly**; treat sequence patterns like `re.fullmatch`, not `re.match`; use wildcards at boundaries as needed. ([LibCST][3])

* `AtLeastN(n=..., matcher=DoNotCare())`: count-based or “each element matches” constraints. ([LibCST][3])
* `ZeroOrMore(matcher=DoNotCare())`: convenience wrapper for `AtLeastN(n=0, ...)`. ([LibCST][3])
* `AtMostN(n=..., matcher=DoNotCare())`, `ZeroOrOne(...)`: analogous upper-bound constraints (same mental model). ([LibCST][3])

**Index-grade patterns**

* “calls with ≥3 args”: `m.Call(args=[m.AtLeastN(n=3)])` ([LibCST][3])
* “calls whose *first* arg is int, rest arbitrary”: `m.Call(args=[m.Arg(m.Integer()), m.ZeroOrMore()])` ([LibCST][3])
* “calls with a string arg anywhere”: `m.Call(args=[m.ZeroOrMore(), m.Arg(m.SimpleString()), m.ZeroOrMore()])` ([LibCST][3])

---

### 3.3 Turning matcher hits into structured index rows (extractall-first pattern)

When you’re indexing, you generally want **structured captures**, not bare node lists.

```python
from __future__ import annotations

from typing import Iterable, cast
import libcst as cst
import libcst.matchers as m
from libcst.metadata import MetadataWrapper

# Capture the callee expression (Name or Attribute) and the Call node itself.
CALL_CAPTURE = m.Call(
    func=m.SaveMatchedNode(m.OneOf(m.Name(), m.Attribute()), "callee"),
)

def iter_calls(module: cst.Module) -> Iterable[cst.CSTNode]:
    wrapper = MetadataWrapper(module)  # later: add providers for spans/qualified names
    # Use findall for "where are the calls", extractall when you need capture dicts.
    return m.findall(wrapper, CALL_CAPTURE)

def extract_callees(module: cst.Module) -> list[cst.CSTNode]:
    wrapper = MetadataWrapper(module)
    rows = []
    for d in m.extractall(wrapper, CALL_CAPTURE):
        rows.append(cast(cst.CSTNode, d["callee"]))
    return rows
```

Notes:

* Passing `MetadataWrapper` as the tree enables metadata-aware matchers later and also auto-wires the resolver if you don’t pass `metadata_resolver` explicitly. ([LibCST][3])
* If you need “callee anywhere in args” or “capture multiple args,” use sequence wildcards + `SaveMatchedNode` around the relevant sequence location (and remember capture collision rules). ([LibCST][3])

---

### 3.4 Metadata-aware matching (`MatchMetadata` / `MatchMetadataIfTrue`) for semantic indexing

#### `MatchMetadata(provider, value)`

* Looks up provider metadata at the current node and compares for equality; **if provider is unresolved, raises `LookupError`** (signal to wrap in `MetadataWrapper`). If metadata missing at a node, it’s “not a match.” ([LibCST][3])

Example: restrict to STORE contexts (assignment targets), or to LOAD contexts for call arguments. ([LibCST][3])

#### `MatchMetadataIfTrue(provider, func)`

* Fetch metadata and predicate over it; useful for “any qualified name equals X” sets, or position-based constraints. ([LibCST][3])

Example pattern (qualified name membership):

```python
import libcst.matchers as m
import libcst.metadata as meta

DICT_ARG = m.Arg(
    m.MatchMetadataIfTrue(
        meta.QualifiedNameProvider,
        lambda qualnames: any(q.name == "typing.Dict" for q in qualnames),
    )
)
```

This enables high-precision indexing where raw syntax is ambiguous (aliases, re-exports, `import as`, etc.). ([LibCST][3])

---

### 3.5 Decorator-driven matcher traversal (“where am I?” gating without manual stacks)

#### When to use

* You want visitor lifecycle + state, but with matcher-driven dispatch instead of `visit_<Node>` boilerplate.
* You want cheap scoping: “only run this logic inside any `FunctionDef` / `ClassDef` / `If TYPE_CHECKING` block,” etc.

#### Key decorators

* `@m.visit(matcher)` / `@m.leave(matcher)` attach matcher dispatch to methods; **decorated `visit` functions cannot stop child traversal by returning `False`** (unlike explicit `visit_<Node>`). ([LibCST][3])
* `@m.call_if_inside(matcher)` gates calls to the method unless the current node or any parent matches the supplied matcher; `call_if_not_inside` is the complement. ([LibCST][3])
* To use these decorators, subclass `MatcherDecoratableVisitor` / `MatcherDecoratableTransformer` (regular visitors don’t pay the overhead). ([LibCST][3])

```python
from __future__ import annotations

import libcst as cst
import libcst.matchers as m

class CallCollector(m.MatcherDecoratableVisitor):
    def __init__(self) -> None:
        self.calls_in_funcs: int = 0

    @m.visit(m.Call())
    @m.call_if_inside(m.FunctionDef())  # only count calls inside any function
    def _count_calls_in_functions(self, node: cst.CSTNode) -> None:
        self.calls_in_funcs += 1
```

Traversal foot-gun (important for correctness):

* If you also implement an explicit `visit_<Node>` that returns `False`, LibCST will skip traversing that node’s children (so matcher-decorated methods for descendants won’t fire). This is a powerful pruning tool but easy to misuse; treat it as a global “subtree cut.” ([LibCST][3])

[1]: https://libcst.readthedocs.io/en/latest/helpers.html "Helpers — LibCST  documentation"
[2]: https://libcst.readthedocs.io/en/latest/_modules/libcst/helpers/_template.html "libcst.helpers._template — LibCST  documentation"
[3]: https://libcst.readthedocs.io/en/latest/matchers.html "Matchers — LibCST  documentation"

## 4) Metadata for indexing: operational mastery

### Mental model (indexer-grade invariants)

* **Metadata is node-identity keyed.** `MetadataWrapper` deep-copies the module so a node identity appears only once; you must analyze **`wrapper.module`**, not the original module, or your node keys won’t hit metadata maps. ([LibCST][1])
* **Two consumption modes:** (a) *pull* via `wrapper.resolve(Provider)` → `{CSTNode: T}`; (b) *push* via visitor dependencies (`METADATA_DEPENDENCIES`) + `self.get_metadata(...)` during traversal. ([LibCST][1])
* **Providers are explicit + typed + dependency-driven.** A provider can declare dependencies; `resolve_many()` does **not** automatically include undeclared dependency metadata in its return map, so “include what you need.” ([LibCST][1])
* **Repo-wide providers require external cache.** Providers can implement `gen_cache`; `FullRepoManager` precomputes per-path caches and injects them into `MetadataWrapper(cache=...)`. ([LibCST][1])

---

### 4.1 Access patterns: `resolve` vs dependency declaration (choose deliberately)

#### Pattern A: `MetadataWrapper.resolve()` (pull; fast to script; good for “index extraction” passes)

```python
import libcst as cst
from libcst.metadata import MetadataWrapper, PositionProvider

m = cst.parse_module("x = 1\n")
w = MetadataWrapper(m)

pos_map = w.resolve(PositionProvider)          # Mapping[CSTNode, CodeRange]; copy returned
x_name = w.module.body[0].body[0].targets[0].target.value  # w.module nodes only
x_range = pos_map[x_name].start.line
```

`resolve(provider)` returns a **copy** of that provider’s mapping; treat it as immutable snapshot. ([LibCST][1])

#### Pattern B: visitor dependencies (push; best when you already traverse and want cheap per-node reads)

```python
import libcst as cst
from libcst.metadata import MetadataWrapper, PositionProvider

class NamePrinter(cst.CSTVisitor):
    METADATA_DEPENDENCIES = (PositionProvider,)
    def visit_Name(self, node: cst.Name) -> None:
        p = self.get_metadata(PositionProvider, node).start
        # do not print in real indexers; emit rows
        _ = (node.value, p.line, p.column)

w = MetadataWrapper(cst.parse_module("x = 1"))
w.visit(NamePrinter())  # NB: wrapper.visit (not module.visit)
```

`MetadataDependent.get_metadata()` is only legal for providers declared in the MRO’s `METADATA_DEPENDENCIES`; `wrapper.visit(...)` auto-resolves those dependencies. ([LibCST][1])

#### Pattern C: manual dependency lifecycle (`MetadataDependent.resolve(wrapper)` context manager)

Use when you want to reuse the same visitor across multiple traversals without recomputing deps each time; the cache is cleared on exit. ([LibCST][1])

---

### 4.2 `MetadataWrapper` identity rules (the #1 failure mode in advanced pipelines)

* `MetadataWrapper(module).module == module` is `False` because the wrapper **deep-copies** by default to prevent duplicate node identities. ([LibCST][1])
* `unsafe_skip_copy=True` skips cloning (small perf gain) and is only safe if you *know* there are no duplicate node identities (e.g., a fresh parser output). ([LibCST][1])
* Practical rule: **never store CST node objects across wrappers** (or across parses); persist **spans + stable IDs**, re-resolve nodes per run.

---

### 4.3 Provider taxonomy + extension points (how metadata is computed)

LibCST metadata providers are typed and come in three main base classes: ([LibCST][1])

* `BaseMetadataProvider[T]`: non-visitor providers; also the base that defines `gen_cache` and `set_metadata`. ([LibCST][1])
* `BatchableMetadataProvider[T]`: **preferred** visitor-based providers (more efficient; batchable traversal). ([LibCST][1])
* `VisitorMetadataProvider[T]`: visitor-based but non-batchable (use only if you must). ([LibCST][1])

Core APIs:

* `set_metadata(node, value | LazyValue[value])`: record metadata for a node (LazyValue supported for deferred computation). ([LibCST][1])
* `METADATA_DEPENDENCIES`: provider dependencies (e.g., `QualifiedNameProvider` depends on `ScopeProvider`). ([LibCST][1])
* `gen_cache(...)`: optional classmethod hook signaling repo-wide cache needs; invoked by `FullRepoManager`. ([LibCST][1])

---

### 4.4 Multi-provider resolution and batching (token-cheap + CPU-cheap patterns)

* `resolve_many({P1, P2, ...}) -> {Provider: {node: value}}` returns **only those providers’ own maps**, not extra dependency maps. If you need the dependency maps as top-level results (rare), include them explicitly. ([LibCST][1])
* `MetadataWrapper.visit_batched(visitors)` exists for running multiple *batchable visitors* in one traversal (useful when you compute many indexes from one parse). ([LibCST][1])

---

### 4.5 Position metadata (spans are your primary index keys)

#### Line/column: `PositionProvider` vs `WhitespaceInclusivePositionProvider`

* `PositionProvider`: start/end bounds ignore most leading/trailing whitespace when not syntactically significant (good for semantic spans). ([LibCST][1])
* `WhitespaceInclusivePositionProvider`: includes all whitespace owned by the node (good for patching that preserves trivia ownership). ([LibCST][1])
* Representation: `CodeRange(start: CodePosition, end: CodePosition)` with **start inclusive / end exclusive**; line numbers **1-indexed**, columns **0-indexed**. ([LibCST][1])

#### Byte spans: `ByteSpanPositionProvider` + `CodeSpan`

* Emits `(start_offset_bytes, length_bytes)`; whitespace owned by node not included in length; offsets measure **bytes, not characters** (critical for UTF-8 multibyte). ([LibCST][1])

**Indexer stance**

* Persist **both** a human span (`CodeRange`) and a byte span (`CodeSpan`) when you need exact slicing against raw bytes; treat byte spans as canonical for “evidence snippet extraction.”

---

### 4.6 Expression context metadata (read/write/delete classification)

`ExpressionContextProvider` mimics `ast.expr_context` for a fixed set of node types: `Attribute`, `Subscript`, `StarredElement`, `List`, `Tuple`, `Name`; but a `Name` **may not always have context** (notably the `attr` in an attribute access is a `Name` in LibCST but a `str` in `ast`, so LibCST avoids assigning it context to match AST semantics). ([LibCST][1])

**Index recipe**

* Reference index wants `LOAD/STORE/DEL` roles; use ExpressionContext to label *syntax-level* reads/writes, then refine with Scope/QualifiedName for referents.

---

### 4.7 Parent node metadata (bottom-up queries without manual stacks)

`ParentNodeProvider` exists because CST nodes only link to children; parent links enable bottom-up traversal and “enclosing construct” inference (e.g., find the `FunctionDef` containing a `Call`). ([LibCST][1])

**Common indexer pattern**

* Given a hit node, walk parents until you hit a boundary node type (`FunctionDef`, `ClassDef`, `Module`) and attach that as the “container id.”

---

### 4.8 Scope metadata (turn syntax into a symbol/reference graph)

#### Scope model (Python-accurate)

* Python is function-scoped; new scopes exist for **modules, classes, functions, comprehensions**; conditionals/loops/try don’t create scopes. LibCST provides `BuiltinScope`, `GlobalScope`, `FunctionScope`, `ClassScope`, `ComprehensionScope`. ([LibCST][1])

#### Assignments/accesses (what you actually index)

* Scopes let you inspect what local variables are assigned or accessed. Import statements are represented as assignments; dotted imports (`import a.b.c`) generate multiple assignments (`a`, `a.b`, `a.b.c`), and accesses record only the most specific applicable assignment. ([LibCST][1])
* `Access` records an access and includes:

  * `node` (usually a `Name`; special cases include dotted imports and (some) string-annotation forms)
  * `scope` (access scope may be a child of assignment scope)
  * `is_annotation`, `is_type_hint`
  * `referents`: the assignments this access can refer to ([LibCST][1])
* Scope lookup APIs:

  * `name in scope` via `__contains__`
  * `scope[name] -> Set[BaseAssignment]` (set, not single binding; Python permits rebinding and analysis must model multiple assignments). ([LibCST][1])
* Explicit limitation: scope analysis is about **local variable names**; it does not treat arbitrary attribute names as local bindings (e.g., `a.b.c = 1` records assignment to `a`, not `c`). ([LibCST][1])

**Reference index core loop (semantic, not just syntactic)**

* Use `ExpressionContextProvider` to tag each access as read/write.
* Use `ScopeProvider`’s `Access.referents` to attach candidate definitions/bindings.
* Store “may refer to multiple” as a first-class concept (sets → edges).

---

### 4.9 Qualified names (definition localization under ambiguity)

#### `QualifiedNameProvider` (module-relative; can be non-unique)

* Provides possible qualified names for a node, conceptually aligned with PEP 3155 “qualified name,” but **module-relative** (so not called “fully qualified”). ([LibCST][1])
* **Multiple qualified names can be returned** (conditional imports, shadowing); provider uses `get_qualified_names_for()` and depends on `ScopeProvider`. ([LibCST][1])
* Qualified name includes `source`: `IMPORT | BUILTIN | LOCAL`. ([LibCST][1])

#### `FullyQualifiedNameProvider` (repo-absolute; resolves relative imports)

* Like `QualifiedNameProvider`, but produces **absolute identifiers** using module location relative to a python root (typically repo root); integrates with `FullRepoManager` and resolves relative imports; the module’s own fully-qualified name is stored as metadata on the `Module` node. ([LibCST][1])

**Indexer stance**

* Store `QualifiedNameProvider` outputs for single-file indexes; upgrade to `FullyQualifiedNameProvider` when you need repo-global identity and you can afford `FullRepoManager` setup.

---

### 4.10 File path metadata (attach disk identity to the CST)

`FilePathProvider` provides the current file path as metadata on the root `Module` node; requires `FullRepoManager` and returns an **absolute** `pathlib.Path.resolve()` value. ([LibCST][1])

---

### 4.11 Type inference metadata (Pyre-backed; operationally heavy but high-value)

`TypeInferenceProvider` uses the **Pyre Query API**, requires Watchman and a running Pyre server, and returns inferred type annotations as strings. Pyre infers types for `Name`, `Attribute`, `Call`; names are fully qualified regardless of import aliasing. IPC is managed by `FullRepoManager`. ([LibCST][1])

**Indexer stance**

* Treat type inference as an optional enrichment layer: emit rows with `type_str | null` plus a `source="pyre"` marker; keep the rest of the index functional without it.

---

### 4.12 FullRepoManager (repo-wide cache orchestration; pre-fork control)

`FullRepoManager(repo_root_dir, paths, providers, timeout=..., use_pyproject_toml=...)` manages repo-wide cache for providers that need it (documented support: `TypeInferenceProvider`, `FullyQualifiedNameProvider`). It exposes:

* `cache`: `{Provider: {path: cache_obj}}`
* `resolve_cache()`: explicitly resolve caches (recommended before forking to control where expensive work happens)
* `get_cache_for_path(path)` → `{Provider: cache_obj}` to pass into `MetadataWrapper(module, cache=...)`
* `get_metadata_wrapper_for_path(path)` reads/parses file and returns a wrapper (path is relative to repo root). ([LibCST][1])

**Canonical pre-fork pattern**

```python
from libcst.metadata import FullRepoManager, FullyQualifiedNameProvider, TypeInferenceProvider

mgr = FullRepoManager(
    repo_root_dir=".",
    paths=set(py_files_rel),
    providers={FullyQualifiedNameProvider, TypeInferenceProvider},
)
mgr.resolve_cache()  # control timing before fork/spawn
# worker: wrapper = mgr.get_metadata_wrapper_for_path(rel_path)  OR parse yourself + MetadataWrapper(module, cache=mgr.get_cache_for_path(rel_path))
```

This is the “ops-grade” mechanism that keeps repo-wide providers deterministic and amortized. ([LibCST][1])

---

### 4.13 Writing custom providers (index-specific metadata without ad-hoc passes)

LibCST explicitly recommends extending `BatchableMetadataProvider` “for most cases” because batchable providers resolve more efficiently. Minimal pattern: track state during traversal, call `set_metadata` on target nodes, optionally read your own provider metadata via `get_metadata(type(self), node, default)` to avoid double-setting. ([LibCST][2])

```python
import libcst as cst

class IsParamProvider(cst.BatchableMetadataProvider[bool]):
    def __init__(self) -> None:
        super().__init__()
        self.is_param = False

    def visit_Param(self, node: cst.Param) -> None:
        self.set_metadata(node.name, True)

    def visit_Name(self, node: cst.Name) -> None:
        if not self.get_metadata(type(self), node, False):
            self.set_metadata(node, False)
```

This is the cleanest way to add “index-only semantics” (e.g., “is exported symbol”, “is public API surface”, “is docstring-bearing def”) without building parallel traversals. ([LibCST][2])

[1]: https://libcst.readthedocs.io/en/latest/metadata.html "Metadata — LibCST  documentation"
[2]: https://libcst.readthedocs.io/en/latest/metadata_tutorial.html "Working with Metadata — LibCST  documentation"


## 5) Repo-wide indexing with `FullRepoManager` (and concurrency)

### Mental model (what `FullRepoManager` actually *is*)

* **Purpose:** coordinate **repo-scoped cache** for metadata providers whose `gen_cache(...)` needs external context/IPC (documented: `TypeInferenceProvider`, `FullyQualifiedNameProvider`), then feed *per-file cache shards* into `MetadataWrapper(cache=...)`. ([LibCST][1])
* **Key inputs:** `repo_root_dir` (project root), `paths` (collection of file paths you will index), `providers` (collection of provider classes that require repo access), `timeout`, `use_pyproject_toml`. ([LibCST][1])
* **Invariant:** `get_cache_for_path(path)` only works if `path` was included in `paths` at construction; otherwise it raises. ([LibCST][2])
* **Cache shape:** `manager.cache` is `Dict[ProviderT, Mapping[path, cache_obj]]` and calling `.cache` implicitly triggers `resolve_cache()` if not already done. ([LibCST][1])

---

### 5.1 Lifecycle + cache model (resolution timing is the concurrency lever)

#### Cache resolution (`resolve_cache`)

* `resolve_cache()` computes provider caches *once* by calling each provider’s `gen_cache(root_path, paths, timeout=..., use_pyproject_toml=...)` and storing results in `self._cache`. ([LibCST][2])
* Docs explicitly note: you **don’t need** to call it because `get_cache_for_path()` calls it, but you **should** call it explicitly if you want “single cache resolution pass before forking” (i.e., control where expensive cache build happens). ([LibCST][1])

#### Per-file cache extraction (`get_cache_for_path`)

* After ensuring the path is allowed and the cache is resolved, `get_cache_for_path(path)` returns a `Mapping[ProviderT, object]` containing only the cache objects for that specific path. ([LibCST][1])

#### Convenience wrapper (`get_metadata_wrapper_for_path`)

* `get_metadata_wrapper_for_path(path)` expects **path relative to repo root**, reads & parses the file, fetches cache via `get_cache_for_path`, and returns `MetadataWrapper(module, unsafe_skip_copy=True, cache=cache)`. ([LibCST][1])
* Implementation detail worth knowing: it uses `Path.read_text()` + `cst.parse_module(str)` (not bytes), then constructs `MetadataWrapper(..., True, cache)` (i.e., `unsafe_skip_copy=True`). ([LibCST][2])

---

### 5.2 Two repo-indexing wiring patterns (choose based on IO fidelity needs)

#### Pattern A: “Manager wrapper does IO + parse” (fastest to wire; str-based)

Use `get_metadata_wrapper_for_path(rel_path)` and operate on `wrapper.module`.

```python
from __future__ import annotations
from pathlib import Path
import libcst as cst
from libcst.metadata import FullRepoManager, FullyQualifiedNameProvider, TypeInferenceProvider

repo_root = Path(".").resolve()
paths = {"pkg/a.py", "pkg/b.py"}  # relative-to-root strings
providers = {FullyQualifiedNameProvider, TypeInferenceProvider}

mgr = FullRepoManager(repo_root, paths, providers)
mgr.resolve_cache()  # optional; becomes critical pre-fork

w = mgr.get_metadata_wrapper_for_path("pkg/a.py")
m = w.module  # analyze wrapper.module only
```

**Contract surface (doc-grade):** method exists, reads/parses module, path is relative to root, uses cache. ([LibCST][1])

#### Pattern B: “You own IO/parse; manager supplies cache” (bytes-first pipelines)

Parse bytes yourself (encoding-fidelity, file policy control), then inject cache via `MetadataWrapper(module, cache=mgr.get_cache_for_path(rel_path))`. This is *exactly* the pattern shown in the metadata docs for `get_cache_for_path`. ([LibCST][1])

```python
from pathlib import Path
import libcst as cst
from libcst.metadata import MetadataWrapper

rel_path = "pkg/a.py"
raw = (repo_root / rel_path).read_bytes()
module = cst.parse_module(raw)  # bytes parse; preserves encoding/newlines at Module-level

cache = mgr.get_cache_for_path(rel_path)
w = MetadataWrapper(module, cache=cache)
```

---

### 5.3 Concurrency primitives (what changes in Python 3.14+ and why you care)

#### Process start methods now materially affect your design

* Python documents that:

  * **macOS:** since 3.8, default start method is **spawn** (fork considered unsafe). ([Python documentation][3])
  * **POSIX overall:** since 3.14, default start method is **forkserver** (fork is no longer default on any platform). ([Python documentation][3])
* Consequences:

  * In spawn/forkserver modes, **process targets + arguments must be picklable**, and `__main__` must be importable (classic “don’t define worker functions in REPL” constraint). ([Python documentation][3])
  * Python explicitly advises libraries using `multiprocessing` or `ProcessPoolExecutor` to let users provide their own multiprocessing context and to document start-method requirements. ([Python documentation][3])

**Implication for `FullRepoManager`:** whether you can “share the manager object” across workers depends on start method; under fork you often get COW-sharing; under spawn/forkserver you pay pickling and must ensure the object is picklable.

---

### 5.4 Reference concurrency pattern (LibCST codemod runner = canonical example)

LibCST’s own parallel codemod runner (`parallel_exec_transform_with_prettyprint`) embodies the key repo-wide + concurrency pattern:

1. **Deduplicate + sort absolute file paths** to avoid write races and ensure deterministic scheduling. ([LibCST][4])
2. **Create `FullRepoManager(repo_root, files, transform.get_inherited_dependencies())` when `repo_root` is provided**, then **call `resolve_cache()` explicitly before forking**. ([LibCST][4])
3. Use `ProcessPoolExecutor` for parallelism. ([LibCST][4])
4. **Warm the parser pre-fork** by calling `parse_module("", config=PartialParserConfig(...))`. ([LibCST][4])

That excerpt is the best “systems context” for how LibCST expects these parts to compose under load. ([LibCST][4])

---

### 5.5 A repo indexer: “precompute cache; parallel per-file parse + wrapper(cache) + emit”

Below is a minimal, *indexing-shaped* skeleton (not codemod-shaped) that mirrors LibCST’s intended lifecycle:

```python
from __future__ import annotations

from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import libcst as cst
from libcst.metadata import (
    FullRepoManager,
    MetadataWrapper,
    FullyQualifiedNameProvider,
    TypeInferenceProvider,
)

@dataclass(frozen=True)
class IndexRow:
    path: str
    kind: str
    payload: dict

def _index_one(repo_root: str, rel_path: str, cache: dict) -> list[IndexRow]:
    # bytes-first parse for fidelity; manager cache is still keyed by rel_path.
    raw = (Path(repo_root) / rel_path).read_bytes()
    mod = cst.parse_module(raw)

    w = MetadataWrapper(mod, cache=cache)
    m = w.module

    # Placeholder: your real index pass(es) here (matchers + metadata-dependent visitors).
    # Emit token-cheap rows, never CSTNodes.
    return [IndexRow(path=rel_path, kind="parse_ok", payload={"encoding": m.encoding})]

def index_repo(repo_root: Path, rel_paths: Iterable[str]) -> list[IndexRow]:
    repo_root = repo_root.resolve()
    rel_paths = sorted(set(rel_paths))

    providers = {FullyQualifiedNameProvider, TypeInferenceProvider}
    mgr = FullRepoManager(repo_root, set(rel_paths), providers)
    mgr.resolve_cache()  # deterministic pre-fork cache build

    # Pre-extract per-file caches to avoid shipping the whole manager into workers.
    # (If caches are heavy, you can instead compute cache in-worker via mgr.get_cache_for_path,
    # but then mgr must be available/picklable to workers.)
    cache_by_path = {p: mgr.get_cache_for_path(p) for p in rel_paths}

    rows: list[IndexRow] = []
    with ProcessPoolExecutor() as ex:
        futures = [
            ex.submit(_index_one, str(repo_root), p, cache_by_path[p])
            for p in rel_paths
        ]
        for fut in as_completed(futures):
            rows.extend(fut.result())
    return rows
```

Notes, grounded in docs/source behavior:

* `FullRepoManager.get_cache_for_path(p)` requires `p` to be in the manager’s `paths` set. ([LibCST][1])
* `resolve_cache()` is the explicit “before forking” hook. ([LibCST][1])
* On modern Python (3.14+ default forkserver; macOS spawn default), shipping large non-picklable objects to workers is a failure mode; hence the common “pre-extract per-file cache shard” strategy. ([Python documentation][3])

---

### 5.6 Worker topology options (pick one per start method reality)

#### Topology 1: Parent builds manager + resolves cache; workers receive **only per-file cache**

* Minimizes cross-process object graph.
* Compatible with spawn/forkserver (subject to cache object picklability). ([Python documentation][3])

#### Topology 2: Parent builds manager; workers inherit it (fork-friendly)

* Natural on fork (COW), but **fork is no longer default** on POSIX in Python 3.14 and must be explicitly requested. ([Python documentation][3])

#### Topology 3: Each worker builds its own manager (rare; heavy)

* Avoids pickling manager; repeats cache resolution unless you provide external persisted caches.

(These are operational compositions; the key doc-lever remains `resolve_cache()` timing + what crosses the process boundary.)

---

### 5.7 Concurrency safety + determinism checklist (repo indexers)

* **De-dup/sort file list** for deterministic order and to avoid accidental double-write hazards (codemod runner does this). ([LibCST][4])
* Ensure all multiprocessing entrypoints live under `if __name__ == "__main__":` when spawn/forkserver is possible (Python docs). ([Python documentation][3])
* If you need a specific start method (e.g., fork for COW-sharing), Python requires explicitly selecting it via `set_start_method()` / `get_context()` and notes default behavior changes. ([Python documentation][3])

---

[1]: https://libcst.readthedocs.io/en/latest/metadata.html "Metadata — LibCST  documentation"
[2]: https://libcst.readthedocs.io/en/latest/_modules/libcst/metadata/full_repo_manager.html "libcst.metadata.full_repo_manager — LibCST  documentation"
[3]: https://docs.python.org/3/library/multiprocessing.html "multiprocessing — Process-based parallelism — Python 3.14.2 documentation"
[4]: https://libcst.readthedocs.io/en/latest/_modules/libcst/codemod/_cli.html "libcst.codemod._cli — LibCST  documentation"


## 6) High-leverage helper APIs for indexers

### Mental model (how to stay fast + correct)

* **Prefer “semantic helpers” over ad-hoc tree spelunking**: they compress common indexing transforms (names, imports, docstrings, deep edits, structural fingerprints) into a small API surface.
* **Distinguish three name layers**:

  1. *syntactic dotted string* (`helpers.get_full_name_for_node`)
  2. *in-file semantic symbol* (`QualifiedNameProvider`)
  3. *repo-absolute semantic symbol* (`FullyQualifiedNameProvider` via `FullRepoManager`)
     Do **not** mistake (1) for (2)/(3). ([LibCST][1])

---

### 6.1 Syntactic dotted-name extraction: `get_full_name_for_node*`

**API**

* `libcst.helpers.get_full_name_for_node(node: str | CSTNode) -> str | None`
* `libcst.helpers.get_full_name_for_node_or_raise(...) -> str` ([LibCST][2])

**What it supports (exactly, per implementation)**

* `Name` → `value`
* `Attribute` → recurse `value` + `.` + `attr.value`
* `Call` → recurse `func`
* `Subscript` → recurse `value`
* `FunctionDef`/`ClassDef` → recurse `.name`
* `Decorator` → recurse `.decorator`
* else → `None` (or raise in `*_or_raise`) ([LibCST][1])

**Indexer usage patterns**

* **Call-site “cheap callee label”** (good for *shape-indexing*, not resolution):

  ```python
  import libcst as cst
  from libcst import helpers

  def callee_label(call: cst.Call) -> str | None:
      # "pkg.mod.fn" if syntactically present, else None for dynamic expressions.
      return helpers.get_full_name_for_node(call.func)
  ```
* **Import “module string” extraction**: same helper works on `ImportFrom.module` (a `Name|Attribute|None`), returning `None` for `from . import X` forms where module is absent. ([LibCST][3])

**Hard caveat (agents should internalize)**

* This is *purely syntactic flattening*; it does **not** resolve aliases, reexports, shadowing, conditionals, `import as`, or package roots. Use metadata providers for that. ([LibCST][2])

---

### 6.2 Import/module resolution helpers (when you need “absolute module strings” without full repo metadata)

LibCST ships pragmatic helpers (in `libcst.helpers.module`) used by codemod infra to reason about *relative imports* and *file→module name mapping*.

#### 6.2.1 Relative import → absolute module (string)

Key helpers (all return `Optional[str]`; `*_or_raise` variants throw):

* `get_absolute_module_for_import(current_module: str|None, import_node: ImportFrom) -> str|None`
* `get_absolute_module_from_package_for_import(current_package: str|None, import_node: ImportFrom) -> str|None` ([LibCST][3])

**Semantics (from source)**

* For `ImportFrom`, compute `module_name` via `get_full_name_for_node(import_node.module)` and `num_dots = len(import_node.relative)`, then resolve against `current_module` or `current_package`. Returns `None` if resolution is impossible (e.g., missing context or “goes past repo root”). ([LibCST][3])

**Indexer recipe: import edges with fallback absolute module**

```python
from __future__ import annotations
from pathlib import Path
import libcst as cst
from libcst import helpers

def import_from_edges(repo_root: Path, filename: Path, use_pyproject_toml: bool) -> list[tuple[str, str|None]]:
    # Map file path -> (module, package) strings.
    mp = helpers.calculate_module_and_package(repo_root, filename, use_pyproject_toml=use_pyproject_toml)
    current_module, current_package = mp.name, mp.package  # string IDs
    mod = cst.parse_module(filename.read_bytes())

    edges: list[tuple[str, str|None]] = []
    for stmt in mod.body:
        if isinstance(stmt, cst.SimpleStatementLine):
            for small in stmt.body:
                if isinstance(small, cst.ImportFrom):
                    abs_mod = helpers.get_absolute_module_from_package_for_import(current_package, small)
                    edges.append((current_module, abs_mod))
    return edges
```

* This yields **string-level import targets** adequate for import-graph indexing when you don’t (yet) want `FullRepoManager`. ([LibCST][3])

#### 6.2.2 File path → module + package (`calculate_module_and_package`)

* `calculate_module_and_package(repo_root, filename, use_pyproject_toml=False) -> ModuleNameAndPackage(name, package)` ([LibCST][3])
  **Notable behaviors**
* Strips `.py` suffix; handles `__init__.py` / `__main__.py` as package modules (module name becomes package path). ([LibCST][3])
* If `use_pyproject_toml=True`, walks up from file’s directory to find a `pyproject.toml` to establish a package root (nested roots). ([LibCST][3])

---

### 6.3 Type refinement helper: `ensure_type` (make type checkers obey your prior proofs)

* `libcst.helpers.ensure_type(obj, NodeType) -> NodeType` does `isinstance` + raises if mismatch; designed for “I already proved this structurally but the checker can’t see it.” ([LibCST][2])

**Use cases**

* Template internals use it to assert template parse returns the expected node type. ([LibCST][4])
* Indexers use it after matcher extracts when static type narrowing is inadequate.

---

### 6.4 Node-field introspection & filtering (generic structural fingerprints, debug views, semantic hashing)

Helpers (documented) ([LibCST][2]):

* `get_node_fields(node) -> Sequence[dataclasses.Field]`
* `filter_node_fields(node, *, show_defaults, show_syntax, show_whitespace) -> Sequence[Field]`
* `is_whitespace_node_field`, `is_syntax_node_field`, `is_default_node_field`, `get_field_default_value`

**What these functions actually do (relevant implementation details)**

* Whitespace fields are detected by field name patterns (`"whitespace"`, `"leading_lines"`, `"lines_after_decorators"`) and special-casing `Module/IndentedBlock` header/footer/indent. ([LibCST][5])
* Syntax fields include `Module`’s `encoding/default_indent/default_newline/has_trailing_newline`, most “Sentinel” optional syntax nodes (except some exclusions), and separator node types like `Semicolon/Colon/Comma/Dot/AssignEqual` based on field type strings. ([LibCST][5])
* Default detection uses `deep_equals(getattr(node, field), default_value)` (so “default” is *representation-aware*, not identity). ([LibCST][5])

**Indexer-grade application: semantic fingerprint (format-insensitive)**

```python
import hashlib
import libcst as cst
from libcst.helpers import filter_node_fields

def semantic_fingerprint(node: cst.CSTNode) -> str:
    fields = filter_node_fields(node, show_defaults=False, show_syntax=False, show_whitespace=False)
    # Lightweight; for real hashing you’d recurse + normalize sequences.
    payload = "|".join(f"{f.name}={getattr(node, f.name)!r}" for f in fields)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
```

This is a practical bridge between “exact CST” and “AST-like canonical shape” for indexing/dedup.

---

### 6.5 Deep-edit primitives on `CSTNode` (when you need patch previews without nested `with_changes`)

These are *node methods* (not `libcst.helpers`), but they function as “high-leverage helpers” for refactor/index tooling:

* `deep_replace(old_node, new_node)` — replace by **identity**, recursively; replaces **all** occurrences if the old node appears multiple times. ([LibCST][6])
* `deep_remove(old_node)` — remove by identity (all occurrences). ([LibCST][6])
* `with_deep_changes(old_node, **changes)` — apply `with_changes` to a deep child without chaining. ([LibCST][6])
* `deep_clone()` — re-materialize an equal-by-representation tree with distinct identities (useful in caching experiments / avoiding aliasing). ([LibCST][6])
* `deep_equals(other)` — representation equality across entire subtrees (stronger than `==` identity semantics). ([LibCST][6])

**Index/refactor pattern**

* Store a finding as `(node_id/span, old_node_identity)`, compute replacement node (often via templates), then `module.deep_replace(old, new)` to generate preview diffs.

---

### 6.6 Docstring extraction: `get_docstring(clean=True)`

Docstring extraction is “indexer gold” (API documentation, semantic summarization, linting).

* `Module.get_docstring(clean=True) -> str|None`
* `FunctionDef.get_docstring(clean=True) -> str|None`
* `ClassDef.get_docstring(clean=True) -> str|None` ([LibCST][6])

**Semantics**

* Mirrors `ast.get_docstring` behavior and can optionally apply `inspect.cleandoc` when `clean=True`. ([LibCST][7])

**Usage**

```python
import libcst as cst

m = cst.parse_module(b'"""hi""" \n\ndef f():\n    """doc"""\n    pass\n')
module_doc = m.get_docstring()
```

---

### 6.7 `CSTNode.field(...)` (safe dataclass defaults for CST nodes)

* `CSTNode.field(...)` is a classmethod helper for using CSTNodes as dataclass default values without aliasing node identities across instances. ([LibCST][6])
  This matters if your index pipeline defines dataclass “templates” containing CST node defaults (rare, but when it happens it prevents subtle identity-sharing bugs).

---

### 6.8 Practical “minimum helper kit” for indexers

If you force an LLM agent to keep only a few helpers in working memory, it should be:

* `helpers.get_full_name_for_node` (cheap syntactic names) ([LibCST][1])
* `helpers.calculate_module_and_package` + `get_absolute_module_*` (import graph + module IDs) ([LibCST][3])
* `helpers.ensure_type` (post-matcher narrowing) ([LibCST][8])
* `helpers.filter_node_fields` (semantic fingerprints) ([LibCST][5])
* `node.deep_replace / with_deep_changes` (patch previews) ([LibCST][6])
* `get_docstring` (doc indexes) ([LibCST][6])

[1]: https://libcst.readthedocs.io/en/latest/_modules/libcst/helpers/expression.html "libcst.helpers.expression — LibCST  documentation"
[2]: https://libcst.readthedocs.io/en/latest/helpers.html "Helpers — LibCST  documentation"
[3]: https://libcst.readthedocs.io/en/latest/_modules/libcst/helpers/module.html "libcst.helpers.module — LibCST  documentation"
[4]: https://libcst.readthedocs.io/en/latest/_modules/libcst/helpers/_template.html?utm_source=chatgpt.com "Source code for libcst.helpers._template"
[5]: https://libcst.readthedocs.io/en/latest/_modules/libcst/helpers/node_fields.html "libcst.helpers.node_fields — LibCST  documentation"
[6]: https://libcst.readthedocs.io/en/latest/nodes.html "Nodes — LibCST  documentation"
[7]: https://libcst.readthedocs.io/en/latest/_modules/libcst/_nodes/statement.html?utm_source=chatgpt.com "Source code for libcst._nodes.statement"
[8]: https://libcst.readthedocs.io/en/latest/_modules/libcst/helpers/common.html "libcst.helpers.common — LibCST  documentation"


## 7) Index products: canonical schemas and derivation recipes

### Mental model (indexer-grade invariants)

* **Indexes are “facts,” not CST objects**: emit *stable IDs + spans + strings + small enums*; never persist `CSTNode` identities (wrapper copies, parse cycles, and multiprocessing make them non-stable).
* **Spans are primary keys**: store both **human** (`CodeRange`) and **canonical slice** (`CodeSpan` bytes) for every entity you emit; `CodeRange.start` is inclusive and `CodeRange.end` exclusive; lines are 1-indexed, cols 0-indexed. ([LibCST][1])
* **Ambiguity is first-class**: qualified-name resolution can return *multiple candidates* (conditional imports / shadowing); call resolution and reference resolution must store sets, not scalars. ([LibCST][2])
* **Two name layers (minimum):**

  * *syntactic* dotted strings (cheap): from CST shape (e.g., `Call.func` is often `Name|Attribute`). ([LibCST][3])
  * *semantic* qualified names (expensive/accurate): `QualifiedNameProvider` (module-relative) and `FullyQualifiedNameProvider` (repo-absolute + resolves relative imports; stores module FQN on `Module`). ([LibCST][2])

---

### 7.0 Shared primitives (normalize once; reuse everywhere)

#### 7.0.1 Path + module identity

* `path`: repo-relative POSIX string (canonical), e.g. `"pkg/mod.py"`.
* `module_fqn` (optional but high-value): resolved by `FullyQualifiedNameProvider` on the `Module` node. ([LibCST][2])

#### 7.0.2 Span (store both)

* `range`: from `PositionProvider` (semantic; ignores non-significant whitespace). ([LibCST][4])
* `bspan`: from `ByteSpanPositionProvider`: `CodeSpan(start: int, length: int)` in **bytes**, whitespace owned by the node excluded. ([LibCST][5])

#### 7.0.3 Stable IDs (byte-span keyed)

Canonical `id` rule (works for every entity type):

* `id = f"{path}:{byte_start}:{byte_len}:{kind}"` (then optionally hash for compactness).

```python
import hashlib

def stable_id(path: str, byte_start: int, byte_len: int, kind: str) -> str:
    raw = f"{path}:{byte_start}:{byte_len}:{kind}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]  # short stable key
```

#### 7.0.4 Qualified-name value encoding

`QualifiedNameProvider` returns a **set** of `QualifiedName(name, source)`; sources include import/local/builtin semantics; store as list of `{name, source}` (dedup+sorted). ([LibCST][2])

---

### 7.1 Index suite overview (minimal complete set)

Emit as independent JSONL streams (or DuckDB tables) with shared keys `{path, span, id}`:

1. **Parse manifest** (per file): parse ok + file-level formatting facts (`encoding`, `default_indent`, `default_newline`, `future_imports`, etc.).
2. **Parse errors** (per file): `ParserSyntaxError` fields (message + line/col + context).
3. **Definitions index**: `ClassDef`, `FunctionDef` (including async), optional `Lambda` (if you care). ([LibCST][3])
4. **Reference index**: variable accesses + referents from scope analysis (`ScopeProvider`). ([LibCST][6])
5. **Import index / import graph edges**: `Import` + `ImportFrom` normalized per alias, incl. relative level + star imports. ([LibCST][3])
6. **Call-site index**: `Call` nodes with callee candidates (qualified names) + arg-shape signature. ([LibCST][3])
7. **Type index** (optional): `TypeInferenceProvider` type strings for selected expr nodes (`Name|Attribute|Call` are the usual ROI). ([LibCST][7])

---

### 7.2 Parse manifest + error index (schemas + derivation)

#### 7.2.1 `parse_manifest` row (per file, success only)

Core fields:

* `path`
* `file_sha256` (raw bytes hash; used for incremental skipping)
* `encoding`, `default_indent`, `default_newline`, `has_trailing_newline`, `future_imports` (from `Module` fields)
* `parser_backend` (if you expose this operationally)

#### 7.2.2 `parse_error` row (per file, failure only)

Core fields:

* `path`, `file_sha256`
* `message`, `raw_line`, `raw_column`, `editor_line`, `editor_column`, `context`

(Exact `ParserSyntaxError` surface is already in §1.4; treat this as a canonical “error fact” stream.)

---

### 7.3 Definitions index (canonical schema + extraction recipe)

#### 7.3.1 What counts as a “definition”

* `ClassDef`: `name: Name`, `bases: Sequence[Arg]`, `keywords`, `decorators`, optional `type_parameters`, docstring. ([LibCST][3])
* `FunctionDef`: `name: Name`, `params: Parameters`, `decorators`, optional `returns`, optional `asynchronous` (async def), optional `type_parameters`, docstring. ([LibCST][3])

#### 7.3.2 Definition row (recommended minimal-but-powerful)

`def_row` fields:

* Identity: `def_id`, `path`, `kind ∈ {"class","function"}`, `is_async: bool`
* Containment: `container_def_id` (enclosing class/function; else null), `module_fqn` (optional)
* Naming:

  * `name` (raw identifier; `Name.value`) ([LibCST][3])
  * `qnames[]` (QualifiedNameProvider)
  * `fqnames[]` (FullyQualifiedNameProvider; optional/expensive)
* Signature (structured, no strings unless needed):

  * `params`: normalized `Parameters` breakdown (see 7.3.3)
  * `returns_code` (optional: `Module.code_for_node(node.returns)` when present)
  * `decorators[]` (optional: store decorator callee syntactic strings)
* `span` (both range + bspan)
* `docstring` (cleaned) if present (`get_docstring(clean=True)`). ([LibCST][3])

#### 7.3.3 Parameter normalization (directly from node fields)

`Parameters` fields you should map 1:1:

* `posonly_params: Sequence[Param]`
* `posonly_ind: ParamSlash | MaybeSentinel` (presence indicates `/`)
* `params: Sequence[Param]` (positional-or-keyword)
* `star_arg: Param | ParamStar | MaybeSentinel` (`*` sentinel or `*args`)
* `kwonly_params: Sequence[Param]`
* `star_kwarg: Param | None` (`**kwargs`) ([LibCST][3])

Each `Param`:

* `name: Name`, optional `annotation`, optional `default` (and its `equal`), optional `comma`. ([LibCST][3])

**Indexer tactic:** store each param as `{name, has_annotation, has_default, default_code?}`; only store `*_code` when you need refactor previews.

#### 7.3.4 Container resolution (`container_def_id`)

Use `ParentNodeProvider` to walk up until you hit the nearest enclosing `FunctionDef` or `ClassDef` (or `Module` = no container). ([LibCST][5])

---

### 7.4 Reference index (scope-backed, not “all Name nodes”)

#### 7.4.1 Ground truth source: `ScopeProvider` (assignments + accesses)

Scope analysis stores variable `Assignment` and `Access` objects in `Scope` containers; typical iteration is:

* `scopes = set(wrapper.resolve(ScopeProvider).values())`
* then `for assignment in scope.assignments: ...`
* and `for access in scope.accesses: ...` ([LibCST][6])

Key semantics:

* `Access.node` is typically a `Name` (or `Attribute` / string literal cases); `Access.referents` is the set of assignments it may refer to. ([LibCST][8])
* Limitation: scope analysis targets **local variable names**, not arbitrary attribute names (e.g., `a.b.c = 1` records `a` as assignment, not `c`). ([LibCST][8])

#### 7.4.2 Reference row

`ref_row` fields:

* `ref_id`, `path`, `span`
* `name` (for `Name`: `node.value`; for `Attribute`: store dotted syntactic string; for string-annotation cases: store raw string)
* `role ∈ {"read","write","del"}` from `ExpressionContextProvider` (or infer from Access context when available) ([LibCST][9])
* `scope_kind` (GlobalScope/ClassScope/FunctionScope/ComprehensionScope if you choose to expose)
* `referents[]`: list of `{assignment_name, assignment_kind, assignment_span?, qnames?}`; empty list => undefined reference (common lint use-case). ([LibCST][6])

**Ambiguity encoding:** `referents` is a list; do not collapse.

---

### 7.5 Import index / import graph (node-normalized + alias-normalized)

#### 7.5.1 Node surfaces

* `Import.names: Sequence[ImportAlias]` ([LibCST][3])
* `ImportFrom` fields:

  * `module: Name|Attribute|None` (None allowed for pure relative import)
  * `names: Sequence[ImportAlias] | ImportStar`
  * `relative: Sequence[Dot]` (relative level = `len(relative)`) ([LibCST][3])
* `ImportAlias` conveniences:

  * `evaluated_name: str`
  * `evaluated_alias: Optional[str]` ([LibCST][3])

#### 7.5.2 Import-alias row (recommended)

`import_row` fields:

* `import_id`, `path`, `span` (span of alias node and/or whole statement)
* `stmt_kind ∈ {"import","from_import"}`
* `module` (string for from-import module, None allowed)
* `relative_level: int`
* `imported: str` (ImportAlias.evaluated_name)
* `asname: Optional[str]` (ImportAlias.evaluated_alias)
* `is_star: bool`
* `abs_module` (optional: if you also compute repo module/package and can resolve relative)

(You can also derive import-graph edges: `edge = (current_module_fqn -> abs_module)`.)

---

### 7.6 Call-site index (callee candidates + arg-shape)

#### 7.6.1 Node surface

* `Call.func: BaseExpression` (often `Name|Attribute`)
* `Call.args: Sequence[Arg]` ([LibCST][3])
* `Arg` fields you index for shape:

  * `keyword: Optional[Name]` (keyword arg)
  * `star: Literal["", "*", "**"]` (positional vs *args vs **kwargs) ([LibCST][3])

#### 7.6.2 Call row

`call_row` fields:

* `call_id`, `path`, `span`
* `caller_def_id` (enclosing function/method; else null)
* `callee_syntax` (optional cheap string; e.g., dotted syntactic name if `func` is `Name|Attribute`)
* `callee_qnames[]` (semantic candidates): **use `QualifiedNameProvider` on the `Call` node** (official example shows looking up qnames for a `Call` expression like `c()`). ([LibCST][2])
* `argc`, `n_kw`, `n_star`, `n_starstar` (shape summary from `Arg.keyword` and `Arg.star`) ([LibCST][3])

**Ambiguity encoding:** `callee_qnames` is a list; keep all.

---

### 7.7 Type index (optional enrichment; repo-heavy)

#### 7.7.1 Provider surface

`TypeInferenceProvider` returns inferred type annotation strings via **Pyre Query API**, requires Watchman + running pyre server; types are strings (typing annotation syntax). ([LibCST][7])
`FullRepoManager` is the orchestrator for repo IPC and explicitly documents support for `TypeInferenceProvider` and `FullyQualifiedNameProvider`. ([LibCST][10])

#### 7.7.2 Type row

`type_row` fields:

* `type_id`, `path`, `span`
* `expr_kind ∈ {"Name","Attribute","Call",...}`
* `type_str` (string)
* `provider="pyre"` (explicit)
* `confidence` (optional; if you build heuristics later)

---

### 7.8 Incremental indexing (minimal policy)

Use `file_sha256` to skip unchanged files. Recompute categories:

* **Per-file only** (no repo cache): defs/imports/calls/parse manifest/errors can be recomputed file-by-file.
* **Repo-cached**: fully-qualified names + type inference need `FullRepoManager` cache; changes to file *paths* (moves/renames) affect module/package resolution and can invalidate caches broadly (`FullyQualifiedNameProvider.gen_cache` maps paths→module/package). ([LibCST][2])

---

If you want next: I can turn §7 into a **single “reference implementation skeleton”** that emits *all* rows in one pass per file (using `resolve_many`, `ScopeProvider` iteration, and optional `FullRepoManager` caches), with the exact minimal provider set and a deterministic ordering/normalization contract for every emitted list field.

[1]: https://libcst.readthedocs.io/en/latest/_modules/libcst/_position.html?utm_source=chatgpt.com "Source code for libcst._position"
[2]: https://libcst.readthedocs.io/en/latest/_modules/libcst/metadata/name_provider.html "libcst.metadata.name_provider — LibCST  documentation"
[3]: https://libcst.readthedocs.io/en/latest/nodes.html "Nodes — LibCST  documentation"
[4]: https://libcst.readthedocs.io/en/latest/_modules/libcst/metadata/position_provider.html?utm_source=chatgpt.com "Source code for libcst.metadata.position_provider"
[5]: https://libcst.readthedocs.io/en/latest/metadata.html?utm_source=chatgpt.com "Metadata — LibCST documentation"
[6]: https://libcst.readthedocs.io/en/latest/scope_tutorial.html "Scope Analysis — LibCST  documentation"
[7]: https://libcst.readthedocs.io/en/latest/_modules/libcst/metadata/type_inference_provider.html?utm_source=chatgpt.com "Source code for libcst.metadata.type_inference_provider"
[8]: https://libcst.readthedocs.io/en/latest/metadata.html "Metadata — LibCST  documentation"
[9]: https://libcst.readthedocs.io/en/latest/_modules/libcst/metadata/expression_context_provider.html?utm_source=chatgpt.com "Source code for libcst.metadata.expression_context_provider"
[10]: https://libcst.readthedocs.io/en/latest/_modules/libcst/metadata/full_repo_manager.html?utm_source=chatgpt.com "Source code for libcst.metadata.full_repo_manager"


## 8) Performance, correctness, and drift resistance (indexer ops)

### Mental model (ops invariants)

* **CST fidelity is “free” only if you preserve the IO boundary correctly**: bytes-first parse, byte-span anchoring, and treating `Module.code_for_node()` as *presentation* not *evidence*.
* **Metadata is deterministic only if node identity is disciplined**: analyze `wrapper.module`, never persist CST nodes, and prefer span-keyed stable IDs. `MetadataWrapper` deep-copies by default to enforce unique node identity.
* **Repo-wide semantic providers are expensive by design** (IPC, cross-file resolution); isolate them behind explicit feature flags + pre-fork cache resolution.
* **Python multiprocessing defaults are drifting** (macOS spawn default; POSIX forkserver default since 3.14), so “it worked on fork” is not an ops plan.

---

### 8.1 Correctness & fidelity contracts (what you assert, what you merely observe)

#### 8.1.1 Parse fidelity contract (bytes-first)

* **Primary invariant:** `raw_bytes -> parse_module(raw_bytes) -> module.bytes` should be byte-identical *for the overwhelming majority of valid files*; treat any mismatch as “fidelity anomaly” and persist `{path, sha256(raw), sha256(rendered), diff}` for diagnosis, not silent acceptance. (`Module.bytes` is `code.encode(encoding)`.)
* **Never use `parse_expression/parse_statement` outputs as canonical evidence slices**; those node types cannot store arbitrary leading/trailing trivia. Evidence slices come from stored byte spans against the original bytes.

Minimal “fidelity sentinel”:

```python
import hashlib, libcst as cst

def sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def fidelity_sentinel(raw: bytes) -> tuple[bool, str, str]:
    m = cst.parse_module(raw)
    out = m.bytes
    return (out == raw, sha256(raw), sha256(out))
```

(If you see mismatches in your corpus, keep indexing but mark `parse_fidelity=false` in the manifest.)

#### 8.1.2 Metadata identity contract

* **Hard rule:** if you construct `MetadataWrapper(module)` and then traverse `module` (original), metadata lookups will miss because wrapper nodes differ; traverse **`wrapper.module`** or `wrapper.visit(visitor)`. Wrapper copying exists to prevent duplicate node identities.
* `unsafe_skip_copy=True` is a performance lever, not a default: safe only when you know there are no duplicate node identities and you won’t reuse nodes across wrappers.

#### 8.1.3 Span contract (stable IDs, evidence, and patch safety)

* Store **both**:

  * `PositionProvider` / `WhitespaceInclusivePositionProvider` for human-centric ranges (debugging, UI).
  * `ByteSpanPositionProvider` for canonical evidence slicing (UTF-8 safe, multiline stable).
* Treat `CodeRange.end` as exclusive; line 1-indexed, col 0-indexed; do not re-interpret.

---

### 8.2 Determinism (ordering, hashing, and “same repo → same rows”)

#### 8.2.1 Deterministic row ordering

* For every emitted list field (qualified names, referents, imports, etc.): **sort + dedup** with a documented comparator (`(name, source)` for qnames). Qualified names can be multi-valued; you must not collapse.
* Deterministic file traversal: de-dup and sort paths before execution; LibCST’s parallel codemod runner does this as an anti-drift pattern.

#### 8.2.2 Stable IDs

* Key on `(path, byte_start, byte_len, kind)`; then hash for compactness. Byte spans are canonical because they slice raw evidence without re-tokenization.

#### 8.2.3 “Same parse config” determinism

* Persist file-level parse config in the manifest (`encoding/default_indent/default_newline/has_trailing_newline/future_imports`). These originate from `Module` and influence code generation and template parsing.

---

### 8.3 Performance levers (what actually moves the needle)

#### 8.3.1 Avoid repeated traversals

* Prefer **one wrapper + one traversal** that emits multiple index products:

  * `resolve_many({providers})` when you need provider maps.
  * `visit_batched([...])` for multiple batchable visitors in a single walk.
* Avoid “N passes for N indexes” unless N is tiny or passes are trivially pruned.

#### 8.3.2 Choose matchers vs visitors by data shape

* Matchers (`findall/extractall`) are ideal when:

  * you want a small subset of nodes (calls, imports, defs) without state;
  * you can capture subnodes with `SaveMatchedNode`.
* Visitors are ideal when:

  * you need parent/stack state, cross-node accumulation, or you already depend on metadata via `METADATA_DEPENDENCIES`.

#### 8.3.3 Gate expensive providers

* `FullyQualifiedNameProvider` and `TypeInferenceProvider` are repo-wide and mediated by `FullRepoManager`; do not run them unless the downstream product actually needs them.
* Treat type inference as optional enrichment (ops dependencies: Pyre Query API + Watchman + server).

#### 8.3.4 IO strategy (throughput + fidelity)

* Prefer `read_bytes()` + `parse_module(bytes)` to minimize lossy encode/decode churn and to keep byte spans meaningful. `parse_module` supports bytes input; `Module.bytes` exists for bytes output.

---

### 8.4 Concurrency (safe under spawn/forkserver; fast under fork; deterministic everywhere)

#### 8.4.1 Start-method reality (plan for it)

* macOS default is **spawn** (since 3.8); POSIX default is **forkserver** (since 3.14). Don’t assume fork semantics.
* Under spawn/forkserver: worker entrypoints/args must be picklable; use `if __name__ == "__main__":` and keep worker functions top-level.

#### 8.4.2 Canonical LibCST pattern: warm parser + pre-resolve repo caches

LibCST’s own parallel runner:

* warms the parser pre-pool (`parse_module("", config=PartialParserConfig(...))`);
* constructs `FullRepoManager(...); resolve_cache()` before forking/spawning;
* then runs per-file work in `ProcessPoolExecutor`.

Minimal “ops-grade” topology:

* Parent: build file list, sort/dedup, build `FullRepoManager`, `resolve_cache()`.
* Parent: extract per-path cache shards (`get_cache_for_path`) and ship only those + `(repo_root, rel_path)` to workers. `get_cache_for_path` requires `path ∈ paths`.

---

### 8.5 Drift resistance (prevent silent semantic changes)

#### 8.5.1 Version + backend fingerprints

Persist per run:

* `libcst_version` (package version)
* `python_runtime_version`
* `parser_mode/backend` (if you manage env toggles; at minimum track whether native extension is available as a boolean)
* `use_pyproject_toml` and repo root resolution settings (affects module/package mapping and FQNs)

Why: supported syntax and repo-root resolution can change across versions; you need a forensic trail.

#### 8.5.2 “Golden” corpus tests (schema + semantics)

Maintain a small fixture set that hits:

* relative imports + `__init__.py` package mapping (module/package ID drift)
* conditional imports (qname ambiguity)
* comprehensions (scope edge cases)
* encoding cookies + non-UTF8 bytes (bytes-first fidelity)
  For each fixture: assert stable row sets (after deterministic sorting) + stable IDs + non-empty spans.

#### 8.5.3 Contract tests for “no regression in fundamentals”

* **Span sanity:** every emitted entity must have byte span present and within file length.
* **Row determinism:** running twice produces identical JSONL (byte-for-byte) after sorting.
* **Ambiguity preservation:** qname/reference/call candidate lists are sets and never coerced to scalar.
* **Wrapper discipline:** assert you never emit node identities (only IDs/spans/strings).

---

### 8.6 Observability (you can’t optimize what you can’t attribute)

Emit per run (and optionally per stage/file):

* timings: parse time, metadata resolve time, index extraction time, serialization time
* counts: defs/imports/calls/refs rows, parse errors, “fidelity anomalies”
* provider usage: which providers resolved (esp. repo-wide)
* concurrency: start method, workers, chunking policy

Keep a lightweight “perf row” stream:

```json
{"path":"pkg/a.py","parse_ms":3.1,"meta_ms":7.8,"emit_ms":1.2,"rows":421,"ok":true}
```

Then you can rank hotspots without re-running profilers.

---

### 8.7 Benchmark harness (repeatable, representative, cheap)

* Use a fixed corpus + pinned versions.
* Run with single-thread (baseline) and your production concurrency settings.
* Track p50/p95 file latency + total throughput and memory envelope.
* If you use repo-wide providers, benchmark with and without `resolve_cache()` pre-pool to ensure you’re amortizing correctly.

---

### 8.8 Practical “ops checklist” (compress to muscle memory)

* [ ] Read bytes; parse bytes; store manifest (encoding/newline/indent/future_imports).
* [ ] Use `MetadataWrapper`; analyze `wrapper.module`; never persist CST nodes.
* [ ] Store both `CodeRange` + `CodeSpan`; slice evidence from raw bytes by `CodeSpan`.
* [ ] Sort/dedup file list + all emitted candidate lists; encode ambiguity as lists.
* [ ] Gate repo-wide providers; if enabled: build `FullRepoManager`, `resolve_cache()` before pool, ship per-path cache shards.
* [ ] Assume spawn/forkserver; keep worker entrypoints picklable + guarded.
* [ ] Golden fixtures + contract tests + perf rows; persist version/backend fingerprints.
