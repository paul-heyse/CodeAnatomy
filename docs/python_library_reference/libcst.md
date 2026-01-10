Below is a **deep, practitioner‑level overview of LibCST**—what it is, how its major subsystems work (parsing, visitors/transformers, matchers, metadata, codemods, codegen), and **how to exploit those capabilities to power a hosted MCP (Model Context Protocol) service that offers structural search over a Python repository**.

---

## 1) What LibCST is (and why you would choose it)

**LibCST** parses Python source **into a lossless, concrete syntax tree (CST)** that preserves every character—**comments, whitespace, parentheses, quote styles, trailing commas**, etc.—while offering an **AST‑like** node API for ergonomic analysis/rewriting. You can round‑trip: parse → transform → regenerate code, without losing formatting. Current releases support Python **3.0 through 3.14** grammar, ship prebuilt wheels, and include a native parser extension. ([PyPI][1])

---

## 2) Install & runtime model

```bash
pip install libcst
```

* **Python requirement:** 3.9+ (project guidance). Wheels are published; if a wheel isn’t available for your platform, LibCST builds a native extension (Rust/PyO3) during install. ([GitHub][2])
* **Native parser:** exposed as `libcst.native` and used by default—this is how new Python grammar features remain fast and compatible. ([Docs.rs][3])

---

## 3) Parsing, trees, and code generation

* **Parse** whole modules/expressions/statements:
  `cst.parse_module(src)`, `cst.parse_expression(code)`, `cst.parse_statement(code)`. Use the granular entry points when synthesizing nodes you plan to insert. ([LibCST][4])
* **Trees are immutable.** Nodes are frozen dataclasses; **transform by copying** using `.with_changes(...)`, `deep_replace(...)`, or `with_deep_changes(...)`. ([LibCST][5])
* **Visitors/Transformers:**

  * `CSTVisitor` (read‑only), `CSTTransformer` (read/write) with `visit_*/leave_*` hooks; you return the updated node from `leave_*`. Best‑practices: **return the `updated_node`** unless you explicitly want to discard child edits. ([LibCST][6])
* **Codegen:** Regenerate source with `Module.code` (string) after edits—simple and lossless. ([LibCST][6])

> *Why immutability matters for tooling:* it makes transforms **predictable and thread‑safe to reason about**; you compose changes functionally and always know which version you’re holding. ([LibCST][7])

---

## 4) Matchers: structural patterns that read like the grammar

LibCST’s **matcher DSL** lets you express shapes (“find a `Call` whose callee is a `Name('open')` and whose arguments include a `keyword='encoding'`”) declaratively, instead of hand‑navigating child lists. You can use **functions** (`m.matches(node, pattern)`), **combinators** (`AllOf`, `OneOf`, `Unless`), and **decorators** (`@m.visit`, `@m.leave`, `@m.call_if_inside`) on visitor methods. ([LibCST][8])

Example, find calls to `open()` without an `encoding=` kwarg:

```python
import libcst as cst, libcst.matchers as m

is_open_call_missing_encoding = m.Call(
    func=m.Name("open"),
    args=~m.Contains(m.Arg(keyword=m.Name("encoding")))
)
```

This matcher can be used **purely for search** (collect matches) or inside a Transformer to fix issues (e.g., add `encoding="utf-8"`). See the “Working with Matchers” tutorial for more decorator variants like `@m.call_if_inside`. ([LibCST][9])

---

## 5) Metadata: semantics on top of syntax (for search/indexing)

LibCST ships a **metadata framework**. You wrap your tree in a `MetadataWrapper`, request providers, and use `get_metadata`/`resolve` to access facts for nodes. Providers run **lazily** and may be **batched** for speed. Key providers: ([LibCST][10])

* **Position providers**: `PositionProvider` (line/col), `ByteSpanPositionProvider` (byte spans), `WhitespaceInclusivePositionProvider` (counts leading/trailing whitespace)—useful for rendering search hits, diffs and precise slices. ([LibCST][7])
* **ParentNodeProvider:** get a node’s parent—handy when you want “surrounding context” in search results. ([LibCST][7])
* **ExpressionContextProvider:** `Load` vs `Store` semantics for `Name`, `Attribute`, etc.—lets you filter “read” vs “write” references in search. ([LibCST][7])
* **ScopeProvider:** computes scope trees + assignments/accesses; use it to find **definitions, references, and reachability** without re‑implementing Python’s scoping rules. ([LibCST][10])
* **QualifiedNameProvider:** resolve **possible qualified names** for a symbol (handles imports, PEP‑3155 semantics). Useful when mapping a `Call` to a likely callee. ([LibCST][11])
* **FullyQualifiedNameProvider:** like the above, but **absolute** (module‑qualified) names; great for cross‑repo indexing where relative imports must be normalized. ([LibCST][10])
* **FilePathProvider:** emits the **absolute file path** of the current module (requires full‑repo context). ([LibCST][7])
* **TypeInferenceProvider:** bridges to **Pyre**’s query API to attach inferred types to `Name`/`Attribute`/`Call` nodes (requires Watchman + a Pyre server). ([LibCST][12])

### Full‑repo metadata (critical for hosted services)

Use `metadata.FullRepoManager(root, paths, providers, use_pyproject_toml=False)` to compute **repository‑wide caches** for providers that need whole‑project context (e.g., `FullyQualifiedNameProvider`, `TypeInferenceProvider`). You can then ask for a `MetadataWrapper` per file via `get_metadata_wrapper_for_path(...)`. The manager understands module roots (optionally via **pyproject.toml**) and coordinates with Pyre/Watchman as needed. ([LibCST][13])

---

## 6) Codemods: optional, but great for “search → preview → apply”

LibCST’s **codemod framework** gives you a CLI‑ready transformer base with context (filename, repo root), utilities (e.g., `AddImportsVisitor`, `RemoveUnusedImports`), and a **runner** that can walk a large repo. Use `python -m libcst.tool codemod ...` to execute your codemod class over files; or wire the same API into your own service. You can even **skip** files programmatically by raising `SkipFile`. ([LibCST][14])

---

## 7) Putting LibCST to work for **search** (patterns you’ll reuse)

1. **Textual → structural:** Use matchers to replace fragile regex with grammar‑aware queries (e.g., “find calls to `requests.get` with a timeout literal < 1.0”). ([LibCST][8])
2. **Symbols / defs / refs:** Combine `ScopeProvider` (assignments/accesses) + `QualifiedNameProvider` to list definitions and all reference sites, respecting imports and aliasing. ([LibCST][10])
3. **FQNs at scale:** Use `FullyQualifiedNameProvider` (via `FullRepoManager`) to normalize names across packages and nested packages. ([LibCST][7])
4. **Precise locations:** Attach `PositionProvider`/`ByteSpanPositionProvider` to produce **clickable ranges** and stable byte spans for rendering diffs/snippets. ([LibCST][7])
5. **Types (optional):** If you run Pyre, `TypeInferenceProvider` enables type‑aware search like “find all calls where argument `x` has type `Path`”. ([LibCST][12])

---

## 8) Architecting a **hosted MCP service** for repo search with LibCST

> **MCP recap.** The Model Context Protocol standardizes how LLM apps discover and call your server‑exposed **tools** and access read‑only **resources**. Tools do work; resources provide contextual data (often addressable via URIs). Clients (e.g., ChatGPT, Claude) can connect to your server and list/invoke tools, or browse resources. Hosted MCP integrations can also proxy the full round‑trip so the model invokes your server directly. ([Model Context Protocol][15])

### 8.1 Capabilities to expose

**Tools** (MCP tools are named ops with JSON schemas): ([Model Context Protocol][16])

* `repo.search.structural` — takes a **matcher spec** (your JSON DSL → compiled to `libcst.matchers`), optional filters (path globs, include tests, max results), and returns a list of hits with `{uri, start, end, preview}`. Use providers for positions and parents to craft high‑signal snippets. ([LibCST][8])
* `repo.search.symbols` — query by **(fully) qualified name**; returns definitions and references (grouped by kind). Backed by `ScopeProvider` + `QualifiedNameProvider` and, for cross‑package correctness, `FullyQualifiedNameProvider`. ([LibCST][10])
* `repo.search.calls` — “find all callers of FQN `pkg.mod.Class.method`”; derive by scanning `Call` nodes and mapping callee QNs. (Optionally enrich with Pyre types.) ([LibCST][11])
* `repo.search.text` — fallback plain or regex search to catch unparsable files or failing edge cases.

**Resources** (enumerate and fetch read‑only data, each with a URI): ([Model Context Protocol][17])

* `resource://repo/symbol-index` — a **materialized symbol table** (JSON or SQLite) keyed by FQN; includes decl kind, file URI, byte/line spans, docstring (see `get_docstring()` helpers on Module/ClassDef/FunctionDef). ([LibCST][18])
* `resource://repo/import-graph` — import edges for navigation/impact analysis.
* `resource://repo/call-graph` — optional (coarse, static) call graph built from `Call` + QN mapping.

> If you’re using **OpenAI’s hosted MCP tool integration** from the Agents SDK, you can register your remote MCP server label and let the model discover/invoke these tools without callbacks in your process. ([OpenAI GitHub][19])

### 8.2 Service pipeline (indexing + query path)

**Indexing (cold start & incremental):**

1. **Discover files** (git ls-files or glob).
2. For each `.py`:

   * Parse with `cst.parse_module`. Wrap in `MetadataWrapper`. Request **batched providers**: `PositionProvider`, `ScopeProvider`, `QualifiedNameProvider`. For cross‑module naming, drive `FullyQualifiedNameProvider` via **`FullRepoManager`** (and pass `use_pyproject_toml=True` if your package roots depend on it). ([LibCST][4])
   * Extract **definitions** (`FunctionDef`, `ClassDef`, assignments in module scope), **imports**, **docstrings**, **calls**. Persist as rows `{fqn, kind, file_uri, byte_span, line_span, parent_fqn, docstring|None}`. `ByteSpanPositionProvider` is ideal for stable slicing; also store line/columns for display. ([LibCST][7])
3. **Cache per‑file metadata** (LibCST and your own) and build lightweight inverted indexes (by identifier token, by FQN segments).

**Serving queries:**

* **Structural search:** compile the incoming JSON DSL to a matcher (e.g., `{"Call": {"func": {"Name": "open"}, "args": {"without_kw": "encoding"}}}` → the matcher in §4). Traverse files that match your path filters; **short‑circuit** using token prefilters (e.g., only visit files containing the identifier). Attach **parent and position** metadata to craft “smart snippets” with the function/class header around the hit. ([LibCST][8])
* **Symbol search:** hit your symbol table by exact or fuzzy FQN; **join** to references using `ScopeProvider`’s accesses and `QualifiedNameProvider` on `Name`/`Attribute` nodes (or precomputed tables). ([LibCST][10])
* **Return URIs** the client can `fetch` (e.g., `repo://pkg/mod.py#L120-L132`) to comply with MCP’s **resource** semantics for browsing. ([Model Context Protocol][17])

### 8.3 Schemas (sketch)

* **Tool: `repo.search.structural`**

  * **Input:** `{ "pattern": <your-DSL>, "paths": ["src/**.py"], "limit": 200 }`
  * **Output:** `[{ "uri": "repo://src/mod.py", "start": {"line":..., "col":...}, "end": {...}, "preview": "..." , "context": {"enclosing": "def foo(...):"} }, ...]`
* **Resource: `resource://repo/symbol-index`**

  * **Body:** lines or rows of `{ "fqn": "...", "kind": "function|class|var", "uri": "...", "span": {"start":..., "length":...} }`

(Use MCP’s **Tools**/**Resources** specs for exact shapes and metadata fields.) ([Model Context Protocol][16])

### 8.4 Scaling & quality

* **Batch providers** (prefer `BatchableMetadataProvider`) and precompute FullRepo caches; LibCST’s metadata design supports lazy + batched runs for speed. ([LibCST][10])
* **Partial failures:** if a file can’t parse (syntax error), fall back to **text/regex** and mark results as “syntactic only”.
* **Type‑aware search:** gate behind a feature flag; **Pyre** must be running for `TypeInferenceProvider`, orchestrated via `FullRepoManager`. ([LibCST][12])
* **Security guardrails:** enforce per‑search time/row limits; only expose file paths under allowed roots; redact secrets in previews.

---

## 9) Core APIs you’ll reach for (cheat sheet)

| Area      | Key types / calls                                                                                                                                                 | Why you care                                                                  |
| --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| Parse     | `parse_module`, `parse_expression`, `parse_statement`                                                                                                             | Build CSTs from code; synthesize nodes for insertion. ([LibCST][4])           |
| Transform | `CSTTransformer`, `with_changes`, `with_deep_changes`, `deep_replace`                                                                                             | Immutably rewrite trees; safest way to fix code. ([LibCST][5])                |
| Match     | `libcst.matchers` (`matches`, `AllOf`, `Unless`, decorators)                                                                                                      | Declarative shape queries for search/refactors. ([LibCST][8])                 |
| Metadata  | `MetadataWrapper`, providers (`Position*`, `ScopeProvider`, `QualifiedNameProvider`, `FullyQualifiedNameProvider`, `ParentNodeProvider`, `TypeInferenceProvider`) | Turn syntax into semantics: defs/refs, FQNs, positions, types. ([LibCST][10]) |
| Full repo | `FullRepoManager(...).get_metadata_wrapper_for_path()`                                                                                                            | Compute provider caches across a repo; normalize module roots. ([LibCST][13]) |
| Codegen   | `Module.code`                                                                                                                                                     | High‑fidelity round‑trip regeneration. ([LibCST][6])                          |
| Codemods  | `python -m libcst.tool codemod`, `CodemodCommand`, `SkipFile`, visitors like `AddImportsVisitor`, `RemoveUnusedImports`                                           | CLI/automation to run transforms at repo scale. ([LibCST][14])                |

---

## 10) Design patterns that work well in an MCP “repo search” server

1. **Index once, then answer fast.** Build a **symbol/ref index** (FQN → [defs, refs]) using `ScopeProvider` + `QualifiedNameProvider`; persist byte spans for O(1) snippet slicing. Expose the index as an MCP **resource** for fast client paging. ([LibCST][10])
2. **“Structured grep” via matchers.** Accept a small JSON (or S‑expr) matcher DSL; compile to `libcst.matchers` and run **only on candidate files** (token prefilter) to keep latency low. ([LibCST][8])
3. **FQN normalization across packages.** Use `FullRepoManager` + `FullyQualifiedNameProvider` (optionally `use_pyproject_toml=True`) so `from .a import b as c` normalizes to the same absolute symbol as `pkg.a.b`. ([LibCST][13])
4. **High‑signal results.** Attach `ParentNodeProvider` and `PositionProvider` to show the **enclosing def/class** and precise ranges in previews. ([LibCST][7])
5. **Optional type filters.** When enabled, add Pyre‑backed filters (argument/return types) via `TypeInferenceProvider`. Make it best‑effort, with fallbacks when Pyre isn’t ready. ([LibCST][12])
6. **Hosted MCP integration.** If you’re exposing this to ChatGPT/Agents, register your server as a **hosted MCP tool** so the model lists and calls your tools server‑side; shape your tool schemas to MCP’s **Tools** spec and publish your indexes as **Resources**. ([OpenAI GitHub][19])

---

## 11) Gotchas & practical notes

* **Immutability is strict.** Don’t mutate nodes; always use `.with_changes(...)` and return the **updated** instance from `leave_*`. This avoids subtle “lost edit” bugs. ([LibCST][20])
* **Positions:** Prefer **byte spans** for exact slicing, **line/col** for UI. Choose `ByteSpanPositionProvider` vs `PositionProvider` based on your renderer. ([LibCST][7])
* **Module/code roots:** If your repo uses non‑standard layout, pass `use_pyproject_toml=True` to `FullRepoManager` so FQNs are computed from your declared package roots. ([LibCST][13])
* **Types require infra.** `TypeInferenceProvider` depends on a running **Pyre** server + **Watchman**; plan devops accordingly (and cache aggressively). ([LibCST][12])
* **CLI parity:** Everything you can do via `libcst.tool codemod` you can also do programmatically in your server; the CLI is helpful for local testing and gold‑set validation. ([LibCST][14])

---

## 12) Minimal end‑to‑end sketch (index → search)

```python
# Index (offline)
from pathlib import Path
import libcst as cst
from libcst.metadata import (MetadataWrapper, PositionProvider,
    ScopeProvider, QualifiedNameProvider, FullyQualifiedNameProvider,
    FullRepoManager, ByteSpanPositionProvider)

root = Path("/repo")
files = [str(p) for p in root.rglob("**/*.py")]

mgr = FullRepoManager(
    repo_root_dir=root,
    paths=files,
    providers={FullyQualifiedNameProvider},   # add TypeInferenceProvider if needed
    use_pyproject_toml=True
)

def index_one(path: str):
    wrapper = mgr.get_metadata_wrapper_for_path(path)  # MetadataWrapper
    module = wrapper.module
    pos = wrapper.resolve(PositionProvider)
    spans = wrapper.resolve(ByteSpanPositionProvider)
    qn = wrapper.resolve(QualifiedNameProvider)
    fqn = wrapper.resolve(FullyQualifiedNameProvider)

    # Walk with matchers or a Visitor to collect defs/refs/calls/imports;
    # store (fqn, kind, file_uri, byte_span, line_span, parent_fqn, docstring)
```

That’s the same plumbing you’ll call from an MCP **tool handler** when you need to re‑index a file or materialize a **resource** like `resource://repo/symbol-index`. ([LibCST][13])

---

## 13) Where to read deeper / keep handy

* **Docs home & tutorials** (parsing, visitors/transformers, codegen): parsing functions and `Module.code`. ([LibCST][4])
* **Matchers**: guide + decorators. ([LibCST][8])
* **Metadata**: providers list, usage, repo‑wide manager, FQNs, positions. ([LibCST][10])
* **Scope analysis** tutorial (unused imports/undefined refs). ([LibCST][21])
* **Codemods**: CLI and framework. ([LibCST][14])
* **MCP**: spec and hosted tools background (tools/resources). ([Model Context Protocol][15])

---

### Final takeaways for an MCP search service

* **Lead with LibCST matchers + metadata.** You’ll get highly precise, grammar‑aware search that returns **semantically rich** results (defs/refs/types), not just text lines. ([LibCST][8])
* **Normalize names with FQNs.** That’s the key to **repo‑wide** correctness across packages and relative imports. ([LibCST][7])
* **Package it as MCP tools + resources.** Expose structural search as tools, serve symbol/call/import indexes as resources; consider OpenAI’s **hosted MCP** integration for zero‑callback invocation. ([Model Context Protocol][16])

If you want, I can produce a **concrete JSON schema** for the `repo.search.structural` tool and a reference implementation (indexer + MCP server skeleton) you can drop into your infrastructure.

[1]: https://pypi.org/project/libcst/?utm_source=chatgpt.com "libcst"
[2]: https://github.com/Instagram/LibCST?utm_source=chatgpt.com "Instagram/LibCST: A concrete syntax tree parser ..."
[3]: https://docs.rs/crate/libcst/latest?utm_source=chatgpt.com "libcst 1.8.5"
[4]: https://libcst.readthedocs.io/en/latest/parser.html?utm_source=chatgpt.com "Parsing — LibCST documentation - Read the Docs"
[5]: https://libcst.readthedocs.io/en/latest/nodes.html?utm_source=chatgpt.com "Nodes — LibCST documentation"
[6]: https://libcst.readthedocs.io/en/latest/tutorial.html?utm_source=chatgpt.com "Parsing and Visiting — LibCST documentation - Read the Docs"
[7]: https://libcst.readthedocs.io/_/downloads/en/latest/pdf/?utm_source=chatgpt.com "LibCST Documentation"
[8]: https://libcst.readthedocs.io/en/latest/matchers.html?utm_source=chatgpt.com "Matchers — LibCST documentation - Read the Docs"
[9]: https://libcst.readthedocs.io/en/latest/matchers_tutorial.html?utm_source=chatgpt.com "Working with Matchers — LibCST documentation"
[10]: https://libcst.readthedocs.io/en/latest/metadata.html?utm_source=chatgpt.com "Metadata — LibCST documentation"
[11]: https://libcst.readthedocs.io/en/latest/_modules/libcst/metadata/name_provider.html?utm_source=chatgpt.com "Source code for libcst.metadata.name_provider"
[12]: https://libcst.readthedocs.io/en/latest/_modules/libcst/metadata/type_inference_provider.html?utm_source=chatgpt.com "Source code for libcst.metadata.type_inference_provider"
[13]: https://libcst.readthedocs.io/en/latest/_modules/libcst/metadata/full_repo_manager.html?utm_source=chatgpt.com "Source code for libcst.metadata.full_repo_manager"
[14]: https://libcst.readthedocs.io/en/latest/codemods_tutorial.html?utm_source=chatgpt.com "Working With Codemods — LibCST documentation"
[15]: https://modelcontextprotocol.io/?utm_source=chatgpt.com "Model Context Protocol"
[16]: https://modelcontextprotocol.io/specification/2025-06-18/server/tools?utm_source=chatgpt.com "Tools"
[17]: https://modelcontextprotocol.io/specification/2025-06-18/server/resources?utm_source=chatgpt.com "Resources"
[18]: https://libcst.readthedocs.io/en/latest/genindex.html?utm_source=chatgpt.com "Index — LibCST documentation"
[19]: https://openai.github.io/openai-agents-python/mcp/?utm_source=chatgpt.com "Model context protocol (MCP) - OpenAI Agents SDK"
[20]: https://libcst.readthedocs.io/en/latest/best_practices.html?utm_source=chatgpt.com "Best Practices — LibCST documentation - Read the Docs"
[21]: https://libcst.readthedocs.io/en/latest/scope_tutorial.html?utm_source=chatgpt.com "Scope Analysis — LibCST documentation - Read the Docs"
