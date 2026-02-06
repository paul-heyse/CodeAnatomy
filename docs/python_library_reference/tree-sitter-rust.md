According to a document from **April 1, 2025**, `tree-sitter-rust` **0.24.0** is published as a Python package (Python ≥3.9) and is the canonical “Rust grammar bundle” you’ll consume from Python. ([PyPI][1])

# B) Rust grammar “artifact surfaces” you can exploit from Python

This chapter is about **treating the Rust grammar as a *versioned data bundle***, not just a `Language` object: you’ll harvest **(1) a manifest (`tree-sitter.json`)**, **(2) a node schema (`node-types.json`)**, and **(3) query packs (`*.scm`)** and compile them into reproducible, lintable “query pack artifacts” for your own indexer.

---

## B.0 Artifact inventory (what exists in the Rust grammar, concretely)

### B.0.1 `tree-sitter.json` is the grammar manifest (what files exist + where)

The Rust grammar’s `tree-sitter.json` explicitly declares:

* grammar name: `"rust"`, scope `"source.rust"`, file-types `["rs"]`
* query pack paths:

  * `"highlights": ["queries/highlights.scm"]`
  * `"injections": ["queries/injections.scm"]`
  * `"tags": ["queries/tags.scm"]`
* `injection-regex: "rust"`
* `metadata.version: "0.24.0"`
* `bindings.python: true` ([Docs.rs][2])

**Actionable implication:** you can treat `tree-sitter.json` as the *single source of truth* for **which query packs exist**, their **paths**, and a **semantic version** you should stamp onto every downstream artifact.

### B.0.2 `src/node-types.json` is the generated CST schema

Tree-sitter generates `node-types.json` as **structured data about every possible syntax node** in a grammar, including each node’s `"type"`, `"named"` flag, possible `"fields"`, `"children"`, and (optionally) `"subtypes"` for supertypes. ([tree-sitter.github.io][3])

In the Rust grammar bundle, this file lives at `src/node-types.json`. ([Docs.rs][4])

### B.0.3 Query packs are the “standard” `.scm` files

Tree-sitter’s CLI/doc model expects query packs in a `queries/` folder, and highlights/injections/locals are explicitly described as the standard pack types. ([tree-sitter.github.io][5])

For Rust 0.24.0, the `queries/` directory contains `highlights.scm`, `injections.scm`, and `tags.scm`. ([Docs.rs][6])

---

## B.1 How to *extract* these artifacts in Python (robustly)

### B.1.1 Install

```bash
python -m pip install tree-sitter tree-sitter-rust
```

(That gets you the Python bindings plus the Rust grammar wheel.) ([PyPI][7])

### B.1.2 Locate files shipped in the installed distribution

Do **not** assume the runtime module layout — instead, enumerate distribution files and pick out the bundle artifacts by suffix:

```python
from __future__ import annotations

from importlib.metadata import distribution, version
from pathlib import Path

DIST = "tree-sitter-rust"

dist = distribution(DIST)
print("pkg_version:", version(DIST))

artifact_paths: list[Path] = []
for f in dist.files or []:
    s = str(f)
    if s.endswith(("tree-sitter.json", "node-types.json")) or s.endswith(".scm"):
        artifact_paths.append(dist.locate_file(f))

for p in sorted(artifact_paths):
    print(p)
```

**Operational contract you want:**

* If `tree-sitter.json` and `queries/*.scm` are present: treat them as **authoritative pack sources**.
* If the wheel ships only the compiled parser: you **vendor** the missing artifacts from the grammar repo at the *same version* (and record the provenance + hash).
  The manifest itself tells you which files should exist for Rust (highlights/injections/tags). ([Docs.rs][2])

---

## B.2 `tree-sitter.json` deep dive (use it as your “bundle manifest”)

Tree-sitter docs define `tree-sitter.json` as the CLI-facing language configuration, including language detection keys (`file-types`, `first-line-regex`, etc.) and query pack paths (`highlights`, `locals`, `injections`). ([tree-sitter.github.io][5])

For Rust specifically, the manifest lines you should treat as “indexer inputs” are:

* `file-types: ["rs"]` → map `*.rs` to this grammar
* `injection-regex: "rust"` → injection sites that ask for “rust” can bind to this grammar
* `highlights/injections/tags` arrays → *these packs exist and are intended to be loaded* ([Docs.rs][2])

### B.2.1 Parse into a stable in-memory spec

```python
from __future__ import annotations
from dataclasses import dataclass
import json
from pathlib import Path

@dataclass(frozen=True)
class GrammarEntry:
    name: str
    scope: str
    file_types: tuple[str, ...]
    injection_regex: str | None
    highlights: tuple[str, ...]
    injections: tuple[str, ...]
    tags: tuple[str, ...]
    version: str

def load_tree_sitter_manifest(tree_sitter_json: Path) -> GrammarEntry:
    obj = json.loads(tree_sitter_json.read_text(encoding="utf8"))
    g = obj["grammars"][0]  # rust bundle is single-grammar
    meta = obj.get("metadata", {})
    return GrammarEntry(
        name=g["name"],
        scope=g["scope"],
        file_types=tuple(g.get("file-types", [])),
        injection_regex=g.get("injection-regex"),
        highlights=tuple(g.get("highlights", [])),
        injections=tuple(g.get("injections", [])),
        tags=tuple(g.get("tags", [])),
        version=str(meta.get("version", "")),
    )
```

### B.2.2 Use the manifest to drive loading (no hardcoded filenames)

* **Never** hardcode `queries/highlights.scm` etc in your indexer.
* Always load the query paths from the manifest, because grammars can add packs (e.g., `locals.scm`) or split packs later. ([tree-sitter.github.io][5])

---

## B.3 `node-types.json` deep dive (turn it into a “grammar schema object”)

Tree-sitter’s spec for `node-types.json` is explicit and machine-friendly:

* Each node type is uniquely identified by `(type, named)`
* `fields` and `children` describe valid child-node sets with `(required, multiple, types[])`
* `subtypes` exist for supertypes when the grammar declares them as supertypes ([tree-sitter.github.io][3])

### B.3.1 Build the minimal schema indices you’ll reuse everywhere

You already have general node-types loading in your `tree-sitter_advanced` doc; here’s the Rust-specific operationalization:

* Build these in memory *once per grammar version*:

  * `named_kinds`: set of node kinds where `named=True`
  * `anon_kinds`: tokens where `named=False` (punctuation/operators)
  * `fields[kind][field] -> ChildSpec`
  * `children[kind] -> ChildSpec|None`
  * `supertypes[super_kind] -> tuple[sub_kind...]`

```python
from __future__ import annotations
from dataclasses import dataclass
from typing import Any

NodeTypeId = tuple[str, bool]  # (type, named)

@dataclass(frozen=True)
class ChildSpec:
    required: bool
    multiple: bool
    types: tuple[NodeTypeId, ...]

@dataclass(frozen=True)
class NodeSpec:
    id: NodeTypeId
    fields: dict[str, ChildSpec]
    children: ChildSpec | None
    subtypes: tuple[NodeTypeId, ...] | None  # present for supertypes

def _childspec(o: dict[str, Any]) -> ChildSpec:
    return ChildSpec(
        required=bool(o.get("required", False)),
        multiple=bool(o.get("multiple", False)),
        types=tuple((t["type"], bool(t["named"])) for t in o.get("types", [])),
    )

def load_node_types(node_types_json_path) -> dict[NodeTypeId, NodeSpec]:
    import json
    arr = json.loads(node_types_json_path.read_text(encoding="utf8"))
    out: dict[NodeTypeId, NodeSpec] = {}
    for e in arr:
        nid: NodeTypeId = (e["type"], bool(e["named"]))
        out[nid] = NodeSpec(
            id=nid,
            fields={k: _childspec(v) for k, v in e.get("fields", {}).items()},
            children=_childspec(e["children"]) if "children" in e else None,
            subtypes=tuple((t["type"], bool(t["named"])) for t in e.get("subtypes", [])) or None,
        )
    return out
```

### B.3.2 What you *do* with this schema (Rust indexer posture)

1. **Query-pack linting (fast fail)**
   Before runtime indexing, validate that every node type and every field referenced in your `.scm` packs actually exists in the schema. Tree-sitter explicitly positions `node-types.json` as the complete node universe. ([tree-sitter.github.io][3])

2. **Typed facades / preventing “field hallucination”**
   Even in Python, generating typed wrappers from this schema has massive agent leverage: “fields are attributes”, unions are explicit, and supertypes become protocols/unions.

3. **Drift detection**
   If `node-types.json` changes between Rust grammar versions, you treat it like a schema migration (regen wrappers, re-lint packs, diff capture contracts).

---

## B.4 Rust query packs as *declarative specs* (compile → introspect → persist)

Tree-sitter’s highlighting system describes queries as first-class configuration: `tree-sitter.json` declares query file paths and the query files define behavior. ([tree-sitter.github.io][5])

### B.4.1 Compile queries into an IR (and then interrogate the IR)

In py-tree-sitter, a `Query` is compiled via `Query(language, source)` and exposes introspection APIs:

* capture table: `capture_count`, `capture_name(i)`
* per-pattern slicing: `start_byte_for_pattern(i)`, `end_byte_for_pattern(i)`
* property metadata: `pattern_settings(i)` for `#set!`, and `pattern_assertions(i)` for `#is?/#is-not?`
* perf gates: `is_pattern_rooted(i)`, `is_pattern_non_local(i)` ([tree-sitter.github.io][8])

This is exactly what you want for “query packs as data”: compile once, extract a contract, store it.

### B.4.2 The Rust-provided packs (and why they’re useful *beyond* editors)

#### (1) Highlights query = “structural tokenizer”

The Rust `HIGHLIGHTS_QUERY` includes captures like:

* `(type_identifier) @type`, `(primitive_type) @type.builtin`, field identifiers as `@property`
* call sites as `@function` / `@function.method`
* macros as `@function.macro`
* keyword tokens captured as `@keyword`
* and uses predicates like `#match?` to classify identifiers by naming convention ([Docs.rs][9])

**Indexer repurpose:** treat captures as a *stable token taxonomy* you can project into:

* “semantic token streams” (byte-span + capture name)
* coarse retrieval anchors (“show me all `@function` and `@type` spans in this file”)

#### (2) Injections query = “embedded parse planner”

Tree-sitter defines injections using:

* captures: `@injection.content`, `@injection.language`
* plus properties like `injection.language`, `injection.combined`, `injection.include-children` ([tree-sitter.github.io][5])

Rust’s injection pack is small but important: it treats macro token trees as Rust again:

```text
(macro_invocation (token_tree) @injection.content)
(#set! injection.language "rust")
(#set! injection.include-children)
```

…and similarly for `macro_rule`. ([Docs.rs][10])

**Indexer repurpose:** this is a *declarative mapping* that says “this subtree should be reparsed as Rust”. For Rust, it’s a direct way to build a **macro-body extraction pipeline** (even though Tree-sitter doesn’t expand macros semantically, you can still structurally parse the token tree).

#### (3) Tags query = “definition/reference event stream”

Rust’s `TAGS_QUERY` is already shaped like an indexing spec:

* Definitions:

  * `struct_item` / `enum_item` / `union_item` / `type_item` → `@definition.class`
  * `function_item` → `@definition.function`
  * trait → `@definition.interface`
  * `mod_item` → `@definition.module`
  * macro definitions → `@definition.macro`
* References:

  * call expressions and macro invocations → `@reference.call`
  * impls → `@reference.implementation` ([Docs.rs][11])

**Indexer repurpose:** you can convert tags matches into a normalized event table:

* `event_kind` = one of `definition.class`, `definition.function`, `reference.call`, …
* `name` = captured `@name` node text
* `span_def` = byte span of the definition/reference node capture (e.g., `definition.function`)
* `span_name` = byte span of the `@name` capture

---

## B.5 Concrete Python scaffolds (load packs → compile → emit versioned artifacts)

### B.5.1 Load Rust grammar (Python)

```python
from tree_sitter import Language, Parser
import tree_sitter_rust as tsrust

RUST = Language(tsrust.language())
parser = Parser(RUST)
```

### B.5.2 Compile + introspect a query pack into a “contract artifact”

```python
from __future__ import annotations
from dataclasses import dataclass
import hashlib
from tree_sitter import Query, QueryError

@dataclass(frozen=True)
class QueryPackContract:
    name: str
    source_sha256: str
    capture_names: tuple[str, ...]
    pattern_count: int
    patterns: tuple[dict, ...]  # small dict per pattern: offsets, settings, perf flags

def compile_pack(lang, name: str, source: str) -> QueryPackContract:
    try:
        q = Query(lang, source)
    except QueryError as e:
        raise ValueError(f"[{name}] query failed to compile: {e}") from e

    src_hash = hashlib.sha256(source.encode("utf8")).hexdigest()
    caps = tuple(q.capture_name(i) for i in range(q.capture_count))

    patterns: list[dict] = []
    for i in range(q.pattern_count):
        patterns.append({
            "i": i,
            "start": q.start_byte_for_pattern(i),
            "end": q.end_byte_for_pattern(i),
            "rooted": q.is_pattern_rooted(i),
            "non_local": q.is_pattern_non_local(i),
            "settings": q.pattern_settings(i),     # from #set!
            "assertions": q.pattern_assertions(i), # from #is?/#is-not?
        })

    return QueryPackContract(
        name=name,
        source_sha256=src_hash,
        capture_names=caps,
        pattern_count=q.pattern_count,
        patterns=tuple(patterns),
    )
```

This uses the exact introspection surface documented for `Query` (pattern slicing + settings/assertions). ([tree-sitter.github.io][8])

### B.5.3 Run the Rust tags query and emit symbol events

```python
from __future__ import annotations
from tree_sitter import QueryCursor

def node_text(src: bytes, n) -> str:
    return src[n.start_byte:n.end_byte].decode("utf8", errors="replace")

def run_tags(src: bytes, tree, q_tags):
    cur = QueryCursor(q_tags)
    for pattern_i, caps in cur.matches(tree.root_node):
        # caps: dict[capture_name -> Node]
        if "name" not in caps:
            continue

        name = node_text(src, caps["name"])

        # definition.* / reference.* captures are “event kinds”
        event_kind = next((k for k in caps.keys() if k.startswith("definition.") or k.startswith("reference.")), None)
        if not event_kind:
            continue

        target = caps[event_kind]
        yield {
            "kind": event_kind,
            "name": name,
            "span_target": (target.start_byte, target.end_byte),
            "span_name": (caps["name"].start_byte, caps["name"].end_byte),
            "pattern_i": pattern_i,
        }
```

The semantic structure (definition/reference taxonomy) is directly present in the Rust `TAGS_QUERY`. ([Docs.rs][11])

### B.5.4 Interpret injection packs via `pattern_settings`

You *must not* treat injections as “magic”: they’re just query matches plus metadata.

```python
from tree_sitter import QueryCursor

def run_injections(src: bytes, tree, q_inj):
    cur = QueryCursor(q_inj)
    for pattern_i, caps in cur.matches(tree.root_node):
        settings = q_inj.pattern_settings(pattern_i)  # reads #set! properties :contentReference[oaicite:21]{index=21}
        content = caps.get("injection.content")
        if not content:
            continue

        yield {
            "language": settings.get("injection.language"),
            "include_children": "injection.include-children" in settings,
            "span_content": (content.start_byte, content.end_byte),
            "pattern_i": pattern_i,
        }
```

Rust’s injection query explicitly sets `injection.language` to `"rust"` and `injection.include-children`, so a simple slice of `span_content` is consistent with the intent of the pack. ([Docs.rs][10])

---

## B.6 “Versioned query pack artifacts” (what you should persist)

Your *minimum viable* persisted bundle for Rust should include:

* `grammar_version` (from `tree-sitter.json` metadata or Python package version)
* `tree-sitter.json` (verbatim + sha256)
* `src/node-types.json` (verbatim + sha256)
* each query pack source (verbatim + sha256)
* compiled **QueryPackContract** (capture list + per-pattern metadata: rooted/non-local/settings/assertions)

This turns “a bunch of `.scm` files” into a deterministic, diffable, CI-gateable surface.

**Recommended on-disk layout (repo artifacting):**

```
artifacts/tree_sitter/rust/0.24.0/
  tree-sitter.json
  node-types.json
  queries/
    highlights.scm
    injections.scm
    tags.scm
  contracts/
    highlights.contract.json
    injections.contract.json
    tags.contract.json
  manifest.json
```

The reason this works is that Tree-sitter itself defines these as the canonical inputs for highlighting/injection behavior (manifest + query packs), and it defines `node-types.json` as the complete node schema you can treat like a typed interface. ([tree-sitter.github.io][5])

[1]: https://pypi.org/project/tree-sitter-rust/ "tree-sitter-rust · PyPI"
[2]: https://docs.rs/crate/tree-sitter-rust/latest/source/tree-sitter.json "tree-sitter-rust 0.24.0 - Docs.rs"
[3]: https://tree-sitter.github.io/tree-sitter/using-parsers/6-static-node-types "Static Node Types - Tree-sitter"
[4]: https://docs.rs/crate/tree-sitter-rust/latest/source/src/ "tree-sitter-rust 0.24.0 - Docs.rs"
[5]: https://tree-sitter.github.io/tree-sitter/3-syntax-highlighting.html "Syntax Highlighting - Tree-sitter"
[6]: https://docs.rs/crate/tree-sitter-rust/latest/source/queries/ "tree-sitter-rust 0.24.0 - Docs.rs"
[7]: https://pypi.org/project/tree-sitter/?utm_source=chatgpt.com "Python Tree-sitter"
[8]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Query.html "Query — py-tree-sitter 0.25.2 documentation"
[9]: https://docs.rs/tree-sitter-rust/latest/tree_sitter_rust/constant.HIGHLIGHTS_QUERY.html "HIGHLIGHTS_QUERY in tree_sitter_rust - Rust"
[10]: https://docs.rs/tree-sitter-rust/latest/tree_sitter_rust/constant.INJECTIONS_QUERY.html "INJECTIONS_QUERY in tree_sitter_rust - Rust"
[11]: https://docs.rs/tree-sitter-rust/latest/tree_sitter_rust/constant.TAGS_QUERY.html "TAGS_QUERY in tree_sitter_rust - Rust"

# C) Node-kind taxonomy for Rust (the “semantic slices” you’ll index)

Rust’s Tree-sitter grammar has **hundreds of node kinds** (e.g., third-party generators report **280+ node types** for `tree-sitter-rust` v0.24.0), so “agent-usable” means: **(1) start from the grammar’s own `node-types.json`**, **(2) leverage its supertypes (`_expression`, `_type`, `_pattern`, etc.)**, and **(3) project raw kinds into a smaller, stable semantic taxonomy + query pack contract**. ([Docs.rs][1])

The Rust grammar bundle exposes `NODE_TYPES` (the content of `node-types.json`) and the query packs (`HIGHLIGHTS_QUERY`, `INJECTIONS_QUERY`, `TAGS_QUERY`) in the upstream `tree_sitter_rust` crate (useful as an authoritative reference for what “counts” as the Rust CST). ([Docs.rs][2])

---

## C.0 The “don’t guess node kinds” rule: taxonomy must be derived from `node-types.json`

### C.0.1 Your primary primitives: Rust grammar supertypes

The Rust grammar uses **supertypes** to define “unions” of related kinds, which is exactly what you want for taxonomy:

* `_declaration_statement` is a supertype whose `subtypes` include `function_item`, `struct_item`, `enum_item`, `trait_item`, `impl_item`, `mod_item`, `type_item`, `const_item`, `static_item`, `macro_definition`, `use_declaration`, etc. ([Docs.rs][3])
* `_expression` enumerates all expression-ish kinds like `call_expression`, `field_expression`, `index_expression`, `closure_expression`, `match_expression`, `if_expression`, `for_expression`, `while_expression`, `async_block`, `await_expression`, etc. ([Docs.rs][3])
* `_pattern` enumerates binding/destructuring patterns (`struct_pattern`, `tuple_struct_pattern`, `range_pattern`, `mut_pattern`, `ref_pattern`, `or_pattern`, …). ([Docs.rs][3])
* `_type` enumerates type syntax families (`reference_type`, `pointer_type`, `generic_type`, `function_type`, `dynamic_type`, `abstract_type`, …). ([Docs.rs][3])
* `_literal` enumerates literal families (`char_literal`, `string_literal`, `raw_string_literal`, `integer_literal`, …). ([Docs.rs][3])

**Indexer implication:** your “semantic slices” should be built *from these supertypes first*, then refined.

---

## C.1 Build a “kind map” programmatically (and make it a versioned artifact)

### C.1.1 Extract supertypes + subtypes (this becomes your canonical kind registry)

`node-types.json` is a list of objects. **Supertypes** are the entries with a `subtypes` array (e.g., `_expression`, `_type`, `_pattern`). ([Docs.rs][3])

Representative Python skeleton:

```python
import json
from pathlib import Path
from collections import defaultdict

def load_node_types(path: Path) -> list[dict]:
    return json.loads(path.read_text(encoding="utf-8"))

def build_supertypes_index(node_types: list[dict]):
    # supertype -> [(subtype_name, named_bool), ...]
    supertypes: dict[str, list[tuple[str, bool]]] = {}
    # subtype -> [supertype, ...]  (some subtypes can appear in multiple supertypes)
    subtype_to_supertypes: dict[tuple[str, bool], list[str]] = defaultdict(list)

    for entry in node_types:
        t = entry["type"]
        if "subtypes" in entry:
            subs = [(s["type"], bool(s["named"])) for s in entry["subtypes"]]
            supertypes[t] = subs
            for st in subs:
                subtype_to_supertypes[st].append(t)

    return supertypes, subtype_to_supertypes
```

**Persist as artifacts** (per grammar version):

* `node-types.json` (verbatim)
* `kind_registry.json` (computed: supertypes + per-kind fields/children signatures)
* `taxonomy.yaml` (your semantic projection)

This makes taxonomy drift a **diffable migration** instead of “something broke”.

---

## C.2 Top-level items (declaration statements) — the symbol-defining surface

### C.2.1 “What counts as a top-level item” in Rust Tree-sitter

Rust’s `_declaration_statement` supertype explicitly enumerates the “statement-level” declaration forms you’ll see as siblings under `source_file` and inside `block`. ([Docs.rs][3])

Also note the parse roots:

* `source_file` is the root node; its children include `_declaration_statement` and `expression_statement`. ([Docs.rs][3])
* `block` children can include `_declaration_statement`, `_expression`, and `expression_statement`. ([Docs.rs][3])

### C.2.2 Kind map: core symbol definitions (with field signatures you should index)

#### `function_item`

Fields:

* `name: identifier | metavariable` (required)
* `parameters: parameters` (required)
* `body: block` (required)
* `return_type: _type` (optional)
* `type_parameters: type_parameters` (optional)
  Children may include `visibility_modifier`, `function_modifiers`, and `where_clause`. ([Docs.rs][3])

**Definition query (minimal, stable):**

```scm
(function_item
  name: (identifier) @def.fn.name
  parameters: (parameters) @def.fn.params
  body: (block) @def.fn.body
  (where_clause)? @def.fn.where
) @def.fn
```

#### `struct_item`

Fields:

* `name: type_identifier` (required)
* `body: field_declaration_list | ordered_field_declaration_list` (optional)
* `type_parameters: type_parameters` (optional)
  Children can include `visibility_modifier` and `where_clause`. ([Docs.rs][3])

**Definition queries (cover both body shapes):**

```scm
(struct_item name: (type_identifier) @def.struct.name
            body: (field_declaration_list) @def.struct.body) @def.struct
(struct_item name: (type_identifier) @def.struct.name
            body: (ordered_field_declaration_list) @def.struct.body) @def.struct
(struct_item name: (type_identifier) @def.struct.name) @def.struct
```

#### `enum_item`

Fields:

* `name: type_identifier` (required)
* `body: enum_variant_list` (required)
* `type_parameters: type_parameters` (optional)
  Children can include `visibility_modifier` and `where_clause`. ([Docs.rs][3])

```scm
(enum_item
  name: (type_identifier) @def.enum.name
  body: (enum_variant_list) @def.enum.variants
  (where_clause)? @def.enum.where
) @def.enum
```

#### `union_item`

Fields:

* `name: type_identifier` (required)
* `body: field_declaration_list` (required)
* `type_parameters: type_parameters` (optional)
  Children can include `visibility_modifier` and `where_clause`. ([Docs.rs][3])

```scm
(union_item
  name: (type_identifier) @def.union.name
  body: (field_declaration_list) @def.union.fields
) @def.union
```

#### `trait_item`

Fields:

* `name: type_identifier` (required)
* `body: declaration_list` (required)
* `bounds: trait_bounds` (optional)
* `type_parameters: type_parameters` (optional)
  Children can include `visibility_modifier` and `where_clause`. ([Docs.rs][3])

```scm
(trait_item
  name: (type_identifier) @def.trait.name
  bounds: (trait_bounds)? @def.trait.bounds
  body: (declaration_list) @def.trait.body
) @def.trait
```

#### `impl_item`

Fields:

* `type: _type` (required)
* `trait: generic_type | scoped_type_identifier | type_identifier` (optional)
* `body: declaration_list` (optional)
* `type_parameters: type_parameters` (optional)
  Child may include `where_clause`. ([Docs.rs][3])

```scm
(impl_item
  trait: (_) ? @impl.trait
  type: (_type) @impl.for_type
  body: (declaration_list)? @impl.body
  (where_clause)? @impl.where
) @impl
```

#### `mod_item`

Fields:

* `name: identifier` (required)
* `body: declaration_list` (optional)
  Child may include `visibility_modifier`. ([Docs.rs][3])

```scm
(mod_item name: (identifier) @def.mod.name
         body: (declaration_list)? @def.mod.body) @def.mod
```

#### `type_item` (type alias)

Fields:

* `name: type_identifier` (required)
* `type: _type` (required)
* `type_parameters: type_parameters` (optional) ([Docs.rs][3])

```scm
(type_item name: (type_identifier) @def.type_alias.name
          type: (_type) @def.type_alias.rhs) @def.type_alias
```

#### `const_item` / `static_item`

`const_item` fields:

* `name: identifier` (required)
* `type: _type` (required)
* `value: _expression` (optional) ([Docs.rs][3])

`static_item` fields:

* `name: identifier` (required)
* `type: _type` (required)
* `value: _expression` (optional)
  Children can include `mutable_specifier` and `visibility_modifier`. ([Docs.rs][3])

```scm
(const_item name: (identifier) @def.const.name
           type: (_type) @def.const.type
           value: (_expression)? @def.const.value) @def.const

(static_item name: (identifier) @def.static.name
            type: (_type) @def.static.type
            value: (_expression)? @def.static.value) @def.static
```

#### `macro_definition` / `macro_invocation`

`macro_definition`:

* field `name: identifier` (required), children include `macro_rule`. ([Docs.rs][3])

`macro_invocation`:

* field `macro: identifier | scoped_identifier` (required)
* child `token_tree` (required) ([Docs.rs][3])

```scm
(macro_definition name: (identifier) @def.macro.name) @def.macro

(macro_invocation macro: (_) @call.macro.name
                 (token_tree) @call.macro.args) @call.macro
```

#### `use_declaration` (import graph surface)

Field `argument` can be: `scoped_use_list`, `use_list`, `use_wildcard`, `use_as_clause`, `scoped_identifier`, `crate/self/super`, etc; child may include `visibility_modifier`. ([Docs.rs][3])

```scm
(use_declaration argument: (_) @use.arg) @use
(use_as_clause path: (_) @use.path alias: (identifier) @use.alias) @use.as
(scoped_use_list path: (_) @use.base list: (use_list) @use.list) @use.scoped_list
```

---

## C.3 Names & paths — build a stable “path segment” model

### C.3.1 `scoped_identifier` and `scoped_type_identifier` are your “qualified path” nodes

`scoped_identifier` has:

* `name` (required): `identifier | super`
* `path` (optional): can chain through `identifier`, `scoped_identifier`, `crate/self/super`, and can include type-ish nodes like `generic_type` / `bracketed_type`. ([Docs.rs][3])

`scoped_type_identifier` is the type-name analogue:

* `name: type_identifier`
* `path` chain similar to above. ([Docs.rs][3])

**Indexer rule:** represent “qualified names” as:

* `segments: [{kind, text, span}]`
* `root_kind`: one of `crate|self|super|identifier|type_identifier`
* `raw_node_kind`: `scoped_identifier | scoped_type_identifier | identifier | type_identifier`

### C.3.2 Query patterns for “path extraction anchors”

You typically want both the **whole path node** and the **leaf name**:

```scm
(scoped_identifier
  path: (_) @path.base
  name: (_) @path.leaf
) @path

(scoped_type_identifier
  path: (_) @type_path.base
  name: (type_identifier) @type_path.leaf
) @type_path
```

Then flatten in Python by following `path:` until absent.

---

## C.4 Type syntax — index `_type` families, not ad-hoc strings

### C.4.1 `_type` supertype gives you the full type-kind universe

Rust `_type` subtypes include: `reference_type`, `pointer_type`, `generic_type`, `function_type`, `dynamic_type`, `abstract_type`, `bounded_type`, `tuple_type`, `array_type`, `primitive_type`, `never_type`, `unit_type`, etc. ([Docs.rs][3])

### C.4.2 High-ROI type nodes + field/child signatures

#### `reference_type` (`&T`, `&'a mut T`)

* field `type: _type` (required)
* children can include `lifetime` and `mutable_specifier`. ([Docs.rs][3])

```scm
(reference_type
  (lifetime)? @type.ref.lifetime
  (mutable_specifier)? @type.ref.mut
  type: (_type) @type.ref.inner
) @type.ref
```

#### `pointer_type` (`*const T`, `*mut T`)

* field `type: _type` (required)
* child `mutable_specifier` optional (grammar represents mutability as a node). ([Docs.rs][3])

#### `generic_type` (`Foo<T>`)

* fields:

  * `type: identifier | scoped_identifier | scoped_type_identifier | type_identifier` (required)
  * `type_arguments: type_arguments` (required) ([Docs.rs][3])

#### `type_arguments` payload model

`type_arguments` children can include `_type`, `lifetime`, `trait_bounds`, `type_binding`, and even `_literal`/`block` (const generics / inline blocks). ([Docs.rs][3])

#### `trait_bounds` / `higher_ranked_trait_bound`

* `trait_bounds` children include `_type`, `higher_ranked_trait_bound`, `lifetime`. ([Docs.rs][3])
* `higher_ranked_trait_bound` fields include `type` and `type_parameters`. ([Docs.rs][3])

#### `where_clause` / `where_predicate`

`where_clause` children include `where_predicate`, and `where_predicate` binds:

* `bounds: trait_bounds` (required)
* `left`: many possible type nodes (`generic_type`, `reference_type`, `type_identifier`, etc). ([Docs.rs][3])

```scm
(where_predicate
  left: (_) @where.left
  bounds: (trait_bounds) @where.bounds
) @where.pred
```

---

## C.5 Expression syntax — index `_expression` families and normalize “calls”

### C.5.1 `_expression` supertype enumerates what you should treat as an “expression event”

The grammar lists `call_expression`, `field_expression`, `index_expression`, `closure_expression`, `match_expression`, `if_expression`, `for_expression`, `while_expression`, `await_expression`, etc. ([Docs.rs][3])

### C.5.2 Calls: `call_expression` + (method call = `field_expression` as callee)

`call_expression` fields:

* `function` (required)
* `arguments: arguments` (required) ([Docs.rs][3])

`field_expression` fields:

* `value: _expression` (required)
* `field: field_identifier | integer_literal` (required) ([Docs.rs][3])

**Normalize into 3 call classes**:

1. **Free function call**: `call_expression` where `function` is `identifier | scoped_identifier | generic_function`
2. **Method call**: `call_expression` where `function` is `field_expression` (receiver+method)
3. **Macro call**: `macro_invocation` separately (don’t pretend it’s a `call_expression`) ([Docs.rs][3])

Queries:

```scm
;; 1) Call sites (capture both callee node and arguments)
(call_expression
  function: (_) @call.callee
  arguments: (arguments) @call.args
) @call

;; 2) Method calls (callee is field_expression)
(call_expression
  function: (field_expression
              value: (_) @call.recv
              field: (field_identifier) @call.method)
  arguments: (arguments) @call.args
) @call.method_call
```

### C.5.3 Closures

`closure_expression` fields:

* `parameters: closure_parameters` (required)
* `body: _ | _expression` (required)
* `return_type: _type` (optional) ([Docs.rs][3])

```scm
(closure_expression
  parameters: (closure_parameters) @closure.params
  body: (_) @closure.body
  return_type: (_type)? @closure.ret
) @closure
```

### C.5.4 Match expressions (patterns + guards + arms)

`match_expression` fields: `value` and `body: match_block`.
`match_block` children: `match_arm`.
`match_arm` fields: `pattern: match_pattern`, `value: _expression`.
`match_pattern` has optional `condition` and contains `_pattern` as child. ([Docs.rs][3])

```scm
(match_arm
  pattern: (match_pattern
             ( _ ) @match.guard?   ;; keep loose; guard is optional and can be expr/let_chain
             (_) @match.pat_node)  ;; the child pattern node
  value: (_expression) @match.arm.value
) @match.arm
```

(If you need precise “guard” extraction, use the fact that `match_pattern.condition` is optional and can be `_expression | let_chain | let_condition`.) ([Docs.rs][3])

---

## C.6 Pattern syntax — destructuring, bindings, guards

### C.6.1 `_pattern` subtype universe (what you should expect)

Patterns include: `identifier`, `scoped_identifier`, `struct_pattern`, `tuple_struct_pattern`, `tuple_pattern`, `slice_pattern`, `range_pattern`, `mut_pattern`, `ref_pattern`, `reference_pattern`, `or_pattern`, etc. ([Docs.rs][3])

### C.6.2 High-ROI pattern nodes

#### `struct_pattern`

Field `type: scoped_type_identifier | type_identifier` (required), children include `field_pattern` and `remaining_field_pattern`. ([Docs.rs][3])

#### `field_pattern`

Fields:

* `name: field_identifier | shorthand_field_identifier` (required)
* `pattern: _pattern` (optional)
  Children can include `mutable_specifier`. ([Docs.rs][3])

#### `range_pattern`

Fields include `left` (optional) with many admissible node kinds (`_literal_pattern`, `identifier`, `crate`, etc). ([Docs.rs][3])

**Match-arm pattern query (pragmatic):**

```scm
(match_arm pattern: (match_pattern (_) @pat) value: (_) @val) @arm
```

Then **normalize** in Python by checking `@pat.kind` (e.g., `struct_pattern`, `tuple_struct_pattern`, `identifier`, …).

---

## C.7 Attributes & doc comments — attach “leading trivia” deterministically

### C.7.1 Attribute nodes (outer vs inner)

* `attribute` has optional `arguments: token_tree` and optional `value: _expression`, and its children are the attribute path (`crate|identifier|scoped_identifier|self|super|metavariable`). ([Docs.rs][3])
* `attribute_item` is a wrapper whose child is a single `attribute`. ([Docs.rs][3])
* `inner_attribute_item` also wraps an `attribute` as a child. ([Docs.rs][3])

**Attach algorithm (works for both `source_file` and `block`):**

1. Walk children in order.
2. Accumulate contiguous `attribute_item` + doc-comments into a `pending` buffer.
3. When you hit a symbol-defining node kind (`function_item`, `struct_item`, …), attach `pending` to that node (by spans), then clear.
   This is valid because both `source_file` and `block` include declaration nodes as ordered children. ([Docs.rs][3])

### C.7.2 Doc comments live on comment nodes, not on items

Both `line_comment` and `block_comment` expose:

* `doc: doc_comment` (optional)
* markers `inner_doc_comment_marker` / `outer_doc_comment_marker` (optional) ([Docs.rs][3])

**Operational rule:** treat doc comments as **preceding trivia** to attach (same buffer mechanism as attributes).

---

## C.8 String/char literal families — and the byte-string reality

### C.8.1 Literal type universe (per grammar)

`_literal` includes: `char_literal`, `string_literal`, `raw_string_literal`, `integer_literal`, `float_literal`, `boolean_literal`. ([Docs.rs][3])

### C.8.2 `string_literal` vs `raw_string_literal` payload nodes

* `string_literal` children can include `escape_sequence` and `string_content`. ([Docs.rs][3])
* `raw_string_literal` contains `string_content` (no escape nodes). ([Docs.rs][3])

**Extraction queries (for injection + indexing):**

```scm
(string_literal (string_content) @lit.string.content)
(string_literal (escape_sequence) @lit.string.escape)
(raw_string_literal (string_content) @lit.raw_string.content)
(char_literal) @lit.char
```

### C.8.3 Byte strings / byte chars are *not* separate node kinds here

There are **no `byte_string*` / `byte_char*` node types** in `tree-sitter-rust` v0.24.0 `node-types.json`. ([Docs.rs][3])

**Practical implication:** classify byte literals at the **lexeme layer** (look at source bytes around the literal span for prefixes like `b"…"`, `br"…"`, `b'…'`), rather than expecting dedicated CST node kinds.

---

## C.9 What you should standardize as the “agent-usable taxonomy” (minimal contract)

A good minimal contract is:

* **Definitions**: `def.fn`, `def.struct`, `def.enum`, `def.union`, `def.trait`, `def.mod`, `def.type_alias`, `def.const`, `def.static`, `def.macro`
* **Calls**: `call` (free calls), `call.method_call`, `call.macro`
* **Types**: `type.ref`, `type.ptr`, `type.generic`, `type.fn`, `type.bounds`, `type.where`
* **Patterns**: `pat` (root), plus `pat.struct`, `pat.tuple_struct`, `pat.range`, `pat.or`, `pat.bind`
* **Trivia attachment**: `attr`, `doc`

Everything above is directly grounded in the Rust grammar’s own node-kind + field signatures (so it remains stable across repos), and your query packs become “declarative specs” over this contract. ([Docs.rs][3])

[1]: https://docs.rs/tss-rust/latest/tree_sitter_symbols_rust/?utm_source=chatgpt.com "tree_sitter_symbols_rust - Rust"
[2]: https://docs.rs/tree-sitter-rust/0.24.0/tree_sitter_rust/ "tree_sitter_rust - Rust"
[3]: https://docs.rs/crate/tree-sitter-rust/latest/source/src/node-types.json "tree-sitter-rust 0.24.0 - Docs.rs"

# D) Rust query-pack design (production-grade conventions for captures)

This chapter is about turning `.scm` files into a **stable, typed extraction contract** for Rust—*not* “whatever the editor uses today.” Concretely: you will (1) **fork/derive** from grammar-provided packs, (2) standardize **capture naming** as your API, (3) use `#set!` as a **schema/metadata layer**, and (4) enforce **rooted + local (“windowable”)** patterns so you can re-run queries only on changed ranges. ([tree-sitter.github.io][1])

---

## D.0 The core model you’re designing against (queries are patterns + captures + metadata)

### D.0.1 Captures are your output surface area (treat them as API)

Tree-sitter queries associate names with matched nodes using **captures** written as `@capture_name` *after* the node pattern. That’s the mechanism you will use to emit `def/ref/call/type/...` events. ([tree-sitter.github.io][2])

### D.0.2 Predicates & directives are **not “magic”** — they’re structured data you interpret

Tree-sitter describes predicates (`#eq?`, `#match?`, `#any-of?`, `#is?`, …) and directives (`#set!`, `#select-adjacent!`, `#strip!`) as metadata/conditions attached to patterns. Crucially: **predicates/directives aren’t handled by the Tree-sitter C library**; they’re exposed so higher-level tooling (bindings/engines) can implement them. ([tree-sitter.github.io][1])

**Design implication:** your indexer should treat `#set!` as a **schema layer** (pattern → metadata), and treat predicates as *optional* filters that must be supported by your runtime. ([tree-sitter.github.io][1])

---

## D.1 “Index query packs” vs “editor highlight packs” (fork, don’t reuse blindly)

### D.1.1 Editor highlight packs optimize for *visual classification*, not semantic stability

Highlighting is driven by query files like `highlights.scm` (and optionally `locals.scm` / `injections.scm`) referenced by the grammar’s configuration; Tree-sitter’s highlighting doc treats these as editor-facing configuration and even provides a **special unit-test format** for highlight behavior. ([tree-sitter.github.io][3])

Highlight packs routinely:

* capture “token-ish” roles like `@keyword`, `@type`, `@function.method`
* use regex predicates (`#match?`) and name heuristics
* prioritize “looks right” over “is stable across grammar versions”

That’s fine for editors; it’s **not** a reliable downstream contract for indexing.

### D.1.2 Index query packs optimize for *data extraction contracts*

Your index packs should:

* have **small, typed capture namespaces**: `@def.* @ref.* @call.* @type.* @attr.* @macro.* @doc.*`
* emit captures that map 1:1 to your tables/Arrow schemas
* be **linted** against `node-types.json`
* be **windowable** (rooted/local) so incremental reindex works

### D.1.3 Recommended strategy: “derive” from grammar packs, but keep them separate

Use grammar-provided packs as *inputs*:

* `tags.scm` is closest to “indexing,” but it often uses multi-root patterns (doc adjacency) that can be **non-local**.
* `injections.scm` shows how the ecosystem uses `#set! injection.language` / `#set! injection.include-children`. ([Docs.rs][4])

Then create your own pack namespace, e.g.:

```
queries_rust_index/
  00_defs.scm
  10_refs.scm
  20_calls.scm
  30_types.scm
  40_attrs_docs.scm
  _shared.scm      # optional; see “include” note below
contracts/
  rust-<grammar_version>/
    00_defs.contract.json
    ...
```

> Tree-sitter query language itself doesn’t define an “include” directive; treat `_shared.scm` as a *build-time concatenation unit* (your loader concatenates sources before compilation).

### D.1.4 Debug commands you should standardize on (practical workflow)

For quick sanity checks, the Tree-sitter CLI is still the fastest “human loop” tool. It’s installable via cargo or npm. ([Docs.rs][5])

```bash
cargo install --locked tree-sitter-cli
# or: npm install tree-sitter-cli
```

Even if your production pipeline is Python, keep the CLI available for:

* `tree-sitter parse` to confirm grammar output
* `tree-sitter query` / playground tooling to iterate on `.scm` patterns
* `tree-sitter highlight` when comparing against the grammar’s highlight packs (useful to understand “what the grammar authors intended”). ([Docs.rs][5])

---

## D.2 Capture naming conventions (your stable, typed downstream contract)

### D.2.1 Rules of the road

1. **First segment = semantic channel**: `def | ref | call | type | attr | macro | doc`
2. **Second segment = semantic kind**: `fn | struct | enum | trait | impl | mod | type_alias | const | static | path | ...`
3. **Remaining segments = slots**: `name | node | callee | args | recv | method | bounds | where | ...`
4. **Every “event” pattern must capture one canonical event node**: `@def.fn` or `@call` etc. (This is the row anchor.)
5. **Private helper captures** must be visually obvious and excluded from emission: use `@_tmp.*` (only for predicates / disambiguation).

Captures are literally how Tree-sitter associates names with nodes; this is the mechanism described in the official query operator docs. ([tree-sitter.github.io][2])

### D.2.2 Minimal required capture sets per channel

These are “contract minima” that make downstream table schemas stable.

#### `@def.*` (definitions)

Required:

* `@def.<kind>` → the definition node (row anchor)
* `@def.<kind>.name` → name node (identifier/type_identifier/etc)

Optional (common):

* `.params`, `.body`, `.type_params`, `.where`, `.vis`, `.attrs`

Example (function definition):

```scm
(function_item
  name: (identifier) @def.fn.name
  parameters: (parameters) @def.fn.params
  body: (block) @def.fn.body
  (where_clause)? @def.fn.where
) @def.fn
```

#### `@ref.*` (references)

Required:

* `@ref.<kind>` → reference node (or smallest stable container)
* `@ref.<kind>.name` (or `.path`) → what you’ll later resolve

Example (type reference via scoped type path):

```scm
(scoped_type_identifier
  name: (type_identifier) @ref.type.name
) @ref.type
```

#### `@call.*` (calls)

Required:

* `@call` or `@call.method_call` → call_expression node
* `@call.callee` → callee node
* `@call.args` → arguments node

Method-call specialization:

```scm
(call_expression
  function: (field_expression
              value: (_) @call.recv
              field: (field_identifier) @call.method)
  arguments: (arguments) @call.args
) @call.method_call
```

#### `@type.*` (type syntax)

Required:

* `@type.node` → the `_type` node (anchor)
  Optional:
* `.inner`, `.bounds`, `.lifetime`, `.mut`, etc.

Example (reference type):

```scm
(reference_type
  (lifetime)? @type.ref.lifetime
  (mutable_specifier)? @type.ref.mut
  type: (_type) @type.ref.inner
) @type.node
```

#### `@attr.*` and `@doc.*` (trivia attachment)

In practice, **don’t bake adjacency into the same pattern** if you want windowability (see D.4). Prefer:

* `@attr.item` captures all `attribute_item` nodes with `.path` and `.args`
* `@doc.comment` captures doc-comment nodes + their spans
  …and then attach in a second pass by ordering/spans.

---

## D.3 Pattern settings & metadata: `#set!` as a schema layer

### D.3.1 What `#set!` is (and why it’s perfect for index-pack schemas)

Tree-sitter’s directive docs define `#set!` as a way to associate **arbitrary key/value metadata** with a pattern (commonly used for `injection.language`). ([tree-sitter.github.io][1])

In py-tree-sitter, you can read these via:

* `Query.pattern_settings(pattern_index)` → dict of properties set via `#set!`
* `Query.pattern_assertions(pattern_index)` → assertions from `#is?` / `#is-not?` ([tree-sitter.github.io][6])

### D.3.2 `#set!` supports boolean-ish keys (key without value)

Rust’s own injection query uses `(#set! injection.include-children)` with **no value**, and it’s valid. This matters because it lets you represent flags in your schema. ([Docs.rs][4])

### D.3.3 Recommended metadata keys (treat as a stable “pack schema”)

Use a reserved namespace for your indexer, e.g. `cq.*`:

* `cq.kind` (string): canonical emitted kind, e.g. `"def.fn"`, `"call.method_call"`
* `cq.emit` (string): destination stream/table, e.g. `"rust_defs"`
* `cq.name_capture` (string): which capture is the name field, e.g. `"def.fn.name"`
* `cq.windowable` (flag): pattern must be rooted + local; safe for range execution
* `cq.experimental` (flag): allowed to change without “schema semver bump”

Example:

```scm
(function_item
  name: (identifier) @def.fn.name
  parameters: (parameters) @def.fn.params
  body: (block) @def.fn.body
) @def.fn
(#set! cq.kind "def.fn")
(#set! cq.emit "rust_defs")
(#set! cq.name_capture "def.fn.name")
(#set! cq.windowable)
```

### D.3.4 Python: compile packs and harvest metadata contracts

```python
from tree_sitter import Query

def compile_and_contract(language, source: str) -> dict:
    q = Query(language, source)

    contract = {
        "capture_names": [q.capture_name(i) for i in range(q.capture_count)],
        "patterns": [],
    }
    for i in range(q.pattern_count):
        contract["patterns"].append({
            "i": i,
            "rooted": q.is_pattern_rooted(i),
            "non_local": q.is_pattern_non_local(i),
            "settings": q.pattern_settings(i),      # #set!
            "assertions": q.pattern_assertions(i),  # #is?/#is-not?
        })
    return contract
```

Why these fields matter is explicit in py-tree-sitter docs: `pattern_settings` is from `#set!`, and `is_pattern_non_local` disables range execution optimizations. ([tree-sitter.github.io][6])

### D.3.5 Footgun: directives/predicates portability

Tree-sitter explicitly notes predicates/directives aren’t handled by the C library; higher-level tooling chooses what to implement. For maximum portability in Python, stick to the predicates py-tree-sitter supports by default (`#eq?/#match?/#any-of?/#is?/#set!`). ([tree-sitter.github.io][1])

---

## D.4 Rooted/local constraints: enforcing “windowable” packs for changed-range execution

### D.4.1 What “rooted” and “non-local” mean operationally

py-tree-sitter exposes:

* `Query.is_pattern_rooted(i)` → pattern has a single root node
* `Query.is_pattern_non_local(i)` → pattern is “non-local” (multiple root nodes; can match within repeating sequences), and **non-local patterns disable certain optimizations for range execution**. ([tree-sitter.github.io][6])

**Practical rule:** any pattern you plan to run with `QueryCursor.set_byte_range` must be:

* **rooted**
* **not non-local**
* and should capture a single event node (`@def.fn`, `@call`, …)

### D.4.2 Range execution primitives you should standardize on

`QueryCursor` supports:

* `set_byte_range(start, end)` → returns matches that **intersect** the byte range
* `set_containing_byte_range(start, end)` (added in 0.26.0) → only returns matches **fully contained** in the range (critical for incremental update correctness). ([tree-sitter.github.io][7])

### D.4.3 Incremental parse → changed ranges → windowed reindex loop

The canonical incremental loop in py-tree-sitter:

1. `tree.edit(...)`
2. `new_tree = parser.parse(new_src, old_tree)`
3. `for r in old_tree.changed_ranges(new_tree): ...` ([PyPI][8])

Then you re-run windowable packs only over those ranges.

```python
from tree_sitter import QueryCursor

def reindex_changed_ranges(parser, language, packs, old_tree, new_src: bytes, edit):
    # 1) edit old tree in-place
    old_tree.edit(**edit)

    # 2) reparse incrementally
    new_tree = parser.parse(new_src, old_tree)

    # 3) compute changed ranges
    ranges = list(old_tree.changed_ranges(new_tree))

    for pack_name, query in packs.items():
        cursor = QueryCursor(query)
        for r in ranges:
            cursor.set_byte_range(r.start_byte, r.end_byte)
            # If available in your pinned py-tree-sitter:
            # cursor.set_containing_byte_range(r.start_byte, r.end_byte)

            for pattern_i, caps in cursor.matches(new_tree.root_node):
                yield pack_name, pattern_i, caps

    return new_tree
```

The behavior of `changed_ranges` and the incremental parse API is documented in the py-tree-sitter README, and range semantics are documented on `QueryCursor`. ([PyPI][8])

### D.4.4 Lint rule: “windowable” patterns must be rooted + local

You should fail CI if a windowable pattern violates constraints:

```python
def lint_windowable_patterns(query):
    for i in range(query.pattern_count):
        settings = query.pattern_settings(i)
        if "cq.windowable" not in settings:
            continue
        if not query.is_pattern_rooted(i):
            raise ValueError(f"pattern {i} is not rooted")
        if query.is_pattern_non_local(i):
            raise ValueError(f"pattern {i} is non-local (breaks range optimizations)")
```

These exact predicate/inspection APIs are part of py-tree-sitter’s `Query` interface. ([tree-sitter.github.io][6])

### D.4.5 Query-language operators you should lean on to keep patterns local

To prevent “overmatching” inside a rooted pattern:

* Use field constraints (`name: (...)`) to make patterns specific. ([tree-sitter.github.io][9])
* Use the anchor operator `.` to require immediate siblings / first child / last child, preventing combinatorial matches. ([tree-sitter.github.io][2])
* Prefer alternations `[...]` *inside* a single rooted pattern over writing multiple sibling-root patterns. ([tree-sitter.github.io][2])

---

## D.5 Production pack lifecycle: build, contract, test, and debug

### D.5.1 Build step (CI gate)

For each `.scm` pack:

1. compile `Query(language, source)` (fail fast on QueryError)
2. emit a contract JSON:

   * capture list (`capture_name(i)`)
   * per-pattern flags: `rooted`, `non_local`
   * `pattern_settings` / `pattern_assertions`
3. run lint rules:

   * capture naming convention
   * required captures per channel (`@def.*` has `.name`, etc)
   * windowability constraints for patterns flagged `cq.windowable`

py-tree-sitter provides all introspection needed (capture names, pattern count, settings/assertions, rooted/non-local checks). ([tree-sitter.github.io][6])

### D.5.2 Tests: copy the spirit of Tree-sitter highlight tests, but for index events

Tree-sitter has a built-in highlight test system (special comments in `test/highlight`) used to verify highlighting correctness. Use that *idea* as your model: write tiny Rust fixtures and assert emitted events/spans/kinds. ([tree-sitter.github.io][3])

### D.5.3 Debug loop: CLI + Python

* Install CLI once (`cargo install --locked tree-sitter-cli`) and use it to validate parsing/query assumptions quickly. ([Docs.rs][5])
* In Python, prefer `QueryCursor.matches()` over `captures()` when captures are related, since matches keep captures grouped per pattern match (this is explicitly called out in py-tree-sitter docs). ([PyPI][8])

---

If you want the natural next deliverable for this chapter: I can propose the **exact pack set** you should standardize for Rust (`00_defs/10_refs/20_calls/30_types/40_attrs_docs`) *plus* the concrete **capture contracts + `cq.*` schema keys** for each, so your downstream tables can be generated mechanically from the query pack contracts.

[1]: https://tree-sitter.github.io/tree-sitter/using-parsers/queries/3-predicates-and-directives.html "Predicates and Directives - Tree-sitter"
[2]: https://tree-sitter.github.io/tree-sitter/using-parsers/queries/2-operators.html "Operators - Tree-sitter"
[3]: https://tree-sitter.github.io/tree-sitter/3-syntax-highlighting.html "Syntax Highlighting - Tree-sitter"
[4]: https://docs.rs/tree-sitter-rust/latest/tree_sitter_rust/constant.INJECTIONS_QUERY.html?utm_source=chatgpt.com "INJECTIONS_QUERY in tree_sitter_rust - Rust"
[5]: https://docs.rs/tree-sitter-cli "tree_sitter_cli - Rust"
[6]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Query.html "Query — py-tree-sitter 0.25.2 documentation"
[7]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.QueryCursor.html "QueryCursor — py-tree-sitter 0.25.2 documentation"
[8]: https://pypi.org/project/tree-sitter/ "tree-sitter · PyPI"
[9]: https://tree-sitter.github.io/tree-sitter/using-parsers/queries/1-syntax.html "Basic Syntax - Tree-sitter"

# D.6 Standard Rust index pack set + contracts (CqQueryPack v1)

This deliverable standardizes **five Rust “index query packs”** and an **explicit `cq.*` pattern-metadata schema** so you can:

1. compile `.scm` → `Query`
2. read `Query.pattern_settings(i)` (from `#set!`)
3. mechanically generate **downstream table schemas** + per-match row extraction logic.

Tree-sitter explicitly supports attaching **arbitrary key/value metadata** to a pattern via `#set!`. ([tree-sitter.github.io][1])
py-tree-sitter exposes this as `Query.pattern_settings(i)` (properties set via `#set!`) and exposes “windowability” inspection (`is_pattern_rooted`, `is_pattern_non_local`). ([tree-sitter.github.io][2])
Range execution semantics you rely on for incremental indexing are defined on `QueryCursor.set_byte_range` / `set_containing_byte_range`. ([tree-sitter.github.io][3])

---

## D.6.0 Pack directory layout (exact set)

```
queries_rust_index/
  00_defs.scm
  10_refs.scm
  20_calls.scm
  30_types.scm
  40_attrs_docs.scm
```

**Rules:**

* **Never** put editor highlight captures (`@keyword`, `@type.builtin`, …) in these packs. Those belong to editor/query packs described in Tree-sitter’s highlighting system. ([tree-sitter.github.io][4])
* Every “emit pattern” MUST set:

  * `cq.pack`, `cq.emit`, `cq.kind`, `cq.anchor`, and `cq.windowable` (flag)
  * one or more `cq.slot.*` mappings

---

## D.6.1 `cq.*` schema keys (the contract)

Tree-sitter’s `#set!` directive is the supported way to associate metadata with a pattern. ([tree-sitter.github.io][1])
py-tree-sitter returns a **dict of properties with optional values** for `pattern_settings` (values can be absent for flag-like keys). ([tree-sitter.github.io][2])

### D.6.1.1 Required keys (per emitting pattern)

* `cq.pack` *(string)* — e.g. `"00_defs"`
* `cq.emit` *(string)* — table/stream id, e.g. `"rust_defs"`
* `cq.kind` *(string)* — semantic event kind, e.g. `"def.fn"`
* `cq.anchor` *(string)* — capture name of the anchor node, e.g. `"def.fn"`
* `cq.windowable` *(flag key)* — pattern must be rooted+local (see D.6.3)

### D.6.1.2 Slot mapping keys (per emitting pattern)

Each slot tells the runtime **which capture** populates it and **how to extract** it.

* `cq.slot.<slot>` *(string)* — capture name, e.g. `"def.fn.name"`
* `cq.slot.<slot>.extract` *(string)* — one of:

  * `span` → (bstart,bend) only
  * `text` → text only (decoded slice)
  * `span_text` → span + text
  * `kind` → node kind only
  * `span_kind` → span + kind
* `cq.slot.<slot>.normalize` *(string, optional)* — post-processing hint (your code implements it), e.g.:

  * `rust:path_segments`
  * `rust:doc_strip_markers`
  * `rust:attr_path`

### D.6.1.3 Recommended global keys (pack-level / optional)

* `cq.contract_version` *(string)* — `"v1"`
* `cq.notes` *(string)* — free text (kept short)
* `cq.deprecated` *(flag)*

---

## D.6.2 Downstream tables (emit targets) — fixed set

You want **five tables** (1:1 with packs). Schema is generated by collecting all slots used by patterns with `cq.emit=<table>`.

* `rust_defs`
* `rust_refs`
* `rust_calls`
* `rust_types`
* `rust_attrs_docs`

All rows also carry:

* `kind` (from `cq.kind`)
* `anchor_kind` (from `caps[anchor].type`)
* `anchor_bstart`, `anchor_bend` (byte span)
* `pattern_i` (pattern index in the compiled query)
* `pack` (from `cq.pack`)

This aligns with QueryCursor returning matches as `(pattern_index, capture_name → node)` tuples. ([tree-sitter.github.io][3])

---

## D.6.3 Windowability enforcement (rooted + local)

py-tree-sitter:

* defines **non-local patterns** as having multiple root nodes, and notes they disable optimizations for range execution ([tree-sitter.github.io][2])
* exposes `is_pattern_rooted(i)` and `is_pattern_non_local(i)` ([tree-sitter.github.io][2])

**Hard rule:** any pattern with `cq.windowable` must satisfy:

* `query.is_pattern_rooted(i) == True`
* `query.is_pattern_non_local(i) == False`

**Why:** you will run these patterns via `QueryCursor.set_byte_range` / `set_containing_byte_range` during incremental updates. `set_byte_range` returns matches that merely intersect the range; `set_containing_byte_range` restricts matches to those fully contained (added in 0.26.0). ([tree-sitter.github.io][3])

---

## D.6.4 Pack 00_defs.scm → `rust_defs`

### D.6.4.1 Definition events (exact `cq.kind` set)

* `def.fn`
* `def.struct`
* `def.enum`
* `def.union`
* `def.trait`
* `def.mod`
* `def.type_alias`
* `def.const`
* `def.static`
* `def.macro_def`
* `def.impl` *(container-ish, still useful for navigation; name slot may be absent)*

### D.6.4.2 Capture contract (names + required slots)

All patterns in this pack:

* Anchor capture must be `@def.*` (per kind)
* MUST set slot `name` when the construct is named
* MUST set slot `attrs_leading`? **No** — do attrs in 40 pack and attach later

#### `def.fn`

```scm
(function_item
  name: (identifier) @def.fn.name
  parameters: (parameters) @def.fn.params
  body: (block) @def.fn.body
  (where_clause)? @def.fn.where
) @def.fn
(#set! cq.contract_version "v1")
(#set! cq.pack "00_defs")
(#set! cq.emit "rust_defs")
(#set! cq.kind "def.fn")
(#set! cq.anchor "def.fn")
(#set! cq.windowable)

(#set! cq.slot.name "def.fn.name")
(#set! cq.slot.name.extract "span_text")
(#set! cq.slot.params "def.fn.params")
(#set! cq.slot.params.extract "span")
(#set! cq.slot.body "def.fn.body")
(#set! cq.slot.body.extract "span")
(#set! cq.slot.where "def.fn.where")
(#set! cq.slot.where.extract "span")
```

#### `def.struct`

```scm
(struct_item
  name: (type_identifier) @def.struct.name
  body: (_) ? @def.struct.body
  (where_clause)? @def.struct.where
) @def.struct
(#set! cq.pack "00_defs") (#set! cq.emit "rust_defs") (#set! cq.kind "def.struct")
(#set! cq.anchor "def.struct") (#set! cq.windowable)
(#set! cq.slot.name "def.struct.name") (#set! cq.slot.name.extract "span_text")
(#set! cq.slot.body "def.struct.body") (#set! cq.slot.body.extract "span")
(#set! cq.slot.where "def.struct.where") (#set! cq.slot.where.extract "span")
```

#### `def.enum`

```scm
(enum_item
  name: (type_identifier) @def.enum.name
  body: (enum_variant_list) @def.enum.variants
  (where_clause)? @def.enum.where
) @def.enum
(#set! cq.pack "00_defs") (#set! cq.emit "rust_defs") (#set! cq.kind "def.enum")
(#set! cq.anchor "def.enum") (#set! cq.windowable)
(#set! cq.slot.name "def.enum.name") (#set! cq.slot.name.extract "span_text")
(#set! cq.slot.variants "def.enum.variants") (#set! cq.slot.variants.extract "span")
(#set! cq.slot.where "def.enum.where") (#set! cq.slot.where.extract "span")
```

#### `def.union`

```scm
(union_item
  name: (type_identifier) @def.union.name
  body: (field_declaration_list) @def.union.fields
) @def.union
(#set! cq.pack "00_defs") (#set! cq.emit "rust_defs") (#set! cq.kind "def.union")
(#set! cq.anchor "def.union") (#set! cq.windowable)
(#set! cq.slot.name "def.union.name") (#set! cq.slot.name.extract "span_text")
(#set! cq.slot.fields "def.union.fields") (#set! cq.slot.fields.extract "span")
```

#### `def.trait`

```scm
(trait_item
  name: (type_identifier) @def.trait.name
  bounds: (trait_bounds)? @def.trait.bounds
  body: (declaration_list) @def.trait.body
  (where_clause)? @def.trait.where
) @def.trait
(#set! cq.pack "00_defs") (#set! cq.emit "rust_defs") (#set! cq.kind "def.trait")
(#set! cq.anchor "def.trait") (#set! cq.windowable)
(#set! cq.slot.name "def.trait.name") (#set! cq.slot.name.extract "span_text")
(#set! cq.slot.bounds "def.trait.bounds") (#set! cq.slot.bounds.extract "span")
(#set! cq.slot.body "def.trait.body") (#set! cq.slot.body.extract "span")
(#set! cq.slot.where "def.trait.where") (#set! cq.slot.where.extract "span")
```

#### `def.mod`

```scm
(mod_item
  name: (identifier) @def.mod.name
  body: (declaration_list)? @def.mod.body
) @def.mod
(#set! cq.pack "00_defs") (#set! cq.emit "rust_defs") (#set! cq.kind "def.mod")
(#set! cq.anchor "def.mod") (#set! cq.windowable)
(#set! cq.slot.name "def.mod.name") (#set! cq.slot.name.extract "span_text")
(#set! cq.slot.body "def.mod.body") (#set! cq.slot.body.extract "span")
```

#### `def.type_alias`

```scm
(type_item
  name: (type_identifier) @def.type_alias.name
  type: (_type) @def.type_alias.rhs
) @def.type_alias
(#set! cq.pack "00_defs") (#set! cq.emit "rust_defs") (#set! cq.kind "def.type_alias")
(#set! cq.anchor "def.type_alias") (#set! cq.windowable)
(#set! cq.slot.name "def.type_alias.name") (#set! cq.slot.name.extract "span_text")
(#set! cq.slot.rhs "def.type_alias.rhs") (#set! cq.slot.rhs.extract "span")
```

#### `def.const` / `def.static`

```scm
(const_item name: (identifier) @def.const.name type: (_type) @def.const.type value: (_expression)? @def.const.value) @def.const
(#set! cq.pack "00_defs") (#set! cq.emit "rust_defs") (#set! cq.kind "def.const")
(#set! cq.anchor "def.const") (#set! cq.windowable)
(#set! cq.slot.name "def.const.name") (#set! cq.slot.name.extract "span_text")
(#set! cq.slot.type "def.const.type") (#set! cq.slot.type.extract "span")
(#set! cq.slot.value "def.const.value") (#set! cq.slot.value.extract "span")

(static_item name: (identifier) @def.static.name type: (_type) @def.static.type value: (_expression)? @def.static.value) @def.static
(#set! cq.pack "00_defs") (#set! cq.emit "rust_defs") (#set! cq.kind "def.static")
(#set! cq.anchor "def.static") (#set! cq.windowable)
(#set! cq.slot.name "def.static.name") (#set! cq.slot.name.extract "span_text")
(#set! cq.slot.type "def.static.type") (#set! cq.slot.type.extract "span")
(#set! cq.slot.value "def.static.value") (#set! cq.slot.value.extract "span")
```

#### `def.macro_def`

```scm
(macro_definition
  name: (identifier) @def.macro_def.name
) @def.macro_def
(#set! cq.pack "00_defs") (#set! cq.emit "rust_defs") (#set! cq.kind "def.macro_def")
(#set! cq.anchor "def.macro_def") (#set! cq.windowable)
(#set! cq.slot.name "def.macro_def.name") (#set! cq.slot.name.extract "span_text")
```

#### `def.impl` (name is “derived”, so slot is spans only)

```scm
(impl_item
  trait: (_) ? @def.impl.trait
  type: (_type) @def.impl.for_type
  body: (declaration_list)? @def.impl.body
  (where_clause)? @def.impl.where
) @def.impl
(#set! cq.pack "00_defs") (#set! cq.emit "rust_defs") (#set! cq.kind "def.impl")
(#set! cq.anchor "def.impl") (#set! cq.windowable)
(#set! cq.slot.trait "def.impl.trait") (#set! cq.slot.trait.extract "span")
(#set! cq.slot.for_type "def.impl.for_type") (#set! cq.slot.for_type.extract "span")
(#set! cq.slot.body "def.impl.body") (#set! cq.slot.body.extract "span")
(#set! cq.slot.where "def.impl.where") (#set! cq.slot.where.extract "span")
```

---

## D.6.5 Pack 10_refs.scm → `rust_refs`

This pack emits “reference atoms” that later resolvers can interpret.

### D.6.5.1 Reference events (exact `cq.kind` set)

* `ref.path` *(value-level identifiers / scoped_identifier chains)*
* `ref.type` *(type_identifier / scoped_type_identifier / generic_type root)*
* `ref.use` *(use-declaration payload nodes)*
* `ref.macro` *(macro_invocation macro path)*
* `ref.lifetime` *(lifetime nodes)*

### D.6.5.2 Patterns (use fields to be specific)

Tree-sitter explicitly recommends using **field constraints** (e.g., `name:`) to make patterns specific. ([tree-sitter.github.io][5])

#### `ref.path` (scoped_identifier)

```scm
(scoped_identifier
  name: (_) @ref.path.leaf
  path: (_) ? @ref.path.base
) @ref.path
(#set! cq.pack "10_refs") (#set! cq.emit "rust_refs") (#set! cq.kind "ref.path")
(#set! cq.anchor "ref.path") (#set! cq.windowable)
(#set! cq.slot.leaf "ref.path.leaf") (#set! cq.slot.leaf.extract "span_text")
(#set! cq.slot.base "ref.path.base") (#set! cq.slot.base.extract "span")
(#set! cq.slot.leaf.normalize "rust:path_segments")
```

#### `ref.type` (scoped_type_identifier)

```scm
(scoped_type_identifier
  name: (type_identifier) @ref.type.leaf
  path: (_) ? @ref.type.base
) @ref.type
(#set! cq.pack "10_refs") (#set! cq.emit "rust_refs") (#set! cq.kind "ref.type")
(#set! cq.anchor "ref.type") (#set! cq.windowable)
(#set! cq.slot.leaf "ref.type.leaf") (#set! cq.slot.leaf.extract "span_text")
(#set! cq.slot.base "ref.type.base") (#set! cq.slot.base.extract "span")
(#set! cq.slot.leaf.normalize "rust:path_segments")
```

#### `ref.type` (generic_type root + args span)

```scm
(generic_type
  type: (_) @ref.type.callee
  type_arguments: (type_arguments) @ref.type.args
) @ref.type.generic
(#set! cq.pack "10_refs") (#set! cq.emit "rust_refs") (#set! cq.kind "ref.type")
(#set! cq.anchor "ref.type.generic") (#set! cq.windowable)
(#set! cq.slot.callee "ref.type.callee") (#set! cq.slot.callee.extract "span_text")
(#set! cq.slot.args "ref.type.args") (#set! cq.slot.args.extract "span")
```

#### `ref.use`

```scm
(use_declaration
  argument: (_) @ref.use.arg
) @ref.use
(#set! cq.pack "10_refs") (#set! cq.emit "rust_refs") (#set! cq.kind "ref.use")
(#set! cq.anchor "ref.use") (#set! cq.windowable)
(#set! cq.slot.arg "ref.use.arg") (#set! cq.slot.arg.extract "span")
```

#### `ref.macro`

```scm
(macro_invocation
  macro: (_) @ref.macro.path
  (token_tree) @ref.macro.args
) @ref.macro
(#set! cq.pack "10_refs") (#set! cq.emit "rust_refs") (#set! cq.kind "ref.macro")
(#set! cq.anchor "ref.macro") (#set! cq.windowable)
(#set! cq.slot.path "ref.macro.path") (#set! cq.slot.path.extract "span_text")
(#set! cq.slot.args "ref.macro.args") (#set! cq.slot.args.extract "span")
```

#### `ref.lifetime`

```scm
(lifetime) @ref.lifetime
(#set! cq.pack "10_refs") (#set! cq.emit "rust_refs") (#set! cq.kind "ref.lifetime")
(#set! cq.anchor "ref.lifetime") (#set! cq.windowable)
(#set! cq.slot.name "ref.lifetime") (#set! cq.slot.name.extract "span_text")
```

---

## D.6.6 Pack 20_calls.scm → `rust_calls`

### D.6.6.1 Call events (exact `cq.kind` set)

* `call` *(call_expression where callee is not field_expression)*
* `call.method` *(call_expression where callee is field_expression)*
* `call.macro` *(macro_invocation treated as call-like)*

### D.6.6.2 Patterns

#### `call` (generic)

```scm
(call_expression
  function: (_) @call.callee
  arguments: (arguments) @call.args
) @call
(#set! cq.pack "20_calls") (#set! cq.emit "rust_calls") (#set! cq.kind "call")
(#set! cq.anchor "call") (#set! cq.windowable)
(#set! cq.slot.callee "call.callee") (#set! cq.slot.callee.extract "span_text")
(#set! cq.slot.args "call.args") (#set! cq.slot.args.extract "span")
```

#### `call.method`

```scm
(call_expression
  function: (field_expression
              value: (_) @call.recv
              field: (field_identifier) @call.method)
  arguments: (arguments) @call.args
) @call.method
(#set! cq.pack "20_calls") (#set! cq.emit "rust_calls") (#set! cq.kind "call.method")
(#set! cq.anchor "call.method") (#set! cq.windowable)
(#set! cq.slot.recv "call.recv") (#set! cq.slot.recv.extract "span_text")
(#set! cq.slot.method "call.method") (#set! cq.slot.method.extract "span_text")
(#set! cq.slot.args "call.args") (#set! cq.slot.args.extract "span")
```

#### `call.macro`

```scm
(macro_invocation
  macro: (_) @call.macro.path
  (token_tree) @call.macro.args
) @call.macro
(#set! cq.pack "20_calls") (#set! cq.emit "rust_calls") (#set! cq.kind "call.macro")
(#set! cq.anchor "call.macro") (#set! cq.windowable)
(#set! cq.slot.path "call.macro.path") (#set! cq.slot.path.extract "span_text")
(#set! cq.slot.args "call.macro.args") (#set! cq.slot.args.extract "span")
```

---

## D.6.7 Pack 30_types.scm → `rust_types`

### D.6.7.1 Type events (exact `cq.kind` set)

* `type.ref` *(reference_type)*
* `type.ptr` *(pointer_type)*
* `type.generic` *(generic_type)*
* `type.fn` *(function_type)*
* `type.dyn` *(dynamic_type)*
* `type.impl` *(abstract_type / impl-trait-ish surface in this grammar)*
* `type.bounds` *(trait_bounds)*
* `type.where_pred` *(where_predicate)*

### D.6.7.2 Patterns

#### `type.ref`

```scm
(reference_type
  (lifetime)? @type.ref.lifetime
  (mutable_specifier)? @type.ref.mut
  type: (_type) @type.ref.inner
) @type.ref
(#set! cq.pack "30_types") (#set! cq.emit "rust_types") (#set! cq.kind "type.ref")
(#set! cq.anchor "type.ref") (#set! cq.windowable)
(#set! cq.slot.inner "type.ref.inner") (#set! cq.slot.inner.extract "span")
(#set! cq.slot.lifetime "type.ref.lifetime") (#set! cq.slot.lifetime.extract "span_text")
(#set! cq.slot.mut "type.ref.mut") (#set! cq.slot.mut.extract "span_text")
```

#### `type.ptr`

```scm
(pointer_type
  (mutable_specifier)? @type.ptr.mut
  type: (_type) @type.ptr.inner
) @type.ptr
(#set! cq.pack "30_types") (#set! cq.emit "rust_types") (#set! cq.kind "type.ptr")
(#set! cq.anchor "type.ptr") (#set! cq.windowable)
(#set! cq.slot.inner "type.ptr.inner") (#set! cq.slot.inner.extract "span")
(#set! cq.slot.mut "type.ptr.mut") (#set! cq.slot.mut.extract "span_text")
```

#### `type.generic`

```scm
(generic_type
  type: (_) @type.generic.base
  type_arguments: (type_arguments) @type.generic.args
) @type.generic
(#set! cq.pack "30_types") (#set! cq.emit "rust_types") (#set! cq.kind "type.generic")
(#set! cq.anchor "type.generic") (#set! cq.windowable)
(#set! cq.slot.base "type.generic.base") (#set! cq.slot.base.extract "span_text")
(#set! cq.slot.args "type.generic.args") (#set! cq.slot.args.extract "span")
```

#### `type.fn`

```scm
(function_type
  parameters: (parameters) @type.fn.params
  return_type: (_type)? @type.fn.ret
) @type.fn
(#set! cq.pack "30_types") (#set! cq.emit "rust_types") (#set! cq.kind "type.fn")
(#set! cq.anchor "type.fn") (#set! cq.windowable)
(#set! cq.slot.params "type.fn.params") (#set! cq.slot.params.extract "span")
(#set! cq.slot.ret "type.fn.ret") (#set! cq.slot.ret.extract "span")
```

#### `type.dyn` / `type.impl`

```scm
(dynamic_type
  (trait_bounds) @type.dyn.bounds
) @type.dyn
(#set! cq.pack "30_types") (#set! cq.emit "rust_types") (#set! cq.kind "type.dyn")
(#set! cq.anchor "type.dyn") (#set! cq.windowable)
(#set! cq.slot.bounds "type.dyn.bounds") (#set! cq.slot.bounds.extract "span")

(abstract_type
  (trait_bounds) @type.impl.bounds
) @type.impl
(#set! cq.pack "30_types") (#set! cq.emit "rust_types") (#set! cq.kind "type.impl")
(#set! cq.anchor "type.impl") (#set! cq.windowable)
(#set! cq.slot.bounds "type.impl.bounds") (#set! cq.slot.bounds.extract "span")
```

#### `type.bounds`

```scm
(trait_bounds) @type.bounds
(#set! cq.pack "30_types") (#set! cq.emit "rust_types") (#set! cq.kind "type.bounds")
(#set! cq.anchor "type.bounds") (#set! cq.windowable)
(#set! cq.slot.bounds "type.bounds") (#set! cq.slot.bounds.extract "span")
```

#### `type.where_pred`

```scm
(where_predicate
  left: (_) @type.where.left
  bounds: (trait_bounds) @type.where.bounds
) @type.where_pred
(#set! cq.pack "30_types") (#set! cq.emit "rust_types") (#set! cq.kind "type.where_pred")
(#set! cq.anchor "type.where_pred") (#set! cq.windowable)
(#set! cq.slot.left "type.where.left") (#set! cq.slot.left.extract "span")
(#set! cq.slot.bounds "type.where.bounds") (#set! cq.slot.bounds.extract "span")
```

---

## D.6.8 Pack 40_attrs_docs.scm → `rust_attrs_docs`

### D.6.8.1 Why this is a combined pack

* Attributes and doc comments are “leading trivia” you typically **attach by ordering + spans**.
* Avoid directives like `#select-adjacent!` for attachment in your Python pipeline: support can vary by consumer (there are reports of it only being honored in specific components). ([GitHub][6])

### D.6.8.2 Attr/doc events (exact `cq.kind` set)

* `attr.outer`
* `attr.inner`
* `doc.line`
* `doc.block`
* `lit.string`
* `lit.raw_string`
* `lit.char`

### D.6.8.3 Patterns

#### `attr.outer` / `attr.inner`

```scm
(attribute_item
  (attribute
    (identifier) @attr.path.leaf
    (token_tree)? @attr.args
    value: (_expression)? @attr.value
  ) @attr.node
) @attr.outer
(#set! cq.pack "40_attrs_docs") (#set! cq.emit "rust_attrs_docs") (#set! cq.kind "attr.outer")
(#set! cq.anchor "attr.outer") (#set! cq.windowable)
(#set! cq.slot.attr "attr.node") (#set! cq.slot.attr.extract "span")
(#set! cq.slot.path_leaf "attr.path.leaf") (#set! cq.slot.path_leaf.extract "span_text")
(#set! cq.slot.args "attr.args") (#set! cq.slot.args.extract "span")
(#set! cq.slot.value "attr.value") (#set! cq.slot.value.extract "span")
(#set! cq.slot.path_leaf.normalize "rust:attr_path")

(inner_attribute_item
  (attribute
    (identifier) @attr.path.leaf
    (token_tree)? @attr.args
    value: (_expression)? @attr.value
  ) @attr.node
) @attr.inner
(#set! cq.pack "40_attrs_docs") (#set! cq.emit "rust_attrs_docs") (#set! cq.kind "attr.inner")
(#set! cq.anchor "attr.inner") (#set! cq.windowable)
(#set! cq.slot.attr "attr.node") (#set! cq.slot.attr.extract "span")
(#set! cq.slot.path_leaf "attr.path.leaf") (#set! cq.slot.path_leaf.extract "span_text")
(#set! cq.slot.args "attr.args") (#set! cq.slot.args.extract "span")
(#set! cq.slot.value "attr.value") (#set! cq.slot.value.extract "span")
```

#### `doc.line` / `doc.block` (capture the comment node + normalize later)

Tree-sitter exposes doc-comment-ness as structure in the grammar (e.g., marker fields), but **you should normalize in Python** so you don’t depend on `#strip!` support differences.

```scm
(line_comment
  doc: (doc_comment)? @doc.marker
) @doc.line
(#set! cq.pack "40_attrs_docs") (#set! cq.emit "rust_attrs_docs") (#set! cq.kind "doc.line")
(#set! cq.anchor "doc.line") (#set! cq.windowable)
(#set! cq.slot.marker "doc.marker") (#set! cq.slot.marker.extract "span")
(#set! cq.slot.text "doc.line") (#set! cq.slot.text.extract "span_text")
(#set! cq.slot.text.normalize "rust:doc_strip_markers")

(block_comment
  doc: (doc_comment)? @doc.marker
) @doc.block
(#set! cq.pack "40_attrs_docs") (#set! cq.emit "rust_attrs_docs") (#set! cq.kind "doc.block")
(#set! cq.anchor "doc.block") (#set! cq.windowable)
(#set! cq.slot.marker "doc.marker") (#set! cq.slot.marker.extract "span")
(#set! cq.slot.text "doc.block") (#set! cq.slot.text.extract "span_text")
(#set! cq.slot.text.normalize "rust:doc_strip_markers")
```

#### Literals (for your own injection + extraction)

```scm
(string_literal
  (string_content) @lit.string.content
) @lit.string
(#set! cq.pack "40_attrs_docs") (#set! cq.emit "rust_attrs_docs") (#set! cq.kind "lit.string")
(#set! cq.anchor "lit.string") (#set! cq.windowable)
(#set! cq.slot.content "lit.string.content") (#set! cq.slot.content.extract "span_text")

(raw_string_literal
  (string_content) @lit.raw_string.content
) @lit.raw_string
(#set! cq.pack "40_attrs_docs") (#set! cq.emit "rust_attrs_docs") (#set! cq.kind "lit.raw_string")
(#set! cq.anchor "lit.raw_string") (#set! cq.windowable)
(#set! cq.slot.content "lit.raw_string.content") (#set! cq.slot.content.extract "span_text")

(char_literal) @lit.char
(#set! cq.pack "40_attrs_docs") (#set! cq.emit "rust_attrs_docs") (#set! cq.kind "lit.char")
(#set! cq.anchor "lit.char") (#set! cq.windowable)
(#set! cq.slot.text "lit.char") (#set! cq.slot.text.extract "span_text")
```

---

## D.6.9 Contract compilation + mechanical schema generation (Python)

### D.6.9.1 Compile packs and harvest `cq.*` settings

py-tree-sitter:

* `Query.pattern_settings(i)` returns properties set via `#set!` ([tree-sitter.github.io][2])
* supported predicates include `#set!` by default ([tree-sitter.github.io][7])

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tree_sitter import Query
from typing import Any

@dataclass(frozen=True)
class PatternContract:
    pack: str
    emit: str
    kind: str
    anchor: str
    windowable: bool
    slots: dict[str, dict[str, str]]  # slot -> {"cap": "...", "extract": "...", "normalize": "..."}

def parse_pattern_contract(q: Query, i: int) -> PatternContract | None:
    s: dict[str, Any] = q.pattern_settings(i)  # cq.* via #set!
    if "cq.emit" not in s:
        return None

    pack = s["cq.pack"]
    emit = s["cq.emit"]
    kind = s["cq.kind"]
    anchor = s["cq.anchor"]
    windowable = "cq.windowable" in s

    slots: dict[str, dict[str, str]] = {}
    for k, v in s.items():
        if not k.startswith("cq.slot."):
            continue
        rest = k[len("cq.slot."):]  # e.g. "name" or "name.extract"
        parts = rest.split(".")
        slot = parts[0]
        slots.setdefault(slot, {})
        if len(parts) == 1:
            slots[slot]["cap"] = v
        else:
            slots[slot][parts[1]] = v  # "extract" / "normalize"

    return PatternContract(pack, emit, kind, anchor, windowable, slots)
```

### D.6.9.2 Lint: enforce windowability rules

```python
def lint_windowability(q: Query, i: int, pc: PatternContract) -> None:
    if not pc.windowable:
        return
    if not q.is_pattern_rooted(i):
        raise ValueError(f"{pc.pack}:{pc.kind} pattern {i} is not rooted")
    if q.is_pattern_non_local(i):
        raise ValueError(f"{pc.pack}:{pc.kind} pattern {i} is non-local (breaks range optimizations)")
```

Non-local definition + optimization warning is documented on `Query.is_pattern_non_local`. ([tree-sitter.github.io][2])

### D.6.9.3 Build table schemas mechanically

Schema = base columns + union of slot columns.

```python
BASE_COLS = [
    "pack", "kind", "pattern_i",
    "anchor_kind", "anchor_bstart", "anchor_bend",
]

def slot_cols(slot: str, extract: str) -> list[str]:
    cols = []
    if extract in ("span", "span_text", "span_kind"):
        cols += [f"{slot}_bstart", f"{slot}_bend"]
    if extract in ("text", "span_text"):
        cols += [f"{slot}_text"]
    if extract in ("kind", "span_kind"):
        cols += [f"{slot}_kind"]
    return cols

def build_table_schemas(pattern_contracts: list[tuple[int, PatternContract]]) -> dict[str, list[str]]:
    tables: dict[str, set[str]] = {}
    for pattern_i, pc in pattern_contracts:
        cols = tables.setdefault(pc.emit, set(BASE_COLS))
        for slot, meta in pc.slots.items():
            extract = meta.get("extract", "span")
            for c in slot_cols(slot, extract):
                cols.add(c)
    return {t: BASE_COLS + sorted(c for c in cols if c not in BASE_COLS) for t, cols in tables.items()}
```

### D.6.9.4 Runtime extraction: matches → rows

`QueryCursor.matches()` returns tuples `(pattern_index, {capture_name: node})`. ([tree-sitter.github.io][3])

```python
def node_span(n) -> tuple[int, int]:
    return (n.start_byte, n.end_byte)

def node_text(src: bytes, n) -> str:
    return src[n.start_byte:n.end_byte].decode("utf-8", errors="replace")

def emit_rows(src: bytes, root, q: Query, contracts_by_pattern: dict[int, PatternContract]):
    from tree_sitter import QueryCursor
    cur = QueryCursor(q)

    for pattern_i, caps in cur.matches(root):
        pc = contracts_by_pattern.get(pattern_i)
        if not pc:
            continue

        anchor_node = caps[pc.anchor]
        row = {
            "pack": pc.pack,
            "kind": pc.kind,
            "pattern_i": pattern_i,
            "anchor_kind": anchor_node.type,
            "anchor_bstart": anchor_node.start_byte,
            "anchor_bend": anchor_node.end_byte,
        }

        for slot, meta in pc.slots.items():
            cap_name = meta.get("cap")
            if not cap_name:
                continue
            n = caps.get(cap_name)
            if not n:
                continue

            extract = meta.get("extract", "span")
            if extract in ("span", "span_text", "span_kind"):
                b0, b1 = node_span(n)
                row[f"{slot}_bstart"] = b0
                row[f"{slot}_bend"] = b1
            if extract in ("text", "span_text"):
                row[f"{slot}_text"] = node_text(src, n)
            if extract in ("kind", "span_kind"):
                row[f"{slot}_kind"] = n.type

            # normalize hooks happen after emission (pc.slots[slot].get("normalize"))

        yield pc.emit, row
```

---

## D.6.10 One explicit “bridge” to grammar-provided packs (for context, not reuse)

Rust’s grammar ships **injection metadata** via query properties like `injection.language` and the flag-like `injection.include-children`. ([Docs.rs][8])
That’s the same technique you’re using with `cq.*`: `#set!` properties become machine-readable directives. ([tree-sitter.github.io][1])

---

If you want the next step after this: I can give you a **full “contract registry”** (a single JSON/YAML spec) that enumerates every `cq.kind` with its expected slots + extraction modes, and a **pack linter** that validates (a) capture naming, (b) slots exist in capture_names, and (c) patterns meet windowability constraints before you ever run them.

[1]: https://tree-sitter.github.io/tree-sitter/using-parsers/queries/3-predicates-and-directives.html "Predicates and Directives - Tree-sitter"
[2]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Query.html "Query — py-tree-sitter 0.25.2 documentation"
[3]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.QueryCursor.html "QueryCursor — py-tree-sitter 0.25.2 documentation"
[4]: https://tree-sitter.github.io/tree-sitter/3-syntax-highlighting.html "Syntax Highlighting - Tree-sitter"
[5]: https://tree-sitter.github.io/tree-sitter/using-parsers/queries/1-syntax.html "Basic Syntax - Tree-sitter"
[6]: https://github.com/tree-sitter/tree-sitter/issues/4261?utm_source=chatgpt.com "`#select-adjacent!` not respected by queries · Issue #4261"
[7]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Query.html?utm_source=chatgpt.com "Query — py-tree-sitter 0.25.2 documentation"
[8]: https://docs.rs/tree-sitter-rust/latest/tree_sitter_rust/constant.INJECTIONS_QUERY.html?utm_source=chatgpt.com "INJECTIONS_QUERY in tree_sitter_rust - Rust"

# D.7 Contract registry + pack linter (CqQueryPack v1)

You want **one authoritative registry** that defines the *semantic API* (`cq.kind` → slots + extraction modes), and a **linter** that proves every `.scm` pack actually implements that API *before you ever run it*.

This leverages two facts:

* Tree-sitter queries let you attach **arbitrary key/value metadata** to patterns via the `#set!` directive. ([tree-sitter.github.io][1])
* In py-tree-sitter, that metadata is programmatically available via `Query.pattern_settings(pattern_index)` (properties set via `#set!`), and you can also inspect **range-execution safety** with `is_pattern_rooted` + `is_pattern_non_local`. ([tree-sitter.github.io][2])
* Query execution returns matches as `(pattern_index, {capture_name: node})` and you can constrain execution via byte ranges (`set_byte_range`, `set_containing_byte_range`). ([tree-sitter.github.io][3])
* Captures are written using `@name` after nodes, and anchors (`.`) constrain sibling/child matching behavior (important for making patterns local/windowable). ([tree-sitter.github.io][4])

---

## D.7.1 Contract Registry: single YAML spec (authoritative API)

**File:** `contracts/cq_rust_registry.v1.yaml`
**Purpose:** defines the *only* allowed `cq.kind` values and the exact slot contracts (slot names + extraction modes + normalization hints + required slot sets).

### D.7.1.1 Registry schema (top-level)

```yaml
registry_id: cq_rust_index_v1
contract_version: v1
language:
  name: rust
  grammar: tree-sitter-rust
  # optional: helps humans; linter doesn’t require it
  grammar_version: "0.24.0"

# Capture names in the query source use @, but py-tree-sitter exposes capture names without the '@'.
# Captures are the API surface; treat naming as a linted contract. :contentReference[oaicite:4]{index=4}
capture_name_rules:
  allowed_regexes:
    - '^(def|ref|call|type|attr|doc|macro|lit)(\.[A-Za-z0-9_]+)+$'
    - '^_tmp\.[A-Za-z0-9_]+$'
  banned_prefixes:
    - 'variable.'
    - 'function.'
    - 'type.'        # editor-highlight style capture namespaces (avoid)
    - 'keyword.'

extract_modes:
  # used by linter + schema generator (span/text/kind combos)
  allowed: [span, text, span_text, kind, span_kind]

normalize_hints:
  rust:path_segments:
    doc: "Flatten scoped_identifier / scoped_type_identifier chains into segment arrays."
  rust:attr_path:
    doc: "Normalize attribute paths (including scoped_identifier)."
  rust:doc_strip_markers:
    doc: "Strip ///, //!, /** */, /*! */ markers and leading *."

tables:
  rust_defs:       {kind_prefixes: ["def."]}
  rust_refs:       {kind_prefixes: ["ref."]}
  rust_calls:      {kind_prefixes: ["call"]}
  rust_types:      {kind_prefixes: ["type."]}
  rust_attrs_docs: {kind_prefixes: ["attr.", "doc.", "lit."]}

# The canonical semantic API:
kinds: {}
```

---

## D.7.2 Full `kinds` registry (all cq.kind values, slots, modes, required sets)

Below is a **complete** `kinds:` block consistent with the pack set you standardized:

* `00_defs.scm` → `rust_defs`
* `10_refs.scm` → `rust_refs`
* `20_calls.scm` → `rust_calls`
* `30_types.scm` → `rust_types`
* `40_attrs_docs.scm` → `rust_attrs_docs`

> **Registry design rule:** If a kind has multiple pattern shapes (e.g., `ref.type` can be `scoped_type_identifier` *or* `generic_type`), encode that as `required_slot_sets` so the linter can validate either shape.

```yaml
kinds:
  # -------------------------
  # 00_defs → rust_defs
  # -------------------------
  def.fn:
    emit: rust_defs
    allowed_anchors: ["def.fn"]
    windowable: true
    required_slot_sets:
      - ["name", "params", "body"]
    slots:
      name:  {extract: span_text}
      params:{extract: span}
      body:  {extract: span}
      where: {extract: span, optional: true}

  def.struct:
    emit: rust_defs
    allowed_anchors: ["def.struct"]
    windowable: true
    required_slot_sets:
      - ["name"]
    slots:
      name:  {extract: span_text}
      body:  {extract: span, optional: true}
      where: {extract: span, optional: true}

  def.enum:
    emit: rust_defs
    allowed_anchors: ["def.enum"]
    windowable: true
    required_slot_sets:
      - ["name", "variants"]
    slots:
      name:    {extract: span_text}
      variants:{extract: span}
      where:   {extract: span, optional: true}

  def.union:
    emit: rust_defs
    allowed_anchors: ["def.union"]
    windowable: true
    required_slot_sets:
      - ["name", "fields"]
    slots:
      name:   {extract: span_text}
      fields: {extract: span}

  def.trait:
    emit: rust_defs
    allowed_anchors: ["def.trait"]
    windowable: true
    required_slot_sets:
      - ["name", "body"]
    slots:
      name:   {extract: span_text}
      bounds: {extract: span, optional: true}
      body:   {extract: span}
      where:  {extract: span, optional: true}

  def.mod:
    emit: rust_defs
    allowed_anchors: ["def.mod"]
    windowable: true
    required_slot_sets:
      - ["name"]
    slots:
      name: {extract: span_text}
      body: {extract: span, optional: true}

  def.type_alias:
    emit: rust_defs
    allowed_anchors: ["def.type_alias"]
    windowable: true
    required_slot_sets:
      - ["name", "rhs"]
    slots:
      name: {extract: span_text}
      rhs:  {extract: span}

  def.const:
    emit: rust_defs
    allowed_anchors: ["def.const"]
    windowable: true
    required_slot_sets:
      - ["name", "type"]
    slots:
      name:  {extract: span_text}
      type:  {extract: span}
      value: {extract: span, optional: true}

  def.static:
    emit: rust_defs
    allowed_anchors: ["def.static"]
    windowable: true
    required_slot_sets:
      - ["name", "type"]
    slots:
      name:  {extract: span_text}
      type:  {extract: span}
      value: {extract: span, optional: true}

  def.macro_def:
    emit: rust_defs
    allowed_anchors: ["def.macro_def"]
    windowable: true
    required_slot_sets:
      - ["name"]
    slots:
      name: {extract: span_text}

  def.impl:
    emit: rust_defs
    allowed_anchors: ["def.impl"]
    windowable: true
    required_slot_sets:
      - ["for_type"]
    slots:
      trait:   {extract: span, optional: true}
      for_type:{extract: span}
      body:    {extract: span, optional: true}
      where:   {extract: span, optional: true}

  # -------------------------
  # 10_refs → rust_refs
  # -------------------------
  ref.path:
    emit: rust_refs
    allowed_anchors: ["ref.path"]
    windowable: true
    required_slot_sets:
      - ["leaf"]
    slots:
      leaf: {extract: span_text, normalize: rust:path_segments}
      base: {extract: span, optional: true}

  # ref.type has two valid “shapes”: path-ish or generic-ish.
  ref.type:
    emit: rust_refs
    allowed_anchors: ["ref.type", "ref.type.generic"]
    windowable: true
    required_slot_sets:
      - ["leaf"]            # scoped_type_identifier variant
      - ["callee", "args"]  # generic_type variant
    slots:
      leaf:   {extract: span_text, normalize: rust:path_segments, optional: true}
      base:   {extract: span, optional: true}
      callee: {extract: span_text, optional: true}
      args:   {extract: span, optional: true}

  ref.use:
    emit: rust_refs
    allowed_anchors: ["ref.use"]
    windowable: true
    required_slot_sets:
      - ["arg"]
    slots:
      arg: {extract: span}

  ref.macro:
    emit: rust_refs
    allowed_anchors: ["ref.macro"]
    windowable: true
    required_slot_sets:
      - ["path", "args"]
    slots:
      path: {extract: span_text}
      args: {extract: span}

  ref.lifetime:
    emit: rust_refs
    allowed_anchors: ["ref.lifetime"]
    windowable: true
    required_slot_sets:
      - ["name"]
    slots:
      name: {extract: span_text}

  # -------------------------
  # 20_calls → rust_calls
  # -------------------------
  call:
    emit: rust_calls
    allowed_anchors: ["call"]
    windowable: true
    required_slot_sets:
      - ["callee", "args"]
    slots:
      callee: {extract: span_text}
      args:   {extract: span}

  call.method:
    emit: rust_calls
    allowed_anchors: ["call.method"]
    windowable: true
    required_slot_sets:
      - ["recv", "method", "args"]
    slots:
      recv:   {extract: span_text}
      method: {extract: span_text}
      args:   {extract: span}

  call.macro:
    emit: rust_calls
    allowed_anchors: ["call.macro"]
    windowable: true
    required_slot_sets:
      - ["path", "args"]
    slots:
      path: {extract: span_text}
      args: {extract: span}

  # -------------------------
  # 30_types → rust_types
  # -------------------------
  type.ref:
    emit: rust_types
    allowed_anchors: ["type.ref"]
    windowable: true
    required_slot_sets:
      - ["inner"]
    slots:
      inner:    {extract: span}
      lifetime: {extract: span_text, optional: true}
      mut:      {extract: span_text, optional: true}

  type.ptr:
    emit: rust_types
    allowed_anchors: ["type.ptr"]
    windowable: true
    required_slot_sets:
      - ["inner"]
    slots:
      inner: {extract: span}
      mut:   {extract: span_text, optional: true}

  type.generic:
    emit: rust_types
    allowed_anchors: ["type.generic"]
    windowable: true
    required_slot_sets:
      - ["base", "args"]
    slots:
      base: {extract: span_text}
      args: {extract: span}

  type.fn:
    emit: rust_types
    allowed_anchors: ["type.fn"]
    windowable: true
    required_slot_sets:
      - ["params"]
    slots:
      params: {extract: span}
      ret:    {extract: span, optional: true}

  type.dyn:
    emit: rust_types
    allowed_anchors: ["type.dyn"]
    windowable: true
    required_slot_sets:
      - ["bounds"]
    slots:
      bounds: {extract: span}

  type.impl:
    emit: rust_types
    allowed_anchors: ["type.impl"]
    windowable: true
    required_slot_sets:
      - ["bounds"]
    slots:
      bounds: {extract: span}

  type.bounds:
    emit: rust_types
    allowed_anchors: ["type.bounds"]
    windowable: true
    required_slot_sets:
      - ["bounds"]
    slots:
      bounds: {extract: span}

  type.where_pred:
    emit: rust_types
    allowed_anchors: ["type.where_pred"]
    windowable: true
    required_slot_sets:
      - ["left", "bounds"]
    slots:
      left:   {extract: span}
      bounds: {extract: span}

  # -------------------------
  # 40_attrs_docs → rust_attrs_docs
  # -------------------------
  attr.outer:
    emit: rust_attrs_docs
    allowed_anchors: ["attr.outer"]
    windowable: true
    required_slot_sets:
      - ["attr", "path_leaf"]
    slots:
      attr:      {extract: span}
      path_leaf: {extract: span_text, normalize: rust:attr_path}
      args:      {extract: span, optional: true}
      value:     {extract: span, optional: true}

  attr.inner:
    emit: rust_attrs_docs
    allowed_anchors: ["attr.inner"]
    windowable: true
    required_slot_sets:
      - ["attr", "path_leaf"]
    slots:
      attr:      {extract: span}
      path_leaf: {extract: span_text}
      args:      {extract: span, optional: true}
      value:     {extract: span, optional: true}

  doc.line:
    emit: rust_attrs_docs
    allowed_anchors: ["doc.line"]
    windowable: true
    required_slot_sets:
      - ["text"]
    slots:
      marker: {extract: span, optional: true}
      text:   {extract: span_text, normalize: rust:doc_strip_markers}

  doc.block:
    emit: rust_attrs_docs
    allowed_anchors: ["doc.block"]
    windowable: true
    required_slot_sets:
      - ["text"]
    slots:
      marker: {extract: span, optional: true}
      text:   {extract: span_text, normalize: rust:doc_strip_markers}

  lit.string:
    emit: rust_attrs_docs
    allowed_anchors: ["lit.string"]
    windowable: true
    required_slot_sets:
      - ["content"]
    slots:
      content: {extract: span_text}

  lit.raw_string:
    emit: rust_attrs_docs
    allowed_anchors: ["lit.raw_string"]
    windowable: true
    required_slot_sets:
      - ["content"]
    slots:
      content: {extract: span_text}

  lit.char:
    emit: rust_attrs_docs
    allowed_anchors: ["lit.char"]
    windowable: true
    required_slot_sets:
      - ["text"]
    slots:
      text: {extract: span_text}
```

---

## D.8 Pack linter: validates schema + captures + windowability *before runtime*

### D.8.1 What the linter must prove

For every **emitting** pattern (i.e., any pattern with `cq.kind`/`cq.emit`):

1. **Capture naming**

   * Every capture name in the compiled query must match the allowed regexes (e.g., `def.fn.name`)
   * Captures are the API and are written with `@…` in query source. ([tree-sitter.github.io][4])

2. **Slots exist**

   * Each `cq.slot.<slot>` must reference a capture name present in `Query`’s capture table (`capture_count`, `capture_name(i)`). ([tree-sitter.github.io][2])
   * Slot extraction mode must be one of allowed modes and must match the registry for that kind.

3. **Windowability constraints**

   * If pattern has `cq.windowable`, it must be rooted and not non-local (`is_pattern_rooted(i)` true, `is_pattern_non_local(i)` false).
   * py-tree-sitter explicitly notes non-local patterns disable optimizations for executing on a specific range. ([tree-sitter.github.io][2])
   * This is critical because you will execute packs on changed byte ranges using `QueryCursor.set_byte_range` / `set_containing_byte_range`. ([tree-sitter.github.io][3])

Also: metadata must be readable via `Query.pattern_settings(i)` (properties are set using `#set!`). ([tree-sitter.github.io][2])

---

## D.8.2 Linter implementation (Python, strict by default)

**File:** `tools/lint_rust_query_packs.py`

```python
from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from tree_sitter import Language, Query, QueryError
import tree_sitter_rust as tsrust

# ----------------------------
# Registry loading
# ----------------------------

def load_registry(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix in {".json"}:
        return json.loads(text)
    # YAML optional: avoid hard dependency
    try:
        import yaml  # type: ignore
    except Exception as e:
        raise RuntimeError("YAML registry requires PyYAML (pip install pyyaml)") from e
    return yaml.safe_load(text)

@dataclass(frozen=True)
class SlotSpec:
    extract: str
    normalize: str | None
    optional: bool

@dataclass(frozen=True)
class KindSpec:
    emit: str
    allowed_anchors: tuple[str, ...]
    windowable: bool
    required_slot_sets: tuple[tuple[str, ...], ...]
    slots: dict[str, SlotSpec]

@dataclass(frozen=True)
class Registry:
    registry_id: str
    contract_version: str
    allowed_capture_regexes: tuple[re.Pattern[str], ...]
    banned_prefixes: tuple[str, ...]
    allowed_extract_modes: frozenset[str]
    kinds: dict[str, KindSpec]

def parse_registry(raw: dict[str, Any]) -> Registry:
    rules = raw["capture_name_rules"]
    allowed = tuple(re.compile(rgx) for rgx in rules["allowed_regexes"])
    banned = tuple(rules.get("banned_prefixes", []))
    allowed_modes = frozenset(raw["extract_modes"]["allowed"])

    kinds: dict[str, KindSpec] = {}
    for kind, kraw in raw["kinds"].items():
        slots: dict[str, SlotSpec] = {}
        for slot, sraw in kraw.get("slots", {}).items():
            slots[slot] = SlotSpec(
                extract=sraw["extract"],
                normalize=sraw.get("normalize"),
                optional=bool(sraw.get("optional", False)),
            )
        kinds[kind] = KindSpec(
            emit=kraw["emit"],
            allowed_anchors=tuple(kraw["allowed_anchors"]),
            windowable=bool(kraw.get("windowable", False)),
            required_slot_sets=tuple(tuple(x) for x in kraw.get("required_slot_sets", [])),
            slots=slots,
        )

    return Registry(
        registry_id=raw["registry_id"],
        contract_version=raw["contract_version"],
        allowed_capture_regexes=allowed,
        banned_prefixes=banned,
        allowed_extract_modes=allowed_modes,
        kinds=kinds,
    )

# ----------------------------
# Query pack linting
# ----------------------------

@dataclass
class LintIssue:
    level: str   # "ERROR" | "WARN"
    code: str
    pack: str
    pattern_i: int | None
    msg: str

def is_capture_name_allowed(reg: Registry, name: str) -> bool:
    if any(name.startswith(p) for p in reg.banned_prefixes):
        return False
    return any(rgx.match(name) for rgx in reg.allowed_capture_regexes)

def collect_capture_names(q: Query) -> list[str]:
    # Query.capture_count + capture_name(i) are the authoritative capture table. :contentReference[oaicite:10]{index=10}
    return [q.capture_name(i) for i in range(q.capture_count)]

def parse_pattern_slots(settings: dict[str, Any]) -> dict[str, dict[str, str]]:
    # Extract cq.slot.<slot>, cq.slot.<slot>.extract, cq.slot.<slot>.normalize
    slots: dict[str, dict[str, str]] = {}
    for k, v in settings.items():
        if not k.startswith("cq.slot."):
            continue
        rest = k[len("cq.slot."):]  # e.g. "name" or "name.extract"
        parts = rest.split(".")
        slot = parts[0]
        slots.setdefault(slot, {})
        if len(parts) == 1:
            slots[slot]["cap"] = str(v)
        else:
            slots[slot][parts[1]] = str(v)
    return slots

def satisfies_required_sets(pattern_slots: dict[str, dict[str, str]], required_sets: tuple[tuple[str, ...], ...]) -> bool:
    if not required_sets:
        return True
    present = set(pattern_slots.keys())
    return any(all(s in present for s in reqset) for reqset in required_sets)

def lint_pack(reg: Registry, pack_path: Path, language: Language, strict: bool = True) -> list[LintIssue]:
    issues: list[LintIssue] = []
    pack = pack_path.stem
    source = pack_path.read_text(encoding="utf-8")

    try:
        q = Query(language, source)
    except QueryError as e:
        issues.append(LintIssue("ERROR", "Q001", pack, None, f"Query failed to compile: {e}"))
        return issues

    capture_names = collect_capture_names(q)
    capture_set = set(capture_names)

    # (a) capture naming
    for cn in capture_names:
        if not is_capture_name_allowed(reg, cn):
            issues.append(LintIssue("ERROR", "C001", pack, None, f"Capture name violates policy: {cn!r}"))

    # Pattern-local checks
    for i in range(q.pattern_count):
        settings = q.pattern_settings(i)  # set via #set! :contentReference[oaicite:11]{index=11}
        if "cq.kind" not in settings and "cq.emit" not in settings:
            continue  # non-emitting pattern (allowed)

        # Required cq.* keys
        for key in ("cq.kind", "cq.emit", "cq.anchor"):
            if key not in settings:
                issues.append(LintIssue("ERROR", "S001", pack, i, f"Missing required setting: {key}"))
                continue

        kind = str(settings.get("cq.kind", ""))
        emit = str(settings.get("cq.emit", ""))
        anchor = str(settings.get("cq.anchor", ""))

        if kind not in reg.kinds:
            issues.append(LintIssue("ERROR", "R001", pack, i, f"Unknown cq.kind: {kind!r} (not in registry)"))
            continue
        kspec = reg.kinds[kind]

        # emit must match registry (strict)
        if emit != kspec.emit:
            issues.append(LintIssue("ERROR" if strict else "WARN", "R002", pack, i,
                                    f"cq.emit mismatch: pattern={emit!r} registry={kspec.emit!r}"))

        # anchor validation
        if anchor not in kspec.allowed_anchors:
            issues.append(LintIssue("ERROR", "A001", pack, i,
                                    f"cq.anchor {anchor!r} not in allowed_anchors for {kind}: {kspec.allowed_anchors}"))
        if anchor and anchor not in capture_set:
            issues.append(LintIssue("ERROR", "A002", pack, i,
                                    f"cq.anchor capture {anchor!r} not present in query capture table"))

        # (c) windowability constraints
        wants_windowable = "cq.windowable" in settings or kspec.windowable
        if wants_windowable:
            if not q.is_pattern_rooted(i):
                issues.append(LintIssue("ERROR", "W001", pack, i, "Windowable pattern is not rooted"))
            if q.is_pattern_non_local(i):
                issues.append(LintIssue("ERROR", "W002", pack, i, "Windowable pattern is non-local (breaks range optimizations)"))

        # slots + extraction modes
        slots = parse_pattern_slots(settings)

        # required slot sets
        if not satisfies_required_sets(slots, kspec.required_slot_sets):
            issues.append(LintIssue("ERROR", "S002", pack, i,
                                    f"Pattern does not satisfy any required_slot_sets for kind {kind}: {kspec.required_slot_sets}"))

        for slot, meta in slots.items():
            if slot not in kspec.slots:
                issues.append(LintIssue("ERROR", "S010", pack, i, f"Unknown slot {slot!r} for kind {kind}"))
                continue

            cap = meta.get("cap")
            if not cap:
                issues.append(LintIssue("ERROR", "S011", pack, i, f"Slot {slot!r} missing cq.slot.{slot} capture binding"))
                continue

            # (b) slot capture exists
            if cap not in capture_set:
                issues.append(LintIssue("ERROR", "S012", pack, i,
                                        f"Slot {slot!r} bound to capture {cap!r} not present in query capture table"))

            # extraction mode (strict: must be declared and must match registry)
            expected = kspec.slots[slot].extract
            got = meta.get("extract")
            if got is None:
                issues.append(LintIssue("ERROR" if strict else "WARN", "S013", pack, i,
                                        f"Slot {slot!r} missing cq.slot.{slot}.extract (expected {expected!r})"))
            else:
                if got not in reg.allowed_extract_modes:
                    issues.append(LintIssue("ERROR", "S014", pack, i, f"Invalid extract mode {got!r} for slot {slot!r}"))
                elif got != expected:
                    issues.append(LintIssue("ERROR", "S015", pack, i,
                                            f"Extract mode mismatch for slot {slot!r}: pattern={got!r} registry={expected!r}"))

            # optional normalize hint check (if set, should match registry)
            norm = meta.get("normalize")
            expected_norm = kspec.slots[slot].normalize
            if norm and expected_norm and norm != expected_norm:
                issues.append(LintIssue("ERROR" if strict else "WARN", "S016", pack, i,
                                        f"Normalize mismatch slot {slot!r}: pattern={norm!r} registry={expected_norm!r}"))

    return issues

# ----------------------------
# CLI
# ----------------------------

def main(argv: list[str]) -> int:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", required=True, type=Path)
    ap.add_argument("--packs-dir", required=True, type=Path)
    ap.add_argument("--strict", action="store_true", default=True)
    args = ap.parse_args(argv)

    reg = parse_registry(load_registry(args.registry))

    rust_lang = Language(tsrust.language())

    packs = sorted(args.packs_dir.glob("*.scm"))
    if not packs:
        print(f"ERROR: no .scm packs found in {args.packs_dir}", file=sys.stderr)
        return 2

    all_issues: list[LintIssue] = []
    for p in packs:
        all_issues.extend(lint_pack(reg, p, rust_lang, strict=args.strict))

    # Print issues
    for it in all_issues:
        loc = f"{it.pack}"
        if it.pattern_i is not None:
            loc += f":pattern[{it.pattern_i}]"
        print(f"{it.level} {it.code} {loc} - {it.msg}")

    # Fail if any ERROR
    return 1 if any(i.level == "ERROR" for i in all_issues) else 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
```

### Why these checks are “grounded”

* **Metadata**: `Query.pattern_settings(index)` returns properties set with `#set!`, and py-tree-sitter lists `#set!` as a supported predicate. ([tree-sitter.github.io][2])
* **Windowability**: `is_pattern_rooted` / `is_pattern_non_local` are part of `Query`, and non-local patterns explicitly disable optimizations for range execution. ([tree-sitter.github.io][2])
* **Range execution**: `QueryCursor.set_byte_range` returns matches that *intersect* the range; `set_containing_byte_range` restricts matches to be fully contained (added in 0.26.0). ([tree-sitter.github.io][3])
* **Match shape**: `QueryCursor.matches()` returns `(pattern_index, dict[capture_name → node])`. ([tree-sitter.github.io][3])
* **Captures syntax**: captures are written with `@` and are the mechanism for associating names with nodes. ([tree-sitter.github.io][4])

---

## D.8.3 CLI usage + CI wiring

### Run locally

```bash
python tools/lint_rust_query_packs.py \
  --registry contracts/cq_rust_registry.v1.yaml \
  --packs-dir queries_rust_index \
  --strict
```

### CI target (make it an invariant)

* Add a job that runs the linter on every PR.
* Fail the build on any `ERROR` exit code.

---

## D.8.4 Optional “extra correctness” checks (high ROI)

These aren’t required by your prompt, but they’re cheap wins because the `Query` API exposes pattern source ranges:

* `Query.start_byte_for_pattern(i)` / `end_byte_for_pattern(i)` lets the linter print the exact failing pattern snippet. ([tree-sitter.github.io][2])
* Enforce “only emitting patterns contain cq.* keys” (warn on `cq.slot.*` without `cq.kind`).
* Add `--mode freeze` to auto-generate a **derived registry** from packs (useful for bootstrapping), but keep the YAML registry as authoritative.

If you want, the next deep dive can be: **a “freeze + diff” workflow** (registry is semver’d, packs are validated, and grammar upgrades produce an automatic “contract drift report” keyed by `cq.kind`).

[1]: https://tree-sitter.github.io/tree-sitter/using-parsers/queries/3-predicates-and-directives.html "Predicates and Directives - Tree-sitter"
[2]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Query.html "Query — py-tree-sitter 0.25.2 documentation"
[3]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.QueryCursor.html "QueryCursor — py-tree-sitter 0.25.2 documentation"
[4]: https://tree-sitter.github.io/tree-sitter/using-parsers/queries/2-operators.html "Operators - Tree-sitter"

# D.9 Freeze + diff workflow (registry is semver’d; upgrades emit a cq.kind-keyed drift report)

This workflow makes **Tree-sitter grammar upgrades** and **query-pack edits** deterministic:

* **Freeze** = compile packs + read `#set!` metadata → emit a canonical *contract snapshot*.
* **Diff** = compare two freezes → produce a *contract drift report keyed by `cq.kind`* + a **required semver bump** recommendation.
* **Gate** = CI fails if drift exists without (a) an acknowledged freeze update and (b) a correct registry semver bump.

This is possible because:

* `#set!` attaches arbitrary key/value metadata to patterns (and predicates/directives are intentionally exposed so higher-level code can interpret them). ([tree-sitter.github.io][1])
* py-tree-sitter exposes those settings via `Query.pattern_settings(i)` (“Properties are set using the `#set!` predicate.”). ([tree-sitter.github.io][2])
* py-tree-sitter exposes **windowability** checks (`is_pattern_rooted`, `is_pattern_non_local`) and notes non-local patterns disable optimizations for range execution. ([tree-sitter.github.io][2])
* `QueryCursor.set_byte_range` vs `set_containing_byte_range` lets you implement incremental “changed-range reindex” precisely. ([tree-sitter.github.io][3])
* `node-types.json` is the grammar’s machine-readable universe of node types (`type` + `named` uniquely identify a node kind), so you can hash/diff it and treat it like an ABI surface. ([tree-sitter.github.io][4])

---

## D.9.0 Vocabulary (two independent version axes)

You version *two* things independently:

1. **Registry semver** (your API): `contracts/cq_rust_registry.v1.yaml`

   * e.g. `registry_version: 1.7.0`
   * This changes when the *meaning/shape* of emitted rows changes.

2. **Grammar version** (upstream): `tree-sitter-rust` + py-tree-sitter ABI compatibility

   * Record `tree_sitter.__version__`, plus `tree_sitter.LANGUAGE_VERSION` and `MIN_COMPATIBLE_LANGUAGE_VERSION` because py-tree-sitter documents ABI forward-incompatibility (newer language ABIs may not load on older runtimes). ([tree-sitter.github.io][5])

---

## D.9.1 Freeze artifact model (what you “lock” and later compare)

### D.9.1.1 On-disk layout (recommended)

```
contracts/
  cq_rust_registry.v1.yaml
  frozen/
    rust/
      tree-sitter-rust@0.24.0/
        cq_rust_index_v1@1.7.0/
          freeze.manifest.json
          node-types.json.sha256
          packs/
            00_defs.scm
            10_refs.scm
            20_calls.scm
            30_types.scm
            40_attrs_docs.scm
          compiled/
            00_defs.query_contract.json
            ...
          derived/
            kind_contracts.json
            kind_to_patterns.json
```

### D.9.1.2 `freeze.manifest.json` (canonical header)

Minimal header fields that make diffs explainable:

```json
{
  "registry": {
    "registry_id": "cq_rust_index_v1",
    "registry_version": "1.7.0",
    "contract_version": "v1",
    "registry_sha256": "..."
  },
  "grammar": {
    "language": "rust",
    "grammar_dist": "tree-sitter-rust",
    "grammar_version": "0.24.0",
    "node_types_sha256": "..."
  },
  "runtime": {
    "py_tree_sitter_version": "0.25.2",
    "ts_language_version": 15,
    "ts_min_compatible_language_version": 13
  },
  "inputs": {
    "packs_dir_sha256": "...",
    "packs": {
      "00_defs": {"sha256": "..."},
      "10_refs": {"sha256": "..."}
    }
  }
}
```

Where the runtime ABI fields come directly from py-tree-sitter’s published constants (`LANGUAGE_VERSION`, `MIN_COMPATIBLE_LANGUAGE_VERSION`). ([tree-sitter.github.io][5])

---

## D.9.2 Freeze step (compile → lint → emit canonical contracts)

### D.9.2.1 Freeze invariant: linter must pass before freeze is written

Your linter uses:

* `Query.pattern_settings(i)` to read `cq.*` keys (set via `#set!`). ([tree-sitter.github.io][2])
* `is_pattern_rooted` + `is_pattern_non_local` to enforce windowability. ([tree-sitter.github.io][2])

If the linter fails, you **do not** write a freeze snapshot.

### D.9.2.2 Pack compilation contract (what you record per pack)

For each pack, persist a `*.query_contract.json` with:

* `capture_names` from `Query.capture_name(i)` / `capture_count` (lets you validate slot bindings exist)
* per-pattern:

  * `pattern_i`
  * `start_byte_for_pattern(i)` / `end_byte_for_pattern(i)` (slice snippet for diff/debug)
  * `is_pattern_rooted(i)` / `is_pattern_non_local(i)`
  * `pattern_settings(i)` (your `cq.*` schema layer; from `#set!`) ([tree-sitter.github.io][2])
  * `pattern_assertions(i)` (from `#is?/#is-not?`, if you use them)

py-tree-sitter explicitly documents `pattern_settings` as “Properties are set using the `#set!` predicate.” ([tree-sitter.github.io][2])
Tree-sitter explicitly documents `#set!` as a directive for associating key-value metadata with a pattern. ([tree-sitter.github.io][1])

### D.9.2.3 Derived kind contracts (what you diff by `cq.kind`)

Your freeze should create a normalized `derived/kind_contracts.json` keyed by `cq.kind`, **not** by pattern index, e.g.:

```json
{
  "def.fn": {
    "emit": "rust_defs",
    "anchors": ["def.fn"],
    "windowable": true,
    "required_slot_sets": [["name","params","body"]],
    "slots": {
      "name":  {"extract":"span_text","normalize":null,"optional":false},
      "params": {"extract":"span","normalize":null,"optional":false},
      "body":  {"extract":"span","normalize":null,"optional":false},
      "where": {"extract":"span","normalize":null,"optional":true}
    }
  }
}
```

And a `derived/kind_to_patterns.json` that maps each `cq.kind` to the concrete implementing patterns (pack + pattern_i + snippet hash), e.g.:

```json
{
  "def.fn": [
    {"pack":"00_defs","pattern_i":12,"pattern_sha256":"...","rooted":true,"non_local":false}
  ]
}
```

**Why two layers?**

* `kind_contracts.json` is your API contract surface (semver’d).
* `kind_to_patterns.json` is your implementation detail surface (useful for diff/debug, but often “patch-only”).

### D.9.2.4 Freeze command (CLI)

```bash
python tools/cq_contract.py freeze \
  --registry contracts/cq_rust_registry.v1.yaml \
  --packs-dir queries_rust_index \
  --out contracts/frozen/rust/tree-sitter-rust@0.24.0/cq_rust_index_v1@1.7.0
```

---

## D.9.3 Diff step (contract drift report keyed by `cq.kind`)

### D.9.3.1 Diff inputs

Two freeze directories:

* `--old` = baseline (main branch freeze)
* `--new` = candidate (PR branch freeze)

Diff reads:

* `freeze.manifest.json` (versions + hashes)
* `derived/kind_contracts.json` (contract surface)
* `derived/kind_to_patterns.json` (implementation surface)
* `node-types.json.sha256` and optionally a full `node-types.json` snapshot

### D.9.3.2 Drift classification (what’s “breaking” vs “additive”)

**Breaking (requires MAJOR bump):**

* `cq.kind` removed
* `emit` changed (row moves tables)
* required slot removed or renamed
* slot `extract` changed (e.g. `span_text → span`) for a required slot
* slot `optional` flips `true → false` (tightens contract)
* `windowable` flips `true → false` **or** any implementing pattern becomes non-rooted/non-local (breaks incremental execution assumptions) ([tree-sitter.github.io][2])

**Additive (requires MINOR bump):**

* new `cq.kind`
* new optional slot
* new alternative `required_slot_set` that broadens accepted pattern shapes without removing prior shapes

**Patch-only (PATCH bump or no bump, your policy):**

* pattern text changes but `kind_contracts.json` identical
* implementation moved across packs but contract unchanged
* pattern count changes for same kind (additional patterns), contract unchanged

### D.9.3.3 Diff output: two files

1. `contract_drift_report.json` (machine-usable)
2. `contract_drift_report.md` (human-friendly)

---

## D.9.4 Drift report schema (JSON) — keyed by `cq.kind`

```json
{
  "summary": {
    "old": {"registry_version":"1.7.0","grammar_version":"0.24.0"},
    "new": {"registry_version":"1.7.1","grammar_version":"0.25.0"},
    "recommended_bump": "major",
    "counts": {"breaking":3,"additive":4,"patch":12}
  },
  "kinds": {
    "def.fn": {
      "severity": "breaking",
      "changes": [
        {"field":"slots.name.extract","old":"span_text","new":"span"},
        {"field":"windowable","old":true,"new":false}
      ],
      "old_contract": { "...": "..." },
      "new_contract": { "...": "..." },
      "impl": {
        "old_patterns": [{"pack":"00_defs","pattern_i":12,"sha":"..."}],
        "new_patterns": [{"pack":"00_defs","pattern_i":14,"sha":"...","non_local":true}]
      },
      "actions": [
        "Bump registry major",
        "Restore windowability: ensure rooted pattern and avoid non-local constructs"
      ]
    }
  }
}
```

Note: `non_local` and rooted/non-rooted detection are derived from py-tree-sitter’s `is_pattern_non_local` / `is_pattern_rooted`. ([tree-sitter.github.io][2])

---

## D.9.5 Implementation: `cq_contract.py diff` (core logic)

### D.9.5.1 Command

```bash
python tools/cq_contract.py diff \
  --old contracts/frozen/rust/tree-sitter-rust@0.24.0/cq_rust_index_v1@1.7.0 \
  --new contracts/frozen/rust/tree-sitter-rust@0.25.0/cq_rust_index_v1@1.8.0 \
  --out reports/contract_drift
```

### D.9.5.2 Severity computation (minimal reference implementation)

```python
def kind_severity(old_k, new_k):
    if old_k is None: return "additive"
    if new_k is None: return "breaking"

    # breaking fields
    if old_k["emit"] != new_k["emit"]:
        return "breaking"
    if old_k.get("windowable") and not new_k.get("windowable"):
        return "breaking"

    # slots diff
    old_slots = old_k["slots"]; new_slots = new_k["slots"]
    # removed required slot => breaking
    for slot, spec in old_slots.items():
        if spec.get("optional"): 
            continue
        if slot not in new_slots:
            return "breaking"

    # changed required slot semantics => breaking
    for slot, spec in old_slots.items():
        if slot not in new_slots: 
            continue
        if not spec.get("optional"):
            if spec["extract"] != new_slots[slot]["extract"]:
                return "breaking"
            if bool(spec.get("optional")) != bool(new_slots[slot].get("optional")):
                # required→optional is additive-ish; optional→required is breaking
                if spec.get("optional") and not new_slots[slot].get("optional"):
                    return "breaking"

    # additive if new optional slots appear
    for slot, spec in new_slots.items():
        if slot not in old_slots and spec.get("optional"):
            return "additive"

    return "patch"
```

---

## D.9.6 Node-types drift integration (grammar upgrade risk signal)

Even if contracts don’t change, a grammar upgrade that changes `node-types.json` is a **real semantic risk** because `node-types.json` “provides structured data about every possible syntax node in a grammar” and node kinds are uniquely identified by `(type, named)`. ([tree-sitter.github.io][4])

**Freeze** should record:

* `node_types_sha256`
* (optional) `node_types_summary.json` with the full `(type,named)` set

**Diff** should report:

* added node types
* removed node types
* changed node type definitions (fields/children/subtypes changes)

This becomes a “secondary section” in the drift report (not keyed by cq.kind), but you can also annotate kinds whose patterns mention removed node types (if you store pattern snippets/hashes).

---

## D.9.7 CI gating (how this becomes operationally real)

### D.9.7.1 CI jobs (minimum)

1. `lint_query_packs`
2. `freeze_contract` (writes freeze to a temp dir)
3. `diff_contract_against_main` (compares to baseline freeze committed in repo)
4. `enforce_semver_bump` (registry_version must satisfy drift severity)

### D.9.7.2 Enforcement rules

* If diff has **breaking** changes → registry must bump **major**
* If diff has **additive** changes → registry must bump **minor** (unless already major)
* If diff has only **patch** → registry patch bump optional, but freeze update must be committed or explicitly waived

---

## D.9.8 Grammar upgrade runbook (the “human loop”)

1. **Bump grammar** (e.g., `tree-sitter-rust` version).
2. Run `freeze` on branch → new freeze dir.
3. Run `diff` vs baseline freeze → read `contract_drift_report.md`.
4. If breaking/additive:

   * update packs and/or registry until contract matches intent
   * bump `registry_version` accordingly
   * re-freeze + re-diff until clean
5. Commit:

   * updated packs
   * updated registry
   * updated freeze snapshot
   * drift report (optional, but very helpful in PR)

---

## D.9.9 One key incremental-indexing constraint to keep front-of-mind

Your whole “windowable” promise depends on byte-range execution semantics:

* `QueryCursor.set_byte_range` returns matches that **intersect** the range (captures may extend outside). ([tree-sitter.github.io][3])
* `QueryCursor.set_containing_byte_range` returns only matches **fully contained** in the range (added in v0.26.0). ([tree-sitter.github.io][3])

Therefore, treat any change that makes a previously-windowable pattern non-local/non-rooted as **breaking**, because it undermines your incremental reindex correctness/perf guarantees. ([tree-sitter.github.io][2])

If you want the next deep dive after this: I can propose a **“contract drift triage playbook”** (how to quickly localize which `.scm` pattern caused each `cq.kind` drift using `start_byte_for_pattern/end_byte_for_pattern` + pattern snippet hashing + a suggested “fix ladder” for restoring windowability).

[1]: https://tree-sitter.github.io/tree-sitter/using-parsers/queries/3-predicates-and-directives.html "Predicates and Directives - Tree-sitter"
[2]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Query.html "Query — py-tree-sitter 0.25.2 documentation"
[3]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.QueryCursor.html "QueryCursor — py-tree-sitter 0.25.2 documentation"
[4]: https://tree-sitter.github.io/tree-sitter/using-parsers/6-static-node-types "Static Node Types - Tree-sitter"
[5]: https://tree-sitter.github.io/py-tree-sitter/ "py-tree-sitter — py-tree-sitter 0.25.2 documentation"

According to a document from **April 29, 2022**, GitHub’s “search-based code navigation” pipeline uses **Tree-sitter tag queries** (tested via `tree-sitter tags`) to extract **jump-to-definition** and **find-all-references** data when new commits/PRs are pushed. ([The GitHub Blog][1])

# E) Symbol tagging (TAGS_QUERY) as a “Rust definition index”

This chapter is about using **Tree-sitter “tags” queries** (`queries/tags.scm`, often exposed as `TAGS_QUERY`) as a **coarse but extremely high-ROI definition/reference index** for Rust: *what symbols exist? where are they defined? where are they referenced?* — without full semantic resolution.

---

## E.0 Quick gotchas (high-impact)

1. **Tagging ≠ resolution.** Tagging is “identify entities that can be named” + “identify reference sites,” not “prove which definition each reference resolves to.” Tree-sitter’s docs describe tags as a code-navigation system driven by queries and captures. ([Tree-sitter][2])

2. **The contract is capture names.** The tags system’s convention is:

* outer capture: `@role.kind` where `role ∈ {definition, reference}`
* inner capture: **always** `@name` for the identifier text
* optional: `@doc` for docstrings ([Tree-sitter][2])

3. **The tags engine supports doc helpers** `#select-adjacent!` and `#strip!` (for cleaning docstrings + attaching adjacent doc comments). These are described as built-ins “for convenience purposes” in the tagging system docs. ([Tree-sitter][2])
   *In pure Python `QueryCursor` usage, do not assume these directives are executed unless you’re using an engine that implements them (the Tree-sitter CLI and `tree-sitter-tags` do).*

4. **Where tags live + how they’re wired**:

* Default location: `queries/tags.scm` ([Tree-sitter][2])
* `tree-sitter.json` can point to it via a `tags` query path key (default `queries/tags.scm`). ([Tree-sitter][2])

---

## E.1 What TAGS_QUERY is (spec + capture vocabulary)

### E.1.1 “Code Navigation Systems” spec (the authoritative model)

Tree-sitter’s docs describe a code navigation system based on the `tree-sitter tags` command, which outputs a textual dump of “interesting syntactic nodes.” ([Tree-sitter][2])

**Core data model (your symbol index is just a structured version of this):**

* role: definition vs reference
* kind: e.g., class/function/module/call
* name: extracted via `@name` capture ([Tree-sitter][2])

**Standard vocabulary** (common denominator for consumers like GitHub):

* `@definition.class`, `@definition.function`, `@definition.interface`, `@definition.method`, `@definition.module`
* `@reference.call`, `@reference.class`, `@reference.implementation` ([Tree-sitter][2])

**Important nuance:** the docs explicitly say consumers may extend or recognize subsets, but standardization is desirable. ([Tree-sitter][2])

### E.1.2 TAGS_QUERY in the wild: “languages extend”

Even if you stick to the standard set for GitHub integration, many grammars emit additional tags (e.g., `@definition.type`, `@reference.type`)—see Go’s `TAGS_QUERY` constant as an example (it includes doc capture + stripping + adjacency helpers, and emits type defs/refs). ([Docs.rs][3])

**Design posture (recommended):**

* **External compatibility mode**: emit only the standard vocabulary.
* **Internal richness mode**: emit richer kinds (type alias, const/static, macro, etc.) but treat them as *optional* for downstream consumers.

---

## E.2 Rust: what you should tag (definitions + references)

### E.2.1 Rust “definition” surface (what becomes a Symbol row)

A production Rust tags pack usually aims to tag:

**Type-ish defs**

* `struct_item`, `enum_item`, `union_item` → `@definition.class` (or `@definition.type` in internal mode)
* `trait_item` → `@definition.interface`

**Callable defs**

* `function_item` → `@definition.function`
* methods inside `impl_item` still appear as `function_item` in the CST → you decide whether to classify as `function` vs `method` based on container context (see E.5.3).

**Module defs**

* `mod_item` → `@definition.module`

**Macro defs**

* `macro_definition` → internal: `@definition.macro` (non-standard; keep optional)

### E.2.2 Rust “reference” surface

**Calls**

* `call_expression` (free fn call, method call, UFCS-ish shapes) → `@reference.call`
* `macro_invocation` → often treated as `@reference.call` for navigation purposes (even though it’s not a function call semantically)

**Type references**

* `type_identifier`, `scoped_type_identifier`, `generic_type` → internal: `@reference.type` (non-standard; keep optional)

---

## E.3 A production-grade Rust `queries/tags.scm` (two modes)

### E.3.1 Compatibility mode (stick to the documented standard vocabulary)

This keeps you aligned with the documented tag classes. ([Tree-sitter][2])

```scm
; -----------------------------
; Type / interface definitions
; -----------------------------
(struct_item name: (type_identifier) @name) @definition.class
(enum_item   name: (type_identifier) @name) @definition.class
(union_item  name: (type_identifier) @name) @definition.class

(trait_item  name: (type_identifier) @name) @definition.interface

; -----------------------------
; Module definitions
; -----------------------------
(mod_item name: (identifier) @name) @definition.module

; -----------------------------
; Function definitions
; -----------------------------
(function_item name: (identifier) @name) @definition.function

; -----------------------------
; Call references (free + method)
; -----------------------------
(call_expression
  function: (identifier) @name
) @reference.call

(call_expression
  function: (field_expression field: (field_identifier) @name)
) @reference.call

; -----------------------------
; Macro invocations treated as calls
; -----------------------------
(macro_invocation macro: (_) @name) @reference.call
```

### E.3.2 Internal richness mode (add types, macro defs, etc.)

Use this only if your downstream consumer can tolerate extra kinds.

```scm
(type_item name: (type_identifier) @name) @definition.type
(type_identifier) @name @reference.type

(macro_definition name: (identifier) @name) @definition.macro
```

*(Again: Go’s TAGS_QUERY shows real-world usage of doc capture + directives and additional kinds like `definition.type` / `reference.type`.) ([Docs.rs][3])*

---

## E.4 CLI workflow: validate tags quickly (and write conformance tests)

### E.4.1 Run tags locally (`tree-sitter tags`)

Tree-sitter’s docs show the canonical dev loop: run `tree-sitter tags <files...>` to test your `tags.scm`, and it prints the matched entities’ **name, role, location, first line, and docstring**. ([Tree-sitter][2])

```bash
# install CLI
cargo install tree-sitter-cli --locked

# run tags against Rust
tree-sitter tags path/to/file.rs
```

The docs also state the expected location: `queries/tags.scm`. ([Tree-sitter][2])

### E.4.2 Unit testing (`test/tags/` + `tree-sitter test`)

Tree-sitter’s docs: tags queries may be tested with `tree-sitter test`, and files under `test/tags/` use the same comment assertion system as highlights tests. ([Tree-sitter][2])

**Rust tags test pattern (shape):**

```text
fn foo() {}
#  ^ definition.function
```

---

## E.5 From tags → symbol tables (Rust symbol catalog)

### E.5.1 Canonical “Tag” record shape (borrow this)

The `tree-sitter-tags` library makes the implicit schema explicit: tags have a `kind`, a `range`, a `name_range`, and extracted `docs`. ([Docs.rs][4])

Treat your Python output schema similarly:

```python
@dataclass(frozen=True)
class TagEvent:
    kind: str          # e.g. "definition.function", "reference.call"
    anchor_span: tuple[int, int]     # the outer captured node span
    name_span: tuple[int, int]       # span of @name node
    name_text: str
    docs: str | None   # optional
```

### E.5.2 Python extraction: compile tags.scm and emit TagEvents

You already have all the generic `Query/QueryCursor` mechanics in your base docs; here’s the tags-specific binding logic:

**Rules:**

* Outer tag capture is the event classifier (`definition.*` / `reference.*`)
* Inner `name` capture is the symbol text
* Optionally `doc` capture exists (but see E.5.4 on repeated captures)

```python
from importlib.metadata import distribution
from pathlib import Path
from tree_sitter import Language, Parser, Query, QueryCursor
import tree_sitter_rust as tsrust

# 1) load grammar
RUST = Language(tsrust.language())
parser = Parser(RUST)

# 2) locate tags.scm inside the installed distribution
dist = distribution("tree-sitter-rust")
tags_path: Path | None = None
for f in dist.files or []:
    if str(f).endswith("queries/tags.scm"):
        tags_path = dist.locate_file(f)
        break
assert tags_path is not None

tags_src = tags_path.read_text("utf8")
q = Query(RUST, tags_src)
cur = QueryCursor(q)

def node_text(src: bytes, n) -> str:
    return src[n.start_byte:n.end_byte].decode("utf8", errors="replace")

def extract_tags(src: bytes):
    tree = parser.parse(src)
    for pattern_i, caps in cur.matches(tree.root_node):
        # Find the outer tag capture key:
        tag_kind = next((k for k in caps.keys()
                         if k.startswith("definition.") or k.startswith("reference.")),
                        None)
        name_node = caps.get("name")
        if not tag_kind or not name_node:
            continue

        anchor = caps[tag_kind]
        yield {
            "kind": tag_kind,
            "anchor_span": (anchor.start_byte, anchor.end_byte),
            "name_span": (name_node.start_byte, name_node.end_byte),
            "name_text": node_text(src, name_node),
            # docs handled separately unless you’re using a tags engine that implements #strip!/adjacency
            "docs": None,
            "pattern_i": pattern_i,
        }
```

### E.5.3 Container item derivation (Rust-specific “method vs function” + nesting)

Tags give you a **flat stream**. Your symbol catalog needs **context**.

**Container algorithm (byte-span canonical):**

1. For each definition anchor node, walk parents until you hit a “container item” (`impl_item`, `trait_item`, `mod_item`, `function_item`, etc).
2. Record `(container_kind, container_name?, container_span)`.

**Method classification heuristic:**

* if a `function_item` is inside an `impl_item` or `trait_item`, classify as `method` (even though CST node is still `function_item`).

This directly supports “Symbols pane” UX (methods under impl/trait) without requiring typechecking.

### E.5.4 Docs + attrs enrichment (do NOT rely on `#strip!/#select-adjacent!` in pure Python)

Tree-sitter’s tagging docs describe `@doc` plus helper directives `#strip!` and `#select-adjacent!` to attach docstrings and clean comment prefixes. ([Tree-sitter][2])
But unless your Python tags runner implements these directives, treat docs/attrs as a **second pass**:

* reuse your `40_attrs_docs` pack (from your standardized Rust index packs) to extract:

  * `attribute_item` / `inner_attribute_item`
  * doc comments (line/block) and normalize markers
* attach them to the next definition by byte order (the “leading trivia buffer” approach from your Rust CST taxonomy chapter)

This has two major benefits:

* avoids repeated `@doc` captures (which can be lossy if you use `matches()`)
* preserves **windowability**: doc extraction is its own pack that you can re-run on changed ranges

---

## E.6 Stable IDs (byte-span canonical)

You asked for stable IDs derived from:

> `(path, item-kind, name-span, container-span)` using byte spans as canonical anchors.

That is exactly the right “snapshot identity” key for deterministic builds.

```python
import hashlib

def stable_symbol_id(
    file_relpath: str,
    item_kind: str,                     # e.g. "definition.function"
    name_span: tuple[int, int],          # (bstart, bend)
    container_span: tuple[int, int] | None,
) -> str:
    cs = container_span or (-1, -1)
    payload = f"{file_relpath}\0{item_kind}\0{name_span[0]}:{name_span[1]}\0{cs[0]}:{cs[1]}".encode("utf8")
    return hashlib.blake2b(payload, digest_size=16).hexdigest()
```

**Why this matches the tags ecosystem:**

* tags are fundamentally about **names** and **ranges** (the CLI prints locations; and `tree-sitter-tags` exposes `range` + `name_range`). ([Tree-sitter][2])
* byte spans are your canonical join key across CST-derived tables (attrs/docs, defs, refs, etc.).

---

## E.7 Where this fits in your overall Rust indexing stack

* **TAGS_QUERY** is the fastest route to a “symbols index” (defs + call-ish refs) suitable for navigation UIs.
* Your **00_defs/10_refs/20_calls/30_types/40_attrs_docs** packs are the “production semantic index” that can exceed tags in precision and schema richness.
* The clean integration is:

  * **tags → bootstrap symbol catalog** (fast, broad)
  * **index packs → enrich + normalize** (attrs/docs/visibility/container classification + later resolution stages)

Tree-sitter’s docs explicitly frame tags as a code navigation system built on queries and captures; GitHub’s docs describe code navigation as linking definitions and references (and note it uses tree-sitter). ([Tree-sitter][2])

[1]: https://github.blog/news-insights/product-news/bringing-code-navigation-to-communities/ "Bringing code navigation to communities - The GitHub Blog"
[2]: https://tree-sitter.github.io/tree-sitter/print.html "Tree-sitter"
[3]: https://docs.rs/tree-sitter-go/latest/tree_sitter_go/constant.TAGS_QUERY.html?utm_source=chatgpt.com "TAGS_QUERY in tree_sitter_go - Rust"
[4]: https://docs.rs/tree-sitter-tags "tree_sitter_tags - Rust"

According to a document from **July 15, 2025**, Helix summarizes the core “injections” idea as: an `injections.scm` query marks ranges that should be treated as a different language, then the editor runs another Tree-sitter parser *for that range* and nests that tree under the host tree (a “tree of trees”). ([Helix Editor][1])

# F) Injections query for Rust (embedded languages in macro/string contexts)

This section assumes you already understand the basic injection capture vocabulary; here we focus on **Rust-specific hotspot engineering** (SQL/regex/html) and a **Python implementation** that preserves **host byte coordinates** end-to-end.

---

## F.0 Why injections exist + how grammars declare them

### F.0.1 Injections are a standard query pack (like highlights/locals)

Tree-sitter treats injections as a first-class query pack, discovered via `tree-sitter.json`’s `injections` path (default `queries/injections.scm`). ([Tree-sitter][2])

Key `tree-sitter.json` knobs you should mirror in your *Python* registry layer:

* `injection-regex`: used by the CLI/editor engines to decide if a language is eligible as an injection target. ([Tree-sitter][2])
* query paths: `highlights`, `locals`, `injections` (all relative to the `tree-sitter.json` directory). ([Tree-sitter][2])

### F.0.2 The injection “spec surface” you must implement (in Python)

Tree-sitter defines injections via:

* captures:

  * `@injection.content`
  * `@injection.language` ([Tree-sitter][2])
* properties (`#set!`-driven pattern settings):

  * `injection.language` (force language name)
  * `injection.combined` (parse all matching nodes as one nested document)
  * `injection.include-children` (include child text; default is to exclude child text)
  * `injection.self`, `injection.parent` ([Tree-sitter][2])

Neovim’s docs add one more capture that is extremely useful in practice:

* `@injection.filename` → derive language from filename/filetype mapping. ([Neovim][3])
  Helix also documents `@injection.filename` and `@injection.shebang` as extensions; you can adopt the same semantics in your Python pipeline if you maintain a filetype→Language registry. ([Helix Editor][4])

---

## F.1 What Rust ships by default (baseline you will fork/extend)

`tree-sitter-rust`’s built-in `INJECTIONS_QUERY` is intentionally minimal: it injects **Rust into Rust macro token trees** and macro_rules token trees:

```text
((macro_invocation
  (token_tree) @injection.content)
 (#set! injection.language "rust")
 (#set! injection.include-children))

((macro_rule
  (token_tree) @injection.content)
 (#set! injection.language "rust")
 (#set! injection.include-children))
```

([Docs.rs][5])

**Interpretation:** this is not about SQL/regex/HTML at all; it’s a “macro body is Rust tokens” policy. Your SQL/regex/HTML injections are **additional patterns** (often in a forked/derived `injections.scm`), usually *more specific* than the generic macro rule.

---

## F.2 Rust-specific injection hotspots (and the query patterns you actually want)

### F.2.1 SQL injection: `sqlx::query!` / `query_as!` (macro token trees → SQL)

**Why this is injectable:** `sqlx::query!()` requires the SQL be a **string literal** (or concatenation of string literals using `+`) to support compile-time introspection. ([Docs.rs][6])

**Practical rule:** capture **`string_content`** (not `string_literal`) so the injected parser sees *only SQL*, not Rust quotes.

#### A) Minimal `sqlx` macro injection (string_literal + raw_string_literal)

```scm
; queries_rust_injections/10_sql.scm

(
  (macro_invocation
    macro: (scoped_identifier
             path: (identifier) @sqlx.path (#eq? @sqlx.path "sqlx")
             name: (identifier) @sqlx.name
               (#any-of? @sqlx.name "query" "query_as" "query_scalar" "query_file" "query_file_as"))
    (token_tree
      [
        (string_literal (string_content) @injection.content)
        (raw_string_literal (string_content) @injection.content)
      ])
  )
  (#set! injection.language "sql")
)
```

* `#eq?` / `#any-of?` are standard predicates used broadly in injection authoring. ([Helix Editor][4])

#### B) Concatenated SQL strings: use `injection.combined`

If you want to support `"... " + "..."`

* allow **multiple** `@injection.content` captures and set `injection.combined`
* your Python engine must then parse multiple ranges as one nested document (see F.4.3).

Tree-sitter defines `injection.combined` exactly for this. ([Tree-sitter][2])

```scm
(
  (macro_invocation
    macro: (scoped_identifier
             path: (identifier) @sqlx.path (#eq? @sqlx.path "sqlx")
             name: (identifier) @sqlx.name (#any-of? @sqlx.name "query" "query_as"))
    (token_tree
      [
        (string_literal (string_content) @injection.content)
        (raw_string_literal (string_content) @injection.content)
      ]+)
  )
  (#set! injection.language "sql")
  (#set! injection.combined)
)
```

**Footgun:** query arguments may include other strings (rare, but possible). If you need “only the SQL argument,” your engine can post-filter by choosing the earliest string_content span inside the macro invocation, or by scanning top-level commas in the `token_tree`.

---

### F.2.2 SQL injection: `diesel::sql_query(...)` (call_expression → SQL)

Diesel’s `sql_query` is explicitly “construct a full SQL query using raw SQL” and takes something `Into<String>`. ([Docs.rs][7])

**Injection posture:** inject only when the argument is a literal/raw literal (you cannot infer runtime `format!`).

```scm
; queries_rust_injections/11_diesel_sql.scm

(
  (call_expression
    function: [
      (identifier) @fn (#eq? @fn "sql_query")
      (scoped_identifier name: (identifier) @fn (#eq? @fn "sql_query"))
    ]
    arguments: (arguments
      [
        (string_literal (string_content) @injection.content)
        (raw_string_literal (string_content) @injection.content)
      ])
  )
  (#set! injection.language "sql")
)
```

---

### F.2.3 Regex injection: `regex!` / `lazy_regex!` (macro token trees → regex grammar)

The `lazy-regex` crate documents `regex!` and `lazy_regex!` macros that compile regexes (checked at compile time, lazily built). ([Docs.rs][8])

**Injection posture:** inject the pattern string as a “regex” language (your registry decides the actual Language name—some ecosystems use `regex`, `regexp`, etc).

```scm
; queries_rust_injections/20_regex.scm

(
  (macro_invocation
    macro: (identifier) @m (#any-of? @m "regex" "lazy_regex" "bytes_regex" "bytes_lazy_regex")
    (token_tree
      [
        (string_literal (string_content) @injection.content)
        (raw_string_literal (string_content) @injection.content)
      ])
  )
  (#set! injection.language "regex")
)
```

---

### F.2.4 HTML injection: `yew::html! { ... }` (token trees → HTML-like syntax)

Yew states the `html!` macro lets you write **HTML and SVG declaratively**, JSX-like, and it mixes literals and Rust expressions (`{ ... }`). ([yew.rs][9])

This is exactly the case where “**exclude child text by default**” is valuable:

* Tree-sitter defines `injection.include-children`: if **unset**, child node text is excluded from the injected document; if **set**, it’s included. ([Tree-sitter][2])

#### Option A) Inject whole token_tree as HTML (best-effort)

```scm
; queries_rust_injections/30_yew_html.scm

(
  (macro_invocation
    macro: [
      (identifier) @m (#eq? @m "html")
      (scoped_identifier name: (identifier) @m (#eq? @m "html"))
    ]
    (token_tree) @injection.content
  )
  (#set! injection.language "html")
  ; IMPORTANT: do NOT set injection.include-children if you want Rust `{...}` groups excluded.
)
```

**Reality check:** plain HTML parsers often choke on Yew’s `{ expr }` and component syntax. The “exclude children” behavior can make this workable *if* the macro’s token_tree children correspond to embedded Rust expression groups—but it yields **disjoint (hole-punched) ranges**, which means you must parse via `included_ranges` (F.4.3) to preserve coordinates.

#### Option B) Comment/filename-driven HTML injection for raw strings (very robust)

If you prefer “HTML lives in a raw string literal” patterns (templating or embedded docs), use `@injection.filename` / `@injection.language` style triggers (mirroring Neovim/Helix semantics). ([Neovim][3])

---

## F.3 Python engine: from injections query → parse plan → embedded trees

### F.3.1 Compile the injection query and read pattern properties

Pattern properties are set with `#set!` and retrieved via `Query.pattern_settings(i)`. ([Tree-sitter][10])

```python
from __future__ import annotations
from dataclasses import dataclass
from tree_sitter import Language, Parser, Query, QueryCursor, Range, Point

@dataclass(frozen=True)
class InjectionSpec:
    lang_name: str
    combined: bool
    include_children: bool
    content_nodes: list  # list[Node]
    # optional: language/filename nodes for debugging
```

### F.3.2 Prefer `matches()` for injection (you need pattern_i → settings)

Modern py-tree-sitter reports `Query.matches(...)` as `list[tuple[int, dict[str, list[Node]]]]`, so repeated captures (multiple `@injection.content`) are representable. ([GitHub][11])

```python
def collect_injections(q: Query, root) -> list[tuple[int, dict[str, list]]]:
    cur = QueryCursor(q)
    return cur.matches(root)  # [(pattern_i, {"injection.content":[...], ...}), ...]
```

---

## F.4 Parsing the injected language and keeping host coordinates

This is the core: **don’t lose byte coordinates**. You want injected trees whose nodes’ `.start_byte/.end_byte` live in the **host Rust file coordinate space** so you can join them back to Rust spans directly.

### F.4.1 Strategy A (recommended): parse over host bytes using `included_ranges`

In py-tree-sitter, `Parser` accepts `included_ranges` (a list of `Range` objects) and exposes them as `Parser.included_ranges`. ([Tree-sitter][12])

The underlying Tree-sitter core semantics (Rust docs) are exactly what you want: included ranges let you parse only a portion (or **multiple disjoint portions**) while returning a tree whose ranges match the document as a whole, with the constraint that ranges must be ordered and non-overlapping. ([Docs.rs][13])

```python
def to_range(n) -> Range:
    return Range(n.start_point, n.end_point, n.start_byte, n.end_byte)

def parse_injected_over_host_bytes(
    embedded_lang: Language,
    host_bytes: bytes,
    ranges: list[Range],
) :
    p = Parser(embedded_lang, included_ranges=ranges)
    return p.parse(host_bytes)
```

#### Handling `injection.combined`

* Tree-sitter: `injection.combined` means parse all matching nodes as one nested document. ([Tree-sitter][2])
* Implementation: build **multiple disjoint ranges** and parse once.

```python
def parse_combined(embedded_lang, host_bytes, content_nodes):
    ranges = [to_range(n) for n in sorted(content_nodes, key=lambda n: n.start_byte)]
    # must be ordered + non-overlapping (Tree-sitter requirement)
    return parse_injected_over_host_bytes(embedded_lang, host_bytes, ranges)
```

#### Handling “exclude children” (default) vs `injection.include-children`

Tree-sitter defines that, by default, child nodes’ text is excluded from the injected document; `injection.include-children` flips that. ([Tree-sitter][2])

If you capture a **container node** (e.g., a `token_tree`) and want to exclude its children (for Yew/templating), compute **hole-punched included ranges** by subtracting child spans:

```python
def hole_punched_ranges(content_node) -> list[Range]:
    # include content_node bytes except for its direct children spans
    children = list(content_node.children)  # or named_children depending on your policy
    children.sort(key=lambda c: c.start_byte)

    out: list[Range] = []
    cur_b = content_node.start_byte
    cur_p = content_node.start_point

    for ch in children:
        if cur_b < ch.start_byte:
            out.append(Range(cur_p, ch.start_point, cur_b, ch.start_byte))
        cur_b = max(cur_b, ch.end_byte)
        cur_p = ch.end_point

    if cur_b < content_node.end_byte:
        out.append(Range(cur_p, content_node.end_point, cur_b, content_node.end_byte))

    return [r for r in out if r.start_byte < r.end_byte]
```

This is the practical way to make Yew/templating injections viable without rewriting the content into a new buffer.

### F.4.2 Strategy B (fallback): parse extracted bytes, then shift the tree with `root_node_with_offset`

If your injected content is a single contiguous slice and you parse it standalone, py-tree-sitter lets you shift node coordinates forward using `Tree.root_node_with_offset(offset_bytes, offset_extent)`; `Point(row, column)` models offset_extent. ([Tree-sitter][14])

```python
def parse_standalone_with_offset(parser: Parser, host_bytes: bytes, content_node):
    sub = host_bytes[content_node.start_byte:content_node.end_byte]
    t = parser.parse(sub)

    # Shift root node into host coordinate space
    shifted_root = t.root_node_with_offset(
        content_node.start_byte,
        Point(content_node.start_point[0], content_node.start_point[1]),
    )
    return t, shifted_root
```

**Limitation:** this cannot represent “exclude children” (hole-punched content) or `combined` injections across disjoint ranges; use Strategy A for that.

---

## F.5 Minimal “injection planner” loop (end-to-end)

```python
def resolve_injection_language(settings, caps, host_bytes, registry) -> str | None:
    # 1) forced injection.language property
    forced = settings.get("injection.language")
    if forced:
        return forced

    # 2) capture-driven language name
    lang_nodes = caps.get("injection.language") or []
    if lang_nodes:
        n = lang_nodes[0]
        return host_bytes[n.start_byte:n.end_byte].decode("utf8", "replace")

    # 3) filename-driven (Neovim/Helix semantics)
    fn_nodes = caps.get("injection.filename") or []
    if fn_nodes:
        filename = host_bytes[fn_nodes[0].start_byte:fn_nodes[0].end_byte].decode("utf8", "replace")
        return registry.language_for_filename(filename)

    return None

def run_injections(host_lang: Language, inj_scm: str, host_bytes: bytes, host_tree, registry):
    q = Query(host_lang, inj_scm)
    cur = QueryCursor(q)

    for pattern_i, caps in cur.matches(host_tree.root_node):
        settings = q.pattern_settings(pattern_i)  # #set! properties :contentReference[oaicite:24]{index=24}
        lang_name = resolve_injection_language(settings, caps, host_bytes, registry)
        if not lang_name:
            continue

        content_nodes = caps.get("injection.content") or []
        if not content_nodes:
            continue

        combined = "injection.combined" in settings
        include_children = "injection.include-children" in settings

        embedded_lang = registry.resolve(lang_name)
        if embedded_lang is None:
            continue

        # build ranges
        if combined:
            # if include_children, ranges are each node’s full span; if not, hole-punch each
            ranges = []
            for n in content_nodes:
                if include_children:
                    ranges.append(to_range(n))
                else:
                    ranges.extend(hole_punched_ranges(n))
            ranges.sort(key=lambda r: r.start_byte)
            embedded_tree = parse_injected_over_host_bytes(embedded_lang, host_bytes, ranges)
        else:
            # one tree per content node
            for n in content_nodes:
                ranges = [to_range(n)] if include_children else hole_punched_ranges(n)
                embedded_tree = parse_injected_over_host_bytes(embedded_lang, host_bytes, ranges)
                yield (lang_name, n.start_byte, n.end_byte, embedded_tree)
            continue

        yield (lang_name, None, None, embedded_tree)
```

This implements the same injection semantics described by Tree-sitter (`@injection.content`, `@injection.language`, `injection.*` properties). ([Tree-sitter][2])

---

## F.6 What you should persist (index artifacts)

Treat injections as first-class facts you can join to everything else:

* `host_file`
* `injection_language` (normalized)
* `policy_flags`: `combined`, `include_children`
* `content_spans`: list of `(bstart,bend)` (multiple if combined)
* `parse_ranges`: the actual `included_ranges` you fed the embedded parser (critical for debugging; required to be ordered/non-overlapping in the core library). ([Docs.rs][13])
* `embedded_tree_root_span` (host coordinates) for sanity checks

If you want the next deep dive after this: **G) Macro reality** (macros are not expanded; how to build “useful enough” injection + tagging + partial parsing for macro-heavy Rust codebases).

[1]: https://helix-editor.com/news/release-25-07-highlights/ "
      Release 25.07 Highlights |  Helix
    "
[2]: https://tree-sitter.github.io/tree-sitter/3-syntax-highlighting.html "Syntax Highlighting - Tree-sitter"
[3]: https://neovim.io/doc/user/treesitter.html " Treesitter - Neovim docs"
[4]: https://docs.helix-editor.com/guides/injection.html "Adding injection queries"
[5]: https://docs.rs/tree-sitter-rust/latest/tree_sitter_rust/constant.INJECTIONS_QUERY.html "INJECTIONS_QUERY in tree_sitter_rust - Rust"
[6]: https://docs.rs/sqlx/latest/sqlx/macro.query.html "query in sqlx - Rust"
[7]: https://docs.rs/diesel/latest/diesel/fn.sql_query.html "sql_query in diesel - Rust"
[8]: https://docs.rs/lazy-regex "lazy_regex - Rust"
[9]: https://yew.rs/docs/concepts/html "HTML | Yew"
[10]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Query.html?utm_source=chatgpt.com "Query — py-tree-sitter 0.25.2 documentation"
[11]: https://github.com/tree-sitter/py-tree-sitter/releases?utm_source=chatgpt.com "Releases · tree-sitter/py-tree-sitter"
[12]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Parser.html "Parser — py-tree-sitter 0.25.2 documentation"
[13]: https://docs.rs/tree-sitter/%2A/tree_sitter/struct.Parser.html "Parser in tree_sitter - Rust"
[14]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Tree.html "Tree — py-tree-sitter 0.25.2 documentation"

# G) Macro reality (the biggest Rust-specific limitation)

Rust macros are **token → token transforms** that the *compiler / analyzer* must expand to produce the “real” AST/HIR. Tree-sitter, by design, operates on the **original source text** and gives you a CST of what’s *written*, not what it *expands into*. The Rust compiler explicitly describes macro expansion as the process that iteratively expands macros until you have a crate AST “with no unexpanded macros.” ([rustc-dev-guide.rust-lang.org][1])

---

## G.0 What “not expanded” means in practice

### G.0.1 Tree-sitter shows the *macro surface syntax*, not generated items

For Rust, that means you can reliably see:

* **macro invocations**: `foo!(...)`, `crate::foo!(...)`, `#[attr]`, `#[derive(...)]`
* the **token_tree** arguments (balanced delimiters, nested token trees)
* the **literal arguments** inside the token tree (string/char/int/etc)

…but you **cannot** see:

* the generated `impl` blocks from `#[derive(...)]`
* the rewritten function body/signature from `#[tokio::main]`-style attribute macros
* the generated methods/types from `macro_rules!` expansions

rust-analyzer’s macro writeup makes this concrete: an attribute like `tokio::main` is “an opaque bit of code” that takes the tokens of the annotated item as input and produces tokens that replace the original item—meaning the “real program” is the expansion, not the surface. ([rust-analyzer.github.io][2])

### G.0.2 Why this is inherently hard (and why Tree-sitter doesn’t “just do it”)

Macro expansion is not a “syntax tweak” — it’s a compilation phase with project context (crate graph, cfg/features, proc-macro server, dependencies). The compiler expands macros to integrate the expansion into the crate AST. ([rustc-dev-guide.rust-lang.org][1])
That’s outside the scope of Tree-sitter’s injection/highlight/query pipeline (which is designed around parsing *the text you have* and optionally re-parsing *subranges* as nested languages). ([Tree-sitter][3])

---

## G.1 Failure modes you must assume (index correctness boundaries)

### G.1.1 “Missing defs” and “phantom refs”

Common high-impact cases:

* `#[derive(Serialize, Deserialize)]` generates trait impls and helper code → your defs/refs/calls index will look “missing” unless you use an expansion source. (Tree-sitter will only see the attribute and the struct/enum item.)
* Attribute macros can *rewrite control flow* and even identifiers → static “call graphs” from Tree-sitter will be wrong in macro-heavy code. ([rust-analyzer.github.io][2])
* Macro-heavy frameworks (`tokio::select!`, `yew::html!`, `leptos::view!`) embed “custom languages” or DSL-ish token patterns; Tree-sitter-rust can only parse these as `token_tree` (unless you add injection logic or a grammar fork).

### G.1.2 Nested macros amplify ambiguity

Inside a `token_tree`, tokens can be “Rust-like” or “DSL-like,” depending on the macro. Tree-sitter-rust intentionally does not assume it can interpret arbitrary macro bodies as Rust semantics; you get structure, not meaning.

---

## G.2 Mitigation 1: treat macro bodies as opaque, but **index invocation sites** (high ROI, always correct)

This is the “never wrong, always useful” baseline.

### G.2.1 Emit a `macro_invocation` event stream (call-like)

Capture:

* macro path/name (`identifier` or `scoped_identifier`)
* argument span (`token_tree` span)
* container span (file/module/function span for grouping)
* optional “first literal argument” span(s) (for known-macro heuristics later)

Minimal query (fits your `cq.*` pack architecture):

```scm
(macro_invocation
  macro: (_) @call.macro.path
  (token_tree) @call.macro.args
) @call.macro
(#set! cq.pack "20_calls")
(#set! cq.emit "rust_calls")
(#set! cq.kind "call.macro")
(#set! cq.anchor "call.macro")
(#set! cq.windowable)
(#set! cq.slot.path "call.macro.path") (#set! cq.slot.path.extract "span_text")
(#set! cq.slot.args "call.macro.args") (#set! cq.slot.args.extract "span")
```

### G.2.2 Table schema (what you actually persist)

A practical Arrow/Delta row shape:

* `macro_call_id` (stable ID; see G.5)
* `macro_path_text`, `macro_path_span`
* `args_span` (token_tree span)
* `container_def_id` (nearest enclosing def symbol id)
* `file_relpath`, `anchor_bstart/bend`
* `pattern_i`, `pack`

This gives you:

* “Find all uses of `tokio::select!` / `sqlx::query!` / `html!`”
* “Show me the token spans of the arguments”
* stable anchors for later enrichment (SQL parse, expansion parse, etc.)

---

## G.3 Mitigation 2: selective “known macro” parsing (embedded DSLs, string literals, token trees)

This is where Tree-sitter stays valuable **without expansion**: you parse *content* that macros carry.

### G.3.1 The governing model: injections = “parse a captured range with another language”

Tree-sitter injection queries specify:

* `@injection.content` for the range to re-parse
* optional `@injection.language` or `#set! injection.language "..."` to select language
* flags like `injection.combined` and `injection.include-children`. ([Tree-sitter][3])

### G.3.2 A “KnownMacroSpec” registry (don’t hardcode patterns in Python)

Create a small registry that maps macro-path → extraction strategy:

```yaml
known_macros:
  - macro: "sqlx::query"
    lang: "sql"
    arg_policy: "first_string_literal"
  - macro: "diesel::sql_query"
    lang: "sql"
    arg_policy: "first_string_literal"
  - macro: "regex"
    lang: "regex"
    arg_policy: "first_string_literal"
  - macro: "yew::html"
    lang: "html"
    arg_policy: "token_tree_best_effort"
```

Then your injection pack emits `@injection.content` ranges, and your Python “injection planner” resolves:

* target language name
* whether ranges are combined
* included ranges and coordinate mapping

(You already built this for Section F; this is the macro-specific specialization of the same machinery.) ([Tree-sitter][3])

### G.3.3 Known-macro parsing outputs (what to store)

For each injected parse:

* `macro_call_id` (join key back to `rust_calls`)
* `injected_language`
* `host_ranges`: list of `(bstart,bend)` you parsed
* `embedded_tree_artifact`: (optional) serialized CST summary or extracted facts only
* extracted facts tables per language (e.g., `sql_tables`, `sql_columns`, `regex_literals`, `html_tags`)

This gives you meaningful “semantics” in macro-heavy Rust repos even without expansion.

---

## G.4 Mitigation 3: parse `token_tree` as Rust (still not expansion, but unlocks structure inside macro inputs)

Rust grammar ships an **injection query that re-parses macro token trees as Rust**:

* it captures `(token_tree) @injection.content`
* sets `injection.language "rust"`
* sets `injection.include-children` ([Docs.rs][4])

This is valuable for macros whose *input* is Rust-like (e.g., `tokio::select!` arms, many `macro_rules!` invocations), even though you still don’t get the *expanded output*.

### G.4.1 What this gives you

Inside the injected Rust parse of the token tree, you can run:

* your `20_calls` pack to find call sites *inside macro arguments*
* your `10_refs` pack to find identifier/path references
* your `40_attrs_docs` pack to harvest literals

### G.4.2 What it does **not** give you

* Generated functions/types/impls
* Correct name resolution / type inference
* Any guarantee that token-tree text forms a valid Rust subtree (expect parse errors)

### G.4.3 Production rule: treat “token-tree-as-rust” results as **auxiliary evidence**

Store them in a separate stream like `rust_macro_input_facts` and never merge them into the “definitive” call graph without a resolver.

---

## G.5 Stable IDs for macro sites (the anchor you enrich forever)

Use the same principle as your symbol keys:

**Stable macro call ID** = hash of `(file_relpath, macro_path_text, macro_path_span, args_span, container_span)`.

Why:

* token spans are stable under most edits outside the macro site
* it joins cleanly to all enrichments: injected SQL parse, regex parse, rust-analyzer expansion, etc.

---

## G.6 Mitigation 4: optional bridge to rust-analyzer (expansion-derived semantics)

This is the “semantic integration” path. Treat it as **an optional enrichment layer**, not a Tree-sitter feature.

### G.6.1 rust-analyzer provides an explicit Expand Macro LSP extension

rust-analyzer documents an LSP extension:

* Method: `rust-analyzer/expandMacro`
* Params: `{ textDocument, position }`
* Response: `{ name, expansion }` where `expansion` is a string of expanded code. ([Android Source][5])

This is exactly the primitive you need: *given a macro call site*, ask rust-analyzer for the expansion text.

### G.6.2 Minimal JSON-RPC request (what you send)

At a position inside the macro call (typically on the macro name token):

```json
{
  "jsonrpc": "2.0",
  "id": 42,
  "method": "rust-analyzer/expandMacro",
  "params": {
    "textDocument": { "uri": "file:///…/src/lib.rs" },
    "position": { "line": 123, "character": 17 }
  }
}
```

And you’ll receive something like:

```json
{
  "jsonrpc":"2.0",
  "id":42,
  "result":{"name":"sqlx::query","expansion":"…expanded Rust code…"}
}
```

(The method and response shape are per the rust-analyzer extension spec.) ([Android Source][5])

### G.6.3 Getting rust-analyzer initialized correctly (don’t skip this)

rust-analyzer expects configuration in `initializationOptions` (the `"rust-analyzer"` section) because it can’t always fetch configuration during initialization; the LSP extensions doc calls this out explicitly. ([Android Source][5])

**Operationally:** run rust-analyzer with your workspace root (`Cargo.toml`), send `initialize`, then `initialized`, then `textDocument/didOpen` with file contents.

### G.6.4 Mapping Tree-sitter byte spans → LSP positions

Tree-sitter uses byte offsets (`start_byte/end_byte`); LSP uses `(line, character)`.

You already have a bytemap layer in your architecture—use it here:

* convert `macro_path_span.start_byte` → `(line, col)` for the `position`
* keep the macro call stable ID and store the returned expansion as an artifact keyed by that ID

### G.6.5 What you do with expansion text

Once you have `expansion: string`:

1. Parse it with **tree-sitter-rust** as a standalone buffer (it’s Rust code now).
2. Run your existing packs (`00_defs/10_refs/20_calls/30_types/40_attrs_docs`) on the expansion tree.
3. Persist as *separate* tables, e.g.:

   * `rust_macro_expansion_defs`
   * `rust_macro_expansion_calls`
4. Join back to the macro call by `macro_call_id`.

**Important:** the expansion is *not located in the original file*, so spans are in “expansion-buffer coordinates.” If you need source mapping back to the original macro invocation, that becomes a separate, heuristic layer (rust-analyzer’s own macro writeup emphasizes how hard it is to map IDE features between expanded and source code). ([rust-analyzer.github.io][2])

---

## G.7 The production playbook (recommended defaults)

1. **Always** index macro invocations as call-like events (correct + cheap).
2. **Add known-macro injections** for embedded languages (SQL/regex/html) where the macro *inputs* are the value.
3. **Optionally** parse token trees as Rust (macro-input facts), but keep it “aux evidence.”
4. **Only if you need real semantics:** integrate rust-analyzer `expandMacro` and store expansions as separate artifacts keyed by stable macro call IDs. ([Android Source][5])

If you want the next deep dive after this, the natural follow-on is: **H) Module/import graph extraction** (how to build a best-effort module resolver above Tree-sitter, and where rust-analyzer becomes necessary for precise resolution).

[1]: https://rustc-dev-guide.rust-lang.org/macro-expansion.html "Macro expansion - Rust Compiler Development Guide"
[2]: https://rust-analyzer.github.io//blog/2021/11/21/ides-and-macros.html "IDEs and Macros"
[3]: https://tree-sitter.github.io/tree-sitter/3-syntax-highlighting.html "Syntax Highlighting - Tree-sitter"
[4]: https://docs.rs/tree-sitter-rust/latest/tree_sitter_rust/constant.INJECTIONS_QUERY.html?utm_source=chatgpt.com "INJECTIONS_QUERY in tree_sitter_rust - Rust"
[5]: https://android.googlesource.com/toolchain/rustc/%2B/HEAD/src/tools/rust-analyzer/docs/dev/lsp-extensions.md "LSP Extensions"

# H) Module/import graph extraction (Rust project structure above a single file)

Tree-sitter can tell you **what the code *says*** (`mod foo;`, `use a::b::{c, d};`) but it cannot tell you **what files that implies** or **what those paths resolve to**. Rust module loading and Cargo target discovery are *filesystem + manifest rules* that you must implement on top.

This section gives you:

* A **CST extraction layer** (tree-sitter → `ModDecl` + `UseDecl` IR)
* A **Cargo/FS crate-root discovery layer** (targets → root module files)
* A **module resolver** (parent module file + `mod` decl → child file path)
* A **use-tree normalizer** (use syntax → import edges)
* A **graph output contract** (module nodes + edges + unresolved diagnostics)

---

## H.0 What you’re actually building

### H.0.1 The 3 graphs (keep them separate)

1. **Crate graph** *(Cargo-level targets/workspace)*
   Nodes: crate targets (lib/bin/example/test/bench), edges: target depends on packages.
   Source of truth: `cargo metadata` (recommended with `--format-version`). ([Rust Documentation][1])

2. **Module containment graph** *(Rust module tree per crate)*
   Nodes: modules (inline or file-backed), edges: `parent_module ─contains→ child_module`.

3. **Import graph** *(use/re-export bindings per module/block scope)*
   Nodes: modules + “import bindings”, edges: `module ─imports→ path` and `module ─reexports→ path`.

> Key boundary: **Rust compiler decides module file loading**; tree-sitter only provides the syntax you must interpret. Rust Reference explicitly defines the module filename mapping rules and `#[path]` behavior. ([Rust Documentation][2])

---

## H.1 Crate root discovery (Cargo target → root module file)

You cannot build a correct module tree without knowing which files are **crate roots**.

### H.1.1 Default Cargo layout rules (autodiscovery)

Cargo’s project layout guide defines conventional paths: source code in `src/`, default library `src/lib.rs`, default executable `src/main.rs`, and additional binaries in `src/bin/`; examples/benches/tests live in their respective directories. ([Rust Documentation][3])

Cargo targets reference clarifies:

* Library target defaults to `src/lib.rs`
* A binary can be `src/main.rs` and/or stored in `src/bin/` ([Rust Documentation][4])

### H.1.2 Explicit target `path` overrides (must support)

Cargo allows you to set `path = "..."` per target; it is **relative to `Cargo.toml`**. ([Rust Documentation][4])

That means your crate-root discovery must be:

1. Load targets from `cargo metadata` (best)
2. For each target, use the target’s `src_path` (or computed equivalent) as the **crate root file**

### H.1.3 Canonical crate discovery command

Use Cargo’s own machine output; Cargo recommends pinning the schema with `--format-version` and you can restrict to workspace members with `--no-deps`. ([Rust Documentation][1])

```bash
cargo metadata --no-deps --format-version 1
# optionally:
cargo metadata --no-deps --format-version 1 --manifest-path path/to/Cargo.toml
```

**Output you care about:**

* `packages[]` (workspace members)
* each package’s `targets[]` with `kind` + `src_path` (root file)
* `workspace_root` (helps normalize relative paths)

---

## H.2 CST extraction (tree-sitter → declarative “candidate set”)

### H.2.1 Extract `mod` declarations (inline vs external-file modules)

Rust Reference: “A module without a body is loaded from an external file.” ([Rust Documentation][2])

So from CST you need:

* module name
* whether it has an inline body (`mod foo { ... }`)
* attributes on the `mod` item (`#[path="..."]`, `#[cfg(...)]`, etc.)
* visibility (`pub`, `pub(crate)`, …) *(important for public API graphs)*

**Tree-sitter query skeleton (Rust grammar):**

```scm
; mod foo;  (external file)
(mod_item
  name: (identifier) @mod.name
) @mod.item

; mod foo { ... } (inline)
(mod_item
  name: (identifier) @mod.name
  body: (declaration_list) @mod.body
) @mod.inline
```

**Attribute capture (leading trivia)**
Prefer a separate pass (your `40_attrs_docs` machinery) to attach attributes by byte order; then interpret `#[path = "..."]` and `#[cfg(...)]` on `mod_item`.

### H.2.2 Extract `use` declarations as structured trees (not strings)

Rust Reference defines `use` syntax as a `UseTree` with brace-grouping, `self`, `as`, glob, and nested groups. ([Rust Documentation][5])

You need a `UseTreeIR` that preserves:

* prefix segments (`a::b::...`)
* group lists (`{c, d, e::f}`)
* `self` binding
* alias (`as x`)
* glob (`*`)
* visibility (`pub use` re-export)

Rust Reference also notes: use declarations may appear in **modules and blocks** (not just module top level). ([Rust Documentation][5])

**Tree-sitter query anchors:**

```scm
(use_declaration argument: (_) @use.tree) @use.item
```

Then parse `@use.tree` by its node kind (`use_list`, `scoped_use_list`, `use_as_clause`, `use_wildcard`, …) into an IR.

---

## H.3 Module file resolution (the heart of “module graph extraction”)

Tree-sitter gives you `mod foo;`. Rust Reference defines how that maps to files.

### H.3.1 Default filename mapping (no `#[path]`)

Rust Reference rules (canonical):

* “Ancestor module path components are directories” and the module contents are in a file `name.rs`. ([Rust Documentation][2])
* Alternatively, the module can be a directory `name/` with contents in `name/mod.rs`. ([Rust Documentation][2])
* **It is not allowed to have both `name.rs` and `name/mod.rs`.** ([Rust Documentation][2])

### H.3.2 The “module directory” rule (mod-rs vs non-mod-rs parent file)

The Rust Reference’s `#[path]` section formalizes the distinction:

* “mod-rs” source files: crate roots (`lib.rs`/`main.rs`) and modules whose file is named `mod.rs`
* “non-mod-rs” files: all other module files ([Rust Documentation][2])

You should implement a consistent directory rule that matches the Reference examples:

```python
def is_mod_rs(filename: str) -> bool:
    return filename in {"lib.rs", "main.rs", "mod.rs"}

def module_dir_for_file(file_path: Path) -> Path:
    # If parent is mod-rs: module dir is the containing directory.
    if is_mod_rs(file_path.name):
        return file_path.parent
    # If parent is non-mod-rs (foo.rs): module dir is a sibling directory named foo/
    return file_path.parent / file_path.stem
```

Then for `mod child;` (no path attribute):

```python
def candidate_child_files(parent_file: Path, child: str) -> list[Path]:
    base = module_dir_for_file(parent_file)
    return [base / f"{child}.rs", base / child / "mod.rs"]
```

When checking existence:

* 0 matches → unresolved module (likely `cfg` gated or missing file)
* 1 match → resolved
* 2 matches → **hard error** (Rust Reference forbids both). ([Rust Documentation][2])

### H.3.3 `#[path = "..."]` attribute (you must implement this exactly)

Rust Reference:

* For `#[path]` on modules **not inside inline module blocks**, the path is **relative to the directory the source file is located**. ([Rust Documentation][2])
* For `#[path]` **inside inline module blocks**, the relative base depends on whether the containing file is mod-rs or non-mod-rs; the Reference gives explicit examples and rules. ([Rust Documentation][2])

**Minimal implementation posture (recommended):**

* Support `#[path]` for:

  * `mod item;` at file/module scope (common)
  * `mod inline { #[path="..."] mod inner; }` (less common but real)

**Key example from Rust Reference (commit this to tests):**
Inline module `#[path]` behaves differently depending on parent file:

* If located in `src/a/b.rs`, then `mod inline { #[path="other.rs"] mod inner; }` loads `src/a/b/inline/other.rs`
* If located in `src/a/mod.rs`, it loads `src/a/inline/other.rs` ([Rust Documentation][2])

So you need:

```python
def resolve_path_attr(
    parent_file: Path,
    inline_module_stack: list[str],  # e.g. ["inline", "nested"] for mod inline { mod nested { ... } }
    rel_path: str,
) -> Path:
    # base starts at directory of the parent_file
    base = parent_file.parent

    if inline_module_stack:
        if is_mod_rs(parent_file.name):
            # mod-rs: base includes inline modules as directories
            base = base.joinpath(*inline_module_stack)
        else:
            # non-mod-rs: base starts with a directory named after the non-mod-rs module
            base = (parent_file.parent / parent_file.stem).joinpath(*inline_module_stack)

    return (base / rel_path).resolve()
```

This is directly aligned with the Reference’s description of how paths are computed in mod-rs vs non-mod-rs contexts. ([Rust Documentation][2])

---

## H.4 Import graph extraction (use-tree normalization → edges)

### H.4.1 What `use` means (binding + optional re-export)

Rust Reference:

* `use` creates one or more **local name bindings** synonymous with a path. ([Rust Documentation][5])
* A `use` declaration can be `pub` to **re-export** a name. ([Rust Documentation][5])

So your import graph should distinguish:

* `import` (private binding)
* `reexport` (public binding)

### H.4.2 UseTree IR (shape-preserving, resolvable later)

Rust Reference lists the core syntactic shortcuts you must support: brace grouping, `self`, `as`, glob, nested groups. ([Rust Documentation][5])

A minimal IR that preserves semantics:

```python
@dataclass(frozen=True)
class UseTree:
    prefix: tuple[str, ...]          # e.g. ("std","collections","hash_map")
    kind: Literal["single","group","glob","self"]
    # single:
    leaf: str | None                 # final segment (identifier) when kind=="single"
    alias: str | None                # "as x"
    # group:
    items: tuple["UseTree", ...]     # nested trees when kind=="group"
```

Normalization rule:

* Always carry the **prefix** down (so `use a::b::{c, d::e};` becomes two items with prefix `("a","b")` and leaves `c`, `e` with additional segments). This matches the brace grouping semantics described in the Reference. ([Rust Documentation][5])

### H.4.3 Edge emission contract

For each normalized leaf binding, emit:

* `scope_module_id` (the module/file where the `use` lives)
* `visibility` (`pub` or private)
* `imported_path` segments (including `crate|self|super` as segment kinds)
* `binding_name` (alias if present, else leaf name, else `*` for glob)
* `is_glob`
* `span_use_item` (byte span)
* `span_name` (byte span of bound name or `*`)
* `cfg_guard` / attributes (optional condition)

---

## H.5 Where tree-sitter stops (and what your resolver must do)

### H.5.1 Tree-sitter gives syntax, not crate/module resolution

Tree-sitter will happily parse:

* `use foo::bar;`
* `mod baz;`
  …even if:
* `foo` is an external crate dependency
* `bar` doesn’t exist
* `baz.rs` is missing but conditionally compiled out

Your resolver must add *project semantics*:

1. **Crate root mapping** (Cargo targets → root files) ([Rust Documentation][3])
2. **Module file loading rules** (`name.rs` vs `name/mod.rs`, `#[path]`) ([Rust Documentation][2])
3. **Visibility and re-export** (`pub use`) ([Rust Documentation][5])
4. Optional: **cfg evaluation** (features/target cfg) using `cargo metadata` plus cfg parsing

### H.5.2 Practical “candidate set” policy (recommended default)

Do *not* attempt full name resolution initially. Produce:

* a **module containment graph** that is correct for existing files (and flags ambiguous/missing per Reference rules)
* an **import binding graph** that preserves `use` structure and visibility
* an **unresolved queue** for:

  * external crates
  * missing modules
  * cfg-gated ambiguity (same `mod` name under different cfgs)

Then optionally enrich with rust-analyzer later (as an integration layer).

---

## H.6 End-to-end Python workflow (concrete plan)

### H.6.1 Phase 1 — discover crate roots

* Run `cargo metadata --no-deps --format-version 1` and parse JSON. ([Rust Documentation][1])
* For each target, record:

  * `crate_id = (package_id, target_name, target_kind)`
  * `root_file = target.src_path` (respects `path = ...` overrides). ([Rust Documentation][4])

### H.6.2 Phase 2 — build module graph (file-backed resolution)

For each crate root file:

1. Parse file with tree-sitter-rust.
2. Extract `mod_item` declarations (inline and external).
3. For each external `mod child;`:

   * if `#[path]` present → resolve via Rust Reference `#[path]` rules ([Rust Documentation][2])
   * else compute candidates `{child.rs, child/mod.rs}` under `module_dir_for_file(parent_file)` ([Rust Documentation][2])
   * check existence:

     * none → unresolved edge
     * one → load that file as child module node
     * both → emit **hard error** (“not allowed to have both …”). ([Rust Documentation][2])
4. Recurse (DFS/BFS) to discover the full module tree reachable from crate root.

### H.6.3 Phase 3 — extract imports (`use`) per module/block scope

For each parsed module file:

* Extract all `use_declaration` nodes.
* Normalize each `UseTree` into leaf bindings.
* Emit edges:

  * `imports` (private)
  * `reexports` (public `pub use`) ([Rust Documentation][5])
* Record scope:

  * module scope vs block scope (Rust Reference: `use` may appear in modules and blocks). ([Rust Documentation][5])

---

## H.7 Debug/observability checklist (you’ll need this in practice)

1. **Crate roots sanity**

```bash
cargo metadata --no-deps --format-version 1 | jq '.packages[].targets[] | {name,kind,src_path}'
```

(Format-version pinning and `--no-deps` are documented options.) ([Rust Documentation][1])

2. **Ambiguous module file errors**

* Detect and print any module where both `foo.rs` and `foo/mod.rs` exist; Rust Reference forbids it. ([Rust Documentation][2])

3. **`#[path]` correctness tests**

* Include the exact Rust Reference examples for mod-rs vs non-mod-rs inline path behavior as golden fixtures. ([Rust Documentation][2])

---

If you want the natural next deep dive: **I) Building a “Rust CST index” in Python (tables you’ll materialize)** — now that you can enumerate modules correctly, you can produce per-module symbol tables, import edges, and “public API surface” views (`pub mod` + `pub use`) with deterministic module IDs.

[1]: https://doc.rust-lang.org/cargo/commands/cargo-metadata.html "cargo metadata - The Cargo Book"
[2]: https://doc.rust-lang.org/reference/items/modules.html "Modules - The Rust Reference"
[3]: https://doc.rust-lang.org/cargo/guide/project-layout.html "Package Layout - The Cargo Book"
[4]: https://doc.rust-lang.org/cargo/reference/cargo-targets.html "Cargo Targets - The Cargo Book"
[5]: https://doc.rust-lang.org/reference/items/use-declarations.html "Use declarations - The Rust Reference"

# I) Building a “Rust CST index” in Python (tables you’ll materialize)

This is the “CST index layer”: **deterministic**, **byte-span anchored**, and **resolver-agnostic**. You’ll parse Rust source into a Tree-sitter CST, run **windowable query packs** (plus a few structured walkers), and emit a small set of Arrow/Delta-friendly tables.

Two hard constraints drive everything:

* **Byte spans are canonical**: every row anchors to `start_byte/end_byte` from Tree-sitter nodes, so joins are stable across all CST-derived tables. ([Tree-sitter][1])
* **Incremental-friendly**: you should be able to re-run the packs on `tree.changed_ranges(new_tree)` and only touch affected rows. ([Tree-sitter][2])

---

## I.0 Indexer invariants (don’t start without these)

### I.0.1 Use `QueryCursor.matches()` as your default execution primitive

`matches()` groups related captures together per pattern match (vs `captures()` which is a flat stream). This matters for “one row per semantic event.” ([GitHub][3])

### I.0.2 Errors and missing tokens are first-class data

In py-tree-sitter:

* `Node.is_error`: this node *is* a syntax error
* `Node.has_error`: this node or any descendant contains syntax errors
* `Node.is_missing`: this node was inserted during error recovery ([Tree-sitter][1])

In the query language, **missing nodes are not captured by `(ERROR)`**; you must also query `(MISSING)` if you want complete diagnostics. ([Tree-sitter][4])

### I.0.3 Stable IDs: hash `(file_relpath, kind, anchor_span, name_span?, container_span?)`

This keeps rows stable across re-runs and supports “delete + upsert” without needing global integers.

```python
import hashlib
def stable_id(*parts: str) -> str:
    payload = "\0".join(parts).encode("utf-8")
    return hashlib.blake2b(payload, digest_size=16).hexdigest()
```

### I.0.4 Node-kind universe is grammar-versioned

`node-types.json` is the authoritative universe of node kinds/fields/children; treat grammar bumps as schema migrations. ([Tree-sitter][5])

---

## I.1 `rust_items` — item definitions (defs you can navigate to)

### Query (emit one row per “item anchor”)

You can implement this as a single pack (`queries_rust_index/items.scm`) or reuse your `00_defs.scm` and set `cq.emit = "rust_items"`.

**Representative patterns (minimal, stable):**

```scm
(function_item name: (identifier) @item.name) @item.fn
(#set! cq.emit "rust_items") (#set! cq.kind "item.fn") (#set! cq.anchor "item.fn")
(#set! cq.slot.name "item.name") (#set! cq.slot.name.extract "span_text")

(struct_item name: (type_identifier) @item.name) @item.struct
(#set! cq.emit "rust_items") (#set! cq.kind "item.struct") (#set! cq.anchor "item.struct")
(#set! cq.slot.name "item.name") (#set! cq.slot.name.extract "span_text")

(enum_item name: (type_identifier) @item.name) @item.enum
...

(trait_item name: (type_identifier) @item.name) @item.trait
...

(impl_item type: (_type) @item.impl.for_type) @item.impl
(#set! cq.emit "rust_items") (#set! cq.kind "item.impl") (#set! cq.anchor "item.impl")
(#set! cq.slot.for_type "item.impl.for_type") (#set! cq.slot.for_type.extract "span")

(mod_item name: (identifier) @item.name) @item.mod
...

(type_item name: (type_identifier) @item.name type: (_type) @item.type_alias.rhs) @item.type_alias
...

(const_item name: (identifier) @item.name type: (_type) @item.type) @item.const
(static_item name: (identifier) @item.name type: (_type) @item.type) @item.static

(macro_definition name: (identifier) @item.name) @item.macro_def
```

### Output schema (recommended)

**One row per item definition anchor.** Keep it “pure CST facts”; attach attrs/docs later via joins.

* `item_id` (stable hash)
* `file_relpath`
* `crate_id`, `module_id` (from your H-layer module resolver)
* `item_kind` (e.g., `item.fn`, `item.struct`, …)
* `anchor_bstart`, `anchor_bend`
* `name_bstart`, `name_bend`, `name_text` (nullable for `impl`)
* `vis_text` (nullable; capture `visibility_modifier` if you want)
* `container_item_id` (nullable; for nested items inside blocks/impls/mods)

### Edge cases + mitigation

* **Methods are still `function_item`**: classify `item_kind = item.method` if parent chain contains `impl_item`/`trait_item` (container inference in Python).
* **`impl` has no “name”**: store `for_type` span/text; optionally also store `trait` span if present.
* **Inline vs external modules**: `mod_item` may have a body; your module resolver decides whether it’s file-backed (H-layer).
* **Macro items**: `macro_definition` is easy; *proc-macro expansions are not visible* (see G).

---

## I.2 `rust_uses` — import/re-export edges (use tree → leaf bindings)

Rust `use` declarations “may appear in modules and blocks” and if public are “re-exports.” ([Rust Documentation][6])
So `rust_uses` needs to preserve **scope** and **visibility**, and normalize brace trees into leaf bindings.

### Query (capture the `use_declaration` + its argument subtree)

```scm
(use_declaration
  argument: (_) @use.tree
) @use.item
(#set! cq.emit "rust_uses") (#set! cq.kind "use.decl") (#set! cq.anchor "use.item")
(#set! cq.slot.tree "use.tree") (#set! cq.slot.tree.extract "span_kind")
```

### Output schema (normalized leaf-edge rows)

Emit **one row per leaf binding** after expanding `use` trees in Python.

* `use_id` (stable hash of file + decl span + leaf path + bound name)
* `file_relpath`, `module_id`
* `decl_bstart`, `decl_bend` (span of whole `use_declaration`)
* `is_pub` (bool)
* `binding_name` (alias if present else leaf name else `"*"` for glob)
* `is_glob` (bool)
* `path_segments` (list<struct{kind,text,bstart,bend}>) — preserve `crate/self/super` as segment kinds
* `leaf_bstart`, `leaf_bend` (span of leaf token or `*`)
* `alias_bstart`, `alias_bend` (nullable)
* `scope_kind` (`module` | `block`) + `scope_anchor_span`

### Edge cases + mitigation

* **Brace groups**: `use a::b::{c, d::e};` → two leaf rows with shared prefix.
* **`self`**: `use a::b::{self, c};` emits one binding for `b` plus one for `c`.
* **Globs**: `use a::b::*;` emits `is_glob=True`, binding `"*"`.
* **Block-local uses**: record scope anchor as the nearest `block` / `function_item` span; Rust explicitly allows uses in blocks. ([Rust Documentation][6])
* **Re-exports**: `pub use …` sets `is_pub=True`; treat these edges as “export surface” later.

**Normalization skeleton (shape-preserving, resolvable later):**

```python
def flatten_use_tree(node, prefix_segments):
    """
    Returns list of leaf bindings:
      {segments: [...], leaf: {...}, alias: {...}|None, is_glob: bool, binds_self: bool}
    """
    # Dispatch by node.type: scoped_use_list/use_list/use_as_clause/use_wildcard/identifier/self/super/crate...
    ...
```

---

## I.3 `rust_calls` — call sites + macro invocations (call/method/macro)

### Query

Use rooted patterns so you can window by changed ranges (see your D chapter + `is_pattern_non_local` guidance). ([Tree-sitter][7])

```scm
;; free/unknown calls
(call_expression
  function: (_) @call.callee
  arguments: (arguments) @call.args
) @call
(#set! cq.emit "rust_calls") (#set! cq.kind "call") (#set! cq.anchor "call")
(#set! cq.slot.callee "call.callee") (#set! cq.slot.callee.extract "span_kind")
(#set! cq.slot.args "call.args") (#set! cq.slot.args.extract "span")

;; method calls
(call_expression
  function: (field_expression
              value: (_) @call.recv
              field: (field_identifier) @call.method)
  arguments: (arguments) @call.args
) @call.method
(#set! cq.emit "rust_calls") (#set! cq.kind "call.method") (#set! cq.anchor "call.method")
(#set! cq.slot.recv "call.recv") (#set! cq.slot.recv.extract "span_text")
(#set! cq.slot.method "call.method") (#set! cq.slot.method.extract "span_text")
(#set! cq.slot.args "call.args") (#set! cq.slot.args.extract "span")

;; macro invocations treated separately
(macro_invocation
  macro: (_) @macro.path
  (token_tree) @macro.args
) @call.macro
(#set! cq.emit "rust_calls") (#set! cq.kind "call.macro") (#set! cq.anchor "call.macro")
(#set! cq.slot.path "macro.path") (#set! cq.slot.path.extract "span_text")
(#set! cq.slot.args "macro.args") (#set! cq.slot.args.extract "span")
```

### Output schema

* `call_id`
* `file_relpath`, `module_id`, `container_item_id`
* `call_kind` (`call` | `call.method` | `call.macro`)
* `anchor_bstart`, `anchor_bend`
* `callee_kind`, `callee_bstart`, `callee_bend` (for `call`)
* `callee_text` (optional; store if cheap)
* `recv_text`, `method_text` (for `call.method`)
* `args_bstart`, `args_bend`
* `macro_path_text` (for `call.macro`)
* `token_tree_span` (for `call.macro`)

### Edge cases + mitigation

* **Callee isn’t always an identifier**: `call_expression.function` can be a path, field expression, parenthesized expr, closure var, etc. Store `callee_kind` and keep resolution for later.
* **UFCS-like shapes**: `Type::method(x)` often appears as a path callee, not a `field_expression`.
* **Macros are not expanded** (G): treat invocation site as the stable “call-like” fact; optionally enrich via injections (F) or rust-analyzer.

---

## I.4 `rust_types` — type refs + bounds + lifetimes (typed nodes, plus context)

This table is most useful when you **capture types via field roles** (param/return/field/type-alias RHS/where bounds) instead of “every `_type` node anywhere,” which becomes huge and noisy.

### Query (role-aware captures)

```scm
;; function signature types
(function_item
  name: (identifier) @owner.name
  parameters: (parameters) @sig.params
  return_type: (_type)? @sig.ret
) @owner.fn
(#set! cq.emit "rust_types") (#set! cq.kind "type.fn_sig") (#set! cq.anchor "owner.fn")
(#set! cq.slot.owner_name "owner.name") (#set! cq.slot.owner_name.extract "span_text")
(#set! cq.slot.params "sig.params") (#set! cq.slot.params.extract "span")
(#set! cq.slot.ret "sig.ret") (#set! cq.slot.ret.extract "span")

;; where predicates
(where_predicate
  left: (_) @where.left
  bounds: (trait_bounds) @where.bounds
) @where.pred
(#set! cq.emit "rust_types") (#set! cq.kind "type.where_pred") (#set! cq.anchor "where.pred")
(#set! cq.slot.left "where.left") (#set! cq.slot.left.extract "span_kind")
(#set! cq.slot.bounds "where.bounds") (#set! cq.slot.bounds.extract "span")

;; explicit lifetimes
(lifetime) @type.lifetime
(#set! cq.emit "rust_types") (#set! cq.kind "type.lifetime") (#set! cq.anchor "type.lifetime")
(#set! cq.slot.name "type.lifetime") (#set! cq.slot.name.extract "span_text")
```

(You’ll add more role patterns: field declarations, type aliases, const/static types, trait bounds in `trait_item`/`impl_item`, etc.)

### Output schema

Two good options:

**Option A (single table, role-driven):**

* `type_fact_id`
* `file_relpath`, `module_id`, `owner_item_id` (nullable)
* `type_fact_kind` (`type.fn_sig`, `type.where_pred`, `type.lifetime`, …)
* `anchor_bstart`, `anchor_bend`
* `role` (string enum: `return_type`, `param_list`, `where_left`, `where_bounds`, …)
* `node_kind`, `bstart`, `bend`
* `text` (optional; usually omit for large `_type` nodes)

**Option B (split nodes vs edges):**

* `rust_type_nodes` (one row per `_type`/`trait_bounds`/`lifetime` node)
* `rust_type_edges` (role edges from owner item → type node)
  If you’re already Arrow-native, this is often cleaner (dedupe nodes).

### Edge cases + mitigation

* **`type_arguments` contains types + lifetimes + const generics**: if you later index `generic_type`, don’t assume children are only `_type`.
* **Macro-heavy signatures**: types inside macros may not parse cleanly; rely on `rust_errors` to flag low-quality regions.

---

## I.5 `rust_attrs` — attribute attachment table (outer/inner, path, args, owner)

### Query (capture attr nodes; attach in Python)

Tree-sitter node API supports byte spans and parent navigation; you attach by **order** (“leading trivia buffer”) and/or by parent kind. ([Tree-sitter][1])

```scm
(attribute_item (attribute) @attr.node) @attr.outer
(#set! cq.emit "rust_attrs") (#set! cq.kind "attr.outer") (#set! cq.anchor "attr.outer")
(#set! cq.slot.attr "attr.node") (#set! cq.slot.attr.extract "span")

(inner_attribute_item (attribute) @attr.node) @attr.inner
(#set! cq.emit "rust_attrs") (#set! cq.kind "attr.inner") (#set! cq.anchor "attr.inner")
(#set! cq.slot.attr "attr.node") (#set! cq.slot.attr.extract "span")
```

Then parse the attribute node into:

* path segments (identifier/scoped_identifier)
* optional `token_tree` arguments
* optional `value: _expression`

### Output schema

* `attr_id`
* `file_relpath`, `module_id`
* `attr_kind` (`outer` | `inner`)
* `attr_bstart`, `attr_bend`
* `path_segments` (list<struct{kind,text,bstart,bend}>)
* `args_bstart`, `args_bend` (nullable)
* `value_bstart`, `value_bend` (nullable)
* `owner_kind`, `owner_id`, `owner_span`
* `attach_policy` (`leading_trivia`, `parent_field`, `manual`)

### Edge cases + mitigation

* **Stacked attrs**: multiple `#[...]` in a row; attach all until first non-attr/doc node.
* **Inner attrs** (`#![...]`) attach to the module/file, not the next item; treat `attr.inner` as “module-owned” unless you’re inside an inline `mod {}`.
* **Attrs on fields/variants/params**: you’ll need additional attachment contexts beyond items (e.g., attach to the next `field_declaration` within a struct body).

---

## I.6 `rust_docs` — doc comment spans attached to items/fields/variants

### Query (doc comments are comment nodes with doc markers)

In Rust’s CST, doc comments are represented as comment nodes with a doc marker; in Python you’ll normalize the raw text and attach similarly to attrs.

```scm
(line_comment doc: (doc_comment)? @doc.marker) @doc.line
(#set! cq.emit "rust_docs") (#set! cq.kind "doc.line") (#set! cq.anchor "doc.line")
(#set! cq.slot.text "doc.line") (#set! cq.slot.text.extract "span_text")

(block_comment doc: (doc_comment)? @doc.marker) @doc.block
(#set! cq.emit "rust_docs") (#set! cq.kind "doc.block") (#set! cq.anchor "doc.block")
(#set! cq.slot.text "doc.block") (#set! cq.slot.text.extract "span_text")
```

### Output schema

* `doc_id`
* `file_relpath`, `module_id`
* `doc_kind` (`line` | `block` | `inner_line` | `inner_block` if you choose to split)
* `raw_bstart`, `raw_bend`
* `raw_text`
* `norm_text` (marker-stripped, whitespace normalized)
* `owner_kind`, `owner_id`, `owner_span`
* `group_id` (to group consecutive `///` lines into one logical doc block)

### Edge cases + mitigation

* **Blank line breaks attachment**: decide policy (most tools break doc attachment on blank line or non-comment tokens).
* **Docs on fields/variants**: same as attrs—attach within struct/enum bodies by local ordering.
* **Inner doc comments** (`//!` / `/*! */`) attach to the module/file scope.

Rust reference frames `use` as import/re-export and module as item container; doc/attr attachment is a tooling policy you define on top (Tree-sitter gives spans, you decide ownership). ([Rust Documentation][6])

---

## I.7 `rust_strings` — literal content spans (injection candidates)

### Query (string families + escape sequences)

```scm
(string_literal (string_content) @str.content) @str.lit
(raw_string_literal (string_content) @str.content) @str.raw
(string_literal (escape_sequence) @str.escape) @str.escape
(char_literal) @ch.lit
```

### Output schema

* `string_id`
* `file_relpath`, `module_id`, `container_item_id`, `container_macro_call_id` (optional; useful for F)
* `string_kind` (`string` | `raw_string` | `char`)
* `literal_bstart`, `literal_bend` (span of the literal node)
* `content_bstart`, `content_bend` (span of `string_content` where available)
* `content_text` (optional; consider truncating)
* `has_escapes` (bool)
* `prefix_class` (`normal` | `byte` | `raw_byte`) — computed by lexeme inspection

### Edge cases + mitigation

* **Byte strings aren’t always distinct node kinds**: classify by inspecting the source prefix around `literal_bstart` (`b"..."`, `br"..."`) rather than assuming separate CST node types.
* **Raw strings**: `r#"..."#` and more `#` depth—content span is still `string_content`; delim depth is lexical.
* **Concatenated literals**: Rust allows adjacent string literals in some contexts via macro expansion more than core syntax—if you need “combined injection,” use your injection planner semantics (F).

---

## I.8 `rust_errors` — diagnostics stream (ERROR + MISSING + context)

### Query (both ERROR and MISSING)

Tree-sitter query syntax explicitly supports `(MISSING)` and notes missing nodes aren’t captured by `(ERROR)`. ([Tree-sitter][4])

```scm
(ERROR) @err
(MISSING) @missing
```

### Output schema

* `diag_id`
* `file_relpath`, `module_id`
* `diag_kind` (`ERROR` | `MISSING`)
* `node_type` (actual node.type, e.g., `identifier`, `";"`, …)
* `bstart`, `bend`
* `parent_type` (nullable)
* `context_bstart`, `context_bend` (small window around error for UX)
* `has_error_ancestor` (bool; computed via parent chain)

### Edge cases + mitigation

* **MISSING nodes may have “normal-looking” types** (e.g., an inserted `identifier`); rely on `node.is_missing` and/or `(MISSING ...)` queries rather than type alone. ([Tree-sitter][1])
* **No human-readable messages**: Tree-sitter exposes structure, not compiler diagnostics; emit spans + context and let higher layers (rust-analyzer) provide real messages if needed.

---

## I.9 Execution plan (how to materialize all tables per file)

### I.9.1 Per-file indexing pipeline

1. Parse file bytes with `Parser.parse`.
2. Run windowable query packs with `QueryCursor.matches()` for:

   * items, uses (declarations), calls, types, attrs/docs/strings
3. Normalize:

   * `rust_uses`: expand use trees into leaf edges
   * `rust_attrs`/`rust_docs`: attach by ordering rules
   * `rust_strings`: classify prefix type, link to nearest macro call if desired
4. Emit diagnostics:

   * query `(ERROR)` and `(MISSING)`
   * optionally also walk tree and check `node.has_error` for fast “bail/flag” ([Tree-sitter][1])

### I.9.2 Incremental rebuild (changed-range windowing)

py-tree-sitter supports incremental parsing: edit old tree, parse with old tree, then get `changed_ranges(new_tree)`; ranges represent where structure changed. ([GitHub][8])
Run only windowable packs within those ranges (and reattach attrs/docs in the affected neighborhood).

---

If you want the next deep dive after this, the natural follow-on is: **J) Correctness & drift management** for these tables (grammar bumps, pack contract bumps, and a golden-fixture harness that asserts table rows/spans are stable under controlled edits).

[1]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Node.html "Node — py-tree-sitter 0.25.2 documentation"
[2]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Tree.html?utm_source=chatgpt.com "Tree — py-tree-sitter 0.25.2 documentation"
[3]: https://github.com/tree-sitter/py-tree-sitter?utm_source=chatgpt.com "Python bindings to the Tree-sitter parsing library"
[4]: https://tree-sitter.github.io/tree-sitter/using-parsers/queries/1-syntax.html?utm_source=chatgpt.com "Query Syntax"
[5]: https://tree-sitter.github.io/tree-sitter/using-parsers/6-static-node-types?utm_source=chatgpt.com "Static Node Types"
[6]: https://doc.rust-lang.org/reference/items/use-declarations.html?utm_source=chatgpt.com "Use declarations - The Rust Reference"
[7]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Query.html?utm_source=chatgpt.com "Query — py-tree-sitter 0.25.2 documentation"
[8]: https://github.com/tree-sitter/py-tree-sitter/blob/master/examples/usage.py?utm_source=chatgpt.com "py-tree-sitter/examples/usage.py at master"

# J) Correctness & drift management (grammar evolution is real)

You want upgrades of **`tree-sitter` (py bindings)** and **`tree-sitter-rust` (grammar wheel)** to behave like **schema migrations**: deterministic, diffable, CI-gated, and producing a concrete “what broke” report.

This section standardizes three pillars:

1. **Version pinning + ABI gates** (don’t let incompatible Language ABIs reach runtime)
2. **`node-types.json` diffing** as your primary breaking-change detector
3. **Query-pack conformance tests** (compile-time validation + golden fixtures keyed by your capture contracts)

---

## J.0 Drift surfaces you must track (what changes can break you)

### J.0.1 ABI drift (runtime ↔ language binary mismatch)

py-tree-sitter is **backwards-compatible but not forwards-compatible** with language ABIs; it exposes `LANGUAGE_VERSION` and `MIN_COMPATIBLE_LANGUAGE_VERSION` constants and explicitly calls out the forwards-incompatibility risk. ([Tree-sitter][1])

### J.0.2 Grammar schema drift (`node-types.json`)

Tree-sitter generates `node-types.json` containing **structured data about every possible syntax node**, where `(type, named)` is a unique identifier for node types. ([Tree-sitter][2])

### J.0.3 Query contract drift (your packs are an API)

Packs can “compile” but still drift semantically:

* captures renamed
* `cq.slot.*` bindings changed
* patterns become non-windowable (non-rooted / non-local) → incremental indexing correctness/perf regression

py-tree-sitter exposes `Query.pattern_settings` (from `#set!`) and `is_pattern_rooted` / `is_pattern_non_local` for exactly this kind of introspection. ([Tree-sitter][1])

---

## J.1 Version pinning (treat updates as schema migrations)

### J.1.1 Pin the full “runtime+grammar” set

Minimum pins:

* `tree-sitter==X.Y.Z` (py-tree-sitter)
* `tree-sitter-rust==A.B.C` (grammar wheel)

If you use the CLI in CI/dev loops:

* `tree-sitter-cli==...` (via cargo/npm) — it includes `test` and other commands. ([Docs.rs][3])

**Examples (choose your ecosystem):**

**pip / requirements.txt**

```txt
tree-sitter==0.25.2
tree-sitter-rust==0.24.0
```

**uv**

```bash
uv add "tree-sitter==0.25.2" "tree-sitter-rust==0.24.0"
uv lock
```

**Poetry**

```bash
poetry add "tree-sitter==0.25.2" "tree-sitter-rust==0.24.0"
poetry lock
```

> Treat *any* change in `tree-sitter-rust` as a schema migration because it can change `node-types.json` (kinds/fields) and the shipped `queries/*.scm`.

### J.1.2 ABI gate: fail fast when the language wheel doesn’t match the runtime

py-tree-sitter publishes:

* `tree_sitter.LANGUAGE_VERSION` = latest ABI supported
* `tree_sitter.MIN_COMPATIBLE_LANGUAGE_VERSION` = earliest ABI supported
  and explains the compatibility rule (backwards-compatible, not forwards-compatible). ([Tree-sitter][1])

**CI sanity check (import-time):**

```python
import tree_sitter as ts
import tree_sitter_rust as tsrust
from tree_sitter import Language

print("py-tree-sitter:", ts.__version__)
print("ABI supported:", ts.MIN_COMPATIBLE_LANGUAGE_VERSION, "..", ts.LANGUAGE_VERSION)

# This will raise if the grammar wheel's embedded Language is incompatible.
RUST = Language(tsrust.language())
```

If you ever build languages yourself with the CLI (not your case for `tree-sitter-rust`, but common in multi-lang setups), enforce that the CLI version used to generate the language is not newer than your runtime’s supported ABI window. ([Tree-sitter][1])

### J.1.3 Freeze a “grammar+packs manifest” alongside your code

Persist (per pinned grammar version):

* `tree-sitter-rust` version
* `tree-sitter` version + ABI window
* `node-types.json` sha256
* `queries/*.scm` sha256
* compiled query contracts (captures + `pattern_settings` + rooted/non-local flags)

This gives you deterministic “what changed” diffs during upgrades.

---

## J.2 Diffing `node-types.json` as your breaking-change detector

Tree-sitter positions `node-types.json` as structured data about **every possible syntax node**, and states that `(type, named)` uniquely identifies a node type. ([Tree-sitter][2])

### J.2.1 Extract the installed `node-types.json` from the grammar wheel

```python
from importlib.metadata import distribution
from pathlib import Path

dist = distribution("tree-sitter-rust")
node_types_path: Path | None = None
for f in dist.files or []:
    if str(f).endswith("src/node-types.json"):
        node_types_path = dist.locate_file(f)
        break
assert node_types_path is not None
print(node_types_path)
```

### J.2.2 Normalize into a canonical, diffable form

You want a *stable sort order* and a *stable representation*.

**Canonical projection (recommended):**

* key: `(type, named)`
* fields: map `field_name -> {required,multiple,types[]}`
* children: same shape (if present)
* subtypes: for supertypes (if present)

Tree-sitter defines these exact structures (`fields`, `children`, `required`, `multiple`, `types`) in the static node types spec. ([Tree-sitter][2])

```python
import json

def normalize_node_types(path: Path) -> list[dict]:
    arr = json.loads(path.read_text("utf-8"))
    out = []
    for e in arr:
        item = {
            "type": e["type"],
            "named": bool(e["named"]),
            "fields": e.get("fields", {}),
            "children": e.get("children"),
            "subtypes": e.get("subtypes", []),
        }
        out.append(item)
    # stable sort: (type, named)
    out.sort(key=lambda x: (x["type"], x["named"]))
    return out
```

Write it to `artifacts/node-types.normalized.json` for each grammar version and diff it like a schema.

### J.2.3 Classify drift into actionable buckets

Because `(type, named)` is a unique identifier, changes to this set are the highest-signal events. ([Tree-sitter][2])

**Breaking (must fix packs / bump registry major):**

* `(type,named)` removed
* field removed or renamed for a node you reference in any pack
* field’s admissible child types changed in a way that invalidates your patterns

**Risky (review required):**

* new `(type,named)` added (usually safe, but can affect “supertypes” and broad queries)
* new field added (safe unless you relied on exhaustive field sets)
* subtype composition changes for a supertype you use as a “semantic slice”

### J.2.4 CI gate: “no schema drift without an explicit migration”

On any dependency bump PR:

1. extract + normalize `node-types.json`
2. diff against baseline
3. fail CI unless:

   * a migration note exists, and
   * packs/registry updated as needed, and
   * freeze snapshot updated

---

## J.3 Query-pack conformance tests (compile-time + golden fixtures)

### J.3.1 Compile-time validation: Query must compile, contract must match registry

Tree-sitter query metadata is attached via `#set!` directives; py-tree-sitter exposes those properties via `Query.pattern_settings`. ([Tree-sitter][1])

Minimum compile-time validations:

* `Query(...)` compiles (no `QueryError`)
* every emitting pattern has required `cq.*` settings
* every `cq.slot.*` capture exists in the query’s capture table (`capture_name(i)`)
* `cq.windowable` patterns must be rooted and not non-local (`is_pattern_rooted`, `is_pattern_non_local`) ([Tree-sitter][1])

### J.3.2 Structural validation against `node-types.json` (no “field hallucination”)

Use `node-types.json` to validate:

* every node kind referenced in a pattern exists
* every field constraint used in patterns exists for that node kind
  Tree-sitter’s static node types spec defines `fields` explicitly and what they mean. ([Tree-sitter][2])

This is the “semantic linter” layer that catches cases where queries compile but match nothing because a kind/field drifted.

### J.3.3 Use Tree-sitter’s built-in test harness where it exists

**Highlight query unit tests:** Tree-sitter documents a built-in unit testing format stored in `test/highlight` inside a grammar repository, with caret (`^`) and arrow (`<-`) assertions and negation via `!`. ([Tree-sitter][4])

Even if your index packs aren’t highlight packs, this is still useful for:

* validating any derived highlight/injection packs you maintain
* regression testing how a grammar upgrade changes capture classification

**Run tests via CLI:**

* Install: `cargo install --locked tree-sitter-cli` ([Docs.rs][3])
* Run: `tree-sitter test` (CLI docs describe this command as running unit tests for the parser in the current working directory). ([Docs.rs][3])

### J.3.4 Golden fixtures for *your* index packs (the real correctness harness)

Tree-sitter’s highlight tests validate highlight scopes; your indexer needs **row-level assertions**.

**Recommended structure:**

```
tests/fixtures/rust/
  simple_items.rs
  use_trees.rs
  macros_calls.rs
  types_bounds.rs
  attrs_docs.rs
  broken_syntax.rs
tests/golden/rust/
  simple_items.rust_items.jsonl
  use_trees.rust_uses.jsonl
  ...
```

**Golden test loop:**

1. parse fixture
2. run packs
3. emit normalized JSONL rows (sorted by `(kind, anchor_bstart, anchor_bend, name_text?)`)
4. diff against golden

**Key: keep goldens keyed by your `cq.kind` and spans.** That makes drift reports immediately actionable and aligns with your “contract registry” semantics.

---

## J.4 Upgrade runbook (what a safe grammar bump looks like)

### Step 1 — bump pins

Update `tree-sitter` and/or `tree-sitter-rust` versions in your lockfile.

### Step 2 — ABI gate

Run a smoke test that loads `Language(tsrust.language())`. If the ABI window is incompatible, py-tree-sitter will fail; the ABI compatibility rule is documented via `LANGUAGE_VERSION` and `MIN_COMPATIBLE_LANGUAGE_VERSION`. ([Tree-sitter][1])

### Step 3 — schema diff (`node-types.json`)

* extract + normalize
* compute diff buckets (breaking/risky/additive)

### Step 4 — pack lint (compile-time + node-types-based structural lint)

* compile all packs
* validate registry conformance
* validate windowability (rooted + local)
* validate all referenced kinds/fields exist per node-types schema ([Tree-sitter][2])

### Step 5 — run goldens

* index fixtures
* diff JSONL outputs
* produce a drift report keyed by `cq.kind`

### Step 6 — decide semver bump + freeze

* breaking drift → major bump
* additive drift → minor bump
* no contract drift → patch/no bump
  …and write a new “freeze snapshot” for the upgraded version.

---

## J.5 Optional accelerator: codegen wrappers from `node-types.json`

If you want stronger compile-time guarantees, tools like `type-sitter` generate type-safe node/query wrappers from `node-types.json` and queries. This can turn some “runtime lint errors” into “build-time errors” during migration work. ([Docs.rs][5])

[1]: https://tree-sitter.github.io/py-tree-sitter/ "py-tree-sitter — py-tree-sitter 0.25.2 documentation"
[2]: https://tree-sitter.github.io/tree-sitter/using-parsers/6-static-node-types "Static Node Types - Tree-sitter"
[3]: https://docs.rs/tree-sitter-cli "tree_sitter_cli - Rust"
[4]: https://tree-sitter.github.io/tree-sitter/3-syntax-highlighting.html "Syntax Highlighting - Tree-sitter"
[5]: https://docs.rs/type-sitter?utm_source=chatgpt.com "type_sitter - Rust"

# K) Performance playbook for Rust repos (Python deployment posture)

This is the “make it fast, keep it correct, don’t blow up” layer: **incremental parsing + windowed queries + aggressive caching**, with explicit rails for macro-heavy, generated-code-heavy Rust repos.

---

## K.0 Default posture (what you optimize for)

### K.0.1 Two levers dominate cost

1. **Parsing cost** (CST build / incremental reparse)
2. **Query cost** (pattern explosion on large/deep trees)

Your job is to:

* reuse work (incremental parse, cached compiled queries)
* shrink search space (changed-range windowing, max-start-depth)
* cap worst cases (timeouts + match limits + file-size/dir guards)

---

## K.1 Incremental parsing posture (Rust-indexer defaults)

### K.1.1 Canonical loop: edit → parse(old_tree) → changed_ranges → windowed queries

py-tree-sitter gives you the primitives explicitly:

* `Tree.edit(...)` to synchronize an old tree with an edit (byte offsets + row/col points). ([tree-sitter.github.io][1])
* `Parser.parse(source, old_tree=...)` to reparse incrementally; returns `Tree` or `None` if timeout expired. ([tree-sitter.github.io][2])
* `old_tree.changed_ranges(new_tree)` to get the ranges whose hierarchical structure changed; docs recommend calling it right after parsing. ([tree-sitter.github.io][1])

**Rust-indexer default (per file):**

* keep `(bytes, tree, bytemap)` in a per-file cache entry
* edits arrive as `InputEdit` (start_byte/old_end/new_end + points)
* reparse with old tree
* run only *windowable packs* inside changed ranges

```python
from dataclasses import dataclass
from tree_sitter import Parser, QueryCursor

@dataclass
class FileState:
    src: bytes
    tree: object  # tree_sitter.Tree

def apply_edit_and_reparse(parser: Parser, st: FileState, edit) -> tuple[FileState, list]:
    # 1) apply edit to old tree so changed_ranges is meaningful
    st.tree.edit(
        edit.start_byte, edit.old_end_byte, edit.new_end_byte,
        edit.start_point, edit.old_end_point, edit.new_end_point,
    )  # Tree.edit(...) :contentReference[oaicite:3]{index=3}

    # 2) update bytes (your own bytemap/editor integration supplies this)
    new_src = edit.apply_to_bytes(st.src)

    # 3) incremental parse
    new_tree = parser.parse(new_src, old_tree=st.tree)  # Parser.parse(old_tree=...) :contentReference[oaicite:4]{index=4}
    if new_tree is None:
        # timeout expired (see K.4.1); decide policy: retry w/ higher timeout, or mark file “degraded”
        return FileState(new_src, st.tree), []

    # 4) get changed ranges (call on the *old* edited tree)
    ranges = list(st.tree.changed_ranges(new_tree))  # Tree.changed_ranges :contentReference[oaicite:5]{index=5}

    return FileState(new_src, new_tree), ranges
```

### K.1.2 Windowed query execution defaults (containment-first)

For each changed range, prefer **containment** windowing when available:

* `QueryCursor.set_byte_range(start,end)` returns matches that **intersect** the range (captures may extend outside). ([tree-sitter.github.io][3])
* `QueryCursor.set_containing_byte_range(start,end)` restricts matches so **all nodes are fully contained** (added in 0.26.0). ([tree-sitter.github.io][3])

**Rust-indexer default:**

* use `set_containing_byte_range` for incremental correctness
* optionally combine with `set_byte_range` for “intersect line N but contained in neighborhood” (doc suggests this pattern) ([tree-sitter.github.io][3])

```python
def run_packs_in_ranges(root_node, packs: dict[str, object], ranges):
    for pack_name, query in packs.items():
        cur = QueryCursor(query)
        for r in ranges:
            # strongest correctness: fully-contained matches only
            cur.set_containing_byte_range(r.start_byte, r.end_byte)  # :contentReference[oaicite:9]{index=9}
            for pattern_i, caps in cur.matches(root_node):  # matches() returns (pattern_i, {cap->node}) :contentReference[oaicite:10]{index=10}
                yield pack_name, pattern_i, caps
```

### K.1.3 “Windowable” pack invariant (enforced)

Non-local patterns disable optimizations when executing on specific ranges; py-tree-sitter documents this explicitly. ([tree-sitter.github.io][4])

**Rule:** any pack you plan to run in changed ranges must have patterns that are:

* rooted (`is_pattern_rooted`)
* not non-local (`is_pattern_non_local == False`) ([tree-sitter.github.io][4])

(You already built this into your pack linter; keep it as a CI gate.)

---

## K.2 Batching strategy (caching + scheduling)

### K.2.1 Cache compiled Queries (pack compilation is not free)

**Do once per process**:

* load `.scm` sources
* compile `Query(language, source)`
* keep `Query` objects in a `dict[(lang, pack_name)]`

Then instantiate *fresh* `QueryCursor` per execution (cursor stores execution state and exposes per-run flags like `did_exceed_match_limit`). ([tree-sitter.github.io][3])

### K.2.2 Cache per-file Trees for incremental (LRU, size-aware)

Minimal viable:

* `FileState` holds `src: bytes`, `tree: Tree`
* LRU by “recently edited / recently queried”
* drop cold entries and fall back to full parse when needed

**Threading constraint:** syntax trees are not thread-safe to use on more than one thread at a time (Tree-sitter docs are quoted in the project’s thread-safety discussion). ([GitHub][5])
So either:

* keep each `Tree` confined to one worker thread, or
* `Tree.copy()` when you must share snapshots across threads (and never edit the shared copy). ([tree-sitter.github.io][1])

### K.2.3 Parser reuse vs per-thread Parsers

Tree-sitter maintainer guidance: you generally only need multiple parsers if parsing on multiple threads; otherwise reuse one parser (even across buffers). ([GitHub][6])

**Default topology:**

* single-thread batch: 1 `Parser` reused across files
* parallel: `N` workers, each owns:

  * 1 `Parser(language, timeout_micros=...)`
  * shared compiled `Query` objects (read-only)
  * per-task fresh `QueryCursor` instances

### K.2.4 Avoid copying file bytes when you can (callback parsing)

`Parser.parse()` can parse bytes provided in chunks by a callback that takes `(byte_offset, position)` and returns bytes starting at that offset. ([tree-sitter.github.io][2])
For huge repos / big generated files, this enables:

* mmap-backed reads
* partial materialization
* reduced peak memory

---

## K.3 Parallelism: “parse vs query” scheduling choices

### K.3.1 One-pass per file (recommended default)

In each worker:

1. parse (or incremental reparse)
2. run packs
3. emit rows

This keeps:

* tree + bytes “hot” in cache
* no cross-thread `Tree` sharing (avoids thread-safety problems) ([GitHub][5])

### K.3.2 Split parse and query (only if you must)

Only do this if you have a reason (e.g., you want to parse once and run many pack families). If you split:

* don’t ship `Tree` across threads unless you copy it (and accept memory overhead). ([GitHub][5])

### K.3.3 QueryCursor tuning knobs (cheap wins)

py-tree-sitter exposes:

* `set_max_start_depth(max_start_depth)` to limit where matching can begin (useful to avoid deep subtrees in huge files). ([tree-sitter.github.io][3])
* `match_limit` + `did_exceed_match_limit` to cap worst-case match explosion. ([tree-sitter.github.io][3])

---

## K.4 Memory safety rails (timeouts + match limits + windowing + file guards)

### K.4.1 Parser timeouts + “resume vs reset” semantics

`Parser(language, timeout_micros=...)` is supported; `parse()` returns `None` if the timeout expired. ([tree-sitter.github.io][2])
If parsing failed due to a timeout, the parser *resumes where it left off* on the next `parse()` by default; if you want to parse a different document instead, you must call `reset()`. ([tree-sitter.github.io][2])

**Operational rule:**

* timeout in interactive mode is expected → retry later / degrade gracefully
* timeout in batch mode is a bug signal → log + quarantine file
* always `parser.reset()` before reusing a timed-out parser on a different file ([tree-sitter.github.io][2])

### K.4.2 Query match limits (hard cap for pathological patterns)

`QueryCursor` exposes:

* `match_limit` = max number of in-progress matches
* `did_exceed_match_limit` = whether the cursor exceeded the limit last execution ([tree-sitter.github.io][3])

**Default policy:**

* set a finite `match_limit` for every run
* if exceeded:

  * mark results “partial”
  * optionally re-run a smaller-scope query (e.g., only defs) inside a tighter range

### K.4.3 Windowing standards for large files

**Always window** in incremental mode:

* containment window (`set_containing_byte_range`) as the correctness default ([tree-sitter.github.io][3])
* intersection window (`set_byte_range`) only when you intentionally want “touching” behavior (and you accept matches extending outside) ([tree-sitter.github.io][3])

### K.4.4 Pack-level de-risking: disable patterns/captures for “cheap modes”

py-tree-sitter lets you permanently disable captures and patterns on a `Query` (no undo). ([tree-sitter.github.io][4])
This is useful for “degraded mode” on massive files:

* disable expensive patterns (e.g., deep type traversal)
* keep only `rust_items`/`rust_uses`/`rust_errors`

### K.4.5 File guardrails (Rust repo reality)

In practice, you’ll want **index-tiering**:

* Tier 0: normal `.rs` files
* Tier 1: large files (over size threshold) → defs/uses/errors only, strict match_limit, strict timeouts
* Tier 2: excluded dirs (`target/`, vendored snapshots, generated dirs) → skip entirely unless explicitly requested

(These are deployment policy choices; Tree-sitter provides the knobs above to make them safe.)

---

## K.5 Recommended “starting defaults” (tune with metrics)

### K.5.1 Interactive / incremental (editor-like)

* `Parser(timeout_micros=20_000–100_000)` (20–100ms budget per parse call; retry on subsequent edits) ([tree-sitter.github.io][2])
* `QueryCursor(match_limit=10_000–50_000)` + check `did_exceed_match_limit` ([tree-sitter.github.io][3])
* always `set_containing_byte_range` around changed ranges (or a padded neighborhood) ([tree-sitter.github.io][3])
* `set_max_start_depth(3–8)` for “broad packs” (calls/types) on huge files ([tree-sitter.github.io][3])

### K.5.2 Batch / offline indexing

* higher `timeout_micros` or `None` (but still log outliers) ([tree-sitter.github.io][2])
* higher `match_limit`, but *never infinite* in untrusted repos ([tree-sitter.github.io][3])
* concurrency: per-worker Parser instances (don’t share Parser across threads) ([GitHub][6])
* avoid sharing Trees across threads; if you must, copy them ([GitHub][5])

---

## K.6 Minimal perf instrumentation (so tuning is mechanical)

Emit per-file + per-pack metrics:

* `parse_time_ms`, `parse_timed_out` (parse returns `None`) ([tree-sitter.github.io][2])
* `changed_ranges_count` + total bytes covered (changed_ranges contract) ([tree-sitter.github.io][1])
* per pack:

  * `pack_time_ms`
  * `match_limit`, `did_exceed_match_limit` ([tree-sitter.github.io][3])
  * `patterns_run` (count)
  * `rows_emitted`

This is the data you need to choose:

* tighter windows vs broader windows
* lower `max_start_depth`
* “degraded pack mode” cutovers
* directory/file exclusion thresholds

[1]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Tree.html "Tree — py-tree-sitter 0.25.2 documentation"
[2]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Parser.html "Parser — py-tree-sitter 0.25.2 documentation"
[3]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.QueryCursor.html "QueryCursor — py-tree-sitter 0.25.2 documentation"
[4]: https://tree-sitter.github.io/py-tree-sitter/classes/tree_sitter.Query.html "Query — py-tree-sitter 0.25.2 documentation"
[5]: https://github.com/tree-sitter/tree-sitter/issues/359 "Thread-safety question · Issue #359 · tree-sitter/tree-sitter · GitHub"
[6]: https://github.com/tree-sitter/tree-sitter/discussions/4495 "Can I use one parser for multiple buffers? · tree-sitter tree-sitter · Discussion #4495 · GitHub"

