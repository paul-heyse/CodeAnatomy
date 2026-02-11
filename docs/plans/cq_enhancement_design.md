According to a document from **February 10, 2026**, here’s the absolute best-in-class design I’d build on top of your existing CQ architecture to (1) deepen target insight, (2) add parent/child/sibling + call-context understanding, (3) keep output *high-signal by default*, and (4) stay fully **targeted + on-demand** with parallel collectors.

---

## The north-star design: Targeted Semantic Neighborhood Bundles + Progressive Disclosure

CQ already has the right “spine” for this: multi-language querying, AST-first targeted scanning, strong serialized contracts (msgspec), and a place for bundle concepts (`core/bundles.py`).

The best-in-class extension is to formalize a single idea:

### **Every query result should produce a “Semantic Neighborhood Bundle” (SNB) for each top target**

A bundle is:

* **Anchored** at a specific symbol/callsite span (not “the whole repo”)
* Built via **parallel, independent collectors** (AST-grep/tree-sitter, pyrefly LSP, rust-analyzer LSP, existing CQ enrichers)
* **Normalized** into stable schemas (no LSP unions leak)
* **Rendered** into an LDMD “rich markdown” artifact that supports **INDEX/GET/SEARCH/NEIGHBORS** reveal (so you don’t materialize everything into agent context up-front)

This keeps CQ targeted, but lets you go *much deeper* when the agent asks.

---

## 1) Data model upgrade: separate “semantic evidence” from “presentation markdown”

Your current `CqResult` is a pragmatic envelope, but it explicitly “conflates presentation and data,” and `Finding.category`/`details.data` have underspecified/loosely-typed areas that will get worse as you add more LSP richness.

### Recommended best-in-class change (compatible, incremental)

Keep `CqResult` as the outer envelope, but make **artifacts** the primary vessel for “heavy” semantics:

* `CqResult.key_findings`: **only** top-level, high-signal findings
* `CqResult.sections`: summary sections **with small payloads**
* `CqResult.artifacts`: add:

  * `format="bundle.msgpack"` (or json) → the normalized SNB(s)
  * `format="ldmd.md"` → the progressive-disclosure document for humans/agents

This matches your existing boundary discipline (msgspec structs cross module boundaries) and avoids ballooning `details.data` dicts.

### SNB schema (what I’d standardize)

A minimal SNB should include:

* `meta`: tool versions, server versions, config fingerprint, negotiated position encoding
* `anchor`: `{uri, range/position, anchor_kind}`
* `symbol_identity`: canonical symbol handle(s)
* `neighbors`: parent/child/sibling + type hierarchy + impl/override sets
* `usage`: references + callers + callees (depth-limited)
* `callsite_context`: containing function + local mutation signal + signature binding
* `diagnostics`: LSP diagnostics keyed to the relevant spans (target + context window)
* `raw_handles`: preserved LSP `data` tokens where required (call hierarchy items, etc.)

The pyrefly bundle docs you attached are already pointing in this direction (bundle_kind, meta, anchor, policy, findings, stable primitives like SpanRef/CallItem/data_token).

---

## 2) A unified “Collector → Normalizer → Assembler → Renderer” pipeline

CQ search already follows a multi-stage pipeline with parallelism and enrichment, plus pyrefly LSP integration (hover/diagnostics today).

Extend that pattern, but make it **explicit** for “neighborhood bundles”:

### 2.1 Collector layer (parallel, independent, targeted)

Collectors must obey:

* **top-K anchors**
* **small ranges**
* **depth-limited expansions**
* **timeouts**
* **caching by (workspace_root, config_fingerprint, uri, content_hash, method, pos/range)**

Those are exactly the fast-defaults your pyrefly bundle plan recommends (top-K, per-file didOpen, range-scoped inlay hints, cache keying, config fingerprinting).

Concretely:

#### A) Structural collector (fast, local)

* From existing `ScanContext.interval_index` (O(log n) containment queries) + def/call records, build:

  * containing def
  * sibling defs (same parent)
  * child defs (nested)
    This builds parent/child/sibling *even without LSP*, and is consistent with how CQ already assigns calls to defs and finds containing scopes.

#### B) Python semantic collector (pyrefly LSP)

Default “must-have” calls per anchor:

* hover (types/docs)
* signatureHelp when in call context
* definition/typeDefinition/implementation when needed for grounding
* references/callHierarchy only when agent expands “usage/call graph”

This aligns with the pyrefly minimal request plan (hover + signatureHelp + range-scoped inlay hints, caching).

#### C) Rust semantic collector (rust-analyzer LSP)

Default “must-have” calls per anchor:

* hover
* definition/typeDefinition/implementation
* documentSymbol (for file outline / parent-child symbol tree)
  Then expand on-demand:
* callHierarchy incoming/outgoing (call graph)
* typeHierarchy supertypes/subtypes (trait/impl relationships)
* references (usage edges)

These surfaces are explicitly called out in your rust LSP doc, including the value of `documentSymbol` as a per-file symbol index and the call/type hierarchy endpoints.

### 2.2 Normalization layer (make outputs stable + compact)

Best-in-class rule: **“no unions leak.”**
Normalize all LSP union return types (Location|Location[]|LocationLink[]|null; DocumentSymbol[]|SymbolInformation[]; etc.) into stable internal shapes. Your pyrefly notes explicitly emphasize this approach and why it matters for downstream stability.

Also: persist and normalize *position encoding* because range joins silently break if you get UTF-16 vs UTF-8 wrong (especially when you also track byte spans).

### 2.3 Assembler layer (build the neighborhood graph)

Build a **small graph** per target:

* **Scope tree edges** (parent/child/sibling) from:

  * AST containment (always available)
  * LSP `documentSymbol` when available (more accurate, includes impl blocks, traits, etc.)

* **Behavior edges**

  * Incoming/outgoing call edges via LSP callHierarchy (depth-limited)
  * Implementation/override expansion via LSP `implementation` / typeHierarchy (bounded fanout)
  * For Python, your pyrefly usage/callgraph bundle already encodes policies like max_depth/max_nodes/max_edges, which is exactly how to keep it targeted.

* **Callsite functional context**

  * containing def (AST interval)
  * signature binding from signatureHelp (argument-to-parameter mapping)
  * local mutation risk signal from `documentHighlight` (Read/Write) on demand

### 2.4 Renderer layer (prioritized front view + revealable depth)

This is where your “don’t consume agent context” goal is won or lost.

---

## 3) Progressive disclosure output: LDMD artifact + reveal commands (agent-friendly)

Your rich markdown notes are clear: Rich Markdown itself doesn’t give collapsible sections; real expand/collapse is done via Textual UI, while “hidden for LLM” is done by **render-once → reveal slices** via deterministic markers and a tiny protocol.

### Best-in-class output contract

When CQ runs (`search`, `q`, `calls`, etc.):

1. **Return minimal markdown** (high priority only)

   * 5–15 line executive summary
   * Key Findings (top N)
   * A “What else is available?” section listing expandable IDs + one-line descriptions + size hints

2. **Persist an LDMD doc artifact**

   * Use HTML comment markers (`<!--LDMD:BEGIN ...-->`) so they won’t materialize in Rich rendering
   * Include TL;DR blocks per section so agents can fetch summary-first

3. Support the reveal protocol:

   * `INDEX(path)` → metadata only
   * `GET(path, id, mode=tldr|body|full, depth, limit_bytes)` → slice
   * `SEARCH(path, query, scope_ids, max_hits)` → small snippets
   * `NEIGHBORS(path, id, before, after)` → surrounding context

### How this satisfies your requirement #4

You can *ship much deeper analysis* (parents/children/siblings, call graphs, impl sets, diagnostics), but the agent only pulls what it needs into context.

---

## 4) What “parent / child / sibling” should mean (and how to compute it correctly)

Best-in-class agents get confused if “parent” sometimes means “enclosing scope” and other times means “supertype.” So I recommend **three explicit relationship families**:

### A) Lexical scope relationships (always available)

* **Parent**: enclosing def/class/module (AST containment)
* **Children**: nested defs/methods inside the parent range
* **Siblings**: other children of the same parent

This can be computed from CQ’s interval index and records (already central to query execution).

### B) Symbol outline relationships (LSP-enhanced)

* Use `textDocument/documentSymbol`:

  * gives hierarchical `DocumentSymbol.children` when supported
  * great for Rust (impl blocks, traits, modules) and for “file outline without parsing”

Normalize into a canonical internal `SymbolNode` tree and reconcile with AST containment when they disagree.

### C) Type/dispatch relationships (semantic “parents”)

* Rust: type hierarchy supertypes/subtypes + implementations for trait/impl edges
* Python: pyrefly “go to implementation” to enumerate reimplementations/overrides (bounded)

In the rendered output, label these explicitly as **Type Parents / Type Siblings / Override Set**, not “parent/child” generically.

---

## 5) “Functional context where it is called”: best-in-class callsite cards

CQ query already builds callers/callees sections by mapping call records to defs and enriching containing definitions; keep that, but upgrade the *callsite payload* to be far more useful to an agent.

For each important callsite, emit a compact “Callsite Card”:

* **Callsite**: file:line + snippet
* **Containing function**: name + signature + return type (LSP hover or existing enrichment facts)
* **Argument binding**: parameters ↔ arguments (signatureHelp)
* **Receiver type** (when method call): inferred receiver type if available
* **Mutation risk**: local write occurrences (documentHighlight) *on-demand*
* **Dispatch expansion**: if base method, show small override set (implementation) with caps

Then put the “long tail” (full reference list, deep call hierarchy expansion, all diagnostics) behind LDMD sections.

---

## 6) Reuse CQ’s existing “Code Facts clusters” as the unifying presentation spine

You already have an enrichment fact cluster system with consistent semantic buckets (Identity & Grounding, Type Contract, Call Graph, Class/Method Context, etc.). It even notes Call Graph is Python-only today due to pyrefly integration—this is exactly where rust-analyzer callHierarchy should plug in.

Best-in-class move: **make the cluster system the canonical “view model”**, populated from:

* AST enrichment (baseline)
* LSP enrichment (preferred when present)
* Existing macros (bytecode surface, scopes, etc.)

This yields consistent output across Python and Rust.

---

## 7) Capability-based planning (so you stay targeted and fast)

Your query subsystem docs already suggest capability-based dispatching as an improvement: let the planner compute required capabilities and validate availability before executing.

Do exactly that for LSP depth:

* Default plan only requires:

  * AST-grep/tree-sitter + (optional) hover
* Expansion plan adds:

  * references/callHierarchy/typeHierarchy/etc.

This avoids “starting rust-analyzer + indexing the world” unless the agent explicitly asks for deeper semantic edges.

---

## 8) Concrete “best-in-class” feature set to implement first

If you want the highest ROI sequence:

### Phase 1 — Progressive disclosure foundation (biggest context win)

1. Add LDMD artifact rendering + INDEX/GET command (or subcommand)
2. Update markdown renderer to output only prioritized content by default (and link to section IDs)

(Your markdown renderer is currently large/monolithic; extracting sub-renderers will make this change much easier to test and evolve.)

### Phase 2 — Rust LSP parity with your pyrefly approach

1. Implement rust-analyzer session handshake persistence (capabilities + positionEncoding)
2. Add `documentSymbol` → parent/child/sibling outline sections
3. Add callHierarchy incoming/outgoing (depth-limited) behind an expansion section

### Phase 3 — Callsite cards (functional context)

1. signatureHelp + argument binding for Python callsites (pyrefly)
2. Rust callsites: callHierarchy + hover + definition/typeDefinition as needed
3. Optional on-demand mutation signal via documentHighlight (Python first)

### Phase 4 — Schema tightening (future-proofing)

* Promote `Finding.category` to an enum / canonical taxonomy (eliminate “callsite” vs “call_site” drift)
* Replace untyped dict escape hatches with discriminated unions where it matters most (LSP normalized payloads)

---

## What this design gives you against your 5 targets

1. **Enrich outputs for the search target**
   → hover/signature/type-definition/implementation + normalized “Identity & Grounding / Type Contract” clusters.

2. **Parent/child/sibling objects**
   → AST containment tree (always) + LSP `documentSymbol` tree (better) + explicit “type parents/override sets.”

3. **Functional context of callsites**
   → callsite cards: containing def + arg binding + dispatch expansion + (optional) mutation signal.

4. **Prioritized initial markdown + expandability**
   → LDMD artifact + reveal protocol (INDEX/GET/SEARCH/NEIGHBORS) + Textual UI for real folding.

5. **Targeted, on-demand, parallel**
   → top-K anchors + range-scoped requests + depth caps + caching + capability-gated expansions (no CQ-built global index).

---

If you want, I can also propose a concrete **`SemanticNeighborhoodBundleV1`** schema (msgspec.Struct fields + `details.kind` registry for both Python+Rust) that maps 1:1 onto your existing `Finding`/`Section` model while keeping heavy payloads in artifacts—so you can roll this out without breaking current consumers.

Below is a **concrete, versioned** `SemanticNeighborhoodBundleV1` contract that:

* **Reuses your existing `Finding` + `Section` + `Artifact` types** so it can be rendered/consumed the same way as today (1:1 mapping via the `view` field). `Finding` already has `category/message/anchor/severity/details`, and `details` already supports `kind/score/data` for typed-ish payloads without changing the envelope. 【turn3:0†06_data_models.md†L62-L76】【turn5:0†06_data_models.md†L12-L16】【turn28:8†06_data_models.md†L22-L45】
* Keeps **heavy payloads** (large lists, raw LSP responses, trees/graphs) in **artifacts** (`Artifact(path, format)`), while keeping only **small previews + counts** in the materialized `details.data` and/or bundle fields.【turn28:8†06_data_models.md†L37-L50】
* Aligns with the already-documented **pyrefly symbol grounding bundle pattern** (`bundle_kind` + normalized shape + meta) and similarly accommodates Rust LSP expansions like call/type hierarchy (which you explicitly want to integrate).【turn28:6†pyrefly_lsp_data.md†L51-L77】【turn20:4†rust_lsp.md†L26-L46】

---

## 1) Concrete schema: `SemanticNeighborhoodBundleV1` (msgspec.Struct / CqStruct)

> This is written to drop cleanly into your existing `tools/cq/core/contracts.py` style: `bundle_kind` is the discriminator, and the `view` is **literally** `Finding/Section` objects so you can surface it without any new rendering logic.
> Heavy stuff goes behind `Artifact` pointers (path+format).【turn28:8†06_data_models.md†L22-L45】

```python
from __future__ import annotations

from typing import Literal, TypeAlias
import msgspec

# Reuse your existing schema primitives (1:1 mapping to current consumers)
from cq.core.schema import CqStruct, Anchor, Finding, Section, Artifact


# -------------------------
# Shared small primitives
# -------------------------

LanguageId: TypeAlias = Literal["python", "rust"]

NeighborhoodSliceKind: TypeAlias = Literal[
    # structural neighborhood
    "parents",
    "children",
    "siblings",
    "enclosing_context",   # "where is this used / what scope contains it?"
    # functional neighborhood
    "callers",
    "callees",
    "references",
    "implementations",
    "type_supertypes",
    "type_subtypes",
    # module-level neighborhood
    "imports",
    "importers",
    # optional catch-all
    "related",
]

RelationKind: TypeAlias = Literal[
    "contains",          # parent -> child
    "calls",             # caller -> callee
    "references",        # referrer -> referent
    "imports",           # importer -> imported
    "type_super",        # subtype -> supertype
    "type_sub",          # supertype -> subtype
    "implements",        # impl -> trait/interface
    "implemented_by",
]


class LspPositionV1(CqStruct, frozen=True):
    # LSP positions are 0-based (line/character)
    line: int
    character: int


class LspRangeV1(CqStruct, frozen=True):
    start: LspPositionV1
    end: LspPositionV1


class LspSpanRefV1(CqStruct, frozen=True):
    # Minimal "anchor" form used by your pyrefly bundle docs: uri + pos + selection.
    uri: str
    pos: LspPositionV1
    selection: LspRangeV1


# -------------------------
# Neighborhood graph layer
# -------------------------

class SemanticNodeRefV1(CqStruct, frozen=True):
    """
    Lightweight node descriptor. Keep this SMALL.
    If you need big payloads (doc trees, full hover markdown, long reference lists),
    use `attachments` to point at artifacts.
    """
    node_id: str                       # opaque but stable within a run/bundle
    language: LanguageId
    display_name: str                  # what you show in markdown
    symbol_kind: str | None = None     # e.g. function/class/method/trait/struct/enum/module
    qname: str | None = None           # best-effort fully qualified name
    anchor: Anchor | None = None       # CQ anchor (file/line/col); may be missing for externals
    lsp: LspSpanRefV1 | None = None    # optional LSP span if available
    container_id: str | None = None    # parent container in doc-symbol tree (if known)
    relevance: float | None = None     # optional rank score (0..1) for ordering

    # "attachments" are *pointers* to heavy expansions; each entry typically maps
    # to a details.kind schema in the registry below.
    attachments: tuple["AttachmentRefV1", ...] = ()


class SemanticEdgeV1(CqStruct, frozen=True):
    kind: RelationKind
    src: str  # node_id
    dst: str  # node_id
    confidence: float | None = None
    note: str | None = None
    evidence_artifact: Artifact | None = None  # optional pointer to raw evidence list


class AttachmentRefV1(CqStruct, frozen=True):
    """
    A typed pointer to something heavy (raw LSP result, full list, rich-md doc).
    `kind` should correspond to a details.kind entry in the registry below.
    """
    kind: str
    # tiny preview only (counts / short strings). keep this small.
    preview: dict[str, object] = msgspec.field(default_factory=dict)
    # heavy payload location
    artifact: Artifact | None = None


class NeighborhoodSliceV1(CqStruct, frozen=True):
    """
    A slice is what becomes a Section in the standard CQ view.
    """
    kind: NeighborhoodSliceKind
    title: str | None = None

    total: int = 0
    preview: tuple[SemanticNodeRefV1, ...] = ()
    full_artifact: Artifact | None = None

    # default UX hint: your Section has `collapsed`
    collapsed: bool = True

    notes: tuple[str, ...] = ()


class NeighborhoodGraphSummaryV1(CqStruct, frozen=True):
    node_count: int = 0
    edge_count: int = 0

    # Full node/edge lists can be kept out of the main bundle if desired
    # (or keep them inline if they remain small).
    full_graph_artifact: Artifact | None = None


# -------------------------
# 1:1 mapping to CQ output
# -------------------------

class BundleViewV1(CqStruct):
    """
    This is the 1:1 bridge to the current CQ presentation model:
    it is literally (Finding/Section) lists, so you can drop it into CqResult
    without breaking current consumers.
    """
    key_findings: list[Finding] = msgspec.field(default_factory=list)
    sections: list[Section] = msgspec.field(default_factory=list)
    evidence: list[Finding] = msgspec.field(default_factory=list)


class SemanticNeighborhoodMetaV1(CqStruct, frozen=True):
    tool: str = "cq"
    tool_version: str | None = None

    # provenance / reproducibility
    workspace_root: str | None = None
    query_text: str | None = None
    created_at_ms: float | None = None

    # optional: capture LSP server identity/config in a small way
    lsp_servers: tuple[dict[str, object], ...] = ()  # keep small; dump full into an artifact if needed

    # deterministic preview sizing knobs used when producing the bundle
    limits: dict[str, int] = msgspec.field(default_factory=dict)


class SemanticNeighborhoodBundleV1(CqStruct, frozen=True):
    """
    The canonical "semantic neighborhood" contract (store as an Artifact).
    """
    bundle_kind: Literal["cq.semantic_neighborhood_bundle.v1"] = "cq.semantic_neighborhood_bundle.v1"

    meta: SemanticNeighborhoodMetaV1 = msgspec.field(default_factory=SemanticNeighborhoodMetaV1)

    # The "subject" of the neighborhood (the thing the user searched / matched)
    subject: SemanticNodeRefV1 | None = None

    # Structured neighborhood (parents/children/siblings/callers/etc)
    slices: tuple[NeighborhoodSliceV1, ...] = ()

    # Optional lightweight graph summary; full graph can be an artifact
    graph: NeighborhoodGraphSummaryV1 | None = None

    # The 1:1 “materialized view” in your existing output primitives
    view: BundleViewV1 = msgspec.field(default_factory=BundleViewV1)

    # Any extra artifacts emitted while building the bundle (raw LSP JSON, rich-md docs, etc.)
    artifacts: tuple[Artifact, ...] = ()

    diagnostics: tuple[str, ...] = ()


# -------------------------
# A tiny "pointer" payload for Finding.details.data
# -------------------------

class SemanticNeighborhoodBundleRefV1(CqStruct, frozen=True):
    """
    Put THIS in Finding.details.data for a single stub Finding,
    so consumers can lazy-load the full bundle from artifacts.
    """
    bundle_kind: Literal["cq.semantic_neighborhood_bundle.v1"] = "cq.semantic_neighborhood_bundle.v1"
    bundle_artifact: Artifact

    subject_node_id: str | None = None

    # small rollup so you can show value without loading the artifact
    counts: dict[str, int] = msgspec.field(default_factory=dict)
    notes: tuple[str, ...] = ()
```

**Why this maps 1:1 cleanly:** `BundleViewV1` is *literally* `key_findings: list[Finding]`, `sections: list[Section]`, and `evidence: list[Finding]`, matching the existing `CqResult` containers. Your `Section` already holds `findings` and `collapsed`, and `Finding.details` already holds `kind/score/data` for attaching structured payloads. 【turn28:8†06_data_models.md†L22-L45】【turn3:0†06_data_models.md†L62-L76】【turn5:0†06_data_models.md†L12-L16】

---

## 2) LSP bundle contracts to plug into `attachments` (small previews + artifact pointers)

### 2.1 Pyrefly symbol grounding (preview + artifact for large target sets)

This intentionally follows your documented normalized `pyrefly.symbol_grounding_bundle.v1` pattern (bundle_kind + meta + anchor + target sets).【turn28:6†pyrefly_lsp_data.md†L51-L77】

```python
class LspSymbolTargetV1(CqStruct, frozen=True):
    uri: str
    selection: LspRangeV1

    name: str | None = None
    kind: str | None = None
    container_name: str | None = None
    detail: str | None = None

    score: float | None = None
    deprecated: bool | None = None


class TargetSetV1(CqStruct, frozen=True):
    total: int = 0
    preview: tuple[LspSymbolTargetV1, ...] = ()
    full_artifact: Artifact | None = None


class PyreflySymbolGroundingBundleV1(CqStruct, frozen=True):
    bundle_kind: Literal["pyrefly.symbol_grounding_bundle.v1"] = "pyrefly.symbol_grounding_bundle.v1"

    meta: dict[str, object] = msgspec.field(default_factory=dict)
    anchor: LspSpanRefV1

    # optional server-specific symbol identity
    symbol: str | None = None

    definition_targets: TargetSetV1 = msgspec.field(default_factory=TargetSetV1)
    declaration_targets: TargetSetV1 = msgspec.field(default_factory=TargetSetV1)
    type_definition_targets: TargetSetV1 = msgspec.field(default_factory=TargetSetV1)
    implementation_targets: TargetSetV1 = msgspec.field(default_factory=TargetSetV1)
    references_targets: TargetSetV1 = msgspec.field(default_factory=TargetSetV1)

    hover: str | None = None
    signature: str | None = None

    # keep policies/assumptions small here; dump full to an artifact if needed
    policy: dict[str, object] = msgspec.field(default_factory=dict)
```

### 2.2 Rust LSP call/type hierarchy bundles (preview + artifact)

Rust LSP expansions you called out include **workspace symbols**, **call hierarchy**, and **type hierarchy**.【turn20:4†rust_lsp.md†L26-L46】

```python
class LspCallHierarchyItemV1(CqStruct, frozen=True):
    name: str
    kind: str | None = None

    uri: str | None = None
    selection: LspRangeV1 | None = None
    detail: str | None = None
    container_name: str | None = None


class CallLinkV1(CqStruct, frozen=True):
    peer: LspCallHierarchyItemV1
    ranges: tuple[LspRangeV1, ...] = ()


class CallLinkSetV1(CqStruct, frozen=True):
    total: int = 0
    preview: tuple[CallLinkV1, ...] = ()
    full_artifact: Artifact | None = None


class RustCallHierarchyBundleV1(CqStruct, frozen=True):
    bundle_kind: Literal["rust.call_hierarchy_bundle.v1"] = "rust.call_hierarchy_bundle.v1"

    meta: dict[str, object] = msgspec.field(default_factory=dict)
    anchor: LspSpanRefV1

    # LSP’s prepare phase may return multiple items; keep preview small
    prepared: tuple[LspCallHierarchyItemV1, ...] = ()

    incoming: CallLinkSetV1 = msgspec.field(default_factory=CallLinkSetV1)
    outgoing: CallLinkSetV1 = msgspec.field(default_factory=CallLinkSetV1)

    # record known semantics knobs / approximations (keep small)
    semantics: dict[str, object] = msgspec.field(default_factory=dict)


class RustTypeHierarchyBundleV1(CqStruct, frozen=True):
    bundle_kind: Literal["rust.type_hierarchy_bundle.v1"] = "rust.type_hierarchy_bundle.v1"

    meta: dict[str, object] = msgspec.field(default_factory=dict)
    anchor: LspSpanRefV1

    prepared: tuple[LspCallHierarchyItemV1, ...] = ()  # reuse item shape for type nodes

    supertypes: CallLinkSetV1 = msgspec.field(default_factory=CallLinkSetV1)
    subtypes: CallLinkSetV1 = msgspec.field(default_factory=CallLinkSetV1)

    semantics: dict[str, object] = msgspec.field(default_factory=dict)
```

---

## 3) `details.kind` registry (Python + Rust + CQ neighborhood)

`Finding.details.kind` is already a string discriminator with `data: dict[str, object]` payload, so you can add these kinds without touching the envelope. 【turn5:0†06_data_models.md†L12-L16】【turn3:0†06_data_models.md†L62-L76】

Here’s a **concrete registry** that (a) introduces the CQ neighborhood kinds, and (b) registers the LSP bundle kinds you’re integrating (pyrefly + rust). For pyrefly, I include both the “kind” strings used in your docs (e.g. `pyrefly.lsp.symbol_grounding`) and a versioned alias, so you have a safe migration path. 【turn28:6†pyrefly_lsp_data.md†L66-L77】【turn20:4†rust_lsp.md†L26-L46】

```python
DETAILS_KIND_REGISTRY: dict[str, type[msgspec.Struct]] = {
    # --------------------------------
    # CQ Semantic Neighborhood (new)
    # --------------------------------
    "cq.semantic_neighborhood.bundle_ref.v1": SemanticNeighborhoodBundleRefV1,
    "cq.semantic_neighborhood.node.v1": SemanticNodeRefV1,
    "cq.semantic_neighborhood.edge.v1": SemanticEdgeV1,
    "cq.semantic_neighborhood.slice.v1": NeighborhoodSliceV1,

    # --------------------------------
    # Pyrefly LSP (new integrations)
    # --------------------------------
    # canonical doc-style kind
    "pyrefly.lsp.symbol_grounding": PyreflySymbolGroundingBundleV1,
    # versioned alias (recommended for long-term stability)
    "pyrefly.lsp.symbol_grounding.v1": PyreflySymbolGroundingBundleV1,

    # (Optional next: document symbols / workspace symbols / highlights)
    # These are explicitly called out as useful surfaces in your pyrefly LSP notes.
    # Keep their payloads "preview+artifact" like the symbol grounding bundle.

    # --------------------------------
    # Rust LSP (new integrations)
    # --------------------------------
    "rust.lsp.call_hierarchy.v1": RustCallHierarchyBundleV1,
    "rust.lsp.type_hierarchy.v1": RustTypeHierarchyBundleV1,
    # (Optional next: rust workspace symbols search/resolve)
}
```

---

## 4) How this rolls out without breaking consumers

* **Current consumers** keep reading `CqResult.key_findings/sections/evidence` as usual. Your `Section`/`Finding` structures stay unchanged.【turn28:8†06_data_models.md†L72-L84】【turn3:0†06_data_models.md†L62-L76】
* You add **one new Finding** (often in `key_findings` or a small top Section) whose `details.kind = "cq.semantic_neighborhood.bundle_ref.v1"` and whose `details.data` is the builtins form of `SemanticNeighborhoodBundleRefV1` pointing to an artifact path.
* The **full** `SemanticNeighborhoodBundleV1` is stored as an **artifact** (json/msgpack). This matches your existing artifact mechanism (`Artifact(path, format)`).【turn28:8†06_data_models.md†L37-L50】

If you want to go one step further for rich-markdown + textual UI, make `SemanticNeighborhoodBundleV1.artifacts` include a `rich-md` document artifact that contains the “expandable” blocks; your rich-md approach is explicitly designed to avoid materializing everything up front. 【turn21:0†rich_md_overview.md†L1-L15】

---

If you want, I can also sketch the **exact section layout** I’d recommend emitting into `bundle.view.sections` (titles + default collapsed flags + which slices go where) so it aligns with your “prioritized initial markdown + expandable detail” goal—without changing any of the above contracts.

According to a document from **February 11, 2026**, CQ already gives you exactly the two primitives you need for the “prioritized initial markdown + expandable detail” UX: a **`Section(title, findings, collapsed)`** container with an explicit default-collapse flag, and a flexible **`Finding(category, message, anchor, severity, details(kind, score, data))`** payload that can point to artifacts for heavy data. 

Below is the **exact `bundle.view.sections` layout** I’d emit for **SemanticNeighborhoodBundleV1**, including **titles, default collapsed behavior, and which slices go where**—while staying 100% within your existing `Section` + `Finding` contracts (no schema changes, just `details.kind` taxonomy + ordering). 

---

## bundle.view.sections layout (ordered)

### 01) Target — TL;DR

* **collapsed:** `False`
* **Purpose:** “Enrich outputs directly pertaining to the search target” (your goal #1) with *high-signal, low-bytes* facts only.
* **Include these slices / finding kinds (thin only):**

  * `symbol.identity.v1` (name, kind, language, canonical URI/path, primary span/anchor)
  * `symbol.signature.summary.v1` (short signature label; Python: from hover/signatureHelp; Rust: from hover)
  * `symbol.doc.summary.v1` (1–3 line summary; no full docstring)
  * `symbol.grounding.confidence` (derived rubric; e.g. `pyrefly.derive.symbol_grounding.confidence`) so the agent knows whether it’s safe to act automatically. 
* **If available (still thin):**

  * Python target: `pyrefly.lsp.hover` **but trimmed** into a normalized “type_string + short docs” structure.
  * Rust target: `rust.lsp.hover` **trimmed** similarly.
* **Heavy payload rule:** never inline full hover markdown blocks here—store full hover in an artifact and put only `artifact_ref` + a short extracted summary in `details.data`.

---

### 02) Neighborhood — Summary

* **collapsed:** `False`
* **Purpose:** Satisfy your goals #2 and #3 *without dumping the world*: show **counts + top-K** parents/children/siblings + call-context pointers.
* **Include these slices / kinds (thin):**

  * `neighborhood.summary.v1` (counts + top-K lists: parents, children, siblings, incoming callers, outgoing callees)
  * `call_context.summary.v1` (how many callsites were found; how many distinct enclosing scopes; top-K most relevant callers)
* **Default top-K:** `K=5` per relationship category in the rendered markdown; store full lists in artifacts.
* **Where the data comes from (targeted):**

  * Python: use your existing targeted scanning + optional pyrefly enrich on *selected anchors* (you already recommend `K=15` anchors total and per-file caps). 
  * Rust: call hierarchy / type hierarchy are explicitly *symbol-targeted* LSP surfaces (incoming/outgoing calls, supertypes/subtypes). 

---

### 03) Parents — Containers / Supertypes

* **collapsed:** `False` if `parent_count <= 3`, else `True`
* **Purpose:** Parent chain context (goal #2), but keep it compact by default.
* **Include these slices / kinds:**

  * `neighborhood.parent.list.v1` (thin list entries)

    * Each entry as a `Finding` with:

      * `category="symbol.parent"`
      * `details.kind="neighborhood.parent.item.v1"`
      * `details.data={name, kind, uri/path, span/anchor, relationship="contains|owns|encloses|supertype", score}`
  * **Rust-specific optional add-on (still thin):**

    * `rust.lsp.type_hierarchy.supertypes.summary` (names + anchors only; full items/artifacts elsewhere). 
* **Defer-by-default (artifact-backed):**

  * parent hover/type detail snapshots (only fetch when agent expands)

---

### 04) Children — Members / Nested / Subtypes

* **collapsed:** `True` (almost always)
* **Purpose:** Show child objects (goal #2) without exploding context (classes → many methods; modules → many defs).
* **Include these slices / kinds:**

  * `neighborhood.child.list.v1` (thin list; top few rendered; full list artifact)
  * Rust: `rust.lsp.type_hierarchy.subtypes.summary` if relevant. 
* **Sizing rule:**

  * Inline at most `min(10, child_count)` items as individual Findings.
  * If more: emit **one** “Children index” Finding + `artifact_ref` to full JSON/msgpack.

---

### 05) Siblings — Same Parent Scope

* **collapsed:** `True`
* **Purpose:** Explicit sibling view (goal #2), usually high fan-out.
* **Include these slices / kinds:**

  * `neighborhood.sibling.list.v1`
  * Each sibling item is thin: name/kind/anchor + short “why it’s a sibling” label (same class, same module, same impl block).
* **Render rule:** show only top 5 siblings by relevance score; full list → artifact.

---

### 06) Callers (Incoming) — Who calls this

* **collapsed:** `True`
* **Purpose:** Your goal #3 (“functional context in which it is called”) starts here: callers are the entry points into target behavior.
* **Include these slices / kinds:**

  * `callgraph.incoming.summary.v1` (counts, distinct caller scopes)
  * `callgraph.incoming.topk.v1` (top-K caller scopes)
  * `callsite.incoming.topk.v1` (top-K callsites, each with anchor + containing-scope pointer)
* **Rust path (preferred):**

  * Use `textDocument/prepareCallHierarchy` + `callHierarchy/incomingCalls` summarized into thin Findings (full raw → artifact). 
* **Python path (preferred):**

  * Keep using your existing targeted callsite census mechanics (consistent with current “callers” expander semantics). 

---

### 07) Callees (Outgoing) — What this calls

* **collapsed:** `True`
* **Purpose:** Completes call-context (goal #3): what the target depends on.
* **Include these slices / kinds:**

  * `callgraph.outgoing.summary.v1`
  * `callgraph.outgoing.topk.v1`
  * `callsite.outgoing.topk.v1` (optional)
* **Rust path (preferred):**

  * `callHierarchy/outgoingCalls` summarized thinly. 
* **Python path (preferred):**

  * Use your existing “callees” extraction within definitions (matches current query subsystem behavior). 

---

### 08) Callsite Context — Enclosing function / local contract

* **collapsed:** `True` (but auto-expand if there are ≤3 callsites total)
* **Purpose:** This is the “behavioral context” section: **for each high-priority callsite**, show the *enclosing function’s* signature/role and any local type info that changes how the call behaves.
* **Include these slices / kinds (thin by default):**

  * `callsite.context.enclosing_scope.summary.v1` per callsite:

    * caller function/method name
    * caller anchor
    * 1–2 line snippet around the call (already a CQ strength)
* **Python enriched add-on (targeted, efficient):**

  * For only the top-K callsites: request `pyrefly` **Hover** at the call expression + **SignatureHelp** when call context is detected, and **InlayHints** once per small windowed range (not per call). 
  * Store the full normalized LSP payloads in artifacts; inline only extracted “active signature + active param + inferred types” summaries.

---

### 09) Imports / Module Context (Targeted)

* **collapsed:** `True`
* **Purpose:** Give enough import/module context to disambiguate symbols and help agents avoid wrong edits—without full-file dumps.
* **Include these slices / kinds:**

  * `module.context.summary.v1` (module path, package, nearest import statements related to target)
  * `symbol.resolution.notes.v1` (any disambiguation notes)
* **Alignment:** this mirrors how CQ already has an “imports” section in definition query processing. 

---

### 10) LSP Deep Signals — Types, tokens, outline (Deferred)

* **collapsed:** `True`
* **Purpose:** Park *high value but potentially huge* LSP-derived structures behind an explicit expand.
* **Include these slices / kinds:**

  * Python:

    * `pyrefly.lsp.inlay_hints.range` (summary + artifact ref)
    * `pyrefly.lsp.document_symbols` (summary + artifact ref)
    * `pyrefly.lsp.semantic_tokens.*` (never inline decoded token streams; summarize + artifact ref) 
  * Rust:

    * `rust.lsp.document_symbols` summary + artifact ref
    * `rust.lsp.semantic_tokens` summary + artifact ref
* **Why this stays collapsed:** it’s exactly the kind of data that “needlessly consumes agent context” unless explicitly requested.

---

### 11) Suggested Follow-ups (Commands)

* **collapsed:** `False`
* **Purpose:** Keep the tool “agent-drivable”: present the next 3–8 *targeted* expansions the agent can request.
* **Include these slices / kinds:**

  * `suggested_followups.v1` as Findings like:

    * “Expand children (full)”
    * “Expand incoming callers (depth=2)”
    * “Fetch pyrefly inlay hints for enclosing scope”
    * “Fetch rust call hierarchy for top caller”
* **Alignment:** mirrors existing CQ “Suggested Follow-ups” section in smart search. 

---

### 12) Provenance / Budgets / Artifacts

* **collapsed:** `True`
* **Purpose:** Debuggability and reproducibility without clutter.
* **Include these slices / kinds:**

  * `run.meta.v1` (toolchain versions, timing, caps hit)
  * `budget.report.v1` (how many anchors queried; what was deferred)
  * `artifact.index.v1` (list artifacts with purpose tags)
* **Why:** Your contracts already treat artifacts as first-class outputs, and sections are order-driven (no priority field), so keeping this last + collapsed matches how CQ already relies on section ordering + collapse for UX. 

---

## How this layout achieves “prioritized initial markdown + expandable detail” without schema changes

* **Collapse is the contract-level lever** (already in `Section`) you’re using in search (“Non-code matches” collapsed by default), and you can reuse the same posture here for high-fanout relationship sets. 
* **“Not materialized unless agent commands”** is implemented by:

  1. emitting only **summary Findings** in uncollapsed sections, and
  2. storing heavy payloads (full LSP responses, full neighbor lists) in **artifacts**, referenced by `artifact_ref` in `details.data`.
* If you want the *UI/interaction* to feel clean, your `rich_md_overview`’s “reveal protocol” pattern (metadata index first, then targeted GET with byte/line limits) maps extremely well to this: show *indexes by default*, fetch bodies only on expand. 

---

## Optional (but I recommend it): deterministic collapse heuristics

Even though I listed defaults above, you can make it best-in-class by setting `Section.collapsed` with a simple deterministic rule (still no schema changes):

* Collapse if **estimated rendered bytes** would exceed your “default view budget”.
* Or collapse if `len(section.findings) > N` (e.g., 12).
* Always keep **01**, **02**, **11** uncollapsed.

This keeps output consistent and prevents “one weird symbol” from blowing up the view.

---


