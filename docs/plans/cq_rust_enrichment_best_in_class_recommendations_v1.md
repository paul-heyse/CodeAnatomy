# CQ Rust Enrichment: Best-in-Class Recommendations v1

**Revision**: Updated 2026-02-06 incorporating review feedback from
`docs/plans/cq_rust_enrichment_proposal_review_2026-02-06.md`.

## Problem Statement

The current Rust enrichment in `tools/cq/search/tree_sitter_rust.py` produces four fields per match:

| Field | Value |
|-------|-------|
| `node_kind` | Tree-sitter node type at match point |
| `scope_chain` | List of enclosing scope names |
| `scope_kind` | Kind of nearest enclosing scope |
| `scope_name` | Name of nearest enclosing scope |

This is minimal context. An agent reviewing Rust code must issue follow-up queries to answer routine questions that a richer single-pass enrichment could answer immediately:

- "Is this a method or a free function?"
- "What are the parameters and return type?"
- "Is this inside a trait impl or an inherent impl?"
- "Does this function have `#[test]` or `#[cfg(...)]` attributes?"
- "Is this `pub`, `pub(crate)`, or private?"
- "Is this a function call, method call, or macro invocation?"

Each of these questions currently costs a separate query. The recommendations below eliminate 2-4 follow-up queries per Rust match by extracting high-value structured fields from existing parse infrastructure.

## Architecture: Two-Tier Extraction Pipeline

CQ already parses Rust through ast-grep in the search/classifier and rule-scanning paths. Rather than relying exclusively on tree-sitter-rust for all enrichment, the design uses a two-tier extraction model:

**Tier 1 (default): ast-grep-py extraction** from the existing parsed `SgRoot`. This is the primary semantic extraction path. It avoids redundant parsing and keeps primary logic aligned across Python and Rust surfaces.

**Tier 2 (targeted): tree-sitter-rust augmentation** only when Tier 1 cannot provide requested fields. Tree-sitter is uniquely strong for: query packs, injection metadata, token-tree scoped analysis, and fine-grained scoped captures.

### Deterministic Precedence Rules

1. Use the ast-grep value when present and non-empty.
2. Fill gaps from tree-sitter.
3. Never override an existing non-empty value unless explicitly configured as a canonical override.

This avoids conflicting output across identical matches and keeps enrichment deterministic.

## Compute Budget

Tier 1 operates on the ast-grep `SgRoot` that the classifier already produces. Tier 2 uses tree-sitter field-based child access (`child_by_field_name`) on an already-parsed and cached tree, which is O(1) per field lookup. The incremental cost per match is microseconds of node walking, well within the 2-3x compute budget.

## Current Integration Point

```
smart_search.py:_maybe_rust_tree_sitter_enrichment()
  -> tree_sitter_rust.py:enrich_rust_context()
     -> returns dict[str, object] stored as EnrichedMatch.rust_tree_sitter
        -> passed through to Finding.details.data["rust_tree_sitter"]
```

The `scope_name` field is also used as a fallback for `containing_scope` in match grouping. All other fields are passed through as opaque data.

## Enrichment Contract

The enrichment contract is **strictly additive**:

> All fields produced by enrichment are strictly additive.
> They **never** affect: confidence scores, match counts, category classification, or relevance ranking.
> They **may** affect: `containing_scope` display (used only for grouping in output), kept deterministic and optional.

Enrichment fields live exclusively in `details.data["rust_tree_sitter"]`. No enrichment signal feeds into scoring, ranking, or classification logic. The existing `containing_scope` fill-in from `scope_name` remains the sole display-level fallback.

## Unified Enrichment Field Groups

All enrichment fields are organized into explicit groups:

| Group | Fields | Source Tier |
|-------|--------|-------------|
| **identity** | `node_kind`, `item_role`, `language` | Tier 1 primary |
| **scope** | `scope_chain`, `scope_kind`, `scope_name`, `impl_context` (`impl_type`, `impl_trait`, `impl_kind`) | Tier 1 + Tier 2 gap-fill |
| **signature** | `signature`, `params`, `return_type`, `generics`, `is_async`, `is_unsafe` | Tier 2 primary |
| **modifiers** | `visibility`, `attributes` | Tier 1 + Tier 2 gap-fill |
| **call** | `call_target`, `call_receiver`, `call_method`, `macro_name` | Tier 1 primary |
| **shape** | `struct_field_count`, `struct_fields`, `enum_variant_count`, `enum_variants` | Tier 2 primary |
| **runtime** | `enrichment_status`, `enrichment_sources`, `degrade_reason` | Always |

The `runtime` group provides observability into which enrichment tiers contributed:
- `enrichment_status`: `"applied"` | `"skipped"` | `"degraded"`
- `enrichment_sources`: `["ast_grep"]`, `["ast_grep", "tree_sitter"]`, or `["tree_sitter"]`
- `degrade_reason`: Optional string when status is `"degraded"`

---

## Recommendation 1: Function Signature Extraction

### What

When the match or its nearest scope is a `function_item`, extract the full signature skeleton.

### Fields Produced

| Field | Source | Example |
|-------|--------|---------|
| `signature` | Byte-sliced from source before body | `pub fn build_graph(ctx: &Context, opts: Options) -> Result<Graph>` |
| `params` | `parameters` field children | `["ctx: &Context", "opts: Options"]` |
| `return_type` | `return_type` field | `Result<Graph>` |
| `generics` | `type_parameters` field | `<T: Clone>` |
| `is_async` | Check for `async` keyword child | `true` / `false` |
| `is_unsafe` | Check for `unsafe` keyword child | `true` / `false` |

### Tree-Sitter Node Structure

```
function_item [fields: visibility_modifier, name, type_parameters, parameters, return_type, body]
```

All fields are accessed via `node.child_by_field_name(...)`. The `body` field is explicitly **not** extracted (would bloat output).

### Implementation

**Correction from review (C2):** The original implementation sliced decoded strings with byte offsets, which is incorrect for non-ASCII source. The correct approach slices bytes first, then decodes:

```python
def _extract_function_signature(fn_node: Node, source_bytes: bytes) -> dict[str, object]:
    sig: dict[str, object] = {}
    # Parameters (exclude body)
    params_node = fn_node.child_by_field_name("parameters")
    if params_node:
        param_texts = [_node_text(c, source_bytes) for c in params_node.named_children]
        sig["params"] = [p for p in param_texts if p is not None]
    ret = fn_node.child_by_field_name("return_type")
    if ret:
        sig["return_type"] = _node_text(ret, source_bytes)
    type_params = fn_node.child_by_field_name("type_parameters")
    if type_params:
        sig["generics"] = _node_text(type_params, source_bytes)
    # Reconstruct compact signature line: slice BYTES first, then decode
    body = fn_node.child_by_field_name("body")
    if body:
        sig_bytes = source_bytes[fn_node.start_byte:body.start_byte]
        sig["signature"] = sig_bytes.decode("utf-8", errors="replace").strip()
    # Async/unsafe modifiers
    for child in fn_node.children:
        if child.type == "async":
            sig["is_async"] = True
        elif child.type == "unsafe":
            sig["is_unsafe"] = True
    return sig
```

### Payload Bounds

| Field | Max Length | Truncation |
|-------|-----------|------------|
| `signature` | 200 chars | Truncate with `...` suffix |
| `params` | 12 items | Suffix `"... and N more"` |
| `return_type` | 100 chars | Truncate with `...` suffix |
| `generics` | 100 chars | Truncate with `...` suffix |

### Queries Eliminated

- "What is the signature of X?" (was: `/cq q "entity=function name=X lang=rust"`)
- "What are the parameters?" (was: pattern query for param list)
- "Is this async?" (was: pattern query or grep for `async fn`)

---

## Recommendation 2: Visibility and Attribute Tags

### What

For any definition-category match (function, struct, enum, trait, impl), extract a compact tag array summarizing visibility and attached attributes.

### Fields Produced

| Field | Source | Example |
|-------|--------|---------|
| `visibility` | Field lookup + child-kind fallback | `"pub"`, `"pub(crate)"`, `"pub(super)"`, `"private"` |
| `attributes` | Preceding `attribute_item` siblings | `["derive(Debug, Clone)", "cfg(test)", "test"]` |

### Tree-Sitter Node Structure

Attributes in Rust are siblings that precede the item. They have kind `attribute_item` and contain an inner `attribute` node with a `path` field and optional `token_tree` arguments.

```
attribute_item -> "[" attribute "]"
attribute [fields: path, arguments]
```

### Implementation

**Correction from review (C3):** Visibility is not always exposed as a named field in every item kind. The implementation uses field lookup first, then falls back to scanning children for a `visibility_modifier` node. All values are normalized to canonical strings.

```python
def _extract_visibility(item_node: Node, source_bytes: bytes) -> str:
    # Try field lookup first
    vis = item_node.child_by_field_name("visibility_modifier")
    if vis is None:
        # Child-kind fallback: scan children for visibility_modifier node
        for child in item_node.children:
            if child.type == "visibility_modifier":
                vis = child
                break
    if vis is None:
        return "private"
    text = _node_text(vis, source_bytes)
    if text is None:
        return "private"
    # Normalize to canonical values
    if text == "pub":
        return "pub"
    if "crate" in text:
        return "pub(crate)"
    if "super" in text:
        return "pub(super)"
    return text  # e.g., pub(in path)
```

**Correction from review (C4):** The attribute sibling walk must handle intervening comments and doc comments. The policy is:

- Include contiguous leading **outer** attributes (`#[...]`) for the target item.
- Skip `line_comment` and `block_comment` nodes between attributes and the item.
- Exclude inner attributes (`#![...]`).
- Doc comments (`///`, `//!`) are **not** promoted to attribute tags (they are already captured by `context_snippet`).

```python
_MAX_ATTRIBUTES = 10

def _extract_attributes(item_node: Node, source_bytes: bytes) -> list[str]:
    attrs: list[str] = []
    prev = item_node.prev_named_sibling
    # Walk backwards, skipping comments, collecting attribute_items
    while prev is not None:
        if prev.type == "attribute_item":
            text = _node_text(prev, source_bytes)
            if text:
                inner = text.lstrip("#").strip("[]").strip()
                if inner and not inner.startswith("!"):  # Exclude inner attributes
                    attrs.append(inner)
        elif prev.type in ("line_comment", "block_comment"):
            pass  # Skip intervening comments
        else:
            break  # Stop at non-attribute, non-comment siblings
        prev = prev.prev_named_sibling
    attrs.reverse()  # Restore declaration order
    return attrs[:_MAX_ATTRIBUTES]
```

### Payload Bounds

| Field | Max | Truncation |
|-------|-----|------------|
| `attributes` | 10 items | Drop excess silently |
| Each attribute string | 60 chars | Truncate with `...` |

### Queries Eliminated

- "Is this public?" (was: grep for `pub fn` or pattern query)
- "Does this have `#[test]`?" (was: relational query)
- "What derives does this struct have?" (was: pattern query for `#[derive(...)]`)
- "Is this `#[cfg(test)]` gated?" (was: grep or pattern query)

---

## Recommendation 3: Impl Context Resolution

### What

When a match is inside an `impl_item`, extract the impl target type and trait (if trait impl), discriminating inherent methods from trait implementations.

### Fields Produced

| Field | Source | Example |
|-------|--------|---------|
| `impl_type` | `type` field of enclosing `impl_item` | `"GraphBuilder"` |
| `impl_trait` | `trait` field (if present) | `"Display"` or absent |
| `impl_kind` | Derived | `"inherent"` or `"trait"` |
| `impl_generics` | `type_parameters` field | `<T: Clone>` or absent |

### Tree-Sitter Node Structure

```
impl_item [fields: type_parameters, trait, type, body]
```

For `impl Display for GraphBuilder`:
- `trait` field -> `Display`
- `type` field -> `GraphBuilder`

For `impl GraphBuilder`:
- `trait` field -> None
- `type` field -> `GraphBuilder`

### Implementation

```python
def _extract_impl_context(impl_node: Node, source_bytes: bytes) -> dict[str, object]:
    ctx: dict[str, object] = {}
    type_node = impl_node.child_by_field_name("type")
    if type_node:
        ctx["impl_type"] = _node_text(type_node, source_bytes)
    trait_node = impl_node.child_by_field_name("trait")
    if trait_node:
        ctx["impl_trait"] = _node_text(trait_node, source_bytes)
        ctx["impl_kind"] = "trait"
    else:
        ctx["impl_kind"] = "inherent"
    type_params = impl_node.child_by_field_name("type_parameters")
    if type_params:
        ctx["impl_generics"] = _node_text(type_params, source_bytes)
    return ctx
```

### Queries Eliminated

- "What type does this method belong to?" (was: scope query or expanding context)
- "Is this a trait impl or inherent impl?" (was: pattern query for `impl Trait for`)
- "What trait is being implemented?" (was: search for `impl ... for Type`)

---

## Recommendation 4: Item Kind Discrimination

### What

Refine the bare `node_kind` into a high-level semantic classification that answers "what kind of thing is this match inside?" without requiring the consumer to interpret raw tree-sitter node types.

### Fields Produced

| Field | Source | Example Values |
|-------|--------|----------------|
| `item_role` | Derived from node ancestry + attributes | `"free_function"`, `"method"`, `"trait_method"`, `"test_function"`, `"struct_field"`, `"enum_variant"`, `"use_import"`, `"macro_call"`, `"const_item"`, `"type_alias"` |

### Derivation Rules

| Condition | `item_role` |
|-----------|-------------|
| `function_item` inside `impl_item` with trait | `"trait_method"` |
| `function_item` inside `impl_item` without trait | `"method"` |
| `function_item` with `#[test]` / `#[tokio::test]` / `#[rstest]` attribute | `"test_function"` |
| `function_item` at module level | `"free_function"` |
| `call_expression` with `field_expression` function child | `"method_call"` |
| `call_expression` | `"function_call"` |
| `macro_invocation` | `"macro_call"` |
| `use_declaration` | `"use_import"` |
| `field_declaration` | `"struct_field"` |
| `enum_variant` | `"enum_variant"` |
| `const_item` | `"const_item"` |
| `type_item` | `"type_alias"` |
| `static_item` | `"static_item"` |
| Other | Falls back to raw `node.type` |

### Implementation

```python
_ITEM_ROLE_SIMPLE: dict[str, str] = {
    "use_declaration": "use_import",
    "macro_invocation": "macro_call",
    "field_declaration": "struct_field",
    "enum_variant": "enum_variant",
    "const_item": "const_item",
    "type_item": "type_alias",
    "static_item": "static_item",
}

_TEST_ATTRIBUTE_NAMES: frozenset[str] = frozenset({
    "test", "tokio::test", "rstest", "async_std::test",
})

def _classify_item_role(
    node: Node,
    scope: Node | None,
    source_bytes: bytes,
    attributes: list[str],
) -> str:
    kind = node.type
    if kind in _ITEM_ROLE_SIMPLE:
        return _ITEM_ROLE_SIMPLE[kind]
    if kind == "call_expression":
        func = node.child_by_field_name("function")
        if func and func.type == "field_expression":
            return "method_call"
        return "function_call"
    if kind == "function_item":
        if any(a in _TEST_ATTRIBUTE_NAMES for a in attributes):
            return "test_function"
        if scope and scope.type == "impl_item":
            trait_node = scope.child_by_field_name("trait")
            return "trait_method" if trait_node else "method"
        return "free_function"
    return kind  # Fallback to raw node type
```

### Queries Eliminated

- "Is this a method or a free function?" (was: relational query with `inside='impl ...'`)
- "Is this a test?" (was: attribute/pattern query)
- "Is this a method call or function call?" (was: pattern query)

---

## Recommendation 5: Call Target Extraction

### What

For matches that are `call_expression` or `macro_invocation` nodes, extract the call target identity so the agent knows what is being called without a follow-up.

### Fields Produced

| Field | Source | Example |
|-------|--------|---------|
| `call_target` | `function` field of call_expression | `"ctx.build_graph"` |
| `call_receiver` | Receiver of method call | `"ctx"` |
| `call_method` | Method name of method call | `"build_graph"` |
| `macro_name` | `macro` field of macro_invocation | `"println"` |

### Tree-Sitter Node Structure

```
call_expression [fields: function, arguments]
  function: field_expression [fields: value, field]  -- for method calls
  function: identifier                                -- for function calls

macro_invocation [fields: macro, token_tree]
```

### Implementation

```python
def _extract_call_target(node: Node, source_bytes: bytes) -> dict[str, object]:
    target: dict[str, object] = {}
    if node.type == "call_expression":
        func = node.child_by_field_name("function")
        if func:
            target["call_target"] = _node_text(func, source_bytes)
            if func.type == "field_expression":
                receiver = func.child_by_field_name("value")
                method = func.child_by_field_name("field")
                if receiver:
                    target["call_receiver"] = _node_text(receiver, source_bytes)
                if method:
                    target["call_method"] = _node_text(method, source_bytes)
    elif node.type == "macro_invocation":
        macro = node.child_by_field_name("macro")
        if macro:
            target["macro_name"] = _node_text(macro, source_bytes)
    return target
```

### Payload Bounds

| Field | Max Length |
|-------|-----------|
| `call_target` | 120 chars |
| `call_receiver` | 80 chars |

### Queries Eliminated

- "What function is being called here?" (was: expand context or pattern query)
- "What is the receiver of this method call?" (was: pattern query with `$RECV.$METHOD`)

---

## Recommendation 6: Struct/Enum Shape Summary

### What

When the match or nearest scope is a `struct_item` or `enum_item`, extract a compact shape summary showing field count and variant count. This gives the agent an at-a-glance understanding of the data structure without reading the full definition.

### Fields Produced

| Field | Source | Example |
|-------|--------|---------|
| `struct_fields` | Named children of `field_declaration_list` | `["name: String", "age: u32"]` |
| `struct_field_count` | Count | `2` |
| `enum_variants` | Named children of `enum_variant_list` | `["Ok(T)", "Err(E)"]` |
| `enum_variant_count` | Count | `2` |

### Bounded Extraction

To prevent output bloat on large types, explicit caps are enforced with truncation metadata:

| Field | Max Items | Max Item Length | Truncation |
|-------|-----------|-----------------|------------|
| `struct_fields` | 8 | 60 chars | Suffix `"... and N more"` |
| `enum_variants` | 12 | 60 chars | Suffix `"... and N more"` |

When truncation occurs, the `runtime` group includes `"truncated_fields": true`.

### Implementation

```python
_MAX_FIELDS_SHOWN = 8
_MAX_VARIANTS_SHOWN = 12
_MAX_MEMBER_TEXT_LEN = 60

def _extract_struct_shape(struct_node: Node, source_bytes: bytes) -> dict[str, object]:
    shape: dict[str, object] = {}
    body = struct_node.child_by_field_name("body")
    if body and body.type == "field_declaration_list":
        fields = [c for c in body.named_children if c.type == "field_declaration"]
        shape["struct_field_count"] = len(fields)
        shown = []
        for f in fields[:_MAX_FIELDS_SHOWN]:
            text = _node_text(f, source_bytes)
            if text:
                shown.append(text[:_MAX_MEMBER_TEXT_LEN])
        if len(fields) > _MAX_FIELDS_SHOWN:
            shown.append(f"... and {len(fields) - _MAX_FIELDS_SHOWN} more")
        shape["struct_fields"] = shown
    return shape

def _extract_enum_shape(enum_node: Node, source_bytes: bytes) -> dict[str, object]:
    shape: dict[str, object] = {}
    body = enum_node.child_by_field_name("body")
    if body and body.type == "enum_variant_list":
        variants = [c for c in body.named_children if c.type == "enum_variant"]
        shape["enum_variant_count"] = len(variants)
        shown = []
        for v in variants[:_MAX_VARIANTS_SHOWN]:
            text = _node_text(v, source_bytes)
            if text:
                shown.append(text[:_MAX_MEMBER_TEXT_LEN])
        if len(variants) > _MAX_VARIANTS_SHOWN:
            shown.append(f"... and {len(variants) - _MAX_VARIANTS_SHOWN} more")
        shape["enum_variants"] = shown
    return shape
```

### Queries Eliminated

- "What fields does this struct have?" (was: entity query + context expansion)
- "How many variants does this enum have?" (was: reading the full definition)

---

## Prerequisite: Correctness and Stability Fixes

Before expanding enrichment features, three correctness issues in the current runtime must be addressed.

### P1. Byte-Safe Coordinate Handling

**Problem:** The current path feeds ripgrep `submatches.start/end` (byte offsets) into span columns, then uses line/col for tree-sitter `Point` lookups. Tree-sitter point columns are **character-based**, not byte-based. Unicode source can desynchronize offsets.

**Fix:**
1. Derive the enrichment anchor from byte offsets where available.
2. Prefer `named_descendant_for_byte_range` for exact byte-anchored node lookup.
3. Add a per-line byte-to-column mapping helper as a fallback when only line/col is available.

```python
# Preferred: byte-range based lookup (exact)
node = tree.root_node.named_descendant_for_byte_range(byte_start, byte_end)

# Fallback: character-based point lookup (for non-byte sources)
def _byte_col_to_char_col(source_bytes: bytes, line_start_byte: int, byte_col: int) -> int:
    line_prefix = source_bytes[line_start_byte:line_start_byte + byte_col]
    return len(line_prefix.decode("utf-8", errors="replace"))
```

### P2. Cache Staleness and Memory Controls

**Problem:** Current `_TREE_CACHE` is keyed only by `cache_key` and unbounded. Same `cache_key` can serve stale trees after file changes. Cache can grow indefinitely on large repos.

**Fix:**
1. Key entries by `(cache_key, source_hash)` and verify source hash before reuse.
2. Replace `dict` with a bounded LRU cache.
3. Expose counters for cache hits, misses, and evictions in debug diagnostics.

```python
from hashlib import blake2b

_MAX_TREE_CACHE_ENTRIES = 64

def _source_hash(source_bytes: bytes) -> str:
    return blake2b(source_bytes, digest_size=16).hexdigest()

# Cache keyed by (cache_key, hash), bounded to _MAX_TREE_CACHE_ENTRIES
# Evict oldest entry when at capacity
```

### P3. Signature Byte-Safety

Covered in Recommendation 1 (C2): always slice bytes first, then decode. The expression `source_bytes[fn_node.start_byte:body.start_byte].decode(...)` is the correct approach. Never slice a decoded string with byte offsets.

---

## Additional Improvement Opportunities

### O1. Node-Types Schema Lint for Extraction Code

From `tree-sitter-rust.md`: `node-types.json` is the canonical node and field schema for tree-sitter-rust.

**Implementation:** Add a CQ test utility that validates every node kind and field name referenced in enrichment code against the pinned `node-types.json` schema. Fail CI on unknown kinds/fields after grammar upgrades.

**Impact:** Prevents silent breakage when tree-sitter-rust updates its grammar.

### O2. Query-Pack Driven Enrichment Micro-Rules

From `tree-sitter-rust.md`: `.scm` query packs with `#set!` metadata provide stable capture semantics.

**Implementation:** Introduce minimal CQ-owned enrichment packs under `tools/cq/search/queries/rust/`:
- `enrichment_calls.scm` - Call/method/macro capture with metadata routing.
- `enrichment_attrs.scm` - Attribute + visibility capture.
- `enrichment_impl.scm` - Impl context capture.

Use `#set! cq.group` metadata for field-group routing. Enforce rooted/local pattern constraints for range-bounded execution.

**Impact:** Better maintainability than long imperative node-walk code for complex capture logic. Especially valuable as enrichment complexity grows.

### O3. Injection-Aware Macro Token-Tree Analysis

From `tree-sitter-rust.md`: Rust injections include token-tree parsing intent.

**Implementation:** Add opt-in extraction for macro token-tree context:
- Detect injected Rust fragments in macro bodies.
- Expose concise tags like `macro_body_has_match_expression`.
- Keep this bounded and **disabled by default** for heavy paths.

**Impact:** Substantial context gain for macro-heavy Rust code with controlled cost.

### O4. Ast-Grep Context/Selector Templates for Rust Rules

From `ast-grep-py_rust_deepdive.md`: Rust fragment matching should prefer `context + selector`.

**Implementation:** Extend `tools/cq/astgrep/rules_rust.py` with high-value selector-based rules for:
- Methods inside impl (discriminate inherent vs trait).
- Function signatures without bodies (`function_signature_item`).
- Attribute-bearing items.
- Scoped calls and turbofish forms.

Reuse these rules for both classification and enrichment bootstrap (Tier 1).

**Impact:** Higher parity with Python query surfaces using one structural mechanism.

### O5. Cross-Check Mode for Extraction Confidence

**Implementation:** Add optional debug mode that compares ast-grep and tree-sitter extracted fields for the same match. Emit mismatch diagnostics in test/dev mode only.

**Impact:** Faster hardening and safer evolution of enrichment rules.

### O6. Telemetry for Quality and Cost Control

**Implementation:** Record counters in summary/artifacts:
- Enrichment applied/degraded/skipped counts.
- Cache hit/miss/eviction stats.
- Average enrichment latency per match.

**Impact:** Enables data-driven tuning for quality vs latency tradeoffs.

---

## Composite Enrichment Payload

After implementing all six recommendations plus the runtime metadata, the enrichment payload evolves from:

### Before (current)

```json
{
  "node_kind": "identifier",
  "scope_chain": ["function_item:build_graph", "impl_item:GraphBuilder"],
  "scope_kind": "function_item",
  "scope_name": "build_graph"
}
```

### After (proposed) - definition match

```json
{
  "node_kind": "identifier",
  "item_role": "method",
  "language": "rust",
  "scope_chain": ["function_item:build_graph", "impl_item:GraphBuilder"],
  "scope_kind": "function_item",
  "scope_name": "build_graph",
  "visibility": "pub",
  "attributes": ["inline"],
  "signature": "pub fn build_graph(&self, ctx: &Context) -> Result<Graph>",
  "params": ["&self", "ctx: &Context"],
  "return_type": "Result<Graph>",
  "impl_type": "GraphBuilder",
  "impl_kind": "inherent",
  "enrichment_status": "applied",
  "enrichment_sources": ["ast_grep", "tree_sitter"]
}
```

### After (proposed) - callsite match

```json
{
  "node_kind": "call_expression",
  "item_role": "method_call",
  "language": "rust",
  "scope_chain": ["function_item:process"],
  "scope_kind": "function_item",
  "scope_name": "process",
  "call_target": "builder.build_graph",
  "call_receiver": "builder",
  "call_method": "build_graph",
  "enrichment_status": "applied",
  "enrichment_sources": ["tree_sitter"]
}
```

### Volume Analysis

The current payload is ~150-300 bytes JSON. The proposed payload is ~400-800 bytes JSON. This is a 2-3x increase in enrichment data, but it **replaces 2-4 follow-up queries** that would each return 500-2000 bytes of formatted output. Net text volume to the agent decreases.

---

## Implementation Plan

### Phase 1: Correctness and Stability Fixes (P1-P3)

**File**: `tools/cq/search/tree_sitter_rust.py`

1. Implement byte-safe coordinate handling (P1): add `named_descendant_for_byte_range` path and byte-to-char column helper.
2. Implement cache staleness detection (P2): source-hash keying, bounded LRU, eviction counters.
3. These are prerequisites; no new enrichment fields yet.

**Estimated diff**: +60 lines.

### Phase 2: Enrichment Contract and Payload Policy

**File**: `tools/cq/search/tree_sitter_rust.py`

1. Define the unified field groups (A2) as constants.
2. Add runtime metadata fields (`enrichment_status`, `enrichment_sources`, `degrade_reason`).
3. Add bounded payload policy (O6): explicit caps for all list and string fields, truncation tracking.

**Estimated diff**: +40 lines.

### Phase 3: Ast-Grep-First Extraction (O4 + Tier 1)

**Files**: `tools/cq/astgrep/rules_rust.py`, `tools/cq/search/classifier.py`

1. Add selector-based rules for impl-method, trait-method, attribute-bearing items.
2. Wire Tier 1 extraction into the enrichment pipeline: `item_role`, `visibility`, `call_target` fields from ast-grep classification results.

**Estimated diff**: +80 lines.

### Phase 4: Tree-Sitter Augmentation (Recommendations 1-3, 5-6)

**File**: `tools/cq/search/tree_sitter_rust.py`

1. Add signature extraction with byte-safe slicing (Rec 1).
2. Add visibility extraction with field + child-kind fallback (Rec 2).
3. Add attribute extraction with robust sibling/trivia handling (Rec 2).
4. Add impl context resolution (Rec 3).
5. Add call target extraction (Rec 5).
6. Add struct/enum shape extraction (Rec 6).
7. Apply deterministic precedence (A3): tree-sitter fills gaps, never overrides ast-grep.

**Estimated diff**: +180 lines.

### Phase 5: Semantic Classification (Recommendation 4)

**File**: `tools/cq/search/tree_sitter_rust.py`

1. Add `_classify_item_role()` consuming attributes and impl context from Phase 4.
2. Integrate into `enrich_rust_context()`.

**Estimated diff**: +40 lines.

### Phase 6: Consumer Integration

**File**: `tools/cq/search/smart_search.py`

1. The current integration already passes through the full `rust_tree_sitter` dict to `Finding.details.data`. No changes needed to the data pipeline.
2. Use `impl_type` to improve `containing_scope` display (e.g., `"GraphBuilder::build_graph"` instead of just `"build_graph"`). This is a display-only change consistent with the existing `scope_name` fallback.
3. **No ranking, scoring, or classification changes.** Enrichment fields live exclusively in `details.data["rust_tree_sitter"]`.

**Estimated diff**: +15 lines.

### Phase 7: Tests

**File**: `tests/unit/cq/search/test_tree_sitter_rust.py`

1. Expand `_RUST_SAMPLE` fixture with richer Rust code (impl blocks, traits, attributes, macros).
2. Add tests for each new extraction function.
3. Add specific test categories from review:
   - **Unicode coordinate tests**: verify byte offsets map correctly to tree-sitter target nodes.
   - **Cache staleness tests**: same `cache_key`, changed source must produce updated payload.
   - **Deterministic precedence tests**: ast-grep vs tree-sitter conflicting values resolve consistently.
   - **Payload cap tests**: truncation flags set when limits are reached.
   - **Schema-lint tests**: referenced node kinds/fields must exist in pinned grammar schema.
   - **Macro token-tree optional tests**: enabled path enriches; disabled path is unchanged.

**Estimated diff**: +250 lines of tests.

### Total Estimated Diff

~650 lines of new code across existing files plus test expansion. No new top-level modules required (enrichment `.scm` packs from O2 are a later phase).

---

## Delivery Sequence

Per review recommendation, phases are ordered by dependency:

| Order | Phase | Content | Prerequisite |
|-------|-------|---------|--------------|
| 1 | Correctness fixes | P1, P2, P3 | None |
| 2 | Contract + payload policy | A2, A3, O6 | Phase 1 |
| 3 | Ast-grep-first extraction | O4 + Tier 1 fields | Phase 2 |
| 4 | Tree-sitter augmentation | Recs 1-3, 5-6 | Phase 2 |
| 5 | Semantic classification | Rec 4 | Phases 3-4 |
| 6 | Consumer integration | Display improvements | Phase 5 |
| 7 | Tests | Full test suite | All phases |
| 8 | Observability + hardening | O1, O5, O6 telemetry | Phase 7 |
| 9 | Query packs (future) | O2, O3 | Phase 8 |

Phases 3 and 4 can proceed in parallel since they operate on different files.

---

## Fields NOT Recommended

The following were considered and rejected:

| Rejected Field | Reason |
|----------------|--------|
| Function body text | Too verbose; already available via context_snippet |
| Full type annotation trees | Too complex; raw text is sufficient |
| Lifetime annotations | Niche; not useful for general code review |
| Where clause expansion | Rare; generics text captures this compactly |
| Macro expansion output | Requires compilation; out of scope for tree-sitter |
| Cross-file type resolution | Requires full semantic analysis; not feasible in enrichment |
| Documentation comments | Already captured by context_snippet when present |
| Trait bound enumeration | Generics string captures this; separate field is noise |

---

## Acceptance Criteria

1. **No ranking impact.** No change to search ranking, confidence, or match counts from enrichment.
2. **Fail-open preserved.** Rust enrichment remains fail-open under parser/query failures. Any extraction error degrades to `enrichment_status: "degraded"` with a `degrade_reason`, never to an exception.
3. **Bounded and deterministic.** Enrichment payload stays bounded by explicit caps and is deterministic across repeated runs on the same source.
4. **Ast-grep primary.** Ast-grep provides primary semantic fields for shared Rust query semantics.
5. **Tree-sitter additive.** Tree-sitter augmentation adds measurable context without correctness regressions. Precedence rules ensure tree-sitter never overrides ast-grep values.
6. **Byte-safe coordinates.** All node lookups handle Unicode source correctly.
7. **Cache correctness.** Stale cache entries are detected and evicted.

---

## Expected Query Reduction

For a typical Rust code review session involving 10 function matches:

| Scenario | Before (queries) | After (queries) | Saved |
|----------|-------------------|------------------|-------|
| Understand function signatures | 10 | 0 | 10 |
| Check visibility | 5-10 | 0 | 5-10 |
| Determine method vs function | 5-10 | 0 | 5-10 |
| Check for test attributes | 3-5 | 0 | 3-5 |
| Identify impl context | 5-10 | 0 | 5-10 |
| Understand call targets | 5 | 0 | 5 |
| **Total** | **33-55** | **0** | **33-55** |

Conservative estimate: **3-5x reduction in follow-up queries** for Rust code review tasks, with a **~2x increase in per-match enrichment compute** (all from cached node walking, no new parsing).
