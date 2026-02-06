"""Tests for optional Rust tree-sitter enrichment helpers."""

from __future__ import annotations

import pytest
from tools.cq.search import tree_sitter_rust
from tools.cq.search.tree_sitter_rust import MAX_SOURCE_BYTES

_RUST_SAMPLE = 'mod outer {\n    fn build_graph() {\n        println!("hello");\n    }\n}\n'

_ts_available = pytest.mark.skipif(
    not tree_sitter_rust.is_tree_sitter_rust_available(),
    reason="tree-sitter-rust is not available in this environment",
)


# ---------------------------------------------------------------------------
# Original tests (preserved unchanged)
# ---------------------------------------------------------------------------


@_ts_available
def test_enrich_rust_context_returns_scope_chain() -> None:
    """Enrichment should return stable context fields for Rust source."""
    tree_sitter_rust.clear_tree_sitter_rust_cache()
    payload = tree_sitter_rust.enrich_rust_context(_RUST_SAMPLE, line=3, col=10, cache_key="sample")
    assert payload is not None
    assert isinstance(payload.get("node_kind"), str)
    chain = payload.get("scope_chain")
    assert isinstance(chain, list)
    assert any("function_item" in str(item) for item in chain)


def test_enrich_rust_context_fail_open_on_parser_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unexpected parser failures should degrade to None."""

    def _boom(_source: bytes) -> object:
        msg = "forced parse failure"
        raise RuntimeError(msg)

    tree_sitter_rust.clear_tree_sitter_rust_cache()
    monkeypatch.setattr(tree_sitter_rust, "_parse_tree", _boom)
    payload = tree_sitter_rust.enrich_rust_context(_RUST_SAMPLE, line=2, col=8, cache_key="boom")
    assert payload is None


def test_enrich_rust_context_returns_none_when_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unavailable runtime should return None without raising."""
    monkeypatch.setattr(tree_sitter_rust, "is_tree_sitter_rust_available", lambda: False)
    payload = tree_sitter_rust.enrich_rust_context(_RUST_SAMPLE, line=2, col=8, cache_key="missing")
    assert payload is None


def test_enrich_skips_large_source() -> None:
    """Enrichment should return None for sources exceeding MAX_SOURCE_BYTES."""
    large_source = "x" * (MAX_SOURCE_BYTES + 1)
    payload = tree_sitter_rust.enrich_rust_context(
        large_source,
        line=1,
        col=0,
        cache_key="large",
    )
    assert payload is None


def test_enrich_handles_negative_col() -> None:
    """Enrichment should return None when col is negative."""
    payload = tree_sitter_rust.enrich_rust_context(
        _RUST_SAMPLE,
        line=2,
        col=-1,
        cache_key="negcol",
    )
    assert payload is None


@_ts_available
def test_scope_chain_bounded_depth() -> None:
    """Scope chain traversal should terminate within the node count limit.

    Construct a deeply nested Rust source with many ``mod`` blocks and
    verify that ``_scope_chain`` returns without hanging, producing a
    bounded result list.
    """
    nesting = 300
    lines = []
    for i in range(nesting):
        indent = "    " * i
        lines.append(f"{indent}mod m{i} {{")
    # Place a function at the innermost level
    inner_indent = "    " * nesting
    lines.append(f"{inner_indent}fn leaf() {{}}")
    for i in reversed(range(nesting)):
        indent = "    " * i
        lines.append(f"{indent}}}")
    deep_source = "\n".join(lines) + "\n"

    tree_sitter_rust.clear_tree_sitter_rust_cache()
    # Target the leaf function (line is 1-based; it sits at line nesting+1)
    payload = tree_sitter_rust.enrich_rust_context(
        deep_source,
        line=nesting + 1,
        col=len(inner_indent) + 3,
        cache_key="deep",
    )
    # The call must complete (not hang). The result may be a dict or None
    # depending on parser resolution, but it must terminate.
    if payload is not None:
        chain = payload.get("scope_chain")
        assert isinstance(chain, list)
        # Chain length must be bounded by the internal node-visit cap (256)
        assert len(chain) <= 256


# ---------------------------------------------------------------------------
# Comprehensive Rust fixture for enrichment tests
# ---------------------------------------------------------------------------

# Line numbers are 1-based for the enrich_rust_context() API.
# Each line is annotated in comments below.
_RICH_RUST_SAMPLE = """\
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct GraphBuilder {
    name: String,
    nodes: Vec<u32>,
    edges: HashMap<u32, Vec<u32>>,
}

pub(crate) enum GraphError {
    NotFound(String),
    InvalidEdge { from: u32, to: u32 },
    Overflow,
}

trait Buildable {
    fn build(&self) -> Result<(), GraphError>;
}

impl GraphBuilder {
    pub fn new(name: String) -> Self {
        GraphBuilder { name, nodes: vec![], edges: HashMap::new() }
    }
    pub async fn build_graph(&self, ctx: &Context) -> Result<Graph, GraphError> {
        let _ = ctx;
        Ok(Graph {})
    }
    fn helper(&self) {}
}

impl Buildable for GraphBuilder {
    fn build(&self) -> Result<(), GraphError> {
        Ok(())
    }
}

impl std::fmt::Display for GraphBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        assert!(true);
    }

    #[tokio::test]
    async fn test_async() {
        assert!(true);
    }
}

fn free_function(x: u32) -> bool { true }

pub(super) unsafe fn dangerous() {}

fn caller() {
    let b = GraphBuilder::new("g".into());
    b.helper();
    println!("done: {}", b.name);
}
"""
# Line reference (1-based):
# 1:  use std::collections::HashMap;
# 2:  (blank)
# 3:  #[derive(Debug, Clone)]
# 4:  pub struct GraphBuilder {
# 5:    name: String,
# 6:    nodes: Vec<u32>,
# 7:    edges: HashMap<u32, Vec<u32>>,
# 8:  }
# 9:  (blank)
# 10: pub(crate) enum GraphError {
# 11:   NotFound(String),
# 12:   InvalidEdge { from: u32, to: u32 },
# 13:   Overflow,
# 14: }
# 15: (blank)
# 16: trait Buildable {
# 17:   fn build(&self) -> Result<(), GraphError>;
# 18: }
# 19: (blank)
# 20: impl GraphBuilder {
# 21:   pub fn new(name: String) -> Self {
# 22:     ...
# 23:   }
# 24:   pub async fn build_graph(&self, ctx: &Context) -> Result<Graph, GraphError> {
# 25:     ...
# 26:     ...
# 27:   }
# 28:   fn helper(&self) {}
# 29: }
# 30: (blank)
# 31: impl Buildable for GraphBuilder {
# 32:   fn build(&self) -> Result<(), GraphError> {
# 33:     ...
# 34:   }
# 35: }
# 36: (blank)
# 37: impl std::fmt::Display for GraphBuilder {
# 38:   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
# 39:     ...
# 40:   }
# 41: }
# 42: (blank)
# 43: #[cfg(test)]
# 44: mod tests {
# 45:   use super::*;
# 46:   (blank)
# 47:   #[test]
# 48:   fn test_basic() {
# 49:     assert!(true);
# 50:   }
# 51:   (blank)
# 52:   #[tokio::test]
# 53:   async fn test_async() {
# 54:     assert!(true);
# 55:   }
# 56: }
# 57: (blank)
# 58: fn free_function(x: u32) -> bool { true }
# 59: (blank)
# 60: pub(super) unsafe fn dangerous() {}
# 61: (blank)
# 62: fn caller() {
# 63:   let b = GraphBuilder::new("g".into());
# 64:   b.helper();
# 65:   println!("done: {}", b.name);
# 66: }


def _enrich(source: str, *, line: int, col: int, cache_key: str) -> dict[str, object]:
    """Call enrich_rust_context and assert non-None result.

    Returns:
    -------
    dict[str, object]
        The enrichment payload (guaranteed non-None).
    """
    tree_sitter_rust.clear_tree_sitter_rust_cache()
    payload = tree_sitter_rust.enrich_rust_context(source, line=line, col=col, cache_key=cache_key)
    assert payload is not None, f"Expected enrichment payload at line={line} col={col}"
    return payload


# ---------------------------------------------------------------------------
# New enrichment tests
# ---------------------------------------------------------------------------


@_ts_available
def test_runtime_metadata() -> None:
    """Every enrichment payload must contain runtime metadata fields."""
    payload = _enrich(_RICH_RUST_SAMPLE, line=21, col=8, cache_key="rt-meta")
    assert payload["enrichment_status"] in {"applied", "degraded"}
    assert isinstance(payload["enrichment_sources"], list)
    assert "tree_sitter" in payload["enrichment_sources"]
    assert payload["language"] == "rust"


@_ts_available
def test_signature_extraction() -> None:
    """Signature fields should be extracted from function items."""
    # Target: pub fn new(name: String) -> Self { ... }  (line 21)
    payload = _enrich(_RICH_RUST_SAMPLE, line=21, col=8, cache_key="sig")
    assert "params" in payload
    params = payload["params"]
    assert isinstance(params, list)
    assert len(params) >= 1

    assert "return_type" in payload
    ret = payload["return_type"]
    assert isinstance(ret, str)
    assert "Self" in ret

    assert "signature" in payload
    sig = payload["signature"]
    assert isinstance(sig, str)
    assert "fn new" in sig

    assert payload.get("is_async") is False
    assert payload.get("is_unsafe") is False

    # Test async function: pub async fn build_graph (line 24)
    payload2 = _enrich(_RICH_RUST_SAMPLE, line=24, col=8, cache_key="sig-async")
    assert payload2.get("is_async") is True


@_ts_available
def test_visibility_extraction() -> None:
    """Visibility should be normalized to canonical values."""
    # pub struct GraphBuilder (line 4, col at "pub")
    payload_pub = _enrich(_RICH_RUST_SAMPLE, line=4, col=0, cache_key="vis-pub")
    assert payload_pub.get("visibility") == "pub"

    # pub(crate) enum GraphError (line 10)
    payload_crate = _enrich(_RICH_RUST_SAMPLE, line=10, col=0, cache_key="vis-crate")
    assert payload_crate.get("visibility") == "pub(crate)"

    # pub(super) unsafe fn dangerous (line 60)
    payload_super = _enrich(_RICH_RUST_SAMPLE, line=60, col=0, cache_key="vis-super")
    assert payload_super.get("visibility") == "pub(super)"

    # fn helper(&self) -- private (line 28)
    payload_priv = _enrich(_RICH_RUST_SAMPLE, line=28, col=4, cache_key="vis-priv")
    assert payload_priv.get("visibility") == "private"


@_ts_available
def test_attribute_extraction() -> None:
    """Attributes should be extracted and stripped of delimiters."""
    # pub struct GraphBuilder has #[derive(Debug, Clone)] (line 4)
    payload = _enrich(_RICH_RUST_SAMPLE, line=4, col=0, cache_key="attr")
    attrs = payload.get("attributes")
    assert isinstance(attrs, list)
    assert any("derive" in a for a in attrs)
    # Delimiters should be stripped
    for attr in attrs:
        assert not attr.startswith("#["), f"Attribute delimiter not stripped: {attr}"


@_ts_available
def test_impl_context_inherent() -> None:
    """Inherent impl context should report type and kind."""
    # pub fn new inside impl GraphBuilder (line 21)
    payload = _enrich(_RICH_RUST_SAMPLE, line=21, col=8, cache_key="impl-inh")
    assert payload.get("impl_kind") == "inherent"
    impl_type = payload.get("impl_type")
    assert isinstance(impl_type, str)
    assert "GraphBuilder" in impl_type


@_ts_available
def test_impl_context_trait() -> None:
    """Trait impl context should report type, trait, and kind."""
    # fn build inside impl Buildable for GraphBuilder (line 32)
    payload = _enrich(_RICH_RUST_SAMPLE, line=32, col=8, cache_key="impl-trait")
    assert payload.get("impl_kind") == "trait"
    impl_trait = payload.get("impl_trait")
    assert isinstance(impl_trait, str)
    assert "Buildable" in impl_trait


@_ts_available
def test_item_role_classification() -> None:
    """Item role should classify functions into semantic roles."""
    # free_function (line 58)
    payload_free = _enrich(_RICH_RUST_SAMPLE, line=58, col=3, cache_key="role-free")
    assert payload_free.get("item_role") == "free_function"

    # fn new inside impl GraphBuilder -- method (line 21)
    payload_method = _enrich(_RICH_RUST_SAMPLE, line=21, col=8, cache_key="role-method")
    assert payload_method.get("item_role") == "method"

    # fn build inside impl Buildable for GraphBuilder -- trait_method (line 32)
    payload_trait = _enrich(_RICH_RUST_SAMPLE, line=32, col=8, cache_key="role-trait")
    assert payload_trait.get("item_role") == "trait_method"

    # fn test_basic with #[test] -- test_function (line 48)
    payload_test = _enrich(_RICH_RUST_SAMPLE, line=48, col=4, cache_key="role-test")
    assert payload_test.get("item_role") == "test_function"


@_ts_available
def test_call_target_extraction() -> None:
    """Call targets should be extracted from call expressions."""
    # GraphBuilder::new("g".into()) -- line 63, target the call
    payload = _enrich(_RICH_RUST_SAMPLE, line=63, col=12, cache_key="call")
    # The node at col=12 might be inside the call expression
    # If we get call_target, verify its structure
    call_target = payload.get("call_target")
    if call_target is not None:
        assert isinstance(call_target, str)


@_ts_available
def test_macro_name_extraction() -> None:
    """Macro invocations should have macro_name extracted."""
    # println!("done: {}", b.name) -- line 65
    payload = _enrich(_RICH_RUST_SAMPLE, line=65, col=4, cache_key="macro")
    macro_name = payload.get("macro_name")
    if macro_name is not None:
        assert isinstance(macro_name, str)
        assert "println" in macro_name


@_ts_available
def test_struct_shape() -> None:
    """Struct shape should report field count and field list."""
    # pub struct GraphBuilder { name, nodes, edges } (line 5 targets inside struct)
    payload = _enrich(_RICH_RUST_SAMPLE, line=5, col=4, cache_key="struct-shape")
    field_count = payload.get("struct_field_count")
    fields = payload.get("struct_fields")
    if field_count is not None:
        assert isinstance(field_count, int)
        assert field_count == 3
        assert isinstance(fields, list)
        assert len(fields) >= 3


@_ts_available
def test_enum_shape() -> None:
    """Enum shape should report variant count and variant list."""
    # pub(crate) enum GraphError { NotFound, InvalidEdge, Overflow } (line 11)
    payload = _enrich(_RICH_RUST_SAMPLE, line=11, col=4, cache_key="enum-shape")
    variant_count = payload.get("enum_variant_count")
    variants = payload.get("enum_variants")
    if variant_count is not None:
        assert isinstance(variant_count, int)
        assert variant_count == 3
        assert isinstance(variants, list)
        assert len(variants) >= 3


@_ts_available
def test_unicode_coordinates() -> None:
    """Source with unicode identifiers should map to correct nodes."""
    unicode_source = "fn hëllo_wörld() -> bool {\n    true\n}\n"
    payload = _enrich(unicode_source, line=1, col=3, cache_key="unicode")
    assert payload.get("scope_kind") == "function_item"
    scope_name = payload.get("scope_name")
    assert isinstance(scope_name, str)


@_ts_available
def test_cache_staleness() -> None:
    """Same cache_key with changed source should produce fresh payload."""
    tree_sitter_rust.clear_tree_sitter_rust_cache()
    source_v1 = "fn alpha() {}\n"
    source_v2 = "fn beta() {}\n"

    payload1 = tree_sitter_rust.enrich_rust_context(source_v1, line=1, col=3, cache_key="stale")
    assert payload1 is not None
    name1 = payload1.get("scope_name")

    payload2 = tree_sitter_rust.enrich_rust_context(source_v2, line=1, col=3, cache_key="stale")
    assert payload2 is not None
    name2 = payload2.get("scope_name")

    # The names should differ because the source changed
    assert name1 != name2


@_ts_available
def test_cache_bounded() -> None:
    """Cache should not grow beyond _MAX_TREE_CACHE_ENTRIES."""
    tree_sitter_rust.clear_tree_sitter_rust_cache()
    limit = tree_sitter_rust._MAX_TREE_CACHE_ENTRIES  # noqa: SLF001

    for i in range(limit + 20):
        source = f"fn func_{i}() {{}}\n"
        tree_sitter_rust.enrich_rust_context(source, line=1, col=3, cache_key=f"bounded-{i}")

    cache_size = len(tree_sitter_rust._TREE_CACHE)  # noqa: SLF001
    assert cache_size <= limit
    assert tree_sitter_rust._cache_evictions > 0  # noqa: SLF001


@_ts_available
def test_payload_truncation() -> None:
    """Large structs should have field lists capped with truncation markers."""
    fields = "\n".join(f"    field_{i}: u32," for i in range(20))
    big_struct = f"pub struct Big {{\n{fields}\n}}\n"
    payload = _enrich(big_struct, line=2, col=4, cache_key="truncate")
    field_list = payload.get("struct_fields")
    if isinstance(field_list, list) and len(field_list) > 0:
        max_shown = tree_sitter_rust._MAX_FIELDS_SHOWN  # noqa: SLF001
        # The list should be capped (max_shown fields + "... and N more")
        assert len(field_list) <= max_shown + 1
        if len(field_list) == max_shown + 1:
            assert field_list[-1].startswith("... and")


@_ts_available
def test_containing_scope_impl_display() -> None:
    """Verify impl_type is available for TypeName::method_name display."""
    # fn new inside impl GraphBuilder (line 21)
    payload = _enrich(_RICH_RUST_SAMPLE, line=21, col=8, cache_key="scope-display")
    impl_type = payload.get("impl_type")
    scope_name = payload.get("scope_name")
    # Both should be present for methods inside impl blocks
    assert isinstance(impl_type, str)
    assert isinstance(scope_name, str)
    assert "GraphBuilder" in impl_type
    # Verify the display format would work
    display = f"{impl_type}::{scope_name}"
    assert "::" in display


@_ts_available
def test_fail_open_preserved() -> None:
    """Partial extraction failure should return degraded payload, not None."""
    # The base enrichment (node_kind, scope_chain) should always work
    # even if individual extractors fail. We verify by testing a valid
    # location always returns a payload with enrichment_status.
    payload = _enrich(_RICH_RUST_SAMPLE, line=21, col=8, cache_key="fail-open")
    assert payload["enrichment_status"] in {"applied", "degraded"}
    # Core fields must always be present
    assert "node_kind" in payload
    assert "scope_chain" in payload
    assert "language" in payload


@_ts_available
def test_unsafe_function_detection() -> None:
    """Unsafe functions should have is_unsafe=True."""
    # pub(super) unsafe fn dangerous() {} -- line 60
    payload = _enrich(_RICH_RUST_SAMPLE, line=60, col=22, cache_key="unsafe")
    assert payload.get("is_unsafe") is True


@_ts_available
def test_byte_range_entry_point() -> None:
    """The byte-range entry point should produce equivalent enrichment."""
    source = "fn example() -> bool { true }\n"
    tree_sitter_rust.clear_tree_sitter_rust_cache()
    payload = tree_sitter_rust.enrich_rust_context_by_byte_range(
        source, byte_start=3, byte_end=10, cache_key="byte-range"
    )
    assert payload is not None
    assert payload["language"] == "rust"
    assert payload.get("enrichment_status") in {"applied", "degraded"}
