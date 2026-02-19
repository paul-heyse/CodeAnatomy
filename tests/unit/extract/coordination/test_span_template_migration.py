"""Span-template migration tests for extract coordination and stubs."""

from __future__ import annotations

import ast
from dataclasses import dataclass

from extract.extractors.ast.builders import _span_spec_from_node
from extract.row_builder import SpanTemplateSpec, make_span_spec_dict
from test_support.datafusion_ext_stub import _tree_sitter_node_row

_STUB_BYTE_START = 10
_STUB_BYTE_LEN = 5


@dataclass
class _StubNode:
    start_byte: int = _STUB_BYTE_START
    end_byte: int = _STUB_BYTE_START + _STUB_BYTE_LEN
    type: str = "name"
    is_named: bool = True
    has_error: bool = False
    is_error: bool = False
    is_missing: bool = False
    is_extra: bool = False


def test_ast_span_spec_uses_span_template_spec() -> None:
    """AST span conversion should produce canonical SpanTemplateSpec payloads."""
    module = ast.parse("x = 1")
    node = module.body[0]
    spec = _span_spec_from_node(node, line_offsets=None)

    assert isinstance(spec, SpanTemplateSpec)
    span = make_span_spec_dict(spec)
    assert span is not None
    assert span["start_line0"] == 0


def test_tree_sitter_stub_row_emits_span_via_make_span_spec_dict() -> None:
    """Tree-sitter stub rows should emit spans through the shared span helper."""
    row = _tree_sitter_node_row(node=_StubNode(), node_uid=1, file_id="f1")
    span = row["span"]

    assert isinstance(span, dict)
    assert span.get("byte_start") == _STUB_BYTE_START
    assert span.get("byte_len") == _STUB_BYTE_LEN
