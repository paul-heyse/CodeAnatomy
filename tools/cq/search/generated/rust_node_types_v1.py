"""Generated Rust node-type snapshot used when runtime assets are unavailable."""

from __future__ import annotations

NODE_TYPES: tuple[tuple[str, bool, tuple[str, ...]], ...] = (
    ("source_file", True, ()),
    ("function_item", True, ("name", "parameters", "return_type", "body")),
    ("struct_item", True, ("name", "type_parameters", "body")),
    ("enum_item", True, ("name", "type_parameters", "body")),
    ("trait_item", True, ("name", "type_parameters", "body")),
    ("impl_item", True, ("trait", "type", "body")),
    ("mod_item", True, ("name", "body")),
    ("use_declaration", True, ("argument",)),
    ("call_expression", True, ("function", "arguments")),
    ("macro_invocation", True, ("macro", "token_tree")),
    ("identifier", True, ()),
    ("scoped_identifier", True, ("path", "name")),
)

__all__ = ["NODE_TYPES"]
