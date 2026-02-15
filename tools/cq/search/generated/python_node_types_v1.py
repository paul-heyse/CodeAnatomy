"""Generated Python node-type snapshot used when runtime assets are unavailable."""

from __future__ import annotations

NODE_TYPES: tuple[tuple[str, bool, tuple[str, ...]], ...] = (
    ("module", True, ()),
    ("function_definition", True, ("name", "parameters", "body", "return_type")),
    ("class_definition", True, ("name", "superclasses", "body")),
    ("import_statement", True, ("name",)),
    ("import_from_statement", True, ("module_name", "name")),
    ("call", True, ("function", "arguments")),
    ("identifier", True, ()),
    ("attribute", True, ("object", "attribute")),
)

__all__ = ["NODE_TYPES"]
