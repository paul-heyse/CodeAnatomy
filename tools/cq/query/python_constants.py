"""Python-specific query constants."""

from __future__ import annotations

# Common Python AST field names for field-scoped searches.
PYTHON_AST_FIELDS: frozenset[str] = frozenset(
    {
        # Function
        "name",
        "args",
        "body",
        "decorator_list",
        "returns",
        "type_params",
        # Arguments
        "posonlyargs",
        "vararg",
        "kwonlyargs",
        "kw_defaults",
        "kwarg",
        "defaults",
        # Class
        "bases",
        "keywords",
        # Dict
        "keys",
        "values",
        # Call
        "func",
        # Subscript
        "value",
        "slice",
        # Attribute
        "attr",
        # Assign
        "targets",
        # If/While/For
        "test",
        "orelse",
        # Try
        "handlers",
        "finalbody",
        # With
        "items",
        # Match
        "subject",
        "cases",
    }
)


__all__ = ["PYTHON_AST_FIELDS"]
