"""ast-grep config-rule helpers for Python lane lookups."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ast_grep_py import SgNode


def find_import_aliases(node: SgNode) -> list[SgNode]:
    """Find ``import X as Y`` alias bindings with identifier constraints."""
    return node.find_all(
        {
            "rule": {"pattern": "import $X as $Y"},
            "constraints": {"Y": {"regex": "^[A-Za-z_][A-Za-z0-9_]*$"}},
        }
    )


__all__ = ["find_import_aliases"]
