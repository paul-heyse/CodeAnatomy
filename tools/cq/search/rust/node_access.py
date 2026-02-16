"""Node-access adapters for shared Rust extractors."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from ast_grep_py import SgNode
    from tree_sitter import Node


class RustNodeAccess(Protocol):
    """Protocol for Rust node adapters consumed by shared extractors."""

    def kind(self) -> str:
        """Return the node kind/type label."""
        ...

    def text(self) -> str:
        """Return decoded source text for this node."""
        ...

    def child_by_field_name(self, name: str) -> RustNodeAccess | None:
        """Return field child by grammar name, when present."""
        ...

    def children(self) -> list[RustNodeAccess]:
        """Return named child nodes."""
        ...

    def parent(self) -> RustNodeAccess | None:
        """Return parent node if available."""
        ...


@dataclass(frozen=True)
class SgRustNodeAccess:
    """ast-grep adapter implementing :class:`RustNodeAccess`."""

    node: SgNode

    def kind(self) -> str:
        """Return ast-grep node kind."""
        return self.node.kind()

    def text(self) -> str:
        """Return ast-grep node text."""
        return self.node.text()

    def child_by_field_name(self, name: str) -> RustNodeAccess | None:
        """Return wrapped ast-grep child node for ``name``."""
        child = self.node.field(name)
        if child is None:
            return None
        return SgRustNodeAccess(child)

    def children(self) -> list[RustNodeAccess]:
        """Return wrapped named ast-grep children."""
        return [SgRustNodeAccess(child) for child in self.node.children() if child.is_named()]

    def parent(self) -> RustNodeAccess | None:
        """Return wrapped ast-grep parent node."""
        parent = self.node.parent()
        if parent is None:
            return None
        return SgRustNodeAccess(parent)


@dataclass(frozen=True)
class TreeSitterRustNodeAccess:
    """tree-sitter adapter implementing :class:`RustNodeAccess`."""

    node: Node
    source_bytes: bytes

    def kind(self) -> str:
        """Return tree-sitter node type."""
        return self.node.type

    def text(self) -> str:
        """Return decoded source text for the tree-sitter node span."""
        return self.source_bytes[self.node.start_byte : self.node.end_byte].decode(
            "utf-8", errors="replace"
        )

    def child_by_field_name(self, name: str) -> RustNodeAccess | None:
        """Return wrapped tree-sitter child node for ``name``."""
        child = self.node.child_by_field_name(name)
        if child is None:
            return None
        return TreeSitterRustNodeAccess(child, self.source_bytes)

    def children(self) -> list[RustNodeAccess]:
        """Return wrapped named tree-sitter children."""
        return [
            TreeSitterRustNodeAccess(child, self.source_bytes) for child in self.node.named_children
        ]

    def parent(self) -> RustNodeAccess | None:
        """Return wrapped tree-sitter parent node."""
        parent = self.node.parent
        if parent is None:
            return None
        return TreeSitterRustNodeAccess(parent, self.source_bytes)


__all__ = ["RustNodeAccess", "SgRustNodeAccess", "TreeSitterRustNodeAccess"]
