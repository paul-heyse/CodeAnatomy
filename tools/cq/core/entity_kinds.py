"""Canonical entity-kind registry for query execution."""

from __future__ import annotations

import msgspec


class EntityKindRegistry(msgspec.Struct, frozen=True):
    """Immutable registry of normalized entity kind sets."""

    function_kinds: frozenset[str] = frozenset(
        {
            "function",
            "async_function",
            "function_typeparams",
        }
    )
    class_kinds: frozenset[str] = frozenset(
        {
            "class",
            "class_bases",
            "class_typeparams",
            "class_typeparams_bases",
            "struct",
            "enum",
            "trait",
        }
    )
    import_kinds: frozenset[str] = frozenset(
        {
            "import",
            "import_as",
            "from_import",
            "from_import_as",
            "from_import_multi",
            "from_import_paren",
            "use_declaration",
        }
    )

    @property
    def decorator_kinds(self) -> frozenset[str]:
        """Kinds that can carry decorators."""
        return self.function_kinds | self.class_kinds


ENTITY_KINDS = EntityKindRegistry()


__all__ = ["ENTITY_KINDS", "EntityKindRegistry"]
