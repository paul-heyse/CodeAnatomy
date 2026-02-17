"""Canonical entity-kind registry for query execution."""

from __future__ import annotations

import msgspec

_ENTITY_RECORD_TYPES: dict[str, frozenset[str]] = {
    "function": frozenset({"def"}),
    "class": frozenset({"def"}),
    "method": frozenset({"def"}),
    "module": frozenset({"def"}),
    "decorator": frozenset({"def"}),
    "callsite": frozenset({"call"}),
    "import": frozenset({"import"}),
}

_ENTITY_EXTRA_RECORD_TYPES: dict[str, frozenset[str]] = {
    "function": frozenset({"call"}),
    "class": frozenset({"call"}),
    "method": frozenset({"call"}),
}


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
    entity_record_types: dict[str, frozenset[str]] = msgspec.field(
        default_factory=lambda: dict(_ENTITY_RECORD_TYPES)
    )
    entity_extra_record_types: dict[str, frozenset[str]] = msgspec.field(
        default_factory=lambda: dict(_ENTITY_EXTRA_RECORD_TYPES)
    )

    def record_types_for_entity(self, entity_type: str | None) -> frozenset[str]:
        """Return base record types needed for one entity type."""
        if entity_type is None:
            return frozenset()
        return self.entity_record_types.get(entity_type, frozenset())

    def extra_record_types_for_entity(self, entity_type: str | None) -> frozenset[str]:
        """Return extra record types needed for one entity type."""
        if entity_type is None:
            return frozenset()
        return self.entity_extra_record_types.get(entity_type, frozenset())

    @property
    def decorator_kinds(self) -> frozenset[str]:
        """Kinds that can carry decorators."""
        return self.function_kinds | self.class_kinds

    def _kind_matcher_map(self) -> dict[str, frozenset[str] | str | None]:
        return {
            "function": self.function_kinds,
            "method": self.function_kinds,
            "class": self.class_kinds,
            "import": self.import_kinds,
            "decorator": self.decorator_kinds,
            "module": "module",
            # Callsite selectors are record_type-gated and accept all call kinds.
            "callsite": None,
        }

    def matches(self, *, entity_type: str | None, record_kind: str, record_type: str) -> bool:
        """Check whether one scan record matches an entity selector.

        Returns:
        -------
        bool
            True when the entity selector accepts the record kind and type.
        """
        if entity_type is None:
            return False
        if record_type not in self.record_types_for_entity(entity_type):
            return False
        matcher = self._kind_matcher_map().get(entity_type)
        if matcher is None:
            return entity_type == "callsite"
        if isinstance(matcher, frozenset):
            return record_kind in matcher
        if isinstance(matcher, str):
            return record_kind == matcher
        return False


ENTITY_KINDS = EntityKindRegistry()


__all__ = ["ENTITY_KINDS", "EntityKindRegistry"]
