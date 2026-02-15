"""Structural diff helpers for grammar schema drift reporting."""

from __future__ import annotations

from tools.cq.core.structs import CqStruct


class GrammarDiffV1(CqStruct, frozen=True):
    """Structured grammar-diff payload used by drift reports."""

    added_node_kinds: tuple[str, ...] = ()
    removed_node_kinds: tuple[str, ...] = ()
    added_fields: tuple[str, ...] = ()
    removed_fields: tuple[str, ...] = ()


class _SchemaIndexLike(CqStruct, frozen=True):
    all_node_kinds: tuple[str, ...] = ()
    field_names: tuple[str, ...] = ()


def _as_index(value: object) -> _SchemaIndexLike:
    return _SchemaIndexLike(
        all_node_kinds=tuple(
            sorted(
                str(item) for item in getattr(value, "all_node_kinds", ()) if isinstance(item, str)
            )
        ),
        field_names=tuple(
            sorted(str(item) for item in getattr(value, "field_names", ()) if isinstance(item, str))
        ),
    )


def diff_schema(old_index: object, new_index: object) -> GrammarDiffV1:
    """Build structural diff between two grammar-schema indexes."""
    old = _as_index(old_index)
    new = _as_index(new_index)
    old_nodes = set(old.all_node_kinds)
    new_nodes = set(new.all_node_kinds)
    old_fields = set(old.field_names)
    new_fields = set(new.field_names)
    return GrammarDiffV1(
        added_node_kinds=tuple(sorted(new_nodes - old_nodes)),
        removed_node_kinds=tuple(sorted(old_nodes - new_nodes)),
        added_fields=tuple(sorted(new_fields - old_fields)),
        removed_fields=tuple(sorted(old_fields - new_fields)),
    )


def has_breaking_changes(diff: GrammarDiffV1) -> bool:
    """Return whether a schema diff removes node kinds or fields."""
    return bool(diff.removed_node_kinds or diff.removed_fields)


__all__ = ["GrammarDiffV1", "diff_schema", "has_breaking_changes"]
