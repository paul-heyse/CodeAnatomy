"""Relationship dataset contract payloads.

This module intentionally keeps only serialization contracts required by
schema/msgspec registries while runtime relationship planning has moved
behind Rust-owned boundaries.
"""

from __future__ import annotations

from serde_msgspec import StructBaseStrict

RELATIONSHIP_SCHEMA_VERSION: int = 1


class RelationshipData(StructBaseStrict, frozen=True):
    """Serializable relationship contract descriptor.

    Attributes:
    ----------
    name
        Logical relationship name.
    table_name
        Physical table name for relationship rows.
    entity_id_col
        Primary relationship entity identifier column.
    dedupe_keys
        Optional dedupe key override.
    extra_sort_keys
        Optional additional sort keys.
    """

    name: str
    table_name: str
    entity_id_col: str
    dedupe_keys: tuple[str, ...] | None = None
    extra_sort_keys: tuple[str, ...] = ()


RELATIONSHIP_DATA: tuple[RelationshipData, ...] = (
    RelationshipData(
        name="rel_name_symbol",
        table_name="rel_name_symbol",
        entity_id_col="entity_id",
    ),
    RelationshipData(
        name="rel_import_symbol",
        table_name="rel_import_symbol",
        entity_id_col="entity_id",
    ),
    RelationshipData(
        name="rel_def_symbol",
        table_name="rel_def_symbol",
        entity_id_col="entity_id",
    ),
    RelationshipData(
        name="rel_callsite_symbol",
        table_name="rel_callsite_symbol",
        entity_id_col="entity_id",
    ),
)


__all__ = ["RELATIONSHIP_DATA", "RELATIONSHIP_SCHEMA_VERSION", "RelationshipData"]
