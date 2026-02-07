"""Entity-centric data model for semantic declarations.

Elevate the semantic model from schema-centric table specs to entity
declarations that capture identity, location, and content concepts.
Each ``EntityDeclaration`` generates a ``SemanticTableSpec`` losslessly.
"""

from __future__ import annotations

from semantics.specs import SpanUnit
from serde_msgspec import StructBaseStrict


class IdentitySpec(StructBaseStrict, frozen=True):
    """Specify how an entity ID column is derived.

    Parameters
    ----------
    namespace
        Stable namespace used for deterministic ID hashing
        (e.g. ``"cst_ref"``, ``"cst_def"``).
    id_column
        Output column name for the derived entity ID
        (e.g. ``"ref_id"``, ``"def_id"``).
    """

    namespace: str
    id_column: str


class LocationSpec(StructBaseStrict, frozen=True):
    """Specify the span location columns for an entity.

    Parameters
    ----------
    start_col
        Source column for the span start byte offset
        (e.g. ``"bstart"``, ``"def_bstart"``).
    end_col
        Source column for the span end byte offset
        (e.g. ``"bend"``, ``"def_bend"``).
    """

    start_col: str
    end_col: str


class ForeignKeySpec(StructBaseStrict, frozen=True):
    """Specify a foreign key reference to another entity.

    Parameters
    ----------
    column
        Output column name for the derived foreign key ID
        (e.g. ``"container_def_id"``, ``"call_id"``).
    target_entity
        Namespace of the referenced entity
        (e.g. ``"cst_def"``, ``"cst_call"``).
    ref_start_col
        Source column for the reference span start byte offset.
    ref_end_col
        Source column for the reference span end byte offset.
    guard_null_if
        Columns whose NULL values cause the FK to be set to NULL.
    """

    column: str
    target_entity: str
    ref_start_col: str
    ref_end_col: str
    guard_null_if: tuple[str, ...] = ()


class EntityDeclaration(StructBaseStrict, frozen=True):
    """Entity-centric declaration that generates a ``SemanticTableSpec``.

    Each declaration captures the conceptual entity (identity, location,
    content, relationships) at a higher level than the table-centric
    ``SemanticTableSpec``.  The converter in ``entity_registry`` produces
    structurally identical ``SemanticTableSpec`` instances from these
    declarations.

    Parameters
    ----------
    name
        Logical entity name (e.g. ``"cst_ref"``, ``"cst_def"``).
    source_table
        Extraction-layer table name (e.g. ``"cst_refs"``, ``"cst_defs"``).
    identity
        How the entity ID is derived.
    location
        Span columns that locate the entity in source code.
    normalized_name
        Override for the normalized output view name.  When ``None``,
        the default ``"<source_table>_norm"`` convention is used.
    content
        Content columns carried through normalization
        (e.g. ``("ref_text",)``).
    foreign_keys
        Foreign key references to other entities.
    span_unit
        Unit for span columns.  Currently only ``"byte"`` is supported.
    include_in_cpg_nodes
        Whether the normalized entity participates in the CPG nodes union.
    """

    name: str
    source_table: str
    identity: IdentitySpec
    location: LocationSpec
    normalized_name: str | None = None
    content: tuple[str, ...] = ()
    foreign_keys: tuple[ForeignKeySpec, ...] = ()
    span_unit: SpanUnit = "byte"
    include_in_cpg_nodes: bool = True

    def effective_normalized_name(self) -> str:
        """Return the normalized output view name for this entity.

        When ``normalized_name`` is set, return it directly.  Otherwise
        fall back to the ``"<source_table>_norm"`` convention.

        Returns:
        -------
        str
            Normalized view name.
        """
        if self.normalized_name is not None:
            return self.normalized_name
        return f"{self.source_table}_norm"


__all__ = [
    "EntityDeclaration",
    "ForeignKeySpec",
    "IdentitySpec",
    "LocationSpec",
]
