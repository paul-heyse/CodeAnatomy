"""Declarative specifications for relationship DataFrame builders.

This module provides a DSL for defining relationship extraction specifications
that capture the unique parameters for each relationship type while enabling
a generic builder to construct the DataFusion DataFrames.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Final

from cpg.kind_catalog import (
    EDGE_KIND_PY_CALLS_QNAME,
    EDGE_KIND_PY_CALLS_SYMBOL,
    EDGE_KIND_PY_DEFINES_SYMBOL,
    EDGE_KIND_PY_IMPORTS_SYMBOL,
    EDGE_KIND_PY_REFERENCES_SYMBOL,
    EdgeKindId,
)

if TYPE_CHECKING:
    from collections.abc import Sequence

# Minimum columns required for coalesce
_MIN_COALESCE_COLUMNS: Final[int] = 2


class RelationshipOrigin(StrEnum):
    """Define origin labels for relationship extraction sources."""

    CST = "cst"
    SCIP = "scip"
    SYMTABLE = "symtable"


@dataclass(frozen=True, slots=True)
class ColumnRef:
    """Reference to a single column by name."""

    name: str

    def __str__(self) -> str:
        """Return the column name for debugging.

        Returns
        -------
        str
            The column name.
        """
        return self.name


@dataclass(frozen=True, slots=True)
class CoalesceRef:
    """Reference to a coalesced column expression.

    The first non-null value from the ordered columns is selected.
    """

    columns: tuple[str, ...]

    def __post_init__(self) -> None:
        """Validate that at least two columns are provided.

        Raises
        ------
        ValueError
            Raised when fewer than two columns are specified.
        """
        if len(self.columns) < _MIN_COALESCE_COLUMNS:
            msg = "CoalesceRef requires at least two columns."
            raise ValueError(msg)

    def __str__(self) -> str:
        """Return the column list for debugging.

        Returns
        -------
        str
            Comma-separated column names wrapped in coalesce().
        """
        return f"coalesce({', '.join(self.columns)})"


type ColumnSpec = ColumnRef | CoalesceRef


def col_ref(name: str) -> ColumnRef:
    """Create a simple column reference.

    Parameters
    ----------
    name
        Column name to reference.

    Returns
    -------
    ColumnRef
        Column reference for the specified name.
    """
    return ColumnRef(name)


def coalesce_ref(*columns: str) -> CoalesceRef:
    """Create a coalesced column reference.

    Parameters
    ----------
    *columns
        Column names to coalesce in priority order.

    Returns
    -------
    CoalesceRef
        Coalesced column reference.
    """
    return CoalesceRef(columns)


@dataclass(frozen=True, slots=True)
class RelationshipSpec:
    """Declarative specification for a relationship DataFrame builder.

    This captures all the unique parameters needed to build a relationship
    extraction DataFrame, enabling a single generic builder function.

    Parameters
    ----------
    name
        Unique identifier for the relationship spec (e.g., "name_symbol").
    edge_kind
        Target edge kind for the relationship output.
    origin
        Origin label for the extraction source.
    src_table
        Source table name in the DataFusion context.
    entity_id_col
        Column specification for the entity ID.
    symbol_col
        Column specification for the symbol lookup value.
    resolution_method
        Resolution method label for provenance.
    output_view_name
        Canonical output view name with version suffix.
    bstart_col
        Column specification for byte start position.
    bend_col
        Column specification for byte end position.
    path_col
        Column name for file path.
    edge_owner_file_id_col
        Primary column for edge owner file ID.
    edge_owner_file_id_fallback
        Fallback column for edge owner file ID.
    symbol_roles_col
        Column name for symbol roles (if present).
    confidence
        Default confidence score for the relationship.
    score
        Default score for the relationship.
    """

    name: str
    edge_kind: EdgeKindId
    origin: RelationshipOrigin
    src_table: str
    entity_id_col: ColumnSpec
    symbol_col: ColumnSpec
    resolution_method: str
    output_view_name: str
    bstart_col: ColumnSpec = field(default_factory=lambda: col_ref("bstart"))
    bend_col: ColumnSpec = field(default_factory=lambda: col_ref("bend"))
    path_col: str = "path"
    edge_owner_file_id_col: str = "edge_owner_file_id"
    edge_owner_file_id_fallback: str = "file_id"
    symbol_roles_col: str | None = "symbol_roles"
    confidence: float = 0.5
    score: float = 0.5

    @property
    def entity_id_alias(self) -> str:
        """Return the output alias for the entity ID column.

        Returns
        -------
        str
            Alias for the entity ID column in the output.
        """
        if isinstance(self.entity_id_col, ColumnRef):
            return self.entity_id_col.name
        return self.entity_id_col.columns[0]


@dataclass(frozen=True, slots=True)
class QNameRelationshipSpec:
    """Declarative specification for qualified name relationship builders.

    QName relationships have a different output structure than symbol
    relationships, requiring a separate spec type.

    Parameters
    ----------
    name
        Unique identifier for the relationship spec.
    edge_kind
        Target edge kind for the relationship output.
    origin
        Origin label for the extraction source.
    src_table
        Source table name in the DataFusion context.
    entity_id_col
        Column specification for the entity ID.
    qname_col
        Column name for the qualified name.
    qname_source_col
        Column name for the qname source label.
    output_view_name
        Canonical output view name with version suffix.
    bstart_col
        Column specification for byte start position.
    bend_col
        Column specification for byte end position.
    path_col
        Column name for file path.
    edge_owner_file_id_col
        Column name for edge owner file ID.
    ambiguity_group_col
        Column name for ambiguity group ID.
    confidence
        Default confidence score for the relationship.
    score
        Default score for the relationship.
    """

    name: str
    edge_kind: EdgeKindId
    origin: RelationshipOrigin
    src_table: str
    entity_id_col: ColumnSpec
    qname_col: str
    qname_source_col: str
    output_view_name: str
    bstart_col: ColumnSpec = field(default_factory=lambda: col_ref("call_bstart"))
    bend_col: ColumnSpec = field(default_factory=lambda: col_ref("call_bend"))
    path_col: str = "path"
    edge_owner_file_id_col: str = "edge_owner_file_id"
    ambiguity_group_col: str = "ambiguity_group_id"
    confidence: float = 0.5
    score: float = 0.5

    @property
    def entity_id_alias(self) -> str:
        """Return the output alias for the entity ID column.

        Returns
        -------
        str
            Alias for the entity ID column in the output.
        """
        if isinstance(self.entity_id_col, ColumnRef):
            return self.entity_id_col.name
        return self.entity_id_col.columns[0]


# -----------------------------------------------------------------------------
# Relationship Spec Definitions
# -----------------------------------------------------------------------------

REL_NAME_SYMBOL_SPEC = RelationshipSpec(
    name="name_symbol",
    edge_kind=EDGE_KIND_PY_REFERENCES_SYMBOL,
    origin=RelationshipOrigin.CST,
    src_table="cst_refs",
    entity_id_col=col_ref("ref_id"),
    symbol_col=col_ref("ref_text"),
    resolution_method="cst_ref_text",
    output_view_name="rel_name_symbol_v1",
    bstart_col=col_ref("bstart"),
    bend_col=col_ref("bend"),
    confidence=0.5,
    score=0.5,
)

REL_IMPORT_SYMBOL_SPEC = RelationshipSpec(
    name="import_symbol",
    edge_kind=EDGE_KIND_PY_IMPORTS_SYMBOL,
    origin=RelationshipOrigin.CST,
    src_table="cst_imports",
    entity_id_col=coalesce_ref("import_alias_id", "import_id"),
    symbol_col=coalesce_ref("name", "module"),
    resolution_method="cst_import_name",
    output_view_name="rel_import_symbol_v1",
    bstart_col=coalesce_ref("alias_bstart", "stmt_bstart"),
    bend_col=coalesce_ref("alias_bend", "stmt_bend"),
    confidence=0.5,
    score=0.5,
)

REL_DEF_SYMBOL_SPEC = RelationshipSpec(
    name="def_symbol",
    edge_kind=EDGE_KIND_PY_DEFINES_SYMBOL,
    origin=RelationshipOrigin.CST,
    src_table="cst_defs",
    entity_id_col=col_ref("def_id"),
    symbol_col=col_ref("name"),
    resolution_method="cst_def_name",
    output_view_name="rel_def_symbol_v1",
    bstart_col=coalesce_ref("name_bstart", "def_bstart"),
    bend_col=coalesce_ref("name_bend", "def_bend"),
    confidence=0.6,
    score=0.6,
)

REL_CALLSITE_SYMBOL_SPEC = RelationshipSpec(
    name="callsite_symbol",
    edge_kind=EDGE_KIND_PY_CALLS_SYMBOL,
    origin=RelationshipOrigin.CST,
    src_table="cst_callsites",
    entity_id_col=col_ref("call_id"),
    symbol_col=coalesce_ref("callee_text", "callee_dotted"),
    resolution_method="cst_callsite",
    output_view_name="rel_callsite_symbol_v1",
    bstart_col=col_ref("call_bstart"),
    bend_col=col_ref("call_bend"),
    confidence=0.6,
    score=0.6,
)

REL_CALLSITE_QNAME_SPEC = QNameRelationshipSpec(
    name="callsite_qname",
    edge_kind=EDGE_KIND_PY_CALLS_QNAME,
    origin=RelationshipOrigin.CST,
    src_table="callsite_qname_candidates_v1",
    entity_id_col=col_ref("call_id"),
    qname_col="qname",
    qname_source_col="qname_source",
    output_view_name="rel_callsite_qname_v1",
    bstart_col=col_ref("call_bstart"),
    bend_col=col_ref("call_bend"),
    confidence=0.5,
    score=0.5,
)


# -----------------------------------------------------------------------------
# Spec Registry
# -----------------------------------------------------------------------------

SYMBOL_RELATIONSHIP_SPECS: tuple[RelationshipSpec, ...] = (
    REL_NAME_SYMBOL_SPEC,
    REL_IMPORT_SYMBOL_SPEC,
    REL_DEF_SYMBOL_SPEC,
    REL_CALLSITE_SYMBOL_SPEC,
)

QNAME_RELATIONSHIP_SPECS: tuple[QNameRelationshipSpec, ...] = (REL_CALLSITE_QNAME_SPEC,)

ALL_RELATIONSHIP_SPECS: tuple[RelationshipSpec | QNameRelationshipSpec, ...] = (
    *SYMBOL_RELATIONSHIP_SPECS,
    *QNAME_RELATIONSHIP_SPECS,
)


def get_relationship_spec(name: str) -> RelationshipSpec | QNameRelationshipSpec:
    """Return the relationship spec by name.

    Parameters
    ----------
    name
        Unique identifier for the relationship spec.

    Returns
    -------
    RelationshipSpec | QNameRelationshipSpec
        The matching relationship specification.

    Raises
    ------
    KeyError
        Raised when no spec matches the given name.
    """
    for spec in ALL_RELATIONSHIP_SPECS:
        if spec.name == name:
            return spec
    msg = f"Unknown relationship spec: {name!r}"
    raise KeyError(msg)


def get_symbol_specs() -> Sequence[RelationshipSpec]:
    """Return all symbol-based relationship specs.

    Returns
    -------
    Sequence[RelationshipSpec]
        Tuple of symbol relationship specifications.
    """
    return SYMBOL_RELATIONSHIP_SPECS


def get_qname_specs() -> Sequence[QNameRelationshipSpec]:
    """Return all qname-based relationship specs.

    Returns
    -------
    Sequence[QNameRelationshipSpec]
        Tuple of qname relationship specifications.
    """
    return QNAME_RELATIONSHIP_SPECS


__all__ = [
    "ALL_RELATIONSHIP_SPECS",
    "QNAME_RELATIONSHIP_SPECS",
    "REL_CALLSITE_QNAME_SPEC",
    "REL_CALLSITE_SYMBOL_SPEC",
    "REL_DEF_SYMBOL_SPEC",
    "REL_IMPORT_SYMBOL_SPEC",
    "REL_NAME_SYMBOL_SPEC",
    "SYMBOL_RELATIONSHIP_SPECS",
    "CoalesceRef",
    "ColumnRef",
    "ColumnSpec",
    "QNameRelationshipSpec",
    "RelationshipOrigin",
    "RelationshipSpec",
    "coalesce_ref",
    "col_ref",
    "get_qname_specs",
    "get_relationship_spec",
    "get_symbol_specs",
]
