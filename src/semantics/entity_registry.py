"""Entity registry: declarations and converters to table specs.

Provide a parallel entity-centric registry that converts
``EntityDeclaration`` instances into the existing ``SemanticTableSpec``
and ``SemanticNormalizationSpec`` types.  The generated specs must be
structurally identical to the hand-written entries in
``semantics.registry``.
"""

from __future__ import annotations

from typing import Final

from semantics.entity_model import (
    EntityDeclaration,
    ForeignKeySpec,
    IdentitySpec,
    LocationSpec,
)
from semantics.registry import SemanticNormalizationSpec
from semantics.specs import (
    ForeignKeyDerivation,
    IdDerivation,
    SemanticTableSpec,
    SpanBinding,
)


def entity_to_table_spec(decl: EntityDeclaration) -> SemanticTableSpec:
    """Convert an entity declaration to a ``SemanticTableSpec``.

    The conversion is lossless: every field on the produced
    ``SemanticTableSpec`` matches what the hand-written registry would
    produce for the same entity.

    Parameters
    ----------
    decl
        Entity declaration to convert.

    Returns:
    -------
    SemanticTableSpec
        Equivalent table spec.
    """
    primary_span = SpanBinding(
        start_col=decl.location.start_col,
        end_col=decl.location.end_col,
        unit=decl.span_unit,
    )

    entity_id = IdDerivation(
        out_col=decl.identity.id_column,
        namespace=decl.identity.namespace,
        start_col=decl.location.start_col,
        end_col=decl.location.end_col,
    )

    foreign_keys = tuple(_foreign_key_spec_to_derivation(fk) for fk in decl.foreign_keys)

    return SemanticTableSpec(
        table=decl.source_table,
        primary_span=primary_span,
        entity_id=entity_id,
        foreign_keys=foreign_keys,
        text_cols=decl.content,
    )


def _foreign_key_spec_to_derivation(fk: ForeignKeySpec) -> ForeignKeyDerivation:
    """Convert a ``ForeignKeySpec`` to a ``ForeignKeyDerivation``.

    Returns:
        ForeignKeyDerivation: Equivalent foreign-key derivation.
    """
    return ForeignKeyDerivation(
        out_col=fk.column,
        target_namespace=fk.target_entity,
        start_col=fk.ref_start_col,
        end_col=fk.ref_end_col,
        guard_null_if=fk.guard_null_if,
    )


def entity_to_normalization_spec(decl: EntityDeclaration) -> SemanticNormalizationSpec:
    """Convert an entity declaration to a ``SemanticNormalizationSpec``.

    Parameters
    ----------
    decl
        Entity declaration to convert.

    Returns:
    -------
    SemanticNormalizationSpec
        Equivalent normalization spec.
    """
    return SemanticNormalizationSpec(
        source_table=decl.source_table,
        normalized_name=decl.effective_normalized_name(),
        spec=entity_to_table_spec(decl),
        include_in_cpg_nodes=decl.include_in_cpg_nodes,
    )


def generate_table_specs(
    decls: tuple[EntityDeclaration, ...],
) -> dict[str, SemanticTableSpec]:
    """Generate a ``SemanticTableSpec`` dict from entity declarations.

    The returned dict is keyed by ``source_table``, matching the layout
    of ``SEMANTIC_TABLE_SPECS`` in ``semantics.registry``.

    Parameters
    ----------
    decls
        Entity declarations to convert.

    Returns:
    -------
    dict[str, SemanticTableSpec]
        Table specs keyed by source table name.
    """
    return {decl.source_table: entity_to_table_spec(decl) for decl in decls}


def generate_normalization_specs(
    decls: tuple[EntityDeclaration, ...],
) -> tuple[SemanticNormalizationSpec, ...]:
    """Generate ``SemanticNormalizationSpec`` entries from entity declarations.

    The returned tuple preserves the declaration order, matching the
    layout of ``SEMANTIC_NORMALIZATION_SPECS`` in ``semantics.registry``.

    Parameters
    ----------
    decls
        Entity declarations to convert.

    Returns:
    -------
    tuple[SemanticNormalizationSpec, ...]
        Normalization specs in declaration order.
    """
    return tuple(entity_to_normalization_spec(decl) for decl in decls)


# ---------------------------------------------------------------------------
# Canonical entity declarations
# ---------------------------------------------------------------------------

ENTITY_DECLARATIONS: Final[tuple[EntityDeclaration, ...]] = (
    EntityDeclaration(
        name="cst_ref",
        source_table="cst_refs",
        identity=IdentitySpec(namespace="cst_ref", id_column="ref_id"),
        location=LocationSpec(start_col="bstart", end_col="bend"),
        content=("ref_text",),
    ),
    EntityDeclaration(
        name="cst_def",
        source_table="cst_defs",
        identity=IdentitySpec(namespace="cst_def", id_column="def_id"),
        location=LocationSpec(start_col="def_bstart", end_col="def_bend"),
        foreign_keys=(
            ForeignKeySpec(
                column="container_def_id",
                target_entity="cst_def",
                ref_start_col="container_def_bstart",
                ref_end_col="container_def_bend",
                guard_null_if=("container_def_kind",),
            ),
        ),
    ),
    EntityDeclaration(
        name="cst_import",
        source_table="cst_imports",
        identity=IdentitySpec(namespace="cst_import", id_column="import_id"),
        location=LocationSpec(start_col="alias_bstart", end_col="alias_bend"),
    ),
    EntityDeclaration(
        name="cst_call",
        source_table="cst_callsites",
        identity=IdentitySpec(namespace="cst_call", id_column="call_id"),
        location=LocationSpec(start_col="call_bstart", end_col="call_bend"),
        normalized_name="cst_calls_norm",
    ),
    EntityDeclaration(
        name="cst_call_arg",
        source_table="cst_call_args",
        identity=IdentitySpec(namespace="cst_call_arg", id_column="call_arg_id"),
        location=LocationSpec(start_col="bstart", end_col="bend"),
        content=("arg_text",),
        foreign_keys=(
            ForeignKeySpec(
                column="call_id",
                target_entity="cst_call",
                ref_start_col="call_bstart",
                ref_end_col="call_bend",
            ),
        ),
    ),
    EntityDeclaration(
        name="cst_docstring",
        source_table="cst_docstrings",
        identity=IdentitySpec(namespace="cst_docstring", id_column="docstring_id"),
        location=LocationSpec(start_col="bstart", end_col="bend"),
        content=("docstring",),
        foreign_keys=(
            ForeignKeySpec(
                column="owner_def_id",
                target_entity="cst_def",
                ref_start_col="owner_def_bstart",
                ref_end_col="owner_def_bend",
            ),
        ),
    ),
    EntityDeclaration(
        name="cst_decorator",
        source_table="cst_decorators",
        identity=IdentitySpec(namespace="cst_decorator", id_column="decorator_id"),
        location=LocationSpec(start_col="bstart", end_col="bend"),
        content=("decorator_text",),
        foreign_keys=(
            ForeignKeySpec(
                column="owner_def_id",
                target_entity="cst_def",
                ref_start_col="owner_def_bstart",
                ref_end_col="owner_def_bend",
            ),
        ),
    ),
)


__all__ = [
    "ENTITY_DECLARATIONS",
    "entity_to_normalization_spec",
    "entity_to_table_spec",
    "generate_normalization_specs",
    "generate_table_specs",
]
