"""Canonical semantic table and relationship specs.

This module provides:
- SEMANTIC_TABLE_SPECS: Registry of table normalization specs for ambiguous schemas
- RELATIONSHIP_SPECS: Registry of declarative relationship specifications

Relationship specs define how to join normalized tables to build CPG edges.
New relationships can be added by extending RELATIONSHIP_SPECS without
modifying pipeline code.

All view names use the canonical naming policy from semantics.naming.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Literal

from semantics.naming import canonical_output_name
from semantics.quality import QualityRelationshipSpec
from semantics.quality_specs import (
    REL_CALLSITE_SYMBOL,
    REL_DEF_SYMBOL,
    REL_IMPORT_SYMBOL,
    REL_NAME_SYMBOL,
)
from semantics.specs import (
    ForeignKeyDerivation,
    IdDerivation,
    SemanticTableSpec,
    SpanBinding,
)

SEMANTIC_TABLE_SPECS: Final[dict[str, SemanticTableSpec]] = {
    "cst_refs": SemanticTableSpec(
        table="cst_refs",
        primary_span=SpanBinding("bstart", "bend"),
        entity_id=IdDerivation(
            out_col="ref_id",
            namespace="cst_ref",
            start_col="bstart",
            end_col="bend",
        ),
        text_cols=("ref_text",),
    ),
    "cst_defs": SemanticTableSpec(
        table="cst_defs",
        primary_span=SpanBinding("def_bstart", "def_bend"),
        entity_id=IdDerivation(
            out_col="def_id",
            namespace="cst_def",
            start_col="def_bstart",
            end_col="def_bend",
        ),
        foreign_keys=(
            ForeignKeyDerivation(
                out_col="container_def_id",
                target_namespace="cst_def",
                start_col="container_def_bstart",
                end_col="container_def_bend",
                guard_null_if=("container_def_kind",),
            ),
        ),
    ),
    "cst_imports": SemanticTableSpec(
        table="cst_imports",
        primary_span=SpanBinding("alias_bstart", "alias_bend"),
        entity_id=IdDerivation(
            out_col="import_id",
            namespace="cst_import",
            start_col="alias_bstart",
            end_col="alias_bend",
        ),
    ),
    "cst_callsites": SemanticTableSpec(
        table="cst_callsites",
        primary_span=SpanBinding("call_bstart", "call_bend"),
        entity_id=IdDerivation(
            out_col="call_id",
            namespace="cst_call",
            start_col="call_bstart",
            end_col="call_bend",
        ),
    ),
    "cst_call_args": SemanticTableSpec(
        table="cst_call_args",
        primary_span=SpanBinding("bstart", "bend"),
        entity_id=IdDerivation(
            out_col="call_arg_id",
            namespace="cst_call_arg",
            start_col="bstart",
            end_col="bend",
        ),
        foreign_keys=(
            ForeignKeyDerivation(
                out_col="call_id",
                target_namespace="cst_call",
                start_col="call_bstart",
                end_col="call_bend",
            ),
        ),
        text_cols=("arg_text",),
    ),
    "cst_docstrings": SemanticTableSpec(
        table="cst_docstrings",
        primary_span=SpanBinding("bstart", "bend"),
        entity_id=IdDerivation(
            out_col="docstring_id",
            namespace="cst_docstring",
            start_col="bstart",
            end_col="bend",
        ),
        foreign_keys=(
            ForeignKeyDerivation(
                out_col="owner_def_id",
                target_namespace="cst_def",
                start_col="owner_def_bstart",
                end_col="owner_def_bend",
            ),
        ),
        text_cols=("docstring",),
    ),
    "cst_decorators": SemanticTableSpec(
        table="cst_decorators",
        primary_span=SpanBinding("bstart", "bend"),
        entity_id=IdDerivation(
            out_col="decorator_id",
            namespace="cst_decorator",
            start_col="bstart",
            end_col="bend",
        ),
        foreign_keys=(
            ForeignKeyDerivation(
                out_col="owner_def_id",
                target_namespace="cst_def",
                start_col="owner_def_bstart",
                end_col="owner_def_bend",
            ),
        ),
        text_cols=("decorator_text",),
    ),
}


@dataclass(frozen=True)
class SemanticNormalizationSpec:
    """Normalization spec entry with output naming policy."""

    source_table: str
    normalized_name: str
    spec: SemanticTableSpec
    include_in_cpg_nodes: bool = False

    @property
    def output_name(self) -> str:
        """Return the canonical output view name."""
        return canonical_output_name(self.normalized_name)


SEMANTIC_NORMALIZATION_SPECS: Final[tuple[SemanticNormalizationSpec, ...]] = (
    SemanticNormalizationSpec(
        source_table="cst_refs",
        normalized_name="cst_refs_norm",
        spec=SEMANTIC_TABLE_SPECS["cst_refs"],
        include_in_cpg_nodes=True,
    ),
    SemanticNormalizationSpec(
        source_table="cst_defs",
        normalized_name="cst_defs_norm",
        spec=SEMANTIC_TABLE_SPECS["cst_defs"],
        include_in_cpg_nodes=True,
    ),
    SemanticNormalizationSpec(
        source_table="cst_imports",
        normalized_name="cst_imports_norm",
        spec=SEMANTIC_TABLE_SPECS["cst_imports"],
        include_in_cpg_nodes=True,
    ),
    SemanticNormalizationSpec(
        source_table="cst_callsites",
        normalized_name="cst_calls_norm",
        spec=SEMANTIC_TABLE_SPECS["cst_callsites"],
        include_in_cpg_nodes=True,
    ),
    SemanticNormalizationSpec(
        source_table="cst_call_args",
        normalized_name="cst_call_args_norm",
        spec=SEMANTIC_TABLE_SPECS["cst_call_args"],
        include_in_cpg_nodes=True,
    ),
    SemanticNormalizationSpec(
        source_table="cst_docstrings",
        normalized_name="cst_docstrings_norm",
        spec=SEMANTIC_TABLE_SPECS["cst_docstrings"],
        include_in_cpg_nodes=True,
    ),
    SemanticNormalizationSpec(
        source_table="cst_decorators",
        normalized_name="cst_decorators_norm",
        spec=SEMANTIC_TABLE_SPECS["cst_decorators"],
        include_in_cpg_nodes=True,
    ),
)

_NORMALIZATION_BY_OUTPUT: Final[dict[str, SemanticNormalizationSpec]] = {
    spec.output_name: spec for spec in SEMANTIC_NORMALIZATION_SPECS
}
_NORMALIZATION_BY_SOURCE: Final[dict[str, SemanticNormalizationSpec]] = {
    spec.source_table: spec for spec in SEMANTIC_NORMALIZATION_SPECS
}


def normalization_spec_for_output(output_name: str) -> SemanticNormalizationSpec | None:
    """Return the normalization spec for a given output view name.

    Returns
    -------
    SemanticNormalizationSpec | None
        Normalization spec for the output when registered.
    """
    return _NORMALIZATION_BY_OUTPUT.get(output_name)


def normalization_spec_for_source(source_table: str) -> SemanticNormalizationSpec | None:
    """Return the normalization spec for a given source table name.

    Returns
    -------
    SemanticNormalizationSpec | None
        Normalization spec for the source when registered.
    """
    return _NORMALIZATION_BY_SOURCE.get(source_table)


def normalization_output_name(source_table: str) -> str:
    """Return the canonical normalization output name for a source table.

    Returns
    -------
    str
        Canonical normalization output name.
    """
    spec = normalization_spec_for_source(source_table)
    if spec is None:
        return canonical_output_name(f"{source_table}_norm")
    return spec.output_name


def normalization_output_names(*, include_nodes_only: bool = False) -> tuple[str, ...]:
    """Return normalization output names in registry order.

    Returns
    -------
    tuple[str, ...]
        Normalization output names in registry order.
    """
    specs = SEMANTIC_NORMALIZATION_SPECS
    if include_nodes_only:
        specs = tuple(spec for spec in specs if spec.include_in_cpg_nodes)
    return tuple(spec.output_name for spec in specs)


def spec_for_table(table: str) -> SemanticTableSpec | None:
    """Return a semantic spec for the table name when registered.

    Returns
    -------
    SemanticTableSpec | None
        Registered spec or None when absent.
    """
    return SEMANTIC_TABLE_SPECS.get(table)


# -----------------------------------------------------------------------------
# Relationship Specifications
# -----------------------------------------------------------------------------
# These specs define how normalized tables are joined to build CPG edges.
# New relationships require spec changes only - no pipeline edits needed.
# All names use the canonical naming policy for consistent versioned outputs.

# Canonical normalized table names
_SCIP_NORM = canonical_output_name("scip_occurrences_norm")

RELATIONSHIP_SPECS: Final[tuple[QualityRelationshipSpec, ...]] = (
    REL_NAME_SYMBOL,
    REL_DEF_SYMBOL,
    REL_IMPORT_SYMBOL,
    REL_CALLSITE_SYMBOL,
)


def spec_for_relationship(name: str) -> QualityRelationshipSpec | None:
    """Return a relationship spec by name when registered.

    Parameters
    ----------
    name
        The relationship name (e.g., "rel_name_symbol").

    Returns
    -------
    QualityRelationshipSpec | None
        Registered spec or None when absent.
    """
    for spec in RELATIONSHIP_SPECS:
        if spec.name == name:
            return spec
    return None


def relationship_names() -> tuple[str, ...]:
    """Return names of all registered relationships.

    Returns
    -------
    tuple[str, ...]
        Tuple of relationship names in registration order.
    """
    return tuple(spec.name for spec in RELATIONSHIP_SPECS)


SpecKind = Literal["normalize", "scip_normalize", "relate", "union_nodes", "union_edges"]


@dataclass(frozen=True)
class SemanticSpecIndex:
    """Index entry for spec-driven semantic pipeline generation."""

    name: str
    kind: SpecKind
    inputs: tuple[str, ...]
    outputs: tuple[str, ...]
    output_policy: Literal["raw", "v1"] = "v1"


def _build_semantic_spec_index() -> tuple[SemanticSpecIndex, ...]:
    """Build the full spec index for the semantic pipeline.

    Returns
    -------
    tuple[SemanticSpecIndex, ...]
        Spec index in dependency-safe order.
    """
    specs: list[SemanticSpecIndex] = []
    specs.extend(
        [
            SemanticSpecIndex(
                name=spec.output_name,
                kind="normalize",
                inputs=(spec.source_table,),
                outputs=(spec.output_name,),
            )
            for spec in SEMANTIC_NORMALIZATION_SPECS
        ]
    )
    specs.append(
        SemanticSpecIndex(
            name=_SCIP_NORM,
            kind="scip_normalize",
            inputs=("scip_occurrences", "file_line_index_v1"),
            outputs=(_SCIP_NORM,),
        )
    )
    specs.extend(
        [
            SemanticSpecIndex(
                name=spec.name,
                kind="relate",
                inputs=(spec.left_view, spec.right_view),
                outputs=(spec.name,),
            )
            for spec in RELATIONSHIP_SPECS
        ]
    )
    specs.append(
        SemanticSpecIndex(
            name=canonical_output_name("cpg_edges"),
            kind="union_edges",
            inputs=tuple(spec.name for spec in RELATIONSHIP_SPECS),
            outputs=(canonical_output_name("cpg_edges"),),
        )
    )
    specs.append(
        SemanticSpecIndex(
            name=canonical_output_name("cpg_nodes"),
            kind="union_nodes",
            inputs=normalization_output_names(include_nodes_only=True),
            outputs=(canonical_output_name("cpg_nodes"),),
        )
    )
    return tuple(specs)


SEMANTIC_SPEC_INDEX: Final[tuple[SemanticSpecIndex, ...]] = _build_semantic_spec_index()


def semantic_spec_index() -> tuple[SemanticSpecIndex, ...]:
    """Return the full spec index for the semantic pipeline.

    Returns
    -------
    tuple[SemanticSpecIndex, ...]
        Immutable semantic spec index.
    """
    return SEMANTIC_SPEC_INDEX


__all__ = [
    "RELATIONSHIP_SPECS",
    "SEMANTIC_NORMALIZATION_SPECS",
    "SEMANTIC_SPEC_INDEX",
    "SEMANTIC_TABLE_SPECS",
    "SemanticNormalizationSpec",
    "SemanticSpecIndex",
    "normalization_output_name",
    "normalization_output_names",
    "normalization_spec_for_output",
    "normalization_spec_for_source",
    "relationship_names",
    "semantic_spec_index",
    "spec_for_relationship",
    "spec_for_table",
]
