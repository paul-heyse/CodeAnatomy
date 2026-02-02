"""Unified semantic registry and model generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

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

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from cpg.specs import NodePlanSpec, PropTableSpec
    from semantics.catalog.dataset_rows import SemanticDatasetRow
    from semantics.cpg_entity_specs import CpgEntitySpec

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
        Normalization spec when registered, otherwise ``None``.
    """
    return _NORMALIZATION_BY_OUTPUT.get(output_name)


def normalization_spec_for_source(source_table: str) -> SemanticNormalizationSpec | None:
    """Return the normalization spec for a given source table name.

    Returns
    -------
    SemanticNormalizationSpec | None
        Normalization spec when registered, otherwise ``None``.
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
        Semantic table spec when registered, otherwise ``None``.
    """
    return SEMANTIC_TABLE_SPECS.get(table)


RELATIONSHIP_SPECS: Final[tuple[QualityRelationshipSpec, ...]] = (
    REL_NAME_SYMBOL,
    REL_DEF_SYMBOL,
    REL_IMPORT_SYMBOL,
    REL_CALLSITE_SYMBOL,
)


def spec_for_relationship(name: str) -> QualityRelationshipSpec | None:
    """Return a relationship spec by name when registered.

    Returns
    -------
    QualityRelationshipSpec | None
        Relationship spec when registered, otherwise ``None``.
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
        Registered relationship names.
    """
    return SEMANTIC_MODEL.relationship_names()


@dataclass(frozen=True)
class SemanticModel:
    """Single source of truth for semantic normalization and relationships."""

    normalization_specs: tuple[SemanticNormalizationSpec, ...]
    relationship_specs: tuple[QualityRelationshipSpec, ...]
    semantic_nodes_union_name: str
    semantic_edges_union_name: str

    def relationship_names(self) -> tuple[str, ...]:
        """Return names of all registered relationships.

        Returns
        -------
        tuple[str, ...]
            Registered relationship names.
        """
        return tuple(spec.name for spec in self.relationship_specs)

    def dataset_rows(self) -> tuple[SemanticDatasetRow, ...]:
        """Return semantic dataset rows generated from the model.

        Returns
        -------
        tuple[SemanticDatasetRow, ...]
            Dataset rows in dependency order.
        """
        from semantics.catalog.dataset_rows import build_semantic_dataset_rows

        return build_semantic_dataset_rows(self)

    def cpg_node_specs(self) -> tuple[NodePlanSpec, ...]:
        """Return CPG node plan specs generated from the model.

        Returns
        -------
        tuple[NodePlanSpec, ...]
            CPG node plan specs.
        """
        from cpg.spec_registry import build_node_plan_specs

        return build_node_plan_specs(self)

    def cpg_prop_specs(
        self,
        *,
        source_columns_lookup: Callable[[str], Sequence[str] | None] | None = None,
    ) -> tuple[PropTableSpec, ...]:
        """Return CPG prop table specs generated from the model.

        Parameters
        ----------
        source_columns_lookup
            Optional lookup for source columns by table name.

        Returns
        -------
        tuple[PropTableSpec, ...]
            CPG prop table specs.
        """
        from cpg.spec_registry import build_prop_table_specs

        return build_prop_table_specs(self, source_columns_lookup=source_columns_lookup)

    def cpg_entity_specs(self) -> tuple[CpgEntitySpec, ...]:
        """Return CPG entity specs generated from the model.

        Returns
        -------
        tuple[CpgEntitySpec, ...]
            CPG entity specs for node and prop emission.
        """
        from semantics.cpg_entity_specs import build_cpg_entity_specs

        return build_cpg_entity_specs(self)

    def normalization_output_names(self, *, include_nodes_only: bool = False) -> tuple[str, ...]:
        """Return normalization output names in model order.

        Returns
        -------
        tuple[str, ...]
            Normalization output names from the semantic model.
        """
        specs = self.normalization_specs
        if include_nodes_only:
            specs = tuple(spec for spec in specs if spec.include_in_cpg_nodes)
        return tuple(spec.output_name for spec in specs)


def build_semantic_model() -> SemanticModel:
    """Build the canonical semantic model from registry specs.

    Returns
    -------
    SemanticModel
        Unified semantic model.
    """
    return SemanticModel(
        normalization_specs=SEMANTIC_NORMALIZATION_SPECS,
        relationship_specs=RELATIONSHIP_SPECS,
        semantic_nodes_union_name=canonical_output_name("semantic_nodes_union"),
        semantic_edges_union_name=canonical_output_name("semantic_edges_union"),
    )


SEMANTIC_MODEL: SemanticModel = build_semantic_model()

__all__ = [
    "RELATIONSHIP_SPECS",
    "SEMANTIC_MODEL",
    "SEMANTIC_NORMALIZATION_SPECS",
    "SEMANTIC_TABLE_SPECS",
    "SemanticModel",
    "SemanticNormalizationSpec",
    "build_semantic_model",
    "normalization_output_name",
    "normalization_output_names",
    "normalization_spec_for_output",
    "normalization_spec_for_source",
    "relationship_names",
    "spec_for_relationship",
    "spec_for_table",
]
