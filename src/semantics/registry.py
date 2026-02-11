"""Unified semantic registry and model generation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Literal

from semantics.naming import canonical_output_name
from semantics.quality import QualityRelationshipSpec
from semantics.quality_specs import (
    REL_CALLSITE_SYMBOL,
    REL_DEF_SYMBOL,
    REL_IMPORT_SYMBOL,
    REL_NAME_SYMBOL,
)
from semantics.specs import SemanticTableSpec
from utils.registry_protocol import MappingRegistryAdapter

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from semantics.catalog.dataset_rows import SemanticDatasetRow
    from semantics.cpg.specs import NodePlanSpec, PropTableSpec
    from semantics.cpg_entity_specs import CpgEntitySpec


def _generate_semantic_table_specs() -> dict[str, SemanticTableSpec]:
    """Generate semantic table specs from entity declarations.

    Deferred into a helper to break the import cycle between
    ``semantics.registry`` and ``semantics.entity_registry``.

    Returns:
    -------
    dict[str, SemanticTableSpec]
        Table specs keyed by source table name.
    """
    from semantics.entity_registry import ENTITY_DECLARATIONS, generate_table_specs

    return generate_table_specs(ENTITY_DECLARATIONS)


SEMANTIC_TABLE_SPECS: Final[dict[str, SemanticTableSpec]] = _generate_semantic_table_specs()


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


def semantic_table_registry() -> MappingRegistryAdapter[str, SemanticTableSpec]:
    """Return a registry adapter for semantic table specs.

    Returns:
    -------
    MappingRegistryAdapter[str, SemanticTableSpec]
        Read-only registry adapter for semantic table specs.
    """
    return MappingRegistryAdapter.from_mapping(SEMANTIC_TABLE_SPECS, read_only=True)


def semantic_normalization_registry() -> MappingRegistryAdapter[str, SemanticNormalizationSpec]:
    """Return a registry adapter for semantic normalization specs.

    Returns:
    -------
    MappingRegistryAdapter[str, SemanticNormalizationSpec]
        Read-only registry adapter for semantic normalization specs.
    """
    mapping = {spec.output_name: spec for spec in SEMANTIC_NORMALIZATION_SPECS}
    return MappingRegistryAdapter.from_mapping(mapping, read_only=True)


def normalization_spec_for_output(output_name: str) -> SemanticNormalizationSpec | None:
    """Return the normalization spec for a given output view name.

    Returns:
    -------
    SemanticNormalizationSpec | None
        Normalization spec when registered, otherwise ``None``.
    """
    return _NORMALIZATION_BY_OUTPUT.get(output_name)


def normalization_spec_for_source(source_table: str) -> SemanticNormalizationSpec | None:
    """Return the normalization spec for a given source table name.

    Returns:
    -------
    SemanticNormalizationSpec | None
        Normalization spec when registered, otherwise ``None``.
    """
    return _NORMALIZATION_BY_SOURCE.get(source_table)


def normalization_output_name(source_table: str) -> str:
    """Return the canonical normalization output name for a source table.

    Returns:
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

    Returns:
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

    Returns:
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


def semantic_relationship_registry() -> MappingRegistryAdapter[str, QualityRelationshipSpec]:
    """Return a registry adapter for semantic relationship specs.

    Returns:
    -------
    MappingRegistryAdapter[str, QualityRelationshipSpec]
        Read-only registry adapter for semantic relationship specs.
    """
    mapping = {spec.name: spec for spec in RELATIONSHIP_SPECS}
    return MappingRegistryAdapter.from_mapping(mapping, read_only=True)


def spec_for_relationship(name: str) -> QualityRelationshipSpec | None:
    """Return a relationship spec by name when registered.

    Returns:
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

    Returns:
    -------
    tuple[str, ...]
        Registered relationship names.
    """
    return SEMANTIC_MODEL.relationship_names()


SemanticOutputKind = Literal["table", "diagnostic", "export"]


@dataclass(frozen=True)
class SemanticOutputSpec:
    """Output specification for semantic pipeline products."""

    name: str
    kind: SemanticOutputKind = "table"
    contract_ref: str | None = None


@dataclass(frozen=True)
class SemanticModel:
    """Single source of truth for semantic normalization and relationships."""

    normalization_specs: tuple[SemanticNormalizationSpec, ...]
    relationship_specs: tuple[QualityRelationshipSpec, ...]
    semantic_nodes_union_name: str
    semantic_edges_union_name: str
    outputs: tuple[SemanticOutputSpec, ...]

    def relationship_names(self) -> tuple[str, ...]:
        """Return names of all registered relationships.

        Returns:
        -------
        tuple[str, ...]
            Registered relationship names.
        """
        return tuple(spec.name for spec in self.relationship_specs)

    @staticmethod
    def dataset_rows() -> tuple[SemanticDatasetRow, ...]:
        """Return semantic dataset rows generated from the model.

        Returns:
        -------
        tuple[SemanticDatasetRow, ...]
            Dataset rows in dependency order.
        """
        from semantics.compile_context import semantic_ir_for_outputs

        return semantic_ir_for_outputs().dataset_rows

    def cpg_node_specs(self) -> tuple[NodePlanSpec, ...]:
        """Return CPG node plan specs generated from the model.

        Returns:
        -------
        tuple[NodePlanSpec, ...]
            CPG node plan specs.
        """
        from semantics.cpg.spec_registry import build_node_plan_specs

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

        Returns:
        -------
        tuple[PropTableSpec, ...]
            CPG prop table specs.
        """
        from semantics.cpg.spec_registry import build_prop_table_specs

        return build_prop_table_specs(self, source_columns_lookup=source_columns_lookup)

    def cpg_entity_specs(self) -> tuple[CpgEntitySpec, ...]:
        """Return CPG entity specs generated from the model.

        Returns:
        -------
        tuple[CpgEntitySpec, ...]
            CPG entity specs for node and prop emission.
        """
        from semantics.cpg_entity_specs import build_cpg_entity_specs

        return build_cpg_entity_specs(self)

    def normalization_output_names(self, *, include_nodes_only: bool = False) -> tuple[str, ...]:
        """Return normalization output names in model order.

        Returns:
        -------
        tuple[str, ...]
            Normalization output names from the semantic model.
        """
        specs = self.normalization_specs
        if include_nodes_only:
            specs = tuple(spec for spec in specs if spec.include_in_cpg_nodes)
        return tuple(spec.output_name for spec in specs)


def _build_output_specs() -> tuple[SemanticOutputSpec, ...]:
    from relspec.view_defs import RELATION_OUTPUT_NAME
    from semantics.diagnostics import SEMANTIC_DIAGNOSTIC_VIEW_NAMES
    from semantics.naming import canonical_output_name

    ordered: list[SemanticOutputSpec] = []
    seen: set[str] = set()

    def _append(spec: SemanticOutputSpec) -> None:
        if spec.name in seen:
            return
        seen.add(spec.name)
        ordered.append(spec)

    for spec in SEMANTIC_NORMALIZATION_SPECS:
        _append(
            SemanticOutputSpec(name=spec.output_name, kind="table", contract_ref=spec.output_name)
        )

    scip_norm = canonical_output_name("scip_occurrences_norm")
    _append(SemanticOutputSpec(name=scip_norm, kind="table", contract_ref=scip_norm))

    for spec in RELATIONSHIP_SPECS:
        _append(SemanticOutputSpec(name=spec.name, kind="table", contract_ref=spec.name))

    _append(
        SemanticOutputSpec(
            name=canonical_output_name("semantic_nodes_union"),
            kind="table",
            contract_ref=canonical_output_name("semantic_nodes_union"),
        )
    )
    _append(
        SemanticOutputSpec(
            name=canonical_output_name("semantic_edges_union"),
            kind="table",
            contract_ref=canonical_output_name("semantic_edges_union"),
        )
    )

    for name in (
        "cpg_nodes",
        "cpg_edges",
        "cpg_props",
        "cpg_props_map",
        "cpg_edges_by_src",
        "cpg_edges_by_dst",
    ):
        canonical = canonical_output_name(name)
        _append(SemanticOutputSpec(name=canonical, kind="table", contract_ref=canonical))

    for name in (RELATION_OUTPUT_NAME,):
        _append(SemanticOutputSpec(name=name, kind="table", contract_ref=name))

    for name in SEMANTIC_DIAGNOSTIC_VIEW_NAMES:
        _append(SemanticOutputSpec(name=name, kind="diagnostic", contract_ref=name))

    for name in ("dim_exported_defs",):
        _append(SemanticOutputSpec(name=name, kind="export", contract_ref=name))

    return tuple(ordered)


def build_semantic_model() -> SemanticModel:
    """Build the canonical semantic model from registry specs.

    Returns:
    -------
    SemanticModel
        Unified semantic model.
    """
    return SemanticModel(
        normalization_specs=SEMANTIC_NORMALIZATION_SPECS,
        relationship_specs=RELATIONSHIP_SPECS,
        semantic_nodes_union_name=canonical_output_name("semantic_nodes_union"),
        semantic_edges_union_name=canonical_output_name("semantic_edges_union"),
        outputs=_build_output_specs(),
    )


SEMANTIC_MODEL: SemanticModel = build_semantic_model()

__all__ = [
    "RELATIONSHIP_SPECS",
    "SEMANTIC_MODEL",
    "SEMANTIC_NORMALIZATION_SPECS",
    "SEMANTIC_TABLE_SPECS",
    "SemanticModel",
    "SemanticNormalizationSpec",
    "SemanticOutputKind",
    "SemanticOutputSpec",
    "build_semantic_model",
    "normalization_output_name",
    "normalization_output_names",
    "normalization_spec_for_output",
    "normalization_spec_for_source",
    "relationship_names",
    "semantic_normalization_registry",
    "semantic_relationship_registry",
    "semantic_table_registry",
    "spec_for_relationship",
    "spec_for_table",
]
