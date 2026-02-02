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

from semantics import registry as _registry

RELATIONSHIP_SPECS = _registry.RELATIONSHIP_SPECS
SEMANTIC_NORMALIZATION_SPECS = _registry.SEMANTIC_NORMALIZATION_SPECS
SEMANTIC_TABLE_SPECS = _registry.SEMANTIC_TABLE_SPECS
SemanticNormalizationSpec = _registry.SemanticNormalizationSpec
normalization_output_name = _registry.normalization_output_name
normalization_output_names = _registry.normalization_output_names
normalization_spec_for_output = _registry.normalization_spec_for_output
normalization_spec_for_source = _registry.normalization_spec_for_source
relationship_names = _registry.relationship_names
spec_for_relationship = _registry.spec_for_relationship
spec_for_table = _registry.spec_for_table

SpecKind = Literal[
    "normalize",
    "scip_normalize",
    "symtable",
    "diagnostic",
    "export",
    "projection",
    "finalize",
    "artifact",
    "join_group",
    "relate",
    "union_nodes",
    "union_edges",
]


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
    from semantics.ir_pipeline import build_semantic_ir

    ir = build_semantic_ir()
    return tuple(
        SemanticSpecIndex(
            name=view.name,
            kind=view.kind,
            inputs=view.inputs,
            outputs=view.outputs,
        )
        for view in ir.views
    )


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
