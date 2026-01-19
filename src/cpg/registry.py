"""OO registry facade for CPG datasets and plan specs."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import cache
from typing import ClassVar

import pyarrow as pa

from arrowdsl.core.interop import SchemaLike
from cpg.edge_specs import edge_plan_specs_from_table
from cpg.registry_builders import build_dataset_spec
from cpg.registry_readers import dataset_rows_from_table
from cpg.registry_tables import dataset_rows_table
from cpg.spec_tables import (
    node_plan_spec_table,
    node_plan_specs_from_table,
    prop_table_spec_table,
    prop_table_specs_from_table,
)
from cpg.specs import EdgePlanSpec, NodePlanSpec, PropTableSpec
from relspec.model import RelationshipRule
from relspec.rules.cache import rule_table_cached
from relspec.rules.handlers.cpg import relationship_rule_from_definition
from relspec.rules.spec_tables import rule_definitions_from_table
from schema_spec.system import ContractSpec, DatasetSpec


@dataclass(frozen=True)
class CpgRegistry:
    """Registry facade exposing datasets and plan specs."""

    NODES_DATASET: ClassVar[str] = "cpg_nodes_v1"
    EDGES_DATASET: ClassVar[str] = "cpg_edges_v1"
    PROPS_DATASET: ClassVar[str] = "cpg_props_v1"
    PROPS_JSON_DATASET: ClassVar[str] = "cpg_props_json_v1"

    dataset_specs: Mapping[str, DatasetSpec]
    node_plan_spec_table: pa.Table
    prop_table_spec_table: pa.Table
    relation_rule_table: pa.Table

    def dataset_spec(self, name: str) -> DatasetSpec:
        """Return the DatasetSpec for the dataset name.

        Returns
        -------
        DatasetSpec
            Dataset specification for the dataset name.
        """
        return self.dataset_specs[name]

    def dataset_schema(self, name: str) -> SchemaLike:
        """Return the Arrow schema for the dataset name.

        Returns
        -------
        SchemaLike
            Arrow schema for the dataset name.
        """
        return self.dataset_spec(name).schema()

    def dataset_contract_spec(self, name: str) -> ContractSpec:
        """Return the ContractSpec for the dataset name.

        Returns
        -------
        ContractSpec
            Contract specification for the dataset name.
        """
        return self.dataset_spec(name).contract_spec_or_default()

    def nodes_spec(self) -> DatasetSpec:
        """Return the DatasetSpec for CPG nodes.

        Returns
        -------
        DatasetSpec
            Dataset specification for CPG nodes.
        """
        return self.dataset_spec(self.NODES_DATASET)

    def edges_spec(self) -> DatasetSpec:
        """Return the DatasetSpec for CPG edges.

        Returns
        -------
        DatasetSpec
            Dataset specification for CPG edges.
        """
        return self.dataset_spec(self.EDGES_DATASET)

    def props_spec(self) -> DatasetSpec:
        """Return the DatasetSpec for CPG props.

        Returns
        -------
        DatasetSpec
            Dataset specification for CPG props.
        """
        return self.dataset_spec(self.PROPS_DATASET)

    def props_json_spec(self) -> DatasetSpec:
        """Return the DatasetSpec for JSON-heavy CPG props.

        Returns
        -------
        DatasetSpec
            Dataset specification for JSON props.
        """
        return self.dataset_spec(self.PROPS_JSON_DATASET)

    def node_plan_specs(self) -> tuple[NodePlanSpec, ...]:
        """Return node plan specs decoded from the registry table.

        Returns
        -------
        tuple[NodePlanSpec, ...]
            Node plan specs decoded from the registry table.
        """
        return node_plan_specs_from_table(self.node_plan_spec_table)

    def edge_plan_specs(self) -> tuple[EdgePlanSpec, ...]:
        """Return edge plan specs decoded from the registry table.

        Returns
        -------
        tuple[EdgePlanSpec, ...]
            Edge plan specs decoded from the registry table.
        """
        return edge_plan_specs_from_table(self.relation_rule_table)

    def prop_table_specs(self) -> tuple[PropTableSpec, ...]:
        """Return prop table specs decoded from the registry table.

        Returns
        -------
        tuple[PropTableSpec, ...]
            Prop table specs decoded from the registry table.
        """
        return prop_table_specs_from_table(self.prop_table_spec_table)

    def relation_rules(self) -> tuple[RelationshipRule, ...]:
        """Return relationship rules decoded from the registry table.

        Returns
        -------
        tuple[RelationshipRule, ...]
            Relationship rules decoded from the registry table.
        """
        definitions = rule_definitions_from_table(self.relation_rule_table)
        return tuple(
            relationship_rule_from_definition(defn) for defn in definitions if defn.domain == "cpg"
        )


@cache
def default_cpg_registry() -> CpgRegistry:
    """Return the default CPG registry constructed from row specs.

    Returns
    -------
    CpgRegistry
        Default registry instance.
    """
    rows = dataset_rows_from_table(dataset_rows_table())
    dataset_specs = {row.name: build_dataset_spec(row) for row in rows}
    return CpgRegistry(
        dataset_specs=dataset_specs,
        node_plan_spec_table=node_plan_spec_table(),
        prop_table_spec_table=prop_table_spec_table(),
        relation_rule_table=relation_rule_table_cached(),
    )


@cache
def relation_rule_table_cached() -> pa.Table:
    """Return the canonical relationship rule table for CPG edges.

    Returns
    -------
    pa.Table
        Canonical rule table containing CPG relationship rules.
    """
    return rule_table_cached("cpg")


__all__ = ["CpgRegistry", "default_cpg_registry", "relation_rule_table_cached"]
