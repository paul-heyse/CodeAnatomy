"""CPG Arrow schemas and contracts."""

from __future__ import annotations

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import table_from_schema
from cpg.registry_rows import SCHEMA_VERSION as _SCHEMA_VERSION
from cpg.registry_specs import dataset_contract_spec, dataset_schema, dataset_spec
from schema_spec.system import GLOBAL_SCHEMA_REGISTRY, SchemaRegistry

CPG_NODES_SPEC = dataset_spec("cpg_nodes_v1")
CPG_EDGES_SPEC = dataset_spec("cpg_edges_v1")
CPG_PROPS_SPEC = dataset_spec("cpg_props_v1")

CPG_NODES_SCHEMA = dataset_schema("cpg_nodes_v1")
CPG_EDGES_SCHEMA = dataset_schema("cpg_edges_v1")
CPG_PROPS_SCHEMA = dataset_schema("cpg_props_v1")

CPG_NODES_CONTRACT_SPEC = dataset_contract_spec("cpg_nodes_v1")
CPG_EDGES_CONTRACT_SPEC = dataset_contract_spec("cpg_edges_v1")
CPG_PROPS_CONTRACT_SPEC = dataset_contract_spec("cpg_props_v1")

CPG_NODES_CONTRACT = CPG_NODES_CONTRACT_SPEC.to_contract()
CPG_EDGES_CONTRACT = CPG_EDGES_CONTRACT_SPEC.to_contract()
CPG_PROPS_CONTRACT = CPG_PROPS_CONTRACT_SPEC.to_contract()
SCHEMA_VERSION = _SCHEMA_VERSION


def register_cpg_specs(registry: SchemaRegistry) -> SchemaRegistry:
    """Register CPG table and contract specs into the registry.

    Returns
    -------
    SchemaRegistry
        Registry with CPG specs added.
    """
    registry.register_dataset(CPG_NODES_SPEC)
    registry.register_dataset(CPG_EDGES_SPEC)
    registry.register_dataset(CPG_PROPS_SPEC)
    return registry


CPG_SCHEMA_REGISTRY = register_cpg_specs(GLOBAL_SCHEMA_REGISTRY)


def empty_nodes() -> TableLike:
    """Return an empty nodes table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty nodes table.
    """
    return table_from_schema(CPG_NODES_SCHEMA, columns={}, num_rows=0)


def empty_edges() -> TableLike:
    """Return an empty edges table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty edges table.
    """
    return table_from_schema(CPG_EDGES_SCHEMA, columns={}, num_rows=0)


def empty_props() -> TableLike:
    """Return an empty props table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty props table.
    """
    return table_from_schema(CPG_PROPS_SCHEMA, columns={}, num_rows=0)
