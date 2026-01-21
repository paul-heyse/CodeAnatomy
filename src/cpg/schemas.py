"""CPG Arrow schemas and contracts."""

from __future__ import annotations

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import table_from_schema
from cpg.registry_rows import SCHEMA_VERSION as _SCHEMA_VERSION
from cpg.registry_specs import dataset_contract_spec, dataset_schema, dataset_spec

CPG_NODES_SPEC = dataset_spec("cpg_nodes_v1")
CPG_EDGES_SPEC = dataset_spec("cpg_edges_v1")
CPG_PROPS_SPEC = dataset_spec("cpg_props_v1")
CPG_PROPS_JSON_SPEC = dataset_spec("cpg_props_json_v1")
CPG_PROPS_BY_FILE_ID_SPEC = dataset_spec("cpg_props_by_file_id_v1")
CPG_PROPS_GLOBAL_SPEC = dataset_spec("cpg_props_global_v1")

CPG_NODES_SCHEMA = dataset_schema("cpg_nodes_v1")
CPG_EDGES_SCHEMA = dataset_schema("cpg_edges_v1")
CPG_PROPS_SCHEMA = dataset_schema("cpg_props_v1")
CPG_PROPS_JSON_SCHEMA = dataset_schema("cpg_props_json_v1")
CPG_PROPS_BY_FILE_ID_SCHEMA = dataset_schema("cpg_props_by_file_id_v1")
CPG_PROPS_GLOBAL_SCHEMA = dataset_schema("cpg_props_global_v1")

CPG_NODES_CONTRACT_SPEC = dataset_contract_spec("cpg_nodes_v1")
CPG_EDGES_CONTRACT_SPEC = dataset_contract_spec("cpg_edges_v1")
CPG_PROPS_CONTRACT_SPEC = dataset_contract_spec("cpg_props_v1")
CPG_PROPS_JSON_CONTRACT_SPEC = dataset_contract_spec("cpg_props_json_v1")
CPG_PROPS_BY_FILE_ID_CONTRACT_SPEC = dataset_contract_spec("cpg_props_by_file_id_v1")
CPG_PROPS_GLOBAL_CONTRACT_SPEC = dataset_contract_spec("cpg_props_global_v1")

CPG_NODES_CONTRACT = CPG_NODES_CONTRACT_SPEC.to_contract()
CPG_EDGES_CONTRACT = CPG_EDGES_CONTRACT_SPEC.to_contract()
CPG_PROPS_CONTRACT = CPG_PROPS_CONTRACT_SPEC.to_contract()
CPG_PROPS_JSON_CONTRACT = CPG_PROPS_JSON_CONTRACT_SPEC.to_contract()
CPG_PROPS_BY_FILE_ID_CONTRACT = CPG_PROPS_BY_FILE_ID_CONTRACT_SPEC.to_contract()
CPG_PROPS_GLOBAL_CONTRACT = CPG_PROPS_GLOBAL_CONTRACT_SPEC.to_contract()
SCHEMA_VERSION = _SCHEMA_VERSION


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


def empty_props_json() -> TableLike:
    """Return an empty JSON props table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty JSON props table.
    """
    return table_from_schema(CPG_PROPS_JSON_SCHEMA, columns={}, num_rows=0)


def empty_props_by_file_id() -> TableLike:
    """Return an empty props-by-file table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty props-by-file table.
    """
    return table_from_schema(CPG_PROPS_BY_FILE_ID_SCHEMA, columns={}, num_rows=0)


def empty_props_global() -> TableLike:
    """Return an empty global props table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty global props table.
    """
    return table_from_schema(CPG_PROPS_GLOBAL_SCHEMA, columns={}, num_rows=0)
