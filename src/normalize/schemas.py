"""Programmatic schema definitions for normalize datasets."""

from __future__ import annotations

from normalize.registry_fields import DIAG_DETAIL_STRUCT, DIAG_DETAILS_TYPE
from normalize.registry_rows import SCHEMA_VERSION
from normalize.registry_specs import (
    dataset_contract,
    dataset_query,
    dataset_schema,
    dataset_spec,
)

_TYPE_EXPRS_NAME = "type_exprs_norm_v1"
_TYPE_NODES_NAME = "type_nodes_v1"
_CFG_BLOCKS_NAME = "py_bc_blocks_norm_v1"
_CFG_EDGES_NAME = "py_bc_cfg_edges_norm_v1"
_DEF_USE_NAME = "py_bc_def_use_events_v1"
_REACHES_NAME = "py_bc_reaches_v1"
_SPAN_ERRORS_NAME = "span_errors_v1"
_DIAG_NAME = "diagnostics_norm_v1"

TYPE_EXPRS_NORM_SPEC = dataset_spec(_TYPE_EXPRS_NAME)
TYPE_EXPRS_QUERY = dataset_query(_TYPE_EXPRS_NAME)
TYPE_EXPRS_CONTRACT = dataset_contract(_TYPE_EXPRS_NAME)
TYPE_EXPRS_NORM_SCHEMA = dataset_schema(_TYPE_EXPRS_NAME)

TYPE_NODES_SPEC = dataset_spec(_TYPE_NODES_NAME)
TYPE_NODES_QUERY = dataset_query(_TYPE_NODES_NAME)
TYPE_NODES_CONTRACT = dataset_contract(_TYPE_NODES_NAME)
TYPE_NODES_SCHEMA = dataset_schema(_TYPE_NODES_NAME)

CFG_BLOCKS_NORM_SPEC = dataset_spec(_CFG_BLOCKS_NAME)
CFG_BLOCKS_QUERY = dataset_query(_CFG_BLOCKS_NAME)
CFG_BLOCKS_CONTRACT = dataset_contract(_CFG_BLOCKS_NAME)
CFG_BLOCKS_NORM_SCHEMA = dataset_schema(_CFG_BLOCKS_NAME)

CFG_EDGES_NORM_SPEC = dataset_spec(_CFG_EDGES_NAME)
CFG_EDGES_QUERY = dataset_query(_CFG_EDGES_NAME)
CFG_EDGES_CONTRACT = dataset_contract(_CFG_EDGES_NAME)
CFG_EDGES_NORM_SCHEMA = dataset_schema(_CFG_EDGES_NAME)

DEF_USE_SPEC = dataset_spec(_DEF_USE_NAME)
DEF_USE_QUERY = dataset_query(_DEF_USE_NAME)
DEF_USE_CONTRACT = dataset_contract(_DEF_USE_NAME)
DEF_USE_SCHEMA = dataset_schema(_DEF_USE_NAME)

REACHES_SPEC = dataset_spec(_REACHES_NAME)
REACHES_QUERY = dataset_query(_REACHES_NAME)
REACHES_CONTRACT = dataset_contract(_REACHES_NAME)
REACHES_SCHEMA = dataset_schema(_REACHES_NAME)

SPAN_ERROR_SPEC = dataset_spec(_SPAN_ERRORS_NAME)
SPAN_ERROR_SCHEMA = dataset_schema(_SPAN_ERRORS_NAME)

DIAG_SPEC = dataset_spec(_DIAG_NAME)
DIAG_QUERY = dataset_query(_DIAG_NAME)
DIAG_CONTRACT = dataset_contract(_DIAG_NAME)
DIAG_SCHEMA = dataset_schema(_DIAG_NAME)

__all__ = [
    "CFG_BLOCKS_CONTRACT",
    "CFG_BLOCKS_NORM_SCHEMA",
    "CFG_BLOCKS_NORM_SPEC",
    "CFG_BLOCKS_QUERY",
    "CFG_EDGES_CONTRACT",
    "CFG_EDGES_NORM_SCHEMA",
    "CFG_EDGES_NORM_SPEC",
    "CFG_EDGES_QUERY",
    "DEF_USE_CONTRACT",
    "DEF_USE_QUERY",
    "DEF_USE_SCHEMA",
    "DEF_USE_SPEC",
    "DIAG_CONTRACT",
    "DIAG_DETAILS_TYPE",
    "DIAG_DETAIL_STRUCT",
    "DIAG_QUERY",
    "DIAG_SCHEMA",
    "DIAG_SPEC",
    "REACHES_CONTRACT",
    "REACHES_QUERY",
    "REACHES_SCHEMA",
    "REACHES_SPEC",
    "SCHEMA_VERSION",
    "SPAN_ERROR_SCHEMA",
    "SPAN_ERROR_SPEC",
    "TYPE_EXPRS_CONTRACT",
    "TYPE_EXPRS_NORM_SCHEMA",
    "TYPE_EXPRS_NORM_SPEC",
    "TYPE_EXPRS_QUERY",
    "TYPE_NODES_CONTRACT",
    "TYPE_NODES_QUERY",
    "TYPE_NODES_SCHEMA",
    "TYPE_NODES_SPEC",
]
