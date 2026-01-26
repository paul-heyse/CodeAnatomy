"""CPG Arrow schemas and contracts."""

from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING, Any

from arrowdsl.core.interop import TableLike
from arrowdsl.schema.build import table_from_schema

if TYPE_CHECKING:
    import pyarrow as pa

    from schema_spec.dataset_handle import DatasetSpec

SCHEMA_VERSION = 1

# Lazy accessor functions to avoid circular imports


@cache
def _get_dataset_spec(name: str) -> DatasetSpec:
    from schema_spec.system import dataset_spec_from_schema

    schema = _get_dataset_schema(name)
    return dataset_spec_from_schema(name, schema)


@cache
def _get_dataset_schema(name: str) -> pa.Schema:
    from datafusion_engine.schema_registry import schema_for
    import pyarrow as pa

    if name == "cpg_props_by_file_id_v1":
        base = schema_for("cpg_props_v1")
        if "file_id" in base.names:
            return base
        fields = list(base)
        fields.append(pa.field("file_id", pa.string(), nullable=True))
        return pa.schema(fields, metadata=base.metadata)
    if name in {"cpg_props_global_v1", "cpg_props_json_v1"}:
        return schema_for("cpg_props_v1")
    return schema_for(name)


# Lazy property accessors


def _cpg_nodes_spec() -> DatasetSpec:
    return _get_dataset_spec("cpg_nodes_v1")


def _cpg_edges_spec() -> DatasetSpec:
    return _get_dataset_spec("cpg_edges_v1")


def _cpg_props_spec() -> DatasetSpec:
    return _get_dataset_spec("cpg_props_v1")


def _cpg_props_json_spec() -> DatasetSpec:
    return _get_dataset_spec("cpg_props_json_v1")


def _cpg_props_by_file_id_spec() -> DatasetSpec:
    return _get_dataset_spec("cpg_props_by_file_id_v1")


def _cpg_props_global_spec() -> DatasetSpec:
    return _get_dataset_spec("cpg_props_global_v1")


def _cpg_nodes_schema() -> pa.Schema:
    return _get_dataset_schema("cpg_nodes_v1")


def _cpg_edges_schema() -> pa.Schema:
    return _get_dataset_schema("cpg_edges_v1")


def _cpg_props_schema() -> pa.Schema:
    return _get_dataset_schema("cpg_props_v1")


def _cpg_props_json_schema() -> pa.Schema:
    return _get_dataset_schema("cpg_props_json_v1")


def _cpg_props_by_file_id_schema() -> pa.Schema:
    return _get_dataset_schema("cpg_props_by_file_id_v1")


def _cpg_props_global_schema() -> pa.Schema:
    return _get_dataset_schema("cpg_props_global_v1")


# Module-level lazy attribute access
_LAZY_ATTRS: dict[str, Any] = {
    "CPG_NODES_SPEC": _cpg_nodes_spec,
    "CPG_EDGES_SPEC": _cpg_edges_spec,
    "CPG_PROPS_SPEC": _cpg_props_spec,
    "CPG_PROPS_JSON_SPEC": _cpg_props_json_spec,
    "CPG_PROPS_BY_FILE_ID_SPEC": _cpg_props_by_file_id_spec,
    "CPG_PROPS_GLOBAL_SPEC": _cpg_props_global_spec,
    "CPG_NODES_SCHEMA": _cpg_nodes_schema,
    "CPG_EDGES_SCHEMA": _cpg_edges_schema,
    "CPG_PROPS_SCHEMA": _cpg_props_schema,
    "CPG_PROPS_JSON_SCHEMA": _cpg_props_json_schema,
    "CPG_PROPS_BY_FILE_ID_SCHEMA": _cpg_props_by_file_id_schema,
    "CPG_PROPS_GLOBAL_SCHEMA": _cpg_props_global_schema,
}

_DERIVED_ATTRS: dict[str, tuple[str, str]] = {
    "CPG_NODES_CONTRACT_SPEC": ("CPG_NODES_SPEC", "contract_spec_or_default"),
    "CPG_EDGES_CONTRACT_SPEC": ("CPG_EDGES_SPEC", "contract_spec_or_default"),
    "CPG_PROPS_CONTRACT_SPEC": ("CPG_PROPS_SPEC", "contract_spec_or_default"),
    "CPG_PROPS_JSON_CONTRACT_SPEC": ("CPG_PROPS_JSON_SPEC", "contract_spec_or_default"),
    "CPG_PROPS_BY_FILE_ID_CONTRACT_SPEC": ("CPG_PROPS_BY_FILE_ID_SPEC", "contract_spec_or_default"),
    "CPG_PROPS_GLOBAL_CONTRACT_SPEC": ("CPG_PROPS_GLOBAL_SPEC", "contract_spec_or_default"),
}

_CONTRACT_ATTRS: dict[str, str] = {
    "CPG_NODES_CONTRACT": "CPG_NODES_CONTRACT_SPEC",
    "CPG_EDGES_CONTRACT": "CPG_EDGES_CONTRACT_SPEC",
    "CPG_PROPS_CONTRACT": "CPG_PROPS_CONTRACT_SPEC",
    "CPG_PROPS_JSON_CONTRACT": "CPG_PROPS_JSON_CONTRACT_SPEC",
    "CPG_PROPS_BY_FILE_ID_CONTRACT": "CPG_PROPS_BY_FILE_ID_CONTRACT_SPEC",
    "CPG_PROPS_GLOBAL_CONTRACT": "CPG_PROPS_GLOBAL_CONTRACT_SPEC",
}

_SCHEMA_CONTRACT_ATTRS: dict[str, str] = {
    "CPG_NODES_SCHEMA_CONTRACT": "CPG_NODES_SPEC",
    "CPG_EDGES_SCHEMA_CONTRACT": "CPG_EDGES_SPEC",
    "CPG_PROPS_SCHEMA_CONTRACT": "CPG_PROPS_SPEC",
    "CPG_PROPS_JSON_SCHEMA_CONTRACT": "CPG_PROPS_JSON_SPEC",
    "CPG_PROPS_BY_FILE_ID_SCHEMA_CONTRACT": "CPG_PROPS_BY_FILE_ID_SPEC",
    "CPG_PROPS_GLOBAL_SCHEMA_CONTRACT": "CPG_PROPS_GLOBAL_SPEC",
}


def __getattr__(name: str) -> Any:
    if name in _LAZY_ATTRS:
        return _LAZY_ATTRS[name]()
    if name in _DERIVED_ATTRS:
        base_name, method_name = _DERIVED_ATTRS[name]
        base = __getattr__(base_name)
        return getattr(base, method_name)()
    if name in _CONTRACT_ATTRS:
        spec_name = _CONTRACT_ATTRS[name]
        spec = __getattr__(spec_name)
        return spec.to_contract()
    if name in _SCHEMA_CONTRACT_ATTRS:
        spec_name = _SCHEMA_CONTRACT_ATTRS[name]
        spec = __getattr__(spec_name)
        from datafusion_engine.schema_contracts import schema_contract_from_dataset_spec

        return schema_contract_from_dataset_spec(name=spec.name, spec=spec)
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)


def empty_nodes() -> TableLike:
    """Return an empty nodes table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty nodes table.
    """
    return table_from_schema(_cpg_nodes_schema(), columns={}, num_rows=0)


def empty_edges() -> TableLike:
    """Return an empty edges table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty edges table.
    """
    return table_from_schema(_cpg_edges_schema(), columns={}, num_rows=0)


def empty_props() -> TableLike:
    """Return an empty props table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty props table.
    """
    return table_from_schema(_cpg_props_schema(), columns={}, num_rows=0)


def empty_props_json() -> TableLike:
    """Return an empty JSON props table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty JSON props table.
    """
    return table_from_schema(_cpg_props_json_schema(), columns={}, num_rows=0)


def empty_props_by_file_id() -> TableLike:
    """Return an empty props-by-file table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty props-by-file table.
    """
    return table_from_schema(_cpg_props_by_file_id_schema(), columns={}, num_rows=0)


def empty_props_global() -> TableLike:
    """Return an empty global props table with the canonical schema.

    Returns
    -------
    pyarrow.Table
        Empty global props table.
    """
    return table_from_schema(_cpg_props_global_schema(), columns={}, num_rows=0)
