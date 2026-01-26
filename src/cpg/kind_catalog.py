"""Define stable CPG kind identifiers and edge requirements."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, NewType, cast

import pyarrow as pa

if TYPE_CHECKING:
    from arrowdsl.core.interop import SchemaLike

NodeKindId = NewType("NodeKindId", str)
EdgeKindId = NewType("EdgeKindId", str)


class EntityKind(StrEnum):
    """Define entity kind labels for CPG properties."""

    NODE = "node"
    EDGE = "edge"


NODE_KIND_PY_FILE = NodeKindId("PY_FILE")
NODE_KIND_CST_REF = NodeKindId("CST_REF")
NODE_KIND_CST_IMPORT_ALIAS = NodeKindId("CST_IMPORT_ALIAS")
NODE_KIND_CST_CALLSITE = NodeKindId("CST_CALLSITE")
NODE_KIND_CST_DEF = NodeKindId("CST_DEF")
NODE_KIND_SYM_SCOPE = NodeKindId("SYM_SCOPE")
NODE_KIND_SYM_SYMBOL = NodeKindId("SYM_SYMBOL")
NODE_KIND_PY_SCOPE = NodeKindId("PY_SCOPE")
NODE_KIND_PY_BINDING = NodeKindId("PY_BINDING")
NODE_KIND_PY_DEF_SITE = NodeKindId("PY_DEF_SITE")
NODE_KIND_PY_USE_SITE = NodeKindId("PY_USE_SITE")
NODE_KIND_TYPE_PARAM = NodeKindId("TYPE_PARAM")
NODE_KIND_PY_QUALIFIED_NAME = NodeKindId("PY_QUALIFIED_NAME")
NODE_KIND_SCIP_SYMBOL = NodeKindId("SCIP_SYMBOL")
NODE_KIND_TS_NODE = NodeKindId("TS_NODE")
NODE_KIND_TS_ERROR = NodeKindId("TS_ERROR")
NODE_KIND_TS_MISSING = NodeKindId("TS_MISSING")
NODE_KIND_TYPE_EXPR = NodeKindId("TYPE_EXPR")
NODE_KIND_TYPE = NodeKindId("TYPE")
NODE_KIND_DIAG = NodeKindId("DIAG")
NODE_KIND_RT_OBJECT = NodeKindId("RT_OBJECT")
NODE_KIND_RT_SIGNATURE = NodeKindId("RT_SIGNATURE")
NODE_KIND_RT_SIGNATURE_PARAM = NodeKindId("RT_SIGNATURE_PARAM")
NODE_KIND_RT_MEMBER = NodeKindId("RT_MEMBER")


EDGE_KIND_PY_CALLS_QNAME = EdgeKindId("PY_CALLS_QNAME")
EDGE_KIND_SCOPE_PARENT = EdgeKindId("SCOPE_PARENT")
EDGE_KIND_SCOPE_BINDS = EdgeKindId("SCOPE_BINDS")
EDGE_KIND_BINDING_RESOLVES_TO = EdgeKindId("BINDING_RESOLVES_TO")
EDGE_KIND_DEF_SITE_OF = EdgeKindId("DEF_SITE_OF")
EDGE_KIND_USE_SITE_OF = EdgeKindId("USE_SITE_OF")
EDGE_KIND_TYPE_PARAM_OF = EdgeKindId("TYPE_PARAM_OF")
EDGE_KIND_PY_DEFINES_SYMBOL = EdgeKindId("PY_DEFINES_SYMBOL")
EDGE_KIND_PY_REFERENCES_SYMBOL = EdgeKindId("PY_REFERENCES_SYMBOL")
EDGE_KIND_PY_READS_SYMBOL = EdgeKindId("PY_READS_SYMBOL")
EDGE_KIND_PY_WRITES_SYMBOL = EdgeKindId("PY_WRITES_SYMBOL")
EDGE_KIND_SCIP_SYMBOL_REFERENCE = EdgeKindId("SCIP_SYMBOL_REFERENCE")
EDGE_KIND_SCIP_SYMBOL_IMPLEMENTATION = EdgeKindId("SCIP_SYMBOL_IMPLEMENTATION")
EDGE_KIND_SCIP_SYMBOL_TYPE_DEFINITION = EdgeKindId("SCIP_SYMBOL_TYPE_DEFINITION")
EDGE_KIND_SCIP_SYMBOL_DEFINITION = EdgeKindId("SCIP_SYMBOL_DEFINITION")
EDGE_KIND_PY_IMPORTS_SYMBOL = EdgeKindId("PY_IMPORTS_SYMBOL")
EDGE_KIND_PY_CALLS_SYMBOL = EdgeKindId("PY_CALLS_SYMBOL")
EDGE_KIND_HAS_DIAGNOSTIC = EdgeKindId("HAS_DIAGNOSTIC")
EDGE_KIND_HAS_ANNOTATION = EdgeKindId("HAS_ANNOTATION")
EDGE_KIND_INFERRED_TYPE = EdgeKindId("INFERRED_TYPE")
EDGE_KIND_RT_HAS_SIGNATURE = EdgeKindId("RT_HAS_SIGNATURE")
EDGE_KIND_RT_HAS_PARAM = EdgeKindId("RT_HAS_PARAM")
EDGE_KIND_RT_HAS_MEMBER = EdgeKindId("RT_HAS_MEMBER")


@dataclass(frozen=True)
class EdgeKindSpec:
    """Define required relation-output props for an edge kind."""

    kind: EdgeKindId
    required_props: tuple[str, ...] = ()
    description: str = ""


EDGE_KIND_SPECS: tuple[EdgeKindSpec, ...] = (
    EdgeKindSpec(
        kind=EDGE_KIND_PY_DEFINES_SYMBOL,
        required_props=("symbol_roles", "origin", "resolution_method"),
    ),
    EdgeKindSpec(
        kind=EDGE_KIND_PY_REFERENCES_SYMBOL,
        required_props=("symbol_roles", "origin", "resolution_method"),
    ),
    EdgeKindSpec(
        kind=EDGE_KIND_PY_READS_SYMBOL,
        required_props=("symbol_roles", "origin", "resolution_method"),
    ),
    EdgeKindSpec(
        kind=EDGE_KIND_PY_WRITES_SYMBOL,
        required_props=("symbol_roles", "origin", "resolution_method"),
    ),
    EdgeKindSpec(
        kind=EDGE_KIND_SCIP_SYMBOL_REFERENCE,
        required_props=("origin", "resolution_method"),
    ),
    EdgeKindSpec(
        kind=EDGE_KIND_SCIP_SYMBOL_IMPLEMENTATION,
        required_props=("origin", "resolution_method"),
    ),
    EdgeKindSpec(
        kind=EDGE_KIND_SCIP_SYMBOL_TYPE_DEFINITION,
        required_props=("origin", "resolution_method"),
    ),
    EdgeKindSpec(
        kind=EDGE_KIND_SCIP_SYMBOL_DEFINITION,
        required_props=("origin", "resolution_method"),
    ),
    EdgeKindSpec(
        kind=EDGE_KIND_PY_IMPORTS_SYMBOL,
        required_props=("symbol_roles", "origin", "resolution_method"),
    ),
    EdgeKindSpec(
        kind=EDGE_KIND_PY_CALLS_SYMBOL,
        required_props=("origin", "resolution_method", "score"),
    ),
    EdgeKindSpec(
        kind=EDGE_KIND_PY_CALLS_QNAME,
        required_props=("origin", "qname_source", "score"),
    ),
    EdgeKindSpec(
        kind=EDGE_KIND_HAS_DIAGNOSTIC,
        required_props=("diag_source",),
    ),
    EdgeKindSpec(
        kind=EDGE_KIND_HAS_ANNOTATION,
        required_props=("origin",),
    ),
    EdgeKindSpec(
        kind=EDGE_KIND_INFERRED_TYPE,
        required_props=("origin", "score"),
    ),
    EdgeKindSpec(kind=EDGE_KIND_RT_HAS_SIGNATURE),
    EdgeKindSpec(kind=EDGE_KIND_RT_HAS_PARAM),
    EdgeKindSpec(kind=EDGE_KIND_RT_HAS_MEMBER),
    EdgeKindSpec(kind=EDGE_KIND_SCOPE_PARENT),
    EdgeKindSpec(kind=EDGE_KIND_SCOPE_BINDS),
    EdgeKindSpec(
        kind=EDGE_KIND_BINDING_RESOLVES_TO,
        required_props=("kind",),
    ),
    EdgeKindSpec(
        kind=EDGE_KIND_DEF_SITE_OF,
        required_props=("def_site_kind",),
    ),
    EdgeKindSpec(
        kind=EDGE_KIND_USE_SITE_OF,
        required_props=("use_kind",),
    ),
    EdgeKindSpec(kind=EDGE_KIND_TYPE_PARAM_OF),
)


def edge_kind_required_props() -> dict[str, set[str]]:
    """Return required relation-output props by edge kind name.

    Returns
    -------
    dict[str, set[str]]
        Mapping of edge kind identifiers to required property names.
    """
    return {str(spec.kind): set(spec.required_props) for spec in EDGE_KIND_SPECS}


def validate_edge_kind_requirements(schema: SchemaLike) -> None:
    """Validate edge-kind requirements against relation output schema.

    Parameters
    ----------
    schema:
        Relation output schema to validate against.

    Raises
    ------
    ValueError
        Raised when required edge props are missing from the schema.
    """
    available = _schema_names(schema)
    errors: list[str] = []
    for spec in EDGE_KIND_SPECS:
        missing = sorted(prop for prop in spec.required_props if prop not in available)
        if missing:
            errors.append(f"{spec.kind}: missing required props {missing}")
    if errors:
        msg = "Edge kind requirements not satisfied:\n- " + "\n- ".join(errors)
        raise ValueError(msg)


def _schema_names(schema: SchemaLike) -> set[str]:
    if isinstance(schema, pa.Schema):
        return set(schema.names)
    names = getattr(schema, "names", None)
    if names is not None:
        return set(names)
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            resolved_schema = cast("pa.Schema", resolved)
            return set(resolved_schema.names)
    to_pyarrow = getattr(schema, "to_pyarrow", None)
    if callable(to_pyarrow):
        resolved = to_pyarrow()
        if isinstance(resolved, pa.Schema):
            resolved_schema = cast("pa.Schema", resolved)
            return set(resolved_schema.names)
    msg = f"Unable to resolve schema names from {type(schema)!r}."
    raise TypeError(msg)


__all__ = [
    "EDGE_KIND_BINDING_RESOLVES_TO",
    "EDGE_KIND_DEF_SITE_OF",
    "EDGE_KIND_HAS_ANNOTATION",
    "EDGE_KIND_HAS_DIAGNOSTIC",
    "EDGE_KIND_INFERRED_TYPE",
    "EDGE_KIND_PY_CALLS_QNAME",
    "EDGE_KIND_PY_CALLS_SYMBOL",
    "EDGE_KIND_PY_DEFINES_SYMBOL",
    "EDGE_KIND_PY_IMPORTS_SYMBOL",
    "EDGE_KIND_PY_READS_SYMBOL",
    "EDGE_KIND_PY_REFERENCES_SYMBOL",
    "EDGE_KIND_PY_WRITES_SYMBOL",
    "EDGE_KIND_RT_HAS_MEMBER",
    "EDGE_KIND_RT_HAS_PARAM",
    "EDGE_KIND_RT_HAS_SIGNATURE",
    "EDGE_KIND_SCIP_SYMBOL_DEFINITION",
    "EDGE_KIND_SCIP_SYMBOL_IMPLEMENTATION",
    "EDGE_KIND_SCIP_SYMBOL_REFERENCE",
    "EDGE_KIND_SCIP_SYMBOL_TYPE_DEFINITION",
    "EDGE_KIND_SCOPE_BINDS",
    "EDGE_KIND_SCOPE_PARENT",
    "EDGE_KIND_SPECS",
    "EDGE_KIND_TYPE_PARAM_OF",
    "EDGE_KIND_USE_SITE_OF",
    "NODE_KIND_CST_CALLSITE",
    "NODE_KIND_CST_DEF",
    "NODE_KIND_CST_IMPORT_ALIAS",
    "NODE_KIND_CST_REF",
    "NODE_KIND_DIAG",
    "NODE_KIND_PY_BINDING",
    "NODE_KIND_PY_DEF_SITE",
    "NODE_KIND_PY_FILE",
    "NODE_KIND_PY_QUALIFIED_NAME",
    "NODE_KIND_PY_SCOPE",
    "NODE_KIND_PY_USE_SITE",
    "NODE_KIND_RT_MEMBER",
    "NODE_KIND_RT_OBJECT",
    "NODE_KIND_RT_SIGNATURE",
    "NODE_KIND_RT_SIGNATURE_PARAM",
    "NODE_KIND_SCIP_SYMBOL",
    "NODE_KIND_SYM_SCOPE",
    "NODE_KIND_SYM_SYMBOL",
    "NODE_KIND_TS_ERROR",
    "NODE_KIND_TS_MISSING",
    "NODE_KIND_TS_NODE",
    "NODE_KIND_TYPE",
    "NODE_KIND_TYPE_EXPR",
    "NODE_KIND_TYPE_PARAM",
    "EdgeKindId",
    "EdgeKindSpec",
    "EntityKind",
    "NodeKindId",
    "edge_kind_required_props",
    "validate_edge_kind_requirements",
]
