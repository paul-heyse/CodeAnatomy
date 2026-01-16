"""Engine-agnostic relational plan nodes and signatures."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field

from arrowdsl.core.context import Ordering
from arrowdsl.core.interop import SchemaLike
from arrowdsl.spec.expr_ir import ExprIR
from ibis_engine.query_compiler import IbisQuerySpec
from relspec.model import DatasetRef, HashJoinConfig
from relspec.rules.rel_ops import AggregateExpr


@dataclass(frozen=True)
class RelSource:
    """Source dataset reference for a relational plan."""

    ref: DatasetRef
    query: IbisQuerySpec | None = None
    label: str = ""


@dataclass(frozen=True)
class RelProject:
    """Projection node for a relational plan."""

    source: RelNode
    columns: tuple[str, ...] = ()
    derived: Mapping[str, ExprIR] = field(default_factory=dict)
    label: str = ""


@dataclass(frozen=True)
class RelFilter:
    """Filter node for a relational plan."""

    source: RelNode
    predicate: ExprIR
    pushdown: bool = False
    label: str = ""


@dataclass(frozen=True)
class RelJoin:
    """Join node for a relational plan."""

    left: RelNode
    right: RelNode
    join: HashJoinConfig
    label: str = ""


@dataclass(frozen=True)
class RelAggregate:
    """Aggregate node for a relational plan."""

    source: RelNode
    group_by: tuple[str, ...] = ()
    aggregates: tuple[AggregateExpr, ...] = ()
    label: str = ""


@dataclass(frozen=True)
class RelUnion:
    """Union node for a relational plan."""

    inputs: tuple[RelNode, ...]
    distinct: bool = False
    label: str = ""


RelNode = RelSource | RelProject | RelFilter | RelJoin | RelAggregate | RelUnion


@dataclass(frozen=True)
class RelPlan:
    """Engine-agnostic relational plan with metadata."""

    root: RelNode
    schema: SchemaLike | None = None
    ordering: Ordering = field(default_factory=Ordering.unordered)
    label: str = ""


def rel_plan_signature(plan: RelPlan) -> str:
    """Return a stable signature hash for a relational plan.

    Returns
    -------
    str
        Deterministic hash of the relational plan payload.
    """
    payload = _rel_plan_payload(plan)
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _rel_plan_payload(plan: RelPlan) -> dict[str, object]:
    """Build a JSON-serializable payload for a relational plan.

    Parameters
    ----------
    plan
        Relational plan to serialize.

    Returns
    -------
    dict[str, object]
        Serialized plan payload.
    """
    return {
        "label": plan.label,
        "ordering": _ordering_payload(plan.ordering),
        "schema": _schema_payload(plan.schema),
        "root": _node_payload(plan.root),
    }


def _ordering_payload(ordering: Ordering) -> dict[str, object]:
    """Serialize ordering metadata for a plan payload.

    Parameters
    ----------
    ordering
        Ordering metadata to serialize.

    Returns
    -------
    dict[str, object]
        Serialized ordering metadata.
    """
    return {"level": ordering.level.value, "keys": list(ordering.keys)}


def _schema_payload(schema: SchemaLike | None) -> dict[str, object] | None:
    """Serialize a schema to a JSON-friendly payload.

    Parameters
    ----------
    schema
        Schema to serialize, if present.

    Returns
    -------
    dict[str, object] | None
        Serialized schema payload or ``None`` when absent.
    """
    if schema is None:
        return None
    return {
        "fields": [(field.name, str(field.type)) for field in schema],
        "metadata": _metadata_payload(schema.metadata),
    }


def _metadata_payload(metadata: Mapping[bytes, bytes] | None) -> dict[str, str]:
    """Serialize schema metadata to string key/value pairs.

    Parameters
    ----------
    metadata
        Raw metadata mapping from Arrow schemas.

    Returns
    -------
    dict[str, str]
        UTF-8 decoded metadata mapping.
    """
    if not metadata:
        return {}
    return {key.decode("utf-8"): value.decode("utf-8") for key, value in metadata.items()}


def _node_payload(node: RelNode) -> dict[str, object]:
    """Serialize a relational plan node to a JSON-friendly payload.

    Parameters
    ----------
    node
        Relational plan node to serialize.

    Returns
    -------
    dict[str, object]
        Serialized node payload.

    Raises
    ------
    TypeError
        Raised when the node type is unsupported.
    """
    if isinstance(node, RelSource):
        return {
            "kind": "source",
            "dataset": node.ref.name,
            "label": node.label,
            "query": _query_payload(node.query),
        }
    if isinstance(node, RelProject):
        return {
            "kind": "project",
            "columns": list(node.columns),
            "derived": {name: expr.to_dict() for name, expr in node.derived.items()},
            "label": node.label,
            "source": _node_payload(node.source),
        }
    if isinstance(node, RelFilter):
        return {
            "kind": "filter",
            "predicate": node.predicate.to_dict(),
            "pushdown": node.pushdown,
            "label": node.label,
            "source": _node_payload(node.source),
        }
    if isinstance(node, RelJoin):
        return {
            "kind": "join",
            "left": _node_payload(node.left),
            "right": _node_payload(node.right),
            "join": _join_payload(node.join),
            "label": node.label,
        }
    if isinstance(node, RelAggregate):
        return {
            "kind": "aggregate",
            "group_by": list(node.group_by),
            "aggregates": [_aggregate_payload(spec) for spec in node.aggregates],
            "label": node.label,
            "source": _node_payload(node.source),
        }
    if isinstance(node, RelUnion):
        return {
            "kind": "union",
            "distinct": node.distinct,
            "label": node.label,
            "inputs": [_node_payload(item) for item in node.inputs],
        }
    msg = f"Unsupported RelNode type: {type(node).__name__}."
    raise TypeError(msg)


def _query_payload(query: IbisQuerySpec | None) -> dict[str, object] | None:
    """Serialize an optional query spec into a payload.

    Parameters
    ----------
    query
        Query specification to serialize.

    Returns
    -------
    dict[str, object] | None
        Serialized query payload or ``None`` when absent.
    """
    if query is None:
        return None
    derived = {name: _expr_payload(expr) for name, expr in query.projection.derived.items()}
    return {
        "projection": {
            "base": list(query.projection.base),
            "derived": derived,
        },
        "predicate": _expr_payload(query.predicate),
        "pushdown_predicate": _expr_payload(query.pushdown_predicate),
    }


def _expr_payload(expr: object | None) -> object | None:
    """Serialize an expression to a JSON-friendly payload.

    Parameters
    ----------
    expr
        Expression to serialize.

    Returns
    -------
    object | None
        Serialized expression payload or ``None`` when absent.
    """
    if expr is None:
        return None
    if isinstance(expr, ExprIR):
        return expr.to_dict()
    return repr(expr)


def _join_payload(spec: HashJoinConfig) -> dict[str, object]:
    """Serialize a hash join configuration to a payload.

    Parameters
    ----------
    spec
        Hash join configuration to serialize.

    Returns
    -------
    dict[str, object]
        Serialized join configuration.
    """

    def _output(value: tuple[str, ...] | None) -> list[str] | None:
        if value is None:
            return None
        return list(value)

    return {
        "join_type": spec.join_type,
        "left_keys": list(spec.left_keys),
        "right_keys": list(spec.right_keys),
        "left_output": _output(spec.left_output),
        "right_output": _output(spec.right_output),
        "output_suffix_for_left": spec.output_suffix_for_left,
        "output_suffix_for_right": spec.output_suffix_for_right,
    }


def _aggregate_payload(spec: AggregateExpr) -> dict[str, object]:
    """Serialize an aggregate expression to a payload.

    Parameters
    ----------
    spec
        Aggregate expression specification to serialize.

    Returns
    -------
    dict[str, object]
        Serialized aggregate payload.
    """
    return {
        "name": spec.name,
        "func": spec.func,
        "distinct": spec.distinct,
        "args": [arg.to_dict() for arg in spec.args],
    }


__all__ = [
    "RelAggregate",
    "RelFilter",
    "RelJoin",
    "RelNode",
    "RelPlan",
    "RelProject",
    "RelSource",
    "RelUnion",
    "rel_plan_signature",
]
