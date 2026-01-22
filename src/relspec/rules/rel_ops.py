"""Typed relational operations for centralized rule pipelines."""

from __future__ import annotations

import base64
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal, cast

import pyarrow as pa

from arrowdsl.io.ipc import ipc_hash, ipc_table, payload_ipc_bytes
from arrowdsl.spec.expr_ir import ExprIR
from ibis_engine.query_compiler import IbisProjectionSpec, IbisQuerySpec
from relspec.errors import RelspecValidationError
from relspec.model import HashJoinConfig, JoinType

REL_OP_SIGNATURE_VERSION = 1
_JOIN_CONFIG_STRUCT = pa.struct(
    [
        pa.field("join_type", pa.string(), nullable=False),
        pa.field("left_keys", pa.list_(pa.string()), nullable=False),
        pa.field("right_keys", pa.list_(pa.string()), nullable=False),
        pa.field("left_output", pa.list_(pa.string()), nullable=True),
        pa.field("right_output", pa.list_(pa.string()), nullable=True),
        pa.field("output_suffix_for_left", pa.string(), nullable=True),
        pa.field("output_suffix_for_right", pa.string(), nullable=True),
    ]
)
_JOIN_PAYLOAD_SCHEMA = pa.schema(
    [
        pa.field("left", pa.string(), nullable=False),
        pa.field("right", pa.string(), nullable=False),
        pa.field("join", _JOIN_CONFIG_STRUCT, nullable=False),
    ]
)
_AGGREGATE_EXPR_STRUCT = pa.struct(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("func", pa.string(), nullable=False),
        pa.field("distinct", pa.bool_(), nullable=False),
        pa.field("args", pa.list_(pa.string()), nullable=False),
    ]
)
_AGGREGATE_PAYLOAD_SCHEMA = pa.schema(
    [
        pa.field("group_by", pa.list_(pa.string()), nullable=False),
        pa.field("aggregates", pa.list_(_AGGREGATE_EXPR_STRUCT), nullable=False),
    ]
)
_UNION_PAYLOAD_SCHEMA = pa.schema(
    [
        pa.field("inputs", pa.list_(pa.string()), nullable=False),
        pa.field("distinct", pa.bool_(), nullable=False),
    ]
)
_PARAM_PAYLOAD_SCHEMA = pa.schema(
    [
        pa.field("name", pa.string(), nullable=False),
        pa.field("dtype", pa.string(), nullable=False),
    ]
)

RelOpKind = Literal[
    "scan",
    "project",
    "derive",
    "filter",
    "pushdown_filter",
    "join",
    "aggregate",
    "union",
    "param",
]


def _literal_none_expr() -> ExprIR:
    return ExprIR(op="literal", value=None)


def _literal_true_expr() -> ExprIR:
    return ExprIR(op="literal", value=True)


@dataclass(frozen=True)
class RelOp:
    """Base class for relational operations."""

    kind: RelOpKind


@dataclass(frozen=True)
class ScanOp(RelOp):
    """Reference a named dataset for a plan."""

    kind: Literal["scan"] = "scan"
    source: str = ""
    columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class ProjectOp(RelOp):
    """Select a subset of columns."""

    kind: Literal["project"] = "project"
    columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class DeriveOp(RelOp):
    """Add or derive a computed column."""

    kind: Literal["derive"] = "derive"
    name: str = ""
    expr: ExprIR = field(default_factory=_literal_none_expr)


@dataclass(frozen=True)
class FilterOp(RelOp):
    """Filter rows by a predicate."""

    kind: Literal["filter"] = "filter"
    predicate: ExprIR = field(default_factory=_literal_true_expr)


@dataclass(frozen=True)
class PushdownFilterOp(RelOp):
    """Filter rows using a pushdown predicate."""

    kind: Literal["pushdown_filter"] = "pushdown_filter"
    predicate: ExprIR = field(default_factory=_literal_true_expr)


@dataclass(frozen=True)
class JoinOp(RelOp):
    """Join two named inputs with a join spec."""

    kind: Literal["join"] = "join"
    left: str = ""
    right: str = ""
    join: HashJoinConfig = field(default_factory=HashJoinConfig)


@dataclass(frozen=True)
class AggregateExpr:
    """Aggregate expression specification."""

    name: str
    func: str
    args: tuple[ExprIR, ...] = ()
    distinct: bool = False


@dataclass(frozen=True)
class AggregateOp(RelOp):
    """Aggregate rows grouped by key columns."""

    kind: Literal["aggregate"] = "aggregate"
    group_by: tuple[str, ...] = ()
    aggregates: tuple[AggregateExpr, ...] = ()


@dataclass(frozen=True)
class UnionOp(RelOp):
    """Union named inputs."""

    kind: Literal["union"] = "union"
    inputs: tuple[str, ...] = ()
    distinct: bool = False


@dataclass(frozen=True)
class ParamOp(RelOp):
    """Parameter placeholder used in plans."""

    kind: Literal["param"] = "param"
    name: str = ""
    dtype: str = ""


RelOpT = (
    ScanOp
    | ProjectOp
    | DeriveOp
    | FilterOp
    | PushdownFilterOp
    | JoinOp
    | AggregateOp
    | UnionOp
    | ParamOp
)


@dataclass
class _RelOpState:
    scan_seen: bool = False
    project_seen: bool = False
    filter_seen: bool = False
    pushdown_seen: bool = False


@dataclass
class _QuerySpecState:
    base: tuple[str, ...] = ()
    derived: dict[str, ExprIR] = field(default_factory=dict)
    predicate: ExprIR | None = None
    pushdown: ExprIR | None = None


def _mark_once(*, value: bool, label: str) -> bool:
    if value:
        msg = f"Multiple {label} entries are not allowed."
        raise RelspecValidationError(msg)
    return True


def _validate_rel_op(op: RelOpT, *, idx: int, state: _RelOpState) -> None:
    if isinstance(op, ScanOp):
        if idx != 0:
            msg = "ScanOp must be the first op in the sequence."
            raise RelspecValidationError(msg)
        state.scan_seen = _mark_once(value=state.scan_seen, label="ScanOp")
        return
    if isinstance(op, ProjectOp):
        state.project_seen = _mark_once(value=state.project_seen, label="ProjectOp")
        return
    if isinstance(op, FilterOp):
        state.filter_seen = _mark_once(value=state.filter_seen, label="FilterOp")
        return
    if isinstance(op, PushdownFilterOp):
        state.pushdown_seen = _mark_once(
            value=state.pushdown_seen,
            label="PushdownFilterOp",
        )


def validate_rel_ops(ops: Sequence[RelOpT]) -> None:
    """Validate relational op sequences."""
    state = _RelOpState()
    for idx, op in enumerate(ops):
        _validate_rel_op(op, idx=idx, state=state)


def query_spec_from_rel_ops(ops: Sequence[RelOpT]) -> IbisQuerySpec | None:
    """Decode a query spec from relational ops.

    Returns
    -------
    QuerySpec | None
        Query spec built from relational ops.
    """
    if not ops:
        return None
    validate_rel_ops(ops)
    state = _QuerySpecState()
    for op in ops:
        _apply_query_op(state, op)
    if not state.base:
        state.base = tuple(state.derived)
    return IbisQuerySpec(
        projection=IbisProjectionSpec(base=state.base, derived=state.derived),
        predicate=state.predicate,
        pushdown_predicate=state.pushdown,
    )


def _apply_query_op(state: _QuerySpecState, op: RelOpT) -> None:
    if isinstance(op, ScanOp):
        if op.columns:
            state.base = op.columns
        return
    if isinstance(op, ProjectOp):
        state.base = op.columns
        return
    if isinstance(op, DeriveOp):
        state.derived[op.name] = op.expr
        return
    if isinstance(op, FilterOp):
        state.predicate = op.predicate
        return
    if isinstance(op, PushdownFilterOp):
        state.pushdown = op.predicate
        return
    if isinstance(op, ParamOp):
        return
    msg = f"Unsupported rel op for QuerySpec: {op.kind!r}."
    raise RelspecValidationError(msg)


def rel_ops_to_rows(ops: Sequence[RelOpT]) -> list[dict[str, object]] | None:
    """Encode relational ops into row dictionaries.

    Returns
    -------
    list[dict[str, object]] | None
        Encoded rows or ``None`` when no ops are provided.

    """
    if not ops:
        return None
    return [_rel_op_to_row(op) for op in ops]


def rel_ops_from_rows(rows: Sequence[Mapping[str, object]] | None) -> tuple[RelOpT, ...]:
    """Decode relational ops from row dictionaries.

    Returns
    -------
    tuple[RelOpT, ...]
        Decoded ops.

    """
    if not rows:
        return ()
    ops = tuple(_rel_op_from_row(row) for row in rows)
    validate_rel_ops(ops)
    return ops


def rel_ops_signature(ops: Sequence[RelOpT]) -> str:
    """Return a stable signature hash for relational ops.

    Returns
    -------
    str
        Stable hash signature of the relational ops.
    """
    rows = rel_ops_to_rows(ops) or []
    table = pa.Table.from_pylist([{"version": REL_OP_SIGNATURE_VERSION, "ops": rows}])
    return ipc_hash(table)


def _encode_scan(op: RelOpT) -> dict[str, object]:
    scan = cast("ScanOp", op)
    return {"name": scan.source, "columns": list(scan.columns) or None}


def _encode_project(op: RelOpT) -> dict[str, object]:
    project = cast("ProjectOp", op)
    return {"columns": list(project.columns) or None}


def _encode_derive(op: RelOpT) -> dict[str, object]:
    derive = cast("DeriveOp", op)
    return {"name": derive.name, "expr_json": derive.expr.to_json()}


def _encode_filter(op: RelOpT) -> dict[str, object]:
    filt = cast("FilterOp", op)
    return {"expr_json": filt.predicate.to_json()}


def _encode_pushdown(op: RelOpT) -> dict[str, object]:
    filt = cast("PushdownFilterOp", op)
    return {"expr_json": filt.predicate.to_json()}


def _encode_join(op: RelOpT) -> dict[str, object]:
    join = cast("JoinOp", op)
    return {
        "payload_json": _payload_ipc_text(
            {"left": join.left, "right": join.right, "join": _join_payload(join.join)},
            schema=_JOIN_PAYLOAD_SCHEMA,
        )
    }


def _encode_aggregate(op: RelOpT) -> dict[str, object]:
    agg = cast("AggregateOp", op)
    return {
        "payload_json": _payload_ipc_text(
            {
                "group_by": list(agg.group_by),
                "aggregates": [_aggregate_payload(spec) for spec in agg.aggregates],
            },
            schema=_AGGREGATE_PAYLOAD_SCHEMA,
        )
    }


def _encode_union(op: RelOpT) -> dict[str, object]:
    union = cast("UnionOp", op)
    return {
        "payload_json": _payload_ipc_text(
            {"inputs": list(union.inputs), "distinct": union.distinct},
            schema=_UNION_PAYLOAD_SCHEMA,
        )
    }


def _encode_param(op: RelOpT) -> dict[str, object]:
    param = cast("ParamOp", op)
    return {
        "payload_json": _payload_ipc_text(
            {"name": param.name, "dtype": param.dtype},
            schema=_PARAM_PAYLOAD_SCHEMA,
        )
    }


_REL_OP_ENCODERS: dict[RelOpKind, Callable[[RelOpT], dict[str, object]]] = {
    "scan": _encode_scan,
    "project": _encode_project,
    "derive": _encode_derive,
    "filter": _encode_filter,
    "pushdown_filter": _encode_pushdown,
    "join": _encode_join,
    "aggregate": _encode_aggregate,
    "union": _encode_union,
    "param": _encode_param,
}


def _rel_op_to_row(op: RelOpT) -> dict[str, object]:
    encoder = _REL_OP_ENCODERS.get(op.kind)
    if encoder is None:
        msg = f"Unsupported rel op encoding: {op!r}."
        raise TypeError(msg)
    row = encoder(op)
    row["kind"] = op.kind
    return row


def _decode_scan(row: Mapping[str, object]) -> RelOpT:
    return ScanOp(source=str(row.get("name", "")), columns=_parse_columns(row.get("columns")))


def _decode_project(row: Mapping[str, object]) -> RelOpT:
    return ProjectOp(columns=_parse_columns(row.get("columns")))


def _expr_from_row(row: Mapping[str, object], *, label: str) -> ExprIR:
    expr_json = row.get("expr_json")
    if expr_json is None:
        msg = f"{label} op requires expr_json."
        raise RelspecValidationError(msg)
    return ExprIR.from_json(str(expr_json))


def _decode_derive(row: Mapping[str, object]) -> RelOpT:
    return DeriveOp(
        name=str(row.get("name", "")),
        expr=_expr_from_row(row, label="Derive"),
    )


def _decode_filter(row: Mapping[str, object]) -> RelOpT:
    return FilterOp(predicate=_expr_from_row(row, label="Filter"))


def _decode_pushdown(row: Mapping[str, object]) -> RelOpT:
    return PushdownFilterOp(predicate=_expr_from_row(row, label="Pushdown filter"))


def _decode_join(row: Mapping[str, object]) -> RelOpT:
    payload = _payload_from_row(row, schema=_JOIN_PAYLOAD_SCHEMA)
    return JoinOp(
        left=str(payload.get("left", "")),
        right=str(payload.get("right", "")),
        join=_join_from_payload(payload.get("join")),
    )


def _decode_aggregate(row: Mapping[str, object]) -> RelOpT:
    payload = _payload_from_row(row, schema=_AGGREGATE_PAYLOAD_SCHEMA)
    return AggregateOp(
        group_by=_parse_columns(payload.get("group_by")),
        aggregates=_aggregates_from_payload(payload.get("aggregates")),
    )


def _decode_union(row: Mapping[str, object]) -> RelOpT:
    payload = _payload_from_row(row, schema=_UNION_PAYLOAD_SCHEMA)
    return UnionOp(
        inputs=_parse_columns(payload.get("inputs")),
        distinct=bool(payload.get("distinct", False)),
    )


def _decode_param(row: Mapping[str, object]) -> RelOpT:
    payload = _payload_from_row(row, schema=_PARAM_PAYLOAD_SCHEMA)
    return ParamOp(name=str(payload.get("name", "")), dtype=str(payload.get("dtype", "")))


_REL_OP_DECODERS: dict[str, Callable[[Mapping[str, object]], RelOpT]] = {
    "scan": _decode_scan,
    "project": _decode_project,
    "derive": _decode_derive,
    "filter": _decode_filter,
    "pushdown_filter": _decode_pushdown,
    "join": _decode_join,
    "aggregate": _decode_aggregate,
    "union": _decode_union,
    "param": _decode_param,
}


def _rel_op_from_row(row: Mapping[str, object]) -> RelOpT:
    kind = str(row.get("kind", ""))
    decoder = _REL_OP_DECODERS.get(kind)
    if decoder is None:
        msg = f"Unsupported rel op kind: {kind!r}."
        raise RelspecValidationError(msg)
    return decoder(row)


def _payload_ipc_text(payload: Mapping[str, object], *, schema: pa.Schema) -> str:
    payload_bytes = payload_ipc_bytes(payload, schema)
    return base64.b64encode(payload_bytes).decode("ascii")


def _payload_from_row(row: Mapping[str, object], *, schema: pa.Schema) -> dict[str, object]:
    payload_text = row.get("payload_json")
    if payload_text is None:
        return {}
    if isinstance(payload_text, str):
        raw = base64.b64decode(payload_text.encode("ascii"))
        table = ipc_table(raw)
        if not table.schema.equals(schema, check_metadata=False):
            msg = "Rel op payload schema mismatch."
            raise RelspecValidationError(msg)
        rows = table.to_pylist()
        if not rows:
            return {}
        payload = rows[0]
        if not isinstance(payload, Mapping):
            msg = "Rel op payload_json must decode to a mapping."
            raise TypeError(msg)
        return dict(payload)
    msg = "Rel op payload_json must be a string."
    raise TypeError(msg)


def _parse_columns(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(str(item) for item in value)
    msg = "Expected list of column names."
    raise TypeError(msg)


def _parse_optional_columns(value: object | None) -> tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(str(item) for item in value)
    msg = "Expected list of column names."
    raise TypeError(msg)


def _parse_join_type(value: object | None) -> JoinType:
    join_map: dict[str, JoinType] = {
        "inner": "inner",
        "left outer": "left outer",
        "right outer": "right outer",
        "full outer": "full outer",
        "left semi": "left semi",
        "right semi": "right semi",
        "left anti": "left anti",
        "right anti": "right anti",
    }
    normalized = "inner" if value is None else str(value)
    if normalized in join_map:
        return join_map[normalized]
    msg = f"Unsupported join type: {normalized!r}."
    raise RelspecValidationError(msg)


def _join_payload(spec: HashJoinConfig) -> dict[str, object]:
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


def _join_from_payload(payload: object | None) -> HashJoinConfig:
    if not isinstance(payload, Mapping):
        return HashJoinConfig()
    join_type = _parse_join_type(payload.get("join_type"))
    return HashJoinConfig(
        join_type=join_type,
        left_keys=_parse_columns(payload.get("left_keys")),
        right_keys=_parse_columns(payload.get("right_keys")),
        left_output=_parse_optional_columns(payload.get("left_output")),
        right_output=_parse_optional_columns(payload.get("right_output")),
        output_suffix_for_left=str(payload.get("output_suffix_for_left", "")),
        output_suffix_for_right=str(payload.get("output_suffix_for_right", "")),
    )


def _aggregate_payload(spec: AggregateExpr) -> dict[str, object]:
    return {
        "name": spec.name,
        "func": spec.func,
        "distinct": spec.distinct,
        "args": [arg.to_json() for arg in spec.args],
    }


def _aggregates_from_payload(payload: object | None) -> tuple[AggregateExpr, ...]:
    if payload is None:
        return ()
    if not isinstance(payload, Sequence):
        msg = "Aggregate payload must be a list."
        raise TypeError(msg)
    specs: list[AggregateExpr] = []
    for item in payload:
        if not isinstance(item, Mapping):
            msg = "Aggregate payload entries must be mappings."
            raise TypeError(msg)
        args_payload = item.get("args") or ()
        if not isinstance(args_payload, Sequence):
            msg = "Aggregate args must be a list."
            raise TypeError(msg)
        specs.append(
            AggregateExpr(
                name=str(item.get("name", "")),
                func=str(item.get("func", "")),
                distinct=bool(item.get("distinct", False)),
                args=tuple(ExprIR.from_json(str(arg)) for arg in args_payload if arg is not None),
            )
        )
    return tuple(specs)


__all__ = [
    "AggregateExpr",
    "AggregateOp",
    "DeriveOp",
    "FilterOp",
    "JoinOp",
    "ParamOp",
    "ProjectOp",
    "PushdownFilterOp",
    "RelOp",
    "RelOpKind",
    "RelOpT",
    "ScanOp",
    "UnionOp",
    "query_spec_from_rel_ops",
    "rel_ops_from_rows",
    "rel_ops_signature",
    "rel_ops_to_rows",
    "validate_rel_ops",
]
