"""Expression IR helpers for spec tables."""

from __future__ import annotations

import base64
import importlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, cast, runtime_checkable

import pyarrow as pa
from ibis.expr.types import Table, Value

from arrowdsl.core.expr_types import ScalarValue
from arrowdsl.core.interop import (
    ArrayLike,
    ComputeExpression,
    ScalarLike,
    call_expression_function,
    ensure_expression,
    pc,
)
from arrowdsl.schema.build import (
    dictionary_array_from_indices,
    rows_from_table,
    union_array_from_values,
)
from arrowdsl.schema.dictionary import normalize_dictionaries
from arrowdsl.spec.codec import (
    decode_options_payload,
    decode_scalar_payload,
    decode_scalar_union,
    encode_options_payload,
    encode_scalar_payload,
    encode_scalar_union,
)
from arrowdsl.spec.scalar_union import SCALAR_UNION_TYPE
from registry_common.arrow_payloads import ipc_table, payload_ipc_bytes

if TYPE_CHECKING:
    from ibis_engine.expr_compiler import IbisExprRegistry


@runtime_checkable
class FunctionOptionsProto(Protocol):
    """Protocol for Arrow compute function options."""

    def serialize(self) -> bytes:
        """Serialize options to bytes."""
        ...

    @classmethod
    def deserialize(cls, payload: bytes) -> FunctionOptionsProto:
        """Deserialize options from bytes."""
        ...


type FunctionOptionsPayload = bytes | bytearray | FunctionOptionsProto


def _options_bytes(options: FunctionOptionsPayload | None) -> bytes | None:
    if options is None:
        return None
    if isinstance(options, bytearray):
        return bytes(options)
    if isinstance(options, bytes):
        return options
    if isinstance(options, FunctionOptionsProto):
        return options.serialize()
    msg = "ExprIR options must be FunctionOptions or serialized bytes."
    raise TypeError(msg)


def _encode_options(options: FunctionOptionsPayload | None) -> object | None:
    return encode_options_payload(_options_bytes(options))


def _decode_options(payload: object | None) -> bytes | None:
    return decode_options_payload(payload)


def _coerce_int(value: object, *, label: str) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, (str, bytes, bytearray, float)):
        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            msg = f"{label} must be an int."
            raise TypeError(msg) from exc
    msg = f"{label} must be an int."
    raise TypeError(msg)


def _deserialize_options(payload: bytes | None) -> FunctionOptionsProto | None:
    if payload is None:
        return None
    options_type = cast("type[FunctionOptionsProto] | None", getattr(pc, "FunctionOptions", None))
    if options_type is None:
        msg = "Arrow compute FunctionOptions is unavailable."
        raise TypeError(msg)
    return options_type.deserialize(payload)


def _require_expr_name(name: str | None, *, op: str) -> str:
    if name is None:
        msg = f"ExprIR {op} op requires name."
        raise ValueError(msg)
    return name


def _ensure_arg_count(name: str, args: Sequence[object], *, expected: int) -> None:
    if len(args) != expected:
        msg = f"ExprIR call op {name!r} expects {expected} arguments."
        raise ValueError(msg)


def _fill_null_expression(args: Sequence[ComputeExpression]) -> ComputeExpression:
    _ensure_arg_count("fill_null", args, expected=2)
    return ensure_expression(pc.if_else(pc.is_null(args[0]), args[1], args[0]))


def _fill_null_array(values: Sequence[ArrayLike]) -> ArrayLike:
    _ensure_arg_count("fill_null", values, expected=2)
    return pc.if_else(pc.is_null(values[0]), values[1], values[0])


def _scalar_to_array(value: ScalarLike, *, size: int) -> ArrayLike:
    if size == 0:
        return pa.array([], type=value.type)
    return pa.array([value.as_py()] * size, type=value.type)


@dataclass(frozen=True)
class ExprRegistry:
    """Registry of compute function aliases for expression compilation."""

    udfs: Mapping[str, str] = field(default_factory=dict)

    def ensure(self, name: str) -> str:
        """Return the registered name for the compute function.

        Returns
        -------
        str
            Registered function name.
        """
        return self.udfs.get(name, name)


EXPR_OP_VALUES: tuple[str, ...] = ("field", "literal", "call")
EXPR_OP_INDEX = {value: idx for idx, value in enumerate(EXPR_OP_VALUES)}
EXPR_OP_TYPE = pa.dictionary(pa.int8(), pa.string())

EXPR_NODE_SCHEMA = pa.schema(
    [
        pa.field("expr_id", pa.int64(), nullable=False),
        pa.field("op", EXPR_OP_TYPE, nullable=False),
        pa.field("name", pa.string(), nullable=True),
        pa.field("value_union", SCALAR_UNION_TYPE, nullable=True),
        pa.field("args", pa.list_(pa.int64()), nullable=True),
        pa.field("options_ipc", pa.binary(), nullable=True),
    ],
    metadata={b"spec_kind": b"expr_ir_nodes"},
)
EXPR_IR_PAYLOAD_VERSION = 1
_EXPR_IR_NODE_STRUCT = pa.struct(
    [
        pa.field("expr_id", pa.int64(), nullable=False),
        pa.field("op", pa.string(), nullable=False),
        pa.field("name", pa.string(), nullable=True),
        pa.field("value_union", SCALAR_UNION_TYPE, nullable=True),
        pa.field("args", pa.list_(pa.int64()), nullable=True),
        pa.field("options_ipc", pa.binary(), nullable=True),
    ]
)
_EXPR_IR_PAYLOAD_SCHEMA = pa.schema(
    [
        pa.field("version", pa.int32(), nullable=False),
        pa.field("root_ids", pa.list_(pa.int64()), nullable=False),
        pa.field("nodes", pa.list_(_EXPR_IR_NODE_STRUCT), nullable=False),
    ]
)


@dataclass(frozen=True)
class ExprIR:
    """Minimal IPC-serializable expression IR."""

    op: str
    name: str | None = None
    value: ScalarValue | None = None
    args: tuple[ExprIR, ...] = ()
    options: FunctionOptionsPayload | None = None

    def to_expression(self, *, registry: ExprRegistry | None = None) -> ComputeExpression:
        """Compile the IR into a compute expression.

        Returns
        -------
        ComputeExpression
            Arrow compute expression.

        Raises
        ------
        ValueError
            Raised when the IR is missing required fields or has an unsupported op.
        """
        if self.op == "field":
            if self.name is None:
                msg = "ExprIR field op requires name."
                raise ValueError(msg)
            return pc.field(self.name)
        if self.op == "literal":
            return pc.scalar(self.value)
        if self.op == "call":
            if self.name is None:
                msg = "ExprIR call op requires name."
                raise ValueError(msg)
            name = registry.ensure(self.name) if registry is not None else self.name
            args = [arg.to_expression(registry=registry) for arg in self.args]
            options = _options_bytes(self.options)
            opts = _deserialize_options(options)
            if name == "fill_null":
                return _fill_null_expression(args)
            if name == "stringify":
                _ensure_arg_count("stringify", args, expected=1)
                return ensure_expression(pc.cast(args[0], pa.string()))
            return call_expression_function(name, args, options=opts)
        msg = f"Unsupported ExprIR op: {self.op}"
        raise ValueError(msg)

    def to_ibis_expr(
        self,
        table: Table,
        *,
        registry: IbisExprRegistry | None = None,
    ) -> Value:
        """Compile the IR into an Ibis expression.

        Returns
        -------
        ibis.expr.types.Value
            Ibis expression for the provided ExprIR node.
        """
        module = importlib.import_module("ibis_engine.expr_compiler")
        registry_cls = cast("type[object]", module.IbisExprRegistry)
        expr_ir_to_ibis = cast("Callable[..., Value]", module.expr_ir_to_ibis)
        registry = registry or cast("IbisExprRegistry", registry_cls())
        return expr_ir_to_ibis(self, table, registry=registry)

    def to_json(self) -> str:
        """Serialize the IR to IPC base64 text.

        Returns
        -------
        str
            IPC base64 representation of the expression.
        """
        table_payload = expr_ir_table((self,))
        payload = {
            "version": EXPR_IR_PAYLOAD_VERSION,
            "root_ids": list(table_payload.root_ids),
            "nodes": rows_from_table(table_payload.table),
        }
        payload_bytes = payload_ipc_bytes(payload, _EXPR_IR_PAYLOAD_SCHEMA)
        return base64.b64encode(payload_bytes).decode("ascii")

    def to_dict(self) -> dict[str, Any]:
        """Return an IPC-friendly dictionary.

        Returns
        -------
        dict[str, Any]
            IR payload.
        """
        return {
            "op": self.op,
            "name": self.name,
            "value": encode_scalar_payload(self.value),
            "args": [arg.to_dict() for arg in self.args],
            "options": _encode_options(self.options),
        }

    @staticmethod
    def from_dict(payload: dict[str, Any]) -> ExprIR:
        """Build ExprIR from a dictionary.

        Returns
        -------
        ExprIR
            Parsed expression IR.

        Raises
        ------
        TypeError
            Raised when the args payload is not a list.
        """
        raw_args = payload.get("args")
        if raw_args is None:
            args: tuple[ExprIR, ...] = ()
        elif isinstance(raw_args, list):
            args = tuple(ExprIR.from_dict(arg) for arg in raw_args if arg is not None)
        else:
            msg = "ExprIR args must be a list."
            raise TypeError(msg)
        name = payload.get("name")
        name_value = str(name) if name is not None else None
        return ExprIR(
            op=str(payload.get("op", "")),
            name=name_value,
            value=decode_scalar_payload(payload.get("value")),
            args=args,
            options=_decode_options(payload.get("options")),
        )

    @staticmethod
    def from_json(text: str) -> ExprIR:
        """Parse ExprIR from IPC base64 text.

        Returns
        -------
        ExprIR
            Parsed expression IR.

        Raises
        ------
        TypeError
            Raised when the decoded payload is not a mapping.
        ValueError
            Raised when the payload version is unsupported.
        """
        raw = base64.b64decode(text.encode("ascii"))
        table = ipc_table(raw)
        rows = table.to_pylist()
        if not rows or not isinstance(rows[0], dict):
            msg = "ExprIR payload must decode to a dict."
            raise TypeError(msg)
        payload = rows[0]
        version = payload.get("version")
        if version is not None and int(version) != EXPR_IR_PAYLOAD_VERSION:
            msg = "ExprIR payload version mismatch."
            raise ValueError(msg)
        nodes = payload.get("nodes")
        if not isinstance(nodes, list):
            msg = "ExprIR payload nodes must be a list."
            raise TypeError(msg)
        root_ids = payload.get("root_ids")
        if not isinstance(root_ids, list):
            msg = "ExprIR payload root_ids must be a list."
            raise TypeError(msg)
        table = pa.Table.from_pylist(nodes, schema=EXPR_NODE_SCHEMA)
        return expr_ir_from_table(table, root_ids=root_ids)[0]


def _coerce_materialized(
    result: ArrayLike | pa.Scalar,
    *,
    table_rows: int,
    op: str,
) -> ArrayLike:
    if isinstance(result, pa.ChunkedArray):
        return cast("pa.ChunkedArray", result).combine_chunks()
    if isinstance(result, pa.Array):
        return cast("ArrayLike", result)
    if isinstance(result, pa.Scalar):
        return _scalar_to_array(cast("ScalarLike", result), size=table_rows)
    msg = f"ExprIR call op {op!r} materialization returned {type(result).__name__}."
    raise TypeError(msg)


@dataclass(frozen=True)
class ExprIRTable:
    """Arrow table representation of ExprIR graphs."""

    table: pa.Table
    root_ids: tuple[int, ...]


def _expr_op_indices(values: Sequence[str]) -> list[int]:
    indices: list[int] = []
    for value in values:
        idx = EXPR_OP_INDEX.get(value)
        if idx is None:
            msg = f"Unsupported ExprIR op: {value!r}."
            raise ValueError(msg)
        indices.append(idx)
    return indices


def expr_ir_table(exprs: Sequence[ExprIR]) -> ExprIRTable:
    """Build a table representation for expression IR entries.

    Returns
    -------
    ExprIRTable
        Table payload with root ids for the expressions.
    """
    nodes: list[ExprIR] = []
    id_map: dict[int, int] = {}

    def _ensure_id(expr: ExprIR) -> int:
        key = id(expr)
        if key in id_map:
            return id_map[key]
        expr_id = len(nodes)
        id_map[key] = expr_id
        nodes.append(expr)
        for arg in expr.args:
            _ensure_id(arg)
        return expr_id

    root_ids = tuple(_ensure_id(expr) for expr in exprs)
    expr_ids = list(range(len(nodes)))
    op_values = [expr.op for expr in nodes]
    op_indices = _expr_op_indices(op_values)
    op_array = dictionary_array_from_indices(
        op_indices,
        EXPR_OP_VALUES,
        index_type=pa.int8(),
    )
    names = [expr.name for expr in nodes]
    values = [encode_scalar_union(expr.value) for expr in nodes]
    args = [[id_map[id(arg)] for arg in expr.args] or None for expr in nodes]
    options_ipc = [_options_bytes(expr.options) for expr in nodes]
    table = pa.Table.from_arrays(
        [
            pa.array(expr_ids, type=pa.int64()),
            op_array,
            pa.array(names, type=pa.string()),
            union_array_from_values(values, union_type=SCALAR_UNION_TYPE),
            pa.array(args, type=pa.list_(pa.int64())),
            pa.array(options_ipc, type=pa.binary()),
        ],
        schema=EXPR_NODE_SCHEMA,
    )
    table = normalize_dictionaries(table, combine_chunks=False)
    return ExprIRTable(table=table, root_ids=root_ids)


def expr_ir_from_table(
    table: pa.Table,
    *,
    root_ids: Sequence[int] | None = None,
) -> tuple[ExprIR, ...]:
    """Reconstruct expression IR entries from a table.

    Returns
    -------
    tuple[ExprIR, ...]
        Reconstructed expression IR roots.
    """
    rows = rows_from_table(table)
    nodes_by_id = _expr_nodes_by_id(rows)
    root_ids = _expr_root_ids(rows, nodes_by_id, root_ids)
    return _build_expr_ir(nodes_by_id, root_ids)


def _expr_nodes_by_id(rows: Sequence[Mapping[str, object]]) -> dict[int, dict[str, Any]]:
    nodes_by_id: dict[int, dict[str, Any]] = {}
    for row in rows:
        expr_id_value = row.get("expr_id")
        if not isinstance(expr_id_value, int):
            msg = "ExprIR row missing expr_id."
            raise TypeError(msg)
        expr_id = expr_id_value
        if expr_id in nodes_by_id:
            msg = f"Duplicate expr_id in ExprIR table: {expr_id}."
            raise ValueError(msg)
        nodes_by_id[expr_id] = dict(row)
    return nodes_by_id


def _expr_root_ids(
    rows: Sequence[Mapping[str, object]],
    nodes_by_id: Mapping[int, Mapping[str, object]],
    root_ids: Sequence[int] | None,
) -> tuple[int, ...]:
    if root_ids is not None:
        return tuple(int(expr_id) for expr_id in root_ids)
    referenced: set[int] = set()
    for row in rows:
        for arg in _expr_args_payload(row.get("args")):
            referenced.add(_coerce_int(arg, label="arg"))
    return tuple(sorted(set(nodes_by_id) - referenced))


def _expr_args_payload(payload: object) -> Sequence[object]:
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        return payload
    return ()


def _build_expr_ir(
    nodes_by_id: Mapping[int, Mapping[str, object]],
    root_ids: Sequence[int],
) -> tuple[ExprIR, ...]:
    cache: dict[int, ExprIR] = {}

    def _build(expr_id: int) -> ExprIR:
        if expr_id in cache:
            return cache[expr_id]
        row = nodes_by_id.get(expr_id)
        if row is None:
            msg = f"ExprIR table missing expr_id: {expr_id}."
            raise ValueError(msg)
        args = tuple(
            _build(_coerce_int(arg, label="arg")) for arg in _expr_args_payload(row.get("args"))
        )
        name = row.get("name")
        name_value = str(name) if name is not None else None
        value = decode_scalar_union(row.get("value_union"))
        options_payload = row.get("options_ipc")
        if options_payload is None:
            options = None
        elif isinstance(options_payload, (bytes, bytearray)):
            options = _decode_options(options_payload)
        else:
            msg = "ExprIR options_ipc must be bytes."
            raise TypeError(msg)
        expr = ExprIR(
            op=str(row.get("op", "")),
            name=name_value,
            value=value,
            args=args,
            options=options,
        )
        cache[expr_id] = expr
        return expr

    return tuple(_build(expr_id) for expr_id in root_ids)


__all__ = [
    "EXPR_NODE_SCHEMA",
    "ExprIR",
    "ExprIRTable",
    "ExprRegistry",
    "expr_ir_from_table",
    "expr_ir_table",
]
