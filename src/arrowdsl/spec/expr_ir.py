"""Expression IR helpers for spec tables."""

from __future__ import annotations

import importlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

import pyarrow as pa
from ibis.expr.types import Table, Value

from arrowdsl.compute.expr_core import ComputeExprSpec, ExprSpec, ScalarValue
from arrowdsl.compute.macros import ConstExpr, FieldExpr
from arrowdsl.compute.options import (
    FunctionOptionsPayload,
    FunctionOptionsProto,
    deserialize_options,
    serialize_options,
)
from arrowdsl.compute.registry import ComputeRegistry, UdfSpec, default_registry
from arrowdsl.core.interop import (
    ArrayLike,
    ChunkedArrayLike,
    ComputeExpression,
    TableLike,
    ensure_expression,
    pc,
)
from arrowdsl.ir.expr import expr_from_expr_ir
from arrowdsl.json_factory import JsonPolicy, dumps_text
from arrowdsl.schema.build import dictionary_array_from_indices, union_array_from_values
from arrowdsl.schema.dictionary import normalize_dictionaries
from arrowdsl.spec.codec import (
    decode_json_text,
    decode_options_payload,
    decode_scalar_payload,
    decode_scalar_union,
    encode_json_text,
    encode_options_payload,
    encode_scalar_payload,
    encode_scalar_union,
)
from arrowdsl.spec.infra import SCALAR_UNION_TYPE

if TYPE_CHECKING:
    from ibis_engine.expr_compiler import IbisExprRegistry


def _options_bytes(options: FunctionOptionsPayload | None) -> bytes | None:
    try:
        return serialize_options(options)
    except TypeError as exc:
        msg = "ExprIR options must be FunctionOptions or serialized bytes."
        raise TypeError(msg) from exc


def _encode_options(options: FunctionOptionsPayload | None) -> object | None:
    return encode_options_payload(_options_bytes(options))


def _decode_options(payload: object | None) -> bytes | None:
    return decode_options_payload(payload)


def _encode_options_text(options: FunctionOptionsPayload | None) -> str | None:
    return encode_json_text(_options_bytes(options))


def _decode_options_text(payload: str | None) -> bytes | None:
    decoded = decode_json_text(payload)
    if decoded is None:
        return None
    if isinstance(decoded, bytearray):
        return bytes(decoded)
    if isinstance(decoded, bytes):
        return decoded
    msg = "ExprIR options JSON must decode to bytes."
    raise TypeError(msg)


def _deserialize_options(payload: bytes | None) -> FunctionOptionsProto | None:
    return deserialize_options(payload)


@dataclass(frozen=True)
class ExprRegistry:
    """Registry of compute UDFs for expression compilation."""

    compute_registry: ComputeRegistry = field(default_factory=default_registry)
    udfs: Mapping[str, UdfSpec] = field(default_factory=dict)

    def ensure(self, name: str) -> str:
        """Ensure the compute function is registered.

        Returns
        -------
        str
            Registered function name.
        """
        spec = self.udfs.get(name)
        if spec is None:
            return name
        return self.compute_registry.ensure(spec)


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
        pa.field("options_json", pa.string(), nullable=True),
    ],
    metadata={b"spec_kind": b"expr_ir_nodes"},
)


@dataclass(frozen=True)
class ExprIR:
    """Minimal JSON-serializable expression IR."""

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
            return ensure_expression(pc.call_function(name, args, options=opts))
        msg = f"Unsupported ExprIR op: {self.op}"
        raise ValueError(msg)

    def to_expr_spec(self, *, registry: ExprRegistry | None = None) -> ExprSpec:
        """Compile the IR into an ExprSpec.

        Returns
        -------
        ExprSpec
            Expression spec wrapper.

        Raises
        ------
        ValueError
            Raised when the IR is missing required fields or has an unsupported op.
        """
        if self.op == "field":
            if self.name is None:
                msg = "ExprIR field op requires name."
                raise ValueError(msg)
            return FieldExpr(name=self.name)
        if self.op == "literal":
            return ConstExpr(value=self.value)
        if self.op == "call":
            if self.name is None:
                msg = "ExprIR call op requires name."
                raise ValueError(msg)
            args = tuple(arg.to_expr_spec(registry=registry) for arg in self.args)
            name = registry.ensure(self.name) if registry is not None else self.name
            options = _options_bytes(self.options)
            opts = _deserialize_options(options)
            node = expr_from_expr_ir(self)

            def _materialize(table: TableLike) -> ArrayLike:
                values = [arg.materialize(table) for arg in args]
                result = pc.call_function(name, values, options=opts)
                if isinstance(result, ChunkedArrayLike):
                    return result.combine_chunks()
                if isinstance(result, ArrayLike):
                    return result
                msg = "ExprIR call op materialization returned non-array output."
                raise TypeError(msg)

            return ComputeExprSpec(expr=node, materialize_fn=_materialize, registry=registry)
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
        """Serialize the IR to JSON.

        Returns
        -------
        str
            JSON representation of the expression.
        """
        payload = self.to_dict()
        policy = JsonPolicy(ascii_only=True)
        return dumps_text(payload, policy=policy)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dictionary.

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
        """Parse ExprIR from JSON.

        Returns
        -------
        ExprIR
            Parsed expression IR.

        Raises
        ------
        TypeError
            Raised when the decoded JSON is not a mapping.
        """
        payload = json.loads(text)
        if not isinstance(payload, dict):
            msg = "ExprIR JSON must decode to a dict."
            raise TypeError(msg)
        return ExprIR.from_dict(payload)


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
    options_json = [_encode_options_text(expr.options) for expr in nodes]
    table = pa.Table.from_arrays(
        [
            pa.array(expr_ids, type=pa.int64()),
            op_array,
            pa.array(names, type=pa.string()),
            union_array_from_values(values, union_type=SCALAR_UNION_TYPE),
            pa.array(args, type=pa.list_(pa.int64())),
            pa.array(options_json, type=pa.string()),
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

    Raises
    ------
    ValueError
        Raised when duplicate expr_id entries are found.
    """
    rows = table.to_pylist()
    nodes_by_id: dict[int, dict[str, Any]] = {}
    for row in rows:
        expr_id = int(row["expr_id"])
        if expr_id in nodes_by_id:
            msg = f"Duplicate expr_id in ExprIR table: {expr_id}."
            raise ValueError(msg)
        nodes_by_id[expr_id] = row

    if root_ids is None:
        referenced: set[int] = set()
        for row in rows:
            for arg in row.get("args") or ():
                referenced.add(int(arg))
        root_ids = tuple(sorted(set(nodes_by_id) - referenced))
    else:
        root_ids = tuple(int(expr_id) for expr_id in root_ids)

    cache: dict[int, ExprIR] = {}

    def _build(expr_id: int) -> ExprIR:
        if expr_id in cache:
            return cache[expr_id]
        row = nodes_by_id.get(expr_id)
        if row is None:
            msg = f"ExprIR table missing expr_id: {expr_id}."
            raise ValueError(msg)
        args = tuple(_build(int(arg)) for arg in row.get("args") or ())
        name = row.get("name")
        name_value = str(name) if name is not None else None
        value = decode_scalar_union(row.get("value_union"))
        options = _decode_options_text(row.get("options_json"))
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


def expr_spec_from_json(text: str, *, registry: ExprRegistry | None = None) -> ExprSpec:
    """Compile ExprSpec from JSON IR.

    Returns
    -------
    ExprSpec
        Compiled expression spec.
    """
    return ExprIR.from_json(text).to_expr_spec(registry=registry)


__all__ = [
    "EXPR_NODE_SCHEMA",
    "ExprIR",
    "ExprIRTable",
    "ExprRegistry",
    "expr_ir_from_table",
    "expr_ir_table",
    "expr_spec_from_json",
]
