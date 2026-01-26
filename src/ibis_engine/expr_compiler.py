"""ExprIR to Ibis expression compiler."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Protocol, cast

import ibis
import ibis.expr.datashape as ds
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
from ibis.expr.types import (
    ArrayValue,
    BooleanValue,
    NumericValue,
    Scalar,
    StringValue,
    Table,
    Value,
)

from ibis_engine.builtin_udfs import ibis_udf_registry

IbisExprFn = Callable[..., Value]
PORTABILITY_GATE_OPS: tuple[type[ops.Node], ...] = (
    ops.TableUnnest,
    ops.Unnest,
    ops.WindowFunction,
)
SUPPORTED_JOIN_KINDS: tuple[str, ...] = (
    "inner",
    "left",
    "right",
    "outer",
    "cross",
)


def _fill_null_expr(value: Value, fill_value: Scalar) -> Value:
    return value.fill_null(fill_value)


def _if_else_expr(cond: Value, true_value: Value, false_value: Value) -> Value:
    return ibis.ifelse(cond, true_value, false_value)


def _equal_expr(left: Value, right: Value) -> Value:
    return left == right


def _not_equal_expr(left: Value, right: Value) -> Value:
    return left != right


def _invert_expr(value: BooleanValue) -> BooleanValue:
    return ~value


def _bitwise_and_expr(left: BooleanValue, right: BooleanValue) -> BooleanValue:
    return left & right


def _bitwise_or_expr(left: BooleanValue, right: BooleanValue) -> BooleanValue:
    return left | right


def _add_expr(left: NumericValue, right: NumericValue) -> NumericValue:
    return left + right


def _subtract_expr(left: NumericValue, right: NumericValue) -> NumericValue:
    return left - right


def _concat_expr(*values: Value) -> Value:
    if not values:
        msg = "concat requires at least one input."
        raise ValueError(msg)
    if all(value.type().is_string() for value in values):
        result = cast("StringValue", values[0])
        for part in values[1:]:
            result = result.concat(cast("StringValue", part))
        return result
    if all(value.type().is_array() for value in values):
        result = cast("ArrayValue", values[0])
        for part in values[1:]:
            result = result.concat(cast("ArrayValue", part))
        return result
    msg = "concat requires all string or all array inputs."
    raise ValueError(msg)


def _strip_expr(value: StringValue) -> StringValue:
    return value.strip()


def _stringify_expr(value: Value) -> StringValue:
    return value.cast("string")


def _is_null_expr(value: Value) -> BooleanValue:
    return value.isnull()


def _coalesce_expr(*values: Value) -> Value:
    if not values:
        msg = "coalesce requires at least one input."
        raise ValueError(msg)
    result = values[0]
    for value in values[1:]:
        result = ibis.ifelse(result.isnull(), value, result)
    return result


def _starts_with_expr(value: StringValue, prefix: Value) -> BooleanValue:
    return value.startswith(cast("StringValue | str", prefix))


def _in_set_expr(value: Value, *items: Value) -> BooleanValue:
    if not items:
        return cast("BooleanValue", ibis.literal(value=False))
    return value.isin(list(items))


def _binary_join_element_wise_expr(*values: Value) -> Value:
    min_inputs = 2
    if len(values) < min_inputs:
        msg = "binary_join_element_wise requires at least two inputs."
        raise ValueError(msg)
    *parts, sep = values
    separator = cast("ops.Value[dt.String, ds.DataShape]", sep.op())
    resolved_parts = tuple(cast("ops.Value[dt.String, ds.DataShape]", part.op()) for part in parts)
    joined = ops.StringJoin(resolved_parts, separator)
    return cast("StringValue", joined.to_expr())


class OperationSupportBackend(Protocol):
    """Protocol for backends exposing operation support checks."""

    def has_operation(self, operation: type[ops.Node], /) -> bool:
        """Return whether the backend supports an operation type."""
        ...


class ExprIRLike(Protocol):
    """Protocol for ExprIR-compatible nodes."""

    @property
    def op(self) -> str:
        """Return the operation name."""
        ...

    @property
    def name(self) -> str | None:
        """Return the operation name or field identifier."""
        ...

    @property
    def value(self) -> object | None:
        """Return the literal value when present."""
        ...

    @property
    def args(self) -> Sequence[ExprIRLike]:
        """Return child expression nodes."""
        ...

    @property
    def options(self) -> object | None:
        """Return optional call options payload."""
        ...


@dataclass(frozen=True)
class IbisExprRegistry:
    """Registry for mapping ExprIR call names to Ibis functions."""

    functions: Mapping[str, IbisExprFn] = field(default_factory=dict)

    def resolve(self, name: str) -> IbisExprFn:
        """Return the Ibis function for a given call name.

        Returns
        -------
        IbisExprFn
            Callable that produces an Ibis expression.

        Raises
        ------
        KeyError
            Raised when the function name cannot be resolved.
        """
        fn = self.functions.get(name)
        if fn is not None:
            return fn
        msg = f"Unsupported Ibis function: {name!r}."
        raise KeyError(msg)


def default_expr_registry() -> IbisExprRegistry:
    """Return a registry with project-specific helpers registered.

    Returns
    -------
    IbisExprRegistry
        Registry with default helper mappings.
    """
    functions: dict[str, IbisExprFn] = {
        "add": _add_expr,
        "array": ibis.array,
        "binary_join_element_wise": _binary_join_element_wise_expr,
        "bit_wise_or": _bitwise_or_expr,
        "coalesce": _coalesce_expr,
        "fill_null": _fill_null_expr,
        "concat": _concat_expr,
        "if_else": _if_else_expr,
        "in_set": _in_set_expr,
        "equal": _equal_expr,
        "map": ibis.map,
        "not_equal": _not_equal_expr,
        "invert": _invert_expr,
        "bit_wise_and": _bitwise_and_expr,
        "strip": _strip_expr,
        "struct": ibis.struct,
        "starts_with": _starts_with_expr,
        "subtract": _subtract_expr,
        "stringify": _stringify_expr,
        "is_null": _is_null_expr,
        "utf8_trim_whitespace": _strip_expr,
    }
    functions.update(ibis_udf_registry())
    return IbisExprRegistry(functions=functions)


def expr_ir_to_ibis(
    expr: ExprIRLike,
    table: Table,
    *,
    registry: IbisExprRegistry | None = None,
) -> Value:
    """Compile an ExprIR-like node into an Ibis expression.

    Returns
    -------
    ibis.expr.types.Value
        Ibis expression for the provided ExprIR node.

    Raises
    ------
    ValueError
        Raised when the node cannot be compiled.
    """
    registry = registry or default_expr_registry()
    if expr.op == "field":
        if expr.name is None:
            msg = "ExprIR field op requires name."
            raise ValueError(msg)
        return table[expr.name]
    if expr.op == "literal":
        return ibis.literal(expr.value)
    if expr.op == "call":
        if expr.name is None:
            msg = "ExprIR call op requires name."
            raise ValueError(msg)
        if expr.options is not None:
            msg = "ExprIR options are not supported in the Ibis compiler."
            raise ValueError(msg)
        fn = registry.resolve(expr.name)
        args = [expr_ir_to_ibis(arg, table, registry=registry) for arg in expr.args]
        return fn(*args)
    msg = f"Unsupported ExprIR op: {expr.op!r}."
    raise ValueError(msg)


def unsupported_operations(
    expr: Value | Table,
    *,
    backend: OperationSupportBackend,
) -> tuple[str, ...]:
    """Return unsupported portability-gated operations for a backend.

    Returns
    -------
    tuple[str, ...]
        Sorted operation class names not supported by the backend.
    """
    missing: set[str] = set()
    try:
        nodes = list(expr.op().find(ops.Node))
    except AttributeError:
        return ()
    has_op = getattr(backend, "has_operation", None)
    if not callable(has_op):
        return _missing_portability_ops(nodes)
    for node in nodes:
        op_type = type(node)
        if op_type not in PORTABILITY_GATE_OPS:
            continue
        try:
            supported = has_op(op_type)
        except NotImplementedError:
            missing.add(op_type.__name__)
            continue
        except (AttributeError, RuntimeError, TypeError, ValueError):
            return _missing_portability_ops(nodes)
        if not supported:
            missing.add(op_type.__name__)
    return tuple(sorted(missing))


def _missing_portability_ops(nodes: Sequence[object]) -> tuple[str, ...]:
    missing = {type(node).__name__ for node in nodes if type(node) in PORTABILITY_GATE_OPS}
    return tuple(sorted(missing))


def unsupported_join_kinds(expr: Value | Table) -> tuple[str, ...]:
    """Return unsupported join kinds detected in an expression.

    Returns
    -------
    tuple[str, ...]
        Sorted join kind labels that require fallback handling.
    """
    missing: set[str] = set()
    try:
        nodes = list(expr.op().find(ops.JoinChain))
    except AttributeError:
        return ()
    for chain in nodes:
        rest = getattr(chain, "rest", ())
        if not isinstance(rest, Sequence):
            continue
        for link in rest:
            how = getattr(link, "how", None)
            if how is None:
                continue
            how_label = str(how)
            if how_label not in SUPPORTED_JOIN_KINDS:
                missing.add(f"join:{how_label}")
    return tuple(sorted(missing))


@dataclass(frozen=True)
class PortabilityFallbackResult:
    """Result of portability preflight and fallback rewrites."""

    expr: Table
    missing_ops: tuple[str, ...]
    fallback_reason: str | None = None


def preflight_portability(
    expr: Table,
    *,
    backend: OperationSupportBackend,
    dialect: str | None = None,
) -> PortabilityFallbackResult:
    """Apply preflight support checks and optional fallbacks.

    Returns
    -------
    PortabilityFallbackResult
        Result with missing ops metadata and any fallback expression.
    """
    _ = dialect
    missing_ops = unsupported_operations(expr, backend=backend)
    missing_joins = unsupported_join_kinds(expr)
    missing = tuple(sorted({*missing_ops, *missing_joins}))
    if not missing:
        return PortabilityFallbackResult(expr=expr, missing_ops=())
    return PortabilityFallbackResult(
        expr=expr,
        missing_ops=missing,
        fallback_reason="unsupported_ops",
    )


def align_set_op_tables(tables: Sequence[Table]) -> list[Table]:
    """Align tables to a shared schema for set operations.

    Returns
    -------
    list[ibis.expr.types.Table]
        Tables aligned to a compatible schema order.

    Raises
    ------
    ValueError
        Raised when no tables are provided or column types conflict.
    """
    if not tables:
        msg = "Set operations require at least one table."
        raise ValueError(msg)
    names: list[str] = []
    types: dict[str, dt.DataType] = {}
    for table in tables:
        schema = table.schema()
        schema_names = cast("Sequence[str]", schema.names)
        schema_types = cast("Sequence[dt.DataType]", schema.types)
        for name, dtype in zip(schema_names, schema_types, strict=True):
            if name in types:
                if types[name] != dtype:
                    msg = f"Set operation type mismatch for column: {name}."
                    raise ValueError(msg)
                continue
            names.append(name)
            types[name] = dtype
    aligned: list[Table] = []
    for table in tables:
        cols: list[Value] = []
        for name in names:
            if name in table.columns:
                cols.append(table[name])
            else:
                cols.append(ibis.literal(None, type=types[name]).name(name))
        aligned.append(table.select(cols))
    return aligned


def union_tables(tables: Sequence[Table], *, distinct: bool = False) -> Table:
    """Union tables with explicit distinct semantics.

    Returns
    -------
    ibis.expr.types.Table
        Unioned table expression.
    """
    aligned = align_set_op_tables(tables)
    head, *rest = aligned
    return head.union(*rest, distinct=distinct)


def intersect_tables(tables: Sequence[Table], *, distinct: bool = True) -> Table:
    """Intersect tables with explicit distinct semantics.

    Returns
    -------
    ibis.expr.types.Table
        Intersected table expression.
    """
    aligned = align_set_op_tables(tables)
    head, *rest = aligned
    return head.intersect(*rest, distinct=distinct)


def difference_tables(tables: Sequence[Table], *, distinct: bool = True) -> Table:
    """Difference tables with explicit distinct semantics.

    Returns
    -------
    ibis.expr.types.Table
        Difference table expression.
    """
    aligned = align_set_op_tables(tables)
    head, *rest = aligned
    return head.difference(*rest, distinct=distinct)


__all__ = [
    "ExprIRLike",
    "IbisExprFn",
    "IbisExprRegistry",
    "OperationSupportBackend",
    "PortabilityFallbackResult",
    "align_set_op_tables",
    "default_expr_registry",
    "difference_tables",
    "expr_ir_to_ibis",
    "intersect_tables",
    "preflight_portability",
    "union_tables",
    "unsupported_join_kinds",
    "unsupported_operations",
]
