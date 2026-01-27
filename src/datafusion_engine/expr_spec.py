"""DataFusion-backed expression specs for derived fields and predicates."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from arrowdsl.core.expr_types import ScalarValue
from arrowdsl.core.interop import ScalarLike

if TYPE_CHECKING:
    from datafusion.expr import Expr

_EXACT_ONE = 1
_EXACT_TWO = 2
_EXACT_THREE = 3
_MIN_IN_SET = 2
_MIN_BINARY_JOIN = 3


def _sql_literal(value: ScalarValue) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    if isinstance(value, bytes):
        text = value.decode("utf-8", errors="replace")
        return _sql_literal(text)
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, ScalarLike):
        resolved = value.as_py()
        if isinstance(resolved, (bool, int, float, str, bytes)) or resolved is None:
            return _sql_literal(resolved)
        msg = f"Unsupported literal value: {type(resolved).__name__}."
        raise TypeError(msg)
    msg = f"Unsupported literal value: {type(value).__name__}."
    raise TypeError(msg)


def _sql_identifier(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


@dataclass(frozen=True)
class ExprIR:
    """Minimal expression spec for DataFusion SQL emission."""

    op: str
    name: str | None = None
    value: ScalarValue | None = None
    args: tuple[ExprIR, ...] = ()

    def to_sql(self) -> str:
        """Return DataFusion SQL expression text for this spec.

        Returns
        -------
        str
            DataFusion SQL expression text.

        Raises
        ------
        ValueError
            Raised when the expression is missing required fields.
        """
        if self.op == "field":
            if self.name is None:
                msg = "ExprIR field op requires name."
                raise ValueError(msg)
            return _sql_identifier(self.name)
        if self.op == "literal":
            return _sql_literal(self.value)
        if self.op == "call":
            if self.name is None:
                msg = "ExprIR call op requires name."
                raise ValueError(msg)
            return _render_call(self.name, self.args)
        msg = f"Unsupported ExprIR op: {self.op!r}."
        raise ValueError(msg)

    def to_expr(self) -> Expr:
        """Return a DataFusion expression for this spec.

        Returns
        -------
        Expr
            DataFusion expression for the expression spec.

        Raises
        ------
        ValueError
            Raised when the expression is missing required fields.
        """
        if self.op == "field":
            if self.name is None:
                msg = "ExprIR field op requires name."
                raise ValueError(msg)
            from datafusion import col

            return col(self.name)
        if self.op == "literal":
            from datafusion import lit

            return lit(self.value)
        if self.op != "call":
            msg = f"Unsupported ExprIR op: {self.op!r}."
            raise ValueError(msg)
        if self.name is None:
            msg = "ExprIR call op requires name."
            raise ValueError(msg)
        args = [arg.to_expr() for arg in self.args]
        return _call_expr(self.name, args=args, ir_args=self.args)


def _render_call(name: str, args: Sequence[ExprIR]) -> str:
    rendered = [arg.to_sql() for arg in args]
    _validate_call_name(name, rendered)
    renderer = _SQL_CALLS.get(name)
    if renderer is not None:
        return renderer(rendered)
    return _default_sql_call(name, rendered)


def _default_sql_call(name: str, rendered: Sequence[str]) -> str:
    return f"{name}({', '.join(rendered)})"


def _sql_call_binary_join(rendered: Sequence[str], *, separator_idx: int) -> str:
    separator = rendered[separator_idx]
    values = rendered[:separator_idx]
    return f"concat_ws({separator}, {', '.join(values)})"


def _sql_call_bitwise(rendered: Sequence[str], *, operator: str) -> str:
    return f" {operator} ".join(f"({item})" for item in rendered)


def _validate_call_name(name: str, args: Sequence[object]) -> None:
    exact = _EXACT_CALL_COUNTS.get(name)
    if exact is not None and len(args) != exact:
        msg = f"{name} expects exactly {exact} arguments."
        raise ValueError(msg)
    minimum = _MIN_CALL_COUNTS.get(name)
    if minimum is not None and len(args) < minimum:
        msg = f"{name} expects at least {minimum} arguments."
        raise ValueError(msg)


def _literal_prefix(args: Sequence[ExprIR], *, name: str) -> str:
    prefix = args[0].value
    if not isinstance(prefix, str):
        msg = f"{name} expects a literal string prefix."
        raise TypeError(msg)
    return prefix


def _literal_string_arg(args: Sequence[ExprIR], *, name: str, index: int) -> str:
    if not args:
        msg = f"{name} expects a literal string argument."
        raise ValueError(msg)
    literal = args[index].value
    if not isinstance(literal, str):
        msg = f"{name} expects a literal string argument."
        raise TypeError(msg)
    return literal


def _call_expr(name: str, *, args: Sequence[Expr], ir_args: Sequence[ExprIR]) -> Expr:
    _validate_call_name(name, args)
    handler = _EXPR_CALLS.get(name)
    if handler is None:
        msg = f"Unsupported ExprIR call: {name!r}."
        raise ValueError(msg)
    return handler(args, ir_args)


def _bitwise_fold(args: Sequence[Expr], *, op: str) -> Expr:
    combined = args[0]
    if op == "or":
        for expr in args[1:]:
            combined |= expr
        return combined
    for expr in args[1:]:
        combined &= expr
    return combined


def _expr_stringify(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion import functions as f
    from datafusion import lit

    return f.arrow_cast(args[0], lit("Utf8"))


def _expr_trim(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion import functions as f

    return f.trim(args[0])


def _expr_coalesce(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion import functions as f

    return f.coalesce(*args)


def _expr_bitwise_or(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    return _bitwise_fold(args, op="or")


def _expr_bitwise_and(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    return _bitwise_fold(args, op="and")


def _expr_equal(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    return args[0] == args[1]


def _expr_starts_with(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion import functions as f

    return f.starts_with(args[0], args[1])


def _expr_if_else(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion import functions as f

    return f.when(args[0], args[1]).otherwise(args[2])


def _expr_in_set(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion import functions as f

    return f.in_list(args[0], list(args[1:]))


def _expr_invert(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    return ~args[0]


def _expr_is_null(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    return args[0].is_null()


def _expr_binary_join(args: Sequence[Expr], ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion import functions as f

    separator = _literal_string_arg(ir_args, name="binary_join_element_wise", index=-1)
    return f.concat_ws(separator, *args[:-1])


def _expr_stable_id(args: Sequence[Expr], ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import stable_id

    prefix = _literal_prefix(ir_args, name="stable_id")
    return stable_id(prefix, args[1])


def _expr_stable_hash64(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import stable_hash64

    return stable_hash64(args[0])


def _expr_stable_hash128(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import stable_hash128

    return stable_hash128(args[0])


def _expr_prefixed_hash64(args: Sequence[Expr], ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import prefixed_hash64

    prefix = _literal_prefix(ir_args, name="prefixed_hash64")
    return prefixed_hash64(prefix, args[1])


_EXACT_CALL_COUNTS: dict[str, int] = {
    "stringify": _EXACT_ONE,
    "utf8_trim_whitespace": _EXACT_ONE,
    "equal": _EXACT_TWO,
    "starts_with": _EXACT_TWO,
    "if_else": _EXACT_THREE,
    "invert": _EXACT_ONE,
    "is_null": _EXACT_ONE,
    "stable_id": _EXACT_TWO,
    "stable_hash64": _EXACT_ONE,
    "stable_hash128": _EXACT_ONE,
    "prefixed_hash64": _EXACT_TWO,
}
_MIN_CALL_COUNTS: dict[str, int] = {
    "bit_wise_or": _EXACT_ONE,
    "bit_wise_and": _EXACT_ONE,
    "in_set": _MIN_IN_SET,
    "binary_join_element_wise": _MIN_BINARY_JOIN,
}
_EXPR_CALLS: dict[str, Callable[[Sequence[Expr], Sequence[ExprIR]], Expr]] = {
    "stringify": _expr_stringify,
    "utf8_trim_whitespace": _expr_trim,
    "coalesce": _expr_coalesce,
    "bit_wise_or": _expr_bitwise_or,
    "bit_wise_and": _expr_bitwise_and,
    "equal": _expr_equal,
    "starts_with": _expr_starts_with,
    "if_else": _expr_if_else,
    "in_set": _expr_in_set,
    "invert": _expr_invert,
    "is_null": _expr_is_null,
    "binary_join_element_wise": _expr_binary_join,
    "stable_id": _expr_stable_id,
    "stable_hash64": _expr_stable_hash64,
    "stable_hash128": _expr_stable_hash128,
    "prefixed_hash64": _expr_prefixed_hash64,
}
_SQL_CALLS: dict[str, Callable[[Sequence[str]], str]] = {
    "stringify": lambda rendered: f"CAST({rendered[0]} AS STRING)",
    "utf8_trim_whitespace": lambda rendered: f"TRIM({rendered[0]})",
    "coalesce": lambda rendered: f"COALESCE({', '.join(rendered)})",
    "bit_wise_or": lambda rendered: _sql_call_bitwise(rendered, operator="OR"),
    "bit_wise_and": lambda rendered: _sql_call_bitwise(rendered, operator="AND"),
    "equal": lambda rendered: f"({rendered[0]} = {rendered[1]})",
    "starts_with": lambda rendered: f"starts_with({rendered[0]}, {rendered[1]})",
    "if_else": lambda rendered: (
        f"(CASE WHEN {rendered[0]} THEN {rendered[1]} ELSE {rendered[2]} END)"
    ),
    "in_set": lambda rendered: f"({rendered[0]} IN ({', '.join(rendered[1:])}))",
    "invert": lambda rendered: f"(NOT {rendered[0]})",
    "is_null": lambda rendered: f"({rendered[0]} IS NULL)",
    "binary_join_element_wise": lambda rendered: _sql_call_binary_join(
        rendered,
        separator_idx=-1,
    ),
}


@dataclass(frozen=True)
class ExprSpec:
    """DataFusion SQL expression specification."""

    sql: str | None = None
    expr_ir: ExprIR | None = None

    def __post_init__(self) -> None:
        """Populate SQL from ExprIR when needed.

        Raises
        ------
        ValueError
            Raised when neither SQL nor ExprIR is provided.
        """
        if self.sql is None and self.expr_ir is None:
            msg = "ExprSpec requires sql or expr_ir."
            raise ValueError(msg)
        if self.sql is None and self.expr_ir is not None:
            object.__setattr__(self, "sql", self.expr_ir.to_sql())

    def to_sql(self) -> str:
        """Return SQL expression text.

        Returns
        -------
        str
            DataFusion SQL expression string.

        Raises
        ------
        ValueError
            Raised when the SQL expression is unavailable.
        """
        if self.sql is None:
            msg = "ExprSpec missing SQL expression."
            raise ValueError(msg)
        return self.sql

    def to_expr(self) -> Expr:
        """Return a DataFusion expression for this spec.

        Returns
        -------
        Expr
            DataFusion expression resolved from the spec.

        Raises
        ------
        ValueError
            Raised when the expression spec cannot be resolved.
        """
        if self.expr_ir is not None:
            return self.expr_ir.to_expr()
        msg = "ExprSpec missing ExprIR; SQL-only expressions are not supported."
        raise ValueError(msg)


__all__ = ["ExprIR", "ExprSpec"]
