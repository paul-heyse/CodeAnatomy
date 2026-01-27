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
_EXACT_FOUR = 4
_MIN_IN_SET = 2
_MIN_BINARY_JOIN = 3
_MAX_STABLE_ID_PART_ARGS = 65
_MAX_SPAN_MAKE_ARGS = 5
_SPAN_ID_KIND_ARG_COUNT = 5
_MIN_STABLE_HASH_ANY_CANONICAL_ARGS = 2
_MIN_STABLE_HASH_ANY_NULL_SENTINEL_ARGS = 3
_MIN_UTF8_NORMALIZE_FORM_ARGS = 2
_MIN_UTF8_NORMALIZE_CASEFOLD_ARGS = 3
_MIN_UTF8_NORMALIZE_COLLAPSE_ARGS = 4
_MIN_QNAME_LANG_ARGS = 3
_MIN_MAP_NORMALIZE_KEY_CASE_ARGS = 2
_MIN_MAP_NORMALIZE_SORT_KEYS_ARGS = 3
_MAX_STRUCT_PICK_FIELDS = 6


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


def _literal_bool_arg(args: Sequence[ExprIR], *, name: str, index: int) -> bool:
    if not args:
        msg = f"{name} expects a literal boolean argument."
        raise ValueError(msg)
    literal = args[index].value
    if not isinstance(literal, bool):
        msg = f"{name} expects a literal boolean argument."
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
    from datafusion_ext import stable_id_parts

    prefix = _literal_prefix(ir_args, name="stable_id")
    return stable_id_parts(prefix, args[1])


def _expr_stable_hash64(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import stable_hash64

    return stable_hash64(args[0])


def _expr_stable_hash128(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import stable_hash128

    return stable_hash128(args[0])


def _expr_prefixed_hash64(args: Sequence[Expr], ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import prefixed_hash_parts64

    prefix = _literal_prefix(ir_args, name="prefixed_hash64")
    return prefixed_hash_parts64(prefix, args[1])


def _expr_stable_id_parts(args: Sequence[Expr], ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import stable_id_parts

    prefix = _literal_prefix(ir_args, name="stable_id_parts")
    if len(args) > _MAX_STABLE_ID_PART_ARGS:
        max_parts = _MAX_STABLE_ID_PART_ARGS - 1
        msg = f"stable_id_parts supports up to {max_parts} parts."
        raise ValueError(msg)
    return stable_id_parts(prefix, args[1], *args[2:])


def _expr_prefixed_hash_parts64(args: Sequence[Expr], ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import prefixed_hash_parts64

    prefix = _literal_prefix(ir_args, name="prefixed_hash_parts64")
    if len(args) > _MAX_STABLE_ID_PART_ARGS:
        max_parts = _MAX_STABLE_ID_PART_ARGS - 1
        msg = f"prefixed_hash_parts64 supports up to {max_parts} parts."
        raise ValueError(msg)
    return prefixed_hash_parts64(prefix, args[1], *args[2:])


def _expr_stable_hash_any(args: Sequence[Expr], ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import stable_hash_any

    canonical = None
    null_sentinel = None
    if len(ir_args) >= _MIN_STABLE_HASH_ANY_CANONICAL_ARGS:
        canonical = _literal_bool_arg(ir_args, name="stable_hash_any", index=1)
    if len(ir_args) >= _MIN_STABLE_HASH_ANY_NULL_SENTINEL_ARGS:
        null_sentinel = _literal_string_arg(ir_args, name="stable_hash_any", index=2)
    return stable_hash_any(args[0], canonical=canonical, null_sentinel=null_sentinel)


def _expr_span_make(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import span_make

    if len(args) > _MAX_SPAN_MAKE_ARGS:
        msg = "span_make supports up to five arguments."
        raise ValueError(msg)
    return span_make(args[0], args[1], *args[2:])


def _expr_span_len(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import span_len

    return span_len(args[0])


def _expr_span_overlaps(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import span_overlaps

    return span_overlaps(args[0], args[1])


def _expr_span_contains(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import span_contains

    return span_contains(args[0], args[1])


def _expr_span_id(args: Sequence[Expr], ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import span_id

    prefix = _literal_prefix(ir_args, name="span_id")
    kind = args[4] if len(args) >= _SPAN_ID_KIND_ARG_COUNT else None
    return span_id(prefix, args[1], args[2], args[3], kind=kind)


def _expr_utf8_normalize(args: Sequence[Expr], ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import utf8_normalize as udf_utf8_normalize

    form = None
    casefold = None
    collapse_ws = None
    if len(ir_args) >= _MIN_UTF8_NORMALIZE_FORM_ARGS:
        form = _literal_string_arg(ir_args, name="utf8_normalize", index=1)
    if len(ir_args) >= _MIN_UTF8_NORMALIZE_CASEFOLD_ARGS:
        casefold = _literal_bool_arg(ir_args, name="utf8_normalize", index=2)
    if len(ir_args) >= _MIN_UTF8_NORMALIZE_COLLAPSE_ARGS:
        collapse_ws = _literal_bool_arg(ir_args, name="utf8_normalize", index=3)
    return udf_utf8_normalize(args[0], form=form, casefold=casefold, collapse_ws=collapse_ws)


def _expr_utf8_null_if_blank(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import utf8_null_if_blank

    return utf8_null_if_blank(args[0])


def _expr_qname_normalize(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import qname_normalize

    module = args[1] if len(args) > 1 else None
    lang = args[2] if len(args) >= _MIN_QNAME_LANG_ARGS else None
    return qname_normalize(args[0], module=module, lang=lang)


def _expr_map_get_default(args: Sequence[Expr], ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import map_get_default

    key = _literal_string_arg(ir_args, name="map_get_default", index=1)
    return map_get_default(args[0], key, args[2])


def _expr_map_normalize(args: Sequence[Expr], ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import map_normalize

    key_case = None
    sort_keys = None
    if len(ir_args) >= _MIN_MAP_NORMALIZE_KEY_CASE_ARGS:
        key_case = _literal_string_arg(ir_args, name="map_normalize", index=1)
    if len(ir_args) >= _MIN_MAP_NORMALIZE_SORT_KEYS_ARGS:
        sort_keys = _literal_bool_arg(ir_args, name="map_normalize", index=2)
    return map_normalize(args[0], key_case=key_case, sort_keys=sort_keys)


def _expr_list_compact(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import list_compact

    return list_compact(args[0])


def _expr_list_unique_sorted(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import list_unique_sorted

    return list_unique_sorted(args[0])


def _expr_struct_pick(args: Sequence[Expr], ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import struct_pick

    field_names = [
        _literal_string_arg(ir_args, name="struct_pick", index=index)
        for index in range(1, len(ir_args))
    ]
    if not field_names:
        msg = "struct_pick requires at least one field name."
        raise ValueError(msg)
    if len(field_names) > _MAX_STRUCT_PICK_FIELDS:
        msg = "struct_pick supports up to six field names."
        raise ValueError(msg)
    return struct_pick(args[0], field_names[0], *field_names[1:])


def _expr_cdf_change_rank(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import cdf_change_rank

    return cdf_change_rank(args[0])


def _expr_cdf_is_upsert(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import cdf_is_upsert

    return cdf_is_upsert(args[0])


def _expr_cdf_is_delete(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import cdf_is_delete

    return cdf_is_delete(args[0])


def _expr_first_value_agg(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import first_value_agg

    return first_value_agg(args[0])


def _expr_last_value_agg(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import last_value_agg

    return last_value_agg(args[0])


def _expr_count_distinct_agg(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import count_distinct_agg

    return count_distinct_agg(args[0])


def _expr_string_agg(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion_ext import string_agg

    if len(args) == 1:
        from datafusion import lit

        return string_agg(args[0], lit(","))
    return string_agg(args[0], args[1])


def _expr_row_number_window(args: Sequence[Expr], _ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion import functions as func

    _ = args  # row_number takes no arguments
    return func.row_number()


def _expr_lag_window(args: Sequence[Expr], ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion import functions as func

    value = args[0]
    offset = _window_offset(ir_args, index=1, default=1)
    return func.lag(value, offset)


def _expr_lead_window(args: Sequence[Expr], ir_args: Sequence[ExprIR]) -> Expr:
    from datafusion import functions as func

    value = args[0]
    offset = _window_offset(ir_args, index=1, default=1)
    return func.lead(value, offset)


def _window_offset(ir_args: Sequence[ExprIR], *, index: int, default: int) -> int:
    resolved = default
    if len(ir_args) > index:
        raw = ir_args[index].value
        if raw is not None:
            if isinstance(raw, ScalarLike):
                raw = raw.as_py()
            if isinstance(raw, bool):
                return resolved
            if isinstance(raw, int):
                resolved = raw
            elif isinstance(raw, float):
                if raw.is_integer():
                    resolved = int(raw)
            elif isinstance(raw, str):
                try:
                    resolved = int(raw)
                except ValueError:
                    resolved = default
    return resolved


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
    "span_len": _EXACT_ONE,
    "span_overlaps": _EXACT_TWO,
    "span_contains": _EXACT_TWO,
    "utf8_null_if_blank": _EXACT_ONE,
    "map_get_default": _EXACT_THREE,
    "list_compact": _EXACT_ONE,
    "list_unique_sorted": _EXACT_ONE,
    "cdf_change_rank": _EXACT_ONE,
    "cdf_is_upsert": _EXACT_ONE,
    "cdf_is_delete": _EXACT_ONE,
    "first_value_agg": _EXACT_ONE,
    "last_value_agg": _EXACT_ONE,
    "count_distinct_agg": _EXACT_ONE,
    "row_number_window": _EXACT_ONE,
}
_MIN_CALL_COUNTS: dict[str, int] = {
    "bit_wise_or": _EXACT_ONE,
    "bit_wise_and": _EXACT_ONE,
    "in_set": _MIN_IN_SET,
    "binary_join_element_wise": _MIN_BINARY_JOIN,
    "stable_id_parts": _EXACT_TWO,
    "prefixed_hash_parts64": _EXACT_TWO,
    "stable_hash_any": _EXACT_ONE,
    "span_make": _EXACT_TWO,
    "span_id": _EXACT_FOUR,
    "utf8_normalize": _EXACT_ONE,
    "qname_normalize": _EXACT_ONE,
    "map_normalize": _EXACT_ONE,
    "struct_pick": _EXACT_TWO,
    "string_agg": _EXACT_ONE,
    "lag_window": _EXACT_ONE,
    "lead_window": _EXACT_ONE,
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
    "stable_id_parts": _expr_stable_id_parts,
    "prefixed_hash_parts64": _expr_prefixed_hash_parts64,
    "stable_hash_any": _expr_stable_hash_any,
    "span_make": _expr_span_make,
    "span_len": _expr_span_len,
    "span_overlaps": _expr_span_overlaps,
    "span_contains": _expr_span_contains,
    "span_id": _expr_span_id,
    "utf8_normalize": _expr_utf8_normalize,
    "utf8_null_if_blank": _expr_utf8_null_if_blank,
    "qname_normalize": _expr_qname_normalize,
    "map_get_default": _expr_map_get_default,
    "map_normalize": _expr_map_normalize,
    "list_compact": _expr_list_compact,
    "list_unique_sorted": _expr_list_unique_sorted,
    "struct_pick": _expr_struct_pick,
    "cdf_change_rank": _expr_cdf_change_rank,
    "cdf_is_upsert": _expr_cdf_is_upsert,
    "cdf_is_delete": _expr_cdf_is_delete,
    "first_value_agg": _expr_first_value_agg,
    "last_value_agg": _expr_last_value_agg,
    "count_distinct_agg": _expr_count_distinct_agg,
    "string_agg": _expr_string_agg,
    "row_number_window": _expr_row_number_window,
    "lag_window": _expr_lag_window,
    "lead_window": _expr_lead_window,
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
