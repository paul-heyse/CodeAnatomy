"""Dispatch table builders for expression-spec call handling."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, Protocol, cast

ExprDispatchHandler = Callable[[Sequence[Any], Sequence[Any]], Any]
SqlDispatchRenderer = Callable[[Sequence[str]], str]


class _SqlCallBitwise(Protocol):
    def __call__(self, rendered: Sequence[str], *, operator: str) -> str: ...


class _SqlCallBinaryJoin(Protocol):
    def __call__(self, rendered: Sequence[str], *, separator_idx: int) -> str: ...


_EXPR_HANDLER_NAMES: Mapping[str, str] = {
    "stringify": "_expr_stringify",
    "utf8_trim_whitespace": "_expr_trim",
    "coalesce": "_expr_coalesce",
    "bit_wise_or": "_expr_bitwise_or",
    "bit_wise_and": "_expr_bitwise_and",
    "equal": "_expr_equal",
    "starts_with": "_expr_starts_with",
    "if_else": "_expr_if_else",
    "in_set": "_expr_in_set",
    "invert": "_expr_invert",
    "is_null": "_expr_is_null",
    "binary_join_element_wise": "_expr_binary_join",
    "stable_id": "_expr_stable_id",
    "stable_hash64": "_expr_stable_hash64",
    "stable_hash128": "_expr_stable_hash128",
    "prefixed_hash64": "_expr_prefixed_hash64",
    "stable_id_parts": "_expr_stable_id_parts",
    "prefixed_hash_parts64": "_expr_prefixed_hash_parts64",
    "stable_hash_any": "_expr_stable_hash_any",
    "span_make": "_expr_span_make",
    "span_len": "_expr_span_len",
    "span_overlaps": "_expr_span_overlaps",
    "span_contains": "_expr_span_contains",
    "span_id": "_expr_span_id",
    "utf8_normalize": "_expr_utf8_normalize",
    "utf8_null_if_blank": "_expr_utf8_null_if_blank",
    "qname_normalize": "_expr_qname_normalize",
    "map_get_default": "_expr_map_get_default",
    "map_normalize": "_expr_map_normalize",
    "list_compact": "_expr_list_compact",
    "list_unique_sorted": "_expr_list_unique_sorted",
    "struct_pick": "_expr_struct_pick",
    "cdf_change_rank": "_expr_cdf_change_rank",
    "cdf_is_upsert": "_expr_cdf_is_upsert",
    "cdf_is_delete": "_expr_cdf_is_delete",
    "first_value_agg": "_expr_first_value_agg",
    "last_value_agg": "_expr_last_value_agg",
    "count_distinct_agg": "_expr_count_distinct_agg",
    "string_agg": "_expr_string_agg",
    "row_number_window": "_expr_row_number_window",
    "lag_window": "_expr_lag_window",
    "lead_window": "_expr_lead_window",
}


def build_expr_calls(namespace: Mapping[str, Any]) -> dict[str, ExprDispatchHandler]:
    """Build expression-call dispatch from a namespace of handler callables.

    Returns:
    -------
    dict[str, ExprDispatchHandler]
        Mapping of expression call names to concrete dispatch handlers.

    Raises:
        TypeError: If any expected expression handler is missing or not callable.
    """
    dispatch: dict[str, ExprDispatchHandler] = {}
    for call_name, handler_name in _EXPR_HANDLER_NAMES.items():
        handler = namespace.get(handler_name)
        if not callable(handler):
            msg = f"Missing expression handler: {handler_name!r}"
            raise TypeError(msg)
        dispatch[call_name] = cast("ExprDispatchHandler", handler)
    return dispatch


def build_sql_calls(
    *,
    sql_call_bitwise: _SqlCallBitwise,
    sql_call_binary_join: _SqlCallBinaryJoin,
) -> dict[str, SqlDispatchRenderer]:
    """Build SQL-render dispatch for expression-call SQL lowering.

    Returns:
    -------
    dict[str, SqlDispatchRenderer]
        Mapping of expression call names to SQL renderer functions.
    """
    return {
        "stringify": lambda rendered: f"CAST({rendered[0]} AS STRING)",
        "utf8_trim_whitespace": lambda rendered: f"TRIM({rendered[0]})",
        "coalesce": lambda rendered: f"COALESCE({', '.join(rendered)})",
        "bit_wise_or": lambda rendered: sql_call_bitwise(rendered, operator="OR"),
        "bit_wise_and": lambda rendered: sql_call_bitwise(rendered, operator="AND"),
        "equal": lambda rendered: f"({rendered[0]} = {rendered[1]})",
        "starts_with": lambda rendered: f"starts_with({rendered[0]}, {rendered[1]})",
        "if_else": lambda rendered: (
            f"(CASE WHEN {rendered[0]} THEN {rendered[1]} ELSE {rendered[2]} END)"
        ),
        "in_set": lambda rendered: f"({rendered[0]} IN ({', '.join(rendered[1:])}))",
        "invert": lambda rendered: f"(NOT {rendered[0]})",
        "is_null": lambda rendered: f"({rendered[0]} IS NULL)",
        "binary_join_element_wise": lambda rendered: sql_call_binary_join(
            rendered,
            separator_idx=-1,
        ),
    }


__all__ = ["build_expr_calls", "build_sql_calls"]
