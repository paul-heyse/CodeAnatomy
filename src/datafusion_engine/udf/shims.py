"""Compatibility shims for DataFusion extension expression UDFs."""

from __future__ import annotations

import importlib
from collections.abc import Callable

import pyarrow as pa
from datafusion import Expr
from datafusion import functions as f

_NULL_SEPARATOR = "\x1f"


def _require_callable(name: str) -> Callable[..., object]:
    for module_name in ("datafusion._internal", "datafusion_ext"):
        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue
        func = getattr(module, name, None)
        if isinstance(func, Callable):
            return func
    msg = f"DataFusion extension entrypoint {name} is unavailable."
    raise TypeError(msg)


def _unwrap_expr_arg(value: object) -> object:
    if isinstance(value, Expr):
        return value.expr
    return value


def _wrap_result(result: object) -> Expr:
    if isinstance(result, Expr):
        return result
    try:
        return Expr(result)
    except TypeError as exc:
        msg = "datafusion._internal returned a non-Expr result."
        raise TypeError(msg) from exc


def _fallback_expr(name: str, *args: object, **kwargs: object) -> Expr | None:
    try:
        from datafusion_engine.udf.fallback import fallback_expr
    except ImportError:
        return None
    try:
        return fallback_expr(name, *args, **kwargs)
    except (RuntimeError, TypeError, ValueError):
        return None


def _call_expr(name: str, *args: object, **kwargs: object) -> Expr:
    try:
        func = _require_callable(name)
        resolved_args = tuple(_unwrap_expr_arg(arg) for arg in args)
        resolved_kwargs = {key: _unwrap_expr_arg(value) for key, value in kwargs.items()}
        result = func(*resolved_args, **resolved_kwargs)
    except (RuntimeError, TypeError):
        fallback = _fallback_expr(name, *args, **kwargs)
        if fallback is not None:
            return fallback
        raise
    try:
        return _wrap_result(result)
    except TypeError as exc:
        fallback = _fallback_expr(name, *args, **kwargs)
        if fallback is not None:
            return fallback
        msg = f"DataFusion extension entrypoint {name} returned a non-Expr result."
        raise TypeError(msg) from exc


def map_entries(expr: Expr) -> Expr:
    """Return an expression for map entries.

    Parameters
    ----------
    expr
        Map expression to inspect.

    Returns
    -------
    Expr
        Expression yielding map entries.
    """
    return _call_expr("map_entries", expr)


def map_keys(expr: Expr) -> Expr:
    """Return an expression for map keys.

    Parameters
    ----------
    expr
        Map expression to inspect.

    Returns
    -------
    Expr
        Expression yielding map keys.
    """
    return _call_expr("map_keys", expr)


def map_values(expr: Expr) -> Expr:
    """Return an expression for map values.

    Parameters
    ----------
    expr
        Map expression to inspect.

    Returns
    -------
    Expr
        Expression yielding map values.
    """
    return _call_expr("map_values", expr)


def map_extract(expr: Expr, key: str) -> Expr:
    """Return an expression extracting a map value by key.

    Parameters
    ----------
    expr
        Map expression to inspect.
    key
        Map key to extract.

    Returns
    -------
    Expr
        Expression yielding the map value.
    """
    return _call_expr("map_extract", expr, key)


def list_extract(expr: Expr, index: int) -> Expr:
    """Return an expression extracting a list value by index.

    Parameters
    ----------
    expr
        List expression to inspect.
    index
        Index to extract.

    Returns
    -------
    Expr
        Expression yielding the list element.
    """
    return _call_expr("list_extract", expr, index)


def list_unique(expr: Expr) -> Expr:
    """Return an expression that removes duplicate list elements.

    Parameters
    ----------
    expr
        List expression to normalize.

    Returns
    -------
    Expr
        Expression yielding unique list elements.
    """
    return _call_expr("list_unique", expr)


def first_value_agg(expr: Expr) -> Expr:
    """Return an expression for a first-value aggregate.

    Parameters
    ----------
    expr
        Expression to aggregate.

    Returns
    -------
    Expr
        Aggregate expression for the first value.
    """
    return _call_expr("first_value_agg", expr)


def last_value_agg(expr: Expr) -> Expr:
    """Return an expression for a last-value aggregate.

    Parameters
    ----------
    expr
        Expression to aggregate.

    Returns
    -------
    Expr
        Aggregate expression for the last value.
    """
    return _call_expr("last_value_agg", expr)


def count_distinct_agg(expr: Expr) -> Expr:
    """Return an expression for a count-distinct aggregate.

    Parameters
    ----------
    expr
        Expression to aggregate.

    Returns
    -------
    Expr
        Aggregate expression for distinct counts.
    """
    return _call_expr("count_distinct_agg", expr)


def string_agg(value: Expr, delimiter: Expr) -> Expr:
    """Return an expression for string aggregation.

    Parameters
    ----------
    value
        Expression providing values to aggregate.
    delimiter
        Expression providing the delimiter.

    Returns
    -------
    Expr
        Aggregate expression for concatenated strings.
    """
    return _call_expr("string_agg", value, delimiter)


def row_number_window(expr: Expr) -> Expr:
    """Return an expression for a row-number window function.

    Parameters
    ----------
    expr
        Expression defining the window partition.

    Returns
    -------
    Expr
        Window expression for row numbers.
    """
    return _call_expr("row_number_window", expr)


def lag_window(expr: Expr) -> Expr:
    """Return an expression for a lag window function.

    Parameters
    ----------
    expr
        Expression defining the window partition.

    Returns
    -------
    Expr
        Window expression for lag values.
    """
    return _call_expr("lag_window", expr)


def lead_window(expr: Expr) -> Expr:
    """Return an expression for a lead window function.

    Parameters
    ----------
    expr
        Expression defining the window partition.

    Returns
    -------
    Expr
        Window expression for lead values.
    """
    return _call_expr("lead_window", expr)


def arrow_metadata(expr: Expr, key: str | None = None) -> Expr:
    """Return an expression for Arrow metadata extraction.

    Parameters
    ----------
    expr
        Expression carrying Arrow metadata.
    key
        Optional metadata key to extract.

    Returns
    -------
    Expr
        Expression yielding metadata values.
    """
    return _call_expr("arrow_metadata", expr, key)


def union_tag(expr: Expr) -> Expr:
    """Return an expression for extracting a union tag.

    Parameters
    ----------
    expr
        Union expression.

    Returns
    -------
    Expr
        Expression yielding the union tag.
    """
    return _call_expr("union_tag", expr)


def union_extract(expr: Expr, tag: str) -> Expr:
    """Return an expression for extracting a union variant.

    Parameters
    ----------
    expr
        Union expression.
    tag
        Union tag to extract.

    Returns
    -------
    Expr
        Expression yielding the tagged variant.
    """
    return _call_expr("union_extract", expr, tag)


def stable_hash64(value: Expr) -> Expr:
    """Return an expression for stable 64-bit hashing.

    Parameters
    ----------
    value
        Expression to hash.

    Returns
    -------
    Expr
        Expression yielding 64-bit hashes.
    """
    try:
        return _call_expr("stable_hash64", value)
    except (RuntimeError, TypeError):
        from datafusion import functions as f

        return f.md5(value).alias("stable_hash64")


def stable_hash128(value: Expr) -> Expr:
    """Return an expression for stable 128-bit hashing.

    Parameters
    ----------
    value
        Expression to hash.

    Returns
    -------
    Expr
        Expression yielding 128-bit hashes.
    """
    return _call_expr("stable_hash128", value)


def prefixed_hash64(prefix: str, value: Expr) -> Expr:
    """Return an expression for prefixed 64-bit hashing.

    Parameters
    ----------
    prefix
        Hash namespace prefix.
    value
        Expression to hash.

    Returns
    -------
    Expr
        Expression yielding prefixed hashes.
    """
    return _call_expr("prefixed_hash64", prefix, value)


def stable_id(prefix: str, value: Expr) -> Expr:
    """Return an expression for stable identifier generation.

    Parameters
    ----------
    prefix
        Identifier namespace prefix.
    value
        Expression to hash.

    Returns
    -------
    Expr
        Expression yielding stable identifiers.
    """
    return _call_expr("stable_id", prefix, value)


def semantic_tag(semantic_type: str, value: Expr) -> Expr:
    """Return an expression for semantic tag annotations.

    Parameters
    ----------
    semantic_type
        Semantic type identifier.
    value
        Expression to tag.

    Returns
    -------
    Expr
        Expression yielding tagged values.
    """
    try:
        return _call_expr("semantic_tag", semantic_type, value)
    except (RuntimeError, TypeError):
        return value


def stable_id_parts(prefix: str, part1: Expr, *parts: Expr) -> Expr:
    """Return an expression for stable identifiers from multiple parts.

    Parameters
    ----------
    prefix
        Identifier namespace prefix.
    part1
        First expression part.
    *parts
        Additional expression parts.

    Returns
    -------
    Expr
        Expression yielding stable identifiers.
    """
    try:
        return _call_expr("stable_id_parts", prefix, part1, *parts)
    except (RuntimeError, TypeError):
        null_sentinel = Expr.string_literal("None")
        prefix_expr = Expr.string_literal(prefix)
        normalized = [f.coalesce(part.cast(pa.string()), null_sentinel) for part in (part1, *parts)]
        joined = f.concat_ws(_NULL_SEPARATOR, prefix_expr, *normalized)
        hashed = f.encode(f.sha256(joined), Expr.string_literal("hex"))
        return f.concat_ws(":", prefix_expr, hashed)


def prefixed_hash_parts64(prefix: str, part1: Expr, *parts: Expr) -> Expr:
    """Return an expression for prefixed hashing over multiple parts.

    Parameters
    ----------
    prefix
        Hash namespace prefix.
    part1
        First expression part.
    *parts
        Additional expression parts.

    Returns
    -------
    Expr
        Expression yielding prefixed hashes.
    """
    try:
        return _call_expr("prefixed_hash_parts64", prefix, part1, *parts)
    except (RuntimeError, TypeError):
        prefix_expr = Expr.string_literal(prefix)
        joined = f.concat_ws(_NULL_SEPARATOR, prefix_expr, part1, *parts)
        return stable_hash64(joined)


def stable_hash_any(
    value: Expr,
    *,
    canonical: bool | None = None,
    null_sentinel: str | None = None,
) -> Expr:
    """Return an expression for stable hashing with normalization controls.

    Parameters
    ----------
    value
        Expression to hash.
    canonical
        Whether to normalize values before hashing.
    null_sentinel
        Sentinel value to use for nulls.

    Returns
    -------
    Expr
        Expression yielding stable hashes.
    """
    try:
        return _call_expr(
            "stable_hash_any",
            value,
            canonical=canonical,
            null_sentinel=null_sentinel,
        )
    except (RuntimeError, TypeError):
        return stable_hash64(value)


def span_make(
    bstart: Expr,
    bend: Expr,
    line_base: Expr | None = None,
    col_unit: Expr | None = None,
    end_exclusive: Expr | None = None,
) -> Expr:
    """Return an expression constructing a span.

    Parameters
    ----------
    bstart
        Span start expression.
    bend
        Span end expression.
    line_base
        Optional line base override.
    col_unit
        Optional column unit override.
    end_exclusive
        Optional end-exclusive flag.

    Returns
    -------
    Expr
        Expression yielding spans.
    """
    try:
        return _call_expr(
            "span_make",
            bstart,
            bend,
            line_base,
            col_unit,
            end_exclusive,
        )
    except (RuntimeError, TypeError):
        col_unit_expr = col_unit if col_unit is not None else Expr.string_literal("byte")
        end_exclusive_expr = (
            end_exclusive if end_exclusive is not None else Expr.literal(value=True)
        )
        null_line = Expr.literal(value=None).cast(pa.int32())
        start_struct = f.named_struct([("line0", null_line), ("col", null_line)])
        end_struct = f.named_struct([("line0", null_line), ("col", null_line)])
        byte_start_src = bstart.cast(pa.int64())
        byte_end_src = bend.cast(pa.int64())
        byte_start = byte_start_src.cast(pa.int32())
        byte_len = (byte_end_src - byte_start_src).cast(pa.int32())
        byte_span = f.named_struct([("byte_start", byte_start), ("byte_len", byte_len)])
        return f.named_struct(
            [
                ("start", start_struct),
                ("end", end_struct),
                ("end_exclusive", end_exclusive_expr),
                ("col_unit", col_unit_expr),
                ("byte_span", byte_span),
            ]
        )


def span_len(span: Expr) -> Expr:
    """Return an expression computing span length.

    Parameters
    ----------
    span
        Span expression to measure.

    Returns
    -------
    Expr
        Expression yielding span lengths.
    """
    try:
        return _call_expr("span_len", span)
    except (RuntimeError, TypeError):
        return span["byte_span"]["byte_len"].cast(pa.int64())


def span_start(span: Expr) -> Expr:
    """Return an expression extracting span start.

    Parameters
    ----------
    span
        Span struct expression.

    Returns
    -------
    Expr
        Expression yielding span start.
    """
    try:
        return _call_expr("span_start", span)
    except (RuntimeError, TypeError):
        return span["byte_span"]["byte_start"].cast(pa.int64())


def span_end(span: Expr) -> Expr:
    """Return an expression extracting span end.

    Parameters
    ----------
    span
        Span struct expression.

    Returns
    -------
    Expr
        Expression yielding span end.
    """
    try:
        return _call_expr("span_end", span)
    except (RuntimeError, TypeError):
        return span["byte_span"]["byte_start"].cast(pa.int64()) + span["byte_span"][
            "byte_len"
        ].cast(pa.int64())


def span_overlaps(span_a: Expr, span_b: Expr) -> Expr:
    """Return an expression checking span overlap.

    Parameters
    ----------
    span_a
        First span expression.
    span_b
        Second span expression.

    Returns
    -------
    Expr
        Expression yielding overlap checks.
    """
    try:
        return _call_expr("span_overlaps", span_a, span_b)
    except (RuntimeError, TypeError):
        start_a = span_start(span_a)
        end_a = span_end(span_a)
        start_b = span_start(span_b)
        end_b = span_end(span_b)
        return (start_a < end_b) & (start_b < end_a)


def span_contains(span_a: Expr, span_b: Expr) -> Expr:
    """Return an expression checking span containment.

    Parameters
    ----------
    span_a
        Outer span expression.
    span_b
        Inner span expression.

    Returns
    -------
    Expr
        Expression yielding containment checks.
    """
    try:
        return _call_expr("span_contains", span_a, span_b)
    except (RuntimeError, TypeError):
        start_a = span_start(span_a)
        end_a = span_end(span_a)
        start_b = span_start(span_b)
        end_b = span_end(span_b)
        return (start_a <= start_b) & (end_a >= end_b)


def interval_align_score(
    left_start: Expr,
    left_end: Expr,
    right_start: Expr,
    right_end: Expr,
) -> Expr:
    """Return an expression computing interval alignment score.

    Parameters
    ----------
    left_start
        Start of left interval.
    left_end
        End of left interval.
    right_start
        Start of right interval.
    right_end
        End of right interval.

    Returns
    -------
    Expr
        Expression yielding alignment scores.
    """
    return _call_expr("interval_align_score", left_start, left_end, right_start, right_end)


def span_id(
    prefix: str,
    path: Expr,
    bstart: Expr,
    bend: Expr,
    *,
    kind: Expr | None = None,
) -> Expr:
    """Return an expression computing span identifiers.

    Parameters
    ----------
    prefix
        Identifier namespace prefix.
    path
        Path expression for the span.
    bstart
        Span start expression.
    bend
        Span end expression.
    kind
        Optional span kind expression.

    Returns
    -------
    Expr
        Expression yielding span identifiers.
    """
    try:
        return _call_expr("span_id", prefix, path, bstart, bend, kind=kind)
    except (RuntimeError, TypeError):
        parts = [path, bstart.cast(pa.string()), bend.cast(pa.string())]
        if kind is not None:
            parts.insert(0, kind.cast(pa.string()))
        return stable_id_parts(prefix, parts[0], *parts[1:])


def utf8_normalize(
    value: Expr,
    *,
    form: str | None = None,
    casefold: bool | None = None,
    collapse_ws: bool | None = None,
) -> Expr:
    """Return an expression normalizing UTF-8 text.

    Parameters
    ----------
    value
        Text expression to normalize.
    form
        Unicode normalization form.
    casefold
        Whether to apply case folding.
    collapse_ws
        Whether to collapse whitespace.

    Returns
    -------
    Expr
        Expression yielding normalized text.
    """
    try:
        return _call_expr(
            "utf8_normalize",
            value,
            form=form,
            casefold=casefold,
            collapse_ws=collapse_ws,
        )
    except (RuntimeError, TypeError):
        normalized = value
        if casefold:
            normalized = f.lower(normalized)
        if collapse_ws:
            normalized = f.regexp_replace(
                normalized,
                Expr.string_literal(r"\s+"),
                Expr.string_literal(" "),
            )
            normalized = f.trim(normalized)
        return normalized


def utf8_null_if_blank(value: Expr) -> Expr:
    """Return an expression converting blank strings to null.

    Parameters
    ----------
    value
        Text expression to transform.

    Returns
    -------
    Expr
        Expression yielding nulls for blanks.
    """
    try:
        return _call_expr("utf8_null_if_blank", value)
    except (RuntimeError, TypeError):
        trimmed = f.trim(value)
        return f.nullif(trimmed, Expr.string_literal(""))


def qname_normalize(
    symbol: Expr,
    *,
    module: Expr | None = None,
    lang: Expr | None = None,
) -> Expr:
    """Return an expression normalizing qualified names.

    Parameters
    ----------
    symbol
        Symbol expression to normalize.
    module
        Optional module expression.
    lang
        Optional language expression.

    Returns
    -------
    Expr
        Expression yielding normalized qualified names.
    """
    return _call_expr("qname_normalize", symbol, module=module, lang=lang)


def map_get_default(map_expr: Expr, key: str, default_value: Expr) -> Expr:
    """Return an expression retrieving a map value with default.

    Parameters
    ----------
    map_expr
        Map expression to inspect.
    key
        Map key to extract.
    default_value
        Default expression when key is missing.

    Returns
    -------
    Expr
        Expression yielding map values or defaults.
    """
    return _call_expr("map_get_default", map_expr, key, default_value)


def map_normalize(
    map_expr: Expr,
    *,
    key_case: str | None = None,
    sort_keys: bool | None = None,
) -> Expr:
    """Return an expression normalizing map keys.

    Parameters
    ----------
    map_expr
        Map expression to normalize.
    key_case
        Optional key case normalization.
    sort_keys
        Whether to sort map keys.

    Returns
    -------
    Expr
        Expression yielding normalized maps.
    """
    return _call_expr("map_normalize", map_expr, key_case=key_case, sort_keys=sort_keys)


def list_compact(list_expr: Expr) -> Expr:
    """Return an expression compacting list elements.

    Parameters
    ----------
    list_expr
        List expression to compact.

    Returns
    -------
    Expr
        Expression yielding compacted lists.
    """
    return _call_expr("list_compact", list_expr)


def list_unique_sorted(list_expr: Expr) -> Expr:
    """Return an expression yielding sorted unique list elements.

    Parameters
    ----------
    list_expr
        List expression to normalize.

    Returns
    -------
    Expr
        Expression yielding unique sorted lists.
    """
    return _call_expr("list_unique_sorted", list_expr)


def struct_pick(struct_expr: Expr, field: str, *fields: str | None) -> Expr:
    """Return an expression selecting fields from a struct.

    Parameters
    ----------
    struct_expr
        Struct expression to project.
    field
        First field name to select.
    *fields
        Additional field names to select.

    Returns
    -------
    Expr
        Expression yielding projected structs.
    """
    selected = tuple(name for name in (field, *fields) if name is not None)
    return _call_expr("struct_pick", struct_expr, *selected)


def cdf_change_rank(change_type: Expr) -> Expr:
    """Return an expression ranking change data feed types.

    Parameters
    ----------
    change_type
        Change type expression.

    Returns
    -------
    Expr
        Expression yielding CDF rank values.
    """
    return _call_expr("cdf_change_rank", change_type)


def cdf_is_upsert(change_type: Expr) -> Expr:
    """Return an expression checking for CDF upserts.

    Parameters
    ----------
    change_type
        Change type expression.

    Returns
    -------
    Expr
        Expression yielding upsert checks.
    """
    return _call_expr("cdf_is_upsert", change_type)


def cdf_is_delete(change_type: Expr) -> Expr:
    """Return an expression checking for CDF deletes.

    Parameters
    ----------
    change_type
        Change type expression.

    Returns
    -------
    Expr
        Expression yielding delete checks.
    """
    return _call_expr("cdf_is_delete", change_type)


def col_to_byte(line_text: Expr, col_index: Expr, col_unit: Expr) -> Expr:
    """Return an expression converting column offsets to byte offsets.

    Parameters
    ----------
    line_text
        Line text expression.
    col_index
        Column index expression.
    col_unit
        Column unit expression.

    Returns
    -------
    Expr
        Expression yielding byte offsets.
    """
    return _call_expr("col_to_byte", line_text, col_index, col_unit)


__all__ = [
    "arrow_metadata",
    "cdf_change_rank",
    "cdf_is_delete",
    "cdf_is_upsert",
    "col_to_byte",
    "count_distinct_agg",
    "first_value_agg",
    "interval_align_score",
    "lag_window",
    "lead_window",
    "list_compact",
    "list_extract",
    "list_unique",
    "list_unique_sorted",
    "map_entries",
    "map_extract",
    "map_get_default",
    "map_keys",
    "map_normalize",
    "map_values",
    "prefixed_hash64",
    "prefixed_hash_parts64",
    "qname_normalize",
    "row_number_window",
    "semantic_tag",
    "span_contains",
    "span_end",
    "span_id",
    "span_len",
    "span_make",
    "span_overlaps",
    "span_start",
    "stable_hash64",
    "stable_hash128",
    "stable_hash_any",
    "stable_id",
    "stable_id_parts",
    "string_agg",
    "struct_pick",
    "union_extract",
    "union_tag",
    "utf8_normalize",
    "utf8_null_if_blank",
]
