"""View expression DSL for declarative view construction.

This module provides a fluent API for building view expressions, reducing
repetition in view definitions by encapsulating common patterns like span
extraction, identity columns, and attribute mapping.

Examples
--------
>>> builder = ViewExprBuilder()
>>> exprs = (
...     builder.add_identity_cols("file_id", "path", "ast_id")
...     .add_span_fields()
...     .add_attrs_col()
...     .add_ast_record()
...     .build()
... )
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Self

from datafusion import col, lit
from datafusion import functions as f
from datafusion.expr import Expr

if TYPE_CHECKING:
    from collections.abc import Sequence


def _arrow_cast(expr: Expr, data_type: str) -> Expr:
    """Cast expression to Arrow data type.

    Returns
    -------
    Expr
        Casted expression.
    """
    return f.arrow_cast(expr, lit(data_type))


def _null_expr(data_type: str) -> Expr:
    """Create a null literal with specified type.

    Returns
    -------
    Expr
        Null literal expression.
    """
    return _arrow_cast(lit(None), data_type)


def _span_struct(span: Expr) -> Expr:
    """Build canonical span struct from span expression.

    Returns
    -------
    Expr
        Named struct expression with span fields.
    """
    return f.named_struct(
        [
            ("start", span["start"]),
            ("end", span["end"]),
            ("end_exclusive", span["end_exclusive"]),
            ("col_unit", span["col_unit"]),
            ("byte_span", span["byte_span"]),
        ]
    )


def _ast_record(span: Expr, attrs: Expr) -> Expr:
    """Build AST record struct from span and attrs.

    Returns
    -------
    Expr
        Named struct with span and attrs fields.
    """
    return f.named_struct([("span", _span_struct(span)), ("attrs", attrs)])


@dataclass(frozen=True, slots=True)
class SpanExprs:
    """Span field extraction expressions for nested span structs.

    Encapsulates the repeated pattern of extracting lineno, col_offset,
    end_lineno, end_col_offset, line_base, col_unit, end_exclusive,
    and full span struct from a span column.

    Parameters
    ----------
    span_col
        Name of the span column to extract from. Defaults to "span".

    Examples
    --------
    >>> span = SpanExprs("span")
    >>> lineno_expr = span.lineno()
    >>> all_fields = span.all_span_fields()
    """

    span_col: str = "span"

    def _span(self) -> Expr:
        """Get the span column expression.

        Returns
        -------
        Expr
            Column expression for the span.
        """
        return col(self.span_col)

    def lineno(self) -> Expr:
        """Extract 1-based line number (start.line0 + 1) as Int64.

        Returns
        -------
        Expr
            Line number expression.
        """
        return _arrow_cast(((self._span()["start"])["line0"] + lit(1)), "Int64")

    def col_offset(self) -> Expr:
        """Extract column offset (start.col) as Int64.

        Returns
        -------
        Expr
            Column offset expression.
        """
        return _arrow_cast((self._span()["start"])["col"], "Int64")

    def end_lineno(self) -> Expr:
        """Extract 1-based end line number (end.line0 + 1) as Int64.

        Returns
        -------
        Expr
            End line number expression.
        """
        return _arrow_cast(((self._span()["end"])["line0"] + lit(1)), "Int64")

    def end_col_offset(self) -> Expr:
        """Extract end column offset (end.col) as Int64.

        Returns
        -------
        Expr
            End column offset expression.
        """
        return _arrow_cast((self._span()["end"])["col"], "Int64")

    def line_base(self) -> Expr:  # noqa: PLR6301
        """Return constant line base (1) as Int32.

        Returns
        -------
        Expr
            Constant line base expression.
        """
        return _arrow_cast(lit(1), "Int32")

    def col_unit(self) -> Expr:
        """Extract column unit as Utf8.

        Returns
        -------
        Expr
            Column unit expression.
        """
        return _arrow_cast(self._span()["col_unit"], "Utf8")

    def end_exclusive(self) -> Expr:
        """Extract end_exclusive flag as Boolean.

        Returns
        -------
        Expr
            End exclusive flag expression.
        """
        return _arrow_cast(self._span()["end_exclusive"], "Boolean")

    def span_struct(self) -> Expr:
        """Build full span struct from span column.

        Returns
        -------
        Expr
            Span struct expression.
        """
        return _span_struct(self._span())

    def bstart(self) -> Expr:
        """Extract byte start offset as Int64.

        Returns
        -------
        Expr
            Byte start expression.
        """
        return _arrow_cast((self._span()["byte_span"])["byte_start"], "Int64")

    def bend(self) -> Expr:
        """Extract byte end offset (byte_start + byte_len) as Int64.

        Returns
        -------
        Expr
            Byte end expression.
        """
        byte_start = (self._span()["byte_span"])["byte_start"]
        byte_len = (self._span()["byte_span"])["byte_len"]
        return _arrow_cast(byte_start + byte_len, "Int64")

    def all_span_fields(self) -> tuple[Expr, ...]:
        """Return all standard span fields as aliased expressions.

        Returns
        -------
        tuple[Expr, ...]
            Tuple of (lineno, col_offset, end_lineno, end_col_offset,
            line_base, col_unit, end_exclusive, span).
        """
        return (
            self.lineno().alias("lineno"),
            self.col_offset().alias("col_offset"),
            self.end_lineno().alias("end_lineno"),
            self.end_col_offset().alias("end_col_offset"),
            self.line_base().alias("line_base"),
            self.col_unit().alias("col_unit"),
            self.end_exclusive().alias("end_exclusive"),
            self.span_struct().alias("span"),
        )

    def all_span_fields_with_bytes(self) -> tuple[Expr, ...]:
        """Return all span fields including byte offsets.

        Returns
        -------
        tuple[Expr, ...]
            Standard span fields plus bstart and bend.
        """
        return (
            self.lineno().alias("lineno"),
            self.col_offset().alias("col_offset"),
            self.end_lineno().alias("end_lineno"),
            self.end_col_offset().alias("end_col_offset"),
            self.line_base().alias("line_base"),
            self.col_unit().alias("col_unit"),
            self.end_exclusive().alias("end_exclusive"),
            self.bstart().alias("bstart"),
            self.bend().alias("bend"),
            self.span_struct().alias("span"),
        )


@dataclass
class ViewExprBuilder:
    """Fluent builder for view select expressions.

    Provides a chainable API for constructing view expressions,
    reducing boilerplate and ensuring consistency across views.

    Examples
    --------
    >>> exprs = (
    ...     ViewExprBuilder()
    ...     .add_identity_cols("file_id", "path", "ast_id")
    ...     .add_span_fields()
    ...     .add_attrs_col()
    ...     .build()
    ... )
    """

    _exprs: list[Expr] = field(default_factory=list)

    def add_expr(self, expr: Expr) -> Self:
        """Add a single expression to the builder.

        Parameters
        ----------
        expr
            Expression to add.

        Returns
        -------
        Self
            Builder for chaining.
        """
        self._exprs.append(expr)
        return self

    def add_exprs(self, exprs: Sequence[Expr]) -> Self:
        """Add multiple expressions to the builder.

        Parameters
        ----------
        exprs
            Expressions to add.

        Returns
        -------
        Self
            Builder for chaining.
        """
        self._exprs.extend(exprs)
        return self

    def add_identity_cols(self, *cols: str) -> Self:
        """Add identity columns (col("x").alias("x")).

        Parameters
        ----------
        *cols
            Column names to add as identity columns.

        Returns
        -------
        Self
            Builder for chaining.
        """
        for c in cols:
            self._exprs.append(col(c).alias(c))
        return self

    def add_col(self, col_name: str, alias: str | None = None) -> Self:
        """Add a single column with optional alias.

        Parameters
        ----------
        col_name
            Source column name.
        alias
            Output alias. Defaults to col_name.

        Returns
        -------
        Self
            Builder for chaining.
        """
        self._exprs.append(col(col_name).alias(alias or col_name))
        return self

    def add_span_fields(self, span_col: str = "span") -> Self:
        """Add all standard span extraction fields.

        Adds: lineno, col_offset, end_lineno, end_col_offset,
        line_base, col_unit, end_exclusive, span.

        Parameters
        ----------
        span_col
            Name of the span column. Defaults to "span".

        Returns
        -------
        Self
            Builder for chaining.
        """
        self._exprs.extend(SpanExprs(span_col).all_span_fields())
        return self

    def add_span_fields_with_bytes(self, span_col: str = "span") -> Self:
        """Add all span fields including byte offsets.

        Adds standard span fields plus bstart and bend.

        Parameters
        ----------
        span_col
            Name of the span column. Defaults to "span".

        Returns
        -------
        Self
            Builder for chaining.
        """
        self._exprs.extend(SpanExprs(span_col).all_span_fields_with_bytes())
        return self

    def add_attrs_col(self, col_name: str = "attrs") -> Self:
        """Add attrs column as identity.

        Parameters
        ----------
        col_name
            Name of the attrs column. Defaults to "attrs".

        Returns
        -------
        Self
            Builder for chaining.
        """
        self._exprs.append(col(col_name).alias(col_name))
        return self

    def add_ast_record(
        self,
        span_col: str = "span",
        attrs_col: str = "attrs",
        alias: str = "ast_record",
    ) -> Self:
        """Add ast_record struct from span and attrs.

        Parameters
        ----------
        span_col
            Name of the span column. Defaults to "span".
        attrs_col
            Name of the attrs column. Defaults to "attrs".
        alias
            Output alias. Defaults to "ast_record".

        Returns
        -------
        Self
            Builder for chaining.
        """
        self._exprs.append(_ast_record(col(span_col), col(attrs_col)).alias(alias))
        return self

    def add_kv_extraction(
        self,
        kv_col: str = "kv",
        key_alias: str = "attr_key",
        value_alias: str = "attr_value",
    ) -> Self:
        """Add key-value extraction from kv struct column.

        Parameters
        ----------
        kv_col
            Name of the kv column. Defaults to "kv".
        key_alias
            Alias for extracted key. Defaults to "attr_key".
        value_alias
            Alias for extracted value. Defaults to "attr_value".

        Returns
        -------
        Self
            Builder for chaining.
        """
        kv = col(kv_col)
        self._exprs.append(kv["key"].alias(key_alias))
        self._exprs.append(kv["value"].alias(value_alias))
        return self

    def add_map_attr(
        self,
        key: str,
        cast_type: str | None = None,
        alias: str | None = None,
        attrs_col: str = "attrs",
    ) -> Self:
        """Extract and optionally cast a value from attrs map.

        Parameters
        ----------
        key
            Map key to extract.
        cast_type
            Arrow type to cast to. None for no cast.
        alias
            Output alias. Defaults to key.
        attrs_col
            Name of the attrs column. Defaults to "attrs".

        Returns
        -------
        Self
            Builder for chaining.
        """
        from datafusion_engine.udf.shims import list_extract, map_extract

        expr = list_extract(map_extract(col(attrs_col), key), 1)
        if cast_type is not None:
            expr = _arrow_cast(expr, cast_type)
        self._exprs.append(expr.alias(alias or key))
        return self

    def add_map_attrs(
        self,
        *keys: str,
        cast_type: str | None = None,
        attrs_col: str = "attrs",
    ) -> Self:
        """Extract multiple values from attrs map with same cast.

        Parameters
        ----------
        *keys
            Map keys to extract.
        cast_type
            Arrow type to cast to. None for no cast.
        attrs_col
            Name of the attrs column. Defaults to "attrs".

        Returns
        -------
        Self
            Builder for chaining.
        """
        for key in keys:
            self.add_map_attr(key, cast_type=cast_type, attrs_col=attrs_col)
        return self

    def build(self) -> tuple[Expr, ...]:
        """Build and return the expression tuple.

        Returns
        -------
        tuple[Expr, ...]
            Immutable tuple of all added expressions.
        """
        return tuple(self._exprs)


# Convenience factory functions for common view patterns


def identity_cols(*cols: str) -> tuple[Expr, ...]:
    """Create identity column expressions.

    Parameters
    ----------
    *cols
        Column names.

    Returns
    -------
    tuple[Expr, ...]
        Identity expressions.
    """
    return tuple(col(c).alias(c) for c in cols)


def span_fields(span_col: str = "span") -> tuple[Expr, ...]:
    """Create standard span field expressions.

    Parameters
    ----------
    span_col
        Name of the span column.

    Returns
    -------
    tuple[Expr, ...]
        Span field expressions.
    """
    return SpanExprs(span_col).all_span_fields()


def span_fields_with_bytes(span_col: str = "span") -> tuple[Expr, ...]:
    """Create span field expressions including byte offsets.

    Parameters
    ----------
    span_col
        Name of the span column.

    Returns
    -------
    tuple[Expr, ...]
        Span field expressions with bstart/bend.
    """
    return SpanExprs(span_col).all_span_fields_with_bytes()


def kv_extraction(
    kv_col: str = "kv",
    key_alias: str = "attr_key",
    value_alias: str = "attr_value",
) -> tuple[Expr, ...]:
    """Create key-value extraction expressions.

    Parameters
    ----------
    kv_col
        Name of the kv column.
    key_alias
        Alias for extracted key.
    value_alias
        Alias for extracted value.

    Returns
    -------
    tuple[Expr, ...]
        Key and value extraction expressions.
    """
    kv = col(kv_col)
    return (kv["key"].alias(key_alias), kv["value"].alias(value_alias))


def ast_record_expr(span_col: str = "span", attrs_col: str = "attrs") -> Expr:
    """Create ast_record struct expression.

    Parameters
    ----------
    span_col
        Name of the span column.
    attrs_col
        Name of the attrs column.

    Returns
    -------
    Expr
        AST record struct expression (not aliased).
    """
    return _ast_record(col(span_col), col(attrs_col))


__all__ = [
    "SpanExprs",
    "ViewExprBuilder",
    "ast_record_expr",
    "identity_cols",
    "kv_extraction",
    "span_fields",
    "span_fields_with_bytes",
]
