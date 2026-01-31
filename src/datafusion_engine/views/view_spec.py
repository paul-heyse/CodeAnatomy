"""Declarative view projection specifications.

This module provides a DSL for defining view projections that capture the
unique parameters for each view while enabling a generic builder to construct
the DataFusion expressions. This replaces thousands of lines of hand-coded
expression tuples with concise, declarative specifications.

Usage
-----
Define views using ViewProjectionSpec::

    from datafusion_engine.views.view_spec import ViewProjectionSpec, ColumnTransform

    spec = ViewProjectionSpec(
        name="ast_calls",
        base_table="ast_files_v1",
        include_file_identity=True,
        passthrough_cols=("ast_id", "parent_ast_id"),
        include_span_struct=True,
    )

    # Generate expressions from spec
    exprs = spec.to_exprs(base_schema)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final

from datafusion import col, lit
from datafusion import functions as f

from datafusion_engine.udf.shims import (
    arrow_metadata,
    list_extract,
    map_extract,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    import pyarrow as pa
    from datafusion.expr import Expr

    # Type alias for transform handlers - uses forward reference
    TransformHandler = Callable[["ColumnTransform"], "Expr | None"]

# Minimum number of columns for coalesce
_COALESCE_MIN_COLUMNS: Final[int] = 2


# ---------------------------------------------------------------------------
# Column Transform Types
# ---------------------------------------------------------------------------


class ColumnTransformKind(StrEnum):
    """Types of column transformations."""

    PASSTHROUGH = "passthrough"  # col(x).alias(x)
    RENAME = "rename"  # col(x).alias(y)
    CAST = "cast"  # arrow_cast(col(x), type)
    STRUCT_FIELD = "struct_field"  # col(struct)[field]
    NESTED_STRUCT_FIELD = "nested_struct"  # col(struct)[f1][f2]
    MAP_EXTRACT = "map_extract"  # list_extract(map_extract(col(x), key), 1)
    METADATA_EXTRACT = "metadata"  # arrow_metadata(col(struct), key)
    COALESCE = "coalesce"  # coalesce(col(a), col(b))
    LITERAL = "literal"  # lit(value)
    EXPRESSION = "expression"  # Custom expression


@dataclass(frozen=True, slots=True)
class ColumnTransform:
    """Specification for a single column transformation.

    Parameters
    ----------
    kind
        Type of transformation to apply.
    source_col
        Source column name (not used for LITERAL kind).
    output_name
        Output alias; defaults to source_col if not specified.
    cast_type
        Arrow type string for CAST kind (e.g., "Int32", "Utf8").
    struct_path
        Path for nested struct access (e.g., ("span", "start", "line0")).
    map_key
        Key for MAP_EXTRACT kind.
    metadata_key
        Key for METADATA_EXTRACT kind.
    fallback_cols
        Fallback columns for COALESCE kind.
    literal_value
        Literal value for LITERAL kind.
    adjust_expr
        Optional expression adjustment (e.g., +1 for line numbers).
    """

    kind: ColumnTransformKind
    source_col: str = ""
    output_name: str | None = None
    cast_type: str | None = None
    struct_path: tuple[str, ...] = ()
    map_key: str | None = None
    metadata_key: str | None = None
    fallback_cols: tuple[str, ...] = ()
    literal_value: object = None
    adjust_expr: int | None = None  # For line number +1 adjustments

    @property
    def alias(self) -> str:
        """Return the output column alias.

        Returns
        -------
        str
            Output column name.
        """
        if self.output_name is not None:
            return self.output_name
        if self.struct_path:
            return self.struct_path[-1]
        if self.map_key:
            return self.map_key
        if self.metadata_key:
            return self.metadata_key
        return self.source_col


# ---------------------------------------------------------------------------
# Transform Constructors (convenience functions)
# ---------------------------------------------------------------------------


def passthrough(*cols: str) -> tuple[ColumnTransform, ...]:
    """Create passthrough transforms for multiple columns.

    Parameters
    ----------
    *cols
        Column names to pass through unchanged.

    Returns
    -------
    tuple[ColumnTransform, ...]
        Passthrough transforms.
    """
    return tuple(ColumnTransform(kind=ColumnTransformKind.PASSTHROUGH, source_col=c) for c in cols)


def rename(source: str, target: str) -> ColumnTransform:
    """Create a rename transform.

    Parameters
    ----------
    source
        Source column name.
    target
        Target column name.

    Returns
    -------
    ColumnTransform
        Rename transform.
    """
    return ColumnTransform(kind=ColumnTransformKind.RENAME, source_col=source, output_name=target)


def cast(source: str, dtype: str, *, alias: str | None = None) -> ColumnTransform:
    """Create a cast transform.

    Parameters
    ----------
    source
        Source column name.
    dtype
        Arrow type string (e.g., "Int32", "Utf8").
    alias
        Optional output alias.

    Returns
    -------
    ColumnTransform
        Cast transform.
    """
    return ColumnTransform(
        kind=ColumnTransformKind.CAST, source_col=source, cast_type=dtype, output_name=alias
    )


def struct_field(source: str, *path: str, alias: str | None = None) -> ColumnTransform:
    """Create a struct field extraction transform.

    Parameters
    ----------
    source
        Source struct column name.
    *path
        Path to nested field (e.g., "start", "line0").
    alias
        Optional output alias.

    Returns
    -------
    ColumnTransform
        Struct field transform.
    """
    return ColumnTransform(
        kind=ColumnTransformKind.NESTED_STRUCT_FIELD,
        source_col=source,
        struct_path=path,
        output_name=alias,
    )


def map_extract_col(
    source: str, key: str, dtype: str, *, alias: str | None = None
) -> ColumnTransform:
    """Create a map extraction transform with cast.

    Parameters
    ----------
    source
        Source map column name.
    key
        Map key to extract.
    dtype
        Arrow type string for cast.
    alias
        Optional output alias.

    Returns
    -------
    ColumnTransform
        Map extraction transform.
    """
    return ColumnTransform(
        kind=ColumnTransformKind.MAP_EXTRACT,
        source_col=source,
        map_key=key,
        cast_type=dtype,
        output_name=alias,
    )


def metadata_col(source: str, key: str, *, alias: str | None = None) -> ColumnTransform:
    """Create a metadata extraction transform.

    Parameters
    ----------
    source
        Source column name.
    key
        Metadata key to extract.
    alias
        Optional output alias.

    Returns
    -------
    ColumnTransform
        Metadata extraction transform.
    """
    return ColumnTransform(
        kind=ColumnTransformKind.METADATA_EXTRACT,
        source_col=source,
        metadata_key=key,
        output_name=alias,
    )


def coalesce_col(*cols: str, alias: str | None = None) -> ColumnTransform:
    """Create a coalesce transform.

    Parameters
    ----------
    *cols
        Columns to coalesce (first non-null wins).
    alias
        Optional output alias; defaults to first column.

    Returns
    -------
    ColumnTransform
        Coalesce transform.

    Raises
    ------
    ValueError
        Raised when fewer than two columns are provided.
    """
    if len(cols) < _COALESCE_MIN_COLUMNS:
        msg = "Coalesce requires at least 2 columns."
        raise ValueError(msg)
    return ColumnTransform(
        kind=ColumnTransformKind.COALESCE,
        source_col=cols[0],
        fallback_cols=cols[1:],
        output_name=alias,
    )


def literal_col(value: object, alias: str) -> ColumnTransform:
    """Create a literal value column.

    Parameters
    ----------
    value
        Literal value.
    alias
        Output column name.

    Returns
    -------
    ColumnTransform
        Literal transform.
    """
    return ColumnTransform(kind=ColumnTransformKind.LITERAL, literal_value=value, output_name=alias)


# ---------------------------------------------------------------------------
# Span Field Patterns
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SpanFieldConfig:
    """Configuration for span field extraction patterns.

    Parameters
    ----------
    span_col
        Source span column name.
    include_legacy_fields
        Include lineno, col_offset, end_lineno, end_col_offset.
    include_line_base
        Include line_base field.
    include_col_unit
        Include col_unit field.
    include_end_exclusive
        Include end_exclusive field.
    include_span_struct
        Include the full span struct.
    include_ast_record
        Include the ast_record composite struct.
    attrs_col
        Attrs column for ast_record (if included).
    line_base_value
        Value for line_base literal.
    """

    span_col: str = "span"
    include_legacy_fields: bool = True
    include_line_base: bool = True
    include_col_unit: bool = True
    include_end_exclusive: bool = True
    include_span_struct: bool = True
    include_ast_record: bool = False
    attrs_col: str = "attrs"
    line_base_value: int = 1


# Preset configurations
SPAN_FIELDS_STANDARD: Final[SpanFieldConfig] = SpanFieldConfig()
SPAN_FIELDS_WITH_AST_RECORD: Final[SpanFieldConfig] = SpanFieldConfig(include_ast_record=True)
SPAN_FIELDS_MINIMAL: Final[SpanFieldConfig] = SpanFieldConfig(
    include_legacy_fields=False,
    include_line_base=False,
    include_col_unit=False,
    include_end_exclusive=False,
)


# ---------------------------------------------------------------------------
# View Projection Specification
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ViewProjectionSpec:
    """Declarative view projection specification.

    This captures all parameters needed to generate DataFusion select expressions
    for a view, enabling a single generic builder to handle all view types.

    Parameters
    ----------
    name
        View name (e.g., "ast_calls").
    base_table
        Base table or view name for the projection.
    comment
        Optional documentation comment.
    include_file_identity
        Include file_id and path columns.
    include_file_sha256
        Include file_sha256 column.
    passthrough_cols
        Column names to pass through unchanged.
    transforms
        Custom column transformations.
    span_config
        Span field configuration (None to skip span fields).
    attrs_extracts
        Attrs map extractions as (key, dtype) tuples.
    unnest_column
        Column to unnest before projection.
    """

    name: str
    base_table: str
    comment: str = ""
    include_file_identity: bool = True
    include_file_sha256: bool = False
    passthrough_cols: tuple[str, ...] = ()
    transforms: tuple[ColumnTransform, ...] = ()
    span_config: SpanFieldConfig | None = None
    attrs_extracts: tuple[tuple[str, str], ...] = ()  # (key, dtype)
    unnest_column: str | None = None

    def to_exprs(self, base_schema: pa.Schema | None = None) -> tuple[Expr, ...]:
        """Convert specification to DataFusion expressions.

        Parameters
        ----------
        base_schema
            Optional base schema for column validation.

        Returns
        -------
        tuple[Expr, ...]
            DataFusion select expressions.
        """
        schema_names = set(base_schema.names) if base_schema is not None else None
        exprs = _build_identity_exprs(self, schema_names)
        exprs.extend(_build_passthrough_exprs(self, schema_names))
        exprs.extend(_build_attrs_exprs(self, schema_names))
        if self.span_config is not None:
            exprs.extend(_build_span_exprs(self.span_config, schema_names))
        exprs.extend(_build_transform_exprs(self.transforms, schema_names))
        return tuple(exprs)


# ---------------------------------------------------------------------------
# Expression Helpers (migrated from registry.py)
# ---------------------------------------------------------------------------


def _arrow_cast(expr: Expr, data_type: str) -> Expr:
    # Cast expression to Arrow type.
    return f.arrow_cast(expr, lit(data_type))


def _span_struct(span: Expr) -> Expr:
    # Build a span struct from span column.
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
    # Build an ast_record composite struct.
    return f.named_struct([("span", _span_struct(span)), ("attrs", attrs)])


def _has_col(name: str, schema_names: set[str] | None) -> bool:
    # Check if a column exists in schema.
    return schema_names is None or name in schema_names


# ---------------------------------------------------------------------------
# Expression Builder Helpers (refactored from ViewProjectionSpec methods)
# ---------------------------------------------------------------------------


def _build_identity_exprs(spec: ViewProjectionSpec, schema_names: set[str] | None) -> list[Expr]:
    # Build file identity expressions.
    exprs: list[Expr] = []
    if spec.include_file_identity:
        if _has_col("file_id", schema_names):
            exprs.append(col("file_id").alias("file_id"))
        if _has_col("path", schema_names):
            exprs.append(col("path").alias("path"))
    if spec.include_file_sha256 and _has_col("file_sha256", schema_names):
        exprs.append(col("file_sha256").alias("file_sha256"))
    return exprs


def _build_passthrough_exprs(spec: ViewProjectionSpec, schema_names: set[str] | None) -> list[Expr]:
    # Build passthrough column expressions.
    return [
        col(col_name).alias(col_name)
        for col_name in spec.passthrough_cols
        if _has_col(col_name, schema_names)
    ]


def _build_attrs_exprs(spec: ViewProjectionSpec, schema_names: set[str] | None) -> list[Expr]:
    # Build attrs map extraction expressions.
    if not _has_col("attrs", schema_names):
        return []
    return [
        _arrow_cast(list_extract(map_extract(col("attrs"), key), 1), dtype).alias(key)
        for key, dtype in spec.attrs_extracts
    ]


def _build_span_exprs(config: SpanFieldConfig, schema_names: set[str] | None) -> list[Expr]:
    # Generate span field expressions.
    if not _has_col(config.span_col, schema_names):
        return []

    exprs: list[Expr] = []
    span_col = col(config.span_col)

    if config.include_legacy_fields:
        exprs.extend(_build_legacy_span_fields(span_col))

    if config.include_line_base:
        exprs.append(_arrow_cast(lit(config.line_base_value), "Int32").alias("line_base"))
    if config.include_col_unit:
        exprs.append(_arrow_cast(span_col["col_unit"], "Utf8").alias("col_unit"))
    if config.include_end_exclusive:
        exprs.append(_arrow_cast(span_col["end_exclusive"], "Boolean").alias("end_exclusive"))
    if config.include_span_struct:
        exprs.append(_span_struct(span_col).alias("span"))

    if config.include_ast_record and _has_col(config.attrs_col, schema_names):
        attrs_col = col(config.attrs_col)
        exprs.append(_ast_record(span_col, attrs_col).alias("ast_record"))
        exprs.append(col(config.attrs_col).alias("attrs"))

    return exprs


def _build_legacy_span_fields(span_col: Expr) -> list[Expr]:
    # Build legacy lineno/col_offset fields with +1 adjustment.
    return [
        _arrow_cast(span_col["start"]["line0"] + lit(1), "Int64").alias("lineno"),
        _arrow_cast(span_col["start"]["col"], "Int64").alias("col_offset"),
        _arrow_cast(span_col["end"]["line0"] + lit(1), "Int64").alias("end_lineno"),
        _arrow_cast(span_col["end"]["col"], "Int64").alias("end_col_offset"),
    ]


def _build_transform_exprs(
    transforms: tuple[ColumnTransform, ...], schema_names: set[str] | None
) -> list[Expr]:
    # Build expressions from custom transforms.
    exprs: list[Expr] = []
    for transform in transforms:
        expr = _transform_to_expr(transform, schema_names)
        if expr is not None:
            exprs.append(expr)
    return exprs


def _transform_to_expr(transform: ColumnTransform, schema_names: set[str] | None) -> Expr | None:
    # Convert a single transform to a DataFusion expression.
    if transform.source_col and not _has_col(transform.source_col, schema_names):
        return None

    handler = _TRANSFORM_HANDLERS.get(transform.kind)
    if handler is None:
        return None
    return handler(transform)


# ---------------------------------------------------------------------------
# Transform Dispatch Handlers
# ---------------------------------------------------------------------------


def _handle_passthrough(t: ColumnTransform) -> Expr:
    return col(t.source_col).alias(t.alias)


def _handle_rename(t: ColumnTransform) -> Expr:
    return col(t.source_col).alias(t.alias)


def _handle_cast(t: ColumnTransform) -> Expr:
    expr = col(t.source_col)
    if t.adjust_expr is not None:
        expr += lit(t.adjust_expr)
    return _arrow_cast(expr, t.cast_type or "Utf8").alias(t.alias)


def _handle_struct_field(t: ColumnTransform) -> Expr:
    return col(t.source_col)[t.struct_path[0]].alias(t.alias)


def _handle_nested_struct(t: ColumnTransform) -> Expr:
    expr = col(t.source_col)
    for field_name in t.struct_path:
        expr = expr[field_name]
    if t.cast_type:
        expr = _arrow_cast(expr, t.cast_type)
    if t.adjust_expr is not None:
        expr += lit(t.adjust_expr)
    return expr.alias(t.alias)


def _handle_map_extract(t: ColumnTransform) -> Expr | None:
    if t.map_key is None:
        return None
    expr = list_extract(map_extract(col(t.source_col), t.map_key), 1)
    if t.cast_type:
        expr = _arrow_cast(expr, t.cast_type)
    return expr.alias(t.alias)


def _handle_metadata_extract(t: ColumnTransform) -> Expr | None:
    if t.metadata_key is None:
        return None
    return arrow_metadata(col(t.source_col), t.metadata_key).alias(t.alias)


def _handle_coalesce(t: ColumnTransform) -> Expr:
    cols = [col(t.source_col), *[col(c) for c in t.fallback_cols]]
    return f.coalesce(*cols).alias(t.alias)


def _handle_literal(t: ColumnTransform) -> Expr:
    return lit(t.literal_value).alias(t.alias)


_TRANSFORM_HANDLERS: Final[dict[ColumnTransformKind, TransformHandler]] = {
    ColumnTransformKind.PASSTHROUGH: _handle_passthrough,
    ColumnTransformKind.RENAME: _handle_rename,
    ColumnTransformKind.CAST: _handle_cast,
    ColumnTransformKind.STRUCT_FIELD: _handle_struct_field,
    ColumnTransformKind.NESTED_STRUCT_FIELD: _handle_nested_struct,
    ColumnTransformKind.MAP_EXTRACT: _handle_map_extract,
    ColumnTransformKind.METADATA_EXTRACT: _handle_metadata_extract,
    ColumnTransformKind.COALESCE: _handle_coalesce,
    ColumnTransformKind.LITERAL: _handle_literal,
    # EXPRESSION kind returns None (requires custom handling)
}


__all__ = [
    "SPAN_FIELDS_MINIMAL",
    "SPAN_FIELDS_STANDARD",
    "SPAN_FIELDS_WITH_AST_RECORD",
    "ColumnTransform",
    "ColumnTransformKind",
    "SpanFieldConfig",
    "ViewProjectionSpec",
    "cast",
    "coalesce_col",
    "literal_col",
    "map_extract_col",
    "metadata_col",
    "passthrough",
    "rename",
    "struct_field",
]
