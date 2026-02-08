"""Projection builders for semantic catalog outputs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa

from semantics.types.annotated_schema import AnnotatedSchema
from semantics.types.core import CompatibilityGroup, SemanticType

if TYPE_CHECKING:
    from collections.abc import Callable

    from datafusion.dataframe import DataFrame
    from datafusion.expr import Expr


@dataclass(frozen=True)
class RelationOutputSpec:
    """Parameters for relation_output projection.

    Specifies how to transform a semantic relationship DataFrame into
    the canonical relation_output schema, mapping source and destination
    columns and providing constant values for edge classification.

    Attributes:
    ----------
    src_col
        Source column name to map to "src" in output.
    dst_col
        Destination column name to map to "dst" in output.
    kind
        Edge kind constant for this relationship type.
    origin
        Origin identifier (e.g., "cst", "scip").
    qname_source_col
        Optional column for qualified name source. When None, output is null.
    ambiguity_group_col
        Optional column for ambiguity group ID. When None, output is null.
    """

    src_col: str
    dst_col: str
    kind: str
    origin: str
    qname_source_col: str | None = None
    ambiguity_group_col: str | None = None


def _resolve_edge_owner(
    annotated: AnnotatedSchema,
    null_expr: Expr,
    *,
    coalesce: Callable[..., Expr],
) -> Expr:
    """Resolve the edge_owner expression from file-identity columns.

    Uses the FILE_IDENTITY compatibility group to find available file
    identity columns, then applies coalesce logic: prefer
    ``edge_owner_file_id`` with ``file_id`` as fallback.

    Parameters
    ----------
    annotated
        Annotated schema for the input DataFrame.
    null_expr
        Null string expression to use when no file identity column is available.
    coalesce
        DataFusion ``coalesce`` function reference.

    Returns:
    -------
    Expr
        Expression for the edge_owner_file_id output column.
    """
    from datafusion import col

    file_id_cols = {
        c.name for c in annotated.columns_by_compatibility_group(CompatibilityGroup.FILE_IDENTITY)
    }
    has_edge_owner = "edge_owner_file_id" in file_id_cols
    has_file_id = "file_id" in file_id_cols

    if has_edge_owner and has_file_id:
        return coalesce(col("edge_owner_file_id"), col("file_id"))
    if has_edge_owner:
        return col("edge_owner_file_id")
    if has_file_id:
        return col("file_id")
    return null_expr


def relation_output_projection(
    df: DataFrame,
    spec: RelationOutputSpec,
) -> DataFrame:
    """Project a semantic relationship DataFrame to relation_output schema.

    Transforms input columns according to the spec, adding constant values
    for kind and origin, and handling optional columns with null defaults.

    Parameters
    ----------
    df
        Input DataFrame with relationship data.
    spec
        Projection specification defining column mappings.

    Returns:
    -------
    DataFrame
        Projected DataFrame matching relation_output schema.
    """
    from datafusion import col, lit
    from datafusion import functions as f

    schema = df.schema()
    names = set(schema.names)
    annotated = AnnotatedSchema.from_arrow_schema(schema)
    null_str = lit(None).cast(pa.string())

    def _optional(name: str, dtype: pa.DataType, *, default: Expr | None = None) -> Expr:
        if name in names:
            return col(name).cast(dtype)
        if default is None:
            return lit(None).cast(dtype)
        return default.cast(dtype)

    edge_owner = _resolve_edge_owner(annotated, null_str, coalesce=f.coalesce)

    if "resolution_method" in names:
        resolution_method = col("resolution_method").cast(pa.string())
    elif annotated.has_semantic_type(SemanticType.ORIGIN):
        resolution_method = col("origin").cast(pa.string())
    else:
        resolution_method = null_str

    qname_source = (
        col(spec.qname_source_col).cast(pa.string())
        if spec.qname_source_col and spec.qname_source_col in names
        else null_str
    )
    ambiguity_group = (
        col(spec.ambiguity_group_col).cast(pa.string())
        if spec.ambiguity_group_col and spec.ambiguity_group_col in names
        else null_str
    )

    return df.select(
        col(spec.src_col).alias("src"),
        col(spec.dst_col).alias("dst"),
        col("path").alias("path"),
        edge_owner.alias("edge_owner_file_id"),
        col("bstart").alias("bstart"),
        col("bend").alias("bend"),
        lit(spec.origin).alias("origin"),
        resolution_method.alias("resolution_method"),
        _optional("binding_kind", pa.string()).alias("binding_kind"),
        _optional("def_site_kind", pa.string()).alias("def_site_kind"),
        _optional("use_kind", pa.string()).alias("use_kind"),
        lit(spec.kind).alias("kind"),
        _optional("reason", pa.string()).alias("reason"),
        _optional("confidence", pa.float64(), default=lit(0.5)).alias("confidence"),
        _optional("score", pa.float64(), default=lit(0.5)).alias("score"),
        _optional("symbol_roles", pa.int32()).alias("symbol_roles"),
        qname_source.alias("qname_source"),
        ambiguity_group.alias("ambiguity_group_id"),
        _optional("diag_source", pa.string()).alias("diag_source"),
        _optional("severity", pa.string()).alias("severity"),
        _optional("task_name", pa.string()).alias("task_name"),
        _optional("task_priority", pa.int32()).alias("task_priority"),
    )


__all__ = [
    "RelationOutputSpec",
    "relation_output_projection",
]
