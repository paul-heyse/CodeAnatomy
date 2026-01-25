"""SQLGlot AST-first DDL/COPY generation for DataFusion SQL statements.

This module provides AST-based builders for DDL and DML statements,
replacing string assembly with structured SQLGlot expression building.
All builders produce SQLGlot expressions that can be rendered to
dialect-specific SQL.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlglot_tools.compat import exp
from sqlglot_tools.optimizer import (
    ExternalTableCompressionProperty,
    ExternalTableOptionsProperty,
    ExternalTableOrderProperty,
)

if TYPE_CHECKING:
    from ibis_engine.registry import DatasetLocation


@dataclass(frozen=True)
class ExternalTableDDLConfig:
    """Configuration for external table DDL generation.

    Parameters
    ----------
    schema
        Explicit column definitions (name -> type).
    schema_expressions
        SQLGlot schema expressions (ColumnDef + constraints) when available.
    file_format
        File format (PARQUET, CSV, JSON, ARROW).
    options
        Format-specific options for the table provider.
    compression
        Compression type for external table registration.
    partitioned_by
        Partition columns for hive-style partitioning.
    file_sort_order
        File sort order columns for DataFusion listing tables.
    unbounded
        Whether to mark the table as UNBOUNDED EXTERNAL (streaming).
    dialect
        SQL dialect name for rendering.
    """

    schema: dict[str, str] | None = None
    schema_expressions: list[exp.Expression] | None = None
    file_format: str = "PARQUET"
    options: dict[str, str] | None = None
    compression: str | None = None
    partitioned_by: tuple[str, ...] | None = None
    file_sort_order: tuple[str, ...] | None = None
    unbounded: bool = False
    dialect: str | None = "datafusion"


def build_copy_to_ast(
    *,
    query: exp.Expression,
    path: str,
    file_format: str | None = "PARQUET",
    options: dict[str, object] | None = None,
    partition_by: tuple[str, ...] = (),
) -> exp.Copy:
    r"""
    Build COPY TO statement as SQLGlot AST.

    Constructs a COPY TO expression for writing query results to a file
    or object store location. Supports format-specific options, compression
    settings, and Hive-style partitioning.

    Parameters
    ----------
    query
        The source query expression to copy from.
    path
        Destination path (local filesystem or object store URI).
    file_format
        Output format (PARQUET, CSV, JSON, ARROW).
    options
        Format-specific options (compression, delimiter, etc.).
    partition_by
        Columns for Hive-style partitioning.

    Returns
    -------
    exp.Copy
        SQLGlot COPY expression ready for dialect rendering.

    Examples
    --------
    >>> from sqlglot_tools.compat import parse_one
    >>> query = parse_one("SELECT * FROM mytable")
    >>> copy_expr = build_copy_to_ast(
    ...     query=query,
    ...     path="/tmp/output.parquet",
    ...     file_format="PARQUET",
    ...     options={"compression": "zstd"},
    ...     partition_by=("year", "month"),
    ... )
    >>> copy_expr.sql(dialect="postgres")
    'COPY (SELECT * FROM mytable) TO \'/tmp/output.parquet\' ...'
    """
    option_exprs: list[exp.Expression] = []

    # Format option (omit if None)
    if file_format:
        option_exprs.append(
            exp.Property(this=exp.Var(this="FORMAT"), value=exp.Var(this=file_format))
        )

    # User-provided options (ignore explicit format override)
    for key, value in (options or {}).items():
        if str(key).lower() == "format":
            continue
        option_exprs.append(
            exp.Property(
                this=exp.Var(this=key.upper()),
                value=exp.Literal.string(str(value)),
            )
        )

    copy_expr = exp.Copy(
        this=exp.Subquery(this=query),
        kind=exp.Var(this="TO"),
        files=[exp.Literal.string(path)],
        options=option_exprs,
    )

    # Add PARTITIONED BY if specified
    if partition_by:
        copy_expr.set(
            "partition",
            [exp.to_identifier(col) for col in partition_by],
        )

    return copy_expr


def build_external_table_ddl(
    *,
    name: str,
    location: DatasetLocation,
    config: ExternalTableDDLConfig | None = None,
) -> str:
    """
    Build CREATE EXTERNAL TABLE DDL via SQLGlot AST.

    Constructs a CREATE EXTERNAL TABLE statement for registering
    external datasets in DataFusion. Supports schema inference,
    explicit column definitions, and format-specific options.

    Parameters
    ----------
    name
        Table name for registration.
    location
        Dataset location metadata including path and storage options.
    config
        Configuration for schema, format, options, and SQL dialect.

    Returns
    -------
    str
        Rendered SQL DDL statement for the configured dialect.

    Examples
    --------
    >>> from ibis_engine.registry import DatasetLocation
    >>> location = DatasetLocation(path="/data/events.parquet")
    >>> config = ExternalTableDDLConfig(
    ...     schema={"id": "BIGINT", "timestamp": "TIMESTAMP"},
    ...     file_format="PARQUET",
    ... )
    >>> ddl = build_external_table_ddl(name="events", location=location, config=config)
    >>> print(ddl)
    CREATE EXTERNAL TABLE events (id BIGINT, timestamp TIMESTAMP) ...
    """
    ddl_config = config or ExternalTableDDLConfig()
    schema = ddl_config.schema
    schema_expressions = ddl_config.schema_expressions
    file_format = ddl_config.file_format
    options = ddl_config.options
    compression = ddl_config.compression
    partitioned_by = ddl_config.partitioned_by
    file_sort_order = ddl_config.file_sort_order
    unbounded = ddl_config.unbounded

    # Build column definitions if schema provided
    columns = None
    if schema_expressions:
        columns = exp.Schema(expressions=list(schema_expressions))
    elif schema:
        columns = exp.Schema(
            expressions=[
                exp.ColumnDef(
                    this=exp.to_identifier(col_name),
                    kind=exp.DataType.build(col_type),
                )
                for col_name, col_type in schema.items()
            ]
        )

    stored_as = "DELTATABLE" if file_format.lower() == "delta" else file_format.upper()
    properties: list[exp.Expression] = [
        exp.FileFormatProperty(this=exp.Var(this=stored_as)),
        exp.LocationProperty(this=exp.Literal.string(str(location.path))),
    ]
    if compression:
        properties.append(ExternalTableCompressionProperty(this=exp.Var(this=compression)))
    if partitioned_by:
        properties.append(
            exp.PartitionedByProperty(
                this=exp.Tuple(
                    expressions=[exp.Identifier(this=name) for name in partitioned_by]
                )
            )
        )
    if file_sort_order:
        properties.append(
            ExternalTableOrderProperty(
                expressions=[exp.Identifier(this=name) for name in file_sort_order]
            )
        )
    options_property = _external_table_options_property(options)
    if options_property is not None:
        properties.append(options_property)

    # Build CREATE EXTERNAL TABLE expression
    create_expr = exp.Create(
        this=exp.Table(this=exp.to_identifier(name)),
        kind="UNBOUNDED EXTERNAL" if unbounded else "EXTERNAL",
        expression=columns,
        properties=exp.Properties(expressions=properties),
    )

    # Render with appropriate dialect (default to datafusion)
    dialect_name = ddl_config.dialect or "datafusion"
    if dialect_name in {"datafusion", "datafusion_ext"}:
        from sqlglot_tools.optimizer import register_datafusion_dialect

        register_datafusion_dialect()
    return create_expr.sql(dialect=dialect_name)


def _external_table_options_property(
    options: dict[str, str] | None,
) -> ExternalTableOptionsProperty | None:
    if not options:
        return None
    entries: list[exp.Expression] = []
    for key, value in options.items():
        rendered = _option_literal_value(value)
        if rendered is None:
            continue
        entries.append(
            exp.Tuple(
                expressions=[
                    exp.Literal.string(str(key)),
                    exp.Literal.string(rendered),
                ]
            )
        )
    if not entries:
        return None
    return ExternalTableOptionsProperty(expressions=entries)


def _option_literal_value(value: object) -> str | None:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value
    return None


def build_insert_into_ast(
    *,
    target_table: str,
    source_query: exp.Expression,
    columns: tuple[str, ...] | None = None,
    overwrite: bool = False,
) -> exp.Insert:
    """
    Build INSERT INTO statement as SQLGlot AST.

    Constructs an INSERT INTO expression for loading query results
    into a target table. Supports column list specification and
    overwrite mode.

    Parameters
    ----------
    target_table
        Name of the target table.
    source_query
        Source query expression to insert from.
    columns
        Optional column list for targeted insertion.
    overwrite
        If True, replace existing table contents (INSERT OVERWRITE).

    Returns
    -------
    exp.Insert
        SQLGlot INSERT expression ready for dialect rendering.

    Examples
    --------
    >>> from sqlglot_tools.compat import parse_one
    >>> source = parse_one("SELECT id, name FROM staging")
    >>> insert_expr = build_insert_into_ast(
    ...     target_table="production",
    ...     source_query=source,
    ...     columns=("id", "name"),
    ... )
    >>> insert_expr.sql(dialect="postgres")
    'INSERT INTO production (id, name) SELECT id, name FROM staging'
    """
    insert_expr = exp.Insert(
        this=exp.Table(this=exp.to_identifier(target_table)),
        expression=source_query,
        overwrite=overwrite,
    )

    if columns:
        insert_expr.set(
            "columns",
            [exp.to_identifier(col) for col in columns],
        )

    return insert_expr


__all__ = [
    "build_copy_to_ast",
    "build_external_table_ddl",
    "build_insert_into_ast",
]
