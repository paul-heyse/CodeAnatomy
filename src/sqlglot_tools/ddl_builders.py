"""SQLGlot AST-first DDL/COPY generation for DataFusion SQL statements.

This module provides AST-based builders for DDL and DML statements,
replacing string assembly with structured SQLGlot expression building.
All builders produce SQLGlot expressions that can be rendered to
dialect-specific SQL.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlglot_tools.compat import exp

if TYPE_CHECKING:
    from ibis_engine.registry import DatasetLocation


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
    schema: dict[str, str] | None = None,
    file_format: str = "PARQUET",
    options: dict[str, str] | None = None,
    dialect: str | None = "datafusion",
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
    schema
        Explicit column definitions (name -> type).
    file_format
        File format (PARQUET, CSV, JSON, ARROW).
    options
        Format-specific options for the table provider.

    Returns
    -------
    str
        Rendered SQL DDL statement for postgres dialect.

    Examples
    --------
    >>> from ibis_engine.registry import DatasetLocation
    >>> location = DatasetLocation(path="/data/events.parquet")
    >>> ddl = build_external_table_ddl(
    ...     name="events",
    ...     location=location,
    ...     schema={"id": "BIGINT", "timestamp": "TIMESTAMP"},
    ...     file_format="PARQUET",
    ... )
    >>> print(ddl)
    CREATE EXTERNAL TABLE events (id BIGINT, timestamp TIMESTAMP) ...
    """
    # Build column definitions if schema provided
    columns = None
    if schema:
        columns = exp.Schema(
            expressions=[
                exp.ColumnDef(
                    this=exp.to_identifier(col_name),
                    kind=exp.DataType.build(col_type),
                )
                for col_name, col_type in schema.items()
            ]
        )

    # Build CREATE EXTERNAL TABLE expression
    create_expr = exp.Create(
        this=exp.Table(this=exp.to_identifier(name)),
        kind="EXTERNAL TABLE",
        expression=columns,
        properties=exp.Properties(
            expressions=[
                exp.FileFormatProperty(this=exp.Var(this=file_format)),
                exp.LocationProperty(this=exp.Literal.string(str(location.path))),
            ]
        ),
    )

    # Add OPTIONS if provided
    if options:
        option_props = [
            exp.Property(
                this=exp.Literal.string(k),
                value=exp.Literal.string(v),
            )
            for k, v in options.items()
        ]
        create_expr.args["properties"].append(exp.SettingsProperty(expressions=option_props))

    # Render with appropriate dialect (default to datafusion)
    dialect_name = dialect or "datafusion"
    if dialect_name in {"datafusion", "datafusion_ext"}:
        from sqlglot_tools.optimizer import register_datafusion_dialect

        register_datafusion_dialect()
    return create_expr.sql(dialect=dialect_name)


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
