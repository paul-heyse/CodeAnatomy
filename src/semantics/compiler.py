"""Semantic compiler - transforms extraction tables to CPG outputs.

The compiler encodes the 10 semantic rules:

| # | Name              | Condition               | Operation                          |
|---|-------------------|-------------------------|------------------------------------|
| 1 | Derive Entity ID  | PATH + SPAN             | stable_id(prefix, path, bstart, bend) |
| 2 | Derive Span       | SPAN_START + SPAN_END   | span_make(bstart, bend, "byte")    |
| 3 | Normalize Text    | TEXT columns            | utf8_normalize(text, "NFC")        |
| 4 | Path Join         | both have PATH          | equijoin on path                   |
| 5 | Span Overlap      | both EVIDENCE           | filter span_overlaps(a, b)         |
| 6 | Span Contains     | both EVIDENCE           | filter span_contains(a, b)         |
| 7 | Relation Project  | ENTITY_ID + SYMBOL      | project to relation schema         |
| 8 | Union             | compatible schemas      | union with discriminator           |
| 9 | Aggregate         | GROUP_KEY + VALUES      | array_agg(values)                  |
| 10| Dedupe            | KEY + SCORE             | keep best per key                  |

Usage
-----
>>> compiler = SemanticCompiler(ctx)
>>> ctx.register_view("refs_norm", compiler.normalize("cst_refs", prefix="ref"))
>>> ctx.register_view(
...     "rel_name_symbol",
...     compiler.relate(
...         "refs_norm",
...         "scip_occurrences",
...         join_type="overlap",
...         origin="cst_ref",
...     ),
... )
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import pyarrow as pa

from semantics.config import SemanticConfig
from semantics.join_helpers import join_by_span_contains, join_by_span_overlap
from semantics.schema import SemanticSchema, SemanticSchemaError

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext
    from datafusion.expr import Expr

    from semantics.specs import SemanticTableSpec


CANONICAL_NODE_SCHEMA: tuple[tuple[str, pa.DataType], ...] = (
    ("entity_id", pa.string()),
    ("path", pa.string()),
    ("bstart", pa.int64()),
    ("bend", pa.int64()),
    ("file_id", pa.string()),
    ("file_sha256", pa.string()),
)

CANONICAL_EDGE_SCHEMA: tuple[tuple[str, pa.DataType], ...] = (
    ("entity_id", pa.string()),
    ("symbol", pa.string()),
    ("path", pa.string()),
    ("bstart", pa.int64()),
    ("bend", pa.int64()),
    ("origin", pa.string()),
)


@dataclass
class TableInfo:
    """Analyzed table with semantic information.

    Attributes
    ----------
    name
        Table name in the session context.
    df
        DataFrame handle.
    sem
        Semantic schema analysis.
    """

    name: str
    df: DataFrame
    sem: SemanticSchema

    @classmethod
    def analyze(
        cls,
        name: str,
        df: DataFrame,
        *,
        config: SemanticConfig | None = None,
    ) -> TableInfo:
        """Analyze a DataFrame and wrap with semantic info.

        Parameters
        ----------
        name
            Table name.
        df
            DataFrame to analyze.
        config
            Optional semantic configuration overrides.

        Returns
        -------
        TableInfo
            Analyzed table.
        """
        return cls(
            name=name,
            df=df,
            sem=SemanticSchema.from_df(df, table_name=name, config=config),
        )


class SemanticCompiler:
    """Compiles semantic operations to DataFusion plans.

    The compiler maintains a registry of analyzed tables and provides
    high-level operations that apply semantic rules.
    """

    def __init__(self, ctx: SessionContext, *, config: SemanticConfig | None = None) -> None:
        """Initialize the compiler.

        Parameters
        ----------
        ctx
            DataFusion session context with registered tables.
        config
            Optional semantic configuration overrides.
        """
        self.ctx = ctx
        self._tables: dict[str, TableInfo] = {}
        self._udf_snapshot: dict[str, object] | None = None
        self._config = config or SemanticConfig()

    def _require_udfs(self, required: tuple[str, ...]) -> None:
        from datafusion_engine.udf.runtime import rust_udf_snapshot, validate_required_udfs

        if not required:
            return
        if self._udf_snapshot is None:
            self._udf_snapshot = dict(rust_udf_snapshot(self.ctx))
        validate_required_udfs(self._udf_snapshot, required=required)

    def _spec_for_table(self, table_name: str) -> SemanticTableSpec | None:
        config_spec = self._config.spec_for(table_name)
        if config_spec is not None:
            return config_spec
        from semantics.spec_registry import spec_for_table

        return spec_for_table(table_name)

    @staticmethod
    def _schema_names(df: DataFrame) -> tuple[str, ...]:
        schema = df.schema()
        if hasattr(schema, "names"):
            return tuple(schema.names)
        return tuple(field.name for field in schema)

    @staticmethod
    def _prefix_df(df: DataFrame, prefix: str) -> DataFrame:
        from datafusion import col

        names = SemanticCompiler._schema_names(df)
        exprs = [col(name).alias(f"{prefix}{name}") for name in names]
        return df.select(*exprs)

    @staticmethod
    def _ensure_columns_present(
        names: set[str],
        required: tuple[str, ...],
        *,
        table_name: str,
    ) -> None:
        missing = [name for name in required if name and name not in names]
        if missing:
            msg = f"Table {table_name!r} missing required columns: {sorted(missing)!r}."
            raise SemanticSchemaError(msg)

    @staticmethod
    def _null_guard(
        expr: Expr,
        *,
        columns: tuple[str, ...],
    ) -> Expr:
        from datafusion import col, lit
        from datafusion import functions as f

        if not columns:
            return expr
        condition = None
        for name in columns:
            if not name:
                continue
            clause = col(name).is_null()
            condition = clause if condition is None else condition | clause
        if condition is None:
            return expr
        return f.when(condition, lit(None)).otherwise(expr)

    def _project_to_schema(
        self,
        df: DataFrame,
        schema: tuple[tuple[str, pa.DataType], ...],
    ) -> DataFrame:
        from datafusion import col, lit

        names = set(self._schema_names(df))
        exprs: list[Expr] = []
        for name, dtype in schema:
            if name in names:
                exprs.append(col(name).cast(dtype).alias(name))
            else:
                exprs.append(lit(None).cast(dtype).alias(name))
        return df.select(*exprs)

    def _union_canonical(
        self,
        table_names: list[str],
        *,
        discriminator: str,
        schema: tuple[tuple[str, pa.DataType], ...],
    ) -> DataFrame:
        from datafusion import col, lit

        if not table_names:
            msg = "Union requires at least one table."
            raise SemanticSchemaError(msg)

        dfs: list[DataFrame] = []
        for name in table_names:
            info = self.get(name)
            projected = self._project_to_schema(info.df, schema)
            dfs.append(projected.with_column(discriminator, lit(name)))

        result = dfs[0]
        for df in dfs[1:]:
            result = result.union(df)

        ordered = [name for name, _dtype in schema] + [discriminator]
        return result.select(*[col(name).alias(name) for name in ordered])

    @staticmethod
    def _require_span_unit_compatibility(
        left_sem: SemanticSchema,
        right_sem: SemanticSchema,
        *,
        left_table: str,
        right_table: str,
    ) -> None:
        left_unit = left_sem.require_span_unit(table=left_table)
        right_unit = right_sem.require_span_unit(table=right_table)
        if left_unit != right_unit:
            msg = (
                "Span unit mismatch for join: "
                f"{left_table!r}({left_unit}) vs {right_table!r}({right_unit})."
            )
            raise SemanticSchemaError(msg)

    def register(self, name: str) -> TableInfo:
        """Register and analyze a table.

        Parameters
        ----------
        name
            Table name in the session context.

        Returns
        -------
        TableInfo
            Analyzed table information.
        """
        df = self.ctx.table(name)
        info = TableInfo.analyze(name, df, config=self._config)
        self._tables[name] = info
        return info

    def get(self, name: str) -> TableInfo:
        """Get or analyze a table.

        Parameters
        ----------
        name
            Table name.

        Returns
        -------
        TableInfo
            Table information.
        """
        if name not in self._tables:
            return self.register(name)
        return self._tables[name]

    # -------------------------------------------------------------------------
    # Rule 1 + 2: Normalization
    # -------------------------------------------------------------------------

    def normalize_from_spec(self, spec: SemanticTableSpec) -> DataFrame:
        """Apply normalization rules using an explicit table spec."""
        from datafusion import col

        from datafusion_engine.udf.shims import span_make, stable_id_parts

        self._require_udfs(("stable_id_parts", "span_make"))
        info = self.get(spec.table)
        df = info.df
        names = set(self._schema_names(df))

        required: set[str] = {
            spec.path_col,
            spec.primary_span.start_col,
            spec.primary_span.end_col,
            spec.entity_id.path_col,
            spec.entity_id.start_col,
            spec.entity_id.end_col,
        }
        for key in spec.foreign_keys:
            required.update({key.path_col, key.start_col, key.end_col})
            required.update(key.guard_null_if)
        required.update(spec.text_cols)
        required_tuple = tuple(name for name in required if name)
        self._ensure_columns_present(names, required_tuple, table_name=spec.table)

        if spec.path_col != "path":
            df = df.with_column("path", col(spec.path_col))

        df = df.with_column(spec.primary_span.canonical_start, col(spec.primary_span.start_col))
        df = df.with_column(spec.primary_span.canonical_end, col(spec.primary_span.end_col))
        df = df.with_column(
            spec.primary_span.canonical_span,
            span_make(
                col(spec.primary_span.canonical_start),
                col(spec.primary_span.canonical_end),
            ),
        )
        if spec.primary_span.canonical_start != "bstart":
            df = df.with_column("bstart", col(spec.primary_span.canonical_start))
        if spec.primary_span.canonical_end != "bend":
            df = df.with_column("bend", col(spec.primary_span.canonical_end))

        id_expr = stable_id_parts(
            spec.entity_id.namespace,
            col(spec.entity_id.path_col),
            col(spec.entity_id.start_col),
            col(spec.entity_id.end_col),
        )
        if spec.entity_id.null_if_any_null:
            id_expr = self._null_guard(
                id_expr,
                columns=(
                    spec.entity_id.path_col,
                    spec.entity_id.start_col,
                    spec.entity_id.end_col,
                ),
            )
        df = df.with_column(spec.entity_id.out_col, id_expr)
        if spec.entity_id.canonical_entity_id is not None:
            df = df.with_column(spec.entity_id.canonical_entity_id, col(spec.entity_id.out_col))

        for foreign_key in spec.foreign_keys:
            fk_expr = stable_id_parts(
                foreign_key.target_namespace,
                col(foreign_key.path_col),
                col(foreign_key.start_col),
                col(foreign_key.end_col),
            )
            guard_cols: list[str] = []
            if foreign_key.null_if_any_null:
                guard_cols.extend(
                    [
                        foreign_key.path_col,
                        foreign_key.start_col,
                        foreign_key.end_col,
                    ]
                )
            guard_cols.extend(foreign_key.guard_null_if)
            fk_expr = self._null_guard(fk_expr, columns=tuple(guard_cols))
            df = df.with_column(foreign_key.out_col, fk_expr)

        return df

    def normalize(self, table_name: str, *, prefix: str) -> DataFrame:
        """Apply normalization rules to an evidence table.

        Rule 1: IF PATH + SPAN THEN derive entity_id
        Rule 2: IF SPAN_START + SPAN_END THEN derive span struct

        Parameters
        ----------
        table_name
            Name of extraction table.
        prefix
            Entity type prefix (e.g., "ref", "def").

        Returns
        -------
        DataFrame
            Normalized DataFrame with entity_id and span columns.
        """
        from datafusion import col

        spec = self._spec_for_table(table_name)
        if spec is not None:
            return self.normalize_from_spec(spec)

        info = self.get(table_name)
        sem = info.sem
        if sem._has_ambiguous_span():
            msg = (
                f"Table {table_name!r} has ambiguous span columns: "
                f"start={sem._span_start_candidates()!r}, end={sem._span_end_candidates()!r}. "
                "Provide a SemanticTableSpec to select the primary span."
            )
            raise SemanticSchemaError(msg)
        sem.require_evidence(table=table_name)
        self._require_udfs(("stable_id_parts", "span_make"))

        df = info.df.with_column(f"{prefix}_id", sem.entity_id_expr(prefix))
        df = df.with_column("entity_id", col(f"{prefix}_id"))
        if sem.span_start_name() != "bstart":
            df = df.with_column("bstart", sem.span_start_col())
        if sem.span_end_name() != "bend":
            df = df.with_column("bend", sem.span_end_col())
        df = df.with_column("span", sem.span_expr())
        return df

    # -------------------------------------------------------------------------
    # Rule 3: Text normalization
    # -------------------------------------------------------------------------

    def normalize_text(
        self,
        table_name: str,
        *,
        columns: list[str] | None = None,
        form: str = "NFC",
        casefold: bool = False,
        collapse_ws: bool = False,
        output_suffix: str = "_norm",
    ) -> DataFrame:
        """Apply text normalization to text columns.

        Rule 3: IF TEXT columns THEN utf8_normalize(text, form)

        Parameters
        ----------
        table_name
            Table name.
        columns
            Specific columns to normalize. If None, normalizes all TEXT columns.
        form
            Unicode normalization form.
        casefold
            Whether to casefold text.
        collapse_ws
            Whether to collapse whitespace.
        output_suffix
            Suffix for normalized columns.

        Returns
        -------
        DataFrame
            DataFrame with normalized text columns.
        """
        from datafusion import col

        from datafusion_engine.udf.shims import utf8_normalize

        info = self.get(table_name)
        self._require_udfs(("utf8_normalize",))
        df = info.df

        if output_suffix == "":
            msg = "Text normalization output_suffix must be non-empty."
            raise ValueError(msg)

        spec = self._spec_for_table(table_name)
        if columns is None:
            if spec is not None and spec.text_cols:
                target_cols = list(spec.text_cols)
            else:
                target_cols = list(info.sem.text_names())
        else:
            target_cols = list(columns)

        if not target_cols:
            return df

        names = set(self._schema_names(df))
        for col_name in target_cols:
            if col_name not in names:
                msg = f"Table {table_name!r} missing text column {col_name!r}."
                raise SemanticSchemaError(msg)
            output_name = f"{col_name}{output_suffix}"
            if output_name in names:
                msg = f"Table {table_name!r} already has column {output_name!r}."
                raise SemanticSchemaError(msg)
            df = df.with_column(
                output_name,
                utf8_normalize(
                    col(col_name),
                    form=form,
                    casefold=casefold,
                    collapse_ws=collapse_ws,
                ),
            )
            names.add(output_name)

        return df

    # -------------------------------------------------------------------------
    # Rules 5, 6, 7: Relationship building
    # -------------------------------------------------------------------------

    def relate(
        self,
        left_table: str,
        right_table: str,
        *,
        join_type: Literal["overlap", "contains"] = "overlap",
        filter_sql: str | None = None,
        origin: str,
    ) -> DataFrame:
        """Build a relationship between two tables.

        Rules applied:
        - Rule 5 or 6: Join by span overlap/containment
        - Rule 7: Project to relation schema

        Parameters
        ----------
        left_table
            Entity table (must have entity_id).
        right_table
            Symbol table (must have symbol).
        join_type
            "overlap" for span_overlaps, "contains" for span_contains.
        filter_sql
            Optional SQL filter expression.
        origin
            Origin label for the relationship.

        Returns
        -------
        DataFrame
            Relationship table with schema:
            (entity_id, symbol, path, bstart, bend, origin)
        """
        from datafusion import lit

        left_info = self.get(left_table)
        right_info = self.get(right_table)
        left_info.sem.require_entity(table=left_table)
        right_info.sem.require_symbol_source(table=right_table)
        self._require_span_unit_compatibility(
            left_info.sem,
            right_info.sem,
            left_table=left_table,
            right_table=right_table,
        )
        self._require_udfs(("span_overlaps", "span_contains"))

        left_prefix = "left__"
        right_prefix = "right__"
        left_df = self._prefix_df(left_info.df, left_prefix)
        right_df = self._prefix_df(right_info.df, right_prefix)
        left_sem = left_info.sem.prefixed(left_prefix)
        right_sem = right_info.sem.prefixed(right_prefix)

        # Apply Rule 5 or 6: span-based join
        if join_type == "overlap":
            joined = join_by_span_overlap(left_df, right_df, left_sem, right_sem)
        else:
            joined = join_by_span_contains(left_df, right_df, left_sem, right_sem)

        # Apply optional filter
        if filter_sql:
            joined = joined.filter(filter_sql)

        # Rule 7: Project to relation schema
        return joined.select(
            left_sem.entity_id_col().alias("entity_id"),
            right_sem.symbol_col().alias("symbol"),
            left_sem.path_col().alias("path"),
            left_sem.span_start_col().alias("bstart"),
            left_sem.span_end_col().alias("bend"),
            lit(origin).alias("origin"),
        ).distinct()

    # -------------------------------------------------------------------------
    # Rule 8: Union with discriminator
    # -------------------------------------------------------------------------

    def union_with_discriminator(
        self,
        table_names: list[str],
        *,
        discriminator: str = "kind",
    ) -> DataFrame:
        """Union tables with a discriminator column.

        Rule 8: IF tables have compatible schemas THEN union with discriminator.

        Parameters
        ----------
        table_names
            List of table names to union.
        discriminator
            Name of the discriminator column.

        Returns
        -------
        DataFrame
            Unioned DataFrame with discriminator column.

        Raises
        ------
        SemanticSchemaError
            Raised when the union inputs are invalid or incompatible.
        """
        from datafusion import lit

        if not table_names:
            msg = "Union requires at least one table."
            raise SemanticSchemaError(msg)

        dfs: list[DataFrame] = []
        for name in table_names:
            info = self.get(name)
            dfs.append(info.df.with_column(discriminator, lit(name)))

        first_schema = dfs[0].schema()
        columns = (
            tuple(first_schema.names)
            if hasattr(first_schema, "names")
            else tuple(field.name for field in first_schema)
        )

        for df in dfs:
            schema = df.schema()
            current = (
                tuple(schema.names)
                if hasattr(schema, "names")
                else tuple(field.name for field in schema)
            )
            if current != columns:
                msg = "Union inputs have mismatched schemas."
                raise SemanticSchemaError(msg)

        from datafusion import col

        projected = [df.select(*[col(name).alias(name) for name in columns]) for df in dfs]
        result = projected[0]
        for df in projected[1:]:
            result = result.union(df)

        return result

    def union_nodes(
        self,
        table_names: list[str],
        *,
        discriminator: str = "node_kind",
    ) -> DataFrame:
        """Union node tables after canonical projection."""
        return self._union_canonical(
            table_names,
            discriminator=discriminator,
            schema=CANONICAL_NODE_SCHEMA,
        )

    def union_edges(
        self,
        table_names: list[str],
        *,
        discriminator: str = "edge_kind",
    ) -> DataFrame:
        """Union edge tables after canonical projection."""
        return self._union_canonical(
            table_names,
            discriminator=discriminator,
            schema=CANONICAL_EDGE_SCHEMA,
        )

    # -------------------------------------------------------------------------
    # Rule 9: Aggregation
    # -------------------------------------------------------------------------

    def aggregate(
        self,
        table_name: str,
        *,
        group_by: list[str],
        aggregate: dict[str, str],
    ) -> DataFrame:
        """Aggregate with array collection.

        Rule 9: IF GROUP_KEY + VALUES THEN array_agg(values).

        Parameters
        ----------
        table_name
            Table to aggregate.
        group_by
            Columns to group by.
        aggregate
            Mapping of output column name to input column for array_agg.

        Returns
        -------
        DataFrame
            Aggregated DataFrame.
        """
        from datafusion import col, functions

        info = self.get(table_name)
        df = info.df

        # Build aggregation expressions
        agg_exprs = []
        for output_name, input_col in aggregate.items():
            agg_exprs.append(functions.array_agg(col(input_col)).alias(output_name))

        return df.aggregate([col(c) for c in group_by], agg_exprs)

    # -------------------------------------------------------------------------
    # Rule 10: Deduplication
    # -------------------------------------------------------------------------

    def dedupe(
        self,
        table_name: str,
        *,
        key_columns: list[str],
        score_column: str,
        descending: bool = True,
    ) -> DataFrame:
        """Deduplicate keeping best row per key.

        Rule 10: IF KEY + SCORE THEN keep row with best SCORE per KEY.

        Parameters
        ----------
        table_name
            Table to deduplicate.
        key_columns
            Columns that define uniqueness.
        score_column
            Column to rank by.
        descending
            If True, keep highest score. If False, keep lowest.

        Returns
        -------
        DataFrame
            Deduplicated DataFrame.
        """
        from datafusion import col, functions, lit

        info = self.get(table_name)

        # Use window function to rank, then filter to rank 1
        partition_by = [col(c) for c in key_columns]

        # Create sort expression for ordering (desc if keeping highest score)
        order_expr = col(score_column).sort(ascending=not descending)

        # row_number() partitioned and ordered - use single SortExpr
        row_num = functions.row_number(partition_by=partition_by, order_by=order_expr)

        ranked = info.df.with_column("_rank", row_num)
        return ranked.filter(col("_rank") == lit(1)).drop("_rank")


__all__ = ["SemanticCompiler", "TableInfo"]
