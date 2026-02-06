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
...         "scip_occurrences_norm",
...         join_type="overlap",
...         origin="cst_ref",
...     ),
... )
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import pyarrow as pa

from obs.otel.scopes import SCOPE_SEMANTICS
from obs.otel.tracing import stage_span
from semantics.config import SemanticConfig
from semantics.join_helpers import join_by_span_contains, join_by_span_overlap
from semantics.joins import JoinStrategy, JoinStrategyType
from semantics.schema import SemanticSchema, SemanticSchemaError
from semantics.types.core import columns_are_joinable

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext
    from datafusion.expr import Expr

    from semantics.exprs import ExprContextImpl, ExprSpec
    from semantics.ir import SemanticIRJoinGroup
    from semantics.quality import JoinHow
    from semantics.specs import SemanticTableSpec
    from semantics.types import AnnotatedSchema


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

_LEFT_ALIAS_PREFIX = "l__"
_RIGHT_ALIAS_PREFIX = "r__"
_FILE_QUALITY_PREFIX = "fq__"


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TextNormalizationOptions:
    """Options for text normalization."""

    form: str = "NFC"
    casefold: bool = False
    collapse_ws: bool = False
    output_suffix: str = "_norm"


@dataclass(frozen=True)
class _QualityOutputContext:
    """Context for projecting quality relationship outputs."""

    rank_available: set[str]
    hard_columns: list[str]
    feature_columns: set[str]
    required_columns: set[str]


@dataclass
class TableInfo:
    """Analyzed table with semantic information.

    Attributes:
    ----------
    name
        Table name in the session context.
    df
        DataFrame handle.
    sem
        Semantic schema analysis.
    annotated
        Annotated schema with semantic type metadata.
    """

    name: str
    df: DataFrame
    sem: SemanticSchema
    annotated: AnnotatedSchema

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

        Returns:
        -------
        TableInfo
            Analyzed table.
        """
        from semantics.types import AnnotatedSchema

        return cls(
            name=name,
            df=df,
            sem=SemanticSchema.from_df(df, table_name=name, config=config),
            annotated=AnnotatedSchema.from_dataframe(df),
        )


@dataclass(frozen=True)
class UnionSpec:
    """Specification for canonical union operations."""

    table_names: list[str]
    discriminator: str
    schema: tuple[tuple[str, pa.DataType], ...]


@dataclass(frozen=True)
class RelationOptions:
    """Options for relationship inference and filtering."""

    join_type: Literal["overlap", "contains"] | None = None
    strategy_hint: JoinStrategyType | None = None
    filter_sql: str | None = None
    origin: str = "cst"
    use_cdf: bool = False
    output_name: str | None = None


@dataclass(frozen=True)
class JoinInputs:
    """Inputs required for strategy-driven joins."""

    left_df: DataFrame
    right_df: DataFrame
    left_sem: SemanticSchema
    right_sem: SemanticSchema


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
    def _validate_join_keys(
        *,
        left_info: TableInfo,
        right_info: TableInfo,
        left_on: Sequence[str],
        right_on: Sequence[str],
    ) -> None:
        left_view = left_info.name
        right_view = right_info.name
        if not left_on or not right_on:
            msg = (
                "Quality relationship join requires non-empty keys: "
                f"{left_view!r} -> {right_view!r}."
            )
            raise SemanticSchemaError(msg)
        if len(left_on) != len(right_on):
            msg = (
                "Join key length mismatch for "
                f"{left_view!r} ({len(left_on)}) vs {right_view!r} ({len(right_on)})."
            )
            raise SemanticSchemaError(msg)

        left_missing = [key for key in left_on if key not in left_info.annotated]
        right_missing = [key for key in right_on if key not in right_info.annotated]
        if left_missing or right_missing:
            msg = (
                "Join keys missing in source tables: "
                f"{left_view!r} missing={sorted(left_missing)!r}, "
                f"{right_view!r} missing={sorted(right_missing)!r}."
            )
            raise SemanticSchemaError(msg)

        for left_key, right_key in zip(left_on, right_on, strict=True):
            if columns_are_joinable(left_key, right_key):
                continue
            left_col = left_info.annotated.get(left_key)
            right_col = right_info.annotated.get(right_key)
            left_type = left_col.semantic_type if left_col is not None else None
            right_type = right_col.semantic_type if right_col is not None else None
            msg = (
                "Join key semantic type mismatch for "
                f"{left_view!r}.{left_key} ({left_type}) vs "
                f"{right_view!r}.{right_key} ({right_type})."
            )
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
        condition: Expr | None = None
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

    def _join_with_strategy(
        self,
        inputs: JoinInputs,
        *,
        strategy: JoinStrategy,
        filter_sql: str | None,
    ) -> tuple[DataFrame, SemanticSchema, SemanticSchema]:
        left_prefix = "left__"
        right_prefix = "right__"
        left_pref = self._prefix_df(inputs.left_df, left_prefix)
        right_pref = self._prefix_df(inputs.right_df, right_prefix)
        left_sem_pref = inputs.left_sem.prefixed(left_prefix)
        right_sem_pref = inputs.right_sem.prefixed(right_prefix)

        if strategy.strategy_type == JoinStrategyType.SPAN_OVERLAP:
            joined = join_by_span_overlap(left_pref, right_pref, left_sem_pref, right_sem_pref)
            filter_expr = filter_sql
        elif strategy.strategy_type == JoinStrategyType.SPAN_CONTAINS:
            joined = join_by_span_contains(left_pref, right_pref, left_sem_pref, right_sem_pref)
            filter_expr = filter_sql
        else:
            left_keys = tuple(f"{left_prefix}{key}" for key in strategy.left_keys)
            right_keys = tuple(f"{right_prefix}{key}" for key in strategy.right_keys)
            if not left_keys or not right_keys:
                msg = f"Join strategy {strategy.strategy_type} missing join keys."
                raise SemanticSchemaError(msg)
            joined = left_pref.join(
                right_pref,
                left_on=list(left_keys),
                right_on=list(right_keys),
                how="inner",
            )
            if strategy.filter_expr and filter_sql:
                filter_expr = f"({strategy.filter_expr}) AND ({filter_sql})"
            elif strategy.filter_expr:
                filter_expr = strategy.filter_expr
            else:
                filter_expr = filter_sql

        if filter_expr:
            joined = joined.filter(filter_expr)

        return joined, left_sem_pref, right_sem_pref

    def _union_canonical(self, spec: UnionSpec) -> DataFrame:
        from datafusion import col, lit

        if not spec.table_names:
            msg = "Union requires at least one table."
            raise SemanticSchemaError(msg)

        dfs: list[DataFrame] = []
        for name in spec.table_names:
            info = self.get(name)
            projected = self._project_to_schema(info.df, spec.schema)
            dfs.append(projected.with_column(spec.discriminator, lit(name)))

        result = dfs[0]
        for df in dfs[1:]:
            result = result.union(df)

        ordered = [name for name, _dtype in spec.schema] + [spec.discriminator]
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

        Returns:
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

        Returns:
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
        """Apply normalization rules using an explicit table spec.

        Returns:
        -------
        DataFrame
            Normalized DataFrame with canonical columns.

        """
        with stage_span(
            "semantics.normalize_from_spec",
            stage="semantics",
            scope_name=SCOPE_SEMANTICS,
            attributes={
                "codeanatomy.table": spec.table,
                "codeanatomy.namespace": spec.entity_id.namespace,
                "codeanatomy.foreign_key_count": len(spec.foreign_keys),
            },
        ):
            from datafusion import col

            from datafusion_engine.udf.expr import udf_expr

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

            start_expr = col(spec.primary_span.start_col)
            end_expr = col(spec.primary_span.end_col)
            if spec.primary_span.end_col.endswith("byte_len"):
                end_expr = start_expr + end_expr
            df = df.with_column(spec.primary_span.canonical_start, start_expr)
            df = df.with_column(spec.primary_span.canonical_end, end_expr)
            df = df.with_column(
                spec.primary_span.canonical_span,
                udf_expr(
                    "span_make",
                    col(spec.primary_span.canonical_start),
                    col(spec.primary_span.canonical_end),
                ),
            )
            if spec.primary_span.canonical_start != "bstart":
                df = df.with_column("bstart", col(spec.primary_span.canonical_start))
            if spec.primary_span.canonical_end != "bend":
                df = df.with_column("bend", col(spec.primary_span.canonical_end))

            id_expr = udf_expr(
                "stable_id_parts",
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
                fk_expr = udf_expr(
                    "stable_id_parts",
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

            from semantics.span_normalize import drop_line_columns

            return drop_line_columns(df)

    def normalize(self, table_name: str, *, prefix: str) -> DataFrame:
        """Apply normalization rules to an evidence table.

        Args:
            table_name: Input evidence table name.
            prefix: Entity prefix used for generated IDs.

        Returns:
            DataFrame: Result.

        Raises:
            SemanticSchemaError: If schema normalization cannot be resolved.
        """
        with stage_span(
            "semantics.normalize",
            stage="semantics",
            scope_name=SCOPE_SEMANTICS,
            attributes={
                "codeanatomy.table_name": table_name,
                "codeanatomy.prefix": prefix,
            },
        ):
            from datafusion import col

            from semantics.column_types import ColumnType

            spec = self._spec_for_table(table_name)
            if spec is not None:
                return self.normalize_from_spec(spec)

            info = self.get(table_name)
            sem = info.sem
            span_start_candidates = tuple(
                name
                for name, col_type in sem.column_types.items()
                if col_type == ColumnType.SPAN_START
            )
            span_end_candidates = tuple(
                name
                for name, col_type in sem.column_types.items()
                if col_type == ColumnType.SPAN_END
            )
            if len(span_start_candidates) > 1 or len(span_end_candidates) > 1:
                msg = (
                    f"Table {table_name!r} has ambiguous span columns: "
                    f"start={span_start_candidates!r}, end={span_end_candidates!r}. "
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
            from semantics.span_normalize import drop_line_columns

            return drop_line_columns(df)

    # -------------------------------------------------------------------------
    # Rule 3: Text normalization
    # -------------------------------------------------------------------------

    def normalize_text(
        self,
        table_name: str,
        *,
        columns: list[str] | None = None,
        options: TextNormalizationOptions | None = None,
    ) -> DataFrame:
        """Apply text normalization to text columns.

        Args:
            table_name: Input table name.
            columns: Optional subset of text columns to normalize.
            options: Optional text normalization settings.

        Returns:
            DataFrame: Result.

        Raises:
            SemanticSchemaError: If semantic schema lookup fails.
            ValueError: If requested text columns are invalid.
        """
        resolved = options or TextNormalizationOptions()
        with stage_span(
            "semantics.normalize_text",
            stage="semantics",
            scope_name=SCOPE_SEMANTICS,
            attributes={
                "codeanatomy.table_name": table_name,
                "codeanatomy.column_count": len(columns) if columns else 0,
                "codeanatomy.form": resolved.form,
            },
        ):
            from datafusion import col

            from datafusion_engine.udf.expr import udf_expr

            info = self.get(table_name)
            self._require_udfs(("utf8_normalize",))
            df = info.df

            if not resolved.output_suffix:
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
                output_name = f"{col_name}{resolved.output_suffix}"
                if output_name in names:
                    msg = f"Table {table_name!r} already has column {output_name!r}."
                    raise SemanticSchemaError(msg)
                df = df.with_column(
                    output_name,
                    udf_expr(
                        "utf8_normalize",
                        col(col_name),
                        form=resolved.form,
                        casefold=resolved.casefold,
                        collapse_ws=resolved.collapse_ws,
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
        options: RelationOptions,
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
        options
            Relationship inference options and filters.

        Returns:
        -------
        DataFrame
            Relationship table with schema:
            (entity_id, symbol, path, bstart, bend, origin)
        """
        with stage_span(
            f"semantics.relate.{options.join_type or 'infer'}",
            stage="semantics",
            scope_name=SCOPE_SEMANTICS,
            attributes={
                "codeanatomy.left_table": left_table,
                "codeanatomy.right_table": right_table,
                "codeanatomy.join_type": options.join_type,
                "codeanatomy.origin": options.origin,
                "codeanatomy.has_filter": options.filter_sql is not None,
                "codeanatomy.use_cdf": options.use_cdf,
            },
        ):
            from datafusion import lit

            from semantics.joins import require_join_strategy

            left_info = self.get(left_table)
            right_info = self.get(right_table)
            left_info.sem.require_entity(table=left_table)
            right_info.sem.require_symbol_source(table=right_table)
            hint = options.strategy_hint
            if hint is None and options.join_type is not None:
                hint = (
                    JoinStrategyType.SPAN_CONTAINS
                    if options.join_type == "contains"
                    else JoinStrategyType.SPAN_OVERLAP
                )
            strategy = require_join_strategy(
                left_info.annotated,
                right_info.annotated,
                hint=hint,
                left_name=left_table,
                right_name=right_table,
            )

            if strategy.strategy_type in {
                JoinStrategyType.SPAN_OVERLAP,
                JoinStrategyType.SPAN_CONTAINS,
            }:
                self._require_span_unit_compatibility(
                    left_info.sem,
                    right_info.sem,
                    left_table=left_table,
                    right_table=right_table,
                )
                self._require_udfs(("span_overlaps", "span_contains"))

            if options.use_cdf:
                from semantics.incremental import (
                    CDFJoinSpec,
                    build_incremental_join,
                    incremental_join_enabled,
                    merge_incremental_results,
                )

                if incremental_join_enabled(left_info.df, right_info.df):
                    key_columns = strategy.left_keys or strategy.right_keys
                    if not key_columns:
                        key_columns = ("entity_id",)
                    cdf_spec = CDFJoinSpec(
                        left_table=left_table,
                        right_table=right_table,
                        output_name=f"{left_table}__{right_table}__cdf",
                        key_columns=key_columns,
                    )

                    def _join_builder(left_df: DataFrame, right_df: DataFrame) -> DataFrame:
                        joined, _left_sem, _right_sem = self._join_with_strategy(
                            JoinInputs(
                                left_df=left_df,
                                right_df=right_df,
                                left_sem=left_info.sem,
                                right_sem=right_info.sem,
                            ),
                            strategy=strategy,
                            filter_sql=options.filter_sql,
                        )
                        return joined

                    joined = build_incremental_join(
                        self.ctx,
                        cdf_spec,
                        left_df=left_info.df,
                        right_df=right_info.df,
                        join_builder=_join_builder,
                    )
                    if options.output_name is not None:
                        joined = merge_incremental_results(
                            self.ctx,
                            incremental_df=joined,
                            base_table=options.output_name,
                            key_columns=key_columns,
                        )
                    left_sem = left_info.sem.prefixed("left__")
                    right_sem = right_info.sem.prefixed("right__")
                else:
                    joined, left_sem, right_sem = self._join_with_strategy(
                        JoinInputs(
                            left_df=left_info.df,
                            right_df=right_info.df,
                            left_sem=left_info.sem,
                            right_sem=right_info.sem,
                        ),
                        strategy=strategy,
                        filter_sql=options.filter_sql,
                    )
            else:
                joined, left_sem, right_sem = self._join_with_strategy(
                    JoinInputs(
                        left_df=left_info.df,
                        right_df=right_info.df,
                        left_sem=left_info.sem,
                        right_sem=right_info.sem,
                    ),
                    strategy=strategy,
                    filter_sql=options.filter_sql,
                )

            # Rule 7: Project to relation schema
            return joined.select(
                left_sem.entity_id_col().alias("entity_id"),
                right_sem.symbol_col().alias("symbol"),
                left_sem.path_col().alias("path"),
                left_sem.span_start_col().alias("bstart"),
                left_sem.span_end_col().alias("bend"),
                lit(options.origin).alias("origin"),
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

        Args:
            table_names: Table names to union.
            discriminator: Name of discriminator column.

        Returns:
            DataFrame: Result.

        Raises:
            SemanticSchemaError: If table schemas are incompatible.
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
        """Union node tables after canonical projection.

        Returns:
        -------
        DataFrame
            Unioned node DataFrame.
        """
        with stage_span(
            "semantics.union_nodes",
            stage="semantics",
            scope_name=SCOPE_SEMANTICS,
            attributes={
                "codeanatomy.table_count": len(table_names),
                "codeanatomy.tables": ",".join(table_names),
            },
        ):
            return self._union_canonical(
                UnionSpec(
                    table_names=table_names,
                    discriminator=discriminator,
                    schema=CANONICAL_NODE_SCHEMA,
                )
            )

    def union_edges(
        self,
        table_names: list[str],
        *,
        discriminator: str = "edge_kind",
    ) -> DataFrame:
        """Union edge tables after canonical projection.

        Returns:
        -------
        DataFrame
            Unioned edge DataFrame.
        """
        with stage_span(
            "semantics.union_edges",
            stage="semantics",
            scope_name=SCOPE_SEMANTICS,
            attributes={
                "codeanatomy.table_count": len(table_names),
                "codeanatomy.tables": ",".join(table_names),
            },
        ):
            return self._union_canonical(
                UnionSpec(
                    table_names=table_names,
                    discriminator=discriminator,
                    schema=CANONICAL_EDGE_SCHEMA,
                )
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

        Returns:
        -------
        DataFrame
            Aggregated DataFrame.
        """
        with stage_span(
            "semantics.aggregate",
            stage="semantics",
            scope_name=SCOPE_SEMANTICS,
            attributes={
                "codeanatomy.table_name": table_name,
                "codeanatomy.group_by_count": len(group_by),
                "codeanatomy.aggregate_count": len(aggregate),
            },
        ):
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

        Returns:
        -------
        DataFrame
            Deduplicated DataFrame.
        """
        with stage_span(
            "semantics.dedupe",
            stage="semantics",
            scope_name=SCOPE_SEMANTICS,
            attributes={
                "codeanatomy.table_name": table_name,
                "codeanatomy.key_column_count": len(key_columns),
                "codeanatomy.score_column": score_column,
                "codeanatomy.descending": descending,
            },
        ):
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

    # -------------------------------------------------------------------------
    # Quality-aware relationship compilation
    # -------------------------------------------------------------------------

    def _build_joined_tables(
        self,
        *,
        left_view: str,
        right_view: str,
        left_on: Sequence[str],
        right_on: Sequence[str],
        how: JoinHow,
    ) -> DataFrame:
        left_info = self.get(left_view)
        right_info = self.get(right_view)
        self._validate_join_keys(
            left_info=left_info,
            right_info=right_info,
            left_on=left_on,
            right_on=right_on,
        )

        left_aliased = self._prefix_df(left_info.df, _LEFT_ALIAS_PREFIX)
        right_aliased = self._prefix_df(right_info.df, _RIGHT_ALIAS_PREFIX)

        left_keys = [f"{_LEFT_ALIAS_PREFIX}{k}" for k in left_on]
        right_keys = [f"{_RIGHT_ALIAS_PREFIX}{k}" for k in right_on]

        return left_aliased.join(
            right_aliased,
            left_on=left_keys,
            right_on=right_keys,
            how=how,
        )

    def build_join_group(self, group: SemanticIRJoinGroup) -> DataFrame:
        """Build a shared join group for multiple relationships.

        Returns:
        -------
        DataFrame
            Joined DataFrame with left/right prefixes.
        """
        return self._build_joined_tables(
            left_view=group.left_view,
            right_view=group.right_view,
            left_on=group.left_on,
            right_on=group.right_on,
            how=group.how,
        )

    def compile_relationship_from_join(
        self,
        joined: DataFrame,
        spec: QualityRelationshipSpec,
        *,
        file_quality_df: DataFrame | None = None,
    ) -> DataFrame:
        """Compile a relationship from a pre-joined DataFrame.

        Parameters
        ----------
        joined
            Pre-joined DataFrame with left/right prefixes.
        spec
            Quality relationship specification.
        file_quality_df
            Optional file quality DataFrame for confidence adjustment.

        Returns:
        -------
        DataFrame
            Compiled relationship output.
        """
        return self.compile_relationship_with_quality(
            spec,
            file_quality_df=file_quality_df,
            joined=joined,
        )

    def _maybe_join_file_quality(
        self,
        joined: DataFrame,
        *,
        spec: QualityRelationshipSpec,
        file_quality_df: DataFrame | None,
    ) -> DataFrame:
        if not spec.join_file_quality:
            return joined
        fq_df = file_quality_df
        if fq_df is None:
            try:
                fq_df = self.ctx.table(spec.file_quality_view)
            except (KeyError, RuntimeError, TypeError, ValueError):
                return joined
        if fq_df is None:
            return joined
        fq_aliased = self._prefix_df(fq_df, _FILE_QUALITY_PREFIX)
        left_file_id = f"{_LEFT_ALIAS_PREFIX}file_id"
        fq_file_id = f"{_FILE_QUALITY_PREFIX}file_id"
        return joined.join(
            fq_aliased,
            left_on=[left_file_id],
            right_on=[fq_file_id],
            how="left",
        )

    def _compile_relationship_with_quality_inner(
        self,
        spec: QualityRelationshipSpec,
        *,
        file_quality_df: DataFrame | None,
        joined: DataFrame | None,
    ) -> DataFrame:
        # 1. Build equi-join (require keys) unless pre-joined is provided
        if joined is None:
            joined = self._build_joined_tables(
                left_view=spec.left_view,
                right_view=spec.right_view,
                left_on=spec.left_on,
                right_on=spec.right_on,
                how=spec.how,
            )

        # 2. Optionally join file quality signals
        joined = self._maybe_join_file_quality(
            joined,
            spec=spec,
            file_quality_df=file_quality_df,
        )

        # 3. Set up expression context with prefixes
        from semantics.exprs import ExprContextImpl

        expr_ctx = ExprContextImpl(left_alias="l", right_alias="r")
        schema_names = set(self._schema_names(joined))
        feature_columns = self._feature_columns(spec)
        rank_available = self._rank_available(schema_names, feature_columns, spec)
        required_columns = self._initial_required_columns(spec)

        # 4. Apply hard predicates
        joined, hard_columns, required_columns = self._apply_hard_predicates(
            joined,
            spec=spec,
            expr_ctx=expr_ctx,
            schema_names=schema_names,
            required_columns=required_columns,
        )

        # 5. Add feature columns and compute score
        joined, required_columns = self._apply_feature_columns(
            joined,
            spec=spec,
            expr_ctx=expr_ctx,
            schema_names=schema_names,
            required_columns=required_columns,
        )

        # 6. Compute confidence with quality adjustment
        joined = self._apply_confidence(joined, spec=spec)

        # 7. Add metadata columns
        joined = self._apply_metadata(joined, spec=spec)

        # 8. Apply ranking if specified
        joined, required_columns = self._apply_ranking(
            joined,
            spec=spec,
            expr_ctx=expr_ctx,
            rank_available=rank_available,
            required_columns=required_columns,
        )

        # 9. Project final output columns if specified
        output_ctx = _QualityOutputContext(
            rank_available=rank_available,
            hard_columns=hard_columns,
            feature_columns=feature_columns,
            required_columns=required_columns,
        )
        return self._project_quality_output(
            joined,
            spec=spec,
            expr_ctx=expr_ctx,
            output_ctx=output_ctx,
        )

    @staticmethod
    def _record_expr_issue(exc: Exception, *, expr_label: str) -> None:
        logger.warning(
            "Semantic expression validation failed for %s (%s).",
            expr_label,
            exc,
        )

    @staticmethod
    def _collect_expr_columns(
        expr_spec: ExprSpec,
        *,
        available_columns: set[str],
        expr_label: str,
    ) -> set[str] | None:
        from semantics.exprs import ExprValidationContext, validate_expr_spec

        try:
            validate_expr_spec(
                expr_spec,
                available_columns=available_columns,
                expr_label=expr_label,
            )
        except ValueError as exc:
            SemanticCompiler._record_expr_issue(exc, expr_label=expr_label)
            return None
        validation_ctx = ExprValidationContext()
        _ = expr_spec(validation_ctx)
        return validation_ctx.used_columns()

    @staticmethod
    def _feature_columns(spec: QualityRelationshipSpec) -> set[str]:
        return {f"feat_{feature.name}" for feature in spec.signals.features}

    @staticmethod
    def _rank_available(
        schema_names: set[str],
        feature_columns: set[str],
        spec: QualityRelationshipSpec,
    ) -> set[str]:
        rank_available = {
            *schema_names,
            *feature_columns,
            "score",
            "confidence",
            "origin",
            "provider",
        }
        if spec.rule_name is not None:
            rank_available.add("rule_name")
        return rank_available

    @staticmethod
    def _initial_required_columns(spec: QualityRelationshipSpec) -> set[str]:
        required_columns: set[str] = set()
        required_columns.update({f"{_LEFT_ALIAS_PREFIX}{key}" for key in spec.left_on})
        required_columns.update({f"{_RIGHT_ALIAS_PREFIX}{key}" for key in spec.right_on})
        return required_columns

    def _apply_hard_predicates(
        self,
        joined: DataFrame,
        *,
        spec: QualityRelationshipSpec,
        expr_ctx: ExprContextImpl,
        schema_names: set[str],
        required_columns: set[str],
    ) -> tuple[DataFrame, list[str], set[str]]:
        from datafusion import col

        hard_columns: list[str] = []
        for index, hard_pred in enumerate(spec.signals.hard, start=1):
            hard_required = self._collect_expr_columns(
                hard_pred.predicate,
                available_columns=schema_names,
                expr_label=f"{spec.name}.hard[{index}]",
            )
            if hard_required is None:
                continue
            required_columns.update(hard_required)
            pred_expr = hard_pred.predicate(expr_ctx)
            hard_col = f"hard_{index}"
            try:
                joined = joined.with_column(hard_col, pred_expr)
                joined = joined.filter(col(hard_col))
            except (RuntimeError, TypeError, ValueError) as exc:
                self._record_expr_issue(exc, expr_label=f"{spec.name}.hard[{index}]")
                continue
            hard_columns.append(hard_col)
        return joined, hard_columns, required_columns

    def _apply_feature_columns(
        self,
        joined: DataFrame,
        *,
        spec: QualityRelationshipSpec,
        expr_ctx: ExprContextImpl,
        schema_names: set[str],
        required_columns: set[str],
    ) -> tuple[DataFrame, set[str]]:
        from datafusion import col, lit

        base_score = lit(spec.signals.base_score)
        for feature in spec.signals.features:
            feature_required = self._collect_expr_columns(
                feature.expr,
                available_columns=schema_names,
                expr_label=f"{spec.name}.feature[{feature.name}]",
            )
            if feature_required is None:
                continue
            required_columns.update(feature_required)
            feature_expr = feature.expr(expr_ctx)
            feature_col = f"feat_{feature.name}"
            try:
                joined = joined.with_column(feature_col, feature_expr)
            except (RuntimeError, TypeError, ValueError) as exc:
                self._record_expr_issue(exc, expr_label=f"{spec.name}.feature[{feature.name}]")
                continue
            base_score += col(feature_col) * lit(feature.weight)
        joined = joined.with_column("score", base_score)
        return joined, required_columns

    def _apply_confidence(
        self,
        joined: DataFrame,
        *,
        spec: QualityRelationshipSpec,
    ) -> DataFrame:
        from datafusion import col, functions, lit

        from semantics.exprs import clamp

        base_conf = lit(spec.signals.base_confidence)
        quality_col = f"{_FILE_QUALITY_PREFIX}{spec.signals.quality_score_column}"
        schema_names = set(self._schema_names(joined))
        if quality_col in schema_names:
            quality_adj = functions.coalesce(col(quality_col), lit(1000.0)) * lit(
                spec.signals.quality_weight
            )
            raw_conf = base_conf + (col("score") / lit(10000.0)) + quality_adj
        else:
            raw_conf = base_conf + (col("score") / lit(10000.0))
        conf_expr = clamp(raw_conf, min_value=lit(0.0), max_value=lit(1.0))
        return joined.with_column("confidence", conf_expr)

    @staticmethod
    def _apply_metadata(joined: DataFrame, *, spec: QualityRelationshipSpec) -> DataFrame:
        from datafusion import lit

        joined = joined.with_column("origin", lit(spec.origin))
        joined = joined.with_column("provider", lit(spec.provider))
        if spec.rule_name is not None:
            joined = joined.with_column("rule_name", lit(spec.rule_name))
        return joined

    def _apply_ranking(
        self,
        joined: DataFrame,
        *,
        spec: QualityRelationshipSpec,
        expr_ctx: ExprContextImpl,
        rank_available: set[str],
        required_columns: set[str],
    ) -> tuple[DataFrame, set[str]]:
        if spec.rank is None:
            return joined, required_columns
        from datafusion import col, functions, lit
        from datafusion.expr import SortKey

        key_required = self._collect_expr_columns(
            spec.rank.ambiguity_key_expr,
            available_columns=set(self._schema_names(joined)),
            expr_label=f"{spec.name}.rank.ambiguity_key_expr",
        )
        if key_required is None:
            return joined, required_columns
        required_columns.update(key_required)
        group_key = spec.rank.ambiguity_key_expr(expr_ctx)
        group_id_expr = group_key
        if spec.rank.ambiguity_group_id_expr is not None:
            group_id_required = self._collect_expr_columns(
                spec.rank.ambiguity_group_id_expr,
                available_columns=set(self._schema_names(joined)),
                expr_label=f"{spec.name}.rank.ambiguity_group_id_expr",
            )
            if group_id_required is not None:
                required_columns.update(group_id_required)
                group_id_expr = spec.rank.ambiguity_group_id_expr(expr_ctx)
        joined = joined.with_column("ambiguity_group_id", group_id_expr)
        if not spec.rank.order_by:
            return joined, required_columns
        order_exprs: list[SortKey] = []
        for index, order in enumerate(spec.rank.order_by, start=1):
            order_required = self._collect_expr_columns(
                order.expr,
                available_columns=rank_available,
                expr_label=f"{spec.name}.rank.order_by[{index}]",
            )
            if order_required is None:
                return joined, required_columns
            required_columns.update(order_required)
            order_exprs.append(order.expr(expr_ctx).sort(ascending=(order.direction == "asc")))
        row_num = functions.row_number(
            partition_by=[col("ambiguity_group_id")],
            order_by=order_exprs,
        )
        joined = joined.with_column("_rn", row_num)
        if spec.rank.keep == "best":
            joined = joined.filter(col("_rn") <= lit(spec.rank.top_k))
        joined = joined.drop("_rn")
        return joined, required_columns

    @staticmethod
    def _existing_cols(schema_names: set[str], names: Sequence[str]) -> list[Expr]:
        from datafusion import col

        return [col(name) for name in names if name in schema_names]

    def _project_quality_output(
        self,
        joined: DataFrame,
        *,
        spec: QualityRelationshipSpec,
        expr_ctx: ExprContextImpl,
        output_ctx: _QualityOutputContext,
    ) -> DataFrame:
        if spec.select_exprs:
            return self._select_quality_expr_output(
                joined,
                spec=spec,
                expr_ctx=expr_ctx,
                output_ctx=output_ctx,
            )
        return self._select_quality_default_output(
            joined,
            spec=spec,
            output_ctx=output_ctx,
        )

    def _select_quality_expr_output(
        self,
        joined: DataFrame,
        *,
        spec: QualityRelationshipSpec,
        expr_ctx: ExprContextImpl,
        output_ctx: _QualityOutputContext,
    ) -> DataFrame:
        schema_names = set(self._schema_names(joined))
        select_available = {
            *output_ctx.rank_available,
            "ambiguity_group_id",
        }
        select_cols: list[Expr] = []
        for select_expr in spec.select_exprs:
            select_required = self._collect_expr_columns(
                select_expr.expr,
                available_columns=select_available,
                expr_label=f"{spec.name}.select[{select_expr.alias}]",
            )
            if select_required is None:
                continue
            select_cols.append(select_expr.expr(expr_ctx).alias(select_expr.alias))
        select_cols.extend(
            self._existing_cols(schema_names, ("confidence", "score", "origin", "provider"))
        )
        if spec.rank is not None:
            select_cols.extend(self._existing_cols(schema_names, ("ambiguity_group_id",)))
        if spec.rule_name is not None:
            select_cols.extend(self._existing_cols(schema_names, ("rule_name",)))
        select_cols.extend(self._existing_cols(schema_names, output_ctx.hard_columns))
        select_cols.extend(self._existing_cols(schema_names, sorted(output_ctx.feature_columns)))
        return joined.select(*select_cols)

    def _select_quality_default_output(
        self,
        joined: DataFrame,
        *,
        spec: QualityRelationshipSpec,
        output_ctx: _QualityOutputContext,
    ) -> DataFrame:
        from datafusion import col

        schema_names = set(self._schema_names(joined))
        select_cols: list[Expr] = []
        seen: set[str] = set()

        def _append_col(name: str) -> None:
            if name in seen or name not in schema_names:
                return
            select_cols.append(col(name))
            seen.add(name)

        join_cols = [f"{_LEFT_ALIAS_PREFIX}{key}" for key in spec.left_on] + [
            f"{_RIGHT_ALIAS_PREFIX}{key}" for key in spec.right_on
        ]
        for name in join_cols:
            _append_col(name)
        for name in sorted(output_ctx.required_columns):
            _append_col(name)
        for name in ("confidence", "score", "origin", "provider"):
            _append_col(name)
        if spec.rule_name is not None:
            _append_col("rule_name")
        if spec.rank is not None:
            _append_col("ambiguity_group_id")
        for hard_col in output_ctx.hard_columns:
            _append_col(hard_col)
        for feature_name in sorted(output_ctx.feature_columns):
            _append_col(feature_name)
        return joined.select(*select_cols)

    def compile_relationship_with_quality(
        self,
        spec: QualityRelationshipSpec,
        *,
        file_quality_df: DataFrame | None = None,
        joined: DataFrame | None = None,
    ) -> DataFrame:
        """Compile a relationship with quality signals.

        This method implements quality-aware relationship compilation:
        1. Alias left/right tables with prefixes to prevent column collision
        2. Equi-join on hard keys (left_on/right_on)
        3. Optionally join file_quality signals via file_id
        4. Apply hard predicates via filter()
        5. Add feature columns via with_column()
        6. Compute score and confidence
        7. Add metadata columns (origin, provider, rule_name)
        8. Apply ambiguity grouping with row_number() window function
        9. Filter to top_k if keep="best"

        Parameters
        ----------
        spec
            Quality relationship specification.
        file_quality_df
            Optional pre-built file quality DataFrame. If None and
            spec.join_file_quality is True, will try to use file_quality_v1.
        joined
            Optional pre-joined DataFrame with left/right prefixes.

        Returns:
        -------
        DataFrame
            Compiled relationship with confidence, score, and ambiguity_group_id.

        """
        with stage_span(
            "semantics.compile_relationship_with_quality",
            stage="semantics",
            scope_name=SCOPE_SEMANTICS,
            attributes={
                "codeanatomy.spec_name": spec.name,
                "codeanatomy.left_view": spec.left_view,
                "codeanatomy.right_view": spec.right_view,
                "codeanatomy.join_how": spec.how,
                "codeanatomy.join_file_quality": spec.join_file_quality,
            },
        ):
            return self._compile_relationship_with_quality_inner(
                spec,
                file_quality_df=file_quality_df,
                joined=joined,
            )


# Type import for compile_relationship_with_quality
if TYPE_CHECKING:
    from semantics.quality import QualityRelationshipSpec


__all__ = ["SemanticCompiler", "TableInfo"]
