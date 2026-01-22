"""DataFusion fragment view builders."""

from __future__ import annotations

from collections.abc import Callable, Sequence

from datafusion import SessionContext
from datafusion.dataframe import DataFrame

from datafusion_engine.df_builder import df_from_sqlglot
from datafusion_engine.query_fragments_sql import FRAGMENT_SQL
from datafusion_engine.schema_introspection import SchemaIntrospector
from datafusion_engine.sql_options import sql_options_for_profile
from datafusion_engine.udf_registry import register_datafusion_udfs
from schema_spec.view_specs import ViewSpec, view_spec_from_builder
from sqlglot_tools.compat import Expression
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    SchemaMapping,
    default_sqlglot_policy,
    normalize_expr,
    parse_sql_strict,
)


def fragment_view_specs(
    ctx: SessionContext,
    *,
    exclude: Sequence[str] | None = None,
) -> tuple[ViewSpec, ...]:
    """Return ViewSpecs for fragment expressions using schema-aware normalization.

    Parameters
    ----------
    ctx:
        DataFusion session context used to infer fragment schemas.
    exclude:
        Optional fragment names to skip when building view specs.

    Returns
    -------
    tuple[ViewSpec, ...]
        View specifications derived from fragment expressions.
    """
    excluded = set(exclude or ())
    names = sorted(name for name in FRAGMENT_SQL if name not in excluded)
    sql_options = sql_options_for_profile(None)
    schema_map = SchemaIntrospector(ctx, sql_options=sql_options).schema_map()
    specs: list[ViewSpec] = []
    for name in names:
        expr = _normalize_fragment_expr(
            name,
            schema_map=schema_map,
        )
        specs.append(
            view_spec_from_builder(
                ctx,
                name=name,
                builder=_fragment_df_builder(expr),
                sql=None,
            )
        )
    return tuple(specs)


def _fragment_df_builder(expr: Expression) -> Callable[[SessionContext], DataFrame]:
    def _build(ctx: SessionContext) -> DataFrame:
        register_datafusion_udfs(ctx)
        return df_from_sqlglot(ctx, expr.copy())

    return _build


def _normalize_fragment_expr(
    name: str,
    *,
    schema_map: SchemaMapping | None,
) -> Expression:
    sql = FRAGMENT_SQL[name]
    policy = default_sqlglot_policy()
    expr = parse_sql_strict(sql, dialect=policy.read_dialect)
    return normalize_expr(
        expr,
        options=NormalizeExprOptions(
            schema=schema_map,
            policy=policy,
            sql=sql,
        ),
    )


__all__ = ["fragment_view_specs"]
