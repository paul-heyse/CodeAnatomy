"""DataFusion fragment view builders."""

from __future__ import annotations

from collections.abc import Callable, Sequence

from datafusion import SessionContext
from datafusion.dataframe import DataFrame
from sqlglot.serde import load

from datafusion_engine.compile_pipeline import CompilationPipeline, CompileOptions
from datafusion_engine.query_fragments_ast import FRAGMENT_AST_PAYLOADS
from datafusion_engine.schema_introspection import SchemaIntrospector
from datafusion_engine.sql_options import sql_options_for_profile
from datafusion_engine.udf_registry import register_datafusion_udfs
from schema_spec.view_specs import ViewSpec, view_spec_from_builder
from sqlglot_tools.compat import Expression
from sqlglot_tools.optimizer import (
    NormalizeExprOptions,
    SchemaMapping,
    normalize_expr,
    resolve_sqlglot_policy,
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
    names = sorted(name for name in FRAGMENT_AST_PAYLOADS if name not in excluded)
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
        pipeline = CompilationPipeline(ctx, CompileOptions())
        compiled = pipeline.compile_ast(expr.copy())
        return pipeline.execute(compiled)

    return _build


def _normalize_fragment_expr(
    name: str,
    *,
    schema_map: SchemaMapping | None,
) -> Expression:
    policy = resolve_sqlglot_policy(name="datafusion_compile")
    expr = load(FRAGMENT_AST_PAYLOADS[name])
    return normalize_expr(
        expr,
        options=NormalizeExprOptions(
            schema=schema_map,
            policy=policy,
            sql=None,
        ),
    )


def _fragment_sql(name: str) -> str:
    policy = resolve_sqlglot_policy(name="datafusion_compile")
    expr = load(FRAGMENT_AST_PAYLOADS[name])
    return expr.sql(dialect=policy.write_dialect)


def cst_callsites_attrs_sql() -> str:
    """Return SQL for CST callsite attribute fragments.

    Returns
    -------
    str
        SQL fragment for CST callsite attributes.
    """
    return _fragment_sql("cst_callsites_attrs")


def cst_defs_attrs_sql() -> str:
    """Return SQL for CST definition attribute fragments.

    Returns
    -------
    str
        SQL fragment for CST definition attributes.
    """
    return _fragment_sql("cst_defs_attrs")


def cst_edges_attrs_sql() -> str:
    """Return SQL for CST edge attribute fragments.

    Returns
    -------
    str
        SQL fragment for CST edge attributes.
    """
    return _fragment_sql("cst_edges_attrs")


def cst_imports_attrs_sql() -> str:
    """Return SQL for CST import attribute fragments.

    Returns
    -------
    str
        SQL fragment for CST import attributes.
    """
    return _fragment_sql("cst_imports_attrs")


def cst_nodes_attrs_sql() -> str:
    """Return SQL for CST node attribute fragments.

    Returns
    -------
    str
        SQL fragment for CST node attributes.
    """
    return _fragment_sql("cst_nodes_attrs")


def cst_refs_attrs_sql() -> str:
    """Return SQL for CST ref attribute fragments.

    Returns
    -------
    str
        SQL fragment for CST ref attributes.
    """
    return _fragment_sql("cst_refs_attrs")


__all__ = [
    "cst_callsites_attrs_sql",
    "cst_defs_attrs_sql",
    "cst_edges_attrs_sql",
    "cst_imports_attrs_sql",
    "cst_nodes_attrs_sql",
    "cst_refs_attrs_sql",
    "fragment_view_specs",
]
