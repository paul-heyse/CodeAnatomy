"""DSL-based view expression builders.

This module provides refactored view definitions using the DSL,
demonstrating how to reduce boilerplate in view definitions.

Note: For views that require map attribute extraction (ast_calls, ast_defs),
the map_attr methods require the UDF platform to be installed. These views
demonstrate the pattern but are built lazily when the platform is available.
"""

from __future__ import annotations

from datafusion.expr import Expr

from datafusion_engine.views.dsl import ViewExprBuilder


def build_ast_docstrings_exprs() -> tuple[Expr, ...]:
    """Build expressions for the ast_docstrings view.

    Returns
    -------
    tuple[Expr, ...]
        View select expressions.

    Notes
    -----
    Original definition was 19 lines. DSL version is 10 lines.
    """
    return (
        ViewExprBuilder()
        .add_identity_cols(
            "file_id", "path", "owner_ast_id", "owner_kind", "owner_name", "docstring", "source"
        )
        .add_span_fields()
        .add_attrs_col()
        .add_ast_record()
        .build()
    )


def build_ast_errors_exprs() -> tuple[Expr, ...]:
    """Build expressions for the ast_errors view.

    Returns
    -------
    tuple[Expr, ...]
        View select expressions.

    Notes
    -----
    Original definition was 16 lines. DSL version is 9 lines.
    """
    return (
        ViewExprBuilder()
        .add_identity_cols("file_id", "path", "error_type", "message")
        .add_span_fields()
        .add_attrs_col()
        .add_ast_record()
        .build()
    )


def build_ast_imports_exprs() -> tuple[Expr, ...]:
    """Build expressions for the ast_imports view.

    Returns
    -------
    tuple[Expr, ...]
        View select expressions.

    Notes
    -----
    Original definition was 22 lines. DSL version is 12 lines.
    """
    return (
        ViewExprBuilder()
        .add_identity_cols(
            "file_id",
            "path",
            "ast_id",
            "parent_ast_id",
            "kind",
            "module",
            "name",
            "asname",
            "alias_index",
            "level",
        )
        .add_span_fields()
        .add_attrs_col()
        .add_ast_record()
        .build()
    )


def build_ast_def_attrs_exprs() -> tuple[Expr, ...]:
    """Build expressions for the ast_def_attrs view.

    Returns
    -------
    tuple[Expr, ...]
        View select expressions.

    Notes
    -----
    Original definition was 9 lines. DSL version is 6 lines.
    """
    return (
        ViewExprBuilder()
        .add_identity_cols("file_id", "path", "ast_id", "parent_ast_id", "kind", "name")
        .add_kv_extraction()
        .build()
    )


def build_ast_edge_attrs_exprs() -> tuple[Expr, ...]:
    """Build expressions for the ast_edge_attrs view.

    Returns
    -------
    tuple[Expr, ...]
        View select expressions.

    Notes
    -----
    Original definition was 10 lines. DSL version is 6 lines.
    """
    return (
        ViewExprBuilder()
        .add_identity_cols("file_id", "path", "src", "dst", "kind", "slot", "idx")
        .add_kv_extraction()
        .build()
    )


def build_ast_type_ignores_exprs() -> tuple[Expr, ...]:
    """Build expressions for the ast_type_ignores view.

    Returns
    -------
    tuple[Expr, ...]
        View select expressions.

    Notes
    -----
    Original definition was 16 lines. DSL version is 9 lines.
    """
    return (
        ViewExprBuilder()
        .add_identity_cols("file_id", "path", "ast_id", "tag")
        .add_span_fields()
        .add_attrs_col()
        .add_ast_record()
        .build()
    )


def build_ast_node_attrs_exprs() -> tuple[Expr, ...]:
    """Build expressions for the ast_node_attrs view.

    Returns
    -------
    tuple[Expr, ...]
        View select expressions.

    Notes
    -----
    Original definition was 9 lines. DSL version is 6 lines.
    """
    return (
        ViewExprBuilder()
        .add_identity_cols("file_id", "path", "ast_id", "parent_ast_id", "kind", "name")
        .add_kv_extraction()
        .build()
    )


# Dictionary mapping view names to DSL builders (views that don't require UDF platform)
DSL_VIEW_BUILDERS: dict[str, tuple[Expr, ...]] = {
    "ast_docstrings": build_ast_docstrings_exprs(),
    "ast_errors": build_ast_errors_exprs(),
    "ast_imports": build_ast_imports_exprs(),
    "ast_def_attrs": build_ast_def_attrs_exprs(),
    "ast_edge_attrs": build_ast_edge_attrs_exprs(),
    "ast_type_ignores": build_ast_type_ignores_exprs(),
    "ast_node_attrs": build_ast_node_attrs_exprs(),
}


__all__ = [
    "DSL_VIEW_BUILDERS",
    "build_ast_def_attrs_exprs",
    "build_ast_docstrings_exprs",
    "build_ast_edge_attrs_exprs",
    "build_ast_errors_exprs",
    "build_ast_imports_exprs",
    "build_ast_node_attrs_exprs",
    "build_ast_type_ignores_exprs",
]
