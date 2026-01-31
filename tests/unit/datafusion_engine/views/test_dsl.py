"""Tests for the view expression DSL."""

from __future__ import annotations

from datafusion.expr import Expr

from datafusion_engine.views.dsl import (
    SpanExprs,
    ViewExprBuilder,
    identity_cols,
    kv_extraction,
    span_fields,
)

# Expected expression counts for views
SPAN_FIELDS_COUNT = 8
SPAN_FIELDS_WITH_BYTES_COUNT = 10
KV_EXTRACTION_COUNT = 2


# Tests for SpanExprs class


def test_span_col_default() -> None:
    """Verify default span column is 'span'."""
    span = SpanExprs()
    assert span.span_col == "span"


def test_span_col_custom() -> None:
    """Verify custom span column is preserved."""
    span = SpanExprs("custom_span")
    assert span.span_col == "custom_span"


def test_all_span_fields_count() -> None:
    """Verify all_span_fields returns 8 expressions."""
    span = SpanExprs()
    fields = span.all_span_fields()
    assert len(fields) == SPAN_FIELDS_COUNT


def test_all_span_fields_with_bytes_count() -> None:
    """Verify all_span_fields_with_bytes returns 10 expressions."""
    span = SpanExprs()
    fields = span.all_span_fields_with_bytes()
    assert len(fields) == SPAN_FIELDS_WITH_BYTES_COUNT


def test_lineno_expression_type() -> None:
    """Verify lineno returns an Expr."""
    span = SpanExprs()
    expr = span.lineno()
    assert isinstance(expr, Expr)


def test_col_offset_expression_type() -> None:
    """Verify col_offset returns an Expr."""
    span = SpanExprs()
    expr = span.col_offset()
    assert isinstance(expr, Expr)


# Tests for ViewExprBuilder class


def test_add_identity_cols() -> None:
    """Verify add_identity_cols adds correct number of expressions."""
    builder = ViewExprBuilder()
    builder.add_identity_cols("a", "b", "c")
    expected_count = 3
    assert len(builder.build()) == expected_count


def test_add_span_fields() -> None:
    """Verify add_span_fields adds 8 expressions."""
    builder = ViewExprBuilder()
    builder.add_span_fields()
    assert len(builder.build()) == SPAN_FIELDS_COUNT


def test_add_span_fields_with_bytes() -> None:
    """Verify add_span_fields_with_bytes adds 10 expressions."""
    builder = ViewExprBuilder()
    builder.add_span_fields_with_bytes()
    assert len(builder.build()) == SPAN_FIELDS_WITH_BYTES_COUNT


def test_add_kv_extraction() -> None:
    """Verify add_kv_extraction adds 2 expressions."""
    builder = ViewExprBuilder()
    builder.add_kv_extraction()
    assert len(builder.build()) == KV_EXTRACTION_COUNT


def test_add_attrs_col() -> None:
    """Verify add_attrs_col adds 1 expression."""
    builder = ViewExprBuilder()
    builder.add_attrs_col()
    assert len(builder.build()) == 1


def test_add_ast_record() -> None:
    """Verify add_ast_record adds 1 expression."""
    builder = ViewExprBuilder()
    builder.add_ast_record()
    assert len(builder.build()) == 1


def test_chaining() -> None:
    """Verify methods can be chained."""
    exprs = (
        ViewExprBuilder()
        .add_identity_cols("file_id", "path")
        .add_span_fields()
        .add_attrs_col()
        .add_ast_record()
        .build()
    )
    # 2 identity + 8 span + 1 attrs + 1 ast_record = 12
    expected_count = 12
    assert len(exprs) == expected_count


def test_build_returns_tuple() -> None:
    """Verify build returns a tuple."""
    builder = ViewExprBuilder()
    builder.add_identity_cols("a")
    result = builder.build()
    assert isinstance(result, tuple)


# Tests for convenience factory functions


def test_identity_cols() -> None:
    """Verify identity_cols creates correct expressions."""
    exprs = identity_cols("a", "b", "c")
    expected_count = 3
    assert len(exprs) == expected_count
    assert all(isinstance(e, Expr) for e in exprs)


def test_span_fields() -> None:
    """Verify span_fields creates 8 expressions."""
    exprs = span_fields()
    assert len(exprs) == SPAN_FIELDS_COUNT


def test_kv_extraction() -> None:
    """Verify kv_extraction creates 2 expressions."""
    exprs = kv_extraction()
    assert len(exprs) == KV_EXTRACTION_COUNT


# Tests for DSL-based view definitions


def test_ast_docstrings_exprs() -> None:
    """Verify ast_docstrings DSL builder produces correct expression count."""
    from datafusion_engine.views.dsl_views import build_ast_docstrings_exprs

    exprs = build_ast_docstrings_exprs()
    # 7 identity + 8 span + 1 attrs + 1 ast_record = 17
    expected_count = 17
    assert len(exprs) == expected_count


def test_ast_errors_exprs() -> None:
    """Verify ast_errors DSL builder produces correct expression count."""
    from datafusion_engine.views.dsl_views import build_ast_errors_exprs

    exprs = build_ast_errors_exprs()
    # 4 identity + 8 span + 1 attrs + 1 ast_record = 14
    expected_count = 14
    assert len(exprs) == expected_count


def test_ast_imports_exprs() -> None:
    """Verify ast_imports DSL builder produces correct expression count."""
    from datafusion_engine.views.dsl_views import build_ast_imports_exprs

    exprs = build_ast_imports_exprs()
    # 10 identity + 8 span + 1 attrs + 1 ast_record = 20
    expected_count = 20
    assert len(exprs) == expected_count


def test_ast_def_attrs_exprs() -> None:
    """Verify ast_def_attrs DSL builder produces correct expression count."""
    from datafusion_engine.views.dsl_views import build_ast_def_attrs_exprs

    exprs = build_ast_def_attrs_exprs()
    # 6 identity + 2 kv = 8
    expected_count = 8
    assert len(exprs) == expected_count


def test_ast_edge_attrs_exprs() -> None:
    """Verify ast_edge_attrs DSL builder produces correct expression count."""
    from datafusion_engine.views.dsl_views import build_ast_edge_attrs_exprs

    exprs = build_ast_edge_attrs_exprs()
    # 7 identity + 2 kv = 9
    expected_count = 9
    assert len(exprs) == expected_count


def test_ast_type_ignores_exprs() -> None:
    """Verify ast_type_ignores DSL builder produces correct expression count."""
    from datafusion_engine.views.dsl_views import build_ast_type_ignores_exprs

    exprs = build_ast_type_ignores_exprs()
    # 4 identity + 8 span + 1 attrs + 1 ast_record = 14
    expected_count = 14
    assert len(exprs) == expected_count


def test_dsl_view_builders_dict() -> None:
    """Verify DSL_VIEW_BUILDERS dictionary is populated."""
    from datafusion_engine.views.dsl_views import DSL_VIEW_BUILDERS

    min_views = 7
    assert len(DSL_VIEW_BUILDERS) >= min_views
    for exprs in DSL_VIEW_BUILDERS.values():
        assert isinstance(exprs, tuple)
        assert all(isinstance(e, Expr) for e in exprs)
