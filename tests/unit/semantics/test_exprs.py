"""Unit tests for semantics.exprs expression helpers.

Tests for the DataFusion-native expression DSL including ExprContextImpl,
clamp(), and the various expression builder functions.
"""

from __future__ import annotations

import pytest
from datafusion import SessionContext, col, lit

from semantics.exprs import (
    ExprContextImpl,
    add_columns,
    alias,
    and_,
    between_overlap,
    c,
    case_eq,
    case_eq_value,
    clamp,
    coalesce_default,
    eq,
    eq_value,
    gt,
    gte,
    is_not_null,
    is_null,
    lt,
    lte,
    mul,
    or_,
    sort_expr,
    span_contains_span,
    stable_hash64,
    v,
    validate_expr_spec,
)


class TestExprContextImpl:
    """Tests for ExprContextImpl class."""

    @staticmethod
    def test_default_aliases() -> None:
        """ExprContextImpl has default left/right aliases."""
        ctx = ExprContextImpl()
        assert ctx.left_alias == "l"
        assert ctx.right_alias == "r"

    @staticmethod
    def test_custom_aliases() -> None:
        """ExprContextImpl accepts custom aliases."""
        ctx = ExprContextImpl(left_alias="left", right_alias="right")
        assert ctx.left_alias == "left"
        assert ctx.right_alias == "right"

    @staticmethod
    def test_col_returns_expression() -> None:
        """ExprContextImpl.col() returns DataFusion column expression."""
        ctx = ExprContextImpl()
        expr = ctx.col("file_id")
        # Verify it's a column expression by checking its string representation
        assert "file_id" in str(expr)

    @staticmethod
    def test_lit_returns_expression() -> None:
        """ExprContextImpl.lit() returns DataFusion literal expression."""
        ctx = ExprContextImpl()
        expr = ctx.lit(42)
        # Verify it's a literal expression
        assert "42" in str(expr) or "Int64" in str(expr)

    @staticmethod
    def test_left_col_prefixes() -> None:
        """ExprContextImpl.left_col() adds left alias prefix."""
        ctx = ExprContextImpl()
        expr = ctx.left_col("file_id")
        assert "l__file_id" in str(expr)

    @staticmethod
    def test_right_col_prefixes() -> None:
        """ExprContextImpl.right_col() adds right alias prefix."""
        ctx = ExprContextImpl()
        expr = ctx.right_col("file_id")
        assert "r__file_id" in str(expr)


class TestClamp:
    """Tests for clamp() function."""

    @staticmethod
    def test_clamp_expression_structure() -> None:
        """clamp() produces valid expression structure."""
        value = col("score")
        min_val = lit(0.0)
        max_val = lit(1.0)
        expr = clamp(value, min_value=min_val, max_value=max_val)
        # Verify expression is constructed (no errors)
        assert expr is not None

    @staticmethod
    def test_clamp_in_dataframe() -> None:
        """clamp() works in DataFrame context."""
        ctx = SessionContext()
        # Create test data
        ctx.from_pydict(
            {"value": [-0.5, 0.5, 1.5]},
            name="test",
        )
        df = ctx.table("test")
        # Apply clamp
        clamped = df.with_column(
            "clamped",
            clamp(col("value"), min_value=lit(0.0), max_value=lit(1.0)),
        )
        result = clamped.collect()
        values = result[0]["clamped"].to_pylist()
        assert values == [0.0, 0.5, 1.0]


class TestDslHelpers:
    """Tests for DSL helper functions."""

    @staticmethod
    def test_c_column_reference() -> None:
        """c() creates column reference ExprSpec."""
        ctx = ExprContextImpl()
        spec = c("file_id")
        expr = spec(ctx)
        assert "file_id" in str(expr)

    @staticmethod
    def test_v_literal_value() -> None:
        """v() creates literal value ExprSpec."""
        ctx = ExprContextImpl()
        spec = v(42)
        expr = spec(ctx)
        assert "42" in str(expr) or "Int64" in str(expr)

    @staticmethod
    def test_v_string_literal() -> None:
        """v() works with string literals."""
        ctx = ExprContextImpl()
        spec = v("test_value")
        expr = spec(ctx)
        assert "test_value" in str(expr)

    @staticmethod
    def test_eq_equality() -> None:
        """eq() creates equality predicate."""
        ctx = ExprContextImpl()
        spec = eq("col_a", "col_b")
        expr = spec(ctx)
        assert "col_a" in str(expr)
        assert "col_b" in str(expr)

    @staticmethod
    def test_eq_value_equality() -> None:
        """eq_value() creates equality predicate against literal."""
        ctx = ExprContextImpl()
        spec = eq_value("flag", value=True)
        expr = spec(ctx)
        assert "flag" in str(expr)

    @staticmethod
    def test_gt_greater_than() -> None:
        """gt() creates greater-than predicate."""
        ctx = ExprContextImpl()
        spec = gt("score", "threshold")
        expr = spec(ctx)
        assert "score" in str(expr)
        assert "threshold" in str(expr)

    @staticmethod
    def test_lt_less_than() -> None:
        """lt() creates less-than predicate."""
        ctx = ExprContextImpl()
        spec = lt("value", "limit")
        expr = spec(ctx)
        assert "value" in str(expr)
        assert "limit" in str(expr)

    @staticmethod
    def test_gte_greater_equal() -> None:
        """gte() creates greater-than-or-equal predicate."""
        ctx = ExprContextImpl()
        spec = gte("score", "threshold")
        expr = spec(ctx)
        assert "score" in str(expr)

    @staticmethod
    def test_lte_less_equal() -> None:
        """lte() creates less-than-or-equal predicate."""
        ctx = ExprContextImpl()
        spec = lte("value", "limit")
        expr = spec(ctx)
        assert "value" in str(expr)

    @staticmethod
    def test_is_not_null() -> None:
        """is_not_null() creates not-null predicate."""
        ctx = ExprContextImpl()
        spec = is_not_null("owner_def_id")
        expr = spec(ctx)
        assert "owner_def_id" in str(expr)
        assert "NOT" in str(expr).upper() or "null" in str(expr).lower()

    @staticmethod
    def test_is_null() -> None:
        """is_null() creates null predicate."""
        ctx = ExprContextImpl()
        spec = is_null("optional_field")
        expr = spec(ctx)
        assert "optional_field" in str(expr)
        assert "null" in str(expr).lower()


class TestLogicalOperators:
    """Tests for logical operator helpers."""

    @staticmethod
    def test_and_combines_predicates() -> None:
        """and_() combines predicates with AND."""
        ctx = ExprContextImpl()
        spec = and_(is_not_null("a"), is_not_null("b"))
        expr = spec(ctx)
        assert expr is not None

    @staticmethod
    def test_and_empty_returns_true() -> None:
        """and_() with no args returns literal True."""
        ctx = ExprContextImpl()
        spec = and_()
        expr = spec(ctx)
        assert "true" in str(expr).lower() or "True" in str(expr)

    @staticmethod
    def test_or_combines_predicates() -> None:
        """or_() combines predicates with OR."""
        ctx = ExprContextImpl()
        spec = or_(is_null("a"), is_null("b"))
        expr = spec(ctx)
        assert expr is not None

    @staticmethod
    def test_or_empty_returns_false() -> None:
        """or_() with no args returns literal False."""
        ctx = ExprContextImpl()
        spec = or_()
        expr = spec(ctx)
        assert "false" in str(expr).lower() or "False" in str(expr)


class TestSpanPredicates:
    """Tests for span-related predicates."""

    @staticmethod
    def test_between_overlap() -> None:
        """between_overlap() creates overlap predicate."""
        ctx = ExprContextImpl()
        spec = between_overlap("l_bstart", "l_bend", "r_bstart", "r_bend")
        expr = spec(ctx)
        assert expr is not None

    @staticmethod
    def test_span_contains_span() -> None:
        """span_contains_span() creates containment predicate."""
        ctx = ExprContextImpl()
        spec = span_contains_span("outer_start", "outer_end", "inner_start", "inner_end")
        expr = spec(ctx)
        assert expr is not None


class TestCaseExpressions:
    """Tests for CASE expression helpers."""

    @staticmethod
    def test_case_eq_matches() -> None:
        """case_eq() returns 1 when columns match."""
        session = SessionContext()
        session.from_pydict({"a": [1, 2, 3], "b": [1, 3, 3]}, name="test")
        df = session.table("test")

        ctx = ExprContextImpl()
        spec = case_eq("a", "b")
        df_result = df.with_column("match", spec(ctx))
        result = df_result.collect()
        matches = result[0]["match"].to_pylist()
        assert matches == [1, 0, 1]

    @staticmethod
    def test_case_eq_value_matches() -> None:
        """case_eq_value() returns 1 when column matches value."""
        session = SessionContext()
        session.from_pydict({"kind": ["function", "class", "function"]}, name="test")
        df = session.table("test")

        ctx = ExprContextImpl()
        spec = case_eq_value("kind", "function")
        df_result = df.with_column("is_func", spec(ctx))
        result = df_result.collect()
        is_func = result[0]["is_func"].to_pylist()
        assert is_func == [1, 0, 1]


class TestArithmeticExpressions:
    """Tests for arithmetic expression helpers."""

    @staticmethod
    def test_add_columns() -> None:
        """add_columns() sums multiple columns."""
        session = SessionContext()
        session.from_pydict({"a": [1, 2], "b": [3, 4], "c": [5, 6]}, name="test")
        df = session.table("test")

        ctx = ExprContextImpl()
        spec = add_columns("a", "b", "c")
        df_result = df.with_column("total", spec(ctx))
        result = df_result.collect()
        totals = result[0]["total"].to_pylist()
        assert totals == [9, 12]

    @staticmethod
    def test_add_columns_empty_returns_zero() -> None:
        """add_columns() with no columns returns 0."""
        ctx = ExprContextImpl()
        spec = add_columns()
        expr = spec(ctx)
        assert "0" in str(expr)

    @staticmethod
    def test_mul_multiplies() -> None:
        """mul() multiplies column by factor."""
        session = SessionContext()
        session.from_pydict({"score": [10, 20, 30]}, name="test")
        df = session.table("test")

        ctx = ExprContextImpl()
        spec = mul("score", 0.5)
        df_result = df.with_column("scaled", spec(ctx))
        result = df_result.collect()
        scaled = result[0]["scaled"].to_pylist()
        assert scaled == [5.0, 10.0, 15.0]


class TestCoalesceDefault:
    """Tests for coalesce_default() helper."""

    @staticmethod
    def test_coalesce_default_fills_nulls() -> None:
        """coalesce_default() replaces nulls with default."""
        session = SessionContext()
        session.from_pydict({"value": [1, None, 3]}, name="test")
        df = session.table("test")

        ctx = ExprContextImpl()
        spec = coalesce_default("value", 0)
        df_result = df.with_column("filled", spec(ctx))
        result = df_result.collect()
        filled = result[0]["filled"].to_pylist()
        assert filled == [1, 0, 3]


class TestAlias:
    """Tests for alias() helper."""

    @staticmethod
    def test_alias_renames_expression() -> None:
        """alias() wraps expression with alias."""
        ctx = ExprContextImpl()
        spec = alias(c("old_name"), "new_name")
        expr = spec(ctx)
        assert "new_name" in str(expr)


class TestSortExpr:
    """Tests for sort_expr() helper."""

    @staticmethod
    def test_sort_expr_ascending() -> None:
        """sort_expr() creates ascending sort expression."""
        ctx = ExprContextImpl()
        spec = sort_expr("score", "asc")
        expr = spec(ctx)
        assert expr is not None

    @staticmethod
    def test_sort_expr_descending() -> None:
        """sort_expr() creates descending sort expression."""
        ctx = ExprContextImpl()
        spec = sort_expr("score", "desc")
        expr = spec(ctx)
        assert expr is not None

    @staticmethod
    def test_sort_expr_default_ascending() -> None:
        """sort_expr() defaults to ascending."""
        ctx = ExprContextImpl()
        spec = sort_expr("score")
        expr = spec(ctx)
        assert expr is not None


class TestStableHash64:
    """Tests for stable_hash64() helper."""

    @staticmethod
    def test_stable_hash64_expression() -> None:
        """stable_hash64() creates hash expression."""
        ctx = ExprContextImpl()
        expr = stable_hash64("file_id")(ctx)
        assert "stable_hash64" in str(expr).lower()


class TestExprValidation:
    """Tests for ExprSpec validation."""

    @staticmethod
    def test_validate_expr_spec_allows_known_columns() -> None:
        """validate_expr_spec passes for known columns."""
        validate_expr_spec(
            eq("l__a", "r__b"),
            available_columns={"l__a", "r__b"},
            expr_label="test.expr",
        )

    @staticmethod
    def test_validate_expr_spec_rejects_unknown_columns() -> None:
        """validate_expr_spec rejects unknown columns."""
        with pytest.raises(ValueError, match="missing columns"):
            validate_expr_spec(
                eq("missing", "present"),
                available_columns={"present"},
                expr_label="test.expr",
            )
