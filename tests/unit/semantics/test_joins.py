"""Unit tests for join strategy inference."""

from __future__ import annotations

import pyarrow as pa
import pytest

from semantics.joins import (
    FILE_EQUI_JOIN,
    SPAN_CONTAINS_STRATEGY,
    SPAN_OVERLAP_STRATEGY,
    JoinInferenceError,
    JoinStrategyType,
    infer_join_strategy,
)
from semantics.joins.inference import JoinCapabilities, require_join_strategy
from semantics.joins.strategies import make_fk_strategy, make_symbol_match_strategy
from semantics.types import AnnotatedSchema


class TestJoinStrategyType:
    """Test JoinStrategyType enum."""

    def test_strategy_types_are_strings(self) -> None:
        """Verify strategy types are StrEnum members."""
        assert JoinStrategyType.EQUI_JOIN == "equi_join"
        assert JoinStrategyType.SPAN_OVERLAP == "span_overlap"
        assert JoinStrategyType.SPAN_CONTAINS == "span_contains"
        assert JoinStrategyType.FOREIGN_KEY == "foreign_key"
        assert JoinStrategyType.SYMBOL_MATCH == "symbol_match"


class TestJoinStrategy:
    """Test JoinStrategy dataclass."""

    def test_describe_equi_join(self) -> None:
        """Verify equi-join description."""
        strategy = FILE_EQUI_JOIN
        assert "EquiJoin" in strategy.describe()
        assert "file_id=file_id" in strategy.describe()

    def test_describe_span_overlap(self) -> None:
        """Verify span overlap description."""
        strategy = SPAN_OVERLAP_STRATEGY
        assert "SpanOverlap" in strategy.describe()

    def test_describe_span_contains(self) -> None:
        """Verify span contains description."""
        strategy = SPAN_CONTAINS_STRATEGY
        assert "SpanContains" in strategy.describe()

    def test_describe_fk(self) -> None:
        """Verify foreign key description."""
        strategy = make_fk_strategy("def_id", "entity_id")
        assert "ForeignKey" in strategy.describe()

    def test_describe_symbol_match(self) -> None:
        """Verify symbol match description."""
        strategy = make_symbol_match_strategy()
        assert "SymbolMatch" in strategy.describe()

    def test_with_filter(self) -> None:
        """Verify filter expression chaining."""
        strategy = FILE_EQUI_JOIN.with_filter("is_read = true")
        assert strategy.filter_expr == "is_read = true"

        chained = strategy.with_filter("is_write = false")
        assert chained.filter_expr is not None
        assert "(is_read = true)" in chained.filter_expr
        assert "(is_write = false)" in chained.filter_expr


class TestJoinCapabilities:
    """Test JoinCapabilities extraction."""

    def test_extract_file_identity(self) -> None:
        """Verify file identity detection."""
        schema = pa.schema([("file_id", pa.string()), ("data", pa.int64())])
        annotated = AnnotatedSchema.from_arrow_schema(schema)
        caps = JoinCapabilities.from_schema(annotated)

        assert caps.has_file_identity is True
        assert caps.has_spans is False

    def test_extract_spans(self) -> None:
        """Verify span detection."""
        schema = pa.schema(
            [
                ("file_id", pa.string()),
                ("bstart", pa.int64()),
                ("bend", pa.int64()),
            ]
        )
        annotated = AnnotatedSchema.from_arrow_schema(schema)
        caps = JoinCapabilities.from_schema(annotated)

        assert caps.has_file_identity is True
        assert caps.has_spans is True

    def test_extract_entity_id(self) -> None:
        """Verify entity_id detection."""
        schema = pa.schema([("entity_id", pa.string()), ("value", pa.string())])
        annotated = AnnotatedSchema.from_arrow_schema(schema)
        caps = JoinCapabilities.from_schema(annotated)

        assert caps.has_entity_id is True

    def test_extract_fk_columns(self) -> None:
        """Verify FK column detection."""
        schema = pa.schema(
            [
                ("entity_id", pa.string()),
                ("def_id", pa.string()),
                ("ref_id", pa.string()),
            ]
        )
        annotated = AnnotatedSchema.from_arrow_schema(schema)
        caps = JoinCapabilities.from_schema(annotated)

        # def_id and ref_id are detected as FK columns (entity_id excluded)
        assert "def_id" in caps.fk_columns
        assert "ref_id" in caps.fk_columns
        assert "entity_id" not in caps.fk_columns

    def test_extract_symbol(self) -> None:
        """Verify symbol detection."""
        schema = pa.schema([("symbol", pa.string()), ("data", pa.int64())])
        annotated = AnnotatedSchema.from_arrow_schema(schema)
        caps = JoinCapabilities.from_schema(annotated)

        assert caps.has_symbol is True


class TestInferJoinStrategy:
    """Test infer_join_strategy function."""

    def test_infer_span_overlap(self) -> None:
        """Infer span overlap when both schemas have file + spans."""
        left = pa.schema(
            [
                ("file_id", pa.string()),
                ("bstart", pa.int64()),
                ("bend", pa.int64()),
            ]
        )
        right = pa.schema(
            [
                ("file_id", pa.string()),
                ("bstart", pa.int64()),
                ("bend", pa.int64()),
            ]
        )
        left_annotated = AnnotatedSchema.from_arrow_schema(left)
        right_annotated = AnnotatedSchema.from_arrow_schema(right)

        strategy = infer_join_strategy(left_annotated, right_annotated)

        assert strategy is not None
        assert strategy.strategy_type == JoinStrategyType.SPAN_OVERLAP
        assert strategy.confidence == 0.95

    def test_infer_span_contains_with_hint(self) -> None:
        """Force span contains with hint."""
        left = pa.schema(
            [
                ("file_id", pa.string()),
                ("bstart", pa.int64()),
                ("bend", pa.int64()),
            ]
        )
        right = pa.schema(
            [
                ("file_id", pa.string()),
                ("bstart", pa.int64()),
                ("bend", pa.int64()),
            ]
        )
        left_annotated = AnnotatedSchema.from_arrow_schema(left)
        right_annotated = AnnotatedSchema.from_arrow_schema(right)

        strategy = infer_join_strategy(
            left_annotated, right_annotated, hint=JoinStrategyType.SPAN_CONTAINS
        )

        assert strategy is not None
        assert strategy.strategy_type == JoinStrategyType.SPAN_CONTAINS
        assert strategy.confidence == 0.95

    def test_infer_fk_join(self) -> None:
        """Infer FK join when left has FK and right has entity_id."""
        left = pa.schema([("def_id", pa.string()), ("name", pa.string())])
        right = pa.schema([("entity_id", pa.string()), ("path", pa.string())])

        left_annotated = AnnotatedSchema.from_arrow_schema(left)
        right_annotated = AnnotatedSchema.from_arrow_schema(right)

        strategy = infer_join_strategy(left_annotated, right_annotated)

        assert strategy is not None
        assert strategy.strategy_type == JoinStrategyType.FOREIGN_KEY
        assert strategy.left_keys == ("def_id",)
        assert strategy.right_keys == ("entity_id",)
        assert strategy.confidence == 0.85

    def test_infer_symbol_join(self) -> None:
        """Infer symbol join when both have symbols."""
        left = pa.schema([("symbol", pa.string()), ("file_id", pa.string())])
        right = pa.schema([("symbol", pa.string()), ("definition", pa.string())])

        left_annotated = AnnotatedSchema.from_arrow_schema(left)
        right_annotated = AnnotatedSchema.from_arrow_schema(right)

        # Symbol join is lower priority than file equi-join
        strategy = infer_join_strategy(
            left_annotated, right_annotated, hint=JoinStrategyType.SYMBOL_MATCH
        )

        assert strategy is not None
        assert strategy.strategy_type == JoinStrategyType.SYMBOL_MATCH
        assert strategy.confidence == 0.75

    def test_infer_file_equi_join(self) -> None:
        """Infer file equi-join when both have file_id but no spans."""
        left = pa.schema([("file_id", pa.string()), ("name", pa.string())])
        right = pa.schema([("file_id", pa.string()), ("value", pa.int64())])

        left_annotated = AnnotatedSchema.from_arrow_schema(left)
        right_annotated = AnnotatedSchema.from_arrow_schema(right)

        strategy = infer_join_strategy(left_annotated, right_annotated)

        assert strategy is not None
        assert strategy.strategy_type == JoinStrategyType.EQUI_JOIN
        assert strategy.confidence == 0.6

    def test_infer_none_no_common(self) -> None:
        """Return None when no common join capability."""
        left = pa.schema([("name", pa.string())])
        right = pa.schema([("value", pa.int64())])

        left_annotated = AnnotatedSchema.from_arrow_schema(left)
        right_annotated = AnnotatedSchema.from_arrow_schema(right)

        strategy = infer_join_strategy(left_annotated, right_annotated)

        assert strategy is None

    def test_hint_unsatisfied_returns_none(self) -> None:
        """Return None when hint cannot be satisfied."""
        left = pa.schema([("file_id", pa.string())])
        right = pa.schema([("file_id", pa.string())])

        left_annotated = AnnotatedSchema.from_arrow_schema(left)
        right_annotated = AnnotatedSchema.from_arrow_schema(right)

        # Span overlap hint unsatisfied (no spans)
        strategy = infer_join_strategy(
            left_annotated, right_annotated, hint=JoinStrategyType.SPAN_OVERLAP
        )

        assert strategy is None


class TestRequireJoinStrategy:
    """Test require_join_strategy function."""

    def test_returns_strategy_on_success(self) -> None:
        """Return strategy when inference succeeds."""
        left = pa.schema(
            [
                ("file_id", pa.string()),
                ("bstart", pa.int64()),
                ("bend", pa.int64()),
            ]
        )
        right = pa.schema(
            [
                ("file_id", pa.string()),
                ("bstart", pa.int64()),
                ("bend", pa.int64()),
            ]
        )
        left_annotated = AnnotatedSchema.from_arrow_schema(left)
        right_annotated = AnnotatedSchema.from_arrow_schema(right)

        strategy = require_join_strategy(left_annotated, right_annotated)

        assert strategy.strategy_type == JoinStrategyType.SPAN_OVERLAP

    def test_raises_on_failure(self) -> None:
        """Raise JoinInferenceError when inference fails."""
        left = pa.schema([("name", pa.string())])
        right = pa.schema([("value", pa.int64())])

        left_annotated = AnnotatedSchema.from_arrow_schema(left)
        right_annotated = AnnotatedSchema.from_arrow_schema(right)

        with pytest.raises(JoinInferenceError) as exc_info:
            require_join_strategy(
                left_annotated, right_annotated, left_name="refs", right_name="defs"
            )

        assert "refs" in str(exc_info.value)
        assert "defs" in str(exc_info.value)
        assert "file_identity=False" in str(exc_info.value)

    def test_error_includes_hint(self) -> None:
        """Error message includes unsatisfied hint."""
        left = pa.schema([("file_id", pa.string())])
        right = pa.schema([("file_id", pa.string())])

        left_annotated = AnnotatedSchema.from_arrow_schema(left)
        right_annotated = AnnotatedSchema.from_arrow_schema(right)

        with pytest.raises(JoinInferenceError) as exc_info:
            require_join_strategy(
                left_annotated, right_annotated, hint=JoinStrategyType.SPAN_OVERLAP
            )

        assert "span_overlap" in str(exc_info.value)
