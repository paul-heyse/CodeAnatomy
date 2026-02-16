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
    build_join_inference_confidence,
    infer_join_strategy,
    infer_join_strategy_with_confidence,
)
from semantics.joins.inference import JoinCapabilities, JoinStrategyResult, require_join_strategy
from semantics.joins.strategies import make_fk_strategy, make_symbol_match_strategy
from semantics.types import AnnotatedSchema
from tests.test_helpers.immutability import assert_immutable_assignment

SPAN_CONFIDENCE = 0.95
FK_CONFIDENCE = 0.85
SYMBOL_CONFIDENCE = 0.75
EQUI_JOIN_CONFIDENCE = 0.6
HIGH_CONFIDENCE_THRESHOLD = 0.8
LOW_CONFIDENCE_THRESHOLD = 0.5


class TestJoinStrategyType:
    """Test JoinStrategyType enum."""

    @staticmethod
    def test_strategy_types_are_strings() -> None:
        """Verify strategy types are StrEnum members."""
        assert JoinStrategyType.EQUI_JOIN == "equi_join"
        assert JoinStrategyType.SPAN_OVERLAP == "span_overlap"
        assert JoinStrategyType.SPAN_CONTAINS == "span_contains"
        assert JoinStrategyType.FOREIGN_KEY == "foreign_key"
        assert JoinStrategyType.SYMBOL_MATCH == "symbol_match"


class TestJoinStrategy:
    """Test JoinStrategy dataclass."""

    @staticmethod
    def test_describe_equi_join() -> None:
        """Verify equi-join description."""
        strategy = FILE_EQUI_JOIN
        assert "EquiJoin" in strategy.describe()
        assert "file_id=file_id" in strategy.describe()

    @staticmethod
    def test_describe_span_overlap() -> None:
        """Verify span overlap description."""
        strategy = SPAN_OVERLAP_STRATEGY
        assert "SpanOverlap" in strategy.describe()

    @staticmethod
    def test_describe_span_contains() -> None:
        """Verify span contains description."""
        strategy = SPAN_CONTAINS_STRATEGY
        assert "SpanContains" in strategy.describe()

    @staticmethod
    def test_describe_fk() -> None:
        """Verify foreign key description."""
        strategy = make_fk_strategy("def_id", "entity_id")
        assert "ForeignKey" in strategy.describe()

    @staticmethod
    def test_describe_symbol_match() -> None:
        """Verify symbol match description."""
        strategy = make_symbol_match_strategy()
        assert "SymbolMatch" in strategy.describe()

    @staticmethod
    def test_with_filter() -> None:
        """Verify filter expression chaining."""
        strategy = FILE_EQUI_JOIN.with_filter("is_read = true")
        assert strategy.filter_expr == "is_read = true"

        chained = strategy.with_filter("is_write = false")
        assert chained.filter_expr is not None
        assert "(is_read = true)" in chained.filter_expr
        assert "(is_write = false)" in chained.filter_expr


class TestJoinCapabilities:
    """Test JoinCapabilities extraction."""

    @staticmethod
    def test_extract_file_identity() -> None:
        """Verify file identity detection."""
        schema = pa.schema([("file_id", pa.string()), ("data", pa.int64())])
        annotated = AnnotatedSchema.from_arrow_schema(schema)
        caps = JoinCapabilities.from_schema(annotated)

        assert caps.has_file_identity is True
        assert caps.has_spans is False

    @staticmethod
    def test_extract_spans() -> None:
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

    @staticmethod
    def test_extract_entity_id() -> None:
        """Verify entity_id detection."""
        schema = pa.schema([("entity_id", pa.string()), ("value", pa.string())])
        annotated = AnnotatedSchema.from_arrow_schema(schema)
        caps = JoinCapabilities.from_schema(annotated)

        assert caps.has_entity_id is True

    @staticmethod
    def test_extract_fk_columns() -> None:
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

    @staticmethod
    def test_extract_symbol() -> None:
        """Verify symbol detection."""
        schema = pa.schema([("symbol", pa.string()), ("data", pa.int64())])
        annotated = AnnotatedSchema.from_arrow_schema(schema)
        caps = JoinCapabilities.from_schema(annotated)

        assert caps.has_symbol is True


class TestInferJoinStrategy:
    """Test infer_join_strategy function."""

    @staticmethod
    def test_infer_span_overlap() -> None:
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
        assert strategy.confidence == SPAN_CONFIDENCE

    @staticmethod
    def test_infer_span_contains_with_hint() -> None:
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
        assert strategy.confidence == SPAN_CONFIDENCE

    @staticmethod
    def test_infer_fk_join() -> None:
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
        assert strategy.confidence == FK_CONFIDENCE

    @staticmethod
    def test_infer_symbol_join() -> None:
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
        assert strategy.confidence == SYMBOL_CONFIDENCE

    @staticmethod
    def test_infer_file_equi_join() -> None:
        """Infer file equi-join when both have file_id but no spans."""
        left = pa.schema([("file_id", pa.string()), ("name", pa.string())])
        right = pa.schema([("file_id", pa.string()), ("value", pa.int64())])

        left_annotated = AnnotatedSchema.from_arrow_schema(left)
        right_annotated = AnnotatedSchema.from_arrow_schema(right)

        strategy = infer_join_strategy(left_annotated, right_annotated)

        assert strategy is not None
        assert strategy.strategy_type == JoinStrategyType.EQUI_JOIN
        assert strategy.confidence == EQUI_JOIN_CONFIDENCE

    @staticmethod
    def test_infer_none_no_common() -> None:
        """Return None when no common join capability."""
        left = pa.schema([("name", pa.string())])
        right = pa.schema([("value", pa.int64())])

        left_annotated = AnnotatedSchema.from_arrow_schema(left)
        right_annotated = AnnotatedSchema.from_arrow_schema(right)

        strategy = infer_join_strategy(left_annotated, right_annotated)

        assert strategy is None

    @staticmethod
    def test_hint_unsatisfied_returns_none() -> None:
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

    @staticmethod
    def test_returns_strategy_on_success() -> None:
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

    @staticmethod
    def test_raises_on_failure() -> None:
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

    @staticmethod
    def test_error_includes_hint() -> None:
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


class TestBuildJoinInferenceConfidence:
    """Test build_join_inference_confidence helper."""

    @staticmethod
    def test_none_strategy_returns_none() -> None:
        """Return None when strategy is None."""
        result = build_join_inference_confidence(None)
        assert result is None

    @staticmethod
    def test_high_confidence_for_span_overlap() -> None:
        """Span overlap strategy should produce high confidence."""
        strategy = SPAN_OVERLAP_STRATEGY
        confidence = build_join_inference_confidence(strategy)

        assert confidence is not None
        assert confidence.confidence_score >= HIGH_CONFIDENCE_THRESHOLD
        assert confidence.decision_type == "join_strategy"
        assert confidence.decision_value == "span_overlap"
        assert confidence.fallback_reason is None
        assert "schema" in confidence.evidence_sources

    @staticmethod
    def test_high_confidence_for_fk() -> None:
        """FK strategy should produce high confidence."""
        strategy = make_fk_strategy("def_id", "entity_id")
        # FK confidence is 0.85 in the inference module
        from dataclasses import replace

        strategy = replace(strategy, confidence=FK_CONFIDENCE)
        confidence = build_join_inference_confidence(strategy)

        assert confidence is not None
        assert confidence.confidence_score >= HIGH_CONFIDENCE_THRESHOLD
        assert confidence.decision_type == "join_strategy"
        assert confidence.decision_value == "foreign_key"

    @staticmethod
    def test_low_confidence_for_equi_join() -> None:
        """Equi-join strategy should produce low confidence (0.6 < 0.8 threshold)."""
        from dataclasses import replace

        strategy = replace(FILE_EQUI_JOIN, confidence=EQUI_JOIN_CONFIDENCE)
        confidence = build_join_inference_confidence(strategy)

        assert confidence is not None
        assert confidence.confidence_score < LOW_CONFIDENCE_THRESHOLD
        assert confidence.decision_type == "join_strategy"
        assert confidence.decision_value == "equi_join"
        assert confidence.fallback_reason == "weak_schema_evidence"

    @staticmethod
    def test_evidence_sources_vary_by_strategy_type() -> None:
        """Evidence sources differ by strategy type."""
        from dataclasses import replace

        span_strategy = replace(SPAN_OVERLAP_STRATEGY, confidence=SPAN_CONFIDENCE)
        span_conf = build_join_inference_confidence(span_strategy)
        assert span_conf is not None
        assert "semantic_type" in span_conf.evidence_sources
        assert "compatibility_group" in span_conf.evidence_sources

        symbol_strategy = replace(make_symbol_match_strategy(), confidence=SYMBOL_CONFIDENCE)
        sym_conf = build_join_inference_confidence(symbol_strategy)
        assert sym_conf is not None
        assert "compatibility_group" in sym_conf.evidence_sources


class TestInferJoinStrategyWithConfidence:
    """Test infer_join_strategy_with_confidence function."""

    @staticmethod
    def test_returns_result_for_span_overlap() -> None:
        """Return JoinStrategyResult with confidence for span schemas."""
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

        result = infer_join_strategy_with_confidence(left_annotated, right_annotated)

        assert result is not None
        assert isinstance(result, JoinStrategyResult)
        assert result.strategy.strategy_type == JoinStrategyType.SPAN_OVERLAP
        assert result.confidence.confidence_score >= HIGH_CONFIDENCE_THRESHOLD
        assert result.confidence.decision_type == "join_strategy"

    @staticmethod
    def test_returns_none_when_no_strategy() -> None:
        """Return None when no join strategy can be inferred."""
        left = pa.schema([("name", pa.string())])
        right = pa.schema([("value", pa.int64())])

        left_annotated = AnnotatedSchema.from_arrow_schema(left)
        right_annotated = AnnotatedSchema.from_arrow_schema(right)

        result = infer_join_strategy_with_confidence(left_annotated, right_annotated)

        assert result is None

    @staticmethod
    def test_result_is_frozen() -> None:
        """JoinStrategyResult should be immutable."""
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

        result = infer_join_strategy_with_confidence(left_annotated, right_annotated)
        assert result is not None
        assert_immutable_assignment(
            factory=lambda: result,
            attribute="strategy",
            attempted_value=None,
            expected_exception=AttributeError,
        )
