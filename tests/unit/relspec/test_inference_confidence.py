"""Tests for inference confidence model."""

from __future__ import annotations

from relspec.inference_confidence import (
    InferenceConfidence,
    high_confidence,
    low_confidence,
)
from tests.test_helpers.immutability import assert_immutable_assignment

CONFIDENCE_SCORE_FULL = 0.85
CONFIDENCE_SCORE_MINIMAL = 0.5
HIGH_CONFIDENCE_DEFAULT = 0.9
HIGH_CONFIDENCE_CUSTOM = 0.95
HIGH_CONFIDENCE_MIN = 0.8
LOW_CONFIDENCE_DEFAULT = 0.4
LOW_CONFIDENCE_CUSTOM = 0.3
LOW_CONFIDENCE_MAX = 0.49


class TestInferenceConfidence:
    """Test InferenceConfidence struct construction and field access."""

    def test_construction_with_all_fields(self) -> None:
        """Construct with every field populated."""
        conf = InferenceConfidence(
            confidence_score=CONFIDENCE_SCORE_FULL,
            evidence_sources=("lineage", "stats"),
            fallback_reason=None,
            decision_type="scan_policy",
            decision_value="small_table",
        )
        assert conf.confidence_score == CONFIDENCE_SCORE_FULL
        assert conf.evidence_sources == ("lineage", "stats")
        assert conf.fallback_reason is None
        assert conf.decision_type == "scan_policy"
        assert conf.decision_value == "small_table"

    def test_construction_minimal(self) -> None:
        """Construct with only required fields uses defaults."""
        conf = InferenceConfidence(confidence_score=CONFIDENCE_SCORE_MINIMAL)
        assert conf.confidence_score == CONFIDENCE_SCORE_MINIMAL
        assert conf.evidence_sources == ()
        assert conf.fallback_reason is None
        assert conf.decision_type == ""
        assert conf.decision_value == ""

    def test_frozen(self) -> None:
        """Verify struct is immutable."""
        conf = InferenceConfidence(confidence_score=CONFIDENCE_SCORE_MINIMAL)
        assert_immutable_assignment(
            factory=lambda: conf,
            attribute="confidence_score",
            attempted_value=0.9,
            expected_exception=AttributeError,
        )

    def test_confidence_boundary_values(self) -> None:
        """Construct at boundary values of 0 and 1."""
        low = InferenceConfidence(confidence_score=0.0)
        assert low.confidence_score == 0.0
        high = InferenceConfidence(confidence_score=1.0)
        assert high.confidence_score == 1.0


class TestHighConfidence:
    """Test the high_confidence helper."""

    def test_default_score(self) -> None:
        """Default score is 0.9."""
        conf = high_confidence(
            "scan_policy",
            "small_table",
            ("stats", "capabilities"),
        )
        assert conf.confidence_score == HIGH_CONFIDENCE_DEFAULT
        assert conf.decision_type == "scan_policy"
        assert conf.decision_value == "small_table"
        assert conf.evidence_sources == ("stats", "capabilities")
        assert conf.fallback_reason is None

    def test_custom_score_above_threshold(self) -> None:
        """Custom score >= 0.8 is preserved."""
        conf = high_confidence(
            "join_strategy",
            "span_overlap",
            ("lineage",),
            score=HIGH_CONFIDENCE_CUSTOM,
        )
        assert conf.confidence_score == HIGH_CONFIDENCE_CUSTOM

    def test_score_clamped_to_minimum(self) -> None:
        """Score below 0.8 is clamped up to 0.8."""
        conf = high_confidence(
            "cache_policy",
            "cached",
            (),
            score=CONFIDENCE_SCORE_MINIMAL,
        )
        assert conf.confidence_score == HIGH_CONFIDENCE_MIN

    def test_score_clamped_to_maximum(self) -> None:
        """Score above 1.0 is clamped to 1.0."""
        conf = high_confidence(
            "cache_policy",
            "cached",
            (),
            score=1.5,
        )
        assert conf.confidence_score == 1.0


class TestLowConfidence:
    """Test the low_confidence helper."""

    def test_default_score(self) -> None:
        """Default score is 0.4."""
        conf = low_confidence(
            "scan_policy",
            "no_override",
            "insufficient_stats",
            ("lineage",),
        )
        assert conf.confidence_score == LOW_CONFIDENCE_DEFAULT
        assert conf.decision_type == "scan_policy"
        assert conf.decision_value == "no_override"
        assert conf.fallback_reason == "insufficient_stats"
        assert conf.evidence_sources == ("lineage",)

    def test_custom_score_below_threshold(self) -> None:
        """Custom score < 0.5 is preserved."""
        conf = low_confidence(
            "join_strategy",
            "equi_join",
            "no_spans",
            (),
            score=LOW_CONFIDENCE_CUSTOM,
        )
        assert conf.confidence_score == LOW_CONFIDENCE_CUSTOM

    def test_score_clamped_to_maximum(self) -> None:
        """Score >= 0.5 is clamped to 0.49."""
        conf = low_confidence(
            "scan_policy",
            "default",
            "missing_data",
            (),
            score=HIGH_CONFIDENCE_MIN,
        )
        assert conf.confidence_score == LOW_CONFIDENCE_MAX

    def test_score_clamped_to_minimum(self) -> None:
        """Score below 0.0 is clamped to 0.0."""
        conf = low_confidence(
            "scan_policy",
            "default",
            "no_evidence",
            (),
            score=-0.5,
        )
        assert conf.confidence_score == 0.0
