"""Tests for scoring module."""

from __future__ import annotations

from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    bucket,
    confidence_score,
    impact_score,
)

_IMPACT_SCORE_ZERO = 0.0
_IMPACT_SCORE_ONE = 1.0
_IMPACT_SCORE_LOWER_BOUND = 0.34
_IMPACT_SCORE_UPPER_BOUND = 0.36
_CONFIDENCE_RESOLVED_AST = 0.95
_CONFIDENCE_BYTECODE = 0.90
_CONFIDENCE_HEURISTIC = 0.60
_CONFIDENCE_RG_ONLY = 0.45
_CONFIDENCE_UNRESOLVED = 0.30
_BUCKET_HIGH_THRESHOLD = 0.7
_BUCKET_HIGH_ALT = 0.8
_BUCKET_MED_THRESHOLD = 0.4
_BUCKET_MED_ALT = 0.5
_BUCKET_MED_UPPER = 0.69
_BUCKET_LOW_MIN = 0.0
_BUCKET_LOW_ALT = 0.2
_BUCKET_LOW_UPPER = 0.39
_BUCKET_JUST_BELOW_HIGH = 0.699999
_BUCKET_JUST_BELOW_MED = 0.399999


class TestImpactScore:
    """Tests for impact_score function."""

    @staticmethod
    def test_zero_signals_returns_zero() -> None:
        """Return zero when all impact signals are absent."""
        signals = ImpactSignals()
        assert impact_score(signals) == _IMPACT_SCORE_ZERO

    @staticmethod
    def test_max_signals_returns_one() -> None:
        """Clamp impact score to one for maxed signals."""
        signals = ImpactSignals(
            sites=100,
            files=20,
            depth=10,
            breakages=10,
            ambiguities=10,
        )
        assert impact_score(signals) == _IMPACT_SCORE_ONE

    @staticmethod
    def test_saturates_at_one() -> None:
        """Saturate impact score at one for oversized signals."""
        signals = ImpactSignals(
            sites=500,
            files=100,
            depth=50,
            breakages=50,
            ambiguities=50,
        )
        assert impact_score(signals) == _IMPACT_SCORE_ONE

    @staticmethod
    def test_sites_weight_is_highest() -> None:
        """Weight sites higher than files in impact score."""
        sites_only = ImpactSignals(sites=100)
        files_only = ImpactSignals(files=20)
        assert impact_score(sites_only) > impact_score(files_only)

    @staticmethod
    def test_partial_values() -> None:
        """Return expected score for partial signals."""
        signals = ImpactSignals(sites=50, files=10)
        score = impact_score(signals)
        # Expected score roughly in the mid 0.35 range for partial signals.
        assert _IMPACT_SCORE_LOWER_BOUND < score < _IMPACT_SCORE_UPPER_BOUND

    @staticmethod
    def test_depth_contribution() -> None:
        """Increase impact when depth signal is present."""
        without_depth = ImpactSignals(sites=10, files=5)
        with_depth = ImpactSignals(sites=10, files=5, depth=5)
        assert impact_score(with_depth) > impact_score(without_depth)

    @staticmethod
    def test_breakages_contribution() -> None:
        """Increase impact when breakages signal is present."""
        without_breakages = ImpactSignals(sites=10)
        with_breakages = ImpactSignals(sites=10, breakages=5)
        assert impact_score(with_breakages) > impact_score(without_breakages)

    @staticmethod
    def test_ambiguities_contribution() -> None:
        """Increase impact when ambiguities signal is present."""
        without_ambig = ImpactSignals(sites=10)
        with_ambig = ImpactSignals(sites=10, ambiguities=5)
        assert impact_score(with_ambig) > impact_score(without_ambig)


class TestConfidenceScore:
    """Tests for confidence_score function."""

    @staticmethod
    def test_resolved_ast_highest() -> None:
        """Return highest confidence for resolved AST evidence."""
        signals = ConfidenceSignals(evidence_kind="resolved_ast")
        assert confidence_score(signals) == _CONFIDENCE_RESOLVED_AST

    @staticmethod
    def test_bytecode_high() -> None:
        """Return high confidence for bytecode evidence."""
        signals = ConfidenceSignals(evidence_kind="bytecode")
        assert confidence_score(signals) == _CONFIDENCE_BYTECODE

    @staticmethod
    def test_heuristic_medium() -> None:
        """Return medium confidence for heuristic evidence."""
        signals = ConfidenceSignals(evidence_kind="heuristic")
        assert confidence_score(signals) == _CONFIDENCE_HEURISTIC

    @staticmethod
    def test_rg_only_low() -> None:
        """Return low confidence for rg-only evidence."""
        signals = ConfidenceSignals(evidence_kind="rg_only")
        assert confidence_score(signals) == _CONFIDENCE_RG_ONLY

    @staticmethod
    def test_unresolved_lowest() -> None:
        """Return lowest confidence for unresolved evidence."""
        signals = ConfidenceSignals(evidence_kind="unresolved")
        assert confidence_score(signals) == _CONFIDENCE_UNRESOLVED

    @staticmethod
    def test_default_is_unresolved() -> None:
        """Default confidence kind to unresolved."""
        signals = ConfidenceSignals()
        assert confidence_score(signals) == _CONFIDENCE_UNRESOLVED

    @staticmethod
    def test_unknown_kind_returns_default() -> None:
        """Return default confidence for unknown kinds."""
        signals = ConfidenceSignals(evidence_kind="unknown_kind")
        assert confidence_score(signals) == _CONFIDENCE_UNRESOLVED


class TestBucket:
    """Tests for bucket function."""

    @staticmethod
    def test_high_threshold() -> None:
        """Bucket high thresholds correctly."""
        assert bucket(_BUCKET_HIGH_THRESHOLD) == "high"
        assert bucket(_BUCKET_HIGH_ALT) == "high"
        assert bucket(_IMPACT_SCORE_ONE) == "high"

    @staticmethod
    def test_medium_threshold() -> None:
        """Bucket medium thresholds correctly."""
        assert bucket(_BUCKET_MED_THRESHOLD) == "med"
        assert bucket(_BUCKET_MED_ALT) == "med"
        assert bucket(_BUCKET_MED_UPPER) == "med"

    @staticmethod
    def test_low_threshold() -> None:
        """Bucket low thresholds correctly."""
        assert bucket(_BUCKET_LOW_MIN) == "low"
        assert bucket(_BUCKET_LOW_ALT) == "low"
        assert bucket(_BUCKET_LOW_UPPER) == "low"

    @staticmethod
    def test_exact_boundaries() -> None:
        """Handle boundary values for buckets."""
        # Exact boundary values
        assert bucket(_BUCKET_HIGH_THRESHOLD) == "high"
        assert bucket(_BUCKET_MED_THRESHOLD) == "med"
        # Just below boundaries
        assert bucket(_BUCKET_JUST_BELOW_HIGH) == "med"
        assert bucket(_BUCKET_JUST_BELOW_MED) == "low"


class TestScoreIntegration:
    """Integration tests combining scoring functions."""

    @staticmethod
    def test_typical_high_impact_finding() -> None:
        """Combine signals for a high impact/high confidence case."""
        # High impact: 100 sites, 20 files, depth 10 = max score
        imp_signals = ImpactSignals(sites=100, files=20, depth=10, breakages=10)
        conf_signals = ConfidenceSignals(evidence_kind="resolved_ast")

        imp = impact_score(imp_signals)
        conf = confidence_score(conf_signals)

        assert bucket(imp) == "high"
        assert bucket(conf) == "high"

    @staticmethod
    def test_typical_medium_impact_finding() -> None:
        """Combine signals for a medium impact/medium confidence case."""
        # Medium impact: ~50 sites, ~10 files gives ~0.35 + some depth
        imp_signals = ImpactSignals(sites=50, files=10, depth=5)
        conf_signals = ConfidenceSignals(evidence_kind="heuristic")

        imp = impact_score(imp_signals)
        conf = confidence_score(conf_signals)

        assert bucket(imp) == "med"
        assert bucket(conf) == "med"

    @staticmethod
    def test_typical_low_impact_finding() -> None:
        """Combine signals for a low impact/low confidence case."""
        imp_signals = ImpactSignals(sites=2, files=1)
        conf_signals = ConfidenceSignals(evidence_kind="unresolved")

        imp = impact_score(imp_signals)
        conf = confidence_score(conf_signals)

        assert bucket(imp) == "low"
        assert bucket(conf) == "low"
