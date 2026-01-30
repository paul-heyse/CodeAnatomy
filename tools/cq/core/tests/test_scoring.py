"""Tests for scoring module."""

from __future__ import annotations

from tools.cq.core.scoring import (
    ConfidenceSignals,
    ImpactSignals,
    bucket,
    confidence_score,
    impact_score,
)


class TestImpactScore:
    """Tests for impact_score function."""

    def test_zero_signals_returns_zero(self) -> None:
        signals = ImpactSignals()
        assert impact_score(signals) == 0.0

    def test_max_signals_returns_one(self) -> None:
        signals = ImpactSignals(
            sites=100,
            files=20,
            depth=10,
            breakages=10,
            ambiguities=10,
        )
        assert impact_score(signals) == 1.0

    def test_saturates_at_one(self) -> None:
        signals = ImpactSignals(
            sites=500,
            files=100,
            depth=50,
            breakages=50,
            ambiguities=50,
        )
        assert impact_score(signals) == 1.0

    def test_sites_weight_is_highest(self) -> None:
        sites_only = ImpactSignals(sites=100)
        files_only = ImpactSignals(files=20)
        assert impact_score(sites_only) > impact_score(files_only)

    def test_partial_values(self) -> None:
        signals = ImpactSignals(sites=50, files=10)
        score = impact_score(signals)
        # sites: 50/100 * 0.45 = 0.225
        # files: 10/20 * 0.25 = 0.125
        # total: 0.35
        assert 0.34 < score < 0.36

    def test_depth_contribution(self) -> None:
        without_depth = ImpactSignals(sites=10, files=5)
        with_depth = ImpactSignals(sites=10, files=5, depth=5)
        assert impact_score(with_depth) > impact_score(without_depth)

    def test_breakages_contribution(self) -> None:
        without_breakages = ImpactSignals(sites=10)
        with_breakages = ImpactSignals(sites=10, breakages=5)
        assert impact_score(with_breakages) > impact_score(without_breakages)

    def test_ambiguities_contribution(self) -> None:
        without_ambig = ImpactSignals(sites=10)
        with_ambig = ImpactSignals(sites=10, ambiguities=5)
        assert impact_score(with_ambig) > impact_score(without_ambig)


class TestConfidenceScore:
    """Tests for confidence_score function."""

    def test_resolved_ast_highest(self) -> None:
        signals = ConfidenceSignals(evidence_kind="resolved_ast")
        assert confidence_score(signals) == 0.95

    def test_bytecode_high(self) -> None:
        signals = ConfidenceSignals(evidence_kind="bytecode")
        assert confidence_score(signals) == 0.90

    def test_heuristic_medium(self) -> None:
        signals = ConfidenceSignals(evidence_kind="heuristic")
        assert confidence_score(signals) == 0.60

    def test_rg_only_low(self) -> None:
        signals = ConfidenceSignals(evidence_kind="rg_only")
        assert confidence_score(signals) == 0.45

    def test_unresolved_lowest(self) -> None:
        signals = ConfidenceSignals(evidence_kind="unresolved")
        assert confidence_score(signals) == 0.30

    def test_default_is_unresolved(self) -> None:
        signals = ConfidenceSignals()
        assert confidence_score(signals) == 0.30

    def test_unknown_kind_returns_default(self) -> None:
        signals = ConfidenceSignals(evidence_kind="unknown_kind")
        assert confidence_score(signals) == 0.30


class TestBucket:
    """Tests for bucket function."""

    def test_high_threshold(self) -> None:
        assert bucket(0.7) == "high"
        assert bucket(0.8) == "high"
        assert bucket(1.0) == "high"

    def test_medium_threshold(self) -> None:
        assert bucket(0.4) == "med"
        assert bucket(0.5) == "med"
        assert bucket(0.69) == "med"

    def test_low_threshold(self) -> None:
        assert bucket(0.0) == "low"
        assert bucket(0.2) == "low"
        assert bucket(0.39) == "low"

    def test_exact_boundaries(self) -> None:
        # Exact boundary values
        assert bucket(0.7) == "high"
        assert bucket(0.4) == "med"
        # Just below boundaries
        assert bucket(0.699999) == "med"
        assert bucket(0.399999) == "low"


class TestScoreIntegration:
    """Integration tests combining scoring functions."""

    def test_typical_high_impact_finding(self) -> None:
        # High impact: 100 sites, 20 files, depth 10 = max score
        imp_signals = ImpactSignals(sites=100, files=20, depth=10, breakages=10)
        conf_signals = ConfidenceSignals(evidence_kind="resolved_ast")

        imp = impact_score(imp_signals)
        conf = confidence_score(conf_signals)

        assert bucket(imp) == "high"
        assert bucket(conf) == "high"

    def test_typical_medium_impact_finding(self) -> None:
        # Medium impact: ~50 sites, ~10 files gives ~0.35 + some depth
        imp_signals = ImpactSignals(sites=50, files=10, depth=5)
        conf_signals = ConfidenceSignals(evidence_kind="heuristic")

        imp = impact_score(imp_signals)
        conf = confidence_score(conf_signals)

        assert bucket(imp) == "med"
        assert bucket(conf) == "med"

    def test_typical_low_impact_finding(self) -> None:
        imp_signals = ImpactSignals(sites=2, files=1)
        conf_signals = ConfidenceSignals(evidence_kind="unresolved")

        imp = impact_score(imp_signals)
        conf = confidence_score(conf_signals)

        assert bucket(imp) == "low"
        assert bucket(conf) == "low"
