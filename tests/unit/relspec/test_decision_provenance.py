"""Tests for decision provenance graph types, recorder, and query helpers."""

from __future__ import annotations

import pytest

from relspec.decision_provenance import (
    DecisionOutcome,
    DecisionProvenanceGraph,
    DecisionRecord,
    EvidenceRecord,
    decision_chain,
    decision_children,
    decisions_above_confidence,
    decisions_by_domain,
    decisions_with_fallback,
)
from relspec.decision_recorder import DecisionRecorder
from serde_artifacts import DecisionProvenanceGraphArtifact

# ---------------------------------------------------------------------------
# EvidenceRecord
# ---------------------------------------------------------------------------


class TestEvidenceRecord:
    """Test EvidenceRecord construction and field access."""

    def test_construction(self) -> None:
        """Construct an EvidenceRecord with source, key, and value."""
        rec = EvidenceRecord(source="plan_signals", key="num_rows", value="500")
        assert rec.source == "plan_signals"
        assert rec.key == "num_rows"
        assert rec.value == "500"

    def test_frozen(self) -> None:
        """EvidenceRecord is immutable."""
        rec = EvidenceRecord(source="stats", key="k", value="v")
        with pytest.raises(AttributeError):
            rec.source = "other"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# DecisionOutcome
# ---------------------------------------------------------------------------


class TestDecisionOutcome:
    """Test DecisionOutcome construction and defaults."""

    def test_all_fields(self) -> None:
        """Construct a DecisionOutcome with all fields."""
        out = DecisionOutcome(
            success=False,
            metric_name="scan_ms",
            metric_value=42.5,
            notes="slow scan",
        )
        assert out.success is False
        assert out.metric_name == "scan_ms"
        assert out.metric_value == 42.5
        assert out.notes == "slow scan"

    def test_defaults(self) -> None:
        """DecisionOutcome defaults are sensible."""
        out = DecisionOutcome()
        assert out.success is True
        assert out.metric_name == ""
        assert out.metric_value is None
        assert out.notes == ""


# ---------------------------------------------------------------------------
# DecisionRecord
# ---------------------------------------------------------------------------


class TestDecisionRecord:
    """Test DecisionRecord construction, defaults, and field access."""

    def test_all_fields(self) -> None:
        """Construct a DecisionRecord with all fields specified."""
        evidence = (EvidenceRecord(source="s", key="k", value="v"),)
        outcome = DecisionOutcome(success=True, metric_name="m", metric_value=1.0)
        rec = DecisionRecord(
            decision_id="d1",
            domain="scan_policy",
            decision_type="override",
            decision_value="mmap",
            confidence_score=0.9,
            evidence=evidence,
            parent_ids=("p1",),
            fallback_reason="insufficient stats",
            outcome=outcome,
            timestamp_ms=12345,
            context_label="my_dataset",
        )
        assert rec.decision_id == "d1"
        assert rec.domain == "scan_policy"
        assert rec.decision_type == "override"
        assert rec.decision_value == "mmap"
        assert rec.confidence_score == 0.9
        assert len(rec.evidence) == 1
        assert rec.parent_ids == ("p1",)
        assert rec.fallback_reason == "insufficient stats"
        assert rec.outcome is outcome
        assert rec.timestamp_ms == 12345
        assert rec.context_label == "my_dataset"

    def test_defaults(self) -> None:
        """DecisionRecord defaults produce a minimal valid record."""
        rec = DecisionRecord(
            decision_id="d1",
            domain="scan_policy",
            decision_type="t",
            decision_value="v",
        )
        assert rec.confidence_score == 1.0
        assert rec.evidence == ()
        assert rec.parent_ids == ()
        assert rec.fallback_reason is None
        assert rec.outcome is None
        assert rec.timestamp_ms == 0
        assert rec.context_label == ""

    def test_with_evidence_tuple(self) -> None:
        """DecisionRecord stores evidence as a tuple."""
        ev1 = EvidenceRecord(source="a", key="k1", value="v1")
        ev2 = EvidenceRecord(source="b", key="k2", value="v2")
        rec = DecisionRecord(
            decision_id="d1",
            domain="join_strategy",
            decision_type="t",
            decision_value="v",
            evidence=(ev1, ev2),
        )
        assert len(rec.evidence) == 2
        assert rec.evidence[0].source == "a"
        assert rec.evidence[1].source == "b"

    def test_with_parent_ids(self) -> None:
        """DecisionRecord stores parent IDs as a tuple."""
        rec = DecisionRecord(
            decision_id="d2",
            domain="cache_policy",
            decision_type="t",
            decision_value="v",
            parent_ids=("p1", "p2"),
        )
        assert rec.parent_ids == ("p1", "p2")


# ---------------------------------------------------------------------------
# DecisionProvenanceGraph
# ---------------------------------------------------------------------------


class TestDecisionProvenanceGraph:
    """Test DecisionProvenanceGraph construction."""

    def test_empty_graph(self) -> None:
        """Construct an empty provenance graph."""
        graph = DecisionProvenanceGraph(run_id="run-1")
        assert graph.run_id == "run-1"
        assert graph.decisions == ()
        assert graph.root_ids == ()

    def test_single_decision(self) -> None:
        """Construct a graph with a single decision."""
        d = DecisionRecord(
            decision_id="d1",
            domain="scan_policy",
            decision_type="t",
            decision_value="v",
        )
        graph = DecisionProvenanceGraph(
            run_id="run-1",
            decisions=(d,),
            root_ids=("d1",),
        )
        assert len(graph.decisions) == 1
        assert graph.root_ids == ("d1",)

    def test_parent_child_chain(self) -> None:
        """Construct a graph with parent-child relationships."""
        parent = DecisionRecord(
            decision_id="p1",
            domain="scan_policy",
            decision_type="t",
            decision_value="v",
        )
        child = DecisionRecord(
            decision_id="c1",
            domain="cache_policy",
            decision_type="t",
            decision_value="v",
            parent_ids=("p1",),
        )
        graph = DecisionProvenanceGraph(
            run_id="run-1",
            decisions=(parent, child),
            root_ids=("p1",),
        )
        assert len(graph.decisions) == 2
        assert graph.root_ids == ("p1",)


# ---------------------------------------------------------------------------
# DecisionRecorder
# ---------------------------------------------------------------------------


class TestDecisionRecorder:
    """Test the mutable DecisionRecorder builder."""

    def test_record_returns_id(self) -> None:
        """Record returns a non-empty decision ID."""
        recorder = DecisionRecorder(run_id="run-1")
        d_id = recorder.record(
            domain="scan_policy",
            decision_type="override",
            decision_value="mmap",
        )
        assert isinstance(d_id, str)
        assert len(d_id) > 0

    def test_record_outcome_attaches(self) -> None:
        """Record outcome attaches to the decision in the built graph."""
        recorder = DecisionRecorder(run_id="run-1")
        d_id = recorder.record(
            domain="scan_policy",
            decision_type="override",
            decision_value="mmap",
        )
        recorder.record_outcome(
            d_id,
            DecisionOutcome(success=True, metric_name="scan_ms", metric_value=12.5),
        )
        graph = recorder.build()
        assert len(graph.decisions) == 1
        decision = graph.decisions[0]
        assert decision.outcome is not None
        assert decision.outcome.success is True
        assert decision.outcome.metric_name == "scan_ms"
        assert decision.outcome.metric_value == 12.5

    def test_build_immutable_graph(self) -> None:
        """Build produces an immutable DecisionProvenanceGraph."""
        recorder = DecisionRecorder(run_id="run-1")
        recorder.record(
            domain="scan_policy",
            decision_type="t",
            decision_value="v",
        )
        graph = recorder.build()
        assert isinstance(graph, DecisionProvenanceGraph)
        assert graph.run_id == "run-1"
        assert len(graph.decisions) == 1

    def test_parent_child_relationships(self) -> None:
        """Recorder correctly tracks parent-child relationships."""
        recorder = DecisionRecorder(run_id="run-1")
        parent_id = recorder.record(
            domain="scan_policy",
            decision_type="base",
            decision_value="default",
        )
        child_id = recorder.record(
            domain="cache_policy",
            decision_type="derived",
            decision_value="eager",
            parent_ids=(parent_id,),
        )
        graph = recorder.build()
        assert len(graph.decisions) == 2
        child = next(d for d in graph.decisions if d.decision_id == child_id)
        assert parent_id in child.parent_ids

    def test_root_ids_computed(self) -> None:
        """Root IDs are computed correctly in the built graph."""
        recorder = DecisionRecorder(run_id="run-1")
        root_id = recorder.record(
            domain="scan_policy",
            decision_type="base",
            decision_value="default",
        )
        recorder.record(
            domain="cache_policy",
            decision_type="derived",
            decision_value="eager",
            parent_ids=(root_id,),
        )
        graph = recorder.build()
        assert len(graph.root_ids) == 1
        assert graph.root_ids[0] == root_id

    def test_timestamps_set(self) -> None:
        """Recorded decisions have non-zero timestamps."""
        recorder = DecisionRecorder(run_id="run-1")
        recorder.record(
            domain="scan_policy",
            decision_type="t",
            decision_value="v",
        )
        graph = recorder.build()
        assert graph.decisions[0].timestamp_ms > 0

    def test_run_id_property(self) -> None:
        """Recorder exposes its run_id."""
        recorder = DecisionRecorder(run_id="run-abc")
        assert recorder.run_id == "run-abc"

    def test_multiple_roots(self) -> None:
        """Multiple independent decisions are all roots."""
        recorder = DecisionRecorder(run_id="run-1")
        id_a = recorder.record(
            domain="scan_policy",
            decision_type="a",
            decision_value="va",
        )
        id_b = recorder.record(
            domain="join_strategy",
            decision_type="b",
            decision_value="vb",
        )
        graph = recorder.build()
        assert len(graph.root_ids) == 2
        assert id_a in graph.root_ids
        assert id_b in graph.root_ids

    def test_decision_without_outcome(self) -> None:
        """Decision without outcome has None outcome in built graph."""
        recorder = DecisionRecorder(run_id="run-1")
        recorder.record(
            domain="scan_policy",
            decision_type="t",
            decision_value="v",
        )
        graph = recorder.build()
        assert graph.decisions[0].outcome is None

    def test_evidence_preserved(self) -> None:
        """Evidence records survive the build process."""
        recorder = DecisionRecorder(run_id="run-1")
        ev = EvidenceRecord(source="stats", key="num_rows", value="1000")
        recorder.record(
            domain="scan_policy",
            decision_type="t",
            decision_value="v",
            evidence=(ev,),
        )
        graph = recorder.build()
        assert len(graph.decisions[0].evidence) == 1
        assert graph.decisions[0].evidence[0].key == "num_rows"

    def test_fallback_reason_preserved(self) -> None:
        """Fallback reason survives the build process."""
        recorder = DecisionRecorder(run_id="run-1")
        recorder.record(
            domain="scan_policy",
            decision_type="t",
            decision_value="v",
            fallback_reason="missing stats",
        )
        graph = recorder.build()
        assert graph.decisions[0].fallback_reason == "missing stats"


# ---------------------------------------------------------------------------
# Graph query helpers
# ---------------------------------------------------------------------------


def _build_test_graph() -> DecisionProvenanceGraph:
    """Build a test graph with diverse decisions.

    Returns:
    -------
    DecisionProvenanceGraph
        Test graph with scan_policy, cache_policy, and join_strategy decisions.
    """
    d1 = DecisionRecord(
        decision_id="d1",
        domain="scan_policy",
        decision_type="override",
        decision_value="mmap",
        confidence_score=0.9,
    )
    d2 = DecisionRecord(
        decision_id="d2",
        domain="cache_policy",
        decision_type="select",
        decision_value="eager",
        confidence_score=0.6,
        parent_ids=("d1",),
    )
    d3 = DecisionRecord(
        decision_id="d3",
        domain="scan_policy",
        decision_type="fallback",
        decision_value="default",
        confidence_score=0.3,
        fallback_reason="no stats available",
    )
    d4 = DecisionRecord(
        decision_id="d4",
        domain="join_strategy",
        decision_type="inferred",
        decision_value="hash_join",
        confidence_score=0.95,
        parent_ids=("d2",),
    )
    return DecisionProvenanceGraph(
        run_id="test-run",
        decisions=(d1, d2, d3, d4),
        root_ids=("d1", "d3"),
    )


class TestGraphQueries:
    """Test graph query helper functions."""

    def test_decisions_by_domain(self) -> None:
        """Filter by domain returns matching decisions."""
        graph = _build_test_graph()
        scan_decisions = decisions_by_domain(graph, "scan_policy")
        assert len(scan_decisions) == 2
        assert all(d.domain == "scan_policy" for d in scan_decisions)

    def test_decisions_by_domain_no_match(self) -> None:
        """Filter by unknown domain returns empty tuple."""
        graph = _build_test_graph()
        result = decisions_by_domain(graph, "nonexistent")
        assert result == ()

    def test_decisions_above_confidence(self) -> None:
        """Filter by confidence threshold returns qualifying decisions."""
        graph = _build_test_graph()
        high = decisions_above_confidence(graph, 0.8)
        assert len(high) == 2
        assert all(d.confidence_score >= 0.8 for d in high)

    def test_decisions_above_confidence_none_qualify(self) -> None:
        """Filter with very high threshold returns empty."""
        graph = _build_test_graph()
        result = decisions_above_confidence(graph, 0.99)
        assert len(result) == 0

    def test_decisions_with_fallback(self) -> None:
        """Return only decisions with fallback_reason set."""
        graph = _build_test_graph()
        fallbacks = decisions_with_fallback(graph)
        assert len(fallbacks) == 1
        assert fallbacks[0].decision_id == "d3"

    def test_decisions_with_fallback_none(self) -> None:
        """Return empty when no fallbacks exist."""
        graph = DecisionProvenanceGraph(
            run_id="test",
            decisions=(
                DecisionRecord(
                    decision_id="d1",
                    domain="scan_policy",
                    decision_type="t",
                    decision_value="v",
                ),
            ),
            root_ids=("d1",),
        )
        result = decisions_with_fallback(graph)
        assert result == ()

    def test_decision_children(self) -> None:
        """Return direct children of a decision."""
        graph = _build_test_graph()
        children = decision_children(graph, "d1")
        assert len(children) == 1
        assert children[0].decision_id == "d2"

    def test_decision_children_no_children(self) -> None:
        """Return empty when decision has no children."""
        graph = _build_test_graph()
        children = decision_children(graph, "d4")
        assert children == ()

    def test_decision_chain_root_first(self) -> None:
        """Return ancestor chain with root first."""
        graph = _build_test_graph()
        chain = decision_chain(graph, "d4")
        assert len(chain) == 3
        assert chain[0].decision_id == "d1"
        assert chain[1].decision_id == "d2"
        assert chain[2].decision_id == "d4"

    def test_decision_chain_root_decision(self) -> None:
        """Chain for a root decision returns just that decision."""
        graph = _build_test_graph()
        chain = decision_chain(graph, "d1")
        assert len(chain) == 1
        assert chain[0].decision_id == "d1"

    def test_decision_chain_missing_id(self) -> None:
        """Chain for unknown ID returns empty."""
        graph = _build_test_graph()
        chain = decision_chain(graph, "nonexistent")
        assert chain == ()

    def test_empty_graph_queries(self) -> None:
        """All queries on empty graph return empty tuples."""
        graph = DecisionProvenanceGraph(run_id="empty")
        assert decisions_by_domain(graph, "scan_policy") == ()
        assert decisions_above_confidence(graph, 0.5) == ()
        assert decisions_with_fallback(graph) == ()
        assert decision_children(graph, "x") == ()
        assert decision_chain(graph, "x") == ()


# ---------------------------------------------------------------------------
# DecisionProvenanceGraphArtifact
# ---------------------------------------------------------------------------


class TestDecisionProvenanceGraphArtifact:
    """Test the artifact payload struct."""

    def test_construction(self) -> None:
        """Construct artifact with all required fields."""
        artifact = DecisionProvenanceGraphArtifact(
            run_id="run-1",
            decision_count=5,
            root_count=2,
            domain_counts={"scan_policy": 3, "cache_policy": 2},
            fallback_count=1,
            mean_confidence=0.75,
        )
        assert artifact.run_id == "run-1"
        assert artifact.decision_count == 5
        assert artifact.root_count == 2
        assert artifact.domain_counts is not None
        assert artifact.domain_counts["scan_policy"] == 3
        assert artifact.fallback_count == 1
        assert artifact.mean_confidence == 0.75

    def test_defaults(self) -> None:
        """Artifact defaults produce a minimal valid instance."""
        artifact = DecisionProvenanceGraphArtifact(
            run_id="run-1",
            decision_count=0,
            root_count=0,
        )
        assert artifact.domain_counts is None
        assert artifact.fallback_count == 0
        assert artifact.mean_confidence is None

    def test_frozen(self) -> None:
        """Artifact is immutable."""
        artifact = DecisionProvenanceGraphArtifact(
            run_id="run-1",
            decision_count=0,
            root_count=0,
        )
        with pytest.raises(AttributeError):
            artifact.run_id = "other"  # type: ignore[misc]
