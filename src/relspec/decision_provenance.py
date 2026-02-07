"""Decision provenance graph for inference-driven pipeline decisions.

Capture compile-time decisions, evidence used, confidence levels, and
runtime outcome metrics.  Enable explainability for "why policy X was
chosen" and "whether it helped."
"""

from __future__ import annotations

from typing import Final, Literal

from serde_msgspec import StructBaseCompat

DecisionDomain = Literal[
    "scan_policy",
    "join_strategy",
    "cache_policy",
    "maintenance",
    "scheduling",
    "udf_requirements",
    "schema_contract",
]

# ---------------------------------------------------------------------------
# Evidence & Outcome records
# ---------------------------------------------------------------------------


class EvidenceRecord(StructBaseCompat, frozen=True):
    """Single piece of evidence that fed into a decision.

    Attributes:
    ----------
    source
        Evidence source category (e.g. "plan_signals", "capabilities",
        "schema", "lineage", "runtime").
    key
        Specific evidence key (e.g. "stats.num_rows", "has_pushed_filters").
    value
        Serialized evidence value.
    """

    source: str
    key: str
    value: str


class DecisionOutcome(StructBaseCompat, frozen=True):
    """Runtime outcome metrics for a decision.

    Filled in after execution to close the feedback loop.

    Attributes:
    ----------
    success
        Whether the decision produced the expected result.
    metric_name
        Name of the outcome metric (e.g. "scan_volume_bytes",
        "execution_time_ms").
    metric_value
        Numeric metric value.
    notes
        Free-form notes on the outcome.
    """

    success: bool = True
    metric_name: str = ""
    metric_value: float | None = None
    notes: str = ""


# ---------------------------------------------------------------------------
# Core decision record
# ---------------------------------------------------------------------------


class DecisionRecord(StructBaseCompat, frozen=True):
    """A single decision node in the provenance graph.

    Each record captures what was decided, why (evidence + confidence),
    and optionally what happened (outcome).

    Attributes:
    ----------
    decision_id
        Unique identifier for this decision.
    domain
        Decision domain category.
    decision_type
        Specific decision type within the domain.
    decision_value
        The actual value chosen.
    confidence_score
        Numeric confidence in [0.0, 1.0].
    evidence
        Evidence records that fed into this decision.
    parent_ids
        IDs of parent decisions that influenced this one.
    fallback_reason
        Populated when a conservative fallback was chosen.
    outcome
        Runtime outcome metrics (filled in post-execution).
    timestamp_ms
        Unix timestamp in milliseconds when the decision was made.
    context_label
        Pipeline context label (e.g. dataset name, view name).
    """

    decision_id: str
    domain: str
    decision_type: str
    decision_value: str
    confidence_score: float = 1.0
    evidence: tuple[EvidenceRecord, ...] = ()
    parent_ids: tuple[str, ...] = ()
    fallback_reason: str | None = None
    outcome: DecisionOutcome | None = None
    timestamp_ms: int = 0
    context_label: str = ""


# ---------------------------------------------------------------------------
# Provenance graph
# ---------------------------------------------------------------------------


class DecisionProvenanceGraph(StructBaseCompat, frozen=True):
    """Complete decision provenance graph for a pipeline run.

    Collect all decisions made during a single pipeline execution,
    enabling post-hoc queries on decision chains, confidence levels,
    and outcome attribution.

    Attributes:
    ----------
    run_id
        Unique identifier for the pipeline run.
    decisions
        All decisions recorded during the run.
    root_ids
        IDs of decisions with no parents (entry points).
    """

    run_id: str
    decisions: tuple[DecisionRecord, ...] = ()
    root_ids: tuple[str, ...] = ()


# ---------------------------------------------------------------------------
# Graph query helpers
# ---------------------------------------------------------------------------

_EMPTY: Final[tuple[DecisionRecord, ...]] = ()


def decisions_by_domain(
    graph: DecisionProvenanceGraph,
    domain: str,
) -> tuple[DecisionRecord, ...]:
    """Filter decisions by domain.

    Parameters
    ----------
    graph
        Provenance graph to query.
    domain
        Domain string to match.

    Returns:
    -------
    tuple[DecisionRecord, ...]
        Decisions matching the domain.
    """
    matches = tuple(d for d in graph.decisions if d.domain == domain)
    return matches or _EMPTY


def decisions_above_confidence(
    graph: DecisionProvenanceGraph,
    min_confidence: float,
) -> tuple[DecisionRecord, ...]:
    """Filter decisions at or above a confidence threshold.

    Parameters
    ----------
    graph
        Provenance graph to query.
    min_confidence
        Minimum confidence score (inclusive).

    Returns:
    -------
    tuple[DecisionRecord, ...]
        Decisions with ``confidence_score >= min_confidence``.
    """
    matches = tuple(d for d in graph.decisions if d.confidence_score >= min_confidence)
    return matches or _EMPTY


def decisions_with_fallback(
    graph: DecisionProvenanceGraph,
) -> tuple[DecisionRecord, ...]:
    """Return decisions that used a fallback.

    Parameters
    ----------
    graph
        Provenance graph to query.

    Returns:
    -------
    tuple[DecisionRecord, ...]
        Decisions where ``fallback_reason`` is set.
    """
    matches = tuple(d for d in graph.decisions if d.fallback_reason is not None)
    return matches or _EMPTY


def decision_children(
    graph: DecisionProvenanceGraph,
    decision_id: str,
) -> tuple[DecisionRecord, ...]:
    """Return direct children of a decision.

    Parameters
    ----------
    graph
        Provenance graph to query.
    decision_id
        Parent decision ID.

    Returns:
    -------
    tuple[DecisionRecord, ...]
        Decisions listing ``decision_id`` in their ``parent_ids``.
    """
    matches = tuple(d for d in graph.decisions if decision_id in d.parent_ids)
    return matches or _EMPTY


def decision_chain(
    graph: DecisionProvenanceGraph,
    decision_id: str,
) -> tuple[DecisionRecord, ...]:
    """Return the ancestor chain leading to a decision (root first).

    Walk from the target decision towards root decisions via
    ``parent_ids``, collecting ancestors in root-first order.

    Parameters
    ----------
    graph
        Provenance graph to query.
    decision_id
        Target decision ID.

    Returns:
    -------
    tuple[DecisionRecord, ...]
        Ancestor chain with root decisions first, target last.
    """
    index: dict[str, DecisionRecord] = {d.decision_id: d for d in graph.decisions}
    target = index.get(decision_id)
    if target is None:
        return _EMPTY

    chain: list[DecisionRecord] = []
    visited: set[str] = set()
    stack = [target]

    while stack:
        current = stack.pop()
        if current.decision_id in visited:
            continue
        visited.add(current.decision_id)
        chain.append(current)
        for pid in current.parent_ids:
            parent = index.get(pid)
            if parent is not None and parent.decision_id not in visited:
                stack.append(parent)

    chain.reverse()
    return tuple(chain)


__all__ = [
    "DecisionDomain",
    "DecisionOutcome",
    "DecisionProvenanceGraph",
    "DecisionRecord",
    "EvidenceRecord",
    "decision_chain",
    "decision_children",
    "decisions_above_confidence",
    "decisions_by_domain",
    "decisions_with_fallback",
]
