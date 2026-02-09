"""Mutable recorder for building decision provenance graphs incrementally.

Accumulate ``DecisionRecord`` instances during a pipeline run and produce
an immutable ``DecisionProvenanceGraph`` at the end.
"""

from __future__ import annotations

import time

from relspec.decision_provenance import (
    DecisionOutcome,
    DecisionProvenanceGraph,
    DecisionRecord,
    EvidenceRecord,
)
from utils.uuid_factory import uuid7_hex


class DecisionRecorder:
    """Incrementally build a ``DecisionProvenanceGraph``.

    Usage::

        recorder = DecisionRecorder(run_id="run-123")
        d_id = recorder.record(
            domain="scan_policy",
            decision_type="small_table_override",
            decision_value="memory_mapped",
            confidence_score=0.9,
            evidence=(
                EvidenceRecord(
                    source="stats",
                    key="num_rows",
                    value="500",
                ),
            ),
            context_label="my_dataset",
        )
        # Later, attach outcome:
        recorder.record_outcome(
            d_id,
            DecisionOutcome(
                success=True,
                metric_name="scan_ms",
                metric_value=12.5,
            ),
        )
        graph = recorder.build()

    Parameters
    ----------
    run_id
        Pipeline run identifier.
    """

    def __init__(self, run_id: str) -> None:
        """Initialize a recorder for one pipeline run."""
        self._run_id = run_id
        self._decisions: list[DecisionRecord] = []
        self._outcomes: dict[str, DecisionOutcome] = {}

    @property
    def run_id(self) -> str:
        """Return the pipeline run identifier.

        Returns:
        -------
        str
            Pipeline run identifier.
        """
        return self._run_id

    def record(  # noqa: PLR0913
        self,
        *,
        domain: str,
        decision_type: str,
        decision_value: str,
        confidence_score: float = 1.0,
        evidence: tuple[EvidenceRecord, ...] = (),
        parent_ids: tuple[str, ...] = (),
        fallback_reason: str | None = None,
        context_label: str = "",
    ) -> str:
        """Record a decision and return its ID.

        Parameters
        ----------
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
        context_label
            Pipeline context label (e.g. dataset name, view name).

        Returns:
        -------
        str
            Unique decision identifier.
        """
        decision_id = uuid7_hex()
        record = DecisionRecord(
            decision_id=decision_id,
            domain=domain,
            decision_type=decision_type,
            decision_value=decision_value,
            confidence_score=confidence_score,
            evidence=evidence,
            parent_ids=parent_ids,
            fallback_reason=fallback_reason,
            timestamp_ms=int(time.time() * _MS_PER_SECOND),
            context_label=context_label,
        )
        self._decisions.append(record)
        return decision_id

    def record_outcome(
        self,
        decision_id: str,
        outcome: DecisionOutcome,
    ) -> None:
        """Attach an outcome to a previously recorded decision.

        Parameters
        ----------
        decision_id
            ID of the decision to attach the outcome to.
        outcome
            Runtime outcome metrics.
        """
        self._outcomes[decision_id] = outcome

    def build(self) -> DecisionProvenanceGraph:
        """Build the immutable provenance graph.

        Apply any recorded outcomes to their corresponding decisions and
        compute the root decision IDs.

        Returns:
        -------
        DecisionProvenanceGraph
            Immutable graph of all recorded decisions.
        """
        all_parent_ids: set[str] = set()
        final: list[DecisionRecord] = []

        for decision_record in self._decisions:
            all_parent_ids.update(decision_record.parent_ids)
            outcome = self._outcomes.get(decision_record.decision_id)
            final_decision = decision_record
            if outcome is not None:
                final_decision = DecisionRecord(
                    decision_id=decision_record.decision_id,
                    domain=decision_record.domain,
                    decision_type=decision_record.decision_type,
                    decision_value=decision_record.decision_value,
                    confidence_score=decision_record.confidence_score,
                    evidence=decision_record.evidence,
                    parent_ids=decision_record.parent_ids,
                    fallback_reason=decision_record.fallback_reason,
                    outcome=outcome,
                    timestamp_ms=decision_record.timestamp_ms,
                    context_label=decision_record.context_label,
                )
            final.append(final_decision)

        all_ids = {d.decision_id for d in final}
        root_ids = tuple(
            d.decision_id
            for d in final
            if not d.parent_ids or not any(pid in all_ids for pid in d.parent_ids)
        )

        return DecisionProvenanceGraph(
            run_id=self._run_id,
            decisions=tuple(final),
            root_ids=root_ids,
        )


_MS_PER_SECOND = 1000

__all__ = [
    "DecisionRecorder",
]
