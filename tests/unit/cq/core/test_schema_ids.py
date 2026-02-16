"""Tests for test_schema_ids."""

from __future__ import annotations

from tools.cq.core.schema import (
    Anchor,
    CqResult,
    Finding,
    RunMeta,
    Section,
    assign_result_finding_ids,
)


def _build_result(*, run_id: str) -> CqResult:
    return CqResult(
        run=RunMeta(
            macro="q",
            argv=["q"],
            root="/repo",
            started_ms=0.0,
            elapsed_ms=1.0,
            run_id=run_id,
        ),
        key_findings=[
            Finding(
                category="call_site",
                message="foo called",
                anchor=Anchor(file="a.py", line=10, col=2),
            )
        ],
        evidence=[
            Finding(
                category="import",
                message="imports pathlib",
                anchor=Anchor(file="a.py", line=1, col=0),
            )
        ],
        sections=[
            Section(
                title="Preview",
                findings=[
                    Finding(
                        category="context",
                        message="inside module",
                        anchor=Anchor(file="a.py", line=1, col=0),
                    )
                ],
            )
        ],
    )



def test_assign_result_finding_ids_sets_stable_and_execution_fields() -> None:
    """Assign both stable and execution IDs on all result findings."""
    result = _build_result(run_id="run-A")

    assign_result_finding_ids(result)

    for finding in [*result.key_findings, *result.evidence, *result.sections[0].findings]:
        assert isinstance(finding.stable_id, str)
        assert finding.stable_id
        assert isinstance(finding.execution_id, str)
        assert finding.execution_id
        assert finding.id_taxonomy == "stable_execution"



def test_stable_ids_match_across_runs_but_execution_ids_differ() -> None:
    """Keep stable IDs deterministic while execution IDs remain run-scoped."""
    result_a = _build_result(run_id="run-A")
    result_b = _build_result(run_id="run-B")

    assign_result_finding_ids(result_a)
    assign_result_finding_ids(result_b)

    assert result_a.key_findings[0].stable_id == result_b.key_findings[0].stable_id
    assert result_a.key_findings[0].execution_id != result_b.key_findings[0].execution_id
