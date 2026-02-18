"""Tests for typed query summary-update contracts."""

from __future__ import annotations

import msgspec
from tools.cq.core.schema import mk_result, mk_runmeta
from tools.cq.core.summary_types import apply_summary_mapping
from tools.cq.core.summary_update_contracts import EntitySummaryUpdateV1
from tools.cq.query.executor_runtime import _entity_summary_updates

EXPECTED_MATCHES = 7
EXPECTED_TOTAL_DEFS = 5


def test_entity_summary_updates_returns_typed_contract() -> None:
    """Verify entity summary updates are emitted as typed summary contracts."""
    result = mk_result(
        mk_runmeta(
            macro="q",
            argv=["cq", "q"],
            root=".",
            started_ms=0.0,
            toolchain={},
        )
    )
    result = msgspec.structs.replace(
        result,
        summary=apply_summary_mapping(
            result.summary,
            {
                "matches": EXPECTED_MATCHES,
                "total_defs": EXPECTED_TOTAL_DEFS,
                "total_calls": 3,
                "total_imports": 2,
            },
        ),
    )

    summary_update = _entity_summary_updates(result)

    assert isinstance(summary_update, EntitySummaryUpdateV1)
    assert summary_update.matches == EXPECTED_MATCHES
    assert summary_update.total_defs == EXPECTED_TOTAL_DEFS
