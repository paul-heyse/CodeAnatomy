"""Tests for typed summary update contracts."""

from __future__ import annotations

import msgspec
from tools.cq.core.summary_update_contracts import (
    CallsSummaryUpdateV1,
    EntitySummaryUpdateV1,
    ImpactSummaryUpdateV1,
)


def test_entity_summary_update_deterministic_serialization_order() -> None:
    """Verify deterministic key ordering for entity summary payloads."""
    payload = EntitySummaryUpdateV1(matches=3, total_defs=4, total_calls=5, total_imports=6)
    builtins = msgspec.to_builtins(payload, order="deterministic")

    assert isinstance(builtins, dict)
    assert list(builtins.keys()) == ["matches", "total_defs", "total_calls", "total_imports"]


def test_macro_summary_update_contracts_encode_to_mappings() -> None:
    """Verify macro summary payload contracts encode to mapping objects."""
    calls_payload = CallsSummaryUpdateV1(
        query="foo",
        mode="macro:calls",
        function="foo",
        signature="",
        total_sites=1,
        files_with_calls=1,
        total_py_files=2,
        candidate_files=1,
        scanned_files=1,
        call_records=1,
        rg_candidates=0,
        scan_method="ast-grep",
    )
    impact_payload = ImpactSummaryUpdateV1(
        query="foo --param x",
        mode="impact",
        function="foo",
        parameter="x",
        taint_sites=1,
        max_depth=2,
        functions_analyzed=3,
        callers_found=4,
    )

    assert isinstance(msgspec.to_builtins(calls_payload, order="deterministic"), dict)
    assert isinstance(msgspec.to_builtins(impact_payload, order="deterministic"), dict)
