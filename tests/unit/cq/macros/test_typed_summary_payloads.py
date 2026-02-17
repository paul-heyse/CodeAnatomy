"""Tests for typed macro summary payload builders."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.summary_update_contracts import CallsSummaryUpdateV1, ImpactSummaryUpdateV1
from tools.cq.core.toolchain import Toolchain
from tools.cq.macros.calls.entry import CallScanResult, _build_calls_summary
from tools.cq.macros.impact import ImpactRequest, _build_impact_summary


def test_build_calls_summary_returns_typed_contract() -> None:
    """Verify calls macro summary builder returns typed payload contract."""
    summary = _build_calls_summary(
        "target",
        CallScanResult(
            candidate_files=[],
            scan_files=[],
            total_py_files=0,
            call_records=[],
            used_fallback=False,
            all_sites=[],
            files_with_calls=0,
            rg_candidates=0,
            signature_info="",
        ),
    )

    assert isinstance(summary, CallsSummaryUpdateV1)
    assert summary.function == "target"


def test_build_impact_summary_returns_typed_contract(tmp_path: Path) -> None:
    """Verify impact macro summary builder returns typed payload contract."""
    request = ImpactRequest(
        tc=Toolchain.detect(),
        root=tmp_path,
        argv=[],
        function_name="target",
        param_name="x",
        max_depth=2,
    )
    summary = _build_impact_summary(request, functions=[], all_sites=[], caller_sites=[])

    assert isinstance(summary, ImpactSummaryUpdateV1)
    assert summary.function == "target"
    assert summary.parameter == "x"
