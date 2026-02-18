"""Tests for typed macro summary payload builders."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.summary_update_contracts import CallsSummaryUpdateV1, ImpactSummaryUpdateV1
from tools.cq.core.toolchain import Toolchain
from tools.cq.macros.calls.entry_runtime import CallScanResult
from tools.cq.macros.calls.entry_summary import CallsSummaryMetrics, build_calls_summary
from tools.cq.macros.impact import ImpactRequest, _build_impact_summary


def test_build_calls_summary_returns_typed_contract() -> None:
    """Verify calls macro summary builder returns typed payload contract."""
    scan = CallScanResult(
        candidate_files=[],
        scan_files=[],
        total_py_files=0,
        call_records=[],
        used_fallback=False,
        all_sites=[],
        files_with_calls=0,
        rg_candidates=0,
        signature_info="",
    )
    summary = build_calls_summary(
        function_name="target",
        signature=scan.signature_info,
        metrics=CallsSummaryMetrics(
            total_sites=len(scan.all_sites),
            files_with_calls=scan.files_with_calls,
            total_py_files=scan.total_py_files,
            candidate_files=len(scan.candidate_files),
            scanned_files=len(scan.scan_files),
            call_records=len(scan.call_records),
            rg_candidates=scan.rg_candidates,
            used_fallback=scan.used_fallback,
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
