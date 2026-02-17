"""Tests for frozen query scan contracts."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest
from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.query.scan import build_entity_candidates, build_scan_context


def _def_record() -> SgRecord:
    return SgRecord(
        record="def",
        kind="function",
        file="a.py",
        start_line=1,
        start_col=0,
        end_line=10,
        end_col=0,
        text="def target(): pass",
        rule_id="py_def_function",
    )


def _call_record() -> SgRecord:
    return SgRecord(
        record="call",
        kind="name_call",
        file="a.py",
        start_line=5,
        start_col=4,
        end_line=5,
        end_col=12,
        text="target()",
        rule_id="py_call_name",
    )


def test_scan_context_uses_frozen_tuple_backed_fields() -> None:
    """Scan context should expose tuple-backed immutable surfaces."""
    records = [_def_record(), _call_record()]
    scan = build_scan_context(records)

    assert isinstance(scan.def_records, tuple)
    assert isinstance(scan.call_records, tuple)
    assert isinstance(scan.all_records, tuple)
    assert isinstance(scan.calls_by_def.get(records[0]), tuple)

    with pytest.raises(FrozenInstanceError):
        scan.def_records = ()

    with pytest.raises(TypeError):
        scan.calls_by_def[records[0]] = ()


def test_entity_candidates_are_frozen_and_tuple_backed() -> None:
    """Entity candidates should preserve immutable tuple-backed record buckets."""
    records = [_def_record(), _call_record()]
    scan = build_scan_context(records)
    candidates = build_entity_candidates(scan, records)

    assert isinstance(candidates.def_records, tuple)
    assert isinstance(candidates.import_records, tuple)
    assert isinstance(candidates.call_records, tuple)

    with pytest.raises(FrozenInstanceError):
        candidates.def_records = ()
