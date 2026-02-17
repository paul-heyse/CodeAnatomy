"""Immutability regression tests for core CQ schema contracts."""

from __future__ import annotations

import pytest
from tools.cq.core.schema import CqResult, DetailPayload, Finding, RunMeta


def _run_meta() -> RunMeta:
    return RunMeta(
        macro="search",
        argv=["cq", "search", "x"],
        root=".",
        started_ms=0.0,
        elapsed_ms=1.0,
    )


def test_finding_is_frozen_at_attribute_boundary() -> None:
    """Finding contracts reject attribute rebinding."""
    finding = Finding(category="info", message="m")
    message_attr = "message"
    with pytest.raises(AttributeError):
        setattr(finding, message_attr, "updated")


def test_cq_result_is_frozen_at_attribute_boundary() -> None:
    """CqResult contracts reject direct field replacement."""
    result = CqResult(run=_run_meta())
    summary_attr = "summary"
    key_findings_attr = "key_findings"
    with pytest.raises(AttributeError):
        setattr(result, summary_attr, result.summary)
    with pytest.raises(AttributeError):
        setattr(result, key_findings_attr, ())


def test_detail_payload_with_entry_is_copy_on_write() -> None:
    """Detail payload updates return new values and preserve the original."""
    original = DetailPayload(kind="a")
    updated = original.with_entry("name", "target")
    assert original.get("name") is None
    assert updated.get("name") == "target"
