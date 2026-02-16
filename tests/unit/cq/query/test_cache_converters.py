"""Tests for query cache converter helpers."""

from __future__ import annotations

from tools.cq.astgrep.sgpy_scanner import SgRecord
from tools.cq.core.schema import Anchor, Finding
from tools.cq.query.cache_converters import (
    cache_record_to_record,
    finding_sort_key_detailed,
    finding_sort_key_lightweight,
    record_sort_key_detailed,
    record_sort_key_lightweight,
    record_to_cache_record,
)


def _sample_record() -> SgRecord:
    """Build a representative `SgRecord` fixture for converter tests.

    Returns:
        SgRecord: Representative query record fixture.
    """
    return SgRecord(
        record="def",
        kind="function",
        file="src/mod.py",
        start_line=10,
        start_col=2,
        end_line=12,
        end_col=1,
        text="def target():\n    pass\n",
        rule_id="python.def.function",
    )


def test_record_cache_roundtrip_preserves_fields() -> None:
    """Cache conversion should preserve all serialized `SgRecord` fields."""
    record = _sample_record()
    cache_record = record_to_cache_record(record)
    decoded = cache_record_to_record(cache_record)
    assert decoded == record


def test_record_sort_keys_match_expected_shapes() -> None:
    """Record sort keys should expose stable tuple shapes for ordering."""
    record = _sample_record()
    assert record_sort_key_lightweight(record) == ("src/mod.py", 10, 2)
    assert record_sort_key_detailed(record) == (
        "src/mod.py",
        10,
        2,
        "def",
        "function",
        "python.def.function",
        "def target():\n    pass\n",
    )


def test_finding_sort_keys_match_expected_shapes() -> None:
    """Finding sort keys should include anchor position and message fields."""
    finding = Finding(
        category="definition",
        message="function: target",
        anchor=Anchor(file="src/mod.py", line=10, col=2),
    )
    assert finding_sort_key_lightweight(finding) == ("src/mod.py", 10, 2)
    assert finding_sort_key_detailed(finding) == ("src/mod.py", 10, 2, "function: target")
