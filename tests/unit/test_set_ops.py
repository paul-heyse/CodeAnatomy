"""Tests for set operation helpers."""

from __future__ import annotations

import ibis
import pytest

from arrowdsl.core.ordering import Ordering
from ibis_engine.expr_compiler import (
    align_set_op_tables,
    difference_tables,
    intersect_tables,
    union_tables,
)
from ibis_engine.plan import IbisPlan
from relspec.compiler_graph import union_plans

EXPECTED_UNION_ALL_COUNT = 4
EXPECTED_UNION_DISTINCT_COUNT = 3


def test_union_intersect_difference_consistency() -> None:
    """Validate set operation semantics with explicit distinct flags."""
    backend = ibis.datafusion.connect()
    left = ibis.memtable({"a": [1, 2]})
    right = ibis.memtable({"a": [2, 3]})
    union_all = union_tables([left, right], distinct=False)
    union_distinct = union_tables([left, right], distinct=True)
    intersected = intersect_tables([left, right], distinct=True)
    diff = difference_tables([left, right], distinct=True)
    assert len(backend.execute(union_all)) == EXPECTED_UNION_ALL_COUNT
    assert len(backend.execute(union_distinct)) == EXPECTED_UNION_DISTINCT_COUNT
    assert backend.execute(intersected)["a"].tolist() == [2]
    assert backend.execute(diff)["a"].tolist() == [1]


def test_align_set_op_tables_rejects_type_mismatch() -> None:
    """Reject mismatched schemas during alignment."""
    left = ibis.memtable({"a": [1]})
    right = ibis.memtable({"a": ["x"]})
    with pytest.raises(ValueError, match="type mismatch"):
        align_set_op_tables([left, right])


def test_union_plans_rejects_mismatched_ordering() -> None:
    """Require consistent ordering metadata for set ops."""
    table = ibis.memtable({"a": [1]})
    plan_ordered = IbisPlan(expr=table, ordering=Ordering.explicit((("a", "asc"),)))
    plan_unordered = IbisPlan(expr=table, ordering=Ordering.unordered())
    with pytest.raises(ValueError, match="ordering levels"):
        union_plans([plan_ordered, plan_unordered], label="set_ops")
