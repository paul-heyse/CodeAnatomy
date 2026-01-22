"""Smoke tests for engine session construction."""

from __future__ import annotations

import ibis
import pyarrow as pa
import pytest

from arrowdsl.core.execution_context import ExecutionContext
from arrowdsl.core.ordering import Ordering
from arrowdsl.core.runtime_profiles import runtime_profile_factory
from engine import build_plan_product
from engine.session_factory import build_engine_session
from ibis_engine.execution import IbisExecutionContext
from ibis_engine.plan import IbisPlan

EXPECTED_ROWS = 2


@pytest.mark.integration
def test_engine_session_runs_plan() -> None:
    """Build an EngineSession and run a trivial plan."""
    runtime = runtime_profile_factory("default")
    ctx = ExecutionContext(runtime=runtime)
    session = build_engine_session(ctx=ctx)
    plan = IbisPlan(
        expr=ibis.memtable(pa.table({"value": [1, 2]})),
        ordering=Ordering.unordered(),
    )
    execution = IbisExecutionContext(ctx=session.ctx, ibis_backend=session.ibis_backend)
    product = build_plan_product(plan, execution=execution, policy=session.surface_policy)
    table = product.materialize_table()
    assert table.num_rows == EXPECTED_ROWS
