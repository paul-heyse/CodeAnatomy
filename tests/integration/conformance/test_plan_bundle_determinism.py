"""Deterministic plan-bundle conformance checks."""

from __future__ import annotations

import pyarrow as pa
import pytest

from datafusion_engine.plan.substrait_artifacts import substrait_bytes_from_rust_bundle
from tests.harness.profiles import conformance_profile


@pytest.mark.integration
def test_plan_bundle_substrait_bytes_are_deterministic(conformance_backend: str) -> None:
    """Repeated bundle capture for the same query should be byte-stable."""
    _ = conformance_backend
    profile = conformance_profile()
    ctx = profile.session_context()
    ctx.from_arrow(pa.table({"id": [1, 2, 3]}), name="dataset")
    df = ctx.sql("SELECT id FROM dataset WHERE id > 1")

    first_bytes, first_udfs = substrait_bytes_from_rust_bundle(
        ctx,
        df,
        session_runtime=None,
    )
    second_bytes, second_udfs = substrait_bytes_from_rust_bundle(
        ctx,
        df,
        session_runtime=None,
    )

    assert first_bytes == second_bytes
    assert first_udfs == second_udfs
