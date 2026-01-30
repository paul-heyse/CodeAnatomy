"""Plan bundle helpers for tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa

from datafusion_engine.plan.bundle import DataFusionPlanBundle, PlanBundleOptions, build_plan_bundle
from tests.test_helpers.arrow_seed import register_arrow_table

if TYPE_CHECKING:
    from datafusion import SessionContext

    from datafusion_engine.session.runtime import SessionRuntime


def bundle_for_table(
    ctx: SessionContext,
    *,
    table: pa.Table,
    name: str,
    session_runtime: SessionRuntime,
) -> DataFusionPlanBundle:
    """Build a plan bundle for a seeded table.

    Parameters
    ----------
    ctx
        DataFusion session context to use.
    table
        PyArrow table to register.
    name
        Table name to register.
    session_runtime
        Session runtime used to populate plan bundle options.

    Returns
    -------
    DataFusionPlanBundle
        Built plan bundle for the provided table.
    """
    df = register_arrow_table(ctx, name=name, value=table)
    return build_plan_bundle(ctx, df, options=PlanBundleOptions(session_runtime=session_runtime))
