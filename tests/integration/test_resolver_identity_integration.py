"""Integration coverage for resolver identity across runtime boundaries."""

from __future__ import annotations

import pytest

from datafusion_engine.dataset.resolution import apply_scan_unit_overrides
from datafusion_engine.delta.cdf import register_cdf_inputs
from datafusion_engine.session.facade import DataFusionExecutionFacade
from datafusion_engine.session.runtime import record_dataset_readiness
from semantics.compile_context import build_semantic_execution_context
from semantics.resolver_identity import resolver_identity_tracking
from tests.test_helpers.datafusion_runtime import df_profile
from tests.test_helpers.optional_deps import require_datafusion_udfs

require_datafusion_udfs()
pytest.importorskip("rustworkx")


@pytest.mark.integration
def test_resolver_identity_single_instance_across_pipeline_boundaries() -> None:
    """View registration, scan/CDF, and readiness should share one resolver object."""
    profile = df_profile()
    ctx = profile.session_context()
    semantic_context = build_semantic_execution_context(
        runtime_profile=profile,
        ctx=ctx,
        outputs=("rel_def_symbol",),
    )
    resolver = semantic_context.dataset_resolver
    facade = DataFusionExecutionFacade(ctx=ctx, runtime_profile=profile)

    with resolver_identity_tracking(label="integration_pipeline", strict=True) as tracker:
        facade.ensure_view_graph(
            semantic_manifest=semantic_context.manifest,
            dataset_resolver=resolver,
        )
        apply_scan_unit_overrides(
            ctx,
            scan_units=(),
            runtime_profile=profile,
            dataset_resolver=resolver,
        )
        register_cdf_inputs(
            ctx,
            profile,
            table_names=(),
            dataset_resolver=resolver,
        )
        record_dataset_readiness(
            profile,
            dataset_names=(),
            dataset_resolver=resolver,
        )

    assert tracker.distinct_resolvers() == 1
    assert tracker.verify_identity() == []
