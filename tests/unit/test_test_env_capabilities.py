"""Test profile capability sanity checks."""

from __future__ import annotations

import importlib

from datafusion_engine.session.runtime import (
    DataFusionRuntimeProfile,
    FeatureGatesConfig,
    PolicyBundleConfig,
)
from tests.test_helpers.optional_deps import (
    require_datafusion,
    require_datafusion_udfs,
    require_delta_extension,
    require_deltalake,
)


def test_required_runtime_capabilities_available() -> None:
    """Fail fast when required runtime dependencies are missing."""
    datafusion = require_datafusion()
    _ = require_datafusion_udfs()
    _ = require_deltalake()
    _ = require_delta_extension()

    substrait_module = importlib.import_module("datafusion.substrait")
    assert callable(getattr(substrait_module.Consumer, "from_substrait_plan", None))
    assert callable(getattr(substrait_module.Producer, "to_substrait_plan", None))
    extension = importlib.import_module("datafusion_ext")
    assert callable(getattr(extension, "register_codeanatomy_udfs", None))
    assert callable(getattr(extension, "delta_write_ipc", None))
    assert hasattr(datafusion, "SessionContext")

    profile = DataFusionRuntimeProfile(
        features=FeatureGatesConfig(enable_async_udfs=True),
        policies=PolicyBundleConfig(
            async_udf_timeout_ms=500,
            async_udf_batch_size=64,
        ),
    )
    runtime = profile.session_runtime()
    assert runtime.profile.features.enable_async_udfs is True
