"""Session and runtime management."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datafusion_engine.bootstrap.zero_row import (
        ZeroRowBootstrapReport,
        ZeroRowBootstrapRequest,
        ZeroRowDatasetPlan,
    )
    from datafusion_engine.session.runtime import (
        DataFusionRuntimeProfile,
        ZeroRowBootstrapConfig,
    )

__all__ = [
    "DataFusionRuntimeProfile",
    "ZeroRowBootstrapConfig",
    "ZeroRowBootstrapReport",
    "ZeroRowBootstrapRequest",
    "ZeroRowDatasetPlan",
]


def __getattr__(name: str) -> object:
    if name in {"DataFusionRuntimeProfile", "ZeroRowBootstrapConfig"}:
        from datafusion_engine.session.runtime import (
            DataFusionRuntimeProfile,
            ZeroRowBootstrapConfig,
        )

        runtime_exports = {
            "DataFusionRuntimeProfile": DataFusionRuntimeProfile,
            "ZeroRowBootstrapConfig": ZeroRowBootstrapConfig,
        }
        return runtime_exports[name]
    if name in {"ZeroRowBootstrapReport", "ZeroRowBootstrapRequest", "ZeroRowDatasetPlan"}:
        from datafusion_engine.bootstrap.zero_row import (
            ZeroRowBootstrapReport,
            ZeroRowBootstrapRequest,
            ZeroRowDatasetPlan,
        )

        bootstrap_exports = {
            "ZeroRowBootstrapReport": ZeroRowBootstrapReport,
            "ZeroRowBootstrapRequest": ZeroRowBootstrapRequest,
            "ZeroRowDatasetPlan": ZeroRowDatasetPlan,
        }
        return bootstrap_exports[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
