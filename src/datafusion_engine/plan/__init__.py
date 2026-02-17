"""Plan building and execution."""

from __future__ import annotations

from typing import TYPE_CHECKING

from utils.lazy_module import make_lazy_loader

if TYPE_CHECKING:
    from datafusion_engine.plan.artifact_store_core import (
        ensure_pipeline_events_table,
        ensure_plan_artifacts_table,
    )
    from datafusion_engine.plan.bundle_artifact import (
        DataFusionPlanArtifact,
        PlanBundleOptions,
        build_plan_artifact,
    )

__all__ = [
    "DataFusionPlanArtifact",
    "PlanBundleOptions",
    "build_plan_artifact",
    "ensure_pipeline_events_table",
    "ensure_plan_artifacts_table",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "DataFusionPlanArtifact": ("datafusion_engine.plan.bundle_artifact", "DataFusionPlanArtifact"),
    "PlanBundleOptions": ("datafusion_engine.plan.bundle_artifact", "PlanBundleOptions"),
    "build_plan_artifact": ("datafusion_engine.plan.bundle_artifact", "build_plan_artifact"),
    "ensure_pipeline_events_table": (
        "datafusion_engine.plan.artifact_store_core",
        "ensure_pipeline_events_table",
    ),
    "ensure_plan_artifacts_table": (
        "datafusion_engine.plan.artifact_store_core",
        "ensure_plan_artifacts_table",
    ),
}

__getattr__, __dir__ = make_lazy_loader(_EXPORT_MAP, __name__, globals())
