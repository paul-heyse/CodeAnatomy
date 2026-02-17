"""Engine-ring observation helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from utils.lazy_module import make_lazy_loader

if TYPE_CHECKING:
    from datafusion_engine.obs.datafusion_runs import (
        DataFusionRun,
        create_run_context,
        finish_run,
        start_run,
        tracked_run,
    )
    from datafusion_engine.obs.diagnostics_bridge import (
        record_dataframe_validation_error,
        record_rust_udf_snapshot,
        record_view_artifact,
        record_view_contract_violations,
        record_view_fingerprints,
        record_view_udf_parity,
    )
    from datafusion_engine.obs.metrics_bridge import (
        column_stats_table,
        dataset_stats_table,
        empty_scan_telemetry_table,
        scan_telemetry_table,
        table_summary,
    )

_EXPORT_MAP: dict[str, str | tuple[str, str]] = {
    "DataFusionRun": ("datafusion_engine.obs.datafusion_runs", "DataFusionRun"),
    "create_run_context": ("datafusion_engine.obs.datafusion_runs", "create_run_context"),
    "finish_run": ("datafusion_engine.obs.datafusion_runs", "finish_run"),
    "start_run": ("datafusion_engine.obs.datafusion_runs", "start_run"),
    "tracked_run": ("datafusion_engine.obs.datafusion_runs", "tracked_run"),
    "record_dataframe_validation_error": (
        "datafusion_engine.obs.diagnostics_bridge",
        "record_dataframe_validation_error",
    ),
    "record_rust_udf_snapshot": (
        "datafusion_engine.obs.diagnostics_bridge",
        "record_rust_udf_snapshot",
    ),
    "record_view_artifact": ("datafusion_engine.obs.diagnostics_bridge", "record_view_artifact"),
    "record_view_contract_violations": (
        "datafusion_engine.obs.diagnostics_bridge",
        "record_view_contract_violations",
    ),
    "record_view_fingerprints": (
        "datafusion_engine.obs.diagnostics_bridge",
        "record_view_fingerprints",
    ),
    "record_view_udf_parity": (
        "datafusion_engine.obs.diagnostics_bridge",
        "record_view_udf_parity",
    ),
    "column_stats_table": ("datafusion_engine.obs.metrics_bridge", "column_stats_table"),
    "dataset_stats_table": ("datafusion_engine.obs.metrics_bridge", "dataset_stats_table"),
    "empty_scan_telemetry_table": (
        "datafusion_engine.obs.metrics_bridge",
        "empty_scan_telemetry_table",
    ),
    "scan_telemetry_table": ("datafusion_engine.obs.metrics_bridge", "scan_telemetry_table"),
    "table_summary": ("datafusion_engine.obs.metrics_bridge", "table_summary"),
}

__getattr__, __dir__ = make_lazy_loader(_EXPORT_MAP, __name__, globals())

__all__ = [
    "DataFusionRun",
    "column_stats_table",
    "create_run_context",
    "dataset_stats_table",
    "empty_scan_telemetry_table",
    "finish_run",
    "record_dataframe_validation_error",
    "record_rust_udf_snapshot",
    "record_view_artifact",
    "record_view_contract_violations",
    "record_view_fingerprints",
    "record_view_udf_parity",
    "scan_telemetry_table",
    "start_run",
    "table_summary",
    "tracked_run",
]
