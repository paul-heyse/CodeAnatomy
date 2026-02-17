"""Canonical required runtime entrypoints for native datafusion extension."""

from __future__ import annotations

REQUIRED_RUNTIME_ENTRYPOINTS: tuple[str, ...] = (
    "capabilities_snapshot",
    "session_context_contract_probe",
    "install_codeanatomy_runtime",
    "registry_snapshot",
    "udf_docs_snapshot",
    "delta_write_ipc_request",
    "delta_merge_request_payload",
    "delta_cdf_table_provider",
    "build_extraction_session",
    "register_dataset_provider",
    "register_cache_tables",
    "install_tracing",
    "capture_plan_bundle_runtime",
    "build_plan_bundle_artifact_with_warnings",
)

__all__ = ["REQUIRED_RUNTIME_ENTRYPOINTS"]
