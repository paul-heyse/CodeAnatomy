"""Delta Lake storage utilities and registry exports."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

__all__ = (
    "DeltaCdfOptions",
    "DeltaDataCheckRequest",
    "DeltaSchemaPolicy",
    "DeltaVacuumOptions",
    "DeltaWriteOptions",
    "DeltaWritePolicy",
    "DeltaWriteResult",
    "FileIndexEntry",
    "FilePruningPolicy",
    "FilePruningResult",
    "PartitionFilter",
    "StatsFilter",
    "StorageOptions",
    "build_commit_properties",
    "build_delta_file_index",
    "build_delta_scan_config",
    "cleanup_delta_log",
    "coerce_delta_table",
    "create_delta_checkpoint",
    "delta_cdf_enabled",
    "delta_commit_metadata",
    "delta_data_checker",
    "delta_history_snapshot",
    "delta_protocol_snapshot",
    "delta_schema_configuration",
    "delta_table_features",
    "delta_table_schema",
    "delta_table_version",
    "delta_write_configuration",
    "enable_delta_features",
    "evaluate_and_select_files",
    "evaluate_filters_against_index",
    "idempotent_commit_properties",
    "open_delta_table",
    "query_builder_available",
    "query_delta_via_querybuilder",
    "read_delta_cdf",
    "select_candidate_files",
    "vacuum_delta",
    "write_delta_table",
)

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "DeltaSchemaPolicy": ("storage.deltalake.config", "DeltaSchemaPolicy"),
    "DeltaWritePolicy": ("storage.deltalake.config", "DeltaWritePolicy"),
    "delta_schema_configuration": ("storage.deltalake.config", "delta_schema_configuration"),
    "delta_write_configuration": ("storage.deltalake.config", "delta_write_configuration"),
    "DeltaCdfOptions": ("storage.deltalake.delta", "DeltaCdfOptions"),
    "DeltaDataCheckRequest": ("storage.deltalake.delta", "DeltaDataCheckRequest"),
    "DeltaVacuumOptions": ("storage.deltalake.delta", "DeltaVacuumOptions"),
    "DeltaWriteOptions": ("storage.deltalake.delta", "DeltaWriteOptions"),
    "DeltaWriteResult": ("storage.deltalake.delta", "DeltaWriteResult"),
    "StorageOptions": ("storage.deltalake.delta", "StorageOptions"),
    "build_commit_properties": ("storage.deltalake.delta", "build_commit_properties"),
    "idempotent_commit_properties": ("storage.deltalake.delta", "idempotent_commit_properties"),
    "build_delta_scan_config": ("storage.deltalake.scan_profile", "build_delta_scan_config"),
    "cleanup_delta_log": ("storage.deltalake.delta", "cleanup_delta_log"),
    "coerce_delta_table": ("storage.deltalake.delta", "coerce_delta_table"),
    "create_delta_checkpoint": ("storage.deltalake.delta", "create_delta_checkpoint"),
    "delta_cdf_enabled": ("storage.deltalake.delta", "delta_cdf_enabled"),
    "delta_commit_metadata": ("storage.deltalake.delta", "delta_commit_metadata"),
    "delta_data_checker": ("storage.deltalake.delta", "delta_data_checker"),
    "delta_history_snapshot": ("storage.deltalake.delta", "delta_history_snapshot"),
    "delta_protocol_snapshot": ("storage.deltalake.delta", "delta_protocol_snapshot"),
    "delta_table_features": ("storage.deltalake.delta", "delta_table_features"),
    "delta_table_schema": ("storage.deltalake.delta", "delta_table_schema"),
    "delta_table_version": ("storage.deltalake.delta", "delta_table_version"),
    "enable_delta_features": ("storage.deltalake.delta", "enable_delta_features"),
    "open_delta_table": ("storage.deltalake.delta", "open_delta_table"),
    "query_builder_available": ("storage.deltalake.query_builder", "query_builder_available"),
    "query_delta_via_querybuilder": (
        "storage.deltalake.query_builder",
        "query_delta_via_querybuilder",
    ),
    "read_delta_cdf": ("storage.deltalake.delta", "read_delta_cdf"),
    "write_delta_table": ("storage.deltalake.delta", "write_delta_table"),
    "vacuum_delta": ("storage.deltalake.delta", "vacuum_delta"),
    "FileIndexEntry": ("storage.deltalake.file_index", "FileIndexEntry"),
    "build_delta_file_index": ("storage.deltalake.file_index", "build_delta_file_index"),
    "PartitionFilter": ("storage.deltalake.file_pruning", "PartitionFilter"),
    "StatsFilter": ("storage.deltalake.file_pruning", "StatsFilter"),
    "FilePruningPolicy": ("storage.deltalake.file_pruning", "FilePruningPolicy"),
    "FilePruningResult": ("storage.deltalake.file_pruning", "FilePruningResult"),
    "evaluate_and_select_files": ("storage.deltalake.file_pruning", "evaluate_and_select_files"),
    "evaluate_filters_against_index": (
        "storage.deltalake.file_pruning",
        "evaluate_filters_against_index",
    ),
    "select_candidate_files": ("storage.deltalake.file_pruning", "select_candidate_files"),
}

if TYPE_CHECKING:
    import storage.deltalake.config as _delta_config
    import storage.deltalake.delta as _delta_io
    import storage.deltalake.file_index as _file_index
    import storage.deltalake.file_pruning as _file_pruning
    import storage.deltalake.query_builder as _query_builder
    import storage.deltalake.scan_profile as _scan_profile

    DeltaSchemaPolicy = _delta_config.DeltaSchemaPolicy
    DeltaWritePolicy = _delta_config.DeltaWritePolicy
    delta_schema_configuration = _delta_config.delta_schema_configuration
    delta_write_configuration = _delta_config.delta_write_configuration
    DeltaCdfOptions = _delta_io.DeltaCdfOptions
    DeltaDataCheckRequest = _delta_io.DeltaDataCheckRequest
    DeltaVacuumOptions = _delta_io.DeltaVacuumOptions
    DeltaWriteOptions = _delta_io.DeltaWriteOptions
    DeltaWriteResult = _delta_io.DeltaWriteResult
    StorageOptions = _delta_io.StorageOptions
    build_commit_properties = _delta_io.build_commit_properties
    idempotent_commit_properties = _delta_io.idempotent_commit_properties
    build_delta_scan_config = _scan_profile.build_delta_scan_config
    cleanup_delta_log = _delta_io.cleanup_delta_log
    coerce_delta_table = _delta_io.coerce_delta_table
    create_delta_checkpoint = _delta_io.create_delta_checkpoint
    delta_commit_metadata = _delta_io.delta_commit_metadata
    delta_data_checker = _delta_io.delta_data_checker
    delta_history_snapshot = _delta_io.delta_history_snapshot
    delta_protocol_snapshot = _delta_io.delta_protocol_snapshot
    delta_cdf_enabled = _delta_io.delta_cdf_enabled
    delta_table_features = _delta_io.delta_table_features
    delta_table_schema = _delta_io.delta_table_schema
    delta_table_version = _delta_io.delta_table_version
    enable_delta_features = _delta_io.enable_delta_features
    open_delta_table = _delta_io.open_delta_table
    query_builder_available = _query_builder.query_builder_available
    query_delta_via_querybuilder = _query_builder.query_delta_via_querybuilder
    read_delta_cdf = _delta_io.read_delta_cdf
    write_delta_table = _delta_io.write_delta_table
    vacuum_delta = _delta_io.vacuum_delta
    FileIndexEntry = _file_index.FileIndexEntry
    build_delta_file_index = _file_index.build_delta_file_index
    PartitionFilter = _file_pruning.PartitionFilter
    StatsFilter = _file_pruning.StatsFilter
    FilePruningPolicy = _file_pruning.FilePruningPolicy
    FilePruningResult = _file_pruning.FilePruningResult
    evaluate_and_select_files = _file_pruning.evaluate_and_select_files
    evaluate_filters_against_index = _file_pruning.evaluate_filters_against_index
    select_candidate_files = _file_pruning.select_candidate_files


def __getattr__(name: str) -> object:
    export = _EXPORT_MAP.get(name)
    if export is None:
        msg = f"module {__name__!r} has no attribute {name!r}"
        raise AttributeError(msg)
    module_name, attr = export
    module = importlib.import_module(module_name)
    value = getattr(module, attr)
    globals()[name] = value
    return value
