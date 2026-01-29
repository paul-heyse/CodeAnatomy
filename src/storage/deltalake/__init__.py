"""Delta Lake storage utilities and registry exports."""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING

__all__ = (
    "DeltaCdfOptions",
    "DeltaDataCheckRequest",
    "DeltaDeleteWhereRequest",
    "DeltaFeatureMutationOptions",
    "DeltaMergeArrowRequest",
    "DeltaReadRequest",
    "DeltaSchemaPolicy",
    "DeltaSchemaRequest",
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
    "build_delta_file_index_from_add_actions",
    "build_delta_scan_config",
    "cleanup_delta_log",
    "coerce_delta_table",
    "create_delta_checkpoint",
    "delta_add_constraints",
    "delta_cdf_enabled",
    "delta_commit_metadata",
    "delta_data_checker",
    "delta_delete_where",
    "delta_drop_constraints",
    "delta_history_snapshot",
    "delta_merge_arrow",
    "delta_protocol_snapshot",
    "delta_schema_configuration",
    "delta_table_features",
    "delta_table_schema",
    "delta_table_version",
    "delta_write_configuration",
    "disable_delta_change_data_feed",
    "disable_delta_check_constraints",
    "disable_delta_checkpoint_protection",
    "disable_delta_column_mapping",
    "disable_delta_deletion_vectors",
    "disable_delta_generated_columns",
    "disable_delta_in_commit_timestamps",
    "disable_delta_invariants",
    "disable_delta_row_tracking",
    "disable_delta_v2_checkpoints",
    "disable_delta_vacuum_protocol_check",
    "enable_delta_change_data_feed",
    "enable_delta_check_constraints",
    "enable_delta_checkpoint_protection",
    "enable_delta_column_mapping",
    "enable_delta_deletion_vectors",
    "enable_delta_features",
    "enable_delta_generated_columns",
    "enable_delta_in_commit_timestamps",
    "enable_delta_invariants",
    "enable_delta_row_tracking",
    "enable_delta_v2_checkpoints",
    "enable_delta_vacuum_protocol_check",
    "evaluate_and_select_files",
    "evaluate_filters_against_index",
    "idempotent_commit_properties",
    "query_delta_sql",
    "read_delta_cdf",
    "read_delta_table",
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
    "DeltaDeleteWhereRequest": ("storage.deltalake.delta", "DeltaDeleteWhereRequest"),
    "DeltaFeatureMutationOptions": ("storage.deltalake.delta", "DeltaFeatureMutationOptions"),
    "DeltaMergeArrowRequest": ("storage.deltalake.delta", "DeltaMergeArrowRequest"),
    "DeltaReadRequest": ("storage.deltalake.delta", "DeltaReadRequest"),
    "DeltaSchemaRequest": ("storage.deltalake.delta", "DeltaSchemaRequest"),
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
    "delta_delete_where": ("storage.deltalake.delta", "delta_delete_where"),
    "delta_add_constraints": ("storage.deltalake.delta", "delta_add_constraints"),
    "delta_drop_constraints": ("storage.deltalake.delta", "delta_drop_constraints"),
    "delta_history_snapshot": ("storage.deltalake.delta", "delta_history_snapshot"),
    "delta_merge_arrow": ("storage.deltalake.delta", "delta_merge_arrow"),
    "delta_protocol_snapshot": ("storage.deltalake.delta", "delta_protocol_snapshot"),
    "delta_table_features": ("storage.deltalake.delta", "delta_table_features"),
    "delta_table_schema": ("storage.deltalake.delta", "delta_table_schema"),
    "delta_table_version": ("storage.deltalake.delta", "delta_table_version"),
    "disable_delta_change_data_feed": (
        "storage.deltalake.delta",
        "disable_delta_change_data_feed",
    ),
    "disable_delta_check_constraints": (
        "storage.deltalake.delta",
        "disable_delta_check_constraints",
    ),
    "disable_delta_checkpoint_protection": (
        "storage.deltalake.delta",
        "disable_delta_checkpoint_protection",
    ),
    "disable_delta_column_mapping": (
        "storage.deltalake.delta",
        "disable_delta_column_mapping",
    ),
    "disable_delta_deletion_vectors": (
        "storage.deltalake.delta",
        "disable_delta_deletion_vectors",
    ),
    "disable_delta_generated_columns": (
        "storage.deltalake.delta",
        "disable_delta_generated_columns",
    ),
    "disable_delta_in_commit_timestamps": (
        "storage.deltalake.delta",
        "disable_delta_in_commit_timestamps",
    ),
    "disable_delta_invariants": ("storage.deltalake.delta", "disable_delta_invariants"),
    "disable_delta_row_tracking": ("storage.deltalake.delta", "disable_delta_row_tracking"),
    "disable_delta_v2_checkpoints": (
        "storage.deltalake.delta",
        "disable_delta_v2_checkpoints",
    ),
    "disable_delta_vacuum_protocol_check": (
        "storage.deltalake.delta",
        "disable_delta_vacuum_protocol_check",
    ),
    "enable_delta_features": ("storage.deltalake.delta", "enable_delta_features"),
    "enable_delta_change_data_feed": (
        "storage.deltalake.delta",
        "enable_delta_change_data_feed",
    ),
    "enable_delta_check_constraints": (
        "storage.deltalake.delta",
        "enable_delta_check_constraints",
    ),
    "enable_delta_checkpoint_protection": (
        "storage.deltalake.delta",
        "enable_delta_checkpoint_protection",
    ),
    "enable_delta_column_mapping": ("storage.deltalake.delta", "enable_delta_column_mapping"),
    "enable_delta_deletion_vectors": (
        "storage.deltalake.delta",
        "enable_delta_deletion_vectors",
    ),
    "enable_delta_generated_columns": (
        "storage.deltalake.delta",
        "enable_delta_generated_columns",
    ),
    "enable_delta_in_commit_timestamps": (
        "storage.deltalake.delta",
        "enable_delta_in_commit_timestamps",
    ),
    "enable_delta_invariants": ("storage.deltalake.delta", "enable_delta_invariants"),
    "enable_delta_row_tracking": ("storage.deltalake.delta", "enable_delta_row_tracking"),
    "enable_delta_v2_checkpoints": ("storage.deltalake.delta", "enable_delta_v2_checkpoints"),
    "enable_delta_vacuum_protocol_check": (
        "storage.deltalake.delta",
        "enable_delta_vacuum_protocol_check",
    ),
    "query_delta_sql": ("storage.deltalake.delta", "query_delta_sql"),
    "read_delta_cdf": ("storage.deltalake.delta", "read_delta_cdf"),
    "read_delta_table": ("storage.deltalake.delta", "read_delta_table"),
    "write_delta_table": ("storage.deltalake.delta", "write_delta_table"),
    "vacuum_delta": ("storage.deltalake.delta", "vacuum_delta"),
    "FileIndexEntry": ("storage.deltalake.file_index", "FileIndexEntry"),
    "build_delta_file_index_from_add_actions": (
        "storage.deltalake.file_index",
        "build_delta_file_index_from_add_actions",
    ),
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
    import storage.deltalake.scan_profile as _scan_profile

    DeltaSchemaPolicy = _delta_config.DeltaSchemaPolicy
    DeltaWritePolicy = _delta_config.DeltaWritePolicy
    delta_schema_configuration = _delta_config.delta_schema_configuration
    delta_write_configuration = _delta_config.delta_write_configuration
    DeltaCdfOptions = _delta_io.DeltaCdfOptions
    DeltaDataCheckRequest = _delta_io.DeltaDataCheckRequest
    DeltaDeleteWhereRequest = _delta_io.DeltaDeleteWhereRequest
    DeltaFeatureMutationOptions = _delta_io.DeltaFeatureMutationOptions
    DeltaMergeArrowRequest = _delta_io.DeltaMergeArrowRequest
    DeltaReadRequest = _delta_io.DeltaReadRequest
    DeltaSchemaRequest = _delta_io.DeltaSchemaRequest
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
    delta_delete_where = _delta_io.delta_delete_where
    delta_add_constraints = _delta_io.delta_add_constraints
    delta_drop_constraints = _delta_io.delta_drop_constraints
    delta_history_snapshot = _delta_io.delta_history_snapshot
    delta_merge_arrow = _delta_io.delta_merge_arrow
    delta_protocol_snapshot = _delta_io.delta_protocol_snapshot
    delta_cdf_enabled = _delta_io.delta_cdf_enabled
    delta_table_features = _delta_io.delta_table_features
    delta_table_schema = _delta_io.delta_table_schema
    delta_table_version = _delta_io.delta_table_version
    disable_delta_change_data_feed = _delta_io.disable_delta_change_data_feed
    disable_delta_check_constraints = _delta_io.disable_delta_check_constraints
    disable_delta_checkpoint_protection = _delta_io.disable_delta_checkpoint_protection
    disable_delta_column_mapping = _delta_io.disable_delta_column_mapping
    disable_delta_deletion_vectors = _delta_io.disable_delta_deletion_vectors
    disable_delta_generated_columns = _delta_io.disable_delta_generated_columns
    disable_delta_in_commit_timestamps = _delta_io.disable_delta_in_commit_timestamps
    disable_delta_invariants = _delta_io.disable_delta_invariants
    disable_delta_row_tracking = _delta_io.disable_delta_row_tracking
    disable_delta_v2_checkpoints = _delta_io.disable_delta_v2_checkpoints
    disable_delta_vacuum_protocol_check = _delta_io.disable_delta_vacuum_protocol_check
    enable_delta_features = _delta_io.enable_delta_features
    enable_delta_change_data_feed = _delta_io.enable_delta_change_data_feed
    enable_delta_check_constraints = _delta_io.enable_delta_check_constraints
    enable_delta_checkpoint_protection = _delta_io.enable_delta_checkpoint_protection
    enable_delta_column_mapping = _delta_io.enable_delta_column_mapping
    enable_delta_deletion_vectors = _delta_io.enable_delta_deletion_vectors
    enable_delta_generated_columns = _delta_io.enable_delta_generated_columns
    enable_delta_in_commit_timestamps = _delta_io.enable_delta_in_commit_timestamps
    enable_delta_invariants = _delta_io.enable_delta_invariants
    enable_delta_row_tracking = _delta_io.enable_delta_row_tracking
    enable_delta_v2_checkpoints = _delta_io.enable_delta_v2_checkpoints
    enable_delta_vacuum_protocol_check = _delta_io.enable_delta_vacuum_protocol_check
    query_delta_sql = _delta_io.query_delta_sql
    read_delta_cdf = _delta_io.read_delta_cdf
    read_delta_table = _delta_io.read_delta_table
    write_delta_table = _delta_io.write_delta_table
    vacuum_delta = _delta_io.vacuum_delta
    FileIndexEntry = _file_index.FileIndexEntry
    build_delta_file_index_from_add_actions = _file_index.build_delta_file_index_from_add_actions
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
