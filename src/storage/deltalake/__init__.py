"""Delta Lake storage utilities and registry exports."""

from __future__ import annotations

from typing import TYPE_CHECKING

from utils.lazy_module import make_lazy_loader

__all__ = (
    "DeltaCdfOptions",
    "DeltaDeleteWhereRequest",
    "DeltaFeatureMutationOptions",
    "DeltaMergeArrowRequest",
    "DeltaMutationPolicy",
    "DeltaReadRequest",
    "DeltaRetryPolicy",
    "DeltaSchemaPolicy",
    "DeltaSchemaRequest",
    "DeltaVacuumOptions",
    "DeltaWritePolicy",
    "DeltaWriteResult",
    "FilePruningPolicy",
    "FilePruningResult",
    "PartitionFilter",
    "SnapshotKey",
    "StatsFilter",
    "StorageOptions",
    "build_commit_properties",
    "build_delta_file_index_from_add_actions",
    "canonical_table_uri",
    "cleanup_delta_log",
    "coerce_delta_input",
    "coerce_delta_table",
    "create_delta_checkpoint",
    "delta_add_constraints",
    "delta_cdf_enabled",
    "delta_commit_metadata",
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
    "enable_delta_change_data_feed",
    "enable_delta_check_constraints",
    "enable_delta_column_mapping",
    "enable_delta_deletion_vectors",
    "enable_delta_features",
    "enable_delta_in_commit_timestamps",
    "enable_delta_row_tracking",
    "enable_delta_v2_checkpoints",
    "evaluate_and_select_files",
    "evaluate_filters_against_index",
    "idempotent_commit_properties",
    "read_delta_cdf",
    "read_delta_cdf_eager",
    "read_delta_table",
    "read_delta_table_eager",
    "select_candidate_files",
    "snapshot_key_for_table",
    "vacuum_delta",
)

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "DeltaMutationPolicy": ("storage.deltalake.config", "DeltaMutationPolicy"),
    "DeltaRetryPolicy": ("storage.deltalake.config", "DeltaRetryPolicy"),
    "DeltaSchemaPolicy": ("storage.deltalake.config", "DeltaSchemaPolicy"),
    "DeltaWritePolicy": ("storage.deltalake.config", "DeltaWritePolicy"),
    "delta_schema_configuration": ("storage.deltalake.config", "delta_schema_configuration"),
    "delta_write_configuration": ("storage.deltalake.config", "delta_write_configuration"),
    "DeltaCdfOptions": ("storage.deltalake.delta_read", "DeltaCdfOptions"),
    "DeltaDeleteWhereRequest": ("storage.deltalake.delta_read", "DeltaDeleteWhereRequest"),
    "DeltaFeatureMutationOptions": ("storage.deltalake.delta_write", "DeltaFeatureMutationOptions"),
    "DeltaMergeArrowRequest": ("storage.deltalake.delta_read", "DeltaMergeArrowRequest"),
    "DeltaReadRequest": ("storage.deltalake.delta_read", "DeltaReadRequest"),
    "DeltaSchemaRequest": ("storage.deltalake.delta_metadata", "DeltaSchemaRequest"),
    "SnapshotKey": ("storage.deltalake.delta_metadata", "SnapshotKey"),
    "DeltaVacuumOptions": ("storage.deltalake.delta_read", "DeltaVacuumOptions"),
    "DeltaWriteResult": ("storage.deltalake.delta_write", "DeltaWriteResult"),
    "StorageOptions": ("storage.deltalake.delta_read", "StorageOptions"),
    "build_commit_properties": ("storage.deltalake.delta_write", "build_commit_properties"),
    "canonical_table_uri": ("storage.deltalake.delta_metadata", "canonical_table_uri"),
    "idempotent_commit_properties": (
        "storage.deltalake.delta_write",
        "idempotent_commit_properties",
    ),
    "cleanup_delta_log": ("storage.deltalake.delta_maintenance", "cleanup_delta_log"),
    "coerce_delta_input": ("storage.deltalake.delta_read", "coerce_delta_input"),
    "coerce_delta_table": ("storage.deltalake.delta_read", "coerce_delta_table"),
    "create_delta_checkpoint": (
        "storage.deltalake.delta_maintenance",
        "create_delta_checkpoint",
    ),
    "delta_cdf_enabled": ("storage.deltalake.delta_read", "delta_cdf_enabled"),
    "delta_commit_metadata": ("storage.deltalake.delta_read", "delta_commit_metadata"),
    "delta_delete_where": ("storage.deltalake.delta_write", "delta_delete_where"),
    "delta_add_constraints": ("storage.deltalake.delta_write", "delta_add_constraints"),
    "delta_drop_constraints": ("storage.deltalake.delta_write", "delta_drop_constraints"),
    "delta_history_snapshot": ("storage.deltalake.delta_read", "delta_history_snapshot"),
    "delta_merge_arrow": ("storage.deltalake.delta_write", "delta_merge_arrow"),
    "delta_protocol_snapshot": ("storage.deltalake.delta_read", "delta_protocol_snapshot"),
    "delta_table_features": ("storage.deltalake.delta_read", "delta_table_features"),
    "delta_table_schema": ("storage.deltalake.delta_metadata", "delta_table_schema"),
    "delta_table_version": ("storage.deltalake.delta_read", "delta_table_version"),
    "read_delta_cdf": ("storage.deltalake.delta_read", "read_delta_cdf"),
    "read_delta_cdf_eager": ("storage.deltalake.delta_read", "read_delta_cdf_eager"),
    "read_delta_table": ("storage.deltalake.delta_read", "read_delta_table"),
    "read_delta_table_eager": ("storage.deltalake.delta_read", "read_delta_table_eager"),
    "snapshot_key_for_table": ("storage.deltalake.delta_metadata", "snapshot_key_for_table"),
    "enable_delta_features": ("storage.deltalake.delta_write", "enable_delta_features"),
    "enable_delta_change_data_feed": (
        "storage.deltalake.delta_write",
        "enable_delta_change_data_feed",
    ),
    "enable_delta_check_constraints": (
        "storage.deltalake.delta_write",
        "enable_delta_check_constraints",
    ),
    "enable_delta_column_mapping": (
        "storage.deltalake.delta_write",
        "enable_delta_column_mapping",
    ),
    "enable_delta_deletion_vectors": (
        "storage.deltalake.delta_write",
        "enable_delta_deletion_vectors",
    ),
    "enable_delta_in_commit_timestamps": (
        "storage.deltalake.delta_write",
        "enable_delta_in_commit_timestamps",
    ),
    "enable_delta_row_tracking": ("storage.deltalake.delta_write", "enable_delta_row_tracking"),
    "enable_delta_v2_checkpoints": (
        "storage.deltalake.delta_write",
        "enable_delta_v2_checkpoints",
    ),
    "vacuum_delta": ("storage.deltalake.delta_maintenance", "vacuum_delta"),
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
    import storage.deltalake.delta_maintenance as _delta_maintenance
    import storage.deltalake.delta_metadata as _delta_metadata
    import storage.deltalake.delta_read as _delta_io
    import storage.deltalake.delta_write as _delta_write
    import storage.deltalake.file_index as _file_index
    import storage.deltalake.file_pruning as _file_pruning

    DeltaMutationPolicy = _delta_config.DeltaMutationPolicy
    DeltaRetryPolicy = _delta_config.DeltaRetryPolicy
    DeltaSchemaPolicy = _delta_config.DeltaSchemaPolicy
    DeltaWritePolicy = _delta_config.DeltaWritePolicy
    delta_schema_configuration = _delta_config.delta_schema_configuration
    delta_write_configuration = _delta_config.delta_write_configuration
    DeltaCdfOptions = _delta_io.DeltaCdfOptions
    DeltaDeleteWhereRequest = _delta_io.DeltaDeleteWhereRequest
    DeltaFeatureMutationOptions = _delta_write.DeltaFeatureMutationOptions
    DeltaMergeArrowRequest = _delta_io.DeltaMergeArrowRequest
    DeltaReadRequest = _delta_io.DeltaReadRequest
    DeltaSchemaRequest = _delta_metadata.DeltaSchemaRequest
    SnapshotKey = _delta_metadata.SnapshotKey
    DeltaVacuumOptions = _delta_io.DeltaVacuumOptions
    DeltaWriteResult = _delta_write.DeltaWriteResult
    StorageOptions = _delta_io.StorageOptions
    build_commit_properties = _delta_write.build_commit_properties
    canonical_table_uri = _delta_metadata.canonical_table_uri
    idempotent_commit_properties = _delta_write.idempotent_commit_properties
    cleanup_delta_log = _delta_maintenance.cleanup_delta_log
    coerce_delta_input = _delta_io.coerce_delta_input
    coerce_delta_table = _delta_io.coerce_delta_table
    create_delta_checkpoint = _delta_maintenance.create_delta_checkpoint
    delta_commit_metadata = _delta_io.delta_commit_metadata
    delta_delete_where = _delta_write.delta_delete_where
    delta_add_constraints = _delta_write.delta_add_constraints
    delta_drop_constraints = _delta_write.delta_drop_constraints
    delta_history_snapshot = _delta_io.delta_history_snapshot
    delta_merge_arrow = _delta_write.delta_merge_arrow
    delta_protocol_snapshot = _delta_io.delta_protocol_snapshot
    delta_cdf_enabled = _delta_io.delta_cdf_enabled
    delta_table_features = _delta_io.delta_table_features
    delta_table_schema = _delta_metadata.delta_table_schema
    delta_table_version = _delta_io.delta_table_version
    enable_delta_features = _delta_write.enable_delta_features
    enable_delta_change_data_feed = _delta_write.enable_delta_change_data_feed
    enable_delta_check_constraints = _delta_write.enable_delta_check_constraints
    enable_delta_column_mapping = _delta_write.enable_delta_column_mapping
    enable_delta_deletion_vectors = _delta_write.enable_delta_deletion_vectors
    enable_delta_in_commit_timestamps = _delta_write.enable_delta_in_commit_timestamps
    enable_delta_row_tracking = _delta_write.enable_delta_row_tracking
    enable_delta_v2_checkpoints = _delta_write.enable_delta_v2_checkpoints
    read_delta_cdf = _delta_io.read_delta_cdf
    read_delta_cdf_eager = _delta_io.read_delta_cdf_eager
    read_delta_table = _delta_io.read_delta_table
    read_delta_table_eager = _delta_io.read_delta_table_eager
    snapshot_key_for_table = _delta_metadata.snapshot_key_for_table
    vacuum_delta = _delta_maintenance.vacuum_delta
    build_delta_file_index_from_add_actions = _file_index.build_delta_file_index_from_add_actions
    PartitionFilter = _file_pruning.PartitionFilter
    StatsFilter = _file_pruning.StatsFilter
    FilePruningPolicy = _file_pruning.FilePruningPolicy
    FilePruningResult = _file_pruning.FilePruningResult
    evaluate_and_select_files = _file_pruning.evaluate_and_select_files
    evaluate_filters_against_index = _file_pruning.evaluate_filters_against_index
    select_candidate_files = _file_pruning.select_candidate_files


__getattr__, __dir__ = make_lazy_loader(_EXPORT_MAP, __name__, globals())
