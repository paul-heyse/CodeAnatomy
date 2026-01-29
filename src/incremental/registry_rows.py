"""Dataset row specifications for incremental artifacts."""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa

import datafusion_engine.arrow_interop as interop
from schema_spec.contract_row import ContractRow
from schema_spec.specs import ArrowFieldSpec
from schema_spec.system import TableSpecConstraints

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class DatasetRow:
    """Row specification for incremental datasets."""

    name: str
    version: int
    fields: tuple[ArrowFieldSpec, ...]
    constraints: TableSpecConstraints | None = None
    contract: ContractRow | None = None
    metadata_extra: dict[bytes, bytes] = field(default_factory=dict)


DATASET_ROWS: tuple[DatasetRow, ...] = (
    DatasetRow(
        name="py_module_index_v1",
        version=SCHEMA_VERSION,
        fields=(
            ArrowFieldSpec(name="file_id", dtype=interop.string()),
            ArrowFieldSpec(name="path", dtype=interop.string()),
            ArrowFieldSpec(name="module_fqn", dtype=interop.string()),
            ArrowFieldSpec(name="is_init", dtype=interop.bool_()),
        ),
        constraints=TableSpecConstraints(
            required_non_null=("file_id", "path"),
            key_fields=("file_id",),
        ),
    ),
    DatasetRow(
        name="py_imports_resolved_v1",
        version=SCHEMA_VERSION,
        fields=(
            ArrowFieldSpec(name="importer_file_id", dtype=interop.string()),
            ArrowFieldSpec(name="importer_path", dtype=interop.string()),
            ArrowFieldSpec(name="imported_module_fqn", dtype=interop.string()),
            ArrowFieldSpec(name="imported_name", dtype=interop.string()),
            ArrowFieldSpec(name="is_star", dtype=interop.bool_()),
        ),
        constraints=TableSpecConstraints(
            required_non_null=(
                "importer_file_id",
                "importer_path",
                "imported_module_fqn",
                "is_star",
            ),
            key_fields=(
                "importer_file_id",
                "imported_module_fqn",
                "imported_name",
                "is_star",
            ),
        ),
    ),
    DatasetRow(
        name="dim_exported_defs_v1",
        version=SCHEMA_VERSION,
        fields=(
            ArrowFieldSpec(name="file_id", dtype=interop.string()),
            ArrowFieldSpec(name="path", dtype=interop.string()),
            ArrowFieldSpec(name="def_id", dtype=interop.string()),
            ArrowFieldSpec(name="def_kind_norm", dtype=interop.string()),
            ArrowFieldSpec(name="name", dtype=interop.string()),
            ArrowFieldSpec(name="qname_id", dtype=interop.string()),
            ArrowFieldSpec(name="qname", dtype=interop.string()),
            ArrowFieldSpec(name="qname_source", dtype=interop.string()),
            ArrowFieldSpec(name="symbol", dtype=interop.string()),
            ArrowFieldSpec(name="symbol_roles", dtype=interop.int32()),
        ),
        constraints=TableSpecConstraints(
            required_non_null=("file_id", "def_id", "qname_id", "qname"),
            key_fields=("file_id", "def_id", "qname_id"),
        ),
    ),
    DatasetRow(
        name="inc_changed_exports_v1",
        version=SCHEMA_VERSION,
        fields=(
            ArrowFieldSpec(name="delta_kind", dtype=interop.string()),
            ArrowFieldSpec(name="file_id", dtype=interop.string()),
            ArrowFieldSpec(name="path", dtype=interop.string()),
            ArrowFieldSpec(name="qname_id", dtype=interop.string()),
            ArrowFieldSpec(name="qname", dtype=interop.string()),
            ArrowFieldSpec(name="symbol", dtype=interop.string()),
        ),
        constraints=TableSpecConstraints(
            required_non_null=("delta_kind", "file_id", "qname_id", "qname"),
            key_fields=("delta_kind", "file_id", "qname_id"),
        ),
    ),
    DatasetRow(
        name="inc_impacted_callers_v1",
        version=SCHEMA_VERSION,
        fields=(
            ArrowFieldSpec(name="file_id", dtype=interop.string()),
            ArrowFieldSpec(name="path", dtype=interop.string()),
            ArrowFieldSpec(name="reason_kind", dtype=interop.string()),
            ArrowFieldSpec(name="reason_ref", dtype=interop.string()),
        ),
        constraints=TableSpecConstraints(
            required_non_null=("file_id", "path", "reason_kind"),
            key_fields=("file_id", "reason_kind", "reason_ref"),
        ),
    ),
    DatasetRow(
        name="inc_impacted_importers_v1",
        version=SCHEMA_VERSION,
        fields=(
            ArrowFieldSpec(name="file_id", dtype=interop.string()),
            ArrowFieldSpec(name="path", dtype=interop.string()),
            ArrowFieldSpec(name="reason_kind", dtype=interop.string()),
            ArrowFieldSpec(name="reason_ref", dtype=interop.string()),
        ),
        constraints=TableSpecConstraints(
            required_non_null=("file_id", "path", "reason_kind"),
            key_fields=("file_id", "reason_kind", "reason_ref"),
        ),
    ),
    DatasetRow(
        name="inc_impacted_files_v2",
        version=SCHEMA_VERSION,
        fields=(
            ArrowFieldSpec(name="file_id", dtype=interop.string()),
            ArrowFieldSpec(name="path", dtype=interop.string()),
            ArrowFieldSpec(name="reason_kind", dtype=interop.string()),
            ArrowFieldSpec(name="reason_ref", dtype=interop.string()),
        ),
        constraints=TableSpecConstraints(
            required_non_null=("file_id", "path", "reason_kind"),
            key_fields=("file_id", "reason_kind", "reason_ref"),
        ),
    ),
    DatasetRow(
        name="inc_output_fingerprint_changes_v1",
        version=SCHEMA_VERSION,
        fields=(
            ArrowFieldSpec(name="dataset_name", dtype=interop.string()),
            ArrowFieldSpec(name="change_kind", dtype=interop.string()),
            ArrowFieldSpec(name="prev_fingerprint", dtype=interop.string()),
            ArrowFieldSpec(name="cur_fingerprint", dtype=interop.string()),
        ),
        constraints=TableSpecConstraints(
            required_non_null=("dataset_name", "change_kind"),
            key_fields=("dataset_name",),
        ),
    ),
    DatasetRow(
        name="runtime_profile_snapshots_v1",
        version=SCHEMA_VERSION,
        fields=(
            ArrowFieldSpec(name="profile_name", dtype=interop.string()),
            ArrowFieldSpec(name="profile_hash", dtype=interop.string()),
            ArrowFieldSpec(name="snapshot_ipc", dtype=interop.binary()),
        ),
        constraints=TableSpecConstraints(
            required_non_null=("profile_name", "profile_hash", "snapshot_ipc"),
            key_fields=("profile_hash",),
        ),
    ),
    DatasetRow(
        name="function_registry_snapshots_v1",
        version=SCHEMA_VERSION,
        fields=(
            ArrowFieldSpec(name="registry_hash", dtype=interop.string()),
            ArrowFieldSpec(name="snapshot_ipc", dtype=interop.binary()),
        ),
        constraints=TableSpecConstraints(
            required_non_null=("registry_hash", "snapshot_ipc"),
            key_fields=("registry_hash",),
        ),
    ),
    DatasetRow(
        name="datafusion_plan_artifacts_v8",
        version=SCHEMA_VERSION,
        fields=(
            ArrowFieldSpec(name="event_time_unix_ms", dtype=interop.int64()),
            ArrowFieldSpec(name="profile_name", dtype=interop.string()),
            ArrowFieldSpec(name="event_kind", dtype=interop.string()),
            ArrowFieldSpec(name="view_name", dtype=interop.string()),
            ArrowFieldSpec(name="plan_fingerprint", dtype=interop.string()),
            ArrowFieldSpec(name="plan_identity_hash", dtype=interop.string()),
            ArrowFieldSpec(name="udf_snapshot_hash", dtype=interop.string()),
            ArrowFieldSpec(name="function_registry_hash", dtype=interop.string()),
            ArrowFieldSpec(name="required_udfs", dtype=interop.list_(interop.string())),
            ArrowFieldSpec(name="required_rewrite_tags", dtype=interop.list_(interop.string())),
            ArrowFieldSpec(name="domain_planner_names", dtype=interop.list_(interop.string())),
            ArrowFieldSpec(name="delta_inputs_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="df_settings", dtype=pa.map_(pa.string(), pa.string())),
            ArrowFieldSpec(name="planning_env_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="planning_env_hash", dtype=interop.string()),
            ArrowFieldSpec(name="rulepack_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="rulepack_hash", dtype=interop.string()),
            ArrowFieldSpec(name="information_schema_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="information_schema_hash", dtype=interop.string()),
            ArrowFieldSpec(name="substrait_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="logical_plan_proto_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="optimized_plan_proto_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="execution_plan_proto_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="explain_tree_rows_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="explain_verbose_rows_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="explain_analyze_duration_ms", dtype=pa.float64()),
            ArrowFieldSpec(name="explain_analyze_output_rows", dtype=interop.int64()),
            ArrowFieldSpec(name="substrait_validation_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="lineage_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="scan_units_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="scan_keys", dtype=interop.list_(interop.string())),
            ArrowFieldSpec(name="plan_details_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="function_registry_snapshot_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="udf_snapshot_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="udf_planner_snapshot_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="udf_compatibility_ok", dtype=interop.bool_()),
            ArrowFieldSpec(name="udf_compatibility_detail_msgpack", dtype=pa.binary()),
            ArrowFieldSpec(name="execution_duration_ms", dtype=pa.float64()),
            ArrowFieldSpec(name="execution_status", dtype=interop.string()),
            ArrowFieldSpec(name="execution_error", dtype=interop.string()),
        ),
        constraints=TableSpecConstraints(
            required_non_null=(
                "event_time_unix_ms",
                "event_kind",
                "view_name",
                "plan_fingerprint",
                "plan_identity_hash",
                "udf_snapshot_hash",
                "function_registry_hash",
                "required_udfs",
                "required_rewrite_tags",
                "domain_planner_names",
                "delta_inputs_msgpack",
                "df_settings",
                "planning_env_msgpack",
                "planning_env_hash",
                "information_schema_msgpack",
                "information_schema_hash",
                "lineage_msgpack",
                "scan_units_msgpack",
                "scan_keys",
                "plan_details_msgpack",
                "function_registry_snapshot_msgpack",
                "udf_snapshot_msgpack",
                "udf_compatibility_ok",
                "udf_compatibility_detail_msgpack",
            ),
            key_fields=("view_name", "plan_identity_hash", "event_time_unix_ms"),
        ),
    ),
    DatasetRow(
        name="datafusion_hamilton_events_v2",
        version=SCHEMA_VERSION,
        fields=(
            ArrowFieldSpec(name="event_time_unix_ms", dtype=interop.int64()),
            ArrowFieldSpec(name="profile_name", dtype=interop.string()),
            ArrowFieldSpec(name="run_id", dtype=interop.string()),
            ArrowFieldSpec(name="event_name", dtype=interop.string()),
            ArrowFieldSpec(name="plan_signature", dtype=interop.string()),
            ArrowFieldSpec(name="reduced_plan_signature", dtype=interop.string()),
            ArrowFieldSpec(name="event_payload_msgpack", dtype=interop.binary()),
            ArrowFieldSpec(name="event_payload_hash", dtype=interop.string()),
        ),
        constraints=TableSpecConstraints(
            required_non_null=(
                "event_time_unix_ms",
                "run_id",
                "event_name",
                "plan_signature",
                "reduced_plan_signature",
                "event_payload_msgpack",
                "event_payload_hash",
            ),
            key_fields=("run_id", "event_name", "event_payload_hash", "event_time_unix_ms"),
        ),
    ),
)


__all__ = ["DATASET_ROWS", "SCHEMA_VERSION", "ContractRow", "DatasetRow"]
