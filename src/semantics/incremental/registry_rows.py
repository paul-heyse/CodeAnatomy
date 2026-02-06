"""Dataset row specifications for incremental artifacts."""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa

from datafusion_engine.arrow import interop
from schema_spec.arrow_types import arrow_type_from_pyarrow
from schema_spec.field_spec import FieldSpec
from schema_spec.system import ContractRow, TableSpecConstraints

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class DatasetRow:
    """Row specification for incremental datasets."""

    name: str
    version: int
    fields: tuple[FieldSpec, ...]
    constraints: TableSpecConstraints | None = None
    contract: ContractRow | None = None
    metadata_extra: dict[bytes, bytes] = field(default_factory=dict)


DATASET_ROWS: tuple[DatasetRow, ...] = (
    DatasetRow(
        name="py_module_index_v1",
        version=SCHEMA_VERSION,
        fields=(
            FieldSpec(name="file_id", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="path", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="module_fqn", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="is_init", dtype=arrow_type_from_pyarrow(interop.bool_())),
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
            FieldSpec(name="importer_file_id", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="importer_path", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="imported_module_fqn", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="imported_name", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="is_star", dtype=arrow_type_from_pyarrow(interop.bool_())),
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
        name="dim_exported_defs",
        version=SCHEMA_VERSION,
        fields=(
            FieldSpec(name="file_id", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="path", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="def_id", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="def_kind_norm", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="name", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="qname_id", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="qname", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="qname_source", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="symbol", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="symbol_roles", dtype=arrow_type_from_pyarrow(interop.int32())),
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
            FieldSpec(name="delta_kind", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="file_id", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="path", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="qname_id", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="qname", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="symbol", dtype=arrow_type_from_pyarrow(interop.string())),
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
            FieldSpec(name="file_id", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="path", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="reason_kind", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="reason_ref", dtype=arrow_type_from_pyarrow(interop.string())),
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
            FieldSpec(name="file_id", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="path", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="reason_kind", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="reason_ref", dtype=arrow_type_from_pyarrow(interop.string())),
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
            FieldSpec(name="file_id", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="path", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="reason_kind", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="reason_ref", dtype=arrow_type_from_pyarrow(interop.string())),
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
            FieldSpec(name="dataset_name", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="change_kind", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="prev_fingerprint", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="cur_fingerprint", dtype=arrow_type_from_pyarrow(interop.string())),
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
            FieldSpec(name="profile_name", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="profile_hash", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="snapshot_ipc", dtype=arrow_type_from_pyarrow(interop.binary())),
        ),
        constraints=TableSpecConstraints(
            required_non_null=("profile_name", "profile_hash", "snapshot_ipc"),
            key_fields=("profile_hash",),
        ),
    ),
    DatasetRow(
        name="datafusion_plan_artifacts_v9",
        version=SCHEMA_VERSION,
        fields=(
            FieldSpec(name="event_time_unix_ms", dtype=arrow_type_from_pyarrow(interop.int64())),
            FieldSpec(name="profile_name", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="event_kind", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="view_name", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="plan_fingerprint", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="plan_identity_hash", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="udf_snapshot_hash", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(
                name="function_registry_hash", dtype=arrow_type_from_pyarrow(interop.string())
            ),
            FieldSpec(
                name="required_udfs", dtype=arrow_type_from_pyarrow(interop.list_(interop.string()))
            ),
            FieldSpec(
                name="required_rewrite_tags",
                dtype=arrow_type_from_pyarrow(interop.list_(interop.string())),
            ),
            FieldSpec(
                name="domain_planner_names",
                dtype=arrow_type_from_pyarrow(interop.list_(interop.string())),
            ),
            FieldSpec(name="delta_inputs_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())),
            FieldSpec(
                name="df_settings", dtype=arrow_type_from_pyarrow(pa.map_(pa.string(), pa.string()))
            ),
            FieldSpec(name="planning_env_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())),
            FieldSpec(name="planning_env_hash", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="rulepack_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())),
            FieldSpec(name="rulepack_hash", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(
                name="information_schema_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())
            ),
            FieldSpec(
                name="information_schema_hash", dtype=arrow_type_from_pyarrow(interop.string())
            ),
            FieldSpec(name="substrait_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())),
            FieldSpec(
                name="logical_plan_proto_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())
            ),
            FieldSpec(
                name="optimized_plan_proto_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())
            ),
            FieldSpec(
                name="execution_plan_proto_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())
            ),
            FieldSpec(name="explain_tree_rows_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())),
            FieldSpec(
                name="explain_verbose_rows_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())
            ),
            FieldSpec(
                name="explain_analyze_duration_ms", dtype=arrow_type_from_pyarrow(pa.float64())
            ),
            FieldSpec(
                name="explain_analyze_output_rows", dtype=arrow_type_from_pyarrow(interop.int64())
            ),
            FieldSpec(
                name="substrait_validation_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())
            ),
            FieldSpec(name="lineage_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())),
            FieldSpec(name="scan_units_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())),
            FieldSpec(
                name="scan_keys", dtype=arrow_type_from_pyarrow(interop.list_(interop.string()))
            ),
            FieldSpec(name="plan_details_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())),
            FieldSpec(name="udf_snapshot_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())),
            FieldSpec(
                name="udf_planner_snapshot_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())
            ),
            FieldSpec(name="udf_compatibility_ok", dtype=arrow_type_from_pyarrow(interop.bool_())),
            FieldSpec(
                name="udf_compatibility_detail_msgpack", dtype=arrow_type_from_pyarrow(pa.binary())
            ),
            FieldSpec(name="execution_duration_ms", dtype=arrow_type_from_pyarrow(pa.float64())),
            FieldSpec(name="execution_status", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="execution_error", dtype=arrow_type_from_pyarrow(interop.string())),
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
                "substrait_msgpack",
                "lineage_msgpack",
                "scan_units_msgpack",
                "scan_keys",
                "plan_details_msgpack",
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
            FieldSpec(name="event_time_unix_ms", dtype=arrow_type_from_pyarrow(interop.int64())),
            FieldSpec(name="profile_name", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="run_id", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="event_name", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(name="plan_signature", dtype=arrow_type_from_pyarrow(interop.string())),
            FieldSpec(
                name="reduced_plan_signature", dtype=arrow_type_from_pyarrow(interop.string())
            ),
            FieldSpec(
                name="event_payload_msgpack", dtype=arrow_type_from_pyarrow(interop.binary())
            ),
            FieldSpec(name="event_payload_hash", dtype=arrow_type_from_pyarrow(interop.string())),
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
