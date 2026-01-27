"""Dataset row specifications for incremental artifacts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa

from arrow_utils.core import interop
from schema_spec.specs import ArrowFieldSpec
from schema_spec.system import TableSpecConstraints

if TYPE_CHECKING:
    from schema_spec.system import DedupeSpecSpec, SortKeySpec

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ContractRow:
    """Row configuration for a dataset contract."""

    dedupe: DedupeSpecSpec | None = None
    canonical_sort: tuple[SortKeySpec, ...] = ()
    constraints: tuple[str, ...] = ()
    version: int | None = None


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
        name="datafusion_plan_artifacts_v1",
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
            ArrowFieldSpec(name="required_udfs_json", dtype=interop.string()),
            ArrowFieldSpec(name="required_rewrite_tags_json", dtype=interop.string()),
            ArrowFieldSpec(name="domain_planner_names_json", dtype=interop.string()),
            ArrowFieldSpec(name="delta_inputs_json", dtype=interop.string()),
            ArrowFieldSpec(name="df_settings_json", dtype=interop.string()),
            ArrowFieldSpec(name="substrait_b64", dtype=interop.string()),
            ArrowFieldSpec(name="logical_plan_display", dtype=interop.string()),
            ArrowFieldSpec(name="optimized_plan_display", dtype=interop.string()),
            ArrowFieldSpec(name="optimized_plan_pgjson", dtype=interop.string()),
            ArrowFieldSpec(name="optimized_plan_graphviz", dtype=interop.string()),
            ArrowFieldSpec(name="execution_plan_display", dtype=interop.string()),
            ArrowFieldSpec(name="lineage_json", dtype=interop.string()),
            ArrowFieldSpec(name="scan_units_json", dtype=interop.string()),
            ArrowFieldSpec(name="scan_keys_json", dtype=interop.string()),
            ArrowFieldSpec(name="plan_details_json", dtype=interop.string()),
            ArrowFieldSpec(name="function_registry_snapshot_json", dtype=interop.string()),
            ArrowFieldSpec(name="udf_snapshot_json", dtype=interop.string()),
            ArrowFieldSpec(name="udf_compatibility_ok", dtype=interop.bool_()),
            ArrowFieldSpec(name="udf_compatibility_detail_json", dtype=interop.string()),
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
            ),
            key_fields=("view_name", "plan_identity_hash", "event_time_unix_ms"),
        ),
    ),
    DatasetRow(
        name="datafusion_hamilton_events_v1",
        version=SCHEMA_VERSION,
        fields=(
            ArrowFieldSpec(name="event_time_unix_ms", dtype=interop.int64()),
            ArrowFieldSpec(name="profile_name", dtype=interop.string()),
            ArrowFieldSpec(name="run_id", dtype=interop.string()),
            ArrowFieldSpec(name="event_name", dtype=interop.string()),
            ArrowFieldSpec(name="plan_signature", dtype=interop.string()),
            ArrowFieldSpec(name="reduced_plan_signature", dtype=interop.string()),
            ArrowFieldSpec(name="event_payload_json", dtype=interop.string()),
            ArrowFieldSpec(name="event_payload_hash", dtype=interop.string()),
        ),
        constraints=TableSpecConstraints(
            required_non_null=(
                "event_time_unix_ms",
                "run_id",
                "event_name",
                "plan_signature",
                "reduced_plan_signature",
                "event_payload_json",
                "event_payload_hash",
            ),
            key_fields=("run_id", "event_name", "event_payload_hash", "event_time_unix_ms"),
        ),
    ),
)


__all__ = ["DATASET_ROWS", "SCHEMA_VERSION", "ContractRow", "DatasetRow"]
