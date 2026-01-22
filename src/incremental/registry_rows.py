"""Dataset row specifications for incremental artifacts."""

from __future__ import annotations

from dataclasses import dataclass, field

import pyarrow as pa

from arrowdsl.core import interop
from registry_common.registry_rows import ContractRow
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
        name="sqlglot_policy_snapshots_v1",
        version=SCHEMA_VERSION,
        fields=(
            ArrowFieldSpec(name="policy_hash", dtype=interop.string()),
            ArrowFieldSpec(name="snapshot_ipc", dtype=interop.binary()),
        ),
        constraints=TableSpecConstraints(
            required_non_null=("policy_hash", "snapshot_ipc"),
            key_fields=("policy_hash",),
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
            ArrowFieldSpec(name="plan_hash", dtype=interop.string()),
            ArrowFieldSpec(name="sql", dtype=interop.string()),
            ArrowFieldSpec(name="explain_artifact_path", dtype=interop.string()),
            ArrowFieldSpec(name="explain_artifact_format", dtype=interop.string()),
            ArrowFieldSpec(name="explain_schema_fingerprint", dtype=interop.string()),
            ArrowFieldSpec(name="explain_analyze_artifact_path", dtype=interop.string()),
            ArrowFieldSpec(name="explain_analyze_artifact_format", dtype=interop.string()),
            ArrowFieldSpec(name="explain_analyze_schema_fingerprint", dtype=interop.string()),
            ArrowFieldSpec(name="substrait_b64", dtype=interop.string()),
            ArrowFieldSpec(name="substrait_validation_status", dtype=interop.string()),
            ArrowFieldSpec(name="substrait_validation_stage", dtype=interop.string()),
            ArrowFieldSpec(name="substrait_validation_error", dtype=interop.string()),
            ArrowFieldSpec(name="substrait_validation_match", dtype=interop.bool_()),
            ArrowFieldSpec(name="substrait_validation_datafusion_rows", dtype=interop.int64()),
            ArrowFieldSpec(name="substrait_validation_datafusion_hash", dtype=interop.string()),
            ArrowFieldSpec(name="substrait_validation_substrait_rows", dtype=interop.int64()),
            ArrowFieldSpec(name="substrait_validation_substrait_hash", dtype=interop.string()),
            ArrowFieldSpec(name="sqlglot_ast", dtype=interop.string()),
            ArrowFieldSpec(name="read_dialect", dtype=interop.string()),
            ArrowFieldSpec(name="write_dialect", dtype=interop.string()),
            ArrowFieldSpec(name="canonical_fingerprint", dtype=interop.string()),
            ArrowFieldSpec(name="lineage_tables", dtype=pa.list_(pa.string())),
            ArrowFieldSpec(name="lineage_columns", dtype=pa.list_(pa.string())),
            ArrowFieldSpec(name="lineage_scopes", dtype=pa.list_(pa.string())),
            ArrowFieldSpec(name="unparsed_sql", dtype=interop.string()),
            ArrowFieldSpec(name="unparse_error", dtype=interop.string()),
            ArrowFieldSpec(name="logical_plan", dtype=interop.string()),
            ArrowFieldSpec(name="optimized_plan", dtype=interop.string()),
            ArrowFieldSpec(name="physical_plan", dtype=interop.string()),
            ArrowFieldSpec(name="graphviz", dtype=interop.string()),
            ArrowFieldSpec(name="partition_count", dtype=interop.int64()),
            ArrowFieldSpec(name="join_operators", dtype=pa.list_(pa.string())),
        ),
        constraints=TableSpecConstraints(
            required_non_null=("plan_hash", "sql"),
            key_fields=("plan_hash",),
        ),
    ),
)


__all__ = ["DATASET_ROWS", "SCHEMA_VERSION", "ContractRow", "DatasetRow"]
