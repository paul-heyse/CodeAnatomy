"""Dataset row specifications for incremental artifacts."""

from __future__ import annotations

from dataclasses import dataclass, field

from arrowdsl.core import interop
from schema_spec.specs import ArrowFieldSpec
from schema_spec.system import DedupeSpecSpec, SortKeySpec, TableSpecConstraints

SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ContractRow:
    """Row configuration for a dataset contract."""

    dedupe: DedupeSpecSpec | None = None
    canonical_sort: tuple[SortKeySpec, ...] = ()
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
)


__all__ = ["DATASET_ROWS", "SCHEMA_VERSION", "ContractRow", "DatasetRow"]
