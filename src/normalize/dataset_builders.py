"""Programmatic builders for normalize dataset specs and schemas."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

import pyarrow as pa

from arrow_utils.core.ordering import OrderingLevel
from datafusion_engine.arrow_interop import SchemaLike
from datafusion_engine.arrow_schema.metadata import (
    SchemaMetadataSpec,
    merge_metadata_specs,
    ordering_metadata_spec,
)
from datafusion_engine.arrow_schema.semantic_types import (
    SPAN_STORAGE,
    edge_id_metadata,
    edge_id_type,
    span_id_metadata,
    span_id_type,
)
from datafusion_engine.delta_protocol import DeltaFeatureGate
from datafusion_engine.query_spec import ProjectionSpec, QuerySpec
from normalize.dataset_bundles import bundle
from normalize.dataset_rows import DatasetRow
from normalize.dataset_templates import template
from normalize.diagnostic_types import DIAG_DETAILS_TYPE
from schema_spec.contract_row import ContractRow
from schema_spec.registration import DatasetRegistration, register_dataset
from schema_spec.specs import ArrowFieldSpec, TableSchemaSpec, dict_field
from schema_spec.system import (
    ContractSpec,
    DatasetSpec,
    DeltaCdfPolicy,
    DeltaMaintenancePolicy,
    VirtualFieldSpec,
    make_contract_spec,
    make_table_spec,
)
from storage.deltalake.config import DeltaSchemaPolicy, DeltaWritePolicy, ParquetWriterPolicy


@dataclass(frozen=True)
class MetadataContext:
    """Context options for building metadata."""

    stage: str
    ordering_level: OrderingLevel = OrderingLevel.IMPLICIT


def _spec(
    name: str,
    dtype: pa.DataType,
    *,
    nullable: bool = True,
    metadata: Mapping[str, str] | Mapping[bytes, bytes] | None = None,
) -> ArrowFieldSpec:
    resolved_metadata: dict[str, str] = {}
    if metadata is not None:
        for key, value in metadata.items():
            decoded_key = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            decoded_value = value.decode("utf-8") if isinstance(value, bytes) else str(value)
            resolved_metadata[decoded_key] = decoded_value
    return ArrowFieldSpec(
        name=name,
        dtype=dtype,
        nullable=nullable,
        metadata=resolved_metadata,
    )


def _dict(name: str) -> ArrowFieldSpec:
    return dict_field(name)


_FIELD_SPECS: dict[str, ArrowFieldSpec] = {
    "file_id": _spec("file_id", pa.string()),
    "path": _spec("path", pa.string()),
    "line_base": _spec("line_base", pa.int32()),
    "col_unit": _spec("col_unit", pa.string()),
    "end_exclusive": _spec("end_exclusive", pa.bool_()),
    "span_id": _spec("span_id", span_id_type(), metadata=span_id_metadata()),
    "span": _spec("span", SPAN_STORAGE),
    "bstart": _spec("bstart", pa.int64()),
    "bend": _spec("bend", pa.int64()),
    "evidence_family": _spec("evidence_family", pa.string()),
    "source": _spec("source", pa.string()),
    "role": _spec("role", pa.string()),
    "confidence": _spec("confidence", pa.float32()),
    "ambiguity_group_id": _spec("ambiguity_group_id", pa.string()),
    "task_name": _spec("task_name", pa.string()),
    "type_expr_id": _spec("type_expr_id", pa.string()),
    "owner_def_id": _spec("owner_def_id", pa.string()),
    "param_name": _spec("param_name", pa.string()),
    "expr_kind": _dict("expr_kind"),
    "expr_role": _dict("expr_role"),
    "expr_text": _spec("expr_text", pa.string()),
    "type_repr": _spec("type_repr", pa.string()),
    "type_id": _spec("type_id", pa.string()),
    "type_form": _dict("type_form"),
    "origin": _dict("origin"),
    "block_id": _spec("block_id", pa.string()),
    "code_unit_id": _spec("code_unit_id", pa.string()),
    "start_offset": _spec("start_offset", pa.int32()),
    "end_offset": _spec("end_offset", pa.int32()),
    "kind": _dict("kind"),
    "edge_id": _spec("edge_id", edge_id_type(), metadata=edge_id_metadata()),
    "src_block_id": _spec("src_block_id", pa.string()),
    "dst_block_id": _spec("dst_block_id", pa.string()),
    "cond_instr_id": _spec("cond_instr_id", pa.string()),
    "exc_index": _spec("exc_index", pa.int32()),
    "event_id": _spec("event_id", pa.string()),
    "instr_id": _spec("instr_id", pa.string()),
    "symbol": _spec("symbol", pa.string()),
    "opname": _dict("opname"),
    "offset": _spec("offset", pa.int32()),
    "argval_str": _spec("argval_str", pa.string()),
    "argrepr": _spec("argrepr", pa.string()),
    "def_event_id": _spec("def_event_id", pa.string()),
    "use_event_id": _spec("use_event_id", pa.string()),
    "document_id": _spec("document_id", pa.string()),
    "reason": _dict("reason"),
    "diag_id": _spec("diag_id", pa.string()),
    "severity": _dict("severity"),
    "message": _spec("message", pa.string()),
    "diag_source": _dict("diag_source"),
    "code": _spec("code", pa.string()),
    "details": _spec("details", DIAG_DETAILS_TYPE),
}


def field(key: str) -> ArrowFieldSpec:
    """Return the ArrowFieldSpec for a field key.

    Returns
    -------
    ArrowFieldSpec
        Field specification for the key.
    """
    return _FIELD_SPECS[key]


def fields(keys: Sequence[str]) -> list[ArrowFieldSpec]:
    """Return ArrowFieldSpec instances for field keys.

    Returns
    -------
    list[ArrowFieldSpec]
        Field specifications for the keys.
    """
    return [field(key) for key in keys]


def field_name(key: str) -> str:
    """Return the column name for a field key.

    Returns
    -------
    str
        Column name for the key.
    """
    return field(key).name


def _dedupe(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _dedupe_specs(specs: Iterable[ArrowFieldSpec]) -> list[ArrowFieldSpec]:
    seen: set[str] = set()
    out: list[ArrowFieldSpec] = []
    for spec in specs:
        name = spec.name
        if name in seen:
            continue
        seen.add(name)
        out.append(spec)
    return out


def _bundle_field_keys(bundle_names: Sequence[str]) -> list[str]:
    keys: list[str] = []
    for name in bundle_names:
        keys.extend(field_spec.name for field_spec in bundle(name).fields)
    return keys


def _derived_names(row: DatasetRow) -> set[str]:
    return {spec.name for spec in row.derived}


def _base_field_keys(row: DatasetRow) -> list[str]:
    derived = _derived_names(row)
    bundle_keys = _bundle_field_keys(row.bundles)
    row_keys = [field_name(key) for key in row.fields if field_name(key) not in derived]
    return _dedupe((*bundle_keys, *row_keys))


def _input_field_specs(row: DatasetRow) -> list[ArrowFieldSpec]:
    derived = _derived_names(row)
    bundle_fields = [bundle_field for name in row.bundles for bundle_field in bundle(name).fields]
    base_fields = [field(key) for key in row.fields if field_name(key) not in derived]
    extras = fields(row.input_fields)
    return _dedupe_specs((*bundle_fields, *base_fields, *extras))


def _virtual_spec(contract: ContractRow) -> VirtualFieldSpec | None:
    if not contract.virtual_fields and not contract.virtual_field_docs:
        return None
    return VirtualFieldSpec(fields=contract.virtual_fields, docs=contract.virtual_field_docs)


def normalize_metadata_spec(
    dataset_name: str,
    *,
    ctx: MetadataContext,
    extra: Mapping[bytes, bytes] | None = None,
) -> SchemaMetadataSpec:
    """Return schema metadata for normalize datasets.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec for normalize datasets.
    """
    meta = {
        b"normalize_stage": ctx.stage.encode("utf-8"),
        b"normalize_dataset": dataset_name.encode("utf-8"),
    }
    if extra:
        meta.update(extra)
    return SchemaMetadataSpec(schema_metadata=meta)


def build_query_spec(row: DatasetRow) -> QuerySpec:
    """Build the QuerySpec for a dataset row.

    Returns
    -------
    QuerySpec
        DataFusion query specification for the dataset.
    """
    base_cols = _base_field_keys(row)
    derived_map = {spec.name: spec.expr for spec in row.derived}
    if not derived_map:
        return QuerySpec.simple(*base_cols)
    return QuerySpec(projection=ProjectionSpec(base=tuple(base_cols), derived=derived_map))


def build_input_schema(row: DatasetRow) -> SchemaLike:
    """Build the input schema for normalize plan sources.

    Returns
    -------
    SchemaLike
        Input schema for plan sources.
    """
    input_fields = _input_field_specs(row)
    return make_table_spec(
        name=f"{row.name}_input",
        version=row.version,
        bundles=(),
        fields=tuple(input_fields),
    ).to_arrow_schema()


def build_contract_spec(row: DatasetRow, *, table_spec: TableSchemaSpec) -> ContractSpec | None:
    """Build the ContractSpec for a dataset row.

    Returns
    -------
    ContractSpec | None
        Contract specification for the dataset, when configured.
    """
    if row.contract is None:
        return None
    virtual = _virtual_spec(row.contract)
    return make_contract_spec(
        table_spec=table_spec,
        dedupe=row.contract.dedupe,
        canonical_sort=row.contract.canonical_sort,
        version=row.contract.version,
        virtual=virtual,
        validation=row.contract.validation,
    )


def build_metadata_spec(row: DatasetRow) -> SchemaMetadataSpec:
    """Build the metadata spec for a dataset row.

    Returns
    -------
    SchemaMetadataSpec
        Metadata spec for the dataset schema.
    """
    templ = template(row.template) if row.template is not None else None
    stage = templ.stage if templ is not None else "normalize"
    level = templ.ordering_level if templ is not None else OrderingLevel.IMPLICIT
    extra: dict[bytes, bytes] = dict(templ.metadata_extra or {}) if templ is not None else {}
    if templ is not None and templ.determinism_tier is not None:
        extra.setdefault(
            b"determinism_tier",
            templ.determinism_tier.value.encode("utf-8"),
        )
    extra.update(row.metadata_extra)
    metadata = normalize_metadata_spec(row.name, ctx=MetadataContext(stage=stage), extra=extra)
    if not row.join_keys:
        return metadata
    ordering = ordering_metadata_spec(
        level,
        keys=tuple((name, "ascending") for name in row.join_keys),
    )
    return merge_metadata_specs(metadata, ordering)


def build_dataset_spec(row: DatasetRow) -> DatasetSpec:
    """Build the DatasetSpec for a dataset row.

    Returns
    -------
    DatasetSpec
        Dataset spec including query, contract, and metadata registration.
    """
    bundles = tuple(bundle(name) for name in row.bundles)
    table_spec = make_table_spec(
        name=row.name,
        version=row.version,
        bundles=bundles,
        fields=fields(row.fields),
    )
    contract_spec = build_contract_spec(row, table_spec=table_spec)
    metadata_spec = build_metadata_spec(row)
    registration = DatasetRegistration(
        query_spec=build_query_spec(row),
        contract_spec=contract_spec,
        metadata_spec=metadata_spec,
        delta_cdf_policy=_normalize_cdf_policy(row),
        delta_maintenance_policy=_normalize_maintenance_policy(row),
        delta_write_policy=_normalize_write_policy(row),
        delta_schema_policy=_normalize_schema_policy(),
        delta_feature_gate=_normalize_feature_gate(),
    )
    return register_dataset(table_spec=table_spec, registration=registration)


def _normalize_schema_policy() -> DeltaSchemaPolicy:
    return DeltaSchemaPolicy(schema_mode="merge", column_mapping_mode="name")


def _normalize_feature_gate() -> DeltaFeatureGate:
    return DeltaFeatureGate(
        required_writer_features=("change_data_feed", "column_mapping", "v2_checkpoints"),
        required_reader_features=(),
    )


def _normalize_cdf_policy(_row: DatasetRow) -> DeltaCdfPolicy:
    return DeltaCdfPolicy(required=True, allow_out_of_range=False)


def _normalize_maintenance_policy(row: DatasetRow) -> DeltaMaintenancePolicy:
    z_order_cols = tuple(row.join_keys)
    z_order_when = "after_partition_complete" if z_order_cols else "never"
    return DeltaMaintenancePolicy(
        optimize_on_write=True,
        optimize_target_size=256 * 1024 * 1024,
        z_order_cols=z_order_cols,
        z_order_when=z_order_when,
        vacuum_on_write=False,
    )


def _normalize_write_policy(row: DatasetRow) -> DeltaWritePolicy:
    bloom_columns = _bloom_filter_columns(row)
    parquet_policy = None
    if bloom_columns:
        parquet_policy = ParquetWriterPolicy(
            bloom_filter_enabled=bloom_columns,
            bloom_filter_fpp=0.01,
            bloom_filter_ndv=10_000_000,
        )
    stats_columns = _stats_columns_for_row(row, bloom_columns)
    return DeltaWritePolicy(
        target_file_size=128 * 1024 * 1024,
        zorder_by=tuple(row.join_keys),
        stats_policy="explicit",
        stats_columns=stats_columns,
        parquet_writer_policy=parquet_policy,
        enable_features=("change_data_feed", "column_mapping", "v2_checkpoints"),
    )


def _bloom_filter_columns(row: DatasetRow) -> tuple[str, ...]:
    candidates = [
        field_name(key)
        for key in (*row.fields, *row.input_fields)
        if field_name(key).endswith("_id")
    ]
    derived = [spec.name for spec in row.derived if spec.name.endswith("_id")]
    return tuple(dict.fromkeys((*candidates, *derived)))


def _stats_columns_for_row(
    row: DatasetRow,
    bloom_columns: tuple[str, ...],
) -> tuple[str, ...] | None:
    if row.join_keys or bloom_columns:
        return tuple(dict.fromkeys((*row.join_keys, *bloom_columns)))
    return None


__all__ = [
    "MetadataContext",
    "build_contract_spec",
    "build_dataset_spec",
    "build_input_schema",
    "build_metadata_spec",
    "build_query_spec",
    "field",
    "field_name",
    "fields",
    "normalize_metadata_spec",
]
