"""Build and write run manifest records for observability."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Annotated, TypeVar, cast

import msgspec
import pyarrow as pa
from sqlglot.errors import ParseError

from arrowdsl.core.interop import SchemaLike, TableLike
from arrowdsl.core.metrics import table_summary
from arrowdsl.schema.metadata import ordering_from_schema
from arrowdsl.schema.serialization import dataset_fingerprint, schema_fingerprint
from core_types import JsonDict, JsonValue, PathLike, ensure_path
from obs.repro import ReproInfo
from serde_msgspec import StructBase, to_builtins
from sqlglot_tools.optimizer import ParseSqlOptions, parse_sql, planner_dag_snapshot
from storage.io import (
    delta_commit_metadata,
    delta_history_snapshot,
    delta_protocol_snapshot,
    delta_table_features,
    delta_table_version,
)

if TYPE_CHECKING:
    from arrowdsl.core.scan_telemetry import ScanTelemetry
    from extract.evidence_specs import EvidenceSpec
    from extract.evidence_plan import EvidencePlan
    from ibis_engine.execution import IbisExecutionContext
    from ibis_engine.param_tables import ParamTableArtifact
    from normalize.runner import ResolvedNormalizeRule
    from relspec.compiler import CompiledOutput
    from relspec.model import RelationshipRule
    from relspec.registry import DatasetLocation
    from obs.repro import ReproInfo


T = TypeVar("T")

NonNegInt = Annotated[int, msgspec.Meta(ge=0)]

DIAGNOSTICS_SCHEMA_VERSION = "v1"


class DatasetRecord(StructBase):
    """Record a dataset artifact."""

    name: str
    kind: str  # "input" | "intermediate" | "relationship_output" | "cpg_output"
    path: str | None = None
    format: str | None = None
    delta_version: NonNegInt | None = None
    delta_features: Mapping[str, str] | None = None
    delta_commit_metadata: JsonDict | None = None
    delta_protocol: JsonDict | None = None
    delta_history: JsonDict | None = None
    delta_write_policy: JsonDict | None = None
    delta_schema_policy: JsonDict | None = None
    delta_constraints: list[str] | None = None

    rows: NonNegInt | None = None
    columns: NonNegInt | None = None
    schema_fingerprint: str | None = None
    ddl_fingerprint: str | None = None
    ordering_level: str | None = None
    ordering_keys: list[list[str]] | None = None

    # Optional: include a small schema description (safe for debugging)
    schema: list[JsonDict] | None = None

class DatasetRecordMetadata(StructBase):
    """Metadata for dataset records."""

    path: str | None = None
    data_format: str | None = None
    delta_version: NonNegInt | None = None
    delta_features: Mapping[str, str] | None = None
    delta_commit_metadata: JsonDict | None = None
    delta_protocol: JsonDict | None = None
    delta_history: JsonDict | None = None
    delta_write_policy: JsonDict | None = None
    delta_schema_policy: JsonDict | None = None
    delta_constraints: list[str] | None = None


class RuleRecord(StructBase):
    """Record a rule definition."""

    name: str
    output_dataset: str
    kind: str
    contract_name: str | None
    priority: NonNegInt
    inputs: list[str] = msgspec.field(default_factory=list)
    evidence: JsonDict | None = None
    confidence_policy: JsonDict | None = None
    ambiguity_policy: JsonDict | None = None


class OutputRecord(StructBase):
    """Record a produced output (e.g., relationship outputs, cpg outputs)."""

    name: str
    rows: NonNegInt | None = None
    schema_fingerprint: str | None = None
    ddl_fingerprint: str | None = None
    ordering_level: str | None = None
    ordering_keys: list[list[str]] | None = None
    plan_hash: str | None = None
    profile_hash: str | None = None
    writer_strategy: str | None = None
    input_fingerprints: list[str] | None = None
    dataset_fingerprint: str | None = None


class OutputFingerprintInputs(StructBase):
    """Inputs required to compute output dataset fingerprints."""

    plan_hash: str | None = None
    profile_hash: str | None = None
    writer_strategy: str | None = None
    input_fingerprints: Sequence[str] | None = None


class OutputLineageRecord(StructBase):
    """Record rule lineage for a relationship output."""

    output_dataset: str
    rules: list[str] = msgspec.field(default_factory=list)


class ExtractRecord(StructBase):
    """Record extract output lineage and metadata."""

    name: str
    alias: str
    template: str | None
    evidence_family: str | None
    coordinate_system: str | None
    evidence_rank: NonNegInt | None
    ambiguity_policy: str | None
    required_columns: list[str] = msgspec.field(default_factory=list)
    sources: list[str] = msgspec.field(default_factory=list)
    schema_fingerprint: str | None = None
    error_rows: NonNegInt | None = None


class Manifest(StructBase):
    """Top-level manifest record."""

    manifest_version: NonNegInt
    created_at_unix_s: NonNegInt

    repo_root: str | None
    relspec_mode: str
    work_dir: str | None
    output_dir: str | None

    repro: ReproInfo
    datasets: list[DatasetRecord] = msgspec.field(default_factory=list)
    rules: list[RuleRecord] = msgspec.field(default_factory=list)
    outputs: list[OutputRecord] = msgspec.field(default_factory=list)
    lineage: list[OutputLineageRecord] = msgspec.field(default_factory=list)
    extracts: list[ExtractRecord] = msgspec.field(default_factory=list)
    params: JsonDict = msgspec.field(default_factory=dict)
    notes: JsonDict = msgspec.field(default_factory=dict)

    def to_dict(self) -> JsonDict:
        """Convert the manifest to a plain dictionary.

        Returns
        -------
        JsonDict
            Manifest data as a dictionary.
        """
        return cast("JsonDict", to_builtins(self))


class ManifestContext(StructBase):
    """Core context fields for manifest construction."""

    repo_root: str | None
    relspec_mode: str
    work_dir: str | None
    output_dir: str | None


class ManifestData(StructBase):
    """Optional inputs used to populate manifest records."""

    relspec_input_tables: Mapping[str, TableLike] | None = None
    relspec_input_locations: Mapping[str, DatasetLocation] | None = None
    relationship_outputs: Mapping[str, TableLike] | None = None
    compiled_relationship_outputs: Mapping[str, CompiledOutput] | None = None
    cpg_nodes: TableLike | None = None
    cpg_edges: TableLike | None = None
    cpg_props: TableLike | None = None
    cpg_props_json: TableLike | None = None
    extract_evidence_plan: EvidencePlan | None = None
    extract_error_counts: Mapping[str, int] | None = None
    relationship_rules: Sequence[RelationshipRule] | None = None
    normalize_rules: Sequence[ResolvedNormalizeRule] | None = None
    produced_relationship_output_names: Sequence[str] | None = None
    relationship_output_lineage: Mapping[str, Sequence[str]] | None = None
    normalize_output_lineage: Mapping[str, Sequence[str]] | None = None
    relspec_scan_telemetry: Mapping[str, Mapping[str, ScanTelemetry]] | None = None
    datafusion_settings: Sequence[Mapping[str, str]] | None = None
    datafusion_settings_hash: str | None = None
    datafusion_feature_gates: Mapping[str, str] | None = None
    datafusion_metrics: Mapping[str, object] | None = None
    datafusion_traces: Mapping[str, object] | None = None
    datafusion_function_catalog: Sequence[Mapping[str, object]] | None = None
    datafusion_function_catalog_hash: str | None = None
    datafusion_schema_map: Mapping[str, object] | None = None
    datafusion_schema_map_hash: str | None = None
    runtime_profile_snapshot: Mapping[str, object] | None = None
    runtime_profile_hash: str | None = None
    sqlglot_policy_snapshot: Mapping[str, object] | None = None
    function_registry_snapshot: Mapping[str, object] | None = None
    function_registry_hash: str | None = None
    sqlglot_ast_payloads: Sequence[Mapping[str, object]] | None = None
    dataset_registry_snapshot: Sequence[Mapping[str, object]] | None = None
    param_table_artifacts: Mapping[str, ParamTableArtifact] | None = None
    param_scalar_signature: str | None = None
    materialization_reports: JsonDict | None = None
    runtime_profile_name: str | None = None
    determinism_tier: str | None = None
    writer_strategy: str | None = None
    notes: JsonDict | None = None


def _dataset_record_from_table(
    *,
    name: str,
    kind: str,
    table: TableLike | None,
    metadata: DatasetRecordMetadata | None = None,
) -> DatasetRecord:
    from schema_spec.system import dataset_table_ddl_fingerprint

    metadata = metadata or DatasetRecordMetadata()
    if table is None:
        return DatasetRecord(
            name=name,
            kind=kind,
            path=metadata.path,
            format=metadata.data_format,
            delta_version=metadata.delta_version,
            delta_features=metadata.delta_features,
            delta_commit_metadata=metadata.delta_commit_metadata,
            delta_protocol=metadata.delta_protocol,
            delta_history=metadata.delta_history,
            delta_write_policy=metadata.delta_write_policy,
            delta_schema_policy=metadata.delta_schema_policy,
            delta_constraints=metadata.delta_constraints,
        )

    summ = table_summary(table)
    ddl_fp = dataset_table_ddl_fingerprint(name)
    ordering_level, ordering_keys = _ordering_payload(table.schema)
    return DatasetRecord(
        name=name,
        kind=kind,
        path=metadata.path,
        format=metadata.data_format,
        delta_version=metadata.delta_version,
        delta_features=metadata.delta_features,
        delta_commit_metadata=metadata.delta_commit_metadata,
        delta_protocol=metadata.delta_protocol,
        delta_history=metadata.delta_history,
        delta_write_policy=metadata.delta_write_policy,
        delta_schema_policy=metadata.delta_schema_policy,
        delta_constraints=metadata.delta_constraints,
        rows=summ["rows"],
        columns=summ["columns"],
        schema_fingerprint=summ["schema_fingerprint"],
        ddl_fingerprint=ddl_fp,
        ordering_level=ordering_level,
        ordering_keys=ordering_keys,
        schema=summ["schema"],
    )


def _output_record_from_table(
    *,
    name: str,
    table: TableLike | None,
    fingerprints: OutputFingerprintInputs | None = None,
) -> OutputRecord:
    from schema_spec.system import dataset_table_ddl_fingerprint

    schema_fp = schema_fingerprint(table.schema) if table is not None else None
    ddl_fp = dataset_table_ddl_fingerprint(name) if table is not None else None
    ordering_level, ordering_keys = (
        _ordering_payload(table.schema) if table is not None else (None, None)
    )
    normalized_inputs = (
        list(fingerprints.input_fingerprints)
        if fingerprints is not None and fingerprints.input_fingerprints is not None
        else None
    )
    output_fingerprint = None
    if (
        fingerprints is not None
        and fingerprints.plan_hash is not None
        and schema_fp is not None
        and fingerprints.profile_hash is not None
        and fingerprints.writer_strategy is not None
    ):
        output_fingerprint = dataset_fingerprint(
            plan_hash=fingerprints.plan_hash,
            schema_fingerprint=schema_fp,
            profile_hash=fingerprints.profile_hash,
            writer_strategy=fingerprints.writer_strategy,
            input_fingerprints=fingerprints.input_fingerprints or (),
        )
    return OutputRecord(
        name=name,
        rows=int(table.num_rows) if table is not None else None,
        schema_fingerprint=schema_fp,
        ddl_fingerprint=ddl_fp,
        ordering_level=ordering_level,
        ordering_keys=ordering_keys,
        plan_hash=fingerprints.plan_hash if fingerprints is not None else None,
        profile_hash=fingerprints.profile_hash if fingerprints is not None else None,
        writer_strategy=fingerprints.writer_strategy if fingerprints is not None else None,
        input_fingerprints=normalized_inputs,
        dataset_fingerprint=output_fingerprint,
    )


def _ordering_payload(schema: SchemaLike) -> tuple[str | None, list[list[str]] | None]:
    ordering = ordering_from_schema(schema)
    if not ordering.keys:
        return ordering.level.value, None
    return ordering.level.value, [list(key) for key in ordering.keys]


def _input_fingerprint_values(
    input_names: Sequence[str],
    *,
    known_fingerprints: Mapping[str, str],
    schema_fingerprints: Mapping[str, str],
) -> list[str]:
    values: list[str] = []
    for name in input_names:
        if name in known_fingerprints:
            values.append(known_fingerprints[name])
            continue
        schema_fp = schema_fingerprints.get(name)
        if schema_fp is not None:
            values.append(schema_fp)
    return values


def _delta_metadata_from_path(
    path: str | None,
    *,
    storage_options: Mapping[str, str] | None = None,
) -> tuple[
    int | None,
    Mapping[str, str] | None,
    JsonDict | None,
    JsonDict | None,
    JsonDict | None,
]:
    if path is None:
        return None, None, None, None, None
    version = delta_table_version(path, storage_options=storage_options)
    if version is None:
        return None, None, None, None, None
    features = delta_table_features(path, storage_options=storage_options)
    commit = delta_commit_metadata(path, storage_options=storage_options)
    protocol = delta_protocol_snapshot(path, storage_options=storage_options)
    history = delta_history_snapshot(path, storage_options=storage_options)
    commit_payload = cast("JsonDict | None", commit)
    protocol_payload = cast("JsonDict | None", protocol)
    history_payload = cast("JsonDict | None", history)
    return version, features, commit_payload, protocol_payload, history_payload


def _materialization_artifacts(data: ManifestData) -> Mapping[str, object]:
    if data.materialization_reports is None:
        return {}
    artifacts = data.materialization_reports.get("artifacts")
    if not isinstance(artifacts, Mapping):
        return {}
    return artifacts


def _materialized_delta_path(payload: Mapping[str, object]) -> str | None:
    paths = payload.get("paths")
    if not isinstance(paths, Mapping):
        return None
    data_path = paths.get("data")
    return data_path if isinstance(data_path, str) else None


def _materialized_delta_version(payload: Mapping[str, object]) -> int | None:
    delta_versions = payload.get("delta_versions")
    if not isinstance(delta_versions, Mapping):
        return None
    version_value = delta_versions.get("data")
    return version_value if isinstance(version_value, int) else None


class DeltaMaterializationMetadata(StructBase):
    """Delta metadata captured from materialization reports."""

    path: str | None
    version: NonNegInt | None
    features: Mapping[str, str] | None
    commit_metadata: JsonDict | None
    protocol: JsonDict | None
    history: JsonDict | None
    write_policy: JsonDict | None
    schema_policy: JsonDict | None
    constraints: list[str] | None


def _materialized_delta_metadata(
    data: ManifestData,
    *,
    key: str,
) -> DeltaMaterializationMetadata | None:
    payload = _materialization_artifacts(data).get(key)
    if not isinstance(payload, Mapping):
        return None
    path = _materialized_delta_path(payload)
    delta_version = _materialized_delta_version(payload)
    version, features, commit, protocol, history = _delta_metadata_from_path(path)
    resolved_version = delta_version if delta_version is not None else version
    write_policy = payload.get("delta_write_policy")
    schema_policy = payload.get("delta_schema_policy")
    constraints = payload.get("delta_constraints")
    return DeltaMaterializationMetadata(
        path=path,
        version=resolved_version,
        features=features,
        commit_metadata=commit,
        protocol=protocol,
        history=history,
        write_policy=cast("JsonDict | None", write_policy),
        schema_policy=cast("JsonDict | None", schema_policy),
        constraints=list(constraints) if isinstance(constraints, Sequence) else None,
    )


def _location_metadata(loc: DatasetLocation | None) -> DatasetRecordMetadata:
    path = str(loc.path) if loc is not None else None
    fmt = loc.format if loc is not None else None
    delta_version = loc.delta_version if loc is not None else None
    delta_features: Mapping[str, str] | None = None
    delta_commit: JsonDict | None = None
    delta_protocol: JsonDict | None = None
    delta_history: JsonDict | None = None
    if loc is not None and fmt == "delta":
        storage_options = dict(loc.storage_options) if loc.storage_options is not None else None
        version, features, commit, protocol, history = _delta_metadata_from_path(
            path, storage_options=storage_options
        )
        if delta_version is None:
            delta_version = version
        delta_features = features
        delta_commit = commit
        delta_protocol = protocol
        delta_history = history
    return DatasetRecordMetadata(
        path=path,
        data_format=fmt,
        delta_version=delta_version,
        delta_features=delta_features,
        delta_commit_metadata=delta_commit,
        delta_protocol=delta_protocol,
        delta_history=delta_history,
    )


def _collect_relationship_inputs(data: ManifestData) -> list[DatasetRecord]:
    if not data.relspec_input_tables:
        return []

    records: list[DatasetRecord] = []
    for name in sorted(data.relspec_input_tables):
        table = data.relspec_input_tables[name]
        loc = data.relspec_input_locations.get(name) if data.relspec_input_locations else None
        metadata = _location_metadata(loc)
        records.append(
            _dataset_record_from_table(
                name=name,
                kind="relationship_input",
                table=table,
                metadata=metadata,
            )
        )
    return records


def _collect_relationship_outputs(
    data: ManifestData,
) -> tuple[list[DatasetRecord], list[OutputRecord]]:
    if not data.relationship_outputs:
        return [], []

    input_schema_fps: dict[str, str] = {}
    if data.relspec_input_tables:
        for name, table in data.relspec_input_tables.items():
            input_schema_fps[name] = schema_fingerprint(table.schema)

    compiled_outputs = data.compiled_relationship_outputs or {}
    profile_hash = data.runtime_profile_hash or data.datafusion_settings_hash
    writer_strategy = data.writer_strategy
    output_fingerprints: dict[str, str] = {}

    datasets: list[DatasetRecord] = []
    outputs: list[OutputRecord] = []
    for name in sorted(data.relationship_outputs):
        table = data.relationship_outputs[name]
        datasets.append(
            _dataset_record_from_table(name=name, kind="relationship_output", table=table)
        )
        compiled = compiled_outputs.get(name)
        plan_hash = compiled.plan_hash if compiled is not None else None
        input_fps = None
        if compiled is not None and compiled.input_datasets:
            input_fps = _input_fingerprint_values(
                compiled.input_datasets,
                known_fingerprints=output_fingerprints,
                schema_fingerprints=input_schema_fps,
            )
        fingerprints = OutputFingerprintInputs(
            plan_hash=plan_hash,
            profile_hash=profile_hash,
            writer_strategy=writer_strategy,
            input_fingerprints=input_fps,
        )
        record = _output_record_from_table(
            name=name,
            table=table,
            fingerprints=fingerprints,
        )
        outputs.append(record)
        if record.dataset_fingerprint is not None:
            output_fingerprints[name] = record.dataset_fingerprint
        elif record.schema_fingerprint is not None:
            output_fingerprints[name] = record.schema_fingerprint
    return datasets, outputs


def relationship_output_fingerprints(data: ManifestData) -> dict[str, str]:
    """Return dataset fingerprints for relationship outputs.

    Returns
    -------
    dict[str, str]
        Mapping of output dataset name to dataset fingerprint.
    """
    _, outputs = _collect_relationship_outputs(data)
    return {
        record.name: record.dataset_fingerprint
        for record in outputs
        if record.dataset_fingerprint is not None
    }


def _collect_cpg_outputs(data: ManifestData) -> tuple[list[DatasetRecord], list[OutputRecord]]:
    datasets: list[DatasetRecord] = []
    outputs: list[OutputRecord] = []
    profile_hash = data.runtime_profile_hash or data.datafusion_settings_hash
    writer_strategy = data.writer_strategy

    if data.cpg_nodes is not None:
        delta_meta = _materialized_delta_metadata(data, key="cpg_nodes_delta")
        metadata = DatasetRecordMetadata()
        if delta_meta is not None:
            metadata = DatasetRecordMetadata(
                path=delta_meta.path,
                data_format="delta" if delta_meta.path is not None else None,
                delta_version=delta_meta.version,
                delta_features=delta_meta.features,
                delta_commit_metadata=delta_meta.commit_metadata,
                delta_protocol=delta_meta.protocol,
                delta_history=delta_meta.history,
                delta_write_policy=delta_meta.write_policy,
                delta_schema_policy=delta_meta.schema_policy,
                delta_constraints=delta_meta.constraints,
            )
        datasets.append(
            _dataset_record_from_table(
                name="cpg_nodes",
                kind="cpg_output",
                table=data.cpg_nodes,
                metadata=metadata,
            )
        )
        outputs.append(
            _output_record_from_table(
                name="cpg_nodes",
                table=data.cpg_nodes,
                fingerprints=OutputFingerprintInputs(
                    profile_hash=profile_hash,
                    writer_strategy=writer_strategy,
                ),
            )
        )
    if data.cpg_edges is not None:
        delta_meta = _materialized_delta_metadata(data, key="cpg_edges_delta")
        metadata = DatasetRecordMetadata()
        if delta_meta is not None:
            metadata = DatasetRecordMetadata(
                path=delta_meta.path,
                data_format="delta" if delta_meta.path is not None else None,
                delta_version=delta_meta.version,
                delta_features=delta_meta.features,
                delta_commit_metadata=delta_meta.commit_metadata,
                delta_protocol=delta_meta.protocol,
                delta_history=delta_meta.history,
                delta_write_policy=delta_meta.write_policy,
                delta_schema_policy=delta_meta.schema_policy,
                delta_constraints=delta_meta.constraints,
            )
        datasets.append(
            _dataset_record_from_table(
                name="cpg_edges",
                kind="cpg_output",
                table=data.cpg_edges,
                metadata=metadata,
            )
        )
        outputs.append(
            _output_record_from_table(
                name="cpg_edges",
                table=data.cpg_edges,
                fingerprints=OutputFingerprintInputs(
                    profile_hash=profile_hash,
                    writer_strategy=writer_strategy,
                ),
            )
        )
    if data.cpg_props is not None:
        delta_meta = _materialized_delta_metadata(data, key="cpg_props_delta")
        metadata = DatasetRecordMetadata()
        if delta_meta is not None:
            metadata = DatasetRecordMetadata(
                path=delta_meta.path,
                data_format="delta" if delta_meta.path is not None else None,
                delta_version=delta_meta.version,
                delta_features=delta_meta.features,
                delta_commit_metadata=delta_meta.commit_metadata,
                delta_protocol=delta_meta.protocol,
                delta_history=delta_meta.history,
                delta_write_policy=delta_meta.write_policy,
                delta_schema_policy=delta_meta.schema_policy,
                delta_constraints=delta_meta.constraints,
            )
        datasets.append(
            _dataset_record_from_table(
                name="cpg_props",
                kind="cpg_output",
                table=data.cpg_props,
                metadata=metadata,
            )
        )
        outputs.append(
            _output_record_from_table(
                name="cpg_props",
                table=data.cpg_props,
                fingerprints=OutputFingerprintInputs(
                    profile_hash=profile_hash,
                    writer_strategy=writer_strategy,
                ),
            )
        )
    if data.cpg_props_json is not None:
        datasets.append(
            _dataset_record_from_table(
                name="cpg_props_json",
                kind="cpg_output",
                table=data.cpg_props_json,
            )
        )
        outputs.append(
            _output_record_from_table(
                name="cpg_props_json",
                table=data.cpg_props_json,
                fingerprints=OutputFingerprintInputs(
                    profile_hash=profile_hash,
                    writer_strategy=writer_strategy,
                ),
            )
        )
    return datasets, outputs


def _collect_rule_records(data: ManifestData) -> list[RuleRecord]:
    records: list[RuleRecord] = []
    if data.relationship_rules:
        records.extend(
            [
                RuleRecord(
                    name=str(rule.name),
                    output_dataset=str(rule.output_dataset),
                    kind=str(rule.kind),
                    contract_name=rule.contract_name,
                    priority=int(rule.priority),
                    inputs=[dref.name for dref in rule.inputs],
                    evidence=(
                        cast("JsonDict", to_builtins(rule.evidence))
                        if rule.evidence is not None
                        else None
                    ),
                    confidence_policy=(
                        cast("JsonDict", to_builtins(rule.confidence_policy))
                        if rule.confidence_policy is not None
                        else None
                    ),
                    ambiguity_policy=(
                        cast("JsonDict", to_builtins(rule.ambiguity_policy))
                        if rule.ambiguity_policy is not None
                        else None
                    ),
                )
                for rule in data.relationship_rules
            ]
        )
    if data.normalize_rules:
        records.extend(
            [
                RuleRecord(
                    name=str(rule.name),
                    output_dataset=str(rule.output),
                    kind="normalize",
                    contract_name=None,
                    priority=int(rule.priority),
                    inputs=list(rule.inputs),
                    evidence=(
                        cast("JsonDict", to_builtins(rule.evidence))
                        if rule.evidence is not None
                        else None
                    ),
                    confidence_policy=(
                        cast("JsonDict", to_builtins(rule.confidence_policy))
                        if rule.confidence_policy is not None
                        else None
                    ),
                    ambiguity_policy=(
                        cast("JsonDict", to_builtins(rule.ambiguity_policy))
                        if rule.ambiguity_policy is not None
                        else None
                    ),
                )
                for rule in data.normalize_rules
            ]
        )
    return records


def _collect_lineage_records(data: ManifestData) -> list[OutputLineageRecord]:
    records: list[OutputLineageRecord] = []
    if data.relationship_output_lineage:
        for name in sorted(data.relationship_output_lineage):
            rules = data.relationship_output_lineage[name]
            records.append(OutputLineageRecord(output_dataset=name, rules=list(rules)))
    if data.normalize_output_lineage:
        for name in sorted(data.normalize_output_lineage):
            rules = data.normalize_output_lineage[name]
            records.append(OutputLineageRecord(output_dataset=name, rules=list(rules)))
    return records


def _extract_record_from_spec(
    spec: EvidenceSpec,
    *,
    required_columns: Sequence[str] | None = None,
    error_rows: int | None = None,
) -> ExtractRecord:
    from datafusion_engine.extract_extractors import extractor_spec
    from datafusion_engine.extract_registry import dataset_schema

    sources: list[str] = []
    if spec.template is not None:
        try:
            sources = list(extractor_spec(spec.template).required_inputs)
        except KeyError:
            sources = []
    schema = dataset_schema(spec.name)
    return ExtractRecord(
        name=spec.name,
        alias=spec.alias,
        template=spec.template,
        evidence_family=spec.evidence_family,
        coordinate_system=spec.coordinate_system,
        evidence_rank=spec.evidence_rank,
        ambiguity_policy=spec.ambiguity_policy,
        required_columns=list(required_columns or spec.required_columns),
        sources=sources,
        schema_fingerprint=schema_fingerprint(schema),
        error_rows=error_rows,
    )


def _collect_extract_records(data: ManifestData) -> list[ExtractRecord]:
    from extract.evidence_specs import evidence_spec, evidence_specs

    plan = data.extract_evidence_plan
    records: list[ExtractRecord] = []
    seen: set[str] = set()
    error_counts = data.extract_error_counts or {}
    if plan is None:
        records.extend(
            [
                _extract_record_from_spec(
                    spec,
                    error_rows=error_counts.get(spec.name),
                )
                for spec in evidence_specs()
            ]
        )
        return records

    for req in plan.requirements:
        try:
            spec = evidence_spec(req.name)
        except KeyError:
            continue
        if spec.name in seen:
            continue
        seen.add(spec.name)
        records.append(
            _extract_record_from_spec(
                spec,
                required_columns=req.required_columns or spec.required_columns,
                error_rows=error_counts.get(spec.name),
            )
        )
    return records


def _scan_telemetry_payload(
    telemetry: Mapping[str, Mapping[str, ScanTelemetry]],
) -> JsonDict:
    payload: JsonDict = {}
    for output_name, entries in telemetry.items():
        payload[output_name] = {
            dataset_name: {
                "fragment_count": entry.fragment_count,
                "row_group_count": entry.row_group_count,
                "count_rows": entry.count_rows,
                "estimated_rows": entry.estimated_rows,
                "file_hints": list(entry.file_hints),
                "fragment_paths": list(entry.fragment_paths),
                "partition_expressions": list(entry.partition_expressions),
                "dataset_schema": entry.dataset_schema,
                "projected_schema": entry.projected_schema,
                "discovery_policy": entry.discovery_policy,
                "scan_profile": entry.scan_profile,
            }
            for dataset_name, entry in entries.items()
        }
    return payload


def _json_dict_list(rows: Sequence[Mapping[str, object]]) -> list[JsonDict]:
    payload = to_builtins(list(rows))
    return cast("list[JsonDict]", payload)


def _param_tables_payload(data: ManifestData) -> JsonDict:
    payload: JsonDict = {}
    if data.param_scalar_signature:
        payload["_scalar_signature"] = data.param_scalar_signature
    if not data.param_table_artifacts:
        return payload
    for name, artifact in data.param_table_artifacts.items():
        payload[name] = {
            "rows": int(artifact.rows),
            "signature": artifact.signature,
            "schema_fingerprint": artifact.schema_fingerprint,
        }
    return payload


def _sqlglot_planner_dag_hashes(
    payloads: Sequence[Mapping[str, object]] | None,
) -> list[JsonDict] | None:
    if not payloads:
        return None
    results: list[JsonDict] = []
    for payload in payloads:
        optimized_sql = payload.get("optimized_sql")
        if not isinstance(optimized_sql, str) or not optimized_sql:
            continue
        try:
            dialect = payload.get("sql_dialect") or "datafusion_ext"
            expr = parse_sql(
                optimized_sql,
                options=ParseSqlOptions(dialect=str(dialect)),
            )
        except (TypeError, ValueError, ParseError):
            continue
        dag = planner_dag_snapshot(expr)
        subplan_hashes = [str(step["step_id"]) for step in dag.steps]
        results.append(
            {
                "domain": _optional_str(payload.get("domain")),
                "rule_name": _optional_str(payload.get("rule_name")),
                "plan_signature": _optional_str(payload.get("plan_signature")),
                "plan_fingerprint": _optional_str(payload.get("plan_fingerprint")),
                "sqlglot_policy_hash": _optional_str(payload.get("sqlglot_policy_hash")),
                "sql_dialect": _optional_str(payload.get("sql_dialect")),
                "planner_dag_hash": dag.dag_hash,
                "subplan_hashes": subplan_hashes,
            }
        )
    return results or None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    return str(value)


def _optional_note(value: T | None, transform: Callable[[T], JsonValue]) -> JsonValue | None:
    if not value:
        return None
    return transform(value)


def _to_json_value(value: object) -> JsonValue:
    return cast("JsonValue", to_builtins(value))


def _manifest_notes(data: ManifestData) -> JsonDict:
    notes: JsonDict = dict(data.notes) if data.notes else {}
    notes["diagnostics_schema_version"] = DIAGNOSTICS_SCHEMA_VERSION
    optional_notes: dict[str, JsonValue | None] = {
        "relspec_scan_telemetry": _optional_note(
            data.relspec_scan_telemetry,
            _scan_telemetry_payload,
        ),
        "datafusion_settings": _optional_note(data.datafusion_settings, list),
        "datafusion_settings_hash": _optional_note(data.datafusion_settings_hash, _to_json_value),
        "datafusion_feature_gates": _optional_note(data.datafusion_feature_gates, dict),
        "datafusion_metrics": _optional_note(data.datafusion_metrics, _to_json_value),
        "datafusion_traces": _optional_note(data.datafusion_traces, _to_json_value),
        "datafusion_function_catalog": _optional_note(
            data.datafusion_function_catalog,
            _json_dict_list,
        ),
        "datafusion_function_catalog_hash": _optional_note(
            data.datafusion_function_catalog_hash,
            _to_json_value,
        ),
        "datafusion_schema_map": _optional_note(data.datafusion_schema_map, _to_json_value),
        "datafusion_schema_map_hash": _optional_note(
            data.datafusion_schema_map_hash,
            _to_json_value,
        ),
        "runtime_profile_snapshot": _optional_note(
            data.runtime_profile_snapshot,
            _to_json_value,
        ),
        "runtime_profile_hash": _optional_note(data.runtime_profile_hash, _to_json_value),
        "sqlglot_policy_snapshot": _optional_note(data.sqlglot_policy_snapshot, _to_json_value),
        "function_registry_snapshot": _optional_note(
            data.function_registry_snapshot,
            _to_json_value,
        ),
        "function_registry_hash": _optional_note(data.function_registry_hash, _to_json_value),
        "sqlglot_ast_payloads": _optional_note(data.sqlglot_ast_payloads, _to_json_value),
        "sqlglot_planner_dag_hashes": _optional_note(
            data.sqlglot_ast_payloads,
            _sqlglot_planner_dag_hashes,
        ),
        "dataset_registry_snapshot": _optional_note(
            data.dataset_registry_snapshot,
            _json_dict_list,
        ),
        "runtime_profile_name": _optional_note(data.runtime_profile_name, _to_json_value),
        "determinism_tier": _optional_note(data.determinism_tier, _to_json_value),
    }
    for key, value in optional_notes.items():
        if value is not None:
            notes[key] = value
    return notes


def build_manifest(context: ManifestContext, data: ManifestData) -> Manifest:
    """Construct a run manifest with the required fields.

    Includes:
      - dataset paths (when available)
      - schema fingerprints
      - row counts
      - rule outputs produced

    Returns
    -------
    Manifest
        Manifest record for the current run.
    """
    now = int(time.time())

    datasets = _collect_relationship_inputs(data)

    rel_datasets, rel_outputs = _collect_relationship_outputs(data)
    datasets.extend(rel_datasets)
    outputs = list(rel_outputs)

    cpg_datasets, cpg_outputs = _collect_cpg_outputs(data)
    datasets.extend(cpg_datasets)
    outputs.extend(cpg_outputs)

    rules = _collect_rule_records(data)
    lineage = _collect_lineage_records(data)
    extracts = _collect_extract_records(data)

    if data.produced_relationship_output_names:
        outputs.append(
            OutputRecord(
                name="produced_relationship_outputs",
                rows=len(data.produced_relationship_output_names),
            )
        )

    repro_extra: JsonDict = {}
    if data.runtime_profile_snapshot:
        repro_extra["runtime_profile_snapshot"] = cast("JsonValue", data.runtime_profile_snapshot)
    if data.runtime_profile_hash:
        repro_extra["runtime_profile_hash"] = data.runtime_profile_hash
    if data.sqlglot_policy_snapshot:
        repro_extra["sqlglot_policy_snapshot"] = cast("JsonValue", data.sqlglot_policy_snapshot)
    if data.datafusion_schema_map_hash:
        repro_extra["datafusion_schema_map_hash"] = data.datafusion_schema_map_hash
    if data.function_registry_hash:
        repro_extra["function_registry_hash"] = data.function_registry_hash
    if data.function_registry_snapshot:
        repro_extra["function_registry_snapshot"] = cast(
            "JsonValue", data.function_registry_snapshot
        )
    from obs.repro import collect_repro_info

    repro = collect_repro_info(context.repo_root, extra=repro_extra or None)
    notes = _manifest_notes(data)
    params_payload = _param_tables_payload(data)

    return Manifest(
        manifest_version=1,
        created_at_unix_s=now,
        repo_root=context.repo_root,
        relspec_mode=context.relspec_mode,
        work_dir=context.work_dir,
        output_dir=context.output_dir,
        datasets=datasets,
        rules=sorted(rules, key=lambda rr: (rr.output_dataset, rr.priority, rr.name)),
        outputs=outputs,
        lineage=lineage,
        extracts=extracts,
        params=params_payload,
        repro=repro,
        notes=notes,
    )


def write_manifest_delta(
    manifest: Manifest | JsonDict,
    path: PathLike,
    *,
    overwrite: bool = True,
    execution: IbisExecutionContext,
) -> str:
    """Write manifest Delta table to the provided path.

    Returns
    -------
    str
        Path to the written Delta table.

    Raises
    ------
    FileExistsError
        Raised when the manifest already exists and overwrite is False.
    """
    from ibis_engine.io_bridge import (
        IbisDatasetWriteOptions,
        IbisDeltaWriteOptions,
        write_ibis_dataset_delta,
    )

    payload = manifest.to_dict() if isinstance(manifest, Manifest) else manifest
    target = ensure_path(path)
    if target.exists() and not overwrite:
        msg = f"Manifest already exists at {target}."
        raise FileExistsError(msg)
    table = pa.Table.from_pylist([dict(payload)])
    options = IbisDeltaWriteOptions(
        mode="overwrite" if overwrite else "error",
        schema_mode="overwrite" if overwrite else None,
    )
    result = write_ibis_dataset_delta(
        table,
        str(target),
        options=IbisDatasetWriteOptions(
            execution=execution,
            writer_strategy="datafusion",
            delta_options=options,
        ),
    )
    return result.path
