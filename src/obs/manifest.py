"""Build and write run manifest records for observability."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import TYPE_CHECKING, cast

from arrowdsl.core.interop import TableLike
from arrowdsl.json_factory import JsonPolicy, dump_path, json_default
from arrowdsl.plan.metrics import table_summary
from arrowdsl.schema.serialization import schema_fingerprint
from core_types import JsonDict, JsonValue, PathLike, ensure_path
from extract.evidence_specs import EvidenceSpec, evidence_spec, evidence_specs
from extract.registry_extractors import extractor_spec
from extract.registry_specs import dataset_schema
from ibis_engine.param_tables import ParamTableArtifact
from obs.repro import collect_repro_info

if TYPE_CHECKING:
    from arrowdsl.plan.query import ScanTelemetry
    from extract.evidence_plan import EvidencePlan
    from normalize.rule_model import NormalizeRule
    from relspec.model import RelationshipRule
    from relspec.registry import DatasetLocation


@dataclass(frozen=True)
class DatasetRecord:
    """Record a dataset artifact."""

    name: str
    kind: str  # "input" | "intermediate" | "relationship_output" | "cpg_output"
    path: str | None = None
    format: str | None = None

    rows: int | None = None
    columns: int | None = None
    schema_fingerprint: str | None = None

    # Optional: include a small schema description (safe for debugging)
    schema: list[JsonDict] | None = None


@dataclass(frozen=True)
class RuleRecord:
    """Record a rule definition."""

    name: str
    output_dataset: str
    kind: str
    contract_name: str | None
    priority: int
    inputs: list[str] = field(default_factory=list)
    evidence: JsonDict | None = None
    confidence_policy: JsonDict | None = None
    ambiguity_policy: JsonDict | None = None


@dataclass(frozen=True)
class OutputRecord:
    """Record a produced output (e.g., relationship outputs, cpg outputs)."""

    name: str
    rows: int | None = None
    schema_fingerprint: str | None = None


@dataclass(frozen=True)
class OutputLineageRecord:
    """Record rule lineage for a relationship output."""

    output_dataset: str
    rules: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ExtractRecord:
    """Record extract output lineage and metadata."""

    name: str
    alias: str
    template: str | None
    evidence_family: str | None
    coordinate_system: str | None
    evidence_rank: int | None
    ambiguity_policy: str | None
    required_columns: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)
    schema_fingerprint: str | None = None
    error_rows: int | None = None


@dataclass(frozen=True)
class Manifest:
    """Top-level manifest record."""

    manifest_version: int
    created_at_unix_s: int

    repo_root: str | None
    relspec_mode: str
    work_dir: str | None
    output_dir: str | None

    datasets: list[DatasetRecord] = field(default_factory=list)
    rules: list[RuleRecord] = field(default_factory=list)
    outputs: list[OutputRecord] = field(default_factory=list)
    lineage: list[OutputLineageRecord] = field(default_factory=list)
    extracts: list[ExtractRecord] = field(default_factory=list)
    params: JsonDict = field(default_factory=dict)

    repro: JsonDict = field(default_factory=dict)
    notes: JsonDict = field(default_factory=dict)

    def to_dict(self) -> JsonDict:
        """Convert the manifest to a plain dictionary.

        Returns
        -------
        JsonDict
            Manifest data as a dictionary.
        """
        return asdict(self)


@dataclass(frozen=True)
class ManifestContext:
    """Core context fields for manifest construction."""

    repo_root: str | None
    relspec_mode: str
    work_dir: str | None
    output_dir: str | None


@dataclass(frozen=True)
class ManifestData:
    """Optional inputs used to populate manifest records."""

    relspec_input_tables: Mapping[str, TableLike] | None = None
    relspec_input_locations: Mapping[str, DatasetLocation] | None = None
    relationship_outputs: Mapping[str, TableLike] | None = None
    cpg_nodes: TableLike | None = None
    cpg_edges: TableLike | None = None
    cpg_props: TableLike | None = None
    extract_evidence_plan: EvidencePlan | None = None
    extract_error_counts: Mapping[str, int] | None = None
    relationship_rules: Sequence[RelationshipRule] | None = None
    normalize_rules: Sequence[NormalizeRule] | None = None
    produced_relationship_output_names: Sequence[str] | None = None
    relationship_output_lineage: Mapping[str, Sequence[str]] | None = None
    normalize_output_lineage: Mapping[str, Sequence[str]] | None = None
    relspec_scan_telemetry: Mapping[str, Mapping[str, ScanTelemetry]] | None = None
    datafusion_settings: Sequence[Mapping[str, str]] | None = None
    datafusion_settings_hash: str | None = None
    datafusion_feature_gates: Mapping[str, str] | None = None
    datafusion_metrics: Mapping[str, object] | None = None
    datafusion_traces: Mapping[str, object] | None = None
    dataset_registry_snapshot: Sequence[Mapping[str, object]] | None = None
    param_table_artifacts: Mapping[str, ParamTableArtifact] | None = None
    param_scalar_signature: str | None = None
    runtime_profile_name: str | None = None
    determinism_tier: str | None = None
    notes: JsonDict | None = None


def _dataset_record_from_table(
    *,
    name: str,
    kind: str,
    table: TableLike | None,
    path: str | None = None,
    data_format: str | None = None,
) -> DatasetRecord:
    if table is None:
        return DatasetRecord(name=name, kind=kind, path=path, format=data_format)

    summ = table_summary(table)
    return DatasetRecord(
        name=name,
        kind=kind,
        path=path,
        format=data_format,
        rows=summ["rows"],
        columns=summ["columns"],
        schema_fingerprint=summ["schema_fingerprint"],
        schema=summ["schema"],
    )


def _output_record_from_table(name: str, table: TableLike | None) -> OutputRecord:
    if table is None:
        return OutputRecord(name=name)
    return OutputRecord(
        name=name,
        rows=int(table.num_rows),
        schema_fingerprint=schema_fingerprint(table.schema),
    )


def _collect_relationship_inputs(data: ManifestData) -> list[DatasetRecord]:
    if not data.relspec_input_tables:
        return []

    records: list[DatasetRecord] = []
    for name in sorted(data.relspec_input_tables):
        table = data.relspec_input_tables[name]
        loc = data.relspec_input_locations.get(name) if data.relspec_input_locations else None
        path = str(loc.path) if loc is not None else None
        fmt = loc.format if loc is not None else None
        records.append(
            _dataset_record_from_table(
                name=name,
                kind="relationship_input",
                table=table,
                path=path,
                data_format=fmt,
            )
        )
    return records


def _collect_relationship_outputs(
    data: ManifestData,
) -> tuple[list[DatasetRecord], list[OutputRecord]]:
    if not data.relationship_outputs:
        return [], []

    datasets: list[DatasetRecord] = []
    outputs: list[OutputRecord] = []
    for name in sorted(data.relationship_outputs):
        table = data.relationship_outputs[name]
        datasets.append(
            _dataset_record_from_table(name=name, kind="relationship_output", table=table)
        )
        outputs.append(_output_record_from_table(name=name, table=table))
    return datasets, outputs


def _collect_cpg_outputs(data: ManifestData) -> tuple[list[DatasetRecord], list[OutputRecord]]:
    datasets: list[DatasetRecord] = []
    outputs: list[OutputRecord] = []

    if data.cpg_nodes is not None:
        datasets.append(
            _dataset_record_from_table(name="cpg_nodes", kind="cpg_output", table=data.cpg_nodes)
        )
        outputs.append(_output_record_from_table("cpg_nodes", data.cpg_nodes))
    if data.cpg_edges is not None:
        datasets.append(
            _dataset_record_from_table(name="cpg_edges", kind="cpg_output", table=data.cpg_edges)
        )
        outputs.append(_output_record_from_table("cpg_edges", data.cpg_edges))
    if data.cpg_props is not None:
        datasets.append(
            _dataset_record_from_table(name="cpg_props", kind="cpg_output", table=data.cpg_props)
        )
        outputs.append(_output_record_from_table("cpg_props", data.cpg_props))
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
                    evidence=asdict(rule.evidence) if rule.evidence is not None else None,
                    confidence_policy=asdict(rule.confidence_policy)
                    if rule.confidence_policy is not None
                    else None,
                    ambiguity_policy=asdict(rule.ambiguity_policy)
                    if rule.ambiguity_policy is not None
                    else None,
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
                    evidence=asdict(rule.evidence) if rule.evidence is not None else None,
                    confidence_policy=asdict(rule.confidence_policy)
                    if rule.confidence_policy is not None
                    else None,
                    ambiguity_policy=asdict(rule.ambiguity_policy)
                    if rule.ambiguity_policy is not None
                    else None,
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
            }
            for dataset_name, entry in entries.items()
        }
    return payload


def _json_dict_list(rows: Sequence[Mapping[str, object]]) -> list[JsonDict]:
    payload: list[JsonDict] = []
    for row in rows:
        record: JsonDict = {}
        for key, value in row.items():
            record[str(key)] = cast("JsonValue", json_default(value))
        payload.append(record)
    return payload


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


def _manifest_notes(data: ManifestData) -> JsonDict:
    notes: JsonDict = dict(data.notes) if data.notes else {}
    if data.relspec_scan_telemetry:
        notes["relspec_scan_telemetry"] = _scan_telemetry_payload(data.relspec_scan_telemetry)
    if data.datafusion_settings:
        notes["datafusion_settings"] = list(data.datafusion_settings)
    if data.datafusion_settings_hash:
        notes["datafusion_settings_hash"] = data.datafusion_settings_hash
    if data.datafusion_feature_gates:
        notes["datafusion_feature_gates"] = dict(data.datafusion_feature_gates)
    if data.datafusion_metrics:
        notes["datafusion_metrics"] = cast("JsonValue", data.datafusion_metrics)
    if data.datafusion_traces:
        notes["datafusion_traces"] = cast("JsonValue", data.datafusion_traces)
    if data.dataset_registry_snapshot:
        notes["dataset_registry_snapshot"] = _json_dict_list(data.dataset_registry_snapshot)
    if data.runtime_profile_name:
        notes["runtime_profile_name"] = data.runtime_profile_name
    if data.determinism_tier:
        notes["determinism_tier"] = data.determinism_tier
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

    repro = collect_repro_info(context.repo_root)
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


def write_manifest_json(
    manifest: Manifest | JsonDict, path: PathLike, *, overwrite: bool = True
) -> str:
    """Write manifest JSON to the provided path.

    Returns
    -------
    str
        Path to the written JSON file.
    """
    payload = manifest.to_dict() if isinstance(manifest, Manifest) else manifest
    policy = JsonPolicy(pretty=True, sort_keys=True)
    return dump_path(ensure_path(path), payload, policy=policy, overwrite=overwrite)
