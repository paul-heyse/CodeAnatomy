"""Build and write run manifest records for observability."""

from __future__ import annotations

import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from arrowdsl.pyarrow_protocols import TableLike
from core_types import JsonDict, PathLike, ensure_path
from obs.repro import collect_repro_info
from obs.stats import schema_fingerprint, table_summary

if TYPE_CHECKING:
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
    """Record a relationship rule."""

    name: str
    output_dataset: str
    kind: str
    contract_name: str | None
    priority: int
    inputs: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class OutputRecord:
    """Record a produced output (e.g., relationship outputs, cpg outputs)."""

    name: str
    rows: int | None = None
    schema_fingerprint: str | None = None


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
    relationship_rules: Sequence[RelationshipRule] | None = None
    produced_relationship_output_names: Sequence[str] | None = None
    notes: JsonDict | None = None


def _ensure_dir(path: Path) -> None:
    path.mkdir(exist_ok=True, parents=True)


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
    for name, table in data.relspec_input_tables.items():
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
    for name, table in data.relationship_outputs.items():
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
    if not data.relationship_rules:
        return []

    return [
        RuleRecord(
            name=str(rule.name),
            output_dataset=str(rule.output_dataset),
            kind=str(rule.kind),
            contract_name=rule.contract_name,
            priority=int(rule.priority),
            inputs=[dref.name for dref in rule.inputs],
        )
        for rule in data.relationship_rules
    ]


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

    if data.produced_relationship_output_names:
        outputs.append(
            OutputRecord(
                name="produced_relationship_outputs",
                rows=len(data.produced_relationship_output_names),
            )
        )

    repro = collect_repro_info(context.repo_root)
    notes = dict(data.notes) if data.notes else {}

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
    target = ensure_path(path)
    _ensure_dir(target.parent)
    if overwrite and target.exists():
        target.unlink()

    payload = manifest.to_dict() if isinstance(manifest, Manifest) else manifest
    with target.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False, sort_keys=True)

    return str(target)
