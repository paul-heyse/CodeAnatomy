from __future__ import annotations

import json
import os
import pathlib
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any

import pyarrow as pa

from .repro import collect_repro_info
from .stats import schema_fingerprint, table_summary


@dataclass(frozen=True)
class DatasetRecord:
    """
    Records one dataset artifact.
    """

    name: str
    kind: str  # "input" | "intermediate" | "relationship_output" | "cpg_output"
    path: str | None = None
    format: str | None = None

    rows: int | None = None
    columns: int | None = None
    schema_fingerprint: str | None = None

    # Optional: include a small schema description (safe for debugging)
    schema: list[dict[str, Any]] | None = None


@dataclass(frozen=True)
class RuleRecord:
    """
    Records one relationship rule.
    """

    name: str
    output_dataset: str
    kind: str
    contract_name: str | None
    priority: int
    inputs: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class OutputRecord:
    """
    Records a produced output (e.g., relationship outputs, cpg outputs).
    """

    name: str
    rows: int | None = None
    schema_fingerprint: str | None = None


@dataclass(frozen=True)
class Manifest:
    """
    Top-level manifest record.
    """

    manifest_version: int
    created_at_unix_s: int

    repo_root: str | None
    relspec_mode: str
    work_dir: str | None
    output_dir: str | None

    datasets: list[DatasetRecord] = field(default_factory=list)
    rules: list[RuleRecord] = field(default_factory=list)
    outputs: list[OutputRecord] = field(default_factory=list)

    repro: dict[str, Any] = field(default_factory=dict)
    notes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _ensure_dir(path: str) -> None:
    pathlib.Path(path).mkdir(exist_ok=True, parents=True)


def _dataset_record_from_table(
    *,
    name: str,
    kind: str,
    table: pa.Table | None,
    path: str | None = None,
    format: str | None = None,
) -> DatasetRecord:
    if table is None:
        return DatasetRecord(name=name, kind=kind, path=path, format=format)

    summ = table_summary(table)
    return DatasetRecord(
        name=name,
        kind=kind,
        path=path,
        format=format,
        rows=summ.get("rows"),
        columns=summ.get("columns"),
        schema_fingerprint=summ.get("schema_fingerprint"),
        schema=summ.get("schema"),
    )


def _output_record_from_table(name: str, table: pa.Table | None) -> OutputRecord:
    if table is None:
        return OutputRecord(name=name)
    return OutputRecord(
        name=name,
        rows=int(table.num_rows),
        schema_fingerprint=schema_fingerprint(table.schema),
    )


def build_manifest(
    *,
    repo_root: str | None,
    relspec_mode: str,
    work_dir: str | None,
    output_dir: str | None,
    # relationship inputs (tables) and (optional) persisted locations
    relspec_input_tables: Mapping[str, pa.Table] | None = None,
    relspec_input_locations: Mapping[str, Any] | None = None,  # DatasetLocation-like objects
    # relationship outputs (tables)
    relationship_outputs: Mapping[str, pa.Table] | None = None,
    # final outputs
    cpg_nodes: pa.Table | None = None,
    cpg_edges: pa.Table | None = None,
    cpg_props: pa.Table | None = None,
    # rule metadata
    relationship_rules: Sequence[Any] | None = None,  # RelationshipRule-like objects
    produced_relationship_output_names: Sequence[str] | None = None,
    # extra notes
    notes: dict[str, Any] | None = None,
) -> Manifest:
    """
    Construct a run manifest with the required fields:
      - dataset paths (when available)
      - schema fingerprints
      - row counts
      - rule outputs produced
    """
    now = int(time.time())

    datasets: list[DatasetRecord] = []
    outputs: list[OutputRecord] = []
    rules: list[RuleRecord] = []

    # Relationship inputs
    if relspec_input_tables:
        for name, t in relspec_input_tables.items():
            loc = None
            if relspec_input_locations and name in relspec_input_locations:
                loc = relspec_input_locations[name]
            path = getattr(loc, "path", None) if loc is not None else None
            fmt = getattr(loc, "format", None) if loc is not None else None
            datasets.append(
                _dataset_record_from_table(
                    name=name,
                    kind="relationship_input",
                    table=t,
                    path=str(path) if path is not None else None,
                    format=str(fmt) if fmt is not None else None,
                )
            )

    # Relationship outputs
    if relationship_outputs:
        for name, t in relationship_outputs.items():
            datasets.append(
                _dataset_record_from_table(name=name, kind="relationship_output", table=t)
            )
            outputs.append(_output_record_from_table(name=name, table=t))

    # CPG outputs
    if cpg_nodes is not None:
        datasets.append(
            _dataset_record_from_table(name="cpg_nodes", kind="cpg_output", table=cpg_nodes)
        )
        outputs.append(_output_record_from_table("cpg_nodes", cpg_nodes))
    if cpg_edges is not None:
        datasets.append(
            _dataset_record_from_table(name="cpg_edges", kind="cpg_output", table=cpg_edges)
        )
        outputs.append(_output_record_from_table("cpg_edges", cpg_edges))
    if cpg_props is not None:
        datasets.append(
            _dataset_record_from_table(name="cpg_props", kind="cpg_output", table=cpg_props)
        )
        outputs.append(_output_record_from_table("cpg_props", cpg_props))

    # Rule metadata
    if relationship_rules:
        for r in relationship_rules:
            # RelationshipRule-like object
            rules.append(
                RuleRecord(
                    name=str(getattr(r, "name", "")),
                    output_dataset=str(getattr(r, "output_dataset", "")),
                    kind=str(getattr(r, "kind", "")),
                    contract_name=getattr(r, "contract_name", None),
                    priority=int(getattr(r, "priority", 0)),
                    inputs=[str(getattr(dref, "name", dref)) for dref in getattr(r, "inputs", [])],
                )
            )

    # Record which outputs were produced (even if you didn't capture tables)
    if produced_relationship_output_names:
        outputs.append(
            OutputRecord(
                name="produced_relationship_outputs",
                rows=len(list(produced_relationship_output_names)),
            )
        )

    repro = collect_repro_info(repo_root)

    return Manifest(
        manifest_version=1,
        created_at_unix_s=now,
        repo_root=repo_root,
        relspec_mode=relspec_mode,
        work_dir=work_dir,
        output_dir=output_dir,
        datasets=datasets,
        rules=sorted(rules, key=lambda rr: (rr.output_dataset, rr.priority, rr.name)),
        outputs=outputs,
        repro=repro,
        notes=dict(notes or {}),
    )


def write_manifest_json(
    manifest: Manifest | dict[str, Any], path: str, *, overwrite: bool = True
) -> str:
    """
    Write manifest JSON to `path`.
    """
    _ensure_dir(os.path.dirname(path) or ".")
    if overwrite and pathlib.Path(path).exists():
        pathlib.Path(path).unlink()

    payload = manifest.to_dict() if isinstance(manifest, Manifest) else manifest
    with pathlib.Path(path).open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False, sort_keys=True)

    return path
