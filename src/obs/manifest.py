from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, asdict, field
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union

import pyarrow as pa

from .stats import schema_fingerprint, table_summary
from .repro import collect_repro_info


@dataclass(frozen=True)
class DatasetRecord:
    """
    Records one dataset artifact.
    """
    name: str
    kind: str  # "input" | "intermediate" | "relationship_output" | "cpg_output"
    path: Optional[str] = None
    format: Optional[str] = None

    rows: Optional[int] = None
    columns: Optional[int] = None
    schema_fingerprint: Optional[str] = None

    # Optional: include a small schema description (safe for debugging)
    schema: Optional[List[Dict[str, Any]]] = None


@dataclass(frozen=True)
class RuleRecord:
    """
    Records one relationship rule.
    """
    name: str
    output_dataset: str
    kind: str
    contract_name: Optional[str]
    priority: int
    inputs: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class OutputRecord:
    """
    Records a produced output (e.g., relationship outputs, cpg outputs).
    """
    name: str
    rows: Optional[int] = None
    schema_fingerprint: Optional[str] = None


@dataclass(frozen=True)
class Manifest:
    """
    Top-level manifest record.
    """
    manifest_version: int
    created_at_unix_s: int

    repo_root: Optional[str]
    relspec_mode: str
    work_dir: Optional[str]
    output_dir: Optional[str]

    datasets: List[DatasetRecord] = field(default_factory=list)
    rules: List[RuleRecord] = field(default_factory=list)
    outputs: List[OutputRecord] = field(default_factory=list)

    repro: Dict[str, Any] = field(default_factory=dict)
    notes: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _dataset_record_from_table(
    *,
    name: str,
    kind: str,
    table: Optional[pa.Table],
    path: Optional[str] = None,
    format: Optional[str] = None,
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


def _output_record_from_table(name: str, table: Optional[pa.Table]) -> OutputRecord:
    if table is None:
        return OutputRecord(name=name)
    return OutputRecord(
        name=name,
        rows=int(table.num_rows),
        schema_fingerprint=schema_fingerprint(table.schema),
    )


def build_manifest(
    *,
    repo_root: Optional[str],
    relspec_mode: str,
    work_dir: Optional[str],
    output_dir: Optional[str],

    # relationship inputs (tables) and (optional) persisted locations
    relspec_input_tables: Optional[Mapping[str, pa.Table]] = None,
    relspec_input_locations: Optional[Mapping[str, Any]] = None,  # DatasetLocation-like objects

    # relationship outputs (tables)
    relationship_outputs: Optional[Mapping[str, pa.Table]] = None,

    # final outputs
    cpg_nodes: Optional[pa.Table] = None,
    cpg_edges: Optional[pa.Table] = None,
    cpg_props: Optional[pa.Table] = None,

    # rule metadata
    relationship_rules: Optional[Sequence[Any]] = None,  # RelationshipRule-like objects
    produced_relationship_output_names: Optional[Sequence[str]] = None,

    # extra notes
    notes: Optional[Dict[str, Any]] = None,
) -> Manifest:
    """
    Construct a run manifest with the required fields:
      - dataset paths (when available)
      - schema fingerprints
      - row counts
      - rule outputs produced
    """
    now = int(time.time())

    datasets: List[DatasetRecord] = []
    outputs: List[OutputRecord] = []
    rules: List[RuleRecord] = []

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
            datasets.append(_dataset_record_from_table(name=name, kind="relationship_output", table=t))
            outputs.append(_output_record_from_table(name=name, table=t))

    # CPG outputs
    if cpg_nodes is not None:
        datasets.append(_dataset_record_from_table(name="cpg_nodes", kind="cpg_output", table=cpg_nodes))
        outputs.append(_output_record_from_table("cpg_nodes", cpg_nodes))
    if cpg_edges is not None:
        datasets.append(_dataset_record_from_table(name="cpg_edges", kind="cpg_output", table=cpg_edges))
        outputs.append(_output_record_from_table("cpg_edges", cpg_edges))
    if cpg_props is not None:
        datasets.append(_dataset_record_from_table(name="cpg_props", kind="cpg_output", table=cpg_props))
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
        outputs.append(OutputRecord(name="produced_relationship_outputs", rows=len(list(produced_relationship_output_names))))

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


def write_manifest_json(manifest: Union[Manifest, Dict[str, Any]], path: str, *, overwrite: bool = True) -> str:
    """
    Write manifest JSON to `path`.
    """
    _ensure_dir(os.path.dirname(path) or ".")
    if overwrite and os.path.exists(path):
        os.remove(path)

    payload = manifest.to_dict() if isinstance(manifest, Manifest) else manifest
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False, sort_keys=True)

    return path
