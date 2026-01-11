"""Reproducibility helpers for manifests and run bundles."""

from __future__ import annotations

import hashlib
import json
import platform
import shutil
import sys
import time
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from importlib import metadata as importlib_metadata
from importlib.metadata import PackageNotFoundError
from pathlib import Path

from arrowdsl.contracts import Contract, DedupeSpec, SortKey
from arrowdsl.pyarrow_protocols import DataTypeLike, SchemaLike, TableLike
from core_types import JsonDict, JsonValue, PathLike, ensure_path
from relspec.compiler import CompiledOutput
from relspec.model import RelationshipRule
from relspec.registry import ContractCatalog, DatasetLocation, RelationshipRegistry

# -----------------------
# Basic environment capture
# -----------------------


def _pkg_version(name: str) -> str | None:
    try:
        return importlib_metadata.version(name)
    except PackageNotFoundError:
        return None


def try_get_git_info(repo_root: str | None) -> JsonDict:
    """Collect best-effort git info without shelling out.

    Looks for:
      .git/HEAD and referenced ref file.

    Returns
    -------
    JsonDict
        Git metadata when available, otherwise a minimal status dict.
    """
    if not repo_root:
        return {"present": False}

    git_dir = Path(repo_root) / ".git"
    head_path = git_dir / "HEAD"
    if not head_path.exists():
        return {"present": False}

    try:
        head = head_path.read_text(encoding="utf-8").strip()
    except OSError:
        return {"present": True, "error": "failed_to_read_HEAD"}

    if head.startswith("ref:"):
        ref = head.split(":", 1)[1].strip()
        ref_path = git_dir / Path(ref)
        sha: str | None = None
        if ref_path.exists():
            with suppress(OSError):
                sha = ref_path.read_text(encoding="utf-8").strip()
        return {"present": True, "head": head, "ref": ref, "commit": sha}

    # Detached head case: HEAD contains commit sha
    return {"present": True, "head": "detached", "commit": head}


def collect_repro_info(
    repo_root: str | None = None, *, extra: Mapping[str, JsonValue] | None = None
) -> JsonDict:
    """Capture a compact reproducibility bundle.

    Intentionally avoids huge payloads (pip freeze, full env vars).

    Returns
    -------
    JsonDict
        Reproducibility metadata snapshot.
    """
    info: JsonDict = {
        "python": {
            "version": sys.version,
            "executable": sys.executable,
        },
        "platform": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python_implementation": platform.python_implementation(),
        },
        "packages": {
            "pyarrow": _pkg_version("pyarrow"),
            "sf-hamilton": _pkg_version("sf-hamilton") or _pkg_version("hamilton"),
            "libcst": _pkg_version("libcst"),
        },
        "git": try_get_git_info(repo_root),
    }
    if extra:
        info["extra"] = dict(extra)
    return info


# -----------------------
# JSON serialization helpers
# -----------------------


def _ensure_dir(path: Path) -> None:
    path.mkdir(exist_ok=True, parents=True)


def _json_default(obj: object) -> JsonValue:
    # Arrow types
    if isinstance(obj, SchemaLike):
        return arrow_schema_to_dict(obj)
    if isinstance(obj, DataTypeLike):
        return str(obj)

    # Common containers
    if isinstance(obj, set):
        return sorted((_json_default(v) for v in obj), key=str)
    if isinstance(obj, tuple):
        return [_json_default(v) for v in obj]

    # Dataclasses / objects
    obj_dict = getattr(obj, "__dict__", None)
    if isinstance(obj_dict, Mapping):
        return {str(k): _json_default(v) for k, v in obj_dict.items()}

    return str(obj)


def _write_json(path: PathLike, data: JsonValue, *, overwrite: bool = True) -> str:
    target = ensure_path(path)
    _ensure_dir(target.parent)
    if overwrite and target.exists():
        target.unlink()
    with target.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True, default=_json_default)
    return str(target)


# -----------------------
# Schema + contract + rule snapshots
# -----------------------


def arrow_schema_to_dict(schema: SchemaLike) -> JsonDict:
    """Serialize an Arrow schema to a plain dictionary.

    Returns
    -------
    JsonDict
        JSON-serializable schema representation.
    """
    return {
        "fields": [
            {"name": f.name, "type": str(f.type), "nullable": bool(f.nullable)} for f in schema
        ]
    }


def schema_fingerprint(schema: SchemaLike) -> str:
    """Compute a stable schema fingerprint hash.

    Returns
    -------
    str
        SHA-256 fingerprint of the schema.
    """
    payload = json.dumps(arrow_schema_to_dict(schema), sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def serialize_contract(contract: Contract) -> JsonDict:
    """Serialize a ``Contract`` into JSON-friendly metadata.

    Returns
    -------
    JsonDict
        Serialized contract metadata.
    """

    def _serialize_sort_keys(keys: tuple[SortKey, ...] | None) -> list[JsonDict] | None:
        if keys is None:
            return None
        return [{"column": key.column, "order": key.order} for key in keys]

    dedupe_dict: JsonDict | None = None
    if contract.dedupe is not None:
        dedupe: DedupeSpec = contract.dedupe
        dedupe_dict = {
            "keys": list(dedupe.keys),
            "strategy": dedupe.strategy,
            "tie_breakers": _serialize_sort_keys(dedupe.tie_breakers),
        }

    return {
        "name": contract.name,
        "version": contract.version,
        "required_non_null": list(contract.required_non_null),
        "key_fields": list(contract.key_fields),
        "schema_fingerprint": schema_fingerprint(contract.schema),
        "schema": arrow_schema_to_dict(contract.schema),
        "dedupe": dedupe_dict,
        "canonical_sort": _serialize_sort_keys(contract.canonical_sort),
    }


def serialize_contract_catalog(contract_catalog: ContractCatalog) -> JsonDict:
    """Serialize a ``ContractCatalog`` snapshot.

    Returns
    -------
    JsonDict
        Serialized contract catalog.
    """
    names = list(contract_catalog.names())
    return {"contracts": [serialize_contract(contract_catalog.get(name)) for name in names]}


def serialize_relationship_rule(rule: RelationshipRule) -> JsonDict:
    """Serialize a ``RelationshipRule`` into JSON-friendly metadata.

    Returns
    -------
    JsonDict
        Serialized rule metadata.
    """
    inputs = [ref.name for ref in rule.inputs]
    return {
        "name": rule.name,
        "kind": str(rule.kind),
        "output_dataset": rule.output_dataset,
        "contract_name": rule.contract_name,
        "priority": int(rule.priority),
        "emit_rule_meta": bool(rule.emit_rule_meta),
        "inputs": inputs,
        "hash_join": _json_default(rule.hash_join) if rule.hash_join is not None else None,
        "interval_align": _json_default(rule.interval_align)
        if rule.interval_align is not None
        else None,
        "project": _json_default(rule.project) if rule.project is not None else None,
        "post_kernels": [_json_default(k) for k in rule.post_kernels],
    }


def serialize_relationship_registry(registry: RelationshipRegistry) -> JsonDict:
    """Serialize a ``RelationshipRegistry`` snapshot.

    Returns
    -------
    JsonDict
        Serialized relationship registry.
    """
    return {
        "rules": [serialize_relationship_rule(r) for r in registry.rules()],
        "outputs": list(registry.outputs()),
        "inputs": list(registry.inputs()),
    }


def serialize_dataset_locations(locations: Mapping[str, DatasetLocation]) -> JsonDict:
    """Serialize dataset locations returned by persistence helpers.

    Returns
    -------
    JsonDict
        Serialized dataset location mapping.
    """
    out: JsonDict = {}
    for name, loc in locations.items():
        out[name] = {
            "path": str(loc.path),
            "format": loc.format,
            "partitioning": loc.partitioning,
        }
    return out


def serialize_compiled_outputs(compiled: Mapping[str, CompiledOutput]) -> JsonDict:
    """Serialize compiled relationship outputs.

    Returns
    -------
    JsonDict
        Serialized compiled outputs.
    """
    outputs: dict[str, JsonDict] = {}
    for key, obj in compiled.items():
        contributors: list[JsonDict] = []
        for compiled_rule in obj.contributors:
            rule = compiled_rule.rule
            contributors.append(
                {
                    "rule_name": rule.name,
                    "priority": rule.priority,
                    "kind": str(rule.kind),
                    "inputs": [ref.name for ref in rule.inputs],
                }
            )
        outputs[key] = {
            "output_dataset": obj.output_dataset,
            "contract_name": obj.contract_name,
            "contributors": contributors,
        }
    return {
        "outputs": outputs,
        "produced_output_keys": sorted(compiled.keys()),
    }


# -----------------------
# Run bundle writer
# -----------------------


def make_run_bundle_name(
    run_manifest: Mapping[str, JsonValue], run_config: Mapping[str, JsonValue]
) -> str:
    """Build a deterministic-ish run bundle name.

    Returns
    -------
    str
        Bundle name in the form run_<created_at>_<hash10>.
    """
    created_value = run_manifest.get("created_at_unix_s")
    if isinstance(created_value, bool):
        created_value = None
    if isinstance(created_value, int):
        ts = created_value
    elif isinstance(created_value, float) or (
        isinstance(created_value, str) and created_value.isdigit()
    ):
        ts = int(created_value)
    else:
        ts = int(time.time())
    payload = json.dumps(
        {"manifest": run_manifest, "config": run_config}, sort_keys=True, default=str
    ).encode("utf-8")
    h = hashlib.sha256(payload).hexdigest()[:10]
    return f"run_{ts}_{h}"


@dataclass(frozen=True)
class RunBundleContext:
    """Inputs required to write a reproducible run bundle."""

    base_dir: PathLike
    run_manifest: Mapping[str, JsonValue]
    run_config: Mapping[str, JsonValue]

    relationship_registry: RelationshipRegistry | None = None
    relationship_contracts: ContractCatalog | None = None
    compiled_relationship_outputs: Mapping[str, CompiledOutput] | None = None

    relspec_input_locations: Mapping[str, DatasetLocation] | None = None
    relspec_input_tables: Mapping[str, TableLike] | None = None
    relationship_output_tables: Mapping[str, TableLike] | None = None
    cpg_output_tables: Mapping[str, TableLike] | None = None

    include_schemas: bool = True
    overwrite: bool = True


def _ensure_bundle_dir(bundle_dir: Path, *, overwrite: bool) -> None:
    if overwrite and bundle_dir.exists():
        with suppress(OSError):
            shutil.rmtree(bundle_dir)
    _ensure_dir(bundle_dir)


def _write_manifest_files(
    bundle_dir: Path,
    context: RunBundleContext,
    *,
    bundle_name: str,
    files_written: list[str],
) -> None:
    files_written.append(
        _write_json(bundle_dir / "manifest.json", dict(context.run_manifest), overwrite=True)
    )
    files_written.append(
        _write_json(bundle_dir / "config.json", dict(context.run_config), overwrite=True)
    )
    repo_root_value = context.run_config.get("repo_root")
    repo_root = repo_root_value if isinstance(repo_root_value, str) else None
    repro = collect_repro_info(repo_root=repo_root, extra={"bundle_name": bundle_name})
    files_written.append(_write_json(bundle_dir / "repro.json", repro, overwrite=True))


def _write_dataset_locations(
    bundle_dir: Path, context: RunBundleContext, files_written: list[str]
) -> None:
    if context.relspec_input_locations is None:
        return
    ds_dir = bundle_dir / "datasets"
    _ensure_dir(ds_dir)
    locs = serialize_dataset_locations(context.relspec_input_locations)
    files_written.append(_write_json(ds_dir / "locations.json", locs, overwrite=True))


def _write_relspec_snapshots(
    bundle_dir: Path, context: RunBundleContext, files_written: list[str]
) -> None:
    relspec_dir = bundle_dir / "relspec"
    _ensure_dir(relspec_dir)
    if context.relationship_registry is not None:
        snap = serialize_relationship_registry(context.relationship_registry)
        files_written.append(_write_json(relspec_dir / "rules.json", snap, overwrite=True))
    if context.relationship_contracts is not None:
        snap = serialize_contract_catalog(context.relationship_contracts)
        files_written.append(_write_json(relspec_dir / "contracts.json", snap, overwrite=True))
    if context.compiled_relationship_outputs is not None:
        snap = serialize_compiled_outputs(context.compiled_relationship_outputs)
        files_written.append(
            _write_json(relspec_dir / "compiled_outputs.json", snap, overwrite=True)
        )


def _write_schema_snapshot(
    schemas_dir: Path,
    *,
    name: str,
    table: TableLike,
    files_written: list[str],
) -> None:
    doc: JsonDict = {
        "name": name,
        "rows": int(table.num_rows),
        "schema_fingerprint": schema_fingerprint(table.schema),
        "schema": arrow_schema_to_dict(table.schema),
    }
    files_written.append(_write_json(schemas_dir / f"{name}.schema.json", doc, overwrite=True))


def _write_schema_group(
    schemas_dir: Path,
    *,
    prefix: str,
    tables: Mapping[str, TableLike] | None,
    files_written: list[str],
) -> None:
    if not tables:
        return
    for name, table in tables.items():
        if table is not None:
            _write_schema_snapshot(
                schemas_dir,
                name=f"{prefix}{name}",
                table=table,
                files_written=files_written,
            )


def _write_schema_snapshots(
    bundle_dir: Path, context: RunBundleContext, files_written: list[str]
) -> None:
    if not context.include_schemas:
        return
    schemas_dir = bundle_dir / "schemas"
    _ensure_dir(schemas_dir)

    _write_schema_group(
        schemas_dir,
        prefix="relspec_input__",
        tables=context.relspec_input_tables,
        files_written=files_written,
    )
    _write_schema_group(
        schemas_dir,
        prefix="relationship_output__",
        tables=context.relationship_output_tables,
        files_written=files_written,
    )
    _write_schema_group(
        schemas_dir,
        prefix="cpg_output__",
        tables=context.cpg_output_tables,
        files_written=files_written,
    )


def write_run_bundle(
    *,
    context: RunBundleContext,
) -> JsonDict:
    """Write a run bundle directory capturing enough metadata to replay a run later.

    This works even if code changes.

    Directory layout:
      <base_dir>/<bundle_name>/
        manifest.json
        config.json
        repro.json
        datasets/locations.json
        relspec/rules.json
        relspec/contracts.json
        relspec/compiled_outputs.json
        schemas/*.schema.json

    Parameters
    ----------
    context:
        Bundle inputs and snapshot tables.

    Returns
    -------
    JsonDict
        Bundle metadata including paths and files written.
    """
    base_path = ensure_path(context.base_dir)
    _ensure_dir(base_path)
    bundle_name = make_run_bundle_name(context.run_manifest, context.run_config)
    bundle_dir = base_path / bundle_name

    _ensure_bundle_dir(bundle_dir, overwrite=context.overwrite)

    files_written: list[str] = []

    _write_manifest_files(bundle_dir, context, bundle_name=bundle_name, files_written=files_written)
    _write_dataset_locations(bundle_dir, context, files_written)
    _write_relspec_snapshots(bundle_dir, context, files_written)
    _write_schema_snapshots(bundle_dir, context, files_written)

    return {
        "bundle_dir": str(bundle_dir),
        "bundle_name": bundle_name,
        "files_written": files_written,
    }
