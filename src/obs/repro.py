from __future__ import annotations

import hashlib
import json
import os
import pathlib
import platform
import sys
import time
from collections.abc import Mapping
from typing import Any

import pyarrow as pa

try:
    import importlib.metadata as importlib_metadata  # py3.8+
except Exception:  # pragma: no cover
    import importlib_metadata  # type: ignore


# -----------------------
# Basic environment capture
# -----------------------


def _pkg_version(name: str) -> str | None:
    try:
        return importlib_metadata.version(name)
    except Exception:
        return None


def try_get_git_info(repo_root: str | None) -> dict[str, Any]:
    """
    Best-effort git info without shelling out.
    Looks for:
      .git/HEAD and referenced ref file.
    """
    if not repo_root:
        return {"present": False}

    git_dir = os.path.join(repo_root, ".git")
    head_path = os.path.join(git_dir, "HEAD")
    if not pathlib.Path(head_path).exists():
        return {"present": False}

    try:
        head = pathlib.Path(head_path).open("r", encoding="utf-8").read().strip()
    except Exception:
        return {"present": True, "error": "failed_to_read_HEAD"}

    if head.startswith("ref:"):
        ref = head.split(":", 1)[1].strip()
        ref_path = os.path.join(git_dir, ref.replace("/", os.sep))
        sha = None
        if pathlib.Path(ref_path).exists():
            try:
                sha = pathlib.Path(ref_path).open("r", encoding="utf-8").read().strip()
            except Exception:
                sha = None
        return {"present": True, "head": head, "ref": ref, "commit": sha}

    # Detached head case: HEAD contains commit sha
    return {"present": True, "head": "detached", "commit": head}


def collect_repro_info(
    repo_root: str | None = None, *, extra: dict[str, Any] | None = None
) -> dict[str, Any]:
    """
    Captures a compact reproducibility bundle.

    Intentionally avoids huge payloads (pip freeze, full env vars).
    """
    info: dict[str, Any] = {
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


def _ensure_dir(path: str) -> None:
    pathlib.Path(path).mkdir(exist_ok=True, parents=True)


def _json_default(obj: Any) -> Any:
    # Arrow types
    if isinstance(obj, pa.Schema):
        return arrow_schema_to_dict(obj)
    if isinstance(obj, pa.DataType):
        return str(obj)

    # Common containers
    if isinstance(obj, set):
        return sorted(list(obj))
    if isinstance(obj, tuple):
        return list(obj)

    # Dataclasses / objects
    if hasattr(obj, "__dict__"):
        # best-effort shallow dict
        try:
            return {k: _json_default(v) for k, v in obj.__dict__.items()}
        except Exception:
            return str(obj)

    return str(obj)


def _write_json(path: str, data: Any, *, overwrite: bool = True) -> str:
    _ensure_dir(os.path.dirname(path) or ".")
    if overwrite and pathlib.Path(path).exists():
        pathlib.Path(path).unlink()
    with pathlib.Path(path).open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, sort_keys=True, default=_json_default)
    return path


# -----------------------
# Schema + contract + rule snapshots
# -----------------------


def arrow_schema_to_dict(schema: pa.Schema) -> dict[str, Any]:
    return {
        "fields": [
            {"name": f.name, "type": str(f.type), "nullable": bool(f.nullable)} for f in schema
        ]
    }


def schema_fingerprint(schema: pa.Schema) -> str:
    payload = json.dumps(arrow_schema_to_dict(schema), sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def serialize_contract(contract: Any) -> dict[str, Any]:
    """
    Serialize an arrowdsl.contracts.Contract-like object.
    Works with our earlier Contract shape, but is intentionally defensive.
    """
    schema = getattr(contract, "schema", None)
    sch_dict = arrow_schema_to_dict(schema) if isinstance(schema, pa.Schema) else None
    sch_fp = schema_fingerprint(schema) if isinstance(schema, pa.Schema) else None

    dedupe = getattr(contract, "dedupe", None)
    canonical_sort = getattr(contract, "canonical_sort", None)

    def _serialize_sort_keys(keys: Any) -> Any:
        if keys is None:
            return None
        out = []
        for k in keys:
            col = getattr(k, "column", None)
            order = getattr(k, "order", None)
            if col is None and isinstance(k, dict):
                col = k.get("column")
                order = k.get("order")
            out.append({"column": col, "order": order})
        return out

    dedupe_dict = None
    if dedupe is not None:
        dedupe_dict = {
            "keys": list(getattr(dedupe, "keys", []) or []),
            "strategy": getattr(dedupe, "strategy", None),
            "tie_breakers": _serialize_sort_keys(getattr(dedupe, "tie_breakers", None)),
        }

    return {
        "name": getattr(contract, "name", None),
        "version": getattr(contract, "version", None),
        "required_non_null": list(getattr(contract, "required_non_null", []) or []),
        "key_fields": list(getattr(contract, "key_fields", []) or []),
        "schema_fingerprint": sch_fp,
        "schema": sch_dict,
        "dedupe": dedupe_dict,
        "canonical_sort": _serialize_sort_keys(canonical_sort),
    }


def serialize_contract_catalog(contract_catalog: Any) -> dict[str, Any]:
    """
    Serialize a ContractCatalog-like object from relspec/registry.py
    """
    names = []
    if hasattr(contract_catalog, "names"):
        names = list(contract_catalog.names())
    elif hasattr(contract_catalog, "_contracts"):
        names = sorted(list(contract_catalog._contracts.keys()))  # type: ignore

    out = {"contracts": []}
    for name in names:
        c = (
            contract_catalog.get(name)
            if hasattr(contract_catalog, "get")
            else contract_catalog._contracts[name]
        )  # type: ignore
        out["contracts"].append(serialize_contract(c))
    return out


def serialize_relationship_rule(rule: Any) -> dict[str, Any]:
    """
    Serialize a relspec.model.RelationshipRule-like object.
    """

    def _drefs() -> list[str]:
        ins = getattr(rule, "inputs", None) or []
        names = []
        for d in ins:
            nm = getattr(d, "name", None)
            names.append(nm if nm is not None else str(d))
        return names

    return {
        "name": getattr(rule, "name", None),
        "kind": str(getattr(rule, "kind", None)),
        "output_dataset": getattr(rule, "output_dataset", None),
        "contract_name": getattr(rule, "contract_name", None),
        "priority": int(getattr(rule, "priority", 0)),
        "emit_rule_meta": bool(getattr(rule, "emit_rule_meta", True)),
        "inputs": _drefs(),
        # configs (best-effort)
        "hash_join": getattr(
            getattr(rule, "hash_join", None), "__dict__", getattr(rule, "hash_join", None)
        ),
        "interval_align": getattr(
            getattr(rule, "interval_align", None), "__dict__", getattr(rule, "interval_align", None)
        ),
        "project": getattr(
            getattr(rule, "project", None), "__dict__", getattr(rule, "project", None)
        ),
        "post_kernels": [
            getattr(k, "__dict__", str(k)) for k in (getattr(rule, "post_kernels", None) or [])
        ],
    }


def serialize_relationship_registry(registry: Any) -> dict[str, Any]:
    """
    Serialize a RelationshipRegistry-like object from relspec/registry.py
    """
    rules = []
    if hasattr(registry, "rules"):
        rules = registry.rules()
    elif hasattr(registry, "_rules_by_name"):
        rules = list(registry._rules_by_name.values())  # type: ignore

    return {
        "rules": [serialize_relationship_rule(r) for r in rules],
        "outputs": list(registry.outputs()) if hasattr(registry, "outputs") else None,
        "inputs": list(registry.inputs()) if hasattr(registry, "inputs") else None,
    }


def serialize_dataset_locations(locations: Mapping[str, Any]) -> dict[str, Any]:
    """
    Serialize the mapping returned by persist_relspec_input_datasets.
    Supports either DatasetLocation dataclass objects or plain dicts.
    """
    out: dict[str, Any] = {}
    for name, loc in locations.items():
        if isinstance(loc, dict):
            out[name] = dict(loc)
        else:
            out[name] = {
                "path": getattr(loc, "path", None),
                "format": getattr(loc, "format", None),
                "partitioning": getattr(loc, "partitioning", None),
            }
    return out


def serialize_compiled_outputs(compiled: Mapping[str, Any]) -> dict[str, Any]:
    """
    Serialize compiled relationship outputs (CompiledOutput-like objects).
    """
    out: dict[str, Any] = {"outputs": {}}
    for key, obj in compiled.items():
        # CompiledOutput(output_dataset, contract_name, contributors)
        rec = {
            "output_dataset": getattr(obj, "output_dataset", key),
            "contract_name": getattr(obj, "contract_name", None),
            "contributors": [],
        }
        contributors = getattr(obj, "contributors", None) or []
        for cr in contributors:
            rule = getattr(cr, "rule", None)
            rec["contributors"].append(
                {
                    "rule_name": getattr(rule, "name", None),
                    "priority": getattr(rule, "priority", None),
                    "kind": str(getattr(rule, "kind", None)),
                    "inputs": [
                        getattr(d, "name", str(d)) for d in (getattr(rule, "inputs", None) or [])
                    ],
                }
            )
        out["outputs"][key] = rec
    out["produced_output_keys"] = sorted(list(compiled.keys()))
    return out


# -----------------------
# Run bundle writer
# -----------------------


def make_run_bundle_name(run_manifest: Mapping[str, Any], run_config: Mapping[str, Any]) -> str:
    """
    Deterministic-ish bundle name: run_<created_at>_<hash10>
    """
    ts = int(run_manifest.get("created_at_unix_s") or time.time())
    payload = json.dumps(
        {"manifest": run_manifest, "config": run_config}, sort_keys=True, default=str
    ).encode("utf-8")
    h = hashlib.sha256(payload).hexdigest()[:10]
    return f"run_{ts}_{h}"


def write_run_bundle(
    *,
    base_dir: str,
    run_manifest: Mapping[str, Any],
    run_config: Mapping[str, Any],
    # snapshots
    relationship_registry: Any | None = None,
    relationship_contracts: Any | None = None,
    compiled_relationship_outputs: Mapping[str, Any] | None = None,
    # persisted locations (filesystem mode)
    relspec_input_locations: Mapping[str, Any] | None = None,
    # schema snapshots (tables)
    relspec_input_tables: Mapping[str, pa.Table] | None = None,
    relationship_output_tables: Mapping[str, pa.Table] | None = None,
    cpg_output_tables: Mapping[str, pa.Table] | None = None,
    include_schemas: bool = True,
    overwrite: bool = True,
) -> dict[str, Any]:
    """
    Writes a “run bundle” directory capturing enough metadata to replay a run later,
    even if code changes.

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
    """
    _ensure_dir(base_dir)
    bundle_name = make_run_bundle_name(run_manifest, run_config)
    bundle_dir = os.path.join(base_dir, bundle_name)

    if overwrite and pathlib.Path(bundle_dir).exists():
        # remove contents
        for root, dirs, files in os.walk(bundle_dir, topdown=False):
            for fn in files:
                try:
                    pathlib.Path(os.path.join(root, fn)).unlink()
                except Exception:
                    pass
            for dn in dirs:
                try:
                    pathlib.Path(os.path.join(root, dn)).rmdir()
                except Exception:
                    pass

    _ensure_dir(bundle_dir)

    files_written: list[str] = []

    # 1) manifest
    files_written.append(
        _write_json(os.path.join(bundle_dir, "manifest.json"), dict(run_manifest), overwrite=True)
    )

    # 2) config (the values that drove this run)
    files_written.append(
        _write_json(os.path.join(bundle_dir, "config.json"), dict(run_config), overwrite=True)
    )

    # 3) repro info (env, pkg versions, git)
    repo_root = run_config.get("repo_root") if isinstance(run_config, dict) else None
    repro = collect_repro_info(
        repo_root=str(repo_root) if repo_root else None, extra={"bundle_name": bundle_name}
    )
    files_written.append(_write_json(os.path.join(bundle_dir, "repro.json"), repro, overwrite=True))

    # 4) dataset locations (filesystem mode)
    if relspec_input_locations is not None:
        ds_dir = os.path.join(bundle_dir, "datasets")
        _ensure_dir(ds_dir)
        locs = serialize_dataset_locations(relspec_input_locations)
        files_written.append(
            _write_json(os.path.join(ds_dir, "locations.json"), locs, overwrite=True)
        )

    # 5) relationship registry + contracts + compiled outputs snapshots
    relspec_dir = os.path.join(bundle_dir, "relspec")
    _ensure_dir(relspec_dir)

    if relationship_registry is not None:
        snap = serialize_relationship_registry(relationship_registry)
        files_written.append(
            _write_json(os.path.join(relspec_dir, "rules.json"), snap, overwrite=True)
        )

    if relationship_contracts is not None:
        snap = serialize_contract_catalog(relationship_contracts)
        files_written.append(
            _write_json(os.path.join(relspec_dir, "contracts.json"), snap, overwrite=True)
        )

    if compiled_relationship_outputs is not None:
        snap = serialize_compiled_outputs(compiled_relationship_outputs)
        files_written.append(
            _write_json(os.path.join(relspec_dir, "compiled_outputs.json"), snap, overwrite=True)
        )

    # 6) schema snapshots for tables
    if include_schemas:
        schemas_dir = os.path.join(bundle_dir, "schemas")
        _ensure_dir(schemas_dir)

        def _dump_schema(name: str, t: pa.Table) -> None:
            doc = {
                "name": name,
                "rows": int(t.num_rows),
                "schema_fingerprint": schema_fingerprint(t.schema),
                "schema": arrow_schema_to_dict(t.schema),
            }
            files_written.append(
                _write_json(os.path.join(schemas_dir, f"{name}.schema.json"), doc, overwrite=True)
            )

        # relspec input schemas
        if relspec_input_tables:
            for n, t in relspec_input_tables.items():
                if t is not None:
                    _dump_schema(f"relspec_input__{n}", t)

        # relationship output schemas
        if relationship_output_tables:
            for n, t in relationship_output_tables.items():
                if t is not None:
                    _dump_schema(f"relationship_output__{n}", t)

        # cpg output schemas
        if cpg_output_tables:
            for n, t in cpg_output_tables.items():
                if t is not None:
                    _dump_schema(f"cpg_output__{n}", t)

    return {
        "bundle_dir": bundle_dir,
        "bundle_name": bundle_name,
        "files_written": files_written,
    }
