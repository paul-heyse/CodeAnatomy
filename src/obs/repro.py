"""Reproducibility helpers for manifests and run bundles."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import platform
import re
import shutil
import sys
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from importlib import metadata as importlib_metadata
from importlib.metadata import PackageNotFoundError
from pathlib import Path
from typing import cast

import pyarrow as pa
from sqlglot.serde import load as sqlglot_load

from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
from arrowdsl.finalize.finalize import Contract
from arrowdsl.io.ipc import IpcWriteInput, ipc_write_config_payload, write_ipc_bundle
from arrowdsl.json_factory import JsonPolicy, dump_path, dumps_bytes, json_default
from arrowdsl.plan.ops import DedupeSpec, SortKey
from arrowdsl.schema.serialization import schema_fingerprint, schema_to_dict
from arrowdsl.spec.io import IpcWriteConfig, write_spec_table
from core_types import JsonDict, JsonValue, PathLike, ensure_path
from engine.plan_cache import PlanCacheEntry
from engine.plan_product import PlanProduct
from ibis_engine.param_tables import ParamTableArtifact, ParamTableSpec
from ibis_engine.params_bridge import ScalarParamSpec
from obs.parquet_writers import write_obs_dataset
from relspec.compiler import CompiledOutput
from relspec.model import RelationshipRule
from relspec.param_deps import RuleDependencyReport
from relspec.registry import ContractCatalog, DatasetLocation
from relspec.rules.diagnostics import rule_diagnostics_from_table
from schema_spec.system import ddl_fingerprint_from_schema, table_spec_from_schema
from sqlglot_tools.optimizer import planner_dag_snapshot

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
            "datafusion": _pkg_version("datafusion"),
            "ibis-framework": _pkg_version("ibis-framework"),
            "sqlglot": _pkg_version("sqlglot"),
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


def _write_json(path: PathLike, data: JsonValue, *, overwrite: bool = True) -> str:
    target = ensure_path(path)
    policy = JsonPolicy(pretty=True, sort_keys=True)
    return dump_path(target, data, policy=policy, overwrite=overwrite)


# -----------------------
# Schema + contract + rule snapshots
# -----------------------


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
        "ddl_fingerprint": ddl_fingerprint_from_schema(contract.name, contract.schema),
        "schema": cast("JsonValue", schema_to_dict(contract.schema)),
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
        "hash_join": cast("JsonValue", json_default(rule.hash_join))
        if rule.hash_join is not None
        else None,
        "interval_align": cast("JsonValue", json_default(rule.interval_align))
        if rule.interval_align is not None
        else None,
        "winner_select": cast("JsonValue", json_default(rule.winner_select))
        if rule.winner_select is not None
        else None,
        "project": cast("JsonValue", json_default(rule.project))
        if rule.project is not None
        else None,
        "post_kernels": [cast("JsonValue", json_default(k)) for k in rule.post_kernels],
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
        record: JsonDict = {
            "path": str(loc.path),
            "format": loc.format,
            "partitioning": loc.partitioning,
        }
        if loc.files is not None:
            record["files"] = list(loc.files)
        out[name] = record
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
        telemetry = {
            name: {
                "fragment_count": entry.fragment_count,
                "row_group_count": entry.row_group_count,
                "count_rows": entry.count_rows,
                "estimated_rows": entry.estimated_rows,
                "file_hints": list(entry.file_hints),
            }
            for name, entry in obj.telemetry.items()
        }
        payload: JsonDict = {
            "output_dataset": obj.output_dataset,
            "contract_name": obj.contract_name,
            "contributors": contributors,
        }
        if obj.plan_hash is not None:
            payload["plan_hash"] = obj.plan_hash
        if obj.input_datasets:
            payload["input_datasets"] = list(obj.input_datasets)
        if telemetry:
            payload["telemetry"] = telemetry
        outputs[key] = payload
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
    policy = JsonPolicy(sort_keys=True)
    payload = dumps_bytes({"manifest": run_manifest, "config": run_config}, policy=policy)
    h = hashlib.sha256(payload).hexdigest()[:10]
    return f"run_{ts}_{h}"


@dataclass(frozen=True)
class RunBundleContext:
    """Inputs required to write a reproducible run bundle."""

    base_dir: PathLike
    run_manifest: Mapping[str, JsonValue]
    run_config: Mapping[str, JsonValue]

    rule_table: pa.Table | None = None
    template_table: pa.Table | None = None
    template_diagnostics: pa.Table | None = None
    rule_diagnostics: pa.Table | None = None
    relationship_contracts: ContractCatalog | None = None
    compiled_relationship_outputs: Mapping[str, CompiledOutput] | None = None
    datafusion_metrics: JsonDict | None = None
    datafusion_traces: JsonDict | None = None
    datafusion_fallbacks: pa.Table | None = None
    datafusion_explains: pa.Table | None = None
    datafusion_plan_artifacts: pa.Table | None = None
    datafusion_plan_cache: Sequence[PlanCacheEntry] | None = None
    datafusion_cache_events: Sequence[Mapping[str, object]] | None = None
    datafusion_prepared_statements: Sequence[Mapping[str, object]] | None = None
    datafusion_input_plugins: Sequence[Mapping[str, object]] | None = None
    datafusion_arrow_ingest: Sequence[Mapping[str, object]] | None = None
    datafusion_view_registry: Sequence[Mapping[str, object]] | None = None
    datafusion_dml_statements: Sequence[Mapping[str, object]] | None = None
    datafusion_listing_tables: Sequence[Mapping[str, object]] | None = None
    datafusion_delta_tables: Sequence[Mapping[str, object]] | None = None
    datafusion_table_providers: Sequence[Mapping[str, object]] | None = None
    delta_maintenance_reports: Sequence[Mapping[str, object]] | None = None
    datafusion_udf_registry: Sequence[Mapping[str, object]] | None = None
    arrow_kernel_registry: Mapping[str, object] | None = None
    ibis_sql_ingest_artifacts: Sequence[Mapping[str, object]] | None = None
    ibis_namespace_actions: Sequence[Mapping[str, object]] | None = None
    ibis_cache_events: Sequence[Mapping[str, object]] | None = None
    ibis_support_matrix: Sequence[Mapping[str, object]] | None = None
    feature_state: pa.Table | None = None
    relspec_scan_telemetry: pa.Table | None = None
    relspec_rule_exec_events: pa.Table | None = None
    incremental_diff: pa.Table | None = None
    incremental_plan_diff: pa.Table | None = None
    incremental_changed_exports: pa.Table | None = None
    incremental_impacted_callers: pa.Table | None = None
    incremental_impacted_importers: pa.Table | None = None
    incremental_impacted_files: pa.Table | None = None
    incremental_output_fingerprint_changes: pa.Table | None = None

    relspec_input_locations: Mapping[str, DatasetLocation] | None = None
    relspec_input_tables: Mapping[str, TableLike] | None = None
    relationship_output_tables: Mapping[str, TableLike] | None = None
    cpg_output_tables: Mapping[str, TableLike] | None = None
    param_table_specs: Sequence[ParamTableSpec] | None = None
    param_scalar_specs: Sequence[ScalarParamSpec] | None = None
    param_table_artifacts: Mapping[str, ParamTableArtifact] | None = None
    param_scalar_signature: str | None = None
    param_dependency_reports: Sequence[RuleDependencyReport] | None = None
    param_reverse_index: Mapping[str, Sequence[str]] | None = None
    include_param_table_data: bool = False

    include_schemas: bool = True
    ipc_dump_enabled: bool = False
    ipc_write_config: IpcWriteConfig | None = None
    overwrite: bool = True
    allocator_debug: bool = False


def _ensure_bundle_dir(bundle_dir: Path, *, overwrite: bool) -> None:
    if overwrite and bundle_dir.exists():
        with suppress(OSError):
            shutil.rmtree(bundle_dir)
    _ensure_dir(bundle_dir)


@contextmanager
def _allocator_debug_context(*, enabled: bool) -> Iterator[None]:
    if not enabled:
        yield
        return
    pa.log_memory_allocations(enabled=True)
    try:
        yield
    finally:
        pa.log_memory_allocations(enabled=False)


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
    repro = collect_repro_info(
        repo_root=repo_root,
        extra={
            "bundle_name": bundle_name,
            "allocator_debug": bool(context.allocator_debug),
        },
    )
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
    if context.rule_table is not None:
        target = relspec_dir / "rules.arrow"
        write_spec_table(target, context.rule_table)
        files_written.append(str(target))
    if context.template_table is not None:
        target = relspec_dir / "templates.arrow"
        write_spec_table(target, context.template_table)
        files_written.append(str(target))
    if context.template_diagnostics is not None:
        target = relspec_dir / "template_diagnostics.arrow"
        write_spec_table(target, context.template_diagnostics)
        files_written.append(str(target))
    if context.rule_diagnostics is not None:
        target = relspec_dir / "rule_diagnostics.arrow"
        write_spec_table(target, context.rule_diagnostics)
        files_written.append(str(target))
        files_written.append(
            write_obs_dataset(
                relspec_dir,
                name="rule_diagnostics",
                table=context.rule_diagnostics,
                overwrite=context.overwrite,
            )
        )
        _write_sqlglot_ast_payloads(relspec_dir, context.rule_diagnostics, files_written)
        _write_sqlglot_planner_dag(relspec_dir, context.rule_diagnostics, files_written)
        _write_sqlglot_qualification_failures(relspec_dir, context.rule_diagnostics, files_written)
    if context.relationship_contracts is not None:
        snap = serialize_contract_catalog(context.relationship_contracts)
        files_written.append(_write_json(relspec_dir / "contracts.json", snap, overwrite=True))
        ddl = _contract_schema_ddls(context.relationship_contracts)
        if ddl:
            files_written.append(
                _write_json(relspec_dir / "contracts_ddl.json", ddl, overwrite=True)
            )
        asts = _contract_schema_asts(context.relationship_contracts)
        if asts:
            files_written.append(
                _write_json(relspec_dir / "contracts_ast.json", asts, overwrite=True)
            )
    if context.compiled_relationship_outputs is not None:
        snap = serialize_compiled_outputs(context.compiled_relationship_outputs)
        files_written.append(
            _write_json(relspec_dir / "compiled_outputs.json", snap, overwrite=True)
        )
    _write_runtime_artifacts(relspec_dir, context, files_written)
    if context.rule_diagnostics is not None:
        _write_substrait_artifacts(relspec_dir, context.rule_diagnostics, files_written)


def _write_incremental_artifacts(
    bundle_dir: Path,
    context: RunBundleContext,
    files_written: list[str],
) -> None:
    incremental_dir = bundle_dir / "incremental"
    tables: tuple[tuple[str, pa.Table | None], ...] = (
        ("incremental_diff", context.incremental_diff),
        ("incremental_plan_diff", context.incremental_plan_diff),
        ("inc_changed_exports_v1", context.incremental_changed_exports),
        ("inc_impacted_callers_v1", context.incremental_impacted_callers),
        ("inc_impacted_importers_v1", context.incremental_impacted_importers),
        ("inc_impacted_files_v2", context.incremental_impacted_files),
        (
            "inc_output_fingerprint_changes_v1",
            context.incremental_output_fingerprint_changes,
        ),
    )
    if not any(table is not None for _, table in tables):
        return
    _ensure_dir(incremental_dir)
    for name, table in tables:
        if table is None:
            continue
        files_written.append(
            write_obs_dataset(
                incremental_dir,
                name=name,
                table=table,
                overwrite=context.overwrite,
            )
        )


def _write_runtime_artifacts(
    relspec_dir: Path,
    context: RunBundleContext,
    files_written: list[str],
) -> None:
    def _json_list(entries: Sequence[object]) -> list[JsonValue]:
        return [cast("JsonValue", json_default(entry)) for entry in entries]

    json_artifacts = [
        ("datafusion_metrics.json", context.datafusion_metrics),
        ("datafusion_traces.json", context.datafusion_traces),
        (
            "arrow_kernel_registry.json",
            cast("JsonValue", json_default(context.arrow_kernel_registry))
            if context.arrow_kernel_registry
            else None,
        ),
    ]
    list_artifacts: list[tuple[str, str, list[JsonValue] | None]] = [
        (
            "datafusion_input_plugins.json",
            "plugins",
            _json_list(context.datafusion_input_plugins)
            if context.datafusion_input_plugins
            else None,
        ),
        (
            "datafusion_arrow_ingest.json",
            "ingest",
            _json_list(context.datafusion_arrow_ingest)
            if context.datafusion_arrow_ingest
            else None,
        ),
        (
            "datafusion_view_registry.json",
            "views",
            _json_list(context.datafusion_view_registry)
            if context.datafusion_view_registry
            else None,
        ),
        (
            "datafusion_cache_events.json",
            "events",
            _json_list(context.datafusion_cache_events)
            if context.datafusion_cache_events
            else None,
        ),
        (
            "datafusion_prepared_statements.json",
            "statements",
            _json_list(context.datafusion_prepared_statements)
            if context.datafusion_prepared_statements
            else None,
        ),
        (
            "datafusion_dml_statements.json",
            "statements",
            _json_list(context.datafusion_dml_statements)
            if context.datafusion_dml_statements
            else None,
        ),
        (
            "datafusion_listing_tables.json",
            "registrations",
            _json_list(context.datafusion_listing_tables)
            if context.datafusion_listing_tables
            else None,
        ),
        (
            "datafusion_delta_tables.json",
            "registrations",
            _json_list(context.datafusion_delta_tables)
            if context.datafusion_delta_tables
            else None,
        ),
        (
            "datafusion_table_providers.json",
            "providers",
            _json_list(context.datafusion_table_providers)
            if context.datafusion_table_providers
            else None,
        ),
        (
            "delta_maintenance_reports.json",
            "reports",
            _json_list(context.delta_maintenance_reports)
            if context.delta_maintenance_reports
            else None,
        ),
        (
            "datafusion_udf_registry.json",
            "udfs",
            _json_list(context.datafusion_udf_registry)
            if context.datafusion_udf_registry
            else None,
        ),
        (
            "ibis_sql_ingest_artifacts.json",
            "artifacts",
            _json_list(context.ibis_sql_ingest_artifacts)
            if context.ibis_sql_ingest_artifacts
            else None,
        ),
        (
            "ibis_namespace_actions.json",
            "actions",
            _json_list(context.ibis_namespace_actions) if context.ibis_namespace_actions else None,
        ),
        (
            "ibis_cache_events.json",
            "events",
            _json_list(context.ibis_cache_events) if context.ibis_cache_events else None,
        ),
        (
            "ibis_support_matrix.json",
            "entries",
            _json_list(context.ibis_support_matrix) if context.ibis_support_matrix else None,
        ),
    ]
    table_artifacts = [
        ("datafusion_fallbacks", context.datafusion_fallbacks),
        ("datafusion_explains", context.datafusion_explains),
        ("datafusion_plan_artifacts_v1", context.datafusion_plan_artifacts),
        ("feature_state", context.feature_state),
        ("scan_telemetry", context.relspec_scan_telemetry),
        ("rule_exec_events", context.relspec_rule_exec_events),
    ]
    for filename, payload in json_artifacts:
        if payload is None:
            continue
        files_written.append(_write_json(relspec_dir / filename, payload, overwrite=True))
    for filename, key, entries in list_artifacts:
        if not entries:
            continue
        files_written.append(
            _write_json(
                relspec_dir / filename,
                {key: entries},
                overwrite=True,
            )
        )
    for name, table in table_artifacts:
        if table is None:
            continue
        files_written.append(
            write_obs_dataset(
                relspec_dir,
                name=name,
                table=table,
                overwrite=context.overwrite,
            )
        )
    if context.datafusion_plan_cache:
        _write_plan_cache_artifacts(
            relspec_dir,
            entries=context.datafusion_plan_cache,
            files_written=files_written,
        )


def _contract_schema_ddls(contracts: ContractCatalog) -> JsonDict:
    """Return SQL DDL statements for contract schemas.

    Returns
    -------
    JsonDict
        Mapping of contract names to CREATE TABLE statements.
    """
    payload: JsonDict = {}
    for name in contracts.names():
        contract = contracts.get(name)
        spec = table_spec_from_schema(name, contract.schema)
        payload[name] = spec.to_create_table_sql(dialect="datafusion")
    return payload


def _contract_schema_asts(contracts: ContractCatalog) -> JsonDict:
    """Return SQLGlot column-def ASTs for contract schemas.

    Returns
    -------
    JsonDict
        Mapping of contract names to SQLGlot column definitions.
    """
    payload: JsonDict = {}
    for name in contracts.names():
        contract = contracts.get(name)
        spec = table_spec_from_schema(name, contract.schema)
        column_defs = spec.to_sqlglot_column_defs(dialect="datafusion")
        payload[name] = [repr(column) for column in column_defs]
    return payload


def _safe_label(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("_")
    return sanitized or "unknown"


def _substrait_filename(
    diagnostic: object,
    *,
    used: set[str],
) -> str:
    name = getattr(diagnostic, "rule_name", None) or getattr(diagnostic, "template", None) or ""
    domain = getattr(diagnostic, "domain", None) or "unknown"
    signature = getattr(diagnostic, "plan_signature", None) or "no_signature"
    base = _safe_label(f"{domain}__{name}__{signature}")
    candidate = base or "substrait"
    if candidate not in used:
        used.add(candidate)
        return f"{candidate}.substrait"
    idx = 1
    while f"{candidate}_{idx}" in used:
        idx += 1
    used.add(f"{candidate}_{idx}")
    return f"{candidate}_{idx}.substrait"


def _plan_cache_filename(entry: PlanCacheEntry) -> str:
    base = _safe_label(f"{entry.plan_hash}__{entry.profile_hash}")
    return f"{base}.substrait"


def _write_plan_cache_artifacts(
    relspec_dir: Path,
    *,
    entries: Sequence[PlanCacheEntry],
    files_written: list[str],
) -> None:
    if not entries:
        return
    cache_dir = relspec_dir / "substrait_cache"
    _ensure_dir(cache_dir)
    used: set[str] = set()
    index: list[JsonDict] = []
    for entry in entries:
        base = _plan_cache_filename(entry)
        filename = base
        if filename in used:
            idx = 1
            while f"{base}_{idx}" in used:
                idx += 1
            filename = f"{base}_{idx}"
        used.add(filename)
        target = cache_dir / filename
        target.write_bytes(entry.plan_bytes)
        files_written.append(str(target))
        index.append(
            {
                "plan_hash": entry.plan_hash,
                "profile_hash": entry.profile_hash,
                "file": str(target.relative_to(relspec_dir)),
            }
        )
    files_written.append(
        _write_json(
            relspec_dir / "substrait_cache_index.json",
            {"plans": index},
            overwrite=True,
        )
    )


def _write_substrait_artifacts(
    relspec_dir: Path,
    diagnostics_table: pa.Table,
    files_written: list[str],
) -> None:
    diagnostics = rule_diagnostics_from_table(diagnostics_table)
    substrait_dir = relspec_dir / "substrait"
    used: set[str] = set()
    index: list[JsonDict] = []
    for diagnostic in diagnostics:
        payload_b64 = diagnostic.metadata.get("substrait_plan_b64")
        if not payload_b64:
            continue
        try:
            payload = base64.b64decode(payload_b64)
        except (binascii.Error, ValueError):
            continue
        if not payload:
            continue
        _ensure_dir(substrait_dir)
        filename = _substrait_filename(diagnostic, used=used)
        target = substrait_dir / filename
        target.write_bytes(payload)
        files_written.append(str(target))
        index.append(
            {
                "file": str(target.relative_to(relspec_dir)),
                "domain": str(diagnostic.domain),
                "rule_name": diagnostic.rule_name,
                "template": diagnostic.template,
                "plan_signature": diagnostic.plan_signature,
            }
        )
    if index:
        files_written.append(
            _write_json(
                relspec_dir / "substrait_index.json",
                {"plans": index},
                overwrite=True,
            )
        )


def _write_sqlglot_ast_payloads(
    relspec_dir: Path,
    diagnostics_table: pa.Table,
    files_written: list[str],
) -> None:
    diagnostics = rule_diagnostics_from_table(diagnostics_table)
    payloads: list[JsonDict] = []
    for diagnostic in diagnostics:
        metadata = diagnostic.metadata
        raw_payload = metadata.get("ast_payload_raw")
        optimized_payload = metadata.get("ast_payload_optimized")
        if not raw_payload or not optimized_payload:
            continue
        try:
            raw_ast = json.loads(raw_payload)
            optimized_ast = json.loads(optimized_payload)
        except (json.JSONDecodeError, TypeError):
            continue
        payloads.append(
            {
                "domain": str(diagnostic.domain),
                "rule_name": diagnostic.rule_name,
                "plan_signature": diagnostic.plan_signature,
                "plan_fingerprint": metadata.get("plan_fingerprint"),
                "sqlglot_policy_hash": metadata.get("sqlglot_policy_hash"),
                "normalization_distance": metadata.get("normalization_distance"),
                "normalization_max_distance": metadata.get("normalization_max_distance"),
                "normalization_applied": metadata.get("normalization_applied"),
                "raw_ast": raw_ast,
                "optimized_ast": optimized_ast,
            }
        )
    if payloads:
        files_written.append(
            _write_json(
                relspec_dir / "sqlglot_ast_payloads.json",
                {"payloads": payloads},
                overwrite=True,
            )
        )


def _write_sqlglot_planner_dag(
    relspec_dir: Path,
    diagnostics_table: pa.Table,
    files_written: list[str],
) -> None:
    diagnostics = rule_diagnostics_from_table(diagnostics_table)
    payloads: list[JsonDict] = []
    for diagnostic in diagnostics:
        metadata = diagnostic.metadata
        optimized_payload = metadata.get("ast_payload_optimized")
        if not optimized_payload:
            continue
        try:
            optimized_ast = json.loads(optimized_payload)
        except (json.JSONDecodeError, TypeError):
            continue
        try:
            expr = sqlglot_load(optimized_ast)
        except (TypeError, ValueError):
            continue
        dag = planner_dag_snapshot(expr)
        steps = [
            {str(key): cast("JsonValue", json_default(value)) for key, value in row.items()}
            for row in dag.steps
        ]
        edges = [
            {str(key): cast("JsonValue", json_default(value)) for key, value in row.items()}
            for row in dag.edges
        ]
        payloads.append(
            {
                "domain": str(diagnostic.domain),
                "rule_name": diagnostic.rule_name,
                "plan_signature": diagnostic.plan_signature,
                "plan_fingerprint": metadata.get("plan_fingerprint"),
                "sqlglot_policy_hash": metadata.get("sqlglot_policy_hash"),
                "planner_dag_hash": dag.dag_hash,
                "steps": steps,
                "edges": edges,
            }
        )
    if payloads:
        files_written.append(
            _write_json(
                relspec_dir / "sqlglot_planner_dag.json",
                {"payloads": payloads},
                overwrite=True,
            )
        )


def _write_sqlglot_qualification_failures(
    relspec_dir: Path,
    diagnostics_table: pa.Table,
    files_written: list[str],
) -> None:
    diagnostics = rule_diagnostics_from_table(diagnostics_table)
    payloads: list[JsonDict] = []
    for diagnostic in diagnostics:
        payload_raw = diagnostic.metadata.get("qualification_payload")
        if not payload_raw:
            continue
        try:
            payload = json.loads(payload_raw)
        except (json.JSONDecodeError, TypeError):
            continue
        payloads.append(
            {
                "domain": str(diagnostic.domain),
                "rule_name": diagnostic.rule_name,
                "plan_signature": diagnostic.plan_signature,
                "payload": payload,
            }
        )
    if payloads:
        files_written.append(
            _write_json(
                relspec_dir / "sqlglot_qualification_failures.json",
                {"payloads": payloads},
                overwrite=True,
            )
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
        "ddl_fingerprint": ddl_fingerprint_from_schema(name, table.schema),
        "schema": cast("JsonValue", schema_to_dict(table.schema)),
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


def _ipc_schema(table: IpcWriteInput) -> pa.Schema:
    data = table.value() if isinstance(table, PlanProduct) else table
    if isinstance(data, RecordBatchReaderLike):
        return data.schema
    return data.schema


def _ipc_metadata_payload(
    name: str,
    *,
    table: IpcWriteInput,
    config: IpcWriteConfig | None,
) -> JsonDict:
    schema = _ipc_schema(table)
    return {
        "name": name,
        "schema": schema_to_dict(schema),
        "schema_fingerprint": schema_fingerprint(schema),
        "ddl_fingerprint": ddl_fingerprint_from_schema(name, schema),
        "ipc_options": ipc_write_config_payload(config),
    }


def _write_ipc_group(
    bundle_dir: Path,
    *,
    prefix: str,
    tables: Mapping[str, IpcWriteInput] | None,
    context: RunBundleContext,
    files_written: list[str],
) -> None:
    if not context.ipc_dump_enabled or not tables:
        return
    ipc_dir = bundle_dir / "ipc" / prefix
    _ensure_dir(ipc_dir)
    for name, table in tables.items():
        if table is None:
            continue
        base_path = ipc_dir / name
        artifacts = write_ipc_bundle(
            table,
            base_path,
            overwrite=context.overwrite,
            config=context.ipc_write_config,
        )
        payload = _ipc_metadata_payload(
            name,
            table=table,
            config=context.ipc_write_config,
        )
        payload["artifacts"] = artifacts
        files_written.append(_write_json(base_path.with_suffix(".json"), payload, overwrite=True))
        files_written.extend(artifacts.values())


def _write_ipc_dumps(
    bundle_dir: Path,
    context: RunBundleContext,
    files_written: list[str],
) -> None:
    _write_ipc_group(
        bundle_dir,
        prefix="relspec_input",
        tables=context.relspec_input_tables,
        context=context,
        files_written=files_written,
    )
    _write_ipc_group(
        bundle_dir,
        prefix="relationship_output",
        tables=context.relationship_output_tables,
        context=context,
        files_written=files_written,
    )
    _write_ipc_group(
        bundle_dir,
        prefix="cpg_output",
        tables=context.cpg_output_tables,
        context=context,
        files_written=files_written,
    )


def _write_param_tables(
    bundle_dir: Path,
    context: RunBundleContext,
    files_written: list[str],
) -> None:
    if not _param_tables_present(context):
        return
    params_dir = bundle_dir / "params"
    _ensure_dir(params_dir)
    _write_param_specs(params_dir, context=context, files_written=files_written)
    _write_param_signatures(params_dir, context=context, files_written=files_written)
    _write_param_table_data(params_dir, context=context, files_written=files_written)
    _write_param_dependency_reports(params_dir, context=context, files_written=files_written)
    _write_param_reverse_index(params_dir, context=context, files_written=files_written)


def _param_tables_present(context: RunBundleContext) -> bool:
    return bool(
        context.param_table_specs
        or context.param_scalar_specs
        or context.param_table_artifacts
        or context.param_scalar_signature is not None
    )


def _write_param_specs(
    params_dir: Path,
    *,
    context: RunBundleContext,
    files_written: list[str],
) -> None:
    if not context.param_table_specs and not context.param_scalar_specs:
        return
    specs_payload = {
        "specs": [_param_spec_payload(spec) for spec in context.param_table_specs or ()],
        "scalar_specs": [
            _scalar_param_spec_payload(spec) for spec in context.param_scalar_specs or ()
        ],
    }
    files_written.append(_write_json(params_dir / "specs.json", specs_payload, overwrite=True))


def _write_param_signatures(
    params_dir: Path,
    *,
    context: RunBundleContext,
    files_written: list[str],
) -> None:
    signatures: dict[str, JsonDict] = {}
    if context.param_scalar_signature:
        signatures["_scalar_signature"] = {"signature": context.param_scalar_signature}
    if context.param_table_artifacts:
        for name, artifact in context.param_table_artifacts.items():
            signatures[name] = {
                "rows": int(artifact.rows),
                "signature": artifact.signature,
                "schema_fingerprint": artifact.schema_fingerprint,
                "ddl_fingerprint": ddl_fingerprint_from_schema(name, artifact.table.schema),
            }
    if not signatures:
        return
    files_written.append(_write_json(params_dir / "signatures.json", signatures, overwrite=True))


def _write_param_table_data(
    params_dir: Path,
    *,
    context: RunBundleContext,
    files_written: list[str],
) -> None:
    if not context.param_table_artifacts or not context.include_param_table_data:
        return
    for name, artifact in context.param_table_artifacts.items():
        files_written.append(
            write_obs_dataset(
                params_dir,
                name=name,
                table=artifact.table,
                overwrite=context.overwrite,
            )
        )


def _write_param_dependency_reports(
    params_dir: Path,
    *,
    context: RunBundleContext,
    files_written: list[str],
) -> None:
    if not context.param_dependency_reports:
        return
    deps_payload = {
        report.rule_name: {
            "param_tables": list(report.param_tables),
            "dataset_tables": list(report.dataset_tables),
        }
        for report in context.param_dependency_reports
    }
    files_written.append(
        _write_json(params_dir / "rule_param_deps.json", deps_payload, overwrite=True)
    )


def _write_param_reverse_index(
    params_dir: Path,
    *,
    context: RunBundleContext,
    files_written: list[str],
) -> None:
    if not context.param_reverse_index:
        return
    reverse_payload = {name: list(rules) for name, rules in context.param_reverse_index.items()}
    files_written.append(
        _write_json(params_dir / "param_rule_reverse_index.json", reverse_payload, overwrite=True)
    )


def _param_spec_payload(spec: ParamTableSpec) -> JsonDict:
    return {
        "logical_name": spec.logical_name,
        "key_col": spec.key_col,
        "schema": schema_to_dict(spec.schema),
        "empty_semantics": spec.empty_semantics,
        "distinct": bool(spec.distinct),
    }


def _scalar_param_spec_payload(spec: ScalarParamSpec) -> JsonDict:
    return {
        "name": spec.name,
        "dtype": spec.dtype,
        "default": cast("JsonValue", json_default(spec.default)),
        "required": bool(spec.required),
    }


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
        relspec/rules.arrow
        relspec/templates.arrow
        relspec/template_diagnostics.arrow
        relspec/rule_diagnostics.arrow
        relspec/rule_diagnostics/
        relspec/datafusion_metrics.json
        relspec/datafusion_traces.json
        relspec/datafusion_fallbacks/
        relspec/datafusion_explains/
        relspec/datafusion_plan_artifacts_v1/
        relspec/datafusion_input_plugins.json
        relspec/datafusion_arrow_ingest.json
        relspec/datafusion_view_registry.json
        relspec/datafusion_cache_events.json
        relspec/datafusion_prepared_statements.json
        relspec/datafusion_dml_statements.json
        relspec/datafusion_listing_tables.json
        relspec/datafusion_delta_tables.json
        relspec/datafusion_table_providers.json
        relspec/datafusion_udf_registry.json
        relspec/arrow_kernel_registry.json
        relspec/substrait_cache/
        relspec/substrait_cache_index.json
        relspec/ibis_sql_ingest_artifacts.json
        relspec/ibis_namespace_actions.json
        relspec/ibis_cache_events.json
        relspec/ibis_support_matrix.json
        relspec/sqlglot_planner_dag.json
        relspec/sqlglot_qualification_failures.json
        relspec/contracts.json
        relspec/contracts_ddl.json
        relspec/compiled_outputs.json
        relspec/scan_telemetry/
        relspec/rule_exec_events/
        incremental/incremental_diff/
        incremental/incremental_plan_diff/
        incremental/inc_output_fingerprint_changes_v1/
        relspec/substrait/*.substrait
        relspec/substrait_index.json
        schemas/*.schema.json
        ipc/<group>/<name>.arrow
        ipc/<group>/<name>.arrows
        ipc/<group>/<name>.json
        params/specs.json
        params/signatures.json
        params/rule_param_deps.json
        params/param_rule_reverse_index.json
        params/<table_name>/

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

    with _allocator_debug_context(enabled=context.allocator_debug):
        _write_manifest_files(
            bundle_dir,
            context,
            bundle_name=bundle_name,
            files_written=files_written,
        )
        _write_dataset_locations(bundle_dir, context, files_written)
        _write_relspec_snapshots(bundle_dir, context, files_written)
        _write_incremental_artifacts(bundle_dir, context, files_written)
        _write_schema_snapshots(bundle_dir, context, files_written)
        _write_ipc_dumps(bundle_dir, context, files_written)
        _write_param_tables(bundle_dir, context, files_written)

    return {
        "bundle_dir": str(bundle_dir),
        "bundle_name": bundle_name,
        "files_written": files_written,
    }
