"""Planning-environment and rulepack snapshot helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

from datafusion import SessionContext

from datafusion_engine.plan.bundle_environment import (
    capture_udf_metadata_for_plan as _capture_udf_metadata_for_plan,
)
from datafusion_engine.plan.bundle_environment import (
    function_registry_hash as _function_registry_hash,
)
from datafusion_engine.plan.plan_utils import extract_rule_names, suppress_runtime_errors
from datafusion_engine.schema.introspection_core import SchemaIntrospector
from utils.hashing import hash_msgpack_canonical

if TYPE_CHECKING:
    from datafusion_engine.session.runtime_session import SessionRuntime


def session_config_snapshot(ctx: SessionContext) -> Mapping[str, object]:
    """Capture raw DataFusion session config payload when available.

    Returns:
        Mapping[str, object]: Session config mapping payload.
    """
    config_method = getattr(ctx, "config", None)
    if not callable(config_method):
        return {}
    try:
        config = config_method()
    except (RuntimeError, TypeError, ValueError):
        return {}
    to_dict = getattr(config, "to_dict", None)
    if not callable(to_dict):
        return {}
    try:
        payload = to_dict()
    except (RuntimeError, TypeError, ValueError):
        return {}
    if isinstance(payload, Mapping):
        return dict(payload)
    return {}


def _delta_protocol_support_payload(
    support: object | None,
) -> Mapping[str, object] | None:
    if support is None:
        return None
    max_reader = getattr(support, "max_reader_version", None)
    max_writer = getattr(support, "max_writer_version", None)
    reader_features = getattr(support, "supported_reader_features", ())
    writer_features = getattr(support, "supported_writer_features", ())
    return {
        "max_reader_version": max_reader,
        "max_writer_version": max_writer,
        "supported_reader_features": list(reader_features),
        "supported_writer_features": list(writer_features),
    }


def planning_env_snapshot(
    session_runtime: SessionRuntime | None,
) -> Mapping[str, object]:
    """Return planning-relevant environment settings for determinism."""
    if session_runtime is None:
        return {}
    profile = session_runtime.profile
    session_config = session_config_snapshot(session_runtime.ctx)
    sql_policy_payload = None
    if profile.policies.sql_policy is not None:
        sql_policy_payload = {
            "allow_ddl": profile.policies.sql_policy.allow_ddl,
            "allow_dml": profile.policies.sql_policy.allow_dml,
            "allow_statements": profile.policies.sql_policy.allow_statements,
        }
    schema_hardening = profile.policies.schema_hardening
    return {
        "datafusion_version": getattr(profile, "datafusion_version", None),
        "session_config": session_config,
        "settings_payload": profile.settings_payload(),
        "settings_hash": profile.settings_hash(),
        "sql_policy_name": profile.policies.sql_policy_name,
        "sql_policy": sql_policy_payload,
        "explain_controls": {
            "capture_explain": profile.diagnostics.capture_explain,
            "explain_verbose": profile.diagnostics.explain_verbose,
            "explain_analyze": profile.diagnostics.explain_analyze,
            "explain_analyze_level": profile.diagnostics.explain_analyze_level,
        },
        "execution": {
            "target_partitions": profile.execution.target_partitions,
            "batch_size": profile.execution.batch_size,
            "repartition_aggregations": profile.execution.repartition_aggregations,
            "repartition_windows": profile.execution.repartition_windows,
            "repartition_file_scans": profile.execution.repartition_file_scans,
            "repartition_file_min_size": profile.execution.repartition_file_min_size,
        },
        "runtime_env": {
            "spill_dir": profile.execution.spill_dir,
            "memory_pool": profile.execution.memory_pool,
            "memory_limit_bytes": profile.execution.memory_limit_bytes,
            "enable_cache_manager": profile.features.enable_cache_manager,
        },
        "async_udf": {
            "enable_async_udfs": profile.features.enable_async_udfs,
            "async_udf_timeout_ms": profile.policies.async_udf_timeout_ms,
            "async_udf_batch_size": profile.policies.async_udf_batch_size,
        },
        "delta_protocol": {
            "mode": profile.policies.delta_protocol_mode,
            "support": _delta_protocol_support_payload(profile.policies.delta_protocol_support),
        },
        "schema_hardening": {
            "explain_format": schema_hardening.explain_format if schema_hardening else None,
            "enable_view_types": schema_hardening.enable_view_types if schema_hardening else None,
        },
    }


def planning_env_hash(snapshot: Mapping[str, object]) -> str:
    """Return a deterministic hash for planning environment snapshots."""
    return hash_msgpack_canonical(snapshot)


def rulepack_snapshot(ctx: SessionContext) -> Mapping[str, object] | None:
    """Capture planner rulepack metadata when available.

    Returns:
        Mapping[str, object] | None: Rulepack metadata payload.
    """
    containers: list[object] = [ctx]
    for attr in ("state", "session_state"):
        candidate = getattr(ctx, attr, None)
        if callable(candidate):
            with suppress_runtime_errors():
                candidate = candidate()
        if candidate is not None:
            containers.append(candidate)
    snapshot: dict[str, object] = {}
    for container in containers:
        analyzer = extract_rule_names(container, "analyzer_rules")
        optimizer = extract_rule_names(container, "optimizer_rules")
        physical = extract_rule_names(container, "physical_optimizer_rules")
        if analyzer is not None:
            snapshot["analyzer_rules"] = analyzer
        if optimizer is not None:
            snapshot["optimizer_rules"] = optimizer
        if physical is not None:
            snapshot["physical_optimizer_rules"] = physical
    if snapshot:
        snapshot["status"] = "ok"
        snapshot["source"] = type(containers[-1]).__name__
        return snapshot
    return {"status": "unavailable", "reason": "rulepack APIs not exposed"}


def rulepack_hash(snapshot: Mapping[str, object] | None) -> str | None:
    """Return hash for rulepack snapshots when available."""
    if not snapshot:
        return None
    return hash_msgpack_canonical(snapshot)


def function_registry_artifacts(
    ctx: SessionContext,
    *,
    session_runtime: SessionRuntime | None,
) -> str:
    """Return stable function-registry hash for plan artifact fingerprints."""
    if not _capture_udf_metadata_for_plan(session_runtime):
        return _function_registry_hash({"functions": []})

    functions: Sequence[Mapping[str, object]] = ()
    try:
        sql_options = None
        if session_runtime is not None:
            try:
                from datafusion_engine.sql.options import planning_sql_options

                sql_options = planning_sql_options(session_runtime.profile)
            except (RuntimeError, TypeError, ValueError, ImportError):
                sql_options = None
        introspector = SchemaIntrospector(ctx, sql_options=sql_options)
        functions = introspector.function_catalog_snapshot(include_parameters=True)
    except (RuntimeError, TypeError, ValueError, Warning):
        functions = ()
    snapshot: Mapping[str, object] = {"functions": list(functions)}
    return _function_registry_hash(snapshot)


__all__ = [
    "function_registry_artifacts",
    "planning_env_hash",
    "planning_env_snapshot",
    "rulepack_hash",
    "rulepack_snapshot",
    "session_config_snapshot",
]
