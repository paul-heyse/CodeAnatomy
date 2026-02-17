"""Plan-details diagnostics builders for plan artifacts."""

from __future__ import annotations

import contextlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field

import pyarrow as pa
from datafusion import DataFrame

from datafusion_engine.arrow.interop import arrow_schema_from_df
from datafusion_engine.identity import schema_identity_hash
from datafusion_engine.plan.profiler import ExplainCapture
from serde_artifacts import PlanArtifacts, PlanProtoStatus
from serde_msgspec import to_builtins
from serde_schema_registry import schema_contract_hash
from utils.hashing import hash_settings


@dataclass(frozen=True)
class PlanDetailContext:
    """Optional inputs for plan detail diagnostics."""

    cdf_windows: Sequence[Mapping[str, object]] = ()
    delta_store_policy_hash: str | None = None
    information_schema_hash: str | None = None
    snapshot_keys: Sequence[Mapping[str, object]] = ()
    write_outcomes: Sequence[Mapping[str, object]] = ()


@dataclass(frozen=True)
class PlanDetailInputs:
    """Inputs used to assemble plan detail diagnostics."""

    artifacts: PlanArtifacts
    plan_fingerprint: str
    logical: object | None
    optimized: object | None
    execution: object | None
    explain_tree: ExplainCapture | None
    explain_verbose: ExplainCapture | None
    explain_analyze: ExplainCapture | None
    substrait_validation: Mapping[str, object] | None = None
    proto_status: PlanProtoStatus | None = None
    detail_context: PlanDetailContext | None = None


@dataclass(frozen=True)
class _PlanDisplaySection:
    """Display-oriented plan details and physical plan text."""

    payload: dict[str, object]
    physical_plan: str | None


@dataclass
class _PlanDetailsBuilder:
    """Mutable builder for plan detail payload assembly."""

    details: dict[str, object] = field(default_factory=dict)

    def add_section(self, section: Mapping[str, object]) -> None:
        self.details.update(section)

    def build(self) -> dict[str, object]:
        return dict(self.details)


def collect_plan_details(
    df: DataFrame,
    *,
    detail_inputs: PlanDetailInputs,
) -> dict[str, object]:
    """Collect plan details for diagnostics.

    Returns:
        dict[str, object]: Serialized plan-details payload.
    """
    context = detail_inputs.detail_context or PlanDetailContext()
    display_section = _plan_display_section(detail_inputs)
    builder = _PlanDetailsBuilder()
    builder.add_section(display_section.payload)
    builder.add_section(
        _plan_statistics_section(
            execution=detail_inputs.execution,
            physical_plan=display_section.physical_plan,
        )
    )
    builder.add_section(_plan_explain_section(detail_inputs))
    builder.add_section(_plan_schema_section(df))
    builder.add_section(_plan_context_section(context))
    details = builder.build()
    details["plan_manifest"] = _plan_manifest_payload(
        detail_inputs=detail_inputs,
        details=details,
        context=context,
    )
    details["determinism_audit"] = _determinism_audit_bundle(
        detail_inputs,
        context=context,
    )
    return details


def _plan_display(plan: object | None, *, method: str) -> str | None:
    if plan is None:
        return None
    if isinstance(plan, str):
        return plan
    display_method = getattr(plan, method, None)
    if callable(display_method):
        try:
            return str(display_method())
        except (RuntimeError, TypeError, ValueError):
            return None
    return str(plan)


def _plan_pgjson(plan: object | None) -> str | None:
    if plan is None:
        return None
    method = getattr(plan, "display_pgjson", None)
    if not callable(method):
        method = getattr(plan, "display_pg_json", None)
        if not callable(method):
            return None
    try:
        return str(method())
    except (RuntimeError, TypeError, ValueError):
        return None


def _plan_display_section(detail_inputs: PlanDetailInputs) -> _PlanDisplaySection:
    physical_plan = _plan_display(
        detail_inputs.execution,
        method="display_indent",
    )
    return _PlanDisplaySection(
        payload={
            "logical_plan": _plan_display(
                detail_inputs.logical,
                method="display_indent_schema",
            ),
            "optimized_plan": _plan_display(
                detail_inputs.optimized,
                method="display_indent_schema",
            ),
            "physical_plan": physical_plan,
            "graphviz": _plan_graphviz(detail_inputs.optimized),
            "optimized_plan_pgjson": _plan_pgjson(detail_inputs.optimized),
        },
        physical_plan=physical_plan,
    )


def _plan_statistics_section(
    *,
    execution: object | None,
    physical_plan: str | None,
) -> dict[str, object]:
    section: dict[str, object] = {
        "partition_count": _plan_partition_count(execution),
        "repartition_count": _repartition_count_from_display(physical_plan),
        "dynamic_filter_count": _dynamic_filter_count_from_display(physical_plan),
    }
    stats_payload = _plan_statistics_payload(execution)
    if stats_payload is not None:
        section["statistics"] = stats_payload
    return section


def _plan_explain_section(detail_inputs: PlanDetailInputs) -> dict[str, object]:
    section: dict[str, object] = {}
    if detail_inputs.explain_tree is not None:
        section["explain_tree"] = detail_inputs.explain_tree.text
    if detail_inputs.explain_verbose is not None:
        section["explain_verbose"] = detail_inputs.explain_verbose.text
    if detail_inputs.explain_analyze is not None:
        section["explain_analyze"] = detail_inputs.explain_analyze.text
        section["explain_analyze_duration_ms"] = detail_inputs.explain_analyze.duration_ms
        section["explain_analyze_output_rows"] = detail_inputs.explain_analyze.output_rows
    if detail_inputs.substrait_validation is not None:
        section["substrait_validation"] = detail_inputs.substrait_validation
    if detail_inputs.proto_status is not None:
        section["proto_serialization"] = to_builtins(
            detail_inputs.proto_status,
            str_keys=True,
        )
    return section


def _plan_schema_section(df: DataFrame) -> dict[str, object]:
    schema_names: list[str] = list(df.schema().names) if hasattr(df.schema(), "names") else []
    return {
        "schema_names": schema_names,
        "schema_describe": _schema_describe_rows(df),
        "schema_provenance": _schema_provenance(df),
    }


def _plan_context_section(context: PlanDetailContext) -> dict[str, object]:
    section: dict[str, object] = {}
    if context.cdf_windows:
        section["cdf_windows"] = [dict(window) for window in context.cdf_windows]
    if context.delta_store_policy_hash is not None:
        section["delta_store_policy_hash"] = context.delta_store_policy_hash
    if context.information_schema_hash is not None:
        section["information_schema_hash"] = context.information_schema_hash
    if context.snapshot_keys:
        section["snapshot_keys"] = [dict(key) for key in context.snapshot_keys]
    if context.write_outcomes:
        section["write_outcomes"] = [dict(outcome) for outcome in context.write_outcomes]
    return section


def _plan_manifest_payload(
    *,
    detail_inputs: PlanDetailInputs,
    details: Mapping[str, object],
    context: PlanDetailContext,
) -> dict[str, object]:
    settings = {
        str(key): str(value) for key, value in sorted(detail_inputs.artifacts.df_settings.items())
    }
    return {
        "version": 1,
        "plan_fingerprint": detail_inputs.plan_fingerprint,
        "logical_plan_text": details.get("logical_plan"),
        "optimized_plan_text": details.get("optimized_plan"),
        "physical_plan_text": details.get("physical_plan"),
        "optimized_plan_pgjson": details.get("optimized_plan_pgjson"),
        "logical_plan_proto_present": detail_inputs.artifacts.logical_plan_proto is not None,
        "optimized_plan_proto_present": detail_inputs.artifacts.optimized_plan_proto is not None,
        "execution_plan_proto_present": detail_inputs.artifacts.execution_plan_proto is not None,
        "planning_env_hash": detail_inputs.artifacts.planning_env_hash,
        "information_schema_hash": detail_inputs.artifacts.information_schema_hash,
        "df_settings": settings,
        "df_settings_hash": hash_settings(detail_inputs.artifacts.df_settings),
        "snapshot_keys": [dict(key) for key in context.snapshot_keys],
        "write_outcomes": [dict(outcome) for outcome in context.write_outcomes],
    }


def _plan_graphviz(plan: object | None) -> str | None:
    if plan is None:
        return None
    method = getattr(plan, "display_graphviz", None)
    if not callable(method):
        return None
    try:
        return str(method())
    except (RuntimeError, TypeError, ValueError):
        return None


def _plan_partition_count(plan: object | None) -> int | None:
    if plan is None:
        return None
    count = getattr(plan, "partition_count", None)
    if count is None:
        return None
    if isinstance(count, bool):
        return None
    if isinstance(count, (int, float)):
        return int(count)
    try:
        return int(count)
    except (TypeError, ValueError):
        return None


def _repartition_count_from_display(plan_display: str | None) -> int | None:
    if plan_display is None:
        return None
    token = "RepartitionExec"
    count = plan_display.count(token)
    return count if count > 0 else None


def _dynamic_filter_count_from_display(plan_display: str | None) -> int | None:
    if plan_display is None:
        return None
    token = "DynamicFilter"
    count = plan_display.count(token)
    return count if count > 0 else None


def _plan_statistics_payload(plan: object | None) -> Mapping[str, object] | None:
    if plan is None:
        return None
    stats_method = getattr(plan, "statistics", None)
    if callable(stats_method):
        with _suppress_errors():
            stats = stats_method()
        normalized = _normalize_statistics_payload(stats)
        if normalized is None:
            return None
        column_stats = normalized.get("column_statistics")
        column_present = (
            isinstance(column_stats, Sequence)
            and not isinstance(column_stats, (str, bytes, bytearray))
            and len(column_stats) > 0
        )
        return {
            "source": "execution_plan.statistics",
            "column_statistics_present": column_present,
            **normalized,
        }
    return None


def _normalize_statistics_payload(stats: object) -> Mapping[str, object] | None:
    if stats is None:
        return None
    if isinstance(stats, Mapping):
        return dict(stats)
    payload: dict[str, object] = {}
    for key in ("num_rows", "row_count", "total_byte_size", "total_bytes", "column_statistics"):
        value = getattr(stats, key, None)
        if value is None:
            continue
        payload[key] = _statistics_value(value)
    return payload or None


def _statistics_value(value: object) -> object:
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    inner = getattr(value, "value", None)
    if isinstance(inner, (int, float, str, bool)) or inner is None:
        return inner
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_statistics_value(item) for item in value]
    if isinstance(value, Mapping):
        return {str(k): _statistics_value(v) for k, v in value.items()}
    return str(value)


def _schema_metadata_payload(schema: pa.Schema) -> dict[str, str]:
    metadata = schema.metadata or {}
    items = sorted(metadata.items(), key=lambda item: str(item[0]))
    return {
        (key.decode("utf-8", errors="replace") if isinstance(key, bytes) else str(key)): (
            value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value)
        )
        for key, value in items
    }


def _schema_provenance(df: DataFrame) -> Mapping[str, object]:
    schema: pa.Schema | None = None
    with _suppress_errors():
        schema = arrow_schema_from_df(df)
    if schema is None:
        return {}

    try:
        from datafusion_engine.schema.contracts import SCHEMA_ABI_FINGERPRINT_META

        abi_meta = SCHEMA_ABI_FINGERPRINT_META
    except ImportError:
        abi_meta = b"schema_abi_fingerprint"

    metadata_payload = _schema_metadata_payload(schema)
    abi_key = abi_meta.decode("utf-8") if isinstance(abi_meta, bytes) else str(abi_meta)
    abi_value = metadata_payload.get(abi_key)
    return {
        "source": "arrow_schema",
        "schema_identity_hash": schema_identity_hash(schema),
        "schema_metadata": metadata_payload,
        "explicit_schema": abi_value is not None,
        "schema_abi_fingerprint": abi_value,
    }


def _schema_describe_rows(df: DataFrame) -> list[dict[str, object]]:
    resolved_schema: pa.Schema | None = None
    with _suppress_errors():
        resolved_schema = arrow_schema_from_df(df)
    if resolved_schema is None:
        return []
    return [
        {
            "column_name": field.name,
            "data_type": str(field.type),
            "nullable": field.nullable,
            "source": "arrow_schema",
        }
        for field in resolved_schema
    ]


def _determinism_audit_bundle(
    detail_inputs: PlanDetailInputs,
    *,
    context: PlanDetailContext,
) -> Mapping[str, object]:
    artifacts = detail_inputs.artifacts
    return {
        "plan_fingerprint": detail_inputs.plan_fingerprint,
        "planning_env_hash": artifacts.planning_env_hash,
        "rulepack_hash": artifacts.rulepack_hash,
        "information_schema_hash": context.information_schema_hash,
        "df_settings_hash": hash_settings(artifacts.df_settings),
        "udf_snapshot_hash": artifacts.udf_snapshot_hash,
        "function_registry_hash": artifacts.function_registry_hash,
        "schema_contract_hash": schema_contract_hash(),
    }


def _suppress_errors() -> contextlib.AbstractContextManager[None]:
    return contextlib.suppress(RuntimeError, TypeError, ValueError)


__all__ = ["PlanDetailContext", "PlanDetailInputs", "collect_plan_details"]
