"""Compile-time option models for DataFusion bridging."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion import SQLOptions

from core.config_base import config_fingerprint
from core_types import IdentifierStr, NonNegativeInt, RunIdStr
from serde_msgspec import StructBaseStrict, to_builtins

if TYPE_CHECKING:
    from datafusion_engine.plan.cache import PlanCache
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile
    from datafusion_engine.session.runtime_profile_config import ExplainRows
    from runtime_models.compile import DataFusionCompileOptionsRuntime

SchemaMapping = (
    Mapping[str, Mapping[str, str]]
    | Mapping[str, Mapping[str, Mapping[str, str]]]
    | Mapping[str, Mapping[str, Mapping[str, Mapping[str, str]]]]
)


class DataFusionSqlPolicy(StructBaseStrict, frozen=True):
    """Policy for SQL execution in DataFusion sessions."""

    allow_ddl: bool = True
    allow_dml: bool = True
    allow_statements: bool = True

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return fingerprint payload for the SQL policy.

        Returns:
        -------
        Mapping[str, object]
            Payload describing SQL policy settings.
        """
        return {
            "allow_ddl": self.allow_ddl,
            "allow_dml": self.allow_dml,
            "allow_statements": self.allow_statements,
        }

    def fingerprint(self) -> str:
        """Return fingerprint for the SQL policy.

        Returns:
        -------
        str
            Deterministic fingerprint for the policy.
        """
        return config_fingerprint(self.fingerprint_payload())

    def to_sql_options(self) -> SQLOptions:
        """Return SQLOptions matching this policy.

        Returns:
        -------
        datafusion.SQLOptions
            SQL options configured from the policy settings.
        """
        return (
            SQLOptions()
            .with_allow_ddl(self.allow_ddl)
            .with_allow_dml(self.allow_dml)
            .with_allow_statements(self.allow_statements)
        )


class DataFusionCompileOptionsSpec(StructBaseStrict, frozen=True):
    """Serializable compile-time options for DataFusion execution."""

    cache: bool | None = None
    cache_max_columns: NonNegativeInt | None = 64
    param_identifier_allowlist: tuple[str, ...] | None = None
    prepared_statements: bool = True
    prepared_param_types: Mapping[str, str] | None = None
    sql_policy_name: IdentifierStr | None = None
    enforce_sql_policy: bool = True
    enforce_preflight: bool = True
    dialect: str = "datafusion"
    enable_rewrites: bool = True
    capture_explain: bool = False
    explain_analyze: bool = False
    capture_plan_artifacts: bool = False
    substrait_validation: bool = False
    diagnostics_allow_sql: bool = False
    capture_semantic_diff: bool = False
    prefer_substrait: bool = False
    prefer_ast_execution: bool = True
    record_substrait_gaps: bool = False
    dynamic_projection: bool = True


SQL_POLICY_PRESETS: Mapping[str, DataFusionSqlPolicy] = {
    "read_only": DataFusionSqlPolicy(
        allow_ddl=False,
        allow_dml=False,
        allow_statements=False,
    ),
    "write": DataFusionSqlPolicy(
        allow_ddl=False,
        allow_dml=True,
        allow_statements=True,
    ),
    "service": DataFusionSqlPolicy(
        allow_ddl=False,
        allow_dml=False,
        allow_statements=False,
    ),
    "admin": DataFusionSqlPolicy(
        allow_ddl=True,
        allow_dml=True,
        allow_statements=True,
    ),
}


def resolve_sql_policy(
    name: str | None,
    *,
    fallback: DataFusionSqlPolicy | None = None,
) -> DataFusionSqlPolicy:
    """Return a SQL policy from the preset matrix.

    Args:
        name: Description.
        fallback: Description.

    Raises:
        ValueError: If the operation cannot be completed.
    """
    if name is None:
        return fallback or DataFusionSqlPolicy()
    policy = SQL_POLICY_PRESETS.get(name)
    if policy is None:
        msg = f"Unknown SQL policy name: {name!r}."
        raise ValueError(msg)
    return policy


@dataclass(frozen=True)
class DataFusionCacheEvent:
    """Diagnostics payload for DataFusion cache decisions.

    Uses plan_fingerprint from DataFusionPlanArtifact.
    """

    cache_enabled: bool
    cache_max_columns: NonNegativeInt | None
    column_count: int
    reason: str
    profile_hash: str | None = None
    plan_fingerprint: str | None = None


@dataclass(frozen=True)
class DataFusionSubstraitFallbackEvent:
    """Diagnostics payload for Substrait fallback decisions.

    Uses plan_fingerprint from DataFusionPlanArtifact.
    """

    reason: str
    expr_type: str
    profile_hash: str | None = None
    run_id: RunIdStr | None = None
    plan_fingerprint: str | None = None


@dataclass(frozen=True)
class DataFusionDmlOptions:
    """Options for DataFusion DML execution."""

    sql_options: SQLOptions | None = None
    sql_policy: DataFusionSqlPolicy | None = None
    sql_policy_name: str | None = None
    session_policy: DataFusionSqlPolicy | None = None
    table_policy: DataFusionSqlPolicy | None = None
    param_identifier_allowlist: tuple[str, ...] | None = None
    params: Mapping[str, object] | None = None
    named_params: Mapping[str, object] | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
    dialect: str = "datafusion"
    record_hook: Callable[[Mapping[str, object]], None] | None = None


@dataclass(frozen=True)
class DataFusionCompileOptions:
    """Compilation options for DataFusion bridge execution."""

    schema_map: SchemaMapping | None = None
    schema_map_hash: str | None = None
    optimize: bool = True
    cache: bool | None = None
    cache_max_columns: NonNegativeInt | None = 64
    cache_event_hook: Callable[[DataFusionCacheEvent], None] | None = None
    params: Mapping[str, object] | None = None
    named_params: Mapping[str, object] | None = None
    param_identifier_allowlist: tuple[str, ...] | None = None
    prepared_statements: bool = True
    prepared_param_types: Mapping[str, str] | None = None
    sql_options: SQLOptions | None = None
    sql_policy: DataFusionSqlPolicy | None = None
    sql_policy_name: str | None = None
    enforce_sql_policy: bool = True
    enforce_preflight: bool = True
    dialect: str = "datafusion"
    enable_rewrites: bool = True
    rewrite_hook: Callable[[object], object] | None = None
    sql_ingest_hook: Callable[[Mapping[str, object]], None] | None = None
    capture_explain: bool = False
    explain_analyze: bool = False
    explain_hook: Callable[[str, ExplainRows], None] | None = None
    capture_plan_artifacts: bool = False
    plan_artifacts_hook: Callable[[Mapping[str, object]], None] | None = None
    substrait_fallback_hook: Callable[[DataFusionSubstraitFallbackEvent], None] | None = None
    substrait_validation: bool = False
    diagnostics_allow_sql: bool = False
    substrait_plan_override: bytes | None = None
    capture_semantic_diff: bool = False
    semantic_diff_base_expr: object | None = None
    semantic_diff_base_sql: str | None = None
    semantic_diff_hook: Callable[[Mapping[str, object]], None] | None = None
    plan_cache: PlanCache | None = None
    profile_hash: str | None = None
    runtime_profile: DataFusionRuntimeProfile | None = None
    run_id: RunIdStr | None = None
    prefer_substrait: bool = False
    prefer_ast_execution: bool = True
    record_substrait_gaps: bool = False
    dynamic_projection: bool = True


def compile_options_runtime(
    spec: DataFusionCompileOptionsSpec,
) -> DataFusionCompileOptionsRuntime:
    """Validate compile options spec via runtime models.

    Parameters
    ----------
    spec
        Serializable compile options spec.

    Returns:
    -------
    DataFusionCompileOptionsRuntime
        Validated runtime compile options.
    """
    from runtime_models.adapters import COMPILE_OPTIONS_ADAPTER

    payload = to_builtins(spec, str_keys=True)
    return COMPILE_OPTIONS_ADAPTER.validate_python(payload)


def compile_options_from_spec(
    spec: DataFusionCompileOptionsSpec,
) -> DataFusionCompileOptions:
    """Return runtime compile options derived from a spec.

    Parameters
    ----------
    spec
        Serializable compile options spec.

    Returns:
    -------
    DataFusionCompileOptions
        Runtime compile options for execution.
    """
    resolved = compile_options_runtime(spec)
    return DataFusionCompileOptions(
        cache=resolved.cache,
        cache_max_columns=resolved.cache_max_columns,
        param_identifier_allowlist=resolved.param_identifier_allowlist,
        prepared_statements=resolved.prepared_statements,
        prepared_param_types=resolved.prepared_param_types,
        sql_policy_name=resolved.sql_policy_name,
        enforce_sql_policy=resolved.enforce_sql_policy,
        enforce_preflight=resolved.enforce_preflight,
        dialect=resolved.dialect,
        enable_rewrites=resolved.enable_rewrites,
        capture_explain=resolved.capture_explain,
        explain_analyze=resolved.explain_analyze,
        capture_plan_artifacts=resolved.capture_plan_artifacts,
        substrait_validation=resolved.substrait_validation,
        diagnostics_allow_sql=resolved.diagnostics_allow_sql,
        capture_semantic_diff=resolved.capture_semantic_diff,
        prefer_substrait=resolved.prefer_substrait,
        prefer_ast_execution=resolved.prefer_ast_execution,
        record_substrait_gaps=resolved.record_substrait_gaps,
        dynamic_projection=resolved.dynamic_projection,
    )
