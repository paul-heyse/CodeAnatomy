"""Compile-time option models for DataFusion bridging."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from datafusion import SQLOptions

from sqlglot_tools.optimizer import SqlGlotSurface, sqlglot_surface_policy

if TYPE_CHECKING:
    from ibis.expr.types import Value

    from arrowdsl.core.interop import RecordBatchReaderLike, TableLike
    from engine.plan_cache import PlanCache
    from sqlglot_tools.compat import Expression
    from sqlglot_tools.optimizer import SqlGlotPolicy

    ExplainRows = TableLike | RecordBatchReaderLike
else:
    ExplainRows = object

SchemaMapping = (
    Mapping[str, Mapping[str, str]]
    | Mapping[str, Mapping[str, Mapping[str, str]]]
    | Mapping[str, Mapping[str, Mapping[str, Mapping[str, str]]]]
)


@dataclass(frozen=True)
class DataFusionSqlPolicy:
    """Policy for SQL execution in DataFusion sessions."""

    allow_ddl: bool = False
    allow_dml: bool = False
    allow_statements: bool = False

    def to_sql_options(self) -> SQLOptions:
        """Return SQLOptions matching this policy.

        Returns
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


SQL_POLICY_PRESETS: Mapping[str, DataFusionSqlPolicy] = {
    "read_only": DataFusionSqlPolicy(),
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

    Returns
    -------
    DataFusionSqlPolicy
        SQL policy resolved from presets or fallback.

    Raises
    ------
    ValueError
        Raised when the policy name is unknown.
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
    """Diagnostics payload for DataFusion cache decisions."""

    cache_enabled: bool
    cache_max_columns: int | None
    column_count: int
    reason: str
    plan_hash: str | None = None
    profile_hash: str | None = None


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
    dialect: str = field(
        default_factory=lambda: sqlglot_surface_policy(SqlGlotSurface.DATAFUSION_DML).dialect
    )
    record_hook: Callable[[Mapping[str, object]], None] | None = None


@dataclass(frozen=True)
class DataFusionCompileOptions:
    """Compilation options for DataFusion bridge execution."""

    schema_map: SchemaMapping | None = None
    schema_map_hash: str | None = None
    optimize: bool = True
    cache: bool | None = None
    cache_max_columns: int | None = 64
    cache_event_hook: Callable[[DataFusionCacheEvent], None] | None = None
    params: Mapping[str, object] | Mapping[Value, object] | None = None
    param_identifier_allowlist: tuple[str, ...] | None = None
    sql_options: SQLOptions | None = None
    sql_policy: DataFusionSqlPolicy | None = None
    sql_policy_name: str | None = None
    enforce_sql_policy: bool = True
    enforce_preflight: bool = True
    dialect: str = field(
        default_factory=lambda: sqlglot_surface_policy(SqlGlotSurface.DATAFUSION_COMPILE).dialect
    )
    enable_rewrites: bool = True
    rewrite_hook: Callable[[Expression], Expression] | None = None
    sql_ingest_hook: Callable[[Mapping[str, object]], None] | None = None
    capture_explain: bool = False
    explain_analyze: bool = False
    explain_hook: Callable[[str, ExplainRows], None] | None = None
    capture_plan_artifacts: bool = False
    plan_artifacts_hook: Callable[[Mapping[str, object]], None] | None = None
    substrait_validation: bool = False
    plan_cache: PlanCache | None = None
    plan_hash: str | None = None
    profile_hash: str | None = None
    sqlglot_policy: SqlGlotPolicy | None = None
    sqlglot_policy_hash: str | None = None
    run_id: str | None = None
    prefer_substrait: bool = False
    record_substrait_gaps: bool = False
