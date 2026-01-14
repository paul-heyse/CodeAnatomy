"""Compile-time option models for DataFusion bridging."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion import SQLOptions

if TYPE_CHECKING:
    from ibis.expr.types import Value
    from sqlglot import Expression

SchemaMapping = Mapping[str, Mapping[str, str]]


@dataclass(frozen=True)
class DataFusionSqlPolicy:
    """Policy for SQL execution in DataFusion fallback paths."""

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


@dataclass(frozen=True)
class DataFusionFallbackEvent:
    """Diagnostics payload for SQL fallback execution."""

    reason: str
    error: str
    expression_type: str
    sql: str
    dialect: str
    policy_violations: tuple[str, ...] = ()


@dataclass(frozen=True)
class DataFusionCompileOptions:
    """Compilation options for DataFusion bridge execution."""

    schema_map: SchemaMapping | None = None
    optimize: bool = True
    cache: bool | None = None
    cache_max_columns: int | None = 64
    params: Mapping[str, object] | Mapping[Value, object] | None = None
    sql_options: SQLOptions | None = None
    sql_policy: DataFusionSqlPolicy | None = None
    dialect: str = "datafusion_ext"
    enable_rewrites: bool = True
    rewrite_hook: Callable[[Expression], Expression] | None = None
    fallback_hook: Callable[[DataFusionFallbackEvent], None] | None = None
    capture_explain: bool = False
    explain_analyze: bool = False
    explain_hook: Callable[[str, Sequence[Mapping[str, object]]], None] | None = None
