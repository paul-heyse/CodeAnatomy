"""SQL execution safety gates using DataFusion SQLOptions.

This module provides defense-in-depth for SQL execution by enforcing
execution policies that control DDL, DML, and statement execution.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING

from datafusion import SQLOptions

if TYPE_CHECKING:
    from datafusion import DataFrame, SessionContext

    from datafusion_engine.runtime import DataFusionRuntimeProfile


class ExecutionContext(Enum):
    """Context determining what SQL operations are allowed."""

    QUERY_ONLY = auto()  # SELECT only
    QUERY_WITH_TEMP = auto()  # SELECT + temp table creation
    FULL_DDL = auto()  # All DDL operations
    FULL_DML = auto()  # All DML operations
    ADMIN = auto()  # Everything including statements


@dataclass(frozen=True)
class ExecutionPolicy:
    """Policy for what SQL operations are allowed.

    Use this to enforce principle of least privilege for SQL execution.

    Parameters
    ----------
    allow_ddl
        Whether to allow DDL operations (CREATE, DROP, ALTER).
    allow_dml
        Whether to allow DML operations (INSERT, UPDATE, DELETE).
    allow_statements
        Whether to allow statements (SET, BEGIN TRANSACTION, etc.).
    """

    allow_ddl: bool = False
    allow_dml: bool = False
    allow_statements: bool = False

    @classmethod
    def for_context(cls, context: ExecutionContext) -> ExecutionPolicy:
        """Create policy for a specific context.

        Parameters
        ----------
        context
            Execution context to create policy for.

        Returns
        -------
        ExecutionPolicy
            Policy configured for the context.

        Raises
        ------
        ValueError
            If the execution context is not recognized.
        """
        match context:
            case ExecutionContext.QUERY_ONLY:
                return cls(allow_ddl=False, allow_dml=False, allow_statements=False)
            case ExecutionContext.QUERY_WITH_TEMP:
                return cls(allow_ddl=True, allow_dml=False, allow_statements=False)
            case ExecutionContext.FULL_DDL:
                return cls(allow_ddl=True, allow_dml=False, allow_statements=True)
            case ExecutionContext.FULL_DML:
                return cls(allow_ddl=True, allow_dml=True, allow_statements=True)
            case ExecutionContext.ADMIN:
                return cls(allow_ddl=True, allow_dml=True, allow_statements=True)
            case _:
                msg = f"Unsupported execution context: {context}"
                raise ValueError(msg)

    def to_sql_options(self) -> SQLOptions:
        """Convert to DataFusion SQLOptions.

        Returns
        -------
        SQLOptions
            DataFusion SQLOptions configured with policy settings.
        """
        return (
            SQLOptions()
            .with_allow_ddl(self.allow_ddl)
            .with_allow_dml(self.allow_dml)
            .with_allow_statements(self.allow_statements)
        )


def statement_sql_options_for_profile(
    profile: DataFusionRuntimeProfile | None,
) -> SQLOptions:
    """Build SQLOptions from runtime profile.

    Parameters
    ----------
    profile
        Runtime profile containing execution policy. If None, defaults to QUERY_ONLY.

    Returns
    -------
    SQLOptions
        DataFusion SQLOptions configured from profile policy.

    Notes
    -----
    Currently returns QUERY_ONLY default. Will be updated when
    execution_policy is added to DataFusionRuntimeProfile.
    """
    policy = execution_policy_for_profile(profile, allow_statements=True)
    return policy.to_sql_options()


def execution_policy_for_profile(
    profile: DataFusionRuntimeProfile | None,
    *,
    allow_statements: bool | None = None,
) -> ExecutionPolicy:
    """Return an ExecutionPolicy derived from the runtime profile.

    Returns
    -------
    ExecutionPolicy
        Execution policy derived from the profile settings.
    """
    if profile is None:
        base = ExecutionPolicy.for_context(ExecutionContext.QUERY_ONLY)
        return (
            ExecutionPolicy(
                allow_ddl=base.allow_ddl,
                allow_dml=base.allow_dml,
                allow_statements=allow_statements if allow_statements is not None else base.allow_statements,
            )
            if allow_statements is not None
            else base
        )
    from datafusion_engine.compile_options import resolve_sql_policy

    policy = profile.sql_policy or resolve_sql_policy(profile.sql_policy_name)
    return ExecutionPolicy(
        allow_ddl=policy.allow_ddl,
        allow_dml=policy.allow_dml,
        allow_statements=allow_statements if allow_statements is not None else policy.allow_statements,
    )


def safe_executor_for_profile(
    ctx: SessionContext,
    *,
    profile: DataFusionRuntimeProfile | None,
    allow_statements: bool | None = None,
) -> SafeExecutor:
    """Return a SafeExecutor configured from the runtime profile.

    Returns
    -------
    SafeExecutor
        Safe executor configured with the derived policy.
    """
    policy = execution_policy_for_profile(profile, allow_statements=allow_statements)
    return SafeExecutor(ctx=ctx, default_policy=policy)


def execute_with_policy(
    ctx: SessionContext,
    sql: str,
    policy: ExecutionPolicy,
) -> DataFrame:
    """Execute SQL with policy validation.

    The query is validated against the policy before execution.
    Raises an error if the query would perform disallowed operations.

    Parameters
    ----------
    ctx
        DataFusion session context.
    sql
        SQL query to execute.
    policy
        Execution policy to enforce.

    Returns
    -------
    DataFrame
        Query result.
    """
    return ctx.sql_with_options(
        sql,
        policy.to_sql_options(),
    )


def validate_sql_safety(
    sql: str,
    policy: ExecutionPolicy,
    *,
    dialect: str = "postgres",
) -> list[str]:
    """Validate SQL against policy without executing.

    Returns list of policy violations (empty if valid).

    Parameters
    ----------
    sql
        SQL query to validate.
    policy
        Execution policy to validate against.
    dialect
        SQL dialect for parsing (default: "postgres").

    Returns
    -------
    list[str]
        List of policy violations. Empty list indicates valid SQL.
    """
    from sqlglot_tools.compat import exp, parse_one

    violations: list[str] = []

    try:
        ast = parse_one(sql, dialect=dialect)
    except Exception as e:  # noqa: BLE001
        parse_error = f"Parse error: {e}"
        violations.append(parse_error)
        return violations

    # Check for DDL
    if not policy.allow_ddl and isinstance(ast, (exp.Create, exp.Drop, exp.Alter)):
        violations.append(f"DDL not allowed: {type(ast).__name__}")

    # Check for DML
    if not policy.allow_dml:
        if isinstance(ast, (exp.Insert, exp.Update, exp.Delete)):
            violations.append(f"DML not allowed: {type(ast).__name__}")
        # COPY is also DML
        elif isinstance(ast, exp.Copy):
            violations.append("COPY not allowed (DML)")

    # Check for statements
    if not policy.allow_statements and isinstance(ast, (exp.Set, exp.Transaction)):
        violations.append(f"Statement not allowed: {type(ast).__name__}")

    # Check for external locations (high-risk)
    for location in ast.find_all(exp.LocationProperty):
        path = location.this.this if hasattr(location.this, "this") else str(location.this)
        if path.startswith(("s3://", "gs://", "az://", "http://", "https://")):
            violations.append(f"External location detected: {path[:50]}...")

    return violations


@dataclass
class SafeExecutor:
    """Executor with built-in safety policies.

    Provides a safe interface for SQL execution with
    configurable policies.

    Parameters
    ----------
    ctx
        DataFusion session context.
    default_policy
        Default execution policy to use.
    """

    ctx: SessionContext
    default_policy: ExecutionPolicy

    def execute(
        self,
        sql: str,
        *,
        policy: ExecutionPolicy | None = None,
        validate_first: bool = True,
    ) -> DataFrame:
        """Execute SQL with safety validation.

        Parameters
        ----------
        sql
            SQL query to execute.
        policy
            Override policy (uses default if not provided).
        validate_first
            Whether to validate before execution.

        Returns
        -------
        DataFrame
            Execution result.

        Raises
        ------
        ValueError
            If SQL violates policy.
        """
        effective_policy = policy or self.default_policy

        if validate_first:
            violations = validate_sql_safety(sql, effective_policy)
            if violations:
                msg = f"SQL policy violations: {'; '.join(violations)}"
                raise ValueError(msg)

        return execute_with_policy(
            self.ctx,
            sql,
            effective_policy,
        )
