"""View artifact registry payloads derived from canonical SQLGlot ASTs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import sqlglot.expressions as exp
from sqlglot.schema import MappingSchema

from arrowdsl.schema.abi import schema_to_dict
from datafusion_engine.schema_introspection import schema_map_for_sqlglot
from datafusion_engine.sql_policy_engine import CompilationArtifacts, SQLPolicyProfile
from sqlglot_tools.lineage import referenced_udf_calls
from sqlglot_tools.optimizer import (
    StrictParseOptions,
    parse_sql_strict,
    register_datafusion_dialect,
    sqlglot_policy_snapshot_for,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from datafusion import SessionContext
    from datafusion.dataframe import DataFrame

    from datafusion_engine.schema_introspection import SchemaIntrospector


@dataclass(frozen=True)
class ViewArtifact:
    """Canonical artifact bundle for a registered view."""

    name: str
    ast: exp.Expression
    serde_payload: list[dict[str, object]]
    ast_fingerprint: str
    policy_hash: str
    schema: pa.Schema
    lineage: dict[str, set[tuple[str, str]]]
    required_udfs: tuple[str, ...]
    sql: str | None = None

    def payload(self) -> dict[str, object]:
        """Return a JSON-ready payload for diagnostics and persistence.

        Returns
        -------
        dict[str, object]
            JSON-serializable payload for diagnostics and storage.
        """
        return {
            "name": self.name,
            "ast_fingerprint": self.ast_fingerprint,
            "policy_hash": self.policy_hash,
            "schema": schema_to_dict(self.schema),
            "required_udfs": list(self.required_udfs),
            "lineage": _lineage_payload(self.lineage),
            "sql": self.sql,
            "serde_payload": list(self.serde_payload),
        }


@dataclass(frozen=True)
class ViewArtifactInputs:
    """Inputs for building a view artifact."""

    ctx: SessionContext
    name: str
    ast: exp.Expression
    schema: pa.Schema
    required_udfs: Sequence[str] | None = None
    policy_profile: SQLPolicyProfile | None = None
    sql: str | None = None


@dataclass(frozen=True)
class DataFrameArtifactInputs:
    """Inputs for building a view artifact from a DataFrame."""

    ctx: SessionContext
    name: str
    df: DataFrame
    ast: exp.Expression | None = None
    required_udfs: Sequence[str] | None = None
    policy_profile: SQLPolicyProfile | None = None
    sql: str | None = None


def build_view_artifact(inputs: ViewArtifactInputs) -> ViewArtifact:
    """Build a ViewArtifact from a canonical SQLGlot AST.

    Returns
    -------
    ViewArtifact
        View artifact bundle for diagnostics and caching.
    """
    profile = inputs.policy_profile or SQLPolicyProfile()
    policy = profile.to_sqlglot_policy()
    policy_hash = sqlglot_policy_snapshot_for(policy).policy_hash
    schema_map = schema_map_for_sqlglot(_schema_introspector(inputs.ctx))
    mapping = MappingSchema(dict(schema_map))
    artifacts = CompilationArtifacts.from_ast(inputs.ast, mapping)
    required_udfs = (
        tuple(inputs.required_udfs)
        if inputs.required_udfs is not None
        else tuple(sorted(referenced_udf_calls(inputs.ast)))
    )
    return ViewArtifact(
        name=inputs.name,
        ast=inputs.ast,
        serde_payload=artifacts.serde_payload,
        ast_fingerprint=artifacts.ast_fingerprint,
        policy_hash=policy_hash,
        schema=inputs.schema,
        lineage=artifacts.lineage_by_column,
        required_udfs=required_udfs,
        sql=inputs.sql,
    )


def build_view_artifact_from_dataframe(inputs: DataFrameArtifactInputs) -> ViewArtifact:
    """Build a ViewArtifact from a DataFusion DataFrame.

    Returns
    -------
    ViewArtifact
        View artifact bundle for diagnostics and caching.

    Raises
    ------
    ValueError
        Raised when the AST cannot be derived from the DataFrame.
    """
    resolved_ast = inputs.ast or _sqlglot_from_dataframe(inputs.df, sql=inputs.sql)
    if resolved_ast is None:
        msg = f"Failed to derive SQLGlot AST for view {inputs.name!r}."
        raise ValueError(msg)
    schema = _schema_from_df(inputs.df)
    return build_view_artifact(
        ViewArtifactInputs(
            ctx=inputs.ctx,
            name=inputs.name,
            ast=resolved_ast,
            schema=schema,
            required_udfs=inputs.required_udfs,
            policy_profile=inputs.policy_profile,
            sql=inputs.sql,
        )
    )


def _schema_introspector(ctx: SessionContext) -> SchemaIntrospector:
    from datafusion_engine.schema_introspection import SchemaIntrospector

    return SchemaIntrospector(ctx)


def _schema_from_df(df: DataFrame) -> pa.Schema:
    schema = df.schema()
    if isinstance(schema, pa.Schema):
        return schema
    to_arrow = getattr(schema, "to_arrow", None)
    if callable(to_arrow):
        resolved = to_arrow()
        if isinstance(resolved, pa.Schema):
            return resolved
    msg = "Failed to resolve DataFusion schema."
    raise TypeError(msg)


def _sqlglot_from_dataframe(df: DataFrame, *, sql: str | None) -> exp.Expression | None:
    logical_plan = getattr(df, "logical_plan", None)
    if callable(logical_plan):
        try:
            from datafusion.plan import LogicalPlan as DataFusionLogicalPlan
            from datafusion.unparser import Dialect as DataFusionDialect
            from datafusion.unparser import Unparser as DataFusionUnparser
        except ImportError:
            return None
        try:
            plan_obj = logical_plan()
            if isinstance(plan_obj, DataFusionLogicalPlan):
                unparser = DataFusionUnparser(DataFusionDialect.default())
                sql = str(unparser.plan_to_sql(plan_obj))
        except (RuntimeError, TypeError, ValueError):
            return None
    if not sql:
        return None
    register_datafusion_dialect()
    try:
        return parse_sql_strict(
            sql,
            dialect="datafusion_ext",
            options=StrictParseOptions(error_level=None),
        )
    except (TypeError, ValueError):
        return None


def _lineage_payload(
    lineage: Mapping[str, set[tuple[str, str]]],
) -> dict[str, list[dict[str, str]]]:
    payload: dict[str, list[dict[str, str]]] = {}
    for target, sources in lineage.items():
        payload[target] = [{"table": table, "column": column} for table, column in sorted(sources)]
    return payload


__all__ = [
    "DataFrameArtifactInputs",
    "ViewArtifact",
    "ViewArtifactInputs",
    "build_view_artifact",
    "build_view_artifact_from_dataframe",
]
