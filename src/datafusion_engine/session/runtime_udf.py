"""UDF validation, schema helpers, and planner rule resolvers for DataFusion runtime.

This module provides leaf-level utilities for validating rulepack signatures,
resolving planner rule installers, and schema introspection helpers. It has NO
dependency on DataFusionRuntimeProfile to avoid circular imports.
"""

from __future__ import annotations

import importlib
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

import pyarrow as pa
from datafusion import SessionContext, SQLOptions

from datafusion_engine.arrow.interop import empty_table_for_schema
from datafusion_engine.arrow.metadata import schema_constraints_from_metadata
from datafusion_engine.schema.introspection import (
    SchemaIntrospector,
    table_constraint_rows,
)

if TYPE_CHECKING:
    from datafusion_engine.session.runtime_profile_config import PreparedStatementSpec
    from datafusion_engine.session.runtime_session import (
        DataFusionViewRegistry,
        SessionRuntime,
    )

# DataFusion SQL error is just Exception (comment from original code)
_DATAFUSION_SQL_ERROR = Exception

_logger = logging.getLogger(__name__)


__all__ = [
    "SchemaRegistryValidationResult",
    "_PlannerRuleInstallers",
    "_collect_view_sql_parse_errors",
    "_constraint_drift_entries",
    "_constraint_key_fields",
    "_install_schema_evolution_adapter_factory",
    "_is_nullable",
    "_load_schema_evolution_adapter_factory",
    "_merge_signature_errors",
    "_prepare_statement_sql",
    "_register_schema_table",
    "_relationship_constraint_errors",
    "_resolve_planner_rule_installers",
    "_rulepack_available_functions",
    "_rulepack_function_errors",
    "_rulepack_missing_functions",
    "_rulepack_parameter_counts",
    "_rulepack_parameter_signatures",
    "_rulepack_required_functions",
    "_rulepack_row_value",
    "_rulepack_signature_errors",
    "_rulepack_signature_for_spec",
    "_rulepack_signature_type_errors",
    "_rulepack_signature_validation",
    "_sql_parse_errors",
    "_table_dfschema_tree",
    "_table_logical_plan",
]


# ============================================================================
# Schema validation result
# ============================================================================


@dataclass(frozen=True)
class SchemaRegistryValidationResult:
    """Summary of schema registry validation checks."""

    missing: tuple[str, ...] = ()
    type_errors: dict[str, str] = field(default_factory=dict)
    view_errors: dict[str, str] = field(default_factory=dict)
    constraint_drift: tuple[dict[str, object], ...] = ()
    relationship_constraint_errors: dict[str, object] | None = None

    def has_errors(self) -> bool:
        """Return whether any validation errors are present.

        Returns:
        -------
        bool
            ``True`` when the validation result includes any errors.
        """
        return bool(
            self.missing
            or self.type_errors
            or self.view_errors
            or self.constraint_drift
            or self.relationship_constraint_errors
        )


# ============================================================================
# Planner rule installer types
# ============================================================================


@dataclass(frozen=True)
class _PlannerRuleInstallers:
    config_installer: Callable[[SessionContext, bool, bool, bool], None]
    physical_config_installer: Callable[[SessionContext, bool], None]
    rule_installer: Callable[[SessionContext], None]
    physical_installer: Callable[[SessionContext], None]


def _resolve_planner_rule_installers() -> _PlannerRuleInstallers | None:
    imported_any = False
    for module_name in ("datafusion_engine.extensions.datafusion_ext",):
        try:
            candidate = importlib.import_module(module_name)
        except ImportError:
            continue
        imported_any = True
        config_installer = getattr(candidate, "install_codeanatomy_policy_config", None)
        physical_config_installer = getattr(candidate, "install_codeanatomy_physical_config", None)
        rule_installer = getattr(candidate, "install_planner_rules", None)
        if not (
            callable(config_installer)
            and callable(physical_config_installer)
            and callable(rule_installer)
        ):
            continue
        physical_installer = getattr(candidate, "install_physical_rules", None)
        if not callable(physical_installer):
            msg = "Physical rule installer is unavailable in the DataFusion extension module."
            raise TypeError(msg)
        return _PlannerRuleInstallers(
            config_installer=cast(
                "Callable[[SessionContext, bool, bool, bool], None]",
                config_installer,
            ),
            physical_config_installer=cast(
                "Callable[[SessionContext, bool], None]",
                physical_config_installer,
            ),
            rule_installer=cast("Callable[[SessionContext], None]", rule_installer),
            physical_installer=cast("Callable[[SessionContext], None]", physical_installer),
        )
    if not imported_any:  # pragma: no cover - optional dependency
        msg = "Planner policy rules require datafusion_ext."
        raise RuntimeError(msg)
    return None


# ============================================================================
# Schema helpers
# ============================================================================


def _prepare_statement_sql(statement: PreparedStatementSpec) -> str:
    sql = statement.sql.strip()
    if sql.lower().startswith("prepare "):
        return sql
    if statement.param_types:
        params = ", ".join(statement.param_types)
        return f"PREPARE {statement.name}({params}) AS {statement.sql}"
    return f"PREPARE {statement.name} AS {statement.sql}"


def _table_logical_plan(ctx: SessionContext, *, name: str) -> str:
    try:
        module = importlib.import_module("datafusion_engine.extensions.datafusion_ext")
    except ImportError:
        module = None
    if module is not None:
        fn = getattr(module, "table_logical_plan", None)
        if callable(fn):
            return str(fn(ctx, name))
    df = ctx.table(name)
    return str(df.logical_plan())


def _table_dfschema_tree(ctx: SessionContext, *, name: str) -> str:
    try:
        module = importlib.import_module("datafusion_engine.extensions.datafusion_ext")
    except ImportError:
        module = None
    if module is not None:
        fn = getattr(module, "table_dfschema_tree", None)
        if callable(fn):
            return str(fn(ctx, name))
    df = ctx.table(name)
    schema = df.schema()
    tree_string = getattr(schema, "tree_string", None)
    if callable(tree_string):
        return str(tree_string())
    return str(schema)


def _sql_parse_errors(
    ctx: SessionContext,
    sql: str,
    *,
    sql_options: SQLOptions,
) -> list[dict[str, object]] | None:
    """Parse SQL using DataFusion and collect parsing errors.

    Parameters
    ----------
    ctx
        DataFusion SessionContext used for SQL parsing.
    sql
        SQL string to parse and validate.
    sql_options
        SQL options that gate SQL execution behavior.

    Returns:
    -------
    list[dict[str, object]] | None
        List of parsing errors if any, None if parsing succeeds.
    """
    # Import here to avoid circular dependency
    from datafusion_engine.session.runtime_session import _sql_with_options

    try:
        _ = _sql_with_options(ctx, sql, sql_options=sql_options)
    except ValueError as exc:
        return [{"message": str(exc)}]
    return None


def _collect_view_sql_parse_errors(
    ctx: SessionContext,
    registry: DataFusionViewRegistry,
    *,
    sql_options: SQLOptions,
) -> dict[str, list[dict[str, object]]] | None:
    errors: dict[str, list[dict[str, object]]] = {}
    for name, entry in registry.entries.items():
        sql = entry if isinstance(entry, str) else getattr(entry, "sql", None)
        if not isinstance(sql, str) or not sql:
            continue
        parse_errors = _sql_parse_errors(ctx, sql, sql_options=sql_options)
        if parse_errors:
            errors[name] = parse_errors
    return errors or None


def _constraint_key_fields(rows: Sequence[Mapping[str, object]]) -> list[str]:
    constraints: dict[tuple[str, str], list[tuple[int, str]]] = {}
    for row in rows:
        constraint_type = row.get("constraint_type")
        if not isinstance(constraint_type, str):
            continue
        constraint_kind = constraint_type.upper()
        if constraint_kind not in {"PRIMARY KEY", "UNIQUE"}:
            continue
        constraint_name = row.get("constraint_name")
        column_name = row.get("column_name")
        if not isinstance(constraint_name, str) or not constraint_name:
            continue
        if not isinstance(column_name, str) or not column_name:
            continue
        ordinal = row.get("ordinal_position")
        position = int(ordinal) if isinstance(ordinal, (int, float)) else 0
        constraints.setdefault((constraint_kind, constraint_name), []).append(
            (position, column_name)
        )
    if not constraints:
        return []
    for kind in ("PRIMARY KEY", "UNIQUE"):
        candidates = {key: values for key, values in constraints.items() if key[0] == kind}
        if not candidates:
            continue
        _, values = sorted(candidates.items(), key=lambda item: item[0][1])[0]
        return [name for _, name in sorted(values, key=lambda item: item[0])]
    return []


def _is_nullable(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"no", "false", "0"}:
            return False
        if normalized in {"yes", "true", "1"}:
            return True
    return True


def _constraint_drift_entries(
    introspector: SchemaIntrospector,
    *,
    names: Sequence[str],
    schemas: Mapping[str, pa.Schema] | None = None,
) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    schema_map = dict(schemas or {})
    for name in names:
        schema = schema_map.get(name)
        if schema is None:
            continue
        expected_required, expected_keys = schema_constraints_from_metadata(schema.metadata)
        expected_required_set = set(expected_required)
        expected_keys_set = set(expected_keys)
        try:
            columns = introspector.table_columns(name)
            constraint_rows = table_constraint_rows(
                introspector.ctx,
                table_name=name,
                sql_options=introspector.sql_options,
            )
        except (RuntimeError, TypeError, ValueError):
            continue
        observed_required = {
            str(row["column_name"])
            for row in columns
            if row.get("column_name") is not None and not _is_nullable(row.get("is_nullable"))
        }
        observed_keys = set(_constraint_key_fields(constraint_rows))
        missing_required = sorted(expected_required_set - observed_required)
        extra_required = sorted(observed_required - expected_required_set)
        missing_keys = sorted(expected_keys_set - observed_keys)
        extra_keys = sorted(observed_keys - expected_keys_set)
        if not missing_required and not extra_required and not missing_keys and not extra_keys:
            continue
        entries.append(
            {
                "schema_name": name,
                "expected_required_non_null": sorted(expected_required_set) or None,
                "observed_required_non_null": sorted(observed_required) or None,
                "missing_required_non_null": missing_required or None,
                "extra_required_non_null": extra_required or None,
                "expected_key_fields": list(expected_keys) or None,
                "observed_key_fields": sorted(observed_keys) or None,
                "missing_key_fields": missing_keys or None,
                "extra_key_fields": extra_keys or None,
            }
        )
    return entries


def _relationship_constraint_errors(
    session_runtime: SessionRuntime,
    *,
    sql_options: SQLOptions,
) -> Mapping[str, object] | None:
    _ = session_runtime
    _ = sql_options
    return None


# ============================================================================
# Schema install functions
# ============================================================================


def _register_schema_table(ctx: SessionContext, name: str, schema: pa.Schema) -> None:
    """Register a schema-only table via an empty table provider."""
    table = empty_table_for_schema(schema)
    from datafusion_engine.io.adapter import DataFusionIOAdapter

    adapter = DataFusionIOAdapter(ctx=ctx, profile=None)
    adapter.register_arrow_table(name, table)


def _load_schema_evolution_adapter_factory() -> object:
    """Return a schema evolution adapter factory from the native extension."""
    from datafusion_engine.extensions.schema_evolution import load_schema_evolution_adapter_factory

    return load_schema_evolution_adapter_factory()


def _install_schema_evolution_adapter_factory(ctx: SessionContext) -> None:
    """Install the schema evolution adapter factory via the native extension.

    Args:
    ----
        ctx: Description.
    """
    from datafusion_engine.extensions.schema_evolution import (
        install_schema_evolution_adapter_factory,
    )

    install_schema_evolution_adapter_factory(ctx)


# ============================================================================
# Rulepack validation functions
# ============================================================================


def _rulepack_parameter_counts(rows: Sequence[Mapping[str, object]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        name: str | None = None
        for key in ("specific_name", "routine_name", "function_name", "name"):
            value = row.get(key)
            if isinstance(value, str):
                name = value
                break
        if name is None:
            continue
        normalized = name.lower()
        counts[normalized] = counts.get(normalized, 0) + 1
    return counts


def _rulepack_parameter_signatures(
    rows: Sequence[Mapping[str, object]],
) -> dict[str, set[tuple[str, ...]]]:
    signatures: dict[str, dict[str, list[tuple[int, str]]]] = {}
    for row in rows:
        name = _rulepack_row_value(row, ("routine_name", "specific_name", "function_name", "name"))
        if name is None:
            continue
        data_type = _rulepack_row_value(row, ("data_type",))
        if data_type is None:
            continue
        ordinal = row.get("ordinal_position")
        if (isinstance(ordinal, (int, float)) and not isinstance(ordinal, bool)) or (
            isinstance(ordinal, str) and ordinal.isdigit()
        ):
            position = int(ordinal)
        else:
            continue
        specific = _rulepack_row_value(row, ("specific_name",)) or name
        routine_key = name.lower()
        specific_key = specific.lower()
        signatures.setdefault(routine_key, {}).setdefault(specific_key, []).append(
            (position, data_type.lower())
        )
    resolved: dict[str, set[tuple[str, ...]]] = {}
    for routine_name, specifics in signatures.items():
        for values in specifics.values():
            ordered = tuple(dtype for _, dtype in sorted(values, key=lambda item: item[0]))
            resolved.setdefault(routine_name, set()).add(ordered)
    return resolved


def _rulepack_row_value(row: Mapping[str, object], keys: Sequence[str]) -> str | None:
    for key in keys:
        value = row.get(key)
        if isinstance(value, str):
            return value
    return None


def _rulepack_signature_errors(
    required: Mapping[str, int],
    counts: Mapping[str, int],
) -> dict[str, list[str]]:
    missing: list[str] = []
    mismatched: list[str] = []
    for name, min_args in required.items():
        count = counts.get(name.lower())
        if count is None:
            missing.append(name)
            continue
        if count < min_args:
            mismatched.append(f"{name} ({count} < {min_args})")
    details: dict[str, list[str]] = {}
    if missing:
        details["missing"] = missing
    if mismatched:
        details["mismatched"] = mismatched
    return details


def _rulepack_signature_type_errors(
    required: Mapping[str, set[tuple[str, ...]]],
    parameters: Sequence[Mapping[str, object]],
) -> dict[str, list[str]]:
    if not required:
        return {}
    available = _rulepack_parameter_signatures(parameters)
    missing: list[str] = []
    mismatched: list[str] = []
    for name, expected_signatures in required.items():
        actual = available.get(name.lower())
        if not actual:
            missing.append(name)
            continue
        for expected in expected_signatures:
            if expected not in actual:
                sig = ", ".join(expected)
                mismatched.append(f"{name} ({sig})")
    details: dict[str, list[str]] = {}
    if missing:
        details["missing_types"] = missing
    if mismatched:
        details["type_mismatch"] = mismatched
    return details


def _merge_signature_errors(
    counts: Mapping[str, Sequence[str]],
    types: Mapping[str, Sequence[str]],
) -> dict[str, list[str]] | None:
    merged: dict[str, list[str]] = {}
    for errors in (counts, types):
        for key, values in errors.items():
            if values:
                merged.setdefault(key, []).extend(values)
    if not merged:
        return None
    return merged


def _rulepack_required_functions(
    *,
    datafusion_function_catalog: Sequence[Mapping[str, object]] | None = None,
) -> tuple[
    dict[str, set[str]],
    dict[str, int],
    dict[str, set[tuple[str, ...]]],
]:
    _ = datafusion_function_catalog
    return {}, {}, {}


def _rulepack_signature_for_spec(spec: object) -> tuple[str, ...] | None:
    # Import here to avoid circular dependency
    from datafusion_engine.session.runtime_dataset_io import _datafusion_type_name

    input_types = getattr(spec, "input_types", None)
    if not isinstance(input_types, tuple):
        return None
    try:
        return tuple(_datafusion_type_name(dtype).lower() for dtype in input_types)
    except (RuntimeError, TypeError, ValueError):
        return None


def _rulepack_function_errors(
    ctx: SessionContext,
    *,
    required: Mapping[str, set[str]],
    required_counts: Mapping[str, int],
    required_signatures: Mapping[str, set[tuple[str, ...]]],
    sql_options: SQLOptions | None = None,
) -> dict[str, str]:
    errors: dict[str, str] = {}
    available = _rulepack_available_functions(ctx, errors, sql_options=sql_options)
    missing = _rulepack_missing_functions(required, available)
    if missing:
        errors["missing_functions"] = str(missing)
    signature_errors = _rulepack_signature_validation(
        ctx,
        required_counts,
        required_signatures,
        errors,
        sql_options=sql_options,
    )
    if signature_errors is not None:
        errors["function_signatures"] = str(signature_errors)
    return errors


def _rulepack_available_functions(
    ctx: SessionContext,
    errors: dict[str, str],
    *,
    sql_options: SQLOptions | None = None,
) -> set[str]:
    try:
        return SchemaIntrospector(ctx, sql_options=sql_options).function_names()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["function_catalog"] = str(exc)
        return set()


def _rulepack_missing_functions(
    required: Mapping[str, set[str]],
    available: set[str],
) -> dict[str, list[str]]:
    available_lower = {name.lower() for name in available}
    return {
        name: sorted(rules)
        for name, rules in required.items()
        if name.lower() not in available_lower
    }


def _rulepack_signature_validation(
    ctx: SessionContext,
    required_counts: Mapping[str, int],
    required_signatures: Mapping[str, set[tuple[str, ...]]],
    errors: dict[str, str],
    *,
    sql_options: SQLOptions | None = None,
) -> dict[str, list[str]] | None:
    if not required_counts and not required_signatures:
        return None
    try:
        parameters = SchemaIntrospector(ctx, sql_options=sql_options).parameters_snapshot()
    except (RuntimeError, TypeError, ValueError) as exc:
        errors["function_parameters"] = str(exc)
        return None
    counts = _rulepack_parameter_counts(parameters)
    count_errors = _rulepack_signature_errors(required_counts, counts)
    type_errors = _rulepack_signature_type_errors(required_signatures, parameters)
    return _merge_signature_errors(count_errors, type_errors)
