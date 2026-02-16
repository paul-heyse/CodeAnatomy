"""Parity checks between Rust UDF registry snapshots and runtime metadata."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion_engine.sql.options import sql_options_for_profile
from datafusion_engine.udf.extension_core import (
    rust_udf_snapshot,
    snapshot_function_names,
    snapshot_parameter_names,
    validate_rust_udf_snapshot,
)

if TYPE_CHECKING:
    from datafusion import SessionContext


@dataclass(frozen=True)
class UdfParityMismatch:
    """Record a mismatch between Rust registry metadata sources."""

    func_id: str
    kind: str
    issue: str
    runtime_value: object | None = None
    rust_value: object | None = None


@dataclass(frozen=True)
class UdfParityReport:
    """Summary of UDF parity mismatches."""

    missing_in_rust: tuple[str, ...]
    param_name_mismatches: tuple[UdfParityMismatch, ...]
    volatility_mismatches: tuple[UdfParityMismatch, ...]

    def payload(self) -> dict[str, object]:
        """Return a diagnostics payload for parity mismatches.

        Returns:
        -------
        dict[str, object]
            Serializable diagnostics payload.
        """
        return {
            "missing_in_rust": list(self.missing_in_rust),
            "param_name_mismatches": [
                {
                    "func_id": item.func_id,
                    "kind": item.kind,
                    "issue": item.issue,
                    "runtime_value": item.runtime_value,
                    "rust_value": item.rust_value,
                }
                for item in self.param_name_mismatches
            ],
            "volatility_mismatches": [
                {
                    "func_id": item.func_id,
                    "kind": item.kind,
                    "issue": item.issue,
                    "runtime_value": item.runtime_value,
                    "rust_value": item.rust_value,
                }
                for item in self.volatility_mismatches
            ],
        }


@dataclass(frozen=True)
class UdfInfoSchemaParityReport:
    """Summary of Rust registry parity with information_schema."""

    missing_in_information_schema: tuple[str, ...]
    param_name_mismatches: tuple[UdfParityMismatch, ...]
    routines_available: bool
    parameters_available: bool
    error: str | None = None

    def payload(self) -> dict[str, object]:
        """Return a diagnostics payload for information_schema parity.

        Returns:
        -------
        dict[str, object]
            Serializable diagnostics payload.
        """
        return {
            "missing_in_information_schema": list(self.missing_in_information_schema),
            "param_name_mismatches": [
                {
                    "func_id": item.func_id,
                    "kind": item.kind,
                    "issue": item.issue,
                    "runtime_value": item.runtime_value,
                    "rust_value": item.rust_value,
                }
                for item in self.param_name_mismatches
            ],
            "routines_available": self.routines_available,
            "parameters_available": self.parameters_available,
            "error": self.error,
        }


def udf_parity_report(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object] | None = None,
) -> UdfParityReport:
    """Return parity mismatches for Rust registry snapshot metadata.

    Returns:
    -------
    UdfParityReport
        Summary of mismatches for diagnostics (empty when Rust snapshot is authoritative).
    """
    resolved_snapshot = snapshot or rust_udf_snapshot(ctx)
    validate_rust_udf_snapshot(resolved_snapshot)
    return UdfParityReport(
        missing_in_rust=(),
        param_name_mismatches=(),
        volatility_mismatches=(),
    )


def udf_info_schema_parity_report(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object] | None = None,
) -> UdfInfoSchemaParityReport:
    """Return parity mismatches between Rust registry and information_schema.

    Returns:
    -------
    UdfInfoSchemaParityReport
        Summary of parity mismatches for diagnostics.
    """
    resolved_snapshot = snapshot or rust_udf_snapshot(ctx)
    registry_names = set(snapshot_function_names(resolved_snapshot, include_aliases=True))
    table_names = _rust_table_function_names(resolved_snapshot)
    registry_names.difference_update(table_names)
    try:
        from datafusion_engine.catalog.introspection import introspection_cache_for_ctx
    except ImportError as exc:
        return UdfInfoSchemaParityReport(
            missing_in_information_schema=(),
            param_name_mismatches=(),
            routines_available=False,
            parameters_available=False,
            error=str(exc),
        )
    try:
        snapshot_tables = introspection_cache_for_ctx(ctx).snapshot
    except (RuntimeError, TypeError, ValueError) as exc:
        return UdfInfoSchemaParityReport(
            missing_in_information_schema=(),
            param_name_mismatches=(),
            routines_available=False,
            parameters_available=False,
            error=str(exc),
        )
    routines_table = snapshot_tables.routines
    if routines_table is None:
        return UdfInfoSchemaParityReport(
            missing_in_information_schema=(),
            param_name_mismatches=(),
            routines_available=False,
            parameters_available=False,
            error="information_schema.routines unavailable",
        )
    routines = list(routines_table.to_pylist())
    info_names: set[str] = set()
    for row in routines:
        name = row.get("routine_name") or row.get("function_name") or row.get("name")
        if isinstance(name, str) and name:
            info_names.add(name.lower())
    missing = sorted(name for name in registry_names if name.lower() not in info_names)
    parameters_table = snapshot_tables.parameters
    if parameters_table is None:
        return UdfInfoSchemaParityReport(
            missing_in_information_schema=tuple(missing),
            param_name_mismatches=(),
            routines_available=bool(info_names),
            parameters_available=False,
            error="information_schema.parameters unavailable",
        )
    param_rows = list(parameters_table.to_pylist())
    registry_params = snapshot_parameter_names(resolved_snapshot)
    allowed_names = {name.lower() for name in registry_names}
    filtered_params = {
        name: params
        for name, params in registry_params.items()
        if name.lower() in info_names and name.lower() in allowed_names
    }
    param_mismatches = _parameter_name_mismatches(
        registry_params=filtered_params,
        routines=routines,
        parameters=param_rows,
    )
    return UdfInfoSchemaParityReport(
        missing_in_information_schema=tuple(missing),
        param_name_mismatches=tuple(param_mismatches),
        routines_available=bool(info_names),
        parameters_available=True,
        error=None,
    )


def _rust_table_function_names(snapshot: Mapping[str, object]) -> set[str]:
    values = snapshot.get("table")
    if isinstance(values, Iterable) and not isinstance(values, (str, bytes)):
        return {str(value) for value in values if value is not None}
    return set()


def _parameter_name_mismatches(
    *,
    registry_params: Mapping[str, tuple[str, ...]],
    routines: Sequence[Mapping[str, object]],
    parameters: Sequence[Mapping[str, object]],
) -> list[UdfParityMismatch]:
    routine_lookup = _routine_specific_lookup(routines)
    per_specific = _parameter_entries(parameters, routine_lookup)
    info_param_names = _info_schema_param_sets(per_specific)
    return _registry_param_mismatches(registry_params, info_param_names)


def _routine_specific_lookup(
    routines: Sequence[Mapping[str, object]],
) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for row in routines:
        specific = row.get("specific_name")
        routine = row.get("routine_name") or row.get("function_name") or row.get("name")
        if isinstance(specific, str) and isinstance(routine, str):
            lookup[specific.lower()] = routine
    return lookup


def _parameter_entries(
    parameters: Sequence[Mapping[str, object]],
    routine_lookup: Mapping[str, str],
) -> dict[tuple[str, str], list[tuple[int, str]]]:
    per_specific: dict[tuple[str, str], list[tuple[int, str]]] = {}
    for row in parameters:
        entry = _parameter_entry(row, routine_lookup)
        if entry is None:
            continue
        key, position, param_name = entry
        per_specific.setdefault(key, []).append((position, param_name))
    return per_specific


def _parameter_entry(
    row: Mapping[str, object],
    routine_lookup: Mapping[str, str],
) -> tuple[tuple[str, str], int, str] | None:
    specific = row.get("specific_name")
    if not isinstance(specific, str):
        return None
    routine = routine_lookup.get(specific.lower())
    if routine is None:
        return None
    param_name = row.get("parameter_name")
    if not isinstance(param_name, str):
        return None
    ordinal = _parse_ordinal(row.get("ordinal_position"))
    if ordinal is None:
        return None
    return (routine.lower(), specific.lower()), ordinal, param_name


def _parse_ordinal(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _info_schema_param_sets(
    per_specific: Mapping[tuple[str, str], Sequence[tuple[int, str]]],
) -> dict[str, set[tuple[str, ...]]]:
    info_param_names: dict[str, set[tuple[str, ...]]] = {}
    for (routine, _specific), entries in per_specific.items():
        ordered = tuple(name for _, name in sorted(entries, key=lambda item: item[0]))
        info_param_names.setdefault(routine, set()).add(ordered)
    return info_param_names


def _registry_param_mismatches(
    registry_params: Mapping[str, tuple[str, ...]],
    info_param_names: Mapping[str, set[tuple[str, ...]]],
) -> list[UdfParityMismatch]:
    mismatches: list[UdfParityMismatch] = []
    for name, rust_params in registry_params.items():
        info_sets = info_param_names.get(name.lower())
        if not info_sets:
            mismatches.append(
                UdfParityMismatch(
                    func_id=name,
                    kind="info_schema",
                    issue="missing_parameters",
                    runtime_value=None,
                    rust_value=rust_params,
                )
            )
            continue
        if rust_params not in info_sets:
            mismatches.append(
                UdfParityMismatch(
                    func_id=name,
                    kind="info_schema",
                    issue="param_names",
                    runtime_value=sorted(info_sets),
                    rust_value=rust_params,
                )
            )
    return mismatches


def _rust_volatility(snapshot: Mapping[str, object]) -> dict[str, str]:
    values: dict[str, str] = {}
    raw = snapshot.get("volatility")
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            if key is None or value is None:
                continue
            values[str(key)] = str(value)
    return values


@dataclass(frozen=True)
class UdfSignatureConformanceReport:
    """Report on UDF signature completeness for scheduling.

    Validates that all UDFs have:
    - return_type implementations visible to information_schema
    - rewrite_tags coverage for planning domain classification
    """

    missing_return_type: tuple[str, ...]
    missing_rewrite_tags: tuple[str, ...]
    total_udf_count: int
    covered_rewrite_tags: tuple[str, ...]

    def payload(self) -> dict[str, object]:
        """Return a diagnostics payload for signature conformance.

        Returns:
        -------
        dict[str, object]
            Serializable diagnostics payload.
        """
        return {
            "missing_return_type": list(self.missing_return_type),
            "missing_rewrite_tags": list(self.missing_rewrite_tags),
            "total_udf_count": self.total_udf_count,
            "covered_rewrite_tags": list(self.covered_rewrite_tags),
            "return_type_coverage": (
                (self.total_udf_count - len(self.missing_return_type)) / self.total_udf_count
                if self.total_udf_count > 0
                else 1.0
            ),
            "rewrite_tags_coverage": (
                (self.total_udf_count - len(self.missing_rewrite_tags)) / self.total_udf_count
                if self.total_udf_count > 0
                else 1.0
            ),
        }


def udf_signature_conformance_report(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object] | None = None,
) -> UdfSignatureConformanceReport:
    """Build a conformance report for UDF signature completeness.

    Parameters
    ----------
    ctx
        DataFusion SessionContext for information_schema queries.
    snapshot
        Optional Rust UDF snapshot. If None, queries the current snapshot.

    Returns:
    -------
    UdfSignatureConformanceReport
        Report containing coverage metrics for return_type and rewrite_tags.
    """
    resolved_snapshot = snapshot if snapshot is not None else rust_udf_snapshot(ctx)
    validate_rust_udf_snapshot(resolved_snapshot)

    all_names = set(snapshot_function_names(resolved_snapshot, include_aliases=True))
    total_count = len(all_names)

    # Check return_type visibility via information_schema
    missing_return_type = _missing_return_types(ctx, all_names)

    # Check rewrite_tags coverage
    rewrite_tags = resolved_snapshot.get("rewrite_tags")
    covered_tags = set()
    missing_tags_names: list[str] = []
    if isinstance(rewrite_tags, Mapping):
        for name in all_names:
            tags = rewrite_tags.get(name)
            if tags is None or (isinstance(tags, Iterable) and not tags):
                missing_tags_names.append(name)
            elif isinstance(tags, Iterable):
                for tag in tags:
                    if tag is not None:
                        covered_tags.add(str(tag))
    else:
        missing_tags_names = list(all_names)

    return UdfSignatureConformanceReport(
        missing_return_type=tuple(sorted(missing_return_type)),
        missing_rewrite_tags=tuple(sorted(missing_tags_names)),
        total_udf_count=total_count,
        covered_rewrite_tags=tuple(sorted(covered_tags)),
    )


def _missing_return_types(
    ctx: SessionContext,
    udf_names: set[str],
) -> list[str]:
    """Return UDF names missing from information_schema routines.

    Returns:
    -------
    list[str]
        Names of UDFs missing from information_schema routines.
    """
    try:
        routines_df = ctx.sql_with_options(
            "SELECT routine_name FROM information_schema.routines",
            sql_options_for_profile(None),
        )
        routines_table = routines_df.collect()
    except (RuntimeError, ValueError, TypeError):
        return list(udf_names)
    if not routines_table:
        return list(udf_names)
    info_names = {
        str(row.get("routine_name")).lower()
        for batch in routines_table
        for row in batch.to_pylist()
        if row.get("routine_name") is not None
    }
    return [name for name in udf_names if name.lower() not in info_names]


__all__ = [
    "UdfInfoSchemaParityReport",
    "UdfParityMismatch",
    "UdfParityReport",
    "UdfSignatureConformanceReport",
    "udf_info_schema_parity_report",
    "udf_parity_report",
    "udf_signature_conformance_report",
]
