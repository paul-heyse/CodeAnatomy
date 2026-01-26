"""Parity checks between Rust UDF registry snapshots and Ibis metadata."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from datafusion_engine.udf_runtime import rust_udf_snapshot

if TYPE_CHECKING:
    from datafusion import SessionContext

    from ibis_engine.builtin_udfs import IbisUdfSpec


@dataclass(frozen=True)
class UdfParityMismatch:
    """Record a mismatch between Rust registry and Ibis metadata."""

    func_id: str
    kind: str
    issue: str
    ibis_value: object | None = None
    rust_value: object | None = None


@dataclass(frozen=True)
class UdfParityReport:
    """Summary of UDF parity mismatches."""

    missing_in_rust: tuple[str, ...]
    param_name_mismatches: tuple[UdfParityMismatch, ...]
    volatility_mismatches: tuple[UdfParityMismatch, ...]

    def payload(self) -> dict[str, object]:
        """Return a diagnostics payload for parity mismatches.

        Returns
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
                    "ibis_value": item.ibis_value,
                    "rust_value": item.rust_value,
                }
                for item in self.param_name_mismatches
            ],
            "volatility_mismatches": [
                {
                    "func_id": item.func_id,
                    "kind": item.kind,
                    "issue": item.issue,
                    "ibis_value": item.ibis_value,
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
        """Return a diagnostics payload for information_schema parity."""
        return {
            "missing_in_information_schema": list(self.missing_in_information_schema),
            "param_name_mismatches": [
                {
                    "func_id": item.func_id,
                    "kind": item.kind,
                    "issue": item.issue,
                    "ibis_value": item.ibis_value,
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
    ibis_specs: Iterable[IbisUdfSpec],
) -> UdfParityReport:
    """Return parity mismatches between Rust registry snapshot and Ibis specs.

    Returns
    -------
    UdfParityReport
        Summary of mismatches for diagnostics.
    """
    snapshot = rust_udf_snapshot(ctx)
    rust_names = _rust_function_names(snapshot)
    rust_params = _rust_param_names(snapshot)
    rust_volatility = _rust_volatility(snapshot)

    missing: list[str] = []
    param_mismatches: list[UdfParityMismatch] = []
    volatility_mismatches: list[UdfParityMismatch] = []

    for spec in ibis_specs:
        if spec.func_id not in rust_names and spec.engine_name not in rust_names:
            missing.append(spec.func_id)
            continue
        if spec.arg_names is not None:
            rust_values = rust_params.get(spec.engine_name) or rust_params.get(spec.func_id)
            if rust_values is not None and tuple(rust_values) != tuple(spec.arg_names):
                param_mismatches.append(
                    UdfParityMismatch(
                        func_id=spec.func_id,
                        kind=spec.kind,
                        issue="param_names",
                        ibis_value=spec.arg_names,
                        rust_value=tuple(rust_values),
                    )
                )
        rust_vol = rust_volatility.get(spec.engine_name) or rust_volatility.get(spec.func_id)
        if rust_vol is not None and rust_vol != spec.volatility:
            volatility_mismatches.append(
                UdfParityMismatch(
                    func_id=spec.func_id,
                    kind=spec.kind,
                    issue="volatility",
                    ibis_value=spec.volatility,
                    rust_value=rust_vol,
                )
            )

    return UdfParityReport(
        missing_in_rust=tuple(sorted(set(missing))),
        param_name_mismatches=tuple(param_mismatches),
        volatility_mismatches=tuple(volatility_mismatches),
    )


def udf_info_schema_parity_report(
    ctx: SessionContext,
    *,
    snapshot: Mapping[str, object] | None = None,
) -> UdfInfoSchemaParityReport:
    """Return parity mismatches between Rust registry and information_schema.

    Returns
    -------
    UdfInfoSchemaParityReport
        Summary of parity mismatches for diagnostics.
    """
    resolved_snapshot = snapshot or rust_udf_snapshot(ctx)
    registry_names = _rust_function_names(resolved_snapshot)
    try:
        from datafusion_engine.introspection import introspection_cache_for_ctx
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
    param_mismatches = _parameter_name_mismatches(
        registry_params=_rust_param_names(resolved_snapshot),
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


def _rust_function_names(snapshot: Mapping[str, object]) -> set[str]:
    names: set[str] = set()
    for key in ("scalar", "aggregate", "window", "table"):
        values = snapshot.get(key)
        if isinstance(values, Iterable):
            for value in values:
                if value is None:
                    continue
                names.add(str(value))
    aliases = snapshot.get("aliases")
    if isinstance(aliases, Mapping):
        for alias, target in aliases.items():
            if alias is not None:
                names.add(str(alias))
            if target is not None:
                names.add(str(target))
    return names


def _rust_param_names(snapshot: Mapping[str, object]) -> dict[str, tuple[str, ...]]:
    params: dict[str, tuple[str, ...]] = {}
    raw = snapshot.get("parameter_names")
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            if key is None:
                continue
            if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
                params[str(key)] = tuple(str(item) for item in value if item is not None)
    return params


def _parameter_name_mismatches(
    *,
    registry_params: Mapping[str, tuple[str, ...]],
    routines: Sequence[Mapping[str, object]],
    parameters: Sequence[Mapping[str, object]],
) -> list[UdfParityMismatch]:
    specific_to_routine: dict[str, str] = {}
    for row in routines:
        specific = row.get("specific_name")
        routine = row.get("routine_name") or row.get("function_name") or row.get("name")
        if isinstance(specific, str) and isinstance(routine, str):
            specific_to_routine[specific.lower()] = routine
    info_param_names: dict[str, set[tuple[str, ...]]] = {}
    per_specific: dict[tuple[str, str], list[tuple[int, str]]] = {}
    for row in parameters:
        specific = row.get("specific_name")
        if not isinstance(specific, str):
            continue
        routine = specific_to_routine.get(specific.lower())
        if routine is None:
            continue
        ordinal = row.get("ordinal_position")
        param_name = row.get("parameter_name")
        if not isinstance(param_name, str):
            continue
        if isinstance(ordinal, bool) or ordinal is None:
            continue
        if isinstance(ordinal, int):
            position = ordinal
        elif isinstance(ordinal, float):
            position = int(ordinal)
        elif isinstance(ordinal, str) and ordinal.isdigit():
            position = int(ordinal)
        else:
            continue
        key = (routine.lower(), specific.lower())
        per_specific.setdefault(key, []).append((position, param_name))
    for (routine, _specific), entries in per_specific.items():
        ordered = tuple(name for _, name in sorted(entries, key=lambda item: item[0]))
        info_param_names.setdefault(routine, set()).add(ordered)
    mismatches: list[UdfParityMismatch] = []
    for name, rust_params in registry_params.items():
        info_sets = info_param_names.get(name.lower())
        if not info_sets:
            mismatches.append(
                UdfParityMismatch(
                    func_id=name,
                    kind="info_schema",
                    issue="missing_parameters",
                    ibis_value=None,
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
                    ibis_value=sorted(info_sets),
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


__all__ = [
    "UdfInfoSchemaParityReport",
    "UdfParityMismatch",
    "UdfParityReport",
    "udf_info_schema_parity_report",
    "udf_parity_report",
]
