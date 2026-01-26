"""Parity checks between Rust UDF registry snapshots and Ibis metadata."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
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


def _rust_volatility(snapshot: Mapping[str, object]) -> dict[str, str]:
    values: dict[str, str] = {}
    raw = snapshot.get("volatility")
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            if key is None or value is None:
                continue
            values[str(key)] = str(value)
    return values


__all__ = ["UdfParityMismatch", "UdfParityReport", "udf_parity_report"]
