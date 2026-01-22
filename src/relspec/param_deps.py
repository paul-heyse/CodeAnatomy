"""Helpers for parameter-table dependency inference."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from ibis_engine.param_tables import ParamTablePolicy, ParamTableSpec
from sqlglot_tools.lineage import TableRef


@dataclass(frozen=True)
class ParamDep:
    """Param table dependency metadata."""

    logical_name: str
    table_name: str


@dataclass(frozen=True)
class RuleDependencyReport:
    """Rule-level dependency report."""

    rule_name: str
    param_tables: tuple[str, ...]
    dataset_tables: tuple[str, ...] = ()


@dataclass(frozen=True)
class ActiveParamSet:
    """Active param-table logical names inferred for a run."""

    active: frozenset[str]


def infer_param_deps(
    table_refs: Sequence[TableRef],
    *,
    policy: ParamTablePolicy,
) -> tuple[ParamDep, ...]:
    """Infer param-table dependencies from table references.

    Returns
    -------
    tuple[ParamDep, ...]
        Param-table dependencies inferred from table references.
    """
    deps: set[ParamDep] = set()
    for ref in table_refs:
        if not ref.name.startswith(policy.prefix):
            continue
        logical = ref.name[len(policy.prefix) :]
        deps.add(ParamDep(logical_name=logical, table_name=ref.name))
    return tuple(sorted(deps, key=lambda dep: dep.logical_name))


def validate_param_deps(
    *,
    rule_name: str,
    deps: Sequence[ParamDep],
    specs: Mapping[str, ParamTableSpec],
) -> None:
    """Validate inferred param deps against declared param specs.

    Raises
    ------
    ValueError
        Raised when a rule references undeclared param tables.
    """
    missing = sorted(dep.logical_name for dep in deps if dep.logical_name not in specs)
    if missing:
        msg = f"Rule {rule_name!r} references undeclared param tables: {missing}."
        raise ValueError(msg)


def dataset_table_names(
    table_refs: Sequence[TableRef],
    *,
    policy: ParamTablePolicy,
) -> tuple[str, ...]:
    """Return non-parameter table names referenced by a rule.

    Returns
    -------
    tuple[str, ...]
        Sorted dataset table names referenced by the rule.
    """
    names = {ref.name for ref in table_refs if not ref.name.startswith(policy.prefix)}
    return tuple(sorted(names))


def build_param_reverse_index(
    reports: Sequence[RuleDependencyReport],
) -> dict[str, tuple[str, ...]]:
    """Build a reverse index mapping param names to rule names.

    Returns
    -------
    dict[str, tuple[str, ...]]
        Param-name reverse index with sorted rule names.
    """
    reverse: dict[str, list[str]] = {}
    for report in reports:
        for name in report.param_tables:
            reverse.setdefault(name, []).append(report.rule_name)
    return {key: tuple(sorted(values)) for key, values in reverse.items()}


__all__ = [
    "ActiveParamSet",
    "ParamDep",
    "RuleDependencyReport",
    "build_param_reverse_index",
    "dataset_table_names",
    "infer_param_deps",
    "validate_param_deps",
]
