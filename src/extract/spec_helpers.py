"""Programmatic extractor option helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from functools import cache

from arrowdsl.schema.metadata import extractor_option_defaults_from_metadata
from extract.evidence_plan import EvidencePlan
from extract.registry_rows import DATASET_ROWS, DatasetRow
from extract.registry_specs import dataset_schema, extractor_defaults
from relspec.rules.options import RuleExecutionOptions


def _rows_for_template(template_name: str) -> tuple[DatasetRow, ...]:
    return tuple(row for row in DATASET_ROWS if row.template == template_name)


@cache
def _feature_flag_rows(template_name: str) -> Mapping[str, tuple[DatasetRow, ...]]:
    flags: dict[str, list[DatasetRow]] = {}
    for row in _rows_for_template(template_name):
        if row.feature_flag is None:
            continue
        flags.setdefault(row.feature_flag, []).append(row)
    return {flag: tuple(rows) for flag, rows in flags.items()}


@cache
def _metadata_defaults(template_name: str) -> dict[str, object]:
    rows = _rows_for_template(template_name)
    if not rows:
        return {}
    schema = dataset_schema(rows[0].name)
    return extractor_option_defaults_from_metadata(schema)


def plan_requires_row(plan: EvidencePlan, row: DatasetRow) -> bool:
    """Return whether the evidence plan requires the dataset row.

    Returns
    -------
    bool
        ``True`` when the plan requires the dataset.
    """
    return plan.requires_dataset(row.output_name()) or plan.requires_dataset(row.name)


def plan_feature_flags(template_name: str, plan: EvidencePlan | None) -> dict[str, bool]:
    """Return feature-flag overrides derived from an evidence plan.

    Returns
    -------
    dict[str, bool]
        Feature flag values when a plan is provided.
    """
    if plan is None:
        return {}
    flag_rows = _feature_flag_rows(template_name)
    if not plan.requires_template(template_name):
        return dict.fromkeys(flag_rows, False)
    resolved: dict[str, bool] = {}
    for flag, rows in flag_rows.items():
        resolved[flag] = any(plan_requires_row(plan, row) for row in rows)
    return resolved


def _flag_value(values: Mapping[str, object], name: str, *, fallback: bool) -> bool:
    value = values.get(name, fallback)
    return value if isinstance(value, bool) else fallback


def _apply_cst_derivations(
    values: dict[str, object],
    *,
    override_keys: set[str],
) -> None:
    if "compute_expr_context" not in override_keys:
        values["compute_expr_context"] = _flag_value(
            values,
            "include_name_refs",
            fallback=True,
        )
    if "compute_qualified_names" not in override_keys:
        include_callsites = _flag_value(values, "include_callsites", fallback=True)
        include_defs = _flag_value(values, "include_defs", fallback=True)
        values["compute_qualified_names"] = include_callsites or include_defs


def _apply_derived_options(
    template_name: str,
    values: dict[str, object],
    *,
    override_keys: set[str],
) -> None:
    if template_name == "cst":
        _apply_cst_derivations(values, override_keys=override_keys)


def extractor_option_values(
    template_name: str,
    plan: EvidencePlan | None,
    *,
    overrides: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Return merged extractor option values for a template.

    Returns
    -------
    dict[str, object]
        Extractor option values derived from registry defaults and plan flags.
    """
    execution = rule_execution_options(template_name, plan, overrides=overrides)
    values = extractor_defaults(template_name)
    values.update(execution.metadata_defaults)
    values.update(execution.feature_flags)
    override_values: dict[str, object] = dict(overrides or {})
    if override_values:
        values.update(override_values)
    _apply_derived_options(
        template_name,
        values,
        override_keys=set(override_values),
    )
    return values


def rule_execution_options(
    template_name: str,
    plan: EvidencePlan | None,
    *,
    overrides: Mapping[str, object] | None = None,
) -> RuleExecutionOptions:
    """Return execution options for template gating.

    Returns
    -------
    RuleExecutionOptions
        Execution options for stage gating.
    """
    override_values = dict(overrides or {})
    module_allowlist = _module_allowlist_from_overrides(override_values)
    metadata_defaults = _metadata_defaults(template_name)
    feature_flags = plan_feature_flags(template_name, plan)
    feature_flags.update(_bool_overrides(override_values))
    return RuleExecutionOptions(
        module_allowlist=module_allowlist,
        feature_flags=feature_flags,
        metadata_defaults=metadata_defaults,
    )


def _module_allowlist_from_overrides(overrides: Mapping[str, object]) -> tuple[str, ...]:
    value = overrides.get("module_allowlist", ())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(str(item) for item in value)
    return ()


def _bool_overrides(overrides: Mapping[str, object]) -> dict[str, bool]:
    return {key: value for key, value in overrides.items() if isinstance(value, bool)}


__all__ = [
    "extractor_option_values",
    "plan_feature_flags",
    "plan_requires_row",
    "rule_execution_options",
]
