"""Dataset policy overrides for extract schemas."""

from __future__ import annotations

from dataclasses import dataclass

from arrowdsl.schema.schema import CastErrorPolicy


@dataclass(frozen=True)
class DatasetPolicyRow:
    """Optional schema policy overrides for a dataset."""

    name: str
    safe_cast: bool | None = None
    keep_extra_columns: bool | None = None
    on_error: CastErrorPolicy | None = None


@dataclass(frozen=True)
class TemplatePolicyRow:
    """Optional schema policy overrides for a template."""

    template: str
    safe_cast: bool | None = None
    keep_extra_columns: bool | None = None
    on_error: CastErrorPolicy | None = None


_TEMPLATE_POLICIES: dict[str, TemplatePolicyRow] = {
    "repo_scan": TemplatePolicyRow(template="repo_scan", keep_extra_columns=True),
    "runtime_inspect": TemplatePolicyRow(
        template="runtime_inspect",
        keep_extra_columns=True,
    ),
    "scip": TemplatePolicyRow(template="scip", keep_extra_columns=False),
}
_POLICY_ROWS: tuple[DatasetPolicyRow, ...] = ()
_POLICY_BY_NAME: dict[str, DatasetPolicyRow] = {row.name: row for row in _POLICY_ROWS}


def policy_row(name: str) -> DatasetPolicyRow | None:
    """Return the policy row for a dataset name.

    Returns
    -------
    DatasetPolicyRow | None
        Policy overrides or ``None`` when not configured.
    """
    return _POLICY_BY_NAME.get(name)


def template_policy_row(template: str | None) -> TemplatePolicyRow | None:
    """Return the policy row for a template name.

    Returns
    -------
    TemplatePolicyRow | None
        Policy overrides or ``None`` when not configured.
    """
    if template is None:
        return None
    return _TEMPLATE_POLICIES.get(template)


__all__ = [
    "DatasetPolicyRow",
    "TemplatePolicyRow",
    "policy_row",
    "template_policy_row",
]
