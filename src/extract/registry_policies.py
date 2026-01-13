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


__all__ = ["DatasetPolicyRow", "policy_row"]
