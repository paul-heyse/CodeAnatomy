"""Validation helpers for extract registry rows."""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from extract.registry_bundles import bundle
from extract.registry_fields import field_name
from extract.registry_rows import DATASET_ROWS, DatasetRow


def validate_dataset_rows(rows: Sequence[DatasetRow] | None = None) -> None:
    """Validate extract dataset registry rows."""
    for row in rows or DATASET_ROWS:
        available = _available_fields(row)
        _validate_join_keys(row, available)
        _validate_derived_ids(row, available)


def _available_fields(row: DatasetRow) -> set[str]:
    keys = set(row.fields) | set(row.row_fields) | set(row.row_extras)
    names = {field_name(key) for key in keys}
    bundle_fields = _bundle_field_names(row.bundles)
    return keys | names | bundle_fields


def _bundle_field_names(names: Iterable[str]) -> set[str]:
    fields: set[str] = set()
    for bundle_name in names:
        fields.update(field.name for field in bundle(bundle_name).fields)
    return fields


def _validate_join_keys(row: DatasetRow, available: set[str]) -> None:
    missing = sorted(key for key in row.join_keys if key not in available)
    if missing:
        msg = f"Extract dataset join_keys reference missing columns for {row.name!r}: {missing}"
        raise ValueError(msg)


def _validate_derived_ids(row: DatasetRow, available: set[str]) -> None:
    for spec in row.derived:
        missing = sorted(key for key in spec.required if key not in available)
        if missing:
            msg = (
                "Extract derived id spec references missing columns for "
                f"{row.name!r} derived {spec.name!r}: {missing}"
            )
            raise ValueError(msg)


__all__ = ["validate_dataset_rows"]
