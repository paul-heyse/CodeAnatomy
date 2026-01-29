"""Normalization utilities for storage option mappings."""

from __future__ import annotations

from collections.abc import Mapping


def _stringify_mapping(values: Mapping[str, object] | None) -> dict[str, str]:
    if not values:
        return {}
    return {str(key): str(value) for key, value in values.items()}


def normalize_storage_options(
    storage_options: Mapping[str, object] | None,
    log_storage_options: Mapping[str, object] | None = None,
    *,
    fallback_log_to_storage: bool = False,
) -> tuple[dict[str, str] | None, dict[str, str] | None]:
    """Normalize storage and log storage option mappings.

    Parameters
    ----------
    storage_options
        Storage option mapping to normalize.
    log_storage_options
        Log storage option mapping to normalize.
    fallback_log_to_storage
        Whether to mirror storage options into log storage when absent.

    Returns
    -------
    tuple[dict[str, str] | None, dict[str, str] | None]
        Normalized storage and log storage mappings (or ``None`` when empty).
    """
    storage = _stringify_mapping(storage_options)
    log_storage = _stringify_mapping(log_storage_options)
    if fallback_log_to_storage and storage and not log_storage:
        log_storage = dict(storage)
    return storage or None, log_storage or None


def merged_storage_options(
    storage_options: Mapping[str, object] | None,
    log_storage_options: Mapping[str, object] | None = None,
    *,
    fallback_log_to_storage: bool = False,
) -> dict[str, str] | None:
    """Return a merged storage options mapping.

    Returns
    -------
    dict[str, str] | None
        Combined storage option mapping, or ``None`` when empty.
    """
    storage, log_storage = normalize_storage_options(
        storage_options,
        log_storage_options,
        fallback_log_to_storage=fallback_log_to_storage,
    )
    merged: dict[str, str] = {}
    if storage:
        merged.update(storage)
    if log_storage:
        merged.update(log_storage)
    return merged or None


__all__ = [
    "merged_storage_options",
    "normalize_storage_options",
]
