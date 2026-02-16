"""Delta Lake write entry points."""

from __future__ import annotations

from collections.abc import Mapping

from deltalake import CommitProperties, Transaction

from storage.deltalake.delta_read import (
    DeltaWriteResult,
    IdempotentWriteOptions,
    _normalize_commit_metadata,
)


def build_commit_properties(
    *,
    app_id: str | None = None,
    version: int | None = None,
    commit_metadata: Mapping[str, str] | None = None,
) -> CommitProperties | None:
    """Build commit metadata properties for Delta writes.

    Returns:
    -------
    CommitProperties | None
        Commit properties payload when metadata/transaction info is present.
    """
    custom_metadata = _normalize_commit_metadata(commit_metadata)
    app_transactions = None
    if app_id is not None and version is not None:
        app_transactions = [Transaction(app_id=app_id, version=version)]
    if custom_metadata is None and app_transactions is None:
        return None
    return CommitProperties(
        app_transactions=app_transactions,
        custom_metadata=custom_metadata,
    )


def idempotent_commit_properties(
    *,
    operation: str,
    mode: str,
    idempotent: IdempotentWriteOptions | None = None,
    extra_metadata: Mapping[str, str] | None = None,
) -> CommitProperties:
    """Build idempotent commit metadata properties for Delta writes.

    Returns:
    -------
    CommitProperties
        Commit properties with deterministic operation metadata.

    Raises:
        RuntimeError: If commit metadata cannot be constructed.
    """
    metadata: dict[str, str] = {
        "codeanatomy_operation": str(operation),
        "codeanatomy_mode": str(mode),
    }
    if extra_metadata:
        metadata.update({str(key): str(value) for key, value in extra_metadata.items()})
    app_id = idempotent.app_id if idempotent is not None else None
    version = idempotent.version if idempotent is not None else None
    commit_properties = build_commit_properties(
        app_id=app_id,
        version=version,
        commit_metadata=metadata,
    )
    if commit_properties is None:
        msg = "idempotent_commit_properties requires commit metadata."
        raise RuntimeError(msg)
    return commit_properties


__all__ = [
    "DeltaWriteResult",
    "IdempotentWriteOptions",
    "build_commit_properties",
    "idempotent_commit_properties",
]
