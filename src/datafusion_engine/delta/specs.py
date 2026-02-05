"""Msgspec-friendly Delta control-plane option specs."""

from __future__ import annotations

from collections.abc import Mapping

import msgspec

from serde_msgspec import StructBaseStrict


class DeltaAppTransactionSpec(StructBaseStrict, frozen=True):
    """Serializable app transaction spec for Delta commit options."""

    app_id: str
    version: int
    last_updated: int | None = None


class DeltaCommitOptionsSpec(StructBaseStrict, frozen=True):
    """Serializable commit options spec for Delta control-plane requests."""

    metadata: Mapping[str, object] = msgspec.field(default_factory=dict)
    app_transaction: DeltaAppTransactionSpec | None = None
    max_retries: int | None = None
    create_checkpoint: bool | None = None


class DeltaCdfOptionsSpec(StructBaseStrict, frozen=True):
    """Serializable CDF options spec for Delta control-plane requests."""

    starting_version: int = 0
    ending_version: int | None = None
    starting_timestamp: str | None = None
    ending_timestamp: str | None = None
    columns: tuple[str, ...] | None = None
    predicate: str | None = None
    allow_out_of_range: bool = False


__all__ = [
    "DeltaAppTransactionSpec",
    "DeltaCdfOptionsSpec",
    "DeltaCommitOptionsSpec",
]
