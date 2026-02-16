"""Named commit payload parts for Rust Delta control-plane entrypoints."""

from __future__ import annotations

from dataclasses import dataclass

type CommitPayloadTuple = tuple[
    list[tuple[str, str]] | None,
    str | None,
    int | None,
    int | None,
    int | None,
    bool | None,
]


@dataclass(frozen=True)
class CommitPayloadParts:
    """Named commit payload components returned by ``delta.payload.commit_payload``."""

    metadata_payload: list[tuple[str, str]] | None
    app_id: str | None
    app_version: int | None
    app_last_updated: int | None
    max_retries: int | None
    create_checkpoint: bool | None


def commit_payload_parts(values: CommitPayloadTuple) -> CommitPayloadParts:
    """Convert positional commit payload tuple to named fields.

    Returns:
    -------
    CommitPayloadParts
        Named payload parts view of the positional tuple.
    """
    return CommitPayloadParts(
        metadata_payload=values[0],
        app_id=values[1],
        app_version=values[2],
        app_last_updated=values[3],
        max_retries=values[4],
        create_checkpoint=values[5],
    )


__all__ = ["CommitPayloadParts", "commit_payload_parts"]
