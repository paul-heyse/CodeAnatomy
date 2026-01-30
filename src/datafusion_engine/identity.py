"""Identity payload and fingerprint helpers."""

from __future__ import annotations

from collections.abc import Mapping

from datafusion_engine.arrow.abi import schema_abi_payload
from datafusion_engine.arrow.interop import SchemaLike
from utils.hashing import hash_msgpack_canonical


def build_identity_payload(
    *,
    sources: Mapping[str, object] | None = None,
    view: Mapping[str, object] | None = None,
) -> dict[str, object]:
    """Build a canonical identity payload.

    Parameters
    ----------
    sources
        Mapping of source dataset identities.
    view
        Mapping describing the view definition identity.

    Returns
    -------
    dict[str, object]
        Canonical identity payload.
    """
    return {
        "sources": dict(sources or {}),
        "view": dict(view or {}),
    }


def identity_fingerprint(payload: Mapping[str, object]) -> str:
    """Return a deterministic fingerprint for an identity payload.

    Parameters
    ----------
    payload
        Identity payload mapping.

    Returns
    -------
    str
        SHA-256 hash of the canonical MessagePack payload.
    """
    return hash_msgpack_canonical(payload)


def schema_identity_payload(schema: SchemaLike) -> dict[str, object]:
    """Return an identity payload for a schema.

    Parameters
    ----------
    schema
        Arrow schema to capture.

    Returns
    -------
    dict[str, object]
        Identity payload for the schema.
    """
    return {"schema": schema_abi_payload(schema)}


def schema_identity_hash(schema: SchemaLike) -> str:
    """Return an identity hash for a schema payload.

    Parameters
    ----------
    schema
        Arrow schema to fingerprint.

    Returns
    -------
    str
        Schema identity hash.
    """
    return identity_fingerprint(schema_identity_payload(schema))


__all__ = [
    "build_identity_payload",
    "identity_fingerprint",
    "schema_identity_hash",
    "schema_identity_payload",
]
