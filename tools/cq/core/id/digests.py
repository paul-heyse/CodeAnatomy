"""Stable digest derivation from canonical payloads."""

from __future__ import annotations

import hashlib

import msgspec

from tools.cq.core.id.canonical import canonicalize_payload


def stable_digest(value: object) -> str:
    """Return SHA-256 digest of canonicalized payload value.

    Returns:
        str: Full hex SHA-256 digest.
    """
    canonical = canonicalize_payload(value)
    return hashlib.sha256(msgspec.json.encode(canonical)).hexdigest()


def stable_digest24(value: object) -> str:
    """Return 24-char stable digest for compact deterministic IDs.

    Returns:
        str: 24-character deterministic digest prefix.
    """
    return stable_digest(value)[:24]


__all__ = ["stable_digest", "stable_digest24"]
