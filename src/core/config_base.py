"""Base configuration fingerprinting helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from serde_msgspec import to_builtins
from utils.hashing import hash_msgpack_canonical


class FingerprintableConfig(Protocol):
    """Protocol for configs that provide fingerprint payloads."""

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return a canonical payload for fingerprinting."""
        ...


def config_fingerprint(payload: Mapping[str, object]) -> str:
    """Return a deterministic fingerprint for configuration payloads.

    Parameters
    ----------
    payload
        Mapping of configuration values.

    Returns
    -------
    str
        SHA-256 hexdigest for the payload.
    """
    normalized = to_builtins(payload, str_keys=True)
    return hash_msgpack_canonical(normalized)


__all__ = ["FingerprintableConfig", "config_fingerprint"]
