"""Base configuration fingerprinting helpers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, runtime_checkable

from utils.hashing import hash_json_canonical


@runtime_checkable
class FingerprintableConfig(Protocol):
    """Protocol for configs that provide fingerprint payloads."""

    def fingerprint_payload(self) -> Mapping[str, object]:
        """Return a canonical payload for fingerprinting."""
        ...

    def fingerprint(self) -> str:
        """Return a deterministic fingerprint for the payload.

        Returns
        -------
        str
            Deterministic fingerprint for the payload.
        """
        return config_fingerprint(self.fingerprint_payload())


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
    return hash_json_canonical(payload, str_keys=True)


__all__ = ["FingerprintableConfig", "config_fingerprint"]
