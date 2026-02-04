"""Base configuration fingerprinting helpers.

Fingerprints must remain stable across releases. When a payload is expected
to evolve, include an explicit version and compose sub-hashes (for example
via CompositeFingerprint) instead of re-serializing large objects directly.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol, runtime_checkable

from utils.hashing import hash_json_canonical


@runtime_checkable
class FingerprintableConfig(Protocol):
    """Protocol for configs that provide fingerprint payloads.

    Implementations should:
    - include a version key when the payload may evolve,
    - avoid embedding large nested payloads directly (prefer component hashes),
    - keep keys and values JSON-compatible for canonical hashing.
    """

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
