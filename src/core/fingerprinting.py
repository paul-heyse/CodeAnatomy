"""Composite fingerprinting utilities."""

from __future__ import annotations

from collections.abc import Mapping

from serde_msgspec import StructBaseStrict
from utils.hashing import CacheKeyBuilder


class FingerprintComponent(StructBaseStrict, frozen=True):
    """Single component of a composite fingerprint."""

    name: str
    value: str
    algorithm: str = "sha256"
    truncated: bool = False


class CompositeFingerprint(StructBaseStrict, frozen=True):
    """Composite fingerprint with canonical cache-key encoding."""

    version: int
    components: tuple[FingerprintComponent, ...]

    @classmethod
    def from_components(
        cls,
        version: int,
        **components: str,
    ) -> CompositeFingerprint:
        """Create a composite fingerprint from named components.

        Returns
        -------
        CompositeFingerprint
            Composite fingerprint instance.
        """
        return cls(
            version=version,
            components=tuple(
                FingerprintComponent(name=name, value=value)
                for name, value in sorted(components.items())
            ),
        )

    def as_cache_key(self, *, prefix: str = "") -> str:
        """Return a deterministic cache key for the fingerprint.

        Returns
        -------
        str
            Cache key string.
        """
        builder = CacheKeyBuilder(prefix=prefix)
        builder.add("version", self.version)
        builder.add(
            "components", {component.name: component.value for component in self.components}
        )
        return builder.build()

    def payload(self) -> Mapping[str, object]:
        """Return a mapping payload for diagnostics/serialization.

        Returns
        -------
        Mapping[str, object]
            Serialized fingerprint payload.
        """
        return {
            "version": self.version,
            "components": {
                component.name: {
                    "value": component.value,
                    "algorithm": component.algorithm,
                    "truncated": component.truncated,
                }
                for component in self.components
            },
        }

    def extend(self, **additional: str) -> CompositeFingerprint:
        """Return a new fingerprint with additional components.

        Returns
        -------
        CompositeFingerprint
            New composite fingerprint.
        """
        merged: dict[str, FingerprintComponent] = {
            component.name: component for component in self.components
        }
        for name, value in additional.items():
            merged[name] = FingerprintComponent(name=name, value=value)
        return CompositeFingerprint(
            version=self.version,
            components=tuple(merged[name] for name in sorted(merged)),
        )


__all__ = ["CompositeFingerprint", "FingerprintComponent"]
