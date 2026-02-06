"""Standard registry protocols and base implementations.

Use these base classes for simple key/value registries with minimal behavior.
Registries that perform richer validation, caching, or computed lookups should
prefer composition over inheritance to avoid coupling to the base API.
"""

from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Protocol, TypeVar, runtime_checkable

K = TypeVar("K")
V = TypeVar("V")


@runtime_checkable
class Registry(Protocol[K, V]):
    """Protocol for registry implementations."""

    @abstractmethod
    def register(self, key: K, value: V) -> None:
        """Register a value with the given key."""

    @abstractmethod
    def get(self, key: K) -> V | None:
        """Retrieve a value by key, or None if not found."""

    @abstractmethod
    def __contains__(self, key: K) -> bool:
        """Check if key is registered."""

    @abstractmethod
    def __iter__(self) -> Iterator[K]:
        """Iterate over registered keys."""

    @abstractmethod
    def __len__(self) -> int:
        """Return count of registered items."""


@runtime_checkable
class SnapshotRegistry(Protocol[K, V]):
    """Protocol for registries that support snapshot/restore."""

    @abstractmethod
    def snapshot(self) -> Mapping[K, V]:
        """Return a snapshot of the registry state."""

    @abstractmethod
    def restore(self, snapshot: Mapping[K, V]) -> None:
        """Restore the registry state from a snapshot."""


@dataclass
class MutableRegistry[K, V]:
    """Standard mutable registry with dict storage."""

    _entries: dict[K, V] = field(default_factory=dict)

    def register(self, key: K, value: V, *, overwrite: bool = False) -> None:
        """Register a value for the provided key.

        Args:
            key: Description.
            value: Description.
            overwrite: Description.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if key in self._entries and not overwrite:
            msg = f"Key {key!r} already registered. Use overwrite=True."
            raise ValueError(msg)
        self._entries[key] = value

    def get(self, key: K) -> V | None:
        """Retrieve a value by key.

        Parameters
        ----------
        key
            Key to look up.

        Returns:
        -------
        V | None
            Registered value, or ``None`` when missing.
        """
        return self._entries.get(key)

    def __contains__(self, key: K) -> bool:
        """Check whether a key is registered.

        Parameters
        ----------
        key
            Key to check.

        Returns:
        -------
        bool
            ``True`` if the key is registered.
        """
        return key in self._entries

    def __iter__(self) -> Iterator[K]:
        """Iterate over registered keys.

        Returns:
        -------
        Iterator[K]
            Iterator over registered keys.
        """
        return iter(self._entries)

    def __len__(self) -> int:
        """Return the count of registered entries.

        Returns:
        -------
        int
            Number of registered entries.
        """
        return len(self._entries)

    def items(self) -> Iterator[tuple[K, V]]:
        """Iterate over registry entries.

        Returns:
        -------
        Iterator[tuple[K, V]]
            Iterator over key/value pairs.
        """
        return iter(self._entries.items())

    def snapshot(self) -> Mapping[K, V]:
        """Return immutable snapshot of current state.

        Returns:
        -------
        Mapping[K, V]
            Snapshot of registry entries.
        """
        return dict(self._entries)

    def restore(self, snapshot: Mapping[K, V]) -> None:
        """Restore registry entries from a snapshot.

        Parameters
        ----------
        snapshot
            Mapping of registry entries to restore.
        """
        self._entries = dict(snapshot)


@dataclass(frozen=True)
class ImmutableRegistry[K, V]:
    """Frozen registry built from a sequence of entries."""

    _entries: tuple[tuple[K, V], ...]

    def get(self, key: K) -> V | None:
        """Retrieve a value by key.

        Parameters
        ----------
        key
            Key to look up.

        Returns:
        -------
        V | None
            Registered value, or ``None`` when missing.
        """
        for k, v in self._entries:
            if k == key:
                return v
        return None

    def __contains__(self, key: K) -> bool:
        """Check whether a key is registered.

        Parameters
        ----------
        key
            Key to check.

        Returns:
        -------
        bool
            ``True`` if the key is registered.
        """
        return any(k == key for k, _ in self._entries)

    def __iter__(self) -> Iterator[K]:
        """Iterate over registered keys.

        Returns:
        -------
        Iterator[K]
            Iterator over registered keys.
        """
        return (k for k, _ in self._entries)

    def __len__(self) -> int:
        """Return the count of registered entries.

        Returns:
        -------
        int
            Number of registered entries.
        """
        return len(self._entries)

    @classmethod
    def from_dict(cls, data: Mapping[K, V]) -> ImmutableRegistry[K, V]:
        """Create an immutable registry from a mapping.

        Parameters
        ----------
        data
            Source mapping of entries.

        Returns:
        -------
        ImmutableRegistry[K, V]
            Frozen registry containing the mapping entries.
        """
        return cls(tuple(data.items()))


@dataclass
class MappingRegistryAdapter[K, V]:
    """Adapter for mapping-backed registries with optional read-only mode."""

    entries: dict[K, V] = field(default_factory=dict)
    read_only: bool = False
    allow_overwrite: bool = False

    def register(self, key: K, value: V) -> None:
        """Register a value for the provided key.

        Args:
            key: Description.
            value: Description.

        Raises:
            ValueError: If the operation cannot be completed.
        """
        if self.read_only:
            msg = "Registry is read-only."
            raise ValueError(msg)
        if key in self.entries and not self.allow_overwrite:
            msg = f"Key {key!r} already registered. Use allow_overwrite=True."
            raise ValueError(msg)
        self.entries[key] = value

    def get(self, key: K) -> V | None:
        """Retrieve a value by key.

        Returns:
        -------
        V | None
            Registered value, or ``None`` when missing.
        """
        return self.entries.get(key)

    def __contains__(self, key: K) -> bool:
        """Check whether a key is registered.

        Returns:
        -------
        bool
            ``True`` when the key is registered.
        """
        return key in self.entries

    def __iter__(self) -> Iterator[K]:
        """Iterate over registered keys.

        Returns:
        -------
        Iterator[K]
            Iterator over registered keys.
        """
        return iter(self.entries)

    def __len__(self) -> int:
        """Return the count of registered entries.

        Returns:
        -------
        int
            Count of registered entries.
        """
        return len(self.entries)

    def snapshot(self) -> Mapping[K, V]:
        """Return immutable snapshot of current state.

        Returns:
        -------
        Mapping[K, V]
            Snapshot of registry entries.
        """
        return dict(self.entries)

    def restore(self, snapshot: Mapping[K, V]) -> None:
        """Restore registry entries from a snapshot."""
        self.entries = dict(snapshot)

    @classmethod
    def from_mapping(
        cls,
        mapping: Mapping[K, V],
        *,
        read_only: bool = False,
        allow_overwrite: bool = False,
    ) -> MappingRegistryAdapter[K, V]:
        """Create a registry adapter from a mapping.

        Returns:
        -------
        MappingRegistryAdapter[K, V]
            Registry adapter initialized from the mapping.
        """
        return cls(
            entries=dict(mapping),
            read_only=read_only,
            allow_overwrite=allow_overwrite,
        )


__all__ = [
    "ImmutableRegistry",
    "MappingRegistryAdapter",
    "MutableRegistry",
    "Registry",
    "SnapshotRegistry",
]
