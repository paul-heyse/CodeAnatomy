"""Ordering metadata helpers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

type OrderingKey = tuple[str, str]


class OrderingLevel(StrEnum):
    """Ordering metadata levels."""

    UNORDERED = "unordered"
    IMPLICIT = "implicit"
    EXPLICIT = "explicit"


class OrderingEffect(StrEnum):
    """Ordering effect classification for operations."""

    PRESERVE = "preserve"
    UNORDERED = "unordered"
    IMPLICIT = "implicit"
    EXPLICIT = "explicit"


@dataclass(frozen=True)
class Ordering:
    """Ordering metadata propagated through Plan operations.

    Parameters
    ----------
    level:
        Ordering level classification.
    keys:
        Tuple of (column, order) pairs.
    """

    level: OrderingLevel = OrderingLevel.UNORDERED
    keys: tuple[OrderingKey, ...] = ()

    @staticmethod
    def unordered() -> Ordering:
        """Return an unordered ordering marker.

        Returns:
        -------
        Ordering
            Unordered marker.
        """
        return Ordering(OrderingLevel.UNORDERED, ())

    @staticmethod
    def implicit() -> Ordering:
        """Return an implicit ordering marker.

        Returns:
        -------
        Ordering
            Implicit ordering marker.
        """
        return Ordering(OrderingLevel.IMPLICIT, ())

    @staticmethod
    def explicit(keys: tuple[OrderingKey, ...]) -> Ordering:
        """Return an explicit ordering marker.

        Parameters
        ----------
        keys:
            Explicit ordering keys.

        Returns:
        -------
        Ordering
            Explicit ordering marker.
        """
        return Ordering(OrderingLevel.EXPLICIT, tuple(keys))


__all__ = ["Ordering", "OrderingEffect", "OrderingKey", "OrderingLevel"]
