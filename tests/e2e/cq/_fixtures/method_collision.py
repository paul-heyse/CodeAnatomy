"""Fixture module for method name collision tests."""

from __future__ import annotations


class Alpha:
    """Class without collide method."""

    def do_work(self) -> int:
        """Call a method name that exists on another class.

        Returns
        -------
        int
            Result of the collide call.

        Raises
        ------
        TypeError
            Raised when collide is not callable.
        """
        from collections.abc import Callable
        from typing import cast

        candidate = getattr(self, "collide", None)
        if not callable(candidate):
            msg = "collide method not available"
            raise TypeError(msg)
        collide = cast("Callable[[], int]", candidate)
        return collide()


class Beta:
    """Class that defines the collide method."""

    def collide(self) -> int:
        """Return a constant value.

        Returns
        -------
        int
            Constant value used by tests.
        """
        return 1
