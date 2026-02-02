"""Sample Python code with closures and nested functions for testing.

Used for testing scope queries, closure detection, and variable capture analysis.
"""

from __future__ import annotations


def outer_function(x: int) -> callable:
    """Create a closure that captures the input value.

    Returns
    -------
    callable
        Closure that adds the captured value.
    """
    captured = x * 2

    def inner_closure(y: int) -> int:
        """Add the captured value to the input.

        Returns
        -------
        int
            Sum of the captured value and input.
        """
        return captured + y

    return inner_closure


def generator_closure(items: list[int]) -> callable:
    """Create a closure over generator state.

    Returns
    -------
    callable
        Closure that returns successive items or None.
    """
    index = 0

    def next_item() -> int | None:
        """Return the next item from the captured list.

        Returns
        -------
        int | None
            Next item in the list or None when exhausted.
        """
        nonlocal index
        if index < len(items):
            result = items[index]
            index += 1
            return result
        return None

    return next_item


class ClosureFactory:
    """Class with methods that create closures."""

    def __init__(self, multiplier: int) -> None:
        self.multiplier = multiplier

    def create_multiplier(self) -> callable:
        """Create a closure that captures self.multiplier.

        Returns
        -------
        callable
            Closure that multiplies by the captured factor.
        """
        m = self.multiplier

        def multiply(x: int) -> int:
            """Multiply by the captured multiplier.

            Returns
            -------
            int
                Product of input and multiplier.
            """
            return x * m

        return multiply


def nested_three_levels() -> callable:
    """Create a nested closure chain.

    Returns
    -------
    callable
        Closure returning the deepest nested function.
    """
    level1 = "outer"

    def level_two() -> callable:
        """Return the next nested closure.

        Returns
        -------
        callable
            Closure returning the final string builder.
        """
        level2 = "middle"

        def level_three() -> str:
            """Return the composed nested string.

            Returns
            -------
            str
                Composed nested string.
            """
            return f"{level1}-{level2}-inner"

        return level_three

    return level_two


def no_closure_function(x: int) -> int:
    """Return a computed value without closures.

    Returns
    -------
    int
        Computed result.
    """
    return x * 2


def closure_with_cells() -> tuple[callable, callable]:
    """Return multiple closures sharing cell variables.

    Returns
    -------
    tuple[callable, callable]
        Increment and getter closures over the same counter.
    """
    counter = 0

    def increment() -> int:
        """Increment the captured counter.

        Returns
        -------
        int
            Updated counter value.
        """
        nonlocal counter
        counter += 1
        return counter

    def get_count() -> int:
        """Return the captured counter.

        Returns
        -------
        int
            Current counter value.
        """
        return counter

    return increment, get_count
