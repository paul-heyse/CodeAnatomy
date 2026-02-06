"""Mini repo fixture module for parity tests."""


class Greeter:
    """Simple greeting helper."""

    def __init__(self, prefix: str) -> None:
        """Create a greeter with a prefix."""
        self.prefix = prefix

    def greet(self, name: str) -> str:
        """Return a greeting string.

        Returns:
        -------
        str
            Greeting text.
        """
        return f"{self.prefix} {name}"


def add(a: int, b: int) -> int:
    """Return the sum of two integers.

    Returns:
    -------
    int
        Sum of a and b.
    """
    return a + b
