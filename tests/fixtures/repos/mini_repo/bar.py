"""Mini repo fixture module with imports and callsites."""

from tests.fixtures.repos.mini_repo.foo import Greeter, add


def build_message(name: str) -> str:
    """Build a greeting using helper functions.

    Returns:
    -------
    str
        Greeting message.
    """
    greeter = Greeter("hello")
    total = add(1, 2)
    return greeter.greet(f"{name} {total}")
