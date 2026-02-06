"""Sample Python code with complex control flow for testing.

Used for testing CFG reconstruction, exception handling, and bytecode analysis.
"""

from __future__ import annotations

import asyncio
import logging
import threading

LOGGER = logging.getLogger(__name__)


def simple_branch(x: int) -> str:
    """Return a label for a simple branch.

    Returns:
    -------
    str
        Branch label based on input sign.
    """
    if x > 0:
        return "positive"
    return "non-positive"


def multiple_branches(x: int, y: int) -> str:
    """Return a label based on multiple branches.

    Returns:
    -------
    str
        Label describing the relation between x and y.
    """
    if x > y:
        return "x greater"
    if x < y:
        return "y greater"
    return "equal"


def nested_conditions(a: int, b: int, c: int) -> str:
    """Return a label based on nested conditions.

    Returns:
    -------
    str
        Label describing the positive/negative state.
    """
    if a > 0:
        if b > 0:
            if c > 0:
                return "all positive"
            return "a,b positive"
        return "a positive"
    return "a not positive"


def try_except_basic() -> int:
    """Return a value using try-except control flow.

    Returns:
    -------
    int
        Result value or fallback on error.
    """
    try:
        value = 1 / 1
    except ZeroDivisionError:
        return 0
    else:
        return int(value)


def try_except_else_finally() -> str:
    """Return a value using try-except-else-finally.

    Returns:
    -------
    str
        Success or error label.
    """
    try:
        value = int("123")
    except ValueError:
        return "error"
    else:
        return f"success: {value}"
    finally:
        LOGGER.info("cleanup")


def try_eval_single() -> str:
    """Return a value using eval within try/except.

    Returns:
    -------
    str
        Success or error label.
    """
    try:
        result = eval("1 + 1")
        return str(result)
    except ValueError:
        return "error"


def multiple_except_handlers() -> str:
    """Return a label from multiple exception handlers.

    Returns:
    -------
    str
        Handler-specific label.
    """
    try:
        result = eval("1/0")
        return str(result)
    except ZeroDivisionError:
        return "div zero"
    except TypeError:
        return "type error"
    except (SyntaxError, ValueError) as exc:
        return f"other: {exc}"


def loop_with_break(items: list[int]) -> int:
    """Sum items until a negative value is seen.

    Returns:
    -------
    int
        Sum of items before the break.
    """
    result = 0
    for item in items:
        if item < 0:
            break
        result += item
    return result


def loop_with_continue(items: list[int]) -> int:
    """Sum odd items while skipping even values.

    Returns:
    -------
    int
        Sum of odd items.
    """
    result = 0
    for item in items:
        if item % 2 == 0:
            continue
        result += item
    return result


def while_with_else(n: int) -> str:
    """Return a label based on while loop search.

    Returns:
    -------
    str
        Label for found or exhausted state.
    """
    i = 0
    while i < n:
        if i == 5:
            return "found 5"
        i += 1
    return "exhausted"


def nested_loops(matrix: list[list[int]]) -> int:
    """Return sentinel values based on nested loops.

    Returns:
    -------
    int
        Sentinel value for early exit or success.
    """
    for row in matrix:
        for cell in row:
            if cell == 0:
                return -1
    return 0


async def async_control_flow(x: int) -> str:
    """Return a label using async control flow.

    Returns:
    -------
    str
        Async branch label.
    """
    if x > 0:
        await some_async_call(x)
        return "positive async"
    return "non-positive async"


async def async_blocking_sleep() -> None:
    """Perform blocking thread wait inside async function for testing purposes."""
    threading.Event().wait(0.01)
    await asyncio.sleep(0)


async def some_async_call(n: int) -> None:
    """Simulate an async call."""
    _ = n
    await asyncio.sleep(0)


def match_statement(value: int | str) -> str:
    """Return a label using pattern matching.

    Returns:
    -------
    str
        Label for the matched pattern.
    """
    match value:
        case 0:
            return "zero"
        case int() if value > 0:
            return "positive"
        case int():
            return "negative"
        case str():
            return f"string: {value}"
        case _:
            return "unknown"
