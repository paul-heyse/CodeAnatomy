"""Sample Python code with dynamic dispatch patterns for testing.

Used for testing dynamic dispatch and call analysis.
"""

from __future__ import annotations

import pathlib
import pickle
from typing import Any


def uses_getattr(obj: Any, attr_name: str) -> Any:
    """Return a dynamically accessed attribute.

    Returns:
    -------
    Any
        Attribute value from getattr.
    """
    return getattr(obj, attr_name)


def uses_getattr_with_default(obj: Any, attr_name: str, default: Any = None) -> Any:
    """Return a dynamically accessed attribute with default.

    Returns:
    -------
    Any
        Attribute value or default.
    """
    return getattr(obj, attr_name, default)


def uses_setattr(obj: Any, attr_name: str, value: Any) -> None:
    """Set a dynamic attribute."""
    setattr(obj, attr_name, value)


def uses_hasattr(obj: Any, attr_name: str) -> bool:
    """Return whether an attribute exists.

    Returns:
    -------
    bool
        True when attribute is present.
    """
    return hasattr(obj, attr_name)


def uses_eval_dangerous(code: str) -> Any:
    """Evaluate dynamic code and return the result.

    Returns:
    -------
    Any
        Result of eval.
    """
    return eval(code)


def uses_exec_dangerous(code: str) -> None:
    """Execute dynamic code."""
    exec(code)


def uses_compile_and_exec(code: str) -> Any:
    """Compile code and evaluate it.

    Returns:
    -------
    Any
        Result of eval on compiled code.
    """
    compiled = compile(code, "<string>", "eval")
    return eval(compiled)


def uses_pickle_load(filepath: str) -> Any:
    """Load a pickled object from a file path.

    Returns:
    -------
    Any
        Deserialized object.
    """
    with pathlib.Path(filepath).open("rb") as f:
        return pickle.load(f)


def uses_pickle_loads(data: bytes) -> Any:
    """Load a pickled object from bytes.

    Returns:
    -------
    Any
        Deserialized object.
    """
    return pickle.loads(data)


class DynamicDispatcher:
    """Class using dynamic dispatch patterns."""

    def dispatch(self, method_name: str, *args: Any) -> Any:
        """Dispatch to a method by name.

        Returns:
        -------
        Any
            Result of the dispatched method.
        """
        method = getattr(self, method_name)
        return method(*args)

    def __getattr__(self, name: str) -> Any:
        """Return a placeholder for missing attributes.

        Returns:
        -------
        Any
            Placeholder string for missing attributes.
        """
        return f"missing_{name}"


def forwarding_with_args(*args: Any, **kwargs: Any) -> Any:
    """Forward arguments to the target function.

    Returns:
    -------
    Any
        Result of the target call.
    """
    return some_target(*args, **kwargs)


def forwarding_partial(*args: Any, extra: int = 10) -> Any:
    """Forward arguments with an extra parameter.

    Returns:
    -------
    Any
        Result of the target call.
    """
    return some_target(*args, extra=extra)


def some_target(*args: Any, **kwargs: Any) -> tuple[Any, ...]:
    """Return forwarded arguments as a tuple.

    Returns:
    -------
    tuple[Any, ...]
        Combined positional and keyword values.
    """
    return args + tuple(kwargs.values())


def safe_function(x: int, y: int) -> int:
    """Return the sum of two integers.

    Returns:
    -------
    int
        Sum of inputs.
    """
    return x + y


def uses_globals() -> dict[str, Any]:
    """Return the globals dictionary.

    Returns:
    -------
    dict[str, Any]
        Module globals mapping.
    """
    return globals()


def uses_locals() -> dict[str, Any]:
    """Return the locals dictionary.

    Returns:
    -------
    dict[str, Any]
        Local variables mapping.
    """
    x = 1
    y = 2
    return locals()
