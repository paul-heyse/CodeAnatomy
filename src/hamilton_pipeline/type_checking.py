"""Hamilton type checking helpers for CodeAnatomy."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from types import UnionType
from typing import Annotated, Any, Literal, TypeAliasType, Union, get_args, get_origin

from hamilton import htypes
from hamilton.lifecycle import api as lifecycle_api

_PAIR_LENGTH = 2


def _unwrap_type_alias(type_: Any) -> Any:
    while isinstance(type_, TypeAliasType):
        type_ = type_.__value__
    return type_


def _safe_isinstance(value: Any, expected: Any) -> bool:
    try:
        return isinstance(value, expected)
    except TypeError:
        return False


def _is_mapping_origin(origin: Any) -> bool:
    if origin in {dict, Mapping}:
        return True
    try:
        return issubclass(origin, Mapping)
    except TypeError:
        return False


def _check_instance_fallback(value: Any, expected: Any) -> bool:  # noqa: C901, PLR0911, PLR0912
    if expected in {Any, object}:
        return True
    if expected is None or expected is type(None):
        return value is None

    origin = get_origin(expected)
    args = get_args(expected)

    if origin in {Union, UnionType}:
        return any(_check_instance_fallback(value, arg) for arg in args)

    if origin is Literal:
        return value in args

    if origin is Annotated and args:
        return _check_instance_fallback(value, args[0])

    if origin is not None:
        if origin in {htypes.Collect, htypes.Parallelizable}:
            if isinstance(value, (bytes, bytearray, str)):
                return False
            if not isinstance(value, Iterable):
                return False
            element_type = args[0] if args else Any
            return all(_check_instance_fallback(item, element_type) for item in value)

        if not _safe_isinstance(value, origin):
            return False

        if not args:
            return True

        if _is_mapping_origin(origin):
            key_type = args[0] if len(args) >= 1 else Any
            value_type = Any
            if len(args) >= _PAIR_LENGTH:
                value_type = next(iter(args[1:2]), Any)
            if not isinstance(value, Mapping):
                return False
            return all(
                _check_instance_fallback(key, key_type)
                and _check_instance_fallback(item_value, value_type)
                for key, item_value in value.items()
            )

        if origin is tuple:
            if not isinstance(value, tuple):
                return False
            if len(args) == _PAIR_LENGTH and args[1:2] == (Ellipsis,):
                return all(_check_instance_fallback(item, args[0]) for item in value)
            if len(args) != len(value):
                return False
            return all(
                _check_instance_fallback(item, item_type)
                for item, item_type in zip(value, args, strict=True)
            )

        if origin in {list, set, frozenset}:
            if not _safe_isinstance(value, origin):
                return False
            item_type = args[0] if args else Any
            return all(_check_instance_fallback(item, item_type) for item in value)

        if origin is Sequence:
            if isinstance(value, (bytes, bytearray, str)):
                return False
            if not isinstance(value, Sequence):
                return False
            item_type = args[0] if args else Any
            return all(_check_instance_fallback(item, item_type) for item in value)

        return True

    return _safe_isinstance(value, expected)


def _safe_check_instance(value: Any, expected: Any) -> bool:
    try:
        return htypes.check_instance(value, expected)
    except TypeError:
        return _check_instance_fallback(value, expected)


class CodeAnatomyTypeChecker(lifecycle_api.NodeExecutionHook):
    """Type checker that understands PEP-695 TypeAliasType hints."""

    def __init__(self, *, check_input: bool = True, check_output: bool = True) -> None:
        """__init__."""
        self.check_input = check_input
        self.check_output = check_output

    def run_before_node_execution(
        self,
        *,
        node_name: str,
        node_kwargs: dict[str, Any],
        node_input_types: dict[str, Any] | None = None,
        **_future_kwargs: Any,
    ) -> None:
        """Validate node inputs against declared types.

        Args:
            node_name: Description.
            node_kwargs: Description.
            node_input_types: Description.
            **_future_kwargs: Description.

        Raises:
            TypeError: If the operation cannot be completed.
        """
        if not self.check_input:
            return
        input_types = node_input_types or {}
        for input_name, input_value in node_kwargs.items():
            expected = _unwrap_type_alias(input_types.get(input_name, object))
            if not _safe_check_instance(input_value, expected):
                msg = (
                    f"Node {node_name} received an input of type {type(input_value)} "
                    f"for {input_name}, expected {expected}"
                )
                raise TypeError(msg)

    def run_after_node_execution(
        self,
        *,
        node_name: str,
        node_return_type: Any,
        result: Any,
        **_future_kwargs: Any,
    ) -> None:
        """Validate node output against declared return types.

        Args:
            node_name: Description.
            node_return_type: Description.
            result: Description.
            **_future_kwargs: Description.

        Raises:
            TypeError: If the operation cannot be completed.
        """
        if not self.check_output:
            return
        expected = _unwrap_type_alias(node_return_type)
        if not _safe_check_instance(result, expected):
            msg = f"Node {node_name} returned a result of type {type(result)}, expected {expected}"
            raise TypeError(msg)


__all__ = ["CodeAnatomyTypeChecker"]
