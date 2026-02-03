"""Hamilton type checking helpers for CodeAnatomy."""

from __future__ import annotations

from typing import Any, TypeAliasType

from hamilton import htypes
from hamilton.lifecycle import api as lifecycle_api


def _unwrap_type_alias(type_: Any) -> Any:
    while isinstance(type_, TypeAliasType):
        type_ = type_.__value__
    return type_


class CodeAnatomyTypeChecker(lifecycle_api.NodeExecutionHook):
    """Type checker that understands PEP-695 TypeAliasType hints."""

    def __init__(self, *, check_input: bool = True, check_output: bool = True) -> None:
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

        Raises
        ------
        TypeError
            Raised when an input value does not satisfy its expected type.
        """
        if not self.check_input:
            return
        input_types = node_input_types or {}
        for input_name, input_value in node_kwargs.items():
            expected = _unwrap_type_alias(input_types.get(input_name, object))
            if not htypes.check_instance(input_value, expected):
                msg = (
                    f"Node {node_name} received an input of type {type(input_value)} "
                    f"for {input_name}, expected {expected}"
                )
                raise TypeError(msg)

    def run_after_node_execution(
        self,
        *,
        node_name: str,
        node_return_type: type,
        result: Any,
        **_future_kwargs: Any,
    ) -> None:
        """Validate node output against declared return types.

        Raises
        ------
        TypeError
            Raised when the result does not satisfy its expected type.
        """
        if not self.check_output:
            return
        expected = _unwrap_type_alias(node_return_type)
        if not htypes.check_instance(result, expected):
            msg = f"Node {node_name} returned a result of type {type(result)}, expected {expected}"
            raise TypeError(msg)


__all__ = ["CodeAnatomyTypeChecker"]
