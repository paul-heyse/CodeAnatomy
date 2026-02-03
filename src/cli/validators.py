"""Custom validators for CLI parameter groups."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from cyclopts import ArgumentCollection
from cyclopts.utils import UNSET

_LOGGER = logging.getLogger(__name__)


def _resolve_params(
    args: tuple[object, ...],
    resolved_params: dict[str, Any],
) -> dict[str, Any]:
    if resolved_params:
        return resolved_params
    if len(args) != 1:
        return {}
    collection = args[0]
    if not isinstance(collection, ArgumentCollection):
        return {}
    values: dict[str, Any] = {}
    for argument in collection:
        value = argument.value
        values[argument.field_info.name] = None if value is UNSET else value
    return values


@dataclass(frozen=True)
class ConditionalRequired:
    """Validator that requires certain parameters when a condition is met.

    Parameters
    ----------
    condition_param
        Name of the parameter that triggers the condition.
    condition_value
        Value that triggers the requirement.
    required_params
        Parameters that are required when condition is met.
    """

    condition_param: str
    condition_value: Any
    required_params: tuple[str, ...]

    def __call__(
        self,
        *args: object,
        **resolved_params: Any,
    ) -> None:
        """Validate that required parameters are present when condition is met.

        Parameters
        ----------
        *args
            Positional args forwarded by cyclopts for parameter resolution.
        resolved_params
            Dictionary of resolved parameter values.

        Raises
        ------
        ValueError
            When required parameters are missing.
        """
        params = _resolve_params(args, resolved_params)
        condition = params.get(self.condition_param)
        if condition == self.condition_value:
            missing = [param for param in self.required_params if params.get(param) is None]
            if missing:
                msg = (
                    f"When {self.condition_param}={self.condition_value!r}, "
                    f"the following parameters are required: {', '.join(missing)}"
                )
                raise ValueError(msg)


@dataclass(frozen=True)
class ConditionalDisabled:
    """Validator that warns when parameters are set but a condition disables them.

    This validator emits a warning when parameters are configured but will be
    ignored due to a disabling condition (e.g., --disable-scip with --scip-output-dir).

    Parameters
    ----------
    condition_param
        Name of the parameter that disables functionality.
    condition_value
        Value that indicates the feature is disabled.
    disabled_params
        Parameters that are ignored when the condition is met.
    """

    condition_param: str
    condition_value: Any
    disabled_params: tuple[str, ...]

    def __call__(
        self,
        *args: object,
        **resolved_params: Any,
    ) -> None:
        """Warn when disabled parameters are configured.

        Parameters
        ----------
        *args
            Positional args forwarded by cyclopts for parameter resolution.
        resolved_params
            Dictionary of resolved parameter values.
        """
        params = _resolve_params(args, resolved_params)
        condition = params.get(self.condition_param)
        if condition == self.condition_value:
            configured = [param for param in self.disabled_params if params.get(param) is not None]
            if configured:
                _LOGGER.warning(
                    "The following parameters are ignored when %s=%r: %s",
                    self.condition_param,
                    self.condition_value,
                    ", ".join(configured),
                )


__all__ = ["ConditionalDisabled", "ConditionalRequired"]
