"""Relspec error types for validation and execution."""

from __future__ import annotations


class RelspecError(Exception):
    """Base class for relspec errors."""


class RelspecValidationError(RelspecError, ValueError):
    """Raised when relspec validation fails."""


class RelspecCompilationError(RelspecError, RuntimeError):
    """Raised when relspec compilation fails."""


class RelspecExecutionError(RelspecError, RuntimeError):
    """Raised when relspec execution fails."""


class RelspecCapabilityError(RelspecError, RuntimeError):
    """Raised when runtime capabilities are insufficient."""


class RelspecExecutionAuthorityError(RelspecError, RuntimeError):
    """Raised when execution authority validation fails."""


__all__ = [
    "RelspecCapabilityError",
    "RelspecCompilationError",
    "RelspecError",
    "RelspecExecutionAuthorityError",
    "RelspecExecutionError",
    "RelspecValidationError",
]
