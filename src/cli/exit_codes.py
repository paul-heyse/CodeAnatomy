"""Exit code taxonomy for the CodeAnatomy CLI."""

from __future__ import annotations

from enum import IntEnum


class ExitCode(IntEnum):
    """Standard exit codes for CLI commands.

    Exit codes follow Unix conventions with domain-specific extensions:
    - 0: Success
    - 1-9: General errors (parse, validation, config)
    - 10-19: Pipeline stage errors
    - 20-29: Backend/integration errors
    """

    SUCCESS = 0
    GENERAL_ERROR = 1
    PARSE_ERROR = 2
    VALIDATION_ERROR = 3
    CONFIG_ERROR = 4

    # Pipeline stage errors (10-19)
    EXTRACTION_ERROR = 10
    NORMALIZATION_ERROR = 11
    SCHEDULING_ERROR = 12
    EXECUTION_ERROR = 13

    # Backend errors (20-29)
    BACKEND_ERROR = 20
    DATAFUSION_ERROR = 21

    @classmethod
    def from_exception(cls, exc: BaseException) -> ExitCode:
        """Map an exception to an appropriate exit code.

        Parameters
        ----------
        exc
            Exception to classify.

        Returns
        -------
        ExitCode
            Exit code appropriate for the exception type.
        """
        cyclopts_code = _exit_code_for_cyclopts(exc)
        if cyclopts_code is not None:
            return cyclopts_code

        name_code = _exit_code_for_exception_name(exc)
        if name_code is not None:
            return name_code

        type_code = _exit_code_for_exception_type(exc)
        if type_code is not None:
            return type_code

        module_code = _exit_code_for_exception_module(exc)
        if module_code is not None:
            return module_code

        return cls.GENERAL_ERROR


def _exit_code_for_cyclopts(exc: BaseException) -> ExitCode | None:
    module = exc.__class__.__module__
    if not module.startswith("cyclopts"):
        return None
    name = exc.__class__.__name__
    if name == "ValidationError":
        return ExitCode.VALIDATION_ERROR
    if name in {
        "UnknownCommandError",
        "UnknownOptionError",
        "MissingArgumentError",
        "RepeatArgumentError",
        "CoercionError",
    }:
        return ExitCode.PARSE_ERROR
    return ExitCode.PARSE_ERROR


def _exit_code_for_exception_name(exc: BaseException) -> ExitCode | None:
    if exc.__class__.__name__ in {"ConfigError", "ConfigurationError", "TOMLDecodeError"}:
        return ExitCode.CONFIG_ERROR
    return None


def _exit_code_for_exception_type(exc: BaseException) -> ExitCode | None:
    if isinstance(exc, (ValueError, TypeError)):
        return ExitCode.VALIDATION_ERROR
    if isinstance(exc, (FileNotFoundError, FileExistsError, PermissionError)):
        return ExitCode.CONFIG_ERROR
    return None


def _exit_code_for_exception_module(exc: BaseException) -> ExitCode | None:
    module = exc.__class__.__module__.lower()
    mappings: tuple[tuple[tuple[str, ...], ExitCode], ...] = (
        (("extract",), ExitCode.EXTRACTION_ERROR),
        (("normalize", "semantics"), ExitCode.NORMALIZATION_ERROR),
        (("relspec", "schedule"), ExitCode.SCHEDULING_ERROR),
        (("engine", "hamilton"), ExitCode.EXECUTION_ERROR),
        (("datafusion",), ExitCode.DATAFUSION_ERROR),
        (("delta", "storage"), ExitCode.BACKEND_ERROR),
    )
    for needles, exit_code in mappings:
        if any(needle in module for needle in needles):
            return exit_code
    return None


__all__ = ["ExitCode"]
