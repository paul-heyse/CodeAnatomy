"""CLI context and result types for cq."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from tools.cq.cli_app.options import CommonFilters
from tools.cq.cli_app.types import OutputFormat
from tools.cq.core.structs import CqStruct
from tools.cq.index.repo import resolve_repo_context

if TYPE_CHECKING:
    from collections.abc import Mapping

    from tools.cq.core.bootstrap import CqRuntimeServices
    from tools.cq.core.schema import CqResult
    from tools.cq.core.toolchain import Toolchain


@dataclass(frozen=True, slots=True)
class CliContextOptions:
    """Optional CLI context overrides."""

    root: Path | None = None
    verbose: int = 0
    output_format: OutputFormat | None = None
    artifact_dir: Path | None = None
    save_artifact: bool = True

    @classmethod
    def from_kwargs(cls, kwargs: Mapping[str, object]) -> CliContextOptions:
        """Build options from legacy keyword arguments.

        Returns:
        -------
        CliContextOptions
            Normalized option bundle.
        """
        root_raw = kwargs.get("root")
        artifact_raw = kwargs.get("artifact_dir")
        verbose_raw = kwargs.get("verbose", 0)
        output_format_raw = kwargs.get("output_format")
        output_format = output_format_raw if isinstance(output_format_raw, OutputFormat) else None
        return cls(
            root=(root_raw if isinstance(root_raw, Path) or root_raw is None else None),
            verbose=int(verbose_raw) if isinstance(verbose_raw, int | float) else 0,
            output_format=output_format,
            artifact_dir=(
                artifact_raw if isinstance(artifact_raw, Path) or artifact_raw is None else None
            ),
            save_artifact=bool(kwargs.get("save_artifact", True)),
        )


class CliContext(CqStruct, frozen=True):
    """Runtime context for CLI commands.

    Parameters
    ----------
    argv
        Original command-line arguments.
    root
        Resolved repository root path.
    toolchain
        Detected toolchain with available tools.
    verbose
        Verbosity level (0=normal, 1=verbose, 2+=debug).
    output_format
        Default output format from global options.
    artifact_dir
        Directory for saving artifacts, or None for default.
    save_artifact
        Whether to save artifacts (can be disabled via --no-save-artifact).
    """

    argv: list[str]
    root: Path
    toolchain: Toolchain
    services: CqRuntimeServices
    verbose: int = 0
    output_format: OutputFormat | None = None
    artifact_dir: Path | None = None
    save_artifact: bool = True

    @classmethod
    def from_parts(
        cls,
        *,
        root: Path,
        toolchain: Toolchain,
        services: CqRuntimeServices,
        argv: list[str] | None = None,
        options: CliContextOptions | None = None,
    ) -> CliContext:
        """Build context from explicit injected dependencies.

        Returns:
            CliContext: Fully initialized CLI context.
        """
        resolved_options = options or CliContextOptions()
        return cls(
            argv=list(argv or []),
            root=root,
            toolchain=toolchain,
            services=services,
            verbose=resolved_options.verbose,
            output_format=resolved_options.output_format,
            artifact_dir=resolved_options.artifact_dir,
            save_artifact=resolved_options.save_artifact,
        )

    @classmethod
    def build(
        cls,
        argv: list[str],
        options: CliContextOptions | None = None,
        **kwargs: object,
    ) -> CliContext:
        """Build a CLI context from arguments.

        Args:
            argv: Original CLI argument vector.
            options: Optional context options object.
            **kwargs: Legacy keyword overrides for context options.

        Returns:
            CliContext: Fully initialized CLI context.

        Raises:
            ValueError: If both `options` and legacy keyword overrides are provided.
        """
        from tools.cq.core.toolchain import Toolchain

        if options is None:
            options = CliContextOptions.from_kwargs(kwargs)
        elif kwargs:
            msg = "Pass either options or legacy keyword overrides, not both."
            raise ValueError(msg)

        # Resolve root from explicit arg or auto-detect
        root = options.root
        if root is None:
            repo_context = resolve_repo_context()
            root = repo_context.repo_root

        root = root.resolve()
        toolchain = Toolchain.detect()

        from tools.cq.core.bootstrap import resolve_runtime_services

        services = resolve_runtime_services(root)

        return cls.from_parts(
            root=root,
            toolchain=toolchain,
            services=services,
            argv=argv,
            options=options,
        )


FilterConfig = CommonFilters


class CliTextResult(CqStruct, frozen=True):
    """Text payload for non-analysis protocol commands.

    Parameters
    ----------
    text
        The text content to output.
    media_type
        MIME type of the content (default: text/plain).
    """

    text: str
    media_type: str = "text/plain"


if TYPE_CHECKING:
    CliResultPayload = CqResult | CliTextResult | int


class CliResult(CqStruct, frozen=True):
    """Result from a CLI command execution.

    Parameters
    ----------
    result
        The command result (CqResult or int exit code).
    context
        The CLI context used for execution.
    exit_code
        Optional explicit exit code.
    filters
        Filter configuration for result filtering.
    """

    result: CliResultPayload
    context: CliContext
    exit_code: int | None = None
    filters: FilterConfig | None = None

    @property
    def is_cq_result(self) -> bool:
        """Check if result is a CqResult.

        Returns:
        -------
        bool
            True if result is a CqResult.
        """
        from tools.cq.core.schema import CqResult as CqResultType

        return isinstance(self.result, CqResultType)

    def get_exit_code(self) -> int:
        """Get the exit code for this result.

        Returns:
        -------
        int
            Exit code (0 for success).
        """
        if self.exit_code is not None:
            return self.exit_code
        if isinstance(self.result, int):
            return self.result
        return 0
