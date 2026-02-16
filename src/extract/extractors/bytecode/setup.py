"""Bytecode extractor option and file-context setup types."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import cast

from core_types import RowRich as Row
from extract.coordination.context import FileContext, file_identity_row
from extract.infrastructure.options import RepoOptions, WorkerOptions, WorklistQueueOptions


@dataclass(frozen=True)
class BytecodeExtractOptions(RepoOptions, WorklistQueueOptions, WorkerOptions):
    """Bytecode extraction options."""

    optimize: int = 0
    dont_inherit: bool = True
    adaptive: bool = False
    include_cfg_derivations: bool = True
    parallel_min_files: int = 8
    parallel_max_bytes: int = 50_000_000
    terminator_opnames: Sequence[str] = (
        "RETURN_VALUE",
        "RETURN_CONST",
        "RAISE_VARARGS",
        "RERAISE",
    )


@dataclass(frozen=True)
class BytecodeFileContext:
    """Per-file context for bytecode extraction."""

    file_ctx: FileContext
    options: BytecodeExtractOptions

    @property
    def file_id(self) -> str:
        """Return stable file identifier."""
        return self.file_ctx.file_id

    @property
    def path(self) -> str:
        """Return repository-relative file path."""
        return self.file_ctx.path

    @property
    def file_sha256(self) -> str | None:
        """Return file SHA256 when available."""
        return self.file_ctx.file_sha256

    def identity_row(self) -> Row:
        """Return normalized file identity row payload."""
        return cast("Row", file_identity_row(self.file_ctx))


@dataclass(frozen=True)
class BytecodeCacheResult:
    """Cached bytecode extraction payload for a single file."""

    code_objects: list[dict[str, object]]
    errors: list[dict[str, object]]


__all__ = [
    "BytecodeCacheResult",
    "BytecodeExtractOptions",
    "BytecodeFileContext",
]
