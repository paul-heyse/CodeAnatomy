"""CST visitor context contracts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from libcst.metadata import FullRepoManager

from core_types import RowPermissive as Row
from extract.coordination.context import FileContext
from extract.extractors.cst.setup import CstExtractOptions

if TYPE_CHECKING:
    from extract.coordination.evidence_plan import EvidencePlan


@dataclass(frozen=True)
class CSTFileContext:
    """Per-file context for CST extraction."""

    file_ctx: FileContext
    options: CstExtractOptions

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


@dataclass
class CSTExtractContext:
    """Shared extraction context and output buffers."""

    options: CstExtractOptions
    manifest_rows: list[Row]
    error_rows: list[Row]
    ref_rows: list[Row]
    import_rows: list[Row]
    call_rows: list[Row]
    def_rows: list[Row]
    type_expr_rows: list[Row]
    docstring_rows: list[Row]
    decorator_rows: list[Row]
    call_arg_rows: list[Row]
    evidence_plan: EvidencePlan | None = None
    repo_manager: FullRepoManager | None = None

    @classmethod
    def build(
        cls,
        options: CstExtractOptions,
        *,
        evidence_plan: EvidencePlan | None = None,
        repo_manager: FullRepoManager | None = None,
    ) -> CSTExtractContext:
        """Build empty row buffers for CST extraction.

        Returns:
            CSTExtractContext: Initialized extraction context.
        """
        return cls(
            options=options,
            manifest_rows=[],
            error_rows=[],
            ref_rows=[],
            import_rows=[],
            call_rows=[],
            def_rows=[],
            type_expr_rows=[],
            docstring_rows=[],
            decorator_rows=[],
            call_arg_rows=[],
            evidence_plan=evidence_plan,
            repo_manager=repo_manager,
        )


@dataclass(frozen=True)
class TypeExprOwner:
    """Context for a collected type expression."""

    owner_def_kind: str
    owner_def_bstart: int
    owner_def_bend: int
    expr_role: str
    param_name: str | None = None


__all__ = ["CSTExtractContext", "CSTFileContext", "TypeExprOwner"]
