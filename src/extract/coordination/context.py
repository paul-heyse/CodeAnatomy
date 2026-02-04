"""File context and span specification for extractor coordination.

This module provides FileContext, RepoFileRow, SpanSpec and related utilities
that form the core identity and payload context for all extractors.

The row building utilities delegate to the canonical implementations in
``extract.row_builder`` for consistency across the extraction layer.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec

from arrow_utils.core.array_iter import iter_table_rows
from datafusion_engine.arrow.interop import TableLike
from datafusion_engine.session.runtime import DataFusionRuntimeProfile
from engine.runtime_profile import RuntimeProfileSpec, resolve_runtime_profile
from extract.coordination.evidence_plan import EvidencePlan
from extract.session import ExtractSession, build_extract_session
from serde_msgspec import (
    StructBaseCompat,
    convert,
    validation_error_payload,
)
from utils.file_io import decode_bytes

if TYPE_CHECKING:
    from core_types import DeterminismTier
    from extract.scanning.scope_manifest import ScopeManifest


@dataclass(frozen=True)
class FileContext:
    """Canonical file identity and payload context for extractors."""

    file_id: str
    path: str
    abs_path: str | None
    file_sha256: str | None
    encoding: str | None = None
    text: str | None = None
    data: bytes | None = None

    @classmethod
    def _payload_from_row(cls, row: Mapping[str, object]) -> RepoFileRow:
        """Convert a repo_files row into a typed payload.

        Returns
        -------
        RepoFileRow
            Typed row payload.

        Raises
        ------
        ValueError
            Raised when the row payload does not conform to the expected schema.
        """
        try:
            return convert(dict(row), target_type=RepoFileRow, strict=False)
        except msgspec.ValidationError as exc:
            details = validation_error_payload(exc)
            msg = f"Repo file payload validation failed: {details}"
            raise ValueError(msg) from exc

    @classmethod
    def from_repo_row(cls, row: Mapping[str, object]) -> FileContext:
        """Build a FileContext from a repo_files row.

        Parameters
        ----------
        row:
            Row mapping from repo_files output.

        Returns
        -------
        FileContext
            Parsed file context.
        """
        payload = cls._payload_from_row(row)
        file_id = payload.file_id or ""
        path = payload.path or ""
        abs_path = payload.abs_path
        file_sha256 = payload.file_sha256
        encoding = payload.encoding
        text = payload.text
        data = payload.data

        return cls(
            file_id=file_id,
            path=path,
            abs_path=abs_path,
            file_sha256=file_sha256,
            encoding=encoding,
            text=text,
            data=data,
        )


class RepoFileRow(StructBaseCompat, frozen=True):
    """Typed repo_files row payload for extractor ingestion."""

    file_id: str | None = None
    path: str | None = None
    abs_path: str | None = None
    file_sha256: str | None = None
    encoding: str | None = None
    text: str | None = None
    data: bytes | None = msgspec.field(name="bytes", default=None)


@dataclass(frozen=True)
class ExtractExecutionContext:
    """Execution context bundle for extract entry points."""

    file_contexts: Iterable[FileContext] | None = None
    evidence_plan: EvidencePlan | None = None
    scope_manifest: ScopeManifest | None = None
    session: ExtractSession | None = None
    runtime_spec: RuntimeProfileSpec | None = None
    profile: str = "default"

    def ensure_session(self) -> ExtractSession:
        """Return the effective extract session.

        Returns
        -------
        ExtractSession
            Provided session or a profile-derived session when missing.
        """
        if self.session is not None:
            return self.session
        runtime_spec = self.runtime_spec or resolve_runtime_profile(self.profile)
        return build_extract_session(runtime_spec)

    def ensure_runtime_profile(self) -> DataFusionRuntimeProfile:
        """Return the DataFusion runtime profile for extraction.

        Returns
        -------
        DataFusionRuntimeProfile
            Resolved DataFusion runtime profile.
        """
        profile = self.ensure_session().engine_session.datafusion_profile
        if profile.diagnostics.capture_plan_artifacts:
            return msgspec.structs.replace(
                profile,
                diagnostics=msgspec.structs.replace(
                    profile.diagnostics,
                    capture_plan_artifacts=False,
                ),
            )
        return profile

    def determinism_tier(self) -> DeterminismTier:
        """Return the determinism tier for extract execution.

        Returns
        -------
        DeterminismTier
            Determinism tier for extract execution.
        """
        return self.ensure_session().engine_session.surface_policy.determinism_tier


@dataclass(frozen=True)
class SpanSpec:
    """Span specification for nested span structs."""

    start_line0: int | None
    start_col: int | None
    end_line0: int | None
    end_col: int | None
    end_exclusive: bool | None
    col_unit: str | None
    byte_start: int | None = None
    byte_len: int | None = None


def iter_file_contexts(repo_files: TableLike) -> Iterator[FileContext]:
    """Yield FileContext objects from a repo_files table.

    Parameters
    ----------
    repo_files:
        Repo files table.

    Yields
    ------
    FileContext
        Parsed file context rows with required identity fields.
    """
    for row in iter_table_rows(repo_files):
        ctx = FileContext.from_repo_row(row)
        if ctx.file_id and ctx.path:
            yield ctx


def file_identity_row(file_ctx: FileContext) -> dict[str, str | None]:
    """Return the standard file identity columns for extractor rows.

    Delegates to :class:`ExtractionRowBuilder` for canonical implementation.

    Returns
    -------
    dict[str, str | None]
        Row fragment with file_id, path, and file_sha256.
    """
    # Lazy import to avoid circular dependency
    from extract.row_builder import ExtractionRowBuilder as _ExtractionRowBuilder

    builder = _ExtractionRowBuilder.from_file_context(file_ctx)
    return builder.add_identity()


def attrs_map(values: Mapping[str, object] | None) -> list[tuple[str, str]]:
    """Return map entries for nested Arrow map fields.

    Delegates to :func:`make_attrs_list` for canonical implementation.

    Returns
    -------
    list[tuple[str, str]]
        List of key/value map entries.
    """
    # Lazy import to avoid circular dependency
    from extract.row_builder import make_attrs_list as _make_attrs_list

    return _make_attrs_list(values)


def pos_dict(line0: int | None, col: int | None) -> dict[str, int | None] | None:
    """Return a position dict for nested span structs.

    Returns
    -------
    dict[str, int | None] | None
        Position mapping or ``None`` when empty.
    """
    if line0 is None and col is None:
        return None
    return {"line0": line0, "col": col}


def byte_span_dict(byte_start: int | None, byte_len: int | None) -> dict[str, int | None] | None:
    """Return a byte-span dict for nested span structs.

    Returns
    -------
    dict[str, int | None] | None
        Byte-span mapping or ``None`` when empty.
    """
    if byte_start is None and byte_len is None:
        return None
    return {"byte_start": byte_start, "byte_len": byte_len}


def span_dict(spec: SpanSpec) -> dict[str, object] | None:
    """Return a span dict for nested span structs.

    Delegates to :func:`make_span_spec_dict` for canonical implementation.

    Returns
    -------
    dict[str, object] | None
        Span mapping or ``None`` when empty.
    """
    # Lazy import to avoid circular dependency
    from extract.row_builder import SpanTemplateSpec as _SpanTemplateSpec
    from extract.row_builder import make_span_spec_dict as _make_span_spec_dict

    template_spec = _SpanTemplateSpec(
        start_line0=spec.start_line0,
        start_col=spec.start_col,
        end_line0=spec.end_line0,
        end_col=spec.end_col,
        end_exclusive=spec.end_exclusive,
        col_unit=spec.col_unit,
        byte_start=spec.byte_start,
        byte_len=spec.byte_len,
    )
    return _make_span_spec_dict(template_spec)


def text_from_file_ctx(file_ctx: FileContext) -> str | None:
    """Return decoded text from a file context, if available.

    Returns
    -------
    str | None
        Decoded text or ``None`` when unavailable.
    """
    if file_ctx.text:
        return file_ctx.text
    data = bytes_from_file_ctx(file_ctx)
    if data is None:
        return None
    _encoding, text = decode_bytes(data, encoding=file_ctx.encoding)
    return text


def bytes_from_file_ctx(file_ctx: FileContext) -> bytes | None:
    """Return raw bytes from a file context.

    Returns
    -------
    bytes | None
        Raw file bytes or ``None`` when unavailable.
    """
    if file_ctx.data is not None:
        return file_ctx.data
    if file_ctx.text is not None:
        encoding = file_ctx.encoding or "utf-8"
        return file_ctx.text.encode(encoding, errors="replace")
    if file_ctx.abs_path:
        try:
            return Path(file_ctx.abs_path).read_bytes()
        except OSError:
            return None
    return None


def iter_contexts(
    repo_files: TableLike,
    file_contexts: Iterable[FileContext] | None = None,
) -> Iterator[FileContext]:
    """Iterate file contexts from provided contexts or a repo_files table.

    Yields
    ------
    FileContext
        File contexts for extraction.
    """
    if file_contexts is None:
        yield from iter_file_contexts(repo_files)
        return
    yield from file_contexts


__all__ = [
    "ExtractExecutionContext",
    "FileContext",
    "RepoFileRow",
    "SpanSpec",
    "attrs_map",
    "byte_span_dict",
    "bytes_from_file_ctx",
    "file_identity_row",
    "iter_contexts",
    "iter_file_contexts",
    "pos_dict",
    "span_dict",
    "text_from_file_ctx",
]
