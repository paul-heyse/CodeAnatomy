"""Extractor protocol contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Protocol

import pyarrow as pa

from extract.coordination.context import ExtractExecutionContext, FileContext


class ExtractorPort(Protocol):
    """Structural interface for extraction implementations."""

    @property
    def name(self) -> str:
        """Return the extractor name."""
        ...

    @property
    def output_names(self) -> Sequence[str]:
        """Return output table names produced by this extractor."""
        ...

    def extract(
        self,
        file_contexts: Sequence[FileContext],
        *,
        execution_context: ExtractExecutionContext,
    ) -> Mapping[str, pa.Table]:
        """Extract table payloads from file contexts."""
        ...


__all__ = ["ExtractorPort"]
