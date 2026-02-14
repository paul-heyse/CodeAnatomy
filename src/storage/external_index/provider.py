"""External index provider contracts.

Providers map dataset scan requests to candidate-file selections using
external metadata indexes.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from datafusion import SessionContext

if TYPE_CHECKING:
    from datafusion_engine.dataset.registry import DatasetLocation
    from datafusion_engine.lineage.reporting import ScanLineage
    from datafusion_engine.session.runtime import DataFusionRuntimeProfile

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExternalIndexRequest:
    """Inputs for external index candidate selection."""

    dataset_name: str
    location: DatasetLocation
    lineage: ScanLineage
    runtime_profile: DataFusionRuntimeProfile | None = None


@dataclass(frozen=True)
class ExternalIndexSelection:
    """Candidate-file selection from an external index provider."""

    candidate_files: tuple[Path, ...]
    total_files: int
    candidate_file_count: int
    pruned_file_count: int
    metadata: Mapping[str, object] = field(default_factory=dict)


class ExternalIndexProvider(Protocol):
    """Contract implemented by external index providers."""

    provider_name: str

    def supports(self, request: ExternalIndexRequest) -> bool:
        """Return whether this provider supports the request."""
        ...

    def select_candidates(
        self,
        ctx: SessionContext,
        *,
        request: ExternalIndexRequest,
    ) -> ExternalIndexSelection | None:
        """Return candidate files for ``request`` or ``None``."""
        ...


def select_candidates_with_external_indexes(
    ctx: SessionContext,
    *,
    request: ExternalIndexRequest,
    providers: Sequence[ExternalIndexProvider],
) -> tuple[ExternalIndexSelection | None, str | None]:
    """Resolve candidate files from the first provider that returns a result.

    Returns:
    -------
    tuple[ExternalIndexSelection | None, str | None]
        Candidate selection and provider name when available.
    """
    for provider in providers:
        if not provider.supports(request):
            continue
        try:
            selection = provider.select_candidates(ctx, request=request)
        except Exception:
            _LOGGER.exception(
                "external_index_provider_failed",
                extra={
                    "dataset_name": request.dataset_name,
                    "provider_name": provider.provider_name,
                },
            )
            continue
        if selection is not None:
            return selection, provider.provider_name
    return None, None


__all__ = [
    "ExternalIndexProvider",
    "ExternalIndexRequest",
    "ExternalIndexSelection",
    "select_candidates_with_external_indexes",
]
