"""Orchestration authority context for execution-layer composition."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from extract.coordination.evidence_plan import EvidencePlan
    from semantics.compile_context import SemanticExecutionContext
    from semantics.program_manifest import ManifestDatasetResolver, SemanticProgramManifest


@dataclass(frozen=True)
class ExecutionAuthorityContext:
    """Orchestration authority context composing semantic + execution concerns.

    Waves 1-2 thread ``SemanticExecutionContext`` (semantic compile artifacts only).
    Waves 3+ build ``ExecutionAuthorityContext`` on top for orchestration fields.

    Parameters
    ----------
    semantic_context
        Compiled semantic execution context (manifest, resolver, runtime profile).
    evidence_plan
        Evidence plan describing required extract datasets.
    extract_executor_map
        Mapping of executor names to executor instances.
    capability_snapshot
        DataFusion runtime capability detection result.
    session_runtime_fingerprint
        Stable fingerprint of the session runtime configuration.
    """

    semantic_context: SemanticExecutionContext
    evidence_plan: EvidencePlan | None = None
    extract_executor_map: Mapping[str, Any] | None = None
    capability_snapshot: object | None = None
    session_runtime_fingerprint: str | None = None

    @property
    def manifest(self) -> SemanticProgramManifest:
        """Return the semantic program manifest from the semantic context."""
        return self.semantic_context.manifest

    @property
    def dataset_resolver(self) -> ManifestDatasetResolver:
        """Return the dataset resolver from the semantic context."""
        return self.semantic_context.dataset_resolver


__all__ = [
    "ExecutionAuthorityContext",
]
