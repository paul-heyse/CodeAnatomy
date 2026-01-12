"""Finalize context utilities for contract enforcement."""

from __future__ import annotations

from dataclasses import dataclass, field

from arrowdsl.contracts import Contract
from arrowdsl.finalize import (
    ERROR_ARTIFACT_SPEC,
    ErrorArtifactSpec,
    FinalizeOptions,
    FinalizeResult,
    finalize,
)
from arrowdsl.kernels import ChunkPolicy
from arrowdsl.pyarrow_protocols import TableLike
from arrowdsl.runtime import ExecutionContext
from arrowdsl.schema_ops import SchemaTransform


@dataclass(frozen=True)
class FinalizeContext:
    """Reusable finalize configuration for a contract."""

    contract: Contract
    error_spec: ErrorArtifactSpec = ERROR_ARTIFACT_SPEC
    transform: SchemaTransform | None = None
    chunk_policy: ChunkPolicy = field(default_factory=ChunkPolicy)

    def run(self, table: TableLike, ctx: ExecutionContext) -> FinalizeResult:
        """Finalize a table using the stored contract and context.

        Returns
        -------
        FinalizeResult
            Finalized table bundle.
        """
        options = FinalizeOptions(
            error_spec=self.error_spec,
            transform=self.transform,
            chunk_policy=self.chunk_policy,
        )
        return finalize(
            table,
            contract=self.contract,
            ctx=ctx,
            options=options,
        )
