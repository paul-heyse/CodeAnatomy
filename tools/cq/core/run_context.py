"""Run context helpers for CQ pipelines."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from tools.cq.core.schema import RunMeta, mk_runmeta
from tools.cq.core.structs import CqStruct
from tools.cq.utils.uuid_temporal_contracts import resolve_run_identity_contract

if TYPE_CHECKING:
    from tools.cq.core.bootstrap import CqRuntimeServices
    from tools.cq.core.toolchain import Toolchain
    from tools.cq.query.enrichment import SymtableEnricher


class RunContext(CqStruct, frozen=True):
    """Shared run metadata context for CQ commands."""

    root: Path
    argv: tuple[str, ...]
    tc: Toolchain | None = None
    started_ms: float = 0.0
    run_id: str | None = None
    run_uuid_version: int | None = None
    run_created_ms: float | None = None

    @classmethod
    def from_parts(
        cls,
        *,
        root: Path,
        argv: list[str] | tuple[str, ...] | None,
        tc: Toolchain | None,
        started_ms: float,
        run_id: str | None = None,
    ) -> RunContext:
        """Construct a run context from common command inputs.

        Returns:
        -------
        RunContext
            Fully populated run context.
        """
        identity = resolve_run_identity_contract(run_id)
        return cls(
            root=root,
            argv=tuple(argv) if argv else (),
            tc=tc,
            started_ms=started_ms,
            run_id=identity.run_id,
            run_uuid_version=identity.run_uuid_version,
            run_created_ms=float(identity.run_created_ms),
        )

    def to_runmeta(self, macro: str) -> RunMeta:
        """Build RunMeta from the context.

        Returns:
        -------
        RunMeta
            Run metadata for the command.
        """
        toolchain = self.tc.to_dict() if self.tc else {}
        return mk_runmeta(
            macro=macro,
            argv=list(self.argv),
            root=str(self.root),
            started_ms=self.started_ms,
            toolchain=toolchain,
            run_id=self.run_id,
        )


class RunExecutionContext(Protocol):
    """CLI-independent execution context protocol for run engine flows."""

    @property
    def root(self) -> Path:
        """Workspace root."""
        ...

    @property
    def argv(self) -> list[str]:
        """Command argv."""
        ...

    @property
    def toolchain(self) -> Toolchain:
        """Detected toolchain capabilities."""
        ...

    @property
    def artifact_dir(self) -> Path | None:
        """Optional artifact directory."""
        ...

    @property
    def services(self) -> CqRuntimeServices:
        """Runtime service bundle resolved at composition root."""
        ...

    @property
    def symtable_enricher(self) -> SymtableEnricher:
        """Injected symtable enricher used by q execution paths."""
        ...


__all__ = [
    "RunContext",
    "RunExecutionContext",
]
