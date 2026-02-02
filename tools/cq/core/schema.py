"""Schema definitions for cq results.

Structs defining the structured output format for all cq macros.
"""

from __future__ import annotations

from typing import Annotated, Literal

import msgspec

from tools.cq import SCHEMA_VERSION


class Anchor(msgspec.Struct, frozen=True, omit_defaults=True):
    """Source code location anchor.

    Parameters
    ----------
    file : str
        Relative file path from repo root.
    line : int
        1-indexed line number.
    col : int | None
        0-indexed column offset, if available.
    end_line : int | None
        End line for multi-line spans.
    end_col : int | None
        End column for multi-line spans.
    """

    file: str
    line: Annotated[int, msgspec.Meta(ge=1)]
    col: int | None = None
    end_line: int | None = None
    end_col: int | None = None

    def to_ref(self) -> str:
        """Return file:line reference string.

        Returns
        -------
        str
            File reference string.
        """
        return f"{self.file}:{self.line}"


class Finding(msgspec.Struct):
    """A discrete analysis finding.

    Parameters
    ----------
    category : str
        Finding type (e.g., "call_site", "import", "exception").
    message : str
        Human-readable description.
    anchor : Anchor | None
        Source location, if applicable.
    severity : str
        One of "info", "warning", "error".
    details : dict[str, Any]
        Additional structured data.
    """

    category: str
    message: str
    anchor: Anchor | None = None
    severity: Literal["info", "warning", "error"] = "info"
    details: dict[str, object] = msgspec.field(default_factory=dict)


class Section(msgspec.Struct):
    """A logical grouping of findings with a heading.

    Parameters
    ----------
    title : str
        Section heading.
    findings : list[Finding]
        Findings in this section.
    collapsed : bool
        Whether to render collapsed by default.
    """

    title: str
    findings: list[Finding] = msgspec.field(default_factory=list)
    collapsed: bool = False


class Artifact(msgspec.Struct):
    """A saved analysis artifact reference.

    Parameters
    ----------
    path : str
        Relative path to saved artifact.
    format : str
        File format (e.g., "json", "csv").
    """

    path: str
    format: str = "json"


class RunMeta(msgspec.Struct):
    """Metadata about a cq invocation.

    Parameters
    ----------
    macro : str
        Name of the macro invoked.
    argv : list[str]
        Command-line arguments.
    root : str
        Repository root path.
    started_ms : float
        Start timestamp in milliseconds since epoch.
    elapsed_ms : float
        Elapsed time in milliseconds.
    toolchain : dict[str, str | None]
        Available tool versions.
    schema_version : str
        Schema version string.
    run_id : str | None
        Stable run identifier for this invocation.
    """

    macro: str
    argv: list[str]
    root: str
    started_ms: float
    elapsed_ms: float
    toolchain: dict[str, str | None] = msgspec.field(default_factory=dict)
    schema_version: str = SCHEMA_VERSION
    run_id: str | None = None


class CqResult(msgspec.Struct):
    """Complete result of a cq macro invocation.

    Parameters
    ----------
    run : RunMeta
        Invocation metadata.
    summary : dict[str, Any]
        Key metrics and counts.
    key_findings : list[Finding]
        Top-level actionable findings.
    evidence : list[Finding]
        Supporting evidence findings.
    sections : list[Section]
        Organized finding groups.
    artifacts : list[Artifact]
        Saved artifact references.
    """

    run: RunMeta
    summary: dict[str, object] = msgspec.field(default_factory=dict)
    key_findings: list[Finding] = msgspec.field(default_factory=list)
    evidence: list[Finding] = msgspec.field(default_factory=list)
    sections: list[Section] = msgspec.field(default_factory=list)
    artifacts: list[Artifact] = msgspec.field(default_factory=list)


def mk_runmeta(
    macro: str,
    argv: list[str],
    root: str,
    started_ms: float,
    toolchain: dict[str, str | None],
) -> RunMeta:
    """Create RunMeta with elapsed time calculated from now.

    Parameters
    ----------
    macro : str
        Macro name.
    argv : list[str]
        Command arguments.
    root : str
        Repo root path.
    started_ms : float
        Start time in ms.
    toolchain : dict[str, str | None]
        Tool versions.

    Returns
    -------
    RunMeta
        Populated metadata.
    """
    import time
    from tools.cq.utils.uuid_factory import uuid7_str

    elapsed = time.time() * 1000 - started_ms
    return RunMeta(
        macro=macro,
        argv=argv,
        root=root,
        started_ms=started_ms,
        elapsed_ms=elapsed,
        toolchain=toolchain,
        run_id=uuid7_str(),
    )


def mk_result(run: RunMeta) -> CqResult:
    """Create empty CqResult with run metadata.

    Parameters
    ----------
    run : RunMeta
        Run metadata.

    Returns
    -------
    CqResult
        Empty result ready to populate.
    """
    return CqResult(run=run)


def ms() -> float:
    """Return current time in milliseconds since epoch.

    Returns
    -------
    float
        Current time in milliseconds.
    """
    import time

    return time.time() * 1000


__all__ = [
    "Anchor",
    "Artifact",
    "CqResult",
    "Finding",
    "RunMeta",
    "Section",
    "mk_result",
    "mk_runmeta",
    "ms",
]
