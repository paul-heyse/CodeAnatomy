"""Schema definitions for cq results.

Dataclasses defining the structured output format for all cq macros.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from tools.cq import SCHEMA_VERSION


@dataclass
class Anchor:
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
    line: int
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

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict.

        Returns
        -------
        dict[str, Any]
            JSON-serializable representation.
        """
        d: dict[str, Any] = {"file": self.file, "line": self.line}
        if self.col is not None:
            d["col"] = self.col
        if self.end_line is not None:
            d["end_line"] = self.end_line
        if self.end_col is not None:
            d["end_col"] = self.end_col
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Anchor:
        """Create Anchor from a dict."""
        return cls(
            file=str(data.get("file", "")),
            line=int(data.get("line", 0)),
            col=data.get("col"),
            end_line=data.get("end_line"),
            end_col=data.get("end_col"),
        )


@dataclass
class Finding:
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
    severity: str = "info"
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict.

        Returns
        -------
        dict[str, Any]
            JSON-serializable representation.
        """
        return {
            "category": self.category,
            "message": self.message,
            "anchor": self.anchor.to_dict() if self.anchor else None,
            "severity": self.severity,
            "details": self.details,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Finding:
        """Create Finding from a dict."""
        anchor_data = data.get("anchor")
        anchor = Anchor.from_dict(anchor_data) if isinstance(anchor_data, dict) else None
        return cls(
            category=str(data.get("category", "")),
            message=str(data.get("message", "")),
            anchor=anchor,
            severity=str(data.get("severity", "info")),
            details=dict(data.get("details", {})),
        )


@dataclass
class Section:
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
    findings: list[Finding] = field(default_factory=list)
    collapsed: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict.

        Returns
        -------
        dict[str, Any]
            JSON-serializable representation.
        """
        return {
            "title": self.title,
            "findings": [f.to_dict() for f in self.findings],
            "collapsed": self.collapsed,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Section:
        """Create Section from a dict."""
        findings_data = data.get("findings", [])
        findings = [Finding.from_dict(f) for f in findings_data if isinstance(f, dict)]
        return cls(
            title=str(data.get("title", "")),
            findings=findings,
            collapsed=bool(data.get("collapsed", False)),
        )


@dataclass
class Artifact:
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

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict.

        Returns
        -------
        dict[str, Any]
            JSON-serializable representation.
        """
        return {"path": self.path, "format": self.format}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Artifact:
        """Create Artifact from a dict."""
        return cls(
            path=str(data.get("path", "")),
            format=str(data.get("format", "json")),
        )


@dataclass
class RunMeta:
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
    """

    macro: str
    argv: list[str]
    root: str
    started_ms: float
    elapsed_ms: float
    toolchain: dict[str, str | None] = field(default_factory=dict)
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict.

        Returns
        -------
        dict[str, Any]
            JSON-serializable representation.
        """
        return {
            "schema_version": self.schema_version,
            "macro": self.macro,
            "argv": self.argv,
            "root": self.root,
            "started_ms": self.started_ms,
            "elapsed_ms": self.elapsed_ms,
            "toolchain": self.toolchain,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RunMeta:
        """Create RunMeta from a dict."""
        return cls(
            schema_version=str(data.get("schema_version", "cq.v1")),
            macro=str(data.get("macro", "")),
            argv=list(data.get("argv", [])),
            root=str(data.get("root", "")),
            started_ms=float(data.get("started_ms", 0.0)),
            elapsed_ms=float(data.get("elapsed_ms", 0.0)),
            toolchain=dict(data.get("toolchain", {})),
        )


@dataclass
class CqResult:
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
    summary: dict[str, Any] = field(default_factory=dict)
    key_findings: list[Finding] = field(default_factory=list)
    evidence: list[Finding] = field(default_factory=list)
    sections: list[Section] = field(default_factory=list)
    artifacts: list[Artifact] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict.

        Returns
        -------
        dict[str, Any]
            JSON-serializable representation.
        """
        return {
            "run": self.run.to_dict(),
            "summary": self.summary,
            "key_findings": [f.to_dict() for f in self.key_findings],
            "evidence": [f.to_dict() for f in self.evidence],
            "sections": [s.to_dict() for s in self.sections],
            "artifacts": [a.to_dict() for a in self.artifacts],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CqResult:
        """Create CqResult from a dict."""
        run_data = data.get("run", {})
        run = RunMeta.from_dict(run_data) if isinstance(run_data, dict) else RunMeta(
            schema_version="cq.v1",
            macro="",
            argv=[],
            root="",
            started_ms=0.0,
            elapsed_ms=0.0,
            toolchain={},
        )
        key_findings = [
            Finding.from_dict(f) for f in data.get("key_findings", []) if isinstance(f, dict)
        ]
        evidence = [
            Finding.from_dict(f) for f in data.get("evidence", []) if isinstance(f, dict)
        ]
        sections = [
            Section.from_dict(s) for s in data.get("sections", []) if isinstance(s, dict)
        ]
        artifacts = [
            Artifact.from_dict(a) for a in data.get("artifacts", []) if isinstance(a, dict)
        ]
        return cls(
            run=run,
            summary=dict(data.get("summary", {})),
            key_findings=key_findings,
            evidence=evidence,
            sections=sections,
            artifacts=artifacts,
        )


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

    elapsed = time.time() * 1000 - started_ms
    return RunMeta(
        macro=macro,
        argv=argv,
        root=root,
        started_ms=started_ms,
        elapsed_ms=elapsed,
        toolchain=toolchain,
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
