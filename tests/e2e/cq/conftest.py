"""Pytest fixtures and utilities for cq E2E tests.

Provides reusable test infrastructure for end-to-end cq command testing.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import msgspec
import pytest

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult, Finding


@dataclass(frozen=True)
class FindingCriteria:
    category: str | None = None
    message_contains: str | None = None
    file: str | None = None
    severity: str | None = None
    detail_checks: dict[str, str | int | bool] = field(default_factory=dict)

    def matches(self, finding: Finding) -> bool:
        if self.category is not None and finding.category != self.category:
            return False
        if self.message_contains is not None and self.message_contains not in finding.message:
            return False
        if self.file is not None and (finding.anchor is None or self.file not in finding.anchor.file):
            return False
        if self.severity is not None and finding.severity != self.severity:
            return False
        if not self.detail_checks:
            return True
        return all(finding.details.get(k) == v for k, v in self.detail_checks.items())

    def summary(self) -> str:
        criteria = []
        if self.category:
            criteria.append(f"category={self.category}")
        if self.message_contains:
            criteria.append(f"message_contains={self.message_contains}")
        if self.file:
            criteria.append(f"file={self.file}")
        if self.severity:
            criteria.append(f"severity={self.severity}")
        for key, value in self.detail_checks.items():
            criteria.append(f"{key}={value}")
        return ", ".join(criteria)


def _iter_findings(result: CqResult) -> Iterable[Finding]:
    yield from result.key_findings
    yield from result.evidence
    for section in result.sections:
        yield from section.findings


def _find_matching_finding(
    result: CqResult,
    criteria: FindingCriteria,
) -> tuple[Finding | None, int]:
    findings = list(_iter_findings(result))
    for finding in findings:
        if criteria.matches(finding):
            return finding, len(findings)
    return None, len(findings)


def _build_error_message(criteria: FindingCriteria, total_findings: int) -> str:
    criteria_str = criteria.summary()
    return f"No finding matching criteria: {criteria_str}\nTotal findings: {total_findings}"


def _find_repo_root() -> Path:
    """Find the CodeAnatomy repository root.

    Returns
    -------
    Path
        Absolute path to repository root.

    Raises
    ------
    RuntimeError
        If repository root cannot be determined.
    """
    current = Path(__file__).resolve()
    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent
    msg = "Could not find repository root from test file"
    raise RuntimeError(msg)


@pytest.fixture(scope="session")
def repo_root() -> Path:
    """Provide the CodeAnatomy repository root Path.

    Returns
    -------
    Path
        Absolute path to the repository root.
    """
    return _find_repo_root()


@pytest.fixture
def run_command(repo_root: Path) -> Callable[[list[str]], subprocess.CompletedProcess[str]]:
    """Provide a helper to run cq CLI commands.

    Parameters
    ----------
    repo_root : Path
        Repository root fixture.

    Returns
    -------
    Callable[[list[str]], subprocess.CompletedProcess[str]]
        Function that runs commands and returns CompletedProcess.

    Examples
    --------
    >>> result = run_command(["cq", "q", "entity=function name=foo"])
    >>> assert result.returncode == 0
    """

    def _run(args: list[str]) -> subprocess.CompletedProcess[str]:
        """Run a command in the repository root.

        Parameters
        ----------
        args : list[str]
            Command arguments (e.g., ["cq", "q", "entity=function"]).

        Returns
        -------
        subprocess.CompletedProcess[str]
            Completed process with stdout, stderr, returncode.
        """
        return subprocess.run(
            args,
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
            timeout=180,
        )

    return _run


@pytest.fixture
def run_query(
    run_command: Callable[[list[str]], subprocess.CompletedProcess[str]],
) -> Callable[[str], CqResult]:
    """Provide a helper to run cq query commands and parse results.

    Parameters
    ----------
    run_command : Callable
        Command runner fixture.

    Returns
    -------
    Callable[[str], CqResult]
        Function that executes a query and returns parsed CqResult.

    Examples
    --------
    >>> result = run_query("entity=function name=build_graph_product")
    >>> assert result.run.macro == "q"
    """

    def _query(query_string: str) -> CqResult:
        """Execute a cq query and parse the JSON result.

        Parameters
        ----------
        query_string : str
            Query string (e.g., "entity=function name=foo").

        Returns
        -------
        CqResult
            Parsed query result.

        Raises
        ------
        RuntimeError
            If command fails or JSON parsing fails.
        """
        from tools.cq.core.schema import CqResult

        proc = run_command(
            [
                "uv",
                "run",
                "python",
                "-m",
                "tools.cq.cli",
                "q",
                query_string,
                "--format",
                "json",
                "--no-save-artifact",
            ]
        )

        if proc.returncode != 0:
            msg = f"Query failed with code {proc.returncode}: {proc.stderr}"
            raise RuntimeError(msg)

        try:
            return msgspec.json.decode(proc.stdout.encode("utf-8"), type=CqResult)
        except msgspec.ValidationError as e:
            msg = f"Failed to decode CQ JSON output: {e}\nOutput: {proc.stdout[:500]}"
            raise RuntimeError(msg) from e

    return _query


@pytest.fixture
def assert_finding_exists() -> Callable[..., Finding]:
    """Provide a helper to assert a finding with specific criteria exists.

    Returns
    -------
    Callable[..., Finding]
        Function that searches for and returns a matching finding.

    Examples
    --------
    >>> finding = assert_finding_exists(result, category="call_site", file="graph.py")
    >>> assert finding.anchor.file == "graph.py"
    """
    def _assert(
        result: CqResult,
        *,
        category: str | None = None,
        message_contains: str | None = None,
        file: str | None = None,
        severity: str | None = None,
        **detail_checks: str | int | bool,
    ) -> Finding:
        """Assert a finding matching criteria exists in result.

        Parameters
        ----------
        result : CqResult
            Query result to search.
        category : str | None
            Required category value.
        message_contains : str | None
            Substring that must appear in message.
        file : str | None
            Required file path (in anchor).
        severity : str | None
            Required severity level.
        **detail_checks : str | int | bool
            Additional key=value checks in finding.details.

        Returns
        -------
        Finding
            First matching finding.

        Raises
        ------
        AssertionError
            If no matching finding is found.
        """
        criteria = FindingCriteria(
            category=category,
            message_contains=message_contains,
            file=file,
            severity=severity,
            detail_checks=dict(detail_checks),
        )
        finding, total_findings = _find_matching_finding(result, criteria)
        if finding is not None:
            return finding
        msg = _build_error_message(criteria, total_findings)
        raise AssertionError(msg)

    return _assert


@pytest.fixture
def update_golden(request: pytest.FixtureRequest) -> bool:
    """Provide flag indicating whether to update golden snapshots.

    Parameters
    ----------
    request : pytest.FixtureRequest
        Pytest request fixture.

    Returns
    -------
    bool
        True if --update-golden was passed, False otherwise.

    Examples
    --------
    >>> if update_golden:
    ...     write_golden(result)
    >>> else:
    ...     assert result == read_golden()
    """
    return request.config.getoption("--update-golden", default=False)


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers for cq E2E tests.

    Parameters
    ----------
    config : pytest.Config
        Pytest configuration object.
    """
    config.addinivalue_line(
        "markers",
        "requires_ast_grep: mark test as requiring ast-grep (sg) to be installed",
    )


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Skip tests that require ast-grep if it is not installed.

    Parameters
    ----------
    items : list[pytest.Item]
        Collected test items.
    """
    from tools.cq.core.toolchain import Toolchain

    tc = Toolchain.detect()

    skip_marker = pytest.mark.skip(reason="ast-grep-py is not installed")

    for item in items:
        if "requires_ast_grep" in item.keywords and not tc.has_sgpy:
            item.add_marker(skip_marker)
