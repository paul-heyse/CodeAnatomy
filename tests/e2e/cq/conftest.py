"""Pytest fixtures and utilities for cq E2E tests.

Provides reusable test infrastructure for end-to-end cq command testing.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

    from tools.cq.core.schema import CqResult, Finding


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
            timeout=60,
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
        TypeError
            If parsed JSON output is not a dictionary.
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
            data = json.loads(proc.stdout)
        except json.JSONDecodeError as e:
            msg = f"Failed to parse JSON output: {e}\nOutput: {proc.stdout[:500]}"
            raise RuntimeError(msg) from e

        if not isinstance(data, dict):
            msg = "Expected CQ JSON output to be a dictionary."
            raise TypeError(msg)
        return CqResult.from_dict(cast("dict[str, Any]", data))

    return _query


@pytest.fixture
def assert_finding_exists() -> Callable[..., Finding]:  # noqa: C901
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

    def _matches_criteria(  # noqa: PLR0913
        finding: Finding,
        *,
        category: str | None,
        message_contains: str | None,
        file: str | None,
        severity: str | None,
        detail_checks: dict[str, str | int | bool],
    ) -> bool:
        """Check if finding matches all criteria.

        Returns
        -------
        bool
            True if all criteria match.
        """
        if category is not None and finding.category != category:
            return False
        if message_contains is not None and message_contains not in finding.message:
            return False
        if file is not None and (finding.anchor is None or file not in finding.anchor.file):
            return False
        if severity is not None and finding.severity != severity:
            return False
        return not (
            detail_checks and not all(finding.details.get(k) == v for k, v in detail_checks.items())
        )

    def _build_error_message(  # noqa: PLR0913, PLR0917
        category: str | None,
        message_contains: str | None,
        file: str | None,
        severity: str | None,
        detail_checks: dict[str, str | int | bool],
        total_findings: int,
    ) -> str:
        """Build helpful error message for assertion failure.

        Returns
        -------
        str
            Formatted error message.
        """
        criteria = []
        if category:
            criteria.append(f"category={category}")
        if message_contains:
            criteria.append(f"message_contains={message_contains}")
        if file:
            criteria.append(f"file={file}")
        if severity:
            criteria.append(f"severity={severity}")
        for k, v in detail_checks.items():
            criteria.append(f"{k}={v}")

        criteria_str = ", ".join(criteria)
        return f"No finding matching criteria: {criteria_str}\nTotal findings: {total_findings}"

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
        all_findings = result.key_findings + result.evidence
        for section in result.sections:
            all_findings.extend(section.findings)

        for finding in all_findings:
            if _matches_criteria(
                finding,
                category=category,
                message_contains=message_contains,
                file=file,
                severity=severity,
                detail_checks=detail_checks,
            ):
                return finding

        msg = _build_error_message(
            category, message_contains, file, severity, detail_checks, len(all_findings)
        )
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

    skip_marker = pytest.mark.skip(reason="ast-grep (sg) is not installed")

    for item in items:
        if "requires_ast_grep" in item.keywords and not tc.has_sg:
            item.add_marker(skip_marker)
