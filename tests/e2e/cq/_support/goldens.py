"""Golden snapshot utilities for cq E2E tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from tools.cq.core.schema import CqResult

GOLDENS_DIR = Path(__file__).resolve().parent.parent / "fixtures"


def _normalize_result_for_snapshot(result: CqResult) -> dict[str, Any]:
    """Normalize a CqResult for golden snapshot comparison.

    Removes timestamps and other non-deterministic fields.

    Parameters
    ----------
    result : CqResult
        Query result to normalize.

    Returns:
    -------
    dict[str, Any]
        Normalized result dictionary suitable for snapshot comparison.
    """
    # Convert to dict first
    result_dict = _result_to_dict(result)

    # Remove timestamp fields that vary between runs
    if "run" in result_dict:
        result_dict["run"].pop("started_ms", None)
        result_dict["run"].pop("elapsed_ms", None)
        result_dict["run"].pop("run_id", None)
        root = result_dict["run"].get("root")
        if isinstance(root, str):
            result_dict["run"]["root"] = "<repo_root>"
        toolchain = result_dict["run"].get("toolchain")
        if isinstance(toolchain, dict) and isinstance(toolchain.get("rg"), str):
            toolchain["rg"] = "<ripgrep>"

    summary = result_dict.get("summary")
    if isinstance(summary, dict):
        summary.pop("cache", None)
        summary.pop("cache_stats", None)

    # Sort findings by file path and line number for deterministic ordering
    for key in ("key_findings", "evidence"):
        if key in result_dict:
            result_dict[key] = sorted(
                result_dict[key],
                key=lambda f: (
                    f.get("anchor", {}).get("file", ""),
                    f.get("anchor", {}).get("line", 0),
                ),
            )

    if "sections" in result_dict:
        for section in result_dict["sections"]:
            if "findings" in section:
                section["findings"] = sorted(
                    section["findings"],
                    key=lambda f: (
                        f.get("anchor", {}).get("file", ""),
                        f.get("anchor", {}).get("line", 0),
                    ),
                )

    return result_dict


def _result_to_dict(result: CqResult) -> dict[str, Any]:
    """Convert CqResult to dictionary.

    Parameters
    ----------
    result : CqResult
        Query result to convert.

    Returns:
    -------
    dict[str, Any]
        Result as dictionary.
    """
    from tools.cq.core.serialization import to_builtins

    return to_builtins(result)


def assert_json_snapshot(
    fixture_name: str,
    actual: CqResult,
    *,
    update: bool = False,
) -> None:
    """Assert that actual result matches the golden JSON snapshot.

    Args:
        fixture_name: Description.
        actual: Description.
        update: Description.

    Raises:
        AssertionError: If the operation cannot be completed.
    """
    fixture_path = GOLDENS_DIR / fixture_name

    normalized = _normalize_result_for_snapshot(actual)

    if update:
        fixture_path.parent.mkdir(parents=True, exist_ok=True)
        fixture_path.write_text(
            json.dumps(normalized, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        return

    if not fixture_path.exists():
        msg = f"Golden file not found: {fixture_path}. Run with --update-golden to create."
        raise AssertionError(msg)

    expected = json.loads(fixture_path.read_text(encoding="utf-8"))

    if normalized != expected:
        # Show diff for debugging
        import difflib

        expected_str = json.dumps(expected, indent=2, sort_keys=True)
        actual_str = json.dumps(normalized, indent=2, sort_keys=True)

        diff = difflib.unified_diff(
            expected_str.splitlines(keepends=True),
            actual_str.splitlines(keepends=True),
            fromfile=f"expected ({fixture_name})",
            tofile="actual",
        )
        diff_str = "".join(diff)
        msg = f"Golden file mismatch for {fixture_name}:\n{diff_str}"
        raise AssertionError(msg)


def load_golden_query_spec(fixture_name: str) -> dict[str, Any]:
    """Load a golden query specification fixture.

    Args:
        fixture_name: Description.

    Returns:
        dict[str, Any]: Result.

    Raises:
        FileNotFoundError: If the operation cannot be completed.
    """
    fixture_path = GOLDENS_DIR / fixture_name

    if not fixture_path.exists():
        msg = f"Fixture file not found: {fixture_path}"
        raise FileNotFoundError(msg)

    return json.loads(fixture_path.read_text(encoding="utf-8"))


__all__ = [
    "GOLDENS_DIR",
    "assert_json_snapshot",
    "load_golden_query_spec",
]
