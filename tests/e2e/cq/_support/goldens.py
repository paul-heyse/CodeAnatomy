"""Golden snapshot utilities for CQ E2E tests."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

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

    _normalize_run_block(result_dict)
    _normalize_summary_block(result_dict)
    _sort_result_findings(result_dict)

    return cast("dict[str, Any]", _scrub_unstable(result_dict))


def _finding_sort_key(finding: dict[str, object]) -> tuple[str, int]:
    anchor = finding.get("anchor")
    if isinstance(anchor, dict):
        file_value = anchor.get("file")
        line_value = anchor.get("line")
        return (
            file_value if isinstance(file_value, str) else "",
            line_value if isinstance(line_value, int) else 0,
        )
    return ("", 0)


def _normalize_run_block(result_dict: dict[str, Any]) -> None:
    run = result_dict.get("run")
    if not isinstance(run, dict):
        return
    run.pop("started_ms", None)
    run.pop("elapsed_ms", None)
    run.pop("run_id", None)
    run.pop("run_uuid_version", None)
    run.pop("run_created_ms", None)
    root = run.get("root")
    if isinstance(root, str):
        run["root"] = "<repo_root>"
    toolchain = run.get("toolchain")
    if isinstance(toolchain, dict) and isinstance(toolchain.get("rg"), str):
        toolchain["rg"] = "<ripgrep>"


def _normalize_summary_block(result_dict: dict[str, Any]) -> None:
    summary = result_dict.get("summary")
    if not isinstance(summary, dict):
        return
    summary.pop("cache", None)
    summary.pop("cache_stats", None)
    summary.pop("cache_backend", None)
    summary.pop("step_summaries", None)
    summary.pop("semantic_planes", None)


def _sort_result_findings(result_dict: dict[str, Any]) -> None:
    for key in ("key_findings", "evidence"):
        if key in result_dict:
            result_dict[key] = sorted(result_dict[key], key=_finding_sort_key)

    sections = result_dict.get("sections")
    if not isinstance(sections, list):
        return
    for section in sections:
        if isinstance(section, dict) and "findings" in section:
            section["findings"] = sorted(section["findings"], key=_finding_sort_key)


def _normalize_json_value(value: object) -> object:
    """Normalize arbitrary JSON-compatible values for stable snapshots.

    Returns:
        object: Normalized snapshot-safe value.
    """
    from tools.cq.core.serialization import to_builtins

    return _scrub_unstable(to_builtins(value))


def _normalize_text_value(value: str) -> str:
    """Normalize text snapshots for machine/path stability.

    Returns:
        str: Normalized text value.
    """
    return _scrub_text_paths(value)


_ABS_PATH_PATTERNS = (
    re.compile(r"/Users/[^/]+/CodeAnatomy"),
    re.compile(r"/home/[^/]+/CodeAnatomy"),
    re.compile(r"[A-Za-z]:\\\\[^\\s]+CodeAnatomy"),
)
_DURATION_PATTERN = re.compile(r"(\*\*(?:Created|Elapsed):\*\*)\s+[0-9]+(?:\.[0-9]+)?ms")
_BUNDLE_ID_PATTERN = re.compile(r"(\*\*Bundle ID:\*\*)\s+[0-9a-fA-F]+")


def _scrub_text_paths(text: str) -> str:
    normalized = text.replace("\\r\\n", "\\n")
    for pattern in _ABS_PATH_PATTERNS:
        normalized = pattern.sub("<repo_root>", normalized)
    normalized = _DURATION_PATTERN.sub(r"\1 <duration_ms>", normalized)
    return _BUNDLE_ID_PATTERN.sub(r"\1 <bundle_id>", normalized)


def _scrub_unstable(value: object) -> object:
    if isinstance(value, str):
        return _scrub_text_paths(value)
    if isinstance(value, Mapping):
        return _scrub_mapping(dict(value))
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_scrub_unstable(item) for item in value]
    return value


def _scrub_mapping(value: dict[str, object]) -> dict[str, object]:
    scrubbed = {str(k): _scrub_unstable(v) for k, v in value.items()}
    for id_key in ("stable_id", "execution_id", "id_taxonomy"):
        scrubbed.pop(id_key, None)

    run = scrubbed.get("run")
    if isinstance(run, dict):
        for unstable_key in (
            "started_ms",
            "elapsed_ms",
            "run_id",
            "run_uuid_version",
            "run_created_ms",
        ):
            run.pop(unstable_key, None)
        root = run.get("root")
        if isinstance(root, str):
            run["root"] = "<repo_root>"
        toolchain = run.get("toolchain")
        if isinstance(toolchain, dict):
            rg_path = toolchain.get("rg")
            if isinstance(rg_path, str):
                toolchain["rg"] = "<ripgrep>"

    summary = scrubbed.get("summary")
    if isinstance(summary, dict):
        summary.pop("cache", None)
        summary.pop("cache_stats", None)
        summary.pop("cache_backend", None)
        summary.pop("step_summaries", None)
        summary.pop("semantic_planes", None)
        summary.pop("timings", None)
        summary.pop("timing", None)

    return scrubbed


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

    Raises:
        TypeError: If serialization does not produce a dictionary payload.
    """
    from tools.cq.core.serialization import to_builtins

    builtins_result = to_builtins(result)
    if isinstance(builtins_result, dict):
        return cast("dict[str, Any]", builtins_result)
    msg = "expected dictionary payload from to_builtins(CqResult)"
    raise TypeError(msg)


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


def assert_json_snapshot_data(
    fixture_name: str,
    actual: Mapping[str, object] | Sequence[object],
    *,
    update: bool = False,
) -> None:
    """Assert arbitrary JSON payload against a golden snapshot.

    Raises:
        AssertionError: If snapshot is missing or payload mismatches.
    """
    fixture_path = GOLDENS_DIR / fixture_name
    normalized = _normalize_json_value(actual)
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


def assert_text_snapshot(
    fixture_name: str,
    actual: str,
    *,
    update: bool = False,
) -> None:
    """Assert text payload against a golden text snapshot.

    Raises:
        AssertionError: If snapshot is missing or text mismatches.
    """
    fixture_path = GOLDENS_DIR / fixture_name
    normalized = _normalize_text_value(actual)
    if update:
        fixture_path.parent.mkdir(parents=True, exist_ok=True)
        fixture_path.write_text(normalized, encoding="utf-8")
        return
    if not fixture_path.exists():
        msg = f"Golden file not found: {fixture_path}. Run with --update-golden to create."
        raise AssertionError(msg)
    expected = _normalize_text_value(fixture_path.read_text(encoding="utf-8"))
    if normalized != expected:
        import difflib

        diff = difflib.unified_diff(
            expected.splitlines(keepends=True),
            normalized.splitlines(keepends=True),
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


def load_golden_spec(fixture_name: str) -> dict[str, Any]:
    """Load any golden spec fixture by file name.

    Returns:
        dict[str, Any]: Loaded JSON fixture payload.
    """
    return load_golden_query_spec(fixture_name)


__all__ = [
    "GOLDENS_DIR",
    "assert_json_snapshot",
    "assert_json_snapshot_data",
    "assert_text_snapshot",
    "load_golden_query_spec",
    "load_golden_spec",
]
