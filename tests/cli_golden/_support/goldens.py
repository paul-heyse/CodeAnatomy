"""Golden file utilities for CLI tests."""

from __future__ import annotations

from pathlib import Path

GOLDENS_DIR = Path(__file__).resolve().parent.parent / "fixtures"


def assert_text_snapshot(
    fixture_name: str,
    actual: str,
    *,
    update: bool = False,
) -> None:
    """Assert that actual text matches the golden file, optionally updating it.

    Args:
        fixture_name: Description.
        actual: Description.
        update: Description.

    Raises:
        AssertionError: If the operation cannot be completed.
    """
    fixture_path = GOLDENS_DIR / fixture_name

    if update:
        fixture_path.parent.mkdir(parents=True, exist_ok=True)
        fixture_path.write_text(actual, encoding="utf-8")
        return

    if not fixture_path.exists():
        msg = f"Golden file not found: {fixture_path}. Run with --update-golden to create."
        raise AssertionError(msg)

    expected = fixture_path.read_text(encoding="utf-8")
    if actual != expected:
        # Show diff for debugging
        import difflib

        diff = difflib.unified_diff(
            expected.splitlines(keepends=True),
            actual.splitlines(keepends=True),
            fromfile=f"expected ({fixture_name})",
            tofile="actual",
        )
        diff_str = "".join(diff)
        msg = f"Golden file mismatch for {fixture_name}:\n{diff_str}"
        raise AssertionError(msg)


__all__ = ["GOLDENS_DIR", "assert_text_snapshot"]
