"""Golden snapshot helpers for msgspec contract tests."""

from __future__ import annotations

import base64
from pathlib import Path

GOLDENS_DIR = Path(__file__).resolve().parents[1] / "goldens"


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def assert_text_snapshot(*, path: Path, text: str, update: bool) -> None:
    """Compare text against a golden file, optionally updating.

    Raises
    ------
    AssertionError
        Raised when the golden snapshot is missing or mismatched.
    """
    if not text.endswith("\n"):
        text += "\n"

    if not path.exists():
        if update:
            _ensure_parent(path)
            path.write_text(text, encoding="utf-8")
            return
        msg = (
            f"Golden missing: {path}\n"
            "Run: pytest -q --update-goldens tests/msgspec_contract\n"
            "or set UPDATE_GOLDENS=1"
        )
        raise AssertionError(msg)

    if update:
        _ensure_parent(path)
        path.write_text(text, encoding="utf-8")
        return

    expected = path.read_text(encoding="utf-8")
    assert text == expected


def assert_bytes_snapshot_b64(*, path: Path, data: bytes, update: bool) -> None:
    """Snapshot binary bytes as base64 text."""
    b64 = base64.b64encode(data).decode("ascii")
    assert_text_snapshot(path=path, text=b64, update=update)


def read_bytes_snapshot_b64(path: Path) -> bytes:
    """Read base64 snapshot and return raw bytes.

    Returns
    -------
    bytes
        Raw decoded bytes from the snapshot file.
    """
    raw = path.read_text(encoding="utf-8").strip()
    return base64.b64decode(raw)
