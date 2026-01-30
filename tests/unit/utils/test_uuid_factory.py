"""Tests for UUID factory helpers."""

from __future__ import annotations

import re
from pathlib import Path

from utils.uuid_factory import (
    UUID7_HEX_LENGTH,
    secure_token_hex,
    uuid7,
    uuid7_hex,
    uuid7_str,
    uuid7_suffix,
)

_REPO_ROOT = Path(__file__).resolve().parents[3]
_UUID4_PATTERN = re.compile(r"\buuid4\b|uuid\.uuid4")
_UUID_PARTS_COUNT = 5
_DEFAULT_SUFFIX_LENGTH = 12
_DEFAULT_TOKEN_BYTES = 16
_DEFAULT_TOKEN_HEX_LENGTH = _DEFAULT_TOKEN_BYTES * 2


def _iter_py_files(base: Path) -> list[Path]:
    return [path for path in base.rglob("*.py") if path.is_file()]


def test_uuid7_monotonic_order() -> None:
    """UUIDv7 values are monotone in a single-threaded sequence."""
    values = [uuid7().int for _ in range(200)]
    assert values == sorted(values)


def test_uuid7_hex_format() -> None:
    """UUIDv7 hex helper returns a 32-character lowercase hex string."""
    value = uuid7_hex()
    assert len(value) == UUID7_HEX_LENGTH
    assert all(ch in "0123456789abcdef" for ch in value)


def test_uuid7_str_format() -> None:
    """UUIDv7 string helper returns a standard UUID string."""
    value = uuid7_str()
    parts = value.split("-")
    assert len(parts) == _UUID_PARTS_COUNT
    assert all(parts)


def test_uuid7_suffix_length() -> None:
    """UUIDv7 suffix helper returns a hex suffix of the requested length."""
    value = uuid7_suffix(_DEFAULT_SUFFIX_LENGTH)
    assert len(value) == _DEFAULT_SUFFIX_LENGTH
    assert all(ch in "0123456789abcdef" for ch in value)


def test_secure_token_hex_length() -> None:
    """CSPRNG helper returns an expected-length hex token."""
    value = secure_token_hex(_DEFAULT_TOKEN_BYTES)
    assert len(value) == _DEFAULT_TOKEN_HEX_LENGTH
    assert all(ch in "0123456789abcdef" for ch in value)


def test_no_uuid4_usage_in_code() -> None:
    """Repository code should not reference uuid4 in executable paths."""
    scan_roots = [
        _REPO_ROOT / "src",
        _REPO_ROOT / "scripts",
        _REPO_ROOT / "rust" / "datafusion_python" / "python",
    ]
    offenders: list[str] = []
    for root in scan_roots:
        if not root.exists():
            continue
        for path in _iter_py_files(root):
            text = path.read_text(encoding="utf-8")
            if _UUID4_PATTERN.search(text):
                offenders.append(str(path.relative_to(_REPO_ROOT)))
    assert not offenders
