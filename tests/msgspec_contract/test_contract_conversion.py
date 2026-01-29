"""Conversion contract tests for msgspec helpers."""

from __future__ import annotations

from dataclasses import dataclass

import msgspec
import pytest

from serde_msgspec import convert_from_attributes
from storage.deltalake.config import DeltaWritePolicy
from tests.msgspec_contract._support.goldens import GOLDENS_DIR, assert_text_snapshot
from tests.msgspec_contract._support.normalize import normalize_exception

TARGET_FILE_SIZE: int = 128


@dataclass(frozen=True)
class WritePolicyPayload:
    """Provide attribute payloads for conversion tests."""

    target_file_size: int
    stats_policy: str


def test_convert_from_attributes_success() -> None:
    """Validate attribute-based conversion for msgspec structs."""
    raw = WritePolicyPayload(target_file_size=TARGET_FILE_SIZE, stats_policy="auto")
    converted = convert_from_attributes(raw, target_type=DeltaWritePolicy, strict=True)
    assert converted.target_file_size == TARGET_FILE_SIZE
    assert converted.stats_policy == "auto"


def test_convert_from_attributes_error(*, update_goldens: bool) -> None:
    """Snapshot validation errors from attribute-based conversion."""
    raw = WritePolicyPayload(target_file_size=0, stats_policy="auto")
    with pytest.raises(msgspec.ValidationError) as exc_info:
        convert_from_attributes(raw, target_type=DeltaWritePolicy, strict=True)
    normalized = normalize_exception(exc_info.value)
    assert_text_snapshot(
        path=GOLDENS_DIR / "delta_write_policy.from_attributes.error.json",
        text=msgspec.json.format(msgspec.json.encode(normalized), indent=2).decode("utf-8"),
        update=update_goldens,
    )
