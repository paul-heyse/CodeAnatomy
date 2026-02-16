# ruff: noqa: D103
"""Tests for extractor protocol definitions."""

from __future__ import annotations

from typing import Protocol

from extract.protocols import ExtractorPort


def test_extractor_port_is_protocol() -> None:
    assert issubclass(ExtractorPort, Protocol)
