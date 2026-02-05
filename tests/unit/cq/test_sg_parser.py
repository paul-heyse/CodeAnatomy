"""Tests for sg_parser helpers."""

from __future__ import annotations

import pytest
from tools.cq.query.parser import QueryParseError
from tools.cq.query.sg_parser import normalize_record_types


def test_normalize_record_types_invalid() -> None:
    """Invalid record types should raise a QueryParseError."""
    with pytest.raises(QueryParseError, match="Invalid record types"):
        normalize_record_types({"bogus"})
