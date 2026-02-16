"""Tests for flattened id module."""
# ruff: noqa: D101, D102

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.core.id import canonicalize_payload, stable_digest, stable_digest24

DIGEST24_LENGTH = 24


class TestCanonicalizePayload:
    def test_none(self) -> None:
        assert canonicalize_payload(None) is None

    def test_string(self) -> None:
        assert canonicalize_payload("hello") == "hello"

    def test_dict_sorted(self) -> None:
        result = canonicalize_payload({"b": 2, "a": 1})
        assert isinstance(result, Mapping)
        assert list(result.keys()) == ["a", "b"]

    def test_list(self) -> None:
        assert canonicalize_payload([3, 1, 2]) == [3, 1, 2]


class TestStableDigest:
    def test_deterministic(self) -> None:
        d1 = stable_digest({"a": 1, "b": 2})
        d2 = stable_digest({"b": 2, "a": 1})
        assert d1 == d2

    def test_digest24_length(self) -> None:
        d = stable_digest24({"x": 1})
        assert len(d) == DIGEST24_LENGTH
