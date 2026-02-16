"""Tests for flattened id module."""
# ruff: noqa: D101, D102

from __future__ import annotations

from collections.abc import Mapping

from tools.cq.core.id import canonicalize_payload, stable_digest, stable_digest24

DIGEST24_LENGTH = 24


class TestCanonicalizePayload:
    @staticmethod
    def test_none() -> None:
        assert canonicalize_payload(None) is None

    @staticmethod
    def test_string() -> None:
        assert canonicalize_payload("hello") == "hello"

    @staticmethod
    def test_dict_sorted() -> None:
        result = canonicalize_payload({"b": 2, "a": 1})
        assert isinstance(result, Mapping)
        assert list(result.keys()) == ["a", "b"]

    @staticmethod
    def test_list() -> None:
        assert canonicalize_payload([3, 1, 2]) == [3, 1, 2]


class TestStableDigest:
    @staticmethod
    def test_deterministic() -> None:
        d1 = stable_digest({"a": 1, "b": 2})
        d2 = stable_digest({"b": 2, "a": 1})
        assert d1 == d2

    @staticmethod
    def test_digest24_length() -> None:
        d = stable_digest24({"x": 1})
        assert len(d) == DIGEST24_LENGTH
