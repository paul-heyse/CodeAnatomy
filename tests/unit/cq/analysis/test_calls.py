from __future__ import annotations

from dataclasses import dataclass

from tools.cq.analysis.calls import (
    classify_call_against_signature,
    classify_calls_against_signature,
)
from tools.cq.analysis.signature import parse_signature


@dataclass(frozen=True)
class _Site:
    num_args: int
    kwargs: list[str]
    has_star_args: bool = False
    has_star_kwargs: bool = False


def test_classify_call_would_break_missing_required() -> None:
    params = parse_signature("foo(a, b)")
    bucket, reason = classify_call_against_signature(_Site(num_args=1, kwargs=[]), params)
    assert bucket == "would_break"
    assert "not provided" in reason


def test_classify_call_ambiguous_with_star_args() -> None:
    params = parse_signature("foo(a)")
    bucket, _ = classify_call_against_signature(_Site(num_args=0, kwargs=[], has_star_args=True), params)
    assert bucket == "ambiguous"


def test_classify_calls_groups_buckets() -> None:
    params = parse_signature("foo(a)")
    buckets = classify_calls_against_signature(
        [_Site(num_args=1, kwargs=[]), _Site(num_args=0, kwargs=[])],
        params,
    )
    assert len(buckets["ok"]) == 1
    assert len(buckets["would_break"]) == 1
