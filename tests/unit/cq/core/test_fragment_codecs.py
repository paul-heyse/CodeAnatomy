"""Tests for fragment cache codecs."""

from __future__ import annotations

from tools.cq.core.cache.contracts import SearchCandidatesCacheV1
from tools.cq.core.cache.fragment_codecs import (
    decode_fragment_payload,
    encode_fragment_payload,
    is_fragment_cache_payload,
)


def test_fragment_codec_roundtrip_via_msgpack_bytes() -> None:
    payload = SearchCandidatesCacheV1(
        pattern=r"\btarget\b",
        raw_matches=[{"file": "module.py", "line": 1}],
        stats={"total_matches": 1},
    )

    encoded = encode_fragment_payload(payload)
    assert isinstance(encoded, (bytes, bytearray))
    assert is_fragment_cache_payload(encoded)

    decoded = decode_fragment_payload(encoded, type_=SearchCandidatesCacheV1)
    assert decoded is not None
    assert decoded.pattern == payload.pattern
    assert decoded.raw_matches == payload.raw_matches
    assert decoded.stats == payload.stats


def test_fragment_codec_decodes_dict_fallback_payload() -> None:
    payload = {
        "pattern": "x",
        "raw_matches": [{"file": "f.py"}],
        "stats": {"total_matches": 1},
    }

    decoded = decode_fragment_payload(payload, type_=SearchCandidatesCacheV1)
    assert decoded is not None
    assert decoded.pattern == "x"
