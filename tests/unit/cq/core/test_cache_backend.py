"""Tests for test_cache_backend."""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest
from tools.cq.core.cache import (
    build_cache_key,
    build_cache_tag,
    build_run_cache_tag,
    build_scope_hash,
    canonicalize_cache_payload,
    close_cq_cache_backend,
    get_cq_cache_backend,
)

INCREMENTED_COUNTER_VALUE = 3
DECREMENTED_COUNTER_VALUE = 2
EXPECTED_BULK_WRITES = 2
FANOUT_INIT_ATTEMPTS = 2


def test_build_cache_key_is_deterministic() -> None:
    """Check cache keys remain stable when unordered payload inputs are reused."""
    key_a = build_cache_key(
        "semantic_front_door",
        version="v2",
        workspace="/repo",
        language="python",
        target="src/app.py",
        extras={"line": 12, "col": 4},
    )
    key_b = build_cache_key(
        "semantic_front_door",
        version="v2",
        workspace="/repo",
        language="python",
        target="src/app.py",
        extras={"col": 4, "line": 12},
    )
    key_c = build_cache_key(
        "semantic_front_door",
        version="v2",
        workspace="/repo",
        language="python",
        target="src/app.py",
        extras={"line": 13, "col": 4},
    )

    assert key_a == key_b
    assert key_a != key_c
    assert key_a.startswith("cq:semantic_front_door:v2:")



def test_canonicalize_cache_payload_sorts_unordered_values() -> None:
    """Normalize cache payload ordering before hashing or serialization."""
    payload = canonicalize_cache_payload(
        {
            "b": {"z": {"x", "y"}},
            "a": [3, 2, 1],
        }
    )
    assert list(payload) == ["a", "b"]
    assert payload["b"] == {"z": ["x", "y"]}



def test_build_cache_tag() -> None:
    """Generate a canonical workspace-level cache tag."""
    tag = build_cache_tag(workspace="/repo", language="rust")
    assert tag.startswith("ws:")
    assert "|lang:rust" in tag



def test_build_run_cache_tag() -> None:
    """Include run context in cache tags when requested."""
    tag = build_run_cache_tag(workspace="/repo", language="python", run_id="abc123")
    assert tag.startswith("ws:")
    assert "|lang:python|" in tag
    assert "run:" in tag



def test_build_scope_hash_is_stable() -> None:
    """Verify scope hash is stable independent of key ordering."""
    hash_a = build_scope_hash({"paths": ["a", "b"], "globs": ("*.py",)})
    hash_b = build_scope_hash({"globs": ("*.py",), "paths": ["a", "b"]})
    assert isinstance(hash_a, str)
    assert hash_a == hash_b



def test_cache_backend_roundtrip(tmp_path: Path) -> None:
    """Validate set/get/evict flow for the active backend."""
    close_cq_cache_backend()
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")
    os.environ["CQ_CACHE_ENABLED"] = "1"

    backend = get_cq_cache_backend(root=tmp_path)
    assert backend.set("k", {"v": 1}, expire=30, tag="x") is True
    assert backend.get("k") == {"v": 1}

    assert backend.evict_tag("x") is True
    assert backend.get("k") is None

    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_DIR", None)
    os.environ.pop("CQ_CACHE_ENABLED", None)



def test_cache_backend_advanced_operations(tmp_path: Path) -> None:
    """Exercise add/increment/decrement/set-many and transaction helpers."""
    close_cq_cache_backend()
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache_adv")
    os.environ["CQ_CACHE_ENABLED"] = "1"

    backend = get_cq_cache_backend(root=tmp_path)
    assert backend.add("counter", 1, expire=30, tag="x") is True
    assert backend.add("counter", 2, expire=30, tag="x") is False
    assert backend.incr("counter", 2, default=0) == INCREMENTED_COUNTER_VALUE
    assert backend.decr("counter", 1, default=0) == DECREMENTED_COUNTER_VALUE
    written = backend.set_many(
        {
            "bulk:k1": {"value": 1},
            "bulk:k2": {"value": 2},
        },
        expire=30,
        tag="bulk",
    )
    assert written == EXPECTED_BULK_WRITES
    assert backend.get("bulk:k1") == {"value": 1}
    assert backend.get("bulk:k2") == {"value": 2}

    with backend.transact():
        assert backend.set("k1", 1, expire=30, tag="x") is True
        assert backend.set("k2", 2, expire=30, tag="x") is True

    stats = backend.stats()
    assert isinstance(stats, dict)
    volume = backend.volume()
    assert volume is None or isinstance(volume, int)
    removed = backend.cull()
    assert removed is None or isinstance(removed, int)

    assert backend.delete("k1") is True
    assert backend.get("k1") is None

    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_DIR", None)
    os.environ.pop("CQ_CACHE_ENABLED", None)



def test_cache_backend_noop_when_disabled(tmp_path: Path) -> None:
    """Expect no writes or reads when CQ cache is disabled by env."""
    close_cq_cache_backend()
    os.environ["CQ_CACHE_ENABLED"] = "0"

    backend = get_cq_cache_backend(root=tmp_path)
    assert backend.set("k", 1) is False
    assert backend.get("k") is None

    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)



def test_cache_backend_is_workspace_keyed(tmp_path: Path) -> None:
    """Ensure cache backends are separate for different workspaces."""
    close_cq_cache_backend()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")
    workspace_a = tmp_path / "a"
    workspace_b = tmp_path / "b"
    workspace_a.mkdir(parents=True, exist_ok=True)
    workspace_b.mkdir(parents=True, exist_ok=True)

    backend_a = get_cq_cache_backend(root=workspace_a)
    backend_b = get_cq_cache_backend(root=workspace_b)

    assert backend_a is not backend_b

    close_cq_cache_backend(root=workspace_a)
    backend_a2 = get_cq_cache_backend(root=workspace_a)
    assert backend_a2 is not backend_a

    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)


def test_cache_backend_fails_open_on_corrupt_diskcache(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify corrupt diskcache initialization falls back to a safe backend."""
    close_cq_cache_backend()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")

    from tools.cq.core.cache import diskcache_backend

    attempts = {"count": 0}

    def _raise_corrupt(*_args: object, **_kwargs: object) -> object:
        attempts["count"] += 1
        msg = "database disk image is malformed"
        raise sqlite3.DatabaseError(msg)

    monkeypatch.setattr(diskcache_backend, "FanoutCache", _raise_corrupt)
    backend = get_cq_cache_backend(root=tmp_path)
    assert backend.set("k", {"v": 1}) is False
    assert backend.get("k") is None
    assert attempts["count"] == FANOUT_INIT_ATTEMPTS

    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)
