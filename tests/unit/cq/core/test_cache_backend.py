from __future__ import annotations

import os
from pathlib import Path

from tools.cq.core.cache import (
    build_cache_key,
    build_cache_tag,
    build_run_cache_tag,
    close_cq_cache_backend,
    get_cq_cache_backend,
)


def test_build_cache_key_is_deterministic() -> None:
    key_a = build_cache_key(
        "lsp_front_door",
        version="v2",
        workspace="/repo",
        language="python",
        target="src/app.py",
        extras={"line": 12, "col": 4},
    )
    key_b = build_cache_key(
        "lsp_front_door",
        version="v2",
        workspace="/repo",
        language="python",
        target="src/app.py",
        extras={"line": 12, "col": 4},
    )
    key_c = build_cache_key(
        "lsp_front_door",
        version="v2",
        workspace="/repo",
        language="python",
        target="src/app.py",
        extras={"line": 13, "col": 4},
    )

    assert key_a == key_b
    assert key_a != key_c
    assert key_a.startswith("cq:lsp_front_door:v2:")


def test_build_cache_tag() -> None:
    assert build_cache_tag(workspace="/repo", language="rust") == "/repo:rust"


def test_build_run_cache_tag() -> None:
    assert (
        build_run_cache_tag(workspace="/repo", language="python", run_id="abc123")
        == "/repo:python:run:abc123"
    )


def test_cache_backend_roundtrip(tmp_path: Path) -> None:
    close_cq_cache_backend()
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")
    os.environ["CQ_CACHE_ENABLED"] = "1"

    backend = get_cq_cache_backend(root=tmp_path)
    backend.set("k", {"v": 1}, expire=30, tag="x")
    value = backend.get("k")

    assert value == {"v": 1}

    backend.evict_tag("x")
    assert backend.get("k") is None

    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_DIR", None)
    os.environ.pop("CQ_CACHE_ENABLED", None)


def test_cache_backend_noop_when_disabled(tmp_path: Path) -> None:
    close_cq_cache_backend()
    os.environ["CQ_CACHE_ENABLED"] = "0"

    backend = get_cq_cache_backend(root=tmp_path)
    backend.set("k", 1)
    assert backend.get("k") is None

    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)


def test_cache_backend_is_workspace_keyed(tmp_path: Path) -> None:
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
