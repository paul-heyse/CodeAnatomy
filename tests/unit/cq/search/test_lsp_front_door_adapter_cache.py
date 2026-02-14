from __future__ import annotations

import os
import threading
import time
from pathlib import Path

import pytest
from tools.cq.core.cache import close_cq_cache_backend
from tools.cq.search import lsp_front_door_adapter as adapter
from tools.cq.search.lsp_front_door_adapter import (
    LanguageLspEnrichmentOutcome,
    LanguageLspEnrichmentRequest,
    enrich_with_language_lsp,
)


def _reset_cache_env(tmp_path: Path) -> None:
    close_cq_cache_backend()
    os.environ["CQ_CACHE_ENABLED"] = "1"
    os.environ["CQ_CACHE_DIR"] = str(tmp_path / "cq_cache")


def _clear_cache_env() -> None:
    close_cq_cache_backend()
    os.environ.pop("CQ_CACHE_ENABLED", None)
    os.environ.pop("CQ_CACHE_DIR", None)


def test_lsp_front_door_caches_negative_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_cache_env(tmp_path)
    sample = tmp_path / "sample.py"
    sample.write_text("x = 1\n", encoding="utf-8")

    monkeypatch.setattr(adapter, "resolve_lsp_provider_root", lambda **_kwargs: tmp_path)
    attempts = {"count": 0}

    def _fake_retry(*_args: object, **_kwargs: object) -> tuple[object | None, bool]:
        attempts["count"] += 1
        return None, True

    monkeypatch.setattr(adapter, "call_with_retry", _fake_retry)
    request = LanguageLspEnrichmentRequest(
        language="python",
        mode="search",
        root=tmp_path,
        file_path=sample,
        line=1,
        col=0,
    )

    first = enrich_with_language_lsp(request)
    second = enrich_with_language_lsp(request)

    assert first.failure_reason == "request_timeout"
    assert second.failure_reason == "request_timeout"
    assert attempts["count"] == 1

    _clear_cache_env()


def test_lsp_front_door_caches_positive_outcome(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_cache_env(tmp_path)
    sample = tmp_path / "sample.py"
    sample.write_text("x = 1\n", encoding="utf-8")

    monkeypatch.setattr(adapter, "resolve_lsp_provider_root", lambda **_kwargs: tmp_path)
    attempts = {"count": 0}

    def _fake_retry(*_args: object, **_kwargs: object) -> tuple[object | None, bool]:
        attempts["count"] += 1
        return {"coverage": {"status": "applied"}}, False

    monkeypatch.setattr(adapter, "call_with_retry", _fake_retry)
    request = LanguageLspEnrichmentRequest(
        language="python",
        mode="search",
        root=tmp_path,
        file_path=sample,
        line=1,
        col=0,
    )

    first = enrich_with_language_lsp(request)
    second = enrich_with_language_lsp(request)

    assert isinstance(first.payload, dict)
    assert first.failure_reason is None
    assert isinstance(second.payload, dict)
    assert second.failure_reason is None
    assert attempts["count"] == 1

    _clear_cache_env()


def test_lsp_front_door_single_flight_under_contention(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _reset_cache_env(tmp_path)
    sample = tmp_path / "sample.py"
    sample.write_text("x = 1\n", encoding="utf-8")

    monkeypatch.setattr(adapter, "resolve_lsp_provider_root", lambda **_kwargs: tmp_path)
    calls_lock = threading.Lock()
    attempts = {"count": 0}

    def _fake_retry(*_args: object, **_kwargs: object) -> tuple[object | None, bool]:
        with calls_lock:
            attempts["count"] += 1
        time.sleep(0.15)
        return {"coverage": {"status": "applied"}}, False

    monkeypatch.setattr(adapter, "call_with_retry", _fake_retry)
    request = LanguageLspEnrichmentRequest(
        language="python",
        mode="search",
        root=tmp_path,
        file_path=sample,
        line=1,
        col=0,
    )

    barrier = threading.Barrier(4)
    outcomes: list[object] = []
    outcomes_lock = threading.Lock()

    def _worker() -> None:
        barrier.wait()
        outcome = enrich_with_language_lsp(request)
        with outcomes_lock:
            outcomes.append(outcome)

    threads = [threading.Thread(target=_worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert attempts["count"] == 1
    assert len(outcomes) == 4
    for outcome in outcomes:
        assert isinstance(outcome, LanguageLspEnrichmentOutcome)
        assert isinstance(outcome.payload, dict)

    _clear_cache_env()
