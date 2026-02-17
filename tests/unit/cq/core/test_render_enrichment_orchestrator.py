"""Tests for render-enrichment orchestrator helpers."""

from __future__ import annotations

from pathlib import Path

from tools.cq.core.render_enrichment_orchestrator import (
    maybe_attach_render_enrichment,
    precompute_render_enrichment_cache,
    select_enrichment_target_files,
)
from tools.cq.core.schema import Anchor, CqResult, DetailPayload, Finding, RunMeta


def _run_meta(root: Path) -> RunMeta:
    return RunMeta(
        macro="search",
        argv=["cq", "search", "target"],
        root=str(root),
        started_ms=0.0,
        elapsed_ms=1.0,
        toolchain={},
    )


def _finding(file: str, *, score: float) -> Finding:
    return Finding(
        category="definition",
        message=f"target in {file}",
        anchor=Anchor(file=file, line=1, col=0),
        details=DetailPayload.from_legacy(
            {"language": "python", "score": score, "match_text": "target"}
        ),
    )


class _FakePort:
    @staticmethod
    def enrich_anchor(
        *,
        root: Path,
        file: str,
        line: int,
        col: int,
        language: str,
        candidates: list[str],
    ) -> dict[str, object]:
        _ = (root, file, line, col, language)
        return {"python": {"candidate": candidates[0] if candidates else ""}}


def test_select_enrichment_target_files_returns_ranked_files(tmp_path: Path) -> None:
    """Target file selection keeps highest-priority ranked files."""
    result = CqResult(
        run=_run_meta(tmp_path),
        key_findings=(_finding("src/a.py", score=8.0), _finding("src/b.py", score=3.0)),
    )
    selected = select_enrichment_target_files(result)
    assert "src/a.py" in selected
    assert "src/b.py" in selected


def test_precompute_render_enrichment_cache_populates_cache(tmp_path: Path) -> None:
    """Precompute helper materializes enrichment payload cache entries."""
    result = CqResult(
        run=_run_meta(tmp_path),
        key_findings=(_finding("src/a.py", score=8.0),),
    )
    cache: dict[tuple[str, int, int, str], dict[str, object]] = {}

    tasks = precompute_render_enrichment_cache(
        result=result,
        root=tmp_path,
        cache=cache,
        allowed_files={"src/a.py"},
        port=_FakePort(),
    )

    assert len(tasks) == 1
    key = ("src/a.py", 1, 0, "python")
    assert key in cache
    assert isinstance(cache[key].get("python"), dict)


def test_maybe_attach_render_enrichment_uses_cache(tmp_path: Path) -> None:
    """On-demand attachment reads and applies precomputed cache payloads."""
    finding = _finding("src/a.py", score=8.0)
    cache: dict[tuple[str, int, int, str], dict[str, object]] = {
        ("src/a.py", 1, 0, "python"): {"python": {"item_role": "free_function"}},
    }

    finding = maybe_attach_render_enrichment(
        finding,
        root=tmp_path,
        cache=cache,
        allowed_files=None,
        port=None,
    )

    python_payload = finding.details.get("python")
    assert isinstance(python_payload, dict)
    assert python_payload["item_role"] == "free_function"
