"""Tests for CQ CLI docs/completion asset generation helpers."""

from __future__ import annotations

from pathlib import Path

from scripts.generate_cq_cli_docs import cq_reference_needs_update, generate_cq_reference
from scripts.generate_cq_completion import cq_completions_need_update, generate_cq_completions


def test_generate_cq_reference(tmp_path: Path) -> None:
    """Ensure CQ reference generator writes docs."""
    output_path = tmp_path / "cq_cli.md"
    text = generate_cq_reference(output_path)

    assert output_path.exists()
    assert "Code Query - High-signal code analysis macros" in text
    assert text == output_path.read_text(encoding="utf-8")


def test_cq_reference_needs_update(tmp_path: Path) -> None:
    """Ensure CQ docs freshness check detects stale files."""
    output_path = tmp_path / "cq_cli.md"
    assert cq_reference_needs_update(output_path) is True

    generate_cq_reference(output_path)
    assert cq_reference_needs_update(output_path) is False

    output_path.write_text("# stale\n", encoding="utf-8")
    assert cq_reference_needs_update(output_path) is True


def test_generate_cq_completions(tmp_path: Path) -> None:
    """Ensure completion generator writes all supported shell files."""
    written = generate_cq_completions(tmp_path, program_name="cq")

    assert set(written) == {"bash", "zsh", "fish"}
    for shell in ("bash", "zsh", "fish"):
        path = tmp_path / f"cq.{shell}"
        assert path.exists()
        assert "cq" in path.read_text(encoding="utf-8")


def test_cq_completions_need_update(tmp_path: Path) -> None:
    """Ensure completion freshness check detects stale files."""
    stale = cq_completions_need_update(tmp_path, program_name="cq")
    assert stale

    generate_cq_completions(tmp_path, program_name="cq")
    assert cq_completions_need_update(tmp_path, program_name="cq") == []

    (tmp_path / "cq.bash").write_text("stale", encoding="utf-8")
    stale = cq_completions_need_update(tmp_path, program_name="cq")
    assert (tmp_path / "cq.bash") in stale
